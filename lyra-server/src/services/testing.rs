// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! State setup for tests of the services layer and the layers above it.

use std::{
    path::PathBuf,
    sync::LazyLock,
    time::{
        SystemTime,
        UNIX_EPOCH,
    },
};

use lyra_db::bootstrap::{
    DbConfig,
    DbKind,
};
use tokio::sync::{
    Mutex,
    MutexGuard,
};

#[cfg(test)]
pub(crate) use self::test_state::{
    create_session,
    for_each_db_gap,
    init_default_test_state,
    init_library_test_state,
    publish_config,
    run_with_recycle_at,
    user_principal,
};
use crate::{
    config::{
        BootConfig,
        ConfigFile,
        LibraryFile,
    },
    services::{
        STATE,
        libraries::LibraryConfig,
        settings::server::{
            self as server_settings,
            ResolvedSettings,
        },
        state::resolve_settings,
    },
};

static RUNTIME_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

pub(crate) async fn runtime_test_lock() -> MutexGuard<'static, ()> {
    RUNTIME_TEST_LOCK.lock().await
}

/// Boot config backed by a uniquely named in-memory DB so tests never touch
/// the data directory or any on-disk database.
fn memory_boot_config() -> anyhow::Result<BootConfig> {
    let unique_db_name = format!(
        "lyra-harmony-test-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    Ok(BootConfig {
        port: 0,
        data_dir: std::env::temp_dir().join(&unique_db_name),
        db: DbConfig {
            kind: DbKind::Memory,
            path: PathBuf::from(unique_db_name),
        },
    })
}

/// Opens a memory database and resolves `file` against it the way the server
/// does. Memory kind is required: state is re-initialized per test, and an
/// on-disk DB would block on the live process lock.
pub(crate) fn open_memory_state(
    file: ConfigFile,
) -> anyhow::Result<(BootConfig, lyra_db::Created, ResolvedSettings)> {
    let boot = memory_boot_config()?;
    assert!(
        matches!(boot.db.kind, DbKind::Memory),
        "test state must use a memory database"
    );
    let library = LibraryConfig::resolve(file.library.as_ref())?;
    let file_settings = server_settings::normalize_file(&file.settings, &boot)?;
    let mut created = lyra_db::create(&boot.db)?;
    let resolved = resolve_settings(&mut created, &boot, library, file_settings)?;
    Ok((boot, created, resolved))
}

/// A `config.json` whose library is `directory`.
pub(crate) fn library_config(
    directory: PathBuf,
    language: Option<String>,
    country: Option<String>,
) -> ConfigFile {
    ConfigFile {
        library: Some(LibraryFile {
            path: Some(directory),
            name: None,
            language,
            country,
        }),
        ..ConfigFile::default()
    }
}

/// The server record and HLS state the runtime starts with.
pub(crate) async fn start_runtime() -> anyhow::Result<()> {
    lyra_db::server::ensure(&mut *STATE.db.write().await)?;
    crate::services::hls::init::initialize().await;
    crate::services::transcoding::initialize().await;
    Ok(())
}

#[cfg(test)]
mod test_state {
    use std::{
        path::PathBuf,
        sync::Arc,
    };

    use agdb::{
        DbAny,
        DbId,
    };

    use super::{
        library_config,
        open_memory_state,
        start_runtime,
    };
    use crate::{
        config::ConfigFile,
        services::STATE,
    };

    fn initialize_state(file: ConfigFile) -> anyhow::Result<()> {
        let (boot, created, resolved) = open_memory_state(file)?;
        STATE.initialize(boot, created, resolved)
    }

    /// Minimal services state init for tests that exercise application state without a
    /// library fixture: default config, in-memory DB. Call while holding
    /// [`runtime_test_lock`].
    pub(crate) fn init_default_test_state() -> anyhow::Result<()> {
        initialize_state(ConfigFile::default())
    }

    /// Principal for an existing user, built the way request auth builds one.
    pub(crate) async fn user_principal(
        user_db_id: DbId,
    ) -> anyhow::Result<crate::services::auth::Principal> {
        let db = STATE.db.read().await;
        let user = lyra_db::users::get_by_id(&db, user_db_id)?
            .ok_or_else(|| anyhow::anyhow!("user {user_db_id:?} does not exist"))?;
        Ok(crate::services::auth::resolve_principal(
            &db, user_db_id, user,
        ))
    }

    pub(crate) async fn create_session(
        user_db_id: DbId,
        metadata: crate::services::auth::sessions::SessionMetadata,
    ) -> anyhow::Result<crate::services::auth::sessions::CreatedSession> {
        let principal = user_principal(user_db_id).await?;
        Ok(crate::services::auth::sessions::create_session_for_user(&principal, metadata).await?)
    }

    /// Publishes `config` in place of the current one, keeping the current
    /// provenance and file layer. For tests that poke typed config fields;
    /// the settings API tests resolve real values instead.
    pub(crate) fn publish_config(config: crate::services::settings::server::Config) {
        let current = STATE.settings.get();
        STATE.publish_settings(Arc::new(
            crate::services::settings::server::ResolvedSettings {
                config: Arc::new(config),
                effective: current.effective.clone(),
                file: current.file.clone(),
            },
        ));
    }

    /// Runs `operation` to completion, handing the DB write lock to `between` each time `operation`
    /// releases the lock and waits on it again. `between` gets the gap's index, starting at 0, so a
    /// test can mutate the DB between any two guards `operation` takes. The lock is FIFO, so a writer
    /// queued behind `operation`'s pending acquisition runs as soon as `operation` releases it.
    pub(crate) async fn run_with_db_gaps<T>(
        operation: impl std::future::Future<Output = T>,
        mut between: impl FnMut(usize, &mut DbAny),
    ) -> T {
        use std::task::Poll;

        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        let mut operation = std::pin::pin!(operation);
        let mut held = Some(STATE.db.write().await);
        for gap in 0.. {
            if let Poll::Ready(output) = operation.as_mut().poll(&mut cx) {
                return output;
            }
            let mut next = Box::pin(STATE.db.write());
            assert!(next.as_mut().poll(&mut cx).is_pending());
            drop(held.take());
            loop {
                if let Poll::Ready(output) = operation.as_mut().poll(&mut cx) {
                    return output;
                }
                if let Poll::Ready(mut guard) = next.as_mut().poll(&mut cx) {
                    between(gap, &mut guard);
                    held = Some(guard);
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        unreachable!("gap counter overflowed")
    }

    /// Runs `operation`, applying `recycle` in gap `gap` only; `None` is a control run that recycles
    /// nothing. Returns whether the gap was reached, so the recycle actually ran, and the output.
    pub(crate) async fn run_with_recycle_at<T>(
        gap: Option<usize>,
        operation: impl std::future::Future<Output = T>,
        recycle: impl FnOnce(&mut DbAny),
    ) -> (bool, T) {
        let mut recycle = Some(recycle);
        let output = run_with_db_gaps(operation, |index, db| {
            if Some(index) == gap
                && let Some(recycle) = recycle.take()
            {
                recycle(db);
            }
        })
        .await;
        (recycle.is_none(), output)
    }

    /// Runs `attempt` at gaps 0, 1, … until one is never reached, handing each reached run's output
    /// to `check`. Fails unless at least one gap ran.
    pub(crate) async fn for_each_db_gap<T>(
        mut attempt: impl AsyncFnMut(Option<usize>) -> anyhow::Result<(bool, T)>,
        mut check: impl AsyncFnMut(usize, T) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        for gap in 0.. {
            let (reached, output) = attempt(Some(gap)).await?;
            if !reached {
                anyhow::ensure!(gap > 0, "the operation released no guard to recycle in");
                return Ok(());
            }
            check(gap, output).await?;
        }
        unreachable!("gap counter overflowed")
    }

    /// Services state over a library at `directory`, started like the runtime.
    pub(crate) async fn init_library_test_state(directory: PathBuf) -> anyhow::Result<()> {
        initialize_state(library_config(directory, None, None))?;
        start_runtime().await
    }
}
