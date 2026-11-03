// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! State setup for tests of the services layer and the layers above it.

use std::{
    path::PathBuf,
    sync::{
        LazyLock,
        Mutex as StdMutex,
        PoisonError,
    },
    time::{
        SystemTime,
        UNIX_EPOCH,
    },
};

use lyra_db::bootstrap::{
    DbConfig,
    DbKind,
};
use tempfile::TempDir;
use tokio::sync::{
    Mutex,
    MutexGuard,
};

#[cfg(test)]
pub(crate) use self::fake_dispatch::{
    FakePluginDispatch,
    FakeResponse,
};
#[cfg(test)]
pub(crate) use self::test_state::{
    create_session,
    for_each_db_gap,
    init_default_test_state,
    init_library_test_state,
    publish_config,
    record_plugin_dispatch,
    run_with_recycle_at,
    user_principal,
};
use crate::services::{
    STATE,
    boot::{
        Boot,
        BuildInfo,
    },
    libraries::LibraryConfig,
    settings::server::{
        self as server_settings,
        FileSettings,
        ResolvedSettings,
    },
    state::resolve_settings,
};

static RUNTIME_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// Held for the whole test; removes the data directory of the state initialized under it before
/// releasing the lock.
#[must_use]
pub struct RuntimeTestGuard {
    _lock: MutexGuard<'static, ()>,
}

impl Drop for RuntimeTestGuard {
    fn drop(&mut self) {
        DATA_DIR
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
    }
}

pub(crate) async fn runtime_test_lock() -> RuntimeTestGuard {
    RuntimeTestGuard {
        _lock: RUNTIME_TEST_LOCK.lock().await,
    }
}

/// The data directory of the current test state, removed when the [`RuntimeTestGuard`] it was
/// created under drops or the next test state replaces it.
static DATA_DIR: StdMutex<Option<TempDir>> = StdMutex::new(None);

/// A fresh data directory for the next test state, in place of the current one.
fn replace_data_dir() -> anyhow::Result<PathBuf> {
    let data_dir = tempfile::Builder::new()
        .prefix("lyra-harmony-test-")
        .tempdir()?;
    let path = data_dir.path().to_path_buf();
    let previous = DATA_DIR
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .replace(data_dir);
    drop(previous);
    Ok(path)
}

/// Boot values backed by a uniquely named in-memory DB and a data directory
/// the test state owns, so tests never touch the server's data or any on-disk
/// database.
fn memory_boot() -> anyhow::Result<Boot> {
    let unique_db_name = format!(
        "lyra-harmony-test-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let data_dir = replace_data_dir()?;
    Ok(Boot {
        port: 0,
        transcode_cache_dir: data_dir.join("transcodes"),
        hls_dir: data_dir.join("hls"),
        data_dir,
        db: DbConfig {
            kind: DbKind::Memory,
            path: PathBuf::from(unique_db_name),
        },
        plugins_dir: PathBuf::from("plugins"),
        build: BuildInfo::default(),
    })
}

/// Opens a memory database and resolves `library` and the `settings` file layer
/// against it the way the server does. Memory kind is required: state is
/// re-initialized per test, and an on-disk DB would block on the live process lock.
pub(crate) fn open_memory_state(
    library: Option<LibraryConfig>,
    settings: FileSettings,
) -> anyhow::Result<(Boot, lyra_db::Created, ResolvedSettings)> {
    let boot = memory_boot()?;
    assert!(
        matches!(boot.db.kind, DbKind::Memory),
        "test state must use a memory database"
    );
    let file_settings = server_settings::normalize_file(&settings)?;
    let mut created = lyra_db::create(&boot.db)?;
    let resolved = resolve_settings(&mut created, &boot.data_dir, library, file_settings)?;
    Ok((boot, created, resolved))
}

/// A library seed at `directory`.
pub(crate) fn library_config(
    directory: PathBuf,
    language: Option<String>,
    country: Option<String>,
) -> anyhow::Result<LibraryConfig> {
    LibraryConfig::resolve(
        Some(directory),
        None,
        language.as_deref(),
        country.as_deref(),
    )
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
        sync::{
            Arc,
            Mutex as StdMutex,
            PoisonError,
        },
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
    use crate::services::STATE;

    fn initialize_state(library: Option<super::LibraryConfig>) -> anyhow::Result<()> {
        let (boot, created, resolved) = open_memory_state(library, Default::default())?;
        STATE.initialize(boot, created, resolved)
    }

    /// Minimal services state init for tests that exercise application state without a
    /// library fixture: default config, in-memory DB. Call while holding
    /// [`runtime_test_lock`].
    pub(crate) fn init_default_test_state() -> anyhow::Result<()> {
        initialize_state(None)
    }

    /// A plugin runtime stand-in that keeps the listen updates services hand it.
    #[derive(Default)]
    pub(crate) struct RecordingDispatch {
        listen_updates: StdMutex<Vec<crate::services::listens::ListenUpdatePayload>>,
    }

    impl RecordingDispatch {
        pub(crate) fn listen_updates(&self) -> Vec<crate::services::listens::ListenUpdatePayload> {
            self.listen_updates
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }
    }

    impl crate::services::plugin_dispatch::PluginDispatch for RecordingDispatch {
        fn dispatch_metadata_refresh(
            &self,
            _request: crate::services::plugin_dispatch::MetadataRefreshRequest,
        ) -> futures::future::BoxFuture<
            '_,
            anyhow::Result<crate::services::plugin_dispatch::MetadataRefreshResult>,
        > {
            Box::pin(async { anyhow::bail!("the recording dispatch runs no plugins") })
        }

        fn dispatch_mix_handler(
            &self,
            _request: crate::services::plugin_dispatch::MixHandlerRequest,
        ) -> futures::future::BoxFuture<
            '_,
            anyhow::Result<crate::services::plugin_dispatch::MixHandlerResult>,
        > {
            Box::pin(async { anyhow::bail!("the recording dispatch runs no plugins") })
        }

        fn dispatch_similar_releases(
            &self,
            _request: crate::services::plugin_dispatch::SimilarReleasesDispatchRequest,
        ) -> futures::future::BoxFuture<
            '_,
            anyhow::Result<crate::services::plugin_dispatch::SimilarReleasesDispatchResult>,
        > {
            Box::pin(async { anyhow::bail!("the recording dispatch runs no plugins") })
        }

        fn dispatch_playback_update(
            &self,
            _payload: crate::services::playback_sessions::PlaybackUpdatePayload,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        fn dispatch_listen_update(
            &self,
            payload: crate::services::listens::ListenUpdatePayload,
        ) -> anyhow::Result<()> {
            self.listen_updates
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(payload);
            Ok(())
        }
    }

    /// Installs a [`RecordingDispatch`] as this generation's plugin runtime.
    pub(crate) fn record_plugin_dispatch() -> Arc<RecordingDispatch> {
        let dispatch = Arc::new(RecordingDispatch::default());
        STATE
            .generation()
            .plugin_dispatch
            .replace(Some(dispatch.clone()));
        dispatch
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
            let mut next = loop {
                if let Poll::Ready(output) = operation.as_mut().poll(&mut cx) {
                    return output;
                }
                let mut next = Box::pin(STATE.db.write());
                assert!(next.as_mut().poll(&mut cx).is_pending());
                drop(held.take());
                match next.as_mut().poll(&mut cx) {
                    Poll::Pending => break next,
                    // Nothing was queued ahead, so `operation` waits on something besides the lock.
                    Poll::Ready(guard) => {
                        held = Some(guard);
                        tokio::task::yield_now().await;
                    }
                }
            };
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
        initialize_state(Some(library_config(directory, None, None)?))?;
        start_runtime().await
    }
}

#[cfg(test)]
mod fake_dispatch {
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            Mutex,
        },
    };

    use anyhow::{
        Result,
        anyhow,
        bail,
    };
    use futures::future::BoxFuture;
    use tokio::sync::Semaphore;

    use crate::services::{
        STATE,
        listens::ListenUpdatePayload,
        playback_sessions::PlaybackUpdatePayload,
        plugin_dispatch::{
            MetadataRefreshRequest,
            MetadataRefreshResult,
            MixHandlerRequest,
            MixHandlerResult,
            PluginCallbackHandle,
            PluginDispatch,
            SimilarReleasesDispatchRequest,
            SimilarReleasesDispatchResult,
        },
    };

    #[derive(Clone)]
    pub(crate) enum FakeResponse {
        Value(serde_json::Value),
        Error(String),
        /// Answers with the inner response once the semaphore grants a permit.
        Held(Arc<Semaphore>, Box<FakeResponse>),
        /// Never answers, so the call ends only when its dispatch is dropped.
        Stall,
        /// Fails at the request's deadline, as the runtime ends a late handler.
        UntilDeadline,
    }

    /// A plugin runtime that answers metadata refresh dispatches with the
    /// response added for each handler, recording every call's context.
    #[derive(Default)]
    pub(crate) struct FakePluginDispatch {
        handlers: Mutex<Vec<FakeResponse>>,
        calls: Mutex<HashMap<u64, Vec<serde_json::Value>>>,
    }

    /// The runtime id of every [`FakePluginDispatch`] handle.
    const FAKE_VM_ID: u64 = u64::MAX;

    impl FakePluginDispatch {
        /// A fake published as the current generation's plugin dispatch.
        pub(crate) fn install() -> Arc<Self> {
            let dispatch = Arc::new(Self::default());
            STATE
                .generation()
                .plugin_dispatch
                .replace(Some(dispatch.clone()));
            dispatch
        }

        /// Adds a handler answering with `response`, returning its handle.
        pub(crate) fn add(&self, response: FakeResponse) -> PluginCallbackHandle {
            let mut handlers = self.handlers.lock().expect("fake handlers poisoned");
            handlers.push(response);
            PluginCallbackHandle {
                vm_id: FAKE_VM_ID,
                handler_id: handlers.len() as u64,
            }
        }

        /// The contexts dispatched to `handler`, in call order.
        pub(crate) fn calls(&self, handler: PluginCallbackHandle) -> Vec<serde_json::Value> {
            self.calls
                .lock()
                .expect("fake calls poisoned")
                .get(&handler.handler_id)
                .cloned()
                .unwrap_or_default()
        }

        /// Registers `provider_id` with an artist cover handler answering with `response`,
        /// returning the handler.
        pub(crate) async fn register_artist_cover_handler(
            &self,
            provider_id: &str,
            response: FakeResponse,
        ) -> Result<PluginCallbackHandle> {
            let handler = self.add(response);
            lyra_db::providers::register(&mut *STATE.db.write().await, provider_id)?;
            let registry = crate::services::providers::provider_registry();
            let mut registry = registry.write().await;
            registry.register(
                crate::services::plugin_scope::PluginId::new(
                    format!("{provider_id}_plugin").as_str(),
                )?,
                provider_id.to_string(),
                None,
            )?;
            registry.set_cover_handler(
                provider_id,
                crate::services::EntityType::Artist,
                crate::services::providers::ProviderCoverSpec {
                    timeout: std::time::Duration::from_secs(5),
                    require: Default::default(),
                    handler,
                },
            );
            Ok(handler)
        }

        /// The `artist_name` of each artist context dispatched to `handler`, in call order.
        pub(crate) fn artist_names(&self, handler: PluginCallbackHandle) -> Vec<String> {
            self.calls(handler)
                .iter()
                .map(|context| {
                    context["artist_name"]
                        .as_str()
                        .expect("artist name")
                        .to_string()
                })
                .collect()
        }

        fn response(&self, handler_id: u64) -> Option<FakeResponse> {
            let index = usize::try_from(handler_id).ok()?.checked_sub(1)?;
            self.handlers
                .lock()
                .expect("fake handlers poisoned")
                .get(index)
                .cloned()
        }
    }

    async fn answer(mut response: FakeResponse) -> Result<serde_json::Value> {
        loop {
            match response {
                FakeResponse::Value(value) => return Ok(value),
                FakeResponse::Error(message) => bail!(message),
                FakeResponse::Held(release, then) => {
                    release.acquire().await?.forget();
                    response = *then;
                }
                FakeResponse::Stall | FakeResponse::UntilDeadline => {
                    std::future::pending::<()>().await
                }
            }
        }
    }

    impl PluginDispatch for FakePluginDispatch {
        fn dispatch_metadata_refresh(
            &self,
            request: MetadataRefreshRequest,
        ) -> BoxFuture<'_, Result<MetadataRefreshResult>> {
            Box::pin(async move {
                let handler_id = request.handler.handler_id_in(FAKE_VM_ID)?;
                let response = self
                    .response(handler_id)
                    .ok_or_else(|| anyhow!("metadata handler {handler_id} not found"))?;
                self.calls
                    .lock()
                    .expect("fake calls poisoned")
                    .entry(handler_id)
                    .or_default()
                    .push(request.context);
                if let FakeResponse::UntilDeadline = response {
                    tokio::time::sleep_until(tokio::time::Instant::from_std(request.deadline))
                        .await;
                    bail!("plugin handler deadline expired");
                }
                Ok(MetadataRefreshResult {
                    values: vec![answer(response).await?],
                })
            })
        }

        fn dispatch_mix_handler(
            &self,
            _request: MixHandlerRequest,
        ) -> BoxFuture<'_, Result<MixHandlerResult>> {
            Box::pin(async { bail!("the fake plugin dispatch has no mix handlers") })
        }

        fn dispatch_similar_releases(
            &self,
            _request: SimilarReleasesDispatchRequest,
        ) -> BoxFuture<'_, Result<SimilarReleasesDispatchResult>> {
            Box::pin(async { bail!("the fake plugin dispatch has no similar releases handlers") })
        }

        fn dispatch_playback_update(&self, _payload: PlaybackUpdatePayload) -> Result<()> {
            Ok(())
        }

        fn dispatch_listen_update(&self, _payload: ListenUpdatePayload) -> Result<()> {
            Ok(())
        }
    }
}

/// An image file that exists for the test run, for covers whose file must be there to show.
#[cfg(test)]
pub(crate) fn existing_cover_path() -> String {
    static PATH: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    PATH.get_or_init(|| {
        let path = std::env::temp_dir().join(format!("lyra-test-cover-{}.jpg", std::process::id()));
        std::fs::write(&path, b"cover").expect("write test cover");
        path
    })
    .to_string_lossy()
    .into_owned()
}
