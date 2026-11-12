// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! State setup for tests of the services layer and the layers above it.

use std::{
    path::PathBuf,
    sync::{
        Arc,
        LazyLock,
        Mutex as StdMutex,
        PoisonError,
    },
    time::{
        SystemTime,
        UNIX_EPOCH,
    },
};

#[cfg(test)]
use futures::future::BoxFuture;
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
use crate::services::plugin_dispatch::{
    DispatchNotStarted,
    MetadataRefreshRequest,
    MetadataRefreshResult,
    MixHandlerRequest,
    MixHandlerResult,
    PluginDispatch,
    SimilarReleasesDispatchRequest,
    SimilarReleasesDispatchResult,
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
    publishing_test_db,
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

/// Publishes `config` in place of the current one, keeping the current
/// provenance and file layer. For tests that poke typed config fields;
/// the settings API tests resolve real values instead.
pub(crate) fn publish_config(config: server_settings::Config) {
    let current = STATE.settings.get();
    STATE.publish_settings(Arc::new(ResolvedSettings {
        config: Arc::new(config),
        effective: current.effective.clone(),
        file: current.file.clone(),
    }));
}

/// Signs the user in with a new session and returns them as that session authenticates them,
/// whether or not authentication is enabled.
pub(crate) async fn session_auth(
    user_db_id: agdb::DbId,
) -> anyhow::Result<crate::services::auth::ResolvedAuth> {
    use crate::services::auth;

    let principal = {
        let db = STATE.db.read().await;
        auth::Principal::load(&*db, user_db_id)?
            .ok_or_else(|| anyhow::anyhow!("user {user_db_id:?} does not exist"))?
    };
    let created = auth::sessions::create_session_for_user(&principal, Default::default()).await?;
    let db = STATE.db.read().await;
    let (_, session, _) =
        lyra_db::users::find_by_session_token_hash(&*db, &auth::hash_secret(&created.token))?
            .ok_or_else(|| anyhow::anyhow!("the new session is stored"))?;
    Ok(auth::ResolvedAuth {
        principal,
        credential: auth::AuthCredential::Session {
            public_id: session.id,
        },
        client_name: session.client_name,
    })
}

/// Merges `loser_id` into `winner_id` without a request's checks, recording it as a request
/// would, for tests of what runs beside a merge.
pub(crate) async fn merge_entities(winner_id: &str, loser_id: &str) -> anyhow::Result<()> {
    use crate::services::metadata::changes::{
        merge_into,
        model::MergeKind,
    };

    let mut db = STATE.db.write().await;
    let id_of = |db: &agdb::DbAny, id: &str| {
        lyra_db::lookup::find_node_id_by_id(db, id)?
            .ok_or_else(|| anyhow::anyhow!("{id} is missing"))
    };
    let (winner, loser) = (id_of(&db, winner_id)?, id_of(&db, loser_id)?);
    let kind = MergeKind::of(&*db, winner)?
        .ok_or_else(|| anyhow::anyhow!("{winner_id} can't be merged into"))?;
    let merge = db.transaction_mut(|t| merge_into(t, kind, winner, loser))?;
    STATE.generation().merge_journal.record(merge);
    Ok(())
}

/// Starts the server over as one that takes a setup code, returning the code it drew, if any.
#[cfg(test)]
pub(crate) async fn require_setup_code() -> anyhow::Result<Option<String>> {
    use crate::services::auth::setup_code;

    let boot = STATE.boot.get();
    STATE.boot.replace(Arc::new(Boot {
        require_setup_code: true,
        ..(*boot).clone()
    }));
    setup_code::issue().await?;
    Ok(setup_code::current())
}

/// The dispatcher of a scope with no runtime behind it: nothing it is asked to run starts.
#[cfg(test)]
pub(crate) struct IdleDispatch;

#[cfg(test)]
impl PluginDispatch for IdleDispatch {
    fn dispatch_metadata_refresh(
        &self,
        _request: MetadataRefreshRequest,
    ) -> BoxFuture<'_, anyhow::Result<MetadataRefreshResult>> {
        Box::pin(async { Err(DispatchNotStarted.into()) })
    }

    fn dispatch_mix_handler(
        &self,
        _request: MixHandlerRequest,
    ) -> BoxFuture<'_, anyhow::Result<MixHandlerResult>> {
        Box::pin(async { Err(DispatchNotStarted.into()) })
    }

    fn dispatch_similar_releases(
        &self,
        _request: SimilarReleasesDispatchRequest,
    ) -> BoxFuture<'_, anyhow::Result<SimilarReleasesDispatchResult>> {
        Box::pin(async { Err(DispatchNotStarted.into()) })
    }

    fn dispatch_playback_update(
        &self,
        _payload: crate::services::playback_sessions::PlaybackUpdatePayload,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn dispatch_listen_update(
        &self,
        _payload: crate::services::listens::ListenUpdatePayload,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn dispatch_favorite_update(
        &self,
        _payload: crate::services::favorites::FavoriteUpdatePayload,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn dispatch_rating_update(
        &self,
        _payload: crate::services::ratings::RatingUpdatePayload,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn dispatch_bookmark_update(
        &self,
        _payload: crate::services::bookmarks::BookmarkUpdatePayload,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Publishes `dispatch` with fresh registries of its own, as a plugin reload publishes a new
/// runtime.
#[cfg(test)]
pub(crate) fn publish_dispatch(dispatch: std::sync::Arc<dyn PluginDispatch>) {
    let generation = STATE.generation();
    let current = generation.latest().expect("published test scope");
    generation
        .runtime
        .replace(Some(std::sync::Arc::new(current.republished(dispatch))));
}

/// Answers similar-release calls with `dispatch`, reading handlers from the published scope.
#[cfg(test)]
#[derive(Clone)]
pub(crate) struct ClosureCalls<F>(pub(crate) F);

#[cfg(test)]
impl<F, Fut> crate::services::plugin_dispatch::CallSource for ClosureCalls<F>
where
    F: Fn(SimilarReleasesDispatchRequest) -> Fut + Clone,
    Fut: std::future::Future<Output = anyhow::Result<SimilarReleasesDispatchResult>>,
{
    type Calls = Self;

    async fn reach(&self, _target: &str) -> Option<Self> {
        Some(self.clone())
    }

    fn current(&self) -> Option<Self> {
        Some(self.clone())
    }

    fn in_transition(&self, _target: &str) -> bool {
        false
    }
}

#[cfg(test)]
impl<F, Fut> crate::services::plugin_dispatch::RuntimeCalls for ClosureCalls<F>
where
    F: Fn(SimilarReleasesDispatchRequest) -> Fut + Clone,
    Fut: std::future::Future<Output = anyhow::Result<SimilarReleasesDispatchResult>>,
{
    async fn providers(
        &self,
    ) -> std::sync::Arc<tokio::sync::RwLock<crate::services::providers::ProviderRegistry>> {
        crate::services::providers::provider_registry()
    }

    async fn mixers(
        &self,
    ) -> std::sync::Arc<tokio::sync::RwLock<crate::services::mix::MixRegistry>> {
        crate::services::mix::mix_registry()
    }

    async fn metadata_refresh(
        &self,
        _request: MetadataRefreshRequest,
    ) -> anyhow::Result<MetadataRefreshResult> {
        anyhow::bail!("the closure calls only answer similar releases")
    }

    async fn mix_handler(&self, _request: MixHandlerRequest) -> anyhow::Result<MixHandlerResult> {
        anyhow::bail!("the closure calls only answer similar releases")
    }

    async fn similar_releases(
        &self,
        request: SimilarReleasesDispatchRequest,
    ) -> anyhow::Result<SimilarReleasesDispatchResult> {
        (self.0)(request).await
    }
}

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
        tls: None,
        transcode_cache_dir: data_dir.join("transcodes"),
        cover_cache_dir: data_dir.join("cover-transforms"),
        hls_dir: data_dir.join("hls"),
        require_setup_code: false,
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
        STATE.initialize(boot, created, resolved)?;
        crate::services::rate_limit::password_checks::reset();
        STATE.generation().runtime.replace(Some(Arc::new(
            crate::services::plugin_scope::RuntimeScope::without_plugin_layer(
                Arc::new(super::IdleDispatch),
                Default::default(),
            ),
        )));
        Ok(())
    }

    /// Minimal services state init for tests that exercise application state without a
    /// library fixture: default config, in-memory DB. Call while holding
    /// [`runtime_test_lock`].
    pub(crate) fn init_default_test_state() -> anyhow::Result<()> {
        initialize_state(None)
    }

    /// A database for a synchronous test of a service whose writes publish updates to plugins,
    /// with the default test state they publish through, held while the guard lives.
    pub(crate) fn publishing_test_db() -> anyhow::Result<(agdb::DbAny, super::RuntimeTestGuard)> {
        let guard = futures::executor::block_on(super::runtime_test_lock());
        init_default_test_state()?;
        Ok((lyra_db::test_db::new_test_db()?, guard))
    }

    /// A plugin runtime stand-in that keeps the updates services hand it.
    #[derive(Default)]
    pub(crate) struct RecordingDispatch {
        listen_updates: StdMutex<Vec<crate::services::listens::ListenUpdatePayload>>,
        favorite_updates: StdMutex<Vec<crate::services::favorites::FavoriteUpdatePayload>>,
        rating_updates: StdMutex<Vec<crate::services::ratings::RatingUpdatePayload>>,
        bookmark_updates: StdMutex<Vec<crate::services::bookmarks::BookmarkUpdatePayload>>,
    }

    impl RecordingDispatch {
        pub(crate) fn listen_updates(&self) -> Vec<crate::services::listens::ListenUpdatePayload> {
            self.listen_updates
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }

        pub(crate) fn favorite_updates(
            &self,
        ) -> Vec<crate::services::favorites::FavoriteUpdatePayload> {
            self.favorite_updates
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }

        pub(crate) fn rating_updates(&self) -> Vec<crate::services::ratings::RatingUpdatePayload> {
            self.rating_updates
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }

        pub(crate) fn bookmark_updates(
            &self,
        ) -> Vec<crate::services::bookmarks::BookmarkUpdatePayload> {
            self.bookmark_updates
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

        fn dispatch_favorite_update(
            &self,
            payload: crate::services::favorites::FavoriteUpdatePayload,
        ) -> anyhow::Result<()> {
            self.favorite_updates
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(payload);
            Ok(())
        }

        fn dispatch_rating_update(
            &self,
            payload: crate::services::ratings::RatingUpdatePayload,
        ) -> anyhow::Result<()> {
            self.rating_updates
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(payload);
            Ok(())
        }

        fn dispatch_bookmark_update(
            &self,
            payload: crate::services::bookmarks::BookmarkUpdatePayload,
        ) -> anyhow::Result<()> {
            self.bookmark_updates
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(payload);
            Ok(())
        }
    }

    /// Installs a [`RecordingDispatch`] as this generation's plugin runtime.
    pub(crate) fn record_plugin_dispatch() -> Arc<RecordingDispatch> {
        let dispatch = Arc::new(RecordingDispatch::default());
        super::publish_dispatch(dispatch.clone());
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
            MixHandlerTrack,
            PluginCallbackHandle,
            PluginDispatch,
            RefreshTargetGone,
            SimilarReleasesDispatchRequest,
            SimilarReleasesDispatchResult,
        },
    };

    #[derive(Clone)]
    pub(crate) enum FakeResponse {
        Value(serde_json::Value),
        Error(String),
        /// Fails as a handler does when something it was writing is gone.
        TargetGone(String),
        /// Answers with the inner response once the semaphore grants a permit.
        Held(Arc<Semaphore>, Box<FakeResponse>),
        /// Never answers, so the call ends only when its dispatch is dropped.
        Stall,
        /// Fails at the request's deadline, as the runtime ends a late handler.
        UntilDeadline,
    }

    /// A plugin runtime that answers metadata refresh dispatches with the
    /// response added for each handler, recording every call's context.
    pub(crate) struct FakePluginDispatch {
        vm_id: u64,
        handlers: Mutex<Vec<FakeResponse>>,
        calls: Mutex<HashMap<u64, Vec<serde_json::Value>>>,
    }

    /// The runtime id of a [`FakePluginDispatch`] handle unless it was installed as another.
    const FAKE_VM_ID: u64 = u64::MAX;

    impl FakePluginDispatch {
        /// A fake published as the current generation's plugin dispatch.
        pub(crate) fn install() -> Arc<Self> {
            Self::install_as(FAKE_VM_ID)
        }

        /// A fake with runtime id `vm_id` published in place of the current dispatch, as a
        /// plugin reload publishes its new runtime.
        pub(crate) fn install_as(vm_id: u64) -> Arc<Self> {
            let dispatch = Arc::new(Self {
                vm_id,
                handlers: Mutex::default(),
                calls: Mutex::default(),
            });
            crate::services::testing::publish_dispatch(dispatch.clone());
            dispatch
        }

        /// Adds a handler answering with `response`, returning its handle.
        pub(crate) fn add(&self, response: FakeResponse) -> PluginCallbackHandle {
            let mut handlers = self.handlers.lock().expect("fake handlers poisoned");
            handlers.push(response);
            PluginCallbackHandle {
                vm_id: self.vm_id,
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

        pub(crate) async fn wait_for_calls(&self, handler: PluginCallbackHandle, calls: usize) {
            for _ in 0..500 {
                if self.calls(handler).len() >= calls {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            panic!(
                "handler {} was not called {calls} times",
                handler.handler_id
            );
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

        fn record(&self, handler_id: u64, context: serde_json::Value) {
            self.calls
                .lock()
                .expect("fake calls poisoned")
                .entry(handler_id)
                .or_default()
                .push(context);
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
                FakeResponse::TargetGone(message) => {
                    return Err(RefreshTargetGone(message).into());
                }
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
                let handler_id = request.handler.handler_id_in(self.vm_id)?;
                let response = self
                    .response(handler_id)
                    .ok_or_else(|| anyhow!("metadata handler {handler_id} not found"))?;
                self.record(handler_id, request.context);
                if let FakeResponse::UntilDeadline = response {
                    tokio::time::sleep(request.timeout).await;
                    bail!("plugin handler deadline expired");
                }
                Ok(MetadataRefreshResult {
                    values: vec![answer(response).await?],
                })
            })
        }

        fn dispatch_mix_handler(
            &self,
            request: MixHandlerRequest,
        ) -> BoxFuture<'_, Result<MixHandlerResult>> {
            Box::pin(async move {
                let handler_id = request.handler.handler_id_in(self.vm_id)?;
                let response = self
                    .response(handler_id)
                    .ok_or_else(|| anyhow!("mix handler {handler_id} not found"))?;
                self.record(handler_id, serde_json::Value::Null);
                let tracks = answer(response).await?;
                let tracks = tracks
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(serde_json::Value::as_str)
                    .map(|track_id| MixHandlerTrack {
                        track_id: track_id.to_string(),
                        score: None,
                    })
                    .collect();
                Ok(MixHandlerResult { tracks })
            })
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

        fn dispatch_favorite_update(
            &self,
            _payload: crate::services::favorites::FavoriteUpdatePayload,
        ) -> Result<()> {
            Ok(())
        }

        fn dispatch_rating_update(
            &self,
            _payload: crate::services::ratings::RatingUpdatePayload,
        ) -> Result<()> {
            Ok(())
        }

        fn dispatch_bookmark_update(
            &self,
            _payload: crate::services::bookmarks::BookmarkUpdatePayload,
        ) -> Result<()> {
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
