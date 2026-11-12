// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    any::{
        Any,
        TypeId,
    },
    collections::{
        HashMap,
        HashSet,
    },
    path::Path,
    sync::{
        Arc,
        Mutex as StdMutex,
        OnceLock,
        RwLock as StdRwLock,
    },
};

use agdb::{
    DbAny,
    DbId,
};
use anyhow::Result;
use tokio::sync::OwnedRwLockReadGuard;

use crate::services::{
    auth::{
        device_logins::DeviceLoginStore,
        media_tokens::MediaTokenStore,
        setup_code::SetupCode,
        socket_tickets::SocketTicketStore,
    },
    boot::Boot,
    hls::cleanup::HlsCleanupState,
    libraries::LibraryConfig,
    libraries::LibrarySyncRegistries,
    playback_sessions::PlaybackUpdateRegistries,
    playbacks::{
        bindings::ConnectionBindings,
        socket_claims::SocketClaims,
    },
    plugin_dispatch::TransitionWait,
    plugin_scope::{
        PluginId,
        RuntimeScope,
    },
    providers::ProviderLocks,
    settings::server::{
        self as server_settings,
        Config,
        EffectiveSetting,
        FileSettings,
        ResolvedSettings,
    },
};
use lyra_db::DbAsync;

#[derive(Clone)]
pub(crate) struct SwapHandle<T: Clone> {
    inner: Arc<StdRwLock<T>>,
}

impl<T: Clone + Default> Default for SwapHandle<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: Clone> SwapHandle<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            inner: Arc::new(StdRwLock::new(value)),
        }
    }

    pub(crate) fn get(&self) -> T {
        self.inner.read().expect("handle poisoned").clone()
    }

    pub(crate) fn replace(&self, value: T) {
        *self.inner.write().expect("handle poisoned") = value;
    }
}

/// DB slot. `reset_with` is **not** safe concurrently; serialize externally
/// (today `RUNTIME_TEST_LOCK`).
#[derive(Clone)]
pub(crate) struct DbHandle {
    db: SwapHandle<DbAsync>,
    lock: Arc<StdMutex<Option<lyra_db::process_lock::DbProcessLock>>>,
}

impl DbHandle {
    fn new(created: lyra_db::Created) -> Self {
        Self {
            db: SwapHandle::new(created.db),
            lock: Arc::new(StdMutex::new(created.lock)),
        }
    }

    pub(crate) fn get(&self) -> DbAsync {
        self.db.get()
    }

    fn reset(&self, created: lyra_db::Created) {
        let old_lock = self.lock.lock().expect("db lock poisoned").take();
        drop(old_lock);

        self.db.replace(created.db);
        *self.lock.lock().expect("db lock poisoned") = created.lock;
    }

    pub(crate) async fn read(&self) -> OwnedRwLockReadGuard<DbAny> {
        self.get().read_owned().await
    }

    pub(crate) async fn write(&self) -> lyra_db::OwnedDbWriteGuard {
        self.get().write_owned().await
    }

    /// A write lock for changes that leave the catalog as it is; see
    /// [`lyra_db::DbAsync::write_outside_catalog`].
    pub(crate) async fn write_outside_catalog(&self) -> lyra_db::OwnedDbWriteGuard {
        self.get().write_outside_catalog_owned().await
    }
}

pub(crate) type BootHandle = SwapHandle<Arc<Boot>>;
pub(crate) type SettingsHandle = SwapHandle<Arc<ResolvedSettings>>;
pub(crate) type RuntimeScopeHandle = SwapHandle<Option<Arc<RuntimeScope>>>;

/// Auth bookkeeping derived from the current database. Lives on
/// [`GenerationState`] instead of module statics so a DB swap cannot carry
/// ids or grants into the next database (memory DBs reuse small ids).
#[derive(Default)]
pub(crate) struct AuthCaches {
    pub(crate) api_key_last_used: StdMutex<HashMap<DbId, i64>>,
    pub(crate) session_last_seen: StdMutex<HashMap<String, i64>>,
    pub(crate) media_tokens: MediaTokenStore,
    pub(crate) socket_tickets: SocketTicketStore,
    pub(crate) device_logins: DeviceLoginStore,
    pub(crate) setup_code: SetupCode,
}

/// Everything services derive from the current database/config generation. A
/// reset swaps the whole struct for a fresh [`Default`], so generation-bound
/// state dies with its generation by construction — there is no per-registry
/// reset list to keep in sync. New DB- or config-derived service registries
/// belong here, never in module statics; layers above keep their own
/// generation, swapped by the same reset.
#[derive(Default)]
pub(crate) struct GenerationState {
    /// The published plugin runtime, with everything derived from it.
    pub(crate) runtime: RuntimeScopeHandle,
    transition: tokio::sync::watch::Sender<Transition>,
    /// Generation state of the layers above services, swapped with this one.
    pub(crate) layers: LayerStates,
    pub(crate) auth_caches: AuthCaches,
    pub(crate) providers: ProviderLocks,
    pub(crate) merge_journal: crate::services::metadata::merge_journal::MergeJournal,
    pub(crate) playback_updates: PlaybackUpdateRegistries,
    pub(crate) connection_bindings: ConnectionBindings,
    pub(crate) socket_claims: SocketClaims,
    pub(crate) library_sync: LibrarySyncRegistries,
    pub(crate) catalog_index: crate::services::catalog::index::IndexCache,
    pub(crate) hls_cleanup: HlsCleanupState,
    pub(crate) transcode_cache: Arc<crate::services::transcoding::cache::TranscodeCache>,
    pub(crate) cover_transforms: OnceLock<Arc<crate::services::covers::TransformCache>>,
    pub(crate) cover_discards: crate::services::covers::CoverDiscards,
}

impl GenerationState {
    /// The published runtime scope, without waiting for anything.
    pub(crate) fn latest(&self) -> Option<Arc<RuntimeScope>> {
        self.runtime.get()
    }

    /// The published scope once no transition covers `target`. An `Err` means the wait ended
    /// first; the scope published then may be the one being replaced, so it isn't returned.
    pub(crate) async fn live(
        &self,
        target: &str,
        wait: &TransitionWait,
    ) -> Result<Option<Arc<RuntimeScope>>, TransitionNotSettled> {
        self.settled(wait, |transition| transition.covers(target))
            .await?;
        Ok(self.latest())
    }

    pub(crate) fn in_transition(&self, target: &str) -> bool {
        self.transition.borrow().covers(target)
    }

    /// Waits until no transition covers a nested call to `target` from a chain with `admitted`
    /// running. It waits only on the restart of a plugin not in the chain: a reload is waiting
    /// for the very handler making this call, and so is the drain of a plugin the chain runs in.
    pub(crate) async fn settled_for_nested(
        &self,
        admitted: &[PluginId],
        target: &str,
        wait: &TransitionWait,
    ) -> Result<(), TransitionNotSettled> {
        self.settled(wait, |transition| {
            transition.holds_up_nested(admitted, target)
        })
        .await
    }

    /// Holds up `targets` as well, for a plugin's transition that found them only once its new
    /// run registered them.
    pub(crate) fn extend_transition(&self, more: HashSet<String>) {
        self.transition.send_modify(|transition| {
            if let Transition::Plugin { targets, .. } = transition {
                Arc::make_mut(targets).extend(more);
            }
        });
    }

    /// Waits until no transition covers any of `plugins`.
    pub(crate) async fn plugins_settled(
        &self,
        plugins: &[PluginId],
        wait: &TransitionWait,
    ) -> Result<(), TransitionNotSettled> {
        self.settled(wait, |transition| {
            plugins
                .iter()
                .any(|plugin| transition.covers_plugin(plugin))
        })
        .await
    }

    #[cfg(test)]
    pub(crate) fn route_is_held(&self, plugin: &PluginId) -> bool {
        self.transition.borrow().covers_plugin(plugin)
    }

    pub(crate) fn nested_in_transition(&self, admitted: &[PluginId], target: &str) -> bool {
        self.transition.borrow().holds_up_nested(admitted, target)
    }

    async fn settled(
        &self,
        wait: &TransitionWait,
        mut holds_up: impl FnMut(&Transition) -> bool,
    ) -> Result<(), TransitionNotSettled> {
        let mut transition = self.transition.subscribe();
        let settled = transition.wait_for(|transition| !holds_up(transition));
        match wait {
            TransitionWait::Until(deadline) => tokio::time::timeout_at(*deadline, settled)
                .await
                .map(|_| ())
                .map_err(|_| TransitionNotSettled),
            TransitionWait::Settled { cancel } => tokio::select! {
                biased;
                _ = settled => Ok(()),
                () = cancel.cancelled() => Err(TransitionNotSettled),
            },
        }
    }

    /// New calls covered by `transition` wait in [`Self::live`] until the returned guard drops.
    #[must_use]
    pub(crate) fn begin_transition(self: &Arc<Self>, transition: Transition) -> TransitionGuard {
        self.transition.send_replace(transition);
        TransitionGuard {
            generation: self.clone(),
        }
    }
}

/// What is being replaced, and so which new calls wait.
#[derive(Clone, Default)]
pub(crate) enum Transition {
    #[default]
    Idle,
    /// Every plugin.
    Runtime,
    /// One plugin; `targets` are its providers and mixers.
    Plugin {
        plugin: PluginId,
        targets: Arc<HashSet<String>>,
    },
}

/// A wait for a transition ended before the transition did.
#[derive(Debug)]
pub(crate) struct TransitionNotSettled;

impl Transition {
    fn covers(&self, target: &str) -> bool {
        match self {
            Self::Idle => false,
            Self::Runtime => true,
            Self::Plugin { targets, .. } => targets.contains(target),
        }
    }

    fn covers_plugin(&self, plugin_id: &PluginId) -> bool {
        match self {
            Self::Idle => false,
            Self::Runtime => true,
            Self::Plugin { plugin, .. } => plugin == plugin_id,
        }
    }

    fn holds_up_nested(&self, admitted: &[PluginId], target: &str) -> bool {
        match self {
            Self::Plugin { plugin, targets } => {
                !admitted.contains(plugin) && targets.contains(target)
            }
            Self::Idle | Self::Runtime => false,
        }
    }
}

/// Settles the transition however its owner ends.
pub(crate) struct TransitionGuard {
    generation: Arc<GenerationState>,
}

impl Drop for TransitionGuard {
    fn drop(&mut self) {
        self.generation.transition.send_replace(Transition::Idle);
    }
}

/// Per-generation state owned by layers above services, keyed by type. Each
/// layer reads its own type through [`LayerStates::get`]; services never name
/// it. Living inside [`GenerationState`] puts every layer's generation under
/// the one reset.
#[derive(Default)]
pub(crate) struct LayerStates {
    slots: StdMutex<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

impl LayerStates {
    /// This generation's `T`, created on first access. `T::default` runs
    /// under the slot lock, so it must not read [`STATE`].
    pub(crate) fn get<T: Default + Send + Sync + 'static>(&self) -> Arc<T> {
        let slot = Arc::clone(
            self.slots
                .lock()
                .expect("layer states poisoned")
                .entry(TypeId::of::<T>())
                .or_insert_with(|| Arc::new(T::default())),
        );
        slot.downcast::<T>()
            .unwrap_or_else(|_| unreachable!("layer state slots are keyed by their type"))
    }
}

pub(crate) struct AppState {
    pub(crate) db: DbHandle,
    /// What the server was started with: port, directories, and build.
    pub(crate) boot: BootHandle,
    /// The current resolution: config, per-setting provenance, and the file
    /// layer it was resolved against. Read the config through
    /// [`AppState::config`].
    pub(crate) settings: SettingsHandle,
    /// The policy used by restart-required consumers for this process.
    pub(crate) startup_config: SwapHandle<Arc<Config>>,
    /// The values this process started with; restart-required settings are
    /// compared against them.
    pub(crate) startup_settings: SwapHandle<Arc<[EffectiveSetting]>>,
    /// Signalled by every [`AppState::publish_settings`] so long-lived loops
    /// re-read the config.
    pub(crate) settings_changed: tokio::sync::Notify,
    generation: SwapHandle<Arc<GenerationState>>,
}

fn build_app_state(boot: Boot, created: lyra_db::Created, resolved: ResolvedSettings) -> AppState {
    AppState {
        db: DbHandle::new(created),
        boot: BootHandle::new(Arc::new(boot)),
        startup_config: SwapHandle::new(Arc::clone(&resolved.config)),
        startup_settings: SwapHandle::new(Arc::from(resolved.effective.clone())),
        settings: SettingsHandle::new(Arc::new(resolved)),
        settings_changed: tokio::sync::Notify::new(),
        generation: SwapHandle::default(),
    }
}

impl AppState {
    /// Snapshot of the current generation. Callers that must not observe a
    /// mid-operation reset should hold one snapshot for the whole operation.
    pub(crate) fn generation(&self) -> Arc<GenerationState> {
        self.generation.get()
    }

    /// `created` was opened while the live DB still held its process lock,
    /// so it must not share a lockfile with the live one (memory DBs, or a
    /// different path). The generation, with every layer's state in it,
    /// is swapped before the database so the new database never meets the
    /// old generation's plugin runtime.
    fn reset(&self, boot: Boot, created: lyra_db::Created, resolved: ResolvedSettings) {
        self.generation.replace(Arc::default());
        self.db.reset(created);
        self.boot.replace(Arc::new(boot));
        self.startup_config.replace(Arc::clone(&resolved.config));
        self.startup_settings
            .replace(Arc::from(resolved.effective.clone()));
        self.publish_settings(Arc::new(resolved));
    }

    /// The runtime config of the current resolution.
    pub(crate) fn config(&self) -> Arc<Config> {
        Arc::clone(&self.settings.get().config)
    }

    /// Publishes a re-resolution and wakes anything waiting on a change.
    pub(crate) fn publish_settings(&self, resolved: Arc<ResolvedSettings>) {
        self.settings.replace(resolved);
        self.settings_changed.notify_one();
    }
}

/// Reads stored server settings and resolves the runtime config against the
/// freshly opened, not yet shared database.
pub(crate) fn resolve_settings(
    created: &mut lyra_db::Created,
    data_dir: &Path,
    library: Option<LibraryConfig>,
    file_settings: FileSettings,
) -> Result<ResolvedSettings> {
    let db = created
        .unshared_mut()
        .ok_or_else(|| anyhow::anyhow!("database was shared before settings were resolved"))?;
    let stored = server_settings::load_stored(db)?;
    server_settings::resolve(data_dir, library, file_settings, &stored)
}

/// Application state slot with explicit initialization. Entry points that
/// need state (`run_server`, the test harness) must call
/// [`StateCell::initialize`] before anything dereferences `STATE`; access
/// before that is a bug and panics.
pub(crate) struct StateCell {
    inner: OnceLock<AppState>,
}

impl StateCell {
    /// Publishes an already-open database with its boot config and resolved
    /// settings.
    /// First call builds the state; later calls swap in a fresh
    /// DB/config/generation via [`AppState::reset`] (test-harness reuse), so
    /// a later `created` must not share a lockfile with the live DB or
    /// `create` would have blocked on it. Both paths end on a clean
    /// generation. Serialize calls externally (today `RUNTIME_TEST_LOCK`).
    pub(crate) fn initialize(
        &self,
        boot: Boot,
        created: lyra_db::Created,
        resolved: ResolvedSettings,
    ) -> Result<()> {
        match self.inner.get() {
            Some(state) => {
                state.reset(boot, created, resolved);
                Ok(())
            }
            None => self
                .inner
                .set(build_app_state(boot, created, resolved))
                .map_err(|_| anyhow::anyhow!("application state initialized concurrently")),
        }
    }
}

impl StateCell {
    /// The state, or `None` before it is initialized.
    pub(crate) fn get(&self) -> Option<&AppState> {
        self.inner.get()
    }
}

impl std::ops::Deref for StateCell {
    type Target = AppState;

    fn deref(&self) -> &AppState {
        self.inner
            .get()
            .expect("application state accessed before STATE.initialize(config)")
    }
}

pub(crate) static STATE: StateCell = StateCell {
    inner: OnceLock::new(),
};
