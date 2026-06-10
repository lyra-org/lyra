// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Lyra server application crate.
#![cfg_attr(all(test, feature = "nightly"), feature(test))]

use std::{
    collections::HashMap,
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
use anyhow::{
    Context,
    Result,
};
use harmony_core::plugin::PluginManifest;
use tokio::sync::{
    OwnedRwLockReadGuard,
    OwnedRwLockWriteGuard,
};

mod config;
mod db;
mod locale;
mod plugins;
mod routes;
mod services;
pub mod testing;

use config::{
    Config,
    load_config,
};
pub(crate) use db::Library;
use db::{
    DbAsync,
    create,
};
use plugins::lifecycle::PluginRegistries;
use plugins::settings::SettingsRegistries;
use services::auth::media_tokens::MediaTokenStore;
use services::hls::cleanup::HlsCleanupState;
use services::libraries::LibrarySyncRegistries;
use services::mix::MixRegistries;
use services::playback_sessions::{
    PlaybackScopes,
    PlaybackUpdateRegistries,
};
use services::providers::ProviderRegistries;

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
    lock: Arc<StdMutex<Option<db::process_lock::DbProcessLock>>>,
}

impl DbHandle {
    fn new(created: db::Created) -> Self {
        Self {
            db: SwapHandle::new(created.db),
            lock: Arc::new(StdMutex::new(created.lock)),
        }
    }

    pub(crate) fn get(&self) -> DbAsync {
        self.db.get()
    }

    fn reset_with<F>(&self, factory: F) -> Result<()>
    where
        F: FnOnce() -> Result<db::Created>,
    {
        let old_lock = self.lock.lock().expect("db lock poisoned").take();
        drop(old_lock);

        let created = factory()?;
        self.db.replace(created.db);
        *self.lock.lock().expect("db lock poisoned") = created.lock;
        Ok(())
    }

    pub(crate) async fn read(&self) -> OwnedRwLockReadGuard<DbAny> {
        self.get().read_owned().await
    }

    pub(crate) async fn write(&self) -> OwnedRwLockWriteGuard<DbAny> {
        self.get().write_owned().await
    }
}

pub(crate) type ConfigHandle = SwapHandle<Arc<Config>>;
pub(crate) type PluginManifestHandle = SwapHandle<Arc<[PluginManifest]>>;
pub(crate) type PluginRuntimeHandle = SwapHandle<Option<crate::plugins::bootstrap::PluginRuntime>>;

/// Auth bookkeeping derived from the current database. Lives on
/// [`GenerationState`] instead of module statics so a DB swap cannot carry
/// ids or grants into the next database (memory DBs reuse small ids).
#[derive(Default)]
pub(crate) struct AuthCaches {
    pub(crate) api_key_last_used: StdMutex<HashMap<DbId, i64>>,
    pub(crate) session_last_seen: StdMutex<HashMap<DbId, i64>>,
    pub(crate) media_tokens: MediaTokenStore,
}

/// Everything derived from the current database/config generation. A reset
/// swaps the whole struct for a fresh [`Default`], so generation-bound state
/// dies with its generation by construction — there is no per-registry reset
/// list to keep in sync. New DB- or config-derived registries belong here,
/// never in module statics.
#[derive(Default)]
pub(crate) struct GenerationState {
    pub(crate) plugin_manifests: PluginManifestHandle,
    pub(crate) plugin_runtime: PluginRuntimeHandle,
    pub(crate) plugin_registries: PluginRegistries,
    pub(crate) auth_caches: AuthCaches,
    pub(crate) plugin_settings: SettingsRegistries,
    pub(crate) providers: ProviderRegistries,
    pub(crate) mix: MixRegistries,
    pub(crate) playback_updates: PlaybackUpdateRegistries,
    pub(crate) playback_scopes: PlaybackScopes,
    pub(crate) library_sync: LibrarySyncRegistries,
    pub(crate) hls_cleanup: HlsCleanupState,
}

pub(crate) struct AppState {
    pub(crate) db: DbHandle,
    pub(crate) config: ConfigHandle,
    generation: SwapHandle<Arc<GenerationState>>,
}

pub(crate) fn build_app_state(config: Config) -> Result<AppState> {
    let created = create(&config.db)?;
    Ok(AppState {
        db: DbHandle::new(created),
        config: ConfigHandle::new(Arc::new(config)),
        generation: SwapHandle::default(),
    })
}

impl AppState {
    /// Snapshot of the current generation. Callers that must not observe a
    /// mid-operation reset should hold one snapshot for the whole operation.
    pub(crate) fn generation(&self) -> Arc<GenerationState> {
        self.generation.get()
    }

    pub(crate) fn reset(&self, config: Config) -> Result<()> {
        // Keep `?` before the remaining replacements so a DB reset failure
        // cannot leave Lua/config/plugin state pointed at the wrong database.
        self.db.reset_with(|| create(&config.db))?;
        self.config.replace(Arc::new(config));
        self.generation.replace(Arc::default());
        Ok(())
    }
}

/// Application state slot with explicit initialization. Entry points that
/// need state (`run_server`, `testing::initialize_runtime`) must call
/// [`StateCell::initialize`] before anything dereferences `STATE`; access
/// before that is a bug and panics.
pub(crate) struct StateCell {
    inner: OnceLock<AppState>,
}

impl StateCell {
    /// First call builds the state from `config`; later calls swap in a fresh
    /// DB/config/generation via [`AppState::reset`] (test-harness reuse).
    /// Both paths end on a clean generation. Serialize calls externally
    /// (today `RUNTIME_TEST_LOCK`).
    pub(crate) fn initialize(&self, config: Config) -> Result<()> {
        match self.inner.get() {
            Some(state) => state.reset(config),
            None => {
                let state = build_app_state(config)?;
                self.inner
                    .set(state)
                    .map_err(|_| anyhow::anyhow!("application state initialized concurrently"))
            }
        }
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

pub fn outbound_user_agent() -> String {
    let version = env!("CARGO_PKG_VERSION");
    let short_hash = env!("LYRA_GIT_HASH");
    if short_hash.is_empty() {
        format!("Lyra/{version} (blue@spook.rip)")
    } else {
        format!("Lyra/{version}-{short_hash} (blue@spook.rip)")
    }
}

pub async fn run_server(capture_path: Option<String>) -> Result<()> {
    let config = load_config()?;
    // Bind first: a port collision with a running server must fail fast
    // instead of blocking on that server's DB process lock.
    let listener = services::startup::bind_configured_listener(config.port).await?;
    STATE.initialize(config)?;
    services::startup::run_server(capture_path, listener).await
}

#[cfg(feature = "docgen")]
pub fn run_docs_command(args: &[String]) -> Result<()> {
    plugins::docs::run_command(args)
}

/// Installs plugins from a Git repository into the plugins directory.
/// Disk-only: the next server start (or a runtime reload through the API)
/// loads them, so this works whether or not the server is running.
pub async fn run_plugins_add(url: &str, git_ref: Option<&str>) -> Result<()> {
    let report = services::plugin_repositories::install_to_disk(url, git_ref, None)
        .await
        .map_err(|err| anyhow::anyhow!("{err}"))?;

    for plugin in &report.installed {
        let commit = plugin
            .commit
            .as_deref()
            .map(|commit| format!(" ({commit})"))
            .unwrap_or_default();
        println!("installed {} v{}{commit}", plugin.id, plugin.version);
    }
    for failure in &report.failed {
        eprintln!("failed to install {}: {}", failure.id, failure.error);
    }
    if !report.failed.is_empty() {
        anyhow::bail!("{} plugin(s) failed to install", report.failed.len());
    }

    println!("restart the server (or reload plugins through the API) to load them");
    Ok(())
}

/// Force-compact the DB from the CLI. Reserves the configured port first, opens
/// in `DbFile` regardless of `config.kind`, and skips schema init.
pub async fn run_db_optimize() -> Result<()> {
    let config = load_config()?;
    if matches!(config.db.kind, config::DbKind::Memory) {
        anyhow::bail!(
            "nothing to optimize: db kind is memory; configure DbKind::File or DbKind::Mmap to use this command"
        );
    }

    let _lock_guard =
        db::process_lock::acquire(&config.db, db::process_lock::LockMode::NonBlocking)?;
    let db_path = config.db.path.clone();

    // After the open: WAL recovery may have grown the file before optimize runs.
    let mut db = db::bootstrap::open(config::DbKind::File, db_path.to_string_lossy().as_ref())?;
    let before_logical = db.size();
    let before_file = std::fs::metadata(&db_path)
        .with_context(|| {
            format!(
                "failed to read db metadata at {} after open",
                db_path.display()
            )
        })?
        .len();

    db.optimize_storage()
        .map_err(|err| anyhow::anyhow!("optimize_storage failed: {err}"))?;
    let after_logical = db.size();
    drop(db);
    let after_file = std::fs::metadata(&db_path)
        .with_context(|| {
            format!(
                "failed to read db metadata at {} after optimize",
                db_path.display()
            )
        })?
        .len();

    let reclaimed = before_file.saturating_sub(after_file);
    eprintln!("optimize_storage complete:");
    eprintln!("  logical bytes: {before_logical} -> {after_logical}");
    eprintln!("  file bytes:    {before_file} -> {after_file} (reclaimed {reclaimed})");

    Ok(())
}
