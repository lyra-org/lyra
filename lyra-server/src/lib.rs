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
    BootConfig,
    Config,
    LibraryConfig,
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
use services::pagination::SnapshotRegistry;
use services::playback_sessions::{
    PlaybackScopes,
    PlaybackUpdateRegistries,
};
use services::providers::ProviderRegistries;
use services::settings::server::{
    self as server_settings,
    EffectiveSetting,
    ResolvedSettings,
};
pub use services::startup::CaptureArgs;

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

    fn reset(&self, created: db::Created) {
        let old_lock = self.lock.lock().expect("db lock poisoned").take();
        drop(old_lock);

        self.db.replace(created.db);
        *self.lock.lock().expect("db lock poisoned") = created.lock;
    }

    pub(crate) async fn read(&self) -> OwnedRwLockReadGuard<DbAny> {
        self.get().read_owned().await
    }

    pub(crate) async fn write(&self) -> OwnedRwLockWriteGuard<DbAny> {
        self.get().write_owned().await
    }
}

pub(crate) type BootHandle = SwapHandle<Arc<BootConfig>>;
pub(crate) type SettingsHandle = SwapHandle<Arc<ResolvedSettings>>;
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
    pub(crate) pagination: SnapshotRegistry,
    pub(crate) playback_updates: PlaybackUpdateRegistries,
    pub(crate) playback_scopes: PlaybackScopes,
    pub(crate) library_sync: LibrarySyncRegistries,
    pub(crate) hls_cleanup: HlsCleanupState,
}

pub(crate) struct AppState {
    pub(crate) db: DbHandle,
    /// Canonical home for port, data dir, and db location.
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

fn build_app_state(boot: BootConfig, created: db::Created, resolved: ResolvedSettings) -> AppState {
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
    /// different path).
    fn reset(&self, boot: BootConfig, created: db::Created, resolved: ResolvedSettings) {
        self.db.reset(created);
        self.boot.replace(Arc::new(boot));
        self.startup_config.replace(Arc::clone(&resolved.config));
        self.startup_settings
            .replace(Arc::from(resolved.effective.clone()));
        self.publish_settings(Arc::new(resolved));
        self.generation.replace(Arc::default());
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

/// Application state slot with explicit initialization. Entry points that
/// need state (`run_server`, `testing::initialize_runtime`) must call
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
        boot: BootConfig,
        created: db::Created,
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

pub async fn run_server(capture: Option<CaptureArgs>) -> Result<()> {
    let _tracing_guard = services::startup::init_tracing();
    let loaded = config::load()?;
    // Validate the file before touching the port, directories, or database.
    let library = LibraryConfig::resolve(loaded.file.library.as_ref())?;
    let file_settings = server_settings::normalize_file(&loaded.file.settings, &loaded.boot)?;
    // Bind first: a port collision with a running server must fail fast
    // instead of blocking on that server's DB process lock.
    let listener = services::startup::bind_configured_listener(loaded.boot.port).await?;
    loaded.boot.ensure_directories()?;
    let mut created = create(&loaded.boot.db)?;
    let resolved = resolve_settings(&mut created, &loaded.boot, library, file_settings)?;
    STATE.initialize(loaded.boot, created, resolved)?;
    services::startup::run_server(capture, listener).await
}

/// Reads stored server settings and resolves the runtime config against the
/// freshly opened, not yet shared database.
pub(crate) fn resolve_settings(
    created: &mut db::Created,
    boot: &BootConfig,
    library: Option<LibraryConfig>,
    file_settings: config::FileSettings,
) -> Result<ResolvedSettings> {
    let db = Arc::get_mut(&mut created.db)
        .ok_or_else(|| anyhow::anyhow!("database was shared before settings were resolved"))?
        .get_mut();
    let stored = server_settings::load_stored(db)?;
    server_settings::resolve(boot, library, file_settings, &stored)
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

/// A persistent database opened for a CLI subcommand: the process lock is
/// held for as long as the value lives. Opens in `DbFile` regardless of the
/// configured kind and skips schema init.
struct CliDb {
    db: DbAny,
    path: std::path::PathBuf,
    _lock: Option<db::process_lock::DbProcessLock>,
}

/// `command` is the CLI command name (for example `db optimize`) used in
/// diagnostics; `verb` describes the action in "nothing to <verb>" errors.
fn open_cli_db(command: &'static str, verb: &str) -> Result<CliDb> {
    let boot = config::load()?.boot;
    if matches!(boot.db.kind, config::DbKind::Memory) {
        anyhow::bail!(
            "nothing to {verb}: db kind is memory; configure DbKind::File or DbKind::Mmap to use this command"
        );
    }

    let path = boot.db.path.clone();
    if !path.is_file() {
        anyhow::bail!("nothing to {verb}: no database file at {}", path.display());
    }

    let lock = db::process_lock::acquire(
        &boot.db,
        db::process_lock::LockMode::NonBlocking { command },
    )?;
    let db = db::bootstrap::open(config::DbKind::File, path.to_string_lossy().as_ref())?;
    Ok(CliDb {
        db,
        path,
        _lock: lock,
    })
}

/// Force-compact the DB from the CLI.
pub async fn run_db_optimize() -> Result<()> {
    let _tracing_guard = services::startup::init_tracing();
    let CliDb {
        mut db,
        path: db_path,
        _lock,
    } = open_cli_db("db optimize", "optimize")?;
    tracing::info!(path = %db_path.display(), "optimizing db");

    // After the open: WAL recovery may have grown the file before optimize runs.
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

/// Clears every stored server setting so a database whose entries no longer
/// validate can start again.
pub async fn run_settings_reset() -> Result<()> {
    let _tracing_guard = services::startup::init_tracing();
    let mut cli = open_cli_db("settings reset", "reset")?;
    let removed = server_settings::reset_stored(&mut cli.db)?;

    if removed.is_empty() {
        eprintln!("no stored server settings to remove");
    } else {
        eprintln!("removed {} stored server setting(s):", removed.len());
        for key in removed {
            eprintln!("  {key}");
        }
    }
    Ok(())
}
