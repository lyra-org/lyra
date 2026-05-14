// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Lyra server application crate.
#![cfg_attr(test, feature(test))]

use std::sync::{
    Arc,
    LazyLock,
    Mutex as StdMutex,
    OnceLock,
    RwLock as StdRwLock,
};

use agdb::DbAny;
use anyhow::{
    Context,
    Result,
};
use harmony_core::{
    Harmony,
    PluginManifest,
};
use mlua::Lua;
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

#[derive(Clone)]
pub(crate) struct SwapHandle<T: Clone> {
    inner: Arc<StdRwLock<T>>,
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

pub(crate) type LuaHandle = SwapHandle<Arc<Lua>>;
pub(crate) type ConfigHandle = SwapHandle<Arc<Config>>;
pub(crate) type PluginManifestHandle = SwapHandle<Arc<[PluginManifest]>>;
pub(crate) type PluginRuntimeHandle = SwapHandle<Option<Arc<Harmony>>>;

pub(crate) struct AppState {
    pub(crate) db: DbHandle,
    pub(crate) lua: LuaHandle,
    pub(crate) config: ConfigHandle,
    pub(crate) plugin_manifests: PluginManifestHandle,
    pub(crate) plugin_runtime: PluginRuntimeHandle,
    pub(crate) plugin_registries: PluginRegistries,
}

fn new_lua() -> Result<Arc<Lua>> {
    let lua = Lua::new();
    let package_table = lua.create_table()?;
    lua.globals().set("package", package_table)?;
    harmony_core::set_caller_resolver(&lua, crate::plugins::globals::caller_resolver());
    crate::plugins::caller::install_context_propagator(&lua);
    Ok(lua.into())
}

pub(crate) fn build_app_state(config: Config) -> Result<AppState> {
    let created = create(&config.db)?;
    let lua = new_lua()?;
    Ok(AppState {
        db: DbHandle::new(created),
        lua: LuaHandle::new(lua),
        config: ConfigHandle::new(Arc::new(config)),
        plugin_manifests: PluginManifestHandle::new(Arc::from(Vec::<PluginManifest>::new())),
        plugin_runtime: PluginRuntimeHandle::new(None),
        plugin_registries: PluginRegistries::new(),
    })
}

impl AppState {
    pub(crate) fn reset(&self, config: Config) -> Result<()> {
        // Keep `?` before the remaining replacements so a DB reset failure
        // cannot leave Lua/config/plugin state pointed at the wrong database.
        self.db.reset_with(|| create(&config.db))?;
        let lua = new_lua()?;
        self.lua.replace(lua);
        self.config.replace(Arc::new(config));
        self.plugin_manifests
            .replace(Arc::from(Vec::<PluginManifest>::new()));
        self.plugin_runtime.replace(None);
        Ok(())
    }
}

fn default_app_state() -> AppState {
    let config = match INITIAL_CONFIG.get().cloned() {
        Some(config) => config,
        None => match load_config() {
            Ok(config) => config,
            Err(_) if cfg!(test) => Config::default(),
            Err(err) => panic!("failed to load config: {err}"),
        },
    };
    build_app_state(config).unwrap_or_else(|err| {
        panic!("failed to initialize application state: {err}");
    })
}

static INITIAL_CONFIG: OnceLock<Config> = OnceLock::new();
pub(crate) static STATE: LazyLock<AppState> = LazyLock::new(default_app_state);

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
    let port = config.port;
    let listener = services::startup::bind_configured_listener(port).await?;
    let _ = INITIAL_CONFIG.set(config);
    services::startup::run_server(capture_path, listener).await
}

pub fn run_docs_command(args: &[String]) -> Result<()> {
    plugins::docs::run_command(args)
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
