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
    DbProcessLock,
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

/// DB slot and process-lock guard in independent fields so `reset_with` can
/// drop the lock atomically with the slot replacement — bundling them under
/// one `Arc` would let a stray DB clone keep the lock alive. `reset_with` is
/// **not** safe concurrently; serialize externally (today `RUNTIME_TEST_LOCK`).
#[derive(Clone)]
pub(crate) struct DbHandle {
    db: SwapHandle<DbAsync>,
    /// Mutex (not RwLock): every access take/replaces the inner `Option`.
    lock: Arc<StdMutex<Option<DbProcessLock>>>,
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

    /// Drop-then-acquire reload: release the current lock before the factory
    /// acquires a new one (same-process double `flock` deadlocks). On factory
    /// error, rollback per the gate documented on [`Self::release_for_reload`].
    fn reset_with<F>(&self, factory: F) -> Result<()>
    where
        F: FnOnce() -> Result<db::Created>,
    {
        let release = self.release_for_reload()?;

        match factory() {
            Ok(created) => {
                self.db.replace(created.db);
                *self.lock.lock().expect("db lock poisoned") = created.lock;
                drop(release.old_db);
                Ok(())
            }
            Err(err) => {
                if release.old_lock_was_never_real {
                    // Rollback assumes the caller hasn't advanced other state
                    // before observing this Err. `AppState::reset` propagates
                    // with `?` first; a future inversion needs this re-narrowed.
                    self.db.replace(release.old_db);
                } else {
                    drop(release.old_db);
                    tracing::error!(
                        error = %err,
                        "db reset_with: factory failed for path-backed db; old lock has been released, in-memory placeholder remains. Restart server to recover."
                    );
                }
                Err(err)
            }
        }
    }

    /// Take+swap+drop, returning what the caller needs to commit or roll back.
    /// Encapsulates the lock-fd-closes-first ordering so it can't be reordered
    /// by a downstream edit.
    fn release_for_reload(&self) -> Result<ReloadRelease> {
        let old_db = self.db.get();
        let old_lock = self.lock.lock().expect("db lock poisoned").take();
        let old_lock_was_never_real = old_lock.is_none();
        let placeholder = db::bootstrap::placeholder()?;
        self.db.replace(placeholder);
        drop(old_lock);
        Ok(ReloadRelease {
            old_db,
            old_lock_was_never_real,
        })
    }

    pub(crate) async fn read(&self) -> OwnedRwLockReadGuard<DbAny> {
        self.get().read_owned().await
    }

    pub(crate) async fn write(&self) -> OwnedRwLockWriteGuard<DbAny> {
        self.get().write_owned().await
    }
}

/// What `release_for_reload` produces: the previous DB (for rollback or drop)
/// and a flag describing whether the previous slot ever held a real fd-lock.
struct ReloadRelease {
    old_db: DbAsync,
    old_lock_was_never_real: bool,
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
        // `?` here is load-bearing: `reset_with`'s rollback assumes no other
        // state has been advanced. Reorder and narrow the gate in tandem.
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
    let config = match load_config() {
        Ok(config) => config,
        Err(_) if cfg!(test) => Config::default(),
        Err(err) => panic!("failed to load config: {err}"),
    };
    build_app_state(config).unwrap_or_else(|err| {
        panic!("failed to initialize application state: {err}");
    })
}

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
    services::startup::run_server(capture_path).await
}

pub fn run_docs_command(args: &[String]) -> Result<()> {
    plugins::docs::run_command(args)
}

/// Force-compact the DB from the CLI. Fail-fast on the cross-process lock,
/// open in `DbFile` regardless of `config.kind` (mmap may refuse on a ballooned
/// DB), skip schema init.
pub async fn run_db_optimize() -> Result<()> {
    let config = load_config()?;
    if matches!(config.db.kind, config::DbKind::Memory) {
        anyhow::bail!(
            "nothing to optimize: db kind is memory; configure DbKind::File or DbKind::Mmap to use this command"
        );
    }

    let db_path = config.db.path.clone();

    // Lock spans open + metadata + optimize. Full name (not `_lock`) so a
    // future `_` rename can't silently turn "hold for scope" into "drop now."
    let _lock_guard =
        db::process_lock::acquire(&config.db, db::process_lock::LockMode::NonBlocking)?;

    // After the open: WAL recovery may have grown the file before optimize runs.
    let mut db = db::bootstrap::open(config::DbKind::File, db_path.to_string_lossy().as_ref())?;
    let before_logical = db.size();
    // Anomalous after a successful open; bail rather than weaken the guard with zero.
    let before_file = std::fs::metadata(&db_path)
        .with_context(|| {
            format!(
                "failed to read db metadata at {} after open",
                db_path.display()
            )
        })?
        .len();

    // Same disk-full guard as the pre-open path.
    let parent = db_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    db::compact::ensure_space_for_optimize(parent, before_file, before_logical)?;

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
