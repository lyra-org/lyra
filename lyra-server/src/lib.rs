// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Lyra server application crate.
#![cfg_attr(test, feature(test))]

use std::sync::{
    Arc,
    LazyLock,
    RwLock as StdRwLock,
};

use agdb::DbAny;
use anyhow::Result;
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

#[derive(Clone)]
pub(crate) struct DbHandle(SwapHandle<DbAsync>);

impl DbHandle {
    fn new(db: DbAsync) -> Self {
        Self(SwapHandle::new(db))
    }

    pub(crate) fn get(&self) -> DbAsync {
        self.0.get()
    }

    fn replace(&self, db: DbAsync) {
        self.0.replace(db);
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
    Ok(lua.into())
}

pub(crate) fn build_app_state(config: Config) -> Result<AppState> {
    let db = create(&config.db)?;
    let lua = new_lua()?;
    Ok(AppState {
        db: DbHandle::new(db),
        lua: LuaHandle::new(lua),
        config: ConfigHandle::new(Arc::new(config)),
        plugin_manifests: PluginManifestHandle::new(Arc::from(Vec::<PluginManifest>::new())),
        plugin_runtime: PluginRuntimeHandle::new(None),
        plugin_registries: PluginRegistries::new(),
    })
}

impl AppState {
    pub(crate) fn reset(&self, config: Config) -> Result<()> {
        let db = create(&config.db)?;
        let lua = new_lua()?;
        self.db.replace(db);
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
