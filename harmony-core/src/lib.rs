// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashSet;
use std::path::PathBuf;
use std::{
    fs,
    path::Path,
    sync::{
        Arc,
        RwLock,
    },
};

use anyhow::{
    Context,
    Result,
};
use mlua::{
    Lua,
    Table,
};

mod r#async;
mod luaurc;
mod plugin;
mod require;

pub use r#async::{
    LuaAsyncExt,
    LuaFunctionAsyncExt,
    LuaUserDataAsyncExt,
    cancel_thread,
    ensure_scheduler,
    run_function_async,
    run_thread,
};
pub use luaurc::LuaurcConfig;
pub use plugin::{
    LoadedPlugin,
    PluginLoadError,
    PluginManager,
    PluginManifest,
};
pub use require::CapabilityDenied;

type ModuleSetup = dyn Fn(&Lua) -> Result<Table> + Send + Sync + 'static;
type GlobalInstall = dyn Fn(&Lua) -> Result<()> + Send + Sync + 'static;

/// Risk tier surfaced to the plugin-install UI alongside each scope's
/// description. Advisory — does not change enforcement behavior.
/// `Negligible` covers pure helpers (JSON parsing, id conversion);
/// the install UI can collapse these into a utility summary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Danger {
    Negligible,
    Low,
    Medium,
    High,
}

/// Capability declaration. Every `Module` has one — a plugin lists
/// the `id` in its manifest `scopes` field; the runtime gate denies
/// any `require` of a module whose scope the plugin has not declared.
#[derive(Clone, Debug)]
pub struct Scope {
    /// Canonical dotted id (e.g. `"lyra.datastore"`). Matched against
    /// plugin manifest entries verbatim.
    pub id: Arc<str>,
    /// Consent-framed description shown in the install prompt.
    pub description: &'static str,
    pub danger: Danger,
}

#[derive(Clone)]
pub struct Module {
    pub path: Arc<str>,
    pub setup: Arc<ModuleSetup>,
    /// Every module is gated. A plugin must list `scope.id` in its
    /// manifest to `require` this module.
    pub scope: Scope,
}

#[derive(Clone)]
pub struct Global {
    pub name: Arc<str>,
    pub install: Arc<GlobalInstall>,
}

pub trait CallerResolver: Send + Sync + 'static {
    fn resolve(&self, lua: &Lua) -> Option<Arc<str>>;
}

struct CallerResolverSlot(Arc<dyn CallerResolver>);
struct CallerResolverWarned;

pub fn set_caller_resolver(lua: &Lua, resolver: Arc<dyn CallerResolver>) {
    lua.set_app_data(CallerResolverSlot(resolver));
}

pub fn resolve_caller(lua: &Lua) -> Option<Arc<str>> {
    if let Some(slot) = lua.app_data_ref::<CallerResolverSlot>() {
        return slot.0.resolve(lua);
    }
    let already_warned = lua.app_data_ref::<CallerResolverWarned>().is_some();
    if !already_warned {
        lua.set_app_data(CallerResolverWarned);
        // Rust tracing::warn!, not Luau warn — Luau warn calls resolve_caller, recurses.
        tracing::warn!(
            "harmony_core::resolve_caller called but no CallerResolver registered; \
             attribution will be None"
        );
    }
    None
}

/// Removes `loadstring` / `load` / `loadbuffer` from `_G`. They accept a
/// caller-supplied chunkname that flows into `info.source`, which every
/// `plugin_id` attribution path trusts — plugins could forge identity. `require`
/// is server-controlled and sufficient.
pub fn strip_unsafe_dynamic_load(lua: &Lua) -> mlua::Result<()> {
    let globals = lua.globals();
    globals.set("loadstring", mlua::Value::Nil)?;
    globals.set("load", mlua::Value::Nil)?;
    globals.set("loadbuffer", mlua::Value::Nil)?;
    Ok(())
}

/// Every attribution path parses this prefix; ad-hoc strings break attribution silently.
pub fn format_plugin_chunk_name(plugin_id: &str, suffix: &str) -> String {
    format!("plugins/{plugin_id}/{suffix}")
}

/// Anchored at start — `evil/plugins/victim/x` is not misread as `victim`.
pub fn parse_plugin_id(source: &str) -> Option<&str> {
    let trimmed = source.strip_prefix('@').unwrap_or(source);
    let tail = trimmed.strip_prefix("plugins/")?;
    let id = tail.split('/').next()?;
    if id.is_empty() { None } else { Some(id) }
}

pub struct Harmony {
    lua: Arc<Lua>,
    plugin_manager: Arc<RwLock<PluginManager>>,
    valid_scope_ids: HashSet<Arc<str>>,
    require_cache: require::RequireCache,
}

impl Harmony {
    pub fn new<P: AsRef<Path>>(
        lua: Arc<Lua>,
        path: P,
        modules: Arc<[Module]>,
        globals: Arc<[Global]>,
        resolver: Option<Arc<dyn CallerResolver>>,
        plugins_dir: Option<PathBuf>,
    ) -> Result<Self> {
        r#async::ensure_scheduler(lua.as_ref())?;

        let globals_table = lua.globals();

        let package: Table = if let Ok(table) = globals_table.get("package") {
            table
        } else {
            // Create package table if it doesn't exist
            let table = lua.create_table()?;
            globals_table.set("package", table.clone())?;
            table
        };

        package.set("path", format!("{}/?.luau", path.as_ref().display()))?;

        // Collect the set of scope ids the workspace actually registers.
        // Plugin manifests are validated against this — unknown scopes
        // fail load rather than hit a confusing denial at first require.
        let valid_scope_ids: HashSet<Arc<str>> = modules
            .iter()
            .map(|module| module.scope.id.clone())
            .collect();

        // Initialize plugin manager
        let plugins_dir = plugins_dir.unwrap_or_else(|| PathBuf::from("plugins"));
        let mut plugin_manager = PluginManager::new(plugins_dir);

        match plugin_manager.discover_plugins(&valid_scope_ids) {
            Ok(errors) => {
                for error in errors {
                    tracing::warn!("plugin load error: {}", error);
                }
            }
            Err(e) => {
                tracing::error!("failed to scan plugins directory: {}", e);
            }
        }

        let plugin_manager = Arc::new(RwLock::new(plugin_manager));

        if let Some(resolver) = resolver {
            lua.set_app_data(CallerResolverSlot(resolver));
        }

        for global in globals.iter() {
            (global.install)(lua.as_ref())?;
        }

        let require_cache = require::setup(&lua, &modules, plugin_manager.clone())?;

        strip_unsafe_dynamic_load(lua.as_ref())?;

        // After all globals mutations; plugin chunks load under this sandbox.
        lua.sandbox(true)?;

        Ok(Self {
            lua,
            plugin_manager,
            valid_scope_ids,
            require_cache,
        })
    }

    pub fn plugin_manifests(&self) -> Vec<PluginManifest> {
        let plugin_manager = self
            .plugin_manager
            .read()
            .expect("plugin manager lock poisoned while listing manifests");
        let mut manifests = plugin_manager
            .list_plugins()
            .map(|plugin| plugin.manifest.clone())
            .collect::<Vec<_>>();
        manifests.sort_by(|left, right| left.id.cmp(&right.id));
        manifests
    }

    pub fn has_plugin(&self, plugin_id: &str) -> bool {
        self.plugin_manager
            .read()
            .expect("plugin manager lock poisoned while checking plugin")
            .get_plugin(plugin_id)
            .is_some()
    }

    pub fn reload_plugin_manifest(&self, plugin_id: &str) -> Result<()> {
        self.plugin_manager
            .write()
            .expect("plugin manager lock poisoned while reloading plugin")
            .reload_plugin(plugin_id, &self.valid_scope_ids)
            .map_err(anyhow::Error::new)
    }

    fn load_plugin_entrypoint(&self, plugin: &LoadedPlugin) -> Result<mlua::Function> {
        let contents = fs::read_to_string(&plugin.entrypoint_path)?;
        let chunk_name = format_plugin_chunk_name(&plugin.manifest.id, "init");

        Ok(self
            .lua
            .load(&contents)
            .set_name(&chunk_name)
            .into_function()?)
    }

    fn invalidate_plugin_require_cache(&self, plugin: &LoadedPlugin) {
        self.require_cache.invalidate_plugin_root(&plugin.directory);
        self.require_cache
            .invalidate_plugin_root(&PathBuf::from("plugins").join(&plugin.manifest.id));
    }

    pub async fn exec_plugin(&self, plugin_id: &str) -> Result<()> {
        let plugin = {
            let plugin_manager = self
                .plugin_manager
                .read()
                .expect("plugin manager lock poisoned while resolving plugin");
            plugin_manager
                .get_plugin(plugin_id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("plugin not found: {plugin_id}"))?
        };

        self.invalidate_plugin_require_cache(&plugin);
        let function = self
            .load_plugin_entrypoint(&plugin)
            .with_context(|| format!("load plugin '{}' entrypoint", plugin.manifest.id))?;
        r#async::run_function_async::<()>(self.lua.as_ref(), &function, ()).await?;
        tracing::info!("plugin '{}' executed", plugin.manifest.id);
        Ok(())
    }

    pub async fn exec_all(&self) -> Result<()> {
        // Execute all plugin entrypoints
        let plugins = {
            let plugin_manager = self
                .plugin_manager
                .read()
                .expect("plugin manager lock poisoned while listing plugins");
            plugin_manager.list_plugins().cloned().collect::<Vec<_>>()
        };
        for plugin in plugins {
            self.invalidate_plugin_require_cache(&plugin);
            let function = self
                .load_plugin_entrypoint(&plugin)
                .with_context(|| format!("load plugin '{}' entrypoint", plugin.manifest.id))?;

            match r#async::run_function_async::<()>(self.lua.as_ref(), &function, ()).await {
                Ok(_) => tracing::info!("plugin '{}' executed", plugin.manifest.id),
                Err(e) => tracing::error!("plugin '{}' error: {}", plugin.manifest.id, e),
            }
        }

        Ok(())
    }

    pub async fn exec(self) -> Result<()> {
        self.exec_all().await
    }
}
