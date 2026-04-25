// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;
use std::fmt;
use std::sync::{
    Arc,
    RwLock,
};

use mlua::{
    Error,
    Function,
    Lua,
    Value,
};

use crate::{
    Module,
    PluginManager,
};

mod engine;
pub(crate) use engine::RequireCache;

#[cfg(test)]
mod tests;

/// Runtime denial: plugin `require`s a gated module whose scope the
/// plugin has not declared in its manifest. Propagates through
/// `Error::external` so callers can `pcall` + downcast via
/// `Error::downcast_ref::<CapabilityDenied>()`.
#[derive(Debug, Clone)]
pub struct CapabilityDenied {
    pub plugin_id: String,
    pub module_path: String,
    pub scope_id: String,
}

impl fmt::Display for CapabilityDenied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "plugin '{}' required '@{}' without declaring scope '{}'",
            self.plugin_id, self.module_path, self.scope_id
        )
    }
}

impl std::error::Error for CapabilityDenied {}

pub(crate) fn setup(
    lua: &Lua,
    modules: &Arc<[Module]>,
    plugin_manager: Arc<RwLock<PluginManager>>,
) -> anyhow::Result<RequireCache, Error> {
    for module in modules.iter() {
        let exports = (module.setup)(lua).map_err(Error::external)?;
        lua.register_module(&format!("@{}", module.path), exports)?;
    }

    // Alias path → its declared scope. Lookup table for the wrapper
    // below; rationale for wrapping at all is on `build_require_wrapper`.
    let gated_modules: Arc<HashMap<String, Arc<crate::Scope>>> = Arc::new(
        modules
            .iter()
            .map(|module| (module.path.to_string(), Arc::new(module.scope.clone())))
            .collect(),
    );

    let require_cache = RequireCache::new();
    let inner =
        lua.create_require_function(engine::ModuleRequirer::new(modules, require_cache.clone()))?;
    let wrapper = build_require_wrapper(lua, inner, gated_modules, plugin_manager)?;
    lua.globals().set("require", wrapper)?;
    Ok(require_cache)
}

/// Build the scope-checking wrapper. Fires per-call, before the inner
/// `require` consults any cache, so a plugin that pre-warmed a gated
/// module cannot mask another plugin's denial.
fn build_require_wrapper(
    lua: &Lua,
    inner: Function,
    gated_modules: Arc<HashMap<String, Arc<crate::Scope>>>,
    plugin_manager: Arc<RwLock<PluginManager>>,
) -> mlua::Result<Function> {
    lua.create_function(move |lua, target: Value| -> mlua::Result<Value> {
        let Value::String(target_str) = &target else {
            // Non-string argument — let the inner require produce its
            // own type error rather than second-guessing it here.
            return inner.call::<Value>(target.clone());
        };
        let target_bytes = target_str.as_bytes();
        let Ok(target_slice) = std::str::from_utf8(&target_bytes) else {
            return inner.call::<Value>(target.clone());
        };

        // Only alias-prefixed targets (`@<alias>/<path>`) can hit
        // a registered Rust module. Relative (`./helper`) and
        // plain (`helper`) paths are plugin-internal Luau files.
        let Some(module_path) = target_slice.strip_prefix('@').and_then(|rest| {
            gated_modules
                .get(rest)
                .map(|scope| (rest.to_string(), scope.clone()))
        }) else {
            return inner.call::<Value>(target.clone());
        };
        let (module_path, scope) = module_path;

        let (plugin_id, allowed) = {
            let plugin_manager = plugin_manager
                .read()
                .map_err(|_| Error::runtime("plugin manager lock poisoned"))?;
            let plugin_id = resolve_requiring_plugin_id(lua, &plugin_manager);

            let declared = plugin_id
                .as_ref()
                .and_then(|id| plugin_manager.get_plugin(id))
                .map(|plugin| &plugin.declared_scopes);

            let allowed = declared
                .map(|scopes| scopes.contains(&scope.id))
                .unwrap_or(false);
            (plugin_id, allowed)
        };

        if !allowed {
            return Err(Error::external(CapabilityDenied {
                plugin_id: plugin_id
                    .as_deref()
                    .unwrap_or("<non-plugin caller>")
                    .to_string(),
                module_path,
                scope_id: scope.id.to_string(),
            }));
        }

        inner.call::<Value>(target.clone())
    })
}

/// Walk the Lua call stack and return the id of the first frame whose
/// chunk name resolves to a `PluginManager`-registered plugin. A caller
/// that names itself `plugins/<fake>/init` returns `None` if `<fake>`
/// isn't installed — closes the chunk-name forgery oracle.
///
/// Callers must map `None` to `CapabilityDenied` for gated modules per
/// the Global/Root fail-closed policy.
fn resolve_requiring_plugin_id(lua: &Lua, plugin_manager: &PluginManager) -> Option<String> {
    let mut level = 1usize;
    while let Some(function) = lua.inspect_stack(level, |debug| debug.function()) {
        let info = function.info();
        let source = info.source.as_deref().or(info.short_src.as_deref());
        if let Some(source) = source
            && let Some(id) = extract_plugin_id_from_source(source)
            && plugin_manager.get_plugin(&id).is_some()
        {
            return Some(id);
        }
        level += 1;
    }
    None
}

fn extract_plugin_id_from_source(source: &str) -> Option<String> {
    let source = source.strip_prefix('@').unwrap_or(source);
    let tail = source.split("plugins/").nth(1)?;
    let id = tail.split('/').next()?;
    if id.is_empty() {
        None
    } else {
        Some(id.to_string())
    }
}
