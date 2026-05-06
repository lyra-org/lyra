use std::{
    future::Future,
    sync::Arc,
};

use mlua::Result;
use tokio::task_local;

use crate::services::{
    SystemContext,
    auth::Principal,
};

#[derive(Clone, Debug)]
pub(crate) struct RequestCaller {
    pub(crate) plugin_id: Arc<str>,
    pub(crate) principal: Principal,
}

#[derive(Clone, Debug)]
pub(crate) struct SystemCaller {
    pub(crate) system_ctx: SystemContext,
}

task_local! {
    static REQUEST_PRINCIPAL: Principal;
    static SYSTEM_CONTEXT: SystemContext;
}

pub(crate) async fn scope_request<T>(principal: Principal, future: impl Future<Output = T>) -> T {
    REQUEST_PRINCIPAL.scope(principal, future).await
}

pub(crate) async fn scope_system<T>(
    system_ctx: SystemContext,
    future: impl Future<Output = T>,
) -> T {
    SYSTEM_CONTEXT.scope(system_ctx, future).await
}

fn require_plugin_id(plugin_id: Option<Arc<str>>, context: &str) -> Result<Arc<str>> {
    plugin_id.ok_or_else(|| mlua::Error::runtime(format!("{context} requires plugin identity")))
}

fn lua_call_site(lua: &mlua::Lua, plugin_id: &str) -> Option<String> {
    let mut fallback = None;
    let expected_segment = format!("plugins/{plugin_id}/");

    for level in 1..=12 {
        let Some(source) = lua
            .inspect_stack(level, |debug| {
                debug.source().source.map(|cow| cow.into_owned())
            })
            .flatten()
        else {
            continue;
        };

        if source.contains(&expected_segment) {
            return Some(source);
        }
        fallback.get_or_insert(source);
    }

    fallback
}

fn context_required_error(context: &str, plugin_id: &str, source: Option<String>) -> mlua::Error {
    match source {
        Some(source) => mlua::Error::runtime(format!(
            "{context} context required for plugin '{plugin_id}' at {source}"
        )),
        None => mlua::Error::runtime(format!(
            "{context} context required for plugin '{plugin_id}'"
        )),
    }
}

pub(crate) fn request_caller(plugin_id: Option<Arc<str>>) -> Result<RequestCaller> {
    let plugin_id = require_plugin_id(plugin_id, "request plugin call")?;
    let principal = REQUEST_PRINCIPAL
        .try_with(Clone::clone)
        .map_err(|_| context_required_error("request", &plugin_id, None))?;
    Ok(RequestCaller {
        plugin_id,
        principal,
    })
}

pub(crate) fn request_caller_at(
    lua: &mlua::Lua,
    plugin_id: Option<Arc<str>>,
) -> Result<RequestCaller> {
    let plugin_id = require_plugin_id(plugin_id, "request plugin call")?;
    let source = lua_call_site(lua, &plugin_id);
    let principal = REQUEST_PRINCIPAL
        .try_with(Clone::clone)
        .map_err(|_| context_required_error("request", &plugin_id, source))?;
    Ok(RequestCaller {
        plugin_id,
        principal,
    })
}

pub(crate) fn system_caller(plugin_id: Option<Arc<str>>) -> Result<SystemCaller> {
    let plugin_id = require_plugin_id(plugin_id, "system plugin call")?;
    let system_ctx = SYSTEM_CONTEXT
        .try_with(|ctx| *ctx)
        .map_err(|_| context_required_error("system", &plugin_id, None))?;
    Ok(SystemCaller { system_ctx })
}

pub(crate) fn system_caller_at(
    lua: &mlua::Lua,
    plugin_id: Option<Arc<str>>,
) -> Result<SystemCaller> {
    let plugin_id = require_plugin_id(plugin_id, "system plugin call")?;
    let source = lua_call_site(lua, &plugin_id);
    let system_ctx = SYSTEM_CONTEXT
        .try_with(|ctx| *ctx)
        .map_err(|_| context_required_error("system", &plugin_id, source))?;
    Ok(SystemCaller { system_ctx })
}

impl harmony_core::ModuleContext for RequestCaller {
    fn from_lua_plugin_id(lua: &mlua::Lua, plugin_id: Option<Arc<str>>) -> Result<Self> {
        request_caller_at(lua, plugin_id)
    }
}

impl harmony_core::ModuleContext for SystemCaller {
    fn from_lua_plugin_id(lua: &mlua::Lua, plugin_id: Option<Arc<str>>) -> Result<Self> {
        system_caller_at(lua, plugin_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_context_error_includes_plugin_source() -> anyhow::Result<()> {
        let lua = mlua::Lua::new();
        let plugin_id = Arc::<str>::from("alpha");
        let call = lua.create_function(move |lua, ()| {
            let err =
                request_caller_at(lua, Some(plugin_id.clone())).expect_err("no request scope");
            Ok(err.to_string())
        })?;
        lua.globals().set("call", call)?;

        let error: String = lua
            .load("return call()")
            .set_name("@plugins/alpha/init.luau")
            .eval()?;

        assert!(error.contains("plugin 'alpha'"), "error was: {error}");
        assert!(
            error.contains("plugins/alpha/init.luau"),
            "error was: {error}"
        );
        Ok(())
    }
}
