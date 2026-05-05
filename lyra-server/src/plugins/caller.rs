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
    #[allow(dead_code)]
    pub(crate) plugin_id: Arc<str>,
    pub(crate) principal: Principal,
}

#[derive(Clone, Debug)]
pub(crate) struct SystemCaller {
    #[allow(dead_code)]
    pub(crate) plugin_id: Arc<str>,
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

pub(crate) fn request_caller(plugin_id: Option<Arc<str>>) -> Result<RequestCaller> {
    let plugin_id = require_plugin_id(plugin_id, "request plugin call")?;
    let principal = REQUEST_PRINCIPAL.try_with(Clone::clone).map_err(|_| {
        mlua::Error::runtime(format!("request context required for plugin '{plugin_id}'"))
    })?;
    Ok(RequestCaller {
        plugin_id,
        principal,
    })
}

pub(crate) fn system_caller(plugin_id: Option<Arc<str>>) -> Result<SystemCaller> {
    let plugin_id = require_plugin_id(plugin_id, "system plugin call")?;
    let system_ctx = SYSTEM_CONTEXT.try_with(|ctx| *ctx).map_err(|_| {
        mlua::Error::runtime(format!("system context required for plugin '{plugin_id}'"))
    })?;
    Ok(SystemCaller {
        plugin_id,
        system_ctx,
    })
}

impl harmony_core::RequestModuleContext for RequestCaller {
    fn from_lua_plugin_id(_lua: &mlua::Lua, plugin_id: Option<Arc<str>>) -> Result<Self> {
        request_caller(plugin_id)
    }
}

impl harmony_core::SystemModuleContext for SystemCaller {
    fn from_lua_plugin_id(_lua: &mlua::Lua, plugin_id: Option<Arc<str>>) -> Result<Self> {
        system_caller(plugin_id)
    }
}
