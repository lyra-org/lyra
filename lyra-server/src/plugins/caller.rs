use std::{
    collections::HashMap,
    future::Future,
    sync::{
        Arc,
        Mutex,
    },
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

#[derive(Clone, Debug)]
enum CallerContext {
    Request(Principal),
    System(SystemContext),
}

#[derive(Default)]
struct CallerContextMap {
    contexts: Mutex<HashMap<usize, CallerContext>>,
}

struct CallerContextPropagator;

task_local! {
    static REQUEST_PRINCIPAL: Principal;
    static SYSTEM_CONTEXT: SystemContext;
}

pub(crate) fn install_context_propagator(lua: &mlua::Lua) {
    lua.set_app_data(CallerContextMap::default());
    harmony_core::set_async_context_propagator(lua, Arc::new(CallerContextPropagator));
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

pub(crate) async fn scope_request_thread<T>(
    lua: &mlua::Lua,
    thread: &mlua::Thread,
    principal: Principal,
    future: impl Future<Output = T>,
) -> T {
    let context = CallerContext::Request(principal.clone());
    let previous = set_thread_context(lua, thread, context);
    let result = scope_request(principal, future).await;
    restore_thread_context(lua, thread, previous);
    result
}

pub(crate) async fn scope_current_thread_context<T>(
    lua: &mlua::Lua,
    thread: &mlua::Thread,
    future: impl Future<Output = T>,
) -> T {
    let Some(context) = task_context() else {
        return future.await;
    };
    let previous = set_thread_context(lua, thread, context.clone());
    let result = match context {
        CallerContext::Request(principal) => scope_request(principal, future).await,
        CallerContext::System(system_ctx) => scope_system(system_ctx, future).await,
    };
    restore_thread_context(lua, thread, previous);
    result
}

fn require_plugin_id(plugin_id: Option<Arc<str>>, context: &str) -> Result<Arc<str>> {
    plugin_id.ok_or_else(|| mlua::Error::runtime(format!("{context} requires plugin identity")))
}

fn thread_key(thread: &mlua::Thread) -> usize {
    thread.to_pointer() as usize
}

fn ensure_context_map(lua: &mlua::Lua) {
    let missing = lua.app_data_ref::<CallerContextMap>().is_none();
    if missing {
        lua.set_app_data(CallerContextMap::default());
    }
}

fn set_thread_context(
    lua: &mlua::Lua,
    thread: &mlua::Thread,
    context: CallerContext,
) -> Option<CallerContext> {
    ensure_context_map(lua);
    let map = lua
        .app_data_ref::<CallerContextMap>()
        .expect("caller context map installed");
    map.contexts
        .lock()
        .expect("caller context map poisoned")
        .insert(thread_key(thread), context)
}

fn restore_thread_context(lua: &mlua::Lua, thread: &mlua::Thread, previous: Option<CallerContext>) {
    let Some(map) = lua.app_data_ref::<CallerContextMap>() else {
        return;
    };
    let mut contexts = map.contexts.lock().expect("caller context map poisoned");
    match previous {
        Some(previous) => {
            contexts.insert(thread_key(thread), previous);
        }
        None => {
            contexts.remove(&thread_key(thread));
        }
    }
}

fn task_context() -> Option<CallerContext> {
    if let Ok(principal) = REQUEST_PRINCIPAL.try_with(Clone::clone) {
        return Some(CallerContext::Request(principal));
    }
    if let Ok(system_ctx) = SYSTEM_CONTEXT.try_with(|ctx| *ctx) {
        return Some(CallerContext::System(system_ctx));
    }
    None
}

fn thread_context(lua: &mlua::Lua) -> Option<CallerContext> {
    let thread = lua.current_thread();
    let key = thread_key(&thread);
    let map = lua.app_data_ref::<CallerContextMap>()?;
    map.contexts
        .lock()
        .expect("caller context map poisoned")
        .get(&key)
        .cloned()
}

fn current_context(lua: &mlua::Lua) -> Option<CallerContext> {
    task_context().or_else(|| thread_context(lua))
}

fn request_principal(lua: Option<&mlua::Lua>) -> Option<Principal> {
    if let Ok(principal) = REQUEST_PRINCIPAL.try_with(Clone::clone) {
        return Some(principal);
    }
    match lua.and_then(thread_context) {
        Some(CallerContext::Request(principal)) => Some(principal),
        _ => None,
    }
}

fn system_context(lua: Option<&mlua::Lua>) -> Option<SystemContext> {
    if let Ok(system_ctx) = SYSTEM_CONTEXT.try_with(|ctx| *ctx) {
        return Some(system_ctx);
    }
    match lua.and_then(thread_context) {
        Some(CallerContext::System(system_ctx)) => Some(system_ctx),
        _ => None,
    }
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
    let principal = request_principal(None)
        .ok_or_else(|| context_required_error("request", &plugin_id, None))?;
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
    let principal = request_principal(Some(lua))
        .ok_or_else(|| context_required_error("request", &plugin_id, source))?;
    Ok(RequestCaller {
        plugin_id,
        principal,
    })
}

pub(crate) fn system_caller(plugin_id: Option<Arc<str>>) -> Result<SystemCaller> {
    let plugin_id = require_plugin_id(plugin_id, "system plugin call")?;
    let system_ctx =
        system_context(None).ok_or_else(|| context_required_error("system", &plugin_id, None))?;
    Ok(SystemCaller { system_ctx })
}

pub(crate) fn system_caller_at(
    lua: &mlua::Lua,
    plugin_id: Option<Arc<str>>,
) -> Result<SystemCaller> {
    let plugin_id = require_plugin_id(plugin_id, "system plugin call")?;
    let source = lua_call_site(lua, &plugin_id);
    let system_ctx = system_context(Some(lua))
        .ok_or_else(|| context_required_error("system", &plugin_id, source))?;
    Ok(SystemCaller { system_ctx })
}

impl harmony_core::AsyncContextPropagator for CallerContextPropagator {
    fn wrap_lua_future(
        &self,
        lua: &mlua::Lua,
        future: harmony_core::LuaAsyncFuture,
    ) -> harmony_core::LuaAsyncFuture {
        match current_context(lua) {
            Some(CallerContext::Request(principal)) => Box::pin(scope_request(principal, future)),
            Some(CallerContext::System(system_ctx)) => Box::pin(scope_system(system_ctx, future)),
            None => future,
        }
    }
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

    #[tokio::test]
    async fn system_context_survives_scheduler_resume() -> anyhow::Result<()> {
        use harmony_core::LuaAsyncExt;

        let lua = mlua::Lua::new();
        install_context_propagator(&lua);

        let plugin_id = Arc::<str>::from("alpha");
        let async_call = {
            let plugin_id = plugin_id.clone();
            lua.create_async_function(move |lua, ()| {
                let plugin_id = plugin_id.clone();
                async move {
                    tokio::task::yield_now().await;
                    system_caller_at(&lua, Some(plugin_id))?;
                    Ok(())
                }
            })?
        };
        lua.globals().set("async_call", async_call)?;

        let root = lua
            .load(
                r#"
                return function()
                    async_call()
                    async_call()
                end
                "#,
            )
            .eval::<mlua::Function>()?;
        let thread = lua.create_thread(root)?;
        let call = harmony_core::run_thread::<()>(&lua, thread.clone(), ());
        scope_system_thread(
            &lua,
            &thread,
            crate::services::libraries::system_context(),
            call,
        )
        .await?;

        Ok(())
    }

    async fn scope_system_thread<T>(
        lua: &mlua::Lua,
        thread: &mlua::Thread,
        system_ctx: SystemContext,
        future: impl Future<Output = T>,
    ) -> T {
        let context = CallerContext::System(system_ctx);
        let previous = set_thread_context(lua, thread, context);
        let result = scope_system(system_ctx, future).await;
        restore_thread_context(lua, thread, previous);
        result
    }
}
