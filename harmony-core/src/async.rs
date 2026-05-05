// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::future::Future;

use mlua::{
    FromLuaMulti,
    Function,
    IntoLuaMulti,
    Lua,
    MaybeSend,
    Thread,
    UserDataMethods,
    UserDataRef,
};
use mlua_scheduler::{
    LuaSchedulerAsync,
    MaybeSync,
    XRc,
    taskmgr::{
        NoopHooks,
        SchedulerImpl,
    },
};

pub type LuaScheduler = mlua_scheduler::schedulers::rodan::CoreScheduler;

pub fn ensure_scheduler(lua: &Lua) -> anyhow::Result<()> {
    if lua.app_data_ref::<LuaScheduler>().is_some() {
        return Ok(());
    }

    LuaScheduler::setup(lua, XRc::new(NoopHooks {}))
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;
    Ok(())
}

fn scheduler(lua: &Lua) -> mlua::Result<LuaScheduler> {
    ensure_scheduler(lua).map_err(mlua::Error::external)?;

    let Some(scheduler) = lua.app_data_ref::<LuaScheduler>() else {
        return Err(mlua::Error::runtime("lua scheduler is not initialized"));
    };

    Ok(scheduler.clone())
}

pub async fn run_function_async<R>(
    lua: &Lua,
    function: &Function,
    args: impl IntoLuaMulti,
) -> mlua::Result<R>
where
    R: FromLuaMulti,
{
    let scheduler = scheduler(lua)?;
    let thread = lua.create_thread(function.clone())?;
    let args = args.into_lua_multi(lua)?;
    let values = scheduler.run_in_scheduler(thread, args).await?;
    R::from_lua_multi(values, lua)
}

/// Run a caller-owned thread on the scheduler so the caller can later
/// cancel it via [`cancel_thread`] or close it via [`mlua::Thread::close`].
pub async fn run_thread<R>(lua: &Lua, thread: Thread, args: impl IntoLuaMulti) -> mlua::Result<R>
where
    R: FromLuaMulti,
{
    let scheduler = scheduler(lua)?;
    let args = args.into_lua_multi(lua)?;
    let values = scheduler.run_in_scheduler(thread, args).await?;
    R::from_lua_multi(values, lua)
}

/// Cancel a thread's in-flight scheduler work; does not close the thread.
/// Returns false if the scheduler never saw it. Call [`mlua::Thread::close`]
/// afterwards to reset the Lua state.
pub fn cancel_thread(lua: &Lua, thread: &Thread) -> mlua::Result<bool> {
    let scheduler = scheduler(lua)?;
    Ok(scheduler.cancel_thread(thread))
}

#[allow(async_fn_in_trait)]
pub trait LuaFunctionAsyncExt {
    async fn call_async<R>(&self, args: impl IntoLuaMulti) -> mlua::Result<R>
    where
        R: FromLuaMulti;
}

impl LuaFunctionAsyncExt for Function {
    async fn call_async<R>(&self, args: impl IntoLuaMulti) -> mlua::Result<R>
    where
        R: FromLuaMulti,
    {
        let Some(lua) = self.weak_lua().try_upgrade() else {
            return Err(mlua::Error::runtime("lua instance is no longer valid"));
        };

        run_function_async(&lua, self, args).await
    }
}

pub trait LuaAsyncExt {
    fn create_async_function<A, F, R, FR>(&self, func: F) -> mlua::Result<Function>
    where
        A: mlua::FromLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        F: Fn(Lua, A) -> FR + mlua::MaybeSend + MaybeSync + Clone + 'static,
        R: mlua::IntoLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        FR: Future<Output = mlua::Result<R>> + mlua::MaybeSend + MaybeSync + 'static;

    /// Like [`create_async_function`], but runs `prelude` at the sync-entry
    /// callback boundary (where `lua_getstack` can still see the caller's
    /// coroutine frames) and passes its result into the future. Used to
    /// capture call-site-dependent context (e.g. plugin id resolved from
    /// the Lua stack) that would be unavailable once the scheduler has
    /// suspended the coroutine.
    fn create_async_function_with_prelude<P, PFn, A, F, R, FR>(
        &self,
        prelude: PFn,
        func: F,
    ) -> mlua::Result<Function>
    where
        P: mlua::MaybeSend + MaybeSync + 'static,
        PFn: Fn(&Lua) -> P + mlua::MaybeSend + MaybeSync + Clone + 'static,
        A: mlua::FromLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        F: Fn(Lua, P, A) -> FR + mlua::MaybeSend + MaybeSync + Clone + 'static,
        R: mlua::IntoLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        FR: Future<Output = mlua::Result<R>> + mlua::MaybeSend + MaybeSync + 'static;
}

impl LuaAsyncExt for Lua {
    fn create_async_function<A, F, R, FR>(&self, func: F) -> mlua::Result<Function>
    where
        A: mlua::FromLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        F: Fn(Lua, A) -> FR + mlua::MaybeSend + MaybeSync + Clone + 'static,
        R: mlua::IntoLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        FR: Future<Output = mlua::Result<R>> + mlua::MaybeSend + MaybeSync + 'static,
    {
        ensure_scheduler(self).map_err(mlua::Error::external)?;
        self.create_scheduler_async_function(func)
    }

    fn create_async_function_with_prelude<P, PFn, A, F, R, FR>(
        &self,
        prelude: PFn,
        func: F,
    ) -> mlua::Result<Function>
    where
        P: mlua::MaybeSend + MaybeSync + 'static,
        PFn: Fn(&Lua) -> P + mlua::MaybeSend + MaybeSync + Clone + 'static,
        A: mlua::FromLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        F: Fn(Lua, P, A) -> FR + mlua::MaybeSend + MaybeSync + Clone + 'static,
        R: mlua::IntoLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        FR: Future<Output = mlua::Result<R>> + mlua::MaybeSend + MaybeSync + 'static,
    {
        ensure_scheduler(self).map_err(mlua::Error::external)?;
        self.create_function(move |lua, args: A| {
            let prelude_val = prelude(&lua);
            let function_ref = func.clone();
            let weak_lua = lua.weak();
            let fut = async move {
                let Some(lua) = weak_lua.try_upgrade() else {
                    return Err(mlua::Error::runtime("lua instance is no longer valid"));
                };
                match function_ref(lua, prelude_val, args).await {
                    Ok(result) => {
                        let Some(lua) = weak_lua.try_upgrade() else {
                            return Err(mlua::Error::runtime("lua instance is no longer valid"));
                        };
                        result.into_lua_multi(&lua)
                    }
                    Err(error) => Err(error),
                }
            };
            let scheduler = scheduler(&lua)?;
            scheduler.schedule_async_dyn(lua.current_thread(), Box::pin(fut));
            lua.yield_with(())?;
            Ok(())
        })
    }
}

pub trait LuaUserDataAsyncExt<T> {
    fn add_async_function<F, A, FR, R>(&mut self, name: impl ToString, function: F)
    where
        F: Fn(Lua, A) -> FR + mlua::MaybeSend + MaybeSync + Clone + 'static,
        A: mlua::FromLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        FR: Future<Output = mlua::Result<R>> + mlua::MaybeSend + MaybeSync + 'static,
        R: mlua::IntoLuaMulti + mlua::MaybeSend + MaybeSync + 'static;

    fn add_async_function_with_prelude<P, PFn, F, A, FR, R>(
        &mut self,
        name: impl ToString,
        prelude: PFn,
        function: F,
    ) where
        P: mlua::MaybeSend + MaybeSync + 'static,
        PFn: Fn(&Lua) -> P + mlua::MaybeSend + MaybeSync + Clone + 'static,
        F: Fn(Lua, P, A) -> FR + mlua::MaybeSend + MaybeSync + Clone + 'static,
        A: mlua::FromLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        FR: Future<Output = mlua::Result<R>> + mlua::MaybeSend + MaybeSync + 'static,
        R: mlua::IntoLuaMulti + mlua::MaybeSend + MaybeSync + 'static;

    fn add_async_method<M, A, MR, R>(&mut self, name: impl ToString, method: M)
    where
        T: 'static + MaybeSend + MaybeSync + Clone,
        M: Fn(Lua, T, A) -> MR + mlua::MaybeSend + MaybeSync + Clone + 'static,
        A: mlua::FromLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        MR: Future<Output = mlua::Result<R>> + mlua::MaybeSend + MaybeSync + 'static,
        R: mlua::IntoLuaMulti + mlua::MaybeSend + MaybeSync + 'static;

    fn add_async_method_with_prelude<P, PFn, M, A, MR, R>(
        &mut self,
        name: impl ToString,
        prelude: PFn,
        method: M,
    ) where
        T: 'static + MaybeSend + MaybeSync + Clone,
        P: mlua::MaybeSend + MaybeSync + 'static,
        PFn: Fn(&Lua) -> P + mlua::MaybeSend + MaybeSync + Clone + 'static,
        M: Fn(Lua, P, T, A) -> MR + mlua::MaybeSend + MaybeSync + Clone + 'static,
        A: mlua::FromLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        MR: Future<Output = mlua::Result<R>> + mlua::MaybeSend + MaybeSync + 'static,
        R: mlua::IntoLuaMulti + mlua::MaybeSend + MaybeSync + 'static;
}

impl<T, I> LuaUserDataAsyncExt<T> for I
where
    I: UserDataMethods<T>,
    T: 'static + MaybeSend + MaybeSync + Clone,
{
    fn add_async_function<F, A, FR, R>(&mut self, name: impl ToString, function: F)
    where
        F: Fn(Lua, A) -> FR + mlua::MaybeSend + MaybeSync + Clone + 'static,
        A: mlua::FromLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        FR: Future<Output = mlua::Result<R>> + mlua::MaybeSend + MaybeSync + 'static,
        R: mlua::IntoLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
    {
        self.add_function(name.to_string(), move |lua, args: A| {
            let function_ref = function.clone();
            let weak_lua = lua.weak();

            let fut = async move {
                let Some(lua) = weak_lua.try_upgrade() else {
                    return Err(mlua::Error::runtime("lua instance is no longer valid"));
                };

                match function_ref(lua, args).await {
                    Ok(result) => {
                        let Some(lua) = weak_lua.try_upgrade() else {
                            return Err(mlua::Error::runtime("lua instance is no longer valid"));
                        };

                        result.into_lua_multi(&lua)
                    }
                    Err(error) => Err(error),
                }
            };

            let scheduler = scheduler(&lua)?;
            scheduler.schedule_async_dyn(lua.current_thread(), Box::pin(fut));
            lua.yield_with(())?;
            Ok(())
        });
    }

    fn add_async_method<M, A, MR, R>(&mut self, name: impl ToString, method: M)
    where
        T: 'static + MaybeSend + MaybeSync + Clone,
        M: Fn(Lua, T, A) -> MR + mlua::MaybeSend + MaybeSync + Clone + 'static,
        A: mlua::FromLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        MR: Future<Output = mlua::Result<R>> + mlua::MaybeSend + MaybeSync + 'static,
        R: mlua::IntoLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
    {
        self.add_function(
            name.to_string(),
            move |lua, (this, args): (UserDataRef<T>, A)| {
                let method_ref = method.clone();
                let weak_lua = lua.weak();
                // Own a snapshot before yielding so reentrant calls do not hold a
                // live userdata borrow across scheduler suspension.
                let this = (*this).clone();
                let fut = async move {
                    let Some(lua) = weak_lua.try_upgrade() else {
                        return Err(mlua::Error::runtime("lua instance is no longer valid"));
                    };

                    match method_ref(lua, this, args).await {
                        Ok(result) => {
                            let Some(lua) = weak_lua.try_upgrade() else {
                                return Err(mlua::Error::runtime(
                                    "lua instance is no longer valid",
                                ));
                            };

                            result.into_lua_multi(&lua)
                        }
                        Err(error) => Err(error),
                    }
                };

                let scheduler = scheduler(&lua)?;
                scheduler.schedule_async_dyn(lua.current_thread(), Box::pin(fut));
                lua.yield_with(())?;
                Ok(())
            },
        );
    }

    fn add_async_function_with_prelude<P, PFn, F, A, FR, R>(
        &mut self,
        name: impl ToString,
        prelude: PFn,
        function: F,
    ) where
        P: mlua::MaybeSend + MaybeSync + 'static,
        PFn: Fn(&Lua) -> P + mlua::MaybeSend + MaybeSync + Clone + 'static,
        F: Fn(Lua, P, A) -> FR + mlua::MaybeSend + MaybeSync + Clone + 'static,
        A: mlua::FromLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        FR: Future<Output = mlua::Result<R>> + mlua::MaybeSend + MaybeSync + 'static,
        R: mlua::IntoLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
    {
        self.add_function(name.to_string(), move |lua, args: A| {
            let prelude_val = prelude(&lua);
            let function_ref = function.clone();
            let weak_lua = lua.weak();
            let fut = async move {
                let Some(lua) = weak_lua.try_upgrade() else {
                    return Err(mlua::Error::runtime("lua instance is no longer valid"));
                };
                match function_ref(lua, prelude_val, args).await {
                    Ok(result) => {
                        let Some(lua) = weak_lua.try_upgrade() else {
                            return Err(mlua::Error::runtime("lua instance is no longer valid"));
                        };
                        result.into_lua_multi(&lua)
                    }
                    Err(error) => Err(error),
                }
            };
            let scheduler = scheduler(&lua)?;
            scheduler.schedule_async_dyn(lua.current_thread(), Box::pin(fut));
            lua.yield_with(())?;
            Ok(())
        });
    }

    fn add_async_method_with_prelude<P, PFn, M, A, MR, R>(
        &mut self,
        name: impl ToString,
        prelude: PFn,
        method: M,
    ) where
        T: 'static + MaybeSend + MaybeSync + Clone,
        P: mlua::MaybeSend + MaybeSync + 'static,
        PFn: Fn(&Lua) -> P + mlua::MaybeSend + MaybeSync + Clone + 'static,
        M: Fn(Lua, P, T, A) -> MR + mlua::MaybeSend + MaybeSync + Clone + 'static,
        A: mlua::FromLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        MR: Future<Output = mlua::Result<R>> + mlua::MaybeSend + MaybeSync + 'static,
        R: mlua::IntoLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
    {
        self.add_function(
            name.to_string(),
            move |lua, (this, args): (UserDataRef<T>, A)| {
                let prelude_val = prelude(&lua);
                let method_ref = method.clone();
                let weak_lua = lua.weak();
                // Own a snapshot before yielding so reentrant calls do not
                // hold a live userdata borrow across scheduler suspension.
                let this = (*this).clone();
                let fut = async move {
                    let Some(lua) = weak_lua.try_upgrade() else {
                        return Err(mlua::Error::runtime("lua instance is no longer valid"));
                    };
                    match method_ref(lua, prelude_val, this, args).await {
                        Ok(result) => {
                            let Some(lua) = weak_lua.try_upgrade() else {
                                return Err(mlua::Error::runtime(
                                    "lua instance is no longer valid",
                                ));
                            };
                            result.into_lua_multi(&lua)
                        }
                        Err(error) => Err(error),
                    }
                };
                let scheduler = scheduler(&lua)?;
                scheduler.schedule_async_dyn(lua.current_thread(), Box::pin(fut));
                lua.yield_with(())?;
                Ok(())
            },
        );
    }
}
