use crate::{MaybeSync, taskmgr::SchedulerVTable};
use mluau::prelude::*;

pub fn create_async_lua_function<A, F, R, FR>(lua: &Lua, func: F) -> LuaResult<LuaFunction>
where
    A: FromLuaMulti + mluau::MaybeSend + MaybeSync + 'static,
    F: Fn(Lua, A) -> FR + mluau::MaybeSend + MaybeSync + Clone + 'static,
    R: mluau::IntoLuaMulti + mluau::MaybeSend + MaybeSync + 'static,
    FR: futures_util::Future<Output = LuaResult<R>> + mluau::MaybeSend + MaybeSync + 'static,
{
    lua.create_function(move |lua, args| {
        let func_ref = func.clone();

        let weak_lua = lua.weak();

        let fut = async move {
            let Some(lua) = weak_lua.try_upgrade() else {
                return Err(LuaError::RuntimeError(
                    "Lua instance is no longer valid".to_string(),
                ));
            };

            match (func_ref)(lua, args).await {
                Ok(res) => {
                    let Some(lua) = weak_lua.try_upgrade() else {
                        return Err(LuaError::RuntimeError(
                            "Lua instance is no longer valid".to_string(),
                        ));
                    };

                    res.into_lua_multi(&lua)
                }
                Err(e) => Err(e),
            }
        };

        let taskmgr = lua
            .app_data_ref::<SchedulerVTable>()
            .expect("Failed to get task manager")
            .0
            .clone_box();

        taskmgr.schedule_async_dyn(lua.current_thread(), Box::pin(fut));

        lua.yield_with(())?;
        Ok(())
    })
}

pub trait LuaSchedulerAsync {
    /**
     * Creates a scheduler-handled async function
     *
     * # Panics
     *
     * Panics if called outside of a running [`mlua_scheduler::TaskManager`].
     */
    fn create_scheduler_async_function<A, F, R, FR>(&self, func: F) -> LuaResult<LuaFunction>
    where
        A: FromLuaMulti + mluau::MaybeSend + MaybeSync + 'static,
        F: Fn(Lua, A) -> FR + mluau::MaybeSend + MaybeSync + Clone + 'static,
        R: mluau::IntoLuaMulti + mluau::MaybeSend + MaybeSync + 'static,
        FR: futures_util::Future<Output = LuaResult<R>> + mluau::MaybeSend + MaybeSync + 'static;
}

impl LuaSchedulerAsync for Lua {
    fn create_scheduler_async_function<A, F, R, FR>(&self, func: F) -> LuaResult<LuaFunction>
    where
        A: FromLuaMulti + mluau::MaybeSend + MaybeSync + 'static,
        F: Fn(Lua, A) -> FR + mluau::MaybeSend + MaybeSync + Clone + 'static,
        R: mluau::IntoLuaMulti + mluau::MaybeSend + MaybeSync + 'static,
        FR: futures_util::Future<Output = LuaResult<R>> + mluau::MaybeSend + MaybeSync + 'static,
    {
        create_async_lua_function(self, func)
    }
}

pub trait LuaSchedulerAsyncUserData<T> {
    fn add_scheduler_async_method<M, A, MR, R>(&mut self, name: impl ToString, method: M)
    where
        T: 'static + mluau::MaybeSend,
        M: Fn(Lua, mluau::UserDataRef<T>, A) -> MR + mluau::MaybeSend + MaybeSync + Clone + 'static,
        A: FromLuaMulti + mluau::MaybeSend + MaybeSync + 'static,
        MR: std::future::Future<Output = mluau::Result<R>> + mluau::MaybeSend + MaybeSync + 'static,
        R: IntoLuaMulti + mluau::MaybeSend + MaybeSync + 'static;

    fn add_scheduler_async_method_mut<M, A, MR, R>(&mut self, name: impl ToString, method: M)
    where
        T: 'static + mluau::MaybeSend,
        M: Fn(Lua, mluau::UserDataRefMut<T>, A) -> MR
            + mluau::MaybeSend
            + MaybeSync
            + Clone
            + 'static,
        A: FromLuaMulti + mluau::MaybeSend + MaybeSync + 'static,
        MR: std::future::Future<Output = mluau::Result<R>> + mluau::MaybeSend + MaybeSync + 'static,
        R: IntoLuaMulti + mluau::MaybeSend + MaybeSync + 'static;
}

impl<T, I> LuaSchedulerAsyncUserData<T> for I
where
    I: LuaUserDataMethods<T>,
    T: 'static + mluau::MaybeSend,
{
    fn add_scheduler_async_method<M, A, MR, R>(&mut self, name: impl ToString, method: M)
    where
        T: 'static + mluau::MaybeSend,
        M: Fn(Lua, mluau::UserDataRef<T>, A) -> MR + mluau::MaybeSend + MaybeSync + Clone + 'static,
        A: FromLuaMulti + mluau::MaybeSend + MaybeSync + 'static,
        MR: std::future::Future<Output = mluau::Result<R>> + mluau::MaybeSend + MaybeSync + 'static,
        R: IntoLuaMulti + mluau::MaybeSend + MaybeSync + 'static,
    {
        self.add_function(
            name.to_string(),
            move |lua, (this, args): (mluau::UserDataRef<T>, A)| {
                let func_ref = method.clone();

                let weak_lua = lua.weak();

                let fut = async move {
                    let Some(lua) = weak_lua.try_upgrade() else {
                        return Err(LuaError::RuntimeError(
                            "Lua instance is no longer valid".to_string(),
                        ));
                    };

                    match (func_ref)(lua, this, args).await {
                        Ok(res) => {
                            let Some(lua) = weak_lua.try_upgrade() else {
                                return Err(LuaError::RuntimeError(
                                    "Lua instance is no longer valid".to_string(),
                                ));
                            };

                            res.into_lua_multi(&lua)
                        }
                        Err(e) => Err(e),
                    }
                };

                let taskmgr = lua
                    .app_data_ref::<SchedulerVTable>()
                    .expect("Failed to get task manager")
                    .0
                    .clone_box();

                taskmgr.schedule_async_dyn(lua.current_thread(), Box::pin(fut));
                lua.yield_with(())?;
                Ok(())
            },
        );
    }

    fn add_scheduler_async_method_mut<M, A, MR, R>(&mut self, name: impl ToString, method: M)
    where
        T: 'static + mluau::MaybeSend,
        M: Fn(Lua, mluau::UserDataRefMut<T>, A) -> MR
            + mluau::MaybeSend
            + MaybeSync
            + Clone
            + 'static,
        A: FromLuaMulti + mluau::MaybeSend + MaybeSync + 'static,
        MR: std::future::Future<Output = mluau::Result<R>> + mluau::MaybeSend + MaybeSync + 'static,
        R: IntoLuaMulti + mluau::MaybeSend + MaybeSync + 'static,
    {
        self.add_function(
            name.to_string(),
            move |lua, (this, args): (mluau::UserDataRefMut<T>, A)| {
                let func_ref = method.clone();

                let weak_lua = lua.weak();

                let fut = async move {
                    let Some(lua) = weak_lua.try_upgrade() else {
                        return Err(LuaError::RuntimeError(
                            "Lua instance is no longer valid".to_string(),
                        ));
                    };

                    match (func_ref)(lua, this, args).await {
                        Ok(res) => {
                            let Some(lua) = weak_lua.try_upgrade() else {
                                return Err(LuaError::RuntimeError(
                                    "Lua instance is no longer valid".to_string(),
                                ));
                            };

                            res.into_lua_multi(&lua)
                        }
                        Err(e) => Err(e),
                    }
                };

                let taskmgr = lua
                    .app_data_ref::<SchedulerVTable>()
                    .expect("Failed to get task manager")
                    .0
                    .clone_box();

                taskmgr.schedule_async_dyn(lua.current_thread(), Box::pin(fut));
                lua.yield_with(())?;
                Ok(())
            },
        );
    }
}
