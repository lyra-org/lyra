use mluau::prelude::*;

use crate::taskmgr::SchedulerImpl;

/// Creates the `task` library, patching coroutine.resume to handle on_response signals as appropriate.
pub fn task_lib<T: SchedulerImpl + Clone + 'static>(lua: &Lua) -> LuaResult<LuaTable> {
    lua.globals().get::<LuaTable>("coroutine")?.set(
        "resume",
        lua.create_function(move |lua, (coroutine, args): (LuaThread, LuaMultiValue)| {
            let taskmgr = T::get(lua);
            let result = taskmgr.core_actor().resume_thread_result(coroutine, args);

            match result {
                Ok(r) => (true, r).into_lua_multi(lua),
                Err(r) => (false, r).into_lua_multi(lua),
            }
        })?,
    )?;

    lua.globals().get::<LuaTable>("coroutine")?.set(
        "wrap",
        lua.create_function(move |lua, f: LuaFunction| {
            // coroutine.wrap is a convenience function that creates a coroutine and returns a function that resumes it.
            let thread = lua.create_thread(f)?;
            let taskmgr = T::get(lua);

            let f = lua.create_function(move |_lua, args: LuaMultiValue| {
                taskmgr.core_actor().resume_thread_result(thread.clone(), args)
            })?;

            Ok(f)
        })?,
    )?;

    let table = lua.create_table()?;
    let taskmgr_parent = T::get(lua);
    let taskmgr_ref = taskmgr_parent.clone();
    table.set(
        "defer",
        lua.create_function(
            move |lua, (task, args): (LuaEither<LuaFunction, LuaThread>, LuaMultiValue)| {
                let thread = match task {
                    LuaEither::Left(func) => lua.create_thread(func)?,
                    LuaEither::Right(th) => th,
                };

                taskmgr_ref.schedule_deferred(thread.clone(), args);
                Ok(thread)
            },
        )?,
    )?;

    let taskmgr_ref = taskmgr_parent.clone();
    table.set(
        "delay",
        lua.create_function(
            move |lua, (time, task, args): (f64, LuaEither<LuaFunction, LuaThread>, LuaMultiValue)| {
                let thread = match task {
                    LuaEither::Left(func) => lua.create_thread(func)?,
                    LuaEither::Right(th) => th,
                };

                taskmgr_ref.schedule_delay(
                    thread.clone(),
                    std::time::Duration::from_secs_f64(time.max(0.05)),
                    args,
                );
                Ok(thread)
            },
        )?,
    )?;

    table.set(
        "desynchronize",
        lua.create_function(move |_lua, _args: LuaMultiValue| {
            // No-op in this context
            Ok(())
        })?,
    )?;

    table.set(
        "synchronize",
        lua.create_function(move |_lua, _args: LuaMultiValue| {
            // No-op in this context
            Ok(())
        })?,
    )?;

    let taskmgr_ref = taskmgr_parent.clone();
    table.set(
        "wait",
        lua.create_function(move |lua, time: Option<f64>| {
            let thread = lua.current_thread();
            let duration = time.map_or(std::time::Duration::from_secs_f64(0.05), |t| {
                std::time::Duration::from_secs_f64(t.max(0.05))
            });

            taskmgr_ref.schedule_wait(thread, duration);
            lua.yield_with(())
        })?,
    )?;

    let taskmgr_ref = taskmgr_parent.clone();

    table.set(
        "cancel",
        lua.create_function(move |_lua, thread: LuaThread| {
            taskmgr_ref.cancel_thread(&thread);
            Ok(())
        })?,
    )?;

    let taskmgr_ref = taskmgr_parent.clone();
    table.set(
        "spawn",
        lua.create_function(
            move |lua, (task, args): (LuaEither<LuaFunction, LuaThread>, LuaMultiValue)| {
                let thread = match task {
                    LuaEither::Left(func) => lua.create_thread(func)?,
                    LuaEither::Right(th) => th,
                };

                taskmgr_ref.core_actor().resume_thread(thread.clone(), Ok::<_, mluau::Error>(args));

                Ok(thread)
            },
        )?,
    )?;

    table.set_readonly(true);

    Ok(table)
}
