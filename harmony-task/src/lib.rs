// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::time::Duration;

use harmony_luau as luau;
use harmony_luau::{
    DescribeModule,
    LuauType,
    LuauTypeInfo,
    render_definition_file,
};

pub struct TaskModule;

/// The capability hosts conventionally gate this module behind.
pub const SCOPE: &str = "harmony.task";

#[harmony_macros::module(
    id = "harmony/task",
    name = "Task",
    local = "task",
    description = "Task scheduling primitives backed by the Harmony scheduler."
)]
impl TaskModule {
    #[harmony(description = "Schedules a function or thread to resume on the next scheduler tick.")]
    fn defer(
        vm: &luau::Vm,
        context: &luau::CallContext,
        task: TaskTarget,
        args: luau::Variadic<luau::Value>,
    ) -> luau::runtime::Result<luau::Thread> {
        schedule_task(vm, context, task, args, None)
    }

    #[harmony(description = "Schedules a function or thread to resume after a delay in seconds.")]
    fn delay(
        vm: &luau::Vm,
        context: &luau::CallContext,
        time: f64,
        task: TaskTarget,
        args: luau::Variadic<luau::Value>,
    ) -> luau::runtime::Result<luau::Thread> {
        let delay = Duration::try_from_secs_f64(time).map_err(|error| {
            luau::Error::Runtime(format!(
                "task.delay time must be finite and non-negative: {error}"
            ))
        })?;
        schedule_task(vm, context, task, args, Some(delay))
    }

    #[harmony(
        description = "Yields the current thread for at least the requested duration and returns the elapsed seconds.",
        returns(f64)
    )]
    fn wait(time: Option<f64>) -> luau::runtime::Result<luau::ScheduledFuture> {
        let seconds = time.unwrap_or(0.0);
        let delay = Duration::try_from_secs_f64(seconds).map_err(|error| {
            luau::Error::Runtime(format!(
                "task.wait time must be finite and non-negative: {error}"
            ))
        })?;
        Ok(luau::ScheduledFuture::after(
            delay,
            async move { Ok(seconds) },
        ))
    }

    #[harmony(description = "Cancels a scheduled thread and closes it, as `coroutine.close` does.")]
    fn cancel(vm: &luau::Vm, thread: luau::Thread) -> luau::runtime::Result<()> {
        let scheduler = vm.data().get::<harmony_core::LocalScheduler>()?;
        scheduler.cancel_luau_thread(&thread);
        Ok(())
    }

    #[harmony(description = "Starts a function or resumes a thread immediately on the scheduler.")]
    fn spawn(
        vm: &luau::Vm,
        context: &luau::CallContext,
        task: TaskTarget,
        args: luau::Variadic<luau::Value>,
    ) -> luau::runtime::Result<luau::Thread> {
        schedule_task(vm, context, task, args, None)
    }
}

enum TaskTarget {
    Function(luau::Function),
    Thread(luau::Thread),
}

impl<'vm> luau::FromLuau<'vm> for TaskTarget {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        match reader.read::<luau::Value>()? {
            luau::Value::Function(function) => Ok(Self::Function(function)),
            luau::Value::Thread(thread) => Ok(Self::Thread(thread)),
            other => Err(luau::Error::ArgumentType {
                name: std::sync::Arc::from("task"),
                expected: "function or thread",
                actual: other.type_name(),
            }),
        }
    }
}

impl LuauTypeInfo for TaskTarget {
    fn luau_type() -> LuauType {
        let callback = LuauType::function(
            vec![harmony_luau::FunctionParameter {
                name: None,
                ty: LuauType::any(),
                variadic: true,
            }],
            Vec::new(),
        );
        LuauType::union(vec![callback, LuauType::thread()])
    }
}

fn schedule_task(
    vm: &luau::Vm,
    context: &luau::CallContext,
    task: TaskTarget,
    args: luau::Variadic<luau::Value>,
    delay: Option<Duration>,
) -> luau::runtime::Result<luau::Thread> {
    let scheduler = vm.data().get::<harmony_core::LocalScheduler>()?;
    let thread = match task {
        TaskTarget::Function(function) => vm.create_thread(&function)?,
        TaskTarget::Thread(thread) => thread,
    };

    let context = context.clone();
    if let Some(delay) = delay {
        scheduler.schedule_luau_thread_after(context, delay, thread.clone(), args.0);
    } else {
        scheduler.schedule_luau_thread(context, thread.clone(), args.0);
    };
    Ok(thread)
}

pub fn render_luau_definition() -> Result<String, std::fmt::Error> {
    render_definition_file(&TaskModule::module_descriptor(), &[])
}

#[cfg(test)]
mod tests {
    use super::TaskModule;

    #[test]
    fn luau_module_schedules_spawn_and_cancel() -> harmony_luau::runtime::Result<()> {
        let vm = harmony_luau::Vm::new()?;
        vm.open_standard_libraries(harmony_luau::StandardLibraries {
            base: true,
            ..harmony_luau::StandardLibraries::none()
        })?;
        vm.data().insert(harmony_core::LocalScheduler::new())?;
        let scheduler = vm.data().get::<harmony_core::LocalScheduler>()?;
        let spec = TaskModule::module_spec();
        let table =
            harmony_core::luau::install_module(&vm, &harmony_core::ChunkOrigin::default(), &spec)?;
        vm.set_global_table("task", &table)?;

        let values = vm.eval(
            std::sync::Arc::<[u8]>::from(
                &br#"
                    local spawned = task.spawn(function(lhs, rhs)
                        spawned_total = lhs + rhs
                    end, 20, 22)
                    local cancelled = task.spawn(function()
                        cancelled_total = 1
                    end)
                    task.cancel(cancelled)
                    local waited = task.spawn(function()
                        waited_elapsed = task.wait(0)
                    end)
                    return type(spawned), type(waited), type(task.wait)
                "#[..],
            ),
            harmony_luau::ChunkOrigin::default(),
        )?;

        assert_eq!(
            values,
            vec![
                harmony_luau::Value::String(b"thread".to_vec()),
                harmony_luau::Value::String(b"thread".to_vec()),
                harmony_luau::Value::String(b"function".to_vec())
            ]
        );
        assert_eq!(scheduler.poll_ready(), 2);
        assert_eq!(scheduler.poll_ready(), 1);
        assert_eq!(
            vm.eval(
                std::sync::Arc::<[u8]>::from(
                    &b"return spawned_total, cancelled_total, waited_elapsed"[..]
                ),
                harmony_luau::ChunkOrigin::default(),
            )?,
            vec![
                harmony_luau::Value::Number(42.0),
                harmony_luau::Value::Nil,
                harmony_luau::Value::Number(0.0)
            ]
        );
        Ok(())
    }
}
