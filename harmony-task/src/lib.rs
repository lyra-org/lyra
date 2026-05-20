// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::time::Duration;

use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;
use harmony_luau::{
    DescribeModule,
    LuauType,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file,
};

struct TaskModuleDocs;
struct TaskModule;
struct TaskLike;
struct TaskThread;
struct TaskArg;

pub fn module_spec() -> ModuleSpec {
    ModuleSpec::new("harmony/task")
        .capability("harmony.task")
        .function(defer_spec())
        .function(delay_spec())
        .function(noop_spec("desynchronize"))
        .function(noop_spec("synchronize"))
        .function(wait_spec())
        .function(cancel_spec())
        .function(spawn_spec())
        .install(|_| Ok(ModuleExport::new(TaskModule)))
}

fn defer_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("defer")
        .arg_name("task")
        .args::<TaskLike>()
        .variadic_args::<TaskArg>()
        .returns::<TaskThread>();
    spec.call(defer_callback)
}

fn delay_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("delay")
        .arg_name("time")
        .arg_name("task")
        .args::<f64>()
        .args::<TaskLike>()
        .variadic_args::<TaskArg>()
        .returns::<TaskThread>();
    spec.call(delay_callback)
}

fn cancel_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("cancel")
        .arg_name("thread")
        .args::<TaskThread>();
    spec.call(cancel_callback)
}

fn wait_spec() -> FunctionSpec {
    let spec = FunctionSpec::async_fn("wait")
        .arg_name("time")
        .args::<Option<f64>>()
        .returns::<f64>();
    spec.call(wait_callback)
}

fn spawn_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("spawn")
        .arg_name("task")
        .args::<TaskLike>()
        .variadic_args::<TaskArg>()
        .returns::<TaskThread>();
    spec.call(spawn_callback)
}

fn noop_spec(name: &'static str) -> FunctionSpec {
    let spec = FunctionSpec::sync_fn(name);
    spec.call(noop_callback)
}

fn noop_callback(_frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    Ok(())
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

fn defer_callback(frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    schedule_task(frame, None)
}

fn spawn_callback(frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    schedule_task(frame, None)
}

fn delay_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let seconds: f64 = frame.args.read_named("time")?;
    let delay = Duration::try_from_secs_f64(seconds).map_err(|error| {
        luau::Error::Runtime(format!(
            "task.delay time must be finite and non-negative: {error}"
        ))
    })?;
    schedule_task(frame, Some(delay))
}

fn schedule_task(
    mut frame: luau::CallFrame<'_>,
    delay: Option<Duration>,
) -> luau::runtime::Result<()> {
    let task: TaskTarget = frame.args.read_named("task")?;
    let args = frame.args.drain_remaining();
    let scheduler = frame.vm.data().get::<harmony_core::LocalScheduler>()?;
    let thread = match task {
        TaskTarget::Function(function) => frame.vm.create_thread(&function)?,
        TaskTarget::Thread(thread) => thread,
    };
    if thread.vm_id() != frame.vm.id() {
        return Err(luau::Error::VmMismatch {
            reference_vm: thread.vm_id(),
            actual_vm: frame.vm.id(),
        });
    }

    let context = scheduler_context(&frame.context);
    if let Some(delay) = delay {
        let _ = scheduler.spawn_luau_thread_after(
            context,
            delay,
            frame.vm.clone(),
            thread.clone(),
            args,
        );
    } else {
        scheduler.schedule_luau_thread(context, frame.vm.clone(), thread.clone(), args);
    };
    frame.returns.write(thread)?;
    Ok(())
}

fn cancel_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let thread: luau::Thread = frame.args.read_named("thread")?;
    let scheduler = frame.vm.data().get::<harmony_core::LocalScheduler>()?;
    scheduler.cancel_luau_thread(&thread);
    Ok(())
}

fn wait_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let seconds = frame
        .args
        .read_optional_named::<f64>("time")?
        .unwrap_or(0.0);
    let delay = Duration::try_from_secs_f64(seconds).map_err(|error| {
        luau::Error::Runtime(format!(
            "task.wait time must be finite and non-negative: {error}"
        ))
    })?;
    let scheduler = frame.vm.data().get::<harmony_core::LocalScheduler>()?;
    scheduler.park_luau_thread(&frame.thread);
    scheduler.schedule_luau_thread_after(
        scheduler_context(&frame.context),
        delay,
        frame.vm.clone(),
        frame.thread.clone(),
        vec![luau::Value::Number(seconds)],
    );
    frame.yield_now();
    Ok(())
}

fn scheduler_context(context: &luau::CallContext) -> harmony_core::CallContext {
    let mut caller = harmony_core::ContextBag::default();
    for (type_id, value) in context.caller.cloned_entries() {
        caller.insert_shared(type_id, value);
    }
    harmony_core::CallContext {
        origin: harmony_core::ChunkOrigin {
            module: context
                .origin
                .module
                .as_ref()
                .map(|module| harmony_core::ModuleId(module.0.clone())),
            plugin: context.origin.plugin.clone(),
            path: context.origin.path.clone(),
        },
        capability: context
            .capability
            .as_ref()
            .map(|capability| harmony_core::CapabilityId(capability.0.clone())),
        caller,
        task_group: harmony_core::TaskGroupId(context.task_group.0),
    }
}

pub fn render_luau_definition() -> Result<String, std::fmt::Error> {
    render_definition_file(&TaskModuleDocs::module_descriptor(), &[])
}

impl DescribeModule for TaskModuleDocs {
    fn module_descriptor() -> ModuleDescriptor {
        let callback = LuauType::function(
            vec![harmony_luau::FunctionParameter {
                name: None,
                ty: LuauType::any(),
                variadic: true,
            }],
            Vec::new(),
        );
        let task_like = LuauType::union(vec![callback, LuauType::thread()]);

        ModuleDescriptor {
            name: "Task",
            local_name: "task",
            description: Some(
                "Task scheduling primitives backed by the Harmony scheduler.\nAlso patches coroutine.resume and coroutine.wrap to resume scheduled threads correctly.",
            ),
            fields: Vec::new(),
            functions: vec![
                ModuleFunctionDescriptor {
                    path: vec!["defer"],
                    description: Some(
                        "Schedules a function or thread to resume on the next scheduler tick.",
                    ),
                    params: vec![
                        ParameterDescriptor {
                            name: "task",
                            ty: task_like.clone(),
                            description: None,
                            variadic: false,
                        },
                        ParameterDescriptor {
                            name: "args",
                            ty: LuauType::any(),
                            description: None,
                            variadic: true,
                        },
                    ],
                    returns: vec![LuauType::thread()],
                    yields: false,
                },
                ModuleFunctionDescriptor {
                    path: vec!["delay"],
                    description: Some(
                        "Schedules a function or thread to resume after a delay in seconds.",
                    ),
                    params: vec![
                        ParameterDescriptor {
                            name: "time",
                            ty: f64::luau_type(),
                            description: None,
                            variadic: false,
                        },
                        ParameterDescriptor {
                            name: "task",
                            ty: task_like.clone(),
                            description: None,
                            variadic: false,
                        },
                        ParameterDescriptor {
                            name: "args",
                            ty: LuauType::any(),
                            description: None,
                            variadic: true,
                        },
                    ],
                    returns: vec![LuauType::thread()],
                    yields: false,
                },
                ModuleFunctionDescriptor {
                    path: vec!["desynchronize"],
                    description: Some(
                        "No-op compatibility shim for environments that support desynchronized execution.",
                    ),
                    params: Vec::new(),
                    returns: Vec::new(),
                    yields: false,
                },
                ModuleFunctionDescriptor {
                    path: vec!["synchronize"],
                    description: Some(
                        "No-op compatibility shim for environments that support synchronized execution.",
                    ),
                    params: Vec::new(),
                    returns: Vec::new(),
                    yields: false,
                },
                ModuleFunctionDescriptor {
                    path: vec!["wait"],
                    description: Some(
                        "Yields the current thread for at least the requested duration and returns the elapsed seconds.",
                    ),
                    params: vec![ParameterDescriptor {
                        name: "time",
                        ty: LuauType::optional(f64::luau_type()),
                        description: None,
                        variadic: false,
                    }],
                    returns: vec![f64::luau_type()],
                    yields: true,
                },
                ModuleFunctionDescriptor {
                    path: vec!["cancel"],
                    description: Some("Cancels a scheduled thread."),
                    params: vec![ParameterDescriptor {
                        name: "thread",
                        ty: LuauType::thread(),
                        description: None,
                        variadic: false,
                    }],
                    returns: Vec::new(),
                    yields: false,
                },
                ModuleFunctionDescriptor {
                    path: vec!["spawn"],
                    description: Some(
                        "Starts a function or resumes a thread immediately on the scheduler.",
                    ),
                    params: vec![
                        ParameterDescriptor {
                            name: "task",
                            ty: task_like,
                            description: None,
                            variadic: false,
                        },
                        ParameterDescriptor {
                            name: "args",
                            ty: LuauType::any(),
                            description: None,
                            variadic: true,
                        },
                    ],
                    returns: vec![LuauType::thread()],
                    yields: false,
                },
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        module_spec,
        render_luau_definition,
    };

    #[test]
    fn renders_task_module_definition() {
        let rendered = render_luau_definition().expect("render harmony/task docs");

        assert!(rendered.contains("@class Task"));
        assert!(
            rendered
                .contains("function task.defer(task: ((...any) -> ()) | thread, ...: any): thread")
        );
        assert!(rendered.contains(
            "function task.delay(time: number, task: ((...any) -> ()) | thread, ...: any): thread"
        ));
        assert!(rendered.contains("@yields"));
        assert!(rendered.contains("function task.wait(time: number?): number"));
        assert!(rendered.contains("function task.cancel(thread: thread)"));
        assert!(
            rendered
                .contains("function task.spawn(task: ((...any) -> ()) | thread, ...: any): thread")
        );
    }

    #[test]
    fn exposes_handwritten_module_spec() {
        let spec = module_spec();

        assert_eq!(spec.id.0.as_ref(), "harmony/task");
        assert_eq!(spec.capability.as_ref().unwrap().0.as_ref(), "harmony.task");
        assert_eq!(spec.functions.len(), 7);
        assert_eq!(spec.functions[0].name.as_ref(), "defer");
        assert!(spec.functions[0].variadic);
        assert_eq!(spec.functions[1].name.as_ref(), "delay");
        assert!(spec.functions[1].variadic);
        assert!(spec.functions.iter().any(|function| function.yields));
        assert_eq!(
            spec.functions
                .iter()
                .find(|function| function.name.as_ref() == "wait")
                .expect("wait spec")
                .return_types
                .len(),
            1
        );
    }

    #[test]
    fn luau_module_schedules_spawn_and_cancel() -> harmony_luau::runtime::Result<()> {
        let vm = harmony_luau::Vm::new()?;
        vm.open_standard_libraries(harmony_luau::StandardLibraries {
            base: true,
            ..harmony_luau::StandardLibraries::none()
        })?;
        vm.data().insert(harmony_core::LocalScheduler::new())?;
        let scheduler = vm.data().get::<harmony_core::LocalScheduler>()?;
        let spec = module_spec();
        let table =
            harmony_core::install_luau_module(&vm, &harmony_core::ChunkOrigin::default(), &spec)?;
        vm.set_global_table("task", &table)?;

        let values = vm.eval(
            std::sync::Arc::<[u8]>::from(
                &br#"
                    task.desynchronize()
                    task.synchronize()
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
