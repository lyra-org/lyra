// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use harmony_core::{
    Module,
    ensure_scheduler,
};
use harmony_luau::{
    DescribeModule,
    LuauType,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file,
};
use mlua::Lua;
use mlua_scheduler::userdata::task_lib;

type LuaScheduler = mlua_scheduler::schedulers::rodan::CoreScheduler;

struct TaskModuleDocs;

pub fn get_module() -> Module {
    Module {
        path: "harmony/task".into(),
        setup: Arc::new(|lua: &Lua| -> anyhow::Result<mlua::Table> {
            ensure_scheduler(lua)?;
            Ok(task_lib::<LuaScheduler>(lua)?)
        }),
        scope: harmony_core::Scope {
            id: "harmony.task".into(),
            description: "Schedule tasks and sleep on the Lua scheduler.",
            danger: harmony_core::Danger::Low,
        },
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
    use super::render_luau_definition;

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
}
