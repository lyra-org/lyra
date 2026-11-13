use super::plugin_origin;
use crate::plugin::testing::*;
use crate::plugin::{
    LoadedPlugin,
    PluginDiscoveryError,
    PluginLoadError,
    PluginManager,
    PluginManifest,
    RuntimeBuilder,
};
use crate::{
    CallContext,
    ChunkOrigin,
    FunctionSpec,
    GatedModule,
    GlobalSpec,
    MemorySourceLoader,
    ModuleExport,
    ModuleSpec,
};
use crate::{
    scheduler::testing::ending_without_a_missed_wake,
    testing::ManualClock,
};
use harmony_luau as luau;

use std::collections::{
    BTreeMap,
    HashSet,
};
use std::path::PathBuf;
use std::sync::{
    Arc,
    Mutex,
};

#[derive(Clone)]
struct TestStore(&'static str);

fn manifest_arc(manifests: Vec<PluginManifest>) -> Arc<[PluginManifest]> {
    Arc::from(manifests)
}

#[test]
fn runtime_builder_installs_require_and_loads_memory_sources() -> anyhow::Result<()> {
    let mut loader = MemorySourceLoader::new();
    loader.insert("plugin:demo/util", b"return { answer = 42 }".as_slice());
    let mut demo = loaded_plugin("demo", "1.0.0", BTreeMap::new());
    demo.manifest.entrypoint = None;
    let runtime = RuntimeBuilder::new(loader, manifest_arc(vec![demo.manifest.clone()]))
        .plugins(Arc::from(vec![demo]))
        .build()?;
    assert!(runtime.exec_all().is_empty());

    let values = runtime.eval_source_with_context(
        Arc::<[u8]>::from(
            &br#"
                local util = require("@demo/util")
                return util.answer
            "#[..],
        ),
        CallContext::default(),
    )?;

    assert_eq!(values, vec![luau::Value::Number(42.0)]);
    Ok(())
}

#[test]
fn eval_passes_args_as_chunk_varargs() -> anyhow::Result<()> {
    let runtime =
        RuntimeBuilder::new(MemorySourceLoader::new(), manifest_arc(Vec::new())).build()?;

    let values = runtime.eval_source_with_args(
        Arc::<[u8]>::from(&b"local first, second = ...\nreturn first + second"[..]),
        CallContext::default(),
        vec![luau::Value::Number(40.0), luau::Value::Number(2.0)],
    )?;

    assert_eq!(values, vec![luau::Value::Number(42.0)]);
    Ok(())
}

fn write_demo_plugin(plugin_dir: &std::path::Path, answer: &str) {
    std::fs::write(
        plugin_dir.join("lib.luau"),
        format!("return {{ answer = {answer} }}"),
    )
    .expect("write lib");
    std::fs::write(
        plugin_dir.join("init.luau"),
        "demo_answer = require('./lib').answer",
    )
    .expect("write init");
}

#[test]
fn restart_plugin_reloads_sources_from_disk_and_verify_rejects_manifest_changes()
-> anyhow::Result<()> {
    let plugins_dir = temp_plugin_dir("invalidate");
    let plugin_dir = plugins_dir.join("demo");
    std::fs::create_dir_all(&plugin_dir)?;
    std::fs::write(
        plugin_dir.join(crate::plugin::PLUGIN_CONFIG_FILENAME),
        r#"{"schema_version":1,"id":"demo","name":"demo","version":"1.0.0","host":"*","description":"","entrypoint":"init.luau","scopes":[]}"#,
    )?;
    write_demo_plugin(&plugin_dir, "1");

    let manifest = manifest("demo", &[]);
    let plugin = LoadedPlugin {
        manifest: manifest.clone(),
        directory: plugin_dir.clone(),
        declared_scopes: HashSet::new(),
    };
    let runtime = RuntimeBuilder::new(
        crate::FilesystemSourceLoader::new(&plugins_dir, &plugins_dir),
        manifest_arc(vec![manifest]),
    )
    .plugins(Arc::from(vec![plugin]))
    .build()?;
    let read_answer =
        |runtime: &crate::plugin::Runtime| eval_as(runtime, "demo", b"return demo_answer");

    runtime.exec_plugin("demo")?;
    assert_eq!(read_answer(&runtime)?, vec![luau::Value::Number(1.0)]);

    write_demo_plugin(&plugin_dir, "2");
    runtime.exec_plugin("demo")?;
    assert_eq!(
        read_answer(&runtime)?,
        vec![luau::Value::Number(1.0)],
        "exec without invalidation serves the cached entrypoint"
    );

    runtime.verify_plugin_manifest("demo", &host())?;
    runtime.restart_plugin("demo")?;
    assert_eq!(read_answer(&runtime)?, vec![luau::Value::Number(2.0)]);

    std::fs::write(
        plugin_dir.join(crate::plugin::PLUGIN_CONFIG_FILENAME),
        r#"{"schema_version":1,"id":"demo","name":"demo","version":"1.1.0","host":"*","description":"","entrypoint":"init.luau","scopes":[]}"#,
    )?;
    let error = runtime
        .verify_plugin_manifest("demo", &host())
        .expect_err("changed manifest must be rejected");
    assert!(error.to_string().contains("reload plugins"), "{error}");

    std::fs::write(
        plugin_dir.join(crate::plugin::PLUGIN_CONFIG_FILENAME),
        "0".repeat(crate::plugin::PLUGIN_CONFIG_MAX_BYTES as usize + 1),
    )?;
    let error = runtime
        .verify_plugin_manifest("demo", &host())
        .expect_err("oversized manifest must be rejected");
    assert!(format!("{error:#}").contains("exceeds cap"), "{error:#}");

    let _ = std::fs::remove_dir_all(plugins_dir);
    Ok(())
}

#[derive(Default)]
struct TestHandlers(std::cell::RefCell<Vec<(luau::Function, CallContext)>>);

/// `later(seconds, fn)` runs `fn` after a delay in the caller's task
/// group, `register(fn)` keeps `fn` with the caller's group for the test
/// to call, `run()` numbers each call and `record(value)` logs a value.
fn task_test_module(recorded: Arc<Mutex<Vec<String>>>) -> ModuleSpec {
    let runs = Arc::new(Mutex::new(0usize));
    let eventually_recorded = recorded.clone();
    let read_function = |frame: &mut luau::CallFrame<'_>| match frame.args.read()? {
        luau::Value::Function(function) => Ok(function),
        other => Err(luau::Error::Runtime(format!(
            "expected a function, got {}",
            other.type_name()
        ))),
    };
    let group_context = |frame: &luau::CallFrame<'_>| CallContext {
        task_group: frame.context.task_group,
        ..CallContext::default()
    };
    ModuleSpec::new("test/tasks")
        .function(FunctionSpec::new("wake").call(|frame| {
            frame
                .vm
                .data()
                .get::<crate::Scheduler>()?
                .wake_handle()
                .wake();
            Ok(())
        }))
        .function(FunctionSpec::new("later").call(move |mut frame| {
            let seconds: f64 = frame.args.read()?;
            let function = read_function(&mut frame)?;
            let thread = frame.vm.create_thread(&function)?;
            frame
                .vm
                .data()
                .get::<crate::Scheduler>()?
                .spawn_luau_thread_after(
                    group_context(&frame),
                    std::time::Duration::from_secs_f64(seconds),
                    thread,
                    Vec::new(),
                );
            Ok(())
        }))
        .function(FunctionSpec::new("register").call(move |mut frame| {
            let function = read_function(&mut frame)?;
            let context = group_context(&frame);
            frame
                .vm
                .data()
                .get::<TestHandlers>()?
                .0
                .borrow_mut()
                .push((function, context));
            Ok(())
        }))
        .function(FunctionSpec::new("run").call(move |mut frame| {
            let mut runs = runs.lock().expect("runs mutex poisoned");
            *runs += 1;
            frame.returns.write(*runs as i64)?;
            Ok(())
        }))
        .function(FunctionSpec::new("record").call(move |mut frame| {
            let value: String = frame.args.read()?;
            recorded
                .lock()
                .expect("recorded mutex poisoned")
                .push(value);
            Ok(())
        }))
        .function(FunctionSpec::new("check").call(|mut frame| {
            let checked = crate::ensure_current_plugin_run(frame.vm, &frame.context);
            frame
                .returns
                .write(checked.err().map(|error| error.to_string()))?;
            Ok(())
        }))
        .function(FunctionSpec::new("forever").call_async(Arc::new(|_frame| {
            Ok(luau::ScheduledFuture::new(std::future::pending::<
                luau::runtime::Result<()>,
            >()))
        })))
        .function(FunctionSpec::new("soon").call_async(Arc::new(|_frame| {
            Ok(luau::ScheduledFuture::after(
                std::time::Duration::from_millis(10),
                async { Ok(()) },
            ))
        })))
        .function(
            FunctionSpec::new("eventually").call_async(Arc::new(move |mut frame| {
                let value: String = frame.args.read()?;
                let recorded = eventually_recorded.clone();
                Ok(luau::ScheduledFuture::after(
                    std::time::Duration::from_millis(10),
                    async move {
                        recorded
                            .lock()
                            .expect("recorded mutex poisoned")
                            .push(value);
                        Ok(())
                    },
                ))
            })),
        )
        .function(FunctionSpec::new("thread_plugin").call(|mut frame| {
            let plugin = frame
                .thread
                .call_context()?
                .and_then(|context| context.origin.plugin)
                .map(|plugin| plugin.to_string());
            frame.returns.write(plugin)?;
            Ok(())
        }))
        .install(|_| Ok(ModuleExport::new(())))
}

/// A capability only `lib` holds in the task test runtimes.
fn secret_module() -> ModuleSpec {
    ModuleSpec::new("test/secret")
        .function(FunctionSpec::new("get").call(|mut frame| {
            frame.returns.write("secret")?;
            Ok(())
        }))
        .install(|_| Ok(ModuleExport::new(())))
}

/// How long `later` in the tests' sources and the module's `soon` and `eventually` wait.
const LATER: std::time::Duration = std::time::Duration::from_millis(50);
const SOON: std::time::Duration = std::time::Duration::from_millis(10);

/// The entrypoint of a plugin that numbers its run, records it later and registers a handler
/// that never ends.
fn numbered_run_source(plugin: &str) -> String {
    r#"
        local tasks = require('@test/tasks')
        local run = tasks.run()
        tasks.later(0.05, function()
            tasks.record(`{PLUGIN}:{run}`)
        end)
        tasks.register(function()
            tasks.forever()
        end)
    "#
    .replace("{PLUGIN}", plugin)
}

/// Plugins as `(id, dependencies, entrypoint source)` and extra plugin
/// modules as `(source key, source)`.
fn task_test_runtime_with(
    recorded: Arc<Mutex<Vec<String>>>,
    sources: &[(&str, &[&str], &str)],
    modules: &[(&str, &str)],
) -> anyhow::Result<crate::plugin::Runtime> {
    let (loader, plugins) = task_test_plugins(sources, modules);
    task_test_runtime_from(recorded, loader, plugins)
}

fn task_test_plugins(
    sources: &[(&str, &[&str], &str)],
    modules: &[(&str, &str)],
) -> (MemorySourceLoader, Vec<LoadedPlugin>) {
    let mut loader = MemorySourceLoader::new();
    for (key, source) in modules {
        loader.insert(key.to_string(), source.as_bytes());
    }
    let mut plugins = Vec::new();
    for (id, dependencies, source) in sources {
        loader.insert(format!("plugin:{id}/init"), source.as_bytes());
        let scopes: &[&str] = if *id == "lib" {
            &["test.tasks", "test.secret"]
        } else {
            &["test.tasks"]
        };
        let mut plugin_manifest = manifest(id, scopes);
        plugin_manifest.dependencies = dependencies
            .iter()
            .map(|dependency| (dependency.to_string(), self::dependency("1")))
            .collect();
        plugins.push(loaded(plugin_manifest));
    }
    (loader, plugins)
}

fn task_test_runtime_from(
    recorded: Arc<Mutex<Vec<String>>>,
    loader: MemorySourceLoader,
    plugins: Vec<LoadedPlugin>,
) -> anyhow::Result<crate::plugin::Runtime> {
    task_test_builder(recorded, loader, plugins).build()
}

fn task_test_builder(
    recorded: Arc<Mutex<Vec<String>>>,
    loader: MemorySourceLoader,
    plugins: Vec<LoadedPlugin>,
) -> RuntimeBuilder<MemorySourceLoader> {
    RuntimeBuilder::new(
        loader,
        manifest_arc(
            plugins
                .iter()
                .map(|plugin| plugin.manifest.clone())
                .collect(),
        ),
    )
    .plugins(Arc::from(plugins))
    .modules(vec![
        GatedModule::new("test.tasks", task_test_module(recorded)),
        GatedModule::new("test.secret", secret_module()),
    ])
    .configure_vm(|vm| {
        vm.data().insert(TestHandlers::default())?;
        Ok(())
    })
}

/// A task test runtime on a manual clock: the delays its plugins wait out pass when the test
/// advances it, and nothing here waits for them. Its methods stand in for the runtime's own
/// that would.
struct StagedRuntime {
    runtime: crate::plugin::Runtime,
    clock: ManualClock,
    recorded: Arc<Mutex<Vec<String>>>,
}

impl std::ops::Deref for StagedRuntime {
    type Target = crate::plugin::Runtime;

    fn deref(&self) -> &crate::plugin::Runtime {
        &self.runtime
    }
}

impl StagedRuntime {
    fn new(sources: &[(&str, &[&str], &str)], modules: &[(&str, &str)]) -> anyhow::Result<Self> {
        let clock = ManualClock::new();
        let recorded = Arc::<Mutex<Vec<String>>>::default();
        let (loader, plugins) = task_test_plugins(sources, modules);
        let runtime = task_test_builder(recorded.clone(), loader, plugins)
            .manual_clock(clock.clone())
            .build()?;
        Ok(Self {
            runtime,
            clock,
            recorded,
        })
    }

    /// Two plugins, `demo` and `other`, of [`numbered_run_source`].
    fn numbered_runs() -> anyhow::Result<Self> {
        Self::new(
            &[
                ("demo", &[], &numbered_run_source("demo")),
                ("other", &[], &numbered_run_source("other")),
            ],
            &[],
        )
    }

    fn scheduler(&self) -> std::rc::Rc<crate::Scheduler> {
        self.runtime.scheduler().expect("the runtime's scheduler")
    }

    /// Runs `entrypoint` to its end, which it must reach without waiting.
    fn drive(&self, entrypoint: Option<super::RunningEntrypoint>) -> anyhow::Result<()> {
        let Some(entrypoint) = entrypoint else {
            return Ok(());
        };
        let scheduler = self.scheduler();
        let task = scheduler.luau_thread_handle(entrypoint.watched.thread());
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        let mut finish = std::pin::pin!(entrypoint.finish());
        let result = loop {
            {
                let _guard = self.runtime.tokio_runtime.enter();
                scheduler.poll_ready();
            }
            if let std::task::Poll::Ready(result) = finish.as_mut().poll(&mut cx) {
                break result;
            }
            anyhow::ensure!(
                scheduler.next_wake_delay() == Some(std::time::Duration::ZERO),
                "the entrypoint waits"
            );
        };
        if let Some(task) = task {
            scheduler.remove(task.id());
        }
        result
    }

    /// As [`crate::plugin::Runtime::exec_all`].
    fn exec_all(&self) -> Vec<String> {
        self.runtime
            .plugins
            .iter()
            .filter(|plugin| {
                self.runtime
                    .start_entrypoint(plugin)
                    .and_then(|entrypoint| self.drive(entrypoint))
                    .is_err()
            })
            .map(|plugin| plugin.manifest.id.clone())
            .collect()
    }

    /// As [`crate::plugin::Runtime::restart_plugin`].
    fn restart_plugin(&self, plugin_id: &str) -> anyhow::Result<()> {
        self.drive(self.runtime.start_restart_entrypoint(plugin_id)?)
    }

    /// As [`eval_as`], for a chunk that ends without waiting.
    fn eval_as(&self, plugin: &str, source: &'static [u8]) -> anyhow::Result<Vec<luau::Value>> {
        let mut context = CallContext {
            origin: plugin_origin(plugin, "check.luau"),
            ..CallContext::default()
        };
        let thread = self
            .runtime
            .load_source_thread(Arc::<[u8]>::from(source), &mut context)?;
        let scheduler = self.scheduler();
        scheduler.spawn_luau_thread(context, thread.clone(), Vec::new());
        crate::testing::drive_thread_while_ready(&self.runtime.tokio_runtime, &scheduler, &thread)
    }

    /// Runs rounds until no task is ready.
    fn run_ready(&self) {
        let scheduler = self.scheduler();
        loop {
            self.runtime.poll_background_tasks();
            if scheduler.next_wake_delay() != Some(std::time::Duration::ZERO) {
                return;
            }
        }
    }

    /// Lets `duration` pass, then runs what came due and what that made ready.
    fn advance(&self, duration: std::time::Duration) {
        self.clock.advance(duration);
        self.run_ready();
    }

    /// How long until the next delayed task is due.
    fn next_delay(&self) -> Option<std::time::Duration> {
        self.scheduler().next_wake_delay()
    }

    fn recorded(&self) -> Vec<String> {
        self.recorded
            .lock()
            .expect("recorded mutex poisoned")
            .clone()
    }
}

/// An entrypoint exporting these `@test/tasks` functions.
fn lib_exporting(functions: &[&str]) -> String {
    let exports = functions
        .iter()
        .map(|function| format!("{function} = tasks.{function}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("local tasks = require('@test/tasks')\nreturn {{ {exports} }}")
}

fn leftover_tasks(runtime: &crate::plugin::Runtime) -> anyhow::Result<Vec<crate::TaskSnapshot>> {
    let scheduler = runtime.vm.data().get::<crate::Scheduler>()?;
    scheduler.remove_finished();
    Ok(scheduler.snapshots())
}

/// Evaluates `source` as plugin code of `plugin`.
fn eval_as(
    runtime: &crate::plugin::Runtime,
    plugin: &str,
    source: &'static [u8],
) -> anyhow::Result<Vec<luau::Value>> {
    runtime.eval_source_with_context(
        Arc::<[u8]>::from(source),
        CallContext {
            origin: plugin_origin(plugin, "check.luau"),
            ..CallContext::default()
        },
    )
}

fn call_handler(
    runtime: &crate::plugin::Runtime,
    index: usize,
) -> anyhow::Result<std::rc::Rc<crate::LocalLuauTaskCompletion>> {
    let (function, context) = runtime.vm.data().get::<TestHandlers>()?.0.borrow()[index].clone();
    let completion = std::rc::Rc::new(crate::LocalLuauTaskCompletion::default());
    let scheduler = runtime.vm.data().get::<crate::Scheduler>()?;
    scheduler.spawn_luau_thread_with_limit_and_completion(
        context,
        runtime.vm.create_thread(&function)?,
        Vec::new(),
        crate::ResumeLimit::budget(std::time::Duration::from_secs(5)),
        completion.clone(),
    );
    Ok(completion)
}

fn completion_result(
    completion: &crate::LocalLuauTaskCompletion,
) -> Option<Result<Vec<luau::Value>, String>> {
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    match completion.poll(&mut context) {
        std::task::Poll::Ready(result) => Some(result.map_err(|error| error.message)),
        std::task::Poll::Pending => None,
    }
}

fn recording_entrypoint(
    recorded: Arc<Mutex<Vec<String>>>,
) -> anyhow::Result<crate::plugin::Runtime> {
    task_test_runtime_with(
        recorded,
        &[("demo", &[], "require('@test/tasks').record('ran')")],
        &[],
    )
}

#[test]
fn a_host_call_takes_the_wake_handle_while_the_scheduler_polls() -> anyhow::Result<()> {
    let runtime = task_test_runtime_with(
        Arc::default(),
        &[("demo", &[], "require('@test/tasks').wake()")],
        &[],
    )?;
    assert!(runtime.exec_all().is_empty());
    Ok(())
}

#[test]
fn a_cancelled_entrypoint_is_not_resumed_by_the_driver() -> anyhow::Result<()> {
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let runtime = recording_entrypoint(recorded.clone())?;
    assert!(runtime.exec_all().is_empty());
    recorded.lock().expect("recorded").clear();

    let entrypoint = runtime
        .start_restart_entrypoint("demo")?
        .ok_or_else(|| anyhow::anyhow!("the plugin has an entrypoint"))?;
    runtime.entrypoint_cancellation().cancel();
    let error = runtime
        .drive_entrypoint(entrypoint)
        .expect_err("a cancelled entrypoint fails");

    assert!(format!("{error:#}").contains("cancelled"), "{error:#}");
    assert!(recorded.lock().expect("recorded").is_empty());
    Ok(())
}

#[test]
fn cancellation_wins_over_a_completion_that_already_landed() -> anyhow::Result<()> {
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let runtime = recording_entrypoint(recorded.clone())?;
    assert!(runtime.exec_all().is_empty());
    recorded.lock().expect("recorded").clear();

    let entrypoint = runtime
        .start_restart_entrypoint("demo")?
        .ok_or_else(|| anyhow::anyhow!("the plugin has an entrypoint"))?;
    // The entrypoint is ready, so one round lands its completion.
    runtime.poll_background_tasks();
    assert_eq!(*recorded.lock().expect("recorded"), ["ran"]);
    runtime.entrypoint_cancellation().cancel();

    let error = runtime
        .drive_entrypoint(entrypoint)
        .expect_err("cancellation takes precedence");
    assert!(format!("{error:#}").contains("cancelled"), "{error:#}");
    Ok(())
}

/// On the real clock and the real wait, as the runtime's owner runs a started restart: as a
/// task, between rounds that sleep until something wakes the scheduler.
#[test]
fn a_started_restart_runs_as_a_task_and_ends_like_a_restart() {
    ending_without_a_missed_wake(|| {
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let runtime = recording_entrypoint(recorded.clone()).expect("runtime");
        assert!(runtime.exec_all().is_empty());
        recorded.lock().expect("recorded").clear();

        let restart = runtime
            .start_plugin_restart("demo")
            .expect("the restart starts")
            .expect("the plugin has an entrypoint");
        let scheduler = runtime.scheduler().expect("scheduler");
        let outcome = std::rc::Rc::new(std::cell::RefCell::new(None));
        scheduler.spawn_watcher(CallContext::default(), {
            let outcome = outcome.clone();
            async move {
                *outcome.borrow_mut() = Some(restart.await);
                Ok(())
            }
        });
        let outcome = loop {
            runtime.poll_background_tasks();
            if let Some(outcome) = outcome.borrow_mut().take() {
                break outcome;
            }
            scheduler.wait_for_work(None);
        };
        outcome.expect("the restart succeeds");
        assert_eq!(*recorded.lock().expect("recorded"), ["ran"]);
    });
}

#[test]
fn a_restart_whose_entrypoint_fails_ends_the_run_from_its_task() -> anyhow::Result<()> {
    let runtime = StagedRuntime::new(
        &[(
            "demo",
            &[],
            r#"
                local tasks = require('@test/tasks')
                local run = tasks.run()
                tasks.later(0.05, function() tasks.record('late:' .. run) end)
                if run > 1 then error('boom') end
            "#,
        )],
        &[],
    )?;
    assert!(runtime.exec_all().is_empty());
    assert_eq!(runtime.next_delay(), Some(LATER), "the first run waits");

    let restart = runtime
        .start_plugin_restart("demo")?
        .ok_or_else(|| anyhow::anyhow!("the plugin has an entrypoint"))?;
    let outcome = std::rc::Rc::new(std::cell::RefCell::new(None));
    runtime.scheduler().spawn_watcher(CallContext::default(), {
        let outcome = outcome.clone();
        async move {
            *outcome.borrow_mut() = Some(restart.await);
            Ok(())
        }
    });
    runtime.run_ready();
    let error = outcome
        .borrow_mut()
        .take()
        .expect("the restart ended")
        .expect_err("the entrypoint fails");
    assert!(format!("{error:#}").contains("boom"), "{error:#}");

    assert!(
        leftover_tasks(&runtime)?.is_empty(),
        "a run's task is left waiting"
    );
    runtime.advance(LATER);
    assert!(
        runtime.recorded().is_empty(),
        "neither run's task outlives it"
    );
    Ok(())
}

#[test]
fn run_end_hooks_run_whenever_a_run_ends() -> anyhow::Result<()> {
    let mut loader = MemorySourceLoader::new();
    loader.insert("plugin:demo/init".to_string(), &b"return nil"[..]);
    loader.insert("plugin:broken/init".to_string(), &b"error('boom')"[..]);
    let manifests = vec![manifest("demo", &[]), manifest("broken", &[])];
    let plugins = manifests.iter().cloned().map(loaded).collect::<Vec<_>>();
    let ended = Arc::new(Mutex::new(Vec::new()));
    let hook_ended = ended.clone();
    let runtime = RuntimeBuilder::new(loader, manifest_arc(manifests))
        .plugins(Arc::from(plugins))
        .on_run_end(move |_vm, plugin_id| {
            hook_ended.lock().unwrap().push(plugin_id.to_string());
        })
        .build()?;
    let ended = || ended.lock().unwrap().clone();

    assert_eq!(runtime.exec_all(), ["broken"]);
    assert_eq!(ended(), ["broken"], "a failed entrypoint ends its run");

    runtime.restart_plugin("demo")?;
    assert_eq!(ended(), ["broken", "demo"]);

    runtime.stop_plugin("demo")?;
    runtime.stop_plugin("demo")?;
    assert_eq!(
        ended(),
        ["broken", "demo", "demo"],
        "stopping a stopped plugin ends no run"
    );
    Ok(())
}

#[test]
fn restart_plugin_cancels_tasks_of_the_previous_run_only() -> anyhow::Result<()> {
    let runtime = StagedRuntime::numbered_runs()?;
    assert!(runtime.exec_all().is_empty());
    assert_eq!(leftover_tasks(&runtime)?.len(), 2, "each run waits");

    runtime.restart_plugin("demo")?;
    assert_eq!(
        leftover_tasks(&runtime)?.len(),
        2,
        "the restarted plugin's first run still waits"
    );
    assert!(runtime.recorded().is_empty());

    runtime.advance(LATER);
    let mut recorded = runtime.recorded();
    recorded.sort();
    assert_eq!(recorded, ["demo:3", "other:2"]);
    assert!(leftover_tasks(&runtime)?.is_empty());
    Ok(())
}

#[test]
fn restart_plugin_cancels_tasks_spawned_inside_coroutines() -> anyhow::Result<()> {
    let runtime = StagedRuntime::new(
        &[(
            "demo",
            &[],
            r#"
                local tasks = require('@test/tasks')
                local run = tasks.run()
                coroutine.wrap(function()
                    tasks.later(0.05, function()
                        tasks.record(`coroutine:{run}`)
                    end)
                end)()
            "#,
        )],
        &[],
    )?;
    assert!(runtime.exec_all().is_empty());
    assert_eq!(leftover_tasks(&runtime)?.len(), 1, "the first run waits");

    runtime.restart_plugin("demo")?;
    assert_eq!(leftover_tasks(&runtime)?.len(), 1);

    runtime.advance(LATER);
    assert_eq!(runtime.recorded(), ["coroutine:2"]);
    Ok(())
}

#[test]
fn exec_all_runs_dependencies_before_their_dependents() -> anyhow::Result<()> {
    let runtime = StagedRuntime::new(
        &[
            (
                "app",
                &["lib"],
                r#"
                    require('@lib')
                    require('@test/tasks').record("app")
                "#,
            ),
            (
                "lib",
                &[],
                r#"
                    local tasks = require('@test/tasks')
                    tasks.record("lib")
                    tasks.later(0.05, function()
                        tasks.record("lib later")
                    end)
                    return {}
                "#,
            ),
        ],
        &[],
    )?;
    assert!(runtime.exec_all().is_empty());
    assert_eq!(runtime.recorded(), ["lib", "app"]);
    assert_eq!(runtime.next_delay(), Some(LATER), "lib's task waits");

    runtime.stop_plugin("lib")?;
    assert!(leftover_tasks(&runtime)?.is_empty());

    runtime.advance(LATER);
    assert_eq!(
        runtime.recorded(),
        ["lib", "app"],
        "stopping lib cancels the task its entrypoint spawned"
    );
    Ok(())
}

/// On the real clock and the real wait: the entrypoint waits for good, so only the
/// cancellation's wake ends the wait its driver sleeps in.
#[test]
fn cancelled_entrypoints_end_the_running_one_and_skip_the_rest() {
    ending_without_a_missed_wake(|| {
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let runtime = task_test_runtime_with(
            recorded.clone(),
            &[
                (
                    "a",
                    &[],
                    r#"
                        local tasks = require('@test/tasks')
                        tasks.record("a")
                        tasks.forever()
                    "#,
                ),
                ("b", &["a"], "require('@test/tasks').record('b')"),
            ],
            &[],
        )
        .expect("runtime");
        let cancellation = runtime.entrypoint_cancellation();
        let reached = recorded.clone();
        // Cancels once `a` has started, whether or not it has yielded yet.
        let cancel = std::thread::spawn(move || {
            while reached.lock().expect("recorded mutex poisoned").is_empty() {
                std::thread::yield_now();
            }
            cancellation.cancel();
        });

        assert_eq!(runtime.exec_all(), ["a", "b"]);
        assert_eq!(*recorded.lock().expect("recorded mutex poisoned"), ["a"]);
        cancel.join().expect("cancelling thread");
    });
}

/// The entrypoint's driver is asleep, with nothing due, when the cancellation comes.
#[test]
fn a_cancellation_wakes_the_driver_of_an_entrypoint_that_waits() {
    ending_without_a_missed_wake(|| {
        let runtime = task_test_runtime_with(
            Arc::default(),
            &[("demo", &[], "require('@test/tasks').forever()")],
            &[],
        )
        .expect("runtime");
        let cancellation = runtime.entrypoint_cancellation();
        let wake = runtime.scheduler().expect("scheduler").wake_handle();
        let cancel = std::thread::spawn(move || {
            while !wake.owner_is_waiting() {
                std::thread::yield_now();
            }
            cancellation.cancel();
        });

        assert_eq!(runtime.exec_all(), ["demo"]);
        cancel.join().expect("cancelling thread");
    });
}

#[test]
fn a_runtime_on_a_manual_clock_runs_no_code_to_its_end_on_the_calling_thread() -> anyhow::Result<()>
{
    let runtime = StagedRuntime::new(
        &[("demo", &[], "require('@test/tasks').record('ran')")],
        &[],
    )?;
    let refused = |error: anyhow::Error| {
        assert!(
            format!("{error:#}").contains("manual clock can't block"),
            "{error:#}"
        );
    };

    assert_eq!(runtime.runtime.exec_all(), ["demo"]);
    refused(runtime.exec_plugin("demo").expect_err("refused"));
    refused(runtime.runtime.restart_plugin("demo").expect_err("refused"));
    refused(
        eval_as(&runtime, "demo", b"require('@test/tasks').record('ran')").expect_err("refused"),
    );
    runtime.run_ready();
    assert!(runtime.recorded().is_empty(), "refused code still ran");
    assert!(leftover_tasks(&runtime)?.is_empty());
    Ok(())
}

#[test]
fn a_plugin_cannot_change_the_globals_or_libraries_another_plugin_sees() -> anyhow::Result<()> {
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let runtime = task_test_runtime_with(
        recorded.clone(),
        &[
            (
                "first",
                &[],
                r#"
                    local tasks = require('@test/tasks')
                    written = "first"
                    print = nil
                    tasks.record(tostring(pcall(function() string.upper = string.lower end)))
                    tasks.record(tostring(pcall(function() _G.written = "first" end)))
                    return {}
                "#,
            ),
            (
                "second",
                &["first"],
                r#"
                    require('@first')
                    local tasks = require('@test/tasks')
                    tasks.record(tostring(written))
                    tasks.record(type(print))
                    tasks.record(string.upper("a"))
                "#,
            ),
        ],
        &[],
    )?;
    assert!(runtime.exec_all().is_empty());
    assert_eq!(
        *recorded.lock().expect("recorded mutex poisoned"),
        ["false", "false", "nil", "function", "A"]
    );
    Ok(())
}

#[test]
fn a_stopped_plugin_s_cached_sources_are_not_served() -> anyhow::Result<()> {
    let runtime = task_test_runtime_with(
        Arc::new(Mutex::new(Vec::new())),
        &[
            ("lib", &[], "return { answer = 42 }"),
            ("app", &["lib"], ""),
        ],
        &[],
    )?;
    assert!(runtime.exec_all().is_empty());
    assert_eq!(
        eval_as(&runtime, "app", b"return require('@lib').answer")?,
        [luau::Value::Number(42.0)]
    );

    runtime.stop_plugin("lib")?;

    let error = eval_as(&runtime, "app", b"return require('@lib').answer")
        .expect_err("lib's run ended, so its cached exports aren't served");
    assert!(
        format!("{error:#}").contains("plugin 'lib' is not running"),
        "{error:#}"
    );
    Ok(())
}

#[test]
fn a_restarted_library_plugin_can_still_be_required() -> anyhow::Result<()> {
    let mut loader = MemorySourceLoader::new();
    loader.insert("plugin:lib/util", b"return 7".as_slice());
    let mut library = manifest("lib", &[]);
    library.entrypoint = None;
    let mut app = manifest("app", &[]);
    app.dependencies = dependencies(&[("lib", dependency("1"))]);
    let plugins = [library, app].into_iter().map(loaded).collect();
    loader.insert("plugin:app/init", b"".as_slice());
    let runtime = task_test_runtime_from(Arc::new(Mutex::new(Vec::new())), loader, plugins)?;
    assert!(runtime.exec_all().is_empty());

    runtime.restart_plugin("lib")?;

    assert_eq!(
        eval_as(&runtime, "app", b"return require('@lib/util')")?,
        [luau::Value::Number(7.0)]
    );
    Ok(())
}

#[test]
fn tasks_a_live_export_spawns_stop_when_its_plugin_restarts() -> anyhow::Result<()> {
    let runtime = StagedRuntime::new(
        &[
            ("lib", &[], &lib_exporting(&["later"])),
            (
                "app",
                &["lib"],
                r#"
                    local tasks = require('@test/tasks')
                    require('@lib').later(0.05, function()
                        tasks.record("through lib")
                    end)
                    tasks.later(0.05, function()
                        tasks.record("app")
                    end)
                "#,
            ),
        ],
        &[],
    )?;
    assert!(runtime.exec_all().is_empty());
    assert_eq!(leftover_tasks(&runtime)?.len(), 2, "both tasks wait");

    runtime.restart_plugin("lib")?;
    assert_eq!(leftover_tasks(&runtime)?.len(), 1);

    runtime.advance(LATER);
    assert_eq!(runtime.recorded(), ["app"]);
    Ok(())
}

#[test]
fn ending_either_plugin_cancels_an_async_call_between_them() -> anyhow::Result<()> {
    // Stopping the callee, and restarting the caller while it awaits.
    for (callee, ended) in [("forever", "lib"), ("soon", "app")] {
        let app = format!(
            r#"
                local tasks = require('@test/tasks')
                local lib = require('@lib')
                tasks.register(function()
                    lib.{callee}()
                    tasks.record("resumed")
                end)
            "#
        );
        let runtime = StagedRuntime::new(
            &[
                ("lib", &[], &lib_exporting(&[callee])),
                ("app", &["lib"], &app),
            ],
            &[],
        )?;
        assert!(runtime.exec_all().is_empty());
        let call = call_handler(&runtime, 0)?;
        runtime.run_ready();
        assert!(completion_result(&call).is_none(), "{ended}");
        assert_eq!(
            leftover_tasks(&runtime)?.len(),
            1,
            "{ended}: the call waits"
        );

        if ended == "lib" {
            runtime.stop_plugin("lib")?;
        } else {
            runtime.restart_plugin("app")?;
        }

        let error = completion_result(&call)
            .unwrap_or_else(|| panic!("the call must finish when {ended} ends"))
            .expect_err("the call must fail");
        assert!(error.contains("cancelled"), "{ended}: {error}");
        assert!(leftover_tasks(&runtime)?.is_empty(), "{ended}");
        runtime.advance(SOON);
        assert!(runtime.recorded().is_empty(), "{ended}");
    }
    Ok(())
}

#[test]
fn a_kept_wrapper_cannot_require_for_a_restarted_plugin() -> anyhow::Result<()> {
    let runtime = StagedRuntime::new(
        &[
            (
                "lib",
                &[],
                r#"
                    return {
                        lazy = function()
                            return require('./late')
                        end,
                    }
                "#,
            ),
            (
                "app",
                &["lib"],
                r#"
                    kept_lazy = require('@lib').lazy
                "#,
            ),
        ],
        &[(
            "plugin:lib/late",
            r#"
                local tasks = require('@test/tasks')
                tasks.later(0.05, function()
                    tasks.record("late")
                end)
                return { check = tasks.check() }
            "#,
        )],
    )?;
    assert!(runtime.exec_all().is_empty());

    runtime.restart_plugin("lib")?;
    let error = runtime
        .eval_as("app", b"return kept_lazy()")
        .expect_err("a wrapper from lib's ended run must not load lib's modules");
    assert!(
        error.to_string().contains("run that has ended"),
        "{error:#}"
    );
    assert!(
        leftover_tasks(&runtime)?.is_empty(),
        "the refused wrapper loaded the module"
    );

    assert_eq!(
        runtime.eval_as("app", b"return require('@lib').lazy().check")?,
        vec![luau::Value::Nil],
        "the current run's wrapper loads modules that act for that run"
    );
    assert!(runtime.recorded().is_empty());
    runtime.advance(LATER);
    assert_eq!(runtime.recorded(), ["late"]);
    Ok(())
}

#[test]
fn app_code_run_on_another_plugin_s_thread_has_no_require_of_that_plugin() -> anyhow::Result<()> {
    let runtime = StagedRuntime::new(
        &[
            (
                "lib",
                &[],
                r#"
                    local tasks = require('@test/tasks')
                    local kept
                    tasks.register(function()
                        return kept()
                    end)
                    return {
                        keep = function(callback)
                            kept = callback
                        end,
                    }
                "#,
            ),
            (
                "app",
                &["lib"],
                r#"
                    local tasks = require('@test/tasks')
                    require('@lib').keep(function()
                        tasks.record("getfenv:" .. tostring(getfenv))
                        tasks.record("capability:" .. tostring((pcall(require, "@test/secret"))))
                    end)
                "#,
            ),
        ],
        &[],
    )?;
    assert!(runtime.exec_all().is_empty());

    let call = call_handler(&runtime, 0)?;
    runtime.run_ready();
    assert_eq!(runtime.recorded(), ["getfenv:nil", "capability:false"]);
    assert!(matches!(completion_result(&call), Some(Ok(_))));
    Ok(())
}

#[test]
fn a_stale_async_export_fails_its_caller_at_once() -> anyhow::Result<()> {
    let runtime = StagedRuntime::new(
        &[
            ("lib", &[], &lib_exporting(&["soon"])),
            (
                "app",
                &["lib"],
                r#"
                    local tasks = require('@test/tasks')
                    local lib = require('@lib')
                    tasks.register(function()
                        lib.soon()
                        tasks.record("resumed")
                    end)
                "#,
            ),
        ],
        &[],
    )?;
    assert!(runtime.exec_all().is_empty());
    runtime.restart_plugin("lib")?;

    let call = call_handler(&runtime, 0)?;
    // With no time passing, so not once the export's wait would have ended.
    runtime.run_ready();
    let error = completion_result(&call)
        .expect("a call through a stale async export must finish")
        .expect_err("a call through a stale async export must fail");
    assert!(error.contains("run that has ended"), "{error}");
    assert!(leftover_tasks(&runtime)?.is_empty());
    runtime.advance(SOON);
    assert!(runtime.recorded().is_empty());
    Ok(())
}

#[test]
fn async_calls_where_luau_cannot_yield_fail_and_schedule_nothing() -> anyhow::Result<()> {
    // A `table.sort` comparator, and a module's top level during `require`.
    let runtime = StagedRuntime::new(
        &[
            ("lib", &[], &lib_exporting(&["eventually"])),
            (
                "app",
                &["lib"],
                r#"
                    local tasks = require('@test/tasks')
                    local lib = require('@lib')
                    tasks.record("require:" .. tostring((pcall(require, "./helper"))))
                    tasks.register(function()
                        local ok, error = pcall(function()
                            table.sort({ 3, 1, 2 }, function(a, b)
                                lib.eventually("sorted")
                                return a < b
                            end)
                        end)
                        tasks.record("sort:" .. tostring(ok))
                        assert(string.find(error, "non-yieldable", 1, true), error)
                        return "done"
                    end)
                "#,
            ),
        ],
        &[(
            "plugin:app/helper",
            r#"
                require('@lib').eventually("module")
                return 1
            "#,
        )],
    )?;
    assert!(runtime.exec_all().is_empty());
    let call = call_handler(&runtime, 0)?;
    runtime.run_ready();

    assert_eq!(
        completion_result(&call).expect("the call finishes"),
        Ok(vec![luau::Value::String(b"done".to_vec())])
    );
    assert!(leftover_tasks(&runtime)?.is_empty());
    assert_eq!(runtime.next_delay(), None, "a refused call still waits");
    runtime.advance(SOON);
    assert_eq!(runtime.recorded(), ["require:false", "sort:false"]);
    Ok(())
}

#[test]
fn a_coroutine_keeps_its_context_across_scheduler_resumes() -> anyhow::Result<()> {
    let runtime = StagedRuntime::new(
        &[
            ("lib", &[], &lib_exporting(&["soon"])),
            (
                "app",
                &["lib"],
                r#"
                    local tasks = require('@test/tasks')
                    local lib = require('@lib')
                    local co = coroutine.create(function()
                        tasks.record("before:" .. tostring(tasks.thread_plugin()))
                        lib.soon()
                        tasks.record("resumed:" .. tostring(tasks.thread_plugin()))
                        tasks.record("getfenv:" .. tostring(getfenv))
                        tasks.record("private:" .. tostring((pcall(require, "./secret"))))
                        tasks.record("capability:" .. tostring((pcall(require, "@test/secret"))))
                        coroutine.yield()
                        tasks.record("manual:" .. tostring(tasks.thread_plugin()))
                        lib.soon()
                        tasks.record("again:" .. tostring(tasks.thread_plugin()))
                    end)
                    coroutine.resume(co)
                    tasks.register(function()
                        tasks.record("handler:" .. coroutine.status(co))
                        coroutine.resume(co)
                    end)
                "#,
            ),
        ],
        &[("plugin:lib/secret", r#"return "lib secret""#)],
    )?;
    assert!(runtime.exec_all().is_empty());
    assert_eq!(runtime.recorded(), ["before:app"]);
    assert_eq!(runtime.next_delay(), Some(SOON), "the coroutine waits");

    runtime.advance(SOON);
    assert_eq!(
        runtime.recorded()[1..],
        [
            "resumed:app",
            "getfenv:nil",
            "private:false",
            "capability:false"
        ]
    );
    assert!(
        leftover_tasks(&runtime)?.is_empty(),
        "a plain yield hands the coroutine back to the code that resumes it"
    );

    let call = call_handler(&runtime, 0)?;
    runtime.run_ready();
    assert_eq!(runtime.recorded()[5..], ["handler:suspended", "manual:app"]);
    assert_eq!(
        runtime.next_delay(),
        Some(SOON),
        "the coroutine waits again"
    );
    assert!(matches!(completion_result(&call), Some(Ok(_))));

    runtime.advance(SOON);
    assert_eq!(runtime.recorded()[7..], ["again:app"]);
    assert!(leftover_tasks(&runtime)?.is_empty());
    Ok(())
}

#[test]
fn eval_sources_run_as_their_own_chunk() -> anyhow::Result<()> {
    let runtime = task_test_runtime_with(
        Arc::new(Mutex::new(Vec::new())),
        &[("lib", &[], "return {}")],
        &[],
    )?;
    assert!(runtime.exec_all().is_empty());

    let escape = eval_as(
        &runtime,
        "lib",
        b"end end and (function() error('escaped') end)() and function(require) return function(...) return 1",
    )
    .expect_err("source can't close a wrapper around it");
    assert!(!escape.to_string().contains("escaped"), "{escape:#}");

    let error =
        eval_as(&runtime, "lib", b"local x = 1\nerror('boom')").expect_err("the error propagates");
    assert!(error.to_string().contains(":2:"), "{error:#}");
    Ok(())
}

#[test]
fn failed_entrypoint_ends_its_run() -> anyhow::Result<()> {
    let runtime = StagedRuntime::new(
        &[(
            "demo",
            &[],
            r#"
                local tasks = require('@test/tasks')
                tasks.later(0.05, function()
                    tasks.record("demo")
                end)
                error("entrypoint failed")
            "#,
        )],
        &[],
    )?;

    assert_eq!(runtime.exec_all(), ["demo"]);
    assert!(
        leftover_tasks(&runtime)?.is_empty(),
        "the failed run's task is left waiting"
    );
    runtime.advance(LATER);
    assert!(runtime.recorded().is_empty());
    Ok(())
}

#[test]
fn a_require_cannot_start_a_plugin_run() -> anyhow::Result<()> {
    let runtime = StagedRuntime::new(
        &[
            (
                "lib",
                &[],
                r#"
                    require('@test/tasks').record("lib")
                    error("entrypoint failed")
                "#,
            ),
            ("app", &["lib"], "require('@lib')"),
        ],
        &[(
            "plugin:lib/extra",
            r#"require('@test/tasks').record("extra")"#,
        )],
    )?;
    let assert_not_running = |error: anyhow::Error| {
        assert!(
            format!("{error:#}").contains("plugin 'lib' is not running"),
            "{error:#}"
        );
    };

    assert_not_running(
        runtime
            .eval_as("app", b"require('@lib')")
            .expect_err("lib has no run until the host starts one"),
    );
    runtime.run_ready();
    assert!(runtime.recorded().is_empty());
    assert!(leftover_tasks(&runtime)?.is_empty());

    assert_eq!(runtime.exec_all(), ["lib", "app"]);
    assert_not_running(
        runtime
            .eval_as("app", b"require('@lib/extra')")
            .expect_err("lib's run ended, so requiring its code fails"),
    );
    runtime.run_ready();
    assert_eq!(runtime.recorded(), ["lib"]);
    Ok(())
}

#[test]
fn exports_left_by_a_restarted_plugin_cannot_act_for_it() -> anyhow::Result<()> {
    let runtime = task_test_runtime_with(
        Arc::new(Mutex::new(Vec::new())),
        &[
            (
                "lib",
                &[],
                r#"
                    local tasks = require('@test/tasks')
                    own_check = tasks.check()
                    return { check = tasks.check }
                "#,
            ),
            (
                "app",
                &["lib"],
                r#"
                    kept_check = require('@lib').check
                "#,
            ),
        ],
        &[],
    )?;
    assert!(runtime.exec_all().is_empty());
    assert_eq!(
        eval_as(&runtime, "lib", b"return own_check")?,
        vec![luau::Value::Nil]
    );
    assert_eq!(
        eval_as(&runtime, "app", b"return kept_check()")?,
        vec![luau::Value::Nil],
        "a live export acts as its plugin's current run"
    );

    runtime.restart_plugin("lib")?;
    let values = eval_as(&runtime, "app", b"return kept_check()")?;
    let [luau::Value::String(error)] = values.as_slice() else {
        panic!("a stale export must be refused, got {values:?}");
    };
    assert!(
        String::from_utf8_lossy(error).contains("run that has ended"),
        "{}",
        String::from_utf8_lossy(error)
    );
    assert_eq!(
        eval_as(&runtime, "lib", b"return own_check")?,
        vec![luau::Value::Nil]
    );
    Ok(())
}

#[test]
fn restart_plugin_cancels_handler_calls_of_the_previous_run() -> anyhow::Result<()> {
    let runtime = StagedRuntime::numbered_runs()?;
    assert!(runtime.exec_all().is_empty());
    let demo_call = call_handler(&runtime, 0)?;
    let other_call = call_handler(&runtime, 1)?;
    runtime.run_ready();
    assert!(completion_result(&demo_call).is_none());

    runtime.restart_plugin("demo")?;

    let error = completion_result(&demo_call)
        .expect("in-flight call must finish when its plugin restarts")
        .expect_err("in-flight call must fail");
    assert!(error.contains("cancelled"), "{error}");
    let late_call = call_handler(&runtime, 0)?;
    runtime.run_ready();
    let error = completion_result(&late_call)
        .expect("a call into the previous run must finish")
        .expect_err("a call into the previous run must fail");
    assert!(error.contains("cancelled"), "{error}");
    assert!(completion_result(&other_call).is_none());
    Ok(())
}

#[test]
fn runtime_builder_enforces_manifest_capability_policy() -> anyhow::Result<()> {
    let runtime = RuntimeBuilder::new(
        MemorySourceLoader::new(),
        manifest_arc(vec![manifest("demo", &[])]),
    )
    .modules(vec![GatedModule::new(
        "demo.secret",
        ModuleSpec::new("demo/secret").function(FunctionSpec::new("value").call(|mut frame| {
            frame.returns.write("secret")?;
            Ok(())
        })),
    )])
    .build()?;

    let error = runtime
        .eval_source_with_context(
            Arc::<[u8]>::from(&b"return require('@demo/secret').value()"[..]),
            CallContext {
                origin: ChunkOrigin {
                    plugin: Some(Arc::from("demo")),
                    ..ChunkOrigin::default()
                },
                ..CallContext::default()
            },
        )
        .expect_err("capability should be denied");

    assert!(error.to_string().contains("demo.secret"));
    Ok(())
}

#[test]
fn runtime_builder_registers_native_modules() -> anyhow::Result<()> {
    let runtime = RuntimeBuilder::new(
        MemorySourceLoader::new(),
        manifest_arc(vec![manifest("demo", &["demo.math"])]),
    )
    .modules(vec![GatedModule::new(
        "demo.math",
        ModuleSpec::new("demo/math").function(FunctionSpec::new("answer").call(|mut frame| {
            frame.returns.write(42_i64)?;
            Ok(())
        })),
    )])
    .build()?;

    let values = runtime.eval_source_with_context(
        Arc::<[u8]>::from(&b"return require('@demo/math').answer()"[..]),
        CallContext {
            origin: ChunkOrigin {
                plugin: Some(Arc::from("demo")),
                ..ChunkOrigin::default()
            },
            ..CallContext::default()
        },
    )?;

    assert_eq!(values, vec![luau::Value::Number(42.0)]);
    Ok(())
}

#[test]
fn runtime_builder_configure_vm_inserts_host_data() -> anyhow::Result<()> {
    let runtime = RuntimeBuilder::new(
        MemorySourceLoader::new(),
        manifest_arc(vec![manifest("demo", &["demo.store"])]),
    )
    .configure_vm(|vm| {
        vm.data().insert(TestStore("configured"))?;
        Ok(())
    })
    .modules(vec![GatedModule::new(
        "demo.store",
        ModuleSpec::new("demo/store").function(FunctionSpec::new("value").call(|mut frame| {
            let store = frame.vm.data().get::<TestStore>()?;
            frame.returns.write(store.0)?;
            Ok(())
        })),
    )])
    .build()?;

    let values = runtime.eval_source_with_context(
        Arc::<[u8]>::from(&b"return require('@demo/store').value()"[..]),
        CallContext {
            origin: ChunkOrigin {
                plugin: Some(Arc::from("demo")),
                ..ChunkOrigin::default()
            },
            ..CallContext::default()
        },
    )?;

    assert_eq!(values, vec![luau::Value::String(b"configured".to_vec())]);
    Ok(())
}

#[test]
fn runtime_eval_source_returns_values_and_propagates_errors() -> anyhow::Result<()> {
    let runtime = RuntimeBuilder::new(MemorySourceLoader::new(), manifest_arc(Vec::new()))
        .global_specs(vec![GlobalSpec::new("test/noop")])
        .build()?;

    let values = runtime.eval_source_with_context(
        Arc::<[u8]>::from(&b"return 'ok'"[..]),
        CallContext::default(),
    )?;
    assert_eq!(values, vec![luau::Value::String(b"ok".to_vec())]);

    let error = runtime
        .eval_source_with_context(
            Arc::<[u8]>::from(&b"error('source failed')"[..]),
            CallContext::default(),
        )
        .expect_err("source should fail");
    assert!(error.to_string().contains("source failed"));
    Ok(())
}

#[test]
fn runtime_exec_all_runs_loaded_plugins_in_runtime_order() -> anyhow::Result<()> {
    let order = Arc::new(Mutex::new(Vec::<String>::new()));
    let order_for_module = order.clone();
    let module = ModuleSpec::new("test/order")
        .function(FunctionSpec::new("record").call(move |mut frame| {
            let id: String = frame.args.read_named("id")?;
            order_for_module
                .lock()
                .expect("order mutex poisoned")
                .push(id);
            Ok(())
        }))
        .install(|_| Ok(ModuleExport::new(())));

    let mut loader = MemorySourceLoader::new();
    loader.insert(
        "plugin:alpha/init",
        b"require('@test/order').record('alpha')".as_slice(),
    );
    loader.insert(
        "plugin:beta/init",
        b"require('@test/order').record('beta')".as_slice(),
    );

    let alpha = LoadedPlugin {
        manifest: manifest("alpha", &["test.order"]),
        directory: PathBuf::from("plugins/alpha"),
        declared_scopes: HashSet::new(),
    };
    let beta = LoadedPlugin {
        manifest: manifest("beta", &["test.order"]),
        directory: PathBuf::from("plugins/beta"),
        declared_scopes: HashSet::new(),
    };
    let runtime = RuntimeBuilder::new(
        loader,
        manifest_arc(vec![alpha.manifest.clone(), beta.manifest.clone()]),
    )
    .plugins(Arc::from(vec![alpha, beta]))
    .modules(vec![GatedModule::new("test.order", module)])
    .build()?;

    assert!(runtime.exec_all().is_empty());

    assert_eq!(
        *order.lock().expect("order mutex poisoned"),
        vec!["alpha".to_string(), "beta".to_string()]
    );
    Ok(())
}

#[test]
fn optional_dependency_that_failed_to_load_cannot_be_required() -> anyhow::Result<()> {
    let plugins_dir = temp_plugin_dir("failed-optional");
    for (id, extra) in [
        (
            "consumer",
            r#","dependencies":{"helper":{"version":"1","optional":true}}"#,
        ),
        ("helper", ""),
    ] {
        let dir = plugins_dir.join(id);
        std::fs::create_dir_all(&dir)?;
        std::fs::write(dir.join("init.luau"), "")?;
        std::fs::write(
            dir.join(crate::plugin::PLUGIN_CONFIG_FILENAME),
            manifest_json(id, extra),
        )?;
    }
    std::fs::write(
        plugins_dir.join("helper").join("lib.luau"),
        "helper_ran = true\nreturn {}",
    )?;
    std::fs::write(
        plugins_dir
            .join("helper")
            .join(crate::plugin::PLUGIN_CONFIG_FILENAME),
        manifest_json("helper", "").replace(r#""host":"0.0.1""#, r#""host":"0.1""#),
    )?;

    let mut mgr = PluginManager::new(plugins_dir.clone());
    let errors = mgr.discover_plugins(&host())?;
    assert!(
        matches!(
            errors[..],
            [PluginDiscoveryError {
                error: PluginLoadError::IncompatibleHost { .. },
                ..
            }]
        ),
        "{errors:?}"
    );
    let plugins: Vec<_> = mgr.list_plugins().cloned().collect();
    let manifests = plugins
        .iter()
        .map(|plugin| plugin.manifest.clone())
        .collect();
    let runtime = RuntimeBuilder::new(
        crate::FilesystemSourceLoader::new(&plugins_dir, &plugins_dir),
        manifest_arc(manifests),
    )
    .plugins(Arc::from(plugins))
    .build()?;

    let error = runtime
        .eval_source_with_context(
            Arc::<[u8]>::from(&b"return require('@helper/lib')"[..]),
            CallContext {
                origin: plugin_origin("consumer", "init.luau"),
                ..CallContext::default()
            },
        )
        .expect_err("a dependency that failed to load must not be requirable");
    assert!(
        error.to_string().contains("which is not a loaded plugin"),
        "{error}"
    );
    let ran = runtime.eval_source_with_context(
        Arc::<[u8]>::from(&b"return helper_ran"[..]),
        CallContext::default(),
    )?;
    assert_eq!(ran, vec![luau::Value::Nil]);
    let _ = std::fs::remove_dir_all(&plugins_dir);
    Ok(())
}

#[test]
fn cross_plugin_require_needs_declared_dependency() -> anyhow::Result<()> {
    let mut loader = MemorySourceLoader::new();
    loader.insert("plugin:lib/init", b"return { answer = 42 }".as_slice());

    let mut lib = manifest("lib", &[]);
    lib.entrypoint = None;
    let mut consumer = manifest("consumer", &[]);
    consumer.dependencies = dependencies(&[("lib", dependency("1"))]);
    let outsider = manifest("outsider", &[]);

    let library_plugin = LoadedPlugin {
        manifest: lib.clone(),
        directory: PathBuf::from("plugins/lib"),
        declared_scopes: HashSet::new(),
    };
    let runtime = RuntimeBuilder::new(loader, manifest_arc(vec![lib, consumer, outsider]))
        .plugins(Arc::from(vec![library_plugin]))
        .build()?;
    // Library plugins have no entrypoint; exec_all must skip them.
    assert!(runtime.exec_all().is_empty());

    let values = runtime.eval_source_with_context(
        Arc::<[u8]>::from(&b"return require('@lib').answer"[..]),
        CallContext {
            origin: plugin_origin("consumer", "init.luau"),
            ..CallContext::default()
        },
    )?;
    assert_eq!(values, vec![luau::Value::Number(42.0)]);

    let error = runtime
        .eval_source_with_context(
            Arc::<[u8]>::from(&b"return require('@lib').answer"[..]),
            CallContext {
                origin: plugin_origin("outsider", "init.luau"),
                ..CallContext::default()
            },
        )
        .expect_err("undeclared dependency must be denied");
    assert!(
        error
            .to_string()
            .contains("which is not a loaded plugin it declares as a dependency"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn require_of_executed_plugin_returns_entrypoint_exports_without_rerunning() -> anyhow::Result<()> {
    let runs = Arc::new(Mutex::new(0usize));
    let runs_for_module = runs.clone();
    let module = ModuleSpec::new("test/order")
        .function(FunctionSpec::new("record").call(move |_frame| {
            *runs_for_module.lock().expect("runs mutex poisoned") += 1;
            Ok(())
        }))
        .install(|_| Ok(ModuleExport::new(())));

    let mut loader = MemorySourceLoader::new();
    loader.insert(
        "plugin:dep/init",
        b"require('@test/order').record()\nreturn { answer = 7 }".as_slice(),
    );

    let dep = manifest("dep", &["test.order"]);
    let mut consumer = manifest("consumer", &[]);
    consumer.dependencies = dependencies(&[("dep", dependency("1"))]);

    let dep_plugin = LoadedPlugin {
        manifest: dep.clone(),
        directory: PathBuf::from("plugins/dep"),
        declared_scopes: HashSet::new(),
    };
    let runtime = RuntimeBuilder::new(loader, manifest_arc(vec![dep, consumer]))
        .plugins(Arc::from(vec![dep_plugin]))
        .modules(vec![GatedModule::new("test.order", module)])
        .build()?;
    assert!(runtime.exec_all().is_empty());

    let values = runtime.eval_source_with_context(
        Arc::<[u8]>::from(&b"return require('@dep').answer"[..]),
        CallContext {
            origin: plugin_origin("consumer", "init.luau"),
            ..CallContext::default()
        },
    )?;

    assert_eq!(values, vec![luau::Value::Number(7.0)]);
    assert_eq!(
        *runs.lock().expect("runs mutex poisoned"),
        1,
        "entrypoint side effects must not re-run on require"
    );
    Ok(())
}

#[test]
fn require_in_registered_namespace_fails_as_unknown_module() -> anyhow::Result<()> {
    let mut loader = MemorySourceLoader::new();
    // Even a plugin named like a registered namespace must not resolve.
    loader.insert("plugin:test/x", b"return 1".as_slice());
    let runtime = RuntimeBuilder::new(loader, manifest_arc(Vec::new()))
        .modules(vec![GatedModule::new(
            "test.order",
            ModuleSpec::new("test/order").install(|_| Ok(ModuleExport::new(()))),
        )])
        .build()?;

    let error = runtime
        .eval_source_with_context(
            Arc::<[u8]>::from(&b"return require('@test/x')"[..]),
            CallContext::default(),
        )
        .expect_err("registered namespaces must not fall through to plugin sources");
    assert!(
        error.to_string().contains("is not registered"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn require_cycle_fails_fast() -> anyhow::Result<()> {
    let mut loader = MemorySourceLoader::new();
    loader.insert("plugin:demo/a", b"return require('@self/b')".as_slice());
    loader.insert("plugin:demo/b", b"return require('@self/a')".as_slice());
    let runtime = RuntimeBuilder::new(loader, manifest_arc(vec![manifest("demo", &[])])).build()?;

    let error = runtime
        .eval_source_with_context(
            Arc::<[u8]>::from(&b"return require('@self/a')"[..]),
            CallContext {
                origin: plugin_origin("demo", "init.luau"),
                ..CallContext::default()
            },
        )
        .expect_err("require cycles must fail fast");
    assert!(
        error.to_string().contains("require cycle"),
        "unexpected error: {error}"
    );
    Ok(())
}
