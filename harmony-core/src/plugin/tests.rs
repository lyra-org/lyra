// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::discovery::execution_order;
use super::runtime::plugin_origin;
use super::{
    Dependency,
    LoadedManifest,
    LoadedPlugin,
    PluginDiscoveryError,
    PluginHost,
    PluginLoadError,
    PluginManager,
    PluginManifest,
    RuntimeBuilder,
    validate_plugin_id,
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
    scheduler::tests::ending_without_a_missed_wake,
    testing::ManualClock,
};
use harmony_luau as luau;
use semver::{
    Version,
    VersionReq,
};
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

fn manifest(id: &str, scopes: &[&str]) -> PluginManifest {
    PluginManifest {
        schema_version: super::PLUGIN_SCHEMA_VERSION,
        id: id.to_string(),
        name: id.to_string(),
        version: Version::new(1, 0, 0),
        host_requirement: VersionReq::STAR,
        description: String::new(),
        entrypoint: Some("init.luau".to_string()),
        scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
        scope_reasons: BTreeMap::new(),
        scope_grants: BTreeMap::new(),
        dependencies: BTreeMap::new(),
    }
}

fn dependency(requirement: &str) -> Dependency {
    Dependency {
        version: VersionReq::parse(requirement).unwrap(),
        optional: false,
    }
}

fn optional(requirement: &str) -> Dependency {
    Dependency {
        optional: true,
        ..dependency(requirement)
    }
}

fn dependencies(entries: &[(&str, Dependency)]) -> BTreeMap<String, Dependency> {
    entries
        .iter()
        .map(|(id, dependency)| (id.to_string(), dependency.clone()))
        .collect()
}

fn manifest_arc(manifests: Vec<PluginManifest>) -> Arc<[PluginManifest]> {
    Arc::from(manifests)
}

/// `manifest` as discovery would load it from `plugins/<id>`.
fn loaded(manifest: PluginManifest) -> LoadedPlugin {
    LoadedPlugin {
        directory: PathBuf::from(format!("plugins/{}", manifest.id)),
        manifest,
        declared_scopes: HashSet::new(),
    }
}

#[test]
fn accepts_well_formed_ids() {
    for id in ["demo", "Demo_Plugin", "my-plugin", "plugin123", "a"] {
        assert!(validate_plugin_id(id).is_ok(), "rejected {id:?}");
    }
}

#[test]
fn rejects_empty() {
    assert!(validate_plugin_id("").is_err());
}

#[test]
fn rejects_slash() {
    assert!(validate_plugin_id("foo/bar").is_err());
}

#[test]
fn rejects_whitespace_dots_and_other_symbols() {
    for id in [
        "foo bar", "foo.bar", "foo:bar", "foo@bar", "foo/..", "foo\\bar",
    ] {
        assert!(validate_plugin_id(id).is_err(), "accepted {id:?}");
    }
}

fn parse_dependencies(json: &str) -> serde_json::Result<BTreeMap<String, Dependency>> {
    super::manifest::deserialize_dependencies(&mut serde_json::Deserializer::from_str(json))
}

#[test]
fn parses_dependency_short_and_long_forms() {
    let json =
        r#"{"a": "0.2", "b": {"version": "^1.4"}, "c": {"version": "=0.0.3", "optional": true}}"#;
    assert_eq!(
        parse_dependencies(json).unwrap(),
        dependencies(&[
            ("a", dependency("0.2")),
            ("b", dependency("^1.4")),
            ("c", optional("=0.0.3")),
        ])
    );
}

#[test]
fn rejects_malformed_dependencies() {
    for (json, expected) in [
        (
            r#"{"a": "nope"}"#,
            "invalid dependency 'a' version requirement \"nope\"",
        ),
        (
            r#"{"a": "1", "b": {"version": "nope"}}"#,
            "invalid dependency 'b' version requirement \"nope\"",
        ),
        (
            r#"{"a": "1", "b": "1", "a": {"version": "2"}}"#,
            "dependency 'a' is listed more than once",
        ),
        (r#"{"a": {"optional": true}}"#, "missing field `version`"),
        (
            r#"{"a": {"version": "1", "required": false}}"#,
            "unknown field `required`",
        ),
        (r#"{"a": ["b"]}"#, "a version requirement or an object"),
        (r#"["a"]"#, "an object mapping plugin ids to dependencies"),
    ] {
        let error = parse_dependencies(json).unwrap_err();
        assert!(error.to_string().contains(expected), "{json}: {error}");
    }
}

fn loaded_plugin(
    id: &str,
    version: &str,
    dependencies: BTreeMap<String, Dependency>,
) -> LoadedPlugin {
    LoadedPlugin {
        manifest: PluginManifest {
            version: Version::parse(version).unwrap(),
            dependencies,
            ..manifest(id, &[])
        },
        directory: PathBuf::from(format!("plugins/{id}")),
        declared_scopes: HashSet::new(),
    }
}

fn manager_with(plugins: Vec<LoadedPlugin>) -> PluginManager {
    let mut mgr = PluginManager::new(PathBuf::from("plugins"));
    for plugin in plugins {
        mgr.by_root
            .insert(plugin.directory.clone(), plugin.manifest.id.clone());
        mgr.plugins.insert(plugin.manifest.id.clone(), plugin);
    }
    mgr
}

fn order(mgr: &PluginManager) -> Vec<String> {
    execution_order_ids(mgr.list_plugins().cloned().collect())
}

fn execution_order_ids(plugins: Vec<LoadedPlugin>) -> Vec<String> {
    execution_order(plugins)
        .expect("plugins have an execution order")
        .into_iter()
        .map(|plugin| plugin.manifest.id)
        .collect()
}

#[test]
fn execution_order_orders_after_every_installed_dependency() {
    let order = execution_order_ids(vec![
        loaded_plugin(
            "consumer",
            "1.0.0",
            dependencies(&[("a", dependency("1")), ("b", optional("1"))]),
        ),
        loaded_plugin("b", "1.0.0", BTreeMap::new()),
        loaded_plugin("a", "1.0.0", BTreeMap::new()),
    ]);
    assert_eq!(order, vec!["a", "b", "consumer"]);
}

#[test]
fn execution_order_is_stable_via_id_sort() {
    let order = execution_order_ids(vec![
        loaded_plugin("zeta", "1.0.0", BTreeMap::new()),
        loaded_plugin("alpha", "1.0.0", BTreeMap::new()),
        loaded_plugin("mu", "1.0.0", BTreeMap::new()),
    ]);
    assert_eq!(order, vec!["alpha", "mu", "zeta"]);
}

#[test]
fn execution_order_rejects_cycles() {
    let error = execution_order(vec![
        loaded_plugin("a", "1.0.0", dependencies(&[("b", dependency("1"))])),
        loaded_plugin("b", "1.0.0", dependencies(&[("a", dependency("1"))])),
    ])
    .expect_err("a cycle has no execution order");
    assert!(error.to_string().contains("cycle"), "{error}");
}

#[test]
fn resolve_prunes_plugins_with_missing_required_dep() {
    let mut mgr = manager_with(vec![loaded_plugin(
        "consumer",
        "1.0.0",
        dependencies(&[("missing", dependency("1"))]),
    )]);
    let errors = mgr.resolve_dependencies();
    assert_eq!(errors.len(), 1);
    assert_eq!(
        errors[0].to_string(),
        "plugin 'consumer' requires plugin 'missing' ^1, but it is not installed or failed \
         to load"
    );
    assert!(mgr.get_plugin("consumer").is_none());
}

#[test]
fn resolve_keeps_plugins_with_missing_optional_dep() {
    let mut mgr = manager_with(vec![loaded_plugin(
        "consumer",
        "1.0.0",
        dependencies(&[("missing", optional("1"))]),
    )]);
    let errors = mgr.resolve_dependencies();
    assert!(errors.is_empty());
    assert!(mgr.get_plugin("consumer").is_some());
}

#[test]
fn resolve_checks_installed_dependency_versions() {
    let mut mgr = manager_with(vec![
        loaded_plugin("lib", "0.2.5", BTreeMap::new()),
        loaded_plugin("extra", "0.0.4", BTreeMap::new()),
        loaded_plugin(
            "matching",
            "1.0.0",
            dependencies(&[("lib", dependency("0.2")), ("extra", optional("0.0.4"))]),
        ),
        loaded_plugin(
            "too_old",
            "1.0.0",
            dependencies(&[("lib", dependency("0.3"))]),
        ),
        loaded_plugin(
            "optional_mismatch",
            "1.0.0",
            dependencies(&[("extra", optional("0.0.3"))]),
        ),
    ]);
    let errors = mgr.resolve_dependencies();
    let messages: Vec<String> = errors.iter().map(ToString::to_string).collect();
    assert_eq!(
        messages,
        vec![
            "plugin 'optional_mismatch' requires plugin 'extra' ^0.0.3, but 0.0.4 is installed",
            "plugin 'too_old' requires plugin 'lib' ^0.3, but 0.2.5 is installed",
        ]
    );
    assert!(mgr.get_plugin("matching").is_some());
    assert!(mgr.get_plugin("too_old").is_none());
    assert!(mgr.get_plugin("optional_mismatch").is_none());
}

#[test]
fn resolve_cascades_pruning_through_chain() {
    // c → b → a with a missing: b is dropped, and c only when it
    // requires b.
    let mut mgr = manager_with(vec![
        loaded_plugin("c", "1.0.0", dependencies(&[("b", optional("1"))])),
        loaded_plugin("b", "1.0.0", dependencies(&[("a", dependency("1"))])),
    ]);
    let errors = mgr.resolve_dependencies();
    assert_eq!(errors.len(), 1);
    assert!(mgr.get_plugin("b").is_none());
    assert!(mgr.get_plugin("c").is_some());

    let mut mgr = manager_with(vec![
        loaded_plugin("c", "1.0.0", dependencies(&[("b", dependency("1"))])),
        loaded_plugin("b", "1.0.0", dependencies(&[("a", dependency("1"))])),
    ]);
    let errors = mgr.resolve_dependencies();
    let directories: Vec<_> = errors.iter().map(|error| &error.directories).collect();
    assert_eq!(directories, [&["b"], &["c"]]);
    assert!(mgr.get_plugin("b").is_none());
    assert!(mgr.get_plugin("c").is_none());
}

#[test]
fn resolve_prunes_cycles_and_rechecks_what_depends_on_them() {
    let mut mgr = manager_with(vec![
        loaded_plugin("a", "1.0.0", dependencies(&[("b", dependency("1"))])),
        loaded_plugin("b", "1.0.0", dependencies(&[("a", optional("1"))])),
        loaded_plugin("optional", "1.0.0", dependencies(&[("a", optional("1"))])),
        loaded_plugin("required", "1.0.0", dependencies(&[("a", dependency("1"))])),
    ]);
    let errors = mgr.resolve_dependencies();
    let messages: Vec<String> = errors.iter().map(ToString::to_string).collect();
    assert_eq!(
        messages,
        vec![
            r#"dependency cycle detected; the following plugins are in a cycle and were skipped: ["a", "b"]"#,
            "plugin 'required' requires plugin 'a' ^1, but it is not installed or failed to \
             load",
        ]
    );
    assert_eq!(order(&mgr), vec!["optional"]);
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

fn temp_plugin_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "harmony-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create temp plugin dir");
    dir
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
        plugin_dir.join(super::PLUGIN_CONFIG_FILENAME),
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
    let read_answer = |runtime: &super::Runtime| eval_as(runtime, "demo", b"return demo_answer");

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
        plugin_dir.join(super::PLUGIN_CONFIG_FILENAME),
        r#"{"schema_version":1,"id":"demo","name":"demo","version":"1.1.0","host":"*","description":"","entrypoint":"init.luau","scopes":[]}"#,
    )?;
    let error = runtime
        .verify_plugin_manifest("demo", &host())
        .expect_err("changed manifest must be rejected");
    assert!(error.to_string().contains("reload plugins"), "{error}");

    std::fs::write(
        plugin_dir.join(super::PLUGIN_CONFIG_FILENAME),
        "0".repeat(super::PLUGIN_CONFIG_MAX_BYTES as usize + 1),
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
                .get::<crate::LocalScheduler>()?
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
                .get::<crate::LocalScheduler>()?
                .schedule_luau_thread_after(
                    group_context(&frame),
                    std::time::Duration::from_secs_f64(seconds),
                    frame.vm.clone(),
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
                .vm
                .thread_call_context(&frame.thread)?
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
) -> anyhow::Result<super::Runtime> {
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
) -> anyhow::Result<super::Runtime> {
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
    runtime: super::Runtime,
    clock: ManualClock,
    recorded: Arc<Mutex<Vec<String>>>,
}

impl std::ops::Deref for StagedRuntime {
    type Target = super::Runtime;

    fn deref(&self) -> &super::Runtime {
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

    fn scheduler(&self) -> std::rc::Rc<crate::LocalScheduler> {
        self.runtime.scheduler().expect("the runtime's scheduler")
    }

    /// Runs `entrypoint` to its end, which it must reach without waiting.
    fn drive(&self, entrypoint: Option<super::runtime::RunningEntrypoint>) -> anyhow::Result<()> {
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

    /// As [`super::Runtime::exec_all`].
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

    /// As [`super::Runtime::restart_plugin`].
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
        scheduler.spawn_luau_thread(context, self.vm.clone(), thread.clone(), Vec::new());
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

fn leftover_tasks(runtime: &super::Runtime) -> anyhow::Result<Vec<crate::TaskSnapshot>> {
    let scheduler = runtime.vm.data().get::<crate::LocalScheduler>()?;
    scheduler.remove_finished();
    Ok(scheduler.snapshots())
}

/// Evaluates `source` as plugin code of `plugin`.
fn eval_as(
    runtime: &super::Runtime,
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
    runtime: &super::Runtime,
    index: usize,
) -> anyhow::Result<std::rc::Rc<crate::LocalLuauTaskCompletion>> {
    let (function, context) = runtime.vm.data().get::<TestHandlers>()?.0.borrow()[index].clone();
    let completion = std::rc::Rc::new(crate::LocalLuauTaskCompletion::default());
    let scheduler = runtime.vm.data().get::<crate::LocalScheduler>()?;
    scheduler.schedule_luau_thread_with_limit_and_completion(
        context,
        runtime.vm.clone(),
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

fn recording_entrypoint(recorded: Arc<Mutex<Vec<String>>>) -> anyhow::Result<super::Runtime> {
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
        scheduler.schedule_watcher(CallContext::default(), {
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
    runtime
        .scheduler()
        .schedule_watcher(CallContext::default(), {
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

fn manifest_json(id: &str, extra: &str) -> String {
    format!(
        r#"{{"schema_version":1,"id":"{id}","name":"Demo","version":"1.0.0","host":"0.0.1","description":"d","entrypoint":"init.luau","scopes":["net"]{extra}}}"#
    )
}

fn host() -> PluginHost {
    PluginHost::new(
        "host",
        Version::new(0, 0, 1),
        HashSet::from([Arc::<str>::from("net")]),
    )
    .unwrap()
}

fn load_manifest(bytes: &[u8]) -> Result<LoadedManifest, PluginLoadError> {
    LoadedManifest::load(std::path::Path::new("plugin.json"), bytes, &host())
}

#[test]
fn load_manifest_accepts_valid_manifests_without_a_directory() {
    let json = manifest_json(
        "demo",
        r#","dependencies":{"other":"0.2","extra":{"version":"1","optional":true}},"unknown":1"#,
    );
    let loaded = load_manifest(json.as_bytes()).unwrap();

    assert_eq!(loaded.manifest.id, "demo");
    assert_eq!(loaded.manifest.name, "Demo");
    assert_eq!(loaded.manifest.version, Version::new(1, 0, 0));
    assert_eq!(
        loaded.manifest.host_requirement,
        VersionReq::parse("0.0.1").unwrap()
    );
    assert_eq!(loaded.manifest.description, "d");
    assert_eq!(loaded.manifest.scopes, vec!["net"]);
    assert!(loaded.declared_scopes.contains("net"));
    assert_eq!(
        loaded.manifest.dependencies,
        dependencies(&[("other", dependency("0.2")), ("extra", optional("1"))])
    );
}

#[test]
fn load_manifest_rejects_a_narrow_scope_without_its_parent() {
    let host = PluginHost::new(
        "host",
        Version::new(0, 0, 1),
        HashSet::from([Arc::<str>::from("net"), Arc::<str>::from("net.all")]),
    )
    .unwrap()
    .with_scope_parents([("net.all", "net")]);
    let load = |scopes: &str| {
        let json = manifest_json("demo", "").replace(r#"["net"]"#, scopes);
        LoadedManifest::load(std::path::Path::new("plugin.json"), json.as_bytes(), &host)
    };

    assert!(load(r#"["net.all","net"]"#).is_ok());
    let error = load(r#"["net.all"]"#).unwrap_err();
    assert!(matches!(
        &error,
        PluginLoadError::ScopeWithoutParent { scope, parent, .. }
            if scope == "net.all" && parent == "net"
    ));
    assert_eq!(
        error.to_string(),
        "plugin 'demo' declares scope 'net.all' without 'net', the scope it narrows"
    );
}

fn load_scopes(scopes: serde_json::Value) -> Result<LoadedManifest, PluginLoadError> {
    let host = PluginHost::new(
        "host",
        Version::new(0, 0, 1),
        HashSet::from([
            Arc::<str>::from("net"),
            Arc::<str>::from("net.all"),
            Arc::<str>::from("dns"),
        ]),
    )
    .unwrap()
    .with_scope_parents([("net.all", "net")]);
    let json = manifest_json("demo", "").replace(r#"["net"]"#, &scopes.to_string());
    LoadedManifest::load(std::path::Path::new("plugin.json"), json.as_bytes(), &host)
}

fn scope_parse_error(scopes: serde_json::Value) -> String {
    match load_scopes(scopes) {
        Err(PluginLoadError::ConfigParseError { error, .. }) => error,
        other => panic!("expected a parse error: {other:?}"),
    }
}

#[test]
fn scopes_are_plain_ids_or_objects_with_a_reason_in_the_order_written() {
    let plain = load_scopes(serde_json::json!(["net", "dns"]))
        .unwrap()
        .manifest;
    assert_eq!(plain.scopes, ["net", "dns"]);
    assert!(plain.scope_reasons.is_empty());

    let objects = load_scopes(serde_json::json!([
        { "id": "net", "reason": "Fetches things." },
        { "id": "dns", "reason": "Resolves things." },
    ]))
    .unwrap();
    assert_eq!(objects.manifest.scopes, ["net", "dns"]);
    assert_eq!(objects.manifest.scope_reasons.len(), 2);
    assert!(objects.declared_scopes.contains("dns"));

    let mixed = load_scopes(serde_json::json!([
        "net.all",
        { "id": "dns", "reason": "Resolves things." },
        "net",
    ]))
    .unwrap()
    .manifest;
    assert_eq!(mixed.scopes, ["net.all", "dns", "net"]);
    assert_eq!(
        mixed.scope_reasons,
        BTreeMap::from([("dns".to_string(), "Resolves things.".to_string())])
    );
}

#[test]
fn scope_objects_hold_exactly_a_string_id_and_reason() {
    for (scopes, expected) in [
        (
            serde_json::json!([{ "id": "net", "reason": "r", "extra": 1 }]),
            "unknown field `extra`",
        ),
        (
            serde_json::json!([{ "id": "net" }]),
            "missing field `reason`",
        ),
        (serde_json::json!([{ "reason": "r" }]), "missing field `id`"),
        (
            serde_json::json!([{ "id": 1, "reason": "r" }]),
            "invalid type",
        ),
        (
            serde_json::json!([{ "id": "net", "reason": 1 }]),
            "invalid type",
        ),
        (
            serde_json::json!([{ "id": "net", "reason": null }]),
            "invalid type",
        ),
        (serde_json::json!([1]), "a scope id or an object"),
        (
            serde_json::json!(["net", ["dns"]]),
            "a scope id or an object",
        ),
        (serde_json::json!("net"), "invalid type"),
    ] {
        let error = scope_parse_error(scopes.clone());
        assert!(error.contains(expected), "{scopes}: {error}");
    }
    let missing = manifest_json("demo", "").replace(r#""scopes":["net"]"#, r#""z":0"#);
    assert!(matches!(
        load_manifest(missing.as_bytes()),
        Err(PluginLoadError::ConfigParseError { error, .. }) if error.contains("scopes")
    ));
}

#[test]
fn scope_reasons_are_short_single_lines_of_text() {
    let reason = |reason: &str| serde_json::json!([{ "id": "net", "reason": reason }]);
    for bad in [
        "",
        "   ",
        "first\nsecond",
        "tab\there",
        "bell\u{7}",
        "delete\u{7f}",
        "next line\u{85}",
        "line\u{2028}separator",
        "paragraph\u{2029}separator",
        "\u{202e}reversed",
        "\u{2067}isolated",
    ] {
        let error = scope_parse_error(reason(bad));
        assert!(
            error.contains("plugin 'demo' gives scope 'net' an invalid reason"),
            "{bad:?}: {error}"
        );
    }
    assert!(
        scope_parse_error(reason(&"x".repeat(201)))
            .ends_with("an invalid reason: it is longer than 200 characters")
    );

    for accepted in ["x".repeat(200), "é".repeat(200)] {
        let manifest = load_scopes(reason(&accepted)).unwrap().manifest;
        assert_eq!(manifest.scope_reasons["net"], accepted);
    }
}

/// Items are lowercase words, written as an array, in the key `words`.
#[derive(Debug)]
struct Words;

impl crate::scopes::ScopeGrant for Words {
    fn key(&self) -> &str {
        "words"
    }

    fn parse(&self, value: &serde_json::Value) -> Result<Vec<String>, String> {
        let words: Vec<String> = serde_json::from_value(value.clone())
            .map_err(|error| format!("it is not a list of words: {error}"))?;
        let mut items = std::collections::BTreeSet::new();
        for word in words {
            if word.is_empty() || !word.chars().all(|c| c.is_ascii_lowercase()) {
                return Err(format!("'{word}' is not a lowercase word"));
            }
            if !items.insert(word.clone()) {
                return Err(format!("'{word}' is listed more than once"));
            }
        }
        Ok(items.into_iter().collect())
    }

    fn covers(&self, granted: &[String], item: &str) -> bool {
        granted.iter().any(|word| word == item)
    }
}

fn load_granted_scopes(scopes: serde_json::Value) -> Result<LoadedManifest, PluginLoadError> {
    let host = PluginHost::new(
        "host",
        Version::new(0, 0, 1),
        HashSet::from([Arc::<str>::from("net"), Arc::<str>::from("dns")]),
    )
    .unwrap()
    .with_scope_grant("net", Arc::new(Words));
    let json = manifest_json("demo", "").replace(r#"["net"]"#, &scopes.to_string());
    LoadedManifest::load(std::path::Path::new("plugin.json"), json.as_bytes(), &host)
}

fn grant_parse_error(scopes: serde_json::Value) -> String {
    match load_granted_scopes(scopes) {
        Err(PluginLoadError::ConfigParseError { error, .. }) => error,
        other => panic!("expected a parse error: {other:?}"),
    }
}

#[test]
fn a_scope_entry_holds_the_grant_its_scope_takes() {
    let manifest = load_granted_scopes(serde_json::json!([
        "dns",
        { "id": "net", "reason": "Fetches things.", "words": ["b", "a"] },
    ]))
    .unwrap()
    .manifest;

    assert_eq!(manifest.scopes, ["dns", "net"]);
    assert_eq!(
        manifest.scope_grants,
        BTreeMap::from([("net".to_string(), vec!["a".to_string(), "b".to_string()])])
    );

    let empty = load_granted_scopes(
        serde_json::json!([{ "id": "net", "reason": "Encodes things.", "words": [] }]),
    )
    .unwrap()
    .manifest;
    assert_eq!(empty.scope_grants["net"], Vec::<String>::new());
}

#[test]
fn a_scope_entry_takes_no_key_but_its_scopes_grant() {
    for (scopes, expected) in [
        (
            serde_json::json!([{ "id": "net", "reason": "r", "words": [], "extra": 1 }]),
            "plugin 'demo' gives scope 'net' an unknown field `extra`",
        ),
        (
            serde_json::json!([{ "id": "dns", "reason": "r", "words": [] }]),
            "plugin 'demo' gives scope 'dns' an unknown field `words`",
        ),
        (
            serde_json::json!([{ "id": "net", "words": [] }]),
            "missing field `reason`",
        ),
        (
            serde_json::json!([{ "id": "net", "reason": "r", "words": "a" }]),
            "plugin 'demo' gives scope 'net' an invalid `words`: it is not a list of words",
        ),
        (
            serde_json::json!([{ "id": "net", "reason": "r", "words": ["A"] }]),
            "an invalid `words`: 'A' is not a lowercase word",
        ),
        (
            serde_json::json!([{ "id": "net", "reason": "r", "words": ["a", "a"] }]),
            "an invalid `words`: 'a' is listed more than once",
        ),
    ] {
        let error = grant_parse_error(scopes.clone());
        assert!(error.contains(expected), "{scopes}: {error}");
    }

    for (field, twice) in [
        ("id", r#"{"id":"net","id":"net","reason":"r","words":[]}"#),
        (
            "reason",
            r#"{"id":"net","reason":"r","reason":"r","words":[]}"#,
        ),
        (
            "words",
            r#"{"id":"net","reason":"r","words":[],"words":[]}"#,
        ),
    ] {
        let json = manifest_json("demo", "").replace(r#""net""#, twice);
        let error = match load_manifest(json.as_bytes()) {
            Err(PluginLoadError::ConfigParseError { error, .. }) => error,
            other => panic!("expected a parse error: {other:?}"),
        };
        assert!(
            error.contains(&format!("duplicate field `{field}`")),
            "{error}"
        );
    }
}

#[test]
fn a_grant_lists_no_more_items_than_its_cap() {
    let words = |count: usize| -> Vec<String> {
        (0..count)
            .map(|index| {
                let first = char::from(b'a' + (index / 26) as u8);
                let second = char::from(b'a' + (index % 26) as u8);
                format!("{first}{second}")
            })
            .collect()
    };
    let scopes = |count| serde_json::json!([{ "id": "net", "reason": "r", "words": words(count) }]);
    let cap = super::PLUGIN_CONFIG_MAX_SCOPE_GRANT_ITEMS;

    let manifest = load_granted_scopes(scopes(cap)).unwrap().manifest;
    assert_eq!(manifest.scope_grants["net"].len(), cap);
    assert!(
        grant_parse_error(scopes(cap + 1))
            .ends_with("an invalid `words`: it lists 33 items, exceeds cap of 32")
    );
}

#[test]
fn a_scope_without_its_required_grant_parses_but_does_not_load() {
    let object = serde_json::json!([{ "id": "net", "reason": "Fetches things." }]);
    for scopes in [serde_json::json!(["net"]), object.clone()] {
        let error = load_granted_scopes(scopes.clone()).unwrap_err();
        assert!(
            matches!(
                &error,
                PluginLoadError::MissingScopeGrant { plugin_id, scope, key }
                    if plugin_id == "demo" && scope == "net" && key == "words"
            ),
            "{scopes}: {error}"
        );
        assert_eq!(
            error.to_string(),
            "plugin 'demo' declares scope 'net' without `words`, which the scope requires"
        );
    }

    let host = PluginHost::new(
        "host",
        Version::new(0, 0, 1),
        HashSet::from([Arc::<str>::from("net")]),
    )
    .unwrap()
    .with_scope_grant("net", Arc::new(Words));
    let json = manifest_json("demo", "").replace(r#"["net"]"#, &object.to_string());
    let parsed = PluginManifest::from_json(&json, &host).expect("the manifest still parses");
    assert_eq!(parsed.scopes, ["net"]);
    assert!(parsed.scope_grants.is_empty());
}

#[test]
fn a_host_mismatch_is_reported_before_a_missing_grant() {
    let host = PluginHost::new(
        "host",
        Version::new(0, 0, 2),
        HashSet::from([Arc::<str>::from("net")]),
    )
    .unwrap()
    .with_scope_grant("net", Arc::new(Words));
    let error = LoadedManifest::load(
        std::path::Path::new("plugin.json"),
        manifest_json("demo", "").as_bytes(),
        &host,
    )
    .unwrap_err();
    assert!(matches!(error, PluginLoadError::IncompatibleHost { .. }));
}

#[test]
fn a_scope_may_be_listed_once_in_either_form() {
    let object = serde_json::json!({ "id": "net", "reason": "Fetches things." });
    for scopes in [
        serde_json::json!(["net", "net"]),
        serde_json::json!(["net", object]),
        serde_json::json!([object, "net"]),
        serde_json::json!([object, object]),
    ] {
        assert_eq!(
            scope_parse_error(scopes),
            "plugin 'demo' lists scope 'net' more than once"
        );
    }
}

#[test]
fn scope_objects_pass_the_same_checks_as_plain_ids() {
    let explained = |id: &str| serde_json::json!([{ "id": id, "reason": "Needs it." }]);
    assert!(matches!(
        load_scopes(explained("fs")),
        Err(PluginLoadError::UnknownScope { scope, .. }) if scope == "fs"
    ));
    assert!(matches!(
        load_scopes(explained("net.all")),
        Err(PluginLoadError::ScopeWithoutParent { scope, parent, .. })
            if scope == "net.all" && parent == "net"
    ));
    assert!(matches!(
        load_scopes(explained(
            &"s".repeat(super::manifest::PLUGIN_CONFIG_MAX_SCOPE_LEN + 1)
        )),
        Err(PluginLoadError::ScopeEntryTooLong { .. })
    ));
}

#[test]
fn load_manifest_rejects_manifest_level_errors() {
    let long_scope = format!(
        r#"{{"schema_version":1,"id":"demo","name":"d","version":"1.0.0","host":"*","description":"","scopes":["{}"]}}"#,
        "s".repeat(super::manifest::PLUGIN_CONFIG_MAX_SCOPE_LEN + 1)
    );
    let err = |bytes: &[u8]| load_manifest(bytes).unwrap_err();

    assert!(matches!(
        err(&vec![b' '; super::PLUGIN_CONFIG_MAX_BYTES as usize + 1]),
        PluginLoadError::ConfigTooLarge { .. }
    ));
    assert!(matches!(
        err(&[0xff, 0xfe]),
        PluginLoadError::ConfigParseError { error, .. }
            if error == "stream did not contain valid UTF-8"
    ));
    assert!(matches!(
        err(b"{"),
        PluginLoadError::ConfigParseError { .. }
    ));
    assert!(matches!(
        err(manifest_json("demo", "")
            .replace(r#""schema_version":1"#, r#""schema_version":2"#)
            .as_bytes()),
        PluginLoadError::InvalidSchemaVersion { version: 2, .. }
    ));
    assert!(matches!(
        err(manifest_json("not/valid", "").as_bytes()),
        PluginLoadError::InvalidPluginId { .. }
    ));
    assert!(matches!(
        err(long_scope.as_bytes()),
        PluginLoadError::ScopeEntryTooLong { .. }
    ));
    assert!(matches!(
        err(manifest_json("demo", "").replace(r#"["net"]"#, r#"["fs"]"#).as_bytes()),
        PluginLoadError::UnknownScope { scope, .. } if scope == "fs"
    ));
    assert!(matches!(
        err(manifest_json("demo", r#","dependencies":{"other":"1","other":"2"}"#).as_bytes()),
        PluginLoadError::ConfigParseError { error, .. }
            if error.starts_with("dependency 'other' is listed more than once")
    ));
    assert!(matches!(
        err(manifest_json("demo", r#","dependencies":{"demo":"1"}"#).as_bytes()),
        PluginLoadError::SelfDependency { .. }
    ));
    assert!(matches!(
        err(manifest_json("demo", r#","dependencies":{"not/valid":"1"}"#).as_bytes()),
        PluginLoadError::InvalidDependencyId { .. }
    ));
    let too_many = (0..=super::manifest::PLUGIN_CONFIG_MAX_DEPENDENCIES)
        .map(|i| format!(r#""dep{i}":"1""#))
        .collect::<Vec<_>>()
        .join(",");
    assert!(matches!(
        err(manifest_json("demo", &format!(r#","dependencies":{{{too_many}}}"#)).as_bytes()),
        PluginLoadError::TooManyDependencies { .. }
    ));
}

#[test]
fn load_manifest_parses_versions_and_checks_the_host() {
    let err = |json: String| load_manifest(json.as_bytes()).unwrap_err().to_string();

    for (from, to, expected) in [
        (
            r#""version":"1.0.0""#,
            r#""version":"1""#,
            r#"invalid version "1""#,
        ),
        (r#""version":"1.0.0","#, "", "missing field `version`"),
        (
            r#""host":"0.0.1""#,
            r#""host":"latest""#,
            r#"invalid host version requirement "latest""#,
        ),
        (r#""host":"0.0.1","#, "", "missing field `host`"),
    ] {
        let message = err(manifest_json("demo", "").replace(from, to));
        assert!(message.contains(expected), "{message}");
    }

    assert_eq!(
        err(manifest_json("demo", "").replace(r#""host":"0.0.1""#, r#""host":"0.0.2""#)),
        "plugin 'demo' requires host ^0.0.2, but this is host 0.0.1"
    );
    for requirement in ["0.0", ">=0.0.1, <0.1.0", "*"] {
        let json = manifest_json("demo", "")
            .replace(r#""host":"0.0.1""#, &format!(r#""host":"{requirement}""#));
        assert!(load_manifest(json.as_bytes()).is_ok(), "{requirement}");
    }
}

#[test]
fn load_manifest_reads_the_requirement_under_the_host_name() {
    let other = PluginHost::new(
        "other",
        Version::new(2, 0, 0),
        HashSet::from([Arc::<str>::from("net")]),
    )
    .unwrap();
    let load = |json: String| {
        LoadedManifest::load(std::path::Path::new("plugin.json"), json.as_bytes(), &other)
    };
    let named = |requirement: &str| manifest_json("demo", &format!(r#","other":"{requirement}""#));

    let loaded = load(named("2")).unwrap();
    assert_eq!(
        loaded.manifest.host_requirement,
        VersionReq::parse("2").unwrap()
    );
    assert_eq!(
        load(named("1")).unwrap_err().to_string(),
        "plugin 'demo' requires other ^1, but this is other 2.0.0"
    );
    let message = load(manifest_json("demo", "")).unwrap_err().to_string();
    assert!(message.contains("missing field `other`"), "{message}");
}

#[test]
fn load_manifest_rejects_a_host_requirement_named_twice() {
    let json =
        manifest_json("demo", "").replace(r#""host":"0.0.1""#, r#""host":"0.0.1","host":"0.0.2""#);

    let message = load_manifest(json.as_bytes()).unwrap_err().to_string();

    assert!(message.contains("duplicate field `host`"), "{message}");
}

#[test]
fn hosts_reserve_plugin_ids_at_manifest_and_directory_load() {
    let reserving = host().with_reserved_ids(["update"]);
    let json = manifest_json("update", "");

    assert!(load_manifest(json.as_bytes()).is_ok());
    let err = LoadedManifest::load(
        std::path::Path::new("plugin.json"),
        json.as_bytes(),
        &reserving,
    )
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "invalid plugin id 'update': 'update' is reserved by host"
    );

    let plugins_dir = temp_plugin_dir("reserved-ids");
    for id in ["update", "demo"] {
        let dir = plugins_dir.join(id);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("init.luau"), "").unwrap();
        std::fs::write(
            dir.join(super::PLUGIN_CONFIG_FILENAME),
            manifest_json(id, ""),
        )
        .unwrap();
    }
    let mut mgr = PluginManager::new(plugins_dir);
    let errors = mgr.discover_plugins(&reserving).unwrap();
    assert!(
        matches!(errors.as_slice(), [PluginDiscoveryError { directories, error: PluginLoadError::InvalidPluginId { plugin_id, .. } }] if plugin_id == "update" && directories == &["update"]),
        "{errors:?}"
    );
    assert_eq!(order(&mgr), vec!["demo"]);
    assert!(matches!(
        mgr.reload_plugin("update", &reserving),
        Err(PluginLoadError::InvalidPluginId { .. })
    ));
}

#[test]
fn host_names_must_not_shadow_fixed_fields() {
    for name in ["", "id", "version", "dependencies", "commit"] {
        assert_eq!(
            PluginHost::new(name, Version::new(0, 0, 1), HashSet::new()).unwrap_err(),
            super::InvalidHostName(name.to_string()),
            "{name:?}"
        );
    }
}

#[test]
fn missing_entrypoint_passes_the_manifest_but_fails_the_directory() {
    let dir = temp_plugin_dir("missing-entrypoint");
    let json = manifest_json("demo", "");
    std::fs::write(dir.join(super::PLUGIN_CONFIG_FILENAME), &json).unwrap();
    assert!(load_manifest(json.as_bytes()).is_ok());
    let err = PluginManager::load_plugin_dir(&dir, &host()).unwrap_err();
    assert!(
        matches!(&err, PluginLoadError::EntrypointNotFound { path, .. } if *path == dir.join("init.luau")),
        "{err}"
    );

    std::fs::write(dir.join("init.luau"), "").unwrap();
    let loaded = PluginManager::load_plugin_dir(&dir, &host()).unwrap();
    assert_eq!(loaded.directory, dir);
    assert!(loaded.declared_scopes.contains("net"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn load_plugin_dir_reports_config_errors_against_the_config_path() {
    let dir = temp_plugin_dir("config-errors");
    let config = dir.join(super::PLUGIN_CONFIG_FILENAME);

    let err = PluginManager::load_plugin_dir(&dir, &host()).unwrap_err();
    assert!(
        matches!(&err, PluginLoadError::ConfigNotFound(path) if *path == config),
        "{err}"
    );

    std::fs::write(&config, [0xff, 0xfe]).unwrap();
    let err = PluginManager::load_plugin_dir(&dir, &host()).unwrap_err();
    assert_eq!(
        err.to_string(),
        format!(
            "failed to parse {}: stream did not contain valid UTF-8",
            config.display()
        )
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn discover_and_reload_check_host_and_dependency_versions() {
    let plugins_dir = temp_plugin_dir("versions");
    let write = |id: &str, version: &str, extra: &str| {
        let dir = plugins_dir.join(id);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("init.luau"), "").unwrap();
        std::fs::write(
            dir.join(super::PLUGIN_CONFIG_FILENAME),
            manifest_json(id, extra).replace("1.0.0", version),
        )
        .unwrap();
    };
    write("lib", "0.1.2", "");
    write("consumer", "1.0.0", r#","dependencies":{"lib":"0.1"}"#);
    write("future", "1.0.0", "");
    std::fs::write(
        plugins_dir
            .join("future")
            .join(super::PLUGIN_CONFIG_FILENAME),
        manifest_json("future", "").replace(r#""host":"0.0.1""#, r#""host":"0.1""#),
    )
    .unwrap();

    let mut mgr = PluginManager::new(plugins_dir.clone());
    let errors = mgr.discover_plugins(&host()).unwrap();
    let messages: Vec<String> = errors.iter().map(ToString::to_string).collect();
    assert_eq!(
        messages,
        vec!["plugin 'future' requires host ^0.1, but this is host 0.0.1"]
    );
    assert_eq!(errors[0].directories, ["future"]);
    assert_eq!(order(&mgr), vec!["lib", "consumer"]);

    write("lib", "0.2.0", "");
    let err = mgr.reload_plugin("lib", &host()).unwrap_err();
    assert_eq!(
        err.to_string(),
        "plugin 'consumer' requires plugin 'lib' ^0.1, but 0.2.0 is installed"
    );
    assert_eq!(
        mgr.get_plugin("lib").unwrap().manifest.version,
        Version::new(0, 1, 2)
    );

    write("lib", "0.1.3", "");
    mgr.reload_plugin("lib", &host()).unwrap();
    assert_eq!(
        mgr.get_plugin("lib").unwrap().manifest.version,
        Version::new(0, 1, 3)
    );
    let _ = std::fs::remove_dir_all(&plugins_dir);
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
            dir.join(super::PLUGIN_CONFIG_FILENAME),
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
            .join(super::PLUGIN_CONFIG_FILENAME),
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
fn rejects_reserved_self_id() {
    assert!(validate_plugin_id("self").is_err());
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
