// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{
    Duration,
    Instant,
};

use anyhow::Context;
use harmony_luau as luau;

use super::discovery::{
    LoadedPlugin,
    execution_order,
    read_manifest_bytes,
};
use super::manifest::{
    PLUGIN_CONFIG_FILENAME,
    PluginHost,
    PluginManifest,
    parse_manifest,
};
use crate::{
    CallContext,
    GatedModule,
    GlobalSpec,
    ModuleId,
    Scheduler,
    SourceLoader,
    TokioRuntimeContext,
    luau::{
        RequireRuntime,
        ThreadDriveOptions,
        drive_thread,
        install_globals,
    },
    modules::{
        CapabilityPolicy,
        load_chunk_with_require,
    },
};
use harmony_luau::{
    CapabilityId,
    ChunkOrigin,
    TaskGroupId,
};

pub struct ManifestCapabilityPolicy {
    scopes_by_plugin: HashMap<Arc<str>, HashSet<Arc<str>>>,
    dependencies_by_plugin: HashMap<Arc<str>, HashSet<Arc<str>>>,
}

impl ManifestCapabilityPolicy {
    /// `manifests` are the loaded plugins: a declared dependency outside
    /// them, such as an optional one that failed to load, is not requirable
    /// even when its files are on disk.
    pub fn from_manifests(manifests: Arc<[PluginManifest]>) -> Self {
        let loaded: HashSet<&str> = manifests
            .iter()
            .map(|manifest| manifest.id.as_str())
            .collect();
        let mut scopes_by_plugin = HashMap::new();
        let mut dependencies_by_plugin = HashMap::new();
        for manifest in manifests.iter() {
            let id = Arc::<str>::from(manifest.id.as_str());
            let scopes = manifest
                .scopes
                .iter()
                .map(|scope| Arc::<str>::from(scope.as_str()))
                .collect::<HashSet<_>>();
            let dependencies = manifest
                .dependencies
                .keys()
                .filter(|dependency| loaded.contains(dependency.as_str()))
                .map(|dependency| Arc::<str>::from(dependency.as_str()))
                .collect::<HashSet<_>>();
            scopes_by_plugin.insert(id.clone(), scopes);
            dependencies_by_plugin.insert(id, dependencies);
        }

        Self {
            scopes_by_plugin,
            dependencies_by_plugin,
        }
    }
}

impl CapabilityPolicy for ManifestCapabilityPolicy {
    fn is_allowed(&self, origin: &ChunkOrigin, capability: &CapabilityId) -> bool {
        let Some(plugin_id) = origin.plugin.as_ref() else {
            return false;
        };
        self.scopes_by_plugin
            .get(plugin_id)
            .is_some_and(|scopes| scopes.contains(&capability.0))
    }

    fn allows_plugin_dependency(&self, requester: &str, target: &str) -> bool {
        self.dependencies_by_plugin
            .get(requester)
            .is_some_and(|dependencies| dependencies.contains(target))
    }
}

type VmConfigurator = Box<dyn FnOnce(&luau::Vm) -> anyhow::Result<()> + 'static>;
type RunEndHook = Box<dyn Fn(&luau::Vm, &str) + 'static>;

pub struct RuntimeBuilder<L> {
    loader: L,
    manifests: Arc<[PluginManifest]>,
    plugins: Arc<[LoadedPlugin]>,
    modules: Vec<GatedModule>,
    global_specs: Vec<GlobalSpec>,
    memory_limit: usize,
    luau_resume_budget: Option<Duration>,
    clock: crate::scheduler::Clock,
    configure_vm: Vec<VmConfigurator>,
    run_end_hooks: Vec<RunEndHook>,
}

impl<L> RuntimeBuilder<L>
where
    L: SourceLoader + 'static,
{
    pub fn new(loader: L, manifests: Arc<[PluginManifest]>) -> Self {
        Self {
            loader,
            manifests,
            plugins: Arc::from(Vec::<LoadedPlugin>::new()),
            modules: Vec::new(),
            global_specs: Vec::new(),
            memory_limit: 256 * 1024 * 1024,
            luau_resume_budget: Some(Duration::from_secs(300)),
            clock: crate::scheduler::Clock::default(),
            configure_vm: Vec::new(),
            run_end_hooks: Vec::new(),
        }
    }

    pub fn plugins(mut self, plugins: Arc<[LoadedPlugin]>) -> Self {
        self.plugins = plugins;
        self
    }

    pub fn modules(mut self, modules: Vec<GatedModule>) -> Self {
        self.modules = modules;
        self
    }

    pub fn global_specs(mut self, specs: Vec<GlobalSpec>) -> Self {
        self.global_specs = specs;
        self
    }

    pub fn memory_limit(mut self, bytes: usize) -> Self {
        self.memory_limit = bytes;
        self
    }

    pub fn luau_resume_budget(mut self, budget: Option<Duration>) -> Self {
        self.luau_resume_budget = budget;
        self
    }

    /// Runs the runtime's scheduler by `clock`, for a test that polls it; see
    /// [`Scheduler::with_manual_clock`]. What runs a plugin's code to its end on the calling
    /// thread, such as [`Runtime::exec_all`], fails on such a runtime.
    #[cfg(any(test, feature = "testing"))]
    pub fn manual_clock(mut self, clock: crate::testing::ManualClock) -> Self {
        self.clock = crate::scheduler::Clock::Manual(clock);
        self
    }

    pub fn configure_vm<F>(mut self, configure: F) -> Self
    where
        F: FnOnce(&luau::Vm) -> anyhow::Result<()> + 'static,
    {
        self.configure_vm.push(Box::new(configure));
        self
    }

    /// Runs with the plugin's id after each of its runs ends, whatever ended
    /// it: a stop, a restart or a failed entrypoint.
    pub fn on_run_end<F>(mut self, hook: F) -> Self
    where
        F: Fn(&luau::Vm, &str) + 'static,
    {
        self.run_end_hooks.push(Box::new(hook));
        self
    }

    pub fn build(self) -> anyhow::Result<Runtime> {
        let vm =
            luau::Vm::with_options(luau::VmOptions::default().memory_limit(self.memory_limit))?;
        vm.open_standard_libraries(luau::StandardLibraries::all_supported())?;

        let scheduler = Scheduler::with_clock(self.clock);
        scheduler.set_luau_resume_budget(self.luau_resume_budget);
        let scheduler_wake = scheduler.wake_handle();
        vm.data().insert(scheduler)?;

        for configure in self.configure_vm {
            configure(&vm)?;
        }

        let require = RequireRuntime::new(
            self.loader,
            ManifestCapabilityPolicy::from_manifests(self.manifests),
        );
        for module in self.modules {
            require.register(module)?;
        }
        vm.data().insert(require)?;

        for globals in self.global_specs {
            install_globals(&vm, &ChunkOrigin::default(), &globals)?;
        }
        // Every plugin shares these globals and libraries, so none may change them.
        vm.sandbox()?;

        Ok(Runtime {
            vm,
            plugins: Arc::from(execution_order(self.plugins.iter().cloned())?),
            tokio_runtime: TokioRuntimeContext::new()?,
            run_end_hooks: Rc::from(self.run_end_hooks),
            entrypoints_cancelled: crate::Cancellation::new(scheduler_wake),
        })
    }
}

/// What starting a plugin's entrypoint came to.
enum ScheduledEntrypoint {
    NoEntrypoint,
    /// Already evaluated, by an earlier run or a dependent's `require`.
    Cached,
    Running(Box<RunningEntrypoint>),
}

/// A plugin's entrypoint running on the scheduler.
pub(super) struct RunningEntrypoint {
    vm: luau::Vm,
    plugin_id: Arc<str>,
    entrypoint: Arc<str>,
    load: crate::modules::SourceLoad,
    pub(super) watched: crate::WatchedThread,
    deadline: Instant,
    cancelled: crate::Cancellation,
    run_end_hooks: Rc<[RunEndHook]>,
}

impl RunningEntrypoint {
    /// Waits for the entrypoint to end, at its deadline at the latest, and records how it did: a
    /// failed entrypoint ends the run. Cancellation wins over a completion that landed; a
    /// completion wins over the deadline, which only keeps an entrypoint from holding up its
    /// runtime for good: nobody was promised an answer by then, so a plugin that loaded isn't
    /// unloaded for having loaded late. An entrypoint that didn't complete is cancelled.
    pub(super) async fn finish(self) -> anyhow::Result<()> {
        let result = match self
            .watched
            .watch(
                self.cancelled.cancelled(),
                self.deadline,
                crate::LateEnd::Ended,
            )
            .await
        {
            crate::Watched::Ended { result, .. } => {
                result.map_err(|error| anyhow::Error::msg(error.message))
            }
            crate::Watched::Stopped(()) => Err(anyhow::anyhow!("entrypoint was cancelled")),
            crate::Watched::Expired => Err(anyhow::anyhow!("entrypoint did not complete")),
        };
        let finished = self
            .vm
            .data()
            .get::<RequireRuntime>()?
            .finish_source_load(self.load, result)
            .map_err(anyhow::Error::new)
            .with_context(|| {
                format!(
                    "run plugin '{}' entrypoint '{}'",
                    self.plugin_id, self.entrypoint
                )
            });
        if finished.is_err() {
            end_plugin_run(&self.vm, &self.run_end_hooks, &self.plugin_id)?;
        }
        finished.map(|_| ())
    }
}

/// Cancels every task of the plugin's current run, if it has one, then runs the run-end hooks.
fn end_plugin_run(vm: &luau::Vm, hooks: &[RunEndHook], plugin_id: &str) -> anyhow::Result<()> {
    let Some(group) = vm.data().get::<RequireRuntime>()?.end_plugin_run(plugin_id) else {
        return Ok(());
    };
    vm.data().get::<Scheduler>()?.cancel_group(group);
    for hook in hooks {
        hook(vm, plugin_id);
    }
    Ok(())
}

pub struct Runtime {
    pub vm: luau::Vm,
    /// In execution order, dependencies first.
    pub plugins: Arc<[LoadedPlugin]>,
    pub tokio_runtime: TokioRuntimeContext,
    run_end_hooks: Rc<[RunEndHook]>,
    entrypoints_cancelled: crate::Cancellation,
}

impl Runtime {
    pub(super) fn scheduler(&self) -> anyhow::Result<Rc<Scheduler>> {
        Ok(self.vm.data().get::<Scheduler>()?)
    }

    pub fn plugin_manifests(&self) -> Vec<PluginManifest> {
        let mut manifests = self
            .plugins
            .iter()
            .map(|plugin| plugin.manifest.clone())
            .collect::<Vec<_>>();
        manifests.sort_by(|left, right| left.id.cmp(&right.id));
        manifests
    }

    pub fn has_plugin(&self, plugin_id: &str) -> bool {
        self.plugins
            .iter()
            .any(|plugin| plugin.manifest.id == plugin_id)
    }

    pub fn exec_plugin(&self, plugin_id: &str) -> anyhow::Result<()> {
        let plugin = self
            .plugins
            .iter()
            .find(|plugin| plugin.manifest.id == plugin_id)
            .ok_or_else(|| anyhow::anyhow!("plugin not found: {plugin_id}"))?;
        self.exec_loaded_plugin(plugin)
    }

    fn loaded_plugin(&self, plugin_id: &str) -> anyhow::Result<&LoadedPlugin> {
        self.plugins
            .iter()
            .find(|plugin| plugin.manifest.id == plugin_id)
            .ok_or_else(|| anyhow::anyhow!("plugin not found: {plugin_id}"))
    }

    /// Fails when `plugin.json` on disk no longer matches the loaded
    /// manifest. Scopes and dependencies are fixed when the runtime is
    /// built, so manifest changes need a full reload rather than a restart.
    pub fn verify_plugin_manifest(&self, plugin_id: &str, host: &PluginHost) -> anyhow::Result<()> {
        let plugin = self.loaded_plugin(plugin_id)?;
        let config_path = plugin.directory.join(PLUGIN_CONFIG_FILENAME);
        let on_disk = read_manifest_bytes(&config_path)
            .and_then(|bytes| parse_manifest(&config_path, &bytes, host))
            .map_err(|error| anyhow::anyhow!("{error}"))
            .with_context(|| format!("read plugin '{plugin_id}' manifest"))?;
        if on_disk != plugin.manifest {
            anyhow::bail!(
                "plugin '{plugin_id}' manifest changed on disk; reload plugins to apply it"
            );
        }
        Ok(())
    }

    /// Drops every cached artifact owned by `plugin_id` (its evaluated
    /// sources and the module tables installed for its origin) and re-runs
    /// its entrypoint from the files on disk. Other plugins that already
    /// required its modules keep the exports they have until they restart.
    pub fn restart_plugin(&self, plugin_id: &str) -> anyhow::Result<()> {
        self.scheduler()?.ensure_can_wait()?;
        match self.start_restart_entrypoint(plugin_id)? {
            Some(entrypoint) => self.drive_entrypoint(entrypoint),
            None => Ok(()),
        }
    }

    /// [`Self::restart_plugin`] without waiting for the new entrypoint: its thread is scheduled,
    /// and the returned future, for a watcher of the runtime's scheduler to await, completes with
    /// how the restart went once the entrypoint has run. `None` when there was nothing to run.
    /// A failure to start leaves the run ended.
    pub fn start_plugin_restart(
        &self,
        plugin_id: &str,
    ) -> anyhow::Result<Option<impl Future<Output = anyhow::Result<()>> + use<>>> {
        Ok(self
            .start_restart_entrypoint(plugin_id)?
            .map(RunningEntrypoint::finish))
    }

    pub(super) fn start_restart_entrypoint(
        &self,
        plugin_id: &str,
    ) -> anyhow::Result<Option<RunningEntrypoint>> {
        let plugin = self.loaded_plugin(plugin_id)?;
        self.stop_plugin(plugin_id)?;
        let require = self.vm.data().get::<RequireRuntime>()?;
        require.invalidate_plugin(plugin_id);
        let scheduler = self.vm.data().get::<Scheduler>()?;
        require.plugin_group(&scheduler, plugin_id);
        let started = match self.schedule_entrypoint(plugin, &require, &scheduler) {
            Ok(ScheduledEntrypoint::Running(entrypoint)) => Ok(Some(*entrypoint)),
            Ok(ScheduledEntrypoint::NoEntrypoint) => Ok(None),
            // Invalidated just above, so nothing may have cached it since.
            Ok(ScheduledEntrypoint::Cached) => Err(anyhow::anyhow!(
                "plugin '{plugin_id}' entrypoint was already loaded after its sources were \
                 invalidated"
            )),
            Err(error) => Err(error),
        };
        if started.is_err() {
            self.stop_plugin(plugin_id)?;
        }
        started
    }

    fn schedule_entrypoint(
        &self,
        plugin: &LoadedPlugin,
        require: &RequireRuntime,
        scheduler: &Rc<Scheduler>,
    ) -> anyhow::Result<ScheduledEntrypoint> {
        let Some(entrypoint) = plugin.manifest.entrypoint.as_deref() else {
            return Ok(ScheduledEntrypoint::NoEntrypoint);
        };
        let plugin_id = plugin.manifest.id.as_str();
        anyhow::ensure!(
            !self.entrypoints_cancelled.is_cancelled(),
            "plugin '{plugin_id}' entrypoint was cancelled"
        );
        let context = || format!("run plugin '{plugin_id}' entrypoint '{entrypoint}'");
        let specifier = format!("@{plugin_id}/{entrypoint}");
        let load = match require
            .start_source_load(&specifier, &ChunkOrigin::default())
            .map_err(anyhow::Error::new)
            .with_context(context)?
        {
            crate::modules::SourceLoadStart::Cached(_) => return Ok(ScheduledEntrypoint::Cached),
            crate::modules::SourceLoadStart::Started(load) => load,
        };
        let timeout = ThreadDriveOptions::default().timeout;
        let watched = (|| {
            let function = load_chunk_with_require(
                &self.vm,
                load.source.bytes.clone(),
                &load.source.origin,
                load.task_group,
            )?;
            let thread = self.vm.create_thread(&function)?;
            thread.sandbox()?;
            anyhow::Ok(scheduler.schedule_watched_luau_thread(
                CallContext {
                    origin: load.source.origin.clone(),
                    task_group: load.task_group,
                    ..CallContext::default()
                },
                thread,
                Vec::new(),
                None,
            ))
        })();
        match watched {
            Ok(watched) => Ok(ScheduledEntrypoint::Running(Box::new(RunningEntrypoint {
                vm: self.vm.clone(),
                cancelled: self.entrypoints_cancelled.clone(),
                run_end_hooks: self.run_end_hooks.clone(),
                plugin_id: Arc::from(plugin_id),
                entrypoint: Arc::from(entrypoint),
                load,
                watched,
                deadline: scheduler.now() + timeout,
            }))),
            Err(error) => {
                let failed = require.finish_source_load(load, Err(error));
                failed
                    .map(|_| ScheduledEntrypoint::NoEntrypoint)
                    .map_err(anyhow::Error::new)
                    .with_context(context)
            }
        }
    }

    /// Runs `entrypoint` to its end on this thread, driving the scheduler meanwhile.
    pub(super) fn drive_entrypoint(&self, entrypoint: RunningEntrypoint) -> anyhow::Result<()> {
        let scheduler = self.vm.data().get::<Scheduler>()?;
        let task = scheduler.luau_thread_handle(entrypoint.watched.thread());
        let waker = scheduler.wake_handle().waker();
        let mut cx = std::task::Context::from_waker(&waker);
        let mut finish = std::pin::pin!(entrypoint.finish());
        let result = loop {
            // Before resuming anything: a cancelled entrypoint must not run further.
            if let std::task::Poll::Ready(result) = finish.as_mut().poll(&mut cx) {
                break result;
            }
            {
                let _guard = self.tokio_runtime.enter();
                scheduler.poll_ready();
            }
            scheduler.wait_for_work(None);
        };
        if let Some(task) = task {
            scheduler.remove(task.id());
        }
        result
    }

    /// Cancels every task of the plugin's current run, including handler
    /// calls running in it and tasks spawned into it later, then runs the
    /// run-end hooks.
    pub fn stop_plugin(&self, plugin_id: &str) -> anyhow::Result<()> {
        self.loaded_plugin(plugin_id)?;
        end_plugin_run(&self.vm, &self.run_end_hooks, plugin_id)
    }

    /// Runs every plugin's entrypoint, dependencies first, and returns the
    /// ids of the plugins whose entrypoint failed.
    pub fn exec_all(&self) -> Vec<String> {
        let mut failed = Vec::new();
        for plugin in self.plugins.iter() {
            match self.exec_loaded_plugin(plugin) {
                Ok(()) => tracing::debug!("plugin '{}' executed", plugin.manifest.id),
                Err(error) => {
                    tracing::warn!("plugin '{}' error: {error:#}", plugin.manifest.id);
                    failed.push(plugin.manifest.id.clone());
                }
            }
        }
        failed
    }

    /// Starts the plugin's run, libraries included, so its sources can be
    /// required. A failed entrypoint ends the run, so the tasks it spawned
    /// before failing stop with it.
    fn exec_loaded_plugin(&self, plugin: &LoadedPlugin) -> anyhow::Result<()> {
        self.scheduler()?.ensure_can_wait()?;
        match self.start_entrypoint(plugin)? {
            Some(entrypoint) => self.drive_entrypoint(entrypoint),
            None => Ok(()),
        }
    }

    /// Starts the plugin's run and schedules its entrypoint. `None` when there is nothing to
    /// run. A failure to start leaves the run ended.
    pub(super) fn start_entrypoint(
        &self,
        plugin: &LoadedPlugin,
    ) -> anyhow::Result<Option<RunningEntrypoint>> {
        let scheduler = self.scheduler()?;
        let require = self.vm.data().get::<RequireRuntime>()?;
        require.plugin_group(&scheduler, &plugin.manifest.id);
        match self.schedule_entrypoint(plugin, &require, &scheduler) {
            Ok(ScheduledEntrypoint::Running(entrypoint)) => Ok(Some(*entrypoint)),
            Ok(ScheduledEntrypoint::NoEntrypoint | ScheduledEntrypoint::Cached) => Ok(None),
            Err(error) => {
                self.stop_plugin(&plugin.manifest.id)?;
                Err(error)
            }
        }
    }

    /// Once cancelled, from any thread, every entrypoint run fails as
    /// cancelled: the running one at its next yield and later ones before
    /// they start. One-way, for a runtime about to shut down, so a long
    /// entrypoint can't hold up its shutdown.
    pub fn entrypoint_cancellation(&self) -> crate::Cancellation {
        self.entrypoints_cancelled.clone()
    }

    pub fn run_plugin_source(
        &self,
        plugin_id: impl Into<Arc<str>>,
        path: impl Into<Arc<str>>,
        source: impl Into<Arc<[u8]>>,
    ) -> anyhow::Result<()> {
        let origin = plugin_origin(plugin_id, path);
        self.eval_source_with_context(
            source,
            CallContext {
                origin,
                ..CallContext::default()
            },
        )
        .map(|_| ())
    }

    pub fn eval_source_with_context(
        &self,
        source: impl Into<Arc<[u8]>>,
        context: CallContext,
    ) -> anyhow::Result<Vec<luau::Value>> {
        self.eval_source_with_args(source, context, Vec::new())
    }

    /// Evaluates `source` with `args` as the chunk's `...`.
    pub fn eval_source_with_args(
        &self,
        source: impl Into<Arc<[u8]>>,
        context: CallContext,
        args: Vec<luau::Value>,
    ) -> anyhow::Result<Vec<luau::Value>> {
        let scheduler = self.scheduler()?;
        scheduler.ensure_can_wait()?;
        let mut context = context;
        let thread = self.load_source_thread(source, &mut context)?;
        scheduler.spawn_luau_thread(context, thread.clone(), args);
        drive_thread(
            &self.tokio_runtime,
            &scheduler,
            &thread,
            ThreadDriveOptions::default(),
        )
    }

    /// Loads `source` as a sandboxed thread for the caller to schedule under `context`. Plugin
    /// code runs in its plugin's current run, never the default group, so a default group in
    /// `context` becomes the plugin's.
    pub fn load_source_thread(
        &self,
        source: impl Into<Arc<[u8]>>,
        context: &mut CallContext,
    ) -> anyhow::Result<luau::Thread> {
        if let Some(plugin_id) = context.origin.plugin.as_deref()
            && context.task_group == TaskGroupId::default()
        {
            let scheduler = self.vm.data().get::<Scheduler>()?;
            context.task_group = self
                .vm
                .data()
                .get::<RequireRuntime>()?
                .plugin_group(&scheduler, plugin_id);
        }
        let function =
            load_chunk_with_require(&self.vm, source, &context.origin, context.task_group)?;
        let thread = self.vm.create_thread(&function)?;
        thread.sandbox()?;
        Ok(thread)
    }

    pub fn poll_background_tasks(&self) -> usize {
        let Ok(scheduler) = self.vm.data().get::<Scheduler>() else {
            return 0;
        };
        {
            let _guard = self.tokio_runtime.enter();
            scheduler.poll_ready();
        }
        scheduler.remove_finished()
    }
}

pub(super) fn plugin_origin(
    plugin_id: impl Into<Arc<str>>,
    path: impl Into<Arc<str>>,
) -> ChunkOrigin {
    let plugin = plugin_id.into();
    let path = path.into();
    ChunkOrigin {
        module: Some(ModuleId(Arc::from(format!("plugins/{plugin}/{path}")))),
        plugin: Some(plugin.clone()),
        path: Some(Arc::from(format!("plugins/{plugin}/{path}"))),
    }
}
