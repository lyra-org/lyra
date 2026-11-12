// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    cell::{
        Cell,
        RefCell,
    },
    collections::HashMap,
    path::PathBuf,
    rc::Rc,
    sync::Arc,
};

use anyhow::Result;
use harmony_core::{
    CallContext,
    FilesystemSourceLoader,
    ModuleSpec,
    SourceLoader,
    plugin::{
        LoadedPlugin,
        PluginDiscoveryError,
        PluginManager,
        PluginManifest,
        RuntimeBuilder,
    },
};
use harmony_luau as luau;

use crate::{
    plugins::update_callbacks::UpdateCallbackStore,
    services::{
        bookmarks::BookmarkUpdatePayload,
        favorites::FavoriteUpdatePayload,
        listens::ListenUpdatePayload,
        playback_sessions::PlaybackUpdatePayload,
        ratings::RatingUpdatePayload,
    },
};

use super::{
    PluginExecutor,
    WebSocketState,
    messages::TaskIdKey,
    modules::generic_modules,
    stores::PluginModuleStores,
};

/// How long a chunk [`PluginExecutor::start_plugin_source`] runs may take, as long as a plugin's
/// entrypoint may.
const PLUGIN_SOURCE_RUN_BUDGET: std::time::Duration = std::time::Duration::from_secs(300);

impl PluginExecutor {
    pub(crate) fn with_filesystem_sources(
        manifests: Arc<[PluginManifest]>,
        server_info: crate::plugins::server::ServerInfo,
        auth_capabilities: crate::plugins::auth::AuthCapabilities,
        db: lyra_db::DbAsync,
        source_root: impl Into<std::path::PathBuf>,
        plugins_dir: impl Into<std::path::PathBuf>,
    ) -> Result<Self> {
        Self::with_loader(
            manifests,
            server_info,
            auth_capabilities,
            PluginModuleStores::with_db(db),
            FilesystemSourceLoader::new(source_root, plugins_dir),
        )
    }

    #[cfg(test)]
    pub(crate) fn discover_from_plugins_dir_with_db_and_modules(
        plugins_dir: impl Into<PathBuf>,
        server_info: crate::plugins::server::ServerInfo,
        auth_capabilities: crate::plugins::auth::AuthCapabilities,
        db: lyra_db::DbAsync,
        module_overrides: Vec<ModuleSpec>,
    ) -> Result<(Self, Vec<PluginDiscoveryError>)> {
        Self::discover_from_plugins_dir_with_stores(
            plugins_dir,
            server_info,
            auth_capabilities,
            PluginModuleStores::with_db(db).sharing_published_scope(),
            module_overrides,
        )
    }

    pub(super) fn discover_from_plugins_dir_with_stores(
        plugins_dir: impl Into<PathBuf>,
        server_info: crate::plugins::server::ServerInfo,
        auth_capabilities: crate::plugins::auth::AuthCapabilities,
        stores: PluginModuleStores,
        module_overrides: Vec<ModuleSpec>,
    ) -> Result<(Self, Vec<PluginDiscoveryError>)> {
        let discovered = DiscoveredPlugins::in_dir(plugins_dir.into())?;
        let runtime = Self::with_runtime(
            discovered.builder,
            discovered.manifests,
            server_info,
            auth_capabilities,
            stores,
            module_overrides,
        )?;
        Ok((runtime, discovered.errors))
    }

    pub(super) fn with_loader<L>(
        manifests: Arc<[PluginManifest]>,
        server_info: crate::plugins::server::ServerInfo,
        auth_capabilities: crate::plugins::auth::AuthCapabilities,
        stores: PluginModuleStores,
        loader: L,
    ) -> Result<Self>
    where
        L: SourceLoader + 'static,
    {
        Self::with_runtime(
            RuntimeBuilder::new(loader, manifests.clone()),
            manifests,
            server_info,
            auth_capabilities,
            stores,
            Vec::new(),
        )
    }

    /// An executor on the runtime `builder` makes once it holds the executor's modules, stores
    /// and hooks.
    pub(super) fn with_runtime<L>(
        builder: RuntimeBuilder<L>,
        manifests: Arc<[PluginManifest]>,
        server_info: crate::plugins::server::ServerInfo,
        auth_capabilities: crate::plugins::auth::AuthCapabilities,
        stores: PluginModuleStores,
        module_overrides: Vec<ModuleSpec>,
    ) -> Result<Self>
    where
        L: SourceLoader + 'static,
    {
        let scope = stores.scope();
        let running = super::callbacks::RunningHandlers::default();
        let running_for_vm = running.clone();
        let runtime = builder
            .modules(generic_modules(module_overrides)?)
            .global_specs(harmony_globals::plugin_log_global_specs())
            .configure_vm(move |vm| {
                vm.data()
                    .insert(crate::plugins::manifests::PluginManifestModuleStore::new(
                        manifests.clone(),
                    ))?;
                vm.data()
                    .insert(crate::plugins::server::ServerInfoModuleStore::new(
                        server_info,
                        stores.server_settings.clone(),
                    ))?;
                vm.data()
                    .insert(crate::plugins::auth::AuthCapabilitiesModuleStore::new(
                        auth_capabilities,
                        stores.server_settings.clone(),
                    ))?;
                stores.install_into(vm)?;
                vm.data().insert(running_for_vm)?;
                install_callback_stores(vm)
            })
            .on_run_end(remove_plugin_callbacks)
            .on_run_end(super::stream::stop_run_streams)
            .build()?;
        let wake = runtime
            .vm
            .data()
            .get::<harmony_core::LocalScheduler>()?
            .wake_handle();

        Ok(Self {
            wake,
            runtime,
            scope,
            running,
            websocket_tasks: RefCell::new(HashMap::<TaskIdKey, Arc<WebSocketState>>::new()),
            restarting: Rc::default(),
        })
    }

    pub(crate) fn plugin_manifests(&self) -> Vec<PluginManifest> {
        self.runtime.plugin_manifests()
    }

    pub(crate) fn has_plugin(&self, plugin_id: &str) -> bool {
        self.runtime.has_plugin(plugin_id)
    }

    #[cfg(test)]
    pub(crate) fn exec_plugin(&self, plugin_id: &str) -> Result<()> {
        self.runtime.exec_plugin(plugin_id)
    }

    pub(crate) fn verify_plugin_manifest(&self, plugin_id: &str) -> Result<()> {
        self.runtime
            .verify_plugin_manifest(plugin_id, &crate::plugins::plugin_host())
    }

    pub(crate) fn stop_plugin(&self, plugin_id: &str) -> Result<()> {
        self.runtime.stop_plugin(plugin_id)
    }

    /// Refuses new dispatches into the plugin, until [`Self::reopen_plugin`]. The handlers already
    /// running keep its callbacks, so their own nested calls still resolve, until the run stops.
    pub(crate) fn quiesce_plugin(&self, plugin_id: &str) {
        self.running.quiesce(plugin_id);
    }

    /// Admits dispatches again, once what replaced the plugin is live.
    pub(crate) fn reopen_plugin(&self, plugin_id: &str) {
        self.running.reopen(plugin_id);
    }

    pub(super) fn is_quiesced(&self, plugin: Option<&str>) -> bool {
        self.running.is_quiesced(plugin)
    }

    /// Replies once the plugin's new entrypoint has run; the executor serves other commands
    /// meanwhile.
    pub(super) fn start_plugin_restart(
        &self,
        plugin_id: String,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    ) {
        if self.restarting.get() {
            let _ = reply.send(Err(anyhow::anyhow!("a plugin restart is already running")));
            return;
        }
        let started = self.runtime.start_plugin_restart(&plugin_id);
        // The ended run's streams were stopped as it was asked to end; the new run's are not.
        if let Ok(stops) = super::stream::stream_stops(&self.vm) {
            stops.run_started(&plugin_id);
        }
        let restart = match started {
            Ok(Some(restart)) => restart,
            Ok(None) => {
                let _ = reply.send(Ok(()));
                return;
            }
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };
        let scheduler = match self.vm.data().get::<harmony_core::LocalScheduler>() {
            Ok(scheduler) => scheduler,
            Err(error) => {
                let _ = reply.send(Err(error.into()));
                return;
            }
        };
        let restarting = Restarting::begin(self.restarting.clone());
        scheduler.schedule_watcher(
            super::watcher_context(Some(Arc::from(plugin_id))),
            async move {
                let outcome = restart.await;
                drop(restarting);
                let _ = reply.send(outcome);
                Ok(())
            },
        );
    }

    /// The ids of the plugins whose entrypoint failed.
    pub(crate) fn exec_all(&self) -> Vec<String> {
        self.runtime.exec_all()
    }

    /// Cancels every plugin's tasks, polls once so cancelled websocket
    /// handlers close their sockets, then releases the VM's data: its values
    /// hold Luau references, and through them the VM itself.
    pub(crate) fn shutdown(self) -> Result<()> {
        let stopped = self
            .runtime
            .plugins
            .iter()
            .try_for_each(|plugin| self.runtime.stop_plugin(&plugin.manifest.id));
        self.poll_background_tasks();
        super::stream::fail_streams(&self.vm);
        let released = self.vm.data().clear().map_err(anyhow::Error::new);
        stopped.and(released)
    }

    /// Releases what [`Self::shutdown`] would, without running more of the
    /// Luau of a runtime whose thread panicked: closes its websockets, fails
    /// its streams and breaks the cycles through the VM's data. Each step
    /// runs even if one before it panicked.
    pub(super) fn release_after_panic(self) {
        release_step("close websockets", || {
            for state in self.websocket_tasks.borrow().values() {
                state.request_close();
            }
        });
        release_step("fail streams", || super::stream::fail_streams(&self.vm));
        release_step("release VM data", || {
            if let Err(error) = self.vm.data().clear() {
                tracing::warn!(error = %error, "crashed plugin runtime kept its VM data");
            }
        });
        release_step("drop runtime", move || drop(self));
    }

    #[cfg(test)]
    pub(crate) fn run_plugin_source(
        &self,
        plugin_id: impl Into<Arc<str>>,
        path: impl Into<Arc<str>>,
        source: impl Into<Arc<[u8]>>,
    ) -> Result<()> {
        self.runtime.run_plugin_source(plugin_id, path, source)
    }

    /// Runs `source` with `args` as its `...`, as a dispatch acting for its `auth`.
    pub(super) fn start_plugin_source(
        &self,
        run: super::PluginSourceRun,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) {
        let deadline = self.now() + PLUGIN_SOURCE_RUN_BUDGET;
        self.start_callback(
            deadline,
            super::callbacks::CallbackReply::Source(reply),
            permit,
            Some(Arc::from(run.plugin_id.as_str())),
            |limit| {
                let dispatch_auth = crate::plugins::auth::DispatchAuth::default();
                dispatch_auth.record(run.auth);
                let mut context = CallContext {
                    origin: super::plugin_origin(run.plugin_id, run.path),
                    ..CallContext::default()
                };
                context.caller.insert(dispatch_auth);
                context.caller.insert(super::CallChain::root(
                    deadline,
                    context.origin.plugin.clone(),
                ));
                let args = (run.args)(&self.vm, &context.origin)?;
                let thread = self.runtime.load_source_thread(run.source, &mut context)?;
                Ok(self
                    .vm
                    .data()
                    .get::<harmony_core::LocalScheduler>()?
                    .schedule_watched_luau_thread(context, thread, args, Some(limit)))
            },
        );
    }

    #[cfg(test)]
    pub(super) fn run_plugin_source_with_call_context(
        &self,
        source: impl Into<Arc<[u8]>>,
        context: CallContext,
    ) -> Result<()> {
        self.eval_plugin_source_with_call_context(source, context)
            .map(|_| ())
    }

    #[cfg(test)]
    pub(super) fn eval_plugin_source_with_call_context(
        &self,
        source: impl Into<Arc<[u8]>>,
        context: CallContext,
    ) -> Result<Vec<luau::Value>> {
        self.runtime.eval_source_with_context(source, context)
    }
}

/// The plugins found in a directory: a builder of their runtime, their manifests and what failed
/// to load.
pub(super) struct DiscoveredPlugins {
    pub(super) builder: RuntimeBuilder<FilesystemSourceLoader>,
    pub(super) manifests: Arc<[PluginManifest]>,
    pub(super) errors: Vec<PluginDiscoveryError>,
}

impl DiscoveredPlugins {
    pub(super) fn in_dir(plugins_dir: PathBuf) -> Result<Self> {
        let mut plugin_manager = PluginManager::new(plugins_dir.clone());
        let errors = plugin_manager.discover_plugins(&crate::plugins::plugin_host())?;
        let mut plugins = plugin_manager.list_plugins().cloned().collect::<Vec<_>>();
        plugins.sort_by(|left, right| left.manifest.id.cmp(&right.manifest.id));
        let plugins = Arc::<[LoadedPlugin]>::from(plugins);
        let manifests = Arc::<[PluginManifest]>::from(
            plugins
                .iter()
                .map(|plugin| plugin.manifest.clone())
                .collect::<Vec<_>>(),
        );
        let builder = RuntimeBuilder::new(
            FilesystemSourceLoader::new("/", plugins_dir),
            manifests.clone(),
        )
        .plugins(plugins);
        Ok(Self {
            builder,
            manifests,
            errors,
        })
    }
}

/// Marks a restart as running until dropped, however its task ends.
struct Restarting(Rc<Cell<bool>>);

impl Restarting {
    fn begin(running: Rc<Cell<bool>>) -> Self {
        running.set(true);
        Self(running)
    }
}

impl Drop for Restarting {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

fn release_step(step: &str, run: impl FnOnce()) {
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(run)).is_err() {
        tracing::error!(step, "releasing a crashed plugin runtime panicked");
    }
}

/// The stores of the callbacks plugins register, each holding Luau functions
/// that belong to the run that registered them.
fn install_callback_stores(vm: &luau::Vm) -> Result<()> {
    vm.data()
        .insert(crate::plugins::metadata::MetadataCallbackRegistry::new(
            vm.id(),
        ))?;
    vm.data()
        .insert(crate::plugins::mix::MixCallbackRegistry::new(vm.id()))?;
    vm.data()
        .insert(UpdateCallbackStore::<PlaybackUpdatePayload>::new(vm.id()))?;
    vm.data()
        .insert(UpdateCallbackStore::<ListenUpdatePayload>::new(vm.id()))?;
    vm.data()
        .insert(UpdateCallbackStore::<FavoriteUpdatePayload>::new(vm.id()))?;
    vm.data()
        .insert(UpdateCallbackStore::<RatingUpdatePayload>::new(vm.id()))?;
    vm.data()
        .insert(UpdateCallbackStore::<BookmarkUpdatePayload>::new(vm.id()))?;
    vm.data()
        .insert(crate::plugins::api::ApiRouteStore::new(vm.id()))?;
    vm.data().insert(super::stream::StreamTasks::default())?;
    Ok(())
}

/// Drops the callbacks of a run that ended, so nothing dispatches into it.
fn remove_plugin_callbacks(vm: &luau::Vm, plugin_id: &str) {
    use crate::plugins::{
        api::ApiRouteStore,
        metadata::MetadataCallbackRegistry,
        mix::MixCallbackRegistry,
    };
    remove_from_store(vm, plugin_id, MetadataCallbackRegistry::remove_plugin);
    remove_from_store(vm, plugin_id, MixCallbackRegistry::remove_plugin);
    remove_from_store(
        vm,
        plugin_id,
        UpdateCallbackStore::<PlaybackUpdatePayload>::remove_plugin,
    );
    remove_from_store(
        vm,
        plugin_id,
        UpdateCallbackStore::<ListenUpdatePayload>::remove_plugin,
    );
    remove_from_store(
        vm,
        plugin_id,
        UpdateCallbackStore::<FavoriteUpdatePayload>::remove_plugin,
    );
    remove_from_store(
        vm,
        plugin_id,
        UpdateCallbackStore::<RatingUpdatePayload>::remove_plugin,
    );
    remove_from_store(
        vm,
        plugin_id,
        UpdateCallbackStore::<BookmarkUpdatePayload>::remove_plugin,
    );
    remove_from_store(vm, plugin_id, ApiRouteStore::remove_plugin);
}

fn remove_from_store<T: 'static>(vm: &luau::Vm, plugin_id: &str, remove: fn(&T, &str)) {
    match vm.data().get::<T>() {
        Ok(store) => remove(&store, plugin_id),
        Err(error) => tracing::warn!(
            plugin_id,
            store = std::any::type_name::<T>(),
            error = %error,
            "plugin callback store is missing from the VM's data"
        ),
    }
}
