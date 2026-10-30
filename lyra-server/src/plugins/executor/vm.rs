// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    cell::RefCell,
    collections::HashMap,
    path::PathBuf,
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
        PluginLoadError,
        PluginManager,
        PluginManifest,
        RuntimeBuilder,
    },
};
use harmony_luau as luau;

use crate::{
    plugins::update_callbacks::UpdateCallbackStore,
    services::{
        listens::ListenUpdatePayload,
        playback_sessions::PlaybackUpdatePayload,
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
    ) -> Result<(Self, Vec<PluginLoadError>)> {
        Self::discover_from_plugins_dir_with_stores(
            plugins_dir,
            server_info,
            auth_capabilities,
            PluginModuleStores::with_db(db),
            module_overrides,
        )
    }

    pub(super) fn discover_from_plugins_dir_with_stores(
        plugins_dir: impl Into<PathBuf>,
        server_info: crate::plugins::server::ServerInfo,
        auth_capabilities: crate::plugins::auth::AuthCapabilities,
        stores: PluginModuleStores,
        module_overrides: Vec<ModuleSpec>,
    ) -> Result<(Self, Vec<PluginLoadError>)> {
        let plugins_dir = plugins_dir.into();
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
        let runtime = Self::with_loader_and_plugins(
            manifests,
            server_info,
            auth_capabilities,
            FilesystemSourceLoader::new("/", plugins_dir),
            plugins,
            stores,
            module_overrides,
        )?;
        Ok((runtime, errors))
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
        Self::with_loader_and_plugins(
            manifests,
            server_info,
            auth_capabilities,
            loader,
            Arc::from(Vec::<LoadedPlugin>::new()),
            stores,
            Vec::new(),
        )
    }

    fn with_loader_and_plugins<L>(
        manifests: Arc<[PluginManifest]>,
        server_info: crate::plugins::server::ServerInfo,
        auth_capabilities: crate::plugins::auth::AuthCapabilities,
        loader: L,
        plugins: Arc<[LoadedPlugin]>,
        stores: PluginModuleStores,
        module_overrides: Vec<ModuleSpec>,
    ) -> Result<Self>
    where
        L: SourceLoader + 'static,
    {
        let runtime = RuntimeBuilder::new(loader, manifests.clone())
            .plugins(plugins)
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
                install_callback_stores(vm)
            })
            .on_run_end(remove_plugin_callbacks)
            .build()?;

        Ok(Self {
            runtime,
            websocket_tasks: RefCell::new(HashMap::<TaskIdKey, Arc<WebSocketState>>::new()),
            callback_tasks: RefCell::new(Vec::new()),
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

    pub(crate) fn restart_plugin(&self, plugin_id: &str) -> Result<()> {
        self.runtime.restart_plugin(plugin_id)
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
        let released = self.vm.data().clear().map_err(anyhow::Error::new);
        stopped.and(released)
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

    /// Runs `source` with `args` as its `...`, as a dispatch acting for `principal`.
    pub(super) fn start_plugin_source(
        &self,
        run: super::PluginSourceRun,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) {
        self.start_callback(
            std::time::Instant::now() + PLUGIN_SOURCE_RUN_BUDGET,
            super::callbacks::CallbackReply::Source(reply),
            permit,
            |timeout, completion| {
                let dispatch_auth = crate::plugins::auth::DispatchAuth::default();
                dispatch_auth.record(run.principal);
                let mut context = CallContext {
                    origin: super::plugin_origin(run.plugin_id, run.path),
                    ..CallContext::default()
                };
                context.caller.insert(dispatch_auth);
                let args = (run.args)()?;
                let thread = self.runtime.load_source_thread(run.source, &mut context)?;
                let scheduler = self.vm.data().get::<harmony_core::LocalScheduler>()?;
                scheduler.schedule_luau_thread_with_budget_and_completion(
                    context,
                    self.vm.clone(),
                    thread.clone(),
                    args,
                    timeout,
                    completion,
                );
                Ok((scheduler, thread))
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
        .insert(crate::plugins::api::ApiRouteStore::new(vm.id()))?;
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
