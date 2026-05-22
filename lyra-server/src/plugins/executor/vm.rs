// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    cell::RefCell,
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::{
    Context,
    Result,
};
#[cfg(test)]
use harmony_core::MemorySourceLoader;
use harmony_core::{
    CallContext,
    ChunkOrigin,
    FilesystemSourceLoader,
    LoadedPlugin,
    LocalScheduler,
    LuauRequireRuntime,
    ManifestCapabilityPolicy,
    ModuleSpec,
    PluginLoadError,
    PluginManager,
    PluginManifest,
    SourceLoader,
    TokioRuntimeContext,
    install_luau_globals,
    install_luau_require,
};
use harmony_luau as luau;

#[cfg(test)]
use super::default_server_info;
use super::{
    PluginExecutor,
    WebSocketState,
    default_auth_capabilities,
    luau_origin,
    messages::TaskIdKey,
    modules::{
        plugin_scope_ids,
        register_generic_modules,
    },
    plugin_origin,
    runner::drive_luau_thread,
    stores::PluginModuleStores,
};

impl PluginExecutor {
    #[cfg(test)]
    pub(crate) fn with_manifests(manifests: Arc<[harmony_core::PluginManifest]>) -> Result<Self> {
        Self::with_runtime_state(manifests, default_server_info())
    }

    #[cfg(test)]
    pub(crate) fn with_runtime_state(
        manifests: Arc<[harmony_core::PluginManifest]>,
        server_info: crate::plugins::server::ServerInfo,
    ) -> Result<Self> {
        Self::with_loader(
            manifests,
            server_info,
            PluginModuleStores::empty(),
            MemorySourceLoader::new(),
        )
    }

    #[cfg(test)]
    pub(crate) fn with_database(
        manifests: Arc<[harmony_core::PluginManifest]>,
        server_info: crate::plugins::server::ServerInfo,
        db: crate::plugins::db::DbAsync,
    ) -> Result<Self> {
        Self::with_loader(
            manifests,
            server_info,
            PluginModuleStores::with_db(db),
            MemorySourceLoader::new(),
        )
    }

    pub(crate) fn with_filesystem_sources(
        manifests: Arc<[harmony_core::PluginManifest]>,
        server_info: crate::plugins::server::ServerInfo,
        source_root: impl Into<std::path::PathBuf>,
        plugins_dir: impl Into<std::path::PathBuf>,
    ) -> Result<Self> {
        Self::with_loader(
            manifests,
            server_info,
            PluginModuleStores::empty(),
            FilesystemSourceLoader::new(source_root, plugins_dir),
        )
    }

    #[cfg(test)]
    pub(crate) fn discover_from_plugins_dir(
        plugins_dir: impl Into<PathBuf>,
        server_info: crate::plugins::server::ServerInfo,
    ) -> Result<(Self, Vec<PluginLoadError>)> {
        Self::discover_from_plugins_dir_with_stores(
            plugins_dir,
            server_info,
            PluginModuleStores::empty(),
            Vec::new(),
        )
    }

    #[cfg(test)]
    pub(crate) fn discover_from_plugins_dir_with_db(
        plugins_dir: impl Into<PathBuf>,
        server_info: crate::plugins::server::ServerInfo,
        db: crate::plugins::db::DbAsync,
    ) -> Result<(Self, Vec<PluginLoadError>)> {
        Self::discover_from_plugins_dir_with_db_and_modules(
            plugins_dir,
            server_info,
            db,
            Vec::new(),
        )
    }

    pub(crate) fn discover_from_plugins_dir_with_db_and_modules(
        plugins_dir: impl Into<PathBuf>,
        server_info: crate::plugins::server::ServerInfo,
        db: crate::plugins::db::DbAsync,
        module_overrides: Vec<ModuleSpec>,
    ) -> Result<(Self, Vec<PluginLoadError>)> {
        Self::discover_from_plugins_dir_with_stores(
            plugins_dir,
            server_info,
            PluginModuleStores::with_db(db),
            module_overrides,
        )
    }

    fn discover_from_plugins_dir_with_stores(
        plugins_dir: impl Into<PathBuf>,
        server_info: crate::plugins::server::ServerInfo,
        stores: PluginModuleStores,
        module_overrides: Vec<ModuleSpec>,
    ) -> Result<(Self, Vec<PluginLoadError>)> {
        let plugins_dir = plugins_dir.into();
        let mut plugin_manager = PluginManager::new(plugins_dir.clone());
        let errors = plugin_manager.discover_plugins(&plugin_scope_ids())?;
        let plugins = Arc::<[LoadedPlugin]>::from(plugin_manager.topological_order());
        let manifests = Arc::<[PluginManifest]>::from(
            plugins
                .iter()
                .map(|plugin| plugin.manifest.clone())
                .collect::<Vec<_>>(),
        );
        let runtime = Self::with_loader_and_plugins(
            manifests,
            server_info,
            FilesystemSourceLoader::new("/", plugins_dir),
            plugins,
            stores,
            module_overrides,
        )?;
        Ok((runtime, errors))
    }

    fn with_loader<L>(
        manifests: Arc<[harmony_core::PluginManifest]>,
        server_info: crate::plugins::server::ServerInfo,
        stores: PluginModuleStores,
        loader: L,
    ) -> Result<Self>
    where
        L: SourceLoader + 'static,
    {
        Self::with_loader_and_plugins(
            manifests,
            server_info,
            loader,
            Arc::from(Vec::<LoadedPlugin>::new()),
            stores,
            Vec::new(),
        )
    }

    fn with_loader_and_plugins<L>(
        manifests: Arc<[harmony_core::PluginManifest]>,
        server_info: crate::plugins::server::ServerInfo,
        loader: L,
        plugins: Arc<[LoadedPlugin]>,
        stores: PluginModuleStores,
        module_overrides: Vec<ModuleSpec>,
    ) -> Result<Self>
    where
        L: SourceLoader + 'static,
    {
        let vm =
            luau::Vm::with_options(luau::VmOptions::default().memory_limit(256 * 1024 * 1024))?;
        vm.open_standard_libraries(luau::StandardLibraries::all_supported())?;
        let scheduler = LocalScheduler::new();
        scheduler.set_luau_resume_budget(Some(Duration::from_secs(300)));
        vm.data().insert(scheduler)?;
        vm.data()
            .insert(crate::plugins::manifests::PluginManifestModuleStore::new(
                manifests.clone(),
            ))?;
        vm.data()
            .insert(crate::plugins::server::ServerInfoModuleStore::new(
                server_info,
            ))?;
        vm.data()
            .insert(crate::plugins::auth::AuthCapabilitiesModuleStore::new(
                default_auth_capabilities(),
            ))?;
        stores.install_into(&vm)?;
        vm.data()
            .insert(crate::plugins::metadata::MetadataCallbackRegistry::new())?;
        vm.data()
            .insert(crate::plugins::mix::MixCallbackRegistry::new())?;
        vm.data()
            .insert(crate::plugins::playback_sessions::PlaybackUpdateCallbackStore::new())?;

        let require = LuauRequireRuntime::new(
            loader,
            ManifestCapabilityPolicy::from_manifests(manifests.clone()),
        );
        register_generic_modules(&require, module_overrides)?;
        vm.data().insert(require)?;
        vm.data()
            .insert(crate::plugins::api::ApiRouteStore::new())?;

        for globals in harmony_globals::plugin_log_global_specs() {
            install_luau_globals(&vm, &ChunkOrigin::default(), &globals)?;
        }
        install_luau_require(&vm, &ChunkOrigin::default())?;

        let tokio_runtime = TokioRuntimeContext::new()?;

        Ok(Self {
            vm,
            plugins,
            tokio_runtime,
            websocket_tasks: RefCell::new(HashMap::<TaskIdKey, Arc<WebSocketState>>::new()),
        })
    }

    pub(crate) fn plugin_manifests(&self) -> Vec<PluginManifest> {
        let mut manifests = self
            .plugins
            .iter()
            .map(|plugin| plugin.manifest.clone())
            .collect::<Vec<_>>();
        manifests.sort_by(|left, right| left.id.cmp(&right.id));
        manifests
    }

    pub(crate) fn has_plugin(&self, plugin_id: &str) -> bool {
        self.plugins
            .iter()
            .any(|plugin| plugin.manifest.id == plugin_id)
    }

    pub(crate) fn exec_plugin(&self, plugin_id: &str) -> Result<()> {
        let plugin = self
            .plugins
            .iter()
            .find(|plugin| plugin.manifest.id == plugin_id)
            .ok_or_else(|| anyhow::anyhow!("plugin not found: {plugin_id}"))?;
        self.exec_loaded_plugin(plugin)
    }

    pub(crate) fn exec_all(&self) -> Result<()> {
        for plugin in self.plugins.iter() {
            match self.exec_loaded_plugin(plugin) {
                Ok(()) => tracing::debug!("plugin '{}' executed", plugin.manifest.id),
                Err(error) => tracing::warn!("plugin '{}' error: {error}", plugin.manifest.id),
            }
        }
        Ok(())
    }

    fn exec_loaded_plugin(&self, plugin: &LoadedPlugin) -> Result<()> {
        let bytes = fs::read(&plugin.entrypoint_path).with_context(|| {
            format!(
                "load plugin '{}' entrypoint from {}",
                plugin.manifest.id,
                plugin.entrypoint_path.display()
            )
        })?;
        self.run_plugin_source(
            plugin.manifest.id.as_str(),
            plugin.manifest.entrypoint.as_str(),
            bytes,
        )
    }

    #[cfg(test)]
    pub(crate) fn eval_plugin_source(
        &self,
        plugin_id: impl Into<Arc<str>>,
        path: impl Into<Arc<str>>,
        source: impl Into<Arc<[u8]>>,
    ) -> Result<Vec<luau::Value>> {
        let origin = plugin_origin(plugin_id, path);
        self.eval_plugin_source_with_call_context(
            source,
            CallContext {
                origin,
                ..CallContext::default()
            },
        )
    }

    pub(crate) fn run_plugin_source(
        &self,
        plugin_id: impl Into<Arc<str>>,
        path: impl Into<Arc<str>>,
        source: impl Into<Arc<[u8]>>,
    ) -> Result<()> {
        let origin = plugin_origin(plugin_id, path);
        self.run_plugin_source_with_call_context(
            source,
            CallContext {
                origin,
                ..CallContext::default()
            },
        )
    }

    pub(super) fn run_plugin_source_with_call_context(
        &self,
        source: impl Into<Arc<[u8]>>,
        context: CallContext,
    ) -> Result<()> {
        self.eval_plugin_source_with_call_context(source, context)
            .map(|_| ())
    }

    fn eval_plugin_source_with_call_context(
        &self,
        source: impl Into<Arc<[u8]>>,
        context: CallContext,
    ) -> Result<Vec<luau::Value>> {
        let origin = context.origin.clone();

        let function = self
            .vm
            .load_chunk(&luau::Chunk::new(source, luau_origin(&origin)))?;
        let thread = self.vm.create_thread(&function)?;
        self.vm.sandbox_thread(&thread)?;
        let scheduler = self.vm.data().get::<LocalScheduler>()?;
        scheduler.spawn_luau_thread(context, self.vm.clone(), thread.clone(), Vec::new());
        drive_luau_thread(&self.tokio_runtime, &scheduler, &thread)
    }
}
