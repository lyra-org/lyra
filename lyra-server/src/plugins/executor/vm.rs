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
#[cfg(test)]
use harmony_core::CallContext;
use harmony_core::{
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
#[cfg(test)]
use harmony_luau as luau;

use super::{
    PluginExecutor,
    WebSocketState,
    messages::TaskIdKey,
    modules::{
        generic_module_specs,
        plugin_scope_ids,
    },
    stores::PluginModuleStores,
};

impl PluginExecutor {
    pub(crate) fn with_filesystem_sources(
        manifests: Arc<[PluginManifest]>,
        server_info: crate::plugins::server::ServerInfo,
        auth_capabilities: crate::plugins::auth::AuthCapabilities,
        source_root: impl Into<std::path::PathBuf>,
        plugins_dir: impl Into<std::path::PathBuf>,
    ) -> Result<Self> {
        Self::with_loader(
            manifests,
            server_info,
            auth_capabilities,
            PluginModuleStores::empty(),
            FilesystemSourceLoader::new(source_root, plugins_dir),
        )
    }

    #[cfg(test)]
    pub(crate) fn discover_from_plugins_dir_with_db_and_modules(
        plugins_dir: impl Into<PathBuf>,
        server_info: crate::plugins::server::ServerInfo,
        auth_capabilities: crate::plugins::auth::AuthCapabilities,
        db: crate::plugins::db::DbAsync,
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
            .module_specs(generic_module_specs(module_overrides))
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
                vm.data()
                    .insert(crate::plugins::metadata::MetadataCallbackRegistry::new())?;
                vm.data()
                    .insert(crate::plugins::mix::MixCallbackRegistry::new())?;
                vm.data().insert(
                    crate::plugins::playback_sessions::PlaybackUpdateCallbackStore::new(),
                )?;
                vm.data()
                    .insert(crate::plugins::api::ApiRouteStore::new())?;
                Ok(())
            })
            .build()?;

        Ok(Self {
            runtime,
            websocket_tasks: RefCell::new(HashMap::<TaskIdKey, Arc<WebSocketState>>::new()),
        })
    }

    pub(crate) fn plugin_manifests(&self) -> Vec<PluginManifest> {
        self.runtime.plugin_manifests()
    }

    pub(crate) fn has_plugin(&self, plugin_id: &str) -> bool {
        self.runtime.has_plugin(plugin_id)
    }

    pub(crate) fn exec_plugin(&self, plugin_id: &str) -> Result<()> {
        self.runtime.exec_plugin(plugin_id)
    }

    pub(crate) fn exec_all(&self) -> Result<()> {
        self.runtime.exec_all()
    }

    pub(crate) fn run_plugin_source(
        &self,
        plugin_id: impl Into<Arc<str>>,
        path: impl Into<Arc<str>>,
        source: impl Into<Arc<[u8]>>,
    ) -> Result<()> {
        self.runtime.run_plugin_source(plugin_id, path, source)
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
