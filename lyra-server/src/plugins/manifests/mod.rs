// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use harmony_core::ChunkOrigin;
use harmony_luau as luau;

mod descriptors;

#[cfg(feature = "docgen")]
use self::descriptors::PluginManifest;
#[cfg(feature = "docgen")]
use crate::plugins::settings::descriptors::{
    SettingsCallback,
    SettingsConfig,
};
use crate::plugins::settings::{
    self,
    SettingsBuilder,
    UserSettingsAccessor,
};

#[cfg(feature = "docgen")]
pub(crate) use self::descriptors::render_luau_definition;

#[cfg(test)]
mod tests;

#[derive(Clone, Default)]
pub(crate) struct PluginManifestModuleStore {
    manifests: Arc<[harmony_core::plugin::PluginManifest]>,
}

impl PluginManifestModuleStore {
    pub(crate) fn new(manifests: Arc<[harmony_core::plugin::PluginManifest]>) -> Self {
        Self { manifests }
    }

    fn iter(&self) -> impl Iterator<Item = &harmony_core::plugin::PluginManifest> {
        self.manifests.iter()
    }

    fn find(&self, id: &str) -> Option<&harmony_core::plugin::PluginManifest> {
        self.manifests.iter().find(|manifest| manifest.id == id)
    }
}

pub(crate) struct PluginsModule;

#[harmony_macros::module(
    id = "lyra/plugins",
    docs_feature = "docgen",
    userdata(SettingsBuilder, UserSettingsAccessor)
)]
impl PluginsModule {
    #[harmony(args(callback: SettingsCallback), returns(SettingsConfig))]
    async fn declare_settings(
        callback: luau::Function,
        #[context] vm: luau::Vm,
        #[context] context: luau::CallContext,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        settings::declare_settings(callback, vm, context).await
    }

    #[harmony(args(callback: SettingsCallback), returns(UserSettingsAccessor))]
    async fn declare_user_settings(
        callback: luau::Function,
        #[context] vm: luau::Vm,
        #[context] context: luau::CallContext,
    ) -> luau::runtime::Result<luau::UserData> {
        settings::declare_user_settings(callback, vm, context).await
    }

    fn id(origin: &ChunkOrigin) -> luau::runtime::Result<String> {
        origin.plugin.as_deref().map(str::to_string).ok_or_else(|| {
            luau::Error::Runtime("plugins.id must be called from plugin Luau code".to_string())
        })
    }

    #[harmony(returns(PluginManifest))]
    fn manifest(vm: &luau::Vm, origin: &ChunkOrigin) -> luau::runtime::Result<luau::OwnedTable> {
        let Some(plugin_id) = origin.plugin.as_deref() else {
            return Err(luau::Error::Runtime(
                "plugins.manifest must be called from plugin Luau code".to_string(),
            ));
        };
        let manifests = vm.data().get::<PluginManifestModuleStore>()?;
        let Some(manifest) = manifests.find(plugin_id) else {
            return Err(luau::Error::Runtime(format!(
                "plugin manifest not found: {plugin_id}"
            )));
        };
        Ok(manifest_table(manifest))
    }

    #[harmony(returns(Vec<PluginManifest>))]
    fn list(vm: &luau::Vm) -> luau::runtime::Result<luau::OwnedTable> {
        let manifests = vm.data().get::<PluginManifestModuleStore>()?;
        Ok(luau::OwnedTable::from_array(
            manifests.iter().map(manifest_table),
        ))
    }

    #[harmony(returns(Option<PluginManifest>))]
    fn get(vm: &luau::Vm, id: String) -> luau::runtime::Result<luau::Value> {
        let id = id.trim();
        if id.is_empty() {
            return Err(luau::Error::Runtime("id must not be empty".to_string()));
        }

        let manifests = vm.data().get::<PluginManifestModuleStore>()?;
        Ok(match manifests.find(id) {
            Some(manifest) => manifest_table(manifest).into(),
            None => luau::Value::Nil,
        })
    }
}

fn manifest_table(manifest: &harmony_core::plugin::PluginManifest) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 6);
    table.set_field("schema_version", i64::from(manifest.schema_version));
    table.set_field("id", manifest.id.clone());
    table.set_field("name", manifest.name.clone());
    table.set_field("version", manifest.version.to_string());
    table.set_field("description", manifest.description.clone());
    table.set_field(
        "entrypoint",
        match &manifest.entrypoint {
            Some(entrypoint) => luau::Value::String(entrypoint.clone().into_bytes()),
            None => luau::Value::Nil,
        },
    );
    table
}
