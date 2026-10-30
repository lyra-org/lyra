// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;

mod descriptors;

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

pub(super) struct PluginsModule;

impl PluginsModule {}

pub(super) fn plugin_id_spec() -> FunctionSpec {
    let spec = FunctionSpec::new("id");
    spec.call(plugin_id_callback)
}

fn plugin_id_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let Some(plugin_id) = frame.context.origin.plugin.as_deref() else {
        return Err(luau::Error::Runtime(
            "plugins.id must be called from plugin Lua code".to_string(),
        ));
    };
    frame.returns.write(plugin_id)?;
    Ok(())
}

pub(super) fn plugin_manifest_spec() -> FunctionSpec {
    let spec = FunctionSpec::new("manifest");
    spec.call(plugin_manifest_callback)
}

pub(super) fn plugin_list_spec() -> FunctionSpec {
    let spec = FunctionSpec::new("list");
    spec.call(plugin_list_callback)
}

pub(super) fn plugin_get_spec() -> FunctionSpec {
    let spec = FunctionSpec::new("get").arg_name("id");
    spec.call(plugin_get_callback)
}

pub(super) fn module_export() -> ModuleExport {
    ModuleExport::new(PluginsModule)
}

pub(crate) fn module_spec() -> ModuleSpec {
    let spec = ModuleSpec::new("lyra/plugins")
        .function(plugin_id_spec())
        .function(plugin_manifest_spec())
        .function(plugin_list_spec())
        .function(plugin_get_spec())
        .install(|_| Ok(module_export()));
    spec.function(crate::plugins::settings::declare_settings_spec())
        .function(crate::plugins::settings::declare_user_settings_spec())
        .userdata(crate::plugins::settings::user_settings_accessor_spec())
}

fn plugin_manifest_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let Some(plugin_id) = frame.context.origin.plugin.as_deref() else {
        return Err(luau::Error::Runtime(
            "plugins.manifest must be called from plugin Lua code".to_string(),
        ));
    };
    let manifests = frame.vm.data().get::<PluginManifestModuleStore>()?;
    let Some(manifest) = manifests.find(plugin_id) else {
        return Err(luau::Error::Runtime(format!(
            "plugin manifest not found: {plugin_id}"
        )));
    };
    frame.returns.write(manifest_table(manifest))?;
    Ok(())
}

fn plugin_list_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let manifests = frame.vm.data().get::<PluginManifestModuleStore>()?;
    let table = luau::OwnedTable::from_array(manifests.iter().map(manifest_table));
    frame.returns.write(table)?;
    Ok(())
}

fn plugin_get_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let id: String = frame.args.read_named("id")?;
    let id = id.trim();
    if id.is_empty() {
        return Err(luau::Error::Runtime("id must not be empty".to_string()));
    }

    let manifests = frame.vm.data().get::<PluginManifestModuleStore>()?;
    match manifests.find(id) {
        Some(manifest) => frame.returns.write(manifest_table(manifest))?,
        None => frame.returns.write(luau::Value::Nil)?,
    }
    Ok(())
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
