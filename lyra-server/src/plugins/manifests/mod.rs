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

use self::descriptors::PluginManifest;
pub(crate) use self::descriptors::render_luau_definition;

#[cfg(test)]
mod tests;

#[derive(Clone, Default)]
pub(crate) struct PluginManifestModuleStore {
    manifests: Arc<[harmony_core::PluginManifest]>,
}

impl PluginManifestModuleStore {
    pub(crate) fn new(manifests: Arc<[harmony_core::PluginManifest]>) -> Self {
        Self { manifests }
    }

    fn iter(&self) -> impl Iterator<Item = &harmony_core::PluginManifest> {
        self.manifests.iter()
    }

    fn find(&self, id: &str) -> Option<&harmony_core::PluginManifest> {
        self.manifests.iter().find(|manifest| manifest.id == id)
    }
}

pub(super) struct PluginsModule;

impl PluginsModule {}

pub(super) fn plugin_id_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("id").returns::<String>();
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
    let spec = FunctionSpec::sync_fn("manifest").returns::<PluginManifest>();
    spec.call(plugin_manifest_callback)
}

pub(super) fn plugin_list_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("list").returns::<Vec<PluginManifest>>();
    spec.call(plugin_list_callback)
}

pub(super) fn plugin_get_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("get")
        .arg_name("id")
        .args::<String>()
        .returns::<Option<PluginManifest>>();
    spec.call(plugin_get_callback)
}

pub(super) fn module_export() -> ModuleExport {
    ModuleExport::new(PluginsModule)
}

pub(crate) fn module_spec() -> ModuleSpec {
    let spec = ModuleSpec::new("lyra/plugins")
        .capability("lyra.plugins")
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
    let mut table = luau::OwnedTable::with_capacity(manifests.manifests.len(), 0);
    for manifest in manifests.iter() {
        table.push_array(luau::Value::TableData(manifest_table(manifest)));
    }
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

fn manifest_table(manifest: &harmony_core::PluginManifest) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 6);
    table.set_field(
        "schema_version",
        luau::Value::Integer(i64::from(manifest.schema_version)),
    );
    table.set_field("id", luau::Value::String(manifest.id.clone().into_bytes()));
    table.set_field(
        "name",
        luau::Value::String(manifest.name.clone().into_bytes()),
    );
    table.set_field(
        "version",
        luau::Value::String(manifest.version.clone().into_bytes()),
    );
    table.set_field(
        "description",
        luau::Value::String(manifest.description.clone().into_bytes()),
    );
    table.set_field(
        "entrypoint",
        luau::Value::String(manifest.entrypoint.clone().into_bytes()),
    );
    table
}
