// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
};
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeModule,
    render_definition_file_with_support,
};

#[cfg(feature = "docgen")]
use super::PluginsModule;
#[cfg(feature = "docgen")]
use crate::plugins::settings;

#[cfg(feature = "docgen")]
pub(super) struct PluginManifest;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for PluginManifest {
    fn luau_type() -> LuauType {
        LuauType::literal("PluginManifest")
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for PluginManifest {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("PluginManifest", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "schema_version",
                ty: u32::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "id",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "name",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "version",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "description",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "entrypoint",
                ty: Option::<String>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    let type_aliases = settings::descriptors::type_alias_descriptors().to_vec();
    let mut interfaces = vec![PluginManifest::interface_descriptor()];
    interfaces.extend(settings::descriptors::interface_descriptors());
    let classes = settings::descriptors::class_descriptors();

    render_definition_file_with_support(
        &PluginsModule::module_descriptor(),
        &type_aliases,
        &interfaces,
        &classes,
    )
}
