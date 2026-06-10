// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
};
#[cfg(feature = "docgen")]
use harmony_luau::{
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
};

#[cfg(feature = "docgen")]
use crate::plugins::settings;

pub(super) struct PluginManifest;

impl LuauTypeInfo for PluginManifest {
    fn luau_type() -> LuauType {
        LuauType::literal("PluginManifest")
    }
}

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
fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "Plugins",
        local_name: "plugins",
        description: None,
        fields: Vec::new(),
        functions: vec![
            settings::descriptors::declare_settings_descriptor(),
            settings::descriptors::declare_user_settings_descriptor(),
            ModuleFunctionDescriptor {
                path: vec!["id"],
                description: None,
                params: vec![],
                returns: vec![String::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["manifest"],
                description: None,
                params: vec![],
                returns: vec![PluginManifest::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["list"],
                description: None,
                params: vec![],
                returns: vec![Vec::<PluginManifest>::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["get"],
                description: None,
                params: vec![param("id", String::luau_type())],
                returns: vec![Option::<PluginManifest>::luau_type()],
                yields: false,
            },
        ],
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    let type_aliases = settings::descriptors::type_alias_descriptors().to_vec();
    let mut interfaces = vec![PluginManifest::interface_descriptor()];
    interfaces.extend(settings::descriptors::interface_descriptors());
    let classes = settings::descriptors::class_descriptors();

    render_definition_file_with_support(&module_descriptor(), &type_aliases, &interfaces, &classes)
}

#[cfg(feature = "docgen")]
fn param(name: &'static str, ty: LuauType) -> ParameterDescriptor {
    ParameterDescriptor {
        name,
        ty,
        description: None,
        variadic: false,
    }
}
