// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use harmony_luau::{
    ClassDescriptor,
    DescribeInterface,
    DescribeTypeAlias,
    DescribeUserData,
    FieldDescriptor,
    FunctionParameter,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
    MethodDescriptor,
    MethodKind,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    TypeAliasDescriptor,
};

use super::luau::SettingsBuilder;

pub(super) struct SettingsConfig;

impl LuauTypeInfo for SettingsConfig {
    fn luau_type() -> LuauType {
        LuauType::Map {
            key: Box::new(String::luau_type()),
            value: Box::new(LuauType::optional(LuauType::union(vec![
                String::luau_type(),
                f64::luau_type(),
                bool::luau_type(),
            ]))),
        }
    }
}

impl DescribeTypeAlias for SettingsConfig {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "SettingsConfig",
            Self::luau_type(),
            Some("Settings configuration table returned by declare_settings."),
        )
    }
}

pub(super) struct SettingsCallback;

impl LuauTypeInfo for SettingsCallback {
    fn luau_type() -> LuauType {
        LuauType::function(
            vec![FunctionParameter {
                name: Some("ui"),
                ty: LuauType::literal("SettingsBuilder"),
                variadic: false,
            }],
            vec![],
        )
    }
}

impl DescribeTypeAlias for SettingsCallback {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "SettingsCallback",
            Self::luau_type(),
            Some("Callback function that receives a SettingsBuilder to declare plugin settings."),
        )
    }
}

pub(super) struct UserSettingsAccessor;

impl LuauTypeInfo for UserSettingsAccessor {
    fn luau_type() -> LuauType {
        LuauType::literal("UserSettingsAccessor")
    }
}

impl DescribeUserData for UserSettingsAccessor {
    fn class_descriptor() -> ClassDescriptor {
        ClassDescriptor {
            name: "UserSettingsAccessor",
            description: None,
            fields: vec![],
            methods: vec![MethodDescriptor {
                name: "get",
                description: None,
                params: vec![ParameterDescriptor {
                    name: "user_id",
                    ty: i64::luau_type(),
                    description: None,
                    variadic: false,
                }],
                returns: vec![SettingsConfig::luau_type()],
                yields: true,
                kind: MethodKind::Instance,
            }],
        }
    }
}

struct SettingsChoiceOption;

struct SettingsStringProps;

struct SettingsNumberProps;

struct SettingsBoolProps;

struct SettingsChoiceProps;

impl DescribeUserData for SettingsBuilder {
    fn class_descriptor() -> ClassDescriptor {
        ClassDescriptor {
            name: "SettingsBuilder",
            description: Some("Builder for declaring plugin settings."),
            fields: vec![],
            methods: vec![
                MethodDescriptor {
                    name: "group",
                    description: Some("Starts a settings group."),
                    params: vec![
                        ParameterDescriptor {
                            name: "id",
                            ty: String::luau_type(),
                            description: Some("Stable group identifier."),
                            variadic: false,
                        },
                        ParameterDescriptor {
                            name: "label",
                            ty: String::luau_type(),
                            description: Some("Group heading text."),
                            variadic: false,
                        },
                    ],
                    returns: vec![],
                    yields: false,
                    kind: MethodKind::Instance,
                },
                MethodDescriptor {
                    name: "string",
                    description: Some("Declares a string setting."),
                    params: vec![
                        ParameterDescriptor {
                            name: "key",
                            ty: String::luau_type(),
                            description: Some("Unique setting key."),
                            variadic: false,
                        },
                        ParameterDescriptor {
                            name: "props",
                            ty: SettingsStringProps::luau_type(),
                            description: Some("Setting properties."),
                            variadic: false,
                        },
                    ],
                    returns: vec![<Option<String> as LuauTypeInfo>::luau_type()],
                    yields: false,
                    kind: MethodKind::Instance,
                },
                MethodDescriptor {
                    name: "number",
                    description: Some("Declares a number setting."),
                    params: vec![
                        ParameterDescriptor {
                            name: "key",
                            ty: String::luau_type(),
                            description: Some("Unique setting key."),
                            variadic: false,
                        },
                        ParameterDescriptor {
                            name: "props",
                            ty: SettingsNumberProps::luau_type(),
                            description: Some("Setting properties."),
                            variadic: false,
                        },
                    ],
                    returns: vec![<Option<f64> as LuauTypeInfo>::luau_type()],
                    yields: false,
                    kind: MethodKind::Instance,
                },
                MethodDescriptor {
                    name: "bool",
                    description: Some("Declares a boolean setting."),
                    params: vec![
                        ParameterDescriptor {
                            name: "key",
                            ty: String::luau_type(),
                            description: Some("Unique setting key."),
                            variadic: false,
                        },
                        ParameterDescriptor {
                            name: "props",
                            ty: SettingsBoolProps::luau_type(),
                            description: Some("Setting properties."),
                            variadic: false,
                        },
                    ],
                    returns: vec![<Option<bool> as LuauTypeInfo>::luau_type()],
                    yields: false,
                    kind: MethodKind::Instance,
                },
                MethodDescriptor {
                    name: "choice",
                    description: Some("Declares a single-choice setting."),
                    params: vec![
                        ParameterDescriptor {
                            name: "key",
                            ty: String::luau_type(),
                            description: Some("Unique setting key."),
                            variadic: false,
                        },
                        ParameterDescriptor {
                            name: "props",
                            ty: SettingsChoiceProps::luau_type(),
                            description: Some("Setting properties."),
                            variadic: false,
                        },
                    ],
                    returns: vec![<Option<String> as LuauTypeInfo>::luau_type()],
                    yields: false,
                    kind: MethodKind::Instance,
                },
            ],
        }
    }
}

impl LuauTypeInfo for SettingsChoiceOption {
    fn luau_type() -> LuauType {
        LuauType::literal("SettingsChoiceOption")
    }
}

impl DescribeInterface for SettingsChoiceOption {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("SettingsChoiceOption", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "value",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "label",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "description",
                ty: Option::<String>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

fn settings_common_fields() -> Vec<FieldDescriptor> {
    vec![
        FieldDescriptor {
            name: "label",
            ty: String::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "description",
            ty: Option::<String>::luau_type(),
            description: None,
        },
    ]
}

impl LuauTypeInfo for SettingsStringProps {
    fn luau_type() -> LuauType {
        LuauType::literal("SettingsStringProps")
    }
}

impl DescribeInterface for SettingsStringProps {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("SettingsStringProps", None);
        descriptor.fields.extend(settings_common_fields());
        descriptor.fields.extend([
            FieldDescriptor {
                name: "default",
                ty: Option::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "required",
                ty: Option::<bool>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for SettingsNumberProps {
    fn luau_type() -> LuauType {
        LuauType::literal("SettingsNumberProps")
    }
}

impl DescribeInterface for SettingsNumberProps {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("SettingsNumberProps", None);
        descriptor.fields.extend(settings_common_fields());
        descriptor.fields.extend([
            FieldDescriptor {
                name: "default",
                ty: Option::<f64>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "min",
                ty: Option::<f64>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "max",
                ty: Option::<f64>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "required",
                ty: Option::<bool>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for SettingsBoolProps {
    fn luau_type() -> LuauType {
        LuauType::literal("SettingsBoolProps")
    }
}

impl DescribeInterface for SettingsBoolProps {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("SettingsBoolProps", None);
        descriptor.fields.extend(settings_common_fields());
        descriptor.fields.extend([
            FieldDescriptor {
                name: "default",
                ty: Option::<bool>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "required",
                ty: Option::<bool>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for SettingsChoiceProps {
    fn luau_type() -> LuauType {
        LuauType::literal("SettingsChoiceProps")
    }
}

impl DescribeInterface for SettingsChoiceProps {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("SettingsChoiceProps", None);
        descriptor.fields.extend(settings_common_fields());
        descriptor.fields.extend([
            FieldDescriptor {
                name: "default",
                ty: Option::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "options",
                ty: Vec::<SettingsChoiceOption>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "required",
                ty: Option::<bool>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

pub(crate) fn declare_settings_descriptor() -> ModuleFunctionDescriptor {
    ModuleFunctionDescriptor {
        path: vec!["declare_settings"],
        description: None,
        params: vec![param("callback", SettingsCallback::luau_type())],
        returns: vec![SettingsConfig::luau_type()],
        yields: true,
    }
}

pub(crate) fn declare_user_settings_descriptor() -> ModuleFunctionDescriptor {
    ModuleFunctionDescriptor {
        path: vec!["declare_user_settings"],
        description: None,
        params: vec![param("callback", SettingsCallback::luau_type())],
        returns: vec![UserSettingsAccessor::luau_type()],
        yields: true,
    }
}

pub(crate) fn type_alias_descriptors() -> [TypeAliasDescriptor; 2] {
    [
        SettingsConfig::type_alias_descriptor(),
        SettingsCallback::type_alias_descriptor(),
    ]
}

pub(crate) fn interface_descriptors() -> [InterfaceDescriptor; 5] {
    [
        SettingsChoiceOption::interface_descriptor(),
        SettingsStringProps::interface_descriptor(),
        SettingsNumberProps::interface_descriptor(),
        SettingsBoolProps::interface_descriptor(),
        SettingsChoiceProps::interface_descriptor(),
    ]
}

pub(crate) fn class_descriptors() -> [ClassDescriptor; 2] {
    [
        SettingsBuilder::class_descriptor(),
        UserSettingsAccessor::class_descriptor(),
    ]
}

fn param(name: &'static str, ty: LuauType) -> ParameterDescriptor {
    ParameterDescriptor {
        name,
        ty,
        description: None,
        variadic: false,
    }
}
