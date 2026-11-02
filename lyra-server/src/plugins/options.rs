// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use harmony_luau as luau;
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
};

use crate::services::options::{
    OptionChoice,
    OptionDeclaration,
    OptionType,
};

/// The table `declare_option` takes, shared by metadata providers and mixers.
pub(crate) struct OptionConfig;

impl LuauTypeInfo for OptionConfig {
    fn luau_type() -> LuauType {
        LuauType::literal("OptionConfig")
    }
}

impl DescribeInterface for OptionConfig {
    fn interface_descriptor() -> InterfaceDescriptor {
        let field = |name, ty, description| FieldDescriptor {
            name,
            ty,
            description,
        };
        let mut descriptor = InterfaceDescriptor::new("OptionConfig", None);
        descriptor.fields.extend([
            field("name", String::luau_type(), None),
            field("label", String::luau_type(), None),
            field(
                "type",
                LuauType::literal(r#""boolean" | "number" | "string" | "choice""#),
                Some("A `choice` is a string, one of `options`."),
            ),
            field(
                "options",
                LuauType::Optional(Box::new(LuauType::literal("{ OptionChoice }"))),
                Some("The values a `choice` takes, in display order; required for it alone."),
            ),
            field(
                "default",
                LuauType::literal("(boolean | string | number)?"),
                Some("Of the option's type; a `choice` defaults to one of its values."),
            ),
            field(
                "requires_settings",
                Option::<Vec<String>>::luau_type(),
                Some("Plugin settings that must be set for the option to be available."),
            ),
        ]);
        descriptor
    }
}

/// One value a `choice` option takes.
#[cfg(feature = "docgen")]
pub(crate) struct OptionChoiceConfig;

#[cfg(feature = "docgen")]
impl DescribeInterface for OptionChoiceConfig {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("OptionChoice", None);
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
        ]);
        descriptor
    }
}

fn error(message: impl std::fmt::Display) -> luau::Error {
    crate::plugins::runtime_error(format!("declare_option: {message}"))
}

fn required_string(table: &luau::Table, key: &str) -> luau::runtime::Result<String> {
    match table.get(key)? {
        luau::Value::String(bytes) => {
            let value = String::from_utf8(bytes).map_err(error)?;
            let value = value.trim();
            if value.is_empty() {
                return Err(error(format!("{key} must be non-empty")));
            }
            Ok(value.to_string())
        }
        other => Err(error(format!(
            "{key} must be a non-empty string, got {}",
            other.type_name()
        ))),
    }
}

/// Parses a `declare_option` config table.
pub(crate) fn parse_option_declaration(
    config: &luau::Table,
) -> luau::runtime::Result<OptionDeclaration> {
    let name = required_string(config, "name")?;
    let label = required_string(config, "label")?;
    let option_type = match required_string(config, "type")?.as_str() {
        "boolean" => OptionType::Boolean,
        "number" => OptionType::Number,
        "string" => OptionType::String,
        "choice" => OptionType::Choice(parse_choices(config)?),
        other => {
            return Err(error(format!(
                "unsupported type '{other}', expected 'boolean', 'number', 'string', or 'choice'"
            )));
        }
    };
    if !matches!(option_type, OptionType::Choice(_)) && config.get("options")? != luau::Value::Nil {
        return Err(error(format!(
            "options is only for a choice, not a {}",
            option_type.name()
        )));
    }
    let default = match (&option_type, config.get("default")?) {
        (_, luau::Value::Nil) => serde_json::Value::Null,
        (OptionType::Boolean, luau::Value::Boolean(value)) => serde_json::Value::Bool(value),
        (OptionType::Number, luau::Value::Integer(value)) => serde_json::json!(value),
        (OptionType::Number, luau::Value::Number(value)) => serde_json::json!(value),
        (OptionType::String, luau::Value::String(bytes)) => {
            serde_json::Value::String(String::from_utf8(bytes).map_err(error)?)
        }
        (OptionType::Choice(choices), luau::Value::String(bytes)) => {
            let value = String::from_utf8(bytes).map_err(error)?;
            crate::services::options::check_choice(
                &format!("default for '{name}'"),
                choices,
                &value,
            )
            .map_err(error)?;
            serde_json::Value::String(value)
        }
        (expected, actual) => {
            return Err(error(format!(
                "default for '{name}' must be a {}, got {}",
                match expected {
                    OptionType::Choice(_) => "string",
                    other => other.name(),
                },
                actual.type_name()
            )));
        }
    };
    let requires_settings = match config.get("requires_settings")? {
        luau::Value::Nil => Vec::new(),
        luau::Value::Table(table) => {
            let mut values = Vec::new();
            for value in table.array_values()? {
                let luau::Value::String(bytes) = value else {
                    return Err(error("requires_settings must be a string array"));
                };
                values.push(String::from_utf8(bytes).map_err(error)?);
            }
            values
        }
        other => {
            return Err(error(format!(
                "requires_settings must be a string array, got {}",
                other.type_name()
            )));
        }
    };

    Ok(OptionDeclaration {
        name,
        label,
        option_type,
        default,
        requires_settings,
    })
}

fn parse_choices(config: &luau::Table) -> luau::runtime::Result<Vec<OptionChoice>> {
    let luau::Value::Table(table) = config.get("options")? else {
        return Err(error("a choice needs options"));
    };
    let mut choices: Vec<OptionChoice> = Vec::new();
    for entry in table.array_values()? {
        let luau::Value::Table(entry) = entry else {
            return Err(error("options must be { value, label } tables"));
        };
        let value = required_string(&entry, "value")?;
        if choices.iter().any(|choice| choice.value == value) {
            return Err(error(format!("option value '{value}' is listed twice")));
        }
        let label = required_string(&entry, "label")?;
        choices.push(OptionChoice { value, label });
    }
    if choices.is_empty() {
        return Err(error("a choice needs options"));
    }
    Ok(choices)
}
