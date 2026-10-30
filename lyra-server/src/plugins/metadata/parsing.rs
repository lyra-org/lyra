// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::time::Duration;

use agdb::DbId;
use harmony_luau as luau;
use serde_json::Value as JsonValue;

use crate::plugins::args::{
    optional_bool,
    optional_timeout,
};
use crate::services::plugin_scope::PluginId;
use crate::services::{
    EntityType,
    covers::providers::DEFAULT_COVER_HANDLER_TIMEOUT,
    metadata::lyrics::providers::DEFAULT_HANDLER_TIMEOUT,
    options::{
        OptionDeclaration,
        OptionType,
    },
    providers::{
        DEFAULT_SIMILAR_RELEASES_HANDLER_TIMEOUT,
        MAX_SIMILAR_RELEASES_HANDLER_TIMEOUT,
        ProviderIdSpec,
        ProviderRequireSpec,
        validate_id_link_template,
        validate_id_scheme,
    },
};

pub(super) fn entity_type_for_node(
    db: &agdb::DbAny,
    node_id: DbId,
) -> anyhow::Result<Option<EntityType>> {
    if lyra_db::releases::get_by_id(db, node_id)?.is_some() {
        return Ok(Some(EntityType::Release));
    }
    if lyra_db::artists::get_by_id(db, node_id)?.is_some() {
        return Ok(Some(EntityType::Artist));
    }
    if lyra_db::tracks::get_by_id(db, node_id)?.is_some() {
        return Ok(Some(EntityType::Track));
    }

    Ok(None)
}

pub(super) fn required_table_string(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
    method: &str,
) -> luau::runtime::Result<String> {
    optional_table_string(vm, table, key, method)?.ok_or_else(|| {
        crate::plugins::runtime_error(format!("{method}: {key} must be a non-empty string"))
    })
}

pub(super) fn optional_table_string(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
    method: &str,
) -> luau::runtime::Result<Option<String>> {
    match table.get_raw(vm, key)? {
        luau::Value::Nil => Ok(None),
        luau::Value::String(bytes) => {
            let value = String::from_utf8(bytes).map_err(crate::plugins::runtime_error)?;
            let trimmed = value.trim().to_string();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed))
            }
        }
        other => Err(crate::plugins::runtime_error(format!(
            "{method}: {key} must be a string, got {}",
            other.type_name()
        ))),
    }
}

pub(super) fn parse_custom_field_version(raw: &str) -> luau::runtime::Result<u64> {
    let version = raw.trim();
    let Some(number) = version.strip_prefix('v') else {
        return Err(crate::plugins::runtime_error(
            "custom fields version must be formatted as vN with N a positive integer",
        ));
    };
    if number.is_empty()
        || (number.len() > 1 && number.starts_with('0'))
        || !number.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(crate::plugins::runtime_error(
            "custom fields version must be formatted as vN with N a positive integer",
        ));
    }
    let parsed = number.parse::<u64>().map_err(|_| {
        crate::plugins::runtime_error(
            "custom fields version must be formatted as vN with N a positive integer",
        )
    })?;
    if parsed == 0 {
        return Err(crate::plugins::runtime_error(
            "custom fields version must be formatted as vN with N a positive integer",
        ));
    }
    Ok(parsed)
}

pub(super) fn ensure_provider_owner(
    vm: &luau::Vm,
    context: &luau::CallContext,
    plugin_id: &PluginId,
    provider_id: &str,
) -> luau::runtime::Result<()> {
    let caller = context.origin.plugin.as_deref();
    if caller == Some(plugin_id.as_ref()) {
        return harmony_core::ensure_current_plugin_run(vm, context);
    }

    Err(crate::plugins::runtime_error(format!(
        "provider '{provider_id}' method must be called by owning plugin '{plugin_id}'"
    )))
}

pub(super) fn parse_id_spec(
    vm: &luau::Vm,
    spec: &luau::Table,
) -> luau::runtime::Result<ProviderIdSpec> {
    let id_type = required_string(vm, spec, "id_type")?;
    let entity = required_entity_type(vm, spec, "entity")?;
    let unique = optional_bool(vm, spec, "unique", "provider:id spec")?.unwrap_or(false);
    let scheme = optional_table_string(vm, spec, "scheme", "provider:id")?;
    if let Some(scheme) = &scheme {
        validate_id_scheme(scheme)
            .map_err(|err| crate::plugins::runtime_error(format!("provider:id: {err}")))?;
    }
    let label = optional_table_string(vm, spec, "label", "provider:id")?;
    Ok(ProviderIdSpec {
        id_type,
        entity,
        unique,
        scheme,
        label,
    })
}

pub(super) fn parse_cover_spec(
    vm: &luau::Vm,
    config: &luau::Table,
) -> luau::runtime::Result<(Duration, ProviderRequireSpec)> {
    let timeout = optional_timeout(
        vm,
        config,
        "timeout_ms",
        "provider:cover config",
        DEFAULT_COVER_HANDLER_TIMEOUT,
    )?;
    let require = parse_provider_require(vm, config, "provider:cover")?;
    Ok((timeout, require))
}

pub(super) fn parse_similar_releases_spec(
    vm: &luau::Vm,
    config: &luau::Table,
) -> luau::runtime::Result<(Duration, ProviderRequireSpec)> {
    let timeout = optional_timeout(
        vm,
        config,
        "timeout_ms",
        "provider:similar_releases config",
        DEFAULT_SIMILAR_RELEASES_HANDLER_TIMEOUT,
    )?;
    if timeout > MAX_SIMILAR_RELEASES_HANDLER_TIMEOUT {
        return Err(crate::plugins::runtime_error(format!(
            "provider:similar_releases config.timeout_ms must be <= {}",
            MAX_SIMILAR_RELEASES_HANDLER_TIMEOUT.as_millis()
        )));
    }
    let require = parse_provider_require(vm, config, "provider:similar_releases")?;
    Ok((timeout, require))
}

pub(super) fn parse_lyrics_spec(
    vm: &luau::Vm,
    config: &luau::Table,
) -> luau::runtime::Result<(Duration, ProviderRequireSpec)> {
    let timeout = optional_timeout(
        vm,
        config,
        "timeout_ms",
        "provider:lyrics config",
        DEFAULT_HANDLER_TIMEOUT,
    )?;
    let require = parse_provider_require(vm, config, "provider:lyrics")?;
    Ok((timeout, require))
}

fn parse_provider_require(
    vm: &luau::Vm,
    config: &luau::Table,
    method: &str,
) -> luau::runtime::Result<ProviderRequireSpec> {
    match config.get_raw(vm, "require")? {
        luau::Value::Nil => Ok(ProviderRequireSpec::default()),
        luau::Value::Table(require) => Ok(ProviderRequireSpec {
            all_of: parse_require_paths(vm, &require, "all_of", method)?,
            any_of: parse_require_paths(vm, &require, "any_of", method)?,
        }),
        other => Err(crate::plugins::runtime_error(format!(
            "{method} config.require must be a table, got {}",
            other.type_name()
        ))),
    }
}

fn parse_require_paths(
    vm: &luau::Vm,
    require: &luau::Table,
    key: &str,
    method: &str,
) -> luau::runtime::Result<Vec<String>> {
    match require.get_raw(vm, key)? {
        luau::Value::Nil => Ok(Vec::new()),
        luau::Value::Table(table) => {
            let mut parsed = Vec::new();
            for (_, value) in table.pairs_raw(vm)? {
                let luau::Value::String(bytes) = value else {
                    return Err(crate::plugins::runtime_error(format!(
                        "{method} require.{key} must contain only strings"
                    )));
                };
                let path = String::from_utf8(bytes)
                    .map_err(crate::plugins::runtime_error)?
                    .trim()
                    .to_string();
                if path.is_empty() {
                    return Err(crate::plugins::runtime_error(format!(
                        "{method} require.{key} must contain non-empty strings"
                    )));
                }
                if !parsed.contains(&path) {
                    parsed.push(path);
                }
            }
            Ok(parsed)
        }
        other => Err(crate::plugins::runtime_error(format!(
            "{method} require.{key} must be an array of strings, got {}",
            other.type_name()
        ))),
    }
}

pub(super) fn parse_id_url_template(bytes: Vec<u8>) -> luau::runtime::Result<String> {
    let template = String::from_utf8(bytes)
        .map_err(|_| crate::plugins::runtime_error("provider:id URL template must be utf-8"))?
        .trim()
        .to_string();
    validate_id_link_template(&template)
        .map_err(|err| crate::plugins::runtime_error(format!("provider:id {err}")))?;
    Ok(template)
}

pub(super) fn parse_option_declaration(
    vm: &luau::Vm,
    config: &luau::Table,
) -> luau::runtime::Result<OptionDeclaration> {
    let name = required_string(vm, config, "name")?;
    let label = required_string(vm, config, "label")?;
    let option_type = match required_string(vm, config, "type")?.as_str() {
        "boolean" => OptionType::Boolean,
        "string" => OptionType::String,
        "number" => OptionType::Number,
        other => {
            return Err(crate::plugins::runtime_error(format!(
                "declare_option: unsupported type '{other}', expected 'boolean', 'string', or 'number'"
            )));
        }
    };
    let default = match (option_type.clone(), config.get_raw(vm, "default")?) {
        (_, luau::Value::Nil) => JsonValue::Null,
        (OptionType::Boolean, luau::Value::Boolean(value)) => JsonValue::Bool(value),
        (OptionType::Number, luau::Value::Integer(value)) => serde_json::json!(value),
        (OptionType::Number, luau::Value::Number(value)) => serde_json::json!(value),
        (OptionType::String, luau::Value::String(bytes)) => {
            JsonValue::String(String::from_utf8(bytes).map_err(crate::plugins::runtime_error)?)
        }
        (expected, actual) => {
            return Err(crate::plugins::runtime_error(format!(
                "declare_option: default value type mismatch for '{name}': expected {expected:?}, got {}",
                actual.type_name()
            )));
        }
    };
    let requires_settings = match config.get_raw(vm, "requires_settings")? {
        luau::Value::Nil => Vec::new(),
        luau::Value::Table(table) => string_array_from_table(vm, &table)?,
        other => {
            return Err(crate::plugins::runtime_error(format!(
                "declare_option: requires_settings must be a string array, got {}",
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

fn required_string(vm: &luau::Vm, table: &luau::Table, key: &str) -> luau::runtime::Result<String> {
    match table.get_raw(vm, key)? {
        luau::Value::String(bytes) => {
            let value = String::from_utf8(bytes).map_err(crate::plugins::runtime_error)?;
            let trimmed = value.trim().to_string();
            if trimmed.is_empty() {
                Err(crate::plugins::runtime_error(format!(
                    "{key} must be non-empty"
                )))
            } else {
                Ok(trimmed)
            }
        }
        other => Err(crate::plugins::runtime_error(format!(
            "{key} must be a non-empty string, got {}",
            other.type_name()
        ))),
    }
}

fn required_entity_type(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &'static str,
) -> luau::runtime::Result<EntityType> {
    let value = table.get_raw(vm, key)?;
    crate::plugins::types::LuauEntityType::_harmony_userdata_class()
        .read_value(vm, key, value)
        .map(Into::into)
}

pub(super) fn optional_artist_type(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &'static str,
) -> luau::runtime::Result<Option<lyra_db::ArtistType>> {
    let value = table.get_raw(vm, key)?;
    if matches!(value, luau::Value::Nil) {
        return Ok(None);
    }
    crate::plugins::types::LuauArtistType::_harmony_userdata_class()
        .read_value(vm, key, value)
        .map(|value| Some(value.into()))
}

pub(super) fn string_array_from_table(
    vm: &luau::Vm,
    table: &luau::Table,
) -> luau::runtime::Result<Vec<String>> {
    let mut values = Vec::new();
    for (_, value) in table.pairs_raw(vm)? {
        let luau::Value::String(bytes) = value else {
            return Err(crate::plugins::runtime_error(
                "array values must be strings",
            ));
        };
        values.push(String::from_utf8(bytes).map_err(crate::plugins::runtime_error)?);
    }
    Ok(values)
}
