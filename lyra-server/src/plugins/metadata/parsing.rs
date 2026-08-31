// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::time::Duration;

use agdb::DbId;
use harmony_core::CallContext;
use harmony_luau as luau;
use serde_json::Value as JsonValue;

use crate::plugins::db as server_db;
use crate::plugins::lifecycle::PluginId;
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
    },
};

pub(super) fn core_call_context(context: &luau::CallContext) -> CallContext {
    let mut caller = harmony_core::ContextBag::default();
    for (type_id, value) in context.caller.cloned_entries() {
        caller.insert_shared(type_id, value);
    }

    CallContext {
        origin: harmony_core::ChunkOrigin {
            module: context
                .origin
                .module
                .as_ref()
                .map(|module| harmony_core::ModuleId(module.0.clone())),
            plugin: context.origin.plugin.clone(),
            path: context.origin.path.clone(),
        },
        capability: context
            .capability
            .as_ref()
            .map(|capability| harmony_core::CapabilityId(capability.0.clone())),
        caller,
        task_group: harmony_core::TaskGroupId(context.task_group.0),
    }
}

pub(super) fn require_positive_id(value: i64, label: &str) -> luau::runtime::Result<DbId> {
    if value <= 0 {
        return Err(crate::plugins::runtime_error(format!(
            "{label} must be a positive integer, got {value}"
        )));
    }
    Ok(DbId(value))
}

pub(super) fn entity_type_for_node(
    db: &agdb::DbAny,
    node_id: DbId,
) -> anyhow::Result<Option<EntityType>> {
    if server_db::releases::get_by_id(db, node_id)?.is_some() {
        return Ok(Some(EntityType::Release));
    }
    if server_db::artists::get_by_id(db, node_id)?.is_some() {
        return Ok(Some(EntityType::Artist));
    }
    if server_db::tracks::get_by_id(db, node_id)?.is_some() {
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
    context: &luau::CallContext,
    plugin_id: &PluginId,
    provider_id: &str,
) -> luau::runtime::Result<()> {
    let caller = context.origin.plugin.as_deref();
    if caller == Some(plugin_id.as_ref()) {
        return Ok(());
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
    let unique = optional_bool(vm, spec, "unique")?.unwrap_or(false);
    Ok(ProviderIdSpec {
        id_type,
        entity,
        unique,
    })
}

pub(super) fn parse_cover_spec(
    vm: &luau::Vm,
    config: &luau::Table,
) -> luau::runtime::Result<(i64, Duration, ProviderRequireSpec)> {
    let priority = optional_i64(vm, config, "priority", "provider:cover")?.unwrap_or(50);
    let timeout = optional_timeout(
        vm,
        config,
        "timeout_ms",
        "provider:cover",
        DEFAULT_COVER_HANDLER_TIMEOUT,
    )?;
    let require = parse_provider_require(vm, config, "provider:cover")?;
    Ok((priority, timeout, require))
}

pub(super) fn parse_similar_releases_spec(
    vm: &luau::Vm,
    config: &luau::Table,
) -> luau::runtime::Result<(Duration, ProviderRequireSpec)> {
    let timeout = optional_timeout(
        vm,
        config,
        "timeout_ms",
        "provider:similar_releases",
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
) -> luau::runtime::Result<(i32, Duration, ProviderRequireSpec)> {
    let priority = optional_i64(vm, config, "priority", "provider:lyrics")?.unwrap_or(50);
    let priority = i32::try_from(priority).map_err(|_| {
        crate::plugins::runtime_error("provider:lyrics config.priority must fit in i32")
    })?;
    let timeout = optional_timeout(
        vm,
        config,
        "timeout_ms",
        "provider:lyrics",
        DEFAULT_HANDLER_TIMEOUT,
    )?;
    let require = parse_provider_require(vm, config, "provider:lyrics")?;
    Ok((priority, timeout, require))
}

fn optional_i64(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
    method: &str,
) -> luau::runtime::Result<Option<i64>> {
    match table.get_raw(vm, key)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Integer(value) => Ok(Some(value)),
        luau::Value::Number(value) if value.is_finite() && value.fract() == 0.0 => {
            Ok(Some(value as i64))
        }
        other => Err(crate::plugins::runtime_error(format!(
            "{method} config.{key} must be an integer number, got {}",
            other.type_name()
        ))),
    }
}

fn optional_timeout(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
    method: &str,
    default: Duration,
) -> luau::runtime::Result<Duration> {
    let Some(value) = optional_i64(vm, table, key, method)? else {
        return Ok(default);
    };
    if value < 1 {
        return Err(crate::plugins::runtime_error(format!(
            "{method} config.{key} must be an integer >= 1"
        )));
    }
    Ok(Duration::from_millis(value as u64))
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
    if template.is_empty() {
        return Err(crate::plugins::runtime_error(
            "provider:id URL template must be a non-empty string",
        ));
    }
    if template.matches("{id}").count() != 1 {
        return Err(crate::plugins::runtime_error(
            "provider:id URL template must contain exactly one {id} placeholder",
        ));
    }
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
    EntityType::_harmony_userdata_class().read_value(vm, key, value)
}

pub(super) fn optional_artist_type(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &'static str,
) -> luau::runtime::Result<Option<server_db::ArtistType>> {
    let value = table.get_raw(vm, key)?;
    if matches!(value, luau::Value::Nil) {
        return Ok(None);
    }
    server_db::ArtistType::_harmony_userdata_class()
        .read_value(vm, key, value)
        .map(Some)
}

fn optional_bool(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Option<bool>> {
    match table.get_raw(vm, key)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Boolean(value) => Ok(Some(value)),
        other => Err(crate::plugins::runtime_error(format!(
            "{key} must be a boolean, got {}",
            other.type_name()
        ))),
    }
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
