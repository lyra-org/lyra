// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::time::Duration;

use harmony_luau::{
    DescribeInterface,
    DescribeTypeAlias,
    FunctionParameter,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
    TypeAliasDescriptor,
};
use mlua::{
    Lua,
    LuaSerdeExt,
    Result,
    Table,
    Value,
};

use crate::{
    plugins::LUA_SERIALIZE_OPTIONS,
    services::metadata::lyrics::{
        providers::{
            DEFAULT_HANDLER_TIMEOUT,
            LyricsHandlerResult,
            LyricsRequireSpec,
            LyricsTrackContext,
        },
        scorer::LyricsHandlerCandidate,
    },
};

#[harmony_macros::interface]
pub(crate) struct ProviderLyricsRequire {
    all_of: Option<Vec<String>>,
    any_of: Option<Vec<String>>,
}

#[harmony_macros::interface]
pub(crate) struct ProviderLyricsConfig {
    priority: Option<i64>,
    timeout_ms: Option<u64>,
    require: Option<ProviderLyricsRequire>,
}

#[harmony_macros::interface]
pub(crate) struct ProviderLyricsContext {
    track_db_id: i64,
    track_name: String,
    artist_name: String,
    album_name: Option<String>,
    duration_ms: Option<u64>,
    external_ids:
        Option<std::collections::HashMap<String, std::collections::HashMap<String, String>>>,
    force_refresh: bool,
}

pub(super) struct ProviderLyricsHandler;

impl LuauTypeInfo for ProviderLyricsHandler {
    fn luau_type() -> LuauType {
        LuauType::literal("ProviderLyricsHandler")
    }
}

impl ProviderLyricsHandler {
    fn handler_type() -> LuauType {
        LuauType::function(
            vec![FunctionParameter {
                name: Some("ctx"),
                ty: LuauType::literal("ProviderLyricsContext"),
                variadic: false,
            }],
            // Validated by `parse_handler_result` against the closed kind set.
            vec![harmony_luau::JsonValue::luau_type()],
        )
    }
}

impl DescribeTypeAlias for ProviderLyricsHandler {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "ProviderLyricsHandler",
            Self::handler_type(),
            Some(
                "Lyrics handler. Return a table whose `kind` is one of \
                 \"hit\" (with `candidates` array of ≤5 entries), \"miss\", \
                 \"instrumental\", or \"rate_limited\" (with optional \
                 `retry_after_ms`).",
            ),
        )
    }
}

pub(super) fn parse_path_list(field: &str, value: Value) -> Result<Vec<String>> {
    let Value::Table(entries) = value else {
        return Err(mlua::Error::runtime(format!(
            "provider:lyrics require.{field} must be an array of strings",
        )));
    };
    let mut parsed = Vec::new();
    for value in entries.sequence_values::<Value>() {
        let value = value?;
        let Value::String(path) = value else {
            return Err(mlua::Error::runtime(format!(
                "provider:lyrics require.{field} must contain only strings",
            )));
        };
        let path = path
            .to_str()
            .map_err(|_| {
                mlua::Error::runtime(format!(
                    "provider:lyrics require.{field} must be utf-8 strings",
                ))
            })?
            .trim()
            .to_string();
        if path.is_empty() {
            return Err(mlua::Error::runtime(format!(
                "provider:lyrics require.{field} must contain non-empty strings",
            )));
        }
        if !parsed.contains(&path) {
            parsed.push(path);
        }
    }
    Ok(parsed)
}

pub(super) fn parse_require_spec(require: Table) -> Result<LyricsRequireSpec> {
    let all_of = match require.get::<Value>("all_of")? {
        Value::Nil => Vec::new(),
        value => parse_path_list("all_of", value)?,
    };
    let any_of = match require.get::<Value>("any_of")? {
        Value::Nil => Vec::new(),
        value => parse_path_list("any_of", value)?,
    };
    Ok(LyricsRequireSpec { all_of, any_of })
}

/// Returns `(priority, timeout, require)` parsed from the plugin config table.
pub(super) fn parse_lyrics_spec(
    config: Table,
) -> Result<(i32, Duration, LyricsRequireSpec)> {
    let priority_i64 = match config.get::<Value>("priority")? {
        Value::Nil => 50,
        Value::Integer(value) => value,
        Value::Number(value) => {
            if !value.is_finite() || value.fract() != 0.0 {
                return Err(mlua::Error::runtime(
                    "provider:lyrics config.priority must be an integer number",
                ));
            }
            value as i64
        }
        _ => {
            return Err(mlua::Error::runtime(
                "provider:lyrics config.priority must be a number",
            ));
        }
    };
    let priority = i32::try_from(priority_i64).map_err(|_| {
        mlua::Error::runtime("provider:lyrics config.priority must fit in i32")
    })?;

    let timeout = match config.get::<Value>("timeout_ms")? {
        Value::Nil => DEFAULT_HANDLER_TIMEOUT,
        Value::Integer(value) => {
            if value < 1 {
                return Err(mlua::Error::runtime(
                    "provider:lyrics config.timeout_ms must be >= 1",
                ));
            }
            Duration::from_millis(value as u64)
        }
        Value::Number(value) => {
            if !value.is_finite() || value.fract() != 0.0 || value < 1.0 {
                return Err(mlua::Error::runtime(
                    "provider:lyrics config.timeout_ms must be an integer >= 1",
                ));
            }
            Duration::from_millis(value as u64)
        }
        _ => {
            return Err(mlua::Error::runtime(
                "provider:lyrics config.timeout_ms must be a positive integer when set",
            ));
        }
    };

    let require = match config.get::<Value>("require")? {
        Value::Nil => LyricsRequireSpec::default(),
        Value::Table(require) => parse_require_spec(require)?,
        _ => {
            return Err(mlua::Error::runtime(
                "provider:lyrics config.require must be a table",
            ));
        }
    };

    Ok((priority, timeout, require))
}

pub(crate) fn parse_handler_result(lua: &Lua, value: Value) -> Result<LyricsHandlerResult> {
    let Value::Table(table) = value else {
        return Err(mlua::Error::runtime(
            "provider:lyrics handler must return a table",
        ));
    };

    let kind: String = match table.get::<Value>("kind")? {
        Value::String(s) => s
            .to_str()
            .map_err(|_| mlua::Error::runtime("provider:lyrics handler kind must be utf-8"))?
            .to_string(),
        Value::Nil => {
            return Err(mlua::Error::runtime(
                "provider:lyrics handler return is missing 'kind'",
            ));
        }
        _ => {
            return Err(mlua::Error::runtime(
                "provider:lyrics handler 'kind' must be a string",
            ));
        }
    };

    match kind.as_str() {
        "hit" => {
            let candidates_value: Value = table.get("candidates")?;
            let Value::Table(candidates_tbl) = candidates_value else {
                return Err(mlua::Error::runtime(
                    "provider:lyrics handler 'hit' requires a 'candidates' array",
                ));
            };
            let mut parsed: Vec<LyricsHandlerCandidate> = Vec::new();
            for entry in candidates_tbl.sequence_values::<Value>() {
                let entry = entry?;
                let candidate: LyricsHandlerCandidate = lua
                    .from_value(entry)
                    .map_err(|err| mlua::Error::runtime(format!(
                        "provider:lyrics 'hit' candidate did not match LyricsHandlerCandidate: {err}"
                    )))?;
                parsed.push(candidate);
            }
            if parsed.is_empty() {
                return Err(mlua::Error::runtime(
                    "provider:lyrics 'hit' must include at least one candidate",
                ));
            }
            if parsed.len() > 5 {
                return Err(mlua::Error::runtime(
                    "provider:lyrics 'hit' may include at most 5 candidates",
                ));
            }
            Ok(LyricsHandlerResult::Hit { candidates: parsed })
        }
        "miss" => Ok(LyricsHandlerResult::Miss),
        "instrumental" => Ok(LyricsHandlerResult::Instrumental),
        "rate_limited" => {
            let retry_after_ms = match table.get::<Value>("retry_after_ms")? {
                Value::Nil => None,
                Value::Integer(value) => {
                    if value < 0 {
                        return Err(mlua::Error::runtime(
                            "provider:lyrics 'rate_limited' retry_after_ms must be >= 0",
                        ));
                    }
                    Some(value as u64)
                }
                Value::Number(value) => {
                    if !value.is_finite() || value < 0.0 || value.fract() != 0.0 {
                        return Err(mlua::Error::runtime(
                            "provider:lyrics 'rate_limited' retry_after_ms must be a non-negative integer",
                        ));
                    }
                    Some(value as u64)
                }
                _ => {
                    return Err(mlua::Error::runtime(
                        "provider:lyrics 'rate_limited' retry_after_ms must be an integer when set",
                    ));
                }
            };
            Ok(LyricsHandlerResult::RateLimited { retry_after_ms })
        }
        other => Err(mlua::Error::runtime(format!(
            "provider:lyrics handler 'kind' must be one of 'hit', 'miss', 'instrumental', 'rate_limited' (got '{other}')"
        ))),
    }
}

pub(crate) fn track_context_to_lua(lua: &Lua, context: &LyricsTrackContext) -> Result<Value> {
    // snake_case JSON so path keys match what `LyricsRequireSpec` walks.
    let value = serde_json::json!({
        "track_db_id": context.track_db_id,
        "track_name": context.track_name,
        "artist_name": context.artist_name,
        "album_name": context.album_name,
        "duration_ms": context.duration_ms,
        "external_ids": context.external_ids,
        "force_refresh": context.force_refresh,
    });
    lua.to_value_with(&value, LUA_SERIALIZE_OPTIONS)
}

pub(super) fn interface_descriptors() -> Vec<InterfaceDescriptor> {
    vec![
        ProviderLyricsRequire::interface_descriptor(),
        ProviderLyricsConfig::interface_descriptor(),
        ProviderLyricsContext::interface_descriptor(),
    ]
}
