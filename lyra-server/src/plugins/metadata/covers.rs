// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::time::Duration;

use harmony_luau::{
    FunctionParameter,
    JsonValue,
    LuauType,
    LuauTypeInfo,
    TypeAliasDescriptor,
};
use mlua::{
    Result,
    Table,
    Value,
};

use crate::services::{
    covers::providers::DEFAULT_COVER_HANDLER_TIMEOUT,
    providers::ProviderCoverRequireSpec,
};

#[harmony_macros::interface]
pub(crate) struct ProviderCoverRequire {
    all_of: Option<Vec<String>>,
    any_of: Option<Vec<String>>,
}

#[harmony_macros::interface]
pub(crate) struct ProviderCoverConfig {
    priority: Option<i64>,
    timeout_ms: Option<u64>,
    require: Option<ProviderCoverRequire>,
}

#[harmony_macros::interface]
pub(crate) struct ProviderCoverOptions {
    force_refresh: Option<bool>,
}

#[harmony_macros::interface]
pub(crate) struct ProviderCoverLibrary {
    db_id: Option<i64>,
    name: Option<String>,
    directory: Option<String>,
    language: Option<String>,
    country: Option<String>,
}

#[harmony_macros::interface]
pub(crate) struct ProviderCoverArtist {
    db_id: Option<i64>,
    artist_name: Option<String>,
    sort_name: Option<String>,
}

#[harmony_macros::interface]
pub(crate) struct ProviderCoverTrack {
    db_id: Option<i64>,
    track_title: String,
    sort_title: Option<String>,
    disc: Option<u32>,
    track: Option<u32>,
    track_total: Option<u32>,
    duration_ms: Option<u64>,
}

#[harmony_macros::interface]
pub(crate) struct ProviderCoverContext {
    db_id: Option<i64>,
    release_title: Option<String>,
    sort_title: Option<String>,
    release_date: Option<String>,
    tracks: Option<Vec<ProviderCoverTrack>>,
    artists: Option<Vec<ProviderCoverArtist>>,
    artist_names: Option<Vec<String>>,
    ids: Option<std::collections::HashMap<String, String>>,
    library: Option<ProviderCoverLibrary>,
    cover_options: Option<ProviderCoverOptions>,
}

pub(super) struct ProviderCoverHandler;

impl LuauTypeInfo for ProviderCoverHandler {
    fn luau_type() -> LuauType {
        LuauType::literal("ProviderCoverHandler")
    }
}

impl ProviderCoverHandler {
    fn handler_type() -> LuauType {
        LuauType::function(
            vec![FunctionParameter {
                name: Some("ctx"),
                ty: LuauType::literal("ProviderCoverContext"),
                variadic: false,
            }],
            vec![JsonValue::luau_type()],
        )
    }
}

impl harmony_luau::DescribeTypeAlias for ProviderCoverHandler {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "ProviderCoverHandler",
            Self::handler_type(),
            Some(
                "Release cover handler. Return nil, a cover URL string, a candidate object, or a candidates payload.",
            ),
        )
    }
}

pub(super) fn parse_cover_path_list(field: &str, value: Value) -> Result<Vec<String>> {
    let Value::Table(entries) = value else {
        return Err(mlua::Error::runtime(format!(
            "provider:cover require.{field} must be an array of strings",
        )));
    };
    let mut parsed = Vec::new();
    for value in entries.sequence_values::<Value>() {
        let value = value?;
        let Value::String(path) = value else {
            return Err(mlua::Error::runtime(format!(
                "provider:cover require.{field} must contain only strings",
            )));
        };
        let path = path
            .to_str()
            .map_err(|_| {
                mlua::Error::runtime(format!(
                    "provider:cover require.{field} must be utf-8 strings",
                ))
            })?
            .trim()
            .to_string();
        if path.is_empty() {
            return Err(mlua::Error::runtime(format!(
                "provider:cover require.{field} must contain non-empty strings",
            )));
        }
        if !parsed.contains(&path) {
            parsed.push(path);
        }
    }

    Ok(parsed)
}

pub(super) fn parse_cover_require_spec(require: Table) -> Result<ProviderCoverRequireSpec> {
    let all_of = match require.get::<Value>("all_of")? {
        Value::Nil => Vec::new(),
        value => parse_cover_path_list("all_of", value)?,
    };
    let any_of = match require.get::<Value>("any_of")? {
        Value::Nil => Vec::new(),
        value => parse_cover_path_list("any_of", value)?,
    };
    Ok(ProviderCoverRequireSpec { all_of, any_of })
}

pub(super) fn parse_cover_spec(config: Table) -> Result<(i64, Duration, ProviderCoverRequireSpec)> {
    let priority = match config.get::<Value>("priority")? {
        Value::Nil => 50,
        Value::Integer(value) => value,
        Value::Number(value) => {
            if !value.is_finite() || value.fract() != 0.0 {
                return Err(mlua::Error::runtime(
                    "provider:cover config.priority must be an integer number",
                ));
            }
            value as i64
        }
        _ => {
            return Err(mlua::Error::runtime(
                "provider:cover config.priority must be a number",
            ));
        }
    };

    let timeout = match config.get::<Value>("timeout_ms")? {
        Value::Nil => DEFAULT_COVER_HANDLER_TIMEOUT,
        Value::Integer(value) => {
            if value < 1 {
                return Err(mlua::Error::runtime(
                    "provider:cover config.timeout_ms must be >= 1",
                ));
            }
            Duration::from_millis(value as u64)
        }
        Value::Number(value) => {
            if !value.is_finite() || value.fract() != 0.0 || value < 1.0 {
                return Err(mlua::Error::runtime(
                    "provider:cover config.timeout_ms must be an integer >= 1",
                ));
            }
            Duration::from_millis(value as u64)
        }
        _ => {
            return Err(mlua::Error::runtime(
                "provider:cover config.timeout_ms must be a positive integer when set",
            ));
        }
    };

    let require = match config.get::<Value>("require")? {
        Value::Nil => ProviderCoverRequireSpec::default(),
        Value::Table(require) => parse_cover_require_spec(require)?,
        _ => {
            return Err(mlua::Error::runtime(
                "provider:cover config.require must be a table",
            ));
        }
    };

    Ok((priority, timeout, require))
}

pub(super) fn interface_descriptors() -> Vec<harmony_luau::InterfaceDescriptor> {
    vec![
        ProviderCoverRequire::interface_descriptor(),
        ProviderCoverConfig::interface_descriptor(),
        ProviderCoverOptions::interface_descriptor(),
        ProviderCoverLibrary::interface_descriptor(),
        ProviderCoverArtist::interface_descriptor(),
        ProviderCoverTrack::interface_descriptor(),
        ProviderCoverContext::interface_descriptor(),
    ]
}

use harmony_luau::DescribeInterface;
