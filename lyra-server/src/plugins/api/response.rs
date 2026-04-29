// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use axum::http::StatusCode;
use harmony_luau::{
    LuauType,
    LuauTypeInfo,
};
use image::ImageFormat;
use mlua::{
    FromLua,
    IntoLua,
    Lua,
    LuaSerdeExt,
    Table,
    Value,
};
use serde::Serialize;

use super::image::{
    ParsedImageTransformOptions,
    parse_image_transform_options,
};
use crate::plugins::LUA_SERIALIZE_OPTIONS;

#[derive(Clone, Debug)]
pub(super) enum LuaBinaryInput {
    String(mlua::String),
    Buffer(mlua::Buffer),
}

impl LuaBinaryInput {
    pub(super) fn into_lua_value(self) -> Value {
        match self {
            Self::String(text) => Value::String(text),
            Self::Buffer(buffer) => Value::Buffer(buffer),
        }
    }
}

impl FromLua for LuaBinaryInput {
    fn from_lua(value: Value, _lua: &Lua) -> mlua::Result<Self> {
        match value {
            Value::String(text) => Ok(Self::String(text)),
            Value::Buffer(buffer) => Ok(Self::Buffer(buffer)),
            other => Err(mlua::Error::FromLuaConversionError {
                from: other.type_name(),
                to: "(string | buffer)".to_string(),
                message: Some("expected raw byte payload".to_string()),
            }),
        }
    }
}

impl IntoLua for LuaBinaryInput {
    fn into_lua(self, _lua: &Lua) -> mlua::Result<Value> {
        Ok(self.into_lua_value())
    }
}

impl LuauTypeInfo for LuaBinaryInput {
    fn luau_type() -> LuauType {
        LuauType::union(vec![String::luau_type(), LuauType::literal("buffer")])
    }
}

#[derive(Default, Serialize)]
#[harmony_macros::interface]
pub(super) struct TrackServeOptions {
    pub(super) format: Option<String>,
    pub(super) preferred_codecs: Option<Vec<String>>,
    pub(super) bitrate_bps: Option<u32>,
    pub(super) sample_rate_hz: Option<u32>,
    pub(super) channels: Option<u32>,
    pub(super) prefer_vbr: Option<bool>,
    pub(super) start_offset_ms: Option<u64>,
}

#[derive(Default, Serialize)]
#[harmony_macros::interface]
pub(super) struct HlsServeOptions {
    pub(super) preferred_codecs: Option<Vec<String>>,
    pub(super) bitrate_bps: Option<u32>,
    pub(super) sample_rate_hz: Option<u32>,
    pub(super) channels: Option<u32>,
    pub(super) prefer_vbr: Option<bool>,
    pub(super) start_offset_ms: Option<u64>,
}

fn parse_string_list_field(options: &Table, key: &str) -> mlua::Result<Option<Vec<String>>> {
    let Some(values) = options.get::<Option<Vec<String>>>(key)? else {
        return Ok(None);
    };

    let values = values
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if values.is_empty() {
        Ok(None)
    } else {
        Ok(Some(values))
    }
}

pub(super) fn parse_track_serve_options(options: Option<Table>) -> mlua::Result<TrackServeOptions> {
    let Some(options) = options else {
        return Ok(TrackServeOptions::default());
    };

    let format = options
        .get::<Option<String>>("format")?
        .map(|format| format.trim().to_string())
        .filter(|format| !format.is_empty());
    let preferred_codecs = parse_string_list_field(&options, "preferred_codecs")?;
    let bitrate_bps = options.get::<Option<u32>>("bitrate_bps")?;
    let sample_rate_hz = options.get::<Option<u32>>("sample_rate_hz")?;
    let channels = options.get::<Option<u32>>("channels")?;
    let prefer_vbr = options.get::<Option<bool>>("prefer_vbr")?;
    let start_offset_ms = options.get::<Option<u64>>("start_offset_ms")?;

    Ok(TrackServeOptions {
        format,
        preferred_codecs,
        bitrate_bps,
        sample_rate_hz,
        channels,
        prefer_vbr,
        start_offset_ms,
    })
}

pub(super) fn parse_hls_serve_options(options: Option<Table>) -> mlua::Result<HlsServeOptions> {
    let Some(options) = options else {
        return Ok(HlsServeOptions::default());
    };

    let preferred_codecs = parse_string_list_field(&options, "preferred_codecs")?;
    let bitrate_bps = options.get::<Option<u32>>("bitrate_bps")?;
    let sample_rate_hz = options.get::<Option<u32>>("sample_rate_hz")?;
    let channels = options.get::<Option<u32>>("channels")?;
    let prefer_vbr = options.get::<Option<bool>>("prefer_vbr")?;
    let start_offset_ms = options.get::<Option<u64>>("start_offset_ms")?;

    Ok(HlsServeOptions {
        preferred_codecs,
        bitrate_bps,
        sample_rate_hz,
        channels,
        prefer_vbr,
        start_offset_ms,
    })
}

fn merge_response_headers(
    lua: &Lua,
    defaults: &[(&str, &str)],
    custom: Option<Table>,
) -> mlua::Result<Table> {
    let headers = lua.create_table()?;
    for (name, value) in defaults {
        headers.set(*name, *value)?;
    }

    if let Some(custom) = custom {
        for pair in custom.pairs::<String, String>() {
            let (name, value) = pair?;
            headers.set(name, value)?;
        }
    }

    Ok(headers)
}

fn build_kind_response_table(lua: &Lua, kind: &str) -> mlua::Result<Table> {
    let response = lua.create_table()?;
    response.set("kind", kind)?;
    Ok(response)
}

pub(super) fn image_transform_options_to_lua(
    lua: &Lua,
    options: &ParsedImageTransformOptions,
) -> mlua::Result<Table> {
    let table = lua.create_table()?;
    if let Some(format) = options.format {
        let value = match format {
            ImageFormat::Jpeg => "jpg",
            ImageFormat::Png => "png",
            ImageFormat::WebP => "webp",
            _ => unreachable!("unsupported image format should not be stored in response helper"),
        };
        table.set("format", value)?;
    }
    if let Some(quality) = options.quality {
        table.set("quality", quality)?;
    }
    if let Some(max_width) = options.max_width {
        table.set("max_width", max_width)?;
    }
    if let Some(max_height) = options.max_height {
        table.set("max_height", max_height)?;
    }
    Ok(table)
}

pub(super) fn track_serve_options_to_lua(
    lua: &Lua,
    options: &TrackServeOptions,
) -> mlua::Result<Value> {
    lua.to_value_with(options, LUA_SERIALIZE_OPTIONS)
}

pub(super) fn hls_serve_options_to_lua(
    lua: &Lua,
    options: &HlsServeOptions,
) -> mlua::Result<Value> {
    lua.to_value_with(options, LUA_SERIALIZE_OPTIONS)
}

pub(super) fn response_json(
    lua: &Lua,
    (status, body, headers): (u16, Value, Option<Table>),
) -> mlua::Result<Table> {
    let headers = merge_response_headers(lua, &[("content-type", "application/json")], headers)?;
    let response = build_kind_response_table(lua, "json")?;
    response.set("status", status)?;
    response.set("body", body)?;
    response.set("headers", headers)?;
    Ok(response)
}

pub(super) fn response_empty(
    lua: &Lua,
    (status, headers): (Option<u16>, Option<Table>),
) -> mlua::Result<Table> {
    let response = build_kind_response_table(lua, "empty")?;
    response.set("status", status.unwrap_or(StatusCode::NO_CONTENT.as_u16()))?;
    if let Some(headers) = headers {
        response.set("headers", headers)?;
    }
    Ok(response)
}

pub(super) fn response_text(
    lua: &Lua,
    (status, body, headers): (u16, String, Option<Table>),
) -> mlua::Result<Table> {
    let headers = merge_response_headers(
        lua,
        &[("content-type", "text/plain; charset=utf-8")],
        headers,
    )?;
    let response = build_kind_response_table(lua, "text")?;
    response.set("status", status)?;
    response.set("body", body)?;
    response.set("headers", headers)?;
    Ok(response)
}

pub(super) fn response_bytes(
    lua: &Lua,
    (status, body, headers): (u16, LuaBinaryInput, Option<Table>),
) -> mlua::Result<Table> {
    let headers = merge_response_headers(
        lua,
        &[("content-type", "application/octet-stream")],
        headers,
    )?;
    let response = build_kind_response_table(lua, "bytes")?;
    response.set("status", status)?;
    response.set("body", body.into_lua_value())?;
    response.set("headers", headers)?;
    Ok(response)
}

pub(super) fn response_redirect(
    lua: &Lua,
    (status, location, headers): (u16, String, Option<Table>),
) -> mlua::Result<Table> {
    if !(300..400).contains(&status) {
        return Err(mlua::Error::runtime(
            "redirect status must be a 3xx HTTP status code",
        ));
    }

    let location = location.trim();
    if location.is_empty() {
        return Err(mlua::Error::runtime("redirect location must not be empty"));
    }

    let response = build_kind_response_table(lua, "redirect")?;
    response.set("status", status)?;
    let headers = merge_response_headers(lua, &[], headers)?;
    headers.set("location", location)?;
    response.set("headers", headers)?;
    Ok(response)
}

pub(super) fn response_file(
    lua: &Lua,
    (status, path, headers, transform_options): (Option<u16>, String, Option<Table>, Option<Table>),
) -> mlua::Result<Table> {
    let status = status.unwrap_or(StatusCode::OK.as_u16());
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(mlua::Error::runtime("file path must not be empty"));
    }

    let transform_options = parse_image_transform_options(transform_options)?;
    let response = build_kind_response_table(lua, "file")?;
    response.set("status", status)?;
    response.set("path", trimmed)?;
    if let Some(headers) = headers {
        response.set("headers", headers)?;
    }
    if let Some(transform_options) = transform_options {
        response.set(
            "transform",
            image_transform_options_to_lua(lua, &transform_options)?,
        )?;
    }
    Ok(response)
}

pub(super) fn response_stream_track(
    lua: &Lua,
    (track_id, options): (i64, Option<Table>),
) -> mlua::Result<Table> {
    if track_id <= 0 {
        return Err(mlua::Error::runtime("track_id must be a positive id"));
    }

    let options = parse_track_serve_options(options)?;
    let response = build_kind_response_table(lua, "stream_track")?;
    response.set("track_id", track_id)?;
    if options.format.is_some()
        || options.preferred_codecs.is_some()
        || options.bitrate_bps.is_some()
        || options.sample_rate_hz.is_some()
        || options.channels.is_some()
        || options.start_offset_ms.is_some()
    {
        response.set("options", track_serve_options_to_lua(lua, &options)?)?;
    }
    Ok(response)
}

pub(super) fn response_hls_playlist(
    lua: &Lua,
    (track_id, options): (i64, Option<Table>),
) -> mlua::Result<Table> {
    if track_id <= 0 {
        return Err(mlua::Error::runtime("track_id must be a positive id"));
    }

    let options = parse_hls_serve_options(options)?;
    let response = build_kind_response_table(lua, "hls_playlist")?;
    response.set("track_id", track_id)?;
    if options.preferred_codecs.is_some()
        || options.bitrate_bps.is_some()
        || options.sample_rate_hz.is_some()
        || options.channels.is_some()
        || options.start_offset_ms.is_some()
    {
        response.set("options", hls_serve_options_to_lua(lua, &options)?)?;
    }
    Ok(response)
}

pub(super) fn response_download_track(
    lua: &Lua,
    (track_id, options): (i64, Option<Table>),
) -> mlua::Result<Table> {
    if track_id <= 0 {
        return Err(mlua::Error::runtime("track_id must be a positive id"));
    }

    let options = parse_track_serve_options(options)?;
    let response = build_kind_response_table(lua, "download_track")?;
    response.set("track_id", track_id)?;
    if options.format.is_some()
        || options.preferred_codecs.is_some()
        || options.bitrate_bps.is_some()
        || options.sample_rate_hz.is_some()
        || options.channels.is_some()
        || options.start_offset_ms.is_some()
    {
        response.set("options", track_serve_options_to_lua(lua, &options)?)?;
    }
    Ok(response)
}
