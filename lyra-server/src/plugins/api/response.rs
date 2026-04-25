// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use axum::http::StatusCode;
use image::ImageFormat;
use mlua::{
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

#[derive(Default, Serialize)]
#[harmony_macros::interface]
pub(super) struct TrackServeOptions {
    pub(super) format: Option<String>,
    pub(super) codec: Option<String>,
    pub(super) bitrate_bps: Option<u32>,
    pub(super) sample_rate_hz: Option<u32>,
    pub(super) channels: Option<u32>,
}

pub(super) fn parse_track_serve_options(options: Option<Table>) -> mlua::Result<TrackServeOptions> {
    let Some(options) = options else {
        return Ok(TrackServeOptions::default());
    };

    let format = options
        .get::<Option<String>>("format")?
        .map(|format| format.trim().to_string())
        .filter(|format| !format.is_empty());
    let codec = options
        .get::<Option<String>>("codec")?
        .map(|codec| codec.trim().to_string())
        .filter(|codec| !codec.is_empty());
    let bitrate_bps = options.get::<Option<u32>>("bitrate_bps")?;
    let sample_rate_hz = options.get::<Option<u32>>("sample_rate_hz")?;
    let channels = options.get::<Option<u32>>("channels")?;

    Ok(TrackServeOptions {
        format,
        codec,
        bitrate_bps,
        sample_rate_hz,
        channels,
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
    if options.format.is_some() || options.codec.is_some() {
        response.set("options", track_serve_options_to_lua(lua, &options)?)?;
    }
    Ok(response)
}

pub(super) fn response_hls_playlist(
    lua: &Lua,
    (track_id, codec): (i64, Option<String>),
) -> mlua::Result<Table> {
    if track_id <= 0 {
        return Err(mlua::Error::runtime("track_id must be a positive id"));
    }

    let response = build_kind_response_table(lua, "hls_playlist")?;
    response.set("track_id", track_id)?;
    let codec = codec
        .map(|c| c.trim().to_string())
        .filter(|c| !c.is_empty());
    if let Some(codec) = codec {
        response.set("codec", codec)?;
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
    if options.format.is_some() || options.codec.is_some() {
        response.set("options", track_serve_options_to_lua(lua, &options)?)?;
    }
    Ok(response)
}
