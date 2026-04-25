// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;
use std::path::Path as FsPath;

use anyhow::{
    Context,
    Result,
    anyhow,
    bail,
};
use axum::body::{
    Body,
    Bytes,
};
use axum::http::header::{
    ACCEPT_RANGES,
    CONTENT_LENGTH,
    CONTENT_RANGE,
    CONTENT_TYPE,
    HeaderName,
    HeaderValue,
    RANGE,
};
use axum::http::{
    HeaderMap,
    Method,
    StatusCode,
    Uri,
};
use axum::response::{
    IntoResponse,
    Response,
};
use mlua::{
    Lua,
    LuaSerdeExt,
    Table,
    Value,
};

use crate::{
    plugins::{
        LUA_SERIALIZE_OPTIONS,
        auth::{
            plugin_auth_to_value,
            to_plugin_auth,
        },
        from_lua_json_value,
    },
    routes::{
        build_ranged_file_body,
        download_track_response,
        serve_hls_playlist_for_track,
        stream_track_response,
    },
    services::auth::resolve_optional_auth,
};

use super::image::{
    parse_image_transform_options,
    transform_image,
};
use super::registry::RegisteredRoute;
use super::response::parse_track_serve_options;

pub(super) async fn build_context(
    lua: &Lua,
    route: &RegisteredRoute,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    query: &HashMap<String, String>,
    params: Option<&HashMap<String, String>>,
    body: &Bytes,
) -> Result<Table> {
    let ctx = lua.create_table()?;
    ctx.set("plugin_id", route.plugin_id.as_ref())?;
    if let Some(auth) = resolve_optional_auth(headers)
        .await
        .map_err(|err| anyhow::Error::new(err))?
    {
        ctx.set("auth", plugin_auth_to_value(lua, to_plugin_auth(auth))?)?;
    } else {
        ctx.set("auth", Value::Nil)?;
    }

    let request = lua.create_table()?;
    request.set("method", method.as_str())?;
    request.set("path", uri.path())?;

    let headers_table = lua.create_table()?;
    for (name, value) in headers {
        if let Ok(value) = value.to_str() {
            headers_table.set(name.as_str(), value)?;
        }
    }
    request.set("headers", headers_table)?;

    let query_table = lua.create_table()?;
    for (name, value) in query {
        query_table.set(name.as_str(), value.as_str())?;
    }
    request.set("query", query_table)?;
    request.set("body_raw", lua.create_string(body.as_ref())?)?;

    let parsed_json = parse_json_body(lua, headers, body)?;
    if let Some(value) = parsed_json {
        request.set("json", value)?;
    } else {
        request.set("json", Value::Nil)?;
    }

    ctx.set("request", request)?;

    let params_table = lua.create_table()?;
    if let Some(params) = params {
        for (name, value) in params {
            params_table.set(name.as_str(), value.as_str())?;
        }
    }
    ctx.set("params", params_table)?;

    Ok(ctx)
}

fn parse_json_body(lua: &Lua, headers: &HeaderMap, body: &Bytes) -> Result<Option<Value>> {
    if body.is_empty() || !is_json_content_type(headers) {
        return Ok(None);
    }

    let Ok(text) = std::str::from_utf8(body.as_ref()) else {
        return Ok(None);
    };

    let Ok(json_value) = serde_json::from_str::<serde_json::Value>(text) else {
        return Ok(None);
    };

    Ok(Some(lua.to_value_with(&json_value, LUA_SERIALIZE_OPTIONS)?))
}

fn is_json_content_type(headers: &HeaderMap) -> bool {
    let Some(content_type) = headers.get(CONTENT_TYPE) else {
        return false;
    };
    let Ok(content_type) = content_type.to_str() else {
        return false;
    };
    let mime = content_type.to_ascii_lowercase();
    mime.starts_with("application/json") || mime.contains("+json")
}

fn infer_content_type(path: &str) -> &'static str {
    let extension = FsPath::new(path)
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase());
    match extension.as_deref() {
        Some("aac") => "audio/aac",
        Some("aiff") | Some("aif") => "audio/aiff",
        Some("flac") => "audio/flac",
        Some("m4a") | Some("m4b") | Some("mp4") => "audio/mp4",
        Some("mp3") => "audio/mpeg",
        Some("ogg") => "audio/ogg",
        Some("opus") => "audio/ogg",
        Some("wav") => "audio/wav",
        Some("m3u8") => "application/x-mpegurl",
        _ => "application/octet-stream",
    }
}

pub(super) async fn lua_response_to_axum(
    lua: &Lua,
    value: Value,
    request_headers: &HeaderMap,
) -> Result<Response> {
    let table = match value {
        Value::Table(table) => table,
        _ => bail!("handler must return a response table"),
    };

    let status = table.get::<Option<u16>>("status")?.unwrap_or(200);
    let mut status = StatusCode::from_u16(status).context("invalid response status code")?;
    let response_kind = table
        .get::<Option<String>>("kind")?
        .ok_or_else(|| anyhow!("response table must include kind"))?;

    match response_kind.as_str() {
        "stream_track" => {
            let track_id = table
                .get::<Option<i64>>("track_id")?
                .ok_or_else(|| anyhow!("stream_track response requires track_id"))?;
            let options = parse_track_serve_options(table.get::<Option<Table>>("options")?)?;
            return Ok(
                match stream_track_response(
                    request_headers,
                    agdb::DbId(track_id),
                    options.format,
                    options.codec,
                    options.bitrate_bps,
                    options.sample_rate_hz,
                    options.channels,
                )
                .await
                {
                    Ok(response) => response,
                    Err(error) => error.into_response(),
                },
            );
        }
        "download_track" => {
            let track_id = table
                .get::<Option<i64>>("track_id")?
                .ok_or_else(|| anyhow!("download_track response requires track_id"))?;
            let options = parse_track_serve_options(table.get::<Option<Table>>("options")?)?;
            return Ok(
                match download_track_response(
                    request_headers,
                    agdb::DbId(track_id),
                    options.format,
                    options.codec,
                    options.bitrate_bps,
                    options.sample_rate_hz,
                    options.channels,
                )
                .await
                {
                    Ok(response) => response,
                    Err(error) => error.into_response(),
                },
            );
        }
        "hls_playlist" => {
            let track_id = table
                .get::<Option<i64>>("track_id")?
                .ok_or_else(|| anyhow!("hls_playlist response requires track_id"))?;
            let codec = table.get::<Option<String>>("codec")?;
            return Ok(
                match serve_hls_playlist_for_track(request_headers, agdb::DbId(track_id), codec)
                    .await
                {
                    Ok(response) => response,
                    Err(error) => error.into_response(),
                },
            );
        }
        "json" | "empty" | "text" | "file" => {}
        other => bail!("unsupported response kind: {other}"),
    }

    let body_value = table.get::<Value>("body").unwrap_or(Value::Nil);
    let file_path = table.get::<Option<String>>("path")?;
    if response_kind == "file" && file_path.is_none() {
        bail!("file response requires path");
    }
    if response_kind != "file" && file_path.is_some() {
        bail!("{response_kind} response cannot include path");
    }
    let mut content_type = None::<HeaderValue>;
    let mut content_length = None::<HeaderValue>;
    let mut content_range = None::<HeaderValue>;
    let mut accept_ranges = None::<HeaderValue>;
    let transform_options = table.get::<Option<Table>>("transform")?;

    let body = if let Some(path) = file_path {
        let trimmed = path.trim();
        if trimmed.is_empty() {
            bail!("file path must not be empty");
        }
        if let Some(transform_options) = transform_options {
            let parsed_transform_options = parse_image_transform_options(Some(transform_options))?
                .ok_or_else(|| {
                    anyhow!("transform options must contain at least one image transform field")
                })?;
            let owned_path = trimmed.to_owned();
            let (bytes, format) = tokio::task::spawn_blocking(move || {
                transform_image(&owned_path, &parsed_transform_options)
            })
            .await
            .map_err(|err| anyhow!("failed to join image transform task: {}", err))??;
            content_type = Some(HeaderValue::from_static(format.to_mime_type()));
            Body::from(bytes)
        } else {
            let ranged = build_ranged_file_body(
                FsPath::new(trimmed),
                request_headers.get(RANGE),
                status,
                None,
            )
            .await?;
            status = ranged.status;
            content_type = Some(HeaderValue::from_static(infer_content_type(trimmed)));
            content_length = Some(ranged.content_length);
            content_range = ranged.content_range;
            accept_ranges = Some(HeaderValue::from_static("bytes"));
            ranged.body
        }
    } else {
        match body_value {
            Value::Nil if response_kind == "json" => {
                content_type = Some(HeaderValue::from_static("application/json"));
                Body::from(b"null".to_vec())
            }
            Value::Nil => Body::empty(),
            Value::String(text) => Body::from(text.as_bytes().to_vec()),
            other => {
                let json: serde_json::Value = from_lua_json_value(lua, other)?;
                if response_kind == "text" {
                    bail!("text responses require a string body");
                }
                content_type = Some(HeaderValue::from_static("application/json"));
                Body::from(serde_json::to_vec(&json)?)
            }
        }
    };

    let mut response = Response::builder()
        .status(status)
        .body(body)
        .map_err(|err| anyhow!("failed to build response: {}", err))?;

    if let Ok(headers_table) = table.get::<Table>("headers") {
        for pair in headers_table.pairs::<String, String>() {
            let (name, value) = pair?;
            let name = HeaderName::from_bytes(name.as_bytes())
                .with_context(|| format!("invalid header name '{}'", name))?;
            let value = HeaderValue::from_str(&value)
                .with_context(|| format!("invalid header value for '{}'", name))?;
            response.headers_mut().insert(name, value);
        }
    }

    if let Some(content_type) = content_type {
        response
            .headers_mut()
            .entry(CONTENT_TYPE)
            .or_insert(content_type);
    }
    if let Some(content_length) = content_length {
        response
            .headers_mut()
            .entry(CONTENT_LENGTH)
            .or_insert(content_length);
    }
    if let Some(content_range) = content_range {
        response.headers_mut().insert(CONTENT_RANGE, content_range);
    }
    if let Some(accept_ranges) = accept_ranges {
        response.headers_mut().insert(ACCEPT_RANGES, accept_ranges);
    }

    Ok(response)
}
