// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashMap,
    sync::Arc,
};

use anyhow::Result;
use harmony_luau as luau;

use super::messages::{
    ApiHandlerRequest,
    ApiHandlerResponse,
    ApiResponseBody,
    MixHandlerRequest,
    MixHandlerResult,
    WebSocketState,
};

pub(super) fn mix_context_value(request: &MixHandlerRequest) -> Result<luau::Value> {
    let mut table = luau::OwnedTable::with_capacity(0, 5);
    table.set_field("seed_id", luau::Value::Integer(request.seed_id));
    if let Some(limit) = request.limit {
        table.set_field("limit", luau::Value::Integer(limit as i64));
    }
    if let Some(user_id) = request.user_id {
        table.set_field("user_id", luau::Value::Integer(user_id));
    }
    if !request.recent_track_ids.is_empty() {
        let mut recent = luau::OwnedTable::with_capacity(request.recent_track_ids.len(), 0);
        for track_id in &request.recent_track_ids {
            recent.push_array(luau::Value::Integer(*track_id));
        }
        table.set_field("recent_track_ids", luau::Value::TableData(recent));
    }
    if !request.options.is_empty() {
        table.set_field(
            "options",
            harmony_json::json_to_luau_owned(
                serde_json::Value::Object(request.options.clone()),
                0,
            )?,
        );
    }
    Ok(luau::Value::TableData(table))
}

pub(super) fn api_context_value(request: &ApiHandlerRequest) -> Result<luau::Value> {
    let mut ctx = luau::OwnedTable::with_capacity(0, 4);
    ctx.set_field(
        "plugin_id",
        luau::Value::String(request.plugin_id.as_bytes().to_vec()),
    );
    if let Some(auth) = request.auth.clone() {
        ctx.set_field(
            "auth",
            harmony_json::json_to_luau_owned(
                serde_json::to_value(crate::plugins::auth::to_plugin_auth(auth))?,
                0,
            )?,
        );
    } else {
        ctx.set_field("auth", luau::Value::Nil);
    }

    let mut req = luau::OwnedTable::with_capacity(0, 6);
    req.set_field(
        "method",
        luau::Value::String(request.method.as_bytes().to_vec()),
    );
    req.set_field(
        "path",
        luau::Value::String(request.path.as_bytes().to_vec()),
    );
    req.set_field(
        "headers",
        luau::Value::TableData(headers_table(&request.headers)),
    );
    req.set_field("query", luau::Value::TableData(query_table(&request.query)));
    req.set_field("body_raw", luau::Value::String(request.body.clone()));
    if let Some(json) = parse_json_body(&request.headers, &request.body) {
        req.set_field("json", harmony_json::json_to_luau_owned(json, 0)?);
    } else {
        req.set_field("json", luau::Value::Nil);
    }
    ctx.set_field("request", luau::Value::TableData(req));

    let mut params = luau::OwnedTable::with_capacity(0, request.params.len());
    for (name, value) in &request.params {
        params.set_field(name.clone(), luau::Value::String(value.as_bytes().to_vec()));
    }
    ctx.set_field("params", luau::Value::TableData(params));

    Ok(luau::Value::TableData(ctx))
}

pub(super) fn websocket_reader_value(
    context: &harmony_core::CallContext,
    inbound: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<String>>>,
    state: Arc<WebSocketState>,
) -> luau::Value {
    let mut table = luau::OwnedTable::with_capacity(0, 2);
    let recv_inbound = inbound;
    table.set_field(
        "recv",
        luau::Value::NativeFunction(luau::NativeFunctionValue::new(
            websocket_function_options(context, "recv", ["self"]),
            harmony_core::async_luau_callback(Arc::new(move |mut frame| {
                let _self_value: luau::Value = frame.args.read_named("self")?;
                let inbound = recv_inbound.clone();
                Ok(Box::pin(async move {
                    let mut rx = inbound.lock().await;
                    match rx.recv().await {
                        Some(text) => Ok(vec![luau::Value::String(text.into_bytes())]),
                        None => Ok(vec![luau::Value::Nil]),
                    }
                }))
            })),
        )),
    );
    let close_state = state;
    table.set_field(
        "close",
        luau::Value::NativeFunction(luau::NativeFunctionValue::new(
            websocket_function_options(context, "close", ["self"]),
            Arc::new(move |mut frame| {
                let _self_value: luau::Value = frame.args.read_named("self")?;
                close_state.request_close();
                Ok(())
            }),
        )),
    );
    luau::Value::TableData(table)
}

pub(super) fn websocket_sender_value(
    context: &harmony_core::CallContext,
    outbound: tokio::sync::mpsc::Sender<String>,
    state: Arc<WebSocketState>,
) -> luau::Value {
    let mut table = luau::OwnedTable::with_capacity(0, 3);
    let send_outbound = outbound;
    let send_state = state.clone();
    table.set_field(
        "send",
        luau::Value::NativeFunction(luau::NativeFunctionValue::new(
            websocket_function_options(context, "send", ["self", "text"]),
            harmony_core::async_luau_callback(Arc::new(move |mut frame| {
                let _self_value: luau::Value = frame.args.read_named("self")?;
                let text: String = frame.args.read_named("text")?;
                if send_state.is_closed() {
                    return Err(luau::Error::Runtime("websocket is closed".to_string()));
                }
                let outbound = send_outbound.clone();
                Ok(Box::pin(async move {
                    outbound
                        .send(text)
                        .await
                        .map_err(|_| luau::Error::Runtime("websocket is closed".to_string()))?;
                    Ok(Vec::new())
                }))
            })),
        )),
    );
    let is_closed_state = state.clone();
    table.set_field(
        "is_closed",
        luau::Value::NativeFunction(luau::NativeFunctionValue::new(
            websocket_function_options(context, "is_closed", ["self"]),
            Arc::new(move |mut frame| {
                let _self_value: luau::Value = frame.args.read_named("self")?;
                frame.returns.write(is_closed_state.is_closed())
            }),
        )),
    );
    let close_state = state;
    table.set_field(
        "close",
        luau::Value::NativeFunction(luau::NativeFunctionValue::new(
            websocket_function_options(context, "close", ["self"]),
            Arc::new(move |mut frame| {
                let _self_value: luau::Value = frame.args.read_named("self")?;
                close_state.request_close();
                Ok(())
            }),
        )),
    );
    luau::Value::TableData(table)
}

fn websocket_function_options<const N: usize>(
    context: &harmony_core::CallContext,
    name: &'static str,
    args: [&'static str; N],
) -> luau::NativeFunctionOptions {
    luau::NativeFunctionOptions::new(super::luau_origin(&context.origin))
        .function_name(name)
        .argument_names(args.into_iter().map(Arc::<str>::from))
}

fn headers_table(headers: &[(String, String)]) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, headers.len());
    for (name, value) in headers {
        table.set_field(name.clone(), luau::Value::String(value.as_bytes().to_vec()));
    }
    table
}

fn query_table(query: &HashMap<String, Vec<String>>) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, query.len());
    for (name, values) in query {
        let mut array = luau::OwnedTable::with_capacity(values.len(), 0);
        for value in values {
            array.push_array(luau::Value::String(value.as_bytes().to_vec()));
        }
        table.set_field(name.clone(), luau::Value::TableData(array));
    }
    table
}

pub(super) fn parse_json_body(
    headers: &[(String, String)],
    body: &[u8],
) -> Option<serde_json::Value> {
    if body.is_empty() || !is_json_content_type(headers) {
        return None;
    }
    let text = std::str::from_utf8(body).ok()?;
    serde_json::from_str::<serde_json::Value>(text).ok()
}

fn is_json_content_type(headers: &[(String, String)]) -> bool {
    headers.iter().any(|(name, value)| {
        name.eq_ignore_ascii_case("content-type") && {
            let mime = value.to_ascii_lowercase();
            mime.starts_with("application/json") || mime.contains("+json")
        }
    })
}

pub(super) fn parse_api_response(
    vm: &luau::Vm,
    request: &ApiHandlerRequest,
    values: Vec<luau::Value>,
) -> Result<ApiHandlerResponse> {
    let value = values.into_iter().next().unwrap_or(luau::Value::Nil);
    let luau::Value::Table(table) = value else {
        anyhow::bail!(
            "raw API handler {} returned {}, expected response table",
            request.handler_id,
            value.type_name()
        );
    };
    let status = match table.get_raw(vm, "status")? {
        luau::Value::Integer(status) => status,
        luau::Value::Number(status) => status as i64,
        luau::Value::Nil => 200,
        other => anyhow::bail!(
            "response status must be a number, got {}",
            other.type_name()
        ),
    };
    let status = u16::try_from(status)
        .ok()
        .and_then(|status| {
            axum::http::StatusCode::from_u16(status)
                .ok()
                .map(|_| status)
        })
        .ok_or_else(|| anyhow::anyhow!("invalid response status code: {status}"))?;
    let kind = match table.get_raw(vm, "kind")? {
        luau::Value::String(kind) => String::from_utf8(kind)?,
        luau::Value::Nil => anyhow::bail!("response table must include kind"),
        other => anyhow::bail!("response kind must be a string, got {}", other.type_name()),
    };
    let headers = response_headers(vm, &table)?;
    let body_value = table.get_raw(vm, "body").unwrap_or(luau::Value::Nil);
    let path = optional_string(vm, &table, "path")?;
    let transform = optional_json(vm, &table, "transform")?;
    let options = optional_json(vm, &table, "options")?;
    let track_id = optional_i64(vm, &table, "track_id")?;

    let body = match kind.as_str() {
        "json" => Some(ApiResponseBody::Json(
            if matches!(body_value, luau::Value::Nil) {
                serde_json::Value::Null
            } else {
                harmony_json::luau_to_json(vm, &body_value, 0)?
            },
        )),
        "empty" => None,
        "text" => match body_value {
            luau::Value::String(body) => Some(ApiResponseBody::Bytes(body)),
            other => anyhow::bail!(
                "text responses require a string body, got {}",
                other.type_name()
            ),
        },
        "bytes" => match body_value {
            luau::Value::String(body) | luau::Value::Buffer(body) => {
                Some(ApiResponseBody::Bytes(body))
            }
            other => anyhow::bail!(
                "bytes responses require a string or buffer body, got {}",
                other.type_name()
            ),
        },
        "redirect" | "file" | "stream_track" | "download_track" | "hls_playlist" => None,
        other => anyhow::bail!("unsupported response kind: {other}"),
    };

    Ok(ApiHandlerResponse {
        kind,
        status,
        headers,
        body,
        path,
        transform,
        track_id,
        options,
    })
}

fn response_headers(vm: &luau::Vm, table: &luau::Table) -> Result<Vec<(String, String)>> {
    let headers = match table.get_raw(vm, "headers")? {
        luau::Value::Table(headers) => headers,
        luau::Value::Nil => return Ok(Vec::new()),
        other => anyhow::bail!(
            "response headers must be a table, got {}",
            other.type_name()
        ),
    };
    let mut result = Vec::new();
    for (key, value) in headers.pairs_raw(vm)? {
        let (luau::Value::String(key), luau::Value::String(value)) = (key, value) else {
            continue;
        };
        result.push((String::from_utf8(key)?, String::from_utf8(value)?));
    }
    Ok(result)
}

fn optional_string(vm: &luau::Vm, table: &luau::Table, key: &str) -> Result<Option<String>> {
    match table.get_raw(vm, key)? {
        luau::Value::String(value) => Ok(Some(String::from_utf8(value)?)),
        luau::Value::Nil => Ok(None),
        other => anyhow::bail!("{key} must be a string, got {}", other.type_name()),
    }
}

fn optional_i64(vm: &luau::Vm, table: &luau::Table, key: &str) -> Result<Option<i64>> {
    match table.get_raw(vm, key)? {
        luau::Value::Integer(value) => Ok(Some(value)),
        luau::Value::Number(value) => Ok(Some(value as i64)),
        luau::Value::Nil => Ok(None),
        other => anyhow::bail!("{key} must be a number, got {}", other.type_name()),
    }
}

fn optional_json(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> Result<Option<serde_json::Value>> {
    let value = table.get_raw(vm, key)?;
    if matches!(value, luau::Value::Nil) {
        Ok(None)
    } else {
        Ok(Some(harmony_json::luau_to_json(vm, &value, 0)?))
    }
}

pub(super) fn parse_mix_result(
    vm: &luau::Vm,
    mixer_id: &str,
    values: Vec<luau::Value>,
) -> Result<MixHandlerResult> {
    let Some(value) = values.into_iter().next() else {
        return Ok(MixHandlerResult {
            track_ids: Vec::new(),
        });
    };
    let luau::Value::Table(result) = value else {
        anyhow::bail!(
            "raw mixer '{mixer_id}' returned {}, expected table",
            value.type_name()
        );
    };
    let tracks = match result.get_raw(vm, "tracks")? {
        luau::Value::Table(table) => table,
        luau::Value::Nil => {
            return Ok(MixHandlerResult {
                track_ids: Vec::new(),
            });
        }
        other => {
            anyhow::bail!(
                "raw mixer '{mixer_id}' returned tracks as {}, expected table",
                other.type_name()
            );
        }
    };

    let mut entries = Vec::new();
    for (key, value) in tracks.pairs_raw(vm)? {
        let index = match key {
            luau::Value::Integer(index) => index,
            luau::Value::Number(index) => index as i64,
            _ => continue,
        };
        let luau::Value::Table(entry) = value else {
            continue;
        };
        let track_id = match entry.get_raw(vm, "track_id")? {
            luau::Value::Integer(track_id) => track_id,
            luau::Value::Number(track_id) => track_id as i64,
            _ => continue,
        };
        entries.push((index, track_id));
        if entries.len() >= crate::services::mix::MAX_LIMIT {
            break;
        }
    }
    entries.sort_by_key(|(index, _)| *index);
    Ok(MixHandlerResult {
        track_ids: entries.into_iter().map(|(_, track_id)| track_id).collect(),
    })
}
