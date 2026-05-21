// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use anyhow::Result;
use harmony_core::LocalScheduler;
use harmony_luau as luau;

use super::{
    PluginExecutor,
    messages::{
        ApiHandlerRequest,
        ApiHandlerResponse,
        ApiResponseBody,
        ApiResponseKind,
    },
    runner::drive_luau_thread,
};

impl PluginExecutor {
    pub(crate) fn dispatch_api_handler(
        &self,
        request: ApiHandlerRequest,
    ) -> Result<ApiHandlerResponse> {
        let routes = self.vm.data().get::<crate::plugins::api::ApiRouteStore>()?;
        let handler = routes
            .get(request.handler_id)
            .ok_or_else(|| anyhow::anyhow!("API handler {} not found", request.handler_id))?;
        let ctx = api_context_value(&request)?;
        let thread = self.vm.create_thread(&handler.handler)?;
        let mut context = handler.context.clone();
        if let Some(auth) = request.auth.as_ref() {
            context.caller.insert(auth.principal.clone());
        }
        let scheduler = self.vm.data().get::<LocalScheduler>()?;
        scheduler.spawn_luau_thread(context, self.vm.clone(), thread.clone(), vec![ctx]);
        let values = drive_luau_thread(&self.tokio_runtime, &scheduler, &thread)?;
        parse_api_response(&self.vm, &request, values)
    }
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

fn parse_json_body(headers: &[(String, String)], body: &[u8]) -> Option<serde_json::Value> {
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

fn parse_api_response(
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
    let kind = ApiResponseKind::parse(&kind)
        .ok_or_else(|| anyhow::anyhow!("unsupported response kind: {kind}"))?;
    let headers = harmony_json::optional_string_pairs_field(vm, &table, "headers")?;
    let body_value = table.get_raw(vm, "body").unwrap_or(luau::Value::Nil);
    let path = harmony_json::optional_string_field(vm, &table, "path")?;
    let transform = harmony_json::optional_json_field(vm, &table, "transform")?;
    let options = harmony_json::optional_json_field(vm, &table, "options")?;
    let track_id = harmony_json::optional_i64_field(vm, &table, "track_id")?;

    let body = match kind {
        ApiResponseKind::Json => Some(ApiResponseBody::Json(
            if matches!(body_value, luau::Value::Nil) {
                serde_json::Value::Null
            } else {
                harmony_json::luau_to_json(vm, &body_value, 0)?
            },
        )),
        ApiResponseKind::Empty => None,
        ApiResponseKind::Text => match body_value {
            luau::Value::String(body) => Some(ApiResponseBody::Bytes(body)),
            other => anyhow::bail!(
                "text responses require a string body, got {}",
                other.type_name()
            ),
        },
        ApiResponseKind::Bytes => match body_value {
            luau::Value::String(body) | luau::Value::Buffer(body) => {
                Some(ApiResponseBody::Bytes(body))
            }
            other => anyhow::bail!(
                "bytes responses require a string or buffer body, got {}",
                other.type_name()
            ),
        },
        ApiResponseKind::Redirect
        | ApiResponseKind::File
        | ApiResponseKind::StreamTrack
        | ApiResponseKind::DownloadTrack
        | ApiResponseKind::HlsPlaylist => None,
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
