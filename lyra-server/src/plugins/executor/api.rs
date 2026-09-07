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
};

impl PluginExecutor {
    pub(super) fn start_api_handler(
        &self,
        request: ApiHandlerRequest,
        reply: tokio::sync::oneshot::Sender<Result<ApiHandlerResponse>>,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) {
        let dispatch_auth = crate::plugins::auth::DispatchAuth::default();
        if let Some(auth) = request.auth.as_ref() {
            dispatch_auth.record(auth.principal.clone());
        }
        let prepare = || {
            let routes = self.vm.data().get::<crate::plugins::api::ApiRouteStore>()?;
            let handler = routes
                .get(request.handler_id)
                .ok_or_else(|| anyhow::anyhow!("API handler {} not found", request.handler_id))?;
            let ctx = api_context_value(&request)?;
            let thread = self.vm.create_thread(&handler.handler)?;
            let mut context = handler.context.clone();
            context.caller.insert(dispatch_auth.clone());
            context.caller.insert(crate::plugins::auth::DispatchClient(
                request.client_key.clone(),
            ));
            Ok::<_, anyhow::Error>((thread, context, ctx))
        };
        let prepared = prepare();
        self.start_callback(
            std::time::Instant::now() + std::time::Duration::from_secs(300),
            super::callbacks::CallbackReply::Api {
                reply,
                request: Box::new(request),
                auth: dispatch_auth,
            },
            permit,
            |timeout, completion| {
                let (thread, context, argument) = prepared?;
                let scheduler = self.vm.data().get::<LocalScheduler>()?;
                scheduler.schedule_luau_thread_with_budget_and_completion(
                    context,
                    self.vm.clone(),
                    thread.clone(),
                    vec![argument],
                    timeout,
                    completion,
                );
                Ok((scheduler, thread))
            },
        );
    }

    #[cfg(test)]
    pub(crate) fn dispatch_api_handler(
        &self,
        request: ApiHandlerRequest,
    ) -> Result<ApiHandlerResponse> {
        self.drive_callback(|reply, permit| self.start_api_handler(request, reply, permit))
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
            harmony_luau::serializable_to_luau_owned(crate::plugins::auth::to_plugin_auth(auth))?,
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
        req.set_field("json", harmony_serde::json_to_luau_owned(json, 0)?);
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
    let kind = ApiResponseKind::parse(&kind)
        .ok_or_else(|| anyhow::anyhow!("unsupported response kind: {kind}"))?;
    let headers = harmony_serde::optional_string_pairs_field(vm, &table, "headers")?;
    let body_value = table.get_raw(vm, "body").unwrap_or(luau::Value::Nil);
    let path = harmony_serde::optional_string_field(vm, &table, "path")?;
    let transform = harmony_serde::optional_json_field(vm, &table, "transform")?;
    let options = harmony_serde::optional_json_field(vm, &table, "options")?;
    let track_id = harmony_serde::optional_i64_field(vm, &table, "track_id")?;

    let body = match kind {
        ApiResponseKind::Json => Some(ApiResponseBody::Json(
            if matches!(body_value, luau::Value::Nil) {
                serde_json::Value::Null
            } else {
                harmony_serde::luau_to_json(vm, &body_value, 0)?
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
        principal: None,
    })
}
