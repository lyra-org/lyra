// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use anyhow::Result;
use harmony_core::Scheduler;
use harmony_luau as luau;

use super::{
    PluginExecutor,
    messages::{
        ApiFallbackResponse,
        ApiHandlerRequest,
        ApiHandlerResponse,
        ApiResponseBody,
        ApiResponseKind,
    },
};

/// How long an API handler may run.
const API_HANDLER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

impl PluginExecutor {
    pub(super) fn start_api_handler(
        &self,
        request: ApiHandlerRequest,
        reply: tokio::sync::oneshot::Sender<Result<ApiHandlerResponse>>,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) {
        let dispatch_auth = crate::plugins::auth::DispatchAuth::default();
        if let Some(auth) = request.auth.clone() {
            dispatch_auth.record(auth);
        }
        let deadline = self.now() + API_HANDLER_TIMEOUT;
        let prepare = || {
            let routes = self.vm.data().get::<crate::plugins::api::ApiRouteStore>()?;
            let handler = routes.get(request.handler)?;
            let ctx = api_context_value(&request)?;
            let thread = self.vm.create_thread(&handler.handler)?;
            let mut context = handler.context.clone();
            context.caller.insert(dispatch_auth.clone());
            context
                .caller
                .insert(crate::plugins::auth::DispatchClient(request.client));
            context.caller.insert(super::CallChain::root(
                deadline,
                context.origin.plugin.clone(),
            ));
            Ok::<_, anyhow::Error>((thread, context, ctx))
        };
        let (thread, context, argument) = match prepare() {
            Ok(prepared) => prepared,
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };
        self.start_callback(
            deadline,
            super::callbacks::CallbackReply::Api {
                reply,
                request: Box::new(request),
                auth: dispatch_auth,
                context: context.clone(),
            },
            permit,
            context.origin.plugin.clone(),
            |limit| {
                Ok(self
                    .vm
                    .data()
                    .get::<Scheduler>()?
                    .spawn_watched_luau_thread(context, thread, vec![argument], Some(limit)))
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

    let mut req = luau::OwnedTable::with_capacity(0, 7);
    req.set_field(
        "method",
        luau::Value::String(request.method.as_bytes().to_vec()),
    );
    req.set_field(
        "path",
        luau::Value::String(request.path.as_bytes().to_vec()),
    );
    req.set_field("headers", headers_table(&request.headers));
    req.set_field("base_url", request.base_url.clone());
    req.set_field("query", query_table(&request.query));
    req.set_field("body_raw", luau::Value::String(request.body.clone()));
    if let Some(json) = parse_json_body(&request.headers, &request.body) {
        req.set_field("json", harmony_serde::json_to_luau_owned(json, 0)?);
    } else {
        req.set_field("json", luau::Value::Nil);
    }
    ctx.set_field("request", req);

    let mut params = luau::OwnedTable::with_capacity(0, request.params.len());
    for (name, value) in &request.params {
        params.set_field(name.clone(), value.as_str());
    }
    ctx.set_field("params", params);

    Ok(ctx.into())
}

fn headers_table(headers: &[(String, String)]) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, headers.len());
    for (name, value) in headers {
        table.set_field(name.clone(), value.as_str());
    }
    table
}

fn query_table(query: &HashMap<String, Vec<String>>) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, query.len());
    for (name, values) in query {
        let array = luau::OwnedTable::from_array(values.iter().map(String::as_str));
        table.set_field(name.clone(), array);
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

/// A handler's response, with the producer of a streamed one, which writes its body.
pub(super) struct ParsedApiResponse {
    pub(super) response: ApiHandlerResponse,
    pub(super) produce: Option<luau::Function>,
}

pub(super) fn parse_api_response(
    request: &ApiHandlerRequest,
    values: Vec<luau::Value>,
) -> Result<ParsedApiResponse> {
    let value = values.into_iter().next().unwrap_or(luau::Value::Nil);
    let luau::Value::Table(table) = value else {
        anyhow::bail!(
            "raw API handler {} returned {}, expected response table",
            request.handler.handler_id,
            value.type_name()
        );
    };
    let status = response_status(&table)?;
    let kind = response_kind(&table)?;
    let headers = harmony_serde::optional_string_pairs_field(&table, "headers")?;
    let cover = match kind {
        ApiResponseKind::Cover => Some(crate::plugins::api::options::CoverRequest {
            cover_id: table
                .optional_string_field("cover_id")?
                .ok_or_else(|| anyhow::anyhow!("cover responses require cover_id"))?,
            options: crate::plugins::api::options::cover_options(
                &table.get("options")?,
                "cover options",
            )?,
        }),
        _ => None,
    };
    let options = match kind {
        ApiResponseKind::StreamTrack
        | ApiResponseKind::DownloadTrack
        | ApiResponseKind::HlsPlaylist => Some(
            crate::plugins::api::options::track_response_options(kind, &table.get("options")?)?,
        ),
        _ => None,
    };
    let track_id = table.optional_string_field("track_id")?;
    let produce = match (kind, table.get("produce")?) {
        (ApiResponseKind::Stream, luau::Value::Function(produce)) => Some(produce),
        (ApiResponseKind::Stream, other) => anyhow::bail!(
            "stream responses require a produce function, got {}",
            other.type_name()
        ),
        _ => None,
    };

    let body = response_body(kind, &table)?;
    let fallback = match table.get("fallback")? {
        luau::Value::Nil => None,
        luau::Value::Table(fallback) if kind.takes_fallback() => {
            Some(fallback_response(&fallback)?)
        }
        luau::Value::Table(_) => anyhow::bail!("{kind} responses take no fallback"),
        other => anyhow::bail!(
            "response fallback must be a response table, got {}",
            other.type_name()
        ),
    };

    let response = ApiHandlerResponse {
        kind,
        status,
        headers,
        body,
        cover,
        track_id,
        options,
        auth: None,
        client: Default::default(),
        fallback,
    };
    Ok(ParsedApiResponse { response, produce })
}

/// The body-only response a plugin sends in place of Lyra's error.
fn fallback_response(table: &luau::Table) -> Result<ApiFallbackResponse> {
    let kind = response_kind(table)?;
    if !kind.is_body_only() {
        anyhow::bail!(
            "a response fallback must be a json, empty, text or bytes response, got {kind}"
        );
    }
    Ok(ApiFallbackResponse {
        kind,
        status: response_status(table)?,
        headers: harmony_serde::optional_string_pairs_field(table, "headers")?,
        body: response_body(kind, table)?,
    })
}

fn response_status(table: &luau::Table) -> Result<u16> {
    let status = match table.get("status")? {
        luau::Value::Integer(status) => status,
        luau::Value::Number(status) => status as i64,
        luau::Value::Nil => 200,
        other => anyhow::bail!(
            "response status must be a number, got {}",
            other.type_name()
        ),
    };
    u16::try_from(status)
        .ok()
        .and_then(|status| {
            axum::http::StatusCode::from_u16(status)
                .ok()
                .map(|_| status)
        })
        .ok_or_else(|| anyhow::anyhow!("invalid response status code: {status}"))
}

fn response_kind(table: &luau::Table) -> Result<ApiResponseKind> {
    let kind = match table.get("kind")? {
        luau::Value::String(kind) => String::from_utf8(kind)?,
        luau::Value::Nil => anyhow::bail!("response table must include kind"),
        other => anyhow::bail!("response kind must be a string, got {}", other.type_name()),
    };
    ApiResponseKind::parse(&kind)
        .ok_or_else(|| anyhow::anyhow!("unsupported response kind: {kind}"))
}

/// The body of a response of `kind`, for the kinds whose body the table carries.
fn response_body(kind: ApiResponseKind, table: &luau::Table) -> Result<Option<ApiResponseBody>> {
    let body_value = table.get("body").unwrap_or(luau::Value::Nil);
    Ok(match kind {
        ApiResponseKind::Json => Some(ApiResponseBody::Json(
            if matches!(body_value, luau::Value::Nil) {
                serde_json::Value::Null
            } else {
                harmony_serde::luau_to_json(&body_value)?
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
        ApiResponseKind::Filler => Some(ApiResponseBody::Filler(crate::plugins::api::filler_size(
            &table.get("size_bytes")?,
        )?)),
        ApiResponseKind::Redirect
        | ApiResponseKind::Cover
        | ApiResponseKind::StreamTrack
        | ApiResponseKind::DownloadTrack
        | ApiResponseKind::HlsPlaylist
        | ApiResponseKind::Stream => None,
    })
}
