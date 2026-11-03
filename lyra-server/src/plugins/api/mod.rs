// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub(crate) mod options;
mod registry;
mod route_key;
mod route_options;
mod types;

use self::registry::{
    ApiRouteRegistration,
    api_route_registry,
    freeze_registry,
};
pub(crate) use self::registry::{
    ApiRouteRegistries,
    ApiRouteStore,
    CaseInsensitiveRouter,
    RegisteredRoute,
    RouteAuthMode,
    RouteHost,
    RouteKind,
    initialize_registry,
    registered_routes,
};
use self::route_key::lowercase_literal_segments;
pub(crate) use self::route_key::{
    RouteKey,
    placeholder_offset,
};
use self::route_options::{
    MAX_BODY_BYTES,
    RouteOptions,
    WebSocketRouteOptions,
};
use self::types::ApiMethod;
/// The Luau type of the websocket sender `api.websocket` handlers get.
#[cfg(feature = "docgen")]
pub(crate) use self::types::ApiWebSocketSender as WebSocketSenderType;
#[cfg(feature = "docgen")]
use self::types::{
    ApiAuth,
    ApiContext,
    ApiHandler,
    ApiHeaders,
    ApiPathParams,
    ApiQueryParams,
    ApiRequest,
    ApiResponse,
    ApiRouteOptions,
    ApiStreamProducer,
    ApiWebSocketHandler,
    ApiWebSocketOptions,
    ApiWebSocketReader,
    ApiWebSocketSender,
    HlsServeOptions,
    ImageTransformOptions,
    TrackServeOptions,
    TrackStreamOptions,
};
use crate::plugins::executor::ApiResponseKind;

use std::{
    collections::{
        HashMap,
        HashSet,
    },
    sync::Arc,
};

use anyhow::{
    Context,
    Result,
    bail,
};
use axum::http::StatusCode;
use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::render_definition_file_with_support;
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeInterface,
    DescribeModule,
    DescribeTypeAlias,
    FieldDescriptor,
    FunctionParameter,
    JsonValue,
    LuauType,
    LuauTypeInfo,
    TypeAliasDescriptor,
};

#[cfg(feature = "docgen")]
use crate::plugins::auth::{
    AuthCredential,
    Principal as AuthPrincipal,
};
pub(crate) async fn finalize() -> anyhow::Result<()> {
    freeze_registry().await;
    rebuild_registered_routes().await
}

/// Rebuilds the installed HTTP routes from the registry; a no-op until a
/// host installs them.
pub(crate) async fn rebuild_registered_routes() -> anyhow::Result<()> {
    let host = api_route_registry().read_owned().await.host();
    match host {
        Some(host) => host.rebuild().await,
        None => Ok(()),
    }
}

pub(crate) async fn unfreeze_plugin_routes(plugin_id: crate::services::plugin_scope::PluginId) {
    api_route_registry()
        .write_owned()
        .await
        .unfreeze_plugin(plugin_id);
}

pub(crate) async fn refreeze_plugin_routes(plugin_id: &crate::services::plugin_scope::PluginId) {
    api_route_registry()
        .write_owned()
        .await
        .refreeze_plugin(plugin_id);
}

pub(crate) async fn teardown_plugin_routes(plugin_id: &crate::services::plugin_scope::PluginId) {
    api_route_registry()
        .write_owned()
        .await
        .clear_bucket(plugin_id);
    rebuild_registered_routes()
        .await
        .unwrap_or_else(|err| panic!("router rebuild must not fail post-teardown: {err}"));
}

/// An HTTP status code argument.
struct Status(u16);

impl<'vm> luau::FromLuau<'vm> for Status {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let status: i64 = reader.read()?;
        validate_status(status).map(Self)
    }
}

/// The documented type of a `response.bytes` body.
#[cfg(feature = "docgen")]
struct BytesBody;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for BytesBody {
    fn luau_type() -> LuauType {
        LuauType::union(vec![String::luau_type(), LuauType::literal("buffer")])
    }
}

pub(crate) struct ApiModule;

/// The largest `response.filler` body, in bytes.
pub(crate) const MAX_FILLER_BYTES: usize = 100_000_000;

crate::plugins::limits::module_limits!(&[
    crate::plugins::limits::Limit {
        name: "MAX_BODY_BYTES",
        value: MAX_BODY_BYTES,
        description: "The largest `max_body_bytes` a route registers with.",
    },
    crate::plugins::limits::Limit {
        name: "MAX_FILLER_BYTES",
        value: MAX_FILLER_BYTES,
        description: "The largest `size_bytes` of a `response.filler`.",
    },
    crate::plugins::limits::Limit {
        name: "MAX_STREAM_CHUNK_BYTES",
        value: crate::plugins::executor::MAX_STREAM_CHUNK_BYTES,
        description: "The largest chunk an `ApiStreamWriter` writes at once.",
    },
]);

#[harmony_macros::module(
    id = "lyra/api",
    docs_feature = "docgen",
    extend = install_limits,
    describe = describe_limits,
    userdata(
        crate::plugins::executor::ApiWebSocketReader,
        crate::plugins::executor::ApiWebSocketSender,
        crate::plugins::executor::ApiStreamWriter
    )
)]
impl ApiModule {
    #[harmony(
        description = "Registers a plugin API route.",
        args(method: ApiMethod, path: String, handler: ApiHandler, options: Option<ApiRouteOptions>)
    )]
    fn route(
        method: ApiMethod,
        path: String,
        handler: luau::Function,
        options: Option<RouteOptions>,
        context: &luau::CallContext,
        vm: &luau::Vm,
    ) -> luau::runtime::Result<()> {
        register_route(
            vm,
            context,
            method,
            path,
            handler,
            options.unwrap_or_default(),
        )
    }

    #[harmony(
        description = "Registers a GET API route.",
        args(path: String, handler: ApiHandler, options: Option<ApiRouteOptions>)
    )]
    fn get(
        path: String,
        handler: luau::Function,
        options: Option<RouteOptions>,
        context: &luau::CallContext,
        vm: &luau::Vm,
    ) -> luau::runtime::Result<()> {
        register_route(
            vm,
            context,
            ApiMethod::Get,
            path,
            handler,
            options.unwrap_or_default(),
        )
    }

    #[harmony(
        description = "Registers a POST API route.",
        args(path: String, handler: ApiHandler, options: Option<ApiRouteOptions>)
    )]
    fn post(
        path: String,
        handler: luau::Function,
        options: Option<RouteOptions>,
        context: &luau::CallContext,
        vm: &luau::Vm,
    ) -> luau::runtime::Result<()> {
        register_route(
            vm,
            context,
            ApiMethod::Post,
            path,
            handler,
            options.unwrap_or_default(),
        )
    }

    #[harmony(
        description = "Registers a PUT API route.",
        args(path: String, handler: ApiHandler, options: Option<ApiRouteOptions>)
    )]
    fn put(
        path: String,
        handler: luau::Function,
        options: Option<RouteOptions>,
        context: &luau::CallContext,
        vm: &luau::Vm,
    ) -> luau::runtime::Result<()> {
        register_route(
            vm,
            context,
            ApiMethod::Put,
            path,
            handler,
            options.unwrap_or_default(),
        )
    }

    #[harmony(
        description = "Registers a PATCH API route.",
        args(path: String, handler: ApiHandler, options: Option<ApiRouteOptions>)
    )]
    fn patch(
        path: String,
        handler: luau::Function,
        options: Option<RouteOptions>,
        context: &luau::CallContext,
        vm: &luau::Vm,
    ) -> luau::runtime::Result<()> {
        register_route(
            vm,
            context,
            ApiMethod::Patch,
            path,
            handler,
            options.unwrap_or_default(),
        )
    }

    #[harmony(
        description = "Registers a DELETE API route.",
        args(path: String, handler: ApiHandler, options: Option<ApiRouteOptions>)
    )]
    fn delete(
        path: String,
        handler: luau::Function,
        options: Option<RouteOptions>,
        context: &luau::CallContext,
        vm: &luau::Vm,
    ) -> luau::runtime::Result<()> {
        register_route(
            vm,
            context,
            ApiMethod::Delete,
            path,
            handler,
            options.unwrap_or_default(),
        )
    }

    #[harmony(
        description = "Registers a HEAD API route.",
        args(path: String, handler: ApiHandler, options: Option<ApiRouteOptions>)
    )]
    fn head(
        path: String,
        handler: luau::Function,
        options: Option<RouteOptions>,
        context: &luau::CallContext,
        vm: &luau::Vm,
    ) -> luau::runtime::Result<()> {
        register_route(
            vm,
            context,
            ApiMethod::Head,
            path,
            handler,
            options.unwrap_or_default(),
        )
    }

    #[harmony(
        description = "Registers an OPTIONS API route.",
        args(path: String, handler: ApiHandler, options: Option<ApiRouteOptions>)
    )]
    fn options(
        path: String,
        handler: luau::Function,
        options: Option<RouteOptions>,
        context: &luau::CallContext,
        vm: &luau::Vm,
    ) -> luau::runtime::Result<()> {
        register_route(
            vm,
            context,
            ApiMethod::Options,
            path,
            handler,
            options.unwrap_or_default(),
        )
    }

    #[harmony(
        description = "Registers a plugin WebSocket route.",
        args(path: String, handler: ApiWebSocketHandler, options: Option<ApiWebSocketOptions>)
    )]
    fn websocket(
        path: String,
        handler: luau::Function,
        options: Option<WebSocketRouteOptions>,
        context: &luau::CallContext,
        vm: &luau::Vm,
    ) -> luau::runtime::Result<()> {
        let plugin_id = context.origin.plugin.clone().ok_or_else(|| {
            crate::plugins::runtime_error(
                "lyra/api websocket routes must be registered from plugin Luau code",
            )
        })?;
        harmony_core::ensure_current_plugin_run(vm, context)?;
        let routes = vm.data().get::<ApiRouteStore>()?;
        routes.register_websocket(
            plugin_id,
            path,
            handler,
            options.and_then(|options| options.auth),
            context.clone(),
        )
    }

    #[harmony(
        name = "response.json",
        description = "Builds a JSON response.",
        args(status: u16, body: JsonValue, headers: Option<ApiHeaders>),
        returns(ApiResponse)
    )]
    fn response_json(
        status: Status,
        body: luau::Value,
        headers: Option<luau::Value>,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let headers = optional_value(headers);
        let mut response = kind_table("json");
        response.set_field("status", i64::from(status.0));
        response.set_field(
            "headers",
            merge_headers(&[("content-type", "application/json")], headers)?,
        );
        response.set_field("body", body);
        Ok(response)
    }

    #[harmony(
        name = "response.empty",
        description = "Builds an empty response.",
        args(status: Option<u16>, headers: Option<ApiHeaders>),
        returns(ApiResponse)
    )]
    fn response_empty(
        status: Option<Status>,
        headers: Option<luau::Value>,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let status = status.map_or(StatusCode::NO_CONTENT.as_u16(), |status| status.0);
        let headers = optional_value(headers);
        let mut response = kind_table("empty");
        response.set_field("status", i64::from(status));
        if !is_nil(&headers) {
            response.set_field("headers", headers_to_value(headers)?);
        }
        Ok(response)
    }

    #[harmony(
        name = "response.text",
        description = "Builds a text response.",
        args(status: u16, body: String, headers: Option<ApiHeaders>),
        returns(ApiResponse)
    )]
    fn response_text(
        status: Status,
        body: String,
        headers: Option<luau::Value>,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let headers = optional_value(headers);
        let mut response = kind_table("text");
        response.set_field("status", i64::from(status.0));
        response.set_field("body", body);
        response.set_field(
            "headers",
            merge_headers(&[("content-type", "text/plain; charset=utf-8")], headers)?,
        );
        Ok(response)
    }

    #[harmony(
        name = "response.bytes",
        description = "Builds a binary response.",
        args(status: u16, body: BytesBody, headers: Option<ApiHeaders>),
        returns(ApiResponse)
    )]
    fn response_bytes(
        status: Status,
        body: luau::Value,
        headers: Option<luau::Value>,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        if !matches!(body, luau::Value::String(_) | luau::Value::Buffer(_)) {
            return Err(crate::plugins::runtime_error(
                "response.bytes body must be a string or buffer",
            ));
        }
        let headers = optional_value(headers);
        let mut response = kind_table("bytes");
        response.set_field("status", i64::from(status.0));
        response.set_field("body", body);
        response.set_field(
            "headers",
            merge_headers(&[("content-type", "application/octet-stream")], headers)?,
        );
        Ok(response)
    }

    #[harmony(
        name = "response.filler",
        description = "Builds a response of `size_bytes` filler bytes, which Lyra generates as it sends them, for clients measuring their throughput. Its content type defaults to application/octet-stream, and its headers may not include the framing headers content-length, content-range, accept-ranges and transfer-encoding. Raises past `MAX_FILLER_BYTES`.",
        args(status: u16, size_bytes: u64, headers: Option<ApiHeaders>),
        returns(ApiResponse)
    )]
    fn response_filler(
        status: Status,
        size_bytes: luau::Value,
        headers: Option<luau::Value>,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let size_bytes = filler_size(&size_bytes)?;
        let headers = optional_value(headers);
        let mut response = kind_table("filler");
        response.set_field("status", i64::from(status.0));
        response.set_field("size_bytes", luau::Value::Number(size_bytes as f64));
        response.set_field(
            "headers",
            merge_headers(&[("content-type", "application/octet-stream")], headers)?,
        );
        Ok(response)
    }

    #[harmony(
        name = "response.stream",
        description = "Builds a response whose body `produce` writes as it is sent. After the handler returns, `produce` runs in a thread of its own, acting as the handler did, with the writer of the body; the body ends whole only when `produce` returns without a write having raised, and its failure aborts the body, so the client sees a broken transfer rather than a cut-off document. `produce` must be done within the handler's deadline, which counts from the start of the request and includes any wait for one of the few streams Lyra produces at once. A HEAD request or a status without a body never runs it. Its content type defaults to application/octet-stream, and its headers may not include the framing headers content-length, content-range, accept-ranges and transfer-encoding.",
        args(status: u16, produce: ApiStreamProducer, headers: Option<ApiHeaders>),
        returns(ApiResponse)
    )]
    fn response_stream(
        status: Status,
        produce: luau::Function,
        headers: Option<luau::Value>,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let headers = optional_value(headers);
        let mut response = kind_table("stream");
        response.set_field("status", i64::from(status.0));
        response.set_field("produce", luau::Value::Function(produce));
        response.set_field(
            "headers",
            merge_headers(&[("content-type", "application/octet-stream")], headers)?,
        );
        Ok(response)
    }

    #[harmony(
        name = "response.redirect",
        description = "Builds a redirect response.",
        args(status: u16, location: String, headers: Option<ApiHeaders>),
        returns(ApiResponse)
    )]
    fn response_redirect(
        status: Status,
        location: String,
        headers: Option<luau::Value>,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        if !(300..400).contains(&status.0) {
            return Err(crate::plugins::runtime_error(
                "redirect status must be a 3xx HTTP status code",
            ));
        }
        let location = location.trim();
        if location.is_empty() {
            return Err(crate::plugins::runtime_error(
                "redirect location must not be empty",
            ));
        }
        let headers = optional_value(headers);
        let mut response = kind_table("redirect");
        response.set_field("status", i64::from(status.0));
        let mut headers = collect_headers(headers)?;
        headers.set_field(
            "location",
            luau::Value::String(location.as_bytes().to_vec()),
        );
        response.set_field("headers", headers);
        Ok(response)
    }

    #[harmony(
        name = "response.file",
        description = "Builds a file response. The path must resolve inside a library root or the configured cover storage root. Its headers may not include the framing headers content-length, content-range, accept-ranges and transfer-encoding, which Lyra sets for the file it serves; a ranged request answers with 206 or 416 whatever its status.",
        args(status: Option<u16>, path: String, headers: Option<ApiHeaders>, transform: Option<ImageTransformOptions>),
        returns(ApiResponse)
    )]
    fn response_file(
        status: Option<Status>,
        path: String,
        headers: Option<luau::Value>,
        transform: Option<luau::Value>,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let status = status.map_or(StatusCode::OK.as_u16(), |status| status.0);
        let path = path.trim();
        if path.is_empty() {
            return Err(crate::plugins::runtime_error("file path must not be empty"));
        }
        let headers = optional_value(headers);
        let transform = optional_value(transform);
        let mut response = kind_table("file");
        response.set_field("status", i64::from(status));
        response.set_field("path", path);
        if !is_nil(&headers) {
            response.set_field("headers", headers_to_value(headers)?);
        }
        if !is_nil(&transform) {
            response.set_field("transform", transform);
        }
        Ok(response)
    }

    #[harmony(
        name = "response.stream_track",
        description = "Builds a response that streams the track, delivered as `options` ask. A `status` or `headers` set on the response apply to the audio Lyra serves. Its headers replace Lyra's of the same name, and its cache-control also Lyra's pragma and expires, but may not include the framing headers content-length, content-range, accept-ranges and transfer-encoding. Its status, which must be 200 to 203, replaces the 200 of a whole body; a ranged 206 and a 416 keep theirs. When Lyra can't serve the track it answers with its own error, without the response's status or headers.",
        args(track_id: String, options: Option<TrackStreamOptions>),
        returns(ApiResponse)
    )]
    fn response_stream_track(
        track_id: String,
        options: Option<luau::Value>,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        track_response(ApiResponseKind::StreamTrack, track_id, options)
    }

    #[harmony(
        name = "response.download_track",
        description = "Builds a response that sends the track as a download, delivered as `options` ask; without `public` the caller needs the download permission. A `status` or `headers` set on the response apply to the audio Lyra serves. Its headers replace Lyra's of the same name, and its cache-control also Lyra's pragma and expires, but may not include the framing headers content-length, content-range, accept-ranges and transfer-encoding. Its status, which must be 200 to 203, replaces the 200 of a whole body; a ranged 206 and a 416 keep theirs. When Lyra can't serve the track it answers with its own error, without the response's status or headers.",
        args(track_id: String, options: Option<TrackServeOptions>),
        returns(ApiResponse)
    )]
    fn response_download_track(
        track_id: String,
        options: Option<luau::Value>,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        track_response(ApiResponseKind::DownloadTrack, track_id, options)
    }

    #[harmony(
        name = "response.hls_playlist",
        description = "Builds a response that serves the track's HLS media playlist, encoded as `options` ask. A `status` or `headers` set on the response apply to the playlist Lyra serves. Its headers replace Lyra's of the same name, and its cache-control also Lyra's pragma and expires, but may not include the framing headers content-length, content-range, accept-ranges and transfer-encoding. Its status, which must be 200 to 203, replaces Lyra's 200. When Lyra can't serve the track it answers with its own error, without the response's status or headers.",
        args(track_id: String, options: Option<HlsServeOptions>),
        returns(ApiResponse)
    )]
    fn response_hls_playlist(
        track_id: String,
        options: Option<luau::Value>,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        track_response(ApiResponseKind::HlsPlaylist, track_id, options)
    }

    #[harmony(
        description = "The format Lyra delivers for `name`, a format or another name of its container such as `m4b` or `oga`, in any case; nil when Lyra delivers no such format.",
        returns(Option<options::DeliveryFormat>)
    )]
    fn delivery_format(name: String) -> Option<&'static str> {
        lyra_ffmpeg::AudioFormat::parse(&name).map(|format| format.as_str())
    }

    #[harmony(
        name = "query.bool",
        description = "Reads a boolean query parameter.",
        args(query: ApiQueryParams, key: String, default: Option<bool>)
    )]
    fn query_bool(
        query: luau::Table,
        key: String,
        default: Option<bool>,
    ) -> luau::runtime::Result<Option<bool>> {
        let value = first_query_value(&query, &key)?;
        let parsed = value.as_deref().and_then(parse_query_bool);
        Ok(parsed.or(default))
    }

    #[harmony(
        name = "query.int",
        description = "Reads an integer query parameter.",
        args(query: ApiQueryParams, key: String, default: Option<i64>, min: Option<i64>, max: Option<i64>)
    )]
    fn query_int(
        query: luau::Table,
        key: String,
        default: Option<i64>,
        min: Option<i64>,
        max: Option<i64>,
    ) -> luau::runtime::Result<Option<i64>> {
        let parsed = first_query_value(&query, &key)?
            .and_then(|raw| raw.trim().parse::<i64>().ok())
            .filter(|parsed| {
                min.is_none_or(|min| *parsed >= min) && max.is_none_or(|max| *parsed <= max)
            });
        Ok(parsed.or(default))
    }

    #[harmony(
        name = "query.csv",
        description = "Splits comma-separated query parameter values.",
        args(query: ApiQueryParams, key: String),
        returns(Vec<String>)
    )]
    fn query_csv(query: luau::Table, key: String) -> luau::runtime::Result<luau::OwnedTable> {
        let mut result = luau::OwnedTable::new();
        if let luau::Value::Table(occurrences) = query.get(&key)? {
            let mut values = occurrence_values(&occurrences)?;
            values.sort_by_key(|(index, _)| *index);
            for (_, raw) in values {
                for part in raw.split(',') {
                    let item = part.trim();
                    if !item.is_empty() {
                        result.push_array(item);
                    }
                }
            }
        }
        Ok(result)
    }
}

fn register_route(
    vm: &luau::Vm,
    context: &luau::CallContext,
    method: ApiMethod,
    path: String,
    handler: luau::Function,
    options: RouteOptions,
) -> luau::runtime::Result<()> {
    let plugin_id = context.origin.plugin.clone().ok_or_else(|| {
        crate::plugins::runtime_error("lyra/api routes must be registered from plugin Luau code")
    })?;
    harmony_core::ensure_current_plugin_run(vm, context)?;
    let routes = vm.data().get::<ApiRouteStore>()?;
    routes.register(ApiRouteRegistration {
        plugin_id,
        method,
        path,
        handler,
        auth_mode: options.auth,
        case_insensitive: options.case_insensitive,
        max_body_bytes: options.max_body_bytes,
        context: context.clone(),
    })
}

/// A track response of `kind`, raising on `options` it could not be served with.
fn track_response(
    kind: ApiResponseKind,
    track_id: String,
    options: Option<luau::Value>,
) -> luau::runtime::Result<luau::OwnedTable> {
    let options = optional_value(options);
    options::track_response_options(kind, &options)?;
    let mut response = kind_table(kind.as_str());
    response.set_field("track_id", track_id);
    if !is_nil(&options) {
        response.set_field("options", options);
    }
    Ok(response)
}

/// A filler response's size, at most [`MAX_FILLER_BYTES`].
pub(crate) fn filler_size(value: &luau::Value) -> luau::runtime::Result<u64> {
    let size = match value {
        luau::Value::Integer(size) => u64::try_from(*size).ok(),
        luau::Value::Number(size) if size.fract() == 0.0 && *size >= 0.0 => Some(*size as u64),
        _ => None,
    };
    match size {
        Some(size) if size <= MAX_FILLER_BYTES as u64 => Ok(size),
        _ => {
            let got = match value {
                luau::Value::Integer(size) => size.to_string(),
                luau::Value::Number(size) => size.to_string(),
                other => other.type_name().to_string(),
            };
            Err(crate::plugins::runtime_error(format!(
                "filler size_bytes must be an integer from 0 to MAX_FILLER_BYTES ({MAX_FILLER_BYTES}), got {got}"
            )))
        }
    }
}

fn first_query_value(query: &luau::Table, key: &str) -> luau::runtime::Result<Option<String>> {
    match query.get(key)? {
        luau::Value::Table(values) => {
            let mut values = occurrence_values(&values)?;
            values.sort_by_key(|(index, _)| *index);
            Ok(values.into_iter().next().map(|(_, value)| value))
        }
        luau::Value::Nil => Ok(None),
        other => Err(crate::plugins::runtime_error(format!(
            "query parameter '{key}' must be an array table, got {}",
            other.type_name()
        ))),
    }
}

fn occurrence_values(table: &luau::Table) -> luau::runtime::Result<Vec<(i64, String)>> {
    let mut values = Vec::new();
    for (key, value) in table.pairs()? {
        let index = match key {
            luau::Value::Integer(index) => index,
            luau::Value::Number(index) => index as i64,
            _ => continue,
        };
        let luau::Value::String(value) = value else {
            continue;
        };
        values.push((
            index,
            String::from_utf8(value).map_err(crate::plugins::runtime_error)?,
        ));
    }
    Ok(values)
}

fn kind_table(kind: &'static str) -> luau::OwnedTable {
    let mut response = luau::OwnedTable::with_capacity(0, 4);
    response.set_field("kind", kind);
    response
}

fn merge_headers(
    defaults: &[(&'static str, &'static str)],
    custom: luau::Value,
) -> luau::runtime::Result<luau::Value> {
    let mut headers = collect_headers(custom)?;
    for (name, value) in defaults.iter().rev() {
        if !headers_has_field(&headers, name) {
            headers.set_field(*name, *value);
        }
    }
    Ok(headers.into())
}

fn headers_to_value(headers: luau::Value) -> luau::runtime::Result<luau::Value> {
    Ok(collect_headers(headers)?.into())
}

fn collect_headers(custom: luau::Value) -> luau::runtime::Result<luau::OwnedTable> {
    let mut headers = luau::OwnedTable::new();
    match custom {
        luau::Value::Nil => {}
        luau::Value::Table(table) => {
            for (key, value) in table.pairs()? {
                let (luau::Value::String(key), luau::Value::String(value)) = (key, value) else {
                    continue;
                };
                headers.set_field(
                    String::from_utf8(key).map_err(crate::plugins::runtime_error)?,
                    luau::Value::String(value),
                );
            }
        }
        luau::Value::TableData(table) => {
            for (key, value) in table.fields() {
                headers.set_field(key.clone(), value.clone());
            }
        }
        other => {
            return Err(crate::plugins::runtime_error(format!(
                "headers must be a table, got {}",
                other.type_name()
            )));
        }
    }
    Ok(headers)
}

fn headers_has_field(headers: &luau::OwnedTable, name: &str) -> bool {
    headers
        .fields()
        .iter()
        .any(|(key, _)| key.eq_ignore_ascii_case(name))
}

fn is_nil(value: &luau::Value) -> bool {
    matches!(value, luau::Value::Nil)
}

fn optional_value(value: Option<luau::Value>) -> luau::Value {
    value.unwrap_or(luau::Value::Nil)
}

fn validate_status(status: i64) -> luau::runtime::Result<u16> {
    u16::try_from(status)
        .ok()
        .and_then(|status| StatusCode::from_u16(status).ok().map(|_| status))
        .ok_or_else(|| crate::plugins::runtime_error(format!("invalid HTTP status code: {status}")))
}

fn parse_query_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

#[cfg(feature = "docgen")]
fn support_aliases() -> Vec<TypeAliasDescriptor> {
    let mut aliases = vec![
        ApiMethod::type_alias_descriptor(),
        RouteAuthMode::type_alias_descriptor(),
        TypeAliasDescriptor::new(
            "ApiHeaders",
            LuauType::map(String::luau_type(), String::luau_type()),
            Some("String-keyed header map."),
        ),
        TypeAliasDescriptor::new(
            "ApiQueryParams",
            LuauType::map(String::luau_type(), Vec::<String>::luau_type()),
            Some("Query parameter map."),
        ),
        TypeAliasDescriptor::new(
            "ApiPathParams",
            LuauType::map(String::luau_type(), String::luau_type()),
            Some("String-keyed path parameter map."),
        ),
    ];
    aliases.extend(options::delivery_aliases());
    aliases.extend(api_response_aliases());
    aliases.extend([
        JsonValue::type_alias_descriptor(),
        TypeAliasDescriptor::new(
            "ApiHandler",
            LuauType::function(
                vec![FunctionParameter {
                    name: Some("ctx"),
                    ty: ApiContext::luau_type(),
                    variadic: false,
                }],
                vec![ApiResponse::luau_type()],
            ),
            Some("API route handler."),
        ),
        TypeAliasDescriptor::new(
            "ApiStreamProducer",
            LuauType::function(
                vec![FunctionParameter {
                    name: Some("writer"),
                    ty: LuauType::literal("ApiStreamWriter"),
                    variadic: false,
                }],
                Vec::new(),
            ),
            Some("Writes a streamed response's body."),
        ),
        TypeAliasDescriptor::new(
            "ApiWebSocketHandler",
            LuauType::function(
                vec![
                    FunctionParameter {
                        name: Some("reader"),
                        ty: ApiWebSocketReader::luau_type(),
                        variadic: false,
                    },
                    FunctionParameter {
                        name: Some("sender"),
                        ty: ApiWebSocketSender::luau_type(),
                        variadic: false,
                    },
                    FunctionParameter {
                        name: Some("ctx"),
                        ty: ApiContext::luau_type(),
                        variadic: false,
                    },
                ],
                Vec::new(),
            ),
            Some("API WebSocket route handler."),
        ),
    ]);
    aliases
}

#[cfg(feature = "docgen")]
fn api_response_aliases() -> Vec<TypeAliasDescriptor> {
    vec![
        response_alias(
            "ApiJsonResponse",
            "json",
            [field("body", JsonValue::luau_type())],
        ),
        response_alias("ApiEmptyResponse", "empty", []),
        response_alias(
            "ApiTextResponse",
            "text",
            [field("body", String::luau_type())],
        ),
        response_alias(
            "ApiBytesResponse",
            "bytes",
            [field(
                "body",
                LuauType::union(vec![String::luau_type(), LuauType::literal("buffer")]),
            )],
        ),
        response_alias("ApiRedirectResponse", "redirect", []),
        response_alias(
            "ApiFileResponse",
            "file",
            [
                field("path", String::luau_type()),
                field(
                    "transform",
                    LuauType::optional(ImageTransformOptions::luau_type()),
                ),
            ],
        ),
        response_alias(
            "ApiStreamTrackResponse",
            "stream_track",
            [
                field("track_id", String::luau_type()),
                field(
                    "options",
                    LuauType::optional(TrackStreamOptions::luau_type()),
                ),
            ],
        ),
        response_alias(
            "ApiDownloadTrackResponse",
            "download_track",
            [
                field("track_id", String::luau_type()),
                field(
                    "options",
                    LuauType::optional(TrackServeOptions::luau_type()),
                ),
            ],
        ),
        response_alias(
            "ApiHlsPlaylistResponse",
            "hls_playlist",
            [
                field("track_id", String::luau_type()),
                field("options", LuauType::optional(HlsServeOptions::luau_type())),
            ],
        ),
        response_alias(
            "ApiFillerResponse",
            "filler",
            [field("size_bytes", u64::luau_type())],
        ),
        response_alias(
            "ApiStreamResponse",
            "stream",
            [field("produce", LuauType::literal("ApiStreamProducer"))],
        ),
        TypeAliasDescriptor::new(
            "ApiResponse",
            LuauType::union(vec![
                LuauType::literal("ApiJsonResponse"),
                LuauType::literal("ApiEmptyResponse"),
                LuauType::literal("ApiTextResponse"),
                LuauType::literal("ApiBytesResponse"),
                LuauType::literal("ApiRedirectResponse"),
                LuauType::literal("ApiFileResponse"),
                LuauType::literal("ApiStreamTrackResponse"),
                LuauType::literal("ApiDownloadTrackResponse"),
                LuauType::literal("ApiHlsPlaylistResponse"),
                LuauType::literal("ApiFillerResponse"),
                LuauType::literal("ApiStreamResponse"),
            ]),
            Some("API response returned by route handlers."),
        ),
    ]
}

#[cfg(feature = "docgen")]
fn response_alias(
    name: &'static str,
    kind: &'static str,
    fields: impl IntoIterator<Item = FieldDescriptor>,
) -> TypeAliasDescriptor {
    let mut shape = vec![
        field("kind", LuauType::string_literal(kind)),
        field("status", LuauType::optional(u16::luau_type())),
        field("headers", LuauType::optional(ApiHeaders::luau_type())),
    ];
    shape.extend(fields);
    TypeAliasDescriptor::new(name, LuauType::object(shape), None)
}

/// The websocket sender `api.websocket` handlers get, for the definitions of modules that take one.
#[cfg(feature = "docgen")]
pub(crate) fn websocket_sender_interface() -> harmony_luau::InterfaceDescriptor {
    interface(
        "ApiWebSocketSender",
        [
            field(
                "send",
                LuauType::function(
                    vec![
                        FunctionParameter {
                            name: Some("self"),
                            ty: ApiWebSocketSender::luau_type(),
                            variadic: false,
                        },
                        FunctionParameter {
                            name: Some("text"),
                            ty: String::luau_type(),
                            variadic: false,
                        },
                    ],
                    Vec::new(),
                ),
            ),
            field(
                "close",
                LuauType::function(
                    vec![FunctionParameter {
                        name: Some("self"),
                        ty: ApiWebSocketSender::luau_type(),
                        variadic: false,
                    }],
                    Vec::new(),
                ),
            ),
            field(
                "is_closed",
                LuauType::function(
                    vec![FunctionParameter {
                        name: Some("self"),
                        ty: ApiWebSocketSender::luau_type(),
                        variadic: false,
                    }],
                    vec![bool::luau_type()],
                ),
            ),
        ],
    )
}

#[cfg(feature = "docgen")]
fn support_interfaces() -> Vec<harmony_luau::InterfaceDescriptor> {
    vec![
        AuthPrincipal::interface_descriptor(),
        AuthCredential::interface_descriptor(),
        interface(
            "ApiWebSocketReader",
            [field(
                "recv",
                LuauType::function(
                    vec![FunctionParameter {
                        name: Some("self"),
                        ty: ApiWebSocketReader::luau_type(),
                        variadic: false,
                    }],
                    vec![Option::<String>::luau_type()],
                ),
            )],
        ),
        websocket_sender_interface(),
        interface(
            "ApiRouteOptions",
            [
                described_field(
                    "auth",
                    LuauType::optional(RouteAuthMode::luau_type()),
                    "Whether the route needs an authenticated caller. Defaults to \"required\".",
                ),
                described_field(
                    "case_insensitive",
                    LuauType::optional(bool::luau_type()),
                    "Matches the path's literal segments in any case. Defaults to false.",
                ),
                described_field(
                    "max_body_bytes",
                    LuauType::optional(u64::luau_type()),
                    "The largest request body the route accepts; a larger one is answered with 413 before the handler runs. Defaults to the server's limit of 256 KiB, and may be at most `MAX_BODY_BYTES`.",
                ),
            ],
        ),
        interface(
            "ApiWebSocketOptions",
            [described_field(
                "auth",
                LuauType::optional(RouteAuthMode::luau_type()),
                "Whether the route needs an authenticated caller. Defaults to \"required\".",
            )],
        ),
        interface(
            "ApiAuth",
            [
                field("principal", AuthPrincipal::luau_type()),
                field("credential", AuthCredential::luau_type()),
            ],
        ),
        interface(
            "ApiRequest",
            [
                field("method", String::luau_type()),
                field("path", String::luau_type()),
                field("headers", ApiHeaders::luau_type()),
                field("query", ApiQueryParams::luau_type()),
                field("body_raw", String::luau_type()),
                field("json", JsonValue::luau_type()),
            ],
        ),
        interface(
            "ApiContext",
            [
                field("plugin_id", String::luau_type()),
                field("auth", LuauType::optional(ApiAuth::luau_type())),
                field("request", ApiRequest::luau_type()),
                field("params", ApiPathParams::luau_type()),
            ],
        ),
        interface(
            "ImageTransformOptions",
            [
                field("format", LuauType::optional(String::luau_type())),
                field("quality", LuauType::optional(u8::luau_type())),
                field("max_width", LuauType::optional(u32::luau_type())),
                field("max_height", LuauType::optional(u32::luau_type())),
            ],
        ),
        options::track_serve_options_interface(),
        options::track_stream_options_interface(),
        options::hls_serve_options_interface(),
    ]
}

#[cfg(feature = "docgen")]
fn interface(
    name: &'static str,
    fields: impl IntoIterator<Item = FieldDescriptor>,
) -> harmony_luau::InterfaceDescriptor {
    let mut descriptor = harmony_luau::InterfaceDescriptor::new(name, None);
    descriptor.fields.extend(fields);
    descriptor
}

#[cfg(feature = "docgen")]
fn field(name: &'static str, ty: LuauType) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description: None,
    }
}

#[cfg(feature = "docgen")]
fn described_field(name: &'static str, ty: LuauType, description: &'static str) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description: Some(description),
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &ApiModule::module_descriptor(),
        &support_aliases(),
        &support_interfaces(),
        &[<crate::plugins::executor::ApiStreamWriter as harmony_luau::DescribeUserData>::class_descriptor()],
    )
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) async fn registered_handler(
        method: &str,
        path: &str,
    ) -> Option<crate::services::plugin_dispatch::PluginCallbackHandle> {
        let registry = api_route_registry().read_owned().await;
        registry
            .snapshot()
            .into_iter()
            .find(|route| route.key.method.as_ref() == method && route.key.path.as_ref() == path)
            .map(|route| route.handler)
    }
}
