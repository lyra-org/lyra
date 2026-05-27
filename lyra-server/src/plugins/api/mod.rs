// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod headers;
mod image;
mod parsing;
mod router;
mod types;
mod websocket;

use self::headers::{
    header_pairs,
    plugin_api_response_to_axum,
};
use self::image::{
    image_format_mime,
    parse_image_transform_options,
    transform_image,
};
use self::parsing::{
    infer_content_type,
    join_preferred_codecs,
    parse_hls_serve_options,
    parse_track_serve_options,
    parse_u32_json,
};
pub(crate) use self::router::ApiRouteStore;
use self::router::{
    API_ROUTE_REGISTRY,
    RegisteredRoute,
    RouteAuthMode,
    build_router,
    fallback,
    find_static_dir,
    freeze_registry,
    initialize_registry,
    initialize_router,
    install_router,
    installed_static_dir,
};
#[cfg(feature = "docgen")]
use self::types::{
    ApiAuth,
    ApiContext,
    ApiHeaders,
    ApiPathParams,
    ApiRequest,
    ApiResponse,
    ApiWebSocketReader,
    ApiWebSocketSender,
    HlsServeOptions,
    ImageTransformOptions,
    TrackServeOptions,
};
use self::types::{
    ApiHandler,
    ApiMethod,
    ApiQueryParams,
    ApiRouteAuthMode,
    ApiWebSocketHandler,
};
use self::websocket::dispatch_websocket_route;

use std::{
    cell::{
        Cell,
        RefCell,
    },
    collections::{
        HashMap,
        HashSet,
    },
    env,
    ffi::OsString,
    path::{
        Path as FsPath,
        PathBuf,
    },
    sync::{
        Arc,
        LazyLock,
    },
};

use anyhow::{
    Context,
    Result,
    bail,
};
use axum::{
    Router,
    body::{
        Body,
        Bytes,
    },
    extract::{
        Path,
        Query,
        Request,
        WebSocketUpgrade,
        ws::{
            Message,
            WebSocket,
        },
    },
    http::{
        HeaderMap,
        Method,
        StatusCode,
        Uri,
        header::{
            ACCEPT_RANGES,
            CONTENT_LENGTH,
            CONTENT_RANGE,
            CONTENT_TYPE,
            HeaderName,
            HeaderValue,
            RANGE,
        },
    },
    response::{
        IntoResponse,
        Response,
    },
    routing::{
        MethodFilter,
        MethodRouter,
        any,
        get,
        on,
    },
};
use harmony_core::{
    FunctionSpec,
    ModuleSpec,
};
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
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    TypeAliasDescriptor,
};
use tower::ServiceExt;
use tower_http::services::{
    ServeDir,
    ServeFile,
};

#[cfg(feature = "docgen")]
use crate::plugins::auth::{
    AuthCredential,
    Principal as AuthPrincipal,
};
use crate::routes::{
    build_ranged_file_body,
    download_track_response,
    registry::{
        RouteKey,
        is_placeholder_segment,
        lowercase_literal_segments,
    },
    serve_hls_playlist_for_track,
    stream_track_response,
};
use crate::services::auth::resolve_optional_auth;
use crate::services::remote::constants::{
    MAX_MESSAGE_SIZE,
    PING_INTERVAL,
    PONG_TIMEOUT,
    WRITE_TIMEOUT,
};

struct ApiModule;

pub(crate) async fn install(
    app: Router,
    reservations: std::collections::HashSet<RouteKey>,
) -> anyhow::Result<Router> {
    let static_dir = find_static_dir()?;
    if let Some(ref dir) = static_dir {
        tracing::info!(static_dir = %dir.display(), "serving static directory");
    }
    initialize_registry(reservations).await;
    initialize_router(static_dir).await;
    Ok(app.fallback(fallback))
}

pub(crate) async fn finalize() -> anyhow::Result<()> {
    freeze_registry().await;
    rebuild_registered_routes().await
}

pub(crate) async fn rebuild_registered_routes() -> anyhow::Result<()> {
    let (router, ci_router) = build_router(installed_static_dir().await).await?;
    install_router(router, ci_router).await;
    Ok(())
}

pub(crate) async fn unfreeze_plugin_routes(plugin_id: crate::plugins::lifecycle::PluginId) {
    API_ROUTE_REGISTRY.write().await.unfreeze_plugin(plugin_id);
}

pub(crate) async fn refreeze_plugin_routes(plugin_id: &crate::plugins::lifecycle::PluginId) {
    API_ROUTE_REGISTRY.write().await.refreeze_plugin(plugin_id);
}

pub(crate) async fn teardown_plugin_routes(plugin_id: &crate::plugins::lifecycle::PluginId) {
    API_ROUTE_REGISTRY.write().await.clear_bucket(plugin_id);
    rebuild_registered_routes()
        .await
        .unwrap_or_else(|err| panic!("router rebuild must not fail post-teardown: {err}"));
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) async fn registered_handler(method: &str, path: &str) -> Option<u64> {
        let registry = API_ROUTE_REGISTRY.read().await;
        registry
            .snapshot()
            .into_iter()
            .find(|route| route.key.method.as_ref() == method && route.key.path.as_ref() == path)
            .map(|route| route.handler_id)
    }
}

fn route_spec(
    name: &'static str,
    include_method: bool,
    callback: for<'vm> fn(luau::CallFrame<'vm>) -> luau::runtime::Result<()>,
) -> FunctionSpec {
    let mut spec = FunctionSpec::sync_fn(name);
    if include_method {
        spec = spec.arg_name("method").args::<ApiMethod>();
    }
    spec.arg_name("path")
        .args::<String>()
        .arg_name("handler")
        .args::<ApiHandler>()
        .arg_name("auth_mode")
        .args::<Option<ApiRouteAuthMode>>()
        .arg_name("case_insensitive")
        .args::<Option<bool>>()
        .returns::<()>()
        .call(callback)
}

fn response_spec(
    name: &'static str,
    callback: for<'vm> fn(luau::CallFrame<'vm>) -> luau::runtime::Result<()>,
) -> FunctionSpec {
    FunctionSpec::sync_fn(name).call(callback)
}

fn register_route_callback(
    mut frame: luau::CallFrame<'_>,
    method: &'static str,
) -> luau::runtime::Result<()> {
    let path = frame.args.read_named("path")?;
    let handler = frame.args.read_named("handler")?;
    let auth_mode = frame.args.read_optional_named("auth_mode")?;
    let case_insensitive = frame
        .args
        .read_optional_named("case_insensitive")?
        .unwrap_or(false);
    let plugin_id = frame.context.origin.plugin.clone().ok_or_else(|| {
        crate::plugins::runtime_error("lyra/api routes must be registered from plugin Luau code")
    })?;
    let routes = frame.vm.data().get::<ApiRouteStore>()?;
    routes.register(
        plugin_id,
        method,
        path,
        handler,
        auth_mode,
        case_insensitive,
        core_call_context(&frame.context),
    )
}

fn route_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let method: String = frame.args.read_named("method")?;
    let normalized = normalize_api_method(&method)?;
    register_route_callback(frame, normalized)
}

fn get_callback(frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    register_route_callback(frame, "GET")
}

fn post_callback(frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    register_route_callback(frame, "POST")
}

fn put_callback(frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    register_route_callback(frame, "PUT")
}

fn patch_callback(frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    register_route_callback(frame, "PATCH")
}

fn delete_callback(frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    register_route_callback(frame, "DELETE")
}

fn head_callback(frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    register_route_callback(frame, "HEAD")
}

fn options_callback(frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    register_route_callback(frame, "OPTIONS")
}

fn websocket_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let path = frame.args.read_named("path")?;
    let handler = frame.args.read_named("handler")?;
    let auth_mode = frame.args.read_optional_named("auth_mode")?;
    let plugin_id = frame.context.origin.plugin.clone().ok_or_else(|| {
        crate::plugins::runtime_error(
            "lyra/api websocket routes must be registered from plugin Luau code",
        )
    })?;
    let routes = frame.vm.data().get::<ApiRouteStore>()?;
    routes.register_websocket(
        plugin_id,
        path,
        handler,
        auth_mode,
        core_call_context(&frame.context),
    )
}

fn response_json_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let status = read_status(&mut frame, "status")?;
    let body = frame.args.read_named("body")?;
    let headers = optional_value(frame.args.read_optional_named("headers")?);
    let mut response = kind_table("json");
    response.set_field("status", luau::Value::Integer(i64::from(status)));
    response.set_field(
        "headers",
        merge_headers(frame.vm, &[("content-type", "application/json")], headers)?,
    );
    response.set_field("body", body);
    frame.returns.write(luau::Value::TableData(response))
}

fn response_empty_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let status = frame
        .args
        .read_optional_named::<i64>("status")?
        .unwrap_or(i64::from(StatusCode::NO_CONTENT.as_u16()));
    validate_status(status)?;
    let headers = optional_value(frame.args.read_optional_named("headers")?);
    let mut response = kind_table("empty");
    response.set_field("status", luau::Value::Integer(status));
    if !is_nil(&headers) {
        response.set_field("headers", headers_to_value(frame.vm, headers)?);
    }
    frame.returns.write(luau::Value::TableData(response))
}

fn response_text_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let status = read_status(&mut frame, "status")?;
    let body: String = frame.args.read_named("body")?;
    let headers = optional_value(frame.args.read_optional_named("headers")?);
    let mut response = kind_table("text");
    response.set_field("status", luau::Value::Integer(i64::from(status)));
    response.set_field("body", luau::Value::String(body.into_bytes()));
    response.set_field(
        "headers",
        merge_headers(
            frame.vm,
            &[("content-type", "text/plain; charset=utf-8")],
            headers,
        )?,
    );
    frame.returns.write(luau::Value::TableData(response))
}

fn response_bytes_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let status = read_status(&mut frame, "status")?;
    let body = frame.args.read_named("body")?;
    if !matches!(body, luau::Value::String(_) | luau::Value::Buffer(_)) {
        return Err(crate::plugins::runtime_error(
            "response.bytes body must be a string or buffer",
        ));
    }
    let headers = optional_value(frame.args.read_optional_named("headers")?);
    let mut response = kind_table("bytes");
    response.set_field("status", luau::Value::Integer(i64::from(status)));
    response.set_field("body", body);
    response.set_field(
        "headers",
        merge_headers(
            frame.vm,
            &[("content-type", "application/octet-stream")],
            headers,
        )?,
    );
    frame.returns.write(luau::Value::TableData(response))
}

fn response_redirect_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let status = read_status(&mut frame, "status")?;
    if !(300..400).contains(&status) {
        return Err(crate::plugins::runtime_error(
            "redirect status must be a 3xx HTTP status code",
        ));
    }
    let location: String = frame.args.read_named("location")?;
    let location = location.trim();
    if location.is_empty() {
        return Err(crate::plugins::runtime_error(
            "redirect location must not be empty",
        ));
    }
    let headers = optional_value(frame.args.read_optional_named("headers")?);
    let mut response = kind_table("redirect");
    response.set_field("status", luau::Value::Integer(i64::from(status)));
    let mut headers = collect_headers(frame.vm, headers)?;
    headers.set_field(
        "location",
        luau::Value::String(location.as_bytes().to_vec()),
    );
    response.set_field("headers", luau::Value::TableData(headers));
    frame.returns.write(luau::Value::TableData(response))
}

fn response_file_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let status = frame
        .args
        .read_optional_named::<i64>("status")?
        .unwrap_or(i64::from(StatusCode::OK.as_u16()));
    validate_status(status)?;
    let path: String = frame.args.read_named("path")?;
    let path = path.trim();
    if path.is_empty() {
        return Err(crate::plugins::runtime_error("file path must not be empty"));
    }
    let headers = optional_value(frame.args.read_optional_named("headers")?);
    let transform = optional_value(frame.args.read_optional_named("transform")?);
    let mut response = kind_table("file");
    response.set_field("status", luau::Value::Integer(status));
    response.set_field("path", luau::Value::String(path.as_bytes().to_vec()));
    if !is_nil(&headers) {
        response.set_field("headers", headers_to_value(frame.vm, headers)?);
    }
    if !is_nil(&transform) {
        response.set_field("transform", transform);
    }
    frame.returns.write(luau::Value::TableData(response))
}

fn response_stream_track_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    track_response_callback(&mut frame, "stream_track")
}

fn response_download_track_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    track_response_callback(&mut frame, "download_track")
}

fn response_hls_playlist_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    track_response_callback(&mut frame, "hls_playlist")
}

fn track_response_callback(
    frame: &mut luau::CallFrame<'_>,
    kind: &'static str,
) -> luau::runtime::Result<()> {
    let track_id: i64 = frame.args.read_named("track_id")?;
    if track_id <= 0 {
        return Err(crate::plugins::runtime_error(
            "track_id must be a positive id",
        ));
    }
    let options = optional_value(frame.args.read_optional_named("options")?);
    let mut response = kind_table(kind);
    response.set_field("track_id", luau::Value::Integer(track_id));
    if !is_nil(&options) {
        response.set_field("options", options);
    }
    frame.returns.write(luau::Value::TableData(response))
}

fn query_bool_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let query: luau::Table = frame.args.read_named("query")?;
    let key: String = frame.args.read_named("key")?;
    let default: Option<bool> = frame.args.read_optional_named("default")?;
    let value = first_query_value(frame.vm, &query, &key)?;
    let parsed = value.as_deref().and_then(parse_query_bool);
    match parsed.or(default) {
        Some(value) => frame.returns.write(value),
        None => frame.returns.write(luau::Value::Nil),
    }
}

fn query_int_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let query: luau::Table = frame.args.read_named("query")?;
    let key: String = frame.args.read_named("key")?;
    let default: Option<i64> = frame.args.read_optional_named("default")?;
    let min: Option<i64> = frame.args.read_optional_named("min")?;
    let max: Option<i64> = frame.args.read_optional_named("max")?;
    let Some(raw) = first_query_value(frame.vm, &query, &key)? else {
        return match default {
            Some(value) => frame.returns.write(value),
            None => frame.returns.write(luau::Value::Nil),
        };
    };
    let Ok(parsed) = raw.trim().parse::<i64>() else {
        return match default {
            Some(value) => frame.returns.write(value),
            None => frame.returns.write(luau::Value::Nil),
        };
    };
    if min.is_some_and(|min| parsed < min) || max.is_some_and(|max| parsed > max) {
        return match default {
            Some(value) => frame.returns.write(value),
            None => frame.returns.write(luau::Value::Nil),
        };
    }
    frame.returns.write(parsed)
}

fn query_csv_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let query: luau::Table = frame.args.read_named("query")?;
    let key: String = frame.args.read_named("key")?;
    let mut result = luau::OwnedTable::new();
    if let luau::Value::Table(occurrences) = query.get_raw(frame.vm, &key)? {
        let mut values = occurrence_values(frame.vm, &occurrences)?;
        values.sort_by_key(|(index, _)| *index);
        for (_, raw) in values {
            for part in raw.split(',') {
                let item = part.trim();
                if !item.is_empty() {
                    result.push_array(luau::Value::String(item.as_bytes().to_vec()));
                }
            }
        }
    }
    frame.returns.write(luau::Value::TableData(result))
}

fn first_query_value(
    vm: &luau::Vm,
    query: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Option<String>> {
    match query.get_raw(vm, key)? {
        luau::Value::Table(values) => {
            let mut values = occurrence_values(vm, &values)?;
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

fn occurrence_values(
    vm: &luau::Vm,
    table: &luau::Table,
) -> luau::runtime::Result<Vec<(i64, String)>> {
    let mut values = Vec::new();
    for (key, value) in table.pairs_raw(vm)? {
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
    response.set_field("kind", luau::Value::String(kind.as_bytes().to_vec()));
    response
}

fn merge_headers(
    vm: &luau::Vm,
    defaults: &[(&'static str, &'static str)],
    custom: luau::Value,
) -> luau::runtime::Result<luau::Value> {
    let mut headers = collect_headers(vm, custom)?;
    for (name, value) in defaults.iter().rev() {
        if !headers_has_field(&headers, name) {
            headers.set_field(*name, luau::Value::String(value.as_bytes().to_vec()));
        }
    }
    Ok(luau::Value::TableData(headers))
}

fn headers_to_value(vm: &luau::Vm, headers: luau::Value) -> luau::runtime::Result<luau::Value> {
    Ok(luau::Value::TableData(collect_headers(vm, headers)?))
}

fn collect_headers(vm: &luau::Vm, custom: luau::Value) -> luau::runtime::Result<luau::OwnedTable> {
    let mut headers = luau::OwnedTable::new();
    match custom {
        luau::Value::Nil => {}
        luau::Value::Table(table) => {
            for (key, value) in table.pairs_raw(vm)? {
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
    headers.fields().iter().any(|(key, _)| key == name)
}

fn is_nil(value: &luau::Value) -> bool {
    matches!(value, luau::Value::Nil)
}

fn optional_value(value: Option<luau::Value>) -> luau::Value {
    value.unwrap_or(luau::Value::Nil)
}

fn read_status(frame: &mut luau::CallFrame<'_>, name: &'static str) -> luau::runtime::Result<u16> {
    let status: i64 = frame.args.read_named(name)?;
    validate_status(status)
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

fn normalize_api_method(method: &str) -> luau::runtime::Result<&'static str> {
    match method.trim().to_ascii_uppercase().as_str() {
        "GET" => Ok("GET"),
        "POST" => Ok("POST"),
        "PUT" => Ok("PUT"),
        "PATCH" => Ok("PATCH"),
        "DELETE" => Ok("DELETE"),
        "HEAD" => Ok("HEAD"),
        "OPTIONS" => Ok("OPTIONS"),
        _ => Err(crate::plugins::runtime_error(format!(
            "unsupported API method: {method}"
        ))),
    }
}

fn normalize_auth_mode(auth_mode: Option<&str>) -> luau::runtime::Result<RouteAuthMode> {
    match auth_mode
        .map(str::trim)
        .filter(|mode| !mode.is_empty())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        None | Some("required") => Ok(RouteAuthMode::Required),
        Some("public") => Ok(RouteAuthMode::Public),
        Some(other) => Err(crate::plugins::runtime_error(format!(
            "unsupported auth mode '{other}'; expected 'public' or 'required'"
        ))),
    }
}

fn core_call_context(context: &luau::CallContext) -> harmony_core::CallContext {
    let mut caller = harmony_core::ContextBag::default();
    for (type_id, value) in context.caller.cloned_entries() {
        caller.insert_shared(type_id, value);
    }
    harmony_core::CallContext {
        origin: harmony_core::ChunkOrigin {
            module: context
                .origin
                .module
                .as_ref()
                .map(|module| harmony_core::ModuleId(module.0.clone())),
            plugin: context.origin.plugin.clone(),
            path: context.origin.path.clone(),
        },
        capability: context
            .capability
            .as_ref()
            .map(|capability| harmony_core::CapabilityId(capability.0.clone())),
        caller,
        task_group: harmony_core::TaskGroupId(context.task_group.0),
    }
}

#[cfg(feature = "docgen")]
fn param(name: &'static str, ty: LuauType) -> ParameterDescriptor {
    ParameterDescriptor {
        name,
        ty,
        description: None,
        variadic: false,
    }
}

#[cfg(feature = "docgen")]
fn route_params(include_method: bool) -> Vec<ParameterDescriptor> {
    let mut params = Vec::new();
    if include_method {
        params.push(param("method", ApiMethod::luau_type()));
    }
    params.extend([
        param("path", String::luau_type()),
        param("handler", ApiHandler::luau_type()),
        param(
            "auth_mode",
            LuauType::optional(ApiRouteAuthMode::luau_type()),
        ),
        param("case_insensitive", LuauType::optional(bool::luau_type())),
    ]);
    params
}

#[cfg(feature = "docgen")]
fn response_returns() -> Vec<LuauType> {
    vec![ApiResponse::luau_type()]
}

#[cfg(feature = "docgen")]
impl DescribeModule for ApiModule {
    fn module_descriptor() -> ModuleDescriptor {
        let mut descriptor = ModuleDescriptor::new("Api", "api", None);
        descriptor.functions.extend([
            ModuleFunctionDescriptor {
                path: vec!["route"],
                description: Some("Registers a plugin API route."),
                params: route_params(true),
                returns: vec![],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["get"],
                description: Some("Registers a GET API route."),
                params: route_params(false),
                returns: vec![],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["post"],
                description: Some("Registers a POST API route."),
                params: route_params(false),
                returns: vec![],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["put"],
                description: Some("Registers a PUT API route."),
                params: route_params(false),
                returns: vec![],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["patch"],
                description: Some("Registers a PATCH API route."),
                params: route_params(false),
                returns: vec![],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["delete"],
                description: Some("Registers a DELETE API route."),
                params: route_params(false),
                returns: vec![],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["head"],
                description: Some("Registers a HEAD API route."),
                params: route_params(false),
                returns: vec![],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["options"],
                description: Some("Registers an OPTIONS API route."),
                params: route_params(false),
                returns: vec![],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["websocket"],
                description: Some("Registers a plugin WebSocket route."),
                params: vec![
                    param("path", String::luau_type()),
                    param("handler", ApiWebSocketHandler::luau_type()),
                    param(
                        "auth_mode",
                        LuauType::optional(ApiRouteAuthMode::luau_type()),
                    ),
                ],
                returns: vec![],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["response", "json"],
                description: Some("Builds a JSON response."),
                params: vec![
                    param("status", u16::luau_type()),
                    param("body", JsonValue::luau_type()),
                    param("headers", LuauType::optional(ApiHeaders::luau_type())),
                ],
                returns: response_returns(),
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["response", "empty"],
                description: Some("Builds an empty response."),
                params: vec![
                    param("status", LuauType::optional(u16::luau_type())),
                    param("headers", LuauType::optional(ApiHeaders::luau_type())),
                ],
                returns: response_returns(),
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["response", "text"],
                description: Some("Builds a text response."),
                params: vec![
                    param("status", u16::luau_type()),
                    param("body", String::luau_type()),
                    param("headers", LuauType::optional(ApiHeaders::luau_type())),
                ],
                returns: response_returns(),
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["response", "bytes"],
                description: Some("Builds a binary response."),
                params: vec![
                    param("status", u16::luau_type()),
                    param(
                        "body",
                        LuauType::union(vec![String::luau_type(), LuauType::literal("buffer")]),
                    ),
                    param("headers", LuauType::optional(ApiHeaders::luau_type())),
                ],
                returns: response_returns(),
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["response", "redirect"],
                description: Some("Builds a redirect response."),
                params: vec![
                    param("status", u16::luau_type()),
                    param("location", String::luau_type()),
                    param("headers", LuauType::optional(ApiHeaders::luau_type())),
                ],
                returns: response_returns(),
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["response", "file"],
                description: Some("Builds a file response."),
                params: vec![
                    param("status", LuauType::optional(u16::luau_type())),
                    param("path", String::luau_type()),
                    param("headers", LuauType::optional(ApiHeaders::luau_type())),
                    param(
                        "transform",
                        LuauType::optional(ImageTransformOptions::luau_type()),
                    ),
                ],
                returns: response_returns(),
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["response", "stream_track"],
                description: Some("Builds a streamed-track response."),
                params: vec![
                    param("track_id", i64::luau_type()),
                    param(
                        "options",
                        LuauType::optional(TrackServeOptions::luau_type()),
                    ),
                ],
                returns: response_returns(),
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["response", "download_track"],
                description: Some("Builds a download-track response."),
                params: vec![
                    param("track_id", i64::luau_type()),
                    param(
                        "options",
                        LuauType::optional(TrackServeOptions::luau_type()),
                    ),
                ],
                returns: response_returns(),
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["response", "hls_playlist"],
                description: Some("Builds an HLS playlist response."),
                params: vec![
                    param("track_id", i64::luau_type()),
                    param("options", LuauType::optional(HlsServeOptions::luau_type())),
                ],
                returns: response_returns(),
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["query", "bool"],
                description: Some("Reads a boolean query parameter."),
                params: vec![
                    param("query", ApiQueryParams::luau_type()),
                    param("key", String::luau_type()),
                    param("default", LuauType::optional(bool::luau_type())),
                ],
                returns: vec![LuauType::optional(bool::luau_type())],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["query", "int"],
                description: Some("Reads an integer query parameter."),
                params: vec![
                    param("query", ApiQueryParams::luau_type()),
                    param("key", String::luau_type()),
                    param("default", LuauType::optional(i64::luau_type())),
                    param("min", LuauType::optional(i64::luau_type())),
                    param("max", LuauType::optional(i64::luau_type())),
                ],
                returns: vec![LuauType::optional(i64::luau_type())],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["query", "csv"],
                description: Some("Splits comma-separated query parameter values."),
                params: vec![
                    param("query", ApiQueryParams::luau_type()),
                    param("key", String::luau_type()),
                ],
                returns: vec![Vec::<String>::luau_type()],
                yields: false,
            },
        ]);
        descriptor
    }
}

#[cfg(feature = "docgen")]
fn support_aliases() -> Vec<TypeAliasDescriptor> {
    let mut aliases = vec![
        TypeAliasDescriptor::new(
            "ApiMethod",
            LuauType::union(vec![
                LuauType::literal("\"GET\""),
                LuauType::literal("\"POST\""),
                LuauType::literal("\"PUT\""),
                LuauType::literal("\"PATCH\""),
                LuauType::literal("\"DELETE\""),
                LuauType::literal("\"HEAD\""),
                LuauType::literal("\"OPTIONS\""),
            ]),
            Some("HTTP method name."),
        ),
        TypeAliasDescriptor::new(
            "ApiRouteAuthMode",
            LuauType::union(vec![
                LuauType::literal("\"public\""),
                LuauType::literal("\"required\""),
            ]),
            Some("Declared plugin route auth policy. Defaults to \"required\"."),
        ),
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
                field("track_id", i64::luau_type()),
                field(
                    "options",
                    LuauType::optional(TrackServeOptions::luau_type()),
                ),
            ],
        ),
        response_alias(
            "ApiDownloadTrackResponse",
            "download_track",
            [
                field("track_id", i64::luau_type()),
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
                field("track_id", i64::luau_type()),
                field("options", LuauType::optional(HlsServeOptions::luau_type())),
            ],
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
        interface("TrackServeOptions", track_serve_option_fields(true)),
        interface("HlsServeOptions", track_serve_option_fields(false)),
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
fn track_serve_option_fields(include_format: bool) -> Vec<FieldDescriptor> {
    let mut fields = Vec::new();
    if include_format {
        fields.push(field("format", LuauType::optional(String::luau_type())));
    }
    fields.extend([
        field(
            "preferred_codecs",
            LuauType::optional(Vec::<String>::luau_type()),
        ),
        field("bitrate_bps", LuauType::optional(u32::luau_type())),
        field("sample_rate_hz", LuauType::optional(u32::luau_type())),
        field("channels", LuauType::optional(u32::luau_type())),
        field("prefer_vbr", LuauType::optional(bool::luau_type())),
        field("start_offset_ms", LuauType::optional(u64::luau_type())),
    ]);
    fields
}

pub(crate) fn module_spec() -> harmony_core::ModuleSpec {
    ModuleSpec::new("lyra/api")
        .capability("lyra.api")
        .function(route_spec("route", true, route_callback))
        .function(route_spec("get", false, get_callback))
        .function(route_spec("post", false, post_callback))
        .function(route_spec("put", false, put_callback))
        .function(route_spec("patch", false, patch_callback))
        .function(route_spec("delete", false, delete_callback))
        .function(route_spec("head", false, head_callback))
        .function(route_spec("options", false, options_callback))
        .function(
            FunctionSpec::sync_fn("websocket")
                .arg_name("path")
                .args::<String>()
                .arg_name("handler")
                .args::<ApiWebSocketHandler>()
                .arg_name("auth_mode")
                .args::<Option<ApiRouteAuthMode>>()
                .returns::<()>()
                .call(websocket_callback),
        )
        .function(response_spec("response.json", response_json_callback))
        .function(response_spec("response.empty", response_empty_callback))
        .function(response_spec("response.text", response_text_callback))
        .function(response_spec("response.bytes", response_bytes_callback))
        .function(response_spec(
            "response.redirect",
            response_redirect_callback,
        ))
        .function(response_spec("response.file", response_file_callback))
        .function(response_spec(
            "response.stream_track",
            response_stream_track_callback,
        ))
        .function(response_spec(
            "response.download_track",
            response_download_track_callback,
        ))
        .function(response_spec(
            "response.hls_playlist",
            response_hls_playlist_callback,
        ))
        .function(
            FunctionSpec::sync_fn("query.bool")
                .arg_name("query")
                .args::<ApiQueryParams>()
                .arg_name("key")
                .args::<String>()
                .arg_name("default")
                .args::<Option<bool>>()
                .returns::<Option<bool>>()
                .call(query_bool_callback),
        )
        .function(
            FunctionSpec::sync_fn("query.int")
                .arg_name("query")
                .args::<ApiQueryParams>()
                .arg_name("key")
                .args::<String>()
                .arg_name("default")
                .args::<Option<i64>>()
                .arg_name("min")
                .args::<Option<i64>>()
                .arg_name("max")
                .args::<Option<i64>>()
                .returns::<Option<i64>>()
                .call(query_int_callback),
        )
        .function(
            FunctionSpec::sync_fn("query.csv")
                .arg_name("query")
                .args::<ApiQueryParams>()
                .arg_name("key")
                .args::<String>()
                .returns::<Vec<String>>()
                .call(query_csv_callback),
        )
        .install(|_| Ok(harmony_core::ModuleExport::new(ApiModule)))
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &ApiModule::module_descriptor(),
        &support_aliases(),
        &support_interfaces(),
        &[],
    )
}
