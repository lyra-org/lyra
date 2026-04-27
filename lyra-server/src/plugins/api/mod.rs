// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod dispatch;
mod image;
mod registry;
mod response;
mod transport;
mod websocket;

use std::collections::HashSet;
use std::sync::Arc;

use crate::plugins::lifecycle::PluginId;

use anyhow::Result;
use axum::Router;
use harmony_core::{
    LuaAsyncExt,
    Module,
};
use harmony_luau::{
    DescribeTypeAlias,
    FunctionParameter,
    JsonValue,
    LuauType,
    LuauTypeInfo,
    TypeAliasDescriptor,
};
use mlua::{
    Function,
    Lua,
    Table,
    Value,
};

use crate::plugins::auth::{
    AuthCredential,
    Principal as AuthPrincipal,
};

pub(crate) use dispatch::initialize_router;
pub(crate) use registry::{
    freeze_registry,
    initialize_registry,
    teardown_plugin_routes,
};

use crate::routes::registry::RouteKey;
use dispatch::{
    build_router,
    fallback,
    find_static_dir,
    install_router,
    installed_static_dir,
};

pub(crate) async fn install(app: Router, reservations: HashSet<RouteKey>) -> Router {
    let static_dir = find_static_dir();
    if let Some(ref dir) = static_dir {
        tracing::info!(static_dir = %dir.display(), "serving static directory");
    }
    initialize_registry(reservations).await;
    initialize_router(static_dir).await;
    app.fallback(fallback)
}

pub(crate) async fn finalize() -> Result<()> {
    freeze_registry().await;
    rebuild_registered_routes().await
}

pub(crate) async fn rebuild_registered_routes() -> Result<()> {
    let (router, ci_router) = build_router(installed_static_dir().await).await?;
    install_router(router, ci_router).await;
    Ok(())
}

use registry::{
    RouteKind,
    register_route_impl,
};
use response::{
    HlsServeOptions,
    TrackServeOptions,
    response_download_track as build_download_track_response,
    response_empty as build_empty_response,
    response_file as build_file_response,
    response_hls_playlist as build_hls_playlist_response,
    response_json as build_json_response,
    response_stream_track as build_stream_track_response,
    response_text as build_text_response,
};
use websocket::{
    ApiWebSocketHandler,
    ApiWebSocketReader,
    ApiWebSocketSender,
};

pub(crate) async fn unfreeze_plugin_routes(plugin_id: PluginId) {
    registry::unfreeze_plugin_routes(plugin_id).await;
}

pub(crate) async fn refreeze_plugin_routes(plugin_id: &PluginId) {
    registry::refreeze_plugin_routes(plugin_id).await;
}

fn normalize_api_method(method: String) -> mlua::Result<String> {
    let normalized = method.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD" | "OPTIONS" => Ok(normalized),
        _ => Err(mlua::Error::runtime(format!(
            "unsupported API method: {method}"
        ))),
    }
}

async fn register_handler_route(
    caller_plugin_id: Option<PluginId>,
    method: &str,
    path: String,
    auth_mode: Option<String>,
    case_insensitive: Option<bool>,
    handler: Function,
) -> mlua::Result<()> {
    let raw = crate::plugins::id_from_function(&handler)?;
    let handler_plugin_id =
        PluginId::new(raw).map_err(|err| mlua::Error::runtime(err.to_string()))?;

    // The macro's plugin_scoped prelude resolves the caller's plugin id by
    // walking the Lua stack; id_from_function parses the handler's own
    // source name. These should agree — if they don't, plugin A is trying
    // to register a handler whose Lua source claims to belong to plugin B.
    if let Some(caller) = caller_plugin_id
        && caller != handler_plugin_id
    {
        return Err(mlua::Error::runtime(format!(
            "api.route: caller plugin '{caller}' does not match handler source '{handler_plugin_id}'"
        )));
    }

    register_route_impl(
        handler_plugin_id,
        method,
        path,
        auth_mode,
        case_insensitive.unwrap_or(false),
        handler,
        RouteKind::Http,
    )
    .await
}

async fn register_websocket_route(
    caller_plugin_id: Option<PluginId>,
    path: String,
    auth_mode: Option<String>,
    handler: Function,
) -> mlua::Result<()> {
    let raw = crate::plugins::id_from_function(&handler)?;
    let handler_plugin_id =
        PluginId::new(raw).map_err(|err| mlua::Error::runtime(err.to_string()))?;

    if let Some(caller) = caller_plugin_id
        && caller != handler_plugin_id
    {
        return Err(mlua::Error::runtime(format!(
            "api.websocket: caller plugin '{caller}' does not match handler source '{handler_plugin_id}'"
        )));
    }

    register_route_impl(
        handler_plugin_id,
        "GET",
        path,
        auth_mode,
        false,
        handler,
        RouteKind::WebSocket,
    )
    .await
}

#[harmony_macros::interface]
struct ApiAuth {
    principal: AuthPrincipal,
    credential: AuthCredential,
}

#[harmony_macros::interface]
struct ApiRequest {
    method: String,
    path: String,
    headers: ApiHeaders,
    query: ApiQueryParams,
    body_raw: String,
    json: JsonValue,
}

#[harmony_macros::interface]
struct ApiContext {
    plugin_id: String,
    auth: Option<ApiAuth>,
    request: ApiRequest,
    params: ApiPathParams,
}

#[harmony_macros::interface]
struct ImageTransformOptions {
    format: Option<String>,
    quality: Option<u8>,
    max_width: Option<u32>,
    max_height: Option<u32>,
}

#[harmony_macros::interface]
struct ApiJsonResponse {
    kind: String,
    status: u16,
    body: JsonValue,
    headers: Option<ApiHeaders>,
}

#[harmony_macros::interface]
struct ApiEmptyResponse {
    kind: String,
    status: Option<u16>,
    headers: Option<ApiHeaders>,
}

#[harmony_macros::interface]
struct ApiTextResponse {
    kind: String,
    status: u16,
    body: String,
    headers: Option<ApiHeaders>,
}

#[harmony_macros::interface]
struct ApiFileResponse {
    kind: String,
    status: Option<u16>,
    path: String,
    headers: Option<ApiHeaders>,
    transform: Option<ImageTransformOptions>,
}

#[harmony_macros::interface]
struct ApiStreamTrackResponse {
    kind: String,
    track_id: i64,
    options: Option<TrackServeOptions>,
}

#[harmony_macros::interface]
struct ApiDownloadTrackResponse {
    kind: String,
    track_id: i64,
    options: Option<TrackServeOptions>,
}

#[harmony_macros::interface]
struct ApiHlsPlaylistResponse {
    kind: String,
    track_id: i64,
    options: Option<HlsServeOptions>,
}

struct ApiMethod;

impl LuauTypeInfo for ApiMethod {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiMethod")
    }
}

impl DescribeTypeAlias for ApiMethod {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
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
            Some(
                "HTTP method name. Supported values: GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS.",
            ),
        )
    }
}

struct ApiRouteAuthMode;

impl LuauTypeInfo for ApiRouteAuthMode {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiRouteAuthMode")
    }
}

impl DescribeTypeAlias for ApiRouteAuthMode {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "ApiRouteAuthMode",
            LuauType::union(vec![
                LuauType::literal("\"public\""),
                LuauType::literal("\"required\""),
            ]),
            Some("Declared plugin route auth policy. Defaults to \"required\"."),
        )
    }
}

struct ApiHeaders;

impl LuauTypeInfo for ApiHeaders {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiHeaders")
    }
}

impl DescribeTypeAlias for ApiHeaders {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "ApiHeaders",
            LuauType::map(String::luau_type(), String::luau_type()),
            Some("String-keyed header map."),
        )
    }
}

struct ApiQueryParams;

impl LuauTypeInfo for ApiQueryParams {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiQueryParams")
    }
}

impl DescribeTypeAlias for ApiQueryParams {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "ApiQueryParams",
            LuauType::map(String::luau_type(), String::luau_type()),
            Some("String-keyed query parameter map."),
        )
    }
}

struct ApiPathParams;

impl LuauTypeInfo for ApiPathParams {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiPathParams")
    }
}

impl DescribeTypeAlias for ApiPathParams {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "ApiPathParams",
            LuauType::map(String::luau_type(), String::luau_type()),
            Some("String-keyed path parameter map."),
        )
    }
}

struct ApiResponse;

impl LuauTypeInfo for ApiResponse {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiResponse")
    }
}

impl DescribeTypeAlias for ApiResponse {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "ApiResponse",
            LuauType::union(vec![
                LuauType::literal("ApiJsonResponse"),
                LuauType::literal("ApiEmptyResponse"),
                LuauType::literal("ApiTextResponse"),
                LuauType::literal("ApiFileResponse"),
                LuauType::literal("ApiStreamTrackResponse"),
                LuauType::literal("ApiDownloadTrackResponse"),
                LuauType::literal("ApiHlsPlaylistResponse"),
            ]),
            Some("API response returned by route handlers."),
        )
    }
}

struct ApiHandler;

impl LuauTypeInfo for ApiHandler {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiHandler")
    }
}

impl ApiHandler {
    fn handler_type() -> LuauType {
        LuauType::function(
            vec![FunctionParameter {
                name: Some("ctx"),
                ty: LuauType::literal("ApiContext"),
                variadic: false,
            }],
            vec![LuauType::literal("ApiResponse")],
        )
    }
}

impl DescribeTypeAlias for ApiHandler {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "ApiHandler",
            Self::handler_type(),
            Some("API route handler."),
        )
    }
}

fn parse_query_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn query_bool_impl(
    _lua: &Lua,
    (query, key, default): (Table, String, Option<bool>),
) -> mlua::Result<Option<bool>> {
    let value = query.get::<Option<String>>(key.as_str())?;
    let parsed = value.as_deref().and_then(parse_query_bool);
    Ok(parsed.or(default))
}

fn query_int_impl(
    _lua: &Lua,
    (query, key, default, min, max): (Table, String, Option<i64>, Option<i64>, Option<i64>),
) -> mlua::Result<Option<i64>> {
    let Some(raw) = query.get::<Option<String>>(key.as_str())? else {
        return Ok(default);
    };

    let Ok(parsed) = raw.trim().parse::<i64>() else {
        return Ok(default);
    };

    if let Some(min_value) = min
        && parsed < min_value
    {
        return Ok(default);
    }

    if let Some(max_value) = max
        && parsed > max_value
    {
        return Ok(default);
    }

    Ok(Some(parsed))
}

fn query_csv_impl(lua: &Lua, (query, key): (Table, String)) -> mlua::Result<Table> {
    let values = lua.create_table()?;
    let Some(raw) = query.get::<Option<String>>(key.as_str())? else {
        return Ok(values);
    };

    let mut index = 1;
    for part in raw.split(',') {
        let item = part.trim();
        if item.is_empty() {
            continue;
        }
        values.set(index, item)?;
        index += 1;
    }

    Ok(values)
}

struct ApiModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "Api",
    local = "api",
    path = "lyra/api",
    aliases(
        JsonValue,
        ApiMethod,
        ApiRouteAuthMode,
        ApiHeaders,
        ApiQueryParams,
        ApiPathParams,
        ApiResponse,
        ApiHandler,
        ApiWebSocketHandler
    ),
    interfaces(
        AuthPrincipal,
        AuthCredential,
        ApiAuth,
        ApiRequest,
        ApiContext,
        ImageTransformOptions,
        HlsServeOptions,
        TrackServeOptions,
        ApiJsonResponse,
        ApiEmptyResponse,
        ApiTextResponse,
        ApiFileResponse,
        ApiStreamTrackResponse,
        ApiDownloadTrackResponse,
        ApiHlsPlaylistResponse
    ),
    classes(ApiWebSocketReader, ApiWebSocketSender)
)]
impl ApiModule {
    /// Register a route handler at `path` for `method`. `case_insensitive`
    /// routes match mixed-case requests (placeholder captures keep the
    /// client's case); folding is ASCII-only. The CI opt-in is per-path,
    /// not per-(method, path): any CI registration makes every method at
    /// that path accept mixed-case requests, and mixing CS and CI at the
    /// same path is rejected at registration.
    #[harmony(args(method: ApiMethod, path: String, handler: ApiHandler, auth_mode: Option<ApiRouteAuthMode>, case_insensitive: Option<bool>))]
    pub(crate) async fn route(
        plugin_id: Option<Arc<str>>,
        method: String,
        path: String,
        handler: Function,
        auth_mode: Option<String>,
        case_insensitive: Option<bool>,
    ) -> mlua::Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        let method = normalize_api_method(method)?;
        register_handler_route(
            plugin_id,
            &method,
            path,
            auth_mode,
            case_insensitive,
            handler,
        )
        .await
    }

    #[harmony(args(path: String, handler: ApiHandler, auth_mode: Option<ApiRouteAuthMode>, case_insensitive: Option<bool>))]
    pub(crate) async fn get(
        plugin_id: Option<Arc<str>>,
        path: String,
        handler: Function,
        auth_mode: Option<String>,
        case_insensitive: Option<bool>,
    ) -> mlua::Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        register_handler_route(plugin_id, "GET", path, auth_mode, case_insensitive, handler).await
    }

    #[harmony(args(path: String, handler: ApiHandler, auth_mode: Option<ApiRouteAuthMode>, case_insensitive: Option<bool>))]
    pub(crate) async fn post(
        plugin_id: Option<Arc<str>>,
        path: String,
        handler: Function,
        auth_mode: Option<String>,
        case_insensitive: Option<bool>,
    ) -> mlua::Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        register_handler_route(
            plugin_id,
            "POST",
            path,
            auth_mode,
            case_insensitive,
            handler,
        )
        .await
    }

    #[harmony(args(path: String, handler: ApiHandler, auth_mode: Option<ApiRouteAuthMode>, case_insensitive: Option<bool>))]
    pub(crate) async fn put(
        plugin_id: Option<Arc<str>>,
        path: String,
        handler: Function,
        auth_mode: Option<String>,
        case_insensitive: Option<bool>,
    ) -> mlua::Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        register_handler_route(plugin_id, "PUT", path, auth_mode, case_insensitive, handler).await
    }

    #[harmony(args(path: String, handler: ApiHandler, auth_mode: Option<ApiRouteAuthMode>, case_insensitive: Option<bool>))]
    pub(crate) async fn patch(
        plugin_id: Option<Arc<str>>,
        path: String,
        handler: Function,
        auth_mode: Option<String>,
        case_insensitive: Option<bool>,
    ) -> mlua::Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        register_handler_route(
            plugin_id,
            "PATCH",
            path,
            auth_mode,
            case_insensitive,
            handler,
        )
        .await
    }

    #[harmony(path = "delete", args(path: String, handler: ApiHandler, auth_mode: Option<ApiRouteAuthMode>, case_insensitive: Option<bool>))]
    pub(crate) async fn delete_(
        plugin_id: Option<Arc<str>>,
        path: String,
        handler: Function,
        auth_mode: Option<String>,
        case_insensitive: Option<bool>,
    ) -> mlua::Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        register_handler_route(
            plugin_id,
            "DELETE",
            path,
            auth_mode,
            case_insensitive,
            handler,
        )
        .await
    }

    #[harmony(args(path: String, handler: ApiHandler, auth_mode: Option<ApiRouteAuthMode>, case_insensitive: Option<bool>))]
    pub(crate) async fn head(
        plugin_id: Option<Arc<str>>,
        path: String,
        handler: Function,
        auth_mode: Option<String>,
        case_insensitive: Option<bool>,
    ) -> mlua::Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        register_handler_route(
            plugin_id,
            "HEAD",
            path,
            auth_mode,
            case_insensitive,
            handler,
        )
        .await
    }

    #[harmony(args(path: String, handler: ApiHandler, auth_mode: Option<ApiRouteAuthMode>, case_insensitive: Option<bool>))]
    pub(crate) async fn options(
        plugin_id: Option<Arc<str>>,
        path: String,
        handler: Function,
        auth_mode: Option<String>,
        case_insensitive: Option<bool>,
    ) -> mlua::Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        register_handler_route(
            plugin_id,
            "OPTIONS",
            path,
            auth_mode,
            case_insensitive,
            handler,
        )
        .await
    }

    /// Register a WebSocket route. The handler runs once per connection
    /// after the HTTP upgrade; the server closes the socket when it
    /// returns. Paths are always case-sensitive for WebSocket routes.
    #[harmony(args(path: String, handler: ApiWebSocketHandler, auth_mode: Option<ApiRouteAuthMode>))]
    pub(crate) async fn websocket(
        plugin_id: Option<Arc<str>>,
        path: String,
        handler: Function,
        auth_mode: Option<String>,
    ) -> mlua::Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        register_websocket_route(plugin_id, path, auth_mode, handler).await
    }

    #[harmony(path = "response.json", args(status: u16, body: JsonValue, headers: Option<ApiHeaders>), returns(ApiJsonResponse))]
    pub(crate) fn response_json_fn(
        lua: &Lua,
        args: (u16, Value, Option<Table>),
    ) -> mlua::Result<Table> {
        build_json_response(lua, args)
    }

    #[harmony(path = "response.empty", args(status: Option<u16>, headers: Option<ApiHeaders>), returns(ApiEmptyResponse))]
    pub(crate) fn response_empty_fn(
        lua: &Lua,
        args: (Option<u16>, Option<Table>),
    ) -> mlua::Result<Table> {
        build_empty_response(lua, args)
    }

    #[harmony(path = "response.text", args(status: u16, body: String, headers: Option<ApiHeaders>), returns(ApiTextResponse))]
    pub(crate) fn response_text_fn(
        lua: &Lua,
        args: (u16, String, Option<Table>),
    ) -> mlua::Result<Table> {
        build_text_response(lua, args)
    }

    /// Builds a file response with optional image transforms.
    #[harmony(path = "response.file", args(status: Option<u16>, path: String, headers: Option<ApiHeaders>, transform: Option<ImageTransformOptions>), returns(ApiFileResponse))]
    pub(crate) fn response_file_fn(
        lua: &Lua,
        args: (Option<u16>, String, Option<Table>, Option<Table>),
    ) -> mlua::Result<Table> {
        build_file_response(lua, args)
    }

    /// Builds a streamed-track response.
    #[harmony(path = "response.stream_track", args(track_id: i64, options: Option<TrackServeOptions>), returns(ApiStreamTrackResponse))]
    pub(crate) fn response_stream_track_fn(
        lua: &Lua,
        args: (i64, Option<Table>),
    ) -> mlua::Result<Table> {
        build_stream_track_response(lua, args)
    }

    /// Builds a download-track response.
    #[harmony(path = "response.download_track", args(track_id: i64, options: Option<TrackServeOptions>), returns(ApiDownloadTrackResponse))]
    pub(crate) fn response_download_track_fn(
        lua: &Lua,
        args: (i64, Option<Table>),
    ) -> mlua::Result<Table> {
        build_download_track_response(lua, args)
    }

    /// Builds an HLS playlist response for a track. Clients follow segment URLs from
    /// the returned M3U8 back to the native Lyra HLS endpoints, which are authorized
    /// via short-lived signed tokens.
    #[harmony(path = "response.hls_playlist", args(track_id: i64, options: Option<HlsServeOptions>), returns(ApiHlsPlaylistResponse))]
    pub(crate) fn response_hls_playlist_fn(
        lua: &Lua,
        args: (i64, Option<Table>),
    ) -> mlua::Result<Table> {
        build_hls_playlist_response(lua, args)
    }

    #[harmony(path = "query.bool", args(query: ApiQueryParams, key: String, default: Option<bool>))]
    pub(crate) fn query_bool_fn(
        lua: &Lua,
        args: (Table, String, Option<bool>),
    ) -> mlua::Result<Option<bool>> {
        query_bool_impl(lua, args)
    }

    #[harmony(path = "query.int", args(query: ApiQueryParams, key: String, default: Option<i64>, min: Option<i64>, max: Option<i64>))]
    pub(crate) fn query_int_fn(
        lua: &Lua,
        args: (Table, String, Option<i64>, Option<i64>, Option<i64>),
    ) -> mlua::Result<Option<i64>> {
        query_int_impl(lua, args)
    }

    /// Splits a comma-separated query parameter into an array.
    #[harmony(path = "query.csv", args(query: ApiQueryParams, key: String), returns(Vec<String>))]
    pub(crate) fn query_csv_fn(lua: &Lua, args: (Table, String)) -> mlua::Result<Table> {
        query_csv_impl(lua, args)
    }
}

pub(crate) fn get_module() -> Module {
    Module {
        path: "lyra/api".into(),
        setup: std::sync::Arc::new(|lua: &Lua| Ok(ApiModule::_harmony_module_table(lua)?)),
        scope: harmony_core::Scope {
            id: "lyra.api".into(),
            description: "Register HTTP routes on the Lyra server.",
            danger: harmony_core::Danger::High,
        },
    }
}

pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    ApiModule::render_luau_definition()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        STATE,
        db::{
            self,
            Permission,
            User,
            roles::Role,
        },
        services::auth::sessions,
        testing::{
            LibraryFixtureConfig,
            initialize_runtime,
            runtime_test_lock,
        },
    };
    use axum::body::to_bytes;
    use axum::http::header::{
        ACCEPT_RANGES,
        ACCESS_CONTROL_ALLOW_HEADERS,
        ACCESS_CONTROL_ALLOW_METHODS,
        ACCESS_CONTROL_ALLOW_ORIGIN,
        ACCESS_CONTROL_REQUEST_HEADERS,
        ACCESS_CONTROL_REQUEST_METHOD,
        CONTENT_LENGTH,
        CONTENT_RANGE,
        CONTENT_TYPE,
        HeaderValue,
        ORIGIN,
        RANGE,
    };
    use axum::http::{
        HeaderMap,
        Method,
        StatusCode,
    };
    use nanoid::nanoid;
    use registry::{
        API_ROUTE_REGISTRY,
        RouteAuthMode,
    };
    use response::{
        response_file,
        response_json,
        response_text,
    };
    use std::collections::HashSet;
    use std::time::{
        SystemTime,
        UNIX_EPOCH,
    };
    use tower::ServiceExt;
    use transport::lua_response_to_axum;

    async fn initialize_auth_test_runtime() -> anyhow::Result<std::path::PathBuf> {
        let test_dir = std::env::temp_dir().join(format!(
            "lyra-plugin-api-auth-test-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        std::fs::create_dir_all(&test_dir)?;
        initialize_runtime(&LibraryFixtureConfig {
            directory: test_dir.clone(),
            language: None,
            country: None,
        })
        .await?;
        Ok(test_dir)
    }

    async fn create_user_with_permissions(
        username: &str,
        permissions: Vec<Permission>,
    ) -> anyhow::Result<HeaderMap> {
        let user_db_id = {
            let mut db = STATE.db.write().await;
            db::roles::ensure_builtin_roles(&mut db)?;
            let user_db_id = db::users::create(
                &mut db,
                &User {
                    db_id: None,
                    id: nanoid!(),
                    username: username.to_string(),
                    password: "unused".to_string(),
                },
            )?;
            let role_name = if permissions.is_empty() {
                db::roles::BUILTIN_USER_ROLE.to_string()
            } else {
                let role_name = format!("plugin-api-test-{}", nanoid!());
                db::roles::create(
                    &mut db,
                    &Role {
                        db_id: None,
                        id: nanoid!(),
                        name: role_name.clone(),
                        permissions,
                    },
                )?;
                role_name
            };
            db::roles::ensure_user_has_role(&mut db, user_db_id, &role_name)?;
            user_db_id
        };

        let session = sessions::create_session_for_user(user_db_id).await?;
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {}", session.token)
                .parse()
                .expect("valid auth header"),
        );
        Ok(headers)
    }

    fn strip_body_for_head(
        method: &Method,
        response: axum::response::Response,
    ) -> axum::response::Response {
        use axum::body::Body;
        if *method != Method::HEAD {
            return response;
        }
        let (parts, _) = response.into_parts();
        axum::response::Response::from_parts(parts, Body::empty())
    }

    #[test]
    fn plugin_id_is_extracted_from_handler_source() -> anyhow::Result<()> {
        let lua = Lua::new();
        let handler: Function = lua
            .load("return function() return true end")
            .set_name(&harmony_core::format_plugin_chunk_name("demo", "init"))
            .eval()?;
        let plugin_id = crate::plugins::id_from_function(&handler)?;
        assert_eq!(plugin_id.as_ref(), "demo");
        Ok(())
    }

    #[test]
    fn normalize_api_method_accepts_known_verbs() -> anyhow::Result<()> {
        assert_eq!(normalize_api_method("get".to_string())?, "GET");
        assert_eq!(normalize_api_method("  patch ".to_string())?, "PATCH");
        Ok(())
    }

    #[test]
    fn normalize_api_method_rejects_unknown_verbs() {
        assert!(normalize_api_method("trace".to_string()).is_err());
    }

    #[tokio::test]
    async fn route_registers_absolute_root_path() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;

        initialize_registry(HashSet::new()).await;

        let lua = Lua::new();
        let api_table = (get_module().setup)(&lua)?;
        lua.globals().set("api", api_table)?;

        let register_fn = lua
            .load(
                r#"
                api.route("GET", "/System/Info/Public", function()
                    return api.response.json(200, { ok = true })
                end)
            "#,
            )
            .set_name(&harmony_core::format_plugin_chunk_name("demo", "init"))
            .into_function()?;
        register_fn.call_async::<()>(()).await?;

        let registry = API_ROUTE_REGISTRY.read().await;
        assert_eq!(registry.route_count(), 1);
        let route = registry
            .find(&RouteKey::new("GET", "/System/Info/Public")?)
            .ok_or_else(|| anyhow::anyhow!("registered route missing"))?;
        assert!(matches!(route.auth_mode, RouteAuthMode::Required));

        Ok(())
    }

    #[tokio::test]
    async fn route_helpers_register_and_reject_core_collisions() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;

        let mut core_routes = HashSet::new();
        core_routes.insert(RouteKey::new("GET", "/api/health")?);
        initialize_registry(core_routes).await;

        let lua = Lua::new();
        let api_table = (get_module().setup)(&lua)?;
        lua.globals().set("api", api_table)?;

        let register_fn = lua
            .load(
                r#"
                api.get("/api/demo", function(ctx)
                    return api.response.json(200, { ok = true, plugin = ctx.plugin_id })
                end)
            "#,
            )
            .set_name(&harmony_core::format_plugin_chunk_name("demo", "init"))
            .into_function()?;
        register_fn.call_async::<()>(()).await?;

        {
            let registry = API_ROUTE_REGISTRY.read().await;
            assert_eq!(registry.route_count(), 1);
        }

        let conflict_fn = lua
            .load(
                r#"
                api.get("/api/health", function()
                    return api.response.empty(200)
                end)
            "#,
            )
            .set_name(&harmony_core::format_plugin_chunk_name("demo", "init"))
            .into_function()?;

        let err = conflict_fn
            .call_async::<()>(())
            .await
            .expect_err("expected route conflict");
        assert!(err.to_string().contains("conflicts with core route"));

        Ok(())
    }

    #[tokio::test]
    async fn route_normalizes_method_case() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;

        initialize_registry(HashSet::new()).await;

        let lua = Lua::new();
        let api_table = (get_module().setup)(&lua)?;
        lua.globals().set("api", api_table)?;

        let register_fn = lua
            .load(
                r#"
                api.route("post", "/api/normalize-me", function()
                    return api.response.json(200, { ok = true })
                end, "public")
            "#,
            )
            .set_name(&harmony_core::format_plugin_chunk_name("demo", "init"))
            .into_function()?;

        register_fn.call_async::<()>(()).await?;

        let registry = API_ROUTE_REGISTRY.read().await;
        let route = registry
            .find(&RouteKey::new("POST", "/api/normalize-me")?)
            .ok_or_else(|| anyhow::anyhow!("registered route missing"))?;
        assert!(matches!(route.auth_mode, RouteAuthMode::Public));

        Ok(())
    }

    #[tokio::test]
    async fn route_rejects_invalid_auth_modes() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;

        initialize_registry(HashSet::new()).await;

        let lua = Lua::new();
        let api_table = (get_module().setup)(&lua)?;
        lua.globals().set("api", api_table)?;

        let register_fn = lua
            .load(
                r#"
                api.get("/api/demo", function()
                    return api.response.json(200, { ok = true })
                end, "private")
            "#,
            )
            .set_name(&harmony_core::format_plugin_chunk_name("demo", "init"))
            .into_function()?;

        let err = register_fn
            .call_async::<()>(())
            .await
            .expect_err("expected invalid auth mode rejection");
        assert!(err.to_string().contains("unsupported auth mode"));

        Ok(())
    }

    #[tokio::test]
    async fn route_case_insensitive_flag_lowercases_literal_segments() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;

        initialize_registry(HashSet::new()).await;

        let lua = Lua::new();
        let api_table = (get_module().setup)(&lua)?;
        lua.globals().set("api", api_table)?;

        let register_fn = lua
            .load(
                r#"
                api.get("/Users/{userId}/Items", function()
                    return api.response.json(200, { ok = true })
                end, nil, true)
            "#,
            )
            .set_name(&harmony_core::format_plugin_chunk_name("demo", "init"))
            .into_function()?;
        register_fn.call_async::<()>(()).await?;

        let registry = API_ROUTE_REGISTRY.read().await;
        let route = registry
            .find(&RouteKey::new("GET", "/users/{userId}/items")?)
            .ok_or_else(|| anyhow::anyhow!("registered route missing"))?;
        assert_eq!(route.key.path.as_ref(), "/users/{userId}/items");
        assert!(
            registry
                .find(&RouteKey::new("GET", "/Users/{userId}/Items")?)
                .is_none()
        );

        Ok(())
    }

    #[tokio::test]
    async fn build_router_allows_case_insensitive_siblings_sharing_a_path() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;

        initialize_registry(HashSet::new()).await;

        let lua = Lua::new();
        let api_table = (get_module().setup)(&lua)?;
        lua.globals().set("api", api_table)?;

        let register_fn = lua
            .load(
                r#"
                api.get("/Audio/{itemId}/stream", function()
                    return api.response.empty(200)
                end, nil, true)
                api.head("/Audio/{itemId}/stream", function()
                    return api.response.empty(200)
                end, nil, true)
            "#,
            )
            .set_name(&harmony_core::format_plugin_chunk_name("demo", "init"))
            .into_function()?;
        register_fn.call_async::<()>(()).await?;

        super::dispatch::build_registered_router().await?;

        Ok(())
    }

    #[tokio::test]
    async fn case_insensitive_route_dispatches_mixed_case_request() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;

        STATE.reset(crate::config::Config::default())?;
        initialize_registry(HashSet::new()).await;

        let lua = Lua::new();
        let api_table = (get_module().setup)(&lua)?;
        lua.globals().set("api", api_table)?;

        let register_fn = lua
            .load(
                r#"
                api.get("/Audio/{itemId}/stream", function(ctx)
                    return api.response.json(200, { itemId = ctx.params.itemId })
                end, "public", true)
            "#,
            )
            .set_name(&harmony_core::format_plugin_chunk_name("demo", "init"))
            .into_function()?;
        register_fn.call_async::<()>(()).await?;

        let (router, ci_router) = super::dispatch::build_registered_router().await?;
        super::dispatch::install_router(router, ci_router).await;

        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/AUDIO/ABC123/stream")
            .body(axum::body::Body::empty())?;
        let response = super::dispatch::fallback(request).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let json: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(json["itemId"], "ABC123");

        Ok(())
    }

    #[tokio::test]
    async fn cors_layer_handles_plugin_routes_without_lua_preflight() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;

        let mut config = crate::config::Config::default();
        config.cors.allowed_origins = vec!["http://localhost:8080".to_string()];
        STATE.reset(config.clone())?;
        initialize_registry(HashSet::new()).await;

        let lua = Lua::new();
        let api_table = (get_module().setup)(&lua)?;
        lua.globals().set("api", api_table)?;

        let register_fn = lua
            .load(
                r#"
                local count = 0

                api.get("/api/cors-demo", function()
                    count = count + 1
                    return api.response.json(200, { count = count })
                end, "public")

                api.get("/api/cors-error", function()
                    error("boom")
                end, "public")
            "#,
            )
            .set_name(&harmony_core::format_plugin_chunk_name("demo", "init"))
            .into_function()?;
        register_fn.call_async::<()>(()).await?;

        let (router, ci_router) = super::dispatch::build_registered_router().await?;
        super::dispatch::install_router(router, ci_router).await;

        let app = crate::services::cors::apply(
            axum::Router::new().fallback(super::dispatch::fallback),
            &config,
        );

        let preflight = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/api/cors-demo")
                    .header(ORIGIN, "http://localhost:8080")
                    .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .header(ACCESS_CONTROL_REQUEST_HEADERS, "authorization")
                    .body(axum::body::Body::empty())?,
            )
            .await?;
        assert_eq!(preflight.status(), StatusCode::OK);
        assert_eq!(
            preflight
                .headers()
                .get(ACCESS_CONTROL_ALLOW_ORIGIN)
                .and_then(|value| value.to_str().ok()),
            Some("http://localhost:8080")
        );
        assert_eq!(
            preflight
                .headers()
                .get(ACCESS_CONTROL_ALLOW_METHODS)
                .and_then(|value| value.to_str().ok()),
            Some("GET")
        );
        assert_eq!(
            preflight
                .headers()
                .get(ACCESS_CONTROL_ALLOW_HEADERS)
                .and_then(|value| value.to_str().ok()),
            Some("authorization")
        );

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::GET)
                    .uri("/api/cors-demo")
                    .header(ORIGIN, "http://localhost:8080")
                    .body(axum::body::Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(ACCESS_CONTROL_ALLOW_ORIGIN)
                .and_then(|value| value.to_str().ok()),
            Some("http://localhost:8080")
        );
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let json: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(json["count"], 1);

        let error_response = app
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::GET)
                    .uri("/api/cors-error")
                    .header(ORIGIN, "http://localhost:8080")
                    .body(axum::body::Body::empty())?,
            )
            .await?;
        assert_eq!(error_response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            error_response
                .headers()
                .get(ACCESS_CONTROL_ALLOW_ORIGIN)
                .and_then(|value| value.to_str().ok()),
            Some("http://localhost:8080")
        );

        Ok(())
    }

    #[tokio::test]
    async fn route_rejects_core_collisions_for_root_paths() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;

        let mut core_routes = HashSet::new();
        core_routes.insert(RouteKey::new("GET", "/System/Info/Public")?);
        initialize_registry(core_routes).await;

        let lua = Lua::new();
        let api_table = (get_module().setup)(&lua)?;
        lua.globals().set("api", api_table)?;

        let conflict_fn = lua
            .load(
                r#"
                api.get("/System/Info/Public", function()
                    return api.response.json(200, { ok = true })
                end)
            "#,
            )
            .set_name(&harmony_core::format_plugin_chunk_name("demo", "init"))
            .into_function()?;

        let err = conflict_fn
            .call_async::<()>(())
            .await
            .expect_err("expected root-path core route conflict");
        assert!(err.to_string().contains("conflicts with core route"));

        Ok(())
    }

    #[tokio::test]
    async fn route_rejects_registration_after_freeze() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;

        initialize_registry(HashSet::new()).await;

        let lua = Lua::new();
        let api_table = (get_module().setup)(&lua)?;
        lua.globals().set("api", api_table)?;

        let register_fn = lua
            .load(
                r#"
                api.get("/api/demo", function()
                    return api.response.json(200, { ok = true })
                end)
            "#,
            )
            .set_name(&harmony_core::format_plugin_chunk_name("demo", "init"))
            .into_function()?;
        register_fn.call_async::<()>(()).await?;

        freeze_registry().await;

        let after_freeze_fn = lua
            .load(
                r#"
                api.get("/api/demo_after_freeze", function()
                    return api.response.json(200, { ok = true })
                end)
            "#,
            )
            .set_name(&harmony_core::format_plugin_chunk_name("demo", "init"))
            .into_function()?;

        let err = after_freeze_fn
            .call_async::<()>(())
            .await
            .expect_err("expected frozen registry rejection");
        assert!(err.to_string().contains("route registry is frozen"));

        Ok(())
    }

    #[tokio::test]
    async fn route_registration_exemption_is_scoped_to_one_plugin() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;

        initialize_registry(HashSet::new()).await;

        let lua = Lua::new();
        let api_table = (get_module().setup)(&lua)?;
        lua.globals().set("api", api_table)?;

        freeze_registry().await;

        let demo_id = PluginId::new("demo")?;
        unfreeze_plugin_routes(demo_id.clone()).await;

        let exempt_fn = lua
            .load(
                r#"
                api.get("/api/demo_exempt", function()
                    return api.response.json(200, { ok = true })
                end)
            "#,
            )
            .set_name(&harmony_core::format_plugin_chunk_name("demo", "init"))
            .into_function()?;
        exempt_fn.call_async::<()>(()).await?;

        let other_fn = lua
            .load(
                r#"
                api.get("/api/other_blocked", function()
                    return api.response.json(200, { ok = true })
                end)
            "#,
            )
            .set_name(&harmony_core::format_plugin_chunk_name("other", "init"))
            .into_function()?;
        let err = other_fn
            .call_async::<()>(())
            .await
            .expect_err("non-exempt plugin must remain frozen");
        assert!(err.to_string().contains("frozen for plugin 'other'"));

        refreeze_plugin_routes(&demo_id).await;

        let refrozen_fn = lua
            .load(
                r#"
                api.get("/api/demo_refrozen", function()
                    return api.response.json(200, { ok = true })
                end)
            "#,
            )
            .set_name(&harmony_core::format_plugin_chunk_name("demo", "init"))
            .into_function()?;
        let err = refrozen_fn
            .call_async::<()>(())
            .await
            .expect_err("refrozen plugin must reject new routes again");
        assert!(err.to_string().contains("frozen for plugin 'demo'"));

        let registry = API_ROUTE_REGISTRY.read().await;
        assert_eq!(registry.route_count(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn lua_response_table_requires_kind() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;

        let lua = Lua::new();
        let value: Value = lua
            .load(
                r#"
                return {
                    status = 201,
                    body = {
                        ok = true,
                        answer = 42,
                    },
                }
            "#,
            )
            .eval()?;

        let err = lua_response_to_axum(&lua, value, &HeaderMap::new())
            .await
            .expect_err("expected response kind validation failure");
        assert!(err.to_string().contains("response table must include kind"));

        Ok(())
    }

    #[tokio::test]
    async fn lua_response_table_serializes_json_body() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;

        let lua = Lua::new();
        let body: Value = lua
            .load(
                r#"
                return {
                    ok = true,
                    answer = 42,
                }
            "#,
            )
            .eval()?;
        let table = response_json(&lua, (201, body, None))?;
        let response = lua_response_to_axum(&lua, Value::Table(table), &HeaderMap::new()).await?;
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("application/json")
        );

        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let payload: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(payload["ok"], serde_json::Value::Bool(true));
        assert_eq!(payload["answer"], serde_json::Value::Number(42.into()));

        Ok(())
    }

    #[tokio::test]
    async fn lua_response_json_omits_nil_valued_fields() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;

        let lua = Lua::new();
        let body: Value = lua
            .load(
                r#"
                local track = nil
                return {
                    Codec = "flac",
                    Channels = if track then track.channel_count else nil,
                    SampleRate = if track then track.sample_rate_hz else nil,
                    BitDepth = if track then track.bit_depth else nil,
                    IsDefault = true,
                }
            "#,
            )
            .eval()?;
        let table = response_json(&lua, (200, body, None))?;
        let response = lua_response_to_axum(&lua, Value::Table(table), &HeaderMap::new()).await?;
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let payload: serde_json::Value = serde_json::from_slice(&body)?;
        let object = payload.as_object().expect("json body should be an object");
        assert_eq!(object.get("Codec"), Some(&serde_json::json!("flac")));
        assert_eq!(object.get("IsDefault"), Some(&serde_json::json!(true)));
        assert!(
            !object.contains_key("Channels"),
            "nil Channels must be omitted from wire response"
        );
        assert!(
            !object.contains_key("SampleRate"),
            "nil SampleRate must be omitted from wire response"
        );
        assert!(
            !object.contains_key("BitDepth"),
            "nil BitDepth must be omitted from wire response"
        );

        Ok(())
    }

    #[tokio::test]
    async fn head_responses_strip_body_but_keep_headers() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;

        let lua = Lua::new();
        let headers: Table = lua
            .load(
                r#"
                return {
                    ["x-test"] = "ok",
                    ["content-type"] = "text/plain; charset=utf-8",
                }
            "#,
            )
            .eval()?;
        let table = response_text(&lua, (200, "hello world".to_string(), Some(headers)))?;
        let response = lua_response_to_axum(&lua, Value::Table(table), &HeaderMap::new()).await?;
        let response = strip_body_for_head(&Method::HEAD, response);
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("x-test")
                .and_then(|header| header.to_str().ok()),
            Some("ok")
        );
        assert_eq!(
            response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|header| header.to_str().ok()),
            Some("text/plain; charset=utf-8")
        );

        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert!(body.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn head_file_responses_keep_content_length_and_strip_body() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;

        let lua = Lua::new();
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path = std::env::temp_dir().join(format!(
            "lyra-api-head-file-{}-{}.bin",
            std::process::id(),
            nanos
        ));
        let payload = b"lyra-head-file-content";
        let expected_len = payload.len();
        tokio::fs::write(&path, payload)
            .await
            .with_context(|| format!("failed to write temp file '{}'", path.display()))?;

        let test_result: anyhow::Result<()> = async {
            let table = response_file(
                &lua,
                (Some(200), path.to_string_lossy().into_owned(), None, None),
            )?;
            let response =
                lua_response_to_axum(&lua, Value::Table(table), &HeaderMap::new()).await?;
            let content_length = response
                .headers()
                .get(CONTENT_LENGTH)
                .and_then(|header| header.to_str().ok())
                .map(str::to_string);
            let expected_len_header = expected_len.to_string();
            assert_eq!(
                content_length.as_deref(),
                Some(expected_len_header.as_str())
            );

            let response = strip_body_for_head(&Method::HEAD, response);
            assert_eq!(
                response
                    .headers()
                    .get(CONTENT_LENGTH)
                    .and_then(|header| header.to_str().ok()),
                Some(expected_len_header.as_str())
            );

            let body = to_bytes(response.into_body(), usize::MAX).await?;
            assert!(body.is_empty());
            Ok(())
        }
        .await;

        let _ = tokio::fs::remove_file(&path).await;
        test_result
    }

    #[tokio::test]
    async fn file_responses_support_single_byte_ranges() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;

        let lua = Lua::new();
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path = std::env::temp_dir().join(format!(
            "lyra-api-range-file-{}-{}.bin",
            std::process::id(),
            nanos
        ));
        let payload = b"0123456789abcdef";
        tokio::fs::write(&path, payload)
            .await
            .with_context(|| format!("failed to write temp file '{}'", path.display()))?;

        let test_result: anyhow::Result<()> = async {
            let table = response_file(
                &lua,
                (Some(200), path.to_string_lossy().into_owned(), None, None),
            )?;
            let mut request_headers = HeaderMap::new();
            request_headers.insert(RANGE, HeaderValue::from_static("bytes=2-5"));
            let response =
                lua_response_to_axum(&lua, Value::Table(table), &request_headers).await?;

            assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
            assert_eq!(
                response
                    .headers()
                    .get(CONTENT_LENGTH)
                    .and_then(|header| header.to_str().ok()),
                Some("4")
            );
            assert_eq!(
                response
                    .headers()
                    .get(CONTENT_RANGE)
                    .and_then(|header| header.to_str().ok()),
                Some("bytes 2-5/16")
            );
            assert_eq!(
                response
                    .headers()
                    .get(ACCEPT_RANGES)
                    .and_then(|header| header.to_str().ok()),
                Some("bytes")
            );

            let body = to_bytes(response.into_body(), usize::MAX).await?;
            assert_eq!(body.as_ref(), &payload[2..=5]);
            Ok(())
        }
        .await;

        let _ = tokio::fs::remove_file(&path).await;
        test_result
    }

    #[tokio::test]
    async fn file_responses_return_416_for_unsatisfiable_ranges() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;

        let lua = Lua::new();
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path = std::env::temp_dir().join(format!(
            "lyra-api-range-invalid-file-{}-{}.bin",
            std::process::id(),
            nanos
        ));
        let payload = b"0123456789abcdef";
        tokio::fs::write(&path, payload)
            .await
            .with_context(|| format!("failed to write temp file '{}'", path.display()))?;

        let test_result: anyhow::Result<()> = async {
            let table = response_file(
                &lua,
                (Some(200), path.to_string_lossy().into_owned(), None, None),
            )?;
            let mut request_headers = HeaderMap::new();
            request_headers.insert(RANGE, HeaderValue::from_static("bytes=99-120"));
            let response =
                lua_response_to_axum(&lua, Value::Table(table), &request_headers).await?;

            assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
            assert_eq!(
                response
                    .headers()
                    .get(CONTENT_RANGE)
                    .and_then(|header| header.to_str().ok()),
                Some("bytes */16")
            );
            assert_eq!(
                response
                    .headers()
                    .get(CONTENT_LENGTH)
                    .and_then(|header| header.to_str().ok()),
                Some("0")
            );
            assert_eq!(
                response
                    .headers()
                    .get(ACCEPT_RANGES)
                    .and_then(|header| header.to_str().ok()),
                Some("bytes")
            );

            let body = to_bytes(response.into_body(), usize::MAX).await?;
            assert!(body.is_empty());
            Ok(())
        }
        .await;

        let _ = tokio::fs::remove_file(&path).await;
        test_result
    }

    #[tokio::test]
    async fn head_file_responses_preserve_partial_headers() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;

        let lua = Lua::new();
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path = std::env::temp_dir().join(format!(
            "lyra-api-head-range-file-{}-{}.bin",
            std::process::id(),
            nanos
        ));
        let payload = b"0123456789abcdef";
        tokio::fs::write(&path, payload)
            .await
            .with_context(|| format!("failed to write temp file '{}'", path.display()))?;

        let test_result: anyhow::Result<()> = async {
            let table = response_file(
                &lua,
                (Some(200), path.to_string_lossy().into_owned(), None, None),
            )?;
            let mut request_headers = HeaderMap::new();
            request_headers.insert(RANGE, HeaderValue::from_static("bytes=4-9"));
            let response =
                lua_response_to_axum(&lua, Value::Table(table), &request_headers).await?;

            assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);

            let response = strip_body_for_head(&Method::HEAD, response);
            assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
            assert_eq!(
                response
                    .headers()
                    .get(CONTENT_LENGTH)
                    .and_then(|header| header.to_str().ok()),
                Some("6")
            );
            assert_eq!(
                response
                    .headers()
                    .get(CONTENT_RANGE)
                    .and_then(|header| header.to_str().ok()),
                Some("bytes 4-9/16")
            );
            assert_eq!(
                response
                    .headers()
                    .get(ACCEPT_RANGES)
                    .and_then(|header| header.to_str().ok()),
                Some("bytes")
            );

            let body = to_bytes(response.into_body(), usize::MAX).await?;
            assert!(body.is_empty());
            Ok(())
        }
        .await;

        let _ = tokio::fs::remove_file(&path).await;
        test_result
    }

    #[tokio::test]
    async fn stream_track_responses_require_download_permission() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_auth_test_runtime().await?;

        let lua = Lua::new();
        let table = response::response_stream_track(&lua, (1, None))?;
        let headers = create_user_with_permissions("listener", vec![]).await?;

        let response = lua_response_to_axum(&lua, Value::Table(table), &headers).await?;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test]
    async fn hls_playlist_responses_require_download_permission() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_auth_test_runtime().await?;

        let lua = Lua::new();
        let table = response::response_hls_playlist(&lua, (1, None))?;
        let headers = create_user_with_permissions("listener", vec![]).await?;

        let response = lua_response_to_axum(&lua, Value::Table(table), &headers).await?;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test]
    async fn hls_playlist_response_rejects_nonpositive_track_id() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;

        let lua = Lua::new();
        let err = response::response_hls_playlist(&lua, (0, None))
            .expect_err("track_id=0 must be rejected at response-build time");
        assert!(err.to_string().contains("positive id"));

        Ok(())
    }

    #[tokio::test]
    async fn hls_playlist_response_trims_and_filters_empty_codec() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;

        let lua = Lua::new();
        let empty_options = lua.create_table()?;
        empty_options.set("codec", "   ")?;
        let table = response::response_hls_playlist(&lua, (42, Some(empty_options)))?;
        assert_eq!(table.get::<String>("kind")?, "hls_playlist");
        assert_eq!(table.get::<i64>("track_id")?, 42);
        assert!(
            table.get::<Option<Table>>("options")?.is_none(),
            "empty-or-whitespace codec strings should be dropped so the native endpoint falls back to its default"
        );

        let options = lua.create_table()?;
        options.set("codec", " AAC ")?;
        options.set("bitrate_bps", 96_000)?;
        let table = response::response_hls_playlist(&lua, (42, Some(options)))?;
        let options = table
            .get::<Option<Table>>("options")?
            .expect("non-empty HLS options should be preserved");
        assert_eq!(
            options.get::<Option<String>>("codec")?.as_deref(),
            Some("AAC")
        );
        assert_eq!(options.get::<Option<u32>>("bitrate_bps")?, Some(96_000));

        Ok(())
    }

    #[tokio::test]
    async fn download_track_responses_require_authenticated_headers() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_auth_test_runtime().await?;

        let lua = Lua::new();
        let table = response::response_download_track(&lua, (1, None))?;

        let response = lua_response_to_axum(&lua, Value::Table(table), &HeaderMap::new()).await?;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    use anyhow::Context;
    use harmony_core::LuaFunctionAsyncExt;
}
