// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod responses;
#[cfg(test)]
mod stream_tests;
mod websocket;

use std::{
    collections::{
        HashMap,
        HashSet,
    },
    env,
    ffi::OsString,
    net::SocketAddr,
    path::PathBuf,
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
        ConnectInfo,
        DefaultBodyLimit,
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
use futures::future::BoxFuture;
use tower::ServiceExt;
use tower_http::services::ServeDir;

use self::responses::{
    header_pairs,
    plugin_api_response_to_axum,
};
use self::websocket::{
    WebSocketRouteRequest,
    dispatch_websocket_route,
};
use crate::plugins::api::{
    CaseInsensitiveRouter,
    RegisteredRoute,
    RouteAuthMode,
    RouteHost,
    RouteKey,
    RouteKind,
    initialize_registry,
    placeholder_offset,
    registered_routes,
};
use crate::routes::request::{
    base_url,
    bearer_credential,
};
use crate::routes::{
    serve_hls_playlist_for_track,
    track_response,
};
use crate::services::auth::resolve_optional_auth;
use crate::services::remote::constants::{
    AUTH_CHECK_INTERVAL,
    MAX_CONSECUTIVE_AUTH_ERRORS,
    MAX_MESSAGE_SIZE,
    PING_INTERVAL,
    PONG_TIMEOUT,
    WRITE_TIMEOUT,
};

static LIMITER_ROUTE_MATCHER: LazyLock<std::sync::RwLock<Arc<matchit::Router<()>>>> =
    LazyLock::new(|| std::sync::RwLock::new(Arc::new(matchit::Router::new())));
pub(super) static INSTALLED_ROUTERS: LazyLock<Arc<tokio::sync::RwLock<InstalledRouters>>> =
    LazyLock::new(|| Arc::new(tokio::sync::RwLock::new(InstalledRouters::empty())));
pub(super) static INSTALLED_STATIC_DIR: LazyLock<Arc<tokio::sync::RwLock<Option<PathBuf>>>> =
    LazyLock::new(|| Arc::new(tokio::sync::RwLock::new(None)));

pub(super) const LYRA_STATIC_DIR_ENV: &str = "LYRA_STATIC_DIR";

/// Serves registered plugin routes behind `app`'s own routes, with `app`'s
/// routes reserved against plugin registration.
pub(crate) async fn install(app: Router, reservations: HashSet<RouteKey>) -> Result<Router> {
    let static_dir = find_static_dir()?;
    if let Some(ref dir) = static_dir {
        tracing::info!(static_dir = %dir.display(), "serving static directory");
    }
    initialize_registry(reservations, Some(Arc::new(InstalledRoutes))).await;
    initialize_router(static_dir).await;
    Ok(app.fallback(fallback))
}

struct InstalledRoutes;

impl RouteHost for InstalledRoutes {
    fn rebuild(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            let built = build_router(installed_static_dir().await).await?;
            install_router(built).await;
            Ok(())
        })
    }
}

pub(super) struct BuiltRouters {
    main: Router,
    ci: CaseInsensitiveRouter,
    limiter_matcher: matchit::Router<()>,
}

#[derive(Clone)]
pub(super) struct InstalledRouters {
    main: Router,
    ci: Arc<CaseInsensitiveRouter>,
}

impl InstalledRouters {
    fn empty() -> Self {
        Self {
            main: Router::new(),
            ci: Arc::new(CaseInsensitiveRouter::new()),
        }
    }
}

pub(super) async fn api_not_found() -> (StatusCode, &'static str) {
    (StatusCode::NOT_FOUND, "Error: not found")
}

/// Whether the path is served by a registered plugin route, so the rate
/// limiter can distinguish plugin surfaces from static assets.
pub(crate) fn is_plugin_route_path(path: &str) -> bool {
    let matcher = LIMITER_ROUTE_MATCHER
        .read()
        .expect("limiter route matcher poisoned")
        .clone();
    matches_route_path(&matcher, path)
}

fn matches_route_path(matcher: &matchit::Router<()>, path: &str) -> bool {
    if matcher.at(path).is_ok() {
        return true;
    }
    has_ascii_upper(path) && matcher.at(&path.to_ascii_lowercase()).is_ok()
}

pub(super) fn apply_catchall_and_static(router: Router, static_dir: Option<PathBuf>) -> Router {
    let router = router
        .route("/api", any(api_not_found))
        .route("/api/{*path}", any(api_not_found));

    if let Some(static_dir) = static_dir {
        let static_router = Router::new()
            .fallback_service(ServeDir::new(static_dir))
            .layer(axum::middleware::map_response(
                |mut response: Response| async move {
                    if response.headers().get(CONTENT_TYPE).is_some_and(|value| {
                        value.to_str().is_ok_and(|value| {
                            value
                                .split(';')
                                .next()
                                .is_some_and(|mime| mime.trim() == "text/html")
                        })
                    }) {
                        response.headers_mut().insert(
                            axum::http::header::CACHE_CONTROL,
                            HeaderValue::from_static("no-cache"),
                        );
                    }
                    response
                },
            ));
        router.fallback_service(static_router)
    } else {
        router
    }
}

pub(super) async fn initialize_router(static_dir: Option<PathBuf>) {
    *INSTALLED_STATIC_DIR.write().await = static_dir.clone();
    let main = apply_catchall_and_static(Router::new(), static_dir);
    *INSTALLED_ROUTERS.write().await = InstalledRouters {
        main,
        ci: Arc::new(CaseInsensitiveRouter::new()),
    };
    *LIMITER_ROUTE_MATCHER
        .write()
        .expect("limiter route matcher poisoned") = Arc::new(matchit::Router::new());
}

pub(super) async fn installed_static_dir() -> Option<PathBuf> {
    INSTALLED_STATIC_DIR.read().await.clone()
}

pub(super) fn find_static_dir() -> Result<Option<PathBuf>> {
    if let Some(path) = configured_static_dir(env::var_os(LYRA_STATIC_DIR_ENV))? {
        return Ok(Some(path));
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let manifest_parent = manifest_dir
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| manifest_dir.clone());
    let candidates = [
        env::current_dir().ok().map(|cwd| cwd.join("static")),
        env::current_exe().ok().and_then(|exe| {
            let exe_dir = exe.parent()?;
            Some(exe_dir.join("static"))
        }),
        env::current_exe().ok().and_then(|exe| {
            let exe_dir = exe.parent()?;
            Some(exe_dir.join("..").join("static"))
        }),
        Some(manifest_parent.join("static")),
        Some(manifest_dir.join("static")),
    ];

    Ok(candidates.into_iter().flatten().find(|path| path.is_dir()))
}

fn configured_static_dir(value: Option<OsString>) -> Result<Option<PathBuf>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let path = PathBuf::from(value);
    if path.as_os_str().is_empty() {
        return Ok(None);
    }
    if !path.is_dir() {
        bail!(
            "{LYRA_STATIC_DIR_ENV} points to '{}' but it is not a directory",
            path.display()
        );
    }
    Ok(Some(path))
}

pub(super) async fn install_router(built: BuiltRouters) {
    *INSTALLED_ROUTERS.write().await = InstalledRouters {
        main: built.main,
        ci: Arc::new(built.ci),
    };
    *LIMITER_ROUTE_MATCHER
        .write()
        .expect("limiter route matcher poisoned") = Arc::new(built.limiter_matcher);
}

pub(super) async fn build_router(static_dir: Option<PathBuf>) -> Result<BuiltRouters> {
    let mut built = build_registered_router().await?;
    built.main = apply_catchall_and_static(built.main, static_dir);
    Ok(built)
}

pub(super) async fn fallback(mut request: Request) -> Response {
    let routers = { INSTALLED_ROUTERS.read().await.clone() };

    let original_path = request.uri().path();
    if has_ascii_upper(original_path) {
        let lowered = original_path.to_ascii_lowercase();
        if let Some(pattern) = routers.ci.find_pattern(&lowered) {
            let new_path = rewrite_path_for_ci(&pattern, original_path);
            match rebuild_uri_with_path(request.uri(), &new_path) {
                Some(new_uri) => *request.uri_mut() = new_uri,
                None => tracing::warn!(
                    pattern = %pattern,
                    incoming = %original_path,
                    "failed to rebuild URI for case-insensitive rewrite; dispatching original"
                ),
            }
        }
    }

    match routers.main.oneshot(request).await {
        Ok(response) => response,
        Err(err) => match err {},
    }
}

fn has_ascii_upper(path: &str) -> bool {
    path.bytes().any(|byte| byte.is_ascii_uppercase())
}

fn rewrite_path_for_ci(pattern: &str, incoming: &str) -> String {
    let mut out: Vec<String> = Vec::new();
    let mut incoming_iter = incoming.split('/');
    for pattern_seg in pattern.split('/') {
        if pattern_seg.starts_with("{*") {
            for seg in incoming_iter.by_ref() {
                out.push(seg.to_string());
            }
            break;
        }
        let Some(seg) = incoming_iter.next() else {
            continue;
        };
        match placeholder_offset(pattern_seg).and_then(|offset| seg.split_at_checked(offset)) {
            Some((literal, value)) => out.push(format!("{}{value}", literal.to_ascii_lowercase())),
            None => out.push(seg.to_ascii_lowercase()),
        }
    }
    out.join("/")
}

fn rebuild_uri_with_path(uri: &Uri, new_path: &str) -> Option<Uri> {
    let mut parts = uri.clone().into_parts();
    let rebuilt = match parts.path_and_query.as_ref().and_then(|pq| pq.query()) {
        Some(query) => format!("{new_path}?{query}"),
        None => new_path.to_string(),
    };
    parts.path_and_query = Some(rebuilt.parse().ok()?);
    Uri::from_parts(parts).ok()
}

async fn build_registered_router() -> Result<BuiltRouters> {
    let (routes, ci_router) = registered_routes().await;

    let mut probe: matchit::Router<String> = matchit::Router::new();
    let mut limiter_matcher: matchit::Router<()> = matchit::Router::new();
    let mut plugin_by_path: HashMap<String, String> = HashMap::new();
    for (reserved, owner) in [("/api", "core"), ("/api/{*path}", "core")] {
        probe
            .insert(reserved.to_string(), owner.to_string())
            .with_context(|| format!("seeding reserved pattern '{reserved}'"))?;
        plugin_by_path.insert(reserved.to_string(), owner.to_string());
    }
    let mut seen_paths: HashSet<&str> = HashSet::new();
    for route in &routes {
        let path = route.key.path.as_ref();
        if !seen_paths.insert(path) {
            continue;
        }
        let owner = route.plugin_id.as_str().to_string();
        if let Err(err) = probe.insert(path.to_string(), owner.clone()) {
            return Err(describe_probe_conflict(
                &route.plugin_id,
                path,
                err,
                &plugin_by_path,
            ));
        }
        limiter_matcher
            .insert(path.to_string(), ())
            .with_context(|| format!("seeding limiter matcher for '{path}'"))?;
        plugin_by_path.insert(path.to_string(), owner);
    }

    let mut router = Router::new();
    for route in &routes {
        let path = route.key.path.clone();
        let method_router = match route.kind {
            RouteKind::Http => build_http_method_router(route)?,
            RouteKind::WebSocket => build_websocket_method_router(route),
        };
        router = router.route(path.as_ref(), method_router);
    }
    Ok(BuiltRouters {
        main: router,
        ci: ci_router,
        limiter_matcher,
    })
}

fn describe_probe_conflict(
    plugin_id: &crate::services::plugin_scope::PluginId,
    path: &str,
    err: matchit::InsertError,
    plugin_by_path: &HashMap<String, String>,
) -> anyhow::Error {
    if let matchit::InsertError::Conflict { with } = &err {
        let owner = plugin_by_path
            .get(with)
            .map(String::as_str)
            .unwrap_or("unknown");
        return anyhow::anyhow!(
            "plugin '{plugin_id}' route '{path}' conflicts with '{with}' registered by '{owner}'"
        );
    }
    anyhow::anyhow!("plugin '{plugin_id}' route '{path}': {err}")
}

fn build_http_method_router(route: &RegisteredRoute) -> Result<MethodRouter> {
    let method_filter = method_filter_for(&route.key.method)?;
    let dispatch_route = route.clone();
    let router = on(
        method_filter,
        move |method: Method,
              uri: Uri,
              headers: HeaderMap,
              Query(pairs): Query<Vec<(String, String)>>,
              params: Option<Path<HashMap<String, String>>>,
              client: Option<axum::Extension<crate::services::auth::caller::ClientAddress>>,
              peer: Option<axum::Extension<ConnectInfo<SocketAddr>>>,
              body: Bytes| {
            let route = dispatch_route.clone();
            async move {
                let query = collect_query_pairs(pairs);
                let client = client
                    .map(|axum::Extension(client)| client)
                    .unwrap_or_default();
                let peer = peer.map(|axum::Extension(ConnectInfo(peer))| peer);
                let base_url = base_url(peer, &headers, &uri);
                dispatch_registered_route(RegisteredRouteRequest {
                    route,
                    method,
                    uri,
                    headers,
                    base_url,
                    query,
                    params,
                    client,
                    body,
                })
                .await
            }
        },
    );
    // The `Bytes` extractor answers a body past the innermost limit with 413, so this replaces
    // the server's limit for this route alone.
    Ok(match route.max_body_bytes {
        Some(limit) => router.layer(DefaultBodyLimit::max(limit)),
        None => router,
    })
}

fn build_websocket_method_router(route: &RegisteredRoute) -> MethodRouter {
    let dispatch_route = route.clone();
    get(
        move |uri: Uri,
              headers: HeaderMap,
              Query(pairs): Query<Vec<(String, String)>>,
              params: Option<Path<HashMap<String, String>>>,
              ws: WebSocketUpgrade| {
            let route = dispatch_route.clone();
            async move {
                let query = collect_query_pairs(pairs);
                dispatch_websocket_route(
                    WebSocketRouteRequest {
                        route,
                        uri,
                        headers,
                        query,
                        params: params.map(|p| p.0),
                    },
                    ws,
                )
                .await
            }
        },
    )
}

fn collect_query_pairs(pairs: Vec<(String, String)>) -> HashMap<String, Vec<String>> {
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for (key, value) in pairs {
        map.entry(key).or_default().push(value);
    }
    map
}

fn method_filter_for(method: &str) -> Result<MethodFilter> {
    let filter = match method {
        "GET" => MethodFilter::GET,
        "POST" => MethodFilter::POST,
        "PUT" => MethodFilter::PUT,
        "PATCH" => MethodFilter::PATCH,
        "DELETE" => MethodFilter::DELETE,
        "HEAD" => MethodFilter::HEAD,
        "OPTIONS" => MethodFilter::OPTIONS,
        _ => bail!("unsupported method '{}'", method),
    };
    Ok(filter)
}

struct RegisteredRouteRequest {
    route: RegisteredRoute,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    base_url: Option<String>,
    query: HashMap<String, Vec<String>>,
    params: Option<Path<HashMap<String, String>>>,
    client: crate::services::auth::caller::ClientAddress,
    body: Bytes,
}

async fn dispatch_registered_route(request: RegisteredRouteRequest) -> Response {
    let RegisteredRouteRequest {
        route,
        method,
        uri,
        headers,
        base_url,
        query,
        params,
        client,
        body,
    } = request;

    let auth = match resolve_optional_auth(bearer_credential(&headers)).await {
        Ok(auth) => auth,
        Err(err) => {
            tracing::error!(
                plugin_id = %route.plugin_id,
                method = %route.key.method,
                path = %route.key.path,
                error = %err,
                "failed to resolve auth for plugin route"
            );
            return (StatusCode::INTERNAL_SERVER_ERROR, "internal server error").into_response();
        }
    };
    if matches!(route.auth_mode, RouteAuthMode::Required) && auth.is_none() {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }

    let Some(runtime) = crate::plugins::STATE.generation().runtime.get() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "plugin runtime is not ready",
        )
            .into_response();
    };

    let request_headers = headers.clone();
    let request = crate::plugins::executor::ApiHandlerRequest {
        handler: route.handler,
        plugin_id: route.plugin_id.to_string(),
        method: method.as_str().to_string(),
        path: uri.path().to_string(),
        headers: header_pairs(&headers),
        base_url,
        query,
        params: params.map(|params| params.0).unwrap_or_default(),
        body: body.to_vec(),
        auth: auth.clone(),
        client,
    };

    let result = runtime.dispatch_api_handler(request).await;

    match result {
        Ok(response) => {
            match plugin_api_response_to_axum(response, &method, &request_headers).await {
                Ok(response) => response,
                Err(err) => {
                    tracing::error!(
                        plugin_id = %route.plugin_id,
                        method = %route.key.method,
                        path = %route.key.path,
                        error = %err,
                        "plugin route returned invalid response"
                    );
                    (StatusCode::INTERNAL_SERVER_ERROR, "internal server error").into_response()
                }
            }
        }
        Err(err) => {
            tracing::error!(
                plugin_id = %route.plugin_id,
                method = %route.key.method,
                path = %route.key.path,
                error = %err,
                "plugin route handler failed"
            );
            (StatusCode::INTERNAL_SERVER_ERROR, "internal server error").into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn only_static_html_requires_cache_revalidation() {
        use axum::http::header::CACHE_CONTROL;

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "frontend").unwrap();
        std::fs::create_dir(dir.path().join("nested")).unwrap();
        std::fs::write(dir.path().join("nested/index.html"), "nested").unwrap();
        std::fs::write(dir.path().join("app.js"), "export {};").unwrap();
        let routes = Router::new().route(
            "/plugin",
            get(|| async {
                (
                    [(CACHE_CONTROL, "private, max-age=60")],
                    axum::response::Html("plugin"),
                )
            }),
        );
        let router = apply_catchall_and_static(routes, Some(dir.path().to_path_buf()));

        for (path, policy) in [
            ("/", Some("no-cache")),
            ("/index.html?version=1", Some("no-cache")),
            ("/nested/", Some("no-cache")),
            ("/app.js", None),
            ("/missing.html", None),
            ("/api/missing", None),
            ("/plugin", Some("private, max-age=60")),
        ] {
            for method in [Method::GET, Method::HEAD] {
                let response = router
                    .clone()
                    .oneshot(
                        Request::builder()
                            .method(method)
                            .uri(path)
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    response
                        .headers()
                        .get(CACHE_CONTROL)
                        .map(|value| value.to_str().unwrap()),
                    policy,
                    "{path}"
                );
            }
        }
    }

    #[tokio::test]
    async fn static_files_serve_indexes_without_masking_missing_paths() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "frontend").unwrap();
        std::fs::write(dir.path().join("app.js"), "export {};").unwrap();
        let router = apply_catchall_and_static(Router::new(), Some(dir.path().to_path_buf()));

        for (path, status, body) in [
            ("/", StatusCode::OK, "frontend"),
            ("/index.html", StatusCode::OK, "frontend"),
            ("/app.js", StatusCode::OK, "export {};"),
            ("/missing.js", StatusCode::NOT_FOUND, ""),
            ("/missing", StatusCode::NOT_FOUND, ""),
            ("/api", StatusCode::NOT_FOUND, "Error: not found"),
            ("/api/missing", StatusCode::NOT_FOUND, "Error: not found"),
        ] {
            let response = router
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), status, "{path}");
            let actual = axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap();
            assert_eq!(actual.as_ref(), body.as_bytes(), "{path}");
        }
    }

    #[tokio::test]
    async fn registered_routes_take_precedence_over_static_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "frontend").unwrap();
        std::fs::write(dir.path().join("plugin"), "static file").unwrap();
        let routes = Router::new()
            .route("/", get(|| async { "plugin root" }))
            .route(
                "/plugin",
                get(|| async { (StatusCode::NOT_FOUND, "plugin missing") }),
            );
        let router = apply_catchall_and_static(routes, Some(dir.path().to_path_buf()));

        for (path, status, body) in [
            ("/", StatusCode::OK, "plugin root"),
            ("/plugin", StatusCode::NOT_FOUND, "plugin missing"),
        ] {
            let response = router
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), status, "{path}");
            let actual = axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap();
            assert_eq!(actual.as_ref(), body.as_bytes(), "{path}");
        }
    }

    fn http_route(path: &str, max_body_bytes: Option<usize>) -> RegisteredRoute {
        RegisteredRoute {
            key: RouteKey::new("POST", path).unwrap(),
            plugin_id: crate::services::plugin_scope::PluginId::new("limits").unwrap(),
            auth_mode: RouteAuthMode::Required,
            case_insensitive: false,
            max_body_bytes,
            handler: crate::services::plugin_dispatch::PluginCallbackHandle {
                vm_id: 0,
                handler_id: 0,
            },
            kind: RouteKind::Http,
        }
    }

    #[tokio::test]
    async fn a_route_body_limit_replaces_the_server_limit_for_that_route_alone() {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state().unwrap();
        let server_limit = 1024;
        let mut routes = Router::new();
        for route in [
            http_route("/default", None),
            http_route("/large", Some(4 * server_limit)),
            http_route("/small", Some(16)),
        ] {
            routes = routes.route(
                route.key.path.as_ref(),
                build_http_method_router(&route).unwrap(),
            );
        }
        let app = Router::new()
            .fallback_service(routes)
            .layer(DefaultBodyLimit::max(server_limit));

        // A body the route accepts reaches the handler, which refuses the anonymous caller.
        for (path, size, status) in [
            ("/default", server_limit, StatusCode::UNAUTHORIZED),
            ("/default", server_limit + 1, StatusCode::PAYLOAD_TOO_LARGE),
            ("/large", 4 * server_limit, StatusCode::UNAUTHORIZED),
            (
                "/large",
                4 * server_limit + 1,
                StatusCode::PAYLOAD_TOO_LARGE,
            ),
            ("/small", 16, StatusCode::UNAUTHORIZED),
            ("/small", 17, StatusCode::PAYLOAD_TOO_LARGE),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::POST)
                        .uri(path)
                        .body(Body::from(vec![b'x'; size]))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), status, "{path} with {size} bytes");
        }
    }

    #[test]
    fn matches_registered_paths_with_case_insensitive_lowering() {
        let mut matcher: matchit::Router<()> = matchit::Router::new();
        matcher
            .insert("/demo/users/{id}", ())
            .expect("pattern should insert");

        assert!(matches_route_path(&matcher, "/demo/users/abc"));
        assert!(matches_route_path(&matcher, "/Demo/Users/abc"));
        assert!(!matches_route_path(&matcher, "/assets/app.js"));
        assert!(!matches_route_path(&matcher, "/"));
    }

    #[test]
    fn case_insensitive_rewrite_keeps_placeholder_values() {
        assert_eq!(
            rewrite_path_for_ci(
                "/audio/{itemId}/stream.{container}",
                "/Audio/AbC/Stream.MP3"
            ),
            "/audio/AbC/stream.MP3"
        );
        assert_eq!(
            rewrite_path_for_ci("/files/{*rest}", "/Files/A/B"),
            "/files/A/B"
        );
    }
}
