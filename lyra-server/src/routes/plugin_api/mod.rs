// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod body_budget;
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
    time::Duration,
};

use anyhow::{
    Context,
    Result,
    bail,
};
use axum::{
    Router,
    body::Body,
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

use crate::services::plugin_scope::PluginId;
use tower_http::services::ServeDir;

use self::body_budget::PublicBodyBudget;
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
};
use crate::routes::app::REQUEST_BODY_LIMIT_BYTES;
use crate::routes::request::{
    ConnectionScheme,
    UnreadBody,
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
    fn build(
        &self,
        routes: Vec<RegisteredRoute>,
        case_insensitive: CaseInsensitiveRouter,
    ) -> BoxFuture<'_, Result<crate::plugins::api::InstallRoutes>> {
        Box::pin(async move {
            let built =
                build_router(routes, case_insensitive, installed_static_dir().await).await?;
            let install: crate::plugins::api::InstallRoutes =
                Box::new(move || Box::pin(install_router(built)));
            Ok(install)
        })
    }
}

pub(super) struct BuiltRouters {
    main: Router,
    ci: CaseInsensitiveRouter,
    limiter_matcher: matchit::Router<()>,
    owners: matchit::Router<RouteOwners>,
}

/// Each method a path serves and the plugin serving it.
type RouteOwners = HashMap<String, Vec<PluginId>>;

#[derive(Clone)]
pub(super) struct InstalledRouters {
    main: Router,
    ci: Arc<CaseInsensitiveRouter>,
    /// The plugins serving each route, so a request can wait out a transition of theirs.
    owners: Arc<matchit::Router<RouteOwners>>,
}

impl InstalledRouters {
    /// The plugins serving `method` at `path`; `HEAD` is served by a `GET` route.
    fn owners_of(&self, method: &Method, path: &str) -> Option<&Vec<PluginId>> {
        let methods = self.owners.at(path).ok()?.value;
        methods.get(method.as_str()).or_else(|| {
            (method == Method::HEAD)
                .then(|| methods.get(Method::GET.as_str()))
                .flatten()
        })
    }

    fn empty() -> Self {
        Self {
            main: Router::new(),
            ci: Arc::new(CaseInsensitiveRouter::new()),
            owners: Arc::new(matchit::Router::new()),
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
        owners: Arc::new(matchit::Router::new()),
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
        owners: Arc::new(built.owners),
    };
    *LIMITER_ROUTE_MATCHER
        .write()
        .expect("limiter route matcher poisoned") = Arc::new(built.limiter_matcher);
}

pub(super) async fn build_router(
    routes: Vec<RegisteredRoute>,
    case_insensitive: CaseInsensitiveRouter,
    static_dir: Option<PathBuf>,
) -> Result<BuiltRouters> {
    let mut built = build_registered_router(routes, case_insensitive)?;
    built.main = apply_catchall_and_static(built.main, static_dir);
    Ok(built)
}

/// How long a request waits for a transition covering its route's plugin.
const ROUTE_TRANSITION_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

pub(super) async fn fallback(mut request: Request) -> Response {
    // Waited out before routing, so the request reaches the routes installed with whatever
    // replaced the plugin. The routers it picks from may differ from these, so the URI stays as
    // sent until the final routers rewrite it.
    if !wait_for_route_owners(request.method().clone(), request.uri().clone()).await {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "plugin runtime is not ready",
        )
            .into_response();
    }
    let routers = { INSTALLED_ROUTERS.read().await.clone() };
    if let Some(new_uri) = case_insensitive_uri(&routers, request.uri()) {
        *request.uri_mut() = new_uri;
    }
    match routers.main.oneshot(request).await {
        Ok(response) => response,
        Err(err) => match err {},
    }
}

/// Waits for any transition covering the plugins serving `method` at `uri`. False when one
/// outlasted the wait. A path no plugin serves yet deliberately doesn't wait: during a reload it
/// answers 404, even when the next run is about to add a route for it.
async fn wait_for_route_owners(method: Method, uri: Uri) -> bool {
    let routers = { INSTALLED_ROUTERS.read().await.clone() };
    let lookup_uri = case_insensitive_uri(&routers, &uri);
    let path = lookup_uri.as_ref().unwrap_or(&uri).path();
    let Some(owners) = routers.owners_of(&method, path) else {
        return true;
    };
    let wait = crate::services::plugin_dispatch::TransitionWait::Until(
        tokio::time::Instant::now() + ROUTE_TRANSITION_WAIT,
    );
    crate::services::STATE
        .generation()
        .plugins_settled(owners, &wait)
        .await
        .is_ok()
}

/// `uri` as a case-insensitive route of `routers` matches it, when one does and it differs.
fn case_insensitive_uri(routers: &InstalledRouters, uri: &Uri) -> Option<Uri> {
    let original_path = uri.path();
    if !has_ascii_upper(original_path) {
        return None;
    }
    let lowered = original_path.to_ascii_lowercase();
    let pattern = routers.ci.find_pattern(&lowered)?;
    let new_path = rewrite_path_for_ci(&pattern, original_path);
    let rebuilt = rebuild_uri_with_path(uri, &new_path);
    if rebuilt.is_none() {
        tracing::warn!(
            pattern = %pattern,
            incoming = %original_path,
            "failed to rebuild URI for case-insensitive rewrite; dispatching original"
        );
    }
    rebuilt
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

fn build_registered_router(
    routes: Vec<RegisteredRoute>,
    ci_router: CaseInsensitiveRouter,
) -> Result<BuiltRouters> {
    let mut probe: matchit::Router<String> = matchit::Router::new();
    let mut limiter_matcher: matchit::Router<()> = matchit::Router::new();
    let mut owners: matchit::Router<RouteOwners> = matchit::Router::new();
    let mut owners_by_path: HashMap<&str, RouteOwners> = HashMap::new();
    for route in &routes {
        let plugins = owners_by_path
            .entry(route.key.path.as_ref())
            .or_default()
            .entry(route.key.method.to_string())
            .or_default();
        if !plugins.contains(&route.plugin_id) {
            plugins.push(route.plugin_id.clone());
        }
    }
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
        owners
            .insert(
                path.to_string(),
                owners_by_path.get(path).cloned().unwrap_or_default(),
            )
            .with_context(|| format!("seeding route owners for '{path}'"))?;
        plugin_by_path.insert(path.to_string(), owner);
    }

    let mut router = Router::new();
    for route in &routes {
        let path = route.key.path.clone();
        let method_router = match route.kind {
            RouteKind::Http => build_http_method_router(route, PublicBodyBudget::server())?,
            RouteKind::WebSocket => build_websocket_method_router(route),
        };
        router = router.route(path.as_ref(), method_router);
    }
    Ok(BuiltRouters {
        main: router,
        ci: ci_router,
        limiter_matcher,
        owners,
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

fn build_http_method_router(
    route: &RegisteredRoute,
    body_budget: PublicBodyBudget,
) -> Result<MethodRouter> {
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
              connection: Option<axum::Extension<ConnectionScheme>>,
              body: UnreadBody| {
            let route = dispatch_route.clone();
            let body_budget = body_budget.clone();
            async move {
                let query = collect_query_pairs(pairs);
                let client = client
                    .map(|axum::Extension(client)| client)
                    .unwrap_or_default();
                let peer = peer.map(|axum::Extension(ConnectInfo(peer))| peer);
                let connection = connection
                    .map(|axum::Extension(connection)| connection)
                    .unwrap_or_default();
                let base_url = base_url(peer, connection, &headers, &uri);
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
                    body_budget,
                })
                .await
            }
        },
    );
    // Reading the body answers one past the innermost limit with 413, so this replaces the
    // server's limit for this route alone.
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

/// How soon an upload refused for its share of the public body budget is worth retrying.
const BODY_BUDGET_RETRY_AFTER: Duration = Duration::from_secs(5);

struct RegisteredRouteRequest {
    route: RegisteredRoute,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    base_url: Option<String>,
    query: HashMap<String, Vec<String>>,
    params: Option<Path<HashMap<String, String>>>,
    client: crate::services::auth::caller::ClientAddress,
    body: UnreadBody,
    body_budget: PublicBodyBudget,
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
        body_budget,
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

    let raised_limit = route
        .max_body_bytes
        .filter(|limit| *limit > REQUEST_BODY_LIMIT_BYTES);
    let _reservation = match (route.auth_mode, raised_limit) {
        (RouteAuthMode::Public, Some(bytes)) => {
            let Some(reservation) = body_budget.reserve(client, bytes) else {
                return super::rate_limit::too_many_requests_response(BODY_BUDGET_RETRY_AFTER);
            };
            Some(reservation)
        }
        _ => None,
    };
    let body = match body.read().await {
        Ok(body) => body,
        Err(rejection) => return rejection.into_response(),
    };

    let Some(runtime) = crate::plugins::STATE.runtime() else {
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
        // The route belongs to a replaced runtime, or a plugin being restarted: nothing ran.
        Err(err) if crate::services::plugin_dispatch::DispatchNotStarted::caused(&err) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "plugin runtime is not ready",
        )
            .into_response(),
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

/// Installs routes the way the server does, and serves them through what fronts them.
#[cfg(test)]
pub(crate) async fn use_installed_routes() {
    initialize_registry(HashSet::new(), Some(Arc::new(InstalledRoutes))).await;
    initialize_router(None).await;
}

/// The response to one anonymous `GET` of `path`, through the installed routes.
#[cfg(test)]
pub(crate) async fn get_installed(path: &str) -> StatusCode {
    fallback(
        Request::builder()
            .method("GET")
            .uri(path)
            .body(axum::body::Body::empty())
            .expect("request"),
    )
    .await
    .status()
}

/// Sends one anonymous request through `route`'s handler, as an installed route would.
#[cfg(test)]
pub(crate) async fn call_route(route: &RegisteredRoute) -> StatusCode {
    use tower::ServiceExt;

    let router = Router::new().route(
        route.key.path.as_ref(),
        build_http_method_router(route, PublicBodyBudget::server()).expect("route"),
    );
    router
        .oneshot(
            Request::builder()
                .method(route.key.method.as_ref())
                .uri(route.key.path.as_ref())
                .body(axum::body::Body::empty())
                .expect("request"),
        )
        .await
        .expect("infallible")
        .status()
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

    #[tokio::test(start_paused = true)]
    async fn a_request_gets_503_when_the_transition_outlasts_its_wait() {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state().unwrap();
        use_installed_routes().await;
        let route = RegisteredRoute {
            key: RouteKey::new("GET", "/held").unwrap(),
            plugin_id: PluginId::new("held").unwrap(),
            // Past the wait this route answers 401 for an anonymous caller, unlike the wait's 503.
            auth_mode: RouteAuthMode::Required,
            case_insensitive: false,
            max_body_bytes: None,
            handler: crate::services::plugin_dispatch::PluginCallbackHandle {
                vm_id: 0,
                handler_id: 0,
            },
            kind: RouteKind::Http,
        };
        install_router(
            build_router(vec![route], CaseInsensitiveRouter::new(), None)
                .await
                .unwrap(),
        )
        .await;
        let generation = crate::services::STATE.generation();
        let _transition = generation.begin_transition(crate::services::state::Transition::Plugin {
            plugin: PluginId::new("held").unwrap(),
            targets: Default::default(),
        });

        let started = tokio::time::Instant::now();
        let status = get_installed("/held").await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(started.elapsed(), ROUTE_TRANSITION_WAIT);
    }

    fn route_on(owner: &str, method: &str, path: &str, case_insensitive: bool) -> RegisteredRoute {
        RegisteredRoute {
            key: if case_insensitive {
                RouteKey::new_case_insensitive(method, path).unwrap()
            } else {
                RouteKey::new(method, path).unwrap()
            },
            plugin_id: PluginId::new(owner).unwrap(),
            auth_mode: RouteAuthMode::Public,
            case_insensitive,
            max_body_bytes: None,
            handler: crate::services::plugin_dispatch::PluginCallbackHandle {
                vm_id: 0,
                handler_id: 0,
            },
            kind: RouteKind::Http,
        }
    }

    async fn install_routes(routes: Vec<RegisteredRoute>, ci_paths: &[&str]) {
        let mut ci = CaseInsensitiveRouter::new();
        for path in ci_paths {
            ci.insert(path).unwrap();
        }
        install_router(build_router(routes, ci, None).await.unwrap()).await;
    }

    async fn request(method: &str, path: &str) -> StatusCode {
        fallback(
            Request::builder()
                .method(method)
                .uri(path)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .status()
    }

    async fn route_test_state() -> crate::services::testing::RuntimeTestGuard {
        let guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state().unwrap();
        use_installed_routes().await;
        guard
    }

    #[tokio::test(start_paused = true)]
    async fn a_request_waits_only_on_the_plugin_serving_its_method() {
        let _guard = route_test_state().await;
        install_routes(
            vec![
                route_on("restarting", "GET", "/shared", false),
                route_on("other", "POST", "/shared", false),
            ],
            &[],
        )
        .await;
        let generation = crate::services::STATE.generation();
        let _transition = generation.begin_transition(crate::services::state::Transition::Plugin {
            plugin: PluginId::new("restarting").unwrap(),
            targets: Default::default(),
        });

        let started = tokio::time::Instant::now();
        request("POST", "/shared").await;
        assert_eq!(started.elapsed(), std::time::Duration::ZERO);

        let status = request("GET", "/shared").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(started.elapsed(), ROUTE_TRANSITION_WAIT);
    }

    #[tokio::test(start_paused = true)]
    async fn a_path_nobody_serves_is_not_held_by_a_reload() {
        let _guard = route_test_state().await;
        let generation = crate::services::STATE.generation();
        let _transition = generation.begin_transition(crate::services::state::Transition::Runtime);

        let started = tokio::time::Instant::now();
        let status = request("GET", "/nothing").await;

        assert_eq!(started.elapsed(), std::time::Duration::ZERO);
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test(start_paused = true)]
    async fn the_final_router_routes_the_uri_as_sent() {
        let _guard = route_test_state().await;
        install_routes(vec![route_on("plugin", "GET", "/items", true)], &["/items"]).await;
        let generation = crate::services::STATE.generation();
        let transition = generation.begin_transition(crate::services::state::Transition::Runtime);
        let settle = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            // No longer case-insensitive: only the casing as sent reaches it.
            install_routes(vec![route_on("plugin", "GET", "/ITEMS", false)], &[]).await;
            drop(transition);
        });

        let status = request("GET", "/ITEMS").await;

        assert_ne!(
            status,
            StatusCode::NOT_FOUND,
            "the new route matched as sent"
        );
        settle.await.unwrap();
    }

    use std::sync::atomic::Ordering;

    const RUNTIME_NOT_READY: &str = "plugin runtime is not ready";

    fn http_route(
        path: &str,
        auth_mode: RouteAuthMode,
        max_body_bytes: Option<usize>,
    ) -> RegisteredRoute {
        RegisteredRoute {
            key: RouteKey::new("POST", path).unwrap(),
            plugin_id: crate::services::plugin_scope::PluginId::new("limits").unwrap(),
            auth_mode,
            case_insensitive: false,
            max_body_bytes,
            handler: crate::services::plugin_dispatch::PluginCallbackHandle {
                vm_id: 0,
                handler_id: 0,
            },
            kind: RouteKind::Http,
        }
    }

    fn routed(route: &RegisteredRoute, budget: &PublicBodyBudget) -> Router {
        Router::new().route(
            route.key.path.as_ref(),
            build_http_method_router(route, budget.clone()).unwrap(),
        )
    }

    async fn session_token() -> String {
        let user = {
            let mut db = crate::services::STATE.db.write().await;
            lyra_db::users::create(&mut db, &lyra_db::fixtures::test_user("caller").unwrap())
                .unwrap()
        };
        crate::services::testing::create_session(user, Default::default())
            .await
            .unwrap()
            .token
    }

    fn post_from(address: &str, body: Body) -> Request {
        let client = crate::services::auth::caller::ClientAddress::new(address.parse().unwrap());
        Request::builder()
            .method(Method::POST)
            .uri("/upload")
            .extension(client)
            .body(body)
            .unwrap()
    }

    fn pending_body() -> (Body, tokio::sync::oneshot::Sender<()>) {
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel::<()>();
        let body = Body::from_stream(futures::stream::once(async move {
            finish_rx.await.ok();
            Ok::<_, std::io::Error>(axum::body::Bytes::from_static(b"body"))
        }));
        (body, finish_tx)
    }

    async fn wait_for_available(budget: &PublicBodyBudget, bytes: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while budget.available() != bytes {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first request reserves its limit");
    }

    fn assert_refused_as_busy(response: &Response) {
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()["retry-after"], "5");
    }

    fn post_to(path: &str, token: Option<&str>, body: Body) -> Request {
        let mut request = Request::builder().method(Method::POST).uri(path);
        if let Some(token) = token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        request.body(body).unwrap()
    }

    async fn status_and_text(response: Response) -> (StatusCode, String) {
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    #[tokio::test]
    async fn a_route_body_limit_replaces_the_server_limit_for_that_route_alone() {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state().unwrap();
        let token = session_token().await;
        let server_limit = 1024;
        let budget = PublicBodyBudget::server();
        let mut routes = Router::new();
        for route in [
            http_route("/default", RouteAuthMode::Required, None),
            http_route("/large", RouteAuthMode::Required, Some(4 * server_limit)),
            http_route("/small", RouteAuthMode::Required, Some(16)),
        ] {
            routes = routes.route(
                route.key.path.as_ref(),
                build_http_method_router(&route, budget.clone()).unwrap(),
            );
        }
        let app = Router::new()
            .fallback_service(routes)
            .layer(DefaultBodyLimit::max(server_limit));

        // A body the route accepts gets as far as the plugin runtime, which no test server has.
        for (path, size, status) in [
            ("/default", server_limit, StatusCode::SERVICE_UNAVAILABLE),
            ("/default", server_limit + 1, StatusCode::PAYLOAD_TOO_LARGE),
            ("/large", 4 * server_limit, StatusCode::SERVICE_UNAVAILABLE),
            (
                "/large",
                4 * server_limit + 1,
                StatusCode::PAYLOAD_TOO_LARGE,
            ),
            ("/small", 16, StatusCode::SERVICE_UNAVAILABLE),
            ("/small", 17, StatusCode::PAYLOAD_TOO_LARGE),
        ] {
            let response = app
                .clone()
                .oneshot(post_to(path, Some(&token), Body::from(vec![b'x'; size])))
                .await
                .unwrap();
            assert_eq!(response.status(), status, "{path} with {size} bytes");
        }
    }

    #[tokio::test]
    async fn a_required_route_refuses_an_anonymous_caller_before_reading_the_body() {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state().unwrap();
        let app = routed(
            &http_route("/upload", RouteAuthMode::Required, None),
            &PublicBodyBudget::for_tests(0),
        );
        let (body, read) = crate::routes::request::watched_body();

        let response = app.oneshot(post_to("/upload", None, body)).await.unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(!read.load(Ordering::SeqCst), "the body stays unread");
    }

    #[tokio::test]
    async fn a_required_route_answers_without_waiting_for_the_body_over_a_socket() {
        use tokio::io::AsyncWriteExt;

        use crate::routes::listener::tests::{
            connect,
            limits,
            read_until,
            start,
        };

        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state().unwrap();
        let app = routed(
            &http_route("/upload", RouteAuthMode::Required, None),
            &PublicBodyBudget::for_tests(0),
        );
        let server = start(app, limits()).await;

        let mut stream = connect(server.addr).await;
        stream
            .write_all(b"POST /upload HTTP/1.1\r\nHost: lyra.test\r\nContent-Length: 4096\r\n\r\n")
            .await
            .unwrap();
        let response = read_until(&mut stream, "\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 401"), "{response}");

        drop(stream);
        server.stop().await;
    }

    #[tokio::test]
    async fn an_address_holds_one_public_upload_at_a_time() {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state().unwrap();
        let limit = 2 * REQUEST_BODY_LIMIT_BYTES;
        let total = 4 * limit;
        let budget = PublicBodyBudget::for_tests(total);
        let app = routed(
            &http_route("/upload", RouteAuthMode::Public, Some(limit)),
            &budget,
        );

        let (body, finish) = pending_body();
        let first = tokio::spawn(app.clone().oneshot(post_from("203.0.113.1", body)));
        wait_for_available(&budget, total - limit).await;

        let same_address = app
            .clone()
            .oneshot(post_from("203.0.113.1", Body::from("second")))
            .await
            .unwrap();
        assert_refused_as_busy(&same_address);
        assert_eq!(budget.available(), total - limit, "the budget had room");

        let other_address = app
            .clone()
            .oneshot(post_from("203.0.113.2", Body::from("third")))
            .await
            .unwrap();
        assert_eq!(
            status_and_text(other_address).await,
            (
                StatusCode::SERVICE_UNAVAILABLE,
                RUNTIME_NOT_READY.to_string()
            )
        );

        finish.send(()).unwrap();
        first.await.unwrap().unwrap();
        assert_eq!(budget.available(), total);

        let again = app
            .oneshot(post_from("203.0.113.1", Body::from("fourth")))
            .await
            .unwrap();
        assert_eq!(
            status_and_text(again).await,
            (
                StatusCode::SERVICE_UNAVAILABLE,
                RUNTIME_NOT_READY.to_string()
            )
        );
    }

    #[tokio::test]
    async fn an_abandoned_upload_gives_back_its_reservation() {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state().unwrap();
        let limit = 2 * REQUEST_BODY_LIMIT_BYTES;
        let budget = PublicBodyBudget::for_tests(limit);
        let app = routed(
            &http_route("/upload", RouteAuthMode::Public, Some(limit)),
            &budget,
        );

        let (body, _finish) = pending_body();
        let upload = tokio::spawn(app.oneshot(post_from("203.0.113.1", body)));
        wait_for_available(&budget, 0).await;

        upload.abort();
        let _ = upload.await;

        assert_eq!(budget.available(), limit);
    }

    #[tokio::test]
    async fn only_public_routes_raising_their_limit_draw_on_the_budget() {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state().unwrap();
        let token = session_token().await;
        let empty = PublicBodyBudget::for_tests(0);
        let raised = Some(2 * REQUEST_BODY_LIMIT_BYTES);
        for (route, token) in [
            (http_route("/upload", RouteAuthMode::Public, None), None),
            (
                http_route(
                    "/upload",
                    RouteAuthMode::Public,
                    Some(REQUEST_BODY_LIMIT_BYTES),
                ),
                None,
            ),
            (
                http_route("/upload", RouteAuthMode::Required, raised),
                Some(token.as_str()),
            ),
        ] {
            let response = routed(&route, &empty)
                .oneshot(post_to("/upload", token, Body::from("body")))
                .await
                .unwrap();
            assert_eq!(
                status_and_text(response).await,
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    RUNTIME_NOT_READY.to_string()
                ),
                "{:?} with limit {:?}",
                route.auth_mode,
                route.max_body_bytes,
            );
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
