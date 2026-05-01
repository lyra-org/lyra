// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use crate::plugins::lifecycle::PluginId;
use std::collections::{
    HashMap,
    HashSet,
};
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::LazyLock;

use anyhow::{
    Context,
    Result,
    bail,
};
use axum::Router;
use axum::body::{
    Body,
    Bytes,
};
use axum::extract::{
    Path,
    Query,
    Request,
    WebSocketUpgrade,
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
use axum::routing::{
    MethodFilter,
    MethodRouter,
    any,
    get,
    on,
};
use mlua::Value;
use tokio::sync::RwLock;
use tower::ServiceExt;
use tower_http::services::{
    ServeDir,
    ServeFile,
};

use super::registry::{
    API_ROUTE_REGISTRY,
    CaseInsensitiveRouter,
    RegisteredRoute,
    RouteAuthMode,
    RouteKind,
    is_placeholder_segment,
};
use super::transport::{
    build_context,
    lua_response_to_axum,
};
use super::websocket::dispatch_websocket_route;

static INSTALLED_ROUTERS: LazyLock<Arc<RwLock<InstalledRouters>>> =
    LazyLock::new(|| Arc::new(RwLock::new(InstalledRouters::empty())));

/// Captured at `initialize_router` time so post-startup rebuilds
/// (e.g. plugin teardown) produce the same fallback shape startup did,
/// without plumbing `static_dir` through every call site.
///
/// `RwLock<Option<..>>` instead of `OnceLock`: a test harness or
/// alternate bootstrap phase may legitimately re-initialize. `OnceLock`
/// would silently drop the second init and leave the teardown rebuild
/// reading a stale value — every non-API request would then 404 after
/// the first plugin teardown. Overwrite semantics match `INSTALLED_ROUTERS`
/// right above.
static INSTALLED_STATIC_DIR: LazyLock<Arc<RwLock<Option<PathBuf>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(None)));

#[derive(Clone)]
struct InstalledRouters {
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

async fn api_not_found() -> (StatusCode, &'static str) {
    (StatusCode::NOT_FOUND, "Error: not found")
}

fn apply_catchall_and_static(router: Router, static_dir: Option<PathBuf>) -> Router {
    let router = router
        .route("/api", any(api_not_found))
        .route("/api/{*path}", any(api_not_found));

    if let Some(static_dir) = static_dir {
        let static_fallback = ServeFile::new(static_dir.join("index.html"));
        let static_service = ServeDir::new(&static_dir)
            .append_index_html_on_directories(false)
            .not_found_service(static_fallback);
        router.fallback_service(static_service)
    } else {
        router
    }
}

pub(crate) async fn initialize_router(static_dir: Option<PathBuf>) {
    *INSTALLED_STATIC_DIR.write().await = static_dir.clone();
    let main = apply_catchall_and_static(Router::new(), static_dir);
    *INSTALLED_ROUTERS.write().await = InstalledRouters {
        main,
        ci: Arc::new(CaseInsensitiveRouter::new()),
    };
}

pub(crate) async fn installed_static_dir() -> Option<PathBuf> {
    INSTALLED_STATIC_DIR.read().await.clone()
}

pub(crate) fn find_static_dir() -> Option<PathBuf> {
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

    candidates.into_iter().flatten().find(|path| path.is_dir())
}

pub(crate) async fn install_router(router: Router, ci_router: CaseInsensitiveRouter) {
    *INSTALLED_ROUTERS.write().await = InstalledRouters {
        main: router,
        ci: Arc::new(ci_router),
    };
}

pub(crate) async fn build_router(
    static_dir: Option<PathBuf>,
) -> Result<(Router, CaseInsensitiveRouter)> {
    let (router, ci_router) = build_registered_router().await?;
    Ok((apply_catchall_and_static(router, static_dir), ci_router))
}

/// Rebuild the live Axum router from the current registry snapshot and
/// hot-swap it. Used by plugin teardown: clearing the registry bucket
/// removes routes from the *registry* view, but the installed router
/// retains `RegisteredRoute` clones (each embedding a
/// `PluginFunctionHandle`) until it is replaced. Without this swap,
/// drain waits on handles pinned by the old router forever.
pub(crate) async fn rebuild_and_install_router() -> Result<()> {
    let static_dir = installed_static_dir().await;
    let (router, ci_router) = build_router(static_dir).await?;
    install_router(router, ci_router).await;
    Ok(())
}

pub(crate) async fn fallback(mut request: Request) -> Response {
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
    path.bytes().any(|b| b.is_ascii_uppercase())
}

// Rewrite the incoming path to the stored canonical form: lowercase literal
// segments so matchit matches, pass placeholder captures through unchanged
// so the handler sees the client's original case.
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
        if is_placeholder_segment(pattern_seg) {
            if let Some(seg) = incoming_iter.next() {
                out.push(seg.to_string());
            }
        } else if let Some(seg) = incoming_iter.next() {
            out.push(seg.to_ascii_lowercase());
        }
    }
    out.join("/")
}

fn describe_probe_conflict(
    plugin_id: &PluginId,
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

fn rebuild_uri_with_path(uri: &Uri, new_path: &str) -> Option<Uri> {
    let mut parts = uri.clone().into_parts();
    let rebuilt = match parts.path_and_query.as_ref().and_then(|pq| pq.query()) {
        Some(q) => format!("{new_path}?{q}"),
        None => new_path.to_string(),
    };
    parts.path_and_query = Some(rebuilt.parse().ok()?);
    Uri::from_parts(parts).ok()
}

pub(crate) async fn build_registered_router() -> Result<(Router, CaseInsensitiveRouter)> {
    let (routes, ci_router) = {
        let registry = API_ROUTE_REGISTRY.read().await;
        (registry.snapshot(), registry.ci_router_snapshot())
    };

    // Pre-validate path patterns so matchit-level conflicts surface as clean
    // errors here rather than opaque panics from `axum::Router::route`.
    // Seeded with the reserved catchalls that `apply_catchall_and_static`
    // installs after build, so plugin routes conflicting with them fail here.
    let mut probe: matchit::Router<String> = matchit::Router::new();
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

    Ok((router, ci_router))
}

fn build_http_method_router(route: &RegisteredRoute) -> Result<MethodRouter> {
    let method_filter = method_filter_for(&route.key.method)?;
    let dispatch_route = route.clone();
    Ok(on(
        method_filter,
        move |method: Method,
              uri: Uri,
              headers: HeaderMap,
              Query(pairs): Query<Vec<(String, String)>>,
              params: Option<Path<HashMap<String, String>>>,
              body: Bytes| {
            let route = dispatch_route.clone();
            async move {
                let query = collect_query_pairs(pairs);
                dispatch_registered_route(route, method, uri, headers, query, params, body).await
            }
        },
    ))
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
                dispatch_websocket_route(route, uri, headers, query, params.map(|p| p.0), ws).await
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

async fn dispatch_registered_route(
    route: RegisteredRoute,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    query: HashMap<String, Vec<String>>,
    params: Option<Path<HashMap<String, String>>>,
    body: Bytes,
) -> Response {
    let Some(lua) = route.handler.try_upgrade_lua() else {
        tracing::error!(
            plugin_id = %route.plugin_id,
            method = %route.key.method,
            path = %route.key.path,
            "plugin route handler's lua instance is no longer valid"
        );
        return (StatusCode::SERVICE_UNAVAILABLE, "plugin unavailable").into_response();
    };

    let ctx = match build_context(
        &lua,
        &route,
        &method,
        &uri,
        &headers,
        &query,
        params.as_ref().map(|path| &path.0),
        &body,
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(err) => {
            tracing::error!(
                plugin_id = %route.plugin_id,
                method = %route.key.method,
                path = %route.key.path,
                error = %err,
                "failed to construct lua request context"
            );
            return (StatusCode::INTERNAL_SERVER_ERROR, "internal server error").into_response();
        }
    };

    if matches!(route.auth_mode, RouteAuthMode::Required)
        && matches!(ctx.get::<Value>("auth"), Ok(Value::Nil))
    {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }

    let result = route.handler.call_async::<_, Value>(ctx).await;
    let response = match result {
        Ok(value) => lua_response_to_axum(&lua, value, &headers).await,
        Err(err) => {
            tracing::error!(
                plugin_id = %route.plugin_id,
                method = %route.key.method,
                path = %route.key.path,
                error = %err,
                "plugin route handler failed"
            );
            return (StatusCode::INTERNAL_SERVER_ERROR, "internal server error").into_response();
        }
    };

    match response {
        Ok(response) => strip_body_for_head(&method, response),
        Err(err) => {
            tracing::error!(
                plugin_id = %route.plugin_id,
                method = %route.key.method,
                path = %route.key.path,
                error = %err,
                "failed to build plugin response"
            );
            (StatusCode::INTERNAL_SERVER_ERROR, "internal server error").into_response()
        }
    }
}

fn strip_body_for_head(method: &Method, response: Response) -> Response {
    if *method != Method::HEAD {
        return response;
    }

    let (parts, _) = response.into_parts();
    Response::from_parts(parts, Body::empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_ascii_upper_detects_any_uppercase_byte() {
        assert!(has_ascii_upper("/Users/abc"));
        assert!(has_ascii_upper("/a/B/c"));
        assert!(!has_ascii_upper("/users/abc"));
        assert!(!has_ascii_upper("/files/abc/café"));
    }

    #[test]
    fn rewrite_path_for_ci_lowers_literals_preserves_captures() {
        let out = rewrite_path_for_ci("/users/{userId}/items", "/Users/ABC123/Items");
        assert_eq!(out, "/users/ABC123/items");
    }

    #[test]
    fn rewrite_path_for_ci_preserves_catchall_case() {
        let out = rewrite_path_for_ci("/files/{*rest}", "/Files/Deep/Nested/PATH");
        assert_eq!(out, "/files/Deep/Nested/PATH");
    }

    #[test]
    fn rewrite_path_for_ci_handles_trailing_literal() {
        let out = rewrite_path_for_ci("/a/{id}/b", "/A/xyZ/B");
        assert_eq!(out, "/a/xyZ/b");
    }

    #[test]
    fn rebuild_uri_preserves_query_string() {
        let uri: Uri = "/Users/ABC/Items?userId=X&limit=10".parse().unwrap();
        let rebuilt = rebuild_uri_with_path(&uri, "/users/ABC/items").unwrap();
        assert_eq!(rebuilt.path(), "/users/ABC/items");
        assert_eq!(rebuilt.query(), Some("userId=X&limit=10"));
    }

    #[test]
    fn rebuild_uri_without_query() {
        let uri: Uri = "/Users/ABC/Items".parse().unwrap();
        let rebuilt = rebuild_uri_with_path(&uri, "/users/ABC/items").unwrap();
        assert_eq!(rebuilt.path(), "/users/ABC/items");
        assert_eq!(rebuilt.query(), None);
    }

    #[tokio::test]
    async fn ci_router_matches_lowered_and_returns_pattern() {
        let mut ci = CaseInsensitiveRouter::new();
        ci.insert("/users/{userid}/items").unwrap();
        let pattern = ci.find_pattern("/users/abc/items").unwrap();
        assert_eq!(pattern.as_ref(), "/users/{userid}/items");
        assert!(ci.find_pattern("/unknown").is_none());
    }

    #[test]
    fn collect_query_pairs_preserves_repeated_keys_in_order() {
        let pairs = vec![
            ("tag".to_string(), "alpha".to_string()),
            ("tag".to_string(), "beta".to_string()),
            ("count".to_string(), "10".to_string()),
            ("tag".to_string(), "gamma".to_string()),
        ];
        let collected = collect_query_pairs(pairs);
        assert_eq!(
            collected.get("tag"),
            Some(&vec![
                "alpha".to_string(),
                "beta".to_string(),
                "gamma".to_string(),
            ])
        );
        assert_eq!(collected.get("count"), Some(&vec!["10".to_string()]));
    }

    #[test]
    fn collect_query_pairs_handles_empty_input() {
        let collected = collect_query_pairs(Vec::new());
        assert!(collected.is_empty());
    }
}
