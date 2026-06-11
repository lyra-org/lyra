// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::*;

#[derive(Default)]
pub(crate) struct ApiRouteStore {
    next_handler_id: Cell<u64>,
    handlers: RefCell<HashMap<u64, ApiHandlerRegistration>>,
}

impl ApiRouteStore {
    pub(crate) fn new() -> Self {
        Self {
            next_handler_id: Cell::new(1),
            handlers: RefCell::new(HashMap::new()),
        }
    }

    pub(super) fn register(
        &self,
        plugin_id: Arc<str>,
        method: &'static str,
        path: String,
        handler: luau::Function,
        auth_mode: Option<String>,
        case_insensitive: bool,
        context: harmony_core::CallContext,
    ) -> luau::runtime::Result<()> {
        let key = if case_insensitive {
            RouteKey::new_case_insensitive(method, &path)
        } else {
            RouteKey::new(method, &path)
        }
        .map_err(crate::plugins::runtime_error)?;
        let auth_mode = normalize_auth_mode(auth_mode.as_deref())?;
        let plugin = crate::plugins::lifecycle::PluginId::new(plugin_id.to_string())
            .map_err(crate::plugins::runtime_error)?;

        let handler_id = self.next_handler_id.get();
        self.next_handler_id.set(handler_id.saturating_add(1));
        self.handlers
            .borrow_mut()
            .insert(handler_id, ApiHandlerRegistration { handler, context });
        futures::executor::block_on(async {
            API_ROUTE_REGISTRY
                .write()
                .await
                .register(RegisteredRoute {
                    key,
                    plugin_id: plugin,
                    auth_mode,
                    case_insensitive,
                    handler_id,
                    kind: RouteKind::Http,
                })
                .map_err(crate::plugins::runtime_error)
        })?;
        Ok(())
    }

    pub(super) fn register_websocket(
        &self,
        plugin_id: Arc<str>,
        path: String,
        handler: luau::Function,
        auth_mode: Option<String>,
        context: harmony_core::CallContext,
    ) -> luau::runtime::Result<()> {
        let key = RouteKey::new("GET", &path).map_err(crate::plugins::runtime_error)?;
        let auth_mode = normalize_auth_mode(auth_mode.as_deref())?;
        let plugin = crate::plugins::lifecycle::PluginId::new(plugin_id.to_string())
            .map_err(crate::plugins::runtime_error)?;

        let handler_id = self.next_handler_id.get();
        self.next_handler_id.set(handler_id.saturating_add(1));
        self.handlers
            .borrow_mut()
            .insert(handler_id, ApiHandlerRegistration { handler, context });
        futures::executor::block_on(async {
            API_ROUTE_REGISTRY
                .write()
                .await
                .register(RegisteredRoute {
                    key,
                    plugin_id: plugin,
                    auth_mode,
                    case_insensitive: false,
                    handler_id,
                    kind: RouteKind::WebSocket,
                })
                .map_err(crate::plugins::runtime_error)
        })?;
        Ok(())
    }

    pub(crate) fn get(&self, id: u64) -> Option<ApiHandlerRegistration> {
        self.handlers.borrow().get(&id).cloned()
    }
}

#[derive(Clone)]
pub(crate) struct ApiHandlerRegistration {
    pub(crate) handler: luau::Function,
    pub(crate) context: harmony_core::CallContext,
}

pub(super) static API_ROUTE_REGISTRY: LazyLock<Arc<tokio::sync::RwLock<ApiRouteRegistry>>> =
    LazyLock::new(|| Arc::new(tokio::sync::RwLock::new(ApiRouteRegistry::new())));
static LIMITER_ROUTE_MATCHER: LazyLock<std::sync::RwLock<Arc<matchit::Router<()>>>> =
    LazyLock::new(|| std::sync::RwLock::new(Arc::new(matchit::Router::new())));
pub(super) static INSTALLED_ROUTERS: LazyLock<Arc<tokio::sync::RwLock<InstalledRouters>>> =
    LazyLock::new(|| Arc::new(tokio::sync::RwLock::new(InstalledRouters::empty())));
pub(super) static INSTALLED_STATIC_DIR: LazyLock<Arc<tokio::sync::RwLock<Option<PathBuf>>>> =
    LazyLock::new(|| Arc::new(tokio::sync::RwLock::new(None)));

pub(super) const LYRA_STATIC_DIR_ENV: &str = "LYRA_STATIC_DIR";

#[derive(Clone, Debug)]
pub(super) enum RouteAuthMode {
    Public,
    Required,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RouteKind {
    Http,
    WebSocket,
}

#[derive(Clone, Debug)]
pub(super) struct RegisteredRoute {
    pub(super) key: RouteKey,
    pub(super) plugin_id: crate::plugins::lifecycle::PluginId,
    pub(super) auth_mode: RouteAuthMode,
    pub(super) case_insensitive: bool,
    pub(super) handler_id: u64,
    pub(super) kind: RouteKind,
}

#[derive(Default, Clone)]
pub(super) struct CaseInsensitiveRouter {
    inner: matchit::Router<Arc<str>>,
    paths: HashSet<String>,
}

impl CaseInsensitiveRouter {
    fn new() -> Self {
        Self::default()
    }

    fn insert(&mut self, path: &str) -> Result<()> {
        if !self.paths.insert(path.to_string()) {
            return Ok(());
        }
        let pattern: Arc<str> = Arc::from(path);
        self.inner
            .insert(path.to_string(), pattern)
            .with_context(|| format!("register case-insensitive route '{path}'"))?;
        Ok(())
    }

    fn find_pattern(&self, path: &str) -> Option<Arc<str>> {
        self.inner
            .at(path)
            .ok()
            .map(|matched| matched.value.clone())
    }
}

#[derive(Default)]
pub(super) enum FreezeState {
    #[default]
    Open,
    Frozen {
        exemptions: HashSet<crate::plugins::lifecycle::PluginId>,
    },
}

#[derive(Default)]
pub(super) struct ApiRouteRegistry {
    core_routes: HashSet<RouteKey>,
    routes: HashMap<crate::plugins::lifecycle::PluginId, HashMap<RouteKey, RegisteredRoute>>,
    ci_router: CaseInsensitiveRouter,
    state: FreezeState,
}

impl ApiRouteRegistry {
    fn new() -> Self {
        Self::default()
    }

    fn reset(&mut self, core_routes: HashSet<RouteKey>) {
        self.core_routes = core_routes;
        self.routes.clear();
        self.ci_router = CaseInsensitiveRouter::new();
        self.state = FreezeState::Open;
    }

    fn freeze(&mut self) {
        self.state = FreezeState::Frozen {
            exemptions: HashSet::new(),
        };
    }

    pub(super) fn unfreeze_plugin(&mut self, plugin_id: crate::plugins::lifecycle::PluginId) {
        if let FreezeState::Frozen { exemptions } = &mut self.state {
            exemptions.insert(plugin_id);
        }
    }

    pub(super) fn refreeze_plugin(&mut self, plugin_id: &crate::plugins::lifecycle::PluginId) {
        if let FreezeState::Frozen { exemptions } = &mut self.state {
            exemptions.remove(plugin_id);
        }
    }

    fn writes_allowed(&self, plugin_id: &crate::plugins::lifecycle::PluginId) -> bool {
        match &self.state {
            FreezeState::Open => true,
            FreezeState::Frozen { exemptions } => exemptions.contains(plugin_id),
        }
    }

    fn iter_all(&self) -> impl Iterator<Item = &RegisteredRoute> {
        self.routes.values().flat_map(|bucket| bucket.values())
    }

    fn find(&self, key: &RouteKey) -> Option<&RegisteredRoute> {
        self.iter_all().find(|route| &route.key == key)
    }

    pub(super) fn register(&mut self, route: RegisteredRoute) -> Result<()> {
        if !self.writes_allowed(&route.plugin_id) {
            bail!(
                "lyra/api route registry is frozen for plugin '{}'",
                route.plugin_id
            );
        }
        if self.core_routes.contains(&route.key) {
            bail!(
                "plugin route conflicts with core route: {} {}",
                route.key.method,
                route.key.path
            );
        }
        if let Some(existing) = self.find(&route.key) {
            bail!(
                "plugin route conflict: {} {} already registered by plugin '{}'",
                route.key.method,
                route.key.path,
                existing.plugin_id
            );
        }

        let new_lowered = if route.case_insensitive {
            route.key.path.to_string()
        } else {
            lowercase_literal_segments(route.key.path.as_ref())
        };
        for existing in self.iter_all() {
            if existing.key.path == route.key.path {
                continue;
            }
            let existing_lowered = if existing.case_insensitive {
                existing.key.path.to_string()
            } else {
                lowercase_literal_segments(existing.key.path.as_ref())
            };
            if existing_lowered == new_lowered
                && (route.case_insensitive || existing.case_insensitive)
            {
                bail!(
                    "plugin '{}' route '{}' shadows '{}' registered by plugin '{}' via case-insensitive lowering",
                    route.plugin_id,
                    route.key.path,
                    existing.key.path,
                    existing.plugin_id,
                );
            }
        }

        if route.case_insensitive {
            self.ci_router
                .insert(route.key.path.as_ref())
                .map_err(|err| {
                    anyhow::anyhow!(
                        "plugin '{}' route '{}': {}",
                        route.plugin_id,
                        route.key.path,
                        err
                    )
                })?;
        }
        self.routes
            .entry(route.plugin_id.clone())
            .or_default()
            .insert(route.key.clone(), route);
        Ok(())
    }

    pub(super) fn snapshot(&self) -> Vec<RegisteredRoute> {
        let mut routes: Vec<RegisteredRoute> = self.iter_all().cloned().collect();
        routes.sort_by(|left, right| {
            left.key
                .path
                .cmp(&right.key.path)
                .then_with(|| left.key.method.cmp(&right.key.method))
        });
        routes
    }

    fn ci_router_snapshot(&self) -> CaseInsensitiveRouter {
        self.ci_router.clone()
    }

    pub(super) fn clear_bucket(&mut self, plugin_id: &crate::plugins::lifecycle::PluginId) {
        self.routes.remove(plugin_id);
        if let FreezeState::Frozen { exemptions } = &mut self.state {
            exemptions.remove(plugin_id);
        }
        self.rebuild_derived();
    }

    fn rebuild_derived(&mut self) {
        let ci_paths: Vec<(crate::plugins::lifecycle::PluginId, Arc<str>)> = self
            .iter_all()
            .filter(|route| route.case_insensitive)
            .map(|route| (route.plugin_id.clone(), route.key.path.clone()))
            .collect();
        self.ci_router = CaseInsensitiveRouter::new();
        for (plugin_id, path) in ci_paths {
            self.ci_router.insert(&path).unwrap_or_else(|err| {
                panic!(
                    "case-insensitive router rebuild failed for plugin '{plugin_id}' path '{path}' (already validated at register time): {err}"
                )
            });
        }
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
        let static_fallback = ServeFile::new(static_dir.join("index.html"));
        let static_service = ServeDir::new(&static_dir)
            .append_index_html_on_directories(false)
            .not_found_service(static_fallback);
        router.fallback_service(static_service)
    } else {
        router
    }
}

pub(super) async fn initialize_registry(core_routes: HashSet<RouteKey>) {
    API_ROUTE_REGISTRY.write().await.reset(core_routes);
}

pub(super) async fn freeze_registry() {
    API_ROUTE_REGISTRY.write().await.freeze();
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

    let peer = request
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|connect_info| connect_info.0);
    let client_key = crate::services::rate_limit::request_client_key(peer, request.headers());
    request
        .extensions_mut()
        .insert(crate::services::rate_limit::RequestClientKey(
            client_key.into(),
        ));

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
    let (routes, ci_router) = {
        let registry = API_ROUTE_REGISTRY.read().await;
        (registry.snapshot(), registry.ci_router_snapshot())
    };

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
    plugin_id: &crate::plugins::lifecycle::PluginId,
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
    Ok(on(
        method_filter,
        move |method: Method,
              uri: Uri,
              headers: HeaderMap,
              Query(pairs): Query<Vec<(String, String)>>,
              params: Option<Path<HashMap<String, String>>>,
              client: Option<axum::Extension<crate::services::rate_limit::RequestClientKey>>,
              body: Bytes| {
            let route = dispatch_route.clone();
            async move {
                let query = collect_query_pairs(pairs);
                let client_key = client.map(|axum::Extension(key)| key.0.as_ref().to_string());
                dispatch_registered_route(
                    route, method, uri, headers, query, params, client_key, body,
                )
                .await
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
    client_key: Option<String>,
    body: Bytes,
) -> Response {
    let auth = match resolve_optional_auth(&headers).await {
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

    let Some(runtime) = crate::STATE.generation().plugin_runtime.get() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "plugin runtime is not ready",
        )
            .into_response();
    };

    let request_headers = headers.clone();
    let request = crate::plugins::executor::ApiHandlerRequest {
        handler_id: route.handler_id,
        plugin_id: route.plugin_id.to_string(),
        method: method.as_str().to_string(),
        path: uri.path().to_string(),
        headers: header_pairs(&headers),
        query,
        params: params.map(|params| params.0).unwrap_or_default(),
        body: body.to_vec(),
        auth: auth.clone(),
        client_key,
    };

    let result = runtime.dispatch_api_handler(request).await;

    match result {
        Ok(response) => {
            match plugin_api_response_to_axum(response, &request_headers, auth.as_ref()).await {
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
mod limiter_matcher_tests {
    use super::*;

    #[test]
    fn matches_registered_paths_with_case_insensitive_lowering() {
        let mut matcher: matchit::Router<()> = matchit::Router::new();
        matcher
            .insert("/jellyfin/users/{id}", ())
            .expect("pattern should insert");

        assert!(matches_route_path(&matcher, "/jellyfin/users/abc"));
        assert!(matches_route_path(&matcher, "/Jellyfin/Users/abc"));
        assert!(!matches_route_path(&matcher, "/assets/app.js"));
        assert!(!matches_route_path(&matcher, "/"));
    }
}
