// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};
use std::sync::Arc;

use anyhow::{
    Context,
    Result,
    bail,
};
use mlua::Function;
use std::sync::LazyLock;
use tokio::sync::RwLock;

use crate::plugins::lifecycle::{
    PluginFunctionHandle,
    PluginId,
    PluginScopedInner,
    ScopedRegistry,
};
use crate::routes::registry::{
    RouteKey,
    lowercase_literal_segments,
};

pub(crate) use crate::routes::registry::is_placeholder_segment;

pub static API_ROUTE_REGISTRY: LazyLock<Arc<RwLock<ApiRouteRegistry>>> =
    LazyLock::new(|| Arc::new(RwLock::new(ApiRouteRegistry::new())));

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
    pub(super) plugin_id: PluginId,
    pub(super) auth_mode: RouteAuthMode,
    pub(super) case_insensitive: bool,
    pub(super) handler: PluginFunctionHandle,
    pub(super) kind: RouteKind,
}

#[derive(Default, Clone)]
pub(crate) struct CaseInsensitiveRouter {
    inner: matchit::Router<Arc<str>>,
    // Sidecar path set makes insert idempotent for same-path duplicates
    // (e.g. sibling methods on one route). matchit still catches
    // structurally incompatible patterns.
    paths: HashSet<String>,
}

impl CaseInsensitiveRouter {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(super) fn insert(&mut self, path: &str) -> Result<()> {
        if !self.paths.insert(path.to_string()) {
            return Ok(());
        }
        let pattern: Arc<str> = Arc::from(path);
        self.inner
            .insert(path.to_string(), pattern)
            .with_context(|| format!("register case-insensitive route '{path}'"))?;
        Ok(())
    }

    pub(crate) fn find_pattern(&self, path: &str) -> Option<Arc<str>> {
        self.inner.at(path).ok().map(|m| m.value.clone())
    }
}

/// `state` is the *registration* gate, not the router lifecycle gate.
/// Plugin init runs, routes register, then registration freezes at
/// startup. A restart flow can exempt one plugin while keeping every
/// other plugin frozen, then re-freeze it after re-registration.
#[derive(Default)]
enum FreezeState {
    #[default]
    Open,
    Frozen {
        exemptions: HashSet<PluginId>,
    },
}

#[derive(Default)]
pub struct ApiRouteRegistry {
    core_routes: HashSet<RouteKey>,
    pub(super) routes: HashMap<PluginId, HashMap<RouteKey, RegisteredRoute>>,
    pub(super) ci_router: CaseInsensitiveRouter,
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

    pub(super) fn unfreeze_plugin(&mut self, plugin_id: PluginId) {
        if let FreezeState::Frozen { exemptions } = &mut self.state {
            exemptions.insert(plugin_id);
        }
    }

    pub(super) fn refreeze_plugin(&mut self, plugin_id: &PluginId) {
        if let FreezeState::Frozen { exemptions } = &mut self.state {
            exemptions.remove(plugin_id);
        }
    }

    fn writes_allowed(&self, plugin_id: &PluginId) -> bool {
        match &self.state {
            FreezeState::Open => true,
            FreezeState::Frozen { exemptions } => exemptions.contains(plugin_id),
        }
    }

    fn iter_all(&self) -> impl Iterator<Item = &RegisteredRoute> {
        self.routes.values().flat_map(|bucket| bucket.values())
    }

    pub(super) fn find(&self, key: &RouteKey) -> Option<&RegisteredRoute> {
        self.iter_all().find(|route| &route.key == key)
    }

    #[cfg(test)]
    pub(super) fn route_count(&self) -> usize {
        self.routes.values().map(HashMap::len).sum()
    }

    fn register(&mut self, route: RegisteredRoute) -> Result<()> {
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

        // Shadow detection: a CI route claims every casing of its lowered
        // path; a CS route whose path lowers to the same form becomes
        // silently unreachable for mixed-case requests. Reject at call time.
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
            if existing_lowered != new_lowered {
                continue;
            }
            if route.case_insensitive || existing.case_insensitive {
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
        routes.sort_by(|a, b| {
            a.key
                .path
                .cmp(&b.key.path)
                .then_with(|| a.key.method.cmp(&b.key.method))
        });
        routes
    }

    pub(super) fn ci_router_snapshot(&self) -> CaseInsensitiveRouter {
        self.ci_router.clone()
    }
}

impl PluginScopedInner for ApiRouteRegistry {
    fn clear_bucket(&mut self, plugin_id: &PluginId) {
        self.routes.remove(plugin_id);
        if let FreezeState::Frozen { exemptions } = &mut self.state {
            exemptions.remove(plugin_id);
        }
    }

    fn rebuild_derived(&mut self) {
        // matchit has no targeted removal, so reseat `ci_router` from every
        // surviving plugin's case-insensitive routes. Every path here was
        // already validated at register time — a failure means the
        // invariant is broken, not a user error, so we panic rather than
        // silently leave ci_router with stale holes.
        let ci_paths: Vec<(PluginId, Arc<str>)> = self
            .iter_all()
            .filter(|route| route.case_insensitive)
            .map(|route| (route.plugin_id.clone(), route.key.path.clone()))
            .collect();
        self.ci_router = CaseInsensitiveRouter::new();
        for (plugin_id, path) in ci_paths {
            self.ci_router.insert(&path).unwrap_or_else(|err| {
                panic!(
                    "ci_router rebuild failed for plugin '{plugin_id}' path '{path}' (already validated at register time): {err}"
                )
            });
        }
    }
}

fn build_registered_route(
    plugin_id: PluginId,
    method: &str,
    path: &str,
    auth_mode: Option<&str>,
    case_insensitive: bool,
    handler: PluginFunctionHandle,
    kind: RouteKind,
) -> mlua::Result<RegisteredRoute> {
    let key = if case_insensitive {
        RouteKey::new_case_insensitive(method, path)
    } else {
        RouteKey::new(method, path)
    }
    .map_err(|err| mlua::Error::runtime(err.to_string()))?;
    Ok(RegisteredRoute {
        key,
        plugin_id,
        auth_mode: normalize_auth_mode(auth_mode)
            .map_err(|err| mlua::Error::runtime(err.to_string()))?,
        case_insensitive,
        handler,
        kind,
    })
}

fn normalize_auth_mode(auth_mode: Option<&str>) -> Result<RouteAuthMode> {
    let auth_mode = auth_mode
        .map(str::trim)
        .filter(|mode| !mode.is_empty())
        .map(str::to_ascii_lowercase);
    match auth_mode.as_deref() {
        None | Some("required") => Ok(RouteAuthMode::Required),
        Some("public") => Ok(RouteAuthMode::Public),
        Some(other) => bail!(
            "unsupported auth mode '{}'; expected 'public' or 'required'",
            other
        ),
    }
}

pub(super) async fn register_route_impl(
    plugin_id: PluginId,
    method: &str,
    path: String,
    auth_mode: Option<String>,
    case_insensitive: bool,
    handler: Function,
    kind: RouteKind,
) -> mlua::Result<()> {
    let _registration = crate::STATE
        .plugin_registries
        .ensure_registrations_open(&plugin_id)
        .await?;

    let counter = crate::STATE
        .plugin_registries
        .inflight_counter(&plugin_id)
        .await;
    let handle = PluginFunctionHandle::new(plugin_id.clone(), counter, handler);
    let route = build_registered_route(
        plugin_id,
        method,
        &path,
        auth_mode.as_deref(),
        case_insensitive,
        handle,
        kind,
    )?;
    let mut registry = API_ROUTE_REGISTRY.write().await;
    registry
        .register(route)
        .map_err(|err| mlua::Error::runtime(err.to_string()))?;
    Ok(())
}

pub(crate) async fn initialize_registry(core_routes: HashSet<RouteKey>) {
    let mut registry = API_ROUTE_REGISTRY.write().await;
    registry.reset(core_routes);
}

pub(crate) async fn freeze_registry() {
    let mut registry = API_ROUTE_REGISTRY.write().await;
    registry.freeze();
}

pub(super) async fn unfreeze_plugin_routes(plugin_id: PluginId) {
    API_ROUTE_REGISTRY.write().await.unfreeze_plugin(plugin_id);
}

pub(super) async fn refreeze_plugin_routes(plugin_id: &PluginId) {
    API_ROUTE_REGISTRY.write().await.refreeze_plugin(plugin_id);
}

pub(crate) async fn teardown_plugin_routes(plugin_id: &PluginId) {
    ScopedRegistry::from_shared(API_ROUTE_REGISTRY.clone())
        .teardown(plugin_id)
        .await;

    // Registry clear alone leaves the installed Axum router holding
    // `RegisteredRoute` clones (each embedding a `PluginFunctionHandle`)
    // for this plugin's routes. Rebuild + hot-swap so those clones can
    // drop as in-flight requests finish, letting drain reach zero.
    //
    // Rebuilding from a post-clear snapshot is an infallible operation
    // by construction: we're removing routes, not adding, so the probe
    // can't surface new conflicts. Any error here is a broken
    // invariant — matches `rebuild_derived`'s panic-on-failure policy.
    // Log-and-continue would leave drain waiting on the old router's
    // handles forever, which is worse than a crash.
    super::dispatch::rebuild_and_install_router()
        .await
        .unwrap_or_else(|err| {
            panic!("router rebuild must not fail post-teardown (plugin_id={plugin_id}): {err}",);
        });
}
