// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use futures::future::BoxFuture;
use harmony_luau::StringEnum;

use super::*;
use crate::{
    plugins::callback_table::{
        CallbackTable,
        PluginCallback,
    },
    services::plugin_dispatch::PluginCallbackHandle,
};

/// Serves the registered plugin routes over HTTP. Rebuilt whenever the
/// registered set changes, so a failed rebuild surfaces to whoever changed it.
pub(crate) trait RouteHost: Send + Sync {
    fn rebuild(&self) -> BoxFuture<'_, Result<()>>;
}

pub(crate) struct ApiRouteStore {
    handlers: CallbackTable<ApiHandlerRegistration>,
}

pub(super) struct ApiRouteRegistration {
    pub(super) plugin_id: Arc<str>,
    pub(super) method: ApiMethod,
    pub(super) path: String,
    pub(super) handler: luau::Function,
    pub(super) auth_mode: Option<RouteAuthMode>,
    pub(super) case_insensitive: bool,
    pub(super) max_body_bytes: Option<usize>,
    pub(super) context: harmony_core::CallContext,
}

impl ApiRouteStore {
    pub(crate) fn new(vm_id: u64) -> Self {
        Self {
            handlers: CallbackTable::new(vm_id),
        }
    }

    pub(super) fn register(&self, registration: ApiRouteRegistration) -> luau::runtime::Result<()> {
        let ApiRouteRegistration {
            plugin_id,
            method,
            path,
            handler,
            auth_mode,
            case_insensitive,
            max_body_bytes,
            context,
        } = registration;
        let key = if case_insensitive {
            RouteKey::new_case_insensitive(method.as_str(), &path)
        } else {
            RouteKey::new(method.as_str(), &path)
        }?;
        let auth_mode = auth_mode.unwrap_or(RouteAuthMode::Required);
        let plugin = crate::services::plugin_scope::PluginId::new(plugin_id.to_string())?;

        let handler = self
            .handlers
            .insert(ApiHandlerRegistration { handler, context });
        futures::executor::block_on(async {
            api_route_registry()
                .write_owned()
                .await
                .register(RegisteredRoute {
                    key,
                    plugin_id: plugin,
                    auth_mode,
                    case_insensitive,
                    max_body_bytes,
                    handler,
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
        auth_mode: Option<RouteAuthMode>,
        context: harmony_core::CallContext,
    ) -> luau::runtime::Result<()> {
        let key = RouteKey::new("GET", &path)?;
        let auth_mode = auth_mode.unwrap_or(RouteAuthMode::Required);
        let plugin = crate::services::plugin_scope::PluginId::new(plugin_id.to_string())?;

        let handler = self
            .handlers
            .insert(ApiHandlerRegistration { handler, context });
        futures::executor::block_on(async {
            api_route_registry()
                .write_owned()
                .await
                .register(RegisteredRoute {
                    key,
                    plugin_id: plugin,
                    auth_mode,
                    case_insensitive: false,
                    max_body_bytes: None,
                    handler,
                    kind: RouteKind::WebSocket,
                })
                .map_err(crate::plugins::runtime_error)
        })?;
        Ok(())
    }

    pub(crate) fn get(&self, handle: PluginCallbackHandle) -> Result<ApiHandlerRegistration> {
        self.handlers.get(handle)
    }

    pub(crate) fn remove_plugin(&self, plugin_id: &str) {
        self.handlers.remove_plugin(plugin_id);
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.handlers.len()
    }
}

#[derive(Clone)]
pub(crate) struct ApiHandlerRegistration {
    pub(crate) handler: luau::Function,
    pub(crate) context: harmony_core::CallContext,
}

impl PluginCallback for ApiHandlerRegistration {
    fn context(&self) -> &harmony_core::CallContext {
        &self.context
    }
}

/// Generation-owned plugin API routes: the registered routes, their freeze state, and the host
/// that serves them.
#[derive(Default)]
pub(crate) struct ApiRouteRegistries {
    registry: Arc<tokio::sync::RwLock<ApiRouteRegistry>>,
}

pub(super) fn api_route_registry() -> Arc<tokio::sync::RwLock<ApiRouteRegistry>> {
    crate::plugins::STATE.generation().api.registry.clone()
}

#[harmony_macros::string_enum(
    name = "ApiRouteAuthMode",
    description = "Declared plugin route auth policy. Defaults to \"required\"."
)]
#[derive(Clone, Copy, Debug)]
pub(crate) enum RouteAuthMode {
    Public,
    Required,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RouteKind {
    Http,
    WebSocket,
}

#[derive(Clone, Debug)]
pub(crate) struct RegisteredRoute {
    pub(crate) key: RouteKey,
    pub(crate) plugin_id: crate::services::plugin_scope::PluginId,
    pub(crate) auth_mode: RouteAuthMode,
    pub(crate) case_insensitive: bool,
    /// The largest request body the route accepts; the server's limit without one.
    pub(crate) max_body_bytes: Option<usize>,
    pub(crate) handler: PluginCallbackHandle,
    pub(crate) kind: RouteKind,
}

#[derive(Default, Clone)]
pub(crate) struct CaseInsensitiveRouter {
    inner: matchit::Router<Arc<str>>,
    paths: HashSet<String>,
}

impl CaseInsensitiveRouter {
    pub(crate) fn new() -> Self {
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

    pub(crate) fn find_pattern(&self, path: &str) -> Option<Arc<str>> {
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
        exemptions: HashSet<crate::services::plugin_scope::PluginId>,
    },
}

#[derive(Default)]
pub(super) struct ApiRouteRegistry {
    core_routes: HashSet<RouteKey>,
    routes: HashMap<crate::services::plugin_scope::PluginId, HashMap<RouteKey, RegisteredRoute>>,
    ci_router: CaseInsensitiveRouter,
    state: FreezeState,
    host: Option<Arc<dyn RouteHost>>,
}

impl ApiRouteRegistry {
    fn reset(&mut self, core_routes: HashSet<RouteKey>, host: Option<Arc<dyn RouteHost>>) {
        self.core_routes = core_routes;
        self.host = host;
        self.routes.clear();
        self.ci_router = CaseInsensitiveRouter::new();
        self.state = FreezeState::Open;
    }

    pub(super) fn host(&self) -> Option<Arc<dyn RouteHost>> {
        self.host.clone()
    }

    fn freeze(&mut self) {
        self.state = FreezeState::Frozen {
            exemptions: HashSet::new(),
        };
    }

    pub(super) fn unfreeze_plugin(&mut self, plugin_id: crate::services::plugin_scope::PluginId) {
        if let FreezeState::Frozen { exemptions } = &mut self.state {
            exemptions.insert(plugin_id);
        }
    }

    pub(super) fn refreeze_plugin(&mut self, plugin_id: &crate::services::plugin_scope::PluginId) {
        if let FreezeState::Frozen { exemptions } = &mut self.state {
            exemptions.remove(plugin_id);
        }
    }

    fn writes_allowed(&self, plugin_id: &crate::services::plugin_scope::PluginId) -> bool {
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

    pub(super) fn clear_bucket(&mut self, plugin_id: &crate::services::plugin_scope::PluginId) {
        self.routes.remove(plugin_id);
        if let FreezeState::Frozen { exemptions } = &mut self.state {
            exemptions.remove(plugin_id);
        }
        self.rebuild_derived();
    }

    fn rebuild_derived(&mut self) {
        let ci_paths: Vec<(crate::services::plugin_scope::PluginId, Arc<str>)> = self
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

/// Starts an empty registry that rejects `core_routes`. `host` serves the
/// routes; without one they are only recorded.
pub(crate) async fn initialize_registry(
    core_routes: HashSet<RouteKey>,
    host: Option<Arc<dyn RouteHost>>,
) {
    api_route_registry()
        .write_owned()
        .await
        .reset(core_routes, host);
}

/// The registered routes ordered by path then method, with their
/// case-insensitive lookup.
pub(crate) async fn registered_routes() -> (Vec<RegisteredRoute>, CaseInsensitiveRouter) {
    let registry = api_route_registry().read_owned().await;
    (registry.snapshot(), registry.ci_router_snapshot())
}

pub(super) async fn freeze_registry() {
    api_route_registry().write_owned().await.freeze();
}
