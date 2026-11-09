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

/// Installs routes a [`RouteHost`] built.
pub(crate) type InstallRoutes = Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>;

/// Serves the registered plugin routes over HTTP. Routes are built apart from installing them, so
/// a change of the registered set can fail before anything it replaces is touched.
pub(crate) trait RouteHost: Send + Sync {
    fn build(
        &self,
        routes: Vec<RegisteredRoute>,
        case_insensitive: CaseInsensitiveRouter,
    ) -> BoxFuture<'_, Result<InstallRoutes>>;
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

    pub(super) fn register(
        &self,
        scope: &crate::plugins::state::VmScope,
        registration: ApiRouteRegistration,
    ) -> luau::runtime::Result<()> {
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
            scope
                .write_routes(&plugin)
                .await
                .register(RegisteredRoute {
                    key,
                    plugin_id: plugin.clone(),
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
        scope: &crate::plugins::state::VmScope,
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
            scope
                .write_routes(&plugin)
                .await
                .register(RegisteredRoute {
                    key,
                    plugin_id: plugin.clone(),
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

/// The plugin API routes of one runtime and their freeze state.
#[derive(Clone, Default)]
pub(crate) struct ApiRouteRegistries {
    registry: Arc<tokio::sync::RwLock<ApiRouteRegistry>>,
}

impl ApiRouteRegistries {
    pub(crate) fn registry(&self) -> Arc<tokio::sync::RwLock<ApiRouteRegistry>> {
        self.registry.clone()
    }

    pub(crate) async fn teardown_plugin(
        &self,
        plugin_id: &crate::services::plugin_scope::PluginId,
    ) {
        let Self { registry } = self;
        crate::services::plugin_scope::ScopedRegistry::from_shared(registry.clone())
            .teardown(plugin_id)
            .await;
    }

    pub(crate) async fn freeze(&self) {
        self.registry.write().await.freeze();
    }
}

/// How the routes of every runtime in a generation are served: the paths reserved for the server's
/// own routes and the host that installs the rest.
#[derive(Default)]
pub(crate) struct ApiRouteHost {
    core_routes: crate::services::state::SwapHandle<Arc<HashSet<RouteKey>>>,
    host: crate::services::state::SwapHandle<Option<Arc<dyn RouteHost>>>,
}

impl ApiRouteHost {
    pub(super) fn host(&self) -> Option<Arc<dyn RouteHost>> {
        self.host.get()
    }
}

/// The published scope's routes; empty before any scope is published.
pub(super) fn api_route_registry() -> Arc<tokio::sync::RwLock<ApiRouteRegistry>> {
    crate::plugins::STATE
        .scope()
        .map_or_else(Default::default, |scope| {
            scope.vm_scope.plugin.api.registry()
        })
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

    pub(crate) fn insert(&mut self, path: &str) -> Result<()> {
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
pub(crate) struct ApiRouteRegistry {
    routes: HashMap<crate::services::plugin_scope::PluginId, HashMap<RouteKey, RegisteredRoute>>,
    ci_router: CaseInsensitiveRouter,
    frozen: bool,
}

impl ApiRouteRegistry {
    fn reset(&mut self) {
        self.routes.clear();
        self.ci_router = CaseInsensitiveRouter::new();
        self.frozen = false;
    }

    pub(super) fn freeze(&mut self) {
        self.frozen = true;
    }

    fn iter_all(&self) -> impl Iterator<Item = &RegisteredRoute> {
        self.routes.values().flat_map(|bucket| bucket.values())
    }

    pub(crate) fn register(&mut self, route: RegisteredRoute) -> Result<()> {
        if self.frozen {
            bail!(
                "lyra/api route registry is frozen for plugin '{}'",
                route.plugin_id
            );
        }
        let core_routes = crate::plugins::STATE.generation().api.core_routes.get();
        if core_routes.contains(&route.key) {
            bail!(
                "plugin route conflicts with core route: {} {}",
                route.key.method,
                route.key.path
            );
        }
        self.check_route(&route, None)?;

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

    /// Fails if `route` conflicts with a registered route, leaving out `ignored`'s.
    fn check_route(
        &self,
        route: &RegisteredRoute,
        ignored: Option<&crate::services::plugin_scope::PluginId>,
    ) -> Result<()> {
        let others = || {
            self.iter_all()
                .filter(|existing| Some(&existing.plugin_id) != ignored)
        };
        if let Some(existing) = others().find(|existing| existing.key == route.key) {
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
        for existing in others() {
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
}

impl crate::services::plugin_scope::PluginScopedInner for ApiRouteRegistry {
    type Bucket = HashMap<RouteKey, RegisteredRoute>;

    fn take_bucket(&mut self, plugin_id: &crate::services::plugin_scope::PluginId) -> Self::Bucket {
        self.routes.remove(plugin_id).unwrap_or_default()
    }

    fn check_bucket(
        &self,
        plugin_id: &crate::services::plugin_scope::PluginId,
        new: &Self::Bucket,
    ) -> Result<()> {
        new.values()
            .try_for_each(|route| self.check_route(route, Some(plugin_id)))?;
        // The combined case-insensitive lookup, so `rebuild_derived` has nothing left to refuse.
        let mut combined = CaseInsensitiveRouter::new();
        for route in self
            .iter_all()
            .filter(|route| route.plugin_id != *plugin_id)
            .chain(new.values())
            .filter(|route| route.case_insensitive)
        {
            combined.insert(route.key.path.as_ref()).map_err(|err| {
                anyhow::anyhow!(
                    "plugin '{}' route '{}': {err}",
                    route.plugin_id,
                    route.key.path
                )
            })?;
        }
        Ok(())
    }

    fn replace_bucket(
        &mut self,
        plugin_id: &crate::services::plugin_scope::PluginId,
        new: Self::Bucket,
    ) {
        if new.is_empty() {
            self.routes.remove(plugin_id);
        } else {
            self.routes.insert(plugin_id.clone(), new);
        }
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
    let generation = crate::plugins::STATE.generation();
    generation.api.core_routes.replace(Arc::new(core_routes));
    generation.api.host.replace(host);
    api_route_registry().write_owned().await.reset();
}

/// `registries`' routes ordered by path then method, with their case-insensitive lookup.
pub(crate) async fn registered_routes(
    registries: &ApiRouteRegistries,
) -> (Vec<RegisteredRoute>, CaseInsensitiveRouter) {
    let registry = registries.registry.read().await;
    (registry.snapshot(), registry.ci_router_snapshot())
}
