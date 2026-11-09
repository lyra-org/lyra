// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    rc::Rc,
    sync::Arc,
};

use harmony_core::plugin::PluginManifest;
use harmony_luau as luau;
use tokio::sync::RwLock;

use crate::{
    plugins::{
        api::{
            ApiRouteHost,
            ApiRouteRegistries,
            ApiRouteRegistry,
        },
        bootstrap::PluginRuntime,
        lifecycle::PluginRegistries,
        settings::{
            Registry as SettingsRegistry,
            SettingsRegistries,
        },
        supervisor::RuntimeCrashes,
    },
    services::{
        self,
        mix::MixRegistry,
        plugin_scope::{
            PluginId,
            RuntimeScope,
            ScopeRegistries,
        },
        providers::ProviderRegistry,
        state::SwapHandle,
    },
};

pub(crate) type ManifestHandle = SwapHandle<Arc<[PluginManifest]>>;

/// What the plugin layer keeps per database/config generation, across runtimes. It lives in
/// [`services::state::GenerationState::layers`], so the services reset swaps it wholesale with the
/// rest of the generation. Anything derived from one runtime belongs in
/// [`PluginScopeRegistries`] instead.
#[derive(Default)]
pub(crate) struct GenerationState {
    pub(crate) registries: PluginRegistries,
    pub(crate) api: ApiRouteHost,
    pub(crate) crashes: RuntimeCrashes,
}

/// The plugin layer's registries of one runtime, built before its VM.
#[derive(Clone, Default)]
pub(crate) struct PluginScopeRegistries {
    pub(crate) manifests: ManifestHandle,
    pub(crate) settings: SettingsRegistries,
    pub(crate) api: ApiRouteRegistries,
}

impl PluginScopeRegistries {
    /// Empties the plugin's buckets. A new field fails to compile here until it is addressed.
    pub(crate) async fn teardown_plugin(&self, plugin_id: &PluginId) {
        let Self {
            manifests: _,
            settings,
            api,
        } = self;
        api.teardown_plugin(plugin_id).await;
        settings.teardown_plugin(plugin_id).await;
    }

    /// Rejects later registrations of routes and settings.
    pub(crate) async fn freeze(&self) {
        self.api.freeze().await;
        self.settings.freeze().await;
    }
}

/// What a runtime's VM holds of its own scope. Registration and in-VM calls read this, never the
/// published scope, which during a reload belongs to another VM.
#[derive(Clone, Default)]
pub(crate) struct VmScope {
    pub(crate) services: ScopeRegistries,
    pub(crate) plugin: PluginScopeRegistries,
}

impl VmScope {
    pub(crate) fn of(vm: &luau::Vm) -> luau::runtime::Result<Rc<Self>> {
        vm.data().get::<Self>()
    }

    pub(crate) fn providers_for(&self, _plugin_id: &PluginId) -> Arc<RwLock<ProviderRegistry>> {
        self.services.providers.registry()
    }

    pub(crate) fn mix_for(&self, _plugin_id: &PluginId) -> Arc<RwLock<MixRegistry>> {
        self.services.mix.registry()
    }

    pub(crate) fn settings_for(&self, _plugin_id: &PluginId) -> Arc<RwLock<SettingsRegistry>> {
        self.plugin.settings.registry()
    }

    pub(crate) fn routes_for(&self, _plugin_id: &PluginId) -> Arc<RwLock<ApiRouteRegistry>> {
        self.plugin.api.registry()
    }

    pub(crate) async fn teardown_plugin(&self, plugin_id: &PluginId) {
        let Self { services, plugin } = self;
        services.teardown_plugin(plugin_id).await;
        plugin.teardown_plugin(plugin_id).await;
    }
}

/// The plugin layer's half of a published [`RuntimeScope`].
pub(crate) struct RuntimeScopeState {
    pub(crate) runtime: Option<PluginRuntime>,
    pub(crate) vm_scope: VmScope,
}

/// The plugin layer's view of the application state.
pub(crate) struct PluginState;

impl PluginState {
    /// Snapshot of the current generation. Callers that must not observe a
    /// mid-operation reset should hold one snapshot for the whole operation.
    /// Panics before the services state is initialized.
    pub(crate) fn generation(&self) -> Arc<GenerationState> {
        services::STATE.generation().layers.get()
    }

    /// The plugin layer's half of the published scope.
    pub(crate) fn scope(&self) -> Option<Arc<RuntimeScopeState>> {
        let layer = services::STATE
            .generation()
            .latest()?
            .plugin_layer()?
            .clone();
        Some(
            layer
                .downcast()
                .unwrap_or_else(|_| unreachable!("the plugin layer publishes its own state")),
        )
    }

    /// The published scope's registries, shared with its VM.
    pub(crate) fn vm_scope(&self) -> Option<VmScope> {
        let scope = self.scope()?;
        let RuntimeScopeState {
            runtime: _,
            vm_scope,
        } = &*scope;
        Some(vm_scope.clone())
    }

    pub(crate) fn runtime(&self) -> Option<PluginRuntime> {
        self.scope()?.runtime.clone()
    }

    pub(crate) fn manifests(&self) -> Arc<[PluginManifest]> {
        self.scope().map_or_else(
            || Arc::from([]),
            |scope| scope.vm_scope.plugin.manifests.get(),
        )
    }

    /// Publishes `runtime` with its registries, as one scope, to `generation`. From now on a
    /// crash of its thread is reported to the generation's supervisor.
    pub(crate) fn publish_runtime(
        &self,
        generation: &services::state::GenerationState,
        runtime: PluginRuntime,
    ) {
        generation
            .layers
            .get::<GenerationState>()
            .crashes
            .observe(&runtime);
        let vm_scope = runtime.scope().clone();
        let registries = vm_scope.services.clone();
        let published = registries.published.clone();
        let scope = RuntimeScope::new(
            Arc::new(runtime.clone()),
            registries,
            Arc::new(RuntimeScopeState {
                runtime: Some(runtime),
                vm_scope,
            }),
        );
        generation.runtime.replace(Some(Arc::new(scope)));
        published.cancel();
    }

    /// Publishes a scope with no runtime behind it, holding `scope`'s registries.
    #[cfg(test)]
    pub(crate) fn publish_idle(&self, vm_scope: VmScope) {
        let registries = vm_scope.services.clone();
        services::STATE
            .generation()
            .runtime
            .replace(Some(Arc::new(RuntimeScope::new(
                Arc::new(services::testing::IdleDispatch),
                registries,
                Arc::new(RuntimeScopeState {
                    runtime: None,
                    vm_scope,
                }),
            ))));
    }

    #[cfg(test)]
    pub(crate) fn clear_runtime(&self) {
        self.publish_idle(self.vm_scope().unwrap_or_default());
    }
}

pub(crate) static STATE: PluginState = PluginState;
