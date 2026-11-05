// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use harmony_core::plugin::PluginManifest;

use crate::{
    plugins::{
        api::ApiRouteRegistries,
        bootstrap::PluginRuntime,
        lifecycle::PluginRegistries,
        settings::SettingsRegistries,
        supervisor::RuntimeCrashes,
    },
    services::{
        self,
        state::SwapHandle,
    },
};

pub(crate) type ManifestHandle = SwapHandle<Arc<[PluginManifest]>>;
pub(crate) type RuntimeHandle = SwapHandle<Option<PluginRuntime>>;

/// Everything the plugin layer derives from the current database/config
/// generation. It lives in [`services::state::GenerationState::layers`], so
/// the services reset swaps it wholesale with the rest of the generation. New
/// DB- or config-derived plugin registries belong here, never in module
/// statics.
#[derive(Default)]
pub(crate) struct GenerationState {
    pub(crate) manifests: ManifestHandle,
    pub(crate) runtime: RuntimeHandle,
    pub(crate) registries: PluginRegistries,
    pub(crate) settings: SettingsRegistries,
    pub(crate) api: ApiRouteRegistries,
    pub(crate) crashes: RuntimeCrashes,
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

    /// Publishes `runtime` to the plugin layer and, as the dispatcher for the
    /// handlers plugins register, to services: the only writer of both slots.
    /// Both go into one generation, the concrete runtime first, so a caller
    /// that sees the dispatcher also sees the runtime. From now on a crash of
    /// its thread is reported to the generation's supervisor.
    pub(crate) fn publish_runtime(&self, runtime: PluginRuntime) {
        let generation = services::STATE.generation();
        let layer = generation.layers.get::<GenerationState>();
        layer.crashes.observe(&runtime);
        layer.runtime.replace(Some(runtime.clone()));
        generation.plugin_dispatch.replace(Some(Arc::new(runtime)));
    }

    #[cfg(test)]
    pub(crate) fn clear_runtime(&self) {
        let generation = services::STATE.generation();
        generation.plugin_dispatch.replace(None);
        generation
            .layers
            .get::<GenerationState>()
            .runtime
            .replace(None);
    }
}

pub(crate) static STATE: PluginState = PluginState;
