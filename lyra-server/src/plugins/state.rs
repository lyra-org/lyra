// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    rc::Rc,
    sync::{
        Arc,
        Mutex,
        PoisonError,
    },
};

use harmony_core::plugin::{
    PluginDiscoveryError,
    PluginManifest,
};
use harmony_luau as luau;
use tokio::sync::{
    OwnedRwLockWriteGuard,
    RwLock,
};
use tokio_util::sync::CancellationToken;

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
            BucketCommit,
            PluginId,
            RuntimeScope,
            ScopeRegistries,
        },
        providers::ProviderRegistry,
        state::SwapHandle,
    },
};

pub(crate) type ManifestHandle = SwapHandle<Arc<[PluginManifest]>>;
pub(crate) type DiscoveryErrorHandle = SwapHandle<Arc<[PluginDiscoveryError]>>;

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
    /// Why the runtime's discovery left plugins out.
    pub(crate) discovery_errors: DiscoveryErrorHandle,
    pub(crate) settings: SettingsRegistries,
    pub(crate) api: ApiRouteRegistries,
}

/// The plugin half of moving a restarted plugin's buckets into the live scope.
pub(crate) struct PluginScopeCommit {
    settings: BucketCommit<SettingsRegistry>,
    api: BucketCommit<ApiRouteRegistry>,
}

impl PluginScopeCommit {
    fn apply(&mut self) {
        let Self { settings, api } = self;
        settings.apply();
        api.apply();
    }
}

impl PluginScopeRegistries {
    async fn prepare_commit(
        &self,
        staged: &Self,
        plugin_id: &PluginId,
    ) -> anyhow::Result<PluginScopeCommit> {
        let Self {
            manifests: _,
            discovery_errors: _,
            settings,
            api,
        } = self;
        Ok(PluginScopeCommit {
            settings: BucketCommit::prepare(
                &settings.registry(),
                &staged.settings.registry(),
                plugin_id,
            )
            .await?,
            api: BucketCommit::prepare(&api.registry(), &staged.api.registry(), plugin_id).await?,
        })
    }

    /// Empties the plugin's buckets. A new field fails to compile here until it is addressed.
    pub(crate) async fn teardown_plugin(&self, plugin_id: &PluginId) {
        let Self {
            manifests: _,
            discovery_errors: _,
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

/// The registries a restarting plugin's new run fills, apart from the live ones.
struct Staging {
    plugin_id: PluginId,
    services: ScopeRegistries,
    plugin: PluginScopeRegistries,
}

/// What a runtime's VM holds of its own scope. Registration and in-VM calls read this, never the
/// published scope, which during a reload belongs to another VM.
#[derive(Clone, Default)]
pub(crate) struct VmScope {
    pub(crate) services: ScopeRegistries,
    pub(crate) plugin: PluginScopeRegistries,
    staging: Arc<Mutex<Option<Arc<Staging>>>>,
}

impl VmScope {
    pub(crate) fn of(vm: &luau::Vm) -> luau::runtime::Result<Rc<Self>> {
        vm.data().get::<Self>()
    }

    fn staging_of(&self, plugin_id: &PluginId) -> Option<Arc<Staging>> {
        self.staging
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .filter(|staging| staging.plugin_id == *plugin_id)
    }

    /// The registry `plugin_id` registers into: its staging registry while it is restarting, else
    /// the scope's.
    pub(crate) fn providers_for(&self, plugin_id: &PluginId) -> Arc<RwLock<ProviderRegistry>> {
        self.staging_of(plugin_id).map_or_else(
            || self.services.providers.registry(),
            |staging| staging.services.providers.registry(),
        )
    }

    pub(crate) fn mix_for(&self, plugin_id: &PluginId) -> Arc<RwLock<MixRegistry>> {
        self.staging_of(plugin_id).map_or_else(
            || self.services.mix.registry(),
            |staging| staging.services.mix.registry(),
        )
    }

    pub(crate) fn settings_for(&self, plugin_id: &PluginId) -> Arc<RwLock<SettingsRegistry>> {
        self.staging_of(plugin_id).map_or_else(
            || self.plugin.settings.registry(),
            |staging| staging.plugin.settings.registry(),
        )
    }

    pub(crate) fn routes_for(&self, plugin_id: &PluginId) -> Arc<RwLock<ApiRouteRegistry>> {
        self.staging_of(plugin_id).map_or_else(
            || self.plugin.api.registry(),
            |staging| staging.plugin.api.registry(),
        )
    }

    /// The providers as `caller` should see them from its own VM: while it is restarting, what
    /// its new run has staged in place of its old bucket, over the other plugins' live ones.
    pub(crate) async fn provider_view(
        &self,
        caller: Option<&PluginId>,
    ) -> Arc<RwLock<ProviderRegistry>> {
        self.overlaid_view(
            caller,
            self.services.providers.registry(),
            |staging| staging.services.providers.registry(),
            ProviderRegistry::overlaid,
        )
        .await
    }

    /// The mixers as `caller` should see them, as [`Self::provider_view`] gives the providers.
    pub(crate) async fn mix_view(&self, caller: Option<&PluginId>) -> Arc<RwLock<MixRegistry>> {
        self.overlaid_view(
            caller,
            self.services.mix.registry(),
            |staging| staging.services.mix.registry(),
            MixRegistry::overlaid,
        )
        .await
    }

    /// Locks staged before live, the order a commit takes them in. A staging committed or
    /// discarded while this waited holds nothing to overlay, so the live registry is the answer.
    async fn overlaid_view<T: Send + Sync + 'static>(
        &self,
        caller: Option<&PluginId>,
        live: Arc<RwLock<T>>,
        staged: fn(&Staging) -> Arc<RwLock<T>>,
        overlay: fn(&T, &PluginId, &T) -> T,
    ) -> Arc<RwLock<T>> {
        let Some(staging) = caller.and_then(|caller| self.staging_of(caller)) else {
            return live;
        };
        let staged = staged(&staging).read_owned().await;
        let still_staging = caller
            .and_then(|caller| self.staging_of(caller))
            .is_some_and(|current| Arc::ptr_eq(&current, &staging));
        if !still_staging {
            return live;
        }
        let view = overlay(&*live.read().await, &staging.plugin_id, &staged);
        Arc::new(RwLock::new(view))
    }

    /// Fires once what `plugin_id` registers now is live.
    pub(crate) fn published_for(&self, plugin_id: &PluginId) -> CancellationToken {
        self.staging_of(plugin_id).map_or_else(
            || self.services.published.clone(),
            |staging| staging.services.published.clone(),
        )
    }

    /// `target`'s write lock, once `target` still names what was locked: a registration waiting
    /// on a staging registry through its replacement belongs in the live one.
    async fn write_current<T: Send + Sync + 'static>(
        &self,
        target: impl Fn(&Self) -> Arc<RwLock<T>>,
    ) -> OwnedRwLockWriteGuard<T> {
        loop {
            let registry = target(self);
            let guard = registry.clone().write_owned().await;
            if Arc::ptr_eq(&registry, &target(self)) {
                return guard;
            }
        }
    }

    pub(crate) async fn write_providers(
        &self,
        plugin_id: &PluginId,
    ) -> OwnedRwLockWriteGuard<ProviderRegistry> {
        self.write_current(|scope| scope.providers_for(plugin_id))
            .await
    }

    pub(crate) async fn write_mix(
        &self,
        plugin_id: &PluginId,
    ) -> OwnedRwLockWriteGuard<MixRegistry> {
        self.write_current(|scope| scope.mix_for(plugin_id)).await
    }

    pub(crate) async fn write_routes(
        &self,
        plugin_id: &PluginId,
    ) -> OwnedRwLockWriteGuard<ApiRouteRegistry> {
        self.write_current(|scope| scope.routes_for(plugin_id))
            .await
    }

    pub(crate) async fn write_settings(
        &self,
        plugin_id: &PluginId,
    ) -> OwnedRwLockWriteGuard<SettingsRegistry> {
        self.write_current(|scope| scope.settings_for(plugin_id))
            .await
    }

    pub(crate) async fn teardown_plugin(&self, plugin_id: &PluginId) {
        let Self {
            services,
            plugin,
            staging: _,
        } = self;
        services.teardown_plugin(plugin_id).await;
        plugin.teardown_plugin(plugin_id).await;
    }

    /// From now on `plugin_id`'s run registers aside from the live registries.
    pub(crate) fn begin_staging(&self, plugin_id: PluginId) {
        *self.staging.lock().unwrap_or_else(PoisonError::into_inner) = Some(Arc::new(Staging {
            plugin_id,
            services: Default::default(),
            plugin: Default::default(),
        }));
    }

    /// Replaces the plugin's bucket in every live registry with what its run staged, the one
    /// publication of a restart. Conflicts with other plugins can only be seen here, and fail
    /// the whole replacement.
    pub(crate) async fn commit_staging(&self) -> anyhow::Result<()> {
        let staging = self
            .staging
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let Some(staging) = staging else {
            return Ok(());
        };
        let Staging {
            plugin_id,
            services,
            plugin,
        } = &*staging;
        // Every lock is taken before anything applies and held until everything has, in the one
        // order the two halves share: providers, mixers, settings, routes.
        let mut services_commit = self.services.prepare_commit(services, plugin_id).await?;
        let mut plugin_commit = self.plugin.prepare_commit(plugin, plugin_id).await?;
        // What this run added is about to be visible, and stays quiesced until the plugin reopens.
        services::STATE
            .generation()
            .extend_transition(services_commit.targets());
        // Before the locks are released, so a registration waiting on them re-reads its target.
        self.discard_staging();
        services_commit.apply();
        plugin_commit.apply();
        drop((services_commit, plugin_commit));
        services.published.cancel();
        Ok(())
    }

    pub(crate) fn discard_staging(&self) {
        self.staging
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
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

    /// What the published runtime's discovery loaded and what it left out.
    pub(crate) fn discovered(&self) -> (Arc<[PluginManifest]>, Arc<[PluginDiscoveryError]>) {
        self.scope().map_or_else(
            || (Arc::from([]), Arc::from([])),
            |scope| {
                let plugin = &scope.vm_scope.plugin;
                (plugin.manifests.get(), plugin.discovery_errors.get())
            },
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
