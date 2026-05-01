// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        LazyLock,
    },
};

use anyhow::{
    Result,
    bail,
};
use tokio::sync::RwLock;

use super::super::options::OptionDeclaration;
use crate::plugins::lifecycle::{
    PluginFunctionHandle,
    PluginId,
    PluginScopedInner,
    ScopedRegistry,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum MixSeedType {
    Track,
    Release,
    Artist,
    Genre,
    Playlist,
    RecentListens,
}

pub(crate) static MIX_REGISTRY: LazyLock<Arc<RwLock<MixRegistry>>> =
    LazyLock::new(|| Arc::new(RwLock::new(MixRegistry::new())));

/// Registered mixers, bucketed by the plugin that declared them.
/// `plugin_by_mixer` is the derived O(1) dispatch index rebuilt after
/// every teardown — the outer map is the source of truth.
#[derive(Default)]
pub(crate) struct MixRegistry {
    providers: HashMap<PluginId, HashMap<String, MixProviderState>>,
    plugin_by_mixer: HashMap<String, PluginId>,
}

#[derive(Default)]
struct MixProviderState {
    handlers: HashMap<MixSeedType, PluginFunctionHandle>,
    options: Vec<OptionDeclaration>,
}

impl MixRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn clear(&mut self) {
        self.providers.clear();
        self.plugin_by_mixer.clear();
    }

    /// Registers a mixer. Rejects duplicate ids across all plugin buckets.
    pub(crate) fn register(&mut self, plugin_id: PluginId, id: String) -> Result<()> {
        if let Some(existing) = self.plugin_by_mixer.get(&id) {
            bail!("mixer '{id}' already registered by plugin '{existing}'");
        }
        self.providers
            .entry(plugin_id.clone())
            .or_default()
            .insert(id.clone(), MixProviderState::default());
        self.plugin_by_mixer.insert(id, plugin_id);
        Ok(())
    }

    fn state(&self, provider_id: &str) -> Option<&MixProviderState> {
        let plugin_id = self.plugin_by_mixer.get(provider_id)?;
        self.providers.get(plugin_id)?.get(provider_id)
    }

    fn state_mut(&mut self, provider_id: &str) -> Option<&mut MixProviderState> {
        let plugin_id = self.plugin_by_mixer.get(provider_id)?.clone();
        self.providers.get_mut(&plugin_id)?.get_mut(provider_id)
    }

    pub(crate) fn set_handler(
        &mut self,
        provider_id: &str,
        seed_type: MixSeedType,
        handler: PluginFunctionHandle,
    ) {
        if let Some(provider) = self.state_mut(provider_id) {
            provider.handlers.insert(seed_type, handler);
        }
    }

    pub(crate) fn get_handler(
        &self,
        provider_id: &str,
        seed_type: MixSeedType,
    ) -> Option<&PluginFunctionHandle> {
        self.state(provider_id)
            .and_then(|p| p.handlers.get(&seed_type))
    }

    pub(crate) fn has_handler(&self, provider_id: &str, seed_type: MixSeedType) -> bool {
        self.state(provider_id)
            .is_some_and(|p| p.handlers.contains_key(&seed_type))
    }

    pub(crate) fn declare_option(
        &mut self,
        provider_id: &str,
        option: OptionDeclaration,
    ) -> std::result::Result<(), String> {
        if let Some(provider) = self.state_mut(provider_id) {
            if provider.options.iter().any(|o| o.name == option.name) {
                return Err(format!(
                    "option '{}' already declared on mixer '{}'",
                    option.name, provider_id
                ));
            }
            provider.options.push(option);
            Ok(())
        } else {
            Err(format!("mixer '{}' not registered", provider_id))
        }
    }

    pub(crate) fn get_options(&self, provider_id: &str) -> &[OptionDeclaration] {
        self.state(provider_id)
            .map(|p| p.options.as_slice())
            .unwrap_or(&[])
    }
}

impl PluginScopedInner for MixRegistry {
    fn clear_bucket(&mut self, plugin_id: &PluginId) {
        self.providers.remove(plugin_id);
    }

    fn rebuild_derived(&mut self) {
        self.plugin_by_mixer.clear();
        for (plugin_id, bucket) in &self.providers {
            for mixer_id in bucket.keys() {
                self.plugin_by_mixer
                    .insert(mixer_id.clone(), plugin_id.clone());
            }
        }
    }
}

pub(crate) async fn reset_mix_registry_for_test() {
    MIX_REGISTRY.write().await.clear();
}

pub(crate) async fn teardown_plugin_mixers(plugin_id: &PluginId) {
    ScopedRegistry::from_shared(MIX_REGISTRY.clone())
        .teardown(plugin_id)
        .await;
}
