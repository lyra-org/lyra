// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashMap,
    sync::Arc,
};

use anyhow::{
    Result,
    bail,
};
use tokio::sync::RwLock;

use super::super::options::OptionDeclaration;
use crate::plugins::lifecycle::{
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

/// Generation-owned mix state: registered mixers bucketed by plugin.
#[derive(Default)]
pub(crate) struct MixRegistries {
    registry: Arc<RwLock<MixRegistry>>,
}

pub(crate) fn mix_registry() -> Arc<RwLock<MixRegistry>> {
    crate::STATE.generation().mix.registry.clone()
}

/// Registered mixers, bucketed by the plugin that declared them.
/// `plugin_by_mixer` is the derived O(1) dispatch index rebuilt after
/// every teardown — the outer map is the source of truth.
#[derive(Default)]
pub(crate) struct MixRegistry {
    mixers: HashMap<PluginId, HashMap<String, MixerState>>,
    plugin_by_mixer: HashMap<String, PluginId>,
}

#[derive(Default)]
struct MixerState {
    handlers: HashMap<MixSeedType, u64>,
    options: Vec<OptionDeclaration>,
}

impl MixRegistry {
    /// Registers a mixer. Rejects duplicate ids across all plugin buckets.
    pub(crate) fn register(&mut self, plugin_id: PluginId, id: String) -> Result<()> {
        if let Some(existing) = self.plugin_by_mixer.get(&id) {
            bail!("mixer '{id}' already registered by plugin '{existing}'");
        }
        self.mixers
            .entry(plugin_id.clone())
            .or_default()
            .insert(id.clone(), MixerState::default());
        self.plugin_by_mixer.insert(id, plugin_id);
        Ok(())
    }

    fn state(&self, mixer_id: &str) -> Option<&MixerState> {
        let plugin_id = self.plugin_by_mixer.get(mixer_id)?;
        self.mixers.get(plugin_id)?.get(mixer_id)
    }

    fn state_mut(&mut self, mixer_id: &str) -> Option<&mut MixerState> {
        let plugin_id = self.plugin_by_mixer.get(mixer_id)?.clone();
        self.mixers.get_mut(&plugin_id)?.get_mut(mixer_id)
    }

    pub(crate) fn set_seed_callback(
        &mut self,
        mixer_id: &str,
        seed_type: MixSeedType,
        handler_id: u64,
    ) {
        if let Some(mixer) = self.state_mut(mixer_id) {
            mixer.handlers.insert(seed_type, handler_id);
        }
    }

    pub(crate) fn get_seed_callback(&self, mixer_id: &str, seed_type: MixSeedType) -> Option<u64> {
        self.state(mixer_id)
            .and_then(|p| p.handlers.get(&seed_type))
            .copied()
    }

    pub(crate) fn has_handler(&self, mixer_id: &str, seed_type: MixSeedType) -> bool {
        self.state(mixer_id)
            .is_some_and(|p| p.handlers.contains_key(&seed_type))
    }

    pub(crate) fn declare_option(
        &mut self,
        mixer_id: &str,
        option: OptionDeclaration,
    ) -> std::result::Result<(), String> {
        if let Some(mixer) = self.state_mut(mixer_id) {
            if mixer.options.iter().any(|o| o.name == option.name) {
                return Err(format!(
                    "option '{}' already declared on mixer '{}'",
                    option.name, mixer_id
                ));
            }
            mixer.options.push(option);
            Ok(())
        } else {
            Err(format!("mixer '{}' not registered", mixer_id))
        }
    }

    pub(crate) fn get_options(&self, mixer_id: &str) -> &[OptionDeclaration] {
        self.state(mixer_id)
            .map(|p| p.options.as_slice())
            .unwrap_or(&[])
    }
}

impl PluginScopedInner for MixRegistry {
    fn clear_bucket(&mut self, plugin_id: &PluginId) {
        self.mixers.remove(plugin_id);
    }

    fn rebuild_derived(&mut self) {
        self.plugin_by_mixer.clear();
        for (plugin_id, bucket) in &self.mixers {
            for mixer_id in bucket.keys() {
                self.plugin_by_mixer
                    .insert(mixer_id.clone(), plugin_id.clone());
            }
        }
    }
}

pub(crate) async fn teardown_plugin_mixers(plugin_id: &PluginId) {
    ScopedRegistry::from_shared(mix_registry())
        .teardown(plugin_id)
        .await;
}
