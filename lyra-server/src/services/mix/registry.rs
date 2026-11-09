// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        HashMap,
        HashSet,
    },
    sync::Arc,
    time::Duration,
};

use anyhow::{
    Result,
    bail,
};
use tokio::sync::RwLock;

use super::super::options::OptionDeclaration;
use crate::services::plugin_dispatch::PluginCallbackHandle;
use crate::services::plugin_scope::{
    PluginId,
    PluginScopedInner,
    ScopedRegistry,
};

/// The entity types a mix can be seeded from.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize)]
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub(crate) enum MixSeedType {
    Track,
    Release,
    Artist,
    Genre,
    Playlist,
}

impl MixSeedType {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Track => "track",
            Self::Release => "release",
            Self::Artist => "artist",
            Self::Genre => "genre",
            Self::Playlist => "playlist",
        }
    }
}

/// How a mix relates its seeds: a blend of tracks like them, or a path from
/// the first seed to the second.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub(crate) enum MixShape {
    #[default]
    Blend,
    Path,
}

impl MixShape {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Blend => "blend",
            Self::Path => "path",
        }
    }
}

/// What a mixer's handler for one shape accepts.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MixHandlerSpec {
    pub(crate) seed_types: Vec<MixSeedType>,
    /// The most explicit seeds it takes; history seeds fill what is left.
    pub(crate) max_seeds: usize,
    /// Whether it takes excluded seeds, to mix away from.
    pub(crate) exclude: bool,
    pub(crate) query: bool,
}

/// What a request needs from the handler that serves it. Only the explicit seeds count; a
/// request with history needs track seeds too.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MixRequirements {
    pub(crate) seed_types: Vec<MixSeedType>,
    pub(crate) seed_count: usize,
    /// Whether any explicit seed is excluded.
    pub(crate) exclude: bool,
    pub(crate) query: bool,
}

impl MixHandlerSpec {
    pub(crate) fn covers(&self, requirements: &MixRequirements) -> bool {
        requirements.seed_count <= self.max_seeds
            && requirements
                .seed_types
                .iter()
                .all(|seed_type| self.seed_types.contains(seed_type))
            && (!requirements.exclude || self.exclude)
            && (!requirements.query || self.query)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MixHandlerRegistration {
    pub(crate) spec: MixHandlerSpec,
    pub(crate) timeout: Duration,
    pub(crate) handler: PluginCallbackHandle,
}

/// The mix state of one plugin runtime: registered mixers bucketed by plugin.
#[derive(Clone, Default)]
pub(crate) struct MixRegistries {
    registry: Arc<RwLock<MixRegistry>>,
}

impl MixRegistries {
    pub(crate) fn registry(&self) -> Arc<RwLock<MixRegistry>> {
        self.registry.clone()
    }

    pub(crate) async fn teardown_plugin(&self, plugin_id: &PluginId) {
        let Self { registry } = self;
        ScopedRegistry::from_shared(registry.clone())
            .teardown(plugin_id)
            .await;
    }
}

/// The published scope's registry; an empty one before any scope is published.
pub(crate) fn mix_registry() -> Arc<RwLock<MixRegistry>> {
    crate::services::STATE
        .generation()
        .latest()
        .map_or_else(Default::default, |scope| scope.mix().registry())
}

/// Registered mixers, bucketed by the plugin that declared them.
/// `plugin_by_mixer` is the derived O(1) dispatch index rebuilt after
/// every teardown — the outer map is the source of truth.
#[derive(Default)]
pub(crate) struct MixRegistry {
    mixers: HashMap<PluginId, HashMap<String, MixerState>>,
    plugin_by_mixer: HashMap<String, PluginId>,
}

#[derive(Clone, Default)]
pub(crate) struct MixerState {
    handlers: HashMap<MixShape, MixHandlerRegistration>,
    options: Vec<OptionDeclaration>,
}

impl MixRegistry {
    /// Registers a mixer. Rejects duplicate ids across all plugin buckets, and ids with a `.`,
    /// which separates a mixer's id from its option names in `<mixer_id>.<name>`.
    pub(crate) fn register(&mut self, plugin_id: PluginId, id: String) -> Result<()> {
        if id.is_empty() || id.contains('.') {
            bail!("mixer id '{id}' must be non-empty and contain no '.'");
        }
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

    /// A read-only view of this registry with `plugin_id`'s bucket replaced by `staged`'s.
    pub(crate) fn overlaid(&self, plugin_id: &PluginId, staged: &Self) -> Self {
        let mut mixers = self
            .mixers
            .iter()
            .filter(|(owner, _)| *owner != plugin_id)
            .map(|(owner, bucket)| (owner.clone(), bucket.clone()))
            .collect::<HashMap<_, _>>();
        if let Some(bucket) = staged.mixers.get(plugin_id) {
            mixers.insert(plugin_id.clone(), bucket.clone());
        }
        let mut view = Self {
            mixers,
            ..Self::default()
        };
        view.rebuild_derived();
        view
    }

    pub(crate) fn mixer_ids(&self, plugin_id: &PluginId) -> HashSet<String> {
        self.mixers
            .get(plugin_id)
            .map(|bucket| bucket.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub(crate) fn is_registered(&self, mixer_id: &str) -> bool {
        self.plugin_by_mixer.contains_key(mixer_id)
    }

    pub(crate) fn plugin_id(&self, mixer_id: &str) -> Option<&PluginId> {
        self.plugin_by_mixer.get(mixer_id)
    }

    /// Sets the mixer's handler for `shape`, returning the one it replaces.
    pub(crate) fn set_handler(
        &mut self,
        mixer_id: &str,
        shape: MixShape,
        registration: MixHandlerRegistration,
    ) -> Result<Option<PluginCallbackHandle>> {
        let Some(mixer) = self.state_mut(mixer_id) else {
            bail!("mixer '{mixer_id}' not registered");
        };
        Ok(mixer
            .handlers
            .insert(shape, registration)
            .map(|replaced| replaced.handler))
    }

    pub(crate) fn handler(
        &self,
        mixer_id: &str,
        shape: MixShape,
    ) -> Option<&MixHandlerRegistration> {
        self.state(mixer_id)?.handlers.get(&shape)
    }

    pub(crate) fn declare_option(
        &mut self,
        mixer_id: &str,
        option: OptionDeclaration,
    ) -> std::result::Result<(), String> {
        if option.name.is_empty() || option.name.contains('.') {
            return Err(format!(
                "option name '{}' must be non-empty and contain no '.'",
                option.name
            ));
        }
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
    type Bucket = HashMap<String, MixerState>;

    fn take_bucket(&mut self, plugin_id: &PluginId) -> Self::Bucket {
        self.mixers.remove(plugin_id).unwrap_or_default()
    }

    fn check_bucket(&self, plugin_id: &PluginId, new: &Self::Bucket) -> Result<()> {
        for mixer_id in new.keys() {
            if let Some(existing) = self
                .plugin_by_mixer
                .get(mixer_id)
                .filter(|existing| *existing != plugin_id)
            {
                bail!("mixer '{mixer_id}' already registered by plugin '{existing}'");
            }
        }
        Ok(())
    }

    fn replace_bucket(&mut self, plugin_id: &PluginId, new: Self::Bucket) {
        if new.is_empty() {
            self.mixers.remove(plugin_id);
        } else {
            self.mixers.insert(plugin_id.clone(), new);
        }
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

/// Empties the plugin's bucket in the published scope.
#[cfg(test)]
pub(crate) async fn teardown_plugin_mixers(plugin_id: &PluginId) {
    let scope = crate::services::STATE
        .generation()
        .latest()
        .expect("published test scope");
    scope.mix().teardown_plugin(plugin_id).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(seed_types: &[MixSeedType], max_seeds: usize) -> MixHandlerSpec {
        MixHandlerSpec {
            seed_types: seed_types.to_vec(),
            max_seeds,
            exclude: false,
            query: false,
        }
    }

    fn needs(seed_types: &[MixSeedType], seed_count: usize) -> MixRequirements {
        MixRequirements {
            seed_types: seed_types.to_vec(),
            seed_count,
            exclude: false,
            query: false,
        }
    }

    #[test]
    fn a_spec_covers_only_what_it_declares() {
        let tracks = spec(&[MixSeedType::Track, MixSeedType::Release], 3);
        assert!(tracks.covers(&needs(&[MixSeedType::Track], 3)));
        assert!(tracks.covers(&needs(&[MixSeedType::Track, MixSeedType::Release], 2)));
        assert!(!tracks.covers(&needs(&[MixSeedType::Track], 4)));
        assert!(!tracks.covers(&needs(&[MixSeedType::Genre], 1)));
        assert!(!tracks.covers(&MixRequirements {
            exclude: true,
            ..needs(&[MixSeedType::Track], 2)
        }));
        assert!(!tracks.covers(&MixRequirements {
            query: true,
            ..needs(&[], 0)
        }));

        let open = MixHandlerSpec {
            exclude: true,
            query: true,
            ..spec(&[MixSeedType::Track], 2)
        };
        assert!(open.covers(&MixRequirements {
            exclude: true,
            query: true,
            ..needs(&[MixSeedType::Track], 2)
        }));
    }

    #[test]
    fn a_registered_handler_replaces_the_shapes_previous_one() -> Result<()> {
        let mut registry = MixRegistry::default();
        registry.register(PluginId::new("demo".to_string())?, "demo-mixer".to_string())?;
        let handle = |handler_id| PluginCallbackHandle {
            vm_id: 1,
            handler_id,
        };
        let registration = |handler_id| MixHandlerRegistration {
            spec: spec(&[MixSeedType::Track], 1),
            timeout: Duration::from_secs(1),
            handler: handle(handler_id),
        };

        assert_eq!(
            registry.set_handler("demo-mixer", MixShape::Blend, registration(1))?,
            None
        );
        assert_eq!(
            registry.set_handler("demo-mixer", MixShape::Blend, registration(2))?,
            Some(handle(1))
        );
        assert_eq!(
            registry
                .handler("demo-mixer", MixShape::Blend)
                .map(|registration| registration.handler),
            Some(handle(2))
        );
        assert!(registry.handler("demo-mixer", MixShape::Path).is_none());
        assert!(
            registry
                .set_handler("missing", MixShape::Blend, registration(3))
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn mixer_ids_and_option_names_keep_dots_for_option_keys() -> Result<()> {
        let mut registry = MixRegistry::default();
        let plugin = || PluginId::new("demo".to_string());
        assert!(
            registry
                .register(plugin()?, "demo.mixer".to_string())
                .is_err()
        );
        assert!(registry.register(plugin()?, String::new()).is_err());
        registry.register(plugin()?, "demo-mixer".to_string())?;
        let option = |name: &str| OptionDeclaration {
            name: name.to_string(),
            label: name.to_string(),
            option_type: crate::services::options::OptionType::Boolean,
            default: serde_json::Value::Null,
            requires_settings: Vec::new(),
        };
        assert!(
            registry
                .declare_option("demo-mixer", option("a.b"))
                .is_err()
        );
        assert!(registry.declare_option("demo-mixer", option("ab")).is_ok());
        Ok(())
    }
}
