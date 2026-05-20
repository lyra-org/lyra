// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashSet,
    sync::Arc,
};

use harmony_core::{
    CapabilityId,
    CapabilityPolicy,
    ChunkOrigin,
};

pub(super) struct ManifestCapabilityPolicy {
    scopes_by_plugin: std::collections::HashMap<Arc<str>, HashSet<Arc<str>>>,
}

impl ManifestCapabilityPolicy {
    pub(super) fn from_manifests(manifests: Arc<[harmony_core::PluginManifest]>) -> Self {
        let scopes_by_plugin = manifests
            .iter()
            .map(|manifest| {
                let scopes = manifest
                    .scopes
                    .iter()
                    .map(|scope| Arc::<str>::from(scope.as_str()))
                    .collect::<HashSet<_>>();
                (Arc::<str>::from(manifest.id.as_str()), scopes)
            })
            .collect();

        Self { scopes_by_plugin }
    }
}

impl CapabilityPolicy for ManifestCapabilityPolicy {
    fn is_allowed(&self, origin: &ChunkOrigin, capability: &CapabilityId) -> bool {
        let Some(plugin_id) = origin.plugin.as_ref() else {
            return false;
        };
        self.scopes_by_plugin
            .get(plugin_id)
            .is_some_and(|scopes| scopes.contains(&capability.0))
    }
}
