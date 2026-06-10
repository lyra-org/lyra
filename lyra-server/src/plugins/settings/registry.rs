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
};

use anyhow::bail;
use tokio::sync::RwLock;

use crate::plugins::lifecycle::{
    PluginId,
    PluginScopedInner,
    ScopedRegistry,
};

use super::Schema;

/// Generation-owned plugin settings state: declared schemas plus freeze state.
#[derive(Default)]
pub(crate) struct SettingsRegistries {
    registry: Arc<RwLock<Registry>>,
}

pub(crate) fn settings_registry() -> Arc<RwLock<Registry>> {
    crate::STATE.generation().plugin_settings.registry.clone()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SettingsScope {
    Global,
    User,
}

/// Exemptions is the per-plugin escape hatch a restart flow uses to re-run
/// a single plugin's `declare_settings` without letting the rest of the
/// process sneak past the freeze.
#[derive(Default)]
enum FreezeState {
    #[default]
    Open,
    Frozen {
        exemptions: HashSet<PluginId>,
    },
}

/// Plugin settings schemas, bucketed by owning plugin.
#[derive(Default)]
pub(crate) struct Registry {
    schemas: HashMap<PluginId, HashMap<SettingsScope, Schema>>,
    state: FreezeState,
}

impl Registry {
    pub(crate) fn clear(&mut self) {
        self.schemas.clear();
        self.state = FreezeState::Open;
    }

    pub(crate) fn freeze(&mut self) {
        self.state = FreezeState::Frozen {
            exemptions: HashSet::new(),
        };
    }

    /// True iff writes for the given plugin would be rejected.
    pub(crate) fn is_frozen_for_plugin(&self, plugin_id: &PluginId) -> bool {
        match &self.state {
            FreezeState::Open => false,
            FreezeState::Frozen { exemptions } => !exemptions.contains(plugin_id),
        }
    }

    /// Add a plugin to the freeze exemptions. No-op when `Open`.
    pub(crate) fn unfreeze_plugin(&mut self, plugin_id: PluginId) {
        if let FreezeState::Frozen { exemptions } = &mut self.state {
            exemptions.insert(plugin_id);
        }
    }

    /// Remove a plugin from the freeze exemptions. No-op when `Open`.
    pub(crate) fn refreeze_plugin(&mut self, plugin_id: &PluginId) {
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

    pub(crate) fn register_schema(
        &mut self,
        plugin_id: PluginId,
        scope: SettingsScope,
        schema: Schema,
    ) -> anyhow::Result<()> {
        if !self.writes_allowed(&plugin_id) {
            bail!("lyra/plugins settings registry is frozen for plugin '{plugin_id}'");
        }
        let scope_label = match scope {
            SettingsScope::Global => "settings",
            SettingsScope::User => "user settings",
        };
        let bucket = self.schemas.entry(plugin_id.clone()).or_default();
        if bucket.contains_key(&scope) {
            bail!("plugin {scope_label} already declared for plugin '{plugin_id}'");
        }
        bucket.insert(scope, schema);
        Ok(())
    }

    pub(crate) fn get_schema(&self, plugin_id: &str, scope: SettingsScope) -> Option<&Schema> {
        self.schemas
            .get(plugin_id)
            .and_then(|bucket| bucket.get(&scope))
    }
}

impl PluginScopedInner for Registry {
    fn clear_bucket(&mut self, plugin_id: &PluginId) {
        self.schemas.remove(plugin_id);
        // Exemption lifetime is tied to bucket presence; if the caller
        // forgets `refreeze_plugin` on an error path, teardown is what
        // prevents a permanent writable window.
        if let FreezeState::Frozen { exemptions } = &mut self.state {
            exemptions.remove(plugin_id);
        }
    }

    fn rebuild_derived(&mut self) {
        // No derived state: `get_schema` reads the bucket directly.
    }
}

pub(crate) async fn initialize_registry() {
    settings_registry().write_owned().await.clear();
}

pub(crate) async fn freeze_registry() {
    settings_registry().write_owned().await.freeze();
}

pub(crate) async fn unfreeze_plugin_settings(plugin_id: PluginId) {
    settings_registry()
        .write_owned()
        .await
        .unfreeze_plugin(plugin_id);
}

pub(crate) async fn refreeze_plugin_settings(plugin_id: &PluginId) {
    settings_registry()
        .write_owned()
        .await
        .refreeze_plugin(plugin_id);
}

pub(crate) async fn teardown_plugin_settings(plugin_id: &PluginId) {
    ScopedRegistry::from_shared(settings_registry())
        .teardown(plugin_id)
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn demo_id() -> PluginId {
        PluginId::new("demo").unwrap()
    }

    #[test]
    fn register_schema_rejects_duplicate_plugins() {
        let mut registry = Registry::default();
        registry
            .register_schema(
                demo_id(),
                SettingsScope::Global,
                Schema { groups: Vec::new() },
            )
            .expect("first schema registration should succeed");

        let error = registry
            .register_schema(
                demo_id(),
                SettingsScope::Global,
                Schema { groups: Vec::new() },
            )
            .expect_err("duplicate plugin settings schema should be rejected");
        assert!(error.to_string().contains("already declared"));
    }

    #[test]
    fn register_schema_allows_same_plugin_with_different_scopes() {
        let mut registry = Registry::default();
        registry
            .register_schema(
                demo_id(),
                SettingsScope::Global,
                Schema { groups: Vec::new() },
            )
            .expect("global schema registration should succeed");

        registry
            .register_schema(
                demo_id(),
                SettingsScope::User,
                Schema { groups: Vec::new() },
            )
            .expect("user schema registration for same plugin should succeed");
    }

    #[test]
    fn register_schema_rejects_writes_when_registry_is_frozen() {
        let mut registry = Registry::default();
        registry.freeze();

        let error = registry
            .register_schema(
                demo_id(),
                SettingsScope::Global,
                Schema { groups: Vec::new() },
            )
            .expect_err("frozen registry should reject new schemas");
        assert!(error.to_string().contains("frozen"));
    }

    #[test]
    fn unfreeze_plugin_permits_writes_for_that_plugin_only() {
        let mut registry = Registry::default();
        registry.freeze();
        registry.unfreeze_plugin(demo_id());

        registry
            .register_schema(
                demo_id(),
                SettingsScope::Global,
                Schema { groups: Vec::new() },
            )
            .expect("exempt plugin should be able to register while globally frozen");

        let other = PluginId::new("other").unwrap();
        let error = registry
            .register_schema(other, SettingsScope::Global, Schema { groups: Vec::new() })
            .expect_err("non-exempt plugin must still be rejected");
        assert!(error.to_string().contains("frozen"));
    }

    #[test]
    fn refreeze_plugin_restores_rejection() {
        let mut registry = Registry::default();
        registry.freeze();
        registry.unfreeze_plugin(demo_id());
        registry.refreeze_plugin(&demo_id());

        let error = registry
            .register_schema(
                demo_id(),
                SettingsScope::Global,
                Schema { groups: Vec::new() },
            )
            .expect_err("re-frozen plugin should be rejected again");
        assert!(error.to_string().contains("frozen"));
    }
}
