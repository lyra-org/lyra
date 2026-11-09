// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashMap,
    sync::Arc,
};

use anyhow::bail;
use tokio::sync::RwLock;

use crate::services::plugin_scope::{
    PluginId,
    PluginScopedInner,
    ScopedRegistry,
};

use super::Schema;

/// The plugin settings of one runtime: declared schemas plus freeze state.
#[derive(Clone, Default)]
pub(crate) struct SettingsRegistries {
    registry: Arc<RwLock<Registry>>,
}

impl SettingsRegistries {
    pub(crate) fn registry(&self) -> Arc<RwLock<Registry>> {
        self.registry.clone()
    }

    pub(crate) async fn teardown_plugin(&self, plugin_id: &PluginId) {
        let Self { registry } = self;
        ScopedRegistry::from_shared(registry.clone())
            .teardown(plugin_id)
            .await;
    }

    pub(crate) async fn freeze(&self) {
        self.registry.write().await.freeze();
    }
}

/// The published scope's settings; empty before any scope is published.
pub(crate) fn settings_registry() -> Arc<RwLock<Registry>> {
    crate::plugins::STATE
        .scope()
        .map_or_else(Default::default, |scope| {
            scope.vm_scope.plugin.settings.registry()
        })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SettingsScope {
    Global,
    User,
}

/// Plugin settings schemas, bucketed by owning plugin.
#[derive(Default)]
pub(crate) struct Registry {
    schemas: HashMap<PluginId, HashMap<SettingsScope, Schema>>,
    frozen: bool,
}

impl Registry {
    #[cfg(test)]
    pub(crate) fn clear(&mut self) {
        self.schemas.clear();
        self.frozen = false;
    }

    pub(crate) fn freeze(&mut self) {
        self.frozen = true;
    }

    pub(crate) fn is_frozen(&self) -> bool {
        self.frozen
    }

    pub(crate) fn register_schema(
        &mut self,
        plugin_id: PluginId,
        scope: SettingsScope,
        schema: Schema,
    ) -> anyhow::Result<()> {
        if self.frozen {
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
    type Bucket = HashMap<SettingsScope, Schema>;

    fn take_bucket(&mut self, plugin_id: &PluginId) -> Self::Bucket {
        self.schemas.remove(plugin_id).unwrap_or_default()
    }

    fn check_bucket(&self, _plugin_id: &PluginId, _new: &Self::Bucket) -> anyhow::Result<()> {
        Ok(())
    }

    fn replace_bucket(&mut self, plugin_id: &PluginId, new: Self::Bucket) {
        if new.is_empty() {
            self.schemas.remove(plugin_id);
        } else {
            self.schemas.insert(plugin_id.clone(), new);
        }
    }

    fn rebuild_derived(&mut self) {
        // No derived state: `get_schema` reads the bucket directly.
    }
}

#[cfg(test)]
pub(crate) async fn initialize_registry() {
    settings_registry().write_owned().await.clear();
}

#[cfg(test)]
pub(crate) async fn freeze_registry() {
    settings_registry().write_owned().await.freeze();
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
}
