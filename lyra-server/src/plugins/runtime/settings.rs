// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        HashMap,
        HashSet,
    },
    sync::{
        Arc,
        LazyLock,
    },
};

use anyhow::bail;
use tokio::sync::RwLock;

use crate::plugins::lifecycle::{
    PluginId,
    PluginScopedInner,
    ScopedRegistry,
};

pub(crate) static REGISTRY: LazyLock<Arc<RwLock<Registry>>> =
    LazyLock::new(|| Arc::new(RwLock::new(Registry::new())));

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

#[derive(Clone, Debug)]
pub(crate) struct Schema {
    pub(crate) groups: Vec<FieldGroupDefinition>,
}

#[derive(Clone, Debug)]
pub(crate) struct FieldGroupDefinition {
    pub(crate) id: String,
    pub(crate) label: String,
    pub(crate) fields: Vec<FieldDefinition>,
}

#[derive(Clone, Debug)]
pub(crate) struct ChoiceOption {
    pub(crate) value: String,
    pub(crate) label: String,
    pub(crate) description: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct FieldProps {
    pub(crate) label: String,
    pub(crate) description: Option<String>,
    pub(crate) required: bool,
    pub(crate) default_value: Option<serde_json::Value>,
}

#[derive(Clone, Debug)]
pub(crate) enum FieldDefinition {
    String {
        key: String,
        props: FieldProps,
    },
    Number {
        key: String,
        props: FieldProps,
        min: Option<f64>,
        max: Option<f64>,
    },
    Bool {
        key: String,
        props: FieldProps,
    },
    Choice {
        key: String,
        props: FieldProps,
        options: Vec<ChoiceOption>,
    },
}

impl FieldDefinition {
    pub(crate) fn key(&self) -> &str {
        match self {
            Self::String { key, .. }
            | Self::Number { key, .. }
            | Self::Bool { key, .. }
            | Self::Choice { key, .. } => key,
        }
    }

    pub(crate) fn props(&self) -> &FieldProps {
        match self {
            Self::String { props, .. }
            | Self::Number { props, .. }
            | Self::Bool { props, .. }
            | Self::Choice { props, .. } => props,
        }
    }

    pub(crate) fn validate_value(&self, value: &serde_json::Value) -> anyhow::Result<()> {
        match self {
            Self::String { key, props } => validate_string_value(key, props, value),
            Self::Number {
                key,
                props,
                min,
                max,
            } => validate_number_value(key, props, value, *min, *max),
            Self::Bool { key, props } => validate_bool_value(key, props, value),
            Self::Choice {
                key,
                props,
                options,
            } => validate_choice_value(key, props, options, value),
        }
    }
}

fn validate_nullable_value(
    key: &str,
    props: &FieldProps,
    value: &serde_json::Value,
) -> anyhow::Result<bool> {
    if value.is_null() {
        if props.required && props.default_value.is_none() {
            bail!("setting '{key}' is required");
        }
        return Ok(true);
    }

    Ok(false)
}

fn validate_string_value(
    key: &str,
    props: &FieldProps,
    value: &serde_json::Value,
) -> anyhow::Result<()> {
    if validate_nullable_value(key, props, value)? {
        return Ok(());
    }
    if !value.is_string() {
        bail!("setting '{key}' must be a string or null");
    }
    Ok(())
}

fn validate_number_value(
    key: &str,
    props: &FieldProps,
    value: &serde_json::Value,
    min: Option<f64>,
    max: Option<f64>,
) -> anyhow::Result<()> {
    if validate_nullable_value(key, props, value)? {
        return Ok(());
    }

    let Some(number) = value.as_f64() else {
        bail!("setting '{key}' must be a number or null");
    };

    if let Some(min) = min
        && number < min
    {
        bail!("setting '{key}' must be greater than or equal to {min}");
    }
    if let Some(max) = max
        && number > max
    {
        bail!("setting '{key}' must be less than or equal to {max}");
    }

    Ok(())
}

fn validate_bool_value(
    key: &str,
    props: &FieldProps,
    value: &serde_json::Value,
) -> anyhow::Result<()> {
    if validate_nullable_value(key, props, value)? {
        return Ok(());
    }
    if value.is_boolean() {
        return Ok(());
    }
    bail!("setting '{key}' must be a boolean or null");
}

fn validate_choice_value(
    key: &str,
    props: &FieldProps,
    options: &[ChoiceOption],
    value: &serde_json::Value,
) -> anyhow::Result<()> {
    if validate_nullable_value(key, props, value)? {
        return Ok(());
    }

    let Some(choice) = value.as_str() else {
        bail!("setting '{key}' must be a string or null");
    };

    if options.iter().any(|option| option.value == choice) {
        return Ok(());
    }

    bail!(
        "setting '{key}' must be one of: {}",
        options
            .iter()
            .map(|option| option.value.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
}

impl Schema {
    pub(crate) fn field(&self, key: &str) -> Option<&FieldDefinition> {
        self.groups
            .iter()
            .flat_map(|group| group.fields.iter())
            .find(|field| field.key() == key)
    }
}

impl Registry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

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
        // Exemption lifetime is tied to bucket presence — if the caller
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
    REGISTRY.write().await.clear();
}

pub(crate) async fn freeze_registry() {
    REGISTRY.write().await.freeze();
}

pub(crate) async fn unfreeze_plugin_settings(plugin_id: PluginId) {
    REGISTRY.write().await.unfreeze_plugin(plugin_id);
}

pub(crate) async fn refreeze_plugin_settings(plugin_id: &PluginId) {
    REGISTRY.write().await.refreeze_plugin(plugin_id);
}

pub(crate) async fn teardown_plugin_settings(plugin_id: &PluginId) {
    ScopedRegistry::from_shared(REGISTRY.clone())
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
        let mut registry = Registry::new();
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
        let mut registry = Registry::new();
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
        let mut registry = Registry::new();
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
        let mut registry = Registry::new();
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
        let mut registry = Registry::new();
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
