// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    any::Any,
    borrow::Borrow,
    fmt,
    sync::Arc,
};

use anyhow::{
    Result,
    bail,
};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::services::{
    mix::MixRegistries,
    plugin_dispatch::PluginDispatch,
    providers::ProviderRuntimeState,
};

/// The registries a plugin runtime's plugins register into. They are built before the runtime's
/// VM, which reaches them through its own data, so a late task of a replaced runtime can only
/// write into the scope dying with it.
#[derive(Clone, Default)]
pub(crate) struct ScopeRegistries {
    pub(crate) providers: ProviderRuntimeState,
    pub(crate) mix: MixRegistries,
    /// Fires once when the scope goes live.
    pub(crate) published: CancellationToken,
}

impl ScopeRegistries {
    /// Empties the plugin's buckets. A new field fails to compile here until it is addressed.
    pub(crate) async fn teardown_plugin(&self, plugin_id: &PluginId) {
        let Self {
            providers,
            mix,
            published: _,
        } = self;
        providers.teardown_plugin(plugin_id).await;
        mix.teardown_plugin(plugin_id).await;
    }
}

/// One plugin runtime as services use it: the dispatcher and the registries holding the handles
/// it answers, published together. A call holds the `Arc` it read its handler from, so the handle
/// and the dispatcher always belong to each other.
pub(crate) struct RuntimeScope {
    dispatch: Arc<dyn PluginDispatch>,
    registries: ScopeRegistries,
    /// What the layer above keeps in the scope. A test of services alone publishes none.
    plugin_layer: Option<Arc<dyn Any + Send + Sync>>,
}

impl RuntimeScope {
    pub(crate) fn new(
        dispatch: Arc<dyn PluginDispatch>,
        registries: ScopeRegistries,
        plugin_layer: Arc<dyn Any + Send + Sync>,
    ) -> Self {
        Self {
            dispatch,
            registries,
            plugin_layer: Some(plugin_layer),
        }
    }

    /// A scope for a test of services, with no layer above it.
    #[cfg(test)]
    pub(crate) fn without_plugin_layer(
        dispatch: Arc<dyn PluginDispatch>,
        registries: ScopeRegistries,
    ) -> Self {
        Self {
            dispatch,
            registries,
            plugin_layer: None,
        }
    }

    /// This scope's layer above, with `dispatch` and fresh registries of its own.
    #[cfg(test)]
    pub(crate) fn republished(&self, dispatch: Arc<dyn PluginDispatch>) -> Self {
        Self {
            dispatch,
            registries: ScopeRegistries::default(),
            plugin_layer: self.plugin_layer.clone(),
        }
    }

    pub(crate) fn dispatch(&self) -> &Arc<dyn PluginDispatch> {
        &self.dispatch
    }

    pub(crate) fn providers(&self) -> &ProviderRuntimeState {
        &self.registries.providers
    }

    pub(crate) fn mix(&self) -> &MixRegistries {
        &self.registries.mix
    }

    /// The state the layer above keeps in the scope, which services never name.
    pub(crate) fn plugin_layer(&self) -> Option<&Arc<dyn Any + Send + Sync>> {
        self.plugin_layer.as_ref()
    }
}

/// Validated plugin identifier. Outer key for every plugin-scoped registry.
#[derive(Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct PluginId(Arc<str>);

impl PluginId {
    pub(crate) fn new(value: impl Into<Arc<str>>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() {
            bail!("plugin id must not be empty");
        }
        if value.chars().any(char::is_whitespace) {
            bail!("plugin id must not contain whitespace");
        }
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for PluginId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for PluginId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl fmt::Display for PluginId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl AsRef<str> for PluginId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Infallible: all validation must happen at register time so teardown
/// cannot leave a half-live plugin.
pub(crate) trait PluginScopedInner: Send + Sync + 'static {
    /// Missing ids are a no-op.
    fn clear_bucket(&mut self, plugin_id: &PluginId);

    /// Rebuild derived side-car state from the surviving buckets.
    fn rebuild_derived(&mut self);
}

/// Pairs `clear_bucket` with `rebuild_derived` so impls cannot skip the rebuild.
pub(crate) struct ScopedRegistry<T: PluginScopedInner> {
    inner: Arc<RwLock<T>>,
}

impl<T: PluginScopedInner> ScopedRegistry<T> {
    pub(crate) fn from_shared(inner: Arc<RwLock<T>>) -> Self {
        Self { inner }
    }

    pub(crate) async fn teardown(&self, plugin_id: &PluginId) {
        let mut guard = self.inner.write().await;
        guard.clear_bucket(plugin_id);
        guard.rebuild_derived();
    }
}

impl<T: PluginScopedInner> Clone for ScopedRegistry<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl<T: PluginScopedInner> ScopedRegistry<T> {
        fn new(inner: T) -> Self {
            Self {
                inner: Arc::new(RwLock::new(inner)),
            }
        }

        fn shared(&self) -> Arc<RwLock<T>> {
            self.inner.clone()
        }
    }

    #[test]
    fn plugin_id_rejects_empty() {
        assert!(PluginId::new("").is_err());
    }

    #[test]
    fn plugin_id_rejects_whitespace() {
        assert!(PluginId::new("   ").is_err());
        assert!(PluginId::new("  demo  ").is_err());
        assert!(PluginId::new("de mo").is_err());
    }

    struct TestInner {
        cleared: Vec<String>,
        rebuilt: usize,
    }

    impl PluginScopedInner for TestInner {
        fn clear_bucket(&mut self, plugin_id: &PluginId) {
            self.cleared.push(plugin_id.to_string());
        }

        fn rebuild_derived(&mut self) {
            self.rebuilt += 1;
        }
    }

    #[tokio::test]
    async fn scoped_registry_always_rebuilds_after_clear() {
        let registry = ScopedRegistry::new(TestInner {
            cleared: Vec::new(),
            rebuilt: 0,
        });
        let id = PluginId::new("demo").unwrap();

        registry.teardown(&id).await;

        let guard = registry.shared();
        let inner = guard.read().await;
        assert_eq!(inner.cleared, vec!["demo".to_string()]);
        assert_eq!(inner.rebuilt, 1);
    }
}
