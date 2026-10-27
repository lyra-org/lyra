// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    borrow::Borrow,
    fmt,
    sync::Arc,
};

use anyhow::{
    Result,
    bail,
};
use tokio::sync::RwLock;

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
