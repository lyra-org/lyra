// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    borrow::Borrow,
    collections::HashSet,
    fmt,
    sync::Arc,
};

use anyhow::{
    Result,
    bail,
};
use tokio::sync::{
    Mutex,
    RwLock,
    RwLockReadGuard,
};

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

pub(crate) struct PluginRegistrationGuard<'a> {
    _guard: RwLockReadGuard<'a, HashSet<PluginId>>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PluginRestartError {
    #[error("plugin not found: {0}")]
    NotFound(PluginId),
    #[error("failed to restart plugin '{plugin_id}': {source:#}")]
    Failed {
        plugin_id: PluginId,
        #[source]
        source: anyhow::Error,
    },
}

#[derive(Clone)]
pub(crate) struct PluginRegistries {
    restart_lock: Arc<Mutex<()>>,
    teardown_lock: Arc<Mutex<()>>,
    teardown_in_progress: Arc<RwLock<HashSet<PluginId>>>,
}

impl Default for PluginRegistries {
    fn default() -> Self {
        Self::new()
    }
}

impl PluginRegistries {
    pub(crate) fn new() -> Self {
        Self {
            restart_lock: Arc::new(Mutex::new(())),
            teardown_lock: Arc::new(Mutex::new(())),
            teardown_in_progress: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    pub(crate) async fn ensure_registrations_open(
        &self,
        plugin_id: &PluginId,
    ) -> Result<PluginRegistrationGuard<'_>> {
        let guard = self.teardown_in_progress.read().await;
        if guard.contains(plugin_id) {
            bail!("plugin '{plugin_id}' is tearing down; new registrations rejected");
        }
        Ok(PluginRegistrationGuard { _guard: guard })
    }

    pub(crate) async fn teardown_plugin(&self, plugin_id: &PluginId, reopen_registrations: bool) {
        let _teardown = self.teardown_lock.lock().await;

        {
            let mut teardown_in_progress = self.teardown_in_progress.write().await;
            teardown_in_progress.insert(plugin_id.clone());
        }

        crate::plugins::api::refreeze_plugin_routes(plugin_id).await;
        crate::plugins::settings::refreeze_plugin_settings(plugin_id).await;

        crate::plugins::api::teardown_plugin_routes(plugin_id).await;
        crate::services::providers::teardown_plugin_providers(plugin_id).await;
        crate::services::mix::teardown_plugin_mixers(plugin_id).await;
        crate::services::playback_sessions::teardown_plugin_callbacks(plugin_id).await;
        crate::plugins::settings::teardown_plugin_settings(plugin_id).await;

        if reopen_registrations {
            crate::plugins::api::unfreeze_plugin_routes(plugin_id.clone()).await;
            crate::plugins::settings::unfreeze_plugin_settings(plugin_id.clone()).await;
        }

        self.teardown_in_progress.write().await.remove(plugin_id);
    }

    /// Tears down every loaded plugin, rediscovers `plugins_dir`, and
    /// executes the freshly discovered set. This is how installs, updates,
    /// and uninstalls go live without a server restart.
    pub(crate) async fn reload_all_plugins(&self) -> Result<()> {
        let _restart = self.restart_lock.lock().await;

        let old_ids: Vec<PluginId> = crate::STATE
            .generation()
            .plugin_manifests
            .get()
            .iter()
            .filter_map(|manifest| PluginId::new(manifest.id.clone()).ok())
            .collect();

        // Fresh discovery from disk; replaces STATE.generation().plugin_manifests.
        let runtime = crate::plugins::bootstrap::initialize_harmony().await?;
        let new_ids: Vec<PluginId> = crate::STATE
            .generation()
            .plugin_manifests
            .get()
            .iter()
            .filter_map(|manifest| PluginId::new(manifest.id.clone()).ok())
            .collect();

        for plugin_id in &old_ids {
            self.teardown_plugin(plugin_id, true).await;
        }
        // Brand-new plugins were never frozen open; exempt them so their
        // first registrations are accepted.
        for plugin_id in &new_ids {
            if !old_ids.contains(plugin_id) {
                crate::plugins::api::unfreeze_plugin_routes(plugin_id.clone()).await;
                crate::plugins::settings::unfreeze_plugin_settings(plugin_id.clone()).await;
            }
        }

        // Publish before exec: plugin entrypoints dispatch handlers that
        // resolve STATE.generation().plugin_runtime at call time.
        crate::plugins::bootstrap::publish_runtime(runtime.clone());
        let exec_result = runtime.exec_all().await;

        for plugin_id in old_ids.iter().chain(&new_ids) {
            refreeze_plugin_registration_exemptions(plugin_id).await;
        }

        exec_result?;
        crate::plugins::api::rebuild_registered_routes().await?;
        Ok(())
    }

    pub(crate) async fn restart_plugin(
        &self,
        plugin_id: &PluginId,
        runtime: crate::plugins::bootstrap::PluginRuntime,
    ) -> std::result::Result<(), PluginRestartError> {
        let _restart = self.restart_lock.lock().await;

        let has_plugin = runtime
            .has_plugin(plugin_id.as_str())
            .await
            .map_err(|err| PluginRestartError::Failed {
                plugin_id: plugin_id.clone(),
                source: err.context("failed to query plugin runtime"),
            })?;
        if !has_plugin {
            return Err(PluginRestartError::NotFound(plugin_id.clone()));
        }

        let manifests =
            runtime
                .plugin_manifests()
                .await
                .map_err(|err| PluginRestartError::Failed {
                    plugin_id: plugin_id.clone(),
                    source: err.context("failed to read plugin manifests"),
                })?;
        crate::STATE
            .generation()
            .plugin_manifests
            .replace(Arc::from(manifests));

        self.teardown_plugin(plugin_id, true).await;

        match runtime.exec_plugin(plugin_id.as_str()).await {
            Ok(()) => {
                refreeze_plugin_registration_exemptions(plugin_id).await;
                if let Err(err) = crate::plugins::api::rebuild_registered_routes().await {
                    self.teardown_plugin(plugin_id, false).await;
                    return Err(PluginRestartError::Failed {
                        plugin_id: plugin_id.clone(),
                        source: err.context("failed to rebuild plugin API routes"),
                    });
                }
                Ok(())
            }
            Err(err) => {
                refreeze_plugin_registration_exemptions(plugin_id).await;
                self.teardown_plugin(plugin_id, false).await;
                Err(PluginRestartError::Failed {
                    plugin_id: plugin_id.clone(),
                    source: err.context("plugin execution failed"),
                })
            }
        }
    }
}

async fn refreeze_plugin_registration_exemptions(plugin_id: &PluginId) {
    crate::plugins::api::refreeze_plugin_routes(plugin_id).await;
    crate::plugins::settings::refreeze_plugin_settings(plugin_id).await;
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

    #[tokio::test]
    async fn ensure_registrations_open_rejects_during_teardown() {
        let registries = PluginRegistries::new();
        let plugin_id = PluginId::new("demo").unwrap();

        assert!(
            registries
                .ensure_registrations_open(&plugin_id)
                .await
                .is_ok()
        );

        registries
            .teardown_in_progress
            .write()
            .await
            .insert(plugin_id.clone());

        let error = match registries.ensure_registrations_open(&plugin_id).await {
            Ok(_) => panic!("teardown should reject new registrations"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("tearing down"));
    }
}
