// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashSet,
    sync::Arc,
};

use crate::services::plugin_scope::PluginId;
use anyhow::{
    Result,
    bail,
};
use tokio::sync::{
    Mutex,
    RwLock,
    RwLockReadGuard,
};

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

        let old_ids: Vec<PluginId> = crate::plugins::STATE
            .generation()
            .manifests
            .get()
            .iter()
            .filter_map(|manifest| PluginId::new(manifest.id.clone()).ok())
            .collect();

        // Fresh discovery from disk; replaces crate::plugins::STATE.generation().manifests.
        let runtime = crate::plugins::bootstrap::initialize_harmony().await?;
        let new_ids: Vec<PluginId> = crate::plugins::STATE
            .generation()
            .manifests
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
        // resolve the published runtime at call time.
        crate::plugins::STATE.publish_runtime(runtime.clone());
        let exec_result = runtime.exec_all().await;

        for plugin_id in old_ids.iter().chain(&new_ids) {
            refreeze_plugin_registration_exemptions(plugin_id).await;
        }

        exec_result?;
        crate::plugins::api::rebuild_registered_routes().await?;
        Ok(())
    }

    /// Tears the plugin down and re-runs its entrypoint from the files
    /// currently on disk. The manifest stays as loaded: a changed
    /// `plugin.json` is rejected before teardown and needs `reload_all_plugins`.
    /// Other plugins that already required this plugin's modules keep the
    /// old exports until they restart themselves.
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

        runtime
            .verify_plugin_manifest(plugin_id.as_str())
            .await
            .map_err(|err| PluginRestartError::Failed {
                plugin_id: plugin_id.clone(),
                source: err.context("failed to verify plugin manifest"),
            })?;

        let manifests =
            runtime
                .plugin_manifests()
                .await
                .map_err(|err| PluginRestartError::Failed {
                    plugin_id: plugin_id.clone(),
                    source: err.context("failed to read plugin manifests"),
                })?;
        crate::plugins::STATE
            .generation()
            .manifests
            .replace(Arc::from(manifests));

        self.teardown_plugin(plugin_id, true).await;

        // Caches are dropped on the VM thread right before the entrypoint
        // re-runs, after teardown, so no old-plugin work can repopulate them.
        match runtime.restart_plugin(plugin_id.as_str()).await {
            Ok(()) => {
                refreeze_plugin_registration_exemptions(plugin_id).await;
                if let Err(err) = crate::plugins::api::rebuild_registered_routes().await {
                    if let Err(clear_err) = runtime.clear_plugin_callbacks(plugin_id.as_str()).await
                    {
                        tracing::error!(
                            plugin_id = %plugin_id,
                            error = %clear_err,
                            "failed to drop the callbacks of a plugin whose restart failed"
                        );
                    }
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
