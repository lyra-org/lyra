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
    Context,
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
    #[error("plugin runtime is not ready")]
    RuntimeNotReady,
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
        // A plugin about to run again keeps its HTTP limits in force until
        // that run has set its own; `sweep_stale_http_limits` drops the rest.
        harmony_http::mark_plugin_limits_stale(plugin_id.as_str()).await;
        if !reopen_registrations {
            sweep_stale_http_limits(plugin_id).await;
        }

        if reopen_registrations {
            crate::plugins::api::unfreeze_plugin_routes(plugin_id.clone()).await;
            crate::plugins::settings::unfreeze_plugin_settings(plugin_id.clone()).await;
        }

        self.teardown_in_progress.write().await.remove(plugin_id);
    }

    /// Runs the startup runtime's entrypoints and publishes it, so a reload
    /// can't start before there is a published runtime for it to replace. A
    /// reload that got the lock first already published a fresher runtime,
    /// so the startup one is shut down unused.
    pub(crate) async fn start_plugins(
        &self,
        runtime: crate::plugins::bootstrap::PluginRuntime,
    ) -> Result<()> {
        let _restart = self.restart_lock.lock().await;
        if crate::plugins::STATE.generation().runtime.get().is_some() {
            return runtime.shutdown().await;
        }
        let failed = runtime.exec_all().await?;
        crate::plugins::STATE.publish_runtime(runtime);
        self.teardown_failed_plugins(&failed).await;
        Ok(())
    }

    /// Their failed entrypoints already ended their runs.
    async fn teardown_failed_plugins(&self, failed: &[String]) {
        for plugin_id in failed
            .iter()
            .filter_map(|id| PluginId::new(id.clone()).ok())
        {
            self.teardown_plugin(&plugin_id, false).await;
        }
    }

    /// Tears down every loaded plugin, rediscovers `plugins_dir`, and
    /// executes the freshly discovered set. This is how installs, updates,
    /// and uninstalls go live without a server restart.
    ///
    /// Runs as its own task: a caller dropped between stopping the old
    /// runtime and publishing the new one would leave no runtime at all.
    pub(crate) async fn reload_all_plugins(&self) -> Result<()> {
        let registries = self.clone();
        tokio::spawn(async move {
            let result = registries.reload_detached().await;
            if let Err(error) = &result {
                tracing::error!(error = %format!("{error:#}"), "plugin reload failed");
            }
            result
        })
        .await
        .context("plugin reload task failed")?
    }

    async fn reload_detached(self) -> Result<()> {
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

        // The replaced runtime must run no more Lua, so none of its tasks
        // can register once teardown reopens registrations. A failed shutdown
        // means its thread had already exited, which leaves it just as stopped.
        if let Some(old_runtime) = crate::plugins::STATE.generation().runtime.get()
            && let Err(error) = old_runtime.shutdown().await
        {
            tracing::warn!(
                error = %format!("{error:#}"),
                "replaced plugin runtime had already stopped"
            );
        }
        for plugin_id in &old_ids {
            self.teardown_plugin(plugin_id, true).await;
            if !new_ids.contains(plugin_id) {
                sweep_stale_http_limits(plugin_id).await;
            }
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
        if let Ok(failed) = &exec_result {
            self.teardown_failed_plugins(failed).await;
        }
        for plugin_id in &old_ids {
            sweep_stale_http_limits(plugin_id).await;
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
    ///
    /// Runs as its own task, so a dropped caller can't leave the plugin
    /// stopped and torn down without its new run.
    pub(crate) async fn restart_plugin(
        &self,
        plugin_id: &PluginId,
    ) -> std::result::Result<(), PluginRestartError> {
        let registries = self.clone();
        let id = plugin_id.clone();
        tokio::spawn(async move {
            let result = registries.restart_detached(id).await;
            if let Err(error) = &result {
                tracing::error!(error = %error, "plugin restart failed");
            }
            result
        })
        .await
        .unwrap_or_else(|error| {
            Err(PluginRestartError::Failed {
                plugin_id: plugin_id.clone(),
                source: anyhow::Error::new(error).context("plugin restart task failed"),
            })
        })
    }

    async fn restart_detached(
        self,
        plugin_id: PluginId,
    ) -> std::result::Result<(), PluginRestartError> {
        let plugin_id = &plugin_id;
        let _restart = self.restart_lock.lock().await;
        // Read under the lock: a reload ahead of this restart replaces it.
        let runtime = crate::plugins::STATE
            .generation()
            .runtime
            .get()
            .ok_or(PluginRestartError::RuntimeNotReady)?;

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

        // Stop the old run before teardown reopens registrations, so none of
        // its tasks can register into the restarted plugin.
        runtime
            .stop_plugin(plugin_id.as_str())
            .await
            .map_err(|err| PluginRestartError::Failed {
                plugin_id: plugin_id.clone(),
                source: err.context("failed to stop plugin"),
            })?;
        self.teardown_plugin(plugin_id, true).await;

        // Caches are dropped on the VM thread right before the entrypoint
        // re-runs, after teardown, so no old-plugin work can repopulate them.
        match runtime.restart_plugin(plugin_id.as_str()).await {
            Ok(()) => {
                refreeze_plugin_registration_exemptions(plugin_id).await;
                sweep_stale_http_limits(plugin_id).await;
                if let Err(err) = crate::plugins::api::rebuild_registered_routes().await {
                    if let Err(error) = runtime.stop_plugin(plugin_id.as_str()).await {
                        tracing::warn!(
                            plugin_id = %plugin_id,
                            error = %format!("{error:#}"),
                            "failed to stop plugin after its routes failed to rebuild"
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
            // The failed entrypoint already ended its run.
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

async fn sweep_stale_http_limits(plugin_id: &PluginId) {
    harmony_http::remove_stale_plugin_limits(plugin_id.as_str()).await;
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

    fn published_runtime() -> anyhow::Result<crate::plugins::bootstrap::PluginRuntime> {
        crate::plugins::STATE
            .generation()
            .runtime
            .get()
            .context("published plugin runtime")
    }

    async fn is_stopped(runtime: &crate::plugins::bootstrap::PluginRuntime) -> bool {
        runtime
            .has_plugin("demo")
            .await
            .is_err_and(|error| format!("{error:#}").contains("unavailable"))
    }

    async fn publish_startup_runtime() -> anyhow::Result<crate::plugins::bootstrap::PluginRuntime> {
        crate::plugins::testing::init_default_test_state()?;
        crate::plugins::testing::start_runtime().await?;
        let runtime = crate::plugins::bootstrap::initialize_harmony().await?;
        crate::plugins::STATE
            .generation()
            .registries
            .start_plugins(runtime.clone())
            .await?;
        Ok(runtime)
    }

    const TASK_AND_METADATA: &[&str] = &["harmony.task", "lyra.metadata"];

    /// Runs a plugin `id` with `scopes` whose entrypoint is `init`, from a
    /// directory the test can rewrite before a restart.
    async fn exec_test_plugin(
        id: &str,
        scopes: &[&str],
        init: &str,
    ) -> anyhow::Result<(
        crate::testing::RuntimeTestGuard,
        tempfile::TempDir,
        crate::testing::TempPluginsDir,
    )> {
        let source = tempfile::tempdir()?;
        std::fs::write(
            source.path().join("plugin.json"),
            serde_json::json!({
                "schema_version": 1,
                "id": id,
                "name": id,
                "version": "1.0.0",
                "lyra": crate::plugins::testing::HOST_REQUIREMENT,
                "description": "",
                "entrypoint": "init.luau",
                "scopes": scopes,
            })
            .to_string(),
        )?;
        std::fs::write(source.path().join("init.luau"), init)?;
        let guard = crate::testing::initialize_runtime(&crate::testing::LibraryFixtureConfig {
            directory: source.path().to_path_buf(),
            language: None,
            country: None,
        })
        .await?;
        let plugin = crate::testing::PluginUnderTest::locate(source.path())?;
        let sources =
            crate::testing::exec_plugins(&plugin, harmony_http::HttpModule::module_spec()).await?;
        Ok((guard, source, sources))
    }

    /// An entrypoint that registers `provider`'s refresh handler after a
    /// short delay, then runs `rest`.
    fn delayed_provider_entrypoint(provider: &str, rest: &str) -> String {
        format!(
            r#"
                local task = require("@harmony/task")
                local metadata = require("@lyra/metadata")
                task.delay(0.05, function()
                    metadata.Provider.new("{provider}"):refresh(metadata.EntityType.Release, function()
                        return nil
                    end)
                end)
                {rest}
            "#
        )
    }

    async fn has_refresh_handler(provider: &str) -> bool {
        crate::services::providers::provider_registry()
            .read()
            .await
            .get_refresh_callback(provider, crate::services::EntityType::Release)
            .is_some()
    }

    async fn restart(plugin_id: &str) -> std::result::Result<(), PluginRestartError> {
        crate::plugins::STATE
            .generation()
            .registries
            .restart_plugin(&PluginId::new(plugin_id).expect("valid plugin id"))
            .await
    }

    #[tokio::test]
    async fn failed_restart_stops_the_tasks_its_entrypoint_spawned() -> anyhow::Result<()> {
        let (_guard, source, _sources) =
            exec_test_plugin("failed-restart", TASK_AND_METADATA, "").await?;
        std::fs::write(
            source.path().join("init.luau"),
            delayed_provider_entrypoint("failed-restart", r#"error("entrypoint failed")"#),
        )?;

        let error = restart("failed-restart")
            .await
            .expect_err("a failing entrypoint fails the restart");
        assert!(
            matches!(error, PluginRestartError::Failed { .. }),
            "{error}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(!has_refresh_handler("failed-restart").await);
        Ok(())
    }

    /// Fails only the rebuild after the new run; the teardowns around it
    /// rebuild too, and must succeed.
    #[derive(Default)]
    struct FailingRouteHost(std::sync::atomic::AtomicUsize);

    impl crate::plugins::api::RouteHost for FailingRouteHost {
        fn rebuild(&self) -> futures::future::BoxFuture<'_, Result<()>> {
            let call = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async move {
                anyhow::ensure!(call != 1, "route rebuild failed");
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn restart_with_a_failed_route_rebuild_stops_the_new_run() -> anyhow::Result<()> {
        let (_guard, source, _sources) =
            exec_test_plugin("failed-rebuild", TASK_AND_METADATA, "").await?;
        crate::plugins::api::initialize_registry(
            HashSet::new(),
            Some(Arc::new(FailingRouteHost::default())),
        )
        .await;
        std::fs::write(
            source.path().join("init.luau"),
            delayed_provider_entrypoint("failed-rebuild", ""),
        )?;

        let error = restart("failed-rebuild")
            .await
            .expect_err("a failed route rebuild fails the restart");
        assert!(format!("{error}").contains("rebuild"), "{error}");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(!has_refresh_handler("failed-rebuild").await);
        Ok(())
    }

    #[tokio::test]
    async fn restart_completes_after_its_caller_is_dropped() -> anyhow::Result<()> {
        let (_guard, source, _sources) =
            exec_test_plugin("dropped-restart", TASK_AND_METADATA, "").await?;
        std::fs::write(
            source.path().join("init.luau"),
            delayed_provider_entrypoint("dropped-restart", ""),
        )?;

        {
            let restart = restart("dropped-restart");
            futures::pin_mut!(restart);
            assert!(futures::poll!(restart).is_pending());
        }

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !has_refresh_handler("dropped-restart").await {
            assert!(
                std::time::Instant::now() < deadline,
                "the dropped restart must still run the new entrypoint"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn restart_queued_behind_a_reload_uses_the_runtime_it_published() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let first = publish_startup_runtime().await?;
        let registries = crate::plugins::STATE.generation().registries.clone();

        let held = registries.restart_lock.clone().lock_owned().await;
        let queued = tokio::spawn(async { restart("demo").await });
        tokio::task::yield_now().await;
        // What a reload does under the lock: stop the old runtime and publish a new one.
        first.shutdown().await?;
        crate::plugins::STATE
            .publish_runtime(crate::plugins::bootstrap::initialize_harmony().await?);
        drop(held);

        let error = queued
            .await?
            .expect_err("the new runtime has no such plugin");
        assert!(matches!(error, PluginRestartError::NotFound(_)), "{error}");
        Ok(())
    }

    #[tokio::test]
    async fn startup_and_reload_leave_one_live_runtime_in_either_order() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        for reload_first in [false, true] {
            crate::plugins::testing::init_default_test_state()?;
            crate::plugins::testing::start_runtime().await?;
            let registries = crate::plugins::STATE.generation().registries.clone();
            let startup = crate::plugins::bootstrap::initialize_harmony().await?;

            let held = registries.restart_lock.clone().lock_owned().await;
            let start = || {
                let registries = registries.clone();
                let startup = startup.clone();
                tokio::spawn(async move { registries.start_plugins(startup).await })
            };
            let reload = || {
                let registries = registries.clone();
                tokio::spawn(async move { registries.reload_all_plugins().await })
            };
            let (first, second) = if reload_first {
                let reload = reload();
                tokio::task::yield_now().await;
                (reload, start())
            } else {
                let start = start();
                tokio::task::yield_now().await;
                (start, reload())
            };
            tokio::task::yield_now().await;
            drop(held);
            first.await??;
            second.await??;

            assert!(is_stopped(&startup).await, "reload_first: {reload_first}");
            assert!(!published_runtime()?.has_plugin("demo").await?);
        }
        Ok(())
    }

    #[tokio::test]
    async fn reload_replaces_a_runtime_whose_thread_is_gone() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let first = publish_startup_runtime().await?;
        let registries = crate::plugins::STATE.generation().registries.clone();

        first.shutdown().await?;
        registries.reload_all_plugins().await?;
        assert!(!published_runtime()?.has_plugin("demo").await?);
        Ok(())
    }

    #[tokio::test]
    async fn reload_completes_after_its_caller_is_dropped() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let first = publish_startup_runtime().await?;
        let registries = crate::plugins::STATE.generation().registries.clone();

        {
            let reload = registries.reload_all_plugins();
            futures::pin_mut!(reload);
            assert!(futures::poll!(reload).is_pending());
        }

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if is_stopped(&first).await
                && let Ok(runtime) = published_runtime()
                && runtime.has_plugin("demo").await.is_ok()
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the dropped reload must still publish a runtime"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn restart_counts_only_rate_limits_the_new_run_sets() -> anyhow::Result<()> {
        let entrypoint = |rate_limit: &str| {
            format!(
                r#"
                    local http = require("@harmony/http")
                    local metadata = require("@lyra/metadata")
                    {rate_limit}
                    metadata.Provider.new("restart-rate-limit"):lyrics({{}}, function(_ctx)
                        return nil
                    end)
                "#
            )
        };
        let (_guard, source, _sources) = exec_test_plugin(
            "restart-rate-limit",
            &["harmony.http", "lyra.metadata"],
            &entrypoint(r#"http.set_rate_limit({ domain = "restart-rate-limit.example" })"#),
        )
        .await?;
        let write_entrypoint = |rate_limit: &str| {
            std::fs::write(source.path().join("init.luau"), entrypoint(rate_limit))
        };
        assert!(harmony_http::has_rate_limit_for_plugin("restart-rate-limit").await);

        let plugin_id = PluginId::new("restart-rate-limit")?;
        let restart = || async {
            crate::plugins::STATE
                .generation()
                .registries
                .restart_plugin(&plugin_id)
                .await
        };

        write_entrypoint(r#"http.set_rate_limit({ domain = "restart-rate-limit-2.example" })"#)?;
        restart().await?;
        assert!(harmony_http::has_rate_limit_for_plugin("restart-rate-limit").await);

        write_entrypoint("")?;
        let error = restart()
            .await
            .expect_err("lyrics registration needs a rate limit from the new run");

        assert!(
            error
                .to_string()
                .contains("provider:lyrics requires http.set_rate_limit"),
            "{error}"
        );
        assert!(!harmony_http::has_rate_limit_for_plugin("restart-rate-limit").await);
        assert!(
            !harmony_http::test_holds_any_limit("restart-rate-limit").await,
            "a failed restart sweeps the stale limits too"
        );
        Ok(())
    }

    #[tokio::test]
    async fn reload_sweeps_the_limits_of_an_uninstalled_plugin() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        publish_startup_runtime().await?;
        let mut gone = crate::plugins::STATE.generation().manifests.get().to_vec();
        gone.push(harmony_core::plugin::PluginManifest {
            schema_version: 1,
            id: "uninstalled".to_string(),
            name: "Uninstalled".to_string(),
            version: semver::Version::new(1, 0, 0),
            host_requirement: semver::VersionReq::STAR,
            description: String::new(),
            entrypoint: Some("init.luau".to_string()),
            scopes: Vec::new(),
            dependencies: Default::default(),
        });
        crate::plugins::STATE
            .generation()
            .manifests
            .replace(Arc::from(gone));
        harmony_http::test_seed_rate_limit("uninstalled.example", "uninstalled").await;

        crate::plugins::STATE
            .generation()
            .registries
            .reload_all_plugins()
            .await?;
        assert!(!harmony_http::test_holds_any_limit("uninstalled").await);
        Ok(())
    }
}
