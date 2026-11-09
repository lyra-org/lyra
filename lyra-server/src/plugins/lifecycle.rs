// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashSet,
    sync::Arc,
    time::Duration,
};

use crate::{
    plugins::{
        executor::{
            DrainOutcome,
            ExecutorId,
        },
        state::VmScope,
    },
    services::{
        plugin_scope::PluginId,
        state::Transition,
    },
};
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
use tokio_util::sync::CancellationToken;

/// How long a reload lets the calls running on the replaced runtime finish.
const RELOAD_DRAIN_LIMIT: Duration = Duration::from_secs(10);

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

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PublishOutcome {
    Published,
    /// Another runtime took the place it was to take: a reload replaced the
    /// crashed runtime, or published one before startup could.
    Replaced,
    /// Shutdown started before it published a runtime or while its
    /// entrypoints ran, which it cut short.
    ShuttingDown,
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

    #[cfg(test)]
    pub(crate) async fn hold_restart_lock(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.restart_lock.clone().lock_owned().await
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

    pub(crate) async fn teardown_plugin(
        &self,
        scope: &VmScope,
        plugin_id: &PluginId,
        reopen_registrations: bool,
    ) {
        let _teardown = self.teardown_lock.lock().await;

        {
            let mut teardown_in_progress = self.teardown_in_progress.write().await;
            teardown_in_progress.insert(plugin_id.clone());
        }

        scope.plugin.api.refreeze_plugin(plugin_id).await;
        scope.plugin.settings.refreeze_plugin(plugin_id).await;

        scope.teardown_plugin(plugin_id).await;
        crate::plugins::api::rebuild_registered_routes()
            .await
            .unwrap_or_else(|err| panic!("router rebuild must not fail post-teardown: {err}"));
        // A plugin about to run again keeps its HTTP limits in force until
        // that run has set its own; `sweep_stale_http_limits` drops the rest.
        harmony_http::mark_plugin_limits_stale(plugin_id.as_str()).await;
        harmony_http::clear_plugin_cache(plugin_id.as_str());
        if !reopen_registrations {
            sweep_stale_http_limits(plugin_id).await;
        }

        if reopen_registrations {
            scope.plugin.api.unfreeze_plugin(plugin_id.clone()).await;
            scope
                .plugin
                .settings
                .unfreeze_plugin(plugin_id.clone())
                .await;
        }

        self.teardown_in_progress.write().await.remove(plugin_id);
    }

    /// Starts the startup runtime through the path a reload takes, under the lock reloads take. A
    /// reload that got the lock first already published a fresher runtime, so the startup one is
    /// shut down unused.
    pub(crate) async fn start_plugins(
        &self,
        runtime: crate::plugins::bootstrap::PluginRuntime,
        shutdown: CancellationToken,
    ) -> Result<PublishOutcome> {
        let Some(_restart) = self.lock_unless_cancelled(&shutdown).await else {
            stop_unpublished(&runtime).await;
            return Ok(PublishOutcome::ShuttingDown);
        };
        let outcome = if crate::plugins::STATE.runtime().is_some() {
            runtime.shutdown().await?;
            PublishOutcome::Replaced
        } else {
            match self.replace_runtime(&runtime, &shutdown).await {
                Ok(PublishOutcome::ShuttingDown) => return Ok(PublishOutcome::ShuttingDown),
                Ok(outcome) => outcome,
                // The scope is published; the supervisor or the next reload takes it from here.
                Err(error) => {
                    tracing::error!(
                        error = %format!("{error:#}"),
                        "plugin runtime did not start cleanly"
                    );
                    PublishOutcome::Published
                }
            }
        };
        if shutdown.is_cancelled() {
            return Ok(PublishOutcome::ShuttingDown);
        }
        crate::plugins::bootstrap::finalize_startup().await;
        Ok(outcome)
    }

    async fn lock_unless_cancelled(
        &self,
        shutdown: &CancellationToken,
    ) -> Option<tokio::sync::MutexGuard<'_, ()>> {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => None,
            restart = self.restart_lock.lock() => Some(restart),
        }
    }

    /// Their failed entrypoints already ended their runs.
    async fn teardown_failed_plugins(&self, scope: &VmScope, failed: &[String]) {
        for plugin_id in failed
            .iter()
            .filter_map(|id| PluginId::new(id.clone()).ok())
        {
            self.teardown_plugin(scope, &plugin_id, false).await;
        }
    }

    /// Tears down every loaded plugin, rediscovers `plugins_dir`, and
    /// executes the freshly discovered set. This is how installs, updates,
    /// and uninstalls go live without a server restart.
    ///
    /// Runs as its own task: a caller dropped between stopping the old
    /// runtime and publishing the new one would leave no runtime at all.
    pub(crate) async fn reload_all_plugins(&self) -> Result<()> {
        match self
            .spawn_reload(None, crate::services::shutdown::token())
            .await?
        {
            PublishOutcome::ShuttingDown => bail!("plugins are not reloaded during shutdown"),
            PublishOutcome::Published | PublishOutcome::Replaced => Ok(()),
        }
    }

    /// Reloads, as [`Self::reload_all_plugins`] does, to replace the runtime
    /// `crashed`, unless a reload already replaced it. Gives up once
    /// `shutdown` is cancelled.
    pub(crate) async fn reload_if_published(
        &self,
        crashed: ExecutorId,
        shutdown: CancellationToken,
    ) -> Result<PublishOutcome> {
        self.spawn_reload(Some(crashed), shutdown).await
    }

    async fn spawn_reload(
        &self,
        expected: Option<ExecutorId>,
        shutdown: CancellationToken,
    ) -> Result<PublishOutcome> {
        let registries = self.clone();
        tokio::spawn(async move {
            let result = registries.reload_detached(expected, shutdown).await;
            if let Err(error) = &result {
                tracing::error!(error = %format!("{error:#}"), "plugin reload failed");
            }
            result
        })
        .await
        .unwrap_or_else(|error| {
            tracing::error!(error = %error, "plugin reload task failed");
            Err(anyhow::Error::new(error).context("plugin reload task failed"))
        })
    }

    async fn reload_detached(
        self,
        expected: Option<ExecutorId>,
        shutdown: CancellationToken,
    ) -> Result<PublishOutcome> {
        let Some(_restart) = self.lock_unless_cancelled(&shutdown).await else {
            return Ok(PublishOutcome::ShuttingDown);
        };
        // Executor ids are never reused, so a runtime a reload replaced never
        // matches the one published since.
        if let Some(expected) = expected
            && crate::plugins::STATE
                .runtime()
                .is_none_or(|published| published.id() != expected)
        {
            return Ok(PublishOutcome::Replaced);
        }

        // Fresh discovery from disk, built aside from what is live. A failure here leaves the old
        // scope serving.
        let runtime = crate::plugins::bootstrap::initialize_harmony().await?;
        self.replace_runtime(&runtime, &shutdown).await
    }

    /// Replaces the published runtime with `runtime`: stops the old one, runs the entrypoints
    /// into the new scope with new calls waiting, and publishes it whole. Once shutdown is
    /// cancelled it publishes nothing and clears the pointer.
    async fn replace_runtime(
        &self,
        runtime: &crate::plugins::bootstrap::PluginRuntime,
        shutdown: &CancellationToken,
    ) -> Result<PublishOutcome> {
        let generation = crate::services::STATE.generation();
        let transition = generation.begin_transition(Transition::Runtime);

        let old_ids = plugin_ids(&crate::plugins::STATE.manifests());
        let new_ids = plugin_ids(&runtime.scope().plugin.manifests.get());
        let old_scope = crate::plugins::STATE.vm_scope();

        // The replaced runtime must run no more Lua. A failed shutdown means its thread had
        // already exited, which leaves it just as stopped.
        if let Some(old_runtime) = crate::plugins::STATE.runtime() {
            if old_runtime.drain(RELOAD_DRAIN_LIMIT, shutdown).await == DrainOutcome::TimedOut {
                tracing::warn!(
                    limit_ms = RELOAD_DRAIN_LIMIT.as_millis() as u64,
                    "replaced plugin runtime still had calls running; cancelling them"
                );
            }
            if let Err(error) = old_runtime.shutdown().await {
                tracing::warn!(
                    error = %format!("{error:#}"),
                    "replaced plugin runtime had already stopped"
                );
            }
        }
        // Dropping the old registry would do this, but calls still hold the scope.
        if let Some(old_scope) = &old_scope {
            old_scope.services.providers.cancel_lyrics_handlers().await;
        }
        for plugin_id in &old_ids {
            // A plugin about to run again keeps its HTTP limits in force until that run has set
            // its own; the sweep below drops the rest.
            harmony_http::mark_plugin_limits_stale(plugin_id.as_str()).await;
            harmony_http::clear_plugin_cache(plugin_id.as_str());
            if !new_ids.contains(plugin_id) {
                sweep_stale_http_limits(plugin_id).await;
            }
        }

        let exec = if shutdown.is_cancelled() {
            None
        } else {
            Some(run_entrypoints(runtime, shutdown).await)
        };
        let Some(exec) = exec.filter(|_| !shutdown.is_cancelled()) else {
            return Ok(self.abandon_runtime(runtime).await);
        };

        if let Ok(failed) = &exec {
            self.teardown_failed_plugins(runtime.scope(), failed).await;
        }
        runtime.scope().plugin.freeze().await;
        let routes = crate::plugins::api::build_routes(&runtime.scope().plugin.api).await;

        let published = crate::services::shutdown::unless_cancelled(shutdown, || {
            crate::plugins::STATE.publish_runtime(&generation, runtime.clone());
        });
        if published.is_none() {
            return Ok(self.abandon_runtime(runtime).await);
        }
        // Requests waiting on the transition pick their route from the new routers.
        let routes = routes.context("failed to build plugin routes");
        let routes = match (&exec, routes) {
            (Ok(_), Ok(Some(install))) => {
                install().await;
                Ok(())
            }
            (_, Err(error)) if exec.is_ok() => Err(error),
            _ => Ok(()),
        };
        drop(transition);

        for plugin_id in &old_ids {
            sweep_stale_http_limits(plugin_id).await;
        }

        exec?;
        routes?;
        Ok(PublishOutcome::Published)
    }

    async fn abandon_runtime(
        &self,
        runtime: &crate::plugins::bootstrap::PluginRuntime,
    ) -> PublishOutcome {
        crate::services::STATE.generation().runtime.replace(None);
        stop_unpublished(runtime).await;
        PublishOutcome::ShuttingDown
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
            .runtime()
            .ok_or(PluginRestartError::RuntimeNotReady)?;
        let scope = runtime.scope();

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
        scope.plugin.manifests.replace(Arc::from(manifests));

        // Stop the old run before teardown reopens registrations, so none of
        // its tasks can register into the restarted plugin.
        runtime
            .stop_plugin(plugin_id.as_str())
            .await
            .map_err(|err| PluginRestartError::Failed {
                plugin_id: plugin_id.clone(),
                source: err.context("failed to stop plugin"),
            })?;
        self.teardown_plugin(scope, plugin_id, true).await;

        // Caches are dropped on the VM thread right before the entrypoint
        // re-runs, after teardown, so no old-plugin work can repopulate them.
        match runtime.restart_plugin(plugin_id.as_str()).await {
            Ok(()) => {
                refreeze_plugin_registration_exemptions(scope, plugin_id).await;
                sweep_stale_http_limits(plugin_id).await;
                if let Err(err) = crate::plugins::api::rebuild_registered_routes().await {
                    if let Err(error) = runtime.stop_plugin(plugin_id.as_str()).await {
                        tracing::warn!(
                            plugin_id = %plugin_id,
                            error = %format!("{error:#}"),
                            "failed to stop plugin after its routes failed to rebuild"
                        );
                    }
                    self.teardown_plugin(scope, plugin_id, false).await;
                    return Err(PluginRestartError::Failed {
                        plugin_id: plugin_id.clone(),
                        source: err.context("failed to rebuild plugin API routes"),
                    });
                }
                Ok(())
            }
            // The failed entrypoint already ended its run.
            Err(err) => {
                refreeze_plugin_registration_exemptions(scope, plugin_id).await;
                self.teardown_plugin(scope, plugin_id, false).await;
                Err(PluginRestartError::Failed {
                    plugin_id: plugin_id.clone(),
                    source: err.context("plugin execution failed"),
                })
            }
        }
    }
}

/// The plugins whose entrypoint failed. Shutdown cuts the entrypoints short.
async fn run_entrypoints(
    runtime: &crate::plugins::bootstrap::PluginRuntime,
    shutdown: &CancellationToken,
) -> Result<Vec<String>> {
    let exec = runtime.exec_all();
    futures::pin_mut!(exec);
    tokio::select! {
        biased;
        () = shutdown.cancelled() => {
            runtime.cancel_entrypoints();
            exec.await
        }
        result = &mut exec => result,
    }
}

async fn stop_unpublished(runtime: &crate::plugins::bootstrap::PluginRuntime) {
    if let Err(error) = runtime.shutdown().await {
        tracing::warn!(
            error = %format!("{error:#}"),
            "unpublished plugin runtime had already stopped"
        );
    }
}

async fn sweep_stale_http_limits(plugin_id: &PluginId) {
    harmony_http::remove_stale_plugin_limits(plugin_id.as_str()).await;
}

async fn refreeze_plugin_registration_exemptions(scope: &VmScope, plugin_id: &PluginId) {
    scope.plugin.api.refreeze_plugin(plugin_id).await;
    scope.plugin.settings.refreeze_plugin(plugin_id).await;
}

fn plugin_ids(manifests: &[harmony_core::plugin::PluginManifest]) -> Vec<PluginId> {
    manifests
        .iter()
        .filter_map(|manifest| PluginId::new(manifest.id.clone()).ok())
        .collect()
}

#[cfg(test)]
#[path = "lifecycle/reentry_tests.rs"]
mod reentry_tests;

#[cfg(test)]
#[path = "lifecycle/transition_tests.rs"]
mod transition_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::testing::published_runtime;

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
            .start_plugins(runtime.clone(), CancellationToken::new())
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
        crate::plugins::testing::write_plugin(source.path(), id, scopes, init)?;
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
        fn build(
            &self,
            _routes: Vec<crate::plugins::api::RegisteredRoute>,
            _case_insensitive: crate::plugins::api::CaseInsensitiveRouter,
        ) -> futures::future::BoxFuture<'_, Result<crate::plugins::api::InstallRoutes>> {
            let call = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async move {
                anyhow::ensure!(call != 1, "route rebuild failed");
                let install: crate::plugins::api::InstallRoutes = Box::new(|| Box::pin(async {}));
                Ok(install)
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

    #[tokio::test(flavor = "multi_thread")]
    async fn restart_queued_behind_a_reload_uses_the_runtime_it_published() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let first = publish_startup_runtime().await?;
        let registries = crate::plugins::STATE.generation().registries.clone();

        let held = registries.restart_lock.clone().lock_owned().await;
        let queued = tokio::spawn(async { restart("demo").await });
        tokio::task::yield_now().await;
        // What a reload does under the lock: stop the old runtime and publish a new one.
        first.shutdown().await?;
        crate::plugins::STATE.publish_runtime(
            &crate::services::STATE.generation(),
            crate::plugins::bootstrap::initialize_harmony().await?,
        );
        drop(held);

        let error = queued
            .await?
            .expect_err("the new runtime has no such plugin");
        assert!(matches!(error, PluginRestartError::NotFound(_)), "{error}");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
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
                tokio::spawn(async move {
                    registries
                        .start_plugins(startup, CancellationToken::new())
                        .await
                        .map(drop)
                })
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

    #[tokio::test(flavor = "multi_thread")]
    async fn reload_replaces_a_runtime_whose_thread_is_gone() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let first = publish_startup_runtime().await?;
        let registries = crate::plugins::STATE.generation().registries.clone();

        first.shutdown().await?;
        registries.reload_all_plugins().await?;
        assert!(!published_runtime()?.has_plugin("demo").await?);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
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

    #[tokio::test(flavor = "multi_thread")]
    async fn reload_sweeps_the_limits_of_an_uninstalled_plugin() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        publish_startup_runtime().await?;
        let mut gone = crate::plugins::STATE.manifests().to_vec();
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
            .scope()
            .context("published scope")?
            .vm_scope
            .plugin
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

    /// Fresh state whose reloads discover `plugins_dir`.
    async fn init_state_over(plugins_dir: &std::path::Path) -> anyhow::Result<()> {
        crate::plugins::testing::init_default_test_state()?;
        crate::plugins::testing::start_runtime().await?;
        crate::plugins::testing::use_plugins_dir(plugins_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn startup_after_shutdown_publishes_nothing() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let plugins_dir = tempfile::tempdir()?;
        init_state_over(plugins_dir.path()).await?;
        let startup = crate::plugins::bootstrap::initialize_harmony().await?;
        let shutdown = CancellationToken::new();
        shutdown.cancel();

        let outcome = crate::plugins::STATE
            .generation()
            .registries
            .start_plugins(startup.clone(), shutdown)
            .await?;
        assert_eq!(outcome, PublishOutcome::ShuttingDown);
        assert!(published_runtime().is_err());
        assert!(is_stopped(&startup).await);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_cuts_short_the_startup_entrypoints() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let plugins_dir = tempfile::tempdir()?;
        crate::plugins::testing::write_plugin(
            &plugins_dir.path().join("slow"),
            "slow",
            &["harmony.task"],
            r#"require("@harmony/task").wait(600)"#,
        )?;
        init_state_over(plugins_dir.path()).await?;
        let startup = crate::plugins::bootstrap::initialize_harmony().await?;
        let shutdown = CancellationToken::new();
        let start = tokio::spawn({
            let shutdown = shutdown.clone();
            let startup = startup.clone();
            async move {
                crate::plugins::STATE
                    .generation()
                    .registries
                    .start_plugins(startup, shutdown)
                    .await
            }
        });
        // Long enough for the entrypoint to be running.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        shutdown.cancel();

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), start)
            .await
            .context("shutdown never ended the slow startup entrypoint")???;
        assert_eq!(outcome, PublishOutcome::ShuttingDown);
        assert!(published_runtime().is_err());
        assert!(is_stopped(&startup).await);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_handler_running_during_a_reload_finishes_with_its_result() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let plugins_dir = tempfile::tempdir()?;
        crate::plugins::testing::write_plugin(
            &plugins_dir.path().join("held"),
            "held",
            TASK_AND_METADATA,
            r#"
                local task = require("@harmony/task")
                local metadata = require("@lyra/metadata")
                metadata.Provider.new("held"):refresh(metadata.EntityType.Release, function(ctx)
                    task.wait(0.4)
                    return ctx.value
                end)
            "#,
        )?;
        init_state_over(plugins_dir.path()).await?;
        let first = crate::plugins::bootstrap::initialize_harmony().await?;
        let registries = crate::plugins::STATE.generation().registries.clone();
        registries
            .start_plugins(first.clone(), CancellationToken::new())
            .await?;
        let handler = crate::services::providers::provider_registry()
            .read()
            .await
            .get_refresh_callback("held", crate::services::EntityType::Release)
            .copied()
            .context("held handler")?;

        let call = tokio::spawn({
            let first = first.clone();
            async move {
                crate::services::plugin_dispatch::PluginDispatch::dispatch_metadata_refresh(
                    &first,
                    crate::services::plugin_dispatch::MetadataRefreshRequest {
                        handler,
                        context: serde_json::json!({ "value": 42 }),
                        deadline: std::time::Instant::now() + std::time::Duration::from_secs(30),
                    },
                )
                .await
            }
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while first.slots_in_use() == 0 {
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "the handler never took its slot"
            );
            tokio::task::yield_now().await;
        }

        registries.reload_all_plugins().await?;

        assert_eq!(call.await??.values, vec![serde_json::json!(42)]);
        assert!(is_stopped(&first).await);
        assert_ne!(published_runtime()?.id(), first.id());
        Ok(())
    }
}
