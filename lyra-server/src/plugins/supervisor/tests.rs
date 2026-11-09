// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::path::Path;

use super::*;
use crate::plugins::testing::{
    published_runtime,
    use_plugins_dir,
    write_plugin,
};

const PROVIDER: &str = "supervised";

/// Installs a plugin into `plugins_dir` that registers a refresh handler for [`PROVIDER`].
fn install_provider_plugin(plugins_dir: &Path) -> anyhow::Result<()> {
    write_plugin(
        &plugins_dir.join(PROVIDER),
        PROVIDER,
        &["lyra.metadata"],
        &format!(
            r#"
                local metadata = require("@lyra/metadata")
                metadata.Provider.new("{PROVIDER}"):refresh(metadata.EntityType.Release, function()
                    return nil
                end)
            "#
        ),
    )
}

async fn init_state(plugins_dir: &Path) -> anyhow::Result<()> {
    crate::plugins::testing::init_default_test_state()?;
    crate::plugins::testing::start_runtime().await?;
    crate::plugins::api::initialize_registry(std::collections::HashSet::new(), None).await;
    crate::plugins::settings::initialize_registry().await;
    use_plugins_dir(plugins_dir);
    Ok(())
}

/// Fresh state over `plugins_dir`, with the startup runtime started and published as the
/// server does.
async fn start_plugins(plugins_dir: &Path) -> anyhow::Result<PluginRuntime> {
    init_state(plugins_dir).await?;
    let runtime = crate::plugins::bootstrap::initialize_harmony().await?;
    registries()
        .start_plugins(runtime.clone(), CancellationToken::new())
        .await?;
    Ok(runtime)
}

fn registries() -> crate::plugins::lifecycle::PluginRegistries {
    crate::plugins::STATE.generation().registries.clone()
}

fn supervisor(shutdown: &CancellationToken) -> Supervisor {
    Supervisor::new(crate::plugins::STATE.generation(), shutdown.clone())
}

/// Panics `runtime`'s executor thread and waits for it to end.
async fn crash(runtime: &PluginRuntime) -> anyhow::Result<()> {
    let exit = runtime.exit();
    runtime
        .inject_panic()
        .await
        .expect_err("a panicking executor drops the reply");
    anyhow::ensure!(exit.wait().await == ExecutorExitKind::Crashed);
    Ok(())
}

async fn has_refresh_handler() -> bool {
    crate::services::providers::provider_registry()
        .read()
        .await
        .get_refresh_callback(PROVIDER, crate::services::EntityType::Release)
        .is_some()
}

/// Waits for a runtime other than `crashed` to be published and run [`PROVIDER`] again.
async fn wait_for_restart(crashed: &PluginRuntime) -> anyhow::Result<PluginRuntime> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(runtime) = published_runtime()
            && runtime.id() != crashed.id()
            && runtime.has_plugin(PROVIDER).await.unwrap_or(false)
            && has_refresh_handler().await
        {
            return Ok(runtime);
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "no fresh runtime ran the plugin again"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn orderly_exits_report_no_crash() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    let first = start_plugins(plugins_dir.path()).await?;
    let crashes = RuntimeCrashes::default();
    let mut reports = crashes.take_receiver().context("crash receiver")?;
    crashes.observe(&first);

    let unused = crate::plugins::bootstrap::initialize_harmony().await?;
    crashes.observe(&unused);
    unused.shutdown().await?;
    let dropped = crate::plugins::bootstrap::initialize_harmony().await?;
    crashes.observe(&dropped);
    drop(dropped);
    // Shuts the first runtime down to replace it.
    registries().reload_all_plugins().await?;

    // Each watcher keeps a sender until its runtime has ended, so the channel
    // closes once all three have, and only crashes would come before that.
    drop(crashes);
    let report = tokio::time::timeout(Duration::from_secs(10), reports.recv())
        .await
        .context("a runtime never ended")?;
    assert_eq!(report, None);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn supervisor_restarts_a_crashed_runtime_with_its_plugins() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    install_provider_plugin(plugins_dir.path())?;
    let first = start_plugins(plugins_dir.path()).await?;
    assert!(first.has_plugin(PROVIDER).await?);
    assert!(has_refresh_handler().await);

    let shutdown = CancellationToken::new();
    let supervisor = spawn(shutdown.clone())?;
    crash(&first).await?;
    let restarted = wait_for_restart(&first).await?;
    assert_eq!(restarted.plugin_manifests().await?.len(), 1);

    shutdown.cancel();
    supervisor.await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_startup_runtime_that_crashed_before_it_was_published_is_restarted() -> anyhow::Result<()>
{
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    install_provider_plugin(plugins_dir.path())?;
    init_state(plugins_dir.path()).await?;
    let shutdown = CancellationToken::new();
    let supervisor = spawn(shutdown.clone())?;

    let startup = crate::plugins::bootstrap::initialize_harmony().await?;
    crash(&startup).await?;
    registries()
        .start_plugins(startup.clone(), CancellationToken::new())
        .await?;
    wait_for_restart(&startup).await?;

    shutdown.cancel();
    supervisor.await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_of_a_replaced_runtime_is_not_restarted() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    let replaced = start_plugins(plugins_dir.path()).await?;
    let current = crate::plugins::bootstrap::initialize_harmony().await?;
    crate::plugins::STATE.publish_runtime(&crate::services::STATE.generation(), current.clone());

    crash(&replaced).await?;
    let outcome = supervisor(&CancellationToken::new())
        .handle_crash(replaced.id(), Instant::now())
        .await;
    assert_eq!(outcome, CrashOutcome::Replaced);
    assert_eq!(published_runtime()?.id(), current.id());
    assert!(!current.has_plugin("demo").await?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_reported_while_a_reload_holds_the_lock_defers_to_it() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    let crashed = start_plugins(plugins_dir.path()).await?;
    crash(&crashed).await?;

    let held = registries().hold_restart_lock().await;
    let mut supervisor = supervisor(&CancellationToken::new());
    let id = crashed.id();
    let restart = tokio::spawn(async move { supervisor.handle_crash(id, Instant::now()).await });
    tokio::task::yield_now().await;
    // What a reload does under the lock: publish a fresh runtime.
    let reloaded = crate::plugins::bootstrap::initialize_harmony().await?;
    crate::plugins::STATE.publish_runtime(&crate::services::STATE.generation(), reloaded.clone());
    drop(held);

    assert_eq!(restart.await?, CrashOutcome::Replaced);
    assert_eq!(published_runtime()?.id(), reloaded.id());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn supervisor_stops_restarting_after_repeated_crashes() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    let window = Duration::from_secs(600);
    let first = start_plugins(plugins_dir.path()).await?;
    let mut supervisor = supervisor(&CancellationToken::new()).with_limit(1, window);
    let start = Instant::now();

    crash(&first).await?;
    assert_eq!(
        supervisor.handle_crash(first.id(), start).await,
        CrashOutcome::Restarted
    );
    let second = published_runtime()?;
    assert_ne!(second.id(), first.id());

    crash(&second).await?;
    assert_eq!(
        supervisor
            .handle_crash(second.id(), start + Duration::from_secs(1))
            .await,
        CrashOutcome::GaveUp
    );
    assert_eq!(published_runtime()?.id(), second.id());

    registries().reload_all_plugins().await?;
    let third = published_runtime()?;
    crash(&third).await?;
    assert_eq!(
        supervisor.handle_crash(third.id(), start + window).await,
        CrashOutcome::Restarted,
        "restarts that left the window no longer count"
    );
    assert!(!published_runtime()?.has_plugin("demo").await?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_restart_leaves_the_crashed_runtime_published() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    let crashed = start_plugins(plugins_dir.path()).await?;
    crash(&crashed).await?;

    // Discovery fails on a plugins directory that is a file.
    let not_a_directory = plugins_dir.path().join("plugins");
    std::fs::write(&not_a_directory, "")?;
    use_plugins_dir(&not_a_directory);
    let outcome = supervisor(&CancellationToken::new())
        .handle_crash(crashed.id(), Instant::now())
        .await;
    assert_eq!(outcome, CrashOutcome::RestartFailed);
    assert_eq!(published_runtime()?.id(), crashed.id());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_ends_a_restart_waiting_for_the_lock_without_publishing() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    let crashed = start_plugins(plugins_dir.path()).await?;
    crash(&crashed).await?;

    let held = registries().hold_restart_lock().await;
    let shutdown = CancellationToken::new();
    let mut supervisor = supervisor(&shutdown);
    let id = crashed.id();
    let restart = tokio::spawn(async move { supervisor.handle_crash(id, Instant::now()).await });
    tokio::task::yield_now().await;
    shutdown.cancel();

    let outcome = tokio::time::timeout(Duration::from_secs(5), restart)
        .await
        .context("shutdown never ended the restart")??;
    assert_eq!(outcome, CrashOutcome::ShuttingDown);
    drop(held);
    assert_eq!(published_runtime()?.id(), crashed.id());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_cuts_short_the_entrypoints_of_a_restart() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    let crashed = start_plugins(plugins_dir.path()).await?;
    write_plugin(
        &plugins_dir.path().join("slow"),
        "slow",
        &["harmony.task"],
        r#"require("@harmony/task").wait(600)"#,
    )?;
    crash(&crashed).await?;

    let shutdown = CancellationToken::new();
    let mut supervisor = supervisor(&shutdown);
    let id = crashed.id();
    let restart = tokio::spawn(async move { supervisor.handle_crash(id, Instant::now()).await });
    // Long enough for the new entrypoint to be running; its scope publishes after it ends.
    tokio::time::sleep(Duration::from_millis(300)).await;
    shutdown.cancel();

    let outcome = tokio::time::timeout(Duration::from_secs(10), restart)
        .await
        .context("shutdown never ended the slow entrypoint")??;
    assert_eq!(outcome, CrashOutcome::ShuttingDown);
    assert!(published_runtime().is_err(), "shutdown clears the pointer");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_after_a_startup_crash_registers_after_startup_finalizes() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    write_plugin(
        &plugins_dir.path().join("late"),
        "late",
        &["harmony.task", "lyra.api"],
        r#"
            local api = require("@lyra/api")
            require("@harmony/task").wait(0.1)
            api.get("/late", function()
                return api.response.empty(204)
            end, { auth = "public" })
        "#,
    )?;
    init_state(plugins_dir.path()).await?;
    let shutdown = CancellationToken::new();
    let supervisor = spawn(shutdown.clone())?;

    let startup = crate::plugins::bootstrap::initialize_harmony().await?;
    crash(&startup).await?;
    registries()
        .start_plugins(startup.clone(), CancellationToken::new())
        .await?;

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(runtime) = published_runtime()
            && runtime.id() != startup.id()
            && crate::plugins::api::tests::registered_handler("GET", "/late")
                .await
                .is_some()
        {
            break;
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "the restarted plugin never registered its route"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    shutdown.cancel();
    supervisor.await?;
    Ok(())
}
