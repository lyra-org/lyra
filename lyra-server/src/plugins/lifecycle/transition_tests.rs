// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    sync::{
        Arc,
        atomic::{
            AtomicUsize,
            Ordering,
        },
    },
    time::{
        Duration,
        Instant,
    },
};

use anyhow::Context;
use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use tokio_util::sync::CancellationToken;

use super::{
    PublishOutcome,
    reentry_tests::start_plugin,
};
use crate::{
    plugins::{
        executor::{
            PluginSourceRun,
            VmCalls,
        },
        testing::{
            init_default_test_state,
            published_runtime,
            start_runtime,
            use_plugins_dir,
            write_plugin,
        },
    },
    services::{
        EntityType,
        STATE,
        mix::{
            MixRequestFields,
            MixViewer,
        },
        plugin_dispatch::{
            MetadataRefreshRequest,
            PluginCallbackHandle,
            RuntimeCalls,
        },
        providers::{
            ProviderCallError,
            ProviderCallStage,
            ProviderLookup,
            provider_registry,
            with_provider_call,
        },
    },
};

async fn reload() -> anyhow::Result<()> {
    crate::plugins::STATE
        .generation()
        .registries
        .reload_all_plugins()
        .await
}

fn in_transition() -> bool {
    STATE.generation().in_transition("anything")
}

/// Sets the stand-ins for modules a reload builds its runtimes with, until dropped.
struct TestModules;

impl TestModules {
    fn set(modules: fn() -> Vec<ModuleSpec>) -> Self {
        *crate::plugins::bootstrap::TEST_MODULES
            .lock()
            .expect("test modules poisoned") = Some(modules);
        Self
    }
}

impl Drop for TestModules {
    fn drop(&mut self) {
        *crate::plugins::bootstrap::TEST_MODULES
            .lock()
            .expect("test modules poisoned") = None;
    }
}

async fn refresh_call(provider: &str, value: i64) -> Result<i64, ProviderCallError> {
    with_provider_call(
        provider,
        ProviderCallStage::Lyrics,
        crate::services::plugin_dispatch::TransitionWait::background(CancellationToken::new()),
        |registry| {
            registry
                .get_refresh_callback(provider, EntityType::Release)
                .copied()
                .map(ProviderLookup::Call)
        },
        |handler: PluginCallbackHandle, calls| async move {
            let result = calls
                .metadata_refresh(MetadataRefreshRequest {
                    handler,
                    context: serde_json::json!({ "value": value }),
                    deadline: Instant::now() + Duration::from_secs(30),
                })
                .await?;
            result.values[0].as_i64().context("handler's answer")
        },
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pass_running_through_a_reload_skips_nothing() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    start_plugin(
        &plugins_dir,
        "slow-start",
        &["harmony.task", "lyra.metadata"],
        r#"
            local metadata = require("@lyra/metadata")
            metadata.Provider.new("slow-start"):refresh(metadata.EntityType.Release, function(ctx)
                return ctx.value
            end)
            require("@harmony/task").wait(0.3)
        "#,
    )
    .await?;

    let pass = tokio::spawn(async {
        let mut answers = Vec::new();
        for value in 0..40 {
            answers.push(refresh_call("slow-start", value).await);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        answers
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    reload().await?;

    for (value, answer) in pass.await?.into_iter().enumerate() {
        match answer {
            Ok(answer) => assert_eq!(answer, value as i64),
            Err(error) => panic!("item {value} was skipped: {error}"),
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_never_observes_a_partly_registered_runtime() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    start_plugin(
        &plugins_dir,
        "partial",
        &["harmony.task", "lyra.metadata"],
        r#"
            local metadata = require("@lyra/metadata")
            local task = require("@harmony/task")
            local handler = function() return nil end
            metadata.Provider.new("first"):refresh(metadata.EntityType.Release, handler)
            task.wait(0.3)
            metadata.Provider.new("second"):refresh(metadata.EntityType.Release, handler)
        "#,
    )
    .await?;

    let stop = CancellationToken::new();
    let watcher = tokio::spawn({
        let stop = stop.clone();
        async move {
            while !stop.is_cancelled() {
                let registry = provider_registry().read_owned().await;
                let first = registry.has_refresh_callback("first", EntityType::Release);
                let second = registry.has_refresh_callback("second", EntityType::Release);
                assert_eq!(first, second, "a partly registered runtime was published");
                assert!(first, "the plugin was missing from the published scope");
                drop(registry);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    });
    reload().await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    stop.cancel();
    watcher.await?;
    assert!(!in_transition());
    Ok(())
}

/// Mixes through the VM the way a host function reaches a mixer, and returns the tracks it got.
fn mix_module() -> ModuleSpec {
    ModuleSpec::new("harmony/crypt")
        .function(FunctionSpec::new("blend").call_async(Arc::new(|mut frame| {
            let track: String = frame.args.read_named("track")?;
            let mixer: String = frame.args.read_named("mixer")?;
            let calls = VmCalls::of(frame.vm, &frame.context)?;
            Ok(harmony_luau::ScheduledFuture::new(async move {
                let fields: MixRequestFields =
                    serde_json::from_value(serde_json::json!({ "q": track, "mixer": mixer }))
                        .map_err(crate::plugins::runtime_error)?;
                let viewer = MixViewer {
                    principal: None,
                    accessible_library_ids: None,
                    playlists: Default::default(),
                };
                let mixed = crate::services::mix::mix_on(
                    &calls,
                    tokio::time::Instant::now() + Duration::from_secs(15),
                    &fields.into_request(),
                    &viewer,
                )
                .await
                .map_err(crate::plugins::runtime_error)?;
                Ok(mixed.len() as i64)
            }))
        })))
        .function(FunctionSpec::new("chain_budget").call(|frame| {
            let remaining = crate::plugins::executor::CallChain::of(&frame.context)
                .wait_deadline()
                .saturating_duration_since(Instant::now());
            *CHAIN_BUDGET.lock().expect("chain budget") = Some(remaining);
            Ok(())
        }))
        .function(FunctionSpec::new("tick").call(|_frame| {
            RESCANS.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }))
        .install(|_| Ok(ModuleExport::new(())))
}

fn mixer_modules() -> Vec<ModuleSpec> {
    vec![mix_module()]
}

async fn registered_mixer() -> bool {
    crate::services::mix::mix_registry()
        .read()
        .await
        .is_registered("echo")
}

#[tokio::test(flavor = "multi_thread")]
async fn an_entrypoint_that_calls_a_mixer_completes_at_startup_and_in_a_reload()
-> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let _modules = TestModules::set(mixer_modules);
    let plugins_dir = tempfile::tempdir()?;
    init_default_test_state()?;
    start_runtime().await?;
    use_plugins_dir(plugins_dir.path());
    let track = {
        let mut db = STATE.db.write().await;
        let track = lyra_db::fixtures::insert_track(&mut db, "Track")?;
        lyra_db::tracks::get_by_id(&db, track)?.context("track")?.id
    };
    write_plugin(
        &plugins_dir.path().join("nested"),
        "nested",
        &["harmony.crypt", "lyra.mix"],
        &format!(
            r#"
                local mix = require("@lyra/mix")
                mix.Mixer.new("echo"):blend({{ query = true }}, function(ctx)
                    return {{ tracks = {{ {{ track_id = ctx.q }} }} }}
                end)
                assert(require("@harmony/crypt").blend("{track}", "echo") == 1, "the mixer did not run")
            "#
        ),
    )?;
    let runtime = crate::plugins::bootstrap::initialize_harmony().await?;

    tokio::time::timeout(
        Duration::from_secs(30),
        crate::plugins::STATE
            .generation()
            .registries
            .start_plugins(runtime, CancellationToken::new()),
    )
    .await
    .context("the startup entrypoint never finished")??;
    assert!(registered_mixer().await, "the entrypoint failed at startup");

    tokio::time::timeout(Duration::from_secs(30), reload())
        .await
        .context("the reload entrypoint never finished")??;
    assert!(
        registered_mixer().await,
        "the entrypoint failed in a reload"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_router_build_failure_publishes_and_leaves_no_transition_open() -> anyhow::Result<()> {
    struct FailingBuild;

    impl crate::plugins::api::RouteHost for FailingBuild {
        fn build(
            &self,
            _routes: Vec<crate::plugins::api::RegisteredRoute>,
            _case_insensitive: crate::plugins::api::CaseInsensitiveRouter,
        ) -> futures::future::BoxFuture<'_, anyhow::Result<crate::plugins::api::InstallRoutes>>
        {
            Box::pin(async { anyhow::bail!("route build failed") })
        }
    }

    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    let first = start_plugin(&plugins_dir, "routed", &[], "").await?;
    crate::plugins::api::initialize_registry(Default::default(), Some(Arc::new(FailingBuild)))
        .await;

    let error = reload().await.expect_err("the build failure is reported");

    assert!(
        format!("{error:#}").contains("route build failed"),
        "{error:#}"
    );
    assert!(!in_transition());
    assert_ne!(published_runtime()?.id(), first.id());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_during_a_reload_clears_the_pointer_and_settles() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    start_plugin(&plugins_dir, "stuck", &["harmony.task"], "").await?;
    std::fs::write(
        plugins_dir.path().join("stuck/init.luau"),
        r#"require("@harmony/task").wait(600)"#,
    )?;
    let first_shutdown = CancellationToken::new();
    let cut = first_shutdown.clone();
    let reloading = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        cut.cancel();
    });
    let outcome = crate::plugins::STATE
        .generation()
        .registries
        .spawn_reload(None, first_shutdown)
        .await?;
    reloading.await?;

    assert_eq!(outcome, PublishOutcome::ShuttingDown);
    assert!(!in_transition());
    assert!(STATE.generation().latest().is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn an_executor_dead_before_its_entrypoints_publishes_and_settles() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    init_default_test_state()?;
    start_runtime().await?;
    use_plugins_dir(plugins_dir.path());
    let runtime = crate::plugins::bootstrap::initialize_harmony().await?;
    let _ = runtime.inject_panic().await;

    crate::plugins::STATE
        .generation()
        .registries
        .start_plugins(runtime.clone(), CancellationToken::new())
        .await?;

    assert!(!in_transition());
    assert_eq!(published_runtime()?.id(), runtime.id());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn an_old_route_gets_503_and_reaches_no_other_handler() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    let entrypoint = |answer: i64| {
        format!(
            r#"
                local api = require("@lyra/api")
                api.get("/old-route", function()
                    return api.response.json(200, {{ answer = {answer} }})
                end, {{ auth = "public" }})
            "#
        )
    };
    start_plugin(&plugins_dir, "routed", &["lyra.api"], &entrypoint(1)).await?;
    let old_route = crate::plugins::api::registered_routes(
        &crate::plugins::STATE
            .scope()
            .context("published scope")?
            .vm_scope
            .plugin
            .api,
    )
    .await
    .0
    .into_iter()
    .find(|route| route.key.path.as_ref() == "/old-route")
    .context("the route")?;
    std::fs::write(plugins_dir.path().join("routed/init.luau"), entrypoint(2))?;
    reload().await?;

    let response = crate::routes::plugin_api::call_route(&old_route).await;

    assert_eq!(response, axum::http::StatusCode::SERVICE_UNAVAILABLE);
    Ok(())
}

static RESCANS: AtomicUsize = AtomicUsize::new(0);
static CHAIN_BUDGET: std::sync::Mutex<Option<Duration>> = std::sync::Mutex::new(None);

fn counting_modules() -> Vec<ModuleSpec> {
    vec![
        ModuleSpec::new("harmony/crypt")
            .function(FunctionSpec::new("tick").call(|_frame| {
                RESCANS.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }))
            .install(|_| Ok(ModuleExport::new(()))),
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lyrics_handler_registered_during_a_reload_gets_its_rescan() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let _modules = TestModules::set(counting_modules);
    let plugins_dir = tempfile::tempdir()?;
    start_plugin(
        &plugins_dir,
        "lyrics-reload",
        &[
            "harmony.http",
            "harmony.crypt",
            "harmony.task",
            "lyra.metadata",
        ],
        r#"
            local http = require("@harmony/http")
            local metadata = require("@lyra/metadata")
            local ticks = require("@harmony/crypt")
            http.set_rate_limit({ domain = "lyrics-reload.example" })
            metadata.Provider.new("lyrics-reload"):lyrics({}, function()
                ticks.tick()
                return nil
            end)
            require("@harmony/task").wait(0.2)
        "#,
    )
    .await?;
    {
        let mut db = STATE.db.write().await;
        lyra_db::fixtures::insert_track(&mut db, "Track")?;
    }
    RESCANS.store(0, Ordering::SeqCst);

    reload().await?;

    let deadline = Instant::now() + Duration::from_secs(10);
    while RESCANS.load(Ordering::SeqCst) == 0 {
        anyhow::ensure!(
            Instant::now() < deadline,
            "the rescan never reached the new handler"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn id_links_projected_during_entrypoints_use_the_new_vms_generators() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    init_default_test_state()?;
    start_runtime().await?;
    use_plugins_dir(plugins_dir.path());
    let release = {
        let mut db = STATE.db.write().await;
        let release = lyra_db::fixtures::insert_release(&mut db, "Release")?;
        lyra_db::providers::register(&mut db, "linker")?;
        lyra_db::external_ids::upsert(
            &mut db,
            release,
            "linker",
            "thing_id",
            "x",
            lyra_db::IdSource::Resolved,
        )?;
        lyra_db::releases::get_by_id(&db, release)?
            .context("release")?
            .id
    };
    let entrypoint = |host: &str| {
        format!(
            r#"
                local entities = require("@lyra/entities")
                local metadata = require("@lyra/metadata")
                local provider = metadata.Provider.new("linker")
                provider:id(
                    {{ id_type = "thing_id", entity = metadata.EntityType.Release, scheme = "test:linker" }},
                    function(id) return "https://{host}.example/" .. id end
                )
                local release = entities.query_release({{ id = "{release}", include = "links" }})
                assert(release.includes.links[1].url == "https://{host}.example/x", "wrong generator")
            "#
        )
    };
    let id = "linker-plugin";
    write_plugin(
        &plugins_dir.path().join(id),
        id,
        &["lyra.entities", "lyra.metadata"],
        &entrypoint("old"),
    )?;
    let runtime = crate::plugins::bootstrap::initialize_harmony().await?;
    crate::plugins::STATE
        .generation()
        .registries
        .start_plugins(runtime, CancellationToken::new())
        .await?;
    assert_eq!(linked_providers().await, 1, "the startup entrypoint failed");

    std::fs::write(
        plugins_dir.path().join(id).join("init.luau"),
        entrypoint("new"),
    )?;
    reload().await?;

    assert_eq!(linked_providers().await, 1, "the reload entrypoint failed");
    Ok(())
}

async fn linked_providers() -> usize {
    provider_registry()
        .read()
        .await
        .id_pairs(EntityType::Release)
        .len()
}

#[tokio::test(flavor = "multi_thread")]
async fn plugin_source_runs_follow_the_published_runtime() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    let first = start_plugin(&plugins_dir, "sourced", &[], "").await?;
    reload().await?;

    let principal = super::reentry_tests::admin_principal().await?;
    let stale = first
        .run_source(PluginSourceRun {
            plugin_id: "sourced".to_string(),
            path: "chunk.luau".to_string(),
            source: Arc::from(&b""[..]),
            args: Box::new(|_, _| Ok(Vec::new())),
            auth: crate::plugins::testing::acting_as(principal),
        })
        .await
        .expect_err("a replaced runtime runs nothing");
    assert!(crate::services::plugin_dispatch::DispatchNotStarted::caused(&stale));
    Ok(())
}

async fn write_restart_plugins(plugins_dir: &tempfile::TempDir) -> anyhow::Result<()> {
    write_plugin(
        &plugins_dir.path().join("bystander"),
        "bystander",
        &["lyra.metadata"],
        r#"
            local metadata = require("@lyra/metadata")
            metadata.Provider.new("bystander"):refresh(metadata.EntityType.Release, function(ctx)
                return ctx.value
            end)
        "#,
    )?;
    Ok(())
}

const SLOW_RESTARTING: &str = r#"
    local metadata = require("@lyra/metadata")
    metadata.Provider.new("restarting"):refresh(metadata.EntityType.Release, function(ctx)
        return ctx.value
    end)
    require("@harmony/task").wait(1)
"#;

async fn restart(plugin_id: &str) -> Result<(), super::PluginRestartError> {
    crate::plugins::STATE
        .generation()
        .registries
        .restart_plugin(&crate::services::plugin_scope::PluginId::new(plugin_id).expect("id"))
        .await
}

async fn wait_for_transition(target: &str) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !STATE.generation().in_transition(target) {
        anyhow::ensure!(Instant::now() < deadline, "the restart never began");
        tokio::task::yield_now().await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pass_through_a_restart_skips_nothing_and_other_plugins_do_not_wait() -> anyhow::Result<()>
{
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    write_restart_plugins(&plugins_dir).await?;
    start_plugin(
        &plugins_dir,
        "restarting",
        &["harmony.task", "lyra.metadata"],
        SLOW_RESTARTING,
    )
    .await?;

    let pass = tokio::spawn(async {
        let mut answers = Vec::new();
        for value in 0..30 {
            answers.push(refresh_call("restarting", value).await);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        answers
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let restarted = tokio::spawn(async { restart("restarting").await });
    wait_for_transition("restarting").await?;

    assert!(
        !STATE.generation().in_transition("bystander"),
        "a restart holds up only that plugin's calls"
    );
    // Well into the new entrypoint, which takes 1 s.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        refresh_call("bystander", 7)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?,
        7
    );
    assert!(
        !restarted.is_finished(),
        "the bystander's call finished while the entrypoint was still running"
    );

    restarted.await??;
    for (value, answer) in pass.await?.into_iter().enumerate() {
        match answer {
            Ok(answer) => assert_eq!(answer, value as i64),
            Err(error) => panic!("item {value} was skipped: {error}"),
        }
    }
    assert!(!in_transition());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pass_planned_during_a_restart_still_includes_the_plugin() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    start_plugin(
        &plugins_dir,
        "restarting",
        &["harmony.task", "lyra.metadata"],
        SLOW_RESTARTING,
    )
    .await?;

    let restarted = tokio::spawn(async { restart("restarting").await });
    wait_for_transition("restarting").await?;
    let deadline = Instant::now() + Duration::from_millis(200);
    while Instant::now() < deadline {
        let planned = provider_registry()
            .read()
            .await
            .providers_with_refresh_handler(EntityType::Release);
        assert_eq!(planned, vec!["restarting".to_string()]);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    restarted.await??;
    Ok(())
}

async fn has_provider(provider: &str) -> bool {
    provider_registry()
        .read()
        .await
        .has_refresh_callback(provider, EntityType::Release)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_restart_entrypoint_leaves_the_plugin_absent_and_settles() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    start_plugin(
        &plugins_dir,
        "restarting",
        &["harmony.task", "lyra.metadata"],
        SLOW_RESTARTING,
    )
    .await?;
    assert!(has_provider("restarting").await);
    std::fs::write(
        plugins_dir.path().join("restarting/init.luau"),
        r#"error("entrypoint failed")"#,
    )?;

    restart("restarting")
        .await
        .expect_err("the entrypoint fails");

    assert!(!has_provider("restarting").await);
    assert!(!in_transition());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_conflict_at_replacement_leaves_the_plugin_absent_and_settles() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    write_restart_plugins(&plugins_dir).await?;
    start_plugin(
        &plugins_dir,
        "restarting",
        &["harmony.task", "lyra.metadata"],
        SLOW_RESTARTING,
    )
    .await?;
    std::fs::write(
        plugins_dir.path().join("restarting/init.luau"),
        r#"
            local metadata = require("@lyra/metadata")
            metadata.Provider.new("bystander"):refresh(metadata.EntityType.Release, function()
                return nil
            end)
        "#,
    )?;

    let error = restart("restarting")
        .await
        .expect_err("another plugin holds the provider id");

    assert!(error.to_string().contains("already registered"), "{error}");
    assert!(!has_provider("restarting").await);
    assert!(
        has_provider("bystander").await,
        "the other plugin is untouched"
    );
    assert!(!in_transition());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_handler_running_during_a_restart_finishes_with_its_result() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    let first = start_plugin(
        &plugins_dir,
        "held",
        &["harmony.task", "lyra.metadata"],
        r#"
            local metadata = require("@lyra/metadata")
            metadata.Provider.new("held"):refresh(metadata.EntityType.Release, function(ctx)
                require("@harmony/task").wait(0.4)
                return ctx.value
            end)
        "#,
    )
    .await?;

    let call = tokio::spawn(async { refresh_call("held", 42).await });
    let deadline = Instant::now() + Duration::from_secs(5);
    while first.slots_in_use() == 0 {
        anyhow::ensure!(Instant::now() < deadline, "the handler never took its slot");
        tokio::task::yield_now().await;
    }

    restart("held").await?;

    let answer = call.await?.map_err(|error| anyhow::anyhow!("{error}"))?;
    assert_eq!(answer, 42);
    Ok(())
}

fn plugin(id: &str) -> crate::services::plugin_scope::PluginId {
    crate::services::plugin_scope::PluginId::new(id).expect("plugin id")
}

fn route(owner: &str, path: &str, case_insensitive: bool) -> crate::plugins::api::RegisteredRoute {
    crate::plugins::api::RegisteredRoute {
        key: crate::plugins::api::RouteKey::new("GET", path).expect("route key"),
        plugin_id: plugin(owner),
        auth_mode: crate::plugins::api::RouteAuthMode::Public,
        case_insensitive,
        max_body_bytes: None,
        handler: PluginCallbackHandle {
            vm_id: 1,
            handler_id: 1,
        },
        kind: crate::plugins::api::RouteKind::Http,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn no_registry_is_readable_until_every_lock_of_a_restart_commit_is_held() -> anyhow::Result<()>
{
    let _guard = crate::services::testing::runtime_test_lock().await;
    init_default_test_state()?;
    let scope = crate::plugins::state::VmScope::default();
    scope.begin_staging(plugin("restarting"));
    scope
        .write_providers(&plugin("restarting"))
        .await
        .register(plugin("restarting"), "staged".to_string(), None)?;

    // Routes are locked last, so this holds the commit with everything before them locked.
    let routes = scope.plugin.api.registry();
    let held = routes.clone().write_owned().await;
    let commit = {
        let scope = scope.clone();
        tokio::spawn(async move { scope.commit_staging().await })
    };
    let providers = scope.services.providers.registry();
    let settings = scope.plugin.settings.registry();
    let deadline = Instant::now() + Duration::from_secs(5);
    while settings.try_read().is_ok() {
        anyhow::ensure!(
            Instant::now() < deadline,
            "the commit never reached the settings"
        );
        tokio::task::yield_now().await;
    }
    assert!(providers.try_read().is_err(), "providers are locked first");
    assert!(
        scope.services.mix.registry().try_read().is_err(),
        "every registry before the held one is locked"
    );
    assert!(!commit.is_finished());

    drop(held);
    commit.await??;
    assert!(
        providers
            .read()
            .await
            .provider_ids(&plugin("restarting"))
            .contains("staged")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_provider_a_restart_adds_stays_held_until_the_transition_settles() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    init_default_test_state()?;
    let scope = crate::plugins::state::VmScope::default();
    scope.begin_staging(plugin("restarting"));
    scope
        .write_providers(&plugin("restarting"))
        .await
        .register(plugin("restarting"), "added".to_string(), None)?;

    let generation = STATE.generation();
    let transition = generation.begin_transition(crate::services::state::Transition::Plugin {
        plugin: plugin("restarting"),
        targets: Default::default(),
    });
    assert!(!generation.in_transition("added"));

    scope.commit_staging().await?;
    assert!(generation.in_transition("added"));
    drop(transition);
    assert!(!generation.in_transition("added"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_combined_case_insensitive_conflict_fails_the_commit_and_applies_nothing()
-> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    init_default_test_state()?;
    let scope = crate::plugins::state::VmScope::default();
    scope
        .write_routes(&plugin("owner"))
        .await
        .register(route("owner", "/items/{id}", true))?;
    scope.begin_staging(plugin("restarting"));
    scope
        .write_providers(&plugin("restarting"))
        .await
        .register(plugin("restarting"), "staged".to_string(), None)?;
    scope
        .write_routes(&plugin("restarting"))
        .await
        .register(route("restarting", "/items/{slug}", true))?;

    let error = scope
        .commit_staging()
        .await
        .expect_err("the combined lookup refuses both patterns");

    assert!(format!("{error:#}").contains("/items/{slug}"), "{error:#}");
    assert!(
        scope
            .services
            .providers
            .registry()
            .read()
            .await
            .provider_ids(&plugin("restarting"))
            .is_empty(),
        "a refused commit applies nothing"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_route_registered_behind_a_commit_lands_in_the_live_registry() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    init_default_test_state()?;
    let scope = crate::plugins::state::VmScope::default();
    scope.begin_staging(plugin("restarting"));

    let staged = scope.routes_for(&plugin("restarting"));
    let held = staged.clone().write_owned().await;
    let commit = {
        let scope = scope.clone();
        tokio::spawn(async move { scope.commit_staging().await })
    };
    // The lock is fair: the commit is queued first, the registration behind it.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let registration = {
        let scope = scope.clone();
        tokio::spawn(async move {
            scope
                .write_routes(&plugin("restarting"))
                .await
                .register(route("restarting", "/late", false))
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(held);
    commit.await??;
    registration.await??;

    let (routes, _) = crate::plugins::api::registered_routes(&scope.plugin.api).await;
    assert_eq!(
        routes.len(),
        1,
        "the registration reached the live registry"
    );
    Ok(())
}

async fn restart_detached(
    plugin_id: &str,
    shutdown: CancellationToken,
) -> Result<(), super::PluginRestartError> {
    crate::plugins::STATE
        .generation()
        .registries
        .clone()
        .restart_detached(plugin(plugin_id), shutdown)
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_queued_behind_the_lock_gives_up_at_shutdown() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    start_plugin(&plugins_dir, "queued", &[], "").await?;
    let held = crate::plugins::STATE
        .generation()
        .registries
        .hold_restart_lock()
        .await;
    let shutdown = CancellationToken::new();
    let restart = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { restart_detached("queued", shutdown).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!restart.is_finished());

    shutdown.cancel();

    let error = tokio::time::timeout(Duration::from_secs(5), restart)
        .await
        .context("the queued restart never gave up")??
        .expect_err("a queued restart ends at shutdown");
    assert!(error.to_string().contains("shutdown"), "{error}");
    drop(held);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_cuts_short_a_restart_entrypoint_and_settles() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    start_plugin(
        &plugins_dir,
        "stuck",
        &["harmony.task", "lyra.metadata"],
        r#"
            local metadata = require("@lyra/metadata")
            metadata.Provider.new("stuck"):refresh(metadata.EntityType.Release, function()
                return nil
            end)
        "#,
    )
    .await?;
    std::fs::write(
        plugins_dir.path().join("stuck/init.luau"),
        r#"require("@harmony/task").wait(600)"#,
    )?;
    let shutdown = CancellationToken::new();
    let restart = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { restart_detached("stuck", shutdown).await }
    });
    wait_for_transition("stuck").await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    shutdown.cancel();

    tokio::time::timeout(Duration::from_secs(5), restart)
        .await
        .context("shutdown never ended the entrypoint")??
        .expect_err("a restart cut short fails");
    assert!(!in_transition(), "cleaned up before settling");
    assert!(
        !has_provider("stuck").await,
        "the cancelled run left nothing"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_running_handlers_nested_call_to_its_own_mixer_finishes_during_a_drain()
-> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    let track = {
        init_default_test_state()?;
        start_runtime().await?;
        use_plugins_dir(plugins_dir.path());
        let mut db = STATE.db.write().await;
        let track = lyra_db::fixtures::insert_track(&mut db, "Track")?;
        lyra_db::tracks::get_by_id(&db, track)?.context("track")?.id
    };
    write_plugin(
        &plugins_dir.path().join("draining"),
        "draining",
        &["harmony.task", "lyra.mix"],
        r#"
            require("@lyra/mix").Mixer.new("echo"):blend({ query = true }, function(ctx)
                return { tracks = { { track_id = ctx.q } } }
            end)
        "#,
    )?;
    let runtime = crate::plugins::bootstrap::initialize_harmony().await?;
    crate::plugins::STATE
        .generation()
        .registries
        .start_plugins(runtime.clone(), CancellationToken::new())
        .await?;

    let call = super::reentry_tests::run_chunk(
        &runtime,
        "draining",
        r#"
            local track = ...
            require("@harmony/task").wait(0.4)
            local tracks = require("@lyra/mix").blend({ q = track, mixer = "echo" })
            assert(tracks and #tracks == 1, "the mixer did not run")
        "#,
        track,
        super::reentry_tests::admin_principal().await?,
    );
    super::reentry_tests::wait_for_slot(&runtime).await?;

    restart("draining").await?;

    call.await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_nested_call_to_a_restarting_plugin_waits_for_its_new_run() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    init_default_test_state()?;
    start_runtime().await?;
    use_plugins_dir(plugins_dir.path());
    let track = {
        let mut db = STATE.db.write().await;
        let track = lyra_db::fixtures::insert_track(&mut db, "Track")?;
        lyra_db::tracks::get_by_id(&db, track)?.context("track")?.id
    };
    write_plugin(
        &plugins_dir.path().join("target"),
        "target",
        &["harmony.task", "lyra.mix"],
        r#"
            require("@lyra/mix").Mixer.new("target"):blend({ query = true }, function(ctx)
                return { tracks = { { track_id = ctx.q } } }
            end)
            require("@harmony/task").wait(0.6)
        "#,
    )?;
    write_plugin(
        &plugins_dir.path().join("caller"),
        "caller",
        &["harmony.task", "lyra.mix"],
        "",
    )?;
    let runtime = crate::plugins::bootstrap::initialize_harmony().await?;
    crate::plugins::STATE
        .generation()
        .registries
        .start_plugins(runtime.clone(), CancellationToken::new())
        .await?;

    let restarted = tokio::spawn(async { restart("target").await });
    wait_for_transition("target").await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let call = super::reentry_tests::run_chunk(
        &runtime,
        "caller",
        r#"
            local track = ...
            local tracks = require("@lyra/mix").blend({ q = track, mixer = "target" })
            assert(tracks and #tracks == 1, "the call didn't wait for the new mixer")
        "#,
        track,
        super::reentry_tests::admin_principal().await?,
    );

    call.await??;
    restarted.await??;
    Ok(())
}

const WAITED_ROUTE: &str = r#"
    local api = require("@lyra/api")
    api.get("/waited", function()
        return api.response.empty(204)
    end, { auth = "public" })
    require("@harmony/task").wait(0.5)
"#;

async fn serve_waited_route(plugins_dir: &tempfile::TempDir) -> anyhow::Result<()> {
    write_plugin(
        &plugins_dir.path().join("waited"),
        "waited",
        &["harmony.task", "lyra.api"],
        WAITED_ROUTE,
    )?;
    init_default_test_state()?;
    start_runtime().await?;
    use_plugins_dir(plugins_dir.path());
    crate::routes::plugin_api::use_installed_routes().await;
    let runtime = crate::plugins::bootstrap::initialize_harmony().await?;
    crate::plugins::STATE
        .generation()
        .registries
        .start_plugins(runtime, CancellationToken::new())
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_route_request_waits_out_a_restart() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    serve_waited_route(&plugins_dir).await?;
    assert_eq!(
        crate::routes::plugin_api::get_installed("/waited").await,
        axum::http::StatusCode::NO_CONTENT
    );

    let restarted = tokio::spawn(async { restart("waited").await });
    let deadline = Instant::now() + Duration::from_secs(5);
    while !plugin_transition_open() {
        anyhow::ensure!(Instant::now() < deadline, "the restart never began");
        tokio::task::yield_now().await;
    }

    let status = crate::routes::plugin_api::get_installed("/waited").await;

    assert_eq!(status, axum::http::StatusCode::NO_CONTENT);
    restarted.await??;
    Ok(())
}

fn plugin_transition_open() -> bool {
    STATE.generation().route_is_held(&plugin("waited"))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_route_request_waits_out_a_reload() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    serve_waited_route(&plugins_dir).await?;

    let reloading = tokio::spawn(async { reload().await });
    let deadline = Instant::now() + Duration::from_secs(5);
    while !plugin_transition_open() {
        anyhow::ensure!(Instant::now() < deadline, "the reload never began");
        tokio::task::yield_now().await;
    }

    let status = crate::routes::plugin_api::get_installed("/waited").await;

    assert_eq!(status, axum::http::StatusCode::NO_CONTENT);
    reloading.await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn an_entrypoint_calls_the_mixer_it_registers_during_a_restart() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let _modules = TestModules::set(mixer_modules);
    let plugins_dir = tempfile::tempdir()?;
    init_default_test_state()?;
    start_runtime().await?;
    use_plugins_dir(plugins_dir.path());
    let track = {
        let mut db = STATE.db.write().await;
        let track = lyra_db::fixtures::insert_track(&mut db, "Track")?;
        lyra_db::tracks::get_by_id(&db, track)?.context("track")?.id
    };
    write_plugin(
        &plugins_dir.path().join("nested"),
        "nested",
        &["harmony.crypt", "lyra.mix"],
        &format!(
            r#"
                local mix = require("@lyra/mix")
                mix.Mixer.new("echo"):blend({{ query = true }}, function(ctx)
                    return {{ tracks = {{ {{ track_id = ctx.q }} }} }}
                end)
                assert(require("@harmony/crypt").blend("{track}", "echo") == 1, "the mixer did not run")
            "#
        ),
    )?;
    let runtime = crate::plugins::bootstrap::initialize_harmony().await?;
    crate::plugins::STATE
        .generation()
        .registries
        .start_plugins(runtime, CancellationToken::new())
        .await?;
    assert!(registered_mixer().await, "the entrypoint failed at startup");

    tokio::time::timeout(Duration::from_secs(10), restart("nested"))
        .await
        .context("the restart never finished")?
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    assert!(
        registered_mixer().await,
        "the entrypoint failed in the restart"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn id_links_projected_during_a_restart_use_its_new_generators() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    init_default_test_state()?;
    start_runtime().await?;
    use_plugins_dir(plugins_dir.path());
    let release = {
        let mut db = STATE.db.write().await;
        let release = lyra_db::fixtures::insert_release(&mut db, "Release")?;
        lyra_db::providers::register(&mut db, "linker")?;
        lyra_db::external_ids::upsert(
            &mut db,
            release,
            "linker",
            "thing_id",
            "x",
            lyra_db::IdSource::Resolved,
        )?;
        lyra_db::releases::get_by_id(&db, release)?
            .context("release")?
            .id
    };
    let entrypoint = |host: &str| {
        format!(
            r#"
                local entities = require("@lyra/entities")
                local metadata = require("@lyra/metadata")
                local provider = metadata.Provider.new("linker")
                provider:id(
                    {{ id_type = "thing_id", entity = metadata.EntityType.Release, scheme = "test:linker" }},
                    function(id) return "https://{host}.example/" .. id end
                )
                local release = entities.query_release({{ id = "{release}", include = "links" }})
                assert(release.includes.links[1].url == "https://{host}.example/x", "wrong generator")
            "#
        )
    };
    let id = "linker-plugin";
    write_plugin(
        &plugins_dir.path().join(id),
        id,
        &["lyra.entities", "lyra.metadata"],
        &entrypoint("old"),
    )?;
    let runtime = crate::plugins::bootstrap::initialize_harmony().await?;
    crate::plugins::STATE
        .generation()
        .registries
        .start_plugins(runtime, CancellationToken::new())
        .await?;

    std::fs::write(
        plugins_dir.path().join(id).join("init.luau"),
        entrypoint("new"),
    )?;
    restart(id)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?;

    assert_eq!(linked_providers().await, 1, "the restart entrypoint failed");
    Ok(())
}

/// A staging of `restarting` holding one provider, and a commit queued ahead of what the test
/// does next: the commit waits on `held`, the staged registry's write lock.
async fn commit_behind_a_held_staging() -> anyhow::Result<(
    crate::plugins::state::VmScope,
    tokio::sync::OwnedRwLockWriteGuard<crate::services::providers::ProviderRegistry>,
    tokio::task::JoinHandle<anyhow::Result<()>>,
)> {
    let scope = crate::plugins::state::VmScope::default();
    scope.begin_staging(plugin("restarting"));
    scope
        .write_providers(&plugin("restarting"))
        .await
        .register(plugin("restarting"), "staged".to_string(), None)?;
    let held = scope
        .providers_for(&plugin("restarting"))
        .write_owned()
        .await;
    let commit = {
        let scope = scope.clone();
        tokio::spawn(async move { scope.commit_staging().await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    Ok((scope, held, commit))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_view_taken_during_a_commit_cannot_wedge_it() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    init_default_test_state()?;
    let (scope, held, commit) = commit_behind_a_held_staging().await?;

    // A view started now waits behind the commit for the staged lock. Reading live first would
    // leave it holding what the commit needs next.
    let view = {
        let scope = scope.clone();
        tokio::spawn(async move {
            let view = scope.provider_view(Some(&plugin("restarting"))).await;
            view.read().await.provider_ids(&plugin("restarting"))
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(held);

    let (committed, ids) =
        tokio::time::timeout(Duration::from_secs(5), async { (commit.await, view.await) })
            .await
            .context("the view and the commit wedged each other")?;
    committed??;
    assert!(
        ids?.contains("staged"),
        "a view of a staging that committed reads the live registry"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_view_of_a_discarded_staging_reads_the_live_registry() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    init_default_test_state()?;
    let scope = crate::plugins::state::VmScope::default();
    let owner = plugin("restarting");
    scope
        .write_providers(&owner)
        .await
        .register(owner.clone(), "live".to_string(), None)?;
    scope.begin_staging(owner.clone());
    let held = scope.providers_for(&owner).write_owned().await;

    let view = {
        let scope = scope.clone();
        let owner = owner.clone();
        tokio::spawn(async move {
            let view = scope.provider_view(Some(&owner)).await;
            view.read().await.provider_ids(&owner)
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    scope.discard_staging();
    drop(held);

    assert!(view.await?.contains("live"), "the plugin's live bucket");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn external_ids_are_projected_from_the_calling_vms_scope() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    init_default_test_state()?;
    start_runtime().await?;
    use_plugins_dir(plugins_dir.path());
    let release = {
        let mut db = STATE.db.write().await;
        let release = lyra_db::fixtures::insert_release(&mut db, "Release")?;
        lyra_db::providers::register(&mut db, "ids")?;
        lyra_db::external_ids::upsert(
            &mut db,
            release,
            "ids",
            "thing_id",
            "x",
            lyra_db::IdSource::Resolved,
        )?;
        lyra_db::releases::get_by_id(&db, release)?
            .context("release")?
            .id
    };
    // The scheme exists only in this runtime's scope while the entrypoint runs: nothing has
    // published it, and the projection asks for external ids without any links.
    write_plugin(
        &plugins_dir.path().join("ids-plugin"),
        "ids-plugin",
        &["lyra.entities", "lyra.metadata"],
        &format!(
            r#"
                local entities = require("@lyra/entities")
                local metadata = require("@lyra/metadata")
                metadata.Provider.new("ids"):id(
                    {{ id_type = "thing_id", entity = metadata.EntityType.Release, scheme = "test:ids" }}
                )
                local release = entities.query_release({{ id = "{release}", include = "external_ids" }})
                assert(release.includes.external_ids["test:ids"] == "x", "the scheme was not resolved")
            "#
        ),
    )?;
    let runtime = crate::plugins::bootstrap::initialize_harmony().await?;
    crate::plugins::STATE
        .generation()
        .registries
        .start_plugins(runtime, CancellationToken::new())
        .await?;

    assert_eq!(linked_providers().await, 1, "the entrypoint failed");
    Ok(())
}

async fn seeded_track(plugins_dir: &tempfile::TempDir) -> anyhow::Result<String> {
    init_default_test_state()?;
    start_runtime().await?;
    use_plugins_dir(plugins_dir.path());
    let mut db = STATE.db.write().await;
    let track = lyra_db::fixtures::insert_track(&mut db, "Track")?;
    Ok(lyra_db::tracks::get_by_id(&db, track)?.context("track")?.id)
}

async fn start_written_plugins() -> anyhow::Result<crate::plugins::bootstrap::PluginRuntime> {
    let runtime = crate::plugins::bootstrap::initialize_harmony().await?;
    crate::plugins::STATE
        .generation()
        .registries
        .start_plugins(runtime.clone(), CancellationToken::new())
        .await?;
    Ok(runtime)
}

const ECHO_MIXER: &str = r#"
    require("@lyra/mix").Mixer.new("echo"):blend({ query = true }, function(ctx)
        return { tracks = { { track_id = ctx.q } } }
    end)
"#;

#[tokio::test(flavor = "multi_thread")]
async fn a_chain_back_into_a_draining_plugin_finishes() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let _modules = TestModules::set(mixer_modules);
    let plugins_dir = tempfile::tempdir()?;
    let track = seeded_track(&plugins_dir).await?;
    write_plugin(
        &plugins_dir.path().join("first"),
        "first",
        &["harmony.crypt", "harmony.task", "lyra.mix"],
        ECHO_MIXER,
    )?;
    write_plugin(
        &plugins_dir.path().join("second"),
        "second",
        &["harmony.crypt", "lyra.mix"],
        r#"
            local crypt = require("@harmony/crypt")
            require("@lyra/mix").Mixer.new("relay"):blend({ query = true }, function(ctx)
                assert(crypt.blend(ctx.q, "echo") == 1, "the call back into first failed")
                return { tracks = { { track_id = ctx.q } } }
            end)
        "#,
    )?;
    let runtime = start_written_plugins().await?;

    let call = super::reentry_tests::run_chunk(
        &runtime,
        "first",
        r#"
            local track = ...
            require("@harmony/task").wait(0.4)
            assert(require("@harmony/crypt").blend(track, "relay") == 1, "the relay failed")
        "#,
        track,
        super::reentry_tests::admin_principal().await?,
    );
    super::reentry_tests::wait_for_slot(&runtime).await?;

    let started = Instant::now();
    restart("first").await?;

    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the chain waited on its own drain"
    );
    call.await??;
    Ok(())
}

fn listen_update() -> crate::services::listens::ListenUpdatePayload {
    crate::services::listens::ListenUpdatePayload {
        event: "recorded".to_string(),
        user_id: "user".to_string(),
        listens: Vec::new(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_nested_call_from_an_update_callback_never_waits_on_the_executor() -> anyhow::Result<()> {
    use crate::services::plugin_dispatch::PluginDispatch;

    let _guard = crate::services::testing::runtime_test_lock().await;
    let _modules = TestModules::set(mixer_modules);
    let plugins_dir = tempfile::tempdir()?;
    let track = seeded_track(&plugins_dir).await?;
    write_plugin(
        &plugins_dir.path().join("target"),
        "target",
        &["harmony.task", "lyra.mix"],
        &format!("{ECHO_MIXER}\nrequire(\"@harmony/task\").wait(1)"),
    )?;
    write_plugin(
        &plugins_dir.path().join("listener"),
        "listener",
        &["harmony.crypt", "lyra.listens", "lyra.listens.all"],
        &format!(
            r#"
                local crypt = require("@harmony/crypt")
                require("@lyra/listens").on_update(function()
                    if crypt.blend("{track}", "echo") == 0 then
                        crypt.tick()
                    end
                end)
            "#
        ),
    )?;
    let runtime = start_written_plugins().await?;
    RESCANS.store(0, Ordering::SeqCst);

    let restarted = tokio::spawn(async { restart("target").await });
    wait_for_transition("echo").await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    runtime.dispatch_listen_update(listen_update())?;

    tokio::time::timeout(Duration::from_secs(5), restarted)
        .await
        .context("the callback's wait held up the restart it was waiting for")???;
    assert_eq!(
        RESCANS.load(Ordering::SeqCst),
        1,
        "the nested call found nothing and went on"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_background_task_cannot_start_a_handler_in_a_quiesced_plugin() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let _modules = TestModules::set(mixer_modules);
    let plugins_dir = tempfile::tempdir()?;
    let track = seeded_track(&plugins_dir).await?;
    write_plugin(
        &plugins_dir.path().join("target"),
        "target",
        &["lyra.mix"],
        ECHO_MIXER,
    )?;
    write_plugin(
        &plugins_dir.path().join("background"),
        "background",
        &["harmony.crypt", "harmony.task", "lyra.mix"],
        &format!(
            r#"
                local crypt = require("@harmony/crypt")
                require("@harmony/task").delay(0.3, function()
                    if crypt.blend("{track}", "echo") == 0 then
                        crypt.tick()
                    end
                end)
            "#
        ),
    )?;
    let runtime = start_written_plugins().await?;
    RESCANS.store(0, Ordering::SeqCst);

    runtime
        .drain_plugin("target", Duration::from_secs(1), &CancellationToken::new())
        .await?;

    let deadline = Instant::now() + Duration::from_secs(5);
    while RESCANS.load(Ordering::SeqCst) == 0 {
        anyhow::ensure!(
            Instant::now() < deadline,
            "the quiesced plugin ran the handler"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    runtime.reopen_plugin("target").await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn updates_stay_out_of_a_replacement_until_it_is_published() -> anyhow::Result<()> {
    use crate::services::plugin_dispatch::PluginDispatch;

    let _guard = crate::services::testing::runtime_test_lock().await;
    let _modules = TestModules::set(mixer_modules);
    let plugins_dir = tempfile::tempdir()?;
    seeded_track(&plugins_dir).await?;
    write_plugin(
        &plugins_dir.path().join("listener"),
        "listener",
        &[
            "harmony.crypt",
            "harmony.task",
            "lyra.listens",
            "lyra.listens.all",
        ],
        r#"
            local crypt = require("@harmony/crypt")
            require("@lyra/listens").on_update(function()
                crypt.tick()
            end)
            require("@harmony/task").wait(0.6)
        "#,
    )?;
    let runtime = start_written_plugins().await?;
    RESCANS.store(0, Ordering::SeqCst);

    let restarted = tokio::spawn(async { restart("listener").await });
    let deadline = Instant::now() + Duration::from_secs(5);
    while !STATE.generation().route_is_held(&plugin("listener")) {
        anyhow::ensure!(
            Instant::now() < deadline,
            "the restart never reached its gate"
        );
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    runtime.dispatch_listen_update(listen_update())?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        RESCANS.load(Ordering::SeqCst),
        0,
        "the unpublished run got the update"
    );

    restarted.await??;
    runtime.dispatch_listen_update(listen_update())?;
    let deadline = Instant::now() + Duration::from_secs(5);
    while RESCANS.load(Ordering::SeqCst) == 0 {
        anyhow::ensure!(
            Instant::now() < deadline,
            "the published run never got an update"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_listing_omits_a_restarting_plugins_links_and_does_not_remember_that()
-> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    init_default_test_state()?;
    start_runtime().await?;
    use_plugins_dir(plugins_dir.path());
    let release = {
        let mut db = STATE.db.write().await;
        let release = lyra_db::fixtures::insert_release(&mut db, "Release")?;
        lyra_db::providers::register(&mut db, "linker")?;
        lyra_db::external_ids::upsert(
            &mut db,
            release,
            "linker",
            "thing_id",
            "x",
            lyra_db::IdSource::Resolved,
        )?;
        lyra_db::releases::get_by_id(&db, release)?
            .context("release")?
            .id
    };
    write_plugin(
        &plugins_dir.path().join("linker-plugin"),
        "linker-plugin",
        &["harmony.task", "lyra.metadata"],
        r#"
            local metadata = require("@lyra/metadata")
            metadata.Provider.new("linker"):id(
                { id_type = "thing_id", entity = metadata.EntityType.Release, scheme = "test:linker" },
                function(id) return "https://new.example/" .. id end
            )
            require("@harmony/task").wait(0.8)
        "#,
    )?;
    write_plugin(
        &plugins_dir.path().join("reader"),
        "reader",
        &["lyra.entities"],
        "",
    )?;
    let runtime = start_written_plugins().await?;
    let principal = super::reentry_tests::admin_principal().await?;

    // A running handler keeps the drain open, so the old run's generator is still callable.
    let held = super::reentry_tests::run_chunk(
        &runtime,
        "linker-plugin",
        r#"require("@harmony/task").wait(0.8)"#,
        String::new(),
        principal.clone(),
    );
    super::reentry_tests::wait_for_slot(&runtime).await?;
    let restarted = tokio::spawn(async { restart("linker-plugin").await });
    wait_for_transition("linker").await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let started = Instant::now();
    super::reentry_tests::run_chunk(
        &runtime,
        "reader",
        r#"
            local release = require("@lyra/entities").query_release({ id = (...), include = "links" })
            local links = release.includes.links
            assert(links == nil or #links == 0, "a link from a plugin being replaced")
        "#,
        release.clone(),
        principal.clone(),
    )
    .await??;
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "the listing waited"
    );
    held.await??;

    restarted.await??;
    super::reentry_tests::run_chunk(
        &runtime,
        "reader",
        r#"
            local release = require("@lyra/entities").query_release({ id = (...), include = "links" })
            assert(release.includes.links[1].url == "https://new.example/x", "the omission stuck")
        "#,
        release,
        principal,
    )
    .await??;
    Ok(())
}

fn take_chain_budget() -> anyhow::Result<Duration> {
    CHAIN_BUDGET
        .lock()
        .expect("chain budget")
        .take()
        .context("the handler never reported its chain")
}

#[tokio::test(flavor = "multi_thread")]
async fn queued_metadata_handlers_run_with_their_own_deadline_in_the_chain() -> anyhow::Result<()> {
    use crate::services::plugin_dispatch::{
        DispatchCancellation,
        PluginDispatch,
        SimilarReleasesDispatchRequest,
    };

    let _guard = crate::services::testing::runtime_test_lock().await;
    let _modules = TestModules::set(mixer_modules);
    let plugins_dir = tempfile::tempdir()?;
    seeded_track(&plugins_dir).await?;
    write_plugin(
        &plugins_dir.path().join("chained"),
        "chained",
        &["harmony.crypt", "lyra.metadata"],
        r#"
            local crypt = require("@harmony/crypt")
            local metadata = require("@lyra/metadata")
            local provider = metadata.Provider.new("chained")
            provider:refresh(metadata.EntityType.Release, function(ctx)
                crypt.chain_budget()
                return ctx.value
            end)
            provider:similar_releases({}, function()
                crypt.chain_budget()
                return { candidates = {} }
            end)
        "#,
    )?;
    let runtime = start_written_plugins().await?;

    // The refresh has 30 s, more than the 15 s of a task with no chain.
    refresh_call("chained", 1)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    let refresh = take_chain_budget()?;
    assert!(refresh > Duration::from_secs(20), "{refresh:?}");

    let handler = provider_registry()
        .read()
        .await
        .get_similar_releases_handler("chained")
        .context("similar handler")?
        .handler;
    runtime
        .dispatch_similar_releases(SimilarReleasesDispatchRequest {
            provider_id: "chained".to_string(),
            handler,
            context: serde_json::json!({}),
            timeout: Duration::from_secs(8),
            cancellation: DispatchCancellation::default(),
            max_candidates: 1,
        })
        .await?;
    let similar = take_chain_budget()?;
    assert!(similar <= Duration::from_secs(8), "{similar:?}");
    Ok(())
}
