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
            let calls = VmCalls::of(frame.vm)?;
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
            principal,
        })
        .await
        .expect_err("a replaced runtime runs nothing");
    assert!(crate::services::plugin_dispatch::DispatchNotStarted::caused(&stale));
    Ok(())
}

fn plugin(id: &str) -> crate::services::plugin_scope::PluginId {
    crate::services::plugin_scope::PluginId::new(id).expect("plugin id")
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
