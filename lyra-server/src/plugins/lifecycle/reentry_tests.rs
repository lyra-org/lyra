// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    sync::Arc,
    time::{
        Duration,
        Instant,
    },
};

use anyhow::Context;
use tokio_util::sync::CancellationToken;

use crate::{
    plugins::{
        bootstrap::PluginRuntime,
        executor::PluginSourceRun,
        testing::{
            init_default_test_state,
            start_runtime,
            use_plugins_dir,
            write_plugin,
        },
    },
    services::{
        STATE,
        auth::Principal,
        providers::{
            ProviderCallStage,
            ProviderLookup,
            with_provider_call,
        },
    },
};

/// State whose reloads discover `plugin` in `plugins_dir`, with its runtime started.
async fn start_plugin(
    plugins_dir: &tempfile::TempDir,
    id: &str,
    scopes: &[&str],
    init: &str,
) -> anyhow::Result<PluginRuntime> {
    write_plugin(&plugins_dir.path().join(id), id, scopes, init)?;
    init_default_test_state()?;
    start_runtime().await?;
    use_plugins_dir(plugins_dir.path());
    let runtime = crate::plugins::bootstrap::initialize_harmony().await?;
    crate::plugins::STATE
        .generation()
        .registries
        .start_plugins(runtime.clone(), CancellationToken::new())
        .await?;
    Ok(runtime)
}

async fn admin_principal() -> anyhow::Result<Principal> {
    let user_db_id = {
        let mut db = STATE.db.write().await;
        let user_db_id = lyra_db::test_db::insert_user(&mut db, "reentry-admin")?;
        lyra_db::roles::ensure_builtin_roles(&mut db)?;
        let role = lyra_db::roles::get_by_name(&db, lyra_db::roles::BUILTIN_ADMIN_ROLE)?
            .and_then(|role| role.db_id)
            .context("built-in admin role")?;
        lyra_db::roles::assign_role_to_user(&mut db, user_db_id, role)?;
        user_db_id
    };
    crate::services::testing::user_principal(user_db_id).await
}

/// Runs `source` as `plugin_id` on `runtime` with `arg` as its `...`, holding a slot like any
/// dispatch.
fn run_chunk(
    runtime: &PluginRuntime,
    plugin_id: &str,
    source: &'static str,
    arg: String,
    principal: Principal,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    let runtime = runtime.clone();
    let plugin_id = plugin_id.to_string();
    tokio::spawn(async move {
        runtime
            .run_source(PluginSourceRun {
                plugin_id,
                path: "chunk.luau".to_string(),
                source: Arc::from(source.as_bytes()),
                args: Box::new(move |_, _| Ok(vec![harmony_luau::Value::String(arg.into_bytes())])),
                principal,
            })
            .await
    })
}

async fn wait_for_slot(runtime: &PluginRuntime) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.slots_in_use() == 0 {
        anyhow::ensure!(Instant::now() < deadline, "the chunk never took its slot");
        tokio::task::yield_now().await;
    }
    Ok(())
}

const ECHO_MIXER: &str = r#"
    local mix = require("@lyra/mix")
    mix.Mixer.new("echo"):blend({ query = true }, function(ctx)
        return { tracks = { { track_id = ctx.q } } }
    end)
"#;

#[tokio::test(flavor = "multi_thread")]
async fn a_running_handler_that_calls_a_mixer_during_a_drain_finishes() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    let runtime = start_plugin(
        &plugins_dir,
        "drained",
        &["harmony.task", "lyra.mix"],
        ECHO_MIXER,
    )
    .await?;
    let track = {
        let mut db = STATE.db.write().await;
        let track = lyra_db::fixtures::insert_track(&mut db, "Track")?;
        lyra_db::tracks::get_by_id(&db, track)?.context("track")?.id
    };

    // Long enough for the reload below to have started draining.
    let call = run_chunk(
        &runtime,
        "drained",
        r#"
            local track = ...
            require("@harmony/task").wait(0.5)
            local tracks = require("@lyra/mix").blend({ q = track, mixer = "echo" })
            assert(tracks and #tracks == 1 and tracks[1].track.id == track, "the mixer did not run")
        "#,
        track,
        admin_principal().await?,
    );
    wait_for_slot(&runtime).await?;

    let started = Instant::now();
    crate::plugins::STATE
        .generation()
        .registries
        .reload_all_plugins()
        .await?;

    assert!(started.elapsed() < super::RELOAD_DRAIN_LIMIT);
    call.await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_nested_refresh_deduplicates_by_what_the_calling_vm_declares() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    let runtime = start_plugin(
        &plugins_dir,
        "declaring",
        &["lyra.metadata", "lyra.metadata.refresh"],
        r#"
            local metadata = require("@lyra/metadata")
            local ET = metadata.EntityType
            local provider = metadata.Provider.new("declaring")
            provider:id({ id_type = "release_id", entity = ET.Release, unique = true, scheme = "declaring" })
            provider:refresh(ET.Release, function(ctx)
                local layer = provider:layer(ctx.id)
                layer:set_id("release_id", "same")
                layer:save()
            end)
        "#,
    )
    .await?;
    let (library, releases) = {
        let mut db = STATE.db.write().await;
        let library = lyra_db::test_db::insert_library(&mut db, "Declared", "/declared")?;
        let mut releases = Vec::new();
        for title in ["One", "Two"] {
            let release = lyra_db::fixtures::insert_release(&mut db, title)?;
            lyra_db::fixtures::connect(&mut db, library, release)?;
            releases.push(
                lyra_db::releases::get_by_id(&db, release)?
                    .context("release")?
                    .id,
            );
        }
        (library, releases)
    };
    let principal = admin_principal().await?;

    // The calling VM's registries are then the only ones holding the provider's declarations.
    STATE.generation().runtime.replace(None);
    for release in releases {
        let nested = run_chunk(
            &runtime,
            "declaring",
            r#"
                local refreshed = require("@lyra/metadata").refresh((...))
                assert(refreshed and refreshed.provider_ids[1] == "declaring", "not refreshed")
            "#,
            release,
            principal.clone(),
        );
        tokio::time::timeout(Duration::from_secs(10), nested).await???;
    }

    let db = STATE.db.read().await;
    assert_eq!(lyra_db::releases::get(&db, library)?.len(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_nested_refresh_waits_for_the_providers_running_call() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    let runtime = start_plugin(
        &plugins_dir,
        "locked",
        &["lyra.metadata", "lyra.metadata.refresh"],
        r#"
            local metadata = require("@lyra/metadata")
            metadata.Provider.new("locked"):refresh(metadata.EntityType.Release, function()
                return nil
            end)
        "#,
    )
    .await?;
    let release = {
        let mut db = STATE.db.write().await;
        let release = lyra_db::fixtures::insert_release(&mut db, "Release")?;
        lyra_db::releases::get_by_id(&db, release)?
            .context("release")?
            .id
    };

    let entered = Arc::new(tokio::sync::Notify::new());
    let release_pass = Arc::new(tokio::sync::Semaphore::new(0));
    let pass = tokio::spawn({
        let (entered, release_pass) = (entered.clone(), release_pass.clone());
        async move {
            with_provider_call(
                "locked",
                ProviderCallStage::MetadataRefresh,
                |_| Some(ProviderLookup::Call(())),
                |(), _| async {
                    entered.notify_one();
                    release_pass.acquire().await.expect("gate").forget();
                    Ok(())
                },
            )
            .await
        }
    });
    entered.notified().await;

    let nested = run_chunk(
        &runtime,
        "locked",
        r#"
            local refreshed = require("@lyra/metadata").refresh((...))
            assert(refreshed and refreshed.provider_ids[1] == "locked", "the provider did not run")
        "#,
        release,
        admin_principal().await?,
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !nested.is_finished(),
        "a nested refresh waits for the provider's call lock"
    );

    release_pass.add_permits(1);
    pass.await?.map_err(|error| anyhow::anyhow!("{error}"))?;
    tokio::time::timeout(Duration::from_secs(10), nested).await???;
    Ok(())
}
