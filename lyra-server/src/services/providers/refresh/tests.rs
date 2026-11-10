// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use lyra_db::{
    fixtures::{
        connect,
        insert_artist,
        insert_release,
    },
    test_db::insert_library,
};
use tokio::sync::Semaphore;

use super::{
    EntityType,
    LibraryRefreshOptions,
    STATE,
};
use crate::services::covers::CoverSyncOptions;
use crate::services::libraries::{
    SyncRunProgress,
    SyncStageKey,
    SyncTotalState,
};
use crate::services::plugin_dispatch::PluginCallbackHandle;
use crate::services::plugin_scope::PluginId;
use crate::services::providers::provider_registry;
use crate::services::providers::{
    ProviderCallError,
    teardown_plugin_providers,
};
use crate::services::testing::{
    FakePluginDispatch,
    FakeResponse,
    init_default_test_state,
    runtime_test_lock,
};

#[tokio::test(start_paused = true)]
async fn a_late_handler_ends_with_the_runtimes_answer() {
    let _guard = runtime_test_lock().await;
    init_default_test_state().expect("test state");
    let dispatch = FakePluginDispatch::install();
    let handler = dispatch.add(FakeResponse::UntilDeadline);
    register_release_refresher("late-provider", handler)
        .await
        .expect("register");

    let error = super::dispatch::dispatch_refresh(
        &crate::services::plugin_dispatch::Published::interactive(),
        "late-provider",
        EntityType::Release,
        serde_json::json!({}),
    )
    .await
    .expect_err("a handler past its deadline fails");

    assert!(
        format!("{error:#}").contains("plugin handler deadline expired"),
        "the runtime, having cancelled the handler, answers before the server stops waiting: {error:#}"
    );
}

#[tokio::test]
async fn a_refresh_without_a_handler_is_not_registered() {
    let _guard = runtime_test_lock().await;
    init_default_test_state().expect("test state");

    let result = super::dispatch::dispatch_refresh(
        &crate::services::plugin_dispatch::Published::interactive(),
        "nobody",
        EntityType::Release,
        serde_json::json!({}),
    )
    .await;

    assert!(matches!(result, Err(ProviderCallError::NotRegistered)));
}

/// Registers `provider_id` with `handler` as its release refresh handler.
async fn register_release_refresher(
    provider_id: &str,
    handler: crate::services::plugin_dispatch::PluginCallbackHandle,
) -> anyhow::Result<()> {
    lyra_db::providers::register(&mut *STATE.db.write().await, provider_id)?;
    let registry = provider_registry();
    let mut registry = registry.write().await;
    registry.register(
        crate::services::plugin_scope::PluginId::new(format!("{provider_id}_plugin").as_str())?,
        provider_id.to_string(),
        None,
    )?;
    registry.set_refresh_callback(provider_id, EntityType::Release, handler);
    Ok(())
}

fn called_ids(dispatch: &FakePluginDispatch, handler: PluginCallbackHandle) -> Vec<String> {
    dispatch
        .calls(handler)
        .iter()
        .map(|context| context["id"].as_str().expect("release id").to_string())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_release_context_that_cant_be_built_settles_its_units_and_persists_them()
-> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let dispatch = FakePluginDispatch::install();
    let handler = dispatch.add(FakeResponse::Value(serde_json::Value::Null));
    register_release_refresher("unbuildable", handler).await?;
    let (library, release) = {
        let mut db = STATE.db.write().await;
        let library = insert_library(&mut db, "Unbuildable", "/unbuildable")?;
        let release = insert_release(&mut db, "Unbuildable")?;
        connect(&mut db, library, release)?;
        let track = lyra_db::fixtures::insert_track(&mut db, "Malformed")?;
        connect(&mut db, release, track)?;
        db.exec_mut(
            agdb::QueryBuilder::remove()
                .values("track_title")
                .ids(track)
                .query(),
        )?;
        (
            lyra_db::libraries::get_by_id(&db, library)?.expect("library"),
            release,
        )
    };
    let progress = crate::services::libraries::begin_test_run(&library).await;
    progress
        .add_stage_total(SyncStageKey::ProviderRefresh, 1, SyncTotalState::Final)
        .await;
    let plan = super::ScanRefreshPlan::current().await?;

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        super::library::refresh_release_metadata_for_scan(
            library.db_id.expect("db id"),
            release,
            &plan,
            progress.clone(),
            None,
            None,
        ),
    )
    .await
    .expect("settling must not wait on the read guard of the failed build");

    assert!(result.is_err());
    assert!(dispatch.calls(handler).is_empty());
    let run = crate::services::libraries::get_sync_run(progress.run_id())
        .await?
        .expect("run");
    crate::services::libraries::end_test_run(&progress).await;
    assert_eq!(run.progress.skipped_units, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_release_gone_between_providers_settles_the_scans_remaining_units() -> anyhow::Result<()>
{
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let dispatch = FakePluginDispatch::install();
    let gate = Arc::new(Semaphore::new(0));
    let first = dispatch.add(FakeResponse::Held(
        gate.clone(),
        Box::new(FakeResponse::Value(serde_json::Value::Null)),
    ));
    let second = dispatch.add(FakeResponse::Value(serde_json::Value::Null));
    let third = dispatch.add(FakeResponse::Value(serde_json::Value::Null));
    register_release_refresher("first", first).await?;
    register_release_refresher("second", second).await?;
    register_release_refresher("third", third).await?;
    let (library, release, public_id) = {
        let mut db = STATE.db.write().await;
        let library = insert_library(&mut db, "Vanishing", "/vanishing")?;
        let release = insert_release(&mut db, "Vanishing")?;
        connect(&mut db, library, release)?;
        (
            lyra_db::libraries::get_by_id(&db, library)?.expect("library"),
            release,
            lyra_db::lookup::find_id_by_db_id(&*db, release)?,
        )
    };
    let progress = crate::services::libraries::begin_test_run(&library).await;
    progress
        .add_stage_total(SyncStageKey::ProviderRefresh, 3, SyncTotalState::Final)
        .await;

    let plan = super::ScanRefreshPlan::current().await?;
    let scan = {
        let library = library.db_id.expect("db id");
        let progress = progress.clone();
        tokio::spawn(async move {
            super::library::refresh_release_metadata_for_scan(
                library, release, &plan, progress, public_id, None,
            )
            .await
        })
    };
    dispatch.wait_for_calls(first, 1).await;
    STATE
        .db
        .write()
        .await
        .exec_mut(agdb::QueryBuilder::remove().ids(release).query())?;
    gate.add_permits(1);

    scan.await?.map_err(|err| err.into_anyhow())?;
    let run = crate::services::libraries::get_sync_run(progress.run_id())
        .await?
        .expect("run");
    crate::services::libraries::end_test_run(&progress).await;

    assert!(dispatch.calls(second).is_empty() && dispatch.calls(third).is_empty());
    assert_eq!(
        (
            run.progress.completed_units,
            run.progress.skipped_units,
            run.progress.total_units
        ),
        (1, 2, 3),
        "the units of the providers that never ran are skipped"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_release_merged_away_between_providers_is_passed_over() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let dispatch = FakePluginDispatch::install();
    let gate = Arc::new(Semaphore::new(0));
    let first = dispatch.add(FakeResponse::Held(
        gate.clone(),
        Box::new(FakeResponse::Value(serde_json::Value::Null)),
    ));
    let second = dispatch.add(FakeResponse::Value(serde_json::Value::Null));
    register_release_refresher("first", first).await?;
    register_release_refresher("second", second).await?;
    let library = {
        let mut db = STATE.db.write().await;
        let library = insert_library(&mut db, "Library", "/library")?;
        for title in ["One", "Two"] {
            let release = insert_release(&mut db, title)?;
            connect(&mut db, library, release)?;
        }
        library
    };

    let refresh = tokio::spawn(async move {
        let options = LibraryRefreshOptions {
            covers: CoverSyncOptions::SKIP,
            apply_sync_filters: false,
            provider_id: None,
        };
        super::library::refresh_library_metadata(
            library,
            &options,
            SyncRunProgress::unreported_for_test(),
        )
        .await
    });
    // The first provider refreshes one release; it is merged away while the provider
    // refreshes the other.
    dispatch.wait_for_calls(first, 1).await;
    gate.add_permits(1);
    dispatch.wait_for_calls(first, 2).await;
    let first_calls = called_ids(&dispatch, first);
    {
        let mut db = STATE.db.write().await;
        let gone =
            lyra_db::lookup::find_node_id_by_id(&*db, &first_calls[0])?.expect("release exists");
        db.exec_mut(agdb::QueryBuilder::remove().ids(gone).query())?;
    }
    gate.add_permits(1);

    let refreshed = refresh.await?.map_err(|err| err.into_anyhow())?;
    assert_eq!(refreshed, 2);
    assert_eq!(
        called_ids(&dispatch, second),
        [first_calls[1].clone()],
        "the second provider passes over the merged release"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scan_refresh_of_a_release_whose_id_went_to_another_node_calls_no_provider()
-> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let dispatch = FakePluginDispatch::install();
    let handler = dispatch.add(FakeResponse::Value(serde_json::Value::Null));
    register_release_refresher("reused-id", handler).await?;
    let (library, release) = {
        let mut db = STATE.db.write().await;
        let library = insert_library(&mut db, "Reused", "/reused-id")?;
        let release = insert_release(&mut db, "Newcomer")?;
        connect(&mut db, library, release)?;
        (library, release)
    };

    let refreshed = super::library::refresh_release_metadata_for_scan(
        library,
        release,
        &super::ScanRefreshPlan::current().await?,
        SyncRunProgress::unreported_for_test(),
        Some("the-merged-release".to_string()),
        None,
    )
    .await
    .map_err(|err| err.into_anyhow())?;

    assert!(refreshed.is_empty());
    assert!(
        dispatch.calls(handler).is_empty(),
        "the newcomer isn't refreshed under the merged release's name"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refresh_syncs_covers_even_when_no_provider_refreshed_a_release() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let library = {
        let mut db = STATE.db.write().await;
        let library_id = insert_library(&mut db, "Covers", "/covers-library")?;
        let release = insert_release(&mut db, "Release")?;
        connect(&mut db, library_id, release)?;
        lyra_db::libraries::get_by_id(&db, library_id)?.expect("library")
    };
    let progress = crate::services::libraries::begin_test_run(&library).await;
    let options = LibraryRefreshOptions {
        covers: CoverSyncOptions::default(),
        apply_sync_filters: false,
        provider_id: None,
    };
    let refreshed = super::library::refresh_library_metadata(
        library.db_id.expect("db id"),
        &options,
        progress.clone(),
    )
    .await
    .map_err(|err| err.into_anyhow())?;
    let run = crate::services::libraries::get_sync_run(progress.run_id())
        .await?
        .expect("run");
    crate::services::libraries::end_test_run(&progress).await;

    assert_eq!(refreshed, 0);
    assert_eq!(
        (run.progress.completed_units, run.progress.total_units),
        (1, 1),
        "the cover step ran"
    );
    Ok(())
}

fn listed(
    db: &agdb::DbAny,
    artist_id: agdb::DbId,
) -> anyhow::Result<super::artist_covers::ListedArtist> {
    let artist = lyra_db::artists::get_by_id(db, artist_id)?.expect("artist");
    Ok(super::artist_covers::ListedArtist::list(vec![artist])
        .pop()
        .expect("listed"))
}

async fn empty_test_library(name: &str) -> anyhow::Result<lyra_db::Library> {
    let mut db = STATE.db.write().await;
    let library_id = insert_library(&mut db, name, &format!("/{name}"))?;
    Ok(lyra_db::libraries::get_by_id(&db, library_id)?.expect("library"))
}

#[tokio::test(flavor = "multi_thread")]
async fn the_artist_pass_skips_gone_or_replaced_artists_and_runs_on_past_a_failure()
-> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let dispatch = FakePluginDispatch::install();
    let handler = dispatch
        .register_artist_cover_handler(
            "artist_covers",
            FakeResponse::Value(serde_json::Value::Null),
        )
        .await?;
    let artists = {
        let mut db = STATE.db.write().await;
        let first = insert_artist(&mut db, "First")?;
        let unreadable = db
            .exec_mut(
                agdb::QueryBuilder::insert()
                    .nodes()
                    .values([[("db_element_id", "Artist").into()]])
                    .query(),
            )?
            .ids()[0];
        let merged = insert_artist(&mut db, "Merged")?;
        let merged = listed(&db, merged)?;
        db.exec_mut(agdb::QueryBuilder::remove().ids(merged.db_id).query())?;
        let newcomer = insert_artist(&mut db, "Newcomer")?;
        assert_eq!(newcomer, merged.db_id, "agdb reuses the merged artist's id");
        let gone = insert_artist(&mut db, "Gone")?;
        let gone = listed(&db, gone)?;
        let last = insert_artist(&mut db, "Last")?;
        // Removed last, so no insert reuses its id.
        db.exec_mut(agdb::QueryBuilder::remove().ids(gone.db_id).query())?;
        vec![
            listed(&db, first)?,
            super::artist_covers::ListedArtist {
                db_id: unreadable,
                id: "unreadable".to_string(),
                artist_name: "Unreadable".to_string(),
            },
            merged,
            gone,
            listed(&db, last)?,
        ]
    };
    let library = empty_test_library("artist-pass").await?;
    let progress = crate::services::libraries::begin_test_run(&library).await;
    let covers_root = tempfile::tempdir()?;

    let synced = super::artist_covers::sync_artist_covers(
        &artists,
        super::cover_paths(&None, covers_root.path()),
        CoverSyncOptions::default(),
        None,
        &progress,
    )
    .await?;
    let run = crate::services::libraries::get_sync_run(progress.run_id())
        .await?
        .expect("run");
    crate::services::libraries::end_test_run(&progress).await;

    assert_eq!(synced, 0);
    assert_eq!(dispatch.artist_names(handler), ["First", "Last"]);
    let units = &run.progress;
    assert_eq!(
        (
            units.completed_units,
            units.failed_units,
            units.skipped_units,
            units.total_units
        ),
        (2, 1, 2, 5),
        "one unit per artist, every one settled"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn the_pass_refreshes_the_artists_it_fetches_a_cover_for_with_their_cover_providers()
-> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let dispatch = FakePluginDispatch::install();
    dispatch
        .register_artist_cover_handler(
            "refreshing_covers",
            FakeResponse::Value(serde_json::Value::Null),
        )
        .await?;
    let refresher = dispatch.add(FakeResponse::Value(serde_json::Value::Null));
    let coverless = dispatch.add(FakeResponse::Value(serde_json::Value::Null));
    lyra_db::providers::register(&mut *STATE.db.write().await, "coverless")?;
    {
        let registry = provider_registry();
        let mut registry = registry.write().await;
        registry.set_refresh_callback("refreshing_covers", EntityType::Artist, refresher);
        registry.register(
            crate::services::plugin_scope::PluginId::new("coverless_plugin")?,
            "coverless".to_string(),
            None,
        )?;
        registry.set_refresh_callback("coverless", EntityType::Artist, coverless);
    }
    let artists = {
        let mut db = STATE.db.write().await;
        let covered = insert_artist(&mut db, "Covered")?;
        db.transaction_mut(|t| {
            lyra_db::covers::upsert(
                t,
                covered,
                lyra_db::covers::CoverSource::Manual,
                lyra_db::Cover {
                    db_id: None,
                    id: lyra_db::nanoid(),
                    path: crate::services::testing::existing_cover_path(),
                    mime_type: "image/png".to_string(),
                    hash: "covered".to_string(),
                    blurhash: None,
                    provider_id: None,
                },
            )
        })?;
        let bare = insert_artist(&mut db, "Bare")?;
        vec![listed(&db, covered)?, listed(&db, bare)?]
    };
    let covers_root = tempfile::tempdir()?;

    super::artist_covers::sync_artist_covers(
        &artists,
        super::cover_paths(&None, covers_root.path()),
        CoverSyncOptions::default(),
        None,
        &SyncRunProgress::unreported_for_test(),
    )
    .await?;

    assert_eq!(
        dispatch.artist_names(refresher),
        ["Bare"],
        "an artist keeping its cover is not refreshed"
    );
    assert!(
        dispatch.calls(coverless).is_empty(),
        "a provider without artist covers is not refreshed for them"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_cancelled_during_an_artists_refresh_fetches_no_cover() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let dispatch = FakePluginDispatch::install();
    let cover = dispatch
        .register_artist_cover_handler(
            "refresh_then_cover",
            FakeResponse::Value(serde_json::Value::Null),
        )
        .await?;
    let gate = Arc::new(Semaphore::new(0));
    let refresher = dispatch.add(FakeResponse::Held(
        gate.clone(),
        Box::new(FakeResponse::Value(serde_json::Value::Null)),
    ));
    provider_registry().write().await.set_refresh_callback(
        "refresh_then_cover",
        EntityType::Artist,
        refresher,
    );
    let artist = {
        let mut db = STATE.db.write().await;
        let artist = insert_artist(&mut db, "Artist")?;
        listed(&db, artist)?
    };
    let library = empty_test_library("cancelled-refresh-pass").await?;
    let progress = crate::services::libraries::begin_test_run(&library).await;
    let covers_root = tempfile::tempdir()?;

    let pass = tokio::spawn({
        let progress = progress.clone();
        let covers_root = covers_root.path().to_path_buf();
        async move {
            super::artist_covers::sync_artist_covers(
                &[artist],
                super::cover_paths(&None, &covers_root),
                CoverSyncOptions::default(),
                None,
                &progress,
            )
            .await
        }
    });
    dispatch.wait_for_calls(refresher, 1).await;
    crate::services::libraries::cancel_sync_run(progress.run_id()).await?;
    gate.add_permits(1);
    let result = pass.await?;
    crate::services::libraries::end_test_run(&progress).await;

    assert!(
        matches!(
            result,
            Err(crate::services::libraries::SyncRunControlError::Cancelled)
        ),
        "{result:?}"
    );
    assert!(dispatch.calls(cover).is_empty(), "no cover is looked up");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn the_artist_pass_stops_when_the_run_is_cancelled() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let dispatch = FakePluginDispatch::install();
    let handler = dispatch
        .register_artist_cover_handler(
            "cancelled_covers",
            FakeResponse::Value(serde_json::Value::Null),
        )
        .await?;
    let artist = {
        let mut db = STATE.db.write().await;
        let artist = insert_artist(&mut db, "Artist")?;
        listed(&db, artist)?
    };
    let library = empty_test_library("cancelled-artist-pass").await?;
    let progress = crate::services::libraries::begin_test_run(&library).await;
    crate::services::libraries::cancel_sync_run(progress.run_id()).await?;
    let covers_root = tempfile::tempdir()?;

    let result = super::artist_covers::sync_artist_covers(
        &[artist],
        super::cover_paths(&None, covers_root.path()),
        CoverSyncOptions::default(),
        None,
        &progress,
    )
    .await;
    crate::services::libraries::end_test_run(&progress).await;

    assert!(
        matches!(
            result,
            Err(crate::services::libraries::SyncRunControlError::Cancelled)
        ),
        "{result:?}"
    );
    assert!(dispatch.calls(handler).is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_cancelled_during_the_last_artist_ends_cancelled() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let dispatch = FakePluginDispatch::install();
    let gate = Arc::new(Semaphore::new(0));
    let handler = dispatch
        .register_artist_cover_handler(
            "held_covers",
            FakeResponse::Held(
                gate.clone(),
                Box::new(FakeResponse::Value(serde_json::Value::Null)),
            ),
        )
        .await?;
    let artist = {
        let mut db = STATE.db.write().await;
        let artist = insert_artist(&mut db, "Artist")?;
        listed(&db, artist)?
    };
    let library = empty_test_library("held-artist-pass").await?;
    let progress = crate::services::libraries::begin_test_run(&library).await;
    let covers_root = tempfile::tempdir()?;

    let pass = tokio::spawn({
        let progress = progress.clone();
        let covers_root = covers_root.path().to_path_buf();
        async move {
            super::artist_covers::sync_artist_covers(
                &[artist],
                super::cover_paths(&None, &covers_root),
                CoverSyncOptions::default(),
                None,
                &progress,
            )
            .await
        }
    });
    dispatch.wait_for_calls(handler, 1).await;
    crate::services::libraries::cancel_sync_run(progress.run_id()).await?;
    gate.add_permits(1);
    let result = pass.await?;
    crate::services::libraries::end_test_run(&progress).await;

    assert!(
        matches!(
            result,
            Err(crate::services::libraries::SyncRunControlError::Cancelled)
        ),
        "{result:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_library_refresh_passes_its_track_only_artists() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let dispatch = FakePluginDispatch::install();
    let handler = dispatch
        .register_artist_cover_handler(
            "library_artist_covers",
            FakeResponse::Value(serde_json::Value::Null),
        )
        .await?;
    let library = {
        let mut db = STATE.db.write().await;
        let library_id = insert_library(&mut db, "Artists", "/artists-library")?;
        let release = insert_release(&mut db, "Release")?;
        connect(&mut db, library_id, release)?;
        let track = lyra_db::fixtures::insert_track(&mut db, "Track")?;
        connect(&mut db, release, track)?;
        let main = insert_artist(&mut db, "Main")?;
        let featured = insert_artist(&mut db, "Featured")?;
        lyra_db::test_db::connect_artist(&mut db, release, main)?;
        lyra_db::test_db::connect_artist(&mut db, track, featured)?;
        lyra_db::libraries::get_by_id(&db, library_id)?.expect("library")
    };
    let progress = crate::services::libraries::begin_test_run(&library).await;
    let options = LibraryRefreshOptions {
        covers: CoverSyncOptions::default(),
        apply_sync_filters: false,
        provider_id: None,
    };

    super::library::refresh_library_metadata(
        library.db_id.expect("db id"),
        &options,
        progress.clone(),
    )
    .await
    .map_err(|err| err.into_anyhow())?;
    let run = crate::services::libraries::get_sync_run(progress.run_id())
        .await?
        .expect("run");
    crate::services::libraries::end_test_run(&progress).await;

    assert_eq!(dispatch.artist_names(handler), ["Main", "Featured"]);
    assert_eq!(
        (run.progress.completed_units, run.progress.total_units),
        (3, 3),
        "the release covers' unit and one per artist"
    );
    Ok(())
}

#[tokio::test]
async fn a_cancelled_cover_pass_asks_no_provider_for_release_covers() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let dispatch = FakePluginDispatch::install();
    let handler = dispatch
        .register_artist_cover_handler("covers", FakeResponse::Value(serde_json::Value::Null))
        .await?;
    provider_registry().write().await.set_cover_handler(
        "covers",
        EntityType::Release,
        crate::services::providers::ProviderCoverSpec {
            timeout: std::time::Duration::from_secs(5),
            require: Default::default(),
            handler,
        },
    );
    let library = {
        let mut db = STATE.db.write().await;
        let library = insert_library(&mut db, "Covers", "/cancelled-covers")?;
        let release = insert_release(&mut db, "Release")?;
        connect(&mut db, library, release)?;
        library
    };
    let covers_root = tempfile::tempdir()?;
    let sync_covers = async |cancel: &tokio_util::sync::CancellationToken| {
        crate::services::covers::sync_and_persist_release_covers_for_library(
            &STATE.db.get(),
            super::cover_paths(&None, covers_root.path()),
            library,
            CoverSyncOptions::default(),
            None,
            cancel,
        )
        .await
    };

    let cancelled = tokio_util::sync::CancellationToken::new();
    cancelled.cancel();
    sync_covers(&cancelled).await?;
    assert!(dispatch.calls(handler).is_empty());

    sync_covers(&tokio_util::sync::CancellationToken::new()).await?;
    assert_eq!(dispatch.calls(handler).len(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_cancelled_during_its_release_covers_leaves_the_cover_unit_unsettled()
-> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let dispatch = FakePluginDispatch::install();
    let gate = Arc::new(Semaphore::new(0));
    let handler = dispatch
        .register_artist_cover_handler(
            "covers",
            FakeResponse::Held(
                gate.clone(),
                Box::new(FakeResponse::Value(serde_json::Value::Null)),
            ),
        )
        .await?;
    provider_registry().write().await.set_cover_handler(
        "covers",
        EntityType::Release,
        crate::services::providers::ProviderCoverSpec {
            timeout: std::time::Duration::from_secs(5),
            require: Default::default(),
            handler,
        },
    );
    let library = {
        let mut db = STATE.db.write().await;
        let library_id = insert_library(&mut db, "Covers", "/interrupted-covers")?;
        let release = insert_release(&mut db, "Release")?;
        connect(&mut db, library_id, release)?;
        lyra_db::libraries::get_by_id(&db, library_id)?.expect("library")
    };
    let progress = crate::services::libraries::begin_test_run(&library).await;
    let refresh = tokio::spawn({
        let progress = progress.clone();
        let library_id = library.db_id.expect("db id");
        async move {
            let options = LibraryRefreshOptions {
                covers: CoverSyncOptions::default(),
                apply_sync_filters: false,
                provider_id: None,
            };
            super::library::refresh_library_metadata(library_id, &options, progress).await
        }
    });
    dispatch.wait_for_calls(handler, 1).await;
    crate::services::libraries::cancel_sync_run(progress.run_id()).await?;
    gate.add_permits(1);

    let error = refresh
        .await?
        .expect_err("the cancelled refresh ends cancelled");
    let run = crate::services::libraries::get_sync_run(progress.run_id())
        .await?
        .expect("run");
    crate::services::libraries::end_test_run(&progress).await;

    assert!(error.is_cancellation());
    assert_eq!(run.progress.completed_units, 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sync_filter_stopped_by_a_gone_target_is_skipped_and_counts_as_touched()
-> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let dispatch = FakePluginDispatch::install();
    let handler = dispatch.add(FakeResponse::Value(serde_json::Value::Null));
    let filter = dispatch.add(FakeResponse::TargetGone("gone".to_string()));
    register_release_refresher("filtered", handler).await?;
    provider_registry().write().await.set_sync_filter_callback(
        "filtered",
        EntityType::Release,
        filter,
    );
    let library = {
        let mut db = STATE.db.write().await;
        let library = insert_library(&mut db, "Filtered", "/filtered-gone")?;
        let release = insert_release(&mut db, "Release")?;
        connect(&mut db, library, release)?;
        lyra_db::libraries::get_by_id(&db, library)?.expect("library")
    };
    let progress = crate::services::libraries::begin_test_run(&library).await;
    let options = LibraryRefreshOptions {
        covers: CoverSyncOptions::SKIP,
        apply_sync_filters: true,
        provider_id: None,
    };

    let refreshed = super::library::refresh_library_metadata(
        library.db_id.expect("db id"),
        &options,
        progress.clone(),
    )
    .await
    .map_err(|err| err.into_anyhow())?;
    let run = crate::services::libraries::get_sync_run(progress.run_id())
        .await?
        .expect("run");
    crate::services::libraries::end_test_run(&progress).await;

    assert_eq!(
        refreshed, 1,
        "what the filter saved first still gets merged"
    );
    assert_eq!(run.progress.skipped_units, 1);
    assert!(dispatch.calls(handler).is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn an_entity_refresh_passes_over_a_provider_stopped_by_a_gone_target() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let dispatch = FakePluginDispatch::install();
    let gate = Arc::new(Semaphore::new(0));
    let first = dispatch.add(FakeResponse::Held(
        gate.clone(),
        Box::new(FakeResponse::TargetGone("gone".to_string())),
    ));
    let second = dispatch.add(FakeResponse::Value(serde_json::Value::Null));
    register_release_refresher("first", first).await?;
    register_release_refresher("second", second).await?;
    let (admin, release, release_id) = {
        let mut db = STATE.db.write().await;
        lyra_db::roles::ensure_builtin_roles(&mut db)?;
        let user = lyra_db::test_db::insert_user(&mut db, "entity-refresh-admin")?;
        lyra_db::roles::ensure_user_has_role(&mut db, user, lyra_db::roles::BUILTIN_ADMIN_ROLE)?;
        let library = insert_library(&mut db, "Entity", "/entity-gone")?;
        let release = insert_release(&mut db, "Release")?;
        connect(&mut db, library, release)?;
        let id = lyra_db::lookup::find_id_by_db_id(&*db, release)?.expect("public id");
        drop(db);
        (
            crate::services::testing::user_principal(user).await?,
            release,
            id,
        )
    };

    let refresh = tokio::spawn(async move {
        crate::services::providers::refresh_entity(
            &crate::services::plugin_dispatch::Published::new(
                crate::services::plugin_dispatch::TransitionWait::interactive(),
            ),
            &admin,
            &release_id,
            Default::default(),
        )
        .await
    });
    dispatch.wait_for_calls(first, 1).await;
    STATE
        .db
        .write()
        .await
        .exec_mut(agdb::QueryBuilder::remove().ids(release).query())?;
    gate.add_permits(1);

    let result = refresh.await?.expect("the refresh ends without an error");
    assert_eq!(result.providers_called, ["first", "second"]);
    assert_eq!(dispatch.calls(second).len(), 1);
    Ok(())
}

const RELOADED_VM_ID: u64 = 2;

fn plugin_of(provider_id: &str) -> PluginId {
    PluginId::new(format!("{provider_id}_plugin").as_str()).expect("plugin id")
}

/// What a plugin reload does to a provider: its handlers go with its plugin, and a new runtime is
/// published before the plugin registers them again.
async fn reload_provider(provider_id: &str) -> Arc<FakePluginDispatch> {
    teardown_plugin_providers(&plugin_of(provider_id)).await;
    FakePluginDispatch::install_as(RELOADED_VM_ID)
}

fn answering_null() -> FakeResponse {
    FakeResponse::Value(serde_json::Value::Null)
}

fn held(gate: &Arc<Semaphore>) -> FakeResponse {
    FakeResponse::Held(gate.clone(), Box::new(answering_null()))
}

/// Registers `provider_id` with an artist cover handler and `refresher` as its artist refresh
/// handler, returning the cover handler.
async fn register_artist_provider(
    dispatch: &FakePluginDispatch,
    provider_id: &str,
    refresher: PluginCallbackHandle,
) -> anyhow::Result<PluginCallbackHandle> {
    let cover = dispatch
        .register_artist_cover_handler(provider_id, answering_null())
        .await?;
    provider_registry().write().await.set_refresh_callback(
        provider_id,
        EntityType::Artist,
        refresher,
    );
    Ok(cover)
}

async fn listed_artists(names: &[&str]) -> anyhow::Result<Vec<super::artist_covers::ListedArtist>> {
    let mut db = STATE.db.write().await;
    let mut artists = Vec::new();
    for name in names {
        let artist = insert_artist(&mut db, name)?;
        artists.push(listed(&db, artist)?);
    }
    Ok(artists)
}

async fn library_with_releases(name: &str, titles: &[&str]) -> anyhow::Result<lyra_db::Library> {
    let mut db = STATE.db.write().await;
    let library_id = insert_library(&mut db, name, &format!("/{name}"))?;
    for title in titles {
        let release = insert_release(&mut db, title)?;
        connect(&mut db, library_id, release)?;
    }
    Ok(lyra_db::libraries::get_by_id(&db, library_id)?.expect("library"))
}

fn spawn_artist_pass(
    artists: Vec<super::artist_covers::ListedArtist>,
    covers_root: &std::path::Path,
    progress: &SyncRunProgress,
) -> tokio::task::JoinHandle<Result<usize, crate::services::libraries::SyncRunControlError>> {
    let progress = progress.clone();
    let covers_root = covers_root.to_path_buf();
    tokio::spawn(async move {
        super::artist_covers::sync_artist_covers(
            &artists,
            super::cover_paths(&None, &covers_root),
            CoverSyncOptions::default(),
            None,
            &progress,
        )
        .await
    })
}

fn spawn_release_refresh(
    library: &lyra_db::Library,
    apply_sync_filters: bool,
    progress: &SyncRunProgress,
) -> tokio::task::JoinHandle<Result<usize, crate::services::providers::ProviderServiceError>> {
    let progress = progress.clone();
    let library_id = library.db_id.expect("db id");
    tokio::spawn(async move {
        let options = LibraryRefreshOptions {
            covers: CoverSyncOptions::SKIP,
            apply_sync_filters,
            provider_id: None,
        };
        super::library::refresh_library_metadata(library_id, &options, progress).await
    })
}

/// A run's settled units as `(completed, skipped, failed, total)`.
async fn settled_units(progress: &SyncRunProgress) -> anyhow::Result<(u64, u64, u64, u64)> {
    let run = crate::services::libraries::get_sync_run(progress.run_id())
        .await?
        .expect("run");
    crate::services::libraries::end_test_run(progress).await;
    let units = run.progress;
    Ok((
        units.completed_units,
        units.skipped_units,
        units.failed_units,
        units.total_units,
    ))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reload_during_the_artist_pass_reaches_the_new_handlers() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let old = FakePluginDispatch::install();
    let gate = Arc::new(Semaphore::new(0));
    let old_refresher = old.add(held(&gate));
    let old_cover = register_artist_provider(&old, "reloaded", old_refresher).await?;
    let artists = listed_artists(&["First", "Second"]).await?;
    let library = empty_test_library("reloaded-artist-pass").await?;
    let progress = crate::services::libraries::begin_test_run(&library).await;
    let covers_root = tempfile::tempdir()?;

    let pass = spawn_artist_pass(artists, covers_root.path(), &progress);
    old.wait_for_calls(old_refresher, 1).await;
    let new = reload_provider("reloaded").await;
    let new_refresher = new.add(answering_null());
    let new_cover = register_artist_provider(&new, "reloaded", new_refresher).await?;
    gate.add_permits(1);
    pass.await??;

    assert_eq!(old.artist_names(old_refresher), ["First"]);
    assert_eq!(new.artist_names(new_refresher), ["Second"]);
    assert!(old.calls(old_cover).is_empty());
    assert_eq!(new.artist_names(new_cover), ["First", "Second"]);
    assert_eq!(settled_units(&progress).await?, (2, 0, 0, 2));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reload_during_the_release_refresh_reaches_the_new_handlers() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let old = FakePluginDispatch::install();
    let gate = Arc::new(Semaphore::new(0));
    let old_refresher = old.add(held(&gate));
    let old_filter = old.add(FakeResponse::Value(serde_json::json!(true)));
    register_release_refresher("reloaded", old_refresher).await?;
    provider_registry().write().await.set_sync_filter_callback(
        "reloaded",
        EntityType::Release,
        old_filter,
    );
    let library = library_with_releases("reloaded-refresh", &["One", "Two"]).await?;
    let progress = crate::services::libraries::begin_test_run(&library).await;

    let refresh = spawn_release_refresh(&library, true, &progress);
    old.wait_for_calls(old_refresher, 1).await;
    let new = reload_provider("reloaded").await;
    let new_refresher = new.add(answering_null());
    let new_filter = new.add(FakeResponse::Value(serde_json::json!(true)));
    register_release_refresher("reloaded", new_refresher).await?;
    provider_registry().write().await.set_sync_filter_callback(
        "reloaded",
        EntityType::Release,
        new_filter,
    );
    gate.add_permits(1);

    let refreshed = refresh.await?.map_err(|err| err.into_anyhow())?;
    assert_eq!(refreshed, 2);
    assert_eq!(old.calls(old_filter).len(), 1);
    assert_eq!(old.calls(old_refresher).len(), 1);
    assert_eq!(new.calls(new_filter).len(), 1);
    assert_eq!(new.calls(new_refresher).len(), 1);
    assert_eq!(settled_units(&progress).await?, (2, 0, 0, 2));
    Ok(())
}

#[derive(Clone, Default)]
struct CapturedLogs(Arc<std::sync::Mutex<Vec<u8>>>);

impl CapturedLogs {
    fn count(&self, needle: &str) -> usize {
        String::from_utf8_lossy(&self.0.lock().expect("logs"))
            .matches(needle)
            .count()
    }
}

impl std::io::Write for CapturedLogs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("logs").extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// The current-thread runtime keeps the spawned refresh under the test's log subscriber.
#[tokio::test]
async fn a_provider_gone_for_the_rest_of_a_refresh_skips_its_items_with_one_log_line()
-> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let logs = CapturedLogs::default();
    let _logs = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_ansi(false)
            .finish(),
    );
    let dispatch = FakePluginDispatch::install();
    let gate = Arc::new(Semaphore::new(0));
    let handler = dispatch.add(held(&gate));
    register_release_refresher("vanishing", handler).await?;
    let library = library_with_releases("vanishing-refresh", &["One", "Two", "Three"]).await?;
    let progress = crate::services::libraries::begin_test_run(&library).await;

    let refresh = spawn_release_refresh(&library, false, &progress);
    dispatch.wait_for_calls(handler, 1).await;
    teardown_plugin_providers(&plugin_of("vanishing")).await;
    gate.add_permits(1);
    refresh.await?.map_err(|err| err.into_anyhow())?;

    assert_eq!(dispatch.calls(handler).len(), 1);
    assert_eq!(settled_units(&progress).await?, (1, 2, 0, 3));
    assert_eq!(logs.count("provider has no handler registered"), 1);
    assert_eq!(logs.count("vanishing"), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_provider_that_returns_mid_pass_runs_the_items_after_it() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let dispatch = FakePluginDispatch::install();
    let gate = Arc::new(Semaphore::new(0));
    let flaky_refresher = dispatch.add(answering_null());
    register_artist_provider(&dispatch, "flaky", flaky_refresher).await?;
    let steady_refresher = dispatch.add(held(&gate));
    register_artist_provider(&dispatch, "steady", steady_refresher).await?;
    let artists = listed_artists(&["One", "Two", "Three"]).await?;
    let library = empty_test_library("returning-provider").await?;
    let progress = crate::services::libraries::begin_test_run(&library).await;
    let covers_root = tempfile::tempdir()?;

    // Each artist is refreshed by `flaky`, then `steady`, which holds the pass there.
    let pass = spawn_artist_pass(artists, covers_root.path(), &progress);
    dispatch.wait_for_calls(steady_refresher, 1).await;
    teardown_plugin_providers(&plugin_of("flaky")).await;
    gate.add_permits(1);
    dispatch.wait_for_calls(steady_refresher, 2).await;
    let returned_refresher = dispatch.add(answering_null());
    register_artist_provider(&dispatch, "flaky", returned_refresher).await?;
    gate.add_permits(2);
    pass.await??;

    assert_eq!(dispatch.artist_names(flaky_refresher), ["One"]);
    assert_eq!(dispatch.artist_names(returned_refresher), ["Three"]);
    assert_eq!(dispatch.calls(steady_refresher).len(), 3);
    assert_eq!(settled_units(&progress).await?, (2, 1, 0, 3));
    Ok(())
}

async fn set_release_filter(provider_id: &str, filter: PluginCallbackHandle) {
    provider_registry().write().await.set_sync_filter_callback(
        provider_id,
        EntityType::Release,
        filter,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_filter_added_mid_pass_applies_to_the_items_after_it() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let dispatch = FakePluginDispatch::install();
    let gate = Arc::new(Semaphore::new(0));
    let refresher = dispatch.add(held(&gate));
    register_release_refresher("gains_filter", refresher).await?;
    let library = library_with_releases("gained-filter", &["One", "Two"]).await?;
    let progress = crate::services::libraries::begin_test_run(&library).await;

    let refresh = spawn_release_refresh(&library, true, &progress);
    dispatch.wait_for_calls(refresher, 1).await;
    let filter = dispatch.add(FakeResponse::Value(serde_json::json!(false)));
    set_release_filter("gains_filter", filter).await;
    gate.add_permits(1);
    refresh.await?.map_err(|err| err.into_anyhow())?;

    assert_eq!(dispatch.calls(filter).len(), 1);
    assert_eq!(dispatch.calls(refresher).len(), 1);
    assert_eq!(settled_units(&progress).await?, (1, 1, 0, 2));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_filter_dropped_mid_pass_leaves_the_items_after_it_unfiltered() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let old = FakePluginDispatch::install();
    let gate = Arc::new(Semaphore::new(0));
    let old_refresher = old.add(held(&gate));
    let filter = old.add(FakeResponse::Value(serde_json::json!(true)));
    register_release_refresher("loses_filter", old_refresher).await?;
    set_release_filter("loses_filter", filter).await;
    let library = library_with_releases("lost-filter", &["One", "Two"]).await?;
    let progress = crate::services::libraries::begin_test_run(&library).await;

    let refresh = spawn_release_refresh(&library, true, &progress);
    old.wait_for_calls(old_refresher, 1).await;
    let new = reload_provider("loses_filter").await;
    let new_refresher = new.add(answering_null());
    register_release_refresher("loses_filter", new_refresher).await?;
    gate.add_permits(1);
    refresh.await?.map_err(|err| err.into_anyhow())?;

    assert_eq!(old.calls(filter).len(), 1);
    assert_eq!(new.calls(new_refresher).len(), 1);
    assert_eq!(settled_units(&progress).await?, (2, 0, 0, 2));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refresh_without_sync_filters_runs_no_filter() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let dispatch = FakePluginDispatch::install();
    let refresher = dispatch.add(answering_null());
    let filter = dispatch.add(FakeResponse::Value(serde_json::json!(false)));
    register_release_refresher("unfiltered", refresher).await?;
    set_release_filter("unfiltered", filter).await;
    let library = library_with_releases("no-filters", &["One"]).await?;
    let progress = SyncRunProgress::unreported_for_test();

    spawn_release_refresh(&library, false, &progress)
        .await?
        .map_err(|err| err.into_anyhow())?;

    assert!(dispatch.calls(filter).is_empty());
    assert_eq!(dispatch.calls(refresher).len(), 1);
    Ok(())
}

async fn release_ids(library: &lyra_db::Library) -> anyhow::Result<Vec<agdb::DbId>> {
    let db = STATE.db.read().await;
    Ok(lyra_db::releases::get(&db, library.db_id.expect("db id"))?
        .into_iter()
        .filter_map(|release| release.db_id.map(agdb::DbId::from))
        .collect())
}

/// The current-thread runtime keeps the scan under the test's log subscriber.
#[tokio::test]
async fn a_scan_settles_its_planned_units_and_asks_no_other_provider() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let logs = CapturedLogs::default();
    let _logs = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_ansi(false)
            .finish(),
    );
    let dispatch = FakePluginDispatch::install();
    let steady = dispatch.add(answering_null());
    let vanished = dispatch.add(answering_null());
    register_release_refresher("steady", steady).await?;
    register_release_refresher("vanished", vanished).await?;
    let plan = super::ScanRefreshPlan::current().await?;
    teardown_plugin_providers(&plugin_of("vanished")).await;
    let newcomer = dispatch.add(answering_null());
    register_release_refresher("newcomer", newcomer).await?;
    let library = library_with_releases("scan-plan", &["One", "Two"]).await?;
    let progress = crate::services::libraries::begin_test_run(&library).await;
    let releases = release_ids(&library).await?;
    progress
        .add_stage_total(
            crate::services::libraries::SyncStageKey::ProviderRefresh,
            releases.len() as u64 * plan.provider_count(),
            crate::services::libraries::SyncTotalState::Final,
        )
        .await;

    for release in releases {
        super::refresh_release_metadata_for_scan(
            library.db_id.expect("db id"),
            release,
            &plan,
            progress.clone(),
            None,
            None,
        )
        .await
        .map_err(|err| err.into_anyhow())?;
    }

    assert_eq!(dispatch.calls(steady).len(), 2);
    assert!(dispatch.calls(vanished).is_empty());
    assert!(dispatch.calls(newcomer).is_empty());
    assert_eq!(settled_units(&progress).await?, (2, 2, 0, 4));
    assert_eq!(logs.count("provider has no handler registered"), 1);
    Ok(())
}

#[tokio::test]
async fn a_scan_of_a_release_gone_settles_every_planned_unit() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let dispatch = FakePluginDispatch::install();
    let handler = dispatch.add(answering_null());
    register_release_refresher("planned", handler).await?;
    let plan = super::ScanRefreshPlan::current().await?;
    let library = library_with_releases("scan-gone", &["Gone"]).await?;
    let progress = crate::services::libraries::begin_test_run(&library).await;
    progress
        .add_stage_total(
            crate::services::libraries::SyncStageKey::ProviderRefresh,
            plan.provider_count(),
            crate::services::libraries::SyncTotalState::Final,
        )
        .await;
    let release = release_ids(&library).await?[0];
    STATE
        .db
        .write()
        .await
        .exec_mut(agdb::QueryBuilder::remove().ids(release).query())?;

    let result = super::refresh_release_metadata_for_scan(
        library.db_id.expect("db id"),
        release,
        &plan,
        progress.clone(),
        None,
        None,
    )
    .await;

    assert!(result.map_err(|err| err.into_anyhow())?.is_empty());
    assert!(dispatch.calls(handler).is_empty());
    assert_eq!(settled_units(&progress).await?, (0, 1, 0, 1));
    Ok(())
}
