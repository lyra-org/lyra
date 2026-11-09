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
    PluginCallbackHandle,
    STATE,
    provider_registry,
};
use crate::services::covers::CoverSyncOptions;
use crate::services::libraries::{
    SyncRunProgress,
    SyncStageKey,
    SyncTotalState,
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

    let error =
        super::dispatch::dispatch_refresh_callback("late-provider", handler, serde_json::json!({}))
            .await
            .expect_err("a handler past its deadline fails");

    assert!(
        format!("{error:#}").contains("plugin handler deadline expired"),
        "the runtime, having cancelled the handler, answers before the server stops waiting: {error:#}"
    );
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

async fn wait_for_calls(dispatch: &FakePluginDispatch, handler: PluginCallbackHandle, n: usize) {
    for _ in 0..500 {
        if dispatch.calls(handler).len() >= n {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("handler was not called {n} times");
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

    let scan = {
        let library = library.db_id.expect("db id");
        let progress = progress.clone();
        tokio::spawn(async move {
            super::library::refresh_release_metadata_for_scan(
                library, release, progress, public_id, None,
            )
            .await
        })
    };
    wait_for_calls(&dispatch, first, 1).await;
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
    wait_for_calls(&dispatch, first, 1).await;
    gate.add_permits(1);
    wait_for_calls(&dispatch, first, 2).await;
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
                    id: nanoid::nanoid!(),
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
    for _ in 0..500 {
        if !dispatch.calls(refresher).is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(dispatch.calls(refresher).len(), 1, "the refresh started");
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
    for _ in 0..500 {
        if !dispatch.calls(handler).is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(dispatch.calls(handler).len(), 1, "the lookup started");
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
    wait_for_calls(&dispatch, handler, 1).await;
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
        crate::services::providers::refresh_entity(&admin, &release_id, Default::default()).await
    });
    wait_for_calls(&dispatch, first, 1).await;
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
