// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::*;
use super::{
    progress::{
        PlaybackProgressRequest,
        report_progress,
    },
    queue::{
        QueueReplaceRequest,
        replace_queue,
    },
};
use crate::routes::request::bearer_credential;
use crate::services::remote::handoffs;
use axum::{
    body::to_bytes,
    http::header::{
        AUTHORIZATION,
        CONTENT_TYPE,
    },
    response::{
        IntoResponse,
        Response,
    },
};
use tokio::sync::oneshot;

const TEST_PLAY_ID: &str = "route-test-play-0001";
const NEXT_TEST_PLAY_ID: &str = "route-test-play-0002";

struct RouteFixture {
    headers: HeaderMap,
    user_db_id: DbId,
    user_public_id: String,
    first_track_id: String,
    second_track_id: String,
}

#[tokio::test]
async fn reported_reads_share_the_supplied_expiry_cutoff() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let fixture = setup_admin_with_tracks().await?;
    let principal = require_principal(bearer_credential(&fixture.headers))
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    let mut db = STATE.db.write().await;
    let track_db_id = lyra_db::lookup::find_node_id_by_id(&*db, &fixture.first_track_id)?.unwrap();
    let observed_at = 1_000;
    sessions::report_playback_session_with_cleanup(
        &mut db,
        sessions::SessionPlaybackReportRequest {
            plugin_id: "external",
            user_db_id: fixture.user_db_id,
            session_key: "player",
            track_db_id,
            play_id: None,
            client_name: None,
            mutation: PlaybackMutation {
                state: Some(PlaybackState::Playing),
                ..Default::default()
            },
            now_ms: observed_at,
            active_event: sessions::ActiveEvent::Started,
        },
    )?;
    let playback = lyra_db::playbacks::get_reported(
        &*db,
        fixture.user_db_id,
        "external",
        "player",
        observed_at,
    )?
    .unwrap();
    let id = playback.db_id.unwrap();
    let cutoff = observed_at + lyra_db::playbacks::REPORTED_PLAYBACK_TTL_MS;
    for (current_ms, visible) in [(cutoff, true), (cutoff + 1, false)] {
        assert_eq!(
            !playbacks::list_visible_projections(&db, &principal, false, current_ms)?.is_empty(),
            visible,
        );
        assert_eq!(
            playbacks::get_visible_detail(&db, id, &principal, current_ms)?.is_some(),
            visible,
        );
        assert_eq!(
            playbacks::get_owned_projection(&db, id, fixture.user_db_id, current_ms)?.is_some(),
            visible,
        );
    }
    Ok(())
}

async fn setup_admin_with_tracks() -> anyhow::Result<RouteFixture> {
    crate::plugins::testing::init_library_test_state(std::path::PathBuf::from(".")).await?;
    for connection in remote_registry::list_connections().await {
        remote_registry::unregister(connection.connection_id).await;
    }
    let (user_db_id, user_public_id, first_track_id, second_track_id) = {
        let mut db = STATE.db.write().await;
        lyra_db::roles::ensure_builtin_roles(&mut db)?;
        let user = lyra_db::fixtures::test_user(&format!("playback-route-{}", nanoid!()))?;
        let user_public_id = user.id.clone();
        let user_db_id = lyra_db::users::create(&mut db, &user)?;
        lyra_db::roles::ensure_user_has_role(
            &mut db,
            user_db_id,
            lyra_db::roles::BUILTIN_ADMIN_ROLE,
        )?;
        let first_track_db_id = lyra_db::fixtures::insert_track(&mut db, "First")?;
        let second_track_db_id = lyra_db::fixtures::insert_track(&mut db, "Second")?;
        (
            user_db_id,
            user_public_id,
            resolve_id(&*db, first_track_db_id)?,
            resolve_id(&*db, second_track_db_id)?,
        )
    };
    let session = crate::services::testing::create_session(user_db_id, Default::default()).await?;
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, format!("Bearer {}", session.token).parse()?);
    Ok(RouteFixture {
        headers,
        user_db_id,
        user_public_id,
        first_track_id,
        second_track_id,
    })
}

async fn assert_queue_revision_conflict(
    response: Response,
    expected_revision: u64,
    current_revision: u64,
) {
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );
    let body = to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("conflict body should be readable");
    let body: serde_json::Value =
        serde_json::from_slice(&body).expect("conflict body should be JSON");
    assert_eq!(body["code"], "queue_revision_conflict");
    assert_eq!(body["expected_revision"], expected_revision);
    assert_eq!(body["current_revision"], current_revision);
}

#[test]
fn active_filter_requires_recent_non_terminal_current_session() {
    let current_ms = 1_000_000;
    let playback = lyra_db::playbacks::Playback {
        db_id: Some(DbId(1)),
        id: "playback".to_string(),
        queue: Some(lyra_db::playbacks::Queue {
            revision: 1,
            track_ids: vec!["track".to_string()],
            current_index: 0,
            repeat_mode: playbacks::RepeatMode::None,
            shuffle_enabled: false,
        }),
        reported: None,
        created_at_ms: 1,
        updated_at_ms: 1,
    };
    let current = playbacks::CurrentSession {
        playback_session_id: DbId(2),
        track_db_id: DbId(3),
        track_public_id: "track".to_string(),
        playback: lyra_db::PlaybackSession {
            db_id: Some(DbId(2)),
            id: "internal".to_string(),
            client_name: None,
            position_ms: 0,
            duration_ms: None,
            played_ms: 0,
            last_position_ms: 0,
            state: PlaybackState::Playing,
            play_id: nanoid::nanoid!(),
            latency_allowance_used_ms: 0,
            last_position_at_ms: current_ms,
            updated_at_ms: current_ms,
            created_at_ms: 1,
        },
    };
    let record = playbacks::PlaybackDetail {
        playback_db_id: DbId(1),
        playback,
        current_session: Some(current.clone()),
    };
    assert!(playback_is_active(
        record.current_session.as_ref(),
        current_ms
    ));

    let mut terminal = record.clone();
    terminal.current_session.as_mut().unwrap().playback.state = PlaybackState::Stopped;
    assert!(!playback_is_active(
        terminal.current_session.as_ref(),
        current_ms
    ));

    let mut stale = record;
    stale
        .current_session
        .as_mut()
        .unwrap()
        .playback
        .updated_at_ms = current_ms - ACTIVE_PLAYBACK_TIMEOUT_MS - 1;
    assert!(!playback_is_active(
        stale.current_session.as_ref(),
        current_ms
    ));
}

#[test]
fn controller_is_the_only_supported_include() {
    assert!(
        parse_inc(Some(vec!["controller".to_string()]))
            .unwrap()
            .controller
    );
    assert!(parse_inc(Some(vec!["sessions".to_string()])).is_err());
}

#[tokio::test]
async fn queue_revision_conflict_body_is_machine_readable() {
    assert_queue_revision_conflict(revision_conflict(2, 3).into_response(), 2, 3).await;
}

#[tokio::test]
async fn response_keeps_initiating_and_controlling_client_names_distinct() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let mut fixture = setup_admin_with_tracks().await?;
    let session = crate::services::testing::create_session(
        fixture.user_db_id,
        crate::services::auth::sessions::SessionMetadata {
            client_name: Some("Initiating Client".to_string()),
            ..Default::default()
        },
    )
    .await?;
    fixture
        .headers
        .insert(AUTHORIZATION, format!("Bearer {}", session.token).parse()?);
    let registered = remote_registry::register(
        fixture.user_public_id.clone(),
        Some("Controlling Client".to_string()),
        "controller-session".to_string(),
        tokio::sync::watch::channel(None).0,
    )
    .await
    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let target_id = registered.connection_id;

    let (_, Json(created)) = create_playback(
        fixture.headers.clone(),
        Json(PlaybackCreateRequest {
            play_id: TEST_PLAY_ID.to_string(),
            queue: QueueSnapshot::single(fixture.first_track_id),
            position_ms: Some(0),
            duration_ms: Some(100_000),
            state: Some(PlaybackState::Playing),
            connection_session_key: Some("controller-session".to_string()),
        }),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    assert_eq!(
        created
            .current
            .as_ref()
            .and_then(|current| current.client_name.as_deref()),
        Some("Initiating Client")
    );

    let Json(detail) = get_playback(
        fixture.headers,
        Path(created.id.clone()),
        Query(PlaybackDetailQuery {
            inc: Some(vec!["controller".to_string()]),
        }),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    let json = serde_json::to_value(detail)?;
    assert_eq!(json["current"]["client_name"], "Initiating Client");
    assert_eq!(json["controller"]["client_name"], "Controlling Client");
    assert_eq!(json["controller"]["id"], registered.public_id.as_str());
    assert_eq!(json["controller"]["playback_id"], created.id.as_str());
    assert!(json["controller"].get("session_key").is_none());

    remote_registry::unregister(target_id).await;
    drop(registered.command_rx);
    Ok(())
}

#[tokio::test]
async fn create_replace_queue_and_advance_current_track() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let fixture = setup_admin_with_tracks().await?;
    let headers = fixture.headers;
    let first_track_id = fixture.first_track_id;
    let second_track_id = fixture.second_track_id;

    let (status, Json(created)) = create_playback(
        headers.clone(),
        Json(PlaybackCreateRequest {
            play_id: TEST_PLAY_ID.to_string(),
            queue: QueueSnapshot {
                track_ids: vec![first_track_id.clone(), second_track_id.clone()],
                current_index: 0,
                repeat_mode: playbacks::RepeatMode::All,
                shuffle_enabled: false,
            },
            position_ms: Some(0),
            duration_ms: Some(100_000),
            state: Some(PlaybackState::Playing),
            connection_session_key: None,
        }),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(created.queue_revision, Some(1));
    assert_eq!(created.current.as_ref().unwrap().track_id, first_track_id);
    assert_eq!(
        created.updated_at,
        created.current.as_ref().unwrap().updated_at
    );

    let Json(replaced) = replace_queue(
        headers.clone(),
        Path(created.id.clone()),
        Json(QueueReplaceRequest {
            expected_revision: 1,
            snapshot: QueueSnapshot {
                track_ids: vec![first_track_id, second_track_id.clone()],
                current_index: 1,
                repeat_mode: playbacks::RepeatMode::All,
                shuffle_enabled: false,
            },
        }),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    assert_eq!(replaced.revision, 2);

    let stale = replace_queue(
        headers.clone(),
        Path(created.id.clone()),
        Json(QueueReplaceRequest {
            expected_revision: 1,
            snapshot: QueueSnapshot::single("missing-track".to_string()),
        }),
    )
    .await
    .expect_err("stale CAS must fail")
    .into_response();
    assert_queue_revision_conflict(stale, 1, 2).await;

    let Json(progressed) = report_progress(
        headers,
        Path(created.id),
        Json(PlaybackProgressRequest {
            play_id: NEXT_TEST_PLAY_ID.to_string(),
            queue_revision: 2,
            track_id: Some(second_track_id.clone()),
            position_ms: Some(500),
            duration_ms: Some(90_000),
            state: Some(PlaybackState::Playing),
            connection_session_key: None,
            handoff_token: None,
        }),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    assert_eq!(progressed.queue_revision, Some(2));
    assert_eq!(
        progressed.current.as_ref().unwrap().track_id,
        second_track_id
    );
    assert_eq!(
        progressed.updated_at,
        progressed.current.as_ref().unwrap().updated_at
    );
    Ok(())
}

#[tokio::test]
async fn same_track_handoff_completes_on_exact_tokened_progress() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let fixture = setup_admin_with_tracks().await?;
    let (status, Json(created)) = create_playback(
        fixture.headers.clone(),
        Json(PlaybackCreateRequest {
            play_id: TEST_PLAY_ID.to_string(),
            queue: QueueSnapshot::single(fixture.first_track_id.clone()),
            position_ms: Some(0),
            duration_ms: Some(100_000),
            state: Some(PlaybackState::Playing),
            connection_session_key: Some("target-session".to_string()),
        }),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    assert_eq!(status, StatusCode::CREATED);
    let Json(replaced) = replace_queue(
        fixture.headers.clone(),
        Path(created.id.clone()),
        Json(QueueReplaceRequest {
            expected_revision: 1,
            snapshot: QueueSnapshot::single(fixture.first_track_id.clone()),
        }),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    assert_eq!(replaced.revision, 2);

    let registered = remote_registry::register(
        fixture.user_public_id.clone(),
        Some("Target".to_string()),
        "target-session".to_string(),
        tokio::sync::watch::channel(None).0,
    )
    .await
    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let target_id = registered.connection_id;
    let continued_play_id = {
        let db = STATE.db.read().await;
        playbacks::validate_handoff_queue(&db, fixture.user_db_id, &created.id, 2)?
    };
    assert_eq!(
        continued_play_id.as_deref(),
        Some(TEST_PLAY_ID),
        "the target continues the current play"
    );
    let (handoff_token, completion_rx) = handoffs::begin(
        None,
        target_id,
        fixture.user_public_id.clone(),
        created.id.clone(),
        2,
    )
    .await
    .map_err(|error| anyhow::anyhow!(error))?;
    let wrong_target_progress = report_progress(
        fixture.headers.clone(),
        Path(created.id.clone()),
        Json(PlaybackProgressRequest {
            play_id: TEST_PLAY_ID.to_string(),
            queue_revision: 2,
            track_id: Some(fixture.first_track_id.clone()),
            position_ms: Some(100),
            duration_ms: Some(100_000),
            state: Some(PlaybackState::Playing),
            connection_session_key: Some("other-session".to_string()),
            handoff_token: Some(handoff_token.clone()),
        }),
    )
    .await
    .err()
    .expect("another connection session cannot apply the handoff");
    assert_eq!(
        wrong_target_progress.into_response().status(),
        StatusCode::CONFLICT
    );
    let Json(_) = report_progress(
        fixture.headers,
        Path(created.id.clone()),
        Json(PlaybackProgressRequest {
            play_id: TEST_PLAY_ID.to_string(),
            queue_revision: 2,
            track_id: Some(fixture.first_track_id),
            position_ms: Some(100),
            duration_ms: Some(100_000),
            state: Some(PlaybackState::Playing),
            connection_session_key: Some("target-session".to_string()),
            handoff_token: Some(handoff_token.clone()),
        }),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    assert!(completion_rx.await.expect("handoff completion").is_ok());
    remote_registry::unregister(target_id).await;
    drop(registered.command_rx);
    Ok(())
}

#[tokio::test]
async fn changed_track_handoff_progress_creates_session_and_completes() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let fixture = setup_admin_with_tracks().await?;
    let (_, Json(created)) = create_playback(
        fixture.headers.clone(),
        Json(PlaybackCreateRequest {
            play_id: TEST_PLAY_ID.to_string(),
            queue: QueueSnapshot::single(fixture.first_track_id.clone()),
            position_ms: Some(0),
            duration_ms: Some(100_000),
            state: Some(PlaybackState::Playing),
            connection_session_key: None,
        }),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    let Json(replaced) = replace_queue(
        fixture.headers.clone(),
        Path(created.id.clone()),
        Json(QueueReplaceRequest {
            expected_revision: 1,
            snapshot: QueueSnapshot::single(fixture.second_track_id.clone()),
        }),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    assert_eq!(replaced.revision, 2);

    {
        let db = STATE.db.read().await;
        playbacks::validate_handoff_queue(&db, fixture.user_db_id, &created.id, 2)?
    };
    let registered = remote_registry::register(
        fixture.user_public_id.clone(),
        Some("Target".to_string()),
        "target-session".to_string(),
        tokio::sync::watch::channel(None).0,
    )
    .await
    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let target_id = registered.connection_id;
    let (handoff_token, completion_rx) = handoffs::begin(
        None,
        target_id,
        fixture.user_public_id.clone(),
        created.id.clone(),
        2,
    )
    .await
    .map_err(|error| anyhow::anyhow!(error))?;

    let Json(progressed) = report_progress(
        fixture.headers,
        Path(created.id.clone()),
        Json(PlaybackProgressRequest {
            play_id: NEXT_TEST_PLAY_ID.to_string(),
            queue_revision: 2,
            track_id: Some(fixture.second_track_id.clone()),
            position_ms: Some(0),
            duration_ms: Some(90_000),
            state: Some(PlaybackState::Playing),
            connection_session_key: Some("target-session".to_string()),
            handoff_token: Some(handoff_token.clone()),
        }),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    assert_eq!(
        progressed.current.as_ref().unwrap().track_id,
        fixture.second_track_id
    );
    assert!(completion_rx.await.expect("handoff completion").is_ok());
    remote_registry::unregister(target_id).await;
    drop(registered.command_rx);
    Ok(())
}

#[tokio::test]
async fn queue_replacement_cannot_overtake_committed_handoff_finish() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let fixture = setup_admin_with_tracks().await?;
    let (_, Json(created)) = create_playback(
        fixture.headers.clone(),
        Json(PlaybackCreateRequest {
            play_id: TEST_PLAY_ID.to_string(),
            queue: QueueSnapshot::single(fixture.first_track_id.clone()),
            position_ms: Some(0),
            duration_ms: Some(100_000),
            state: Some(PlaybackState::Playing),
            connection_session_key: None,
        }),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    let registered = remote_registry::register(
        fixture.user_public_id.clone(),
        Some("Target".to_string()),
        "target-session".to_string(),
        tokio::sync::watch::channel(None).0,
    )
    .await
    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let target_id = registered.connection_id;
    let (handoff_token, completion_rx) = handoffs::begin(
        None,
        target_id,
        fixture.user_public_id.clone(),
        created.id.clone(),
        1,
    )
    .await
    .map_err(anyhow::Error::msg)?;

    let committed = {
        let current_ms = now_ms().map_err(|error| anyhow::anyhow!("{error:?}"))?;
        let mut db = STATE.db.write().await;
        let playback_db_id = lyra_db::lookup::find_node_id_by_id(&*db, &created.id)?
            .expect("created playback should remain present");
        let current_track_db_id =
            lyra_db::lookup::find_node_id_by_id(&*db, &fixture.first_track_id)?
                .expect("created track should remain present");
        let claim = handoffs::claim_progress(
            &handoff_token,
            &fixture.user_public_id,
            "target-session",
            &created.id,
            1,
        )
        .await
        .map_err(anyhow::Error::msg)?;
        let update = playbacks::report_progress(
            &mut db,
            playbacks::ReportProgressRequest {
                play_id: TEST_PLAY_ID.to_string(),
                playback_db_id,
                user_db_id: fixture.user_db_id,
                client_name: Some("Target".to_string()),
                queue_revision: 1,
                current_track_db_id,
                mutation: PlaybackMutation {
                    position_ms: Some(100),
                    duration_ms: Some(100_000),
                    state: Some(PlaybackState::Playing),
                },
                now_ms: current_ms,
                require_full_queue_access: true,
            },
        )?;
        claim.commit(handoffs::AppliedProgress {
            user_public_id: fixture.user_public_id.clone(),
            playback_db_id,
            playback_public_id: created.id.clone(),
            queue_revision: 1,
            expected_session: update.session.playback.clone(),
        })
    };

    let registry_guard = handoffs::hold_registry_write_lock_for_test().await;
    let (db_locked_tx, db_locked_rx) = oneshot::channel();
    let finish_task = tokio::spawn(committed.finish_after_db_lock(db_locked_tx));
    db_locked_rx
        .await
        .expect("handoff finish should report after acquiring the DB read guard");
    assert!(
        STATE.db.get().try_write().is_err(),
        "handoff finish must retain the DB guard while waiting for the registry"
    );

    let (replacement_started_tx, replacement_started_rx) = oneshot::channel();
    let replacement_task = tokio::spawn(async move {
        let _ = replacement_started_tx.send(());
        replace_queue(
            fixture.headers,
            Path(created.id.clone()),
            Json(QueueReplaceRequest {
                expected_revision: 1,
                snapshot: QueueSnapshot::single(fixture.second_track_id),
            }),
        )
        .await
    });
    replacement_started_rx
        .await
        .expect("queue replacement should start while handoff finish holds the DB guard");
    drop(registry_guard);

    assert!(finish_task.await?);
    assert!(completion_rx.await?.is_ok());
    let Json(replaced) = replacement_task
        .await?
        .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    assert_eq!(replaced.revision, 2);
    let target_scope = sessions::PlaybackScopeKey {
        plugin_id: NATIVE_PLAYBACK_PLUGIN_ID,
        user_public_id: &fixture.user_public_id,
        session_key: "target-session",
    };
    assert_eq!(
        sessions::get_playback_session(&target_scope)
            .and_then(|scope| scope.current_playback_session_id),
        None,
        "the later queue replacement must clear the serialized handoff binding"
    );
    remote_registry::unregister(target_id).await;
    drop(registered.command_rx);
    Ok(())
}

#[tokio::test]
async fn a_listen_is_announced_even_when_its_handoff_then_fails() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let fixture = setup_admin_with_tracks().await?;
    let dispatch = crate::services::testing::record_plugin_dispatch();
    {
        // A 10 s track counts after 2 s, within the latency allowance of a single report.
        let mut db = STATE.db.write().await;
        let track_db_id = lyra_db::lookup::find_node_id_by_id(&*db, &fixture.first_track_id)?
            .expect("track exists");
        let mut track = lyra_db::tracks::get_by_id(&*db, track_db_id)?.expect("track exists");
        track.duration_ms = Some(10_000);
        lyra_db::tracks::update(&mut db, &track)?;
    }
    let (_, Json(created)) = create_playback(
        fixture.headers.clone(),
        Json(PlaybackCreateRequest {
            play_id: TEST_PLAY_ID.to_string(),
            queue: QueueSnapshot::single(fixture.first_track_id.clone()),
            position_ms: Some(0),
            duration_ms: Some(10_000),
            state: Some(PlaybackState::Playing),
            connection_session_key: None,
        }),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    let registered = remote_registry::register(
        fixture.user_public_id.clone(),
        Some("Target".to_string()),
        "target-session".to_string(),
        tokio::sync::watch::channel(None).0,
    )
    .await
    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let (handoff_token, completion_rx) = handoffs::begin(
        None,
        registered.connection_id,
        fixture.user_public_id.clone(),
        created.id.clone(),
        1,
    )
    .await
    .map_err(anyhow::Error::msg)?;

    // The progress route's steps: claim, report, commit, then finish once the DB is released.
    let committed = {
        let current_ms = now_ms().map_err(|error| anyhow::anyhow!("{error:?}"))?;
        let mut db = STATE.db.write().await;
        let playback_db_id =
            lyra_db::lookup::find_node_id_by_id(&*db, &created.id)?.expect("playback exists");
        let current_track_db_id =
            lyra_db::lookup::find_node_id_by_id(&*db, &fixture.first_track_id)?
                .expect("track exists");
        let claim = handoffs::claim_progress(
            &handoff_token,
            &fixture.user_public_id,
            "target-session",
            &created.id,
            1,
        )
        .await
        .map_err(anyhow::Error::msg)?;
        let update = playbacks::report_progress(
            &mut db,
            playbacks::ReportProgressRequest {
                play_id: TEST_PLAY_ID.to_string(),
                playback_db_id,
                user_db_id: fixture.user_db_id,
                client_name: Some("Target".to_string()),
                queue_revision: 1,
                current_track_db_id,
                mutation: PlaybackMutation {
                    position_ms: Some(5_000),
                    duration_ms: Some(10_000),
                    state: Some(PlaybackState::Playing),
                },
                now_ms: current_ms,
                require_full_queue_access: true,
            },
        )?;
        claim.commit(handoffs::AppliedProgress {
            user_public_id: fixture.user_public_id.clone(),
            playback_db_id,
            playback_public_id: created.id.clone(),
            queue_revision: 1,
            expected_session: update.session.playback.clone(),
        })
    };
    assert_eq!(
        dispatch.listen_updates().len(),
        1,
        "the listen is announced when its report commits"
    );

    handoffs::fail_for_playback(&created.id).await;
    assert!(
        !committed.finish().await,
        "the failed handoff fails the finish"
    );
    assert!(completion_rx.await?.is_err());
    assert_eq!(dispatch.listen_updates().len(), 1);
    remote_registry::unregister(registered.connection_id).await;
    drop(registered.command_rx);
    Ok(())
}

#[tokio::test]
async fn stale_revision_is_rejected_before_handoff_is_queued() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let fixture = setup_admin_with_tracks().await?;
    let (_, Json(created)) = create_playback(
        fixture.headers.clone(),
        Json(PlaybackCreateRequest {
            play_id: TEST_PLAY_ID.to_string(),
            queue: QueueSnapshot::single(fixture.first_track_id.clone()),
            position_ms: Some(0),
            duration_ms: Some(100_000),
            state: Some(PlaybackState::Playing),
            connection_session_key: None,
        }),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    let registered = remote_registry::register(
        fixture.user_public_id.clone(),
        Some("Target".to_string()),
        "target-session".to_string(),
        tokio::sync::watch::channel(None).0,
    )
    .await
    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let target_id = registered.connection_id;
    let target = remote_registry::list_connections()
        .await
        .into_iter()
        .find(|connection| connection.connection_id == target_id)
        .expect("target should remain registered");

    let Json(updated) = replace_queue(
        fixture.headers,
        Path(created.id.clone()),
        Json(QueueReplaceRequest {
            expected_revision: 1,
            snapshot: QueueSnapshot::single(fixture.first_track_id),
        }),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    assert_eq!(updated.revision, 2);

    let error = handoffs::dispatch_and_wait(None, &target, &created.id, 1)
        .await
        .expect_err("the registered handoff must reject the stale revision before queueing");
    assert!(error.contains("current 2"));
    let mut command_rx = registered.command_rx;
    assert!(matches!(
        command_rx.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    remote_registry::unregister(target_id).await;
    Ok(())
}

#[tokio::test]
async fn reported_playback_shares_collection_and_rejects_queue_operations() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let fixture = setup_admin_with_tracks().await?;
    let (_, Json(native)) = create_playback(
        fixture.headers.clone(),
        Json(PlaybackCreateRequest {
            play_id: TEST_PLAY_ID.to_string(),
            queue: QueueSnapshot::single(fixture.first_track_id.clone()),
            position_ms: Some(0),
            duration_ms: Some(100_000),
            state: Some(PlaybackState::Playing),
            connection_session_key: None,
        }),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    let track_db_id = {
        let db = STATE.db.read().await;
        lyra_db::lookup::find_node_id_by_id(&*db, &fixture.second_track_id)?.unwrap()
    };
    {
        let mut db = STATE.db.write().await;
        sessions::report_playback_session_with_cleanup(
            &mut db,
            sessions::SessionPlaybackReportRequest {
                plugin_id: "external",
                user_db_id: fixture.user_db_id,
                session_key: "reported-player",
                track_db_id,
                play_id: None,
                client_name: Some("External player".into()),
                mutation: PlaybackMutation {
                    position_ms: Some(1_200),
                    duration_ms: Some(100_000),
                    state: Some(PlaybackState::Playing),
                },
                now_ms: sessions::now_ms()?,
                active_event: sessions::ActiveEvent::Started,
            },
        )?;
    }
    let Json(page) = list_playbacks(
        fixture.headers.clone(),
        Query(PlaybackQuery {
            active: Some(true),
            ..Default::default()
        }),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    assert_eq!(page.items.len(), 2);
    let reported = page.items.iter().find(|item| item.id != native.id).unwrap();
    assert_eq!(reported.queue_revision, None);
    assert_eq!(
        reported.current.as_ref().unwrap().track_id,
        fixture.second_track_id
    );
    assert_eq!(reported.current.as_ref().unwrap().position_ms, 1_200);
    let id = reported.id.clone();
    let Json(detail) = get_playback(
        fixture.headers.clone(),
        Path(id.clone()),
        Query(PlaybackDetailQuery::default()),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    assert_eq!(detail.id, id);
    assert!(serde_json::to_value(&detail)?["queue_revision"].is_null());

    let queue_status = match queue::get_queue(
        fixture.headers.clone(),
        Path(id.clone()),
        Query(queue::QueueGetQuery { revision: None }),
    )
    .await
    {
        Ok(_) => panic!("reported playback must not have a fabricated queue"),
        Err(error) => error.into_response().status(),
    };
    assert_eq!(queue_status, StatusCode::CONFLICT);
    let replace_status = match replace_queue(
        fixture.headers.clone(),
        Path(id.clone()),
        Json(QueueReplaceRequest {
            expected_revision: 0,
            snapshot: QueueSnapshot::single(fixture.second_track_id.clone()),
        }),
    )
    .await
    {
        Ok(_) => panic!("queue replacement must not convert reported playback"),
        Err(error) => error.into_response().status(),
    };
    assert_eq!(replace_status, StatusCode::CONFLICT);
    let progress_status = match report_progress(
        fixture.headers.clone(),
        Path(id.clone()),
        Json(PlaybackProgressRequest {
            play_id: TEST_PLAY_ID.to_string(),
            queue_revision: 0,
            track_id: Some(fixture.second_track_id.clone()),
            position_ms: Some(2_000),
            duration_ms: None,
            state: Some(PlaybackState::Playing),
            connection_session_key: None,
            handoff_token: None,
        }),
    )
    .await
    {
        Ok(_) => panic!("native progress must require a server-managed queue"),
        Err(error) => error.into_response().status(),
    };
    assert_eq!(progress_status, StatusCode::CONFLICT);

    let other_user_id = {
        let mut db = STATE.db.write().await;
        let user = lyra_db::fixtures::test_user(&format!("other-playback-user-{}", nanoid!()))?;
        let user_id = lyra_db::users::create(&mut db, &user)?;
        lyra_db::roles::ensure_user_has_role(&mut db, user_id, lyra_db::roles::BUILTIN_ADMIN_ROLE)?;
        user_id
    };
    let other_session =
        crate::services::testing::create_session(other_user_id, Default::default()).await?;
    let mut other_headers = HeaderMap::new();
    other_headers.insert(
        AUTHORIZATION,
        format!("Bearer {}", other_session.token).parse()?,
    );
    let foreign_status = match get_playback(
        other_headers.clone(),
        Path(id.clone()),
        Query(PlaybackDetailQuery::default()),
    )
    .await
    {
        Ok(_) => panic!("reported playback must be scoped to its owner"),
        Err(error) => error.into_response().status(),
    };
    assert_eq!(foreign_status, StatusCode::NOT_FOUND);
    let Json(other_page) = list_playbacks(other_headers, Query(PlaybackQuery::default()))
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    assert!(other_page.items.is_empty());
    {
        let mut db = STATE.db.write().await;
        sessions::report_playback_session_with_cleanup(
            &mut db,
            sessions::SessionPlaybackReportRequest {
                plugin_id: "external",
                user_db_id: fixture.user_db_id,
                session_key: "reported-player",
                track_db_id,
                play_id: None,
                client_name: None,
                mutation: PlaybackMutation {
                    state: Some(PlaybackState::Stopped),
                    ..Default::default()
                },
                now_ms: sessions::now_ms()?,
                active_event: sessions::ActiveEvent::Progress,
            },
        )?;
    }
    let Json(active) = list_playbacks(
        fixture.headers.clone(),
        Query(PlaybackQuery {
            active: Some(true),
            ..Default::default()
        }),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    assert_eq!(active.items.len(), 1);
    assert_eq!(active.items[0].id, native.id);
    let Json(stopped) = get_playback(
        fixture.headers,
        Path(id),
        Query(PlaybackDetailQuery::default()),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    assert_eq!(stopped.current.unwrap().state, PlaybackState::Stopped);
    Ok(())
}

#[tokio::test]
async fn malformed_play_ids_are_rejected() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let fixture = setup_admin_with_tracks().await?;
    for play_id in ["too-short", "has spaces in the id", &"x".repeat(65)] {
        let error = create_playback(
            fixture.headers.clone(),
            Json(PlaybackCreateRequest {
                play_id: play_id.to_string(),
                queue: QueueSnapshot::single(fixture.first_track_id.clone()),
                position_ms: Some(0),
                duration_ms: Some(100_000),
                state: Some(PlaybackState::Playing),
                connection_session_key: None,
            }),
        )
        .await
        .err()
        .expect("a malformed play_id is rejected");
        assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);
    }
    Ok(())
}

#[tokio::test]
async fn progress_with_a_new_play_id_starts_a_new_track_session() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let fixture = setup_admin_with_tracks().await?;
    let (_, Json(created)) = create_playback(
        fixture.headers.clone(),
        Json(PlaybackCreateRequest {
            play_id: TEST_PLAY_ID.to_string(),
            queue: QueueSnapshot::single(fixture.first_track_id.clone()),
            position_ms: Some(0),
            duration_ms: Some(100_000),
            state: Some(PlaybackState::Playing),
            connection_session_key: None,
        }),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    assert_eq!(created.current.as_ref().unwrap().play_id, TEST_PLAY_ID);
    let first_session = current_session_id(&created.id).await?;

    let progress = |play_id: &str| PlaybackProgressRequest {
        play_id: play_id.to_string(),
        queue_revision: 1,
        track_id: Some(fixture.first_track_id.clone()),
        position_ms: Some(1_000),
        duration_ms: Some(100_000),
        state: Some(PlaybackState::Playing),
        connection_session_key: None,
        handoff_token: None,
    };
    let Json(_) = report_progress(
        fixture.headers.clone(),
        Path(created.id.clone()),
        Json(progress(TEST_PLAY_ID)),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    assert_eq!(current_session_id(&created.id).await?, first_session);

    let Json(repeated) = report_progress(
        fixture.headers.clone(),
        Path(created.id.clone()),
        Json(progress(NEXT_TEST_PLAY_ID)),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    assert_eq!(
        repeated.current.as_ref().unwrap().play_id,
        NEXT_TEST_PLAY_ID
    );
    let next_session = current_session_id(&created.id).await?;
    assert_ne!(next_session, first_session);
    {
        let db = STATE.db.read().await;
        let replaced = lyra_db::playback_sessions::get_by_id(&*db, first_session.unwrap())?
            .expect("the replaced session is kept");
        assert_eq!(replaced.state, PlaybackState::Paused);
        assert_eq!(
            replaced.position_ms, 1_000,
            "paused at its last reported position"
        );
    }

    let late = report_progress(
        fixture.headers.clone(),
        Path(created.id.clone()),
        Json(progress(TEST_PLAY_ID)),
    )
    .await
    .err()
    .expect("a replaced play cannot come back");
    let late = late.into_response();
    assert_eq!(late.status(), StatusCode::CONFLICT);
    let body = to_bytes(late.into_body(), 64 * 1024).await?;
    let body: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(body["code"], "play_conflict");
    assert_eq!(body["play_id"], TEST_PLAY_ID);
    assert_eq!(current_session_id(&created.id).await?, next_session);

    let error = report_progress(
        fixture.headers,
        Path(created.id),
        Json(progress("bad play id")),
    )
    .await
    .err()
    .expect("a malformed play_id is rejected");
    assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);
    Ok(())
}

async fn current_session_id(playback_public_id: &str) -> anyhow::Result<Option<DbId>> {
    let db = STATE.db.read().await;
    let playback_db_id = lyra_db::lookup::find_node_id_by_id(&*db, playback_public_id)?
        .ok_or_else(|| anyhow::anyhow!("playback missing"))?;
    Ok(lyra_db::playbacks::get_current_session_id(
        &*db,
        playback_db_id,
    )?)
}
