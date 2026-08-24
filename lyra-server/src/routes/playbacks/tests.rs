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
use std::sync::Arc;
use tokio::sync::{
    Notify,
    oneshot,
};

struct RouteFixture {
    headers: HeaderMap,
    user_db_id: DbId,
    user_public_id: String,
    first_track_id: String,
    second_track_id: String,
}

async fn setup_admin_with_tracks() -> anyhow::Result<RouteFixture> {
    crate::testing::initialize_runtime(&crate::testing::LibraryFixtureConfig {
        directory: std::path::PathBuf::from("."),
        language: None,
        country: None,
    })
    .await?;
    for connection in remote_registry::list_connections().await {
        remote_registry::unregister(connection.connection_id).await;
    }
    let (user_db_id, user_public_id, first_track_id, second_track_id) = {
        let mut db = STATE.db.write().await;
        db::roles::ensure_builtin_roles(&mut db)?;
        let user = db::test_db::test_user(&format!("playback-route-{}", nanoid!()))?;
        let user_public_id = user.id.clone();
        let user_db_id = db::users::create(&mut db, &user)?;
        db::roles::ensure_user_has_role(&mut db, user_db_id, db::roles::BUILTIN_ADMIN_ROLE)?;
        let first_track_db_id = db::test_db::insert_track(&mut db, "First")?;
        let second_track_db_id = db::test_db::insert_track(&mut db, "Second")?;
        (
            user_db_id,
            user_public_id,
            resolve_id(&*db, first_track_db_id)?,
            resolve_id(&*db, second_track_db_id)?,
        )
    };
    let session =
        crate::services::auth::sessions::create_session_for_user(user_db_id, Default::default())
            .await?;
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
    let playback = db::playbacks::Playback {
        db_id: Some(DbId(1)),
        id: "playback".to_string(),
        queue_revision: 1,
        track_ids: vec!["track".to_string()],
        current_index: 0,
        repeat_mode: playbacks::RepeatMode::None,
        shuffle_enabled: false,
        created_at_ms: 1,
        updated_at_ms: 1,
    };
    let current = playbacks::CurrentSession {
        playback_session_id: DbId(2),
        track_db_id: DbId(3),
        track_public_id: "track".to_string(),
        playback: db::PlaybackSession {
            db_id: Some(DbId(2)),
            id: "internal".to_string(),
            client_name: None,
            position_ms: 0,
            duration_ms: None,
            activity_ms: Some(0),
            last_position_ms: Some(0),
            state: PlaybackState::Playing,
            listen_recorded: None,
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
    let _guard = crate::testing::runtime_test_lock().await;
    let mut fixture = setup_admin_with_tracks().await?;
    let session = crate::services::auth::sessions::create_session_for_user(
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
        fixture.user_db_id,
        fixture.user_public_id,
        Some("Controlling Client".to_string()),
        "controller-session".to_string(),
        Arc::new(Notify::new()),
    )
    .await
    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let target_id = registered.connection_id;

    let (_, Json(created)) = create_playback(
        fixture.headers.clone(),
        Json(PlaybackCreateRequest {
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
        Path(created.id),
        Query(PlaybackDetailQuery {
            inc: Some(vec!["controller".to_string()]),
        }),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    let json = serde_json::to_value(detail)?;
    assert_eq!(json["current"]["client_name"], "Initiating Client");
    assert_eq!(json["controller"]["client_name"], "Controlling Client");

    remote_registry::unregister(target_id).await;
    drop(registered.command_rx);
    Ok(())
}

#[tokio::test]
async fn create_replace_queue_and_advance_current_track() -> anyhow::Result<()> {
    let _guard = crate::testing::runtime_test_lock().await;
    let fixture = setup_admin_with_tracks().await?;
    let headers = fixture.headers;
    let first_track_id = fixture.first_track_id;
    let second_track_id = fixture.second_track_id;

    let (status, Json(created)) = create_playback(
        headers.clone(),
        Json(PlaybackCreateRequest {
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
    assert_eq!(created.queue_revision, 1);
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
            snapshot: replaced.snapshot.clone(),
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
    assert_eq!(progressed.queue_revision, 2);
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
    let _guard = crate::testing::runtime_test_lock().await;
    let fixture = setup_admin_with_tracks().await?;
    let (status, Json(created)) = create_playback(
        fixture.headers.clone(),
        Json(PlaybackCreateRequest {
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
        fixture.user_db_id,
        fixture.user_public_id,
        Some("Target".to_string()),
        "target-session".to_string(),
        Arc::new(Notify::new()),
    )
    .await
    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let target_id = registered.connection_id;
    {
        let db = STATE.db.read().await;
        playbacks::validate_handoff_queue(&db, fixture.user_db_id, &created.id, 2)?
    };
    let (handoff_token, completion_rx) =
        handoffs::begin(None, target_id, fixture.user_db_id, created.id.clone(), 2)
            .await
            .map_err(|error| anyhow::anyhow!(error))?;
    let wrong_target_progress = report_progress(
        fixture.headers.clone(),
        Path(created.id.clone()),
        Json(PlaybackProgressRequest {
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
    let _guard = crate::testing::runtime_test_lock().await;
    let fixture = setup_admin_with_tracks().await?;
    let (_, Json(created)) = create_playback(
        fixture.headers.clone(),
        Json(PlaybackCreateRequest {
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
        fixture.user_db_id,
        fixture.user_public_id,
        Some("Target".to_string()),
        "target-session".to_string(),
        Arc::new(Notify::new()),
    )
    .await
    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let target_id = registered.connection_id;
    let (handoff_token, completion_rx) =
        handoffs::begin(None, target_id, fixture.user_db_id, created.id.clone(), 2)
            .await
            .map_err(|error| anyhow::anyhow!(error))?;

    let Json(progressed) = report_progress(
        fixture.headers,
        Path(created.id.clone()),
        Json(PlaybackProgressRequest {
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
    let _guard = crate::testing::runtime_test_lock().await;
    let fixture = setup_admin_with_tracks().await?;
    let (_, Json(created)) = create_playback(
        fixture.headers.clone(),
        Json(PlaybackCreateRequest {
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
        fixture.user_db_id,
        fixture.user_public_id,
        Some("Target".to_string()),
        "target-session".to_string(),
        Arc::new(Notify::new()),
    )
    .await
    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let target_id = registered.connection_id;
    let (handoff_token, completion_rx) =
        handoffs::begin(None, target_id, fixture.user_db_id, created.id.clone(), 1)
            .await
            .map_err(anyhow::Error::msg)?;

    let committed = {
        let current_ms = now_ms()?;
        let mut db = STATE.db.write().await;
        let playback_db_id = db::lookup::find_node_id_by_id(&*db, &created.id)?
            .expect("created playback should remain present");
        let current_track_db_id = db::lookup::find_node_id_by_id(&*db, &fixture.first_track_id)?
            .expect("created track should remain present");
        let claim = handoffs::claim_progress(
            &handoff_token,
            fixture.user_db_id,
            "target-session",
            &created.id,
            1,
        )
        .await
        .map_err(anyhow::Error::msg)?;
        let update = playbacks::report_progress(
            &mut db,
            playbacks::ReportProgressRequest {
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
            user_db_id: fixture.user_db_id,
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
        STATE.db.try_write().is_err(),
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
        user_db_id: fixture.user_db_id,
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
async fn stale_revision_is_rejected_before_handoff_is_queued() -> anyhow::Result<()> {
    let _guard = crate::testing::runtime_test_lock().await;
    let fixture = setup_admin_with_tracks().await?;
    let (_, Json(created)) = create_playback(
        fixture.headers.clone(),
        Json(PlaybackCreateRequest {
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
        fixture.user_db_id,
        fixture.user_public_id,
        Some("Target".to_string()),
        "target-session".to_string(),
        Arc::new(Notify::new()),
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
