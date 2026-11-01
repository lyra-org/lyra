// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    DbId,
    QueryBuilder,
};
use lyra_db::{
    PlaybackState,
    test_db::new_test_db,
};
use nanoid::nanoid;

use super::*;
use crate::services::{
    playback_sessions::{
        PlaybackRecord,
        collect_playback_records,
        report_playback,
    },
    playbacks::cleanup_evicted_playbacks,
    testing::RuntimeTestGuard,
};

struct Reported {
    playback: Option<PlaybackRecord>,
}

fn report_playback_session_with_cleanup(
    db: &mut DbAny,
    request: ReportedPlaybackRequest<'_>,
) -> anyhow::Result<Reported> {
    Ok(match report_reported_playback(db, request)? {
        ReportOutcome::Applied(update) => Reported {
            playback: Some(update.session),
        },
        ReportOutcome::Ignored(_) => Reported { playback: None },
    })
}

fn report_playback_session(
    db: &mut DbAny,
    request: ReportedPlaybackRequest<'_>,
) -> anyhow::Result<Option<PlaybackRecord>> {
    Ok(report_playback_session_with_cleanup(db, request)?.playback)
}

fn report_test_playback_session(
    db: &mut DbAny,
    user_db_id: DbId,
    track_db_id: DbId,
    active_event: ActiveEvent,
    mutation: PlaybackMutation,
    now_ms: u64,
) -> anyhow::Result<Option<PlaybackRecord>> {
    report_playback_session(
        db,
        ReportedPlaybackRequest {
            plugin_id: "external",
            user_db_id,
            session_key: "auth:1",
            track_db_id,
            play_id: None,
            client_name: None,
            mutation,
            now_ms,
            active_event,
        },
    )
}

fn report_test_playback_session_with_cleanup(
    db: &mut DbAny,
    user_db_id: DbId,
    track_db_id: DbId,
    active_event: ActiveEvent,
    mutation: PlaybackMutation,
    now_ms: u64,
) -> anyhow::Result<Reported> {
    report_playback_session_with_cleanup(
        db,
        ReportedPlaybackRequest {
            plugin_id: "external",
            user_db_id,
            session_key: "auth:1",
            track_db_id,
            play_id: None,
            client_name: None,
            mutation,
            now_ms,
            active_event,
        },
    )
}

/// A realistic server time; listens need starts after 2010.
const T0: u64 = 1_800_000_000_000;

async fn new_scoped_test_db() -> anyhow::Result<(DbAny, RuntimeTestGuard)> {
    let guard = crate::services::testing::runtime_test_lock().await;
    crate::services::testing::init_default_test_state()?;
    Ok((new_test_db()?, guard))
}

fn user_public_id(db: &DbAny, user_db_id: DbId) -> String {
    lyra_db::users::get_by_id(db, user_db_id)
        .expect("user lookup")
        .expect("user exists")
        .id
}

fn insert_user(db: &mut DbAny, username: &str) -> anyhow::Result<DbId> {
    let user = lyra_db::User {
        db_id: None,
        id: nanoid!(),
        username: username.to_string(),
        password: "test".to_string(),
    };
    let user_db_id = db
        .exec_mut(QueryBuilder::insert().element(&user).query())?
        .ids()[0];
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from("users")
            .to(user_db_id)
            .query(),
    )?;
    Ok(user_db_id)
}

fn insert_track(db: &mut DbAny, title: &str, duration_ms: u64) -> anyhow::Result<DbId> {
    let track = lyra_db::Track {
        db_id: None,
        id: nanoid!(),
        track_title: title.to_string(),
        sort_title: None,
        year: None,
        disc: None,
        disc_total: None,
        track: None,
        track_total: None,
        duration_ms: Some(duration_ms),
        track_gain_db: None,
        album_gain_db: None,
        locked: Some(false),
        created_at_ms: Some(0),
        file_created_at_ms: Some(0),
    };
    let track_db_id = db
        .exec_mut(QueryBuilder::insert().element(&track).query())?
        .ids()[0];
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from("tracks")
            .to(track_db_id)
            .query(),
    )?;
    Ok(track_db_id)
}

#[tokio::test]
async fn first_paused_report_establishes_playback_but_terminal_report_does_not()
-> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let track = insert_track(&mut db, "A", 200_000)?;
    assert!(
        report_test_playback_session(
            &mut db,
            user,
            track,
            ActiveEvent::Progress,
            PlaybackMutation {
                state: Some(PlaybackState::Stopped),
                ..Default::default()
            },
            1_000
        )?
        .is_none()
    );
    assert!(lyra_db::playbacks::get_reported(&db, user, "external", "auth:1", 1_000)?.is_none());
    let paused = report_test_playback_session(
        &mut db,
        user,
        track,
        ActiveEvent::Progress,
        PlaybackMutation {
            state: Some(PlaybackState::Paused),
            position_ms: Some(10_000),
            ..Default::default()
        },
        2_000,
    )?
    .unwrap();
    let root = lyra_db::playbacks::get_reported(&db, user, "external", "auth:1", 2_000)?.unwrap();
    assert_eq!(
        lyra_db::playbacks::get_current_session_id(&db, root.db_id.unwrap())?,
        Some(paused.playback_session_id)
    );
    assert_eq!(paused.playback.state, PlaybackState::Paused);
    Ok(())
}

#[tokio::test]
async fn reported_playback_survives_switch_stop_and_runtime_restart() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let track_a = insert_track(&mut db, "A", 200_000)?;
    let track_b = insert_track(&mut db, "B", 200_000)?;
    let playing = || PlaybackMutation {
        state: Some(PlaybackState::Playing),
        duration_ms: Some(200_000),
        position_ms: Some(0),
    };
    let a = report_test_playback_session(
        &mut db,
        user,
        track_a,
        ActiveEvent::Progress,
        playing(),
        1_000,
    )?
    .unwrap();
    let root = lyra_db::playbacks::get_reported(&db, user, "external", "auth:1", 1_000)?.unwrap();
    assert!(root.queue().is_none());
    let root_id = root.db_id.unwrap();
    let resumed = report_test_playback_session(
        &mut db,
        user,
        track_a,
        ActiveEvent::Progress,
        playing(),
        1_500,
    )?
    .unwrap();
    assert_eq!(resumed.playback_session_id, a.playback_session_id);
    let b = report_test_playback_session(
        &mut db,
        user,
        track_b,
        ActiveEvent::Started,
        playing(),
        2_000,
    )?
    .unwrap();
    assert_eq!(
        lyra_db::playbacks::get_current_session_id(&db, root_id)?,
        Some(b.playback_session_id)
    );
    let late = report_test_playback_session(
        &mut db,
        user,
        track_a,
        ActiveEvent::Progress,
        PlaybackMutation {
            state: Some(PlaybackState::Stopped),
            ..Default::default()
        },
        3_000,
    )?
    .unwrap();
    assert_eq!(late.playback_session_id, a.playback_session_id);
    let unchanged =
        lyra_db::playbacks::get_reported(&db, user, "external", "auth:1", 3_000)?.unwrap();
    assert_eq!(unchanged.id, root.id);
    assert_eq!(unchanged.updated_at_ms, 2_000);
    assert_eq!(
        lyra_db::playbacks::get_current_session_id(&db, root_id)?,
        Some(b.playback_session_id)
    );
    report_test_playback_session(
        &mut db,
        user,
        track_b,
        ActiveEvent::Progress,
        PlaybackMutation {
            state: Some(PlaybackState::Stopped),
            ..Default::default()
        },
        4_000,
    )?
    .unwrap();
    assert_eq!(
        lyra_db::playbacks::get_current_session_id(&db, root_id)?,
        Some(b.playback_session_id)
    );
    assert!(
        report_test_playback_session(
            &mut db,
            user,
            track_b,
            ActiveEvent::Progress,
            PlaybackMutation::default(),
            5_000
        )?
        .is_none()
    );
    let stopped =
        lyra_db::playbacks::get_reported(&db, user, "external", "auth:1", 5_000)?.unwrap();
    assert_eq!(stopped.updated_at_ms, 4_000);
    let restarted = report_test_playback_session(
        &mut db,
        user,
        track_b,
        ActiveEvent::Progress,
        playing(),
        6_000,
    )?
    .unwrap();
    assert_ne!(
        restarted.playback_session_public_id,
        b.playback_session_public_id
    );
    assert_eq!(
        lyra_db::playbacks::get_reported(&db, user, "external", "auth:1", 6_000)?
            .unwrap()
            .id,
        root.id
    );
    Ok(())
}

#[tokio::test]
async fn terminal_current_resumes_without_promoting_previous_progress() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let track = insert_track(&mut db, "A", 200_000)?;
    let other = insert_track(&mut db, "B", 200_000)?;
    let mut now = 1_000;
    let mut prior = report_test_playback_session(
        &mut db,
        user,
        track,
        ActiveEvent::Started,
        PlaybackMutation {
            state: Some(PlaybackState::Playing),
            ..Default::default()
        },
        now,
    )?
    .unwrap();
    let root_id = lyra_db::playbacks::get_reported(&db, user, "external", "auth:1", now)?
        .unwrap()
        .id;
    for state in [
        PlaybackState::Playing,
        PlaybackState::Paused,
        PlaybackState::Buffering,
    ] {
        now += 1_000;
        report_test_playback_session(
            &mut db,
            user,
            track,
            ActiveEvent::Progress,
            PlaybackMutation {
                state: Some(PlaybackState::Stopped),
                ..Default::default()
            },
            now,
        )?
        .unwrap();
        now += 1_000;
        assert!(
            report_test_playback_session(
                &mut db,
                user,
                track,
                ActiveEvent::Progress,
                PlaybackMutation {
                    state: Some(PlaybackState::Stopped),
                    ..Default::default()
                },
                now
            )?
            .is_none()
        );
        assert!(
            report_test_playback_session(
                &mut db,
                user,
                track,
                ActiveEvent::Progress,
                PlaybackMutation::default(),
                now
            )?
            .is_none()
        );
        let resumed = report_test_playback_session(
            &mut db,
            user,
            track,
            ActiveEvent::Progress,
            PlaybackMutation {
                state: Some(state),
                ..Default::default()
            },
            now,
        )?
        .unwrap();
        assert_ne!(
            resumed.playback_session_public_id,
            prior.playback_session_public_id
        );
        assert_eq!(resumed.playback.state, state);
        assert_eq!(
            lyra_db::playbacks::get_reported(&db, user, "external", "auth:1", now)?
                .unwrap()
                .id,
            root_id
        );
        prior = resumed;
    }
    now += 1_000;
    report_test_playback_session(
        &mut db,
        user,
        track,
        ActiveEvent::Progress,
        PlaybackMutation {
            state: Some(PlaybackState::Stopped),
            ..Default::default()
        },
        now,
    )?;
    now += 1_000;
    let current = report_test_playback_session(
        &mut db,
        user,
        other,
        ActiveEvent::Started,
        PlaybackMutation {
            state: Some(PlaybackState::Playing),
            ..Default::default()
        },
        now,
    )?
    .unwrap();
    now += 1_000;
    assert!(
        report_test_playback_session(
            &mut db,
            user,
            track,
            ActiveEvent::Progress,
            PlaybackMutation {
                state: Some(PlaybackState::Playing),
                ..Default::default()
            },
            now
        )?
        .is_none()
    );
    let root = lyra_db::playbacks::get_reported(&db, user, "external", "auth:1", now)?.unwrap();
    assert_eq!(root.updated_at_ms, now - 1_000);
    assert_eq!(
        lyra_db::playbacks::get_current_session_id(&db, root.db_id.unwrap())?,
        Some(current.playback_session_id)
    );
    now += 1_000;
    report_test_playback_session(
        &mut db,
        user,
        other,
        ActiveEvent::Progress,
        PlaybackMutation {
            state: Some(PlaybackState::Stopped),
            ..Default::default()
        },
        now,
    )?;
    now += 1_000;
    let restarted = report_test_playback_session(
        &mut db,
        user,
        other,
        ActiveEvent::Started,
        PlaybackMutation::default(),
        now,
    )?
    .unwrap();
    assert_eq!(restarted.playback.state, PlaybackState::Playing);
    assert_ne!(
        restarted.playback_session_public_id,
        current.playback_session_public_id
    );
    assert_eq!(
        lyra_db::playbacks::get_reported(&db, user, "external", "auth:1", now)?
            .unwrap()
            .id,
        root_id
    );
    Ok(())
}

#[tokio::test]
async fn reported_playback_expires_without_losing_credited_listen() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let track = insert_track(&mut db, "A", 200_000)?;
    report_test_playback_session_with_cleanup(
        &mut db,
        user,
        track,
        ActiveEvent::Started,
        PlaybackMutation {
            position_ms: Some(0),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        T0 + 1_000,
    )?;
    let credited = report_test_playback_session_with_cleanup(
        &mut db,
        user,
        track,
        ActiveEvent::Progress,
        PlaybackMutation {
            position_ms: Some(110_000),
            state: Some(PlaybackState::Playing),
            ..Default::default()
        },
        T0 + 111_000,
    )?
    .playback
    .unwrap();
    assert!(lyra_db::listens::find_by_play_id(&db, user, &credited.playback.play_id)?.is_some());
    let root =
        lyra_db::playbacks::get_reported(&db, user, "external", "auth:1", T0 + 111_000)?.unwrap();
    cleanup_evicted_playbacks(&mut db, T0 + 411_001)?;
    assert!(
        lyra_db::playbacks::get_reported(&db, user, "external", "auth:1", T0 + 411_001)?.is_none()
    );
    report_test_playback_session(
        &mut db,
        user,
        track,
        ActiveEvent::Progress,
        PlaybackMutation {
            position_ms: Some(0),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        T0 + 412_000,
    )?;
    let fresh =
        lyra_db::playbacks::get_reported(&db, user, "external", "auth:1", T0 + 412_000)?.unwrap();
    assert_ne!(fresh.id, root.id);
    cleanup_evicted_playbacks(&mut db, T0 + 900_000)?;
    assert!(
        lyra_db::playback_sessions::get_by_id(&db, credited.playback_session_id)?.is_none(),
        "a counted play's session goes once nothing holds it"
    );
    let counts = lyra_db::listens::get_counts(&db, &[track], user)?;
    assert_eq!(counts.get(&track).copied(), Some(1));
    Ok(())
}

#[tokio::test]
async fn playback_client_name_snapshot_is_persisted_and_stable() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user_db_id = insert_user(&mut db, "alice")?;
    let track_db_id = insert_track(&mut db, "Track A", 200_000)?;

    let started = report_playback_session(
        &mut db,
        ReportedPlaybackRequest {
            plugin_id: "external",
            user_db_id,
            session_key: "browser:1",
            track_db_id,
            play_id: None,
            client_name: Some("Living Room".to_string()),
            mutation: PlaybackMutation {
                position_ms: Some(0),
                duration_ms: Some(200_000),
                state: Some(PlaybackState::Playing),
            },
            now_ms: 1_000,
            active_event: ActiveEvent::Started,
        },
    )?
    .expect("playback should start");

    assert_eq!(started.playback.client_name.as_deref(), Some("Living Room"));
    let persisted = lyra_db::playback_sessions::get_by_id(&db, started.playback_session_id)?
        .expect("playback should be persisted");
    assert_eq!(persisted.client_name.as_deref(), Some("Living Room"));

    let updated = report_playback(
        &mut db,
        started.playback_session_id,
        ReportPlaybackRequest {
            user_db_id,
            mutation: PlaybackMutation {
                position_ms: Some(1_000),
                duration_ms: None,
                state: None,
            },
            now_ms: 2_000,
            active_event: ActiveEvent::Progress,
        },
    )?;
    assert_eq!(updated.playback.client_name.as_deref(), Some("Living Room"));
    Ok(())
}

#[tokio::test]
async fn report_playback_session_rotates_current_to_previous_on_track_switch() -> anyhow::Result<()>
{
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user_db_id = insert_user(&mut db, "alice")?;
    let track_a_id = insert_track(&mut db, "Track A", 200_000)?;
    let track_b_id = insert_track(&mut db, "Track B", 200_000)?;

    let started_a = report_test_playback_session(
        &mut db,
        user_db_id,
        track_a_id,
        ActiveEvent::Started,
        PlaybackMutation {
            position_ms: Some(0),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        1_000,
    )?
    .expect("track A should start");
    let started_b = report_test_playback_session(
        &mut db,
        user_db_id,
        track_b_id,
        ActiveEvent::Started,
        PlaybackMutation {
            position_ms: Some(0),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        2_000,
    )?
    .expect("track B should start");

    let root_db_id =
        lyra_db::playbacks::get_reported(&db, user_db_id, "external", "auth:1", 2_000)?
            .and_then(|root| root.db_id)
            .expect("the reported playback");
    assert_eq!(
        lyra_db::playbacks::get_current_session_id(&db, root_db_id)?,
        Some(started_b.playback_session_id)
    );
    assert_eq!(
        lyra_db::playbacks::get_previous_session(&db, root_db_id)?,
        Some(lyra_db::playbacks::PreviousSession {
            session_db_id: started_a.playback_session_id,
            demoted_at_ms: 2_000,
            expires_at_ms: 2_000 + PREVIOUS_PLAYBACK_GRACE_MS,
        })
    );
    assert_eq!(started_a.track_db_id, track_a_id);
    assert_eq!(started_b.track_db_id, track_b_id);
    Ok(())
}

#[tokio::test]
async fn report_playback_session_routes_late_terminal_to_previous_track() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user_db_id = insert_user(&mut db, "alice")?;
    let track_a_id = insert_track(&mut db, "Track A", 200_000)?;
    let track_b_id = insert_track(&mut db, "Track B", 200_000)?;

    let started_a = report_test_playback_session(
        &mut db,
        user_db_id,
        track_a_id,
        ActiveEvent::Started,
        PlaybackMutation {
            position_ms: Some(0),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        1_000,
    )?
    .expect("track A should start");
    let playback_a_id = started_a.playback_session_id;

    report_test_playback_session(
        &mut db,
        user_db_id,
        track_b_id,
        ActiveEvent::Started,
        PlaybackMutation {
            position_ms: Some(0),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        2_000,
    )?
    .expect("track B should start");

    let late_terminal_a = report_test_playback_session(
        &mut db,
        user_db_id,
        track_a_id,
        ActiveEvent::Progress,
        PlaybackMutation {
            position_ms: Some(198_000),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Completed),
        },
        3_000,
    )?
    .expect("late terminal update should apply to previous track");

    assert_eq!(late_terminal_a.playback_session_id, playback_a_id);
    assert_eq!(late_terminal_a.playback.state, PlaybackState::Completed);

    Ok(())
}

#[tokio::test]
async fn report_playback_session_ignores_displaced_previous_progress() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user_db_id = insert_user(&mut db, "alice")?;
    let track_a_id = insert_track(&mut db, "Track A", 200_000)?;
    let track_b_id = insert_track(&mut db, "Track B", 200_000)?;

    let started_a = report_test_playback_session(
        &mut db,
        user_db_id,
        track_a_id,
        ActiveEvent::Started,
        PlaybackMutation {
            position_ms: Some(0),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        1_000,
    )?
    .expect("track A should start");

    report_test_playback_session(
        &mut db,
        user_db_id,
        track_b_id,
        ActiveEvent::Started,
        PlaybackMutation {
            position_ms: Some(0),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        2_000,
    )?
    .expect("track B should start");

    let ignored = report_test_playback_session(
        &mut db,
        user_db_id,
        track_a_id,
        ActiveEvent::Progress,
        PlaybackMutation {
            position_ms: Some(30_000),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        3_000,
    )?;
    assert!(ignored.is_none());

    let persisted = lyra_db::playback_sessions::get_by_id(&db, started_a.playback_session_id)?
        .expect("playback should exist");
    assert_eq!(persisted.position_ms, 0);
    Ok(())
}

#[tokio::test]
async fn report_playback_session_ignores_expired_previous_terminal_update() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user_db_id = insert_user(&mut db, "alice")?;
    let track_a_id = insert_track(&mut db, "Track A", 200_000)?;
    let track_b_id = insert_track(&mut db, "Track B", 200_000)?;

    let started_a = report_test_playback_session(
        &mut db,
        user_db_id,
        track_a_id,
        ActiveEvent::Started,
        PlaybackMutation {
            position_ms: Some(0),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        1_000,
    )?
    .expect("track A should start");

    report_test_playback_session(
        &mut db,
        user_db_id,
        track_b_id,
        ActiveEvent::Started,
        PlaybackMutation {
            position_ms: Some(0),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        2_000,
    )?
    .expect("track B should start");

    let ignored = report_test_playback_session(
        &mut db,
        user_db_id,
        track_a_id,
        ActiveEvent::Progress,
        PlaybackMutation {
            position_ms: Some(198_000),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Completed),
        },
        2_000 + PREVIOUS_PLAYBACK_GRACE_MS + 1,
    )?;
    assert!(ignored.is_none());

    let persisted = lyra_db::playback_sessions::get_by_id(&db, started_a.playback_session_id)?
        .expect("playback should exist");
    assert_eq!(persisted.state, PlaybackState::Playing);
    Ok(())
}

#[tokio::test]
async fn report_playback_session_allows_progress_only_track_switch() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user_db_id = insert_user(&mut db, "alice")?;
    let track_a_id = insert_track(&mut db, "Track A", 200_000)?;
    let track_b_id = insert_track(&mut db, "Track B", 200_000)?;

    report_test_playback_session(
        &mut db,
        user_db_id,
        track_a_id,
        ActiveEvent::Started,
        PlaybackMutation {
            position_ms: Some(0),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        1_000,
    )?
    .expect("track A should start");

    let switched = report_test_playback_session(
        &mut db,
        user_db_id,
        track_b_id,
        ActiveEvent::Progress,
        PlaybackMutation {
            position_ms: Some(7_000),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        2_000,
    )?
    .expect("progress-only switch should create track B playback");

    assert_eq!(switched.track_db_id, track_b_id);
    let records = collect_playback_records(&db)?;
    assert_eq!(records.len(), 2);
    Ok(())
}

#[tokio::test]
async fn ignored_terminal_report_persists_previous_cleanup_without_refreshing_root()
-> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    for missing in [false, true] {
        let user = insert_user(&mut db, if missing { "missing" } else { "expired" })?;
        let a = insert_track(&mut db, "A", 200_000)?;
        let b = insert_track(&mut db, "B", 200_000)?;
        let previous = report_test_playback_session(
            &mut db,
            user,
            a,
            ActiveEvent::Started,
            PlaybackMutation::default(),
            1_000,
        )?
        .unwrap();
        report_test_playback_session(
            &mut db,
            user,
            b,
            ActiveEvent::Started,
            PlaybackMutation::default(),
            2_000,
        )?;
        report_test_playback_session(
            &mut db,
            user,
            b,
            ActiveEvent::Progress,
            PlaybackMutation {
                state: Some(PlaybackState::Stopped),
                ..Default::default()
            },
            3_000,
        )?;
        let before =
            lyra_db::playbacks::get_reported(&db, user, "external", "auth:1", 3_000)?.unwrap();
        assert!(lyra_db::playbacks::get_previous_session(&db, before.db_id.unwrap())?.is_some());
        let now = if missing {
            db.exec_mut(
                QueryBuilder::remove()
                    .ids(previous.playback_session_id)
                    .query(),
            )?;
            4_000
        } else {
            2_001 + PREVIOUS_PLAYBACK_GRACE_MS
        };
        assert!(
            report_test_playback_session(
                &mut db,
                user,
                b,
                ActiveEvent::Progress,
                PlaybackMutation {
                    state: Some(PlaybackState::Stopped),
                    ..Default::default()
                },
                now,
            )?
            .is_none()
        );
        let after =
            lyra_db::playbacks::get_reported(&db, user, "external", "auth:1", now)?.unwrap();
        assert!(lyra_db::playbacks::get_previous_session(&db, after.db_id.unwrap())?.is_none());
        assert_eq!(after.updated_at_ms, 3_000);
    }
    Ok(())
}

#[tokio::test]
async fn report_playback_session_credits_two_listens_for_started_replay() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user_db_id = insert_user(&mut db, "alice")?;
    let track_db_id = insert_track(&mut db, "Track A", 200_000)?;

    report_test_playback_session_with_cleanup(
        &mut db,
        user_db_id,
        track_db_id,
        ActiveEvent::Started,
        PlaybackMutation {
            position_ms: Some(0),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        T0 + 1_000,
    )?;
    report_test_playback_session_with_cleanup(
        &mut db,
        user_db_id,
        track_db_id,
        ActiveEvent::Progress,
        PlaybackMutation {
            position_ms: Some(60_000),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        T0 + 61_000,
    )?;

    let counts_after_first = lyra_db::listens::get_counts(&db, &[track_db_id], user_db_id)?;
    assert_eq!(counts_after_first.get(&track_db_id).copied(), Some(1));

    // Explicit restart of the same track.
    report_test_playback_session_with_cleanup(
        &mut db,
        user_db_id,
        track_db_id,
        ActiveEvent::Started,
        PlaybackMutation {
            position_ms: Some(0),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        T0 + 62_000,
    )?;
    report_test_playback_session_with_cleanup(
        &mut db,
        user_db_id,
        track_db_id,
        ActiveEvent::Progress,
        PlaybackMutation {
            position_ms: Some(60_000),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        T0 + 122_000,
    )?;

    let counts_after_replay = lyra_db::listens::get_counts(&db, &[track_db_id], user_db_id)?;
    assert_eq!(counts_after_replay.get(&track_db_id).copied(), Some(2));

    let records = collect_playback_records(&db)?;
    assert_eq!(records.len(), 2);
    Ok(())
}

#[tokio::test]
async fn report_playback_session_counts_a_seek_to_start_replay_once() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user_db_id = insert_user(&mut db, "alice")?;
    let track_db_id = insert_track(&mut db, "Track A", 200_000)?;

    report_test_playback_session_with_cleanup(
        &mut db,
        user_db_id,
        track_db_id,
        ActiveEvent::Started,
        PlaybackMutation {
            position_ms: Some(0),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        T0 + 1_000,
    )?;
    report_test_playback_session_with_cleanup(
        &mut db,
        user_db_id,
        track_db_id,
        ActiveEvent::Progress,
        PlaybackMutation {
            position_ms: Some(60_000),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        T0 + 61_000,
    )?;

    let counts_after_first = lyra_db::listens::get_counts(&db, &[track_db_id], user_db_id)?;
    assert_eq!(counts_after_first.get(&track_db_id).copied(), Some(1));

    // No Started event — pure seek-to-zero replay within the same session.
    report_test_playback_session_with_cleanup(
        &mut db,
        user_db_id,
        track_db_id,
        ActiveEvent::Progress,
        PlaybackMutation {
            position_ms: Some(1_000),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        T0 + 62_000,
    )?;
    report_test_playback_session_with_cleanup(
        &mut db,
        user_db_id,
        track_db_id,
        ActiveEvent::Progress,
        PlaybackMutation {
            position_ms: Some(60_000),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        T0 + 122_000,
    )?;

    // Without a started event or a new play id, it is the same play.
    let counts_after_replay = lyra_db::listens::get_counts(&db, &[track_db_id], user_db_id)?;
    assert_eq!(counts_after_replay.get(&track_db_id).copied(), Some(1));

    let records = collect_playback_records(&db)?;
    assert_eq!(records.len(), 1);
    Ok(())
}

#[tokio::test]
async fn report_playback_session_credits_two_listens_for_started_replay_via_previous_binding()
-> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user_db_id = insert_user(&mut db, "alice")?;
    let track_a_id = insert_track(&mut db, "Track A", 200_000)?;
    let track_b_id = insert_track(&mut db, "Track B", 200_000)?;

    let started_a = report_test_playback_session_with_cleanup(
        &mut db,
        user_db_id,
        track_a_id,
        ActiveEvent::Started,
        PlaybackMutation {
            position_ms: Some(0),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        T0 + 1_000,
    )?
    .playback
    .expect("track A should start");
    report_test_playback_session_with_cleanup(
        &mut db,
        user_db_id,
        track_a_id,
        ActiveEvent::Progress,
        PlaybackMutation {
            position_ms: Some(60_000),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        T0 + 61_000,
    )?;

    // Switch to B; A rotates to previous.
    report_test_playback_session_with_cleanup(
        &mut db,
        user_db_id,
        track_b_id,
        ActiveEvent::Started,
        PlaybackMutation {
            position_ms: Some(0),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        T0 + 62_000,
    )?;

    // Started for A from the previous slot must open a fresh session.
    let restarted_a = report_test_playback_session_with_cleanup(
        &mut db,
        user_db_id,
        track_a_id,
        ActiveEvent::Started,
        PlaybackMutation {
            position_ms: Some(0),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        T0 + 80_000,
    )?
    .playback
    .expect("restart should produce a record");
    assert_ne!(
        restarted_a.playback_session_id,
        started_a.playback_session_id
    );

    report_test_playback_session_with_cleanup(
        &mut db,
        user_db_id,
        track_a_id,
        ActiveEvent::Progress,
        PlaybackMutation {
            position_ms: Some(60_000),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        T0 + 140_000,
    )?;

    let counts = lyra_db::listens::get_counts(&db, &[track_a_id], user_db_id)?;
    assert_eq!(counts.get(&track_a_id).copied(), Some(2));
    Ok(())
}

#[tokio::test]
async fn report_playback_session_does_not_split_started_without_credited_listen()
-> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user_db_id = insert_user(&mut db, "alice")?;
    let track_db_id = insert_track(&mut db, "Track A", 200_000)?;

    let started = report_test_playback_session_with_cleanup(
        &mut db,
        user_db_id,
        track_db_id,
        ActiveEvent::Started,
        PlaybackMutation {
            position_ms: Some(0),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        1_000,
    )?
    .playback
    .expect("track should start");
    report_test_playback_session_with_cleanup(
        &mut db,
        user_db_id,
        track_db_id,
        ActiveEvent::Progress,
        PlaybackMutation {
            position_ms: Some(5_000),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        6_000,
    )?;

    // Duplicate Started before a credited listen must not split.
    let restarted = report_test_playback_session_with_cleanup(
        &mut db,
        user_db_id,
        track_db_id,
        ActiveEvent::Started,
        PlaybackMutation {
            position_ms: Some(0),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
        },
        7_000,
    )?
    .playback
    .expect("restart should produce a record");
    assert_eq!(restarted.playback_session_id, started.playback_session_id);

    let records = collect_playback_records(&db)?;
    assert_eq!(records.len(), 1);
    Ok(())
}
fn report_play(
    db: &mut DbAny,
    user_db_id: DbId,
    track_db_id: DbId,
    play_id: &str,
    position_ms: u64,
    now_ms: u64,
) -> anyhow::Result<PlaybackRecord> {
    report_playback_session_with_cleanup(
        db,
        ReportedPlaybackRequest {
            plugin_id: "external",
            user_db_id,
            session_key: "auth:1",
            track_db_id,
            play_id: Some(play_id),
            client_name: None,
            mutation: PlaybackMutation {
                position_ms: Some(position_ms),
                duration_ms: Some(200_000),
                state: Some(PlaybackState::Playing),
            },
            now_ms,
            active_event: ActiveEvent::Progress,
        },
    )?
    .playback
    .ok_or_else(|| anyhow::anyhow!("report should produce a playback"))
}

fn listen_for_play(db: &DbAny, user_db_id: DbId, play_id: &str) -> anyhow::Result<lyra_db::Listen> {
    let listen_db_id = lyra_db::listens::find_by_play_id(db, user_db_id, play_id)?
        .ok_or_else(|| anyhow::anyhow!("no listen for {play_id}"))?;
    lyra_db::graph::bulk_fetch_typed::<lyra_db::Listen>(db, vec![listen_db_id], "Listen")?
        .remove(&listen_db_id)
        .ok_or_else(|| anyhow::anyhow!("listen {listen_db_id:?} missing"))
}

#[tokio::test]
async fn a_new_play_id_starts_a_new_session_for_the_same_track() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let track = insert_track(&mut db, "A", 200_000)?;

    let first = report_play(&mut db, user, track, "first-play-000001", 0, T0)?;
    let first_again = report_play(
        &mut db,
        user,
        track,
        "first-play-000001",
        40_000,
        T0 + 40_000,
    )?;
    assert_eq!(first_again.playback_session_id, first.playback_session_id);

    let second = report_play(&mut db, user, track, "second-play-00001", 0, T0 + 50_000)?;
    assert_ne!(second.playback_session_id, first.playback_session_id);
    report_play(
        &mut db,
        user,
        track,
        "second-play-00001",
        35_000,
        T0 + 85_000,
    )?;

    let counts = lyra_db::listens::get_counts(&db, &[track], user)?;
    assert_eq!(counts.get(&track).copied(), Some(2));
    let first_listen = listen_for_play(&db, user, "first-play-000001")?;
    assert_eq!(first_listen.started_at_ms, T0);
    assert_eq!(first_listen.played_ms, 40_000);
    let second_listen = listen_for_play(&db, user, "second-play-00001")?;
    assert_eq!(second_listen.started_at_ms, T0 + 50_000);
    assert_eq!(second_listen.played_ms, 35_000);
    Ok(())
}

#[tokio::test]
async fn a_live_play_judges_against_the_catalog_duration() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let track = insert_track(&mut db, "Short", 60_000)?;

    // The client claims a 200 s duration; the 60 s catalog duration sets a 12 s threshold.
    report_play(&mut db, user, track, "short-play-000001", 0, T0)?;
    report_play(
        &mut db,
        user,
        track,
        "short-play-000001",
        12_000,
        T0 + 12_000,
    )?;

    assert_eq!(
        listen_for_play(&db, user, "short-play-000001")?.played_ms,
        12_000
    );
    Ok(())
}

#[tokio::test]
async fn a_play_reported_live_then_offline_counts_once() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let track = insert_track(&mut db, "A", 200_000)?;

    report_play(&mut db, user, track, "shared-play-00001", 0, T0)?;
    report_play(
        &mut db,
        user,
        track,
        "shared-play-00001",
        60_000,
        T0 + 60_000,
    )?;
    let offline = db.transaction_mut(|t| {
        crate::services::listens::record(
            t,
            user,
            crate::services::listens::Play {
                play_id: "shared-play-00001",
                track_db_id: track,
                started_at_ms: T0,
                played_ms: 200_000,
            },
            T0 + 300_000,
        )
    })?;

    assert_eq!(offline, crate::services::listens::RecordOutcome::Duplicate);
    let counts = lyra_db::listens::get_counts(&db, &[track], user)?;
    assert_eq!(counts.get(&track).copied(), Some(1));
    Ok(())
}

#[tokio::test]
async fn a_play_reported_offline_then_live_counts_once() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let track = insert_track(&mut db, "A", 200_000)?;

    let offline = db.transaction_mut(|t| {
        crate::services::listens::record(
            t,
            user,
            crate::services::listens::Play {
                play_id: "shared-play-00001",
                track_db_id: track,
                started_at_ms: T0,
                played_ms: 60_000,
            },
            T0 + 60_000,
        )
    })?;
    assert!(
        matches!(
            offline,
            crate::services::listens::RecordOutcome::Recorded(_)
        ),
        "{offline:?}"
    );
    report_play(
        &mut db,
        user,
        track,
        "shared-play-00001",
        60_000,
        T0 + 70_000,
    )?;
    report_play(
        &mut db,
        user,
        track,
        "shared-play-00001",
        120_000,
        T0 + 130_000,
    )?;

    let counts = lyra_db::listens::get_counts(&db, &[track], user)?;
    assert_eq!(counts.get(&track).copied(), Some(1));
    Ok(())
}

#[tokio::test]
async fn a_burst_of_reports_cannot_make_a_play_count() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let track = insert_track(&mut db, "A", 200_000)?;

    report_play(&mut db, user, track, "burst-play-000001", 0, T0)?;
    for report in 1..=20 {
        report_play(
            &mut db,
            user,
            track,
            "burst-play-000001",
            report * 10_000,
            T0 + report,
        )?;
    }

    assert!(lyra_db::listens::find_by_play_id(&db, user, "burst-play-000001")?.is_none());
    Ok(())
}
#[tokio::test]
async fn a_late_report_for_a_replaced_play_is_ignored() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let track = insert_track(&mut db, "A", 200_000)?;

    let first = report_play(&mut db, user, track, "first-play-000001", 0, T0)?;
    let second = report_play(&mut db, user, track, "second-play-00001", 0, T0 + 10_000)?;
    let late = report_playback_session_with_cleanup(
        &mut db,
        ReportedPlaybackRequest {
            plugin_id: "external",
            user_db_id: user,
            session_key: "auth:1",
            track_db_id: track,
            play_id: Some("first-play-000001"),
            client_name: None,
            mutation: PlaybackMutation {
                position_ms: Some(5_000),
                duration_ms: Some(200_000),
                state: Some(PlaybackState::Playing),
            },
            // After the previous slot's grace, so the late report can't land there.
            now_ms: T0 + 60_000,
            active_event: ActiveEvent::Progress,
        },
    )?;
    assert!(late.playback.is_none());

    let continued = report_play(
        &mut db,
        user,
        track,
        "second-play-00001",
        40_000,
        T0 + 50_000,
    )?;
    assert_eq!(continued.playback_session_id, second.playback_session_id);
    assert_ne!(continued.playback_session_id, first.playback_session_id);
    assert_eq!(
        listen_for_play(&db, user, "second-play-00001")?.started_at_ms,
        T0 + 10_000
    );
    assert_eq!(collect_playback_records(&db)?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn a_live_listen_keeps_the_plays_full_playing_time() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let track = insert_track(&mut db, "A", 200_000)?;

    report_play(&mut db, user, track, "long-play-0000001", 0, T0)?;
    report_play(
        &mut db,
        user,
        track,
        "long-play-0000001",
        30_000,
        T0 + 30_000,
    )?;
    assert_eq!(
        listen_for_play(&db, user, "long-play-0000001")?.played_ms,
        30_000
    );
    report_play(
        &mut db,
        user,
        track,
        "long-play-0000001",
        150_000,
        T0 + 150_000,
    )?;

    assert_eq!(
        listen_for_play(&db, user, "long-play-0000001")?.played_ms,
        150_000
    );
    let counts = lyra_db::listens::get_counts(&db, &[track], user)?;
    assert_eq!(counts.get(&track).copied(), Some(1));
    Ok(())
}
#[tokio::test]
async fn a_named_play_resumes_its_own_stopped_session() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let track = insert_track(&mut db, "A", 200_000)?;
    let report = |db: &mut DbAny, position_ms: u64, state: PlaybackState, now_ms: u64| {
        report_playback_session_with_cleanup(
            db,
            ReportedPlaybackRequest {
                plugin_id: "external",
                user_db_id: user,
                session_key: "auth:1",
                track_db_id: track,
                play_id: Some("resumed-play-00001"),
                client_name: None,
                mutation: PlaybackMutation {
                    position_ms: Some(position_ms),
                    duration_ms: Some(200_000),
                    state: Some(state),
                },
                now_ms,
                active_event: ActiveEvent::Progress,
            },
        )
    };

    let started = report(&mut db, 0, PlaybackState::Playing, T0)?
        .playback
        .expect("the play starts");
    report(&mut db, 20_000, PlaybackState::Stopped, T0 + 20_000)?;
    let resumed = report(&mut db, 20_000, PlaybackState::Playing, T0 + 60_000)?
        .playback
        .expect("the play resumes");
    assert_eq!(resumed.playback_session_id, started.playback_session_id);
    assert_eq!(resumed.playback.state, PlaybackState::Playing);
    report(&mut db, 40_000, PlaybackState::Playing, T0 + 80_000)?;

    assert_eq!(
        listen_for_play(&db, user, "resumed-play-00001")?.played_ms,
        40_000
    );
    Ok(())
}
#[tokio::test]
async fn a_live_listen_is_announced_once_when_it_counts() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let dispatch = crate::services::testing::record_plugin_dispatch();
    let user = insert_user(&mut db, "alice")?;
    let track = insert_track(&mut db, "A", 200_000)?;
    let track_public_id = lyra_db::lookup::find_id_by_db_id(&db, track)?.expect("track id");

    report_play(&mut db, user, track, "announced-play-001", 0, T0)?;
    assert!(dispatch.listen_updates().is_empty());
    report_play(
        &mut db,
        user,
        track,
        "announced-play-001",
        30_000,
        T0 + 30_000,
    )?;
    // Later reports only raise the stored playing time.
    report_play(
        &mut db,
        user,
        track,
        "announced-play-001",
        60_000,
        T0 + 60_000,
    )?;

    let listen = listen_for_play(&db, user, "announced-play-001")?;
    assert_eq!(listen.played_ms, 60_000);
    assert_eq!(
        dispatch.listen_updates(),
        vec![crate::services::listens::ListenUpdatePayload {
            event: "recorded".to_string(),
            user_id: user_public_id(&db, user),
            listens: vec![crate::services::listens::ListenPayload {
                listen_id: listen.id,
                track_id: track_public_id,
                started_at_ms: T0,
                played_ms: 30_000,
            }],
        }]
    );
    Ok(())
}
