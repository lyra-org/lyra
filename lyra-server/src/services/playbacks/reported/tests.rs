// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    DbId,
    QueryBuilder,
};
use lyra_db::nanoid;
use lyra_db::{
    PlaybackState,
    test_db::new_test_db,
};

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
use lyra_db::roots::{
    self,
    Root,
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
        id: nanoid(),
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
        id: nanoid(),
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
    roots::link(db, Root::Tracks, track_db_id)?;
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
async fn reported_playback_keeps_its_root_across_switch_late_stop_and_resume() -> anyhow::Result<()>
{
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let track_a = insert_track(&mut db, "A", 200_000)?;
    let track_b = insert_track(&mut db, "B", 200_000)?;
    let playing = || PlaybackMutation {
        state: Some(PlaybackState::Playing),
        duration_ms: Some(200_000),
        position_ms: Some(0),
        ..PlaybackMutation::default()
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
    assert_eq!(
        restarted.playback_session_public_id, b.playback_session_public_id,
        "a report after the stop resumes the play; only a start begins a new one"
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
        assert_eq!(
            resumed.playback_session_public_id, prior.playback_session_public_id,
            "a report after the stop resumes the same play"
        );
        assert_eq!(resumed.playback.play_id, prior.playback.play_id);
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
            ..PlaybackMutation::default()
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
    let lapsed = credited.playback.live_until_ms + 1;
    cleanup_evicted_playbacks(&mut db, lapsed)?;
    assert!(lyra_db::playbacks::get_reported(&db, user, "external", "auth:1", lapsed)?.is_none());
    report_test_playback_session(
        &mut db,
        user,
        track,
        ActiveEvent::Progress,
        PlaybackMutation {
            position_ms: Some(0),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
            ..PlaybackMutation::default()
        },
        lapsed + 1_000,
    )?;
    let fresh =
        lyra_db::playbacks::get_reported(&db, user, "external", "auth:1", lapsed + 1_000)?.unwrap();
    assert_ne!(fresh.id, root.id);
    cleanup_evicted_playbacks(&mut db, lapsed + 400_000)?;
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
                ..PlaybackMutation::default()
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
                ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
        },
        3_000,
    )?;
    assert!(ignored.is_none());

    let persisted = lyra_db::playback_sessions::get_by_id(&db, started_a.playback_session_id)?
        .expect("playback should exist");
    assert_eq!(
        persisted.position_ms, 1_000,
        "the replacement credited the second A played; the displaced progress is ignored"
    );
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
        },
        2_000 + PREVIOUS_PLAYBACK_GRACE_MS + 1,
    )?;
    assert!(ignored.is_none());

    let persisted = lyra_db::playback_sessions::get_by_id(&db, started_a.playback_session_id)?
        .expect("playback should exist");
    assert_eq!(
        persisted.state,
        PlaybackState::Paused,
        "paused at its replacement, and the expired stop doesn't end it"
    );
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
            ..PlaybackMutation::default()
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
                ..PlaybackMutation::default()
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
    assert_eq!(
        first_listen.played_ms,
        Some(50_000),
        "the second play's start credits the first the 10 s since its last report"
    );
    let second_listen = listen_for_play(&db, user, "second-play-00001")?;
    assert_eq!(second_listen.started_at_ms, T0 + 50_000);
    assert_eq!(second_listen.played_ms, Some(35_000));
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
        Some(12_000)
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
                ..PlaybackMutation::default()
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
        Some(30_000)
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
        Some(150_000)
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
                    ..PlaybackMutation::default()
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
        Some(40_000)
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
    assert_eq!(listen.played_ms, Some(60_000));
    assert_eq!(
        dispatch.listen_updates(),
        vec![crate::services::listens::ListenUpdatePayload {
            event: "recorded".to_string(),
            user_id: user_public_id(&db, user),
            listens: vec![crate::services::listens::ListenPayload {
                listen_id: listen.id,
                track_id: track_public_id,
                started_at_ms: T0,
                played_ms: Some(30_000),
            }],
        }]
    );
    Ok(())
}

fn report_for(
    db: &mut DbAny,
    plugin_id: &str,
    user_db_id: DbId,
    track_db_id: DbId,
    position_ms: u64,
    now_ms: u64,
) -> anyhow::Result<PlaybackRecord> {
    report_playback_session(
        db,
        ReportedPlaybackRequest {
            plugin_id,
            user_db_id,
            session_key: "device:1",
            track_db_id,
            play_id: None,
            client_name: None,
            mutation: PlaybackMutation {
                position_ms: Some(position_ms),
                duration_ms: Some(200_000),
                state: Some(PlaybackState::Playing),
                ..PlaybackMutation::default()
            },
            now_ms,
            active_event: ActiveEvent::Started,
        },
    )?
    .ok_or_else(|| anyhow::anyhow!("the report starts a session"))
}

/// Claims the user's `device:1` session, as the plugin reports it, for a new socket.
fn claim_device(db: &DbAny, plugin_id: &str, user_db_id: DbId) -> socket_claims::SocketId {
    let socket = socket_claims::SocketId::next();
    assert!(socket_claims::claim(
        socket,
        ClaimedSession {
            plugin_id: plugin_id.to_string(),
            user_public_id: user_public_id(db, user_db_id),
            session_key: "device:1".to_string(),
        },
        || true,
    ));
    socket
}

/// Closes the socket and pauses what its close leaves playing, returning the paused sessions.
fn close_socket(
    db: &mut DbAny,
    socket: socket_claims::SocketId,
    now_ms: u64,
) -> Result<(Vec<PlaybackRecord>, Vec<EvictedPlaybackRecord>), PlaybackError> {
    pause_released_sessions(db, socket_claims::release_socket(socket, || {}), now_ms)
}

#[tokio::test]
async fn a_close_pauses_the_current_session_after_a_replacement_paused_the_previous()
-> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let a = insert_track(&mut db, "A", 200_000)?;
    let b = insert_track(&mut db, "B", 200_000)?;
    let socket = claim_device(&db, "demo", user);
    let started_a = report_for(&mut db, "demo", user, a, 10_000, 1_000)?;
    let started_b = report_for(&mut db, "demo", user, b, 20_000, 2_000)?;

    let (paused, evicted) = close_socket(&mut db, socket, 5_000)?;

    assert!(evicted.is_empty());
    let state = |session: &lyra_db::PlaybackSession| {
        (
            session.state,
            session.position_ms,
            session.played_ms,
            session.updated_at_ms,
        )
    };
    assert_eq!(
        state(&stored(&db, &started_a)?),
        (PlaybackState::Paused, 11_000, 1_000, 2_000),
        "B's start credited A the second it played since its report, and paused it"
    );
    assert_eq!(paused.len(), 1, "only the current session was left playing");
    assert_eq!(paused[0].playback_session_id, started_b.playback_session_id);
    assert_eq!(
        state(&paused[0].playback),
        (PlaybackState::Paused, 20_000, 0, 5_000)
    );
    let root = lyra_db::playbacks::get_reported(&db, user, "demo", "device:1", 5_000)?
        .expect("the playback stays for the player to resume");
    assert_eq!(root.updated_at_ms, 2_000);
    assert_eq!(
        lyra_db::playbacks::get_current_session_id(&db, root.db_id.unwrap())?,
        Some(started_b.playback_session_id)
    );

    let resumed = report_playback_session(
        &mut db,
        ReportedPlaybackRequest {
            plugin_id: "demo",
            user_db_id: user,
            session_key: "device:1",
            track_db_id: b,
            play_id: None,
            client_name: None,
            mutation: PlaybackMutation {
                state: Some(PlaybackState::Playing),
                ..Default::default()
            },
            now_ms: 6_000,
            active_event: ActiveEvent::Progress,
        },
    )?
    .expect("the player resumes");
    assert_eq!(resumed.playback_session_id, started_b.playback_session_id);
    assert_eq!(resumed.playback.state, PlaybackState::Playing);
    Ok(())
}

#[tokio::test]
async fn a_close_leaves_other_reporters_of_the_session_key_playing() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let other_user = insert_user(&mut db, "bob")?;
    let track = insert_track(&mut db, "A", 200_000)?;
    let own = report_for(&mut db, "demo", user, track, 0, 1_000)?;
    let other_plugin = report_for(&mut db, "other", user, track, 0, 1_000)?;
    let other_user_session = report_for(&mut db, "demo", other_user, track, 0, 1_000)?;
    let socket = claim_device(&db, "demo", user);

    let (paused, _) = close_socket(&mut db, socket, 2_000)?;

    assert_eq!(
        paused
            .iter()
            .map(|paused| paused.playback_session_id)
            .collect::<Vec<_>>(),
        [own.playback_session_id]
    );
    for untouched in [other_plugin, other_user_session] {
        let session =
            lyra_db::playback_sessions::get_by_id(&db, untouched.playback_session_id)?.unwrap();
        assert_eq!(session.state, PlaybackState::Playing);
    }
    let socket = claim_device(&db, "demo", user);
    let (paused, _) = close_socket(&mut db, socket, 3_000)?;
    assert!(paused.is_empty(), "a paused session stays paused");
    Ok(())
}

#[tokio::test]
async fn a_close_leaves_a_playback_a_later_report_changed() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let a = insert_track(&mut db, "A", 200_000)?;
    let b = insert_track(&mut db, "B", 200_000)?;
    let socket = claim_device(&db, "demo", user);
    report_for(&mut db, "demo", user, a, 0, 1_000)?;
    let released = socket_claims::release_socket(socket, || {});
    // The replacement lands in the same millisecond, so only the report count tells them apart.
    let replacement = report_for(&mut db, "demo", user, b, 0, 1_000)?;

    let (paused, _) = pause_released_sessions(&mut db, released, 1_000)?;

    assert!(
        paused.is_empty(),
        "the socket closed before the later report"
    );
    let session =
        lyra_db::playback_sessions::get_by_id(&db, replacement.playback_session_id)?.unwrap();
    assert_eq!(session.state, PlaybackState::Playing);
    Ok(())
}

#[tokio::test]
async fn a_close_leaves_a_resume_in_the_same_millisecond_playing() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let track = insert_track(&mut db, "A", 200_000)?;
    let socket = claim_device(&db, "demo", user);
    let started = report_for(&mut db, "demo", user, track, 0, 1_000)?;
    let progress = |db: &mut DbAny, state| {
        report_playback_session(
            db,
            ReportedPlaybackRequest {
                plugin_id: "demo",
                user_db_id: user,
                session_key: "device:1",
                track_db_id: track,
                play_id: None,
                client_name: None,
                mutation: PlaybackMutation {
                    state: Some(state),
                    ..Default::default()
                },
                now_ms: 2_000,
                active_event: ActiveEvent::Progress,
            },
        )
    };
    progress(&mut db, PlaybackState::Paused)?.expect("the pause is accepted");
    let released = socket_claims::release_socket(socket, || {});
    progress(&mut db, PlaybackState::Playing)?.expect("the resume is accepted");

    let (paused, _) = pause_released_sessions(&mut db, released, 2_000)?;

    assert!(paused.is_empty(), "the resume came after the socket closed");
    let session = lyra_db::playback_sessions::get_by_id(&db, started.playback_session_id)?.unwrap();
    assert_eq!(session.state, PlaybackState::Playing);
    Ok(())
}

#[tokio::test]
async fn a_close_leaves_a_session_another_socket_claims_playing() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let track = insert_track(&mut db, "A", 200_000)?;
    let first = claim_device(&db, "demo", user);
    let second = claim_device(&db, "demo", user);
    let started = report_for(&mut db, "demo", user, track, 0, 1_000)?;

    let (paused, _) = close_socket(&mut db, first, 2_000)?;
    assert!(
        paused.is_empty(),
        "the other socket still claims the session"
    );

    let (paused, _) = close_socket(&mut db, second, 3_000)?;
    assert_eq!(
        paused
            .iter()
            .map(|paused| paused.playback_session_id)
            .collect::<Vec<_>>(),
        [started.playback_session_id]
    );
    Ok(())
}

fn report_at(
    db: &mut DbAny,
    user_db_id: DbId,
    track_db_id: DbId,
    active_event: ActiveEvent,
    state: PlaybackState,
    position_ms: u64,
    now_ms: u64,
) -> anyhow::Result<Option<PlaybackRecord>> {
    report_test_playback_session(
        db,
        user_db_id,
        track_db_id,
        active_event,
        PlaybackMutation {
            position_ms: Some(position_ms),
            duration_ms: lyra_db::tracks::get_by_id(db, track_db_id)?.and_then(|t| t.duration_ms),
            state: Some(state),
            ..PlaybackMutation::default()
        },
        now_ms,
    )
}

fn stored(db: &DbAny, record: &PlaybackRecord) -> anyhow::Result<lyra_db::PlaybackSession> {
    lyra_db::playback_sessions::get_by_id(db, record.playback_session_id)?
        .ok_or_else(|| anyhow::anyhow!("the session is kept"))
}

fn listen_count(db: &DbAny, user_db_id: DbId, track_db_id: DbId) -> anyhow::Result<u64> {
    Ok(
        lyra_db::listens::get_counts(db, &[track_db_id], user_db_id)?
            .get(&track_db_id)
            .copied()
            .unwrap_or(0),
    )
}

#[tokio::test]
async fn a_reported_position_past_the_duration_reads_as_the_end() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let track = insert_track(&mut db, "A", 200_000)?;
    report_at(
        &mut db,
        user,
        track,
        ActiveEvent::Started,
        PlaybackState::Playing,
        0,
        T0,
    )?;
    report_at(
        &mut db,
        user,
        track,
        ActiveEvent::Progress,
        PlaybackState::Playing,
        150_000,
        T0 + 150_000,
    )?;

    let ended = report_at(
        &mut db,
        user,
        track,
        ActiveEvent::Progress,
        PlaybackState::Completed,
        200_040,
        T0 + 200_040,
    )?
    .expect("the overshooting stop applies");

    assert_eq!(ended.playback.state, PlaybackState::Completed);
    assert_eq!(ended.playback.position_ms, 200_000);
    assert_eq!(ended.playback.played_ms, 200_000);

    let other = insert_track(&mut db, "B", 200_000)?;
    let started = report_at(
        &mut db,
        user,
        other,
        ActiveEvent::Started,
        PlaybackState::Playing,
        200_500,
        T0 + 300_000,
    )?
    .expect("an overshooting start applies");
    assert_eq!(started.playback.position_ms, 200_000);
    Ok(())
}

#[tokio::test]
async fn a_replaced_play_is_credited_since_its_last_report_and_counts() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let dispatch = crate::services::testing::record_plugin_dispatch();
    let user = insert_user(&mut db, "alice")?;
    let short = insert_track(&mut db, "Short", 60_000)?;
    let next = insert_track(&mut db, "Next", 200_000)?;
    let a = report_at(
        &mut db,
        user,
        short,
        ActiveEvent::Started,
        PlaybackState::Playing,
        0,
        T0,
    )?
    .expect("A starts");

    let Some(ReportOutcome::Applied(update)) = Some(report_reported_playback(
        &mut db,
        ReportedPlaybackRequest {
            plugin_id: "external",
            user_db_id: user,
            session_key: "auth:1",
            track_db_id: next,
            play_id: None,
            client_name: None,
            mutation: PlaybackMutation {
                position_ms: Some(0),
                duration_ms: Some(200_000),
                state: Some(PlaybackState::Playing),
                ..PlaybackMutation::default()
            },
            now_ms: T0 + 20_000,
            active_event: ActiveEvent::Started,
        },
    )?) else {
        anyhow::bail!("B's start applies");
    };

    assert!(
        update.displaced_session.is_none(),
        "only the credited play's listen is announced, not its session"
    );
    let a_now = stored(&db, &a)?;
    assert_eq!(a_now.played_ms, 20_000);
    assert_eq!(a_now.position_ms, 20_000);
    assert_eq!(
        a_now.state,
        PlaybackState::Paused,
        "paused at its replacement"
    );
    let listen = listen_for_play(&db, user, &a.playback.play_id)?;
    assert_eq!(listen.played_ms, Some(20_000));
    assert_eq!(
        dispatch.listen_updates().len(),
        1,
        "the listen is announced"
    );
    Ok(())
}

#[tokio::test]
async fn a_replaced_play_is_credited_at_most_to_its_tracks_end() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let short = insert_track(&mut db, "Short", 60_000)?;
    let next = insert_track(&mut db, "Next", 200_000)?;
    let a = report_at(
        &mut db,
        user,
        short,
        ActiveEvent::Started,
        PlaybackState::Playing,
        0,
        T0,
    )?
    .expect("A starts");
    report_at(
        &mut db,
        user,
        short,
        ActiveEvent::Progress,
        PlaybackState::Playing,
        50_000,
        T0 + 50_000,
    )?;
    report_at(
        &mut db,
        user,
        next,
        ActiveEvent::Started,
        PlaybackState::Playing,
        0,
        T0 + 100_000,
    )?;

    let a_now = stored(&db, &a)?;
    assert_eq!(a_now.played_ms, 60_000);
    assert_eq!(a_now.position_ms, 60_000);
    Ok(())
}

#[tokio::test]
async fn a_replaced_play_not_playing_is_not_credited() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let a_track = insert_track(&mut db, "A", 200_000)?;
    let b_track = insert_track(&mut db, "B", 200_000)?;
    let a = report_at(
        &mut db,
        user,
        a_track,
        ActiveEvent::Started,
        PlaybackState::Playing,
        0,
        T0,
    )?
    .expect("A starts");
    report_at(
        &mut db,
        user,
        a_track,
        ActiveEvent::Progress,
        PlaybackState::Paused,
        10_000,
        T0 + 10_000,
    )?;
    report_at(
        &mut db,
        user,
        b_track,
        ActiveEvent::Started,
        PlaybackState::Playing,
        0,
        T0 + 60_000,
    )?;

    assert_eq!(stored(&db, &a)?.played_ms, 10_000);
    Ok(())
}

#[tokio::test]
async fn a_replaced_play_moves_at_its_rate_and_is_credited_real_time() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let a_track = insert_track(&mut db, "A", 200_000)?;
    let b_track = insert_track(&mut db, "B", 200_000)?;
    let a = report_test_playback_session(
        &mut db,
        user,
        a_track,
        ActiveEvent::Started,
        PlaybackMutation {
            position_ms: Some(0),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
            rate_permille: Some(2_000),
            counts_as_listen: None,
        },
        T0,
    )?
    .expect("A starts");
    report_at(
        &mut db,
        user,
        b_track,
        ActiveEvent::Started,
        PlaybackState::Playing,
        0,
        T0 + 40_000,
    )?;

    let a_now = stored(&db, &a)?;
    assert_eq!(a_now.position_ms, 80_000);
    assert_eq!(a_now.played_ms, 40_000);
    assert_eq!(a_now.latency_allowance_used_ms, 0);
    assert_eq!(
        listen_for_play(&db, user, &a.playback.play_id)?.played_ms,
        Some(40_000)
    );
    Ok(())
}

#[tokio::test]
async fn a_replaced_buffering_play_pauses_without_credit() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let a_track = insert_track(&mut db, "A", 200_000)?;
    let b_track = insert_track(&mut db, "B", 200_000)?;
    let a = report_at(
        &mut db,
        user,
        a_track,
        ActiveEvent::Started,
        PlaybackState::Playing,
        0,
        T0,
    )?
    .expect("A starts");
    report_at(
        &mut db,
        user,
        a_track,
        ActiveEvent::Progress,
        PlaybackState::Buffering,
        10_000,
        T0 + 10_000,
    )?;
    report_at(
        &mut db,
        user,
        b_track,
        ActiveEvent::Started,
        PlaybackState::Playing,
        0,
        T0 + 60_000,
    )?;

    let a_now = stored(&db, &a)?;
    assert_eq!(a_now.state, PlaybackState::Paused);
    assert_eq!(a_now.played_ms, 10_000);
    assert_eq!(a_now.position_ms, 10_000);
    Ok(())
}

#[tokio::test]
async fn a_late_stop_of_a_credited_replaced_play_adds_no_time_twice() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let a_track = insert_track(&mut db, "A", 200_000)?;
    let b_track = insert_track(&mut db, "B", 200_000)?;
    let a = report_at(
        &mut db,
        user,
        a_track,
        ActiveEvent::Started,
        PlaybackState::Playing,
        0,
        T0,
    )?
    .expect("A starts");
    report_at(
        &mut db,
        user,
        a_track,
        ActiveEvent::Progress,
        PlaybackState::Playing,
        60_000,
        T0 + 60_000,
    )?;
    report_at(
        &mut db,
        user,
        b_track,
        ActiveEvent::Started,
        PlaybackState::Playing,
        0,
        T0 + 100_000,
    )?;
    assert_eq!(stored(&db, &a)?.played_ms, 100_000);

    let stopped = report_at(
        &mut db,
        user,
        a_track,
        ActiveEvent::Progress,
        PlaybackState::Stopped,
        100_000,
        T0 + 101_000,
    )?
    .expect("the late stop lands on the replaced play");

    assert_eq!(stopped.playback_session_id, a.playback_session_id);
    assert_eq!(stopped.playback.state, PlaybackState::Stopped);
    assert_eq!(stopped.playback.played_ms, 100_000);
    assert_eq!(
        listen_for_play(&db, user, &a.playback.play_id)?.played_ms,
        Some(100_000)
    );
    Ok(())
}

#[tokio::test]
async fn a_lapsed_play_is_credited_up_to_its_tracks_end_and_counts() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let dispatch = crate::services::testing::record_plugin_dispatch();
    let user = insert_user(&mut db, "alice")?;
    let short = insert_track(&mut db, "Short", 60_000)?;
    let a = report_at(
        &mut db,
        user,
        short,
        ActiveEvent::Started,
        PlaybackState::Playing,
        0,
        T0,
    )?
    .expect("A starts");
    report_at(
        &mut db,
        user,
        short,
        ActiveEvent::Progress,
        PlaybackState::Playing,
        10_000,
        T0 + 10_000,
    )?;
    assert_eq!(listen_count(&db, user, short)?, 0);

    let lapsed_at = stored(&db, &a)?.live_until_ms;
    let later = lapsed_at + 1;
    let other = insert_track(&mut db, "Other", 200_000)?;
    report_at(
        &mut db,
        user,
        other,
        ActiveEvent::Started,
        PlaybackState::Playing,
        0,
        later,
    )?;

    let a_now = stored(&db, &a)?;
    assert_eq!(a_now.played_ms, 60_000);
    assert_eq!(a_now.position_ms, 60_000);
    assert_eq!(
        a_now.updated_at_ms, lapsed_at,
        "the credit is dated to the lapse, not the sweep"
    );
    assert_eq!(listen_count(&db, user, short)?, 1);
    assert_eq!(
        dispatch.listen_updates().len(),
        1,
        "the listen is announced"
    );
    Ok(())
}

#[tokio::test]
async fn the_sweep_credits_a_lapsed_play_once() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let short = insert_track(&mut db, "Short", 60_000)?;
    let a = report_at(
        &mut db,
        user,
        short,
        ActiveEvent::Started,
        PlaybackState::Playing,
        0,
        T0,
    )?
    .expect("A starts");

    let lapsed = stored(&db, &a)?.live_until_ms + 1;
    crate::services::playbacks::expire_all_lapsed(&mut db, lapsed - 1)?;
    assert_eq!(
        stored(&db, &a)?.played_ms,
        0,
        "still live, it isn't credited"
    );
    crate::services::playbacks::expire_all_lapsed(&mut db, lapsed)?;
    assert_eq!(stored(&db, &a)?.played_ms, 60_000);
    assert_eq!(listen_count(&db, user, short)?, 1);

    crate::services::playbacks::expire_all_lapsed(&mut db, lapsed + 1_000)?;
    assert_eq!(stored(&db, &a)?.played_ms, 60_000);
    Ok(())
}

#[tokio::test]
async fn a_long_play_stays_active_unreported_and_lapses_after_its_planned_end() -> anyhow::Result<()>
{
    const MINUTE_MS: u64 = 60 * 1000;
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let long = insert_track(&mut db, "Long", 20 * MINUTE_MS)?;
    let a = report_at(
        &mut db,
        user,
        long,
        ActiveEvent::Started,
        PlaybackState::Playing,
        2 * MINUTE_MS,
        T0,
    )?
    .expect("A starts");
    let planned_end = T0 + 18 * MINUTE_MS;
    assert_eq!(stored(&db, &a)?.live_until_ms, planned_end + 5 * MINUTE_MS);

    let owner = lyra_db::users::get_by_id(&db, user)?.expect("user");
    let viewer = crate::services::auth::Principal::for_user(
        &db,
        user,
        vec![lyra_db::Permission::Admin],
        std::collections::HashSet::new(),
    );
    let active = |db: &DbAny, now_ms| {
        crate::services::playbacks::list_active(
            db,
            &viewer,
            std::slice::from_ref(&owner),
            false,
            now_ms,
        )
    };

    let unreported = T0 + 6 * MINUTE_MS;
    crate::services::playbacks::expire_all_lapsed(&mut db, unreported)?;
    let listed = active(&db, unreported)?;
    assert_eq!(listed.len(), 1, "six minutes unreported, it still plays");
    assert_eq!(
        crate::services::playback_sessions::current_position_ms(&listed[0].session, unreported),
        8 * MINUTE_MS
    );
    let last_live = planned_end + 5 * MINUTE_MS;
    crate::services::playbacks::expire_all_lapsed(&mut db, last_live)?;
    assert_eq!(active(&db, last_live)?.len(), 1);
    assert_eq!(stored(&db, &a)?.played_ms, 0);

    crate::services::playbacks::expire_all_lapsed(&mut db, last_live + 1)?;
    assert!(active(&db, last_live + 1)?.is_empty());
    let credited = stored(&db, &a)?;
    assert_eq!(
        credited.state,
        PlaybackState::Stopped,
        "settled at its lapse"
    );
    assert!(
        !credited.is_live(last_live),
        "an orphaned play is not live again"
    );
    crate::services::playbacks::expire_all_lapsed(&mut db, last_live + 2)?;
    assert_eq!(stored(&db, &a)?.played_ms, 18 * MINUTE_MS, "credited once");
    assert_eq!(
        credited.played_ms,
        18 * MINUTE_MS,
        "credited up to its lapse, at most what was left of the track"
    );
    assert_eq!(credited.position_ms, 20 * MINUTE_MS);
    assert_eq!(credited.updated_at_ms, last_live, "dated to the lapse");
    assert_eq!(listen_count(&db, user, long)?, 1);
    Ok(())
}

#[tokio::test]
async fn a_paused_play_keeps_its_context_for_thirty_minutes() -> anyhow::Result<()> {
    const MINUTE_MS: u64 = 60 * 1000;
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let track = insert_track(&mut db, "Track", 4 * MINUTE_MS)?;
    let a = report_at(
        &mut db,
        user,
        track,
        ActiveEvent::Started,
        PlaybackState::Playing,
        0,
        T0,
    )?
    .expect("A starts");
    report_at(
        &mut db,
        user,
        track,
        ActiveEvent::Progress,
        PlaybackState::Paused,
        MINUTE_MS,
        T0 + MINUTE_MS,
    )?;
    let reported = |db: &DbAny, now_ms| {
        lyra_db::playbacks::get_reported(db, user, "external", "auth:1", now_ms)
            .map(|found| found.is_some())
    };
    let last_live = T0 + 31 * MINUTE_MS;
    crate::services::playbacks::expire_all_lapsed(&mut db, last_live)?;
    assert!(reported(&db, last_live)?, "paused for thirty minutes");
    crate::services::playbacks::expire_all_lapsed(&mut db, last_live + 1)?;
    assert!(!reported(&db, last_live + 1)?);
    assert_eq!(
        stored(&db, &a)?.played_ms,
        MINUTE_MS,
        "a paused play earns nothing more"
    );
    Ok(())
}

fn report_named(
    db: &mut DbAny,
    user_db_id: DbId,
    track_db_id: DbId,
    play_id: &str,
    position_ms: Option<u64>,
    now_ms: u64,
) -> anyhow::Result<Option<PlaybackRecord>> {
    report_playback_session(
        db,
        ReportedPlaybackRequest {
            plugin_id: "external",
            user_db_id,
            session_key: "auth:1",
            track_db_id,
            play_id: Some(play_id),
            client_name: None,
            mutation: PlaybackMutation {
                position_ms,
                duration_ms: Some(200_000),
                state: Some(PlaybackState::Playing),
                ..PlaybackMutation::default()
            },
            now_ms,
            active_event: ActiveEvent::Started,
        },
    )
}

#[tokio::test]
async fn a_promoted_play_accrues_nothing_while_it_was_replaced() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let a_track = insert_track(&mut db, "A", 200_000)?;
    let b_track = insert_track(&mut db, "B", 200_000)?;
    let c_track = insert_track(&mut db, "C", 200_000)?;
    let a =
        report_named(&mut db, user, a_track, "promoted-play-a-01", Some(0), T0)?.expect("A starts");
    report_named(
        &mut db,
        user,
        b_track,
        "promoted-play-b-01",
        Some(0),
        T0 + 10_000,
    )?;
    assert_eq!(stored(&db, &a)?.played_ms, 10_000);

    let promoted = report_named(
        &mut db,
        user,
        a_track,
        "promoted-play-a-01",
        None,
        T0 + 30_000,
    )?
    .expect("A's start brings it back");
    assert_eq!(promoted.playback_session_id, a.playback_session_id);
    assert_eq!(promoted.playback.played_ms, 10_000);

    report_named(
        &mut db,
        user,
        c_track,
        "promoted-play-c-01",
        Some(0),
        T0 + 40_000,
    )?;
    assert_eq!(
        stored(&db, &a)?.played_ms,
        20_000,
        "A played 0-10 s and 30-40 s, not the 20 s B played"
    );
    Ok(())
}

#[tokio::test]
async fn a_promoted_play_with_a_position_accrues_nothing_while_it_was_replaced()
-> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let a_track = insert_track(&mut db, "A", 200_000)?;
    let b_track = insert_track(&mut db, "B", 200_000)?;
    let a =
        report_named(&mut db, user, a_track, "promoted-play-a-02", Some(0), T0)?.expect("A starts");
    report_named(
        &mut db,
        user,
        b_track,
        "promoted-play-b-02",
        Some(0),
        T0 + 10_000,
    )?;

    let promoted = report_named(
        &mut db,
        user,
        a_track,
        "promoted-play-a-02",
        Some(40_000),
        T0 + 30_000,
    )?
    .expect("A's start brings it back");

    assert_eq!(promoted.playback_session_id, a.playback_session_id);
    assert_eq!(promoted.playback.played_ms, 10_000);
    assert_eq!(promoted.playback.position_ms, 40_000);
    Ok(())
}

#[tokio::test]
async fn a_late_stop_past_the_credited_position_adds_nothing_after_the_replacement()
-> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let a_track = insert_track(&mut db, "A", 200_000)?;
    let b_track = insert_track(&mut db, "B", 200_000)?;
    let a = report_at(
        &mut db,
        user,
        a_track,
        ActiveEvent::Started,
        PlaybackState::Playing,
        0,
        T0,
    )?
    .expect("A starts");
    report_at(
        &mut db,
        user,
        b_track,
        ActiveEvent::Started,
        PlaybackState::Playing,
        0,
        T0 + 100_000,
    )?;

    let stopped = report_at(
        &mut db,
        user,
        a_track,
        ActiveEvent::Progress,
        PlaybackState::Stopped,
        103_000,
        T0 + 101_000,
    )?
    .expect("the late stop lands on the replaced play");

    assert_eq!(stopped.playback_session_id, a.playback_session_id);
    assert_eq!(stopped.playback.position_ms, 103_000);
    assert_eq!(
        stopped.playback.played_ms, 100_000,
        "B was playing after the replacement"
    );
    Ok(())
}

#[tokio::test]
async fn a_replaced_play_of_unknown_duration_gets_nothing_from_a_late_stop() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let a_track = insert_track(&mut db, "A", 200_000)?;
    let b_track = insert_track(&mut db, "B", 200_000)?;
    let unknown_duration = |position_ms, state| PlaybackMutation {
        position_ms: Some(position_ms),
        duration_ms: None,
        state: Some(state),
        ..PlaybackMutation::default()
    };
    let a = report_test_playback_session(
        &mut db,
        user,
        a_track,
        ActiveEvent::Started,
        unknown_duration(0, PlaybackState::Playing),
        T0,
    )?
    .expect("A starts");
    report_at(
        &mut db,
        user,
        b_track,
        ActiveEvent::Started,
        PlaybackState::Playing,
        0,
        T0 + 60_000,
    )?;
    assert_eq!(stored(&db, &a)?.played_ms, 0);

    let stopped = report_test_playback_session(
        &mut db,
        user,
        a_track,
        ActiveEvent::Progress,
        unknown_duration(70_000, PlaybackState::Stopped),
        T0 + 61_000,
    )?
    .expect("the late stop lands on the replaced play");

    assert_eq!(stopped.playback_session_id, a.playback_session_id);
    assert_eq!(stopped.playback.state, PlaybackState::Stopped);
    assert_eq!(
        stopped.playback.played_ms, 0,
        "paused at its replacement, it accrues nothing after"
    );
    Ok(())
}

#[tokio::test]
async fn a_start_without_a_state_resumes_the_paused_replaced_play() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let a_track = insert_track(&mut db, "A", 200_000)?;
    let b_track = insert_track(&mut db, "B", 200_000)?;
    let a =
        report_named(&mut db, user, a_track, "resumed-play-a-001", Some(0), T0)?.expect("A starts");
    report_named(
        &mut db,
        user,
        b_track,
        "resumed-play-b-001",
        Some(0),
        T0 + 10_000,
    )?;

    let resumed = report_playback_session(
        &mut db,
        ReportedPlaybackRequest {
            plugin_id: "external",
            user_db_id: user,
            session_key: "auth:1",
            track_db_id: a_track,
            play_id: Some("resumed-play-a-001"),
            client_name: None,
            mutation: PlaybackMutation::default(),
            now_ms: T0 + 20_000,
            active_event: ActiveEvent::Started,
        },
    )?
    .expect("A's start brings it back");

    assert_eq!(resumed.playback_session_id, a.playback_session_id);
    assert_eq!(resumed.playback.state, PlaybackState::Playing);
    assert_eq!(resumed.playback.played_ms, 10_000);
    Ok(())
}

#[tokio::test]
async fn a_report_after_an_early_stop_resumes_the_play_and_counts_once() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let track = insert_track(&mut db, "A", 200_000)?;
    let next = insert_track(&mut db, "B", 200_000)?;
    let a = report_at(
        &mut db,
        user,
        track,
        ActiveEvent::Started,
        PlaybackState::Playing,
        0,
        T0,
    )?
    .expect("A starts");
    report_at(
        &mut db,
        user,
        track,
        ActiveEvent::Progress,
        PlaybackState::Completed,
        200_000,
        T0 + 130_000,
    )?
    .expect("the early stop ends the play");
    assert_eq!(listen_count(&db, user, track)?, 1);

    let paused = report_at(
        &mut db,
        user,
        track,
        ActiveEvent::Progress,
        PlaybackState::Paused,
        132_000,
        T0 + 130_200,
    )?
    .expect("a pause after the stop applies");
    assert_eq!(paused.playback_session_id, a.playback_session_id);
    assert_eq!(paused.playback.play_id, a.playback.play_id);
    report_at(
        &mut db,
        user,
        track,
        ActiveEvent::Progress,
        PlaybackState::Playing,
        132_000,
        T0 + 130_300,
    )?;
    report_at(
        &mut db,
        user,
        next,
        ActiveEvent::Started,
        PlaybackState::Playing,
        0,
        T0 + 150_000,
    )?;

    assert_eq!(
        listen_count(&db, user, track)?,
        1,
        "the resumed play counts once"
    );
    let resumed = stored(&db, &a)?;
    assert_eq!(resumed.state, PlaybackState::Paused);
    assert_eq!(resumed.played_ms, 135_000 + 19_700);
    assert_eq!(
        listen_for_play(&db, user, &a.playback.play_id)?.played_ms,
        Some(135_000 + 19_700),
        "the listen keeps the play's longest playing time"
    );
    Ok(())
}

const UNCOUNTED_PLAY_ID: &str = "uncounted-play-0001";

fn report_uncounted(
    db: &mut DbAny,
    user_db_id: DbId,
    track_db_id: DbId,
    (active_event, state): (ActiveEvent, PlaybackState),
    position_ms: u64,
    counts_as_listen: Option<bool>,
    now_ms: u64,
) -> Result<ReportOutcome, PlaybackError> {
    report_reported_playback(
        db,
        ReportedPlaybackRequest {
            plugin_id: "external",
            user_db_id,
            session_key: "auth:1",
            track_db_id,
            play_id: Some(UNCOUNTED_PLAY_ID),
            client_name: None,
            mutation: PlaybackMutation {
                position_ms: Some(position_ms),
                duration_ms: Some(200_000),
                state: Some(state),
                counts_as_listen,
                ..PlaybackMutation::default()
            },
            now_ms,
            active_event,
        },
    )
}

fn applied(outcome: Result<ReportOutcome, PlaybackError>) -> anyhow::Result<PlaybackRecord> {
    match outcome? {
        ReportOutcome::Applied(update) => Ok(update.session),
        ReportOutcome::Ignored(_) => anyhow::bail!("the report applies"),
    }
}

#[tokio::test]
async fn a_play_declared_not_to_count_records_no_listen_when_stopped() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let dispatch = crate::services::testing::record_plugin_dispatch();
    let user = insert_user(&mut db, "alice")?;
    let track = insert_track(&mut db, "A", 200_000)?;
    let started = applied(report_uncounted(
        &mut db,
        user,
        track,
        (ActiveEvent::Started, PlaybackState::Playing),
        0,
        Some(false),
        T0,
    ))?;
    assert!(!started.playback.counts_as_listen);

    // Later reports may leave the flag out or repeat it.
    applied(report_uncounted(
        &mut db,
        user,
        track,
        (ActiveEvent::Progress, PlaybackState::Playing),
        100_000,
        None,
        T0 + 100_000,
    ))?;
    let stopped = applied(report_uncounted(
        &mut db,
        user,
        track,
        (ActiveEvent::Progress, PlaybackState::Stopped),
        200_000,
        Some(false),
        T0 + 200_000,
    ))?;

    assert_eq!(
        stopped.playback.played_ms, 200_000,
        "the play is still accounted"
    );
    assert!(!stopped.playback.counts_as_listen);
    assert_eq!(listen_count(&db, user, track)?, 0);
    assert!(dispatch.listen_updates().is_empty());
    Ok(())
}

#[tokio::test]
async fn a_report_cannot_change_whether_its_play_counts() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let track = insert_track(&mut db, "A", 200_000)?;
    applied(report_uncounted(
        &mut db,
        user,
        track,
        (ActiveEvent::Started, PlaybackState::Playing),
        0,
        Some(false),
        T0,
    ))?;

    let changed = report_uncounted(
        &mut db,
        user,
        track,
        (ActiveEvent::Progress, PlaybackState::Playing),
        100_000,
        Some(true),
        T0 + 100_000,
    );
    assert!(matches!(
        changed,
        Err(PlaybackError::Session(
            PlaybackServiceError::CountsAsListenConflict {
                counts_as_listen: false,
                ..
            }
        ))
    ));
    let session_db_id = lyra_db::playback_sessions::find_by_play_id(&db, user, UNCOUNTED_PLAY_ID)?
        .expect("the play keeps its session");
    let session = lyra_db::playback_sessions::get_by_id(&db, session_db_id)?.expect("stored");
    assert!(!session.counts_as_listen);
    assert_eq!(session.played_ms, 0, "the refused report changed nothing");
    assert_eq!(listen_count(&db, user, track)?, 0);
    Ok(())
}

#[tokio::test]
async fn a_replaced_or_lapsed_play_declared_not_to_count_records_no_listen() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let replaced = insert_track(&mut db, "Replaced", 200_000)?;
    let lapsed = insert_track(&mut db, "Lapsed", 200_000)?;
    let a = applied(report_uncounted(
        &mut db,
        user,
        replaced,
        (ActiveEvent::Started, PlaybackState::Playing),
        0,
        Some(false),
        T0,
    ))?;

    let b = report_test_playback_session(
        &mut db,
        user,
        lapsed,
        ActiveEvent::Started,
        PlaybackMutation {
            position_ms: Some(0),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
            counts_as_listen: Some(false),
            ..PlaybackMutation::default()
        },
        T0 + 100_000,
    )?
    .expect("B starts");
    assert_eq!(
        stored(&db, &a)?.played_ms,
        100_000,
        "the replaced play is credited"
    );
    assert_eq!(listen_count(&db, user, replaced)?, 0);

    let b_lapsed_at = stored(&db, &b)?.live_until_ms + 1;
    expire_all_lapsed(&mut db, b_lapsed_at)?;
    assert!(
        stored(&db, &b)?.played_ms >= 100_000,
        "the lapsed play is credited"
    );
    assert_eq!(listen_count(&db, user, lapsed)?, 0);
    Ok(())
}

#[tokio::test]
async fn a_submitted_play_counts_after_its_live_play_was_declared_not_to() -> anyhow::Result<()> {
    let (mut db, _guard) = new_scoped_test_db().await?;
    let user = insert_user(&mut db, "alice")?;
    let track = insert_track(&mut db, "A", 200_000)?;
    applied(report_uncounted(
        &mut db,
        user,
        track,
        (ActiveEvent::Started, PlaybackState::Playing),
        0,
        Some(false),
        T0,
    ))?;
    applied(report_uncounted(
        &mut db,
        user,
        track,
        (ActiveEvent::Progress, PlaybackState::Stopped),
        100_000,
        None,
        T0 + 100_000,
    ))?;
    assert_eq!(listen_count(&db, user, track)?, 0);

    let principal = crate::services::auth::Principal::for_user(
        &db,
        user,
        vec![lyra_db::Permission::Admin],
        std::collections::HashSet::new(),
    );
    let track_public_id = lyra_db::lookup::find_id_by_db_id(&db, track)?.expect("track id");
    let outcomes = crate::services::listens::record_submitted(
        &mut db,
        &principal,
        user,
        vec![crate::services::listens::SubmittedPlay {
            play_id: UNCOUNTED_PLAY_ID.to_string(),
            track: lyra_db::ResolveId::Nanoid(track_public_id),
            started_at_ms: T0,
            played_ms: Some(100_000),
            listened: false,
        }],
        T0 + 200_000,
    )?;
    assert!(matches!(
        outcomes.get(UNCOUNTED_PLAY_ID),
        Some(crate::services::listens::SubmissionOutcome::Judged(
            crate::services::listens::RecordOutcome::Recorded(_)
        ))
    ));
    assert_eq!(listen_count(&db, user, track)?, 1);
    Ok(())
}
