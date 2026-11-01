// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    fmt,
    time::{
        SystemTime,
        UNIX_EPOCH,
    },
};

use agdb::DbId;

use serde::Deserialize;

mod sessions;
mod updates;
mod workflow;

pub(crate) use self::sessions::PlaybackScopes;
pub(crate) use self::sessions::{
    CurrentBindingSnapshot,
    PlaybackScopeKey,
    bind_current_playback_session_scope,
    clear_current_binding_if_unchanged,
    clear_playback_scopes_for_user,
    clear_session_bindings_for_playback,
    get_playback_session,
    mark_command_dispatched,
    remote_control_degraded_at_ms,
    snapshot_current_binding,
    with_no_current_binding_for_playback,
};
#[cfg(test)]
pub(crate) use self::sessions::{
    NATIVE_PLUGIN_ID,
    clear_playback_session_scope,
    is_remote_control_degraded,
};
pub(crate) use self::workflow::{
    cleanup_after_commit,
    cleanup_evicted_playbacks,
    pause_playback_in_transaction,
    pause_playing_scopes_on_disconnect,
    report_playback_in_transaction,
    report_playback_session_with_cleanup,
    resolve_merged_track_ids_for_play_count,
    start_playback_in_transaction,
};
pub(crate) use lyra_db::PlaybackSession;
use lyra_db::PlaybackState;
pub(crate) use updates::{
    PlaybackUpdatePayload,
    PlaybackUpdateRegistries,
    dispatch_evicted_updates,
    dispatch_evicted_updates_for_caller,
    dispatch_playback_update,
    dispatch_playback_update_for_caller,
    notify_connection_change,
    subscribe_playback_events,
    unsubscribe_playback_events,
    watch_connection_changes,
};

pub(crate) const ACTIVE_SESSION_TTL_MS: u64 = 5 * 60 * 1000;
const PREVIOUS_PLAYBACK_GRACE_MS: u64 = 30_000;
/// Playing time a play may be credited beyond the server's elapsed time, to absorb report latency.
const PLAY_LATENCY_ALLOWANCE_MS: u64 = 5_000;

#[derive(Debug, thiserror::Error)]
pub(crate) enum PlaybackServiceError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    /// The play already has a session that isn't the one being reported to.
    #[error("play {0} already has a session")]
    PlayConflict(String),
    #[error(transparent)]
    Internal(anyhow::Error),
}

impl PlaybackServiceError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest(message.into())
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound(message.into())
    }

    fn internal(error: impl Into<anyhow::Error>) -> Self {
        Self::Internal(error.into())
    }
}

impl From<agdb::DbError> for PlaybackServiceError {
    fn from(error: agdb::DbError) -> Self {
        Self::internal(error)
    }
}

pub(crate) type ServiceResult<T> = std::result::Result<T, PlaybackServiceError>;

#[derive(Clone, Debug, Default)]
pub(crate) struct PlaybackMutation {
    pub(crate) position_ms: Option<u64>,
    pub(crate) duration_ms: Option<u64>,
    pub(crate) state: Option<PlaybackState>,
}

#[derive(Clone, Debug)]
pub(crate) struct StartPlaybackRequest {
    pub(crate) play_id: String,
    pub(crate) track_db_id: DbId,
    pub(crate) user_db_id: DbId,
    pub(crate) client_name: Option<String>,
    /// The reporting plugin; `None` for Lyra's own clients.
    pub(crate) plugin_id: Option<String>,
    pub(crate) mutation: PlaybackMutation,
    pub(crate) now_ms: u64,
    pub(crate) active_event: ActiveEvent,
}

#[derive(Clone, Debug)]
pub(crate) struct ReportPlaybackRequest {
    /// The playback's owner; reports against anyone else's playback are rejected.
    pub(crate) user_db_id: DbId,
    pub(crate) mutation: PlaybackMutation,
    pub(crate) now_ms: u64,
    pub(crate) active_event: ActiveEvent,
}

#[derive(Clone, Debug)]
pub(crate) struct SessionPlaybackReportRequest<'a> {
    pub(crate) plugin_id: &'a str,
    pub(crate) user_db_id: DbId,
    pub(crate) session_key: &'a str,
    pub(crate) track_db_id: DbId,
    /// The reporter's play id; without one, a play is told apart by its track and events.
    pub(crate) play_id: Option<&'a str>,
    pub(crate) client_name: Option<String>,
    pub(crate) mutation: PlaybackMutation,
    pub(crate) now_ms: u64,
    pub(crate) active_event: ActiveEvent,
}

#[derive(Clone, Debug)]
pub(crate) struct PlaybackRecord {
    pub(crate) playback_session_id: DbId,
    pub(crate) playback_session_public_id: String,
    pub(crate) track_db_id: DbId,
    pub(crate) track_public_id: String,
    pub(crate) user_db_id: DbId,
    pub(crate) user_public_id: String,
    pub(crate) library_public_id: Option<String>,
    pub(crate) playback: PlaybackSession,
}

#[derive(Clone, Debug)]
pub(crate) struct TransactionPlaybackUpdate {
    pub(crate) playback: PlaybackRecord,
    pub(crate) event: String,
    /// The listen the report recorded, for the transaction's owner to announce after commit.
    pub(crate) recorded_listen: Option<lyra_db::Listen>,
}

impl TransactionPlaybackUpdate {
    /// Announces the recorded listen, if any; call right after the transaction commits.
    pub(crate) fn announce_recorded_listen(&self) {
        crate::services::listens::dispatch_recorded(
            &self.playback.user_public_id,
            self.recorded_listen.clone(),
        );
    }
}

#[derive(Clone, Debug)]
pub(crate) struct EvictedPlaybackRecord {
    pub(crate) playback_session_id: DbId,
    pub(crate) playback_session_public_id: String,
    pub(crate) track_db_id: DbId,
    pub(crate) track_public_id: String,
    pub(crate) user_db_id: DbId,
    pub(crate) user_public_id: String,
    pub(crate) library_public_id: Option<String>,
    pub(crate) playback: PlaybackSession,
}

#[derive(Clone, Debug)]
pub(crate) struct OptionalPlaybackUpdateResult {
    pub(crate) playback: Option<PlaybackRecord>,
    pub(crate) event: Option<String>,
    pub(crate) evicted_playbacks: Vec<EvictedPlaybackRecord>,
}

impl From<EvictedPlaybackRecord> for PlaybackRecord {
    fn from(value: EvictedPlaybackRecord) -> Self {
        Self {
            playback_session_id: value.playback_session_id,
            playback_session_public_id: value.playback_session_public_id,
            track_db_id: value.track_db_id,
            track_public_id: value.track_public_id,
            user_db_id: value.user_db_id,
            user_public_id: value.user_public_id,
            library_public_id: value.library_public_id,
            playback: value.playback,
        }
    }
}

pub(crate) fn now_ms() -> ServiceResult<u64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| PlaybackServiceError::bad_request("system clock is before unix epoch"))?;
    Ok(now.as_millis() as u64)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ActiveEvent {
    Started,
    Progress,
}

impl std::fmt::Display for ActiveEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Started => f.write_str("started"),
            Self::Progress => f.write_str("progress"),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum PlaybackEvent {
    Started,
    Progress,
    Stopped,
    Completed,
}

impl fmt::Display for PlaybackEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Started => f.write_str("started"),
            Self::Progress => f.write_str("progress"),
            Self::Stopped => f.write_str("stopped"),
            Self::Completed => f.write_str("completed"),
        }
    }
}

impl PlaybackEvent {
    fn to_active_event(self) -> ActiveEvent {
        match self {
            Self::Started => ActiveEvent::Started,
            _ => ActiveEvent::Progress,
        }
    }
}

/// Classifies a raw event string into an active-session event category.
/// Terminal events (stopped/completed) map to Progress because the terminal
/// state is recorded via PlaybackState, not the event — see `event_for_state`.
pub(crate) fn classify_active_event(event: Option<PlaybackEvent>) -> ServiceResult<ActiveEvent> {
    Ok(event.unwrap_or(PlaybackEvent::Progress).to_active_event())
}

#[cfg(test)]
mod tests {
    use super::workflow::{
        apply_playback_mutation,
        collect_unique_track_external_id_keys,
        pause_playing_scopes_on_disconnect,
        report_playback,
        report_playback_session,
        report_playback_session_with_cleanup,
    };
    use super::*;
    use agdb::DbAny;
    use agdb::QueryBuilder;
    use lyra_db::IdSource;
    use lyra_db::NodeId;
    use lyra_db::test_db::new_test_db;
    use nanoid::nanoid;
    use std::collections::HashSet;
    use std::iter::FromIterator;

    use crate::services::testing::RuntimeTestGuard;

    use super::sessions::{
        PlaybackScopeKey,
        get_playback_session,
        resolve_current_playback,
        resolve_previous_playback,
    };

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

    fn report_test_playback_session(
        db: &mut DbAny,
        user_db_id: DbId,
        track_db_id: DbId,
        active_event: ActiveEvent,
        mutation: PlaybackMutation,
        now_ms: u64,
    ) -> anyhow::Result<Option<PlaybackRecord>> {
        Ok(report_playback_session(
            db,
            SessionPlaybackReportRequest {
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
        )?)
    }

    fn report_test_playback_session_with_cleanup(
        db: &mut DbAny,
        user_db_id: DbId,
        track_db_id: DbId,
        active_event: ActiveEvent,
        mutation: PlaybackMutation,
        now_ms: u64,
    ) -> anyhow::Result<OptionalPlaybackUpdateResult> {
        Ok(report_playback_session_with_cleanup(
            db,
            SessionPlaybackReportRequest {
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
        )?)
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
        assert!(
            lyra_db::playbacks::get_reported(&db, user, "external", "auth:1", 1_000)?.is_none()
        );
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
        let root =
            lyra_db::playbacks::get_reported(&db, user, "external", "auth:1", 2_000)?.unwrap();
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
        let scope = PlaybackScopeKey {
            plugin_id: "external",
            user_public_id: &user_public_id(&db, user),
            session_key: "auth:1",
        };
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
        let root =
            lyra_db::playbacks::get_reported(&db, user, "external", "auth:1", 1_000)?.unwrap();
        assert!(root.queue.is_none());
        let root_id = root.db_id.unwrap();
        sessions::clear_playback_session_scope(&scope);
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
        sessions::clear_playback_session_scope(&scope);
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
        sessions::clear_playback_session_scope(&scope);
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
        assert!(
            lyra_db::listens::find_by_play_id(&db, user, &credited.playback.play_id)?.is_some()
        );
        let root = lyra_db::playbacks::get_reported(&db, user, "external", "auth:1", T0 + 111_000)?
            .unwrap();
        workflow::cleanup_evicted_playbacks(&mut db, T0 + 411_001)?;
        assert!(
            lyra_db::playbacks::get_reported(&db, user, "external", "auth:1", T0 + 411_001)?
                .is_none()
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
            lyra_db::playbacks::get_reported(&db, user, "external", "auth:1", T0 + 412_000)?
                .unwrap();
        assert_ne!(fresh.id, root.id);
        workflow::cleanup_evicted_playbacks(&mut db, T0 + 900_000)?;
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
            SessionPlaybackReportRequest {
                plugin_id: NATIVE_PLUGIN_ID,
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

    #[test]
    fn unique_track_external_id_key_filter_matches_declared_pairs_only() {
        let external_ids = vec![
            lyra_db::external_ids::ExternalId {
                db_id: Some(NodeId::from(DbId(1))),
                provider_id: "musicbrainz".to_string(),
                id_type: "recording_id".to_string(),
                id_value: "abc123".to_string(),
                source: IdSource::Resolved,
                resolved_value: None,
            },
            lyra_db::external_ids::ExternalId {
                db_id: Some(NodeId::from(DbId(2))),
                provider_id: "musicbrainz".to_string(),
                id_type: "release_id".to_string(),
                id_value: "release-1".to_string(),
                source: IdSource::Resolved,
                resolved_value: None,
            },
            lyra_db::external_ids::ExternalId {
                db_id: Some(NodeId::from(DbId(3))),
                provider_id: "discogs".to_string(),
                id_type: "recording_id".to_string(),
                id_value: "x".to_string(),
                source: IdSource::Resolved,
                resolved_value: None,
            },
        ];
        let unique_track_id_pairs =
            HashSet::from_iter([("musicbrainz".to_string(), "recording_id".to_string())]);

        let keys = collect_unique_track_external_id_keys(&external_ids, &unique_track_id_pairs);
        let expected = HashSet::from_iter([(
            "musicbrainz".to_string(),
            "recording_id".to_string(),
            "abc123".to_string(),
        )]);
        assert_eq!(keys, expected);
    }

    #[test]
    fn unique_track_external_id_key_filter_ignores_blank_values() {
        let external_ids = vec![lyra_db::external_ids::ExternalId {
            db_id: Some(NodeId::from(DbId(1))),
            provider_id: "musicbrainz".to_string(),
            id_type: "recording_id".to_string(),
            id_value: "   ".to_string(),
            source: IdSource::Resolved,
            resolved_value: None,
        }];
        let unique_track_id_pairs =
            HashSet::from_iter([("musicbrainz".to_string(), "recording_id".to_string())]);

        let keys = collect_unique_track_external_id_keys(&external_ids, &unique_track_id_pairs);
        assert!(keys.is_empty());
    }

    #[test]
    fn resolve_merged_track_ids_finds_tracks_sharing_external_ids() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let track_a = insert_track(&mut db, "Track A", 200_000)?;
        let track_b = insert_track(&mut db, "Track B", 200_000)?;
        let track_c = insert_track(&mut db, "Track C", 200_000)?;

        lyra_db::external_ids::upsert(
            &mut db,
            track_a,
            "musicbrainz",
            "recording_id",
            "rec-123",
            lyra_db::external_ids::IdSource::Resolved,
        )?;
        lyra_db::external_ids::upsert(
            &mut db,
            track_b,
            "musicbrainz",
            "recording_id",
            "rec-123",
            lyra_db::external_ids::IdSource::Resolved,
        )?;
        lyra_db::external_ids::upsert(
            &mut db,
            track_c,
            "musicbrainz",
            "recording_id",
            "rec-other",
            lyra_db::external_ids::IdSource::Resolved,
        )?;

        let pairs = HashSet::from_iter([("musicbrainz".to_string(), "recording_id".to_string())]);
        let mut merged = resolve_merged_track_ids_for_play_count(&db, track_a, &pairs)?;
        merged.sort_by_key(|id| id.0);
        let mut expected = vec![track_a, track_b];
        expected.sort_by_key(|id| id.0);
        assert_eq!(merged, expected);

        Ok(())
    }

    #[test]
    fn resolve_merged_track_ids_returns_self_when_no_pairs() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track_db_id = insert_track(&mut db, "Solo Track", 200_000)?;

        let empty_pairs = HashSet::new();
        let merged = resolve_merged_track_ids_for_play_count(&db, track_db_id, &empty_pairs)?;
        assert_eq!(merged, vec![track_db_id]);

        Ok(())
    }

    #[test]
    fn playback_mutation_counts_sparse_progress_with_elapsed_time_window() {
        let mut playback = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            client_name: None,
            plugin_id: None,
            position_ms: 0,
            duration_ms: Some(300_000),
            played_ms: 0,
            last_position_ms: 0,
            state: PlaybackState::Playing,
            play_id: nanoid::nanoid!(),
            latency_allowance_used_ms: 0,
            last_position_at_ms: 1_000,
            updated_at_ms: 1_000,
            created_at_ms: 0,
        };

        apply_playback_mutation(
            &mut playback,
            &PlaybackMutation {
                position_ms: Some(150_000),
                duration_ms: None,
                state: Some(PlaybackState::Playing),
            },
            151_000,
        )
        .expect("mutation should succeed");

        assert_eq!(playback.played_ms, 150_000);
    }

    #[test]
    fn playback_mutation_caps_a_forward_seek_at_elapsed_time_and_latency_allowance() {
        let mut playback = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            client_name: None,
            plugin_id: None,
            position_ms: 0,
            duration_ms: Some(300_000),
            played_ms: 0,
            last_position_ms: 0,
            state: PlaybackState::Playing,
            play_id: nanoid::nanoid!(),
            latency_allowance_used_ms: 0,
            last_position_at_ms: 1_000,
            updated_at_ms: 1_000,
            created_at_ms: 0,
        };

        apply_playback_mutation(
            &mut playback,
            &PlaybackMutation {
                position_ms: Some(30_000),
                duration_ms: None,
                state: Some(PlaybackState::Playing),
            },
            11_000,
        )
        .expect("mutation should succeed");

        assert_eq!(playback.played_ms, 15_000);
        assert_eq!(
            playback.latency_allowance_used_ms,
            PLAY_LATENCY_ALLOWANCE_MS
        );
    }

    #[test]
    fn playback_mutation_spends_the_latency_allowance_once_per_play() {
        let mut playback = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            play_id: nanoid!(),
            client_name: None,
            plugin_id: None,
            position_ms: 0,
            duration_ms: Some(300_000),
            played_ms: 0,
            last_position_ms: 0,
            latency_allowance_used_ms: 0,
            state: PlaybackState::Playing,
            last_position_at_ms: 1_000,
            updated_at_ms: 1_000,
            created_at_ms: 0,
        };

        // A burst of reports, each claiming 4 s of progress with no time passing.
        for report in 1..=10 {
            apply_playback_mutation(
                &mut playback,
                &PlaybackMutation {
                    position_ms: Some(report * 4_000),
                    duration_ms: None,
                    state: Some(PlaybackState::Playing),
                },
                1_000,
            )
            .expect("mutation should succeed");
        }

        assert_eq!(playback.played_ms, PLAY_LATENCY_ALLOWANCE_MS);
        assert_eq!(
            playback.latency_allowance_used_ms,
            PLAY_LATENCY_ALLOWANCE_MS
        );
    }

    #[test]
    fn playback_mutation_credits_progress_up_to_a_pause() {
        let mut playback = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            client_name: None,
            plugin_id: None,
            position_ms: 0,
            duration_ms: Some(300_000),
            played_ms: 0,
            last_position_ms: 0,
            state: PlaybackState::Playing,
            play_id: nanoid::nanoid!(),
            latency_allowance_used_ms: 0,
            last_position_at_ms: 1_000,
            updated_at_ms: 1_000,
            created_at_ms: 0,
        };

        apply_playback_mutation(
            &mut playback,
            &PlaybackMutation {
                position_ms: Some(5_000),
                duration_ms: None,
                state: Some(PlaybackState::Paused),
            },
            6_000,
        )
        .expect("mutation should succeed");

        assert_eq!(playback.played_ms, 5_000);
    }

    #[test]
    fn playback_mutation_credits_the_final_report_after_playing() {
        let mut playback = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            client_name: None,
            plugin_id: None,
            position_ms: 22_000,
            duration_ms: Some(200_000),
            played_ms: 22_000,
            last_position_ms: 22_000,
            state: PlaybackState::Playing,
            play_id: nanoid::nanoid!(),
            latency_allowance_used_ms: 0,
            last_position_at_ms: 22_000,
            updated_at_ms: 22_000,
            created_at_ms: 0,
        };

        apply_playback_mutation(
            &mut playback,
            &PlaybackMutation {
                position_ms: Some(196_000),
                duration_ms: None,
                state: Some(PlaybackState::Stopped),
            },
            197_000,
        )
        .expect("mutation should succeed");

        assert_eq!(playback.played_ms, 196_000);
    }

    #[test]
    fn playback_mutation_does_not_credit_progress_reported_while_paused() {
        let mut playback = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            client_name: None,
            plugin_id: None,
            position_ms: 22_000,
            duration_ms: Some(200_000),
            played_ms: 22_000,
            last_position_ms: 22_000,
            state: PlaybackState::Paused,
            play_id: nanoid::nanoid!(),
            latency_allowance_used_ms: 0,
            last_position_at_ms: 22_000,
            updated_at_ms: 22_000,
            created_at_ms: 0,
        };

        apply_playback_mutation(
            &mut playback,
            &PlaybackMutation {
                position_ms: Some(196_000),
                duration_ms: None,
                state: Some(PlaybackState::Stopped),
            },
            197_000,
        )
        .expect("mutation should succeed");

        assert_eq!(playback.played_ms, 22_000);
    }

    #[tokio::test]
    async fn report_playback_session_rotates_current_to_previous_on_track_switch()
    -> anyhow::Result<()> {
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

        let scope = PlaybackScopeKey {
            plugin_id: "external",
            user_public_id: &user_public_id(&db, user_db_id),
            session_key: "auth:1",
        };
        let session = get_playback_session(&scope).expect("session should exist");
        let current =
            resolve_current_playback(&db, &session)?.expect("current playback should exist");
        let previous =
            resolve_previous_playback(&db, &session)?.expect("previous playback should exist");
        assert_eq!(current.playback_session_id, started_b.playback_session_id);
        assert_eq!(current.track_db_id, track_b_id);
        assert_eq!(previous.playback_session_id, started_a.playback_session_id);
        assert_eq!(previous.track_db_id, track_a_id);
        assert_eq!(session.previous_demoted_at_ms, Some(2_000));
        assert_eq!(
            session.previous_expires_at_ms,
            Some(2_000 + PREVIOUS_PLAYBACK_GRACE_MS)
        );
        Ok(())
    }

    #[tokio::test]
    async fn report_playback_session_routes_late_terminal_to_previous_track() -> anyhow::Result<()>
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
    async fn report_playback_session_ignores_expired_previous_terminal_update() -> anyhow::Result<()>
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
        let records = workflow::collect_playback_records(&db)?;
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
            assert!(before.reported.unwrap().previous.is_some());
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
            assert!(after.reported.unwrap().previous.is_none());
            assert_eq!(after.updated_at_ms, 3_000);
        }
        Ok(())
    }

    #[tokio::test]
    async fn previous_report_preserves_current_command_and_binding_epoch() -> anyhow::Result<()> {
        let (mut db, _guard) = new_scoped_test_db().await?;
        let user = insert_user(&mut db, "alice")?;
        let a = insert_track(&mut db, "A", 200_000)?;
        let b = insert_track(&mut db, "B", 200_000)?;
        for (track, now) in [(a, 1_000), (b, 2_000)] {
            report_test_playback_session(
                &mut db,
                user,
                track,
                ActiveEvent::Started,
                PlaybackMutation::default(),
                now,
            )?;
        }
        let scope = PlaybackScopeKey {
            plugin_id: "external",
            user_public_id: &user_public_id(&db, user),
            session_key: "auth:1",
        };
        mark_command_dispatched(&scope, 3_000);
        let before = sessions::get_playback_session(&scope).unwrap();
        report_test_playback_session(
            &mut db,
            user,
            a,
            ActiveEvent::Progress,
            PlaybackMutation {
                state: Some(PlaybackState::Paused),
                ..Default::default()
            },
            4_000,
        )?;
        let after = sessions::get_playback_session(&scope).unwrap();
        assert_eq!(after.current_binding_epoch, before.current_binding_epoch);
        assert_eq!(after.command_dispatched_at_ms, Some(3_000));
        assert_eq!(after.updated_at_ms, before.updated_at_ms);
        report_test_playback_session(
            &mut db,
            user,
            b,
            ActiveEvent::Progress,
            PlaybackMutation::default(),
            5_000,
        )?;
        let observed = sessions::get_playback_session(&scope).unwrap();
        assert_eq!(
            observed.current_binding_epoch,
            before.current_binding_epoch.wrapping_add(1)
        );
        assert_eq!(observed.command_dispatched_at_ms, None);
        assert_eq!(observed.updated_at_ms, 5_000);
        Ok(())
    }

    #[tokio::test]
    async fn circuit_breaker_not_degraded_without_dispatch() -> anyhow::Result<()> {
        let (mut db, _guard) = new_scoped_test_db().await?;
        let user_db_id = insert_user(&mut db, "alice")?;
        let track_db_id = insert_track(&mut db, "Track A", 200_000)?;

        report_test_playback_session(
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
        )?;

        let scope = PlaybackScopeKey {
            plugin_id: "external",
            user_public_id: &user_public_id(&db, user_db_id),
            session_key: "auth:1",
        };
        assert!(!is_remote_control_degraded(&scope, 100_000));
        Ok(())
    }

    #[tokio::test]
    async fn circuit_breaker_degrades_after_timeout() -> anyhow::Result<()> {
        let (mut db, _guard) = new_scoped_test_db().await?;
        let user_db_id = insert_user(&mut db, "alice")?;
        let track_db_id = insert_track(&mut db, "Track A", 200_000)?;

        report_test_playback_session(
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
        )?;

        let scope = PlaybackScopeKey {
            plugin_id: "external",
            user_public_id: &user_public_id(&db, user_db_id),
            session_key: "auth:1",
        };

        mark_command_dispatched(&scope, 10_000);
        assert!(!is_remote_control_degraded(&scope, 10_000 + 29_000));
        assert!(is_remote_control_degraded(&scope, 10_000 + 30_000));
        Ok(())
    }

    #[tokio::test]
    async fn circuit_breaker_clears_on_state_update() -> anyhow::Result<()> {
        let (mut db, _guard) = new_scoped_test_db().await?;
        let user_db_id = insert_user(&mut db, "alice")?;
        let track_db_id = insert_track(&mut db, "Track A", 200_000)?;

        report_test_playback_session(
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
        )?;

        let scope = PlaybackScopeKey {
            plugin_id: "external",
            user_public_id: &user_public_id(&db, user_db_id),
            session_key: "auth:1",
        };

        mark_command_dispatched(&scope, 10_000);
        assert!(is_remote_control_degraded(&scope, 10_000 + 30_000));

        report_test_playback_session(
            &mut db,
            user_db_id,
            track_db_id,
            ActiveEvent::Progress,
            PlaybackMutation {
                position_ms: Some(50_000),
                duration_ms: Some(200_000),
                state: Some(PlaybackState::Playing),
            },
            50_000,
        )?;

        assert!(!is_remote_control_degraded(&scope, 50_000 + 30_000));
        Ok(())
    }

    #[tokio::test]
    async fn disconnect_pauses_playing_scopes_at_last_reported_position() -> anyhow::Result<()> {
        let (mut db, _guard) = new_scoped_test_db().await?;
        let user_db_id = insert_user(&mut db, "alice")?;
        let alice_public_id = user_public_id(&db, user_db_id);
        let track_db_id = insert_track(&mut db, "Track A", 200_000)?;
        let native_track_db_id = insert_track(&mut db, "Track B", 200_000)?;

        let demo_started = report_playback_session(
            &mut db,
            SessionPlaybackReportRequest {
                plugin_id: "demo",
                user_db_id,
                session_key: "device:1",
                track_db_id,
                play_id: None,
                client_name: None,
                mutation: PlaybackMutation {
                    position_ms: Some(10_000),
                    duration_ms: Some(200_000),
                    state: Some(PlaybackState::Playing),
                },
                now_ms: 1_000,
                active_event: ActiveEvent::Started,
            },
        )?
        .expect("started playback");
        let native_started = report_playback_session(
            &mut db,
            SessionPlaybackReportRequest {
                plugin_id: NATIVE_PLUGIN_ID,
                user_db_id,
                session_key: "device:1",
                track_db_id: native_track_db_id,
                play_id: None,
                client_name: None,
                mutation: PlaybackMutation {
                    position_ms: Some(20_000),
                    duration_ms: Some(200_000),
                    state: Some(PlaybackState::Playing),
                },
                now_ms: 2_000,
                active_event: ActiveEvent::Started,
            },
        )?
        .expect("started native playback");

        let (paused_playbacks, evicted_playbacks) =
            pause_playing_scopes_on_disconnect(&mut db, &alice_public_id, "device:1", 4_000)?;

        assert_eq!(paused_playbacks.len(), 2);
        assert!(evicted_playbacks.is_empty());

        let demo_paused = paused_playbacks
            .iter()
            .find(|playback| playback.playback_session_id == demo_started.playback_session_id)
            .expect("demo playback paused");
        assert_eq!(demo_paused.playback.position_ms, 10_000);
        assert_eq!(demo_paused.playback.state, PlaybackState::Paused);
        assert_eq!(demo_paused.playback.played_ms, 0);
        assert_eq!(demo_paused.playback.updated_at_ms, 4_000);

        let native_paused = paused_playbacks
            .iter()
            .find(|playback| playback.playback_session_id == native_started.playback_session_id)
            .expect("native playback paused");
        assert_eq!(native_paused.playback.position_ms, 20_000);
        assert_eq!(native_paused.playback.state, PlaybackState::Paused);
        assert_eq!(native_paused.playback.played_ms, 0);
        assert_eq!(native_paused.playback.updated_at_ms, 4_000);

        let demo_scope = PlaybackScopeKey {
            plugin_id: "demo",
            user_public_id: &user_public_id(&db, user_db_id),
            session_key: "device:1",
        };
        assert!(get_playback_session(&demo_scope).is_none());

        let native_user_public_id = user_public_id(&db, user_db_id);
        let native_scope = PlaybackScopeKey::native(&native_user_public_id, "device:1");
        assert!(get_playback_session(&native_scope).is_none());

        let persisted =
            lyra_db::playback_sessions::get_by_id(&db, demo_started.playback_session_id)?
                .expect("persisted playback");
        assert_eq!(persisted.position_ms, 10_000);
        assert_eq!(persisted.state, PlaybackState::Paused);

        let persisted =
            lyra_db::playback_sessions::get_by_id(&db, native_started.playback_session_id)?
                .expect("persisted native playback");
        assert_eq!(persisted.position_ms, 20_000);
        assert_eq!(persisted.state, PlaybackState::Paused);
        Ok(())
    }

    #[tokio::test]
    async fn disconnect_pauses_previous_scope_at_demotion_time() -> anyhow::Result<()> {
        let (mut db, _guard) = new_scoped_test_db().await?;
        let user_db_id = insert_user(&mut db, "alice")?;
        let alice_public_id = user_public_id(&db, user_db_id);
        let track_a_id = insert_track(&mut db, "Track A", 200_000)?;
        let track_b_id = insert_track(&mut db, "Track B", 200_000)?;

        let started_a = report_playback_session(
            &mut db,
            SessionPlaybackReportRequest {
                plugin_id: "demo",
                user_db_id,
                session_key: "device:1",
                track_db_id: track_a_id,
                play_id: None,
                client_name: None,
                mutation: PlaybackMutation {
                    position_ms: Some(10_000),
                    duration_ms: Some(200_000),
                    state: Some(PlaybackState::Playing),
                },
                now_ms: 1_000,
                active_event: ActiveEvent::Started,
            },
        )?
        .expect("track A should start");
        let started_b = report_playback_session(
            &mut db,
            SessionPlaybackReportRequest {
                plugin_id: "demo",
                user_db_id,
                session_key: "device:1",
                track_db_id: track_b_id,
                play_id: None,
                client_name: None,
                mutation: PlaybackMutation {
                    position_ms: Some(20_000),
                    duration_ms: Some(200_000),
                    state: Some(PlaybackState::Playing),
                },
                now_ms: 2_000,
                active_event: ActiveEvent::Started,
            },
        )?
        .expect("track B should start");

        let (paused_playbacks, evicted_playbacks) =
            pause_playing_scopes_on_disconnect(&mut db, &alice_public_id, "device:1", 5_000)?;

        assert_eq!(paused_playbacks.len(), 2);
        assert!(evicted_playbacks.is_empty());

        let paused_a = paused_playbacks
            .iter()
            .find(|playback| playback.playback_session_id == started_a.playback_session_id)
            .expect("previous playback paused");
        assert_eq!(paused_a.playback.position_ms, 10_000);
        assert_eq!(paused_a.playback.state, PlaybackState::Paused);
        assert_eq!(paused_a.playback.played_ms, 0);
        assert_eq!(paused_a.playback.updated_at_ms, 2_000);

        let paused_b = paused_playbacks
            .iter()
            .find(|playback| playback.playback_session_id == started_b.playback_session_id)
            .expect("current playback paused");
        assert_eq!(paused_b.playback.position_ms, 20_000);
        assert_eq!(paused_b.playback.state, PlaybackState::Paused);
        assert_eq!(paused_b.playback.played_ms, 0);
        assert_eq!(paused_b.playback.updated_at_ms, 5_000);

        let scope = PlaybackScopeKey {
            plugin_id: "demo",
            user_public_id: &user_public_id(&db, user_db_id),
            session_key: "device:1",
        };
        assert!(get_playback_session(&scope).is_none());

        let persisted_a =
            lyra_db::playback_sessions::get_by_id(&db, started_a.playback_session_id)?
                .expect("persisted previous playback");
        assert_eq!(persisted_a.position_ms, 10_000);
        assert_eq!(persisted_a.state, PlaybackState::Paused);

        let persisted_b =
            lyra_db::playback_sessions::get_by_id(&db, started_b.playback_session_id)?
                .expect("persisted current playback");
        assert_eq!(persisted_b.position_ms, 20_000);
        assert_eq!(persisted_b.state, PlaybackState::Paused);
        Ok(())
    }

    #[test]
    fn playback_mutation_keeps_played_time_after_seeking_back_to_the_start() {
        let mut playback = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            play_id: nanoid!(),
            client_name: None,
            plugin_id: None,
            position_ms: 150_000,
            duration_ms: Some(200_000),
            played_ms: 150_000,
            last_position_ms: 150_000,
            latency_allowance_used_ms: 0,
            state: PlaybackState::Playing,
            last_position_at_ms: 150_000,
            updated_at_ms: 150_000,
            created_at_ms: 0,
        };

        apply_playback_mutation(
            &mut playback,
            &PlaybackMutation {
                position_ms: Some(1_500),
                duration_ms: None,
                state: Some(PlaybackState::Playing),
            },
            151_000,
        )
        .expect("mutation should succeed");

        assert_eq!(playback.played_ms, 150_000);
        assert_eq!(playback.last_position_ms, 1_500);
        assert_eq!(
            playback.created_at_ms, 0,
            "a seek does not start a new play"
        );
    }

    #[tokio::test]
    async fn report_playback_session_credits_two_listens_for_started_replay() -> anyhow::Result<()>
    {
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

        let records = workflow::collect_playback_records(&db)?;
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

        let records = workflow::collect_playback_records(&db)?;
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

        let records = workflow::collect_playback_records(&db)?;
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
            SessionPlaybackReportRequest {
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

    fn listen_for_play(
        db: &DbAny,
        user_db_id: DbId,
        play_id: &str,
    ) -> anyhow::Result<lyra_db::Listen> {
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
            SessionPlaybackReportRequest {
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
        assert_eq!(workflow::collect_playback_records(&db)?.len(), 2);
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

    #[test]
    fn playback_mutation_measures_elapsed_time_from_the_last_reported_position() {
        let mut playback = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            play_id: nanoid!(),
            client_name: None,
            plugin_id: None,
            position_ms: 0,
            duration_ms: Some(300_000),
            played_ms: 0,
            last_position_ms: 0,
            last_position_at_ms: 0,
            latency_allowance_used_ms: 0,
            state: PlaybackState::Playing,
            updated_at_ms: 0,
            created_at_ms: 0,
        };

        for (mutation, now_ms) in [
            (
                PlaybackMutation {
                    state: Some(PlaybackState::Playing),
                    ..PlaybackMutation::default()
                },
                14_000,
            ),
            (
                PlaybackMutation {
                    position_ms: Some(30_000),
                    state: Some(PlaybackState::Playing),
                    ..PlaybackMutation::default()
                },
                30_000,
            ),
        ] {
            apply_playback_mutation(&mut playback, &mutation, now_ms)
                .expect("mutation should succeed");
        }

        assert_eq!(playback.played_ms, 30_000);
        assert_eq!(playback.latency_allowance_used_ms, 0);

        // Paused without a position, resumed without one after 10 minutes, then a jump far
        // ahead: only the time since the resume counts, plus the latency allowance.
        let reports = [
            (None, PlaybackState::Paused, 31_000),
            (None, PlaybackState::Playing, 631_000),
            (Some(205_000), PlaybackState::Playing, 636_000),
        ];
        for (position_ms, state, now_ms) in reports {
            apply_playback_mutation(
                &mut playback,
                &PlaybackMutation {
                    position_ms,
                    state: Some(state),
                    ..PlaybackMutation::default()
                },
                now_ms,
            )
            .expect("mutation should succeed");
        }

        assert_eq!(
            playback.played_ms,
            30_000 + 5_000 + PLAY_LATENCY_ALLOWANCE_MS
        );
    }
    #[tokio::test]
    async fn a_named_play_resumes_its_own_stopped_session() -> anyhow::Result<()> {
        let (mut db, _guard) = new_scoped_test_db().await?;
        let user = insert_user(&mut db, "alice")?;
        let track = insert_track(&mut db, "A", 200_000)?;
        let report = |db: &mut DbAny, position_ms: u64, state: PlaybackState, now_ms: u64| {
            report_playback_session_with_cleanup(
                db,
                SessionPlaybackReportRequest {
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
}
