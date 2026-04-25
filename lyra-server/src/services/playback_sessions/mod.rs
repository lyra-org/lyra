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

use harmony_luau::{
    DescribeTypeAlias,
    LuauType,
    LuauTypeInfo,
    TypeAliasDescriptor,
};
use serde::Deserialize;

mod sessions;
mod updates;
mod workflow;

pub(crate) use self::sessions::{
    PlaybackScopeKey,
    get_playback_session,
    is_remote_control_degraded,
    mark_command_dispatched,
};
pub(crate) use self::workflow::{
    cleanup_evicted_playbacks,
    clear_playback_session,
    list_playbacks,
    playback_activity_ms,
    report_playback_session_with_cleanup,
    report_playback_with_cleanup,
    reset_scopes_for_test,
    resolve_merged_track_ids_for_play_count,
    start_playback_with_cleanup,
};
use crate::db::{
    PlaybackSession,
    PlaybackState,
};
#[cfg(test)]
pub(crate) use updates::dispatch_update;
pub(crate) use updates::{
    PLAYBACK_CALLBACK_REGISTRY,
    PlaybackUpdatePayload,
    dispatch_evicted_updates,
    dispatch_playback_update,
    reset_callback_registry_for_test,
    subscribe_playback_events,
    teardown_plugin_callbacks,
};

pub(crate) const ACTIVE_SESSION_TTL_MS: u64 = 5 * 60 * 1000;
const POSITION_DELTA_GRACE_MS: u64 = 5_000;
const PREVIOUS_PLAYBACK_GRACE_MS: u64 = 30_000;

#[derive(Debug, thiserror::Error)]
pub(crate) enum PlaybackServiceError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
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

type ServiceResult<T> = std::result::Result<T, PlaybackServiceError>;

#[derive(Clone, Copy, Debug)]
pub(crate) enum ActivityPolicy {
    AnyState,
    PlayingOnly,
}

impl ActivityPolicy {
    fn records_position_delta(self, previous_state: PlaybackState, state: PlaybackState) -> bool {
        match self {
            Self::AnyState => true,
            Self::PlayingOnly => {
                state == PlaybackState::Playing
                    || (state.is_terminal() && previous_state == PlaybackState::Playing)
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct PlaybackMutation {
    pub(crate) position_ms: Option<u64>,
    pub(crate) duration_ms: Option<u64>,
    pub(crate) state: Option<PlaybackState>,
}

#[derive(Clone, Debug)]
pub(crate) struct StartPlaybackRequest {
    pub(crate) track_db_id: DbId,
    pub(crate) user_db_id: DbId,
    pub(crate) mutation: PlaybackMutation,
    pub(crate) now_ms: u64,
    pub(crate) active_event: ActiveEvent,
}

#[derive(Clone, Debug)]
pub(crate) struct ReportPlaybackRequest {
    pub(crate) playback_session_id: DbId,
    pub(crate) user_db_id: Option<DbId>,
    pub(crate) mutation: PlaybackMutation,
    pub(crate) now_ms: u64,
    pub(crate) activity_policy: ActivityPolicy,
    pub(crate) active_event: ActiveEvent,
}

#[derive(Clone, Debug)]
pub(crate) struct SessionPlaybackReportRequest<'a> {
    pub(crate) plugin_id: &'a str,
    pub(crate) user_db_id: DbId,
    pub(crate) session_key: &'a str,
    pub(crate) track_db_id: DbId,
    pub(crate) mutation: PlaybackMutation,
    pub(crate) now_ms: u64,
    pub(crate) active_event: ActiveEvent,
    pub(crate) stale_ttl_ms: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct PlaybackRecord {
    pub(crate) playback_session_id: DbId,
    pub(crate) track_db_id: DbId,
    pub(crate) user_db_id: DbId,
    pub(crate) playback: PlaybackSession,
}

#[derive(Clone, Debug)]
pub(crate) struct EvictedPlaybackRecord {
    pub(crate) playback_session_id: DbId,
    pub(crate) track_db_id: DbId,
    pub(crate) user_db_id: DbId,
    pub(crate) playback: PlaybackSession,
}

#[derive(Clone, Debug)]
pub(crate) struct PlaybackUpdateResult {
    pub(crate) playback: PlaybackRecord,
    pub(crate) event: String,
    pub(crate) evicted_playbacks: Vec<EvictedPlaybackRecord>,
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
            track_db_id: value.track_db_id,
            user_db_id: value.user_db_id,
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

impl LuauTypeInfo for PlaybackEvent {
    fn luau_type() -> LuauType {
        LuauType::union(vec![
            LuauType::literal("\"started\""),
            LuauType::literal("\"progress\""),
            LuauType::literal("\"stopped\""),
            LuauType::literal("\"completed\""),
        ])
    }
}

impl DescribeTypeAlias for PlaybackEvent {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "PlaybackEvent",
            Self::luau_type(),
            Some("Playback event reported by plugin helpers."),
        )
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
        report_playback_session,
    };
    use super::*;
    use crate::db;
    use crate::db::IdSource;
    use crate::db::NodeId;
    use crate::db::test_db::new_test_db;
    use agdb::DbAny;
    use agdb::QueryBuilder;
    use nanoid::nanoid;
    use std::collections::HashSet;
    use std::iter::FromIterator;
    use tokio::sync::MutexGuard;

    use super::sessions::{
        PlaybackScopeKey,
        get_playback_session,
        resolve_current_playback,
        resolve_previous_playback,
    };

    async fn new_scoped_test_db() -> anyhow::Result<(DbAny, MutexGuard<'static, ()>)> {
        let guard = crate::testing::runtime_test_lock().await;
        sessions::clear_all_scopes_for_test();
        Ok((new_test_db()?, guard))
    }

    fn insert_user(db: &mut DbAny, username: &str) -> anyhow::Result<DbId> {
        let user = db::User {
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
        let track = db::Track {
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
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
            locked: Some(false),
            created_at: Some(0),
            ctime: Some(0),
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
                plugin_id: "jellyfin",
                user_db_id,
                session_key: "auth:1",
                track_db_id,
                mutation,
                now_ms,
                active_event,
                stale_ttl_ms: ACTIVE_SESSION_TTL_MS,
            },
        )?)
    }

    #[test]
    fn unique_track_external_id_key_filter_matches_declared_pairs_only() {
        let external_ids = vec![
            db::external_ids::ExternalId {
                db_id: Some(NodeId::from(DbId(1))),
                id: nanoid!(),
                provider_id: "musicbrainz".to_string(),
                id_type: "recording_id".to_string(),
                id_value: "abc123".to_string(),
                source: IdSource::Plugin,
            },
            db::external_ids::ExternalId {
                db_id: Some(NodeId::from(DbId(2))),
                id: nanoid!(),
                provider_id: "musicbrainz".to_string(),
                id_type: "release_id".to_string(),
                id_value: "release-1".to_string(),
                source: IdSource::Plugin,
            },
            db::external_ids::ExternalId {
                db_id: Some(NodeId::from(DbId(3))),
                id: nanoid!(),
                provider_id: "discogs".to_string(),
                id_type: "recording_id".to_string(),
                id_value: "x".to_string(),
                source: IdSource::Plugin,
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
        let external_ids = vec![db::external_ids::ExternalId {
            db_id: Some(NodeId::from(DbId(1))),
            id: nanoid!(),
            provider_id: "musicbrainz".to_string(),
            id_type: "recording_id".to_string(),
            id_value: "   ".to_string(),
            source: IdSource::Plugin,
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

        db::external_ids::upsert(
            &mut db,
            track_a,
            "musicbrainz",
            "recording_id",
            "rec-123",
            db::external_ids::IdSource::Plugin,
        )?;
        db::external_ids::upsert(
            &mut db,
            track_b,
            "musicbrainz",
            "recording_id",
            "rec-123",
            db::external_ids::IdSource::Plugin,
        )?;
        db::external_ids::upsert(
            &mut db,
            track_c,
            "musicbrainz",
            "recording_id",
            "rec-other",
            db::external_ids::IdSource::Plugin,
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
            position_ms: 0,
            duration_ms: Some(300_000),
            activity_ms: Some(0),
            last_position_ms: Some(0),
            state: PlaybackState::Playing,
            listen_recorded: None,
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
            ActivityPolicy::PlayingOnly,
        )
        .expect("mutation should succeed");

        assert_eq!(playback.activity_ms, Some(150_000));
    }

    #[test]
    fn playback_mutation_ignores_large_seek_beyond_elapsed_window() {
        let mut playback = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            position_ms: 0,
            duration_ms: Some(300_000),
            activity_ms: Some(0),
            last_position_ms: Some(0),
            state: PlaybackState::Playing,
            listen_recorded: None,
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
            ActivityPolicy::PlayingOnly,
        )
        .expect("mutation should succeed");

        assert_eq!(playback.activity_ms, Some(0));
    }

    #[test]
    fn playback_mutation_does_not_record_paused_activity_in_playing_only_policy() {
        let mut playback = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            position_ms: 0,
            duration_ms: Some(300_000),
            activity_ms: Some(0),
            last_position_ms: Some(0),
            state: PlaybackState::Playing,
            listen_recorded: None,
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
            ActivityPolicy::PlayingOnly,
        )
        .expect("mutation should succeed");

        assert_eq!(playback.activity_ms, Some(0));
    }

    #[test]
    fn playback_mutation_records_terminal_delta_after_playing_state_in_playing_only_policy() {
        let mut playback = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            position_ms: 22_000,
            duration_ms: Some(200_000),
            activity_ms: Some(22_000),
            last_position_ms: Some(22_000),
            state: PlaybackState::Playing,
            listen_recorded: None,
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
            ActivityPolicy::PlayingOnly,
        )
        .expect("mutation should succeed");

        assert_eq!(playback.activity_ms, Some(196_000));
    }

    #[test]
    fn playback_mutation_does_not_record_terminal_delta_after_paused_state_in_playing_only_policy()
    {
        let mut playback = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            position_ms: 22_000,
            duration_ms: Some(200_000),
            activity_ms: Some(22_000),
            last_position_ms: Some(22_000),
            state: PlaybackState::Paused,
            listen_recorded: None,
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
            ActivityPolicy::PlayingOnly,
        )
        .expect("mutation should succeed");

        assert_eq!(playback.activity_ms, Some(22_000));
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
            plugin_id: "jellyfin",
            user_db_id,
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

        let persisted = db::playback_sessions::get_by_id(&db, started_a.playback_session_id)?
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

        let persisted = db::playback_sessions::get_by_id(&db, started_a.playback_session_id)?
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
            plugin_id: "jellyfin",
            user_db_id,
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
            plugin_id: "jellyfin",
            user_db_id,
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
            plugin_id: "jellyfin",
            user_db_id,
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
}
