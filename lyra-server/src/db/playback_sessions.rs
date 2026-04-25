// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::fmt;

use agdb::{
    DbAny,
    DbElement,
    DbError,
    DbId,
    DbTypeMarker,
    DbValue,
    QueryBuilder,
};
use harmony_luau::{
    LuauType,
    LuauTypeInfo,
};
use serde::{
    Deserialize,
    Serialize,
};

use super::tracks::get_by_id as get_track_by_id;
use super::users::get_by_id as get_user_by_id;

const STALE_TTL_MS: u64 = 5 * 60 * 1000;
const MIN_ACTIVITY_MS: u64 = 30_000;
const SHORT_TRACK_MS: u64 = 180_000;
const SHORT_TRACK_ACTIVITY_PERCENT: u64 = 20;

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, DbTypeMarker,
)]
#[serde(rename_all = "lowercase")]
pub(crate) enum PlaybackState {
    Playing,
    Paused,
    Stopped,
    Buffering,
    Completed,
}

impl PlaybackState {
    pub(crate) fn is_terminal(self) -> bool {
        matches!(self, Self::Stopped | Self::Completed)
    }

    fn as_db_str(self) -> &'static str {
        match self {
            Self::Playing => "playing",
            Self::Paused => "paused",
            Self::Stopped => "stopped",
            Self::Buffering => "buffering",
            Self::Completed => "completed",
        }
    }

    fn from_db_str(value: &str) -> Result<Self, DbError> {
        match value {
            "playing" => Ok(Self::Playing),
            "paused" => Ok(Self::Paused),
            "stopped" => Ok(Self::Stopped),
            "buffering" => Ok(Self::Buffering),
            "completed" => Ok(Self::Completed),
            _ => Err(DbError::from(format!(
                "invalid PlaybackState value '{value}'"
            ))),
        }
    }
}

impl fmt::Display for PlaybackState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_db_str())
    }
}

impl From<PlaybackState> for DbValue {
    fn from(value: PlaybackState) -> Self {
        Self::from(value.as_db_str())
    }
}

impl From<&PlaybackState> for DbValue {
    fn from(value: &PlaybackState) -> Self {
        (*value).into()
    }
}

impl TryFrom<DbValue> for PlaybackState {
    type Error = DbError;

    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        Self::from_db_str(value.string()?)
    }
}

impl LuauTypeInfo for PlaybackState {
    fn luau_type() -> LuauType {
        LuauType::union(vec![
            LuauType::literal("\"playing\""),
            LuauType::literal("\"paused\""),
            LuauType::literal("\"stopped\""),
            LuauType::literal("\"buffering\""),
            LuauType::literal("\"completed\""),
        ])
    }
}

#[derive(DbElement, Clone, Debug)]
pub(crate) struct PlaybackSession {
    pub(crate) db_id: Option<DbId>,
    pub(crate) id: String,
    pub(crate) position_ms: u64,
    pub(crate) duration_ms: Option<u64>,
    pub(crate) activity_ms: Option<u64>,
    pub(crate) last_position_ms: Option<u64>,
    pub(crate) state: PlaybackState,
    pub(crate) listen_recorded: Option<bool>,
    pub(crate) updated_at_ms: u64,
    pub(crate) created_at_ms: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct EvictedPlayback {
    pub(crate) playback: PlaybackSession,
    pub(crate) track_db_id: DbId,
    pub(crate) user_db_id: DbId,
}

pub(crate) fn get_track_id(db: &DbAny, playback_session_id: DbId) -> anyhow::Result<Option<DbId>> {
    for edge in super::graph::direct_edges_from(db, playback_session_id)? {
        let Some(target_id) = edge.to else {
            continue;
        };
        if get_track_by_id(db, target_id)?.is_some() {
            return Ok(Some(target_id));
        }
    }

    Ok(None)
}

pub(crate) fn get_user_id(db: &DbAny, playback_session_id: DbId) -> anyhow::Result<Option<DbId>> {
    for edge in super::graph::direct_edges_from(db, playback_session_id)? {
        let Some(target_id) = edge.to else {
            continue;
        };
        if get_user_by_id(db, target_id)?.is_some() {
            return Ok(Some(target_id));
        }
    }

    Ok(None)
}

pub(crate) fn create(
    db: &mut DbAny,
    playback_session: &PlaybackSession,
    track_db_id: DbId,
    user_db_id: DbId,
) -> anyhow::Result<DbId> {
    db.transaction_mut(|t| -> anyhow::Result<DbId> {
        let playback_session_id = t
            .exec_mut(QueryBuilder::insert().element(playback_session).query())?
            .ids()[0];

        t.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("playback_sessions")
                .to(playback_session_id)
                .query(),
        )?;
        t.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(playback_session_id)
                .to(track_db_id)
                .query(),
        )?;
        t.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(playback_session_id)
                .to(user_db_id)
                .query(),
        )?;

        Ok(playback_session_id)
    })
}

pub(crate) fn get_by_id(
    db: &impl super::DbAccess,
    playback_session_id: DbId,
) -> anyhow::Result<Option<PlaybackSession>> {
    super::graph::fetch_typed_by_id(db, playback_session_id, "PlaybackSession")
}

pub(crate) fn get(db: &DbAny) -> anyhow::Result<Vec<PlaybackSession>> {
    let playback_sessions: Vec<PlaybackSession> = db
        .exec(
            QueryBuilder::select()
                .elements::<PlaybackSession>()
                .search()
                .from("playback_sessions")
                .query(),
        )?
        .try_into()?;

    Ok(playback_sessions)
}

pub(crate) fn min_required_activity_ms(duration_ms: Option<u64>) -> u64 {
    if let Some(duration_ms) = duration_ms
        && duration_ms > 0
        && duration_ms < SHORT_TRACK_MS
    {
        // Round up to avoid a zero threshold for very short tracks.
        return duration_ms
            .saturating_mul(SHORT_TRACK_ACTIVITY_PERCENT)
            .saturating_add(99)
            / 100;
    }

    MIN_ACTIVITY_MS
}

pub(crate) fn activity_meets_listen_threshold(activity_ms: u64, duration_ms: Option<u64>) -> bool {
    activity_ms >= min_required_activity_ms(duration_ms)
}

pub(crate) fn playback_session_meets_listen_threshold(playback_session: &PlaybackSession) -> bool {
    let activity_ms = playback_session
        .activity_ms
        .unwrap_or(playback_session.position_ms);
    activity_meets_listen_threshold(activity_ms, playback_session.duration_ms)
}

pub(crate) fn evict_low_activity(
    db: &mut DbAny,
    now_ms: u64,
    is_active: impl Fn(DbId) -> bool,
) -> anyhow::Result<Vec<EvictedPlayback>> {
    let cutoff_ms = now_ms.saturating_sub(STALE_TTL_MS);
    let mut evicted = Vec::new();
    let mut to_remove = Vec::new();

    for playback_session in get(db)? {
        if playback_session.updated_at_ms >= cutoff_ms {
            continue;
        }

        if playback_session_meets_listen_threshold(&playback_session) {
            continue;
        }

        let Some(playback_session_id) = playback_session.db_id else {
            continue;
        };
        if is_active(playback_session_id) {
            continue;
        }

        let track_db_id = get_track_id(db, playback_session_id)?.unwrap_or_default();
        let user_db_id = get_user_id(db, playback_session_id)?.unwrap_or_default();

        to_remove.push(playback_session_id);
        evicted.push(EvictedPlayback {
            playback: playback_session,
            track_db_id,
            user_db_id,
        });
    }

    if to_remove.is_empty() {
        return Ok(evicted);
    }

    db.exec_mut(QueryBuilder::remove().ids(&to_remove).query())?;
    Ok(evicted)
}

pub(crate) fn update(
    db: &mut impl super::DbAccess,
    playback_session: &PlaybackSession,
) -> anyhow::Result<()> {
    db.exec_mut(QueryBuilder::insert().element(playback_session).query())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use nanoid::nanoid;

    use super::*;
    use crate::db::test_db::new_test_db;
    use crate::db::tracks::Track;
    use crate::db::users::User;
    use agdb::{
        DbAny,
        DbValue,
        QueryBuilder,
    };

    fn create_test_user(db: &mut DbAny) -> anyhow::Result<DbId> {
        let user = User {
            db_id: None,
            id: nanoid!(),
            username: "testuser".to_string(),
            password: "hashed".to_string(),
        };
        let user_id = db
            .exec_mut(QueryBuilder::insert().element(&user).query())?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("users")
                .to(user_id)
                .query(),
        )?;
        Ok(user_id)
    }

    fn create_test_track(db: &mut DbAny, title: &str) -> anyhow::Result<DbId> {
        let track = Track {
            db_id: None,
            id: nanoid!(),
            track_title: title.to_string(),
            sort_title: None,
            year: None,
            disc: None,
            disc_total: None,
            track: None,
            track_total: None,
            duration_ms: None,
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
            locked: None,
            created_at: None,
            ctime: None,
        };
        let track_id = db
            .exec_mut(QueryBuilder::insert().element(&track).query())?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("tracks")
                .to(track_id)
                .query(),
        )?;
        Ok(track_id)
    }

    fn make_session(now_ms: u64) -> PlaybackSession {
        PlaybackSession {
            db_id: None,
            id: nanoid!(),
            position_ms: 0,
            duration_ms: Some(300_000),
            activity_ms: Some(0),
            last_position_ms: None,
            state: PlaybackState::Playing,
            listen_recorded: None,
            updated_at_ms: now_ms,
            created_at_ms: now_ms,
        }
    }

    fn element_value(element: &agdb::DbElement, key: &str) -> Option<DbValue> {
        element.values.iter().find_map(|kv| {
            let Ok(found_key) = kv.key.string() else {
                return None;
            };
            if found_key == key {
                Some(kv.value.clone())
            } else {
                None
            }
        })
    }

    #[test]
    fn playback_state_uses_stable_string_db_values() -> anyhow::Result<()> {
        assert_eq!(
            DbValue::from(PlaybackState::Playing),
            DbValue::from("playing")
        );
        assert_eq!(
            PlaybackState::try_from(DbValue::from("buffering"))?,
            PlaybackState::Buffering
        );
        assert!(PlaybackState::try_from(DbValue::from("queued")).is_err());
        Ok(())
    }

    #[test]
    fn create_and_get_by_id() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_id = create_test_user(&mut db)?;
        let track_id = create_test_track(&mut db, "Test Track")?;

        let session = make_session(1000);
        let session_id = create(&mut db, &session, track_id, user_id)?;

        let fetched = get_by_id(&db, session_id)?.expect("session should exist");
        assert_eq!(fetched.position_ms, 0);
        assert_eq!(fetched.state, PlaybackState::Playing);
        Ok(())
    }

    #[test]
    fn playback_session_persists_state_as_string() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_id = create_test_user(&mut db)?;
        let track_id = create_test_track(&mut db, "Stored State Track")?;

        let session = make_session(1000);
        let session_id = create(&mut db, &session, track_id, user_id)?;

        let element = db
            .exec(QueryBuilder::select().ids(session_id).query())?
            .elements
            .into_iter()
            .next()
            .expect("playback session element");

        assert_eq!(
            element_value(&element, "state"),
            Some(DbValue::from("playing"))
        );
        Ok(())
    }

    #[test]
    fn get_by_id_returns_none_for_missing() -> anyhow::Result<()> {
        let db = new_test_db()?;
        let result = get_by_id(&db, DbId(999999))?;
        assert!(result.is_none());
        Ok(())
    }

    #[test]
    fn get_track_id_resolves_linked_track() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_id = create_test_user(&mut db)?;
        let track_id = create_test_track(&mut db, "Linked Track")?;

        let session = make_session(1000);
        let session_id = create(&mut db, &session, track_id, user_id)?;

        let resolved = get_track_id(&db, session_id)?;
        assert_eq!(resolved, Some(track_id));
        Ok(())
    }

    #[test]
    fn get_user_id_resolves_linked_user() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_id = create_test_user(&mut db)?;
        let track_id = create_test_track(&mut db, "Track")?;

        let session = make_session(1000);
        let session_id = create(&mut db, &session, track_id, user_id)?;

        let resolved = get_user_id(&db, session_id)?;
        assert_eq!(resolved, Some(user_id));
        Ok(())
    }

    #[test]
    fn activity_meets_listen_threshold_normal_track() {
        // 30s is the threshold for tracks >= 3 minutes
        assert!(!activity_meets_listen_threshold(29_999, Some(300_000)));
        assert!(activity_meets_listen_threshold(30_000, Some(300_000)));
    }

    #[test]
    fn activity_meets_listen_threshold_short_track() {
        // For a 60s track, threshold is 20% = 12s
        assert!(!activity_meets_listen_threshold(11_999, Some(60_000)));
        assert!(activity_meets_listen_threshold(12_000, Some(60_000)));
    }

    #[test]
    fn activity_meets_listen_threshold_none_duration() {
        // No duration falls back to MIN_ACTIVITY_MS (30s)
        assert!(!activity_meets_listen_threshold(29_999, None));
        assert!(activity_meets_listen_threshold(30_000, None));
    }

    #[test]
    fn playback_state_is_terminal() {
        assert!(!PlaybackState::Playing.is_terminal());
        assert!(!PlaybackState::Paused.is_terminal());
        assert!(!PlaybackState::Buffering.is_terminal());
        assert!(PlaybackState::Stopped.is_terminal());
        assert!(PlaybackState::Completed.is_terminal());
    }

    #[test]
    fn evict_low_activity_removes_stale_low_activity_sessions() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_id = create_test_user(&mut db)?;
        let track_id = create_test_track(&mut db, "Track")?;

        let now_ms = 1_000_000;
        let stale_time = now_ms - STALE_TTL_MS - 1;

        // Create a stale session with low activity
        let session = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            position_ms: 1000,
            duration_ms: Some(300_000),
            activity_ms: Some(100),
            last_position_ms: None,
            state: PlaybackState::Playing,
            listen_recorded: None,
            updated_at_ms: stale_time,
            created_at_ms: stale_time,
        };
        let session_id = create(&mut db, &session, track_id, user_id)?;

        let evicted = evict_low_activity(&mut db, now_ms, |_| false)?;
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].track_db_id, track_id);
        assert_eq!(evicted[0].user_db_id, user_id);

        // Session should be gone
        assert!(get_by_id(&db, session_id)?.is_none());
        Ok(())
    }

    #[test]
    fn evict_low_activity_keeps_recent_sessions() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_id = create_test_user(&mut db)?;
        let track_id = create_test_track(&mut db, "Track")?;

        let now_ms = 1_000_000;

        // Create a recent session with low activity (should not be evicted)
        let session = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            position_ms: 100,
            duration_ms: Some(300_000),
            activity_ms: Some(100),
            last_position_ms: None,
            state: PlaybackState::Playing,
            listen_recorded: None,
            updated_at_ms: now_ms,
            created_at_ms: now_ms,
        };
        let session_id = create(&mut db, &session, track_id, user_id)?;

        let evicted = evict_low_activity(&mut db, now_ms, |_| false)?;
        assert!(evicted.is_empty());

        // Session should still exist
        assert!(get_by_id(&db, session_id)?.is_some());
        Ok(())
    }

    #[test]
    fn evict_low_activity_keeps_sessions_meeting_threshold() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_id = create_test_user(&mut db)?;
        let track_id = create_test_track(&mut db, "Track")?;

        let now_ms = 1_000_000;
        let stale_time = now_ms - STALE_TTL_MS - 1;

        // Create a stale session with enough activity (should not be evicted)
        let session = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            position_ms: 60_000,
            duration_ms: Some(300_000),
            activity_ms: Some(30_000),
            last_position_ms: None,
            state: PlaybackState::Playing,
            listen_recorded: None,
            updated_at_ms: stale_time,
            created_at_ms: stale_time,
        };
        let session_id = create(&mut db, &session, track_id, user_id)?;

        let evicted = evict_low_activity(&mut db, now_ms, |_| false)?;
        assert!(evicted.is_empty());

        // Session should still exist
        assert!(get_by_id(&db, session_id)?.is_some());
        Ok(())
    }

    #[test]
    fn evict_low_activity_skips_active_sessions() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_id = create_test_user(&mut db)?;
        let track_id = create_test_track(&mut db, "Track")?;

        let now_ms = 1_000_000;
        let stale_time = now_ms - STALE_TTL_MS - 1;

        let session = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            position_ms: 100,
            duration_ms: Some(300_000),
            activity_ms: Some(100),
            last_position_ms: None,
            state: PlaybackState::Playing,
            listen_recorded: None,
            updated_at_ms: stale_time,
            created_at_ms: stale_time,
        };
        let session_id = create(&mut db, &session, track_id, user_id)?;

        // Mark it as active via the callback
        let evicted = evict_low_activity(&mut db, now_ms, |id| id == session_id)?;
        assert!(evicted.is_empty());

        // Session should still exist
        assert!(get_by_id(&db, session_id)?.is_some());
        Ok(())
    }
}
