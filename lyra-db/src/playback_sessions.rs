// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        HashMap,
        HashSet,
    },
    fmt,
};

use agdb::{
    CountComparison,
    DbAny,
    DbElement,
    DbError,
    DbId,
    DbTypeMarker,
    DbValue,
    QueryBuilder,
};
use serde::{
    Deserialize,
    Serialize,
};

use super::tracks::get_by_id as get_track_by_id;
use super::users::get_by_id as get_user_by_id;

const STALE_TTL_MS: u64 = 5 * 60 * 1000;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, DbTypeMarker)]
#[serde(rename_all = "lowercase")]
pub enum PlaybackState {
    Playing,
    Paused,
    Stopped,
    Buffering,
    Completed,
}

impl PlaybackState {
    pub fn is_terminal(self) -> bool {
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
            _ => Err(DbError::serialization(
                agdb::DbErrorType::TypeError,
                format!("invalid PlaybackState value '{value}'"),
            )),
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

#[derive(DbElement, Clone, Debug, PartialEq, Eq)]
pub struct PlaybackSession {
    pub db_id: Option<DbId>,
    pub id: String,
    /// The client's id for the play this session tracks; one play per session.
    pub play_id: String,
    pub client_name: Option<String>,
    /// The plugin that reported the play; `None` for Lyra's own clients.
    pub plugin_id: Option<String>,
    pub position_ms: u64,
    pub duration_ms: Option<u64>,
    /// Playing time credited to the play so far.
    pub played_ms: u64,
    /// The position and time of the latest report that carried a position.
    pub last_position_ms: u64,
    pub last_position_at_ms: u64,
    /// Playing time credited beyond the server's elapsed time, for report latency.
    pub latency_allowance_used_ms: u64,
    pub state: PlaybackState,
    pub updated_at_ms: u64,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug)]
pub struct EvictedPlayback {
    pub playback: PlaybackSession,
    pub track_db_id: DbId,
    pub user_db_id: DbId,
}

#[derive(Clone, Copy, Debug)]
pub struct PlaybackSessionListProjection {
    pub state: PlaybackState,
    pub updated_at_ms: u64,
}

pub fn get_track_id(
    db: &impl super::DbAccess,
    playback_session_id: DbId,
) -> anyhow::Result<Option<DbId>> {
    for edge in super::graph::direct_edges_from(db, playback_session_id)? {
        let target_id = edge.to;
        if target_id.0 == 0 {
            continue;
        }
        if get_track_by_id(db, target_id)?.is_some() {
            return Ok(Some(target_id));
        }
    }

    Ok(None)
}

pub fn get_user_id(
    db: &impl super::DbAccess,
    playback_session_id: DbId,
) -> anyhow::Result<Option<DbId>> {
    for edge in super::graph::direct_edges_from(db, playback_session_id)? {
        let target_id = edge.to;
        if target_id.0 == 0 {
            continue;
        }
        if get_user_by_id(db, target_id)?.is_some() {
            return Ok(Some(target_id));
        }
    }

    Ok(None)
}

#[cfg(any(test, feature = "testing"))]
pub fn create(
    db: &mut DbAny,
    playback_session: &PlaybackSession,
    track_db_id: DbId,
    user_db_id: DbId,
) -> anyhow::Result<DbId> {
    db.transaction_mut(|t| insert(t, playback_session, track_db_id, user_db_id))
}

pub fn insert(
    db: &mut impl super::DbAccess,
    playback_session: &PlaybackSession,
    track_db_id: DbId,
    user_db_id: DbId,
) -> anyhow::Result<DbId> {
    let playback_session_id = db
        .exec_mut(QueryBuilder::insert().element(playback_session).query())?
        .ids()[0];

    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from("playback_sessions")
            .to(playback_session_id)
            .query(),
    )?;
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(playback_session_id)
            .to(track_db_id)
            .query(),
    )?;
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(playback_session_id)
            .to(user_db_id)
            .query(),
    )?;

    Ok(playback_session_id)
}

/// The user's session for the play, if one was ever started.
pub fn find_by_play_id(
    db: &impl super::DbAccess,
    user_db_id: DbId,
    play_id: &str,
) -> anyhow::Result<Option<DbId>> {
    super::graph::find_owned_by_index(db, "play_id", play_id, "PlaybackSession", user_db_id)
}

pub fn get_by_id(
    db: &impl super::DbAccess,
    playback_session_id: DbId,
) -> anyhow::Result<Option<PlaybackSession>> {
    super::graph::fetch_typed_by_id(db, playback_session_id, "PlaybackSession")
}

pub fn get_list_projections_by_ids(
    db: &impl super::DbAccess,
    playback_session_ids: Vec<DbId>,
) -> anyhow::Result<HashMap<DbId, PlaybackSessionListProjection>> {
    if playback_session_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let typed_ids = db
        .exec(
            QueryBuilder::select()
                .values(vec![DbValue::from("db_element_id")])
                .ids(playback_session_ids)
                .query(),
        )?
        .elements
        .into_iter()
        .filter_map(|element| {
            element
                .values
                .iter()
                .any(|kv| {
                    kv.key == DbValue::from("db_element_id")
                        && kv.value == DbValue::from("PlaybackSession")
                })
                .then_some(element.id)
        })
        .collect::<Vec<_>>();
    if typed_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let elements = db.exec(
        QueryBuilder::select()
            .values(vec![DbValue::from("state"), DbValue::from("updated_at_ms")])
            .ids(typed_ids)
            .query(),
    )?;
    let mut projections = HashMap::with_capacity(elements.elements.len());
    for element in elements.elements {
        let value = |key: &str| {
            element
                .values
                .iter()
                .find(|kv| kv.key == DbValue::from(key))
                .map(|kv| kv.value.clone())
                .ok_or_else(|| anyhow::anyhow!("playback session {} missing {key}", element.id.0))
        };
        let state = PlaybackState::try_from(value("state")?)?;
        let updated_at_ms = match value("updated_at_ms")? {
            DbValue::U64(value) => value,
            value => anyhow::bail!(
                "playback session {} has invalid updated_at_ms: {value:?}",
                element.id.0
            ),
        };
        projections.insert(
            element.id,
            PlaybackSessionListProjection {
                state,
                updated_at_ms,
            },
        );
    }
    Ok(projections)
}

pub fn track_ids_for_sessions(
    db: &impl super::DbAccess,
    playback_session_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, DbId>> {
    if playback_session_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let session_ids = playback_session_ids.iter().copied().collect::<HashSet<_>>();
    let edges = db.exec(
        QueryBuilder::select()
            .search()
            .from("playback_sessions")
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(3))
            .and()
            .not_beyond()
            .where_()
            .key("db_element_id")
            .value("PlaybackSession")
            .and()
            .not()
            .ids(playback_session_ids.to_vec())
            .end_where()
            .and()
            .not_beyond()
            .distance(CountComparison::Equal(3))
            .query(),
    )?;
    let candidates = edges
        .elements
        .into_iter()
        .filter_map(|edge| {
            (edge.id.0 < 0 && session_ids.contains(&edge.from) && edge.to.0 > 0)
                .then_some((edge.from, edge.to))
        })
        .collect::<Vec<_>>();
    let target_ids = candidates
        .iter()
        .map(|(_, target)| *target)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let typed = if target_ids.is_empty() {
        Vec::new()
    } else {
        db.exec(
            QueryBuilder::select()
                .values(vec![DbValue::from("db_element_id")])
                .ids(target_ids)
                .query(),
        )?
        .elements
    };
    let track_ids = typed
        .into_iter()
        .filter_map(|element| {
            element
                .values
                .iter()
                .any(|kv| {
                    kv.key == DbValue::from("db_element_id") && kv.value == DbValue::from("Track")
                })
                .then_some(element.id)
        })
        .collect::<HashSet<_>>();
    Ok(candidates
        .into_iter()
        .filter_map(|(session_id, target_id)| {
            track_ids
                .contains(&target_id)
                .then_some((session_id, target_id))
        })
        .collect())
}

pub fn get(db: &DbAny) -> anyhow::Result<Vec<PlaybackSession>> {
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

/// Removes sessions untouched for longer than the stale TTL, except a playback's current session
/// and those `is_active` holds for.
pub fn evict_stale(
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

        let Some(playback_session_id) = playback_session.db_id else {
            continue;
        };
        if is_active(playback_session_id)
            || super::playbacks::is_current_session(db, playback_session_id)?
        {
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

pub fn update(
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
    use crate::test_db::new_test_db;
    use crate::tracks::Track;
    use crate::users::User;
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
            track_gain_db: None,
            album_gain_db: None,
            locked: None,
            created_at_ms: None,
            file_created_at_ms: None,
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
            play_id: nanoid!(),
            client_name: None,
            plugin_id: None,
            position_ms: 0,
            duration_ms: Some(300_000),
            played_ms: 0,
            last_position_ms: 0,
            state: PlaybackState::Playing,
            latency_allowance_used_ms: 0,
            last_position_at_ms: now_ms,
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
        let projection = get_list_projections_by_ids(&db, vec![session_id])?
            .remove(&session_id)
            .expect("session projection should exist");
        assert_eq!(projection.state, PlaybackState::Playing);
        assert_eq!(projection.updated_at_ms, 1000);
        assert_eq!(
            track_ids_for_sessions(&db, &[session_id])?.get(&session_id),
            Some(&track_id)
        );
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
    fn find_by_play_id_matches_only_the_users_session() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_id = create_test_user(&mut db)?;
        let other_user_id = crate::users::create(&mut db, &crate::fixtures::test_user("other")?)?;
        let track_id = create_test_track(&mut db, "Track")?;
        let session = make_session(1000);
        let session_id = create(&mut db, &session, track_id, user_id)?;

        assert_eq!(
            find_by_play_id(&db, user_id, &session.play_id)?,
            Some(session_id)
        );
        assert_eq!(find_by_play_id(&db, other_user_id, &session.play_id)?, None);
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
    fn playback_state_is_terminal() {
        assert!(!PlaybackState::Playing.is_terminal());
        assert!(!PlaybackState::Paused.is_terminal());
        assert!(!PlaybackState::Buffering.is_terminal());
        assert!(PlaybackState::Stopped.is_terminal());
        assert!(PlaybackState::Completed.is_terminal());
    }

    #[test]
    fn evict_stale_removes_stale_sessions() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_id = create_test_user(&mut db)?;
        let track_id = create_test_track(&mut db, "Track")?;

        let now_ms = 1_000_000;
        let stale_time = now_ms - STALE_TTL_MS - 1;

        let session = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            play_id: nanoid!(),
            client_name: None,
            plugin_id: None,
            position_ms: 1000,
            duration_ms: Some(300_000),
            played_ms: 100,
            last_position_ms: 0,
            state: PlaybackState::Playing,
            latency_allowance_used_ms: 0,
            last_position_at_ms: stale_time,
            updated_at_ms: stale_time,
            created_at_ms: stale_time,
        };
        let session_id = create(&mut db, &session, track_id, user_id)?;

        let evicted = evict_stale(&mut db, now_ms, |_| false)?;
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].track_db_id, track_id);
        assert_eq!(evicted[0].user_db_id, user_id);

        // Session should be gone
        assert!(get_by_id(&db, session_id)?.is_none());
        Ok(())
    }

    #[test]
    fn evict_stale_keeps_recent_sessions() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_id = create_test_user(&mut db)?;
        let track_id = create_test_track(&mut db, "Track")?;

        let now_ms = 1_000_000;

        let session = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            play_id: nanoid!(),
            client_name: None,
            plugin_id: None,
            position_ms: 100,
            duration_ms: Some(300_000),
            played_ms: 100,
            last_position_ms: 0,
            state: PlaybackState::Playing,
            latency_allowance_used_ms: 0,
            last_position_at_ms: now_ms,
            updated_at_ms: now_ms,
            created_at_ms: now_ms,
        };
        let session_id = create(&mut db, &session, track_id, user_id)?;

        let evicted = evict_stale(&mut db, now_ms, |_| false)?;
        assert!(evicted.is_empty());

        // Session should still exist
        assert!(get_by_id(&db, session_id)?.is_some());
        Ok(())
    }

    #[test]
    fn evict_stale_skips_active_sessions() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_id = create_test_user(&mut db)?;
        let track_id = create_test_track(&mut db, "Track")?;

        let now_ms = 1_000_000;
        let stale_time = now_ms - STALE_TTL_MS - 1;

        let session = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            play_id: nanoid!(),
            client_name: None,
            plugin_id: None,
            position_ms: 100,
            duration_ms: Some(300_000),
            played_ms: 100,
            last_position_ms: 0,
            state: PlaybackState::Playing,
            latency_allowance_used_ms: 0,
            last_position_at_ms: stale_time,
            updated_at_ms: stale_time,
            created_at_ms: stale_time,
        };
        let session_id = create(&mut db, &session, track_id, user_id)?;

        let evicted = evict_stale(&mut db, now_ms, |id| id == session_id)?;
        assert!(evicted.is_empty());

        // Session should still exist
        assert!(get_by_id(&db, session_id)?.is_some());
        Ok(())
    }
}
