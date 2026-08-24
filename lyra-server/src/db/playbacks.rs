// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::fmt;

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

use super::DbAccess;

const OWNER_EDGE_KEY: &str = "owner";
const CURRENT_SESSION_EDGE_KEY: &str = "current_session";
pub(crate) const MAX_PLAYBACKS_PER_USER: usize = 10_000;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, DbTypeMarker)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RepeatMode {
    None,
    One,
    All,
}

impl RepeatMode {
    fn as_db_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::One => "one",
            Self::All => "all",
        }
    }

    fn from_db_str(value: &str) -> Result<Self, DbError> {
        match value {
            "none" => Ok(Self::None),
            "one" => Ok(Self::One),
            "all" => Ok(Self::All),
            _ => Err(DbError::serialization(
                agdb::DbErrorType::TypeError,
                format!("invalid RepeatMode value '{value}'"),
            )),
        }
    }
}

impl fmt::Display for RepeatMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_db_str())
    }
}

impl From<RepeatMode> for DbValue {
    fn from(value: RepeatMode) -> Self {
        Self::from(value.as_db_str())
    }
}

impl From<&RepeatMode> for DbValue {
    fn from(value: &RepeatMode) -> Self {
        (*value).into()
    }
}

impl TryFrom<DbValue> for RepeatMode {
    type Error = DbError;

    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        Self::from_db_str(value.string()?)
    }
}

#[derive(DbElement, Clone, Debug)]
pub(crate) struct Playback {
    pub(crate) db_id: Option<DbId>,
    pub(crate) id: String,
    pub(crate) queue_revision: u64,
    pub(crate) track_ids: Vec<String>,
    pub(crate) current_index: u64,
    pub(crate) repeat_mode: RepeatMode,
    pub(crate) shuffle_enabled: bool,
    pub(crate) created_at_ms: u64,
    pub(crate) updated_at_ms: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct PlaybackListProjection {
    pub(crate) db_id: DbId,
    pub(crate) id: String,
    pub(crate) queue_revision: u64,
    pub(crate) updated_at_ms: u64,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReplaceQueueError {
    #[error("playback not found")]
    NotFound,
    #[error("queue revision conflict: expected {expected_revision}, current {current_revision}")]
    RevisionConflict {
        expected_revision: u64,
        current_revision: u64,
    },
    #[error("queue revision exhausted")]
    RevisionExhausted,
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
    #[error(transparent)]
    Database(#[from] DbError),
}

#[cfg(test)]
pub(crate) fn create(
    db: &mut DbAny,
    playback: &Playback,
    owner_db_id: DbId,
    current_session_db_id: DbId,
) -> anyhow::Result<DbId> {
    db.transaction_mut(|t| insert(t, playback, owner_db_id, current_session_db_id))
}

pub(crate) fn insert(
    db: &mut impl DbAccess,
    playback: &Playback,
    owner_db_id: DbId,
    current_session_db_id: DbId,
) -> anyhow::Result<DbId> {
    let playback_db_id = db
        .exec_mut(QueryBuilder::insert().element(playback).query())?
        .ids()[0];
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from("playbacks")
            .to(playback_db_id)
            .query(),
    )?;

    insert_tagged_edge(db, playback_db_id, owner_db_id, OWNER_EDGE_KEY)?;
    insert_tagged_edge(
        db,
        playback_db_id,
        current_session_db_id,
        CURRENT_SESSION_EDGE_KEY,
    )?;
    Ok(playback_db_id)
}

fn insert_tagged_edge(
    db: &mut impl DbAccess,
    from: DbId,
    to: DbId,
    key: &str,
) -> Result<(), DbError> {
    let edge_id = db
        .exec_mut(QueryBuilder::insert().edges().from(from).to(to).query())?
        .ids()[0];
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform([(key, 1_u64).into()])
            .ids(edge_id)
            .query(),
    )?;
    Ok(())
}

fn tagged_edge(
    db: &impl DbAccess,
    playback_db_id: DbId,
    key: &str,
) -> Result<Option<(DbId, DbId)>, DbError> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .from(playback_db_id)
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(1))
            .and()
            .keys(key)
            .query(),
    )?;
    Ok(result
        .elements
        .into_iter()
        .find(|element| element.id.0 < 0 && element.to.0 > 0)
        .map(|element| (element.id, element.to)))
}

pub(crate) fn get_by_id(
    db: &impl DbAccess,
    playback_db_id: DbId,
) -> anyhow::Result<Option<Playback>> {
    super::graph::fetch_typed_by_id(db, playback_db_id, "Playback")
}

pub(crate) fn list_projections_for_user(
    db: &impl DbAccess,
    user_db_id: DbId,
) -> anyhow::Result<Vec<PlaybackListProjection>> {
    let keys = ["db_element_id", "id", "queue_revision", "updated_at_ms"]
        .into_iter()
        .map(DbValue::from)
        .collect::<Vec<_>>();
    let result = db.exec(
        QueryBuilder::select()
            .values(keys)
            .search()
            .to(user_db_id)
            .limit((MAX_PLAYBACKS_PER_USER + 1) as u64)
            .where_()
            .distance(CountComparison::Equal(2))
            .and()
            .node()
            .and()
            .key("db_element_id")
            .value("Playback")
            .end_where()
            .query(),
    )?;
    let mut projections = Vec::new();
    for element in result.elements {
        if let Some(projection) = projection_from_element(&element)? {
            projections.push(projection);
        }
    }
    Ok(projections)
}

pub(crate) fn count_for_user_up_to_limit(
    db: &impl DbAccess,
    user_db_id: DbId,
) -> anyhow::Result<usize> {
    let result = db.exec(
        QueryBuilder::select()
            .values(vec![DbValue::from("db_element_id")])
            .search()
            .to(user_db_id)
            .limit(MAX_PLAYBACKS_PER_USER as u64)
            .where_()
            .distance(CountComparison::Equal(2))
            .and()
            .node()
            .and()
            .key("db_element_id")
            .value("Playback")
            .end_where()
            .query(),
    )?;
    Ok(result.elements.len())
}

fn projection_from_element(
    element: &agdb::DbElement,
) -> anyhow::Result<Option<PlaybackListProjection>> {
    if element.id.0 <= 0
        || !element.values.iter().any(|kv| {
            kv.key == DbValue::from("db_element_id") && kv.value == DbValue::from("Playback")
        })
    {
        return Ok(None);
    }
    let value = |key: &str| {
        element
            .values
            .iter()
            .find(|kv| kv.key == DbValue::from(key))
            .map(|kv| kv.value.clone())
            .ok_or_else(|| anyhow::anyhow!("playback {} missing {key}", element.id.0))
    };
    let id = value("id")?.string()?.clone();
    let queue_revision = match value("queue_revision")? {
        DbValue::U64(value) => value,
        value => anyhow::bail!(
            "playback {} has invalid queue_revision: {value:?}",
            element.id.0
        ),
    };
    let updated_at_ms = match value("updated_at_ms")? {
        DbValue::U64(value) => value,
        value => anyhow::bail!(
            "playback {} has invalid updated_at_ms: {value:?}",
            element.id.0
        ),
    };
    Ok(Some(PlaybackListProjection {
        db_id: element.id,
        id,
        queue_revision,
        updated_at_ms,
    }))
}

pub(crate) fn get_projection_by_id(
    db: &impl DbAccess,
    playback_db_id: DbId,
) -> anyhow::Result<Option<PlaybackListProjection>> {
    let typed = db.exec(
        QueryBuilder::select()
            .values(vec![DbValue::from("db_element_id")])
            .ids(playback_db_id)
            .query(),
    )?;
    if !typed.elements.into_iter().any(|element| {
        element.values.iter().any(|kv| {
            kv.key == DbValue::from("db_element_id") && kv.value == DbValue::from("Playback")
        })
    }) {
        return Ok(None);
    }
    let result = db.exec(
        QueryBuilder::select()
            .values(vec![
                DbValue::from("db_element_id"),
                DbValue::from("id"),
                DbValue::from("queue_revision"),
                DbValue::from("updated_at_ms"),
            ])
            .ids(playback_db_id)
            .query(),
    )?;
    let Some(element) = result.elements.into_iter().next() else {
        return Ok(None);
    };
    projection_from_element(&element)
}

pub(crate) fn current_session_ids(
    db: &impl DbAccess,
    playback_ids: &[DbId],
) -> anyhow::Result<std::collections::HashMap<DbId, DbId>> {
    if playback_ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let playback_ids = playback_ids
        .iter()
        .copied()
        .collect::<std::collections::HashSet<_>>();
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .from("playbacks")
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(3))
            .and()
            .keys(CURRENT_SESSION_EDGE_KEY)
            .and()
            .not_beyond()
            .where_()
            .key("db_element_id")
            .value("Playback")
            .and()
            .not()
            .ids(playback_ids.iter().copied().collect::<Vec<_>>())
            .end_where()
            .and()
            .not_beyond()
            .distance(CountComparison::Equal(3))
            .query(),
    )?;
    Ok(result
        .elements
        .into_iter()
        .filter_map(|element| {
            (element.id.0 < 0 && playback_ids.contains(&element.from) && element.to.0 > 0)
                .then_some((element.from, element.to))
        })
        .collect())
}

pub(crate) fn get_owner_id(
    db: &impl DbAccess,
    playback_db_id: DbId,
) -> Result<Option<DbId>, DbError> {
    Ok(tagged_edge(db, playback_db_id, OWNER_EDGE_KEY)?.map(|(_, target)| target))
}

pub(crate) fn get_current_session_id(
    db: &impl DbAccess,
    playback_db_id: DbId,
) -> Result<Option<DbId>, DbError> {
    Ok(tagged_edge(db, playback_db_id, CURRENT_SESSION_EDGE_KEY)?.map(|(_, target)| target))
}

pub(crate) fn link_current_session(
    db: &mut impl DbAccess,
    playback_db_id: DbId,
    current_session_db_id: DbId,
    updated_at_ms: u64,
) -> anyhow::Result<()> {
    remove_current_session_edge(db, playback_db_id)?;
    insert_tagged_edge(
        db,
        playback_db_id,
        current_session_db_id,
        CURRENT_SESSION_EDGE_KEY,
    )?;
    touch(db, playback_db_id, updated_at_ms)
}

pub(crate) fn touch(
    db: &mut impl DbAccess,
    playback_db_id: DbId,
    updated_at_ms: u64,
) -> anyhow::Result<()> {
    let mut playback =
        get_by_id(db, playback_db_id)?.ok_or_else(|| anyhow::anyhow!("playback not found"))?;
    playback.updated_at_ms = updated_at_ms;
    db.exec_mut(QueryBuilder::insert().element(&playback).query())?;
    Ok(())
}

fn remove_current_session_edge(
    db: &mut impl DbAccess,
    playback_db_id: DbId,
) -> Result<(), DbError> {
    if let Some((edge_id, _)) = tagged_edge(db, playback_db_id, CURRENT_SESSION_EDGE_KEY)? {
        db.exec_mut(QueryBuilder::remove().ids(edge_id).query())?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn replace_queue(
    db: &mut DbAny,
    playback_db_id: DbId,
    expected_revision: u64,
    track_ids: Vec<String>,
    current_index: u64,
    repeat_mode: RepeatMode,
    shuffle_enabled: bool,
    updated_at_ms: u64,
    clear_current_session: bool,
) -> Result<Playback, ReplaceQueueError> {
    db.transaction_mut(|t| {
        replace_queue_in_transaction(
            t,
            playback_db_id,
            expected_revision,
            track_ids,
            current_index,
            repeat_mode,
            shuffle_enabled,
            updated_at_ms,
            clear_current_session,
        )
    })
}

pub(crate) fn replace_queue_in_transaction(
    db: &mut impl DbAccess,
    playback_db_id: DbId,
    expected_revision: u64,
    track_ids: Vec<String>,
    current_index: u64,
    repeat_mode: RepeatMode,
    shuffle_enabled: bool,
    updated_at_ms: u64,
    clear_current_session: bool,
) -> Result<Playback, ReplaceQueueError> {
    let mut playback = get_by_id(db, playback_db_id)
        .map_err(ReplaceQueueError::Internal)?
        .ok_or(ReplaceQueueError::NotFound)?;
    if playback.queue_revision != expected_revision {
        return Err(ReplaceQueueError::RevisionConflict {
            expected_revision,
            current_revision: playback.queue_revision,
        });
    }
    playback.queue_revision = playback
        .queue_revision
        .checked_add(1)
        .ok_or(ReplaceQueueError::RevisionExhausted)?;
    playback.track_ids = track_ids;
    playback.current_index = current_index;
    playback.repeat_mode = repeat_mode;
    playback.shuffle_enabled = shuffle_enabled;
    playback.updated_at_ms = updated_at_ms;
    db.exec_mut(QueryBuilder::insert().element(&playback).query())?;
    if clear_current_session {
        remove_current_session_edge(db, playback_db_id)?;
    }
    Ok(playback)
}

pub(crate) fn delete(db: &mut DbAny, playback_db_id: DbId) -> anyhow::Result<()> {
    db.exec_mut(QueryBuilder::remove().ids(playback_db_id).query())?;
    Ok(())
}

pub(crate) fn delete_for_user(db: &mut impl DbAccess, user_db_id: DbId) -> anyhow::Result<u64> {
    let mut count = 0_u64;
    loop {
        let playback_ids = list_projections_for_user(db, user_db_id)?
            .into_iter()
            .map(|projection| projection.db_id)
            .collect::<Vec<_>>();
        if playback_ids.is_empty() {
            break;
        }
        count = count.saturating_add(playback_ids.len() as u64);
        db.exec_mut(QueryBuilder::remove().ids(playback_ids).query())?;
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use nanoid::nanoid;

    use super::*;
    use crate::db::{
        PlaybackSession,
        PlaybackState,
        test_db::{
            insert_track,
            new_test_db,
            test_user,
        },
    };

    fn setup() -> anyhow::Result<(DbAny, DbId, DbId)> {
        let mut db = new_test_db()?;
        let user_id = crate::db::users::create(&mut db, &test_user("playback-owner")?)?;
        let track_id = insert_track(&mut db, "Track")?;
        let session_id = crate::db::playback_sessions::create(
            &mut db,
            &PlaybackSession {
                db_id: None,
                id: nanoid!(),
                client_name: None,
                position_ms: 0,
                duration_ms: None,
                activity_ms: Some(0),
                last_position_ms: Some(0),
                state: PlaybackState::Playing,
                listen_recorded: None,
                updated_at_ms: 1,
                created_at_ms: 1,
            },
            track_id,
            user_id,
        )?;
        Ok((db, user_id, session_id))
    }

    fn playback() -> Playback {
        Playback {
            db_id: None,
            id: nanoid!(),
            queue_revision: 1,
            track_ids: vec!["track".to_string()],
            current_index: 0,
            repeat_mode: RepeatMode::None,
            shuffle_enabled: false,
            created_at_ms: 1,
            updated_at_ms: 1,
        }
    }

    #[test]
    fn repeat_mode_uses_stable_string_db_values() -> anyhow::Result<()> {
        assert_eq!(DbValue::from(RepeatMode::All), DbValue::from("all"));
        assert_eq!(RepeatMode::try_from(DbValue::from("one"))?, RepeatMode::One);
        assert!(RepeatMode::try_from(DbValue::from("invalid")).is_err());
        Ok(())
    }

    #[test]
    fn create_links_owner_and_current_session() -> anyhow::Result<()> {
        let (mut db, user_id, session_id) = setup()?;
        let playback_id = create(&mut db, &playback(), user_id, session_id)?;
        assert_eq!(get_owner_id(&db, playback_id)?, Some(user_id));
        assert_eq!(get_current_session_id(&db, playback_id)?, Some(session_id));
        assert_eq!(
            current_session_ids(&db, &[playback_id])?.get(&playback_id),
            Some(&session_id)
        );
        Ok(())
    }

    #[test]
    fn queue_replace_is_atomic_compare_and_swap() -> anyhow::Result<()> {
        let (mut db, user_id, session_id) = setup()?;
        let playback_id = create(&mut db, &playback(), user_id, session_id)?;
        let updated = replace_queue(
            &mut db,
            playback_id,
            1,
            vec!["updated".to_string()],
            0,
            RepeatMode::All,
            true,
            2,
            false,
        )?;
        assert_eq!(updated.queue_revision, 2);

        let error = replace_queue(
            &mut db,
            playback_id,
            1,
            vec!["stale".to_string()],
            0,
            RepeatMode::None,
            false,
            3,
            false,
        )
        .expect_err("stale revision must fail");
        assert!(matches!(
            error,
            ReplaceQueueError::RevisionConflict {
                expected_revision: 1,
                current_revision: 2,
            }
        ));
        let stored = get_by_id(&db, playback_id)?.unwrap();
        assert_eq!(stored.track_ids, vec!["updated"]);
        assert_eq!(stored.repeat_mode, RepeatMode::All);
        assert!(stored.shuffle_enabled);
        Ok(())
    }

    #[test]
    fn queue_replace_can_detach_current_session() -> anyhow::Result<()> {
        let (mut db, user_id, session_id) = setup()?;
        let playback_id = create(&mut db, &playback(), user_id, session_id)?;
        replace_queue(
            &mut db,
            playback_id,
            1,
            vec!["track".to_string()],
            0,
            RepeatMode::None,
            false,
            2,
            true,
        )?;
        assert_eq!(get_current_session_id(&db, playback_id)?, None);
        assert!(crate::db::playback_sessions::get_by_id(&db, session_id)?.is_some());
        Ok(())
    }

    #[test]
    fn user_playback_lookup_ignores_other_owned_resource_types() -> anyhow::Result<()> {
        let (mut db, user_id, session_id) = setup()?;
        let playback_id = create(&mut db, &playback(), user_id, session_id)?;
        let playlist_id = crate::db::playlists::create(
            &mut db,
            &crate::db::Playlist {
                db_id: None,
                id: nanoid!(),
                name: "Owned playlist".to_string(),
                description: None,
                is_public: None,
                created_at: None,
                updated_at: None,
            },
            user_id,
        )?;

        let playbacks = list_projections_for_user(&db, user_id)?;
        assert_eq!(playbacks.len(), 1);
        assert_eq!(playbacks[0].db_id, playback_id);
        assert_eq!(delete_for_user(&mut db, user_id)?, 1);
        assert!(crate::db::playlists::get_by_id(&db, playlist_id)?.is_some());
        Ok(())
    }
}
