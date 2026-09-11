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

mod reported;
pub(crate) use reported::{
    REPORTED_PLAYBACK_TTL_MS,
    ReportedPlaybackLimitReached,
    ReportedPrevious,
    ReportedSource,
    expire_reported,
    expire_reported_for_user,
    get_reported,
    insert_reported,
    update_reported_previous,
};

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

#[derive(Clone, Debug)]
pub(crate) struct Playback {
    pub(crate) db_id: Option<DbId>,
    pub(crate) id: String,
    pub(crate) queue: Option<Queue>,
    pub(crate) reported: Option<ReportedSource>,
    pub(crate) created_at_ms: u64,
    pub(crate) updated_at_ms: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct Queue {
    pub(crate) revision: u64,
    pub(crate) track_ids: Vec<String>,
    pub(crate) current_index: u64,
    pub(crate) repeat_mode: RepeatMode,
    pub(crate) shuffle_enabled: bool,
}

#[derive(agdb::DbType, Clone, Debug)]
struct PlaybackRecord {
    db_element_id: String,
    db_id: Option<DbId>,
    id: String,
    reported: Option<ReportedSource>,
    created_at_ms: u64,
    updated_at_ms: u64,
}

#[derive(DbElement, Clone, Debug)]
struct QueueRecord {
    db_id: Option<DbId>,
    revision: u64,
    track_ids: Vec<String>,
    current_index: u64,
    repeat_mode: RepeatMode,
    shuffle_enabled: bool,
}

impl QueueRecord {
    fn from_queue(queue: &Queue, db_id: Option<DbId>) -> Self {
        Self {
            db_id,
            revision: queue.revision,
            track_ids: queue.track_ids.clone(),
            current_index: queue.current_index,
            repeat_mode: queue.repeat_mode,
            shuffle_enabled: queue.shuffle_enabled,
        }
    }

    fn into_queue(self) -> Queue {
        Queue {
            revision: self.revision,
            track_ids: self.track_ids,
            current_index: self.current_index,
            repeat_mode: self.repeat_mode,
            shuffle_enabled: self.shuffle_enabled,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PlaybackListProjection {
    pub(crate) db_id: DbId,
    pub(crate) id: String,
    pub(crate) queue_revision: Option<u64>,
    pub(crate) updated_at_ms: u64,
    pub(crate) reported: bool,
}

const QUEUE_EDGE_KEY: &str = "queue";

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReplaceQueueError {
    #[error("playback not found")]
    NotFound,
    #[error("queue revision conflict: expected {expected_revision}, current {current_revision}")]
    RevisionConflict {
        expected_revision: u64,
        current_revision: u64,
    },
    #[error("playback has no server-managed queue")]
    QueueUnavailable,
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
    anyhow::ensure!(
        playback.queue.is_some() || playback.reported.is_some(),
        "playback requires a queue or reporting source"
    );
    let record = PlaybackRecord {
        db_element_id: "Playback".to_owned(),
        db_id: playback.db_id,
        id: playback.id.clone(),
        reported: playback.reported.clone(),
        created_at_ms: playback.created_at_ms,
        updated_at_ms: playback.updated_at_ms,
    };
    let playback_db_id = db
        .exec_mut(QueryBuilder::insert().element(&record).query())?
        .ids()[0];
    if let Some(queue) = &playback.queue {
        let queue_id = db
            .exec_mut(
                QueryBuilder::insert()
                    .element(QueueRecord::from_queue(queue, None))
                    .query(),
            )?
            .ids()[0];
        insert_tagged_edge(db, playback_db_id, queue_id, QUEUE_EDGE_KEY)?;
    }
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
    let Some(record) =
        super::graph::fetch_typed_by_id::<PlaybackRecord>(db, playback_db_id, "Playback")?
    else {
        return Ok(None);
    };
    let queue = match tagged_edge(db, playback_db_id, QUEUE_EDGE_KEY)? {
        Some((_, queue_id)) => Some(
            super::graph::fetch_typed_by_id::<QueueRecord>(db, queue_id, "QueueRecord")?
                .ok_or_else(|| anyhow::anyhow!("playback {} has invalid queue edge", record.id))?
                .into_queue(),
        ),
        None => None,
    };
    Ok(Some(Playback {
        db_id: record.db_id,
        id: record.id,
        queue,
        reported: record.reported,
        created_at_ms: record.created_at_ms,
        updated_at_ms: record.updated_at_ms,
    }))
}

pub(crate) fn list_projections_for_user(
    db: &impl DbAccess,
    user_db_id: DbId,
) -> anyhow::Result<Vec<PlaybackListProjection>> {
    let keys = ["db_element_id", "id", "reported", "updated_at_ms"]
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
    let ids = projections.iter().map(|p| p.db_id).collect::<Vec<_>>();
    let revisions = queue_revisions(db, &ids)?;
    for projection in &mut projections {
        projection.queue_revision = revisions.get(&projection.db_id).copied();
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
    let queue_revision = None;
    let reported = element
        .values
        .iter()
        .any(|kv| kv.key == DbValue::from("reported"));
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
        reported,
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
            .values(Vec::<DbValue>::new())
            .ids(playback_db_id)
            .query(),
    )?;
    let Some(element) = result.elements.into_iter().next() else {
        return Ok(None);
    };
    let mut projection = projection_from_element(&element)?;
    if let Some(projection) = &mut projection {
        projection.queue_revision = match tagged_edge(db, playback_db_id, QUEUE_EDGE_KEY)? {
            Some((_, queue_id)) => {
                let result = db.exec(
                    QueryBuilder::select()
                        .values(vec![DbValue::from("revision")])
                        .ids(queue_id)
                        .query(),
                )?;
                Some(
                    result
                        .elements
                        .first()
                        .and_then(|element| element.values.first())
                        .ok_or_else(|| anyhow::anyhow!("queue missing revision"))?
                        .value
                        .to_u64()?,
                )
            }
            None => None,
        };
    }
    Ok(projection)
}

pub(crate) fn current_session_ids(
    db: &impl DbAccess,
    playback_ids: &[DbId],
) -> anyhow::Result<std::collections::HashMap<DbId, DbId>> {
    tagged_targets(db, playback_ids, CURRENT_SESSION_EDGE_KEY)
}

fn tagged_targets(
    db: &impl DbAccess,
    playback_ids: &[DbId],
    key: &str,
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
            .keys(key)
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

fn queue_revisions(
    db: &impl DbAccess,
    playback_ids: &[DbId],
) -> anyhow::Result<std::collections::HashMap<DbId, u64>> {
    let queue_ids = tagged_targets(db, playback_ids, QUEUE_EDGE_KEY)?;
    if queue_ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let records = db.exec(
        QueryBuilder::select()
            .values(vec![DbValue::from("revision")])
            .ids(queue_ids.values().copied().collect::<Vec<_>>())
            .query(),
    )?;
    let revisions = records
        .elements
        .into_iter()
        .map(|element| {
            Ok((
                element.id,
                element
                    .values
                    .first()
                    .ok_or_else(|| anyhow::anyhow!("queue missing revision"))?
                    .value
                    .to_u64()?,
            ))
        })
        .collect::<anyhow::Result<std::collections::HashMap<_, _>>>()?;
    queue_ids
        .into_iter()
        .map(|(playback_id, queue_id)| {
            Ok((
                playback_id,
                *revisions
                    .get(&queue_id)
                    .ok_or_else(|| anyhow::anyhow!("queue missing revision"))?,
            ))
        })
        .collect()
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
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform([("updated_at_ms", updated_at_ms).into()])
            .ids(playback_db_id)
            .query(),
    )?;
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

pub(crate) struct QueueReplacement {
    pub(crate) expected_revision: u64,
    pub(crate) track_ids: Vec<String>,
    pub(crate) current_index: u64,
    pub(crate) repeat_mode: RepeatMode,
    pub(crate) shuffle_enabled: bool,
    pub(crate) updated_at_ms: u64,
    pub(crate) clear_current_session: bool,
}

#[cfg(test)]
pub(crate) fn replace_queue(
    db: &mut DbAny,
    playback_db_id: DbId,
    replacement: QueueReplacement,
) -> Result<Playback, ReplaceQueueError> {
    db.transaction_mut(|t| replace_queue_in_transaction(t, playback_db_id, replacement))
}

pub(crate) fn replace_queue_in_transaction(
    db: &mut impl DbAccess,
    playback_db_id: DbId,
    replacement: QueueReplacement,
) -> Result<Playback, ReplaceQueueError> {
    let QueueReplacement {
        expected_revision,
        track_ids,
        current_index,
        repeat_mode,
        shuffle_enabled,
        updated_at_ms,
        clear_current_session,
    } = replacement;
    let mut playback = get_by_id(db, playback_db_id)
        .map_err(ReplaceQueueError::Internal)?
        .ok_or(ReplaceQueueError::NotFound)?;
    let queue = playback
        .queue
        .as_mut()
        .ok_or(ReplaceQueueError::QueueUnavailable)?;
    if queue.revision != expected_revision {
        return Err(ReplaceQueueError::RevisionConflict {
            expected_revision,
            current_revision: queue.revision,
        });
    }
    queue.revision = queue
        .revision
        .checked_add(1)
        .ok_or(ReplaceQueueError::RevisionExhausted)?;
    queue.track_ids = track_ids;
    queue.current_index = current_index;
    queue.repeat_mode = repeat_mode;
    queue.shuffle_enabled = shuffle_enabled;
    playback.updated_at_ms = updated_at_ms;
    let (_, queue_id) = tagged_edge(db, playback_db_id, QUEUE_EDGE_KEY)?
        .ok_or(ReplaceQueueError::QueueUnavailable)?;
    db.exec_mut(
        QueryBuilder::insert()
            .element(QueueRecord::from_queue(queue, Some(queue_id)))
            .query(),
    )?;
    touch(db, playback_db_id, updated_at_ms)?;
    if clear_current_session {
        remove_current_session_edge(db, playback_db_id)?;
    }
    Ok(playback)
}

pub(crate) fn delete(db: &mut DbAny, playback_db_id: DbId) -> anyhow::Result<()> {
    db.transaction_mut(|t| delete_in_transaction(t, playback_db_id))
}

fn delete_in_transaction(db: &mut impl DbAccess, playback_db_id: DbId) -> anyhow::Result<()> {
    if let Some((_, queue_id)) = tagged_edge(db, playback_db_id, QUEUE_EDGE_KEY)? {
        db.exec_mut(QueryBuilder::remove().ids(queue_id).query())?;
    }
    db.exec_mut(QueryBuilder::remove().ids(playback_db_id).query())?;
    Ok(())
}

pub(crate) fn delete_for_user(db: &mut impl DbAccess, user_db_id: DbId) -> anyhow::Result<u64> {
    let mut count = 0_u64;
    loop {
        let playback_ids = db
            .exec(
                QueryBuilder::search()
                    .to(user_db_id)
                    .limit(MAX_PLAYBACKS_PER_USER as u64)
                    .where_()
                    .neighbor()
                    .and()
                    .key("db_element_id")
                    .value("Playback")
                    .query(),
            )?
            .ids();
        if playback_ids.is_empty() {
            break;
        }
        count = count.saturating_add(playback_ids.len() as u64);
        for id in playback_ids {
            delete_in_transaction(db, id)?;
        }
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
            queue: Some(Queue {
                revision: 1,
                track_ids: vec!["track".to_string()],
                current_index: 0,
                repeat_mode: RepeatMode::None,
                shuffle_enabled: false,
            }),
            reported: None,
            created_at_ms: 1,
            updated_at_ms: 1,
        }
    }

    #[test]
    fn reported_binding_survives_stops_and_retains_previous_session() -> anyhow::Result<()> {
        let (mut db, user_id, session_id) = setup()?;
        let root = db.transaction_mut(|t| {
            insert_reported(t, user_id, "external", "device", session_id, 1)
        })?;
        let root_id = root.db_id.unwrap();
        db.transaction_mut(|t| {
            update_reported_previous(
                t,
                root_id,
                Some(ReportedPrevious {
                    session_public_id: "prior-track".into(),
                    demoted_at_ms: 2,
                    expires_at_ms: 10,
                }),
            )?;
            link_current_session(t, root_id, session_id, 3)
        })?;
        let mut stopped = crate::db::playback_sessions::get_by_id(&db, session_id)?.unwrap();
        stopped.state = PlaybackState::Stopped;
        db.exec_mut(QueryBuilder::insert().element(&stopped).query())?;
        let restored = get_reported(&db, user_id, "external", "device", 4)?.unwrap();
        assert_eq!(restored.id, root.id);
        assert!(restored.queue.is_none());
        assert_eq!(
            restored
                .reported
                .unwrap()
                .previous
                .unwrap()
                .session_public_id,
            "prior-track"
        );
        assert!(get_reported(&db, user_id, "other", "device", 4)?.is_none());
        assert!(get_reported(&db, user_id, "external", "other", 4)?.is_none());
        assert_eq!(get_current_session_id(&db, root_id)?, Some(session_id));
        assert_eq!(
            list_projections_for_user(&db, user_id)?[0].queue_revision,
            None
        );
        Ok(())
    }

    #[test]
    fn reported_expiry_removes_binding_but_preserves_native_roots_and_accounting()
    -> anyhow::Result<()> {
        let (mut db, user_id, session_id) = setup()?;
        let native_id = create(&mut db, &playback(), user_id, session_id)?;
        let reported = db.transaction_mut(|t| {
            insert_reported(t, user_id, "external", "device", session_id, 1)
        })?;
        let expired_at = REPORTED_PLAYBACK_TTL_MS + 2;
        assert!(get_reported(&db, user_id, "external", "device", expired_at)?.is_none());
        assert_eq!(
            db.transaction_mut(|t| expire_reported_for_user(t, user_id, expired_at))?,
            1
        );
        assert!(get_by_id(&db, reported.db_id.unwrap())?.is_none());
        assert!(get_by_id(&db, native_id)?.is_some());
        assert!(crate::db::playback_sessions::get_by_id(&db, session_id)?.is_some());
        let new = db.transaction_mut(|t| {
            insert_reported(t, user_id, "external", "device", session_id, expired_at)
        })?;
        assert_ne!(new.id, reported.id);
        Ok(())
    }

    #[test]
    fn global_reported_expiry_keeps_live_contexts_and_accounting() -> anyhow::Result<()> {
        let (mut db, user_id, session_id) = setup()?;
        let now_ms = REPORTED_PLAYBACK_TTL_MS + 2;
        let (expired, live) = db.transaction_mut(|t| -> anyhow::Result<_> {
            let live = insert_reported(t, user_id, "reporter", "live", session_id, now_ms)?;
            let expired = insert_reported(t, user_id, "reporter", "expired", session_id, 1)?;
            Ok((expired, live))
        })?;
        assert_eq!(db.transaction_mut(|t| expire_reported(t, now_ms))?, 1);
        assert!(get_by_id(&db, expired.db_id.unwrap())?.is_none());
        assert!(get_by_id(&db, live.db_id.unwrap())?.is_some());
        assert!(crate::db::playback_sessions::get_by_id(&db, session_id)?.is_some());
        Ok(())
    }

    #[test]
    fn reported_limit_preserves_native_capacity_and_reclaims_expired_contexts() -> anyhow::Result<()>
    {
        let (mut db, user_id, session_id) = setup()?;
        db.transaction_mut(|t| -> anyhow::Result<()> {
            for index in 0..reported::MAX_REPORTED_PLAYBACKS_PER_USER {
                insert_reported(t, user_id, "reporter", &index.to_string(), session_id, 1)?;
            }
            Ok(())
        })?;
        let error = db
            .transaction_mut(|t| {
                insert_reported(t, user_id, "another-reporter", "extra", session_id, 1)
            })
            .unwrap_err();
        assert!(error.is::<ReportedPlaybackLimitReached>());
        let native_id = create(&mut db, &playback(), user_id, session_id)?;
        assert_eq!(
            count_for_user_up_to_limit(&db, user_id)?,
            reported::MAX_REPORTED_PLAYBACKS_PER_USER + 1
        );
        assert!(get_reported(&db, user_id, "reporter", "0", 1)?.is_some());
        let replacement = db.transaction_mut(|t| {
            insert_reported(
                t,
                user_id,
                "reporter",
                "new",
                session_id,
                REPORTED_PLAYBACK_TTL_MS + 2,
            )
        })?;
        assert!(replacement.queue.is_none());
        assert_eq!(count_for_user_up_to_limit(&db, user_id)?, 2);
        assert!(get_by_id(&db, native_id)?.is_some());
        Ok(())
    }

    #[test]
    fn deleting_playback_removes_owned_queue_only() -> anyhow::Result<()> {
        let (mut db, user_id, session_id) = setup()?;
        let root_id = create(&mut db, &playback(), user_id, session_id)?;
        let (_, queue_id) = tagged_edge(&db, root_id, QUEUE_EDGE_KEY)?.unwrap();
        delete(&mut db, root_id)?;
        assert!(
            super::super::graph::fetch_typed_by_id::<QueueRecord>(&db, queue_id, "QueueRecord")?
                .is_none()
        );
        assert!(crate::db::playback_sessions::get_by_id(&db, session_id)?.is_some());
        Ok(())
    }

    #[test]
    fn reported_playback_cannot_acquire_a_queue_by_replacement() -> anyhow::Result<()> {
        let (mut db, user_id, session_id) = setup()?;
        let root = db.transaction_mut(|t| {
            insert_reported(t, user_id, "external", "device", session_id, 1)
        })?;
        let error = replace_queue(
            &mut db,
            root.db_id.unwrap(),
            QueueReplacement {
                expected_revision: 0,
                track_ids: vec!["track".into()],
                current_index: 0,
                repeat_mode: RepeatMode::None,
                shuffle_enabled: false,
                updated_at_ms: 2,
                clear_current_session: true,
            },
        )
        .unwrap_err();
        assert!(matches!(error, ReplaceQueueError::QueueUnavailable));
        assert_eq!(
            get_current_session_id(&db, root.db_id.unwrap())?,
            Some(session_id)
        );
        assert_eq!(
            get_by_id(&db, root.db_id.unwrap())?.unwrap().updated_at_ms,
            1
        );
        Ok(())
    }

    #[test]
    fn delete_for_user_removes_queues_and_preserves_sessions() -> anyhow::Result<()> {
        let (mut db, user_id, session_id) = setup()?;
        let valid_id = create(&mut db, &playback(), user_id, session_id)?;
        let (_, valid_queue_id) = tagged_edge(&db, valid_id, QUEUE_EDGE_KEY)?.unwrap();
        assert_eq!(delete_for_user(&mut db, user_id)?, 1);
        assert!(list_projections_for_user(&db, user_id)?.is_empty());
        assert!(
            super::super::graph::fetch_typed_by_id::<QueueRecord>(
                &db,
                valid_queue_id,
                "QueueRecord"
            )?
            .is_none()
        );
        assert!(crate::db::playback_sessions::get_by_id(&db, session_id)?.is_some());
        Ok(())
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
            QueueReplacement {
                expected_revision: 1,
                track_ids: vec!["updated".to_string()],
                current_index: 0,
                repeat_mode: RepeatMode::All,
                shuffle_enabled: true,
                updated_at_ms: 2,
                clear_current_session: false,
            },
        )?;
        assert_eq!(updated.queue.as_ref().unwrap().revision, 2);

        let error = replace_queue(
            &mut db,
            playback_id,
            QueueReplacement {
                expected_revision: 1,
                track_ids: vec!["stale".to_string()],
                current_index: 0,
                repeat_mode: RepeatMode::None,
                shuffle_enabled: false,
                updated_at_ms: 3,
                clear_current_session: false,
            },
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
        assert_eq!(stored.queue.as_ref().unwrap().track_ids, vec!["updated"]);
        assert_eq!(stored.queue.as_ref().unwrap().repeat_mode, RepeatMode::All);
        assert!(stored.queue.as_ref().unwrap().shuffle_enabled);
        Ok(())
    }

    #[test]
    fn queue_replace_can_detach_current_session() -> anyhow::Result<()> {
        let (mut db, user_id, session_id) = setup()?;
        let playback_id = create(&mut db, &playback(), user_id, session_id)?;
        replace_queue(
            &mut db,
            playback_id,
            QueueReplacement {
                expected_revision: 1,
                track_ids: vec!["track".to_string()],
                current_index: 0,
                repeat_mode: RepeatMode::None,
                shuffle_enabled: false,
                updated_at_ms: 2,
                clear_current_session: true,
            },
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
