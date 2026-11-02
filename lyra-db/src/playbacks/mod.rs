// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::fmt;

use agdb::{
    CountComparison,
    DbAny,
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
pub use reported::{
    ExpiredReported,
    REPORTED_PLAYBACK_TTL_MS,
    ReportedPlaybackLimitReached,
    ReportedSource,
    expire_reported,
    expire_reported_for_user,
    get_reported,
    insert_reported,
};

const OWNER_EDGE_KEY: &str = "owner";
const CURRENT_SESSION_EDGE_KEY: &str = "current_session";
const PREVIOUS_SESSION_EDGE_KEY: &str = "previous_session";
pub const MAX_PLAYBACKS_PER_USER: usize = 10_000;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, DbTypeMarker)]
#[serde(rename_all = "snake_case")]
pub enum RepeatMode {
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
pub struct Playback {
    pub db_id: Option<DbId>,
    pub id: String,
    pub source: PlaybackSource,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl Playback {
    pub fn queue(&self) -> Option<&Queue> {
        match &self.source {
            PlaybackSource::Queue(queue) => Some(queue),
            PlaybackSource::Reported(_) => None,
        }
    }

    pub fn is_reported(&self) -> bool {
        matches!(self.source, PlaybackSource::Reported(_))
    }
}

/// What drives a playback: a queue the server manages, or a player that reports what it plays.
#[derive(Clone, Debug)]
pub enum PlaybackSource {
    Queue(Queue),
    Reported(ReportedSource),
}

#[derive(Clone, Debug)]
pub struct Queue {
    pub revision: u64,
    pub track_ids: Vec<String>,
    pub current_index: u64,
    pub repeat_mode: RepeatMode,
    pub shuffle_enabled: bool,
}

#[derive(agdb::DbType, Clone, Debug)]
struct PlaybackRecord {
    db_element_id: String,
    db_id: Option<DbId>,
    id: String,
    reported: Option<ReportedSource>,
    queue_revision: Option<u64>,
    queue_track_ids: Option<Vec<String>>,
    queue_current_index: Option<u64>,
    queue_repeat_mode: Option<RepeatMode>,
    queue_shuffle_enabled: Option<bool>,
    created_at_ms: u64,
    updated_at_ms: u64,
}

impl PlaybackRecord {
    fn from_playback(playback: &Playback) -> Self {
        let queue = playback.queue();
        let reported = match &playback.source {
            PlaybackSource::Queue(_) => None,
            PlaybackSource::Reported(source) => Some(source.clone()),
        };
        Self {
            db_element_id: "Playback".to_owned(),
            db_id: playback.db_id,
            id: playback.id.clone(),
            reported,
            queue_revision: queue.map(|queue| queue.revision),
            queue_track_ids: queue.map(|queue| queue.track_ids.clone()),
            queue_current_index: queue.map(|queue| queue.current_index),
            queue_repeat_mode: queue.map(|queue| queue.repeat_mode),
            queue_shuffle_enabled: queue.map(|queue| queue.shuffle_enabled),
            created_at_ms: playback.created_at_ms,
            updated_at_ms: playback.updated_at_ms,
        }
    }

    fn into_playback(self) -> anyhow::Result<Playback> {
        let queue = match (
            self.queue_revision,
            self.queue_track_ids,
            self.queue_current_index,
            self.queue_repeat_mode,
            self.queue_shuffle_enabled,
        ) {
            (
                Some(revision),
                Some(track_ids),
                Some(current_index),
                Some(repeat_mode),
                Some(shuffle_enabled),
            ) => Some(Queue {
                revision,
                track_ids,
                current_index,
                repeat_mode,
                shuffle_enabled,
            }),
            (None, None, None, None, None) => None,
            _ => anyhow::bail!("playback {} has incomplete queue fields", self.id),
        };
        let source = match (queue, self.reported) {
            (Some(queue), None) => PlaybackSource::Queue(queue),
            (None, Some(source)) => PlaybackSource::Reported(source),
            (Some(_), Some(_)) => anyhow::bail!("playback {} has a queue and a reporter", self.id),
            (None, None) => anyhow::bail!("playback {} has no queue or reporter", self.id),
        };
        Ok(Playback {
            db_id: self.db_id,
            id: self.id,
            source,
            created_at_ms: self.created_at_ms,
            updated_at_ms: self.updated_at_ms,
        })
    }
}

#[derive(Clone, Debug)]
pub struct PlaybackListProjection {
    pub db_id: DbId,
    pub id: String,
    pub queue_revision: Option<u64>,
    pub updated_at_ms: u64,
    pub reported: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ReplaceQueueError {
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

#[cfg(any(test, feature = "testing"))]
pub fn create(
    db: &mut DbAny,
    playback: &Playback,
    owner_db_id: DbId,
    current_session_db_id: DbId,
) -> anyhow::Result<DbId> {
    db.transaction_mut(|t| insert(t, playback, owner_db_id, current_session_db_id))
}

pub fn insert(
    db: &mut impl DbAccess,
    playback: &Playback,
    owner_db_id: DbId,
    current_session_db_id: DbId,
) -> anyhow::Result<DbId> {
    let record = PlaybackRecord::from_playback(playback);
    let playback_db_id = db
        .exec_mut(QueryBuilder::insert().element(&record).query())?
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

pub fn get_by_id(db: &impl DbAccess, playback_db_id: DbId) -> anyhow::Result<Option<Playback>> {
    super::graph::fetch_typed_by_id::<PlaybackRecord>(db, playback_db_id, "Playback")?
        .map(PlaybackRecord::into_playback)
        .transpose()
}

pub fn list_projections_for_user(
    db: &impl DbAccess,
    user_db_id: DbId,
) -> anyhow::Result<Vec<PlaybackListProjection>> {
    let keys = [
        "db_element_id",
        "id",
        "reported",
        "queue_revision",
        "updated_at_ms",
    ]
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

/// A playback with the track session it currently plays.
#[derive(Clone, Debug)]
pub struct CurrentPlayback {
    pub playback: Playback,
    pub owner_db_id: DbId,
    pub session: crate::PlaybackSession,
    pub track_db_id: DbId,
}

/// The playbacks whose current track session was updated at or after `since_ms`: every user's
/// without `owners`, otherwise only those the given users own.
pub fn list_current_since(
    db: &impl DbAccess,
    owners: Option<&[DbId]>,
    since_ms: u64,
) -> anyhow::Result<Vec<CurrentPlayback>> {
    let Some(owners) = owners else {
        return list_every_current_since(db, since_ms);
    };
    let mut current = Vec::new();
    for &owner_db_id in owners {
        let playback_db_ids = db
            .exec(
                QueryBuilder::search()
                    .to(owner_db_id)
                    .limit((MAX_PLAYBACKS_PER_USER + 1) as u64)
                    .where_()
                    .distance(CountComparison::Equal(2))
                    .and()
                    .node()
                    .and()
                    .key("db_element_id")
                    .value("Playback")
                    .query(),
            )?
            .ids();
        let mut current_session_ids = Vec::with_capacity(playback_db_ids.len());
        for playback_db_id in playback_db_ids {
            if let Some(session_db_id) = get_current_session_id(db, playback_db_id)? {
                current_session_ids.push((playback_db_id, session_db_id));
            }
        }
        let projections = crate::playback_sessions::get_list_projections_by_ids(
            db,
            current_session_ids
                .iter()
                .map(|(_, session_db_id)| *session_db_id)
                .collect(),
        )?;
        for (playback_db_id, session_db_id) in current_session_ids {
            if projections
                .get(&session_db_id)
                .is_some_and(|projection| projection.updated_at_ms >= since_ms)
                && let Some(session) = crate::playback_sessions::get_by_id(db, session_db_id)?
                && let Some(found) =
                    current_playback(db, playback_db_id, Some(owner_db_id), session)?
            {
                current.push(found);
            }
        }
    }
    Ok(current)
}

fn list_every_current_since(
    db: &impl DbAccess,
    since_ms: u64,
) -> anyhow::Result<Vec<CurrentPlayback>> {
    let sessions: Vec<crate::PlaybackSession> = db
        .exec(
            QueryBuilder::select()
                .elements::<crate::PlaybackSession>()
                .search()
                .from("playback_sessions")
                .where_()
                .neighbor()
                .and()
                .key("updated_at_ms")
                .greater_than_or_equal(since_ms)
                .query(),
        )?
        .try_into()?;
    let mut current = Vec::new();
    for session in sessions {
        let Some(session_db_id) = session.db_id else {
            continue;
        };
        let holders = db.exec(
            QueryBuilder::select()
                .search()
                .to(session_db_id)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .and()
                .keys(CURRENT_SESSION_EDGE_KEY)
                .query(),
        )?;
        let Some(playback_db_id) = holders
            .elements
            .iter()
            .find(|edge| edge.id.0 < 0 && edge.from.0 > 0)
            .map(|edge| edge.from)
        else {
            continue;
        };
        if let Some(found) = current_playback(db, playback_db_id, None, session)? {
            current.push(found);
        }
    }
    Ok(current)
}

fn current_playback(
    db: &impl DbAccess,
    playback_db_id: DbId,
    owner_db_id: Option<DbId>,
    session: crate::PlaybackSession,
) -> anyhow::Result<Option<CurrentPlayback>> {
    let Some(session_db_id) = session.db_id else {
        return Ok(None);
    };
    let owner_db_id = match owner_db_id {
        Some(owner_db_id) => Some(owner_db_id),
        None => get_owner_id(db, playback_db_id)?,
    };
    let (Some(playback), Some(owner_db_id), Some(track_db_id)) = (
        get_by_id(db, playback_db_id)?,
        owner_db_id,
        crate::playback_sessions::get_track_id(db, session_db_id)?,
    ) else {
        return Ok(None);
    };
    Ok(Some(CurrentPlayback {
        playback,
        owner_db_id,
        session,
        track_db_id,
    }))
}

pub fn count_for_user_up_to_limit(db: &impl DbAccess, user_db_id: DbId) -> anyhow::Result<usize> {
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
    let queue_revision = element
        .values
        .iter()
        .find(|kv| kv.key == DbValue::from("queue_revision"))
        .map(|kv| kv.value.to_u64())
        .transpose()?;
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

pub fn get_projection_by_id(
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
    projection_from_element(&element)
}

pub fn current_session_ids(
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

pub fn get_owner_id(db: &impl DbAccess, playback_db_id: DbId) -> Result<Option<DbId>, DbError> {
    Ok(tagged_edge(db, playback_db_id, OWNER_EDGE_KEY)?.map(|(_, target)| target))
}

pub fn get_current_session_id(
    db: &impl DbAccess,
    playback_db_id: DbId,
) -> Result<Option<DbId>, DbError> {
    Ok(tagged_edge(db, playback_db_id, CURRENT_SESSION_EDGE_KEY)?.map(|(_, target)| target))
}

/// Whether a playback holds the session: as its current session, or as its previous one while
/// both the playback and the previous session's grace last past `cutoff_ms`.
pub fn holds_session(
    db: &impl DbAccess,
    playback_session_db_id: DbId,
    cutoff_ms: u64,
) -> anyhow::Result<bool> {
    let edges = db.exec(
        QueryBuilder::select()
            .search()
            .to(playback_session_db_id)
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(1))
            .query(),
    )?;
    for edge in edges.elements {
        if edge.id.0 >= 0 {
            continue;
        }
        let value = |key: &str| {
            edge.values
                .iter()
                .find(|kv| kv.key == DbValue::from(key))
                .map(|kv| &kv.value)
        };
        if value(CURRENT_SESSION_EDGE_KEY).is_some() {
            return Ok(true);
        }
        if value(PREVIOUS_SESSION_EDGE_KEY).is_some()
            && previous_session_from_edge(&edge)?.expires_at_ms >= cutoff_ms
            && get_by_id(db, edge.from)?.is_some_and(|playback| playback.updated_at_ms >= cutoff_ms)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn link_current_session(
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

/// The session a playback's current one replaced, which late reports of it may still reach until
/// it expires.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreviousSession {
    pub session_db_id: DbId,
    pub demoted_at_ms: u64,
    pub expires_at_ms: u64,
}

pub fn get_previous_session(
    db: &impl DbAccess,
    playback_db_id: DbId,
) -> anyhow::Result<Option<PreviousSession>> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .from(playback_db_id)
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(1))
            .and()
            .keys(PREVIOUS_SESSION_EDGE_KEY)
            .query(),
    )?;
    result
        .elements
        .iter()
        .find(|element| element.id.0 < 0 && element.to.0 > 0)
        .map(previous_session_from_edge)
        .transpose()
}

fn previous_session_from_edge(edge: &agdb::DbElement) -> anyhow::Result<PreviousSession> {
    let value = |key: &str| -> anyhow::Result<u64> {
        edge.values
            .iter()
            .find(|kv| kv.key == DbValue::from(key))
            .ok_or_else(|| anyhow::anyhow!("previous session edge {} missing {key}", edge.id.0))?
            .value
            .to_u64()
            .map_err(Into::into)
    };
    Ok(PreviousSession {
        session_db_id: edge.to,
        demoted_at_ms: value("demoted_at_ms")?,
        expires_at_ms: value("expires_at_ms")?,
    })
}

/// Replaces the playback's previous session, or clears it with `None`.
pub fn set_previous_session(
    db: &mut impl DbAccess,
    playback_db_id: DbId,
    previous: Option<PreviousSession>,
) -> anyhow::Result<()> {
    if let Some((edge_id, _)) = tagged_edge(db, playback_db_id, PREVIOUS_SESSION_EDGE_KEY)? {
        db.exec_mut(QueryBuilder::remove().ids(edge_id).query())?;
    }
    let Some(previous) = previous else {
        return Ok(());
    };
    let edge_id = db
        .exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(playback_db_id)
                .to(previous.session_db_id)
                .query(),
        )?
        .ids()[0];
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform([
                (PREVIOUS_SESSION_EDGE_KEY, 1_u64).into(),
                ("demoted_at_ms", previous.demoted_at_ms).into(),
                ("expires_at_ms", previous.expires_at_ms).into(),
            ])
            .ids(edge_id)
            .query(),
    )?;
    Ok(())
}

pub fn touch(
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

pub struct QueueReplacement {
    pub expected_revision: u64,
    pub track_ids: Vec<String>,
    pub current_index: u64,
    pub repeat_mode: RepeatMode,
    pub shuffle_enabled: bool,
    pub updated_at_ms: u64,
    pub clear_current_session: bool,
}

#[cfg(test)]
pub(crate) fn replace_queue(
    db: &mut DbAny,
    playback_db_id: DbId,
    replacement: QueueReplacement,
) -> Result<Playback, ReplaceQueueError> {
    db.transaction_mut(|t| replace_queue_in_transaction(t, playback_db_id, replacement))
}

pub fn replace_queue_in_transaction(
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
    let PlaybackSource::Queue(queue) = &mut playback.source else {
        return Err(ReplaceQueueError::QueueUnavailable);
    };
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
    db.exec_mut(
        QueryBuilder::insert()
            .element(PlaybackRecord::from_playback(&playback))
            .query(),
    )?;
    if clear_current_session {
        remove_current_session_edge(db, playback_db_id)?;
    }
    Ok(playback)
}

pub fn delete(db: &mut DbAny, playback_db_id: DbId) -> anyhow::Result<()> {
    db.transaction_mut(|t| delete_in_transaction(t, playback_db_id))
}

fn delete_in_transaction(db: &mut impl DbAccess, playback_db_id: DbId) -> anyhow::Result<()> {
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
    use crate::{
        PlaybackSession,
        PlaybackState,
        fixtures::{
            insert_track,
            test_user,
        },
        test_db::new_test_db,
    };

    fn setup() -> anyhow::Result<(DbAny, DbId, DbId)> {
        let mut db = new_test_db()?;
        let user_id = crate::users::create(&mut db, &test_user("playback-owner")?)?;
        let track_id = insert_track(&mut db, "Track")?;
        let session_id = crate::playback_sessions::create(
            &mut db,
            &PlaybackSession {
                db_id: None,
                id: nanoid!(),
                client_name: None,
                plugin_id: None,
                position_ms: 0,
                duration_ms: None,
                played_ms: 0,
                last_position_ms: 0,
                state: PlaybackState::Playing,
                play_id: nanoid::nanoid!(),
                latency_allowance_used_ms: 0,
                rate_permille: 1000,
                last_position_at_ms: 1,
                updated_at_ms: 1,
                created_at_ms: 1,
            },
            track_id,
            user_id,
        )?;
        Ok((db, user_id, session_id))
    }

    fn insert_session(db: &mut DbAny, user_id: DbId) -> anyhow::Result<DbId> {
        let track_id = insert_track(db, "Other")?;
        crate::playback_sessions::create(
            db,
            &PlaybackSession {
                db_id: None,
                id: nanoid!(),
                client_name: None,
                plugin_id: None,
                position_ms: 0,
                duration_ms: None,
                played_ms: 0,
                last_position_ms: 0,
                state: PlaybackState::Paused,
                play_id: nanoid!(),
                latency_allowance_used_ms: 0,
                rate_permille: 1000,
                last_position_at_ms: 1,
                updated_at_ms: 1,
                created_at_ms: 1,
            },
            track_id,
            user_id,
        )
    }

    #[test]
    fn current_playbacks_span_users_and_skip_unheld_or_older_sessions() -> anyhow::Result<()> {
        let (mut db, user_id, session_id) = setup()?;
        let other_id = crate::users::create(&mut db, &test_user("other")?)?;
        let other_session_id = insert_session(&mut db, other_id)?;
        let mut other_session =
            crate::playback_sessions::get_by_id(&db, other_session_id)?.expect("session");
        other_session.updated_at_ms = 50;
        db.transaction_mut(|t| crate::playback_sessions::update(t, &other_session))?;
        let unheld_id = insert_session(&mut db, other_id)?;
        db.transaction_mut(|t| insert_reported(t, user_id, "external", "a", session_id, 1))?;
        let other_root = db.transaction_mut(|t| {
            insert_reported(t, other_id, "external", "b", other_session_id, 50)
        })?;
        db.transaction_mut(|t| {
            set_previous_session(
                t,
                other_root.db_id.unwrap(),
                Some(PreviousSession {
                    session_db_id: unheld_id,
                    demoted_at_ms: 50,
                    expires_at_ms: 100,
                }),
            )
        })?;

        let listed = |owners: Option<&[DbId]>, since_ms| -> anyhow::Result<Vec<(DbId, DbId)>> {
            let mut listed = list_current_since(&db, owners, since_ms)?
                .into_iter()
                .map(|current| (current.owner_db_id, current.session.db_id.unwrap()))
                .collect::<Vec<_>>();
            listed.sort();
            Ok(listed)
        };
        let mut both = vec![(user_id, session_id), (other_id, other_session_id)];
        both.sort();
        assert_eq!(listed(None, 0)?, both, "a previous session is not current");
        assert_eq!(listed(Some(&[user_id, other_id]), 0)?, both);
        assert_eq!(listed(Some(&[user_id]), 0)?, vec![(user_id, session_id)]);
        assert_eq!(listed(None, 10)?, vec![(other_id, other_session_id)]);
        assert_eq!(listed(Some(&[user_id]), 10)?, vec![]);
        assert_eq!(
            listed(Some(&[other_id]), 10)?,
            vec![(other_id, other_session_id)]
        );
        for owners in [None, Some(&[other_id][..])] {
            let current = list_current_since(&db, owners, 10)?.remove(0);
            assert_eq!(current.playback.id, other_root.id);
            assert!(matches!(
                &current.playback.source,
                PlaybackSource::Reported(source) if source.session_key() == "b"
            ));
        }
        Ok(())
    }

    #[test]
    fn previous_session_is_replaced_and_held_through_its_grace() -> anyhow::Result<()> {
        let (mut db, user_id, session_id) = setup()?;
        let root_id = db
            .transaction_mut(|t| insert_reported(t, user_id, "external", "device", session_id, 1))?
            .db_id
            .unwrap();
        let first = insert_session(&mut db, user_id)?;
        let second = insert_session(&mut db, user_id)?;
        let held = |db: &DbAny, session, cutoff| holds_session(db, session, cutoff);
        let previous = |session_db_id| PreviousSession {
            session_db_id,
            demoted_at_ms: 1,
            expires_at_ms: 20,
        };
        db.transaction_mut(|t| set_previous_session(t, root_id, Some(previous(first))))?;
        db.transaction_mut(|t| set_previous_session(t, root_id, Some(previous(second))))?;
        assert_eq!(get_previous_session(&db, root_id)?, Some(previous(second)));
        assert!(!held(&db, first, 0)?);
        assert!(held(&db, second, 1)?);
        assert!(
            !held(&db, second, 2)?,
            "the playback went untouched past the cutoff"
        );
        db.transaction_mut(|t| touch(t, root_id, 30))?;
        assert!(held(&db, second, 20)?);
        assert!(!held(&db, second, 21)?, "the grace ended before the cutoff");
        db.exec_mut(QueryBuilder::remove().ids(second).query())?;
        assert_eq!(get_previous_session(&db, root_id)?, None);
        db.transaction_mut(|t| set_previous_session(t, root_id, Some(previous(first))))?;
        db.transaction_mut(|t| set_previous_session(t, root_id, None))?;
        assert_eq!(get_previous_session(&db, root_id)?, None);
        assert_eq!(get_current_session_id(&db, root_id)?, Some(session_id));
        Ok(())
    }

    fn playback() -> Playback {
        Playback {
            db_id: None,
            id: nanoid!(),
            source: PlaybackSource::Queue(Queue {
                revision: 1,
                track_ids: vec!["track".to_string()],
                current_index: 0,
                repeat_mode: RepeatMode::None,
                shuffle_enabled: false,
            }),
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
        let prior_id = insert_session(&mut db, user_id)?;
        let previous = PreviousSession {
            session_db_id: prior_id,
            demoted_at_ms: 2,
            expires_at_ms: 10,
        };
        db.transaction_mut(|t| {
            set_previous_session(t, root_id, Some(previous))?;
            link_current_session(t, root_id, session_id, 3)
        })?;
        let mut stopped = crate::playback_sessions::get_by_id(&db, session_id)?.unwrap();
        stopped.state = PlaybackState::Stopped;
        db.exec_mut(QueryBuilder::insert().element(&stopped).query())?;
        let restored = get_reported(&db, user_id, "external", "device", 4)?.unwrap();
        assert_eq!(restored.id, root.id);
        assert!(restored.queue().is_none());
        assert_eq!(get_previous_session(&db, root_id)?, Some(previous));
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
            vec![ExpiredReported {
                user_db_id: user_id,
                current_session_db_id: Some(session_id),
                updated_at_ms: 1,
            }]
        );
        assert!(get_by_id(&db, reported.db_id.unwrap())?.is_none());
        assert!(get_by_id(&db, native_id)?.is_some());
        assert!(crate::playback_sessions::get_by_id(&db, session_id)?.is_some());
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
        assert_eq!(
            db.transaction_mut(|t| expire_reported(t, now_ms))?,
            vec![ExpiredReported {
                user_db_id: user_id,
                current_session_db_id: Some(session_id),
                updated_at_ms: 1,
            }]
        );
        assert!(get_by_id(&db, expired.db_id.unwrap())?.is_none());
        assert!(get_by_id(&db, live.db_id.unwrap())?.is_some());
        assert!(crate::playback_sessions::get_by_id(&db, session_id)?.is_some());
        Ok(())
    }

    #[test]
    fn eviction_keeps_a_playbacks_current_session_until_the_playback_goes() -> anyhow::Result<()> {
        let (mut db, user_id, session_id) = setup()?;
        let root_id = create(&mut db, &playback(), user_id, session_id)?;
        let stale_now_ms = 24 * 60 * 60 * 1000;

        assert!(holds_session(&db, session_id, stale_now_ms)?);
        assert!(
            crate::playback_sessions::evict_stale(&mut db, stale_now_ms, |_| false)?.is_empty()
        );

        delete(&mut db, root_id)?;
        assert!(!holds_session(&db, session_id, 0)?);
        let evicted = crate::playback_sessions::evict_stale(&mut db, stale_now_ms, |_| false)?;
        assert_eq!(evicted.len(), 1);
        assert!(crate::playback_sessions::get_by_id(&db, session_id)?.is_none());
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
            let now_ms = REPORTED_PLAYBACK_TTL_MS + 2;
            expire_reported_for_user(t, user_id, now_ms)?;
            insert_reported(t, user_id, "reporter", "new", session_id, now_ms)
        })?;
        assert!(replacement.queue().is_none());
        assert_eq!(count_for_user_up_to_limit(&db, user_id)?, 2);
        assert!(get_by_id(&db, native_id)?.is_some());
        Ok(())
    }

    #[test]
    fn queue_is_stored_on_playback_node() -> anyhow::Result<()> {
        let (mut db, user_id, session_id) = setup()?;
        let playback_id = create(&mut db, &playback(), user_id, session_id)?;
        let stored = get_by_id(&db, playback_id)?.unwrap();
        let queue = stored.queue().unwrap();
        assert_eq!(queue.revision, 1);
        assert_eq!(queue.track_ids, vec!["track"]);
        assert_eq!(queue.current_index, 0);
        assert_eq!(queue.repeat_mode, RepeatMode::None);
        assert!(!queue.shuffle_enabled);
        let outgoing = db
            .exec(
                QueryBuilder::search()
                    .from(playback_id)
                    .where_()
                    .edge()
                    .and()
                    .distance(CountComparison::Equal(1))
                    .query(),
            )?
            .elements;
        let mut targets = outgoing.iter().map(|edge| edge.to).collect::<Vec<_>>();
        targets.sort();
        let mut expected = vec![user_id, session_id];
        expected.sort();
        assert_eq!(targets, expected);
        assert_eq!(
            list_projections_for_user(&db, user_id)?[0].queue_revision,
            Some(1)
        );
        assert_eq!(
            get_projection_by_id(&db, playback_id)?
                .unwrap()
                .queue_revision,
            Some(1)
        );
        Ok(())
    }

    #[test]
    fn partial_queue_fields_are_rejected() -> anyhow::Result<()> {
        let (mut db, user_id, session_id) = setup()?;
        let playback_id = create(&mut db, &playback(), user_id, session_id)?;
        db.exec_mut(
            QueryBuilder::remove()
                .values(["queue_track_ids".to_string()])
                .ids(playback_id)
                .query(),
        )?;
        assert!(get_by_id(&db, playback_id).is_err());
        Ok(())
    }

    #[test]
    fn deleting_playback_preserves_sessions() -> anyhow::Result<()> {
        let (mut db, user_id, session_id) = setup()?;
        let root_id = create(&mut db, &playback(), user_id, session_id)?;
        delete(&mut db, root_id)?;
        assert!(get_by_id(&db, root_id)?.is_none());
        assert!(crate::playback_sessions::get_by_id(&db, session_id)?.is_some());
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
    fn delete_for_user_removes_playbacks_and_preserves_sessions() -> anyhow::Result<()> {
        let (mut db, user_id, session_id) = setup()?;
        let playback_id = create(&mut db, &playback(), user_id, session_id)?;
        assert_eq!(delete_for_user(&mut db, user_id)?, 1);
        assert!(list_projections_for_user(&db, user_id)?.is_empty());
        assert!(get_by_id(&db, playback_id)?.is_none());
        assert!(crate::playback_sessions::get_by_id(&db, session_id)?.is_some());
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
        assert_eq!(updated.queue().unwrap().revision, 2);

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
        assert_eq!(stored.queue().unwrap().track_ids, vec!["updated"]);
        assert_eq!(stored.queue().unwrap().repeat_mode, RepeatMode::All);
        assert!(stored.queue().unwrap().shuffle_enabled);
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
        assert!(crate::playback_sessions::get_by_id(&db, session_id)?.is_some());
        Ok(())
    }

    #[test]
    fn user_playback_lookup_ignores_other_owned_resource_types() -> anyhow::Result<()> {
        let (mut db, user_id, session_id) = setup()?;
        let playback_id = create(&mut db, &playback(), user_id, session_id)?;
        let playlist_id = crate::playlists::create(
            &mut db,
            &crate::Playlist {
                db_id: None,
                id: nanoid!(),
                name: "Owned playlist".to_string(),
                description: None,
                is_public: None,
                created_at_ms: None,
                updated_at_ms: None,
            },
            user_id,
        )?;

        let playbacks = list_projections_for_user(&db, user_id)?;
        assert_eq!(playbacks.len(), 1);
        assert_eq!(playbacks[0].db_id, playback_id);
        assert_eq!(delete_for_user(&mut db, user_id)?, 1);
        assert!(crate::playlists::get_by_id(&db, playlist_id)?.is_some());
        Ok(())
    }
}
