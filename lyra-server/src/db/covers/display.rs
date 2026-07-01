// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashSet,
    fmt,
    time::{
        SystemTime,
        UNIX_EPOCH,
    },
};

use agdb::{
    DbElement,
    DbError,
    DbId,
    DbKeyValue,
    DbType,
    DbTypeMarker,
    DbValue,
    QueryBuilder,
};
use nanoid::nanoid;

use crate::db::{
    DbAccess,
    NodeId,
    genres,
    graph,
    releases,
    tracks,
    users,
};

pub(crate) const ALGORITHM_VERSION: u32 = 1;

const COLLECTION_ALIAS: &str = "display_covers";
const PROFILE_PREFIX: &str = "cover_profile";
const CANDIDATE_PREFIX: &str = "cover_candidate";
const WINNER_PREFIX: &str = "cover_winner";
const REPAIR_PREFIX: &str = "cover_repair";
const ROLE_KEY: &str = "role";
const WINNER_KIND_KEY: &str = "winner_kind";
const LISTEN_COUNT_KEY: &str = "listen_count";
const USER_LISTEN_COUNT_KEY: &str = "user_listen_count";
const INSTANCE_LISTEN_COUNT_KEY: &str = "instance_listen_count";
const LAST_LISTENED_AT_MS_KEY: &str = "last_listened_at_ms";
const RANDOM_SCORE_KEY: &str = "random_score";
const HAS_COVER_KEY: &str = "has_cover";
const UPDATED_AT_MS_KEY: &str = "updated_at_ms";
const REPAIR_COLLECTION_ALIAS: &str = "display_cover_repairs";

#[derive(DbElement, Clone, Debug)]
pub(crate) struct DisplayCoverProfile {
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) identity: String,
    pub(crate) target_kind: DisplayCoverTargetKind,
    pub(crate) scope: DisplayCoverScope,
    pub(crate) algorithm_version: u32,
    pub(crate) user_total_listens: u64,
    pub(crate) instance_total_listens: u64,
    pub(crate) active_listener_count: u64,
    pub(crate) dirty: Option<bool>,
    pub(crate) updated_at_ms: u64,
}

#[derive(DbElement, Clone, Debug)]
pub(crate) struct DisplayCoverRepair {
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) identity: String,
    pub(crate) target_kind: DisplayCoverTargetKind,
    pub(crate) target_public_id: String,
    pub(crate) user_public_id: Option<String>,
    pub(crate) reason: String,
    pub(crate) state: String,
    pub(crate) algorithm_version: u32,
    pub(crate) created_at_ms: u64,
    pub(crate) updated_at_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, DbTypeMarker)]
pub(crate) enum DisplayCoverTargetKind {
    Genre,
    Playlist,
}

impl DisplayCoverTargetKind {
    pub(crate) const fn as_db_str(self) -> &'static str {
        match self {
            Self::Genre => "genre",
            Self::Playlist => "playlist",
        }
    }

    fn from_db_str(value: &str) -> Result<Self, DbError> {
        match value {
            "genre" => Ok(Self::Genre),
            "playlist" => Ok(Self::Playlist),
            _ => Err(DbError::serialization(
                agdb::DbErrorType::TypeError,
                format!("invalid DisplayCoverTargetKind value '{value}'"),
            )),
        }
    }
}

impl fmt::Display for DisplayCoverTargetKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_db_str())
    }
}

impl From<DisplayCoverTargetKind> for DbValue {
    fn from(value: DisplayCoverTargetKind) -> Self {
        Self::from(value.as_db_str())
    }
}

impl From<&DisplayCoverTargetKind> for DbValue {
    fn from(value: &DisplayCoverTargetKind) -> Self {
        (*value).into()
    }
}

impl TryFrom<DbValue> for DisplayCoverTargetKind {
    type Error = DbError;

    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        Self::from_db_str(value.string()?)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, DbTypeMarker)]
pub(crate) enum DisplayCoverScope {
    User,
    Instance,
}

impl DisplayCoverScope {
    pub(crate) const fn as_db_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Instance => "instance",
        }
    }

    fn from_db_str(value: &str) -> Result<Self, DbError> {
        match value {
            "user" => Ok(Self::User),
            "instance" => Ok(Self::Instance),
            _ => Err(DbError::serialization(
                agdb::DbErrorType::TypeError,
                format!("invalid DisplayCoverScope value '{value}'"),
            )),
        }
    }
}

impl fmt::Display for DisplayCoverScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_db_str())
    }
}

impl From<DisplayCoverScope> for DbValue {
    fn from(value: DisplayCoverScope) -> Self {
        Self::from(value.as_db_str())
    }
}

impl From<&DisplayCoverScope> for DbValue {
    fn from(value: &DisplayCoverScope) -> Self {
        (*value).into()
    }
}

impl TryFrom<DbValue> for DisplayCoverScope {
    type Error = DbError;

    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        Self::from_db_str(value.string()?)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DisplayCoverWinnerKind {
    Personal,
    Instance,
    Random,
}

impl DisplayCoverWinnerKind {
    pub(crate) const fn as_db_str(self) -> &'static str {
        match self {
            Self::Personal => "personal",
            Self::Instance => "instance",
            Self::Random => "random",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DisplayCoverWinner {
    pub(crate) release_db_id: DbId,
    pub(crate) listen_count: u64,
    pub(crate) random_score: u64,
}

#[derive(Clone, Debug)]
struct DisplayCoverCandidate {
    edge_id: DbId,
    user_listen_count: u64,
    instance_listen_count: u64,
    last_listened_at_ms: Option<u64>,
    random_score: u64,
}

#[derive(Clone, Copy, Debug)]
enum CandidateIncrementScope {
    User,
    Instance,
}

struct CandidateUpdate<'a> {
    profile_db_id: DbId,
    profile_public_id: &'a str,
    release_db_id: DbId,
    release_public_id: &'a str,
    random_score: u64,
    now_ms: u64,
    increment: Option<(CandidateIncrementScope, u64)>,
}

struct WinnerUpdate<'a> {
    profile_db_id: DbId,
    profile_public_id: &'a str,
    release_db_id: DbId,
    winner_kind: DisplayCoverWinnerKind,
    listen_count: u64,
    random_score: u64,
    now_ms: u64,
}

pub(crate) struct DisplayCoverTarget<'a> {
    pub(crate) kind: DisplayCoverTargetKind,
    pub(crate) db_id: DbId,
    pub(crate) public_id: &'a str,
}

pub(crate) struct DisplayCoverUser<'a> {
    pub(crate) db_id: DbId,
    pub(crate) public_id: &'a str,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock drift")
        .as_millis() as u64
}

pub(crate) fn profile_identity(
    scope: DisplayCoverScope,
    target_kind: DisplayCoverTargetKind,
    user_public_id: Option<&str>,
    target_public_id: &str,
) -> String {
    match scope {
        DisplayCoverScope::User => format!(
            "{PROFILE_PREFIX}:{}:{}:{}:{}",
            scope.as_db_str(),
            target_kind.as_db_str(),
            user_public_id.expect("user profile identity requires user id"),
            target_public_id
        ),
        DisplayCoverScope::Instance => format!(
            "{PROFILE_PREFIX}:{}:{}:{}",
            scope.as_db_str(),
            target_kind.as_db_str(),
            target_public_id
        ),
    }
}

fn candidate_identity(profile_public_id: &str, release_public_id: &str) -> String {
    format!("{CANDIDATE_PREFIX}:{profile_public_id}:{release_public_id}")
}

fn winner_identity(profile_public_id: &str, winner_kind: DisplayCoverWinnerKind) -> String {
    format!(
        "{WINNER_PREFIX}:{}:{}",
        profile_public_id,
        winner_kind.as_db_str()
    )
}

fn repair_identity(profile_identity: &str, reason: &str) -> String {
    format!("{REPAIR_PREFIX}:{profile_identity}:{reason}:{ALGORITHM_VERSION}")
}

pub(crate) fn deterministic_random_score(
    target_kind: DisplayCoverTargetKind,
    target_public_id: &str,
    release_public_id: &str,
) -> u64 {
    let input = format!(
        "{}:{target_public_id}:{release_public_id}:{ALGORITHM_VERSION}",
        target_kind.as_db_str()
    );
    let hash = blake3::hash(input.as_bytes());
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&hash.as_bytes()[..8]);
    u64::from_le_bytes(bytes)
}

fn value<'a>(element: &'a DbElement, key: &str) -> Option<&'a DbValue> {
    element.values.iter().find_map(|kv| {
        if matches!(&kv.key, DbValue::String(found) if found == key) {
            Some(&kv.value)
        } else {
            None
        }
    })
}

fn u64_value(element: &DbElement, key: &str) -> Option<u64> {
    match value(element, key)? {
        DbValue::U64(value) => Some(*value),
        DbValue::I64(value) => u64::try_from(*value).ok(),
        _ => None,
    }
}

fn search_identity(db: &impl DbAccess, identity: &str) -> anyhow::Result<Vec<DbElement>> {
    let result = db.exec(
        QueryBuilder::search()
            .index("identity")
            .value(identity)
            .query(),
    )?;
    let ids = result.ids();
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    Ok(db.exec(QueryBuilder::select().ids(ids).query())?.elements)
}

fn find_edge_by_identity(db: &impl DbAccess, identity: &str) -> anyhow::Result<Option<DbElement>> {
    Ok(search_identity(db, identity)?
        .into_iter()
        .find(|element| element.id.0 < 0))
}

pub(crate) fn find_profile_by_identity(
    db: &impl DbAccess,
    identity: &str,
) -> anyhow::Result<Option<(DbId, DisplayCoverProfile)>> {
    for element in search_identity(db, identity)? {
        if element.id.0 <= 0 || !graph::is_element_type(&element, "DisplayCoverProfile") {
            continue;
        }
        return Ok(Some((
            element.id,
            DisplayCoverProfile::from_db_element(&element)?,
        )));
    }
    Ok(None)
}

fn repair_exists(db: &impl DbAccess, identity: &str) -> anyhow::Result<bool> {
    Ok(search_identity(db, identity)?
        .into_iter()
        .any(|element| element.id.0 > 0 && graph::is_element_type(&element, "DisplayCoverRepair")))
}

fn enqueue_repair(
    db: &mut impl DbAccess,
    target: &DisplayCoverTarget<'_>,
    profile_db_id: Option<DbId>,
    profile_identity: &str,
    user_public_id: Option<&str>,
    reason: &str,
    now_ms: u64,
) -> anyhow::Result<()> {
    let identity = repair_identity(profile_identity, reason);
    if repair_exists(db, &identity)? {
        return Ok(());
    }
    let repair = DisplayCoverRepair {
        db_id: None,
        id: nanoid!(),
        identity,
        target_kind: target.kind,
        target_public_id: target.public_id.to_string(),
        user_public_id: user_public_id.map(ToOwned::to_owned),
        reason: reason.to_string(),
        state: "pending".to_string(),
        algorithm_version: ALGORITHM_VERSION,
        created_at_ms: now_ms,
        updated_at_ms: now_ms,
    };
    let repair_db_id = db
        .exec_mut(QueryBuilder::insert().element(&repair).query())?
        .ids()
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("display cover repair insert returned no id"))?;
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(REPAIR_COLLECTION_ALIAS)
            .to(repair_db_id)
            .query(),
    )?;
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(repair_db_id)
            .to(target.db_id)
            .query(),
    )?;
    if let Some(profile_db_id) = profile_db_id {
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(repair_db_id)
                .to(profile_db_id)
                .query(),
        )?;
    }
    Ok(())
}

pub(crate) fn get_profile(
    db: &impl DbAccess,
    scope: DisplayCoverScope,
    target_kind: DisplayCoverTargetKind,
    user_public_id: Option<&str>,
    target_public_id: &str,
) -> anyhow::Result<Option<(DbId, DisplayCoverProfile)>> {
    let identity = profile_identity(scope, target_kind, user_public_id, target_public_id);
    find_profile_by_identity(db, &identity)
}

fn save_profile(
    db: &mut impl DbAccess,
    profile_db_id: DbId,
    profile: &DisplayCoverProfile,
) -> anyhow::Result<()> {
    let mut profile = profile.clone();
    profile.db_id = Some(profile_db_id.into());
    db.exec_mut(QueryBuilder::insert().element(&profile).query())?;
    Ok(())
}

pub(crate) fn ensure_profile(
    db: &mut impl DbAccess,
    scope: DisplayCoverScope,
    target: &DisplayCoverTarget<'_>,
    user: Option<&DisplayCoverUser<'_>>,
    now_ms: u64,
) -> anyhow::Result<(DbId, DisplayCoverProfile)> {
    let identity = profile_identity(
        scope,
        target.kind,
        user.map(|user| user.public_id),
        target.public_id,
    );
    if let Some((profile_db_id, mut profile)) = find_profile_by_identity(db, &identity)? {
        if profile.algorithm_version != ALGORITHM_VERSION {
            profile.algorithm_version = ALGORITHM_VERSION;
            profile.dirty = Some(true);
            profile.updated_at_ms = now_ms;
            save_profile(db, profile_db_id, &profile)?;
        }
        return Ok((profile_db_id, profile));
    }

    let profile = DisplayCoverProfile {
        db_id: None,
        id: nanoid!(),
        identity,
        target_kind: target.kind,
        scope,
        algorithm_version: ALGORITHM_VERSION,
        user_total_listens: 0,
        instance_total_listens: 0,
        active_listener_count: 0,
        dirty: Some(false),
        updated_at_ms: now_ms,
    };
    let profile_db_id = db
        .exec_mut(QueryBuilder::insert().element(&profile).query())?
        .ids()
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("display cover profile insert returned no id"))?;
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(COLLECTION_ALIAS)
            .to(profile_db_id)
            .query(),
    )?;
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(target.db_id)
            .to(profile_db_id)
            .query(),
    )?;
    if let Some(user) = user {
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(user.db_id)
                .to(profile_db_id)
                .query(),
        )?;
    }

    Ok((profile_db_id, profile))
}

fn update_profile_listens(
    db: &mut impl DbAccess,
    profile_db_id: DbId,
    mut profile: DisplayCoverProfile,
    active_listener_delta: u64,
    now_ms: u64,
) -> anyhow::Result<DisplayCoverProfile> {
    match profile.scope {
        DisplayCoverScope::User => {
            profile.user_total_listens = profile.user_total_listens.saturating_add(1);
        }
        DisplayCoverScope::Instance => {
            profile.instance_total_listens = profile.instance_total_listens.saturating_add(1);
            profile.active_listener_count = profile
                .active_listener_count
                .saturating_add(active_listener_delta);
        }
    }
    profile.dirty = Some(false);
    profile.updated_at_ms = now_ms;
    save_profile(db, profile_db_id, &profile)?;
    Ok(profile)
}

fn candidate_from_edge(edge: &DbElement) -> DisplayCoverCandidate {
    DisplayCoverCandidate {
        edge_id: edge.id,
        user_listen_count: u64_value(edge, USER_LISTEN_COUNT_KEY).unwrap_or(0),
        instance_listen_count: u64_value(edge, INSTANCE_LISTEN_COUNT_KEY).unwrap_or(0),
        last_listened_at_ms: u64_value(edge, LAST_LISTENED_AT_MS_KEY),
        random_score: u64_value(edge, RANDOM_SCORE_KEY).unwrap_or(u64::MAX),
    }
}

fn get_candidate(
    db: &impl DbAccess,
    profile_public_id: &str,
    release_public_id: &str,
) -> anyhow::Result<Option<DisplayCoverCandidate>> {
    Ok(find_edge_by_identity(
        db,
        &candidate_identity(profile_public_id, release_public_id),
    )?
    .as_ref()
    .map(candidate_from_edge))
}

fn candidate_values(
    identity: &str,
    candidate: &DisplayCoverCandidate,
    now_ms: u64,
) -> Vec<DbKeyValue> {
    let mut values = vec![
        DbKeyValue {
            key: DbValue::from("identity"),
            value: DbValue::from(identity),
        },
        DbKeyValue {
            key: DbValue::from(ROLE_KEY),
            value: DbValue::from("candidate"),
        },
        DbKeyValue {
            key: DbValue::from(USER_LISTEN_COUNT_KEY),
            value: DbValue::from(candidate.user_listen_count),
        },
        DbKeyValue {
            key: DbValue::from(INSTANCE_LISTEN_COUNT_KEY),
            value: DbValue::from(candidate.instance_listen_count),
        },
        DbKeyValue {
            key: DbValue::from(RANDOM_SCORE_KEY),
            value: DbValue::from(candidate.random_score),
        },
        DbKeyValue {
            key: DbValue::from(HAS_COVER_KEY),
            value: DbValue::from(1_u64),
        },
        DbKeyValue {
            key: DbValue::from(UPDATED_AT_MS_KEY),
            value: DbValue::from(now_ms),
        },
    ];
    if let Some(last_listened_at_ms) = candidate.last_listened_at_ms {
        values.push(DbKeyValue {
            key: DbValue::from(LAST_LISTENED_AT_MS_KEY),
            value: DbValue::from(last_listened_at_ms),
        });
    }
    values
}

fn upsert_candidate(
    db: &mut impl DbAccess,
    update: CandidateUpdate<'_>,
) -> anyhow::Result<DisplayCoverCandidate> {
    let identity = candidate_identity(update.profile_public_id, update.release_public_id);
    let mut candidate = get_candidate(db, update.profile_public_id, update.release_public_id)?
        .unwrap_or(DisplayCoverCandidate {
            edge_id: DbId(0),
            user_listen_count: 0,
            instance_listen_count: 0,
            last_listened_at_ms: None,
            random_score: update.random_score,
        });
    candidate.random_score = candidate.random_score.min(update.random_score);
    if let Some((scope, listened_at_ms)) = update.increment {
        match scope {
            CandidateIncrementScope::User => {
                candidate.user_listen_count = candidate.user_listen_count.saturating_add(1);
            }
            CandidateIncrementScope::Instance => {
                candidate.instance_listen_count = candidate.instance_listen_count.saturating_add(1);
            }
        }
        candidate.last_listened_at_ms = Some(
            candidate
                .last_listened_at_ms
                .unwrap_or(0)
                .max(listened_at_ms),
        );
    }

    if candidate.edge_id.0 == 0 {
        candidate.edge_id = db
            .exec_mut(
                QueryBuilder::insert()
                    .edges()
                    .from(update.profile_db_id)
                    .to(update.release_db_id)
                    .query(),
            )?
            .ids()
            .first()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("display cover candidate edge missing id"))?;
    }
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform(candidate_values(&identity, &candidate, update.now_ms))
            .ids(candidate.edge_id)
            .query(),
    )?;
    Ok(candidate)
}

fn winner_from_edge(edge: &DbElement) -> DisplayCoverWinner {
    DisplayCoverWinner {
        release_db_id: edge.to,
        listen_count: u64_value(edge, LISTEN_COUNT_KEY).unwrap_or(0),
        random_score: u64_value(edge, RANDOM_SCORE_KEY).unwrap_or(u64::MAX),
    }
}

pub(crate) fn get_winner(
    db: &impl DbAccess,
    profile: &DisplayCoverProfile,
    winner_kind: DisplayCoverWinnerKind,
) -> anyhow::Result<Option<DisplayCoverWinner>> {
    Ok(
        find_edge_by_identity(db, &winner_identity(&profile.id, winner_kind))?
            .as_ref()
            .map(winner_from_edge),
    )
}

fn winner_values(
    identity: &str,
    winner_kind: DisplayCoverWinnerKind,
    listen_count: u64,
    random_score: u64,
    now_ms: u64,
) -> Vec<DbKeyValue> {
    vec![
        DbKeyValue {
            key: DbValue::from("identity"),
            value: DbValue::from(identity),
        },
        DbKeyValue {
            key: DbValue::from(ROLE_KEY),
            value: DbValue::from("winner"),
        },
        DbKeyValue {
            key: DbValue::from(WINNER_KIND_KEY),
            value: DbValue::from(winner_kind.as_db_str()),
        },
        DbKeyValue {
            key: DbValue::from(LISTEN_COUNT_KEY),
            value: DbValue::from(listen_count),
        },
        DbKeyValue {
            key: DbValue::from(RANDOM_SCORE_KEY),
            value: DbValue::from(random_score),
        },
        DbKeyValue {
            key: DbValue::from(UPDATED_AT_MS_KEY),
            value: DbValue::from(now_ms),
        },
    ]
}

fn replace_winner(db: &mut impl DbAccess, update: &WinnerUpdate<'_>) -> anyhow::Result<()> {
    let identity = winner_identity(update.profile_public_id, update.winner_kind);
    if let Some(existing) = find_edge_by_identity(db, &identity)? {
        db.exec_mut(QueryBuilder::remove().ids(existing.id).query())?;
    }
    let edge_id = db
        .exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(update.profile_db_id)
                .to(update.release_db_id)
                .query(),
        )?
        .ids()
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("display cover winner edge missing id"))?;
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform(winner_values(
                &identity,
                update.winner_kind,
                update.listen_count,
                update.random_score,
                update.now_ms,
            ))
            .ids(edge_id)
            .query(),
    )?;
    Ok(())
}

fn set_winner_if_better(
    db: &mut impl DbAccess,
    profile: &DisplayCoverProfile,
    update: WinnerUpdate<'_>,
) -> anyhow::Result<()> {
    let current = get_winner(db, profile, update.winner_kind)?;
    let replace = match (update.winner_kind, current.as_ref()) {
        (_, None) => true,
        (DisplayCoverWinnerKind::Random, Some(current)) => {
            update.random_score < current.random_score
        }
        (_, Some(current)) => update.listen_count > current.listen_count,
    };
    if replace {
        replace_winner(db, &update)?;
    }
    Ok(())
}

fn candidate_update<'a>(
    profile_db_id: DbId,
    profile_public_id: &'a str,
    release_db_id: DbId,
    release_public_id: &'a str,
    random_score: u64,
    now_ms: u64,
    increment: Option<(CandidateIncrementScope, u64)>,
) -> CandidateUpdate<'a> {
    CandidateUpdate {
        profile_db_id,
        profile_public_id,
        release_db_id,
        release_public_id,
        random_score,
        now_ms,
        increment,
    }
}

fn winner_update<'a>(
    profile_db_id: DbId,
    profile_public_id: &'a str,
    release_db_id: DbId,
    winner_kind: DisplayCoverWinnerKind,
    listen_count: u64,
    random_score: u64,
    now_ms: u64,
) -> WinnerUpdate<'a> {
    WinnerUpdate {
        profile_db_id,
        profile_public_id,
        release_db_id,
        winner_kind,
        listen_count,
        random_score,
        now_ms,
    }
}

fn release_target(
    db: &impl DbAccess,
    release_db_id: DbId,
) -> anyhow::Result<Option<(String, releases::Release)>> {
    let Some(release) = releases::get_by_id(db, release_db_id)? else {
        return Ok(None);
    };
    Ok(Some((release.id.clone(), release)))
}

fn genre_target(
    db: &impl DbAccess,
    genre_db_id: DbId,
) -> anyhow::Result<Option<(String, genres::Genre)>> {
    let Some(genre) = genres::get_by_id(db, genre_db_id)? else {
        return Ok(None);
    };
    Ok(Some((genre.id.clone(), genre)))
}

pub(crate) fn ensure_genre_release_random_candidate(
    db: &mut impl DbAccess,
    genre_db_id: DbId,
    release_db_id: DbId,
    now_ms: u64,
) -> anyhow::Result<()> {
    if super::get(db, release_db_id)?.is_none() {
        return Ok(());
    }
    let Some((genre_public_id, _)) = genre_target(db, genre_db_id)? else {
        return Ok(());
    };
    let Some((release_public_id, _)) = release_target(db, release_db_id)? else {
        return Ok(());
    };
    let target = DisplayCoverTarget {
        kind: DisplayCoverTargetKind::Genre,
        db_id: genre_db_id,
        public_id: &genre_public_id,
    };
    let (profile_db_id, profile) =
        ensure_profile(db, DisplayCoverScope::Instance, &target, None, now_ms)?;
    let random_score = deterministic_random_score(
        DisplayCoverTargetKind::Genre,
        &genre_public_id,
        &release_public_id,
    );
    upsert_candidate(
        db,
        candidate_update(
            profile_db_id,
            &profile.id,
            release_db_id,
            &release_public_id,
            random_score,
            now_ms,
            None,
        ),
    )?;
    set_winner_if_better(
        db,
        &profile,
        winner_update(
            profile_db_id,
            &profile.id,
            release_db_id,
            DisplayCoverWinnerKind::Random,
            0,
            random_score,
            now_ms,
        ),
    )?;
    Ok(())
}

pub(crate) fn sync_release_random_candidates(
    db: &mut impl DbAccess,
    release_db_id: DbId,
) -> anyhow::Result<()> {
    if releases::get_by_id(db, release_db_id)?.is_none() || super::get(db, release_db_id)?.is_none()
    {
        return Ok(());
    }
    let now_ms = now_ms();
    for genre in genres::get_for_release(db, release_db_id)? {
        let Some(genre_db_id) = genre.db_id.map(DbId::from) else {
            continue;
        };
        ensure_genre_release_random_candidate(db, genre_db_id, release_db_id, now_ms)?;
    }
    Ok(())
}

pub(crate) fn record_genre_listen(
    db: &mut impl DbAccess,
    track_db_id: DbId,
    user_db_id: DbId,
    listened_at_ms: u64,
) -> anyhow::Result<()> {
    let Some(user) = users::get_by_id(db, user_db_id)? else {
        return Ok(());
    };
    let user = DisplayCoverUser {
        db_id: user_db_id,
        public_id: &user.id,
    };
    let releases = releases::get_by_track(db, track_db_id)?;
    let mut seen_release_genres = HashSet::new();
    let mut incremented_genres = HashSet::new();
    for release in releases {
        let Some(release_db_id) = release.db_id.clone().map(DbId::from) else {
            continue;
        };
        let release_public_id = release.id;
        let has_cover = super::get(db, release_db_id)?.is_some();
        for genre in genres::get_for_release(db, release_db_id)? {
            let Some(genre_db_id) = genre.db_id.clone().map(DbId::from) else {
                continue;
            };
            if !seen_release_genres.insert((release_db_id, genre_db_id)) {
                continue;
            }
            let target = DisplayCoverTarget {
                kind: DisplayCoverTargetKind::Genre,
                db_id: genre_db_id,
                public_id: &genre.id,
            };
            let (user_profile_db_id, user_profile) = ensure_profile(
                db,
                DisplayCoverScope::User,
                &target,
                Some(&user),
                listened_at_ms,
            )?;
            let user_was_inactive = user_profile.user_total_listens == 0;
            let (instance_profile_db_id, instance_profile) = ensure_profile(
                db,
                DisplayCoverScope::Instance,
                &target,
                None,
                listened_at_ms,
            )?;

            let increment_totals = incremented_genres.insert(genre_db_id);
            let user_profile = if increment_totals {
                update_profile_listens(db, user_profile_db_id, user_profile, 0, listened_at_ms)?
            } else {
                user_profile
            };
            let instance_profile = if increment_totals {
                update_profile_listens(
                    db,
                    instance_profile_db_id,
                    instance_profile,
                    u64::from(user_was_inactive),
                    listened_at_ms,
                )?
            } else {
                instance_profile
            };

            if !has_cover {
                continue;
            }

            let random_score = deterministic_random_score(
                DisplayCoverTargetKind::Genre,
                &genre.id,
                &release_public_id,
            );
            let user_candidate = upsert_candidate(
                db,
                candidate_update(
                    user_profile_db_id,
                    &user_profile.id,
                    release_db_id,
                    &release_public_id,
                    random_score,
                    listened_at_ms,
                    Some((CandidateIncrementScope::User, listened_at_ms)),
                ),
            )?;
            set_winner_if_better(
                db,
                &user_profile,
                winner_update(
                    user_profile_db_id,
                    &user_profile.id,
                    release_db_id,
                    DisplayCoverWinnerKind::Personal,
                    user_candidate.user_listen_count,
                    random_score,
                    listened_at_ms,
                ),
            )?;

            let instance_candidate = upsert_candidate(
                db,
                candidate_update(
                    instance_profile_db_id,
                    &instance_profile.id,
                    release_db_id,
                    &release_public_id,
                    random_score,
                    listened_at_ms,
                    Some((CandidateIncrementScope::Instance, listened_at_ms)),
                ),
            )?;
            set_winner_if_better(
                db,
                &instance_profile,
                winner_update(
                    instance_profile_db_id,
                    &instance_profile.id,
                    release_db_id,
                    DisplayCoverWinnerKind::Instance,
                    instance_candidate.instance_listen_count,
                    random_score,
                    listened_at_ms,
                ),
            )?;
            set_winner_if_better(
                db,
                &instance_profile,
                winner_update(
                    instance_profile_db_id,
                    &instance_profile.id,
                    release_db_id,
                    DisplayCoverWinnerKind::Random,
                    0,
                    random_score,
                    listened_at_ms,
                ),
            )?;
        }
    }
    Ok(())
}

pub(crate) fn mark_genre_profiles_dirty_for_release(
    db: &mut impl DbAccess,
    release_db_id: DbId,
) -> anyhow::Result<()> {
    let now_ms = now_ms();
    for genre in genres::get_for_release(db, release_db_id)? {
        let Some(genre_db_id) = genre.db_id.map(DbId::from) else {
            continue;
        };
        let target = DisplayCoverTarget {
            kind: DisplayCoverTargetKind::Genre,
            db_id: genre_db_id,
            public_id: &genre.id,
        };
        let profiles: Vec<DisplayCoverProfile> = db
            .exec(
                QueryBuilder::select()
                    .elements::<DisplayCoverProfile>()
                    .search()
                    .from(genre_db_id)
                    .where_()
                    .neighbor()
                    .end_where()
                    .query(),
            )?
            .try_into()?;
        for profile in profiles {
            let Some(profile_db_id) = profile.db_id.clone().map(DbId::from) else {
                continue;
            };
            if profile.target_kind != DisplayCoverTargetKind::Genre {
                continue;
            }
            let mut profile = profile;
            profile.dirty = Some(true);
            profile.updated_at_ms = now_ms;
            save_profile(db, profile_db_id, &profile)?;
            enqueue_repair(
                db,
                &target,
                Some(profile_db_id),
                &profile.identity,
                None,
                "release_cover_changed",
                now_ms,
            )?;
        }
    }
    Ok(())
}

pub(crate) fn mark_release_track_link_changed(
    db: &mut impl DbAccess,
    from_id: DbId,
    to_id: DbId,
) -> anyhow::Result<()> {
    if releases::get_by_id(db, from_id)?.is_some() && tracks::get_by_id(db, to_id)?.is_some() {
        mark_genre_profiles_dirty_for_release(db, from_id)?;
    }
    Ok(())
}

pub(crate) fn profile_is_clean(profile: &DisplayCoverProfile) -> bool {
    profile.algorithm_version == ALGORITHM_VERSION && !profile.dirty.unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{
        covers,
        genres,
        releases,
        test_db::{
            connect,
            insert_release,
            insert_track,
            new_test_db,
            test_user,
        },
        users,
    };

    fn insert_cover(db: &mut agdb::DbAny, release_db_id: DbId, id: &str) -> anyhow::Result<()> {
        covers::upsert(
            db,
            release_db_id,
            covers::Cover {
                db_id: None,
                id: id.to_string(),
                path: format!("/tmp/{id}.jpg"),
                mime_type: "image/jpeg".to_string(),
                hash: "a".repeat(64),
                blurhash: None,
            },
        )?;
        Ok(())
    }

    #[test]
    fn genre_listen_updates_personal_and_instance_winners() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = users::create(&mut db, &test_user("covers")?)?;
        let genre_db_id = genres::resolve_by_name(&mut db, "Rock")?;
        let release_a = insert_release(&mut db, "A")?;
        let release_b = insert_release(&mut db, "B")?;
        let track_a = insert_track(&mut db, "A1")?;
        let track_b = insert_track(&mut db, "B1")?;
        connect(&mut db, release_a, track_a)?;
        connect(&mut db, release_b, track_b)?;
        genres::link_to_release(&mut db, genre_db_id, release_a)?;
        genres::link_to_release(&mut db, genre_db_id, release_b)?;
        insert_cover(&mut db, release_a, "a")?;
        insert_cover(&mut db, release_b, "b")?;

        record_genre_listen(&mut db, track_a, user_db_id, 1_000)?;
        record_genre_listen(&mut db, track_b, user_db_id, 2_000)?;
        record_genre_listen(&mut db, track_b, user_db_id, 3_000)?;

        let user = users::get_by_id(&db, user_db_id)?.expect("user should exist");
        let genre = genres::get_by_id(&db, genre_db_id)?.expect("genre should exist");
        let (_, profile) = get_profile(
            &db,
            DisplayCoverScope::User,
            DisplayCoverTargetKind::Genre,
            Some(&user.id),
            &genre.id,
        )?
        .expect("user profile should exist");
        let winner = get_winner(&db, &profile, DisplayCoverWinnerKind::Personal)?
            .expect("personal winner should exist");

        assert_eq!(winner.release_db_id, release_b);
        assert_eq!(winner.listen_count, 2);

        let (_, instance_profile) = get_profile(
            &db,
            DisplayCoverScope::Instance,
            DisplayCoverTargetKind::Genre,
            None,
            &genre.id,
        )?
        .expect("instance profile should exist");
        let instance = get_winner(&db, &instance_profile, DisplayCoverWinnerKind::Instance)?
            .expect("instance winner should exist");
        assert_eq!(instance.release_db_id, release_b);
        assert_eq!(instance.listen_count, 2);
        assert_eq!(instance_profile.instance_total_listens, 3);
        assert_eq!(instance_profile.active_listener_count, 1);
        Ok(())
    }

    #[test]
    fn random_winner_is_deterministic_for_genre_release() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let genre_db_id = genres::resolve_by_name(&mut db, "Jazz")?;
        let release_a = insert_release(&mut db, "A")?;
        let release_b = insert_release(&mut db, "B")?;
        genres::link_to_release(&mut db, genre_db_id, release_a)?;
        genres::link_to_release(&mut db, genre_db_id, release_b)?;
        insert_cover(&mut db, release_a, "a")?;
        insert_cover(&mut db, release_b, "b")?;

        sync_release_random_candidates(&mut db, release_a)?;
        sync_release_random_candidates(&mut db, release_b)?;

        let genre = genres::get_by_id(&db, genre_db_id)?.expect("genre should exist");
        let (_, profile) = get_profile(
            &db,
            DisplayCoverScope::Instance,
            DisplayCoverTargetKind::Genre,
            None,
            &genre.id,
        )?
        .expect("instance profile should exist");
        let winner = get_winner(&db, &profile, DisplayCoverWinnerKind::Random)?
            .expect("random winner should exist");
        let release_a_public_id = releases::get_by_id(&db, release_a)?.unwrap().id;
        let release_b_public_id = releases::get_by_id(&db, release_b)?.unwrap().id;
        let expected = if deterministic_random_score(
            DisplayCoverTargetKind::Genre,
            &genre.id,
            &release_a_public_id,
        ) < deterministic_random_score(
            DisplayCoverTargetKind::Genre,
            &genre.id,
            &release_b_public_id,
        ) {
            release_a
        } else {
            release_b
        };

        assert_eq!(winner.release_db_id, expected);
        Ok(())
    }

    #[test]
    fn cover_removal_dirties_profile_and_enqueues_repair() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let genre_db_id = genres::resolve_by_name(&mut db, "Pop")?;
        let release = insert_release(&mut db, "Covered Pop")?;
        genres::link_to_release(&mut db, genre_db_id, release)?;
        insert_cover(&mut db, release, "pop")?;

        let genre = genres::get_by_id(&db, genre_db_id)?.expect("genre should exist");
        let (_, profile) = get_profile(
            &db,
            DisplayCoverScope::Instance,
            DisplayCoverTargetKind::Genre,
            None,
            &genre.id,
        )?
        .expect("instance profile should exist");
        assert!(!profile.dirty.unwrap_or(false));

        covers::remove(&mut db, release)?;

        let (_, profile) = get_profile(
            &db,
            DisplayCoverScope::Instance,
            DisplayCoverTargetKind::Genre,
            None,
            &genre.id,
        )?
        .expect("instance profile should still exist");
        assert_eq!(profile.dirty, Some(true));

        let repairs: Vec<DisplayCoverRepair> = db
            .exec(
                QueryBuilder::select()
                    .elements::<DisplayCoverRepair>()
                    .search()
                    .from("display_cover_repairs")
                    .query(),
            )?
            .try_into()?;
        assert_eq!(repairs.len(), 1);
        assert_eq!(repairs[0].state, "pending");
        assert_eq!(repairs[0].reason, "release_cover_changed");
        Ok(())
    }
}
