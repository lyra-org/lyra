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
    playlists,
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

fn remove_winner(db: &mut impl DbAccess, identity: &str) -> anyhow::Result<()> {
    if let Some(existing) = find_edge_by_identity(db, identity)? {
        db.exec_mut(QueryBuilder::remove().ids(existing.id).query())?;
    }
    Ok(())
}

fn replace_winner(db: &mut impl DbAccess, update: &WinnerUpdate<'_>) -> anyhow::Result<()> {
    let identity = winner_identity(update.profile_public_id, update.winner_kind);
    remove_winner(db, &identity)?;
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

/// Releases reachable from a playlist's entries that currently have a cover,
/// paired with their public ids for deterministic scoring.
fn playlist_cover_candidates(
    db: &impl DbAccess,
    playlist_db_id: DbId,
) -> anyhow::Result<Vec<(DbId, String)>> {
    let entries = playlists::get_tracks(db, playlist_db_id)?;
    let edge_ids = entries
        .iter()
        .map(|entry| entry.edge_id)
        .collect::<Vec<_>>();
    let mut candidates = Vec::new();
    let mut seen = HashSet::new();
    for track_db_id in playlists::resolve_edge_targets(db, &edge_ids)? {
        for release in releases::get_by_track(db, track_db_id)? {
            let Some(release_db_id) = release.db_id.clone().map(DbId::from) else {
                continue;
            };
            if !seen.insert(release_db_id) {
                continue;
            }
            if super::get(db, release_db_id)?.is_none() {
                continue;
            }
            candidates.push((release_db_id, release.id));
        }
    }
    Ok(candidates)
}

/// Recompute a playlist's display cover from its current membership.
///
/// Playlist membership is authoritative and fully known at call time, so the
/// winner is replaced outright rather than accumulated through candidate edges
/// the way listen-driven genre winners are.
pub(crate) fn sync_playlist_cover(
    db: &mut impl DbAccess,
    playlist_db_id: DbId,
) -> anyhow::Result<()> {
    #[cfg(test)]
    PLAYLIST_COVER_SYNC_COUNT.with(|count| count.set(count.get().saturating_add(1)));

    let Some(playlist) = playlists::get_by_id(db, playlist_db_id)? else {
        return Ok(());
    };
    let now_ms = now_ms();
    let target = DisplayCoverTarget {
        kind: DisplayCoverTargetKind::Playlist,
        db_id: playlist_db_id,
        public_id: &playlist.id,
    };
    let (profile_db_id, mut profile) =
        ensure_profile(db, DisplayCoverScope::Instance, &target, None, now_ms)?;

    let winner = playlist_cover_candidates(db, playlist_db_id)?
        .into_iter()
        .map(|(release_db_id, release_public_id)| {
            let random_score = deterministic_random_score(
                DisplayCoverTargetKind::Playlist,
                &playlist.id,
                &release_public_id,
            );
            (random_score, release_public_id, release_db_id)
        })
        .min_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));

    match winner {
        Some((random_score, _, release_db_id)) => replace_winner(
            db,
            &winner_update(
                profile_db_id,
                &profile.id,
                release_db_id,
                DisplayCoverWinnerKind::Random,
                0,
                random_score,
                now_ms,
            ),
        ),
        None => remove_winner(
            db,
            &winner_identity(&profile.id, DisplayCoverWinnerKind::Random),
        ),
    }?;
    mark_profile_current(db, profile_db_id, &mut profile, now_ms)
}

fn mark_profile_current(
    db: &mut impl DbAccess,
    profile_db_id: DbId,
    profile: &mut DisplayCoverProfile,
    now_ms: u64,
) -> anyhow::Result<()> {
    if profile_is_clean(profile) {
        return Ok(());
    }
    profile.algorithm_version = ALGORITHM_VERSION;
    profile.dirty = Some(false);
    profile.updated_at_ms = now_ms;
    save_profile(db, profile_db_id, profile)
}

/// Remove a playlist's display-cover profile before deleting the playlist node.
/// The playlist's public id is required to locate the separately rooted profile.
pub(crate) fn remove_playlist_cover_profile(
    db: &mut impl DbAccess,
    playlist_db_id: DbId,
) -> anyhow::Result<()> {
    let Some(playlist) = playlists::get_by_id(db, playlist_db_id)? else {
        return Ok(());
    };
    let identity = profile_identity(
        DisplayCoverScope::Instance,
        DisplayCoverTargetKind::Playlist,
        None,
        &playlist.id,
    );
    let Some((profile_db_id, _)) = find_profile_by_identity(db, &identity)? else {
        return Ok(());
    };
    db.exec_mut(QueryBuilder::remove().ids(profile_db_id).query())?;
    Ok(())
}

/// Offer a single release to a playlist's display cover.
///
/// The winner is the minimum deterministic score over the playlist's covered
/// releases, so a newly covered release only has to beat a current incumbent.
/// Dirty profiles delegate to [`sync_playlist_cover`] before they can be marked
/// current.
pub(crate) fn offer_playlist_cover_candidate(
    db: &mut impl DbAccess,
    playlist_db_id: DbId,
    release_db_id: DbId,
) -> anyhow::Result<()> {
    let Some(playlist) = playlists::get_by_id(db, playlist_db_id)? else {
        return Ok(());
    };
    let Some(release) = releases::get_by_id(db, release_db_id)? else {
        return Ok(());
    };
    if super::get(db, release_db_id)?.is_none() {
        return Ok(());
    }

    let now_ms = now_ms();
    let target = DisplayCoverTarget {
        kind: DisplayCoverTargetKind::Playlist,
        db_id: playlist_db_id,
        public_id: &playlist.id,
    };
    let (profile_db_id, profile) =
        ensure_profile(db, DisplayCoverScope::Instance, &target, None, now_ms)?;
    if !profile_is_clean(&profile) {
        return sync_playlist_cover(db, playlist_db_id);
    }
    let random_score =
        deterministic_random_score(DisplayCoverTargetKind::Playlist, &playlist.id, &release.id);
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

/// A track joined a playlist: offer each of its releases as a cover candidate.
pub(crate) fn offer_track_to_playlist_cover(
    db: &mut impl DbAccess,
    playlist_db_id: DbId,
    track_db_id: DbId,
) -> anyhow::Result<()> {
    for release in releases::get_by_track(db, track_db_id)? {
        let Some(release_db_id) = release.db_id.clone().map(DbId::from) else {
            continue;
        };
        offer_playlist_cover_candidate(db, playlist_db_id, release_db_id)?;
    }
    Ok(())
}

/// Playlists holding at least one track of `release_db_id`.
fn playlists_for_release(db: &impl DbAccess, release_db_id: DbId) -> anyhow::Result<Vec<DbId>> {
    let mut playlist_db_ids = Vec::new();
    let mut seen = HashSet::new();
    for track in tracks::get_direct(db, release_db_id)? {
        let Some(track_db_id) = track.db_id.clone().map(DbId::from) else {
            continue;
        };
        for playlist_db_id in playlists::get_by_track(db, track_db_id)? {
            if seen.insert(playlist_db_id) {
                playlist_db_ids.push(playlist_db_id);
            }
        }
    }
    Ok(playlist_db_ids)
}

/// A release gained a cover: offer it to every playlist holding one of its
/// tracks without rescanning each playlist's entries.
pub(crate) fn offer_release_to_playlist_covers(
    db: &mut impl DbAccess,
    release_db_id: DbId,
) -> anyhow::Result<()> {
    for playlist_db_id in playlists_for_release(db, release_db_id)? {
        offer_playlist_cover_candidate(db, playlist_db_id, release_db_id)?;
    }
    Ok(())
}

/// A release lost its cover, so playlists holding its current tracks need a
/// full recompute in case it was their winner.
pub(crate) fn resync_playlist_covers_for_release(
    db: &mut impl DbAccess,
    release_db_id: DbId,
) -> anyhow::Result<()> {
    for playlist_db_id in playlists_for_release(db, release_db_id)? {
        sync_playlist_cover(db, playlist_db_id)?;
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

pub(crate) fn offer_release_to_playlists_with_track(
    db: &mut impl DbAccess,
    release_db_id: DbId,
    track_db_id: DbId,
) -> anyhow::Result<()> {
    for playlist_db_id in playlists::get_by_track(db, track_db_id)? {
        offer_playlist_cover_candidate(db, playlist_db_id, release_db_id)?;
    }
    Ok(())
}

pub(crate) fn resync_playlists_with_track(
    db: &mut impl DbAccess,
    track_db_id: DbId,
) -> anyhow::Result<()> {
    for playlist_db_id in playlists::get_by_track(db, track_db_id)? {
        sync_playlist_cover(db, playlist_db_id)?;
    }
    Ok(())
}

pub(crate) fn playlist_ids_for_tracks(
    db: &impl DbAccess,
    track_db_ids: &[DbId],
) -> anyhow::Result<Vec<DbId>> {
    let tracks = tracks::get_by_ids(db, track_db_ids)?;
    let mut playlist_db_ids = Vec::new();
    let mut seen = HashSet::new();
    for track_db_id in tracks.keys().copied() {
        for playlist_db_id in playlists::get_by_track(db, track_db_id)? {
            if seen.insert(playlist_db_id) {
                playlist_db_ids.push(playlist_db_id);
            }
        }
    }
    Ok(playlist_db_ids)
}

pub(crate) fn sync_playlist_covers(
    db: &mut impl DbAccess,
    playlist_db_ids: &[DbId],
) -> anyhow::Result<()> {
    for playlist_db_id in playlist_db_ids.iter().copied() {
        sync_playlist_cover(db, playlist_db_id)?;
    }
    Ok(())
}

#[cfg(test)]
std::thread_local! {
    static PLAYLIST_COVER_SYNC_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_playlist_cover_sync_count() {
    PLAYLIST_COVER_SYNC_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn playlist_cover_sync_count() -> usize {
    PLAYLIST_COVER_SYNC_COUNT.with(std::cell::Cell::get)
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
        db.transaction_mut(|t| {
            covers::upsert(
                t,
                release_db_id,
                covers::Cover {
                    db_id: None,
                    id: id.to_string(),
                    path: format!("/tmp/{id}.jpg"),
                    mime_type: "image/jpeg".to_string(),
                    hash: "a".repeat(64),
                    blurhash: None,
                },
            )
        })?;
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

    fn insert_playlist(db: &mut agdb::DbAny, user_db_id: DbId) -> anyhow::Result<(DbId, String)> {
        let playlist = crate::db::playlists::Playlist {
            db_id: None,
            id: nanoid!(),
            name: "Display Cover Playlist".to_string(),
            description: None,
            is_public: None,
            created_at: None,
            updated_at: None,
        };
        let playlist_db_id = crate::db::playlists::create(db, &playlist, user_db_id)?;
        Ok((playlist_db_id, playlist.id))
    }

    #[test]
    fn playlist_cover_tracks_membership() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = users::create(&mut db, &test_user("playlist-covers")?)?;
        let release_db_id = insert_release(&mut db, "Covered")?;
        let track_db_id = insert_track(&mut db, "Covered Track")?;
        connect(&mut db, release_db_id, track_db_id)?;
        insert_cover(&mut db, release_db_id, "playlist-winner")?;

        let (playlist_db_id, playlist_public_id) = insert_playlist(&mut db, user_db_id)?;
        let entry = db
            .transaction_mut(|t| crate::db::playlists::add_track(t, playlist_db_id, track_db_id))?;
        sync_playlist_cover(&mut db, playlist_db_id)?;

        let (_, profile) = get_profile(
            &db,
            DisplayCoverScope::Instance,
            DisplayCoverTargetKind::Playlist,
            None,
            &playlist_public_id,
        )?
        .expect("playlist profile should exist");
        let winner = get_winner(&db, &profile, DisplayCoverWinnerKind::Random)?
            .expect("playlist winner should exist");
        assert_eq!(winner.release_db_id, release_db_id);

        crate::db::playlists::remove_track(&mut db, entry.edge_id)?;
        sync_playlist_cover(&mut db, playlist_db_id)?;

        let (_, profile) = get_profile(
            &db,
            DisplayCoverScope::Instance,
            DisplayCoverTargetKind::Playlist,
            None,
            &playlist_public_id,
        )?
        .expect("playlist profile should still exist");
        assert!(
            get_winner(&db, &profile, DisplayCoverWinnerKind::Random)?.is_none(),
            "winner should be cleared once the playlist holds no covered release"
        );

        Ok(())
    }

    #[test]
    fn playlist_sync_leaves_profile_current() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = users::create(&mut db, &test_user("playlist-dirty")?)?;
        let release_db_id = insert_release(&mut db, "Covered")?;
        let track_db_id = insert_track(&mut db, "Covered Track")?;
        connect(&mut db, release_db_id, track_db_id)?;
        insert_cover(&mut db, release_db_id, "clean")?;

        let (playlist_db_id, playlist_public_id) = insert_playlist(&mut db, user_db_id)?;
        db.transaction_mut(|t| crate::db::playlists::add_track(t, playlist_db_id, track_db_id))?;
        sync_playlist_cover(&mut db, playlist_db_id)?;

        // Simulate the state an ALGORITHM_VERSION bump leaves behind. Without a
        // listen-driven writer, nothing else would ever clear this, and the
        // playlist would report no cover forever.
        let (profile_db_id, mut profile) = get_profile(
            &db,
            DisplayCoverScope::Instance,
            DisplayCoverTargetKind::Playlist,
            None,
            &playlist_public_id,
        )?
        .expect("playlist profile should exist");
        profile.dirty = Some(true);
        save_profile(&mut db, profile_db_id, &profile)?;

        sync_playlist_cover(&mut db, playlist_db_id)?;

        let (_, profile) = get_profile(
            &db,
            DisplayCoverScope::Instance,
            DisplayCoverTargetKind::Playlist,
            None,
            &playlist_public_id,
        )?
        .expect("playlist profile should exist");
        assert!(
            profile_is_clean(&profile),
            "a recomputed playlist profile is current by construction"
        );

        Ok(())
    }

    #[test]
    fn playlist_offer_recomputes_an_old_algorithm_profile() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = users::create(&mut db, &test_user("playlist-version")?)?;
        let release_a = insert_release(&mut db, "A")?;
        let release_b = insert_release(&mut db, "B")?;
        let track_a = insert_track(&mut db, "A1")?;
        let track_b = insert_track(&mut db, "B1")?;
        connect(&mut db, release_a, track_a)?;
        connect(&mut db, release_b, track_b)?;
        insert_cover(&mut db, release_a, "version-a")?;
        insert_cover(&mut db, release_b, "version-b")?;

        let (playlist_db_id, playlist_public_id) = insert_playlist(&mut db, user_db_id)?;
        for track_db_id in [track_a, track_b] {
            db.transaction_mut(|t| {
                crate::db::playlists::add_track(t, playlist_db_id, track_db_id)
            })?;
        }
        sync_playlist_cover(&mut db, playlist_db_id)?;

        let release_a_public_id = releases::get_by_id(&db, release_a)?.unwrap().id;
        let release_b_public_id = releases::get_by_id(&db, release_b)?.unwrap().id;
        let (expected, stale) = if deterministic_random_score(
            DisplayCoverTargetKind::Playlist,
            &playlist_public_id,
            &release_a_public_id,
        ) < deterministic_random_score(
            DisplayCoverTargetKind::Playlist,
            &playlist_public_id,
            &release_b_public_id,
        ) {
            (release_a, release_b)
        } else {
            (release_b, release_a)
        };

        let (profile_db_id, mut profile) = get_profile(
            &db,
            DisplayCoverScope::Instance,
            DisplayCoverTargetKind::Playlist,
            None,
            &playlist_public_id,
        )?
        .expect("playlist profile should exist");
        replace_winner(
            &mut db,
            &winner_update(
                profile_db_id,
                &profile.id,
                stale,
                DisplayCoverWinnerKind::Random,
                0,
                0,
                now_ms(),
            ),
        )?;
        profile.algorithm_version = 0;
        profile.dirty = Some(false);
        save_profile(&mut db, profile_db_id, &profile)?;

        offer_playlist_cover_candidate(&mut db, playlist_db_id, expected)?;

        let (_, profile) = get_profile(
            &db,
            DisplayCoverScope::Instance,
            DisplayCoverTargetKind::Playlist,
            None,
            &playlist_public_id,
        )?
        .expect("playlist profile should still exist");
        assert!(profile_is_clean(&profile));
        let winner = get_winner(&db, &profile, DisplayCoverWinnerKind::Random)?
            .expect("playlist winner should exist");
        assert_eq!(winner.release_db_id, expected);

        Ok(())
    }

    #[test]
    fn removing_a_track_node_resyncs_its_playlist_cover() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = users::create(&mut db, &test_user("playlist-cascade")?)?;
        let release_db_id = insert_release(&mut db, "Covered")?;
        let playlist_track_db_id = insert_track(&mut db, "Playlist Track")?;
        let surviving_track_db_id = insert_track(&mut db, "Surviving Track")?;
        connect(&mut db, release_db_id, playlist_track_db_id)?;
        connect(&mut db, release_db_id, surviving_track_db_id)?;
        insert_cover(&mut db, release_db_id, "cascade")?;

        let (playlist_db_id, playlist_public_id) = insert_playlist(&mut db, user_db_id)?;
        db.transaction_mut(|t| {
            crate::db::playlists::add_track(t, playlist_db_id, playlist_track_db_id)
        })?;
        sync_playlist_cover(&mut db, playlist_db_id)?;

        crate::db::metadata::cascade_remove_entities(&mut db, &[playlist_track_db_id])?;

        assert!(crate::db::playlists::get_tracks(&db, playlist_db_id)?.is_empty());
        let (_, profile) = get_profile(
            &db,
            DisplayCoverScope::Instance,
            DisplayCoverTargetKind::Playlist,
            None,
            &playlist_public_id,
        )?
        .expect("playlist profile should still exist");
        assert!(get_winner(&db, &profile, DisplayCoverWinnerKind::Random)?.is_none());
        assert!(releases::get_by_id(&db, release_db_id)?.is_some());

        Ok(())
    }

    #[test]
    fn deleting_a_playlist_removes_its_cover_profile() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = users::create(&mut db, &test_user("playlist-delete")?)?;
        let release_db_id = insert_release(&mut db, "Covered")?;
        let track_db_id = insert_track(&mut db, "Covered Track")?;
        connect(&mut db, release_db_id, track_db_id)?;
        insert_cover(&mut db, release_db_id, "doomed")?;

        let (playlist_db_id, playlist_public_id) = insert_playlist(&mut db, user_db_id)?;
        db.transaction_mut(|t| crate::db::playlists::add_track(t, playlist_db_id, track_db_id))?;
        sync_playlist_cover(&mut db, playlist_db_id)?;
        assert!(
            get_profile(
                &db,
                DisplayCoverScope::Instance,
                DisplayCoverTargetKind::Playlist,
                None,
                &playlist_public_id,
            )?
            .is_some()
        );

        crate::db::playlists::delete(&mut db, playlist_db_id)?;

        assert!(
            get_profile(
                &db,
                DisplayCoverScope::Instance,
                DisplayCoverTargetKind::Playlist,
                None,
                &playlist_public_id,
            )?
            .is_none(),
            "profile must not outlive the playlist that owns it"
        );

        Ok(())
    }

    #[test]
    fn playlist_cover_winner_is_deterministic() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = users::create(&mut db, &test_user("playlist-random")?)?;
        let release_a = insert_release(&mut db, "A")?;
        let release_b = insert_release(&mut db, "B")?;
        let track_a = insert_track(&mut db, "A1")?;
        let track_b = insert_track(&mut db, "B1")?;
        connect(&mut db, release_a, track_a)?;
        connect(&mut db, release_b, track_b)?;
        insert_cover(&mut db, release_a, "a")?;
        insert_cover(&mut db, release_b, "b")?;

        let (playlist_db_id, playlist_public_id) = insert_playlist(&mut db, user_db_id)?;
        for track_db_id in [track_a, track_b] {
            db.transaction_mut(|t| {
                crate::db::playlists::add_track(t, playlist_db_id, track_db_id)
            })?;
        }
        sync_playlist_cover(&mut db, playlist_db_id)?;

        let release_a_public_id = releases::get_by_id(&db, release_a)?.unwrap().id;
        let release_b_public_id = releases::get_by_id(&db, release_b)?.unwrap().id;
        let expected = if deterministic_random_score(
            DisplayCoverTargetKind::Playlist,
            &playlist_public_id,
            &release_a_public_id,
        ) < deterministic_random_score(
            DisplayCoverTargetKind::Playlist,
            &playlist_public_id,
            &release_b_public_id,
        ) {
            release_a
        } else {
            release_b
        };

        let (_, profile) = get_profile(
            &db,
            DisplayCoverScope::Instance,
            DisplayCoverTargetKind::Playlist,
            None,
            &playlist_public_id,
        )?
        .expect("playlist profile should exist");
        let winner = get_winner(&db, &profile, DisplayCoverWinnerKind::Random)?
            .expect("playlist winner should exist");
        assert_eq!(winner.release_db_id, expected);

        // Re-syncing an unchanged playlist must not move the winner.
        sync_playlist_cover(&mut db, playlist_db_id)?;
        let winner = get_winner(&db, &profile, DisplayCoverWinnerKind::Random)?
            .expect("playlist winner should still exist");
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
