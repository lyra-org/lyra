// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbId,
    QueryBuilder,
};

use super::{
    DbAccess,
    MAX_PLAYBACKS_PER_USER,
    Playback,
    PlaybackSource,
    count_for_user_up_to_limit,
    delete_in_transaction,
    get_by_id,
    get_current_session_id,
    get_owner_id,
    insert,
};
use crate::playback_sessions::{
    PlaybackSessionListProjection,
    get_list_projections_by_ids,
};

pub(super) const MAX_REPORTED_PLAYBACKS_PER_USER: usize = 100;

#[derive(Debug, thiserror::Error)]
#[error(
    "playback context limit reached; delete unused playbacks or allow inactive reported contexts to expire"
)]
pub struct ReportedPlaybackLimitReached;

/// How long a reported playback outlasts its last current observation however its current play
/// stands, so a report resuming a stopped play still reaches it.
pub const REPORTED_PLAYBACK_RETENTION_MS: u64 = 5 * 60 * 1_000;

/// Whether a reported playback last observed at `updated_at_ms` lapsed by `now_ms`: its retention
/// passed and its current play, if any, is no longer live.
pub fn reported_lapsed(
    updated_at_ms: u64,
    current: Option<&PlaybackSessionListProjection>,
    now_ms: u64,
) -> bool {
    updated_at_ms < now_ms.saturating_sub(REPORTED_PLAYBACK_RETENTION_MS)
        && !current.is_some_and(|current| current.is_live(now_ms))
}

/// [`reported_lapsed`] for the stored reported playback, reading its current session.
pub fn is_reported_lapsed(
    db: &impl DbAccess,
    playback_db_id: DbId,
    updated_at_ms: u64,
    now_ms: u64,
) -> anyhow::Result<bool> {
    if !reported_lapsed(updated_at_ms, None, now_ms) {
        return Ok(false);
    }
    let Some(session_db_id) = get_current_session_id(db, playback_db_id)? else {
        return Ok(true);
    };
    let current = get_list_projections_by_ids(db, vec![session_db_id])?;
    Ok(reported_lapsed(
        updated_at_ms,
        current.get(&session_db_id),
        now_ms,
    ))
}

/// The player session a plugin reports a playback for. The plugin is the one its sessions name.
#[derive(Clone, Debug)]
pub struct ReportedSource {
    pub(crate) session_key: String,
}

impl ReportedSource {
    pub fn session_key(&self) -> &str {
        &self.session_key
    }
}

fn reported_alias(user_db_id: DbId, plugin_id: &str, session_key: &str) -> String {
    format!(
        "reported-playback:{}:{}:{plugin_id}:{session_key}",
        user_db_id.0,
        plugin_id.len()
    )
}

pub fn get_reported(
    db: &impl DbAccess,
    user_db_id: DbId,
    plugin_id: &str,
    session_key: &str,
    now_ms: u64,
) -> anyhow::Result<Option<Playback>> {
    let alias = reported_alias(user_db_id, plugin_id, session_key);
    let result = match db.exec(QueryBuilder::select().ids(alias).query()) {
        Ok(result) => result,
        Err(error) if error.ty == agdb::DbErrorType::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let Some(element) = result.elements.first() else {
        return Ok(None);
    };
    let Some(playback) = get_by_id(db, element.id)? else {
        anyhow::bail!("invalid reported playback binding")
    };
    anyhow::ensure!(
        get_owner_id(db, element.id)? == Some(user_db_id)
            && matches!(
                &playback.source,
                PlaybackSource::Reported(source) if source.session_key == session_key
            ),
        "reported playback binding identity mismatch"
    );
    Ok((!is_reported_lapsed(db, element.id, playback.updated_at_ms, now_ms)?).then_some(playback))
}

/// Binds a new reported playback. The caller expires the user's lapsed ones first, with
/// [`expire_reported_for_user`], so they hold no capacity.
pub fn insert_reported(
    db: &mut impl DbAccess,
    user_db_id: DbId,
    plugin_id: &str,
    session_key: &str,
    current_session_db_id: DbId,
    now_ms: u64,
) -> anyhow::Result<Playback> {
    anyhow::ensure!(
        get_reported(db, user_db_id, plugin_id, session_key, now_ms)?.is_none(),
        "reported playback binding already exists"
    );
    let reported_count = db
        .exec(
            QueryBuilder::search()
                .to(user_db_id)
                .limit(MAX_REPORTED_PLAYBACKS_PER_USER as u64)
                .where_()
                .neighbor()
                .and()
                .key("db_element_id")
                .value("Playback")
                .and()
                .keys("reported_session_key")
                .query(),
        )?
        .elements
        .len();
    if reported_count >= MAX_REPORTED_PLAYBACKS_PER_USER
        || count_for_user_up_to_limit(db, user_db_id)? >= MAX_PLAYBACKS_PER_USER
    {
        return Err(ReportedPlaybackLimitReached.into());
    }
    let mut playback = Playback {
        db_id: None,
        id: crate::nanoid(),
        source: PlaybackSource::Reported(ReportedSource {
            session_key: session_key.to_owned(),
        }),
        created_at_ms: now_ms,
        updated_at_ms: now_ms,
    };
    let id = insert(db, &playback, user_db_id, current_session_db_id)?;
    db.exec_mut(
        QueryBuilder::insert()
            .aliases(reported_alias(user_db_id, plugin_id, session_key))
            .ids(id)
            .query(),
    )?;
    playback.db_id = Some(id);
    Ok(playback)
}

/// A reported playback expiry removed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExpiredReported {
    pub user_db_id: DbId,
    /// The track session it held current, which outlives it until eviction.
    pub current_session_db_id: Option<DbId>,
}

pub fn expire_reported_for_user(
    db: &mut impl DbAccess,
    user_db_id: DbId,
    now_ms: u64,
) -> anyhow::Result<Vec<ExpiredReported>> {
    let cutoff = now_ms.saturating_sub(REPORTED_PLAYBACK_RETENTION_MS);
    let candidates = db
        .exec(
            QueryBuilder::search()
                .to(user_db_id)
                .limit(MAX_PLAYBACKS_PER_USER as u64)
                .where_()
                .neighbor()
                .and()
                .key("db_element_id")
                .value("Playback")
                .and()
                .keys("reported_session_key")
                .and()
                .key("updated_at_ms")
                .less_than(cutoff)
                .query(),
        )?
        .ids();
    remove_expired(db, candidates, now_ms)
}

pub fn expire_reported(
    db: &mut impl DbAccess,
    now_ms: u64,
) -> anyhow::Result<Vec<ExpiredReported>> {
    let cutoff = now_ms.saturating_sub(REPORTED_PLAYBACK_RETENTION_MS);
    let candidates = db
        .exec(
            QueryBuilder::search()
                .from("playbacks")
                .where_()
                .neighbor()
                .and()
                .key("db_element_id")
                .value("Playback")
                .and()
                .keys("reported_session_key")
                .and()
                .key("updated_at_ms")
                .less_than(cutoff)
                .query(),
        )?
        .ids();
    remove_expired(db, candidates, now_ms)
}

/// Removes the candidates, reported playbacks past their retention, whose current play is no
/// longer live.
fn remove_expired(
    db: &mut impl DbAccess,
    candidates: Vec<DbId>,
    now_ms: u64,
) -> anyhow::Result<Vec<ExpiredReported>> {
    let current_ids = super::current_session_ids(db, &candidates)?;
    let sessions =
        get_list_projections_by_ids(db, current_ids.values().copied().collect::<Vec<_>>())?;
    let mut expired = Vec::new();
    for id in candidates {
        let current_session_db_id = current_ids.get(&id).copied();
        if current_session_db_id
            .and_then(|session_db_id| sessions.get(&session_db_id))
            .is_some_and(|current| current.is_live(now_ms))
        {
            continue;
        }
        let user_db_id = get_owner_id(db, id)?
            .ok_or_else(|| anyhow::anyhow!("playback {} has no owner", id.0))?;
        expired.push(ExpiredReported {
            user_db_id,
            current_session_db_id,
        });
        delete_in_transaction(db, id)?;
    }
    Ok(expired)
}
