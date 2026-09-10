// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
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

use super::{
    DbAccess,
    MAX_PLAYBACKS_PER_USER,
    Playback,
    count_for_user_up_to_limit,
    delete_in_transaction,
    get_by_id,
    get_owner_id,
    insert,
};

pub(super) const MAX_REPORTED_PLAYBACKS_PER_USER: usize = 100;

#[derive(Debug, thiserror::Error)]
#[error(
    "playback context limit reached; delete unused playbacks or allow inactive reported contexts to expire"
)]
pub(crate) struct ReportedPlaybackLimitReached;

pub(crate) const REPORTED_PLAYBACK_TTL_MS: u64 = 5 * 60 * 1_000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, DbTypeMarker)]
pub(crate) struct ReportedPrevious {
    pub(crate) session_public_id: String,
    pub(crate) demoted_at_ms: u64,
    pub(crate) expires_at_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, DbTypeMarker)]
pub(crate) struct ReportedSource {
    pub(crate) plugin_id: String,
    pub(crate) session_key: String,
    pub(crate) previous: Option<ReportedPrevious>,
}

impl From<ReportedSource> for DbValue {
    fn from(source: ReportedSource) -> Self {
        (&source).into()
    }
}
impl From<&ReportedSource> for DbValue {
    fn from(source: &ReportedSource) -> Self {
        serde_json::to_string(source)
            .expect("reported source consists of serializable scalar fields")
            .into()
    }
}
impl TryFrom<DbValue> for ReportedSource {
    type Error = DbError;
    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        serde_json::from_str(value.string()?).map_err(|error| {
            DbError::serialization(agdb::DbErrorType::TypeError, error.to_string())
        })
    }
}

fn reported_alias(user_db_id: DbId, plugin_id: &str, session_key: &str) -> String {
    format!(
        "reported-playback:{}:{}:{plugin_id}:{session_key}",
        user_db_id.0,
        plugin_id.len()
    )
}

pub(crate) fn get_reported(
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
            && playback.reported.as_ref().is_some_and(
                |source| source.plugin_id == plugin_id && source.session_key == session_key
            ),
        "reported playback binding identity mismatch"
    );
    Ok(
        (playback.updated_at_ms >= now_ms.saturating_sub(REPORTED_PLAYBACK_TTL_MS))
            .then_some(playback),
    )
}

pub(crate) fn insert_reported(
    db: &mut impl DbAccess,
    user_db_id: DbId,
    plugin_id: &str,
    session_key: &str,
    current_session_db_id: DbId,
    now_ms: u64,
) -> anyhow::Result<Playback> {
    expire_reported_for_user(db, user_db_id, now_ms)?;
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
                .keys("reported")
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
        id: nanoid::nanoid!(),
        queue: None,
        reported: Some(ReportedSource {
            plugin_id: plugin_id.to_owned(),
            session_key: session_key.to_owned(),
            previous: None,
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

pub(crate) fn update_reported_previous(
    db: &mut impl DbAccess,
    playback_db_id: DbId,
    previous: Option<ReportedPrevious>,
) -> anyhow::Result<()> {
    let playback =
        get_by_id(db, playback_db_id)?.ok_or_else(|| anyhow::anyhow!("playback not found"))?;
    let mut source = playback
        .reported
        .ok_or_else(|| anyhow::anyhow!("playback is not reported"))?;
    source.previous = previous;
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform([("reported", DbValue::from(source)).into()])
            .ids(playback_db_id)
            .query(),
    )?;
    Ok(())
}

pub(crate) fn expire_reported_for_user(
    db: &mut impl DbAccess,
    user_db_id: DbId,
    now_ms: u64,
) -> anyhow::Result<usize> {
    let cutoff = now_ms.saturating_sub(REPORTED_PLAYBACK_TTL_MS);
    let expired = db
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
                .keys("reported")
                .and()
                .key("updated_at_ms")
                .value(agdb::Comparison::LessThan(cutoff.into()))
                .query(),
        )?
        .ids();
    for id in &expired {
        delete_in_transaction(db, *id)?;
    }
    Ok(expired.len())
}

pub(crate) fn expire_reported(db: &mut impl DbAccess, now_ms: u64) -> anyhow::Result<usize> {
    let cutoff = now_ms.saturating_sub(REPORTED_PLAYBACK_TTL_MS);
    let expired = db
        .exec(
            QueryBuilder::search()
                .from("playbacks")
                .where_()
                .neighbor()
                .and()
                .key("db_element_id")
                .value("Playback")
                .and()
                .keys("reported")
                .and()
                .key("updated_at_ms")
                .value(agdb::Comparison::LessThan(cutoff.into()))
                .query(),
        )?
        .ids();
    for id in &expired {
        delete_in_transaction(db, *id)?;
    }
    Ok(expired.len())
}
