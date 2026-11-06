// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};
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

use super::super::DbAccess;
use serde::{
    Deserialize,
    Serialize,
};

use super::super::NodeId;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(DbElement, Serialize, Clone, Debug)]
pub struct ExternalId {
    #[serde(skip)]
    pub db_id: Option<NodeId>,
    pub provider_id: String,
    pub id_type: String,
    /// The effective value: the manual value when present, else the resolved one.
    pub id_value: String,
    pub source: IdSource,
    /// The resolved value, kept beneath a manual one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_value: Option<String>,
}

impl ExternalId {
    /// The value a plugin last set, whether or not a manual value hides it.
    pub fn resolved_value(&self) -> Option<&str> {
        match self.source {
            IdSource::Resolved => Some(&self.id_value),
            IdSource::Manual => self.resolved_value.as_deref(),
        }
    }
}

/// A value a merged duplicate held, kept so that it still finds the entity it was merged into.
#[derive(DbElement, Clone, Debug)]
pub struct ExternalIdAlias {
    pub db_id: Option<NodeId>,
    pub provider_id: String,
    pub id_type: String,
    pub id_value: String,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, DbTypeMarker)]
#[serde(rename_all = "snake_case")]
pub enum IdSource {
    /// Set by a provider plugin.
    Resolved,
    /// Set by a user; it takes effect over a resolved value.
    Manual,
}

impl IdSource {
    // Stored under the names these had before, so existing rows still load.
    fn as_db_str(self) -> &'static str {
        match self {
            Self::Resolved => "plugin",
            Self::Manual => "user",
        }
    }

    fn from_db_str(value: &str) -> Result<Self, DbError> {
        match value {
            "plugin" => Ok(Self::Resolved),
            "user" => Ok(Self::Manual),
            _ => Err(DbError::serialization(
                agdb::DbErrorType::TypeError,
                format!("invalid IdSource value '{value}'"),
            )),
        }
    }
}

impl fmt::Display for IdSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Resolved => "resolved",
            Self::Manual => "manual",
        })
    }
}

impl From<IdSource> for DbValue {
    fn from(value: IdSource) -> Self {
        Self::from(value.as_db_str())
    }
}

impl From<&IdSource> for DbValue {
    fn from(value: &IdSource) -> Self {
        (*value).into()
    }
}

impl TryFrom<DbValue> for IdSource {
    type Error = DbError;

    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        Self::from_db_str(value.string()?)
    }
}

pub fn get_for_album_tracks(
    db: &impl DbAccess,
    release_db_id: DbId,
) -> anyhow::Result<Vec<ExternalId>> {
    let ext_ids: Vec<ExternalId> = db
        .exec(
            QueryBuilder::select()
                .elements::<ExternalId>()
                .search()
                .from(release_db_id)
                .where_()
                .beyond()
                .where_()
                .not()
                .key("db_element_id")
                .value("ExternalId")
                .end_where()
                .query(),
        )?
        .try_into()?;
    Ok(ext_ids)
}

pub fn get_for_entity(db: &DbAny, node_id: DbId) -> anyhow::Result<Vec<ExternalId>> {
    get_for_entity_inside_tx(db, node_id)
}

/// Transaction-capable variant of [`get_for_entity`].
pub fn get_for_entity_inside_tx(
    db: &impl DbAccess,
    node_id: DbId,
) -> anyhow::Result<Vec<ExternalId>> {
    let ids: Vec<ExternalId> = db
        .exec(
            QueryBuilder::select()
                .elements::<ExternalId>()
                .search()
                .from(node_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    Ok(ids)
}

/// The values kept for `node_id` from the duplicates merged into it.
pub fn get_aliases_for_entity(
    db: &impl DbAccess,
    node_id: DbId,
) -> anyhow::Result<Vec<ExternalIdAlias>> {
    Ok(db
        .exec(
            QueryBuilder::select()
                .elements::<ExternalIdAlias>()
                .search()
                .from(node_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?)
}

pub fn get_owner_id(db: &impl DbAccess, external_id: DbId) -> anyhow::Result<Option<DbId>> {
    let result = db.exec(
        QueryBuilder::search()
            .to(external_id)
            .where_()
            .not()
            .edge()
            .and()
            .distance(agdb::CountComparison::Equal(2))
            .query(),
    )?;
    Ok(result.elements.into_iter().next().map(|e| e.id))
}

pub fn get_owner(
    db: &impl DbAccess,
    provider_id: &str,
    id_type: &str,
    id_value: &str,
    owner_discriminator: Option<&str>,
) -> anyhow::Result<Option<DbId>> {
    let Ok(matching_ids) = matching_external_id_ids(db, provider_id, id_type, id_value) else {
        return Ok(None);
    };
    for ext_id_db_id in matching_ids {
        let Some(owner_id) = get_owner_id(db, ext_id_db_id)? else {
            continue;
        };
        if let Some(disc) = owner_discriminator {
            let Ok(result) = db.exec(QueryBuilder::select().ids(owner_id).query()) else {
                continue;
            };
            let is_match = result
                .elements
                .first()
                .is_some_and(|e| super::super::graph::is_element_type(e, disc));
            if !is_match {
                continue;
            }
        }
        return Ok(Some(owner_id));
    }
    Ok(None)
}

pub fn get_owners(
    db: &impl DbAccess,
    provider_id: &str,
    id_type: &str,
    id_value: &str,
    owner_discriminator: Option<&str>,
) -> anyhow::Result<Vec<DbId>> {
    let matching_ids = matching_external_id_ids(db, provider_id, id_type, id_value)?;
    if matching_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut owner_ids = Vec::new();
    for external_id in matching_ids {
        owner_ids.extend(
            db.exec(
                QueryBuilder::search()
                    .to(external_id)
                    .where_()
                    .not()
                    .edge()
                    .and()
                    .distance(agdb::CountComparison::Equal(2))
                    .query(),
            )?
            .ids()
            .into_iter()
            .filter(|id| id.0 > 0),
        );
    }
    owner_ids.sort_by_key(|id| id.0);
    owner_ids.dedup();
    if owner_ids.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(discriminator) = owner_discriminator {
        let selected = db.exec(QueryBuilder::select().ids(owner_ids.clone()).query())?;
        let matching_owner_ids = selected
            .elements
            .into_iter()
            .filter(|element| super::super::graph::is_element_type(element, discriminator))
            .map(|element| element.id)
            .collect::<std::collections::HashSet<_>>();
        owner_ids.retain(|id| matching_owner_ids.contains(id));
    }
    Ok(owner_ids)
}

/// The rows holding the value, ahead of the aliases holding it.
fn matching_external_id_ids(
    db: &impl DbAccess,
    provider_id: &str,
    id_type: &str,
    id_value: &str,
) -> anyhow::Result<Vec<DbId>> {
    fn holding<T: agdb::DbType>(
        db: &impl DbAccess,
        provider_id: &str,
        id_type: &str,
        id_value: &str,
    ) -> anyhow::Result<Vec<DbId>> {
        Ok(db
            .exec(
                QueryBuilder::search()
                    .index("id_value")
                    .value(id_value)
                    .where_()
                    .element::<T>()
                    .and()
                    .key("provider_id")
                    .value(provider_id)
                    .and()
                    .key("id_type")
                    .value(id_type)
                    .query(),
            )?
            .ids())
    }

    let mut ids = holding::<ExternalId>(db, provider_id, id_type, id_value)?;
    ids.extend(holding::<ExternalIdAlias>(
        db,
        provider_id,
        id_type,
        id_value,
    )?);
    Ok(ids)
}

pub fn get(
    db: &DbAny,
    node_id: DbId,
    provider_id: &str,
    id_type: &str,
) -> anyhow::Result<Option<ExternalId>> {
    get_inside_tx(db, node_id, provider_id, id_type)
}

/// Transaction-capable variant of [`get`].
pub fn get_inside_tx(
    db: &impl DbAccess,
    node_id: DbId,
    provider_id: &str,
    id_type: &str,
) -> anyhow::Result<Option<ExternalId>> {
    let ids = get_for_entity_inside_tx(db, node_id)?;
    Ok(ids
        .into_iter()
        .find(|id| id.provider_id == provider_id && id.id_type == id_type))
}

pub fn upsert(
    db: &mut DbAny,
    node_id: DbId,
    provider_id: &str,
    id_type: &str,
    id_value: &str,
    source: IdSource,
) -> anyhow::Result<DbId> {
    db.transaction_mut(|t| -> anyhow::Result<DbId> {
        upsert_inside_tx(t, node_id, provider_id, id_type, id_value, source)
    })
}

/// Transaction-capable variant of [`upsert`].
///
/// Values are stored trimmed. A manual write keeps the resolved value beneath it, and a resolved
/// write under a manual value updates only that kept value. A resolved write of one of the
/// entity's aliases leaves a resolved value it already has: the lookup that led here found the
/// entity through the alias, so the value names the same thing and the entity's ID stays stable.
pub fn upsert_inside_tx(
    db: &mut impl DbAccess,
    node_id: DbId,
    provider_id: &str,
    id_type: &str,
    id_value: &str,
    source: IdSource,
) -> anyhow::Result<DbId> {
    let id_value = id_value.trim();
    let existing = get_inside_tx(db, node_id, provider_id, id_type)?;

    if source == IdSource::Resolved
        && let Some(existing) = &existing
        && let Some(db_id) = &existing.db_id
        && existing
            .resolved_value()
            .is_some_and(|current| !current.trim().is_empty() && current != id_value)
        && has_alias(db, node_id, provider_id, id_type, id_value)?
    {
        return Ok(db_id.clone().into());
    }

    let (effective, new_source, resolved_value) = match (&existing, source) {
        (Some(existing), IdSource::Resolved) if existing.source == IdSource::Manual => (
            existing.id_value.clone(),
            IdSource::Manual,
            Some(id_value.to_string()),
        ),
        (Some(existing), IdSource::Manual) => (
            id_value.to_string(),
            IdSource::Manual,
            existing.resolved_value().map(str::to_string),
        ),
        _ => (id_value.to_string(), source, None),
    };

    // Avoid no-op rewrites when a provider repeatedly submits the same ID.
    if let Some(existing_id) = &existing
        && existing_id.id_value == effective
        && existing_id.source == new_source
        && existing_id.resolved_value == resolved_value
        && let Some(db_id) = &existing_id.db_id
    {
        return Ok(db_id.clone().into());
    }

    let existing_db_id = existing
        .as_ref()
        .and_then(|e| e.db_id.clone())
        .map(DbId::from);
    let external_id = ExternalId {
        db_id: existing_db_id.map(Into::into),
        provider_id: provider_id.to_string(),
        id_type: id_type.to_string(),
        id_value: effective,
        source: new_source,
        resolved_value,
    };

    let result = db.exec_mut(QueryBuilder::insert().element(&external_id).query())?;
    let id_db_id = existing_db_id
        .or_else(|| result.elements.first().map(|element| element.id))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "upsert external id returned no id (node_id={}, provider_id='{}', id_type='{}')",
                node_id.0,
                provider_id,
                id_type
            )
        })?;

    // Create edge from entity to external ID if new
    if existing.is_none() {
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(node_id)
                .to(id_db_id)
                .query(),
        )?;
    }

    Ok(id_db_id)
}

/// Merges `external_id`, a merged duplicate's, into `node_id`'s value of its
/// type. `node_id`'s values win, except that a manual value takes effect over
/// a resolved one, keeping it beneath, and a resolved value fills in where
/// `node_id` has none. Empty values say nothing, on either side.
pub fn merge_inside_tx(
    db: &mut impl DbAccess,
    node_id: DbId,
    external_id: &ExternalId,
) -> anyhow::Result<()> {
    if let Some(kept) = merge_values(db, node_id, external_id)? {
        tracing::warn!(
            node_id = node_id.0,
            provider_id = %external_id.provider_id,
            id_type = %external_id.id_type,
            kept = %kept,
            dropped = %external_id.id_value,
            "dropping a merged duplicate's differing manual external id"
        );
    }
    Ok(())
}

/// One ID type's values on an entity, as a merge weighs them. Blank values say nothing.
#[derive(Default)]
struct IdValues {
    resolved: Option<String>,
    manual: Option<String>,
}

impl IdValues {
    fn of(external_id: &ExternalId) -> Self {
        fn present(value: &str) -> Option<String> {
            let value = value.trim();
            (!value.is_empty()).then(|| value.to_string())
        }

        Self {
            resolved: external_id.resolved_value().and_then(present),
            manual: (external_id.source == IdSource::Manual)
                .then(|| present(&external_id.id_value))
                .flatten(),
        }
    }

    /// These values with `other`'s where they have none.
    fn filled_from(&self, other: &Self) -> Self {
        Self {
            resolved: self.resolved.clone().or_else(|| other.resolved.clone()),
            manual: self.manual.clone().or_else(|| other.manual.clone()),
        }
    }

    /// The value lookups find the entity by.
    fn effective(&self) -> Option<&str> {
        self.manual.as_deref().or(self.resolved.as_deref())
    }
}

/// Moves what `external_id` can give `node_id`. Returns `node_id`'s manual value when it differs
/// from `external_id`'s, which then stays out.
fn merge_values(
    db: &mut impl DbAccess,
    node_id: DbId,
    external_id: &ExternalId,
) -> anyhow::Result<Option<String>> {
    let ExternalId {
        provider_id,
        id_type,
        ..
    } = external_id;
    let current = get_inside_tx(db, node_id, provider_id, id_type)?
        .as_ref()
        .map(IdValues::of)
        .unwrap_or_default();
    let incoming = IdValues::of(external_id);
    let merged = current.filled_from(&incoming);

    if merged.resolved != current.resolved
        && let Some(resolved) = &merged.resolved
    {
        upsert_inside_tx(
            db,
            node_id,
            provider_id,
            id_type,
            resolved,
            IdSource::Resolved,
        )?;
    }
    if merged.manual != current.manual
        && let Some(manual) = &merged.manual
    {
        upsert_inside_tx(db, node_id, provider_id, id_type, manual, IdSource::Manual)?;
    }
    Ok(current.manual.filter(|kept| {
        incoming
            .manual
            .as_ref()
            .is_some_and(|manual| manual != kept)
    }))
}

fn has_alias(
    db: &impl DbAccess,
    node_id: DbId,
    provider_id: &str,
    id_type: &str,
    id_value: &str,
) -> anyhow::Result<bool> {
    Ok(get_aliases_for_entity(db, node_id)?.iter().any(|alias| {
        alias.provider_id == provider_id && alias.id_type == id_type && alias.id_value == id_value
    }))
}

/// Adds `id_value` to `node_id`'s aliases unless it is blank or one already.
fn ensure_alias(
    db: &mut impl DbAccess,
    node_id: DbId,
    provider_id: &str,
    id_type: &str,
    id_value: &str,
) -> anyhow::Result<()> {
    let id_value = id_value.trim();
    if id_value.is_empty() || has_alias(db, node_id, provider_id, id_type, id_value)? {
        return Ok(());
    }
    let alias = ExternalIdAlias {
        db_id: None,
        provider_id: provider_id.to_string(),
        id_type: id_type.to_string(),
        id_value: id_value.to_string(),
    };
    let alias_id = db
        .exec_mut(QueryBuilder::insert().element(&alias).query())?
        .ids()
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("external id alias insert returned no id"))?;
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(node_id)
            .to(alias_id)
            .query(),
    )?;
    Ok(())
}

/// Moves `loser`'s IDs onto `winner` as [`merge_inside_tx`] does, adding the aliases
/// [`absorb_plan`] names.
pub fn absorb_inside_tx(db: &mut impl DbAccess, winner: DbId, loser: DbId) -> anyhow::Result<()> {
    let aliases = absorb_plan(db, winner, &[loser])?;
    for external_id in get_for_entity_inside_tx(db, loser)? {
        merge_values(db, winner, &external_id)?;
    }
    for alias in aliases {
        ensure_alias(
            db,
            winner,
            &alias.provider_id,
            &alias.id_type,
            &alias.id_value,
        )?;
    }
    Ok(())
}

/// The aliases `winner` gains if each of `losers` merges into it in turn. Lookups search only a
/// row's effective value, so every value that held one and doesn't after the merge is kept: a
/// loser's that `winner` couldn't take, a loser's alias, and a value of `winner`'s row that a
/// manual value from a loser takes over from.
pub fn absorb_plan(
    db: &impl DbAccess,
    winner: DbId,
    losers: &[DbId],
) -> anyhow::Result<Vec<ExternalIdAlias>> {
    type Pair = (String, String);

    let mut rows: HashMap<Pair, IdValues> = get_for_entity_inside_tx(db, winner)?
        .iter()
        .map(|row| {
            (
                (row.provider_id.clone(), row.id_type.clone()),
                IdValues::of(row),
            )
        })
        .collect();
    let mut aliased: HashSet<(Pair, String)> = get_aliases_for_entity(db, winner)?
        .into_iter()
        .map(|alias| ((alias.provider_id, alias.id_type), alias.id_value))
        .collect();
    let mut planned = Vec::new();
    let mut keep = |pair: &Pair, effective: Option<&str>, value: &str| {
        let value = value.trim();
        if !value.is_empty()
            && effective != Some(value)
            && aliased.insert((pair.clone(), value.to_string()))
        {
            planned.push(ExternalIdAlias {
                db_id: None,
                provider_id: pair.0.clone(),
                id_type: pair.1.clone(),
                id_value: value.to_string(),
            });
        }
    };

    for &loser in losers {
        for row in get_for_entity_inside_tx(db, loser)? {
            let pair = (row.provider_id.clone(), row.id_type.clone());
            let incoming = IdValues::of(&row);
            let current = rows.entry(pair.clone()).or_default();
            let merged = current.filled_from(&incoming);
            for value in [
                current.effective(),
                incoming.resolved.as_deref(),
                incoming.manual.as_deref(),
            ]
            .into_iter()
            .flatten()
            {
                keep(&pair, merged.effective(), value);
            }
            *current = merged;
        }
        for alias in get_aliases_for_entity(db, loser)? {
            let pair = (alias.provider_id.clone(), alias.id_type.clone());
            let effective = rows.get(&pair).and_then(IdValues::effective);
            keep(&pair, effective, &alias.id_value);
        }
    }
    Ok(planned)
}

/// Keeps `node_id`'s resolved value as an alias when `id_value`, about to be written, would
/// replace it.
pub(crate) fn keep_replaced_inside_tx(
    db: &mut impl DbAccess,
    node_id: DbId,
    provider_id: &str,
    id_type: &str,
    id_value: &str,
) -> anyhow::Result<()> {
    let row = get_inside_tx(db, node_id, provider_id, id_type)?;
    match row.as_ref().and_then(ExternalId::resolved_value) {
        Some(current) if current != id_value.trim() => {
            ensure_alias(db, node_id, provider_id, id_type, current)
        }
        _ => Ok(()),
    }
}

/// Removes the alias. Returns false when `node_id` had none with that value.
pub fn remove_alias(
    db: &mut impl DbAccess,
    node_id: DbId,
    provider_id: &str,
    id_type: &str,
    id_value: &str,
) -> anyhow::Result<bool> {
    let ids: Vec<DbId> = get_aliases_for_entity(db, node_id)?
        .into_iter()
        .filter(|alias| {
            alias.provider_id == provider_id
                && alias.id_type == id_type
                && alias.id_value == id_value
        })
        .filter_map(|alias| alias.db_id.map(DbId::from))
        .collect();
    if ids.is_empty() {
        return Ok(false);
    }
    db.exec_mut(QueryBuilder::remove().ids(ids).query())?;
    Ok(true)
}

/// Removes the manual value, so the resolved one takes effect again. Returns
/// false when there was no manual value.
pub fn remove_manual_inside_tx(
    db: &mut impl DbAccess,
    node_id: DbId,
    provider_id: &str,
    id_type: &str,
) -> anyhow::Result<bool> {
    let Some(existing) = get_inside_tx(db, node_id, provider_id, id_type)? else {
        return Ok(false);
    };
    if existing.source != IdSource::Manual {
        return Ok(false);
    }
    let db_id = existing
        .db_id
        .clone()
        .map(DbId::from)
        .ok_or_else(|| anyhow::anyhow!("external id row has no db id"))?;
    match existing.resolved_value {
        Some(resolved_value) => {
            let restored = ExternalId {
                db_id: Some(db_id.into()),
                provider_id: provider_id.to_string(),
                id_type: id_type.to_string(),
                id_value: resolved_value,
                source: IdSource::Resolved,
                resolved_value: None,
            };
            db.exec_mut(QueryBuilder::insert().element(&restored).query())?;
            // Inserting an element leaves the keys its `None` fields omit.
            db.exec_mut(
                QueryBuilder::remove()
                    .values([DbValue::from("resolved_value")])
                    .ids(db_id)
                    .query(),
            )?;
        }
        None => {
            db.exec_mut(QueryBuilder::remove().ids(db_id).query())?;
        }
    }
    Ok(true)
}

/// Remove every `ExternalId` and `ExternalIdAlias` attached to an owner. Call before deleting the
/// owner so the `external_ids` root edge doesn't keep orphaned rows alive.
pub(crate) fn remove_all_for_owner(db: &mut impl DbAccess, owner_id: DbId) -> anyhow::Result<()> {
    let ids: Vec<DbId> = db
        .exec(
            QueryBuilder::search()
                .from(owner_id)
                .where_()
                .distance(agdb::CountComparison::Equal(2))
                .and()
                .where_()
                .key("db_element_id")
                .value("ExternalId")
                .or()
                .key("db_element_id")
                .value("ExternalIdAlias")
                .end_where()
                .query(),
        )?
        .ids()
        .into_iter()
        .filter(|id| id.0 > 0)
        .collect();

    if ids.is_empty() {
        return Ok(());
    }

    db.exec_mut(QueryBuilder::remove().ids(ids).query())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_db::TestDb;
    use agdb::{
        DbValue,
        QueryBuilder,
    };
    use anyhow::anyhow;

    fn new_test_db() -> anyhow::Result<DbAny> {
        Ok(TestDb::new()?.into_inner())
    }

    fn new_initialized_test_db() -> anyhow::Result<DbAny> {
        Ok(TestDb::initialized()?.into_inner())
    }

    fn insert_entity(db: &mut DbAny) -> anyhow::Result<DbId> {
        let result = db.exec_mut(QueryBuilder::insert().nodes().count(1).query())?;
        result
            .elements
            .first()
            .map(|element| element.id)
            .ok_or_else(|| anyhow!("entity insert did not return an id"))
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
    fn values_are_stored_trimmed() -> anyhow::Result<()> {
        let mut db = crate::test_db::new_test_db()?;
        let track = crate::fixtures::insert_track(&mut db, "Track")?;
        upsert(
            &mut db,
            track,
            "musicbrainz",
            "recording_id",
            "  rec \n",
            IdSource::Manual,
        )?;

        let stored = get(&db, track, "musicbrainz", "recording_id")?.expect("stored id");
        assert_eq!(stored.id_value, "rec");
        assert_eq!(
            get_owners(&db, "musicbrainz", "recording_id", "rec", Some("Track"))?,
            [track]
        );
        Ok(())
    }

    #[test]
    fn id_source_uses_stable_string_db_values() -> anyhow::Result<()> {
        assert_eq!(DbValue::from(IdSource::Resolved), DbValue::from("plugin"));
        assert_eq!(IdSource::try_from(DbValue::from("user"))?, IdSource::Manual);
        assert!(IdSource::try_from(DbValue::from("imported")).is_err());
        Ok(())
    }

    #[test]
    fn upsert_external_id_update_reuses_node_and_updates_value() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let node_id = insert_entity(&mut db)?;

        let first_id = upsert(
            &mut db,
            node_id,
            "discogs",
            "release_id",
            "abc-1",
            IdSource::Manual,
        )?;
        let second_id = upsert(
            &mut db,
            node_id,
            "discogs",
            "release_id",
            "abc-2",
            IdSource::Manual,
        )?;

        assert_eq!(first_id, second_id);

        let external_ids = get_for_entity(&db, node_id)?;
        assert_eq!(external_ids.len(), 1);
        assert_eq!(external_ids[0].provider_id, "discogs");
        assert_eq!(external_ids[0].id_type, "release_id");
        assert_eq!(external_ids[0].id_value, "abc-2");

        Ok(())
    }

    #[test]
    fn upsert_external_id_same_value_is_noop() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let node_id = insert_entity(&mut db)?;

        let first_id = upsert(
            &mut db,
            node_id,
            "discogs",
            "release_id",
            "abc-1",
            IdSource::Resolved,
        )?;
        let second_id = upsert(
            &mut db,
            node_id,
            "discogs",
            "release_id",
            "abc-1",
            IdSource::Resolved,
        )?;

        assert_eq!(first_id, second_id);

        let external_ids = get_for_entity(&db, node_id)?;
        assert_eq!(external_ids.len(), 1);
        assert_eq!(external_ids[0].id_value, "abc-1");
        assert_eq!(external_ids[0].source, IdSource::Resolved);

        Ok(())
    }

    #[test]
    fn external_id_persists_source_as_string() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let node_id = insert_entity(&mut db)?;
        let external_id = upsert(
            &mut db,
            node_id,
            "discogs",
            "release_id",
            "abc-1",
            IdSource::Resolved,
        )?;

        let element = db
            .exec(QueryBuilder::select().ids(external_id).query())?
            .elements
            .into_iter()
            .next()
            .expect("external id element");

        assert_eq!(
            element_value(&element, "source"),
            Some(DbValue::from("plugin"))
        );
        Ok(())
    }

    fn get_release_id(db: &DbAny, node_id: DbId) -> anyhow::Result<ExternalId> {
        get(db, node_id, "musicbrainz", "release_id")?.ok_or_else(|| anyhow!("release_id missing"))
    }

    #[test]
    fn resolved_write_under_manual_id_keeps_both_values() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let node_id = insert_entity(&mut db)?;

        upsert(
            &mut db,
            node_id,
            "musicbrainz",
            "release_id",
            "plugin-1",
            IdSource::Resolved,
        )?;
        upsert(
            &mut db,
            node_id,
            "musicbrainz",
            "release_id",
            "user",
            IdSource::Manual,
        )?;
        let manual = get_release_id(&db, node_id)?;
        assert_eq!(manual.id_value, "user");
        assert_eq!(manual.source, IdSource::Manual);
        assert_eq!(manual.resolved_value(), Some("plugin-1"));

        upsert(
            &mut db,
            node_id,
            "musicbrainz",
            "release_id",
            "plugin-2",
            IdSource::Resolved,
        )?;
        let manual = get_release_id(&db, node_id)?;
        assert_eq!(manual.id_value, "user");
        assert_eq!(manual.source, IdSource::Manual);
        assert_eq!(manual.resolved_value(), Some("plugin-2"));

        upsert(
            &mut db,
            node_id,
            "musicbrainz",
            "release_id",
            "user-2",
            IdSource::Manual,
        )?;
        let manual = get_release_id(&db, node_id)?;
        assert_eq!(manual.id_value, "user-2");
        assert_eq!(manual.resolved_value(), Some("plugin-2"));
        assert_eq!(get_for_entity(&db, node_id)?.len(), 1);
        Ok(())
    }

    #[test]
    fn owner_lookup_finds_only_the_effective_value() -> anyhow::Result<()> {
        let mut db = new_initialized_test_db()?;
        let release = crate::fixtures::insert_release(&mut db, "Release")?;

        upsert(
            &mut db,
            release,
            "musicbrainz",
            "release_id",
            "plugin",
            IdSource::Resolved,
        )?;
        upsert(
            &mut db,
            release,
            "musicbrainz",
            "release_id",
            "user",
            IdSource::Manual,
        )?;

        assert_eq!(
            get_owner(&db, "musicbrainz", "release_id", "user", None)?,
            Some(release)
        );
        assert_eq!(
            get_owner(&db, "musicbrainz", "release_id", "plugin", None)?,
            None
        );
        Ok(())
    }

    #[test]
    fn removing_manual_id_restores_resolved_value() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let node_id = insert_entity(&mut db)?;

        let row_id = upsert(
            &mut db,
            node_id,
            "musicbrainz",
            "release_id",
            "plugin",
            IdSource::Resolved,
        )?;
        upsert(
            &mut db,
            node_id,
            "musicbrainz",
            "release_id",
            "user",
            IdSource::Manual,
        )?;

        assert!(remove_manual_inside_tx(
            &mut db,
            node_id,
            "musicbrainz",
            "release_id"
        )?);
        let restored = get_release_id(&db, node_id)?;
        assert_eq!(restored.id_value, "plugin");
        assert_eq!(restored.source, IdSource::Resolved);
        assert_eq!(restored.resolved_value, None);

        let element = db
            .exec(QueryBuilder::select().ids(row_id).query())?
            .elements
            .into_iter()
            .next()
            .expect("external id element");
        assert_eq!(element_value(&element, "resolved_value"), None);

        assert!(!remove_manual_inside_tx(
            &mut db,
            node_id,
            "musicbrainz",
            "release_id"
        )?);
        Ok(())
    }

    #[test]
    fn removing_manual_id_without_resolved_value_removes_the_id() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let node_id = insert_entity(&mut db)?;

        upsert(
            &mut db,
            node_id,
            "musicbrainz",
            "release_id",
            "user",
            IdSource::Manual,
        )?;

        assert!(remove_manual_inside_tx(
            &mut db,
            node_id,
            "musicbrainz",
            "release_id"
        )?);
        assert!(get_for_entity(&db, node_id)?.is_empty());
        assert!(!remove_manual_inside_tx(
            &mut db,
            node_id,
            "musicbrainz",
            "release_id"
        )?);
        Ok(())
    }

    #[test]
    fn merge_into_an_entity_without_the_id_carries_both_values() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let from = insert_entity(&mut db)?;
        let to = insert_entity(&mut db)?;

        upsert(
            &mut db,
            from,
            "musicbrainz",
            "release_id",
            "plugin",
            IdSource::Resolved,
        )?;
        upsert(
            &mut db,
            from,
            "musicbrainz",
            "release_id",
            "user",
            IdSource::Manual,
        )?;
        let source = get_release_id(&db, from)?;
        db.transaction_mut(|t| merge_inside_tx(t, to, &source))?;

        let copied = get_release_id(&db, to)?;
        assert_eq!(copied.id_value, "user");
        assert_eq!(copied.source, IdSource::Manual);
        assert_eq!(copied.resolved_value(), Some("plugin"));
        Ok(())
    }

    #[test]
    fn merge_keeps_the_entitys_values_but_lets_a_manual_value_take_effect() -> anyhow::Result<()> {
        type Value = (&'static str, IdSource);
        // Into's values, then the merged values, then the expected effective
        // and resolved values.
        type Case = (
            &'static [Value],
            &'static [Value],
            Value,
            Option<&'static str>,
        );
        let cases: [Case; 8] = [
            (
                &[("kept", IdSource::Resolved)],
                &[("merged", IdSource::Resolved)],
                ("kept", IdSource::Resolved),
                Some("kept"),
            ),
            (
                &[("kept", IdSource::Resolved)],
                &[("merged", IdSource::Manual)],
                ("merged", IdSource::Manual),
                Some("kept"),
            ),
            (
                &[("kept", IdSource::Resolved)],
                &[
                    ("beneath", IdSource::Resolved),
                    ("merged", IdSource::Manual),
                ],
                ("merged", IdSource::Manual),
                Some("kept"),
            ),
            (
                &[("kept", IdSource::Manual)],
                &[
                    ("beneath", IdSource::Resolved),
                    ("merged", IdSource::Manual),
                ],
                ("kept", IdSource::Manual),
                Some("beneath"),
            ),
            (
                &[("kept", IdSource::Resolved), ("pinned", IdSource::Manual)],
                &[("merged", IdSource::Manual)],
                ("pinned", IdSource::Manual),
                Some("kept"),
            ),
            (
                &[("kept", IdSource::Resolved), (" ", IdSource::Manual)],
                &[("merged", IdSource::Manual)],
                ("merged", IdSource::Manual),
                Some("kept"),
            ),
            (
                &[(" ", IdSource::Resolved)],
                &[("merged", IdSource::Resolved)],
                ("merged", IdSource::Resolved),
                Some("merged"),
            ),
            (
                &[(" ", IdSource::Resolved)],
                &[
                    ("beneath", IdSource::Resolved),
                    ("merged", IdSource::Manual),
                ],
                ("merged", IdSource::Manual),
                Some("beneath"),
            ),
        ];
        for (into_values, merged_values, (effective, source), resolved) in cases {
            let mut db = new_test_db()?;
            let into = insert_entity(&mut db)?;
            let merged = insert_entity(&mut db)?;
            for (node_id, values) in [(into, into_values), (merged, merged_values)] {
                for &(value, value_source) in values.iter() {
                    upsert(
                        &mut db,
                        node_id,
                        "musicbrainz",
                        "release_id",
                        value,
                        value_source,
                    )?;
                }
            }
            let source_id = get_release_id(&db, merged)?;
            db.transaction_mut(|t| merge_inside_tx(t, into, &source_id))?;

            let result = get_release_id(&db, into)?;
            let case = format!("{into_values:?} <- {merged_values:?}");
            assert_eq!(
                (result.id_value.as_str(), result.source),
                (effective, source),
                "{case}"
            );
            assert_eq!(result.resolved_value(), resolved, "{case}");
        }
        Ok(())
    }

    #[test]
    fn plural_owner_lookup_propagates_index_errors() -> anyhow::Result<()> {
        let db = new_test_db()?;

        assert!(get_owners(&db, "discogs", "release_id", "abc-1", None).is_err());
        assert_eq!(
            get_owner(&db, "discogs", "release_id", "abc-1", None)?,
            None
        );
        Ok(())
    }

    #[test]
    fn get_owners_returns_all_matching_owners_deduplicated_and_filtered() -> anyhow::Result<()> {
        let mut db = new_initialized_test_db()?;
        let release_a = crate::fixtures::insert_release(&mut db, "Release A")?;
        let release_b = crate::fixtures::insert_release(&mut db, "Release B")?;
        let track = crate::fixtures::insert_track(&mut db, "Track")?;

        let release_b_external_id = upsert(
            &mut db,
            release_b,
            "musicbrainz",
            "release_group_id",
            "group-1",
            IdSource::Resolved,
        )?;
        upsert(
            &mut db,
            release_a,
            "musicbrainz",
            "release_group_id",
            "group-1",
            IdSource::Resolved,
        )?;
        upsert(
            &mut db,
            track,
            "musicbrainz",
            "release_group_id",
            "group-1",
            IdSource::Resolved,
        )?;

        assert_eq!(
            get_owner(
                &db,
                "musicbrainz",
                "release_group_id",
                "group-1",
                Some("Release"),
            )?,
            Some(release_b)
        );

        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(release_a)
                .to(release_b_external_id)
                .query(),
        )?;

        let mut expected_all = vec![release_a, release_b, track];
        expected_all.sort_by_key(|id| id.0);
        assert_eq!(
            get_owners(&db, "musicbrainz", "release_group_id", "group-1", None,)?,
            expected_all
        );

        let mut expected_releases = vec![release_a, release_b];
        expected_releases.sort_by_key(|id| id.0);
        assert_eq!(
            get_owners(
                &db,
                "musicbrainz",
                "release_group_id",
                "group-1",
                Some("Release"),
            )?,
            expected_releases
        );
        assert!(get_owners(&db, "musicbrainz", "release_id", "group-1", None,)?.is_empty());
        assert!(get_owners(&db, "discogs", "release_group_id", "group-1", None,)?.is_empty());

        Ok(())
    }

    fn aliases_of(db: &DbAny, node_id: DbId) -> anyhow::Result<Vec<String>> {
        let mut values: Vec<String> = get_aliases_for_entity(db, node_id)?
            .into_iter()
            .map(|alias| alias.id_value)
            .collect();
        values.sort();
        Ok(values)
    }

    fn rows_of_kind(db: &DbAny, kind: &str) -> anyhow::Result<usize> {
        Ok(db
            .exec(
                QueryBuilder::search()
                    .elements()
                    .where_()
                    .key("db_element_id")
                    .value(kind)
                    .query(),
            )?
            .elements
            .len())
    }

    fn two_entities(
        winner_value: &str,
        loser_value: &str,
        source: IdSource,
    ) -> anyhow::Result<(DbAny, DbId, DbId)> {
        let mut db = new_initialized_test_db()?;
        let winner = insert_entity(&mut db)?;
        let loser = insert_entity(&mut db)?;
        upsert(&mut db, winner, "p", "t", winner_value, IdSource::Resolved)?;
        upsert(&mut db, loser, "p", "t", loser_value, source)?;
        Ok((db, winner, loser))
    }

    #[test]
    fn absorbing_keeps_a_differing_value_that_still_finds_the_winner() -> anyhow::Result<()> {
        let (mut db, winner, loser) = two_entities("a", "b", IdSource::Resolved)?;

        absorb_inside_tx(&mut db, winner, loser)?;
        db.exec_mut(QueryBuilder::remove().ids(loser).query())?;
        remove_loser_rows(&mut db)?;

        assert_eq!(aliases_of(&db, winner)?, ["b"]);
        assert_eq!(get_owner(&db, "p", "t", "b", None)?, Some(winner));
        assert_eq!(get_owners(&db, "p", "t", "b", None)?, [winner]);
        assert_eq!(get(&db, winner, "p", "t")?.expect("row").id_value, "a");
        Ok(())
    }

    /// Rows left by removing an entity's node alone, as `remove_all_for_owner` would have taken.
    fn remove_loser_rows(db: &mut DbAny) -> anyhow::Result<()> {
        let orphans: Vec<DbId> = db
            .exec(
                QueryBuilder::search()
                    .elements()
                    .where_()
                    .key("db_element_id")
                    .value("ExternalId")
                    .query(),
            )?
            .ids()
            .into_iter()
            .filter(|&id| get_owner_id(db, id).is_ok_and(|owner| owner.is_none()))
            .collect();
        db.exec_mut(QueryBuilder::remove().ids(orphans).query())?;
        Ok(())
    }

    #[test]
    fn get_owners_returns_row_holders_and_alias_holders() -> anyhow::Result<()> {
        let (mut db, winner, loser) = two_entities("a", "b", IdSource::Resolved)?;
        let holder = insert_entity(&mut db)?;
        upsert(&mut db, holder, "p", "t", "b", IdSource::Resolved)?;

        absorb_inside_tx(&mut db, winner, loser)?;

        let mut expected = vec![winner, loser, holder];
        expected.sort_by_key(|id| id.0);
        assert_eq!(get_owners(&db, "p", "t", "b", None)?, expected);
        assert_eq!(
            get_owner(&db, "p", "t", "b", None)?.filter(|id| *id != winner),
            get_owner(&db, "p", "t", "b", None)?,
            "a row comes before an alias"
        );
        Ok(())
    }

    #[test]
    fn absorbing_keeps_both_a_manual_value_and_the_resolved_one_beneath() -> anyhow::Result<()> {
        let (mut db, winner, loser) = two_entities("a", "resolved", IdSource::Resolved)?;
        upsert(&mut db, winner, "p", "t", "m", IdSource::Manual)?;
        upsert(&mut db, loser, "p", "t", "other", IdSource::Manual)?;

        let kept = absorb_plan(&db, winner, &[loser])?;
        absorb_inside_tx(&mut db, winner, loser)?;

        assert_eq!(aliases_of(&db, winner)?, ["other", "resolved"]);
        let mut predicted: Vec<String> = kept.into_iter().map(|alias| alias.id_value).collect();
        predicted.sort();
        assert_eq!(predicted, ["other", "resolved"]);
        Ok(())
    }

    #[test]
    fn absorbing_adds_no_alias_for_a_value_the_winner_takes_or_holds() -> anyhow::Result<()> {
        let mut db = new_initialized_test_db()?;
        let winner = insert_entity(&mut db)?;
        let loser = insert_entity(&mut db)?;
        let same = insert_entity(&mut db)?;
        upsert(&mut db, loser, "p", "t", "b", IdSource::Resolved)?;
        upsert(&mut db, same, "p", "t", "b", IdSource::Resolved)?;

        assert!(absorb_plan(&db, winner, &[loser, same])?.is_empty());
        absorb_inside_tx(&mut db, winner, loser)?;
        absorb_inside_tx(&mut db, winner, same)?;

        assert_eq!(get(&db, winner, "p", "t")?.expect("row").id_value, "b");
        assert!(aliases_of(&db, winner)?.is_empty());
        Ok(())
    }

    #[test]
    fn a_value_a_later_manual_value_displaces_still_finds_the_winner() -> anyhow::Result<()> {
        let mut db = new_initialized_test_db()?;
        let winner = insert_entity(&mut db)?;
        let first = insert_entity(&mut db)?;
        let second = insert_entity(&mut db)?;
        upsert(&mut db, first, "p", "t", "a", IdSource::Resolved)?;
        upsert(&mut db, second, "p", "t", "b", IdSource::Manual)?;

        let planned: Vec<String> = absorb_plan(&db, winner, &[first, second])?
            .into_iter()
            .map(|alias| alias.id_value)
            .collect();
        assert_eq!(planned, ["a"]);

        absorb_inside_tx(&mut db, winner, first)?;
        absorb_inside_tx(&mut db, winner, second)?;
        for loser in [first, second] {
            remove_all_for_owner(&mut db, loser)?;
        }

        let row = get(&db, winner, "p", "t")?.expect("row");
        assert_eq!(row.id_value, "b");
        assert_eq!(row.resolved_value(), Some("a"));
        assert_eq!(aliases_of(&db, winner)?, ["a"]);
        for value in ["a", "b"] {
            assert_eq!(get_owner(&db, "p", "t", value, None)?, Some(winner));
        }
        Ok(())
    }

    #[test]
    fn the_winners_own_value_stays_findable_under_a_losers_manual_value() -> anyhow::Result<()> {
        let (mut db, winner, loser) = two_entities("a", "b", IdSource::Manual)?;

        absorb_inside_tx(&mut db, winner, loser)?;
        remove_all_for_owner(&mut db, loser)?;

        assert_eq!(get(&db, winner, "p", "t")?.expect("row").id_value, "b");
        assert_eq!(get_owner(&db, "p", "t", "a", None)?, Some(winner));
        Ok(())
    }

    #[test]
    fn a_losers_value_that_matches_only_what_a_manual_value_hides_is_kept() -> anyhow::Result<()> {
        let (mut db, winner, loser) = two_entities("r", "r", IdSource::Resolved)?;
        upsert(&mut db, winner, "p", "t", "m", IdSource::Manual)?;

        absorb_inside_tx(&mut db, winner, loser)?;
        remove_all_for_owner(&mut db, loser)?;

        assert_eq!(get_owner(&db, "p", "t", "r", None)?, Some(winner));
        Ok(())
    }

    #[test]
    fn chained_absorbs_carry_aliases_once() -> anyhow::Result<()> {
        let (mut db, first, second) = two_entities("a", "b", IdSource::Resolved)?;
        let third = insert_entity(&mut db)?;
        upsert(&mut db, third, "p", "t", "c", IdSource::Resolved)?;

        absorb_inside_tx(&mut db, second, third)?;
        absorb_inside_tx(&mut db, first, second)?;
        absorb_inside_tx(&mut db, first, second)?;

        assert_eq!(aliases_of(&db, first)?, ["b", "c"]);
        Ok(())
    }

    #[test]
    fn a_resolved_write_of_an_alias_leaves_the_id_stable() -> anyhow::Result<()> {
        let (mut db, winner, loser) = two_entities("a", "b", IdSource::Resolved)?;
        absorb_inside_tx(&mut db, winner, loser)?;

        for value in ["b", "a", "b", "b", "a"] {
            upsert(&mut db, winner, "p", "t", value, IdSource::Resolved)?;
            assert_eq!(get(&db, winner, "p", "t")?.expect("row").id_value, "a");
        }
        assert_eq!(aliases_of(&db, winner)?, ["b"]);
        Ok(())
    }

    #[test]
    fn a_new_resolved_value_or_a_manual_alias_write_goes_through() -> anyhow::Result<()> {
        let (mut db, winner, loser) = two_entities("a", "b", IdSource::Resolved)?;
        absorb_inside_tx(&mut db, winner, loser)?;

        upsert(&mut db, winner, "p", "t", "c", IdSource::Resolved)?;
        assert_eq!(get(&db, winner, "p", "t")?.expect("row").id_value, "c");

        upsert(&mut db, winner, "p", "t", "b", IdSource::Manual)?;
        let row = get(&db, winner, "p", "t")?.expect("row");
        assert_eq!(row.id_value, "b");
        assert_eq!(row.resolved_value(), Some("c"));
        Ok(())
    }

    #[test]
    fn an_alias_written_where_no_resolved_value_exists_goes_through() -> anyhow::Result<()> {
        let (mut db, winner, loser) = two_entities("a", "b", IdSource::Resolved)?;
        absorb_inside_tx(&mut db, winner, loser)?;
        upsert(&mut db, winner, "p", "t", "m", IdSource::Manual)?;
        // Without a resolved value beneath the manual one.
        let row = get(&db, winner, "p", "t")?.expect("row");
        db.exec_mut(
            QueryBuilder::remove()
                .values([DbValue::from("resolved_value")])
                .ids(DbId::from(row.db_id.clone().expect("id")))
                .query(),
        )?;

        upsert(&mut db, winner, "p", "t", "b", IdSource::Resolved)?;

        let row = get(&db, winner, "p", "t")?.expect("row");
        assert_eq!(row.id_value, "m");
        assert_eq!(row.resolved_value(), Some("b"));
        Ok(())
    }

    #[test]
    fn merging_a_track_id_keeps_no_alias() -> anyhow::Result<()> {
        let (mut db, winner, loser) = two_entities("a", "b", IdSource::Resolved)?;

        for id in get_for_entity(&db, loser)? {
            merge_inside_tx(&mut db, winner, &id)?;
        }

        assert!(aliases_of(&db, winner)?.is_empty());
        Ok(())
    }

    #[test]
    fn removing_an_alias_stops_it_resolving() -> anyhow::Result<()> {
        let (mut db, winner, loser) = two_entities("a", "b", IdSource::Resolved)?;
        absorb_inside_tx(&mut db, winner, loser)?;
        db.exec_mut(QueryBuilder::remove().ids(loser).query())?;
        remove_loser_rows(&mut db)?;

        assert!(!remove_alias(&mut db, winner, "p", "t", "missing")?);
        assert!(!remove_alias(&mut db, winner, "p", "other", "b")?);
        assert!(remove_alias(&mut db, winner, "p", "t", "b")?);

        assert_eq!(get_owner(&db, "p", "t", "b", None)?, None);
        assert_eq!(get(&db, winner, "p", "t")?.expect("row").id_value, "a");
        Ok(())
    }

    #[test]
    fn removing_an_owners_ids_takes_its_aliases() -> anyhow::Result<()> {
        let (mut db, winner, loser) = two_entities("a", "b", IdSource::Resolved)?;
        absorb_inside_tx(&mut db, winner, loser)?;

        remove_all_for_owner(&mut db, winner)?;

        assert_eq!(rows_of_kind(&db, "ExternalIdAlias")?, 0);
        assert_eq!(rows_of_kind(&db, "ExternalId")?, 1, "only the loser's row");
        Ok(())
    }
}
