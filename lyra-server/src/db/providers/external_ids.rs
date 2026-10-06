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

use super::super::DbAccess;
use serde::{
    Deserialize,
    Serialize,
};

use super::super::NodeId;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(DbElement, Serialize, Clone, Debug)]
pub(crate) struct ExternalId {
    #[serde(skip)]
    pub(crate) db_id: Option<NodeId>,
    pub(crate) provider_id: String,
    pub(crate) id_type: String,
    /// The effective value: the manual value when present, else the resolved one.
    pub(crate) id_value: String,
    pub(crate) source: IdSource,
    /// The resolved value, kept beneath a manual one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) resolved_value: Option<String>,
}

impl ExternalId {
    /// The value a plugin last set, whether or not a manual value hides it.
    pub(crate) fn resolved_value(&self) -> Option<&str> {
        match self.source {
            IdSource::Resolved => Some(&self.id_value),
            IdSource::Manual => self.resolved_value.as_deref(),
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, DbTypeMarker)]
#[serde(rename_all = "snake_case")]
pub(crate) enum IdSource {
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

pub(crate) fn get_for_album_tracks(
    db: &DbAny,
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

pub(crate) fn get_for_entity(db: &DbAny, node_id: DbId) -> anyhow::Result<Vec<ExternalId>> {
    get_for_entity_inside_tx(db, node_id)
}

/// Transaction-capable variant of [`get_for_entity`].
pub(crate) fn get_for_entity_inside_tx(
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

pub(crate) fn get_all_for_tracks(db: &DbAny) -> anyhow::Result<Vec<ExternalId>> {
    let ids: Vec<ExternalId> = db
        .exec(
            QueryBuilder::select()
                .elements::<ExternalId>()
                .search()
                .from("tracks")
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

    Ok(ids)
}

pub(crate) fn get_owner_id(db: &impl DbAccess, external_id: DbId) -> anyhow::Result<Option<DbId>> {
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

pub(crate) fn get_owner(
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

pub(crate) fn get_owners(
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

fn matching_external_id_ids(
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
                .element::<ExternalId>()
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

pub(crate) fn get(
    db: &DbAny,
    node_id: DbId,
    provider_id: &str,
    id_type: &str,
) -> anyhow::Result<Option<ExternalId>> {
    get_inside_tx(db, node_id, provider_id, id_type)
}

/// Transaction-capable variant of [`get`].
pub(crate) fn get_inside_tx(
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

pub(crate) fn upsert(
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
/// A manual write keeps the resolved value beneath it, and a resolved write
/// under a manual value updates only that kept value.
pub(crate) fn upsert_inside_tx(
    db: &mut impl DbAccess,
    node_id: DbId,
    provider_id: &str,
    id_type: &str,
    id_value: &str,
    source: IdSource,
) -> anyhow::Result<DbId> {
    let existing = get_inside_tx(db, node_id, provider_id, id_type)?;

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

/// Copies both layers of `external_id` onto `node_id`, the way a plugin write
/// and then a user write would land.
pub(crate) fn copy_inside_tx(
    db: &mut impl DbAccess,
    node_id: DbId,
    external_id: &ExternalId,
) -> anyhow::Result<()> {
    if let Some(resolved_value) = external_id.resolved_value() {
        upsert_inside_tx(
            db,
            node_id,
            &external_id.provider_id,
            &external_id.id_type,
            resolved_value,
            IdSource::Resolved,
        )?;
    }
    if external_id.source == IdSource::Manual {
        upsert_inside_tx(
            db,
            node_id,
            &external_id.provider_id,
            &external_id.id_type,
            &external_id.id_value,
            IdSource::Manual,
        )?;
    }
    Ok(())
}

/// Removes the manual value, so the resolved one takes effect again. Returns
/// false when there was no manual value.
pub(crate) fn remove_manual(
    db: &mut DbAny,
    node_id: DbId,
    provider_id: &str,
    id_type: &str,
) -> anyhow::Result<bool> {
    db.transaction_mut(|t| -> anyhow::Result<bool> {
        let Some(existing) = get_inside_tx(t, node_id, provider_id, id_type)? else {
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
                t.exec_mut(QueryBuilder::insert().element(&restored).query())?;
                // Inserting an element leaves the keys its `None` fields omit.
                t.exec_mut(
                    QueryBuilder::remove()
                        .values([DbValue::from("resolved_value")])
                        .ids(db_id)
                        .query(),
                )?;
            }
            None => {
                t.exec_mut(QueryBuilder::remove().ids(db_id).query())?;
            }
        }
        Ok(true)
    })
}

/// Remove every `ExternalId` attached to an owner. Call before deleting the
/// owner so the `external_ids` root edge doesn't keep orphaned rows alive.
pub(crate) fn remove_all_for_owner(db: &mut impl DbAccess, owner_id: DbId) -> anyhow::Result<()> {
    let ids: Vec<DbId> = db
        .exec(
            QueryBuilder::search()
                .from(owner_id)
                .where_()
                .distance(agdb::CountComparison::Equal(2))
                .and()
                .key("db_element_id")
                .value("ExternalId")
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
    use crate::db::test_db::TestDb;
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
        let release = crate::db::test_db::insert_release(&mut db, "Release")?;

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

        assert!(remove_manual(
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

        assert!(!remove_manual(
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

        assert!(remove_manual(
            &mut db,
            node_id,
            "musicbrainz",
            "release_id"
        )?);
        assert!(get_for_entity(&db, node_id)?.is_empty());
        assert!(!remove_manual(
            &mut db,
            node_id,
            "musicbrainz",
            "release_id"
        )?);
        Ok(())
    }

    #[test]
    fn copy_carries_manual_and_resolved_values() -> anyhow::Result<()> {
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
        db.transaction_mut(|t| copy_inside_tx(t, to, &source))?;

        let copied = get_release_id(&db, to)?;
        assert_eq!(copied.id_value, "user");
        assert_eq!(copied.source, IdSource::Manual);
        assert_eq!(copied.resolved_value(), Some("plugin"));
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
        let release_a = crate::db::test_db::insert_release(&mut db, "Release A")?;
        let release_b = crate::db::test_db::insert_release(&mut db, "Release B")?;
        let track = crate::db::test_db::insert_track(&mut db, "Track")?;

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
}
