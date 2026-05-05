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
use nanoid::nanoid;
use schemars::JsonSchema;
use serde::{
    Deserialize,
    Serialize,
};

use super::super::NodeId;

#[derive(DbElement, Serialize, Clone, Debug, JsonSchema)]
pub(crate) struct ExternalId {
    #[serde(skip)]
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) provider_id: String,
    pub(crate) id_type: String,
    pub(crate) id_value: String,
    pub(crate) source: IdSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, DbTypeMarker)]
#[serde(rename_all = "snake_case")]
pub(crate) enum IdSource {
    Plugin,
    User,
}

impl IdSource {
    fn as_db_str(self) -> &'static str {
        match self {
            Self::Plugin => "plugin",
            Self::User => "user",
        }
    }

    fn from_db_str(value: &str) -> Result<Self, DbError> {
        match value {
            "plugin" => Ok(Self::Plugin),
            "user" => Ok(Self::User),
            _ => Err(DbError::from(format!("invalid IdSource value '{value}'"))),
        }
    }
}

impl fmt::Display for IdSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_db_str())
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
    let Ok(index_result) = db.exec(
        QueryBuilder::search()
            .index("id_value")
            .value(id_value)
            .query(),
    ) else {
        return Ok(None);
    };

    for ext_id_db_id in index_result.ids().into_iter().filter(|id| id.0 > 0) {
        let ext_ids: Vec<ExternalId> = db
            .exec(QueryBuilder::select().ids(ext_id_db_id).query())?
            .try_into()?;
        let Some(ext) = ext_ids.into_iter().next() else {
            continue;
        };
        if ext.provider_id != provider_id || ext.id_type != id_type {
            continue;
        }
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
pub(crate) fn upsert_inside_tx(
    db: &mut impl DbAccess,
    node_id: DbId,
    provider_id: &str,
    id_type: &str,
    id_value: &str,
    source: IdSource,
) -> anyhow::Result<DbId> {
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
    let existing = ids
        .into_iter()
        .find(|id| id.provider_id == provider_id && id.id_type == id_type);

    // User-set IDs take priority over plugin-set IDs
    if let Some(existing_id) = &existing
        && existing_id.source == IdSource::User
        && source == IdSource::Plugin
        && let Some(db_id) = &existing_id.db_id
    {
        return Ok(db_id.clone().into());
    }

    // Avoid no-op rewrites when a provider repeatedly submits the same ID.
    if let Some(existing_id) = &existing
        && existing_id.id_value == id_value
        && existing_id.source == source
        && let Some(db_id) = &existing_id.db_id
    {
        return Ok(db_id.clone().into());
    }

    let external_id = ExternalId {
        db_id: existing.as_ref().and_then(|e| e.db_id.clone()),
        id: nanoid!(),
        provider_id: provider_id.to_string(),
        id_type: id_type.to_string(),
        id_value: id_value.to_string(),
        source,
    };

    let result = db.exec_mut(QueryBuilder::insert().element(&external_id).query())?;
    let id_db_id = existing
        .as_ref()
        .and_then(|e| e.db_id.clone())
        .map(DbId::from)
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
        assert_eq!(DbValue::from(IdSource::Plugin), DbValue::from("plugin"));
        assert_eq!(IdSource::try_from(DbValue::from("user"))?, IdSource::User);
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
            IdSource::User,
        )?;
        let second_id = upsert(
            &mut db,
            node_id,
            "discogs",
            "release_id",
            "abc-2",
            IdSource::User,
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
            IdSource::Plugin,
        )?;
        let second_id = upsert(
            &mut db,
            node_id,
            "discogs",
            "release_id",
            "abc-1",
            IdSource::Plugin,
        )?;

        assert_eq!(first_id, second_id);

        let external_ids = get_for_entity(&db, node_id)?;
        assert_eq!(external_ids.len(), 1);
        assert_eq!(external_ids[0].id_value, "abc-1");
        assert_eq!(external_ids[0].source, IdSource::Plugin);

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
            IdSource::Plugin,
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
}
