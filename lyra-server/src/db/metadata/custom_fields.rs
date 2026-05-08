// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    DbElement,
    DbId,
    QueryBuilder,
};
use nanoid::nanoid;
use schemars::JsonSchema;
use serde::Serialize;

use super::super::{
    DbAccess,
    NodeId,
};

#[derive(DbElement, Serialize, Clone, Debug, JsonSchema)]
pub(crate) struct ProviderCustomFields {
    #[serde(skip)]
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) provider_id: String,
    pub(crate) version: u64,
    pub(crate) fields: String,
    pub(crate) updated_at: u64,
}

pub(crate) fn get_for_entity(
    db: &impl DbAccess,
    node_id: DbId,
) -> anyhow::Result<Vec<ProviderCustomFields>> {
    let fields: Vec<ProviderCustomFields> = db
        .exec(
            QueryBuilder::select()
                .elements::<ProviderCustomFields>()
                .search()
                .from(node_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    Ok(fields)
}

pub(crate) fn get(
    db: &impl DbAccess,
    node_id: DbId,
    provider_id: &str,
    version: u64,
) -> anyhow::Result<Option<ProviderCustomFields>> {
    Ok(get_for_entity(db, node_id)?
        .into_iter()
        .find(|row| row.provider_id == provider_id && row.version == version))
}

pub(crate) fn upsert(
    db: &mut DbAny,
    node_id: DbId,
    row: &ProviderCustomFields,
) -> anyhow::Result<DbId> {
    db.transaction_mut(|t| -> anyhow::Result<DbId> { upsert_inside_tx(t, node_id, row) })
}

pub(crate) fn upsert_inside_tx(
    db: &mut impl DbAccess,
    node_id: DbId,
    row: &ProviderCustomFields,
) -> anyhow::Result<DbId> {
    let existing = get(db, node_id, &row.provider_id, row.version)?;

    if let Some(existing_row) = &existing
        && existing_row.fields == row.fields
        && let Some(db_id) = existing_row.db_id.clone()
    {
        return Ok(db_id.into());
    }

    let mut row_to_save = row.clone();
    row_to_save.db_id = existing.as_ref().and_then(|row| row.db_id.clone());
    if row_to_save.id.is_empty() {
        row_to_save.id = nanoid!();
    }

    let result = db.exec_mut(QueryBuilder::insert().element(&row_to_save).query())?;
    let row_db_id = existing
        .as_ref()
        .and_then(|row| row.db_id.clone())
        .map(DbId::from)
        .or_else(|| result.elements.first().map(|element| element.id))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "upsert provider custom fields returned no id (node_id={}, provider_id='{}', version={})",
                node_id.0,
                row.provider_id,
                row.version
            )
        })?;

    if existing.is_none() {
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(node_id)
                .to(row_db_id)
                .query(),
        )?;
    }

    Ok(row_db_id)
}

pub(crate) fn remove(
    db: &mut impl DbAccess,
    node_id: DbId,
    provider_id: &str,
    version: u64,
) -> anyhow::Result<bool> {
    let Some(existing) = get(db, node_id, provider_id, version)? else {
        return Ok(false);
    };
    let Some(db_id) = existing.db_id.map(DbId::from) else {
        return Ok(false);
    };

    db.exec_mut(QueryBuilder::remove().ids(db_id).query())?;
    Ok(true)
}

pub(crate) fn copy_between_entities(
    db: &mut impl DbAccess,
    from_node_id: DbId,
    to_node_id: DbId,
) -> anyhow::Result<bool> {
    let mut existing_by_key = std::collections::HashMap::new();
    for row in get_for_entity(db, to_node_id)? {
        existing_by_key.insert((row.provider_id.clone(), row.version), row);
    }

    let mut wrote = false;
    for row in get_for_entity(db, from_node_id)? {
        let should_upsert = existing_by_key
            .get(&(row.provider_id.clone(), row.version))
            .is_none_or(|existing| row.updated_at > existing.updated_at);
        if !should_upsert {
            continue;
        }

        let mut row_to_upsert = row.clone();
        row_to_upsert.db_id = None;
        let row_db_id = upsert_inside_tx(db, to_node_id, &row_to_upsert)?;
        row_to_upsert.db_id = Some(row_db_id.into());
        existing_by_key.insert((row.provider_id.clone(), row.version), row_to_upsert);
        wrote = true;
    }

    Ok(wrote)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::TestDb;
    use agdb::QueryBuilder;

    fn new_test_db() -> anyhow::Result<DbAny> {
        Ok(TestDb::new()?.into_inner())
    }

    fn insert_entity(db: &mut DbAny) -> anyhow::Result<DbId> {
        Ok(db
            .exec_mut(QueryBuilder::insert().nodes().count(1).query())?
            .ids()[0])
    }

    fn row(provider_id: &str, version: u64, fields: &str, updated_at: u64) -> ProviderCustomFields {
        ProviderCustomFields {
            db_id: None,
            id: nanoid!(),
            provider_id: provider_id.to_string(),
            version,
            fields: fields.to_string(),
            updated_at,
        }
    }

    #[test]
    fn upsert_reuses_provider_version_node() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let node_id = insert_entity(&mut db)?;

        let first_id = upsert(&mut db, node_id, &row("musicbrainz", 1, r#"{"a":1}"#, 100))?;
        let second_id = upsert(&mut db, node_id, &row("musicbrainz", 1, r#"{"a":2}"#, 200))?;

        assert_eq!(first_id, second_id);
        let rows = get_for_entity(&db, node_id)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].provider_id, "musicbrainz");
        assert_eq!(rows[0].version, 1);
        assert_eq!(rows[0].fields, r#"{"a":2}"#);
        assert_eq!(rows[0].updated_at, 200);
        Ok(())
    }

    #[test]
    fn versions_coexist_and_can_be_removed() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let node_id = insert_entity(&mut db)?;

        upsert(&mut db, node_id, &row("musicbrainz", 1, r#"{"a":1}"#, 100))?;
        upsert(&mut db, node_id, &row("musicbrainz", 2, r#"{"b":2}"#, 200))?;

        assert_eq!(get_for_entity(&db, node_id)?.len(), 2);
        assert!(remove(&mut db, node_id, "musicbrainz", 1)?);
        assert!(!remove(&mut db, node_id, "musicbrainz", 3)?);

        let rows = get_for_entity(&db, node_id)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].version, 2);
        Ok(())
    }

    #[test]
    fn copy_between_entities_keeps_newer_destination_row() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let from = insert_entity(&mut db)?;
        let to = insert_entity(&mut db)?;

        upsert(
            &mut db,
            from,
            &row("musicbrainz", 1, r#"{"from":true}"#, 100),
        )?;
        upsert(&mut db, to, &row("musicbrainz", 1, r#"{"to":true}"#, 200))?;
        upsert(
            &mut db,
            from,
            &row("musicbrainz", 2, r#"{"next":true}"#, 100),
        )?;

        assert!(copy_between_entities(&mut db, from, to)?);

        let rows = get_for_entity(&db, to)?;
        assert_eq!(rows.len(), 2);
        let version_1 = rows
            .iter()
            .find(|row| row.version == 1)
            .expect("version 1 row");
        let version_2 = rows
            .iter()
            .find(|row| row.version == 2)
            .expect("version 2 row");
        assert_eq!(version_1.fields, r#"{"to":true}"#);
        assert_eq!(version_2.fields, r#"{"next":true}"#);
        Ok(())
    }
}
