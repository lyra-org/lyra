// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    DbElement,
    DbId,
    DbValue,
    QueryBuilder,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MetadataEntityType {
    Release,
    Track,
    Artist,
}

fn classify_element(element: &DbElement) -> Option<MetadataEntityType> {
    let key = DbValue::from("db_element_id");
    let value = element.values.iter().find(|kv| kv.key == key)?;
    match &value.value {
        DbValue::String(s) => match s.as_str() {
            "Release" => Some(MetadataEntityType::Release),
            "Track" => Some(MetadataEntityType::Track),
            "Artist" => Some(MetadataEntityType::Artist),
            _ => None,
        },
        _ => None,
    }
}

pub(crate) fn metadata_entity_type(
    db: &DbAny,
    node_id: DbId,
) -> anyhow::Result<Option<MetadataEntityType>> {
    let result = db.exec(QueryBuilder::select().ids(node_id).query())?;
    let Some(element) = result.elements.first() else {
        return Ok(None);
    };

    Ok(classify_element(element))
}

pub(crate) fn get_element_type(db: &DbAny, node_id: DbId) -> anyhow::Result<Option<String>> {
    let Ok(result) = db.exec(QueryBuilder::select().ids(node_id).query()) else {
        return Ok(None);
    };
    let Some(element) = result.elements.first() else {
        return Ok(None);
    };
    let key = DbValue::from("db_element_id");
    let value = element.values.iter().find(|kv| kv.key == key);
    match value.map(|kv| &kv.value) {
        Some(DbValue::String(s)) => Ok(Some(s.clone())),
        _ => Ok(None),
    }
}

pub(crate) fn exists(db: &DbAny, node_id: DbId) -> anyhow::Result<bool> {
    Ok(metadata_entity_type(db, node_id)?.is_some())
}

pub(crate) fn set_locked(db: &mut DbAny, node_id: DbId, locked: bool) -> anyhow::Result<bool> {
    db.transaction_mut(|t| -> anyhow::Result<bool> {
        let result = t.exec(QueryBuilder::select().ids(node_id).query())?;
        let Some(element) = result.elements.first() else {
            return Ok(false);
        };
        let Some(_entity_type) = classify_element(element) else {
            return Ok(false);
        };

        t.exec_mut(
            QueryBuilder::insert()
                .values([[("locked", locked).into()]])
                .ids(node_id)
                .query(),
        )?;

        Ok(true)
    })
}
