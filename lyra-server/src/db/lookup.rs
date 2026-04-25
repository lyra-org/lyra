// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use super::DbAccess;
use agdb::{
    DbElement,
    DbId,
    DbValue,
    QueryBuilder,
};

fn string_field_value<'a>(element: &'a DbElement, field_name: &str) -> Option<&'a str> {
    element.values.iter().find_map(|kv| {
        if matches!(&kv.key, DbValue::String(key) if key == field_name) {
            match &kv.value {
                DbValue::String(value) => Some(value.as_str()),
                _ => None,
            }
        } else {
            None
        }
    })
}

fn collection_contains_id(
    db: &impl DbAccess,
    collection_alias: &str,
    id: DbId,
) -> anyhow::Result<bool> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .from(collection_alias)
            .where_()
            .neighbor()
            .and()
            .ids(id)
            .query(),
    )?;

    Ok(!result.elements.is_empty())
}

pub(crate) fn find_node_id_by_id(db: &impl DbAccess, id: &str) -> anyhow::Result<Option<DbId>> {
    let result = db.exec(QueryBuilder::search().index("id").value(id).query())?;
    Ok(result.ids().into_iter().find(|id| id.0 > 0))
}

pub(crate) fn find_id_by_db_id(db: &impl DbAccess, db_id: DbId) -> anyhow::Result<Option<String>> {
    let result = db.exec(
        QueryBuilder::select()
            .values(vec![DbValue::String("id".into())])
            .ids(db_id)
            .query(),
    )?;
    Ok(result.elements.into_iter().next().and_then(|element| {
        element.values.into_iter().find_map(|kv| {
            if matches!(&kv.key, DbValue::String(key) if key == "id") {
                match kv.value {
                    DbValue::String(value) => Some(value),
                    _ => None,
                }
            } else {
                None
            }
        })
    }))
}

pub(crate) fn find_ids_by_db_ids(
    db: &impl DbAccess,
    db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, String>> {
    if db_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let result = db.exec(
        QueryBuilder::select()
            .values(vec![DbValue::String("id".into())])
            .ids(db_ids)
            .query(),
    )?;

    let mut map = HashMap::with_capacity(result.elements.len());
    for element in result.elements {
        if element.id.0 <= 0 {
            continue;
        }
        if let Some(id_value) = string_field_value(&element, "id") {
            map.insert(element.id, id_value.to_string());
        }
    }

    Ok(map)
}

pub(crate) fn find_node_ids_by_ids(
    db: &impl DbAccess,
    ids: &[&str],
) -> anyhow::Result<HashMap<String, DbId>> {
    let mut map = HashMap::with_capacity(ids.len());
    for id in ids {
        if let Some(db_id) = find_node_id_by_id(db, id)? {
            map.insert((*id).to_string(), db_id);
        }
    }
    Ok(map)
}

pub(crate) fn find_id_by_string_field(
    db: &impl DbAccess,
    collection_alias: &str,
    field_name: &str,
    value: &str,
) -> anyhow::Result<Option<DbId>> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .from(collection_alias)
            .where_()
            .neighbor()
            .query(),
    )?;

    Ok(result.elements.into_iter().find_map(|element| {
        (string_field_value(&element, field_name) == Some(value)).then_some(element.id)
    }))
}

pub(crate) fn find_id_by_indexed_string_field(
    db: &impl DbAccess,
    collection_alias: &str,
    field_name: &str,
    index_name: &str,
    value: &str,
) -> anyhow::Result<Option<DbId>> {
    let indexed = db.exec(
        QueryBuilder::search()
            .index(index_name)
            .value(value)
            .query(),
    );
    if let Ok(result) = indexed {
        for id in result.ids().into_iter().filter(|id| id.0 > 0) {
            if collection_contains_id(db, collection_alias, id)? {
                return Ok(Some(id));
            }
        }
    }

    find_id_by_string_field(db, collection_alias, field_name, value)
}
