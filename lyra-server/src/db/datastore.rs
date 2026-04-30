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
use anyhow::anyhow;
use nanoid::nanoid;
use serde::Serialize;

use super::NodeId;

#[derive(DbElement, Serialize, Clone, Debug)]
#[harmony_macros::structure]
pub(crate) struct DataStore {
    #[serde(skip)]
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) name: String,
}

#[derive(DbElement, Serialize, Clone, Debug)]
pub(crate) struct DataStoreEntry {
    #[serde(skip)]
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) key: String,
    pub(crate) value: String,
}

pub(crate) fn get_entry(
    db: &DbAny,
    datastore_id: DbId,
    key: &str,
) -> anyhow::Result<Option<DataStoreEntry>> {
    let mut entries: Vec<DataStoreEntry> = db
        .exec(
            QueryBuilder::select()
                .elements::<DataStoreEntry>()
                .search()
                .from(datastore_id)
                .where_()
                .key("key")
                .value(key)
                .end_where()
                .query(),
        )?
        .try_into()?;
    Ok(entries.pop())
}

pub(crate) fn upsert_entry(
    db: &mut DbAny,
    datastore_id: DbId,
    key: String,
    value: String,
) -> anyhow::Result<DbId> {
    db.transaction_mut(|t| -> anyhow::Result<DbId> {
        let entries: Vec<DataStoreEntry> = t
            .exec(
                QueryBuilder::select()
                    .elements::<DataStoreEntry>()
                    .search()
                    .from(datastore_id)
                    .where_()
                    .key("key")
                    .value(&key)
                    .end_where()
                    .query(),
            )?
            .try_into()?;

        let existing_db_id = entries.first().and_then(|existing| existing.db_id.clone());
        let entry = DataStoreEntry {
            db_id: existing_db_id.clone(),
            id: nanoid!(),
            key,
            value,
        };
        let result = t.exec_mut(QueryBuilder::insert().element(&entry).query())?;
        let entry_db_id = existing_db_id
            .clone()
            .map(Into::<DbId>::into)
            .or_else(|| result.ids().first().copied())
            .ok_or_else(|| anyhow!("datastore upsert returned no id"))?;

        if existing_db_id.is_none() {
            t.exec_mut(
                QueryBuilder::insert()
                    .edges()
                    .from(datastore_id)
                    .to(entry_db_id)
                    .query(),
            )?;
        }

        Ok(entry_db_id)
    })
}

/// Returns `true` if an entry was removed, `false` if the key wasn't present.
pub(crate) fn remove_entry(db: &mut DbAny, datastore_id: DbId, key: &str) -> anyhow::Result<bool> {
    db.transaction_mut(|t| -> anyhow::Result<bool> {
        let entries: Vec<DataStoreEntry> = t
            .exec(
                QueryBuilder::select()
                    .elements::<DataStoreEntry>()
                    .search()
                    .from(datastore_id)
                    .where_()
                    .key("key")
                    .value(key)
                    .end_where()
                    .query(),
            )?
            .try_into()?;

        let Some(existing_node_id) = entries.first().and_then(|e| e.db_id.clone()) else {
            return Ok(false);
        };
        let target: DbId = existing_node_id.into();
        t.exec_mut(QueryBuilder::remove().ids(target).query())?;
        Ok(true)
    })
}

/// Returns the number of entries removed.
pub(crate) fn clear_entries(db: &mut DbAny, datastore_id: DbId) -> anyhow::Result<usize> {
    db.transaction_mut(|t| -> anyhow::Result<usize> {
        let entries: Vec<DataStoreEntry> = t
            .exec(
                QueryBuilder::select()
                    .elements::<DataStoreEntry>()
                    .search()
                    .from(datastore_id)
                    .query(),
            )?
            .try_into()?;

        let ids: Vec<DbId> = entries
            .iter()
            .filter_map(|e| e.db_id.clone().map(DbId::from))
            .collect();
        if ids.is_empty() {
            return Ok(0);
        }

        let count = ids.len();
        t.exec_mut(QueryBuilder::remove().ids(&ids).query())?;
        Ok(count)
    })
}

/// Removes entries across all datastores whose JSON value carries a passed
/// `expires_at`. Entries without the field are persistent and kept. Returns
/// the number removed.
pub(crate) fn sweep_expired_entries(db: &mut DbAny, now_ms: u64) -> anyhow::Result<usize> {
    db.transaction_mut(|t| -> anyhow::Result<usize> {
        let datastores: Vec<DataStore> = t
            .exec(
                QueryBuilder::select()
                    .elements::<DataStore>()
                    .search()
                    .from("datastore")
                    .query(),
            )?
            .try_into()?;

        let mut to_remove: Vec<DbId> = Vec::new();
        for datastore in datastores {
            let Some(datastore_id) = datastore.db_id.map(DbId::from) else {
                continue;
            };
            let entries: Vec<DataStoreEntry> = t
                .exec(
                    QueryBuilder::select()
                        .elements::<DataStoreEntry>()
                        .search()
                        .from(datastore_id)
                        .query(),
                )?
                .try_into()?;

            for entry in entries {
                let Some(entry_db_id) = entry.db_id.clone().map(DbId::from) else {
                    continue;
                };
                if entry_is_expired(&entry.value, now_ms) {
                    to_remove.push(entry_db_id);
                }
            }
        }

        if to_remove.is_empty() {
            return Ok(0);
        }

        let count = to_remove.len();
        t.exec_mut(QueryBuilder::remove().ids(&to_remove).query())?;
        Ok(count)
    })
}

fn entry_is_expired(value_json: &str, now_ms: u64) -> bool {
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(value_json) else {
        return false;
    };
    let Some(expires_at) = parsed.get("expires_at").and_then(|v| v.as_u64()) else {
        return false;
    };
    expires_at <= now_ms
}

pub(crate) fn find_by_name(db: &DbAny, name: &str) -> anyhow::Result<Option<DataStore>> {
    let datastore: Vec<DataStore> = db
        .exec(
            QueryBuilder::select()
                .elements::<DataStore>()
                .search()
                .from("datastore")
                .where_()
                .key("name")
                .value(name)
                .end_where()
                .query(),
        )?
        .try_into()?;
    Ok(datastore.into_iter().next())
}

pub(crate) fn get_or_create(db: &mut DbAny, name: String) -> anyhow::Result<DataStore> {
    let datastore: Vec<DataStore> = db
        .exec(
            QueryBuilder::select()
                .elements::<DataStore>()
                .search()
                .from("datastore")
                .where_()
                .key("name")
                .value(&name)
                .end_where()
                .query(),
        )?
        .try_into()?;

    if datastore.is_empty() {
        return db.transaction_mut(|t| -> anyhow::Result<DataStore> {
            let datastore_id = nanoid!();
            let datastore = DataStore {
                db_id: None,
                id: datastore_id.clone(),
                name: name.clone(),
            };
            let result = t.exec_mut(QueryBuilder::insert().element(&datastore).query())?;
            let result_id = result.ids()[0];
            t.exec_mut(
                QueryBuilder::insert()
                    .edges()
                    .from("datastore")
                    .to(result_id)
                    .query(),
            )?;

            Ok(DataStore {
                db_id: Some(result_id.into()),
                id: datastore_id,
                name,
            })
        });
    }

    Ok(datastore[0].clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::new_test_db;

    #[test]
    fn get_or_create_returns_the_persisted_id() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let created = get_or_create(&mut db, "cover-cache".to_string())?;
        let fetched = get_or_create(&mut db, "cover-cache".to_string())?;

        assert_eq!(created.db_id.map(DbId::from), fetched.db_id.map(DbId::from));
        assert_eq!(created.id, fetched.id);
        Ok(())
    }

    fn store_id(store: &DataStore) -> DbId {
        store
            .db_id
            .clone()
            .map(DbId::from)
            .expect("store should be persisted")
    }

    #[test]
    fn remove_entry_returns_true_when_present_and_false_when_absent() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let store = get_or_create(&mut db, "cache".to_string())?;
        let id = store_id(&store);
        upsert_entry(&mut db, id, "k".to_string(), "\"v\"".to_string())?;

        assert!(remove_entry(&mut db, id, "k")?);
        assert!(get_entry(&db, id, "k")?.is_none());
        assert!(!remove_entry(&mut db, id, "k")?);
        Ok(())
    }

    #[test]
    fn clear_entries_removes_only_the_targeted_store() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let store_a = get_or_create(&mut db, "a".to_string())?;
        let store_b = get_or_create(&mut db, "b".to_string())?;
        let id_a = store_id(&store_a);
        let id_b = store_id(&store_b);

        upsert_entry(&mut db, id_a, "x".to_string(), "1".to_string())?;
        upsert_entry(&mut db, id_a, "y".to_string(), "2".to_string())?;
        upsert_entry(&mut db, id_b, "z".to_string(), "3".to_string())?;

        let removed = clear_entries(&mut db, id_a)?;
        assert_eq!(removed, 2);
        assert!(get_entry(&db, id_a, "x")?.is_none());
        assert!(get_entry(&db, id_a, "y")?.is_none());
        assert!(get_entry(&db, id_b, "z")?.is_some());
        Ok(())
    }

    #[test]
    fn entry_is_expired_only_when_field_present_and_past() {
        assert!(!entry_is_expired("\"plain\"", 1000));
        assert!(!entry_is_expired("{}", 1000));
        assert!(!entry_is_expired("{\"value\":\"x\"}", 1000));

        assert!(entry_is_expired("{\"expires_at\":500}", 1000));
        assert!(entry_is_expired("{\"expires_at\":1000}", 1000));

        assert!(!entry_is_expired("{\"expires_at\":2000}", 1000));

        assert!(!entry_is_expired("not json", 1000));
        assert!(!entry_is_expired("{\"expires_at\":\"soon\"}", 1000));
    }

    #[test]
    fn sweep_expired_entries_drops_only_past_expires_at_entries() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let store = get_or_create(&mut db, "cache".to_string())?;
        let id = store_id(&store);

        upsert_entry(
            &mut db,
            id,
            "stale".to_string(),
            "{\"expires_at\":100}".to_string(),
        )?;
        upsert_entry(
            &mut db,
            id,
            "fresh".to_string(),
            "{\"expires_at\":9999}".to_string(),
        )?;
        upsert_entry(
            &mut db,
            id,
            "persistent".to_string(),
            "{\"value\":\"keep me\"}".to_string(),
        )?;

        let removed = sweep_expired_entries(&mut db, 1000)?;
        assert_eq!(removed, 1);
        assert!(get_entry(&db, id, "stale")?.is_none());
        assert!(get_entry(&db, id, "fresh")?.is_some());
        assert!(get_entry(&db, id, "persistent")?.is_some());
        Ok(())
    }
}
