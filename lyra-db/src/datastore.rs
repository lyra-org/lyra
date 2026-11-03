// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    Comparison,
    DbAny,
    DbAnyTransactionMut,
    DbElement,
    DbId,
    QueryBuilder,
};

use crate::DbAccess;
use crate::roots::MEMBERS;
use anyhow::anyhow;
use nanoid::nanoid;
use serde::Serialize;

use super::NodeId;

#[derive(DbElement, Serialize, Clone, Debug)]
pub struct DataStore {
    #[serde(skip)]
    pub db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) name: String,
}

#[derive(DbElement, Serialize, Clone, Debug)]
pub struct DataStoreEntry {
    #[serde(skip)]
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) key: String,
    pub value: String,
    pub expires_at_ms: Option<u64>,
}

impl DataStoreEntry {
    fn is_live(&self, now_ms: u64) -> bool {
        self.expires_at_ms
            .is_none_or(|expires_at_ms| expires_at_ms > now_ms)
    }
}

/// A value to store under `key`; without `expires_at_ms` the entry is
/// persistent.
pub struct EntryWrite {
    pub key: String,
    pub value: String,
    pub expires_at_ms: Option<u64>,
}

fn find_entry(
    db: &impl DbAccess,
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

/// Returns the entry under `key` unless it is missing or expired at `now_ms`.
pub fn get_entry(
    db: &impl DbAccess,
    datastore_id: DbId,
    key: &str,
    now_ms: u64,
) -> anyhow::Result<Option<DataStoreEntry>> {
    Ok(find_entry(db, datastore_id, key)?.filter(|entry| entry.is_live(now_ms)))
}

pub fn upsert_entry(db: &mut DbAny, datastore_id: DbId, write: EntryWrite) -> anyhow::Result<()> {
    db.transaction_mut(|t| upsert(t, datastore_id, write))
}

/// Writes every entry in one transaction.
pub fn upsert_many(
    db: &mut DbAny,
    datastore_id: DbId,
    writes: Vec<EntryWrite>,
) -> anyhow::Result<()> {
    db.transaction_mut(|t| -> anyhow::Result<()> {
        for write in writes {
            upsert(t, datastore_id, write)?;
        }
        Ok(())
    })
}

fn upsert(
    t: &mut DbAnyTransactionMut<'_>,
    datastore_id: DbId,
    write: EntryWrite,
) -> anyhow::Result<()> {
    let existing_db_id = find_entry(t, datastore_id, &write.key)?.and_then(|entry| entry.db_id);
    let entry = DataStoreEntry {
        db_id: existing_db_id.clone(),
        id: nanoid!(),
        key: write.key,
        value: write.value,
        expires_at_ms: write.expires_at_ms,
    };
    let result = t.exec_mut(QueryBuilder::insert().element(&entry).query())?;

    match existing_db_id.map(DbId::from) {
        Some(entry_db_id) => {
            // An insert keeps fields the new element omits.
            if entry.expires_at_ms.is_none() {
                t.exec_mut(
                    QueryBuilder::remove()
                        .values(["expires_at_ms"])
                        .ids(entry_db_id)
                        .query(),
                )?;
            }
        }
        None => {
            let entry_db_id = result
                .ids()
                .first()
                .copied()
                .ok_or_else(|| anyhow!("datastore upsert returned no id"))?;
            t.exec_mut(
                QueryBuilder::insert()
                    .edges()
                    .from(datastore_id)
                    .to(entry_db_id)
                    .query(),
            )?;
        }
    }
    Ok(())
}

/// Removes the entry under `key`. Returns whether a live entry was removed.
pub fn remove_entry(
    db: &mut DbAny,
    datastore_id: DbId,
    key: &str,
    now_ms: u64,
) -> anyhow::Result<bool> {
    db.transaction_mut(|t| -> anyhow::Result<bool> {
        let Some(entry) = find_entry(t, datastore_id, key)? else {
            return Ok(false);
        };
        let Some(entry_db_id) = entry.db_id.clone().map(DbId::from) else {
            return Ok(false);
        };
        t.exec_mut(QueryBuilder::remove().ids(entry_db_id).query())?;
        Ok(entry.is_live(now_ms))
    })
}

/// Removes every entry. Returns the number of live entries removed.
pub fn clear_entries(db: &mut DbAny, datastore_id: DbId, now_ms: u64) -> anyhow::Result<usize> {
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

        t.exec_mut(QueryBuilder::remove().ids(&ids).query())?;
        Ok(entries.iter().filter(|e| e.is_live(now_ms)).count())
    })
}

pub fn list_ids(db: &impl DbAccess) -> anyhow::Result<Vec<DbId>> {
    let datastores: Vec<DataStore> = db
        .exec(
            QueryBuilder::select()
                .elements::<DataStore>()
                .search()
                .from("datastore")
                .where_()
                .distance(MEMBERS)
                .query(),
        )?
        .try_into()?;
    Ok(datastores
        .into_iter()
        .filter_map(|datastore| datastore.db_id.map(DbId::from))
        .collect())
}

/// Removes up to `limit` entries of one datastore that expired by `now_ms`,
/// without reading their values. Returns the number removed.
pub fn remove_expired_entries(
    db: &mut DbAny,
    datastore_id: DbId,
    now_ms: u64,
    limit: u64,
) -> anyhow::Result<usize> {
    db.transaction_mut(|t| -> anyhow::Result<usize> {
        let ids = t
            .exec(
                QueryBuilder::search()
                    .from(datastore_id)
                    .limit(limit)
                    .where_()
                    .key("expires_at_ms")
                    .value(Comparison::LessThanOrEqual(now_ms.into()))
                    .query(),
            )?
            .ids();
        if ids.is_empty() {
            return Ok(0);
        }
        t.exec_mut(QueryBuilder::remove().ids(&ids).query())?;
        Ok(ids.len())
    })
}

pub fn find_by_name(db: &DbAny, name: &str) -> anyhow::Result<Option<DataStore>> {
    let datastore: Vec<DataStore> = db
        .exec(
            QueryBuilder::select()
                .elements::<DataStore>()
                .search()
                .from("datastore")
                .where_()
                .distance(MEMBERS)
                .and()
                .key("name")
                .value(name)
                .end_where()
                .query(),
        )?
        .try_into()?;
    Ok(datastore.into_iter().next())
}

pub fn get_or_create(db: &mut DbAny, name: String) -> anyhow::Result<DataStore> {
    let datastore: Vec<DataStore> = db
        .exec(
            QueryBuilder::select()
                .elements::<DataStore>()
                .search()
                .from("datastore")
                .where_()
                .distance(MEMBERS)
                .and()
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
    use crate::test_db::new_test_db;

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

    fn write(key: &str, value: &str, expires_at_ms: Option<u64>) -> EntryWrite {
        EntryWrite {
            key: key.to_string(),
            value: value.to_string(),
            expires_at_ms,
        }
    }

    fn new_store(db: &mut DbAny, name: &str) -> anyhow::Result<DbId> {
        Ok(store_id(&get_or_create(db, name.to_string())?))
    }

    #[test]
    fn remove_entry_reports_only_live_entries() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let id = new_store(&mut db, "cache")?;
        upsert_entry(&mut db, id, write("k", "\"v\"", None))?;
        upsert_entry(&mut db, id, write("old", "1", Some(100)))?;

        assert!(remove_entry(&mut db, id, "k", 1000)?);
        assert!(get_entry(&db, id, "k", 1000)?.is_none());
        assert!(!remove_entry(&mut db, id, "k", 1000)?);
        assert!(!remove_entry(&mut db, id, "old", 1000)?);
        assert!(find_entry(&db, id, "old")?.is_none());
        Ok(())
    }

    #[test]
    fn clear_entries_removes_only_the_targeted_store_and_counts_live_entries() -> anyhow::Result<()>
    {
        let mut db = new_test_db()?;
        let id_a = new_store(&mut db, "a")?;
        let id_b = new_store(&mut db, "b")?;

        upsert_entry(&mut db, id_a, write("x", "1", None))?;
        upsert_entry(&mut db, id_a, write("y", "2", Some(5000)))?;
        upsert_entry(&mut db, id_a, write("expired", "3", Some(100)))?;
        upsert_entry(&mut db, id_b, write("z", "3", None))?;

        assert_eq!(clear_entries(&mut db, id_a, 1000)?, 2);
        assert!(find_entry(&db, id_a, "x")?.is_none());
        assert!(find_entry(&db, id_a, "expired")?.is_none());
        assert!(get_entry(&db, id_b, "z", 1000)?.is_some());
        Ok(())
    }

    #[test]
    fn get_entry_hides_entries_once_they_expire() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let id = new_store(&mut db, "cache")?;
        upsert_entry(&mut db, id, write("k", "1", Some(1000)))?;
        upsert_entry(&mut db, id, write("p", "2", None))?;

        let live = get_entry(&db, id, "k", 999)?.expect("live before expiry");
        assert_eq!(live.expires_at_ms, Some(1000));
        assert!(get_entry(&db, id, "k", 1000)?.is_none());
        assert!(find_entry(&db, id, "k")?.is_some());
        let persistent = get_entry(&db, id, "p", u64::MAX)?.expect("persistent entry");
        assert_eq!(persistent.expires_at_ms, None);
        Ok(())
    }

    #[test]
    fn overwriting_without_expiry_makes_an_entry_persistent() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let id = new_store(&mut db, "cache")?;
        upsert_entry(&mut db, id, write("k", "1", Some(1000)))?;
        upsert_entry(&mut db, id, write("k", "2", None))?;

        let entry = get_entry(&db, id, "k", 5000)?.expect("persistent after overwrite");
        assert_eq!(entry.value, "2");
        assert_eq!(entry.expires_at_ms, None);

        upsert_entry(&mut db, id, write("k", "3", Some(7000)))?;
        assert_eq!(
            get_entry(&db, id, "k", 5000)?.and_then(|e| e.expires_at_ms),
            Some(7000)
        );
        Ok(())
    }

    #[test]
    fn an_entry_stored_without_expiry_field_loads_as_persistent() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let id = new_store(&mut db, "cache")?;
        let result = db.exec_mut(
            QueryBuilder::insert()
                .nodes()
                .values([[
                    ("db_element_id", "DataStoreEntry").into(),
                    ("id", "old").into(),
                    ("key", "k").into(),
                    ("value", "\"v\"").into(),
                ]])
                .query(),
        )?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(id)
                .to(result.ids()[0])
                .query(),
        )?;

        let entry = get_entry(&db, id, "k", u64::MAX)?.expect("old entry loads");
        assert_eq!(entry.value, "\"v\"");
        assert_eq!(entry.expires_at_ms, None);
        assert_eq!(remove_expired_entries(&mut db, id, u64::MAX, 100)?, 0);
        Ok(())
    }

    #[test]
    fn upsert_many_writes_nothing_when_one_write_fails() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let id = new_store(&mut db, "cache")?;
        upsert_many(
            &mut db,
            id,
            vec![write("a", "1", None), write("b", "2", Some(5000))],
        )?;
        assert_eq!(
            get_entry(&db, id, "a", 0)?.map(|e| e.value),
            Some("1".into())
        );
        assert_eq!(
            get_entry(&db, id, "b", 0)?.and_then(|e| e.expires_at_ms),
            Some(5000)
        );

        let node_count = |db: &DbAny| -> anyhow::Result<u64> {
            Ok(db.exec(QueryBuilder::select().node_count().query())?.result)
        };
        let before = node_count(&db)?;
        let missing_store = DbId(i64::MAX);
        assert!(
            upsert_many(
                &mut db,
                missing_store,
                vec![write("c", "3", None), write("d", "4", None)]
            )
            .is_err()
        );
        assert_eq!(node_count(&db)?, before);
        upsert_entry(&mut db, id, write("c", "3", None))?;
        assert_eq!(node_count(&db)?, before + 1);
        Ok(())
    }

    #[test]
    fn remove_expired_entries_drops_only_expired_entries_up_to_the_limit() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let id = new_store(&mut db, "cache")?;
        let other = new_store(&mut db, "other")?;
        upsert_many(
            &mut db,
            id,
            vec![
                write("stale-1", "not json", Some(100)),
                write("stale-2", "{\"expires_at_ms\":9999}", Some(500)),
                write("stale-3", "3", Some(1000)),
                write("fresh", "{\"expires_at_ms\":1}", Some(9999)),
                write("persistent", "5", None),
            ],
        )?;
        upsert_entry(&mut db, other, write("stale", "6", Some(100)))?;

        assert_eq!(remove_expired_entries(&mut db, id, 1000, 2)?, 2);
        assert_eq!(remove_expired_entries(&mut db, id, 1000, 2)?, 1);
        assert_eq!(remove_expired_entries(&mut db, id, 1000, 2)?, 0);
        for key in ["stale-1", "stale-2", "stale-3"] {
            assert!(find_entry(&db, id, key)?.is_none());
        }
        assert!(find_entry(&db, id, "fresh")?.is_some());
        assert!(find_entry(&db, id, "persistent")?.is_some());
        assert!(find_entry(&db, other, "stale")?.is_some());
        assert_eq!(list_ids(&db)?.len(), 2);
        Ok(())
    }
}
