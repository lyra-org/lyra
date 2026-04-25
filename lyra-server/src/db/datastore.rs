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
}
