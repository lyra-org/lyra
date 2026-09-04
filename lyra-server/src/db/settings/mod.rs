// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbElement,
    DbId,
    QueryBuilder,
};
use anyhow::anyhow;
use nanoid::nanoid;
use serde::Serialize;

use super::{
    DbAccess,
    NodeId,
};

pub(crate) mod plugins;

#[derive(DbElement, Serialize, Clone, Debug)]
pub(crate) struct SettingEntry {
    #[serde(skip)]
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) key: String,
    pub(crate) value: String,
}

fn find_setting_entries_with<A: DbAccess>(
    db: &A,
    parent_id: DbId,
    key: Option<&str>,
) -> anyhow::Result<Vec<SettingEntry>> {
    let entries: Vec<SettingEntry> = match key {
        Some(key) => db
            .exec(
                QueryBuilder::select()
                    .elements::<SettingEntry>()
                    .search()
                    .from(parent_id)
                    .where_()
                    .neighbor()
                    .and()
                    .key("key")
                    .value(key)
                    .end_where()
                    .query(),
            )?
            .try_into()?,
        None => db
            .exec(
                QueryBuilder::select()
                    .elements::<SettingEntry>()
                    .search()
                    .from(parent_id)
                    .where_()
                    .neighbor()
                    .end_where()
                    .query(),
            )?
            .try_into()?,
    };

    Ok(entries)
}

pub(crate) fn get_all_settings_with<A: DbAccess>(
    db: &A,
    parent_id: DbId,
) -> anyhow::Result<Vec<SettingEntry>> {
    find_setting_entries_with(db, parent_id, None)
}

pub(crate) fn upsert_setting_with<A: DbAccess>(
    db: &mut A,
    parent_id: DbId,
    key: String,
    value: String,
) -> anyhow::Result<DbId> {
    let existing_entries = find_setting_entries_with(db, parent_id, Some(key.as_str()))?;
    let existing_entry = match existing_entries.as_slice() {
        [] => None,
        [existing] => Some(existing),
        _ => {
            return Err(anyhow!(
                "multiple setting entries found for parent_id={} key='{}'",
                parent_id.0,
                key
            ));
        }
    };
    let existing_db_id = existing_entry.and_then(|entry| entry.db_id.clone());
    let is_new_entry = existing_db_id.is_none();

    let entry = SettingEntry {
        db_id: existing_db_id.clone(),
        id: existing_entry
            .map(|entry| entry.id.clone())
            .unwrap_or_else(|| nanoid!()),
        key,
        value,
    };
    let result = db.exec_mut(QueryBuilder::insert().element(&entry).query())?;
    let entry_db_id = existing_db_id
        .map(Into::<DbId>::into)
        .or_else(|| result.ids().first().copied())
        .ok_or_else(|| anyhow!("settings upsert returned no id"))?;

    if is_new_entry {
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(parent_id)
                .to(entry_db_id)
                .query(),
        )?;
    }

    Ok(entry_db_id)
}

pub(crate) fn remove_setting_with<A: DbAccess>(
    db: &mut A,
    parent_id: DbId,
    key: &str,
) -> anyhow::Result<()> {
    let entries = find_setting_entries_with(db, parent_id, Some(key))?;
    let entry_db_id = match entries.as_slice() {
        [] => None,
        [entry] => Some(entry.db_id.clone().ok_or_else(|| {
            anyhow!(
                "setting entry missing db_id for parent_id={} key='{}'",
                parent_id.0,
                key
            )
        })?),
        _ => {
            return Err(anyhow!(
                "multiple setting entries found for parent_id={} key='{}'",
                parent_id.0,
                key
            ));
        }
    };

    if let Some(entry_db_id) = entry_db_id {
        db.exec_mut(QueryBuilder::remove().ids(DbId::from(entry_db_id)).query())?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::new_test_db;
    use agdb::DbAny;

    fn get_or_create(db: &mut DbAny, plugin_id: &str) -> anyhow::Result<plugins::PluginSettings> {
        db.transaction_mut(|t| plugins::get_or_create_with(t, plugin_id))
    }

    fn upsert(db: &mut DbAny, parent_id: DbId, key: String, value: String) -> anyhow::Result<DbId> {
        db.transaction_mut(|t| upsert_setting_with(t, parent_id, key, value))
    }

    fn get_single(db: &DbAny, parent_id: DbId, key: &str) -> anyhow::Result<Option<SettingEntry>> {
        let mut entries = find_setting_entries_with(db, parent_id, Some(key))?;
        match entries.len() {
            0 => Ok(None),
            1 => Ok(entries.pop()),
            _ => Err(anyhow!(
                "multiple setting entries found for parent_id={} key='{}'",
                parent_id.0,
                key
            )),
        }
    }

    fn remove(db: &mut DbAny, parent_id: DbId, key: &str) -> anyhow::Result<()> {
        db.transaction_mut(|t| remove_setting_with(t, parent_id, key))
    }

    #[test]
    fn upsert_and_get_round_trips() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let plugin = get_or_create(&mut db, "listenbrainz")?;
        let plugin_db_id: DbId = plugin.db_id.unwrap().into();

        upsert(&mut db, plugin_db_id, "token".into(), "\"abc123\"".into())?;

        let entry = get_single(&db, plugin_db_id, "token")?;
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().value, "\"abc123\"");

        upsert(&mut db, plugin_db_id, "token".into(), "\"updated\"".into())?;

        let entry = get_single(&db, plugin_db_id, "token")?;
        assert_eq!(entry.unwrap().value, "\"updated\"");

        Ok(())
    }

    #[test]
    fn upsert_preserves_existing_entry_id() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let plugin = get_or_create(&mut db, "listenbrainz")?;
        let plugin_db_id: DbId = plugin.db_id.unwrap().into();

        upsert(&mut db, plugin_db_id, "token".into(), "\"abc123\"".into())?;
        let original = get_single(&db, plugin_db_id, "token")?.expect("setting should exist");

        upsert(&mut db, plugin_db_id, "token".into(), "\"updated\"".into())?;
        let updated = get_single(&db, plugin_db_id, "token")?.expect("setting should exist");

        assert_eq!(original.id, updated.id);
        assert_eq!(
            original.db_id.map(DbId::from),
            updated.db_id.map(DbId::from)
        );

        Ok(())
    }

    #[test]
    fn get_all_returns_all_entries() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let plugin = get_or_create(&mut db, "test-plugin")?;
        let plugin_db_id: DbId = plugin.db_id.unwrap().into();

        upsert(&mut db, plugin_db_id, "key_a".into(), "\"a\"".into())?;
        upsert(&mut db, plugin_db_id, "key_b".into(), "\"b\"".into())?;
        upsert(&mut db, plugin_db_id, "key_c".into(), "\"c\"".into())?;

        let all = get_all_settings_with(&db, plugin_db_id)?;
        assert_eq!(all.len(), 3);

        let keys: Vec<&str> = all.iter().map(|e| e.key.as_str()).collect();
        assert!(keys.contains(&"key_a"));
        assert!(keys.contains(&"key_b"));
        assert!(keys.contains(&"key_c"));

        Ok(())
    }

    #[test]
    fn get_setting_returns_none_for_missing_key() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let plugin = get_or_create(&mut db, "test-plugin")?;
        let plugin_db_id: DbId = plugin.db_id.unwrap().into();

        let entry = get_single(&db, plugin_db_id, "nonexistent")?;
        assert!(entry.is_none());

        Ok(())
    }

    #[test]
    fn remove_setting_deletes_existing_entries() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let plugin = get_or_create(&mut db, "test-plugin")?;
        let plugin_db_id: DbId = plugin.db_id.unwrap().into();

        upsert(&mut db, plugin_db_id, "key_a".into(), "\"a\"".into())?;
        remove(&mut db, plugin_db_id, "key_a")?;

        let entry = get_single(&db, plugin_db_id, "key_a")?;
        assert!(entry.is_none());

        Ok(())
    }

    #[test]
    fn remove_setting_rejects_duplicate_entries() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let plugin = get_or_create(&mut db, "test-plugin")?;
        let plugin_db_id: DbId = plugin.db_id.unwrap().into();

        let duplicate_a = SettingEntry {
            db_id: None,
            id: "duplicate-a".to_string(),
            key: "key_a".to_string(),
            value: "\"a\"".to_string(),
        };
        let duplicate_a_db_id = db
            .exec_mut(QueryBuilder::insert().element(&duplicate_a).query())?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(plugin_db_id)
                .to(duplicate_a_db_id)
                .query(),
        )?;

        let duplicate_b = SettingEntry {
            db_id: None,
            id: "duplicate-b".to_string(),
            key: "key_a".to_string(),
            value: "\"b\"".to_string(),
        };
        let duplicate_b_db_id = db
            .exec_mut(QueryBuilder::insert().element(&duplicate_b).query())?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(plugin_db_id)
                .to(duplicate_b_db_id)
                .query(),
        )?;

        let error = remove(&mut db, plugin_db_id, "key_a").unwrap_err();
        assert!(error.to_string().contains("multiple setting entries found"));

        Ok(())
    }
}
