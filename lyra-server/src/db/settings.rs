// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    CountComparison,
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

#[derive(DbElement, Serialize, Clone, Debug)]
pub(crate) struct PluginSettings {
    #[serde(skip)]
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) plugin_id: String,
}

#[derive(DbElement, Serialize, Clone, Debug)]
pub(crate) struct UserPluginSettings {
    #[serde(skip)]
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) plugin_id: String,
}

#[derive(DbElement, Serialize, Clone, Debug)]
pub(crate) struct SettingEntry {
    #[serde(skip)]
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) key: String,
    pub(crate) value: String,
}

pub(crate) fn find_plugin_settings_with<A: DbAccess>(
    db: &A,
    plugin_id: &str,
) -> anyhow::Result<Option<PluginSettings>> {
    let mut existing: Vec<PluginSettings> = db
        .exec(
            QueryBuilder::select()
                .elements::<PluginSettings>()
                .search()
                .from("settings")
                .where_()
                .neighbor()
                .and()
                .key("plugin_id")
                .value(plugin_id)
                .end_where()
                .query(),
        )?
        .try_into()?;

    match existing.len() {
        0 => Ok(None),
        1 => Ok(existing.pop()),
        _ => Err(anyhow!(
            "multiple plugin settings nodes found for plugin_id '{plugin_id}'"
        )),
    }
}

fn insert_plugin_settings_with<A: DbAccess>(
    db: &mut A,
    plugin_id: &str,
) -> anyhow::Result<PluginSettings> {
    let id = nanoid!();
    let node = PluginSettings {
        db_id: None,
        id: id.clone(),
        plugin_id: plugin_id.to_string(),
    };
    let result = db.exec_mut(QueryBuilder::insert().element(&node).query())?;
    let result_id = result.ids()[0];
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from("settings")
            .to(result_id)
            .query(),
    )?;

    Ok(PluginSettings {
        db_id: Some(result_id.into()),
        id,
        plugin_id: plugin_id.to_string(),
    })
}

pub(crate) fn get_or_create_plugin_settings_with<A: DbAccess>(
    db: &mut A,
    plugin_id: &str,
) -> anyhow::Result<PluginSettings> {
    if let Some(found) = find_plugin_settings_with(db, plugin_id)? {
        return Ok(found);
    }

    insert_plugin_settings_with(db, plugin_id)
}

pub(crate) fn remove_plugin_settings_with<A: DbAccess>(
    db: &mut A,
    plugin_id: &str,
) -> anyhow::Result<()> {
    let Some(plugin) = find_plugin_settings_with(db, plugin_id)? else {
        return Ok(());
    };

    let plugin_db_id = plugin
        .db_id
        .ok_or_else(|| anyhow!("plugin settings node missing db_id: {plugin_id}"))?;
    let plugin_db_id = DbId::from(plugin_db_id);
    let entry_ids: Vec<DbId> = find_setting_entries_with(db, plugin_db_id, None)?
        .into_iter()
        .map(|entry| {
            entry.db_id.map(DbId::from).ok_or_else(|| {
                anyhow!("setting entry missing db_id while removing plugin settings: {plugin_id}")
            })
        })
        .collect::<anyhow::Result<_>>()?;

    if !entry_ids.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(&entry_ids).query())?;
    }

    db.exec_mut(QueryBuilder::remove().ids(plugin_db_id).query())?;
    Ok(())
}

fn find_setting_entries_with<A: DbAccess>(
    db: &A,
    plugin_settings_id: DbId,
    key: Option<&str>,
) -> anyhow::Result<Vec<SettingEntry>> {
    let entries: Vec<SettingEntry> = match key {
        Some(key) => db
            .exec(
                QueryBuilder::select()
                    .elements::<SettingEntry>()
                    .search()
                    .from(plugin_settings_id)
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
                    .from(plugin_settings_id)
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
    plugin_settings_id: DbId,
) -> anyhow::Result<Vec<SettingEntry>> {
    find_setting_entries_with(db, plugin_settings_id, None)
}

pub(crate) fn upsert_setting_with<A: DbAccess>(
    db: &mut A,
    plugin_settings_id: DbId,
    key: String,
    value: String,
) -> anyhow::Result<DbId> {
    let existing_entries = find_setting_entries_with(db, plugin_settings_id, Some(key.as_str()))?;
    let existing_entry = match existing_entries.as_slice() {
        [] => None,
        [existing] => Some(existing),
        _ => {
            return Err(anyhow!(
                "multiple setting entries found for plugin_settings_id={} key='{}'",
                plugin_settings_id.0,
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
                .from(plugin_settings_id)
                .to(entry_db_id)
                .query(),
        )?;
    }

    Ok(entry_db_id)
}

pub(crate) fn remove_setting_with<A: DbAccess>(
    db: &mut A,
    plugin_settings_id: DbId,
    key: &str,
) -> anyhow::Result<()> {
    let entries = find_setting_entries_with(db, plugin_settings_id, Some(key))?;
    let entry_db_id = match entries.as_slice() {
        [] => None,
        [entry] => Some(entry.db_id.clone().ok_or_else(|| {
            anyhow!(
                "setting entry missing db_id for plugin_settings_id={} key='{}'",
                plugin_settings_id.0,
                key
            )
        })?),
        _ => {
            return Err(anyhow!(
                "multiple setting entries found for plugin_settings_id={} key='{}'",
                plugin_settings_id.0,
                key
            ));
        }
    };

    if let Some(entry_db_id) = entry_db_id {
        db.exec_mut(QueryBuilder::remove().ids(DbId::from(entry_db_id)).query())?;
    }

    Ok(())
}

pub(crate) fn find_user_plugin_settings_with<A: DbAccess>(
    db: &A,
    user_db_id: DbId,
    plugin_id: &str,
) -> anyhow::Result<Option<UserPluginSettings>> {
    let mut results: Vec<UserPluginSettings> = db
        .exec(
            QueryBuilder::select()
                .elements::<UserPluginSettings>()
                .search()
                .to(user_db_id)
                .where_()
                .distance(CountComparison::Equal(2))
                .and()
                .node()
                .and()
                .key("db_element_id")
                .value("UserPluginSettings")
                .and()
                .key("plugin_id")
                .value(plugin_id)
                .end_where()
                .query(),
        )?
        .try_into()?;

    match results.len() {
        0 => Ok(None),
        1 => Ok(results.pop()),
        _ => Err(anyhow!(
            "multiple user plugin settings nodes found for user_db_id={} plugin_id='{plugin_id}'",
            user_db_id.0
        )),
    }
}

fn insert_user_plugin_settings_with<A: DbAccess>(
    db: &mut A,
    user_db_id: DbId,
    plugin_id: &str,
) -> anyhow::Result<UserPluginSettings> {
    let id = nanoid!();
    let node = UserPluginSettings {
        db_id: None,
        id: id.clone(),
        plugin_id: plugin_id.to_string(),
    };
    let result = db.exec_mut(QueryBuilder::insert().element(&node).query())?;
    let result_id = result.ids()[0];

    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from("user_settings")
            .to(result_id)
            .query(),
    )?;
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(result_id)
            .to(user_db_id)
            .query(),
    )?;

    Ok(UserPluginSettings {
        db_id: Some(result_id.into()),
        id,
        plugin_id: plugin_id.to_string(),
    })
}

pub(crate) fn get_or_create_user_plugin_settings_with<A: DbAccess>(
    db: &mut A,
    user_db_id: DbId,
    plugin_id: &str,
) -> anyhow::Result<UserPluginSettings> {
    if let Some(found) = find_user_plugin_settings_with(db, user_db_id, plugin_id)? {
        return Ok(found);
    }

    insert_user_plugin_settings_with(db, user_db_id, plugin_id)
}

pub(crate) fn remove_user_plugin_settings_with<A: DbAccess>(
    db: &mut A,
    user_db_id: DbId,
    plugin_id: &str,
) -> anyhow::Result<()> {
    let Some(node) = find_user_plugin_settings_with(db, user_db_id, plugin_id)? else {
        return Ok(());
    };

    let node_db_id = node.db_id.ok_or_else(|| {
        anyhow!(
            "user plugin settings node missing db_id: user={} plugin={plugin_id}",
            user_db_id.0
        )
    })?;
    let node_db_id = DbId::from(node_db_id);
    let entry_ids: Vec<DbId> = find_setting_entries_with(db, node_db_id, None)?
        .into_iter()
        .map(|entry| {
            entry.db_id.map(DbId::from).ok_or_else(|| {
                anyhow!(
                    "setting entry missing db_id while removing user plugin settings: user={} plugin={plugin_id}",
                    user_db_id.0
                )
            })
        })
        .collect::<anyhow::Result<_>>()?;

    if !entry_ids.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(&entry_ids).query())?;
    }

    db.exec_mut(QueryBuilder::remove().ids(node_db_id).query())?;
    Ok(())
}

pub(crate) fn remove_all_user_plugin_settings_for_user<A: DbAccess>(
    db: &mut A,
    user_db_id: DbId,
) -> anyhow::Result<()> {
    let nodes: Vec<UserPluginSettings> = db
        .exec(
            QueryBuilder::select()
                .elements::<UserPluginSettings>()
                .search()
                .to(user_db_id)
                .where_()
                .distance(CountComparison::Equal(2))
                .and()
                .node()
                .and()
                .key("db_element_id")
                .value("UserPluginSettings")
                .end_where()
                .query(),
        )?
        .try_into()?;

    for node in nodes {
        let Some(node_id) = node.db_id.map(DbId::from) else {
            continue;
        };

        let entry_ids: Vec<DbId> = find_setting_entries_with(db, node_id, None)?
            .into_iter()
            .filter_map(|entry| entry.db_id.map(DbId::from))
            .collect();

        if !entry_ids.is_empty() {
            db.exec_mut(QueryBuilder::remove().ids(&entry_ids).query())?;
        }

        db.exec_mut(QueryBuilder::remove().ids(node_id).query())?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::new_test_db;
    use agdb::DbAny;

    fn get_or_create(db: &mut DbAny, plugin_id: &str) -> anyhow::Result<PluginSettings> {
        db.transaction_mut(|t| get_or_create_plugin_settings_with(t, plugin_id))
    }

    fn upsert(
        db: &mut DbAny,
        plugin_settings_id: DbId,
        key: String,
        value: String,
    ) -> anyhow::Result<DbId> {
        db.transaction_mut(|t| upsert_setting_with(t, plugin_settings_id, key, value))
    }

    fn get_single(
        db: &DbAny,
        plugin_settings_id: DbId,
        key: &str,
    ) -> anyhow::Result<Option<SettingEntry>> {
        let mut entries = find_setting_entries_with(db, plugin_settings_id, Some(key))?;
        match entries.len() {
            0 => Ok(None),
            1 => Ok(entries.pop()),
            _ => Err(anyhow!(
                "multiple setting entries found for plugin_settings_id={} key='{}'",
                plugin_settings_id.0,
                key
            )),
        }
    }

    fn remove(db: &mut DbAny, plugin_settings_id: DbId, key: &str) -> anyhow::Result<()> {
        db.transaction_mut(|t| remove_setting_with(t, plugin_settings_id, key))
    }

    #[test]
    fn get_or_create_is_idempotent() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let created = get_or_create(&mut db, "musicbrainz")?;
        let fetched = get_or_create(&mut db, "musicbrainz")?;

        assert_eq!(created.db_id.map(DbId::from), fetched.db_id.map(DbId::from));
        assert_eq!(created.id, fetched.id);
        assert_eq!(created.plugin_id, "musicbrainz");
        Ok(())
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
    fn remove_plugin_settings_deletes_child_entries() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let plugin = get_or_create(&mut db, "test-plugin")?;
        let plugin_db_id: DbId = plugin.db_id.unwrap().into();
        let entry_db_id = upsert(&mut db, plugin_db_id, "key_a".into(), "\"a\"".into())?;

        db.transaction_mut(|t| remove_plugin_settings_with(t, "test-plugin"))?;

        assert!(find_plugin_settings_with(&db, "test-plugin")?.is_none());
        assert!(
            db.exec(QueryBuilder::select().ids(entry_db_id).query())
                .is_err()
        );

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
