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

use crate::db::{
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

pub(crate) fn find_with<A: DbAccess>(
    db: &A,
    plugin_id: &str,
) -> anyhow::Result<Option<PluginSettings>> {
    let mut existing: Vec<PluginSettings> = db
        .exec(
            QueryBuilder::select()
                .elements::<PluginSettings>()
                .search()
                .from("plugin_settings")
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

fn insert_with<A: DbAccess>(db: &mut A, plugin_id: &str) -> anyhow::Result<PluginSettings> {
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
            .from("plugin_settings")
            .to(result_id)
            .query(),
    )?;

    Ok(PluginSettings {
        db_id: Some(result_id.into()),
        id,
        plugin_id: plugin_id.to_string(),
    })
}

pub(crate) fn get_or_create_with<A: DbAccess>(
    db: &mut A,
    plugin_id: &str,
) -> anyhow::Result<PluginSettings> {
    if let Some(found) = find_with(db, plugin_id)? {
        return Ok(found);
    }

    insert_with(db, plugin_id)
}

pub(crate) fn remove_with<A: DbAccess>(db: &mut A, plugin_id: &str) -> anyhow::Result<()> {
    let Some(plugin) = find_with(db, plugin_id)? else {
        return Ok(());
    };

    let plugin_db_id = plugin
        .db_id
        .ok_or_else(|| anyhow!("plugin settings node missing db_id: {plugin_id}"))?;
    let plugin_db_id = DbId::from(plugin_db_id);
    let entry_ids: Vec<DbId> = super::find_setting_entries_with(db, plugin_db_id, None)?
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

pub(crate) fn find_user_with<A: DbAccess>(
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

fn insert_user_with<A: DbAccess>(
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
            .from("user_plugin_settings")
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

pub(crate) fn get_or_create_user_with<A: DbAccess>(
    db: &mut A,
    user_db_id: DbId,
    plugin_id: &str,
) -> anyhow::Result<UserPluginSettings> {
    if let Some(found) = find_user_with(db, user_db_id, plugin_id)? {
        return Ok(found);
    }

    insert_user_with(db, user_db_id, plugin_id)
}

pub(crate) fn remove_user_with<A: DbAccess>(
    db: &mut A,
    user_db_id: DbId,
    plugin_id: &str,
) -> anyhow::Result<()> {
    let Some(node) = find_user_with(db, user_db_id, plugin_id)? else {
        return Ok(());
    };

    let node_db_id = node.db_id.ok_or_else(|| {
        anyhow!(
            "user plugin settings node missing db_id: user={} plugin={plugin_id}",
            user_db_id.0
        )
    })?;
    let node_db_id = DbId::from(node_db_id);
    let entry_ids: Vec<DbId> = super::find_setting_entries_with(db, node_db_id, None)?
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

pub(crate) fn remove_all_for_user<A: DbAccess>(db: &mut A, user_db_id: DbId) -> anyhow::Result<()> {
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

        let entry_ids: Vec<DbId> = super::find_setting_entries_with(db, node_id, None)?
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
    use crate::db::{
        settings::upsert_setting_with,
        test_db::new_test_db,
    };
    use agdb::DbAny;

    fn get_or_create(db: &mut DbAny, plugin_id: &str) -> anyhow::Result<PluginSettings> {
        db.transaction_mut(|t| get_or_create_with(t, plugin_id))
    }

    fn upsert(db: &mut DbAny, parent_id: DbId, key: String, value: String) -> anyhow::Result<DbId> {
        db.transaction_mut(|t| upsert_setting_with(t, parent_id, key, value))
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
    fn remove_plugin_settings_deletes_child_entries() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let plugin = get_or_create(&mut db, "test-plugin")?;
        let plugin_db_id: DbId = plugin.db_id.unwrap().into();
        let entry_db_id = upsert(&mut db, plugin_db_id, "key_a".into(), "\"a\"".into())?;

        db.transaction_mut(|t| remove_with(t, "test-plugin"))?;

        assert!(find_with(&db, "test-plugin")?.is_none());
        assert!(
            db.exec(QueryBuilder::select().ids(entry_db_id).query())
                .is_err()
        );

        Ok(())
    }
}
