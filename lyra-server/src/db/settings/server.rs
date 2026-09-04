// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    DbElement,
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
pub(crate) struct ServerSettings {
    #[serde(skip)]
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
}

pub(crate) fn find_with<A: DbAccess>(db: &A) -> anyhow::Result<Option<ServerSettings>> {
    let mut nodes: Vec<ServerSettings> = db
        .exec(
            QueryBuilder::select()
                .elements::<ServerSettings>()
                .search()
                .from("server")
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    match nodes.len() {
        0 => Ok(None),
        1 => Ok(nodes.pop()),
        _ => Err(anyhow!("multiple server settings nodes found")),
    }
}

pub(crate) fn ensure(db: &mut DbAny) -> anyhow::Result<ServerSettings> {
    db.transaction_mut(|t| -> anyhow::Result<ServerSettings> {
        if let Some(found) = find_with(t)? {
            return Ok(found);
        }

        let id = nanoid!();
        let node = ServerSettings {
            db_id: None,
            id: id.clone(),
        };
        let db_id = t
            .exec_mut(QueryBuilder::insert().element(&node).query())?
            .ids()[0];
        t.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("server")
                .to(db_id)
                .query(),
        )?;

        Ok(ServerSettings {
            db_id: Some(db_id.into()),
            id,
        })
    })
}

#[cfg(test)]
mod tests {
    use agdb::DbId;

    use super::*;
    use crate::db::{
        server as server_info,
        settings::{
            SettingEntry,
            get_all_settings_with,
            plugins,
            upsert_setting_with,
        },
        test_db::new_test_db,
    };

    fn entry_keys(entries: &[SettingEntry]) -> Vec<&str> {
        let mut keys: Vec<&str> = entries.iter().map(|entry| entry.key.as_str()).collect();
        keys.sort_unstable();
        keys
    }

    fn parent_id(node: &ServerSettings) -> DbId {
        DbId::from(node.db_id.clone().expect("server settings db_id"))
    }

    #[test]
    fn ensure_is_idempotent() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let first = ensure(&mut db)?;
        let second = ensure(&mut db)?;

        assert_eq!(first.id, second.id);
        assert_eq!(parent_id(&first), parent_id(&second));
        assert_eq!(find_with(&db)?.map(|node| node.id), Some(first.id));
        Ok(())
    }

    #[test]
    fn ensure_coexists_with_server_info() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let info = server_info::ensure(&mut db)?;
        ensure(&mut db)?;

        assert_eq!(server_info::get(&db)?.map(|node| node.id), Some(info.id));
        assert!(find_with(&db)?.is_some());
        Ok(())
    }

    #[test]
    fn server_and_plugin_entries_do_not_leak() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let server_parent = parent_id(&ensure(&mut db)?);
        let plugin = db.transaction_mut(|t| plugins::get_or_create_with(t, "demo"))?;
        let plugin_parent = DbId::from(plugin.db_id.expect("plugin settings db_id"));

        upsert_setting_with(&mut db, server_parent, "shared".into(), "\"server\"".into())?;
        upsert_setting_with(&mut db, server_parent, "server_only".into(), "1".into())?;
        upsert_setting_with(&mut db, plugin_parent, "shared".into(), "\"plugin\"".into())?;
        upsert_setting_with(&mut db, plugin_parent, "plugin_only".into(), "2".into())?;

        let server_entries = get_all_settings_with(&db, server_parent)?;
        assert_eq!(entry_keys(&server_entries), vec!["server_only", "shared"]);
        assert!(
            server_entries
                .iter()
                .all(|entry| entry.value != "\"plugin\"")
        );

        let plugin_entries = get_all_settings_with(&db, plugin_parent)?;
        assert_eq!(entry_keys(&plugin_entries), vec!["plugin_only", "shared"]);
        assert!(
            plugin_entries
                .iter()
                .all(|entry| entry.value != "\"server\"")
        );

        db.transaction_mut(|t| plugins::remove_with(t, "demo"))?;
        assert_eq!(get_all_settings_with(&db, server_parent)?.len(), 2);
        Ok(())
    }
}
