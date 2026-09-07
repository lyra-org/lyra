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

use super::DbAccess;

#[derive(DbElement, Clone, Debug)]
pub(crate) struct ServerInfo {
    pub(crate) db_id: Option<DbId>,
    pub(crate) id: String,
}

pub(crate) fn get<A: DbAccess>(db: &A) -> anyhow::Result<Option<ServerInfo>> {
    let mut infos: Vec<ServerInfo> = db
        .exec(
            QueryBuilder::select()
                .elements::<ServerInfo>()
                .search()
                .from("server")
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;
    Ok(infos.pop())
}

pub(crate) fn ensure(db: &mut DbAny) -> anyhow::Result<ServerInfo> {
    db.transaction_mut(|t| -> anyhow::Result<ServerInfo> {
        if let Some(info) = get(t)? {
            return Ok(info);
        }

        let id = nanoid!();
        let info = ServerInfo {
            db_id: None,
            id: id.clone(),
        };
        let db_id = t
            .exec_mut(QueryBuilder::insert().element(&info).query())?
            .ids()[0];
        t.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("server")
                .to(db_id)
                .query(),
        )?;

        Ok(ServerInfo {
            db_id: Some(db_id),
            id,
        })
    })
}

pub(crate) fn plugin_selection_skipped(db: &impl DbAccess) -> anyhow::Result<bool> {
    let result = db.exec(QueryBuilder::select().ids("server").query())?;
    match result.elements.first().and_then(|element| {
        element
            .values
            .iter()
            .find(|value| value.key == "plugin_selection_skipped".into())
    }) {
        Some(value) => Ok(bool::try_from(value.value.clone())?),
        None => Ok(false),
    }
}

pub(crate) fn set_plugin_selection_skipped(
    db: &mut impl DbAccess,
    skipped: bool,
) -> anyhow::Result<()> {
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform([("plugin_selection_skipped", skipped).into()])
            .ids("server")
            .query(),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::new_test_db;

    #[test]
    fn plugin_selection_skip_survives_reinitialization_and_can_be_cleared() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        assert!(!plugin_selection_skipped(&db)?);
        set_plugin_selection_skipped(&mut db, true)?;
        super::super::bootstrap::initialize(&mut db)?;
        ensure(&mut db)?;
        assert!(plugin_selection_skipped(&db)?);
        set_plugin_selection_skipped(&mut db, false)?;
        assert!(!plugin_selection_skipped(&db)?);
        Ok(())
    }

    #[test]
    fn ensure_creates_server_info() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let info = ensure(&mut db)?;
        assert!(!info.id.is_empty());
        Ok(())
    }

    #[test]
    fn ensure_is_idempotent() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let first = ensure(&mut db)?;
        let second = ensure(&mut db)?;
        assert_eq!(first.id, second.id);
        Ok(())
    }
}
