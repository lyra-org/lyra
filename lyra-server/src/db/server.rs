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

#[derive(DbElement, Clone, Debug)]
pub(crate) struct ServerInfo {
    pub(crate) db_id: Option<DbId>,
    pub(crate) id: String,
}

pub(crate) fn get(db: &DbAny) -> anyhow::Result<Option<ServerInfo>> {
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
    if let Some(info) = get(db)? {
        return Ok(info);
    }

    let id = nanoid!();
    let info = ServerInfo {
        db_id: None,
        id: id.clone(),
    };

    let db_id = db.transaction_mut(|t| -> anyhow::Result<DbId> {
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
        Ok(db_id)
    })?;

    Ok(ServerInfo {
        db_id: Some(db_id),
        id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::new_test_db;

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
