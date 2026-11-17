// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The `track_sources` root linked every track source, but nothing reads the whole list. The
//! root goes with its edges, in one removal, since removing its edges one at a time walks its
//! list for each.

use agdb::{
    DbAnyTransactionMut,
    QueryBuilder,
};

pub(super) fn run(t: &mut DbAnyTransactionMut<'_>) -> anyhow::Result<()> {
    t.exec_mut(QueryBuilder::remove().ids("track_sources").query())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use agdb::QueryBuilder;

    use crate::test_db::new_test_db;

    #[test]
    fn the_root_goes_and_its_track_sources_stay() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let root = db
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .aliases("track_sources")
                    .query(),
            )?
            .ids()[0];
        let source = db
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values([[("db_element_id", "TrackSource").into()]])
                    .query(),
            )?
            .ids()[0];
        let edge = db
            .exec_mut(QueryBuilder::insert().edges().from(root).to(source).query())?
            .ids()[0];

        db.transaction_mut(super::run)?;

        assert!(
            db.exec(QueryBuilder::select().ids("track_sources").query())
                .is_err()
        );
        assert!(db.exec(QueryBuilder::select().ids(edge).query()).is_err());
        assert!(db.exec(QueryBuilder::select().ids(source).query()).is_ok());
        Ok(())
    }
}
