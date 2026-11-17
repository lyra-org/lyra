// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Track sources stored their key twice, as `source_key` and as the indexed `identity`. Only
//! `identity` stays.

use agdb::{
    CountComparison,
    DbAnyTransactionMut,
    DbId,
    QueryBuilder,
};

pub(super) fn run(t: &mut DbAnyTransactionMut<'_>) -> anyhow::Result<()> {
    let sources = t.exec(
        QueryBuilder::select()
            .values(["source_key", "identity"])
            .search()
            .from("track_sources")
            .where_()
            .distance(CountComparison::Equal(2))
            .and()
            .key("db_element_id")
            .value("TrackSource")
            .query(),
    )?;
    let mut ids = Vec::<DbId>::with_capacity(sources.elements.len());
    for source in &sources.elements {
        let value = |key: &str| {
            source
                .values
                .iter()
                .find(|kv| kv.key.string().is_ok_and(|stored| stored == key))
                .map(|kv| &kv.value)
        };
        anyhow::ensure!(
            value("source_key") == value("identity"),
            "track source {} has a source key other than its identity",
            source.id.0
        );
        ids.push(source.id);
    }
    if !ids.is_empty() {
        t.exec_mut(
            QueryBuilder::remove()
                .values(["source_key"])
                .ids(ids)
                .query(),
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use agdb::{
        DbAny,
        DbId,
        QueryBuilder,
    };

    use crate::{
        test_db::{
            new_test_db,
            stored_keys,
        },
        track_sources,
    };

    fn before() -> anyhow::Result<DbAny> {
        let mut db = new_test_db()?;
        db.exec_mut(
            QueryBuilder::insert()
                .nodes()
                .aliases("track_sources")
                .query(),
        )?;
        Ok(db)
    }

    fn seed(db: &mut DbAny, source_key: &str, identity: &str) -> anyhow::Result<DbId> {
        let source = db
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values([[
                        ("db_element_id", "TrackSource").into(),
                        ("id", format!("source-{identity}")).into(),
                        ("source_kind", "embedded").into(),
                        ("source_key", source_key).into(),
                        ("identity", identity).into(),
                        ("is_primary", true).into(),
                    ]])
                    .query(),
            )?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("track_sources")
                .to(source)
                .query(),
        )?;
        Ok(source)
    }

    #[test]
    fn a_track_source_keeps_only_its_identity() -> anyhow::Result<()> {
        let mut db = before()?;
        let source = seed(&mut db, "entry:1:embedded", "entry:1:embedded")?;

        db.transaction_mut(super::run)?;

        assert!(!stored_keys(&db, source)?.contains("source_key"));
        let stored = track_sources::get_by_id(&db, source)?.expect("track source");
        assert_eq!(stored.identity, "entry:1:embedded");
        Ok(())
    }

    #[test]
    fn a_source_key_other_than_the_identity_fails() -> anyhow::Result<()> {
        let mut db = before()?;
        let source = seed(&mut db, "entry:1:embedded", "entry:2:embedded")?;

        assert!(db.transaction_mut(super::run).is_err());
        assert!(stored_keys(&db, source)?.contains("source_key"));
        Ok(())
    }
}
