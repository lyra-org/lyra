// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::DbAccess;
use agdb::{
    CountComparison,
    DbElement,
    DbId,
    QueryBuilder,
};
use serde::Serialize;
use std::collections::HashMap;

#[derive(DbElement, Serialize, Clone, Debug)]
pub(crate) struct Cover {
    pub(crate) db_id: Option<DbId>,
    pub(crate) id: String,
    pub(crate) path: String,
    pub(crate) mime_type: String,
    pub(crate) hash: String,
    pub(crate) blurhash: Option<String>,
}

pub(crate) fn get(db: &impl DbAccess, release_db_id: DbId) -> anyhow::Result<Option<Cover>> {
    let mut covers: Vec<Cover> = db
        .exec(
            QueryBuilder::select()
                .elements::<Cover>()
                .search()
                .from(release_db_id)
                .where_()
                .distance(CountComparison::Equal(2))
                .query(),
        )?
        .try_into()?;

    Ok(covers.pop())
}

pub(crate) fn get_many(
    db: &impl super::DbAccess,
    owner_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Cover>> {
    let unique_owner_db_ids = super::dedup_positive_ids(owner_db_ids);

    if unique_owner_db_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let mut covers = HashMap::new();
    for owner_db_id in unique_owner_db_ids {
        if let Some(cover) = get(db, owner_db_id)? {
            covers.insert(owner_db_id, cover);
        }
    }
    Ok(covers)
}

pub(crate) fn remove(db: &mut impl DbAccess, release_db_id: DbId) -> anyhow::Result<bool> {
    let Some(cover) = get(db, release_db_id)? else {
        return Ok(false);
    };
    let cover_id = cover
        .db_id
        .ok_or_else(|| anyhow::anyhow!("cover missing db_id"))?;

    // Remove only the edge from this release to the cover. The cover node
    // may be shared with another release after dedup, so only delete the
    // node itself if no other owners remain.
    super::graph::remove_edges_between(db, release_db_id, cover_id)?;

    let remaining_owners = db
        .exec(
            QueryBuilder::search()
                .to(cover_id)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .and()
                .key("owned")
                .value(1)
                .query(),
        )?
        .elements
        .len();

    if remaining_owners == 0 {
        db.exec_mut(QueryBuilder::remove().ids(cover_id).query())?;
    }

    Ok(true)
}

/// Insert or update the Cover node for a release, creating the owned edge if new.
/// Returns the persisted Cover with its `db_id` populated.
pub(crate) fn upsert(
    db: &mut impl DbAccess,
    release_db_id: DbId,
    mut cover: Cover,
) -> anyhow::Result<Cover> {
    if let Some(existing) = get(db, release_db_id)? {
        let existing_id = existing
            .db_id
            .ok_or_else(|| anyhow::anyhow!("existing cover missing db_id"))?;
        cover.db_id = Some(existing_id);
        db.exec_mut(QueryBuilder::insert().element(&cover).query())?;
        Ok(cover)
    } else {
        let qr = db.exec_mut(QueryBuilder::insert().element(&cover).query())?;
        let cover_id = qr
            .ids()
            .first()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("cover insert returned no id"))?;
        cover.db_id = Some(cover_id);
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(release_db_id)
                .to(cover_id)
                .values_uniform([("owned", 1).into()])
                .query(),
        )?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("covers")
                .to(cover_id)
                .query(),
        )?;
        Ok(cover)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::TestDb;
    use agdb::DbAny;
    use anyhow::anyhow;
    use nanoid::nanoid;

    fn new_test_db() -> anyhow::Result<DbAny> {
        Ok(TestDb::with_root_aliases(&["releases", "covers"])?.into_inner())
    }

    fn insert_release(db: &mut DbAny) -> anyhow::Result<DbId> {
        let qr = db.exec_mut(QueryBuilder::insert().nodes().count(1).query())?;
        qr.ids()
            .first()
            .copied()
            .ok_or_else(|| anyhow!("release insert returned no id"))
    }

    fn direct_edge_exists(db: &DbAny, from: DbId, to: DbId) -> anyhow::Result<bool> {
        let qr = db.exec(
            QueryBuilder::search()
                .from(from)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .query(),
        )?;

        Ok(qr
            .elements
            .into_iter()
            .any(|edge| edge.from == Some(from) && edge.to == Some(to)))
    }

    fn outgoing_edge_count(db: &DbAny, from: DbId) -> anyhow::Result<u64> {
        let qr = db.exec(
            QueryBuilder::search()
                .from(from)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .query(),
        )?;

        Ok(qr
            .elements
            .into_iter()
            .filter(|edge| edge.from == Some(from))
            .count() as u64)
    }

    #[test]
    fn upsert_new_cover_links_album_and_collection() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release(&mut db)?;
        let cover = Cover {
            db_id: None,
            id: nanoid!(),
            path: "/music/test/cover.jpg".to_string(),
            mime_type: "image/jpeg".to_string(),
            hash: "a".repeat(64),
            blurhash: None,
        };

        let cover = upsert(&mut db, release_db_id, cover)?;
        let cover_id = cover
            .db_id
            .ok_or_else(|| anyhow!("upsert did not set db_id"))?;
        let covers_id = db.exec(QueryBuilder::select().ids("covers").query())?.ids()[0];

        assert!(direct_edge_exists(&db, release_db_id, cover_id)?);
        assert!(direct_edge_exists(&db, covers_id, cover_id)?);

        Ok(())
    }

    #[test]
    fn upsert_existing_cover_keeps_single_collection_edge() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release(&mut db)?;
        let cover = Cover {
            db_id: None,
            id: nanoid!(),
            path: "/music/test/cover.jpg".to_string(),
            mime_type: "image/jpeg".to_string(),
            hash: "b".repeat(64),
            blurhash: None,
        };
        let mut cover = upsert(&mut db, release_db_id, cover)?;
        let original_id = cover
            .db_id
            .ok_or_else(|| anyhow!("upsert did not set db_id"))?;

        cover.hash = "c".repeat(64);
        cover.blurhash = Some("LKO2?U%2Tw=w]~RBVZRi};RPxuwH".to_string());

        let cover = upsert(&mut db, release_db_id, cover)?;
        let first_id = cover
            .db_id
            .ok_or_else(|| anyhow!("upsert did not set db_id"))?;
        let cover = upsert(&mut db, release_db_id, cover)?;
        let second_id = cover
            .db_id
            .ok_or_else(|| anyhow!("upsert did not set db_id"))?;
        let covers_id = db.exec(QueryBuilder::select().ids("covers").query())?.ids()[0];

        assert_eq!(first_id, original_id);
        assert_eq!(second_id, original_id);
        assert!(direct_edge_exists(&db, covers_id, original_id)?);
        assert_eq!(outgoing_edge_count(&db, covers_id)?, 1);

        Ok(())
    }
}
