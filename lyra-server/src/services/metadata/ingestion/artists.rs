// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    BTreeMap,
    HashSet,
};
use std::time::{
    SystemTime,
    UNIX_EPOCH,
};

use agdb::{
    DbId,
    QueryBuilder,
};
use nanoid::nanoid;

use crate::db::{
    Artist,
    DbAccess,
};

pub(crate) fn resolve_artist_ids(
    db: &mut impl DbAccess,
    names: &[String],
    cache: &mut BTreeMap<String, DbId>,
) -> anyhow::Result<Vec<DbId>> {
    let mut ids = Vec::new();
    let mut seen = HashSet::new();
    for name in names {
        if !seen.insert(name.clone()) {
            continue;
        }
        if let Some(id) = cache.get(name) {
            ids.push(*id);
            continue;
        }

        let id = if let Some(existing_id) = crate::db::lookup::find_id_by_indexed_string_field(
            db,
            "artists",
            "scan_name",
            "scan_name",
            name,
        )? {
            existing_id
        } else {
            let artist = Artist {
                db_id: None,
                id: nanoid!(),
                artist_name: name.clone(),
                scan_name: name.clone(),
                sort_name: None,
                artist_type: None,
                description: None,
                verified: false,
                locked: None,
                created_at: Some(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                ),
            };
            let insert_result = db.exec_mut(QueryBuilder::insert().element(&artist).query())?;
            let id = insert_result
                .elements
                .first()
                .map(|element| element.id)
                .ok_or_else(|| anyhow::anyhow!("artist insert missing id"))?;
            db.exec_mut(
                QueryBuilder::insert()
                    .edges()
                    .from("artists")
                    .to(id)
                    .query(),
            )?;
            id
        };

        cache.insert(name.clone(), id);
        ids.push(id);
    }

    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::db::Release;
    use crate::db::test_db::new_test_db;
    use agdb::DbAny;

    fn insert_release(db: &mut DbAny, title: &str) -> anyhow::Result<DbId> {
        let release = Release {
            db_id: None,
            id: nanoid!(),
            release_title: title.to_string(),
            sort_title: None,
            release_type: None,
            release_date: None,
            media_formats: None,
            barcode: None,
            locked: None,
            created_at: None,
            ctime: None,
        };
        let insert_result = db.exec_mut(QueryBuilder::insert().element(&release).query())?;
        let release_db_id = insert_result
            .elements
            .first()
            .map(|element| element.id)
            .ok_or_else(|| anyhow::anyhow!("release insert missing id"))?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("releases")
                .to(release_db_id)
                .values_uniform([("owned", 1).into()])
                .query(),
        )?;
        Ok(release_db_id)
    }

    #[test]
    fn replace_primary_for_owner_preserves_desired_order() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release(&mut db, "Ordered Release")?;
        let mut artist_cache = BTreeMap::new();
        let artist_ids = resolve_artist_ids(
            &mut db,
            &["Second Artist".to_string(), "First Artist".to_string()],
            &mut artist_cache,
        )?;

        db::credits::replace_primary_for_owner(&mut db, release_db_id, &artist_ids)?;

        let artists = db::artists::get(&db, release_db_id)?;
        let names: Vec<&str> = artists
            .iter()
            .map(|artist| artist.artist_name.as_str())
            .collect();
        assert_eq!(names, vec!["Second Artist", "First Artist"]);

        Ok(())
    }
}
