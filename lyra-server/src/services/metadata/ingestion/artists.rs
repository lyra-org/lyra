// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
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
    Credit,
    CreditType,
    DbAccess,
    credits,
};

pub(crate) fn sync_artist_edges(
    db: &mut impl DbAccess,
    owner_db_id: DbId,
    desired_ids: &[DbId],
    role: CreditType,
) -> anyhow::Result<()> {
    let mut ordered_desired = Vec::new();
    let mut desired_set = HashSet::new();
    for id in desired_ids {
        if desired_set.insert(*id) {
            ordered_desired.push(*id);
        }
    }

    // Collect existing Credits for this role, mapping artist_db_id → credit_db_id.
    let existing_credits: Vec<Credit> = db
        .exec(
            QueryBuilder::select()
                .elements::<Credit>()
                .search()
                .from(owner_db_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    let mut existing_by_artist: HashMap<DbId, DbId> = HashMap::new();
    for credit in &existing_credits {
        if credit.credit_type != role {
            continue;
        }
        let Some(credit_db_id) = credit.db_id.clone().map(DbId::from) else {
            continue;
        };
        let edges = crate::db::graph::direct_edges_from(db, credit_db_id)?;
        if let Some(artist_db_id) = edges.iter().find_map(|e| e.to.filter(|id| id.0 > 0)) {
            existing_by_artist.insert(artist_db_id, credit_db_id);
        }
    }

    let remove_ids: Vec<DbId> = existing_by_artist
        .iter()
        .filter(|(artist_id, _)| !desired_set.contains(artist_id))
        .map(|(_, credit_id)| *credit_id)
        .collect();
    if !remove_ids.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(remove_ids).query())?;
    }

    for (order, desired_id) in ordered_desired.iter().enumerate() {
        if let Some(credit_db_id) = existing_by_artist.get(desired_id) {
            // Update order on existing owner→credit edge.
            let edge_ids = crate::db::graph::direct_edge_ids(db, owner_db_id, *credit_db_id)?;
            if let Some(edge_id) = edge_ids.first() {
                db.exec_mut(
                    QueryBuilder::insert()
                        .values_uniform([
                            ("owned", 1).into(),
                            (credits::EDGE_ORDER_KEY, order as u64).into(),
                        ])
                        .ids(*edge_id)
                        .query(),
                )?;
            }
        } else {
            let credit = Credit {
                db_id: None,
                id: nanoid!(),
                credit_type: role,
                detail: None,
            };
            let insert_result = db.exec_mut(QueryBuilder::insert().element(&credit).query())?;
            let credit_db_id = insert_result
                .elements
                .first()
                .map(|e| e.id)
                .ok_or_else(|| anyhow::anyhow!("credit insert missing id"))?;

            db.exec_mut(
                QueryBuilder::insert()
                    .edges()
                    .from("credits")
                    .to(credit_db_id)
                    .query(),
            )?;
            db.exec_mut(
                QueryBuilder::insert()
                    .edges()
                    .from(owner_db_id)
                    .to(credit_db_id)
                    .values_uniform([
                        ("owned", 1).into(),
                        (credits::EDGE_ORDER_KEY, order as u64).into(),
                    ])
                    .query(),
            )?;
            db.exec_mut(
                QueryBuilder::insert()
                    .edges()
                    .from(credit_db_id)
                    .to(*desired_id)
                    .query(),
            )?;
        }
    }

    Ok(())
}

pub(crate) fn resolve_artist_ids(
    db: &mut impl DbAccess,
    names: &[String],
    cache: &mut HashMap<String, DbId>,
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
    fn sync_artist_edges_preserves_desired_order() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release(&mut db, "Ordered Release")?;
        let mut artist_cache = HashMap::new();
        let artist_ids = resolve_artist_ids(
            &mut db,
            &["Second Artist".to_string(), "First Artist".to_string()],
            &mut artist_cache,
        )?;

        sync_artist_edges(&mut db, release_db_id, &artist_ids, CreditType::Artist)?;

        let artists = db::artists::get(&db, release_db_id)?;
        let names: Vec<&str> = artists
            .iter()
            .map(|artist| artist.artist_name.as_str())
            .collect();
        assert_eq!(names, vec!["Second Artist", "First Artist"]);

        Ok(())
    }
}
