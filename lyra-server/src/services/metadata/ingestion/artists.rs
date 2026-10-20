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

        let id = if let Some(existing_id) = crate::db::artists::find_id_by_scan_name(db, name)? {
            existing_id
        } else {
            let artist = Artist {
                db_id: None,
                id: nanoid!(),
                artist_name: name.clone(),
                scan_name: name.clone(),
                scan_aliases: Vec::new(),
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
