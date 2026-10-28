// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    BTreeMap,
    HashSet,
};

use agdb::DbId;

use lyra_db::DbAccess;

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

        let id = match lyra_db::artists::find_id_by_scan_name(db, name)? {
            Some(existing_id) => existing_id,
            None => lyra_db::artists::create(db, name, None)?,
        };

        cache.insert(name.clone(), id);
        ids.push(id);
    }

    Ok(ids)
}
