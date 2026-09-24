// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};

use agdb::{
    DbAny,
    DbId,
    QueryBuilder,
};

use super::super::TrackMetadata;
use crate::db::{
    self,
    graph::remove_edges_between,
};

fn prune_stale_track_sources(db: &mut DbAny, metadata: &[TrackMetadata]) -> anyhow::Result<()> {
    let mut expected_by_entry: HashMap<DbId, HashSet<String>> = HashMap::new();
    for meta in metadata {
        expected_by_entry
            .entry(meta.entry_db_id)
            .or_default()
            .insert(meta.source_key.clone());
    }

    for (entry_db_id, expected_source_keys) in expected_by_entry {
        let tracks = db::tracks::get_by_entry(db, entry_db_id)?;
        for track in tracks {
            let Some(track_db_id) = track.db_id.map(Into::into) else {
                continue;
            };
            let sources = db::track_sources::get_by_track(db, track_db_id)?;
            for source in sources {
                let Some(source_db_id) = source.db_id else {
                    continue;
                };
                if expected_source_keys.contains(&source.source_key) {
                    continue;
                }

                remove_edges_between(db, track_db_id, source_db_id)?;
                let still_attached =
                    db::track_sources::get_track_id_by_source_key(db, &source.source_key)?
                        .is_some();
                if !still_attached {
                    db.exec_mut(QueryBuilder::remove().ids(source_db_id).query())?;
                }
            }
        }
    }

    Ok(())
}

pub(crate) fn build_existing_track_map(
    db: &mut DbAny,
    metadata: &[TrackMetadata],
) -> anyhow::Result<HashMap<String, DbId>> {
    prune_stale_track_sources(db, metadata)?;

    let mut track_ids = HashMap::new();

    for meta in metadata {
        if track_ids.contains_key(&meta.source_key) {
            continue;
        }

        if let Some(track_id) = db::track_sources::get_track_id_by_source_key(db, &meta.source_key)?
        {
            track_ids.insert(meta.source_key.clone(), track_id);
        }
    }

    Ok(track_ids)
}
