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

fn metadata_source_identity(meta: &TrackMetadata) -> String {
    meta.source_key
        .clone()
        .unwrap_or_else(|| format!("entry:{}:embedded", meta.entry_db_id.0))
}

fn prune_stale_track_sources(db: &mut DbAny, metadata: &[TrackMetadata]) -> anyhow::Result<()> {
    let mut expected_by_entry: HashMap<DbId, HashSet<String>> = HashMap::new();
    for meta in metadata {
        expected_by_entry
            .entry(meta.entry_db_id)
            .or_default()
            .insert(metadata_source_identity(meta));
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
        let identity = metadata_source_identity(meta);
        if track_ids.contains_key(&identity) {
            continue;
        }

        if let Some(track_id) = db::track_sources::get_track_id_by_source_key(db, &identity)? {
            track_ids.insert(identity, track_id);
        }
    }

    Ok(track_ids)
}
