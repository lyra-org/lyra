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
};

use super::super::TrackMetadata;
use lyra_db::graph::remove_edges_between;

/// Detaches the sources the entries no longer yield from their tracks, then collects what that
/// leaves: the sources, their cue records, and the tracks left with no file, with their emptied
/// releases.
fn prune_stale_track_sources(db: &mut DbAny, metadata: &[TrackMetadata]) -> anyhow::Result<()> {
    let mut expected_by_entry: HashMap<DbId, HashSet<String>> = HashMap::new();
    for meta in metadata {
        expected_by_entry
            .entry(meta.entry_db_id)
            .or_default()
            .insert(meta.source_key.clone());
    }

    let mut stale = Vec::new();
    for (entry_db_id, expected_source_keys) in expected_by_entry {
        let tracks = lyra_db::tracks::get_by_entry(db, entry_db_id)?;
        for track in tracks {
            let Some(track_db_id) = track.db_id.map(Into::into) else {
                continue;
            };
            let sources = lyra_db::track_sources::get_by_track(db, track_db_id)?;
            for source in sources {
                let Some(source_db_id) = source.db_id else {
                    continue;
                };
                if expected_source_keys.contains(&source.source_key)
                    || cue_sheet_missing(db, source_db_id)?
                {
                    continue;
                }
                stale.push((track_db_id, source_db_id));
            }
        }
    }
    if stale.is_empty() {
        return Ok(());
    }

    db.transaction_mut(|t| -> anyhow::Result<_> {
        let mut left = lyra_db::orphans::Candidates::default();
        for &(track_db_id, source_db_id) in &stale {
            remove_edges_between(t, track_db_id, source_db_id)?;
            left.tracks.push(track_db_id);
            left.track_sources.push(source_db_id);
        }
        lyra_db::orphans::collect(t, left)
    })?;
    crate::services::covers::discard_listed_cover_files(db);

    Ok(())
}

/// Whether `source_id` is a cue sheet's segment whose cue sheet is missing: the entry no longer
/// yields it only because the sheet can't be read, so the segment's track stays, unavailable.
fn cue_sheet_missing(db: &DbAny, source_id: DbId) -> anyhow::Result<bool> {
    let Some(cue_entry_id) = lyra_db::track_sources::cue_entry_id(db, source_id)? else {
        return Ok(false);
    };
    Ok(lyra_db::entries::get_by_id(db, cue_entry_id)?.is_some_and(|entry| entry.is_missing()))
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

        if let Some(track_id) =
            lyra_db::track_sources::get_track_id_by_source_key(db, &meta.source_key)?
        {
            track_ids.insert(meta.source_key.clone(), track_id);
        }
    }

    Ok(track_ids)
}
