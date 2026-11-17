// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};

use agdb::{
    DbAnyTransactionMut,
    DbId,
};

use super::super::TrackMetadata;
use lyra_db::graph::remove_edges_between;

/// The source keys a read yielded.
#[derive(Debug, Default)]
pub(crate) struct SourceYields(HashSet<String>);

impl SourceYields {
    pub(crate) fn of(metadata: &[TrackMetadata]) -> Self {
        Self(
            metadata
                .iter()
                .map(|meta| meta.source_key.clone())
                .collect(),
        )
    }

    pub(crate) fn yielded(&self, source_key: &str) -> bool {
        self.0.contains(source_key)
    }
}

/// Detaches from their tracks the sources of the files in `metadata` that their read didn't
/// yield, then collects what that leaves: the sources, their cue records, and the tracks left
/// with no file, with their emptied releases. Only a file's own sources go, those playing it: a
/// track it shares with another file keeps the other's. A source the read yields for another file,
/// as a cue track pointed at other audio, stays for that file's batch to move, with its track.
fn prune_stale_track_sources(
    db: &mut DbAnyTransactionMut<'_>,
    metadata: &[TrackMetadata],
    yields: &SourceYields,
) -> anyhow::Result<()> {
    let entries = metadata
        .iter()
        .map(|meta| meta.entry_db_id)
        .collect::<HashSet<_>>();

    let mut stale = Vec::new();
    for entry_db_id in entries {
        for source_db_id in lyra_db::graph::inbound_neighbor_ids(db, entry_db_id, "TrackSource")? {
            let Some(source) = lyra_db::track_sources::get_by_id(db, source_db_id)? else {
                continue;
            };
            if yields.yielded(&source.identity) || cue_sheet_missing(db, source_db_id)? {
                continue;
            }
            for track_db_id in lyra_db::graph::inbound_neighbor_ids(db, source_db_id, "Track")? {
                stale.push((track_db_id, source_db_id));
            }
        }
    }
    detach_stale_sources(db, stale)
}

/// Detaches the sources keyed `source_keys` from their tracks and collects what that leaves, as
/// [`prune_stale_track_sources`] does. Keys no source holds are skipped.
pub(crate) fn prune_sources(
    db: &mut DbAnyTransactionMut<'_>,
    source_keys: &[String],
) -> anyhow::Result<()> {
    let mut stale = Vec::new();
    for key in source_keys {
        let Some(source_db_id) =
            lyra_db::lookup::find_id_by_index(db, "identity", key, "TrackSource")?
        else {
            continue;
        };
        for track_db_id in lyra_db::graph::inbound_neighbor_ids(db, source_db_id, "Track")? {
            stale.push((track_db_id, source_db_id));
        }
    }
    detach_stale_sources(db, stale)
}

/// Detaches each `(track, source)` pair, then collects the sources, their cue records, and the
/// tracks left with no file, with their emptied releases.
fn detach_stale_sources(
    db: &mut DbAnyTransactionMut<'_>,
    stale: Vec<(DbId, DbId)>,
) -> anyhow::Result<()> {
    let mut left = lyra_db::orphans::Candidates::default();
    for &(track_db_id, source_db_id) in &stale {
        remove_edges_between(db, track_db_id, source_db_id)?;
        left.tracks.push(track_db_id);
        left.track_sources.push(source_db_id);
    }
    lyra_db::orphans::collect(db, left)
}

/// Whether `source_id` is a cue sheet's segment whose cue sheet is missing: the entry no longer
/// yields it only because the sheet can't be read, so the segment's track stays, unavailable.
fn cue_sheet_missing(db: &impl lyra_db::DbAccess, source_id: DbId) -> anyhow::Result<bool> {
    let Some(cue_entry_id) = lyra_db::track_sources::cue_entry_id(db, source_id)? else {
        return Ok(false);
    };
    Ok(lyra_db::entries::get_by_id(db, cue_entry_id)?.is_some_and(|entry| entry.is_missing()))
}

pub(crate) fn build_existing_track_map(
    db: &mut DbAnyTransactionMut<'_>,
    metadata: &[TrackMetadata],
    yields: &SourceYields,
) -> anyhow::Result<HashMap<String, DbId>> {
    prune_stale_track_sources(db, metadata, yields)?;

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
