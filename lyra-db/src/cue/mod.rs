// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub mod sheets;
pub mod tracks;

pub use sheets::CueSheet;
pub use tracks::CueTrack;

use std::collections::HashMap;

use agdb::DbId;

use super::DbAccess;

/// Cue identities key on the cue entry's public id: agdb recycles its `DbId` once the entry is
/// deleted.
fn cue_entry_public_id(db: &impl DbAccess, cue_entry_id: DbId) -> anyhow::Result<String> {
    super::lookup::find_id_by_db_id(db, cue_entry_id)?
        .ok_or_else(|| anyhow::anyhow!("cue entry {} has no public id", cue_entry_id.0))
}

/// The entries `entry_id` shares a cue sheet with: a cue entry's audio, or the cue entries
/// whose tracks play an audio entry.
pub fn linked_entries(db: &impl DbAccess, entry_id: DbId) -> anyhow::Result<Vec<DbId>> {
    use super::graph::{
        inbound_neighbor_ids,
        neighbor_ids,
    };

    let mut linked = Vec::new();
    for sheet in inbound_neighbor_ids(db, entry_id, "CueSheet")? {
        for track in neighbor_ids(db, sheet, "CueTrack")? {
            linked.extend(neighbor_ids(db, track, "Entry")?);
        }
    }
    for track in inbound_neighbor_ids(db, entry_id, "CueTrack")? {
        for sheet in inbound_neighbor_ids(db, track, "CueSheet")? {
            linked.extend(neighbor_ids(db, sheet, "Entry")?);
        }
    }
    Ok(linked)
}

/// The audio entry each stored track of these cue entries' sheets plays, by cue entry and track
/// number.
pub fn audio_links(
    db: &impl DbAccess,
    cue_entry_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, HashMap<u32, DbId>>> {
    use super::graph::{
        inbound_neighbor_ids,
        neighbor_ids,
    };

    let mut links = HashMap::new();
    for &cue_entry_id in cue_entry_ids {
        let mut tracks = HashMap::new();
        for sheet in inbound_neighbor_ids(db, cue_entry_id, "CueSheet")? {
            for cue_track_id in neighbor_ids(db, sheet, "CueTrack")? {
                let Some(cue_track) = tracks::get_by_id(db, cue_track_id)? else {
                    continue;
                };
                if let Some(audio) = neighbor_ids(db, cue_track_id, "Entry")?.into_iter().next() {
                    tracks.insert(cue_track.track_no, audio);
                }
            }
        }
        if !tracks.is_empty() {
            links.insert(cue_entry_id, tracks);
        }
    }
    Ok(links)
}
