// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub mod sheets;
pub mod tracks;

pub use sheets::CueSheet;
pub use tracks::CueTrack;

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
