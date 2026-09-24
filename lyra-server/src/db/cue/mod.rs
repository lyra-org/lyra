// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub(crate) mod sheets;
pub(crate) mod tracks;

pub(crate) use sheets::CueSheet;
pub(crate) use tracks::CueTrack;

use agdb::DbId;

use super::DbAccess;

/// Cue identities key on the cue entry's public id: agdb recycles its `DbId` once the entry is
/// deleted.
fn cue_entry_public_id(db: &impl DbAccess, cue_entry_id: DbId) -> anyhow::Result<String> {
    super::lookup::find_id_by_db_id(db, cue_entry_id)?
        .ok_or_else(|| anyhow::anyhow!("cue entry {} has no public id", cue_entry_id.0))
}
