// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub(crate) mod providers;
pub(crate) mod scorer;
mod selection;
mod upload;

pub(crate) use selection::{
    eligible_candidates,
    get_preferred_detail,
    has_meaningful_synced,
    normalize_language_hint,
    pick_preferred,
};
pub(crate) use upload::{
    LyricsUploadError,
    delete_all_lyrics_for_track,
    delete_personal_lyrics_for_track_by_db_id,
    delete_shared_lyrics_for_track_by_db_id,
    input_from_upload,
    lrc_to_input,
    now_ms,
    upsert_personal_lyrics_by_db_id,
    upsert_shared_lyrics_by_db_id,
};
