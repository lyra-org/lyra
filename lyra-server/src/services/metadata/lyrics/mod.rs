// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod selection;
mod upload;

pub(crate) use selection::{
    get_preferred_detail,
    pick_preferred,
};
pub(crate) use upload::{
    LyricsUploadError,
    delete_all_lyrics_for_track,
    delete_user_lyrics_for_track,
    delete_user_lyrics_for_track_by_db_id,
    input_from_upload,
    lrc_to_input,
    now_ms,
    upsert_plugin_lyrics,
    upsert_user_lyrics,
    upsert_user_lyrics_by_db_id,
};
