// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    DbId,
};
use lyra_ffmpeg::AudioFormat;
use std::path::PathBuf;

use crate::db::{
    self,
    Entry,
};

pub(crate) struct PlaybackSource {
    pub(crate) track_db_id: DbId,
    pub(crate) source_id: DbId,
    pub(crate) source_kind: String,
    pub(crate) source_key: String,
    pub(crate) is_primary: bool,
    pub(crate) input_path: String,
    pub(crate) full_path: PathBuf,
    pub(crate) entry_format: Option<AudioFormat>,
    pub(crate) start_ms: Option<u64>,
    pub(crate) end_ms: Option<u64>,
    pub(crate) entry: Option<Entry>,
}

pub(crate) fn resolve(
    db: &DbAny,
    track_db_id: DbId,
    include_entry: bool,
) -> anyhow::Result<Option<PlaybackSource>> {
    if db::tracks::get_by_id(db, track_db_id)?.is_none() {
        return Ok(None);
    }

    let primary_source_id =
        db::track_sources::get_primary_by_track(db, track_db_id)?.and_then(|source| source.db_id);
    let mut sources = db::track_sources::get_by_track(db, track_db_id)?;
    if let Some(primary_source_id) = primary_source_id {
        sources.sort_by_key(|source| source.db_id != Some(primary_source_id));
    } else {
        sources.sort_by_key(|source| !source.is_primary);
    }

    for source in sources {
        let Some(source_id) = source.db_id else {
            continue;
        };
        let Some(entry_db_id) = db::track_sources::get_entry_id(db, source_id)? else {
            continue;
        };
        let Some(entry) = db::entries::get_by_id(db, entry_db_id)? else {
            continue;
        };
        if entry.kind != db::entries::EntryKind::File || entry.file_kind.as_deref() != Some("audio")
        {
            continue;
        }
        if !entry.full_path.is_file() {
            continue;
        }

        let entry_format = entry
            .full_path
            .extension()
            .and_then(|ext| ext.to_str())
            .and_then(AudioFormat::parse);
        let input_path = entry.full_path.to_string_lossy().into_owned();
        let full_path = entry.full_path.clone();
        let entry = include_entry.then_some(entry);

        return Ok(Some(PlaybackSource {
            track_db_id,
            source_id,
            source_kind: source.source_kind,
            source_key: source.source_key,
            is_primary: source.is_primary,
            input_path,
            full_path,
            entry_format,
            start_ms: source.start_ms,
            end_ms: source.end_ms,
            entry,
        }));
    }

    Ok(None)
}
