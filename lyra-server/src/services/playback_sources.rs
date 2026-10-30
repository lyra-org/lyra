// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    DbId,
};
use lyra_ffmpeg::{
    AudioCodec,
    AudioFormat,
};
use std::{
    path::{
        Path,
        PathBuf,
    },
    time::UNIX_EPOCH,
};

use crate::services::entries::PlacedEntry;
use lyra_db::track_sources::{
    SourceCodec,
    SourceContainer,
    SourceFormat,
};

pub(crate) struct PlaybackSource {
    pub(crate) track_db_id: DbId,
    pub(crate) source_id: DbId,
    pub(crate) source_kind: String,
    pub(crate) source_key: String,
    pub(crate) is_primary: bool,
    pub(crate) input_path: String,
    pub(crate) full_path: PathBuf,
    pub(crate) format: SourceFormat,
    pub(crate) start_ms: Option<u64>,
    pub(crate) end_ms: Option<u64>,
    pub(crate) entry: Option<PlacedEntry>,
}

pub(crate) fn resolve(
    db: &DbAny,
    track_db_id: DbId,
    include_entry: bool,
) -> anyhow::Result<Option<PlaybackSource>> {
    if lyra_db::tracks::get_by_id(db, track_db_id)?.is_none() {
        return Ok(None);
    }

    let primary_source_id = lyra_db::track_sources::get_primary_by_track(db, track_db_id)?
        .and_then(|source| source.db_id);
    let mut sources = lyra_db::track_sources::get_by_track(db, track_db_id)?;
    if let Some(primary_source_id) = primary_source_id {
        sources.sort_by_key(|source| source.db_id != Some(primary_source_id));
    } else {
        sources.sort_by_key(|source| !source.is_primary);
    }

    for source in sources {
        let Some(source_id) = source.db_id else {
            continue;
        };
        let Some(entry_db_id) = lyra_db::track_sources::get_entry_id(db, source_id)? else {
            continue;
        };
        let Some(entry) = lyra_db::entries::get_by_id(db, entry_db_id)? else {
            continue;
        };
        if entry.kind != lyra_db::entries::EntryKind::File
            || entry.file_kind.as_deref() != Some("audio")
        {
            continue;
        }
        if !entry.full_path.is_file() {
            continue;
        }

        let input_path = entry.full_path.to_string_lossy().into_owned();
        let full_path = entry.full_path.clone();
        let entry = include_entry
            .then(|| PlacedEntry::locate(db, entry))
            .transpose()?;

        let format = source.format();
        return Ok(Some(PlaybackSource {
            track_db_id,
            source_id,
            source_kind: source.source_kind,
            source_key: source.source_key,
            is_primary: source.is_primary,
            input_path,
            full_path,
            format,
            start_ms: source.start_ms,
            end_ms: source.end_ms,
            entry,
        }));
    }

    Ok(None)
}

/// The exact file content a playback source resolved to. Anything derived from a source, such as
/// a media token or a cached transcode, compares it to tell when the file was replaced.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SourceVersion {
    pub(crate) source_public_id: String,
    pub(crate) size: u64,
    pub(crate) modified_ns: u128,
}

impl SourceVersion {
    /// Reads the version of the file at `path`, failing when it is not a regular file.
    pub(crate) fn read(source_public_id: String, path: &Path) -> std::io::Result<Self> {
        Self::of(source_public_id, &std::fs::metadata(path)?)
    }

    /// The version of the file `metadata` describes, failing when it is not a regular file.
    pub(crate) fn of(
        source_public_id: String,
        metadata: &std::fs::Metadata,
    ) -> std::io::Result<Self> {
        if !metadata.is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "track source is not a regular file",
            ));
        }
        let modified_ns = metadata
            .modified()?
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos());
        Ok(Self {
            source_public_id,
            size: metadata.len(),
            modified_ns,
        })
    }

    #[cfg(test)]
    pub(crate) fn fixed(source_public_id: &str) -> Self {
        Self {
            source_public_id: source_public_id.to_string(),
            size: 1,
            modified_ns: 1,
        }
    }
}

/// The transcoder's format for a source's container, when it can copy from it.
pub(crate) fn transcoder_format(format: SourceFormat) -> Option<AudioFormat> {
    match (format.container?, format.codec) {
        (SourceContainer::Aac, _) => Some(AudioFormat::Aac),
        (SourceContainer::Aiff, _) => Some(AudioFormat::Aiff),
        (SourceContainer::Flac, _) => Some(AudioFormat::Flac),
        (SourceContainer::M4a, _) => Some(AudioFormat::M4a),
        (SourceContainer::Mp3, None | Some(SourceCodec::Mp3)) => Some(AudioFormat::Mp3),
        (SourceContainer::Ogg, Some(SourceCodec::Opus)) => Some(AudioFormat::Opus),
        (SourceContainer::Ogg, _) => Some(AudioFormat::Ogg),
        (SourceContainer::Wav, _) => Some(AudioFormat::Wav),
        _ => None,
    }
}

/// The transcoder's codec for a source's codec, when it knows it.
pub(crate) fn transcoder_codec(codec: SourceCodec) -> Option<AudioCodec> {
    match codec {
        SourceCodec::Aac => Some(AudioCodec::Aac),
        SourceCodec::Alac => Some(AudioCodec::Alac),
        SourceCodec::Flac => Some(AudioCodec::Flac),
        SourceCodec::Mp3 => Some(AudioCodec::Mp3),
        SourceCodec::Opus => Some(AudioCodec::Opus),
        SourceCodec::Vorbis => Some(AudioCodec::Vorbis),
        SourceCodec::PcmS16Le => Some(AudioCodec::PcmS16Le),
        SourceCodec::PcmS16Be => Some(AudioCodec::PcmS16Be),
        SourceCodec::PcmS24Le => Some(AudioCodec::PcmS24Le),
        SourceCodec::PcmS24Be => Some(AudioCodec::PcmS24Be),
        _ => None,
    }
}
