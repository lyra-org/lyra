// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::DbId;
use serde::Serialize;

#[derive(Debug, Serialize, Clone)]
pub(crate) struct TrackMetadata {
    #[serde(skip)]
    pub(crate) entry_db_id: DbId,
    pub(crate) album: Option<String>,
    pub(crate) album_artists: Option<Vec<String>>,
    pub(crate) date: Option<String>,
    pub(crate) year: Option<u32>,
    pub(crate) title: Option<String>,
    pub(crate) artists: Option<Vec<String>>,
    pub(crate) disc: Option<u32>,
    pub(crate) disc_total: Option<u32>,
    pub(crate) track: Option<u32>,
    pub(crate) track_total: Option<u32>,
    pub(crate) duration_ms: Option<u64>,
    pub(crate) genres: Option<Vec<String>>,
    pub(crate) label: Option<String>,
    pub(crate) catalog_number: Option<String>,
    pub(crate) source_kind: Option<String>,
    pub(crate) source_key: Option<String>,
    pub(crate) segment_start_ms: Option<u64>,
    pub(crate) segment_end_ms: Option<u64>,
    pub(crate) cue_sheet_entry_id: Option<DbId>,
    pub(crate) cue_sheet_hash: Option<String>,
    pub(crate) cue_track_no: Option<u32>,
    pub(crate) cue_audio_entry_id: Option<DbId>,
    pub(crate) cue_index00_frames: Option<u32>,
    pub(crate) cue_index01_frames: Option<u32>,
    pub(crate) sample_rate_hz: Option<u32>,
    pub(crate) channel_count: Option<u32>,
    pub(crate) bit_depth: Option<u32>,
    pub(crate) bitrate_bps: Option<u32>,
}

impl lyra_metadata::ReleaseCoalesceTrack for TrackMetadata {
    fn album(&self) -> Option<&str> {
        self.album.as_deref()
    }

    fn set_album(&mut self, album: Option<String>) {
        self.album = album;
    }

    fn album_artists(&self) -> Option<&[String]> {
        self.album_artists.as_deref()
    }

    fn artists(&self) -> Option<&[String]> {
        self.artists.as_deref()
    }

    fn year(&self) -> Option<u32> {
        self.year
    }

    fn disc(&self) -> Option<u32> {
        self.disc
    }

    fn track(&self) -> Option<u32> {
        self.track
    }
}
