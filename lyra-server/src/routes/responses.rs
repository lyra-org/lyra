// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use schemars::JsonSchema;
use serde::Serialize;

use crate::{
    db,
    db::IdSource,
    services::entities::{
        ArtistCreditSource,
        ResolvedCreditedArtist,
    },
};

#[derive(Serialize, JsonSchema)]
pub struct LyricsResponse {
    #[schemars(description = "Provider-supplied stable ID scoped to `(track, provider_id)`.")]
    pub id: String,
    #[schemars(
        description = "Source provider ID. `user` is reserved and paired with `origin=user`; all other values identify an enabled metadata provider."
    )]
    pub provider_id: String,
    #[schemars(description = "ISO-639-2 language code in lowercase; `und` when unknown.")]
    pub language: String,
    pub origin: LyricsOriginResponse,
    #[schemars(
        description = "Provider-supplied plain text. May be empty when the provider only returned synced lines; in that case clients can reconstruct plain text by joining `lines[].text` with newlines, which is what `?format=plain` does."
    )]
    pub plain_text: String,
    #[schemars(description = "True when any line carries per-word timing in `words`.")]
    pub has_word_cues: bool,
    #[schemars(description = "Seconds since the Unix epoch; updated only when content changes.")]
    pub updated_at: u64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[schemars(description = "Synced lines in playback order. Empty for plain-text-only lyrics.")]
    pub lines: Vec<LyricsLineResponse>,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[schemars(
    description = "`user`: hand-authored override, preferred over every provider. `plugin`: written by an enabled metadata provider."
)]
pub enum LyricsOriginResponse {
    User,
    Plugin,
}

impl From<IdSource> for LyricsOriginResponse {
    fn from(source: IdSource) -> Self {
        match source {
            IdSource::User => Self::User,
            IdSource::Plugin => Self::Plugin,
        }
    }
}

#[derive(Serialize, JsonSchema)]
pub struct LyricsLineResponse {
    #[schemars(description = "Milliseconds from track start.")]
    pub ts_ms: u64,
    pub text: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub words: Vec<LyricsWordResponse>,
}

#[derive(Serialize, JsonSchema)]
pub struct LyricsWordResponse {
    #[schemars(description = "Milliseconds from track start when this word starts.")]
    pub ts_ms: u64,
    #[schemars(
        description = "Inclusive Unicode-scalar (code point) offset into the containing line's `text` where the word begins. Not a byte offset; stable across UTF-8 encoding."
    )]
    pub char_start: u32,
    #[schemars(
        description = "Exclusive Unicode-scalar (code point) offset into the containing line's `text` where the word ends."
    )]
    pub char_end: u32,
}

#[derive(Serialize, JsonSchema)]
pub struct ReleaseResponse {
    pub id: String,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artists: Option<Vec<ArtistResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracks: Option<Vec<TrackResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entries: Option<Vec<EntryResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(description = "Release date as YYYY, YYYY-MM, or YYYY-MM-DD.")]
    pub release_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genres: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cover: Option<Option<ReleaseCoverResponse>>,
}

impl From<db::Release> for ReleaseResponse {
    fn from(release: db::Release) -> Self {
        Self {
            id: release.id,
            title: release.release_title,
            sort_title: release.sort_title,
            release_date: release.release_date,
            genres: None,
            artists: None,
            tracks: None,
            entries: None,
            cover: None,
        }
    }
}

#[derive(Serialize, JsonSchema)]
pub struct ReleaseCoverResponse {
    pub mime_type: String,
    pub hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blurhash: Option<String>,
}

#[derive(Serialize, JsonSchema)]
pub struct TrackResponse {
    pub id: String,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub year: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disc: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disc_total: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub track: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub track_total: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub releases: Option<Vec<ReleaseResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artists: Option<Vec<ArtistResponse>>,
}

impl From<db::Track> for TrackResponse {
    fn from(track: db::Track) -> Self {
        Self {
            id: track.id,
            title: track.track_title,
            sort_title: track.sort_title,
            year: track.year,
            disc: track.disc,
            disc_total: track.disc_total,
            track: track.track,
            track_total: track.track_total,
            duration_ms: track.duration_ms,
            releases: None,
            artists: None,
        }
    }
}

#[derive(Serialize, JsonSchema)]
pub struct ArtistResponse {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub verified: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credit: Option<ArtistCreditResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub releases: Option<Vec<ReleaseResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracks: Option<Vec<TrackResponse>>,
}

#[derive(Serialize, JsonSchema)]
pub struct ArtistCreditResponse {
    #[serde(rename = "type")]
    pub credit_type: db::CreditType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    pub source: ArtistCreditSourceResponse,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ArtistCreditSourceResponse {
    Track,
    Release,
}

impl From<ArtistCreditSource> for ArtistCreditSourceResponse {
    fn from(source: ArtistCreditSource) -> Self {
        match source {
            ArtistCreditSource::Track => Self::Track,
            ArtistCreditSource::Release => Self::Release,
        }
    }
}

impl From<db::Artist> for ArtistResponse {
    fn from(artist: db::Artist) -> Self {
        Self {
            id: artist.id,
            name: artist.artist_name,
            sort_name: artist.sort_name,
            description: artist.description,
            verified: artist.verified,
            credit: None,
            releases: None,
            tracks: None,
        }
    }
}

impl From<ResolvedCreditedArtist> for ArtistResponse {
    fn from(credited_artist: ResolvedCreditedArtist) -> Self {
        let mut response = Self::from(credited_artist.artist);
        response.credit = Some(ArtistCreditResponse {
            credit_type: credited_artist.credit.credit_type,
            detail: credited_artist.credit.detail,
            source: credited_artist.source.into(),
        });
        response
    }
}

#[derive(Serialize, JsonSchema)]
pub struct EntryResponse {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub full_path: Option<String>,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_kind: Option<String>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    pub size: u64,
    pub mtime: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracks: Option<Vec<TrackResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub releases: Option<Vec<ReleaseResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artists: Option<Vec<ArtistResponse>>,
}

impl EntryResponse {
    pub fn from_entry(entry: db::Entry, include_full_path: bool) -> Self {
        Self {
            id: entry.id,
            full_path: include_full_path.then(|| entry.full_path.to_string_lossy().into_owned()),
            kind: entry.kind.to_string(),
            file_kind: entry.file_kind,
            name: entry.name,
            hash: entry.hash,
            size: entry.size,
            mtime: entry.mtime,
            tracks: None,
            releases: None,
            artists: None,
        }
    }
}

impl From<db::Entry> for EntryResponse {
    fn from(entry: db::Entry) -> Self {
        Self::from_entry(entry, true)
    }
}
