// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use agdb::{
    DbAny,
    DbId,
};

use crate::services::catalog::Availability;
use serde::Serialize;

use crate::services::entities::{
    ArtistCreditSource,
    ResolvedCreditedArtist,
};
use lyra_db::lyrics::LyricsKind;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub(crate) struct PageResponse<T> {
    pub items: Vec<T>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "How many items the whole list holds, under the request's filters."
        )
    )]
    pub total: u64,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Where `items` starts in the list: the requested `offset`, or `total` when that is past the end."
        )
    )]
    pub offset: u64,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "The seed of the shuffle, present when the list is sorted at random. Send it as `seed` with another `offset` to page through the same shuffle."
        )
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
}

impl<T> PageResponse<T> {
    /// The page with each item mapped, keeping its place in the list.
    pub(crate) fn map<U>(self, map: impl FnMut(T) -> U) -> PageResponse<U> {
        PageResponse {
            items: self.items.into_iter().map(map).collect(),
            total: self.total,
            offset: self.offset,
            seed: self.seed,
        }
    }

    /// The page with its items replaced by what `convert` makes of them together.
    pub(crate) fn try_map_items<U, E>(
        self,
        convert: impl FnOnce(Vec<T>) -> Result<Vec<U>, E>,
    ) -> Result<PageResponse<U>, E> {
        Ok(PageResponse {
            items: convert(self.items)?,
            total: self.total,
            offset: self.offset,
            seed: self.seed,
        })
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub struct LyricsResponse {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "The lyrics entry's ID, kept while the entry is rewritten: a provider's re-fetch or a new upload to the same scope keeps it."
        )
    )]
    pub id: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Metadata provider ID; omitted for manual lyrics.")
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "The provider's own ID for these lyrics; omitted for manual lyrics."
        )
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_lyrics_id: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Lowercase ISO-639-3 language code; `und` when unknown.")
    )]
    pub language: String,
    pub scope: LyricsScopeResponse,
    pub source: LyricsSourceResponse,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Provider-supplied plain text. May be empty when the provider only returned synced lines; in that case clients can reconstruct plain text by joining `lines[].text` with newlines, which is what `?format=plain` does."
        )
    )]
    pub plain_text: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "True when any line carries per-word timing in `words`.")
    )]
    pub has_word_cues: bool,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "RFC3339 timestamp; updated only when content changes.")
    )]
    pub updated_at: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Synced lines in playback order. Empty for plain-text-only lyrics."
        )
    )]
    pub lines: Vec<LyricsLineResponse>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(
    feature = "docgen",
    schemars(
        description = "`personal` is visible only to its owner; `shared` is visible to every caller with track access."
    )
)]
pub enum LyricsScopeResponse {
    Personal,
    Shared,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum LyricsSourceResponse {
    Manual,
    Provider,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub struct LyricsCandidateSummaryResponse {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "The lyrics entry's ID, as `id` in a lyrics response.")
    )]
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "As `provider_lyrics_id` in a lyrics response.")
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_lyrics_id: Option<String>,
    pub language: String,
    pub scope: LyricsScopeResponse,
    pub source: LyricsSourceResponse,
    pub lrc_available: bool,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "As `has_word_cues` in a lyrics response.")
    )]
    pub has_word_cues: bool,
    pub updated_at: String,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub struct LyricsCandidatesResponse {
    pub items: Vec<LyricsCandidateSummaryResponse>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub struct LyricsRefreshResponse {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Metadata provider IDs whose refresh did not complete: a handler error, a timeout, or a failure to store the result."
        )
    )]
    pub failed_providers: Vec<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Metadata provider IDs skipped because a lookup for this track was already running. That lookup may finish after this response and may not be forced, so the refresh can be retried."
        )
    )]
    pub in_progress_providers: Vec<String>,
}

impl LyricsScopeResponse {
    pub(crate) fn from_lyrics(lyrics: &lyra_db::Lyrics) -> Self {
        match lyrics.kind {
            LyricsKind::Personal => Self::Personal,
            LyricsKind::Shared | LyricsKind::Provider => Self::Shared,
        }
    }
}

impl LyricsSourceResponse {
    pub(crate) fn from_lyrics(lyrics: &lyra_db::Lyrics) -> Self {
        match lyrics.kind {
            LyricsKind::Personal | LyricsKind::Shared => Self::Manual,
            LyricsKind::Provider => Self::Provider,
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub struct LyricsLineResponse {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Milliseconds from track start.")
    )]
    pub ts_ms: u64,
    pub text: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub words: Vec<LyricsWordResponse>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub struct LyricsWordResponse {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Milliseconds from track start when this word starts.")
    )]
    pub ts_ms: u64,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Inclusive Unicode-scalar (code point) offset into the containing line's `text` where the word begins. Not a byte offset; stable across UTF-8 encoding."
        )
    )]
    pub char_start: u32,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Exclusive Unicode-scalar (code point) offset into the containing line's `text` where the word ends."
        )
    )]
    pub char_end: u32,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub struct ReleaseResponse {
    pub id: String,
    pub title: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Whether any of the release's tracks can play; omitted for a release the caller can't see."
        )
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub available: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artists: Option<Vec<ArtistResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracks: Option<Vec<TrackResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entries: Option<Vec<EntryResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Release date as YYYY, YYYY-MM, or YYYY-MM-DD.")
    )]
    pub release_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genres: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cover: Option<Option<CoverResponse>>,
}

impl ReleaseResponse {
    /// The response for `release`, with its availability as `index` has it.
    pub(crate) fn new(release: lyra_db::Release, availability: &Availability) -> Self {
        Self {
            available: Some(
                release
                    .db_id
                    .clone()
                    .map(DbId::from)
                    .is_none_or(|id| availability.release(id)),
            ),
            id: release.id,
            title: release.release_title,
            sort_title: release.sort_title,
            release_date: release.release_date,
            description: release.description,
            genres: None,
            artists: None,
            tracks: None,
            entries: None,
            cover: None,
        }
    }
}

impl ReleaseResponse {
    pub(crate) fn unavailable(id: String) -> Self {
        Self {
            id,
            title: "unavailable".to_string(),
            available: None,
            sort_title: None,
            release_date: None,
            description: None,
            genres: None,
            artists: None,
            tracks: None,
            entries: None,
            cover: None,
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub struct CoverResponse {
    #[cfg_attr(feature = "docgen", schemars(description = "Opaque cover image ID."))]
    pub id: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Public image URL for browser/native image loading. Treat this as an opaque URL; it includes a cache-version query parameter derived from the cover hash."
        )
    )]
    pub url: String,
    pub mime_type: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Content hash used to version the public image URL.")
    )]
    pub hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blurhash: Option<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
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
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Whether the track can play: false when none of its files is there; omitted for a track the caller can't see."
        )
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub available: Option<bool>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "The track's chain; omitted when the track is in none.")
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain: Option<TrackChainResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub releases: Option<Vec<ReleaseResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artists: Option<Vec<ArtistResponse>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "The files holding the track's audio that the caller can see, by name; `[]` when it can see none."
        )
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entries: Option<Vec<EntryResponse>>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize)]
pub struct TrackChainResponse {
    pub id: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "The track's 0-based place in the chain.")
    )]
    pub position: u64,
}

/// The chain places of a batch of tracks, read in one lookup, and their availability as the
/// catalog index has it.
pub(crate) struct TrackChains {
    chains: HashMap<DbId, TrackChainResponse>,
    availability: Availability,
}

impl TrackChains {
    pub(crate) fn of<'a>(
        db: &DbAny,
        tracks: impl IntoIterator<Item = &'a lyra_db::Track>,
    ) -> anyhow::Result<Self> {
        let track_db_ids = tracks
            .into_iter()
            .filter_map(|track| track.db_id.clone().map(DbId::from))
            .collect::<Vec<_>>();
        Ok(Self {
            chains: lyra_db::chains::links_for_tracks(db, &track_db_ids)?
                .into_iter()
                .map(|(track_db_id, link)| {
                    (
                        track_db_id,
                        TrackChainResponse {
                            id: link.chain_id,
                            position: link.position,
                        },
                    )
                })
                .collect(),
            availability: crate::services::catalog::Availability::of(db)?,
        })
    }

    pub(crate) fn get(&self, track: &lyra_db::Track) -> Option<TrackChainResponse> {
        let track_db_id = track.db_id.clone().map(DbId::from)?;
        self.chains.get(&track_db_id).cloned()
    }

    pub(crate) fn availability(&self) -> &Availability {
        &self.availability
    }

    pub(crate) fn available(&self, track: &lyra_db::Track) -> bool {
        track
            .db_id
            .clone()
            .map(DbId::from)
            .is_none_or(|id| self.availability.track(id))
    }

    /// The responses for `tracks`, in order, with their chains read in one lookup.
    pub(crate) fn responses(
        db: &DbAny,
        tracks: Vec<lyra_db::Track>,
    ) -> anyhow::Result<Vec<TrackResponse>> {
        let chains = Self::of(db, &tracks)?;
        Ok(tracks
            .into_iter()
            .map(|track| chains.response(track))
            .collect())
    }

    pub(crate) fn response(&self, track: lyra_db::Track) -> TrackResponse {
        TrackResponse {
            available: Some(self.available(&track)),
            chain: self.get(&track),
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
            entries: None,
        }
    }
}

impl TrackResponse {
    pub(crate) fn unavailable(id: String) -> Self {
        Self {
            id,
            title: "unavailable".to_string(),
            available: None,
            sort_title: None,
            year: None,
            disc: None,
            disc_total: None,
            track: None,
            track_total: None,
            duration_ms: None,
            chain: None,
            releases: None,
            artists: None,
            entries: None,
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub struct ArtistResponse {
    pub id: String,
    pub name: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Whether anything the artist is credited on can play; false for an artist credited on nothing, omitted for an artist the caller can't see."
        )
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub available: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credit: Option<ArtistCreditResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub releases: Option<Vec<ReleaseResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracks: Option<Vec<TrackResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relations: Option<Vec<ArtistRelationResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cover: Option<Option<CoverResponse>>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub struct ArtistRelationResponse {
    #[serde(rename = "type")]
    pub relation_type: lyra_db::ArtistRelationType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<String>,
    pub direction: RelationDirectionResponse,
    pub artist: RelatedArtistResponse,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RelationDirectionResponse {
    Incoming,
    Outgoing,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub struct RelatedArtistResponse {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artist_type: Option<lyra_db::ArtistType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cover: Option<Option<CoverResponse>>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub struct ArtistCreditResponse {
    #[serde(rename = "type")]
    pub credit_type: lyra_db::CreditType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    pub source: ArtistCreditSourceResponse,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
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

impl ArtistResponse {
    /// The response for `artist`, with its availability as `index` has it.
    pub(crate) fn new(artist: lyra_db::Artist, availability: &Availability) -> Self {
        Self {
            available: Some(
                artist
                    .db_id
                    .clone()
                    .map(DbId::from)
                    .is_none_or(|id| availability.artist(id)),
            ),
            id: artist.id,
            name: artist.artist_name,
            sort_name: artist.sort_name,
            description: artist.description,
            credit: None,
            releases: None,
            tracks: None,
            relations: None,
            cover: None,
        }
    }
}

impl ArtistResponse {
    pub(crate) fn credited(
        credited_artist: ResolvedCreditedArtist,
        availability: &Availability,
    ) -> Self {
        let mut response = Self::new(credited_artist.artist, availability);
        response.credit = Some(ArtistCreditResponse {
            credit_type: credited_artist.credit.credit_type,
            detail: credited_artist.credit.detail,
            source: credited_artist.source.into(),
        });
        response
    }
}

impl ArtistResponse {
    pub(crate) fn unavailable(id: String) -> Self {
        Self {
            id,
            name: "unavailable".to_string(),
            available: None,
            sort_name: None,
            description: None,
            credit: None,
            releases: None,
            tracks: None,
            relations: None,
            cover: None,
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
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
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Filesystem modification time as an RFC3339 timestamp.")
    )]
    pub modified_at: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "When a sync first failed to find the file, as an RFC3339 timestamp; omitted while it is there."
        )
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub missing_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracks: Option<Vec<TrackResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub releases: Option<Vec<ReleaseResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artists: Option<Vec<ArtistResponse>>,
}

impl EntryResponse {
    pub fn from_entry(entry: lyra_db::Entry, include_full_path: bool) -> Self {
        Self {
            id: entry.id,
            full_path: include_full_path.then(|| entry.full_path.to_string_lossy().into_owned()),
            kind: entry.kind.to_string(),
            file_kind: entry.file_kind,
            name: entry.name,
            hash: entry.hash,
            size: entry.size,
            modified_at: crate::services::timestamps::unix_ms_to_rfc3339_u64(
                entry.file_modified_at_ms,
            ),
            missing_at: entry
                .missing_at_ms
                .map(crate::services::timestamps::unix_ms_to_rfc3339_u64),
            tracks: None,
            releases: None,
            artists: None,
        }
    }
}

impl From<lyra_db::Entry> for EntryResponse {
    fn from(entry: lyra_db::Entry) -> Self {
        Self::from_entry(entry, true)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serde_json::json;

    use super::*;

    #[test]
    fn entry_response_serializes_modified_at_as_rfc3339() -> anyhow::Result<()> {
        let value = serde_json::to_value(EntryResponse::from_entry(
            lyra_db::Entry {
                db_id: None,
                id: "entry".to_string(),
                full_path: PathBuf::from("/tmp/track.flac"),
                kind: lyra_db::entries::EntryKind::File,
                file_kind: Some("audio".to_string()),
                name: "track.flac".to_string(),
                hash: None,
                size: 123,
                file_modified_at_ms: 1_700_000_000_000,
                file_created_at_ms: 1_700_000_000_000,
                synced_at_ms: None,
                missing_at_ms: None,
            },
            false,
        ))?;

        assert_eq!(value["modified_at"], json!("2023-11-14T22:13:20Z"));
        Ok(())
    }
}
