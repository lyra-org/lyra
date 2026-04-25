// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod artists;
mod coalesce;
mod context;
mod filename;
mod path;
mod year;

pub use artists::{
    normalize_unicode_nfc,
    normalize_unicode_nfkc,
    split_artist_string,
    split_delimited_string,
    split_on_word,
};
pub use coalesce::coalesce_release_groups;
pub use context::{
    build_release_context_from_tags,
    build_release_context_from_tags_with_library_root,
};
pub use filename::fill_from_filename;
pub use path::{
    extract_lookup_hints_from_file_path,
    extract_lookup_hints_from_file_path_with_library_root,
    infer_lookup_hints_from_tracks,
};
pub use year::{
    extract_year,
    find_year_in,
};

use std::{
    cmp::Ordering,
    collections::HashSet,
    path::PathBuf,
};

use serde::{
    Deserialize,
    Serialize,
};

/// Bumped whenever the shipped default metadata mapping rules change.
/// Shared source of truth between the server extraction pipeline
/// and the harmony-test fixture runner.
pub const DEFAULT_MAPPING_VERSION: u64 = 1;

/// Raw tag values read directly from audio files, before any processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawTrackTags {
    pub file_path: String,
    pub album: Option<String>,
    #[serde(default)]
    pub album_artists: Vec<String>,
    #[serde(default)]
    pub artists: Vec<String>,
    pub title: Option<String>,
    pub date: Option<String>,
    pub copyright: Option<String>,
    pub genre: Option<String>,
    pub label: Option<String>,
    pub catalog_number: Option<String>,
    pub disc: Option<u32>,
    pub disc_total: Option<u32>,
    pub track: Option<u32>,
    pub track_total: Option<u32>,
    pub duration_ms: u64,
    #[serde(default)]
    pub sample_rate_hz: Option<u32>,
    #[serde(default)]
    pub channel_count: Option<u32>,
    #[serde(default)]
    pub bit_depth: Option<u32>,
    #[serde(default)]
    pub bitrate_bps: Option<u32>,
}

/// Processed track metadata (pure, no database IDs).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackMetadata {
    #[serde(skip)]
    pub id: i64,
    pub file_path: Option<String>,
    pub album: Option<String>,
    pub album_artists: Option<Vec<String>>,
    pub date: Option<String>,
    pub year: Option<u32>,
    pub title: Option<String>,
    pub artists: Option<Vec<String>>,
    pub disc: Option<u32>,
    pub disc_total: Option<u32>,
    pub track: Option<u32>,
    pub track_total: Option<u32>,
    pub duration_ms: Option<u64>,
    pub genres: Option<Vec<String>>,
    pub label: Option<String>,
    pub catalog_number: Option<String>,
    pub sample_rate_hz: Option<u32>,
    pub channel_count: Option<u32>,
    pub bit_depth: Option<u32>,
    pub bitrate_bps: Option<u32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct LookupHints {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artist_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub album_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub year: Option<u32>,
}

pub trait ReleaseCoalesceTrack: Clone {
    fn album(&self) -> Option<&str>;
    fn set_album(&mut self, album: Option<String>);
    fn album_artists(&self) -> Option<&[String]>;
    fn artists(&self) -> Option<&[String]>;
    fn year(&self) -> Option<u32>;
    fn disc(&self) -> Option<u32>;
    fn track(&self) -> Option<u32>;
}

impl ReleaseCoalesceTrack for TrackMetadata {
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

#[derive(Debug, Clone)]
pub struct ParsedReleaseGroup<T> {
    pub coalesce_group_key: usize,
    pub source_dir: String,
    pub tracks: Vec<T>,
}

/// Process raw tags into sorted, processed `TrackMetadata`.
/// Replicates the pure logic from `parse_metadata` without file I/O.
pub fn process_raw_tags(raw_tags: Vec<RawTrackTags>) -> Vec<TrackMetadata> {
    let mut paths: Vec<PathBuf> = Vec::new();
    let mut processed: Vec<TrackMetadata> = Vec::new();

    for raw in raw_tags {
        let album_artists_vec = split_artist_string(raw.album_artists);
        let artists_vec = split_artist_string(raw.artists);

        // If artists is still empty after split, leave as None
        let album_artists = if album_artists_vec.is_empty() {
            None
        } else {
            Some(album_artists_vec)
        };
        let artists = if artists_vec.is_empty() {
            None
        } else {
            Some(artists_vec)
        };

        let date = raw.date.as_deref().and_then(year::normalize_release_date);
        let year = extract_year(raw.date.as_deref(), raw.copyright.as_deref())
            .or_else(|| path::extract_year_from_file_path_for_canonical(&raw.file_path));

        let mut genres_vec: Vec<String> = raw.genre.map(|s| vec![s]).unwrap_or_default();
        genres_vec = split_delimited_string(genres_vec);
        let genres = if genres_vec.is_empty() {
            None
        } else {
            Some(genres_vec)
        };

        let meta = TrackMetadata {
            id: 0,
            file_path: Some(raw.file_path.clone()),
            album: raw.album,
            album_artists,
            date,
            year,
            title: raw.title,
            artists,
            disc: raw.disc,
            disc_total: raw.disc_total,
            track: raw.track,
            track_total: raw.track_total,
            duration_ms: Some(raw.duration_ms),
            genres,
            label: raw.label.and_then(|s| {
                let t = s.trim();
                if t.is_empty() {
                    None
                } else {
                    Some(t.to_string())
                }
            }),
            catalog_number: raw.catalog_number.and_then(|s| {
                let t = s.trim();
                if t.is_empty() {
                    None
                } else {
                    Some(t.to_string())
                }
            }),
            sample_rate_hz: raw.sample_rate_hz,
            channel_count: raw.channel_count,
            bit_depth: raw.bit_depth,
            bitrate_bps: raw.bitrate_bps,
        };

        paths.push(PathBuf::from(raw.file_path));
        processed.push(meta);
    }

    // Fill missing fields from filenames
    let known_artists: Vec<String> = processed
        .iter()
        .flat_map(|m| {
            m.artists
                .iter()
                .chain(m.album_artists.iter())
                .flat_map(|v| v.iter().cloned())
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    for (meta, path) in processed.iter_mut().zip(paths.iter()) {
        if meta.title.is_none()
            || meta.album.is_none()
            || meta.track.is_none()
            || meta.disc.is_none()
            || meta.artists.is_none()
        {
            fill_from_filename(meta, path, &known_artists);
        }

        let title = meta.title.clone();
        if let Some(artists) = meta.artists.as_mut() {
            artists::enrich_artists_with_title_features(artists, title.as_deref());
        }
    }

    // Sort by album_artists, year, album, disc, track, title
    processed.sort_by(|a, b| {
        let a_artist = a
            .album_artists
            .as_ref()
            .map(|v| v.join(", "))
            .unwrap_or_default()
            .to_ascii_lowercase();
        let b_artist = b
            .album_artists
            .as_ref()
            .map(|v| v.join(", "))
            .unwrap_or_default()
            .to_ascii_lowercase();
        let ord_artist = a_artist.cmp(&b_artist);
        if ord_artist != Ordering::Equal {
            return ord_artist;
        }

        let a_year = a.year.unwrap_or(0);
        let b_year = b.year.unwrap_or(0);
        let ord_year = a_year.cmp(&b_year);
        if ord_year != Ordering::Equal {
            return ord_year;
        }

        let a_album = a.album.as_deref().unwrap_or("").to_ascii_lowercase();
        let b_album = b.album.as_deref().unwrap_or("").to_ascii_lowercase();
        let ord_album = a_album.cmp(&b_album);
        if ord_album != Ordering::Equal {
            return ord_album;
        }

        let a_disc = a.disc.unwrap_or(1);
        let b_disc = b.disc.unwrap_or(1);
        let ord_disc = a_disc.cmp(&b_disc);
        if ord_disc != Ordering::Equal {
            return ord_disc;
        }

        let a_track = a.track.unwrap_or(0);
        let b_track = b.track.unwrap_or(0);
        let ord_track = a_track.cmp(&b_track);
        if ord_track != Ordering::Equal {
            return ord_track;
        }

        let a_title = a.title.as_deref().unwrap_or("").to_ascii_lowercase();
        let b_title = b.title.as_deref().unwrap_or("").to_ascii_lowercase();
        a_title.cmp(&b_title)
    });

    processed
}

#[cfg(test)]
mod tests {
    use super::{
        LookupHints,
        ParsedReleaseGroup,
        RawTrackTags,
        TrackMetadata,
        coalesce_release_groups,
        extract_lookup_hints_from_file_path,
        extract_lookup_hints_from_file_path_with_library_root,
        infer_lookup_hints_from_tracks,
        process_raw_tags,
    };
    use crate::artists::extract_featured_artists_from_title;

    #[test]
    fn extract_featured_artists_from_parenthesized_title() {
        let featured =
            extract_featured_artists_from_title("厭わない (feat. 富田美憂 & 市ノ瀬加那)");
        assert_eq!(featured, vec!["富田美憂", "市ノ瀬加那"]);
    }

    #[test]
    fn process_raw_tags_enriches_single_artist_with_title_features() {
        let raw = RawTrackTags {
            file_path: "/tmp/test.flac".to_string(),
            album: Some("厭わない".to_string()),
            album_artists: vec!["MIMiNARI".to_string()],
            artists: vec!["MIMiNARI".to_string()],
            title: Some("厭わない (feat. 富田美憂 & 市ノ瀬加那)".to_string()),
            date: Some("2023-01-17".to_string()),
            copyright: None,
            genre: None,
            label: None,
            catalog_number: None,
            disc: Some(1),
            disc_total: Some(1),
            track: Some(1),
            track_total: Some(1),
            duration_ms: 213_672,
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
        };

        let processed = process_raw_tags(vec![raw]);
        assert_eq!(processed.len(), 1);

        let artists = processed[0].artists.as_ref().expect("artists should exist");
        assert_eq!(
            artists,
            &vec![
                "MIMiNARI".to_string(),
                "富田美憂".to_string(),
                "市ノ瀬加那".to_string()
            ]
        );
    }

    #[test]
    fn process_raw_tags_does_not_append_when_multiple_artists_already_tagged() {
        let raw = RawTrackTags {
            file_path: "/tmp/test2.flac".to_string(),
            album: Some("厭わない".to_string()),
            album_artists: vec!["MIMiNARI".to_string()],
            artists: vec!["MIMiNARI".to_string(), "富田美憂".to_string()],
            title: Some("厭わない (feat. 富田美憂 & 市ノ瀬加那)".to_string()),
            date: Some("2023-01-17".to_string()),
            copyright: None,
            genre: None,
            label: None,
            catalog_number: None,
            disc: Some(1),
            disc_total: Some(1),
            track: Some(1),
            track_total: Some(1),
            duration_ms: 213_672,
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
        };

        let processed = process_raw_tags(vec![raw]);
        assert_eq!(processed.len(), 1);

        let artists = processed[0].artists.as_ref().expect("artists should exist");
        assert_eq!(
            artists,
            &vec!["MIMiNARI".to_string(), "富田美憂".to_string()]
        );
    }

    #[test]
    fn process_raw_tags_uses_file_path_year_when_tags_missing() {
        let raw = RawTrackTags {
            file_path: "/music/Aimer - Daydream (2016) [FLAC]/01 - Insane Dream.flac".to_string(),
            album: Some("Daydream".to_string()),
            album_artists: vec!["Aimer".to_string()],
            artists: vec!["Aimer".to_string()],
            title: Some("Insane Dream".to_string()),
            date: None,
            copyright: None,
            genre: None,
            label: None,
            catalog_number: None,
            disc: Some(1),
            disc_total: Some(1),
            track: Some(1),
            track_total: Some(10),
            duration_ms: 240_000,
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
        };

        let processed = process_raw_tags(vec![raw]);
        assert_eq!(processed.len(), 1);
        assert_eq!(processed[0].year, Some(2016));
    }

    #[test]
    fn process_raw_tags_prefers_tag_year_over_file_path_year() {
        let raw = RawTrackTags {
            file_path: "/music/Aimer - Daydream (2016) [FLAC]/01 - Insane Dream.flac".to_string(),
            album: Some("Daydream".to_string()),
            album_artists: vec!["Aimer".to_string()],
            artists: vec!["Aimer".to_string()],
            title: Some("Insane Dream".to_string()),
            date: Some("2020-01-01".to_string()),
            copyright: None,
            genre: None,
            label: None,
            catalog_number: None,
            disc: Some(1),
            disc_total: Some(1),
            track: Some(1),
            track_total: Some(10),
            duration_ms: 240_000,
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
        };

        let processed = process_raw_tags(vec![raw]);
        assert_eq!(processed.len(), 1);
        assert_eq!(processed[0].date.as_deref(), Some("2020-01-01"));
        assert_eq!(processed[0].year, Some(2020));
    }

    #[test]
    fn process_raw_tags_does_not_use_weak_file_path_year_pattern_for_canonical_year() {
        let raw = RawTrackTags {
            file_path: "/music/CAT2016 Edition/01 - Track.flac".to_string(),
            album: Some("Edition".to_string()),
            album_artists: vec!["Aimer".to_string()],
            artists: vec!["Aimer".to_string()],
            title: Some("Track".to_string()),
            date: None,
            copyright: None,
            genre: None,
            label: None,
            catalog_number: None,
            disc: Some(1),
            disc_total: Some(1),
            track: Some(1),
            track_total: Some(10),
            duration_ms: 240_000,
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
        };

        let processed = process_raw_tags(vec![raw]);
        assert_eq!(processed.len(), 1);
        assert_eq!(processed[0].year, None);
    }

    #[test]
    fn extract_lookup_hints_from_file_path_extracts_artist_album_and_year() {
        let hints = extract_lookup_hints_from_file_path(
            "/music/Aimer - Daydream (2016) [FLAC]/01 - Insane Dream.flac",
        );
        assert_eq!(
            hints,
            LookupHints {
                artist_name: Some("Aimer".to_string()),
                album_title: Some("Daydream".to_string()),
                year: Some(2016),
            }
        );
    }

    #[test]
    fn extract_lookup_hints_from_file_path_returns_year_only_when_no_dash_title() {
        let hints = extract_lookup_hints_from_file_path(
            "/music/[2017] 青春のエキサイトメント/01 track.flac",
        );
        assert_eq!(
            hints,
            LookupHints {
                artist_name: None,
                album_title: None,
                year: Some(2017),
            }
        );
    }

    #[test]
    fn extract_lookup_hints_from_windows_file_path_extracts_artist_album_and_year() {
        let hints = extract_lookup_hints_from_file_path(
            r"C:\music\Aimer - Daydream (2016) [FLAC]\01 - Insane Dream.flac",
        );
        assert_eq!(
            hints,
            LookupHints {
                artist_name: Some("Aimer".to_string()),
                album_title: Some("Daydream".to_string()),
                year: Some(2016),
            }
        );
    }

    #[test]
    fn extract_lookup_hints_from_file_path_extracts_artist_album_from_hierarchy() {
        let hints = extract_lookup_hints_from_file_path_with_library_root(
            "/music/Green Apelsin/Северный ветер [2021]/01 - Северный ветер.flac",
            Some("/music"),
        );
        assert_eq!(
            hints,
            LookupHints {
                artist_name: Some("Green Apelsin".to_string()),
                album_title: Some("Северный ветер".to_string()),
                year: Some(2021),
            }
        );
    }

    #[test]
    fn extract_lookup_hints_from_windows_file_path_extracts_artist_album_from_hierarchy() {
        let hints = extract_lookup_hints_from_file_path_with_library_root(
            r"C:\Users\User\moosic\Green Apelsin\Северный ветер [2021]\01 - Северный ветер.flac",
            Some(r"C:\Users\User\moosic"),
        );
        assert_eq!(
            hints,
            LookupHints {
                artist_name: Some("Green Apelsin".to_string()),
                album_title: Some("Северный ветер".to_string()),
                year: Some(2021),
            }
        );
    }

    #[test]
    fn extract_lookup_hints_with_nonmatching_root_handles_unicode_path() {
        let hints = extract_lookup_hints_from_file_path_with_library_root(
            "鈴木雅之 - ALL TIME ROCK 'N' ROLL [FLAC]/ESCL-5394/01.flac",
            Some("."),
        );
        assert_eq!(
            hints,
            LookupHints {
                artist_name: Some("鈴木雅之".to_string()),
                album_title: Some("ALL TIME ROCK 'N' ROLL".to_string()),
                year: None,
            }
        );
    }

    #[test]
    fn extract_lookup_hints_from_file_path_does_not_use_hierarchy_without_library_root() {
        let hints = extract_lookup_hints_from_file_path(
            "/music/Green Apelsin/Северный ветер [2021]/01 - Северный ветер.flac",
        );
        assert_eq!(
            hints,
            LookupHints {
                artist_name: None,
                album_title: None,
                year: Some(2021),
            }
        );
    }

    #[test]
    fn infer_lookup_hints_from_tracks_uses_first_artist_album_and_first_year() {
        let hints = infer_lookup_hints_from_tracks(&[
            LookupHints {
                artist_name: None,
                album_title: None,
                year: Some(2019),
            },
            LookupHints {
                artist_name: Some("Aimer".to_string()),
                album_title: Some("Daydream".to_string()),
                year: Some(2016),
            },
            LookupHints {
                artist_name: Some("Other".to_string()),
                album_title: Some("Other Album".to_string()),
                year: Some(2014),
            },
        ]);
        assert_eq!(
            hints,
            LookupHints {
                artist_name: Some("Aimer".to_string()),
                album_title: Some("Daydream".to_string()),
                year: Some(2019),
            }
        );
    }

    fn coalesce_track(
        file_path: &str,
        album: &str,
        artist: &str,
        year: Option<u32>,
        disc: Option<u32>,
        track: u32,
    ) -> TrackMetadata {
        TrackMetadata {
            id: 0,
            file_path: Some(file_path.to_string()),
            album: Some(album.to_string()),
            album_artists: Some(vec![artist.to_string()]),
            date: None,
            year,
            title: Some(format!("Track {track}")),
            artists: Some(vec![artist.to_string()]),
            disc,
            disc_total: None,
            track: Some(track),
            track_total: None,
            duration_ms: None,
            genres: None,
            label: None,
            catalog_number: None,
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
        }
    }

    fn parsed_group(
        coalesce_group_key: usize,
        source_dir: &str,
        tracks: Vec<TrackMetadata>,
    ) -> ParsedReleaseGroup<TrackMetadata> {
        ParsedReleaseGroup {
            coalesce_group_key,
            source_dir: source_dir.to_string(),
            tracks,
        }
    }

    #[test]
    fn coalesce_release_groups_merges_non_overlapping_discs_and_rewrites_album_title() {
        let groups = vec![
            parsed_group(
                0,
                "/music/release/DISC1",
                vec![coalesce_track(
                    "/music/release/DISC1/01.flac",
                    "Release Title",
                    "Artist",
                    Some(2024),
                    Some(1),
                    1,
                )],
            ),
            parsed_group(
                0,
                "/music/release/DISC2",
                vec![coalesce_track(
                    "/music/release/DISC2/01.flac",
                    "Bonus Disc Title",
                    "Artist",
                    Some(2024),
                    Some(2),
                    1,
                )],
            ),
        ];

        let coalesced = coalesce_release_groups(groups);
        assert_eq!(coalesced.len(), 1);
        assert_eq!(coalesced[0].len(), 2);
        assert!(
            coalesced[0]
                .iter()
                .all(|track| track.album.as_deref() == Some("Release Title"))
        );
    }

    #[test]
    fn coalesce_release_groups_merges_multi_artist_discs_without_consensus_artist() {
        let mut disc1_track1 = coalesce_track(
            "/music/release/DISC1/01.flac",
            "Compilation Title",
            "Artist A",
            Some(2024),
            Some(1),
            1,
        );
        disc1_track1.album_artists = None;
        let mut disc1_track2 = coalesce_track(
            "/music/release/DISC1/02.flac",
            "Compilation Title",
            "Artist B",
            Some(2024),
            Some(1),
            2,
        );
        disc1_track2.album_artists = None;

        let mut disc2_track1 = coalesce_track(
            "/music/release/DISC2/01.flac",
            "Compilation Bonus",
            "Artist C",
            Some(2024),
            Some(2),
            1,
        );
        disc2_track1.album_artists = None;
        let mut disc2_track2 = coalesce_track(
            "/music/release/DISC2/02.flac",
            "Compilation Bonus",
            "Artist D",
            Some(2024),
            Some(2),
            2,
        );
        disc2_track2.album_artists = None;

        let groups = vec![
            parsed_group(0, "/music/release/DISC1", vec![disc1_track1, disc1_track2]),
            parsed_group(0, "/music/release/DISC2", vec![disc2_track1, disc2_track2]),
        ];

        let coalesced = coalesce_release_groups(groups);
        assert_eq!(coalesced.len(), 1);
        assert_eq!(coalesced[0].len(), 4);
        assert!(
            coalesced[0]
                .iter()
                .all(|track| track.album.as_deref() == Some("Compilation Title"))
        );
    }

    #[test]
    fn coalesce_release_groups_vetoes_resolved_disc_track_overlap() {
        let groups = vec![
            parsed_group(
                0,
                "/music/release/DISC1",
                vec![coalesce_track(
                    "/music/release/DISC1/01.flac",
                    "Disc One",
                    "Artist",
                    Some(2024),
                    Some(1),
                    1,
                )],
            ),
            parsed_group(
                0,
                "/music/release/DISC2",
                vec![coalesce_track(
                    "/music/release/DISC2/01.flac",
                    "Disc Two",
                    "Artist",
                    Some(2024),
                    Some(1),
                    1,
                )],
            ),
        ];

        let coalesced = coalesce_release_groups(groups);
        assert_eq!(coalesced.len(), 2);
        assert!(coalesced.iter().all(|batch| batch.len() == 1));
    }

    #[test]
    fn coalesce_release_groups_uses_folder_fallback_for_missing_disc_overlap() {
        let groups = vec![
            parsed_group(
                0,
                "/music/release/DISC1",
                vec![coalesce_track(
                    "/music/release/DISC1/01.flac",
                    "Release Title",
                    "Artist",
                    Some(2024),
                    None,
                    1,
                )],
            ),
            parsed_group(
                0,
                "/music/release/DISC2",
                vec![coalesce_track(
                    "/music/release/DISC2/01.flac",
                    "Disc Two Title",
                    "Artist",
                    Some(2024),
                    None,
                    1,
                )],
            ),
        ];

        let coalesced = coalesce_release_groups(groups);
        assert_eq!(coalesced.len(), 1);
        assert_eq!(coalesced[0].len(), 2);
        assert!(
            coalesced[0]
                .iter()
                .all(|track| track.album.as_deref() == Some("Release Title"))
        );
    }

    #[test]
    fn coalesce_release_groups_splits_mixed_folder_titles_and_merges_matching_main_title() {
        let groups = vec![
            parsed_group(
                0,
                "/music/212",
                vec![
                    coalesce_track(
                        "/music/212/01.flac",
                        "212",
                        "nameless; とあ",
                        Some(2015),
                        None,
                        1,
                    ),
                    coalesce_track(
                        "/music/212/02.flac",
                        "212",
                        "nameless; とあ",
                        Some(2015),
                        None,
                        2,
                    ),
                ],
            ),
            parsed_group(
                0,
                "/music/212/Bonus",
                vec![
                    coalesce_track(
                        "/music/212/Bonus/01.flac",
                        "212 ボカロver.CD",
                        "nameless; とあ",
                        Some(2015),
                        None,
                        1,
                    ),
                    coalesce_track(
                        "/music/212/Bonus/02.flac",
                        "212 ボカロver.CD",
                        "nameless; とあ",
                        Some(2015),
                        None,
                        2,
                    ),
                    coalesce_track(
                        "/music/212/Bonus/13.flac",
                        "212",
                        "nameless; とあ",
                        Some(2015),
                        None,
                        13,
                    ),
                ],
            ),
        ];

        let coalesced = coalesce_release_groups(groups);
        assert_eq!(coalesced.len(), 2);

        let merged_main = coalesced
            .iter()
            .find(|batch| batch.iter().any(|track| track.track == Some(13)))
            .expect("expected main-release batch with track 13");
        let mut main_tracks: Vec<u32> =
            merged_main.iter().filter_map(|track| track.track).collect();
        main_tracks.sort_unstable();
        assert_eq!(main_tracks, vec![1, 2, 13]);
        assert!(
            merged_main
                .iter()
                .all(|track| track.album.as_deref() == Some("212"))
        );

        let bonus_only = coalesced
            .iter()
            .find(|batch| {
                batch
                    .iter()
                    .all(|track| track.album.as_deref() == Some("212 ボカロver.CD"))
            })
            .expect("expected separate bonus-release batch");
        let mut bonus_tracks: Vec<u32> =
            bonus_only.iter().filter_map(|track| track.track).collect();
        bonus_tracks.sort_unstable();
        assert_eq!(bonus_tracks, vec![1, 2]);
    }

    #[test]
    fn build_release_context_normalizes_album_title_to_nfc() {
        let processed = process_raw_tags(vec![
            RawTrackTags {
                file_path: "/music/unicode/01.flac".to_string(),
                album: Some("Cafe\u{301} Album".to_string()),
                album_artists: vec!["Artist".to_string()],
                artists: vec!["Artist".to_string()],
                title: Some("Track 1".to_string()),
                date: Some("2024".to_string()),
                copyright: None,
                genre: None,
                label: None,
                catalog_number: None,
                disc: Some(1),
                disc_total: Some(1),
                track: Some(1),
                track_total: Some(2),
                duration_ms: 60_000,
                sample_rate_hz: None,
                channel_count: None,
                bit_depth: None,
                bitrate_bps: None,
            },
            RawTrackTags {
                file_path: "/music/unicode/02.flac".to_string(),
                album: Some("Caf\u{e9} Album".to_string()),
                album_artists: vec!["Artist".to_string()],
                artists: vec!["Artist".to_string()],
                title: Some("Track 2".to_string()),
                date: Some("2024".to_string()),
                copyright: None,
                genre: None,
                label: None,
                catalog_number: None,
                disc: Some(1),
                disc_total: Some(1),
                track: Some(2),
                track_total: Some(2),
                duration_ms: 60_000,
                sample_rate_hz: None,
                channel_count: None,
                bit_depth: None,
                bitrate_bps: None,
            },
        ]);

        let context = super::build_release_context_from_tags(&processed);
        assert_eq!(
            context.get("album_title").and_then(|value| value.as_str()),
            Some("Caf\u{e9} Album")
        );
        assert_eq!(
            context.get("release_date").and_then(|value| value.as_str()),
            Some("2024")
        );
    }
}
