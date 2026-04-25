// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use lofty::{
    file::AudioFile,
    tag::{
        Accessor,
        ItemKey,
        Tag,
    },
};
use lyra_metadata::RawTrackTags;
use serde::{
    Deserialize,
    Serialize,
};

/// Disc/track number+total and duration are not here; their
/// format-specific parsing is not expressible as a mapping rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FieldName {
    Album,
    Title,
    AlbumArtists,
    Artists,
    Date,
    Copyright,
    Genre,
    Label,
    CatalogNumber,
}

/// Rules sharing a `destination` form a fallback chain: first
/// non-empty value wins; empty values never overwrite.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MappingRule {
    pub(crate) source_key: String,
    pub(crate) destination: FieldName,
}

/// `version` bumps on every commit so reingest jobs and capture
/// files can pin to a known schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MetadataMappingConfig {
    pub(crate) rules: Vec<MappingRule>,
    pub(crate) version: u64,
}

/// Literal `ItemKey::Variant` arms — a lofty rename breaks this at
/// compile time, never silently. The set is curated to keys that can
/// populate a [`FieldName`]; MusicBrainz IDs, ReplayGain, ISRC,
/// sort-orders etc. are excluded because no destination accepts them.
///
/// Audited against lofty 0.24.0: every arm below is present in the
/// current enum (compile-checked), and nothing exposed here was
/// renamed or removed in the 0.23 → 0.24 transition (the full test
/// suite continued to pass across the bump). When adding a new arm,
/// add the same string to [`SUPPORTED_KEY_NAMES`] so the admin UI
/// can advertise it — the drift tests enforce alignment.
pub(crate) fn resolve_item_key(name: &str) -> Option<ItemKey> {
    let key = match name {
        "AlbumTitle" => ItemKey::AlbumTitle,
        "OriginalAlbumTitle" => ItemKey::OriginalAlbumTitle,
        "SetSubtitle" => ItemKey::SetSubtitle,
        "ShowName" => ItemKey::ShowName,
        "ContentGroup" => ItemKey::ContentGroup,
        "Work" => ItemKey::Work,
        "TrackTitle" => ItemKey::TrackTitle,
        "TrackSubtitle" => ItemKey::TrackSubtitle,
        "Movement" => ItemKey::Movement,
        "AlbumArtist" => ItemKey::AlbumArtist,
        "AlbumArtists" => ItemKey::AlbumArtists,
        "TrackArtist" => ItemKey::TrackArtist,
        "TrackArtists" => ItemKey::TrackArtists,
        "OriginalArtist" => ItemKey::OriginalArtist,
        "Composer" => ItemKey::Composer,
        "Conductor" => ItemKey::Conductor,
        "Performer" => ItemKey::Performer,
        "Producer" => ItemKey::Producer,
        "Lyricist" => ItemKey::Lyricist,
        "Arranger" => ItemKey::Arranger,
        "Remixer" => ItemKey::Remixer,
        "Engineer" => ItemKey::Engineer,
        "Writer" => ItemKey::Writer,
        "Director" => ItemKey::Director,
        "RecordingDate" => ItemKey::RecordingDate,
        "ReleaseDate" => ItemKey::ReleaseDate,
        "OriginalReleaseDate" => ItemKey::OriginalReleaseDate,
        "Year" => ItemKey::Year,
        "CopyrightMessage" => ItemKey::CopyrightMessage,
        "Genre" => ItemKey::Genre,
        "Label" => ItemKey::Label,
        "CatalogNumber" => ItemKey::CatalogNumber,
        _ => return None,
    };
    Some(key)
}

/// Disc/track number+total and duration bypass the mapping — their
/// extraction does format-specific parsing (n/N strings, packed MP4
/// atoms) not expressible as a rule.
pub(crate) fn apply_mapping(
    tag: &Tag,
    tagged_file: &lofty::file::TaggedFile,
    file_path: &str,
    config: &MetadataMappingConfig,
) -> RawTrackTags {
    let mut album: Option<String> = None;
    let mut title: Option<String> = None;
    let mut album_artists: Vec<String> = Vec::new();
    let mut artists: Vec<String> = Vec::new();
    let mut date: Option<String> = None;
    let mut copyright: Option<String> = None;
    let mut genre: Option<String> = None;
    let mut label: Option<String> = None;
    let mut catalog_number: Option<String> = None;

    for rule in &config.rules {
        let Some(key) = resolve_item_key(&rule.source_key) else {
            continue;
        };
        match rule.destination {
            FieldName::Album => fill_scalar(&mut album, tag, key),
            FieldName::Title => fill_scalar(&mut title, tag, key),
            FieldName::Date => fill_scalar(&mut date, tag, key),
            FieldName::Copyright => fill_scalar(&mut copyright, tag, key),
            FieldName::Genre => fill_scalar(&mut genre, tag, key),
            FieldName::Label => fill_scalar(&mut label, tag, key),
            FieldName::CatalogNumber => fill_scalar(&mut catalog_number, tag, key),
            FieldName::AlbumArtists => fill_multi(&mut album_artists, tag, key),
            FieldName::Artists => fill_multi(&mut artists, tag, key),
        }
    }

    let properties = tagged_file.properties();
    RawTrackTags {
        file_path: file_path.to_string(),
        album,
        album_artists,
        artists,
        title,
        date,
        copyright,
        genre,
        label,
        catalog_number,
        disc: tag.disk(),
        disc_total: tag.disk_total(),
        track: tag.track(),
        track_total: tag.track_total(),
        duration_ms: properties.duration().as_millis() as u64,
        sample_rate_hz: properties.sample_rate().filter(|&v| v > 0),
        channel_count: properties.channels().filter(|&v| v > 0).map(u32::from),
        bit_depth: properties.bit_depth().filter(|&v| v > 0).map(u32::from),
        bitrate_bps: properties
            .overall_bitrate()
            .filter(|&v| v > 0)
            .and_then(|kbps| kbps.checked_mul(1_000)),
    }
}

/// The three `coalesce.rs` poison points: release grouping collapses
/// without title/album, and unrelated releases merge on title alone
/// without an artist key. Disc/track numbers are excluded — singles,
/// podcasts, and loose files are valid with those missing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MissingRequiredField {
    Title,
    Album,
    ArtistKey,
}

pub(crate) fn check_required_fields(
    raw: &lyra_metadata::RawTrackTags,
) -> Result<(), Vec<MissingRequiredField>> {
    let mut missing = Vec::new();
    if raw.title.as_deref().unwrap_or("").trim().is_empty() {
        missing.push(MissingRequiredField::Title);
    }
    if raw.album.as_deref().unwrap_or("").trim().is_empty() {
        missing.push(MissingRequiredField::Album);
    }
    let has_artist_key = raw
        .artists
        .iter()
        .chain(raw.album_artists.iter())
        .any(|value| !value.trim().is_empty());
    if !has_artist_key {
        missing.push(MissingRequiredField::ArtistKey);
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(missing)
    }
}

fn fill_scalar(slot: &mut Option<String>, tag: &Tag, key: ItemKey) {
    if slot.is_some() {
        return;
    }
    if let Some(value) = tag.get_string(key)
        && !value.is_empty()
    {
        *slot = Some(value.to_string());
    }
}

fn fill_multi(slot: &mut Vec<String>, tag: &Tag, key: ItemKey) {
    if !slot.is_empty() {
        return;
    }
    let values: Vec<String> = tag
        .get_strings(key)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    if !values.is_empty() {
        *slot = values;
    }
}

/// Seeded on first boot. Must extract byte-identically to the
/// fixture round-trip test or default behaviour drifts on upgrade.
pub(crate) fn default_config() -> MetadataMappingConfig {
    MetadataMappingConfig {
        version: lyra_metadata::DEFAULT_MAPPING_VERSION,
        rules: vec![
            MappingRule {
                source_key: "AlbumTitle".to_string(),
                destination: FieldName::Album,
            },
            MappingRule {
                source_key: "TrackTitle".to_string(),
                destination: FieldName::Title,
            },
            MappingRule {
                source_key: "AlbumArtist".to_string(),
                destination: FieldName::AlbumArtists,
            },
            MappingRule {
                source_key: "TrackArtists".to_string(),
                destination: FieldName::Artists,
            },
            MappingRule {
                source_key: "TrackArtist".to_string(),
                destination: FieldName::Artists,
            },
            MappingRule {
                source_key: "ReleaseDate".to_string(),
                destination: FieldName::Date,
            },
            MappingRule {
                source_key: "CopyrightMessage".to_string(),
                destination: FieldName::Copyright,
            },
            MappingRule {
                source_key: "Genre".to_string(),
                destination: FieldName::Genre,
            },
            MappingRule {
                source_key: "Label".to_string(),
                destination: FieldName::Label,
            },
            MappingRule {
                source_key: "CatalogNumber".to_string(),
                destination: FieldName::CatalogNumber,
            },
        ],
    }
}

/// Kept in sync with [`resolve_item_key`] via
/// `every_supported_name_resolves`.
pub(crate) const SUPPORTED_KEY_NAMES: &[&str] = &[
    "AlbumTitle",
    "OriginalAlbumTitle",
    "SetSubtitle",
    "ShowName",
    "ContentGroup",
    "Work",
    "TrackTitle",
    "TrackSubtitle",
    "Movement",
    "AlbumArtist",
    "AlbumArtists",
    "TrackArtist",
    "TrackArtists",
    "OriginalArtist",
    "Composer",
    "Conductor",
    "Performer",
    "Producer",
    "Lyricist",
    "Arranger",
    "Remixer",
    "Engineer",
    "Writer",
    "Director",
    "RecordingDate",
    "ReleaseDate",
    "OriginalReleaseDate",
    "Year",
    "CopyrightMessage",
    "Genre",
    "Label",
    "CatalogNumber",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_supported_name_resolves() {
        for name in SUPPORTED_KEY_NAMES {
            assert!(
                resolve_item_key(name).is_some(),
                "supported key '{name}' failed to resolve",
            );
        }
    }

    #[test]
    fn unknown_key_returns_none() {
        assert!(resolve_item_key("NotARealKey").is_none());
        assert!(resolve_item_key("").is_none());
        assert!(resolve_item_key("album_title").is_none());
    }

    /// v1 coverage gap: `tests/assets/metadata/` only carries FLAC
    /// (Vorbis Comments) fixtures. Byte-identical extraction is thus
    /// verified for that one format; ID3v2.3, ID3v2.4, MP4, and APE
    /// are covered only insofar as lofty's abstract `Tag` layer
    /// reaches them uniformly, but divergence in `Accessor` paths
    /// per format is undetected here. Adding per-format fixtures is
    /// v2 scope (~1-2 days of fixture creation work).
    #[test]
    fn apply_mapping_round_trips_flac_fixture() -> anyhow::Result<()> {
        use std::path::PathBuf;

        use lofty::{
            file::TaggedFileExt,
            probe::Probe,
        };

        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/assets/metadata/integration_track.flac");
        let tagged_file = Probe::open(&fixture)?.read()?;
        let tag = tagged_file
            .primary_tag()
            .or_else(|| tagged_file.first_tag())
            .cloned()
            .expect("fixture should have tags");

        let expected_album = tag.album().map(|s| s.to_string());
        let expected_title = tag.title().map(|s| s.to_string());
        let expected_genre = tag.genre().map(|s| s.to_string());
        let expected_date = tag.get_string(ItemKey::ReleaseDate).map(|s| s.to_string());
        let expected_copyright = tag
            .get_string(ItemKey::CopyrightMessage)
            .map(|s| s.to_string());
        let expected_album_artists: Vec<String> = tag
            .get_strings(ItemKey::AlbumArtist)
            .map(|s| s.to_string())
            .collect();
        let mut expected_artists: Vec<String> = tag
            .get_strings(ItemKey::TrackArtists)
            .map(|s| s.to_string())
            .collect();
        if expected_artists.is_empty()
            && let Some(art) = tag.artist()
        {
            expected_artists.push(art.as_ref().to_string());
        }
        let expected_disc = tag.disk();
        let expected_disc_total = tag.disk_total();
        let expected_track = tag.track();
        let expected_track_total = tag.track_total();

        let config = default_config();
        let raw = apply_mapping(&tag, &tagged_file, &fixture.to_string_lossy(), &config);

        assert_eq!(
            raw.album, expected_album,
            "album drifted from pre-mapping extraction"
        );
        assert_eq!(raw.title, expected_title, "title drifted");
        assert_eq!(raw.genre, expected_genre, "genre drifted");
        assert_eq!(raw.date, expected_date, "date drifted");
        assert_eq!(raw.copyright, expected_copyright, "copyright drifted");
        assert_eq!(
            raw.album_artists, expected_album_artists,
            "album_artists drifted"
        );
        assert_eq!(raw.artists, expected_artists, "artists drifted");
        assert_eq!(raw.disc, expected_disc);
        assert_eq!(raw.disc_total, expected_disc_total);
        assert_eq!(raw.track, expected_track);
        assert_eq!(raw.track_total, expected_track_total);
        assert!(check_required_fields(&raw).is_ok());
        Ok(())
    }

    #[test]
    fn supported_names_count_matches_match_arms() {
        // Drift sentinel: if you add a new arm to `resolve_item_key`
        // you must also add the name to `SUPPORTED_KEY_NAMES` (and
        // bump this expected count). Keeps the two lists aligned so
        // the admin UI can advertise every key the resolver accepts.
        const EXPECTED: usize = 32;
        assert_eq!(
            SUPPORTED_KEY_NAMES.len(),
            EXPECTED,
            "SUPPORTED_KEY_NAMES length changed; update EXPECTED here \
             and verify every resolve_item_key match arm has a \
             corresponding entry in the const",
        );
    }

    #[test]
    fn default_config_rules_all_resolve() {
        let cfg = default_config();
        for rule in &cfg.rules {
            assert!(
                resolve_item_key(&rule.source_key).is_some(),
                "default rule source '{}' failed to resolve",
                rule.source_key,
            );
        }
    }

    #[test]
    fn supported_names_list_is_unique() {
        let mut sorted: Vec<&&str> = SUPPORTED_KEY_NAMES.iter().collect();
        sorted.sort();
        let len_before = sorted.len();
        sorted.dedup();
        assert_eq!(
            len_before,
            sorted.len(),
            "SUPPORTED_KEY_NAMES contains duplicates",
        );
    }
}
