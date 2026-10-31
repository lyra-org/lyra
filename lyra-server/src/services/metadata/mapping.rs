// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashSet;

use lofty::{
    properties::FileProperties,
    tag::{
        Accessor,
        ItemKey,
        Tag,
    },
};
use lyra_metadata::{
    RawGainTags,
    RawTrackTags,
};

use lyra_db::metadata::mapping_config::{
    self,
    FieldName,
    MappingRule,
    MetadataMappingConfig,
};

/// Source keys a [`MappingRule`] may name: those that can populate a [`FieldName`].
pub(crate) const SOURCE_KEYS: &[(&str, ItemKey)] = &[
    ("AlbumTitle", ItemKey::AlbumTitle),
    ("OriginalAlbumTitle", ItemKey::OriginalAlbumTitle),
    ("SetSubtitle", ItemKey::SetSubtitle),
    ("ShowName", ItemKey::ShowName),
    ("ContentGroup", ItemKey::ContentGroup),
    ("Work", ItemKey::Work),
    ("TrackTitle", ItemKey::TrackTitle),
    ("TrackSubtitle", ItemKey::TrackSubtitle),
    ("Movement", ItemKey::Movement),
    ("AlbumArtist", ItemKey::AlbumArtist),
    ("AlbumArtists", ItemKey::AlbumArtists),
    ("TrackArtist", ItemKey::TrackArtist),
    ("TrackArtists", ItemKey::TrackArtists),
    ("OriginalArtist", ItemKey::OriginalArtist),
    ("Composer", ItemKey::Composer),
    ("Conductor", ItemKey::Conductor),
    ("Performer", ItemKey::Performer),
    ("Producer", ItemKey::Producer),
    ("Lyricist", ItemKey::Lyricist),
    ("Arranger", ItemKey::Arranger),
    ("Remixer", ItemKey::Remixer),
    ("Engineer", ItemKey::Engineer),
    ("Writer", ItemKey::Writer),
    ("Director", ItemKey::Director),
    ("RecordingDate", ItemKey::RecordingDate),
    ("ReleaseDate", ItemKey::ReleaseDate),
    ("OriginalReleaseDate", ItemKey::OriginalReleaseDate),
    ("Year", ItemKey::Year),
    ("CopyrightMessage", ItemKey::CopyrightMessage),
    ("Genre", ItemKey::Genre),
    ("Label", ItemKey::Label),
    ("CatalogNumber", ItemKey::CatalogNumber),
];

pub(crate) fn resolve_item_key(name: &str) -> Option<ItemKey> {
    SOURCE_KEYS
        .iter()
        .find(|(key_name, _)| *key_name == name)
        .map(|&(_, key)| key)
}

/// The stored rules, seeding [`default_config`] when none exist yet.
pub(crate) fn ensure_config(db: &mut agdb::DbAny) -> anyhow::Result<MetadataMappingConfig> {
    mapping_config::ensure(db, default_config)
}

/// Replaces the stored rules. Unknown `source_key`s are rejected here so
/// no caller can bypass validation.
pub(crate) fn update_config(
    db: &mut agdb::DbAny,
    config: &MetadataMappingConfig,
) -> anyhow::Result<()> {
    for rule in &config.rules {
        if resolve_item_key(&rule.source_key).is_none() {
            anyhow::bail!(
                "unsupported source_key '{}': not a recognised ItemKey variant",
                rule.source_key
            );
        }
    }
    mapping_config::update(db, config)
}

/// Fields without a [`FieldName`] (numbering, audio properties, edition
/// and gain tags) are read directly.
pub(crate) fn apply_mapping(
    tag: &Tag,
    properties: &FileProperties,
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
    // A blank title takes the inference path a missing one does.
    let title = title.filter(|value| !value.trim().is_empty());

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
        media: non_empty_string(tag, ItemKey::OriginalMediaType),
        barcode: non_empty_string(tag, ItemKey::Barcode),
        disc: tag.disk(),
        disc_total: tag.disk_total(),
        track: tag.track(),
        track_total: tag.track_total(),
        duration_ms: properties.duration().as_millis() as u64,
        gain: RawGainTags {
            replaygain_track_gain: non_empty_string(tag, ItemKey::ReplayGainTrackGain),
            replaygain_album_gain: non_empty_string(tag, ItemKey::ReplayGainAlbumGain),
            r128_track_gain: non_empty_string(tag, ItemKey::R128TrackGain),
            r128_album_gain: non_empty_string(tag, ItemKey::R128AlbumGain),
        },
        title_from_tags_only: false,
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

/// The album and artist key a track's mapped tags lack. Local inference
/// may still supply its title, but folder and file names are not safe
/// release keys.
pub(crate) fn missing_required_tags(raw: &RawTrackTags) -> Vec<MissingRequiredField> {
    let mut missing = Vec::new();
    if is_blank(raw.album.as_deref()) {
        missing.push(MissingRequiredField::Album);
    }
    if raw
        .artists
        .iter()
        .chain(&raw.album_artists)
        .all(|value| value.trim().is_empty())
    {
        missing.push(MissingRequiredField::ArtistKey);
    }
    missing
}

pub(crate) fn missing_title(title: Option<&str>) -> Option<MissingRequiredField> {
    is_blank(title).then_some(MissingRequiredField::Title)
}

#[derive(Debug)]
pub(crate) struct ProcessedTags {
    pub(crate) tracks: Vec<lyra_metadata::TrackMetadata>,
    pub(crate) rejected: Vec<RejectedTags>,
}

#[derive(Debug)]
pub(crate) struct RejectedTags {
    pub(crate) file_path: String,
    pub(crate) missing: Vec<MissingRequiredField>,
}

/// Processes a directory group's tags together, so filename artist matching
/// spans the group. Rejects tracks missing a required tag, and tracks whose
/// title may be inferred but wasn't.
pub(crate) fn process_tags(raw_tags: Vec<RawTrackTags>) -> ProcessedTags {
    let mut rejected = Vec::new();
    let mut accepted = Vec::with_capacity(raw_tags.len());
    for raw in raw_tags {
        let missing = missing_required_tags(&raw);
        if missing.is_empty() {
            accepted.push(raw);
        } else {
            rejected.push(RejectedTags {
                file_path: raw.file_path,
                missing,
            });
        }
    }
    let titles_from_tags_only: HashSet<String> = accepted
        .iter()
        .filter(|raw| raw.title_from_tags_only)
        .map(|raw| raw.file_path.clone())
        .collect();

    let mut tracks = Vec::with_capacity(accepted.len());
    for track in lyra_metadata::process_raw_tags(accepted) {
        let file_path = track.file_path.clone().unwrap_or_default();
        match missing_title(track.title.as_deref()) {
            Some(missing) if !titles_from_tags_only.contains(&file_path) => {
                rejected.push(RejectedTags {
                    file_path,
                    missing: vec![missing],
                });
            }
            _ => tracks.push(track),
        }
    }
    ProcessedTags { tracks, rejected }
}

fn is_blank(value: Option<&str>) -> bool {
    value.unwrap_or("").trim().is_empty()
}

fn non_empty_string(tag: &Tag, key: ItemKey) -> Option<String> {
    tag.get_string(key)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
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
                source_key: "RecordingDate".to_string(),
                destination: FieldName::Date,
            },
            MappingRule {
                source_key: "Year".to_string(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_key_returns_none() {
        assert!(resolve_item_key("NotARealKey").is_none());
        assert!(resolve_item_key("").is_none());
        assert!(resolve_item_key("album_title").is_none());
    }

    #[test]
    fn update_config_rejects_unknown_source_key() -> anyhow::Result<()> {
        let mut db = lyra_db::test_db::new_test_db()?;
        ensure_config(&mut db)?;
        let bogus = MetadataMappingConfig {
            rules: vec![MappingRule {
                source_key: "NotARealKey".to_string(),
                destination: FieldName::Album,
            }],
        };
        let err = update_config(&mut db, &bogus).unwrap_err();
        assert!(format!("{err}").contains("unsupported source_key"));
        Ok(())
    }

    /// Only FLAC fixtures exist, so other tag formats go unchecked here.
    #[test]
    fn apply_mapping_round_trips_flac_fixture() -> anyhow::Result<()> {
        use std::path::PathBuf;

        use lofty::{
            file::{
                AudioFile,
                TaggedFileExt,
            },
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
        let expected_date = [ItemKey::ReleaseDate, ItemKey::RecordingDate, ItemKey::Year]
            .into_iter()
            .find_map(|key| tag.get_string(key))
            .map(|s| s.to_string());
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
        let raw = apply_mapping(
            &tag,
            tagged_file.properties(),
            &fixture.to_string_lossy(),
            &config,
        );

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
        assert_eq!(missing_title(raw.title.as_deref()), None);
        assert!(missing_required_tags(&raw).is_empty());
        Ok(())
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
    fn source_key_names_are_unique() {
        let mut names: Vec<&str> = SOURCE_KEYS.iter().map(|&(name, _)| name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(
            names.len(),
            SOURCE_KEYS.len(),
            "SOURCE_KEYS contains duplicates"
        );
    }
}
