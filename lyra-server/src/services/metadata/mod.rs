// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub(crate) mod cleanup;
mod cue;
pub(crate) mod editing;
mod format;
pub(crate) mod id_links;
pub(crate) mod ingestion;
pub(crate) mod layers;
pub(crate) mod lyrics;
pub(crate) mod mapping;
pub(crate) mod mapping_admin;
pub(crate) mod merging;
mod model;

use std::collections::{
    HashMap,
    HashSet,
    VecDeque,
};

use agdb::DbId;
use anyhow::{
    Context,
    anyhow,
};
use lofty::{
    config::ParseOptions,
    file::{
        AudioFile,
        FileType,
        TaggedFile,
        TaggedFileExt,
    },
    flac::FlacFile,
    iff::{
        aiff::AiffFile,
        wav::WavFile,
    },
    mp4::Mp4File,
    mpeg::MpegFile,
    musepack::MpcFile,
    ogg::{
        OpusFile,
        VorbisFile,
        tag::VorbisComments,
    },
    probe::Probe,
    properties::FileProperties,
    tag::{
        ItemKey,
        Tag,
    },
};
use lyra_metadata::normalize_unicode_nfc;

pub(crate) use cue::sort_track_metadata;
use cue::{
    build_audio_lookup,
    build_embedded_source_key,
    merge_embedded_into_cue_metadata,
    parse_cue_metadata_for_entry,
};
use lyra_db::Entry;
use lyra_db::metadata::mapping_config::MetadataMappingConfig;
use lyra_db::track_sources::SourceFormat;
pub(crate) use model::TrackMetadata;

const SOURCE_KIND_EMBEDDED_TAGS: &str = "embedded_tags";

fn classify_entry_file_kind(entry: &Entry) -> Option<String> {
    if entry.kind != lyra_db::entries::EntryKind::File {
        return None;
    }

    entry
        .file_kind
        .clone()
        .or_else(|| lyra_db::entries::classify_file_kind(&entry.full_path).map(str::to_string))
}

pub(crate) fn read_audio_tags(
    path: std::path::PathBuf,
) -> anyhow::Result<(Tag, FileProperties, SourceFormat)> {
    let (mut tagged_file, fallbacks, format) = read_tagged_file(&path, true)
        .with_context(|| format!("failed to read file: {}", path.display()))?;

    let tag_type = tagged_file
        .primary_tag()
        .or_else(|| tagged_file.first_tag())
        .map(Tag::tag_type);
    let mut tag = tag_type
        .and_then(|tag_type| tagged_file.remove(tag_type))
        .ok_or_else(|| anyhow!("no tags found in {}", path.display()))?;

    for (key, value) in [
        (ItemKey::Barcode, fallbacks.barcode),
        (ItemKey::OriginalMediaType, fallbacks.media),
    ] {
        if let Some(value) = value
            && tag.get_string(key).is_none()
        {
            tag.insert_text(key, value);
        }
    }

    Ok((tag, tagged_file.properties().clone(), format))
}

/// A file's container, codec and audio properties, read the way a scan reads them.
pub(crate) fn read_source_format(path: &std::path::Path) -> anyhow::Result<SourceFormat> {
    let (_, _, format) = read_tagged_file(path, false)
        .with_context(|| format!("failed to read file: {}", path.display()))?;
    Ok(format)
}

/// Vorbis comments lofty has no `ItemKey` for, so its generic `Tag` discards them.
#[derive(Default)]
struct VorbisFallbacks {
    /// `UPC`, where lofty only maps `BARCODE`.
    barcode: Option<String>,
    /// `MEDIATYPE` (Mp3tag, One Tagger) or `SOURCEMEDIA`, where lofty only maps `MEDIA`.
    media: Option<String>,
}

impl VorbisFallbacks {
    fn read(comments: Option<&VorbisComments>) -> Self {
        let Some(comments) = comments else {
            return Self::default();
        };
        let first = |keys: &[&str]| {
            keys.iter()
                .filter_map(|key| comments.get(key))
                .map(str::trim)
                .find(|value| !value.is_empty())
                .map(str::to_string)
        };
        Self {
            barcode: first(&["UPC"]),
            media: first(&["MEDIATYPE", "SOURCEMEDIA"]),
        }
    }
}

/// Reads Vorbis comment files through their concrete types to keep [`VorbisFallbacks`], and files
/// whose type holds several codecs through theirs to learn the codec. Without `read_tags`, malformed
/// tags can't fail the read.
fn read_tagged_file(
    path: &std::path::Path,
    read_tags: bool,
) -> anyhow::Result<(TaggedFile, VorbisFallbacks, SourceFormat)> {
    // The contents decide the type; the extension counts only when they don't.
    let probe = Probe::open(path)
        .with_context(|| format!("bad path: {}", path.display()))?
        .guess_file_type()?;

    let options = ParseOptions::new()
        .read_tags(read_tags)
        .read_cover_art(false);
    let Some(file_type) = probe.file_type() else {
        let file = probe.options(options).read()?;
        let format = format::with_properties(SourceFormat::default(), file.properties());
        return Ok((file, VorbisFallbacks::default(), format));
    };
    let by_type = format::of_file_type(file_type);
    let (file, fallbacks, format): (TaggedFile, _, _) = match file_type {
        FileType::Flac => {
            let file = FlacFile::read_from(&mut probe.into_inner(), options)?;
            let fallbacks = VorbisFallbacks::read(file.vorbis_comments());
            (file.into(), fallbacks, by_type)
        }
        FileType::Opus => {
            let file = OpusFile::read_from(&mut probe.into_inner(), options)?;
            let fallbacks = VorbisFallbacks::read(Some(file.vorbis_comments()));
            (file.into(), fallbacks, by_type)
        }
        FileType::Vorbis => {
            let file = VorbisFile::read_from(&mut probe.into_inner(), options)?;
            let fallbacks = VorbisFallbacks::read(Some(file.vorbis_comments()));
            (file.into(), fallbacks, by_type)
        }
        FileType::Mp4 => {
            let file = Mp4File::read_from(&mut probe.into_inner(), options)?;
            let format = format::of_mp4(file.properties());
            (file.into(), VorbisFallbacks::default(), format)
        }
        FileType::Mpeg => {
            let file = MpegFile::read_from(&mut probe.into_inner(), options)?;
            let format = format::of_mpeg(file.properties());
            (file.into(), VorbisFallbacks::default(), format)
        }
        FileType::Mpc => {
            let file = MpcFile::read_from(&mut probe.into_inner(), options)?;
            let format = format::of_mpc(file.properties());
            (file.into(), VorbisFallbacks::default(), format)
        }
        FileType::Wav => {
            let file = WavFile::read_from(&mut probe.into_inner(), options)?;
            let format = format::of_wav(file.properties());
            (file.into(), VorbisFallbacks::default(), format)
        }
        FileType::Aiff => {
            let file = AiffFile::read_from(&mut probe.into_inner(), options)?;
            let format = format::of_aiff(file.properties());
            (file.into(), VorbisFallbacks::default(), format)
        }
        _ => (
            probe.options(options).read()?,
            VorbisFallbacks::default(),
            by_type,
        ),
    };
    let format = format::with_properties(format, file.properties());
    Ok((file, fallbacks, format))
}

#[derive(Debug, Clone)]
pub(crate) enum SkipReason {
    ReadFailed(ReadError),
    TaskFailed(String),
    /// Prevents silent coalesce corruption downstream.
    RequiredFieldEmpty(Vec<mapping::MissingRequiredField>),
    /// Audio entries were claimed by an earlier cue in the batch.
    CueAlreadyClaimed,
    CueParseFailed(ReadError),
}

impl SkipReason {
    /// Whether reading the file again, unchanged, may succeed.
    pub(crate) fn is_transient(&self) -> bool {
        match self {
            Self::ReadFailed(error) | Self::CueParseFailed(error) => error.transient,
            Self::TaskFailed(_) => true,
            Self::RequiredFieldEmpty(_) | Self::CueAlreadyClaimed => false,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ReadError {
    pub(crate) message: String,
    /// The file couldn't be read at all, rather than holding contents that can't be parsed.
    pub(crate) transient: bool,
}

impl From<&anyhow::Error> for ReadError {
    fn from(err: &anyhow::Error) -> Self {
        let transient = err
            .chain()
            .find_map(|cause| cause.downcast_ref::<std::io::Error>())
            .is_some_and(|io| {
                !matches!(
                    io.kind(),
                    std::io::ErrorKind::InvalidData | std::io::ErrorKind::UnexpectedEof
                )
            });
        Self {
            message: format!("{err:#}"),
            transient,
        }
    }
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SkipRecord {
    pub(crate) path: std::path::PathBuf,
    pub(crate) reason: SkipReason,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ParseMetadataOutput {
    pub(crate) metadata: Vec<TrackMetadata>,
    pub(crate) skipped: Vec<SkipRecord>,
}

pub(crate) fn log_skip_summary(skipped: &[SkipRecord]) {
    if skipped.is_empty() {
        return;
    }
    let total = skipped.len();
    let sample: Vec<String> = skipped
        .iter()
        .take(10)
        .map(|record| {
            format!(
                "{} ({})",
                record.path.display(),
                skip_reason_summary(&record.reason),
            )
        })
        .collect();
    tracing::warn!(
        skipped_total = total,
        sample = ?sample,
        "parse_metadata skipped files",
    );
}

pub(crate) fn skip_reason_summary(reason: &SkipReason) -> String {
    match reason {
        SkipReason::ReadFailed(error) => format!("read_failed: {error}"),
        SkipReason::TaskFailed(error) => format!("task_failed: {error}"),
        SkipReason::RequiredFieldEmpty(missing) => {
            format!("required_field_empty: {} missing fields", missing.len())
        }
        SkipReason::CueAlreadyClaimed => "cue_already_claimed".to_string(),
        SkipReason::CueParseFailed(error) => format!("cue_parse_failed: {error}"),
    }
}

pub(crate) async fn load_mapping_config(
    db: &lyra_db::DbAsync,
) -> anyhow::Result<MetadataMappingConfig> {
    {
        let db_read = db.read().await;
        if let Some(cfg) = lyra_db::metadata::mapping_config::get(&db_read)? {
            return Ok(cfg);
        }
    }
    let mut db_write = db.write().await;
    mapping::ensure_config(&mut db_write)
}

pub(crate) async fn parse_metadata(
    mapping_config: &MetadataMappingConfig,
    entries: Vec<Entry>,
) -> anyhow::Result<ParseMetadataOutput> {
    let mut audio_entries = Vec::new();
    let mut cue_entries = Vec::new();

    for entry in entries {
        if entry.kind != lyra_db::entries::EntryKind::File {
            continue;
        }

        match classify_entry_file_kind(&entry).as_deref() {
            Some("audio") => audio_entries.push(entry),
            Some("cue") => cue_entries.push(entry),
            _ => {}
        }
    }

    let audio_lookup = build_audio_lookup(&audio_entries);

    let mut skipped: Vec<SkipRecord> = Vec::new();
    let mut cue_metadata = Vec::new();
    let mut cue_claimed_audio_entries = HashSet::new();
    for cue_entry in &cue_entries {
        match parse_cue_metadata_for_entry(
            cue_entry,
            &audio_lookup.by_path,
            &audio_lookup.by_name,
            &audio_lookup.by_stem,
        )
        .await
        {
            Ok((metadata, claimed_entries)) => {
                if claimed_entries
                    .iter()
                    .any(|id| cue_claimed_audio_entries.contains(id))
                {
                    tracing::warn!(
                        path = %cue_entry.full_path.display(),
                        "skipping cue file because its audio entries are already claimed by another cue"
                    );
                    skipped.push(SkipRecord {
                        path: cue_entry.full_path.clone(),
                        reason: SkipReason::CueAlreadyClaimed,
                    });
                    continue;
                }
                cue_metadata.extend(metadata);
                cue_claimed_audio_entries.extend(claimed_entries);
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    path = %cue_entry.full_path.display(),
                    "skipping cue metadata for file"
                );
                skipped.push(SkipRecord {
                    path: cue_entry.full_path.clone(),
                    reason: SkipReason::CueParseFailed((&err).into()),
                });
            }
        }
    }

    let mut entry_ids_by_path: HashMap<String, VecDeque<(DbId, String, SourceFormat)>> =
        HashMap::new();
    let mut raw_tags = Vec::new();

    for entry in audio_entries {
        let Some(entry_db_id) = entry.db_id else {
            continue;
        };

        let path = entry.full_path.clone();
        let path_for_task = path.clone();
        let task_result = tokio::task::spawn_blocking(move || read_audio_tags(path_for_task)).await;
        let (tag, properties, format) = match task_result {
            Ok(Ok(result)) => result,
            Ok(Err(err)) => {
                tracing::warn!(
                    error = %err,
                    path = %path.display(),
                    "skipping metadata for file"
                );
                skipped.push(SkipRecord {
                    path: path.clone(),
                    reason: SkipReason::ReadFailed((&err).into()),
                });
                continue;
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    path = %path.display(),
                    "metadata task failed"
                );
                skipped.push(SkipRecord {
                    path: path.clone(),
                    reason: SkipReason::TaskFailed(err.to_string()),
                });
                continue;
            }
        };
        let file_path = path.to_string_lossy().to_string();
        let mut raw = mapping::apply_mapping(&tag, &properties, &file_path, mapping_config);
        // Embedded tags override a cue's own, so an untitled cue-backed file's
        // name must not supply the title or the artists derived from it, nor
        // its path a year over the cue's.
        let cue_claimed = cue_claimed_audio_entries.contains(&entry_db_id);
        raw.title_from_tags_only =
            cue_claimed && mapping::missing_title(raw.title.as_deref()).is_some();
        raw.year_from_tags_only = cue_claimed;
        entry_ids_by_path
            .entry(raw.file_path.clone())
            .or_default()
            .push_back((entry_db_id, entry.id.clone(), format));
        raw_tags.push(raw);
    }

    let processed = mapping::process_tags(raw_tags);
    for rejected in processed.rejected {
        skip_required_field_empty(
            &mut skipped,
            std::path::Path::new(&rejected.file_path),
            rejected.missing,
        );
    }
    let mut metadata = Vec::with_capacity(processed.tracks.len() + cue_metadata.len());
    let mut embedded_by_entry_id: HashMap<DbId, TrackMetadata> = HashMap::new();

    for track in processed.tracks {
        let path = track
            .file_path
            .clone()
            .ok_or_else(|| anyhow!("processed metadata missing file_path"))?;
        let (entry_db_id, entry_public_id, format) = entry_ids_by_path
            .get_mut(&path)
            .and_then(|ids| ids.pop_front())
            .ok_or_else(|| anyhow!("missing entry id mapping for processed track: {path}"))?;

        let embedded = embedded_track_metadata(entry_db_id, &entry_public_id, format, track);

        if cue_claimed_audio_entries.contains(&entry_db_id) {
            embedded_by_entry_id.entry(entry_db_id).or_insert(embedded);
        } else {
            metadata.push(embedded);
        }
    }

    // A single embedded tag set on one audio container should not replace
    // per-track cue titles/artists when that container is split into many tracks.
    let mut cue_tracks_per_entry: HashMap<DbId, usize> = HashMap::new();
    for cue_track in &cue_metadata {
        *cue_tracks_per_entry
            .entry(cue_track.entry_db_id)
            .or_default() += 1;
    }

    for cue_track in &mut cue_metadata {
        if let Some(embedded) = embedded_by_entry_id.get(&cue_track.entry_db_id) {
            let preserve_cue_track_identity = cue_tracks_per_entry
                .get(&cue_track.entry_db_id)
                .copied()
                .unwrap_or_default()
                > 1;
            merge_embedded_into_cue_metadata(cue_track, embedded, preserve_cue_track_identity);
        }
    }

    let (cue_metadata, untitled): (Vec<_>, Vec<_>) = cue_metadata.into_iter().partition(|track| {
        track
            .album
            .as_deref()
            .is_some_and(|album| !album.trim().is_empty())
    });
    let untitled_sheets = untitled
        .iter()
        .filter_map(|track| track.cue_sheet_entry_id)
        .collect::<HashSet<_>>();
    for cue_entry in &cue_entries {
        if cue_entry
            .db_id
            .is_some_and(|entry_id| untitled_sheets.contains(&entry_id))
        {
            skip_required_field_empty(
                &mut skipped,
                &cue_entry.full_path,
                vec![mapping::MissingRequiredField::Album],
            );
        }
    }

    metadata.extend(cue_metadata);
    sort_track_metadata(&mut metadata);
    Ok(ParseMetadataOutput { metadata, skipped })
}

fn skip_required_field_empty(
    skipped: &mut Vec<SkipRecord>,
    path: &std::path::Path,
    missing: Vec<mapping::MissingRequiredField>,
) {
    tracing::warn!(
        path = %path.display(),
        missing = ?missing,
        "skipping track: required fields empty"
    );
    skipped.push(SkipRecord {
        path: path.to_path_buf(),
        reason: SkipReason::RequiredFieldEmpty(missing),
    });
}

/// A track's embedded tags as the scanner ingests them, its text NFC-normalized.
pub(crate) fn embedded_track_metadata(
    entry_db_id: DbId,
    entry_public_id: &str,
    format: SourceFormat,
    track: lyra_metadata::TrackMetadata,
) -> TrackMetadata {
    TrackMetadata {
        entry_db_id,
        format,
        album: track.album.map(|value| normalize_unicode_nfc(&value)),
        album_artists: track.album_artists.map(|artists| {
            artists
                .into_iter()
                .map(|value| normalize_unicode_nfc(&value))
                .collect()
        }),
        date: track.date,
        year: track.year,
        title: track.title.map(|value| normalize_unicode_nfc(&value)),
        artists: track.artists.map(|artists| {
            artists
                .into_iter()
                .map(|value| normalize_unicode_nfc(&value))
                .collect()
        }),
        artist_relations: track
            .artist_relations
            .into_iter()
            .map(|relation| lyra_metadata::ArtistRelationMetadata {
                source_artist: normalize_unicode_nfc(&relation.source_artist),
                target_artist: normalize_unicode_nfc(&relation.target_artist),
                relation_type: relation.relation_type,
                source_artist_type: relation.source_artist_type,
                target_artist_type: relation.target_artist_type,
            })
            .collect(),
        disc: track.disc,
        disc_total: track.disc_total,
        track: track.track,
        track_total: track.track_total,
        duration_ms: track.duration_ms,
        genres: track.genres.map(|genres| {
            genres
                .into_iter()
                .map(|value| normalize_unicode_nfc(&value))
                .collect()
        }),
        label: track.label.map(|value| normalize_unicode_nfc(&value)),
        catalog_number: track
            .catalog_number
            .map(|value| normalize_unicode_nfc(&value)),
        media_formats: track.media_formats,
        barcode: track.barcode,
        source_kind: Some(SOURCE_KIND_EMBEDDED_TAGS.to_string()),
        source_key: build_embedded_source_key(entry_public_id),
        segment_start_ms: None,
        segment_end_ms: None,
        cue_sheet_entry_id: None,
        cue_sheet_hash: None,
        cue_track_no: None,
        cue_audio_entry_id: None,
        cue_index00_frames: None,
        cue_index01_frames: None,
        track_gain_db: track.track_gain_db,
        album_gain_db: track.album_gain_db,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use lofty::{
        config::WriteOptions,
        tag::TagExt,
    };

    use super::*;

    /// Copies `fixture` to `path`, dropping the vorbis comments in `removed` and adding `added`.
    pub(super) fn fixture_with_comments(
        path: &std::path::Path,
        fixture: &str,
        removed: &[&str],
        added: &[(&str, &str)],
    ) -> anyhow::Result<()> {
        let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/assets/metadata")
            .join(fixture);
        std::fs::create_dir_all(path.parent().expect("fixture parent"))?;
        std::fs::copy(&source, path)?;
        let mut file = std::fs::File::open(path)?;
        let mut vorbis = if fixture.ends_with(".opus") {
            OpusFile::read_from(&mut file, ParseOptions::new())?
                .vorbis_comments()
                .clone()
        } else {
            FlacFile::read_from(&mut file, ParseOptions::new())?
                .vorbis_comments()
                .cloned()
                .unwrap_or_default()
        };
        for key in removed {
            vorbis.remove(key).for_each(drop);
        }
        for (key, value) in added {
            vorbis.push((*key).to_string(), (*value).to_string());
        }
        vorbis.save_to_path(path, WriteOptions::default())?;
        Ok(())
    }

    pub(super) fn file_entry(db_id: i64, path: &std::path::Path) -> Entry {
        Entry {
            db_id: Some(DbId(db_id)),
            id: format!("entry-{db_id}"),
            full_path: path.to_path_buf(),
            kind: lyra_db::entries::EntryKind::File,
            file_kind: None,
            name: path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default(),
            hash: None,
            size: 0,
            file_modified_at_ms: 0,
            file_created_at_ms: 0,
            synced_at_ms: None,
        }
    }

    fn skipped_for(
        output: &ParseMetadataOutput,
    ) -> Vec<(PathBuf, Vec<mapping::MissingRequiredField>)> {
        output
            .skipped
            .iter()
            .map(|record| match &record.reason {
                SkipReason::RequiredFieldEmpty(missing) => (record.path.clone(), missing.clone()),
                reason => panic!("unexpected skip: {reason:?}"),
            })
            .collect()
    }

    #[tokio::test]
    async fn parse_metadata_infers_a_title_missing_from_tags() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("Folder Album/01 - Path Title.flac");
        fixture_with_comments(&path, "integration_track.flac", &["TITLE"], &[])?;

        let output = parse_metadata(&mapping::default_config(), vec![file_entry(1, &path)]).await?;

        assert!(output.skipped.is_empty());
        assert_eq!(output.metadata.len(), 1);
        let track = &output.metadata[0];
        assert_eq!(track.title.as_deref(), Some("Path Title"));
        assert_eq!(track.album.as_deref(), Some("Integration Album"));
        assert_eq!(track.track, Some(1));
        Ok(())
    }

    #[tokio::test]
    async fn parse_metadata_skips_a_folder_without_tagged_albums() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = [
            dir.path().join("Folder Album/01 - One.flac"),
            dir.path().join("Folder Album/02 - Two.flac"),
        ];
        for path in &paths {
            fixture_with_comments(path, "integration_track.flac", &["ALBUM"], &[])?;
        }

        let output = parse_metadata(
            &mapping::default_config(),
            vec![file_entry(1, &paths[0]), file_entry(2, &paths[1])],
        )
        .await?;

        assert!(output.metadata.is_empty());
        assert_eq!(
            skipped_for(&output),
            paths.map(|path| (path, vec![mapping::MissingRequiredField::Album])),
        );
        Ok(())
    }

    #[tokio::test]
    async fn parse_metadata_skips_a_track_whose_artist_is_only_in_its_file_name()
    -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let tagged = dir.path().join("Folder Album/01 - One.flac");
        let untagged = dir
            .path()
            .join("Folder Album/02 - Integration Artist - Two.flac");
        fixture_with_comments(&tagged, "integration_track.flac", &[], &[])?;
        fixture_with_comments(&untagged, "integration_track.flac", &["ARTIST"], &[])?;

        let output = parse_metadata(
            &mapping::default_config(),
            vec![file_entry(1, &tagged), file_entry(2, &untagged)],
        )
        .await?;

        assert_eq!(output.metadata.len(), 1);
        assert_eq!(output.metadata[0].entry_db_id, DbId(1));
        assert_eq!(
            skipped_for(&output),
            [(untagged, vec![mapping::MissingRequiredField::ArtistKey])],
        );
        Ok(())
    }

    #[tokio::test]
    async fn parse_metadata_keeps_inferred_titles_out_of_cue_tracks() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let audio_path = dir.path().join("Folder Album/album.flac");
        fixture_with_comments(&audio_path, "integration_track.flac", &["TITLE"], &[])?;
        let cue_path = dir.path().join("Folder Album/album.cue");
        std::fs::write(
            &cue_path,
            "TITLE \"Cue Album\"\nPERFORMER \"Cue Artist\"\nFILE \"album.flac\" WAVE\n  TRACK 01 AUDIO\n    TITLE \"One\"\n    INDEX 01 00:00:00\n",
        )?;

        let output = parse_metadata(
            &mapping::default_config(),
            vec![file_entry(1, &audio_path), file_entry(2, &cue_path)],
        )
        .await?;

        assert!(output.skipped.is_empty());
        assert_eq!(output.metadata.len(), 1);
        let track = &output.metadata[0];
        assert_eq!(track.title.as_deref(), Some("One"));
        assert_eq!(track.album.as_deref(), Some("Integration Album"));
        Ok(())
    }

    #[tokio::test]
    async fn parse_metadata_keeps_inferred_titles_out_of_cue_tracks_out_of_order()
    -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let album = dir.path().join("Folder Album");
        let titled = album.join("titled.flac");
        let untitled = album.join("untitled.flac");
        fixture_with_comments(
            &titled,
            "integration_track.flac",
            &[],
            &[("TRACKNUMBER", "2")],
        )?;
        fixture_with_comments(
            &untitled,
            "integration_track.flac",
            &["TITLE"],
            &[("TRACKNUMBER", "1")],
        )?;
        for (name, title) in [("titled", "Cue Titled"), ("untitled", "Cue Untitled")] {
            std::fs::write(
                album.join(format!("{name}.cue")),
                format!(
                    "FILE \"{name}.flac\" WAVE\n  TRACK 01 AUDIO\n    TITLE \"{title}\"\n    INDEX 01 00:00:00\n"
                ),
            )?;
        }

        let output = parse_metadata(
            &mapping::default_config(),
            vec![
                file_entry(1, &titled),
                file_entry(2, &untitled),
                file_entry(3, &album.join("titled.cue")),
                file_entry(4, &album.join("untitled.cue")),
            ],
        )
        .await?;

        assert!(output.skipped.is_empty());
        let title_of = |entry: i64| {
            output
                .metadata
                .iter()
                .find(|track| track.entry_db_id == DbId(entry))
                .and_then(|track| track.title.as_deref())
        };
        assert_eq!(title_of(1), Some("Integration Track"));
        assert_eq!(title_of(2), Some("Cue Untitled"));
        Ok(())
    }

    #[tokio::test]
    async fn parse_metadata_keeps_filename_features_out_of_cue_tracks() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let audio_path = dir.path().join("Folder Album/Container (feat. Guest).flac");
        fixture_with_comments(
            &audio_path,
            "integration_track.flac",
            &["TITLE", "ARTIST"],
            &[("ARTIST", "Container")],
        )?;
        std::fs::write(
            dir.path().join("Folder Album/album.cue"),
            "PERFORMER \"Container\"\nFILE \"Container (feat. Guest).flac\" WAVE\n  TRACK 01 AUDIO\n    TITLE \"One\"\n    INDEX 01 00:00:00\n",
        )?;

        let output = parse_metadata(
            &mapping::default_config(),
            vec![
                file_entry(1, &audio_path),
                file_entry(2, &dir.path().join("Folder Album/album.cue")),
            ],
        )
        .await?;

        assert!(output.skipped.is_empty());
        assert_eq!(output.metadata.len(), 1);
        let track = &output.metadata[0];
        assert_eq!(track.title.as_deref(), Some("One"));
        assert_eq!(track.artists, Some(vec!["Container".to_string()]));
        Ok(())
    }

    #[tokio::test]
    async fn parse_metadata_matches_file_name_artists_tagged_on_cue_audio() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let album = dir.path().join("Folder Album");
        let image = album.join("image.flac");
        let song = album.join("Known Artist - Song.flac");
        fixture_with_comments(
            &image,
            "integration_track.flac",
            &["ARTIST"],
            &[("ARTIST", "Known Artist")],
        )?;
        fixture_with_comments(
            &song,
            "integration_track.flac",
            &["TITLE", "ARTIST"],
            &[("ALBUMARTIST", "Various Artists")],
        )?;
        std::fs::write(
            album.join("image.cue"),
            "FILE \"image.flac\" WAVE\n  TRACK 01 AUDIO\n    TITLE \"One\"\n    INDEX 01 00:00:00\n",
        )?;

        let output = parse_metadata(
            &mapping::default_config(),
            vec![
                file_entry(1, &image),
                file_entry(2, &song),
                file_entry(3, &album.join("image.cue")),
            ],
        )
        .await?;

        assert!(output.skipped.is_empty());
        let song = output
            .metadata
            .iter()
            .find(|track| track.entry_db_id == DbId(2))
            .expect("standalone track");
        assert_eq!(song.title.as_deref(), Some("Song"));
        assert_eq!(song.artists, Some(vec!["Known Artist".to_string()]));
        Ok(())
    }

    #[tokio::test]
    async fn parse_metadata_ingests_a_multi_track_cue_over_untitled_audio() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let audio_path = dir.path().join("Folder Album/album.flac");
        fixture_with_comments(&audio_path, "integration_track.flac", &["TITLE"], &[])?;
        let cue_path = dir.path().join("Folder Album/album.cue");
        std::fs::write(
            &cue_path,
            "FILE \"album.flac\" WAVE\n  TRACK 01 AUDIO\n    TITLE \"One\"\n    INDEX 01 00:00:00\n  TRACK 02 AUDIO\n    TITLE \"Two\"\n    INDEX 01 00:00:50\n",
        )?;

        let output = parse_metadata(
            &mapping::default_config(),
            vec![file_entry(1, &audio_path), file_entry(2, &cue_path)],
        )
        .await?;

        assert!(output.skipped.is_empty());
        let titles: Vec<_> = output
            .metadata
            .iter()
            .map(|track| track.title.as_deref())
            .collect();
        assert_eq!(titles, [Some("One"), Some("Two")]);
        Ok(())
    }

    /// The one track a cue sheet of `comments` makes of a file in `folder` tagged with `added`.
    async fn cue_track_with(
        folder: &str,
        added: &[(&str, &str)],
        comments: &str,
    ) -> anyhow::Result<TrackMetadata> {
        let dir = tempfile::tempdir()?;
        let audio_path = dir.path().join(folder).join("album.flac");
        fixture_with_comments(&audio_path, "integration_track.flac", &[], added)?;
        let cue_path = dir.path().join(folder).join("album.cue");
        std::fs::write(
            &cue_path,
            format!(
                "{comments}FILE \"album.flac\" WAVE\n  TRACK 01 AUDIO\n    TITLE \"One\"\n    INDEX 01 00:00:00\n"
            ),
        )?;

        let mut output = parse_metadata(
            &mapping::default_config(),
            vec![file_entry(1, &audio_path), file_entry(2, &cue_path)],
        )
        .await?;

        assert!(output.skipped.is_empty());
        assert_eq!(output.metadata.len(), 1);
        Ok(output.metadata.remove(0))
    }

    fn dated(track: &TrackMetadata) -> (Option<&str>, Option<u32>) {
        (track.date.as_deref(), track.year)
    }

    #[tokio::test]
    async fn parse_metadata_dates_cue_tracks_by_tag_over_cue() -> anyhow::Result<()> {
        let track =
            cue_track_with("Album (1990)", &[("DATE", "2001-02-03")], "REM DATE 1994\n").await?;
        assert_eq!(dated(&track), (Some("2001-02-03"), Some(2001)));
        Ok(())
    }

    #[tokio::test]
    async fn parse_metadata_dates_cue_tracks_by_cue_over_path() -> anyhow::Result<()> {
        let track = cue_track_with("Album (1990)", &[], "REM DATE \"1994-05-03\"\n").await?;
        assert_eq!(dated(&track), (Some("1994-05-03"), Some(1994)));
        Ok(())
    }

    #[tokio::test]
    async fn parse_metadata_dates_cue_tracks_by_cue_alone() -> anyhow::Result<()> {
        let track = cue_track_with("Album", &[], "rem date 1994\n").await?;
        assert_eq!(dated(&track), (Some("1994"), Some(1994)));
        Ok(())
    }

    #[tokio::test]
    async fn parse_metadata_dates_cue_tracks_by_copyright_year_over_cue() -> anyhow::Result<()> {
        let track = cue_track_with(
            "Album (1990)",
            &[("COPYRIGHT", "℗ 2003 Label")],
            "REM DATE 1994\n",
        )
        .await?;
        assert_eq!(dated(&track), (None, Some(2003)));
        Ok(())
    }

    #[tokio::test]
    async fn parse_metadata_dates_each_cue_track_by_path_without_its_own_date() -> anyhow::Result<()>
    {
        let dir = tempfile::tempdir()?;
        let audio_path = dir.path().join("Album (1990)/album.flac");
        fixture_with_comments(&audio_path, "integration_track.flac", &[], &[])?;
        let cue_path = dir.path().join("Album (1990)/album.cue");
        std::fs::write(
            &cue_path,
            "FILE \"album.flac\" WAVE\n  TRACK 01 AUDIO\n    TITLE \"One\"\n    REM DATE 1994\n    INDEX 01 00:00:00\n  TRACK 02 AUDIO\n    TITLE \"Two\"\n    INDEX 01 00:00:50\n",
        )?;

        let output = parse_metadata(
            &mapping::default_config(),
            vec![file_entry(1, &audio_path), file_entry(2, &cue_path)],
        )
        .await?;

        let date_of = |title: &str| {
            output
                .metadata
                .iter()
                .find(|track| track.title.as_deref() == Some(title))
                .map(dated)
        };
        assert_eq!(date_of("One"), Some((Some("1994"), Some(1994))));
        assert_eq!(date_of("Two"), Some((None, Some(1990))));
        Ok(())
    }

    #[tokio::test]
    async fn parse_metadata_dates_undated_cue_tracks_by_path() -> anyhow::Result<()> {
        let track = cue_track_with("Album (1990)", &[], "").await?;
        assert_eq!(dated(&track), (None, Some(1990)));
        assert_eq!(track.genres, None);
        Ok(())
    }

    #[tokio::test]
    async fn parse_metadata_takes_cue_track_genres_from_tags_over_cue() -> anyhow::Result<()> {
        let tagged = cue_track_with("Album", &[("GENRE", "Jazz")], "REM GENRE Rock\n").await?;
        assert_eq!(tagged.genres, Some(vec!["Jazz".to_string()]));

        let untagged = cue_track_with("Album", &[], "REM GENRE \"Rock\"\n").await?;
        assert_eq!(untagged.genres, Some(vec!["Rock".to_string()]));
        Ok(())
    }

    #[tokio::test]
    async fn parse_metadata_reads_cue_comments_as_it_reads_tags() -> anyhow::Result<()> {
        let (date, genre) = ("1994-05-03T12:00", "Rock, Pop");
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("Album/01 - One.flac");
        fixture_with_comments(
            &path,
            "integration_track.flac",
            &[],
            &[("DATE", date), ("GENRE", genre)],
        )?;
        let tagged = parse_metadata(&mapping::default_config(), vec![file_entry(1, &path)])
            .await?
            .metadata
            .remove(0);

        let cued = cue_track_with(
            "Album",
            &[],
            &format!("REM DATE \"{date}\"\nREM GENRE \"{genre}\"\n"),
        )
        .await?;
        assert_eq!(dated(&cued), dated(&tagged));
        assert_eq!(cued.genres, tagged.genres);
        assert_eq!(
            tagged.genres,
            Some(vec!["Rock".to_string(), "Pop".to_string()])
        );
        Ok(())
    }

    /// The releases a scan of `CD1` and `CD2` folders in `Album (1990)` coalesces into, each
    /// disc an untagged-date image under a cue sheet holding the matching `comments`.
    async fn releases_of_cued_discs(comments: [&str; 2]) -> anyhow::Result<Vec<Vec<Option<u32>>>> {
        let dir = tempfile::tempdir()?;
        let mut entries = Vec::new();
        for (index, comments) in comments.into_iter().enumerate() {
            let disc = dir.path().join(format!("Album (1990)/CD{}", index + 1));
            let audio_path = disc.join("album.flac");
            fixture_with_comments(&audio_path, "integration_track.flac", &[], &[])?;
            let cue_path = disc.join("album.cue");
            std::fs::write(
                &cue_path,
                format!(
                    "{comments}FILE \"album.flac\" WAVE\n  TRACK 01 AUDIO\n    TITLE \"One\"\n    INDEX 01 00:00:00\n"
                ),
            )?;
            let id = 2 * index as i64;
            entries.extend([
                file_entry(id + 1, &audio_path),
                file_entry(id + 2, &cue_path),
            ]);
        }

        let output = parse_metadata(&mapping::default_config(), entries).await?;
        assert!(output.skipped.is_empty());
        let groups = output
            .metadata
            .into_iter()
            .map(|track| ingestion::ParsedMetadataGroup {
                coalesce_group_key: 0,
                source_dir: PathBuf::from(format!("CD{}", track.disc.unwrap_or_default())),
                metadata: vec![track],
            })
            .collect();
        Ok(ingestion::coalesce_disc_groups(groups)
            .into_iter()
            .map(|release| release.iter().map(|track| track.year).collect())
            .collect())
    }

    #[tokio::test]
    async fn cued_discs_coalesce_by_their_shared_folder_year() -> anyhow::Result<()> {
        let releases = releases_of_cued_discs(["", ""]).await?;
        assert_eq!(releases, [vec![Some(1990), Some(1990)]]);
        Ok(())
    }

    #[tokio::test]
    async fn cued_discs_dated_by_one_cue_alone_split() -> anyhow::Result<()> {
        let releases = releases_of_cued_discs(["REM DATE 1994\n", ""]).await?;
        assert_eq!(releases, [vec![Some(1994)], vec![Some(1990)]]);
        Ok(())
    }

    #[tokio::test]
    async fn parse_metadata_skips_cue_tracks_left_without_an_album() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let audio = dir.path().join("album.flac");
        fixture_with_comments(&audio, "integration_track.flac", &["ALBUM"], &[])?;
        let cue = dir.path().join(" .cue");
        std::fs::write(
            &cue,
            "FILE \"album.flac\" WAVE\n  TRACK 01 AUDIO\n    INDEX 01 00:00:00\n",
        )?;

        let output = parse_metadata(
            &mapping::default_config(),
            vec![file_entry(1, &audio), file_entry(2, &cue)],
        )
        .await?;

        assert!(output.metadata.is_empty());
        assert_eq!(
            skipped_for(&output),
            [
                (audio, vec![mapping::MissingRequiredField::Album]),
                (cue, vec![mapping::MissingRequiredField::Album]),
            ],
        );
        Ok(())
    }

    #[tokio::test]
    async fn parse_metadata_tells_unreadable_files_from_unparseable_ones() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir()?;
        let audio = dir.path().join("Album/01.flac");
        fixture_with_comments(&audio, "integration_track.flac", &[], &[])?;
        let corrupt = dir.path().join("Album/02.flac");
        std::fs::write(&corrupt, b"fLaC not really")?;
        let cue = dir.path().join("Album/album.cue");
        std::fs::write(&cue, [0xff, 0xfe, 0x00, b'F'])?;
        std::fs::set_permissions(&audio, std::fs::Permissions::from_mode(0o000))?;
        let readable_anyway = std::fs::read(&audio).is_ok();

        let output = parse_metadata(
            &mapping::default_config(),
            vec![
                file_entry(1, &audio),
                file_entry(2, &corrupt),
                file_entry(3, &cue),
            ],
        )
        .await?;

        let transient = |path: &std::path::Path| {
            output
                .skipped
                .iter()
                .find(|record| record.path == path)
                .map(|record| record.reason.is_transient())
        };
        if !readable_anyway {
            assert_eq!(transient(&audio), Some(true));
        }
        assert_eq!(transient(&corrupt), Some(false));
        assert_eq!(transient(&cue), Some(false));
        Ok(())
    }

    #[tokio::test]
    async fn parse_metadata_infers_a_blank_title_tag() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("Folder Album/01 - Path Title.flac");
        fixture_with_comments(
            &path,
            "integration_track.flac",
            &["TITLE"],
            &[("TITLE", " ")],
        )?;

        let output = parse_metadata(&mapping::default_config(), vec![file_entry(1, &path)]).await?;

        assert!(output.skipped.is_empty());
        assert_eq!(output.metadata[0].title.as_deref(), Some("Path Title"));
        Ok(())
    }

    #[tokio::test]
    async fn parse_metadata_skips_an_untitled_track_named_by_number_alone() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("Folder Album/01.flac");
        fixture_with_comments(&path, "integration_track.flac", &["TITLE"], &[])?;

        let output = parse_metadata(&mapping::default_config(), vec![file_entry(1, &path)]).await?;

        assert!(output.metadata.is_empty());
        assert_eq!(
            skipped_for(&output),
            [(path, vec![mapping::MissingRequiredField::Title])],
        );
        Ok(())
    }

    #[test]
    fn read_audio_tags_falls_back_to_unmapped_vorbis_comments() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("integration_track.flac");
        fixture_with_comments(
            &path,
            "integration_track.flac",
            &[],
            &[
                ("SOURCEMEDIA", "CD"),
                ("MEDIATYPE", "File"),
                ("UPC", "0199957588430"),
            ],
        )?;
        let (tag, _, _) = read_audio_tags(path.clone())?;

        assert_eq!(tag.get_string(ItemKey::OriginalMediaType), Some("File"));
        assert_eq!(tag.get_string(ItemKey::Barcode), Some("0199957588430"));
        Ok(())
    }

    #[test]
    fn read_audio_tags_prefers_mapped_vorbis_comments() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("integration_track.flac");
        fixture_with_comments(
            &path,
            "integration_track.flac",
            &[],
            &[
                ("MEDIA", "CD"),
                ("MEDIATYPE", "File"),
                ("BARCODE", "4988031554531"),
                ("UPC", "0199957588430"),
            ],
        )?;
        let (tag, _, _) = read_audio_tags(path.clone())?;

        assert_eq!(tag.get_string(ItemKey::OriginalMediaType), Some("CD"));
        assert_eq!(tag.get_string(ItemKey::Barcode), Some("4988031554531"));
        Ok(())
    }

    fn processed_date(comments: &[(&str, &str)]) -> anyhow::Result<(Option<String>, Option<u32>)> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("integration_track.flac");
        fixture_with_comments(&path, "integration_track.flac", &[], comments)?;
        let (tag, properties, _) = read_audio_tags(path.clone())?;

        let raw = mapping::apply_mapping(
            &tag,
            &properties,
            &path.to_string_lossy(),
            &mapping::default_config(),
        );
        let track = lyra_metadata::process_raw_tags(vec![raw]).remove(0);
        Ok((track.date, track.year))
    }

    #[test]
    fn date_tags_fall_back_from_release_to_recording_date_and_year() -> anyhow::Result<()> {
        assert_eq!(
            processed_date(&[("YEAR", "2017")])?,
            (Some("2017".to_string()), Some(2017))
        );
        assert_eq!(
            processed_date(&[("YEAR", "2017"), ("DATE", "2019-05-03T12:00")])?,
            (Some("2019-05-03".to_string()), Some(2019))
        );
        assert_eq!(
            processed_date(&[
                ("YEAR", "2017"),
                ("DATE", "2019-05-03"),
                ("RELEASEDATE", "2020-01-10"),
            ])?,
            (Some("2020-01-10".to_string()), Some(2020))
        );
        Ok(())
    }

    #[test]
    fn gain_tags_resolve_to_replaygain_reference() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("integration_track.flac");
        fixture_with_comments(
            &path,
            "integration_track.flac",
            &[],
            &[
                ("REPLAYGAIN_TRACK_GAIN", "-6.54 dB"),
                ("R128_TRACK_GAIN", "-512"),
                ("R128_ALBUM_GAIN", "-512"),
            ],
        )?;
        let (tag, properties, _) = read_audio_tags(path.clone())?;

        let raw = mapping::apply_mapping(
            &tag,
            &properties,
            &path.to_string_lossy(),
            &mapping::default_config(),
        );
        let track = lyra_metadata::process_raw_tags(vec![raw]).remove(0);
        assert_eq!(track.track_gain_db, Some(-6.54));
        assert_eq!(track.album_gain_db, Some(3.0));
        Ok(())
    }

    #[test]
    fn read_audio_tags_reads_opus_comments() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("integration_track.opus");
        fixture_with_comments(
            &path,
            "integration_track.opus",
            &[],
            &[("UPC", "0199957588430"), ("R128_TRACK_GAIN", "-512")],
        )?;
        let (tag, properties, _) = read_audio_tags(path.clone())?;

        assert_eq!(tag.get_string(ItemKey::Barcode), Some("0199957588430"));
        let raw = mapping::apply_mapping(
            &tag,
            &properties,
            &path.to_string_lossy(),
            &mapping::default_config(),
        );
        assert!(raw.duration_ms > 0);
        let track = lyra_metadata::process_raw_tags(vec![raw]).remove(0);
        assert_eq!(track.track_gain_db, Some(3.0));
        Ok(())
    }
}
