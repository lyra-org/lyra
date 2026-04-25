// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub(crate) mod cleanup;
mod cue;
pub(crate) mod ingestion;
pub(crate) mod layers;
pub(crate) mod lyrics;
pub(crate) mod mapping;
pub(crate) mod mapping_admin;
pub(crate) mod merging;
mod model;
pub(crate) mod verification;

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
    file::TaggedFileExt,
    probe::Probe,
    tag::Tag,
};
use lyra_metadata::normalize_unicode_nfc;

use crate::{
    STATE,
    db::Entry,
};
use cue::{
    build_audio_lookup,
    build_embedded_source_key,
    merge_embedded_into_cue_metadata,
    parse_cue_metadata_for_entry,
    sort_track_metadata,
};
use mapping::MetadataMappingConfig;
pub(crate) use model::TrackMetadata;

const SOURCE_KIND_EMBEDDED_TAGS: &str = "embedded_tags";

fn classify_entry_file_kind(entry: &Entry) -> Option<String> {
    if entry.kind != crate::db::entries::EntryKind::File {
        return None;
    }

    entry
        .file_kind
        .clone()
        .or_else(|| crate::db::entries::classify_file_kind(&entry.full_path).map(str::to_string))
}

pub(crate) fn read_audio_tags(
    path: std::path::PathBuf,
) -> anyhow::Result<(Tag, lofty::file::TaggedFile)> {
    let tagged_file = Probe::open(&path)
        .with_context(|| format!("bad path: {}", path.display()))?
        .read()
        .with_context(|| format!("failed to read file: {}", path.display()))?;

    let tag = tagged_file
        .primary_tag()
        .or_else(|| tagged_file.first_tag())
        .cloned()
        .ok_or_else(|| anyhow!("no tags found in {}", path.display()))?;

    Ok((tag, tagged_file))
}

/// Disc/track number+total and duration bypass the mapping — their
/// extraction does format-specific parsing (n/N strings, packed MP4
/// atoms) not expressible as a rule.
pub(crate) fn extract_raw_tags_from_lofty(
    tag: &Tag,
    tagged_file: &lofty::file::TaggedFile,
    file_path: &str,
    config: &MetadataMappingConfig,
) -> lyra_metadata::RawTrackTags {
    mapping::apply_mapping(tag, tagged_file, file_path, config)
}

/// Payloads are read only via `Debug` for tracing — hence `dead_code`.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum SkipReason {
    ReadFailed(String),
    TaskFailed(String),
    /// Prevents silent coalesce corruption downstream.
    RequiredFieldEmpty(Vec<mapping::MissingRequiredField>),
    /// Audio entries were claimed by an earlier cue in the batch.
    CueAlreadyClaimed,
    CueParseFailed(String),
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
                skip_reason_tag(&record.reason),
            )
        })
        .collect();
    tracing::warn!(
        skipped_total = total,
        sample = ?sample,
        "parse_metadata skipped files",
    );
}

fn skip_reason_tag(reason: &SkipReason) -> &'static str {
    match reason {
        SkipReason::ReadFailed(_) => "read_failed",
        SkipReason::TaskFailed(_) => "task_failed",
        SkipReason::RequiredFieldEmpty(_) => "required_field_empty",
        SkipReason::CueAlreadyClaimed => "cue_already_claimed",
        SkipReason::CueParseFailed(_) => "cue_parse_failed",
    }
}

/// Read-fast config loader: the common case (config already seeded)
/// takes only a read lock, avoiding unnecessary write-lock contention
/// on hot paths. Falls through to `ensure` on the first-boot path.
async fn load_mapping_config() -> anyhow::Result<MetadataMappingConfig> {
    {
        let db = STATE.db.read().await;
        if let Some(cfg) = crate::db::metadata::mapping_config::get(&db)? {
            return Ok(cfg);
        }
    }
    let mut db = STATE.db.write().await;
    crate::db::metadata::mapping_config::ensure(&mut db)
}

pub(crate) async fn parse_metadata(entries: Vec<Entry>) -> anyhow::Result<ParseMetadataOutput> {
    let mut audio_entries = Vec::new();
    let mut cue_entries = Vec::new();

    for entry in entries {
        if entry.kind != crate::db::entries::EntryKind::File {
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
                    reason: SkipReason::CueParseFailed(err.to_string()),
                });
            }
        }
    }

    let mut entry_ids_by_path: HashMap<String, VecDeque<DbId>> = HashMap::new();
    let mut raw_tags = Vec::new();
    let mapping_config = load_mapping_config().await?;

    for entry in audio_entries {
        let Some(entry_db_id) = entry.db_id else {
            continue;
        };

        let path = entry.full_path.clone();
        let path_for_task = path.clone();
        let task_result = tokio::task::spawn_blocking(move || read_audio_tags(path_for_task)).await;
        let (tag, tagged_file) = match task_result {
            Ok(Ok(result)) => result,
            Ok(Err(err)) => {
                tracing::warn!(
                    error = %err,
                    path = %path.display(),
                    "skipping metadata for file"
                );
                skipped.push(SkipRecord {
                    path: path.clone(),
                    reason: SkipReason::ReadFailed(err.to_string()),
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
        let raw = extract_raw_tags_from_lofty(&tag, &tagged_file, &file_path, &mapping_config);
        if let Err(missing) = mapping::check_required_fields(&raw) {
            tracing::warn!(
                path = %path.display(),
                missing = ?missing,
                "skipping track: post-mapping required fields empty"
            );
            skipped.push(SkipRecord {
                path: path.clone(),
                reason: SkipReason::RequiredFieldEmpty(missing),
            });
            continue;
        }
        entry_ids_by_path
            .entry(raw.file_path.clone())
            .or_default()
            .push_back(entry_db_id);
        raw_tags.push(raw);
    }

    let processed = lyra_metadata::process_raw_tags(raw_tags);
    let mut metadata = Vec::with_capacity(processed.len() + cue_metadata.len());
    let mut embedded_by_entry_id: HashMap<DbId, TrackMetadata> = HashMap::new();

    for track in processed {
        let path = track
            .file_path
            .clone()
            .ok_or_else(|| anyhow!("processed metadata missing file_path"))?;
        let entry_db_id = entry_ids_by_path
            .get_mut(&path)
            .and_then(|ids| ids.pop_front())
            .ok_or_else(|| anyhow!("missing entry id mapping for processed track: {path}"))?;

        let embedded = TrackMetadata {
            entry_db_id,
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
            source_kind: Some(SOURCE_KIND_EMBEDDED_TAGS.to_string()),
            source_key: Some(build_embedded_source_key(entry_db_id)),
            segment_start_ms: None,
            segment_end_ms: None,
            cue_sheet_entry_id: None,
            cue_sheet_hash: None,
            cue_track_no: None,
            cue_audio_entry_id: None,
            cue_index00_frames: None,
            cue_index01_frames: None,
            sample_rate_hz: track.sample_rate_hz,
            channel_count: track.channel_count,
            bit_depth: track.bit_depth,
            bitrate_bps: track.bitrate_bps,
        };

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

    metadata.extend(cue_metadata);
    sort_track_metadata(&mut metadata);
    Ok(ParseMetadataOutput { metadata, skipped })
}
