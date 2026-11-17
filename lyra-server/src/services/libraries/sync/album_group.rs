// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Reading an album group's files and applying what they yield.

use std::{
    collections::{
        HashMap,
        HashSet,
    },
    path::PathBuf,
    sync::Arc,
    time::Instant,
};

use agdb::DbId;
use lyra_db::{
    DbAsync,
    entries::EntryKind,
    metadata::mapping_config::MetadataMappingConfig,
};

use super::{
    super::{
        MediaRoots,
        orchestrator::{
            SyncRunControlError,
            SyncRunProgress,
            SyncStageKey,
            SyncTotalState,
            SyncWorkDetails,
        },
    },
    mark_entries_synced,
};
use crate::services::metadata::{
    CueLinks,
    TrackMetadata,
    ingestion::{
        MetadataApplyResult,
        MetadataGroup,
        apply_metadata,
        coalesce_group_tracks,
    },
    log_skip_summary,
    parse_metadata,
    skip_reason_summary,
};

/// Parses an album group, coalesces its discs and applies each batch in one write. Returns the
/// release each batch applied to, with the entries marked synced once its artifacts finish. A
/// failed parse or apply is a failed unit, so only a cancelled run fails this.
pub(super) async fn process_album_group(
    db: &DbAsync,
    library_db_id: DbId,
    group: MetadataGroup,
    mapping_config: &MetadataMappingConfig,
    cue_links: &CueLinks,
    roots: &Arc<MediaRoots>,
    progress: SyncRunProgress,
) -> anyhow::Result<Vec<(DbId, Vec<DbId>)>> {
    progress.check_cancelled()?;
    let source_dir = group.source_dir.clone();
    let details = SyncWorkDetails::new("metadata_parse_group")
        .source_dir(Some(source_dir.to_string_lossy().into_owned()));
    let work_id = progress
        .start_work(SyncStageKey::MetadataParse, details.clone())
        .await;
    let candidate_dirs = group.candidate_dirs();
    let group_unsynced = group
        .entries
        .iter()
        .filter(|entry| entry.kind == EntryKind::File && entry.synced_at_ms.is_none())
        .filter_map(|entry| Some((entry.full_path.clone(), entry.db_id?)))
        .collect::<HashMap<_, _>>();
    let parse = async {
        #[cfg(test)]
        super::tests::before_parse(&source_dir).await;
        parse_metadata(mapping_config, group.entries, cue_links, roots).await
    };
    let parsed = tokio::select! {
        parsed = parse => parsed,
        err = progress.cancelled() => return Err(err.into()),
    };
    let parse_output = match parsed {
        Ok(output) => output,
        Err(err) => {
            progress
                .fail_work(
                    work_id,
                    SyncStageKey::MetadataParse,
                    details,
                    format!("{err:#}"),
                )
                .await;
            return Ok(Vec::new());
        }
    };
    if !parse_output.skipped.is_empty() {
        log_skip_summary(&parse_output.skipped);
    }
    let unreadable = parse_output
        .skipped
        .iter()
        .filter(|record| record.reason.is_transient())
        .collect::<Vec<_>>();
    let unreadable_entries = unreadable
        .iter()
        .filter_map(|record| group_unsynced.get(&record.path).copied())
        .collect::<HashSet<_>>();
    let metadata = parse_output.metadata;
    let read = metadata_entry_ids(&metadata).collect::<HashSet<_>>();
    let finished = group_unsynced
        .values()
        .copied()
        .filter(|entry_id| !read.contains(entry_id) && !unreadable_entries.contains(entry_id))
        .collect::<Vec<_>>();
    mark_entries_synced(db, library_db_id, &finished).await;

    match unreadable.first() {
        Some(first) => {
            let err = anyhow::anyhow!(
                "{} files could not be read; {}: {}",
                unreadable.len(),
                first.path.display(),
                skip_reason_summary(&first.reason)
            );
            progress
                .fail_work(
                    work_id,
                    SyncStageKey::MetadataParse,
                    details,
                    format!("{err:#}"),
                )
                .await;
        }
        None => {
            progress
                .complete_work(work_id, SyncStageKey::MetadataParse)
                .await;
        }
    }

    let unsynced_entries = group_unsynced.into_values().collect::<HashSet<_>>();
    let batches = coalesce_group_tracks(&source_dir, &candidate_dirs, metadata)
        .into_iter()
        .filter(|metadata| {
            metadata_entry_ids(metadata).any(|entry_id| unsynced_entries.contains(&entry_id))
        })
        .collect::<Vec<_>>();
    progress
        .add_stage_total(
            SyncStageKey::MetadataApply,
            batches.len() as u64,
            SyncTotalState::Estimated,
        )
        .await;

    let mut applied = Vec::with_capacity(batches.len());
    for metadata in batches {
        let batch_source_dir = metadata
            .first()
            .and_then(|track| candidate_dirs.get(&track.entry_db_id))
            .cloned();
        let entry_ids = metadata_entry_ids(&metadata)
            .filter(|entry_id| !unreadable_entries.contains(entry_id))
            .collect::<Vec<_>>();
        let result = apply_metadata_batch(
            db,
            library_db_id,
            batch_source_dir,
            metadata,
            progress.clone(),
        )
        .await;
        match result {
            Ok(apply_result) => match apply_result.releases.first() {
                Some(&release_id) => applied.push((release_id, entry_ids)),
                None => mark_entries_synced(db, library_db_id, &entry_ids).await,
            },
            Err(err) if err.downcast_ref::<SyncRunControlError>().is_some() => return Err(err),
            Err(_) => {}
        }
    }
    Ok(applied)
}

fn metadata_entry_ids(metadata: &[TrackMetadata]) -> impl Iterator<Item = DbId> + '_ {
    metadata
        .iter()
        .flat_map(|track| std::iter::once(track.entry_db_id).chain(track.cue_sheet_entry_id))
}

async fn apply_metadata_batch(
    db: &DbAsync,
    library_db_id: DbId,
    source_dir: Option<PathBuf>,
    metadata: Vec<TrackMetadata>,
    progress: SyncRunProgress,
) -> anyhow::Result<MetadataApplyResult> {
    let run_id = progress.run_id().to_string();
    progress.check_cancelled()?;
    let source_dir_text = source_dir
        .as_ref()
        .map(|path| path.to_string_lossy().into_owned());
    let work_id = progress
        .start_work(
            SyncStageKey::MetadataApply,
            SyncWorkDetails::new("metadata_apply_group").source_dir(source_dir_text.clone()),
        )
        .await;

    let started = Instant::now();
    tracing::debug!(
        library_db_id = library_db_id.0,
        run_id = %run_id,
        track_count = metadata.len(),
        stage = "metadata_apply",
        "scan group stage started"
    );

    let apply_write_result = async {
        #[cfg(test)]
        super::tests::before_apply(&metadata)?;
        let mut db_write = db.write().await;
        apply_metadata(&mut db_write, library_db_id, metadata)
    }
    .await;
    let apply_result = match apply_write_result {
        Ok(result) => result,
        Err(err) => {
            progress
                .fail_work(
                    work_id,
                    SyncStageKey::MetadataApply,
                    SyncWorkDetails::new("metadata_apply_group").source_dir(source_dir_text),
                    format!("{err:#}"),
                )
                .await;
            return Err(err);
        }
    };

    tracing::info!(
        library_db_id = library_db_id.0,
        run_id = %run_id,
        stage = "metadata_apply",
        release_count = apply_result.releases.len(),
        track_count = apply_result.tracks.len(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "scan group metadata persisted"
    );

    progress
        .complete_work(work_id, SyncStageKey::MetadataApply)
        .await;

    Ok(apply_result)
}
