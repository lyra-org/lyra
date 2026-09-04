// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::DbId;
use futures::StreamExt;
use std::{
    collections::{
        BTreeMap,
        HashSet,
    },
    path::Path,
    time::Instant,
};

use super::{
    orchestrator::{
        SyncRunProgress,
        SyncRunStatus,
        SyncStageKey,
        SyncTotalState,
        SyncWorkDetails,
    },
    scanning::{
        hash_entry_group,
        prepare_entry_scan_plan,
    },
};
pub(crate) use crate::services::system::library_sync_context as system_context;
use crate::{
    Library,
    db::{
        self,
        DbAsync,
        entries::{
            prune_missing_entries,
            sync_entry_group,
        },
    },
    services::{
        CoverPaths,
        CoverSyncOptions,
        covers::{
            configured_covers_root,
            resolve_cover_for_release_id,
            sync_release_cover_for_tracks,
            upsert_release_cover_metadata,
        },
        metadata::{
            cleanup::cleanup_orphaned_metadata,
            ingestion::{
                MetadataApplyResult,
                ParsedMetadataGroup,
                apply_metadata,
                coalesce_disc_groups,
                group_entries,
                source_directory_for_group_entries,
            },
            load_mapping_config,
            log_skip_summary,
            lyrics::providers::{
                MAX_CONCURRENT_DISPATCHES,
                dispatch_for_track as dispatch_lyrics_for_track,
            },
            parse_metadata,
        },
        providers::{
            LibraryRefreshOptions,
            refresh_release_metadata_for_scan_with_progress,
            release_refresh_provider_ids,
        },
    },
};

pub(crate) const MAX_CONCURRENT_ALBUM_PIPELINE: usize = 4;

fn trace_run_id(progress: Option<&SyncRunProgress>) -> String {
    progress
        .map(|progress| progress.run_id().to_string())
        .unwrap_or_default()
}

#[derive(Debug, Default)]
struct IncrementalEntrySyncResult {
    observed_paths: HashSet<std::path::PathBuf>,
    altered_entries: Vec<DbId>,
}

async fn sync_entries_incremental(
    db: &DbAsync,
    library: &Library,
    progress: Option<SyncRunProgress>,
) -> anyhow::Result<IncrementalEntrySyncResult> {
    let run_id = trace_run_id(progress.as_ref());
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;

    let existing = {
        let db_read = db.read().await;
        db::entries::get(&db_read, library_db_id)?
    };
    let scan_plan = prepare_entry_scan_plan(library, existing)?;
    let entry_group_count = scan_plan.groups.len();
    if let Some(progress) = &progress {
        progress
            .add_stage_total(
                SyncStageKey::EntrySync,
                entry_group_count as u64,
                SyncTotalState::Estimated,
            )
            .await;
        progress.set_estimating().await;
    }
    tracing::info!(
        library_db_id = library_db_id.0,
        run_id,
        entry_group_count,
        observed_entry_count = scan_plan.observed_paths.len(),
        stage = "entry_sync",
        "scan entry groups discovered"
    );

    let group_results = futures::stream::iter(scan_plan.groups.into_iter().enumerate())
        .map(|(entry_group_id, group)| {
            let db = db.clone();
            let library = library.clone();
            let progress = progress.clone();
            let run_id = run_id.clone();
            async move {
                let started = Instant::now();
                let source_dir = group.source_dir.clone();
                let entry_count = group.entries.len();
                let work_id = if let Some(progress) = &progress {
                    progress
                        .start_work(
                            SyncStageKey::EntrySync,
                            SyncWorkDetails::new("entry_group_sync")
                                .source_dir(Some(source_dir.to_string_lossy().into_owned())),
                        )
                        .await
                } else {
                    0
                };
                tracing::debug!(
                    library_db_id = library_db_id.0,
                    run_id = %run_id,
                    entry_group_id,
                    source_dir = %source_dir.display(),
                    entry_count,
                    stage = "entry_sync",
                    "scan entry group stage started"
                );
                let result = async {
                    let entries =
                        tokio::task::spawn_blocking(move || hash_entry_group(group.entries))
                            .await
                            .map_err(anyhow::Error::from)?;
                    let result = {
                        let mut db_write = db.write().await;
                        sync_entry_group(&mut db_write, &library, entries)?
                    };
                    Ok::<_, anyhow::Error>(result)
                }
                .await;

                let result = match result {
                    Ok(result) => result,
                    Err(err) => {
                        if let Some(progress) = &progress {
                            progress
                                .fail_work(
                                    work_id,
                                    SyncStageKey::EntrySync,
                                    SyncWorkDetails::new("entry_group_sync").source_dir(Some(
                                        source_dir.to_string_lossy().into_owned(),
                                    )),
                                    err.to_string(),
                                )
                                .await;
                        }
                        return Err(err);
                    }
                };
                tracing::info!(
                    library_db_id = library_db_id.0,
                    run_id = %run_id,
                    entry_group_id,
                    source_dir = %source_dir.display(),
                    entry_count,
                    added = result.added,
                    updated = result.updated,
                    touched_entries = result.altered.len(),
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    stage = "entry_sync",
                    "scan entry group synced"
                );
                if let Some(progress) = &progress {
                    progress
                        .complete_work(work_id, SyncStageKey::EntrySync)
                        .await;
                }
                Ok::<_, anyhow::Error>(result.altered)
            }
        })
        .buffer_unordered(MAX_CONCURRENT_ALBUM_PIPELINE)
        .collect::<Vec<_>>()
        .await;

    let mut result = IncrementalEntrySyncResult::default();
    for group_result in group_results {
        result.altered_entries.extend(group_result?);
    }

    result.observed_paths = scan_plan.observed_paths;

    Ok(result)
}

async fn prune_missing_entries_for_scan(
    db: &DbAsync,
    library: &Library,
    observed_paths: &HashSet<std::path::PathBuf>,
    progress: Option<SyncRunProgress>,
) -> anyhow::Result<usize> {
    let run_id = trace_run_id(progress.as_ref());
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;
    let prune_started = Instant::now();
    let work_id = if let Some(progress) = &progress {
        progress
            .start_work(
                SyncStageKey::Cleanup,
                SyncWorkDetails::new("prune_missing_entries"),
            )
            .await
    } else {
        0
    };
    let prune_write_result = {
        let mut db_write = db.write().await;
        prune_missing_entries(&mut db_write, library, observed_paths)
    };
    let prune_result = match prune_write_result {
        Ok(result) => result,
        Err(err) => {
            if let Some(progress) = &progress {
                progress
                    .fail_work(
                        work_id,
                        SyncStageKey::Cleanup,
                        SyncWorkDetails::new("prune_missing_entries"),
                        err.to_string(),
                    )
                    .await;
            }
            return Err(err);
        }
    };
    if let Some(progress) = &progress {
        progress.complete_work(work_id, SyncStageKey::Cleanup).await;
    }
    tracing::info!(
        library_db_id = library_db_id.0,
        run_id = %run_id,
        deleted_entries = prune_result.deleted,
        elapsed_ms = prune_started.elapsed().as_millis() as u64,
        stage = "cleanup",
        "scan missing entries pruned"
    );
    Ok(prune_result.deleted)
}

async fn cleanup_metadata_orphans(
    db: &DbAsync,
    library_db_id: DbId,
    progress: Option<SyncRunProgress>,
) -> anyhow::Result<()> {
    let run_id = trace_run_id(progress.as_ref());
    let started = Instant::now();
    let work_id = if let Some(progress) = &progress {
        progress
            .start_work(
                SyncStageKey::Cleanup,
                SyncWorkDetails::new("cleanup_orphaned_metadata"),
            )
            .await
    } else {
        0
    };
    let result = {
        let mut db_write = db.write().await;
        cleanup_orphaned_metadata(&mut db_write)
    };
    if let Err(err) = result {
        if let Some(progress) = &progress {
            progress
                .fail_work(
                    work_id,
                    SyncStageKey::Cleanup,
                    SyncWorkDetails::new("cleanup_orphaned_metadata"),
                    err.to_string(),
                )
                .await;
        }
        return Err(err);
    }
    if let Some(progress) = &progress {
        progress.complete_work(work_id, SyncStageKey::Cleanup).await;
    }
    tracing::info!(
        library_db_id = library_db_id.0,
        run_id = %run_id,
        elapsed_ms = started.elapsed().as_millis() as u64,
        stage = "cleanup",
        "scan orphan metadata cleanup finished"
    );
    Ok(())
}

pub(crate) async fn sync_library_pipeline(
    db: &DbAsync,
    library: &Library,
    progress: Option<SyncRunProgress>,
) -> anyhow::Result<()> {
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;
    if let Some(progress) = &progress {
        progress.set_status(SyncRunStatus::Running).await;
        progress.check_cancelled().await?;
    }
    let entry_result = sync_entries_incremental(db, library, progress.clone()).await?;
    if let Some(progress) = &progress {
        progress.check_cancelled().await?;
    }
    let artifact_plans =
        process_metadata_for_entries(db, library, entry_result.altered_entries, progress.clone())
            .await?;
    if let Some(progress) = &progress {
        let provider_count = release_refresh_provider_ids(None, false).await?.len() as u64;
        progress
            .add_stage_total(
                SyncStageKey::ProviderRefresh,
                artifact_plans.len() as u64 * provider_count,
                SyncTotalState::Final,
            )
            .await;
        progress
            .add_stage_total(
                SyncStageKey::LocalCoverMetadata,
                artifact_plans.len() as u64,
                SyncTotalState::Final,
            )
            .await;
        progress
            .add_stage_total(
                SyncStageKey::Lyrics,
                artifact_plans
                    .iter()
                    .map(|plan| plan.track_ids.len() as u64)
                    .sum(),
                SyncTotalState::Final,
            )
            .await;
        progress
            .add_stage_total(
                SyncStageKey::ProviderCover,
                artifact_plans.len() as u64,
                SyncTotalState::Final,
            )
            .await;
        progress
            .add_stage_total(SyncStageKey::Cleanup, 2, SyncTotalState::Final)
            .await;
        progress.set_determinate().await;
    }
    futures::stream::iter(artifact_plans)
        .for_each_concurrent(MAX_CONCURRENT_ALBUM_PIPELINE, |plan| {
            let db = db.clone();
            let library_path = library.path.clone();
            let progress = progress.clone();
            async move {
                if let Some(progress) = &progress
                    && let Err(err) = progress.check_cancelled().await
                {
                    tracing::debug!(run_id = progress.run_id(), error = %err, "sync cancelled before release artifacts");
                    return;
                }
                if let Err(err) =
                    process_release_artifacts(&db, library_db_id, &library_path, plan, progress)
                        .await
                {
                    tracing::warn!(
                        library_db_id = library_db_id.0,
                        error = %err,
                        "scan release artifact pipeline failed"
                    );
                }
            }
        })
        .await;
    if let Some(progress) = &progress {
        progress.check_cancelled().await?;
    }
    prune_missing_entries_for_scan(db, library, &entry_result.observed_paths, progress.clone())
        .await?;
    cleanup_metadata_orphans(db, library_db_id, progress).await?;
    Ok(())
}

async fn process_metadata_for_entries(
    db: &DbAsync,
    library: &Library,
    entries: Vec<DbId>,
    progress: Option<SyncRunProgress>,
) -> anyhow::Result<Vec<ReleaseArtifactPlan>> {
    let run_id = trace_run_id(progress.as_ref());
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;

    if entries.is_empty() {
        return Ok(Vec::new());
    }

    let groups = {
        let db_read = db.read().await;
        group_entries(&db_read, library_db_id, entries)?
    };
    if let Some(progress) = &progress {
        progress
            .add_stage_total(
                SyncStageKey::MetadataParse,
                groups.len() as u64,
                SyncTotalState::Estimated,
            )
            .await;
    }

    let mapping_config = load_mapping_config(db).await?;

    let mut source_dirs_by_entry = BTreeMap::new();
    let mut parsed_groups = Vec::new();
    for (coalesce_group_key, entries) in groups.into_iter().enumerate() {
        if let Some(progress) = &progress {
            progress.check_cancelled().await?;
        }
        let source_dir =
            source_directory_for_group_entries(&entries).unwrap_or_else(|| library.path.clone());
        let source_dir_text = source_dir.to_string_lossy().into_owned();
        let work_id = if let Some(progress) = &progress {
            progress
                .start_work(
                    SyncStageKey::MetadataParse,
                    SyncWorkDetails::new("metadata_parse_group")
                        .source_dir(Some(source_dir_text.clone())),
                )
                .await
        } else {
            0
        };
        let entry_source_dirs: BTreeMap<DbId, std::path::PathBuf> = entries
            .iter()
            .filter_map(|entry| {
                if entry.kind != crate::db::entries::EntryKind::File {
                    return None;
                }
                let entry_db_id = entry.db_id?;
                let parent = entry
                    .full_path
                    .parent()
                    .map(std::path::Path::to_path_buf)
                    .unwrap_or_else(|| source_dir.clone());
                source_dirs_by_entry.insert(entry_db_id, parent.clone());
                Some((entry_db_id, parent))
            })
            .collect();
        let parse_output = match parse_metadata(&mapping_config, entries).await {
            Ok(output) => output,
            Err(err) => {
                if let Some(progress) = &progress {
                    progress
                        .fail_work(
                            work_id,
                            SyncStageKey::MetadataParse,
                            SyncWorkDetails::new("metadata_parse_group")
                                .source_dir(Some(source_dir_text)),
                            err.to_string(),
                        )
                        .await;
                }
                return Err(err);
            }
        };
        if !parse_output.skipped.is_empty() {
            log_skip_summary(&parse_output.skipped);
        }
        let metadata = parse_output.metadata;
        if metadata.is_empty() {
            if let Some(progress) = &progress {
                progress
                    .complete_work(work_id, SyncStageKey::MetadataParse)
                    .await;
            }
            continue;
        }

        let mut metadata_by_source_dir = BTreeMap::new();
        for track in metadata {
            let track_source_dir = entry_source_dirs
                .get(&track.entry_db_id)
                .cloned()
                .unwrap_or_else(|| source_dir.clone());
            metadata_by_source_dir
                .entry(track_source_dir)
                .or_insert_with(Vec::new)
                .push(track);
        }

        for (source_dir, metadata) in metadata_by_source_dir {
            parsed_groups.push(ParsedMetadataGroup {
                coalesce_group_key,
                source_dir,
                metadata,
            });
        }
        if let Some(progress) = &progress {
            progress
                .complete_work(work_id, SyncStageKey::MetadataParse)
                .await;
        }
    }

    let coalesced_groups = coalesce_disc_groups(parsed_groups);
    let coalesced_count = coalesced_groups.len();
    if let Some(progress) = &progress {
        progress
            .add_stage_total(
                SyncStageKey::MetadataApply,
                coalesced_count as u64,
                SyncTotalState::Estimated,
            )
            .await;
    }
    let apply_results = futures::stream::iter(coalesced_groups.into_iter().enumerate())
        .map(|(group_id, metadata)| {
            let db = db.clone();
            let progress = progress.clone();
            let run_id = run_id.clone();
            let source_dir = metadata
                .first()
                .and_then(|track| source_dirs_by_entry.get(&track.entry_db_id))
                .map(std::path::PathBuf::as_path)
                .map(std::path::Path::to_path_buf);
            async move {
                process_metadata_group(&db, library_db_id, group_id, source_dir, metadata, progress)
                    .await
                    .inspect_err(|err| {
                        tracing::warn!(
                            library_db_id = library_db_id.0,
                            run_id = %run_id,
                            group_id,
                            error = %err,
                            "scan metadata group pipeline failed"
                        );
                    })
            }
        })
        .buffer_unordered(MAX_CONCURRENT_ALBUM_PIPELINE)
        .collect::<Vec<_>>()
        .await;

    let mut release_ids = Vec::new();
    let mut seen_release_ids = HashSet::new();
    for result in apply_results {
        let apply_result = result?;
        for release_id in apply_result.releases {
            if seen_release_ids.insert(release_id) {
                release_ids.push(release_id);
            }
        }
    }
    tracing::info!(
        library_db_id = library_db_id.0,
        run_id = %run_id,
        group_count = coalesced_count,
        "scan metadata groups completed"
    );

    let mut plans = Vec::with_capacity(release_ids.len());
    for release_id in release_ids {
        if let Some(plan) = release_artifact_plan(db, release_id).await? {
            plans.push(plan);
        }
    }
    Ok(plans)
}

async fn process_metadata_group(
    db: &DbAsync,
    library_db_id: DbId,
    group_id: usize,
    source_dir: Option<std::path::PathBuf>,
    metadata: Vec<crate::services::metadata::TrackMetadata>,
    progress: Option<SyncRunProgress>,
) -> anyhow::Result<MetadataApplyResult> {
    let run_id = trace_run_id(progress.as_ref());
    if let Some(progress) = &progress {
        progress.check_cancelled().await?;
    }
    let source_dir_text = source_dir
        .as_ref()
        .map(|path| path.to_string_lossy().into_owned());
    let work_id = if let Some(progress) = &progress {
        progress
            .start_work(
                SyncStageKey::MetadataApply,
                SyncWorkDetails::new("metadata_apply_group").source_dir(source_dir_text.clone()),
            )
            .await
    } else {
        0
    };

    let started = Instant::now();
    tracing::debug!(
        library_db_id = library_db_id.0,
        run_id = %run_id,
        group_id,
        track_count = metadata.len(),
        stage = "metadata_apply",
        "scan group stage started"
    );

    let apply_write_result = {
        let mut db_write = db.write().await;
        apply_metadata(&mut db_write, library_db_id, metadata)
    };
    let apply_result = match apply_write_result {
        Ok(result) => result,
        Err(err) => {
            if let Some(progress) = &progress {
                progress
                    .fail_work(
                        work_id,
                        SyncStageKey::MetadataApply,
                        SyncWorkDetails::new("metadata_apply_group").source_dir(source_dir_text),
                        err.to_string(),
                    )
                    .await;
            }
            return Err(err);
        }
    };

    tracing::info!(
        library_db_id = library_db_id.0,
        run_id = %run_id,
        group_id,
        stage = "metadata_apply",
        release_count = apply_result.releases.len(),
        track_count = apply_result.tracks.len(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "scan group metadata persisted"
    );

    if let Some(progress) = &progress {
        progress
            .complete_work(work_id, SyncStageKey::MetadataApply)
            .await;
    }

    Ok(apply_result)
}

struct ReleaseArtifactPlan {
    release_id: DbId,
    release_public_id: Option<String>,
    release_title: Option<String>,
    track_ids: Vec<DbId>,
}

struct ReleaseArtifactContext<'a> {
    db: &'a DbAsync,
    library_db_id: DbId,
    library_path: &'a Path,
    release_id: DbId,
    release_public_id: Option<String>,
    release_title: Option<String>,
    run_id: String,
    progress: Option<SyncRunProgress>,
}

impl ReleaseArtifactContext<'_> {
    fn details(&self, stage: &'static str) -> SyncWorkDetails {
        SyncWorkDetails::release(
            stage,
            self.release_public_id.clone(),
            self.release_title.clone(),
        )
    }
}

async fn release_artifact_plan(
    db: &DbAsync,
    release_id: DbId,
) -> anyhow::Result<Option<ReleaseArtifactPlan>> {
    let db_read = db.read().await;
    let Some(release) = db::releases::get_by_id(&db_read, release_id)? else {
        return Ok(None);
    };
    let release_public_id = db::lookup::find_id_by_db_id(&db_read, release_id)?;
    let track_ids = db::tracks::get_direct(&db_read, release_id)?
        .into_iter()
        .filter_map(|track| track.db_id.map(DbId::from))
        .collect();
    Ok(Some(ReleaseArtifactPlan {
        release_id,
        release_public_id,
        release_title: Some(release.release_title),
        track_ids,
    }))
}

async fn process_release_artifacts(
    db: &DbAsync,
    library_db_id: DbId,
    library_path: &Path,
    plan: ReleaseArtifactPlan,
    progress: Option<SyncRunProgress>,
) -> anyhow::Result<()> {
    let run_id = trace_run_id(progress.as_ref());
    let release_id = plan.release_id;
    let release_public_id = plan.release_public_id.clone();
    let release_title = plan.release_title.clone();
    let context = ReleaseArtifactContext {
        db,
        library_db_id,
        library_path,
        release_id,
        release_public_id: release_public_id.clone(),
        release_title: release_title.clone(),
        run_id: run_id.clone(),
        progress: progress.clone(),
    };
    let options = LibraryRefreshOptions {
        replace_cover: false,
        force_refresh: false,
        apply_sync_filters: false,
        provider_id: None,
    };

    if let Some(progress) = &progress {
        progress.check_cancelled().await?;
    }
    tracing::debug!(
        library_db_id = library_db_id.0,
        run_id = %run_id,
        release_db_id = release_id.0,
        stage = "provider_refresh",
        "scan release stage started"
    );
    refresh_release_metadata_for_scan_with_progress(
        library_db_id,
        release_id,
        &options,
        progress.clone(),
        release_public_id.clone(),
        release_title.clone(),
    )
    .await?;

    let _ = sync_local_release_cover_metadata(&context).await;

    let _ = dispatch_release_lyrics(
        library_db_id,
        release_id,
        &plan,
        run_id.clone(),
        progress.clone(),
    )
    .await?;

    let _ = sync_provider_release_cover(&context).await;
    Ok(())
}

async fn sync_local_release_cover_metadata(context: &ReleaseArtifactContext<'_>) -> bool {
    let started = Instant::now();
    let details = context.details("local_cover_metadata");
    let work_id = if let Some(progress) = &context.progress {
        progress
            .start_work(SyncStageKey::LocalCoverMetadata, details.clone())
            .await
    } else {
        0
    };
    let covers_root = configured_covers_root();
    let cover_paths = CoverPaths {
        library_root: Some(context.library_path),
        covers_root: &covers_root,
    };
    let resolved = {
        let db_read = context.db.read().await;
        match resolve_cover_for_release_id(&db_read, context.release_id, cover_paths) {
            Ok(path) => path,
            Err(err) => {
                tracing::warn!(
                    library_db_id = context.library_db_id.0,
                    run_id = %context.run_id,
                    release_db_id = context.release_id.0,
                    stage = "local_cover_metadata",
                    error = %err,
                    "scan release local cover resolution failed"
                );
                None
            }
        }
    };

    let Some(cover_path) = resolved else {
        tracing::debug!(
            library_db_id = context.library_db_id.0,
            run_id = %context.run_id,
            release_db_id = context.release_id.0,
            stage = "local_cover_metadata",
            "scan release local cover not found"
        );
        if let Some(progress) = &context.progress {
            progress
                .complete_work(work_id, SyncStageKey::LocalCoverMetadata)
                .await;
        }
        return false;
    };

    match upsert_release_cover_metadata(context.db, context.release_id, &cover_path).await {
        Ok(changed) => {
            if let Some(progress) = &context.progress {
                progress
                    .complete_work(work_id, SyncStageKey::LocalCoverMetadata)
                    .await;
            }
            tracing::info!(
                library_db_id = context.library_db_id.0,
                run_id = %context.run_id,
                release_db_id = context.release_id.0,
                stage = "local_cover_metadata",
                changed,
                cover_path = %cover_path.display(),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "scan release local cover metadata synced"
            );
            changed
        }
        Err(err) => {
            if let Some(progress) = &context.progress {
                progress
                    .fail_work(
                        work_id,
                        SyncStageKey::LocalCoverMetadata,
                        details,
                        err.to_string(),
                    )
                    .await;
            }
            tracing::warn!(
                library_db_id = context.library_db_id.0,
                run_id = %context.run_id,
                release_db_id = context.release_id.0,
                stage = "local_cover_metadata",
                cover_path = %cover_path.display(),
                error = %err,
                "scan release local cover metadata sync failed"
            );
            false
        }
    }
}

async fn dispatch_release_lyrics(
    library_db_id: DbId,
    release_id: DbId,
    plan: &ReleaseArtifactPlan,
    run_id: String,
    progress: Option<SyncRunProgress>,
) -> anyhow::Result<usize> {
    if plan.track_ids.is_empty() {
        return Ok(0);
    }

    let started = Instant::now();
    let track_count = plan.track_ids.len();
    futures::stream::iter(plan.track_ids.clone())
        .for_each_concurrent(MAX_CONCURRENT_DISPATCHES, |track_db_id| {
            let progress = progress.clone();
            let run_id = run_id.clone();
            let details = SyncWorkDetails::release(
                "lyrics_track",
                plan.release_public_id.clone(),
                plan.release_title.clone(),
            );
            async move {
                let work_id = if let Some(progress) = &progress {
                    progress
                        .start_work(SyncStageKey::Lyrics, details.clone())
                        .await
                } else {
                    0
                };
                match dispatch_lyrics_for_track(track_db_id, false).await {
                    Ok(()) => {
                        if let Some(progress) = &progress {
                            progress.complete_work(work_id, SyncStageKey::Lyrics).await;
                        }
                    }
                    Err(err) => {
                        if let Some(progress) = &progress {
                            progress
                                .fail_work(work_id, SyncStageKey::Lyrics, details, err.to_string())
                                .await;
                        }
                        tracing::warn!(
                            library_db_id = library_db_id.0,
                            run_id = %run_id,
                            release_db_id = release_id.0,
                            track_db_id = track_db_id.0,
                            error = %err,
                            "lyrics dispatch failed for scanned track"
                        );
                    }
                }
            }
        })
        .await;

    tracing::info!(
        library_db_id = library_db_id.0,
        run_id = %run_id,
        release_db_id = release_id.0,
        stage = "lyrics",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "scan release lyrics dispatch completed"
    );
    Ok(track_count)
}

async fn sync_provider_release_cover(context: &ReleaseArtifactContext<'_>) -> bool {
    let started = Instant::now();
    let details = context.details("provider_cover");
    let work_id = if let Some(progress) = &context.progress {
        progress
            .start_work(SyncStageKey::ProviderCover, details.clone())
            .await
    } else {
        0
    };
    let covers_root = configured_covers_root();
    let cover_paths = CoverPaths {
        library_root: Some(context.library_path),
        covers_root: &covers_root,
    };

    let release_bundle = {
        let db_read = context.db.read().await;
        match db::releases::get_by_id(&db_read, context.release_id) {
            Ok(Some(release)) => {
                let tracks = db::tracks::get(&db_read, context.release_id).unwrap_or_default();
                let artists = db::artists::get(&db_read, context.release_id).unwrap_or_default();
                Ok(Some((release, tracks, artists)))
            }
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    };
    let (release, tracks, artists) = match release_bundle {
        Ok(Some(bundle)) => bundle,
        Ok(None) => {
            if let Some(progress) = &context.progress {
                progress
                    .fail_work(
                        work_id,
                        SyncStageKey::ProviderCover,
                        details,
                        "release missing before provider cover sync",
                    )
                    .await;
            }
            return false;
        }
        Err(err) => {
            if let Some(progress) = &context.progress {
                progress
                    .fail_work(
                        work_id,
                        SyncStageKey::ProviderCover,
                        details,
                        err.to_string(),
                    )
                    .await;
            }
            tracing::warn!(
                library_db_id = context.library_db_id.0,
                run_id = %context.run_id,
                release_db_id = context.release_id.0,
                stage = "provider_cover",
                error = %err,
                "scan release load failed before provider cover sync"
            );
            return false;
        }
    };

    let synced = match sync_release_cover_for_tracks(
        context.db,
        &tracks,
        &release,
        &artists,
        cover_paths,
        CoverSyncOptions {
            replace_existing: false,
            force_refresh: false,
        },
    )
    .await
    {
        Ok(synced) => {
            if let Some(progress) = &context.progress {
                progress
                    .complete_work(work_id, SyncStageKey::ProviderCover)
                    .await;
            }
            synced
        }
        Err(err) => {
            if let Some(progress) = &context.progress {
                progress
                    .fail_work(
                        work_id,
                        SyncStageKey::ProviderCover,
                        details.clone(),
                        err.to_string(),
                    )
                    .await;
            }
            tracing::warn!(
                library_db_id = context.library_db_id.0,
                run_id = %context.run_id,
                release_db_id = context.release_id.0,
                stage = "provider_cover",
                error = %err,
                "scan release provider cover sync failed"
            );
            false
        }
    };

    let resolved = {
        let db_read = context.db.read().await;
        resolve_cover_for_release_id(&db_read, context.release_id, cover_paths)
            .ok()
            .flatten()
    };
    if let Some(cover_path) = resolved
        && let Err(err) =
            upsert_release_cover_metadata(context.db, context.release_id, &cover_path).await
    {
        tracing::warn!(
            library_db_id = context.library_db_id.0,
            run_id = %context.run_id,
            release_db_id = context.release_id.0,
            stage = "provider_cover",
            cover_path = %cover_path.display(),
            error = %err,
            "scan release provider cover metadata sync failed"
        );
    }

    tracing::info!(
        library_db_id = context.library_db_id.0,
        run_id = %context.run_id,
        release_db_id = context.release_id.0,
        stage = "provider_cover",
        synced,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "scan release provider cover stage completed"
    );
    synced
}

pub(crate) async fn sync_library(db: &DbAsync, library: &Library) -> anyhow::Result<()> {
    sync_library_pipeline(db, library, None).await
}
