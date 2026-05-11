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
    sync::{
        Arc,
        atomic::{
            AtomicUsize,
            Ordering,
        },
    },
    time::Instant,
};

use super::{
    orchestrator::{
        LibrarySyncProgress,
        LibrarySyncStage,
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
            refresh_release_metadata_for_scan,
        },
    },
};

pub(crate) const MAX_CONCURRENT_ALBUM_PIPELINE: usize = 4;

fn trace_run_id(progress: Option<LibrarySyncProgress>) -> u64 {
    progress
        .map(LibrarySyncProgress::run_id)
        .unwrap_or_default()
}

#[derive(Debug, Default)]
struct IncrementalEntrySyncResult {
    observed_paths: HashSet<std::path::PathBuf>,
}

async fn sync_entries_incremental(
    db: &DbAsync,
    library: &Library,
    progress: Option<LibrarySyncProgress>,
) -> anyhow::Result<IncrementalEntrySyncResult> {
    let run_id = trace_run_id(progress);
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;
    if let Some(progress) = progress {
        progress.set_stage(LibrarySyncStage::EntrySync).await;
    }

    let existing = {
        let db_read = db.read().await;
        db::entries::get(&db_read, library_db_id)?
    };
    let scan_plan = prepare_entry_scan_plan(library, existing)?;
    let entry_group_count = scan_plan.groups.len();
    if let Some(progress) = progress {
        progress.set_entry_groups(entry_group_count).await;
    }
    tracing::info!(
        library_db_id = library_db_id.0,
        run_id,
        entry_group_count,
        observed_entry_count = scan_plan.observed_paths.len(),
        stage = "entry_sync",
        "scan entry groups discovered"
    );

    let metadata_group_ids = Arc::new(AtomicUsize::new(0));
    let group_results = futures::stream::iter(scan_plan.groups.into_iter().enumerate())
        .map(|(entry_group_id, group)| {
            let db = db.clone();
            let library = library.clone();
            let metadata_group_ids = metadata_group_ids.clone();
            async move {
                let started = Instant::now();
                let source_dir = group.source_dir.clone();
                let entry_count = group.entries.len();
                tracing::debug!(
                    library_db_id = library_db_id.0,
                    run_id,
                    entry_group_id,
                    source_dir = %source_dir.display(),
                    entry_count,
                    stage = "entry_sync",
                    "scan entry group stage started"
                );
                let entries = tokio::task::spawn_blocking(move || hash_entry_group(group.entries))
                    .await
                    .map_err(anyhow::Error::from)?;
                let result = {
                    let mut db_write = db.write().await;
                    sync_entry_group(&mut db_write, &library, entries)?
                };
                tracing::info!(
                    library_db_id = library_db_id.0,
                    run_id,
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
                if let Some(progress) = progress {
                    progress
                        .add_entry_counts(result.added, result.updated, 0)
                        .await;
                }
                if !result.altered.is_empty() {
                    process_metadata_for_entries(
                        &db,
                        &library,
                        result.altered,
                        progress,
                        metadata_group_ids,
                    )
                    .await?;
                }
                Ok::<_, anyhow::Error>(())
            }
        })
        .buffer_unordered(MAX_CONCURRENT_ALBUM_PIPELINE)
        .collect::<Vec<_>>()
        .await;

    let mut result = IncrementalEntrySyncResult::default();
    for group_result in group_results {
        group_result?;
    }

    result.observed_paths = scan_plan.observed_paths;

    Ok(result)
}

async fn prune_missing_entries_for_scan(
    db: &DbAsync,
    library: &Library,
    observed_paths: &HashSet<std::path::PathBuf>,
    progress: Option<LibrarySyncProgress>,
) -> anyhow::Result<usize> {
    let run_id = trace_run_id(progress);
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;
    let prune_started = Instant::now();
    if let Some(progress) = progress {
        progress.set_stage(LibrarySyncStage::Cleanup).await;
    }
    let prune_result = {
        let mut db_write = db.write().await;
        prune_missing_entries(&mut db_write, library, observed_paths)?
    };
    if let Some(progress) = progress {
        progress.add_entry_counts(0, 0, prune_result.deleted).await;
    }
    tracing::info!(
        library_db_id = library_db_id.0,
        run_id,
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
    progress: Option<LibrarySyncProgress>,
) -> anyhow::Result<()> {
    let run_id = trace_run_id(progress);
    if let Some(progress) = progress {
        progress.set_stage(LibrarySyncStage::Cleanup).await;
    }
    let started = Instant::now();
    let mut db_write = db.write().await;
    cleanup_orphaned_metadata(&mut db_write)?;
    drop(db_write);
    tracing::info!(
        library_db_id = library_db_id.0,
        run_id,
        elapsed_ms = started.elapsed().as_millis() as u64,
        stage = "cleanup",
        "scan orphan metadata cleanup finished"
    );
    Ok(())
}

pub(crate) async fn sync_library_pipeline(
    db: &DbAsync,
    library: &Library,
    progress: Option<LibrarySyncProgress>,
) -> anyhow::Result<()> {
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;
    let entry_result = sync_entries_incremental(db, library, progress).await?;
    prune_missing_entries_for_scan(db, library, &entry_result.observed_paths, progress).await?;
    cleanup_metadata_orphans(db, library_db_id, progress).await?;
    Ok(())
}

async fn process_metadata_for_entries(
    db: &DbAsync,
    library: &Library,
    entries: Vec<DbId>,
    progress: Option<LibrarySyncProgress>,
    metadata_group_ids: Arc<AtomicUsize>,
) -> anyhow::Result<()> {
    let run_id = trace_run_id(progress);
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;

    if entries.is_empty() {
        return Ok(());
    }

    if let Some(progress) = progress {
        progress.set_stage(LibrarySyncStage::MetadataParse).await;
    }

    let groups = {
        let db_read = db.read().await;
        group_entries(&db_read, library_db_id, entries)?
    };

    let mapping_config = load_mapping_config(db).await?;

    let mut source_dirs_by_entry = BTreeMap::new();
    let mut parsed_groups = Vec::new();
    for (coalesce_group_key, entries) in groups.into_iter().enumerate() {
        let source_dir =
            source_directory_for_group_entries(&entries).unwrap_or_else(|| library.path.clone());
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
        let parse_output = parse_metadata(&mapping_config, entries).await?;
        if !parse_output.skipped.is_empty() {
            log_skip_summary(&parse_output.skipped);
        }
        let metadata = parse_output.metadata;
        if metadata.is_empty() {
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
    }

    let coalesced_groups = coalesce_disc_groups(parsed_groups);
    let coalesced_count = coalesced_groups.len();
    if let Some(progress) = progress {
        progress.add_discovered_groups(coalesced_count).await;
    }
    futures::stream::iter(coalesced_groups)
        .for_each_concurrent(MAX_CONCURRENT_ALBUM_PIPELINE, |metadata| {
            let group_id = metadata_group_ids.fetch_add(1, Ordering::Relaxed);
            let db = db.clone();
            let library_path = library.path.clone();
            let source_dir = metadata
                .first()
                .and_then(|track| source_dirs_by_entry.get(&track.entry_db_id))
                .map(std::path::PathBuf::as_path)
                .map(std::path::Path::to_path_buf);
            async move {
                if let Some(progress) = progress {
                    progress.start_group(group_id, source_dir.as_deref()).await;
                }
                if metadata.is_empty() {
                    if let Some(progress) = progress {
                        progress.complete_group(group_id).await;
                    }
                    return;
                }
                match process_metadata_group(
                    &db,
                    library_db_id,
                    &library_path,
                    group_id,
                    metadata,
                    progress,
                )
                .await
                {
                    Ok(()) => {
                        if let Some(progress) = progress {
                            progress.complete_group(group_id).await;
                        }
                    }
                    Err(err) => {
                        tracing::warn!(
                            library_db_id = library_db_id.0,
                            run_id,
                            group_id,
                            error = %err,
                            "scan metadata group pipeline failed"
                        );
                        if let Some(progress) = progress {
                            progress
                                .fail_group(group_id, LibrarySyncStage::MetadataApply)
                                .await;
                        }
                    }
                }
            }
        })
        .await;

    tracing::info!(
        library_db_id = library_db_id.0,
        run_id,
        group_count = coalesced_count,
        "scan metadata groups completed"
    );

    Ok(())
}

async fn process_metadata_group(
    db: &DbAsync,
    library_db_id: DbId,
    library_path: &Path,
    group_id: usize,
    metadata: Vec<crate::services::metadata::TrackMetadata>,
    progress: Option<LibrarySyncProgress>,
) -> anyhow::Result<()> {
    let run_id = trace_run_id(progress);
    if let Some(progress) = progress {
        progress
            .update_group_stage(group_id, LibrarySyncStage::MetadataApply, None, None, None)
            .await;
    }

    let started = Instant::now();
    tracing::debug!(
        library_db_id = library_db_id.0,
        run_id,
        group_id,
        track_count = metadata.len(),
        stage = "metadata_apply",
        "scan group stage started"
    );

    let apply_result = {
        let mut db_write = db.write().await;
        apply_metadata(&mut db_write, library_db_id, metadata)?
    };

    tracing::info!(
        library_db_id = library_db_id.0,
        run_id,
        group_id,
        stage = "metadata_apply",
        release_count = apply_result.releases.len(),
        track_count = apply_result.tracks.len(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "scan group metadata persisted"
    );

    if let Some(progress) = progress {
        progress
            .add_counts(
                apply_result.releases.len(),
                apply_result.tracks.len(),
                0,
                0,
                0,
                0,
            )
            .await;
    }

    process_group_releases(
        db,
        library_db_id,
        library_path,
        group_id,
        apply_result,
        progress,
    )
    .await
}

async fn process_group_releases(
    db: &DbAsync,
    library_db_id: DbId,
    library_path: &Path,
    group_id: usize,
    apply_result: MetadataApplyResult,
    progress: Option<LibrarySyncProgress>,
) -> anyhow::Result<()> {
    for release_id in apply_result.releases {
        process_release_artifacts(
            db,
            library_db_id,
            library_path,
            group_id,
            release_id,
            progress,
        )
        .await?;
    }
    Ok(())
}

async fn process_release_artifacts(
    db: &DbAsync,
    library_db_id: DbId,
    library_path: &Path,
    group_id: usize,
    release_id: DbId,
    progress: Option<LibrarySyncProgress>,
) -> anyhow::Result<()> {
    let run_id = trace_run_id(progress);
    let release_title = {
        let db_read = db.read().await;
        db::releases::get_by_id(&db_read, release_id)?.map(|release| release.release_title)
    };
    let options = LibraryRefreshOptions {
        replace_cover: false,
        force_refresh: false,
        apply_sync_filters: false,
        provider_id: None,
    };

    let provider_started = Instant::now();
    if let Some(progress) = progress {
        progress
            .update_group_stage(
                group_id,
                LibrarySyncStage::ProviderRefresh,
                Some(release_id),
                release_title.clone(),
                None,
            )
            .await;
    }
    tracing::debug!(
        library_db_id = library_db_id.0,
        run_id,
        group_id,
        release_db_id = release_id.0,
        stage = "provider_refresh",
        "scan release stage started"
    );
    let provider_refreshes =
        match refresh_release_metadata_for_scan(library_db_id, release_id, &options).await {
            Ok(refreshed) => {
                tracing::info!(
                    library_db_id = library_db_id.0,
                    run_id,
                    group_id,
                    release_db_id = release_id.0,
                    stage = "provider_refresh",
                    refreshed,
                    elapsed_ms = provider_started.elapsed().as_millis() as u64,
                    "scan release provider refresh completed"
                );
                refreshed
            }
            Err(err) => {
                tracing::warn!(
                    library_db_id = library_db_id.0,
                    run_id,
                    group_id,
                    release_db_id = release_id.0,
                    stage = "provider_refresh",
                    error = %err,
                    elapsed_ms = provider_started.elapsed().as_millis() as u64,
                    "scan release provider refresh failed"
                );
                0
            }
        };
    if let Some(progress) = progress {
        progress.add_counts(0, 0, provider_refreshes, 0, 0, 0).await;
    }

    if let Some(progress) = progress {
        progress
            .update_group_stage(
                group_id,
                LibrarySyncStage::LocalCoverMetadata,
                Some(release_id),
                release_title.clone(),
                None,
            )
            .await;
    }
    let local_cover_metadata = sync_local_release_cover_metadata(
        db,
        library_db_id,
        library_path,
        group_id,
        release_id,
        run_id,
    )
    .await;
    if let Some(progress) = progress {
        progress
            .add_counts(0, 0, 0, 0, usize::from(local_cover_metadata), 0)
            .await;
    }

    if let Some(progress) = progress {
        progress
            .update_group_stage(
                group_id,
                LibrarySyncStage::Lyrics,
                Some(release_id),
                release_title.clone(),
                None,
            )
            .await;
    }
    let lyrics_tracks =
        dispatch_release_lyrics(db, library_db_id, group_id, release_id, run_id).await?;
    if let Some(progress) = progress {
        progress.add_counts(0, 0, 0, lyrics_tracks, 0, 0).await;
    }

    if let Some(progress) = progress {
        progress
            .update_group_stage(
                group_id,
                LibrarySyncStage::ProviderCover,
                Some(release_id),
                release_title,
                None,
            )
            .await;
    }
    let provider_cover = sync_provider_release_cover(
        db,
        library_db_id,
        library_path,
        group_id,
        release_id,
        run_id,
    )
    .await;
    if let Some(progress) = progress {
        progress
            .add_counts(0, 0, 0, 0, 0, usize::from(provider_cover))
            .await;
    }
    Ok(())
}

async fn sync_local_release_cover_metadata(
    db: &DbAsync,
    library_db_id: DbId,
    library_path: &Path,
    group_id: usize,
    release_id: DbId,
    run_id: u64,
) -> bool {
    let started = Instant::now();
    let covers_root = configured_covers_root();
    let cover_paths = CoverPaths {
        library_root: Some(library_path),
        covers_root: covers_root.as_deref(),
    };
    let resolved = {
        let db_read = db.read().await;
        match resolve_cover_for_release_id(&db_read, release_id, cover_paths) {
            Ok(path) => path,
            Err(err) => {
                tracing::warn!(
                    library_db_id = library_db_id.0,
                    run_id,
                    group_id,
                    release_db_id = release_id.0,
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
            library_db_id = library_db_id.0,
            run_id,
            group_id,
            release_db_id = release_id.0,
            stage = "local_cover_metadata",
            "scan release local cover not found"
        );
        return false;
    };

    match upsert_release_cover_metadata(db, release_id, &cover_path).await {
        Ok(changed) => {
            tracing::info!(
                library_db_id = library_db_id.0,
                run_id,
                group_id,
                release_db_id = release_id.0,
                stage = "local_cover_metadata",
                changed,
                cover_path = %cover_path.display(),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "scan release local cover metadata synced"
            );
            changed
        }
        Err(err) => {
            tracing::warn!(
                library_db_id = library_db_id.0,
                run_id,
                group_id,
                release_db_id = release_id.0,
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
    db: &DbAsync,
    library_db_id: DbId,
    group_id: usize,
    release_id: DbId,
    run_id: u64,
) -> anyhow::Result<usize> {
    let tracks = {
        let db_read = db.read().await;
        db::tracks::get_direct(&db_read, release_id)?
    };
    let track_ids: Vec<DbId> = tracks
        .into_iter()
        .filter_map(|track| track.db_id.map(DbId::from))
        .collect();
    if track_ids.is_empty() {
        return Ok(0);
    }

    let started = Instant::now();
    let track_count = track_ids.len();
    futures::stream::iter(track_ids)
        .for_each_concurrent(MAX_CONCURRENT_DISPATCHES, |track_db_id| async move {
            if let Err(err) = dispatch_lyrics_for_track(track_db_id, false).await {
                tracing::warn!(
                    library_db_id = library_db_id.0,
                    run_id,
                    group_id,
                    release_db_id = release_id.0,
                    track_db_id = track_db_id.0,
                    error = %err,
                    "lyrics dispatch failed for scanned track"
                );
            }
        })
        .await;

    tracing::info!(
        library_db_id = library_db_id.0,
        run_id,
        group_id,
        release_db_id = release_id.0,
        stage = "lyrics",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "scan release lyrics dispatch completed"
    );
    Ok(track_count)
}

async fn sync_provider_release_cover(
    db: &DbAsync,
    library_db_id: DbId,
    library_path: &Path,
    group_id: usize,
    release_id: DbId,
    run_id: u64,
) -> bool {
    let started = Instant::now();
    let covers_root = configured_covers_root();
    let cover_paths = CoverPaths {
        library_root: Some(library_path),
        covers_root: covers_root.as_deref(),
    };

    let Some((release, tracks, artists)) = ({
        let db_read = db.read().await;
        let release = match db::releases::get_by_id(&db_read, release_id) {
            Ok(release) => release,
            Err(err) => {
                tracing::warn!(
                    library_db_id = library_db_id.0,
                    run_id,
                    group_id,
                    release_db_id = release_id.0,
                    stage = "provider_cover",
                    error = %err,
                    "scan release load failed before provider cover sync"
                );
                None
            }
        };
        release.map(|release| {
            let tracks = db::tracks::get(&db_read, release_id).unwrap_or_default();
            let artists = db::artists::get(&db_read, release_id).unwrap_or_default();
            (release, tracks, artists)
        })
    }) else {
        return false;
    };

    let synced = match sync_release_cover_for_tracks(
        db,
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
        Ok(synced) => synced,
        Err(err) => {
            tracing::warn!(
                library_db_id = library_db_id.0,
                run_id,
                group_id,
                release_db_id = release_id.0,
                stage = "provider_cover",
                error = %err,
                "scan release provider cover sync failed"
            );
            false
        }
    };

    let resolved = {
        let db_read = db.read().await;
        resolve_cover_for_release_id(&db_read, release_id, cover_paths)
            .ok()
            .flatten()
    };
    if let Some(cover_path) = resolved {
        if let Err(err) = upsert_release_cover_metadata(db, release_id, &cover_path).await {
            tracing::warn!(
                library_db_id = library_db_id.0,
                run_id,
                group_id,
                release_db_id = release_id.0,
                stage = "provider_cover",
                cover_path = %cover_path.display(),
                error = %err,
                "scan release provider cover metadata sync failed"
            );
        }
    }

    tracing::info!(
        library_db_id = library_db_id.0,
        run_id,
        group_id,
        release_db_id = release_id.0,
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
