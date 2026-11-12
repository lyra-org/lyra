// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::DbId;
use futures::StreamExt;
use std::{
    collections::{
        BTreeMap,
        HashMap,
        HashSet,
    },
    path::{
        Path,
        PathBuf,
    },
    sync::Arc,
    time::Instant,
};

use super::{
    MediaRoots,
    orchestrator::{
        SyncRunControlError,
        SyncRunProgress,
        SyncRunStatus,
        SyncStageKey,
        SyncTotalState,
        SyncWorkDetails,
    },
    scanning::{
        EntryScanPlan,
        FollowedLinks,
        ScanError,
        ScanGaps,
        hash_entry_group,
        prepare_entry_scan_plan,
    },
};
pub(crate) use crate::services::system::library_sync_context as system_context;
use crate::services::{
    CoverPaths,
    CoverSyncOptions,
    STATE,
    covers::{
        AudioFolders,
        ReleaseCoverSubject,
        configured_covers_root,
        record_local_release_cover,
        refresh_gone_covers_for_library,
        revalidate_local_covers_for_library,
        sync_release_cover_for_tracks,
    },
    metadata::{
        CueLinks,
        TrackMetadata,
        cleanup,
        ingestion::{
            MetadataApplyResult,
            MetadataGroup,
            albums_left_by,
            apply_metadata,
            coalesce_group_tracks,
            metadata_groups,
        },
        load_mapping_config,
        log_skip_summary,
        lyrics::providers::{
            MAX_CONCURRENT_DISPATCHES,
            dispatch_for_track as dispatch_lyrics_for_track,
        },
        parse_metadata,
        skip_reason_summary,
    },
    providers::{
        LibraryRefreshOptions,
        ListedArtist,
        ScanRefreshPlan,
        deduplicate_after_providers,
        node_is_entity,
        provider_registry,
        refresh_library_metadata_after_others,
        refresh_release_metadata_for_scan,
        sync_artist_covers,
    },
};
use lyra_db::{
    DbAsync,
    Entry,
    Library,
    entries::{
        EntryKind,
        EntrySyncResult,
        StoredEntries,
        mark_missing,
    },
    metadata::mapping_config::MetadataMappingConfig,
};

pub(crate) const MAX_CONCURRENT_ALBUM_PIPELINE: usize = 4;

/// How many directories one write applies, so a first scan of a large library doesn't hold the
/// write lock for all of them at once.
const DIR_BATCH: usize = 1024;

/// Which releases a library sync asks the providers about.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SyncReleases {
    /// Only the releases the scan found new or changed.
    #[default]
    Changed,
    /// Every release in the library, as a refresh does, once the scan applied the files.
    All,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct LibrarySyncOptions {
    pub(crate) releases: SyncReleases,
    /// The covers the releases the providers are asked about fetch.
    pub(crate) covers: CoverSyncOptions,
}

/// What the scan saw: the paths it observed and those it couldn't read.
pub(crate) struct ScanObservation {
    pub(super) observed_paths: HashSet<std::path::PathBuf>,
    protected_paths: Vec<std::path::PathBuf>,
}

pub(crate) async fn sync_entries_incremental(
    db: &DbAsync,
    library: &Library,
    progress: SyncRunProgress,
) -> anyhow::Result<ScanObservation> {
    let run_id = progress.run_id().to_string();
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;

    let (existing, followed) = {
        let db_read = db.read().await;
        let followed = if library.follows_links() {
            Some(FollowedLinks::new(MediaRoots::for_principal(
                &*db_read, None,
            )?))
        } else {
            None
        };
        (
            lyra_db::entries::load_existing(&*db_read, library_db_id)?,
            followed,
        )
    };
    let scan = prepare_entry_scan_plan(library, existing, followed);
    // A cancelled run leaves a walk under way to finish on its own; it only reads.
    let scan_plan = tokio::select! {
        plan = scan => plan,
        err = progress.cancelled() => return Err(err.into()),
    };
    let scan_plan = match scan_plan {
        Ok(plan) => plan,
        Err(err) => {
            if let Some(ScanError::EmptyRoot { gaps }) = err.downcast_ref() {
                report_scan_gaps(gaps, &progress).await;
            }
            return Err(err);
        }
    };
    report_scan_gaps(&scan_plan.gaps, &progress).await;
    let protected_paths = scan_plan.gaps.protected_paths();
    let EntryScanPlan {
        dirs,
        groups,
        mut stored,
        observed_paths,
        moved_from,
        ..
    } = scan_plan;
    let entry_group_count = groups.len();
    progress
        .add_stage_total(
            SyncStageKey::EntrySync,
            entry_group_count as u64,
            SyncTotalState::Estimated,
        )
        .await;
    progress.set_estimating().await;
    tracing::info!(
        library_db_id = library_db_id.0,
        run_id,
        entry_group_count,
        observed_entry_count = observed_paths.len(),
        stage = "entry_sync",
        "scan entry groups discovered"
    );

    if !moved_from.is_empty() {
        #[cfg(test)]
        tests::before_unsync_left_by_moves()?;
        lyra_db::entries::unsync_left_by_moves(
            &mut *db.write().await,
            library,
            &observed_paths,
            &moved_from,
            |moved, present| albums_left_by(&library.path, moved, present),
        )?;
    }
    sync_entry_dirs(db, &mut stored, dirs).await?;

    let stored = &stored;
    let group_results = futures::stream::iter(groups.into_iter().enumerate())
        .map(|(entry_group_id, group)| {
            let db = db.clone();
            let progress = progress.clone();
            let run_id = run_id.clone();
            async move {
                progress.check_cancelled()?;
                let started = Instant::now();
                let source_dir = group.source_dir.clone();
                let entry_count = group.entries.len();
                let work_id = progress
                    .start_work(
                        SyncStageKey::EntrySync,
                        SyncWorkDetails::new("entry_group_sync")
                            .source_dir(Some(source_dir.to_string_lossy().into_owned())),
                    )
                    .await;
                tracing::debug!(
                    library_db_id = library_db_id.0,
                    run_id = %run_id,
                    entry_group_id,
                    source_dir = %source_dir.display(),
                    entry_count,
                    stage = "entry_sync",
                    "scan entry group stage started"
                );
                #[cfg(test)]
                tests::before_entry_hash(&source_dir).await;
                let result = apply_entry_group(&db, stored, group.entries, &progress).await;
                #[cfg(test)]
                if result.is_ok() {
                    tests::entry_group_applied(&source_dir);
                }

                let result = match result {
                    Ok(result) => result,
                    Err(err) if err.downcast_ref::<SyncRunControlError>().is_some() => {
                        return Err(err);
                    }
                    Err(err) => {
                        progress
                            .fail_work(
                                work_id,
                                SyncStageKey::EntrySync,
                                SyncWorkDetails::new("entry_group_sync")
                                    .source_dir(Some(source_dir.to_string_lossy().into_owned())),
                                format!("{err:#}"),
                            )
                            .await;
                        return Err(err);
                    }
                };
                tracing::info!(
                    library_db_id = library_db_id.0,
                    run_id = %run_id,
                    entry_group_id,
                    source_dir = %source_dir.display(),
                    entry_count,
                    added = result.added.len(),
                    updated = result.updated,
                    moved = result.moved,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    stage = "entry_sync",
                    "scan entry group synced"
                );
                progress
                    .complete_work(work_id, SyncStageKey::EntrySync)
                    .await;
                Ok::<_, anyhow::Error>(())
            }
        })
        .buffer_unordered(MAX_CONCURRENT_ALBUM_PIPELINE)
        .collect::<Vec<_>>()
        .await;

    let mut failure = None;
    for err in group_results.into_iter().filter_map(Result::err) {
        if failure.is_none() || err.downcast_ref::<SyncRunControlError>().is_some() {
            failure = Some(err);
        }
    }
    if let Some(err) = failure {
        return Err(err);
    }

    Ok(ScanObservation {
        observed_paths,
        protected_paths,
    })
}

/// Hashes the group's files and applies what changed, in one write. A group with no changes takes
/// no write lock, and so counts as no catalog change. The group applies only if the run is not
/// cancelled once it holds the write lock; a cancelled run starts hashing none of its other files.
async fn apply_entry_group(
    db: &DbAsync,
    stored: &StoredEntries,
    entries: Vec<Entry>,
    progress: &SyncRunProgress,
) -> anyhow::Result<EntrySyncResult> {
    progress.check_cancelled()?;
    let entries = tokio::select! {
        entries = hash_entry_group(entries) => entries?,
        err = progress.cancelled() => return Err(err.into()),
    };
    #[cfg(test)]
    tests::after_entry_hash(&entries).await?;
    let changes = stored.changes(entries);
    if changes.is_empty() {
        return Ok(EntrySyncResult::default());
    }
    let mut db_write = db.write().await;
    progress.check_cancelled()?;
    stored.apply_entry_changes(&mut db_write, changes)
}

/// Applies the scanned directories, the ones holding others first, a batch per write, recording
/// the new ones so the files under them find them. A failure stops the sync, since the files would
/// have no parent to go under. Directories left without their files by that are the next sync's:
/// it applies the files, or removes the directories once they are gone.
pub(super) async fn sync_entry_dirs(
    db: &DbAsync,
    stored: &mut StoredEntries,
    dirs: Vec<Entry>,
) -> anyhow::Result<()> {
    for batch in dirs.chunks(DIR_BATCH) {
        let changes = stored.changes(batch.to_vec());
        if changes.is_empty() {
            continue;
        }
        let result = {
            let mut db_write = db.write().await;
            stored.apply_entry_changes(&mut db_write, changes)?
        };
        stored.record(result.added);
    }
    Ok(())
}

/// Reports each path the scan couldn't read as a failed unit and each link it left out as a
/// skipped one, so the run shows them.
async fn report_scan_gaps(gaps: &ScanGaps, progress: &SyncRunProgress) {
    let count = gaps.unlistable.len() + gaps.skipped_links.len();
    if count == 0 {
        return;
    }
    progress
        .add_stage_total(SyncStageKey::Discover, count as u64, SyncTotalState::Final)
        .await;
    for (path, error) in &gaps.unlistable {
        let details = SyncWorkDetails::new("scan_folder")
            .source_dir(Some(path.to_string_lossy().into_owned()));
        let work_id = progress
            .start_work(SyncStageKey::Discover, details.clone())
            .await;
        progress
            .fail_work(work_id, SyncStageKey::Discover, details, error.clone())
            .await;
    }
    for (path, reason) in &gaps.skipped_links {
        let path = path.to_string_lossy().into_owned();
        let work_id = progress
            .start_work(
                SyncStageKey::Discover,
                SyncWorkDetails::new("scan_link").source_dir(Some(path.clone())),
            )
            .await;
        progress
            .skip_work(work_id, SyncStageKey::Discover, format!("{path}: {reason}"))
            .await;
    }
}

pub(super) async fn mark_missing_entries_for_scan(
    db: &DbAsync,
    library: &Library,
    observation: &ScanObservation,
    progress: SyncRunProgress,
) -> anyhow::Result<()> {
    let run_id = progress.run_id().to_string();
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;
    let started = Instant::now();
    let details = SyncWorkDetails::new("mark_missing_entries");
    let work_id = progress
        .start_work(SyncStageKey::Cleanup, details.clone())
        .await;
    let result = {
        let mut db_write = db.write().await;
        mark_missing(
            &mut db_write,
            library,
            &observation.observed_paths,
            &observation.protected_paths,
            |missing, present| albums_left_by(&library.path, missing, present),
        )
    };
    let result = match result {
        Ok(result) => result,
        Err(err) => {
            progress
                .fail_work(work_id, SyncStageKey::Cleanup, details, format!("{err:#}"))
                .await;
            return Err(err);
        }
    };
    progress.complete_work(work_id, SyncStageKey::Cleanup).await;
    tracing::info!(
        library_db_id = library_db_id.0,
        run_id = %run_id,
        marked_missing = result.marked,
        revived = result.revived,
        removed_dirs = result.removed_dirs,
        elapsed_ms = started.elapsed().as_millis() as u64,
        stage = "cleanup",
        "scan missing entries marked"
    );
    Ok(())
}

/// Collects the artists the run's writes, and any since the last collection, left unreferenced.
/// While a provider's refresh elsewhere holds the collection back, the step is skipped: the
/// collection runs once that refresh has ended.
async fn collect_unreferenced_artists(
    db: &DbAsync,
    library_db_id: DbId,
    progress: SyncRunProgress,
) -> anyhow::Result<()> {
    let run_id = progress.run_id().to_string();
    let started = Instant::now();
    let details = SyncWorkDetails::new("collect_unreferenced_artists");
    let work_id = progress
        .start_work(SyncStageKey::Cleanup, details.clone())
        .await;
    let collected = match cleanup::collect_unreferenced_artists(db).await {
        Ok(collected) => collected,
        Err(err) => {
            progress
                .fail_work(work_id, SyncStageKey::Cleanup, details, format!("{err:#}"))
                .await;
            return Err(err);
        }
    };
    if collected.held_back {
        progress
            .skip_work(work_id, SyncStageKey::Cleanup, "refresh_running")
            .await;
        tracing::info!(
            library_db_id = library_db_id.0,
            run_id = %run_id,
            removed_artists = collected.removed,
            elapsed_ms = started.elapsed().as_millis() as u64,
            stage = "cleanup",
            "scan unreferenced artists left for the end of a running refresh"
        );
        return Ok(());
    }
    progress.complete_work(work_id, SyncStageKey::Cleanup).await;
    #[cfg(test)]
    lyra_db::orphans::testing::assert_drained(&*db.read().await)?;
    tracing::info!(
        library_db_id = library_db_id.0,
        run_id = %run_id,
        removed_artists = collected.removed,
        elapsed_ms = started.elapsed().as_millis() as u64,
        stage = "cleanup",
        "scan unreferenced artists collected"
    );
    Ok(())
}

/// Scans the library's files, applies what changed, then asks the providers about the releases
/// `options.releases` picks. With every release, they refresh as a library refresh does before
/// the changed ones run their other stages, so none is refreshed twice.
pub(crate) async fn sync_library_pipeline(
    db: &DbAsync,
    library: &Library,
    options: LibrarySyncOptions,
    progress: SyncRunProgress,
) -> anyhow::Result<()> {
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;
    progress.set_status(SyncRunStatus::Running).await;
    progress.check_cancelled()?;
    let observation = sync_entries_incremental(db, library, progress.clone()).await?;
    mark_missing_entries_for_scan(db, library, &observation, progress.clone()).await?;
    progress.check_cancelled()?;
    let mut plans = process_pending_metadata(db, library, progress.clone()).await?;
    // Every group is applied by now, so a release can't look alone in a folder because another
    // album's files in it are still to come; those a failed group left stay unread.
    let audio_folders = if plans.is_empty() {
        AudioFolders::default()
    } else {
        AudioFolders::of_libraries(&*db.read().await, std::slice::from_ref(library))?
    };
    let audio_folders = Arc::new(audio_folders);
    let artifact_scope = ArtifactScope {
        db,
        library_db_id,
        library_path: &library.path,
        audio_folders: &audio_folders,
    };
    let stages = ReleaseArtifactStages::for_sync(options);
    let refresh_plan = if stages.provider_refresh {
        Some(ScanRefreshPlan::current().await?)
    } else {
        None
    };
    if let Some(refresh_plan) = &refresh_plan {
        progress
            .add_stage_total(
                SyncStageKey::ProviderRefresh,
                plans.len() as u64 * refresh_plan.provider_count(),
                SyncTotalState::Final,
            )
            .await;
    }
    progress
        .add_stage_total(
            SyncStageKey::LocalCoverMetadata,
            plans.len() as u64,
            SyncTotalState::Final,
        )
        .await;
    progress
        .add_stage_total(
            SyncStageKey::Lyrics,
            plans.iter().map(|plan| plan.track_ids.len() as u64).sum(),
            SyncTotalState::Final,
        )
        .await;
    if !stages.covers.skips() {
        progress
            .add_stage_total(
                SyncStageKey::ProviderCover,
                plans.len() as u64,
                SyncTotalState::Final,
            )
            .await;
    }
    progress
        .add_stage_total(SyncStageKey::Cleanup, 2, SyncTotalState::Final)
        .await;
    // A sync of every release finalises the totals once its refresh added its own, and the
    // artist cover pass once it adds its own.
    if stages.provider_refresh && stages.covers.skips() {
        progress.set_determinate().await;
    }
    if options.releases == SyncReleases::All {
        refresh_every_release(library_db_id, options.covers, progress.clone()).await?;
    }
    // Each chunk's refreshes finish before anything merges, so its artifacts run on what the merge
    // left and later work reads fresh contexts. Chunks rather than the whole scan at once, so a
    // cancelled scan keeps the releases it finished.
    for chunk in plans.chunks_mut(MAX_CONCURRENT_ALBUM_PIPELINE) {
        if progress.is_cancelled() {
            break;
        }
        follow_merged_releases(db, chunk).await?;
        if let Some(refresh_plan) = &refresh_plan {
            let refreshed_by = refresh_changed_releases(
                db,
                library_db_id,
                &library.path,
                chunk,
                refresh_plan,
                &progress,
            )
            .await;
            deduplicate_after_providers(&provider_registry(), &refreshed_by, library_db_id).await;
            follow_merged_releases(db, chunk).await?;
        }
        let mut first_on_release = HashSet::new();
        futures::future::join_all(chunk.iter().map(|plan| {
            let covers = first_on_release.insert(plan.release_id);
            let progress = progress.clone();
            let artifact_scope = &artifact_scope;
            async move {
                #[cfg(test)]
                tests::before_release_artifacts(plan).await;
                if process_release_artifacts(artifact_scope, plan, stages, covers, progress).await {
                    mark_entries_synced(db, library_db_id, &plan.entry_ids).await;
                }
            }
        }))
        .await;
    }
    progress.check_cancelled()?;
    let library_covers = async {
        #[cfg(test)]
        tests::before_library_covers(&library.path).await;
        revalidate_local_covers(db, library, library_db_id).await;
        refresh_gone_covers(db, library, library_db_id).await;
    };
    tokio::select! {
        () = library_covers => {}
        err = progress.cancelled() => return Err(err.into()),
    }
    if !stages.covers.skips() {
        sync_changed_artist_covers(db, library, &plans, stages.covers, &progress).await?;
    }
    purge_expired_missing(db, library, &observation, &progress).await?;
    collect_unreferenced_artists(db, library_db_id, progress).await?;
    Ok(())
}

/// Purges the files missing longer than the library keeps them, when it sets a limit, before the
/// collection of the artists that leaves unreferenced.
async fn purge_expired_missing(
    db: &DbAsync,
    library: &Library,
    observation: &ScanObservation,
    progress: &SyncRunProgress,
) -> anyhow::Result<()> {
    let Some(days) = library.purge_missing_after_days else {
        return Ok(());
    };
    let selection = lyra_db::entries::PurgeScope::Expired {
        missing_before_ms: lyra_db::users::now_ms()
            .unsigned_abs()
            .saturating_sub(u64::from(days) * 86_400_000),
        protected: observation.protected_paths.clone(),
    };
    let plan = {
        let db_read = db.read().await;
        lyra_db::entries::missing_purge_plan(&db_read, library, &selection)
            .map_err(anyhow::Error::from)?
    };
    super::purge::apply(db, library, plan, progress).await?;
    Ok(())
}

/// Refreshes every release in the library as a library refresh does, adding its stages' totals,
/// once another refresh of the library, such as a provider sync's, has ended. Fails only when
/// the run is cancelled: the refresh fails per unit, and anything else failing it is a failed
/// unit, so the changed releases' stages still run.
async fn refresh_every_release(
    library_db_id: DbId,
    covers: CoverSyncOptions,
    progress: SyncRunProgress,
) -> anyhow::Result<()> {
    let options = LibraryRefreshOptions {
        covers,
        apply_sync_filters: false,
        provider_id: None,
    };
    let Err(err) =
        refresh_library_metadata_after_others(library_db_id, &options, progress.clone()).await
    else {
        return Ok(());
    };
    if err.is_cancellation() {
        return Err(err.into_anyhow());
    }
    let err = err.into_anyhow();
    progress
        .add_stage_total(SyncStageKey::ProviderRefresh, 1, SyncTotalState::Final)
        .await;
    let details = SyncWorkDetails::new("library_provider_refresh");
    let work_id = progress
        .start_work(SyncStageKey::ProviderRefresh, details.clone())
        .await;
    progress
        .fail_work(
            work_id,
            SyncStageKey::ProviderRefresh,
            details,
            format!("{err:#}"),
        )
        .await;
    progress.set_determinate().await;
    Ok(())
}

/// The stages a changed release runs after the scan applied it.
#[derive(Clone, Copy)]
struct ReleaseArtifactStages {
    provider_refresh: bool,
    covers: CoverSyncOptions,
}

impl ReleaseArtifactStages {
    /// The provider stages run per release only in a sync of the changed releases; one of every
    /// release leaves them to the library-wide refresh.
    fn for_sync(options: LibrarySyncOptions) -> Self {
        match options.releases {
            SyncReleases::Changed => Self {
                provider_refresh: true,
                covers: options.covers,
            },
            SyncReleases::All => Self {
                provider_refresh: false,
                covers: CoverSyncOptions::SKIP,
            },
        }
    }
}

/// Fetches the covers of the artists credited on the changed releases, or the releases they were
/// merged into, or on their tracks, after the releases' provider refreshes stored what they found.
/// A release gone meanwhile has no artists to pass; artists that can't be listed are none to
/// pass, so the totals still settle.
async fn sync_changed_artist_covers(
    db: &DbAsync,
    library: &Library,
    plans: &[ReleaseArtifactPlan],
    options: CoverSyncOptions,
    progress: &SyncRunProgress,
) -> Result<(), SyncRunControlError> {
    let artists = {
        let db_read = db.read().await;
        current_release_ids(&*db_read, plans)
            .and_then(|current| lyra_db::artists::get_by_releases(&db_read, &current))
    };
    let artists = match artists {
        Ok(artists) => ListedArtist::list(artists),
        Err(err) => {
            tracing::warn!(
                library_db_id = ?library.db_id,
                error = %format_args!("{err:#}"),
                "scan could not list the changed releases' artists"
            );
            Vec::new()
        }
    };
    let covers_root = configured_covers_root();
    let paths = CoverPaths {
        library_root: Some(&library.path),
        covers_root: &covers_root,
    };
    sync_artist_covers(&artists, paths, options, None, progress).await?;
    Ok(())
}

/// Checks every stored local cover against the folders as the scan left them, which the release
/// passes above skip for releases whose audio is unchanged, though another album arriving beside
/// them can take a folder away from them.
async fn revalidate_local_covers(db: &DbAsync, library: &Library, library_db_id: DbId) {
    match revalidate_local_covers_for_library(db, library_db_id, Some(&library.path)).await {
        Ok(changed) => tracing::debug!(
            library_db_id = library_db_id.0,
            changed,
            "scan revalidated stored local covers"
        ),
        Err(err) => tracing::warn!(
            library_db_id = library_db_id.0,
            error = %format_args!("{err:#}"),
            "scan could not revalidate stored local covers"
        ),
    }
}

/// Searches again for the covers whose file is gone, which the release passes above skip for
/// releases whose audio is unchanged.
async fn refresh_gone_covers(db: &DbAsync, library: &Library, library_db_id: DbId) {
    match refresh_gone_covers_for_library(db, library_db_id, Some(&library.path)).await {
        Ok(removed) => tracing::debug!(
            library_db_id = library_db_id.0,
            removed,
            "scan removed covers whose file is gone"
        ),
        Err(err) => tracing::warn!(
            library_db_id = library_db_id.0,
            error = %format_args!("{err:#}"),
            "scan could not refresh covers whose file is gone"
        ),
    }
}

async fn mark_entries_synced(db: &DbAsync, library_db_id: DbId, entry_ids: &[DbId]) {
    let result = {
        let mut db_write = db.write().await;
        lyra_db::entries::mark_synced(&mut db_write, entry_ids)
    };
    if let Err(err) = result {
        tracing::error!(
            library_db_id = library_db_id.0,
            entry_count = entry_ids.len(),
            error = %format_args!("{err:#}"),
            "scan entries could not be marked synced"
        );
    }
}

fn is_unsynced_file(entry: &Entry) -> bool {
    entry.kind == EntryKind::File && entry.synced_at_ms.is_none() && !entry.is_missing()
}

/// Reads the metadata groups holding unsynced files and applies the batches those files are in,
/// a few groups at a time, each visible in the catalog before the scan moves past it. Files with
/// nothing left to do are marked synced as their group is read. A failed group, like a
/// transiently unreadable file, is a failed unit, and its files stay unsynced for the next sync.
async fn process_pending_metadata(
    db: &DbAsync,
    library: &Library,
    progress: SyncRunProgress,
) -> anyhow::Result<Vec<ReleaseArtifactPlan>> {
    let run_id = progress.run_id().to_string();
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;

    let entries = {
        let db_read = db.read().await;
        lyra_db::entries::load_existing(&*db_read, library_db_id)?
    }
    .into_iter()
    .filter(|entry| !entry.is_missing())
    .collect::<Vec<_>>();
    if !entries.iter().any(is_unsynced_file) {
        return Ok(Vec::new());
    }
    let (cue_links, roots) = {
        let cue_entry_ids = entries
            .iter()
            .filter(|entry| entry.file_kind.as_deref() == Some("cue"))
            .filter_map(|entry| entry.db_id)
            .collect::<Vec<_>>();
        let db_read = db.read().await;
        (
            lyra_db::cue::audio_links(&*db_read, &cue_entry_ids)?,
            Arc::new(MediaRoots::for_principal(&*db_read, None)?),
        )
    };
    let groups = tokio::select! {
        groups = metadata_groups(&library.path, entries, &cue_links, &roots) => groups,
        err = progress.cancelled() => return Err(err.into()),
    };
    let groups = groups
        .into_iter()
        .filter(|group| group.entries.iter().any(is_unsynced_file))
        .collect::<Vec<_>>();
    progress
        .add_stage_total(
            SyncStageKey::MetadataParse,
            groups.len() as u64,
            SyncTotalState::Estimated,
        )
        .await;

    let mapping_config = load_mapping_config(db).await?;
    let group_count = groups.len();
    let mut applied_groups = futures::stream::iter(groups)
        .map(|group| {
            process_album_group(
                db,
                library_db_id,
                group,
                &mapping_config,
                &cue_links,
                &roots,
                progress.clone(),
            )
        })
        .buffer_unordered(MAX_CONCURRENT_ALBUM_PIPELINE);

    // Groups whose tracks were one release keep it, so its artifacts run once for all of them.
    let mut entries_by_release: BTreeMap<DbId, Vec<DbId>> = BTreeMap::new();
    while let Some(applied) = applied_groups.next().await {
        for (release_id, entry_ids) in applied? {
            entries_by_release
                .entry(release_id)
                .or_default()
                .extend(entry_ids);
        }
    }

    // Built only now: a plan's tracks are read back, and a later group can add to its release.
    let mut plans = Vec::with_capacity(entries_by_release.len());
    for (release_id, entry_ids) in entries_by_release {
        match release_artifact_plan(db, release_id, entry_ids.clone()).await? {
            Some(plan) => plans.push(plan),
            None => mark_entries_synced(db, library_db_id, &entry_ids).await,
        }
    }
    tracing::info!(
        library_db_id = library_db_id.0,
        run_id = %run_id,
        group_count,
        "scan metadata groups completed"
    );
    Ok(plans)
}

/// Parses an album group, coalesces its discs and applies each batch in one write. Returns the
/// release each batch applied to, with the entries marked synced once its artifacts finish. A
/// failed parse or apply is a failed unit, so only a cancelled run fails this.
async fn process_album_group(
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
        tests::before_parse(&source_dir).await;
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
        tests::before_apply(&metadata)?;
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

struct ReleaseArtifactPlan {
    release_id: DbId,
    release_public_id: Option<String>,
    release_title: Option<String>,
    track_ids: Vec<DbId>,
    /// The entries marked synced once the release's artifacts finish.
    entry_ids: Vec<DbId>,
    /// Neither the release nor any of its tracks' releases remain.
    gone: bool,
}

/// What the artifact stages of a scan's releases share.
struct ArtifactScope<'a> {
    db: &'a DbAsync,
    library_db_id: DbId,
    library_path: &'a Path,
    audio_folders: &'a Arc<AudioFolders>,
}

struct ReleaseArtifactContext<'a> {
    db: &'a DbAsync,
    library_db_id: DbId,
    library_path: &'a Path,
    release_id: DbId,
    release_public_id: Option<String>,
    release_title: Option<String>,
    run_id: String,
    progress: SyncRunProgress,
}

impl<'a> ReleaseArtifactContext<'a> {
    fn new(
        db: &'a DbAsync,
        library_db_id: DbId,
        library_path: &'a Path,
        plan: &ReleaseArtifactPlan,
        progress: SyncRunProgress,
    ) -> Self {
        Self {
            db,
            library_db_id,
            library_path,
            release_id: plan.release_id,
            release_public_id: plan.release_public_id.clone(),
            release_title: plan.release_title.clone(),
            run_id: progress.run_id().to_string(),
            progress,
        }
    }

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
    entry_ids: Vec<DbId>,
) -> anyhow::Result<Option<ReleaseArtifactPlan>> {
    let db_read = db.read().await;
    let Some(release) = lyra_db::releases::get_by_id(&db_read, release_id)? else {
        return Ok(None);
    };
    let release_public_id = lyra_db::lookup::find_id_by_db_id(&db_read, release_id)?;
    let track_ids = lyra_db::tracks::get_direct(&db_read, release_id)?
        .into_iter()
        .filter_map(|track| track.db_id.map(DbId::from))
        .collect();
    Ok(Some(ReleaseArtifactPlan {
        release_id,
        release_public_id,
        release_title: Some(release.release_title),
        track_ids,
        entry_ids,
        gone: false,
    }))
}

/// The plan's release as it is now: its own, or the one merges left it in; `None` once it is gone.
fn current_release(
    db: &impl lyra_db::DbAccess,
    plan: &ReleaseArtifactPlan,
) -> anyhow::Result<Option<lyra_db::Release>> {
    let Some(public_id) = plan.release_public_id.as_deref() else {
        return Ok(None);
    };
    if node_is_entity(db, plan.release_id, public_id)? {
        return lyra_db::releases::get_by_id(db, plan.release_id);
    }
    let Some(winner) = STATE.generation().merge_journal.resolve(public_id) else {
        return Ok(None);
    };
    let Some(release_id) = lyra_db::lookup::find_node_id_by_id(db, &winner)? else {
        return Ok(None);
    };
    lyra_db::releases::get_by_id(db, release_id)
}

fn current_release_ids(
    db: &impl lyra_db::DbAccess,
    plans: &[ReleaseArtifactPlan],
) -> anyhow::Result<Vec<DbId>> {
    let mut release_ids = HashSet::new();
    for plan in plans {
        if let Some(release) = current_release(db, plan)? {
            release_ids.extend(release.db_id.map(DbId::from));
        }
    }
    Ok(release_ids.into_iter().collect())
}

/// Points the plans at the releases merges left them in, marking those with none as gone.
async fn follow_merged_releases(
    db: &DbAsync,
    plans: &mut [ReleaseArtifactPlan],
) -> anyhow::Result<()> {
    let db_read = db.read().await;
    for plan in plans.iter_mut().filter(|plan| !plan.gone) {
        match current_release(&*db_read, plan)? {
            Some(release) => {
                plan.release_id = release.db_id.map_or(plan.release_id, Into::into);
                plan.release_public_id = Some(release.id);
                plan.release_title = Some(release.release_title);
            }
            None => plan.gone = true,
        }
    }
    Ok(())
}

/// Settles `units` of `stage` that won't run.
async fn skip_units(
    progress: &SyncRunProgress,
    stage: SyncStageKey,
    units: u64,
    reason: &'static str,
) {
    for _ in 0..units {
        let work_id = progress
            .start_work(stage, SyncWorkDetails::new(reason))
            .await;
        progress.skip_work(work_id, stage, reason).await;
    }
}

/// Refreshes the releases from every enabled provider, returning the providers that refreshed any
/// of them.
async fn refresh_changed_releases(
    db: &DbAsync,
    library_db_id: DbId,
    library_path: &Path,
    plans: &[ReleaseArtifactPlan],
    refresh_plan: &ScanRefreshPlan,
    progress: &SyncRunProgress,
) -> HashSet<String> {
    futures::future::join_all(plans.iter().map(|plan| async move {
        #[cfg(test)]
        tests::before_release_refresh(plan).await;
        if plan.gone {
            skip_units(
                progress,
                SyncStageKey::ProviderRefresh,
                refresh_plan.provider_count(),
                "release_gone",
            )
            .await;
            return Vec::new();
        }
        if progress.is_cancelled() {
            return Vec::new();
        }
        refresh_changed_release(
            &ReleaseArtifactContext::new(db, library_db_id, library_path, plan, progress.clone()),
            refresh_plan,
        )
        .await
    }))
    .await
    .into_iter()
    .flatten()
    .collect()
}

/// Runs a release's artifact stages, its cover ones only when `covers` says no other plan of the
/// same release does; false when its entries need another sync: its local cover failed to persist
/// or the run was cancelled. Lyrics and provider cover failures are failed units that `/refresh`
/// retries, as provider failures are.
async fn process_release_artifacts(
    scope: &ArtifactScope<'_>,
    plan: &ReleaseArtifactPlan,
    stages: ReleaseArtifactStages,
    covers: bool,
    progress: SyncRunProgress,
) -> bool {
    if progress.is_cancelled() {
        return false;
    }
    if plan.gone {
        let lyrics_units = plan.track_ids.len() as u64;
        skip_units(
            &progress,
            SyncStageKey::Lyrics,
            lyrics_units,
            "release_gone",
        )
        .await;
        skip_cover_units(&progress, stages, "release_gone").await;
        return true;
    }
    let run_id = progress.run_id().to_string();
    let context = ReleaseArtifactContext::new(
        scope.db,
        scope.library_db_id,
        scope.library_path,
        plan,
        progress.clone(),
    );

    let local_cover = if covers {
        sync_local_release_cover_metadata(&context, scope.audio_folders).await
    } else {
        Ok(())
    };
    dispatch_release_lyrics(
        scope.library_db_id,
        plan.release_id,
        plan,
        run_id,
        progress.clone(),
    )
    .await;
    if !covers {
        skip_cover_units(&progress, stages, "release_merged").await;
    } else if !stages.covers.skips() {
        sync_provider_release_cover(&context, scope.audio_folders, stages.covers).await;
    }
    local_cover.is_ok() && !progress.is_cancelled()
}

/// Settles the local cover unit of a release and its provider cover unit, when the run has them.
async fn skip_cover_units(
    progress: &SyncRunProgress,
    stages: ReleaseArtifactStages,
    reason: &'static str,
) {
    skip_units(progress, SyncStageKey::LocalCoverMetadata, 1, reason).await;
    if !stages.covers.skips() {
        skip_units(progress, SyncStageKey::ProviderCover, 1, reason).await;
    }
}

async fn refresh_changed_release(
    context: &ReleaseArtifactContext<'_>,
    refresh_plan: &ScanRefreshPlan,
) -> Vec<String> {
    let library_db_id = context.library_db_id;
    let release_id = context.release_id;
    let run_id = &context.run_id;
    tracing::debug!(
        library_db_id = library_db_id.0,
        run_id = %run_id,
        release_db_id = release_id.0,
        stage = "provider_refresh",
        "scan release stage started"
    );
    match refresh_release_metadata_for_scan(
        library_db_id,
        release_id,
        refresh_plan,
        context.progress.clone(),
        context.release_public_id.clone(),
        context.release_title.clone(),
    )
    .await
    {
        Ok(refreshed_by) => refreshed_by,
        Err(err) => {
            tracing::warn!(
                library_db_id = library_db_id.0,
                run_id = %run_id,
                release_db_id = release_id.0,
                stage = "provider_refresh",
                error = %format_args!("{:#}", anyhow::Error::from(err)),
                "scan release provider refresh failed"
            );
            Vec::new()
        }
    }
}

/// Records the release's local cover, failing when it can't be resolved or persisted.
async fn sync_local_release_cover_metadata(
    context: &ReleaseArtifactContext<'_>,
    audio_folders: &AudioFolders,
) -> anyhow::Result<()> {
    let started = Instant::now();
    let details = context.details("local_cover_metadata");
    let work_id = context
        .progress
        .start_work(SyncStageKey::LocalCoverMetadata, details.clone())
        .await;
    let result = record_local_release_cover(
        context.db,
        audio_folders,
        context.release_id,
        Some(context.library_path),
    )
    .await;

    match result {
        Ok(synced) => {
            context
                .progress
                .complete_work(work_id, SyncStageKey::LocalCoverMetadata)
                .await;
            match synced {
                Some((cover_path, changed)) => tracing::info!(
                    library_db_id = context.library_db_id.0,
                    run_id = %context.run_id,
                    release_db_id = context.release_id.0,
                    stage = "local_cover_metadata",
                    changed,
                    cover_path = %cover_path.display(),
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "scan release local cover metadata synced"
                ),
                None => tracing::debug!(
                    library_db_id = context.library_db_id.0,
                    run_id = %context.run_id,
                    release_db_id = context.release_id.0,
                    stage = "local_cover_metadata",
                    "scan release local cover not found"
                ),
            }
            Ok(())
        }
        Err(err) => {
            context
                .progress
                .fail_work(
                    work_id,
                    SyncStageKey::LocalCoverMetadata,
                    details,
                    format!("{err:#}"),
                )
                .await;
            Err(err)
        }
    }
}

async fn dispatch_release_lyrics(
    library_db_id: DbId,
    release_id: DbId,
    plan: &ReleaseArtifactPlan,
    run_id: String,
    progress: SyncRunProgress,
) {
    if plan.track_ids.is_empty() {
        return;
    }

    let started = Instant::now();
    futures::stream::iter(plan.track_ids.clone())
        .for_each_concurrent(MAX_CONCURRENT_DISPATCHES, |track_db_id| {
            let progress = progress.clone();
            let details = SyncWorkDetails::release(
                "lyrics_track",
                plan.release_public_id.clone(),
                plan.release_title.clone(),
            );
            async move {
                let work_id = progress
                    .start_work(SyncStageKey::Lyrics, details.clone())
                    .await;
                match dispatch_lyrics_for_track(
                    track_db_id,
                    false,
                    crate::services::plugin_dispatch::TransitionWait::background(
                        progress.cancellation().clone(),
                    ),
                )
                .await
                {
                    Ok(()) => {
                        progress.complete_work(work_id, SyncStageKey::Lyrics).await;
                    }
                    Err(err) => {
                        progress
                            .fail_work(work_id, SyncStageKey::Lyrics, details, format!("{err:#}"))
                            .await;
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
}

async fn sync_provider_release_cover(
    context: &ReleaseArtifactContext<'_>,
    audio_folders: &Arc<AudioFolders>,
    options: CoverSyncOptions,
) {
    let started = Instant::now();
    let details = context.details("provider_cover");
    let work_id = context
        .progress
        .start_work(SyncStageKey::ProviderCover, details.clone())
        .await;
    let covers_root = configured_covers_root();
    let cover_paths = CoverPaths {
        library_root: Some(context.library_path),
        covers_root: &covers_root,
    };

    let release_bundle = {
        let db_read = context.db.read().await;
        match lyra_db::releases::get_by_id(&db_read, context.release_id) {
            Ok(Some(release)) => {
                let tracks = lyra_db::tracks::get(&db_read, context.release_id).unwrap_or_default();
                let artists =
                    lyra_db::artists::get(&db_read, context.release_id).unwrap_or_default();
                Ok(Some((release, tracks, artists)))
            }
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    };
    let (release, tracks, artists) = match release_bundle {
        Ok(Some(bundle)) => bundle,
        Ok(None) => {
            context
                .progress
                .fail_work(
                    work_id,
                    SyncStageKey::ProviderCover,
                    details,
                    "release missing before provider cover sync",
                )
                .await;
            return;
        }
        Err(err) => {
            context
                .progress
                .fail_work(
                    work_id,
                    SyncStageKey::ProviderCover,
                    details,
                    format!("{err:#}"),
                )
                .await;
            return;
        }
    };

    let synced = match sync_release_cover_for_tracks(
        &crate::services::plugin_dispatch::Published::background(
            context.progress.cancellation().clone(),
        ),
        context.db,
        audio_folders,
        ReleaseCoverSubject {
            release: &release,
            tracks: &tracks,
            artists: &artists,
        },
        cover_paths,
        options,
    )
    .await
    {
        Ok(synced) => {
            context
                .progress
                .complete_work(work_id, SyncStageKey::ProviderCover)
                .await;
            synced
        }
        Err(err) => {
            context
                .progress
                .fail_work(
                    work_id,
                    SyncStageKey::ProviderCover,
                    details.clone(),
                    format!("{err:#}"),
                )
                .await;
            false
        }
    };

    tracing::info!(
        library_db_id = context.library_db_id.0,
        run_id = %context.run_id,
        release_db_id = context.release_id.0,
        stage = "provider_cover",
        synced,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "scan release provider cover stage completed"
    );
}

pub(crate) async fn sync_library(db: &DbAsync, library: &Library) -> anyhow::Result<()> {
    let progress = SyncRunProgress::unreported(crate::services::shutdown::token());
    sync_library_pipeline(db, library, LibrarySyncOptions::default(), progress).await
}

#[cfg(test)]
mod tests {
    use lyra_db::roots::{
        self,
        Root,
    };
    use std::{
        collections::BTreeMap,
        path::{
            Path,
            PathBuf,
        },
        time::{
            Duration,
            SystemTime,
            UNIX_EPOCH,
        },
    };

    use super::*;

    const CUE_SHEET: &str = r#"PERFORMER "Cue Artist"
TITLE "Cue Album"
FILE "album.flac" WAVE
  TRACK 01 AUDIO
    TITLE "Cue Track 1"
    INDEX 01 00:00:00
  TRACK 02 AUDIO
    TITLE "Cue Track 2"
    INDEX 01 00:01:00
"#;

    fn fixture_flac() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/assets/metadata/integration_track.flac")
    }

    fn temp_library(name: &str) -> anyhow::Result<(PathBuf, PathBuf)> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root = std::env::temp_dir().join(format!("lyra-{name}-{}-{nanos}", std::process::id()));
        let album = root.join("Album");
        std::fs::create_dir_all(&album)?;
        Ok((root, album))
    }

    /// Moves `path`'s mtime by `offset_secs` so the scan sees a change within one second.
    fn shift_mtime(path: &Path, offset_secs: u64) -> anyhow::Result<()> {
        let modified = std::fs::metadata(path)?.modified()?;
        std::fs::File::options()
            .write(true)
            .open(path)
            .or_else(|_| std::fs::File::open(path))?
            .set_modified(modified + Duration::from_secs(offset_secs))?;
        Ok(())
    }

    /// Adds a file to the album and moves the album directory's mtime, so its group is
    /// re-ingested.
    fn change_album_directory(album: &Path, marker: &str, offset_secs: u64) -> anyhow::Result<()> {
        std::fs::write(album.join(marker), marker)?;
        shift_mtime(album, offset_secs)
    }

    /// Entry public id and track public ids per file path.
    async fn snapshot(
        library: &Library,
    ) -> anyhow::Result<BTreeMap<PathBuf, (String, Vec<String>)>> {
        let db = STATE.db.read().await;
        let mut out = BTreeMap::new();
        for entry in lyra_db::entries::load_existing(&*db, library.db_id.expect("library db_id"))? {
            let Some(entry_db_id) = entry.db_id else {
                continue;
            };
            let mut tracks = lyra_db::tracks::get_by_entry(&db, entry_db_id)?
                .into_iter()
                .map(|track| track.id)
                .collect::<Vec<_>>();
            tracks.sort();
            out.insert(entry.full_path, (entry.id, tracks));
        }
        Ok(out)
    }

    async fn cue_track_ids() -> anyhow::Result<Vec<DbId>> {
        let db = STATE.db.read().await;
        let mut ids = roots::member_ids(&*db, Root::CueTracks)?;
        ids.sort();
        Ok(ids)
    }

    async fn sync(library: &Library) -> anyhow::Result<()> {
        sync_library(&STATE.db.get(), library).await
    }

    async fn assert_track_survives_file_change(
        name: &str,
        change: impl FnOnce(&Path) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, album) = temp_library(name)?;
        let audio = album.join("track.flac");
        std::fs::copy(fixture_flac(), &audio)?;
        let library = super::super::prepare_capture_library(root.clone()).await?;

        let before = snapshot(&library).await?;
        assert_eq!(before[&audio].1.len(), 1, "fixture must ingest one track");

        change(&audio)?;
        sync(&library).await?;
        change_album_directory(&album, "later.txt", 10)?;
        sync(&library).await?;

        let after = snapshot(&library).await?;
        assert_eq!(after[&audio], before[&audio], "entry id or track changed");
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn entry_id_and_track_survive_an_mtime_touch() -> anyhow::Result<()> {
        assert_track_survives_file_change("entry-touch", |audio| shift_mtime(audio, 5)).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn entry_id_and_track_survive_an_in_place_content_change() -> anyhow::Result<()> {
        assert_track_survives_file_change("entry-content", |audio| {
            let mut bytes = std::fs::read(audio)?;
            bytes.extend_from_slice(&[0; 64]);
            std::fs::write(audio, bytes)?;
            shift_mtime(audio, 5)
        })
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn entry_id_and_track_survive_a_retag_in_place() -> anyhow::Result<()> {
        use lofty::{
            config::WriteOptions,
            prelude::*,
        };

        assert_track_survives_file_change("entry-retag", |audio| {
            let mut tagged = lofty::read_from_path(audio)?;
            let tag = tagged
                .primary_tag_mut()
                .ok_or_else(|| anyhow::anyhow!("fixture has no primary tag"))?;
            tag.set_title("Retagged Title".to_string());
            tag.save_to_path(audio, WriteOptions::default())?;
            shift_mtime(audio, 5)
        })
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cue_sheet_edit_keeps_its_cue_tracks() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, album) = temp_library("cue-edit")?;
        let audio = album.join("album.flac");
        let cue = album.join("album.cue");
        std::fs::copy(fixture_flac(), &audio)?;
        std::fs::write(&cue, CUE_SHEET)?;
        let library = super::super::prepare_capture_library(root.clone()).await?;

        let before = snapshot(&library).await?;
        let cue_tracks_before = cue_track_ids().await?;
        assert_eq!(before[&audio].1.len(), 2, "cue must ingest two tracks");
        assert_eq!(cue_tracks_before.len(), 2);

        std::fs::write(&cue, CUE_SHEET.replace("Cue Track 2", "Cue Track Two"))?;
        shift_mtime(&cue, 5)?;
        shift_mtime(&album, 5)?;
        sync(&library).await?;

        let after = snapshot(&library).await?;
        assert_eq!(
            cue_track_ids().await?,
            cue_tracks_before,
            "cue tracks were recreated"
        );
        assert_eq!(after[&audio].1, before[&audio].1, "tracks were recreated");
        assert_eq!(after[&cue].0, before[&cue].0, "cue entry id changed");
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    const PNG_1X1: &[u8] = &[
        0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44,
        0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x04, 0x00, 0x00, 0x00, 0xB5,
        0x1C, 0x0C, 0x02, 0x00, 0x00, 0x00, 0x0B, 0x49, 0x44, 0x41, 0x54, 0x78, 0xDA, 0x63, 0xFC,
        0xFF, 0x1F, 0x00, 0x03, 0x03, 0x02, 0x00, 0xEF, 0xBF, 0x55, 0x3A, 0x00, 0x00, 0x00, 0x00,
        0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
    ];

    /// Copies the fixture into `dir` as the only track of an album titled `album`.
    fn album_track(dir: &Path, album: &str) -> anyhow::Result<PathBuf> {
        use lofty::{
            config::WriteOptions,
            prelude::*,
        };

        std::fs::create_dir_all(dir)?;
        let audio = dir.join("track.flac");
        std::fs::copy(fixture_flac(), &audio)?;
        let mut tagged = lofty::read_from_path(&audio)?;
        let tag = tagged
            .primary_tag_mut()
            .ok_or_else(|| anyhow::anyhow!("fixture has no primary tag"))?;
        tag.set_album(album.to_string());
        tag.save_to_path(&audio, WriteOptions::default())?;
        Ok(audio)
    }

    async fn entry_for(library: &Library, path: &Path) -> anyhow::Result<lyra_db::Entry> {
        let db = STATE.db.read().await;
        lyra_db::entries::load_existing(&*db, library.db_id.expect("library db_id"))?
            .into_iter()
            .find(|entry| entry.full_path == path)
            .ok_or_else(|| anyhow::anyhow!("no entry for {}", path.display()))
    }

    async fn release_of(audio_entry: DbId) -> anyhow::Result<DbId> {
        let db = STATE.db.read().await;
        let track = lyra_db::tracks::get_by_entry(&db, audio_entry)?
            .into_iter()
            .next()
            .and_then(|track| track.db_id.map(DbId::from))
            .ok_or_else(|| anyhow::anyhow!("entry has no track"))?;
        lyra_db::releases::get_by_track(&db, track)?
            .into_iter()
            .next()
            .and_then(|release| release.db_id.map(DbId::from))
            .ok_or_else(|| anyhow::anyhow!("track has no release"))
    }

    async fn release_cover(audio_entry: DbId) -> anyhow::Result<Option<lyra_db::Cover>> {
        let release = release_of(audio_entry).await?;
        lyra_db::covers::get(&*STATE.db.read().await, release)
    }

    #[derive(Clone, Default)]
    struct CapturedLogs(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl CapturedLogs {
        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().expect("logs")).into_owned()
        }
    }

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("logs").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    async fn empty_library(name: &str) -> anyhow::Result<(PathBuf, Library)> {
        let (root, _) = temp_library(name)?;
        let library = super::super::prepare_capture_library(root.clone()).await?;
        Ok((root, library))
    }

    async fn synced(library: &Library, path: &Path) -> anyhow::Result<bool> {
        Ok(entry_for(library, path).await?.synced_at_ms.is_some())
    }

    /// Makes `path` unreadable, or returns false when this user reads it regardless.
    fn deny_reads(path: &Path) -> anyhow::Result<bool> {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o000))?;
        Ok(std::fs::read(path).is_err())
    }

    fn allow_reads(path: &Path) -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o644))?;
        Ok(())
    }

    type Gate<T> =
        std::sync::Arc<dyn Fn(T) -> futures::future::BoxFuture<'static, ()> + Send + Sync>;
    type ArtifactGate = Gate<Option<String>>;
    /// Runs on a group's hashed files before it applies them, failing the group with its error.
    type EntryGate = std::sync::Arc<
        dyn Fn(Vec<PathBuf>) -> futures::future::BoxFuture<'static, anyhow::Result<()>>
            + Send
            + Sync,
    >;

    #[derive(Default)]
    struct Hooks {
        reject_album: Option<&'static str>,
        applied: std::sync::Mutex<Vec<String>>,
        before_refresh: Option<ArtifactGate>,
        before_artifacts: Option<ArtifactGate>,
        before_parse: Option<Gate<PathBuf>>,
        before_entry_hash: Option<Gate<PathBuf>>,
        after_entry_hash: Option<EntryGate>,
        entry_applied: Option<std::sync::Arc<dyn Fn(PathBuf) + Send + Sync>>,
        before_library_covers: Option<Gate<PathBuf>>,
        /// Fails the write unsyncing what the scan's moves leave behind.
        fail_unsync_left_by_moves: bool,
        planned: std::sync::Mutex<Vec<(Vec<DbId>, Vec<DbId>)>>,
    }

    tokio::task_local! {
        static HOOKS: std::sync::Arc<Hooks>;
    }

    pub(super) fn before_apply(metadata: &[TrackMetadata]) -> anyhow::Result<()> {
        let album = metadata[0].album.clone().unwrap_or_default();
        HOOKS
            .try_with(|hooks| {
                hooks.applied.lock().expect("applied").push(album.clone());
                if hooks.reject_album == Some(album.as_str()) {
                    return Err(anyhow::anyhow!("'release_title' cannot be empty")
                        .context("invalid 'local' metadata layer"));
                }
                Ok(())
            })
            .unwrap_or(Ok(()))
    }

    pub(super) async fn before_parse(source_dir: &Path) {
        let gate = HOOKS
            .try_with(|hooks| hooks.before_parse.clone())
            .ok()
            .flatten();
        if let Some(gate) = gate {
            gate(source_dir.to_path_buf()).await;
        }
    }

    pub(super) async fn before_library_covers(library_path: &Path) {
        let gate = HOOKS
            .try_with(|hooks| hooks.before_library_covers.clone())
            .ok()
            .flatten();
        if let Some(gate) = gate {
            gate(library_path.to_path_buf()).await;
        }
    }

    pub(super) fn before_unsync_left_by_moves() -> anyhow::Result<()> {
        if HOOKS
            .try_with(|hooks| hooks.fail_unsync_left_by_moves)
            .unwrap_or(false)
        {
            anyhow::bail!("the unsync write fails");
        }
        Ok(())
    }

    pub(super) async fn before_entry_hash(source_dir: &Path) {
        let gate = HOOKS
            .try_with(|hooks| hooks.before_entry_hash.clone())
            .ok()
            .flatten();
        if let Some(gate) = gate {
            gate(source_dir.to_path_buf()).await;
        }
    }

    pub(super) async fn after_entry_hash(entries: &[Entry]) -> anyhow::Result<()> {
        let gate = HOOKS
            .try_with(|hooks| hooks.after_entry_hash.clone())
            .ok()
            .flatten();
        match gate {
            Some(gate) => {
                gate(
                    entries
                        .iter()
                        .map(|entry| entry.full_path.clone())
                        .collect(),
                )
                .await
            }
            None => Ok(()),
        }
    }

    pub(super) fn entry_group_applied(source_dir: &Path) {
        let applied = HOOKS
            .try_with(|hooks| hooks.entry_applied.clone())
            .ok()
            .flatten();
        if let Some(applied) = applied {
            applied(source_dir.to_path_buf());
        }
    }

    pub(super) async fn before_release_artifacts(plan: &ReleaseArtifactPlan) {
        let gate = HOOKS
            .try_with(|hooks| {
                hooks
                    .planned
                    .lock()
                    .expect("planned")
                    .push((plan.track_ids.clone(), plan.entry_ids.clone()));
                hooks.before_artifacts.clone()
            })
            .ok()
            .flatten();
        if let Some(gate) = gate {
            gate(plan.release_title.clone()).await;
        }
    }

    pub(super) async fn before_release_refresh(plan: &ReleaseArtifactPlan) {
        let gate = HOOKS
            .try_with(|hooks| hooks.before_refresh.clone())
            .ok()
            .flatten();
        if let Some(gate) = gate {
            gate(plan.release_title.clone()).await;
        }
    }

    async fn sync_with(
        hooks: std::sync::Arc<Hooks>,
        library: &Library,
        progress: SyncRunProgress,
    ) -> anyhow::Result<()> {
        HOOKS
            .scope(
                hooks,
                sync_library_pipeline(
                    &STATE.db.get(),
                    library,
                    LibrarySyncOptions::default(),
                    progress,
                ),
            )
            .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failed_album_is_retried_alone_without_starving_its_siblings() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("failed-album").await?;
        let good = album_track(&root.join("Artist/Good"), "Good Album")?;
        let bad = album_file(&root.join("Artist/Bad/02.flac"), "Bad Album", 2)
            .map(|()| root.join("Artist/Bad/02.flac"))?;
        album_file(&root.join("Artist/Other/03.flac"), "Other Album", 3)?;
        std::fs::write(root.join("Artist/Good/cover.png"), PNG_1X1)?;

        let logs = CapturedLogs::default();
        let progress = super::super::orchestrator::begin_test_run(&library).await;
        {
            let _logs = tracing::subscriber::set_default(
                tracing_subscriber::fmt()
                    .with_writer(logs.clone())
                    .with_ansi(false)
                    .finish(),
            );
            let hooks = Hooks {
                reject_album: Some("Bad Album"),
                ..Hooks::default()
            };
            sync_with(hooks.into(), &library, progress.clone()).await?;
        }
        let run = super::super::orchestrator::get_sync_run(progress.run_id())
            .await?
            .expect("run");
        super::super::orchestrator::end_test_run(&progress).await;

        assert_eq!(run.progress.failed_units, 1);
        assert!(
            logs.text()
                .contains("invalid 'local' metadata layer: 'release_title' cannot be empty"),
            "the failure lost its cause: {}",
            logs.text()
        );
        assert!(snapshot(&library).await?[&bad].1.is_empty());
        let good_entry = entry_for(&library, &good).await?;
        assert!(good_entry.synced_at_ms.is_some());
        assert!(
            release_cover(good_entry.db_id.expect("entry db_id"))
                .await?
                .is_some(),
            "the good album missed its cover"
        );
        assert!(!synced(&library, &bad).await?);

        let hooks = std::sync::Arc::new(Hooks::default());
        sync_with(
            hooks.clone(),
            &library,
            SyncRunProgress::unreported_for_test(),
        )
        .await?;

        assert_eq!(*hooks.applied.lock().expect("applied"), ["Bad Album"]);
        assert_eq!(snapshot(&library).await?[&bad].1.len(), 1);
        assert!(synced(&library, &bad).await?);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn changed_album_in_a_shared_folder_is_applied_alone() -> anyhow::Result<()> {
        use lofty::{
            config::WriteOptions,
            prelude::*,
        };

        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("shared-folder").await?;
        let folder = root.join("Singles");
        let one = album_track(&folder, "One")?;
        let two = folder.join("two.flac");
        std::fs::copy(&one, &two)?;
        let mut tagged = lofty::read_from_path(&two)?;
        let tag = tagged
            .primary_tag_mut()
            .ok_or_else(|| anyhow::anyhow!("fixture has no primary tag"))?;
        tag.set_album("Two".to_string());
        tag.save_to_path(&two, WriteOptions::default())?;
        sync(&library).await?;

        shift_mtime(&two, 5)?;
        let hooks = std::sync::Arc::new(Hooks::default());
        sync_with(
            hooks.clone(),
            &library,
            SyncRunProgress::unreported_for_test(),
        )
        .await?;

        assert_eq!(*hooks.applied.lock().expect("applied"), ["Two"]);
        assert!(synced(&library, &one).await? && synced(&library, &two).await?);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    /// The release ids a provider's refresh handler was called for, sorted.
    fn refreshed_ids(
        dispatch: &crate::services::testing::FakePluginDispatch,
        handler: crate::services::plugin_dispatch::PluginCallbackHandle,
    ) -> Vec<String> {
        let mut ids: Vec<String> = dispatch
            .calls(handler)
            .iter()
            .map(|context| context["id"].as_str().expect("release id").to_string())
            .collect();
        ids.sort();
        ids
    }

    fn artist_file(path: &Path, album: &str, artist: &str, track: u32) -> anyhow::Result<()> {
        use lofty::{
            config::WriteOptions,
            prelude::*,
            tag::ItemKey,
        };
        album_file(path, album, track)?;
        let mut tagged = lofty::read_from_path(path)?;
        let tag = tagged.primary_tag_mut().expect("tag");
        tag.set_artist(artist.to_string());
        tag.set_title(format!("{album} {track}"));
        tag.insert_text(ItemKey::AlbumArtist, artist.to_string());
        tag.save_to_path(path, WriteOptions::default())?;
        Ok(())
    }

    async fn wait_for_active_refreshes(run_id: &str, units: u64) {
        loop {
            let run = super::super::orchestrator::get_sync_run(run_id)
                .await
                .expect("run")
                .expect("run");
            let refreshing = run
                .current
                .is_some_and(|current| matches!(current.stage, SyncStageKey::ProviderRefresh));
            if refreshing && run.active_units >= units {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Runs `init` as a plugin, its entrypoint having registered whatever providers it needs.
    async fn start_plugin(
        id: &str,
        scopes: &[&str],
        init: &str,
    ) -> anyhow::Result<(
        crate::plugins::executor::PluginExecutorHandle,
        tempfile::TempDir,
    )> {
        crate::plugins::testing::start_runtime().await?;
        let plugins = tempfile::tempdir()?;
        crate::plugins::testing::write_plugin(&plugins.path().join(id), id, scopes, init)?;
        let server_info = crate::plugins::server::load_server_info().await?;
        let auth = crate::plugins::auth::AuthCapabilities::from_config(&STATE.config().auth);
        let (runtime, errors) =
            crate::plugins::executor::PluginExecutorHandle::discover_from_plugins_dir_with_db_and_modules(
                plugins.path().to_path_buf(),
                server_info,
                auth,
                STATE.db.get(),
                Vec::new(),
                Some(STATE.settings.clone()),
            )?;
        assert!(errors.is_empty(), "{errors:?}");
        runtime.exec_plugin(id).await?;
        crate::plugins::STATE
            .publish_runtime(&crate::services::STATE.generation(), runtime.clone());
        Ok((runtime, plugins))
    }

    /// Lets `Canonical Spelling` hold the most references, so it survives a merge with `Variant
    /// Spelling`, and returns the path of every file of the variant's albums.
    fn canonical_and_variant_files(root: &Path, variants: &[&str]) -> anyhow::Result<Vec<PathBuf>> {
        for track in 1..=8 {
            artist_file(
                &root.join(format!("Canonical/{track:02}.flac")),
                "Canonical",
                "Canonical Spelling",
                track,
            )?;
        }
        let mut files = Vec::new();
        for album in variants {
            let file = root.join(format!("Variant/{album}/01.flac"));
            artist_file(&file, album, "Variant Spelling", 1)?;
            files.push(file);
        }
        Ok(files)
    }

    async fn artist_names() -> anyhow::Result<Vec<String>> {
        let db = STATE.db.read().await;
        Ok(roots::members::<lyra_db::Artist>(&*db, Root::Artists)?
            .into_iter()
            .map(|artist| artist.artist_name)
            .collect())
    }

    struct QueuedRefreshScan {
        result: anyhow::Result<()>,
        failed_units: u64,
        /// Each release's artist names.
        release_artists: Vec<Vec<String>>,
        artists: Vec<String>,
        variant_files_synced: Vec<bool>,
    }

    /// Scans four releases by two spellings of an artist, whose provider gives both one unique id.
    /// The provider's refreshes queue behind a held call until all four have built their contexts,
    /// with the release of the spelling that survives a merge first, so a merge between refreshes
    /// deletes the artist the ones still queued name.
    async fn scan_with_queued_refreshes(
        name: &str,
        cancel: bool,
    ) -> anyhow::Result<QueuedRefreshScan> {
        const PROVIDER: &str = r#"
            local metadata = require("@lyra/metadata")
            local ET = metadata.EntityType
            local provider = metadata.Provider.new("queued")
            provider:id({ id_type = "artist_id", entity = ET.Artist, unique = true, scheme = "queued:artist" })
            provider:refresh(ET.Release, function(ctx)
                local layer = provider:layer(ctx.artists[1].id)
                layer:set_field("artist_name", "Canonical Spelling")
                layer:set_id("artist_id", "same-artist")
                layer:save()
            end)
        "#;

        crate::plugins::testing::init_default_test_state()?;
        let (root, library) = empty_library(name).await?;
        let variant_files =
            canonical_and_variant_files(&root, &["Variant One", "Variant Two", "Variant Three"])?;
        let (runtime, _plugins) = start_plugin("queued", &["lyra.metadata"], PROVIDER).await?;

        let (held, is_held) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        let hold = std::sync::Mutex::new(Some((held, released)));
        let holder = tokio::spawn(crate::services::providers::with_provider_call(
            "queued",
            crate::services::providers::ProviderCallStage::MetadataRefresh,
            crate::services::plugin_dispatch::TransitionWait::interactive(),
            |_| Some(crate::services::providers::ProviderLookup::Call(())),
            move |(), _calls| {
                let hold = hold.lock().expect("hold").take();
                async move {
                    if let Some((held, released)) = hold {
                        let _ = held.send(());
                        let _ = released.await;
                    }
                    Ok(())
                }
            },
        ));
        is_held.await?;

        let progress = super::super::orchestrator::begin_test_run(&library).await;
        let run_id = progress.run_id().to_string();
        let gate: ArtifactGate = {
            let run_id = run_id.clone();
            std::sync::Arc::new(move |title: Option<String>| {
                let run_id = run_id.clone();
                Box::pin(async move {
                    if title.as_deref() != Some("Canonical") {
                        wait_for_active_refreshes(&run_id, 1).await;
                    }
                })
            })
        };
        let hooks = Hooks {
            before_refresh: Some(gate),
            ..Hooks::default()
        };
        let driver = async {
            wait_for_active_refreshes(&run_id, 4).await;
            if cancel {
                super::super::orchestrator::cancel_sync_run(&run_id)
                    .await
                    .expect("cancel");
            }
            let _ = release.send(());
        };
        let (result, ()) = tokio::time::timeout(
            Duration::from_secs(60),
            futures::future::join(sync_with(hooks.into(), &library, progress.clone()), driver),
        )
        .await?;
        holder.await??;

        let failed_units = super::super::orchestrator::get_sync_run(&run_id)
            .await?
            .expect("run")
            .progress
            .failed_units;
        super::super::orchestrator::end_test_run(&progress).await;

        let release_artists = {
            let db = STATE.db.read().await;
            let mut release_artists = Vec::new();
            for release in lyra_db::releases::get(&db, library.db_id.expect("library db_id"))? {
                let release_id = release.db_id.map(DbId::from).expect("release db_id");
                release_artists.push(
                    lyra_db::artists::get(&db, release_id)?
                        .into_iter()
                        .map(|artist| artist.artist_name)
                        .collect(),
                );
            }
            release_artists
        };
        let mut variant_files_synced = Vec::new();
        for file in &variant_files {
            variant_files_synced.push(synced(&library, file).await?);
        }
        let artists = artist_names().await?;
        runtime.shutdown().await?;
        let _ = std::fs::remove_dir_all(root);
        Ok(QueuedRefreshScan {
            result,
            failed_units,
            release_artists,
            artists,
            variant_files_synced,
        })
    }

    fn assert_merged_into_canonical_spelling(scan: &QueuedRefreshScan) {
        assert_eq!(scan.failed_units, 0, "a queued refresh lost its artist");
        assert_eq!(scan.artists, ["Canonical Spelling"]);
        assert_eq!(scan.release_artists.len(), 4);
        for artists in &scan.release_artists {
            assert_eq!(artists, &["Canonical Spelling"]);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn queued_refreshes_keep_their_artist_until_the_batch_is_done() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let scan = scan_with_queued_refreshes("queued-refreshes", false).await?;
        scan.result.as_ref().expect("scan");
        assert_merged_into_canonical_spelling(&scan);
        assert_eq!(scan.variant_files_synced, [true, true, true]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_scan_cancelled_during_refreshes_merges_them_and_leaves_its_files_unsynced()
    -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let scan = scan_with_queued_refreshes("cancelled-refreshes", true).await?;
        let err = scan.result.as_ref().expect_err("the run wasn't cancelled");
        assert!(
            err.downcast_ref::<SyncRunControlError>().is_some(),
            "{err:#}"
        );
        assert_merged_into_canonical_spelling(&scan);
        assert_eq!(scan.variant_files_synced, [false, false, false]);
        Ok(())
    }

    /// Answers every track's lyrics lookup with the artist name it was asked about.
    const ECHO_LYRICS: &str = r#"
        http.set_rate_limit({ domain = "example.invalid", requests_per_second = 1 })
        provider:lyrics({ timeout_ms = 5000 }, function(ctx)
            return {
                kind = "hit",
                candidates = {{
                    lyrics = { id = ctx.track_id, language = "und", plain_text = ctx.artist_name, lines = {} },
                    title = ctx.track_name,
                    artist = ctx.artist_name,
                    duration_ms = ctx.duration_ms,
                    language = "und",
                }},
            }
        end)
    "#;

    async fn track_of_file(library: &Library, file: &Path) -> anyhow::Result<DbId> {
        let entry = entry_for(library, file).await?.db_id.expect("entry db_id");
        let db = STATE.db.read().await;
        lyra_db::tracks::get_by_entry(&db, entry)?
            .into_iter()
            .next()
            .and_then(|track| track.db_id.map(DbId::from))
            .ok_or_else(|| anyhow::anyhow!("no track for {}", file.display()))
    }

    async fn lyrics_of_track(track: DbId) -> anyhow::Result<Vec<String>> {
        let db = STATE.db.read().await;
        Ok(lyra_db::lyrics::get_visible_for_track(&*db, track, None)?
            .into_iter()
            .map(|row| row.plain_text)
            .collect())
    }

    const LYRICS_SCOPES: &[&str] = &["harmony.http", "lyra.metadata", "lyra.lyrics"];

    #[tokio::test(flavor = "multi_thread")]
    async fn lyrics_are_looked_up_under_the_artist_a_merge_leaves() -> anyhow::Result<()> {
        let init = format!(
            r#"
            local http = require("@harmony/http")
            local metadata = require("@lyra/metadata")
            local ET = metadata.EntityType
            local provider = metadata.Provider.new("renaming")
            provider:id({{ id_type = "artist_id", entity = ET.Artist, unique = true, scheme = "renaming:artist" }})
            provider:refresh(ET.Release, function(ctx)
                local layer = provider:layer(ctx.artists[1].id)
                layer:set_id("artist_id", "same-artist")
                layer:save()
            end)
            {ECHO_LYRICS}
        "#
        );
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
        let (root, library) = empty_library("merged-lyrics").await?;
        let files = canonical_and_variant_files(&root, &["Variant"])?;
        let (runtime, _plugins) = start_plugin("renaming", LYRICS_SCOPES, &init).await?;

        sync(&library).await?;

        assert_eq!(artist_names().await?, ["Canonical Spelling"]);
        assert_eq!(
            lyrics_of_track(track_of_file(&library, &files[0]).await?).await?,
            ["Canonical Spelling"]
        );
        runtime.shutdown().await?;
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_merged_away_release_keeps_its_tracks_lyrics_and_sync() -> anyhow::Result<()> {
        let init = format!(
            r#"
            local http = require("@harmony/http")
            local metadata = require("@lyra/metadata")
            local ET = metadata.EntityType
            local provider = metadata.Provider.new("rejoining")
            provider:id({{ id_type = "release_id", entity = ET.Release, unique = true, scheme = "rejoining:release" }})
            provider:refresh(ET.Release, function(ctx)
                local layer = provider:layer(ctx.id)
                layer:set_id("release_id", "same-release")
                layer:save()
            end)
            {ECHO_LYRICS}
        "#
        );
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
        let (root, library) = empty_library("merged-release").await?;
        let files = [root.join("Disc One/01.flac"), root.join("Disc Two/01.flac")];
        album_file(&files[0], "Disc One", 1)?;
        album_file(&files[1], "Disc Two", 1)?;
        let (runtime, _plugins) = start_plugin("rejoining", LYRICS_SCOPES, &init).await?;

        let progress = super::super::orchestrator::begin_test_run(&library).await;
        sync_with(Hooks::default().into(), &library, progress.clone()).await?;
        let run = super::super::orchestrator::get_sync_run(progress.run_id())
            .await?
            .expect("run");
        super::super::orchestrator::end_test_run(&progress).await;

        assert_eq!(run.progress.failed_units, 0);
        let releases = {
            let db = STATE.db.read().await;
            lyra_db::releases::get(&db, library.db_id.expect("library db_id"))?
        };
        assert_eq!(releases.len(), 1, "the two releases didn't merge");
        for file in &files {
            assert!(synced(&library, file).await?, "{}", file.display());
            assert_eq!(
                lyrics_of_track(track_of_file(&library, file).await?)
                    .await?
                    .len(),
                1,
                "{}",
                file.display()
            );
        }
        runtime.shutdown().await?;
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_merge_between_providers_refreshes_contexts_the_first_left_alone()
    -> anyhow::Result<()> {
        const PROVIDERS: &str = r#"
            local metadata = require("@lyra/metadata")
            local ET = metadata.EntityType
            local first = metadata.Provider.new("first")
            first:id({ id_type = "artist_id", entity = ET.Artist, unique = true, scheme = "first:artist" })
            first:refresh(ET.Release, function(ctx)
                if ctx.release_title == "Variant Unmatched" then
                    error("no match")
                end
                local layer = first:layer(ctx.artists[1].id)
                layer:set_id("artist_id", "same-artist")
                layer:save()
            end)
            local second = metadata.Provider.new("second")
            second:refresh(ET.Release, function(ctx)
                if ctx.artists[1].artist_name ~= "Canonical Spelling" then
                    error("stale context: " .. ctx.artists[1].artist_name)
                end
            end)
        "#;
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
        let (root, library) = empty_library("merge-between-providers").await?;
        canonical_and_variant_files(&root, &["Variant Matched", "Variant Unmatched"])?;
        let (runtime, _plugins) =
            start_plugin("provider-pair", &["lyra.metadata"], PROVIDERS).await?;

        let progress = super::super::orchestrator::begin_test_run(&library).await;
        sync_library_pipeline(
            &STATE.db.get(),
            &library,
            LibrarySyncOptions {
                releases: SyncReleases::All,
                covers: CoverSyncOptions::SKIP,
            },
            progress.clone(),
        )
        .await?;
        let run = super::super::orchestrator::get_sync_run(progress.run_id())
            .await?
            .expect("run");
        super::super::orchestrator::end_test_run(&progress).await;

        assert_eq!(
            run.progress.failed_units, 1,
            "only the first provider's own failure; the second read the artist the merge left"
        );
        assert_eq!(artist_names().await?, ["Canonical Spelling"]);
        runtime.shutdown().await?;
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sync_of_every_release_refreshes_each_once() -> anyhow::Result<()> {
        use crate::services::{
            CoverSyncOptions,
            EntityType,
            providers::provider_registry,
            testing::{
                FakePluginDispatch,
                FakeResponse,
            },
        };

        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("every-release").await?;
        album_track(&root.join("Old"), "Old")?;
        sync(&library).await?;
        album_track(&root.join("New"), "New")?;

        let dispatch = FakePluginDispatch::install();
        let handler = dispatch.add(FakeResponse::Value(serde_json::Value::Null));
        lyra_db::providers::register(&mut *STATE.db.write().await, "counting")?;
        {
            let registry = provider_registry();
            let mut registry = registry.write().await;
            registry.register(
                crate::services::plugin_scope::PluginId::new("counting_plugin")?,
                "counting".to_string(),
                None,
            )?;
            registry.set_refresh_callback("counting", EntityType::Release, handler);
        }

        let options = LibrarySyncOptions {
            releases: SyncReleases::All,
            covers: CoverSyncOptions::SKIP,
        };
        let progress = super::super::orchestrator::begin_test_run(&library).await;
        // Another refresh of the library, as a provider sync's, holds it; the sync waits for it.
        let library_db_id = library.db_id.expect("library db_id");
        let locks = crate::services::providers::library_refresh_locks();
        locks.lock().await.insert(library_db_id);
        let every = tokio::spawn({
            let library = library.clone();
            let progress = progress.clone();
            async move { sync_library_pipeline(&STATE.db.get(), &library, options, progress).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        assert!(!every.is_finished(), "the sync waits for the other refresh");
        assert!(dispatch.calls(handler).is_empty());
        locks.lock().await.remove(&library_db_id);
        every.await??;
        let run = super::super::orchestrator::get_sync_run(progress.run_id())
            .await?
            .expect("run");
        super::super::orchestrator::end_test_run(&progress).await;

        let releases = {
            let db = STATE.db.read().await;
            let mut ids: Vec<String> =
                lyra_db::releases::get(&db, library.db_id.expect("library db_id"))?
                    .into_iter()
                    .map(|release| release.id)
                    .collect();
            ids.sort();
            ids
        };
        assert_eq!(releases.len(), 2);
        assert_eq!(
            refreshed_ids(&dispatch, handler),
            releases,
            "each release, the new one included, is refreshed once"
        );
        let settled =
            run.progress.completed_units + run.progress.failed_units + run.progress.skipped_units;
        assert_eq!(
            settled, run.progress.total_units,
            "every unit counted is settled"
        );

        // Without every release, only the changed one asks the providers.
        album_track(&root.join("Newer"), "Newer")?;
        sync(&library).await?;
        let calls = refreshed_ids(&dispatch, handler);
        assert_eq!(calls.len(), 3);
        assert!(
            !releases
                .iter()
                .any(|id| calls.iter().filter(|call| *call == id).count() > 1),
            "the unchanged releases are not refreshed again"
        );
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_changed_sync_fetches_the_covers_of_its_artists() -> anyhow::Result<()> {
        use crate::services::testing::{
            FakePluginDispatch,
            FakeResponse,
        };
        use lofty::{
            config::WriteOptions,
            prelude::*,
            tag::ItemKey,
        };

        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let dispatch = FakePluginDispatch::install();
        let handler = dispatch
            .register_artist_cover_handler(
                "artist_covers",
                FakeResponse::Value(serde_json::Value::Null),
            )
            .await?;
        let (root, library) = empty_library("artist-covers").await?;
        let audio = album_track(&root.join("Album"), "Album")?;
        let mut tagged = lofty::read_from_path(&audio)?;
        let tag = tagged
            .primary_tag_mut()
            .ok_or_else(|| anyhow::anyhow!("fixture has no primary tag"))?;
        tag.insert_text(ItemKey::AlbumArtist, "Host".to_string());
        tag.set_artist("Guest".to_string());
        tag.save_to_path(&audio, WriteOptions::default())?;

        let progress = super::super::orchestrator::begin_test_run(&library).await;
        sync_library_pipeline(
            &STATE.db.get(),
            &library,
            LibrarySyncOptions::default(),
            progress.clone(),
        )
        .await?;
        let run = super::super::orchestrator::get_sync_run(progress.run_id())
            .await?
            .expect("run");
        super::super::orchestrator::end_test_run(&progress).await;

        let mut names = dispatch.artist_names(handler);
        names.sort();
        assert_eq!(names, ["Guest", "Host"], "the track's artist too");
        let settled =
            run.progress.completed_units + run.progress.failed_units + run.progress.skipped_units;
        assert_eq!(
            settled, run.progress.total_units,
            "every unit counted is settled"
        );
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    /// Copies the fixture to `path` as track `track` of `album`.
    fn album_file(path: &Path, album: &str, track: u32) -> anyhow::Result<()> {
        tagged_file(path, &(album, track, None, None))
    }

    /// A file's album, track, disc and barcode, from 2001.
    type Tags<'a> = (&'a str, u32, Option<u32>, Option<&'a str>);

    /// Copies the fixture to `path` tagged with `tags`.
    fn tagged_file(path: &Path, tags: &Tags<'_>) -> anyhow::Result<()> {
        let &(album, track, disc, barcode) = tags;
        use lofty::{
            config::WriteOptions,
            prelude::*,
            tag::ItemKey,
        };

        std::fs::create_dir_all(path.parent().expect("parent"))?;
        std::fs::copy(fixture_flac(), path)?;
        let mut tagged = lofty::read_from_path(path)?;
        let tag = tagged
            .primary_tag_mut()
            .ok_or_else(|| anyhow::anyhow!("fixture has no primary tag"))?;
        tag.set_album(album.to_string());
        tag.set_track(track);
        tag.insert_text(ItemKey::RecordingDate, "2001".to_string());
        tag.remove_disk();
        tag.remove_disk_total();
        if let Some(disc) = disc {
            tag.set_disk(disc);
        }
        tag.remove_key(ItemKey::Barcode);
        if let Some(barcode) = barcode {
            tag.insert_text(ItemKey::Barcode, barcode.to_string());
        }
        tag.save_to_path(path, WriteOptions::default())?;
        Ok(())
    }

    /// A file under a fresh library, by its path.
    type TaggedFile<'a> = (&'a str, Tags<'a>);

    /// Syncs `files` and returns the titles of the releases they made.
    async fn releases_from(name: &str, files: &[TaggedFile<'_>]) -> anyhow::Result<Vec<String>> {
        let (root, library) = empty_library(name).await?;
        for (path, tags) in files {
            tagged_file(&root.join(path), tags)?;
        }
        sync(&library).await?;
        let mut releases = BTreeMap::new();
        for (path, _) in files {
            let entry = entry_for(&library, &root.join(path))
                .await?
                .db_id
                .expect("entry db_id");
            let release = release_of(entry).await?;
            releases.insert(release, release_title_of(&library, &root.join(path)).await?);
        }
        let _ = std::fs::remove_dir_all(root);
        Ok(releases.into_values().collect())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sibling_discs_coalesce_by_their_tags() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let cases: [(&str, &[TaggedFile<'_>], &[&str]); 9] = [
            (
                "discs-same-title",
                &[
                    ("Box/ESCL-5394/01.flac", ("ALL TIME", 1, Some(1), None)),
                    ("Box/ESCL-5395/01.flac", ("ALL TIME", 1, Some(2), None)),
                ],
                &["ALL TIME"],
            ),
            (
                "discs-titled-per-disc",
                &[
                    (
                        "Box/ESCL-5394/01.flac",
                        ("ALL TIME [Disc 1]", 1, Some(1), None),
                    ),
                    (
                        "Box/ESCL-5395/01.flac",
                        ("ALL TIME [Disc 2]", 1, Some(2), None),
                    ),
                ],
                &["ALL TIME [Disc 1]", "ALL TIME [Disc 2]"],
            ),
            (
                "disc-folders",
                &[
                    ("Box/Disc 1/01.flac", ("Set", 1, None, None)),
                    ("Box/Disc 2/01.flac", ("Set Bonus", 1, None, None)),
                ],
                &["Set"],
            ),
            (
                "separate-albums",
                &[
                    ("Artist/One/01.flac", ("One", 1, None, None)),
                    ("Artist/Two/02.flac", ("Two", 2, None, None)),
                ],
                &["One", "Two"],
            ),
            (
                "separate-first-discs",
                &[
                    ("Artist/One/01.flac", ("One", 1, Some(1), None)),
                    ("Artist/Two/02.flac", ("Two", 2, Some(1), None)),
                ],
                &["One", "Two"],
            ),
            (
                "shared-barcode",
                &[
                    ("Box/A/01.flac", ("Alpha", 1, None, Some("4547366000001"))),
                    ("Box/B/02.flac", ("Beta", 2, None, Some("4547366000001"))),
                ],
                &["Alpha"],
            ),
            (
                "other-barcode",
                &[
                    ("Box/A/01.flac", ("Set", 1, Some(1), Some("4547366000001"))),
                    ("Box/B/01.flac", ("Set", 1, Some(2), Some("4547366000002"))),
                ],
                &["Set", "Set"],
            ),
            (
                "editions",
                &[
                    ("Artist/Standard/01.flac", ("Album", 1, Some(1), None)),
                    (
                        "Artist/Deluxe Bonus/01.flac",
                        ("Album Deluxe", 1, Some(2), None),
                    ),
                ],
                &["Album", "Album Deluxe"],
            ),
            (
                "discs-in-container-folders",
                &[
                    ("Artist/Box/CD1/audio/01.flac", ("Set", 1, Some(1), None)),
                    ("Artist/Box/CD2/audio/01.flac", ("Set", 1, Some(2), None)),
                ],
                &["Set"],
            ),
        ];
        for (name, files, expected) in cases {
            crate::services::testing::init_default_test_state()?;
            let mut titles = releases_from(name, files).await?;
            titles.sort();
            assert_eq!(titles, expected, "{name}");
        }
        Ok(())
    }

    async fn release_title_of(library: &Library, path: &Path) -> anyhow::Result<String> {
        let entry = entry_for(library, path).await?.db_id.expect("entry db_id");
        let release = release_of(entry).await?;
        Ok(
            lyra_db::releases::get_by_id(&*STATE.db.read().await, release)?
                .expect("release")
                .release_title,
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn loose_tracks_in_an_artist_folder_keep_its_albums_apart() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("loose-artist").await?;
        let files = [
            (root.join("Artist/loose.flac"), "Loose", 1),
            (root.join("Artist/One/01.flac"), "One", 2),
            (root.join("Artist/Two/02.flac"), "Two", 3),
        ];
        for (path, album, track) in &files {
            album_file(path, album, *track)?;
        }
        sync(&library).await?;

        for (path, album, _) in &files {
            assert_eq!(release_title_of(&library, path).await?, *album);
        }
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn removing_a_nested_file_reapplies_its_album() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("nested-remove").await?;
        let main = root.join("Album/main.flac");
        let bonus = root.join("Album/Bonus/bonus.flac");
        album_file(&main, "Album", 1)?;
        album_file(&bonus, "Album", 2)?;
        sync(&library).await?;

        std::fs::remove_file(&bonus)?;
        let hooks = std::sync::Arc::new(Hooks::default());
        sync_with(
            hooks.clone(),
            &library,
            SyncRunProgress::unreported_for_test(),
        )
        .await?;

        assert_eq!(*hooks.applied.lock().expect("applied"), ["Album"]);
        assert!(synced(&library, &main).await?);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    async fn follow_links(library: &Library) -> anyhow::Result<Library> {
        let followed = Library {
            follow_links: Some(true),
            ..library.clone()
        };
        STATE
            .db
            .write()
            .await
            .transaction_mut(|t| lyra_db::libraries::update(t, &followed))
            .map_err(anyhow::Error::from)
    }

    async fn local_cover_of(library: &Library, audio: &Path) -> anyhow::Result<Option<String>> {
        let release = release_of(
            entry_for(library, audio)
                .await?
                .db_id
                .expect("stored entry has an id"),
        )
        .await?;
        let db = STATE.db.read().await;
        Ok(
            lyra_db::covers::get_source(&*db, release, lyra_db::covers::CoverSource::Local)?
                .map(|cover| cover.path),
        )
    }

    fn cover_path(dir: &Path) -> String {
        dir.join("cover.png").to_string_lossy().into_owned()
    }

    async fn sole_release_of(library: &Library, audio: &Path) -> anyhow::Result<DbId> {
        release_of(entry_for(library, audio).await?.db_id.expect("entry db_id")).await
    }

    /// Stores a cover for `release` from `source` at `path`.
    async fn store_cover(
        release: DbId,
        source: lyra_db::covers::CoverSource,
        path: &Path,
    ) -> anyhow::Result<()> {
        STATE.db.write().await.transaction_mut(|t| {
            lyra_db::covers::upsert(
                t,
                release,
                source,
                lyra_db::covers::Cover {
                    db_id: None,
                    id: lyra_db::nanoid(),
                    path: path.to_string_lossy().into_owned(),
                    mime_type: "image/png".to_string(),
                    hash: source.as_str().to_string(),
                    blurhash: None,
                    provider_id: None,
                },
            )
        })?;
        Ok(())
    }

    /// The library-wide release cover pass a refresh ends with.
    async fn refresh_release_covers(library: &Library) -> anyhow::Result<usize> {
        let covers_root = configured_covers_root();
        crate::services::covers::sync_and_persist_release_covers_for_library(
            &STATE.db.get(),
            CoverPaths {
                library_root: Some(&library.path),
                covers_root: &covers_root,
            },
            library.db_id.expect("library db_id"),
            CoverSyncOptions::default(),
            None,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_flat_folder_of_two_albums_gives_neither_its_cover() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("cover-flat").await?;
        let (one, two) = (root.join("Mix/one.flac"), root.join("Mix/two.flac"));
        tagged_file(&one, &("One", 1, None, None))?;
        tagged_file(&two, &("Two", 1, None, None))?;
        std::fs::write(root.join("Mix/cover.png"), PNG_1X1)?;

        sync(&library).await?;

        assert_eq!(local_cover_of(&library, &one).await?, None);
        assert_eq!(local_cover_of(&library, &two).await?, None);
        refresh_release_covers(&library).await?;
        assert_eq!(local_cover_of(&library, &one).await?, None);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_image_above_two_album_folders_is_neither_album_s() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("cover-road-trip").await?;
        let one = album_track(&root.join("Road Trip/One"), "One")?;
        let two = album_track(&root.join("Road Trip/Two"), "Two")?;
        std::fs::write(root.join("Road Trip/cover.png"), PNG_1X1)?;

        sync(&library).await?;

        assert_eq!(local_cover_of(&library, &one).await?, None);
        assert_eq!(local_cover_of(&library, &two).await?, None);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn one_album_over_disc_folders_keeps_the_image_above_them() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("cover-discs").await?;
        let (one, two) = (
            root.join("Album/CD1/01.flac"),
            root.join("Album/CD2/01.flac"),
        );
        tagged_file(&one, &("Album", 1, Some(1), None))?;
        tagged_file(&two, &("Album", 1, Some(2), None))?;
        std::fs::write(root.join("Album/cover.png"), PNG_1X1)?;

        sync(&library).await?;

        assert_eq!(
            sole_release_of(&library, &one).await?,
            sole_release_of(&library, &two).await?,
            "the discs make one release"
        );
        assert_eq!(
            local_cover_of(&library, &one).await?,
            Some(cover_path(&root.join("Album")))
        );
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_album_keeps_the_art_in_its_own_folder_beside_other_albums() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("cover-own-folder").await?;
        let own = album_track(&root.join("Artist/Own"), "Own")?;
        let other = album_track(&root.join("Artist/Other"), "Other")?;
        std::fs::write(root.join("Artist/Own/cover.png"), PNG_1X1)?;

        sync(&library).await?;

        assert_eq!(
            local_cover_of(&library, &own).await?,
            Some(cover_path(&root.join("Artist/Own")))
        );
        assert_eq!(local_cover_of(&library, &other).await?, None);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    /// The accepted limit of judging by what the folder holds: one album's folder alone under
    /// another looks just like an album's own disc folder.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_lone_album_under_a_playlist_folder_still_inherits_its_image() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("cover-lone-album").await?;
        let audio = album_track(&root.join("Playlist/Album"), "Album")?;
        std::fs::write(root.join("Playlist/cover.png"), PNG_1X1)?;

        sync(&library).await?;

        assert_eq!(
            local_cover_of(&library, &audio).await?,
            Some(cover_path(&root.join("Playlist")))
        );
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_rescan_and_a_refresh_drop_a_stored_cover_from_a_shared_folder() -> anyhow::Result<()>
    {
        use lyra_db::covers::CoverSource;

        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("cover-stored-wrong").await?;
        let (one, two) = (root.join("Mix/one.flac"), root.join("Mix/two.flac"));
        tagged_file(&one, &("One", 1, None, None))?;
        tagged_file(&two, &("Two", 1, None, None))?;
        let wrong = root.join("Mix/cover.png");
        std::fs::write(&wrong, PNG_1X1)?;
        sync(&library).await?;
        let (first, second) = (
            sole_release_of(&library, &one).await?,
            sole_release_of(&library, &two).await?,
        );
        let elsewhere = tempfile::tempdir()?;
        for (name, source) in [
            ("manual.png", CoverSource::Manual),
            ("provider.png", CoverSource::Provider),
        ] {
            let kept = elsewhere.path().join(name);
            std::fs::write(&kept, PNG_1X1)?;
            store_cover(first, source, &kept).await?;
        }
        store_cover(first, CoverSource::Local, &wrong).await?;
        store_cover(second, CoverSource::Local, &wrong).await?;

        change_album_directory(&root.join("Mix"), "later.txt", 10)?;
        shift_mtime(&one, 10)?;
        sync(&library).await?;

        assert_eq!(local_cover_of(&library, &one).await?, None, "the rescan");
        let kept = lyra_db::covers::sources(&*STATE.db.read().await, first)?
            .into_iter()
            .map(|sourced| sourced.source)
            .collect::<Vec<_>>();
        assert_eq!(kept.len(), 2, "the manual and provider covers stay");
        assert!(kept.contains(&CoverSource::Manual) && kept.contains(&CoverSource::Provider));

        store_cover(second, CoverSource::Local, &wrong).await?;
        refresh_release_covers(&library).await?;
        assert_eq!(local_cover_of(&library, &two).await?, None, "the refresh");
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_locked_release_keeps_a_stored_cover_from_a_shared_folder() -> anyhow::Result<()> {
        use lyra_db::covers::CoverSource;

        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("cover-stored-locked").await?;
        let (one, two) = (root.join("Mix/one.flac"), root.join("Mix/two.flac"));
        tagged_file(&one, &("One", 1, None, None))?;
        tagged_file(&two, &("Two", 1, None, None))?;
        let wrong = root.join("Mix/cover.png");
        std::fs::write(&wrong, PNG_1X1)?;
        sync(&library).await?;
        let release = sole_release_of(&library, &one).await?;
        store_cover(release, CoverSource::Local, &wrong).await?;
        {
            let mut db_write = STATE.db.write().await;
            let mut locked =
                lyra_db::releases::get_by_id(&*db_write, release)?.expect("release exists");
            locked.locked = Some(true);
            lyra_db::releases::update(&mut db_write, &locked)?;
        }

        refresh_release_covers(&library).await?;

        assert_eq!(
            local_cover_of(&library, &one).await?,
            Some(wrong.to_string_lossy().into_owned())
        );
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_stored_cover_stays_while_a_release_folder_is_unavailable() -> anyhow::Result<()> {
        use lyra_db::covers::CoverSource;

        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("cover-stored-unavailable").await?;
        let one = album_track(&root.join("Road Trip/One"), "One")?;
        album_track(&root.join("Road Trip/Two"), "Two")?;
        let wrong = root.join("Road Trip/cover.png");
        std::fs::write(&wrong, PNG_1X1)?;
        sync(&library).await?;
        let release = sole_release_of(&library, &one).await?;
        store_cover(release, CoverSource::Local, &wrong).await?;

        // The album's folder goes away, as an unmounted disc would.
        let away = root.join("Road Trip/One.away");
        std::fs::rename(root.join("Road Trip/One"), &away)?;
        refresh_release_covers(&library).await?;
        let kept = Some(wrong.to_string_lossy().into_owned());
        assert_eq!(local_cover_of(&library, &one).await?, kept, "unmounted");
        std::fs::rename(&away, root.join("Road Trip/One"))?;

        // The album's folder is there but doesn't list.
        {
            use std::os::unix::fs::PermissionsExt;

            let one_dir = root.join("Road Trip/One");
            std::fs::set_permissions(&one_dir, std::fs::Permissions::from_mode(0o000))?;
            if std::fs::read_dir(&one_dir).is_err() {
                let result = refresh_release_covers(&library).await;
                std::fs::set_permissions(&one_dir, std::fs::Permissions::from_mode(0o755))?;
                assert!(result.is_err(), "the unlistable folder went unreported");
                assert_eq!(local_cover_of(&library, &one).await?, kept, "unlistable");
            } else {
                std::fs::set_permissions(&one_dir, std::fs::Permissions::from_mode(0o755))?;
            }
        }

        refresh_release_covers(&library).await?;
        assert_eq!(
            local_cover_of(&library, &one).await?,
            None,
            "listable again"
        );
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_file_marked_missing_still_counts_against_the_folder() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("cover-missing-counts").await?;
        let (one, two) = (root.join("Mix/one.flac"), root.join("Mix/two.flac"));
        tagged_file(&one, &("One", 1, None, None))?;
        tagged_file(&two, &("Two", 1, None, None))?;
        std::fs::write(root.join("Mix/cover.png"), PNG_1X1)?;
        sync(&library).await?;

        std::fs::remove_file(&two)?;
        sync(&library).await?;
        assert!(entry_for(&library, &two).await?.is_missing());

        refresh_release_covers(&library).await?;
        assert_eq!(local_cover_of(&library, &one).await?, None);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_track_on_two_releases_gives_neither_the_folder() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("cover-two-releases").await?;
        let audio = album_track(&root.join("Album"), "Album")?;
        std::fs::write(root.join("Album/cover.png"), PNG_1X1)?;
        sync(&library).await?;
        assert!(local_cover_of(&library, &audio).await?.is_some());
        {
            let entry = entry_for(&library, &audio).await?.db_id.expect("entry");
            let mut db = STATE.db.write().await;
            let track = lyra_db::tracks::get_by_entry(&db, entry)?[0]
                .db_id
                .clone()
                .map(DbId::from)
                .expect("track");
            let other = lyra_db::fixtures::insert_release(&mut db, "Other")?;
            lyra_db::fixtures::connect(&mut db, library.db_id.expect("library"), other)?;
            lyra_db::fixtures::link_track(&mut db, other, track)?;
        }

        refresh_release_covers(&library).await?;

        assert_eq!(local_cover_of(&library, &audio).await?, None);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_album_with_an_unread_file_neither_gains_nor_loses_its_cover() -> anyhow::Result<()>
    {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("cover-unread").await?;
        let album = root.join("Album");
        let audio = album_track(&album, "Album")?;
        std::fs::write(album.join("cover.png"), PNG_1X1)?;
        sync(&library).await?;
        let recorded = local_cover_of(&library, &audio).await?;
        assert!(recorded.is_some());

        let unread = album.join("unread.flac");
        tagged_file(&unread, &("Album", 2, None, None))?;
        if !deny_reads(&unread)? {
            allow_reads(&unread)?;
            return Ok(());
        }
        std::fs::write(album.join("cover.jpg"), PNG_1X1)?;
        shift_mtime(&album, 10)?;
        sync(&library).await?;
        refresh_release_covers(&library).await?;

        assert!(!synced(&library, &unread).await?, "the file went unread");
        assert_eq!(
            local_cover_of(&library, &audio).await?,
            recorded,
            "unread audio could be anyone's, so nothing is recorded or dropped"
        );

        allow_reads(&unread)?;
        shift_mtime(&album, 20)?;
        sync(&library).await?;
        assert_eq!(
            local_cover_of(&library, &audio).await?,
            Some(album.join("cover.jpg").to_string_lossy().into_owned()),
            "once read, the album's own folder gives its cover again"
        );
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_scan_drops_the_cover_of_an_album_it_did_not_change() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("cover-unchanged-album").await?;
        let one = album_track(&root.join("Road Trip/One"), "One")?;
        std::fs::write(root.join("Road Trip/cover.png"), PNG_1X1)?;
        sync(&library).await?;
        assert_eq!(
            local_cover_of(&library, &one).await?,
            Some(cover_path(&root.join("Road Trip"))),
            "alone beneath the folder, the album takes its image"
        );

        album_track(&root.join("Road Trip/Two"), "Two")?;
        sync(&library).await?;

        assert_eq!(local_cover_of(&library, &one).await?, None);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_unchanged_scan_repairs_stored_covers_but_not_without_evidence() -> anyhow::Result<()>
    {
        use lyra_db::covers::CoverSource;

        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("cover-unchanged-scan").await?;
        let parent = root.join("Road Trip");
        let (one, two, three) = (
            album_track(&parent.join("One"), "One")?,
            album_track(&parent.join("Two"), "Two")?,
            album_track(&parent.join("Three"), "Three")?,
        );
        std::fs::write(parent.join("Two/cover.png"), PNG_1X1)?;
        let wrong = parent.join("cover.png");
        std::fs::write(&wrong, PNG_1X1)?;
        sync(&library).await?;
        for audio in [&one, &two, &three] {
            store_cover(
                sole_release_of(&library, audio).await?,
                CoverSource::Local,
                &wrong,
            )
            .await?;
        }

        sync(&library).await?;

        assert_eq!(local_cover_of(&library, &one).await?, None, "dropped");
        assert_eq!(
            local_cover_of(&library, &two).await?,
            Some(cover_path(&parent.join("Two"))),
            "replaced by the image in its own folder"
        );

        store_cover(
            sole_release_of(&library, &three).await?,
            CoverSource::Local,
            &wrong,
        )
        .await?;
        std::fs::remove_dir_all(parent.join("Three"))?;
        sync(&library).await?;
        assert_eq!(
            local_cover_of(&library, &three).await?,
            Some(wrong.to_string_lossy().into_owned()),
            "an unavailable folder is no evidence"
        );
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn refreshing_one_library_keeps_a_shared_release_s_cover_in_another() -> anyhow::Result<()>
    {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (first_root, first) = empty_library("cover-shared-first").await?;
        let second_root = tempfile::tempdir()?.keep();
        let second = {
            let mut db = STATE.db.write().await;
            let path = second_root.to_str().expect("UTF-8 temp path");
            let id = lyra_db::test_db::insert_library(&mut db, "Second", path)?;
            lyra_db::libraries::get_by_id(&*db, id)?.expect("library")
        };
        let kept = album_track(&first_root.join("Album"), "One")?;
        let other = album_track(&second_root.join("Album"), "Two")?;
        std::fs::write(first_root.join("Album/cover.png"), PNG_1X1)?;
        sync(&first).await?;
        sync(&second).await?;
        let (winner, loser) = (
            sole_release_of(&first, &kept).await?,
            sole_release_of(&second, &other).await?,
        );
        STATE.db.write().await.transaction_mut(|t| {
            crate::services::providers::merge_release_into_inside_tx(t, winner, loser)
        })?;
        assert_eq!(sole_release_of(&second, &other).await?, winner);
        let expected = Some(cover_path(&first_root.join("Album")));
        assert_eq!(local_cover_of(&first, &kept).await?, expected);

        refresh_release_covers(&second).await?;
        assert_eq!(
            local_cover_of(&first, &kept).await?,
            expected,
            "the refresh"
        );

        shift_mtime(&other, 10)?;
        sync(&second).await?;
        assert_eq!(local_cover_of(&first, &kept).await?, expected, "the scan");
        let _ = std::fs::remove_dir_all(first_root);
        let _ = std::fs::remove_dir_all(second_root);
        Ok(())
    }

    /// Serves a decodable image to every request until aborted, and returns its URL.
    async fn cover_server() -> anyhow::Result<(String, tokio::task::JoinHandle<()>)> {
        use tokio::io::{
            AsyncReadExt,
            AsyncWriteExt,
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/cover.png", listener.local_addr()?);
        let mut png = std::io::Cursor::new(Vec::new());
        image::RgbImage::from_pixel(2, 2, image::Rgb([200, 0, 0]))
            .write_to(&mut png, image::ImageFormat::Png)?;
        let png = png.into_inner();
        let server = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut request = vec![0; 4096];
                let _ = socket.read(&mut request).await;
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    png.len()
                );
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(&png).await;
            }
        });
        Ok((url, server))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_rejected_local_cover_makes_way_for_the_provider_s_in_one_pass() -> anyhow::Result<()>
    {
        use crate::services::{
            providers::{
                ProviderCoverSpec,
                provider_registry,
            },
            testing::{
                FakePluginDispatch,
                FakeResponse,
            },
        };
        use lyra_db::covers::CoverSource;

        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("cover-provider-asked").await?;
        let (one, two, manual) = (
            root.join("Mix/one.flac"),
            root.join("Mix/two.flac"),
            root.join("Mix/three.flac"),
        );
        tagged_file(&one, &("One", 1, None, None))?;
        tagged_file(&two, &("Two", 1, None, None))?;
        tagged_file(&manual, &("Three", 1, None, None))?;
        let wrong = root.join("Mix/cover.png");
        std::fs::write(&wrong, PNG_1X1)?;
        sync(&library).await?;
        let (first, second, third) = (
            sole_release_of(&library, &one).await?,
            sole_release_of(&library, &two).await?,
            sole_release_of(&library, &manual).await?,
        );
        let elsewhere = tempfile::tempdir()?;
        let manual_file = elsewhere.path().join("manual.png");
        std::fs::write(&manual_file, PNG_1X1)?;
        store_cover(third, CoverSource::Manual, &manual_file).await?;
        for release in [first, second, third] {
            store_cover(release, CoverSource::Local, &wrong).await?;
        }

        let (url, server) = cover_server().await?;
        harmony_http::set_plugin_hosts(
            "release_covers_plugin",
            [harmony_http::HostEntry::Host("127.0.0.1".into())],
        );
        let dispatch = FakePluginDispatch::install();
        let handler = dispatch
            .register_artist_cover_handler(
                "release_covers",
                FakeResponse::Value(serde_json::json!({ "url": url })),
            )
            .await?;
        provider_registry().write().await.set_cover_handler(
            "release_covers",
            crate::services::EntityType::Release,
            ProviderCoverSpec {
                timeout: Duration::from_secs(5),
                require: Default::default(),
                handler,
            },
        );
        let provider_cover = async |release: DbId| -> anyhow::Result<bool> {
            let db = STATE.db.read().await;
            Ok(lyra_db::covers::get_source(&*db, release, CoverSource::Provider)?.is_some())
        };

        let covers_root = configured_covers_root();
        let paths = CoverPaths {
            library_root: Some(&library.path),
            covers_root: &covers_root,
        };
        let (found, tracks, folders) = {
            let db = STATE.db.read().await;
            (
                lyra_db::releases::get_by_id(&db, first)?.expect("release"),
                lyra_db::tracks::get_direct(&db, first)?,
                Arc::new(AudioFolders::of_release(&*db, first)?),
            )
        };
        let synced = sync_release_cover_for_tracks(
            &crate::services::plugin_dispatch::Published::interactive(),
            &STATE.db.get(),
            &folders,
            ReleaseCoverSubject {
                release: &found,
                tracks: &tracks,
                artists: &[],
            },
            paths,
            CoverSyncOptions::default(),
        )
        .await?;
        assert!(synced, "the single release's cover is downloaded");
        assert!(provider_cover(first).await?);

        refresh_release_covers(&library).await?;
        server.abort();
        assert!(provider_cover(second).await?, "the whole-library pass");
        assert_eq!(
            local_cover_of(&library, &two).await?,
            None,
            "dropped in the same pass, leaving no gap"
        );
        assert!(
            !provider_cover(third).await?,
            "a manual cover still counts as having one"
        );
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_cover_link_to_something_else_is_not_registered() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("cover-link").await?;
        let library = follow_links(&library).await?;
        let audio = album_track(&root.join("Album"), "Album")?;
        let outside = tempfile::tempdir()?;
        let link = root.join("Album/cover.jpg");
        let unchanged = root.join("Album/unchanged.flac");
        for name in ["config.json", "private.flac"] {
            let target = outside.path().join(name);
            std::fs::write(&target, "secret")?;
            let _ = std::fs::remove_file(&link);
            std::os::unix::fs::symlink(&target, &link)?;
            std::fs::copy(&audio, &unchanged)?;

            sync(&library).await?;

            assert_eq!(local_cover_of(&library, &audio).await?, None, "{name}");
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_followed_folder_gives_its_tracks_and_cover_lexical_paths() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("followed-folder").await?;
        let outside = tempfile::tempdir()?;
        album_track(outside.path(), "Elsewhere")?;
        std::fs::write(outside.path().join("cover.png"), PNG_1X1)?;
        let linked = root.join("Linked");
        std::os::unix::fs::symlink(outside.path(), &linked)?;
        let audio = linked.join("track.flac");

        sync(&library).await?;
        assert!(entry_for(&library, &audio).await.is_err(), "off by default");

        let library = follow_links(&library).await?;
        sync(&library).await?;

        assert!(!entry_for(&library, &audio).await?.is_missing());
        assert_eq!(
            local_cover_of(&library, &audio).await?,
            Some(linked.join("cover.png").to_string_lossy().into_owned())
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_emptied_root_reports_a_broken_link_it_follows_as_a_failure() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("broken-followed").await?;
        let library = follow_links(&library).await?;
        let audio = album_track(&root.join("Album"), "Album")?;
        sync(&library).await?;
        std::fs::remove_dir_all(root.join("Album"))?;
        std::os::unix::fs::symlink(root.join("nothing"), root.join("broken.flac"))?;

        let progress = super::super::orchestrator::begin_test_run(&library).await;
        let result = sync_library_pipeline(
            &STATE.db.get(),
            &library,
            LibrarySyncOptions::default(),
            progress.clone(),
        )
        .await;

        assert!(result.is_err());
        let run = super::super::orchestrator::get_sync_run(progress.run_id())
            .await?
            .expect("run recorded");
        assert_eq!(run.failure_count, 1);
        assert!(!entry_for(&library, &audio).await?.is_missing());
        super::super::orchestrator::end_test_run(&progress).await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_emptied_root_reports_a_broken_link_and_marks_nothing_missing() -> anyhow::Result<()>
    {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("broken-link-only").await?;
        let audio = album_track(&root.join("Album"), "Album")?;
        sync(&library).await?;
        std::fs::remove_dir_all(root.join("Album"))?;
        std::os::unix::fs::symlink(root.join("nothing"), root.join("broken.flac"))?;

        let progress = super::super::orchestrator::begin_test_run(&library).await;
        let result = sync_library_pipeline(
            &STATE.db.get(),
            &library,
            LibrarySyncOptions::default(),
            progress.clone(),
        )
        .await;

        let err = result.expect_err("the emptied root fails the sync");
        assert!(matches!(
            err.downcast_ref::<ScanError>(),
            Some(ScanError::EmptyRoot { .. })
        ));
        let run = super::super::orchestrator::get_sync_run(progress.run_id())
            .await?
            .expect("run recorded");
        assert_eq!(run.progress.skipped_units, 1);
        assert_eq!(run.failure_count, 0, "a skipped link is no failure");
        assert!(!entry_for(&library, &audio).await?.is_missing());
        super::super::orchestrator::end_test_run(&progress).await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unlistable_cover_folder_keeps_the_other_covers() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("unlistable-cover").await?;
        let readable = album_track(&root.join("Readable"), "Readable")?;
        album_track(&root.join("Hidden"), "Hidden")?;
        sync(&library).await?;
        std::fs::write(root.join("Readable/cover.png"), PNG_1X1)?;
        let hidden = root.join("Hidden");
        std::fs::set_permissions(&hidden, std::fs::Permissions::from_mode(0o000))?;
        if std::fs::read_dir(&hidden).is_ok() {
            std::fs::set_permissions(&hidden, std::fs::Permissions::from_mode(0o755))?;
            return Ok(());
        }

        let covers_root = configured_covers_root();
        let result = crate::services::covers::sync_and_persist_release_covers_for_library(
            &STATE.db.get(),
            CoverPaths {
                library_root: Some(&library.path),
                covers_root: &covers_root,
            },
            library.db_id.expect("library db_id"),
            CoverSyncOptions::default(),
            None,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await;
        std::fs::set_permissions(&hidden, std::fs::Permissions::from_mode(0o755))?;

        let err = result.expect_err("the unlistable folder went unreported");
        assert!(
            format!("{err:#}").contains("failed to list covers"),
            "{err:#}"
        );
        let entry = entry_for(&library, &readable)
            .await?
            .db_id
            .expect("entry db_id");
        assert!(
            release_cover(entry).await?.is_some(),
            "the readable cover was dropped"
        );
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_missing_cue_sheet_keeps_its_segments_unavailable() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("cue-delete").await?;
        let audio = album_track(&root.join("Album"), "Album")?;
        let audio_named = root.join("Album/album.flac");
        std::fs::rename(&audio, &audio_named)?;
        let cue = root.join("Album/album.cue");
        std::fs::write(&cue, CUE_SHEET)?;
        sync(&library).await?;
        assert_eq!(snapshot(&library).await?[&audio_named].1.len(), 2);

        let segments = snapshot(&library).await?[&audio_named].1.clone();

        std::fs::remove_file(&cue)?;
        sync(&library).await?;

        // The segments stay, unavailable, beside the whole file read again on its own.
        let tracks = snapshot(&library).await?;
        assert!(
            tracks.contains_key(&cue),
            "the missing cue's entry was removed"
        );
        assert_eq!(tracks[&audio_named].1.len(), 3);
        assert!(synced(&library, &audio_named).await?);
        let available = |db: &agdb::DbAny, id: &str| -> anyhow::Result<bool> {
            let track_db_id = lyra_db::lookup::find_node_id_by_id(db, id)?.expect("track");
            Ok(crate::services::catalog::Availability::of(db)?.track(track_db_id))
        };
        {
            let db = STATE.db.read().await;
            for segment in &segments {
                assert!(
                    !available(&db, segment)?,
                    "a segment of a missing cue plays"
                );
            }
        }

        // The cue sheet back, its segments play again and the whole file goes.
        std::fs::write(&cue, CUE_SHEET)?;
        sync(&library).await?;
        let tracks = snapshot(&library).await?;
        assert_eq!(tracks[&audio_named].1, segments);
        let db = STATE.db.read().await;
        for segment in &segments {
            assert!(available(&db, segment)?, "a revived segment can't play");
        }
        drop(db);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_unreadable_cue_sheet_keeps_its_segments_when_its_audio_changes()
    -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("cue-unreadable").await?;
        let audio = album_track(&root.join("Album"), "Album")?;
        let audio_named = root.join("Album/album.flac");
        std::fs::rename(&audio, &audio_named)?;
        let cue = root.join("Album/album.cue");
        std::fs::write(&cue, CUE_SHEET)?;
        sync(&library).await?;
        let segments = snapshot(&library).await?[&audio_named].1.clone();
        assert_eq!(segments.len(), 2);

        std::fs::set_permissions(&cue, std::fs::Permissions::from_mode(0o000))?;
        if std::fs::read(&cue).is_ok() {
            // Running with privileges that ignore permissions.
            let _ = std::fs::remove_dir_all(root);
            return Ok(());
        }
        shift_mtime(&audio_named, 5)?;
        let result = sync(&library).await;
        std::fs::set_permissions(&cue, std::fs::Permissions::from_mode(0o644))?;
        result?;

        assert_eq!(
            snapshot(&library).await?[&audio_named].1,
            segments,
            "the audio replaced its segments while its cue sheet couldn't be read"
        );
        assert!(!synced(&library, &audio_named).await?);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test]
    async fn directories_in_later_batches_find_their_parents() -> anyhow::Result<()> {
        let db = DbAsync::new(lyra_db::test_db::new_test_db()?);
        let library = {
            let mut db_write = db.write().await;
            lyra_db::test_db::insert_test_library_node(
                &mut db_write,
                "Batches",
                PathBuf::from("/music"),
            )?
        };
        let dir = |path: PathBuf| Entry {
            db_id: None,
            id: lyra_db::nanoid(),
            name: path
                .file_name()
                .expect("name")
                .to_string_lossy()
                .into_owned(),
            full_path: path,
            kind: EntryKind::Dir,
            file_kind: None,
            hash: None,
            size: 0,
            file_modified_at_ms: 1,
            file_created_at_ms: 1,
            synced_at_ms: None,
            missing_at_ms: None,
        };
        let artists = (0..DIR_BATCH + 1)
            .map(|index| PathBuf::from(format!("/music/{index}")))
            .collect::<Vec<_>>();
        let albums = artists.iter().map(|artist| artist.join("album"));
        let dirs = artists
            .iter()
            .cloned()
            .chain(albums)
            .map(dir)
            .collect::<Vec<_>>();

        let mut stored = StoredEntries::new(&library, Vec::new())?;
        sync_entry_dirs(&db, &mut stored, dirs).await?;

        let db_read = db.read().await;
        let library_db_id = library.db_id.expect("library db_id");
        let entries = lyra_db::entries::load_existing(&*db_read, library_db_id)?;
        assert_eq!(entries.len(), 2 * artists.len());
        assert_eq!(
            lyra_db::entries::children(&*db_read, library_db_id)?.len(),
            artists.len()
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_scan_finding_nothing_changed_writes_nothing() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("nothing-changed").await?;
        album_track(&root.join("Artist/One"), "One")?;
        album_track(&root.join("Artist/Two"), "Two")?;
        let db = STATE.db.get();
        sync_entries_incremental(&db, &library, SyncRunProgress::unreported_for_test()).await?;
        let stamp = db.stamp();

        sync_entries_incremental(&db, &library, SyncRunProgress::unreported_for_test()).await?;

        assert_eq!(db.stamp(), stamp);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_moved_file_keeps_its_entry_and_track() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("move").await?;
        let audio = album_track(&root.join("Album"), "Album")?;
        sync(&library).await?;
        let before = snapshot(&library).await?[&audio].clone();
        assert_eq!(before.1.len(), 1);

        let moved = root.join("Elsewhere/Deeper/renamed.flac");
        std::fs::create_dir_all(moved.parent().expect("parent"))?;
        std::fs::rename(&audio, &moved)?;
        std::fs::remove_dir(root.join("Album"))?;
        sync(&library).await?;

        let after = snapshot(&library).await?;
        assert_eq!(
            after[&moved], before,
            "the moved file kept neither its entry nor its track"
        );
        assert!(!after.contains_key(&audio));
        assert!(
            !after.contains_key(&root.join("Album")),
            "the emptied folder stayed as a tombstone"
        );
        let db = STATE.db.read().await;
        let entry = lyra_db::lookup::find_node_id_by_id(&*db, &after[&moved].0)?.expect("entry");
        let parent = lyra_db::entries::parent_id(&*db, entry)?.expect("parent");
        assert_eq!(
            lyra_db::entries::get_by_id(&*db, parent)?.map(|entry| entry.full_path),
            Some(root.join("Elsewhere/Deeper")),
            "the moved entry sits under its new folder"
        );
        drop(db);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_file_found_elsewhere_after_going_missing_is_revived() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("move-later").await?;
        let audio = album_track(&root.join("Album"), "Album")?;
        let kept = album_track(&root.join("Kept"), "Kept")?;
        sync(&library).await?;
        let before = snapshot(&library).await?[&audio].clone();

        let parked = root.with_extension("parked.flac");
        std::fs::rename(&audio, &parked)?;
        sync(&library).await?;
        assert!(entry_for(&library, &audio).await?.is_missing());

        let moved = root.join("Later/track.flac");
        std::fs::create_dir_all(root.join("Later"))?;
        std::fs::rename(&parked, &moved)?;
        sync(&library).await?;
        let after = snapshot(&library).await?;
        assert_eq!(after[&moved], before);
        assert!(!entry_for(&library, &moved).await?.is_missing());
        assert!(after.contains_key(&kept));
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn identical_copies_take_over_no_entry() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("move-ambiguous").await?;
        let audio = album_track(&root.join("Album"), "Album")?;
        sync(&library).await?;
        let before = snapshot(&library).await?[&audio].clone();

        let copies = [root.join("One/track.flac"), root.join("Two/track.flac")];
        for copy in &copies {
            std::fs::create_dir_all(copy.parent().expect("parent"))?;
            std::fs::copy(&audio, copy)?;
        }
        std::fs::remove_file(&audio)?;
        sync(&library).await?;

        let after = snapshot(&library).await?;
        assert_eq!(after[&audio], before, "the original stays, missing");
        assert!(entry_for(&library, &audio).await?.is_missing());
        for copy in &copies {
            assert_ne!(
                after[copy].0, before.0,
                "an ambiguous copy took over the entry"
            );
        }
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_cue_sheet_moved_with_its_audio_keeps_its_segments() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("move-cue").await?;
        let audio = album_track(&root.join("Album"), "Album")?;
        let audio_named = root.join("Album/album.flac");
        std::fs::rename(&audio, &audio_named)?;
        std::fs::write(root.join("Album/album.cue"), CUE_SHEET)?;
        sync(&library).await?;
        let segments = snapshot(&library).await?[&audio_named].1.clone();
        assert_eq!(segments.len(), 2);

        std::fs::rename(root.join("Album"), root.join("Moved"))?;
        sync(&library).await?;
        assert_eq!(
            snapshot(&library).await?[&root.join("Moved/album.flac")].1,
            segments
        );
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn audio_moved_away_from_its_cue_sheet_keeps_its_segments() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("move-cue-audio").await?;
        let audio = album_track(&root.join("Album"), "Album")?;
        let audio_named = root.join("Album/album.flac");
        std::fs::rename(&audio, &audio_named)?;
        std::fs::write(root.join("Album/album.cue"), CUE_SHEET)?;
        sync(&library).await?;
        let segments = snapshot(&library).await?[&audio_named].1.clone();
        assert_eq!(segments.len(), 2);

        let moved = root.join("Audio/renamed.flac");
        std::fs::create_dir_all(root.join("Audio"))?;
        std::fs::rename(&audio_named, &moved)?;
        sync(&library).await?;
        assert_eq!(
            snapshot(&library).await?[&moved].1,
            segments,
            "the cue sheet's segments didn't follow their audio"
        );
        let db = STATE.db.read().await;
        let availability = crate::services::catalog::Availability::of(&db)?;
        for segment in &segments {
            let id = lyra_db::lookup::find_node_id_by_id(&*db, segment)?.expect("segment");
            assert!(
                availability.track(id),
                "a segment of the moved audio can't play"
            );
        }
        drop(db);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    /// A library of the albums Gone and Kept, a track each, after Gone's file went missing; the
    /// root, the library, and the tracks' ids.
    async fn library_with_a_missing_album(
        name: &str,
    ) -> anyhow::Result<(PathBuf, Library, DbId, DbId)> {
        let (root, library) = empty_library(name).await?;
        let gone = album_track(&root.join("Gone"), "Gone")?;
        let kept = album_track(&root.join("Kept"), "Kept")?;
        sync(&library).await?;
        std::fs::remove_dir_all(root.join("Gone"))?;
        sync(&library).await?;
        let mut tracks = Vec::new();
        for path in [gone, kept] {
            let entry = entry_for(&library, &path).await?;
            let db = STATE.db.read().await;
            tracks.push(
                lyra_db::tracks::get_by_entry(&db, entry.db_id.expect("entry db_id"))?
                    .into_iter()
                    .next()
                    .and_then(|track| track.db_id.map(DbId::from))
                    .ok_or_else(|| anyhow::anyhow!("no track for {}", path.display()))?,
            );
        }
        let (gone_track, kept_track) = (tracks[0], tracks[1]);
        Ok((root, library, gone_track, kept_track))
    }

    async fn track_exists(track: DbId) -> anyhow::Result<bool> {
        Ok(lyra_db::tracks::get_by_id(&*STATE.db.read().await, track)?.is_some())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn purging_removes_missing_tracks_their_files_and_emptied_releases() -> anyhow::Result<()>
    {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library, gone, kept) = library_with_a_missing_album("purge-all").await?;
        let gone_release = {
            let db = STATE.db.read().await;
            lyra_db::releases::get_by_track(&db, gone)?[0]
                .db_id
                .clone()
                .map(DbId::from)
                .expect("release")
        };

        let purged = super::super::purge::purge(
            &STATE.db.get(),
            &library,
            &lyra_db::entries::PurgeScope::All,
            &SyncRunProgress::unreported_for_test(),
        )
        .await?;
        assert_eq!(purged, 1);
        assert!(!track_exists(gone).await?);
        assert!(track_exists(kept).await?);
        let db = STATE.db.read().await;
        assert!(lyra_db::releases::get_by_id(&*db, gone_release)?.is_none());
        let entries = lyra_db::entries::load_existing(&*db, library.db_id.expect("library db_id"))?;
        assert!(entries.iter().all(|entry| !entry.is_missing()));
        assert!(lyra_db::entries::missing_ids(&*db)?.is_empty());
        drop(db);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_purge_of_chosen_tracks_takes_only_unavailable_ones() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library, gone, kept) = library_with_a_missing_album("purge-chosen").await?;
        {
            let db = STATE.db.read().await;
            let refused = lyra_db::entries::missing_purge_plan(
                &db,
                &library,
                &lyra_db::entries::PurgeScope::Tracks(vec![gone, kept]),
            );
            assert!(matches!(
                refused,
                Err(lyra_db::entries::PurgePlanError::NotMissing(_))
            ));
        }
        super::super::purge::purge(
            &STATE.db.get(),
            &library,
            &lyra_db::entries::PurgeScope::Tracks(vec![gone]),
            &SyncRunProgress::unreported_for_test(),
        )
        .await?;
        assert!(!track_exists(gone).await?);
        assert!(track_exists(kept).await?);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_sync_purges_files_missing_longer_than_the_library_keeps_them() -> anyhow::Result<()>
    {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, mut library, gone, kept) = library_with_a_missing_album("purge-expired").await?;
        library.purge_missing_after_days = Some(2);

        sync(&library).await?;
        assert!(
            track_exists(gone).await?,
            "a file missing for less was purged"
        );

        let gone_entry = {
            let db = STATE.db.read().await;
            lyra_db::entries::missing_ids(&*db)?
        };
        let three_days_ago = lyra_db::users::now_ms().unsigned_abs() - 3 * 86_400_000;
        STATE.db.write().await.exec_mut(
            agdb::QueryBuilder::insert()
                .values_uniform([("missing_at_ms", three_days_ago).into()])
                .ids(&gone_entry)
                .query(),
        )?;
        sync(&library).await?;
        assert!(!track_exists(gone).await?, "an expired file's track stayed");
        assert!(track_exists(kept).await?);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_remapped_missing_track_takes_over_the_new_file() -> anyhow::Result<()> {
        use crate::services::metadata::remap::{
            RemapError,
            remap,
        };

        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library, missing, _) = library_with_a_missing_album("remap").await?;
        // The same song, retagged and moved before the sync, reads as a new track.
        let retagged = album_track(&root.join("New"), "New")?;
        sync(&library).await?;
        let target_entry = entry_for(&library, &retagged).await?;
        let (target, missing_id, target_id, admin, other, playlist) = {
            let mut db = STATE.db.write().await;
            let target = lyra_db::tracks::get_by_entry(&db, target_entry.db_id.expect("db id"))?[0]
                .db_id
                .clone()
                .map(DbId::from)
                .expect("target");
            let id = |db: &agdb::DbAny, track| {
                lyra_db::lookup::find_id_by_db_id(db, track).map(|id| id.expect("public id"))
            };
            let missing_id = id(&db, missing)?;
            let target_id = id(&db, target)?;
            let admin = lyra_db::test_db::insert_user(&mut db, "remap-admin")?;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            lyra_db::roles::ensure_user_has_role(
                &mut db,
                admin,
                lyra_db::roles::BUILTIN_ADMIN_ROLE,
            )?;
            let other = lyra_db::test_db::insert_user(&mut db, "remap-other")?;
            let kind = lyra_db::favorites::FavoriteKind::Track;
            lyra_db::favorites::add(&mut *db, admin, missing, kind, 10)?;
            lyra_db::favorites::add(&mut *db, other, target, kind, 20)?;
            lyra_db::favorites::add(&mut *db, admin, target, kind, 5)?;
            let playlist = lyra_db::playlists::create(
                &mut db,
                &lyra_db::playlists::Playlist {
                    db_id: None,
                    id: lyra_db::nanoid(),
                    name: "Both".to_string(),
                    description: None,
                    is_public: None,
                    created_at_ms: None,
                    updated_at_ms: None,
                },
                admin,
            )?;
            lyra_db::playlists::add_tracks(&mut *db, playlist, &[missing, target])?;
            let principal = crate::services::auth::Principal::for_user(
                &*db,
                admin,
                vec![lyra_db::Permission::Admin],
                std::collections::HashSet::new(),
            );
            (target, missing_id, target_id, principal, other, playlist)
        };

        assert!(matches!(
            remap(&admin, &target_id, &missing_id).await,
            Err(RemapError::Conflict(_))
        ));
        assert!(matches!(
            remap(&admin, &missing_id, &missing_id).await,
            Err(RemapError::Conflict(_))
        ));
        remap(&admin, &missing_id, &target_id).await?;

        let db = STATE.db.read().await;
        assert!(
            lyra_db::tracks::get_by_id(&*db, target)?.is_none(),
            "the target stayed"
        );
        assert_eq!(
            lyra_db::lookup::find_node_id_by_id(&*db, &missing_id)?,
            Some(missing),
            "the missing track lost its id"
        );
        assert!(lyra_db::track_sources::track_available(&*db, missing)?);
        assert_eq!(
            lyra_db::entries::get_by_track(&db, missing)?
                .into_iter()
                .map(|entry| entry.full_path)
                .collect::<Vec<_>>(),
            vec![retagged.clone()]
        );
        let release = lyra_db::releases::get_by_track(&db, missing)?;
        assert_eq!(release.len(), 1);
        assert_eq!(release[0].release_title, "New");
        assert!(lyra_db::favorites::has(&*db, other, missing)?);
        assert_eq!(
            lyra_db::favorites::first_favorited_at(
                &*db,
                admin.require(&db)?,
                lyra_db::favorites::FavoriteKind::Track,
            )?
            .get(&missing)
            .copied(),
            Some(5),
            "a user favoriting both keeps the earlier time"
        );
        assert_eq!(
            lyra_db::graph::direct_edge_ids(&*db, playlist, missing)?.len(),
            2,
            "a playlist holding both holds the remapped track twice"
        );
        drop(db);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_cue_sheet_in_a_legacy_encoding_keeps_its_segments() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("cue-latin1").await?;
        let audio = album_track(&root.join("Album"), "Album")?;
        let audio_named = root.join("Album/album.flac");
        std::fs::rename(&audio, &audio_named)?;
        let cue = root.join("Album/album.cue");
        std::fs::write(&cue, CUE_SHEET)?;
        sync(&library).await?;
        let segments = snapshot(&library).await?[&audio_named].1.clone();
        assert_eq!(segments.len(), 2);

        // Rewritten by a tool that saves Latin-1: no longer valid UTF-8.
        let mut latin1 = CUE_SHEET.replace("Cue Track 1", "Caf\u{e9}").into_bytes();
        let accent = latin1
            .windows(2)
            .position(|pair| pair == [0xc3, 0xa9])
            .expect("accent");
        latin1.splice(accent..accent + 2, [0xe9]);
        std::fs::write(&cue, latin1)?;
        shift_mtime(&audio_named, 5)?;
        sync(&library).await?;
        assert_eq!(
            snapshot(&library).await?[&audio_named].1,
            segments,
            "the audio replaced its segments when its cue sheet failed to decode"
        );
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cue_sheet_claims_audio_in_another_folder() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("cue-elsewhere").await?;
        let audio = root.join("Audio/album.flac");
        std::fs::create_dir_all(root.join("Audio"))?;
        std::fs::copy(fixture_flac(), &audio)?;
        std::fs::create_dir_all(root.join("Artist/Album"))?;
        std::fs::write(
            root.join("Artist/Album/album.cue"),
            CUE_SHEET.replace("\"album.flac\"", "\"../../Audio/album.flac\""),
        )?;
        sync(&library).await?;
        assert_eq!(snapshot(&library).await?[&audio].1.len(), 2);

        shift_mtime(&audio, 5)?;
        sync(&library).await?;
        assert_eq!(
            snapshot(&library).await?[&audio].1.len(),
            2,
            "the claimed audio was ingested on its own"
        );
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn retag_inside_a_nested_album_is_ingested() -> anyhow::Result<()> {
        use lofty::{
            config::WriteOptions,
            prelude::*,
        };

        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("nested-retag").await?;
        let audio = album_track(&root.join("Artist/Album"), "Album")?;
        sync(&library).await?;

        let mut tagged = lofty::read_from_path(&audio)?;
        let tag = tagged
            .primary_tag_mut()
            .ok_or_else(|| anyhow::anyhow!("fixture has no primary tag"))?;
        tag.set_title("Retagged Title".to_string());
        tag.save_to_path(&audio, WriteOptions::default())?;
        shift_mtime(&audio, 5)?;
        sync(&library).await?;

        let entry = entry_for(&library, &audio)
            .await?
            .db_id
            .expect("entry db_id");
        let titles = lyra_db::tracks::get_by_entry(&*STATE.db.read().await, entry)?
            .into_iter()
            .map(|track| track.track_title)
            .collect::<Vec<_>>();
        assert_eq!(titles, ["Retagged Title"]);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unreadable_track_is_retried_once_readable() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("unreadable-track").await?;
        let audio = album_track(&root.join("Album"), "Album")?;
        if !deny_reads(&audio)? {
            return Ok(());
        }

        let progress = super::super::orchestrator::begin_test_run(&library).await;
        sync_library_pipeline(
            &STATE.db.get(),
            &library,
            LibrarySyncOptions::default(),
            progress.clone(),
        )
        .await?;
        let run = super::super::orchestrator::get_sync_run(progress.run_id())
            .await?
            .expect("run");
        super::super::orchestrator::end_test_run(&progress).await;
        assert_eq!(
            run.progress.failed_units, 1,
            "the unreadable track went unreported"
        );
        assert!(snapshot(&library).await?[&audio].1.is_empty());
        assert!(!synced(&library, &audio).await?);

        allow_reads(&audio)?;
        sync(&library).await?;
        assert_eq!(snapshot(&library).await?[&audio].1.len(), 1);
        assert!(synced(&library, &audio).await?);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_resync_forgets_a_deleted_cover_of_an_unchanged_album() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("deleted-cover").await?;
        let audio = album_track(&root.join("Album"), "Album")?;
        let cover = root.join("Album/cover.png");
        std::fs::write(&cover, PNG_1X1)?;

        sync(&library).await?;
        let entry = entry_for(&library, &audio)
            .await?
            .db_id
            .expect("entry db_id");
        assert!(release_cover(entry).await?.is_some());

        std::fs::remove_file(&cover)?;
        sync(&library).await?;
        assert!(release_cover(entry).await?.is_none());
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_resync_finds_the_new_cover_of_an_unchanged_album() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("replaced-cover").await?;
        let audio = album_track(&root.join("Album"), "Album")?;
        std::fs::write(root.join("Album/cover.png"), PNG_1X1)?;

        sync(&library).await?;
        let entry = entry_for(&library, &audio)
            .await?
            .db_id
            .expect("entry db_id");
        std::fs::remove_file(root.join("Album/cover.png"))?;
        let replacement = root.join("Album/cover.jpg");
        std::fs::write(&replacement, PNG_1X1)?;
        sync(&library).await?;

        let found = release_cover(entry).await?.map(|cover| cover.path);
        assert_eq!(found, Some(replacement.to_string_lossy().into_owned()));
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unpersisted_local_cover_is_retried() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("unpersisted-cover").await?;
        let audio = album_track(&root.join("Album"), "Album")?;
        let cover = root.join("Album/cover.png");
        std::fs::write(&cover, PNG_1X1)?;
        if !deny_reads(&cover)? {
            return Ok(());
        }

        sync(&library).await?;
        let entry = entry_for(&library, &audio).await?;
        assert_eq!(snapshot(&library).await?[&audio].1.len(), 1);
        assert!(
            release_cover(entry.db_id.expect("entry db_id"))
                .await?
                .is_none()
        );
        assert!(entry.synced_at_ms.is_none());

        allow_reads(&cover)?;
        sync(&library).await?;
        let entry = entry_for(&library, &audio).await?;
        assert!(
            release_cover(entry.db_id.expect("entry db_id"))
                .await?
                .is_some()
        );
        assert!(entry.synced_at_ms.is_some());
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cancelling_stops_the_library_cover_passes() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("cancelled-covers").await?;
        album_track(&root.join("Album"), "Album")?;

        let progress = super::super::orchestrator::begin_test_run(&library).await;
        // The pass cancels the run, then never finishes.
        let gate: Gate<PathBuf> = {
            let run_id = progress.run_id().to_string();
            std::sync::Arc::new(move |_| {
                let run_id = run_id.clone();
                Box::pin(async move {
                    super::super::orchestrator::cancel_sync_run(&run_id)
                        .await
                        .expect("cancel");
                    std::future::pending::<()>().await;
                })
            })
        };
        let hooks = Hooks {
            before_library_covers: Some(gate),
            ..Hooks::default()
        };
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            sync_with(hooks.into(), &library, progress.clone()),
        )
        .await
        .expect("the cancelled scan kept waiting for its cover pass");
        super::super::orchestrator::end_test_run(&progress).await;

        let err = result.expect_err("the run finished despite its cancellation");
        assert!(
            err.downcast_ref::<SyncRunControlError>().is_some(),
            "{err:#}"
        );
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cancelled_sync_keeps_finished_albums_synced() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("cancelled-sync").await?;
        let first = album_track(&root.join("First"), "First")?;
        let second = album_track(&root.join("Second"), "Second")?;
        for album in ["First", "Second"] {
            std::fs::write(root.join(album).join("cover.png"), PNG_1X1)?;
        }

        let progress = super::super::orchestrator::begin_test_run(&library).await;
        let gate: ArtifactGate = {
            let (library, first, run_id) = (
                library.clone(),
                first.clone(),
                progress.run_id().to_string(),
            );
            std::sync::Arc::new(move |title: Option<String>| {
                let (library, first, run_id) = (library.clone(), first.clone(), run_id.clone());
                Box::pin(async move {
                    if title.as_deref() != Some("Second") {
                        return;
                    }
                    while !synced(&library, &first).await.unwrap_or(false) {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    super::super::orchestrator::cancel_sync_run(&run_id)
                        .await
                        .expect("cancel");
                })
            })
        };
        let hooks = Hooks {
            before_artifacts: Some(gate),
            ..Hooks::default()
        };
        let result = sync_with(hooks.into(), &library, progress.clone()).await;
        super::super::orchestrator::end_test_run(&progress).await;

        let err = result.expect_err("the run finished despite its cancellation");
        assert!(
            err.downcast_ref::<SyncRunControlError>().is_some(),
            "{err:#}"
        );
        let first_entry = entry_for(&library, &first).await?;
        assert!(
            first_entry.synced_at_ms.is_some(),
            "the finished album lost its sync"
        );
        assert!(
            release_cover(first_entry.db_id.expect("entry db_id"))
                .await?
                .is_some()
        );
        assert!(
            !synced(&library, &second).await?,
            "the cancelled album counted as synced"
        );

        sync(&library).await?;
        assert!(synced(&library, &second).await?);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    /// How the group of a late file stops a scan once another group stored a move.
    #[derive(Clone, Copy, PartialEq)]
    enum Interruption {
        /// Cancels the run before the group hashes its files.
        CancelBeforeHash,
        /// Cancels the run once the group's files are hashed, before it applies them.
        CancelAfterHash,
        /// Fails the group once its files are hashed.
        Fail,
    }

    /// Scans a library where one file of a two-file album moved and a new file arrived, the new
    /// file's group stopping the scan as `interruption` says once the move is stored. Checks what
    /// any stopped scan must leave, and that the next scan finishes the job; returns the error.
    async fn scan_interrupted_after_a_move(
        name: &str,
        interruption: Interruption,
    ) -> anyhow::Result<anyhow::Error> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library(name).await?;
        let (album, moved_dir, late_dir) =
            (root.join("Album"), root.join("Moved"), root.join("Late"));
        let left = album.join("2.flac");
        album_file(&album.join("1.flac"), "Album", 1)?;
        album_file(&left, "Album", 2)?;
        sync(&library).await?;
        assert!(synced(&library, &left).await?);

        let moved = moved_dir.join("1.flac");
        std::fs::create_dir_all(&moved_dir)?;
        std::fs::rename(album.join("1.flac"), &moved)?;
        let late = late_dir.join("1.flac");
        album_file(&late, "Late", 1)?;

        let progress = super::super::orchestrator::begin_test_run(&library).await;
        let (stored_tx, stored_rx) = tokio::sync::oneshot::channel::<()>();
        let stored_tx = std::sync::Mutex::new(Some(stored_tx));
        let stored_rx = std::sync::Arc::new(tokio::sync::Mutex::new(Some(stored_rx)));
        let applied_dir = moved_dir.clone();
        let entry_applied = std::sync::Arc::new(move |source_dir: PathBuf| {
            if source_dir == applied_dir
                && let Some(stored) = stored_tx.lock().expect("stored").take()
            {
                let _ = stored.send(());
            }
        });
        // The late file's group waits until the move is stored, then stops the scan.
        let interrupt = {
            let run_id = progress.run_id().to_string();
            move || {
                let (run_id, stored_rx) = (run_id.clone(), stored_rx.clone());
                async move {
                    if let Some(stored) = stored_rx.lock().await.take() {
                        stored.await.expect("the move is stored");
                    }
                    if interruption == Interruption::Fail {
                        anyhow::bail!("the late group fails");
                    }
                    super::super::orchestrator::cancel_sync_run(&run_id)
                        .await
                        .expect("cancel");
                    Ok(())
                }
            }
        };
        let mut hooks = Hooks {
            entry_applied: Some(entry_applied),
            ..Hooks::default()
        };
        if interruption == Interruption::CancelBeforeHash {
            let (interrupt, late_dir) = (interrupt.clone(), late_dir.clone());
            hooks.before_entry_hash = Some(std::sync::Arc::new(move |source_dir: PathBuf| {
                let interrupt = (source_dir == late_dir).then(&interrupt);
                Box::pin(async move {
                    if let Some(interrupt) = interrupt {
                        interrupt.await.expect("cancels");
                    }
                })
            }));
        } else {
            let late = late.clone();
            hooks.after_entry_hash = Some(std::sync::Arc::new(move |files: Vec<PathBuf>| {
                let interrupt = files.contains(&late).then(&interrupt);
                Box::pin(async move {
                    match interrupt {
                        Some(interrupt) => interrupt.await,
                        None => Ok(()),
                    }
                })
            }));
        }
        let result = tokio::time::timeout(
            Duration::from_secs(60),
            sync_with(hooks.into(), &library, progress.clone()),
        )
        .await?;
        super::super::orchestrator::end_test_run(&progress).await;

        let err = result.expect_err("the run finished despite its interruption");
        let snapshot = snapshot(&library).await?;
        assert!(snapshot.contains_key(&moved), "the move was stored");
        assert!(
            !snapshot.contains_key(&late),
            "the interrupted group was applied"
        );
        assert!(
            !synced(&library, &left).await?,
            "the album the stored move left keeps its sync"
        );
        assert!(
            !entry_for(&library, &left).await?.is_missing(),
            "a partial scan marked a file missing"
        );

        sync(&library).await?;
        assert!(synced(&library, &left).await?);
        let _ = std::fs::remove_dir_all(root);
        Ok(err)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_scan_cancelled_after_storing_a_move_unsyncs_the_album_it_left() -> anyhow::Result<()>
    {
        let err =
            scan_interrupted_after_a_move("cancelled-move", Interruption::CancelBeforeHash).await?;
        assert!(
            err.downcast_ref::<SyncRunControlError>().is_some(),
            "{err:#}"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_group_cancelled_once_hashed_applies_nothing() -> anyhow::Result<()> {
        let err = scan_interrupted_after_a_move("cancelled-hashed", Interruption::CancelAfterHash)
            .await?;
        assert!(
            err.downcast_ref::<SyncRunControlError>().is_some(),
            "{err:#}"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_scan_failing_after_storing_a_move_unsyncs_the_album_it_left() -> anyhow::Result<()> {
        let err = scan_interrupted_after_a_move("failed-move", Interruption::Fail).await?;
        assert!(
            err.downcast_ref::<SyncRunControlError>().is_none(),
            "{err:#}"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_scan_failing_to_unsync_what_its_moves_leave_stores_none_of_them()
    -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("failed-unsync").await?;
        let (album, moved_dir) = (root.join("Album"), root.join("Moved"));
        album_file(&album.join("1.flac"), "Album", 1)?;
        album_file(&album.join("2.flac"), "Album", 2)?;
        sync(&library).await?;

        let moved = moved_dir.join("1.flac");
        std::fs::create_dir_all(&moved_dir)?;
        std::fs::rename(album.join("1.flac"), &moved)?;
        let hooks = Hooks {
            fail_unsync_left_by_moves: true,
            ..Hooks::default()
        };
        sync_with(
            hooks.into(),
            &library,
            SyncRunProgress::unreported_for_test(),
        )
        .await
        .expect_err("the run finished despite the failed write");
        assert!(
            !snapshot(&library).await?.contains_key(&moved),
            "the move was stored"
        );

        let parsed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let gate: Gate<PathBuf> = {
            let parsed = parsed.clone();
            std::sync::Arc::new(move |source_dir| {
                parsed.lock().expect("parsed").push(source_dir);
                Box::pin(async {})
            })
        };
        let hooks = Hooks {
            before_parse: Some(gate),
            ..Hooks::default()
        };
        sync_with(
            hooks.into(),
            &library,
            SyncRunProgress::unreported_for_test(),
        )
        .await?;
        assert!(snapshot(&library).await?.contains_key(&moved));
        assert!(
            parsed.lock().expect("parsed").contains(&album),
            "the album the move left wasn't read again"
        );
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    /// The title and track count of every release the catalog lists, read under one guard.
    async fn catalog_releases() -> anyhow::Result<Vec<(String, usize)>> {
        use crate::services::catalog::{
            self,
            pipeline::Catalog,
            releases::{
                ReleaseFilter,
                Releases,
            },
        };

        let db = STATE.db.read().await;
        let ids = catalog::order(
            &db,
            &catalog::Viewer::System,
            &catalog::Query::<Releases>::new(ReleaseFilter::default()),
        )?;
        let mut releases = Vec::with_capacity(ids.len());
        for release in Releases::hydrate(&db, &ids)? {
            let release = release.item;
            let release_id = release
                .db_id
                .clone()
                .map(DbId::from)
                .expect("release db_id");
            let tracks = lyra_db::tracks::get_direct(&db, release_id)?.len();
            releases.push((release.release_title, tracks));
        }
        Ok(releases)
    }

    /// Waits until the catalog lists the release titled `title`, alone, and returns the tracks it
    /// had when first seen.
    async fn tracks_when_first_listed(title: &'static str) -> usize {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let [(listed, tracks)] = catalog_releases().await.expect("catalog").as_slice()
                    && listed == title
                {
                    return *tracks;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the finished album never reached the catalog")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_finished_album_is_listed_while_later_groups_wait_to_parse() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("listed-while-parsing").await?;
        let first = (1..=3)
            .map(|track| {
                let path = root.join(format!("First/{track:02}.flac"));
                album_file(&path, "First", track).map(|()| path)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let later = album_track(&root.join("Later"), "Later")?;

        let gate: Gate<PathBuf> = std::sync::Arc::new(|source_dir: PathBuf| {
            Box::pin(async move {
                if source_dir.ends_with("Later") {
                    assert_eq!(
                        tracks_when_first_listed("First").await,
                        3,
                        "the album appeared partially"
                    );
                }
            })
        });
        let hooks = Hooks {
            before_parse: Some(gate),
            ..Hooks::default()
        };
        sync_with(
            hooks.into(),
            &library,
            SyncRunProgress::unreported_for_test(),
        )
        .await?;

        for path in first.iter().chain([&later]) {
            assert!(synced(&library, path).await?);
        }
        let mut releases = catalog_releases().await?;
        releases.sort();
        assert_eq!(
            releases,
            [("First".to_string(), 3), ("Later".to_string(), 1)]
        );
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn groups_applying_to_one_release_are_planned_together() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("one-release").await?;
        let (one, two) = (root.join("A/01.flac"), root.join("B/02.flac"));
        album_file(&one, "Album", 1)?;
        album_file(&two, "Album", 2)?;
        sync(&library).await?;

        let entry_of = async |path: &Path| -> anyhow::Result<DbId> {
            Ok(entry_for(&library, path).await?.db_id.expect("entry db_id"))
        };
        let entries = [entry_of(&one).await?, entry_of(&two).await?];
        let (winner, loser) = (release_of(entries[0]).await?, release_of(entries[1]).await?);
        assert_ne!(winner, loser, "the groups must make separate releases");
        {
            let mut db = STATE.db.write().await;
            let merge = db.transaction_mut(|t| {
                crate::services::providers::merge_release_into_inside_tx(t, winner, loser)
            })?;
            STATE.generation().merge_journal.record(merge);
        }
        shift_mtime(&one, 5)?;
        shift_mtime(&two, 5)?;

        let hooks = std::sync::Arc::new(Hooks::default());
        sync_with(
            hooks.clone(),
            &library,
            SyncRunProgress::unreported_for_test(),
        )
        .await?;

        let tracks = snapshot(&library).await?;
        let db = STATE.db.read().await;
        let mut expected_tracks = Vec::new();
        for entry in entries {
            expected_tracks.extend(
                lyra_db::tracks::get_by_entry(&db, entry)?
                    .into_iter()
                    .filter_map(|track| track.db_id.map(DbId::from)),
            );
        }
        drop(db);
        expected_tracks.sort();
        let mut expected_entries = entries.to_vec();
        expected_entries.sort();
        assert_eq!(tracks[&one].1.len() + tracks[&two].1.len(), 2);

        let mut planned = std::mem::take(&mut *hooks.planned.lock().expect("planned"));
        assert_eq!(planned.len(), 1, "expected one plan");
        let (mut track_ids, mut entry_ids) = planned.remove(0);
        track_ids.sort();
        entry_ids.sort();
        assert_eq!(track_ids, expected_tracks);
        assert_eq!(entry_ids, expected_entries);
        assert!(synced(&library, &one).await? && synced(&library, &two).await?);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_group_of_a_shared_release_is_left_for_the_next_sync() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("shared-release-failure").await?;
        let (good, bad) = (root.join("A/01.flac"), root.join("B/02.flac"));
        album_file(&good, "Good", 1)?;
        album_file(&bad, "Bad", 2)?;
        sync(&library).await?;

        let good_entry = entry_for(&library, &good)
            .await?
            .db_id
            .expect("entry db_id");
        let bad_entry = entry_for(&library, &bad).await?.db_id.expect("entry db_id");
        let (winner, loser) = (release_of(good_entry).await?, release_of(bad_entry).await?);
        assert_ne!(winner, loser, "the groups must make separate releases");
        {
            let mut db = STATE.db.write().await;
            let merge = db.transaction_mut(|t| {
                crate::services::providers::merge_release_into_inside_tx(t, winner, loser)
            })?;
            STATE.generation().merge_journal.record(merge);
        }
        shift_mtime(&good, 5)?;
        shift_mtime(&bad, 5)?;

        let progress = super::super::orchestrator::begin_test_run(&library).await;
        let hooks = std::sync::Arc::new(Hooks {
            reject_album: Some("Bad"),
            ..Hooks::default()
        });
        sync_with(hooks.clone(), &library, progress.clone()).await?;
        let run = super::super::orchestrator::get_sync_run(progress.run_id())
            .await?
            .expect("run");
        super::super::orchestrator::end_test_run(&progress).await;

        assert_eq!(run.progress.failed_units, 1);
        let planned = std::mem::take(&mut *hooks.planned.lock().expect("planned"));
        assert_eq!(planned.len(), 1, "expected one plan");
        assert_eq!(
            planned[0].1,
            [good_entry],
            "the failed group joined the plan"
        );
        assert!(synced(&library, &good).await?);
        assert!(!synced(&library, &bad).await?);

        let hooks = std::sync::Arc::new(Hooks::default());
        sync_with(
            hooks.clone(),
            &library,
            SyncRunProgress::unreported_for_test(),
        )
        .await?;

        assert_eq!(*hooks.applied.lock().expect("applied"), ["Bad"]);
        let planned = std::mem::take(&mut *hooks.planned.lock().expect("planned"));
        assert_eq!(planned.len(), 1, "expected one plan");
        assert_eq!(planned[0].1, [bad_entry]);
        assert!(synced(&library, &bad).await?);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cancelling_during_parsing_leaves_applied_albums_for_the_next_sync()
    -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("cancel-parsing").await?;
        let first = album_track(&root.join("First"), "First")?;
        let later = album_track(&root.join("Later"), "Later")?;

        let progress = super::super::orchestrator::begin_test_run(&library).await;
        let run_id = progress.run_id().to_string();
        let gate: Gate<PathBuf> = std::sync::Arc::new(move |source_dir: PathBuf| {
            let run_id = run_id.clone();
            Box::pin(async move {
                if source_dir.ends_with("Later") {
                    tracks_when_first_listed("First").await;
                    super::super::orchestrator::cancel_sync_run(&run_id)
                        .await
                        .expect("cancel");
                    std::future::pending::<()>().await;
                }
            })
        });
        let hooks = Hooks {
            before_parse: Some(gate),
            ..Hooks::default()
        };
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            sync_with(hooks.into(), &library, progress.clone()),
        )
        .await
        .expect("the cancelled scan kept waiting for a parse");
        super::super::orchestrator::end_test_run(&progress).await;

        let err = result.expect_err("the run finished despite its cancellation");
        assert!(
            err.downcast_ref::<SyncRunControlError>().is_some(),
            "{err:#}"
        );
        let applied = snapshot(&library).await?[&first].clone();
        assert_eq!(applied.1.len(), 1, "the applied album was lost");
        assert!(
            !synced(&library, &first).await?,
            "an album without its artifacts counted as synced"
        );
        assert!(snapshot(&library).await?[&later].1.is_empty());

        sync(&library).await?;
        let after = snapshot(&library).await?;
        assert_eq!(after[&first], applied, "re-applying replaced the track");
        assert_eq!(after[&later].1.len(), 1);
        assert!(synced(&library, &first).await? && synced(&library, &later).await?);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn apply_totals_grow_with_the_batches_a_group_produces() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("apply-totals").await?;
        album_file(&root.join("Singles/one.flac"), "One", 1)?;
        album_file(&root.join("Singles/two.flac"), "Two", 2)?;

        let progress = super::super::orchestrator::begin_test_run(&library).await;
        let hooks = std::sync::Arc::new(Hooks::default());
        sync_with(hooks.clone(), &library, progress.clone()).await?;
        let run = super::super::orchestrator::get_sync_run(progress.run_id())
            .await?
            .expect("run");
        super::super::orchestrator::end_test_run(&progress).await;

        assert_eq!(hooks.applied.lock().expect("applied").len(), 2);
        assert_eq!(run.progress.failed_units, 0);
        assert_eq!(
            run.progress.completed_units + run.progress.skipped_units,
            run.progress.total_units,
            "every counted unit must settle, and no unit may go uncounted"
        );
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn audio_on_no_release_does_not_cost_a_folder_its_cover() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (root, library) = empty_library("cover-no-release").await?;
        let album = root.join("Album");
        let kept = album_track(&album, "Album")?;
        let stray = album.join("stray.flac");
        std::fs::copy(&kept, &stray)?;
        let art = album.join("cover.png");
        std::fs::write(&art, PNG_1X1)?;
        sync(&library).await?;
        let art = art.to_string_lossy().into_owned();
        assert_eq!(
            local_cover_of(&library, &kept).await?.as_deref(),
            Some(art.as_str())
        );

        let stray_entry = entry_for(&library, &stray).await?.db_id.expect("id");
        {
            let mut db = STATE.db.write().await;
            let track = lyra_db::tracks::get_by_entry(&db, stray_entry)?
                .into_iter()
                .next()
                .and_then(|track| track.db_id.map(DbId::from))
                .expect("track");
            for release in lyra_db::releases::get_by_track(&db, track)? {
                let release = release.db_id.map(DbId::from).expect("release id");
                let _ = lyra_db::releases::unlink_track(&mut db, release, track)?;
            }
            assert!(lyra_db::entries::release_ids(&*db, stray_entry)?.is_empty());
        }
        sync(&library).await?;

        assert_eq!(
            local_cover_of(&library, &kept).await?.as_deref(),
            Some(art.as_str()),
            "a file on no release says nothing about who owns the folder"
        );
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }
}
