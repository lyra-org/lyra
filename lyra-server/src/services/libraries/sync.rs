// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::DbId;
use anyhow::Context;
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
    time::Instant,
};

use super::{
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
        hash_entry_group,
        prepare_entry_scan_plan,
    },
};
pub(crate) use crate::services::system::library_sync_context as system_context;
use crate::services::{
    CoverPaths,
    CoverSyncOptions,
    covers::{
        configured_covers_root,
        record_local_release_cover,
        refresh_gone_covers_for_library,
        sync_release_cover_for_tracks,
    },
    metadata::{
        TrackMetadata,
        cleanup::cleanup_orphaned_metadata,
        ingestion::{
            MetadataApplyResult,
            ParsedMetadataGroup,
            albums_left_by,
            apply_metadata,
            coalesce_disc_groups,
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
        refresh_library_metadata_after_others,
        refresh_release_metadata_for_scan_with_progress,
        release_refresh_provider_ids,
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

fn trace_run_id(progress: Option<&SyncRunProgress>) -> String {
    progress
        .map(|progress| progress.run_id().to_string())
        .unwrap_or_default()
}

/// What the scan saw: the paths it observed and those it couldn't read.
pub(crate) struct ScanObservation {
    pub(super) observed_paths: HashSet<std::path::PathBuf>,
    protected_paths: Vec<std::path::PathBuf>,
    /// The entries that moved, as they were before.
    moved_from: Vec<Entry>,
}

pub(crate) async fn sync_entries_incremental(
    db: &DbAsync,
    library: &Library,
    progress: Option<SyncRunProgress>,
) -> anyhow::Result<ScanObservation> {
    let run_id = trace_run_id(progress.as_ref());
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;

    let existing = {
        let db_read = db.read().await;
        lyra_db::entries::load_existing(&*db_read, library_db_id)?
    };
    let scan = {
        let library = library.clone();
        tokio::task::spawn_blocking(move || prepare_entry_scan_plan(&library, existing))
    };
    // A cancelled run leaves the scan to finish on its own; it only reads.
    let scan_plan = match &progress {
        Some(progress) => tokio::select! {
            plan = scan => plan,
            err = until_cancelled(progress) => return Err(err.into()),
        },
        None => scan.await,
    }
    .context("library scan task panicked")??;
    report_unlistable(&scan_plan.unlistable, progress.as_ref()).await;
    let protected_paths = scan_plan.protected_paths();
    let EntryScanPlan {
        dirs,
        groups,
        mut stored,
        observed_paths,
        moved_from,
        ..
    } = scan_plan;
    let entry_group_count = groups.len();
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
        observed_entry_count = observed_paths.len(),
        stage = "entry_sync",
        "scan entry groups discovered"
    );

    sync_entry_dirs(db, &mut stored, dirs).await?;

    let stored = &stored;
    let group_results = futures::stream::iter(groups.into_iter().enumerate())
        .map(|(entry_group_id, group)| {
            let db = db.clone();
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
                let result = apply_entry_group(&db, stored, group.entries).await;

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
                                    format!("{err:#}"),
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
                    added = result.added.len(),
                    updated = result.updated,
                    moved = result.moved,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    stage = "entry_sync",
                    "scan entry group synced"
                );
                if let Some(progress) = &progress {
                    progress
                        .complete_work(work_id, SyncStageKey::EntrySync)
                        .await;
                }
                Ok::<_, anyhow::Error>(())
            }
        })
        .buffer_unordered(MAX_CONCURRENT_ALBUM_PIPELINE)
        .collect::<Vec<_>>()
        .await;

    for group_result in group_results {
        group_result?;
    }

    Ok(ScanObservation {
        observed_paths,
        protected_paths,
        moved_from,
    })
}

/// Hashes the group's files and applies what changed. A group with no changes takes no write lock,
/// and so counts as no catalog change.
async fn apply_entry_group(
    db: &DbAsync,
    stored: &StoredEntries,
    entries: Vec<Entry>,
) -> anyhow::Result<EntrySyncResult> {
    let entries = tokio::task::spawn_blocking(move || hash_entry_group(entries)).await?;
    let changes = stored.changes(entries);
    if changes.is_empty() {
        return Ok(EntrySyncResult::default());
    }
    let mut db_write = db.write().await;
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

/// Reports each path the scan couldn't read as a failed unit, so the run shows it.
async fn report_unlistable(
    unlistable: &[(std::path::PathBuf, String)],
    progress: Option<&SyncRunProgress>,
) {
    let Some(progress) = progress else {
        return;
    };
    if unlistable.is_empty() {
        return;
    }
    progress
        .add_stage_total(
            SyncStageKey::Discover,
            unlistable.len() as u64,
            SyncTotalState::Final,
        )
        .await;
    for (path, error) in unlistable {
        let details = SyncWorkDetails::new("scan_folder")
            .source_dir(Some(path.to_string_lossy().into_owned()));
        let work_id = progress
            .start_work(SyncStageKey::Discover, details.clone())
            .await;
        progress
            .fail_work(work_id, SyncStageKey::Discover, details, error.clone())
            .await;
    }
}

pub(super) async fn mark_missing_entries_for_scan(
    db: &DbAsync,
    library: &Library,
    observation: &ScanObservation,
    progress: Option<SyncRunProgress>,
) -> anyhow::Result<()> {
    let run_id = trace_run_id(progress.as_ref());
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;
    let started = Instant::now();
    let details = SyncWorkDetails::new("mark_missing_entries");
    let work_id = if let Some(progress) = &progress {
        progress
            .start_work(SyncStageKey::Cleanup, details.clone())
            .await
    } else {
        0
    };
    let result = {
        let mut db_write = db.write().await;
        mark_missing(
            &mut db_write,
            library,
            &observation.observed_paths,
            &observation.protected_paths,
            &observation.moved_from,
            |missing, present| albums_left_by(&library.path, missing, present),
        )
    };
    let result = match result {
        Ok(result) => result,
        Err(err) => {
            if let Some(progress) = &progress {
                progress
                    .fail_work(work_id, SyncStageKey::Cleanup, details, format!("{err:#}"))
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
        marked_missing = result.marked,
        revived = result.revived,
        removed_dirs = result.removed_dirs,
        elapsed_ms = started.elapsed().as_millis() as u64,
        stage = "cleanup",
        "scan missing entries marked"
    );
    Ok(())
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
                    format!("{err:#}"),
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

/// Scans the library's files, applies what changed, then asks the providers about the releases
/// `options.releases` picks. With every release, they refresh as a library refresh does before
/// the changed ones run their other stages, so none is refreshed twice.
pub(crate) async fn sync_library_pipeline(
    db: &DbAsync,
    library: &Library,
    options: LibrarySyncOptions,
    progress: Option<SyncRunProgress>,
) -> anyhow::Result<()> {
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;
    if let Some(progress) = &progress {
        progress.set_status(SyncRunStatus::Running).await;
        progress.check_cancelled().await?;
    }
    let observation = sync_entries_incremental(db, library, progress.clone()).await?;
    mark_missing_entries_for_scan(db, library, &observation, progress.clone()).await?;
    if let Some(progress) = &progress {
        progress.check_cancelled().await?;
    }
    let plans = process_pending_metadata(db, library, progress.clone()).await?;
    let stages = ReleaseArtifactStages::for_sync(options);
    if let Some(progress) = &progress {
        if stages.provider_refresh {
            let provider_count = release_refresh_provider_ids(None, false).await?.len() as u64;
            progress
                .add_stage_total(
                    SyncStageKey::ProviderRefresh,
                    plans.len() as u64 * provider_count,
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
    }
    if options.releases == SyncReleases::All {
        refresh_every_release(library_db_id, options.covers, progress.clone()).await?;
    }
    let changed_releases = plans
        .iter()
        .map(|plan| (plan.release_id, plan.release_public_id.clone()))
        .collect::<Vec<_>>();
    futures::stream::iter(plans)
        .for_each_concurrent(MAX_CONCURRENT_ALBUM_PIPELINE, |plan| {
            let db = db.clone();
            let library_path = library.path.clone();
            let progress = progress.clone();
            async move {
                #[cfg(test)]
                tests::before_release_artifacts(&plan).await;
                if process_release_artifacts(
                    &db,
                    library_db_id,
                    &library_path,
                    &plan,
                    stages,
                    progress,
                )
                .await
                {
                    mark_entries_synced(&db, library_db_id, &plan.entry_ids).await;
                }
            }
        })
        .await;
    if let Some(progress) = &progress {
        progress.check_cancelled().await?;
    }
    refresh_gone_covers(db, library, library_db_id).await;
    if !stages.covers.skips() {
        sync_changed_artist_covers(db, library, &changed_releases, stages.covers, &progress)
            .await?;
    }
    purge_expired_missing(db, library, &observation, progress.as_ref()).await?;
    cleanup_metadata_orphans(db, library_db_id, progress).await?;
    Ok(())
}

/// Purges the files missing longer than the library keeps them, when it sets a limit, before the
/// orphan cleanup that collects what they leave.
async fn purge_expired_missing(
    db: &DbAsync,
    library: &Library,
    observation: &ScanObservation,
    progress: Option<&SyncRunProgress>,
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
        lyra_db::entries::missing_purge_plan(&*db_read, library, &selection)
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
    progress: Option<SyncRunProgress>,
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
    let err = err.into_anyhow();
    if err.downcast_ref::<SyncRunControlError>().is_some() {
        return Err(err);
    }
    tracing::warn!(
        library_db_id = library_db_id.0,
        error = %format_args!("{err:#}"),
        "sync of every release could not refresh the library"
    );
    if let Some(progress) = &progress {
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
    }
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

/// Fetches the covers of the artists credited on the changed releases or on their tracks, after
/// the releases' provider refreshes stored what they found. A release gone meanwhile has no
/// artists to pass; artists that can't be listed are none to pass, so the totals still settle.
async fn sync_changed_artist_covers(
    db: &DbAsync,
    library: &Library,
    releases: &[(DbId, Option<String>)],
    options: CoverSyncOptions,
    progress: &Option<SyncRunProgress>,
) -> Result<(), SyncRunControlError> {
    let artists = {
        let db_read = db.read().await;
        let current = releases
            .iter()
            .filter(|(release_id, public_id)| {
                public_id.is_some()
                    && lyra_db::lookup::find_id_by_db_id(&db_read, *release_id)
                        .is_ok_and(|found| found == *public_id)
            })
            .map(|(release_id, _)| *release_id)
            .collect::<Vec<_>>();
        lyra_db::artists::get_by_releases(&db_read, &current)
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
    sync_artist_covers(&artists, paths, options, None, progress.as_ref()).await?;
    Ok(())
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

async fn until_cancelled(progress: &SyncRunProgress) -> SyncRunControlError {
    loop {
        if let Err(err) = progress.check_cancelled().await {
            return err;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn is_cancelled(progress: Option<&SyncRunProgress>) -> bool {
    match progress {
        Some(progress) => progress.check_cancelled().await.is_err(),
        None => false,
    }
}

async fn fail_metadata_work(
    progress: Option<&SyncRunProgress>,
    work_id: u64,
    stage: SyncStageKey,
    details: SyncWorkDetails,
    err: &anyhow::Error,
) {
    match progress {
        Some(progress) => {
            progress
                .fail_work(work_id, stage, details, format!("{err:#}"))
                .await;
        }
        None => tracing::warn!(
            stage = stage.as_str(),
            source_dir = ?details.source_dir,
            error = %format_args!("{err:#}"),
            "sync work failed"
        ),
    }
}

fn is_unsynced_file(entry: &Entry) -> bool {
    entry.kind == EntryKind::File && entry.synced_at_ms.is_none() && !entry.is_missing()
}

/// Reads the metadata groups holding unsynced files and applies the batches those files are in.
/// Files with nothing left to do are marked synced as their group is read. A failed group, like
/// a transiently unreadable file, is a failed unit, and its files stay unsynced for the next
/// sync.
async fn process_pending_metadata(
    db: &DbAsync,
    library: &Library,
    progress: Option<SyncRunProgress>,
) -> anyhow::Result<Vec<ReleaseArtifactPlan>> {
    let run_id = trace_run_id(progress.as_ref());
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
    let cue_links = {
        let cue_entry_ids = entries
            .iter()
            .filter(|entry| entry.file_kind.as_deref() == Some("cue"))
            .filter_map(|entry| entry.db_id)
            .collect::<Vec<_>>();
        let db_read = db.read().await;
        lyra_db::cue::audio_links(&*db_read, &cue_entry_ids)?
    };
    let groups = match &progress {
        Some(progress) => tokio::select! {
            groups = metadata_groups(&library.path, entries, &cue_links) => groups,
            err = until_cancelled(progress) => return Err(err.into()),
        },
        None => metadata_groups(&library.path, entries, &cue_links).await,
    };
    let groups = groups
        .into_iter()
        .filter(|group| group.entries.iter().any(is_unsynced_file))
        .collect::<Vec<_>>();
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
    let mut unsynced_entries = HashSet::new();
    let mut unreadable_entries = HashSet::new();
    let mut parsed_groups = Vec::new();
    for (coalesce_group_key, group) in groups.into_iter().enumerate() {
        if let Some(progress) = &progress {
            progress.check_cancelled().await?;
        }
        let source_dir = group.source_dir.clone();
        let details = SyncWorkDetails::new("metadata_parse_group")
            .source_dir(Some(source_dir.to_string_lossy().into_owned()));
        let work_id = if let Some(progress) = &progress {
            progress
                .start_work(SyncStageKey::MetadataParse, details.clone())
                .await
        } else {
            0
        };
        let mut entry_source_dirs = BTreeMap::new();
        let mut group_unsynced = HashMap::new();
        for entry in &group.entries {
            let Some(entry_db_id) = entry.db_id else {
                continue;
            };
            if entry.kind != EntryKind::File {
                continue;
            }
            let parent = group.candidate_dir(&entry.full_path);
            source_dirs_by_entry.insert(entry_db_id, parent.clone());
            entry_source_dirs.insert(entry_db_id, parent);
            if entry.synced_at_ms.is_none() {
                group_unsynced.insert(entry.full_path.clone(), entry_db_id);
            }
        }
        let parse_output = match parse_metadata(&mapping_config, group.entries, &cue_links).await {
            Ok(output) => output,
            Err(err) => {
                fail_metadata_work(
                    progress.as_ref(),
                    work_id,
                    SyncStageKey::MetadataParse,
                    details,
                    &err,
                )
                .await;
                continue;
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
        unreadable_entries.extend(
            unreadable
                .iter()
                .filter_map(|record| group_unsynced.get(&record.path).copied()),
        );
        let metadata = parse_output.metadata;
        let read = metadata_entry_ids(&metadata).collect::<HashSet<_>>();
        let finished = group_unsynced
            .values()
            .copied()
            .filter(|entry_id| !read.contains(entry_id) && !unreadable_entries.contains(entry_id))
            .collect::<Vec<_>>();
        mark_entries_synced(db, library_db_id, &finished).await;
        unsynced_entries.extend(group_unsynced.into_values());

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
        match unreadable.first() {
            Some(first) => {
                let err = anyhow::anyhow!(
                    "{} files could not be read; {}: {}",
                    unreadable.len(),
                    first.path.display(),
                    skip_reason_summary(&first.reason)
                );
                fail_metadata_work(
                    progress.as_ref(),
                    work_id,
                    SyncStageKey::MetadataParse,
                    details,
                    &err,
                )
                .await;
            }
            None => {
                if let Some(progress) = &progress {
                    progress
                        .complete_work(work_id, SyncStageKey::MetadataParse)
                        .await;
                }
            }
        }
    }

    let batches = coalesce_disc_groups(parsed_groups)
        .into_iter()
        .filter(|metadata| {
            metadata_entry_ids(metadata).any(|entry_id| unsynced_entries.contains(&entry_id))
        })
        .collect::<Vec<_>>();
    let batch_count = batches.len();
    if let Some(progress) = &progress {
        progress
            .add_stage_total(
                SyncStageKey::MetadataApply,
                batch_count as u64,
                SyncTotalState::Estimated,
            )
            .await;
    }
    let apply_results = futures::stream::iter(batches.into_iter().enumerate())
        .map(|(group_id, metadata)| {
            let db = db.clone();
            let progress = progress.clone();
            let source_dir = metadata
                .first()
                .and_then(|track| source_dirs_by_entry.get(&track.entry_db_id))
                .cloned();
            let entry_ids = metadata_entry_ids(&metadata)
                .filter(|entry_id| !unreadable_entries.contains(entry_id))
                .collect::<Vec<_>>();
            async move {
                let result = process_metadata_group(
                    &db,
                    library_db_id,
                    group_id,
                    source_dir,
                    metadata,
                    progress,
                )
                .await;
                (entry_ids, result)
            }
        })
        .buffer_unordered(MAX_CONCURRENT_ALBUM_PIPELINE)
        .collect::<Vec<_>>()
        .await;

    // Batches whose tracks were one release keep it, so its artifacts run once for all of them.
    let mut plans: Vec<ReleaseArtifactPlan> = Vec::new();
    for (entry_ids, result) in apply_results {
        let releases = match result {
            Ok(apply_result) => apply_result.releases,
            Err(err) if err.downcast_ref::<SyncRunControlError>().is_some() => return Err(err),
            Err(_) => continue,
        };
        let Some(&release_id) = releases.first() else {
            mark_entries_synced(db, library_db_id, &entry_ids).await;
            continue;
        };
        match plans.iter_mut().find(|plan| plan.release_id == release_id) {
            Some(plan) => plan.entry_ids.extend(entry_ids),
            None => match release_artifact_plan(db, release_id, entry_ids.clone()).await? {
                Some(plan) => plans.push(plan),
                None => mark_entries_synced(db, library_db_id, &entry_ids).await,
            },
        }
    }
    tracing::info!(
        library_db_id = library_db_id.0,
        run_id = %run_id,
        group_count = batch_count,
        "scan metadata groups completed"
    );
    Ok(plans)
}

fn metadata_entry_ids(metadata: &[TrackMetadata]) -> impl Iterator<Item = DbId> + '_ {
    metadata
        .iter()
        .flat_map(|track| std::iter::once(track.entry_db_id).chain(track.cue_sheet_entry_id))
}

async fn process_metadata_group(
    db: &DbAsync,
    library_db_id: DbId,
    group_id: usize,
    source_dir: Option<PathBuf>,
    metadata: Vec<TrackMetadata>,
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
            fail_metadata_work(
                progress.as_ref(),
                work_id,
                SyncStageKey::MetadataApply,
                SyncWorkDetails::new("metadata_apply_group").source_dir(source_dir_text),
                &err,
            )
            .await;
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
    /// The entries marked synced once the release's artifacts finish.
    entry_ids: Vec<DbId>,
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
    }))
}

/// Runs a release's artifact stages; false when its entries need another sync: its local cover
/// failed to persist or the run was cancelled. Provider, lyrics and provider cover failures are
/// failed units that `/refresh` retries.
async fn process_release_artifacts(
    db: &DbAsync,
    library_db_id: DbId,
    library_path: &Path,
    plan: &ReleaseArtifactPlan,
    stages: ReleaseArtifactStages,
    progress: Option<SyncRunProgress>,
) -> bool {
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
    if is_cancelled(progress.as_ref()).await {
        return false;
    }
    if stages.provider_refresh {
        refresh_changed_release(&context).await;
        if is_cancelled(progress.as_ref()).await {
            return false;
        }
    }

    let local_cover = sync_local_release_cover_metadata(&context).await;
    dispatch_release_lyrics(library_db_id, release_id, plan, run_id, progress.clone()).await;
    if !stages.covers.skips() {
        sync_provider_release_cover(&context, stages.covers).await;
    }
    local_cover.is_ok() && !is_cancelled(progress.as_ref()).await
}

async fn refresh_changed_release(context: &ReleaseArtifactContext<'_>) {
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
    if let Err(err) = refresh_release_metadata_for_scan_with_progress(
        library_db_id,
        release_id,
        context.progress.clone(),
        context.release_public_id.clone(),
        context.release_title.clone(),
    )
    .await
    {
        tracing::warn!(
            library_db_id = library_db_id.0,
            run_id = %run_id,
            release_db_id = release_id.0,
            stage = "provider_refresh",
            error = %format_args!("{:#}", anyhow::Error::from(err)),
            "scan release provider refresh failed"
        );
    }
}

/// Records the release's local cover, failing when it can't be resolved or persisted.
async fn sync_local_release_cover_metadata(
    context: &ReleaseArtifactContext<'_>,
) -> anyhow::Result<()> {
    let started = Instant::now();
    let details = context.details("local_cover_metadata");
    let work_id = if let Some(progress) = &context.progress {
        progress
            .start_work(SyncStageKey::LocalCoverMetadata, details.clone())
            .await
    } else {
        0
    };
    let result =
        record_local_release_cover(context.db, context.release_id, Some(context.library_path))
            .await;

    match result {
        Ok(synced) => {
            if let Some(progress) = &context.progress {
                progress
                    .complete_work(work_id, SyncStageKey::LocalCoverMetadata)
                    .await;
            }
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
            match &context.progress {
                Some(progress) => {
                    progress
                        .fail_work(
                            work_id,
                            SyncStageKey::LocalCoverMetadata,
                            details,
                            format!("{err:#}"),
                        )
                        .await;
                }
                None => tracing::warn!(
                    library_db_id = context.library_db_id.0,
                    run_id = %context.run_id,
                    release_db_id = context.release_id.0,
                    stage = "local_cover_metadata",
                    error = %format_args!("{err:#}"),
                    "scan release local cover metadata sync failed"
                ),
            }
            Err(err)
        }
    }
}

async fn dispatch_release_lyrics(
    library_db_id: DbId,
    release_id: DbId,
    plan: &ReleaseArtifactPlan,
    run_id: String,
    progress: Option<SyncRunProgress>,
) {
    if plan.track_ids.is_empty() {
        return;
    }

    let started = Instant::now();
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
                                .fail_work(
                                    work_id,
                                    SyncStageKey::Lyrics,
                                    details,
                                    format!("{err:#}"),
                                )
                                .await;
                        }
                        tracing::warn!(
                            library_db_id = library_db_id.0,
                            run_id = %run_id,
                            release_db_id = release_id.0,
                            track_db_id = track_db_id.0,
                            error = %format_args!("{err:#}"),
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
}

async fn sync_provider_release_cover(
    context: &ReleaseArtifactContext<'_>,
    options: CoverSyncOptions,
) {
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
            return;
        }
        Err(err) => {
            if let Some(progress) = &context.progress {
                progress
                    .fail_work(
                        work_id,
                        SyncStageKey::ProviderCover,
                        details,
                        format!("{err:#}"),
                    )
                    .await;
            }
            tracing::warn!(
                library_db_id = context.library_db_id.0,
                run_id = %context.run_id,
                release_db_id = context.release_id.0,
                stage = "provider_cover",
                error = %format_args!("{err:#}"),
                "scan release load failed before provider cover sync"
            );
            return;
        }
    };

    let synced = match sync_release_cover_for_tracks(
        context.db,
        &tracks,
        &release,
        &artists,
        cover_paths,
        options,
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
                        format!("{err:#}"),
                    )
                    .await;
            }
            tracing::warn!(
                library_db_id = context.library_db_id.0,
                run_id = %context.run_id,
                release_db_id = context.release_id.0,
                stage = "provider_cover",
                error = %format_args!("{err:#}"),
                "scan release provider cover sync failed"
            );
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
    sync_library_pipeline(db, library, LibrarySyncOptions::default(), None).await
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
    use crate::services::STATE;

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

    type ArtifactGate = std::sync::Arc<
        dyn Fn(Option<String>) -> futures::future::BoxFuture<'static, ()> + Send + Sync,
    >;

    #[derive(Default)]
    struct Hooks {
        reject_album: Option<&'static str>,
        applied: std::sync::Mutex<Vec<String>>,
        before_artifacts: Option<ArtifactGate>,
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

    pub(super) async fn before_release_artifacts(plan: &ReleaseArtifactPlan) {
        let gate = HOOKS
            .try_with(|hooks| hooks.before_artifacts.clone())
            .ok()
            .flatten();
        if let Some(gate) = gate {
            gate(plan.release_title.clone()).await;
        }
    }

    async fn sync_with(
        hooks: std::sync::Arc<Hooks>,
        library: &Library,
        progress: Option<SyncRunProgress>,
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
            sync_with(hooks.into(), &library, Some(progress.clone())).await?;
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
        sync_with(hooks.clone(), &library, None).await?;

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
        sync_with(hooks.clone(), &library, None).await?;

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
            async move {
                sync_library_pipeline(&STATE.db.get(), &library, options, Some(progress)).await
            }
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
            Some(progress.clone()),
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
        sync_with(hooks.clone(), &library, None).await?;

        assert_eq!(*hooks.applied.lock().expect("applied"), ["Album"]);
        assert!(synced(&library, &main).await?);
        let _ = std::fs::remove_dir_all(root);
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
            id: nanoid::nanoid!(),
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
        sync_entries_incremental(&db, &library, None).await?;
        let stamp = db.stamp();

        sync_entries_incremental(&db, &library, None).await?;

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
            None,
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
            None,
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
                    id: nanoid::nanoid!(),
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
            Some(progress.clone()),
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
        let result = sync_with(hooks.into(), &library, Some(progress.clone())).await;
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
}
