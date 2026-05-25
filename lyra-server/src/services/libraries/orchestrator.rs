// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        BTreeMap,
        HashMap,
    },
    path::Path,
    sync::{
        Arc,
        atomic::{
            AtomicU64,
            Ordering,
        },
    },
    time::{
        Instant,
        SystemTime,
        UNIX_EPOCH,
    },
};

use agdb::DbId;
use serde::Serialize;
use std::sync::LazyLock;
use tokio::sync::RwLock;
use tokio::time::{
    Duration,
    sleep,
};

use crate::db::{
    DbAsync,
    Library,
};
use crate::routes::unix_secs_to_rfc3339_u64;

use super::sync::{
    MAX_CONCURRENT_ALBUM_PIPELINE,
    sync_library_pipeline,
};

const MAX_ACTIVE_SYNC_ITEMS: usize = 8;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LibrarySyncRunStatus {
    Idle,
    Running,
    Succeeded,
    Failed,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LibrarySyncPhase {
    FullSync,
    Metadata,
    ProviderRefresh,
    Cleanup,
    Complete,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LibrarySyncStage {
    Discovering,
    EntrySync,
    MetadataParse,
    MetadataApply,
    ProviderRefresh,
    LocalCoverMetadata,
    Lyrics,
    ProviderCover,
    Cleanup,
    Complete,
}

impl LibrarySyncStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Discovering => "discovering",
            Self::EntrySync => "entry_sync",
            Self::MetadataParse => "metadata_parse",
            Self::MetadataApply => "metadata_apply",
            Self::ProviderRefresh => "provider_refresh",
            Self::LocalCoverMetadata => "local_cover_metadata",
            Self::Lyrics => "lyrics",
            Self::ProviderCover => "provider_cover",
            Self::Cleanup => "cleanup",
            Self::Complete => "complete",
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct LibrarySyncCounters {
    pub(crate) entry_groups: usize,
    pub(crate) entries_added: usize,
    pub(crate) entries_updated: usize,
    pub(crate) entries_deleted: usize,
    pub(crate) discovered_groups: usize,
    pub(crate) completed_groups: usize,
    pub(crate) failed_groups: usize,
    pub(crate) releases: usize,
    pub(crate) tracks: usize,
    pub(crate) provider_refreshes: usize,
    pub(crate) lyrics_tracks: usize,
    pub(crate) local_cover_metadata: usize,
    pub(crate) provider_covers: usize,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub(crate) struct LibrarySyncActiveItem {
    pub(crate) group_id: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) source_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) release_db_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) release_title: Option<String>,
    pub(crate) stage: LibrarySyncStage,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) provider_id: Option<String>,
    pub(crate) stage_started_at: u64,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub(crate) struct LibrarySyncStatus {
    pub(crate) run: LibrarySyncRun,
    pub(crate) progress: LibrarySyncStatusProgress,
    pub(crate) changes: LibrarySyncChanges,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) active: Vec<LibrarySyncActiveWork>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub(crate) struct LibrarySyncRun {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<u64>,
    pub(crate) status: LibrarySyncRunStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Run start time as an RFC3339 timestamp.")
    )]
    pub(crate) started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Most recent run status update time as an RFC3339 timestamp.")
    )]
    pub(crate) updated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Run completion time as an RFC3339 timestamp.")
    )]
    pub(crate) finished_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub(crate) struct LibrarySyncStatusProgress {
    pub(crate) work: LibrarySyncWorkProgress,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub(crate) struct LibrarySyncWorkProgress {
    pub(crate) total: usize,
    pub(crate) pending: usize,
    pub(crate) active: usize,
    pub(crate) completed: usize,
    pub(crate) failed: usize,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) active_by_stage: BTreeMap<String, usize>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub(crate) struct LibrarySyncChanges {
    pub(crate) entries: LibrarySyncEntryChanges,
    pub(crate) library: LibrarySyncLibraryChanges,
    pub(crate) providers: LibrarySyncProviderChanges,
    pub(crate) covers: LibrarySyncCoverChanges,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub(crate) struct LibrarySyncEntryChanges {
    pub(crate) added: usize,
    pub(crate) updated: usize,
    pub(crate) deleted: usize,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub(crate) struct LibrarySyncLibraryChanges {
    pub(crate) releases: usize,
    pub(crate) tracks: usize,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub(crate) struct LibrarySyncProviderChanges {
    pub(crate) metadata_refreshes: usize,
    pub(crate) lyrics_tracks: usize,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub(crate) struct LibrarySyncCoverChanges {
    pub(crate) hashed: usize,
    pub(crate) synced: usize,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub(crate) struct LibrarySyncActiveWork {
    pub(crate) stage: LibrarySyncStage,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) source_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) release_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) provider_id: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Active work stage start time as an RFC3339 timestamp.")
    )]
    pub(crate) stage_started_at: String,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub(crate) struct LibrarySyncState {
    pub(crate) library_db_id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) run_id: Option<u64>,
    pub(crate) status: LibrarySyncRunStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) phase: Option<LibrarySyncPhase>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) stage: Option<LibrarySyncStage>,
    pub(crate) pending_groups: usize,
    pub(crate) active_group_count: usize,
    pub(crate) completed_group_count: usize,
    pub(crate) failed_group_count: usize,
    pub(crate) counters: LibrarySyncCounters,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) active: Vec<LibrarySyncActiveItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cleanup_stage: Option<LibrarySyncStage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) started_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) updated_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) finished_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}

impl LibrarySyncState {
    fn idle(library_db_id: DbId) -> Self {
        Self {
            library_db_id: library_db_id.0,
            run_id: None,
            status: LibrarySyncRunStatus::Idle,
            phase: None,
            stage: None,
            pending_groups: 0,
            active_group_count: 0,
            completed_group_count: 0,
            failed_group_count: 0,
            counters: LibrarySyncCounters::default(),
            active: Vec::new(),
            cleanup_stage: None,
            started_at: None,
            updated_at: None,
            finished_at: None,
            error: None,
        }
    }

    fn running(library_db_id: DbId, run_id: u64, now: u64) -> Self {
        Self {
            library_db_id: library_db_id.0,
            run_id: Some(run_id),
            status: LibrarySyncRunStatus::Running,
            phase: Some(LibrarySyncPhase::FullSync),
            stage: Some(LibrarySyncStage::Discovering),
            pending_groups: 0,
            active_group_count: 0,
            completed_group_count: 0,
            failed_group_count: 0,
            counters: LibrarySyncCounters::default(),
            active: Vec::new(),
            cleanup_stage: None,
            started_at: Some(now),
            updated_at: Some(now),
            finished_at: None,
            error: None,
        }
    }
}

impl From<LibrarySyncState> for LibrarySyncStatus {
    fn from(state: LibrarySyncState) -> Self {
        let counters = state.counters;
        let active_by_stage = {
            let mut counts = BTreeMap::new();
            for item in &state.active {
                *counts.entry(item.stage.as_str().to_owned()).or_insert(0) += 1;
            }
            counts
        };
        let active = state
            .active
            .into_iter()
            .map(|item| LibrarySyncActiveWork {
                stage: item.stage,
                source_dir: item.source_dir,
                release_title: item.release_title,
                provider_id: item.provider_id,
                stage_started_at: unix_secs_to_rfc3339_u64(item.stage_started_at),
            })
            .collect();

        Self {
            run: LibrarySyncRun {
                id: state.run_id,
                status: state.status,
                started_at: state.started_at.map(unix_secs_to_rfc3339_u64),
                updated_at: state.updated_at.map(unix_secs_to_rfc3339_u64),
                finished_at: state.finished_at.map(unix_secs_to_rfc3339_u64),
                error: state.error,
            },
            progress: LibrarySyncStatusProgress {
                work: LibrarySyncWorkProgress {
                    total: counters.discovered_groups,
                    pending: state.pending_groups,
                    active: state.active_group_count,
                    completed: state.completed_group_count,
                    failed: state.failed_group_count,
                    active_by_stage,
                },
            },
            changes: LibrarySyncChanges {
                entries: LibrarySyncEntryChanges {
                    added: counters.entries_added,
                    updated: counters.entries_updated,
                    deleted: counters.entries_deleted,
                },
                library: LibrarySyncLibraryChanges {
                    releases: counters.releases,
                    tracks: counters.tracks,
                },
                providers: LibrarySyncProviderChanges {
                    metadata_refreshes: counters.provider_refreshes,
                    lyrics_tracks: counters.lyrics_tracks,
                },
                covers: LibrarySyncCoverChanges {
                    hashed: counters.local_cover_metadata,
                    synced: counters.provider_covers,
                },
            },
            active,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct LibrarySyncProgress {
    library_db_id: DbId,
    run_id: u64,
}

impl LibrarySyncProgress {
    pub(crate) fn new(library_db_id: DbId, run_id: u64) -> Self {
        Self {
            library_db_id,
            run_id,
        }
    }

    pub(crate) fn run_id(self) -> u64 {
        self.run_id
    }

    pub(crate) async fn set_stage(self, stage: LibrarySyncStage) {
        with_current_run(self.library_db_id, self.run_id, |state| {
            state.phase = Some(phase_for_stage(stage));
            state.stage = Some(stage);
            if stage == LibrarySyncStage::Cleanup {
                state.cleanup_stage = Some(stage);
            }
        })
        .await;
    }

    pub(crate) async fn add_discovered_groups(self, count: usize) {
        with_current_run(self.library_db_id, self.run_id, |state| {
            state.pending_groups += count;
            state.counters.discovered_groups += count;
        })
        .await;
    }

    pub(crate) async fn set_entry_groups(self, count: usize) {
        with_current_run(self.library_db_id, self.run_id, |state| {
            state.counters.entry_groups = count;
        })
        .await;
    }

    pub(crate) async fn add_entry_counts(self, added: usize, updated: usize, deleted: usize) {
        with_current_run(self.library_db_id, self.run_id, |state| {
            state.counters.entries_added += added;
            state.counters.entries_updated += updated;
            state.counters.entries_deleted += deleted;
        })
        .await;
    }

    pub(crate) async fn start_group(self, group_id: usize, source_dir: Option<&Path>) {
        let source_dir = source_dir.map(|path| path.to_string_lossy().into_owned());
        with_current_run(self.library_db_id, self.run_id, |state| {
            state.phase = Some(LibrarySyncPhase::Metadata);
            state.stage = Some(LibrarySyncStage::MetadataApply);
            state.pending_groups = state.pending_groups.saturating_sub(1);
            state.active_group_count += 1;
            if state.active.len() < MAX_ACTIVE_SYNC_ITEMS {
                state.active.push(LibrarySyncActiveItem {
                    group_id,
                    source_dir,
                    release_db_id: None,
                    release_title: None,
                    stage: LibrarySyncStage::MetadataApply,
                    provider_id: None,
                    stage_started_at: now_unix_secs(),
                });
            }
        })
        .await;
    }

    pub(crate) async fn update_group_stage(
        self,
        group_id: usize,
        stage: LibrarySyncStage,
        release_db_id: Option<DbId>,
        release_title: Option<String>,
        provider_id: Option<String>,
    ) {
        with_current_run(self.library_db_id, self.run_id, |state| {
            state.phase = Some(phase_for_stage(stage));
            state.stage = Some(stage);
            if let Some(active) = state
                .active
                .iter_mut()
                .find(|item| item.group_id == group_id)
            {
                active.stage = stage;
                active.release_db_id = release_db_id.map(|id| id.0);
                if release_title.is_some() {
                    active.release_title = release_title;
                }
                active.provider_id = provider_id;
                active.stage_started_at = now_unix_secs();
            }
        })
        .await;
    }

    pub(crate) async fn complete_group(self, group_id: usize) {
        with_current_run(self.library_db_id, self.run_id, |state| {
            state.active_group_count = state.active_group_count.saturating_sub(1);
            state.completed_group_count += 1;
            state.counters.completed_groups = state.completed_group_count;
            state.active.retain(|item| item.group_id != group_id);
        })
        .await;
    }

    pub(crate) async fn fail_group(self, group_id: usize, stage: LibrarySyncStage) {
        with_current_run(self.library_db_id, self.run_id, |state| {
            state.phase = Some(phase_for_stage(stage));
            state.stage = Some(stage);
            state.active_group_count = state.active_group_count.saturating_sub(1);
            state.failed_group_count += 1;
            state.counters.failed_groups = state.failed_group_count;
            state.active.retain(|item| item.group_id != group_id);
        })
        .await;
    }

    pub(crate) async fn add_counts(
        self,
        releases: usize,
        tracks: usize,
        provider_refreshes: usize,
        lyrics_tracks: usize,
        local_cover_metadata: usize,
        provider_covers: usize,
    ) {
        with_current_run(self.library_db_id, self.run_id, |state| {
            state.counters.releases += releases;
            state.counters.tracks += tracks;
            state.counters.provider_refreshes += provider_refreshes;
            state.counters.lyrics_tracks += lyrics_tracks;
            state.counters.local_cover_metadata += local_cover_metadata;
            state.counters.provider_covers += provider_covers;
        })
        .await;
    }
}

fn phase_for_stage(stage: LibrarySyncStage) -> LibrarySyncPhase {
    match stage {
        LibrarySyncStage::Discovering | LibrarySyncStage::EntrySync => LibrarySyncPhase::FullSync,
        LibrarySyncStage::MetadataParse | LibrarySyncStage::MetadataApply => {
            LibrarySyncPhase::Metadata
        }
        LibrarySyncStage::ProviderRefresh
        | LibrarySyncStage::LocalCoverMetadata
        | LibrarySyncStage::Lyrics
        | LibrarySyncStage::ProviderCover => LibrarySyncPhase::ProviderRefresh,
        LibrarySyncStage::Cleanup => LibrarySyncPhase::Cleanup,
        LibrarySyncStage::Complete => LibrarySyncPhase::Complete,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StartLibrarySyncResult {
    Started { run_id: u64 },
    AlreadyRunning { run_id: u64 },
}

static NEXT_RUN_ID: AtomicU64 = AtomicU64::new(1);

static LIBRARY_SYNC_STATES: LazyLock<Arc<RwLock<HashMap<DbId, LibrarySyncState>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));

pub(crate) async fn reset_sync_states_for_test() {
    LIBRARY_SYNC_STATES.write().await.clear();
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

async fn with_current_run<F>(library_db_id: DbId, run_id: u64, update: F)
where
    F: FnOnce(&mut LibrarySyncState),
{
    let mut states = LIBRARY_SYNC_STATES.write().await;
    let Some(state) = states.get_mut(&library_db_id) else {
        return;
    };

    if state.status != LibrarySyncRunStatus::Running || state.run_id != Some(run_id) {
        return;
    }

    update(state);
    state.updated_at = Some(now_unix_secs());
}

async fn set_phase(library_db_id: DbId, run_id: u64, phase: LibrarySyncPhase) {
    with_current_run(library_db_id, run_id, |state| {
        state.phase = Some(phase);
    })
    .await;
}

async fn mark_succeeded(library_db_id: DbId, run_id: u64) {
    with_current_run(library_db_id, run_id, |state| {
        let now = now_unix_secs();
        state.status = LibrarySyncRunStatus::Succeeded;
        state.phase = Some(LibrarySyncPhase::Complete);
        state.stage = Some(LibrarySyncStage::Complete);
        state.active_group_count = 0;
        state.pending_groups = 0;
        state.active.clear();
        state.cleanup_stage = None;
        state.finished_at = Some(now);
        state.error = None;
    })
    .await;
}

async fn mark_failed(library_db_id: DbId, run_id: u64, error: anyhow::Error) {
    with_current_run(library_db_id, run_id, |state| {
        let now = now_unix_secs();
        state.status = LibrarySyncRunStatus::Failed;
        state.finished_at = Some(now);
        state.error = Some(error.to_string());
    })
    .await;
}

async fn run_library_sync(db: DbAsync, library: Library, library_db_id: DbId, run_id: u64) {
    let started = Instant::now();
    tracing::info!(
        library_db_id = library_db_id.0,
        library_public_id = %library.id,
        library_name = %library.name,
        run_id,
        album_concurrency = MAX_CONCURRENT_ALBUM_PIPELINE,
        provider_serialization = true,
        "library sync started"
    );
    let progress = LibrarySyncProgress::new(library_db_id, run_id);
    let result = async {
        set_phase(library_db_id, run_id, LibrarySyncPhase::FullSync).await;
        sync_library_pipeline(&db, &library, Some(progress)).await?;

        Ok::<(), anyhow::Error>(())
    }
    .await;

    match result {
        Ok(()) => {
            mark_succeeded(library_db_id, run_id).await;
            let final_state = get_library_sync_state(library_db_id).await;
            tracing::info!(
                library_db_id = library_db_id.0,
                run_id,
                status = ?final_state.status,
                phase = ?final_state.phase,
                stage = ?final_state.stage,
                elapsed_ms = started.elapsed().as_millis() as u64,
                discovered_groups = final_state.counters.discovered_groups,
                entry_groups = final_state.counters.entry_groups,
                entries_added = final_state.counters.entries_added,
                entries_updated = final_state.counters.entries_updated,
                entries_deleted = final_state.counters.entries_deleted,
                completed_groups = final_state.counters.completed_groups,
                failed_groups = final_state.counters.failed_groups,
                releases = final_state.counters.releases,
                tracks = final_state.counters.tracks,
                provider_refreshes = final_state.counters.provider_refreshes,
                lyrics_tracks = final_state.counters.lyrics_tracks,
                local_cover_metadata = final_state.counters.local_cover_metadata,
                provider_covers = final_state.counters.provider_covers,
                "library sync completed"
            );
        }
        Err(err) => {
            let failed_state = get_library_sync_state(library_db_id).await;
            tracing::error!(
                library_db_id = library_db_id.0,
                run_id,
                phase = ?failed_state.phase,
                stage = ?failed_state.stage,
                active_group_count = failed_state.active_group_count,
                failed_groups = failed_state.failed_group_count,
                elapsed_ms = started.elapsed().as_millis() as u64,
                error = %err,
                "library sync failed"
            );
            mark_failed(library_db_id, run_id, err).await;
        }
    }
}

pub(crate) async fn start_library_sync(
    db: DbAsync,
    library: Library,
) -> anyhow::Result<StartLibrarySyncResult> {
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;

    {
        let states = LIBRARY_SYNC_STATES.read().await;
        if let Some(existing) = states.get(&library_db_id)
            && existing.status == LibrarySyncRunStatus::Running
        {
            return Ok(StartLibrarySyncResult::AlreadyRunning {
                run_id: existing.run_id.unwrap_or(0),
            });
        }
    }

    let run_id = NEXT_RUN_ID.fetch_add(1, Ordering::Relaxed);
    let now = now_unix_secs();
    {
        let mut states = LIBRARY_SYNC_STATES.write().await;
        states.insert(
            library_db_id,
            LibrarySyncState::running(library_db_id, run_id, now),
        );
    }

    tokio::spawn(run_library_sync(db, library, library_db_id, run_id));

    Ok(StartLibrarySyncResult::Started { run_id })
}

pub(crate) async fn get_library_sync_state(library_db_id: DbId) -> LibrarySyncState {
    let states = LIBRARY_SYNC_STATES.read().await;
    states
        .get(&library_db_id)
        .cloned()
        .unwrap_or_else(|| LibrarySyncState::idle(library_db_id))
}

pub(crate) async fn get_library_sync_status(library_db_id: DbId) -> LibrarySyncStatus {
    get_library_sync_state(library_db_id).await.into()
}

pub(crate) async fn running_library_sync_count() -> usize {
    let states = LIBRARY_SYNC_STATES.read().await;
    states
        .values()
        .filter(|state| state.status == LibrarySyncRunStatus::Running)
        .count()
}

pub(crate) async fn wait_for_running_library_syncs() {
    loop {
        if running_library_sync_count().await == 0 {
            break;
        }
        sleep(Duration::from_millis(250)).await;
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn sync_status_serialization_omits_internal_ids_and_global_stage() -> anyhow::Result<()> {
        let state = LibrarySyncState {
            library_db_id: 42,
            run_id: Some(7),
            status: LibrarySyncRunStatus::Running,
            phase: Some(LibrarySyncPhase::ProviderRefresh),
            stage: Some(LibrarySyncStage::Lyrics),
            pending_groups: 2,
            active_group_count: 1,
            completed_group_count: 3,
            failed_group_count: 0,
            counters: LibrarySyncCounters {
                entry_groups: 4,
                entries_added: 5,
                entries_updated: 6,
                entries_deleted: 1,
                discovered_groups: 6,
                completed_groups: 3,
                failed_groups: 0,
                releases: 2,
                tracks: 20,
                provider_refreshes: 4,
                lyrics_tracks: 12,
                local_cover_metadata: 2,
                provider_covers: 1,
            },
            active: vec![LibrarySyncActiveItem {
                group_id: 99,
                source_dir: Some("/music/artist/album".to_owned()),
                release_db_id: Some(123),
                release_title: Some("Album".to_owned()),
                stage: LibrarySyncStage::Lyrics,
                provider_id: Some("lyrics-provider".to_owned()),
                stage_started_at: 100,
            }],
            cleanup_stage: None,
            started_at: Some(10),
            updated_at: Some(20),
            finished_at: None,
            error: None,
        };

        let value = serde_json::to_value(LibrarySyncStatus::from(state))?;
        let text = serde_json::to_string(&value)?;

        assert!(!text.contains("db_id"));
        assert!(!text.contains("group_id"));
        assert!(value.get("phase").is_none());
        assert!(value.get("stage").is_none());
        assert_eq!(
            value["progress"]["work"]["active_by_stage"],
            json!({ "lyrics": 1 })
        );
        assert_eq!(value["changes"]["entries"]["added"], 5);
        assert_eq!(value["changes"]["library"]["tracks"], 20);
        assert_eq!(value["changes"]["providers"]["metadata_refreshes"], 4);
        assert!(
            value["changes"]["providers"]
                .get("provider_refreshes")
                .is_none()
        );
        assert_eq!(value["changes"]["covers"]["hashed"], 2);
        assert_eq!(value["changes"]["covers"]["synced"], 1);
        assert_eq!(value["active"][0]["release_title"], "Album");
        assert_eq!(
            value["active"][0]["stage_started_at"],
            "1970-01-01T00:01:40Z"
        );
        assert_eq!(value["run"]["started_at"], "1970-01-01T00:00:10Z");

        Ok(())
    }
}
