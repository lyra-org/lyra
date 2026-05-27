// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        BTreeMap,
        HashMap,
        VecDeque,
    },
    sync::{
        Arc,
        LazyLock,
    },
    time::{
        Instant,
        SystemTime,
        UNIX_EPOCH,
    },
};

use agdb::DbId;
use anyhow::Context;
use nanoid::nanoid;
use serde::{
    Deserialize,
    Serialize,
};
use tokio::sync::{
    Mutex,
    RwLock,
    broadcast,
};
use tokio::time::{
    Duration,
    sleep,
};

use crate::{
    STATE,
    db::{
        self,
        DbAsync,
        Library,
    },
    routes::unix_ms_to_rfc3339_u64,
    services::providers::LibraryRefreshOptions,
};

use super::sync::{
    MAX_CONCURRENT_ALBUM_PIPELINE,
    sync_library_pipeline,
};

const MAX_ACTIVE_SYNC_ITEMS: usize = 16;
const MAX_SYNC_EVENT_LOG_ITEMS: usize = 256;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SyncRunKind {
    LibrarySync,
    LibraryRefresh,
}

impl SyncRunKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::LibrarySync => "library_sync",
            Self::LibraryRefresh => "library_refresh",
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SyncRunStatus {
    Idle,
    Queued,
    Planning,
    Running,
    Cancelling,
    Cancelled,
    Succeeded,
    Failed,
}

impl SyncRunStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Queued => "queued",
            Self::Planning => "planning",
            Self::Running => "running",
            Self::Cancelling => "cancelling",
            Self::Cancelled => "cancelled",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }

    fn is_active(self) -> bool {
        matches!(
            self,
            Self::Queued | Self::Planning | Self::Running | Self::Cancelling
        )
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SyncProgressMode {
    Indeterminate,
    Estimating,
    Determinate,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SyncTotalState {
    Discovering,
    Estimated,
    Final,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SyncStageStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SyncStageKey {
    Discover,
    EntrySync,
    MetadataParse,
    MetadataApply,
    ProviderRefresh,
    LocalCoverMetadata,
    Lyrics,
    ProviderCover,
    Cleanup,
}

impl SyncStageKey {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Discover => "discover",
            Self::EntrySync => "entry_sync",
            Self::MetadataParse => "metadata_parse",
            Self::MetadataApply => "metadata_apply",
            Self::ProviderRefresh => "provider_refresh",
            Self::LocalCoverMetadata => "local_cover_metadata",
            Self::Lyrics => "lyrics",
            Self::ProviderCover => "provider_cover",
            Self::Cleanup => "cleanup",
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SyncRunSnapshot {
    pub(crate) run: SyncRunInfo,
    pub(crate) progress: SyncRunProgressSnapshot,
    pub(crate) stages: Vec<SyncStageSnapshot>,
    pub(crate) active: Vec<SyncActiveWork>,
    pub(crate) failures: Vec<SyncRunFailure>,
    pub(crate) counters: SyncRunCounters,
    pub(crate) concurrency: SyncRunConcurrency,
    pub(crate) sequence: u64,
}

pub(crate) type LibrarySyncStatus = SyncRunSnapshot;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SyncRunInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<String>,
    pub(crate) kind: SyncRunKind,
    pub(crate) library_id: String,
    pub(crate) status: SyncRunStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) updated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) finished_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(crate) cancellation_requested: bool,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SyncRunProgressSnapshot {
    pub(crate) mode: SyncProgressMode,
    pub(crate) total_state: SyncTotalState,
    pub(crate) completed_units: u64,
    pub(crate) failed_units: u64,
    pub(crate) skipped_units: u64,
    pub(crate) total_units: u64,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SyncStageSnapshot {
    pub(crate) key: SyncStageKey,
    pub(crate) status: SyncStageStatus,
    pub(crate) total_state: SyncTotalState,
    pub(crate) completed_units: u64,
    pub(crate) failed_units: u64,
    pub(crate) skipped_units: u64,
    pub(crate) active_units: u64,
    pub(crate) total_units: u64,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct SyncRunCounters {
    pub(crate) entries_added: u64,
    pub(crate) entries_updated: u64,
    pub(crate) entries_deleted: u64,
    pub(crate) releases_touched: u64,
    pub(crate) tracks_touched: u64,
    pub(crate) provider_refreshes: u64,
    pub(crate) lyrics_tracks: u64,
    pub(crate) local_covers_synced: u64,
    pub(crate) provider_covers_synced: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SyncRunCounterDelta {
    pub(crate) entries_added: u64,
    pub(crate) entries_updated: u64,
    pub(crate) entries_deleted: u64,
    pub(crate) releases_touched: u64,
    pub(crate) tracks_touched: u64,
    pub(crate) provider_refreshes: u64,
    pub(crate) lyrics_tracks: u64,
    pub(crate) local_covers_synced: u64,
    pub(crate) provider_covers_synced: u64,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SyncRunConcurrency {
    pub(crate) active_units: u64,
    pub(crate) queued_units: u64,
    pub(crate) max_active_units: u64,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct SyncWorkDetails {
    #[serde(rename = "type")]
    pub(crate) work_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) entity_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) entity_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) entity_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) provider_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) source_dir: Option<String>,
}

impl SyncWorkDetails {
    pub(crate) fn new(work_type: impl Into<String>) -> Self {
        Self {
            work_type: work_type.into(),
            ..Default::default()
        }
    }

    pub(crate) fn release(
        work_type: impl Into<String>,
        release_id: Option<String>,
        release_title: Option<String>,
    ) -> Self {
        Self {
            work_type: work_type.into(),
            entity_type: Some("release".to_string()),
            entity_id: release_id,
            entity_title: release_title,
            provider_id: None,
            source_dir: None,
        }
    }

    pub(crate) fn provider(mut self, provider_id: impl Into<String>) -> Self {
        self.provider_id = Some(provider_id.into());
        self
    }

    pub(crate) fn source_dir(mut self, source_dir: Option<String>) -> Self {
        self.source_dir = source_dir;
        self
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SyncActiveWork {
    #[serde(skip, default)]
    work_id: u64,
    pub(crate) stage: SyncStageKey,
    #[serde(flatten)]
    pub(crate) details: SyncWorkDetails,
    pub(crate) started_at: String,
    pub(crate) elapsed_ms: u64,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SyncRunFailure {
    pub(crate) stage: SyncStageKey,
    #[serde(rename = "type")]
    pub(crate) work_type: String,
    pub(crate) message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) provider_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) entity_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) entity_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) entity_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) elapsed_ms: Option<u64>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SyncRunEvent {
    pub(crate) run_id: String,
    pub(crate) sequence: u64,
    pub(crate) event: SyncRunEventKind,
    pub(crate) snapshot: SyncRunSnapshot,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SyncRunEventKind {
    Snapshot,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SyncStageState {
    status: SyncStageStatus,
    total_state: SyncTotalState,
    completed_units: u64,
    failed_units: u64,
    skipped_units: u64,
    active_units: u64,
    total_units: u64,
}

impl SyncStageState {
    fn new(total_state: SyncTotalState) -> Self {
        Self {
            status: SyncStageStatus::Pending,
            total_state,
            completed_units: 0,
            failed_units: 0,
            skipped_units: 0,
            active_units: 0,
            total_units: 0,
        }
    }

    fn processed_units(&self) -> u64 {
        self.completed_units
            .saturating_add(self.failed_units)
            .saturating_add(self.skipped_units)
    }

    fn queued_units(&self) -> u64 {
        self.total_units
            .saturating_sub(self.processed_units())
            .saturating_sub(self.active_units)
    }

    fn percent(&self) -> Option<f64> {
        percent(self.processed_units(), self.total_units)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SyncRunState {
    run_id: String,
    kind: SyncRunKind,
    library_id: String,
    status: SyncRunStatus,
    progress_mode: SyncProgressMode,
    total_state: SyncTotalState,
    stages: BTreeMap<SyncStageKey, SyncStageState>,
    active: Vec<SyncActiveWork>,
    failures: Vec<SyncRunFailure>,
    counters: SyncRunCounters,
    started_at_ms: u64,
    updated_at_ms: u64,
    finished_at_ms: Option<u64>,
    error: Option<String>,
    cancellation_requested: bool,
    sequence: u64,
    next_work_id: u64,
}

impl SyncRunState {
    fn new(kind: SyncRunKind, library_id: String, now_ms: u64) -> Self {
        Self {
            run_id: nanoid!(),
            kind,
            library_id,
            status: SyncRunStatus::Queued,
            progress_mode: SyncProgressMode::Indeterminate,
            total_state: SyncTotalState::Discovering,
            stages: BTreeMap::new(),
            active: Vec::new(),
            failures: Vec::new(),
            counters: SyncRunCounters::default(),
            started_at_ms: now_ms,
            updated_at_ms: now_ms,
            finished_at_ms: None,
            error: None,
            cancellation_requested: false,
            sequence: 0,
            next_work_id: 1,
        }
    }

    fn idle(library_id: String) -> Self {
        let now_ms = now_unix_ms();
        Self {
            run_id: String::new(),
            kind: SyncRunKind::LibrarySync,
            library_id,
            status: SyncRunStatus::Idle,
            progress_mode: SyncProgressMode::Indeterminate,
            total_state: SyncTotalState::Discovering,
            stages: BTreeMap::new(),
            active: Vec::new(),
            failures: Vec::new(),
            counters: SyncRunCounters::default(),
            started_at_ms: now_ms,
            updated_at_ms: now_ms,
            finished_at_ms: None,
            error: None,
            cancellation_requested: false,
            sequence: 0,
            next_work_id: 1,
        }
    }

    fn snapshot(&self) -> SyncRunSnapshot {
        let now_ms = now_unix_ms();
        let active_by_stage = {
            let mut counts = BTreeMap::new();
            for item in &self.active {
                *counts.entry(item.stage.as_str().to_string()).or_insert(0) += 1;
            }
            counts
        };
        let stages = self
            .stages
            .iter()
            .map(|(&key, stage)| SyncStageSnapshot {
                key,
                status: stage.status,
                total_state: stage.total_state,
                percent: stage.percent(),
                processed_units: stage.processed_units(),
                completed_units: stage.completed_units,
                failed_units: stage.failed_units,
                skipped_units: stage.skipped_units,
                active_units: stage.active_units,
                queued_units: stage.queued_units(),
                total_units: stage.total_units,
            })
            .collect::<Vec<_>>();
        let completed_units: u64 = stages.iter().map(|stage| stage.completed_units).sum();
        let failed_units: u64 = stages.iter().map(|stage| stage.failed_units).sum();
        let skipped_units: u64 = stages.iter().map(|stage| stage.skipped_units).sum();
        let processed_units = completed_units + failed_units + skipped_units;
        let total_units: u64 = stages.iter().map(|stage| stage.total_units).sum();
        let active_units: u64 = stages.iter().map(|stage| stage.active_units).sum();
        let queued_units = total_units
            .saturating_sub(processed_units)
            .saturating_sub(active_units);

        SyncRunSnapshot {
            run: SyncRunInfo {
                id: (!self.run_id.is_empty()).then(|| self.run_id.clone()),
                kind: self.kind,
                library_id: self.library_id.clone(),
                status: self.status,
                started_at: (self.status != SyncRunStatus::Idle)
                    .then(|| unix_ms_to_rfc3339_u64(self.started_at_ms)),
                updated_at: (self.status != SyncRunStatus::Idle)
                    .then(|| unix_ms_to_rfc3339_u64(self.updated_at_ms)),
                finished_at: self.finished_at_ms.map(unix_ms_to_rfc3339_u64),
                error: self.error.clone(),
                cancellation_requested: self.cancellation_requested,
            },
            progress: SyncRunProgressSnapshot {
                mode: self.progress_mode,
                total_state: self.total_state,
                percent: (self.progress_mode == SyncProgressMode::Determinate)
                    .then(|| percent(processed_units, total_units))
                    .flatten(),
                processed_units,
                completed_units,
                failed_units,
                skipped_units,
                total_units,
            },
            stages,
            active: self
                .active
                .iter()
                .cloned()
                .map(|mut item| {
                    item.elapsed_ms = now_ms
                        .saturating_sub(rfc3339_ms(&item.started_at).unwrap_or(self.updated_at_ms));
                    item
                })
                .collect(),
            failures: self.failures.clone(),
            counters: self.counters.clone(),
            concurrency: SyncRunConcurrency {
                active_units,
                queued_units,
                processed_units,
                failed_units,
                max_active_units: MAX_CONCURRENT_ALBUM_PIPELINE as u64,
                active_by_stage,
            },
            sequence: self.sequence,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SyncRunControlError {
    #[error("sync run cancelled")]
    Cancelled,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub(crate) struct SyncRunStartResponse {
    pub(crate) started: bool,
    pub(crate) run: SyncRunSnapshot,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct LibraryRefreshRunOptions {
    pub(crate) replace_cover: bool,
    pub(crate) force_refresh: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct SyncRunProgress {
    db: DbAsync,
    run_id: String,
}

impl SyncRunProgress {
    fn new(db: DbAsync, run_id: String) -> Self {
        Self { db, run_id }
    }

    pub(crate) fn run_id(&self) -> &str {
        &self.run_id
    }

    pub(crate) async fn set_status(&self, status: SyncRunStatus) {
        self.mutate(|state| {
            state.status = status;
        })
        .await;
    }

    pub(crate) async fn set_estimating(&self) {
        self.mutate(|state| {
            state.progress_mode = SyncProgressMode::Estimating;
            state.total_state = SyncTotalState::Estimated;
        })
        .await;
    }

    pub(crate) async fn set_determinate(&self) {
        self.mutate(|state| {
            state.progress_mode = SyncProgressMode::Determinate;
            state.total_state = SyncTotalState::Final;
            for stage in state.stages.values_mut() {
                if stage.total_state != SyncTotalState::Final {
                    stage.total_state = SyncTotalState::Final;
                }
            }
        })
        .await;
    }

    pub(crate) async fn add_stage_total(
        &self,
        stage: SyncStageKey,
        units: u64,
        total_state: SyncTotalState,
    ) {
        if units == 0 {
            return;
        }
        self.mutate(|state| {
            let stage_total_state = if state.progress_mode == SyncProgressMode::Determinate {
                SyncTotalState::Final
            } else {
                total_state
            };
            let stage_state = state
                .stages
                .entry(stage)
                .or_insert_with(|| SyncStageState::new(stage_total_state));
            stage_state.total_units = stage_state.total_units.saturating_add(units);
            stage_state.total_state = stage_total_state;
            if stage_state.status == SyncStageStatus::Succeeded {
                stage_state.status = SyncStageStatus::Pending;
            }
            if state.progress_mode == SyncProgressMode::Indeterminate {
                state.progress_mode = SyncProgressMode::Estimating;
                state.total_state = SyncTotalState::Estimated;
            }
        })
        .await;
    }

    pub(crate) async fn start_work(&self, stage: SyncStageKey, details: SyncWorkDetails) -> u64 {
        let mut work_id = 0;
        self.mutate(|state| {
            let now_ms = now_unix_ms();
            let stage_state = state
                .stages
                .entry(stage)
                .or_insert_with(|| SyncStageState::new(state.total_state));
            stage_state.status = SyncStageStatus::Running;
            stage_state.active_units = stage_state.active_units.saturating_add(1);
            work_id = state.next_work_id;
            state.next_work_id = state.next_work_id.saturating_add(1);
            if state.active.len() < MAX_ACTIVE_SYNC_ITEMS {
                state.active.push(SyncActiveWork {
                    work_id,
                    stage,
                    details,
                    started_at: unix_ms_to_rfc3339_u64(now_ms),
                    elapsed_ms: 0,
                });
            }
        })
        .await;
        work_id
    }

    pub(crate) async fn complete_work(&self, work_id: u64, stage: SyncStageKey) {
        self.finish_work(work_id, stage, WorkOutcome::Complete)
            .await;
    }

    pub(crate) async fn skip_work(
        &self,
        work_id: u64,
        stage: SyncStageKey,
        reason: impl Into<String>,
    ) {
        self.finish_work(work_id, stage, WorkOutcome::Skip(reason.into()))
            .await;
    }

    pub(crate) async fn fail_work(
        &self,
        work_id: u64,
        stage: SyncStageKey,
        details: SyncWorkDetails,
        message: impl Into<String>,
    ) {
        self.finish_work(work_id, stage, WorkOutcome::Fail(details, message.into()))
            .await;
    }

    async fn finish_work(&self, work_id: u64, stage: SyncStageKey, outcome: WorkOutcome) {
        self.mutate(|state| {
            let removed = state
                .active
                .iter()
                .position(|item| item.work_id == work_id)
                .map(|index| state.active.remove(index));
            let stage_state = state
                .stages
                .entry(stage)
                .or_insert_with(|| SyncStageState::new(state.total_state));
            stage_state.active_units = stage_state.active_units.saturating_sub(1);
            match outcome {
                WorkOutcome::Complete => {
                    stage_state.completed_units = stage_state.completed_units.saturating_add(1);
                }
                WorkOutcome::Skip(reason) => {
                    stage_state.skipped_units = stage_state.skipped_units.saturating_add(1);
                    if !reason.is_empty() {
                        tracing::debug!(run_id = %state.run_id, stage = stage.as_str(), reason, "sync work skipped");
                    }
                }
                WorkOutcome::Fail(details, message) => {
                    stage_state.failed_units = stage_state.failed_units.saturating_add(1);
                    let elapsed_ms = removed
                        .as_ref()
                        .and_then(|item| rfc3339_ms(&item.started_at))
                        .map(|started| now_unix_ms().saturating_sub(started));
                    state.failures.push(SyncRunFailure {
                        stage,
                        work_type: details.work_type,
                        message,
                        provider_id: details.provider_id,
                        entity_type: details.entity_type,
                        entity_id: details.entity_id,
                        entity_title: details.entity_title,
                        elapsed_ms,
                    });
                }
            }
            if stage_state.total_units > 0
                && stage_state.processed_units() >= stage_state.total_units
                && stage_state.active_units == 0
            {
                stage_state.status = if stage_state.failed_units == 0 {
                    SyncStageStatus::Succeeded
                } else {
                    SyncStageStatus::Failed
                };
            }
        })
        .await;
    }

    pub(crate) async fn add_counts(&self, delta: SyncRunCounterDelta) {
        self.mutate(|state| {
            state.counters.entries_added = state
                .counters
                .entries_added
                .saturating_add(delta.entries_added);
            state.counters.entries_updated = state
                .counters
                .entries_updated
                .saturating_add(delta.entries_updated);
            state.counters.entries_deleted = state
                .counters
                .entries_deleted
                .saturating_add(delta.entries_deleted);
            state.counters.releases_touched = state
                .counters
                .releases_touched
                .saturating_add(delta.releases_touched);
            state.counters.tracks_touched = state
                .counters
                .tracks_touched
                .saturating_add(delta.tracks_touched);
            state.counters.provider_refreshes = state
                .counters
                .provider_refreshes
                .saturating_add(delta.provider_refreshes);
            state.counters.lyrics_tracks = state
                .counters
                .lyrics_tracks
                .saturating_add(delta.lyrics_tracks);
            state.counters.local_covers_synced = state
                .counters
                .local_covers_synced
                .saturating_add(delta.local_covers_synced);
            state.counters.provider_covers_synced = state
                .counters
                .provider_covers_synced
                .saturating_add(delta.provider_covers_synced);
        })
        .await;
    }

    pub(crate) async fn check_cancelled(&self) -> Result<(), SyncRunControlError> {
        let states = SYNC_RUN_STATES.read().await;
        let Some(state) = states.get(&self.run_id) else {
            return Ok(());
        };
        if state.cancellation_requested || state.status == SyncRunStatus::Cancelling {
            Err(SyncRunControlError::Cancelled)
        } else {
            Ok(())
        }
    }

    async fn mutate<F>(&self, update: F)
    where
        F: FnOnce(&mut SyncRunState),
    {
        let snapshot = {
            let mut states = SYNC_RUN_STATES.write().await;
            let Some(state) = states.get_mut(&self.run_id) else {
                return;
            };
            if !state.status.is_active() {
                return;
            }
            update(state);
            state.updated_at_ms = now_unix_ms();
            state.sequence = state.sequence.saturating_add(1);
            state.snapshot()
        };
        persist_snapshot(&self.db, &snapshot).await;
        publish_event(snapshot).await;
    }
}

enum WorkOutcome {
    Complete,
    Skip(String),
    Fail(SyncWorkDetails, String),
}

static SYNC_RUN_STATES: LazyLock<Arc<RwLock<HashMap<String, SyncRunState>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));
static SYNC_EVENT_LOGS: LazyLock<Arc<RwLock<HashMap<String, VecDeque<SyncRunEvent>>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));
static SYNC_START_LOCK: LazyLock<Arc<Mutex<()>>> = LazyLock::new(|| Arc::new(Mutex::new(())));
static SYNC_EVENT_TX: LazyLock<broadcast::Sender<SyncRunEvent>> = LazyLock::new(|| {
    let (tx, _) = broadcast::channel(512);
    tx
});

pub(crate) async fn reset_sync_states_for_test() {
    SYNC_RUN_STATES.write().await.clear();
    SYNC_EVENT_LOGS.write().await.clear();
}

async fn publish_event(snapshot: SyncRunSnapshot) {
    let Some(run_id) = snapshot.run.id.clone() else {
        return;
    };
    let event = SyncRunEvent {
        run_id: run_id.clone(),
        sequence: snapshot.sequence,
        event: SyncRunEventKind::Snapshot,
        snapshot,
    };
    {
        let mut logs = SYNC_EVENT_LOGS.write().await;
        let log = logs.entry(run_id).or_default();
        log.push_back(event.clone());
        while log.len() > MAX_SYNC_EVENT_LOG_ITEMS {
            log.pop_front();
        }
    }
    let _ = SYNC_EVENT_TX.send(event);
}

pub(crate) async fn sync_run_events_after(run_id: &str, after: u64) -> Vec<SyncRunEvent> {
    let logs = SYNC_EVENT_LOGS.read().await;
    logs.get(run_id)
        .into_iter()
        .flat_map(|events| events.iter())
        .filter(|event| event.sequence > after)
        .cloned()
        .collect()
}

pub(crate) fn subscribe_sync_run_events() -> broadcast::Receiver<SyncRunEvent> {
    SYNC_EVENT_TX.subscribe()
}

pub(crate) async fn get_sync_run(run_id: &str) -> anyhow::Result<Option<SyncRunSnapshot>> {
    reconcile_interrupted_runs(&STATE.db.get()).await?;
    if let Some(snapshot) = {
        let states = SYNC_RUN_STATES.read().await;
        states.get(run_id).map(SyncRunState::snapshot)
    } {
        return Ok(Some(snapshot));
    }
    let db = STATE.db.read().await;
    db::sync_runs::get_by_id(&db, run_id)?
        .map(record_to_snapshot)
        .transpose()
}

pub(crate) async fn cancel_sync_run(run_id: &str) -> anyhow::Result<Option<SyncRunSnapshot>> {
    reconcile_interrupted_runs(&STATE.db.get()).await?;
    let snapshot = {
        let mut states = SYNC_RUN_STATES.write().await;
        let Some(state) = states.get_mut(run_id) else {
            drop(states);
            return get_sync_run(run_id).await;
        };
        if state.status.is_active() {
            state.status = SyncRunStatus::Cancelling;
            state.cancellation_requested = true;
            state.updated_at_ms = now_unix_ms();
            state.sequence = state.sequence.saturating_add(1);
        }
        state.snapshot()
    };
    persist_snapshot(&STATE.db.get(), &snapshot).await;
    publish_event(snapshot.clone()).await;
    Ok(Some(snapshot))
}

pub(crate) async fn start_library_sync(
    db: DbAsync,
    library: Library,
) -> anyhow::Result<SyncRunStartResponse> {
    start_library_run(db, library, SyncRunKind::LibrarySync, None).await
}

pub(crate) async fn start_library_refresh(
    db: DbAsync,
    library: Library,
    options: LibraryRefreshRunOptions,
) -> anyhow::Result<SyncRunStartResponse> {
    start_library_run(db, library, SyncRunKind::LibraryRefresh, Some(options)).await
}

async fn start_library_run(
    db: DbAsync,
    library: Library,
    kind: SyncRunKind,
    refresh_options: Option<LibraryRefreshRunOptions>,
) -> anyhow::Result<SyncRunStartResponse> {
    let _start_guard = SYNC_START_LOCK.lock().await;
    reconcile_interrupted_runs(&db).await?;
    let library_id = library.id.clone();
    if let Some(existing) = active_run_for_library(&db, &library_id).await? {
        return Ok(SyncRunStartResponse {
            started: false,
            run: existing,
        });
    }

    let now_ms = now_unix_ms();
    let state = SyncRunState::new(kind, library_id, now_ms);
    let run_id = state.run_id.clone();
    let snapshot = state.snapshot();
    {
        let mut states = SYNC_RUN_STATES.write().await;
        states.insert(run_id.clone(), state);
    }
    persist_snapshot(&db, &snapshot).await;
    publish_event(snapshot.clone()).await;

    let progress = SyncRunProgress::new(db.clone(), run_id.clone());
    match kind {
        SyncRunKind::LibrarySync => {
            tokio::spawn(run_library_sync(db, library, progress));
        }
        SyncRunKind::LibraryRefresh => {
            let options = refresh_options.context("missing library refresh options")?;
            tokio::spawn(run_library_refresh(library, progress, options));
        }
    }

    Ok(SyncRunStartResponse {
        started: true,
        run: snapshot,
    })
}

async fn run_library_sync(db: DbAsync, library: Library, progress: SyncRunProgress) {
    let started = Instant::now();
    tracing::info!(
        run_id = progress.run_id(),
        library_public_id = %library.id,
        library_name = %library.name,
        album_concurrency = MAX_CONCURRENT_ALBUM_PIPELINE,
        "library sync started"
    );
    progress.set_status(SyncRunStatus::Planning).await;
    let result = async {
        sync_library_pipeline(&db, &library, Some(progress.clone())).await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    finish_run(progress, result, started, "library sync").await;
}

async fn run_library_refresh(
    library: Library,
    progress: SyncRunProgress,
    options: LibraryRefreshRunOptions,
) {
    let started = Instant::now();
    let Some(library_db_id) = library.db_id else {
        finish_run(
            progress,
            Err(anyhow::anyhow!("library missing db_id")),
            started,
            "library refresh",
        )
        .await;
        return;
    };
    tracing::info!(
        run_id = progress.run_id(),
        library_public_id = %library.id,
        library_name = %library.name,
        "library refresh started"
    );
    progress.set_status(SyncRunStatus::Planning).await;
    let refresh_options = LibraryRefreshOptions {
        replace_cover: options.replace_cover,
        force_refresh: options.force_refresh,
        apply_sync_filters: false,
        provider_id: None,
    };
    let result = crate::services::providers::refresh_library_metadata_with_progress(
        library_db_id,
        &refresh_options,
        Some(progress.clone()),
    )
    .await
    .map(|_| ())
    .map_err(anyhow::Error::from);
    finish_run(progress, result, started, "library refresh").await;
}

async fn finish_run(
    progress: SyncRunProgress,
    result: anyhow::Result<()>,
    started: Instant,
    label: &'static str,
) {
    match result {
        Ok(()) => {
            let snapshot = mark_terminal(
                &progress.db,
                progress.run_id(),
                SyncRunStatus::Succeeded,
                None,
            )
            .await;
            if let Some(snapshot) = snapshot {
                tracing::info!(
                    run_id = %progress.run_id(),
                    status = ?snapshot.run.status,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    processed_units = snapshot.progress.processed_units,
                    total_units = snapshot.progress.total_units,
                    failed_units = snapshot.progress.failed_units,
                    "{label} completed"
                );
            }
        }
        Err(err) if err.downcast_ref::<SyncRunControlError>().is_some() => {
            let snapshot = mark_terminal(
                &progress.db,
                progress.run_id(),
                SyncRunStatus::Cancelled,
                None,
            )
            .await;
            if snapshot.is_some() {
                tracing::info!(
                    run_id = %progress.run_id(),
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "{label} cancelled"
                );
            }
        }
        Err(err) => {
            let error = err.to_string();
            let snapshot = mark_terminal(
                &progress.db,
                progress.run_id(),
                SyncRunStatus::Failed,
                Some(error.clone()),
            )
            .await;
            if snapshot.is_some() {
                tracing::error!(
                    run_id = %progress.run_id(),
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    error = %error,
                    "{label} failed"
                );
            }
        }
    }
}

async fn mark_terminal(
    db: &DbAsync,
    run_id: &str,
    status: SyncRunStatus,
    error: Option<String>,
) -> Option<SyncRunSnapshot> {
    let snapshot = {
        let mut states = SYNC_RUN_STATES.write().await;
        let state = states.get_mut(run_id)?;
        let now_ms = now_unix_ms();
        state.status = status;
        state.finished_at_ms = Some(now_ms);
        state.updated_at_ms = now_ms;
        state.error = error;
        state.active.clear();
        state.cancellation_requested = false;
        for stage in state.stages.values_mut() {
            let had_active_units = stage.active_units > 0;
            if stage.active_units > 0 {
                stage.active_units = 0;
            }
            if status == SyncRunStatus::Cancelled {
                if stage.total_units > 0 && stage.processed_units() >= stage.total_units {
                    stage.status = if stage.failed_units == 0 {
                        SyncStageStatus::Succeeded
                    } else {
                        SyncStageStatus::Failed
                    };
                } else if had_active_units
                    || stage.queued_units() > 0
                    || matches!(
                        stage.status,
                        SyncStageStatus::Pending | SyncStageStatus::Running
                    )
                {
                    stage.status = SyncStageStatus::Cancelled;
                }
            } else if stage.total_units > 0 && stage.processed_units() < stage.total_units {
                stage.status = if status == SyncRunStatus::Failed {
                    SyncStageStatus::Failed
                } else {
                    SyncStageStatus::Succeeded
                };
                let remaining = stage.total_units.saturating_sub(stage.processed_units());
                stage.skipped_units = stage.skipped_units.saturating_add(remaining);
            }
        }
        state.sequence = state.sequence.saturating_add(1);
        state.snapshot()
    };
    persist_snapshot(db, &snapshot).await;
    publish_event(snapshot.clone()).await;
    SYNC_RUN_STATES.write().await.remove(run_id);
    Some(snapshot)
}

pub(crate) async fn get_library_sync_status(library_db_id: DbId) -> SyncRunSnapshot {
    let db = STATE.db.get();
    if let Err(err) = reconcile_interrupted_runs(&db).await {
        tracing::warn!(error = %err, "failed to reconcile interrupted sync runs");
    }
    let library = {
        let db_read = db.read().await;
        db::libraries::get_by_id(&db_read, library_db_id)
            .ok()
            .flatten()
    };
    let Some(library) = library else {
        return SyncRunState::idle(String::new()).snapshot();
    };
    match active_or_latest_run_for_library(&db, &library.id).await {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => SyncRunState::idle(library.id).snapshot(),
        Err(err) => {
            tracing::warn!(library_id = %library.id, error = %err, "failed to load library sync run");
            SyncRunState::idle(library.id).snapshot()
        }
    }
}

async fn active_or_latest_run_for_library(
    db: &DbAsync,
    library_id: &str,
) -> anyhow::Result<Option<SyncRunSnapshot>> {
    if let Some(active) = active_run_for_library(db, library_id).await? {
        return Ok(Some(active));
    }
    let db_read = db.read().await;
    db::sync_runs::latest_for_library(&db_read, library_id)?
        .map(record_to_snapshot)
        .transpose()
}

async fn active_run_for_library(
    db: &DbAsync,
    library_id: &str,
) -> anyhow::Result<Option<SyncRunSnapshot>> {
    if let Some(snapshot) = {
        let states = SYNC_RUN_STATES.read().await;
        states
            .values()
            .find(|state| state.library_id == library_id && state.status.is_active())
            .map(SyncRunState::snapshot)
    } {
        return Ok(Some(snapshot));
    }
    let db_read = db.read().await;
    db::sync_runs::active_for_library(&db_read, library_id)?
        .map(record_to_snapshot)
        .transpose()
}

pub(crate) async fn running_library_sync_count() -> usize {
    SYNC_RUN_STATES
        .read()
        .await
        .values()
        .filter(|state| state.status.is_active())
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

async fn reconcile_interrupted_runs(db: &DbAsync) -> anyhow::Result<()> {
    let live_run_ids = {
        let states = SYNC_RUN_STATES.read().await;
        states.keys().cloned().collect::<Vec<_>>()
    };
    let mut db_write = db.write().await;
    let records = db::sync_runs::list(&db_write)?;
    for record in records {
        if live_run_ids.iter().any(|id| id == &record.id) {
            continue;
        }
        if !matches!(
            record.status.as_str(),
            "queued" | "planning" | "running" | "cancelling"
        ) {
            continue;
        }
        let mut snapshot = record_to_snapshot(record.clone())?;
        snapshot.run.status = SyncRunStatus::Failed;
        snapshot.run.finished_at = Some(unix_ms_to_rfc3339_u64(now_unix_ms()));
        snapshot.run.updated_at = snapshot.run.finished_at.clone();
        snapshot.run.error = Some("server stopped before run completed".to_string());
        let updated = snapshot_to_record(record.db_id, &snapshot)?;
        db::sync_runs::update(&mut db_write, &updated)?;
    }
    Ok(())
}

async fn persist_snapshot(db: &DbAsync, snapshot: &SyncRunSnapshot) {
    let Some(run_id) = snapshot.run.id.as_deref() else {
        return;
    };
    let result = async {
        let mut db_write = db.write().await;
        let existing = db::sync_runs::get_by_id(&db_write, run_id)?;
        let record = snapshot_to_record(existing.as_ref().and_then(|r| r.db_id), snapshot)?;
        if existing.is_some() {
            db::sync_runs::update(&mut db_write, &record)?;
        } else {
            db::sync_runs::create(&mut db_write, &record)?;
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;
    if let Err(err) = result {
        tracing::warn!(run_id, error = %err, "failed to persist sync run snapshot");
    }
}

fn snapshot_to_record(
    db_id: Option<DbId>,
    snapshot: &SyncRunSnapshot,
) -> anyhow::Result<db::sync_runs::SyncRunRecord> {
    let run_id = snapshot
        .run
        .id
        .clone()
        .ok_or_else(|| anyhow::anyhow!("cannot persist idle sync run"))?;
    let created_at_ms = snapshot
        .run
        .started_at
        .as_deref()
        .and_then(rfc3339_ms)
        .unwrap_or_else(now_unix_ms);
    let updated_at_ms = snapshot
        .run
        .updated_at
        .as_deref()
        .and_then(rfc3339_ms)
        .unwrap_or(created_at_ms);
    let finished_at_ms = snapshot.run.finished_at.as_deref().and_then(rfc3339_ms);
    Ok(db::sync_runs::SyncRunRecord {
        db_id,
        id: run_id,
        library_id: snapshot.run.library_id.clone(),
        kind: snapshot.run.kind.as_str().to_string(),
        status: snapshot.run.status.as_str().to_string(),
        created_at_ms,
        updated_at_ms,
        finished_at_ms,
        snapshot_json: serde_json::to_string(snapshot)?,
    })
}

fn record_to_snapshot(record: db::sync_runs::SyncRunRecord) -> anyhow::Result<SyncRunSnapshot> {
    let snapshot = serde_json::from_str(&record.snapshot_json)?;
    Ok(snapshot)
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn percent(processed: u64, total: u64) -> Option<f64> {
    if total == 0 {
        return Some(100.0);
    }
    Some(((processed as f64 / total as f64) * 1000.0).round() / 10.0)
}

fn rfc3339_ms(value: &str) -> Option<u64> {
    let parsed =
        time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).ok()?;
    let millis = parsed.unix_timestamp_nanos().checked_div(1_000_000)?;
    u64::try_from(millis).ok()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn snapshot_uses_backend_owned_determinate_percent() {
        let mut state = SyncRunState::new(SyncRunKind::LibrarySync, "lib".to_string(), 10_000);
        state.status = SyncRunStatus::Running;
        state.progress_mode = SyncProgressMode::Determinate;
        state.total_state = SyncTotalState::Final;
        state.stages.insert(
            SyncStageKey::ProviderRefresh,
            SyncStageState {
                status: SyncStageStatus::Running,
                total_state: SyncTotalState::Final,
                completed_units: 3,
                failed_units: 1,
                skipped_units: 1,
                active_units: 1,
                total_units: 10,
            },
        );

        let value = serde_json::to_value(state.snapshot()).expect("serialize snapshot");
        assert_eq!(value["progress"]["percent"], json!(50.0));
        assert_eq!(value["progress"]["processed_units"], json!(5));
        assert_eq!(value["progress"]["total_state"], json!("final"));
        assert_eq!(value["stages"][0]["queued_units"], json!(4));
    }

    #[test]
    fn idle_snapshot_does_not_claim_a_percent() {
        let snapshot = SyncRunState::idle("lib".to_string()).snapshot();
        assert_eq!(snapshot.run.status, SyncRunStatus::Idle);
        assert_eq!(snapshot.progress.mode, SyncProgressMode::Indeterminate);
        assert_eq!(snapshot.progress.percent, None);
        assert_eq!(snapshot.run.id, None);
    }
}
