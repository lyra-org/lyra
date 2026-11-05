// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    any::Any,
    collections::{
        BTreeMap,
        HashMap,
    },
    future::Future,
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
use tokio_util::{
    sync::CancellationToken,
    task::TaskTracker,
};

use crate::{
    services::CoverSyncOptions,
    services::STATE,
    services::providers::LibraryRefreshOptions,
    services::timestamps::unix_ms_to_rfc3339_u64,
};
use lyra_db::{
    DbAsync,
    Library,
};

use super::sync::{
    LibrarySyncOptions,
    MAX_CONCURRENT_ALBUM_PIPELINE,
    sync_library_pipeline,
};
use lyra_db::entries::PurgeScope;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SyncRunKind {
    LibrarySync,
    LibraryRefresh,
    LibraryPurge,
}

impl SyncRunKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::LibrarySync => "library_sync",
            Self::LibraryRefresh => "library_refresh",
            Self::LibraryPurge => "library_purge",
        }
    }

    fn from_str(value: &str) -> anyhow::Result<Self> {
        match value {
            "library_sync" => Ok(Self::LibrarySync),
            "library_refresh" => Ok(Self::LibraryRefresh),
            "library_purge" => Ok(Self::LibraryPurge),
            _ => anyhow::bail!("unknown sync run kind: {value}"),
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

    pub(crate) fn is_active(self) -> bool {
        matches!(
            self,
            Self::Queued | Self::Planning | Self::Running | Self::Cancelling
        )
    }

    fn from_str(value: &str) -> anyhow::Result<Self> {
        match value {
            "idle" => Ok(Self::Idle),
            "queued" => Ok(Self::Queued),
            "planning" => Ok(Self::Planning),
            "running" => Ok(Self::Running),
            "cancelling" => Ok(Self::Cancelling),
            "cancelled" => Ok(Self::Cancelled),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            _ => anyhow::bail!("unknown sync run status: {value}"),
        }
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

impl SyncProgressMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Indeterminate => "indeterminate",
            Self::Estimating => "estimating",
            Self::Determinate => "determinate",
        }
    }

    fn from_str(value: &str) -> anyhow::Result<Self> {
        match value {
            "indeterminate" => Ok(Self::Indeterminate),
            "estimating" => Ok(Self::Estimating),
            "determinate" => Ok(Self::Determinate),
            _ => anyhow::bail!("unknown sync progress mode: {value}"),
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SyncTotalState {
    Discovering,
    Estimated,
    Final,
}

impl SyncTotalState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Discovering => "discovering",
            Self::Estimated => "estimated",
            Self::Final => "final",
        }
    }

    fn from_str(value: &str) -> anyhow::Result<Self> {
        match value {
            "discovering" => Ok(Self::Discovering),
            "estimated" => Ok(Self::Estimated),
            "final" => Ok(Self::Final),
            _ => anyhow::bail!("unknown sync total state: {value}"),
        }
    }
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
    /// The tracks a purge removes, a unit each.
    Purge,
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
            Self::Purge => "purge",
        }
    }

    fn from_str(value: &str) -> anyhow::Result<Self> {
        match value {
            "discover" => Ok(Self::Discover),
            "entry_sync" => Ok(Self::EntrySync),
            "metadata_parse" => Ok(Self::MetadataParse),
            "metadata_apply" => Ok(Self::MetadataApply),
            "provider_refresh" => Ok(Self::ProviderRefresh),
            "local_cover_metadata" => Ok(Self::LocalCoverMetadata),
            "lyrics" => Ok(Self::Lyrics),
            "provider_cover" => Ok(Self::ProviderCover),
            "cleanup" => Ok(Self::Cleanup),
            "purge" => Ok(Self::Purge),
            _ => anyhow::bail!("unknown sync stage: {value}"),
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SyncRunSummary {
    pub(crate) run: SyncRunInfo,
    pub(crate) progress: SyncRunProgressSummary,
    pub(crate) current: Option<SyncRunCurrent>,
    pub(crate) active_units: u64,
    pub(crate) failure_count: u64,
    /// Orders the library's summaries, across its runs: a higher sequence is newer.
    pub(crate) sequence: u64,
}

pub(crate) type LibrarySyncStatus = SyncRunSummary;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SyncRunInfo {
    pub(crate) id: Option<String>,
    pub(crate) kind: SyncRunKind,
    pub(crate) library_id: String,
    pub(crate) status: SyncRunStatus,
    pub(crate) started_at: Option<String>,
    pub(crate) finished_at: Option<String>,
    pub(crate) error: Option<String>,
    pub(crate) cancellation_requested: bool,
    /// `started_at` in Unix milliseconds.
    #[serde(skip)]
    #[cfg_attr(feature = "docgen", schemars(skip))]
    pub(crate) started_at_ms: Option<u64>,
    /// `finished_at` in Unix milliseconds.
    #[serde(skip)]
    #[cfg_attr(feature = "docgen", schemars(skip))]
    pub(crate) finished_at_ms: Option<u64>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SyncRunProgressSummary {
    pub(crate) mode: SyncProgressMode,
    pub(crate) total_state: SyncTotalState,
    pub(crate) completed_units: u64,
    pub(crate) failed_units: u64,
    pub(crate) skipped_units: u64,
    pub(crate) total_units: u64,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SyncRunCurrent {
    pub(crate) stage: SyncStageKey,
    pub(crate) subject: Option<String>,
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

    pub(crate) fn artist(
        work_type: impl Into<String>,
        artist_id: Option<String>,
        artist_name: Option<String>,
    ) -> Self {
        Self {
            work_type: work_type.into(),
            entity_type: Some("artist".to_string()),
            entity_id: artist_id,
            entity_title: artist_name,
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

#[derive(Clone, Debug)]
struct SyncActiveWork {
    work_id: u64,
    stage: SyncStageKey,
    details: SyncWorkDetails,
    started_at_ms: u64,
}

#[derive(Clone, Debug)]
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

    fn settled_units(&self) -> u64 {
        self.completed_units
            .saturating_add(self.failed_units)
            .saturating_add(self.skipped_units)
    }

    fn unstarted_units(&self) -> u64 {
        self.total_units
            .saturating_sub(self.settled_units())
            .saturating_sub(self.active_units)
    }
}

#[derive(Clone, Debug)]
struct SyncRunState {
    run_id: String,
    kind: SyncRunKind,
    library_id: String,
    status: SyncRunStatus,
    progress_mode: SyncProgressMode,
    total_state: SyncTotalState,
    stages: BTreeMap<SyncStageKey, SyncStageState>,
    active: Vec<SyncActiveWork>,
    failure_count: u64,
    started_at_ms: u64,
    updated_at_ms: u64,
    finished_at_ms: Option<u64>,
    error: Option<String>,
    cancellation_requested: bool,
    sequence: u64,
    next_work_id: u64,
    /// Cancelled with the run, and with the server's shutdown.
    cancel: CancellationToken,
}

impl SyncRunState {
    /// A queued run, numbering its summaries from `sequence`.
    fn new(kind: SyncRunKind, library_id: String, now_ms: u64, sequence: u64) -> Self {
        Self {
            run_id: nanoid!(),
            kind,
            library_id,
            status: SyncRunStatus::Queued,
            progress_mode: SyncProgressMode::Indeterminate,
            total_state: SyncTotalState::Discovering,
            stages: BTreeMap::new(),
            active: Vec::new(),
            failure_count: 0,
            started_at_ms: now_ms,
            updated_at_ms: now_ms,
            finished_at_ms: None,
            error: None,
            cancellation_requested: false,
            sequence,
            next_work_id: 1,
            cancel: crate::services::shutdown::token().child_token(),
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
            failure_count: 0,
            started_at_ms: now_ms,
            updated_at_ms: now_ms,
            finished_at_ms: None,
            error: None,
            cancellation_requested: false,
            sequence: 0,
            next_work_id: 1,
            cancel: CancellationToken::new(),
        }
    }

    fn progress_summary(&self) -> SyncRunProgressSummary {
        SyncRunProgressSummary {
            mode: self.progress_mode,
            total_state: self.total_state,
            completed_units: self
                .stages
                .values()
                .map(|stage| stage.completed_units)
                .sum(),
            failed_units: self.stages.values().map(|stage| stage.failed_units).sum(),
            skipped_units: self.stages.values().map(|stage| stage.skipped_units).sum(),
            total_units: self.stages.values().map(|stage| stage.total_units).sum(),
        }
    }

    fn current(&self) -> Option<SyncRunCurrent> {
        if !self.status.is_active() {
            return None;
        }
        self.active
            .iter()
            .min_by_key(|work| work.started_at_ms)
            .map(|work| SyncRunCurrent {
                stage: work.stage,
                subject: current_subject(&work.details),
            })
            .or_else(|| {
                self.stages
                    .iter()
                    .find(|(_, stage)| {
                        matches!(
                            stage.status,
                            SyncStageStatus::Pending | SyncStageStatus::Running
                        )
                    })
                    .map(|(&stage, _)| SyncRunCurrent {
                        stage,
                        subject: None,
                    })
            })
    }

    fn summary(&self) -> SyncRunSummary {
        let started_at_ms = (self.status != SyncRunStatus::Idle).then_some(self.started_at_ms);
        SyncRunSummary {
            run: SyncRunInfo {
                id: (!self.run_id.is_empty()).then(|| self.run_id.clone()),
                kind: self.kind,
                library_id: self.library_id.clone(),
                status: self.status,
                started_at: started_at_ms.map(unix_ms_to_rfc3339_u64),
                finished_at: self.finished_at_ms.map(unix_ms_to_rfc3339_u64),
                error: self.error.clone(),
                cancellation_requested: self.cancellation_requested,
                started_at_ms,
                finished_at_ms: self.finished_at_ms,
            },
            progress: self.progress_summary(),
            current: self.current(),
            active_units: self.active.len() as u64,
            failure_count: self.failure_count,
            sequence: self.sequence,
        }
    }

    fn record(&self, db_id: Option<DbId>) -> anyhow::Result<lyra_db::sync_runs::SyncRunRecord> {
        let run_id = (!self.run_id.is_empty())
            .then(|| self.run_id.clone())
            .ok_or_else(|| anyhow::anyhow!("cannot persist idle sync run"))?;
        let progress = self.progress_summary();
        let current = self.current();
        Ok(lyra_db::sync_runs::SyncRunRecord {
            db_id,
            id: run_id,
            library_id: self.library_id.clone(),
            kind: self.kind.as_str().to_string(),
            status: self.status.as_str().to_string(),
            started_at_ms: self.started_at_ms,
            updated_at_ms: self.updated_at_ms,
            finished_at_ms: self.finished_at_ms,
            error: self.error.clone(),
            cancellation_requested: self.cancellation_requested,
            sequence: self.sequence,
            progress_mode: self.progress_mode.as_str().to_string(),
            total_state: self.total_state.as_str().to_string(),
            completed_units: progress.completed_units,
            failed_units: progress.failed_units,
            skipped_units: progress.skipped_units,
            total_units: progress.total_units,
            current_stage: current
                .as_ref()
                .map(|current| current.stage.as_str().to_string()),
            current_subject: current.and_then(|current| current.subject),
            active_units: self.active.len() as u64,
            failure_count: self.failure_count,
        })
    }
}

fn current_subject(details: &SyncWorkDetails) -> Option<String> {
    details
        .entity_title
        .as_ref()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            details
                .provider_id
                .as_ref()
                .filter(|value| !value.is_empty())
        })
        .or_else(|| {
            details
                .source_dir
                .as_ref()
                .filter(|value| !value.is_empty())
        })
        .cloned()
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
    pub(crate) run: SyncRunSummary,
}

#[derive(Clone, Debug)]
pub(crate) struct SyncRunProgress {
    db: DbAsync,
    run_id: String,
    cancel: CancellationToken,
}

impl SyncRunProgress {
    fn new(db: DbAsync, state: &SyncRunState) -> Self {
        Self {
            db,
            run_id: state.run_id.clone(),
            cancel: state.cancel.clone(),
        }
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
            state.active.push(SyncActiveWork {
                work_id,
                stage,
                details,
                started_at_ms: now_ms,
            });
        })
        .await;
        work_id
    }

    #[cfg(test)]
    pub(crate) async fn set_test_error(&self, error: &str) {
        self.mutate(|state| state.error = Some(error.to_string()))
            .await;
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
                    state.failure_count = state.failure_count.saturating_add(1);
                    let elapsed_ms = removed
                        .as_ref()
                        .map(|item| now_unix_ms().saturating_sub(item.started_at_ms));
                    tracing::warn!(
                        run_id = %state.run_id,
                        stage = stage.as_str(),
                        work_type = %details.work_type,
                        provider_id = ?details.provider_id,
                        entity_type = ?details.entity_type,
                        entity_id = ?details.entity_id,
                        entity_title = ?details.entity_title,
                        source_dir = ?details.source_dir,
                        elapsed_ms,
                        error = %message,
                        "sync work failed"
                    );
                }
            }
            if stage_state.total_units > 0
                && stage_state.settled_units() >= stage_state.total_units
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

    pub(crate) async fn check_cancelled(&self) -> Result<(), SyncRunControlError> {
        if self.cancel.is_cancelled() {
            Err(SyncRunControlError::Cancelled)
        } else {
            Ok(())
        }
    }

    async fn mutate<F>(&self, update: F)
    where
        F: FnOnce(&mut SyncRunState),
    {
        let Some((summary, record)) = ({
            let mut states = sync_run_states().write_owned().await;
            let Some(state) = states.get_mut(&self.run_id) else {
                return;
            };
            if !state.status.is_active() {
                return;
            }
            update(state);
            state.updated_at_ms = now_unix_ms();
            state.sequence = state.sequence.saturating_add(1);
            match state.record(None) {
                Ok(record) => Some((state.summary(), record)),
                Err(err) => {
                    tracing::warn!(
                        run_id = %self.run_id,
                        error = %err,
                        "failed to build sync run record"
                    );
                    None
                }
            }
        }) else {
            return;
        };
        persist_record(&self.db, record).await;
        publish_event(summary);
    }
}

enum WorkOutcome {
    Complete,
    Skip(String),
    Fail(SyncWorkDetails, String),
}

const SYNC_EVENT_BROADCAST_CAPACITY: usize = 512;

// Pure serialization lock with no DB-derived data; intentionally
// process-wide rather than generation-owned.
static SYNC_START_LOCK: LazyLock<Arc<Mutex<()>>> = LazyLock::new(|| Arc::new(Mutex::new(())));

/// Generation-owned library sync tracking: in-flight run states and the broadcast of their
/// summaries.
pub(crate) struct LibrarySyncRegistries {
    run_states: Arc<RwLock<HashMap<String, SyncRunState>>>,
    event_tx: broadcast::Sender<SyncRunSummary>,
    /// The task of each run that ends it, once its work returns, panics or is aborted.
    supervisors: TaskTracker,
}

impl Default for LibrarySyncRegistries {
    fn default() -> Self {
        Self {
            run_states: Arc::default(),
            event_tx: broadcast::channel(SYNC_EVENT_BROADCAST_CAPACITY).0,
            supervisors: TaskTracker::new(),
        }
    }
}

fn sync_run_states() -> Arc<RwLock<HashMap<String, SyncRunState>>> {
    crate::services::STATE
        .generation()
        .library_sync
        .run_states
        .clone()
}

fn run_supervisors() -> TaskTracker {
    crate::services::STATE
        .generation()
        .library_sync
        .supervisors
        .clone()
}

fn publish_event(summary: SyncRunSummary) {
    let _ = crate::services::STATE
        .generation()
        .library_sync
        .event_tx
        .send(summary);
}

/// Every run summary published from now on, of every library.
pub(crate) fn subscribe_sync_run_events() -> broadcast::Receiver<SyncRunSummary> {
    crate::services::STATE
        .generation()
        .library_sync
        .event_tx
        .subscribe()
}

pub(crate) async fn get_sync_run(run_id: &str) -> anyhow::Result<Option<SyncRunSummary>> {
    reconcile_interrupted_runs(&STATE.db.get()).await?;
    if let Some(summary) = {
        let states = sync_run_states().read_owned().await;
        states.get(run_id).map(SyncRunState::summary)
    } {
        return Ok(Some(summary));
    }
    let db = STATE.db.read().await;
    lyra_db::sync_runs::get_by_id(&db, run_id)?
        .map(record_to_summary)
        .transpose()
}

#[cfg(test)]
pub(crate) async fn end_test_run(progress: &SyncRunProgress) {
    sync_run_states()
        .write_owned()
        .await
        .remove(progress.run_id());
}

#[cfg(test)]
pub(crate) async fn begin_test_run(library: &Library) -> SyncRunProgress {
    let sequence = next_run_sequence(&STATE.db.get(), &library.id)
        .await
        .expect("sync run sequence");
    let state = SyncRunState::new(
        SyncRunKind::LibrarySync,
        library.id.clone(),
        now_unix_ms(),
        sequence,
    );
    let progress = SyncRunProgress::new(STATE.db.get(), &state);
    sync_run_states()
        .write_owned()
        .await
        .insert(progress.run_id.clone(), state);
    progress
}

pub(crate) async fn cancel_sync_run(run_id: &str) -> anyhow::Result<Option<SyncRunSummary>> {
    reconcile_interrupted_runs(&STATE.db.get()).await?;
    let (summary, record) = {
        let mut states = sync_run_states().write_owned().await;
        let Some(state) = states.get_mut(run_id) else {
            drop(states);
            return get_sync_run(run_id).await;
        };
        if state.status.is_active() {
            state.status = SyncRunStatus::Cancelling;
            state.cancellation_requested = true;
            state.cancel.cancel();
            state.updated_at_ms = now_unix_ms();
            state.sequence = state.sequence.saturating_add(1);
        }
        (state.summary(), state.record(None)?)
    };
    persist_record(&STATE.db.get(), record).await;
    publish_event(summary.clone());
    Ok(Some(summary))
}

pub(crate) async fn library_mutation_guard() -> tokio::sync::OwnedMutexGuard<()> {
    SYNC_START_LOCK.clone().lock_owned().await
}

pub(crate) async fn library_has_active_run(library_id: &str) -> anyhow::Result<bool> {
    Ok(
        active_run_for_library(&crate::services::STATE.db.get(), library_id)
            .await?
            .is_some(),
    )
}

/// A run to start on a library.
#[derive(Clone, Debug)]
pub(crate) enum LibraryRun {
    Sync(LibrarySyncOptions),
    /// A refresh fetching these covers.
    Refresh(CoverSyncOptions),
    /// A purge of these missing files.
    Purge(PurgeScope),
}

pub(crate) async fn start_library_sync(
    db: DbAsync,
    library: Library,
) -> anyhow::Result<SyncRunStartResponse> {
    start_library_run(db, LibraryRun::Sync(LibrarySyncOptions::default()), |db| {
        still_exists(db, library)
    })
    .await
}

fn still_exists(db: &agdb::DbAny, library: Library) -> anyhow::Result<Library> {
    let current = library
        .db_id
        .map(|id| lyra_db::libraries::get_by_id(db, id))
        .transpose()?
        .flatten();
    anyhow::ensure!(
        current.is_some_and(|current| current.id == library.id),
        "library no longer exists"
    );
    Ok(library)
}

/// Starts `run` on the library `resolve` picks, unless one of either kind is active there.
/// `resolve` runs under the lock that serializes starts, with the database read, so whatever it
/// checks still holds when the run is registered.
pub(crate) async fn start_library_run<E: From<anyhow::Error>>(
    db: DbAsync,
    run: LibraryRun,
    resolve: impl FnOnce(&agdb::DbAny) -> Result<Library, E>,
) -> Result<SyncRunStartResponse, E> {
    start_resolved_run(db, |db| Ok((resolve(db)?, run))).await
}

/// [`start_library_run`] with the run `resolve` picks as well, under the same lock.
pub(crate) async fn start_resolved_run<E: From<anyhow::Error>>(
    db: DbAsync,
    resolve: impl FnOnce(&agdb::DbAny) -> Result<(Library, LibraryRun), E>,
) -> Result<SyncRunStartResponse, E> {
    let _start_guard = SYNC_START_LOCK.lock().await;
    let (library, run) = resolve(&*db.read().await)?;
    Ok(start_locked(db, library, run).await?)
}

async fn start_locked(
    db: DbAsync,
    library: Library,
    run: LibraryRun,
) -> anyhow::Result<SyncRunStartResponse> {
    let shutdown = crate::services::shutdown::token();
    anyhow::ensure!(!shutdown.is_cancelled(), "server is shutting down");
    reconcile_interrupted_runs(&db).await?;
    let library_id = library.id.clone();
    if let Some(existing) = active_run_for_library(&db, &library_id).await? {
        return Ok(SyncRunStartResponse {
            started: false,
            run: existing,
        });
    }

    let (kind, label) = match run {
        LibraryRun::Sync(_) => (SyncRunKind::LibrarySync, "library sync"),
        LibraryRun::Refresh(_) => (SyncRunKind::LibraryRefresh, "library refresh"),
        LibraryRun::Purge(_) => (SyncRunKind::LibraryPurge, "library purge"),
    };
    let now_ms = now_unix_ms();
    let sequence = next_run_sequence(&db, &library_id).await?;
    let state = SyncRunState::new(kind, library_id, now_ms, sequence);
    let summary = state.summary();
    let record = state.record(None)?;
    let progress = SyncRunProgress::new(db.clone(), &state);
    let run_id = progress.run_id.clone();
    let (registered_tx, registered) = tokio::sync::oneshot::channel();
    let supervisor = {
        let summary = summary.clone();
        async move {
            persist_record(&db, record).await;
            publish_event(summary);
            let _ = registered_tx.send(());
            let work_progress = progress.clone();
            // Boxed: the work futures nest too deep for the compiler to prove them `Send`.
            let work: futures::future::BoxFuture<'static, anyhow::Result<()>> = match run {
                LibraryRun::Sync(options) => {
                    Box::pin(run_library_sync(db, library, work_progress, options))
                }
                LibraryRun::Refresh(covers) => {
                    Box::pin(run_library_refresh(library, work_progress, covers))
                }
                LibraryRun::Purge(selection) => {
                    Box::pin(run_library_purge(db, library, work_progress, selection))
                }
            };
            supervise_run(progress, label, work).await;
        }
    };

    // The supervisor is spawned as the state is registered, so no run is left active without
    // one, whenever the caller stops waiting.
    let mut states = sync_run_states().write_owned().await;
    crate::services::shutdown::unless_cancelled(&shutdown, || {
        states.insert(run_id, state);
        run_supervisors().spawn(supervisor);
    })
    .ok_or_else(|| anyhow::anyhow!("server is shutting down"))?;
    drop(states);
    let _ = registered.await;

    Ok(SyncRunStartResponse {
        started: true,
        run: summary,
    })
}

/// Runs `work` to its end and records how the run ended: cancelled when it stopped for a
/// cancellation, failed when it erred, panicked or was aborted, and succeeded otherwise.
async fn supervise_run(
    progress: SyncRunProgress,
    label: &'static str,
    work: impl Future<Output = anyhow::Result<()>> + Send + 'static,
) {
    let started = Instant::now();
    let result = match tokio::spawn(work).await {
        Ok(result) => result,
        Err(err) => Err(match err.try_into_panic() {
            Ok(payload) => anyhow::anyhow!("{label} panicked: {}", panic_message(&*payload)),
            Err(err) => anyhow::anyhow!("{label} ended early: {err}"),
        }),
    };
    finish_run(progress, result, started, label).await;
}

fn panic_message(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("unknown panic")
}

async fn run_library_sync(
    db: DbAsync,
    library: Library,
    progress: SyncRunProgress,
    options: LibrarySyncOptions,
) -> anyhow::Result<()> {
    tracing::info!(
        run_id = progress.run_id(),
        library_public_id = %library.id,
        library_name = %library.name,
        album_concurrency = MAX_CONCURRENT_ALBUM_PIPELINE,
        "library sync started"
    );
    progress.set_status(SyncRunStatus::Planning).await;
    sync_library_pipeline(&db, &library, options, Some(progress)).await?;
    Ok(())
}

async fn run_library_purge(
    db: DbAsync,
    library: Library,
    progress: SyncRunProgress,
    selection: PurgeScope,
) -> anyhow::Result<()> {
    tracing::info!(
        run_id = progress.run_id(),
        library_public_id = %library.id,
        "library purge started"
    );
    progress.set_status(SyncRunStatus::Running).await;
    super::purge::purge(&db, &library, &selection, Some(&progress)).await?;
    Ok(())
}

async fn run_library_refresh(
    library: Library,
    progress: SyncRunProgress,
    covers: CoverSyncOptions,
) -> anyhow::Result<()> {
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;
    tracing::info!(
        run_id = progress.run_id(),
        library_public_id = %library.id,
        library_name = %library.name,
        "library refresh started"
    );
    progress.set_status(SyncRunStatus::Planning).await;
    let refresh_options = LibraryRefreshOptions {
        covers,
        apply_sync_filters: false,
        provider_id: None,
    };
    crate::services::providers::refresh_library_metadata_with_progress(
        library_db_id,
        &refresh_options,
        Some(progress),
    )
    .await
    .map_err(crate::services::providers::ProviderServiceError::into_anyhow)?;
    Ok(())
}

async fn finish_run(
    progress: SyncRunProgress,
    result: anyhow::Result<()>,
    started: Instant,
    label: &'static str,
) {
    match result {
        Ok(()) => {
            let summary = mark_terminal(
                &progress.db,
                progress.run_id(),
                SyncRunStatus::Succeeded,
                None,
            )
            .await;
            if let Some(summary) = summary {
                tracing::info!(
                    run_id = %progress.run_id(),
                    status = ?summary.run.status,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    completed_units = summary.progress.completed_units,
                    skipped_units = summary.progress.skipped_units,
                    total_units = summary.progress.total_units,
                    failed_units = summary.progress.failed_units,
                    "{label} completed"
                );
            }
        }
        Err(err) if err.downcast_ref::<SyncRunControlError>().is_some() => {
            let summary = mark_terminal(
                &progress.db,
                progress.run_id(),
                SyncRunStatus::Cancelled,
                None,
            )
            .await;
            if summary.is_some() {
                tracing::info!(
                    run_id = %progress.run_id(),
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "{label} cancelled"
                );
            }
        }
        Err(err) => {
            let error = format!("{err:#}");
            let summary = mark_terminal(
                &progress.db,
                progress.run_id(),
                SyncRunStatus::Failed,
                Some(error.clone()),
            )
            .await;
            if summary.is_some() {
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
) -> Option<SyncRunSummary> {
    let (summary, record) = {
        let mut states = sync_run_states().write_owned().await;
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
                if stage.total_units > 0 && stage.settled_units() >= stage.total_units {
                    stage.status = if stage.failed_units == 0 {
                        SyncStageStatus::Succeeded
                    } else {
                        SyncStageStatus::Failed
                    };
                } else if had_active_units
                    || stage.unstarted_units() > 0
                    || matches!(
                        stage.status,
                        SyncStageStatus::Pending | SyncStageStatus::Running
                    )
                {
                    stage.status = SyncStageStatus::Cancelled;
                }
            } else if stage.total_units > 0 && stage.settled_units() < stage.total_units {
                stage.status = if status == SyncRunStatus::Failed {
                    SyncStageStatus::Failed
                } else {
                    SyncStageStatus::Succeeded
                };
                let remaining = stage.total_units.saturating_sub(stage.settled_units());
                stage.skipped_units = stage.skipped_units.saturating_add(remaining);
            }
        }
        state.sequence = state.sequence.saturating_add(1);
        let record = match state.record(None) {
            Ok(record) => record,
            Err(err) => {
                tracing::warn!(run_id, error = %err, "failed to build terminal sync run record");
                return None;
            }
        };
        (state.summary(), record)
    };
    persist_record(db, record).await;
    publish_event(summary.clone());
    sync_run_states().write_owned().await.remove(run_id);
    Some(summary)
}

/// Which of a library's runs a status read reports.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RunFilter {
    /// Only runs of this kind.
    pub(crate) kind: Option<SyncRunKind>,
    /// The latest run that ended, rather than the active one or else the latest.
    pub(crate) finished: bool,
}

/// The run of each library `filter` picks, or an idle summary for a library without one, keyed
/// by library id.
pub(crate) async fn library_sync_statuses(
    libraries: &[Library],
    filter: RunFilter,
) -> anyhow::Result<HashMap<String, SyncRunSummary>> {
    let db = STATE.db.get();
    if let Err(err) = reconcile_interrupted_runs(&db).await {
        tracing::warn!(error = %err, "failed to reconcile interrupted sync runs");
    }
    let mut statuses = HashMap::with_capacity(libraries.len());
    for library in libraries {
        if statuses.contains_key(&library.id) {
            continue;
        }
        let summary = filtered_run_for_library(&db, &library.id, filter)
            .await?
            .unwrap_or_else(|| {
                let mut idle = SyncRunState::idle(library.id.clone());
                idle.kind = filter.kind.unwrap_or(SyncRunKind::LibrarySync);
                idle.summary()
            });
        statuses.insert(library.id.clone(), summary);
    }
    Ok(statuses)
}

async fn filtered_run_for_library(
    db: &DbAsync,
    library_id: &str,
    filter: RunFilter,
) -> anyhow::Result<Option<SyncRunSummary>> {
    let kind_matches = |kind: SyncRunKind| filter.kind.is_none_or(|wanted| wanted == kind);
    if !filter.finished
        && let Some(active) = active_run_for_library(db, library_id).await?
        && kind_matches(active.run.kind)
    {
        return Ok(Some(active));
    }
    let db_read = db.read().await;
    for record in lyra_db::sync_runs::list_for_library(&db_read, library_id)? {
        if !kind_matches(SyncRunKind::from_str(&record.kind)?) {
            continue;
        }
        if filter.finished && SyncRunStatus::from_str(&record.status)?.is_active() {
            continue;
        }
        return record_to_summary(record).map(Some);
    }
    Ok(None)
}

/// Cancels the library's active run and returns its summary. Without an active run, returns the
/// latest run's summary, or an idle one when the library never ran.
pub(crate) async fn cancel_library_run(library: &Library) -> anyhow::Result<SyncRunSummary> {
    let db = STATE.db.get();
    reconcile_interrupted_runs(&db).await?;
    if let Some(run_id) = active_run_for_library(&db, &library.id)
        .await?
        .and_then(|active| active.run.id)
        && let Some(summary) = cancel_sync_run(&run_id).await?
    {
        return Ok(summary);
    }
    Ok(active_or_latest_run_for_library(&db, &library.id)
        .await?
        .unwrap_or_else(|| SyncRunState::idle(library.id.clone()).summary()))
}

/// The first sequence of the library's next run: past every sequence its runs reached, live or
/// stored, and past the idle summary's 0, so that a library's summaries order by sequence across
/// its runs.
async fn next_run_sequence(db: &DbAsync, library_id: &str) -> anyhow::Result<u64> {
    let live = sync_run_states()
        .read_owned()
        .await
        .values()
        .filter(|state| state.library_id == library_id)
        .map(|state| state.sequence)
        .max();
    let stored = lyra_db::sync_runs::list_for_library(&*db.read().await, library_id)?
        .into_iter()
        .map(|record| record.sequence)
        .max();
    Ok(live.max(stored).unwrap_or(0).saturating_add(1))
}

async fn active_or_latest_run_for_library(
    db: &DbAsync,
    library_id: &str,
) -> anyhow::Result<Option<SyncRunSummary>> {
    if let Some(active) = active_run_for_library(db, library_id).await? {
        return Ok(Some(active));
    }
    let db_read = db.read().await;
    lyra_db::sync_runs::latest_for_library(&db_read, library_id)?
        .map(record_to_summary)
        .transpose()
}

async fn active_run_for_library(
    db: &DbAsync,
    library_id: &str,
) -> anyhow::Result<Option<SyncRunSummary>> {
    if let Some(summary) = {
        let states = sync_run_states().read_owned().await;
        states
            .values()
            .find(|state| state.library_id == library_id && state.status.is_active())
            .map(SyncRunState::summary)
    } {
        return Ok(Some(summary));
    }
    let db_read = db.read().await;
    lyra_db::sync_runs::active_for_library(&db_read, library_id)?
        .map(record_to_summary)
        .transpose()
}

pub(crate) async fn running_library_sync_count() -> usize {
    sync_run_states()
        .read_owned()
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

/// How long shutdown waits for cancelled library runs to end. A run still going after it is left
/// active, and recorded as interrupted at the next start.
const LIBRARY_RUN_DRAIN_TIMEOUT: Duration = Duration::from_secs(15);

/// Cancels every library run and waits, up to [`LIBRARY_RUN_DRAIN_TIMEOUT`], for each to record
/// how it ended.
pub(crate) async fn drain_library_syncs() {
    drain_within(LIBRARY_RUN_DRAIN_TIMEOUT).await;
}

async fn drain_within(timeout: Duration) {
    for state in sync_run_states().read_owned().await.values() {
        state.cancel.cancel();
    }
    let supervisors = run_supervisors();
    supervisors.close();
    if tokio::time::timeout(timeout, supervisors.wait())
        .await
        .is_err()
    {
        let run_ids = sync_run_states()
            .read_owned()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        tracing::warn!(?run_ids, "library runs still running at shutdown");
    }
}

async fn reconcile_interrupted_runs(db: &DbAsync) -> anyhow::Result<()> {
    let live_run_ids = {
        let states = sync_run_states().read_owned().await;
        states.keys().cloned().collect::<Vec<_>>()
    };
    // Updates sync run records only.
    let mut db_write = db.write_outside_catalog().await;
    let records = lyra_db::sync_runs::list(&db_write)?;
    for mut record in records {
        if live_run_ids.iter().any(|id| id == &record.id) {
            continue;
        }
        if !matches!(
            record.status.as_str(),
            "queued" | "planning" | "running" | "cancelling"
        ) {
            continue;
        }
        let now_ms = now_unix_ms();
        record.status = SyncRunStatus::Failed.as_str().to_string();
        record.finished_at_ms = Some(now_ms);
        record.updated_at_ms = now_ms;
        record.error = Some("server stopped before run completed".to_string());
        record.cancellation_requested = false;
        record.current_stage = None;
        record.current_subject = None;
        record.active_units = 0;
        record.sequence = record.sequence.saturating_add(1);
        lyra_db::sync_runs::update(&mut db_write, &record)?;
    }
    Ok(())
}

async fn persist_record(db: &DbAsync, mut record: lyra_db::sync_runs::SyncRunRecord) {
    let run_id = record.id.clone();
    let result = async {
        // Writes sync run records only.
        let mut db_write = db.write_outside_catalog().await;
        if lyra_db::lookup::find_node_id_by_id(&*db_write, &record.library_id)?.is_none() {
            return Ok(());
        }
        let existing = lyra_db::sync_runs::get_by_id(&db_write, &run_id)?;
        record.db_id = existing.as_ref().and_then(|run| run.db_id);
        if existing.is_some() {
            lyra_db::sync_runs::update(&mut db_write, &record)?;
        } else {
            lyra_db::sync_runs::create(&mut db_write, &record)?;
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;
    if let Err(err) = result {
        tracing::warn!(run_id, error = %err, "failed to persist sync run summary");
    }
}

fn record_to_summary(record: lyra_db::sync_runs::SyncRunRecord) -> anyhow::Result<SyncRunSummary> {
    let current = record
        .current_stage
        .as_deref()
        .map(SyncStageKey::from_str)
        .transpose()?
        .map(|stage| SyncRunCurrent {
            stage,
            subject: record.current_subject.clone(),
        });
    Ok(SyncRunSummary {
        run: SyncRunInfo {
            id: Some(record.id),
            kind: SyncRunKind::from_str(&record.kind)?,
            library_id: record.library_id,
            status: SyncRunStatus::from_str(&record.status)?,
            started_at: Some(unix_ms_to_rfc3339_u64(record.started_at_ms)),
            finished_at: record.finished_at_ms.map(unix_ms_to_rfc3339_u64),
            error: record.error,
            cancellation_requested: record.cancellation_requested,
            started_at_ms: Some(record.started_at_ms),
            finished_at_ms: record.finished_at_ms,
        },
        progress: SyncRunProgressSummary {
            mode: SyncProgressMode::from_str(&record.progress_mode)?,
            total_state: SyncTotalState::from_str(&record.total_state)?,
            completed_units: record.completed_units,
            failed_units: record.failed_units,
            skipped_units: record.skipped_units,
            total_units: record.total_units,
        },
        current,
        active_units: record.active_units,
        failure_count: record.failure_count,
        sequence: record.sequence,
    })
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::services::{
        catalog::index::snapshot,
        testing::{
            init_default_test_state,
            runtime_test_lock,
        },
    };

    struct ShutdownResetGuard;

    impl Drop for ShutdownResetGuard {
        fn drop(&mut self) {
            crate::services::shutdown::reset();
        }
    }

    async fn test_library() -> anyhow::Result<Library> {
        let mut db = STATE.db.write().await;
        let library = lyra_db::test_db::insert_library(&mut db, "Music", "/runs")?;
        Ok(lyra_db::libraries::get_by_id(&db, library)?.expect("library exists"))
    }

    async fn stored_status(run_id: &str) -> anyhow::Result<SyncRunInfo> {
        Ok(get_sync_run(run_id).await?.expect("run stored").run)
    }

    #[tokio::test]
    async fn persisting_run_records_keeps_the_catalog_index() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let library_id = {
            let mut db = STATE.db.write().await;
            let library = lyra_db::test_db::insert_library(&mut db, "Music", "/runs")?;
            lyra_db::fixtures::insert_release(&mut db, "Kept")?;
            lyra_db::libraries::get_by_id(&db, library)?
                .expect("library exists")
                .id
        };
        let before = snapshot(&*STATE.db.read().await)?;

        let db = STATE.db.get();
        let mut state = SyncRunState::new(SyncRunKind::LibrarySync, library_id, 10_000, 0);
        persist_record(&db, state.record(None)?).await;
        state.status = SyncRunStatus::Running;
        persist_record(&db, state.record(None)?).await;
        reconcile_interrupted_runs(&db).await?;
        assert_eq!(lyra_db::sync_runs::list(&*db.read().await)?.len(), 1);
        assert!(std::sync::Arc::ptr_eq(
            &before,
            &snapshot(&*STATE.db.read().await)?
        ));

        lyra_db::fixtures::insert_release(&mut *STATE.db.write().await, "Added")?;
        assert!(!std::sync::Arc::ptr_eq(
            &before,
            &snapshot(&*STATE.db.read().await)?
        ));
        Ok(())
    }

    #[tokio::test]
    async fn a_run_ends_as_its_work_did() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let library = test_library().await?;

        let outcomes: [(anyhow::Result<()>, SyncRunStatus); 3] = [
            (Ok(()), SyncRunStatus::Succeeded),
            (
                Err(SyncRunControlError::Cancelled.into()),
                SyncRunStatus::Cancelled,
            ),
            (Err(anyhow::anyhow!("broke")), SyncRunStatus::Failed),
        ];
        for (outcome, status) in outcomes {
            let progress = begin_test_run(&library).await;
            let run_id = progress.run_id().to_string();
            supervise_run(progress, "library sync", async move { outcome }).await;
            assert_eq!(stored_status(&run_id).await?.status, status);
        }
        assert!(sync_run_states().read_owned().await.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn a_panicking_run_ends_failed_and_frees_the_library() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let library = test_library().await?;
        let progress = begin_test_run(&library).await;
        let run_id = progress.run_id().to_string();

        supervise_run(progress, "library sync", async { panic!("pipeline broke") }).await;

        let run = stored_status(&run_id).await?;
        assert_eq!(run.status, SyncRunStatus::Failed);
        assert_eq!(
            run.error.as_deref(),
            Some("library sync panicked: pipeline broke")
        );
        assert!(sync_run_states().read_owned().await.is_empty());
        tokio::time::timeout(Duration::from_secs(1), wait_for_running_library_syncs()).await?;
        assert!(!library_has_active_run(&library.id).await?);
        Ok(())
    }

    #[tokio::test]
    async fn a_run_ends_when_its_start_is_dropped() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let library = test_library().await?;
        let db = STATE.db.get();

        let mut start = Box::pin(start_library_run(
            db.clone(),
            LibraryRun::Purge(PurgeScope::All),
            {
                let library = library.clone();
                |db| still_exists(db, library)
            },
        ));
        loop {
            assert!(futures::poll!(&mut start).is_pending());
            if running_library_sync_count().await > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        // Holds back the run's first record until its start is gone.
        let held = db.write_outside_catalog().await;
        drop(start);
        drop(held);

        tokio::time::timeout(Duration::from_secs(5), wait_for_running_library_syncs()).await?;
        let latest = lyra_db::sync_runs::latest_for_library(&*db.read().await, &library.id)?
            .expect("run stored");
        assert_eq!(latest.status, "succeeded");
        Ok(())
    }

    #[tokio::test]
    async fn no_run_starts_once_shutdown_began() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let _shutdown = ShutdownResetGuard;
        init_default_test_state()?;
        crate::services::shutdown::reset();
        let library = test_library().await?;
        let running = begin_test_run(&library).await;

        crate::services::shutdown::cancel();

        assert!(running.check_cancelled().await.is_err());
        end_test_run(&running).await;
        let started = start_library_run(STATE.db.get(), LibraryRun::Purge(PurgeScope::All), |db| {
            still_exists(db, library.clone())
        })
        .await;
        assert!(started.is_err());
        assert!(sync_run_states().read_owned().await.is_empty());
        assert!(lyra_db::sync_runs::list(&*STATE.db.read().await)?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn cancelling_a_run_reaches_its_work() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let library = test_library().await?;
        let progress = begin_test_run(&library).await;

        cancel_sync_run(progress.run_id()).await?;

        assert!(progress.cancel.is_cancelled());
        assert!(progress.check_cancelled().await.is_err());
        end_test_run(&progress).await;
        Ok(())
    }

    #[tokio::test]
    async fn draining_cancels_runs_and_waits_for_them_to_end() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let library = test_library().await?;
        let progress = begin_test_run(&library).await;
        let run_id = progress.run_id().to_string();
        let work = {
            let progress = progress.clone();
            async move {
                loop {
                    progress.check_cancelled().await?;
                    sleep(Duration::from_millis(5)).await;
                }
            }
        };
        run_supervisors().spawn(supervise_run(progress, "library sync", work));

        drain_within(Duration::from_secs(5)).await;

        assert!(sync_run_states().read_owned().await.is_empty());
        assert_eq!(
            stored_status(&run_id).await?.status,
            SyncRunStatus::Cancelled
        );
        Ok(())
    }

    #[tokio::test]
    async fn draining_leaves_active_a_run_that_outlasts_it() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let library = test_library().await?;
        let progress = begin_test_run(&library).await;
        let run_id = progress.run_id().to_string();
        run_supervisors().spawn(supervise_run(
            progress,
            "library sync",
            std::future::pending(),
        ));

        drain_within(Duration::from_millis(50)).await;

        let states = sync_run_states().read_owned().await;
        assert!(
            states
                .get(&run_id)
                .is_some_and(|state| state.status.is_active())
        );
        Ok(())
    }

    #[test]
    fn summary_exposes_compact_raw_counts() {
        let mut state = SyncRunState::new(SyncRunKind::LibrarySync, "lib".to_string(), 10_000, 0);
        state.status = SyncRunStatus::Running;
        state.progress_mode = SyncProgressMode::Determinate;
        state.total_state = SyncTotalState::Final;
        state.failure_count = 1;
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
        state.active.push(SyncActiveWork {
            work_id: 1,
            stage: SyncStageKey::ProviderRefresh,
            details: SyncWorkDetails::release(
                "provider_refresh",
                Some("release-a".to_string()),
                Some("Release A".to_string()),
            )
            .provider("provider-a"),
            started_at_ms: 10_000,
        });

        let value = serde_json::to_value(state.summary()).expect("serialize summary");
        assert_eq!(value["progress"]["total_state"], json!("final"));
        assert_eq!(value["progress"]["completed_units"], json!(3));
        assert_eq!(value["progress"]["failed_units"], json!(1));
        assert_eq!(value["progress"]["skipped_units"], json!(1));
        assert_eq!(value["progress"]["total_units"], json!(10));
        assert_eq!(value["current"]["stage"], json!("provider_refresh"));
        assert_eq!(value["current"]["subject"], json!("Release A"));
        assert_eq!(value["active_units"], json!(1));
        assert_eq!(value["failure_count"], json!(1));
    }

    #[test]
    fn idle_summary_uses_indeterminate_progress_without_run_id() {
        let summary = SyncRunState::idle("lib".to_string()).summary();
        assert_eq!(summary.run.status, SyncRunStatus::Idle);
        assert_eq!(summary.progress.mode, SyncProgressMode::Indeterminate);
        assert_eq!(summary.run.id, None);
        assert!(summary.current.is_none());
    }
}
