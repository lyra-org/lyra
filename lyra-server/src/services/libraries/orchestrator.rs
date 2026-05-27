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

    fn from_str(value: &str) -> anyhow::Result<Self> {
        match value {
            "library_sync" => Ok(Self::LibrarySync),
            "library_refresh" => Ok(Self::LibraryRefresh),
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

    fn is_active(self) -> bool {
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

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SyncRunEvent {
    pub(crate) run_id: String,
    pub(crate) sequence: u64,
    pub(crate) event: SyncRunEventKind,
    pub(crate) summary: SyncRunSummary,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SyncRunEventKind {
    Snapshot,
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
            failure_count: 0,
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
            failure_count: 0,
            started_at_ms: now_ms,
            updated_at_ms: now_ms,
            finished_at_ms: None,
            error: None,
            cancellation_requested: false,
            sequence: 0,
            next_work_id: 1,
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
        SyncRunSummary {
            run: SyncRunInfo {
                id: (!self.run_id.is_empty()).then(|| self.run_id.clone()),
                kind: self.kind,
                library_id: self.library_id.clone(),
                status: self.status,
                started_at: (self.status != SyncRunStatus::Idle)
                    .then(|| unix_ms_to_rfc3339_u64(self.started_at_ms)),
                finished_at: self.finished_at_ms.map(unix_ms_to_rfc3339_u64),
                error: self.error.clone(),
                cancellation_requested: self.cancellation_requested,
            },
            progress: self.progress_summary(),
            current: self.current(),
            active_units: self.active.len() as u64,
            failure_count: self.failure_count,
            sequence: self.sequence,
        }
    }

    fn record(&self, db_id: Option<DbId>) -> anyhow::Result<db::sync_runs::SyncRunRecord> {
        let run_id = (!self.run_id.is_empty())
            .then(|| self.run_id.clone())
            .ok_or_else(|| anyhow::anyhow!("cannot persist idle sync run"))?;
        let progress = self.progress_summary();
        let current = self.current();
        Ok(db::sync_runs::SyncRunRecord {
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
        let Some((summary, record)) = ({
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
        publish_event(summary).await;
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

async fn publish_event(summary: SyncRunSummary) {
    let Some(run_id) = summary.run.id.clone() else {
        return;
    };
    let event = SyncRunEvent {
        run_id: run_id.clone(),
        sequence: summary.sequence,
        event: SyncRunEventKind::Snapshot,
        summary,
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

pub(crate) async fn get_sync_run(run_id: &str) -> anyhow::Result<Option<SyncRunSummary>> {
    reconcile_interrupted_runs(&STATE.db.get()).await?;
    if let Some(summary) = {
        let states = SYNC_RUN_STATES.read().await;
        states.get(run_id).map(SyncRunState::summary)
    } {
        return Ok(Some(summary));
    }
    let db = STATE.db.read().await;
    db::sync_runs::get_by_id(&db, run_id)?
        .map(record_to_summary)
        .transpose()
}

pub(crate) async fn cancel_sync_run(run_id: &str) -> anyhow::Result<Option<SyncRunSummary>> {
    reconcile_interrupted_runs(&STATE.db.get()).await?;
    let (summary, record) = {
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
        (state.summary(), state.record(None)?)
    };
    persist_record(&STATE.db.get(), record).await;
    publish_event(summary.clone()).await;
    Ok(Some(summary))
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
    let summary = state.summary();
    let record = state.record(None)?;
    {
        let mut states = SYNC_RUN_STATES.write().await;
        states.insert(run_id.clone(), state);
    }
    persist_record(&db, record).await;
    publish_event(summary.clone()).await;

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
        run: summary,
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
            let error = err.to_string();
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
    publish_event(summary.clone()).await;
    SYNC_RUN_STATES.write().await.remove(run_id);
    Some(summary)
}

pub(crate) async fn get_library_sync_status(library_db_id: DbId) -> SyncRunSummary {
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
        return SyncRunState::idle(String::new()).summary();
    };
    match active_or_latest_run_for_library(&db, &library.id).await {
        Ok(Some(summary)) => summary,
        Ok(None) => SyncRunState::idle(library.id).summary(),
        Err(err) => {
            tracing::warn!(library_id = %library.id, error = %err, "failed to load library sync run");
            SyncRunState::idle(library.id).summary()
        }
    }
}

async fn active_or_latest_run_for_library(
    db: &DbAsync,
    library_id: &str,
) -> anyhow::Result<Option<SyncRunSummary>> {
    if let Some(active) = active_run_for_library(db, library_id).await? {
        return Ok(Some(active));
    }
    let db_read = db.read().await;
    db::sync_runs::latest_for_library(&db_read, library_id)?
        .map(record_to_summary)
        .transpose()
}

async fn active_run_for_library(
    db: &DbAsync,
    library_id: &str,
) -> anyhow::Result<Option<SyncRunSummary>> {
    if let Some(summary) = {
        let states = SYNC_RUN_STATES.read().await;
        states
            .values()
            .find(|state| state.library_id == library_id && state.status.is_active())
            .map(SyncRunState::summary)
    } {
        return Ok(Some(summary));
    }
    let db_read = db.read().await;
    db::sync_runs::active_for_library(&db_read, library_id)?
        .map(record_to_summary)
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
    let removed = db::sync_runs::delete_records_missing_summary_fields(&mut db_write)?;
    if removed > 0 {
        tracing::info!(removed, "dropped legacy sync run records");
    }
    let records = db::sync_runs::list(&db_write)?;
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
        db::sync_runs::update(&mut db_write, &record)?;
    }
    Ok(())
}

async fn persist_record(db: &DbAsync, mut record: db::sync_runs::SyncRunRecord) {
    let run_id = record.id.clone();
    let result = async {
        let mut db_write = db.write().await;
        let existing = db::sync_runs::get_by_id(&db_write, &run_id)?;
        record.db_id = existing.as_ref().and_then(|run| run.db_id);
        if existing.is_some() {
            db::sync_runs::update(&mut db_write, &record)?;
        } else {
            db::sync_runs::create(&mut db_write, &record)?;
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;
    if let Err(err) = result {
        tracing::warn!(run_id, error = %err, "failed to persist sync run summary");
    }
}

fn record_to_summary(record: db::sync_runs::SyncRunRecord) -> anyhow::Result<SyncRunSummary> {
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

    #[test]
    fn summary_exposes_compact_raw_counts_without_diagnostic_fields() {
        let mut state = SyncRunState::new(SyncRunKind::LibrarySync, "lib".to_string(), 10_000);
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
        let mut keys = value
            .as_object()
            .expect("summary object")
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "active_units",
                "current",
                "failure_count",
                "progress",
                "run",
                "sequence",
            ]
        );
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
