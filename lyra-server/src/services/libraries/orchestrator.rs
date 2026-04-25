// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{
            AtomicU64,
            Ordering,
        },
    },
    time::{
        SystemTime,
        UNIX_EPOCH,
    },
};

use agdb::DbId;
use schemars::JsonSchema;
use serde::Serialize;
use std::sync::LazyLock;
use tokio::sync::RwLock;
use tokio::time::{
    Duration,
    sleep,
};

use crate::STATE;
use crate::db::{
    DbAsync,
    Library,
};

use super::sync::{
    add_metadata,
    full_sync,
};
use crate::services::{
    covers::eager_sync_cover_metadata,
    providers::{
        LibraryRefreshOptions,
        refresh_library_metadata,
    },
};

#[derive(Clone, Copy, Debug, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LibrarySyncRunStatus {
    Idle,
    Running,
    Succeeded,
    Failed,
}

#[derive(Clone, Copy, Debug, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LibrarySyncPhase {
    FullSync,
    Metadata,
    ProviderRefresh,
    Complete,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub(crate) struct LibrarySyncState {
    pub(crate) library_db_id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) run_id: Option<u64>,
    pub(crate) status: LibrarySyncRunStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) phase: Option<LibrarySyncPhase>,
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
            started_at: Some(now),
            updated_at: Some(now),
            finished_at: None,
            error: None,
        }
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
    let result = async {
        set_phase(library_db_id, run_id, LibrarySyncPhase::FullSync).await;
        let entries = full_sync(&db, &library).await?;

        set_phase(library_db_id, run_id, LibrarySyncPhase::Metadata).await;
        add_metadata(&db, &library, entries).await?;

        eager_sync_cover_metadata(&db, library_db_id, &library.directory).await;

        set_phase(library_db_id, run_id, LibrarySyncPhase::ProviderRefresh).await;
        let options = LibraryRefreshOptions {
            replace_cover: false,
            force_refresh: false,
            apply_sync_filters: false,
            provider_id: None,
        };
        if let Err(err) = refresh_library_metadata(library_db_id, &options).await {
            tracing::warn!(
                library_db_id = library_db_id.0,
                error = %err,
                "provider refresh failed during library sync"
            );
        }

        Ok::<(), anyhow::Error>(())
    }
    .await;

    match result {
        Ok(()) => {
            let mut db_write = db.write().await;
            if let Err(err) = db_write.optimize_storage() {
                tracing::warn!(error = %err, "failed to optimize storage after sync");
            }
            drop(db_write);
            super::super::storage_monitor::record_baseline(&STATE.config.get().db.path);
            mark_succeeded(library_db_id, run_id).await;
        }
        Err(err) => {
            tracing::error!(
                library_db_id = library_db_id.0,
                run_id,
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
