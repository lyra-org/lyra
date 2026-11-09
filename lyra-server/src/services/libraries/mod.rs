// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod configured;
pub(crate) mod control;
mod orchestrator;
pub(crate) mod purge;
mod roots;
pub(crate) mod scanning;
pub(crate) mod sync;

pub(crate) use configured::{
    LibraryConfig,
    prepare_capture_library,
    prepare_configured_library,
};
pub(crate) use orchestrator::{
    LibraryRun,
    LibrarySyncRegistries,
    LibrarySyncStatus,
    RunFilter,
    SyncRunControlError,
    SyncRunFailure,
    SyncRunKind,
    SyncRunProgress,
    SyncRunStartResponse,
    SyncRunStatus,
    SyncRunSummary,
    SyncStageKey,
    SyncTotalState,
    SyncWorkDetails,
    cancel_sync_run,
    drain_library_syncs,
    library_has_active_run,
    library_mutation_guard,
    running_library_sync_count,
    start_library_sync,
    subscribe_sync_run_events,
    sync_run_failures,
    wait_for_running_library_syncs,
    wait_for_sync_runs,
};
#[cfg(test)]
pub(crate) use orchestrator::{
    begin_test_run,
    end_test_run,
    get_sync_run,
};
pub(crate) use roots::{
    MediaRoots,
    is_media_file,
};
pub(crate) use sync::system_context;
pub(crate) use sync::{
    LibrarySyncOptions,
    SyncReleases,
    sync_library,
};
