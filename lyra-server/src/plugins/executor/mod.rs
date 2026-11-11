// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod actor;
mod api;
mod call_chain;
mod callbacks;
mod messages;
mod metadata;
mod mix;
mod modules;
mod stores;
mod stream;
mod updates;
mod vm;
mod websocket;
pub(crate) use self::actor::{
    DrainOutcome,
    ExecutorExitKind,
    ExecutorId,
    PluginExecutorHandle,
};
pub(crate) use self::call_chain::CallChain;
use self::messages::TaskIdKey;
pub(crate) use self::messages::{
    ApiFallbackResponse,
    ApiHandlerRequest,
    ApiHandlerResponse,
    ApiResponseBody,
    ApiResponseKind,
    IdLinkDispatchContext,
    MetadataDispatchContext,
    PluginSourceRun,
    WebSocketStartRequest,
    WebSocketState,
};
pub(crate) use self::metadata::{
    VmCalls,
    dispatch_id_links_in_vm,
};
pub(crate) use self::stream::{
    ApiResponseStream,
    ApiStreamWriter,
    MAX_STREAM_CHUNK_BYTES,
    StreamOrigin,
    queue_stream,
};
pub(crate) use self::websocket::{
    ApiWebSocketReader,
    ApiWebSocketSender,
};
use crate::services::plugin_dispatch::{
    DispatchCancellation,
    MetadataRefreshRequest,
    MetadataRefreshResult,
    MixHandlerRequest,
    MixHandlerResult,
    SimilarReleasesDispatchRequest,
    SimilarReleasesDispatchResult,
};

use std::{
    cell::RefCell,
    collections::HashMap,
    sync::Arc,
    time::Instant,
};

use harmony_core::ModuleId;
use harmony_core::{
    ChunkOrigin,
    LocalScheduler,
    SchedulerWake,
    TaskState,
    plugin::Runtime,
};

pub(crate) struct PluginExecutor {
    runtime: Runtime,
    scope: crate::plugins::state::VmScope,
    running: callbacks::RunningHandlers,
    websocket_tasks: RefCell<HashMap<TaskIdKey, Arc<WebSocketState>>>,
    callback_tasks: RefCell<Vec<callbacks::PendingCallbackTask>>,
    pending_restart: RefCell<Option<PendingRestart>>,
    /// Ends [`Self::wait_for_work`], for what the executor polls and the scheduler doesn't wake.
    wake: SchedulerWake,
    /// How many rounds the executor has run, for tests that tell a sleeping executor from one
    /// that polls.
    #[cfg(test)]
    rounds: Arc<std::sync::atomic::AtomicU64>,
}

/// A restarted plugin's entrypoint, polled between commands so the other plugins keep running.
struct PendingRestart {
    entrypoint: harmony_core::plugin::RunningEntrypoint,
    reply: tokio::sync::oneshot::Sender<anyhow::Result<()>>,
}

impl std::ops::Deref for PluginExecutor {
    type Target = Runtime;

    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

impl PluginExecutor {
    /// One round of the executor's work, returning the earliest deadline among what it polls.
    ///
    /// What the executor polls itself comes before the scheduler's tasks: a handler whose caller
    /// left or whose deadline passed is cancelled without running further, and a stream queued by
    /// a reply starts in the same round. All of it registers [`Self::wake`], so a task finishing
    /// in this round's scheduler poll brings on the round that reads its result.
    fn poll_background_tasks(&self) -> Option<Instant> {
        #[cfg(test)]
        self.rounds
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let deadline = self.poll_own_work();
        let Ok(scheduler) = self.vm.data().get::<LocalScheduler>() else {
            return deadline;
        };
        {
            let _guard = self.tokio_runtime.enter();
            scheduler.poll_ready();
        }
        let mut websockets = self.websocket_tasks.borrow_mut();
        if !websockets.is_empty() {
            for snapshot in scheduler.snapshots() {
                if snapshot.state != TaskState::Pending
                    && let Some(state) = websockets.remove(&TaskIdKey(snapshot.id.0))
                {
                    state.request_close();
                }
            }
        }
        scheduler.remove_finished();
        deadline
    }

    /// Answers for what the executor polls itself and has ended, returning the earliest deadline of
    /// the rest. Also run before a command, which may hold the thread past the deadline of a
    /// result the last round's scheduler poll left ready.
    fn poll_own_work(&self) -> Option<Instant> {
        [
            self.poll_pending_restart(),
            self.poll_callback_tasks(),
            stream::poll_streams(&self.vm),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// Sleeps until a round has something to do: a task or [`Self::wake`] wakes the scheduler, a
    /// sleeping task is due, or `deadline`, as the last round returned it, passes.
    fn wait_for_work(&self, deadline: Option<Instant>) {
        self.vm
            .data()
            .get::<LocalScheduler>()
            .expect("a runtime keeps its scheduler until it shuts down")
            .wait_for_work(deadline);
    }

    /// Ends [`Self::wait_for_work`] from any thread.
    pub(crate) fn wake_handle(&self) -> SchedulerWake {
        self.wake.clone()
    }
}

fn plugin_origin(plugin_id: impl Into<Arc<str>>, path: impl Into<Arc<str>>) -> ChunkOrigin {
    let plugin = plugin_id.into();
    let path = path.into();
    ChunkOrigin {
        module: Some(ModuleId(Arc::from(format!("plugins/{plugin}/{path}")))),
        plugin: Some(plugin.clone()),
        path: Some(Arc::from(format!("plugins/{plugin}/{path}"))),
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
