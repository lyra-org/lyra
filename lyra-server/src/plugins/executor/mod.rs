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
    cell::{
        Cell,
        RefCell,
    },
    collections::HashMap,
    rc::Rc,
    sync::Arc,
};

use harmony_core::ModuleId;
use harmony_core::{
    CallContext,
    ChunkOrigin,
    LocalScheduler,
    SchedulerWake,
    TaskState,
    plugin::Runtime,
};

/// The executor keeps no work of its own to poll: a handler, a stream and a restart each run as a
/// Luau thread on the scheduler, watched by a task there that waits for the thread's end, its
/// caller's leaving and its deadline, then cancels the thread and answers.
pub(crate) struct PluginExecutor {
    runtime: Runtime,
    scope: crate::plugins::state::VmScope,
    running: callbacks::RunningHandlers,
    websocket_tasks: RefCell<HashMap<TaskIdKey, Arc<WebSocketState>>>,
    /// A restarted plugin's entrypoint is running.
    restarting: Rc<Cell<bool>>,
    /// Ends [`Self::wait_for_work`] for a command sent to the executor.
    wake: SchedulerWake,
}

impl std::ops::Deref for PluginExecutor {
    type Target = Runtime;

    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

impl PluginExecutor {
    /// One round of the executor's work: every task that is ready runs once.
    fn poll_background_tasks(&self) {
        let Ok(scheduler) = self.vm.data().get::<LocalScheduler>() else {
            return;
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
    }

    /// Sleeps until a round has something to do: a task or [`Self::wake`] wakes the scheduler, or
    /// a sleeping task or a deadline is due.
    fn wait_for_work(&self) {
        self.vm
            .data()
            .get::<LocalScheduler>()
            .expect("a runtime keeps its scheduler until it shuts down")
            .wait_for_work(None);
    }

    /// Ends [`Self::wait_for_work`] from any thread.
    pub(crate) fn wake_handle(&self) -> SchedulerWake {
        self.wake.clone()
    }

    /// [`now`] for this executor's VM.
    fn now(&self) -> std::time::Instant {
        now(&self.vm).expect("a runtime keeps its scheduler until it shuts down")
    }
}

/// The time by `vm`'s scheduler. The executor makes every deadline it gives the scheduler from
/// this and checks each against it, so a test that installs a manual clock decides when they
/// pass. A deadline that arrives with a request is the caller's to make: by the real clock in
/// production, which the scheduler's is there, and from the installed clock in such a test.
pub(crate) fn now(vm: &harmony_luau::Vm) -> harmony_luau::runtime::Result<std::time::Instant> {
    Ok(vm.data().get::<LocalScheduler>()?.now())
}

/// A thread its watcher answers for. However the watcher goes, the thread doesn't outlive it.
struct WatchedThread {
    scheduler: std::rc::Weak<LocalScheduler>,
    thread: harmony_luau::Thread,
}

impl WatchedThread {
    fn new(scheduler: &Rc<LocalScheduler>, thread: harmony_luau::Thread) -> Self {
        Self {
            scheduler: Rc::downgrade(scheduler),
            thread,
        }
    }

    /// Cancels the thread, which runs no further, and completes once its work is dropped.
    async fn cancel(&self) {
        let settled = self
            .scheduler
            .upgrade()
            .map(|scheduler| scheduler.cancel_luau_thread_and_settle(&self.thread));
        if let Some(settled) = settled {
            settled.await;
        }
    }
}

impl Drop for WatchedThread {
    fn drop(&mut self) {
        // Nothing for a thread that ended; a watcher that panicked leaves one that hasn't.
        if let Some(scheduler) = self.scheduler.upgrade() {
            scheduler.schedule_cancel_luau_thread(self.thread.clone());
        }
    }
}

/// The context of a task that watches one of `plugin`'s threads. It is in no run's group, so it
/// outlives the run to answer for the thread.
fn watcher_context(plugin: Option<Arc<str>>) -> CallContext {
    CallContext {
        origin: ChunkOrigin {
            plugin,
            ..ChunkOrigin::default()
        },
        ..CallContext::default()
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
