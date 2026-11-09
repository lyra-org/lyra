// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod actor;
mod api;
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
    dispatch_id_links_in_vm,
    dispatch_similar_releases_in_vm,
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
    time::Duration,
};

use harmony_core::ModuleId;
use harmony_core::{
    ChunkOrigin,
    LocalScheduler,
    TaskState,
    plugin::Runtime,
};

pub(crate) struct PluginExecutor {
    runtime: Runtime,
    scope: crate::plugins::state::VmScope,
    websocket_tasks: RefCell<HashMap<TaskIdKey, Arc<WebSocketState>>>,
    callback_tasks: RefCell<Vec<callbacks::PendingCallbackTask>>,
}

impl std::ops::Deref for PluginExecutor {
    type Target = Runtime;

    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

impl PluginExecutor {
    fn poll_background_tasks(&self) {
        self.poll_callback_tasks();
        stream::poll_streams(&self.vm);
        let Ok(scheduler) = self.vm.data().get::<LocalScheduler>() else {
            return;
        };
        {
            let _guard = self.tokio_runtime.enter();
            scheduler.poll_ready();
        }
        let snapshots = scheduler.snapshots();
        let mut completed_websockets = Vec::new();
        for snapshot in snapshots {
            if snapshot.state != TaskState::Pending {
                let key = TaskIdKey(snapshot.id.0);
                if let Some(state) = self.websocket_tasks.borrow().get(&key) {
                    state.request_close();
                    completed_websockets.push(key);
                }
            }
        }
        self.poll_callback_tasks();
        stream::poll_streams(&self.vm);
        scheduler.remove_finished();
        if !completed_websockets.is_empty() {
            let mut tasks = self.websocket_tasks.borrow_mut();
            for key in completed_websockets {
                tasks.remove(&key);
            }
        }
    }

    fn next_scheduler_delay(&self) -> Option<Duration> {
        let scheduler = self.vm.data().get::<LocalScheduler>().ok()?;
        if !scheduler.has_pending() {
            return None;
        }
        scheduler.next_wake_delay()
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
