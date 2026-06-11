// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod actor;
mod api;
mod messages;
mod metadata;
mod mix;
mod modules;
mod playback;
mod stores;
mod vm;
mod websocket;
pub(crate) use self::actor::PluginExecutorHandle;
use self::messages::TaskIdKey;
pub(crate) use self::messages::{
    ApiHandlerRequest,
    ApiHandlerResponse,
    ApiResponseBody,
    ApiResponseKind,
    MetadataRefreshRequest,
    MetadataRefreshResult,
    MixHandlerRequest,
    MixHandlerResult,
    WebSocketStartRequest,
    WebSocketState,
};
pub(crate) use self::modules::plugin_scope_ids_for_test;

use std::{
    cell::RefCell,
    collections::HashMap,
    sync::Arc,
    time::Duration,
};

#[cfg(test)]
use harmony_core::ModuleId;
use harmony_core::{
    ChunkOrigin,
    LocalScheduler,
    TaskState,
    plugin::Runtime,
};
use harmony_luau as luau;

pub(crate) struct PluginExecutor {
    runtime: Runtime,
    websocket_tasks: RefCell<HashMap<TaskIdKey, Arc<WebSocketState>>>,
}

impl std::ops::Deref for PluginExecutor {
    type Target = Runtime;

    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

impl PluginExecutor {
    fn poll_background_tasks(&self) {
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

#[cfg(test)]
fn plugin_origin(plugin_id: impl Into<Arc<str>>, path: impl Into<Arc<str>>) -> ChunkOrigin {
    let plugin = plugin_id.into();
    let path = path.into();
    ChunkOrigin {
        module: Some(ModuleId(Arc::from(format!("plugins/{plugin}/{path}")))),
        plugin: Some(plugin.clone()),
        path: Some(Arc::from(format!("plugins/{plugin}/{path}"))),
    }
}

fn luau_origin(origin: &ChunkOrigin) -> luau::ChunkOrigin {
    luau::ChunkOrigin {
        module: origin
            .module
            .as_ref()
            .map(|module| luau::ModuleId(module.0.clone())),
        plugin: origin.plugin.clone(),
        path: origin.path.clone(),
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
