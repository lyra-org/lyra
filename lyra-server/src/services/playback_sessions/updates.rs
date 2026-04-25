// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        LazyLock,
    },
};

use mlua::LuaSerdeExt;
use serde::{
    Deserialize,
    Serialize,
};
use tokio::{
    spawn,
    sync::{
        RwLock,
        broadcast,
    },
};

use crate::db::PlaybackState;
use crate::plugins::LUA_SERIALIZE_OPTIONS;
use crate::plugins::lifecycle::{
    PluginFunctionHandle,
    PluginId,
    PluginScopedInner,
    ScopedRegistry,
};
use crate::services::playback_sessions as playbacks;

pub(crate) static PLAYBACK_CALLBACK_REGISTRY: LazyLock<Arc<RwLock<PlaybackCallbackRegistry>>> =
    LazyLock::new(|| Arc::new(RwLock::new(PlaybackCallbackRegistry::new())));

/// Broadcast channel capacity for playback state events pushed to WS clients.
const EVENT_BROADCAST_CAPACITY: usize = 64;

static EVENT_BROADCAST: LazyLock<broadcast::Sender<PlaybackUpdatePayload>> =
    LazyLock::new(|| broadcast::channel(EVENT_BROADCAST_CAPACITY).0);

pub(crate) fn subscribe_playback_events() -> broadcast::Receiver<PlaybackUpdatePayload> {
    EVENT_BROADCAST.subscribe()
}

/// Callbacks registered via `lyra.playback_sessions.on_update`, bucketed per
/// plugin. `BTreeMap` for stable dispatch order — don't let hash order become
/// an implicit API.
#[derive(Default)]
pub(crate) struct PlaybackCallbackRegistry {
    update_handlers: BTreeMap<PluginId, Vec<PluginFunctionHandle>>,
}

impl PlaybackCallbackRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn add_update_handler(&mut self, handle: PluginFunctionHandle) {
        let plugin_id = handle.plugin_id().clone();
        self.update_handlers
            .entry(plugin_id)
            .or_default()
            .push(handle);
    }

    pub(crate) fn snapshot_handlers(&self) -> Vec<PluginFunctionHandle> {
        self.update_handlers
            .values()
            .flat_map(|handles| handles.iter().cloned())
            .collect()
    }

    pub(crate) fn clear_all_handlers(&mut self) {
        self.update_handlers.clear();
    }
}

impl PluginScopedInner for PlaybackCallbackRegistry {
    fn clear_bucket(&mut self, plugin_id: &PluginId) {
        self.update_handlers.remove(plugin_id);
    }

    fn rebuild_derived(&mut self) {
        // No side-car state: snapshot_handlers rebuilds on every call.
    }
}

pub(crate) async fn reset_callback_registry_for_test() {
    PLAYBACK_CALLBACK_REGISTRY
        .write()
        .await
        .clear_all_handlers();
}

pub(crate) async fn teardown_plugin_callbacks(plugin_id: &PluginId) {
    ScopedRegistry::from_shared(PLAYBACK_CALLBACK_REGISTRY.clone())
        .teardown(plugin_id)
        .await;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[harmony_macros::interface]
pub(crate) struct PlaybackUpdatePayload {
    pub event: String,
    pub state: PlaybackState,
    pub playback_session_id: i64,
    pub track_id: i64,
    pub user_id: i64,
    pub position_ms: u64,
    pub duration_ms: Option<u64>,
    pub activity_ms: u64,
    pub qualifies_single_listen: bool,
    pub updated_at_ms: u64,
}

pub(crate) fn playback_to_payload(
    playback: &playbacks::PlaybackRecord,
    event: String,
) -> PlaybackUpdatePayload {
    let activity_ms = playbacks::playback_activity_ms(&playback.playback);
    PlaybackUpdatePayload {
        event,
        state: playback.playback.state,
        playback_session_id: playback.playback_session_id.0,
        track_id: playback.track_db_id.0,
        user_id: playback.user_db_id.0,
        position_ms: playback.playback.position_ms,
        duration_ms: playback.playback.duration_ms,
        activity_ms,
        qualifies_single_listen: crate::db::playback_sessions::activity_meets_listen_threshold(
            activity_ms,
            playback.playback.duration_ms,
        ),
        updated_at_ms: playback.playback.updated_at_ms,
    }
}

pub(crate) fn dispatch_playback_update(playback: &playbacks::PlaybackRecord, event: String) {
    dispatch_update(playback_to_payload(playback, event));
}

pub(crate) fn dispatch_evicted_updates(evicted_playbacks: Vec<playbacks::EvictedPlaybackRecord>) {
    for evicted in evicted_playbacks {
        let playback: playbacks::PlaybackRecord = evicted.into();
        dispatch_playback_update(&playback, String::from("evicted"));
    }
}

pub(crate) fn dispatch_update(payload: PlaybackUpdatePayload) {
    // Fan out to WS broadcast (best-effort, dropped if no subscribers or lagging).
    let _ = EVENT_BROADCAST.send(payload.clone());

    spawn(async move {
        let handlers: Vec<PluginFunctionHandle> = {
            let registry = PLAYBACK_CALLBACK_REGISTRY.read().await;
            registry.snapshot_handlers()
        };

        if handlers.is_empty() {
            return;
        }

        let lua = crate::STATE.lua.get();
        for handler in handlers {
            let lua_payload = match lua.to_value_with(&payload, LUA_SERIALIZE_OPTIONS) {
                Ok(value) => value,
                Err(error) => {
                    tracing::warn!(
                        playback_session_id = payload.playback_session_id,
                        event = %payload.event,
                        error = %error,
                        "failed to convert playback update payload to lua value"
                    );
                    continue;
                }
            };
            if let Err(error) = handler.call_async::<_, ()>(lua_payload).await {
                tracing::warn!(
                    playback_session_id = payload.playback_session_id,
                    event = %payload.event,
                    plugin_id = %handler.plugin_id(),
                    error = %error,
                    "playback on_update callback failed"
                );
            }
        }
    });
}
