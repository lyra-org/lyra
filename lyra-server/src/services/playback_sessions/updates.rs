// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        BTreeMap,
        HashMap,
    },
    sync::{
        Arc,
        LazyLock,
        Mutex,
    },
    time::Instant,
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
const DISPATCH_RATE_PER_SECOND: f64 = 10.0;
const DISPATCH_BURST: f64 = 50.0;
const DEFAULT_DISPATCH_CALLER: &str = "route";

static EVENT_BROADCAST: LazyLock<broadcast::Sender<PlaybackUpdatePayload>> =
    LazyLock::new(|| broadcast::channel(EVENT_BROADCAST_CAPACITY).0);
static DISPATCH_BUCKETS: LazyLock<Mutex<HashMap<String, DispatchBucket>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Clone, Copy, Debug)]
struct DispatchBucket {
    tokens: f64,
    last_refill: Instant,
}

impl DispatchBucket {
    fn new(now: Instant) -> Self {
        Self {
            tokens: DISPATCH_BURST,
            last_refill: now,
        }
    }

    fn allow(&mut self, now: Instant) -> bool {
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * DISPATCH_RATE_PER_SECOND).min(DISPATCH_BURST);
        self.last_refill = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

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
    pub playback_session_public_id: String,
    pub track_public_id: String,
    pub user_public_id: String,
    pub library_public_id: Option<String>,
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
        playback_session_public_id: playback.playback_session_public_id.clone(),
        track_public_id: playback.track_public_id.clone(),
        user_public_id: playback.user_public_id.clone(),
        library_public_id: playback.library_public_id.clone(),
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
    dispatch_playback_update_for_caller(DEFAULT_DISPATCH_CALLER, playback, event);
}

pub(crate) fn dispatch_playback_update_for_caller(
    caller: impl Into<String>,
    playback: &playbacks::PlaybackRecord,
    event: String,
) {
    dispatch_update_for_caller(caller, playback_to_payload(playback, event));
}

pub(crate) fn dispatch_evicted_updates(evicted_playbacks: Vec<playbacks::EvictedPlaybackRecord>) {
    dispatch_evicted_updates_for_caller(DEFAULT_DISPATCH_CALLER, evicted_playbacks);
}

pub(crate) fn dispatch_evicted_updates_for_caller(
    caller: impl Into<String>,
    evicted_playbacks: Vec<playbacks::EvictedPlaybackRecord>,
) {
    let caller = caller.into();
    for evicted in evicted_playbacks {
        let playback: playbacks::PlaybackRecord = evicted.into();
        dispatch_playback_update_for_caller(caller.clone(), &playback, String::from("evicted"));
    }
}

pub(crate) fn dispatch_update_for_caller(
    caller: impl Into<String>,
    payload: PlaybackUpdatePayload,
) {
    let caller = caller.into();
    if !dispatch_allowed(&caller) {
        tracing::warn!(
            caller = %caller,
            playback_session_public_id = %payload.playback_session_public_id,
            event = %payload.event,
            "playback update dispatch rate-limited"
        );
        return;
    }

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
                        playback_session_public_id = %payload.playback_session_public_id,
                        event = %payload.event,
                        error = %error,
                        "failed to convert playback update payload to lua value"
                    );
                    continue;
                }
            };
            if let Err(error) = handler.call_async::<_, ()>(lua_payload).await {
                tracing::warn!(
                    playback_session_public_id = %payload.playback_session_public_id,
                    event = %payload.event,
                    plugin_id = %handler.plugin_id(),
                    error = %error,
                    "playback on_update callback failed"
                );
            }
        }
    });
}

fn dispatch_allowed(caller: &str) -> bool {
    let now = Instant::now();
    let mut buckets = DISPATCH_BUCKETS
        .lock()
        .expect("dispatch bucket mutex poisoned");
    buckets
        .entry(caller.to_string())
        .or_insert_with(|| DispatchBucket::new(now))
        .allow(now)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_bucket_allows_burst_then_limits() {
        let now = Instant::now();
        let mut bucket = DispatchBucket::new(now);
        for _ in 0..DISPATCH_BURST as usize {
            assert!(bucket.allow(now));
        }
        assert!(!bucket.allow(now));
        assert!(bucket.allow(now + std::time::Duration::from_millis(100)));
    }
}
