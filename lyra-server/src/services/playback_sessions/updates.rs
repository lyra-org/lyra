// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashMap,
    sync::Mutex,
    time::Instant,
};

use serde::{
    Deserialize,
    Serialize,
};
use tokio::sync::broadcast;

use crate::services::playback_sessions as playbacks;
use lyra_db::PlaybackState;

/// Broadcast channel capacity for playback state events pushed to WS clients.
const EVENT_BROADCAST_CAPACITY: usize = 64;
const DISPATCH_RATE_PER_SECOND: f64 = 10.0;
const DISPATCH_BURST: f64 = 50.0;
const DEFAULT_DISPATCH_CALLER: &str = "route";

/// Generation-owned playback update state.
pub(crate) struct PlaybackUpdateRegistries {
    event_broadcasts: Mutex<HashMap<String, broadcast::Sender<PlaybackUpdatePayload>>>,
    dispatch_buckets: Mutex<HashMap<String, DispatchBucket>>,
}

impl Default for PlaybackUpdateRegistries {
    fn default() -> Self {
        Self {
            event_broadcasts: Mutex::new(HashMap::new()),
            dispatch_buckets: Mutex::new(HashMap::new()),
        }
    }
}

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

pub(crate) fn subscribe_playback_events(
    user_public_id: &str,
) -> broadcast::Receiver<PlaybackUpdatePayload> {
    let generation = crate::services::STATE.generation();
    let mut broadcasts = generation
        .playback_updates
        .event_broadcasts
        .lock()
        .expect("playback event broadcast mutex poisoned");
    broadcasts
        .entry(user_public_id.to_string())
        .or_insert_with(|| broadcast::channel(EVENT_BROADCAST_CAPACITY).0)
        .subscribe()
}

pub(crate) fn unsubscribe_playback_events(user_public_id: &str) {
    let generation = crate::services::STATE.generation();
    let mut broadcasts = generation
        .playback_updates
        .event_broadcasts
        .lock()
        .expect("playback event broadcast mutex poisoned");
    if broadcasts
        .get(user_public_id)
        .is_some_and(|sender| sender.receiver_count() == 0)
    {
        broadcasts.remove(user_public_id);
    }
}

fn broadcast_playback_event(payload: PlaybackUpdatePayload) {
    let generation = crate::services::STATE.generation();
    let mut broadcasts = generation
        .playback_updates
        .event_broadcasts
        .lock()
        .expect("playback event broadcast mutex poisoned");
    let user_public_id = payload.user_public_id.clone();
    let remove_idle = broadcasts
        .get(&user_public_id)
        .is_some_and(|sender| sender.send(payload).is_err());
    if remove_idle {
        broadcasts.remove(&user_public_id);
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct PlaybackUpdatePayload {
    pub event: String,
    pub state: PlaybackState,
    pub playback_session_public_id: String,
    pub track_public_id: String,
    pub user_public_id: String,
    pub library_public_id: Option<String>,
    pub position_ms: u64,
    pub duration_ms: Option<u64>,
    pub played_ms: u64,
    pub updated_at_ms: u64,
}

pub(crate) fn playback_to_payload(
    playback: &playbacks::PlaybackRecord,
    event: String,
) -> PlaybackUpdatePayload {
    PlaybackUpdatePayload {
        event,
        state: playback.playback.state,
        playback_session_public_id: playback.playback_session_public_id.clone(),
        track_public_id: playback.track_public_id.clone(),
        user_public_id: playback.user_public_id.clone(),
        library_public_id: playback.library_public_id.clone(),
        position_ms: playback.playback.position_ms,
        duration_ms: playback.playback.duration_ms,
        played_ms: playback.playback.played_ms,
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
    let generation = crate::services::STATE.generation();

    broadcast_playback_event(payload.clone());

    if !dispatch_allowed(&caller) {
        tracing::warn!(
            caller = %caller,
            playback_session_public_id = %payload.playback_session_public_id,
            event = %payload.event,
            "playback update plugin dispatch rate-limited"
        );
        return;
    }

    if let Some(runtime) = generation.plugin_dispatch.get()
        && let Err(error) = runtime.dispatch_playback_update(payload)
    {
        tracing::warn!(
            error = %error,
            "failed to enqueue plugin playback on_update dispatch"
        );
    }
}

fn dispatch_allowed(caller: &str) -> bool {
    let now = Instant::now();
    let generation = crate::services::STATE.generation();
    let mut buckets = generation
        .playback_updates
        .dispatch_buckets
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

    fn test_payload(session: &str, user_public_id: &str) -> PlaybackUpdatePayload {
        PlaybackUpdatePayload {
            event: "evicted".to_string(),
            state: PlaybackState::Stopped,
            playback_session_public_id: session.to_string(),
            track_public_id: "track".to_string(),
            user_public_id: user_public_id.to_string(),
            library_public_id: None,
            position_ms: 0,
            duration_ms: None,
            played_ms: 0,
            updated_at_ms: 0,
        }
    }

    /// The bucket bounds plugin dispatch only. WS subscribers must still see
    /// every payload, so the broadcast channel's own Lagged contract stays the
    /// single source of truth for dropped updates.
    #[test]
    fn rate_limited_updates_still_reach_event_broadcast() {
        let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
        crate::services::testing::init_default_test_state().expect("init test state");

        let mut receiver = subscribe_playback_events("user");
        let sends = DISPATCH_BURST as usize + 5;
        for index in 0..sends {
            dispatch_update_for_caller(
                DEFAULT_DISPATCH_CALLER,
                test_payload(&format!("session-{index}"), "user"),
            );
        }

        let received = (0..sends)
            .map(|_| receiver.try_recv())
            .take_while(Result::is_ok)
            .count();
        assert_eq!(received, sends.min(EVENT_BROADCAST_CAPACITY));
    }

    #[test]
    fn playback_event_lag_is_isolated_by_user() {
        let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
        crate::services::testing::init_default_test_state().expect("init test state");

        let mut flooded = subscribe_playback_events("flooded");
        let mut unrelated = subscribe_playback_events("unrelated");
        for index in 0..=EVENT_BROADCAST_CAPACITY {
            dispatch_update_for_caller(
                DEFAULT_DISPATCH_CALLER,
                test_payload(&format!("flood-{index}"), "flooded"),
            );
        }

        assert!(matches!(
            flooded.try_recv(),
            Err(broadcast::error::TryRecvError::Lagged(1))
        ));
        assert!(matches!(
            unrelated.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));

        dispatch_update_for_caller(
            DEFAULT_DISPATCH_CALLER,
            test_payload("unrelated-event", "unrelated"),
        );
        assert_eq!(
            unrelated.try_recv().expect("unrelated event"),
            test_payload("unrelated-event", "unrelated")
        );
    }
}
