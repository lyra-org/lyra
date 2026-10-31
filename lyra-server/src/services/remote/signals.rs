// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Per-user notices that one of the user's connections may look different, which each of the
//! user's sockets turns into connection events.

use std::collections::HashMap;
use std::sync::{
    LazyLock,
    Mutex,
};

use tokio::sync::broadcast;

const SIGNAL_CAPACITY: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ConnectionSignal {
    /// The connection registered under this session key registered, declared capabilities, or
    /// had its native scope's current playback change.
    Changed {
        session_key: String,
    },
    Closed {
        id: String,
    },
}

static SENDERS: LazyLock<Mutex<HashMap<String, broadcast::Sender<ConnectionSignal>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub(crate) fn subscribe(user_public_id: &str) -> broadcast::Receiver<ConnectionSignal> {
    SENDERS
        .lock()
        .expect("connection signal mutex poisoned")
        .entry(user_public_id.to_string())
        .or_insert_with(|| broadcast::channel(SIGNAL_CAPACITY).0)
        .subscribe()
}

/// Drops the user's channel once no socket listens to it.
pub(crate) fn unsubscribe(user_public_id: &str) {
    let mut senders = SENDERS.lock().expect("connection signal mutex poisoned");
    if senders
        .get(user_public_id)
        .is_some_and(|sender| sender.receiver_count() == 0)
    {
        senders.remove(user_public_id);
    }
}

fn send(user_public_id: &str, signal: ConnectionSignal) {
    let senders = SENDERS.lock().expect("connection signal mutex poisoned");
    if let Some(sender) = senders.get(user_public_id) {
        let _ = sender.send(signal);
    }
}

pub(crate) fn changed(user_public_id: &str, session_key: &str) {
    send(
        user_public_id,
        ConnectionSignal::Changed {
            session_key: session_key.to_string(),
        },
    );
}

pub(crate) fn closed(user_public_id: &str, id: &str) {
    send(
        user_public_id,
        ConnectionSignal::Closed { id: id.to_string() },
    );
}
