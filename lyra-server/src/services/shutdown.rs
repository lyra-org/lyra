// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::{
    LazyLock,
    Mutex,
    RwLock,
};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

pub(crate) const TRANSCODE_ABORT_TIMEOUT: Duration = Duration::from_secs(2);

static SERVER_SHUTDOWN: LazyLock<RwLock<CancellationToken>> =
    LazyLock::new(|| RwLock::new(CancellationToken::new()));
static FAILURE: Mutex<Option<String>> = Mutex::new(None);
/// Taken to cancel a shutdown token and to act unless it is cancelled, so the
/// two never interleave.
static GATE: RwLock<()> = RwLock::new(());

pub(crate) fn token() -> CancellationToken {
    SERVER_SHUTDOWN
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

pub(crate) fn reset() -> CancellationToken {
    *FAILURE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    let token = CancellationToken::new();
    *SERVER_SHUTDOWN
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = token.clone();
    token
}

pub(crate) fn cancel() {
    cancel_gated(&token());
}

fn cancel_gated(token: &CancellationToken) {
    let _gate = GATE
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    token.cancel();
}

/// Runs `act` unless `shutdown` is cancelled. A shutdown started by [`cancel`]
/// comes wholly before or after it, so whatever `act` starts, shutdown sees.
pub(crate) fn unless_cancelled<T>(
    shutdown: &CancellationToken,
    act: impl FnOnce() -> T,
) -> Option<T> {
    let _gate = GATE.read().unwrap_or_else(|poisoned| poisoned.into_inner());
    (!shutdown.is_cancelled()).then(act)
}

/// Stops the server and makes it exit with an error once shutdown completes.
pub(crate) fn fail(reason: &str) {
    let mut failure = FAILURE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if failure.is_none() {
        tracing::error!(reason, "stopping server after fatal error");
        *failure = Some(reason.to_string());
    }
    drop(failure);
    cancel();
}

pub(crate) fn failure() -> Option<String> {
    FAILURE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        Mutex,
        mpsc,
    };

    use super::*;

    #[test]
    fn a_cancel_waits_for_an_act_it_started_during() {
        let shutdown = CancellationToken::new();
        let events = Arc::new(Mutex::new(Vec::new()));
        let (entered_tx, entered) = mpsc::channel();
        let acting = std::thread::spawn({
            let shutdown = shutdown.clone();
            let events = events.clone();
            move || {
                unless_cancelled(&shutdown, || {
                    entered_tx.send(()).unwrap();
                    std::thread::sleep(Duration::from_millis(50));
                    events.lock().unwrap().push("acted");
                })
            }
        });
        entered.recv().unwrap();
        cancel_gated(&shutdown);
        events.lock().unwrap().push("cancelled");

        assert_eq!(acting.join().unwrap(), Some(()));
        assert_eq!(*events.lock().unwrap(), ["acted", "cancelled"]);
        assert_eq!(unless_cancelled(&shutdown, || ()), None);
    }
}
