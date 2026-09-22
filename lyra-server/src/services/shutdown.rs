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
    token().cancel();
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
