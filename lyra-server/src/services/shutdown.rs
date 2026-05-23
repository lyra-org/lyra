// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::{
    LazyLock,
    RwLock,
};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

pub(crate) const TRANSCODE_ABORT_TIMEOUT: Duration = Duration::from_secs(2);

static SERVER_SHUTDOWN: LazyLock<RwLock<CancellationToken>> =
    LazyLock::new(|| RwLock::new(CancellationToken::new()));

pub(crate) fn token() -> CancellationToken {
    SERVER_SHUTDOWN
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

pub(crate) fn reset() -> CancellationToken {
    let token = CancellationToken::new();
    *SERVER_SHUTDOWN
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = token.clone();
    token
}

pub(crate) fn cancel() {
    token().cancel();
}
