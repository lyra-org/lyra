// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::cleanup::{
    ensure_hls_cleanup_worker_started,
    reset_cleanup_worker_state,
};

pub(crate) async fn initialize() {
    crate::services::transcoding::capacity::notify_changed();
    reset_cleanup_worker_state();
    ensure_hls_cleanup_worker_started().await;
}
