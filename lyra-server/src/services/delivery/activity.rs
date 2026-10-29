// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Whether the server is delivering audio right now, for work that should
//! wait until nothing is playing.

use std::{
    sync::atomic::{
        AtomicUsize,
        Ordering,
    },
    time::{
        SystemTime,
        UNIX_EPOCH,
    },
};

use lyra_db::PlaybackState;

use crate::services::{
    STATE,
    hls::state::hls_registry_counts,
    playback_sessions::ACTIVE_SESSION_TTL_MS,
};

static STREAMS: AtomicUsize = AtomicUsize::new(0);

/// An audio response body still being sent; counts as playing until it
/// drops.
#[must_use]
pub(crate) struct ActiveStream(());

impl Drop for ActiveStream {
    fn drop(&mut self) {
        STREAMS.fetch_sub(1, Ordering::Relaxed);
    }
}

pub(crate) fn begin_stream() -> ActiveStream {
    STREAMS.fetch_add(1, Ordering::Relaxed);
    ActiveStream(())
}

/// Whether anything is playing: an audio response is still being sent, an
/// HLS session is open, or a playback session reported playing or buffering
/// within [`ACTIVE_SESSION_TTL_MS`].
pub(crate) async fn is_playing() -> anyhow::Result<bool> {
    if STREAMS.load(Ordering::Relaxed) > 0 {
        return Ok(true);
    }
    let (_, hls_sessions) = hls_registry_counts().await;
    if hls_sessions > 0 {
        return Ok(true);
    }
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
    let cutoff_ms = now_ms.saturating_sub(ACTIVE_SESSION_TTL_MS);
    let db = STATE.db.read().await;
    Ok(lyra_db::playback_sessions::get(&db)?.iter().any(|session| {
        matches!(
            session.state,
            PlaybackState::Playing | PlaybackState::Buffering
        ) && session.updated_at_ms >= cutoff_ms
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn streams_count_as_playing_until_they_drop() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;

        assert!(!is_playing().await?);
        let stream = begin_stream();
        assert!(is_playing().await?);
        drop(stream);
        assert!(!is_playing().await?);
        Ok(())
    }
}
