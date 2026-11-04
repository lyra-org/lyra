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
/// HLS session is open, or a live playback session is playing or buffering.
pub(crate) async fn is_playing() -> anyhow::Result<bool> {
    if STREAMS.load(Ordering::Relaxed) > 0 {
        return Ok(true);
    }
    let (_, hls_sessions) = hls_registry_counts().await;
    if hls_sessions > 0 {
        return Ok(true);
    }
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
    let db = STATE.db.read().await;
    Ok(lyra_db::playback_sessions::get(&db)?.iter().any(|session| {
        matches!(
            session.state,
            PlaybackState::Playing | PlaybackState::Buffering
        ) && session.is_live(now_ms)
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

    #[tokio::test]
    async fn a_live_playing_session_counts_as_playing_however_long_unreported() -> anyhow::Result<()>
    {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
        let reported_at_ms = now_ms - 10 * 60 * 1000;
        let mut session = lyra_db::PlaybackSession {
            db_id: None,
            id: nanoid::nanoid!(),
            play_id: nanoid::nanoid!(),
            client_name: None,
            plugin_id: Some("external".to_string()),
            position_ms: 0,
            duration_ms: Some(20 * 60 * 1000),
            played_ms: 0,
            last_position_ms: 0,
            last_position_at_ms: reported_at_ms,
            latency_allowance_used_ms: 0,
            rate_permille: 1000,
            counts_as_listen: true,
            state: PlaybackState::Playing,
            updated_at_ms: reported_at_ms,
            live_until_ms: 0,
            created_at_ms: reported_at_ms,
        };
        let session_db_id = {
            let mut db = STATE.db.write().await;
            let user = lyra_db::users::create(&mut db, &lyra_db::fixtures::test_user("player")?)?;
            let track = lyra_db::fixtures::insert_track(&mut db, "Long")?;
            db.transaction_mut(|t| {
                crate::services::playback_sessions::insert_session(t, &mut session, track, user)
            })?
        };
        assert!(
            is_playing().await?,
            "ten minutes unreported, a twenty-minute track still plays"
        );

        session.db_id = Some(session_db_id);
        session.state = PlaybackState::Paused;
        {
            let mut db = STATE.db.write().await;
            db.transaction_mut(|t| {
                crate::services::playback_sessions::update_session(t, &mut session)
            })?;
        }
        assert!(!is_playing().await?, "a live paused session plays nothing");

        session.state = PlaybackState::Playing;
        session.duration_ms = Some(60_000);
        {
            let mut db = STATE.db.write().await;
            db.transaction_mut(|t| {
                crate::services::playback_sessions::update_session(t, &mut session)
            })?;
        }
        assert!(
            !is_playing().await?,
            "past its live window it no longer plays"
        );
        Ok(())
    }
}
