// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The steps every playback report shares, whether one of Lyra's own clients or a plugin makes
//! it, and what follows when a reporter goes away.

use agdb::{
    DbAny,
    DbId,
};
use lyra_db::{
    PlaybackSession,
    PlaybackState,
};

use super::{
    DetachedSession,
    PlaybackError,
    PlaybackUpdate,
    ReportingConnection,
    bindings::{
        self,
        ConnectionKey,
    },
};
use crate::services::playback_sessions::{
    self,
    EvictedPlaybackRecord,
    PlaybackRecord,
    TransactionPlaybackUpdate,
};

/// What a report's transaction changed.
pub(super) struct Transition {
    pub(super) playback: lyra_db::playbacks::Playback,
    pub(super) session: TransactionPlaybackUpdate,
    pub(super) displaced_session: Option<DetachedSession>,
}

/// Finishes a committed report: announces the listens it recorded, binds the reporting
/// connection, unbinds the sessions it ended or displaced, and evicts stale sessions.
pub(super) fn finish(
    db: &mut DbAny,
    transition: Transition,
    reporting_connection: Option<&ReportingConnection>,
    now_ms: u64,
) -> PlaybackUpdate {
    let Transition {
        playback,
        session,
        displaced_session,
    } = transition;
    session.announce_recorded_listen();
    if let Some(displaced) = &displaced_session {
        displaced.announce_recorded_listen();
    }

    let ended = session.playback.playback.state.is_terminal();
    // Bound before the displaced session is cleared, so the reporter moves straight from the
    // replaced track session to its successor.
    if let Some(connection) = reporting_connection
        && !ended
    {
        connection.bind(&playback.id, &session.playback, now_ms);
    }
    if ended {
        bindings::clear_session(
            session.playback.playback_session_id,
            &session.playback.playback_session_public_id,
        );
    }
    if let Some(displaced) = &displaced_session {
        bindings::clear_session(
            displaced.playback_session_id,
            &displaced.playback_session_public_id,
        );
    }
    let evicted_playbacks = cleanup_after_commit(db, now_ms);
    PlaybackUpdate {
        playback,
        event: session.event,
        session: session.playback,
        displaced_session,
        evicted_playbacks,
    }
}

/// Removes stale track sessions nothing holds: no playback, as its current or recent previous
/// session, and no connection bound since the active window.
pub(crate) fn cleanup_evicted_playbacks(
    db: &mut DbAny,
    now_ms: u64,
) -> Result<Vec<EvictedPlaybackRecord>, PlaybackError> {
    bindings::cleanup_stale(now_ms, playback_sessions::ACTIVE_SESSION_TTL_MS);
    let active_cutoff_ms = now_ms.saturating_sub(playback_sessions::ACTIVE_SESSION_TTL_MS);
    let evicted = lyra_db::playback_sessions::evict_stale(db, now_ms, |playback_session_id| {
        bindings::holds_session(playback_session_id, active_cutoff_ms)
    })?;
    bindings::clear_sessions(&evicted);
    Ok(playback_sessions::evicted_records(db, evicted)?)
}

/// [`cleanup_evicted_playbacks`] after a mutation already committed: a failure is logged and only
/// delays eviction to the next cleanup, rather than failing the committed mutation.
pub(crate) fn cleanup_after_commit(db: &mut DbAny, now_ms: u64) -> Vec<EvictedPlaybackRecord> {
    cleanup_evicted_playbacks(db, now_ms).unwrap_or_else(|error| {
        tracing::error!(%error, "playback cleanup failed after durable mutation committed");
        Vec::new()
    })
}

/// The stored session and its track, while the track still exists.
pub(super) fn stored_session(
    db: &impl lyra_db::DbAccess,
    playback_session_id: DbId,
) -> anyhow::Result<Option<(PlaybackSession, DbId)>> {
    let Some(session) = lyra_db::playback_sessions::get_by_id(db, playback_session_id)? else {
        return Ok(None);
    };
    Ok(
        lyra_db::playback_sessions::get_track_id(db, playback_session_id)?
            .map(|track_db_id| (session, track_db_id)),
    )
}

/// Pauses each playing session at its time, as a reporter that went away leaves it; returns the
/// sessions it paused.
pub(super) fn pause_left_sessions(
    db: &mut DbAny,
    user_db_id: DbId,
    sessions: impl IntoIterator<Item = (PlaybackSession, u64)>,
) -> Result<Vec<PlaybackRecord>, PlaybackError> {
    let mut paused = Vec::new();
    for (session, pause_at_ms) in sessions {
        if session.state != PlaybackState::Playing {
            continue;
        }
        let update = db.transaction_mut(|t| {
            playback_sessions::pause_playback_in_transaction(t, session, user_db_id, pause_at_ms)
        })?;
        if let Some(update) = update {
            update.announce_recorded_listen();
            paused.push(update.playback);
        }
    }
    Ok(paused)
}

/// Pauses the playback a closed connection of Lyra's own clients drove, and unbinds it, unless a
/// report rebound its session key since it closed with the binding epoch `binding_epoch_at_close`.
/// Plugin sessions sharing the session key are left alone. Does nothing once the user is gone;
/// deleting a user clears its bindings.
pub(crate) fn pause_disconnected_connection(
    db: &mut DbAny,
    user_public_id: &str,
    session_key: &str,
    binding_epoch_at_close: Option<u64>,
    now_ms: u64,
) -> Result<(Vec<PlaybackRecord>, Vec<EvictedPlaybackRecord>), PlaybackError> {
    let Some(user_db_id) = lyra_db::users::find_db_id_by_public_id(db, user_public_id)? else {
        return Ok((Vec::new(), Vec::new()));
    };
    let key = ConnectionKey::new(user_public_id, session_key);
    let mut paused = Vec::new();
    if let Some(binding) = bindings::get(&key)
        .filter(|binding| Some(binding.current_binding_epoch) == binding_epoch_at_close)
    {
        let bound = |id: Option<DbId>, public_id: Option<&str>| -> anyhow::Result<_> {
            let Some(id) = id else {
                return Ok(None);
            };
            Ok(stored_session(db, id)?
                .map(|(session, _)| session)
                .filter(|session| Some(session.id.as_str()) == public_id))
        };
        let current = bound(
            binding.current_playback_session_id,
            binding.current_playback_session_public_id.as_deref(),
        )?;
        let previous = bound(
            binding.previous_playback_session_id,
            binding.previous_playback_session_public_id.as_deref(),
        )?
        .filter(|_| binding.previous_playback_session_id != binding.current_playback_session_id);
        let left = current.map(|session| (session, now_ms)).into_iter().chain(
            previous.map(|session| (session, binding.previous_demoted_at_ms.unwrap_or(now_ms))),
        );
        paused = pause_left_sessions(db, user_db_id, left)?;
        bindings::clear(&key);
    }
    let evicted_playbacks = cleanup_after_commit(db, now_ms);
    Ok((paused, evicted_playbacks))
}

#[cfg(test)]
mod tests {
    use lyra_db::fixtures::{
        insert_track,
        test_user,
    };

    use super::*;
    use crate::services::playback_sessions::{
        ActiveEvent,
        PlaybackMutation,
    };
    use crate::services::playbacks::{
        CreatePlaybackRequest,
        QueueSnapshot,
        ReportOutcome,
        ReportedPlaybackRequest,
        ValidatedQueue,
        create_playback,
        report_reported_playback,
    };

    fn playing(position_ms: u64) -> PlaybackMutation {
        PlaybackMutation {
            position_ms: Some(position_ms),
            duration_ms: Some(200_000),
            state: Some(PlaybackState::Playing),
            ..PlaybackMutation::default()
        }
    }

    /// Creates a playback of a new track, driven by the user's connection with `session_key`.
    fn drive(
        db: &mut DbAny,
        user_db_id: DbId,
        session_key: &str,
        position_ms: u64,
        now_ms: u64,
    ) -> anyhow::Result<PlaybackRecord> {
        let track_db_id = insert_track(db, "Track")?;
        let user_public_id = lyra_db::lookup::find_id_by_db_id(db, user_db_id)?.expect("user id");
        Ok(create_playback(
            db,
            CreatePlaybackRequest {
                id: nanoid::nanoid!(),
                play_id: nanoid::nanoid!(),
                user_db_id,
                client_name: None,
                queue: ValidatedQueue {
                    snapshot: QueueSnapshot::single(
                        lyra_db::lookup::find_id_by_db_id(db, track_db_id)?.expect("track id"),
                    ),
                    current_track_db_id: track_db_id,
                },
                mutation: playing(position_ms),
                now_ms,
                reporting_connection: Some(ReportingConnection {
                    user_public_id,
                    session_key: session_key.to_string(),
                }),
            },
        )?
        .session)
    }

    fn binding_epoch(user_public_id: &str, session_key: &str) -> Option<u64> {
        bindings::get(&ConnectionKey::new(user_public_id, session_key))
            .map(|binding| binding.current_binding_epoch)
    }

    #[test]
    fn a_closed_connection_pauses_only_its_own_playback() -> anyhow::Result<()> {
        let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
        crate::services::testing::init_default_test_state()?;
        let mut db = lyra_db::test_db::new_test_db()?;
        let user = test_user("alice")?;
        let user_public_id = user.id.clone();
        let user_db_id = lyra_db::users::create(&mut db, &user)?;
        let track_db_id = insert_track(&mut db, "Reported")?;
        let ReportOutcome::Applied(reported) = report_reported_playback(
            &mut db,
            ReportedPlaybackRequest {
                plugin_id: "demo",
                user_db_id,
                session_key: "device:1",
                track_db_id,
                play_id: None,
                client_name: None,
                mutation: playing(10_000),
                now_ms: 1_000,
                active_event: ActiveEvent::Started,
            },
        )?
        else {
            anyhow::bail!("the plugin's report is applied");
        };
        let native = drive(&mut db, user_db_id, "device:1", 20_000, 2_000)?;
        let epoch = binding_epoch(&user_public_id, "device:1");

        let (paused, evicted) =
            pause_disconnected_connection(&mut db, &user_public_id, "device:1", epoch, 4_000)?;

        assert!(evicted.is_empty());
        assert_eq!(paused.len(), 1);
        assert_eq!(paused[0].playback_session_id, native.playback_session_id);
        assert_eq!(paused[0].playback.state, PlaybackState::Paused);
        assert_eq!(paused[0].playback.position_ms, 20_000);
        assert_eq!(paused[0].playback.played_ms, 0);
        assert_eq!(paused[0].playback.updated_at_ms, 4_000);
        assert!(bindings::get(&ConnectionKey::new(&user_public_id, "device:1")).is_none());
        let plugin_session =
            lyra_db::playback_sessions::get_by_id(&db, reported.session.playback_session_id)?
                .expect("the plugin's session");
        assert_eq!(
            plugin_session.state,
            PlaybackState::Playing,
            "a plugin session sharing the session key is not the connection's"
        );
        Ok(())
    }

    #[test]
    fn a_closed_connection_pauses_its_previous_playback_when_it_was_replaced() -> anyhow::Result<()>
    {
        let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
        crate::services::testing::init_default_test_state()?;
        let mut db = lyra_db::test_db::new_test_db()?;
        let user = test_user("alice")?;
        let user_public_id = user.id.clone();
        let user_db_id = lyra_db::users::create(&mut db, &user)?;
        let first = drive(&mut db, user_db_id, "device:1", 10_000, 1_000)?;
        let second = drive(&mut db, user_db_id, "device:1", 20_000, 2_000)?;
        let epoch = binding_epoch(&user_public_id, "device:1");

        let (paused, _) =
            pause_disconnected_connection(&mut db, &user_public_id, "device:1", epoch, 5_000)?;

        let paused_at = |session: &PlaybackRecord| {
            paused
                .iter()
                .find(|paused| paused.playback_session_id == session.playback_session_id)
                .map(|paused| {
                    (
                        paused.playback.state,
                        paused.playback.position_ms,
                        paused.playback.updated_at_ms,
                    )
                })
        };
        assert_eq!(
            paused_at(&first),
            Some((PlaybackState::Paused, 10_000, 2_000)),
            "the replaced playback pauses when it was replaced"
        );
        assert_eq!(
            paused_at(&second),
            Some((PlaybackState::Paused, 20_000, 5_000))
        );
        Ok(())
    }
}
