// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Queues a plugin's clients save, as ordinary queue-backed playbacks any client can resume.

use agdb::{
    DbAny,
    DbId,
};
use lyra_db::{
    PlaybackSession,
    PlaybackState,
    playbacks::Playback,
};

use super::{
    PlayableQueue,
    PlaybackError,
    QueueSnapshot,
    RepeatMode,
    playable_queue,
    removal::{
        self,
        DeletedPlayback,
    },
    validate_queue,
};
use crate::services::{
    auth::Principal,
    playback_sessions,
};

/// Whether a queue-backed playback's session is one a save wrote: stopped, never counting as a
/// listen, and reported by a plugin, which only saves do for queue-backed playbacks. Resuming one
/// starts a new play.
pub(crate) fn is_saved_session(session: &PlaybackSession) -> bool {
    session.state == PlaybackState::Stopped
        && !session.counts_as_listen
        && session.plugin_id.is_some()
}

/// A save of the queue the user's client of a plugin plays.
pub(crate) struct SaveQueueRequest<'a> {
    pub(crate) principal: &'a Principal,
    pub(crate) user_db_id: DbId,
    pub(crate) plugin_id: &'a str,
    /// The client, tidied; each client of the plugin saves to a playback of its own.
    pub(crate) client_name: Option<String>,
    pub(crate) track_ids: Vec<String>,
    pub(crate) current_index: u64,
    pub(crate) position_ms: u64,
    pub(crate) now_ms: u64,
}

pub(crate) struct SavedQueue {
    pub(crate) playback_id: String,
    /// The queue's new revision, when the save changed the queue of a playback it saved before.
    pub(crate) changed_revision: Option<u64>,
}

/// The newest queue-backed playback of the user that has a playable queue.
pub(crate) struct LatestQueue {
    pub(crate) playback_id: String,
    pub(crate) queue: PlayableQueue,
    /// The client and plugin of its current track session.
    pub(crate) client_name: Option<String>,
    pub(crate) plugin_id: Option<String>,
    pub(crate) updated_at_ms: u64,
}

/// The playback the client saved to, with its current session, while that session is still the
/// one the client's last save wrote.
fn own_saved_playback(
    db: &impl lyra_db::DbAccess,
    user_db_id: DbId,
    plugin_id: &str,
    client: &str,
) -> Result<Option<(Playback, DbId, PlaybackSession)>, PlaybackError> {
    let Some(playback) = lyra_db::playbacks::get_saved(db, user_db_id, plugin_id, client)? else {
        return Ok(None);
    };
    let playback_db_id = persisted_id(&playback)?;
    let Some(session_id) = lyra_db::playbacks::get_current_session_id(db, playback_db_id)? else {
        return Ok(None);
    };
    Ok(lyra_db::playback_sessions::get_by_id(db, session_id)?
        .filter(|session| {
            is_saved_session(session)
                && session.plugin_id.as_deref() == Some(plugin_id)
                && session.client_name.as_deref().unwrap_or("") == client
        })
        .map(|session| (playback, session_id, session)))
}

fn persisted_id(playback: &Playback) -> Result<DbId, PlaybackError> {
    Ok(playback
        .db_id
        .ok_or_else(|| anyhow::anyhow!("persisted playback missing db_id"))?)
}

/// Moves the saved session to `position_ms`, within its track.
fn move_saved_session(
    db: &mut impl lyra_db::DbAccess,
    mut session: PlaybackSession,
    position_ms: u64,
    now_ms: u64,
) -> Result<(), PlaybackError> {
    let position_ms = session
        .duration_ms
        .map_or(position_ms, |duration_ms| position_ms.min(duration_ms));
    session.position_ms = position_ms;
    session.last_position_ms = position_ms;
    session.last_position_at_ms = now_ms;
    session.updated_at_ms = now_ms;
    playback_sessions::update_session(db, &mut session)?;
    Ok(())
}

/// Inserts a saved session of the track: stopped at the request's position, within the track,
/// under a play id of the server's.
fn insert_saved_session(
    db: &mut impl lyra_db::DbAccess,
    request: &SaveQueueRequest<'_>,
    track_db_id: DbId,
) -> Result<DbId, PlaybackError> {
    let duration_ms =
        lyra_db::tracks::get_by_id(db, track_db_id)?.and_then(|track| track.duration_ms);
    let position_ms = duration_ms.map_or(request.position_ms, |duration_ms| {
        request.position_ms.min(duration_ms)
    });
    let mut session = PlaybackSession {
        db_id: None,
        id: lyra_db::nanoid(),
        play_id: lyra_db::nanoid(),
        client_name: request.client_name.clone(),
        plugin_id: Some(request.plugin_id.to_string()),
        position_ms,
        duration_ms,
        played_ms: 0,
        last_position_ms: position_ms,
        last_position_at_ms: request.now_ms,
        latency_allowance_used_ms: 0,
        rate_permille: playback_sessions::NORMAL_RATE_PERMILLE,
        counts_as_listen: false,
        state: PlaybackState::Stopped,
        updated_at_ms: request.now_ms,
        live_until_ms: 0,
        created_at_ms: request.now_ms,
    };
    Ok(playback_sessions::insert_session(
        db,
        &mut session,
        track_db_id,
        request.user_db_id,
    )?)
}

/// Saves the queue to the playback the client saved to before, while its last save is still that
/// playback's current session, or else to a new playback. Saving records no listen and announces
/// nothing. The queue the playback has, at the same current index, only moves the position,
/// without checking its tracks again; any other queue must be visible to the caller and replaces
/// the stored one, whatever its revision.
pub(crate) fn save_queue(
    db: &mut DbAny,
    request: SaveQueueRequest<'_>,
) -> Result<SavedQueue, PlaybackError> {
    let client = request.client_name.clone().unwrap_or_default();
    db.transaction_mut(|t| {
        let Some((playback, session_id, session)) =
            own_saved_playback(t, request.user_db_id, request.plugin_id, &client)?
        else {
            lyra_db::playbacks::unclaim_saved(t, request.user_db_id, request.plugin_id, &client)?;
            return create_saved(t, &request, &client);
        };
        let playback_db_id = persisted_id(&playback)?;
        let stored = playback.queue().ok_or(PlaybackError::QueueUnavailable)?;
        if stored.track_ids == request.track_ids && stored.current_index == request.current_index {
            move_saved_session(t, session, request.position_ms, request.now_ms)?;
            lyra_db::playbacks::touch(t, playback_db_id, request.now_ms)?;
            return Ok(SavedQueue {
                playback_id: playback.id,
                changed_revision: None,
            });
        }
        let validated = validate_queue(
            t,
            request.principal,
            QueueSnapshot {
                track_ids: request.track_ids.clone(),
                current_index: request.current_index,
                repeat_mode: stored.repeat_mode,
                shuffle_enabled: stored.shuffle_enabled,
            },
        )?;
        let replaced = lyra_db::playbacks::replace_queue_in_transaction(
            t,
            playback_db_id,
            lyra_db::playbacks::QueueReplacement {
                expected_revision: stored.revision,
                track_ids: validated.snapshot.track_ids,
                current_index: validated.snapshot.current_index,
                repeat_mode: validated.snapshot.repeat_mode,
                shuffle_enabled: validated.snapshot.shuffle_enabled,
                updated_at_ms: request.now_ms,
                clear_current_session: false,
            },
        )
        .map_err(super::replace_error)?;
        if lyra_db::playback_sessions::get_track_id(t, session_id)?
            == Some(validated.current_track_db_id)
        {
            move_saved_session(t, session, request.position_ms, request.now_ms)?;
        } else {
            lyra_db::playback_sessions::delete(t, session_id)?;
            let session_id = insert_saved_session(t, &request, validated.current_track_db_id)?;
            lyra_db::playbacks::link_current_session(
                t,
                playback_db_id,
                session_id,
                request.now_ms,
            )?;
        }
        Ok(SavedQueue {
            changed_revision: replaced.queue().map(|queue| queue.revision),
            playback_id: replaced.id,
        })
    })
}

fn create_saved(
    t: &mut impl lyra_db::DbAccess,
    request: &SaveQueueRequest<'_>,
    client: &str,
) -> Result<SavedQueue, PlaybackError> {
    let validated = validate_queue(
        t,
        request.principal,
        QueueSnapshot {
            track_ids: request.track_ids.clone(),
            current_index: request.current_index,
            repeat_mode: RepeatMode::None,
            shuffle_enabled: false,
        },
    )?;
    if lyra_db::playbacks::count_for_user_up_to_limit(t, request.user_db_id)?
        >= lyra_db::playbacks::MAX_PLAYBACKS_PER_USER
    {
        return Err(PlaybackError::LimitReached);
    }
    let session_id = insert_saved_session(t, request, validated.current_track_db_id)?;
    let playback = Playback {
        db_id: None,
        id: lyra_db::nanoid(),
        source: lyra_db::playbacks::PlaybackSource::Queue(lyra_db::playbacks::Queue {
            revision: 1,
            track_ids: validated.snapshot.track_ids,
            current_index: validated.snapshot.current_index,
            repeat_mode: validated.snapshot.repeat_mode,
            shuffle_enabled: validated.snapshot.shuffle_enabled,
        }),
        created_at_ms: request.now_ms,
        updated_at_ms: request.now_ms,
    };
    let playback_db_id = lyra_db::playbacks::insert(t, &playback, request.user_db_id, session_id)?;
    lyra_db::playbacks::claim_saved(
        t,
        request.user_db_id,
        request.plugin_id,
        client,
        playback_db_id,
    )?;
    Ok(SavedQueue {
        playback_id: playback.id,
        changed_revision: None,
    })
}

/// Deletes the playback the client saved its queue to. One another client has since resumed only
/// stops being the client's.
pub(crate) fn remove_saved_queue(
    db: &mut DbAny,
    user_db_id: DbId,
    plugin_id: &str,
    client_name: Option<&str>,
) -> Result<Option<DeletedPlayback>, PlaybackError> {
    let client = client_name.unwrap_or("");
    db.transaction_mut(
        |t| match own_saved_playback(t, user_db_id, plugin_id, client)? {
            Some((playback, _, _)) => removal::delete_in_transaction(t, persisted_id(&playback)?),
            None => {
                lyra_db::playbacks::unclaim_saved(t, user_db_id, plugin_id, client)?;
                Ok(None)
            }
        },
    )
}

/// The user's most recently updated queue-backed playback whose queue has a track the user can
/// play, from any client.
pub(crate) fn latest_playable(
    db: &DbAny,
    user_db_id: DbId,
    now_ms: u64,
) -> Result<Option<LatestQueue>, PlaybackError> {
    let mut projections = lyra_db::playbacks::list_projections_for_user(db, user_db_id)?
        .into_iter()
        .filter(|projection| projection.queue_revision.is_some())
        .collect::<Vec<_>>();
    projections.sort_by(|left, right| {
        right
            .updated_at_ms
            .cmp(&left.updated_at_ms)
            .then_with(|| left.id.cmp(&right.id))
    });
    for projection in projections {
        let Some(playback) = lyra_db::playbacks::get_by_id(db, projection.db_id)? else {
            continue;
        };
        let Some(queue) = playable_queue(db, user_db_id, &playback, now_ms)? else {
            continue;
        };
        let session = match lyra_db::playbacks::get_current_session_id(db, projection.db_id)? {
            Some(session_id) => lyra_db::playback_sessions::get_by_id(db, session_id)?,
            None => None,
        };
        let (client_name, plugin_id) = session.map_or((None, None), |session| {
            (session.client_name, session.plugin_id)
        });
        return Ok(Some(LatestQueue {
            playback_id: playback.id,
            queue,
            client_name,
            plugin_id,
            updated_at_ms: playback.updated_at_ms,
        }));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use lyra_db::{
        fixtures::{
            insert_track,
            test_user,
        },
        test_db::new_test_db,
    };

    use super::*;
    use crate::services::playbacks::{
        ReportProgressRequest,
        report_progress,
    };

    struct Saver {
        db: DbAny,
        principal: Principal,
        user_db_id: DbId,
        tracks: [(DbId, String); 2],
    }

    fn saver() -> anyhow::Result<Saver> {
        let mut db = new_test_db()?;
        let user_db_id = lyra_db::users::create(&mut db, &test_user("saver")?)?;
        lyra_db::roles::ensure_builtin_roles(&mut db)?;
        lyra_db::roles::ensure_user_has_role(
            &mut db,
            user_db_id,
            lyra_db::roles::BUILTIN_ADMIN_ROLE,
        )?;
        let mut track = |title: &str| -> anyhow::Result<(DbId, String)> {
            let track_db_id = insert_track(&mut db, title)?;
            Ok((
                track_db_id,
                lyra_db::lookup::find_id_by_db_id(&db, track_db_id)?.unwrap(),
            ))
        };
        let tracks = [track("One")?, track("Two")?];
        let principal = Principal::for_user(
            &db,
            user_db_id,
            vec![lyra_db::Permission::Admin],
            HashSet::new(),
        );
        Ok(Saver {
            db,
            principal,
            user_db_id,
            tracks,
        })
    }

    fn save(
        saver: &mut Saver,
        client: &str,
        track_ids: &[&String],
        current_index: u64,
        position_ms: u64,
        now_ms: u64,
    ) -> Result<SavedQueue, PlaybackError> {
        save_queue(
            &mut saver.db,
            SaveQueueRequest {
                principal: &saver.principal,
                user_db_id: saver.user_db_id,
                plugin_id: "subsonic",
                client_name: Some(client.to_string()),
                track_ids: track_ids.iter().map(|id| id.to_string()).collect(),
                current_index,
                position_ms,
                now_ms,
            },
        )
    }

    fn current_session(db: &DbAny, playback_id: &str) -> anyhow::Result<(DbId, PlaybackSession)> {
        let playback_db_id = lyra_db::lookup::find_node_id_by_id(db, playback_id)?.unwrap();
        let session_id = lyra_db::playbacks::get_current_session_id(db, playback_db_id)?.unwrap();
        Ok((
            session_id,
            lyra_db::playback_sessions::get_by_id(db, session_id)?.unwrap(),
        ))
    }

    fn revision(db: &DbAny, playback_id: &str) -> anyhow::Result<u64> {
        let playback_db_id = lyra_db::lookup::find_node_id_by_id(db, playback_id)?.unwrap();
        Ok(lyra_db::playbacks::get_by_id(db, playback_db_id)?
            .unwrap()
            .queue()
            .unwrap()
            .revision)
    }

    #[test]
    fn a_save_is_a_stopped_uncounted_play_moved_in_place() -> anyhow::Result<()> {
        let mut saver = saver()?;
        let [(_, one), (two_db_id, two)] = saver.tracks.clone();

        let saved = save(&mut saver, "Phone", &[&one, &two], 1, 1_500, 10)?;
        let (session_id, session) = current_session(&saver.db, &saved.playback_id)?;
        assert_eq!(session.state, PlaybackState::Stopped);
        assert!(!session.counts_as_listen);
        assert_eq!(session.plugin_id.as_deref(), Some("subsonic"));
        assert_eq!(session.client_name.as_deref(), Some("Phone"));
        assert_eq!(session.position_ms, 1_500);
        assert!(lyra_db::lookup::find_node_id_by_id(&saver.db, &session.id)?.is_some());
        assert_eq!(
            lyra_db::playback_sessions::get_track_id(&saver.db, session_id)?,
            Some(two_db_id)
        );

        let again = save(&mut saver, "Phone", &[&one, &two], 1, 3_000, 20)?;
        assert_eq!(again.playback_id, saved.playback_id);
        assert_eq!(again.changed_revision, None);
        assert_eq!(revision(&saver.db, &saved.playback_id)?, 1);
        let (_, moved) = current_session(&saver.db, &saved.playback_id)?;
        assert_eq!(
            (moved.id.as_str(), moved.position_ms),
            (session.id.as_str(), 3_000)
        );

        let changed = save(&mut saver, "Phone", &[&two, &one], 1, 500, 30)?;
        assert_eq!(changed.playback_id, saved.playback_id);
        assert_eq!(changed.changed_revision, Some(2));
        let (_, replaced) = current_session(&saver.db, &saved.playback_id)?;
        assert_ne!(replaced.id, session.id);
        assert_eq!(replaced.position_ms, 500);
        assert!(
            lyra_db::lookup::find_node_id_by_id(&saver.db, &session.id)?.is_none(),
            "the superseded saved play is gone, so nothing announces its eviction"
        );

        let other = save(&mut saver, "Desktop", &[&one], 0, 0, 40)?;
        assert_ne!(
            other.playback_id, saved.playback_id,
            "each client saves apart"
        );
        assert!(
            lyra_db::listens::find_by_play_id(&saver.db, saver.user_db_id, &replaced.play_id)?
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn a_resumed_save_belongs_to_the_resuming_client() -> anyhow::Result<()> {
        let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
        crate::services::testing::init_default_test_state()?;
        let mut saver = saver()?;
        let [(one_db_id, one), (_, two)] = saver.tracks.clone();
        let saved = save(&mut saver, "Phone", &[&one, &two], 0, 1_500, 10)?;
        let playback_db_id = lyra_db::lookup::find_node_id_by_id(&saver.db, &saved.playback_id)?
            .expect("saved playback");
        let (_, saved_session) = current_session(&saver.db, &saved.playback_id)?;

        let progress = |play_id: &str| ReportProgressRequest {
            playback_db_id,
            play_id: play_id.to_string(),
            user_db_id: saver.user_db_id,
            client_name: None,
            queue_revision: 1,
            current_track_db_id: one_db_id,
            mutation: playback_sessions::PlaybackMutation {
                position_ms: Some(1_500),
                state: Some(PlaybackState::Playing),
                ..playback_sessions::PlaybackMutation::default()
            },
            now_ms: 20,
            require_full_queue_access: false,
            reporting_connection: None,
        };
        assert!(
            matches!(
                report_progress(&mut saver.db, progress(&saved_session.play_id)),
                Err(PlaybackError::Session(
                    playback_sessions::PlaybackServiceError::PlayConflict(_)
                ))
            ),
            "resuming a saved play starts a new one"
        );
        let resumed = report_progress(&mut saver.db, progress("resumed-play"))?;
        assert!(
            resumed.displaced_session.is_none(),
            "a saved play is not displaced"
        );
        assert!(lyra_db::lookup::find_node_id_by_id(&saver.db, &saved_session.id)?.is_none());
        assert_ne!(resumed.session.playback.play_id, saved_session.play_id);

        let next = save(&mut saver, "Phone", &[&one, &two], 0, 2_000, 30)?;
        assert_ne!(
            next.playback_id, saved.playback_id,
            "the resumed playback isn't the client's"
        );
        let (_, live) = current_session(&saver.db, &saved.playback_id)?;
        assert_eq!(live.play_id, "resumed-play");

        let removed =
            remove_saved_queue(&mut saver.db, saver.user_db_id, "subsonic", Some("Phone"))?
                .expect("the client's saved playback");
        assert_eq!(removed.playback_id, next.playback_id);
        assert!(
            remove_saved_queue(&mut saver.db, saver.user_db_id, "subsonic", Some("Phone"))?
                .is_none()
        );
        assert!(lyra_db::lookup::find_node_id_by_id(&saver.db, &saved.playback_id)?.is_some());
        Ok(())
    }
}
