// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Playback a plugin reports for a player session it tracks, keyed by the user, the plugin and the
//! plugin's session key. The playback keeps the player's current track session and, for a grace
//! period after a track change, the one it replaced, so late reports of that track still land.

use agdb::{
    DbAny,
    DbId,
};
use nanoid::nanoid;

use lyra_db::{
    PlaybackSession,
    PlaybackState,
    playbacks::PreviousSession,
};

use super::{
    PlaybackError,
    holds::{
        self,
        HeldSession,
    },
    reporting::{
        self,
        Transition,
    },
};
use crate::services::playback_sessions::{
    self,
    ActiveEvent,
    EvictedPlaybackRecord,
    PREVIOUS_PLAYBACK_GRACE_MS,
    PlaybackMutation,
    PlaybackRecord,
    PlaybackServiceError,
    ReportPlaybackRequest,
    StartPlaybackRequest,
};

#[derive(Clone, Debug)]
pub(crate) struct ReportedPlaybackRequest<'a> {
    pub(crate) plugin_id: &'a str,
    pub(crate) user_db_id: DbId,
    pub(crate) session_key: &'a str,
    pub(crate) track_db_id: DbId,
    /// The reporter's play id; without one, a play is told apart by its track and events.
    pub(crate) play_id: Option<&'a str>,
    pub(crate) client_name: Option<String>,
    pub(crate) mutation: PlaybackMutation,
    pub(crate) now_ms: u64,
    pub(crate) active_event: ActiveEvent,
}

pub(crate) enum ReportOutcome {
    Applied(Box<super::PlaybackUpdate>),
    /// The report changed no track session; cleanup may still have evicted stale ones.
    Ignored(Vec<EvictedPlaybackRecord>),
}

pub(crate) fn report_reported_playback(
    db: &mut DbAny,
    request: ReportedPlaybackRequest<'_>,
) -> Result<ReportOutcome, PlaybackError> {
    let now_ms = request.now_ms;
    let (plugin_id, user_db_id, session_key) =
        (request.plugin_id, request.user_db_id, request.session_key);
    Ok(match db.transaction_mut(|t| transition(t, request))? {
        Some(transition) => {
            let counted = holds::advance(plugin_id, session_key, || {
                Ok(lyra_db::users::get_by_id(db, user_db_id)?.map(|user| user.id))
            });
            if let Err(error) = counted {
                tracing::warn!(%error, "failed to count a report for the sessions sockets hold");
            }
            ReportOutcome::Applied(Box::new(reporting::finish(db, transition, None, now_ms)))
        }
        None => ReportOutcome::Ignored(reporting::cleanup_after_commit(db, now_ms)),
    })
}

/// Pauses what the sessions closed sockets held left playing, for each session that no open socket
/// holds and that took no report since its socket closed. Returns the sessions it paused.
fn pause_released_sessions(
    db: &mut DbAny,
    releases: Vec<holds::Release>,
    now_ms: u64,
) -> Result<(Vec<PlaybackRecord>, Vec<EvictedPlaybackRecord>), PlaybackError> {
    let unheld = releases
        .into_iter()
        .filter_map(holds::Release::settle)
        .collect::<Vec<_>>();
    let mut paused = Vec::new();
    for HeldSession {
        plugin_id,
        user_public_id,
        session_key,
    } in unheld
    {
        // A user deleted since holds nothing.
        let Some(user_db_id) = lyra_db::users::find_db_id_by_public_id(db, &user_public_id)? else {
            continue;
        };
        paused.extend(disconnect_reported_session(
            db,
            &plugin_id,
            user_db_id,
            &session_key,
            now_ms,
        )?);
    }
    Ok((paused, reporting::cleanup_after_commit(db, now_ms)))
}

/// Pauses what the sessions a closed plugin socket held left playing, as
/// [`pause_released_sessions`] does, and announces it.
pub(crate) async fn pause_after_socket_close(releases: Vec<holds::Release>) {
    if releases.is_empty() {
        return;
    }
    let result = match playback_sessions::now_ms() {
        Ok(now_ms) => {
            let mut db = crate::services::STATE.db.write_outside_catalog().await;
            pause_released_sessions(&mut db, releases, now_ms)
        }
        Err(error) => Err(error.into()),
    };
    match result {
        Ok((paused, evicted)) => {
            playback_sessions::dispatch_evicted_updates(evicted);
            for record in paused {
                let event = "progress".to_string();
                match &record.playback.plugin_id {
                    Some(plugin_id) => playback_sessions::dispatch_playback_update_for_caller(
                        format!("plugin:{plugin_id}"),
                        &record,
                        event,
                    ),
                    None => playback_sessions::dispatch_playback_update(&record, event),
                }
            }
        }
        Err(error) => {
            tracing::warn!(%error, "failed to pause playback a closed plugin socket held");
        }
    }
}

/// Pauses the playback the plugin reports for the user's player session, as a closed connection
/// of Lyra's own clients pauses its playback: the playing current session at `now_ms`, and the
/// session it replaced at the time it was replaced. Returns the sessions it paused; the playback
/// stays, so the player can resume it.
fn disconnect_reported_session(
    db: &mut DbAny,
    plugin_id: &str,
    user_db_id: DbId,
    session_key: &str,
    now_ms: u64,
) -> Result<Vec<PlaybackRecord>, PlaybackError> {
    let mut left = Vec::new();
    if let Some(root_db_id) =
        lyra_db::playbacks::get_reported(db, user_db_id, plugin_id, session_key, now_ms)?
            .and_then(|root| root.db_id)
    {
        let current_id = lyra_db::playbacks::get_current_session_id(db, root_db_id)?;
        if let Some((session, _)) = current_id
            .map(|id| reporting::stored_session(db, id))
            .transpose()?
            .flatten()
        {
            left.push((session, now_ms));
        }
        if let Some(previous) = lyra_db::playbacks::get_previous_session(db, root_db_id)?
            && Some(previous.session_db_id) != current_id
            && let Some((session, _)) = reporting::stored_session(db, previous.session_db_id)?
        {
            left.push((session, previous.demoted_at_ms));
        }
    }
    reporting::pause_left_sessions(db, user_db_id, left)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Target {
    Current,
    Previous,
}

/// A stored track session the playback holds.
struct Held {
    playback_session_id: DbId,
    track_db_id: DbId,
    session: PlaybackSession,
}

fn held(db: &impl lyra_db::DbAccess, playback_session_id: DbId) -> anyhow::Result<Option<Held>> {
    Ok(
        reporting::stored_session(db, playback_session_id)?.map(|(session, track_db_id)| Held {
            playback_session_id,
            track_db_id,
            session,
        }),
    )
}

fn demote(current: Option<DbId>, now_ms: u64) -> Option<PreviousSession> {
    current.map(|session_db_id| PreviousSession {
        session_db_id,
        demoted_at_ms: now_ms,
        expires_at_ms: now_ms.saturating_add(PREVIOUS_PLAYBACK_GRACE_MS),
    })
}

fn transition(
    db: &mut impl lyra_db::DbAccess,
    request: ReportedPlaybackRequest<'_>,
) -> Result<Option<Transition>, PlaybackError> {
    let ReportedPlaybackRequest {
        plugin_id,
        user_db_id,
        session_key,
        track_db_id,
        play_id,
        client_name,
        mutation,
        now_ms,
        active_event,
    } = request;

    reporting::expire_lapsed(db, user_db_id, now_ms)?;
    if lyra_db::users::get_by_id(db, user_db_id)?.is_none() {
        return Err(
            PlaybackServiceError::NotFound(format!("user not found: {}", user_db_id.0)).into(),
        );
    }

    let mut root =
        lyra_db::playbacks::get_reported(db, user_db_id, plugin_id, session_key, now_ms)?;
    let root_db_id = root
        .as_ref()
        .map(|root| root.db_id.expect("persisted playback id"));
    let (original_current_id, stored_previous) = match root_db_id {
        Some(root_db_id) => (
            lyra_db::playbacks::get_current_session_id(db, root_db_id)?,
            lyra_db::playbacks::get_previous_session(db, root_db_id)?,
        ),
        None => (None, None),
    };
    let mut current_held = original_current_id
        .map(|id| held(db, id))
        .transpose()?
        .flatten();
    let mut previous_held = stored_previous
        .map(|previous| held(db, previous.session_db_id))
        .transpose()?
        .flatten();
    let mut current = current_held.as_ref().map(|held| held.playback_session_id);
    let mut previous = stored_previous.filter(|_| previous_held.is_some());
    if previous.is_some_and(|previous| previous.expires_at_ms < now_ms) {
        previous_held = None;
        previous = None;
    }

    let is_reported_play = |held: &Held| {
        held.track_db_id == track_db_id
            && play_id.is_none_or(|play_id| held.session.play_id == play_id)
    };
    let mut target = if current_held.as_ref().is_some_and(is_reported_play) {
        Some(Target::Current)
    } else if previous_held.as_ref().is_some_and(is_reported_play) {
        Some(Target::Previous)
    } else {
        None
    };
    let target_held = |target: Target| match target {
        Target::Current => current_held.as_ref(),
        Target::Previous => previous_held.as_ref(),
    };

    let mut state = mutation.state.unwrap_or_else(|| {
        target
            .and_then(target_held)
            .map_or(PlaybackState::Playing, |held| held.session.state)
    });

    if let Some(target_slot) = target
        && let Some(held) = target_held(target_slot)
    {
        if held.session.state.is_terminal() {
            let current_resumed = target_slot == Target::Current
                && mutation.state.is_some_and(|state| !state.is_terminal());
            if !current_resumed && active_event != ActiveEvent::Started {
                if previous.is_none()
                    && stored_previous.is_some()
                    && let Some(root_db_id) = root_db_id
                {
                    lyra_db::playbacks::set_previous_session(db, root_db_id, None)?;
                }
                return Ok(None);
            }
            if active_event == ActiveEvent::Started && mutation.state.is_none() {
                state = PlaybackState::Playing;
            }
            // A named play resumes its own session; without a name, it reopens as a new play.
            if play_id.is_none() {
                target = None;
            }
        } else if active_event == ActiveEvent::Started
            && play_id.is_none()
            && lyra_db::listens::find_by_play_id(db, user_db_id, &held.session.play_id)?.is_some()
        {
            // Without a play id, a start after the play counted begins a new play.
            target = None;
        }
    }

    let report = |db: &mut _, held: Held| {
        playback_sessions::report_playback_in_transaction(
            db,
            ReportPlaybackRequest {
                user_db_id,
                mutation: PlaybackMutation {
                    position_ms: mutation.position_ms,
                    duration_ms: mutation.duration_ms,
                    state: Some(state),
                },
                now_ms,
                active_event,
            },
            held.session,
        )
    };
    let mut session = None;
    let mut current_observed = false;
    match target {
        Some(Target::Current) => {
            let held = current_held.take().ok_or_else(|| {
                anyhow::anyhow!("missing current playback session of the reported playback")
            })?;
            session = Some(report(db, held)?);
            current_observed = true;
        }
        Some(Target::Previous) if active_event == ActiveEvent::Started || state.is_terminal() => {
            let held = previous_held.take().ok_or_else(|| {
                anyhow::anyhow!("missing previous playback session of the reported playback")
            })?;
            let promoted = active_event == ActiveEvent::Started && !state.is_terminal();
            if promoted {
                previous = demote(current_held.as_ref().and(current), now_ms);
                current = Some(held.playback_session_id);
            }
            let updated = report(db, held)?;
            if promoted {
                current_observed = true;
            } else if updated.playback.playback.state.is_terminal() {
                previous = None;
            }
            session = Some(updated);
        }
        Some(Target::Previous) => {}
        None => {
            if !state.is_terminal() {
                let started = playback_sessions::start_playback_in_transaction(
                    db,
                    StartPlaybackRequest {
                        play_id: play_id.map_or_else(|| nanoid!(), str::to_string),
                        track_db_id,
                        user_db_id,
                        client_name,
                        plugin_id: Some(plugin_id.to_string()),
                        mutation: PlaybackMutation {
                            position_ms: mutation.position_ms,
                            duration_ms: mutation.duration_ms,
                            state: Some(state),
                        },
                        now_ms,
                        active_event,
                    },
                );
                let started = match started {
                    Ok(started) => started,
                    // A report for a play whose session was replaced cannot bring it back.
                    Err(PlaybackServiceError::PlayConflict(_)) => return Ok(None),
                    Err(error) => return Err(error.into()),
                };
                if current_held.is_some() {
                    previous = demote(current, now_ms);
                }
                current = Some(started.playback.playback_session_id);
                current_observed = true;
                session = Some(started);
            }
        }
    }

    if current_observed {
        let current_id = current.expect("an observed current session");
        if let Some(root) = root.as_mut() {
            let root_db_id = root.db_id.expect("persisted playback id");
            if original_current_id == Some(current_id) {
                lyra_db::playbacks::touch(db, root_db_id, now_ms)?;
            } else {
                lyra_db::playbacks::link_current_session(db, root_db_id, current_id, now_ms)?;
            }
            root.updated_at_ms = now_ms;
        } else {
            root = Some(
                lyra_db::playbacks::insert_reported(
                    db,
                    user_db_id,
                    plugin_id,
                    session_key,
                    current_id,
                    now_ms,
                )
                .map_err(|error| {
                    if error.is::<lyra_db::playbacks::ReportedPlaybackLimitReached>() {
                        PlaybackError::Session(PlaybackServiceError::BadRequest(error.to_string()))
                    } else {
                        PlaybackError::Internal(error)
                    }
                })?,
            );
        }
    }
    if let Some(root_db_id) = root.as_ref().and_then(|root| root.db_id)
        && stored_previous != previous
    {
        lyra_db::playbacks::set_previous_session(db, root_db_id, previous)?;
    }
    let Some(session) = session else {
        return Ok(None);
    };
    let playback = root.ok_or_else(|| anyhow::anyhow!("reported session has no playback"))?;
    Ok(Some(Transition {
        playback,
        session,
        displaced_session: None,
    }))
}

#[cfg(test)]
mod tests;
