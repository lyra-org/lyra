// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Playback a plugin reports for a player session it tracks, keyed by the user, the plugin and the
//! plugin's session key. The playback keeps the player's current track session and, for a grace
//! period after a track change, the one it replaced, so late reports of that track still land.
//! Only a start begins a new play of the track playing: any other report after its play stopped
//! resumes that play, so it counts at most one listen.

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
    reporting::{
        self,
        Transition,
    },
    socket_claims::{
        self,
        ClaimedSession,
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
    TransactionPlaybackUpdate,
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
    let (left, transition) = db.transaction_mut(|t| {
        let mut left = expire_lapsed(t, user_db_id, now_ms)?;
        let transition = transition(t, request, &mut left)?;
        Ok::<_, PlaybackError>((left, transition))
    })?;
    for update in &left {
        update.announce_recorded_listen();
    }
    Ok(match transition {
        Some(transition) => {
            let counted = socket_claims::count_report(plugin_id, session_key, || {
                Ok(lyra_db::users::get_by_id(db, user_db_id)?.map(|user| user.id))
            });
            if let Err(error) = counted {
                tracing::warn!(%error, "failed to count a report for the sessions sockets claim");
            }
            ReportOutcome::Applied(Box::new(reporting::finish(db, transition, None, now_ms)))
        }
        None => ReportOutcome::Ignored(reporting::cleanup_after_commit(db, now_ms)),
    })
}

/// Pauses what the sessions closed sockets claimed left playing, for each session that no open
/// socket claims and that took no report since its socket closed. Returns the sessions it paused.
fn pause_released_sessions(
    db: &mut DbAny,
    releases: Vec<socket_claims::ReleasedClaim>,
    now_ms: u64,
) -> Result<(Vec<PlaybackRecord>, Vec<EvictedPlaybackRecord>), PlaybackError> {
    let unclaimed = releases
        .into_iter()
        .filter_map(socket_claims::ReleasedClaim::settle)
        .collect::<Vec<_>>();
    let mut paused = Vec::new();
    for ClaimedSession {
        plugin_id,
        user_public_id,
        session_key,
    } in unclaimed
    {
        // A user deleted since claims nothing.
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

/// Pauses what the sessions a closed plugin socket claimed left playing, as
/// [`pause_released_sessions`] does, and announces it.
pub(crate) async fn pause_after_socket_close(releases: Vec<socket_claims::ReleasedClaim>) {
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
/// of Lyra's own clients pauses its playback: the playing current session at `now_ms`. The session
/// it replaced was paused when it was replaced. Returns the sessions it paused; the playback stays,
/// so the player can resume it.
fn disconnect_reported_session(
    db: &mut DbAny,
    plugin_id: &str,
    user_db_id: DbId,
    session_key: &str,
    now_ms: u64,
) -> Result<Vec<PlaybackRecord>, PlaybackError> {
    let Some(root_db_id) =
        lyra_db::playbacks::get_reported(db, user_db_id, plugin_id, session_key, now_ms)?
            .and_then(|root| root.db_id)
    else {
        return Ok(Vec::new());
    };
    let current = lyra_db::playbacks::get_current_session_id(db, root_db_id)?
        .map(|id| reporting::stored_session(db, id))
        .transpose()?
        .flatten();
    reporting::pause_left_sessions(
        db,
        user_db_id,
        current.map(|(session, _)| (session, now_ms)),
    )
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

/// Credits a play its player left playing without a stop report for the time from its last report
/// to `left_at_ms`, moving it at its rate up to at most the end of the track, as a report of
/// reaching that position would,
/// and leaves it in `then` if given. A play of unknown duration gets no credit; a buffering one
/// gets none either, but still leaves in `then`. Returns the update when it changed anything; only
/// its listen is announced.
fn leave_play(
    db: &mut impl lyra_db::DbAccess,
    user_db_id: DbId,
    session: PlaybackSession,
    left_at_ms: u64,
    then: Option<PlaybackState>,
) -> Result<Option<TransactionPlaybackUpdate>, PlaybackError> {
    let credit_ms = match session.state {
        // At most the elapsed time, so no latency allowance is added on top.
        PlaybackState::Playing => session.duration_ms.map_or(0, |duration_ms| {
            playback_sessions::advanced_in(
                left_at_ms.saturating_sub(session.last_position_at_ms),
                session.rate_permille,
            )
            .min(duration_ms.saturating_sub(session.last_position_ms))
        }),
        PlaybackState::Buffering => 0,
        PlaybackState::Paused | PlaybackState::Stopped | PlaybackState::Completed => {
            return Ok(None);
        }
    };
    let position_ms = (credit_ms > 0).then(|| session.last_position_ms + credit_ms);
    if position_ms.is_none() && then.is_none() {
        return Ok(None);
    }
    let now_ms = left_at_ms.max(session.updated_at_ms);
    Ok(Some(playback_sessions::report_playback_in_transaction(
        db,
        ReportPlaybackRequest {
            user_db_id,
            mutation: PlaybackMutation {
                position_ms,
                state: then,
                ..PlaybackMutation::default()
            },
            now_ms,
            active_event: ActiveEvent::Progress,
        },
        session,
    )?))
}

/// Credits the track session a new play replaced for what it played since its last report, and
/// pauses it there, as a track switch of Lyra's own clients pauses the play it replaces.
fn leave_replaced(
    db: &mut impl lyra_db::DbAccess,
    user_db_id: DbId,
    replaced: Option<Held>,
    now_ms: u64,
    left: &mut Vec<TransactionPlaybackUpdate>,
) -> Result<(), PlaybackError> {
    if let Some(replaced) = replaced {
        left.extend(leave_play(
            db,
            user_db_id,
            replaced.session,
            now_ms,
            Some(PlaybackState::Paused),
        )?);
    }
    Ok(())
}

/// Opens a report's transaction: the user's lapsed reported playbacks go first, so they hold no
/// capacity against the limits. A current play each left playing is credited up to when it
/// lapsed; returns those updates, to announce after commit.
pub(super) fn expire_lapsed(
    db: &mut impl lyra_db::DbAccess,
    user_db_id: DbId,
    now_ms: u64,
) -> Result<Vec<TransactionPlaybackUpdate>, PlaybackError> {
    let expired = lyra_db::playbacks::expire_reported_for_user(db, user_db_id, now_ms)?;
    credit_lapsed(db, expired)
}

fn credit_lapsed(
    db: &mut impl lyra_db::DbAccess,
    expired: Vec<lyra_db::playbacks::ExpiredReported>,
) -> Result<Vec<TransactionPlaybackUpdate>, PlaybackError> {
    let mut credited = Vec::new();
    for expired in expired {
        let Some((session, _)) = expired
            .current_session_db_id
            .map(|id| reporting::stored_session(db, id))
            .transpose()?
            .flatten()
        else {
            continue;
        };
        let lapsed_at_ms = expired
            .updated_at_ms
            .saturating_add(lyra_db::playbacks::REPORTED_PLAYBACK_TTL_MS);
        credited.extend(leave_play(
            db,
            expired.user_db_id,
            session,
            lapsed_at_ms,
            None,
        )?);
    }
    Ok(credited)
}

/// Expires every user's lapsed reported playbacks, crediting what each left playing as
/// [`expire_lapsed`] does, and announces the listens that recorded.
pub(crate) fn expire_all_lapsed(db: &mut DbAny, now_ms: u64) -> Result<(), PlaybackError> {
    let credited = db.transaction_mut(|t| {
        let expired = lyra_db::playbacks::expire_reported(t, now_ms)?;
        credit_lapsed(t, expired)
    })?;
    for update in &credited {
        update.announce_recorded_listen();
    }
    Ok(())
}

/// Applies the report. Plays it replaced are credited and paused into `left`, for their listens
/// to be announced after commit.
fn transition(
    db: &mut impl lyra_db::DbAccess,
    request: ReportedPlaybackRequest<'_>,
    left: &mut Vec<TransactionPlaybackUpdate>,
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
            // Only a start without a play id begins a new play; any other report, or a named
            // start, resumes the stopped play.
            if active_event == ActiveEvent::Started && play_id.is_none() {
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
    // A replaced play was paused at its replacement; its start plays it again.
    if target == Some(Target::Previous)
        && active_event == ActiveEvent::Started
        && mutation.state.is_none()
    {
        state = PlaybackState::Playing;
    }

    let report = |db: &mut _, held: Held| {
        playback_sessions::report_playback_in_transaction(
            db,
            ReportPlaybackRequest {
                user_db_id,
                mutation: PlaybackMutation {
                    state: Some(state),
                    ..mutation.clone()
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
                leave_replaced(db, user_db_id, current_held.take(), now_ms, left)?;
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
                            state: Some(state),
                            ..mutation.clone()
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
                    leave_replaced(db, user_db_id, current_held.take(), now_ms, left)?;
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
