// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashSet;

use agdb::{
    DbAny,
    DbId,
};
use nanoid::nanoid;

use crate::db::{
    self,
    PlaybackSession,
    PlaybackState,
};

use super::sessions::{
    self,
    PlaybackScopeKey,
    PlaybackSessionScope,
    clear_playback_session_scope,
    clear_session_bindings_for_playbacks,
    get_playback_session,
    resolve_current_playback,
    resolve_previous_playback,
    upsert_playback_session,
};
use super::{
    ACTIVE_SESSION_TTL_MS,
    ActiveEvent,
    ActivityPolicy,
    EvictedPlaybackRecord,
    OptionalPlaybackUpdateResult,
    POSITION_DELTA_GRACE_MS,
    PREVIOUS_PLAYBACK_GRACE_MS,
    PlaybackMutation,
    PlaybackRecord,
    PlaybackServiceError,
    PlaybackUpdateResult,
    ReportPlaybackRequest,
    ServiceResult,
    SessionPlaybackReportRequest,
    StartPlaybackRequest,
};

fn event_for_state(state: PlaybackState, active_event: ActiveEvent) -> String {
    if state.is_terminal() {
        state.to_string()
    } else {
        active_event.to_string()
    }
}

fn should_create_unbound_session_playback(
    active_event: ActiveEvent,
    state: PlaybackState,
    has_current_playback_for_other_track: bool,
) -> bool {
    has_current_playback_for_other_track
        || active_event == ActiveEvent::Started
        || state == PlaybackState::Playing
        || state == PlaybackState::Buffering
}

fn should_reopen_terminal_same_track_session(active_event: ActiveEvent) -> bool {
    active_event == ActiveEvent::Started
}

fn should_apply_previous_playback_event(active_event: ActiveEvent, state: PlaybackState) -> bool {
    active_event == ActiveEvent::Started || state.is_terminal()
}

#[derive(Clone, Copy, Debug, Default)]
struct SessionBindingState {
    current_playback_session_id: Option<DbId>,
    previous_playback_session_id: Option<DbId>,
    previous_expires_at_ms: Option<u64>,
}

fn clear_current_session_playback(state: &mut SessionBindingState) {
    state.current_playback_session_id = None;
}

fn clear_previous_session_playback(state: &mut SessionBindingState) {
    state.previous_playback_session_id = None;
    state.previous_expires_at_ms = None;
}

fn set_current_session_playback(state: &mut SessionBindingState, playback_session_id: DbId) {
    state.current_playback_session_id = Some(playback_session_id);
}

fn move_current_to_previous_session_playback(state: &mut SessionBindingState, now_ms: u64) {
    let Some(current_playback_session_id) = state.current_playback_session_id else {
        clear_previous_session_playback(state);
        return;
    };

    state.previous_playback_session_id = Some(current_playback_session_id);
    state.previous_expires_at_ms = Some(now_ms.saturating_add(PREVIOUS_PLAYBACK_GRACE_MS));
}

fn previous_session_playback_is_expired(state: &SessionBindingState, now_ms: u64) -> bool {
    state
        .previous_expires_at_ms
        .is_some_and(|expires_at_ms| expires_at_ms < now_ms)
}

fn persist_playback_session_state(
    scope: &PlaybackScopeKey<'_>,
    session: &PlaybackSessionScope,
    state: &SessionBindingState,
) {
    if state.current_playback_session_id.is_none() && state.previous_playback_session_id.is_none() {
        clear_playback_session_scope(scope);
        return;
    }

    let mut updated = session.clone();
    updated.current_playback_session_id = state.current_playback_session_id;
    updated.previous_playback_session_id = state.previous_playback_session_id;
    updated.previous_expires_at_ms = state.previous_expires_at_ms;
    updated.command_dispatched_at_ms = None;
    sessions::update_playback_session(scope, &updated);
}

fn update_session_bound_playback(
    db: &mut DbAny,
    user_db_id: DbId,
    mut bound: sessions::BoundPlayback,
    mutation: PlaybackMutation,
    now_ms: u64,
) -> ServiceResult<PlaybackRecord> {
    apply_playback_mutation(
        &mut bound.playback,
        &mutation,
        now_ms,
        ActivityPolicy::PlayingOnly,
    )?;
    bound.playback.db_id = Some(bound.playback_session_id);
    map_internal(db::playback_sessions::update(db, &bound.playback))?;

    Ok(PlaybackRecord {
        playback_session_id: bound.playback_session_id,
        track_db_id: bound.track_db_id,
        user_db_id,
        playback: bound.playback,
    })
}

fn map_internal<T>(result: anyhow::Result<T>) -> ServiceResult<T> {
    result.map_err(PlaybackServiceError::internal)
}

pub(crate) fn ensure_position_within_duration(
    position_ms: u64,
    duration_ms: Option<u64>,
) -> ServiceResult<()> {
    if let Some(duration_ms) = duration_ms
        && position_ms > duration_ms
    {
        return Err(PlaybackServiceError::bad_request(
            "position_ms cannot be greater than duration_ms",
        ));
    }
    Ok(())
}

pub(crate) fn playback_activity_ms(playback: &PlaybackSession) -> u64 {
    playback.activity_ms.unwrap_or(playback.position_ms)
}

fn maybe_record_listen(
    db: &mut DbAny,
    playback_record: &mut PlaybackRecord,
    now_ms: u64,
) -> ServiceResult<()> {
    if playback_record.playback.listen_recorded.unwrap_or(false) {
        return Ok(());
    }

    let activity_ms = playback_activity_ms(&playback_record.playback);
    if !db::playback_sessions::activity_meets_listen_threshold(
        activity_ms,
        playback_record.playback.duration_ms,
    ) {
        return Ok(());
    }

    let listen = db::Listen {
        db_id: None,
        id: nanoid!(),
        position_ms: playback_record.playback.position_ms,
        duration_ms: playback_record.playback.duration_ms,
        activity_ms,
        state: playback_record.playback.state,
        listened_at_ms: now_ms,
        created_at_ms: now_ms,
    };
    let mut playback_session = playback_record.playback.clone();
    playback_session.listen_recorded = Some(true);
    map_internal(db::listens::create_and_mark_recorded(
        db,
        &listen,
        playback_record.track_db_id,
        playback_record.user_db_id,
        &playback_session,
    ))?;
    playback_record.playback = playback_session;

    Ok(())
}

pub(crate) fn collect_unique_track_external_id_keys(
    external_ids: &[db::external_ids::ExternalId],
    unique_track_id_pairs: &HashSet<(String, String)>,
) -> HashSet<(String, String, String)> {
    let mut keys = HashSet::new();
    for external_id in external_ids {
        if !unique_track_id_pairs
            .contains(&(external_id.provider_id.clone(), external_id.id_type.clone()))
        {
            continue;
        }

        let id_value = external_id.id_value.trim();
        if id_value.is_empty() {
            continue;
        }

        keys.insert((
            external_id.provider_id.clone(),
            external_id.id_type.clone(),
            id_value.to_string(),
        ));
    }

    keys
}

pub(crate) fn resolve_merged_track_ids_for_play_count(
    db: &DbAny,
    track_db_id: DbId,
    unique_track_id_pairs: &HashSet<(String, String)>,
) -> anyhow::Result<Vec<DbId>> {
    if unique_track_id_pairs.is_empty() {
        return Ok(vec![track_db_id]);
    }

    if db::tracks::get_by_id(db, track_db_id)?.is_none() {
        return Ok(vec![track_db_id]);
    }

    let target_external_ids = db::external_ids::get_for_entity(db, track_db_id)?;
    let target_keys =
        collect_unique_track_external_id_keys(&target_external_ids, unique_track_id_pairs);
    if target_keys.is_empty() {
        return Ok(vec![track_db_id]);
    }

    let all_external_ids = db::external_ids::get_all_for_tracks(db)?;
    let matching_ext_ids: Vec<_> = all_external_ids
        .into_iter()
        .filter(|ext_id| {
            let id_value = ext_id.id_value.trim();
            !id_value.is_empty()
                && unique_track_id_pairs
                    .contains(&(ext_id.provider_id.clone(), ext_id.id_type.clone()))
                && target_keys.contains(&(
                    ext_id.provider_id.clone(),
                    ext_id.id_type.clone(),
                    id_value.to_string(),
                ))
        })
        .collect();

    let mut merged_track_ids = HashSet::new();
    for ext_id in matching_ext_ids {
        let Some(ext_db_id) = ext_id.db_id.map(DbId::from) else {
            continue;
        };
        if let Some(owner_db_id) = db::external_ids::get_owner_id(db, ext_db_id)?
            && db::tracks::get_by_id(db, owner_db_id)?.is_some()
        {
            merged_track_ids.insert(owner_db_id);
        }
    }

    if merged_track_ids.is_empty() {
        merged_track_ids.insert(track_db_id);
    }

    Ok(merged_track_ids.into_iter().collect())
}

pub(crate) fn cleanup_evicted_playbacks(
    db: &mut DbAny,
    now_ms: u64,
) -> ServiceResult<Vec<EvictedPlaybackRecord>> {
    sessions::cleanup_stale_scopes(now_ms, ACTIVE_SESSION_TTL_MS);
    let active_session_cutoff_ms = now_ms.saturating_sub(ACTIVE_SESSION_TTL_MS);
    let evicted_playbacks = map_internal(db::playback_sessions::evict_low_activity(
        db,
        now_ms,
        |playback_session_id| {
            sessions::has_active_scope_for_playback(playback_session_id, active_session_cutoff_ms)
        },
    ))?;
    clear_session_bindings_for_playbacks(&evicted_playbacks);

    Ok(evicted_playbacks
        .into_iter()
        .filter_map(|evicted| {
            evicted
                .playback
                .db_id
                .map(|playback_session_id| EvictedPlaybackRecord {
                    playback_session_id,
                    track_db_id: evicted.track_db_id,
                    user_db_id: evicted.user_db_id,
                    playback: evicted.playback,
                })
        })
        .collect())
}

pub(super) fn collect_playback_records(db: &DbAny) -> ServiceResult<Vec<PlaybackRecord>> {
    let playbacks = map_internal(db::playback_sessions::get(db))?;
    let mut records = Vec::new();

    for playback in playbacks {
        let Some(playback_session_id) = playback.db_id else {
            continue;
        };
        let Some(track_db_id) =
            map_internal(db::playback_sessions::get_track_id(db, playback_session_id))?
        else {
            continue;
        };
        let user_db_id = map_internal(db::playback_sessions::get_user_id(db, playback_session_id))?
            .unwrap_or(DbId(0));

        records.push(PlaybackRecord {
            playback_session_id,
            track_db_id,
            user_db_id,
            playback,
        });
    }

    Ok(records)
}

pub(crate) fn list_playbacks(db: &DbAny, user_db_id: DbId) -> ServiceResult<Vec<PlaybackRecord>> {
    Ok(collect_playback_records(db)?
        .into_iter()
        .filter(|playback| playback.user_db_id == user_db_id)
        .collect())
}

pub(super) fn apply_playback_mutation(
    playback: &mut PlaybackSession,
    mutation: &PlaybackMutation,
    now_ms: u64,
    activity_policy: ActivityPolicy,
) -> ServiceResult<()> {
    let previous_state = playback.state;
    let state = mutation.state.unwrap_or(previous_state);
    let position_ms = mutation.position_ms.unwrap_or(playback.position_ms);
    let duration_ms = mutation.duration_ms.or(playback.duration_ms);
    ensure_position_within_duration(position_ms, duration_ms)?;

    let mut activity_ms = playback.activity_ms.unwrap_or(playback.position_ms);
    let mut last_position_ms = playback.last_position_ms.unwrap_or(playback.position_ms);
    if mutation.position_ms.is_some() {
        let delta_ms = position_ms.saturating_sub(last_position_ms);
        let elapsed_since_last_update_ms = now_ms.saturating_sub(playback.updated_at_ms);
        let expected_progress_window_ms =
            elapsed_since_last_update_ms.saturating_add(POSITION_DELTA_GRACE_MS);
        if delta_ms <= expected_progress_window_ms
            && activity_policy.records_position_delta(previous_state, state)
        {
            activity_ms = activity_ms.saturating_add(delta_ms);
        }
        last_position_ms = position_ms;
    }

    playback.position_ms = position_ms;
    playback.duration_ms = duration_ms;
    playback.state = state;
    playback.activity_ms = Some(activity_ms);
    playback.last_position_ms = Some(last_position_ms);
    playback.updated_at_ms = now_ms;

    Ok(())
}

fn finalize_playback_update(
    db: &mut DbAny,
    mut playback: PlaybackRecord,
    active_event: ActiveEvent,
    now_ms: u64,
) -> ServiceResult<PlaybackUpdateResult> {
    maybe_record_listen(db, &mut playback, now_ms)?;
    let event = event_for_state(playback.playback.state, active_event);
    let evicted_playbacks = cleanup_evicted_playbacks(db, now_ms)?;

    Ok(PlaybackUpdateResult {
        playback,
        event,
        evicted_playbacks,
    })
}

fn finalize_optional_playback_update(
    db: &mut DbAny,
    mut playback: Option<PlaybackRecord>,
    active_event: ActiveEvent,
    now_ms: u64,
) -> ServiceResult<OptionalPlaybackUpdateResult> {
    if let Some(playback_record) = playback.as_mut() {
        maybe_record_listen(db, playback_record, now_ms)?;
    }
    let event = playback
        .as_ref()
        .map(|playback| event_for_state(playback.playback.state, active_event));
    let evicted_playbacks = cleanup_evicted_playbacks(db, now_ms)?;

    Ok(OptionalPlaybackUpdateResult {
        playback,
        event,
        evicted_playbacks,
    })
}

pub(crate) fn start_playback_with_cleanup(
    db: &mut DbAny,
    request: StartPlaybackRequest,
) -> ServiceResult<PlaybackUpdateResult> {
    let active_event = request.active_event;
    let now_ms = request.now_ms;
    let playback = start_playback(db, request)?;
    finalize_playback_update(db, playback, active_event, now_ms)
}

pub(crate) fn report_playback_with_cleanup(
    db: &mut DbAny,
    request: ReportPlaybackRequest,
) -> ServiceResult<PlaybackUpdateResult> {
    let active_event = request.active_event;
    let now_ms = request.now_ms;
    let playback = report_playback(db, request)?;
    finalize_playback_update(db, playback, active_event, now_ms)
}

pub(crate) fn report_playback_session_with_cleanup(
    db: &mut DbAny,
    request: SessionPlaybackReportRequest<'_>,
) -> ServiceResult<OptionalPlaybackUpdateResult> {
    let active_event = request.active_event;
    let now_ms = request.now_ms;
    let playback = report_playback_session(db, request)?;
    finalize_optional_playback_update(db, playback, active_event, now_ms)
}

pub(crate) fn start_playback(
    db: &mut DbAny,
    request: StartPlaybackRequest,
) -> ServiceResult<PlaybackRecord> {
    let StartPlaybackRequest {
        track_db_id,
        user_db_id,
        mutation,
        now_ms,
        active_event: _,
    } = request;
    if map_internal(db::tracks::get_by_id(db, track_db_id))?.is_none() {
        return Err(PlaybackServiceError::not_found(format!(
            "track not found: {}",
            track_db_id.0
        )));
    }
    if map_internal(db::users::get_by_id(db, user_db_id))?.is_none() {
        return Err(PlaybackServiceError::not_found(format!(
            "user not found: {}",
            user_db_id.0
        )));
    }

    let state = mutation.state.unwrap_or(PlaybackState::Playing);
    let position_ms = mutation.position_ms.unwrap_or(0);
    ensure_position_within_duration(position_ms, mutation.duration_ms)?;

    let playback = PlaybackSession {
        db_id: None,
        id: nanoid!(),
        position_ms,
        duration_ms: mutation.duration_ms,
        activity_ms: Some(0),
        last_position_ms: Some(position_ms),
        state,
        listen_recorded: None,
        updated_at_ms: now_ms,
        created_at_ms: now_ms,
    };

    let playback_session_id = map_internal(db::playback_sessions::create(
        db,
        &playback,
        track_db_id,
        user_db_id,
    ))?;

    Ok(PlaybackRecord {
        playback_session_id,
        track_db_id,
        user_db_id,
        playback: PlaybackSession {
            db_id: Some(playback_session_id),
            ..playback
        },
    })
}

pub(crate) fn report_playback(
    db: &mut DbAny,
    request: ReportPlaybackRequest,
) -> ServiceResult<PlaybackRecord> {
    let ReportPlaybackRequest {
        playback_session_id,
        user_db_id: expected_user_db_id,
        mutation,
        now_ms,
        activity_policy,
        active_event: _,
    } = request;
    let mut playback = map_internal(db::playback_sessions::get_by_id(db, playback_session_id))?
        .ok_or_else(|| {
            PlaybackServiceError::not_found(format!(
                "playback session not found: {}",
                playback_session_id.0
            ))
        })?;
    let playback_user_db_id =
        map_internal(db::playback_sessions::get_user_id(db, playback_session_id))?;
    if let Some(expected_user_db_id) = expected_user_db_id {
        let Some(owner_db_id) = playback_user_db_id else {
            return Err(PlaybackServiceError::not_found(format!(
                "playback session not found: {}",
                playback_session_id.0
            )));
        };
        if owner_db_id != expected_user_db_id {
            return Err(PlaybackServiceError::not_found(format!(
                "playback session not found: {}",
                playback_session_id.0
            )));
        }
    }

    apply_playback_mutation(&mut playback, &mutation, now_ms, activity_policy)?;
    playback.db_id = Some(playback_session_id);
    map_internal(db::playback_sessions::update(db, &playback))?;
    if playback.state.is_terminal() {
        sessions::clear_session_bindings_for_playback(playback_session_id);
    }

    let track_db_id = map_internal(db::playback_sessions::get_track_id(db, playback_session_id))?
        .ok_or_else(|| {
        PlaybackServiceError::not_found(format!(
            "playback session track not found: {}",
            playback_session_id.0
        ))
    })?;
    let user_db_id = playback_user_db_id.unwrap_or(DbId(0));

    Ok(PlaybackRecord {
        playback_session_id,
        track_db_id,
        user_db_id,
        playback,
    })
}

pub(crate) fn report_playback_session(
    db: &mut DbAny,
    request: SessionPlaybackReportRequest<'_>,
) -> ServiceResult<Option<PlaybackRecord>> {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum SessionTrackTarget {
        Current,
        Previous,
    }

    let SessionPlaybackReportRequest {
        plugin_id,
        user_db_id,
        session_key,
        track_db_id,
        mutation,
        now_ms,
        active_event,
        stale_ttl_ms,
    } = request;

    sessions::cleanup_stale_scopes(now_ms, stale_ttl_ms);
    if map_internal(db::users::get_by_id(db, user_db_id))?.is_none() {
        return Err(PlaybackServiceError::not_found(format!(
            "user not found: {}",
            user_db_id.0
        )));
    }

    let scope = PlaybackScopeKey {
        plugin_id,
        user_db_id,
        session_key,
    };

    let mut session = get_playback_session(&scope);
    if let Some(session) = session.as_mut() {
        session.updated_at_ms = now_ms;
    }

    let mut current_bound_playback = if let Some(session) = session.as_ref() {
        map_internal(resolve_current_playback(db, session))?
    } else {
        None
    };
    let mut previous_bound_playback = if let Some(session) = session.as_ref() {
        map_internal(resolve_previous_playback(db, session))?
    } else {
        None
    };

    let mut session_state = SessionBindingState {
        current_playback_session_id: current_bound_playback
            .as_ref()
            .map(|bound| bound.playback_session_id),
        previous_playback_session_id: previous_bound_playback
            .as_ref()
            .map(|bound| bound.playback_session_id),
        previous_expires_at_ms: session
            .as_ref()
            .and_then(|session| session.previous_expires_at_ms),
    };

    if previous_bound_playback.is_none() && session_state.previous_expires_at_ms.is_some() {
        clear_previous_session_playback(&mut session_state);
    }
    if previous_bound_playback.is_some()
        && previous_session_playback_is_expired(&session_state, now_ms)
    {
        previous_bound_playback = None;
        clear_previous_session_playback(&mut session_state);
    }

    let mut target = if current_bound_playback
        .as_ref()
        .is_some_and(|bound| bound.track_db_id == track_db_id)
    {
        Some(SessionTrackTarget::Current)
    } else if previous_bound_playback
        .as_ref()
        .is_some_and(|bound| bound.track_db_id == track_db_id)
    {
        Some(SessionTrackTarget::Previous)
    } else {
        None
    };

    let state = if let Some(state) = mutation.state {
        state
    } else if target == Some(SessionTrackTarget::Current) {
        current_bound_playback
            .as_ref()
            .map(|bound| bound.playback.state)
            .unwrap_or(PlaybackState::Playing)
    } else if target == Some(SessionTrackTarget::Previous) {
        previous_bound_playback
            .as_ref()
            .map(|bound| bound.playback.state)
            .unwrap_or(PlaybackState::Playing)
    } else {
        PlaybackState::Playing
    };

    if let Some(target_slot) = target {
        let bound_state = match target_slot {
            SessionTrackTarget::Current => current_bound_playback
                .as_ref()
                .map(|bound| bound.playback.state),
            SessionTrackTarget::Previous => previous_bound_playback
                .as_ref()
                .map(|bound| bound.playback.state),
        };

        if let Some(bound_state) = bound_state
            && bound_state.is_terminal()
        {
            if !should_reopen_terminal_same_track_session(active_event) {
                if let Some(session) = session.as_ref() {
                    persist_playback_session_state(&scope, session, &session_state);
                }
                return Ok(None);
            }
            target = None;
        }
    }

    let mut playback = None;
    match target {
        Some(SessionTrackTarget::Current) => {
            let Some(bound) = current_bound_playback.take() else {
                return Err(PlaybackServiceError::internal(anyhow::anyhow!(
                    "missing current playback binding"
                )));
            };

            let updated = update_session_bound_playback(
                db,
                user_db_id,
                bound,
                PlaybackMutation {
                    position_ms: mutation.position_ms,
                    duration_ms: mutation.duration_ms,
                    state: Some(state),
                },
                now_ms,
            )?;

            set_current_session_playback(&mut session_state, updated.playback_session_id);
            if updated.playback.state.is_terminal() {
                clear_current_session_playback(&mut session_state);
            }
            playback = Some(updated);
        }
        Some(SessionTrackTarget::Previous) => {
            if should_apply_previous_playback_event(active_event, state) {
                let mut promoted_to_current = false;
                let previous_binding = previous_bound_playback
                    .as_ref()
                    .map(|bound| bound.playback_session_id);

                if active_event == ActiveEvent::Started && !state.is_terminal() {
                    if session.is_none() {
                        session = Some(upsert_playback_session(&scope, now_ms));
                    }
                    if current_bound_playback.is_some() {
                        move_current_to_previous_session_playback(&mut session_state, now_ms);
                    } else {
                        clear_previous_session_playback(&mut session_state);
                    }

                    if let Some(previous_playback_session_id) = previous_binding {
                        set_current_session_playback(
                            &mut session_state,
                            previous_playback_session_id,
                        );
                        promoted_to_current = true;
                    }
                }

                let Some(bound) = previous_bound_playback.take() else {
                    return Err(PlaybackServiceError::internal(anyhow::anyhow!(
                        "missing previous playback binding"
                    )));
                };

                let updated = update_session_bound_playback(
                    db,
                    user_db_id,
                    bound,
                    PlaybackMutation {
                        position_ms: mutation.position_ms,
                        duration_ms: mutation.duration_ms,
                        state: Some(state),
                    },
                    now_ms,
                )?;

                if promoted_to_current {
                    set_current_session_playback(&mut session_state, updated.playback_session_id);
                    if updated.playback.state.is_terminal() {
                        clear_current_session_playback(&mut session_state);
                    }
                } else if updated.playback.state.is_terminal() {
                    clear_previous_session_playback(&mut session_state);
                }

                playback = Some(updated);
            }
        }
        None => {
            let has_current_playback_for_other_track = current_bound_playback.is_some();
            if !state.is_terminal()
                && should_create_unbound_session_playback(
                    active_event,
                    state,
                    has_current_playback_for_other_track,
                )
            {
                let created = start_playback(
                    db,
                    StartPlaybackRequest {
                        track_db_id,
                        user_db_id,
                        mutation: PlaybackMutation {
                            position_ms: mutation.position_ms,
                            duration_ms: mutation.duration_ms,
                            state: Some(state),
                        },
                        now_ms,
                        active_event,
                    },
                )?;

                if session.is_none() {
                    session = Some(upsert_playback_session(&scope, now_ms));
                }
                session.as_mut().unwrap().updated_at_ms = now_ms;
                if current_bound_playback.is_some() {
                    move_current_to_previous_session_playback(&mut session_state, now_ms);
                }
                set_current_session_playback(&mut session_state, created.playback_session_id);

                playback = Some(created);
            }
        }
    }

    if let Some(session) = session.as_ref() {
        persist_playback_session_state(&scope, session, &session_state);
    }

    Ok(playback)
}

pub(crate) fn clear_playback_session(plugin_id: &str, user_db_id: DbId, session_key: &str) {
    let scope = PlaybackScopeKey {
        plugin_id,
        user_db_id,
        session_key,
    };
    clear_playback_session_scope(&scope);
}

pub(crate) fn reset_scopes_for_test() {
    sessions::clear_all_scopes_for_test();
}
