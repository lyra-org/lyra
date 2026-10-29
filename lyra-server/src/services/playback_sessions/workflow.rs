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

use lyra_db::{
    PlaybackSession,
    PlaybackState,
};

use super::sessions::{
    self,
    PlaybackScopeKey,
    PlaybackSessionScope,
    clear_playback_session_scope,
    clear_session_bindings_for_playbacks,
    get_playback_sessions_for_user_session,
    resolve_current_playback,
    resolve_previous_playback,
};
use super::{
    ACTIVE_SESSION_TTL_MS,
    ActiveEvent,
    EvictedPlaybackRecord,
    OptionalPlaybackUpdateResult,
    PLAY_LATENCY_ALLOWANCE_MS,
    PREVIOUS_PLAYBACK_GRACE_MS,
    PlaybackMutation,
    PlaybackRecord,
    PlaybackServiceError,
    ReportPlaybackRequest,
    ServiceResult,
    SessionPlaybackReportRequest,
    StartPlaybackRequest,
    TransactionPlaybackUpdate,
};

fn event_for_state(state: PlaybackState, active_event: ActiveEvent) -> String {
    if state.is_terminal() {
        state.to_string()
    } else {
        active_event.to_string()
    }
}

fn resolve_library_public_id_for_track(
    db: &impl lyra_db::DbAccess,
    track_db_id: DbId,
) -> ServiceResult<Option<String>> {
    let libraries = map_internal(lyra_db::libraries::get_for_entity(db, track_db_id))?;
    debug_assert!(
        libraries.len() <= 1,
        "track {} maps to {} libraries; expected at most 1",
        track_db_id.0,
        libraries.len(),
    );
    Ok(libraries.into_iter().next().map(|library| library.id))
}

fn resolve_kind_validated_public_id(
    db: &impl lyra_db::DbAccess,
    collection_alias: &str,
    db_id: DbId,
) -> ServiceResult<Option<String>> {
    if !map_internal(lyra_db::lookup::collection_contains_id(
        db,
        collection_alias,
        db_id,
    ))? {
        return Ok(None);
    }
    map_internal(lyra_db::lookup::find_id_by_db_id(db, db_id))
}

fn build_playback_record(
    db: &impl lyra_db::DbAccess,
    playback_session_id: DbId,
    track_db_id: DbId,
    user_db_id: DbId,
    playback: PlaybackSession,
) -> ServiceResult<Option<PlaybackRecord>> {
    let Some(track_public_id) = resolve_kind_validated_public_id(db, "tracks", track_db_id)? else {
        return Ok(None);
    };
    let Some(user_public_id) = resolve_kind_validated_public_id(db, "users", user_db_id)? else {
        return Ok(None);
    };
    let library_public_id = resolve_library_public_id_for_track(db, track_db_id)?;
    let playback_session_public_id = playback.id.clone();
    Ok(Some(PlaybackRecord {
        playback_session_id,
        playback_session_public_id,
        track_db_id,
        track_public_id,
        user_db_id,
        user_public_id,
        library_public_id,
        playback,
    }))
}

fn build_evicted_playback_record(
    db: &impl lyra_db::DbAccess,
    playback_session_id: DbId,
    track_db_id: DbId,
    user_db_id: DbId,
    playback: PlaybackSession,
) -> ServiceResult<Option<EvictedPlaybackRecord>> {
    let Some(track_public_id) = resolve_kind_validated_public_id(db, "tracks", track_db_id)? else {
        return Ok(None);
    };
    let Some(user_public_id) = resolve_kind_validated_public_id(db, "users", user_db_id)? else {
        return Ok(None);
    };
    let library_public_id = resolve_library_public_id_for_track(db, track_db_id)?;
    let playback_session_public_id = playback.id.clone();
    Ok(Some(EvictedPlaybackRecord {
        playback_session_id,
        playback_session_public_id,
        track_db_id,
        track_public_id,
        user_db_id,
        user_public_id,
        library_public_id,
        playback,
    }))
}

fn require_playback_record(
    db: &impl lyra_db::DbAccess,
    playback_session_id: DbId,
    track_db_id: DbId,
    user_db_id: DbId,
    playback: PlaybackSession,
) -> ServiceResult<PlaybackRecord> {
    build_playback_record(db, playback_session_id, track_db_id, user_db_id, playback)?.ok_or_else(
        || {
            PlaybackServiceError::internal(anyhow::anyhow!(
                "missing public id for playback {} (track {}, user {})",
                playback_session_id.0,
                track_db_id.0,
                user_db_id.0,
            ))
        },
    )
}

fn should_reopen_terminal_same_track_session(active_event: ActiveEvent) -> bool {
    active_event == ActiveEvent::Started
}

fn should_apply_previous_playback_event(active_event: ActiveEvent, state: PlaybackState) -> bool {
    active_event == ActiveEvent::Started || state.is_terminal()
}

#[derive(Clone, Debug, Default)]
struct SessionBindingState {
    current_playback_session_id: Option<DbId>,
    current_playback_session_public_id: Option<String>,
    previous_playback_session_id: Option<DbId>,
    previous_playback_session_public_id: Option<String>,
    previous_demoted_at_ms: Option<u64>,
    previous_expires_at_ms: Option<u64>,
}

fn clear_previous_session_playback(state: &mut SessionBindingState) {
    state.previous_playback_session_id = None;
    state.previous_playback_session_public_id = None;
    state.previous_demoted_at_ms = None;
    state.previous_expires_at_ms = None;
}

fn set_current_session_playback(
    state: &mut SessionBindingState,
    playback_session_id: DbId,
    playback_session_public_id: String,
) {
    state.current_playback_session_id = Some(playback_session_id);
    state.current_playback_session_public_id = Some(playback_session_public_id);
}

fn move_current_to_previous_session_playback(state: &mut SessionBindingState, now_ms: u64) {
    let Some(current_playback_session_id) = state.current_playback_session_id else {
        clear_previous_session_playback(state);
        return;
    };

    state.previous_playback_session_id = Some(current_playback_session_id);
    state.previous_playback_session_public_id = state.current_playback_session_public_id.clone();
    state.previous_demoted_at_ms = Some(now_ms);
    state.previous_expires_at_ms = Some(now_ms.saturating_add(PREVIOUS_PLAYBACK_GRACE_MS));
}

fn previous_session_playback_is_expired(state: &SessionBindingState, now_ms: u64) -> bool {
    state
        .previous_expires_at_ms
        .is_some_and(|expires_at_ms| expires_at_ms < now_ms)
}

fn updated_playback_session_state(
    session: &PlaybackSessionScope,
    state: &SessionBindingState,
) -> Option<PlaybackSessionScope> {
    if state.current_playback_session_id.is_none() && state.previous_playback_session_id.is_none() {
        return None;
    }

    let mut updated = session.clone();
    updated.current_playback_session_id = state.current_playback_session_id;
    updated.current_playback_session_public_id = state.current_playback_session_public_id.clone();
    updated.previous_playback_session_id = state.previous_playback_session_id;
    updated.previous_playback_session_public_id = state.previous_playback_session_public_id.clone();
    updated.previous_demoted_at_ms = state.previous_demoted_at_ms;
    updated.previous_expires_at_ms = state.previous_expires_at_ms;
    updated.command_dispatched_at_ms = None;
    updated.current_binding_epoch = updated.current_binding_epoch.wrapping_add(1);
    Some(updated)
}

fn update_session_bound_playback(
    db: &mut impl lyra_db::DbAccess,
    user_db_id: DbId,
    mut bound: sessions::BoundPlayback,
    mutation: PlaybackMutation,
    now_ms: u64,
) -> ServiceResult<(PlaybackRecord, Option<lyra_db::Listen>)> {
    apply_playback_mutation(&mut bound.playback, &mutation, now_ms)?;
    bound.playback.db_id = Some(bound.playback_session_id);
    map_internal(lyra_db::playback_sessions::update(db, &bound.playback))?;

    let playback = require_playback_record(
        db,
        bound.playback_session_id,
        bound.track_db_id,
        user_db_id,
        bound.playback,
    )?;
    let recorded_listen = record_listen(db, &playback, now_ms)?;
    Ok((playback, recorded_listen))
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

/// Records the session's play as a listen once it qualifies, returning the new listen for the
/// transaction's owner to announce after commit.
fn record_listen(
    db: &mut impl lyra_db::DbAccess,
    playback_record: &PlaybackRecord,
    now_ms: u64,
) -> ServiceResult<Option<lyra_db::Listen>> {
    let playback = &playback_record.playback;
    let outcome = map_internal(crate::services::listens::record(
        db,
        playback_record.user_db_id,
        crate::services::listens::Play {
            play_id: &playback.play_id,
            track_db_id: playback_record.track_db_id,
            started_at_ms: playback.created_at_ms,
            played_ms: playback.played_ms,
        },
        now_ms,
    ))?;
    match outcome {
        crate::services::listens::RecordOutcome::Recorded(listen) => return Ok(Some(listen)),
        crate::services::listens::RecordOutcome::InvalidTime => {
            tracing::warn!(
                playback_session_id = playback_record.playback_session_id.0,
                started_at_ms = playback_record.playback.created_at_ms,
                now_ms,
                "live play not recorded: its times are implausible; check the server clock",
            );
        }
        _ => {}
    }
    Ok(None)
}

pub(crate) fn collect_unique_track_external_id_keys(
    external_ids: &[lyra_db::external_ids::ExternalId],
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

    if lyra_db::tracks::get_by_id(db, track_db_id)?.is_none() {
        return Ok(vec![track_db_id]);
    }

    let target_external_ids = lyra_db::external_ids::get_for_entity(db, track_db_id)?;
    let target_keys =
        collect_unique_track_external_id_keys(&target_external_ids, unique_track_id_pairs);
    if target_keys.is_empty() {
        return Ok(vec![track_db_id]);
    }

    let all_external_ids = lyra_db::external_ids::get_all_for_tracks(db)?;
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
        if let Some(owner_db_id) = lyra_db::external_ids::get_owner_id(db, ext_db_id)?
            && lyra_db::tracks::get_by_id(db, owner_db_id)?.is_some()
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
    let evicted_playbacks = map_internal(lyra_db::playback_sessions::evict_stale(
        db,
        now_ms,
        |playback_session_id| {
            sessions::has_active_scope_for_playback(playback_session_id, active_session_cutoff_ms)
        },
    ))?;
    clear_session_bindings_for_playbacks(&evicted_playbacks);

    let mut records = Vec::new();
    for evicted in evicted_playbacks {
        let Some(playback_session_id) = evicted.playback.db_id else {
            continue;
        };
        let track_db_id = evicted.track_db_id;
        let user_db_id = evicted.user_db_id;
        let Some(record) = build_evicted_playback_record(
            db,
            playback_session_id,
            track_db_id,
            user_db_id,
            evicted.playback,
        )?
        else {
            tracing::debug!(
                playback_session_id = playback_session_id.0,
                track_db_id = track_db_id.0,
                user_db_id = user_db_id.0,
                "skipping evicted playback with no resolvable public ids",
            );
            continue;
        };
        records.push(record);
    }
    Ok(records)
}

/// [`cleanup_evicted_playbacks`] after a mutation already committed: a failure is logged and only
/// delays eviction to the next cleanup, rather than failing the committed mutation.
pub(crate) fn cleanup_after_commit(db: &mut DbAny, now_ms: u64) -> Vec<EvictedPlaybackRecord> {
    cleanup_evicted_playbacks(db, now_ms).unwrap_or_else(|error| {
        tracing::error!(%error, "playback cleanup failed after durable mutation committed");
        Vec::new()
    })
}

pub(super) fn collect_playback_records(db: &DbAny) -> ServiceResult<Vec<PlaybackRecord>> {
    let playbacks = map_internal(lyra_db::playback_sessions::get(db))?;
    let mut records = Vec::new();

    for playback in playbacks {
        let Some(playback_session_id) = playback.db_id else {
            continue;
        };
        let Some(track_db_id) = map_internal(lyra_db::playback_sessions::get_track_id(
            db,
            playback_session_id,
        ))?
        else {
            continue;
        };
        let user_db_id = map_internal(lyra_db::playback_sessions::get_user_id(
            db,
            playback_session_id,
        ))?
        .unwrap_or(DbId(0));

        let Some(record) =
            build_playback_record(db, playback_session_id, track_db_id, user_db_id, playback)?
        else {
            tracing::debug!(
                playback_session_id = playback_session_id.0,
                track_db_id = track_db_id.0,
                user_db_id = user_db_id.0,
                "skipping playback with no resolvable public ids",
            );
            continue;
        };
        records.push(record);
    }

    Ok(records)
}

pub(crate) fn list_playbacks(db: &DbAny, user_db_id: DbId) -> ServiceResult<Vec<PlaybackRecord>> {
    Ok(collect_playback_records(db)?
        .into_iter()
        .filter(|playback| playback.user_db_id == user_db_id)
        .collect())
}

/// Applies a report to the session, crediting playing time only for position advanced while the
/// previous report was playing, and never more than the server's elapsed time plus what is left
/// of the play's latency allowance.
pub(crate) fn apply_playback_mutation(
    playback: &mut PlaybackSession,
    mutation: &PlaybackMutation,
    now_ms: u64,
) -> ServiceResult<()> {
    let state = mutation.state.unwrap_or(playback.state);
    let position_ms = mutation.position_ms.unwrap_or(playback.position_ms);
    let duration_ms = mutation.duration_ms.or(playback.duration_ms);
    ensure_position_within_duration(position_ms, duration_ms)?;

    if playback.state != PlaybackState::Playing {
        // Time spent not playing never counts, however the next report reaches a position.
        playback.last_position_at_ms = now_ms;
    }
    if mutation.position_ms.is_some() {
        if playback.state == PlaybackState::Playing {
            let advanced_ms = position_ms.saturating_sub(playback.last_position_ms);
            let elapsed_ms = now_ms.saturating_sub(playback.last_position_at_ms);
            let remaining_allowance_ms =
                PLAY_LATENCY_ALLOWANCE_MS.saturating_sub(playback.latency_allowance_used_ms);
            let credited_ms = advanced_ms.min(elapsed_ms.saturating_add(remaining_allowance_ms));
            playback.latency_allowance_used_ms = playback
                .latency_allowance_used_ms
                .saturating_add(credited_ms.saturating_sub(elapsed_ms));
            playback.played_ms = playback.played_ms.saturating_add(credited_ms);
        }
        playback.last_position_ms = position_ms;
        playback.last_position_at_ms = now_ms;
    }

    playback.position_ms = position_ms;
    playback.duration_ms = duration_ms;
    playback.state = state;
    playback.updated_at_ms = now_ms;

    Ok(())
}

fn finalize_optional_playback_update(
    db: &mut DbAny,
    playback: Option<PlaybackRecord>,
    active_event: ActiveEvent,
    now_ms: u64,
) -> ServiceResult<OptionalPlaybackUpdateResult> {
    let event = playback
        .as_ref()
        .map(|playback| event_for_state(playback.playback.state, active_event));
    let evicted_playbacks = cleanup_after_commit(db, now_ms);

    Ok(OptionalPlaybackUpdateResult {
        playback,
        event,
        evicted_playbacks,
    })
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

fn pause_bound_playback_on_disconnect(
    db: &mut DbAny,
    user_db_id: DbId,
    bound: sessions::BoundPlayback,
    pause_at_ms: u64,
) -> ServiceResult<Option<PlaybackRecord>> {
    if bound.playback.state != PlaybackState::Playing {
        return Ok(None);
    }

    let pause_at_ms = pause_at_ms.max(bound.playback.updated_at_ms); // Keep updated_at_ms monotonic.
    let (playback, recorded_listen) = db.transaction_mut(|t| {
        update_session_bound_playback(
            t,
            user_db_id,
            bound,
            PlaybackMutation {
                state: Some(PlaybackState::Paused),
                ..PlaybackMutation::default()
            },
            pause_at_ms,
        )
    })?;
    crate::services::listens::dispatch_recorded(&playback.user_public_id, recorded_listen);
    Ok(Some(playback))
}

/// Pauses the user's playing scopes for a disconnected session. Does nothing once the user is
/// gone; deleting a user clears its scopes.
pub(crate) fn pause_playing_scopes_on_disconnect(
    db: &mut DbAny,
    user_public_id: &str,
    session_key: &str,
    now_ms: u64,
) -> ServiceResult<(Vec<PlaybackRecord>, Vec<EvictedPlaybackRecord>)> {
    let Some(user_db_id) =
        map_internal(lyra_db::users::find_db_id_by_public_id(db, user_public_id))?
    else {
        return Ok((Vec::new(), Vec::new()));
    };
    let scoped_sessions = get_playback_sessions_for_user_session(user_public_id, session_key);
    let mut playbacks = Vec::new();

    for (scope_key, session) in scoped_sessions {
        let scope = scope_key.as_borrowed();

        let current_bound = map_internal(resolve_current_playback(db, &session))?;
        let current_playback_session_id = current_bound
            .as_ref()
            .map(|bound| bound.playback_session_id);
        if let Some(bound) = current_bound
            && let Some(playback) =
                pause_bound_playback_on_disconnect(db, user_db_id, bound, now_ms)?
        {
            playbacks.push(playback);
        }

        let previous_bound = map_internal(resolve_previous_playback(db, &session))?;
        if let Some(bound) = previous_bound
            && Some(bound.playback_session_id) != current_playback_session_id
        {
            let pause_at_ms = session.previous_demoted_at_ms.unwrap_or(now_ms);
            if let Some(playback) =
                pause_bound_playback_on_disconnect(db, user_db_id, bound, pause_at_ms)?
            {
                playbacks.push(playback);
            }
        }
        clear_playback_session_scope(&scope);
    }

    let evicted_playbacks = cleanup_after_commit(db, now_ms);

    Ok((playbacks, evicted_playbacks))
}

pub(crate) fn start_playback_in_transaction(
    db: &mut impl lyra_db::DbAccess,
    request: StartPlaybackRequest,
) -> ServiceResult<TransactionPlaybackUpdate> {
    let StartPlaybackRequest {
        play_id,
        track_db_id,
        user_db_id,
        client_name,
        mutation,
        now_ms,
        active_event,
    } = request;
    if map_internal(lyra_db::tracks::get_by_id(db, track_db_id))?.is_none() {
        return Err(PlaybackServiceError::not_found(format!(
            "track not found: {}",
            track_db_id.0
        )));
    }
    if map_internal(lyra_db::users::get_by_id(db, user_db_id))?.is_none() {
        return Err(PlaybackServiceError::not_found(format!(
            "user not found: {}",
            user_db_id.0
        )));
    }

    if map_internal(lyra_db::playback_sessions::find_by_play_id(
        db, user_db_id, &play_id,
    ))?
    .is_some()
    {
        return Err(PlaybackServiceError::PlayConflict(play_id));
    }
    let playback = new_playback_session(play_id, client_name, mutation, now_ms)?;

    let playback_session_id = map_internal(lyra_db::playback_sessions::insert(
        db,
        &playback,
        track_db_id,
        user_db_id,
    ))?;

    let playback = require_playback_record(
        db,
        playback_session_id,
        track_db_id,
        user_db_id,
        PlaybackSession {
            db_id: Some(playback_session_id),
            ..playback
        },
    )?;
    let recorded_listen = record_listen(db, &playback, now_ms)?;
    Ok(TransactionPlaybackUpdate {
        event: event_for_state(playback.playback.state, active_event),
        playback,
        recorded_listen,
    })
}

fn new_playback_session(
    play_id: String,
    client_name: Option<String>,
    mutation: PlaybackMutation,
    now_ms: u64,
) -> ServiceResult<PlaybackSession> {
    let state = mutation.state.unwrap_or(PlaybackState::Playing);
    let position_ms = mutation.position_ms.unwrap_or(0);
    ensure_position_within_duration(position_ms, mutation.duration_ms)?;

    Ok(PlaybackSession {
        db_id: None,
        id: nanoid!(),
        play_id,
        client_name,
        position_ms,
        duration_ms: mutation.duration_ms,
        played_ms: 0,
        last_position_ms: position_ms,
        state,
        latency_allowance_used_ms: 0,
        last_position_at_ms: now_ms,
        updated_at_ms: now_ms,
        created_at_ms: now_ms,
    })
}

#[cfg(test)]
pub(crate) fn report_playback(
    db: &mut DbAny,
    playback_session_id: DbId,
    request: ReportPlaybackRequest,
) -> ServiceResult<PlaybackRecord> {
    let update = db.transaction_mut(|t| {
        let playback = map_internal(lyra_db::playback_sessions::get_by_id(
            t,
            playback_session_id,
        ))?
        .ok_or_else(|| PlaybackServiceError::not_found("playback session not found"))?;
        report_playback_in_transaction(t, request, playback)
    })?;
    if update.playback.playback.state.is_terminal() {
        sessions::clear_session_bindings_for_playback(
            update.playback.playback_session_id,
            &update.playback.playback_session_public_id,
        );
    }
    Ok(update.playback)
}

/// Applies the report to `playback`, a stored session.
pub(crate) fn report_playback_in_transaction(
    db: &mut impl lyra_db::DbAccess,
    request: ReportPlaybackRequest,
    mut playback: PlaybackSession,
) -> ServiceResult<TransactionPlaybackUpdate> {
    let ReportPlaybackRequest {
        user_db_id: expected_user_db_id,
        mutation,
        now_ms,
        active_event,
    } = request;
    let playback_session_id = playback.db_id.ok_or_else(|| {
        PlaybackServiceError::internal(anyhow::anyhow!("reported playback session is not stored"))
    })?;
    let playback_user_db_id = map_internal(lyra_db::playback_sessions::get_user_id(
        db,
        playback_session_id,
    ))?;
    if playback_user_db_id != Some(expected_user_db_id) {
        return Err(PlaybackServiceError::not_found(format!(
            "playback session not found: {}",
            playback_session_id.0
        )));
    }

    apply_playback_mutation(&mut playback, &mutation, now_ms)?;
    map_internal(lyra_db::playback_sessions::update(db, &playback))?;
    let track_db_id = map_internal(lyra_db::playback_sessions::get_track_id(
        db,
        playback_session_id,
    ))?
    .ok_or_else(|| {
        PlaybackServiceError::not_found(format!(
            "playback session track not found: {}",
            playback_session_id.0
        ))
    })?;
    let playback = require_playback_record(
        db,
        playback_session_id,
        track_db_id,
        expected_user_db_id,
        playback,
    )?;
    let recorded_listen = record_listen(db, &playback, now_ms)?;
    Ok(TransactionPlaybackUpdate {
        event: event_for_state(playback.playback.state, active_event),
        playback,
        recorded_listen,
    })
}

/// Pauses `playback`, a stored session, at its last reported position.
pub(crate) fn pause_playback_in_transaction(
    db: &mut impl lyra_db::DbAccess,
    playback: PlaybackSession,
    user_db_id: DbId,
    now_ms: u64,
) -> ServiceResult<Option<TransactionPlaybackUpdate>> {
    if !matches!(
        playback.state,
        PlaybackState::Playing | PlaybackState::Buffering
    ) {
        return Ok(None);
    }
    let now_ms = now_ms.max(playback.updated_at_ms);
    report_playback_in_transaction(
        db,
        ReportPlaybackRequest {
            user_db_id,
            mutation: PlaybackMutation {
                state: Some(PlaybackState::Paused),
                ..PlaybackMutation::default()
            },
            now_ms,
            active_event: ActiveEvent::Progress,
        },
        playback,
    )
    .map(Some)
}

pub(crate) fn report_playback_session(
    db: &mut DbAny,
    request: SessionPlaybackReportRequest<'_>,
) -> ServiceResult<Option<PlaybackRecord>> {
    let user =
        map_internal(lyra_db::users::get_by_id(db, request.user_db_id))?.ok_or_else(|| {
            PlaybackServiceError::not_found(format!("user not found: {}", request.user_db_id.0))
        })?;
    let scope = PlaybackScopeKey {
        plugin_id: request.plugin_id,
        user_public_id: &user.id,
        session_key: request.session_key,
    };
    sessions::cleanup_stale_scopes(request.now_ms, ACTIVE_SESSION_TTL_MS);
    let report = db.transaction_mut(|db| report_playback_session_in_transaction(db, request))?;
    crate::services::listens::dispatch_recorded(&user.id, report.recorded_listen);
    if let Some(session) = report.scope {
        sessions::apply_reported_scope(&scope, session, report.current_observed);
    } else {
        clear_playback_session_scope(&scope);
    }
    Ok(report.playback)
}

/// What a session report changed, for its caller to apply after commit.
struct SessionReport {
    playback: Option<PlaybackRecord>,
    scope: Option<PlaybackSessionScope>,
    current_observed: bool,
    recorded_listen: Option<lyra_db::Listen>,
}

impl SessionReport {
    fn ignored(scope: Option<PlaybackSessionScope>) -> Self {
        Self {
            playback: None,
            scope,
            current_observed: false,
            recorded_listen: None,
        }
    }
}

fn report_playback_session_in_transaction(
    db: &mut impl lyra_db::DbAccess,
    request: SessionPlaybackReportRequest<'_>,
) -> ServiceResult<SessionReport> {
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
        play_id,
        client_name,
        mutation,
        now_ms,
        active_event,
    } = request;

    map_internal(lyra_db::playbacks::expire_reported_for_user(
        db, user_db_id, now_ms,
    ))?;
    if map_internal(lyra_db::users::get_by_id(db, user_db_id))?.is_none() {
        return Err(PlaybackServiceError::not_found(format!(
            "user not found: {}",
            user_db_id.0
        )));
    }

    let mut root = map_internal(lyra_db::playbacks::get_reported(
        db,
        user_db_id,
        plugin_id,
        session_key,
        now_ms,
    ))?;
    let mut session = if let Some(root) = root.as_ref() {
        let root_id = root.db_id.expect("persisted playback id");
        let current_id = lyra_db::playbacks::get_current_session_id(db, root_id)?;
        let current_public_id = current_id
            .map(|id| lyra_db::playback_sessions::get_by_id(db, id))
            .transpose()
            .map_err(PlaybackServiceError::internal)?
            .flatten()
            .map(|session| session.id);
        let previous = root
            .reported
            .as_ref()
            .and_then(|source| source.previous.as_ref());
        let previous_id = previous
            .map(|previous| lyra_db::lookup::find_node_id_by_id(db, &previous.session_public_id))
            .transpose()
            .map_err(PlaybackServiceError::internal)?
            .flatten();
        Some(PlaybackSessionScope {
            current_playback_session_id: current_id,
            current_playback_session_public_id: current_public_id,
            previous_playback_session_id: previous_id,
            previous_playback_session_public_id: previous
                .map(|previous| previous.session_public_id.clone()),
            previous_demoted_at_ms: previous.map(|previous| previous.demoted_at_ms),
            previous_expires_at_ms: previous.map(|previous| previous.expires_at_ms),
            updated_at_ms: root.updated_at_ms,
            command_dispatched_at_ms: None,
            current_binding_epoch: 0,
        })
    } else {
        None
    };

    let original_current_id = session
        .as_ref()
        .and_then(|session| session.current_playback_session_id);
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
        current_playback_session_public_id: current_bound_playback
            .as_ref()
            .map(|bound| bound.playback.id.clone()),
        previous_playback_session_id: previous_bound_playback
            .as_ref()
            .map(|bound| bound.playback_session_id),
        previous_playback_session_public_id: previous_bound_playback
            .as_ref()
            .map(|bound| bound.playback.id.clone()),
        previous_demoted_at_ms: session
            .as_ref()
            .and_then(|session| session.previous_demoted_at_ms),
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

    let is_reported_play = |bound: &sessions::BoundPlayback| {
        bound.track_db_id == track_db_id
            && play_id.is_none_or(|play_id| bound.playback.play_id == play_id)
    };
    let mut target = if current_bound_playback
        .as_ref()
        .is_some_and(is_reported_play)
    {
        Some(SessionTrackTarget::Current)
    } else if previous_bound_playback
        .as_ref()
        .is_some_and(is_reported_play)
    {
        Some(SessionTrackTarget::Previous)
    } else {
        None
    };

    let mut state = if let Some(state) = mutation.state {
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

        if let Some(bound_state) = bound_state {
            if bound_state.is_terminal() {
                let current_resumed = target_slot == SessionTrackTarget::Current
                    && mutation.state.is_some_and(|state| !state.is_terminal());
                if !current_resumed && !should_reopen_terminal_same_track_session(active_event) {
                    if session_state.previous_playback_session_public_id.is_none()
                        && let Some(root) = root.as_ref()
                        && root
                            .reported
                            .as_ref()
                            .is_some_and(|source| source.previous.is_some())
                    {
                        map_internal(lyra_db::playbacks::update_reported_previous(
                            db,
                            root.db_id.expect("persisted playback id"),
                            None,
                        ))?;
                    }
                    let session = session.as_ref().and_then(|session| {
                        updated_playback_session_state(session, &session_state)
                    });
                    return Ok(SessionReport::ignored(session));
                }
                if active_event == ActiveEvent::Started && mutation.state.is_none() {
                    state = PlaybackState::Playing;
                }
                // A named play resumes its own session; without a name, it reopens as a new play.
                if play_id.is_none() {
                    target = None;
                }
            } else if active_event == ActiveEvent::Started && play_id.is_none() {
                let bound = match target_slot {
                    SessionTrackTarget::Current => current_bound_playback.as_ref(),
                    SessionTrackTarget::Previous => previous_bound_playback.as_ref(),
                };
                if let Some(bound) = bound
                    && map_internal(lyra_db::listens::find_by_play_id(
                        db,
                        user_db_id,
                        &bound.playback.play_id,
                    ))?
                    .is_some()
                {
                    // Without a play id, a start after the play counted begins a new play.
                    target = None;
                }
            }
        }
    }

    let mut playback = None;
    let mut recorded_listen = None;
    let mut current_observed = false;
    match target {
        Some(SessionTrackTarget::Current) => {
            let Some(bound) = current_bound_playback.take() else {
                return Err(PlaybackServiceError::internal(anyhow::anyhow!(
                    "missing current playback binding"
                )));
            };

            let (updated, listen) = update_session_bound_playback(
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
            recorded_listen = listen;

            set_current_session_playback(
                &mut session_state,
                updated.playback_session_id,
                updated.playback_session_public_id.clone(),
            );
            current_observed = true;
            playback = Some(updated);
        }
        Some(SessionTrackTarget::Previous) => {
            if should_apply_previous_playback_event(active_event, state) {
                let mut promoted_to_current = false;
                let previous_binding = previous_bound_playback
                    .as_ref()
                    .map(|bound| (bound.playback_session_id, bound.playback.id.clone()));

                if active_event == ActiveEvent::Started && !state.is_terminal() {
                    if current_bound_playback.is_some() {
                        move_current_to_previous_session_playback(&mut session_state, now_ms);
                    } else {
                        clear_previous_session_playback(&mut session_state);
                    }

                    if let Some((
                        previous_playback_session_id,
                        previous_playback_session_public_id,
                    )) = previous_binding
                    {
                        set_current_session_playback(
                            &mut session_state,
                            previous_playback_session_id,
                            previous_playback_session_public_id,
                        );
                        promoted_to_current = true;
                    }
                }

                let Some(bound) = previous_bound_playback.take() else {
                    return Err(PlaybackServiceError::internal(anyhow::anyhow!(
                        "missing previous playback binding"
                    )));
                };

                let (updated, listen) = update_session_bound_playback(
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
                recorded_listen = listen;

                if promoted_to_current {
                    set_current_session_playback(
                        &mut session_state,
                        updated.playback_session_id,
                        updated.playback_session_public_id.clone(),
                    );
                    current_observed = true;
                } else if updated.playback.state.is_terminal() {
                    clear_previous_session_playback(&mut session_state);
                }

                playback = Some(updated);
            }
        }
        None => {
            if !state.is_terminal() {
                let started = start_playback_in_transaction(
                    db,
                    StartPlaybackRequest {
                        play_id: play_id.map_or_else(|| nanoid!(), str::to_string),
                        track_db_id,
                        user_db_id,
                        client_name,
                        mutation: PlaybackMutation {
                            position_ms: mutation.position_ms,
                            duration_ms: mutation.duration_ms,
                            state: Some(state),
                        },
                        now_ms,
                        active_event,
                    },
                );
                let created = match started {
                    Ok(started) => {
                        recorded_listen = started.recorded_listen;
                        started.playback
                    }
                    // A report for a play whose session was replaced cannot bring it back.
                    Err(PlaybackServiceError::PlayConflict(_)) => {
                        let session = session.as_ref().and_then(|session| {
                            updated_playback_session_state(session, &session_state)
                        });
                        return Ok(SessionReport::ignored(session));
                    }
                    Err(error) => return Err(error),
                };

                current_observed = true;
                if current_bound_playback.is_some() {
                    move_current_to_previous_session_playback(&mut session_state, now_ms);
                }
                set_current_session_playback(
                    &mut session_state,
                    created.playback_session_id,
                    created.playback_session_public_id.clone(),
                );

                playback = Some(created);
            }
        }
    }

    if current_observed {
        let current_id = session_state
            .current_playback_session_id
            .expect("current observation binding");
        if let Some(root) = root.as_ref() {
            let root_id = root.db_id.expect("persisted playback id");
            if original_current_id == Some(current_id) {
                map_internal(lyra_db::playbacks::touch(db, root_id, now_ms))?;
            } else {
                map_internal(lyra_db::playbacks::link_current_session(
                    db, root_id, current_id, now_ms,
                ))?;
            }
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
                        PlaybackServiceError::bad_request(error.to_string())
                    } else {
                        PlaybackServiceError::internal(error)
                    }
                })?,
            );
        }
    }
    if let Some(root) = root.as_ref() {
        let previous = session_state
            .previous_playback_session_public_id
            .as_ref()
            .map(|id| lyra_db::playbacks::ReportedPrevious {
                session_public_id: id.clone(),
                demoted_at_ms: session_state
                    .previous_demoted_at_ms
                    .expect("previous demotion"),
                expires_at_ms: session_state
                    .previous_expires_at_ms
                    .expect("previous expiry"),
            });
        if root
            .reported
            .as_ref()
            .and_then(|source| source.previous.as_ref())
            != previous.as_ref()
        {
            map_internal(lyra_db::playbacks::update_reported_previous(
                db,
                root.db_id.expect("persisted playback id"),
                previous,
            ))?;
        }
        let scope = session.get_or_insert(PlaybackSessionScope {
            current_playback_session_id: None,
            current_playback_session_public_id: None,
            previous_playback_session_id: None,
            previous_playback_session_public_id: None,
            previous_demoted_at_ms: None,
            previous_expires_at_ms: None,
            updated_at_ms: now_ms,
            command_dispatched_at_ms: None,
            current_binding_epoch: 0,
        });
        if current_observed {
            scope.updated_at_ms = now_ms;
        }
    }
    let session = session
        .as_ref()
        .and_then(|session| updated_playback_session_state(session, &session_state));
    Ok(SessionReport {
        playback,
        scope: session,
        current_observed,
        recorded_listen,
    })
}
