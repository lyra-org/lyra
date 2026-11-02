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

use super::{
    ActiveEvent,
    EvictedPlaybackRecord,
    NORMAL_RATE_PERMILLE,
    PLAY_LATENCY_ALLOWANCE_MS,
    PlaybackMutation,
    PlaybackRecord,
    PlaybackServiceError,
    ReportPlaybackRequest,
    ServiceResult,
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
    kind: &str,
    db_id: DbId,
) -> ServiceResult<Option<String>> {
    if !map_internal(lyra_db::graph::is_kind(db, db_id, kind))? {
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
    let Some(track_public_id) = resolve_kind_validated_public_id(db, "Track", track_db_id)? else {
        return Ok(None);
    };
    let Some(user_public_id) = resolve_kind_validated_public_id(db, "User", user_db_id)? else {
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
    let Some(track_public_id) = resolve_kind_validated_public_id(db, "Track", track_db_id)? else {
        return Ok(None);
    };
    let Some(user_public_id) = resolve_kind_validated_public_id(db, "User", user_db_id)? else {
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

fn map_internal<T>(result: anyhow::Result<T>) -> ServiceResult<T> {
    result.map_err(PlaybackServiceError::internal)
}

/// A player's position past the duration reads as the end: its decoded length can differ from
/// the stored one.
fn within_duration(position_ms: u64, duration_ms: Option<u64>) -> u64 {
    duration_ms.map_or(position_ms, |duration_ms| position_ms.min(duration_ms))
}

/// How far a play at `rate_permille` moves in `elapsed_ms` of real time.
pub(crate) fn advanced_in(elapsed_ms: u64, rate_permille: u32) -> u64 {
    elapsed_ms.saturating_mul(u64::from(rate_permille)) / u64::from(NORMAL_RATE_PERMILLE)
}

/// The real time a play at `rate_permille` takes to move `advanced_ms`.
fn time_to_advance(advanced_ms: u64, rate_permille: u32) -> u64 {
    advanced_ms.saturating_mul(u64::from(NORMAL_RATE_PERMILLE)) / u64::from(rate_permille.max(1))
}

/// The play's position at `now_ms`: its last reported position, advanced while it plays at its
/// rate for the time since that report, within its duration.
pub(crate) fn current_position_ms(playback: &PlaybackSession, now_ms: u64) -> u64 {
    let position_ms = if playback.state == PlaybackState::Playing {
        playback.position_ms.saturating_add(advanced_in(
            now_ms.saturating_sub(playback.last_position_at_ms),
            playback.rate_permille,
        ))
    } else {
        playback.position_ms
    };
    within_duration(position_ms, playback.duration_ms)
}

/// Records the session's play as a listen once it qualifies, returning the new listen for the
/// transaction's owner to announce after commit. A play declared not to count records nothing.
fn record_listen(
    db: &mut impl lyra_db::DbAccess,
    playback_record: &PlaybackRecord,
    now_ms: u64,
) -> ServiceResult<Option<lyra_db::Listen>> {
    let playback = &playback_record.playback;
    if !playback.counts_as_listen {
        return Ok(None);
    }
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

    let mut merged_track_ids = HashSet::new();
    for (provider_id, id_type, id_value) in &target_keys {
        merged_track_ids.extend(lyra_db::external_ids::get_owners(
            db,
            provider_id,
            id_type,
            id_value,
            Some("Track"),
        )?);
    }

    if merged_track_ids.is_empty() {
        merged_track_ids.insert(track_db_id);
    }

    Ok(merged_track_ids.into_iter().collect())
}

/// The records of sessions eviction removed, leaving out those whose public ids no longer
/// resolve.
pub(crate) fn evicted_records(
    db: &DbAny,
    evicted_playbacks: Vec<lyra_db::EvictedPlayback>,
) -> ServiceResult<Vec<EvictedPlaybackRecord>> {
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

#[cfg(test)]
pub(crate) fn collect_playback_records(db: &DbAny) -> ServiceResult<Vec<PlaybackRecord>> {
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

/// Applies a report to the session, crediting playing time only for position advanced while the
/// previous report was playing, and never more than the server's elapsed time plus what is left
/// of the play's latency allowance. Playing time is real time: position advanced counts at the
/// previous report's rate.
pub(crate) fn apply_playback_mutation(
    playback: &mut PlaybackSession,
    mutation: &PlaybackMutation,
    now_ms: u64,
) {
    let state = mutation.state.unwrap_or(playback.state);
    let duration_ms = mutation.duration_ms.or(playback.duration_ms);
    // A rate change without a position settles the time since the last position at the old rate,
    // as a report of the position it reached would.
    let reported_position_ms = mutation.position_ms.or_else(|| {
        (playback.state == PlaybackState::Playing
            && mutation
                .rate_permille
                .is_some_and(|rate_permille| rate_permille != playback.rate_permille))
        .then(|| current_position_ms(playback, now_ms))
    });
    let position_ms = within_duration(
        reported_position_ms.unwrap_or(playback.position_ms),
        duration_ms,
    );

    if playback.state != PlaybackState::Playing {
        // Time spent not playing never counts, however the next report reaches a position.
        playback.last_position_at_ms = now_ms;
    }
    if reported_position_ms.is_some() {
        if playback.state == PlaybackState::Playing {
            let advanced_ms = position_ms.saturating_sub(playback.last_position_ms);
            let playing_ms = time_to_advance(advanced_ms, playback.rate_permille);
            let elapsed_ms = now_ms.saturating_sub(playback.last_position_at_ms);
            let remaining_allowance_ms =
                PLAY_LATENCY_ALLOWANCE_MS.saturating_sub(playback.latency_allowance_used_ms);
            let credited_ms = playing_ms.min(elapsed_ms.saturating_add(remaining_allowance_ms));
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
    if let Some(rate_permille) = mutation.rate_permille {
        playback.rate_permille = rate_permille;
    }
    playback.updated_at_ms = now_ms;
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
        plugin_id,
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
    let playback = new_playback_session(play_id, client_name, plugin_id, mutation, now_ms);

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
    plugin_id: Option<String>,
    mutation: PlaybackMutation,
    now_ms: u64,
) -> PlaybackSession {
    let state = mutation.state.unwrap_or(PlaybackState::Playing);
    let position_ms = within_duration(mutation.position_ms.unwrap_or(0), mutation.duration_ms);

    PlaybackSession {
        db_id: None,
        id: nanoid!(),
        play_id,
        client_name,
        plugin_id,
        position_ms,
        duration_ms: mutation.duration_ms,
        played_ms: 0,
        last_position_ms: position_ms,
        state,
        latency_allowance_used_ms: 0,
        rate_permille: mutation.rate_permille.unwrap_or(NORMAL_RATE_PERMILLE),
        counts_as_listen: mutation.counts_as_listen.unwrap_or(true),
        last_position_at_ms: now_ms,
        updated_at_ms: now_ms,
        created_at_ms: now_ms,
    }
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
    if mutation
        .counts_as_listen
        .is_some_and(|counts_as_listen| counts_as_listen != playback.counts_as_listen)
    {
        return Err(PlaybackServiceError::bad_request(format!(
            "counts_as_listen is {} for play {}; it cannot change during a play",
            playback.counts_as_listen, playback.play_id
        )));
    }

    apply_playback_mutation(&mut playback, &mutation, now_ms);
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
