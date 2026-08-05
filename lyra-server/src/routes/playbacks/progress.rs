// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::*;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
pub(super) struct PlaybackProgressRequest {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Exact queue revision installed by the reporting client.")
    )]
    pub(super) queue_revision: u64,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Current track ID; when present it must match the queue item.")
    )]
    pub(super) track_id: Option<String>,
    pub(super) position_ms: Option<u64>,
    pub(super) duration_ms: Option<u64>,
    pub(super) state: Option<PlaybackState>,
    pub(super) connection_session_key: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Opaque token from a forwarded handoff_queue command. Exact progress from the designated target completes the handoff."
        )
    )]
    pub(super) handoff_token: Option<String>,
}

pub(super) async fn report_progress(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<PlaybackProgressRequest>,
) -> Result<Json<PlaybackResponse>, AppError> {
    let auth = require_auth(&headers).await?;
    let principal = &auth.principal;
    let current_ms = now_ms()?;
    let connection_session_key =
        connection_session_key(request.connection_session_key.as_deref()).map(str::to_string);
    if request.handoff_token.is_some() && connection_session_key.is_none() {
        return Err(AppError::bad_request(
            "connection_session_key is required with handoff_token",
        ));
    }
    let mut db = STATE.db.write().await;
    let record = resolve_visible_playback(&db, principal, &id)?;
    require_revision(request.queue_revision, record.playback.queue_revision)?;
    if request.handoff_token.is_some()
        && request
            .state
            .or_else(|| {
                record
                    .current_session
                    .as_ref()
                    .map(|current| current.playback.state)
            })
            .unwrap_or(PlaybackState::Playing)
            .is_terminal()
    {
        return Err(AppError::bad_request(
            "handoff progress must leave the current track session active",
        ));
    }

    let queue = playbacks::queue_from_playback(&record.playback).map_err(map_playback_error)?;
    let current_track_public_id = queue.current_track_id();
    if let Some(track_id) = request.track_id.as_deref()
        && track_id != current_track_public_id
    {
        return Err(AppError::bad_request(format!(
            "track_id {track_id} does not match queue current track {current_track_public_id}"
        )));
    }
    let current_track_db_id = db::lookup::find_node_id_by_id(&*db, current_track_public_id)?
        .filter(|db_id| db::tracks::get_by_id(&*db, *db_id).ok().flatten().is_some())
        .ok_or_else(|| {
            AppError::not_found(format!("track not found: {current_track_public_id}"))
        })?;
    crate::services::auth::access::require_entity_accessible(
        &*db,
        principal,
        current_track_db_id,
        || AppError::not_found(format!("track not found: {current_track_public_id}")),
    )?;

    let progress_claim = if let Some(handoff_token) = request.handoff_token.as_deref() {
        Some(
            remote_handoffs::claim_progress(
                handoff_token,
                principal.user_db_id,
                connection_session_key
                    .as_deref()
                    .expect("handoff progress requires a connection session key"),
                &id,
                request.queue_revision,
            )
            .await
            .map_err(AppError::conflict)?,
        )
    } else {
        None
    };

    let mutation = PlaybackMutation {
        position_ms: request.position_ms,
        duration_ms: request.duration_ms,
        state: request.state,
    };
    let update = playbacks::report_progress(
        &mut db,
        playbacks::ReportProgressRequest {
            playback_db_id: record.playback_db_id,
            user_db_id: principal.user_db_id,
            client_name: auth.client_name,
            queue_revision: request.queue_revision,
            current_track_db_id,
            mutation,
            now_ms: current_ms,
            require_full_queue_access: request.handoff_token.is_some(),
        },
    )
    .map_err(map_playback_error);
    let update = match update {
        Ok(update) => update,
        Err(error) => {
            drop(db);
            if let Some(progress_claim) = progress_claim {
                progress_claim.abort("handoff progress update failed").await;
            }
            return Err(error);
        }
    };
    let committed_progress = progress_claim.map(|claim| {
        claim.commit(remote_handoffs::AppliedProgress {
            user_db_id: principal.user_db_id,
            playback_db_id: record.playback_db_id,
            playback_public_id: update.playback.id.clone(),
            queue_revision: request.queue_revision,
            expected_session: update.session.playback.clone(),
        })
    });

    if committed_progress.is_none()
        && let Some(session_key) = connection_session_key.as_deref()
        && !update.session.playback.state.is_terminal()
    {
        sessions::bind_current_playback_session_scope(
            &sessions::PlaybackScopeKey {
                plugin_id: NATIVE_PLAYBACK_PLUGIN_ID,
                user_db_id: principal.user_db_id,
                session_key,
            },
            update.session.playback_session_id,
            update.session.playback_session_public_id.clone(),
            current_ms,
        );
    }
    let response = playback_to_response(
        &update.playback,
        Some(&playbacks::CurrentSession::from(&update.session)),
        &principal.user_public_id,
        current_ms,
        PlaybackInc::default(),
        &[],
    );
    drop(db);
    sessions::dispatch_evicted_updates(update.evicted_playbacks);
    if let Some(committed_progress) = committed_progress
        && !committed_progress.finish().await
    {
        return Err(AppError::conflict("handoff failed while applying progress"));
    }
    dispatch_playback_update(&update.session, update.event);
    Ok(Json(response))
}

#[cfg(feature = "docgen")]
pub(super) fn progress_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Report playback progress").description(
        "Reports track-scoped state against an exact queue revision. A target handling handoff_queue completes the handoff by including its opaque handoff_token and connection_session_key here after applying the queue.",
    ).response::<409, Json<QueueRevisionConflictResponse>>()
}
