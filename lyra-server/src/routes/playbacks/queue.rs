// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::*;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
pub(super) struct QueueGetQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Require this exact queue revision, or receive 409.")
    )]
    revision: Option<u64>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
pub(super) struct QueueReplaceRequest {
    pub(super) expected_revision: u64,
    #[serde(flatten)]
    pub(super) snapshot: QueueSnapshot,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize)]
pub(super) struct QueueResponse {
    pub(super) revision: u64,
    #[serde(flatten)]
    pub(super) snapshot: QueueSnapshot,
}

pub(super) async fn get_queue(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<QueueGetQuery>,
) -> Result<Json<QueueResponse>, AppError> {
    let principal = require_principal(&headers).await?;
    let db = STATE.db.read().await;
    let record = resolve_visible_playback(&db, &principal, &id)?;
    let queue = playbacks::queue_visible_to_principal(&db, &principal, &record.playback)
        .map_err(map_playback_error)?
        .ok_or_else(|| AppError::not_found(format!("playback not found: {id}")))?;
    if let Some(revision) = query.revision {
        require_revision(revision, record.playback.queue_revision)?;
    }
    Ok(Json(QueueResponse {
        revision: record.playback.queue_revision,
        snapshot: queue,
    }))
}

pub(super) async fn replace_queue(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<QueueReplaceRequest>,
) -> Result<Json<QueueResponse>, AppError> {
    let principal = require_principal(&headers).await?;
    let current_ms = now_ms()?;
    let mut db = STATE.db.write().await;
    let record = resolve_owned_playback_projection(&db, &principal, &id)?;
    let queue = playbacks::validate_queue(&*db, &principal, request.snapshot)
        .map_err(map_playback_error)?;
    let updated = playbacks::replace_queue(
        &mut db,
        record.db_id,
        principal.user_db_id,
        request.expected_revision,
        queue,
        current_ms,
    )
    .map_err(map_playback_error)?;
    remote_handoffs::fail_for_playback_revision(&id, updated.playback.queue_revision).await;
    if let Some(detached) = updated.detached_session.as_ref() {
        sessions::clear_session_bindings_for_playback(
            detached.playback_session_id,
            &detached.playback_session_public_id,
        );
    }
    let detached_update = updated
        .detached_session
        .as_ref()
        .and_then(|detached| detached.update.clone());
    let response = QueueResponse {
        revision: updated.playback.queue_revision,
        snapshot: updated.queue,
    };
    drop(db);
    if let Some(detached_update) = detached_update {
        dispatch_playback_update(&detached_update.playback, detached_update.event);
    }
    Ok(Json(response))
}

#[cfg(feature = "docgen")]
pub(super) fn get_queue_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get playback queue").description(
        "Returns the latest full queue snapshot. Pass `revision` to require an exact revision for handoff.",
    ).response::<409, Json<QueueRevisionConflictResponse>>()
}

#[cfg(feature = "docgen")]
pub(super) fn replace_queue_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Replace playback queue").description(
        "Atomically replaces the full queue when `expected_revision` matches and increments the server-controlled revision.",
    ).response::<409, Json<QueueRevisionConflictResponse>>()
}
