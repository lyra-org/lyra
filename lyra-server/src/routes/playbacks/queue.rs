// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::*;
use crate::routes::request::bearer_credential;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
pub(super) struct QueueGetQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Require this exact queue revision, or receive 409.")
    )]
    pub(super) revision: Option<u64>,
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
    /// Where to resume the current track: the current track session's position now when it plays
    /// that track, else 0, as it is once the track completed.
    pub(super) position_ms: u64,
}

impl From<playbacks::PlayableQueue> for QueueResponse {
    fn from(queue: playbacks::PlayableQueue) -> Self {
        Self {
            revision: queue.revision,
            snapshot: queue.snapshot,
            position_ms: queue.position_ms,
        }
    }
}

fn playable_queue(
    db: &agdb::DbAny,
    user_db_id: agdb::DbId,
    playback: &lyra_db::playbacks::Playback,
    id: &str,
    current_ms: u64,
) -> Result<QueueResponse, AppError> {
    playbacks::playable_queue(db, user_db_id, playback, current_ms)
        .map_err(map_playback_error)?
        .map(QueueResponse::from)
        .ok_or_else(|| AppError::not_found(format!("playback not found: {id}")))
}

pub(super) async fn get_queue(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<QueueGetQuery>,
) -> Result<Json<QueueResponse>, AppError> {
    let principal = require_principal(bearer_credential(&headers)).await?;
    let current_ms = now_ms()?;
    let db = STATE.db.read().await;
    let user_db_id = principal.require(&db)?;
    let record = resolve_visible_playback(&db, &principal, &id, current_ms)?;
    if let Some(revision) = query.revision {
        require_revision(revision, require_queue_revision(&record.playback)?)?;
    }
    Ok(Json(playable_queue(
        &db,
        user_db_id,
        &record.playback,
        &id,
        current_ms,
    )?))
}

pub(super) async fn replace_queue(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<QueueReplaceRequest>,
) -> Result<Json<QueueResponse>, AppError> {
    let principal = require_principal(bearer_credential(&headers)).await?;
    let current_ms = now_ms()?;
    let mut db = STATE.db.write().await;
    let user_db_id = principal.require(&db)?;
    let record = resolve_owned_playback_projection(&db, &principal, &id, current_ms)?;
    require_revision(
        request.expected_revision,
        record
            .queue_revision
            .ok_or_else(|| map_playback_error(PlaybackError::QueueUnavailable))?,
    )?;
    let queue = playbacks::validate_queue(&*db, &principal, request.snapshot)
        .map_err(map_playback_error)?;
    let updated = playbacks::replace_queue(
        &mut db,
        record.db_id,
        user_db_id,
        request.expected_revision,
        queue,
        current_ms,
    )
    .map_err(map_playback_error)?;
    remote_handoffs::fail_for_playback_revision(&id, require_queue_revision(&updated.playback)?)
        .await;
    if let Some(detached) = updated.detached_session.as_ref() {
        crate::services::playbacks::bindings::clear_session(
            detached.playback_session_id,
            &detached.playback_session_public_id,
        );
    }
    let detached_update = updated
        .detached_session
        .as_ref()
        .and_then(|detached| detached.update.clone());
    let response = playable_queue(&db, user_db_id, &updated.playback, &id, current_ms)?;
    drop(db);
    if let Some(detached_update) = detached_update {
        dispatch_playback_update(&detached_update.playback, detached_update.event);
    }
    Ok(Json(response))
}

#[cfg(feature = "docgen")]
pub(super) fn get_queue_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get playback queue").description(
        "Returns the latest queue as the caller can play it, with `position_ms` to resume its current track from. Tracks missing or hidden from the caller are left out; when that drops the current track, the next track kept becomes current, else the last one before it, from its start. A queue left with no track is not found. Pass `revision` to require an exact revision for handoff; the revision is the stored queue's.",
    ).response::<409, Json<QueueRevisionConflictResponse>>()
}

#[cfg(feature = "docgen")]
pub(super) fn replace_queue_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Replace playback queue").description(
        "Atomically replaces the full queue when `expected_revision` matches and increments the server-controlled revision. Returns the queue as `GET` does.",
    ).response::<409, Json<QueueRevisionConflictResponse>>()
}
