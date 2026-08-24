// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod progress;
mod queue;
#[cfg(test)]
mod tests;

use agdb::DbId;
#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::{
    Json,
    Router,
    extract::{
        Path,
        Query,
    },
    http::{
        HeaderMap,
        StatusCode,
    },
    routing::{
        get,
        post,
    },
};
use nanoid::nanoid;
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    STATE,
    db::{
        self,
        PlaybackState,
    },
    routes::{
        self,
        AppError,
        deserialize_inc,
        responses::PageResponse,
    },
    services::{
        auth::{
            Principal,
            require_auth,
            require_principal,
        },
        pagination::SnapshotKey,
        playback_sessions::{
            self as sessions,
            PlaybackMutation,
            dispatch_evicted_updates,
            dispatch_playback_update,
        },
        playbacks::{
            self,
            PlaybackError,
            QueueSnapshot,
        },
        remote::{
            handoffs as remote_handoffs,
            registry as remote_registry,
        },
    },
};

const NATIVE_PLAYBACK_PLUGIN_ID: &str = "native";
#[cfg(test)]
const ACTIVE_PLAYBACK_TIMEOUT_MS: u64 = sessions::ACTIVE_SESSION_TTL_MS;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct PlaybackCreateRequest {
    #[serde(flatten)]
    queue: QueueSnapshot,
    #[serde(default)]
    position_ms: Option<u64>,
    duration_ms: Option<u64>,
    state: Option<PlaybackState>,
    connection_session_key: Option<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize, Default)]
struct PlaybackQuery {
    #[serde(default)]
    active: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_inc")]
    inc: Option<Vec<String>>,
    #[serde(flatten)]
    page: routes::PageQuery,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize, Default)]
struct PlaybackDetailQuery {
    #[serde(default, deserialize_with = "deserialize_inc")]
    inc: Option<Vec<String>>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct CurrentPlaybackResponse {
    track_id: String,
    client_name: Option<String>,
    position_ms: u64,
    duration_ms: Option<u64>,
    state: PlaybackState,
    activity_ms: u64,
    updated_at: String,
    effective_position_ms: u64,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct ControllerResponse {
    connection_token: String,
    connection_session_key: String,
    client_name: Option<String>,
    supported_commands: Vec<crate::services::remote::constants::RemoteAction>,
    remote_control_degraded: bool,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct PlaybackResponse {
    id: String,
    user_id: String,
    queue_revision: u64,
    current: Option<CurrentPlaybackResponse>,
    created_at: String,
    updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    controller: Option<Option<ControllerResponse>>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct QueueRevisionConflictResponse {
    code: String,
    expected_revision: u64,
    current_revision: u64,
}

#[derive(Clone, Copy, Default)]
struct PlaybackInc {
    controller: bool,
}

fn parse_inc(inc: Option<Vec<String>>) -> Result<PlaybackInc, AppError> {
    let mut parsed = PlaybackInc::default();
    for value in inc.unwrap_or_default() {
        match value.as_str() {
            "controller" => parsed.controller = true,
            _ => {
                return Err(AppError::bad_request(format!(
                    "unsupported inc value: {value}"
                )));
            }
        }
    }
    Ok(parsed)
}

fn now_ms() -> Result<u64, AppError> {
    Ok(sessions::now_ms()?)
}

fn connection_session_key(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

#[cfg(test)]
fn resolve_id(db: &impl db::DbAccess, db_id: DbId) -> anyhow::Result<String> {
    db::lookup::find_id_by_db_id(db, db_id)?
        .ok_or_else(|| anyhow::anyhow!("entity missing id for DbId {}", db_id.0))
}

fn extrapolated_position_ms(
    position_ms: u64,
    duration_ms: Option<u64>,
    state: PlaybackState,
    updated_at_ms: u64,
    server_now_ms: u64,
) -> u64 {
    let position_ms = if state == PlaybackState::Playing {
        position_ms.saturating_add(server_now_ms.saturating_sub(updated_at_ms))
    } else {
        position_ms
    };
    duration_ms.map_or(position_ms, |duration_ms| position_ms.min(duration_ms))
}

fn current_to_response(
    current: &playbacks::CurrentSession,
    server_now_ms: u64,
) -> CurrentPlaybackResponse {
    let playback = &current.playback;
    CurrentPlaybackResponse {
        track_id: current.track_public_id.clone(),
        client_name: playback.client_name.clone(),
        position_ms: playback.position_ms,
        duration_ms: playback.duration_ms,
        state: playback.state,
        activity_ms: sessions::playback_activity_ms(playback),
        updated_at: routes::unix_ms_to_rfc3339_u64(playback.updated_at_ms),
        effective_position_ms: extrapolated_position_ms(
            playback.position_ms,
            playback.duration_ms,
            playback.state,
            playback.updated_at_ms,
            server_now_ms,
        ),
    }
}

fn controlling_connection(
    connections: &[remote_registry::ConnectionSnapshot],
    current_session_id: Option<DbId>,
) -> Option<&remote_registry::ConnectionSnapshot> {
    let current_session_id = current_session_id?;
    connections
        .iter()
        .filter_map(|connection| {
            let scope_key = sessions::PlaybackScopeKey {
                plugin_id: NATIVE_PLAYBACK_PLUGIN_ID,
                user_db_id: connection.user_db_id,
                session_key: &connection.session_key,
            };
            let scope = sessions::get_playback_session(&scope_key)?;
            (scope.current_playback_session_id == Some(current_session_id)).then_some((
                scope.updated_at_ms,
                connection.connection_id,
                connection,
            ))
        })
        .max_by_key(|&(updated_at_ms, connection_id, _)| (updated_at_ms, connection_id))
        .map(|(_, _, connection)| connection)
}

fn controller_to_response(
    connection: &remote_registry::ConnectionSnapshot,
    current_ms: u64,
) -> ControllerResponse {
    let scope_key = sessions::PlaybackScopeKey {
        plugin_id: NATIVE_PLAYBACK_PLUGIN_ID,
        user_db_id: connection.user_db_id,
        session_key: &connection.session_key,
    };
    ControllerResponse {
        connection_token: connection.token.clone(),
        connection_session_key: connection.session_key.clone(),
        client_name: connection.client_name.clone(),
        supported_commands: connection.supported_commands.clone(),
        remote_control_degraded: sessions::is_remote_control_degraded(&scope_key, current_ms),
    }
}

fn playback_to_response(
    playback: &db::playbacks::Playback,
    current_session: Option<&playbacks::CurrentSession>,
    user_public_id: &str,
    current_ms: u64,
    inc: PlaybackInc,
    connections: &[remote_registry::ConnectionSnapshot],
) -> PlaybackResponse {
    let current_session_id = current_session.map(|current| current.playback_session_id);
    let controller = inc.controller.then(|| {
        controlling_connection(connections, current_session_id)
            .map(|connection| controller_to_response(connection, current_ms))
    });
    PlaybackResponse {
        id: playback.id.clone(),
        user_id: user_public_id.to_string(),
        queue_revision: playback.queue_revision,
        current: current_session.map(|current| current_to_response(current, current_ms)),
        created_at: routes::unix_ms_to_rfc3339_u64(playback.created_at_ms),
        updated_at: routes::unix_ms_to_rfc3339_u64(playback.updated_at_ms),
        controller,
    }
}

#[cfg(test)]
fn playback_is_active(
    current_session: Option<&playbacks::CurrentSession>,
    current_ms: u64,
) -> bool {
    current_session.is_some_and(|current| {
        !current.playback.state.is_terminal()
            && current.playback.updated_at_ms
                >= current_ms.saturating_sub(ACTIVE_PLAYBACK_TIMEOUT_MS)
    })
}

fn map_playback_error(error: PlaybackError) -> AppError {
    match error {
        PlaybackError::InvalidQueue(message) => AppError::bad_request(message),
        PlaybackError::NotFound => AppError::not_found("playback not found"),
        PlaybackError::RevisionConflict {
            expected_revision,
            current_revision,
        } => revision_conflict(expected_revision, current_revision),
        PlaybackError::RevisionExhausted => AppError::conflict("queue revision exhausted"),
        PlaybackError::LimitReached => AppError::conflict("playback limit reached"),
        PlaybackError::Session(error) => error.into(),
        PlaybackError::Database(error) => anyhow::Error::from(error).into(),
        PlaybackError::Pagination(error) => error.into(),
        PlaybackError::Internal(error) => error.into(),
    }
}

fn revision_conflict(expected_revision: u64, current_revision: u64) -> AppError {
    AppError::json_conflict(
        serde_json::to_value(QueueRevisionConflictResponse {
            code: "queue_revision_conflict".to_string(),
            expected_revision,
            current_revision,
        })
        .expect("queue revision conflict response must serialize"),
    )
}

fn require_revision(expected_revision: u64, current_revision: u64) -> Result<(), AppError> {
    if expected_revision == current_revision {
        Ok(())
    } else {
        Err(revision_conflict(expected_revision, current_revision))
    }
}

fn resolve_visible_playback(
    db: &agdb::DbAny,
    principal: &Principal,
    id: &str,
) -> Result<playbacks::PlaybackDetail, AppError> {
    let playback_db_id = db::lookup::find_node_id_by_id(db, id)?
        .ok_or_else(|| AppError::not_found(format!("playback not found: {id}")))?;
    playbacks::get_visible_detail(db, playback_db_id, principal)
        .map_err(map_playback_error)?
        .ok_or_else(|| AppError::not_found(format!("playback not found: {id}")))
}

fn resolve_owned_playback_projection(
    db: &agdb::DbAny,
    principal: &Principal,
    id: &str,
) -> Result<db::playbacks::PlaybackListProjection, AppError> {
    let playback_db_id = db::lookup::find_node_id_by_id(db, id)?
        .ok_or_else(|| AppError::not_found(format!("playback not found: {id}")))?;
    playbacks::get_owned_projection(db, playback_db_id, principal.user_db_id)
        .map_err(map_playback_error)?
        .ok_or_else(|| AppError::not_found(format!("playback not found: {id}")))
}

async fn list_playbacks(
    headers: HeaderMap,
    Query(query): Query<PlaybackQuery>,
) -> Result<Json<PageResponse<PlaybackResponse>>, AppError> {
    let principal = require_principal(&headers).await?;
    let inc = parse_inc(query.inc)?;
    let active_only = query.active.unwrap_or(false);
    let page_request = query.page.resolve_snapshot();
    let snapshot_key = SnapshotKey::builder(&principal.user_public_id, "playbacks")
        .field(Some(if active_only { "active" } else { "all" }))
        .finish();
    let current_ms = now_ms()?;
    let connections = if inc.controller {
        remote_registry::list_connections().await
    } else {
        Vec::new()
    };
    let db = STATE.db.read().await;
    let (records, next_cursor) = if let Some(page) = page_request.resume(&snapshot_key)? {
        let mut records = Vec::with_capacity(page.item_ids.len());
        for id in page.item_ids {
            let Some(playback_db_id) = db::lookup::find_node_id_by_id(&*db, &id)? else {
                continue;
            };
            if let Some(record) = playbacks::get_visible_detail(&db, playback_db_id, &principal)
                .map_err(map_playback_error)?
            {
                records.push(record);
            }
        }
        (records, page.next_cursor)
    } else {
        let mut projections =
            playbacks::list_visible_projections(&db, &principal, active_only, current_ms)
                .map_err(map_playback_error)?;
        projections.sort_by(|left, right| {
            right
                .updated_at_ms
                .cmp(&left.updated_at_ms)
                .then_with(|| left.id.cmp(&right.id))
        });
        let page = page_request.start(
            &snapshot_key,
            projections
                .iter()
                .map(|projection| projection.id.clone())
                .collect(),
        )?;
        let mut records = Vec::with_capacity(page.item_ids.len());
        for id in &page.item_ids {
            let Some(playback_db_id) = db::lookup::find_node_id_by_id(&*db, id)? else {
                continue;
            };
            if let Some(record) = playbacks::get_visible_detail(&db, playback_db_id, &principal)
                .map_err(map_playback_error)?
            {
                records.push(record);
            }
        }
        (records, page.next_cursor)
    };
    let responses = records
        .iter()
        .map(|record| {
            playback_to_response(
                &record.playback,
                record.current_session.as_ref(),
                &principal.user_public_id,
                current_ms,
                inc,
                &connections,
            )
        })
        .collect::<Vec<_>>();
    Ok(Json(PageResponse {
        items: responses,
        next_cursor,
    }))
}

async fn create_playback(
    headers: HeaderMap,
    Json(request): Json<PlaybackCreateRequest>,
) -> Result<(StatusCode, Json<PlaybackResponse>), AppError> {
    let auth = require_auth(&headers).await?;
    let current_ms = now_ms()?;
    let mut db = STATE.db.write().await;
    let queue = playbacks::validate_queue(&*db, &auth.principal, request.queue)
        .map_err(map_playback_error)?;
    let update = playbacks::create_playback(
        &mut db,
        playbacks::CreatePlaybackRequest {
            id: nanoid!(),
            user_db_id: auth.principal.user_db_id,
            client_name: auth.client_name,
            queue,
            mutation: PlaybackMutation {
                position_ms: request.position_ms,
                duration_ms: request.duration_ms,
                state: request.state.or(Some(PlaybackState::Playing)),
            },
            now_ms: current_ms,
        },
    )
    .map_err(map_playback_error)?;
    if let Some(session_key) = connection_session_key(request.connection_session_key.as_deref())
        && !update.session.playback.state.is_terminal()
    {
        sessions::bind_current_playback_session_scope(
            &sessions::PlaybackScopeKey {
                plugin_id: NATIVE_PLAYBACK_PLUGIN_ID,
                user_db_id: auth.principal.user_db_id,
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
        &auth.principal.user_public_id,
        current_ms,
        PlaybackInc::default(),
        &[],
    );
    drop(db);
    dispatch_playback_update(&update.session, update.event);
    dispatch_evicted_updates(update.evicted_playbacks);
    Ok((StatusCode::CREATED, Json(response)))
}

async fn get_playback(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<PlaybackDetailQuery>,
) -> Result<Json<PlaybackResponse>, AppError> {
    let principal = require_principal(&headers).await?;
    let inc = parse_inc(query.inc)?;
    let current_ms = now_ms()?;
    let connections = if inc.controller {
        remote_registry::list_connections().await
    } else {
        Vec::new()
    };
    let db = STATE.db.read().await;
    let record = resolve_visible_playback(&db, &principal, &id)?;
    Ok(Json(playback_to_response(
        &record.playback,
        record.current_session.as_ref(),
        &principal.user_public_id,
        current_ms,
        inc,
        &connections,
    )))
}

async fn delete_playback(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    let principal = require_principal(&headers).await?;
    let mut db = STATE.db.write().await;
    let record = resolve_owned_playback_projection(&db, &principal, &id)?;
    let current = match db::playbacks::get_current_session_id(&*db, record.db_id)? {
        Some(session_id) => db::playback_sessions::get_by_id(&*db, session_id)?
            .map(|session| (session_id, session.id)),
        None => None,
    };
    db::playbacks::delete(&mut db, record.db_id)?;
    remote_handoffs::fail_for_playback(&record.id).await;
    if let Some((playback_session_id, playback_session_public_id)) = current {
        sessions::clear_session_bindings_for_playback(
            playback_session_id,
            &playback_session_public_id,
        );
    }
    drop(db);
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(feature = "docgen")]
fn create_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Create playback").description(
        "Creates a durable user-owned playback with the complete initial queue at revision 1.",
    )
}

#[cfg(feature = "docgen")]
fn list_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List playbacks").description(
        "Returns `{ items, next_cursor }` for the authenticated user's durable playbacks, ordered by most recently updated. `active=true` filters the initial snapshot to recent non-terminal current sessions; `inc=controller` includes current WebSocket controller details. Drive pagination from `next_cursor`.",
    )
}

#[cfg(feature = "docgen")]
fn detail_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get playback")
}

#[cfg(feature = "docgen")]
fn delete_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Delete playback").description(
        "Deletes the durable playback aggregate. Track-scoped listen-accounting records are retained.",
    )
}

pub fn playback_routes() -> Router {
    Router::new()
        .route("/", get(list_playbacks).post(create_playback))
        .route("/{id}", get(get_playback).delete(delete_playback))
        .route(
            "/{id}/queue",
            get(queue::get_queue).put(queue::replace_queue),
        )
        .route("/{id}/progress", post(progress::report_progress))
}

#[cfg(feature = "docgen")]
pub(crate) fn playback_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        get_with,
        post_with,
    };

    aide::axum::ApiRouter::new()
        .api_route(
            "/",
            get_with(list_playbacks, list_docs).post_with(create_playback, create_docs),
        )
        .api_route(
            "/{id}",
            get_with(get_playback, detail_docs).delete_with(delete_playback, delete_docs),
        )
        .api_route(
            "/{id}/queue",
            get_with(queue::get_queue, queue::get_queue_docs)
                .put_with(queue::replace_queue, queue::replace_queue_docs),
        )
        .api_route(
            "/{id}/progress",
            post_with(progress::report_progress, progress::progress_docs),
        )
}
