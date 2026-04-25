// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::DbId;
use aide::axum::{
    ApiRouter,
    routing::{
        get_with,
        post_with,
    },
};
use aide::transform::TransformOperation;
use axum::{
    Json,
    extract::{
        Path,
        Query,
    },
    http::HeaderMap,
};
use schemars::JsonSchema;
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
    routes::AppError,
    services::{
        auth::{
            AuthCredential,
            require_auth,
            require_principal,
        },
        playback_sessions::{
            self as playbacks,
            ActiveEvent,
            ActivityPolicy,
            PlaybackMutation,
            PlaybackRecord,
            dispatch_evicted_updates,
            dispatch_playback_update,
        },
        remote::registry as remote_registry,
    },
};

const ACTIVE_PLAYBACK_TIMEOUT_MS: u64 = playbacks::ACTIVE_SESSION_TTL_MS;
const REST_PLAYBACK_PLUGIN_ID: &str = "rest";

#[derive(Deserialize, JsonSchema)]
struct PlaybackStartRequest {
    #[schemars(description = "Track ID for this playback session.")]
    track_id: String,
    #[schemars(description = "Initial playback position in milliseconds.")]
    #[serde(default)]
    position_ms: Option<u64>,
    #[schemars(description = "Track duration in milliseconds, if known.")]
    duration_ms: Option<u64>,
    #[schemars(
        description = "Initial playback state: playing, paused, stopped, buffering, completed."
    )]
    state: Option<PlaybackState>,
}

#[derive(Deserialize, JsonSchema)]
struct PlaybackListQuery {
    #[schemars(description = "When true, returns only active, non-stale playback sessions.")]
    active: Option<bool>,
}

#[derive(Deserialize, JsonSchema)]
struct PlaybackProgressRequest {
    #[schemars(description = "Playback position in milliseconds.")]
    position_ms: Option<u64>,
    #[schemars(description = "Track duration in milliseconds, if known.")]
    duration_ms: Option<u64>,
    #[schemars(description = "Playback state: playing, paused, stopped, buffering, completed.")]
    state: Option<PlaybackState>,
}

#[derive(Serialize, JsonSchema)]
struct PlaybackResponse {
    #[schemars(description = "Playback session ID.")]
    playback_session_id: String,
    #[schemars(description = "Track ID associated with this playback.")]
    track_id: String,
    #[schemars(description = "User ID who owns this playback.")]
    user_id: String,
    #[schemars(description = "Playback position in milliseconds.")]
    position_ms: u64,
    #[schemars(description = "Track duration in milliseconds, if known.")]
    duration_ms: Option<u64>,
    #[schemars(description = "Playback state.")]
    state: PlaybackState,
    #[schemars(description = "Accumulated effective listening activity in milliseconds.")]
    activity_ms: u64,
    #[schemars(description = "Last report timestamp in epoch milliseconds.")]
    updated_at_ms: u64,
    #[schemars(
        description = "Server-time playback position in milliseconds extrapolated from last update while playing."
    )]
    effective_position_ms: u64,
}

fn now_ms() -> Result<u64, AppError> {
    Ok(playbacks::now_ms()?)
}

fn extrapolated_playback_position_ms(
    position_ms: u64,
    duration_ms: Option<u64>,
    state: PlaybackState,
    updated_at_ms: u64,
    server_now_ms: u64,
) -> u64 {
    let mut effective_position_ms = position_ms;
    if state == PlaybackState::Playing {
        let elapsed_since_update_ms = server_now_ms.saturating_sub(updated_at_ms);
        effective_position_ms = effective_position_ms.saturating_add(elapsed_since_update_ms);
    }

    if let Some(duration_ms) = duration_ms {
        effective_position_ms = effective_position_ms.min(duration_ms);
    }

    effective_position_ms
}

fn resolve_id(db: &impl db::DbAccess, db_id: DbId) -> anyhow::Result<String> {
    db::lookup::find_id_by_db_id(db, db_id)?
        .ok_or_else(|| anyhow::anyhow!("entity missing id for DbId {}", db_id.0))
}

fn playback_to_response(
    db: &impl db::DbAccess,
    playback: &PlaybackRecord,
    server_now_ms: u64,
) -> anyhow::Result<PlaybackResponse> {
    let activity_ms = playbacks::playback_activity_ms(&playback.playback);
    let effective_position_ms = extrapolated_playback_position_ms(
        playback.playback.position_ms,
        playback.playback.duration_ms,
        playback.playback.state,
        playback.playback.updated_at_ms,
        server_now_ms,
    );

    Ok(PlaybackResponse {
        playback_session_id: resolve_id(db, playback.playback_session_id)?,
        track_id: resolve_id(db, playback.track_db_id)?,
        user_id: resolve_id(db, playback.user_db_id)?,
        position_ms: playback.playback.position_ms,
        duration_ms: playback.playback.duration_ms,
        state: playback.playback.state,
        activity_ms,
        updated_at_ms: playback.playback.updated_at_ms,
        effective_position_ms,
    })
}

fn rest_playback_session_key(credential: &AuthCredential) -> Option<String> {
    match credential {
        AuthCredential::Session { session_id } => Some(format!("auth:{}", session_id.0)),
        AuthCredential::ApiKey { .. } | AuthCredential::Default => None,
    }
}

fn active_playback_cutoff_ms(current_ms: u64, state: PlaybackState) -> Option<u64> {
    if state.is_terminal() {
        return None;
    }

    Some(current_ms.saturating_sub(ACTIVE_PLAYBACK_TIMEOUT_MS))
}

fn playback_is_recently_active(current_ms: u64, updated_at_ms: u64, state: PlaybackState) -> bool {
    let Some(cutoff_ms) = active_playback_cutoff_ms(current_ms, state) else {
        return false;
    };

    updated_at_ms >= cutoff_ms
}

async fn get_playbacks(
    headers: HeaderMap,
    Query(query): Query<PlaybackListQuery>,
) -> Result<Json<Vec<PlaybackResponse>>, AppError> {
    let principal = require_principal(&headers).await?;

    let current_ms = now_ms()?;
    {
        let mut db = STATE.db.write().await;
        let evicted_playbacks = playbacks::cleanup_evicted_playbacks(&mut db, current_ms)?;
        dispatch_evicted_updates(evicted_playbacks);
    }

    let db = STATE.db.read().await;
    let mut response = Vec::new();
    let only_active = query.active.unwrap_or(false);

    for playback in playbacks::list_playbacks(&db, principal.user_db_id)? {
        if only_active
            && !playback_is_recently_active(
                current_ms,
                playback.playback.updated_at_ms,
                playback.playback.state,
            )
        {
            continue;
        }

        response.push(playback_to_response(&*db, &playback, current_ms)?);
    }

    response.sort_by(|a, b| b.updated_at_ms.cmp(&a.updated_at_ms));

    Ok(Json(response))
}

async fn start_playback(
    headers: HeaderMap,
    Json(request): Json<PlaybackStartRequest>,
) -> Result<Json<PlaybackResponse>, AppError> {
    let auth = require_auth(&headers).await?;
    let principal = &auth.principal;

    let current_ms = now_ms()?;
    let mutation = PlaybackMutation {
        position_ms: request.position_ms,
        duration_ms: request.duration_ms,
        state: request.state.or(Some(PlaybackState::Playing)),
    };

    let mut db = STATE.db.write().await;
    let track_db_id = db::lookup::find_node_id_by_id(&*db, &request.track_id)?
        .ok_or_else(|| AppError::not_found(format!("Track not found: {}", request.track_id)))?;

    let (playback, event, evicted_playbacks) = if let Some(session_key) =
        rest_playback_session_key(&auth.credential)
    {
        let update = playbacks::report_playback_session_with_cleanup(
            &mut db,
            playbacks::SessionPlaybackReportRequest {
                plugin_id: REST_PLAYBACK_PLUGIN_ID,
                user_db_id: principal.user_db_id,
                session_key: &session_key,
                track_db_id,
                mutation: mutation.clone(),
                now_ms: current_ms,
                active_event: ActiveEvent::Started,
                stale_ttl_ms: playbacks::ACTIVE_SESSION_TTL_MS,
            },
        )?;
        let playbacks::OptionalPlaybackUpdateResult {
            playback,
            event,
            evicted_playbacks,
        } = update;
        let playback = playback
            .ok_or_else(|| AppError::bad_request("cannot start playback with terminal state"))?;
        (
            playback,
            event.unwrap_or_else(|| ActiveEvent::Started.to_string()),
            evicted_playbacks,
        )
    } else {
        let update = playbacks::start_playback_with_cleanup(
            &mut db,
            playbacks::StartPlaybackRequest {
                track_db_id,
                user_db_id: principal.user_db_id,
                mutation,
                now_ms: current_ms,
                active_event: ActiveEvent::Started,
            },
        )?;
        (update.playback, update.event, update.evicted_playbacks)
    };
    dispatch_evicted_updates(evicted_playbacks);

    let response = playback_to_response(&*db, &playback, current_ms)?;
    drop(db);
    dispatch_playback_update(&playback, event);
    Ok(Json(response))
}

async fn report_playback_progress(
    headers: HeaderMap,
    Path(playback_session_id): Path<String>,
    Json(request): Json<PlaybackProgressRequest>,
) -> Result<Json<PlaybackResponse>, AppError> {
    let principal = require_principal(&headers).await?;

    let current_ms = now_ms()?;

    let mut db = STATE.db.write().await;
    let session_db_id = db::lookup::find_node_id_by_id(&*db, &playback_session_id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {playback_session_id}")))?;
    let update = playbacks::report_playback_with_cleanup(
        &mut db,
        playbacks::ReportPlaybackRequest {
            playback_session_id: session_db_id,
            user_db_id: Some(principal.user_db_id),
            mutation: PlaybackMutation {
                position_ms: request.position_ms,
                duration_ms: request.duration_ms,
                state: request.state,
            },
            now_ms: current_ms,
            activity_policy: ActivityPolicy::AnyState,
            active_event: ActiveEvent::Progress,
        },
    )?;
    dispatch_evicted_updates(update.evicted_playbacks);

    let response = playback_to_response(&*db, &update.playback, current_ms)?;
    drop(db);
    dispatch_playback_update(&update.playback, update.event);
    Ok(Json(response))
}

#[derive(Serialize, JsonSchema)]
struct ActiveSessionResponse {
    #[schemars(description = "Playback session details.")]
    #[serde(flatten)]
    playback: PlaybackResponse,
    #[schemars(description = "Opaque connection token for the controlling connection, if any.")]
    connection_token: Option<String>,
    #[schemars(description = "Client-provided session key of the controlling connection, if any.")]
    connection_session_key: Option<String>,
    #[schemars(
        description = "Remote control commands supported by the controlling connection, if any."
    )]
    supported_commands: Vec<crate::services::remote::constants::RemoteAction>,
    #[schemars(
        description = "Whether remote control is degraded (command dispatched but no state update received within timeout)."
    )]
    remote_control_degraded: bool,
}

async fn get_active_sessions(
    headers: HeaderMap,
) -> Result<Json<Vec<ActiveSessionResponse>>, AppError> {
    let principal = require_principal(&headers).await?;

    let current_ms = now_ms()?;
    {
        let mut db = STATE.db.write().await;
        let evicted_playbacks = playbacks::cleanup_evicted_playbacks(&mut db, current_ms)?;
        dispatch_evicted_updates(evicted_playbacks);
    }

    let db = STATE.db.read().await;
    let connections = remote_registry::list_connections().await;

    let mut response = Vec::new();
    for playback in playbacks::list_playbacks(&db, principal.user_db_id)? {
        if !playback_is_recently_active(
            current_ms,
            playback.playback.updated_at_ms,
            playback.playback.state,
        ) {
            continue;
        }

        let playback_response = playback_to_response(&*db, &playback, current_ms)?;

        // Match a WS connection to this playback only if the connection's native
        // scope currently owns this playback session.
        let connection = connections.iter().find(|c| {
            let scope_key = playbacks::PlaybackScopeKey {
                plugin_id: "native",
                user_db_id: c.user_db_id,
                session_key: &c.session_key,
            };
            playbacks::get_playback_session(&scope_key)
                .is_some_and(|scope| scope.current_playback_session_id == playback.playback.db_id)
        });

        let degraded = connection
            .map(|c| {
                let scope_key = playbacks::PlaybackScopeKey {
                    plugin_id: "native",
                    user_db_id: c.user_db_id,
                    session_key: &c.session_key,
                };
                playbacks::is_remote_control_degraded(&scope_key, current_ms)
            })
            .unwrap_or(false);

        response.push(ActiveSessionResponse {
            playback: playback_response,
            connection_token: connection.map(|c| c.token.clone()),
            connection_session_key: connection.map(|c| c.session_key.clone()),
            supported_commands: connection
                .map(|c| c.supported_commands.clone())
                .unwrap_or_default(),
            remote_control_degraded: degraded,
        });
    }

    response.sort_by(|a, b| b.playback.updated_at_ms.cmp(&a.playback.updated_at_ms));

    Ok(Json(response))
}

fn start_playback_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Create playback session").description(
        "Starts or resumes session-scoped playback and returns its playback session ID.",
    )
}

fn report_playback_progress_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Report playback progress").description(
        "Updates the latest position and state for one of the authenticated user's playback session IDs.",
    )
}

fn list_playbacks_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List playback sessions")
        .description(
            "Returns the authenticated user's playback sessions by default. Use `active=true` to return only active and recently updated playback sessions.",
        )
}

fn active_sessions_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List active playback sessions")
        .description(
            "Returns the authenticated user's active playback sessions enriched with WebSocket connection info for remote control.",
        )
}

pub fn playback_session_routes() -> ApiRouter {
    ApiRouter::new()
        .api_route(
            "/",
            get_with(get_playbacks, list_playbacks_docs)
                .post_with(start_playback, start_playback_docs),
        )
        .api_route(
            "/active",
            get_with(get_active_sessions, active_sessions_docs),
        )
        .api_route(
            "/{playback_session_id}/progress",
            post_with(report_playback_progress, report_playback_progress_docs),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_filter_includes_non_terminal_playback_within_active_timeout() {
        let current_ms = 1_000_000;
        let recent = current_ms - (ACTIVE_PLAYBACK_TIMEOUT_MS - 1_000);

        assert!(playback_is_recently_active(
            current_ms,
            recent,
            PlaybackState::Paused,
        ));
    }

    #[test]
    fn active_filter_excludes_non_terminal_playback_older_than_active_timeout() {
        let current_ms = 1_000_000;
        let stale = current_ms - (ACTIVE_PLAYBACK_TIMEOUT_MS + 1_000);

        assert!(!playback_is_recently_active(
            current_ms,
            stale,
            PlaybackState::Playing,
        ));
    }

    #[test]
    fn active_filter_excludes_terminal_states() {
        let current_ms = 1_000_000;
        let updated_at_ms = current_ms;

        assert!(!playback_is_recently_active(
            current_ms,
            updated_at_ms,
            PlaybackState::Stopped,
        ));
        assert!(!playback_is_recently_active(
            current_ms,
            updated_at_ms,
            PlaybackState::Completed,
        ));
    }

    #[test]
    fn extrapolated_position_advances_when_playing() {
        let effective = extrapolated_playback_position_ms(
            10_000,
            Some(300_000),
            PlaybackState::Playing,
            1_000,
            3_500,
        );

        assert_eq!(effective, 12_500);
    }

    #[test]
    fn extrapolated_position_does_not_advance_when_not_playing() {
        let effective = extrapolated_playback_position_ms(
            10_000,
            Some(300_000),
            PlaybackState::Paused,
            1_000,
            3_500,
        );

        assert_eq!(effective, 10_000);
    }

    #[test]
    fn extrapolated_position_clamps_to_duration() {
        let effective = extrapolated_playback_position_ms(
            298_000,
            Some(300_000),
            PlaybackState::Playing,
            1_000,
            5_000,
        );

        assert_eq!(effective, 300_000);
    }

    #[test]
    fn extrapolated_position_uses_zero_elapsed_when_server_time_is_behind_update() {
        let effective = extrapolated_playback_position_ms(
            10_000,
            Some(300_000),
            PlaybackState::Playing,
            5_000,
            3_000,
        );

        assert_eq!(effective, 10_000);
    }

    #[test]
    fn rest_playback_session_key_uses_auth_session_id() {
        let credential = AuthCredential::Session {
            session_id: DbId(77),
        };

        assert_eq!(
            rest_playback_session_key(&credential).as_deref(),
            Some("auth:77")
        );
    }

    #[test]
    fn rest_playback_session_key_is_none_without_auth_session() {
        let credential = AuthCredential::ApiKey {
            api_key_id: DbId(77),
            name: String::from("demo"),
        };

        assert!(rest_playback_session_key(&credential).is_none());
    }
}
