// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use agdb::DbId;
use axum::extract::ws::{
    Message,
    WebSocket,
};
use tokio::sync::{
    Notify,
    broadcast,
    mpsc,
};
use tokio::time::{
    Instant,
    interval_at,
};

use super::constants::{
    AUTH_CHECK_INTERVAL,
    PING_INTERVAL,
    PONG_TIMEOUT,
    RemoteAction,
    WRITE_TIMEOUT,
};
use super::messages::{
    ClientCommand,
    EventMessage,
    ForwardedCommand,
    ForwardedCommandData,
    IncomingMessage,
    OutgoingMessage,
    ResponseMessage,
};
use super::registry::{
    self,
    ConnectionId,
};

use crate::services::auth::{
    self,
    AuthError,
};
use crate::services::playback_sessions as playbacks;
use crate::services::playback_sessions::{
    PlaybackUpdatePayload,
    subscribe_playback_events,
};

const NATIVE_SOURCE_ID: &str = "native";

async fn send_raw(socket: &mut WebSocket, msg: Message) -> bool {
    tokio::time::timeout(WRITE_TIMEOUT, socket.send(msg))
        .await
        .is_ok_and(|r| r.is_ok())
}

async fn send(socket: &mut WebSocket, msg: OutgoingMessage) -> bool {
    let text = match serde_json::to_string(&msg) {
        Ok(text) => text,
        Err(_) => return false,
    };
    send_raw(socket, Message::Text(text.into())).await
}

fn extract_id(text: &str) -> String {
    #[derive(serde::Deserialize)]
    struct IdOnly {
        #[serde(default)]
        id: String,
    }
    serde_json::from_str::<IdOnly>(text)
        .map(|v| v.id)
        .unwrap_or_default()
}

async fn handle_message(msg: IncomingMessage, connection_id: ConnectionId) -> OutgoingMessage {
    match msg {
        IncomingMessage::Command(cmd) => {
            let response = handle_command(cmd, connection_id).await;
            OutgoingMessage::Response(response)
        }
    }
}

async fn handle_command(cmd: ClientCommand, connection_id: ConnectionId) -> ResponseMessage {
    match cmd {
        ClientCommand::DeclareCapabilities { id, commands } => {
            handle_declare_capabilities(id, commands, connection_id).await
        }
        ClientCommand::Play(_)
        | ClientCommand::Pause(_)
        | ClientCommand::Unpause(_)
        | ClientCommand::Stop(_)
        | ClientCommand::Seek(_)
        | ClientCommand::NextTrack(_)
        | ClientCommand::PreviousTrack(_)
        | ClientCommand::SetVolume(_) => handle_remote_control(cmd, connection_id).await,
    }
}

async fn handle_declare_capabilities(
    id: String,
    commands: Vec<RemoteAction>,
    connection_id: ConnectionId,
) -> ResponseMessage {
    let set: std::collections::HashSet<RemoteAction> = commands.into_iter().collect();
    if registry::set_supported_commands(connection_id, set).await {
        ResponseMessage::ok(id)
    } else {
        ResponseMessage::error(id, "connection not found")
    }
}

async fn handle_remote_control(cmd: ClientCommand, connection_id: ConnectionId) -> ResponseMessage {
    let id = cmd.id().to_string();
    let action = match cmd.remote_action() {
        Some(a) => a,
        None => return ResponseMessage::error(id, "not a remote control action"),
    };
    let target_str = match cmd.target() {
        Some(t) => t.to_string(),
        None => {
            return ResponseMessage::error(id, "target is required for remote control commands");
        }
    };

    let target_id: ConnectionId = match target_str.parse() {
        Ok(id) => id,
        Err(_) => return ResponseMessage::error(id, format!("invalid target: {target_str}")),
    };

    if target_id == connection_id {
        return ResponseMessage::error(id, "cannot target self");
    }

    let (source, target_snap) = registry::get_connection_pair(connection_id, target_id).await;

    let Some(source) = source else {
        return ResponseMessage::error(id, "source connection not found");
    };
    let Some(target_snap) = target_snap else {
        return ResponseMessage::error(id, "target connection not found");
    };

    if source.user_db_id != target_snap.user_db_id {
        return ResponseMessage::error(id, "not authorized to control target");
    }

    if !target_snap.supported_commands.contains(&action) {
        return ResponseMessage::error(id, format!("target does not support command: {action:?}"));
    }

    let data = match &cmd {
        ClientCommand::Seek(c) => ForwardedCommandData::Seek {
            position_ms: c.position_ms,
        },
        ClientCommand::SetVolume(c) => ForwardedCommandData::Volume {
            level: c.level.clamp(0.0, 1.0),
        },
        _ => ForwardedCommandData::Simple,
    };

    let forwarded = OutgoingMessage::Command(ForwardedCommand {
        action,
        from: Some(connection_id),
        data,
    });

    match registry::send_to_connection(target_id, forwarded).await {
        Ok(()) => {
            if let Ok(now_ms) = playbacks::now_ms() {
                let scope_key = playbacks::PlaybackScopeKey {
                    plugin_id: NATIVE_SOURCE_ID,
                    user_db_id: target_snap.user_db_id,
                    session_key: &target_snap.session_key,
                };
                playbacks::mark_command_dispatched(&scope_key, now_ms);
            }
            ResponseMessage::ok(id)
        }
        Err(err) => ResponseMessage::error(id, err),
    }
}

fn playback_event_to_message(payload: PlaybackUpdatePayload) -> Option<OutgoingMessage> {
    let data = serde_json::to_value(payload).ok()?;
    Some(OutgoingMessage::Event(EventMessage {
        event: "playback_state_changed".to_string(),
        data,
    }))
}

enum AuthStatus {
    Valid,
    Revoked,
    Error,
}

const MAX_CONSECUTIVE_AUTH_ERRORS: u32 = 5;

async fn check_auth(token: &Option<String>) -> AuthStatus {
    match auth::resolve_auth_from_bearer(token.as_deref()).await {
        Ok(Some(_)) => AuthStatus::Valid,
        Ok(None) => AuthStatus::Revoked,
        Err(AuthError::SessionExpired) => AuthStatus::Revoked,
        Err(_) => AuthStatus::Error,
    }
}

pub(crate) async fn run(
    mut socket: WebSocket,
    connection_id: ConnectionId,
    user_db_id: DbId,
    cancel: Arc<Notify>,
    token: Option<String>,
    mut command_rx: mpsc::Receiver<OutgoingMessage>,
) {
    let mut ping_interval = interval_at(Instant::now() + PING_INTERVAL, PING_INTERVAL);
    let mut auth_interval = interval_at(Instant::now() + AUTH_CHECK_INTERVAL, AUTH_CHECK_INTERVAL);
    let mut event_rx = subscribe_playback_events();
    let mut awaiting_pong = false;
    let mut pong_deadline: Option<Instant> = None;
    let mut consecutive_auth_errors: u32 = 0;

    loop {
        let sleep = async {
            if let Some(deadline) = pong_deadline {
                tokio::time::sleep_until(deadline).await;
            } else {
                std::future::pending::<()>().await;
            }
        };

        tokio::select! {
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Pong(_))) => {
                        awaiting_pong = false;
                        pong_deadline = None;
                    }
                    Some(Ok(Message::Ping(data))) => {
                        if !send_raw(&mut socket, Message::Pong(data)).await {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        break;
                    }
                    Some(Ok(Message::Text(text))) => {
                        let response = match serde_json::from_str::<IncomingMessage>(&text) {
                            Ok(msg) => handle_message(msg, connection_id).await,
                            Err(err) => {
                                let id = extract_id(&text);
                                OutgoingMessage::Response(ResponseMessage::error(
                                    id,
                                    format!("malformed message: {err}"),
                                ))
                            }
                        };
                        if !send(&mut socket, response).await {
                            break;
                        }
                    }
                    // Binary frames are intentionally ignored; only JSON text is supported.
                    Some(Ok(Message::Binary(_))) => {}
                    Some(Err(_)) => {
                        break;
                    }
                }
            }

            Some(forwarded) = command_rx.recv() => {
                if !send(&mut socket, forwarded).await {
                    break;
                }
            }

            result = event_rx.recv() => {
                match result {
                    Ok(payload) => {
                        if payload.user_id == user_db_id.0 {
                            if let Some(msg) = playback_event_to_message(payload) {
                                if !send(&mut socket, msg).await {
                                    break;
                                }
                            }
                        }
                    }
                    // Missed events can't be replayed; tell the client to
                    // resync state via REST instead.
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::debug!(connection_id, skipped, "broadcast lagged");
                        let msg = OutgoingMessage::Event(EventMessage {
                            event: "sync_required".to_string(),
                            data: serde_json::Value::Null,
                        });
                        if !send(&mut socket, msg).await {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }

            _ = ping_interval.tick() => {
                if !send_raw(&mut socket, Message::Ping(vec![].into())).await {
                    break;
                }
                awaiting_pong = true;
                pong_deadline = Some(Instant::now() + PONG_TIMEOUT);
            }

            _ = auth_interval.tick() => {
                match check_auth(&token).await {
                    AuthStatus::Valid => {
                        consecutive_auth_errors = 0;
                    }
                    AuthStatus::Revoked => {
                        tracing::info!(connection_id, "session revoked, closing connection");
                        let _ = send_raw(&mut socket, Message::Close(None)).await;
                        break;
                    }
                    AuthStatus::Error => {
                        consecutive_auth_errors += 1;
                        tracing::warn!(
                            connection_id,
                            consecutive_auth_errors,
                            "auth check failed, skipping"
                        );
                        if consecutive_auth_errors >= MAX_CONSECUTIVE_AUTH_ERRORS {
                            tracing::warn!(
                                connection_id,
                                consecutive_auth_errors,
                                "auth check failed repeatedly, closing connection"
                            );
                            let _ = send_raw(&mut socket, Message::Close(None)).await;
                            break;
                        }
                    }
                }
            }

            _ = cancel.notified() => {
                tracing::info!(connection_id, "connection evicted by duplicate session_key");
                let _ = send_raw(&mut socket, Message::Close(None)).await;
                break;
            }

            _ = sleep, if awaiting_pong => {
                tracing::debug!(connection_id, "pong timeout, closing connection");
                let _ = send_raw(&mut socket, Message::Close(None)).await;
                break;
            }
        }
    }

    let handle = registry::unregister(connection_id).await;
    if let Some(handle) = &handle {
        tracing::info!(
            connection_id,
            user_db_id = handle.user_db_id.0,
            session_key = %handle.session_key,
            "websocket disconnected"
        );
    }
}
