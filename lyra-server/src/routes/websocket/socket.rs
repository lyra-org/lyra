// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::time::Duration;

use axum::extract::ws::{
    CloseFrame,
    Message,
    Utf8Bytes,
    WebSocket,
};
use tokio::sync::{
    broadcast,
    mpsc,
    watch,
};
use tokio::task::JoinSet;
use tokio::time::{
    Instant,
    interval_at,
};

use crate::services::auth::{
    AuthCredential,
    Principal,
};
use crate::services::playback_sessions::{
    self,
    subscribe_playback_events,
    watch_connection_changes,
};
use crate::services::remote::{
    commands,
    constants::{
        AUTH_CHECK_INTERVAL,
        CloseReason,
        MAX_CONSECUTIVE_AUTH_ERRORS,
        PING_INTERVAL,
        PONG_TIMEOUT,
        WRITE_TIMEOUT,
    },
    events::{
        self,
        ConnectionEvents,
    },
    lifecycle::{
        self,
        AuthStatus,
    },
    messages::OutgoingMessage,
    registry::ConnectionId,
};

async fn send_message(socket: &mut WebSocket, msg: Message) -> bool {
    tokio::time::timeout(WRITE_TIMEOUT, socket.send(msg))
        .await
        .is_ok_and(|r| r.is_ok())
}

pub(super) async fn send_close(socket: &mut WebSocket, reason: CloseReason) -> bool {
    send_message(
        socket,
        Message::Close(Some(CloseFrame {
            code: reason.code(),
            reason: Utf8Bytes::from_static(reason.reason()),
        })),
    )
    .await
}

async fn send(socket: &mut WebSocket, msg: OutgoingMessage) -> bool {
    let text = match serde_json::to_string(&msg) {
        Ok(text) => text,
        Err(_) => return false,
    };
    send_message(socket, Message::Text(text.into())).await
}

/// Reads the user's connections again and sends what changed since the client last heard,
/// returning whether the socket is still writable.
async fn send_connection_changes(
    socket: &mut WebSocket,
    principal: &Principal,
    connection_events: &mut ConnectionEvents,
) -> bool {
    let Some(connections) = events::current_connections(principal).await else {
        return true;
    };
    for msg in connection_events.changes(&connections) {
        if !send(socket, msg).await {
            return false;
        }
    }
    true
}

/// Waits until the first connection the socket knows of degrades, or forever when none will.
async fn sleep_until_degradation(degradation_at_ms: Option<u64>) {
    match degradation_at_ms {
        Some(at_ms) => {
            let now_ms = playback_sessions::now_ms().unwrap_or(at_ms);
            tokio::time::sleep(Duration::from_millis(at_ms.saturating_sub(now_ms))).await;
        }
        None => std::future::pending().await,
    }
}

pub(super) async fn run(
    mut socket: WebSocket,
    connection_id: ConnectionId,
    public_id: String,
    mut principal: Principal,
    mut close: watch::Receiver<Option<CloseReason>>,
    credential: AuthCredential,
    mut command_rx: mpsc::Receiver<OutgoingMessage>,
) {
    let mut ping_interval = interval_at(Instant::now() + PING_INTERVAL, PING_INTERVAL);
    let mut auth_interval = interval_at(Instant::now() + AUTH_CHECK_INTERVAL, AUTH_CHECK_INTERVAL);
    let user_public_id = principal.user_public_id.clone();
    let mut event_rx = subscribe_playback_events(&user_public_id);
    let mut connection_changes = watch_connection_changes(&user_public_id);
    let mut connection_events = ConnectionEvents::default();
    let mut awaiting_pong = false;
    let mut pong_deadline: Option<Instant> = None;
    let mut consecutive_auth_errors: u32 = 0;
    let mut sync_required_pending = false;
    let mut handoff_responses = JoinSet::new();

    'session: {
        let connected = events::current_connections(&principal)
            .await
            .and_then(|connections| connection_events.connected(&public_id, &connections));
        let Some(connected) = connected else {
            tracing::debug!(connection_id, "connection gone before it was announced");
            break 'session;
        };
        if !send(&mut socket, connected).await {
            break 'session;
        }

        loop {
            let sleep = async {
                if let Some(deadline) = pong_deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            };

            let degradation = sleep_until_degradation(connection_events.next_degradation_at_ms());

            tokio::select! {
                msg = socket.recv() => {
                    match msg {
                        Some(Ok(Message::Pong(_))) => {
                            awaiting_pong = false;
                            pong_deadline = None;
                        }
                        Some(Ok(Message::Ping(data))) => {
                            if !send_message(&mut socket, Message::Pong(data)).await {
                                break;
                            }
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            break;
                        }
                        Some(Ok(Message::Text(text))) => {
                            let Some(response) =
                                commands::respond(&text, &mut handoff_responses, connection_id).await
                            else {
                                continue;
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

                Some(result) = handoff_responses.join_next(), if !handoff_responses.is_empty() => {
                    match result {
                        Ok(response) => {
                            if !send(&mut socket, response).await {
                                break;
                            }
                        }
                        Err(error) => {
                            tracing::warn!(connection_id, error = %error, "command task failed");
                        }
                    }
                }

                result = event_rx.recv() => {
                    match result {
                        Ok(payload) => {
                            if let Some(msg) = events::playback_event_message(
                                payload,
                                &user_public_id,
                                &principal.accessible_library_ids,
                            ) {
                                if !send(&mut socket, msg).await {
                                    break;
                                }
                                sync_required_pending = false;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            tracing::debug!(connection_id, skipped, "broadcast lagged");
                            if !events::mark_sync_required_pending(&mut sync_required_pending) {
                                continue;
                            }
                            if !send(&mut socket, events::sync_required_message()).await {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }

                changed = connection_changes.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    if !send_connection_changes(&mut socket, &principal, &mut connection_events).await {
                        break;
                    }
                }

                _ = degradation => {
                    if !send_connection_changes(&mut socket, &principal, &mut connection_events).await {
                        break;
                    }
                }

                _ = ping_interval.tick() => {
                    if !send_message(&mut socket, Message::Ping(vec![].into())).await {
                        break;
                    }
                    awaiting_pong = true;
                    pong_deadline = Some(Instant::now() + PONG_TIMEOUT);
                }

                _ = auth_interval.tick() => {
                    match lifecycle::check_auth(&credential, &user_public_id).await {
                        AuthStatus::Valid(refreshed_library_ids) => {
                            consecutive_auth_errors = 0;
                            principal.accessible_library_ids = refreshed_library_ids;
                        }
                        AuthStatus::Revoked => {
                            tracing::info!(connection_id, "session revoked, closing connection");
                            let _ = send_message(&mut socket, Message::Close(None)).await;
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
                                let _ = send_message(&mut socket, Message::Close(None)).await;
                                break;
                            }
                        }
                    }
                }

                Ok(()) = close.changed() => {
                    let Some(reason) = *close.borrow_and_update() else {
                        continue;
                    };
                    tracing::info!(connection_id, ?reason, "closing connection at server request");
                    let _ = send_close(&mut socket, reason).await;
                    break;
                }

                _ = sleep, if awaiting_pong => {
                    tracing::debug!(connection_id, "pong timeout, closing connection");
                    let _ = send_message(&mut socket, Message::Close(None)).await;
                    break;
                }
            }
        }
    }

    commands::abort_handoff_responses(&mut handoff_responses).await;
    drop(event_rx);
    drop(connection_changes);
    lifecycle::disconnect(connection_id, &user_public_id).await;
}
