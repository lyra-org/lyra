// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashSet;

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

use crate::services::playback_sessions::subscribe_playback_events;
use crate::services::remote::{
    connection::{
        self,
        AuthStatus,
    },
    constants::{
        AUTH_CHECK_INTERVAL,
        CloseReason,
        MAX_CONSECUTIVE_AUTH_ERRORS,
        PING_INTERVAL,
        PONG_TIMEOUT,
        WRITE_TIMEOUT,
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

pub(super) async fn run(
    mut socket: WebSocket,
    connection_id: ConnectionId,
    user_public_id: String,
    mut accessible_library_ids: HashSet<String>,
    mut close: watch::Receiver<Option<CloseReason>>,
    token: Option<String>,
    mut command_rx: mpsc::Receiver<OutgoingMessage>,
) {
    let mut ping_interval = interval_at(Instant::now() + PING_INTERVAL, PING_INTERVAL);
    let mut auth_interval = interval_at(Instant::now() + AUTH_CHECK_INTERVAL, AUTH_CHECK_INTERVAL);
    let mut event_rx = subscribe_playback_events(&user_public_id);
    let mut awaiting_pong = false;
    let mut pong_deadline: Option<Instant> = None;
    let mut consecutive_auth_errors: u32 = 0;
    let mut sync_required_pending = false;
    let mut handoff_responses = JoinSet::new();

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
                        if !send_message(&mut socket, Message::Pong(data)).await {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        break;
                    }
                    Some(Ok(Message::Text(text))) => {
                        let Some(response) =
                            connection::respond(&text, &mut handoff_responses, connection_id).await
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
                        if let Some(msg) = connection::playback_event_message(
                            payload,
                            &user_public_id,
                            &accessible_library_ids,
                        ) {
                            if !send(&mut socket, msg).await {
                                break;
                            }
                            sync_required_pending = false;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::debug!(connection_id, skipped, "broadcast lagged");
                        if !connection::mark_sync_required_pending(&mut sync_required_pending) {
                            continue;
                        }
                        if !send(&mut socket, connection::sync_required_message()).await {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
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
                match connection::check_auth(&token, &user_public_id).await {
                    AuthStatus::Valid(refreshed_library_ids) => {
                        consecutive_auth_errors = 0;
                        accessible_library_ids = refreshed_library_ids;
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

    connection::abort_handoff_responses(&mut handoff_responses).await;
    drop(event_rx);
    connection::disconnect(connection_id, &user_public_id).await;
}
