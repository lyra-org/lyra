// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::*;

pub(super) async fn dispatch_websocket_route(
    route: RegisteredRoute,
    uri: Uri,
    headers: HeaderMap,
    query: HashMap<String, Vec<String>>,
    params: Option<HashMap<String, String>>,
    ws: WebSocketUpgrade,
) -> Response {
    if let Err(reason) = crate::services::origin::validate(&headers) {
        return (StatusCode::FORBIDDEN, reason).into_response();
    }

    let auth = match resolve_optional_auth(&headers).await {
        Ok(auth) => auth,
        Err(err) => {
            tracing::warn!(
                plugin_id = %route.plugin_id,
                path = %route.key.path,
                error = %err,
                "failed to resolve auth for plugin websocket"
            );
            None
        }
    };
    if matches!(route.auth_mode, RouteAuthMode::Required) && auth.is_none() {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }

    let Some(crate::plugins::bootstrap::PluginRuntime::Executor(runtime)) =
        crate::STATE.plugin_runtime.get()
    else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "plugin runtime is not ready",
        )
            .into_response();
    };

    ws.max_message_size(MAX_MESSAGE_SIZE)
        .on_upgrade(move |socket| async move {
            run_plugin_websocket(socket, runtime, route, uri, headers, query, params, auth).await;
        })
}

pub(super) async fn run_plugin_websocket(
    socket: WebSocket,
    runtime: crate::plugins::executor::PluginExecutorHandle,
    route: RegisteredRoute,
    uri: Uri,
    headers: HeaderMap,
    query: HashMap<String, Vec<String>>,
    params: Option<HashMap<String, String>>,
    auth: Option<crate::services::auth::ResolvedAuth>,
) {
    let (outbound_tx, outbound_rx) = tokio::sync::mpsc::channel::<String>(32);
    let (inbound_tx, inbound_rx) = tokio::sync::mpsc::channel::<String>(32);
    let state = crate::plugins::executor::WebSocketState::new();
    let request = crate::plugins::executor::WebSocketStartRequest {
        handler_id: route.handler_id,
        plugin_id: route.plugin_id.to_string(),
        method: "GET".to_string(),
        path: uri.path().to_string(),
        headers: header_pairs(&headers),
        query,
        params: params.unwrap_or_default(),
        auth,
        inbound: Arc::new(tokio::sync::Mutex::new(inbound_rx)),
        outbound: outbound_tx,
        state: state.clone(),
    };

    let start = tokio::task::spawn_blocking(move || runtime.start_websocket(request)).await;
    match start {
        Ok(Ok(())) => {
            run_plugin_websocket_driver(socket, inbound_tx, outbound_rx, state).await;
        }
        Ok(Err(err)) => {
            tracing::warn!(
                plugin_id = %route.plugin_id,
                path = %route.key.path,
                error = %err,
                "failed to start plugin websocket"
            );
        }
        Err(err) => {
            tracing::warn!(
                plugin_id = %route.plugin_id,
                path = %route.key.path,
                error = %err,
                "plugin websocket start task failed"
            );
        }
    }
}

async fn send_ws_with_timeout(socket: &mut WebSocket, msg: Message) -> bool {
    tokio::time::timeout(WRITE_TIMEOUT, socket.send(msg))
        .await
        .is_ok_and(|result| result.is_ok())
}

async fn run_plugin_websocket_driver(
    mut socket: WebSocket,
    inbound_tx: tokio::sync::mpsc::Sender<String>,
    mut outbound_rx: tokio::sync::mpsc::Receiver<String>,
    state: Arc<crate::plugins::executor::WebSocketState>,
) {
    let mut ping_interval =
        tokio::time::interval_at(tokio::time::Instant::now() + PING_INTERVAL, PING_INTERVAL);
    let mut awaiting_pong = false;
    let mut pong_deadline: Option<tokio::time::Instant> = None;

    loop {
        let pong_wait = async {
            if let Some(deadline) = pong_deadline {
                tokio::time::sleep_until(deadline).await;
            } else {
                std::future::pending::<()>().await;
            }
        };

        tokio::select! {
            maybe_msg = socket.recv() => {
                match maybe_msg {
                    Some(Ok(Message::Text(text))) => {
                        match inbound_tx.try_send(text.to_string()) {
                            Ok(()) => {}
                            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                tracing::warn!("plugin websocket inbound queue full; dropping incoming frame");
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        if !send_ws_with_timeout(&mut socket, Message::Pong(data)).await {
                            break;
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {
                        awaiting_pong = false;
                        pong_deadline = None;
                    }
                    Some(Ok(Message::Binary(_))) => {}
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                }
            }
            Some(out) = outbound_rx.recv() => {
                if !send_ws_with_timeout(&mut socket, Message::Text(out.into())).await {
                    break;
                }
            }
            _ = ping_interval.tick() => {
                if !send_ws_with_timeout(&mut socket, Message::Ping(Vec::new().into())).await {
                    break;
                }
                awaiting_pong = true;
                pong_deadline = Some(tokio::time::Instant::now() + PONG_TIMEOUT);
            }
            _ = pong_wait, if awaiting_pong => break,
            _ = state.closed() => break,
        }
    }

    state.mark_closed();
    drop(inbound_tx);
    let _ = send_ws_with_timeout(&mut socket, Message::Close(None)).await;
}
