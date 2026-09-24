// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::*;

pub(super) struct WebSocketRouteRequest {
    pub(super) route: RegisteredRoute,
    pub(super) uri: Uri,
    pub(super) headers: HeaderMap,
    pub(super) query: HashMap<String, Vec<String>>,
    pub(super) params: Option<HashMap<String, String>>,
}

struct PluginWebSocketContext {
    runtime: crate::plugins::executor::PluginExecutorHandle,
    route: RegisteredRoute,
    uri: Uri,
    headers: HeaderMap,
    query: HashMap<String, Vec<String>>,
    params: Option<HashMap<String, String>>,
    auth: Option<crate::services::auth::ResolvedAuth>,
}

/// Re-resolves an auth-required socket's connect-time credential, keeping the
/// socket's principal slot current and closing the socket once the credential
/// no longer resolves to the same user.
struct SocketReauth {
    headers: HeaderMap,
    user_public_id: String,
    dispatch_auth: crate::plugins::auth::DispatchAuth,
    consecutive_errors: u32,
}

impl SocketReauth {
    /// Returns whether the socket may stay open.
    async fn check(&mut self) -> bool {
        match resolve_optional_auth(&self.headers).await {
            Ok(Some(auth)) if auth.principal.user_public_id == self.user_public_id => {
                self.consecutive_errors = 0;
                self.dispatch_auth.refresh(auth.principal);
                true
            }
            Ok(_) => {
                tracing::info!(
                    user_public_id = %self.user_public_id,
                    "plugin websocket credential no longer valid, closing"
                );
                false
            }
            Err(err) => {
                self.consecutive_errors += 1;
                tracing::warn!(
                    user_public_id = %self.user_public_id,
                    consecutive_errors = self.consecutive_errors,
                    error = %err,
                    "plugin websocket auth check failed"
                );
                self.consecutive_errors < MAX_CONSECUTIVE_AUTH_ERRORS
            }
        }
    }
}

/// Only auth-required routes act as the connect-time principal; public routes
/// start with an empty slot and must resolve a credential to act.
fn socket_auth(
    auth_mode: &RouteAuthMode,
    auth: Option<&crate::services::auth::ResolvedAuth>,
    headers: &HeaderMap,
) -> (crate::plugins::auth::DispatchAuth, Option<SocketReauth>) {
    let dispatch_auth = crate::plugins::auth::DispatchAuth::default();
    let reauth = match (auth_mode, auth) {
        (RouteAuthMode::Required, Some(auth)) => {
            dispatch_auth.record(auth.principal.clone());
            Some(SocketReauth {
                headers: headers.clone(),
                user_public_id: auth.principal.user_public_id.clone(),
                dispatch_auth: dispatch_auth.clone(),
                consecutive_errors: 0,
            })
        }
        _ => None,
    };
    (dispatch_auth, reauth)
}

pub(super) async fn dispatch_websocket_route(
    request: WebSocketRouteRequest,
    ws: WebSocketUpgrade,
) -> Response {
    let WebSocketRouteRequest {
        route,
        uri,
        headers,
        query,
        params,
    } = request;

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

    let Some(runtime) = crate::STATE.generation().plugin_runtime.get() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "plugin runtime is not ready",
        )
            .into_response();
    };

    ws.max_message_size(MAX_MESSAGE_SIZE)
        .on_upgrade(move |socket| async move {
            run_plugin_websocket(
                socket,
                PluginWebSocketContext {
                    runtime,
                    route,
                    uri,
                    headers,
                    query,
                    params,
                    auth,
                },
            )
            .await;
        })
}

async fn run_plugin_websocket(socket: WebSocket, context: PluginWebSocketContext) {
    let PluginWebSocketContext {
        runtime,
        route,
        uri,
        headers,
        query,
        params,
        auth,
    } = context;

    let (outbound_tx, outbound_rx) = tokio::sync::mpsc::channel::<String>(32);
    let (inbound_tx, inbound_rx) = tokio::sync::mpsc::channel::<String>(32);
    let state = crate::plugins::executor::WebSocketState::new();
    let (dispatch_auth, reauth) = socket_auth(&route.auth_mode, auth.as_ref(), &headers);
    let request = crate::plugins::executor::WebSocketStartRequest {
        handler_id: route.handler_id,
        plugin_id: route.plugin_id.to_string(),
        method: "GET".to_string(),
        path: uri.path().to_string(),
        headers: header_pairs(&headers),
        query,
        params: params.unwrap_or_default(),
        auth,
        dispatch_auth: dispatch_auth.clone(),
        inbound: Arc::new(tokio::sync::Mutex::new(inbound_rx)),
        outbound: outbound_tx,
        state: state.clone(),
    };

    match runtime.start_websocket(request).await {
        Ok(()) => {
            run_plugin_websocket_driver(
                socket,
                PluginWebSocketDriver {
                    inbound_tx,
                    outbound_rx,
                    state,
                    dispatch_auth,
                    reauth,
                    auth_check_interval: AUTH_CHECK_INTERVAL,
                },
            )
            .await;
        }
        Err(err) => {
            tracing::warn!(
                plugin_id = %route.plugin_id,
                path = %route.key.path,
                error = %err,
                "failed to start plugin websocket"
            );
        }
    }
}

async fn send_ws_with_timeout<S>(socket: &mut S, msg: Message) -> bool
where
    S: futures::Sink<Message> + Unpin,
{
    tokio::time::timeout(WRITE_TIMEOUT, futures::SinkExt::send(socket, msg))
        .await
        .is_ok_and(|result| result.is_ok())
}

struct PluginWebSocketDriver {
    inbound_tx: tokio::sync::mpsc::Sender<String>,
    outbound_rx: tokio::sync::mpsc::Receiver<String>,
    state: Arc<crate::plugins::executor::WebSocketState>,
    dispatch_auth: crate::plugins::auth::DispatchAuth,
    reauth: Option<SocketReauth>,
    auth_check_interval: std::time::Duration,
}

async fn run_plugin_websocket_driver<S>(mut socket: S, driver: PluginWebSocketDriver)
where
    S: futures::Stream<Item = Result<Message, axum::Error>> + futures::Sink<Message> + Unpin,
{
    let PluginWebSocketDriver {
        inbound_tx,
        mut outbound_rx,
        state,
        dispatch_auth,
        mut reauth,
        auth_check_interval,
    } = driver;
    let mut ping_interval =
        tokio::time::interval_at(tokio::time::Instant::now() + PING_INTERVAL, PING_INTERVAL);
    let mut auth_interval = tokio::time::interval_at(
        tokio::time::Instant::now() + auth_check_interval,
        auth_check_interval,
    );
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
            maybe_msg = futures::StreamExt::next(&mut socket) => {
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
            _ = auth_interval.tick(), if reauth.is_some() => {
                if let Some(reauth) = reauth.as_mut()
                    && !reauth.check().await
                {
                    break;
                }
            }
            _ = pong_wait, if awaiting_pong => break,
            _ = state.closed() => break,
        }
    }

    dispatch_auth.clear();
    state.mark_closed();
    drop(inbound_tx);
    let _ = send_ws_with_timeout(&mut socket, Message::Close(None)).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        STATE,
        db,
    };

    async fn socket_reauth(bearer: &str) -> anyhow::Result<SocketReauth> {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {bearer}").parse()?,
        );
        let auth = resolve_optional_auth(&headers)
            .await?
            .ok_or_else(|| anyhow::anyhow!("credential should resolve"))?;
        let dispatch_auth = crate::plugins::auth::DispatchAuth::default();
        dispatch_auth.record(auth.principal.clone());
        Ok(SocketReauth {
            headers,
            user_public_id: auth.principal.user_public_id,
            dispatch_auth,
            consecutive_errors: 0,
        })
    }

    async fn session_user(username: &str) -> anyhow::Result<(agdb::DbId, String)> {
        let user_db_id = {
            let mut db = STATE.db.write().await;
            db::test_db::insert_user(&mut db, username)?
        };
        let session = crate::testing::create_session(user_db_id, Default::default()).await?;
        Ok((user_db_id, session.token))
    }

    #[tokio::test]
    async fn reauth_closes_on_revoked_session() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        crate::testing::init_default_test_state()?;
        let (_, token) = session_user("socket-revoked").await?;
        let mut reauth = socket_reauth(&token).await?;

        assert!(reauth.check().await);
        assert!(crate::services::auth::sessions::revoke_session_by_token(&token).await?);
        assert!(!reauth.check().await);
        Ok(())
    }

    #[tokio::test]
    async fn reauth_closes_on_revoked_api_key() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        crate::testing::init_default_test_state()?;
        let user_db_id = {
            let mut db = STATE.db.write().await;
            db::test_db::insert_user(&mut db, "socket-key-owner")?
        };
        let principal = crate::testing::user_principal(user_db_id).await?;
        let key = crate::services::auth::api_keys::create_api_key(&principal, "socket")
            .await
            .map_err(|err| anyhow::anyhow!("{err}"))?;
        let mut reauth = socket_reauth(&key.key).await?;

        assert!(reauth.check().await);
        assert!(
            crate::services::auth::api_keys::revoke_api_key_for_user(&principal, &key.id).await?
        );
        assert!(!reauth.check().await);
        Ok(())
    }

    #[tokio::test]
    async fn reauth_refreshes_the_socket_principal() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        crate::testing::init_default_test_state()?;
        let (user_db_id, token) = session_user("socket-promoted").await?;
        let mut reauth = socket_reauth(&token).await?;
        let before = reauth
            .dispatch_auth
            .principal()
            .ok_or_else(|| anyhow::anyhow!("slot should be seeded"))?;
        assert_ne!(
            before.role_name.as_deref(),
            Some(db::roles::BUILTIN_ADMIN_ROLE)
        );

        {
            let mut db = STATE.db.write().await;
            db::roles::ensure_builtin_roles(&mut db)?;
            db::roles::ensure_user_has_role(&mut db, user_db_id, db::roles::BUILTIN_ADMIN_ROLE)?;
        }
        assert!(reauth.check().await);

        let after = reauth
            .dispatch_auth
            .principal()
            .ok_or_else(|| anyhow::anyhow!("slot should stay seeded"))?;
        assert_eq!(
            after.role_name.as_deref(),
            Some(db::roles::BUILTIN_ADMIN_ROLE)
        );
        assert!(after.permissions.len() > before.permissions.len());
        Ok(())
    }

    #[tokio::test]
    async fn reauth_closes_on_deleted_user() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        crate::testing::init_default_test_state()?;
        let (user_db_id, token) = session_user("socket-deleted").await?;
        let mut reauth = socket_reauth(&token).await?;

        {
            let mut db = STATE.db.write().await;
            db::users::delete_user(&mut db, user_db_id)?;
        }
        assert!(!reauth.check().await);
        Ok(())
    }

    #[tokio::test]
    async fn reauth_closes_when_the_credential_resolves_to_another_user() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        crate::testing::init_default_test_state()?;
        {
            let mut db = STATE.db.write().await;
            db::users::ensure_default_user(&mut db)?;
        }
        let (_, token) = session_user("socket-collapsed").await?;
        let mut reauth = socket_reauth(&token).await?;

        let mut config = STATE.config().as_ref().clone();
        config.auth.enabled = false;
        crate::testing::publish_config(config);

        assert!(!reauth.check().await);
        Ok(())
    }

    #[tokio::test]
    async fn reauth_leaves_a_plugin_resolved_principal_alone() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        crate::testing::init_default_test_state()?;
        let (_, token) = session_user("socket-original").await?;
        let (other_db_id, _) = session_user("socket-plugin-resolved").await?;
        let mut reauth = socket_reauth(&token).await?;
        let other = crate::testing::user_principal(other_db_id).await?;
        reauth.dispatch_auth.record(other.clone());

        assert!(reauth.check().await);
        let acting = reauth
            .dispatch_auth
            .principal()
            .ok_or_else(|| anyhow::anyhow!("slot should stay seeded"))?;
        assert_eq!(acting.user_public_id, other.user_public_id);
        Ok(())
    }

    struct TestSocket {
        incoming: tokio::sync::mpsc::UnboundedReceiver<Result<Message, axum::Error>>,
        outgoing: tokio::sync::mpsc::UnboundedSender<Message>,
    }

    impl futures::Stream for TestSocket {
        type Item = Result<Message, axum::Error>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            self.incoming.poll_recv(cx)
        }
    }

    impl futures::Sink<Message> for TestSocket {
        type Error = ();

        fn poll_ready(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), ()>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn start_send(self: std::pin::Pin<&mut Self>, item: Message) -> Result<(), ()> {
            self.outgoing.send(item).map_err(|_| ())
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), ()>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), ()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    struct DriverHarness {
        client_tx: tokio::sync::mpsc::UnboundedSender<Result<Message, axum::Error>>,
        client_rx: tokio::sync::mpsc::UnboundedReceiver<Message>,
        driver: tokio::task::JoinHandle<()>,
    }

    fn spawn_driver(
        dispatch_auth: crate::plugins::auth::DispatchAuth,
        reauth: Option<SocketReauth>,
    ) -> DriverHarness {
        let (client_tx, incoming) = tokio::sync::mpsc::unbounded_channel();
        let (outgoing, client_rx) = tokio::sync::mpsc::unbounded_channel();
        let (inbound_tx, _inbound_rx) = tokio::sync::mpsc::channel(4);
        let (_outbound_tx, outbound_rx) = tokio::sync::mpsc::channel(4);
        let driver = tokio::spawn(run_plugin_websocket_driver(
            TestSocket { incoming, outgoing },
            PluginWebSocketDriver {
                inbound_tx,
                outbound_rx,
                state: crate::plugins::executor::WebSocketState::new(),
                dispatch_auth,
                reauth,
                auth_check_interval: std::time::Duration::from_millis(10),
            },
        ));
        DriverHarness {
            client_tx,
            client_rx,
            driver,
        }
    }

    #[tokio::test]
    async fn driver_closes_and_clears_the_slot_on_revocation() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        crate::testing::init_default_test_state()?;
        let (_, token) = session_user("socket-driver-revoked").await?;
        let reauth = socket_reauth(&token).await?;
        let dispatch_auth = reauth.dispatch_auth.clone();
        let mut harness = spawn_driver(dispatch_auth.clone(), Some(reauth));

        assert!(crate::services::auth::sessions::revoke_session_by_token(&token).await?);
        tokio::time::timeout(std::time::Duration::from_secs(5), &mut harness.driver).await??;

        assert!(dispatch_auth.principal().is_none());
        assert!(matches!(
            harness.client_rx.try_recv(),
            Ok(Message::Close(None))
        ));
        drop(harness.client_tx);
        Ok(())
    }

    #[tokio::test]
    async fn driver_clears_the_slot_on_client_disconnect() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        crate::testing::init_default_test_state()?;
        let (user_db_id, _) = session_user("socket-driver-disconnect").await?;
        let dispatch_auth = crate::plugins::auth::DispatchAuth::default();
        dispatch_auth.record(crate::testing::user_principal(user_db_id).await?);
        let harness = spawn_driver(dispatch_auth.clone(), None);

        drop(harness.client_tx);
        tokio::time::timeout(std::time::Duration::from_secs(5), harness.driver).await??;

        assert!(dispatch_auth.principal().is_none());
        Ok(())
    }

    #[tokio::test]
    async fn only_auth_required_sockets_act_as_the_connect_time_principal() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        crate::testing::init_default_test_state()?;
        let (_, token) = session_user("socket-seeded").await?;
        let seeded = socket_reauth(&token).await?;
        let auth = resolve_optional_auth(&seeded.headers)
            .await?
            .ok_or_else(|| anyhow::anyhow!("credential should resolve"))?;

        let (public_slot, public_reauth) =
            socket_auth(&RouteAuthMode::Public, Some(&auth), &seeded.headers);
        assert!(public_slot.principal().is_none());
        assert!(public_reauth.is_none());

        let (required_slot, required_reauth) =
            socket_auth(&RouteAuthMode::Required, Some(&auth), &seeded.headers);
        assert_eq!(
            required_slot
                .principal()
                .map(|principal| principal.user_public_id),
            Some(auth.principal.user_public_id)
        );
        assert!(required_reauth.is_some());
        Ok(())
    }
}
