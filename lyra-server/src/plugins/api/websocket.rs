// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{
    AtomicBool,
    Ordering,
};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::ws::{
    Message,
    WebSocket,
    WebSocketUpgrade,
};
use axum::http::{
    HeaderMap,
    Method,
    StatusCode,
    Uri,
};
use axum::response::{
    IntoResponse,
    Response,
};
use harmony_core::{
    LuaUserDataAsyncExt,
    cancel_thread,
    run_thread,
};
use harmony_luau::{
    DescribeTypeAlias,
    FunctionParameter,
    LuauType,
    LuauTypeInfo,
    TypeAliasDescriptor,
};
use mlua::{
    Table,
    Value,
};
use tokio::sync::{
    Mutex,
    Notify,
    mpsc,
};
use tokio::time::Instant;

use crate::services::auth::{
    self,
    AuthError,
};
use crate::services::remote::constants::{
    AUTH_CHECK_INTERVAL,
    MAX_MESSAGE_SIZE,
    PING_INTERVAL,
    PONG_TIMEOUT,
    WRITE_TIMEOUT,
};

use super::registry::{
    RegisteredRoute,
    RouteAuthMode,
};
use super::transport::build_context;
use crate::plugins::lifecycle::{
    PluginFunctionHandle,
    PluginId,
};

const WS_CHANNEL_CAPACITY: usize = 32;
const MAX_CONSECUTIVE_AUTH_ERRORS: u32 = 5;
const HANDLER_SHUTDOWN_DEADLINE: Duration = Duration::from_secs(30);

struct SharedState {
    closed: AtomicBool,
    close_signal: Notify,
}

impl SharedState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            closed: AtomicBool::new(false),
            close_signal: Notify::new(),
        })
    }

    fn request_close(&self) {
        self.close_signal.notify_one();
    }

    fn mark_closed(&self) {
        self.closed.store(true, Ordering::Release);
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

/// Concurrent `recv()` calls queue on the inbound `Mutex`; do not store
/// or share this handle outside the owning handler.
#[derive(Clone)]
pub(super) struct ApiWebSocketReader {
    inbound: Arc<Mutex<mpsc::Receiver<String>>>,
    state: Arc<SharedState>,
}

#[harmony_macros::implementation]
impl ApiWebSocketReader {
    pub(crate) async fn recv(&self) -> mlua::Result<Option<String>> {
        let mut rx = self.inbound.lock().await;
        Ok(rx.recv().await)
    }

    pub(crate) fn close(&self) {
        self.state.request_close();
    }
}

harmony_macros::compile!(
    type_path = ApiWebSocketReader,
    fields = false,
    methods = true
);

/// Concurrent senders do not get an intra-call ordering guarantee;
/// plugins mixing broadcast and command dispatch must serialize
/// themselves. `send` returning Ok means the frame was queued, not
/// that it was written — the driver can drop queued frames on
/// teardown.
#[derive(Clone)]
pub(super) struct ApiWebSocketSender {
    outbound: mpsc::Sender<String>,
    state: Arc<SharedState>,
}

#[harmony_macros::implementation]
impl ApiWebSocketSender {
    pub(crate) async fn send(&self, text: String) -> mlua::Result<()> {
        if self.state.is_closed() {
            return Err(mlua::Error::runtime("websocket is closed"));
        }
        self.outbound
            .send(text)
            .await
            .map_err(|_| mlua::Error::runtime("websocket is closed"))?;
        Ok(())
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.state.is_closed()
    }

    pub(crate) fn close(&self) {
        self.state.request_close();
    }
}

harmony_macros::compile!(
    type_path = ApiWebSocketSender,
    fields = false,
    methods = true
);

pub(super) struct ApiWebSocketHandler;

impl LuauTypeInfo for ApiWebSocketHandler {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiWebSocketHandler")
    }
}

impl ApiWebSocketHandler {
    fn handler_type() -> LuauType {
        LuauType::function(
            vec![
                FunctionParameter {
                    name: Some("reader"),
                    ty: LuauType::literal("ApiWebSocketReader"),
                    variadic: false,
                },
                FunctionParameter {
                    name: Some("sender"),
                    ty: LuauType::literal("ApiWebSocketSender"),
                    variadic: false,
                },
                FunctionParameter {
                    name: Some("ctx"),
                    ty: LuauType::literal("ApiContext"),
                    variadic: false,
                },
            ],
            vec![],
        )
    }
}

impl DescribeTypeAlias for ApiWebSocketHandler {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "ApiWebSocketHandler",
            Self::handler_type(),
            Some("The server closes the socket when this function returns."),
        )
    }
}

pub(super) async fn dispatch_websocket_route(
    route: RegisteredRoute,
    uri: Uri,
    headers: HeaderMap,
    query: HashMap<String, String>,
    params: Option<HashMap<String, String>>,
    ws: WebSocketUpgrade,
) -> Response {
    let Some(lua) = route.handler.try_upgrade_lua() else {
        tracing::error!(
            plugin_id = %route.plugin_id,
            path = %route.key.path,
            "plugin websocket handler's lua instance is no longer valid"
        );
        return (StatusCode::SERVICE_UNAVAILABLE, "plugin unavailable").into_response();
    };

    if let Err(reason) = crate::services::origin::validate(&headers) {
        return (StatusCode::FORBIDDEN, reason).into_response();
    }

    let resolved = match auth::resolve_optional_auth(&headers).await {
        Ok(resolved) => resolved,
        Err(_) => None,
    };

    // Api-keys are not revocable mid-connection; only session tokens can
    // be re-validated by the driver's reauth loop.
    if let Some(resolved) = &resolved
        && matches!(resolved.credential, auth::AuthCredential::ApiKey { .. })
    {
        return (
            StatusCode::FORBIDDEN,
            "api key credentials are not accepted on the websocket upgrade",
        )
            .into_response();
    }

    let ctx = match build_context(
        &lua,
        &route,
        &Method::GET,
        &uri,
        &headers,
        &query,
        params.as_ref(),
        &Bytes::new(),
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(err) => {
            tracing::error!(
                plugin_id = %route.plugin_id,
                path = %route.key.path,
                error = %err,
                "failed to construct websocket context"
            );
            return (StatusCode::INTERNAL_SERVER_ERROR, "internal server error").into_response();
        }
    };

    let auth_required = matches!(route.auth_mode, RouteAuthMode::Required);
    if auth_required && matches!(ctx.get::<Value>("auth"), Ok(Value::Nil) | Err(_)) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }

    // Plugins on non-bearer auth are responsible for their own
    // revocation enforcement.
    let reauth_token = if auth_required {
        extract_bearer_token(&headers)
    } else {
        None
    };

    let handler = route.handler.clone();
    let plugin_id = route.plugin_id.clone();
    let path = route.key.path.clone();

    ws.max_message_size(MAX_MESSAGE_SIZE)
        .on_upgrade(move |socket| async move {
            run_plugin_websocket(lua, socket, handler, ctx, plugin_id, path, reauth_token).await;
        })
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    let header = headers.get(axum::http::header::AUTHORIZATION)?;
    let value = header.to_str().ok()?;
    let (scheme, credential) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let credential = credential.trim();
    if credential.is_empty() {
        None
    } else {
        Some(credential.to_string())
    }
}

async fn run_plugin_websocket(
    lua: mlua::Lua,
    socket: WebSocket,
    handler: PluginFunctionHandle,
    ctx: Table,
    plugin_id: PluginId,
    path: Arc<str>,
    reauth_token: Option<String>,
) {
    let (outbound_tx, outbound_rx) = mpsc::channel::<String>(WS_CHANNEL_CAPACITY);
    let (inbound_tx, inbound_rx) = mpsc::channel::<String>(WS_CHANNEL_CAPACITY);
    let state = SharedState::new();

    let reader = ApiWebSocketReader {
        inbound: Arc::new(Mutex::new(inbound_rx)),
        state: state.clone(),
    };
    let sender = ApiWebSocketSender {
        outbound: outbound_tx,
        state: state.clone(),
    };

    let driver_state = state.clone();
    let mut driver = tokio::spawn(run_driver(
        socket,
        inbound_tx,
        outbound_rx,
        driver_state,
        reauth_token,
    ));

    tracing::info!(
        plugin_id = %plugin_id,
        path = %path,
        "plugin websocket opened"
    );

    // Pre-create the thread so the deadline path below can cancel scheduler
    // work and force-close state on timeout. Cloning the inner Function keeps
    // `handler` (the `PluginFunctionHandle`) alive for the whole websocket,
    // so the in-flight counter stays incremented until this scope drops it.
    let thread = match lua.create_thread(handler.inner_function().clone()) {
        Ok(t) => t,
        Err(err) => {
            tracing::error!(
                plugin_id = %plugin_id,
                path = %path,
                error = %err,
                "failed to create plugin websocket handler thread"
            );
            state.request_close();
            let _ = driver.await;
            return;
        }
    };

    // The deadline arms only after the driver exits: the handler then has
    // `HANDLER_SHUTDOWN_DEADLINE` to observe the close signal and return on
    // its own before we force-close the thread.
    let mut handler_fut = std::pin::pin!(run_thread::<Value>(
        &lua,
        thread.clone(),
        (reader, sender, ctx)
    ));
    let mut driver_done = false;
    let mut deadline: Option<Instant> = None;

    let outcome = loop {
        let wait_deadline = async {
            match deadline {
                Some(d) => tokio::time::sleep_until(d).await,
                None => std::future::pending::<()>().await,
            }
        };

        tokio::select! {
            res = &mut handler_fut => break HandlerOutcome::Completed(res),
            _ = &mut driver, if !driver_done => {
                driver_done = true;
                state.request_close();
                deadline = Some(Instant::now() + HANDLER_SHUTDOWN_DEADLINE);
            }
            _ = wait_deadline, if deadline.is_some() => break HandlerOutcome::TimedOut,
        }
    };

    if !driver_done {
        state.request_close();
        let _ = driver.await;
    }

    match outcome {
        HandlerOutcome::Completed(Ok(_)) => {
            tracing::info!(
                plugin_id = %plugin_id,
                path = %path,
                "plugin websocket closed"
            );
        }
        HandlerOutcome::Completed(Err(err)) => {
            tracing::error!(
                plugin_id = %plugin_id,
                path = %path,
                error = %err,
                "plugin websocket handler failed"
            );
        }
        HandlerOutcome::TimedOut => {
            let _ = cancel_thread(&lua, &thread);
            if let Err(err) = thread.close() {
                tracing::warn!(
                    plugin_id = %plugin_id,
                    path = %path,
                    error = %err,
                    "failed to close wedged plugin websocket thread"
                );
            }
            tracing::warn!(
                plugin_id = %plugin_id,
                path = %path,
                timeout_seconds = HANDLER_SHUTDOWN_DEADLINE.as_secs(),
                "plugin websocket handler exceeded shutdown deadline; thread closed"
            );
        }
    }
}

enum HandlerOutcome {
    Completed(mlua::Result<Value>),
    TimedOut,
}

enum ReauthStatus {
    Valid,
    Revoked,
    Error,
}

async fn check_auth(token: &Option<String>) -> ReauthStatus {
    match auth::resolve_auth_from_bearer(token.as_deref()).await {
        Ok(Some(_)) => ReauthStatus::Valid,
        Ok(None) => ReauthStatus::Revoked,
        Err(AuthError::SessionExpired) => ReauthStatus::Revoked,
        Err(_) => ReauthStatus::Error,
    }
}

async fn send_with_timeout(socket: &mut WebSocket, msg: Message) -> bool {
    tokio::time::timeout(WRITE_TIMEOUT, socket.send(msg))
        .await
        .is_ok_and(|r| r.is_ok())
}

async fn run_driver(
    mut socket: WebSocket,
    inbound_tx: mpsc::Sender<String>,
    mut outbound_rx: mpsc::Receiver<String>,
    state: Arc<SharedState>,
    reauth_token: Option<String>,
) {
    let mut ping_interval = tokio::time::interval_at(Instant::now() + PING_INTERVAL, PING_INTERVAL);
    let mut auth_interval =
        tokio::time::interval_at(Instant::now() + AUTH_CHECK_INTERVAL, AUTH_CHECK_INTERVAL);
    let mut awaiting_pong = false;
    let mut pong_deadline: Option<Instant> = None;
    let mut consecutive_auth_errors: u32 = 0;
    let reauth_enabled = reauth_token.is_some();

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
                        // Non-blocking so the select! never stalls here.
                        match inbound_tx.try_send(text.to_string()) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                tracing::warn!(
                                    "plugin websocket inbound queue full; dropping incoming frame"
                                );
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => break,
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        if !send_with_timeout(&mut socket, Message::Pong(data)).await {
                            break;
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {
                        awaiting_pong = false;
                        pong_deadline = None;
                    }
                    Some(Ok(Message::Binary(_))) => {
                        tracing::debug!("dropping binary frame on plugin websocket");
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {
                        break;
                    }
                }
            }
            Some(out) = outbound_rx.recv() => {
                if !send_with_timeout(&mut socket, Message::Text(out.into())).await {
                    break;
                }
            }
            _ = ping_interval.tick() => {
                if !send_with_timeout(&mut socket, Message::Ping(Vec::new().into())).await {
                    break;
                }
                awaiting_pong = true;
                pong_deadline = Some(Instant::now() + PONG_TIMEOUT);
            }
            _ = auth_interval.tick(), if reauth_enabled => {
                match check_auth(&reauth_token).await {
                    ReauthStatus::Valid => {
                        consecutive_auth_errors = 0;
                    }
                    ReauthStatus::Revoked => {
                        tracing::info!("plugin websocket session revoked, closing");
                        break;
                    }
                    ReauthStatus::Error => {
                        consecutive_auth_errors += 1;
                        tracing::warn!(
                            consecutive_auth_errors,
                            "plugin websocket auth check failed, skipping"
                        );
                        if consecutive_auth_errors >= MAX_CONSECUTIVE_AUTH_ERRORS {
                            tracing::warn!("plugin websocket auth checks failed repeatedly, closing");
                            break;
                        }
                    }
                }
            }
            _ = pong_wait, if awaiting_pong => {
                tracing::debug!("plugin websocket pong timeout, closing");
                break;
            }
            _ = state.close_signal.notified() => {
                break;
            }
        }
    }

    // Flag before drain so in-flight `send()` short-circuits instead of
    // racing the close-frame write.
    state.mark_closed();
    drop(inbound_tx);
    let _ = send_with_timeout(&mut socket, Message::Close(None)).await;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::http::HeaderMap;
    use tokio::sync::{
        Mutex,
        mpsc,
    };

    use super::{
        ApiWebSocketSender,
        SharedState,
        extract_bearer_token,
    };

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                axum::http::HeaderValue::from_str(value).unwrap(),
            );
        }
        map
    }

    #[test]
    fn extract_bearer_token_accepts_standard_header() {
        let map = headers(&[("Authorization", "Bearer abc123")]);
        assert_eq!(extract_bearer_token(&map).as_deref(), Some("abc123"));
    }

    #[test]
    fn extract_bearer_token_rejects_non_bearer_schemes() {
        let map = headers(&[("Authorization", "Basic YWxpY2U6c2VjcmV0")]);
        assert!(extract_bearer_token(&map).is_none());
    }

    #[test]
    fn extract_bearer_token_is_case_insensitive_on_scheme() {
        let map = headers(&[("Authorization", "bearer token")]);
        assert_eq!(extract_bearer_token(&map).as_deref(), Some("token"));
    }

    #[test]
    fn extract_bearer_token_returns_none_when_absent() {
        let map = headers(&[]);
        assert!(extract_bearer_token(&map).is_none());
    }

    #[tokio::test]
    async fn sender_is_closed_flips_after_mark_closed() {
        let state = SharedState::new();
        let (tx, _rx) = mpsc::channel::<String>(1);
        let sender = ApiWebSocketSender {
            outbound: tx,
            state: state.clone(),
        };
        assert!(!state.is_closed());
        state.mark_closed();
        assert!(state.is_closed());
        assert!(sender.state.is_closed());
    }

    #[tokio::test]
    async fn sender_close_notifies_shared_state() {
        let state = SharedState::new();
        let (tx, _rx) = mpsc::channel::<String>(1);
        let sender = ApiWebSocketSender {
            outbound: tx,
            state: state.clone(),
        };

        // notify_one stores a permit, so notified() resolves even though
        // it's awaited after request_close().
        sender.state.request_close();
        state.close_signal.notified().await;
    }

    #[tokio::test]
    async fn reader_recv_returns_none_when_inbound_dropped() {
        let (tx, rx) = mpsc::channel::<String>(1);
        let reader_inbound = Arc::new(Mutex::new(rx));
        drop(tx);

        let mut rx = reader_inbound.lock().await;
        assert!(rx.recv().await.is_none());
    }
}
