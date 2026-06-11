// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use anyhow::Result;
use harmony_core::LocalScheduler;
use harmony_luau as luau;

use super::{
    PluginExecutor,
    api::api_context_value,
    messages::{
        ApiHandlerRequest,
        TaskIdKey,
        WebSocketStartRequest,
        WebSocketState,
    },
};

impl PluginExecutor {
    pub(crate) fn start_websocket(&self, request: WebSocketStartRequest) -> Result<()> {
        let routes = self.vm.data().get::<crate::plugins::api::ApiRouteStore>()?;
        let handler = routes
            .get(request.handler_id)
            .ok_or_else(|| anyhow::anyhow!("websocket handler {} not found", request.handler_id))?;
        let origin = super::luau_origin(&handler.context.origin);
        let reader = ApiWebSocketReader::_harmony_userdata_class().create_value(
            &self.vm,
            &origin,
            ApiWebSocketReader {
                inbound: request.inbound,
                state: request.state.clone(),
            },
        )?;
        let sender = ApiWebSocketSender::_harmony_userdata_class().create_value(
            &self.vm,
            &origin,
            ApiWebSocketSender {
                outbound: request.outbound,
                state: request.state.clone(),
            },
        )?;
        let auth_principal = request.auth.as_ref().map(|auth| auth.principal.clone());
        let ctx = api_context_value(&ApiHandlerRequest {
            handler_id: request.handler_id,
            plugin_id: request.plugin_id,
            method: request.method,
            path: request.path,
            headers: request.headers,
            query: request.query,
            params: request.params,
            body: Vec::new(),
            auth: request.auth,
            client_key: None,
        })?;
        let thread = self.vm.create_thread(&handler.handler)?;
        let mut context = handler.context.clone();
        let dispatch_auth = crate::plugins::auth::DispatchAuth::default();
        if let Some(principal) = auth_principal {
            dispatch_auth.record(principal);
        }
        context.caller.insert(dispatch_auth);
        let scheduler = self.vm.data().get::<LocalScheduler>()?;
        let handle = scheduler.spawn_luau_thread(
            context,
            self.vm.clone(),
            thread,
            vec![reader, sender, ctx],
        );
        self.websocket_tasks
            .borrow_mut()
            .insert(TaskIdKey(handle.id().0), request.state);
        Ok(())
    }
}

#[harmony_macros::userdata(name = "ApiWebSocketReader")]
#[derive(Clone)]
struct ApiWebSocketReader {
    inbound: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<String>>>,
    state: Arc<WebSocketState>,
}

#[harmony_macros::userdata_methods]
impl ApiWebSocketReader {
    #[harmony(
        description = "Receives the next websocket text frame. Nil means the websocket is closed.",
        returns(Option<String>)
    )]
    fn recv(&self) -> luau::runtime::Result<luau::ScheduledFuture> {
        let inbound = self.inbound.clone();
        Ok(luau::ScheduledFuture::new(async move {
            let mut rx = inbound.lock().await;
            Ok(rx
                .recv()
                .await
                .map(|text| luau::Value::String(text.into_bytes()))
                .unwrap_or(luau::Value::Nil))
        }))
    }

    #[harmony(description = "Requests that the websocket be closed.")]
    fn close(&self) {
        self.state.request_close();
    }
}

#[harmony_macros::userdata(name = "ApiWebSocketSender")]
#[derive(Clone)]
struct ApiWebSocketSender {
    outbound: tokio::sync::mpsc::Sender<String>,
    state: Arc<WebSocketState>,
}

#[harmony_macros::userdata_methods]
impl ApiWebSocketSender {
    #[harmony(description = "Sends a websocket text frame.")]
    fn send(&self, text: String) -> luau::runtime::Result<luau::ScheduledFuture> {
        if self.state.is_closed() {
            return Err(luau::Error::Runtime("websocket is closed".to_string()));
        }
        let outbound = self.outbound.clone();
        Ok(luau::ScheduledFuture::new(async move {
            outbound
                .send(text)
                .await
                .map_err(|_| luau::Error::Runtime("websocket is closed".to_string()))?;
            Ok(())
        }))
    }

    #[harmony(description = "Returns whether the websocket has been closed.")]
    fn is_closed(&self) -> bool {
        self.state.is_closed()
    }

    #[harmony(description = "Requests that the websocket be closed.")]
    fn close(&self) {
        self.state.request_close();
    }
}
