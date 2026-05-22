// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use anyhow::Result;
use harmony_core::{
    CallContext,
    LocalScheduler,
};
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
        let reader =
            websocket_reader_value(&handler.context, request.inbound, request.state.clone());
        let sender =
            websocket_sender_value(&handler.context, request.outbound, request.state.clone());
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
        })?;
        let thread = self.vm.create_thread(&handler.handler)?;
        let mut context = handler.context.clone();
        if let Some(principal) = auth_principal {
            context.caller.insert(principal);
        }
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

fn websocket_reader_value(
    context: &CallContext,
    inbound: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<String>>>,
    state: Arc<WebSocketState>,
) -> luau::Value {
    let mut table = luau::OwnedTable::with_capacity(0, 2);
    let recv_inbound = inbound;
    table.set_field(
        "recv",
        luau::Value::NativeFunction(luau::NativeFunctionValue::new(
            websocket_function_options(context, "recv", ["self"]),
            harmony_core::async_luau_callback(Arc::new(move |mut frame| {
                let _self_value: luau::Value = frame.args.read_named("self")?;
                let inbound = recv_inbound.clone();
                Ok(luau::ScheduledFuture::new(async move {
                    let mut rx = inbound.lock().await;
                    match rx.recv().await {
                        Some(text) => Ok(luau::Value::String(text.into_bytes())),
                        None => Ok(luau::Value::Nil),
                    }
                }))
            })),
        )),
    );
    let close_state = state;
    table.set_field(
        "close",
        luau::Value::NativeFunction(luau::NativeFunctionValue::new(
            websocket_function_options(context, "close", ["self"]),
            Arc::new(move |mut frame| {
                let _self_value: luau::Value = frame.args.read_named("self")?;
                close_state.request_close();
                Ok(())
            }),
        )),
    );
    luau::Value::TableData(table)
}

fn websocket_sender_value(
    context: &CallContext,
    outbound: tokio::sync::mpsc::Sender<String>,
    state: Arc<WebSocketState>,
) -> luau::Value {
    let mut table = luau::OwnedTable::with_capacity(0, 3);
    let send_outbound = outbound;
    let send_state = state.clone();
    table.set_field(
        "send",
        luau::Value::NativeFunction(luau::NativeFunctionValue::new(
            websocket_function_options(context, "send", ["self", "text"]),
            harmony_core::async_luau_callback(Arc::new(move |mut frame| {
                let _self_value: luau::Value = frame.args.read_named("self")?;
                let text: String = frame.args.read_named("text")?;
                if send_state.is_closed() {
                    return Err(luau::Error::Runtime("websocket is closed".to_string()));
                }
                let outbound = send_outbound.clone();
                Ok(luau::ScheduledFuture::new(async move {
                    outbound
                        .send(text)
                        .await
                        .map_err(|_| luau::Error::Runtime("websocket is closed".to_string()))?;
                    Ok(())
                }))
            })),
        )),
    );
    let is_closed_state = state.clone();
    table.set_field(
        "is_closed",
        luau::Value::NativeFunction(luau::NativeFunctionValue::new(
            websocket_function_options(context, "is_closed", ["self"]),
            Arc::new(move |mut frame| {
                let _self_value: luau::Value = frame.args.read_named("self")?;
                frame.returns.write(is_closed_state.is_closed())
            }),
        )),
    );
    let close_state = state;
    table.set_field(
        "close",
        luau::Value::NativeFunction(luau::NativeFunctionValue::new(
            websocket_function_options(context, "close", ["self"]),
            Arc::new(move |mut frame| {
                let _self_value: luau::Value = frame.args.read_named("self")?;
                close_state.request_close();
                Ok(())
            }),
        )),
    );
    luau::Value::TableData(table)
}

fn websocket_function_options<const N: usize>(
    context: &CallContext,
    name: &'static str,
    args: [&'static str; N],
) -> luau::NativeFunctionOptions {
    luau::NativeFunctionOptions::new(super::luau_origin(&context.origin))
        .function_name(name)
        .argument_names(args.into_iter().map(Arc::<str>::from))
}
