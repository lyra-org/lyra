// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! What a Luau test gets as its chunk's second `...`: a table of helpers that stand in for what a
//! test can't reach from Luau, such as a client.

use std::{
    rc::Rc,
    sync::{
        Arc,
        Mutex,
    },
};

use harmony_core::UserDataBinding;
use harmony_luau as luau;

use crate::plugins::executor::{
    ApiResponseStream,
    ApiWebSocketSender,
    StreamOrigin,
};

/// How long a [`TestPlugin::call`] may run, as long as an API handler may.
const CALL_BUDGET: std::time::Duration = std::time::Duration::from_secs(300);

/// `{ sockets = TestSockets, plugin = TestPlugin, task = TestTask, catalog = TestCatalog }`.
pub(super) fn value(vm: &luau::Vm, origin: &luau::ChunkOrigin) -> anyhow::Result<luau::Value> {
    let harness = vm.create_table()?;
    harness.set(
        "sockets",
        TestSockets::class().create_value(vm, origin, TestSockets)?,
    )?;
    harness.set(
        "plugin",
        TestPlugin::class().create_value(vm, origin, TestPlugin)?,
    )?;
    harness.set(
        "task",
        TestTask::class().create_value(vm, origin, TestTask)?,
    )?;
    harness.set(
        "catalog",
        TestCatalog::class().create_value(vm, origin, TestCatalog)?,
    )?;
    Ok(luau::Value::Table(harness))
}

#[harmony_macros::userdata(
    name = "TestCatalog",
    description = "Changes the catalog as work beside the plugin does."
)]
#[derive(Clone)]
struct TestCatalog;

#[harmony_macros::userdata_methods]
impl TestCatalog {
    /// Merges the entity `loser_id` into `winner_id` of its kind.
    fn merge(
        &self,
        winner_id: String,
        loser_id: String,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        Ok(luau::ScheduledFuture::new(async move {
            crate::services::testing::merge_entities(&winner_id, &loser_id)
                .await
                .map_err(crate::plugins::runtime_error)
        }))
    }

    /// Deletes the entity `id` with nothing surviving it.
    fn remove(&self, id: String) -> luau::runtime::Result<luau::ScheduledFuture> {
        Ok(luau::ScheduledFuture::new(async move {
            let mut db = crate::services::STATE.db.write().await;
            let node = lyra_db::lookup::find_node_id_by_id(&*db, &id)
                .map_err(crate::plugins::runtime_error)?
                .ok_or_else(|| crate::plugins::runtime_error(format!("{id} is missing")))?;
            lyra_db::metadata::cascade_remove_entities(&mut db, &[node])
                .map_err(crate::plugins::runtime_error)?;
            Ok(())
        }))
    }
}

#[harmony_macros::userdata(
    name = "TestTask",
    description = "Waits in the test, whatever scopes the plugin declares."
)]
#[derive(Clone)]
struct TestTask;

#[harmony_macros::userdata_methods]
impl TestTask {
    /// Yields the test for at least `seconds`, and returns them.
    fn wait(&self, seconds: f64) -> luau::runtime::Result<luau::ScheduledFuture> {
        let delay = std::time::Duration::try_from_secs_f64(seconds).map_err(|error| {
            luau::Error::Runtime(format!(
                "wait time must be finite and non-negative: {error}"
            ))
        })?;
        Ok(luau::ScheduledFuture::after(
            delay,
            async move { Ok(seconds) },
        ))
    }
}

#[harmony_macros::userdata(
    name = "TestPlugin",
    description = "Calls the plugin's code as the host dispatches it, apart from the test."
)]
#[derive(Clone)]
struct TestPlugin;

/// The body of a `stream` response a [`TestPlugin::call`] returned, which the host produces as it
/// would for a client.
#[harmony_macros::userdata(
    name = "TestBody",
    description = "A streamed response's body, as a client would read it."
)]
#[derive(Clone)]
struct TestBody(Arc<Mutex<Option<ApiResponseStream>>>);

#[harmony_macros::userdata_methods]
impl TestBody {}

#[harmony_macros::userdata_methods]
impl TestPlugin {
    /// Calls `function` with `args` as a request's handler runs: in a thread of its own, acting
    /// for no user until it authenticates one, so whom it authenticates doesn't become the test's.
    /// Returns what it returns, and raises what it raises.
    fn call(
        &self,
        vm: &luau::Vm,
        context: &luau::CallContext,
        function: luau::Function,
        args: luau::Variadic<luau::Value>,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        let scheduler = vm.data().get::<harmony_core::LocalScheduler>()?;
        let thread = vm.create_thread(&function)?;
        let mut own = luau::CallContext {
            origin: context.origin.clone(),
            capability: context.capability.clone(),
            task_group: context.task_group,
            ..luau::CallContext::default()
        };
        own.caller
            .insert(crate::plugins::auth::DispatchAuth::default());
        let completion = Rc::new(harmony_core::LocalLuauTaskCompletion::default());
        scheduler.schedule_luau_thread_with_budget_and_completion(
            own.clone(),
            vm.clone(),
            thread,
            args.0,
            CALL_BUDGET,
            completion.clone(),
        );
        let deadline = std::time::Instant::now() + CALL_BUDGET;
        let vm = vm.clone();
        Ok(luau::ScheduledFuture::new(async move {
            let values = completion
                .wait()
                .await
                .map_err(|error| luau::Error::Runtime(error.message))?;
            if let Some((response, produce)) = values.first().and_then(stream_response) {
                let body = crate::plugins::executor::queue_stream(
                    &vm,
                    produce,
                    own.clone(),
                    deadline,
                    StreamOrigin {
                        plugin_id: own.origin.plugin.as_deref().unwrap_or_default().to_string(),
                        method: "CALL".to_string(),
                        path: own.origin.path.as_deref().unwrap_or_default().to_string(),
                    },
                )?;
                let body = TestBody(Arc::new(Mutex::new(Some(body))));
                response.set(
                    "body",
                    TestBody::class().create_value(&vm, &own.origin, body)?,
                )?;
            }
            Ok(luau::ReturnValues::Many(values))
        }))
    }

    /// Reads the body of a `stream` response `call` returned as a client keeping up would, while
    /// the host produces it as it would for one: acting as the handler did, once a place is free.
    /// Returns the body, and raises what fails it, including a return after a write raised.
    fn read_body(
        &self,
        vm: &luau::Vm,
        response: luau::Table,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        let luau::Value::UserData(body) = response.get("body")? else {
            return Err(luau::Error::Runtime(
                "read_body takes a stream response that plugin:call returned".to_string(),
            ));
        };
        let body = TestBody::class().read_userdata(vm, &body)?;
        let mut stream = body
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
            .ok_or_else(|| luau::Error::Runtime("the body was already read".to_string()))?;
        Ok(luau::ScheduledFuture::new(async move {
            let mut collected = Vec::new();
            while let Some(chunk) = futures::StreamExt::next(&mut stream).await {
                let chunk = chunk.map_err(|error| luau::Error::Runtime(error.to_string()))?;
                collected.extend_from_slice(&chunk);
            }
            Ok(luau::ByteString(collected))
        }))
    }
}

/// A `stream` response and its producer.
fn stream_response(response: &luau::Value) -> Option<(luau::Table, luau::Function)> {
    let luau::Value::Table(response) = response else {
        return None;
    };
    match (response.get("kind").ok()?, response.get("produce").ok()?) {
        (luau::Value::String(kind), luau::Value::Function(produce)) if kind == b"stream" => {
            Some((response.clone(), produce))
        }
        _ => None,
    }
}

#[harmony_macros::userdata(
    name = "TestSockets",
    description = "Opens and closes plugin websockets in place of a client."
)]
#[derive(Clone)]
struct TestSockets;

#[harmony_macros::userdata_methods]
impl TestSockets {
    /// A websocket's sender, as an `api.websocket` handler gets it, with no client reading it.
    fn open(
        &self,
        vm: &luau::Vm,
        origin: &luau::ChunkOrigin,
    ) -> luau::runtime::Result<luau::Value> {
        ApiWebSocketSender::class().create_value(vm, origin, ApiWebSocketSender::unconnected())
    }

    /// Closes the websocket as its client going away does, returning once the host is done with it.
    fn close(
        &self,
        vm: &luau::Vm,
        sender: luau::UserData,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        let sender = ApiWebSocketSender::class().read_userdata(vm, &sender)?;
        let released = sender.state().mark_closed();
        Ok(luau::ScheduledFuture::new(async move {
            crate::services::playbacks::pause_after_socket_close(released).await;
            Ok(())
        }))
    }
}
