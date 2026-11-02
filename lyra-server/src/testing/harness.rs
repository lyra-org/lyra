// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! What a Luau test gets as its chunk's second `...`: a table of helpers that stand in for what a
//! test can't reach from Luau, such as a client.

use std::rc::Rc;

use harmony_core::UserDataBinding;
use harmony_luau as luau;

use crate::plugins::executor::ApiWebSocketSender;

/// How long a [`TestPlugin::call`] may run, as long as an API handler may.
const CALL_BUDGET: std::time::Duration = std::time::Duration::from_secs(300);

/// `{ sockets = TestSockets, plugin = TestPlugin, task = TestTask }`.
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
    Ok(luau::Value::Table(harness))
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
            own,
            vm.clone(),
            thread,
            args.0,
            CALL_BUDGET,
            completion.clone(),
        );
        Ok(luau::ScheduledFuture::new(async move {
            let values = std::future::poll_fn(|cx| completion.poll(cx))
                .await
                .map_err(luau::Error::Runtime)?;
            Ok(luau::ReturnValues::Many(values))
        }))
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
