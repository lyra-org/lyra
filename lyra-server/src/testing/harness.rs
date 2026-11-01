// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! What a Luau test gets as its chunk's second `...`: a table of helpers that stand in for what a
//! test can't reach from Luau, such as a client.

use harmony_core::UserDataBinding;
use harmony_luau as luau;

use crate::plugins::executor::ApiWebSocketSender;

/// `{ sockets = TestSockets }`.
pub(super) fn value(vm: &luau::Vm, origin: &luau::ChunkOrigin) -> anyhow::Result<luau::Value> {
    let harness = vm.create_table()?;
    harness.set(
        "sockets",
        TestSockets::class().create_value(vm, origin, TestSockets)?,
    )?;
    Ok(luau::Value::Table(harness))
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
