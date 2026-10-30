// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Callbacks plugins register with an `on_update` function, kept per VM and per kind of update.

use std::marker::PhantomData;

use harmony_core::CallContext;
use harmony_luau as luau;

use crate::{
    plugins::callback_table::{
        CallbackTable,
        PluginCallback,
    },
    services::plugin_scope::PluginId,
};

#[derive(Clone)]
pub(crate) struct RegisteredUpdateCallback {
    pub(crate) context: CallContext,
    pub(crate) function: luau::Function,
}

impl PluginCallback for RegisteredUpdateCallback {
    fn context(&self) -> &CallContext {
        &self.context
    }
}

/// The callbacks registered for updates carrying `Payload`.
pub(crate) struct UpdateCallbackStore<Payload> {
    handlers: CallbackTable<RegisteredUpdateCallback>,
    payload: PhantomData<fn(Payload)>,
}

impl<Payload> UpdateCallbackStore<Payload> {
    pub(crate) fn new(vm_id: u64) -> Self {
        Self {
            handlers: CallbackTable::new(vm_id),
            payload: PhantomData,
        }
    }

    /// Every handler, in registration order.
    pub(crate) fn snapshot(&self) -> Vec<RegisteredUpdateCallback> {
        self.handlers.values()
    }

    pub(crate) fn remove_plugin(&self, plugin_id: &str) {
        self.handlers.remove_plugin(plugin_id);
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.handlers.len()
    }
}

/// Registers `function`, called as `function_name`, in the VM's store for `Payload` updates.
pub(crate) fn register<Payload: 'static>(
    vm: &luau::Vm,
    context: &luau::CallContext,
    function: luau::Function,
    function_name: &str,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let plugin_id = context.origin.plugin.clone().ok_or_else(|| {
        luau::Error::Runtime(format!(
            "{function_name} must be called from plugin Luau code"
        ))
    })?;
    let validated_plugin_id = PluginId::new(plugin_id.to_string())?;
    harmony_core::ensure_current_plugin_run(vm, context)?;
    let vm = vm.clone();
    let context = context.clone();
    Ok(luau::ScheduledFuture::new(async move {
        let generation = crate::plugins::STATE.generation();
        let _registration = generation
            .registries
            .ensure_registrations_open(&validated_plugin_id)
            .await?;
        let callbacks = vm.data().get::<UpdateCallbackStore<Payload>>()?;
        callbacks
            .handlers
            .insert(RegisteredUpdateCallback { context, function });
        Ok(())
    }))
}
