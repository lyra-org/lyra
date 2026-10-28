// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use harmony_core::CallContext;
use harmony_luau as luau;

use crate::{
    plugins::callback_table::{
        CallbackTable,
        PluginCallback,
    },
    services::{
        EntityType,
        plugin_dispatch::PluginCallbackHandle,
    },
};

pub(crate) struct MetadataCallbackRegistry {
    handlers: CallbackTable<MetadataCallback>,
}

impl MetadataCallbackRegistry {
    pub(crate) fn new(vm_id: u64) -> Self {
        Self {
            handlers: CallbackTable::new(vm_id),
        }
    }

    pub(super) fn register(
        &self,
        provider_id: String,
        entity_type: EntityType,
        function: luau::Function,
        context: CallContext,
    ) -> PluginCallbackHandle {
        self.handlers.insert(MetadataCallback {
            provider_id,
            entity_type,
            function,
            context,
        })
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.handlers.len()
    }

    /// Ignores a handle another runtime issued.
    pub(crate) fn remove(&self, handle: PluginCallbackHandle) {
        self.handlers.remove(handle);
    }

    pub(crate) fn remove_plugin(&self, plugin_id: &str) {
        self.handlers.remove_plugin(plugin_id);
    }

    pub(crate) fn get(&self, handle: PluginCallbackHandle) -> anyhow::Result<MetadataCallback> {
        self.handlers.get(handle)
    }

    pub(crate) fn get_for_provider(
        &self,
        handle: PluginCallbackHandle,
        provider_id: &str,
        entity_type: EntityType,
    ) -> anyhow::Result<MetadataCallback> {
        let handler = self.handlers.get(handle)?;
        anyhow::ensure!(
            handler.provider_id == provider_id && handler.entity_type == entity_type,
            "handler {} is not provider '{provider_id}''s {entity_type:?} handler",
            handle.handler_id
        );
        Ok(handler)
    }
}

#[derive(Clone)]
pub(crate) struct MetadataCallback {
    provider_id: String,
    entity_type: EntityType,
    pub(crate) function: luau::Function,
    pub(crate) context: CallContext,
}

impl PluginCallback for MetadataCallback {
    fn context(&self) -> &CallContext {
        &self.context
    }
}
