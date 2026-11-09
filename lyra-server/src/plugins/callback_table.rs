// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    cell::{
        Cell,
        RefCell,
    },
    collections::BTreeMap,
};

use anyhow::Result;
use harmony_core::CallContext;

use crate::services::plugin_dispatch::{
    DispatchNotStarted,
    PluginCallbackHandle,
};

/// A callback that runs in the call context of the plugin that registered it.
pub(crate) trait PluginCallback: Clone {
    fn context(&self) -> &CallContext;
}

/// The callbacks one plugin runtime issued handles for, numbered from 1.
pub(crate) struct CallbackTable<T> {
    vm_id: u64,
    next_id: Cell<u64>,
    entries: RefCell<BTreeMap<u64, T>>,
}

impl<T: PluginCallback> CallbackTable<T> {
    pub(crate) fn new(vm_id: u64) -> Self {
        Self {
            vm_id,
            next_id: Cell::new(1),
            entries: RefCell::new(BTreeMap::new()),
        }
    }

    pub(crate) fn insert(&self, value: T) -> PluginCallbackHandle {
        let handler_id = self.next_id.get();
        self.next_id.set(handler_id.saturating_add(1));
        self.entries.borrow_mut().insert(handler_id, value);
        PluginCallbackHandle {
            vm_id: self.vm_id,
            handler_id,
        }
    }

    /// Fails for a handle another runtime issued or one since removed.
    pub(crate) fn get(&self, handle: PluginCallbackHandle) -> Result<T> {
        let handler_id = handle.handler_id_in(self.vm_id)?;
        self.entries
            .borrow()
            .get(&handler_id)
            .cloned()
            .ok_or_else(|| {
                anyhow::Error::new(DispatchNotStarted)
                    .context(format!("handler {handler_id} not found"))
            })
    }

    /// Ignores a handle another runtime issued.
    pub(crate) fn remove(&self, handle: PluginCallbackHandle) {
        if let Ok(handler_id) = handle.handler_id_in(self.vm_id) {
            self.entries.borrow_mut().remove(&handler_id);
        }
    }

    /// Drops the plugin's callbacks so a stopped run can't be dispatched into.
    pub(crate) fn remove_plugin(&self, plugin_id: &str) {
        self.entries
            .borrow_mut()
            .retain(|_, value| value.context().origin.plugin.as_deref() != Some(plugin_id));
    }

    /// Every callback, in registration order.
    pub(crate) fn values(&self) -> Vec<T> {
        self.entries.borrow().values().cloned().collect()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.borrow().len()
    }
}
