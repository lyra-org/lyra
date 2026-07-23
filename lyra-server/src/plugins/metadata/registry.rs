// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    cell::{
        Cell,
        RefCell,
    },
    collections::HashMap,
};

use harmony_core::CallContext;
use harmony_luau as luau;

use crate::services::EntityType;

#[derive(Default)]
pub(crate) struct MetadataCallbackRegistry {
    next_handler_id: Cell<u64>,
    handlers: RefCell<HashMap<u64, MetadataCallback>>,
}

impl MetadataCallbackRegistry {
    pub(crate) fn new() -> Self {
        Self {
            next_handler_id: Cell::new(1),
            handlers: RefCell::new(HashMap::new()),
        }
    }

    pub(super) fn register(
        &self,
        _provider_id: String,
        _entity_type: EntityType,
        function: luau::Function,
        context: CallContext,
    ) -> u64 {
        let id = self.next_handler_id.get();
        self.next_handler_id.set(id.saturating_add(1));
        self.handlers
            .borrow_mut()
            .insert(id, MetadataCallback { function, context });
        id
    }

    pub(crate) fn get(&self, id: u64) -> Option<MetadataCallback> {
        self.handlers.borrow().get(&id).cloned()
    }
}

#[derive(Clone)]
pub(crate) struct MetadataCallback {
    pub(crate) function: luau::Function,
    pub(crate) context: CallContext,
}
