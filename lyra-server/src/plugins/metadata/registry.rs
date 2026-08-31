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
        provider_id: String,
        entity_type: EntityType,
        function: luau::Function,
        context: CallContext,
    ) -> u64 {
        let id = self.next_handler_id.get();
        self.next_handler_id.set(id.saturating_add(1));
        self.handlers.borrow_mut().insert(
            id,
            MetadataCallback {
                provider_id,
                entity_type,
                function,
                context,
            },
        );
        id
    }

    pub(crate) fn get(&self, id: u64) -> Option<MetadataCallback> {
        self.handlers.borrow().get(&id).cloned()
    }

    pub(crate) fn get_for_provider(
        &self,
        id: u64,
        provider_id: &str,
        entity_type: EntityType,
    ) -> Option<MetadataCallback> {
        self.handlers.borrow().get(&id).and_then(|handler| {
            (handler.provider_id == provider_id && handler.entity_type == entity_type)
                .then(|| handler.clone())
        })
    }
}

#[derive(Clone)]
pub(crate) struct MetadataCallback {
    provider_id: String,
    entity_type: EntityType,
    pub(crate) function: luau::Function,
    pub(crate) context: CallContext,
}
