// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        HashMap,
        HashSet,
    },
    sync::{
        Arc,
        Mutex,
    },
};

use agdb::DbId;
use harmony_luau as luau;
use serde_json::Value as JsonValue;

use super::MetadataModuleStore;
use super::parsing::parse_custom_field_version;

#[harmony_macros::userdata(name = "Layer", description = "Metadata layer builder.")]
#[derive(Clone)]
pub(super) struct MetadataLayer {
    store: MetadataModuleStore,
    state: Arc<Mutex<LayerBuilderState>>,
}

#[derive(Clone)]
struct LayerBuilderState {
    node_id: DbId,
    provider_id: String,
    fields: HashMap<String, JsonValue>,
    external_ids: HashMap<String, String>,
    custom_fields: HashMap<u64, HashMap<String, JsonValue>>,
    remove_custom_field_versions: HashSet<u64>,
}

#[harmony_macros::userdata_methods]
impl MetadataLayer {
    fn set_id(&self, id_type: String, id_value: String) -> luau::runtime::Result<()> {
        if id_type.trim().is_empty() {
            return Err(crate::plugins::runtime_error(
                "metadata layer id key must not be empty",
            ));
        }
        self.state
            .lock()
            .expect("metadata layer mutex poisoned")
            .external_ids
            .insert(id_type, id_value);
        Ok(())
    }

    #[harmony(args(name: String, value: luau::JsonValue))]
    fn set_field(
        &self,
        vm: &luau::Vm,
        name: String,
        value: luau::Value,
    ) -> luau::runtime::Result<()> {
        if name.trim().is_empty() {
            return Err(crate::plugins::runtime_error(
                "metadata layer field key must not be empty",
            ));
        }
        if name == "duration_ms" {
            return Err(crate::plugins::runtime_error(
                "duration_ms is read-only and cannot be set by plugins",
            ));
        }
        let value = harmony_serde::luau_to_json(vm, &value, 0)?;
        self.state
            .lock()
            .expect("metadata layer mutex poisoned")
            .fields
            .insert(name, value);
        Ok(())
    }

    fn save(&self) -> luau::runtime::Result<()> {
        let Some(db) = self.store.db.clone() else {
            return Err(crate::plugins::runtime_error(
                "metadata layer save requires a database-backed plugin executor",
            ));
        };
        let layer = self
            .state
            .lock()
            .expect("metadata layer mutex poisoned")
            .clone();
        futures::executor::block_on(async {
            let mut db = db.write().await;
            crate::services::metadata::layers::save_provider_layer(
                &mut db,
                layer.node_id,
                &layer.provider_id,
                &layer.fields,
                &layer.external_ids,
                &layer.custom_fields,
                &layer.remove_custom_field_versions,
            )
            .map_err(crate::plugins::runtime_error)
        })
    }

    #[harmony(args(version: String, name: String, value: luau::JsonValue))]
    fn set_custom_field(
        &self,
        vm: &luau::Vm,
        version: String,
        name: String,
        value: luau::Value,
    ) -> luau::runtime::Result<()> {
        let version = parse_custom_field_version(&version)?;
        let name = name.trim().to_string();
        if name.is_empty() {
            return Err(crate::plugins::runtime_error(
                "custom field name must be a non-empty string",
            ));
        }
        let value = harmony_serde::luau_to_json(vm, &value, 0)?;
        let mut layer = self.state.lock().expect("metadata layer mutex poisoned");
        layer.remove_custom_field_versions.remove(&version);
        layer
            .custom_fields
            .entry(version)
            .or_default()
            .insert(name, value);
        Ok(())
    }

    #[harmony(args(version: String, fields: luau::JsonValue))]
    fn set_custom_fields(
        &self,
        vm: &luau::Vm,
        version: String,
        fields: luau::Value,
    ) -> luau::runtime::Result<()> {
        let version = parse_custom_field_version(&version)?;
        let JsonValue::Object(fields) = harmony_serde::luau_to_json(vm, &fields, 0)? else {
            return Err(crate::plugins::runtime_error(
                "custom fields payload must be a JSON object",
            ));
        };
        let mut layer = self.state.lock().expect("metadata layer mutex poisoned");
        layer.remove_custom_field_versions.remove(&version);
        layer
            .custom_fields
            .insert(version, fields.into_iter().collect());
        Ok(())
    }

    fn clear_custom_fields(&self, version: String) -> luau::runtime::Result<()> {
        let version = parse_custom_field_version(&version)?;
        let mut layer = self.state.lock().expect("metadata layer mutex poisoned");
        layer.custom_fields.remove(&version);
        layer.remove_custom_field_versions.insert(version);
        Ok(())
    }
}

impl MetadataLayer {
    pub(super) fn new_value(
        vm: &luau::Vm,
        origin: &luau::ChunkOrigin,
        store: MetadataModuleStore,
        node_id: DbId,
        provider_id: String,
    ) -> luau::runtime::Result<luau::Value> {
        let layer = Self {
            store,
            state: Arc::new(Mutex::new(LayerBuilderState {
                node_id,
                provider_id,
                fields: HashMap::new(),
                external_ids: HashMap::new(),
                custom_fields: HashMap::new(),
                remove_custom_field_versions: HashSet::new(),
            })),
        };
        Self::_harmony_userdata_class().create_value(vm, origin, layer)
    }
}
