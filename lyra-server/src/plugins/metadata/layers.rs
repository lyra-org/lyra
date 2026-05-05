// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;
use std::sync::Arc;

use crate::plugins::lifecycle::PluginId;

use harmony_core::LuaUserDataAsyncExt;
use harmony_luau::DescribeUserData;
use mlua::{
    ExternalResult,
    Result,
};

use crate::STATE;
use crate::db::NodeId;
use crate::services::metadata::layers::save_provider_layer;

use agdb::DbId;

#[derive(Clone, Debug)]
pub(crate) struct Layer {
    /// Plugin that owns the Provider this Layer was minted from. All
    /// plugins share one Lua state, so a Layer userdata stashed in
    /// `_G` could otherwise be saved by any other plugin and the
    /// write would land under `provider_id` — silent cross-plugin
    /// metadata corruption.
    pub(crate) plugin_id: PluginId,
    pub(crate) provider_id: String,
    pub(crate) entity_id: NodeId,
    pub(crate) fields: HashMap<String, serde_json::Value>,
    pub(crate) external_ids: HashMap<String, String>,
}

impl Layer {
    fn ensure_owner(&self, caller: Option<&PluginId>) -> Result<()> {
        match caller {
            Some(id) if id == &self.plugin_id => Ok(()),
            _ => Err(mlua::Error::runtime(format!(
                "layer for provider '{}' must be saved by owning plugin '{}'",
                self.provider_id, self.plugin_id
            ))),
        }
    }
}

#[harmony_macros::implementation(plugin_scoped)]
impl Layer {
    /// Sets a field on the provider layer.
    #[harmony(args(name: String, value: harmony_luau::JsonValue))]
    pub(crate) fn set_field(&mut self, name: String, value: mlua::Value) -> Result<()> {
        if name == "duration_ms" {
            return Err(mlua::Error::runtime(
                "duration_ms is read-only and cannot be set by plugins",
            ));
        }
        let json_value = serde_json::to_value(&value).into_lua_err()?;
        self.fields.insert(name, json_value);
        Ok(())
    }

    pub(crate) fn set_id(&mut self, id_type: String, id_value: String) {
        self.external_ids.insert(id_type, id_value);
    }

    pub(crate) async fn save(&self, plugin_id: Option<Arc<str>>) -> anyhow::Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        self.ensure_owner(plugin_id.as_ref())?;
        let entity_db_id: DbId = self.entity_id.clone().into();
        let mut db_write = STATE.db.write().await;
        save_provider_layer(
            &mut db_write,
            entity_db_id,
            &self.provider_id,
            &self.fields,
            &self.external_ids,
        )?;

        Ok(())
    }
}

harmony_macros::compile!(type_path = Layer, fields = false, methods = true);

pub(super) fn class_descriptor() -> harmony_luau::ClassDescriptor {
    <Layer as DescribeUserData>::class_descriptor()
}
