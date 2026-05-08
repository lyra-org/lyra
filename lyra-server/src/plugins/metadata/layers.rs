// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};
use std::sync::Arc;

use crate::plugins::lifecycle::PluginId;

use harmony_core::LuaUserDataAsyncExt;
use harmony_luau::DescribeUserData;
use mlua::{
    ExternalResult,
    Result,
};

use crate::STATE;
use crate::plugins::db::NodeId;
use crate::plugins::from_lua_json_value;
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
    pub(crate) custom_fields: HashMap<u64, HashMap<String, serde_json::Value>>,
    pub(crate) remove_custom_field_versions: HashSet<u64>,
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

fn parse_custom_field_version(raw: &str) -> Result<u64> {
    let version = raw.trim();
    let Some(number) = version.strip_prefix('v') else {
        return Err(mlua::Error::runtime(
            "custom fields version must be formatted as vN with N a positive integer",
        ));
    };
    if number.is_empty()
        || (number.len() > 1 && number.starts_with('0'))
        || !number.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(mlua::Error::runtime(
            "custom fields version must be formatted as vN with N a positive integer",
        ));
    }

    let parsed = number.parse::<u64>().map_err(|_| {
        mlua::Error::runtime(
            "custom fields version must be formatted as vN with N a positive integer",
        )
    })?;
    if parsed == 0 {
        return Err(mlua::Error::runtime(
            "custom fields version must be formatted as vN with N a positive integer",
        ));
    }

    Ok(parsed)
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

    /// Sets a custom field under a provider-owned schema version.
    #[harmony(args(version: String, name: String, value: harmony_luau::JsonValue))]
    pub(crate) fn set_custom_field(
        &mut self,
        version: String,
        name: String,
        value: mlua::Value,
    ) -> Result<()> {
        let version = parse_custom_field_version(&version)?;
        let name = name.trim().to_string();
        if name.is_empty() {
            return Err(mlua::Error::runtime(
                "custom field name must be a non-empty string",
            ));
        }

        let lua = STATE.lua.get();
        let json_value: serde_json::Value = from_lua_json_value(lua.as_ref(), value)?;
        self.remove_custom_field_versions.remove(&version);
        self.custom_fields
            .entry(version)
            .or_default()
            .insert(name, json_value);
        Ok(())
    }

    /// Replaces all custom fields under a provider-owned schema version.
    #[harmony(args(version: String, fields: harmony_luau::JsonValue))]
    pub(crate) fn set_custom_fields(&mut self, version: String, fields: mlua::Value) -> Result<()> {
        let version = parse_custom_field_version(&version)?;
        let lua = STATE.lua.get();
        let json_value: serde_json::Value = from_lua_json_value(lua.as_ref(), fields)?;
        let serde_json::Value::Object(fields) = json_value else {
            return Err(mlua::Error::runtime(
                "custom fields payload must be a JSON object",
            ));
        };

        self.remove_custom_field_versions.remove(&version);
        self.custom_fields
            .insert(version, fields.into_iter().collect());
        Ok(())
    }

    /// Removes custom fields under a provider-owned schema version on save.
    pub(crate) fn clear_custom_fields(&mut self, version: String) -> Result<()> {
        let version = parse_custom_field_version(&version)?;
        self.custom_fields.remove(&version);
        self.remove_custom_field_versions.insert(version);
        Ok(())
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
            &self.custom_fields,
            &self.remove_custom_field_versions,
        )?;

        Ok(())
    }
}

harmony_macros::compile!(type_path = Layer, fields = false, methods = true);

pub(super) fn class_descriptor() -> harmony_luau::ClassDescriptor {
    <Layer as DescribeUserData>::class_descriptor()
}

#[cfg(test)]
mod tests {
    use super::parse_custom_field_version;

    #[test]
    fn custom_field_version_accepts_canonical_positive_integer() -> anyhow::Result<()> {
        assert_eq!(parse_custom_field_version("v1")?, 1);
        assert_eq!(parse_custom_field_version(" v12 ")?, 12);
        Ok(())
    }

    #[test]
    fn custom_field_version_rejects_non_canonical_versions() {
        for version in ["", "1", "V1", "v", "v0", "v01", "v1.1", "v-1"] {
            assert!(
                parse_custom_field_version(version).is_err(),
                "{version} should be rejected"
            );
        }
    }
}
