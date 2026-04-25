// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use harmony_core::{
    LuaAsyncExt,
    LuaUserDataAsyncExt,
};
use harmony_luau::JsonValue;
use mlua::{
    LuaSerdeExt,
    Result,
};

use crate::{
    STATE,
    db::{
        self,
        DataStore,
    },
    plugins::{
        LUA_SERIALIZE_OPTIONS,
        from_lua_json_value,
    },
};

#[harmony_macros::implementation(plugin_scoped)]
impl DataStore {
    /// Gets a JSON value from this store by key.
    #[harmony(returns(Option<JsonValue>))]
    pub(crate) async fn get(
        &self,
        _plugin_id: Option<Arc<str>>,
        key: String,
    ) -> anyhow::Result<Option<mlua::Value>> {
        let datastore_id = self
            .db_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("datastore missing db_id"))?
            .into();

        let raw_value = {
            let db = &*STATE.db.read().await;
            match db::datastore::get_entry(db, datastore_id, &key)? {
                Some(entry) => entry.value,
                None => return Ok(None),
            }
        };

        let json: serde_json::Value = serde_json::from_str(&raw_value)?;
        let lua = STATE.lua.get();
        Ok(Some(lua.to_value_with(&json, LUA_SERIALIZE_OPTIONS)?))
    }

    /// Sets a JSON value in this store by key.
    #[harmony(args(key: String, value: JsonValue))]
    pub(crate) async fn set(
        &self,
        _plugin_id: Option<Arc<str>>,
        key: String,
        value: mlua::Value,
    ) -> anyhow::Result<()> {
        let datastore_id = self
            .db_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("datastore missing db_id"))?
            .into();
        let lua = STATE.lua.get();
        let json: serde_json::Value = from_lua_json_value(lua.as_ref(), value)?;
        let json_str = serde_json::to_string(&json)?;

        let db = &mut *STATE.db.write().await;
        db::datastore::upsert_entry(db, datastore_id, key, json_str)?;

        Ok(())
    }
}

harmony_macros::compile!(type_path = DataStore, fields = false, methods = true);

struct DataStoreModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "DataStore",
    local = "datastore",
    path = "lyra/datastore",
    aliases(JsonValue),
    classes(DataStore)
)]
impl DataStoreModule {
    /// Returns a named data store, creating it if needed.
    pub(crate) async fn get_or_create(
        _plugin_id: Option<Arc<str>>,
        name: String,
    ) -> Result<DataStore> {
        {
            let db = STATE.db.read().await;
            if let Some(existing) = db::datastore::find_by_name(&db, &name)
                .map_err(|err| mlua::Error::runtime(err.to_string()))?
            {
                return Ok(existing);
            }
        }

        let datastore = db::datastore::get_or_create(&mut *STATE.db.write().await, name)?;
        Ok(datastore)
    }
}

crate::plugins::plugin_surface_exports!(
    DataStoreModule,
    "lyra.datastore",
    "Read and write this plugin's private key-value store.",
    High
);
