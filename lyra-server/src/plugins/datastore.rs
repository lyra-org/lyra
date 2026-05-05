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

    /// Removes an entry from this store by key. Returns whether a value was removed.
    pub(crate) async fn remove(
        &self,
        _plugin_id: Option<Arc<str>>,
        key: String,
    ) -> anyhow::Result<bool> {
        let datastore_id = self
            .db_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("datastore missing db_id"))?
            .into();

        let db = &mut *STATE.db.write().await;
        db::datastore::remove_entry(db, datastore_id, &key)
    }

    /// Gets multiple JSON values from this store under one read lock.
    /// Returns an array parallel to `keys` with `nil` for missing entries.
    #[harmony(args(keys: Vec<String>), returns(Vec<Option<JsonValue>>))]
    pub(crate) async fn get_many(
        &self,
        _plugin_id: Option<Arc<str>>,
        keys: Vec<String>,
    ) -> anyhow::Result<Vec<Option<mlua::Value>>> {
        let datastore_id = self
            .db_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("datastore missing db_id"))?
            .into();

        let raw_values = {
            let db = &*STATE.db.read().await;
            let mut out = Vec::with_capacity(keys.len());
            for key in &keys {
                out.push(db::datastore::get_entry(db, datastore_id, key)?.map(|entry| entry.value));
            }
            out
        };

        let lua = STATE.lua.get();
        let mut results = Vec::with_capacity(raw_values.len());
        for raw in raw_values {
            match raw {
                Some(s) => {
                    let json: serde_json::Value = serde_json::from_str(&s)?;
                    results.push(Some(lua.to_value_with(&json, LUA_SERIALIZE_OPTIONS)?));
                }
                None => results.push(None),
            }
        }
        Ok(results)
    }

    /// Writes multiple JSON values to this store under one write lock.
    #[harmony(args(entries: std::collections::BTreeMap<String, JsonValue>))]
    pub(crate) async fn set_many(
        &self,
        _plugin_id: Option<Arc<str>>,
        entries: std::collections::BTreeMap<String, mlua::Value>,
    ) -> anyhow::Result<()> {
        let datastore_id = self
            .db_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("datastore missing db_id"))?
            .into();

        let lua = STATE.lua.get();
        let mut prepared = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            let json: serde_json::Value = from_lua_json_value(lua.as_ref(), value)?;
            prepared.push((key, serde_json::to_string(&json)?));
        }

        let db = &mut *STATE.db.write().await;
        for (key, json_str) in prepared {
            db::datastore::upsert_entry(db, datastore_id, key, json_str)?;
        }
        Ok(())
    }

    /// Removes every entry from this store. Returns the number removed.
    pub(crate) async fn clear(&self, _plugin_id: Option<Arc<str>>) -> anyhow::Result<u64> {
        let datastore_id = self
            .db_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("datastore missing db_id"))?
            .into();

        let db = &mut *STATE.db.write().await;
        let removed = db::datastore::clear_entries(db, datastore_id)?;
        Ok(removed as u64)
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
