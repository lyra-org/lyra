// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashSet;
use std::sync::Arc;

use harmony_core::LuaAsyncExt;
use mlua::{
    ExternalResult,
    Lua,
    Result,
    Table,
    Value,
};

use agdb::DbId;

use crate::{
    STATE,
    db,
    plugins::parse_ids,
};

struct IdsModule;

#[harmony_macros::module(plugin_scoped, name = "Ids", local = "ids", path = "lyra/ids")]
impl IdsModule {
    /// Returns the public nanoid string for a given numeric database ID, or nil if not found.
    pub(crate) async fn get_id(_plugin_id: Option<Arc<str>>, db_id: i64) -> Result<Option<String>> {
        let db = STATE.db.read().await;
        let id = db::lookup::find_id_by_db_id(&*db, DbId(db_id)).into_lua_err()?;
        Ok(id)
    }

    /// Returns public nanoid strings for many numeric database IDs.
    #[harmony(args(db_ids: Vec<u64>), returns(std::collections::BTreeMap<u64, Option<String>>))]
    pub(crate) async fn get_ids(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        db_ids: Table,
    ) -> Result<Table> {
        let ids = parse_ids(db_ids)?;
        let resolved = {
            let db = STATE.db.read().await;
            db::lookup::find_ids_by_db_ids(&*db, &ids).into_lua_err()?
        };

        let table = lua.create_table()?;
        for id in ids {
            match resolved.get(&id) {
                Some(nanoid) => table.set(id.0, nanoid.as_str())?,
                None => table.set(id.0, Value::Nil)?,
            }
        }
        Ok(table)
    }

    /// Returns the numeric database ID for a given public nanoid string, or nil if not found.
    pub(crate) async fn get_db_id(_plugin_id: Option<Arc<str>>, id: String) -> Result<Option<i64>> {
        let db = STATE.db.read().await;
        let db_id = db::lookup::find_node_id_by_id(&*db, &id).into_lua_err()?;
        Ok(db_id.map(|id| id.0))
    }

    /// Returns numeric database IDs for many public nanoid strings.
    #[harmony(args(ids: Vec<String>), returns(std::collections::BTreeMap<String, Option<u64>>))]
    pub(crate) async fn get_db_ids(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        ids: Table,
    ) -> Result<Table> {
        let mut strings = Vec::new();
        let mut seen = HashSet::new();
        for value in ids.sequence_values::<String>() {
            let id = value?;
            let trimmed = id.trim().to_string();
            if !trimmed.is_empty() && seen.insert(trimmed.clone()) {
                strings.push(trimmed);
            }
        }

        let str_refs: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
        let resolved = {
            let db = STATE.db.read().await;
            db::lookup::find_node_ids_by_ids(&*db, &str_refs).into_lua_err()?
        };

        let table = lua.create_table()?;
        for id in &strings {
            match resolved.get(id) {
                Some(db_id) => table.set(id.as_str(), db_id.0)?,
                None => table.set(id.as_str(), Value::Nil)?,
            }
        }
        Ok(table)
    }
}

crate::plugins::plugin_surface_exports!(
    IdsModule,
    "lyra.ids",
    "Identifier-conversion utilities.",
    Negligible
);
