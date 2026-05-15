// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashSet;
use std::fmt;

use harmony_core::{
    Danger,
    Module,
    ModuleBuilder,
    ModuleExport,
};
use harmony_luau::ModuleFunctionDescriptor;
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
    plugins::db,
    plugins::parse_ids,
};

async fn get_id(db_id: i64) -> Result<Option<String>> {
    let db = STATE.db.read().await;
    let id = db::lookup::find_id_by_db_id(&*db, DbId(db_id)).into_lua_err()?;
    Ok(id)
}

async fn get_ids(lua: Lua, db_ids: Table) -> Result<Table> {
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

async fn get_db_id(id: String) -> Result<Option<i64>> {
    let db = STATE.db.read().await;
    let db_id = db::lookup::find_node_id_by_id(&*db, &id).into_lua_err()?;
    Ok(db_id.map(|id| id.0))
}

async fn get_db_ids(lua: Lua, ids: Table) -> Result<Table> {
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

fn module_export() -> ModuleExport {
    ModuleBuilder::empty("lyra/ids", "Ids", "ids")
        .scope_id(
            "lyra.ids",
            "Identifier-conversion utilities.",
            Danger::Negligible,
        )
        .async_function(
            ModuleFunctionDescriptor::new("get_id")
                .description(
                    "Returns the public nanoid string for a given numeric database ID, or nil if not found.",
                )
                .param_type::<i64>("db_id")
                .returns::<Option<String>>()
                .yields(),
            |_, db_id| get_id(db_id),
        )
        .async_function(
            ModuleFunctionDescriptor::new("get_ids")
                .description("Returns public nanoid strings for many numeric database IDs.")
                .param_type::<Vec<u64>>("db_ids")
                .returns::<std::collections::BTreeMap<u64, Option<String>>>()
                .yields(),
            get_ids,
        )
        .async_function(
            ModuleFunctionDescriptor::new("get_db_id")
                .description(
                    "Returns the numeric database ID for a given public nanoid string, or nil if not found.",
                )
                .param_type::<String>("id")
                .returns::<Option<i64>>()
                .yields(),
            |_, id| get_db_id(id),
        )
        .async_function(
            ModuleFunctionDescriptor::new("get_db_ids")
                .description("Returns numeric database IDs for many public nanoid strings.")
                .param_type::<Vec<String>>("ids")
                .returns::<std::collections::BTreeMap<String, Option<u64>>>()
                .yields(),
            get_db_ids,
        )
        .build()
}

pub(crate) fn get_module() -> Module {
    module_export().into_module()
}

pub(crate) fn render_luau_definition() -> std::result::Result<String, fmt::Error> {
    module_export().render_luau_definition()
}
