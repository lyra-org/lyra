// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::fmt;

use agdb::QueryId;
use harmony_core::{
    Danger,
    Module,
    ModuleBuilder,
    ModuleExport,
};
use harmony_luau::{
    DescribeInterface,
    ModuleFunctionDescriptor,
};
use mlua::{
    ExternalResult,
    Lua,
    Result,
    Table,
};

use crate::{
    STATE,
    plugins::caller::RequestCaller,
    plugins::db::{
        self,
        NodeId,
        Permission,
        ResolveId,
    },
};

#[harmony_macros::interface]
struct EntryInfo {
    db_id: Option<NodeId>,
    id: String,
    full_path: Option<String>,
    kind: String,
    name: String,
    hash: Option<String>,
    size: u64,
    mtime: u64,
}

use super::entry_to_table;

async fn get(lua: Lua, caller: RequestCaller, id: Option<ResolveId>) -> Result<Table> {
    let db = STATE.db.read().await;
    let include_full_path =
        db::roles::has_permission(&caller.principal.permissions, Permission::ManageLibraries);

    let entries = match id {
        None => db::entries::get(&db, "libraries").into_lua_err()?,
        Some(resolve_id) => {
            let query_id = resolve_id
                .to_query_id(&db)
                .into_lua_err()?
                .ok_or_else(|| mlua::Error::runtime("could not resolve id"))?;
            match query_id {
                QueryId::Id(node_id) => {
                    if db::tracks::get_by_id(&db, node_id)
                        .into_lua_err()?
                        .is_some()
                    {
                        if !crate::routes::entity_accessible_to_principal(
                            &db,
                            &caller.principal,
                            node_id,
                        )
                        .into_lua_err()?
                        {
                            return lua.create_table();
                        }
                        db::entries::get_by_track(&db, node_id).into_lua_err()?
                    } else {
                        db::entries::get(&db, QueryId::Id(node_id)).into_lua_err()?
                    }
                }
                other => db::entries::get(&db, other).into_lua_err()?,
            }
        }
    };

    let rows = lua.create_table()?;
    let mut index = 1usize;
    for entry in entries {
        let Some(entry_db_id) = entry.db_id else {
            continue;
        };
        if !crate::routes::entity_accessible_to_principal(&db, &caller.principal, entry_db_id)
            .into_lua_err()?
        {
            continue;
        }
        rows.set(index, entry_to_table(&lua, entry, include_full_path)?)?;
        index += 1;
    }

    Ok(rows)
}

fn module_export() -> ModuleExport {
    ModuleBuilder::empty("lyra/entries", "Entries", "entries")
        .scope_id(
            "lyra.entries",
            "Read filesystem entry metadata.",
            Danger::Low,
        )
        .with_interface(EntryInfo::interface_descriptor())
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("get")
                .description(
                    "Returns entries related to the given id, or all library entries by default.",
                )
                .param_type::<Option<ResolveId>>("id")
                .returns::<Vec<EntryInfo>>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |lua, caller, id| async move {
                let caller = caller?;
                get(lua, caller, id).await
            },
        )
        .build()
}

pub(crate) fn get_module() -> Module {
    module_export().into_module()
}

pub(crate) fn render_luau_definition() -> std::result::Result<String, fmt::Error> {
    module_export().render_luau_definition()
}
