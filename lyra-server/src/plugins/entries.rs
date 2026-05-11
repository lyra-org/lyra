// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::QueryId;
use harmony_core::LuaAsyncExt;
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

struct EntriesModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "Entries",
    local = "entries",
    path = "lyra/entries",
    interfaces(EntryInfo)
)]
impl EntriesModule {
    /// Returns entries related to the given id, or all library entries by default.
    #[harmony(returns(Vec<EntryInfo>))]
    pub(crate) async fn get(
        lua: Lua,
        #[harmony_context] caller: RequestCaller,
        id: Option<ResolveId>,
    ) -> Result<Table> {
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
}

crate::plugins::plugin_surface_exports!(
    EntriesModule,
    "lyra.entries",
    "Read filesystem entry metadata.",
    Low
);
