// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

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
    plugins::caller::{
        request_caller_at,
        system_caller_at,
    },
    plugins::db::{
        self,
        NodeId,
        ResolveId,
    },
};

#[harmony_macros::interface]
struct Library {
    db_id: Option<NodeId>,
    id: String,
    name: String,
    directory: Option<String>,
    language: Option<String>,
    country: Option<String>,
}

struct LibrariesModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "Libraries",
    local = "libraries",
    path = "lyra/libraries",
    interfaces(Library)
)]
impl LibrariesModule {
    /// Lists libraries matching the given id or alias, or all libraries by default.
    #[harmony(returns(Vec<Library>))]
    pub(crate) async fn list(
        lua: Lua,
        plugin_id: Option<Arc<str>>,
        id: Option<ResolveId>,
    ) -> Result<Table> {
        let db = STATE.db.read().await;

        if let Ok(caller) = request_caller_at(&lua, plugin_id.clone()) {
            let libraries = match id {
                None => db::libraries::accessible(&db, &caller.principal).into_lua_err()?,
                Some(resolve_id) => {
                    let query_id = resolve_id
                        .to_query_id(&db)
                        .into_lua_err()?
                        .ok_or_else(|| mlua::Error::runtime("could not resolve id"))?;
                    match query_id {
                        QueryId::Id(node_id) => {
                            db::libraries::accessible_by_id(&db, &caller.principal, node_id)
                                .into_lua_err()?
                                .into_iter()
                                .collect()
                        }
                        QueryId::Alias(alias) => db::libraries::accessible_by_alias(
                            &db,
                            &caller.principal,
                            alias.as_str(),
                        )
                        .into_lua_err()?,
                    }
                }
            };
            let table = lua.create_table()?;
            for (index, library) in libraries.into_iter().enumerate() {
                table.set(index + 1, library)?;
            }
            return Ok(table);
        }

        let caller = system_caller_at(&lua, plugin_id)?;
        let libraries = match id {
            None => db::libraries::for_system(&db, &caller.system_ctx).into_lua_err()?,
            Some(resolve_id) => {
                let query_id = resolve_id
                    .to_query_id(&db)
                    .into_lua_err()?
                    .ok_or_else(|| mlua::Error::runtime("could not resolve id"))?;
                match query_id {
                    QueryId::Id(node_id) => {
                        db::libraries::for_system_by_id(&db, &caller.system_ctx, node_id)
                            .into_lua_err()?
                            .into_iter()
                            .collect()
                    }
                    QueryId::Alias(alias) => {
                        db::libraries::for_system_by_alias(&db, &caller.system_ctx, alias.as_str())
                            .into_lua_err()?
                    }
                }
            }
        };
        let table = lua.create_table()?;
        for (index, library) in libraries.into_iter().enumerate() {
            table.set(index + 1, library)?;
        }
        Ok(table)
    }

    /// Returns the libraries that contain the given entity (release, artist, track, etc.).
    #[harmony(args(entity_id: NodeId), returns(Vec<Library>))]
    pub(crate) async fn get_for_entity(
        lua: Lua,
        plugin_id: Option<Arc<str>>,
        entity_id: NodeId,
    ) -> Result<Table> {
        let db = STATE.db.read().await;
        let table = lua.create_table()?;
        if let Ok(caller) = request_caller_at(&lua, plugin_id.clone()) {
            let libraries =
                db::libraries::accessible_for_entity(&db, &caller.principal, entity_id.into())
                    .into_lua_err()?;
            for (index, library) in libraries.into_iter().enumerate() {
                table.set(index + 1, library)?;
            }
            return Ok(table);
        }
        let caller = system_caller_at(&lua, plugin_id)?;
        let libraries =
            db::libraries::for_system_for_entity(&db, &caller.system_ctx, entity_id.into())
                .into_lua_err()?;
        for (index, library) in libraries.into_iter().enumerate() {
            table.set(index + 1, library)?;
        }
        Ok(table)
    }

    /// Batch-resolves the first library for each entity. Returns a map of entity db_id → Library.
    #[harmony(args(entity_ids: Vec<u64>), returns(std::collections::BTreeMap<u64, Library>))]
    pub(crate) async fn get_for_entities(
        lua: Lua,
        plugin_id: Option<Arc<str>>,
        entity_ids: Table,
    ) -> Result<Table> {
        let ids = crate::plugins::parse_ids(entity_ids)?;
        let db = STATE.db.read().await;
        let result = lua.create_table()?;
        if let Ok(caller) = request_caller_at(&lua, plugin_id.clone()) {
            let resolved = db::libraries::accessible_for_entities(&db, &caller.principal, &ids)
                .into_lua_err()?;
            for (entity_id, library) in resolved {
                result.set(entity_id.0, library)?;
            }
            return Ok(result);
        }
        let caller = system_caller_at(&lua, plugin_id)?;
        let resolved =
            db::libraries::for_system_for_entities(&db, &caller.system_ctx, &ids).into_lua_err()?;
        for (entity_id, library) in resolved {
            result.set(entity_id.0, library)?;
        }
        Ok(result)
    }
}

crate::plugins::plugin_surface_exports!(
    LibrariesModule,
    "lyra.libraries",
    "Read and modify library configuration.",
    Low
);
