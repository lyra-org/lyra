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
    db::NodeId,
    db::ResolveId,
    db::{
        self,
        Library as DbLibrary,
    },
};

#[harmony_macros::interface]
struct Library {
    db_id: Option<NodeId>,
    id: String,
    name: String,
    directory: String,
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
        _plugin_id: Option<Arc<str>>,
        id: Option<ResolveId>,
    ) -> Result<Vec<DbLibrary>> {
        let db = STATE.db.read().await;

        let libraries = match id {
            None => db::libraries::get(&db).into_lua_err()?,
            Some(resolve_id) => {
                let query_id = resolve_id
                    .to_query_id(&db)
                    .into_lua_err()?
                    .ok_or_else(|| mlua::Error::runtime("could not resolve id"))?;
                match query_id {
                    QueryId::Id(node_id) => db::libraries::get_by_id(&db, node_id)
                        .into_lua_err()?
                        .into_iter()
                        .collect(),
                    QueryId::Alias(alias) => {
                        db::libraries::get_by_alias(&db, alias.as_str()).into_lua_err()?
                    }
                }
            }
        };

        Ok(libraries)
    }

    /// Returns the libraries that contain the given entity (release, artist, track, etc.).
    #[harmony(args(entity_id: NodeId), returns(Vec<Library>))]
    pub(crate) async fn get_for_entity(
        _plugin_id: Option<Arc<str>>,
        entity_id: NodeId,
    ) -> Result<Vec<DbLibrary>> {
        let db = STATE.db.read().await;
        db::libraries::get_for_entity(&db, entity_id.into()).into_lua_err()
    }

    /// Batch-resolves the first library for each entity. Returns a map of entity db_id → Library.
    #[harmony(args(entity_ids: Vec<u64>), returns(std::collections::BTreeMap<u64, Library>))]
    pub(crate) async fn get_for_entities(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        entity_ids: Table,
    ) -> Result<Table> {
        let ids = crate::plugins::parse_ids(entity_ids)?;
        let db = STATE.db.read().await;
        let resolved = db::libraries::get_for_entities(&db, &ids).into_lua_err()?;
        let result = lua.create_table()?;
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
