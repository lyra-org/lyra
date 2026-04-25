// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use harmony_core::LuaAsyncExt;
use mlua::{
    ExternalResult,
    Lua,
    Result,
    Table,
};

use crate::{
    STATE,
    db::{
        self,
        Artist,
        ArtistType,
        ResolveId,
    },
    plugins::{
        PluginSortOrder,
        paged_result_to_table,
        parse_ids,
        parse_list_options,
    },
};

#[harmony_macros::interface]
struct ArtistQueryOptions {
    scope: Option<ResolveId>,
    sort_by: Option<Vec<String>>,
    sort_order: Option<PluginSortOrder>,
    offset: Option<u64>,
    limit: Option<u64>,
    search_term: Option<String>,
}

#[harmony_macros::interface]
struct ArtistQueryResult {
    entities: Vec<Artist>,
    total_count: u64,
    offset: u64,
}

struct ArtistsModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "Artists",
    local = "artists",
    path = "lyra/artists",
    interfaces(ArtistQueryOptions, ArtistQueryResult),
    classes(Artist, ArtistType)
)]
impl ArtistsModule {
    /// Lists artists related to the given scope, or all artists by default.
    pub(crate) async fn list(
        _plugin_id: Option<Arc<str>>,
        scope: Option<ResolveId>,
    ) -> Result<Vec<Artist>> {
        let resolve_id = scope.unwrap_or_else(|| ResolveId::alias("artists"));
        let db = STATE.db.read().await;
        let query_id = resolve_id
            .to_query_id(&db)
            .into_lua_err()?
            .ok_or_else(|| mlua::Error::runtime("could not resolve scope"))?;
        let artists_list = db::artists::get(&*db, query_id).into_lua_err()?;

        Ok(artists_list)
    }

    /// Queries artists with pagination and sorting options.
    #[harmony(args(opts: ArtistQueryOptions), returns(ArtistQueryResult))]
    pub(crate) async fn query(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        opts: Table,
    ) -> Result<Table> {
        let scope: Option<ResolveId> = opts.get("scope")?;
        let list_options = parse_list_options(&opts)?;

        let resolve_id = scope.unwrap_or_else(|| ResolveId::alias("artists"));
        let db = STATE.db.read().await;
        let query_id = resolve_id
            .to_query_id(&db)
            .into_lua_err()?
            .ok_or_else(|| mlua::Error::runtime("could not resolve scope"))?;
        let result = db::artists::query(&db, query_id, &list_options).into_lua_err()?;
        paged_result_to_table(&lua, result)
    }

    /// Lists all artists belonging to a library (via its releases).
    pub(crate) async fn list_by_library(
        _plugin_id: Option<Arc<str>>,
        library_id: crate::db::NodeId,
    ) -> Result<Vec<Artist>> {
        let db = STATE.db.read().await;
        let artists_list = db::artists::get_by_library(&db, library_id.into()).into_lua_err()?;
        Ok(artists_list)
    }

    /// Lists related artists for each owner id.
    #[harmony(args(ids: Vec<u64>), returns(std::collections::BTreeMap<u64, Vec<Artist>>))]
    pub(crate) async fn list_many(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        ids: Table,
    ) -> Result<Table> {
        let ids = parse_ids(ids)?;
        let db = STATE.db.read().await;
        let related = db::artists::get_many_by_owner(&db, &ids).into_lua_err()?;
        let table = lua.create_table()?;
        for id in ids {
            let artists_list = related.get(&id).cloned().unwrap_or_default();
            table.set(id.0, artists_list)?;
        }
        Ok(table)
    }
}

crate::plugins::plugin_surface_exports!(
    ArtistsModule,
    "lyra.artists",
    "Read and modify artist records.",
    Medium
);
