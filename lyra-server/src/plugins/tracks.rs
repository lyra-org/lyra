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
        ResolveId,
        Track,
    },
    plugins::{
        PluginSortOrder,
        paged_result_to_table,
        parse_ids,
        parse_list_options,
    },
};

#[harmony_macros::interface]
struct TrackQueryOptions {
    scope: Option<ResolveId>,
    artist_ids: Option<Vec<u64>>,
    release_artist_ids: Option<Vec<u64>>,
    sort_by: Option<Vec<String>>,
    sort_order: Option<PluginSortOrder>,
    offset: Option<u64>,
    limit: Option<u64>,
    search_term: Option<String>,
}

#[harmony_macros::interface]
struct TrackQueryResult {
    entities: Vec<Track>,
    total_count: u64,
    offset: u64,
}

struct TracksModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "Tracks",
    local = "tracks",
    path = "lyra/tracks",
    interfaces(TrackQueryOptions, TrackQueryResult),
    classes(Track)
)]
impl TracksModule {
    /// Lists tracks related to the given scope, or all tracks by default.
    pub(crate) async fn list(
        _plugin_id: Option<Arc<str>>,
        scope: Option<ResolveId>,
    ) -> Result<Vec<Track>> {
        let resolve_id = scope.unwrap_or_else(|| ResolveId::alias("tracks"));
        let db = STATE.db.read().await;
        let query_id = resolve_id
            .to_query_id(&db)
            .into_lua_err()?
            .ok_or_else(|| mlua::Error::runtime("could not resolve scope"))?;
        let tracks = db::tracks::get(&*db, query_id).into_lua_err()?;

        Ok(tracks)
    }

    /// Queries tracks with pagination, sorting, and optional artist filters.
    #[harmony(args(opts: TrackQueryOptions), returns(TrackQueryResult))]
    pub(crate) async fn query(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        opts: Table,
    ) -> Result<Table> {
        let scope: Option<ResolveId> = opts.get("scope")?;
        let artist_ids: Option<Table> = opts.get("artist_ids")?;
        let release_artist_ids: Option<Table> = opts.get("release_artist_ids")?;
        let list_options = parse_list_options(&opts)?;
        let artist_ids = match artist_ids {
            Some(ids) => parse_ids(ids)?,
            None => Vec::new(),
        };
        let release_artist_ids = match release_artist_ids {
            Some(ids) => parse_ids(ids)?,
            None => Vec::new(),
        };

        let db = STATE.db.read().await;
        let result = if !artist_ids.is_empty() && !release_artist_ids.is_empty() {
            let scope = match scope {
                Some(id) => id.to_query_id(&db).into_lua_err()?,
                None => None,
            };
            db::tracks::query_by_artist_filters(
                &db,
                &artist_ids,
                &release_artist_ids,
                scope,
                &list_options,
            )
            .into_lua_err()?
        } else if !release_artist_ids.is_empty() {
            let scope = match scope {
                Some(id) => id.to_query_id(&db).into_lua_err()?,
                None => None,
            };
            db::tracks::query_by_release_artists(&db, &release_artist_ids, scope, &list_options)
                .into_lua_err()?
        } else if !artist_ids.is_empty() {
            let scope = match scope {
                Some(id) => id.to_query_id(&db).into_lua_err()?,
                None => None,
            };
            db::tracks::query_by_artists(&db, &artist_ids, scope, &list_options).into_lua_err()?
        } else {
            let resolve_id = scope.unwrap_or_else(|| ResolveId::alias("tracks"));
            let query_id = resolve_id
                .to_query_id(&db)
                .into_lua_err()?
                .ok_or_else(|| mlua::Error::runtime("could not resolve scope"))?;
            db::tracks::query(&db, query_id, &list_options).into_lua_err()?
        };
        paged_result_to_table(&lua, result)
    }

    /// Fetches tracks by their own db_ids, returning a map of id → Track.
    #[harmony(args(ids: Vec<u64>), returns(std::collections::BTreeMap<u64, Option<Track>>))]
    pub(crate) async fn get_by_ids(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        ids: Table,
    ) -> Result<Table> {
        let ids = parse_ids(ids)?;
        let db = STATE.db.read().await;
        let tracks = db::tracks::get_by_ids(&db, &ids).into_lua_err()?;
        let table = lua.create_table()?;
        for id in ids {
            if let Some(track) = tracks.get(&id) {
                table.set(id.0, track.clone())?;
            }
        }
        Ok(table)
    }

    /// Lists all tracks belonging to a library.
    pub(crate) async fn list_by_library(
        _plugin_id: Option<Arc<str>>,
        library_id: crate::db::NodeId,
    ) -> Result<Vec<Track>> {
        let db = STATE.db.read().await;
        let tracks = db::tracks::get_by_library(&db, library_id.into()).into_lua_err()?;
        Ok(tracks)
    }

    /// Lists related tracks for each owner id.
    #[harmony(args(ids: Vec<u64>), returns(std::collections::BTreeMap<u64, Vec<Track>>))]
    pub(crate) async fn list_many(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        ids: Table,
    ) -> Result<Table> {
        let ids = parse_ids(ids)?;
        let db = STATE.db.read().await;
        let related = db::tracks::get_direct_many(&db, &ids).into_lua_err()?;
        let table = lua.create_table()?;
        for id in ids {
            let tracks = related.get(&id).cloned().unwrap_or_default();
            table.set(id.0, tracks)?;
        }
        Ok(table)
    }
}

crate::plugins::plugin_surface_exports!(
    TracksModule,
    "lyra.tracks",
    "Read and modify music tracks in the library.",
    Medium
);
