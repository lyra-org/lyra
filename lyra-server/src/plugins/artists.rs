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
        ArtistRelationType,
        ArtistType,
        ResolveId,
    },
    plugins::{
        PluginSortOrder,
        paged_result_to_table,
        parse_ids,
        parse_list_options,
    },
    services::artists::{
        self as artist_services,
        RelationDirection,
        ResolvedRelation,
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

#[harmony_macros::interface]
struct ArtistRelationInfo {
    relation_type: ArtistRelationType,
    direction: String,
    attributes: Option<String>,
    artist: Artist,
}

impl mlua::IntoLua for ArtistRelationInfo {
    fn into_lua(self, lua: &mlua::Lua) -> mlua::Result<mlua::Value> {
        let table = lua.create_table()?;
        table.set("relation_type", self.relation_type)?;
        table.set("direction", self.direction)?;
        table.set("attributes", self.attributes)?;
        table.set("artist", self.artist)?;
        Ok(mlua::Value::Table(table))
    }
}

fn relation_direction_label(direction: RelationDirection) -> &'static str {
    match direction {
        RelationDirection::Incoming => "incoming",
        RelationDirection::Outgoing => "outgoing",
    }
}

fn to_artist_relation_info(relation: ResolvedRelation) -> ArtistRelationInfo {
    ArtistRelationInfo {
        relation_type: relation.relation_type,
        direction: relation_direction_label(relation.direction).to_string(),
        attributes: relation.attributes,
        artist: relation.artist,
    }
}

struct ArtistsModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "Artists",
    local = "artists",
    path = "lyra/artists",
    interfaces(ArtistQueryOptions, ArtistQueryResult, ArtistRelationInfo),
    classes(Artist, ArtistType, ArtistRelationType)
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

    /// Lists typed artist relations for each artist id.
    #[harmony(args(ids: Vec<u64>), returns(std::collections::BTreeMap<u64, Vec<ArtistRelationInfo>>))]
    pub(crate) async fn list_relations_many(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        ids: Table,
    ) -> Result<Table> {
        let ids = parse_ids(ids)?;
        let db = STATE.db.read().await;
        let relations = artist_services::get_relations_many(&db, &ids).into_lua_err()?;
        let table = lua.create_table()?;
        for id in ids {
            let relation_infos = relations
                .get(&id)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(to_artist_relation_info)
                .collect::<Vec<_>>();
            table.set(id.0, relation_infos)?;
        }
        Ok(table)
    }
}

pub(crate) fn get_module() -> harmony_core::Module {
    let mut m = ArtistsModule::module();
    m.scope = harmony_core::Scope {
        id: "lyra.artists".into(),
        description: "Read and modify artist records.",
        danger: harmony_core::Danger::Medium,
    };
    let inner = m.setup;
    m.setup = std::sync::Arc::new(move |lua: &Lua| {
        let table = inner(lua)?;
        table.set("ArtistType", lua.create_proxy::<ArtistType>()?)?;
        table.set(
            "ArtistRelationType",
            lua.create_proxy::<ArtistRelationType>()?,
        )?;
        Ok(table)
    });
    m
}

pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    ArtistsModule::render_luau_definition()
}
