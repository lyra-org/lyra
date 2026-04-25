// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use harmony_core::LuaAsyncExt;
use mlua::{
    ExternalResult,
    Lua,
    LuaSerdeExt,
    Result,
    Table,
    Value,
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    STATE,
    db::{
        self,
        NodeId,
        genres::{
            ResolveExternalId,
            ResolveGenre,
        },
    },
};

#[derive(Debug, Deserialize)]
#[harmony_macros::interface]
struct GenreExternalId {
    provider_id: String,
    id_type: String,
    id: String,
}

#[derive(Debug, Deserialize)]
#[harmony_macros::interface]
struct GenreAliasInput {
    name: String,
    locale: Option<String>,
}

#[derive(Debug, Deserialize)]
#[harmony_macros::interface]
struct GenreAddRequest {
    name: String,
    external_id: Option<GenreExternalId>,
    aliases: Option<Vec<GenreAliasInput>>,
}

#[derive(Debug, Serialize)]
#[harmony_macros::interface]
struct GenreInfo {
    db_id: Option<NodeId>,
    id: String,
    name: String,
}

// Shared by `add` and `resolve`.
fn resolve_genre_from_request(
    db: &mut agdb::DbAny,
    request: &GenreAddRequest,
) -> anyhow::Result<agdb::DbId> {
    let aliases_owned: Vec<(String, Option<String>)> = request
        .aliases
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|a| (a.name.clone(), a.locale.clone()))
        .collect();
    let aliases_refs: Vec<(&str, Option<&str>)> = aliases_owned
        .iter()
        .map(|(name, locale)| (name.as_str(), locale.as_deref()))
        .collect();

    let ext_id = request.external_id.as_ref().map(|e| ResolveExternalId {
        provider_id: &e.provider_id,
        id_type: &e.id_type,
        id_value: &e.id,
    });

    db::genres::resolve(
        db,
        &ResolveGenre {
            name: &request.name,
            aliases: &aliases_refs,
            external_id: ext_id,
        },
    )
}

struct GenresModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "Genres",
    local = "genres",
    path = "lyra/genres",
    interfaces(GenreExternalId, GenreAliasInput, GenreAddRequest, GenreInfo)
)]
impl GenresModule {
    #[harmony(args(release_id: NodeId, request: GenreAddRequest))]
    pub(crate) async fn add(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        release_id: Value,
        request: Value,
    ) -> Result<NodeId> {
        let release_id: agdb::DbId = lua.from_value::<NodeId>(release_id)?.into();
        let request: GenreAddRequest = crate::plugins::from_lua_json_value(&lua, request)?;

        let mut db = STATE.db.write().await;

        let is_locked = db::releases::get_by_id(&db, release_id)
            .into_lua_err()?
            .is_some_and(|a| a.locked.unwrap_or(false));

        let genre_id = resolve_genre_from_request(&mut db, &request).into_lua_err()?;

        if !is_locked {
            db::genres::link_to_release(&mut db, genre_id, release_id).into_lua_err()?;
        }

        Ok(genre_id.into())
    }

    /// Resolve or create a genre without linking it to a release.
    /// Returns the genre's db_id. Useful for registering parent genres
    /// that exist in the hierarchy but aren't directly tagged on a release.
    #[harmony(args(request: GenreAddRequest))]
    pub(crate) async fn resolve(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        request: Value,
    ) -> Result<NodeId> {
        let request: GenreAddRequest = crate::plugins::from_lua_json_value(&lua, request)?;

        let mut db = STATE.db.write().await;
        let genre_id = resolve_genre_from_request(&mut db, &request).into_lua_err()?;

        Ok(genre_id.into())
    }

    /// Link a child genre to a parent genre. Additive — does not remove
    /// existing parents. Rejects self-links and direct cycles.
    #[harmony(args(child_id: NodeId, parent_id: NodeId))]
    pub(crate) async fn add_parent(
        _lua: Lua,
        _plugin_id: Option<Arc<str>>,
        child_id: NodeId,
        parent_id: NodeId,
    ) -> Result<()> {
        let mut db = STATE.db.write().await;
        db::genres::link_to_parent(&mut db, child_id.into(), parent_id.into()).into_lua_err()?;
        Ok(())
    }

    #[harmony(returns(Option<GenreInfo>))]
    pub(crate) async fn get_by_id(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        genre_id: NodeId,
    ) -> Result<Value> {
        let db = STATE.db.read().await;
        let genre = db::genres::get_by_id(&*db, genre_id.into()).into_lua_err()?;
        match genre {
            Some(g) => lua.to_value_with(
                &GenreInfo {
                    db_id: g.db_id,
                    id: g.id,
                    name: g.name,
                },
                crate::plugins::LUA_SERIALIZE_OPTIONS,
            ),
            None => Ok(Value::Nil),
        }
    }

    #[harmony(returns(Vec<GenreInfo>))]
    pub(crate) async fn get_parents(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        genre_id: NodeId,
    ) -> Result<Value> {
        let db = STATE.db.read().await;
        let genres = db::genres::get_parents(&*db, genre_id.into()).into_lua_err()?;
        let infos: Vec<GenreInfo> = genres
            .into_iter()
            .map(|g| GenreInfo {
                db_id: g.db_id,
                id: g.id,
                name: g.name,
            })
            .collect();
        lua.to_value_with(&infos, crate::plugins::LUA_SERIALIZE_OPTIONS)
    }

    #[harmony(returns(Vec<GenreInfo>))]
    pub(crate) async fn get_children(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        genre_id: NodeId,
    ) -> Result<Value> {
        let db = STATE.db.read().await;
        let genres = db::genres::get_children(&*db, genre_id.into()).into_lua_err()?;
        let infos: Vec<GenreInfo> = genres
            .into_iter()
            .map(|g| GenreInfo {
                db_id: g.db_id,
                id: g.id,
                name: g.name,
            })
            .collect();
        lua.to_value_with(&infos, crate::plugins::LUA_SERIALIZE_OPTIONS)
    }

    #[harmony(returns(Vec<u64>))]
    pub(crate) async fn get_releases(
        _lua: Lua,
        _plugin_id: Option<Arc<str>>,
        genre_id: NodeId,
    ) -> Result<Vec<NodeId>> {
        let db = STATE.db.read().await;
        let release_ids = db::genres::get_releases(&*db, genre_id.into()).into_lua_err()?;
        Ok(release_ids.into_iter().map(NodeId::from).collect())
    }

    #[harmony(args(genre_ids: Vec<u64>), returns(std::collections::BTreeMap<u64, Vec<u64>>))]
    pub(crate) async fn get_releases_many(
        _lua: Lua,
        _plugin_id: Option<Arc<str>>,
        genre_ids: Table,
    ) -> Result<Table> {
        let ids = crate::plugins::parse_ids(genre_ids)?;
        let db = STATE.db.read().await;
        let result = db::genres::get_releases_many(&*db, &ids).into_lua_err()?;
        let lua = _lua;
        let table = lua.create_table()?;
        for id in ids {
            let release_ids = result.get(&id).cloned().unwrap_or_default();
            let release_id_values: Vec<NodeId> =
                release_ids.into_iter().map(NodeId::from).collect();
            table.set(
                id.0,
                lua.to_value_with(&release_id_values, crate::plugins::LUA_SERIALIZE_OPTIONS)?,
            )?;
        }
        Ok(table)
    }

    #[harmony(returns(Vec<GenreInfo>))]
    pub(crate) async fn get_for_release(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        release_id: NodeId,
    ) -> Result<Value> {
        let db = STATE.db.read().await;
        let genres = db::genres::get_for_release(&*db, release_id.into()).into_lua_err()?;
        let infos: Vec<GenreInfo> = genres
            .into_iter()
            .map(|g| GenreInfo {
                db_id: g.db_id,
                id: g.id,
                name: g.name,
            })
            .collect();
        lua.to_value_with(&infos, crate::plugins::LUA_SERIALIZE_OPTIONS)
    }

    #[harmony(args(release_ids: Vec<u64>), returns(std::collections::BTreeMap<u64, Vec<GenreInfo>>))]
    pub(crate) async fn get_for_releases_many(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        release_ids: Table,
    ) -> Result<Table> {
        let ids = crate::plugins::parse_ids(release_ids)?;
        let db = STATE.db.read().await;
        let result = db::genres::get_for_releases_many(&*db, &ids).into_lua_err()?;
        let table = lua.create_table()?;
        for id in ids {
            let genres = result.get(&id).cloned().unwrap_or_default();
            let infos: Vec<GenreInfo> = genres
                .into_iter()
                .map(|g| GenreInfo {
                    db_id: g.db_id,
                    id: g.id,
                    name: g.name,
                })
                .collect();
            table.set(
                id.0,
                lua.to_value_with(&infos, crate::plugins::LUA_SERIALIZE_OPTIONS)?,
            )?;
        }
        Ok(table)
    }
}

crate::plugins::plugin_surface_exports!(
    GenresModule,
    "lyra.genres",
    "Read and modify genre records.",
    Low
);
