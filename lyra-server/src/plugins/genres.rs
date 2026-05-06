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
    plugins::caller::{
        request_caller,
        system_caller,
    },
    plugins::db::{
        self,
        NodeId,
        Permission,
        genres::{
            ResolveExternalId,
            ResolveGenre,
        },
    },
    services::auth::Principal,
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

enum CallerAccess {
    Request(Principal),
    System,
}

fn caller_access(plugin_id: Option<Arc<str>>) -> Result<CallerAccess> {
    match request_caller(plugin_id.clone()) {
        Ok(caller) => Ok(CallerAccess::Request(caller.principal)),
        Err(_) => {
            system_caller(plugin_id)?;
            Ok(CallerAccess::System)
        }
    }
}

fn request_can_manage_libraries(principal: &Principal) -> bool {
    db::roles::has_permission(&principal.permissions, Permission::ManageLibraries)
}

fn can_read_entity(
    db: &impl db::DbAccess,
    access: &CallerAccess,
    entity_db_id: agdb::DbId,
) -> Result<bool> {
    match access {
        CallerAccess::System => Ok(true),
        CallerAccess::Request(principal) => {
            crate::routes::entity_accessible_to_principal(db, principal, entity_db_id)
                .into_lua_err()
        }
    }
}

fn can_mutate_release(
    db: &impl db::DbAccess,
    access: &CallerAccess,
    release_db_id: agdb::DbId,
) -> Result<bool> {
    match access {
        CallerAccess::System => Ok(true),
        CallerAccess::Request(principal) => {
            if !request_can_manage_libraries(principal) {
                return Ok(false);
            }
            crate::routes::entity_accessible_to_principal(db, principal, release_db_id)
                .into_lua_err()
        }
    }
}

fn can_mutate_global(access: &CallerAccess) -> bool {
    match access {
        CallerAccess::System => true,
        CallerAccess::Request(principal) => request_can_manage_libraries(principal),
    }
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
        plugin_id: Option<Arc<str>>,
        release_id: Value,
        request: Value,
    ) -> Result<NodeId> {
        let access = caller_access(plugin_id)?;
        let release_id: agdb::DbId = lua.from_value::<NodeId>(release_id)?.into();
        let request: GenreAddRequest = crate::plugins::from_lua_json_value(&lua, request)?;

        let mut db = STATE.db.write().await;
        if !can_mutate_release(&*db, &access, release_id)? {
            return Ok(agdb::DbId(0).into());
        }

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
        plugin_id: Option<Arc<str>>,
        request: Value,
    ) -> Result<NodeId> {
        let access = caller_access(plugin_id)?;
        if !can_mutate_global(&access) {
            return Ok(agdb::DbId(0).into());
        }
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
        plugin_id: Option<Arc<str>>,
        child_id: NodeId,
        parent_id: NodeId,
    ) -> Result<()> {
        let access = caller_access(plugin_id)?;
        if !can_mutate_global(&access) {
            return Ok(());
        }
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

    /// Case-insensitive name lookup. Aliases are not consulted; nil on miss.
    #[harmony(returns(Option<GenreInfo>))]
    pub(crate) async fn find_by_name(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        name: String,
    ) -> Result<Value> {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Ok(Value::Nil);
        }
        let db = STATE.db.read().await;
        let Some(db_id) = db::genres::find_by_name(&*db, trimmed).into_lua_err()? else {
            return Ok(Value::Nil);
        };
        let Some(genre) = db::genres::get_by_id(&*db, db_id).into_lua_err()? else {
            return Ok(Value::Nil);
        };
        lua.to_value_with(
            &GenreInfo {
                db_id: genre.db_id,
                id: genre.id,
                name: genre.name,
            },
            crate::plugins::LUA_SERIALIZE_OPTIONS,
        )
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
        plugin_id: Option<Arc<str>>,
        genre_id: NodeId,
    ) -> Result<Vec<NodeId>> {
        let access = caller_access(plugin_id)?;
        let db = STATE.db.read().await;
        let release_ids = db::genres::get_releases(&*db, genre_id.into()).into_lua_err()?;
        let mut visible = Vec::new();
        for release_id in release_ids {
            if can_read_entity(&*db, &access, release_id)? {
                visible.push(release_id.into());
            }
        }
        Ok(visible)
    }

    #[harmony(args(genre_ids: Vec<u64>), returns(std::collections::BTreeMap<u64, Vec<u64>>))]
    pub(crate) async fn get_releases_many(
        _lua: Lua,
        plugin_id: Option<Arc<str>>,
        genre_ids: Table,
    ) -> Result<Table> {
        let access = caller_access(plugin_id)?;
        let ids = crate::plugins::parse_ids(genre_ids)?;
        let db = STATE.db.read().await;
        let result = db::genres::get_releases_many(&*db, &ids).into_lua_err()?;
        let lua = _lua;
        let table = lua.create_table()?;
        for id in ids {
            let release_ids = result.get(&id).cloned().unwrap_or_default();
            let mut release_id_values: Vec<NodeId> = Vec::new();
            for release_id in release_ids {
                if can_read_entity(&*db, &access, release_id)? {
                    release_id_values.push(release_id.into());
                }
            }
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
        plugin_id: Option<Arc<str>>,
        release_id: NodeId,
    ) -> Result<Value> {
        let access = caller_access(plugin_id)?;
        let db = STATE.db.read().await;
        let release_id: agdb::DbId = release_id.into();
        if !can_read_entity(&*db, &access, release_id)? {
            return lua.to_value_with(
                &Vec::<GenreInfo>::new(),
                crate::plugins::LUA_SERIALIZE_OPTIONS,
            );
        }
        let genres = db::genres::get_for_release(&*db, release_id).into_lua_err()?;
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
        plugin_id: Option<Arc<str>>,
        release_ids: Table,
    ) -> Result<Table> {
        let access = caller_access(plugin_id)?;
        let ids = crate::plugins::parse_ids(release_ids)?;
        let db = STATE.db.read().await;
        let result = db::genres::get_for_releases_many(&*db, &ids).into_lua_err()?;
        let table = lua.create_table()?;
        for id in ids {
            if !can_read_entity(&*db, &access, id)? {
                table.set(
                    id.0,
                    lua.to_value_with(
                        &Vec::<GenreInfo>::new(),
                        crate::plugins::LUA_SERIALIZE_OPTIONS,
                    )?,
                )?;
                continue;
            }
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
