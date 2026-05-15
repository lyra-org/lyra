// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbId,
    QueryId,
};
use std::fmt;

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
    plugins::db::{
        self,
        NodeId,
        ResolveId,
    },
    plugins::{
        LUA_SERIALIZE_OPTIONS,
        caller::RequestCaller,
        from_lua_json_value,
    },
    services::auth::Principal,
    services::playlists as playlist_service,
};

#[derive(Serialize)]
#[harmony_macros::interface]
struct PlaylistInfo {
    db_id: Option<NodeId>,
    id: String,
    name: String,
    description: Option<String>,
    is_public: Option<bool>,
    created_at: Option<u64>,
    updated_at: Option<u64>,
}

#[harmony_macros::interface]
#[derive(Clone, Debug, Serialize)]
struct PlaylistTrackLink {
    entry_id: NodeId,
    track_id: NodeId,
    position: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[harmony_macros::interface]
struct PlaylistCreateRequest {
    user_id: NodeId,
    name: String,
    description: Option<String>,
    is_public: Option<bool>,
    created_at: Option<u64>,
    updated_at: Option<u64>,
}

impl From<PlaylistCreateRequest> for playlist_service::CreatePlaylistRequest {
    fn from(request: PlaylistCreateRequest) -> Self {
        Self {
            user_db_id: request.user_id.into(),
            name: request.name,
            description: request.description,
            is_public: request.is_public,
            created_at: request.created_at,
            updated_at: request.updated_at,
        }
    }
}

#[derive(Debug, Deserialize)]
#[harmony_macros::interface]
struct PlaylistUpdateRequest {
    playlist_id: ResolveId,
    name: Option<String>,
    description: Option<String>,
    is_public: Option<bool>,
    updated_at: Option<u64>,
}

impl PlaylistUpdateRequest {
    fn into_service_request(self, playlist_id: QueryId) -> playlist_service::UpdatePlaylistRequest {
        playlist_service::UpdatePlaylistRequest {
            playlist_id,
            name: self.name,
            description: self.description,
            is_public: self.is_public,
            updated_at: self.updated_at,
        }
    }
}

fn serialize_value<T: Serialize>(lua: &Lua, value: &T) -> Result<Value> {
    lua.to_value_with(value, LUA_SERIALIZE_OPTIONS)
}

fn playlist_owned_by_principal(
    db: &impl db::DbAccess,
    principal: &Principal,
    playlist_db_id: DbId,
) -> Result<bool> {
    Ok(db::playlists::get_owner(db, playlist_db_id).into_lua_err()? == Some(principal.user_db_id))
}

fn playlist_link_to_info(
    db: &impl db::DbAccess,
    principal: &Principal,
    link: playlist_service::PlaylistTrackLink,
) -> Result<Option<PlaylistTrackLink>> {
    if !crate::routes::entity_accessible_to_principal(db, principal, link.track_db_id)
        .into_lua_err()?
    {
        return Ok(None);
    }
    Ok(Some(PlaylistTrackLink {
        entry_id: link.entry_db_id.into(),
        track_id: link.track_db_id.into(),
        position: link.position,
    }))
}

/// Lists all playlists.
pub(crate) async fn list(lua: Lua, caller: RequestCaller, _args: ()) -> Result<Value> {
    let db = STATE.db.read().await;
    let playlists = playlist_service::list(&db)
        .into_lua_err()?
        .into_iter()
        .filter(|playlist| {
            let Some(playlist_db_id) = playlist.db_id.clone().map(DbId::from) else {
                return false;
            };
            crate::routes::playlist_accessible_to_principal(&*db, &caller.principal, playlist_db_id)
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    serialize_value(&lua, &playlists)
}

/// Returns a playlist by id (alias or db_id).
pub(crate) async fn get_by_id(lua: Lua, caller: RequestCaller, id: ResolveId) -> Result<Value> {
    let db = STATE.db.read().await;
    let query_id = id
        .to_query_id(&db)
        .into_lua_err()?
        .ok_or_else(|| mlua::Error::runtime("could not resolve id"))?;
    let playlist = playlist_service::get(&db, query_id).into_lua_err()?;
    match playlist {
        Some(playlist) => {
            let Some(playlist_db_id) = playlist.db_id.clone().map(DbId::from) else {
                return Ok(Value::Nil);
            };
            if !crate::routes::playlist_accessible_to_principal(
                &*db,
                &caller.principal,
                playlist_db_id,
            )
            .into_lua_err()?
            {
                return Ok(Value::Nil);
            }
            serialize_value(&lua, &playlist)
        }
        None => Ok(Value::Nil),
    }
}

/// Lists playlists owned by a user.
pub(crate) async fn get_by_user(lua: Lua, caller: RequestCaller, user_id: NodeId) -> Result<Value> {
    let owner_db_id: DbId = user_id.into();
    if owner_db_id != caller.principal.user_db_id {
        return serialize_value(&lua, &Vec::<PlaylistInfo>::new());
    }
    let db = STATE.db.read().await;
    let playlists = playlist_service::get_by_user(&db, owner_db_id).into_lua_err()?;
    serialize_value(&lua, &playlists)
}

/// Returns the owner id for a playlist.
pub(crate) async fn get_owner(
    _lua: Lua,
    caller: RequestCaller,
    playlist_id: ResolveId,
) -> Result<Option<NodeId>> {
    let db = STATE.db.read().await;
    let Some(playlist_id) = playlist_id.to_query_id(&db).into_lua_err()? else {
        return Ok(None);
    };
    let QueryId::Id(playlist_db_id) = playlist_id else {
        return Ok(None);
    };
    if !crate::routes::playlist_accessible_to_principal(&*db, &caller.principal, playlist_db_id)
        .into_lua_err()?
    {
        return Ok(None);
    }
    let owner_id = playlist_service::get_owner(&db, QueryId::Id(playlist_db_id))
        .into_lua_err()?
        .map(NodeId::from);
    Ok(owner_id)
}

/// Lists track links for a playlist.
pub(crate) async fn get_tracks(
    lua: Lua,
    caller: RequestCaller,
    playlist_id: ResolveId,
) -> Result<Value> {
    let db = STATE.db.read().await;
    let playlist_id = playlist_id
        .to_query_id(&db)
        .into_lua_err()?
        .ok_or_else(|| mlua::Error::runtime("could not resolve playlist id"))?;
    let QueryId::Id(playlist_db_id) = playlist_id else {
        return serialize_value(&lua, &Vec::<PlaylistTrackLink>::new());
    };
    if !crate::routes::playlist_accessible_to_principal(&*db, &caller.principal, playlist_db_id)
        .into_lua_err()?
    {
        return serialize_value(&lua, &Vec::<PlaylistTrackLink>::new());
    }
    let links = playlist_service::get_tracks(&db, QueryId::Id(playlist_db_id)).into_lua_err()?;

    let mut links_out = Vec::with_capacity(links.len());
    for link in links {
        if let Some(link) = playlist_link_to_info(&*db, &caller.principal, link)? {
            links_out.push(link);
        }
    }

    serialize_value(&lua, &links_out)
}

pub(crate) async fn get_tracks_many(
    lua: Lua,
    caller: RequestCaller,
    playlist_ids: Table,
) -> Result<Table> {
    let ids = crate::plugins::parse_ids(playlist_ids)?;
    let db = STATE.db.read().await;
    let result = playlist_service::get_tracks_many(&db, &ids).into_lua_err()?;
    let table = lua.create_table()?;
    for id in ids {
        if !crate::routes::playlist_accessible_to_principal(&*db, &caller.principal, id)
            .into_lua_err()?
        {
            table.set(
                id.0,
                serialize_value(&lua, &Vec::<PlaylistTrackLink>::new())?,
            )?;
            continue;
        }
        let links = result.get(&id).cloned().unwrap_or_default();
        let mut track_links = Vec::with_capacity(links.len());
        for link in links {
            if let Some(link) = playlist_link_to_info(&*db, &caller.principal, link)? {
                track_links.push(link);
            }
        }
        table.set(id.0, serialize_value(&lua, &track_links)?)?;
    }
    Ok(table)
}

pub(crate) async fn create(_lua: Lua, caller: RequestCaller, request: Table) -> Result<NodeId> {
    let request: PlaylistCreateRequest = from_lua_json_value(&_lua, Value::Table(request))?;
    if DbId::from(request.user_id.clone()) != caller.principal.user_db_id {
        return Err(mlua::Error::runtime("user not found"));
    }
    let request: playlist_service::CreatePlaylistRequest = request.into();

    let mut db = STATE.db.write().await;
    let playlist_id = playlist_service::create(&mut db, &request).into_lua_err()?;
    Ok(playlist_id.into())
}

pub(crate) async fn update(lua: Lua, caller: RequestCaller, request: Table) -> Result<Value> {
    let request: PlaylistUpdateRequest = from_lua_json_value(&lua, Value::Table(request))?;
    let mut db = STATE.db.write().await;
    let playlist_id = request
        .playlist_id
        .to_query_id(&db)
        .into_lua_err()?
        .ok_or_else(|| mlua::Error::runtime("could not resolve playlist id"))?;
    let QueryId::Id(playlist_db_id) = playlist_id else {
        return Ok(Value::Nil);
    };
    if !playlist_owned_by_principal(&*db, &caller.principal, playlist_db_id)? {
        return Ok(Value::Nil);
    }
    let request = request.into_service_request(QueryId::Id(playlist_db_id));
    let playlist = playlist_service::update(&mut db, &request).into_lua_err()?;
    match playlist {
        Some(playlist) => serialize_value(&lua, &playlist),
        None => Ok(Value::Nil),
    }
}

/// Adds a track to a playlist and returns the entry node id.
pub(crate) async fn add_track(
    caller: RequestCaller,
    playlist_id: ResolveId,
    track_id: ResolveId,
) -> Result<NodeId> {
    let mut db = STATE.db.write().await;
    let playlist_id = playlist_id
        .to_query_id(&db)
        .into_lua_err()?
        .ok_or_else(|| mlua::Error::runtime("could not resolve playlist id"))?;
    let QueryId::Id(playlist_db_id) = playlist_id else {
        return Err(mlua::Error::runtime("could not resolve playlist id"));
    };
    if !playlist_owned_by_principal(&*db, &caller.principal, playlist_db_id)? {
        return Err(mlua::Error::runtime("playlist not found"));
    }
    let track_id = track_id
        .to_query_id(&db)
        .into_lua_err()?
        .ok_or_else(|| mlua::Error::runtime("could not resolve track id"))?;
    let QueryId::Id(track_db_id) = track_id else {
        return Err(mlua::Error::runtime("could not resolve track id"));
    };
    if !crate::routes::entity_accessible_to_principal(&*db, &caller.principal, track_db_id)
        .into_lua_err()?
    {
        return Err(mlua::Error::runtime("track not found"));
    }
    let pt = playlist_service::add_track(
        &mut db,
        QueryId::Id(playlist_db_id),
        QueryId::Id(track_db_id),
    )
    .into_lua_err()?;
    Ok(pt.edge_id.into())
}

/// Removes a track entry from a playlist.
pub(crate) async fn remove_track(
    _lua: Lua,
    caller: RequestCaller,
    entry_id: ResolveId,
) -> Result<()> {
    let mut db = STATE.db.write().await;
    let entry_id = entry_id
        .to_query_id(&db)
        .into_lua_err()?
        .ok_or_else(|| mlua::Error::runtime("could not resolve entry id"))?;
    let QueryId::Id(entry_db_id) = entry_id else {
        return Err(mlua::Error::runtime("could not resolve entry id"));
    };
    let Some(playlist_db_id) =
        playlist_service::get_playlist_for_entry(&db, entry_db_id).into_lua_err()?
    else {
        return Ok(());
    };
    if !playlist_owned_by_principal(&*db, &caller.principal, playlist_db_id)? {
        return Ok(());
    }
    playlist_service::remove_track(&mut db, QueryId::Id(entry_db_id)).into_lua_err()?;
    Ok(())
}
fn module_export() -> ModuleExport {
    ModuleBuilder::empty("lyra/playlists", "Playlists", "playlists")
        .scope_id(
            "lyra.playlists",
            "Read and modify playlists.",
            Danger::Medium,
        )
        .with_interface(PlaylistInfo::interface_descriptor())
        .with_interface(PlaylistTrackLink::interface_descriptor())
        .with_interface(PlaylistCreateRequest::interface_descriptor())
        .with_interface(PlaylistUpdateRequest::interface_descriptor())
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("list")
                .description("Lists all playlists.")
                .returns::<Vec<PlaylistInfo>>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |lua, caller, args: ()| async move {
                let caller = caller?;
                list(lua, caller, args).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("get_by_id")
                .description("Returns a playlist by id (alias or db_id).")
                .param_type::<ResolveId>("id")
                .returns::<Option<PlaylistInfo>>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |lua, caller, id| async move {
                let caller = caller?;
                get_by_id(lua, caller, id).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("get_by_user")
                .description("Lists playlists owned by a user.")
                .param_type::<NodeId>("user_id")
                .returns::<Vec<PlaylistInfo>>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |lua, caller, user_id| async move {
                let caller = caller?;
                get_by_user(lua, caller, user_id).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("get_owner")
                .description("Returns the owner id for a playlist.")
                .param_type::<ResolveId>("playlist_id")
                .returns::<Option<NodeId>>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |lua, caller, playlist_id| async move {
                let caller = caller?;
                get_owner(lua, caller, playlist_id).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("get_tracks")
                .description("Lists track links for a playlist.")
                .param_type::<ResolveId>("playlist_id")
                .returns::<Vec<PlaylistTrackLink>>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |lua, caller, playlist_id| async move {
                let caller = caller?;
                get_tracks(lua, caller, playlist_id).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("get_tracks_many")
                .param_type::<Vec<u64>>("playlist_ids")
                .returns::<std::collections::BTreeMap<u64, Vec<PlaylistTrackLink>>>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |lua, caller, playlist_ids| async move {
                let caller = caller?;
                get_tracks_many(lua, caller, playlist_ids).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("create")
                .param_type::<PlaylistCreateRequest>("request")
                .returns::<NodeId>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |lua, caller, request| async move {
                let caller = caller?;
                create(lua, caller, request).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("update")
                .param_type::<PlaylistUpdateRequest>("request")
                .returns::<Option<PlaylistInfo>>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |lua, caller, request| async move {
                let caller = caller?;
                update(lua, caller, request).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("add_track")
                .description("Adds a track to a playlist and returns the entry node id.")
                .param_type::<ResolveId>("playlist_id")
                .param_type::<ResolveId>("track_id")
                .returns::<NodeId>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |_, caller, (playlist_id, track_id)| async move {
                let caller = caller?;
                add_track(caller, playlist_id, track_id).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("remove_track")
                .description("Removes a track entry from a playlist.")
                .param_type::<ResolveId>("entry_id")
                .returns::<()>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |lua, caller, entry_id| async move {
                let caller = caller?;
                remove_track(lua, caller, entry_id).await
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
