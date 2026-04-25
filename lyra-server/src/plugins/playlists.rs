// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use agdb::{
    DbId,
    QueryId,
};
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
        NodeId,
        ResolveId,
    },
    plugins::{
        LUA_SERIALIZE_OPTIONS,
        from_lua_json_value,
    },
    services::playlists as playlist_service,
};

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

struct PlaylistsModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "Playlists",
    local = "playlists",
    path = "lyra/playlists",
    interfaces(
        PlaylistInfo,
        PlaylistTrackLink,
        PlaylistCreateRequest,
        PlaylistUpdateRequest
    )
)]
impl PlaylistsModule {
    /// Lists all playlists.
    #[harmony(path = "list", args(), returns(Vec<PlaylistInfo>))]
    pub(crate) async fn list(lua: Lua, _plugin_id: Option<Arc<str>>, _args: ()) -> Result<Value> {
        let db = STATE.db.read().await;
        let playlists = playlist_service::list(&db).into_lua_err()?;
        serialize_value(&lua, &playlists)
    }

    /// Returns a playlist by id (alias or db_id).
    #[harmony(path = "get_by_id", args(id: ResolveId), returns(Option<PlaylistInfo>))]
    pub(crate) async fn get_by_id(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        id: ResolveId,
    ) -> Result<Value> {
        let db = STATE.db.read().await;
        let query_id = id
            .to_query_id(&db)
            .into_lua_err()?
            .ok_or_else(|| mlua::Error::runtime("could not resolve id"))?;
        let playlist = playlist_service::get(&db, query_id).into_lua_err()?;
        match playlist {
            Some(playlist) => serialize_value(&lua, &playlist),
            None => Ok(Value::Nil),
        }
    }

    /// Lists playlists owned by a user.
    #[harmony(returns(Vec<PlaylistInfo>))]
    pub(crate) async fn get_by_user(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        user_id: NodeId,
    ) -> Result<Value> {
        let db = STATE.db.read().await;
        let owner_db_id: DbId = user_id.into();
        let playlists = playlist_service::get_by_user(&db, owner_db_id).into_lua_err()?;
        serialize_value(&lua, &playlists)
    }

    /// Returns the owner id for a playlist.
    pub(crate) async fn get_owner(
        _lua: Lua,
        _plugin_id: Option<Arc<str>>,
        playlist_id: ResolveId,
    ) -> Result<Option<NodeId>> {
        let db = STATE.db.read().await;
        let Some(playlist_id) = playlist_id.to_query_id(&db).into_lua_err()? else {
            return Ok(None);
        };
        let owner_id = playlist_service::get_owner(&db, playlist_id)
            .into_lua_err()?
            .map(NodeId::from);
        Ok(owner_id)
    }

    /// Lists track links for a playlist.
    #[harmony(returns(Vec<PlaylistTrackLink>))]
    pub(crate) async fn get_tracks(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        playlist_id: ResolveId,
    ) -> Result<Value> {
        let db = STATE.db.read().await;
        let playlist_id = playlist_id
            .to_query_id(&db)
            .into_lua_err()?
            .ok_or_else(|| mlua::Error::runtime("could not resolve playlist id"))?;
        let links = playlist_service::get_tracks(&db, playlist_id).into_lua_err()?;

        let links: Vec<PlaylistTrackLink> = links
            .into_iter()
            .map(|link| PlaylistTrackLink {
                entry_id: link.entry_db_id.into(),
                track_id: link.track_db_id.into(),
                position: link.position,
            })
            .collect();

        serialize_value(&lua, &links)
    }

    #[harmony(args(playlist_ids: Vec<u64>), returns(std::collections::BTreeMap<u64, Vec<PlaylistTrackLink>>))]
    pub(crate) async fn get_tracks_many(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        playlist_ids: Table,
    ) -> Result<Table> {
        let ids = crate::plugins::parse_ids(playlist_ids)?;
        let db = STATE.db.read().await;
        let result = playlist_service::get_tracks_many(&db, &ids).into_lua_err()?;
        let table = lua.create_table()?;
        for id in ids {
            let links = result.get(&id).cloned().unwrap_or_default();
            let track_links: Vec<PlaylistTrackLink> = links
                .into_iter()
                .map(|link| PlaylistTrackLink {
                    entry_id: link.entry_db_id.into(),
                    track_id: link.track_db_id.into(),
                    position: link.position,
                })
                .collect();
            table.set(id.0, serialize_value(&lua, &track_links)?)?;
        }
        Ok(table)
    }

    #[harmony(args(request: PlaylistCreateRequest))]
    pub(crate) async fn create(
        _lua: Lua,
        _plugin_id: Option<Arc<str>>,
        request: Table,
    ) -> Result<NodeId> {
        let request: PlaylistCreateRequest = from_lua_json_value(&_lua, Value::Table(request))?;
        let request: playlist_service::CreatePlaylistRequest = request.into();

        let mut db = STATE.db.write().await;
        let playlist_id = playlist_service::create(&mut db, &request).into_lua_err()?;
        Ok(playlist_id.into())
    }

    #[harmony(args(request: PlaylistUpdateRequest), returns(Option<PlaylistInfo>))]
    pub(crate) async fn update(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        request: Table,
    ) -> Result<Value> {
        let request: PlaylistUpdateRequest = from_lua_json_value(&lua, Value::Table(request))?;
        let mut db = STATE.db.write().await;
        let playlist_id = request
            .playlist_id
            .to_query_id(&db)
            .into_lua_err()?
            .ok_or_else(|| mlua::Error::runtime("could not resolve playlist id"))?;
        let request = request.into_service_request(playlist_id);
        let playlist = playlist_service::update(&mut db, &request).into_lua_err()?;
        match playlist {
            Some(playlist) => serialize_value(&lua, &playlist),
            None => Ok(Value::Nil),
        }
    }

    /// Adds a track to a playlist and returns the entry node id.
    pub(crate) async fn add_track(
        _plugin_id: Option<Arc<str>>,
        playlist_id: ResolveId,
        track_id: ResolveId,
    ) -> Result<NodeId> {
        let mut db = STATE.db.write().await;
        let playlist_id = playlist_id
            .to_query_id(&db)
            .into_lua_err()?
            .ok_or_else(|| mlua::Error::runtime("could not resolve playlist id"))?;
        let track_id = track_id
            .to_query_id(&db)
            .into_lua_err()?
            .ok_or_else(|| mlua::Error::runtime("could not resolve track id"))?;
        let pt = playlist_service::add_track(&mut db, playlist_id, track_id).into_lua_err()?;
        Ok(pt.edge_id.into())
    }

    /// Removes a track entry from a playlist.
    pub(crate) async fn remove_track(
        _lua: Lua,
        _plugin_id: Option<Arc<str>>,
        entry_id: ResolveId,
    ) -> Result<()> {
        let mut db = STATE.db.write().await;
        let entry_id = entry_id
            .to_query_id(&db)
            .into_lua_err()?
            .ok_or_else(|| mlua::Error::runtime("could not resolve entry id"))?;
        playlist_service::remove_track(&mut db, entry_id).into_lua_err()?;
        Ok(())
    }
}

crate::plugins::plugin_surface_exports!(
    PlaylistsModule,
    "lyra.playlists",
    "Read and modify playlists.",
    Medium
);
