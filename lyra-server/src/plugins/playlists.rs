// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashSet,
    sync::Arc,
};

use agdb::{
    DbAny,
    DbId,
    QueryId,
};
use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
};
#[cfg(feature = "docgen")]
use harmony_luau::{
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
};
use serde::Serialize;

use crate::{
    plugins::db::{
        self,
        DbAsync,
        ResolveId,
    },
    services::{
        auth::Principal,
        playlists as playlist_service,
    },
};

#[derive(Clone, Default)]
pub(crate) struct PlaylistsModuleStore {
    db: Option<DbAsync>,
}

impl PlaylistsModuleStore {
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: DbAsync) -> Self {
        Self { db: Some(db) }
    }

    fn db(&self) -> luau::runtime::Result<DbAsync> {
        self.db.clone().ok_or_else(|| {
            crate::plugins::runtime_error(
                "lyra/playlists requires a database-backed plugin executor",
            )
        })
    }
}

struct PlaylistsModule;

#[derive(Serialize)]
struct PlaylistInfo {
    db_id: Option<i64>,
    id: String,
    name: String,
    description: Option<String>,
    is_public: Option<bool>,
    created_at: Option<u64>,
    updated_at: Option<u64>,
}

impl From<db::Playlist> for PlaylistInfo {
    fn from(playlist: db::Playlist) -> Self {
        Self {
            db_id: playlist.db_id.map(|id| DbId::from(id).0),
            id: playlist.id,
            name: playlist.name,
            description: playlist.description,
            is_public: playlist.is_public,
            created_at: playlist.created_at,
            updated_at: playlist.updated_at,
        }
    }
}

#[derive(Clone, Serialize)]
struct PlaylistTrackLink {
    entry_id: String,
    track_id: i64,
    position: u64,
}

impl From<playlist_service::PlaylistTrackLink> for PlaylistTrackLink {
    fn from(link: playlist_service::PlaylistTrackLink) -> Self {
        Self {
            entry_id: link.entry_id,
            track_id: link.track_db_id.0,
            position: link.position,
        }
    }
}

struct PlaylistCreateRequest {
    name: String,
    description: Option<String>,
    is_public: Option<bool>,
    created_at: Option<u64>,
    updated_at: Option<u64>,
}

impl PlaylistCreateRequest {
    fn into_service_request(self, user_db_id: DbId) -> playlist_service::CreatePlaylistRequest {
        playlist_service::CreatePlaylistRequest {
            user_db_id,
            name: self.name,
            description: self.description,
            is_public: self.is_public,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

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
            // A Luau table cannot express "present but nil", so this surface
            // can only set a description, never clear it.
            description: self.description.map(Some),
            is_public: self.is_public,
            updated_at: self.updated_at,
        }
    }
}

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/playlists")
        .capability("lyra.playlists")
        .function(list_spec())
        .function(get_by_id_spec())
        .function(list_owned_spec())
        .function(get_owner_spec())
        .function(get_tracks_spec())
        .function(get_tracks_many_spec())
        .function(create_spec())
        .function(update_spec())
        .function(delete_spec())
        .function(add_track_spec())
        .function(remove_track_spec())
        .function(move_track_spec())
        .install(|_| Ok(ModuleExport::new(PlaylistsModule)))
}

fn list_spec() -> FunctionSpec {
    FunctionSpec::async_fn("list")
        .context::<crate::plugins::auth::DispatchAuth>()
        .returns::<Vec<PlaylistInfo>>()
        .call_async(Arc::new(list_callback))
}

fn get_by_id_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_by_id")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("id")
        .args::<ResolveId>()
        .returns::<Option<PlaylistInfo>>()
        .call_async(Arc::new(get_by_id_callback))
}

fn list_owned_spec() -> FunctionSpec {
    FunctionSpec::async_fn("list_owned")
        .context::<crate::plugins::auth::DispatchAuth>()
        .returns::<Vec<PlaylistInfo>>()
        .call_async(Arc::new(list_owned_callback))
}

fn get_owner_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_owner")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("playlist_id")
        .args::<ResolveId>()
        .returns::<Option<String>>()
        .call_async(Arc::new(get_owner_callback))
}

fn get_tracks_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_tracks")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("playlist_id")
        .args::<ResolveId>()
        .returns::<Vec<PlaylistTrackLink>>()
        .call_async(Arc::new(get_tracks_callback))
}

fn get_tracks_many_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_tracks_many")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("playlist_ids")
        .args::<luau::Table>()
        .returns::<luau::Table>()
        .call_async(Arc::new(get_tracks_many_callback))
}

fn create_spec() -> FunctionSpec {
    FunctionSpec::async_fn("create")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("request")
        .args::<PlaylistCreateRequest>()
        .returns::<i64>()
        .call_async(Arc::new(create_callback))
}

fn update_spec() -> FunctionSpec {
    FunctionSpec::async_fn("update")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("request")
        .args::<PlaylistUpdateRequest>()
        .returns::<Option<PlaylistInfo>>()
        .call_async(Arc::new(update_callback))
}

fn delete_spec() -> FunctionSpec {
    FunctionSpec::async_fn("delete")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("playlist_id")
        .args::<ResolveId>()
        .returns::<bool>()
        .call_async(Arc::new(delete_callback))
}

fn add_track_spec() -> FunctionSpec {
    FunctionSpec::async_fn("add_track")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("playlist_id")
        .args::<ResolveId>()
        .arg_name("track_id")
        .args::<ResolveId>()
        .returns::<Option<String>>()
        .call_async(Arc::new(add_track_callback))
}

fn remove_track_spec() -> FunctionSpec {
    FunctionSpec::async_fn("remove_track")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("playlist_id")
        .args::<ResolveId>()
        .arg_name("entry_id")
        .args::<String>()
        .returns::<bool>()
        .call_async(Arc::new(remove_track_callback))
}

fn move_track_spec() -> FunctionSpec {
    FunctionSpec::async_fn("move_track")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("playlist_id")
        .args::<ResolveId>()
        .arg_name("entry_id")
        .args::<String>()
        .arg_name("new_position")
        .args::<u64>()
        .returns::<bool>()
        .call_async(Arc::new(move_track_callback))
}

fn list_callback(frame: luau::AsyncCallFrame<'_>) -> luau::runtime::Result<luau::ScheduledFuture> {
    let store = frame
        .vm
        .data()
        .get::<PlaylistsModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let playlists = playlist_service::list(&db)
            .map_err(crate::plugins::runtime_error)?
            .into_iter()
            .filter(|playlist| {
                let Some(playlist_db_id) = playlist.db_id.clone().map(DbId::from) else {
                    return false;
                };
                crate::services::auth::access::playlist_accessible_as(
                    &db,
                    &principal,
                    user_db_id,
                    playlist_db_id,
                )
                .unwrap_or(false)
            })
            .map(PlaylistInfo::from)
            .collect::<Vec<_>>();
        harmony_luau::serializable_to_luau_owned(playlists)
    }))
}

fn get_by_id_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let id = parse_resolve_id(frame.args.read_named::<luau::Value>("id")?)?;
    let store = frame
        .vm
        .data()
        .get::<PlaylistsModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let query_id = id
            .to_query_id(&db)
            .map_err(crate::plugins::runtime_error)?
            .ok_or_else(|| crate::plugins::runtime_error("could not resolve id"))?;
        let Some(playlist) =
            playlist_service::get(&db, query_id).map_err(crate::plugins::runtime_error)?
        else {
            return Ok(luau::Value::Nil);
        };
        let Some(playlist_db_id) = playlist.db_id.clone().map(DbId::from) else {
            return Ok(luau::Value::Nil);
        };
        if !crate::services::auth::access::playlist_accessible_as(
            &db,
            &principal,
            user_db_id,
            playlist_db_id,
        )
        .map_err(crate::plugins::runtime_error)?
        {
            return Ok(luau::Value::Nil);
        }
        harmony_luau::serializable_to_luau_owned(PlaylistInfo::from(playlist))
    }))
}

fn list_owned_callback(
    frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let db = frame.vm.data().get::<PlaylistsModuleStore>()?.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let owner_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let playlists = playlist_service::get_by_user(&db, owner_db_id)
            .map_err(crate::plugins::runtime_error)?
            .into_iter()
            .map(PlaylistInfo::from)
            .collect::<Vec<_>>();
        harmony_luau::serializable_to_luau_owned(playlists)
    }))
}

fn get_owner_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let playlist_id = parse_resolve_id(frame.args.read_named::<luau::Value>("playlist_id")?)?;
    let store = frame
        .vm
        .data()
        .get::<PlaylistsModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let Some(playlist_id) = playlist_id
            .to_query_id(&db)
            .map_err(crate::plugins::runtime_error)?
        else {
            return Ok(luau::Value::Nil);
        };
        let QueryId::Id(playlist_db_id) = playlist_id else {
            return Ok(luau::Value::Nil);
        };
        if !crate::services::auth::access::playlist_accessible_as(
            &db,
            &principal,
            user_db_id,
            playlist_db_id,
        )
        .map_err(crate::plugins::runtime_error)?
        {
            return Ok(luau::Value::Nil);
        }
        let Some(owner_db_id) = playlist_service::get_owner(&db, QueryId::Id(playlist_db_id))
            .map_err(crate::plugins::runtime_error)?
        else {
            return Ok(luau::Value::Nil);
        };
        let owner =
            db::users::get_by_id(&*db, owner_db_id).map_err(crate::plugins::runtime_error)?;
        Ok(owner
            .map(|owner| luau::Value::String(owner.id.into_bytes()))
            .unwrap_or(luau::Value::Nil))
    }))
}

fn get_tracks_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let playlist_id = parse_resolve_id(frame.args.read_named::<luau::Value>("playlist_id")?)?;
    let store = frame
        .vm
        .data()
        .get::<PlaylistsModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let playlist_id = playlist_id
            .to_query_id(&db)
            .map_err(crate::plugins::runtime_error)?
            .ok_or_else(|| crate::plugins::runtime_error("could not resolve playlist id"))?;
        let QueryId::Id(playlist_db_id) = playlist_id else {
            return harmony_luau::serializable_to_luau_owned(Vec::<PlaylistTrackLink>::new());
        };
        let links = visible_track_links(&db, &principal, user_db_id, playlist_db_id)?;
        harmony_luau::serializable_to_luau_owned(links)
    }))
}

fn get_tracks_many_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let playlist_ids: luau::Table = frame.args.read_named("playlist_ids")?;
    let playlist_ids = parse_db_ids(frame.vm, &playlist_ids)?;
    let store = frame
        .vm
        .data()
        .get::<PlaylistsModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let result = playlist_service::get_tracks_many(&db, &playlist_ids)
            .map_err(crate::plugins::runtime_error)?;
        let mut table = luau::OwnedTable::with_entry_capacity(0, 0, playlist_ids.len());
        for id in playlist_ids {
            let links = if crate::services::auth::access::playlist_accessible_as(
                &db, &principal, user_db_id, id,
            )
            .map_err(crate::plugins::runtime_error)?
            {
                result
                    .get(&id)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|link| playlist_link_to_info(&db, &principal, link).transpose())
                    .collect::<luau::runtime::Result<Vec<_>>>()?
            } else {
                Vec::new()
            };
            let value = harmony_luau::serializable_to_luau_owned(links)?;
            table.set_key(luau::Value::from(id.0), value);
        }
        Ok(luau::Value::TableData(table))
    }))
}

fn create_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let request: luau::Table = frame.args.read_named("request")?;
    let request = parse_create_request(frame.vm, request)?;
    let store = frame
        .vm
        .data()
        .get::<PlaylistsModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let mut db = db.write().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let request = request.into_service_request(user_db_id);
        let playlist_id =
            playlist_service::create(&mut db, &request).map_err(crate::plugins::runtime_error)?;
        Ok(luau::Value::from(playlist_id.0))
    }))
}

fn update_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let request: luau::Table = frame.args.read_named("request")?;
    let request = parse_update_request(frame.vm, request)?;
    let store = frame
        .vm
        .data()
        .get::<PlaylistsModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let mut db = db.write().await;
        let Some(QueryId::Id(playlist_db_id)) = request
            .playlist_id
            .to_query_id(&db)
            .map_err(crate::plugins::runtime_error)?
        else {
            return Ok(luau::Value::Nil);
        };
        if !crate::services::auth::access::playlist_owned(&db, &principal, playlist_db_id)
            .map_err(crate::plugins::runtime_error)?
        {
            return Ok(luau::Value::Nil);
        }
        let request = request.into_service_request(QueryId::Id(playlist_db_id));
        let playlist = playlist_service::update(&mut db, &request)
            .map_err(crate::plugins::runtime_error)?
            .map(PlaylistInfo::from)
            .map(harmony_luau::serializable_to_luau_owned)
            .transpose()?
            .unwrap_or(luau::Value::Nil);
        Ok(playlist)
    }))
}

fn delete_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let playlist_id = parse_resolve_id(frame.args.read_named::<luau::Value>("playlist_id")?)?;
    let db = frame.vm.data().get::<PlaylistsModuleStore>()?.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;
    Ok(luau::ScheduledFuture::new(async move {
        let mut db = db.write().await;
        let Some(QueryId::Id(playlist_db_id)) = playlist_id
            .to_query_id(&db)
            .map_err(crate::plugins::runtime_error)?
        else {
            return Ok(luau::Value::Boolean(false));
        };
        if !crate::services::auth::access::playlist_owned(&db, &principal, playlist_db_id)
            .map_err(crate::plugins::runtime_error)?
        {
            return Ok(luau::Value::Boolean(false));
        }
        let deleted = playlist_service::delete(&mut db, QueryId::Id(playlist_db_id))
            .map_err(crate::plugins::runtime_error)?;
        Ok(luau::Value::Boolean(deleted.is_some()))
    }))
}

fn add_track_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let playlist_id = parse_resolve_id(frame.args.read_named::<luau::Value>("playlist_id")?)?;
    let track_id = parse_resolve_id(frame.args.read_named::<luau::Value>("track_id")?)?;
    let store = frame
        .vm
        .data()
        .get::<PlaylistsModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let mut db = db.write().await;
        let Some(QueryId::Id(playlist_db_id)) = playlist_id
            .to_query_id(&db)
            .map_err(crate::plugins::runtime_error)?
        else {
            return Ok(luau::Value::Nil);
        };
        if !crate::services::auth::access::playlist_owned(&db, &principal, playlist_db_id)
            .map_err(crate::plugins::runtime_error)?
        {
            return Ok(luau::Value::Nil);
        }
        let Some(QueryId::Id(track_db_id)) = track_id
            .to_query_id(&db)
            .map_err(crate::plugins::runtime_error)?
        else {
            return Ok(luau::Value::Nil);
        };
        if db::tracks::get_by_id(&db, track_db_id)
            .map_err(crate::plugins::runtime_error)?
            .is_none()
            || !crate::services::auth::access::entity_accessible(&db, &principal, track_db_id)
                .map_err(crate::plugins::runtime_error)?
        {
            return Ok(luau::Value::Nil);
        }
        let link = playlist_service::add_track(
            &mut db,
            QueryId::Id(playlist_db_id),
            QueryId::Id(track_db_id),
        )
        .map_err(crate::plugins::runtime_error)?;
        Ok(luau::Value::String(link.entry_id.into_bytes()))
    }))
}

fn remove_track_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let playlist_id = parse_resolve_id(frame.args.read_named::<luau::Value>("playlist_id")?)?;
    let entry_id: String = frame.args.read_named("entry_id")?;
    let db = frame.vm.data().get::<PlaylistsModuleStore>()?.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let mut db = db.write().await;
        let Some(playlist_db_id) =
            owned_playlist_with_entry(&db, &principal, playlist_id, &entry_id)?
        else {
            return Ok(luau::Value::Boolean(false));
        };
        playlist_service::remove_tracks(&mut db, playlist_db_id, &[entry_id])
            .map_err(crate::plugins::runtime_error)?;
        Ok(luau::Value::Boolean(true))
    }))
}

fn move_track_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let playlist_id = parse_resolve_id(frame.args.read_named::<luau::Value>("playlist_id")?)?;
    let entry_id: String = frame.args.read_named("entry_id")?;
    let new_position = match frame.args.read_named::<luau::Value>("new_position")? {
        luau::Value::Integer(value) if value >= 0 => value as u64,
        luau::Value::Number(value)
            if value.is_finite()
                && value.fract() == 0.0
                && (0.0..=9_007_199_254_740_991.0).contains(&value) =>
        {
            value as u64
        }
        _ => {
            return Err(crate::plugins::runtime_error(
                "new_position must be a non-negative integer within the exact numeric range",
            ));
        }
    };
    let db = frame.vm.data().get::<PlaylistsModuleStore>()?.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;
    Ok(luau::ScheduledFuture::new(async move {
        let mut db = db.write().await;
        let Some(playlist_db_id) =
            owned_playlist_with_entry(&db, &principal, playlist_id, &entry_id)?
        else {
            return Ok(luau::Value::Boolean(false));
        };
        playlist_service::move_track(&mut db, playlist_db_id, &entry_id, new_position)
            .map_err(crate::plugins::runtime_error)?;
        Ok(luau::Value::Boolean(true))
    }))
}

/// The caller's playlist `playlist_id` when it holds the entry `entry_id`, checked under `db`'s
/// guard.
fn owned_playlist_with_entry(
    db: &DbAny,
    principal: &Principal,
    playlist_id: ResolveId,
    entry_id: &str,
) -> luau::runtime::Result<Option<DbId>> {
    let Some(QueryId::Id(playlist_db_id)) = playlist_id
        .to_query_id(db)
        .map_err(crate::plugins::runtime_error)?
    else {
        return Ok(None);
    };
    if !crate::services::auth::access::playlist_owned(db, principal, playlist_db_id)
        .map_err(crate::plugins::runtime_error)?
    {
        return Ok(None);
    }
    Ok(playlist_service::find_entry(db, playlist_db_id, entry_id)
        .map_err(crate::plugins::runtime_error)?
        .map(|_| playlist_db_id))
}

fn visible_track_links(
    db: &DbAny,
    principal: &Principal,
    user_db_id: DbId,
    playlist_db_id: DbId,
) -> luau::runtime::Result<Vec<PlaylistTrackLink>> {
    if !crate::services::auth::access::playlist_accessible_as(
        db,
        principal,
        user_db_id,
        playlist_db_id,
    )
    .map_err(crate::plugins::runtime_error)?
    {
        return Ok(Vec::new());
    }
    let links = playlist_service::get_tracks(db, QueryId::Id(playlist_db_id))
        .map_err(crate::plugins::runtime_error)?;
    links
        .into_iter()
        .filter_map(|link| playlist_link_to_info(db, principal, link).transpose())
        .collect()
}

fn playlist_link_to_info(
    db: &DbAny,
    principal: &Principal,
    link: playlist_service::PlaylistTrackLink,
) -> luau::runtime::Result<Option<PlaylistTrackLink>> {
    if !crate::services::auth::access::entity_accessible(db, principal, link.track_db_id)
        .map_err(crate::plugins::runtime_error)?
    {
        return Ok(None);
    }
    Ok(Some(link.into()))
}

fn parse_create_request(
    vm: &luau::Vm,
    table: luau::Table,
) -> luau::runtime::Result<PlaylistCreateRequest> {
    Ok(PlaylistCreateRequest {
        name: parse_required_string_field(vm, &table, "name")?,
        description: parse_optional_string_field(vm, &table, "description")?,
        is_public: parse_optional_bool_field(vm, &table, "is_public")?,
        created_at: parse_optional_u64_field(vm, &table, "created_at")?,
        updated_at: parse_optional_u64_field(vm, &table, "updated_at")?,
    })
}

fn parse_update_request(
    vm: &luau::Vm,
    table: luau::Table,
) -> luau::runtime::Result<PlaylistUpdateRequest> {
    let playlist_id = match table.get_raw(vm, "playlist_id")? {
        luau::Value::Nil => {
            return Err(crate::plugins::runtime_error(
                "missing required field: playlist_id",
            ));
        }
        value => parse_resolve_id(value)?,
    };
    Ok(PlaylistUpdateRequest {
        playlist_id,
        name: parse_optional_string_field(vm, &table, "name")?,
        description: parse_optional_string_field(vm, &table, "description")?,
        is_public: parse_optional_bool_field(vm, &table, "is_public")?,
        updated_at: parse_optional_u64_field(vm, &table, "updated_at")?,
    })
}

fn parse_optional_u64_field(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Option<u64>> {
    match table.get_raw(vm, key)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Integer(value) if value >= 0 => Ok(Some(value as u64)),
        luau::Value::Number(value) if value.is_finite() && value.fract() == 0.0 && value >= 0.0 => {
            Ok(Some(value as u64))
        }
        other => Err(crate::plugins::runtime_error(format!(
            "{key} must be a non-negative integer, got {}",
            other.type_name()
        ))),
    }
}

fn parse_required_string_field(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<String> {
    match table.get_raw(vm, key)? {
        luau::Value::String(bytes) => {
            String::from_utf8(bytes).map_err(crate::plugins::runtime_error)
        }
        luau::Value::Nil => Err(crate::plugins::runtime_error(format!(
            "missing required field: {key}"
        ))),
        other => Err(crate::plugins::runtime_error(format!(
            "{key} must be a string, got {}",
            other.type_name()
        ))),
    }
}

fn parse_optional_string_field(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Option<String>> {
    match table.get_raw(vm, key)? {
        luau::Value::Nil => Ok(None),
        luau::Value::String(bytes) => Ok(Some(
            String::from_utf8(bytes).map_err(crate::plugins::runtime_error)?,
        )),
        other => Err(crate::plugins::runtime_error(format!(
            "{key} must be a string, got {}",
            other.type_name()
        ))),
    }
}

fn parse_optional_bool_field(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Option<bool>> {
    match table.get_raw(vm, key)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Boolean(value) => Ok(Some(value)),
        other => Err(crate::plugins::runtime_error(format!(
            "{key} must be a boolean, got {}",
            other.type_name()
        ))),
    }
}

fn parse_resolve_id(value: luau::Value) -> luau::runtime::Result<ResolveId> {
    match value {
        luau::Value::Integer(value) => Ok(ResolveId::DbId(DbId(value))),
        luau::Value::Number(value) if value.is_finite() && value.fract() == 0.0 => {
            Ok(ResolveId::DbId(DbId(value as i64)))
        }
        luau::Value::String(bytes) => {
            let text = String::from_utf8(bytes).map_err(crate::plugins::runtime_error)?;
            if db::ROOT_COLLECTION_ALIASES.contains(&text.as_str()) {
                Ok(ResolveId::Alias(text))
            } else {
                Ok(ResolveId::Nanoid(text))
            }
        }
        other => Err(crate::plugins::runtime_error(format!(
            "expected integer or string id, got {}",
            other.type_name()
        ))),
    }
}

fn parse_db_ids(vm: &luau::Vm, table: &luau::Table) -> luau::runtime::Result<Vec<DbId>> {
    let mut values = Vec::new();
    for (key, value) in table.pairs_raw(vm)? {
        let Some(index) = array_index(key) else {
            continue;
        };
        let Some(id) = db_id_value(value)? else {
            continue;
        };
        values.push((index, id));
    }
    values.sort_by_key(|(index, _)| *index);

    let mut ids = Vec::new();
    let mut seen = HashSet::new();
    for (_, id) in values {
        if seen.insert(id) {
            ids.push(id);
        }
    }
    Ok(ids)
}

fn array_index(value: luau::Value) -> Option<i64> {
    match value {
        luau::Value::Integer(value) if value > 0 => Some(value),
        luau::Value::Number(value) if value.is_finite() && value.fract() == 0.0 && value > 0.0 => {
            Some(value as i64)
        }
        _ => None,
    }
}

fn db_id_value(value: luau::Value) -> luau::runtime::Result<Option<DbId>> {
    match value {
        luau::Value::Integer(value) if value > 0 => Ok(Some(DbId(value))),
        luau::Value::Number(value) if value.is_finite() && value.fract() == 0.0 && value > 0.0 => {
            Ok(Some(DbId(value as i64)))
        }
        luau::Value::Integer(_) | luau::Value::Number(_) => Ok(None),
        other => Err(crate::plugins::runtime_error(format!(
            "id entries must be positive integers, got {}",
            other.type_name()
        ))),
    }
}

impl LuauTypeInfo for PlaylistInfo {
    fn luau_type() -> LuauType {
        LuauType::named("PlaylistInfo")
    }
}

impl DescribeInterface for PlaylistInfo {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("PlaylistInfo", None);
        descriptor.fields.extend([
            field("db_id", Option::<i64>::luau_type()),
            field("id", String::luau_type()),
            field("name", String::luau_type()),
            field("description", Option::<String>::luau_type()),
            field("is_public", Option::<bool>::luau_type()),
            field("created_at", Option::<u64>::luau_type()),
            field("updated_at", Option::<u64>::luau_type()),
        ]);
        descriptor
    }
}

impl LuauTypeInfo for PlaylistTrackLink {
    fn luau_type() -> LuauType {
        LuauType::named("PlaylistTrackLink")
    }
}

impl DescribeInterface for PlaylistTrackLink {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("PlaylistTrackLink", None);
        descriptor.fields.extend([
            field("entry_id", String::luau_type()),
            field("track_id", i64::luau_type()),
            field("position", u64::luau_type()),
        ]);
        descriptor
    }
}

impl LuauTypeInfo for PlaylistCreateRequest {
    fn luau_type() -> LuauType {
        LuauType::named("PlaylistCreateRequest")
    }
}

impl DescribeInterface for PlaylistCreateRequest {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("PlaylistCreateRequest", None);
        descriptor.fields.extend([
            field("name", String::luau_type()),
            field("description", Option::<String>::luau_type()),
            field("is_public", Option::<bool>::luau_type()),
            field("created_at", Option::<u64>::luau_type()),
            field("updated_at", Option::<u64>::luau_type()),
        ]);
        descriptor
    }
}

impl LuauTypeInfo for PlaylistUpdateRequest {
    fn luau_type() -> LuauType {
        LuauType::named("PlaylistUpdateRequest")
    }
}

impl DescribeInterface for PlaylistUpdateRequest {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("PlaylistUpdateRequest", None);
        descriptor.fields.extend([
            field("playlist_id", resolve_id_type()),
            field("name", Option::<String>::luau_type()),
            field("description", Option::<String>::luau_type()),
            field("is_public", Option::<bool>::luau_type()),
            field("updated_at", Option::<u64>::luau_type()),
        ]);
        descriptor
    }
}

#[cfg(feature = "docgen")]
fn param(name: &'static str, ty: LuauType) -> ParameterDescriptor {
    ParameterDescriptor {
        name,
        ty,
        description: None,
        variadic: false,
    }
}

fn field(name: &'static str, ty: LuauType) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description: None,
    }
}

fn resolve_id_type() -> LuauType {
    LuauType::union(vec![i64::luau_type(), String::luau_type()])
}

#[cfg(feature = "docgen")]
fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "Playlists",
        local_name: "playlists",
        description: None,
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["list"],
                description: None,
                params: Vec::new(),
                returns: vec![Vec::<PlaylistInfo>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_by_id"],
                description: None,
                params: vec![param("id", resolve_id_type())],
                returns: vec![Option::<PlaylistInfo>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["list_owned"],
                description: Some("Playlists owned by the dispatch principal."),
                params: Vec::new(),
                returns: vec![Vec::<PlaylistInfo>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_owner"],
                description: Some("The owning user's public id."),
                params: vec![param("playlist_id", resolve_id_type())],
                returns: vec![Option::<String>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_tracks"],
                description: None,
                params: vec![param("playlist_id", resolve_id_type())],
                returns: vec![Vec::<PlaylistTrackLink>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_tracks_many"],
                description: None,
                params: vec![param("playlist_ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(
                    u64::luau_type(),
                    Vec::<PlaylistTrackLink>::luau_type(),
                )],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["create"],
                description: None,
                params: vec![param("request", PlaylistCreateRequest::luau_type())],
                returns: vec![i64::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["update"],
                description: Some(
                    "Returns the updated playlist, or nil if it is missing or not owned by the caller.",
                ),
                params: vec![param("request", PlaylistUpdateRequest::luau_type())],
                returns: vec![Option::<PlaylistInfo>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["delete"],
                description: Some(
                    "Returns true when deleted, or false if the playlist is missing or not owned by the caller.",
                ),
                params: vec![param("playlist_id", resolve_id_type())],
                returns: vec![bool::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["add_track"],
                description: Some(
                    "Returns the entry ID, or nil if the playlist or track is missing or inaccessible.",
                ),
                params: vec![
                    param("playlist_id", resolve_id_type()),
                    param("track_id", resolve_id_type()),
                ],
                returns: vec![Option::<String>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["remove_track"],
                description: Some(
                    "Returns true when removed, or false if the playlist is not owned by the caller or the entry is not in it.",
                ),
                params: vec![
                    param("playlist_id", resolve_id_type()),
                    param("entry_id", String::luau_type()),
                ],
                returns: vec![bool::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["move_track"],
                description: Some(
                    "Moves an entry to a zero-based position, clamping past the end. Returns false if the playlist is not owned by the caller or the entry is not in it. A move to the current position succeeds.",
                ),
                params: vec![
                    param("playlist_id", resolve_id_type()),
                    param("entry_id", String::luau_type()),
                    param("new_position", u64::luau_type()),
                ],
                returns: vec![bool::luau_type()],
                yields: true,
            },
        ],
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &module_descriptor(),
        &[],
        &[
            PlaylistInfo::interface_descriptor(),
            PlaylistTrackLink::interface_descriptor(),
            PlaylistCreateRequest::interface_descriptor(),
            PlaylistUpdateRequest::interface_descriptor(),
        ],
        &[],
    )
}
