// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use std::collections::HashMap;

use agdb::{
    DbAny,
    DbId,
    QueryId,
};
use harmony_luau as luau;
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
};
use serde::Serialize;

use crate::plugins::args;
use crate::{
    plugins::{
        args::{
            ResolveIdArg,
            UniqueIds,
        },
        db::Db,
    },
    services::{
        auth::Principal,
        playlists as playlist_service,
    },
};
use lyra_db::ResolveId;

pub(crate) struct PlaylistsModule;

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

impl From<lyra_db::Playlist> for PlaylistInfo {
    fn from(playlist: lyra_db::Playlist) -> Self {
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
    track_ids: Vec<DbId>,
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
            track_db_ids: self.track_ids,
        }
    }
}

#[derive(Default)]
struct PlaylistFields {
    name: Option<String>,
    description: Option<String>,
    is_public: Option<bool>,
    updated_at: Option<u64>,
}

impl PlaylistFields {
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

struct PlaylistUpdateRequest {
    playlist_id: ResolveId,
    fields: PlaylistFields,
}

struct PlaylistReplaceRequest {
    fields: PlaylistFields,
    track_ids: Vec<DbId>,
}

impl<'vm> luau::FromLuau<'vm> for PlaylistCreateRequest {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let table: luau::Table = reader.read()?;
        parse_create_request(table)
    }
}

impl<'vm> luau::FromLuau<'vm> for PlaylistUpdateRequest {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let table: luau::Table = reader.read()?;
        parse_update_request(table)
    }
}

impl<'vm> luau::FromLuau<'vm> for PlaylistReplaceRequest {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let table: luau::Table = reader.read()?;
        parse_replace_request(table)
    }
}

/// A track id array argument, repeats included.
struct IdSequence(Vec<DbId>);

impl<'vm> luau::FromLuau<'vm> for IdSequence {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let table: luau::Table = reader.read()?;
        args::id_sequence(&table).map(Self)
    }
}

impl LuauTypeInfo for IdSequence {
    fn luau_type() -> LuauType {
        Vec::<u64>::luau_type()
    }
}

/// A string array argument.
struct EntryIds(Vec<String>);

impl<'vm> luau::FromLuau<'vm> for EntryIds {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let table: luau::Table = reader.read()?;
        args::strings(&table).map(Self)
    }
}

impl LuauTypeInfo for EntryIds {
    fn luau_type() -> LuauType {
        Vec::<String>::luau_type()
    }
}

/// A [`playlist_position`] argument.
struct PlaylistPosition(u64);

impl<'vm> luau::FromLuau<'vm> for PlaylistPosition {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let name = reader.next_name();
        let value: luau::Value = reader.read()?;
        playlist_position(value, &name).map(Self)
    }
}

impl LuauTypeInfo for PlaylistPosition {
    fn luau_type() -> LuauType {
        u64::luau_type()
    }
}

#[harmony_macros::module(id = "lyra/playlists", docs_feature = "docgen")]
impl PlaylistsModule {
    #[harmony(returns(Vec<PlaylistInfo>))]
    async fn list(
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        let db = db.read().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let playlists = playlist_service::list(&db)?
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
    }

    #[harmony(returns(Option<PlaylistInfo>))]
    async fn get_by_id(
        id: ResolveIdArg,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        let db = db.read().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let query_id =
            id.0.to_query_id(&db)?
                .ok_or_else(|| crate::plugins::runtime_error("could not resolve id"))?;
        let Some(playlist) = playlist_service::get(&db, query_id)? else {
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
        )? {
            return Ok(luau::Value::Nil);
        }
        harmony_luau::serializable_to_luau_owned(PlaylistInfo::from(playlist))
    }

    #[harmony(
        description = "Playlists owned by the dispatch principal.",
        returns(Vec<PlaylistInfo>)
    )]
    async fn list_owned(
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        let db = db.read().await;
        let owner_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let playlists = playlist_service::get_by_user(&db, owner_db_id)?
            .into_iter()
            .map(PlaylistInfo::from)
            .collect::<Vec<_>>();
        harmony_luau::serializable_to_luau_owned(playlists)
    }

    #[harmony(description = "The owning user's public id.")]
    async fn get_owner(
        playlist_id: ResolveIdArg,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<Option<String>> {
        let db = db.read().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let Some(playlist_id) = playlist_id.0.to_query_id(&db)? else {
            return Ok(None);
        };
        let QueryId::Id(playlist_db_id) = playlist_id else {
            return Ok(None);
        };
        if !crate::services::auth::access::playlist_accessible_as(
            &db,
            &principal,
            user_db_id,
            playlist_db_id,
        )? {
            return Ok(None);
        }
        let Some(owner_db_id) = playlist_service::get_owner(&db, QueryId::Id(playlist_db_id))?
        else {
            return Ok(None);
        };
        let owner = lyra_db::users::get_by_id(&*db, owner_db_id)?;
        Ok(owner.map(|owner| owner.id))
    }

    #[harmony(returns(Vec<PlaylistTrackLink>))]
    async fn get_tracks(
        playlist_id: ResolveIdArg,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        let db = db.read().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let playlist_id = playlist_id
            .0
            .to_query_id(&db)?
            .ok_or_else(|| crate::plugins::runtime_error("could not resolve playlist id"))?;
        let QueryId::Id(playlist_db_id) = playlist_id else {
            return harmony_luau::serializable_to_luau_owned(Vec::<PlaylistTrackLink>::new());
        };
        let links = visible_track_links(&db, &principal, user_db_id, playlist_db_id)?;
        harmony_luau::serializable_to_luau_owned(links)
    }

    #[harmony(returns(HashMap<u64, Vec<PlaylistTrackLink>>))]
    async fn get_tracks_many(
        playlist_ids: UniqueIds,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let playlist_ids = playlist_ids.0;
        let db = db.read().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let result = playlist_service::get_tracks_many(&db, &playlist_ids)?;
        let mut table = luau::OwnedTable::with_entry_capacity(0, 0, playlist_ids.len());
        for id in playlist_ids {
            let links = if crate::services::auth::access::playlist_accessible_as(
                &db, &principal, user_db_id, id,
            )? {
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
            table.set_key(id.0, value);
        }
        Ok(table)
    }

    #[harmony(
        description = "Creates the playlist with `track_ids` as its entries, in order and with repeats, in one transaction. Raises an error, creating nothing, when a track is missing or inaccessible."
    )]
    async fn create(
        request: PlaylistCreateRequest,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<i64> {
        let mut db = db.write().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        require_accessible_tracks(&db, &principal, &request.track_ids)?;
        let request = request.into_service_request(user_db_id);
        let playlist_id = playlist_service::create(&mut db, &request)?;
        Ok(playlist_id.0)
    }

    #[harmony(
        description = "Returns the updated playlist, or nil if it is missing or not owned by the caller.",
        returns(Option<PlaylistInfo>)
    )]
    async fn update(
        request: PlaylistUpdateRequest,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        let mut db = db.write().await;
        let Some(QueryId::Id(playlist_db_id)) = request.playlist_id.to_query_id(&db)? else {
            return Ok(luau::Value::Nil);
        };
        if !crate::services::auth::access::playlist_owned(&db, &principal, playlist_db_id)? {
            return Ok(luau::Value::Nil);
        }
        let request = request
            .fields
            .into_service_request(QueryId::Id(playlist_db_id));
        let playlist = playlist_service::update(&mut db, &request)?
            .map(PlaylistInfo::from)
            .map(harmony_luau::serializable_to_luau_owned)
            .transpose()?
            .unwrap_or(luau::Value::Nil);
        Ok(playlist)
    }

    #[harmony(
        description = "Returns true when deleted, or false if the playlist is missing or the caller neither owns it nor is an admin."
    )]
    async fn delete(
        playlist_id: ResolveIdArg,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let mut db = db.write().await;
        let Some(QueryId::Id(playlist_db_id)) = playlist_id.0.to_query_id(&db)? else {
            return Ok(false);
        };
        if !crate::services::auth::access::playlist_deletable(&db, &principal, playlist_db_id)? {
            return Ok(false);
        }
        let deleted = playlist_service::delete(&mut db, QueryId::Id(playlist_db_id))?;
        Ok(deleted.is_some())
    }

    #[harmony(
        description = "Applies `fields` and makes `track_ids` the entries, in order and with repeats, in one transaction. Returns the updated playlist, or nil if it is missing or not owned by the caller. Raises an error, changing nothing, when a track is missing or inaccessible.",
        returns(Option<PlaylistInfo>)
    )]
    async fn replace(
        playlist_id: ResolveIdArg,
        request: PlaylistReplaceRequest,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        let mut db = db.write().await;
        let Some(playlist_db_id) = owned_playlist(&db, &principal, playlist_id.0)? else {
            return Ok(luau::Value::Nil);
        };
        require_accessible_tracks(&db, &principal, &request.track_ids)?;
        let update = request
            .fields
            .into_service_request(QueryId::Id(playlist_db_id));
        playlist_service::replace(&mut db, &update, &request.track_ids)?
            .map(PlaylistInfo::from)
            .map(harmony_luau::serializable_to_luau_owned)
            .transpose()
            .map(|playlist| playlist.unwrap_or(luau::Value::Nil))
    }

    #[harmony(
        description = "Adds `track_ids`, in order and with repeats, in one transaction: at the 0-based `position` when given (past the end appends), and at the end otherwise. Returns their entry IDs, or nil if the playlist is missing or not owned by the caller. Raises an error, adding nothing, when a track is missing or inaccessible.",
        returns(Option<Vec<String>>)
    )]
    async fn add_tracks(
        playlist_id: ResolveIdArg,
        track_ids: IdSequence,
        position: Option<PlaylistPosition>,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        let track_ids = track_ids.0;
        let position = position.map(|position| position.0);
        let mut db = db.write().await;
        let Some(playlist_db_id) = owned_playlist(&db, &principal, playlist_id.0)? else {
            return Ok(luau::Value::Nil);
        };
        require_accessible_tracks(&db, &principal, &track_ids)?;
        let track_ids = track_ids.into_iter().map(QueryId::Id).collect::<Vec<_>>();
        let entry_ids = playlist_service::add_tracks(
            &mut db,
            QueryId::Id(playlist_db_id),
            &track_ids,
            position,
        )?
        .into_iter()
        .map(|link| link.entry_id)
        .collect::<Vec<_>>();
        harmony_luau::serializable_to_luau_owned(entry_ids)
    }

    #[harmony(
        description = "Returns the entry ID, or nil if the playlist or track is missing or inaccessible."
    )]
    async fn add_track(
        playlist_id: ResolveIdArg,
        track_id: ResolveIdArg,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<Option<String>> {
        let mut db = db.write().await;
        let Some(QueryId::Id(playlist_db_id)) = playlist_id.0.to_query_id(&db)? else {
            return Ok(None);
        };
        if !crate::services::auth::access::playlist_owned(&db, &principal, playlist_db_id)? {
            return Ok(None);
        }
        let Some(QueryId::Id(track_db_id)) = track_id.0.to_query_id(&db)? else {
            return Ok(None);
        };
        if lyra_db::tracks::get_by_id(&db, track_db_id)?.is_none()
            || !crate::services::auth::access::entity_accessible(&db, &principal, track_db_id)?
        {
            return Ok(None);
        }
        let link = playlist_service::add_track(
            &mut db,
            QueryId::Id(playlist_db_id),
            QueryId::Id(track_db_id),
        )?;
        Ok(Some(link.entry_id))
    }

    #[harmony(
        description = "Returns true when removed, or false if the playlist is not owned by the caller or the entry is not in it."
    )]
    async fn remove_track(
        playlist_id: ResolveIdArg,
        entry_id: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let mut db = db.write().await;
        let Some(playlist_db_id) =
            owned_playlist_with_entry(&db, &principal, playlist_id.0, &entry_id)?
        else {
            return Ok(false);
        };
        playlist_service::remove_tracks(&mut db, playlist_db_id, &[entry_id])?;
        Ok(true)
    }

    #[harmony(
        description = "Removes the entries in one transaction. Returns false, removing nothing, if the playlist is not owned by the caller or an entry is not in it."
    )]
    async fn remove_tracks(
        playlist_id: ResolveIdArg,
        entry_ids: EntryIds,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let entry_ids = entry_ids.0;
        let mut db = db.write().await;
        let Some(playlist_db_id) = owned_playlist(&db, &principal, playlist_id.0)? else {
            return Ok(false);
        };
        for entry_id in &entry_ids {
            if playlist_service::find_entry(&db, playlist_db_id, entry_id)?.is_none() {
                return Ok(false);
            }
        }
        playlist_service::remove_tracks(&mut db, playlist_db_id, &entry_ids)?;
        Ok(true)
    }

    #[harmony(
        description = "Moves an entry to a zero-based position, clamping past the end. Returns false if the playlist is not owned by the caller or the entry is not in it. A move to the current position succeeds."
    )]
    async fn move_track(
        playlist_id: ResolveIdArg,
        entry_id: String,
        new_position: PlaylistPosition,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let mut db = db.write().await;
        let Some(playlist_db_id) =
            owned_playlist_with_entry(&db, &principal, playlist_id.0, &entry_id)?
        else {
            return Ok(false);
        };
        playlist_service::move_track(&mut db, playlist_db_id, &entry_id, new_position.0)?;
        Ok(true)
    }
}

/// The caller's playlist `playlist_id`, checked under `db`'s guard.
fn owned_playlist(
    db: &DbAny,
    principal: &Principal,
    playlist_id: ResolveId,
) -> luau::runtime::Result<Option<DbId>> {
    let Some(QueryId::Id(playlist_db_id)) = playlist_id.to_query_id(db)? else {
        return Ok(None);
    };
    Ok(
        crate::services::auth::access::playlist_owned(db, principal, playlist_db_id)?
            .then_some(playlist_db_id),
    )
}

fn require_accessible_tracks(
    db: &DbAny,
    principal: &Principal,
    track_ids: &[DbId],
) -> luau::runtime::Result<()> {
    let accessible = crate::services::auth::access::accessible_entities(db, principal, track_ids)?;
    for track_id in track_ids {
        let exists = lyra_db::tracks::get_by_id(db, *track_id)?.is_some();
        if !exists || !accessible.contains(track_id) {
            return Err(crate::plugins::runtime_error(format!(
                "track not found: {}",
                track_id.0
            )));
        }
    }
    Ok(())
}

/// A non-negative integer playlist position read from `value`.
fn playlist_position(value: luau::Value, name: &str) -> luau::runtime::Result<u64> {
    match value {
        luau::Value::Integer(value) if value >= 0 => Ok(value as u64),
        luau::Value::Number(value)
            if value.is_finite()
                && value.fract() == 0.0
                && (0.0..=9_007_199_254_740_991.0).contains(&value) =>
        {
            Ok(value as u64)
        }
        _ => Err(crate::plugins::runtime_error(format!(
            "{name} must be a non-negative integer within the exact numeric range"
        ))),
    }
}

/// The caller's playlist `playlist_id` when it holds the entry `entry_id`, checked under `db`'s
/// guard.
fn owned_playlist_with_entry(
    db: &DbAny,
    principal: &Principal,
    playlist_id: ResolveId,
    entry_id: &str,
) -> luau::runtime::Result<Option<DbId>> {
    let Some(playlist_db_id) = owned_playlist(db, principal, playlist_id)? else {
        return Ok(None);
    };
    Ok(playlist_service::find_entry(db, playlist_db_id, entry_id)?.map(|_| playlist_db_id))
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
    )? {
        return Ok(Vec::new());
    }
    let links = playlist_service::get_tracks(db, QueryId::Id(playlist_db_id))?;
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
    if !crate::services::auth::access::entity_accessible(db, principal, link.track_db_id)? {
        return Ok(None);
    }
    Ok(Some(link.into()))
}

fn parse_create_request(table: luau::Table) -> luau::runtime::Result<PlaylistCreateRequest> {
    Ok(PlaylistCreateRequest {
        name: table.required_string_field("name")?,
        description: table.optional_string_field("description")?,
        is_public: table.optional_bool_field("is_public")?,
        created_at: args::optional_u64(&table, "created_at")?,
        updated_at: args::optional_u64(&table, "updated_at")?,
        track_ids: args::optional_id_sequence(&table, "track_ids")?,
    })
}

fn parse_update_request(table: luau::Table) -> luau::runtime::Result<PlaylistUpdateRequest> {
    let playlist_id = match table.get("playlist_id")? {
        luau::Value::Nil => {
            return Err(crate::plugins::runtime_error(
                "missing required field: playlist_id",
            ));
        }
        value => args::resolve_id(value)?,
    };
    Ok(PlaylistUpdateRequest {
        playlist_id,
        fields: parse_fields(&table)?,
    })
}

fn parse_replace_request(table: luau::Table) -> luau::runtime::Result<PlaylistReplaceRequest> {
    let fields = match table.get("fields")? {
        luau::Value::Nil => PlaylistFields::default(),
        luau::Value::Table(fields) => parse_fields(&fields)?,
        other => {
            return Err(crate::plugins::runtime_error(format!(
                "fields must be a table, got {}",
                other.type_name()
            )));
        }
    };
    let track_ids = match table.get("track_ids")? {
        luau::Value::Table(track_ids) => args::id_sequence(&track_ids)?,
        other => {
            return Err(crate::plugins::runtime_error(format!(
                "track_ids must be an array of ids, got {}",
                other.type_name()
            )));
        }
    };
    Ok(PlaylistReplaceRequest { fields, track_ids })
}

fn parse_fields(table: &luau::Table) -> luau::runtime::Result<PlaylistFields> {
    Ok(PlaylistFields {
        name: table.optional_string_field("name")?,
        description: table.optional_string_field("description")?,
        is_public: table.optional_bool_field("is_public")?,
        updated_at: args::optional_u64(table, "updated_at")?,
    })
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
            field("track_ids", Option::<Vec<u64>>::luau_type()),
        ]);
        descriptor
    }
}

impl LuauTypeInfo for PlaylistFields {
    fn luau_type() -> LuauType {
        LuauType::named("PlaylistFields")
    }
}

impl DescribeInterface for PlaylistFields {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("PlaylistFields", None);
        descriptor.fields.extend([
            field("name", Option::<String>::luau_type()),
            field("description", Option::<String>::luau_type()),
            field("is_public", Option::<bool>::luau_type()),
            field("updated_at", Option::<u64>::luau_type()),
        ]);
        descriptor
    }
}

impl LuauTypeInfo for PlaylistReplaceRequest {
    fn luau_type() -> LuauType {
        LuauType::named("PlaylistReplaceRequest")
    }
}

impl DescribeInterface for PlaylistReplaceRequest {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("PlaylistReplaceRequest", None);
        descriptor.fields.extend([
            field("fields", Option::<PlaylistFields>::luau_type()),
            field("track_ids", Vec::<u64>::luau_type()),
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
            field("playlist_id", args::resolve_id_type()),
            field("name", Option::<String>::luau_type()),
            field("description", Option::<String>::luau_type()),
            field("is_public", Option::<bool>::luau_type()),
            field("updated_at", Option::<u64>::luau_type()),
        ]);
        descriptor
    }
}

fn field(name: &'static str, ty: LuauType) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description: None,
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    use harmony_luau::DescribeModule;

    harmony_luau::render_definition_file_with_support(
        &PlaylistsModule::module_descriptor(),
        &[],
        &[
            PlaylistInfo::interface_descriptor(),
            PlaylistTrackLink::interface_descriptor(),
            PlaylistCreateRequest::interface_descriptor(),
            PlaylistFields::interface_descriptor(),
            PlaylistUpdateRequest::interface_descriptor(),
            PlaylistReplaceRequest::interface_descriptor(),
        ],
        &[],
    )
}
