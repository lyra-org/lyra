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
            IdSequence,
            UniqueIds,
        },
        db::Db,
        ids::{
            self,
            Resolved,
        },
    },
    services::{
        auth::Principal,
        playlists as playlist_service,
    },
};

pub(crate) struct PlaylistsModule;

#[derive(Serialize)]
struct PlaylistInfo {
    id: String,
    name: String,
    description: Option<String>,
    is_public: Option<bool>,
    created_at_ms: Option<u64>,
    updated_at_ms: Option<u64>,
}

impl From<lyra_db::Playlist> for PlaylistInfo {
    fn from(playlist: lyra_db::Playlist) -> Self {
        Self {
            id: playlist.id,
            name: playlist.name,
            description: playlist.description,
            is_public: playlist.is_public,
            created_at_ms: playlist.created_at_ms,
            updated_at_ms: playlist.updated_at_ms,
        }
    }
}

#[derive(Clone, Serialize)]
struct PlaylistTrackLink {
    entry_id: String,
    track_id: String,
    position: u64,
}

struct PlaylistCreateRequest {
    name: String,
    description: Option<String>,
    is_public: Option<bool>,
    created_at_ms: Option<u64>,
    updated_at_ms: Option<u64>,
    track_ids: Vec<String>,
}

impl PlaylistCreateRequest {
    fn into_service_request(
        self,
        user_db_id: DbId,
        track_db_ids: Vec<DbId>,
    ) -> playlist_service::CreatePlaylistRequest {
        playlist_service::CreatePlaylistRequest {
            user_db_id,
            name: self.name,
            description: self.description,
            is_public: self.is_public,
            created_at_ms: self.created_at_ms,
            updated_at_ms: self.updated_at_ms,
            track_db_ids,
        }
    }
}

#[derive(Default)]
struct PlaylistFields {
    name: Option<String>,
    description: Option<String>,
    is_public: Option<bool>,
    updated_at_ms: Option<u64>,
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
            updated_at_ms: self.updated_at_ms,
        }
    }
}

struct PlaylistUpdateRequest {
    playlist_id: String,
    fields: PlaylistFields,
}

struct PlaylistReplaceRequest {
    fields: PlaylistFields,
    track_ids: Vec<String>,
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
        id: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        let db = db.read().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let Some(playlist_db_id) = ids::db_id(&*db, &id)? else {
            return Ok(luau::Value::Nil);
        };
        let Some(playlist) = playlist_service::get(&db, QueryId::Id(playlist_db_id))? else {
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
        playlist_id: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<Option<String>> {
        let db = db.read().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let Some(playlist_db_id) = ids::db_id(&*db, &playlist_id)? else {
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
        playlist_id: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        let db = db.read().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let Some(playlist_db_id) = ids::db_id(&*db, &playlist_id)? else {
            return harmony_luau::serializable_to_luau_owned(Vec::<PlaylistTrackLink>::new());
        };
        let links = visible_track_links(&db, &principal, user_db_id, playlist_db_id)?;
        harmony_luau::serializable_to_luau_owned(links)
    }

    #[harmony(
        description = "The tracks of each of these playlists that the caller can see, keyed by playlist id. Playlists the caller cannot see are absent.",
        returns(HashMap<String, Vec<PlaylistTrackLink>>)
    )]
    async fn get_tracks_many(
        playlist_ids: UniqueIds,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let db = db.read().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let playlists = Resolved::of(&*db, playlist_ids.0)?;
        let mut result = playlist_service::get_tracks_many(&db, playlists.db_ids())?;
        let mut values = Vec::with_capacity(playlists.db_ids().len());
        for &id in playlists.db_ids() {
            if !crate::services::auth::access::playlist_accessible_as(
                &db, &principal, user_db_id, id,
            )? {
                continue;
            }
            let links = visible_links(&db, &principal, result.remove(&id).unwrap_or_default())?;
            values.push((id, harmony_luau::serializable_to_luau_owned(links)?));
        }
        Ok(playlists.keyed(values))
    }

    #[harmony(
        description = "Creates the playlist with `track_ids` as its entries, in order and with repeats, in one transaction. Raises an error, creating nothing, when a track is missing or inaccessible."
    )]
    async fn create(
        request: PlaylistCreateRequest,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<String> {
        let mut db = db.write().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let track_db_ids = accessible_tracks(&db, &principal, &request.track_ids)?;
        let request = request.into_service_request(user_db_id, track_db_ids);
        let playlist_id = playlist_service::create(&mut db, &request)?;
        ids::public_id(&*db, playlist_id)
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
        let Some(playlist_db_id) = ids::db_id(&*db, &request.playlist_id)? else {
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
        playlist_id: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let mut db = db.write().await;
        let Some(playlist_db_id) = ids::db_id(&*db, &playlist_id)? else {
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
        playlist_id: String,
        request: PlaylistReplaceRequest,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        let mut db = db.write().await;
        let Some(playlist_db_id) = owned_playlist(&db, &principal, &playlist_id)? else {
            return Ok(luau::Value::Nil);
        };
        let track_db_ids = accessible_tracks(&db, &principal, &request.track_ids)?;
        let update = request
            .fields
            .into_service_request(QueryId::Id(playlist_db_id));
        playlist_service::replace(&mut db, &update, &track_db_ids)?
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
        playlist_id: String,
        track_ids: IdSequence,
        position: Option<PlaylistPosition>,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        let position = position.map(|position| position.0);
        let mut db = db.write().await;
        let Some(playlist_db_id) = owned_playlist(&db, &principal, &playlist_id)? else {
            return Ok(luau::Value::Nil);
        };
        let track_ids = accessible_tracks(&db, &principal, &track_ids.0)?
            .into_iter()
            .map(QueryId::Id)
            .collect::<Vec<_>>();
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
        playlist_id: String,
        track_id: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<Option<String>> {
        let mut db = db.write().await;
        let Some(playlist_db_id) = owned_playlist(&db, &principal, &playlist_id)? else {
            return Ok(None);
        };
        let Some(track_db_id) = ids::db_id(&*db, &track_id)? else {
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
        playlist_id: String,
        entry_id: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let mut db = db.write().await;
        let Some(playlist_db_id) =
            owned_playlist_with_entry(&db, &principal, &playlist_id, &entry_id)?
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
        playlist_id: String,
        entry_ids: EntryIds,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let entry_ids = entry_ids.0;
        let mut db = db.write().await;
        let Some(playlist_db_id) = owned_playlist(&db, &principal, &playlist_id)? else {
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
        playlist_id: String,
        entry_id: String,
        new_position: PlaylistPosition,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let mut db = db.write().await;
        let Some(playlist_db_id) =
            owned_playlist_with_entry(&db, &principal, &playlist_id, &entry_id)?
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
    playlist_id: &str,
) -> luau::runtime::Result<Option<DbId>> {
    let Some(playlist_db_id) = ids::db_id(db, playlist_id)? else {
        return Ok(None);
    };
    Ok(
        crate::services::auth::access::playlist_owned(db, principal, playlist_db_id)?
            .then_some(playlist_db_id),
    )
}

/// The tracks `track_ids` name, in order, raising when one is missing or inaccessible.
fn accessible_tracks(
    db: &DbAny,
    principal: &Principal,
    track_ids: &[String],
) -> luau::runtime::Result<Vec<DbId>> {
    let not_found =
        |track_id: &str| crate::plugins::runtime_error(format!("track not found: {track_id}"));
    let mut track_db_ids = Vec::with_capacity(track_ids.len());
    for track_id in track_ids {
        track_db_ids.push(ids::db_id(db, track_id)?.ok_or_else(|| not_found(track_id))?);
    }
    let accessible =
        crate::services::auth::access::accessible_entities(db, principal, &track_db_ids)?;
    for (track_id, track_db_id) in track_ids.iter().zip(&track_db_ids) {
        let exists = lyra_db::tracks::get_by_id(db, *track_db_id)?.is_some();
        if !exists || !accessible.contains(track_db_id) {
            return Err(not_found(track_id));
        }
    }
    Ok(track_db_ids)
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
    playlist_id: &str,
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
    visible_links(db, principal, links)
}

/// The links whose tracks the caller can see, with the tracks' public ids.
fn visible_links(
    db: &DbAny,
    principal: &Principal,
    links: Vec<playlist_service::PlaylistTrackLink>,
) -> luau::runtime::Result<Vec<PlaylistTrackLink>> {
    let track_db_ids = links
        .iter()
        .map(|link| link.track_db_id)
        .collect::<Vec<_>>();
    let accessible =
        crate::services::auth::access::accessible_entities(db, principal, &track_db_ids)?;
    let public_ids = ids::public_ids(db, &track_db_ids)?;
    Ok(links
        .into_iter()
        .filter(|link| accessible.contains(&link.track_db_id))
        .filter_map(|link| {
            Some(PlaylistTrackLink {
                track_id: public_ids.get(&link.track_db_id)?.clone(),
                entry_id: link.entry_id,
                position: link.position,
            })
        })
        .collect())
}

fn parse_create_request(table: luau::Table) -> luau::runtime::Result<PlaylistCreateRequest> {
    Ok(PlaylistCreateRequest {
        name: table.required_string_field("name")?,
        description: table.optional_string_field("description")?,
        is_public: table.optional_bool_field("is_public")?,
        created_at_ms: args::optional_u64(&table, "created_at_ms")?,
        updated_at_ms: args::optional_u64(&table, "updated_at_ms")?,
        track_ids: args::optional_id_sequence(&table, "track_ids")?,
    })
}

fn parse_update_request(table: luau::Table) -> luau::runtime::Result<PlaylistUpdateRequest> {
    let playlist_id = table.required_string_field("playlist_id")?;
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
        updated_at_ms: args::optional_u64(table, "updated_at_ms")?,
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
            field("id", String::luau_type()),
            field("name", String::luau_type()),
            field("description", Option::<String>::luau_type()),
            field("is_public", Option::<bool>::luau_type()),
            field("created_at_ms", Option::<u64>::luau_type()),
            field("updated_at_ms", Option::<u64>::luau_type()),
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
            field("track_id", String::luau_type()),
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
            field("created_at_ms", Option::<u64>::luau_type()),
            field("updated_at_ms", Option::<u64>::luau_type()),
            field("track_ids", Option::<Vec<String>>::luau_type()),
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
            field("updated_at_ms", Option::<u64>::luau_type()),
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
            field("track_ids", Vec::<String>::luau_type()),
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
            field("playlist_id", String::luau_type()),
            field("name", Option::<String>::luau_type()),
            field("description", Option::<String>::luau_type()),
            field("is_public", Option::<bool>::luau_type()),
            field("updated_at_ms", Option::<u64>::luau_type()),
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
