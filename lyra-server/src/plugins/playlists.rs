// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use std::collections::HashMap;
use std::collections::HashSet;

use agdb::{
    DbAny,
    DbId,
    QueryId,
};
use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::TypeAliasDescriptor;
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
};
use serde::Serialize;

use crate::{
    plugins::{
        args::{
            self,
            DistinctIds,
            IdSequence,
            UniqueIds,
        },
        catalog::{
            self,
            PersonalScopes,
        },
        db::Db,
        ids::Resolved,
    },
    services::{
        auth::{
            Principal,
            access,
        },
        catalog::{
            Direction,
            Page,
            PersonalData,
        },
        playlists::{
            self as playlist_service,
            SortKey,
        },
    },
};

pub(crate) struct PlaylistsModule;

crate::plugins::limits::module_limits!(&[crate::plugins::limits::Limit {
    name: "MAX_TRACK_IDS",
    value: playlist_service::MAX_PLAYLIST_TRACK_IDS,
    description: "The most track or entry ids `create`, `replace`, `add_tracks` and `remove_entries` take.",
}]);

/// A playlist as plugins see it.
#[derive(Serialize)]
struct PlaylistRecord {
    id: String,
    owner_id: String,
    name: String,
    description: Option<String>,
    is_public: bool,
    created_at_ms: Option<u64>,
    updated_at_ms: Option<u64>,
}

impl PlaylistRecord {
    fn new(playlist: lyra_db::Playlist, owner_id: String) -> Self {
        Self {
            id: playlist.id,
            owner_id,
            name: playlist.name,
            description: playlist.description,
            is_public: playlist.is_public.unwrap_or(false),
            created_at_ms: playlist.created_at_ms,
            updated_at_ms: playlist.updated_at_ms,
        }
    }
}

#[derive(Clone, Serialize)]
struct PlaylistEntry {
    entry_id: String,
    track_id: String,
    position: u64,
}

/// A description as a request sets it: `""` clears it.
fn description_update(description: Option<String>) -> Option<Option<String>> {
    description.map(|description| (!description.is_empty()).then_some(description))
}

struct PlaylistCreate {
    name: String,
    description: Option<String>,
    is_public: Option<bool>,
    created_at_ms: Option<u64>,
    updated_at_ms: Option<u64>,
    track_ids: Vec<String>,
}

#[derive(Default)]
struct PlaylistFields {
    name: Option<String>,
    description: Option<Option<String>>,
    is_public: Option<bool>,
    updated_at_ms: Option<u64>,
}

impl PlaylistFields {
    fn into_service_request(self, playlist_id: QueryId) -> playlist_service::UpdatePlaylistRequest {
        playlist_service::UpdatePlaylistRequest {
            playlist_id,
            name: self.name,
            description: self.description,
            is_public: self.is_public,
            updated_at_ms: self.updated_at_ms,
        }
    }
}

struct PlaylistReplace {
    fields: PlaylistFields,
    track_ids: Vec<String>,
}

struct AddTracksOptions {
    position: Option<u64>,
}

#[derive(Default)]
struct PlaylistQuery {
    owner_id: Option<String>,
    ids: Option<Vec<String>>,
    exclude_ids: Vec<String>,
    search: Option<String>,
    favorite: Option<bool>,
    sort: Vec<(SortKey, Direction)>,
    offset: u64,
    limit: Option<u64>,
}

impl PlaylistQuery {
    fn personal(&self) -> Option<PersonalData> {
        let by_favorites = self.favorite.is_some()
            || self
                .sort
                .iter()
                .any(|(key, _)| *key == SortKey::FavoritedAt);
        by_favorites.then_some(PersonalData::Favorites)
    }
}

impl<'vm> luau::FromLuau<'vm> for PlaylistCreate {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let table: luau::Table = reader.read()?;
        Ok(Self {
            name: table.required_string_field("name")?,
            description: table
                .optional_string_field("description")?
                .filter(|description| !description.is_empty()),
            is_public: table.optional_bool_field("is_public")?,
            created_at_ms: args::optional_u64(&table, "created_at_ms")?,
            updated_at_ms: args::optional_u64(&table, "updated_at_ms")?,
            track_ids: args::optional_id_sequence(&table, "track_ids")?,
        })
    }
}

impl<'vm> luau::FromLuau<'vm> for PlaylistFields {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let table: luau::Table = reader.read()?;
        parse_fields(&table)
    }
}

impl<'vm> luau::FromLuau<'vm> for PlaylistReplace {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let table: luau::Table = reader.read()?;
        let fields = match table.optional_table_field("fields")? {
            Some(fields) => parse_fields(&fields)?,
            None => PlaylistFields::default(),
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
        Ok(Self { fields, track_ids })
    }
}

impl<'vm> luau::FromLuau<'vm> for AddTracksOptions {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let table: luau::Table = reader.read()?;
        let position = match table.get("position")? {
            luau::Value::Nil => None,
            value => Some(playlist_position(value, "position")?),
        };
        Ok(Self { position })
    }
}

impl<'vm> luau::FromLuau<'vm> for PlaylistQuery {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let table: luau::Table = reader.read()?;
        let sort = match table.optional_table_field("sort")? {
            None => Vec::new(),
            Some(terms) => args::array_values(&terms)?
                .into_iter()
                .map(|(index, term)| read_sort_term(index, term))
                .collect::<luau::runtime::Result<_>>()?,
        };
        Ok(Self {
            owner_id: table.optional_string_field("owner_id")?,
            ids: table
                .optional_table_field("ids")?
                .map(|ids| args::unique_ids(&ids))
                .transpose()?,
            exclude_ids: args::optional_unique_ids(&table, "exclude_ids")?,
            search: table.optional_string_field("search")?,
            favorite: table.optional_bool_field("favorite")?,
            sort,
            offset: args::optional_u64(&table, "offset")?.unwrap_or(0),
            limit: args::optional_u64(&table, "limit")?,
        })
    }
}

fn read_sort_term(index: i64, term: luau::Value) -> luau::runtime::Result<(SortKey, Direction)> {
    let luau::Value::Table(term) = term else {
        return Err(crate::plugins::runtime_error(format!(
            "sort entry {index} must be a table with a key, got {}",
            term.type_name()
        )));
    };
    let token = term.required_string_field("key")?;
    let key = SortKey::parse(&token).ok_or_else(|| {
        let supported = SortKey::KEYS
            .iter()
            .map(|(token, _)| *token)
            .collect::<Vec<_>>()
            .join(", ");
        crate::plugins::runtime_error(format!(
            "unsupported sort key: {token}. Supported keys: {supported}"
        ))
    })?;
    let direction = term
        .optional_string_field("order")?
        .as_deref()
        .map(Direction::parse)
        .transpose()
        .map_err(catalog::error)?
        .unwrap_or(Direction::Ascending);
    Ok((key, direction))
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

#[harmony_macros::module(
    id = "lyra/playlists",
    docs_feature = "docgen",
    extend = install_limits,
    describe = describe_limits
)]
impl PlaylistsModule {
    #[harmony(
        description = "The playlists with these ids that the caller can see, keyed by id. Every role sees its own playlists and other users' public ones; a deleted user's playlists are left out.",
        returns(HashMap<String, PlaylistRecord>)
    )]
    async fn get(
        ids: UniqueIds,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let db = db.read().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let ids = Resolved::of(&*db, ids.0)?;
        let mut found = Vec::with_capacity(ids.db_ids().len());
        for &playlist_db_id in ids.db_ids() {
            if !access::playlist_visible_to_listener(&*db, user_db_id, playlist_db_id)? {
                continue;
            }
            if let Some(playlist) = lyra_db::playlists::get_by_id(&*db, playlist_db_id)? {
                found.push((playlist_db_id, playlist));
            }
        }
        let values = records(&db, found)?
            .into_iter()
            .map(|(playlist_db_id, record)| {
                Ok((
                    playlist_db_id,
                    harmony_luau::serializable_to_luau_owned(record)?,
                ))
            })
            .collect::<luau::runtime::Result<Vec<_>>>()?;
        Ok(ids.keyed(values))
    }

    #[harmony(
        description = "The playlists the caller can see, as `get` sees them, that pass every filter, sorted and paged. Without `sort`, a search ranks by relevance and anything else is by name. Filtering by `favorite` or sorting by `favorited_at` needs the `lyra.favorites` scope.",
        returns(PlaylistPage)
    )]
    async fn query(
        query: Option<PlaylistQuery>,
        #[context] db: Db,
        #[context] principal: Principal,
        #[context] scopes: PersonalScopes,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let query = query.unwrap_or_default();
        if let Some(data) = query.personal() {
            scopes.require(data)?;
        }
        let db = db.read().await;
        crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let empty = || {
            catalog::page_table(Page::<PlaylistRecord> {
                items: Vec::new(),
                total: 0,
                offset: query.offset,
            })
        };
        let owner = match &query.owner_id {
            None => None,
            Some(owner_id) => match lyra_db::lookup::find_node_id_by_id(&*db, owner_id)
                .map_err(crate::plugins::runtime_error)?
            {
                Some(owner_db_id) if lyra_db::users::get_by_id(&*db, owner_db_id)?.is_some() => {
                    Some(owner_db_id)
                }
                _ => return empty(),
            },
        };
        let ids = query
            .ids
            .clone()
            .map(|ids| {
                Resolved::of(&*db, ids)
                    .map(|ids| ids.db_ids().iter().copied().collect::<HashSet<_>>())
            })
            .transpose()?;
        let exclude_ids = Resolved::of(&*db, query.exclude_ids.clone())?
            .db_ids()
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let (playlists, _) = playlist_service::list_visible(
            &db,
            &principal,
            playlist_service::PlaylistFilter {
                owner,
                favorite: query.favorite,
                ids,
                exclude_ids,
                owned: true,
            },
            &query.sort,
            query.search.as_deref(),
        )
        .map_err(crate::plugins::runtime_error)?;
        let total = playlists.len() as u64;
        let start = usize::try_from(query.offset).unwrap_or(usize::MAX);
        let page = playlists
            .into_iter()
            .skip(start)
            .take(query.limit.map_or(usize::MAX, |limit| {
                usize::try_from(limit).unwrap_or(usize::MAX)
            }))
            .filter_map(|playlist| {
                let playlist_db_id = playlist.db_id.clone().map(DbId::from)?;
                Some((playlist_db_id, playlist))
            })
            .collect::<Vec<_>>();
        let items = records(&db, page)?
            .into_iter()
            .map(|(_, record)| record)
            .collect();
        catalog::page_table(Page {
            items,
            total,
            offset: query.offset,
        })
    }

    #[harmony(
        description = "The entries of each of these playlists that the caller can see, in order and with repeats, keyed by playlist id. Entries whose tracks the caller cannot see are left out, keeping the positions they have among all of the playlist's entries; playlists the caller cannot see, as `get` sees them, are absent.",
        returns(HashMap<String, Vec<PlaylistEntry>>)
    )]
    async fn entries(
        playlist_ids: UniqueIds,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let db = db.read().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let playlists = Resolved::of(&*db, playlist_ids.0)?;
        let mut visible = Vec::with_capacity(playlists.db_ids().len());
        for &playlist_db_id in playlists.db_ids() {
            if access::playlist_visible_to_listener(&*db, user_db_id, playlist_db_id)? {
                visible.push(playlist_db_id);
            }
        }
        let mut links = playlist_service::get_tracks_many(&db, &visible)?;
        let mut values = Vec::with_capacity(visible.len());
        for playlist_db_id in visible {
            let entries = visible_entries(
                &db,
                &principal,
                links.remove(&playlist_db_id).unwrap_or_default(),
            )?;
            values.push((
                playlist_db_id,
                harmony_luau::serializable_to_luau_owned(entries)?,
            ));
        }
        Ok(playlists.keyed(values))
    }

    #[harmony(
        description = "Creates the caller's playlist with `track_ids` as its entries, in order and with repeats, in one transaction, and returns it. Raises, creating nothing, when a track is missing or hidden from the caller, or past `MAX_TRACK_IDS` track ids.",
        returns(PlaylistRecord)
    )]
    async fn create(
        request: PlaylistCreate,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        check_id_count("track ids", request.track_ids.len())?;
        let mut db = db.write().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let track_db_ids = accessible_tracks(&db, &principal, &request.track_ids)?;
        let (_, playlist) = playlist_service::create(
            &mut db,
            &playlist_service::CreatePlaylistRequest {
                user_db_id,
                name: request.name,
                description: request.description,
                is_public: request.is_public,
                created_at_ms: request.created_at_ms,
                updated_at_ms: request.updated_at_ms,
                track_db_ids,
            },
        )
        .map_err(crate::plugins::runtime_error)?;
        owned_record(&principal, playlist)
    }

    #[harmony(
        description = "Applies `fields` and returns the updated playlist, or nil if it is missing or not the caller's.",
        returns(Option<PlaylistRecord>)
    )]
    async fn update(
        playlist_id: String,
        fields: PlaylistFields,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        let mut db = db.write().await;
        let Some(playlist_db_id) = owned_playlist(&db, &principal, &playlist_id)? else {
            return Ok(luau::Value::Nil);
        };
        let request = fields.into_service_request(QueryId::Id(playlist_db_id));
        match playlist_service::update(&mut db, &request).map_err(crate::plugins::runtime_error)? {
            Some(playlist) => owned_record(&principal, playlist),
            None => Ok(luau::Value::Nil),
        }
    }

    #[harmony(
        description = "Applies `fields` and makes `track_ids` the entries, in order and with repeats, in one transaction. Returns the updated playlist, or nil if it is missing or not the caller's. Raises, changing nothing, when a track is missing or hidden from the caller, or past `MAX_TRACK_IDS` track ids.",
        returns(Option<PlaylistRecord>)
    )]
    async fn replace(
        playlist_id: String,
        request: PlaylistReplace,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        check_id_count("track ids", request.track_ids.len())?;
        let mut db = db.write().await;
        let Some(playlist_db_id) = owned_playlist(&db, &principal, &playlist_id)? else {
            return Ok(luau::Value::Nil);
        };
        let track_db_ids = accessible_tracks(&db, &principal, &request.track_ids)?;
        let update = request
            .fields
            .into_service_request(QueryId::Id(playlist_db_id));
        match playlist_service::replace(&mut db, &update, &track_db_ids)
            .map_err(crate::plugins::runtime_error)?
        {
            Some(playlist) => owned_record(&principal, playlist),
            None => Ok(luau::Value::Nil),
        }
    }

    #[harmony(
        description = "Adds `track_ids`, in order and with repeats, in one transaction: at the 0-based `position` when given (past the end appends), and at the end otherwise. Returns their entry ids, empty for no tracks, or nil if the playlist is missing or not the caller's. Raises, adding nothing, when a track is missing or hidden from the caller, past `MAX_TRACK_IDS` track ids, or when `position` is not a non-negative integer.",
        returns(Option<Vec<String>>)
    )]
    async fn add_tracks(
        playlist_id: String,
        track_ids: IdSequence,
        options: Option<AddTracksOptions>,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        let position = options.and_then(|options| options.position);
        check_id_count("track ids", track_ids.0.len())?;
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
        )
        .map_err(crate::plugins::runtime_error)?
        .into_iter()
        .map(|link| link.entry_id)
        .collect::<Vec<_>>();
        harmony_luau::serializable_to_luau_owned(entry_ids)
    }

    #[harmony(
        description = "Removes the entries in one transaction. Returns false, removing nothing, if the playlist is not the caller's or an entry is not in it. Raises on an entry listed twice or past `MAX_TRACK_IDS` entry ids."
    )]
    async fn remove_entries(
        playlist_id: String,
        entry_ids: DistinctIds,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let entry_ids = entry_ids.0;
        check_id_count("entry ids", entry_ids.len())?;
        let mut db = db.write().await;
        let Some(playlist_db_id) = owned_playlist(&db, &principal, &playlist_id)? else {
            return Ok(false);
        };
        let held = playlist_service::get_tracks(&db, QueryId::Id(playlist_db_id))?
            .into_iter()
            .map(|link| link.entry_id)
            .collect::<HashSet<_>>();
        if !entry_ids.iter().all(|entry_id| held.contains(entry_id)) {
            return Ok(false);
        }
        playlist_service::remove_tracks(&mut db, playlist_db_id, &entry_ids)
            .map_err(crate::plugins::runtime_error)?;
        Ok(true)
    }

    #[harmony(
        description = "Moves an entry to a 0-based position, clamping past the end. Returns false if the playlist is not the caller's or the entry is not in it. A move to the current position succeeds."
    )]
    async fn move_entry(
        playlist_id: String,
        entry_id: String,
        position: PlaylistPosition,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let mut db = db.write().await;
        let Some(playlist_db_id) = owned_playlist(&db, &principal, &playlist_id)? else {
            return Ok(false);
        };
        if playlist_service::find_entry(&db, playlist_db_id, &entry_id)?.is_none() {
            return Ok(false);
        }
        playlist_service::move_track(&mut db, playlist_db_id, &entry_id, position.0)?;
        Ok(true)
    }

    #[harmony(
        description = "Deletes a playlist the caller can see, as `get` sees them, and owns, or any of those for an admin. Returns true when deleted, and false otherwise."
    )]
    async fn delete(
        playlist_id: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let mut db = db.write().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let Some(playlist_db_id) = crate::plugins::ids::db_id(&*db, &playlist_id)? else {
            return Ok(false);
        };
        if !access::playlist_visible_to_listener(&*db, user_db_id, playlist_db_id)?
            || !access::playlist_deletable(&*db, &principal, playlist_db_id)?
        {
            return Ok(false);
        }
        let deleted = playlist_service::delete(&mut db, QueryId::Id(playlist_db_id))?;
        Ok(deleted.is_some())
    }
}

fn check_id_count(what: &'static str, count: usize) -> luau::runtime::Result<()> {
    playlist_service::check_id_count(what, count).map_err(crate::plugins::runtime_error)
}

/// The records of these playlists with their owners, in order. Playlists without an owner, which
/// the caller never sees, are left out.
fn records(
    db: &DbAny,
    playlists: Vec<(DbId, lyra_db::Playlist)>,
) -> luau::runtime::Result<Vec<(DbId, PlaylistRecord)>> {
    let playlist_db_ids = playlists
        .iter()
        .map(|(playlist_db_id, _)| *playlist_db_id)
        .collect::<Vec<_>>();
    let mut owners = playlist_service::owner_ids(db, &playlist_db_ids)?;
    Ok(playlists
        .into_iter()
        .filter_map(|(playlist_db_id, playlist)| {
            let owner_id = owners.remove(&playlist_db_id)?;
            Some((playlist_db_id, PlaylistRecord::new(playlist, owner_id)))
        })
        .collect())
}

/// The record of a playlist the caller owns.
fn owned_record(
    principal: &Principal,
    playlist: lyra_db::Playlist,
) -> luau::runtime::Result<luau::Value> {
    harmony_luau::serializable_to_luau_owned(PlaylistRecord::new(
        playlist,
        principal.user_public_id.clone(),
    ))
}

/// The caller's playlist `playlist_id`, checked under `db`'s guard.
fn owned_playlist(
    db: &DbAny,
    principal: &Principal,
    playlist_id: &str,
) -> luau::runtime::Result<Option<DbId>> {
    let Some(playlist_db_id) = crate::plugins::ids::db_id(db, playlist_id)? else {
        return Ok(None);
    };
    Ok(access::playlist_owned(db, principal, playlist_db_id)?.then_some(playlist_db_id))
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
        track_db_ids
            .push(crate::plugins::ids::db_id(db, track_id)?.ok_or_else(|| not_found(track_id))?);
    }
    let accessible = access::accessible_entities(db, principal, &track_db_ids)?;
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

/// The entries whose tracks the caller can see, with the tracks' public ids.
fn visible_entries(
    db: &DbAny,
    principal: &Principal,
    links: Vec<playlist_service::PlaylistTrackLink>,
) -> luau::runtime::Result<Vec<PlaylistEntry>> {
    let track_db_ids = links
        .iter()
        .map(|link| link.track_db_id)
        .collect::<Vec<_>>();
    let accessible = access::accessible_entities(db, principal, &track_db_ids)?;
    let public_ids = crate::plugins::ids::public_ids(db, &track_db_ids)?;
    Ok(links
        .into_iter()
        .filter(|link| accessible.contains(&link.track_db_id))
        .filter_map(|link| {
            Some(PlaylistEntry {
                track_id: public_ids.get(&link.track_db_id)?.clone(),
                entry_id: link.entry_id,
                position: link.position,
            })
        })
        .collect())
}

fn parse_fields(table: &luau::Table) -> luau::runtime::Result<PlaylistFields> {
    Ok(PlaylistFields {
        name: table.optional_string_field("name")?,
        description: description_update(table.optional_string_field("description")?),
        is_public: table.optional_bool_field("is_public")?,
        updated_at_ms: args::optional_u64(table, "updated_at_ms")?,
    })
}

fn field(name: &'static str, ty: LuauType) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description: None,
    }
}

fn described_field(name: &'static str, ty: LuauType, description: &'static str) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description: Some(description),
    }
}

/// The fields a create, update or replace sets.
fn field_descriptors(creating: bool) -> Vec<FieldDescriptor> {
    let is_public = Option::<bool>::luau_type();
    vec![
        if creating {
            field("name", String::luau_type())
        } else {
            field("name", Option::<String>::luau_type())
        },
        described_field(
            "description",
            Option::<String>::luau_type(),
            "`\"\"` means none.",
        ),
        if creating {
            described_field("is_public", is_public, "Defaults to false.")
        } else {
            field("is_public", is_public)
        },
        field("updated_at_ms", Option::<u64>::luau_type()),
    ]
}

macro_rules! named_interface {
    ($ty:ty, $name:literal, $fields:expr) => {
        impl LuauTypeInfo for $ty {
            fn luau_type() -> LuauType {
                LuauType::named($name)
            }
        }

        impl DescribeInterface for $ty {
            fn interface_descriptor() -> InterfaceDescriptor {
                let mut descriptor = InterfaceDescriptor::new($name, None);
                descriptor.fields.extend($fields);
                descriptor
            }
        }
    };
}

named_interface!(
    PlaylistRecord,
    "Playlist",
    [
        field("id", String::luau_type()),
        described_field("owner_id", String::luau_type(), "The owning user's id."),
        field("name", String::luau_type()),
        field("description", Option::<String>::luau_type()),
        field("is_public", bool::luau_type()),
        field("created_at_ms", Option::<u64>::luau_type()),
        field("updated_at_ms", Option::<u64>::luau_type()),
    ]
);

named_interface!(
    PlaylistEntry,
    "PlaylistEntry",
    [
        field("entry_id", String::luau_type()),
        field("track_id", String::luau_type()),
        described_field(
            "position",
            u64::luau_type(),
            "0-based, among all of the playlist's entries.",
        ),
    ]
);

named_interface!(PlaylistCreate, "PlaylistCreate", {
    let mut fields = field_descriptors(true);
    fields.insert(3, field("created_at_ms", Option::<u64>::luau_type()));
    fields.push(described_field(
        "track_ids",
        Option::<Vec<String>>::luau_type(),
        "The entries, in order and with repeats. Defaults to none.",
    ));
    fields
});

named_interface!(PlaylistFields, "PlaylistFields", field_descriptors(false));

named_interface!(
    PlaylistReplace,
    "PlaylistReplace",
    [
        described_field(
            "fields",
            Option::<PlaylistFields>::luau_type(),
            "Defaults to changing none.",
        ),
        field("track_ids", Vec::<String>::luau_type()),
    ]
);

named_interface!(
    AddTracksOptions,
    "AddTracksOptions",
    [described_field(
        "position",
        Option::<u64>::luau_type(),
        "0-based; past the end appends. Defaults to the end.",
    )]
);

named_interface!(
    PlaylistQuery,
    "PlaylistQuery",
    [
        described_field(
            "owner_id",
            Option::<String>::luau_type(),
            "Only this user's playlists: all of them for the caller, the public ones for anyone else. An unknown user gives an empty page.",
        ),
        field("ids", Option::<Vec<String>>::luau_type()),
        field("exclude_ids", Option::<Vec<String>>::luau_type()),
        described_field(
            "search",
            Option::<String>::luau_type(),
            "Each word must start a word of the name, ignoring case, accents, apostrophes and punctuation; no character is an operator.",
        ),
        described_field(
            "favorite",
            Option::<bool>::luau_type(),
            "Only playlists the caller has favorited (`true`) or has not (`false`).",
        ),
        field(
            "sort",
            LuauType::optional(LuauType::array(LuauType::object(vec![
                field("key", LuauType::named("PlaylistSortKey")),
                field(
                    "order",
                    LuauType::optional(LuauType::union(vec![
                        LuauType::string_literal("ascending"),
                        LuauType::string_literal("descending"),
                    ])),
                ),
            ]))),
        ),
        described_field("offset", Option::<u64>::luau_type(), "Defaults to 0."),
        described_field(
            "limit",
            Option::<u64>::luau_type(),
            "Defaults to every remaining playlist.",
        ),
    ]
);

/// The `query` result, named for its alias.
#[cfg(feature = "docgen")]
struct PlaylistPage;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for PlaylistPage {
    fn luau_type() -> LuauType {
        LuauType::named("PlaylistPage")
    }
}

#[cfg(feature = "docgen")]
fn type_aliases() -> Vec<TypeAliasDescriptor> {
    vec![
        TypeAliasDescriptor::new(
            "PlaylistSortKey",
            LuauType::union(
                SortKey::KEYS
                    .iter()
                    .map(|(token, _)| LuauType::string_literal(token))
                    .collect(),
            ),
            None,
        ),
        TypeAliasDescriptor::new(
            "PlaylistPage",
            LuauType::object(vec![
                field("items", LuauType::array(LuauType::named("Playlist"))),
                field("total", u64::luau_type()),
                field("offset", u64::luau_type()),
            ]),
            None,
        ),
    ]
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    use harmony_luau::DescribeModule;

    harmony_luau::render_definition_file_with_support(
        &PlaylistsModule::module_descriptor(),
        &type_aliases(),
        &[
            PlaylistRecord::interface_descriptor(),
            PlaylistEntry::interface_descriptor(),
            PlaylistCreate::interface_descriptor(),
            PlaylistFields::interface_descriptor(),
            PlaylistReplace::interface_descriptor(),
            AddTracksOptions::interface_descriptor(),
            PlaylistQuery::interface_descriptor(),
        ],
        &[],
    )
}
