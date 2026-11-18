// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    DbId,
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
    DescribeModule,
    TypeAliasDescriptor,
    render_definition_file_with_support,
};
use serde::{
    Deserialize,
    Serialize,
};
#[cfg(feature = "docgen")]
use std::collections::HashMap;

use crate::plugins::args::UniqueIds;
use crate::plugins::db::{
    Db,
    ViewerDb,
};
use crate::plugins::{
    auth::DispatchPrincipal,
    catalog,
    ids,
};
use crate::services::catalog::genres::GenreKey;
use crate::services::{
    self,
    catalog::{
        genres::{
            GenreFilter,
            Genres,
        },
        lookups,
    },
};
use lyra_db::genres::{
    ResolveExternalId,
    ResolveGenre,
};

pub(crate) struct GenresModule;

/// The `query` argument, read with its page bounds. Its nested matches sit in its filter from
/// the start, so the query reads all they read, and only their ids wait to be resolved.
struct GenreQuery {
    request: catalog::PagedQuery<Genres>,
    releases: Option<catalog::MatchIds>,
    tracks: Option<catalog::MatchIds>,
}

impl<'vm> luau::FromLuau<'vm> for GenreQuery {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let opts: luau::Table = reader.read()?;
        let mut request = catalog::read_query::<Genres>(&opts, GenreFilter::default())?;
        let filter = &mut request.query.matching.filter;
        let releases = catalog::PendingMatch::nested(
            &opts,
            "releases",
            crate::plugins::releases::read_filter,
        )?
        .map(|nested| {
            filter.releases = Some(nested.matching);
            nested.ids
        });
        let tracks =
            catalog::PendingMatch::nested(&opts, "tracks", crate::plugins::tracks::read_filter)?
                .map(|nested| {
                    filter.tracks = Some(nested.matching);
                    nested.ids
                });
        Ok(Self {
            request,
            releases,
            tracks,
        })
    }
}

impl GenreQuery {
    /// The query with every id it names resolved under the guard it reads through.
    fn resolve(self, db: &DbAny) -> luau::runtime::Result<catalog::PagedQuery<Genres>> {
        let Self {
            mut request,
            releases,
            tracks,
        } = self;
        request.resolve_ids(db)?;
        let filter = &mut request.query.matching.filter;
        if let (Some(ids), Some(matching)) = (releases, filter.releases.as_mut()) {
            ids.apply(db, matching)?;
        }
        if let (Some(ids), Some(matching)) = (tracks, filter.tracks.as_mut()) {
            ids.apply(db, matching)?;
        }
        Ok(request)
    }
}

impl LuauTypeInfo for GenreQuery {
    fn luau_type() -> LuauType {
        LuauType::named("GenreQuery")
    }
}

#[cfg(feature = "docgen")]
struct GenrePage;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for GenrePage {
    fn luau_type() -> LuauType {
        LuauType::named("GenrePage")
    }
}

impl<'vm> luau::FromLuau<'vm> for GenreResolveRequest {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let table: luau::Table = reader.read()?;
        genre_request_from_table(table)
    }
}

#[harmony_macros::module(id = "lyra/genres", docs_feature = "docgen")]
impl GenresModule {
    #[harmony(
        description = "The genres of the releases the caller can see that pass every filter, sorted and paged. Without `sort`, a search ranks by relevance and anything else is by name. Sorting by `listen_count` or `last_played_at` needs the `lyra.listens` scope, and reading personal data through `releases` or `tracks` needs the scope `releases.query` or `tracks.query` needs for it.",
        returns(GenrePage)
    )]
    async fn query(
        query: GenreQuery,
        #[context] db: ViewerDb,
        #[context] scopes: catalog::PersonalScopes,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        scopes.check(&query.request.query)?;
        let (db, viewer) = db.read().await?;
        let request = query.resolve(&db)?;
        let page =
            services::catalog::page(&db, &viewer, &request.query, request.offset, request.limit)
                .map_err(catalog::error)?;
        let page = catalog::counted(
            &db,
            &viewer,
            &request.query,
            page,
            &[
                ("release_count", GenreKey::ReleaseCount),
                ("track_count", GenreKey::TrackCount),
            ],
            |genre| genre.db_id.clone().map(DbId::from),
            GenreRecord::from,
        )
        .map_err(catalog::error)?;
        catalog::page_table(page)
    }

    #[harmony(
        description = "The id of the genre matching the request, created when there is none. Returns nil when the caller cannot manage libraries. Needs the `lyra.genres.manage` scope too."
    )]
    async fn resolve(
        request: GenreResolveRequest,
        #[context] caller: DispatchPrincipal,
        #[context] vm: luau::Vm,
    ) -> luau::runtime::Result<Option<String>> {
        if !caller.can_manage_libraries() {
            return Ok(None);
        }
        let db = crate::plugins::db::get(&vm)?;
        let mut db = db.write().await;
        if !caller.manages_libraries_now(&db)? {
            return Ok(None);
        }
        let genre_id =
            resolve_genre_from_request(&mut db, &request).map_err(crate::plugins::runtime_error)?;
        ids::public_id(&*db, genre_id).map(Some)
    }

    #[harmony(
        description = "Links the genre `child_id` under `parent_id`; linking them again is harmless. Raises when either is not a genre, and when the caller cannot manage libraries. Needs the `lyra.genres.manage` scope too."
    )]
    async fn add_parent(
        child_id: String,
        parent_id: String,
        #[context] caller: DispatchPrincipal,
        #[context] vm: luau::Vm,
    ) -> luau::runtime::Result<()> {
        let refused = || {
            crate::plugins::runtime_error(crate::services::auth::Recheck::Missing(
                lyra_db::Permission::ManageLibraries,
            ))
        };
        if !caller.can_manage_libraries() {
            return Err(refused());
        }
        let db = crate::plugins::db::get(&vm)?;
        let mut db = db.write().await;
        if !caller.manages_libraries_now(&db)? {
            return Err(refused());
        }
        let genre = |id: &str, name: &str| {
            ids::db_id_of(&db, id, "Genre")?
                .ok_or_else(|| crate::plugins::runtime_error(format!("{name} {id} is not a genre")))
        };
        let child = genre(&child_id, "child_id")?;
        let parent = genre(&parent_id, "parent_id")?;
        lyra_db::genres::link_to_parent(&mut db, child, parent)
            .map_err(crate::plugins::runtime_error)
    }

    #[harmony(
        description = "The genres with these ids that the caller can see, keyed by id.",
        returns(HashMap<String, GenreRecord>)
    )]
    async fn get(
        ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(&db, ids.0, |db, viewer, ids| {
            Ok(lookups::get::<Genres>(db, viewer, ids)?
                .into_iter()
                .map(|(id, genre)| (id, GenreRecord::from(genre)))
                .collect())
        })
        .await
    }

    #[harmony(
        description = "The genres of each of these releases that the caller can see, keyed by release id.",
        returns(HashMap<String, Vec<GenreRecord>>)
    )]
    async fn by_release(
        release_ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(&db, release_ids.0, |db, viewer, ids| {
            Ok(lookups::genres_by_release(db, viewer, ids)?
                .into_iter()
                .map(|(id, genres)| {
                    (
                        id,
                        genres
                            .into_iter()
                            .map(GenreRecord::from)
                            .collect::<Vec<_>>(),
                    )
                })
                .collect())
        })
        .await
    }

    #[harmony(
        description = "The genre with exactly this name, ignoring case. Not scoped to any caller.",
        returns(Option<GenreRecord>)
    )]
    async fn find_by_name(name: String, #[context] db: Db) -> luau::runtime::Result<luau::Value> {
        let trimmed = name.trim().to_string();
        if trimmed.is_empty() {
            return Ok(luau::Value::Nil);
        }
        let db = db.read().await;
        let genre = match lyra_db::genres::find_by_name(&db, &trimmed)? {
            Some(db_id) => lyra_db::genres::get_by_id(&db, db_id)?,
            None => None,
        };
        optional_genre_value(&db, genre)
    }
}

#[derive(Debug, Deserialize)]
struct GenreExternalId {
    provider_id: String,
    id_type: String,
    id_value: String,
}

#[derive(Debug, Deserialize)]
struct GenreAliasInput {
    name: String,
    locale: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GenreResolveRequest {
    name: String,
    external_id: Option<GenreExternalId>,
    aliases: Option<Vec<GenreAliasInput>>,
}

fn genre_request_from_table(table: luau::Table) -> luau::runtime::Result<GenreResolveRequest> {
    let value = harmony_serde::luau_to_json(&luau::Value::Table(table))?;
    serde_json::from_value(value).map_err(crate::plugins::runtime_error)
}

fn resolve_genre_from_request(
    db: &mut agdb::DbAny,
    request: &GenreResolveRequest,
) -> anyhow::Result<DbId> {
    let aliases_owned = request
        .aliases
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|alias| (alias.name.clone(), alias.locale.clone()))
        .collect::<Vec<_>>();
    let aliases_refs = aliases_owned
        .iter()
        .map(|(name, locale)| (name.as_str(), locale.as_deref()))
        .collect::<Vec<_>>();
    let external_id = request
        .external_id
        .as_ref()
        .map(|external_id| ResolveExternalId {
            provider_id: &external_id.provider_id,
            id_type: &external_id.id_type,
            id_value: &external_id.id_value,
        });

    lyra_db::genres::resolve(
        db,
        &ResolveGenre {
            name: &request.name,
            aliases: &aliases_refs,
            external_id,
        },
    )
}

#[derive(Serialize)]
pub(crate) struct GenreRecord {
    id: String,
    name: String,
    available: bool,
}

impl From<services::catalog::Listed<lyra_db::genres::Genre>> for GenreRecord {
    fn from(genre: services::catalog::Listed<lyra_db::genres::Genre>) -> Self {
        Self {
            id: genre.item.id,
            name: genre.item.name,
            available: genre.available,
        }
    }
}

fn optional_genre_value(
    db: &DbAny,
    genre: Option<lyra_db::genres::Genre>,
) -> luau::runtime::Result<luau::Value> {
    let Some(genre) = genre else {
        return Ok(luau::Value::Nil);
    };
    let availability = services::catalog::Availability::of(db)?;
    let available = genre
        .db_id
        .clone()
        .map(DbId::from)
        .is_none_or(|id| availability.genre(id));
    harmony_luau::serializable_to_luau_owned(GenreRecord {
        id: genre.id,
        name: genre.name,
        available,
    })
}

impl LuauTypeInfo for GenreRecord {
    fn luau_type() -> LuauType {
        LuauType::named("GenreInfo")
    }
}

impl DescribeInterface for GenreRecord {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("GenreInfo", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "id",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "name",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "available",
                ty: bool::luau_type(),
                description: Some("Whether any of the genre's releases can play."),
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for GenreExternalId {
    fn luau_type() -> LuauType {
        LuauType::named("GenreExternalId")
    }
}

impl DescribeInterface for GenreExternalId {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("GenreExternalId", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "provider_id",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "id_type",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "id_value",
                ty: String::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for GenreAliasInput {
    fn luau_type() -> LuauType {
        LuauType::named("GenreAliasInput")
    }
}

impl DescribeInterface for GenreAliasInput {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("GenreAliasInput", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "name",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "locale",
                ty: Option::<String>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for GenreResolveRequest {
    fn luau_type() -> LuauType {
        LuauType::named("GenreResolveRequest")
    }
}

impl DescribeInterface for GenreResolveRequest {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("GenreResolveRequest", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "name",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "external_id",
                ty: Option::<GenreExternalId>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "aliases",
                ty: Option::<Vec<GenreAliasInput>>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    let mut query = InterfaceDescriptor::new("GenreQuery", None);
    query.fields.extend([
        FieldDescriptor {
            name: "ids",
            ty: Option::<Vec<String>>::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "exclude_ids",
            ty: Option::<Vec<String>>::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "library_id",
            ty: Option::<String>::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "releases",
            ty: LuauType::optional(LuauType::named("ReleaseMatch")),
            description: Some(
                "Genres of the releases this keeps, as `releases.query` keeps them; `{ ids = release_ids }` names releases by id. With `library_id` and `tracks`, a release must pass all three, and `release_count` and `track_count` count only those releases.",
            ),
        },
        FieldDescriptor {
            name: "tracks",
            ty: LuauType::optional(LuauType::named("TrackMatch")),
            description: Some(
                "Genres of the releases the tracks this keeps are kept through, as `tracks.query` keeps them: each track's releases the caller can see, within the match's own `library_id`, `release_ids` and `genre_ids`. So filtering the match by any genre found keeps a track it came from. A genre's `track_count` then counts only those tracks, and its `release_count` only the releases holding one.",
            ),
        },
    ]);
    query.fields.extend(catalog::query_fields("GenreSortKey"));
    render_definition_file_with_support(
        &GenresModule::module_descriptor(),
        &std::iter::once(TypeAliasDescriptor::new(
            "GenreSummary",
            LuauType::intersection(vec![
                LuauType::named("GenreInfo"),
                LuauType::object(vec![
                    FieldDescriptor {
                        name: "release_count",
                        ty: u64::luau_type(),
                        description: Some(
                            "The visible releases in the genre that `library_id`, `releases` and `tracks` keep.",
                        ),
                    },
                    FieldDescriptor {
                        name: "track_count",
                        ty: u64::luau_type(),
                        description: Some(
                            "The tracks on those releases, only those `tracks` keeps when it is set.",
                        ),
                    },
                ]),
            ]),
            None,
        ))
        .chain(catalog::type_aliases::<GenreKey>(
            "GenreSortKey",
            "GenrePage",
            "GenreSummary",
        ))
        .chain([catalog::credit_role_alias()])
        .collect::<Vec<_>>(),
        &[
            query,
            crate::plugins::releases::release_match(),
            crate::plugins::tracks::track_match(),
            GenreRecord::interface_descriptor(),
            GenreExternalId::interface_descriptor(),
            GenreAliasInput::interface_descriptor(),
            GenreResolveRequest::interface_descriptor(),
        ],
        &[],
    )
}
