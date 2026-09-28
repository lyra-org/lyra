// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::DbId;
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
    TypeAliasDescriptor,
    render_definition_file_with_support,
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::plugins::args;
use crate::plugins::catalog;
use crate::plugins::db::{
    self,
    DbAsync,
    genres::{
        ResolveExternalId,
        ResolveGenre,
    },
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

#[derive(Clone, Default)]
pub(crate) struct GenresModuleStore {
    db: Option<DbAsync>,
}

impl GenresModuleStore {
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: DbAsync) -> Self {
        Self { db: Some(db) }
    }

    fn db(&self) -> luau::runtime::Result<DbAsync> {
        self.db.clone().ok_or_else(|| {
            crate::plugins::runtime_error("lyra/genres requires a database-backed plugin executor")
        })
    }
}

struct GenresModule;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/genres")
        .capability("lyra.genres")
        .function(query_spec())
        .function(resolve_spec())
        .function(add_parent_spec())
        .function(get_spec())
        .function(by_release_spec())
        .function(find_by_name_spec())
        .function(artwork_release_ids_spec())
        .install(|_| Ok(ModuleExport::new(GenresModule)))
}

fn query_spec() -> FunctionSpec {
    FunctionSpec::async_fn("query")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("query")
        .args::<luau::Table>()
        .returns::<luau::Value>()
        .call_async(std::sync::Arc::new(query_callback))
}

fn query_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let opts: luau::Table = frame.args.read_named("query")?;
    let filter = GenreFilter {
        ids: catalog::optional_ids(frame.vm, &opts, "ids")?,
        exclude_ids: args::optional_unique_ids(frame.vm, &opts, "exclude_ids")?,
        library: args::optional_positive_id(frame.vm, &opts, "library_id")?,
        releases: catalog::optional_ids(frame.vm, &opts, "release_ids")?,
    };
    let request = catalog::read_query::<Genres>(frame.vm, &opts, filter)?;
    let principal = crate::plugins::auth::dispatch_principal(&frame.context)?;
    let db = frame.vm.data().get::<GenresModuleStore>()?.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let viewer = catalog::viewer(&*db, principal)?;
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
        catalog::page_table(page).map(luau::Value::TableData)
    }))
}

fn get_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("ids")
        .args::<Vec<u64>>()
        .returns::<luau::Table>()
        .call_async(std::sync::Arc::new(get_callback))
}

fn by_release_spec() -> FunctionSpec {
    FunctionSpec::async_fn("by_release")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("release_ids")
        .args::<Vec<u64>>()
        .returns::<luau::Table>()
        .call_async(std::sync::Arc::new(by_release_callback))
}

fn get_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let db = frame.vm.data().get::<GenresModuleStore>()?.db()?;
    catalog::keyed_lookup(&mut frame, db, "ids", |db, viewer, ids| {
        Ok(lookups::get::<Genres>(db, viewer, ids)?
            .into_iter()
            .map(|(id, genre)| (id, GenreRecord::from(genre)))
            .collect())
    })
}

fn by_release_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let db = frame.vm.data().get::<GenresModuleStore>()?.db()?;
    catalog::keyed_lookup(&mut frame, db, "release_ids", |db, viewer, ids| {
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
}

fn resolve_spec() -> FunctionSpec {
    FunctionSpec::async_fn("resolve")
        .arg_name("request")
        .args::<luau::Table>()
        .returns::<i64>()
        .call_async(std::sync::Arc::new(resolve_callback))
}

fn add_parent_spec() -> FunctionSpec {
    FunctionSpec::async_fn("add_parent")
        .named_arg::<i64>("child_id")
        .named_arg::<i64>("parent_id")
        .returns::<()>()
        .call_async(std::sync::Arc::new(add_parent_callback))
}

// Unscoped: public artwork routes compose a genre's cover without a caller.
fn artwork_release_ids_spec() -> FunctionSpec {
    FunctionSpec::async_fn("artwork_release_ids")
        .arg_name("genre_id")
        .args::<i64>()
        .returns::<Vec<i64>>()
        .call_async(std::sync::Arc::new(artwork_release_ids_callback))
}

fn artwork_release_ids_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let genre_id = args::positive_id(frame.args.read_named("genre_id")?, "genre_id")?;
    let db = frame.vm.data().get::<GenresModuleStore>()?.db()?;
    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        if db::genres::get_by_id(&*db, genre_id)
            .map_err(crate::plugins::runtime_error)?
            .is_none()
        {
            return harmony_luau::serializable_to_luau_owned(Vec::<i64>::new());
        }
        let releases = db::genres::get_releases_many(&*db, &[genre_id])
            .map_err(crate::plugins::runtime_error)?
            .remove(&genre_id)
            .unwrap_or_default();
        harmony_luau::serializable_to_luau_owned(
            releases.into_iter().map(|id| id.0).collect::<Vec<_>>(),
        )
    }))
}

fn find_by_name_spec() -> FunctionSpec {
    FunctionSpec::async_fn("find_by_name")
        .arg_name("name")
        .args::<String>()
        .returns::<Option<GenreRecord>>()
        .call_async(std::sync::Arc::new(find_by_name_callback))
}

fn resolve_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let request_table: luau::Table = frame.args.read_named("request")?;
    let request = genre_request_from_table(frame.vm, request_table)?;
    let store = frame.vm.data().get::<GenresModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let genre_id = {
            let mut db = db.write().await;
            resolve_genre_from_request(&mut db, &request).map_err(crate::plugins::runtime_error)
        }?;

        Ok(luau::Value::from(genre_id.0))
    }))
}

fn add_parent_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let child_id: i64 = frame.args.read_named("child_id")?;
    let parent_id: i64 = frame.args.read_named("parent_id")?;
    let store = frame.vm.data().get::<GenresModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let mut db = db.write().await;
        db::genres::link_to_parent(&mut db, DbId(child_id), DbId(parent_id))
            .map_err(crate::plugins::runtime_error)
    }))
}

fn find_by_name_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let name: String = frame.args.read_named("name")?;
    let store = frame.vm.data().get::<GenresModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    let trimmed = name.trim().to_string();
    if trimmed.is_empty() {
        return Ok(luau::ScheduledFuture::new(async { Ok(luau::Value::Nil) }));
    }

    Ok(luau::ScheduledFuture::new(async move {
        let genre = {
            let db = db.read().await;
            match db::genres::find_by_name(&db, &trimmed).map_err(crate::plugins::runtime_error)? {
                Some(db_id) => {
                    db::genres::get_by_id(&db, db_id).map_err(crate::plugins::runtime_error)?
                }
                None => None,
            }
        };

        optional_genre_value(genre)
    }))
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

fn genre_request_from_table(
    vm: &luau::Vm,
    table: luau::Table,
) -> luau::runtime::Result<GenreResolveRequest> {
    let value = harmony_serde::luau_to_json(vm, &luau::Value::Table(table), 0)?;
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

    db::genres::resolve(
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
    db_id: Option<i64>,
    id: String,
    name: String,
}

impl From<db::genres::Genre> for GenreRecord {
    fn from(genre: db::genres::Genre) -> Self {
        Self {
            db_id: genre.db_id.map(DbId::from).map(|id| id.0),
            id: genre.id,
            name: genre.name,
        }
    }
}

fn optional_genre_value(genre: Option<db::genres::Genre>) -> luau::runtime::Result<luau::Value> {
    match genre {
        Some(genre) => harmony_luau::serializable_to_luau_owned(GenreRecord::from(genre)),
        None => Ok(luau::Value::Nil),
    }
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
                name: "db_id",
                ty: Option::<i64>::luau_type(),
                description: None,
            },
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
fn param(name: &'static str, ty: LuauType) -> ParameterDescriptor {
    ParameterDescriptor {
        name,
        ty,
        description: None,
        variadic: false,
    }
}

#[cfg(feature = "docgen")]
fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "Genres",
        local_name: "genres",
        description: None,
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["query"],
                description: Some(
                    "The genres of the releases the caller can see that pass every filter, sorted and paged. Without `sort`, a search ranks by relevance and anything else is by name.",
                ),
                params: vec![param("query", LuauType::named("GenreQuery"))],
                returns: vec![LuauType::named("GenrePage")],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["resolve"],
                description: None,
                params: vec![param("request", GenreResolveRequest::luau_type())],
                returns: vec![i64::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["add_parent"],
                description: None,
                params: vec![
                    param("child_id", i64::luau_type()),
                    param("parent_id", i64::luau_type()),
                ],
                returns: Vec::new(),
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get"],
                description: Some(
                    "The genres with these ids that the caller can see, keyed by id.",
                ),
                params: vec![param("ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(u64::luau_type(), GenreRecord::luau_type())],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["by_release"],
                description: Some(
                    "The genres of each of these releases that the caller can see, keyed by release id.",
                ),
                params: vec![param("release_ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(
                    u64::luau_type(),
                    Vec::<GenreRecord>::luau_type(),
                )],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["artwork_release_ids"],
                description: Some(
                    "The ids of the releases in this genre. Not scoped to any caller: it is only for composing the genre's artwork on public artwork routes, which must return nothing but image bytes.",
                ),
                params: vec![param("genre_id", i64::luau_type())],
                returns: vec![Vec::<i64>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["find_by_name"],
                description: Some(
                    "The genre with exactly this name, ignoring case. Not scoped to any caller.",
                ),
                params: vec![param("name", String::luau_type())],
                returns: vec![Option::<GenreRecord>::luau_type()],
                yields: true,
            },
        ],
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    let mut query = InterfaceDescriptor::new("GenreQuery", None);
    query.fields.extend([
        FieldDescriptor {
            name: "ids",
            ty: Option::<Vec<u64>>::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "exclude_ids",
            ty: Option::<Vec<u64>>::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "library_id",
            ty: Option::<u64>::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "release_ids",
            ty: Option::<Vec<u64>>::luau_type(),
            description: Some("Genres of these releases."),
        },
    ]);
    query.fields.extend(catalog::query_fields("GenreSortKey"));
    render_definition_file_with_support(
        &module_descriptor(),
        &std::iter::once(TypeAliasDescriptor::new(
            "GenreSummary",
            LuauType::intersection(vec![
                LuauType::named("GenreInfo"),
                LuauType::object(vec![
                    FieldDescriptor {
                        name: "release_count",
                        ty: u64::luau_type(),
                        description: Some("The visible releases in the genre."),
                    },
                    FieldDescriptor {
                        name: "track_count",
                        ty: u64::luau_type(),
                        description: Some("The tracks on those releases."),
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
        .collect::<Vec<_>>(),
        &[
            query,
            GenreRecord::interface_descriptor(),
            GenreExternalId::interface_descriptor(),
            GenreAliasInput::interface_descriptor(),
            GenreResolveRequest::interface_descriptor(),
        ],
        &[],
    )
}
