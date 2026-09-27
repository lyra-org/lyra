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
#[cfg(feature = "docgen")]
use crate::services::catalog::genres::GenreKey;
use crate::services::{
    self,
    catalog::{
        Page,
        genres::{
            GenreFilter,
            Genres,
        },
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
        .function(get_by_id_spec())
        .function(find_by_name_spec())
        .function(get_parents_spec())
        .function(get_children_spec())
        .function(get_releases_spec())
        .function(get_releases_many_spec())
        .function(get_for_release_spec())
        .function(get_for_releases_many_spec())
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
        catalog::page_table(Page {
            items: page.items.into_iter().map(GenreRecord::from).collect(),
            total: page.total,
            offset: page.offset,
        })
        .map(luau::Value::TableData)
    }))
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

fn get_by_id_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_by_id")
        .arg_name("genre_id")
        .args::<i64>()
        .returns::<Option<GenreRecord>>()
        .call_async(std::sync::Arc::new(get_by_id_callback))
}

fn find_by_name_spec() -> FunctionSpec {
    FunctionSpec::async_fn("find_by_name")
        .arg_name("name")
        .args::<String>()
        .returns::<Option<GenreRecord>>()
        .call_async(std::sync::Arc::new(find_by_name_callback))
}

fn get_parents_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_parents")
        .arg_name("genre_id")
        .args::<i64>()
        .returns::<Vec<GenreRecord>>()
        .call_async(std::sync::Arc::new(get_parents_callback))
}

fn get_children_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_children")
        .arg_name("genre_id")
        .args::<i64>()
        .returns::<Vec<GenreRecord>>()
        .call_async(std::sync::Arc::new(get_children_callback))
}

fn get_releases_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_releases")
        .arg_name("genre_id")
        .args::<i64>()
        .returns::<Vec<i64>>()
        .call_async(std::sync::Arc::new(get_releases_callback))
}

fn get_releases_many_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_releases_many")
        .arg_name("genre_ids")
        .args::<Vec<u64>>()
        .returns::<luau::Table>()
        .call_async(std::sync::Arc::new(get_releases_many_callback))
}

fn get_for_release_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_for_release")
        .arg_name("release_id")
        .args::<i64>()
        .returns::<Vec<GenreRecord>>()
        .call_async(std::sync::Arc::new(get_for_release_callback))
}

fn get_for_releases_many_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_for_releases_many")
        .arg_name("release_ids")
        .args::<Vec<u64>>()
        .returns::<luau::Table>()
        .call_async(std::sync::Arc::new(get_for_releases_many_callback))
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

fn get_by_id_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let genre_id: i64 = frame.args.read_named("genre_id")?;
    let store = frame.vm.data().get::<GenresModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let genre = {
            let db = db.read().await;
            db::genres::get_by_id(&db, DbId(genre_id)).map_err(crate::plugins::runtime_error)
        }?;

        optional_genre_value(genre)
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

fn get_parents_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let genre_id: i64 = frame.args.read_named("genre_id")?;
    let store = frame.vm.data().get::<GenresModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let genres = {
            let db = db.read().await;
            db::genres::get_parents(&db, DbId(genre_id)).map_err(crate::plugins::runtime_error)
        }?;

        harmony_luau::serializable_to_luau_owned(
            genres
                .into_iter()
                .map(GenreRecord::from)
                .collect::<Vec<_>>(),
        )
    }))
}

fn get_children_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let genre_id: i64 = frame.args.read_named("genre_id")?;
    let store = frame.vm.data().get::<GenresModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let genres = {
            let db = db.read().await;
            db::genres::get_children(&db, DbId(genre_id)).map_err(crate::plugins::runtime_error)
        }?;

        harmony_luau::serializable_to_luau_owned(
            genres
                .into_iter()
                .map(GenreRecord::from)
                .collect::<Vec<_>>(),
        )
    }))
}

fn get_releases_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let genre_id: i64 = frame.args.read_named("genre_id")?;
    let store = frame.vm.data().get::<GenresModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let release_ids = {
            let db = db.read().await;
            db::genres::get_releases(&db, DbId(genre_id)).map_err(crate::plugins::runtime_error)
        }?;

        harmony_luau::serializable_to_luau_owned(
            release_ids.into_iter().map(|id| id.0).collect::<Vec<_>>(),
        )
    }))
}

fn get_releases_many_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let ids_table: luau::Table = frame.args.read_named("genre_ids")?;
    let ids = args::unique_ids(frame.vm, &ids_table)?;
    let store = frame.vm.data().get::<GenresModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let releases = {
            let db = db.read().await;
            db::genres::get_releases_many(&db, &ids).map_err(crate::plugins::runtime_error)
        }?;

        let mut table = luau::OwnedTable::with_capacity(0, ids.len());
        for id in ids {
            let release_ids = releases
                .get(&id)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(|release_id| release_id.0)
                .collect::<Vec<_>>();
            table.set_key(
                luau::Value::from(id.0),
                harmony_luau::serializable_to_luau_owned(release_ids)?,
            );
        }
        Ok(luau::Value::TableData(table))
    }))
}

fn get_for_release_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let release_id: i64 = frame.args.read_named("release_id")?;
    let store = frame.vm.data().get::<GenresModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let genres = {
            let db = db.read().await;
            db::genres::get_for_release(&db, DbId(release_id))
                .map_err(crate::plugins::runtime_error)
        }?;

        harmony_luau::serializable_to_luau_owned(
            genres
                .into_iter()
                .map(GenreRecord::from)
                .collect::<Vec<_>>(),
        )
    }))
}

fn get_for_releases_many_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let ids_table: luau::Table = frame.args.read_named("release_ids")?;
    let ids = args::unique_ids(frame.vm, &ids_table)?;
    let store = frame.vm.data().get::<GenresModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let genres = {
            let db = db.read().await;
            db::genres::get_for_releases_many(&db, &ids).map_err(crate::plugins::runtime_error)
        }?;

        let mut table = luau::OwnedTable::with_capacity(0, ids.len());
        for id in ids {
            let value = genres
                .get(&id)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(GenreRecord::from)
                .collect::<Vec<_>>();
            table.set_key(
                luau::Value::from(id.0),
                harmony_luau::serializable_to_luau_owned(value)?,
            );
        }
        Ok(luau::Value::TableData(table))
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
                path: vec!["get_by_id"],
                description: None,
                params: vec![param("genre_id", i64::luau_type())],
                returns: vec![Option::<GenreRecord>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["find_by_name"],
                description: None,
                params: vec![param("name", String::luau_type())],
                returns: vec![Option::<GenreRecord>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_parents"],
                description: None,
                params: vec![param("genre_id", i64::luau_type())],
                returns: vec![Vec::<GenreRecord>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_children"],
                description: None,
                params: vec![param("genre_id", i64::luau_type())],
                returns: vec![Vec::<GenreRecord>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_releases"],
                description: None,
                params: vec![param("genre_id", i64::luau_type())],
                returns: vec![Vec::<i64>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_releases_many"],
                description: None,
                params: vec![param("genre_ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(u64::luau_type(), Vec::<i64>::luau_type())],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_for_release"],
                description: None,
                params: vec![param("release_id", i64::luau_type())],
                returns: vec![Vec::<GenreRecord>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_for_releases_many"],
                description: None,
                params: vec![param("release_ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(
                    u64::luau_type(),
                    Vec::<GenreRecord>::luau_type(),
                )],
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
        &catalog::type_aliases::<GenreKey>("GenreSortKey", "GenrePage", "GenreInfo"),
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
