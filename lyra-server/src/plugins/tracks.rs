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
use harmony_luau::IntoLuauReturn;
#[cfg(feature = "docgen")]
use harmony_luau::{
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    TypeAliasDescriptor,
    render_definition_file_with_support,
};

use crate::plugins::args;
use crate::plugins::db::{
    self,
    DbAsync,
    ListOptions,
    ResolveId,
    Track,
};

#[derive(Clone, Default)]
pub(crate) struct TracksModuleStore {
    db: Option<DbAsync>,
}

impl TracksModuleStore {
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: DbAsync) -> Self {
        Self { db: Some(db) }
    }

    fn db(&self) -> luau::runtime::Result<DbAsync> {
        self.db.clone().ok_or_else(|| {
            crate::plugins::runtime_error("lyra/tracks requires a database-backed plugin executor")
        })
    }
}

struct TracksModule;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/tracks")
        .capability("lyra.tracks")
        .function(list_spec())
        .function(query_spec())
        .function(get_by_ids_spec())
        .function(list_by_library_spec())
        .function(list_many_spec())
        .install(|_| Ok(ModuleExport::new(TracksModule)))
}

fn list_spec() -> FunctionSpec {
    FunctionSpec::async_fn("list")
        .arg_name("scope")
        .args::<Option<ResolveId>>()
        .returns::<Vec<Track>>()
        .call_async(std::sync::Arc::new(list_callback))
}

fn query_spec() -> FunctionSpec {
    FunctionSpec::async_fn("query")
        .arg_name("opts")
        .args::<luau::Table>()
        .returns::<luau::Value>()
        .call_async(std::sync::Arc::new(query_callback))
}

fn get_by_ids_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_by_ids")
        .arg_name("ids")
        .args::<Vec<u64>>()
        .returns::<luau::Table>()
        .call_async(std::sync::Arc::new(get_by_ids_callback))
}

fn list_by_library_spec() -> FunctionSpec {
    FunctionSpec::async_fn("list_by_library")
        .arg_name("library_id")
        .args::<i64>()
        .returns::<Vec<Track>>()
        .call_async(std::sync::Arc::new(list_by_library_callback))
}

fn list_many_spec() -> FunctionSpec {
    FunctionSpec::async_fn("list_many")
        .arg_name("ids")
        .args::<Vec<u64>>()
        .returns::<luau::Table>()
        .call_async(std::sync::Arc::new(list_many_callback))
}

fn list_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let scope = frame
        .args
        .read_optional_named::<luau::Value>("scope")?
        .map(args::resolve_id)
        .transpose()?
        .unwrap_or_else(|| ResolveId::alias("tracks"));
    let store = frame.vm.data().get::<TracksModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let query_id = scope
            .to_query_id(&db)
            .map_err(crate::plugins::runtime_error)?
            .ok_or_else(|| crate::plugins::runtime_error("could not resolve scope"))?;
        let tracks = db::tracks::get(&db, query_id).map_err(crate::plugins::runtime_error)?;
        harmony_luau::serializable_to_luau_owned(tracks)
    }))
}

fn query_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let opts: luau::Table = frame.args.read_named("opts")?;
    let request = parse_query_options(frame.vm, &opts)?;
    let store = frame.vm.data().get::<TracksModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let scope = request
            .scope
            .map(|id| id.to_query_id(&db).map_err(crate::plugins::runtime_error))
            .transpose()?
            .flatten();
        let result = if !request.artist_ids.is_empty() && !request.release_artist_ids.is_empty() {
            db::tracks::query_by_artist_filters(
                &db,
                &request.artist_ids,
                &request.release_artist_ids,
                scope,
                &request.list_options,
            )
            .map_err(crate::plugins::runtime_error)
        } else if !request.release_artist_ids.is_empty() {
            db::tracks::query_by_release_artists(
                &db,
                &request.release_artist_ids,
                scope,
                &request.list_options,
            )
            .map_err(crate::plugins::runtime_error)
        } else if !request.artist_ids.is_empty() {
            db::tracks::query_by_artists(&db, &request.artist_ids, scope, &request.list_options)
                .map_err(crate::plugins::runtime_error)
        } else {
            let query_id = match scope {
                Some(query_id) => query_id,
                None => ResolveId::alias("tracks")
                    .to_query_id(&db)
                    .map_err(crate::plugins::runtime_error)?
                    .ok_or_else(|| crate::plugins::runtime_error("could not resolve scope"))?,
            };
            db::tracks::query(&db, query_id, &request.list_options)
                .map_err(crate::plugins::runtime_error)
        }?;
        args::page_table(result.entries, result.total_count, result.offset)?.into_luau_return()
    }))
}

fn get_by_ids_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let ids_table: luau::Table = frame.args.read_named("ids")?;
    let ids = args::unique_ids(frame.vm, &ids_table)?;
    let store = frame.vm.data().get::<TracksModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let tracks = db::tracks::get_by_ids(&db, &ids).map_err(crate::plugins::runtime_error)?;

        let mut table = luau::OwnedTable::with_entry_capacity(0, 0, ids.len());
        for id in ids {
            let value = tracks
                .get(&id)
                .map(harmony_luau::serializable_to_luau_owned)
                .transpose()?
                .unwrap_or(luau::Value::Nil);
            table.set_key(luau::Value::from(id.0), value);
        }
        table.into_luau_return()
    }))
}

fn list_by_library_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let library_id: i64 = frame.args.read_named("library_id")?;
    let store = frame.vm.data().get::<TracksModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let tracks = db::tracks::get_by_library(&db, DbId(library_id))
            .map_err(crate::plugins::runtime_error)?;
        harmony_luau::serializable_to_luau_owned(tracks)
    }))
}

fn list_many_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let ids_table: luau::Table = frame.args.read_named("ids")?;
    let ids = args::unique_ids(frame.vm, &ids_table)?;
    let store = frame.vm.data().get::<TracksModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let related =
            db::tracks::get_direct_many(&db, &ids).map_err(crate::plugins::runtime_error)?;

        let mut table = luau::OwnedTable::with_entry_capacity(0, 0, ids.len());
        for id in ids {
            let tracks = related.get(&id).cloned().unwrap_or_default();
            table.set_key(
                luau::Value::from(id.0),
                harmony_luau::serializable_to_luau_owned(tracks)?,
            );
        }
        table.into_luau_return()
    }))
}

struct TrackQueryRequest {
    scope: Option<ResolveId>,
    artist_ids: Vec<DbId>,
    release_artist_ids: Vec<DbId>,
    list_options: ListOptions,
}

fn parse_query_options(
    vm: &luau::Vm,
    opts: &luau::Table,
) -> luau::runtime::Result<TrackQueryRequest> {
    let scope = match opts.get_raw(vm, "scope")? {
        luau::Value::Nil => None,
        value => Some(args::resolve_id(value)?),
    };
    let artist_ids = args::optional_unique_ids(vm, opts, "artist_ids")?;
    let release_artist_ids = args::optional_unique_ids(vm, opts, "release_artist_ids")?;
    let list_options = args::list_options(vm, opts)?;

    Ok(TrackQueryRequest {
        scope,
        artist_ids,
        release_artist_ids,
        list_options,
    })
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
fn field(name: &'static str, ty: LuauType) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description: None,
    }
}

#[cfg(feature = "docgen")]
fn track_type() -> LuauType {
    LuauType::object(vec![
        field("db_id", Option::<i64>::luau_type()),
        field("id", String::luau_type()),
        field("track_title", String::luau_type()),
        field("sort_title", Option::<String>::luau_type()),
        field("year", Option::<u32>::luau_type()),
        field("disc", Option::<u32>::luau_type()),
        field("disc_total", Option::<u32>::luau_type()),
        field("track", Option::<u32>::luau_type()),
        field("track_total", Option::<u32>::luau_type()),
        field("duration_ms", Option::<u64>::luau_type()),
        field("sample_rate_hz", Option::<u32>::luau_type()),
        field("channel_count", Option::<u32>::luau_type()),
        field("bit_depth", Option::<u32>::luau_type()),
        field("bitrate_bps", Option::<u32>::luau_type()),
        field("track_gain_db", Option::<f64>::luau_type()),
        field("album_gain_db", Option::<f64>::luau_type()),
        field("locked", Option::<bool>::luau_type()),
        field("created_at", Option::<u64>::luau_type()),
        field("ctime", Option::<u64>::luau_type()),
    ])
}

#[cfg(feature = "docgen")]
fn track_type_aliases() -> Vec<TypeAliasDescriptor> {
    vec![
        TypeAliasDescriptor::new("Track", track_type(), None),
        TypeAliasDescriptor::new(
            "TrackQueryResult",
            LuauType::object(vec![
                field("entities", LuauType::array(LuauType::named("Track"))),
                field("total_count", i64::luau_type()),
                field("offset", i64::luau_type()),
            ]),
            None,
        ),
    ]
}

#[cfg(feature = "docgen")]
fn track_query_options() -> InterfaceDescriptor {
    let mut descriptor = InterfaceDescriptor::new("TrackQueryOptions", None);
    descriptor.fields.extend([
        field("scope", LuauType::optional(args::resolve_id_type())),
        field("artist_ids", Option::<Vec<u64>>::luau_type()),
        field("release_artist_ids", Option::<Vec<u64>>::luau_type()),
        field("sort_by", Option::<Vec<String>>::luau_type()),
        field("sort_order", LuauType::optional(args::sort_order_type())),
        field("offset", Option::<i64>::luau_type()),
        field("limit", Option::<i64>::luau_type()),
        field("search_term", Option::<String>::luau_type()),
    ]);
    descriptor
}

#[cfg(feature = "docgen")]
fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "Tracks",
        local_name: "tracks",
        description: None,
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["list"],
                description: None,
                params: vec![param("scope", LuauType::optional(args::resolve_id_type()))],
                returns: vec![LuauType::array(LuauType::named("Track"))],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["query"],
                description: None,
                params: vec![param("opts", LuauType::named("TrackQueryOptions"))],
                returns: vec![LuauType::named("TrackQueryResult")],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_by_ids"],
                description: None,
                params: vec![param("ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(
                    u64::luau_type(),
                    LuauType::optional(LuauType::named("Track")),
                )],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["list_by_library"],
                description: None,
                params: vec![param("library_id", i64::luau_type())],
                returns: vec![LuauType::array(LuauType::named("Track"))],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["list_many"],
                description: None,
                params: vec![param("ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(
                    u64::luau_type(),
                    LuauType::array(LuauType::named("Track")),
                )],
                yields: true,
            },
        ],
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &module_descriptor(),
        &track_type_aliases(),
        &[track_query_options()],
        &[],
    )
}
