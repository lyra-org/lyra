// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

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
use crate::plugins::catalog;
use crate::plugins::db::DbAsync;
#[cfg(feature = "docgen")]
use crate::services::catalog::tracks::TrackKey;
use crate::services::{
    self,
    catalog::{
        lookups,
        tracks::{
            TrackFilter,
            Tracks,
        },
    },
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
        .function(get_spec())
        .function(query_spec())
        .function(by_release_spec())
        .install(|_| Ok(ModuleExport::new(TracksModule)))
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

fn query_spec() -> FunctionSpec {
    FunctionSpec::async_fn("query")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("query")
        .args::<luau::Table>()
        .returns::<luau::Value>()
        .call_async(std::sync::Arc::new(query_callback))
}

fn get_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let db = frame.vm.data().get::<TracksModuleStore>()?.db()?;
    catalog::keyed_lookup(&mut frame, db, "ids", lookups::get::<Tracks>)
}

fn by_release_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let db = frame.vm.data().get::<TracksModuleStore>()?.db()?;
    catalog::keyed_lookup(&mut frame, db, "release_ids", lookups::tracks_by_release)
}

fn query_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let opts: luau::Table = frame.args.read_named("query")?;
    let filter = read_filter(frame.vm, &opts)?;
    let request = catalog::read_query::<Tracks>(frame.vm, &opts, filter)?;
    let principal = crate::plugins::auth::dispatch_principal(&frame.context)?;
    let store = frame.vm.data().get::<TracksModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let viewer = catalog::viewer(&*db, principal)?;
        let page =
            services::catalog::page(&db, &viewer, &request.query, request.offset, request.limit)
                .map_err(catalog::error)?;
        catalog::page_table(page)?.into_luau_return()
    }))
}

fn read_filter(vm: &luau::Vm, table: &luau::Table) -> luau::runtime::Result<TrackFilter> {
    Ok(TrackFilter {
        ids: catalog::optional_ids(vm, table, "ids")?,
        exclude_ids: args::optional_unique_ids(vm, table, "exclude_ids")?,
        library: args::optional_positive_id(vm, table, "library_id")?,
        releases: catalog::optional_ids(vm, table, "release_ids")?,
        artists: catalog::read_artist_credit(vm, table)?,
        genres: args::optional_unique_ids(vm, table, "genre_ids")?,
        years: catalog::read_years(vm, table)?,
        favorite: luau::table::optional_bool_field(vm, table, "favorite")?,
        listened: luau::table::optional_bool_field(vm, table, "listened")?,
        rating: Default::default(),
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
        catalog::credit_role_alias(),
    ]
    .into_iter()
    .chain(catalog::type_aliases::<TrackKey>(
        "TrackSortKey",
        "TrackPage",
        "Track",
    ))
    .collect()
}

#[cfg(feature = "docgen")]
fn track_query() -> InterfaceDescriptor {
    let mut descriptor = InterfaceDescriptor::new("TrackQuery", None);
    descriptor.fields.extend([
        field("ids", Option::<Vec<u64>>::luau_type()),
        field("exclude_ids", Option::<Vec<u64>>::luau_type()),
        field("library_id", Option::<u64>::luau_type()),
        field("release_ids", Option::<Vec<u64>>::luau_type()),
        field("genre_ids", Option::<Vec<u64>>::luau_type()),
        field("years", Option::<Vec<u32>>::luau_type()),
        field("favorite", Option::<bool>::luau_type()),
        field("listened", Option::<bool>::luau_type()),
    ]);
    descriptor.fields.extend(catalog::credit_fields());
    descriptor
        .fields
        .extend(catalog::query_fields("TrackSortKey"));
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
                path: vec!["get"],
                description: Some(
                    "The tracks with these ids that the caller can see, keyed by id.",
                ),
                params: vec![param("ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(u64::luau_type(), LuauType::named("Track"))],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["query"],
                description: Some(
                    "The tracks the caller can see that pass every filter, sorted and paged. Without `sort`, a search ranks by relevance, a query scoped to one release is in disc and track order, and anything else is by sort name.",
                ),
                params: vec![param("query", LuauType::named("TrackQuery"))],
                returns: vec![LuauType::named("TrackPage")],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["by_release"],
                description: Some(
                    "The tracks on each of these releases that the caller can see, keyed by release id.",
                ),
                params: vec![param("release_ids", Vec::<u64>::luau_type())],
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
        &[track_query()],
        &[],
    )
}
