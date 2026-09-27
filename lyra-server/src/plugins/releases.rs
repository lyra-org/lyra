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
use crate::plugins::catalog;
#[cfg(feature = "docgen")]
use crate::services::catalog::releases::ReleaseKey;
use crate::{
    plugins::db::{
        DbAsync,
        Release,
    },
    services::{
        self,
        catalog::{
            lookups,
            releases::{
                ReleaseFilter,
                Releases,
            },
        },
        releases as release_service,
    },
};

#[derive(Clone, Default)]
pub(crate) struct ReleasesModuleStore {
    db: Option<DbAsync>,
}

impl ReleasesModuleStore {
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
                "lyra/releases requires a database-backed plugin executor",
            )
        })
    }
}

struct ReleasesModule;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/releases")
        .capability("lyra.releases")
        .function(get_spec())
        .function(query_spec())
        .function(by_track_spec())
        .function(by_genre_spec())
        .function(similar_spec())
        .install(|_| Ok(ModuleExport::new(ReleasesModule)))
}

fn get_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("ids")
        .args::<Vec<u64>>()
        .returns::<luau::Table>()
        .call_async(std::sync::Arc::new(get_callback))
}

fn by_track_spec() -> FunctionSpec {
    FunctionSpec::async_fn("by_track")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("track_ids")
        .args::<Vec<u64>>()
        .returns::<luau::Table>()
        .call_async(std::sync::Arc::new(by_track_callback))
}

fn by_genre_spec() -> FunctionSpec {
    FunctionSpec::async_fn("by_genre")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("genre_ids")
        .args::<Vec<u64>>()
        .returns::<luau::Table>()
        .call_async(std::sync::Arc::new(by_genre_callback))
}

fn query_spec() -> FunctionSpec {
    FunctionSpec::async_fn("query")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("query")
        .args::<luau::Table>()
        .returns::<luau::Value>()
        .call_async(std::sync::Arc::new(query_callback))
}

fn similar_spec() -> FunctionSpec {
    FunctionSpec::async_fn("similar")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("release_db_id")
        .args::<i64>()
        .arg_name("opts")
        .args::<Option<luau::Table>>()
        .returns::<Option<Vec<Release>>>()
        .call_async(std::sync::Arc::new(similar_callback))
}

fn get_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let db = frame.vm.data().get::<ReleasesModuleStore>()?.db()?;
    catalog::keyed_lookup(&mut frame, db, "ids", lookups::get::<Releases>)
}

fn by_track_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let db = frame.vm.data().get::<ReleasesModuleStore>()?.db()?;
    catalog::keyed_lookup(&mut frame, db, "track_ids", lookups::releases_by_track)
}

fn by_genre_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let db = frame.vm.data().get::<ReleasesModuleStore>()?.db()?;
    catalog::keyed_lookup(&mut frame, db, "genre_ids", lookups::releases_by_genre)
}

fn query_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let opts: luau::Table = frame.args.read_named("query")?;
    let filter = read_filter(frame.vm, &opts)?;
    let request = catalog::read_query::<Releases>(frame.vm, &opts, filter)?;
    let principal = crate::plugins::auth::dispatch_principal(&frame.context)?;
    let db = frame.vm.data().get::<ReleasesModuleStore>()?.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let viewer = catalog::viewer(&*db, principal)?;
        let page =
            services::catalog::page(&db, &viewer, &request.query, request.offset, request.limit)
                .map_err(catalog::error)?;
        catalog::page_table(page)?.into_luau_return()
    }))
}

fn similar_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    if frame
        .context
        .caller
        .get::<crate::plugins::executor::MetadataDispatchContext>()
        .is_ok()
    {
        return Err(crate::plugins::runtime_error(
            "releases.similar cannot be called from a metadata provider callback",
        ));
    }
    let release_db_id = frame.args.read_named::<i64>("release_db_id")?;
    if release_db_id <= 0 {
        return Err(crate::plugins::runtime_error(
            "releases.similar release_db_id must be a positive integer",
        ));
    }
    let request = parse_similar_options(frame.vm, frame.args.read_optional_named("opts")?)?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;
    let provider_vm = frame.vm.clone();

    Ok(luau::ScheduledFuture::new(async move {
        let options =
            release_service::SimilarReleaseOptions::for_principal(&principal, request.limit);
        let releases = release_service::similar_in_vm(DbId(release_db_id), &options, provider_vm)
            .await
            .map_err(crate::plugins::runtime_error)?;
        match releases {
            Some(releases) => harmony_luau::serializable_to_luau_owned(releases),
            None => Ok(luau::Value::Nil),
        }
    }))
}

struct SimilarReleaseRequest {
    limit: usize,
}

fn parse_similar_options(
    vm: &luau::Vm,
    opts: Option<luau::Table>,
) -> luau::runtime::Result<SimilarReleaseRequest> {
    let Some(opts) = opts else {
        return Ok(SimilarReleaseRequest {
            limit: release_service::DEFAULT_SIMILAR_RELEASE_LIMIT,
        });
    };
    let limit = parse_optional_similar_positive_integer(vm, &opts, "limit")?
        .map(|value| {
            usize::try_from(value).map_err(|_| {
                crate::plugins::runtime_error("releases.similar opts.limit is too large")
            })
        })
        .transpose()?
        .unwrap_or(release_service::DEFAULT_SIMILAR_RELEASE_LIMIT);
    if limit > release_service::MAX_SIMILAR_RELEASE_LIMIT {
        return Err(crate::plugins::runtime_error(format!(
            "releases.similar opts.limit must be <= {}, got {limit}",
            release_service::MAX_SIMILAR_RELEASE_LIMIT
        )));
    }
    Ok(SimilarReleaseRequest { limit })
}

fn parse_optional_similar_positive_integer(
    vm: &luau::Vm,
    opts: &luau::Table,
    field: &str,
) -> luau::runtime::Result<Option<i64>> {
    let value = luau::table::optional_i64_field(vm, opts, field).map_err(|_| {
        crate::plugins::runtime_error(format!(
            "releases.similar opts.{field} must be a positive integer"
        ))
    })?;
    let Some(value) = value else {
        return Ok(None);
    };
    if value <= 0 {
        return Err(crate::plugins::runtime_error(format!(
            "releases.similar opts.{field} must be a positive integer"
        )));
    }
    Ok(Some(value))
}

fn read_filter(vm: &luau::Vm, table: &luau::Table) -> luau::runtime::Result<ReleaseFilter> {
    Ok(ReleaseFilter {
        ids: catalog::optional_ids(vm, table, "ids")?,
        exclude_ids: args::optional_unique_ids(vm, table, "exclude_ids")?,
        exclude_artists: args::optional_unique_ids(vm, table, "exclude_artist_ids")?,
        library: args::optional_positive_id(vm, table, "library_id")?,
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
fn described_field(name: &'static str, ty: LuauType, description: &'static str) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description: Some(description),
    }
}

#[cfg(feature = "docgen")]
fn string_enum(values: impl IntoIterator<Item = &'static str>) -> LuauType {
    LuauType::union(values.into_iter().map(LuauType::string_literal).collect())
}

#[cfg(feature = "docgen")]
fn release_type() -> LuauType {
    LuauType::object(vec![
        field("db_id", Option::<i64>::luau_type()),
        field("id", String::luau_type()),
        field("release_title", String::luau_type()),
        field("sort_title", Option::<String>::luau_type()),
        field(
            "release_type",
            LuauType::optional(LuauType::named("ReleaseType")),
        ),
        field("release_date", Option::<String>::luau_type()),
        field("locked", Option::<bool>::luau_type()),
        field("created_at", Option::<u64>::luau_type()),
        field("ctime", Option::<u64>::luau_type()),
    ])
}

#[cfg(feature = "docgen")]
fn release_type_aliases() -> Vec<TypeAliasDescriptor> {
    vec![
        TypeAliasDescriptor::new(
            "ReleaseType",
            string_enum([
                "album",
                "single",
                "ep",
                "compilation",
                "soundtrack",
                "live",
                "remix",
                "broadcast",
                "other",
                "unknown",
            ]),
            None,
        ),
        TypeAliasDescriptor::new("Release", release_type(), None),
        catalog::credit_role_alias(),
    ]
    .into_iter()
    .chain(catalog::type_aliases::<ReleaseKey>(
        "ReleaseSortKey",
        "ReleasePage",
        "Release",
    ))
    .collect()
}

#[cfg(feature = "docgen")]
fn release_query() -> InterfaceDescriptor {
    let mut descriptor = InterfaceDescriptor::new("ReleaseQuery", None);
    descriptor.fields.extend([
        field("ids", Option::<Vec<u64>>::luau_type()),
        field("exclude_ids", Option::<Vec<u64>>::luau_type()),
        field("library_id", Option::<u64>::luau_type()),
        field("genre_ids", Option::<Vec<u64>>::luau_type()),
        field("years", Option::<Vec<u32>>::luau_type()),
        field("favorite", Option::<bool>::luau_type()),
        described_field(
            "listened",
            Option::<bool>::luau_type(),
            "Whether the caller has listened to every one of the release's tracks.",
        ),
    ]);
    descriptor.fields.extend(catalog::credit_fields());
    descriptor
        .fields
        .extend(catalog::query_fields("ReleaseSortKey"));
    descriptor
}

#[cfg(feature = "docgen")]
fn similar_release_options() -> InterfaceDescriptor {
    let mut descriptor = InterfaceDescriptor::new("SimilarReleaseOptions", None);
    descriptor.fields.extend([described_field(
        "limit",
        Option::<i64>::luau_type(),
        "Maximum number of releases to return. Defaults to 20 and cannot exceed 100.",
    )]);
    descriptor
}

#[cfg(feature = "docgen")]
fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "Releases",
        local_name: "releases",
        description: None,
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["get"],
                description: Some(
                    "The releases with these ids that the caller can see, keyed by id.",
                ),
                params: vec![param("ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(u64::luau_type(), LuauType::named("Release"))],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["query"],
                description: Some(
                    "The releases the caller can see that pass every filter, sorted and paged. Without `sort`, a search ranks by relevance and anything else is by sort name.",
                ),
                params: vec![param("query", LuauType::named("ReleaseQuery"))],
                returns: vec![LuauType::named("ReleasePage")],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["by_track"],
                description: Some(
                    "The releases the caller can see that hold each of these tracks, keyed by track id.",
                ),
                params: vec![param("track_ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(
                    u64::luau_type(),
                    LuauType::array(LuauType::named("Release")),
                )],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["by_genre"],
                description: Some(
                    "The releases the caller can see in each of these genres, keyed by genre id.",
                ),
                params: vec![param("genre_ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(
                    u64::luau_type(),
                    LuauType::array(LuauType::named("Release")),
                )],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["similar"],
                description: Some(
                    "Returns provider-ranked releases related to a local release. Returns nil when the seed release does not exist or is inaccessible to the supplied user.",
                ),
                params: vec![
                    param("release_db_id", i64::luau_type()),
                    param(
                        "opts",
                        LuauType::optional(LuauType::named("SimilarReleaseOptions")),
                    ),
                ],
                returns: vec![LuauType::optional(LuauType::array(LuauType::named(
                    "Release",
                )))],
                yields: true,
            },
        ],
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &module_descriptor(),
        &release_type_aliases(),
        &[release_query(), similar_release_options()],
        &[],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn similar_options_read_fields_directly_from_table() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        let empty = vm.create_table()?;
        let defaults = parse_similar_options(&vm, Some(empty))?;
        assert_eq!(
            defaults.limit,
            release_service::DEFAULT_SIMILAR_RELEASE_LIMIT
        );

        let opts = vm.create_table()?;
        opts.set_raw(&vm, "limit", luau::Value::from(7_i64))?;
        let parsed = parse_similar_options(&vm, Some(opts))?;
        assert_eq!(parsed.limit, 7);
        Ok(())
    }

    #[test]
    fn similar_options_reject_invalid_integer_fields() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        let opts = vm.create_table()?;
        opts.set_raw(&vm, "limit", luau::Value::from(0_i64))?;
        assert!(parse_similar_options(&vm, Some(opts)).is_err());

        let opts = vm.create_table()?;
        opts.set_raw(&vm, "limit", luau::Value::Number(4.5))?;
        assert!(parse_similar_options(&vm, Some(opts)).is_err());
        Ok(())
    }
}
