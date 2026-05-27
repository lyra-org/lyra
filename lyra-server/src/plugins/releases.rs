// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashSet;

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

use crate::{
    plugins::db::{
        self,
        DbAsync,
        ListOptions,
        Release,
        ResolveId,
        parse_sort_direction,
        parse_sort_specs_tokens,
    },
    services::releases as release_service,
};

#[derive(Clone, Default)]
pub(crate) struct ReleasesModuleStore {
    db: Option<DbAsync>,
}

impl ReleasesModuleStore {
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
        .function(list_spec())
        .function(query_spec())
        .function(get_by_artist_spec())
        .function(get_appearances_spec())
        .function(list_many_spec())
        .install(|_| Ok(ModuleExport::new(ReleasesModule)))
}

fn list_spec() -> FunctionSpec {
    FunctionSpec::async_fn("list")
        .arg_name("scope")
        .args::<Option<ResolveId>>()
        .returns::<Vec<Release>>()
        .call_async(std::sync::Arc::new(list_callback))
}

fn query_spec() -> FunctionSpec {
    FunctionSpec::async_fn("query")
        .arg_name("opts")
        .args::<luau::Table>()
        .returns::<luau::Value>()
        .call_async(std::sync::Arc::new(query_callback))
}

fn get_by_artist_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_by_artist")
        .arg_name("artist_id")
        .args::<i64>()
        .returns::<Vec<Release>>()
        .call_async(std::sync::Arc::new(get_by_artist_callback))
}

fn get_appearances_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_appearances")
        .arg_name("artist_id")
        .args::<i64>()
        .returns::<Vec<Release>>()
        .call_async(std::sync::Arc::new(get_appearances_callback))
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
        .map(parse_resolve_id)
        .transpose()?;
    let store = frame
        .vm
        .data()
        .get::<ReleasesModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let query_id = scope
            .map(|id| id.to_query_id(&db).map_err(crate::plugins::runtime_error))
            .transpose()?
            .flatten();
        let releases =
            release_service::get(&db, query_id).map_err(crate::plugins::runtime_error)?;
        Ok(harmony_luau::serializable_to_luau_owned(releases)?)
    }))
}

fn query_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let opts: luau::Table = frame.args.read_named("opts")?;
    let request = parse_query_options(frame.vm, &opts)?;
    let store = frame
        .vm
        .data()
        .get::<ReleasesModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let scope = request
            .scope
            .map(|id| id.to_query_id(&db).map_err(crate::plugins::runtime_error))
            .transpose()?
            .flatten();
        let result = if request.artist_ids.is_empty() {
            release_service::query(&db, scope, &request.list_options)
                .map_err(crate::plugins::runtime_error)
        } else {
            release_service::query_by_artists(
                &db,
                &request.artist_ids,
                scope,
                &request.list_options,
            )
            .map_err(crate::plugins::runtime_error)
        }?;
        query_result_table(result.entries, result.total_count, result.offset)?.into_luau_return()
    }))
}

fn get_by_artist_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let artist_id: i64 = frame.args.read_named("artist_id")?;
    let store = frame
        .vm
        .data()
        .get::<ReleasesModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let releases = db::releases::get_by_artist(&db, DbId(artist_id))
            .map_err(crate::plugins::runtime_error)?;
        Ok(harmony_luau::serializable_to_luau_owned(releases)?)
    }))
}

fn get_appearances_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let artist_id: i64 = frame.args.read_named("artist_id")?;
    let store = frame
        .vm
        .data()
        .get::<ReleasesModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let releases = release_service::get_appearances(&db, DbId(artist_id))
            .map_err(crate::plugins::runtime_error)?;
        Ok(harmony_luau::serializable_to_luau_owned(releases)?)
    }))
}

fn list_many_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let ids_table: luau::Table = frame.args.read_named("ids")?;
    let ids = parse_db_ids(frame.vm, &ids_table)?;
    let store = frame
        .vm
        .data()
        .get::<ReleasesModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let related =
            release_service::get_many_by_track(&db, &ids).map_err(crate::plugins::runtime_error)?;

        let mut table = luau::OwnedTable::with_entry_capacity(0, 0, ids.len());
        for id in ids {
            let releases = related.get(&id).cloned().unwrap_or_default();
            crate::plugins::set_owned_db_id_key(
                &mut table,
                id,
                harmony_luau::serializable_to_luau_owned(releases)?,
            );
        }
        table.into_luau_return()
    }))
}

struct ReleaseQueryRequest {
    scope: Option<ResolveId>,
    artist_ids: Vec<DbId>,
    list_options: ListOptions,
}

fn parse_query_options(
    vm: &luau::Vm,
    opts: &luau::Table,
) -> luau::runtime::Result<ReleaseQueryRequest> {
    let scope = match opts.get_raw(vm, "scope")? {
        luau::Value::Nil => None,
        value => Some(parse_resolve_id(value)?),
    };
    let artist_ids = parse_optional_db_ids(vm, opts, "artist_ids")?;
    let list_options = parse_list_options(vm, opts)?;

    Ok(ReleaseQueryRequest {
        scope,
        artist_ids,
        list_options,
    })
}

fn parse_list_options(vm: &luau::Vm, table: &luau::Table) -> luau::runtime::Result<ListOptions> {
    let sort_by = match table.get_raw(vm, "sort_by")? {
        luau::Value::Nil => None,
        luau::Value::Table(table) => {
            let mut values = Vec::new();
            for (_, value) in table.pairs_raw(vm)? {
                let luau::Value::String(bytes) = value else {
                    return Err(crate::plugins::runtime_error(
                        "sort_by entries must be strings",
                    ));
                };
                values.push(String::from_utf8(bytes).map_err(crate::plugins::runtime_error)?);
            }
            Some(values)
        }
        other => {
            return Err(crate::plugins::runtime_error(format!(
                "sort_by must be an array of strings, got {}",
                other.type_name()
            )));
        }
    };
    let sort_order = match table.get_raw(vm, "sort_order")? {
        luau::Value::Nil => None,
        luau::Value::String(bytes) => {
            Some(String::from_utf8(bytes).map_err(crate::plugins::runtime_error)?)
        }
        other => {
            return Err(crate::plugins::runtime_error(format!(
                "sort_order must be a string, got {}",
                other.type_name()
            )));
        }
    };
    let direction =
        parse_sort_direction(sort_order, true).map_err(crate::plugins::runtime_error)?;
    let sort = parse_sort_specs_tokens(sort_by, direction, |_| true, false)
        .map_err(crate::plugins::runtime_error)?;
    let offset = parse_optional_u64(vm, table, "offset")?;
    let limit = parse_optional_u64(vm, table, "limit")?;
    let search_term = match table.get_raw(vm, "search_term")? {
        luau::Value::Nil => None,
        luau::Value::String(bytes) => {
            Some(String::from_utf8(bytes).map_err(crate::plugins::runtime_error)?)
        }
        other => {
            return Err(crate::plugins::runtime_error(format!(
                "search_term must be a string, got {}",
                other.type_name()
            )));
        }
    };

    Ok(ListOptions {
        sort,
        offset,
        limit,
        search_term,
    })
}

fn parse_optional_db_ids(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Vec<DbId>> {
    match table.get_raw(vm, key)? {
        luau::Value::Nil => Ok(Vec::new()),
        luau::Value::Table(table) => parse_db_ids(vm, &table),
        other => Err(crate::plugins::runtime_error(format!(
            "{key} must be an array of positive integer ids, got {}",
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

fn parse_optional_u64(
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
        _ => Err(crate::plugins::runtime_error(format!(
            "{key} must be a non-negative integer when provided"
        ))),
    }
}

fn query_result_table(
    entries: Vec<Release>,
    total_count: u64,
    offset: u64,
) -> luau::runtime::Result<luau::OwnedTable> {
    let mut table = luau::OwnedTable::with_capacity(0, 3);
    table.set_field(
        "entities",
        harmony_luau::serializable_to_luau_owned(entries)?,
    );
    table.set_field("total_count", luau::Value::Integer(total_count as i64));
    table.set_field("offset", luau::Value::Integer(offset as i64));
    Ok(table)
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
fn resolve_id_type() -> LuauType {
    LuauType::union(vec![i64::luau_type(), String::luau_type()])
}

#[cfg(feature = "docgen")]
fn sort_order_type() -> LuauType {
    LuauType::union(vec![
        LuauType::string_literal("ascending"),
        LuauType::string_literal("descending"),
    ])
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
        TypeAliasDescriptor::new(
            "ReleaseQueryResult",
            LuauType::object(vec![
                field("entities", LuauType::array(LuauType::named("Release"))),
                field("total_count", i64::luau_type()),
                field("offset", i64::luau_type()),
            ]),
            None,
        ),
    ]
}

#[cfg(feature = "docgen")]
fn release_query_options() -> InterfaceDescriptor {
    let mut descriptor = InterfaceDescriptor::new("ReleaseQueryOptions", None);
    descriptor.fields.extend([
        field("scope", LuauType::optional(resolve_id_type())),
        field("artist_ids", Option::<Vec<u64>>::luau_type()),
        field("sort_by", Option::<Vec<String>>::luau_type()),
        field("sort_order", LuauType::optional(sort_order_type())),
        field("offset", Option::<i64>::luau_type()),
        field("limit", Option::<i64>::luau_type()),
        field("search_term", Option::<String>::luau_type()),
    ]);
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
                path: vec!["list"],
                description: None,
                params: vec![param("scope", LuauType::optional(resolve_id_type()))],
                returns: vec![LuauType::array(LuauType::named("Release"))],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["query"],
                description: None,
                params: vec![param("opts", LuauType::named("ReleaseQueryOptions"))],
                returns: vec![LuauType::named("ReleaseQueryResult")],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_by_artist"],
                description: None,
                params: vec![param("artist_id", i64::luau_type())],
                returns: vec![LuauType::array(LuauType::named("Release"))],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_appearances"],
                description: None,
                params: vec![param("artist_id", i64::luau_type())],
                returns: vec![LuauType::array(LuauType::named("Release"))],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["list_many"],
                description: None,
                params: vec![param("ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(
                    u64::luau_type(),
                    LuauType::array(LuauType::named("Release")),
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
        &release_type_aliases(),
        &[release_query_options()],
        &[],
    )
}
