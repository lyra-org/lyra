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
use harmony_luau::render_definition_file_with_support;
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
};
use serde::Serialize;

use crate::{
    plugins::db::{
        self,
        Artist,
        ArtistRelationType,
        ArtistType,
        CreditType,
        DbAsync,
        ListOptions,
        ResolveId,
        parse_sort_direction,
        parse_sort_specs_tokens,
    },
    services::artists::{
        self as artist_services,
        RelationDirection,
        ResolvedRelation,
    },
};

#[derive(Clone, Default)]
pub(crate) struct ArtistsModuleStore {
    db: Option<DbAsync>,
}

impl ArtistsModuleStore {
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: DbAsync) -> Self {
        Self { db: Some(db) }
    }

    fn db(&self) -> luau::runtime::Result<DbAsync> {
        self.db.clone().ok_or_else(|| {
            crate::plugins::runtime_error("lyra/artists requires a database-backed plugin executor")
        })
    }
}

struct ArtistsModule;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/artists")
        .capability("lyra.artists")
        .function(list_spec())
        .function(query_spec())
        .function(query_credited_spec())
        .function(list_by_library_spec())
        .function(list_many_spec())
        .function(list_relations_many_spec())
        .userdata(ArtistType::_harmony_userdata_spec())
        .userdata(ArtistRelationType::_harmony_userdata_spec())
        .userdata(CreditType::_harmony_userdata_spec())
        .install(|_| Ok(ModuleExport::new(ArtistsModule)))
}

fn list_spec() -> FunctionSpec {
    FunctionSpec::async_fn("list")
        .arg_name("scope")
        .args::<Option<ResolveId>>()
        .returns::<Vec<Artist>>()
        .call_async(std::sync::Arc::new(list_callback))
}

fn query_spec() -> FunctionSpec {
    FunctionSpec::async_fn("query")
        .arg_name("opts")
        .args::<luau::Table>()
        .returns::<luau::Value>()
        .call_async(std::sync::Arc::new(query_callback))
}

fn query_credited_spec() -> FunctionSpec {
    FunctionSpec::async_fn("query_credited")
        .arg_name("opts")
        .args::<luau::Table>()
        .returns::<luau::Value>()
        .call_async(std::sync::Arc::new(query_credited_callback))
}

fn list_by_library_spec() -> FunctionSpec {
    FunctionSpec::async_fn("list_by_library")
        .arg_name("library_id")
        .args::<i64>()
        .returns::<Vec<Artist>>()
        .call_async(std::sync::Arc::new(list_by_library_callback))
}

fn list_many_spec() -> FunctionSpec {
    FunctionSpec::async_fn("list_many")
        .arg_name("ids")
        .args::<Vec<u64>>()
        .returns::<luau::Table>()
        .call_async(std::sync::Arc::new(list_many_callback))
}

fn list_relations_many_spec() -> FunctionSpec {
    FunctionSpec::async_fn("list_relations_many")
        .arg_name("ids")
        .args::<Vec<u64>>()
        .returns::<luau::Table>()
        .call_async(std::sync::Arc::new(list_relations_many_callback))
}

fn list_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let scope = frame
        .args
        .read_optional_named::<luau::Value>("scope")?
        .map(parse_resolve_id)
        .transpose()?
        .unwrap_or_else(|| ResolveId::alias("artists"));
    let store = frame
        .vm
        .data()
        .get::<ArtistsModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let query_id = scope
            .to_query_id(&db)
            .map_err(crate::plugins::runtime_error)?
            .ok_or_else(|| crate::plugins::runtime_error("could not resolve scope"))?;
        let artists = db::artists::get(&db, query_id).map_err(crate::plugins::runtime_error)?;
        Ok(harmony_luau::serializable_to_luau_owned(artists)?)
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
        .get::<ArtistsModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let scope = request
            .scope
            .unwrap_or_else(|| ResolveId::alias("artists"))
            .to_query_id(&db)
            .map_err(crate::plugins::runtime_error)?
            .ok_or_else(|| crate::plugins::runtime_error("could not resolve scope"))?;
        let result = db::artists::query(&db, scope, &request.list_options, request.artist_type)
            .map_err(crate::plugins::runtime_error)?;
        query_result_table(result.entries, result.total_count, result.offset)?.into_luau_return()
    }))
}

fn query_credited_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let opts: luau::Table = frame.args.read_named("opts")?;
    let request = parse_credited_query_options(frame.vm, &opts)?;
    let store = frame
        .vm
        .data()
        .get::<ArtistsModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let filters = artist_services::CreditedArtistFilters {
            artist_type: request.artist_type,
            credit_types: request.credit_types,
            exclude_credit_types: request.exclude_credit_types,
        };
        let result = artist_services::query_credited(
            &db,
            request.scope.as_ref(),
            &filters,
            &request.list_options,
        )
        .map_err(crate::plugins::runtime_error)?;
        query_result_table(result.entries, result.total_count, result.offset)?.into_luau_return()
    }))
}

fn list_by_library_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let library_id: i64 = frame.args.read_named("library_id")?;
    let store = frame
        .vm
        .data()
        .get::<ArtistsModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let artists = db::artists::get_by_library(&db, DbId(library_id))
            .map_err(crate::plugins::runtime_error)?;
        Ok(harmony_luau::serializable_to_luau_owned(artists)?)
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
        .get::<ArtistsModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let related =
            db::artists::get_many_by_owner(&db, &ids).map_err(crate::plugins::runtime_error)?;

        let mut table = luau::OwnedTable::with_entry_capacity(0, 0, ids.len());
        for id in ids {
            let artists = related.get(&id).cloned().unwrap_or_default();
            crate::plugins::set_owned_db_id_key(
                &mut table,
                id,
                harmony_luau::serializable_to_luau_owned(artists)?,
            );
        }
        table.into_luau_return()
    }))
}

fn list_relations_many_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let ids_table: luau::Table = frame.args.read_named("ids")?;
    let ids = parse_db_ids(frame.vm, &ids_table)?;
    let store = frame
        .vm
        .data()
        .get::<ArtistsModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let related = artist_services::get_relations_many(&db, &ids)
            .map_err(crate::plugins::runtime_error)?;

        let mut table = luau::OwnedTable::with_entry_capacity(0, 0, ids.len());
        for id in ids {
            let relations = related
                .get(&id)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(to_artist_relation_info)
                .collect::<Vec<_>>();
            crate::plugins::set_owned_db_id_key(
                &mut table,
                id,
                harmony_luau::serializable_to_luau_owned(relations)?,
            );
        }
        table.into_luau_return()
    }))
}

fn query_result_table(
    entries: Vec<Artist>,
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

#[derive(Serialize)]
struct ArtistRelationInfo {
    relation_type: ArtistRelationType,
    direction: &'static str,
    attributes: Option<String>,
    artist: Artist,
}

fn relation_direction_label(direction: RelationDirection) -> &'static str {
    match direction {
        RelationDirection::Incoming => "incoming",
        RelationDirection::Outgoing => "outgoing",
    }
}

fn to_artist_relation_info(relation: ResolvedRelation) -> ArtistRelationInfo {
    ArtistRelationInfo {
        relation_type: relation.relation_type,
        direction: relation_direction_label(relation.direction),
        attributes: relation.attributes,
        artist: relation.artist,
    }
}

struct ArtistQueryRequest {
    scope: Option<ResolveId>,
    artist_type: Option<ArtistType>,
    list_options: ListOptions,
}

struct CreditedArtistQueryRequest {
    scope: Option<ResolveId>,
    artist_type: Option<ArtistType>,
    credit_types: Option<Vec<CreditType>>,
    exclude_credit_types: Option<Vec<CreditType>>,
    list_options: ListOptions,
}

fn parse_query_options(
    vm: &luau::Vm,
    opts: &luau::Table,
) -> luau::runtime::Result<ArtistQueryRequest> {
    let scope = match opts.get_raw(vm, "scope")? {
        luau::Value::Nil => None,
        value => Some(parse_resolve_id(value)?),
    };
    let artist_type = parse_optional_artist_type(vm, opts, "artist_type")?;
    let list_options = parse_list_options(vm, opts)?;

    Ok(ArtistQueryRequest {
        scope,
        artist_type,
        list_options,
    })
}

fn parse_credited_query_options(
    vm: &luau::Vm,
    opts: &luau::Table,
) -> luau::runtime::Result<CreditedArtistQueryRequest> {
    let scope = match opts.get_raw(vm, "scope")? {
        luau::Value::Nil => None,
        value => Some(parse_resolve_id(value)?),
    };
    let artist_type = parse_optional_artist_type(vm, opts, "artist_type")?;
    let credit_types = parse_optional_credit_types(vm, opts, "credit_types")?;
    let exclude_credit_types = parse_optional_credit_types(vm, opts, "exclude_credit_types")?;
    let list_options = parse_list_options(vm, opts)?;

    Ok(CreditedArtistQueryRequest {
        scope,
        artist_type,
        credit_types,
        exclude_credit_types,
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

fn parse_optional_artist_type(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Option<ArtistType>> {
    let value = table.get_raw(vm, key)?;
    if matches!(value, luau::Value::Nil) {
        return Ok(None);
    }
    ArtistType::_harmony_userdata_class()
        .read_value(vm, key, value)
        .map(Some)
}

fn parse_optional_credit_types(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Option<Vec<CreditType>>> {
    match table.get_raw(vm, key)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Table(table) => {
            let mut values = Vec::new();
            for (entry_key, value) in table.pairs_raw(vm)? {
                if array_index(entry_key).is_none() {
                    continue;
                }
                values.push(CreditType::_harmony_userdata_class().read_value(vm, key, value)?);
            }
            Ok(Some(values))
        }
        other => Err(crate::plugins::runtime_error(format!(
            "{key} must be an array of CreditType values, got {}",
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
fn artist_type() -> LuauType {
    LuauType::object(vec![
        field("db_id", Option::<i64>::luau_type()),
        field("id", String::luau_type()),
        field("artist_name", String::luau_type()),
        field("scan_name", String::luau_type()),
        field("sort_name", Option::<String>::luau_type()),
        field(
            "artist_type",
            LuauType::optional(string_enum([
                "person",
                "group",
                "character",
                "orchestra",
                "choir",
            ])),
        ),
        field("description", Option::<String>::luau_type()),
        field("verified", bool::luau_type()),
        field("locked", Option::<bool>::luau_type()),
        field("created_at", Option::<u64>::luau_type()),
    ])
}

#[cfg(feature = "docgen")]
fn artist_type_aliases() -> Vec<TypeAliasDescriptor> {
    vec![
        TypeAliasDescriptor::new("Artist", artist_type(), None),
        TypeAliasDescriptor::new(
            "CreditedArtistQueryOptions",
            LuauType::intersection(vec![
                LuauType::named("ArtistQueryOptions"),
                LuauType::object(vec![
                    field("credit_types", Option::<Vec<CreditType>>::luau_type()),
                    field(
                        "exclude_credit_types",
                        Option::<Vec<CreditType>>::luau_type(),
                    ),
                ]),
            ]),
            None,
        ),
        TypeAliasDescriptor::new(
            "ArtistQueryResult",
            LuauType::object(vec![
                field("entities", LuauType::array(LuauType::named("Artist"))),
                field("total_count", i64::luau_type()),
                field("offset", i64::luau_type()),
            ]),
            None,
        ),
    ]
}

#[cfg(feature = "docgen")]
fn artist_interfaces() -> Vec<InterfaceDescriptor> {
    let mut relation = InterfaceDescriptor::new("ArtistRelationInfo", None);
    relation.fields.extend([
        field("relation_type", string_enum(["voice_actor", "member_of"])),
        field(
            "direction",
            LuauType::union(vec![
                LuauType::string_literal("incoming"),
                LuauType::string_literal("outgoing"),
            ]),
        ),
        field("attributes", Option::<String>::luau_type()),
        field("artist", LuauType::named("Artist")),
    ]);

    let mut query_options = InterfaceDescriptor::new("ArtistQueryOptions", None);
    query_options.fields.extend([
        field("scope", LuauType::optional(resolve_id_type())),
        field("sort_by", Option::<Vec<String>>::luau_type()),
        field("sort_order", LuauType::optional(sort_order_type())),
        field("offset", Option::<i64>::luau_type()),
        field("limit", Option::<i64>::luau_type()),
        field("search_term", Option::<String>::luau_type()),
        field("artist_type", Option::<ArtistType>::luau_type()),
    ]);

    vec![relation, query_options]
}

#[cfg(feature = "docgen")]
fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "Artists",
        local_name: "artists",
        description: None,
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["list"],
                description: None,
                params: vec![param("scope", LuauType::optional(resolve_id_type()))],
                returns: vec![LuauType::array(LuauType::named("Artist"))],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["query"],
                description: None,
                params: vec![param("opts", LuauType::named("ArtistQueryOptions"))],
                returns: vec![LuauType::named("ArtistQueryResult")],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["query_credited"],
                description: None,
                params: vec![param("opts", LuauType::named("CreditedArtistQueryOptions"))],
                returns: vec![LuauType::named("ArtistQueryResult")],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["list_by_library"],
                description: None,
                params: vec![param("library_id", i64::luau_type())],
                returns: vec![LuauType::array(LuauType::named("Artist"))],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["list_many"],
                description: None,
                params: vec![param("ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(
                    u64::luau_type(),
                    LuauType::array(LuauType::named("Artist")),
                )],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["list_relations_many"],
                description: None,
                params: vec![param("ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(
                    u64::luau_type(),
                    LuauType::array(LuauType::named("ArtistRelationInfo")),
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
        &artist_type_aliases(),
        &artist_interfaces(),
        &[
            <ArtistType as harmony_luau::DescribeUserData>::class_descriptor(),
            <ArtistRelationType as harmony_luau::DescribeUserData>::class_descriptor(),
            <CreditType as harmony_luau::DescribeUserData>::class_descriptor(),
        ],
    )
}
