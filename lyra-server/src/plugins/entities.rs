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
use harmony_luau::render_definition_file_with_support;
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeInterface,
    DescribeTypeAlias,
    FieldDescriptor,
    LuauType,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFieldDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    TypeAliasDescriptor,
};

#[cfg(feature = "docgen")]
use crate::services::entities::{
    ArtistProjectionIncludes,
    ArtistProjectionInfo,
    ArtistProjectionKind,
    CreditProjectionInfo,
    CreditedArtistProjectionInfo,
    EntityLookupHints,
    ProjectionEntryInfo,
    ReleaseProjectionIncludes,
    ReleaseProjectionInfo,
    ReleaseProjectionKind,
    ReleaseProjectionTrack,
    TrackProjectionIncludes,
    TrackProjectionInfo,
    TrackProjectionKind,
};
use crate::{
    plugins::db::{
        self,
        DbAsync,
        ResolveId,
    },
    services::entities::{
        EntityInclude,
        EntityProjectionInfo,
        project_entities,
        project_entity,
    },
};

#[derive(Clone, Default)]
pub(crate) struct EntitiesModuleStore {
    db: Option<DbAsync>,
}

impl EntitiesModuleStore {
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: DbAsync) -> Self {
        Self { db: Some(db) }
    }

    fn db(&self) -> luau::runtime::Result<DbAsync> {
        self.db.clone().ok_or_else(|| {
            crate::plugins::runtime_error(
                "lyra/entities requires a database-backed plugin executor",
            )
        })
    }
}

struct EntitiesModule;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/entities")
        .capability("lyra.entities")
        .function(query_spec("query", ProjectionKind::Any))
        .function(query_spec("query_release", ProjectionKind::Release))
        .function(query_spec("query_track", ProjectionKind::Track))
        .function(query_spec("query_artist", ProjectionKind::Artist))
        .function(get_type_spec())
        .function(query_many_spec())
        .initializer(install_entity_constants)
        .install(|_| Ok(ModuleExport::new(EntitiesModule)))
}

#[derive(Clone, Copy)]
enum ProjectionKind {
    Any,
    Release,
    Track,
    Artist,
}

fn query_spec(name: &'static str, kind: ProjectionKind) -> FunctionSpec {
    FunctionSpec::async_fn(name)
        .arg_name("request")
        .args::<luau::Table>()
        .returns::<luau::Value>()
        .call_async(std::sync::Arc::new(move |frame| {
            query_callback(frame, kind)
        }))
}

fn get_type_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_type")
        .arg_name("id")
        .args::<luau::Value>()
        .returns::<Option<String>>()
        .call_async(std::sync::Arc::new(get_type_callback))
}

fn query_many_spec() -> FunctionSpec {
    FunctionSpec::async_fn("query_many")
        .arg_name("request")
        .args::<luau::Table>()
        .returns::<luau::Table>()
        .call_async(std::sync::Arc::new(query_many_callback))
}

fn query_callback(
    mut frame: luau::AsyncCallFrame<'_>,
    kind: ProjectionKind,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let request: luau::Table = frame.args.read_named("request")?;
    let (resolve_id, includes, library_id) = parse_query_request(frame.vm, &request)?;
    let store = frame
        .vm
        .data()
        .get::<EntitiesModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let query_id = resolve_id
            .to_query_id(&db)
            .map_err(crate::plugins::runtime_error)?
            .ok_or_else(|| crate::plugins::runtime_error("could not resolve id"))?;
        let projection = project_entity(&db, query_id, &includes, library_id)
            .map_err(crate::plugins::runtime_error)?;

        let value = match (kind, projection) {
            (ProjectionKind::Any, projection) => projection_to_luau_owned(projection)?,
            (ProjectionKind::Release, EntityProjectionInfo::Release(projection)) => {
                harmony_luau::serializable_to_luau_owned(projection)?
            }
            (ProjectionKind::Track, EntityProjectionInfo::Track(projection)) => {
                harmony_luau::serializable_to_luau_owned(projection)?
            }
            (ProjectionKind::Artist, EntityProjectionInfo::Artist(projection)) => {
                harmony_luau::serializable_to_luau_owned(projection)?
            }
            (ProjectionKind::Release, EntityProjectionInfo::Track(_)) => {
                return Err(crate::plugins::runtime_error(
                    "requested entity is a track, not a release",
                ));
            }
            (ProjectionKind::Release, EntityProjectionInfo::Artist(_)) => {
                return Err(crate::plugins::runtime_error(
                    "requested entity is an artist, not a release",
                ));
            }
            (ProjectionKind::Track, EntityProjectionInfo::Release(_)) => {
                return Err(crate::plugins::runtime_error(
                    "requested entity is a release, not a track",
                ));
            }
            (ProjectionKind::Track, EntityProjectionInfo::Artist(_)) => {
                return Err(crate::plugins::runtime_error(
                    "requested entity is an artist, not a track",
                ));
            }
            (ProjectionKind::Artist, EntityProjectionInfo::Release(_)) => {
                return Err(crate::plugins::runtime_error(
                    "requested entity is a release, not an artist",
                ));
            }
            (ProjectionKind::Artist, EntityProjectionInfo::Track(_)) => {
                return Err(crate::plugins::runtime_error(
                    "requested entity is a track, not an artist",
                ));
            }
        };

        Ok(value)
    }))
}

fn get_type_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let id_value: luau::Value = frame.args.read_named("id")?;
    let resolve_id = parse_resolve_id(id_value)?;
    let store = frame
        .vm
        .data()
        .get::<EntitiesModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let db_id = resolve_id
            .to_db_id(&db)
            .map_err(crate::plugins::runtime_error)?;
        let entity_type = match db_id {
            Some(id) => {
                db::entities::get_element_type(&db, id).map_err(crate::plugins::runtime_error)
            }
            None => Ok(None),
        }?;

        entity_type.into_luau_return()
    }))
}

fn query_many_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let request: luau::Table = frame.args.read_named("request")?;
    let (ids, includes, library_id) = parse_query_many_request(frame.vm, &request)?;
    let store = frame
        .vm
        .data()
        .get::<EntitiesModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let mut query_ids = Vec::new();
        let mut keys = Vec::new();
        for (key, resolve_id) in ids {
            let Some(query_id) = resolve_id
                .to_query_id(&db)
                .map_err(crate::plugins::runtime_error)?
            else {
                continue;
            };
            keys.push(key);
            query_ids.push(query_id);
        }
        let projections = project_entities(&db, query_ids, &includes, library_id)
            .map_err(crate::plugins::runtime_error)?;

        let mut table = luau::OwnedTable::with_capacity(0, keys.len());
        for (key, projection) in keys.into_iter().zip(projections.into_iter()) {
            table.set_field(key, projection_to_luau_owned(projection)?);
        }
        Ok(luau::Value::TableData(table))
    }))
}

fn parse_query_request(
    vm: &luau::Vm,
    request: &luau::Table,
) -> luau::runtime::Result<(ResolveId, Vec<EntityInclude>, Option<agdb::DbId>)> {
    let id = parse_resolve_id(required_value(vm, request, "id")?)?;
    let includes = parse_includes(vm, request.get_raw(vm, "include")?)?;
    let library_id = parse_optional_db_id(vm, request, "library_id")?;
    Ok((id, includes, library_id))
}

fn parse_query_many_request(
    vm: &luau::Vm,
    request: &luau::Table,
) -> luau::runtime::Result<(
    Vec<(String, ResolveId)>,
    Vec<EntityInclude>,
    Option<agdb::DbId>,
)> {
    let ids = match required_value(vm, request, "ids")? {
        luau::Value::Table(table) => {
            let mut ids = Vec::new();
            for (_, value) in table.pairs_raw(vm)? {
                let key = match &value {
                    luau::Value::Integer(value) if *value > 0 => value.to_string(),
                    luau::Value::Number(value)
                        if value.is_finite() && value.fract() == 0.0 && *value > 0.0 =>
                    {
                        (*value as i64).to_string()
                    }
                    luau::Value::String(bytes) => {
                        String::from_utf8(bytes.clone()).map_err(crate::plugins::runtime_error)?
                    }
                    _ => continue,
                };
                ids.push((key, parse_resolve_id(value)?));
            }
            ids
        }
        other => {
            return Err(crate::plugins::runtime_error(format!(
                "ids must be an array of integer or string values, got {}",
                other.type_name()
            )));
        }
    };
    let includes = parse_includes(vm, request.get_raw(vm, "include")?)?;
    let library_id = parse_optional_db_id(vm, request, "library_id")?;
    Ok((ids, includes, library_id))
}

fn parse_includes(vm: &luau::Vm, value: luau::Value) -> luau::runtime::Result<Vec<EntityInclude>> {
    let mut include_values = Vec::new();
    match value {
        luau::Value::Nil => {}
        luau::Value::String(bytes) => {
            include_values.push(String::from_utf8(bytes).map_err(crate::plugins::runtime_error)?)
        }
        luau::Value::Table(table) => {
            for (_, value) in table.pairs_raw(vm)? {
                let luau::Value::String(bytes) = value else {
                    return Err(crate::plugins::runtime_error(
                        "include entries must be strings when include is an array",
                    ));
                };
                include_values
                    .push(String::from_utf8(bytes).map_err(crate::plugins::runtime_error)?);
            }
        }
        other => {
            return Err(crate::plugins::runtime_error(format!(
                "include must be a string or an array of strings, got {}",
                other.type_name()
            )));
        }
    }

    let mut includes = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for include_key in include_values {
        let include = EntityInclude::parse(&include_key).ok_or_else(|| {
            let valid = EntityInclude::ALL
                .iter()
                .map(|include| include.as_key())
                .collect::<Vec<_>>()
                .join(", ");
            crate::plugins::runtime_error(format!(
                "unknown include '{}'; expected one of: {valid}",
                include_key
            ))
        })?;
        if seen.insert(include) {
            includes.push(include);
        }
    }
    Ok(includes)
}

fn parse_resolve_id(value: luau::Value) -> luau::runtime::Result<ResolveId> {
    match value {
        luau::Value::Integer(value) => Ok(ResolveId::DbId(agdb::DbId(value))),
        luau::Value::Number(value) if value.is_finite() && value.fract() == 0.0 => {
            Ok(ResolveId::DbId(agdb::DbId(value as i64)))
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

fn parse_optional_db_id(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Option<agdb::DbId>> {
    match table.get_raw(vm, key)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Integer(value) if value > 0 => Ok(Some(agdb::DbId(value))),
        luau::Value::Number(value) if value.is_finite() && value.fract() == 0.0 && value > 0.0 => {
            Ok(Some(agdb::DbId(value as i64)))
        }
        _ => Err(crate::plugins::runtime_error(format!(
            "{key} must be a positive integer when provided"
        ))),
    }
}

fn required_value(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<luau::Value> {
    let value = table.get_raw(vm, key)?;
    if matches!(value, luau::Value::Nil) {
        Err(crate::plugins::runtime_error(format!(
            "missing required field: {key}"
        )))
    } else {
        Ok(value)
    }
}

fn projection_to_luau_owned(
    projection: EntityProjectionInfo,
) -> luau::runtime::Result<luau::Value> {
    match projection {
        EntityProjectionInfo::Release(projection) => {
            harmony_luau::serializable_to_luau_owned(projection)
        }
        EntityProjectionInfo::Track(projection) => {
            harmony_luau::serializable_to_luau_owned(projection)
        }
        EntityProjectionInfo::Artist(projection) => {
            harmony_luau::serializable_to_luau_owned(projection)
        }
    }
}

fn install_entity_constants(
    vm: &luau::Vm,
    _origin: &harmony_core::ChunkOrigin,
    root: &luau::Table,
) -> luau::runtime::Result<()> {
    install_string_table(
        vm,
        root,
        "CreditType",
        &[
            ("Artist", "artist"),
            ("Vocalist", "vocalist"),
            ("Instrumentalist", "instrumentalist"),
            ("Composer", "composer"),
            ("Lyricist", "lyricist"),
            ("Arranger", "arranger"),
            ("Writer", "writer"),
            ("Producer", "producer"),
            ("Conductor", "conductor"),
            ("Engineer", "engineer"),
            ("Mixer", "mixer"),
            ("Remixer", "remixer"),
        ],
    )?;
    install_string_table(
        vm,
        root,
        "ArtistCreditSource",
        &[("Track", "track"), ("Release", "release")],
    )
}

fn install_string_table(
    vm: &luau::Vm,
    root: &luau::Table,
    key: &str,
    entries: &[(&str, &str)],
) -> luau::runtime::Result<()> {
    let table = vm.create_table()?;
    for (name, value) in entries {
        table.set_raw(vm, name, luau::Value::String(value.as_bytes().to_vec()))?;
    }
    table.set_readonly(vm, true)?;
    root.set_table_raw(vm, key, &table)
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
fn include_selector_type() -> LuauType {
    LuauType::union(vec![String::luau_type(), Vec::<String>::luau_type()])
}

#[cfg(feature = "docgen")]
fn string_enum(values: impl IntoIterator<Item = &'static str>) -> LuauType {
    LuauType::union(values.into_iter().map(LuauType::string_literal).collect())
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
        field("locked", Option::<bool>::luau_type()),
        field("created_at", Option::<u64>::luau_type()),
        field("ctime", Option::<u64>::luau_type()),
    ])
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
fn artist_type() -> LuauType {
    LuauType::object(vec![
        field("db_id", Option::<i64>::luau_type()),
        field("id", String::luau_type()),
        field("artist_name", String::luau_type()),
        field("scan_name", String::luau_type()),
        field("sort_name", Option::<String>::luau_type()),
        field(
            "artist_type",
            LuauType::optional(LuauType::named("ArtistType")),
        ),
        field("description", Option::<String>::luau_type()),
        field("verified", bool::luau_type()),
        field("locked", Option::<bool>::luau_type()),
        field("created_at", Option::<u64>::luau_type()),
    ])
}

#[cfg(feature = "docgen")]
fn entity_type_aliases() -> Vec<TypeAliasDescriptor> {
    vec![
        TypeAliasDescriptor::new(
            "CreditType",
            string_enum([
                "artist",
                "vocalist",
                "instrumentalist",
                "composer",
                "lyricist",
                "arranger",
                "writer",
                "producer",
                "conductor",
                "engineer",
                "mixer",
                "remixer",
            ]),
            None,
        ),
        TypeAliasDescriptor::new(
            "ArtistCreditSource",
            string_enum(["track", "release"]),
            None,
        ),
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
        TypeAliasDescriptor::new(
            "ArtistType",
            string_enum(["person", "group", "character", "orchestra", "choir"]),
            None,
        ),
        TypeAliasDescriptor::new("Track", track_type(), None),
        TypeAliasDescriptor::new("Release", release_type(), None),
        TypeAliasDescriptor::new("Artist", artist_type(), None),
        ReleaseProjectionKind::type_alias_descriptor(),
        TrackProjectionKind::type_alias_descriptor(),
        ArtistProjectionKind::type_alias_descriptor(),
        TypeAliasDescriptor::new("EntityIncludeSelector", include_selector_type(), None),
        TypeAliasDescriptor::new(
            "EntityQueryRequest",
            LuauType::object(vec![
                field("id", resolve_id_type()),
                field(
                    "include",
                    LuauType::optional(LuauType::named("EntityIncludeSelector")),
                ),
                field("library_id", Option::<i64>::luau_type()),
            ]),
            None,
        ),
        TypeAliasDescriptor::new(
            "EntityQueryManyRequest",
            LuauType::object(vec![
                field("ids", LuauType::array(resolve_id_type())),
                field(
                    "include",
                    LuauType::optional(LuauType::named("EntityIncludeSelector")),
                ),
                field("library_id", Option::<i64>::luau_type()),
            ]),
            None,
        ),
        TypeAliasDescriptor::new(
            "EntityProjectionInfo",
            LuauType::union(vec![
                LuauType::named("ReleaseProjectionInfo"),
                LuauType::named("TrackProjectionInfo"),
                LuauType::named("ArtistProjectionInfo"),
            ]),
            Some("Typed entity projection keyed by entity_type."),
        ),
    ]
}

#[cfg(feature = "docgen")]
fn entity_interfaces() -> Vec<harmony_luau::InterfaceDescriptor> {
    vec![
        EntityLookupHints::interface_descriptor(),
        ProjectionEntryInfo::interface_descriptor(),
        CreditProjectionInfo::interface_descriptor(),
        CreditedArtistProjectionInfo::interface_descriptor(),
        ReleaseProjectionTrack::interface_descriptor(),
        ReleaseProjectionIncludes::interface_descriptor(),
        TrackProjectionIncludes::interface_descriptor(),
        ArtistProjectionIncludes::interface_descriptor(),
        ReleaseProjectionInfo::interface_descriptor(),
        TrackProjectionInfo::interface_descriptor(),
        ArtistProjectionInfo::interface_descriptor(),
    ]
}

#[cfg(feature = "docgen")]
fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "Entities",
        local_name: "entities",
        description: None,
        fields: [
            (vec!["CreditType", "Artist"], "artist"),
            (vec!["CreditType", "Vocalist"], "vocalist"),
            (vec!["CreditType", "Instrumentalist"], "instrumentalist"),
            (vec!["CreditType", "Composer"], "composer"),
            (vec!["CreditType", "Lyricist"], "lyricist"),
            (vec!["CreditType", "Arranger"], "arranger"),
            (vec!["CreditType", "Writer"], "writer"),
            (vec!["CreditType", "Producer"], "producer"),
            (vec!["CreditType", "Conductor"], "conductor"),
            (vec!["CreditType", "Engineer"], "engineer"),
            (vec!["CreditType", "Mixer"], "mixer"),
            (vec!["CreditType", "Remixer"], "remixer"),
            (vec!["ArtistCreditSource", "Track"], "track"),
            (vec!["ArtistCreditSource", "Release"], "release"),
        ]
        .into_iter()
        .map(|(path, value)| ModuleFieldDescriptor {
            path,
            description: None,
            ty: LuauType::string_literal(value),
        })
        .collect(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["query"],
                description: None,
                params: vec![param("request", LuauType::named("EntityQueryRequest"))],
                returns: vec![LuauType::named("EntityProjectionInfo")],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["query_release"],
                description: None,
                params: vec![param("request", LuauType::named("EntityQueryRequest"))],
                returns: vec![LuauType::named("ReleaseProjectionInfo")],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["query_track"],
                description: None,
                params: vec![param("request", LuauType::named("EntityQueryRequest"))],
                returns: vec![LuauType::named("TrackProjectionInfo")],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["query_artist"],
                description: None,
                params: vec![param("request", LuauType::named("EntityQueryRequest"))],
                returns: vec![LuauType::named("ArtistProjectionInfo")],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_type"],
                description: None,
                params: vec![param("id", resolve_id_type())],
                returns: vec![Option::<String>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["query_many"],
                description: None,
                params: vec![param("request", LuauType::named("EntityQueryManyRequest"))],
                returns: vec![LuauType::map(
                    String::luau_type(),
                    LuauType::named("EntityProjectionInfo"),
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
        &entity_type_aliases(),
        &entity_interfaces(),
        &[],
    )
}
