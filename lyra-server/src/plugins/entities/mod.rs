// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
mod types;

use harmony_core::ModuleSpec;
use harmony_luau as luau;
use harmony_luau::FromCallFrame;
#[cfg(feature = "docgen")]
use harmony_luau::render_definition_file_with_support;
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeInterface,
    DescribeModule,
    DescribeTypeAlias,
    FieldDescriptor,
    LuauType,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFieldDescriptor,
    TypeAliasDescriptor,
};
#[cfg(feature = "docgen")]
use std::collections::HashMap;

use crate::plugins::args;
use crate::plugins::args::ResolveIdArg;
use crate::{
    plugins::db::{
        Db,
        ViewerDb,
    },
    services::{
        entities::{
            EntityInclude,
            EntityProjectionInfo,
            IdIncludes,
            apply_id_links,
            id_link_targets,
            project_visible_entities,
        },
        providers::{
            provider_registry,
            resolve_id_links,
        },
    },
};
use lyra_db::DbAsync;
use lyra_db::ResolveId;

#[derive(Clone, Copy)]
enum ProjectionKind {
    Any,
    Release,
    Track,
    Artist,
}

struct QueryRequest {
    id: ResolveId,
    includes: Vec<EntityInclude>,
    library_id: Option<agdb::DbId>,
}

impl<'vm> luau::FromLuau<'vm> for QueryRequest {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let request: luau::Table = reader.read()?;
        let id = args::resolve_id(required_value(&request, "id")?)?;
        let includes = parse_includes(request.get("include")?)?;
        let library_id = args::optional_positive_id(&request, "library_id")?;
        Ok(Self {
            id,
            includes,
            library_id,
        })
    }
}

struct QueryManyRequest {
    ids: Vec<(String, ResolveId)>,
    includes: Vec<EntityInclude>,
    library_id: Option<agdb::DbId>,
}

impl<'vm> luau::FromLuau<'vm> for QueryManyRequest {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let request: luau::Table = reader.read()?;
        let ids = match required_value(&request, "ids")? {
            luau::Value::Table(table) => {
                let mut ids = Vec::new();
                for (_, value) in table.pairs()? {
                    let key = match &value {
                        luau::Value::Integer(value) if *value > 0 => value.to_string(),
                        luau::Value::Number(value)
                            if value.is_finite() && value.fract() == 0.0 && *value > 0.0 =>
                        {
                            (*value as i64).to_string()
                        }
                        luau::Value::String(bytes) => String::from_utf8(bytes.clone())
                            .map_err(crate::plugins::runtime_error)?,
                        _ => continue,
                    };
                    ids.push((key, args::resolve_id(value)?));
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
        let includes = parse_includes(request.get("include")?)?;
        let library_id = args::optional_positive_id(&request, "library_id")?;
        Ok(Self {
            ids,
            includes,
            library_id,
        })
    }
}

/// The VM to run link generators on, when the `links` include is requested. Generators can't
/// request links themselves, even from tasks they spawn, since those inherit the caller context.
fn link_generator_vm(
    vm: &luau::Vm,
    context: &luau::CallContext,
    includes: &[EntityInclude],
) -> luau::runtime::Result<Option<luau::Vm>> {
    if !includes.contains(&EntityInclude::Links) {
        return Ok(None);
    }
    if context
        .caller
        .get::<crate::plugins::executor::IdLinkDispatchContext>()
        .is_ok()
    {
        return Err(crate::plugins::runtime_error(
            "the links include cannot be requested from an id link generator",
        ));
    }
    Ok(Some(vm.clone()))
}

#[cfg(feature = "docgen")]
struct EntityQueryRequest;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for EntityQueryRequest {
    fn luau_type() -> LuauType {
        LuauType::named("EntityQueryRequest")
    }
}

#[cfg(feature = "docgen")]
struct EntityQueryManyRequest;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for EntityQueryManyRequest {
    fn luau_type() -> LuauType {
        LuauType::named("EntityQueryManyRequest")
    }
}

#[cfg(feature = "docgen")]
struct EntityProjection;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for EntityProjection {
    fn luau_type() -> LuauType {
        LuauType::named("EntityProjectionInfo")
    }
}

pub(crate) struct EntitiesModule;

#[harmony_macros::module(
    id = "lyra/entities",
    docs_feature = "docgen",
    extend = extend_module,
    describe = describe_module
)]
impl EntitiesModule {
    #[harmony(args(request: EntityQueryRequest), returns(EntityProjection))]
    async fn query(
        request: QueryRequest,
        #[context] vm: luau::Vm,
        #[context] context: luau::CallContext,
    ) -> luau::runtime::Result<luau::Value> {
        query_entity(ProjectionKind::Any, request, vm, context).await
    }

    #[harmony(
        args(request: EntityQueryRequest),
        returns(types::LuauReleaseProjectionInfo)
    )]
    async fn query_release(
        request: QueryRequest,
        #[context] vm: luau::Vm,
        #[context] context: luau::CallContext,
    ) -> luau::runtime::Result<luau::Value> {
        query_entity(ProjectionKind::Release, request, vm, context).await
    }

    #[harmony(
        args(request: EntityQueryRequest),
        returns(types::LuauTrackProjectionInfo)
    )]
    async fn query_track(
        request: QueryRequest,
        #[context] vm: luau::Vm,
        #[context] context: luau::CallContext,
    ) -> luau::runtime::Result<luau::Value> {
        query_entity(ProjectionKind::Track, request, vm, context).await
    }

    #[harmony(
        args(request: EntityQueryRequest),
        returns(types::LuauArtistProjectionInfo)
    )]
    async fn query_artist(
        request: QueryRequest,
        #[context] vm: luau::Vm,
        #[context] context: luau::CallContext,
    ) -> luau::runtime::Result<luau::Value> {
        query_entity(ProjectionKind::Artist, request, vm, context).await
    }

    async fn get_type(
        id: ResolveIdArg,
        #[context] db: Db,
    ) -> luau::runtime::Result<Option<String>> {
        let db = db.read().await;
        match id.0.to_db_id(&db)? {
            Some(id) => {
                lyra_db::entities::get_element_type(&db, id).map_err(crate::plugins::runtime_error)
            }
            None => Ok(None),
        }
    }

    #[harmony(
        args(request: EntityQueryManyRequest),
        returns(HashMap<String, EntityProjection>)
    )]
    async fn query_many(
        request: QueryManyRequest,
        #[context] vm: luau::Vm,
        #[context] context: luau::CallContext,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let QueryManyRequest {
            ids,
            includes,
            library_id,
        } = request;
        let link_vm = link_generator_vm(&vm, &context, &includes)?;
        let scoped = ViewerDb::from_call_frame(&vm, &context)?;
        let mut keys = Vec::new();
        let projections = project_with_links(
            scoped.handle(),
            link_vm,
            &includes,
            library_id,
            |db, id_includes| {
                let mut resolved_keys = Vec::new();
                let mut query_ids = Vec::new();
                for (key, resolve_id) in ids {
                    let Some(query_id) = resolve_id.to_query_id(db)? else {
                        continue;
                    };
                    resolved_keys.push(key);
                    query_ids.push(query_id);
                }
                let viewer = scoped.viewer(db)?;
                let projections = project_visible_entities(
                    db,
                    &viewer,
                    query_ids,
                    &includes,
                    library_id,
                    id_includes,
                )?;
                let mut visible = Vec::with_capacity(projections.len());
                for (key, projection) in resolved_keys.into_iter().zip(projections) {
                    if let Some(projection) = projection {
                        keys.push(key);
                        visible.push(projection);
                    }
                }
                Ok(visible)
            },
        )
        .await?;

        let mut table = luau::OwnedTable::with_capacity(0, keys.len());
        for (key, projection) in keys.into_iter().zip(projections) {
            table.set_field(key, projection_to_luau_owned(projection)?);
        }
        Ok(table)
    }
}

fn extend_module(spec: ModuleSpec) -> ModuleSpec {
    spec.initializer(install_entity_constants)
}

async fn query_entity(
    kind: ProjectionKind,
    request: QueryRequest,
    vm: luau::Vm,
    context: luau::CallContext,
) -> luau::runtime::Result<luau::Value> {
    let QueryRequest {
        id: resolve_id,
        includes,
        library_id,
    } = request;
    let link_vm = link_generator_vm(&vm, &context, &includes)?;
    let scoped = ViewerDb::from_call_frame(&vm, &context)?;
    let mut projections = project_with_links(
        scoped.handle(),
        link_vm,
        &includes,
        library_id,
        |db, id_includes| {
            let query_id = resolve_id
                .to_query_id(db)?
                .ok_or_else(|| crate::plugins::runtime_error("could not resolve id"))?;
            let viewer = scoped.viewer(db)?;
            let projections = project_visible_entities(
                db,
                &viewer,
                vec![query_id],
                &includes,
                library_id,
                id_includes,
            )?;
            Ok(projections.into_iter().flatten().collect())
        },
    )
    .await?;
    let projection = projections
        .pop()
        .ok_or_else(|| crate::plugins::runtime_error("could not resolve id"))?;

    match (kind, projection) {
        (ProjectionKind::Any, projection) => projection_to_luau_owned(projection),
        (ProjectionKind::Release, EntityProjectionInfo::Release(projection)) => {
            harmony_luau::serializable_to_luau_owned(projection)
        }
        (ProjectionKind::Track, EntityProjectionInfo::Track(projection)) => {
            harmony_luau::serializable_to_luau_owned(projection)
        }
        (ProjectionKind::Artist, EntityProjectionInfo::Artist(projection)) => {
            harmony_luau::serializable_to_luau_owned(projection)
        }
        (ProjectionKind::Release, EntityProjectionInfo::Track(_)) => Err(
            crate::plugins::runtime_error("requested entity is a track, not a release"),
        ),
        (ProjectionKind::Release, EntityProjectionInfo::Artist(_)) => Err(
            crate::plugins::runtime_error("requested entity is an artist, not a release"),
        ),
        (ProjectionKind::Track, EntityProjectionInfo::Release(_)) => Err(
            crate::plugins::runtime_error("requested entity is a release, not a track"),
        ),
        (ProjectionKind::Track, EntityProjectionInfo::Artist(_)) => Err(
            crate::plugins::runtime_error("requested entity is an artist, not a track"),
        ),
        (ProjectionKind::Artist, EntityProjectionInfo::Release(_)) => Err(
            crate::plugins::runtime_error("requested entity is a release, not an artist"),
        ),
        (ProjectionKind::Artist, EntityProjectionInfo::Track(_)) => Err(
            crate::plugins::runtime_error("requested entity is a track, not an artist"),
        ),
    }
}

/// Projects under one DB read, then resolves the `links` include after the
/// lock is released, since generator functions may use the database.
async fn project_with_links(
    db: DbAsync,
    link_vm: Option<luau::Vm>,
    includes: &[EntityInclude],
    library_id: Option<agdb::DbId>,
    project: impl FnOnce(
        &agdb::DbAny,
        &mut IdIncludes<'_>,
    ) -> luau::runtime::Result<Vec<EntityProjectionInfo>>,
) -> luau::runtime::Result<Vec<EntityProjectionInfo>> {
    let (schemes, generators) = {
        let registry = provider_registry();
        let registry = registry.read().await;
        (
            registry.id_schemes(),
            link_vm.is_some().then(|| registry.id_link_generators()),
        )
    };
    let (mut projections, links) = {
        let db = db.read().await;
        let mut id_includes = IdIncludes::new(&schemes, includes);
        let projections = project(&db, &mut id_includes)?;
        let links = match (link_vm, generators, id_includes.into_link_rows()) {
            (Some(vm), Some(generators), Some(rows)) => {
                let targets = id_link_targets(&db, rows, library_id, &generators)?;
                Some((vm, generators, targets))
            }
            _ => None,
        };
        (projections, links)
    };
    if let Some((vm, generators, targets)) = links {
        let links = resolve_id_links(targets, &generators, |provider_id, calls| {
            crate::plugins::executor::dispatch_id_links_in_vm(vm.clone(), provider_id, calls)
        })
        .await;
        apply_id_links(&mut projections, &links);
    }
    Ok(projections)
}

fn parse_includes(value: luau::Value) -> luau::runtime::Result<Vec<EntityInclude>> {
    let mut include_values = Vec::new();
    match value {
        luau::Value::Nil => {}
        luau::Value::String(bytes) => {
            include_values.push(String::from_utf8(bytes).map_err(crate::plugins::runtime_error)?)
        }
        luau::Value::Table(table) => {
            for (_, value) in table.pairs()? {
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

fn required_value(table: &luau::Table, key: &str) -> luau::runtime::Result<luau::Value> {
    let value = table.get(key)?;
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
        table.set(name, luau::Value::String(value.as_bytes().to_vec()))?;
    }
    table.set_readonly(true)?;
    root.set_table(key, &table)
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
        field("track_gain_db", Option::<f64>::luau_type()),
        field("album_gain_db", Option::<f64>::luau_type()),
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
        types::LuauReleaseProjectionKind::type_alias_descriptor(),
        types::LuauTrackProjectionKind::type_alias_descriptor(),
        types::LuauArtistProjectionKind::type_alias_descriptor(),
        TypeAliasDescriptor::new("EntityIncludeSelector", include_selector_type(), None),
        TypeAliasDescriptor::new(
            "EntityQueryRequest",
            LuauType::object(vec![
                field("id", args::resolve_id_type()),
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
                field("ids", LuauType::array(args::resolve_id_type())),
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
        types::LuauEntityLookupHints::interface_descriptor(),
        types::LuauProjectionEntryInfo::interface_descriptor(),
        types::LuauCreditProjectionInfo::interface_descriptor(),
        types::LuauCreditedArtistProjectionInfo::interface_descriptor(),
        types::LuauReleaseProjectionTrack::interface_descriptor(),
        types::LuauReleaseProjectionIncludes::interface_descriptor(),
        types::LuauTrackProjectionIncludes::interface_descriptor(),
        types::LuauArtistProjectionIncludes::interface_descriptor(),
        types::LuauReleaseProjectionInfo::interface_descriptor(),
        types::LuauTrackProjectionInfo::interface_descriptor(),
        types::LuauArtistProjectionInfo::interface_descriptor(),
        crate::plugins::types::LuauIdLink::interface_descriptor(),
    ]
}

#[cfg(feature = "docgen")]
fn describe_module(descriptor: &mut ModuleDescriptor) {
    descriptor.fields = [
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
    .collect();
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &EntitiesModule::module_descriptor(),
        &entity_type_aliases(),
        &entity_interfaces(),
        &[],
    )
}
