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

use std::collections::HashMap;

use agdb::{
    DbAny,
    DbId,
};

use crate::plugins::args;
use crate::plugins::catalog;
#[cfg(feature = "docgen")]
use crate::services::catalog::artists::ArtistKey;
use crate::{
    plugins::db::{
        Artist,
        ArtistRelationType,
        ArtistType,
        CreditType,
        DbAsync,
    },
    services::{
        self,
        artists::{
            RelationDirection,
            ResolvedRelation,
        },
        catalog::{
            CatalogError,
            CreditRole,
            Viewer,
            artists::{
                ArtistFilter,
                Artists,
            },
            lookups,
            releases::Releases,
            tracks::Tracks,
        },
    },
};

#[derive(Clone, Default)]
pub(crate) struct ArtistsModuleStore {
    db: Option<DbAsync>,
}

impl ArtistsModuleStore {
    #[cfg(test)]
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
        .function(get_spec())
        .function(query_spec())
        .function(by_release_spec())
        .function(by_track_spec())
        .function(relations_spec())
        .userdata(ArtistType::_harmony_userdata_spec())
        .userdata(ArtistRelationType::_harmony_userdata_spec())
        .userdata(CreditType::_harmony_userdata_spec())
        .install(|_| Ok(ModuleExport::new(ArtistsModule)))
}

fn query_spec() -> FunctionSpec {
    FunctionSpec::async_fn("query")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("query")
        .args::<luau::Table>()
        .returns::<luau::Value>()
        .call_async(std::sync::Arc::new(query_callback))
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

fn by_track_spec() -> FunctionSpec {
    FunctionSpec::async_fn("by_track")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("track_ids")
        .args::<Vec<u64>>()
        .returns::<luau::Table>()
        .call_async(std::sync::Arc::new(by_track_callback))
}

fn relations_spec() -> FunctionSpec {
    FunctionSpec::async_fn("relations")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("ids")
        .args::<Vec<u64>>()
        .returns::<luau::Table>()
        .call_async(std::sync::Arc::new(relations_callback))
}

fn get_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let db = frame.vm.data().get::<ArtistsModuleStore>()?.db()?;
    catalog::keyed_lookup(&mut frame, db, "ids", lookups::get::<Artists>)
}

fn by_release_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let db = frame.vm.data().get::<ArtistsModuleStore>()?.db()?;
    catalog::keyed_lookup(
        &mut frame,
        db,
        "release_ids",
        lookups::artists_by_owner::<Releases>,
    )
}

fn by_track_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let db = frame.vm.data().get::<ArtistsModuleStore>()?.db()?;
    catalog::keyed_lookup(
        &mut frame,
        db,
        "track_ids",
        lookups::artists_by_owner::<Tracks>,
    )
}

fn relations_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let db = frame.vm.data().get::<ArtistsModuleStore>()?.db()?;
    catalog::keyed_lookup(&mut frame, db, "ids", relation_infos)
}

fn relation_infos(
    db: &DbAny,
    viewer: &Viewer,
    ids: &[DbId],
) -> Result<HashMap<DbId, Vec<ArtistRelationInfo>>, CatalogError> {
    Ok(lookups::artist_relations(db, viewer, ids)?
        .into_iter()
        .map(|(id, relations)| {
            (
                id,
                relations.into_iter().map(to_artist_relation_info).collect(),
            )
        })
        .collect())
}

fn query_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let opts: luau::Table = frame.args.read_named("query")?;
    let filter = read_filter(frame.vm, &opts)?;
    let request = catalog::read_query::<Artists>(frame.vm, &opts, filter)?;
    let principal = crate::plugins::auth::dispatch_principal(&frame.context)?;
    let db = frame.vm.data().get::<ArtistsModuleStore>()?.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let viewer = catalog::viewer(&*db, principal)?;
        let page =
            services::catalog::page(&db, &viewer, &request.query, request.offset, request.limit)
                .map_err(catalog::error)?;
        catalog::page_table(page)?.into_luau_return()
    }))
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

fn read_filter(vm: &luau::Vm, table: &luau::Table) -> luau::runtime::Result<ArtistFilter> {
    let role = |key| {
        luau::table::optional_string_field(vm, table, key)?
            .as_deref()
            .map(CreditRole::parse)
            .transpose()
            .map_err(catalog::error)
    };
    Ok(ArtistFilter {
        ids: catalog::optional_ids(vm, table, "ids")?,
        exclude_ids: args::optional_unique_ids(vm, table, "exclude_ids")?,
        library: args::optional_positive_id(vm, table, "library_id")?,
        releases: catalog::optional_ids(vm, table, "release_ids")?,
        tracks: catalog::optional_ids(vm, table, "track_ids")?,
        genres: args::optional_unique_ids(vm, table, "genre_ids")?,
        credit_role: role("credit_role")?,
        exclude_credit_role: role("exclude_credit_role")?,
        credit_types: parse_optional_credit_types(vm, table, "credit_types")?,
        exclude_credit_types: parse_optional_credit_types(vm, table, "exclude_credit_types")?
            .unwrap_or_default(),
        artist_type: parse_optional_artist_type(vm, table, "artist_type")?,
        favorite: luau::table::optional_bool_field(vm, table, "favorite")?,
        listened: luau::table::optional_bool_field(vm, table, "listened")?,
        rating: Default::default(),
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
        luau::Value::Table(table) => args::array_values(vm, &table)?
            .into_iter()
            .map(|(_, value)| CreditType::_harmony_userdata_class().read_value(vm, key, value))
            .collect::<luau::runtime::Result<Vec<_>>>()
            .map(Some),
        other => Err(crate::plugins::runtime_error(format!(
            "{key} must be an array of CreditType values, got {}",
            other.type_name()
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
        catalog::credit_role_alias(),
    ]
    .into_iter()
    .chain(catalog::type_aliases::<ArtistKey>(
        "ArtistSortKey",
        "ArtistPage",
        "Artist",
    ))
    .collect()
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

    let mut query = InterfaceDescriptor::new("ArtistQuery", None);
    query.fields.extend([
        field("ids", Option::<Vec<u64>>::luau_type()),
        field("exclude_ids", Option::<Vec<u64>>::luau_type()),
        field("library_id", Option::<u64>::luau_type()),
        described_field(
            "release_ids",
            Option::<Vec<u64>>::luau_type(),
            "Artists credited on these releases or their tracks.",
        ),
        described_field(
            "track_ids",
            Option::<Vec<u64>>::luau_type(),
            "Artists credited on these tracks.",
        ),
        described_field(
            "genre_ids",
            Option::<Vec<u64>>::luau_type(),
            "Artists credited on a release in these genres, or on its tracks.",
        ),
        described_field(
            "credit_role",
            LuauType::optional(LuauType::named("CreditRole")),
            "Artists credited on a track, a release, or either.",
        ),
        field(
            "exclude_credit_role",
            LuauType::optional(LuauType::named("CreditRole")),
        ),
        field("credit_types", Option::<Vec<CreditType>>::luau_type()),
        field("exclude_credit_types", Option::<Vec<CreditType>>::luau_type()),
        field("artist_type", Option::<ArtistType>::luau_type()),
        field("favorite", Option::<bool>::luau_type()),
        described_field(
            "listened",
            Option::<bool>::luau_type(),
            "Whether the caller has listened to any track credited to the artist or on its releases.",
        ),
    ]);
    query.fields.extend(catalog::query_fields("ArtistSortKey"));

    vec![relation, query]
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
                path: vec!["get"],
                description: Some(
                    "The artists with these ids that the caller can see, keyed by id.",
                ),
                params: vec![param("ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(u64::luau_type(), LuauType::named("Artist"))],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["query"],
                description: Some(
                    "The artists the caller can see that pass every filter, sorted and paged. Artists are visible through credits on releases or tracks the caller can see. Without `sort`, a search ranks by relevance and anything else is by sort name.",
                ),
                params: vec![param("query", LuauType::named("ArtistQuery"))],
                returns: vec![LuauType::named("ArtistPage")],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["by_release"],
                description: Some(
                    "The artists credited on each of these releases that the caller can see, in credit order, keyed by release id.",
                ),
                params: vec![param("release_ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(
                    u64::luau_type(),
                    LuauType::array(LuauType::named("Artist")),
                )],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["by_track"],
                description: Some(
                    "The artists credited on each of these tracks that the caller can see, in credit order, keyed by track id.",
                ),
                params: vec![param("track_ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(
                    u64::luau_type(),
                    LuauType::array(LuauType::named("Artist")),
                )],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["relations"],
                description: Some(
                    "The relations of each of these artists to other artists the caller can see, keyed by artist id.",
                ),
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
