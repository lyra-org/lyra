// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use harmony_luau as luau;
use harmony_luau::IntoLuauReturn;
#[cfg(feature = "docgen")]
use harmony_luau::render_definition_file_with_support;
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeModule,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
    TypeAliasDescriptor,
};
use serde::Serialize;

use std::collections::HashMap;

use agdb::{
    DbAny,
    DbId,
};

use crate::plugins::args::{
    self,
    UniqueIds,
};
use crate::plugins::catalog;
use crate::services::catalog::artists::ArtistKey;
use crate::{
    plugins::db::{
        Db,
        ViewerDb,
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
use lyra_db::Artist;
use lyra_db::ArtistRelationType;
use lyra_db::ArtistType;
use lyra_db::CreditType;

pub(crate) struct ArtistsModule;

/// The `query` argument, read with its page bounds.
struct ArtistQuery(catalog::PagedQuery<Artists>);

impl<'vm> luau::FromLuau<'vm> for ArtistQuery {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let opts: luau::Table = reader.read()?;
        let filter = read_filter(reader.vm(), &opts)?;
        catalog::read_query::<Artists>(reader.vm(), &opts, filter).map(Self)
    }
}

impl luau::LuauTypeInfo for ArtistQuery {
    fn luau_type() -> luau::LuauType {
        luau::LuauType::named("ArtistQuery")
    }
}

#[cfg(feature = "docgen")]
struct ArtistRecord;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for ArtistRecord {
    fn luau_type() -> LuauType {
        LuauType::named("Artist")
    }
}

#[cfg(feature = "docgen")]
struct ArtistPage;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for ArtistPage {
    fn luau_type() -> LuauType {
        LuauType::named("ArtistPage")
    }
}

#[harmony_macros::module(
    id = "lyra/artists",
    docs_feature = "docgen",
    userdata(
        crate::plugins::types::LuauArtistType,
        crate::plugins::types::LuauArtistRelationType,
        crate::plugins::types::LuauCreditType
    )
)]
impl ArtistsModule {
    #[harmony(
        description = "The artists with these ids that the caller can see, keyed by id.",
        returns(HashMap<u64, ArtistRecord>)
    )]
    async fn get(
        ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(db, ids.0, lookups::get::<Artists>).await
    }

    #[harmony(
        description = "The artists the caller can see that pass every filter, sorted and paged. Artists are visible through credits on releases or tracks the caller can see. Without `sort`, a search ranks by relevance and anything else is by sort name. Filtering by `favorite`, `listened` or a rating, or sorting by listens or rating, needs the `lyra.favorites`, `lyra.listens` or `lyra.ratings` scope respectively.",
        returns(ArtistPage)
    )]
    async fn query(
        query: ArtistQuery,
        #[context] db: ViewerDb,
        #[context] scopes: catalog::PersonalScopes,
    ) -> luau::runtime::Result<luau::ReturnValues> {
        let request = query.0;
        scopes.check(&request.query)?;
        let (db, viewer) = db.read().await?;
        let page =
            services::catalog::page(&db, &viewer, &request.query, request.offset, request.limit)
                .and_then(|page| {
                    catalog::counted(
                        &db,
                        &viewer,
                        &request.query,
                        page,
                        &[
                            ("release_count", ArtistKey::ReleaseCount),
                            ("track_count", ArtistKey::TrackCount),
                            ("total_duration", ArtistKey::TotalDuration),
                        ],
                        |artist| artist.db_id.clone().map(DbId::from),
                        |artist| artist,
                    )
                })
                .map_err(catalog::error)?;
        catalog::page_table(page)?.into_luau_return()
    }

    #[harmony(
        description = "The artists credited on each of these releases that the caller can see, in credit order, keyed by release id.",
        returns(HashMap<u64, Vec<ArtistRecord>>)
    )]
    async fn by_release(
        release_ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(db, release_ids.0, lookups::artists_by_owner::<Releases>).await
    }

    #[harmony(
        description = "The artists credited on each of these tracks that the caller can see, in credit order, keyed by track id.",
        returns(HashMap<u64, Vec<ArtistRecord>>)
    )]
    async fn by_track(
        track_ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(db, track_ids.0, lookups::artists_by_owner::<Tracks>).await
    }

    // Unscoped: public artwork routes resolve names without a caller.
    #[harmony(
        description = "The id of the artist with exactly this name, ignoring case. Not scoped to any caller: it is only for resolving artwork on public artwork routes, which must return nothing but image bytes."
    )]
    async fn artwork_id_by_name(
        name: String,
        #[context] db: Db,
    ) -> luau::runtime::Result<Option<i64>> {
        let name = name.trim();
        if name.is_empty() {
            return Ok(None);
        }
        let db = db.read().await;
        let id = lyra_db::artists::find_by_scan_name(&*db, name)?;
        Ok(id.map(|id| id.0))
    }

    #[harmony(
        description = "The relations of each of these artists to other artists the caller can see, keyed by artist id.",
        returns(HashMap<u64, Vec<ArtistRelationInfo>>)
    )]
    async fn relations(
        ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(db, ids.0, relation_infos).await
    }
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
        rating: catalog::read_rating(vm, table)?,
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
    crate::plugins::types::LuauArtistType::_harmony_userdata_class()
        .read_value(vm, key, value)
        .map(|value| Some(value.into()))
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
            .map(|(_, value)| {
                crate::plugins::types::LuauCreditType::_harmony_userdata_class()
                    .read_value(vm, key, value)
                    .map(Into::into)
            })
            .collect::<luau::runtime::Result<Vec<_>>>()
            .map(Some),
        other => Err(crate::plugins::runtime_error(format!(
            "{key} must be an array of CreditType values, got {}",
            other.type_name()
        ))),
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
        field("locked", Option::<bool>::luau_type()),
        field("created_at", Option::<u64>::luau_type()),
    ])
}

#[cfg(feature = "docgen")]
fn artist_type_aliases() -> Vec<TypeAliasDescriptor> {
    vec![
        TypeAliasDescriptor::new("Artist", artist_type(), None),
        TypeAliasDescriptor::new(
            "ArtistSummary",
            LuauType::intersection(vec![
                LuauType::named("Artist"),
                LuauType::object(vec![
                    described_field(
                        "release_count",
                        u64::luau_type(),
                        "The visible releases credited to the artist.",
                    ),
                    described_field(
                        "track_count",
                        u64::luau_type(),
                        "The visible tracks credited to the artist or on its releases.",
                    ),
                    described_field(
                        "total_duration",
                        u64::luau_type(),
                        "The total duration of those tracks, in milliseconds.",
                    ),
                ]),
            ]),
            None,
        ),
        catalog::credit_role_alias(),
    ]
    .into_iter()
    .chain(catalog::type_aliases::<ArtistKey>(
        "ArtistSortKey",
        "ArtistPage",
        "ArtistSummary",
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
        field("credit_types", Option::<Vec<crate::plugins::types::LuauCreditType>>::luau_type()),
        field("exclude_credit_types", Option::<Vec<crate::plugins::types::LuauCreditType>>::luau_type()),
        field("artist_type", Option::<crate::plugins::types::LuauArtistType>::luau_type()),
        field("favorite", Option::<bool>::luau_type()),
        described_field(
            "listened",
            Option::<bool>::luau_type(),
            "Whether the caller has listened to any track credited to the artist or on its releases.",
        ),
    ]);
    query.fields.extend(catalog::rating_fields());
    query.fields.extend(catalog::query_fields("ArtistSortKey"));

    vec![relation, query]
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &ArtistsModule::module_descriptor(),
        &artist_type_aliases(),
        &artist_interfaces(),
        &[
            <crate::plugins::types::LuauArtistType as harmony_luau::DescribeUserData>::class_descriptor(),
            <crate::plugins::types::LuauArtistRelationType as harmony_luau::DescribeUserData>::class_descriptor(),
            <crate::plugins::types::LuauCreditType as harmony_luau::DescribeUserData>::class_descriptor(),
        ],
    )
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for ArtistRelationInfo {
    fn luau_type() -> LuauType {
        LuauType::named("ArtistRelationInfo")
    }
}
