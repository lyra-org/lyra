// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use harmony_luau as luau;
use harmony_luau::IntoLuauReturn;
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeModule,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
    TypeAliasDescriptor,
    render_definition_file_with_support,
};
#[cfg(feature = "docgen")]
use std::collections::HashMap;

use crate::plugins::args::UniqueIds;
use crate::plugins::catalog;
use crate::plugins::db::{
    SystemDb,
    ViewerDb,
};
#[cfg(feature = "docgen")]
use crate::services::catalog::tracks::TrackKey;
use crate::services::{
    self,
    catalog::{
        lookups,
        tracks::{
            TrackFilter,
            Tracks,
            YearRange,
        },
    },
};

pub(crate) struct TracksModule;

/// The `query` argument, read with its page bounds.
struct TrackQuery(catalog::PagedQuery<Tracks>);

impl<'vm> luau::FromLuau<'vm> for TrackQuery {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let opts: luau::Table = reader.read()?;
        let filter = TrackFilter {
            years: catalog::read_years(&opts)?,
            year_range: read_year_range(&opts)?,
            personal: catalog::read_personal(&opts)?,
            ..TrackFilter::default()
        };
        catalog::read_query::<Tracks>(&opts, filter).map(Self)
    }
}

/// The bounds `min_year` and `max_year` give.
fn read_year_range(table: &luau::Table) -> luau::runtime::Result<YearRange> {
    let bound = |key| {
        table
            .optional_i64_field(key)?
            .map(|value| {
                u32::try_from(value).map_err(|_| {
                    crate::plugins::runtime_error(format!(
                        "{key} must be a year from 0 to {}, got {value}",
                        u32::MAX
                    ))
                })
            })
            .transpose()
    };
    YearRange::new(bound("min_year")?, bound("max_year")?)
        .map_err(|_| crate::plugins::runtime_error("min_year must not exceed max_year"))
}

impl luau::LuauTypeInfo for TrackQuery {
    fn luau_type() -> luau::LuauType {
        luau::LuauType::named("TrackQuery")
    }
}

#[cfg(feature = "docgen")]
struct Track;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for Track {
    fn luau_type() -> LuauType {
        LuauType::named("Track")
    }
}

#[cfg(feature = "docgen")]
struct TrackChain;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for TrackChain {
    fn luau_type() -> LuauType {
        LuauType::named("TrackChain")
    }
}

#[cfg(feature = "docgen")]
struct TrackPage;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for TrackPage {
    fn luau_type() -> LuauType {
        LuauType::named("TrackPage")
    }
}

#[harmony_macros::module(id = "lyra/tracks", docs_feature = "docgen")]
impl TracksModule {
    #[harmony(
        description = "The tracks with these ids that the caller can see, keyed by id.",
        returns(HashMap<String, Track>)
    )]
    async fn get(
        ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(&db, ids.0, lookups::get::<Tracks>).await
    }

    #[harmony(
        description = "`get` read as the system. Ignores library visibility, so it is only for public streaming routes, which must serve nothing from it but the audio.",
        returns(HashMap<String, Track>)
    )]
    async fn streamable(
        ids: UniqueIds,
        #[context] db: SystemDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(&db.0, ids.0, lookups::get::<Tracks>).await
    }

    #[harmony(
        description = "The tracks the caller can see that pass every filter, sorted and paged. Without `sort`, a search ranks by relevance, a query scoped to one release is in disc and track order, and anything else is by sort name. Filtering by `favorite`, `listened` or a rating, or sorting by favorite time, listens or rating, needs the `lyra.favorites`, `lyra.listens` or `lyra.ratings` scope respectively.",
        returns(TrackPage)
    )]
    async fn query(
        query: TrackQuery,
        #[context] db: ViewerDb,
        #[context] scopes: catalog::PersonalScopes,
    ) -> luau::runtime::Result<luau::ReturnValues> {
        let mut request = query.0;
        scopes.check(&request.query)?;
        let (db, viewer) = db.read().await?;
        request.resolve_ids(&db)?;
        let page =
            services::catalog::page(&db, &viewer, &request.query, request.offset, request.limit)
                .map_err(catalog::error)?;
        catalog::page_table(page)?.into_luau_return()
    }

    #[harmony(
        description = "The tracks on each of these releases that the caller can see, keyed by release id.",
        returns(HashMap<String, Vec<Track>>)
    )]
    async fn by_release(
        release_ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(&db, release_ids.0, lookups::tracks_by_release).await
    }

    #[harmony(
        description = "The tracks whose audio each of these entries holds, for the entries the caller can see, keyed by entry id.",
        returns(HashMap<String, Vec<Track>>)
    )]
    async fn by_entry(
        entry_ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(&db, entry_ids.0, lookups::tracks_by_entry).await
    }

    #[harmony(
        description = "The chain of each of these tracks that the caller can see and that is in one, keyed by track id.",
        returns(HashMap<String, TrackChain>)
    )]
    async fn chains(
        track_ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(&db, track_ids.0, lookups::chain_places).await
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
        field("created_at_ms", Option::<u64>::luau_type()),
        field("file_created_at_ms", Option::<u64>::luau_type()),
    ])
}

#[cfg(feature = "docgen")]
fn track_type_aliases() -> Vec<TypeAliasDescriptor> {
    vec![
        TypeAliasDescriptor::new("Track", track_type(), None),
        TypeAliasDescriptor::new(
            "TrackChain",
            LuauType::object(vec![
                field("id", String::luau_type()),
                FieldDescriptor {
                    name: "position",
                    ty: u64::luau_type(),
                    description: Some("The track's 0-based place in the chain."),
                },
            ]),
            Some("The chain a track is in."),
        ),
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
        field("ids", Option::<Vec<String>>::luau_type()),
        field("exclude_ids", Option::<Vec<String>>::luau_type()),
        field("library_id", Option::<String>::luau_type()),
        field("release_ids", Option::<Vec<String>>::luau_type()),
        field("genre_ids", Option::<Vec<String>>::luau_type()),
        field("years", Option::<Vec<u32>>::luau_type()),
        FieldDescriptor {
            name: "min_year",
            ty: Option::<u32>::luau_type(),
            description: Some(
                "Inclusive lower bound on the track's own year, as `years` and the `year` sort key read it. Either bound leaves out tracks without a year, and `min_year` above `max_year` raises.",
            ),
        },
        FieldDescriptor {
            name: "max_year",
            ty: Option::<u32>::luau_type(),
            description: Some("Inclusive upper bound on the track's own year."),
        },
        field("favorite", Option::<bool>::luau_type()),
        field("listened", Option::<bool>::luau_type()),
    ]);
    descriptor.fields.extend(catalog::rating_fields());
    descriptor.fields.extend(catalog::credit_fields());
    descriptor
        .fields
        .extend(catalog::query_fields("TrackSortKey"));
    descriptor
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &TracksModule::module_descriptor(),
        &track_type_aliases(),
        &[track_query()],
        &[],
    )
}
