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

use crate::plugins::args::{
    self,
    UniqueIds,
};
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
        },
    },
};

pub(crate) struct TracksModule;

/// The `query` argument, read with its page bounds.
struct TrackQuery(catalog::PagedQuery<Tracks>);

impl<'vm> luau::FromLuau<'vm> for TrackQuery {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let opts: luau::Table = reader.read()?;
        let filter = read_filter(reader.vm(), &opts)?;
        catalog::read_query::<Tracks>(reader.vm(), &opts, filter).map(Self)
    }
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
        returns(HashMap<u64, Track>)
    )]
    async fn get(
        ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(db, ids.0, lookups::get::<Tracks>).await
    }

    #[harmony(
        description = "`get` read as the system. Ignores library visibility, so it is only for public streaming routes, which must serve nothing from it but the audio.",
        returns(HashMap<u64, Track>)
    )]
    async fn streamable(
        ids: UniqueIds,
        #[context] db: SystemDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(db.0, ids.0, lookups::get::<Tracks>).await
    }

    #[harmony(
        description = "The tracks the caller can see that pass every filter, sorted and paged. Without `sort`, a search ranks by relevance, a query scoped to one release is in disc and track order, and anything else is by sort name.",
        returns(TrackPage)
    )]
    async fn query(
        query: TrackQuery,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::ReturnValues> {
        let request = query.0;
        let (db, viewer) = db.read().await?;
        let page =
            services::catalog::page(&db, &viewer, &request.query, request.offset, request.limit)
                .map_err(catalog::error)?;
        catalog::page_table(page)?.into_luau_return()
    }

    #[harmony(
        description = "The tracks on each of these releases that the caller can see, keyed by release id.",
        returns(HashMap<u64, Vec<Track>>)
    )]
    async fn by_release(
        release_ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(db, release_ids.0, lookups::tracks_by_release).await
    }

    #[harmony(
        description = "The tracks whose audio each of these entries holds, for the entries the caller can see, keyed by entry id.",
        returns(HashMap<u64, Vec<Track>>)
    )]
    async fn by_entry(
        entry_ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(db, entry_ids.0, lookups::tracks_by_entry).await
    }

    #[harmony(
        description = "The chain of each of these tracks that the caller can see and that is in one, keyed by track id.",
        returns(HashMap<u64, TrackChain>)
    )]
    async fn chains(
        track_ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(db, track_ids.0, lookups::chain_places).await
    }
}

fn read_filter(vm: &luau::Vm, table: &luau::Table) -> luau::runtime::Result<TrackFilter> {
    Ok(TrackFilter {
        ids: catalog::optional_ids(vm, table, "ids")?,
        exclude_ids: args::optional_unique_ids(vm, table, "exclude_ids")?,
        exclude_artists: args::optional_unique_ids(vm, table, "exclude_artist_ids")?,
        library: args::optional_positive_id(vm, table, "library_id")?,
        releases: catalog::optional_ids(vm, table, "release_ids")?,
        artists: catalog::read_artist_credit(vm, table)?,
        genres: args::optional_unique_ids(vm, table, "genre_ids")?,
        years: catalog::read_years(vm, table)?,
        favorite: luau::table::optional_bool_field(vm, table, "favorite")?,
        listened: luau::table::optional_bool_field(vm, table, "listened")?,
        rating: catalog::read_rating(vm, table)?,
    })
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
        field("ids", Option::<Vec<u64>>::luau_type()),
        field("exclude_ids", Option::<Vec<u64>>::luau_type()),
        field("library_id", Option::<u64>::luau_type()),
        field("release_ids", Option::<Vec<u64>>::luau_type()),
        field("genre_ids", Option::<Vec<u64>>::luau_type()),
        field("years", Option::<Vec<u32>>::luau_type()),
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
