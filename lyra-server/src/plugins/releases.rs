// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::DbId;
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
    PositiveId,
    UniqueIds,
};
use crate::plugins::catalog;
use crate::plugins::db::{
    Db,
    ViewerDb,
};
#[cfg(feature = "docgen")]
use crate::services::catalog::releases::ReleaseKey;
use crate::services::{
    self,
    auth::Principal,
    catalog::{
        lookups,
        releases::{
            ReleaseFilter,
            Releases,
        },
    },
    releases as release_service,
};

pub(crate) struct ReleasesModule;

/// The `query` argument, read with its page bounds.
struct ReleaseQuery(catalog::PagedQuery<Releases>);

impl<'vm> luau::FromLuau<'vm> for ReleaseQuery {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let opts: luau::Table = reader.read()?;
        let filter = read_filter(reader.vm(), &opts)?;
        catalog::read_query::<Releases>(reader.vm(), &opts, filter).map(Self)
    }
}

impl luau::LuauTypeInfo for ReleaseQuery {
    fn luau_type() -> luau::LuauType {
        luau::LuauType::named("ReleaseQuery")
    }
}

/// The `chains` argument: arrays of track ids.
struct ChainsArgument(Vec<Vec<DbId>>);

impl<'vm> luau::FromLuau<'vm> for ChainsArgument {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let chains: luau::Table = reader.read()?;
        let vm = reader.vm();
        args::array_values(vm, &chains)?
            .into_iter()
            .map(|(index, chain)| match chain {
                luau::Value::Table(chain) => args::id_sequence(vm, &chain),
                _ => Err(crate::plugins::runtime_error(format!(
                    "chain {index} must be an array of track ids"
                ))),
            })
            .collect::<luau::runtime::Result<Vec<_>>>()
            .map(Self)
    }
}

impl luau::LuauTypeInfo for ChainsArgument {
    fn luau_type() -> luau::LuauType {
        Vec::<Vec<u64>>::luau_type()
    }
}

/// The `opts` argument of `similar`, parsed once the seed id is checked.
struct SimilarOptions(Option<luau::Table>);

impl<'vm> luau::FromLuau<'vm> for SimilarOptions {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        reader.read().map(Self)
    }
}

impl luau::LuauTypeInfo for SimilarOptions {
    fn luau_type() -> luau::LuauType {
        luau::LuauType::optional(luau::LuauType::named("SimilarReleaseOptions"))
    }
}

#[cfg(feature = "docgen")]
struct Release;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for Release {
    fn luau_type() -> LuauType {
        LuauType::named("Release")
    }
}

#[cfg(feature = "docgen")]
struct ReleaseChain;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for ReleaseChain {
    fn luau_type() -> LuauType {
        LuauType::named("ReleaseChain")
    }
}

#[cfg(feature = "docgen")]
struct ReleasePage;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for ReleasePage {
    fn luau_type() -> LuauType {
        LuauType::named("ReleasePage")
    }
}

#[harmony_macros::module(id = "lyra/releases", docs_feature = "docgen")]
impl ReleasesModule {
    #[harmony(
        description = "The releases with these ids that the caller can see, keyed by id.",
        returns(HashMap<u64, Release>)
    )]
    async fn get(
        ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(db, ids.0, lookups::get::<Releases>).await
    }

    #[harmony(
        description = "The releases the caller can see that pass every filter, sorted and paged. Without `sort`, a search ranks by relevance and anything else is by sort name. Filtering by `favorite`, `listened` or a rating, or sorting by listens or rating, needs the `lyra.favorites`, `lyra.listens` or `lyra.ratings` scope respectively.",
        returns(ReleasePage)
    )]
    async fn query(
        query: ReleaseQuery,
        #[context] db: ViewerDb,
        #[context] scopes: catalog::PersonalScopes,
    ) -> luau::runtime::Result<luau::ReturnValues> {
        let request = query.0;
        scopes.check(&request.query)?;
        let (db, viewer) = db.read().await?;
        let page =
            services::catalog::page(&db, &viewer, &request.query, request.offset, request.limit)
                .map_err(catalog::error)?;
        catalog::page_table(page)?.into_luau_return()
    }

    #[harmony(
        description = "The releases the caller can see that hold each of these tracks, keyed by track id.",
        returns(HashMap<u64, Vec<Release>>)
    )]
    async fn by_track(
        track_ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(db, track_ids.0, lookups::releases_by_track).await
    }

    #[harmony(
        description = "The releases the caller can see in each of these genres, keyed by genre id.",
        returns(HashMap<u64, Vec<Release>>)
    )]
    async fn by_genre(
        genre_ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(db, genre_ids.0, lookups::releases_by_genre).await
    }

    #[harmony(
        description = "The chains on each of these releases that the caller can see, ordered by where their first track sits, keyed by release id.",
        returns(HashMap<u64, Vec<ReleaseChain>>)
    )]
    async fn chains(
        release_ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(db, release_ids.0, lookups::chains_by_release).await
    }

    #[harmony(
        description = "Replaces every chain on a release the caller can see with `chains`, each the release's track ids in play order, and returns the stored chains. An empty list clears them. Errors when a chain has fewer than two tracks, a track is listed more than once, a track is not on the release, or a track is already in a chain on another release. Chain ids may change on every call. Requires ManageMetadata.",
        returns(Vec<ReleaseChain>)
    )]
    async fn replace_chains(
        release_id: PositiveId,
        chains: ChainsArgument,
        #[context] principal: Principal,
        #[context] db: Db,
    ) -> luau::runtime::Result<luau::Value> {
        let release_db_id = release_id.0;
        let chains = chains.0;
        crate::services::auth::require_permission(&principal, lyra_db::Permission::ManageMetadata)
            .map_err(crate::plugins::runtime_error)?;
        let chains = {
            let mut db = db.write().await;
            if !crate::services::auth::access::entity_accessible(&db, &principal, release_db_id)? {
                return Err(crate::plugins::runtime_error(
                    lyra_db::chains::ChainError::ReleaseNotFound,
                ));
            }
            lyra_db::chains::replace_for_release(&mut db, release_db_id, &chains)?
                .map_err(crate::plugins::runtime_error)?
        };
        harmony_luau::serializable_to_luau_owned(
            chains
                .into_iter()
                .map(lookups::ReleaseChain::from)
                .collect::<Vec<_>>(),
        )
    }

    #[harmony(
        description = "Returns provider-ranked releases related to a local release. Returns nil when the seed release does not exist or is inaccessible to the supplied user.",
        returns(Option<Vec<Release>>)
    )]
    fn similar(
        vm: &luau::Vm,
        context: &luau::CallContext,
        release_db_id: i64,
        opts: SimilarOptions,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        if context
            .caller
            .get::<crate::plugins::executor::MetadataDispatchContext>()
            .is_ok()
        {
            return Err(crate::plugins::runtime_error(
                "releases.similar cannot be called from a metadata provider callback",
            ));
        }
        if release_db_id <= 0 {
            return Err(crate::plugins::runtime_error(
                "releases.similar release_db_id must be a positive integer",
            ));
        }
        let request = parse_similar_options(vm, opts.0)?;
        let principal = crate::plugins::auth::require_dispatch_principal(context)?;
        let provider_vm = vm.clone();

        Ok(luau::ScheduledFuture::new(async move {
            let options =
                release_service::SimilarReleaseOptions::for_principal(&principal, request.limit);
            let releases = similar_in_vm(DbId(release_db_id), &options, provider_vm).await?;
            match releases {
                Some(releases) => harmony_luau::serializable_to_luau_owned(releases),
                None => Ok(luau::Value::Nil),
            }
        }))
    }
}

/// Dispatches to the providers on the VM making the call, which the executor
/// is already running; queueing to it would deadlock.
async fn similar_in_vm(
    seed_db_id: DbId,
    options: &release_service::SimilarReleaseOptions,
    provider_vm: luau::Vm,
) -> anyhow::Result<Option<Vec<lyra_db::Release>>> {
    release_service::similar_with_dispatch(seed_db_id, options, move |_runtime, request| {
        let provider_vm = provider_vm.clone();
        async move {
            crate::plugins::executor::dispatch_similar_releases_in_vm(provider_vm, request).await
        }
    })
    .await
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
        TypeAliasDescriptor::new(
            "ReleaseChain",
            LuauType::object(vec![
                field("id", String::luau_type()),
                described_field(
                    "track_ids",
                    Vec::<u64>::luau_type(),
                    "The chain's tracks in play order.",
                ),
            ]),
            Some(
                "A chain contains tracks from one release that Lyra keeps together and in order when Lyra decides the play order.",
            ),
        ),
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
    descriptor.fields.extend(catalog::rating_fields());
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
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &ReleasesModule::module_descriptor(),
        &release_type_aliases(),
        &[release_query(), similar_release_options()],
        &[],
    )
}

#[cfg(test)]
#[path = "releases/similar_tests.rs"]
mod similar_tests;

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
