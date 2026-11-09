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
use std::collections::HashMap;

use crate::plugins::args::{
    self,
    UniqueIds,
};
use crate::plugins::db::{
    Db,
    ViewerDb,
};
use crate::plugins::{
    catalog,
    ids,
};
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
    releases::{
        self as release_service,
        chains as chain_service,
    },
};

pub(crate) struct ReleasesModule;

/// The `query` argument, read with its page bounds.
/// The `query` argument, read with its page bounds.
struct ReleaseQuery(catalog::PagedQuery<Releases>);

impl<'vm> luau::FromLuau<'vm> for ReleaseQuery {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let opts: luau::Table = reader.read()?;
        let filter = ReleaseFilter {
            years: catalog::read_years(&opts)?,
            personal: catalog::read_personal(&opts)?,
            ..ReleaseFilter::default()
        };
        catalog::read_query::<Releases>(&opts, filter).map(Self)
    }
}

impl luau::LuauTypeInfo for ReleaseQuery {
    fn luau_type() -> luau::LuauType {
        luau::LuauType::named("ReleaseQuery")
    }
}

/// The `chains` argument: arrays of track ids.
struct ChainsArgument(Vec<Vec<String>>);

impl<'vm> luau::FromLuau<'vm> for ChainsArgument {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let chains: luau::Table = reader.read()?;
        args::array_values(&chains)?
            .into_iter()
            .map(|(index, chain)| match chain {
                luau::Value::Table(chain) => args::id_sequence(&chain),
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
        Vec::<Vec<String>>::luau_type()
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

crate::plugins::limits::module_limits!(&[crate::plugins::limits::Limit {
    name: "MAX_SIMILAR",
    value: release_service::MAX_SIMILAR_RELEASE_LIMIT,
    description: "The most releases `similar` returns; a larger `limit` is capped to it.",
}]);

#[harmony_macros::module(
    id = "lyra/releases",
    docs_feature = "docgen",
    extend = install_limits,
    describe = describe_limits
)]
impl ReleasesModule {
    #[harmony(
        description = "The releases with these ids that the caller can see, keyed by id.",
        returns(HashMap<String, Release>)
    )]
    async fn get(
        ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(&db, ids.0, lookups::get::<Releases>).await
    }

    #[harmony(
        description = "The releases the caller can see that pass every filter, sorted and paged. Without `sort`, a search ranks by relevance and anything else is by sort name. Filtering by `favorite`, `listened` or a rating, or sorting by favorite time, listens or rating, needs the `lyra.favorites`, `lyra.listens` or `lyra.ratings` scope respectively.",
        returns(ReleasePage)
    )]
    async fn query(
        query: ReleaseQuery,
        #[context] db: ViewerDb,
        #[context] scopes: catalog::PersonalScopes,
    ) -> luau::runtime::Result<luau::ReturnValues> {
        let mut request = query.0;
        scopes.check(&request.query)?;
        let (db, viewer) = db.read().await?;
        request.resolve_ids(&db)?;
        let page =
            services::catalog::page(&db, &viewer, &request.query, request.offset, request.limit)
                .and_then(|page| {
                    catalog::counted(
                        &db,
                        &viewer,
                        &request.query,
                        page,
                        &[
                            ("track_count", ReleaseKey::TrackCount),
                            ("total_duration", ReleaseKey::TotalDuration),
                        ],
                        |release| release.db_id.clone().map(DbId::from),
                        |release| release,
                    )
                })
                .map_err(catalog::error)?;
        catalog::page_table(page)?.into_luau_return()
    }

    #[harmony(
        description = "The releases the caller can see that hold each of these tracks, keyed by track id.",
        returns(HashMap<String, Vec<Release>>)
    )]
    async fn by_track(
        track_ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(&db, track_ids.0, lookups::releases_by_track).await
    }

    #[harmony(
        description = "The releases the caller can see in each of these genres, keyed by genre id.",
        returns(HashMap<String, Vec<Release>>)
    )]
    async fn by_genre(
        genre_ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(&db, genre_ids.0, lookups::releases_by_genre).await
    }

    #[harmony(
        description = "The chains on each of these releases that the caller can see, ordered by where their first track sits, keyed by release id.",
        returns(HashMap<String, Vec<ReleaseChain>>)
    )]
    async fn chains(
        release_ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        catalog::keyed_table(&db, release_ids.0, |db, viewer, ids| {
            let mut chains = HashMap::new();
            for (release, release_chains) in lookups::chains_by_release(db, viewer, ids)? {
                chains.insert(release, chain_service::public_chains(db, release_chains)?);
            }
            Ok(chains)
        })
        .await
    }

    #[harmony(
        description = "Replaces every chain on a release the caller can see with `chains`, each the release's track ids in play order, and returns the stored chains. An empty list clears them. Errors when a chain has fewer than two tracks, a track is listed more than once, a track is not on the release, or a track is already in a chain on another release. Chain ids may change on every call. Requires ManageMetadata. Needs the `lyra.releases.manage` scope too.",
        returns(Vec<ReleaseChain>)
    )]
    async fn replace_chains(
        release_id: String,
        chains: ChainsArgument,
        #[context] principal: Principal,
        #[context] db: Db,
    ) -> luau::runtime::Result<luau::Value> {
        crate::services::auth::require_permission(&principal, lyra_db::Permission::ManageMetadata)
            .map_err(crate::plugins::errors::auth_error)?;
        let mut db = db.write().await;
        let release_db_id =
            ids::visible_to(&db, &principal, &release_id, "Release")?.ok_or_else(|| {
                crate::plugins::runtime_error(format!("release not found: {release_id}"))
            })?;
        let chains = chain_service::replace(&mut db, &principal, release_db_id, &chains.0)
            .map_err(crate::plugins::runtime_error)?;
        harmony_luau::serializable_to_luau_owned(chains)
    }

    #[harmony(
        description = "Returns provider-ranked releases related to a local release. Returns nil when the seed release does not exist or is inaccessible to the supplied user.",
        returns(Option<Vec<Release>>)
    )]
    fn similar(
        vm: &luau::Vm,
        context: &luau::CallContext,
        release_id: String,
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
        let request = parse_similar_options(opts.0)?;
        let principal = crate::plugins::auth::require_dispatch_principal(context)?;
        let db = crate::plugins::db::get(vm)?;
        let calls = crate::plugins::executor::VmCalls::of(vm, context)?;

        Ok(luau::ScheduledFuture::new(async move {
            let Some(release_db_id) = ids::db_id(&*db.read().await, &release_id)? else {
                return Ok(luau::Value::Nil);
            };
            let options =
                release_service::SimilarReleaseOptions::for_principal(&principal, request.limit);
            let Some(releases) =
                release_service::similar_on(&calls, release_db_id, &options).await?
            else {
                return Ok(luau::Value::Nil);
            };
            let availability = services::catalog::Availability::of(&*db.read().await)?;
            let releases = releases
                .into_iter()
                .map(|release| services::catalog::Listed {
                    available: release
                        .db_id
                        .clone()
                        .map(DbId::from)
                        .is_none_or(|id| availability.release(id)),
                    item: release,
                })
                .collect::<Vec<_>>();
            harmony_luau::serializable_to_luau_owned(releases)
        }))
    }
}

struct SimilarReleaseRequest {
    limit: usize,
}

fn parse_similar_options(
    opts: Option<luau::Table>,
) -> luau::runtime::Result<SimilarReleaseRequest> {
    let Some(opts) = opts else {
        return Ok(SimilarReleaseRequest {
            limit: release_service::DEFAULT_SIMILAR_RELEASE_LIMIT,
        });
    };
    let limit = parse_optional_similar_positive_integer(&opts, "limit")?
        .map(|value| usize::try_from(value).unwrap_or(usize::MAX))
        .unwrap_or(release_service::DEFAULT_SIMILAR_RELEASE_LIMIT);
    Ok(SimilarReleaseRequest {
        limit: limit.min(release_service::MAX_SIMILAR_RELEASE_LIMIT),
    })
}

fn parse_optional_similar_positive_integer(
    opts: &luau::Table,
    field: &str,
) -> luau::runtime::Result<Option<i64>> {
    let value = opts.optional_i64_field(field).map_err(|_| {
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
        field("id", String::luau_type()),
        FieldDescriptor {
            name: "available",
            ty: bool::luau_type(),
            description: Some("Whether any of the release's tracks can play."),
        },
        field("release_title", String::luau_type()),
        field("sort_title", Option::<String>::luau_type()),
        field(
            "release_type",
            LuauType::optional(LuauType::named("ReleaseType")),
        ),
        field("release_date", Option::<String>::luau_type()),
        field("description", Option::<String>::luau_type()),
        field("locked", Option::<bool>::luau_type()),
        field("created_at_ms", Option::<u64>::luau_type()),
        field("file_created_at_ms", Option::<u64>::luau_type()),
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
            "ReleaseSummary",
            LuauType::intersection(vec![
                LuauType::named("Release"),
                LuauType::object(vec![
                    described_field(
                        "track_count",
                        u64::luau_type(),
                        "The tracks on the release.",
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
        TypeAliasDescriptor::new(
            "ReleaseChain",
            LuauType::object(vec![
                field("id", String::luau_type()),
                described_field(
                    "track_ids",
                    Vec::<String>::luau_type(),
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
        "ReleaseSummary",
    ))
    .collect()
}

#[cfg(feature = "docgen")]
fn release_query() -> InterfaceDescriptor {
    let mut descriptor = InterfaceDescriptor::new("ReleaseQuery", None);
    descriptor.fields.extend([
        field("ids", Option::<Vec<String>>::luau_type()),
        field("exclude_ids", Option::<Vec<String>>::luau_type()),
        field("library_id", Option::<String>::luau_type()),
        field("genre_ids", Option::<Vec<String>>::luau_type()),
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
        "Maximum number of releases to return, capped to `MAX_SIMILAR`. Defaults to 20.",
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
        let defaults = parse_similar_options(Some(empty))?;
        assert_eq!(
            defaults.limit,
            release_service::DEFAULT_SIMILAR_RELEASE_LIMIT
        );

        let opts = vm.create_table()?;
        opts.set("limit", luau::Value::from(7_i64))?;
        let parsed = parse_similar_options(Some(opts))?;
        assert_eq!(parsed.limit, 7);

        let opts = vm.create_table()?;
        let past = release_service::MAX_SIMILAR_RELEASE_LIMIT as i64 + 1;
        opts.set("limit", luau::Value::from(past))?;
        let capped = parse_similar_options(Some(opts))?;
        assert_eq!(capped.limit, release_service::MAX_SIMILAR_RELEASE_LIMIT);
        Ok(())
    }

    #[test]
    fn similar_options_reject_invalid_integer_fields() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        let opts = vm.create_table()?;
        opts.set("limit", luau::Value::from(0_i64))?;
        assert!(parse_similar_options(Some(opts)).is_err());

        let opts = vm.create_table()?;
        opts.set("limit", luau::Value::Number(4.5))?;
        assert!(parse_similar_options(Some(opts)).is_err());
        Ok(())
    }
}
