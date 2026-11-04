// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The Luau side of catalog queries, shared by the catalog modules.

use std::collections::{
    BTreeMap,
    BTreeSet,
    HashMap,
};

use agdb::{
    DbAny,
    DbId,
};
use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::{
    FieldDescriptor,
    LuauType,
    LuauTypeInfo,
    TypeAliasDescriptor,
};
use lyra_db::ratings::{
    InvalidRatingFilter,
    RatingFilter,
    RatingValue,
};
use serde::Serialize;

use crate::{
    plugins::{
        args,
        db::ViewerDb,
        ids::Resolved,
        scopes::Scope,
    },
    services::{
        self,
        catalog::{
            ArtistCredit,
            CatalogError,
            CreditRole,
            Direction,
            NameRange,
            Page,
            PersonalData,
            PersonalFilter,
            Query,
            Viewer,
            artists::ArtistFilter,
            genres::GenreFilter,
            pipeline::{
                Catalog,
                SortKey,
            },
            releases::ReleaseFilter,
            tracks::TrackFilter,
        },
    },
};

pub(crate) fn error(error: CatalogError) -> luau::Error {
    match error {
        CatalogError::Auth(error) => crate::plugins::errors::auth_error(error),
        error => crate::plugins::runtime_error(error),
    }
}

/// The personal data whose scopes the calling plugin declares. Filtering or
/// sorting by a viewer's bookmarks, favorites, listens or ratings reads them, so a query
/// needs the scope that gates them, as if it used that module.
pub(crate) struct PersonalScopes(BTreeSet<PersonalData>);

impl luau::FromCallFrame for PersonalScopes {
    fn from_call_frame(vm: &luau::Vm, context: &luau::CallContext) -> luau::runtime::Result<Self> {
        let require = vm.data().get::<harmony_core::luau::RequireRuntime>()?;
        Ok(Self(
            [
                PersonalData::Bookmarks,
                PersonalData::Favorites,
                PersonalData::Listens,
                PersonalData::Ratings,
            ]
            .into_iter()
            .filter(|data| require.allows(&context.origin, scope_of(*data).def().id))
            .collect(),
        ))
    }
}

impl PersonalScopes {
    /// Raises when `query` reads personal data whose scope the caller lacks.
    pub(crate) fn check<C: Catalog>(&self, query: &Query<C>) -> luau::runtime::Result<()> {
        query
            .personal()
            .iter()
            .try_for_each(|data| self.require(*data))
    }

    /// Raises when the caller lacks the scope that gates `data`.
    pub(crate) fn require(&self, data: PersonalData) -> luau::runtime::Result<()> {
        if self.0.contains(&data) {
            return Ok(());
        }
        Err(crate::plugins::runtime_error(format!(
            "filtering or sorting by the caller's {} requires the {} scope",
            match data {
                PersonalData::Bookmarks => "bookmarks",
                PersonalData::Favorites => "favorites",
                PersonalData::Listens => "listens",
                PersonalData::Ratings => "ratings",
            },
            scope_of(data).def().id,
        )))
    }
}

fn scope_of(data: PersonalData) -> Scope {
    match data {
        PersonalData::Bookmarks => Scope::Bookmarks,
        PersonalData::Favorites => Scope::Favorites,
        PersonalData::Listens => Scope::Listens,
        PersonalData::Ratings => Scope::Ratings,
    }
}

/// A query read from the fields every catalog shares, with the page bounds.
pub(crate) struct PagedQuery<C: Catalog> {
    pub(crate) query: Query<C>,
    pub(crate) offset: u64,
    pub(crate) limit: Option<u64>,
    ids: QueryIds,
}

impl<C: Catalog> PagedQuery<C>
where
    C::Filter: IdFilter,
{
    /// Sets the filter's ids, resolved under the guard the query reads through.
    pub(crate) fn resolve_ids(&mut self, db: &DbAny) -> luau::runtime::Result<()> {
        let ids = std::mem::take(&mut self.ids).resolve(db)?;
        self.query.filter.apply(ids);
        Ok(())
    }
}

/// A public-id field a catalog query's filter takes.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdKey {
    Ids,
    ExcludeIds,
    ExcludeArtists,
    Library,
    Releases,
    Tracks,
    Genres,
    ArtistCredit,
}

/// A catalog filter that takes public ids, set once they are resolved.
pub(crate) trait IdFilter {
    const KEYS: &'static [IdKey];

    fn apply(&mut self, ids: ResolvedIds);
}

/// The ids a query's filter names, as read with the query.
#[derive(Default)]
struct QueryIds {
    ids: Option<Vec<String>>,
    exclude_ids: Vec<String>,
    exclude_artists: Vec<String>,
    library: Option<String>,
    releases: Option<Vec<String>>,
    tracks: Option<Vec<String>>,
    genres: Vec<String>,
    artists: Option<CreditIds>,
}

/// A query's filter ids, resolved. When a filter's ids all name no node, `ids` is empty, so the
/// query matches nothing rather than dropping the filter.
#[derive(Default)]
pub(crate) struct ResolvedIds {
    pub(crate) ids: Option<Vec<DbId>>,
    pub(crate) exclude_ids: Vec<DbId>,
    pub(crate) exclude_artists: Vec<DbId>,
    pub(crate) library: Option<DbId>,
    pub(crate) releases: Option<Vec<DbId>>,
    pub(crate) tracks: Option<Vec<DbId>>,
    pub(crate) genres: Vec<DbId>,
    pub(crate) artists: Option<ArtistCredit>,
}

impl QueryIds {
    fn read(table: &luau::Table, keys: &[IdKey]) -> luau::runtime::Result<Self> {
        let takes = |key| keys.contains(&key);
        let mut ids = Self::default();
        if takes(IdKey::Ids) {
            ids.ids = optional_ids(table, "ids")?;
        }
        if takes(IdKey::ExcludeIds) {
            ids.exclude_ids = args::optional_unique_ids(table, "exclude_ids")?;
        }
        if takes(IdKey::ExcludeArtists) {
            ids.exclude_artists = args::optional_unique_ids(table, "exclude_artist_ids")?;
        }
        if takes(IdKey::Library) {
            ids.library = table.optional_string_field("library_id")?;
        }
        if takes(IdKey::Releases) {
            ids.releases = optional_ids(table, "release_ids")?;
        }
        if takes(IdKey::Tracks) {
            ids.tracks = optional_ids(table, "track_ids")?;
        }
        if takes(IdKey::Genres) {
            ids.genres = args::optional_unique_ids(table, "genre_ids")?;
        }
        if takes(IdKey::ArtistCredit) {
            ids.artists = read_artist_credit(table)?;
        }
        Ok(ids)
    }

    fn resolve(self, db: &DbAny) -> luau::runtime::Result<ResolvedIds> {
        let mut ids = FilterIds::new(db);
        let mut resolved = ResolvedIds {
            ids: ids.optional(self.ids)?,
            exclude_ids: ids.excluded(self.exclude_ids)?,
            exclude_artists: ids.excluded(self.exclude_artists)?,
            library: ids.one(self.library)?,
            releases: ids.optional(self.releases)?,
            tracks: ids.optional(self.tracks)?,
            genres: ids.among(self.genres)?,
            artists: self
                .artists
                .map(|credit| credit.resolve(&mut ids))
                .transpose()?,
        };
        if ids.matches_nothing() {
            resolved.ids = Some(Vec::new());
        }
        Ok(resolved)
    }
}

pub(crate) fn read_query<C: Catalog>(
    table: &luau::Table,
    filter: C::Filter,
) -> luau::runtime::Result<PagedQuery<C>>
where
    C::Filter: IdFilter,
{
    let mut query = Query::new(filter);
    query.names = NameRange::new(
        table.optional_string_field("sort_name_prefix")?,
        table.optional_string_field("sort_name_at_least")?,
        table.optional_string_field("sort_name_below")?,
    );
    query.name = table.optional_string_field("name")?;
    query.search = table.optional_string_field("search")?;
    query.sort = read_sort(table)?;
    query.seed = args::optional_u64(table, "seed")?.unwrap_or(0);
    Ok(PagedQuery {
        query,
        offset: args::optional_u64(table, "offset")?.unwrap_or(0),
        limit: args::optional_u64(table, "limit")?,
        ids: QueryIds::read(table, C::Filter::KEYS)?,
    })
}

fn read_sort<K: SortKey>(table: &luau::Table) -> luau::runtime::Result<Vec<(K, Direction)>> {
    let Some(terms) = table.optional_table_field("sort")? else {
        return Ok(Vec::new());
    };
    args::array_values(&terms)?
        .into_iter()
        .map(|(index, term)| {
            let luau::Value::Table(term) = term else {
                return Err(crate::plugins::runtime_error(format!(
                    "sort entry {index} must be a table with a key, got {}",
                    term.type_name()
                )));
            };
            let key = K::parse(&term.required_string_field("key")?).map_err(error)?;
            let direction = term
                .optional_string_field("order")?
                .as_deref()
                .map(Direction::parse)
                .transpose()
                .map_err(error)?
                .unwrap_or(Direction::Ascending);
            Ok((key, direction))
        })
        .collect()
}

/// The ids of the optional array field `key`, or `None` when absent.
fn optional_ids(table: &luau::Table, key: &str) -> luau::runtime::Result<Option<Vec<String>>> {
    table
        .optional_table_field(key)?
        .map(|ids| args::unique_ids(&ids))
        .transpose()
}

pub(crate) fn read_years(table: &luau::Table) -> luau::runtime::Result<Vec<u32>> {
    Ok(table
        .optional_table_field("years")?
        .map(|years| args::integers(&years))
        .transpose()?
        .unwrap_or_default())
}

/// The caller's personal filter from `favorite`, `listened`, `rated`, `min_rating` and
/// `max_rating`.
pub(crate) fn read_personal(table: &luau::Table) -> luau::runtime::Result<PersonalFilter> {
    let bound = |key| {
        table
            .optional_i64_field(key)?
            .map(|value| {
                u8::try_from(value)
                    .ok()
                    .and_then(RatingValue::new)
                    .ok_or_else(|| {
                        crate::plugins::runtime_error(format!(
                            "{key} must be an integer from {} to {}, got {value}",
                            lyra_db::ratings::MIN_VALUE,
                            lyra_db::ratings::MAX_VALUE,
                        ))
                    })
            })
            .transpose()
    };
    let rating = RatingFilter::new(
        table.optional_bool_field("rated")?,
        bound("min_rating")?,
        bound("max_rating")?,
    )
    .map_err(|err| crate::plugins::runtime_error(rating_filter_message(err)))?;
    Ok(PersonalFilter {
        favorite: table.optional_bool_field("favorite")?,
        listened: table.optional_bool_field("listened")?,
        rating,
    })
}

fn rating_filter_message(err: InvalidRatingFilter) -> &'static str {
    match err {
        InvalidRatingFilter::Inverted => "min_rating must not exceed max_rating",
        InvalidRatingFilter::BoundsOnUnrated => {
            "min_rating and max_rating cannot be combined with rated = false"
        }
    }
}

/// The `artist_ids` filter with its `credit_role` and `exclude_credit_role`, its ids resolved
/// with the rest of the filter's.
struct CreditIds {
    artists: Vec<String>,
    role: CreditRole,
    excluding: Option<CreditRole>,
}

impl CreditIds {
    fn resolve(self, ids: &mut FilterIds<'_>) -> luau::runtime::Result<ArtistCredit> {
        Ok(ArtistCredit {
            artists: ids.among(self.artists)?,
            role: self.role,
            excluding: self.excluding,
        })
    }
}

fn read_artist_credit(table: &luau::Table) -> luau::runtime::Result<Option<CreditIds>> {
    let role = |key| {
        table
            .optional_string_field(key)?
            .as_deref()
            .map(CreditRole::parse)
            .transpose()
            .map_err(error)
    };
    let role_filter = role("credit_role")?;
    let excluding = role("exclude_credit_role")?;
    let Some(artists) = table.optional_table_field("artist_ids")? else {
        if role_filter.is_some() || excluding.is_some() {
            return Err(crate::plugins::runtime_error(
                "credit_role and exclude_credit_role need artist_ids",
            ));
        }
        return Ok(None);
    };
    Ok(Some(CreditIds {
        artists: args::unique_ids(&artists)?,
        role: role_filter.unwrap_or(CreditRole::Any),
        excluding,
    }))
}

/// Resolves the public ids of a query's filter under the guard the query reads through. Ids
/// that name no node narrow the filter they are in, so a filter left without any matches
/// nothing rather than being dropped.
struct FilterIds<'a> {
    db: &'a DbAny,
    matches_nothing: bool,
}

impl<'a> FilterIds<'a> {
    fn new(db: &'a DbAny) -> Self {
        Self {
            db,
            matches_nothing: false,
        }
    }

    /// Ids a result must be among, or be linked to one of.
    fn among(&mut self, ids: Vec<String>) -> luau::runtime::Result<Vec<DbId>> {
        let given = !ids.is_empty();
        let resolved = Resolved::of(self.db, ids)?.db_ids().to_vec();
        self.matches_nothing |= given && resolved.is_empty();
        Ok(resolved)
    }

    /// [`FilterIds::among`] for an optional filter.
    fn optional(&mut self, ids: Option<Vec<String>>) -> luau::runtime::Result<Option<Vec<DbId>>> {
        ids.map(|ids| self.among(ids)).transpose()
    }

    /// Ids results must avoid; ids naming no node are dropped.
    fn excluded(&mut self, ids: Vec<String>) -> luau::runtime::Result<Vec<DbId>> {
        Ok(Resolved::of(self.db, ids)?.db_ids().to_vec())
    }

    /// The one id a result must be linked to.
    fn one(&mut self, id: Option<String>) -> luau::runtime::Result<Option<DbId>> {
        let Some(id) = id else {
            return Ok(None);
        };
        let resolved = crate::plugins::ids::db_id(self.db, &id)?;
        self.matches_nothing |= resolved.is_none();
        Ok(resolved)
    }

    /// Whether a filter's ids all named no node, so the query matches nothing.
    fn matches_nothing(&self) -> bool {
        self.matches_nothing
    }
}

/// A catalog lookup of the entities with these ids that the viewer can see, keyed by id.
pub(crate) type KeyedLookup<T> =
    fn(&DbAny, &Viewer, &[DbId]) -> Result<HashMap<DbId, T>, CatalogError>;

/// The entities with these ids that the viewer can see, keyed by id.
pub(crate) async fn keyed_table<T: Serialize + 'static>(
    db: &ViewerDb,
    ids: Vec<String>,
    lookup: KeyedLookup<T>,
) -> luau::runtime::Result<luau::OwnedTable> {
    let (db, viewer) = db.read().await?;
    let ids = Resolved::of(&*db, ids)?;
    let found = lookup(&db, &viewer, ids.db_ids()).map_err(error)?;
    let mut values = Vec::with_capacity(found.len());
    for (id, value) in found {
        values.push((id, harmony_luau::serializable_to_luau_owned(value)?));
    }
    Ok(ids.keyed(values))
}

/// A page item with counts the catalog computed for it, flattened beside its fields.
#[derive(Serialize)]
pub(crate) struct Counted<T> {
    #[serde(flatten)]
    item: T,
    #[serde(flatten)]
    counts: BTreeMap<&'static str, u64>,
}

/// The page's items with the values of `counts` for each, named as given. The values are the
/// ones the catalog sorts by, so they follow the viewer and the query's filter.
pub(crate) fn counted<C: Catalog, T>(
    db: &DbAny,
    viewer: &Viewer,
    query: &Query<C>,
    page: Page<C::Item>,
    counts: &[(&'static str, C::Key)],
    id_of: impl Fn(&C::Item) -> Option<DbId>,
    into: impl Fn(C::Item) -> T,
) -> Result<Page<Counted<T>>, CatalogError> {
    let ids = page.items.iter().filter_map(&id_of).collect::<Vec<_>>();
    let keys = counts.iter().map(|(_, key)| *key).collect::<Vec<_>>();
    let mut values = services::catalog::counts::<C>(db, viewer, &query.filter, ids, &keys)?;
    let items = page
        .items
        .into_iter()
        .map(|item| {
            let item_values = id_of(&item)
                .and_then(|id| values.remove(&id))
                .unwrap_or_default();
            Counted {
                counts: counts
                    .iter()
                    .enumerate()
                    .map(|(index, (name, _))| (*name, item_values.get(index).copied().unwrap_or(0)))
                    .collect(),
                item: into(item),
            }
        })
        .collect();
    Ok(Page {
        items,
        total: page.total,
        offset: page.offset,
    })
}

pub(crate) fn page_table<T: Serialize>(page: Page<T>) -> luau::runtime::Result<luau::OwnedTable> {
    let mut table = luau::OwnedTable::with_capacity(0, 3);
    table.set_field(
        "items",
        harmony_luau::serializable_to_luau_owned(page.items)?,
    );
    table.set_field("total", page.total as i64);
    table.set_field("offset", page.offset as i64);
    Ok(table)
}

#[cfg(feature = "docgen")]
fn field(name: &'static str, ty: LuauType) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description: None,
    }
}

/// The query fields every catalog shares, for its query interface.
#[cfg(feature = "docgen")]
pub(crate) fn query_fields(sort_key: &'static str) -> Vec<FieldDescriptor> {
    vec![
        field("sort_name_prefix", Option::<String>::luau_type()),
        field("sort_name_at_least", Option::<String>::luau_type()),
        field("sort_name_below", Option::<String>::luau_type()),
        FieldDescriptor {
            name: "name",
            ty: Option::<String>::luau_type(),
            description: Some(
                "Keeps only items whose name is this one, compared as `search` compares an exact name: ignoring case, accents, apostrophes, punctuation and spaces, so `BJÖRK` finds `Björk` and `ac dc` finds `AC/DC`. Sort names and aliases don't count.",
            ),
        },
        FieldDescriptor {
            name: "search",
            ty: Option::<String>::luau_type(),
            description: Some(
                "Each word must start a word of the item's name or sort name, ignoring case, accents, apostrophes and punctuation, so `ac dc` finds `AC/DC`; no character is an operator. A word in a script written without spaces, such as Japanese, matches anywhere in a word. Tracks also match through their visible releases and those releases' and their own artists, releases through their artists, and artists and genres through their aliases. Without `sort`, results rank exact names first, then names starting with the search, then names holding every word, then names holding some, then matches only through related items.",
            ),
        },
        field(
            "sort",
            LuauType::optional(LuauType::array(LuauType::object(vec![
                field("key", LuauType::named(sort_key)),
                field("order", LuauType::optional(args::sort_order_type())),
            ]))),
        ),
        field("seed", Option::<u64>::luau_type()),
        field("offset", Option::<u64>::luau_type()),
        field("limit", Option::<u64>::luau_type()),
    ]
}

#[cfg(feature = "docgen")]
pub(crate) fn rating_fields() -> Vec<FieldDescriptor> {
    vec![
        FieldDescriptor {
            name: "rated",
            ty: Option::<bool>::luau_type(),
            description: Some(
                "Keeps only items the caller has rated (`true`) or has not rated (`false`). `false` cannot be combined with `min_rating` or `max_rating`.",
            ),
        },
        FieldDescriptor {
            name: "min_rating",
            ty: Option::<u8>::luau_type(),
            description: Some(
                "Inclusive lower bound on the caller's 1-5 rating. Either bound leaves out unrated items.",
            ),
        },
        FieldDescriptor {
            name: "max_rating",
            ty: Option::<u8>::luau_type(),
            description: Some("Inclusive upper bound on the caller's 1-5 rating."),
        },
    ]
}

#[cfg(feature = "docgen")]
pub(crate) fn credit_fields() -> Vec<FieldDescriptor> {
    let role = LuauType::optional(LuauType::named("CreditRole"));
    vec![
        field("artist_ids", Option::<Vec<String>>::luau_type()),
        field("credit_role", role.clone()),
        field("exclude_credit_role", role),
        FieldDescriptor {
            name: "exclude_artist_ids",
            ty: Option::<Vec<String>>::luau_type(),
            description: Some("Leaves out items credited to these artists in either role."),
        },
    ]
}

/// The `CreditRole` type alias, for a catalog module with credit filters.
#[cfg(feature = "docgen")]
pub(crate) fn credit_role_alias() -> TypeAliasDescriptor {
    TypeAliasDescriptor::new(
        "CreditRole",
        LuauType::union(
            CreditRole::KEYS
                .iter()
                .map(|(token, _)| LuauType::string_literal(token))
                .collect(),
        ),
        None,
    )
}

/// The `<Sort>Key` and `<Page>` type aliases for a catalog module.
#[cfg(feature = "docgen")]
pub(crate) fn type_aliases<K: SortKey>(
    sort_key: &'static str,
    page: &'static str,
    item: &'static str,
) -> Vec<TypeAliasDescriptor> {
    vec![
        TypeAliasDescriptor::new(
            sort_key,
            LuauType::union(
                K::KEYS
                    .iter()
                    .map(|(token, _)| LuauType::string_literal(token))
                    .collect(),
            ),
            None,
        ),
        TypeAliasDescriptor::new(
            page,
            LuauType::object(vec![
                field("items", LuauType::array(LuauType::named(item))),
                field("total", u64::luau_type()),
                field("offset", u64::luau_type()),
            ]),
            None,
        ),
    ]
}

impl IdFilter for TrackFilter {
    const KEYS: &'static [IdKey] = &[
        IdKey::Ids,
        IdKey::ExcludeIds,
        IdKey::ExcludeArtists,
        IdKey::Library,
        IdKey::Releases,
        IdKey::Genres,
        IdKey::ArtistCredit,
    ];

    fn apply(&mut self, ids: ResolvedIds) {
        self.ids = ids.ids;
        self.exclude_ids = ids.exclude_ids;
        self.exclude_artists = ids.exclude_artists;
        self.library = ids.library;
        self.releases = ids.releases;
        self.genres = ids.genres;
        self.artists = ids.artists;
    }
}

impl IdFilter for ReleaseFilter {
    const KEYS: &'static [IdKey] = &[
        IdKey::Ids,
        IdKey::ExcludeIds,
        IdKey::ExcludeArtists,
        IdKey::Library,
        IdKey::Genres,
        IdKey::ArtistCredit,
    ];

    fn apply(&mut self, ids: ResolvedIds) {
        self.ids = ids.ids;
        self.exclude_ids = ids.exclude_ids;
        self.exclude_artists = ids.exclude_artists;
        self.library = ids.library;
        self.genres = ids.genres;
        self.artists = ids.artists;
    }
}

impl IdFilter for ArtistFilter {
    const KEYS: &'static [IdKey] = &[
        IdKey::Ids,
        IdKey::ExcludeIds,
        IdKey::Library,
        IdKey::Releases,
        IdKey::Tracks,
        IdKey::Genres,
    ];

    fn apply(&mut self, ids: ResolvedIds) {
        self.ids = ids.ids;
        self.exclude_ids = ids.exclude_ids;
        self.library = ids.library;
        self.releases = ids.releases;
        self.tracks = ids.tracks;
        self.genres = ids.genres;
    }
}

impl IdFilter for GenreFilter {
    const KEYS: &'static [IdKey] = &[
        IdKey::Ids,
        IdKey::ExcludeIds,
        IdKey::Library,
        IdKey::Releases,
    ];

    fn apply(&mut self, ids: ResolvedIds) {
        self.ids = ids.ids;
        self.exclude_ids = ids.exclude_ids;
        self.library = ids.library;
        self.releases = ids.releases;
    }
}
