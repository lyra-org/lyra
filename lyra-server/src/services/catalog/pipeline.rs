// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The one filter, search, sort and page implementation for catalog queries, over the catalog
//! [`index`](super::index).

use std::{
    cmp::Ordering,
    collections::{
        BTreeSet,
        HashMap,
        HashSet,
    },
    sync::Arc,
};

use agdb::{
    DbAny,
    DbId,
};

use lyra_db::{
    favorites::FavoriteKind,
    ratings::{
        RatingBounds,
        RatingFilter,
    },
};

use super::{
    CatalogError,
    Direction,
    Page,
    SortSpec,
    Viewer,
    index::{
        Cache,
        CacheKey,
        Hit,
        MISSING,
        Scope,
        Scoped,
        Snapshot,
        Table,
        bits::Bits,
    },
};
use crate::services::search::{
    Needle,
    Tier,
    Word,
};

/// What a sort key orders by.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyKind {
    /// The search match score, best first.
    Relevance,
    /// A stable shuffle driven by the query's seed.
    Random,
    Id,
    /// A value the catalog computes for each candidate.
    Field,
}

pub(crate) trait SortKey: Copy + Eq + std::fmt::Debug + 'static {
    /// Every key and the token that names it.
    const KEYS: &'static [(&'static str, Self)];
    /// The key ranking search matches, and the default order when searching.
    const RELEVANCE: Self;
    /// The key whose ascending order is the order ties break in.
    const NAME_ORDER: Self;

    fn kind(self) -> KeyKind;

    /// The viewer's personal data the key orders by, if any.
    fn personal(self) -> Option<PersonalData> {
        None
    }

    /// Whether an entity without a value sorts ahead of every value, in both directions.
    /// Otherwise it sorts after them.
    fn missing_first(self) -> bool {
        false
    }

    fn parse(token: &str) -> Result<Self, CatalogError> {
        Self::KEYS
            .iter()
            .find(|(name, _)| *name == token)
            .map(|(_, key)| *key)
            .ok_or_else(|| {
                let supported = Self::KEYS
                    .iter()
                    .map(|(name, _)| *name)
                    .collect::<Vec<_>>()
                    .join(", ");
                CatalogError::Invalid(format!(
                    "unsupported sort key: {token}. Supported keys: {supported}"
                ))
            })
    }

    /// Whether the key's values are places in a name order, which mean nothing outside it.
    fn ranked(self) -> bool {
        false
    }

    /// The key's place in [`Self::KEYS`].
    fn position(self) -> usize {
        Self::KEYS
            .iter()
            .position(|(_, key)| *key == self)
            .expect("every key is listed")
    }
}

/// Data of the viewer's own that a query can filter or sort by.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum PersonalData {
    Bookmarks,
    Favorites,
    Listens,
    Ratings,
}

/// The viewer's own favorites, listens and ratings a query keeps.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct PersonalFilter {
    pub(crate) favorite: Option<bool>,
    pub(crate) listened: Option<bool>,
    pub(crate) rating: RatingFilter,
}

impl PersonalFilter {
    /// The viewer's personal data the filter reads.
    pub(crate) fn reads(self) -> Vec<PersonalData> {
        [
            self.favorite.map(|_| PersonalData::Favorites),
            self.listened.map(|_| PersonalData::Listens),
            (!self.rating.is_any()).then_some(PersonalData::Ratings),
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    /// Narrows `slots` to what the filter keeps, where `None` stands for every entity of `table`
    /// and leaving entities out starts from `all`. `listened_by` gives the slots that count as
    /// listened, given the tracks the viewer listened to.
    pub(crate) fn restrict(
        self,
        cx: &Context<'_>,
        table: &Table,
        slots: &mut Option<Bits>,
        all: &Bits,
        kind: FavoriteKind,
        listened_by: impl Fn(&HashSet<DbId>) -> Bits,
    ) -> Result<(), CatalogError> {
        let mut restrict = |members: Bits, wanted: bool| match (slots.as_mut(), wanted) {
            (Some(slots), true) => slots.and(&members),
            (None, true) => *slots = Some(members),
            (Some(slots), false) => slots.and_not(&members),
            (None, false) => {
                let mut remaining = all.clone();
                remaining.and_not(&members);
                *slots = Some(remaining);
            }
        };
        if let Some(favorite) = self.favorite {
            let user = cx.viewer.user_db_id("the favorite filter")?;
            let favorites = lyra_db::favorites::target_ids(cx.db, user, kind)?;
            restrict(table.slots(&favorites), favorite);
        }
        if let Some(listened) = self.listened {
            let user = cx.viewer.user_db_id("the listened filter")?;
            let tracks = lyra_db::listens::listened_track_ids(cx.db, user)?;
            restrict(listened_by(&tracks), listened);
        }
        let rated = |bounds| -> Result<Bits, CatalogError> {
            let user = cx.viewer.user_db_id("the rating filter")?;
            Ok(table.slots(&lyra_db::ratings::rated_target_ids(cx.db, user, bounds)?))
        };
        match self.rating {
            RatingFilter::Any => {}
            RatingFilter::Rated(bounds) => restrict(rated(bounds)?, true),
            RatingFilter::Unrated => restrict(rated(RatingBounds::default())?, false),
        }
        Ok(())
    }
}

/// What a query reads: the index snapshot its guard sees, the database for the viewer's own
/// data, and what the viewer may see.
pub(crate) struct Context<'a> {
    pub(crate) index: Arc<Snapshot>,
    pub(crate) db: &'a DbAny,
    pub(crate) viewer: &'a Viewer,
    pub(crate) scope: Arc<Scope>,
}

impl<'a> Context<'a> {
    /// The links between kinds, as far as the viewer can see them.
    pub(crate) fn links(&self) -> Scoped<'_> {
        Scoped::new(&self.index, &self.scope)
    }

    /// The column of `key` for `table` read through other entities, kept where
    /// [`Self::cache_for`] says.
    pub(crate) fn linked_column<'b, K: SortKey>(
        &'b self,
        table: &'b Table,
        key: K,
        compute: impl FnOnce() -> Vec<u64>,
    ) -> Column<'b> {
        let cache = self.cache_for(table);
        let column = Column::stored(cache, key, compute);
        self.grown(cache);
        column
    }

    /// Keeps the retained scopes within budget after `cache` may have grown.
    pub(crate) fn grown(&self, cache: &Cache) {
        if std::ptr::eq(cache, &self.scope.cache) {
            self.index.trim_scopes(&self.scope);
        }
    }

    /// Where to keep values of `table` read through other entities: they are the same for
    /// everyone who sees every library, and depend on the libraries otherwise.
    pub(crate) fn cache_for<'b>(&'b self, table: &'b Table) -> &'b Cache {
        if self.scope.restricted {
            &self.scope.cache
        } else {
            &table.cache
        }
    }

    pub(crate) fn new(db: &'a DbAny, viewer: &'a Viewer) -> Result<Self, CatalogError> {
        Ok(Self::with(super::index::snapshot(db)?, db, viewer))
    }

    /// A context reading `index`, which must be a snapshot of `db`.
    pub(crate) fn with(index: Arc<Snapshot>, db: &'a DbAny, viewer: &'a Viewer) -> Self {
        let scope = index.scope(
            viewer
                .restricted()
                .map(|principal| &principal.accessible_library_ids),
        );
        Self {
            index,
            db,
            viewer,
            scope,
        }
    }
}

/// Where a key's column and orders are kept.
fn cache_key<K: SortKey>(key: K) -> CacheKey {
    (std::any::TypeId::of::<K>(), key.position())
}

/// The values of a sort key by slot, [`MISSING`] where an entity has none.
pub(crate) enum Column<'a> {
    /// Values kept in a cache, which orders by them are kept in too.
    Stored(Arc<[u64]>, &'a Cache),
    Fields(&'a [u64]),
    /// Values computed for the query's own slots.
    Computed(Vec<u64>),
}

impl<'a> Column<'a> {
    pub(crate) fn get(&self, slot: u32) -> Option<u64> {
        let value = match self {
            Self::Stored(values, _) => values[slot as usize],
            Self::Fields(values) => values[slot as usize],
            Self::Computed(values) => values[slot as usize],
        };
        (value != MISSING).then_some(value)
    }

    /// Values kept in `cache` under `key`, computed on first use.
    pub(crate) fn stored<K: SortKey>(
        cache: &'a Cache,
        key: K,
        compute: impl FnOnce() -> Vec<u64>,
    ) -> Self {
        Self::Stored(cache.column(cache_key(key), compute), cache)
    }

    /// A column over `len` slots with `value` for each of `slots`.
    pub(crate) fn computed(
        len: usize,
        slots: &[u32],
        mut value: impl FnMut(u32) -> Option<u64>,
    ) -> Self {
        let mut values = vec![MISSING; len];
        for slot in slots {
            values[*slot as usize] = value(*slot).unwrap_or(MISSING);
        }
        Self::Computed(values)
    }
}

pub(crate) trait Catalog {
    type Key: SortKey;
    type Filter;
    type Item;

    /// The viewer's personal data the filter reads.
    fn personal(_filter: &Self::Filter) -> Vec<PersonalData> {
        Vec::new()
    }

    /// The filter that keeps just these ids.
    fn id_filter(ids: Vec<DbId>) -> Self::Filter;

    /// The order used when a query names no keys and has no search term.
    fn default_sort(filter: &Self::Filter) -> SortSpec<Self::Key>;

    fn table(index: &Snapshot) -> &Table;

    /// The slots the viewer can see that pass the filter.
    fn candidates(cx: &Context<'_>, filter: &Self::Filter) -> Result<Bits, CatalogError>;

    /// The values of a field key for at least `slots`.
    fn column<'a>(
        cx: &'a Context<'_>,
        filter: &Self::Filter,
        key: Self::Key,
        slots: &[u32],
    ) -> Result<Column<'a>, CatalogError>;

    /// The slots whose related texts, such as their artists' names, match a search word. Only
    /// the texts of entities the viewer can see may match.
    fn related(_cx: &Context<'_>, _word: &Word) -> Option<Bits> {
        None
    }

    /// The entities with these ids, in this order.
    fn hydrate(db: &DbAny, ids: &[DbId]) -> anyhow::Result<Vec<Self::Item>>;

    /// The shuffle unit of each of `ids` that the random key moves with others. Every other id
    /// shuffles alone.
    fn shuffle_units(
        _cx: &Context<'_>,
        _ids: &[DbId],
    ) -> anyhow::Result<HashMap<DbId, ShuffleUnit>> {
        Ok(HashMap::new())
    }
}

/// What the random key moves an entry with, and the entry's place among the unit's entries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ShuffleUnit {
    pub(crate) id: DbId,
    pub(crate) place: u64,
}

/// Bounds on the lowercase sort name.
#[derive(Clone, Debug, Default)]
pub(crate) struct NameRange {
    prefix: Option<String>,
    at_least: Option<String>,
    below: Option<String>,
}

impl NameRange {
    pub(crate) fn new(
        prefix: Option<String>,
        at_least: Option<String>,
        below: Option<String>,
    ) -> Self {
        let lower = |bound: Option<String>| bound.map(|bound| bound.to_lowercase());
        Self {
            prefix: lower(prefix),
            at_least: lower(at_least),
            below: lower(below),
        }
    }

    fn is_unbounded(&self) -> bool {
        self.prefix.is_none() && self.at_least.is_none() && self.below.is_none()
    }

    pub(crate) fn contains(&self, sort_name: &str) -> bool {
        self.prefix
            .as_deref()
            .is_none_or(|prefix| sort_name.starts_with(prefix))
            && self
                .at_least
                .as_deref()
                .is_none_or(|bound| sort_name >= bound)
            && self.below.as_deref().is_none_or(|bound| sort_name < bound)
    }
}

pub(crate) struct Query<C: Catalog> {
    pub(crate) filter: C::Filter,
    pub(crate) names: NameRange,
    /// Keeps the items whose name is this one, as an exact search match compares names.
    pub(crate) name: Option<String>,
    pub(crate) search: Option<String>,
    /// Empty means relevance when searching, and the catalog's default otherwise.
    pub(crate) sort: SortSpec<C::Key>,
    pub(crate) seed: u64,
}

impl<C: Catalog> Query<C> {
    pub(crate) fn new(filter: C::Filter) -> Self {
        Self {
            filter,
            names: NameRange::default(),
            name: None,
            search: None,
            sort: Vec::new(),
            seed: 0,
        }
    }

    /// The viewer's personal data the query reads to filter or sort.
    pub(crate) fn personal(&self) -> BTreeSet<PersonalData> {
        C::personal(&self.filter)
            .into_iter()
            .chain(self.sort.iter().filter_map(|(key, _)| key.personal()))
            .collect()
    }
}

/// Every matching id in order, for cursor snapshots.
pub(crate) fn order<C: Catalog>(
    db: &DbAny,
    viewer: &Viewer,
    query: &Query<C>,
) -> Result<Vec<DbId>, CatalogError> {
    leading(db, viewer, query, None)
}

/// The first `keep` matching ids in order, or every one without a `keep`.
pub(crate) fn leading<C: Catalog>(
    db: &DbAny,
    viewer: &Viewer,
    query: &Query<C>,
    keep: Option<usize>,
) -> Result<Vec<DbId>, CatalogError> {
    leading_in(&Context::new(db, viewer)?, query, keep)
}

/// [`leading`] in an existing context.
pub(crate) fn leading_in<C: Catalog>(
    cx: &Context<'_>,
    query: &Query<C>,
    keep: Option<usize>,
) -> Result<Vec<DbId>, CatalogError> {
    let (slots, _) = sorted(cx, query, keep)?;
    let table = C::table(&cx.index);
    Ok(slots.into_iter().map(|slot| table.id(slot)).collect())
}

/// The numeric values of `keys` for each of `ids`, as the catalog computes them to sort by;
/// a key with no value reads as zero.
pub(crate) fn counts<C: Catalog>(
    db: &DbAny,
    viewer: &Viewer,
    filter: &C::Filter,
    ids: Vec<DbId>,
    keys: &[C::Key],
) -> Result<HashMap<DbId, Vec<u64>>, CatalogError> {
    Ok(numbers::<C>(db, viewer, filter, ids, keys)?
        .into_iter()
        .map(|(id, values)| {
            (
                id,
                values.into_iter().map(Option::unwrap_or_default).collect(),
            )
        })
        .collect())
}

/// [`counts`] with a key that has no value for an item, such as a last play that never
/// happened, left as `None`. Only the ids the viewer can see come back, and only keys with
/// numeric values, such as counts and times, have any: a name key reads as `None`.
pub(crate) fn numbers<C: Catalog>(
    db: &DbAny,
    viewer: &Viewer,
    filter: &C::Filter,
    ids: Vec<DbId>,
    keys: &[C::Key],
) -> Result<HashMap<DbId, Vec<Option<u64>>>, CatalogError> {
    numbers_in::<C>(&Context::new(db, viewer)?, filter, ids, keys)
}

/// [`numbers`] in an existing context.
pub(crate) fn numbers_in<C: Catalog>(
    cx: &Context<'_>,
    filter: &C::Filter,
    ids: Vec<DbId>,
    keys: &[C::Key],
) -> Result<HashMap<DbId, Vec<Option<u64>>>, CatalogError> {
    let table = C::table(&cx.index);
    let slots = C::candidates(cx, &C::id_filter(ids))?
        .iter()
        .collect::<Vec<_>>();
    let columns = keys
        .iter()
        .map(|key| {
            (!key.ranked())
                .then(|| C::column(cx, filter, *key, &slots))
                .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(slots
        .into_iter()
        .map(|slot| {
            (
                table.id(slot),
                columns
                    .iter()
                    .map(|column| column.as_ref().and_then(|column| column.get(slot)))
                    .collect(),
            )
        })
        .collect())
}

/// One page of matching entities and the number that match.
pub(crate) fn page<C: Catalog>(
    db: &DbAny,
    viewer: &Viewer,
    query: &Query<C>,
    offset: u64,
    limit: Option<u64>,
) -> Result<Page<C::Item>, CatalogError> {
    page_in(&Context::new(db, viewer)?, query, offset, limit)
}

/// [`page`] in an existing context.
pub(crate) fn page_in<C: Catalog>(
    cx: &Context<'_>,
    query: &Query<C>,
    offset: u64,
    limit: Option<u64>,
) -> Result<Page<C::Item>, CatalogError> {
    let offset_len = usize::try_from(offset).unwrap_or(usize::MAX);
    // A page of no items keeps none, wherever it starts.
    let end = limit.map(|limit| match limit {
        0 => 0,
        limit => offset_len.saturating_add(usize::try_from(limit).unwrap_or(usize::MAX)),
    });
    let (slots, total) = sorted(cx, query, end)?;
    let table = C::table(&cx.index);
    let ids = slots
        .into_iter()
        .skip(offset_len)
        .map(|slot| table.id(slot))
        .collect::<Vec<_>>();
    Ok(Page {
        items: C::hydrate(cx.db, &ids)?,
        total: total as u64,
        offset: offset.min(total as u64),
    })
}

struct Entry {
    hit: Hit,
    unit: ShuffleUnit,
}

/// The matching slots in order, keeping only the first `keep` when given, and the number that
/// match.
fn sorted<C: Catalog>(
    cx: &Context<'_>,
    query: &Query<C>,
    keep: Option<usize>,
) -> Result<(Vec<u32>, usize), CatalogError> {
    let needle = query.search.as_deref().and_then(Needle::parse);
    let sort = resolve_sort::<C>(&query.filter, &query.sort, needle.is_some())?;
    // Ties break in name order, so ending on it orders nothing more.
    let mut keys = sort.as_slice();
    while let Some(((key, Direction::Ascending), rest)) = keys.split_last()
        && *key == C::Key::NAME_ORDER
    {
        keys = rest;
    }

    let table = C::table(&cx.index);
    let mut candidates = C::candidates(cx, &query.filter)?;
    let in_names = |slot: &u32| query.names.contains(table.sort_name(*slot));
    // A name matches few slots, so each is checked on its own and only they are sorted.
    let named = query.name.as_deref().map(|name| {
        table
            .named(name)
            .iter()
            .copied()
            .filter(|slot| candidates.contains(*slot) && in_names(slot))
            .collect::<Vec<_>>()
    });
    if named.is_none() && !query.names.is_unbounded() {
        candidates = Bits::of(table.len(), candidates.iter().filter(in_names));
    }
    let unranked = |slot| Entry {
        hit: Hit {
            slot,
            tier: Tier::Related,
            own_words: 0,
            by_name: false,
        },
        unit: ShuffleUnit {
            id: table.id(slot),
            place: 0,
        },
    };

    let entries = match (&needle, named) {
        (Some(needle), named) => table
            .search(
                needle,
                &named.map_or(candidates, |slots| Bits::of(table.len(), slots)),
                |word| C::related(cx, word),
            )
            .into_iter()
            .map(|hit| Entry {
                unit: ShuffleUnit {
                    id: table.id(hit.slot),
                    place: 0,
                },
                hit,
            })
            .collect::<Vec<_>>(),
        (None, Some(slots)) => slots.into_iter().map(unranked).collect(),
        (None, None) => {
            let total = candidates.count();
            if keep == Some(0) {
                // Reading no values still raises any error the sort keys would.
                for (key, _) in keys {
                    if key.kind() == KeyKind::Field {
                        C::column(cx, &query.filter, *key, &[])?;
                    }
                }
                return Ok((Vec::new(), total));
            }
            if let Some(order) = kept_order::<C>(cx, &query.filter, keys)? {
                let keep = keep.unwrap_or(usize::MAX).min(total);
                let mut slots = Vec::with_capacity(keep);
                for slot in order.iter() {
                    if slots.len() == keep {
                        break;
                    }
                    if candidates.contains(*slot) {
                        slots.push(*slot);
                    }
                }
                return Ok((slots, total));
            }
            candidates.iter().map(unranked).collect()
        }
    };
    let mut entries = entries;

    let slots = entries
        .iter()
        .map(|entry| entry.hit.slot)
        .collect::<Vec<_>>();
    let columns = keys
        .iter()
        .map(|(key, _)| match key.kind() {
            KeyKind::Field => C::column(cx, &query.filter, *key, &slots).map(Some),
            _ => Ok(None),
        })
        .collect::<Result<Vec<_>, _>>()?;
    if keys.iter().any(|(key, _)| key.kind() == KeyKind::Random) {
        let ids = entries
            .iter()
            .map(|entry| entry.unit.id)
            .collect::<Vec<_>>();
        let units = C::shuffle_units(cx, &ids)?;
        for entry in &mut entries {
            if let Some(&unit) = units.get(&entry.unit.id) {
                entry.unit = unit;
            }
        }
    }

    let total = entries.len();
    let compare =
        |a: &Entry, b: &Entry| compare(a, b, keys, &columns, &table.ids, &table.rank, query.seed);
    if let Some(keep) = keep.filter(|keep| *keep < entries.len()) {
        if keep == 0 {
            entries.clear();
        } else {
            entries.select_nth_unstable_by(keep - 1, compare);
            entries.truncate(keep);
        }
    }
    entries.sort_by(compare);
    Ok((
        entries.into_iter().map(|entry| entry.hit.slot).collect(),
        total,
    ))
}

/// The kept order of every slot under `keys`, when they are one key whose values do not depend
/// on the viewer or filter, or none.
fn kept_order<C: Catalog>(
    cx: &Context<'_>,
    filter: &C::Filter,
    keys: &[(C::Key, Direction)],
) -> Result<Option<Arc<[u32]>>, CatalogError> {
    let table = C::table(&cx.index);
    let [(key, direction)] = keys else {
        return Ok(keys.is_empty().then(|| table.name_order()));
    };
    match key.kind() {
        KeyKind::Id => {
            let key = cache_key(*key);
            let ids = table
                .cache
                .column(key, || table.ids.iter().map(|id| id.0 as u64).collect());
            Ok(Some(table.cache.order(
                key,
                *direction,
                false,
                &ids,
                &table.rank,
            )))
        }
        KeyKind::Field => {
            let (values, cache) = match C::column(cx, filter, *key, &[])? {
                Column::Stored(values, cache) => (values, cache),
                Column::Fields(values) => (Arc::from(values), &table.cache),
                Column::Computed(_) => return Ok(None),
            };
            let order = cache.order(
                cache_key(*key),
                *direction,
                key.missing_first(),
                &values,
                &table.rank,
            );
            cx.grown(cache);
            Ok(Some(order))
        }
        KeyKind::Relevance | KeyKind::Random => Ok(None),
    }
}

fn resolve_sort<C: Catalog>(
    filter: &C::Filter,
    requested: &SortSpec<C::Key>,
    searching: bool,
) -> Result<SortSpec<C::Key>, CatalogError> {
    let relevance = C::Key::RELEVANCE;
    let sort = if !requested.is_empty() {
        requested.clone()
    } else if searching {
        vec![(relevance, Direction::Ascending)]
    } else {
        C::default_sort(filter)
    };
    if !searching && sort.iter().any(|(key, _)| *key == relevance) {
        return Err(CatalogError::Invalid(
            "the relevance sort key needs a search term".to_string(),
        ));
    }
    Ok(sort)
}

fn compare<K: SortKey>(
    a: &Entry,
    b: &Entry,
    keys: &[(K, Direction)],
    columns: &[Option<Column<'_>>],
    ids: &[DbId],
    rank: &[u32],
    seed: u64,
) -> Ordering {
    for ((key, direction), column) in keys.iter().zip(columns) {
        let ordering = match (key.kind(), column) {
            (KeyKind::Relevance, _) => direction.apply(
                b.hit
                    .tier
                    .cmp(&a.hit.tier)
                    .then_with(|| b.hit.own_words.cmp(&a.hit.own_words))
                    .then_with(|| b.hit.by_name.cmp(&a.hit.by_name)),
            ),
            // A unit keeps its place order whichever way the units run.
            (KeyKind::Random, _) => direction
                .apply(
                    shuffle(seed, a.unit.id)
                        .cmp(&shuffle(seed, b.unit.id))
                        .then_with(|| a.unit.id.0.cmp(&b.unit.id.0)),
                )
                .then_with(|| a.unit.place.cmp(&b.unit.place)),
            (KeyKind::Id, _) => {
                direction.apply(ids[a.hit.slot as usize].0.cmp(&ids[b.hit.slot as usize].0))
            }
            (KeyKind::Field, Some(column)) => compare_values(
                column.get(a.hit.slot),
                column.get(b.hit.slot),
                *direction,
                key.missing_first(),
            ),
            (KeyKind::Field, None) => unreachable!("field keys always have a column"),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    rank[a.hit.slot as usize].cmp(&rank[b.hit.slot as usize])
}

fn compare_values(
    a: Option<u64>,
    b: Option<u64>,
    direction: Direction,
    missing_first: bool,
) -> Ordering {
    let missing = if missing_first {
        Ordering::Less
    } else {
        Ordering::Greater
    };
    match (a, b) {
        (Some(a), Some(b)) => direction.apply(a.cmp(&b)),
        (None, Some(_)) => missing,
        (Some(_), None) => missing.reverse(),
        (None, None) => Ordering::Equal,
    }
}

pub(crate) fn shuffle(seed: u64, id: DbId) -> u64 {
    let mut value = seed ^ (id.0 as u64);
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    use crate::services::catalog::{
        artists::Artists,
        releases::{
            ReleaseKey,
            Releases,
        },
        tracks::{
            TrackKey,
            Tracks,
        },
    };
    use lyra_db::{
        fixtures::{
            insert_release,
            insert_track,
        },
        test_db::{
            insert_credited_artist,
            new_test_db,
        },
    };

    fn search<C: Catalog>(db: &DbAny, filter: C::Filter, term: &str) -> Vec<DbId> {
        let mut query = Query::<C>::new(filter);
        query.search = Some(term.to_string());
        order(db, &Viewer::System, &query).expect("search runs")
    }

    /// Two libraries: a viewer granted only "Shown" must not match any text that lives only in
    /// "Hidden". "Song" sits on a release in each; "Credits" is credited only on the hidden one.
    struct Split {
        db: DbAny,
        shown_id: String,
        song: DbId,
        hidden_release: DbId,
        hidden_artist: DbId,
    }

    fn split() -> anyhow::Result<Split> {
        use lyra_db::{
            fixtures::{
                connect,
                connect_credit,
                insert_artist,
            },
            test_db::insert_library,
        };
        let mut db = new_test_db()?;
        let shown = insert_library(&mut db, "Shown", "/split/shown")?;
        let hidden = insert_library(&mut db, "Hidden", "/split/hidden")?;
        let shown_release = insert_release(&mut db, "Public")?;
        let hidden_release = insert_release(&mut db, "Secretcodename")?;
        connect(&mut db, shown, shown_release)?;
        connect(&mut db, hidden, hidden_release)?;
        let song = insert_track(&mut db, "Song")?;
        connect(&mut db, shown_release, song)?;
        connect(&mut db, hidden_release, song)?;
        let hidden_only = insert_track(&mut db, "Lonely")?;
        connect(&mut db, hidden_release, hidden_only)?;
        let hidden_artist = insert_artist(&mut db, "Privateartist")?;
        connect_credit(
            &mut db,
            hidden_release,
            hidden_artist,
            lyra_db::CreditType::Artist,
            None,
            0,
        )?;
        let shared_artist = insert_artist(&mut db, "Sharedartist")?;
        for owner in [shown_release, hidden_release] {
            connect_credit(
                &mut db,
                owner,
                shared_artist,
                lyra_db::CreditType::Artist,
                None,
                0,
            )?;
        }
        lyra_db::genres::sync_release_genres(&mut db, hidden_release, &["Hiddengenre".into()])?;
        let shown_id = lyra_db::libraries::get_by_id(&db, shown)?
            .expect("library exists")
            .id;
        Ok(Split {
            db,
            shown_id,
            song,
            hidden_release,
            hidden_artist,
        })
    }

    fn found<C: Catalog>(db: &DbAny, viewer: &Viewer, term: &str) -> (Vec<DbId>, u64)
    where
        C::Filter: Default,
    {
        let mut query = Query::<C>::new(C::Filter::default());
        query.search = Some(term.to_string());
        let ids = order(db, viewer, &query).expect("search runs");
        let total = page(db, viewer, &query, 0, Some(1))
            .expect("search runs")
            .total;
        assert_eq!(ids.len() as u64, total);
        (ids, total)
    }

    #[test]
    fn search_matches_only_texts_the_viewer_can_see() -> anyhow::Result<()> {
        use crate::services::{
            auth::Principal,
            catalog::genres::Genres,
        };
        let mut split = split()?;
        let user = lyra_db::test_db::insert_user(&mut split.db, "listener")?;
        let db = &split.db;
        let restricted = Viewer::User {
            principal: Principal::for_user(
                db,
                user,
                Vec::new(),
                HashSet::from([split.shown_id.clone()]),
            ),
            user_db_id: user,
            playlists: Default::default(),
        };
        let none = (Vec::new(), 0);

        for term in ["secretcodename", "privateartist", "song secretcodename"] {
            assert_eq!(found::<Tracks>(db, &restricted, term), none, "{term}");
        }
        assert_eq!(
            found::<Tracks>(db, &restricted, "song"),
            (vec![split.song], 1)
        );
        assert_eq!(
            found::<Tracks>(db, &restricted, "public"),
            (vec![split.song], 1)
        );
        assert_eq!(
            found::<Tracks>(db, &restricted, "sharedartist"),
            (vec![split.song], 1)
        );
        assert_eq!(found::<Releases>(db, &restricted, "privateartist"), none);
        assert_eq!(found::<Releases>(db, &restricted, "secretcodename"), none);
        assert_eq!(found::<Artists>(db, &restricted, "privateartist"), none);
        assert_eq!(found::<Genres>(db, &restricted, "hiddengenre"), none);

        let admin = Viewer::System;
        assert_eq!(found::<Tracks>(db, &admin, "secretcodename").1, 2);
        assert_eq!(found::<Tracks>(db, &admin, "privateartist").1, 2);
        assert_eq!(
            found::<Releases>(db, &admin, "privateartist"),
            (vec![split.hidden_release], 1)
        );
        assert_eq!(
            found::<Artists>(db, &admin, "privateartist"),
            (vec![split.hidden_artist], 1)
        );
        assert_eq!(found::<Genres>(db, &admin, "hiddengenre").1, 1);
        Ok(())
    }

    #[test]
    fn partial_own_name_matches_rank_above_related_matches() -> anyhow::Result<()> {
        use lyra_db::fixtures::{
            connect,
            connect_credit,
            insert_artist,
        };
        let mut db = new_test_db()?;
        let radiohead = insert_artist(&mut db, "Radiohead")?;
        let computer = insert_release(&mut db, "OK Computer")?;
        connect_credit(
            &mut db,
            computer,
            radiohead,
            lyra_db::CreditType::Artist,
            None,
            0,
        )?;
        let mut titled = |title: &str| -> anyhow::Result<DbId> {
            let track = insert_track(&mut db, title)?;
            connect(&mut db, computer, track)?;
            Ok(track)
        };
        let airbag = titled("Airbag")?;
        let ok = titled("OK")?;
        let karma = titled("Karma Police OK")?;
        let exact = titled("Radiohead OK")?;
        let prefix = titled("Radiohead OK Live")?;
        let words = titled("A Radiohead Live OK")?;
        let both = titled("OK Radiohead Karma")?;
        let ids = search::<Tracks>(&db, Default::default(), "radiohead ok");
        // Exact, name prefix, every word in the name, then some words in the name, each tier in
        // name order, then matches only through the release and its artist.
        assert_eq!(ids, [exact, prefix, words, both, karma, ok, airbag]);
        Ok(())
    }

    fn named<C: Catalog>(db: &DbAny, name: &str) -> Vec<DbId>
    where
        C::Filter: Default,
    {
        let mut query = Query::<C>::new(C::Filter::default());
        query.name = Some(name.to_string());
        order(db, &Viewer::System, &query).expect("the query runs")
    }

    #[test]
    fn the_name_filter_keeps_names_equal_once_folded() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let accented = insert_track(&mut db, "Jóga")?;
        let upper = insert_track(&mut db, "JÓGA")?;
        let plain = insert_track(&mut db, "joga")?;
        let punctuated = insert_track(&mut db, "Jo-ga!")?;
        for title in ["Jóga (Live)", "Jog", "Yoga", "Hunter"] {
            insert_track(&mut db, title)?;
        }
        let sorted_as = insert_track(&mut db, "Other")?;
        set_track_sort(&mut db, sorted_as, "Jóga")?;
        let greek = insert_track(&mut db, "Σοφία")?;

        let mut found = named::<Tracks>(&db, "JÓGA");
        found.sort_unstable_by_key(|id| id.0);
        assert_eq!(found, [accented, upper, plain, punctuated]);
        assert_eq!(named::<Tracks>(&db, "ΣΟΦΙΑ"), [greek]);
        assert!(named::<Tracks>(&db, "Jógas").is_empty());
        Ok(())
    }

    #[test]
    fn the_name_filter_keeps_only_credited_artists_and_combines_with_search() -> anyhow::Result<()>
    {
        let mut db = new_test_db()?;
        let credited = insert_credited_artist(&mut db, "Björk")?;
        let shouted = insert_credited_artist(&mut db, "BJÖRK")?;
        insert_credited_artist(&mut db, "Björk Trio")?;
        lyra_db::fixtures::insert_artist(&mut db, "Björk")?;

        let mut found = named::<Artists>(&db, "bjork");
        found.sort_unstable_by_key(|id| id.0);
        assert_eq!(found, [credited, shouted]);

        let mut query = Query::<Artists>::new(Default::default());
        query.name = Some("Björk".to_string());
        query.search = Some("trio".to_string());
        assert!(order(&db, &Viewer::System, &query)?.is_empty());
        Ok(())
    }

    /// The artist name key reads each track's first credited artist, whichever artists and role
    /// the filter matched it through.
    #[test]
    fn artist_name_order_follows_the_lead_credit_under_any_credit_filter() -> anyhow::Result<()> {
        use crate::services::catalog::{
            ArtistCredit,
            CreditRole,
            tracks::TrackFilter,
        };
        use lyra_db::{
            CreditType,
            fixtures::{
                connect,
                connect_credit,
                insert_artist,
            },
        };
        let mut db = new_test_db()?;
        let bach = insert_artist(&mut db, "Bach")?;
        let release = insert_release(&mut db, "Partitas")?;
        connect_credit(&mut db, release, bach, CreditType::Composer, None, 0)?;
        let mut performed = |title: &str, performer: &str| -> anyhow::Result<DbId> {
            let track = insert_track(&mut db, title)?;
            connect(&mut db, release, track)?;
            let performer = insert_artist(&mut db, performer)?;
            connect_credit(
                &mut db,
                track,
                performer,
                CreditType::Instrumentalist,
                None,
                0,
            )?;
            connect_credit(&mut db, track, bach, CreditType::Composer, None, 1)?;
            Ok(track)
        };
        let by_zed = performed("Allemande", "Zed")?;
        let by_ada = performed("Courante", "Ada")?;
        let unordered = insert_track(&mut db, "Gigue")?;
        connect(&mut db, release, unordered)?;
        connect_credit(
            &mut db,
            unordered,
            bach,
            CreditType::Instrumentalist,
            None,
            0,
        )?;

        for (role, expected) in [
            (CreditRole::Track, vec![by_ada, unordered, by_zed]),
            (CreditRole::Any, vec![by_ada, unordered, by_zed]),
            (CreditRole::Release, vec![by_ada, unordered, by_zed]),
        ] {
            let mut query = Query::<Tracks>::new(TrackFilter {
                artists: Some(ArtistCredit {
                    artists: vec![bach],
                    role,
                    excluding: None,
                }),
                ..TrackFilter::default()
            });
            query.sort = vec![(TrackKey::ArtistName, Direction::Ascending)];
            assert_eq!(order(&db, &Viewer::System, &query)?, expected, "{role:?}");
            query.sort = vec![(TrackKey::ArtistName, Direction::Descending)];
            let mut reversed = expected.clone();
            reversed.reverse();
            assert_eq!(order(&db, &Viewer::System, &query)?, reversed, "{role:?}");
        }
        Ok(())
    }

    /// Inserts a `display` entity and a `transliterated` one whose sort name is `sort`.
    fn insert_pair(
        db: &mut DbAny,
        display: &str,
        transliterated: &str,
        sort: &str,
        insert: fn(&mut DbAny, &str) -> anyhow::Result<DbId>,
        set_sort: fn(&mut DbAny, DbId, &str) -> anyhow::Result<()>,
    ) -> anyhow::Result<(DbId, DbId)> {
        let by_name = insert(db, display)?;
        let by_sort = insert(db, transliterated)?;
        set_sort(db, by_sort, sort)?;
        Ok((by_name, by_sort))
    }

    fn set_track_sort(db: &mut DbAny, id: DbId, sort: &str) -> anyhow::Result<()> {
        let mut track = lyra_db::tracks::get_by_id(db, id)?.expect("track exists");
        track.sort_title = Some(sort.to_string());
        lyra_db::tracks::update(db, &track)
    }

    fn set_release_sort(db: &mut DbAny, id: DbId, sort: &str) -> anyhow::Result<()> {
        let mut release = lyra_db::releases::get_by_id(db, id)?.expect("release exists");
        release.sort_title = Some(sort.to_string());
        lyra_db::releases::update(db, &release)
    }

    fn set_artist_sort(db: &mut DbAny, id: DbId, sort: &str) -> anyhow::Result<()> {
        let mut artist = lyra_db::artists::get_by_id(db, id)?.expect("artist exists");
        artist.sort_name = Some(sort.to_string());
        lyra_db::artists::update(db, &artist)
    }

    #[test]
    fn search_matches_sort_names_after_equal_display_name_matches() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let tracks = insert_pair(
            &mut db,
            "Tokyo",
            "東京",
            "Tokyo",
            insert_track,
            set_track_sort,
        )?;
        let releases = insert_pair(
            &mut db,
            "Tokyo",
            "東京",
            "Tokyo",
            insert_release,
            set_release_sort,
        )?;
        let artists = insert_pair(
            &mut db,
            "Tokyo",
            "東京",
            "Tokyo",
            insert_credited_artist,
            set_artist_sort,
        )?;

        for (found, (by_name, by_sort)) in [
            (search::<Tracks>(&db, Default::default(), "tokyo"), tracks),
            (
                search::<Releases>(&db, Default::default(), "tokyo"),
                releases,
            ),
            (search::<Artists>(&db, Default::default(), "tokyo"), artists),
        ] {
            assert_eq!(found[..2], [by_name, by_sort]);
        }
        assert_eq!(
            search::<Tracks>(&db, Default::default(), "東京")[0],
            tracks.1
        );
        Ok(())
    }

    #[test]
    fn search_ranks_a_better_sort_name_match_first() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (weak, strong) = insert_pair(
            &mut db,
            "Tokyo Nights",
            "東京",
            "Tokyo",
            insert_track,
            set_track_sort,
        )?;
        assert_eq!(
            search::<Tracks>(&db, Default::default(), "tokyo"),
            vec![strong, weak]
        );
        Ok(())
    }

    struct Book {
        release: DbId,
        /// Interlude "Awakening" → 祝福 → アイドル, then Interlude "Worship" → 夜に駆ける.
        chain: Vec<DbId>,
        pair: Vec<DbId>,
        singles: Vec<DbId>,
    }

    fn book(db: &mut DbAny) -> anyhow::Result<Book> {
        let release = insert_release(db, "THE BOOK 3")?;
        let mut tracks = Vec::new();
        for (number, title) in [
            "Interlude \"Awakening\"",
            "祝福",
            "アイドル",
            "Interlude \"Worship\"",
            "夜に駆ける",
            "勇者",
            "セブンティーン",
            "ハルカ",
            "群青",
            "怪物",
            "優しい彗星",
            "三原色",
        ]
        .into_iter()
        .enumerate()
        {
            let track_db_id = insert_track(db, title)?;
            let mut track = lyra_db::tracks::get_by_id(db, track_db_id)?.expect("track exists");
            track.track = Some(number as u32 + 1);
            lyra_db::tracks::update(db, &track)?;
            lyra_db::releases::link_track(db, release, track_db_id)?;
            tracks.push(track_db_id);
        }
        let chain = tracks[0..3].to_vec();
        let pair = tracks[3..5].to_vec();
        lyra_db::chains::replace_for_release(db, release, &[chain.clone(), pair.clone()])??;
        Ok(Book {
            release,
            chain,
            pair,
            singles: tracks[5..].to_vec(),
        })
    }

    fn shuffled<C: Catalog>(
        db: &DbAny,
        filter: C::Filter,
        sort: SortSpec<C::Key>,
        seed: u64,
    ) -> Vec<DbId> {
        let mut query = Query::<C>::new(filter);
        query.sort = sort;
        query.seed = seed;
        order(db, &Viewer::System, &query).expect("order runs")
    }

    fn random(direction: Direction) -> SortSpec<TrackKey> {
        vec![(TrackKey::Random, direction)]
    }

    /// The order the `members` appear in, if they appear together.
    fn run_of(order: &[DbId], members: &[DbId]) -> Option<Vec<DbId>> {
        let start = order.iter().position(|id| members.contains(id))?;
        let run = order[start..]
            .iter()
            .take_while(|id| members.contains(id))
            .copied()
            .collect::<Vec<_>>();
        let found = order.iter().filter(|id| members.contains(id)).count();
        (run.len() == found).then_some(run)
    }

    /// Each chain collapsed to its first member, leaving the units in order.
    fn units(order: &[DbId], book: &Book) -> Vec<DbId> {
        order
            .iter()
            .copied()
            .filter(|id| !book.chain[1..].contains(id) && !book.pair[1..].contains(id))
            .collect()
    }

    #[test]
    fn random_tracks_keep_each_chain_together_and_in_order() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let book = book(&mut db)?;

        let mut chain_starts = HashSet::new();
        for seed in 0..32 {
            let order =
                shuffled::<Tracks>(&db, Default::default(), random(Direction::Ascending), seed);
            assert_eq!(order.len(), 12);
            assert_eq!(run_of(&order, &book.chain), Some(book.chain.clone()));
            assert_eq!(run_of(&order, &book.pair), Some(book.pair.clone()));
            assert_eq!(
                order,
                shuffled::<Tracks>(&db, Default::default(), random(Direction::Ascending), seed),
            );
            chain_starts.insert(order.iter().position(|id| *id == book.chain[0]));
        }
        assert!(chain_starts.len() > 1, "the chain moves with the seed");
        Ok(())
    }

    #[test]
    fn random_tracks_group_only_the_chain_members_that_match() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let book = book(&mut db)?;
        let mut ids = book.singles.clone();
        ids.extend([book.chain[0], book.chain[2]]);

        for seed in 0..16 {
            let order = shuffled::<Tracks>(
                &db,
                Tracks::id_filter(ids.clone()),
                random(Direction::Ascending),
                seed,
            );
            assert_eq!(order.len(), ids.len());
            assert!(!order.contains(&book.chain[1]));
            assert_eq!(
                run_of(&order, &book.chain),
                Some(vec![book.chain[0], book.chain[2]])
            );
        }
        Ok(())
    }

    #[test]
    fn descending_random_reverses_the_units_but_not_the_chains() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let book = book(&mut db)?;

        for seed in 0..16 {
            let ascending =
                shuffled::<Tracks>(&db, Default::default(), random(Direction::Ascending), seed);
            let descending =
                shuffled::<Tracks>(&db, Default::default(), random(Direction::Descending), seed);
            assert_eq!(run_of(&descending, &book.chain), Some(book.chain.clone()));
            assert_eq!(run_of(&descending, &book.pair), Some(book.pair.clone()));
            let mut reversed = units(&ascending, &book);
            reversed.reverse();
            assert_eq!(units(&descending, &book), reversed);
        }
        Ok(())
    }

    #[test]
    fn random_pages_count_tracks_and_may_split_a_chain() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let book = book(&mut db)?;
        let mut query = Query::<Tracks>::new(Default::default());
        query.sort = random(Direction::Ascending);

        let mut split = false;
        for seed in 0..32 {
            query.seed = seed;
            let order = order(&db, &Viewer::System, &query)?;
            for limit in 1..=5 {
                let mut paged = Vec::new();
                for offset in (0..12).step_by(limit) {
                    let page = page(
                        &db,
                        &Viewer::System,
                        &query,
                        offset as u64,
                        Some(limit as u64),
                    )?;
                    assert_eq!(page.total, 12);
                    assert!(page.items.len() <= limit);
                    let ids = page
                        .items
                        .iter()
                        .filter_map(|track| track.db_id.clone().map(DbId::from))
                        .collect::<Vec<_>>();
                    split |=
                        limit == 2 && ids.contains(&book.chain[1]) && !ids.contains(&book.chain[0]);
                    paged.extend(ids);
                }
                assert_eq!(paged, order);
            }
        }
        assert!(split, "some page starts inside the chain");
        Ok(())
    }

    #[test]
    fn random_after_another_key_groups_chains_only_within_its_ties() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let book = book(&mut db)?;
        for (track_db_id, year) in [(book.chain[0], 2023), (book.chain[1], 2024)] {
            let mut track = lyra_db::tracks::get_by_id(&db, track_db_id)?.expect("track exists");
            track.year = Some(year);
            lyra_db::tracks::update(&mut db, &track)?;
        }
        for &track_db_id in book.chain[2..].iter().chain(&book.singles[..3]) {
            let mut track = lyra_db::tracks::get_by_id(&db, track_db_id)?.expect("track exists");
            track.year = Some(2024);
            lyra_db::tracks::update(&mut db, &track)?;
        }
        let sort = vec![
            (TrackKey::Year, Direction::Ascending),
            (TrackKey::Random, Direction::Ascending),
        ];

        for seed in 0..16 {
            let order = shuffled::<Tracks>(&db, Default::default(), sort.clone(), seed);
            assert_eq!(order[0], book.chain[0], "2023 sorts first");
            assert_eq!(
                run_of(&order[1..6], &book.chain[1..]),
                Some(book.chain[1..].to_vec())
            );
            assert_eq!(run_of(&order, &book.pair), Some(book.pair.clone()));
        }
        Ok(())
    }

    #[test]
    fn random_releases_and_unchained_tracks_shuffle_by_their_own_ids() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let book = book(&mut db)?;
        let mut releases = vec![book.release];
        for title in ["Urn", "Portal of I", "Citadel"] {
            releases.push(insert_release(&mut db, title)?);
        }

        for seed in 0..8 {
            let mut expected = releases.clone();
            expected.sort_by_key(|id| shuffle(seed, *id));
            let order = shuffled::<Releases>(
                &db,
                Default::default(),
                vec![(ReleaseKey::Random, Direction::Ascending)],
                seed,
            );
            assert_eq!(order, expected);

            let mut singles = book.singles.clone();
            singles.sort_by_key(|id| shuffle(seed, *id));
            let tracks = shuffled::<Tracks>(
                &db,
                Tracks::id_filter(book.singles.clone()),
                random(Direction::Ascending),
                seed,
            );
            assert_eq!(tracks, singles);
        }
        Ok(())
    }
}
