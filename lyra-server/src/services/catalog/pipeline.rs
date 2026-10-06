// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The one sort, tie-break and page implementation for catalog queries.

use std::{
    cmp::Ordering,
    collections::HashMap,
};

use agdb::{
    DbAny,
    DbId,
};

use super::{
    CatalogError,
    Direction,
    Page,
    SortSpec,
    Viewer,
};

/// What a sort key orders by.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyKind {
    /// The search match score, best first.
    Relevance,
    /// A stable shuffle driven by the query's seed.
    Random,
    Id,
    /// A value the catalog loads for each candidate.
    Field,
}

pub(crate) trait SortKey: Copy + Eq + std::fmt::Debug + 'static {
    /// Every key and the token that names it.
    const KEYS: &'static [(&'static str, Self)];
    /// The key ranking search matches, and the default order when searching.
    const RELEVANCE: Self;

    fn kind(self) -> KeyKind;

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
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SortValue {
    Number(u64),
    Text(String),
}

/// A candidate's names and the values of the requested field keys, in key order.
pub(crate) struct Row {
    pub(crate) id: DbId,
    /// The display name, which search matches against.
    pub(crate) name: String,
    /// The lowercase sort name, which ties break on and name ranges bound.
    pub(crate) sort_name: String,
    pub(crate) values: Vec<Option<SortValue>>,
}

pub(crate) trait Catalog {
    type Key: SortKey;
    type Filter;
    type Item;

    /// The filter that keeps just these ids.
    fn id_filter(ids: Vec<DbId>) -> Self::Filter;

    /// The order used when a query names no keys and has no search term.
    fn default_sort(filter: &Self::Filter) -> SortSpec<Self::Key>;

    /// Rejects keys that the filter cannot give a meaning to.
    fn check_sort(_filter: &Self::Filter, _sort: &SortSpec<Self::Key>) -> Result<(), CatalogError> {
        Ok(())
    }

    /// The ids the viewer can see that pass the filter, read from edges and ids only.
    fn candidates(
        db: &DbAny,
        viewer: &Viewer,
        filter: &Self::Filter,
    ) -> Result<Vec<DbId>, CatalogError>;

    /// A row for each candidate id, with a value for each of `keys`.
    fn rows(
        db: &DbAny,
        viewer: &Viewer,
        filter: &Self::Filter,
        ids: Vec<DbId>,
        keys: &[Self::Key],
    ) -> Result<Vec<Row>, CatalogError>;

    /// The entities with these ids, in this order.
    fn hydrate(db: &DbAny, ids: &[DbId]) -> anyhow::Result<Vec<Self::Item>>;
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

    fn contains(&self, sort_name: &str) -> bool {
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
            search: None,
            sort: Vec::new(),
            seed: 0,
        }
    }
}

/// Every matching id in order, for cursor snapshots.
pub(crate) fn order<C: Catalog>(
    db: &DbAny,
    viewer: &Viewer,
    query: &Query<C>,
) -> Result<Vec<DbId>, CatalogError> {
    let (entries, _) = sorted(db, viewer, query, None)?;
    Ok(entries.into_iter().map(|entry| entry.row.id).collect())
}

/// One page of matching entities and the number that match.
/// The numeric values of `keys` for each of `ids`, as the catalog computes them to sort by;
/// a key with no value reads as zero.
pub(crate) fn counts<C: Catalog>(
    db: &DbAny,
    viewer: &Viewer,
    filter: &C::Filter,
    ids: Vec<DbId>,
    keys: &[C::Key],
) -> Result<HashMap<DbId, Vec<u64>>, CatalogError> {
    Ok(C::rows(db, viewer, filter, ids, keys)?
        .into_iter()
        .map(|row| {
            let values = row
                .values
                .into_iter()
                .map(|value| match value {
                    Some(SortValue::Number(number)) => number,
                    _ => 0,
                })
                .collect();
            (row.id, values)
        })
        .collect())
}

pub(crate) fn page<C: Catalog>(
    db: &DbAny,
    viewer: &Viewer,
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
    let (entries, total) = sorted(db, viewer, query, end)?;
    let ids = entries
        .into_iter()
        .skip(offset_len)
        .map(|entry| entry.row.id)
        .collect::<Vec<_>>();
    Ok(Page {
        items: C::hydrate(db, &ids)?,
        total: total as u64,
        offset: offset.min(total as u64),
    })
}

struct Entry {
    row: Row,
    lower_name: String,
    score: u32,
}

/// The matching entries in order, keeping only the first `keep` when given, and the number that
/// match.
fn sorted<C: Catalog>(
    db: &DbAny,
    viewer: &Viewer,
    query: &Query<C>,
    keep: Option<usize>,
) -> Result<(Vec<Entry>, usize), CatalogError> {
    let search = query
        .search
        .as_deref()
        .map(str::trim)
        .filter(|term| !term.is_empty());
    let sort = resolve_sort::<C>(&query.filter, &query.sort, search.is_some())?;

    let mut fields = Vec::new();
    let columns = sort
        .iter()
        .map(|(key, _)| {
            (key.kind() == KeyKind::Field).then(|| {
                fields
                    .iter()
                    .position(|field| field == key)
                    .unwrap_or_else(|| {
                        fields.push(*key);
                        fields.len() - 1
                    })
            })
        })
        .collect::<Vec<_>>();

    let ids = C::candidates(db, viewer, &query.filter)?;
    // Without a search or name bounds every candidate makes a row, so keeping none needs only the
    // count. Reading no rows still raises any error the sort keys would.
    if keep == Some(0) && search.is_none() && query.names.is_unbounded() {
        C::rows(db, viewer, &query.filter, Vec::new(), &fields)?;
        return Ok((Vec::new(), ids.len()));
    }
    let mut entries = C::rows(db, viewer, &query.filter, ids, &fields)?
        .into_iter()
        .filter(|row| query.names.contains(&row.sort_name))
        .map(|row| Entry {
            lower_name: row.name.to_lowercase(),
            row,
            score: 0,
        })
        .collect::<Vec<_>>();
    if let Some(term) = search {
        crate::db::search::fuzzy_filter(
            &mut entries,
            term,
            |entry| entry.row.name.as_str(),
            |entry, score| entry.score = score,
        );
    }

    let total = entries.len();
    let compare = |a: &Entry, b: &Entry| compare(a, b, &sort, &columns, query.seed);
    if let Some(keep) = keep.filter(|keep| *keep < entries.len()) {
        if keep == 0 {
            entries.clear();
        } else {
            entries.select_nth_unstable_by(keep - 1, compare);
            entries.truncate(keep);
        }
    }
    entries.sort_by(compare);
    Ok((entries, total))
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
    C::check_sort(filter, &sort)?;
    Ok(sort)
}

fn compare<K: SortKey>(
    a: &Entry,
    b: &Entry,
    sort: &SortSpec<K>,
    columns: &[Option<usize>],
    seed: u64,
) -> Ordering {
    for ((key, direction), column) in sort.iter().zip(columns) {
        let ordering = match (key.kind(), column) {
            (KeyKind::Relevance, _) => direction.apply(b.score.cmp(&a.score)),
            (KeyKind::Random, _) => {
                direction.apply(shuffle(seed, a.row.id).cmp(&shuffle(seed, b.row.id)))
            }
            (KeyKind::Id, _) => direction.apply(a.row.id.0.cmp(&b.row.id.0)),
            (KeyKind::Field, Some(column)) => compare_values(
                &a.row.values[*column],
                &b.row.values[*column],
                *direction,
                key.missing_first(),
            ),
            (KeyKind::Field, None) => unreachable!("field keys always have a column"),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    a.row
        .sort_name
        .cmp(&b.row.sort_name)
        .then_with(|| a.lower_name.cmp(&b.lower_name))
        .then_with(|| a.row.name.cmp(&b.row.name))
        .then_with(|| a.row.id.0.cmp(&b.row.id.0))
}

fn compare_values(
    a: &Option<SortValue>,
    b: &Option<SortValue>,
    direction: Direction,
    missing_first: bool,
) -> Ordering {
    let missing = if missing_first {
        Ordering::Less
    } else {
        Ordering::Greater
    };
    match (a, b) {
        (Some(a), Some(b)) => direction.apply(a.cmp(b)),
        (None, Some(_)) => missing,
        (Some(_), None) => missing.reverse(),
        (None, None) => Ordering::Equal,
    }
}

fn shuffle(seed: u64, id: DbId) -> u64 {
    let mut value = seed ^ (id.0 as u64);
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
