// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The one sort, tie-break and page implementation for catalog queries.

use std::{
    cmp::Ordering,
    collections::{
        BTreeSet,
        HashMap,
    },
};

use agdb::{
    DbAny,
    DbId,
};

use lyra_db::ratings::RatingFilter;

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
    lower_name: String,
    /// The lowercase sort title, when the entity has one.
    sort_title: Option<String>,
    pub(crate) values: Vec<Option<SortValue>>,
}

impl Row {
    /// A row with no values yet, sorting by `sort_title` when given and by `name` otherwise.
    pub(crate) fn new(id: DbId, name: String, sort_title: Option<&str>) -> Self {
        Self {
            id,
            lower_name: name.to_lowercase(),
            sort_title: sort_title.map(str::to_lowercase),
            name,
            values: Vec::new(),
        }
    }

    pub(crate) fn lower_name(&self) -> &str {
        &self.lower_name
    }

    /// The lowercase sort name, which search also matches, ties break on and name ranges bound.
    pub(crate) fn sort_name(&self) -> &str {
        self.sort_title.as_deref().unwrap_or(&self.lower_name)
    }
}

/// Data of the viewer's own that a query can filter or sort by.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum PersonalData {
    Favorites,
    Listens,
    Ratings,
}

impl PersonalData {
    /// What `favorite`, `listened` and `rating` filters read.
    pub(crate) fn of_filter(
        favorite: Option<bool>,
        listened: Option<bool>,
        rating: RatingFilter,
    ) -> Vec<Self> {
        [
            favorite.map(|_| Self::Favorites),
            listened.map(|_| Self::Listens),
            (!rating.is_empty()).then_some(Self::Ratings),
        ]
        .into_iter()
        .flatten()
        .collect()
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

    /// The shuffle unit of each of `ids` that the random key moves with others. Every other id
    /// shuffles alone.
    fn shuffle_units(_db: &DbAny, _ids: &[DbId]) -> anyhow::Result<HashMap<DbId, ShuffleUnit>> {
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
    unit: ShuffleUnit,
    score: u32,
    /// Whether the display name scored `score`, which ranks ahead of a sort name scoring the same.
    by_name: bool,
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
        .filter(|row| query.names.contains(row.sort_name()))
        .map(|row| Entry {
            unit: ShuffleUnit {
                id: row.id,
                place: 0,
            },
            row,
            score: 0,
            by_name: false,
        })
        .collect::<Vec<_>>();
    if let Some(term) = search {
        crate::services::search::fuzzy_retain(&mut entries, term, |entry, score| {
            let by_name = score(&entry.row.name);
            let by_sort_name = (entry.row.sort_name() != entry.row.lower_name())
                .then(|| score(entry.row.sort_name()))
                .flatten();
            let Some(best) = by_name.max(by_sort_name) else {
                return false;
            };
            entry.score = best;
            entry.by_name = by_name == Some(best);
            true
        });
    }

    if sort.iter().any(|(key, _)| key.kind() == KeyKind::Random) {
        let ids = entries.iter().map(|entry| entry.row.id).collect::<Vec<_>>();
        let units = C::shuffle_units(db, &ids)?;
        for entry in &mut entries {
            if let Some(&unit) = units.get(&entry.row.id) {
                entry.unit = unit;
            }
        }
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
            (KeyKind::Relevance, _) => direction.apply(
                b.score
                    .cmp(&a.score)
                    .then_with(|| b.by_name.cmp(&a.by_name)),
            ),
            // A unit keeps its place order whichever way the units run.
            (KeyKind::Random, _) => direction
                .apply(
                    shuffle(seed, a.unit.id)
                        .cmp(&shuffle(seed, b.unit.id))
                        .then_with(|| a.unit.id.0.cmp(&b.unit.id.0)),
                )
                .then_with(|| a.unit.place.cmp(&b.unit.place)),
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
        .sort_name()
        .cmp(b.row.sort_name())
        .then_with(|| a.row.lower_name().cmp(b.row.lower_name()))
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
            "Tokyo Story",
            "東京",
            "Tokyo",
            insert_track,
            set_track_sort,
        )?;
        let releases = insert_pair(
            &mut db,
            "Tokyo Story",
            "東京",
            "Tokyo",
            insert_release,
            set_release_sort,
        )?;
        let artists = insert_pair(
            &mut db,
            "Tokyo Story",
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
            assert_eq!(found, vec![by_name, by_sort]);
        }
        assert_eq!(
            search::<Tracks>(&db, Default::default(), "東京"),
            vec![tracks.1]
        );
        Ok(())
    }

    #[test]
    fn search_ranks_a_better_sort_name_match_first() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (weak, strong) = insert_pair(
            &mut db,
            "T o k y o",
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
