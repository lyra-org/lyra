// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The scanning pipeline the index replaced, reading every candidate from the database, kept to
//! check the index against. Search follows the index's rules, matched text by text.

mod artists;
mod compare;
mod genres;
mod releases;
mod tracks;

pub(super) use std::{
    cmp::Ordering,
    collections::{
        HashMap,
        HashSet,
    },
};

pub(super) use agdb::{
    DbAny,
    DbId,
    QueryBuilder,
};

pub(super) use super::{
    ArtistCredit,
    CatalogError,
    CreditRole,
    Direction,
    PersonalFilter,
    SortSpec,
    Viewer,
    artists::{
        ArtistFilter,
        ArtistKey,
        Artists,
    },
    genres::{
        GenreFilter,
        GenreKey,
        Genres,
    },
    pipeline::{
        Catalog,
        KeyKind,
        Query,
        ShuffleUnit,
        SortKey,
        shuffle,
    },
    releases::{
        ReleaseFilter,
        ReleaseKey,
        Releases,
    },
    tracks::{
        TrackFilter,
        TrackKey,
        Tracks,
    },
};
pub(super) use crate::services::search::{
    self,
    Needle,
    Tier,
};
pub(super) use lyra_db::{
    CreditType,
    Release,
    Track,
    favorites::FavoriteKind,
};

pub(crate) trait Old: Catalog {
    fn old_candidates(
        db: &DbAny,
        viewer: &Viewer,
        filter: &Self::Filter,
    ) -> Result<Vec<DbId>, CatalogError>;

    fn old_rows(
        db: &DbAny,
        viewer: &Viewer,
        filter: &Self::Filter,
        ids: Vec<DbId>,
        keys: &[Self::Key],
    ) -> Result<Vec<Row>, CatalogError>;

    /// The chains the random key keeps together: those on releases the viewer sees.
    fn old_shuffle_units(
        _db: &DbAny,
        _viewer: &Viewer,
        _ids: &[DbId],
    ) -> anyhow::Result<HashMap<DbId, ShuffleUnit>> {
        Ok(HashMap::new())
    }
}

trait OldCredit {
    fn matching(
        &self,
        db: &DbAny,
        reach: impl Fn(&[DbId], CreditRole) -> anyhow::Result<HashSet<DbId>>,
    ) -> anyhow::Result<HashSet<DbId>>;
}

impl OldCredit for ArtistCredit {
    /// The entities the filter keeps, given the entities each single role reaches from a set of
    /// existing artists.
    fn matching(
        &self,
        db: &DbAny,
        reach: impl Fn(&[DbId], CreditRole) -> anyhow::Result<HashSet<DbId>>,
    ) -> anyhow::Result<HashSet<DbId>> {
        let artists = lyra_db::graph::existing_ids(db, &self.artists, "Artist")?;
        let reach_role = |role| -> anyhow::Result<HashSet<DbId>> {
            match role {
                CreditRole::Any => {
                    let mut reached = reach(&artists, CreditRole::Track)?;
                    reached.extend(reach(&artists, CreditRole::Release)?);
                    Ok(reached)
                }
                role => reach(&artists, role),
            }
        };
        let mut matching = reach_role(self.role)?;
        if let Some(excluding) = self.excluding {
            let excluded = reach_role(excluding)?;
            matching.retain(|id| !excluded.contains(id));
        }
        Ok(matching)
    }
}

/// The releases in the viewer's libraries, or `None` when the viewer sees every release.
fn visible_releases(viewer: &Viewer, db: &DbAny) -> anyhow::Result<Option<HashSet<DbId>>> {
    let Some(principal) = viewer.restricted() else {
        return Ok(None);
    };
    let mut releases = HashSet::new();
    for library in lyra_db::libraries::db_ids_for_public_ids(db, &principal.accessible_library_ids)?
    {
        releases.extend(lyra_db::graph::neighbor_ids(db, library, "Release")?);
    }
    Ok(Some(releases))
}

/// Whether the viewer can see every one of `libraries`.
fn sees_libraries(viewer: &Viewer, db: &DbAny, libraries: &[DbId]) -> anyhow::Result<bool> {
    let Some(principal) = viewer.restricted() else {
        return Ok(true);
    };
    let accessible =
        lyra_db::libraries::db_ids_for_public_ids(db, &principal.accessible_library_ids)?;
    Ok(libraries.iter().all(|library| accessible.contains(library)))
}

/// The releases the viewer can see, narrowed to `library`, to `releases` and to releases in any
/// of `genres` when each is given. Ids that name nothing are dropped.
pub(crate) fn scoped_releases(
    db: &DbAny,
    viewer: &Viewer,
    library: Option<DbId>,
    releases: Option<&[DbId]>,
    genres: &[DbId],
) -> anyhow::Result<Candidates> {
    let mut scoped = Candidates::default();
    let libraries = match library {
        Some(library) => Some(lyra_db::graph::existing_ids(db, &[library], "Library")?),
        None => None,
    };
    // The visible releases are those of the viewer's libraries, so a scope of libraries the
    // viewer can see already lies within them.
    let within_visible = match &libraries {
        Some(libraries) => sees_libraries(viewer, db, libraries)?,
        None => false,
    };
    if !within_visible && let Some(visible) = visible_releases(viewer, db)? {
        scoped.restrict(visible);
    }
    // A library the viewer may not see scopes to nothing.
    if let Some(libraries) = &libraries
        && !sees_libraries(viewer, db, libraries)?
    {
        scoped.restrict(Vec::new());
    }
    if let Some(libraries) = libraries {
        let mut library_releases = Vec::new();
        for library in libraries {
            library_releases.extend(lyra_db::graph::neighbor_ids(db, library, "Release")?);
        }
        scoped.restrict(library_releases);
    }
    if let Some(releases) = releases {
        scoped.restrict(lyra_db::graph::existing_ids(db, releases, "Release")?);
    }
    if !genres.is_empty() {
        let genres = lyra_db::graph::existing_ids(db, genres, "Genre")?;
        scoped.restrict(lyra_db::genres::release_ids_matching_genre_ids(
            db, &genres,
        )?);
    }
    Ok(scoped)
}

pub(crate) fn all_releases(db: &DbAny) -> anyhow::Result<Vec<DbId>> {
    lyra_db::graph::neighbor_ids(db, "releases", "Release")
}

/// The owners of kind `Owner` that credit any of `artists`.
pub(crate) fn credited_owners<Owner: agdb::DbType>(
    db: &DbAny,
    artists: &[DbId],
) -> anyhow::Result<HashSet<DbId>> {
    let mut owners = HashSet::new();
    for artist in artists {
        owners.extend(lyra_db::credits::owner_ids_by_artist::<Owner>(
            db, *artist, 0, 0,
        )?);
    }
    Ok(owners)
}

pub(crate) fn tracks_of_releases(
    db: &DbAny,
    releases: impl IntoIterator<Item = DbId>,
) -> anyhow::Result<HashSet<DbId>> {
    let mut tracks = HashSet::new();
    for release in releases {
        tracks.extend(lyra_db::graph::neighbor_ids(db, release, "Track")?);
    }
    Ok(tracks)
}

pub(crate) fn releases_of_tracks(
    db: &DbAny,
    tracks: impl IntoIterator<Item = DbId>,
) -> anyhow::Result<HashSet<DbId>> {
    let mut releases = HashSet::new();
    for track in tracks {
        releases.extend(lyra_db::graph::inbound_neighbor_ids(db, track, "Release")?);
    }
    Ok(releases)
}

/// What an entity's tracks add up to.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TrackTotals {
    pub(crate) track_count: u64,
    pub(crate) total_duration: u64,
    pub(crate) listen_count: u64,
    pub(crate) last_played_at: Option<u64>,
}

/// Totals over each owner's tracks. Durations and listens are read only when asked for.
pub(crate) fn track_totals(
    db: &DbAny,
    viewer: &Viewer,
    tracks_by_owner: &HashMap<DbId, HashSet<DbId>>,
    durations: bool,
    listens: bool,
) -> Result<HashMap<DbId, TrackTotals>, CatalogError> {
    let tracks = tracks_by_owner
        .values()
        .flatten()
        .copied()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let durations = if durations {
        lyra_db::graph::select_fields(db, &tracks, &["duration_ms"])?
            .into_iter()
            .filter_map(|(id, fields)| Some((id, fields.number("duration_ms")?)))
            .collect()
    } else {
        HashMap::new()
    };
    let listens = if listens {
        let user = viewer.user_db_id("listen sort keys")?;
        lyra_db::listens::get_stats(db, &tracks, user)?
            .into_iter()
            .map(|stats| (stats.db_id, stats))
            .collect()
    } else {
        HashMap::new()
    };
    Ok(tracks_by_owner
        .iter()
        .map(|(owner, tracks)| {
            let mut totals = TrackTotals {
                track_count: tracks.len() as u64,
                ..TrackTotals::default()
            };
            for track in tracks {
                if let Some(duration) = durations.get(track) {
                    totals.total_duration = totals.total_duration.saturating_add(*duration);
                }
                if let Some(stats) = listens.get(track) {
                    totals.listen_count = totals.listen_count.saturating_add(stats.count);
                    totals.last_played_at = totals.last_played_at.max(stats.last_played);
                }
            }
            (*owner, totals)
        })
        .collect())
}

/// The viewer's ratings of `ids`, read only when `wanted`.
pub(crate) fn ratings(
    db: &DbAny,
    viewer: &Viewer,
    ids: &[DbId],
    wanted: bool,
) -> Result<HashMap<DbId, lyra_db::ratings::RatingEdge>, CatalogError> {
    if !wanted {
        return Ok(HashMap::new());
    }
    let user = viewer.user_db_id("rating sort keys")?;
    Ok(lyra_db::ratings::values_for_targets(db, user, ids)?)
}

/// When the viewer first favorited each favorite of `kind`, read only when `wanted`.
fn favorited_at(
    db: &DbAny,
    viewer: &Viewer,
    ids: &[DbId],
    kind: FavoriteKind,
    wanted: bool,
) -> Result<HashMap<DbId, i64>, CatalogError> {
    if !wanted {
        return Ok(HashMap::new());
    }
    let user = viewer.user_db_id("the favorited_at sort key")?;
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    Ok(lyra_db::favorites::first_favorited_at(db, user, kind)?)
}

/// A stored Unix-millisecond timestamp as a sort value.
fn timestamp_value(ms: i64) -> Option<SortValue> {
    u64::try_from(ms).ok().map(SortValue::Number)
}

/// The rating filter as the scanning pipeline applied it.
fn restrict_rating(
    db: &DbAny,
    viewer: &Viewer,
    rating: lyra_db::ratings::RatingFilter,
    candidates: &mut Candidates,
    all: impl FnOnce() -> anyhow::Result<Vec<DbId>>,
) -> Result<(), CatalogError> {
    use lyra_db::ratings::{
        RatingBounds,
        RatingFilter,
    };
    match rating {
        RatingFilter::Any => {}
        RatingFilter::Rated(bounds) => {
            let user = viewer.user_db_id("the rating filter")?;
            candidates.restrict(lyra_db::ratings::rated_target_ids(db, user, bounds)?);
        }
        RatingFilter::Unrated => {
            let user = viewer.user_db_id("the rating filter")?;
            let rated = lyra_db::ratings::rated_target_ids(db, user, RatingBounds::default())?;
            candidates.restrict_membership(rated, false, all)?;
        }
    }
    Ok(())
}

/// A candidate id set built by intersection, where `None` stands for every entity.
#[derive(Default)]
pub(crate) struct Candidates(Option<HashSet<DbId>>);

impl Candidates {
    pub(crate) fn restrict(&mut self, ids: impl IntoIterator<Item = DbId>) {
        let ids = ids.into_iter();
        self.0 = Some(match self.0.take() {
            Some(current) => {
                let ids = ids.collect::<HashSet<_>>();
                current.into_iter().filter(|id| ids.contains(id)).collect()
            }
            None => ids.collect(),
        });
    }

    pub(crate) fn ids(&self) -> Option<&HashSet<DbId>> {
        self.0.as_ref()
    }

    /// Keeps the ids in `ids` when `wanted` is true, and the others when it is false.
    pub(crate) fn restrict_membership(
        &mut self,
        ids: HashSet<DbId>,
        wanted: bool,
        all: impl FnOnce() -> anyhow::Result<Vec<DbId>>,
    ) -> anyhow::Result<()> {
        if wanted {
            self.restrict(ids);
            return Ok(());
        }
        let remaining = match &self.0 {
            Some(current) => current
                .iter()
                .copied()
                .filter(|id| !ids.contains(id))
                .collect(),
            None => all()?.into_iter().filter(|id| !ids.contains(id)).collect(),
        };
        self.0 = Some(remaining);
        Ok(())
    }

    /// The final ids, reading every entity through `all` when nothing restricted them.
    pub(crate) fn resolve(
        self,
        exclude: &[DbId],
        all: impl FnOnce() -> anyhow::Result<Vec<DbId>>,
    ) -> anyhow::Result<Vec<DbId>> {
        let exclude = exclude.iter().copied().collect::<HashSet<_>>();
        let ids = match self.0 {
            Some(ids) => ids.into_iter().collect(),
            None => all()?,
        };
        Ok(ids.into_iter().filter(|id| !exclude.contains(id)).collect())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SortValue {
    Number(u64),
    Text(String),
}

/// A candidate's names and the values of the requested field keys, in key order.
#[derive(Clone)]
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

/// The texts a search reaches beyond an entity's names: an artist's or genre's aliases, a
/// release's artists, and a track's artists, release and release artists.
fn related_texts(db: &DbAny, viewer: &Viewer, kind: &str, id: DbId) -> anyhow::Result<Vec<String>> {
    let artist_texts = |owners: &[DbId]| -> anyhow::Result<Vec<String>> {
        let mut texts = Vec::new();
        for artist in lyra_db::credits::credited_artist_ids(db, owners)? {
            if !viewer.sees_artist(db, artist)? {
                continue;
            }
            if let Some(artist) = lyra_db::artists::get_by_id(db, artist)? {
                texts.push(artist.artist_name);
                texts.extend(artist.sort_name);
                texts.extend(artist.scan_aliases);
            }
        }
        Ok(texts)
    };
    Ok(match kind {
        "Track" => {
            let releases = lyra_db::graph::inbound_neighbor_ids(db, id, "Release")?
                .into_iter()
                .map(|release| Ok((release, viewer.sees_entity(db, release)?)))
                .filter_map(|seen: anyhow::Result<_>| match seen {
                    Ok((release, true)) => Some(Ok(release)),
                    Ok((_, false)) => None,
                    Err(error) => Some(Err(error)),
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            let mut texts = artist_texts(&[id])?;
            texts.extend(artist_texts(&releases)?);
            for release in releases {
                if let Some(release) = lyra_db::releases::get_by_id(db, release)? {
                    texts.push(release.release_title);
                    texts.extend(release.sort_title);
                }
            }
            texts
        }
        "Release" => artist_texts(&[id])?,
        "Artist" => lyra_db::artists::get_by_id(db, id)?
            .map(|artist| artist.scan_aliases)
            .unwrap_or_default(),
        "Genre" => db
            .exec(
                QueryBuilder::select()
                    .search()
                    .from(id)
                    .where_()
                    .neighbor()
                    .and()
                    .key("db_element_id")
                    .value("GenreAlias")
                    .query(),
            )?
            .elements
            .iter()
            .filter_map(|element| {
                element
                    .values
                    .iter()
                    .find(|kv| kv.key == "name".into())
                    .and_then(|kv| kv.value.string().ok().cloned())
            })
            .collect(),
        _ => Vec::new(),
    })
}

/// Whether a release is among the `visible` ones, where `None` stands for every release.
fn seen(visible: &Option<HashSet<DbId>>, release: DbId) -> bool {
    visible
        .as_ref()
        .is_none_or(|visible| visible.contains(&release))
}

/// How `row` matches `needle`, matched text by text: the best tier its name or sort name
/// reaches, or [`Tier::Related`] when the words match only across all its texts.
fn tier(
    db: &DbAny,
    viewer: &Viewer,
    kind: &str,
    needle: &Needle,
    row: &Row,
    sort_title: Option<&str>,
    related: &mut HashMap<DbId, Vec<String>>,
) -> anyhow::Result<Option<(Tier, usize, bool)>> {
    let words = needle.words().len();
    let sort_title = sort_title.filter(|sort| search::fold(sort) != search::fold(&row.name));
    let name = needle.score(&row.name);
    let sort_name = sort_title.and_then(|sort| needle.score(sort));
    if let Some(best) = name.max(sort_name) {
        return Ok(Some((best, words, name == Some(best))));
    }
    if words == 0 {
        return Ok(None);
    }
    let holds = |text: &str, word: &search::Word| {
        let mut found = false;
        search::words(&search::fold(text), |text_word| {
            found = found || word.matches(text_word);
        });
        found
    };
    let mut own = vec![row.name.clone()];
    own.extend(sort_title.map(str::to_string));
    let related = match related.entry(row.id) {
        std::collections::hash_map::Entry::Occupied(texts) => texts.into_mut(),
        std::collections::hash_map::Entry::Vacant(slot) => {
            slot.insert(related_texts(db, viewer, kind, row.id)?)
        }
    };
    let mut own_words = 0;
    for word in needle.words() {
        if own.iter().any(|text| holds(text, word)) {
            own_words += 1;
        } else if !related.iter().any(|text| holds(text, word)) {
            return Ok(None);
        }
    }
    let tier = if own_words > 0 {
        Tier::Partial
    } else {
        Tier::Related
    };
    Ok(Some((tier, own_words, false)))
}

struct Entry {
    row: Row,
    unit: ShuffleUnit,
    tier: Tier,
    own_words: usize,
    by_name: bool,
}

/// Every matching id in order, as the scanning pipeline found them.
/// What the pipeline read for one viewer and filter, kept across queries that differ only in
/// search and sort, since the catalog does not change between them.
#[derive(Default)]
pub(crate) struct Reads {
    candidates: Option<Vec<DbId>>,
    rows: HashMap<Vec<usize>, Vec<Row>>,
    related: HashMap<DbId, Vec<String>>,
}

/// `reads` must come from queries for the same viewer and filter.
pub(crate) fn order<C: Old>(
    db: &DbAny,
    viewer: &Viewer,
    query: &Query<C>,
    kind: &str,
    reads: &mut Reads,
) -> Result<Vec<DbId>, CatalogError> {
    let needle = query.search.as_deref().and_then(Needle::parse);
    let sort = if !query.sort.is_empty() {
        query.sort.clone()
    } else if needle.is_some() {
        vec![(C::Key::RELEVANCE, Direction::Ascending)]
    } else {
        C::default_sort(&query.filter)
    };
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
    let ids = match &reads.candidates {
        Some(ids) => ids.clone(),
        None => reads
            .candidates
            .insert(C::old_candidates(db, viewer, &query.filter)?)
            .clone(),
    };
    let positions = fields.iter().map(|key| key.position()).collect::<Vec<_>>();
    let rows = match reads.rows.get(&positions) {
        Some(rows) => rows.clone(),
        None => {
            let rows = C::old_rows(db, viewer, &query.filter, ids, &fields)?;
            reads.rows.insert(positions, rows.clone());
            rows
        }
    };
    let related = &mut reads.related;
    let mut entries = Vec::new();
    for row in rows {
        if !query.names.contains(row.sort_name()) {
            continue;
        }
        let (tier, own_words, by_name) = match &needle {
            Some(needle) => {
                let sort_title = row.sort_title.clone();
                match tier(
                    db,
                    viewer,
                    kind,
                    needle,
                    &row,
                    sort_title.as_deref(),
                    related,
                )? {
                    Some(found) => found,
                    None => continue,
                }
            }
            None => (Tier::Related, 0, false),
        };
        entries.push(Entry {
            unit: ShuffleUnit {
                id: row.id,
                place: 0,
            },
            row,
            tier,
            own_words,
            by_name,
        });
    }
    if sort.iter().any(|(key, _)| key.kind() == KeyKind::Random) {
        let ids = entries.iter().map(|entry| entry.row.id).collect::<Vec<_>>();
        let units = C::old_shuffle_units(db, viewer, &ids)?;
        for entry in &mut entries {
            if let Some(&unit) = units.get(&entry.row.id) {
                entry.unit = unit;
            }
        }
    }
    entries.sort_by(|a, b| compare(a, b, &sort, &columns, query.seed));
    Ok(entries.into_iter().map(|entry| entry.row.id).collect())
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
                b.tier
                    .cmp(&a.tier)
                    .then_with(|| b.own_words.cmp(&a.own_words))
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
