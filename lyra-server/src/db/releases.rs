// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::cmp::Ordering;
use std::collections::{
    HashMap,
    HashSet,
};

use agdb::{
    Comparison,
    CountComparison,
    DbAny,
    DbElement,
    DbError,
    DbId,
    DbTypeMarker,
    DbValue,
    KeyValueComparison,
    QueryBuilder,
    QueryCondition,
    QueryConditionData,
    QueryConditionLogic,
    QueryConditionModifier,
    QueryId,
    QueryIds,
};
use schemars::JsonSchema;
use serde::{
    Deserialize,
    Serialize,
};

use super::NodeId;
use super::{
    ListOptions,
    PagedResult,
    SortKey,
    SortSpec,
    apply_direction,
    compare_option,
};

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    JsonSchema,
    DbTypeMarker,
)]
#[serde(rename_all = "lowercase")]
#[harmony_macros::enumeration]
pub(crate) enum ReleaseType {
    Album,
    Single,
    #[serde(rename = "ep")]
    EP,
    Compilation,
    Soundtrack,
    Live,
    Remix,
    Broadcast,
    Other,
    #[default]
    Unknown,
}

impl ReleaseType {
    fn as_db_str(self) -> &'static str {
        match self {
            Self::Album => "album",
            Self::Single => "single",
            Self::EP => "ep",
            Self::Compilation => "compilation",
            Self::Soundtrack => "soundtrack",
            Self::Live => "live",
            Self::Remix => "remix",
            Self::Broadcast => "broadcast",
            Self::Other => "other",
            Self::Unknown => "unknown",
        }
    }

    pub(crate) fn from_db_str(value: &str) -> Result<Self, DbError> {
        match value {
            "album" => Ok(Self::Album),
            "single" => Ok(Self::Single),
            "ep" => Ok(Self::EP),
            "compilation" => Ok(Self::Compilation),
            "soundtrack" => Ok(Self::Soundtrack),
            "live" => Ok(Self::Live),
            "remix" => Ok(Self::Remix),
            "broadcast" => Ok(Self::Broadcast),
            "other" => Ok(Self::Other),
            "unknown" => Ok(Self::Unknown),
            _ => Err(DbError::from(format!(
                "invalid ReleaseType value '{value}'"
            ))),
        }
    }
}

impl std::fmt::Display for ReleaseType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_db_str())
    }
}

impl From<ReleaseType> for DbValue {
    fn from(value: ReleaseType) -> Self {
        Self::from(value.as_db_str())
    }
}

impl From<&ReleaseType> for DbValue {
    fn from(value: &ReleaseType) -> Self {
        (*value).into()
    }
}

impl TryFrom<DbValue> for ReleaseType {
    type Error = DbError;

    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        Self::from_db_str(value.string()?)
    }
}

harmony_macros::compile!(type_path = ReleaseType, variants = true);

#[derive(DbElement, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[harmony_macros::structure]
pub(crate) struct Release {
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) release_title: String,
    pub(crate) sort_title: Option<String>,
    pub(crate) release_type: Option<ReleaseType>,
    pub(crate) release_date: Option<String>,
    pub(crate) locked: Option<bool>,
    pub(crate) created_at: Option<u64>,
    pub(crate) ctime: Option<u64>,
}

#[harmony_macros::implementation]
impl Release {
    pub(crate) fn set_release_title(&mut self, release_title: String) {
        self.release_title = release_title;
    }

    pub(crate) fn set_sort_title(&mut self, sort_title: String) {
        self.sort_title = Some(sort_title);
    }

    pub(crate) fn set_release_date(&mut self, release_date: String) {
        self.release_date = normalize_release_date(&release_date);
    }
}

harmony_macros::compile!(type_path = Release, fields = true, methods = true);

pub(crate) fn normalize_release_date(value: &str) -> Option<String> {
    let value = value.trim();
    match value.len() {
        4 if valid_year(value) => Some(value.to_string()),
        7 if valid_year_month(value) => Some(value.to_string()),
        10 if valid_year_month_day(value) => Some(value.to_string()),
        _ => None,
    }
}

pub(crate) fn release_year(release_date: Option<&str>) -> Option<u32> {
    release_date?.get(..4)?.parse::<u32>().ok()
}

fn valid_year(value: &str) -> bool {
    value.as_bytes().iter().all(u8::is_ascii_digit)
}

fn valid_year_month(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.get(4) == Some(&b'-')
        && valid_year(&value[..4])
        && value[5..7].as_bytes().iter().all(u8::is_ascii_digit)
        && value[5..7]
            .parse::<u32>()
            .is_ok_and(|month| (1..=12).contains(&month))
}

fn valid_year_month_day(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.get(7) != Some(&b'-') || !valid_year_month(&value[..7]) {
        return false;
    }
    if !value[8..10].as_bytes().iter().all(u8::is_ascii_digit) {
        return false;
    }

    let Ok(year) = value[..4].parse::<u32>() else {
        return false;
    };
    let Ok(month) = value[5..7].parse::<u32>() else {
        return false;
    };
    let Ok(day) = value[8..10].parse::<u32>() else {
        return false;
    };

    (1..=days_in_month(year, month)).contains(&day)
}

fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: u32) -> bool {
    (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400)
}

pub(crate) fn get(db: &DbAny, from: impl Into<QueryId>) -> anyhow::Result<Vec<Release>> {
    let releases: Vec<Release> = db
        .exec(
            QueryBuilder::select()
                .elements::<Release>()
                .search()
                .from(from)
                .query(),
        )?
        .try_into()?;

    Ok(releases)
}

pub(crate) fn get_direct(db: &DbAny, from: impl Into<QueryId>) -> anyhow::Result<Vec<Release>> {
    let releases: Vec<Release> = db
        .exec(
            QueryBuilder::select()
                .elements::<Release>()
                .search()
                .from(from)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    Ok(releases)
}

#[derive(Default, Clone, Debug)]
pub(crate) struct ReleaseQueryFilters {
    pub year: Option<u32>,
    pub ids: Option<HashSet<DbId>>,
}

impl ReleaseQueryFilters {
    fn is_empty(&self) -> bool {
        self.year.is_none() && self.ids.is_none()
    }
}

fn extra_condition(data: QueryConditionData) -> QueryCondition {
    QueryCondition {
        logic: QueryConditionLogic::And,
        modifier: QueryConditionModifier::None,
        data,
    }
}

fn get_direct_filtered(
    db: &DbAny,
    from: impl Into<QueryId>,
    filters: &ReleaseQueryFilters,
) -> anyhow::Result<Vec<Release>> {
    if filters.is_empty() {
        return get_direct(db, from);
    }

    let mut query = QueryBuilder::select()
        .elements::<Release>()
        .search()
        .from(from)
        .where_()
        .neighbor()
        .end_where()
        .query();

    if let QueryIds::Search(search) = &mut query.ids {
        if let Some(year) = filters.year {
            search
                .conditions
                .push(extra_condition(QueryConditionData::KeyValue(
                    KeyValueComparison {
                        key: DbValue::from("release_date"),
                        value: Comparison::StartsWith(DbValue::from(format!("{year:04}"))),
                    },
                )));
        }
        if let Some(ref ids) = filters.ids {
            search
                .conditions
                .push(extra_condition(QueryConditionData::Ids(
                    ids.iter().map(|id| QueryId::Id(*id)).collect(),
                )));
        }
    }

    let releases: Vec<Release> = db.exec(&query)?.try_into()?;
    Ok(releases)
}

pub(crate) fn get_by_id(
    db: &impl super::DbAccess,
    release_db_id: DbId,
) -> anyhow::Result<Option<Release>> {
    super::graph::fetch_typed_by_id(db, release_db_id, "Release")
}

pub(crate) fn get_by_artist(db: &DbAny, artist_db_id: DbId) -> anyhow::Result<Vec<Release>> {
    // Walk: Artist ← Credit (neighbor) ← Release (neighbor of credit).
    let credits: Vec<super::Credit> = db
        .exec(
            QueryBuilder::select()
                .elements::<super::Credit>()
                .search()
                .to(artist_db_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    if credits.is_empty() {
        return Ok(Vec::new());
    }

    // Collect all owner IDs first, then batch-fetch Release data.
    let mut owner_ids = Vec::new();
    let mut seen_owners = HashSet::new();
    for credit in &credits {
        let Some(credit_db_id) = credit.db_id.clone().map(DbId::from) else {
            continue;
        };
        let incoming: Vec<DbId> = db
            .exec(
                QueryBuilder::search()
                    .to(credit_db_id)
                    .where_()
                    .edge()
                    .and()
                    .distance(agdb::CountComparison::Equal(1))
                    .query(),
            )?
            .elements
            .iter()
            .filter_map(|e| e.from.filter(|id| id.0 > 0))
            .collect();
        for owner_id in incoming {
            if seen_owners.insert(owner_id) {
                owner_ids.push(owner_id);
            }
        }
    }

    if owner_ids.is_empty() {
        return Ok(Vec::new());
    }

    let releases_by_id: HashMap<DbId, Release> =
        super::graph::bulk_fetch_typed(db, owner_ids, "Release")?;

    Ok(releases_by_id.into_values().collect())
}

pub(crate) fn get_by_artists(db: &DbAny, artist_db_ids: &[DbId]) -> anyhow::Result<Vec<Release>> {
    let mut releases = Vec::new();
    let mut seen = HashSet::new();

    for artist_db_id in artist_db_ids {
        for release in get_by_artist(db, *artist_db_id)? {
            if let Some(release_db_id) = release.db_id.clone().map(DbId::from) {
                if seen.insert(release_db_id) {
                    releases.push(release);
                }
                continue;
            }
            releases.push(release);
        }
    }

    Ok(releases)
}

pub(crate) fn get_appearances(db: &DbAny, artist_db_id: DbId) -> anyhow::Result<Vec<Release>> {
    let album_artist_releases = get_by_artist(db, artist_db_id)?;
    let album_artist_ids: HashSet<DbId> = album_artist_releases
        .iter()
        .filter_map(|r| r.db_id.clone().map(DbId::from))
        .collect();

    let tracks = super::tracks::get_by_artist(db, artist_db_id)?;
    let track_db_ids: Vec<DbId> = tracks
        .iter()
        .filter_map(|t| t.db_id.clone().map(DbId::from))
        .collect();

    let releases_by_track = get_by_tracks(db, &track_db_ids)?;
    let mut seen = HashSet::new();
    let mut appears_on = Vec::new();

    for releases in releases_by_track.into_values() {
        for release in releases {
            let Some(release_db_id) = release.db_id.clone().map(DbId::from) else {
                continue;
            };
            if album_artist_ids.contains(&release_db_id) {
                continue;
            }
            if seen.insert(release_db_id) {
                appears_on.push(release);
            }
        }
    }

    Ok(appears_on)
}

pub(crate) fn get_by_track(
    db: &impl super::DbAccess,
    track_db_id: DbId,
) -> anyhow::Result<Vec<Release>> {
    let releases: Vec<Release> = db
        .exec(
            QueryBuilder::select()
                .elements::<Release>()
                .search()
                .to(track_db_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    Ok(releases)
}

pub(crate) fn get_by_tracks(
    db: &DbAny,
    track_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Vec<Release>>> {
    const SMALL_TRACK_BATCH_THRESHOLD: usize = 32;

    let mut unique_track_db_ids = Vec::new();
    let mut seen_track_db_ids = HashSet::new();
    for track_db_id in track_db_ids {
        if track_db_id.0 <= 0 {
            continue;
        }
        if seen_track_db_ids.insert(*track_db_id) {
            unique_track_db_ids.push(*track_db_id);
        }
    }

    let mut related: HashMap<DbId, Vec<Release>> = unique_track_db_ids
        .iter()
        .copied()
        .map(|track_db_id| (track_db_id, Vec::new()))
        .collect();
    if unique_track_db_ids.is_empty() {
        return Ok(related);
    }

    if unique_track_db_ids.len() <= SMALL_TRACK_BATCH_THRESHOLD {
        for track_db_id in unique_track_db_ids {
            let track_releases = get_by_track(db, track_db_id)?;
            let Some(owner_related) = related.get_mut(&track_db_id) else {
                continue;
            };
            let mut seen_release_db_ids = HashSet::new();
            for release in track_releases {
                let Some(release_db_id) = release.db_id.clone().map(DbId::from) else {
                    owner_related.push(release);
                    continue;
                };
                if seen_release_db_ids.insert(release_db_id) {
                    owner_related.push(release);
                }
            }
        }
        return Ok(related);
    }

    fn append_related_id(
        related_ids_by_track: &mut HashMap<DbId, Vec<DbId>>,
        seen_related_ids: &mut HashSet<DbId>,
        all_related_ids: &mut Vec<DbId>,
        track_db_id: DbId,
        related_id: DbId,
    ) {
        if related_id.0 <= 0 || related_id == track_db_id {
            return;
        }
        let Some(track_related_ids) = related_ids_by_track.get_mut(&track_db_id) else {
            return;
        };
        if track_related_ids.contains(&related_id) {
            return;
        }
        track_related_ids.push(related_id);
        if seen_related_ids.insert(related_id) {
            all_related_ids.push(related_id);
        }
    }

    let mut related_ids_by_track: HashMap<DbId, Vec<DbId>> = unique_track_db_ids
        .iter()
        .copied()
        .map(|track_db_id| (track_db_id, Vec::new()))
        .collect();
    let mut all_related_ids = Vec::new();
    let mut seen_related_ids = HashSet::new();
    let track_db_id_set: HashSet<DbId> = unique_track_db_ids.iter().copied().collect();

    // Fast path: collect release relation edges in one traversal from the releases root.
    // This avoids one DB search per track for large batches.
    if let Ok(edges) = db.exec(
        QueryBuilder::search()
            .from("releases")
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(3))
            .query(),
    ) {
        for edge in edges.elements {
            let (Some(from), Some(to)) = (edge.from, edge.to) else {
                continue;
            };
            if track_db_id_set.contains(&from) {
                append_related_id(
                    &mut related_ids_by_track,
                    &mut seen_related_ids,
                    &mut all_related_ids,
                    from,
                    to,
                );
            } else if track_db_id_set.contains(&to) {
                append_related_id(
                    &mut related_ids_by_track,
                    &mut seen_related_ids,
                    &mut all_related_ids,
                    to,
                    from,
                );
            }
        }
    }

    // Fallback: resolve any unresolved tracks directly so mixed/alternate
    // edge directions still work.
    for track_db_id in &unique_track_db_ids {
        if related_ids_by_track
            .get(track_db_id)
            .is_some_and(|track_related_ids| !track_related_ids.is_empty())
        {
            continue;
        }

        // Bounded one-hop fallback from the track in both directions to avoid
        // whole-graph traversal while still handling mixed edge directions.
        let forward_edges = db.exec(
            QueryBuilder::search()
                .from(*track_db_id)
                .where_()
                .not_beyond()
                .distance(CountComparison::Equal(1))
                .and()
                .edge()
                .query(),
        )?;
        let reverse_edges = db.exec(
            QueryBuilder::search()
                .to(*track_db_id)
                .where_()
                .not_beyond()
                .distance(CountComparison::Equal(1))
                .and()
                .edge()
                .query(),
        )?;

        for edges in [forward_edges, reverse_edges] {
            for edge in edges.elements {
                let related_id = match (edge.from, edge.to) {
                    (Some(from), Some(to)) if from == *track_db_id => Some(to),
                    (Some(from), Some(to)) if to == *track_db_id => Some(from),
                    _ => None,
                };
                let Some(related_id) = related_id else {
                    continue;
                };
                append_related_id(
                    &mut related_ids_by_track,
                    &mut seen_related_ids,
                    &mut all_related_ids,
                    *track_db_id,
                    related_id,
                );
            }
        }
    }

    if all_related_ids.is_empty() {
        return Ok(related);
    }

    let releases_by_id: HashMap<DbId, Release> =
        super::graph::bulk_fetch_typed(db, all_related_ids, "Release")?;

    for track_db_id in unique_track_db_ids {
        let Some(owner_related) = related.get_mut(&track_db_id) else {
            continue;
        };
        let Some(track_related_ids) = related_ids_by_track.remove(&track_db_id) else {
            continue;
        };
        for related_id in track_related_ids {
            if let Some(release) = releases_by_id.get(&related_id) {
                owner_related.push(release.clone());
            }
        }
    }

    Ok(related)
}

pub(crate) fn update(db: &mut impl super::DbAccess, release: &Release) -> anyhow::Result<()> {
    db.exec_mut(QueryBuilder::insert().element(release).query())?;
    Ok(())
}

#[derive(Clone)]
struct ReleaseSortEntry {
    release: Release,
    lower_title: String,
    lower_sort_title: Option<String>,
    db_id: Option<i64>,
    release_date: Option<String>,
    date_created: Option<u64>,
    match_score: u32,
}

impl ReleaseSortEntry {
    fn new(release: Release) -> Self {
        Self {
            lower_title: release.release_title.to_lowercase(),
            lower_sort_title: release
                .sort_title
                .as_ref()
                .map(|value| value.to_lowercase()),
            db_id: release.db_id.as_ref().map(|id| DbId::from(id.clone()).0),
            release_date: release.release_date.clone(),
            date_created: release.ctime.or(release.created_at),
            release,
            match_score: 0,
        }
    }
}

fn compare_release_field(a: &ReleaseSortEntry, b: &ReleaseSortEntry, key: SortKey) -> Ordering {
    match key {
        SortKey::SortName => a
            .lower_sort_title
            .as_deref()
            .unwrap_or(a.lower_title.as_str())
            .cmp(
                b.lower_sort_title
                    .as_deref()
                    .unwrap_or(b.lower_title.as_str()),
            ),
        SortKey::Name => a.lower_title.cmp(&b.lower_title),
        SortKey::DateCreated => compare_option(&a.date_created, &b.date_created),
        SortKey::ReleaseDate => compare_option(&a.release_date, &b.release_date),
        SortKey::DbId => compare_option(&a.db_id, &b.db_id),
        SortKey::TrackNumber | SortKey::DiscNumber | SortKey::Duration => Ordering::Equal,
    }
}

fn compare_release_entries(
    a: &ReleaseSortEntry,
    b: &ReleaseSortEntry,
    sort: &[SortSpec],
) -> Ordering {
    for spec in sort {
        let ord = apply_direction(compare_release_field(a, b, spec.key), spec.direction);
        if ord != Ordering::Equal {
            return ord;
        }
    }

    let score_ord = b.match_score.cmp(&a.match_score);
    if score_ord != Ordering::Equal {
        return score_ord;
    }

    let name_ord = a.lower_title.cmp(&b.lower_title);
    if name_ord != Ordering::Equal {
        return name_ord;
    }

    compare_option(&a.db_id, &b.db_id)
}

fn u64_to_usize_saturating(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

fn paginate_releases(mut releases: Vec<Release>, options: &ListOptions) -> PagedResult<Release> {
    let total_count = releases.len() as u64;
    let offset = options.offset.unwrap_or(0).min(total_count);
    let offset = u64_to_usize_saturating(offset).min(releases.len());
    let limit = options.limit.map(u64_to_usize_saturating);

    let entries = match limit {
        Some(limit) => releases.drain(offset..).take(limit).collect(),
        None => releases.drain(offset..).collect(),
    };

    PagedResult {
        entries,
        total_count,
        offset: offset as u64,
    }
}

fn sort_and_paginate_releases(
    mut entries: Vec<ReleaseSortEntry>,
    options: &ListOptions,
) -> PagedResult<Release> {
    let total_count = entries.len() as u64;
    let offset = options.offset.unwrap_or(0).min(total_count);
    let offset = u64_to_usize_saturating(offset).min(entries.len());
    let limit = options.limit.map(u64_to_usize_saturating);

    if entries.is_empty() {
        return PagedResult {
            entries: Vec::new(),
            total_count,
            offset: offset as u64,
        };
    }

    if options.sort.is_empty() {
        let entries = match limit {
            Some(limit) => entries
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|entry| entry.release)
                .collect(),
            None => entries
                .into_iter()
                .skip(offset)
                .map(|entry| entry.release)
                .collect(),
        };

        return PagedResult {
            entries,
            total_count,
            offset: offset as u64,
        };
    }

    let page_end = match limit {
        Some(limit) => offset.saturating_add(limit).min(entries.len()),
        None => entries.len(),
    };
    if page_end == 0 || offset >= page_end {
        return PagedResult {
            entries: Vec::new(),
            total_count,
            offset: offset as u64,
        };
    }

    if page_end < entries.len() {
        let pivot = page_end - 1;
        entries.select_nth_unstable_by(pivot, |a, b| compare_release_entries(a, b, &options.sort));
        entries.truncate(page_end);
    }

    entries.sort_by(|a, b| compare_release_entries(a, b, &options.sort));
    PagedResult {
        entries: match limit {
            Some(limit) => entries
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|entry| entry.release)
                .collect(),
            None => entries
                .into_iter()
                .skip(offset)
                .map(|entry| entry.release)
                .collect(),
        },
        total_count,
        offset: offset as u64,
    }
}

pub(crate) fn query(
    db: &DbAny,
    from: impl Into<QueryId>,
    options: &ListOptions,
    filters: &ReleaseQueryFilters,
) -> anyhow::Result<PagedResult<Release>> {
    let releases = get_direct_filtered(db, from, filters)?;

    if options.search_term.is_none() && options.sort.is_empty() {
        return Ok(paginate_releases(releases, options));
    }

    let mut entries: Vec<ReleaseSortEntry> =
        releases.into_iter().map(ReleaseSortEntry::new).collect();

    if let Some(ref term) = options.search_term {
        super::search::fuzzy_filter(
            &mut entries,
            term,
            |entry| entry.release.release_title.as_str(),
            |entry, score| entry.match_score = score,
        );
    }

    Ok(sort_and_paginate_releases(entries, options))
}

pub(crate) fn query_by_artists(
    db: &DbAny,
    artist_db_ids: &[DbId],
    scope: Option<QueryId>,
    options: &ListOptions,
    filters: &ReleaseQueryFilters,
) -> anyhow::Result<PagedResult<Release>> {
    let mut releases = get_by_artists(db, artist_db_ids)?;
    if let Some(scope) = scope {
        let scoped_ids: HashSet<DbId> = get_direct(db, scope)?
            .into_iter()
            .filter_map(|release| release.db_id.map(DbId::from))
            .collect();
        releases.retain(|release| {
            release
                .db_id
                .clone()
                .map(DbId::from)
                .is_some_and(|release_db_id| scoped_ids.contains(&release_db_id))
        });
    }

    if let Some(year) = filters.year {
        releases.retain(|release| release_year(release.release_date.as_deref()) == Some(year));
    }
    if let Some(ref id_set) = filters.ids {
        releases.retain(|release| {
            release
                .db_id
                .clone()
                .map(DbId::from)
                .is_some_and(|id| id_set.contains(&id))
        });
    }

    if options.search_term.is_none() && options.sort.is_empty() {
        return Ok(paginate_releases(releases, options));
    }

    let mut entries: Vec<ReleaseSortEntry> =
        releases.into_iter().map(ReleaseSortEntry::new).collect();

    if let Some(ref term) = options.search_term {
        super::search::fuzzy_filter(
            &mut entries,
            term,
            |entry| entry.release.release_title.as_str(),
            |entry, score| entry.match_score = score,
        );
    }

    Ok(sort_and_paginate_releases(entries, options))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::{
        connect as link,
        connect_artist as link_artist,
        insert_artist,
        insert_release,
        insert_track,
        new_test_db,
    };

    #[test]
    fn get_by_id_returns_none_for_missing_release() -> anyhow::Result<()> {
        let db = new_test_db()?;
        let result = get_by_id(&db, DbId(999999))?;
        assert!(result.is_none());
        Ok(())
    }

    #[test]
    fn get_by_id_returns_inserted_release() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Test Release")?;

        let release = get_by_id(&db, release_id)?.expect("release should exist");
        assert_eq!(release.release_title, "Test Release");
        Ok(())
    }

    #[test]
    fn get_returns_releases_from_root() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        insert_release(&mut db, "Release A")?;
        insert_release(&mut db, "Release B")?;

        let releases = get(&db, "releases")?;
        assert_eq!(releases.len(), 2);
        let titles: Vec<&str> = releases.iter().map(|a| a.release_title.as_str()).collect();
        assert!(titles.contains(&"Release A"));
        assert!(titles.contains(&"Release B"));
        Ok(())
    }

    #[test]
    fn get_by_track_returns_linked_release() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "My Release")?;
        let track_id = insert_track(&mut db, "My Track")?;
        link(&mut db, release_id, track_id)?;

        let releases = get_by_track(&db, track_id)?;
        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].release_title, "My Release");
        Ok(())
    }

    #[test]
    fn get_by_track_returns_empty_for_unlinked_track() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track_id = insert_track(&mut db, "Lonely Track")?;

        let releases = get_by_track(&db, track_id)?;
        assert!(releases.is_empty());
        Ok(())
    }

    #[test]
    fn get_by_artist_returns_linked_releases() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Artist Release")?;
        let artist_id = insert_artist(&mut db, "Test Artist")?;
        link_artist(&mut db, release_id, artist_id)?;

        let releases = get_by_artist(&db, artist_id)?;
        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].release_title, "Artist Release");
        Ok(())
    }

    #[test]
    fn update_modifies_release_fields() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Original")?;

        let mut release = get_by_id(&db, release_id)?.expect("release should exist");
        release.release_title = "Updated".to_string();
        release.release_date = Some("2024-02-29".to_string());
        update(&mut db, &release)?;

        let updated = get_by_id(&db, release_id)?.expect("release should exist");
        assert_eq!(updated.release_title, "Updated");
        assert_eq!(updated.release_date.as_deref(), Some("2024-02-29"));
        Ok(())
    }

    #[test]
    fn query_with_default_options_returns_all() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        insert_release(&mut db, "Alpha")?;
        insert_release(&mut db, "Beta")?;
        insert_release(&mut db, "Gamma")?;

        let options = ListOptions {
            sort: vec![],
            offset: None,
            limit: None,
            search_term: None,
        };
        let result = query(&db, "releases", &options, &ReleaseQueryFilters::default())?;
        assert_eq!(result.total_count, 3);
        assert_eq!(result.entries.len(), 3);
        Ok(())
    }

    #[test]
    fn query_with_limit_and_offset() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        insert_release(&mut db, "A")?;
        insert_release(&mut db, "B")?;
        insert_release(&mut db, "C")?;

        let options = ListOptions {
            sort: vec![SortSpec {
                key: SortKey::Name,
                direction: super::super::SortDirection::Ascending,
            }],
            offset: Some(1),
            limit: Some(1),
            search_term: None,
        };
        let result = query(&db, "releases", &options, &ReleaseQueryFilters::default())?;
        assert_eq!(result.total_count, 3);
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.offset, 1);
        Ok(())
    }

    #[test]
    fn query_with_search_term_filters() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        insert_release(&mut db, "Rock Anthems")?;
        insert_release(&mut db, "Jazz Classics")?;
        insert_release(&mut db, "Rock Ballads")?;

        let options = ListOptions {
            sort: vec![],
            offset: None,
            limit: None,
            search_term: Some("rock".to_string()),
        };
        let result = query(&db, "releases", &options, &ReleaseQueryFilters::default())?;
        assert_eq!(result.total_count, 2);
        assert_eq!(result.entries.len(), 2);
        Ok(())
    }

    #[test]
    fn query_sort_by_name_ascending() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        insert_release(&mut db, "Zebra")?;
        insert_release(&mut db, "Alpha")?;
        insert_release(&mut db, "Middle")?;

        let options = ListOptions {
            sort: vec![SortSpec {
                key: SortKey::Name,
                direction: super::super::SortDirection::Ascending,
            }],
            offset: None,
            limit: None,
            search_term: None,
        };
        let result = query(&db, "releases", &options, &ReleaseQueryFilters::default())?;
        let titles: Vec<&str> = result
            .entries
            .iter()
            .map(|a| a.release_title.as_str())
            .collect();
        assert_eq!(titles, vec!["Alpha", "Middle", "Zebra"]);
        Ok(())
    }

    #[test]
    fn query_with_blank_search_term_does_not_filter() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        insert_release(&mut db, "Rock Anthems")?;
        insert_release(&mut db, "Jazz Classics")?;

        let options = ListOptions {
            sort: vec![],
            offset: None,
            limit: None,
            search_term: Some("   ".to_string()),
        };
        let result = query(&db, "releases", &options, &ReleaseQueryFilters::default())?;
        assert_eq!(result.total_count, 2);
        assert_eq!(result.entries.len(), 2);
        Ok(())
    }

    #[test]
    fn query_orders_by_match_score_when_sort_is_empty() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        insert_release(&mut db, "Alpha Rock")?;
        insert_release(&mut db, "Rock Solid")?;

        let options = ListOptions {
            sort: vec![],
            offset: None,
            limit: None,
            search_term: Some("rock".to_string()),
        };
        let result = query(&db, "releases", &options, &ReleaseQueryFilters::default())?;
        let titles: Vec<&str> = result
            .entries
            .iter()
            .map(|a| a.release_title.as_str())
            .collect();
        assert_eq!(titles, vec!["Rock Solid", "Alpha Rock"]);
        Ok(())
    }

    #[test]
    fn query_explicit_sort_overrides_match_score() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        insert_release(&mut db, "Alpha Rock")?;
        insert_release(&mut db, "Rock Solid")?;

        let options = ListOptions {
            sort: vec![SortSpec {
                key: SortKey::Name,
                direction: super::super::SortDirection::Ascending,
            }],
            offset: None,
            limit: None,
            search_term: Some("rock".to_string()),
        };
        let result = query(&db, "releases", &options, &ReleaseQueryFilters::default())?;
        let titles: Vec<&str> = result
            .entries
            .iter()
            .map(|a| a.release_title.as_str())
            .collect();
        assert_eq!(titles, vec!["Alpha Rock", "Rock Solid"]);
        Ok(())
    }

    fn insert_dated_release(
        db: &mut DbAny,
        title: &str,
        release_date: &str,
    ) -> anyhow::Result<DbId> {
        let release = Release {
            db_id: None,
            id: nanoid::nanoid!(),
            release_title: title.to_string(),
            sort_title: None,
            release_type: None,
            release_date: Some(release_date.to_string()),
            locked: None,
            created_at: None,
            ctime: None,
        };
        let id = db
            .exec_mut(QueryBuilder::insert().element(&release).query())?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("releases")
                .to(id)
                .query(),
        )?;
        Ok(id)
    }

    #[test]
    fn query_pushes_year_filter_into_agdb() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        insert_dated_release(&mut db, "Old Album", "1995-03-04")?;
        insert_dated_release(&mut db, "New Album", "2024-07-19")?;
        insert_dated_release(&mut db, "Other 2024", "2024-12-01")?;

        let options = ListOptions {
            sort: vec![],
            offset: None,
            limit: None,
            search_term: None,
        };
        let filters = ReleaseQueryFilters {
            year: Some(2024),
            ids: None,
        };
        let result = query(&db, "releases", &options, &filters)?;
        let titles: HashSet<&str> = result
            .entries
            .iter()
            .map(|r| r.release_title.as_str())
            .collect();
        assert_eq!(titles, HashSet::from(["New Album", "Other 2024"]));
        Ok(())
    }

    #[test]
    fn query_pushes_id_filter_into_agdb() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let keep_a = insert_release(&mut db, "Keep A")?;
        insert_release(&mut db, "Drop B")?;
        let keep_c = insert_release(&mut db, "Keep C")?;

        let options = ListOptions {
            sort: vec![],
            offset: None,
            limit: None,
            search_term: None,
        };
        let filters = ReleaseQueryFilters {
            year: None,
            ids: Some(HashSet::from([keep_a, keep_c])),
        };
        let result = query(&db, "releases", &options, &filters)?;
        let titles: HashSet<&str> = result
            .entries
            .iter()
            .map(|r| r.release_title.as_str())
            .collect();
        assert_eq!(titles, HashSet::from(["Keep A", "Keep C"]));
        Ok(())
    }
}
