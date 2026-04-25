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
    DbAny,
    DbElement,
    DbId,
    QueryBuilder,
    QueryId,
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

#[derive(DbElement, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[harmony_macros::structure]
pub(crate) struct Track {
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) track_title: String,
    pub(crate) sort_title: Option<String>,
    pub(crate) year: Option<u32>,
    pub(crate) disc: Option<u32>,
    pub(crate) disc_total: Option<u32>,
    pub(crate) track: Option<u32>,
    pub(crate) track_total: Option<u32>,
    pub(crate) duration_ms: Option<u64>,
    pub(crate) sample_rate_hz: Option<u32>,
    pub(crate) channel_count: Option<u32>,
    pub(crate) bit_depth: Option<u32>,
    pub(crate) bitrate_bps: Option<u32>,
    pub(crate) locked: Option<bool>,
    pub(crate) created_at: Option<u64>,
    pub(crate) ctime: Option<u64>,
}

#[harmony_macros::implementation]
impl Track {
    pub(crate) fn set_track_title(&mut self, track_title: String) {
        self.track_title = track_title;
    }

    pub(crate) fn set_sort_title(&mut self, sort_title: String) {
        self.sort_title = Some(sort_title);
    }

    pub(crate) fn set_year(&mut self, year: u32) {
        self.year = Some(year);
    }

    pub(crate) fn set_disc(&mut self, disc: u32) {
        self.disc = Some(disc);
    }

    pub(crate) fn set_disc_total(&mut self, disc_total: u32) {
        self.disc_total = Some(disc_total);
    }

    pub(crate) fn set_track(&mut self, track: u32) {
        self.track = Some(track);
    }

    pub(crate) fn set_track_total(&mut self, track_total: u32) {
        self.track_total = Some(track_total);
    }

    pub(crate) fn set_duration_ms(&mut self, duration_ms: u64) {
        self.duration_ms = Some(duration_ms);
    }
}

harmony_macros::compile!(type_path = Track, fields = true, methods = true);

pub(crate) fn get(db: &DbAny, from: impl Into<QueryId>) -> anyhow::Result<Vec<Track>> {
    let tracks: Vec<Track> = db
        .exec(
            QueryBuilder::select()
                .elements::<Track>()
                .search()
                .from(from)
                .query(),
        )?
        .try_into()?;

    Ok(tracks)
}

pub(crate) fn get_direct(db: &DbAny, from: impl Into<QueryId>) -> anyhow::Result<Vec<Track>> {
    let tracks: Vec<Track> = db
        .exec(
            QueryBuilder::select()
                .elements::<Track>()
                .search()
                .from(from)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    Ok(tracks)
}

pub(crate) fn get_direct_many(
    db: &DbAny,
    owner_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Vec<Track>>> {
    let unique_owner_db_ids = super::dedup_positive_ids(owner_db_ids);

    let mut related: HashMap<DbId, Vec<Track>> = unique_owner_db_ids
        .iter()
        .copied()
        .map(|owner_db_id| (owner_db_id, Vec::new()))
        .collect();
    if unique_owner_db_ids.is_empty() {
        return Ok(related);
    }

    // Do not require an `owned` edge key here: valid owner<->track links
    // may exist without that marker depending on relation write path.
    let batch = super::graph::collect_related_ids_by_owner(db, &unique_owner_db_ids, |_edge| true)?;

    if batch.all_ids.is_empty() {
        return Ok(related);
    }

    let tracks_by_id: HashMap<DbId, Track> =
        super::graph::bulk_fetch_typed(db, batch.all_ids, "Track")?;

    let mut related_ids_by_owner = batch.per_owner;
    for owner_db_id in unique_owner_db_ids {
        let Some(owner_related) = related.get_mut(&owner_db_id) else {
            continue;
        };
        let Some(owner_related_ids) = related_ids_by_owner.remove(&owner_db_id) else {
            continue;
        };
        for related_id in owner_related_ids {
            if let Some(track) = tracks_by_id.get(&related_id) {
                owner_related.push(track.clone());
            }
        }
    }

    Ok(related)
}

pub(crate) fn get_by_id(
    db: &impl super::DbAccess,
    track_db_id: DbId,
) -> anyhow::Result<Option<Track>> {
    super::graph::fetch_typed_by_id(db, track_db_id, "Track")
}

pub(crate) fn get_by_ids(
    db: &impl super::DbAccess,
    track_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Track>> {
    let unique_ids = super::dedup_positive_ids(track_db_ids);
    if unique_ids.is_empty() {
        return Ok(HashMap::new());
    }
    super::graph::bulk_fetch_typed(db, unique_ids, "Track")
}

/// Returns all tracks belonging to a library via its releases.
pub(crate) fn get_by_library(db: &DbAny, library_id: DbId) -> anyhow::Result<Vec<Track>> {
    let release_ids: Vec<DbId> = super::releases::get_direct(db, library_id)?
        .into_iter()
        .filter_map(|a| a.db_id.map(Into::into))
        .collect();
    if release_ids.is_empty() {
        return Ok(Vec::new());
    }
    let track_map = get_direct_many(db, &release_ids)?;
    Ok(track_map.into_values().flatten().collect())
}

pub(crate) fn get_by_artist(db: &DbAny, artist_db_id: DbId) -> anyhow::Result<Vec<Track>> {
    // Walk: Artist ← Credit (neighbor) ← Track (neighbor of credit).
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

    let tracks_by_id: HashMap<DbId, Track> =
        super::graph::bulk_fetch_typed(db, owner_ids, "Track")?;

    Ok(tracks_by_id.into_values().collect())
}

pub(crate) fn get_by_artists(db: &DbAny, artist_db_ids: &[DbId]) -> anyhow::Result<Vec<Track>> {
    let mut tracks = Vec::new();
    let mut seen = HashSet::new();

    for artist_db_id in artist_db_ids {
        for track in get_by_artist(db, *artist_db_id)? {
            if let Some(track_db_id) = track.db_id.clone().map(DbId::from) {
                if seen.insert(track_db_id) {
                    tracks.push(track);
                }
                continue;
            }
            tracks.push(track);
        }
    }

    Ok(tracks)
}

pub(crate) fn get_by_releases(db: &DbAny, release_db_ids: &[DbId]) -> anyhow::Result<Vec<Track>> {
    let mut unique_release_db_ids = Vec::new();
    let mut seen_release_db_ids = HashSet::new();
    for release_db_id in release_db_ids {
        if release_db_id.0 <= 0 {
            continue;
        }
        if seen_release_db_ids.insert(*release_db_id) {
            unique_release_db_ids.push(*release_db_id);
        }
    }
    if unique_release_db_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut deduped = Vec::new();
    let mut seen_track_ids = HashSet::new();
    let mut tracks_by_release = get_direct_many(db, &unique_release_db_ids)?;
    for release_db_id in unique_release_db_ids {
        let Some(release_tracks) = tracks_by_release.remove(&release_db_id) else {
            continue;
        };
        for track in release_tracks {
            if let Some(track_db_id) = track.db_id.clone().map(DbId::from) {
                if seen_track_ids.insert(track_db_id) {
                    deduped.push(track);
                }
                continue;
            }
            deduped.push(track);
        }
    }

    Ok(deduped)
}

pub(crate) fn get_by_release_artists(
    db: &DbAny,
    artist_db_ids: &[DbId],
) -> anyhow::Result<Vec<Track>> {
    let releases = super::releases::get_by_artists(db, artist_db_ids)?;
    let mut release_db_ids = Vec::new();
    let mut seen_release_db_ids = HashSet::new();
    for release in releases {
        if let Some(release_db_id) = release.db_id.clone().map(DbId::from) {
            if seen_release_db_ids.insert(release_db_id) {
                release_db_ids.push(release_db_id);
            }
        }
    }

    if release_db_ids.is_empty() {
        return Ok(Vec::new());
    }

    get_by_releases(db, &release_db_ids)
}

pub(crate) fn get_by_entry(db: &DbAny, entry_db_id: DbId) -> anyhow::Result<Vec<Track>> {
    let tracks: Vec<Track> = db
        .exec(
            QueryBuilder::select()
                .elements::<Track>()
                .search()
                .to(entry_db_id)
                .where_()
                .beyond()
                .where_()
                .not()
                .key("db_element_id")
                .value("Track")
                .end_where()
                .query(),
        )?
        .try_into()?;

    Ok(tracks)
}

pub(crate) fn update(db: &mut impl super::DbAccess, track: &Track) -> anyhow::Result<()> {
    db.exec_mut(QueryBuilder::insert().element(track).query())?;
    Ok(())
}

#[derive(Clone)]
struct TrackSortEntry {
    track: Track,
    lower_title: String,
    lower_sort_title: Option<String>,
    db_id: Option<i64>,
    date_created: Option<u64>,
    track_number: Option<u32>,
    disc_number: Option<u32>,
    duration: Option<u64>,
}

impl TrackSortEntry {
    fn new(track: Track) -> Self {
        Self {
            lower_title: track.track_title.to_lowercase(),
            lower_sort_title: track.sort_title.as_ref().map(|value| value.to_lowercase()),
            db_id: track.db_id.as_ref().map(|id| DbId::from(id.clone()).0),
            date_created: track.ctime.or(track.created_at),
            track_number: track.track,
            disc_number: track.disc,
            duration: track.duration_ms,
            track,
        }
    }
}

fn compare_track_field(a: &TrackSortEntry, b: &TrackSortEntry, key: SortKey) -> Ordering {
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
        SortKey::TrackNumber => compare_option(&a.track_number, &b.track_number),
        SortKey::DiscNumber => compare_option(&a.disc_number, &b.disc_number),
        SortKey::Duration => compare_option(&a.duration, &b.duration),
        SortKey::DbId => compare_option(&a.db_id, &b.db_id),
        SortKey::ReleaseDate => Ordering::Equal,
    }
}

fn compare_track_entries(a: &TrackSortEntry, b: &TrackSortEntry, sort: &[SortSpec]) -> Ordering {
    for spec in sort {
        let ord = apply_direction(compare_track_field(a, b, spec.key), spec.direction);
        if ord != Ordering::Equal {
            return ord;
        }
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

fn append_unique_tracks(
    target: &mut Vec<Track>,
    seen_track_ids: &mut HashSet<DbId>,
    tracks: Vec<Track>,
) {
    for track in tracks {
        if let Some(track_db_id) = track.db_id.clone().map(DbId::from) {
            if seen_track_ids.insert(track_db_id) {
                target.push(track);
            }
            continue;
        }
        target.push(track);
    }
}

fn apply_track_scope_filter(
    db: &DbAny,
    tracks: &mut Vec<Track>,
    scope: QueryId,
) -> anyhow::Result<()> {
    let scoped_ids: HashSet<DbId> = get(db, scope)?
        .into_iter()
        .filter_map(|track| track.db_id.map(DbId::from))
        .collect();
    tracks.retain(|track| {
        track
            .db_id
            .clone()
            .map(DbId::from)
            .is_some_and(|track_db_id| scoped_ids.contains(&track_db_id))
    });
    Ok(())
}

fn query_tracks_from_candidates(
    tracks: Vec<Track>,
    options: &ListOptions,
) -> anyhow::Result<PagedResult<Track>> {
    if options.search_term.is_none() && options.sort.is_empty() {
        return Ok(paginate_tracks(tracks, options));
    }

    let mut entries: Vec<TrackSortEntry> = tracks.into_iter().map(TrackSortEntry::new).collect();

    if let Some(ref term) = options.search_term {
        let lower_term = term.to_lowercase();
        entries.retain(|entry| entry.lower_title.contains(&lower_term));
    }

    Ok(sort_and_paginate_tracks(entries, options))
}

fn paginate_tracks(mut tracks: Vec<Track>, options: &ListOptions) -> PagedResult<Track> {
    let total_count = tracks.len() as u64;
    let offset = options.offset.unwrap_or(0).min(total_count);
    let offset = u64_to_usize_saturating(offset).min(tracks.len());
    let limit = options.limit.map(u64_to_usize_saturating);

    let entries = match limit {
        Some(limit) => tracks.drain(offset..).take(limit).collect(),
        None => tracks.drain(offset..).collect(),
    };

    PagedResult {
        entries,
        total_count,
        offset: offset as u64,
    }
}

fn sort_and_paginate_tracks(
    mut entries: Vec<TrackSortEntry>,
    options: &ListOptions,
) -> PagedResult<Track> {
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
                .map(|entry| entry.track)
                .collect(),
            None => entries
                .into_iter()
                .skip(offset)
                .map(|entry| entry.track)
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
        entries.select_nth_unstable_by(pivot, |a, b| compare_track_entries(a, b, &options.sort));
        entries.truncate(page_end);
    }

    entries.sort_by(|a, b| compare_track_entries(a, b, &options.sort));
    PagedResult {
        entries: match limit {
            Some(limit) => entries
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|entry| entry.track)
                .collect(),
            None => entries
                .into_iter()
                .skip(offset)
                .map(|entry| entry.track)
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
) -> anyhow::Result<PagedResult<Track>> {
    let tracks = get(db, from)?;

    if options.search_term.is_none() && options.sort.is_empty() {
        return Ok(paginate_tracks(tracks, options));
    }

    let mut entries: Vec<TrackSortEntry> = tracks.into_iter().map(TrackSortEntry::new).collect();

    // Text search filter
    if let Some(ref term) = options.search_term {
        let lower_term = term.to_lowercase();
        entries.retain(|entry| entry.lower_title.contains(&lower_term));
    }

    Ok(sort_and_paginate_tracks(entries, options))
}

pub(crate) fn query_by_artists(
    db: &DbAny,
    artist_db_ids: &[DbId],
    scope: Option<QueryId>,
    options: &ListOptions,
) -> anyhow::Result<PagedResult<Track>> {
    let mut tracks = get_by_artists(db, artist_db_ids)?;
    if let Some(scope) = scope {
        apply_track_scope_filter(db, &mut tracks, scope)?;
    }

    query_tracks_from_candidates(tracks, options)
}

pub(crate) fn query_by_release_artists(
    db: &DbAny,
    artist_db_ids: &[DbId],
    scope: Option<QueryId>,
    options: &ListOptions,
) -> anyhow::Result<PagedResult<Track>> {
    let mut tracks = get_by_release_artists(db, artist_db_ids)?;
    if let Some(scope) = scope {
        apply_track_scope_filter(db, &mut tracks, scope)?;
    }

    query_tracks_from_candidates(tracks, options)
}

pub(crate) fn query_by_artist_filters(
    db: &DbAny,
    artist_db_ids: &[DbId],
    release_artist_db_ids: &[DbId],
    scope: Option<QueryId>,
    options: &ListOptions,
) -> anyhow::Result<PagedResult<Track>> {
    let mut tracks = Vec::new();
    let mut seen_track_ids = HashSet::new();

    if !artist_db_ids.is_empty() {
        append_unique_tracks(
            &mut tracks,
            &mut seen_track_ids,
            get_by_artists(db, artist_db_ids)?,
        );
    }

    if !release_artist_db_ids.is_empty() {
        append_unique_tracks(
            &mut tracks,
            &mut seen_track_ids,
            get_by_release_artists(db, release_artist_db_ids)?,
        );
    }

    if let Some(scope) = scope {
        apply_track_scope_filter(db, &mut tracks, scope)?;
    }

    query_tracks_from_candidates(tracks, options)
}

#[cfg(test)]
mod tests {
    use nanoid::nanoid;

    use super::*;
    use crate::db::test_db::{
        insert_track,
        new_test_db,
    };

    #[test]
    fn get_by_entry_traverses_through_track_source() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track_db_id = insert_track(&mut db, "Test Track")?;

        let entry = super::super::entries::Entry {
            db_id: None,
            id: nanoid!(),
            full_path: std::path::PathBuf::from("/music/test.mp3"),
            kind: super::super::entries::EntryKind::File,
            file_kind: Some("audio".to_string()),
            name: "test.mp3".to_string(),
            hash: None,
            size: 1,
            mtime: 1,
            ctime: 1,
        };
        let entry_db_id = db
            .exec_mut(QueryBuilder::insert().element(&entry).query())?
            .ids()[0];

        let source = super::super::track_sources::TrackSource {
            db_id: None,
            id: nanoid!(),
            source_kind: "embedded_tags".to_string(),
            source_key: "key1".to_string(),
            identity: "key1".to_string(),
            is_primary: true,
            start_ms: None,
            end_ms: None,
        };
        let source_id = db
            .exec_mut(QueryBuilder::insert().element(&source).query())?
            .ids()[0];

        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(track_db_id)
                .to(source_id)
                .query(),
        )?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(source_id)
                .to(entry_db_id)
                .query(),
        )?;

        let tracks = get_by_entry(&db, entry_db_id)?;
        assert_eq!(tracks.len(), 1);
        assert_eq!(tracks[0].db_id.clone().map(DbId::from), Some(track_db_id));

        Ok(())
    }

    #[test]
    fn agdb_reads_track_with_missing_optional_keys_as_none() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let node_id = db
            .exec_mut(QueryBuilder::insert().nodes().count(1).query())?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .values_uniform([
                    ("db_element_id", "Track").into(),
                    ("id", "sparse-track-id").into(),
                    ("track_title", "Sparse Track").into(),
                    ("duration_ms", 120_000_u64).into(),
                ])
                .ids(node_id)
                .query(),
        )?;

        let tracks: Vec<Track> = db
            .exec(
                QueryBuilder::select()
                    .elements::<Track>()
                    .ids(node_id)
                    .query(),
            )?
            .try_into()?;
        assert_eq!(tracks.len(), 1);
        assert_eq!(tracks[0].track_title, "Sparse Track");
        assert_eq!(tracks[0].duration_ms, Some(120_000));
        assert_eq!(tracks[0].sample_rate_hz, None);
        assert_eq!(tracks[0].channel_count, None);
        assert_eq!(tracks[0].bit_depth, None);

        Ok(())
    }

    #[test]
    fn get_by_entry_returns_empty_when_no_tracks() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let entry = super::super::entries::Entry {
            db_id: None,
            id: nanoid!(),
            full_path: std::path::PathBuf::from("/music/orphan.mp3"),
            kind: super::super::entries::EntryKind::File,
            file_kind: Some("audio".to_string()),
            name: "orphan.mp3".to_string(),
            hash: None,
            size: 1,
            mtime: 1,
            ctime: 1,
        };
        let entry_db_id = db
            .exec_mut(QueryBuilder::insert().element(&entry).query())?
            .ids()[0];

        let tracks = get_by_entry(&db, entry_db_id)?;
        assert!(tracks.is_empty());

        Ok(())
    }
}
