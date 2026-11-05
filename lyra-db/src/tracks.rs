// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};

use agdb::{
    CountComparison,
    DbAny,
    DbAnyTransactionMut,
    DbElement,
    DbId,
    QueryBuilder,
};
use serde::{
    Deserialize,
    Serialize,
};

use super::NodeId;
use super::roots::{
    self,
    Root,
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(DbElement, Serialize, Deserialize, Clone, Debug)]
pub struct Track {
    #[serde(skip_serializing)]
    pub db_id: Option<NodeId>,
    pub id: String,
    pub track_title: String,
    pub sort_title: Option<String>,
    pub year: Option<u32>,
    pub disc: Option<u32>,
    pub disc_total: Option<u32>,
    pub track: Option<u32>,
    pub track_total: Option<u32>,
    pub duration_ms: Option<u64>,
    /// ReplayGain 2.0 gains in dB (-18 LUFS reference).
    pub track_gain_db: Option<f64>,
    pub album_gain_db: Option<f64>,
    pub locked: Option<bool>,
    pub created_at_ms: Option<u64>,
    pub file_created_at_ms: Option<u64>,
}

impl Track {
    pub fn set_track_title(&mut self, track_title: String) {
        self.track_title = track_title;
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn set_year(&mut self, year: u32) {
        self.year = Some(year);
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn set_track(&mut self, track: u32) {
        self.track = Some(track);
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn set_track_total(&mut self, track_total: u32) {
        self.track_total = Some(track_total);
    }

    pub fn set_duration_ms(&mut self, duration_ms: u64) {
        self.duration_ms = Some(duration_ms);
    }
}

pub fn get(db: &DbAny, from: DbId) -> anyhow::Result<Vec<Track>> {
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

pub fn get_direct(db: &impl super::DbAccess, from: DbId) -> anyhow::Result<Vec<Track>> {
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

pub fn get_direct_many(
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

pub fn get_by_id(db: &impl super::DbAccess, track_db_id: DbId) -> anyhow::Result<Option<Track>> {
    super::graph::fetch_typed_by_id(db, track_db_id, "Track")
}

pub fn get_by_ids(
    db: &impl super::DbAccess,
    track_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Track>> {
    let unique_ids = super::dedup_positive_ids(track_db_ids);
    if unique_ids.is_empty() {
        return Ok(HashMap::new());
    }
    super::graph::bulk_fetch_typed(db, unique_ids, "Track")
}

/// Fetches the Track nodes that still exist, tolerating missing or wrong-type IDs.
///
/// agdb rejects an explicit multi-ID select when any ID is missing. Resolve the
/// surviving nodes first so the typed bulk fetch remains the normal fast path.
pub fn get_existing_by_ids(
    db: &impl super::DbAccess,
    track_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Track>> {
    let unique_ids = super::dedup_positive_ids(track_db_ids);
    if unique_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let existing_public_ids = super::lookup::find_ids_by_db_ids(db, &unique_ids)?;
    let existing_ids = unique_ids
        .into_iter()
        .filter(|id| existing_public_ids.contains_key(id))
        .collect::<Vec<_>>();
    if existing_ids.is_empty() {
        return Ok(HashMap::new());
    }
    super::graph::bulk_fetch_typed(db, existing_ids, "Track")
}

pub fn ids_with_year(db: &impl super::DbAccess, year: u32) -> anyhow::Result<Vec<DbId>> {
    Ok(db
        .exec(
            QueryBuilder::search()
                .from(Root::Tracks.alias())
                .where_()
                .distance(roots::MEMBERS)
                .and()
                .key("year")
                .value(u64::from(year))
                .query(),
        )?
        .ids())
}

pub fn get_by_artist(db: &impl super::DbAccess, artist_db_id: DbId) -> anyhow::Result<Vec<Track>> {
    let owner_ids = super::credits::owner_ids_by_artist::<Track>(db, artist_db_id, 0, 0)?;
    if owner_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut tracks_by_id: HashMap<DbId, Track> =
        super::graph::bulk_fetch_typed(db, owner_ids.clone(), "Track")?;

    Ok(owner_ids
        .into_iter()
        .filter_map(|owner_id| tracks_by_id.remove(&owner_id))
        .collect())
}

const ARTIST_CREDIT_CANDIDATE_PAGE_SIZE: usize = 200;

/// Returns at most `limit` track-credit owners in stable newest-credit-first order,
/// continuing past tracks outside `accessible_library_ids` when visibility is provided.
pub fn get_bounded_ids_by_artist(
    db: &impl super::DbAccess,
    artist_db_id: DbId,
    accessible_library_ids: Option<&HashSet<String>>,
    limit: usize,
) -> anyhow::Result<Vec<DbId>> {
    if limit == 0 || accessible_library_ids.is_some_and(HashSet::is_empty) {
        return Ok(Vec::new());
    }

    let accessible_library_db_ids = if let Some(library_ids) = accessible_library_ids {
        Some(super::libraries::db_ids_for_public_ids(db, library_ids)?)
    } else {
        None
    };
    if accessible_library_db_ids
        .as_ref()
        .is_some_and(Vec::is_empty)
    {
        return Ok(Vec::new());
    }
    let mut result = Vec::with_capacity(limit);
    let mut offset = 0;
    loop {
        let page_limit = if accessible_library_ids.is_some() {
            ARTIST_CREDIT_CANDIDATE_PAGE_SIZE
        } else {
            limit
        };
        let candidates =
            super::credits::owner_ids_by_artist::<Track>(db, artist_db_id, offset, page_limit)?;
        if candidates.is_empty() {
            break;
        }
        let accessible = accessible_library_db_ids
            .as_deref()
            .map(|library_ids| {
                super::libraries::accessible_track_ids_for_library_db_ids(
                    db,
                    library_ids,
                    &candidates,
                )
            })
            .transpose()?;

        result.extend(candidates.iter().copied().filter(|track_db_id| {
            accessible
                .as_ref()
                .is_none_or(|ids| ids.contains(track_db_id))
        }));
        result.truncate(limit);
        if result.len() == limit || candidates.len() < page_limit {
            break;
        }
        offset += candidates.len();
    }

    Ok(result)
}

pub fn get_by_releases(db: &DbAny, release_db_ids: &[DbId]) -> anyhow::Result<Vec<Track>> {
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

/// The tracks sourced from the entry. The search stops at the library, above which are only the
/// users with access and everything they have done, and at the directories holding the entry,
/// since no track reaches it through them.
pub fn get_by_entry(db: &DbAny, entry_db_id: DbId) -> anyhow::Result<Vec<Track>> {
    let tracks: Vec<Track> = db
        .exec(
            QueryBuilder::select()
                .elements::<Track>()
                .search()
                .to(entry_db_id)
                .where_()
                .not_beyond()
                .where_()
                .key("db_element_id")
                .value("Track")
                .or()
                .key("db_element_id")
                .value("Library")
                .or()
                .where_()
                .distance(CountComparison::GreaterThan(0))
                .and()
                .key("db_element_id")
                .value("Entry")
                .end_where()
                .end_where()
                .query(),
        )?
        .try_into()?;

    Ok(tracks)
}

/// Atomically aligns the stored row to `track`.
#[cfg(any(test, feature = "testing"))]
pub fn update(db: &mut DbAny, track: &Track) -> anyhow::Result<()> {
    db.transaction_mut(|t| update_in_transaction(t, track))
}

pub fn update_in_transaction(
    db: &mut DbAnyTransactionMut<'_>,
    track: &Track,
) -> anyhow::Result<()> {
    let track_db_id = track
        .db_id
        .clone()
        .map(DbId::from)
        .ok_or_else(|| anyhow::anyhow!("track update missing db_id"))?;
    super::replace_element_in_transaction(
        db,
        track_db_id,
        [
            ("sort_title", track.sort_title.is_none()),
            ("year", track.year.is_none()),
            ("disc", track.disc.is_none()),
            ("disc_total", track.disc_total.is_none()),
            ("track", track.track.is_none()),
            ("track_total", track.track_total.is_none()),
            ("duration_ms", track.duration_ms.is_none()),
            ("track_gain_db", track.track_gain_db.is_none()),
            ("album_gain_db", track.album_gain_db.is_none()),
            ("locked", track.locked.is_none()),
            ("created_at_ms", track.created_at_ms.is_none()),
            ("file_created_at_ms", track.file_created_at_ms.is_none()),
        ],
        track,
    )
}

#[cfg(test)]
mod tests {
    use nanoid::nanoid;

    use super::*;
    use crate::fixtures::{
        insert_artist,
        insert_track,
    };
    use crate::test_db::{
        connect_artist,
        new_test_db,
    };

    #[test]
    fn artist_credit_tracks_are_returned_in_stable_reverse_credit_order() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let artist_db_id = insert_artist(&mut db, "Featured Artist")?;
        let first = insert_track(&mut db, "First")?;
        let second = insert_track(&mut db, "Second")?;
        let third = insert_track(&mut db, "Third")?;
        connect_artist(&mut db, first, artist_db_id)?;
        connect_artist(&mut db, second, artist_db_id)?;
        connect_artist(&mut db, third, artist_db_id)?;

        let first_read = get_bounded_ids_by_artist(&db, artist_db_id, None, 2)?;
        let second_read = get_bounded_ids_by_artist(&db, artist_db_id, None, 2)?;

        assert_eq!(first_read, vec![third, second]);
        assert_eq!(second_read, first_read);
        Ok(())
    }

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
            file_modified_at_ms: 1,
            file_created_at_ms: 1,
            synced_at_ms: None,
            missing_at_ms: None,
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
            container: None,
            codec: None,
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
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
    fn update_clears_optional_fields_set_to_none() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track_db_id = insert_track(&mut db, "Clearable Track")?;

        let mut track = get_by_id(&db, track_db_id)?.expect("track should exist");
        track.year = Some(1999);
        track.track = Some(3);
        track.duration_ms = Some(180_000);
        update(&mut db, &track)?;

        let mut track = get_by_id(&db, track_db_id)?.expect("track should exist");
        assert_eq!(track.year, Some(1999));
        track.year = None;
        track.track = None;
        track.duration_ms = None;
        update(&mut db, &track)?;

        let track = get_by_id(&db, track_db_id)?.expect("track should exist");
        assert_eq!(track.year, None);
        assert_eq!(track.track, None);
        assert_eq!(track.duration_ms, None);
        assert_eq!(track.track_title, "Clearable Track");
        Ok(())
    }

    /// Guards the hand-maintained clear list in `update` against struct drift.
    /// Adding an `Option` field to `Track` without adding it to that list leaves
    /// its key stored here, and adding any field breaks the exhaustive literal.
    #[test]
    fn update_clears_every_optional_key() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track_db_id = insert_track(&mut db, "Drift Guard")?;
        let db_id = Some(super::super::NodeId::from(track_db_id));

        update(
            &mut db,
            &Track {
                db_id: db_id.clone(),
                id: "drift-guard".to_string(),
                track_title: "Drift Guard".to_string(),
                sort_title: Some("Drift".to_string()),
                year: Some(1999),
                disc: Some(1),
                disc_total: Some(2),
                track: Some(3),
                track_total: Some(4),
                duration_ms: Some(5),
                track_gain_db: None,
                album_gain_db: None,
                locked: Some(true),
                created_at_ms: Some(6),
                file_created_at_ms: Some(7),
            },
        )?;

        update(
            &mut db,
            &Track {
                db_id,
                id: "drift-guard".to_string(),
                track_title: "Drift Guard".to_string(),
                sort_title: None,
                year: None,
                disc: None,
                disc_total: None,
                track: None,
                track_total: None,
                duration_ms: None,
                track_gain_db: None,
                album_gain_db: None,
                locked: None,
                created_at_ms: None,
                file_created_at_ms: None,
            },
        )?;

        let keys = crate::test_db::stored_keys(&db, track_db_id)?;
        assert_eq!(
            keys,
            ["db_element_id", "id", "track_title"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            "only non-Option keys may remain after an all-None update"
        );
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
            file_modified_at_ms: 1,
            file_created_at_ms: 1,
            synced_at_ms: None,
            missing_at_ms: None,
        };
        let entry_db_id = db
            .exec_mut(QueryBuilder::insert().element(&entry).query())?
            .ids()[0];

        let tracks = get_by_entry(&db, entry_db_id)?;
        assert!(tracks.is_empty());

        Ok(())
    }

    #[test]
    fn get_existing_by_ids_skips_missing_and_wrong_type_nodes() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let existing_track = insert_track(&mut db, "Existing Track")?;
        let deleted_track = insert_track(&mut db, "Deleted Track")?;
        let artist = insert_artist(&mut db, "Not a Track")?;
        db.exec_mut(QueryBuilder::remove().ids(deleted_track).query())?;

        let tracks = get_existing_by_ids(&db, &[deleted_track, artist, existing_track])?;

        assert_eq!(tracks.len(), 1);
        assert_eq!(
            tracks
                .get(&existing_track)
                .map(|track| track.track_title.as_str()),
            Some("Existing Track")
        );
        Ok(())
    }
}
