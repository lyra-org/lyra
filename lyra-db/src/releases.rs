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
    DbError,
    DbId,
    DbTypeMarker,
    DbValue,
    QueryBuilder,
};
use serde::{
    Deserialize,
    Serialize,
};

use super::DbAccess;
use super::NodeId;
use super::roots::{
    self,
    Root,
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, DbTypeMarker,
)]
#[serde(rename_all = "lowercase")]
pub enum ReleaseType {
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
            _ => Err(DbError::serialization(
                agdb::DbErrorType::TypeError,
                format!("invalid ReleaseType value '{value}'"),
            )),
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

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(DbElement, Serialize, Deserialize, Clone, Debug)]
pub struct Release {
    #[serde(skip_serializing)]
    pub db_id: Option<NodeId>,
    pub id: String,
    pub release_title: String,
    pub sort_title: Option<String>,
    pub release_type: Option<ReleaseType>,
    pub release_date: Option<String>,
    /// Canonical `lyra_metadata::MediaFormat` names scanned from the tracks' tags.
    pub media_formats: Option<Vec<String>>,
    /// Normalized barcode scanned from the tracks' tags.
    pub barcode: Option<String>,
    pub locked: Option<bool>,
    pub created_at_ms: Option<u64>,
    pub file_created_at_ms: Option<u64>,
}

impl Release {
    pub fn set_release_title(&mut self, release_title: String) {
        self.release_title = release_title;
    }
}

pub fn normalize_release_date(value: &str) -> Option<String> {
    let value = value.trim();
    match value.len() {
        4 if valid_year(value) => Some(value.to_string()),
        7 if valid_year_month(value) => Some(value.to_string()),
        10 if valid_year_month_day(value) => Some(value.to_string()),
        _ => None,
    }
}

pub fn release_year(release_date: Option<&str>) -> Option<u32> {
    release_date?.get(..4)?.parse::<u32>().ok()
}

fn valid_year(value: &str) -> bool {
    value.as_bytes().iter().all(u8::is_ascii_digit)
}

fn valid_year_month(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.get(4) == Some(&b'-')
        && valid_year(&value[..4])
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && value[5..7]
            .parse::<u32>()
            .is_ok_and(|month| (1..=12).contains(&month))
}

fn valid_year_month_day(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.get(7) != Some(&b'-') || !valid_year_month(&value[..7]) {
        return false;
    }
    if !bytes[8..10].iter().all(u8::is_ascii_digit) {
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

pub fn get(db: &DbAny, from: DbId) -> anyhow::Result<Vec<Release>> {
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

pub fn get_direct(db: &DbAny, from: DbId) -> anyhow::Result<Vec<Release>> {
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

pub fn ids_with_year(db: &impl DbAccess, year: u32) -> anyhow::Result<Vec<DbId>> {
    Ok(db
        .exec(
            QueryBuilder::search()
                .from(Root::Releases.alias())
                .where_()
                .distance(roots::MEMBERS)
                .and()
                .key("release_date")
                .starts_with(format!("{year:04}"))
                .query(),
        )?
        .ids())
}

pub fn get_by_id(
    db: &impl super::DbAccess,
    release_db_id: DbId,
) -> anyhow::Result<Option<Release>> {
    super::graph::fetch_typed_by_id(db, release_db_id, "Release")
}

pub fn link_track(
    db: &mut impl DbAccess,
    release_db_id: DbId,
    track_db_id: DbId,
) -> anyhow::Result<()> {
    if super::graph::ensure_owned_edge(db, release_db_id, track_db_id)? {
        super::covers::display::offer_release_to_playlists_with_track(
            db,
            release_db_id,
            track_db_id,
        )?;
    }
    Ok(())
}

pub fn unlink_track(
    db: &mut impl DbAccess,
    release_db_id: DbId,
    track_db_id: DbId,
) -> anyhow::Result<()> {
    super::chains::remove_for_departing_track(db, release_db_id, track_db_id)?;
    if super::graph::remove_edges_between(db, release_db_id, track_db_id)? {
        super::covers::display::resync_playlists_with_track(db, track_db_id)?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TrackReleaseDelta {
    pub membership_changed: bool,
}

/// Make `release_db_id` the track's sole release without publishing per-edge
/// display-cover updates. The caller owns batching those updates for the wider
/// operation from the returned logical membership delta.
pub fn replace_track_release(
    db: &mut impl DbAccess,
    release_db_id: DbId,
    track_db_id: DbId,
) -> anyhow::Result<TrackReleaseDelta> {
    let mut removed_other = false;
    let mut seen = HashSet::new();
    for release in get_by_track(db, track_db_id)? {
        let Some(current_release_db_id) = release.db_id.map(DbId::from) else {
            continue;
        };
        if current_release_db_id == release_db_id || !seen.insert(current_release_db_id) {
            continue;
        }
        super::chains::remove_for_departing_track(db, current_release_db_id, track_db_id)?;
        removed_other |=
            super::graph::remove_edges_between(db, current_release_db_id, track_db_id)?;
    }

    let target_added = super::graph::ensure_owned_edge(db, release_db_id, track_db_id)?;
    Ok(TrackReleaseDelta {
        membership_changed: removed_other || target_added,
    })
}

pub fn get_by_artist(db: &impl DbAccess, artist_db_id: DbId) -> anyhow::Result<Vec<Release>> {
    let owner_ids = super::credits::owner_ids_by_artist::<Release>(db, artist_db_id, 0, 0)?;
    if owner_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut releases_by_id: HashMap<DbId, Release> =
        super::graph::bulk_fetch_typed(db, owner_ids.clone(), "Release")?;

    Ok(owner_ids
        .into_iter()
        .filter_map(|owner_id| releases_by_id.remove(&owner_id))
        .collect())
}

/// The releases holding the track; none when `track_db_id` is not a track.
pub fn get_by_track(db: &impl super::DbAccess, track_db_id: DbId) -> anyhow::Result<Vec<Release>> {
    if !super::graph::is_kind(db, track_db_id, "Track")? {
        return Ok(Vec::new());
    }
    releases_holding(db, track_db_id)
}

fn releases_holding(db: &impl super::DbAccess, track_db_id: DbId) -> anyhow::Result<Vec<Release>> {
    Ok(db
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
        .try_into()?)
}

/// The releases holding each track, keyed by every id given; ids that are not tracks map to none.
pub fn get_by_tracks(
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
    let unique_track_db_ids = super::graph::existing_ids(db, &unique_track_db_ids, "Track")?;
    if unique_track_db_ids.is_empty() {
        return Ok(related);
    }

    if unique_track_db_ids.len() <= SMALL_TRACK_BATCH_THRESHOLD {
        for track_db_id in unique_track_db_ids {
            let track_releases = releases_holding(db, track_db_id)?;
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
    if let Ok(edges) = roots::member_edges(db, Root::Releases) {
        for edge in edges {
            let (from, to) = (edge.from, edge.to);
            if from.0 == 0 || to.0 == 0 {
                continue;
            }
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
                    (from, to) if from.0 != 0 && to.0 != 0 && from == *track_db_id => Some(to),
                    (from, to) if from.0 != 0 && to.0 != 0 && to == *track_db_id => Some(from),
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

/// Atomically aligns the stored row to `release`.
#[cfg(any(test, feature = "testing"))]
pub fn update(db: &mut DbAny, release: &Release) -> anyhow::Result<()> {
    db.transaction_mut(|t| update_in_transaction(t, release))
}

pub fn update_in_transaction(
    db: &mut DbAnyTransactionMut<'_>,
    release: &Release,
) -> anyhow::Result<()> {
    let release_db_id = release
        .db_id
        .clone()
        .map(DbId::from)
        .ok_or_else(|| anyhow::anyhow!("release update missing db_id"))?;
    super::replace_element_in_transaction(
        db,
        release_db_id,
        [
            ("sort_title", release.sort_title.is_none()),
            ("release_type", release.release_type.is_none()),
            ("release_date", release.release_date.is_none()),
            ("media_formats", release.media_formats.is_none()),
            ("barcode", release.barcode.is_none()),
            ("locked", release.locked.is_none()),
            ("created_at_ms", release.created_at_ms.is_none()),
            ("file_created_at_ms", release.file_created_at_ms.is_none()),
        ],
        release,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::connect as link;
    use crate::fixtures::{
        insert_artist,
        insert_release,
        insert_track,
    };
    use crate::test_db::{
        connect_artist as link_artist,
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
    fn update_clears_optional_fields_set_to_none() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release(&mut db, "Clearable Release")?;

        let mut release = get_by_id(&db, release_db_id)?.expect("release should exist");
        release.release_date = Some("1999-01-01".to_string());
        release.sort_title = Some("Clearable".to_string());
        update(&mut db, &release)?;

        let mut release = get_by_id(&db, release_db_id)?.expect("release should exist");
        assert_eq!(release.release_date.as_deref(), Some("1999-01-01"));
        release.release_date = None;
        release.sort_title = None;
        update(&mut db, &release)?;

        let release = get_by_id(&db, release_db_id)?.expect("release should exist");
        assert_eq!(release.release_date, None);
        assert_eq!(release.sort_title, None);
        assert_eq!(release.release_title, "Clearable Release");
        Ok(())
    }

    /// Guards the hand-maintained clear list in `update` against struct drift.
    #[test]
    fn update_clears_every_optional_key() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release(&mut db, "Drift Guard")?;
        let db_id = Some(super::super::NodeId::from(release_db_id));

        update(
            &mut db,
            &Release {
                db_id: db_id.clone(),
                id: "drift-guard".to_string(),
                release_title: "Drift Guard".to_string(),
                sort_title: Some("Drift".to_string()),
                release_type: Some(ReleaseType::Album),
                release_date: Some("1999-01-01".to_string()),
                media_formats: None,
                barcode: None,
                locked: Some(true),
                created_at_ms: Some(1),
                file_created_at_ms: Some(2),
            },
        )?;

        update(
            &mut db,
            &Release {
                db_id,
                id: "drift-guard".to_string(),
                release_title: "Drift Guard".to_string(),
                sort_title: None,
                release_type: None,
                release_date: None,
                media_formats: None,
                barcode: None,
                locked: None,
                created_at_ms: None,
                file_created_at_ms: None,
            },
        )?;

        let keys = crate::test_db::stored_keys(&db, release_db_id)?;
        assert_eq!(
            keys,
            ["db_element_id", "id", "release_title"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            "only non-Option keys may remain after an all-None update"
        );
        Ok(())
    }

    #[test]
    fn get_returns_the_releases_under_an_owner() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library = crate::test_db::insert_test_library_node(&mut db, "Music", "/music".into())?;
        let library_db_id: DbId = library.db_id.expect("library id");
        for title in ["Release A", "Release B"] {
            let release = insert_release(&mut db, title)?;
            crate::graph::ensure_owned_edge(&mut db, library_db_id, release)?;
        }
        insert_release(&mut db, "Elsewhere")?;

        let releases = get(&db, library_db_id)?;
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
    fn get_by_track_ignores_releases_crediting_an_artist() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Credited Release")?;
        let artist_id = insert_artist(&mut db, "Credited Artist")?;
        link_artist(&mut db, release_id, artist_id)?;

        assert!(get_by_track(&db, artist_id)?.is_empty());
        Ok(())
    }

    #[test]
    fn get_by_tracks_maps_non_tracks_to_no_releases() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Mixed Release")?;
        let artist_id = insert_artist(&mut db, "Mixed Artist")?;
        link_artist(&mut db, release_id, artist_id)?;
        let mut track_ids = Vec::new();
        for index in 0..40 {
            let track_id = insert_track(&mut db, &format!("Mixed Track {index}"))?;
            link(&mut db, release_id, track_id)?;
            track_ids.push(track_id);
        }

        for ids in [
            vec![track_ids[0], artist_id],
            [track_ids.clone(), vec![artist_id]].concat(),
        ] {
            let related = get_by_tracks(&db, &ids)?;
            assert_eq!(related.get(&artist_id).map(Vec::len), Some(0));
            assert_eq!(related.get(&track_ids[0]).map(Vec::len), Some(1));
        }
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
}
