// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    BTreeMap,
    HashMap,
    HashSet,
};

use agdb::{
    CountComparison,
    DbAny,
    DbId,
    QueryBuilder,
};
use lyra_metadata::LookupHints;

use crate::db::{
    self,
    Artist,
    Entry,
    Release,
    Track,
};

use super::{
    ExternalIdsByProvider,
    ResolvedCreditedArtist,
    TrackArtistContext,
    TrackCreditedArtistContext,
    dedupe_db_ids,
    resolve_release_credited_artists_map,
    resolve_track_artists_with_context,
    resolve_track_credited_artists_with_context,
};

#[derive(Default)]
pub(crate) struct ArtistOwnedEntities {
    pub(crate) releases_by_artist: HashMap<DbId, Vec<Release>>,
    pub(crate) tracks_by_artist: HashMap<DbId, Vec<Track>>,
}

pub(crate) fn db_ids_from_tracks(tracks: &[Track]) -> Vec<DbId> {
    tracks
        .iter()
        .filter_map(|track| track.db_id.clone().map(DbId::from))
        .collect()
}

pub(crate) fn db_ids_from_releases(releases: &[Release]) -> Vec<DbId> {
    releases
        .iter()
        .filter_map(|release| release.db_id.clone().map(DbId::from))
        .collect()
}

pub(crate) fn db_ids_from_artists(artists: &[Artist]) -> Vec<DbId> {
    artists
        .iter()
        .filter_map(|artist| artist.db_id.clone().map(DbId::from))
        .collect()
}

pub(crate) fn album_track_sort_key(track: &Track) -> (u32, u32, String, i64) {
    let disc = track.disc.unwrap_or(1);
    let track_number = track.track.unwrap_or(u32::MAX);
    let title = track.track_title.to_ascii_lowercase();
    let db_id = track
        .db_id
        .clone()
        .map(|id| {
            let id: DbId = id.into();
            id.0
        })
        .unwrap_or(i64::MAX);

    (disc, track_number, title, db_id)
}

pub(crate) fn sort_album_tracks(tracks: &mut [Track]) {
    tracks.sort_by_key(album_track_sort_key);
}

pub(crate) fn release_tracks_by_release(
    db: &DbAny,
    release_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Vec<Track>>> {
    let mut tracks_by_release = db::tracks::get_direct_many(db, release_ids)?;
    for tracks in tracks_by_release.values_mut() {
        sort_album_tracks(tracks);
    }
    Ok(tracks_by_release)
}

pub(crate) fn release_tracks(db: &DbAny, release_id: DbId) -> anyhow::Result<Vec<Track>> {
    Ok(release_tracks_by_release(db, &[release_id])?
        .remove(&release_id)
        .unwrap_or_default())
}

pub(crate) fn track_releases_by_track(
    db: &DbAny,
    track_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Vec<Release>>> {
    db::releases::get_by_tracks(db, track_ids)
}

pub(crate) fn release_credited_artists_by_release(
    db: &DbAny,
    release_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Vec<ResolvedCreditedArtist>>> {
    resolve_release_credited_artists_map(db, release_ids)
}

pub(crate) fn raw_artists_by_owner(
    db: &DbAny,
    owner_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Vec<Artist>>> {
    db::artists::get_many_by_owner(db, owner_ids)
}

pub(crate) fn track_artists_by_track(
    db: &DbAny,
    track_ids: &[DbId],
    releases_by_track: Option<&HashMap<DbId, Vec<Release>>>,
    artists_by_release: Option<&HashMap<DbId, Vec<Artist>>>,
) -> anyhow::Result<HashMap<DbId, Vec<Artist>>> {
    let ctx = TrackArtistContext {
        releases_by_track,
        artists_by_release,
        scope_release_id: None,
    };
    resolve_track_artists_with_context(db, track_ids, &ctx)
}

pub(crate) fn track_artists_for_release(
    db: &DbAny,
    track_ids: &[DbId],
    release_id: DbId,
    artists_by_release: Option<&HashMap<DbId, Vec<Artist>>>,
) -> anyhow::Result<HashMap<DbId, Vec<Artist>>> {
    let ctx = TrackArtistContext {
        releases_by_track: None,
        artists_by_release,
        scope_release_id: Some(release_id),
    };
    resolve_track_artists_with_context(db, track_ids, &ctx)
}

pub(crate) fn track_credited_artists_by_track(
    db: &DbAny,
    track_ids: &[DbId],
    releases_by_track: Option<&HashMap<DbId, Vec<Release>>>,
    credited_artists_by_release: Option<&HashMap<DbId, Vec<ResolvedCreditedArtist>>>,
    scope_release_id: Option<DbId>,
) -> anyhow::Result<HashMap<DbId, Vec<ResolvedCreditedArtist>>> {
    let ctx = TrackCreditedArtistContext {
        releases_by_track,
        credited_artists_by_release,
        scope_release_id,
    };
    resolve_track_credited_artists_with_context(db, track_ids, &ctx)
}

pub(crate) fn track_entries(db: &DbAny, track_id: DbId) -> anyhow::Result<Vec<Entry>> {
    db::entries::get_by_track(db, track_id)
}

pub(crate) fn entries_by_track(
    db: &DbAny,
    track_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Vec<Entry>>> {
    let unique_track_ids = dedupe_db_ids(track_ids);
    let mut entries_by_track: HashMap<DbId, Vec<Entry>> = unique_track_ids
        .iter()
        .copied()
        .map(|track_id| (track_id, Vec::new()))
        .collect();

    for track_id in unique_track_ids {
        entries_by_track.insert(track_id, track_entries(db, track_id)?);
    }

    Ok(entries_by_track)
}

pub(crate) fn unique_entries_for_tracks(
    db: &DbAny,
    tracks: &[Track],
) -> anyhow::Result<Vec<Entry>> {
    let track_ids = db_ids_from_tracks(tracks);
    let mut entries_by_track = entries_by_track(db, &track_ids)?;
    let mut entries = Vec::new();
    let mut seen = HashSet::new();

    for track_id in track_ids {
        let Some(track_entries) = entries_by_track.remove(&track_id) else {
            continue;
        };
        for entry in track_entries {
            let Some(entry_db_id) = entry.db_id else {
                continue;
            };
            if seen.insert(entry_db_id) {
                entries.push(entry);
            }
        }
    }

    Ok(entries)
}

pub(crate) fn first_file_path(entries: &[Entry]) -> Option<String> {
    entries
        .iter()
        .filter(|entry| entry.kind == db::entries::EntryKind::File)
        .map(|entry| entry.full_path.to_string_lossy().to_string())
        .min()
}

pub(crate) fn lookup_hints_for_entries(
    entries: &[Entry],
    library_root: Option<&str>,
) -> LookupHints {
    first_file_path(entries)
        .as_deref()
        .map(|path| {
            lyra_metadata::extract_lookup_hints_from_file_path_with_library_root(path, library_root)
        })
        .unwrap_or_default()
}

pub(crate) fn external_ids_for_entity(
    db: &DbAny,
    entity_id: DbId,
) -> anyhow::Result<ExternalIdsByProvider> {
    let ids = db::external_ids::get_for_entity(db, entity_id)?;
    let mut map = BTreeMap::new();
    for id in ids {
        map.entry(id.provider_id)
            .or_insert_with(BTreeMap::new)
            .insert(id.id_type, id.id_value);
    }

    Ok(map)
}

pub(crate) fn external_ids_by_entity(
    db: &DbAny,
    entity_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, ExternalIdsByProvider>> {
    let mut external_ids = HashMap::new();
    for entity_id in dedupe_db_ids(entity_ids) {
        external_ids.insert(entity_id, external_ids_for_entity(db, entity_id)?);
    }
    Ok(external_ids)
}

fn credit_owner_ids_by_artist(
    db: &DbAny,
    artist_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Vec<DbId>>> {
    let unique_artist_ids = dedupe_db_ids(artist_ids);
    let mut owner_ids_by_artist: HashMap<DbId, Vec<DbId>> = unique_artist_ids
        .iter()
        .copied()
        .map(|artist_id| (artist_id, Vec::new()))
        .collect();

    for artist_id in unique_artist_ids {
        let credits: Vec<db::Credit> = db
            .exec(
                QueryBuilder::select()
                    .elements::<db::Credit>()
                    .search()
                    .to(artist_id)
                    .where_()
                    .neighbor()
                    .end_where()
                    .query(),
            )?
            .try_into()?;

        let Some(artist_owner_ids) = owner_ids_by_artist.get_mut(&artist_id) else {
            continue;
        };
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
                        .distance(CountComparison::Equal(1))
                        .query(),
                )?
                .elements
                .iter()
                .filter_map(|e| (e.from.0 > 0).then_some(e.from))
                .collect();
            for owner_id in incoming {
                if seen_owners.insert(owner_id) {
                    artist_owner_ids.push(owner_id);
                }
            }
        }
    }

    Ok(owner_ids_by_artist)
}

pub(crate) fn artist_owned_entities_by_artist(
    db: &DbAny,
    artist_ids: &[DbId],
    include_releases: bool,
    include_tracks: bool,
) -> anyhow::Result<ArtistOwnedEntities> {
    let owner_ids_by_artist = credit_owner_ids_by_artist(db, artist_ids)?;
    let mut all_owner_ids = Vec::new();
    let mut seen_owner_ids = HashSet::new();
    for owner_ids in owner_ids_by_artist.values() {
        for owner_id in owner_ids {
            if seen_owner_ids.insert(*owner_id) {
                all_owner_ids.push(*owner_id);
            }
        }
    }

    let releases_by_id: HashMap<DbId, Release> = if include_releases {
        db::graph::bulk_fetch_typed(db, all_owner_ids.clone(), "Release")?
    } else {
        HashMap::new()
    };
    let tracks_by_id: HashMap<DbId, Track> = if include_tracks {
        db::graph::bulk_fetch_typed(db, all_owner_ids, "Track")?
    } else {
        HashMap::new()
    };

    let mut owned = ArtistOwnedEntities::default();
    for artist_id in dedupe_db_ids(artist_ids) {
        let owner_ids = owner_ids_by_artist
            .get(&artist_id)
            .cloned()
            .unwrap_or_default();

        if include_releases {
            let mut releases = Vec::new();
            let mut seen_release_ids = HashSet::new();
            for owner_id in &owner_ids {
                let Some(release) = releases_by_id.get(owner_id) else {
                    continue;
                };
                if seen_release_ids.insert(*owner_id) {
                    releases.push(release.clone());
                }
            }
            owned.releases_by_artist.insert(artist_id, releases);
        }

        if include_tracks {
            let mut tracks = Vec::new();
            let mut seen_track_ids = HashSet::new();
            for owner_id in &owner_ids {
                let Some(track) = tracks_by_id.get(owner_id) else {
                    continue;
                };
                if seen_track_ids.insert(*owner_id) {
                    tracks.push(track.clone());
                }
            }
            sort_album_tracks(&mut tracks);
            owned.tracks_by_artist.insert(artist_id, tracks);
        }
    }

    Ok(owned)
}

pub(crate) fn artist_releases_by_artist(
    db: &DbAny,
    artist_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Vec<Release>>> {
    Ok(artist_owned_entities_by_artist(db, artist_ids, true, false)?.releases_by_artist)
}

pub(crate) fn artist_tracks_by_artist(
    db: &DbAny,
    artist_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Vec<Track>>> {
    Ok(artist_owned_entities_by_artist(db, artist_ids, false, true)?.tracks_by_artist)
}
