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
    DbAny,
    DbElement,
    DbId,
    DbValue,
    QueryBuilder,
    QueryId,
};
use lyra_metadata::LookupHints;

use crate::db::{
    self,
    Artist,
    Release,
    Track,
};

use super::{
    ArtistProjectionIncludes,
    ArtistProjectionInfo,
    ArtistProjectionKind,
    EntityInclude,
    EntityLookupHints,
    EntityProjectionInfo,
    ProjectionEntryInfo,
    ReleaseProjectionIncludes,
    ReleaseProjectionInfo,
    ReleaseProjectionKind,
    ReleaseProjectionTrack,
    TrackProjectionIncludes,
    TrackProjectionInfo,
    TrackProjectionKind,
    dedupe_artists,
    resolve_track_artists,
};

pub(super) enum DetectedEntityType {
    Release,
    Track,
    Artist,
}

enum ResolvedEntity {
    Release(DbId, Release),
    Track(DbId, Track),
    Artist(DbId, Artist),
}

#[derive(Default)]
pub(super) struct PreFetchedIncludes {
    pub(super) external_ids: Option<HashMap<DbId, BTreeMap<String, String>>>,
    pub(super) artists_by_owner: Option<HashMap<DbId, Vec<Artist>>>,
    pub(super) releases_by_track: Option<HashMap<DbId, Vec<Release>>>,
    pub(super) track_artists: Option<HashMap<DbId, Vec<Artist>>>,
    pub(super) artist_tracks: Option<HashMap<DbId, Vec<Track>>>,
}

fn resolve_entity_id(db: &DbAny, query_id: QueryId) -> anyhow::Result<DbId> {
    match query_id {
        QueryId::Id(entity_id) => Ok(entity_id),
        QueryId::Alias(alias) => {
            if let Ok(parsed) = alias.trim().parse::<i64>()
                && parsed > 0
            {
                return Ok(DbId(parsed));
            }

            let result = db.exec(QueryBuilder::select().ids(alias.as_str()).query())?;
            let ids = result.ids();
            match ids.as_slice() {
                [entity_id] => Ok(*entity_id),
                [] => anyhow::bail!("entity alias not found: {alias}"),
                _ => anyhow::bail!("entity alias resolves to multiple ids: {alias}"),
            }
        }
    }
}

fn build_external_ids_map(db: &DbAny, entity_id: DbId) -> anyhow::Result<BTreeMap<String, String>> {
    let ids = db::external_ids::get_for_entity(db, entity_id)?;
    let mut map = BTreeMap::new();
    for id in ids {
        map.insert(id.id_type, id.id_value);
    }

    Ok(map)
}

fn track_sort_key(track: &Track) -> (u32, u32, String, i64) {
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

fn sorted_release_tracks(db: &DbAny, release_id: DbId) -> anyhow::Result<Vec<Track>> {
    let mut tracks = db::tracks::get(db, release_id)?;
    tracks.sort_by_key(track_sort_key);
    Ok(tracks)
}

fn select_track_file_path(db: &DbAny, track_id: DbId) -> anyhow::Result<Option<String>> {
    Ok(db::entries::get_by_track(db, track_id)?
        .into_iter()
        .filter(|entry| entry.kind == crate::db::entries::EntryKind::File)
        .map(|entry| entry.full_path.to_string_lossy().to_string())
        .min())
}

fn build_release_tracks_with_external_ids(
    db: &DbAny,
    release_id: DbId,
    library_root: Option<&str>,
) -> anyhow::Result<(Vec<ReleaseProjectionTrack>, LookupHints)> {
    let tracks = sorted_release_tracks(db, release_id)?;
    let mut projected = Vec::with_capacity(tracks.len());
    let mut track_lookup_hints = Vec::with_capacity(tracks.len());

    for track in tracks {
        let track_db_id = track.db_id.clone().map(Into::<DbId>::into);
        let (external_ids, artists, lookup_hints) = if let Some(track_id) = track_db_id {
            let file_path = select_track_file_path(db, track_id)?;
            (
                build_external_ids_map(db, track_id)?,
                resolve_track_artists_for_release(db, track_id, release_id)?,
                file_path
                    .as_deref()
                    .map(|path| {
                        lyra_metadata::extract_lookup_hints_from_file_path_with_library_root(
                            path,
                            library_root,
                        )
                    })
                    .unwrap_or_default(),
            )
        } else {
            (BTreeMap::new(), Vec::new(), LookupHints::default())
        };
        track_lookup_hints.push(lookup_hints.clone());
        projected.push(ReleaseProjectionTrack::from_track(
            track,
            external_ids,
            artists,
            lookup_hints.into(),
        ));
    }

    Ok((
        projected,
        lyra_metadata::infer_lookup_hints_from_tracks(&track_lookup_hints),
    ))
}

fn lookup_or_fetch<T: Clone>(
    prefetched: Option<&HashMap<DbId, T>>,
    id: DbId,
    fetch: impl FnOnce() -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    if let Some(value) = prefetched.and_then(|m| m.get(&id)) {
        Ok(value.clone())
    } else {
        fetch()
    }
}

fn resolve_track_artists_for_release(
    db: &DbAny,
    track_id: DbId,
    release_id: DbId,
) -> anyhow::Result<Vec<Artist>> {
    let direct = db::artists::get(db, track_id)?;
    if !direct.is_empty() {
        return Ok(dedupe_artists(direct));
    }

    Ok(dedupe_artists(db::artists::get(db, release_id)?))
}

fn include_not_supported(entity_type: &str, include: EntityInclude) -> anyhow::Error {
    anyhow::anyhow!(
        "include '{}' is not supported for entity_type '{}'",
        include.as_key(),
        entity_type
    )
}

pub(super) fn fetch_raw_entity(db: &DbAny, entity_id: DbId) -> anyhow::Result<DbElement> {
    let result = db.exec(QueryBuilder::select().ids(entity_id).query())?;
    result
        .elements
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("entity not found: {}", entity_id.0))
}

fn fetch_raw_entities(db: &DbAny, entity_ids: &[DbId]) -> anyhow::Result<HashMap<DbId, DbElement>> {
    if entity_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let elements = db
        .exec(QueryBuilder::select().ids(entity_ids.to_vec()).query())?
        .elements;
    Ok(elements
        .into_iter()
        .map(|element| (element.id, element))
        .collect())
}

pub(super) fn detect_entity_type(element: &DbElement) -> anyhow::Result<DetectedEntityType> {
    let db_element_id_key = DbValue::from("db_element_id");
    for kv in &element.values {
        if kv.key == db_element_id_key {
            let type_name = kv
                .value
                .string()
                .map_err(|_| anyhow::anyhow!("db_element_id is not a string"))?;
            return match type_name.as_str() {
                "Release" => Ok(DetectedEntityType::Release),
                "Track" => Ok(DetectedEntityType::Track),
                "Artist" => Ok(DetectedEntityType::Artist),
                other => anyhow::bail!("unknown entity type: {other}"),
            };
        }
    }
    anyhow::bail!("entity missing db_element_id")
}

pub(super) fn project_release(
    db: &DbAny,
    release_id: DbId,
    release: Release,
    includes: &[EntityInclude],
    library_root: Option<&str>,
    prefetched: &PreFetchedIncludes,
) -> anyhow::Result<ReleaseProjectionInfo> {
    let mut projection = ReleaseProjectionInfo {
        entity_type: ReleaseProjectionKind::Release,
        entity: release,
        lookup_hints: EntityLookupHints::default(),
        includes: ReleaseProjectionIncludes::default(),
    };
    let mut release_lookup_hints = LookupHints::default();
    for include in includes {
        match include {
            EntityInclude::ExternalIds => {
                projection.includes.external_ids = Some(lookup_or_fetch(
                    prefetched.external_ids.as_ref(),
                    release_id,
                    || build_external_ids_map(db, release_id),
                )?);
            }
            EntityInclude::Artists => {
                projection.includes.artists = Some(lookup_or_fetch(
                    prefetched.artists_by_owner.as_ref(),
                    release_id,
                    || db::artists::get(db, release_id),
                )?);
            }
            EntityInclude::Tracks => {
                let (tracks, lookup_hints) =
                    build_release_tracks_with_external_ids(db, release_id, library_root)?;
                release_lookup_hints = lookup_hints;
                projection.includes.tracks = Some(tracks);
            }
            EntityInclude::Releases | EntityInclude::Entries => {
                return Err(include_not_supported("release", *include));
            }
        }
    }

    projection.lookup_hints = release_lookup_hints.into();
    Ok(projection)
}

fn project_track(
    db: &DbAny,
    track_id: DbId,
    track: Track,
    includes: &[EntityInclude],
    prefetched: &PreFetchedIncludes,
) -> anyhow::Result<TrackProjectionInfo> {
    let mut projection = TrackProjectionInfo {
        entity_type: TrackProjectionKind::Track,
        entity: track,
        includes: TrackProjectionIncludes::default(),
    };
    for include in includes {
        match include {
            EntityInclude::ExternalIds => {
                projection.includes.external_ids = Some(lookup_or_fetch(
                    prefetched.external_ids.as_ref(),
                    track_id,
                    || build_external_ids_map(db, track_id),
                )?);
            }
            EntityInclude::Releases => {
                projection.includes.releases = Some(lookup_or_fetch(
                    prefetched.releases_by_track.as_ref(),
                    track_id,
                    || db::releases::get_by_track(db, track_id),
                )?);
            }
            EntityInclude::Artists => {
                projection.includes.artists = Some(lookup_or_fetch(
                    prefetched.track_artists.as_ref(),
                    track_id,
                    || resolve_track_artists(db, track_id),
                )?);
            }
            EntityInclude::Entries => {
                let entries = db::entries::get_by_track(db, track_id)?;
                projection.includes.entries =
                    Some(entries.into_iter().map(ProjectionEntryInfo::from).collect());
            }
            EntityInclude::Tracks => {
                return Err(include_not_supported("track", *include));
            }
        }
    }

    Ok(projection)
}

fn project_artist(
    db: &DbAny,
    artist_id: DbId,
    artist: Artist,
    includes: &[EntityInclude],
    prefetched: &PreFetchedIncludes,
) -> anyhow::Result<ArtistProjectionInfo> {
    let mut projection = ArtistProjectionInfo {
        entity_type: ArtistProjectionKind::Artist,
        entity: artist,
        includes: ArtistProjectionIncludes::default(),
    };
    for include in includes {
        match include {
            EntityInclude::ExternalIds => {
                projection.includes.external_ids = Some(lookup_or_fetch(
                    prefetched.external_ids.as_ref(),
                    artist_id,
                    || build_external_ids_map(db, artist_id),
                )?);
            }
            EntityInclude::Releases => {
                projection.includes.releases = Some(db::releases::get_by_artist(db, artist_id)?);
            }
            EntityInclude::Tracks => {
                let mut tracks =
                    lookup_or_fetch(prefetched.artist_tracks.as_ref(), artist_id, || {
                        db::tracks::get_by_artist(db, artist_id)
                    })?;
                tracks.sort_by_key(track_sort_key);
                projection.includes.tracks = Some(tracks);
            }
            EntityInclude::Artists | EntityInclude::Entries => {
                return Err(include_not_supported("artist", *include));
            }
        }
    }

    Ok(projection)
}

pub(crate) fn project_entity(
    db: &DbAny,
    query_id: QueryId,
    includes: &[EntityInclude],
    library_id: Option<DbId>,
) -> anyhow::Result<EntityProjectionInfo> {
    use agdb::DbType;

    let entity_id = resolve_entity_id(db, query_id)?;
    let element = fetch_raw_entity(db, entity_id)?;
    let entity_type = detect_entity_type(&element)?;

    let no_prefetch = PreFetchedIncludes::default();
    match entity_type {
        DetectedEntityType::Release => {
            let release = Release::from_db_element(&element)?;
            let library_root = if let Some(lib_id) = library_id {
                db::libraries::get_by_id(db, lib_id)?
                    .map(|library| library.directory.to_string_lossy().to_string())
            } else {
                None
            };
            Ok(EntityProjectionInfo::Release(project_release(
                db,
                entity_id,
                release,
                includes,
                library_root.as_deref(),
                &no_prefetch,
            )?))
        }
        DetectedEntityType::Track => {
            let track = Track::from_db_element(&element)?;
            Ok(EntityProjectionInfo::Track(project_track(
                db,
                entity_id,
                track,
                includes,
                &no_prefetch,
            )?))
        }
        DetectedEntityType::Artist => {
            let artist = Artist::from_db_element(&element)?;
            Ok(EntityProjectionInfo::Artist(project_artist(
                db,
                entity_id,
                artist,
                includes,
                &no_prefetch,
            )?))
        }
    }
}

pub(crate) fn project_entities(
    db: &DbAny,
    query_ids: Vec<QueryId>,
    includes: &[EntityInclude],
    library_id: Option<DbId>,
) -> anyhow::Result<Vec<EntityProjectionInfo>> {
    use agdb::DbType;

    if query_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut entity_ids = Vec::with_capacity(query_ids.len());
    for query_id in query_ids {
        entity_ids.push(resolve_entity_id(db, query_id)?);
    }

    let unique_entity_ids = super::dedupe_db_ids(&entity_ids);
    let elements_by_id = fetch_raw_entities(db, &unique_entity_ids)?;

    let mut resolved = Vec::with_capacity(entity_ids.len());
    let mut release_ids = Vec::new();
    let mut track_ids = Vec::new();
    let mut artist_ids = Vec::new();
    let mut seen_release_ids = HashSet::new();
    let mut seen_track_ids = HashSet::new();
    let mut seen_artist_ids = HashSet::new();

    for entity_id in entity_ids {
        let element = elements_by_id
            .get(&entity_id)
            .ok_or_else(|| anyhow::anyhow!("entity not found: {}", entity_id.0))?;
        match detect_entity_type(element)? {
            DetectedEntityType::Release => {
                if seen_release_ids.insert(entity_id) {
                    release_ids.push(entity_id);
                }
                resolved.push(ResolvedEntity::Release(
                    entity_id,
                    Release::from_db_element(element)?,
                ));
            }
            DetectedEntityType::Track => {
                if seen_track_ids.insert(entity_id) {
                    track_ids.push(entity_id);
                }
                resolved.push(ResolvedEntity::Track(
                    entity_id,
                    Track::from_db_element(element)?,
                ));
            }
            DetectedEntityType::Artist => {
                if seen_artist_ids.insert(entity_id) {
                    artist_ids.push(entity_id);
                }
                resolved.push(ResolvedEntity::Artist(
                    entity_id,
                    Artist::from_db_element(element)?,
                ));
            }
        }
    }

    let has_include = |target| includes.iter().any(|include| *include == target);
    let library_root = if has_include(EntityInclude::Tracks) {
        if let Some(lib_id) = library_id {
            db::libraries::get_by_id(db, lib_id)?
                .map(|library| library.directory.to_string_lossy().to_string())
        } else {
            None
        }
    } else {
        None
    };

    let artists_by_owner = if has_include(EntityInclude::Artists) && !release_ids.is_empty() {
        Some(db::artists::get_many_by_owner(db, &release_ids)?)
    } else {
        None
    };
    let releases_by_track = if has_include(EntityInclude::Releases) && !track_ids.is_empty() {
        Some(db::releases::get_by_tracks(db, &track_ids)?)
    } else {
        None
    };
    let track_artists = if has_include(EntityInclude::Artists) && !track_ids.is_empty() {
        let ctx = super::TrackArtistContext {
            releases_by_track: releases_by_track.as_ref(),
            artists_by_release: artists_by_owner.as_ref(),
        };
        Some(super::resolve_track_artists_with_context(
            db, &track_ids, &ctx,
        )?)
    } else {
        None
    };
    let prefetched = PreFetchedIncludes {
        external_ids: if has_include(EntityInclude::ExternalIds) {
            let mut map = HashMap::new();
            for entity_id in &unique_entity_ids {
                map.insert(*entity_id, build_external_ids_map(db, *entity_id)?);
            }
            Some(map)
        } else {
            None
        },
        artists_by_owner,
        releases_by_track,
        track_artists,
        artist_tracks: if has_include(EntityInclude::Tracks) && !artist_ids.is_empty() {
            let mut map = db::tracks::get_direct_many(db, &artist_ids)?;
            for tracks in map.values_mut() {
                tracks.sort_by_key(track_sort_key);
            }
            Some(map)
        } else {
            None
        },
    };

    let mut projections = Vec::with_capacity(resolved.len());
    for entity in resolved {
        match entity {
            ResolvedEntity::Release(release_id, release) => {
                projections.push(EntityProjectionInfo::Release(project_release(
                    db,
                    release_id,
                    release,
                    includes,
                    library_root.as_deref(),
                    &prefetched,
                )?));
            }
            ResolvedEntity::Track(track_id, track) => {
                projections.push(EntityProjectionInfo::Track(project_track(
                    db,
                    track_id,
                    track,
                    includes,
                    &prefetched,
                )?));
            }
            ResolvedEntity::Artist(artist_id, artist) => {
                projections.push(EntityProjectionInfo::Artist(project_artist(
                    db,
                    artist_id,
                    artist,
                    includes,
                    &prefetched,
                )?));
            }
        }
    }

    Ok(projections)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::{
        insert_release,
        new_test_db,
    };

    #[test]
    fn resolve_entity_id_accepts_numeric_aliases() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Alias Release")?;

        assert_eq!(
            resolve_entity_id(&db, QueryId::Alias(release_id.0.to_string()))?,
            release_id
        );
        Ok(())
    }

    #[test]
    fn project_entity_rejects_entries_include_for_releases() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Projection Release")?;

        let err = project_entity(
            &db,
            QueryId::Id(release_id),
            &[EntityInclude::Entries],
            None,
        )
        .expect_err("release projections should reject entry includes");

        assert!(err.to_string().contains("not supported"));
        Ok(())
    }
}
