// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod credits;

use std::collections::{
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
    Entry,
    Release,
    Track,
};

use super::{
    ArtistProjectionIncludes,
    ArtistProjectionInfo,
    ArtistProjectionKind,
    CreditedArtistProjectionInfo,
    EntityInclude,
    EntityLookupHints,
    EntityProjectionInfo,
    ExternalIdsByProvider,
    ProjectionEntryInfo,
    ReleaseProjectionIncludes,
    ReleaseProjectionInfo,
    ReleaseProjectionKind,
    ReleaseProjectionTrack,
    TrackProjectionIncludes,
    TrackProjectionInfo,
    TrackProjectionKind,
    relations,
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
    pub(super) external_ids: Option<HashMap<DbId, ExternalIdsByProvider>>,
    pub(super) artists_by_owner: Option<HashMap<DbId, Vec<Artist>>>,
    pub(super) release_tracks: Option<HashMap<DbId, Vec<Track>>>,
    pub(super) releases_by_track: Option<HashMap<DbId, Vec<Release>>>,
    pub(super) track_artists: Option<HashMap<DbId, Vec<Artist>>>,
    pub(super) entries_by_track: Option<HashMap<DbId, Vec<Entry>>>,
    pub(super) artist_releases: Option<HashMap<DbId, Vec<Release>>>,
    pub(super) artist_tracks: Option<HashMap<DbId, Vec<Track>>>,
    pub(super) credits_by_owner: Option<HashMap<DbId, Vec<CreditedArtistProjectionInfo>>>,
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

fn build_release_tracks_with_external_ids(
    db: &DbAny,
    release_id: DbId,
    library_root: Option<&str>,
    prefetched: &PreFetchedIncludes,
) -> anyhow::Result<(Vec<ReleaseProjectionTrack>, LookupHints)> {
    let tracks = lookup_or_fetch(prefetched.release_tracks.as_ref(), release_id, || {
        relations::release_tracks(db, release_id)
    })?;
    let track_ids = relations::db_ids_from_tracks(&tracks);
    let artists_by_track = relations::track_artists_for_release(
        db,
        &track_ids,
        release_id,
        prefetched.artists_by_owner.as_ref(),
    )?;
    let mut projected = Vec::with_capacity(tracks.len());
    let mut track_lookup_hints = Vec::with_capacity(tracks.len());

    for track in tracks {
        let track_db_id = track.db_id.clone().map(Into::<DbId>::into);
        let (external_ids, artists, lookup_hints) = if let Some(track_id) = track_db_id {
            let entries = lookup_or_fetch(prefetched.entries_by_track.as_ref(), track_id, || {
                relations::track_entries(db, track_id)
            })?;
            (
                lookup_or_fetch(prefetched.external_ids.as_ref(), track_id, || {
                    relations::external_ids_for_entity(db, track_id)
                })?,
                artists_by_track.get(&track_id).cloned().unwrap_or_default(),
                relations::lookup_hints_for_entries(&entries, library_root),
            )
        } else {
            (
                ExternalIdsByProvider::new(),
                Vec::new(),
                LookupHints::default(),
            )
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

fn include_not_supported(entity_type: &str, include: EntityInclude) -> anyhow::Error {
    anyhow::anyhow!(
        "include '{}' is not supported for entity_type '{}'",
        include.as_key(),
        entity_type
    )
}

pub(super) fn fetch_entity_element(db: &DbAny, entity_id: DbId) -> anyhow::Result<DbElement> {
    let result = db.exec(QueryBuilder::select().ids(entity_id).query())?;
    result
        .elements
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("entity not found: {}", entity_id.0))
}

fn fetch_entity_elements(
    db: &DbAny,
    entity_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, DbElement>> {
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
                    || relations::external_ids_for_entity(db, release_id),
                )?);
            }
            EntityInclude::Artists => {
                projection.includes.artists = Some(lookup_or_fetch(
                    prefetched.artists_by_owner.as_ref(),
                    release_id,
                    || {
                        relations::raw_artists_by_owner(db, &[release_id])
                            .map(|mut map| map.remove(&release_id).unwrap_or_default())
                    },
                )?);
            }
            EntityInclude::Tracks => {
                let (tracks, lookup_hints) = build_release_tracks_with_external_ids(
                    db,
                    release_id,
                    library_root,
                    prefetched,
                )?;
                release_lookup_hints = lookup_hints;
                projection.includes.tracks = Some(tracks);
            }
            EntityInclude::Credits => {
                projection.includes.credits = Some(lookup_or_fetch(
                    prefetched.credits_by_owner.as_ref(),
                    release_id,
                    || credits::fetch_release(db, release_id),
                )?);
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
                    || relations::external_ids_for_entity(db, track_id),
                )?);
            }
            EntityInclude::Releases => {
                projection.includes.releases = Some(lookup_or_fetch(
                    prefetched.releases_by_track.as_ref(),
                    track_id,
                    || {
                        relations::track_releases_by_track(db, &[track_id])
                            .map(|mut map| map.remove(&track_id).unwrap_or_default())
                    },
                )?);
            }
            EntityInclude::Artists => {
                projection.includes.artists = Some(lookup_or_fetch(
                    prefetched.track_artists.as_ref(),
                    track_id,
                    || {
                        relations::track_artists_by_track(db, &[track_id], None, None)
                            .map(|mut map| map.remove(&track_id).unwrap_or_default())
                    },
                )?);
            }
            EntityInclude::Entries => {
                let entries =
                    lookup_or_fetch(prefetched.entries_by_track.as_ref(), track_id, || {
                        relations::track_entries(db, track_id)
                    })?;
                projection.includes.entries =
                    Some(entries.into_iter().map(ProjectionEntryInfo::from).collect());
            }
            EntityInclude::Credits => {
                projection.includes.credits = Some(lookup_or_fetch(
                    prefetched.credits_by_owner.as_ref(),
                    track_id,
                    || credits::fetch_track(db, track_id),
                )?);
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
                    || relations::external_ids_for_entity(db, artist_id),
                )?);
            }
            EntityInclude::Releases => {
                projection.includes.releases = Some(lookup_or_fetch(
                    prefetched.artist_releases.as_ref(),
                    artist_id,
                    || {
                        relations::artist_releases_by_artist(db, &[artist_id])
                            .map(|mut map| map.remove(&artist_id).unwrap_or_default())
                    },
                )?);
            }
            EntityInclude::Tracks => {
                projection.includes.tracks = Some(lookup_or_fetch(
                    prefetched.artist_tracks.as_ref(),
                    artist_id,
                    || {
                        relations::artist_tracks_by_artist(db, &[artist_id])
                            .map(|mut map| map.remove(&artist_id).unwrap_or_default())
                    },
                )?);
            }
            EntityInclude::Artists | EntityInclude::Entries | EntityInclude::Credits => {
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
    let element = fetch_entity_element(db, entity_id)?;
    let entity_type = detect_entity_type(&element)?;

    let no_prefetch = PreFetchedIncludes::default();
    match entity_type {
        DetectedEntityType::Release => {
            let release = Release::from_db_element(&element)?;
            let library_root = if let Some(lib_id) = library_id {
                db::libraries::get_by_id(db, lib_id)?
                    .map(|library| library.path.to_string_lossy().to_string())
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
    let elements_by_id = fetch_entity_elements(db, &unique_entity_ids)?;

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

    let has_include = |target| includes.contains(&target);
    let library_root = if has_include(EntityInclude::Tracks) {
        if let Some(lib_id) = library_id {
            db::libraries::get_by_id(db, lib_id)?
                .map(|library| library.path.to_string_lossy().to_string())
        } else {
            None
        }
    } else {
        None
    };

    let release_tracks = if has_include(EntityInclude::Tracks) && !release_ids.is_empty() {
        Some(relations::release_tracks_by_release(db, &release_ids)?)
    } else {
        None
    };
    let release_track_ids = release_tracks
        .as_ref()
        .map(|tracks_by_release| {
            tracks_by_release
                .values()
                .flat_map(|tracks| relations::db_ids_from_tracks(tracks))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let needs_release_artists = (has_include(EntityInclude::Artists)
        || has_include(EntityInclude::Tracks))
        && !release_ids.is_empty();
    let artists_by_owner = if needs_release_artists {
        Some(relations::raw_artists_by_owner(db, &release_ids)?)
    } else {
        None
    };
    let needs_releases_by_track = (has_include(EntityInclude::Releases)
        || has_include(EntityInclude::Artists)
        || has_include(EntityInclude::Credits))
        && !track_ids.is_empty();
    let releases_by_track = if needs_releases_by_track {
        Some(relations::track_releases_by_track(db, &track_ids)?)
    } else {
        None
    };
    let track_artists = if has_include(EntityInclude::Artists) && !track_ids.is_empty() {
        Some(relations::track_artists_by_track(
            db,
            &track_ids,
            releases_by_track.as_ref(),
            artists_by_owner.as_ref(),
        )?)
    } else {
        None
    };
    let credits_by_owner = if has_include(EntityInclude::Credits) {
        Some(credits::prefetch_by_owner(
            db,
            &release_ids,
            &track_ids,
            releases_by_track.as_ref(),
        )?)
    } else {
        None
    };
    let mut external_id_ids = Vec::new();
    if has_include(EntityInclude::ExternalIds) {
        external_id_ids.extend(unique_entity_ids.iter().copied());
    }
    if has_include(EntityInclude::Tracks) {
        external_id_ids.extend(release_track_ids.iter().copied());
    }

    let mut entry_track_ids = Vec::new();
    if has_include(EntityInclude::Entries) {
        entry_track_ids.extend(track_ids.iter().copied());
    }
    if has_include(EntityInclude::Tracks) {
        entry_track_ids.extend(release_track_ids.iter().copied());
    }

    let artist_owned_entities = relations::artist_owned_entities_by_artist(
        db,
        &artist_ids,
        has_include(EntityInclude::Releases),
        has_include(EntityInclude::Tracks),
    )?;

    let prefetched = PreFetchedIncludes {
        external_ids: if external_id_ids.is_empty() {
            None
        } else {
            Some(relations::external_ids_by_entity(db, &external_id_ids)?)
        },
        artists_by_owner,
        release_tracks,
        releases_by_track,
        track_artists,
        entries_by_track: if entry_track_ids.is_empty() {
            None
        } else {
            Some(relations::entries_by_track(db, &entry_track_ids)?)
        },
        artist_releases: if has_include(EntityInclude::Releases) && !artist_ids.is_empty() {
            Some(artist_owned_entities.releases_by_artist)
        } else {
            None
        },
        artist_tracks: if has_include(EntityInclude::Tracks) && !artist_ids.is_empty() {
            Some(artist_owned_entities.tracks_by_artist)
        } else {
            None
        },
        credits_by_owner,
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
        connect_artist,
        insert_artist,
        insert_release,
        insert_track,
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

    #[test]
    fn project_entities_prefetches_artist_tracks_through_credits() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let artist_id = insert_artist(&mut db, "Track Artist")?;
        let track_id = insert_track(&mut db, "Credited Track")?;
        connect_artist(&mut db, track_id, artist_id)?;

        let projections = project_entities(
            &db,
            vec![QueryId::Id(artist_id)],
            &[EntityInclude::Tracks],
            None,
        )?;
        let EntityProjectionInfo::Artist(artist) = &projections[0] else {
            panic!("expected artist projection");
        };
        let tracks = artist.includes.tracks.as_ref().expect("tracks included");

        assert_eq!(tracks.len(), 1);
        assert_eq!(tracks[0].track_title, "Credited Track");
        Ok(())
    }
}
