// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Lookups by id and by relation, limited to what the viewer can see.

use std::collections::{
    HashMap,
    HashSet,
};

use agdb::{
    DbAny,
    DbId,
};
use serde::Serialize;

use super::{
    CatalogError,
    Viewer,
    artists::Artists,
    genres::Genres,
    pipeline::Catalog,
    releases::Releases,
    tracks::Tracks,
};
use crate::services::artists::{
    ResolvedRelation,
    get_relations_many,
};
use lyra_db::{
    Artist,
    Release,
    Track,
    genres::Genre,
};

/// The ids among `ids` that the viewer can see.
pub(crate) fn visible<C: Catalog>(
    db: &DbAny,
    viewer: &Viewer,
    ids: &[DbId],
) -> Result<HashSet<DbId>, CatalogError> {
    if ids.is_empty() {
        return Ok(HashSet::new());
    }
    Ok(C::candidates(db, viewer, &C::id_filter(ids.to_vec()))?
        .into_iter()
        .collect())
}

/// The entities with these ids that the viewer can see, keyed by id.
pub(crate) fn get<C: Catalog>(
    db: &DbAny,
    viewer: &Viewer,
    ids: &[DbId],
) -> Result<HashMap<DbId, C::Item>, CatalogError> {
    let visible = visible::<C>(db, viewer, ids)?
        .into_iter()
        .collect::<Vec<_>>();
    Ok(visible
        .iter()
        .copied()
        .zip(C::hydrate(db, &visible)?)
        .collect())
}

/// The ids in `owners` that the viewer can see, each once, in order.
fn visible_in_order<C: Catalog>(
    db: &DbAny,
    viewer: &Viewer,
    owners: &[DbId],
) -> Result<Vec<DbId>, CatalogError> {
    let visible = visible::<C>(db, viewer, owners)?;
    let mut seen = HashSet::new();
    Ok(owners
        .iter()
        .copied()
        .filter(|id| visible.contains(id) && seen.insert(*id))
        .collect())
}

/// The tracks on each visible release.
pub(crate) fn tracks_by_release(
    db: &DbAny,
    viewer: &Viewer,
    releases: &[DbId],
) -> Result<HashMap<DbId, Vec<Track>>, CatalogError> {
    let mut tracks = HashMap::new();
    for release in visible_in_order::<Releases>(db, viewer, releases)? {
        let ids = lyra_db::graph::neighbor_ids(db, release, "Track")?;
        tracks.insert(release, Tracks::hydrate(db, &ids)?);
    }
    Ok(tracks)
}

/// The tracks whose audio each visible entry holds.
pub(crate) fn tracks_by_entry(
    db: &DbAny,
    viewer: &Viewer,
    entries: &[DbId],
) -> Result<HashMap<DbId, Vec<Track>>, CatalogError> {
    let mut tracks = HashMap::new();
    for &entry in entries {
        if crate::services::entries::visible_entry(db, viewer, entry)?.is_some() {
            tracks.insert(entry, lyra_db::tracks::get_by_entry(db, entry)?);
        }
    }
    Ok(tracks)
}

/// A release's chain: its tracks in play order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ReleaseChain {
    pub(crate) id: String,
    pub(crate) track_ids: Vec<i64>,
}

impl From<lyra_db::chains::ReleaseChain> for ReleaseChain {
    fn from(chain: lyra_db::chains::ReleaseChain) -> Self {
        Self {
            id: chain.id,
            track_ids: chain.track_db_ids.into_iter().map(|id| id.0).collect(),
        }
    }
}

/// A track's chain and its 0-based place in it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct TrackChain {
    pub(crate) id: String,
    pub(crate) position: u64,
}

/// The chains on each visible release, ordered by where their first track sits.
pub(crate) fn chains_by_release(
    db: &DbAny,
    viewer: &Viewer,
    releases: &[DbId],
) -> Result<HashMap<DbId, Vec<ReleaseChain>>, CatalogError> {
    let mut chains = HashMap::new();
    for release in visible_in_order::<Releases>(db, viewer, releases)? {
        let release_chains = lyra_db::chains::get_for_release(db, release)?;
        chains.insert(
            release,
            release_chains.into_iter().map(ReleaseChain::from).collect(),
        );
    }
    Ok(chains)
}

/// The chain place of each visible track that is in a chain.
pub(crate) fn chain_places(
    db: &DbAny,
    viewer: &Viewer,
    tracks: &[DbId],
) -> Result<HashMap<DbId, TrackChain>, CatalogError> {
    let tracks = visible_in_order::<Tracks>(db, viewer, tracks)?;
    Ok(lyra_db::chains::links_for_tracks(db, &tracks)?
        .into_iter()
        .map(|(track, link)| {
            (
                track,
                TrackChain {
                    id: link.chain_id,
                    position: link.position,
                },
            )
        })
        .collect())
}

/// The visible releases holding each visible track.
pub(crate) fn releases_by_track(
    db: &DbAny,
    viewer: &Viewer,
    tracks: &[DbId],
) -> Result<HashMap<DbId, Vec<Release>>, CatalogError> {
    let visible_releases = viewer.visible_releases(db)?;
    let mut releases = HashMap::new();
    for track in visible_in_order::<Tracks>(db, viewer, tracks)? {
        let ids = lyra_db::graph::inbound_neighbor_ids(db, track, "Release")?
            .into_iter()
            .filter(|release| {
                visible_releases
                    .as_ref()
                    .is_none_or(|visible| visible.contains(release))
            })
            .collect::<Vec<_>>();
        releases.insert(track, Releases::hydrate(db, &ids)?);
    }
    Ok(releases)
}

/// The visible releases in each visible genre.
pub(crate) fn releases_by_genre(
    db: &DbAny,
    viewer: &Viewer,
    genres: &[DbId],
) -> Result<HashMap<DbId, Vec<Release>>, CatalogError> {
    let visible_releases = viewer.visible_releases(db)?;
    let genres = visible_in_order::<Genres>(db, viewer, genres)?;
    let mut releases = HashMap::new();
    for (genre, ids) in lyra_db::genres::get_releases_many(db, &genres)? {
        let ids = ids
            .into_iter()
            .filter(|release| {
                visible_releases
                    .as_ref()
                    .is_none_or(|visible| visible.contains(release))
            })
            .collect::<Vec<_>>();
        releases.insert(genre, Releases::hydrate(db, &ids)?);
    }
    Ok(releases)
}

/// The artists credited on each visible owner, in credit order. `C` is the owners' catalog.
pub(crate) fn artists_by_owner<C: Catalog>(
    db: &DbAny,
    viewer: &Viewer,
    owners: &[DbId],
) -> Result<HashMap<DbId, Vec<Artist>>, CatalogError> {
    let owners = visible_in_order::<C>(db, viewer, owners)?;
    Ok(lyra_db::artists::get_many_by_owner(db, &owners)?)
}

/// The genres of each visible release.
pub(crate) fn genres_by_release(
    db: &DbAny,
    viewer: &Viewer,
    releases: &[DbId],
) -> Result<HashMap<DbId, Vec<Genre>>, CatalogError> {
    let releases = visible_in_order::<Releases>(db, viewer, releases)?;
    Ok(lyra_db::genres::get_for_releases_many(db, &releases)?)
}

/// The relations of each visible artist to other visible artists.
pub(crate) fn artist_relations(
    db: &DbAny,
    viewer: &Viewer,
    artists: &[DbId],
) -> Result<HashMap<DbId, Vec<ResolvedRelation>>, CatalogError> {
    let artists = visible_in_order::<Artists>(db, viewer, artists)?;
    let mut relations = get_relations_many(db, &artists)?;
    let related = relations
        .values()
        .flatten()
        .filter_map(|relation| relation.artist.db_id.clone().map(DbId::from))
        .collect::<Vec<_>>();
    let visible_related = visible::<Artists>(db, viewer, &related)?;
    for artist_relations in relations.values_mut() {
        artist_relations.retain(|relation| {
            relation
                .artist
                .db_id
                .clone()
                .map(DbId::from)
                .is_some_and(|id| visible_related.contains(&id))
        });
    }
    Ok(relations)
}
