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

use super::{
    CatalogError,
    Viewer,
    artists::Artists,
    genres::Genres,
    pipeline::Catalog,
    releases::Releases,
    tracks::Tracks,
};
use crate::{
    db::{
        self,
        Artist,
        Release,
        Track,
        genres::Genre,
    },
    services::artists::{
        ResolvedRelation,
        get_relations_many,
    },
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
        let ids = db::graph::neighbor_ids(db, release, "Track")?;
        tracks.insert(release, Tracks::hydrate(db, &ids)?);
    }
    Ok(tracks)
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
        let ids = db::graph::inbound_neighbor_ids(db, track, "Release")?
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
    for (genre, ids) in db::genres::get_releases_many(db, &genres)? {
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
    Ok(db::artists::get_many_by_owner(db, &owners)?)
}

/// The genres of each visible release.
pub(crate) fn genres_by_release(
    db: &DbAny,
    viewer: &Viewer,
    releases: &[DbId],
) -> Result<HashMap<DbId, Vec<Genre>>, CatalogError> {
    let releases = visible_in_order::<Releases>(db, viewer, releases)?;
    Ok(db::genres::get_for_releases_many(db, &releases)?)
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
