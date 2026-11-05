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
    Listed,
    Viewer,
    artists::Artists,
    genres::Genres,
    index::Snapshot,
    pipeline::{
        Catalog,
        Context,
    },
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
    visible_in::<C>(&Context::new(db, viewer)?, ids)
}

fn visible_in<C: Catalog>(cx: &Context<'_>, ids: &[DbId]) -> Result<HashSet<DbId>, CatalogError> {
    let table = C::table(&cx.index);
    Ok(C::candidates(cx, &C::id_filter(ids.to_vec()))?
        .iter()
        .map(|slot| table.id(slot))
        .collect())
}

/// Each owner's release slots, hydrated in name order.
fn releases_by_owner(
    cx: &Context<'_>,
    owners: Vec<(DbId, Vec<u32>)>,
) -> Result<HashMap<DbId, Vec<Listed<Release>>>, CatalogError> {
    let index = &cx.index;
    let owners = owners
        .into_iter()
        .map(|(owner, mut releases)| {
            releases.sort_unstable_by_key(|release| index.releases.rank[*release as usize]);
            (owner, releases)
        })
        .collect::<Vec<_>>();
    let mut ids = owners
        .iter()
        .flat_map(|(_, releases)| releases.iter().map(|release| index.releases.id(*release)))
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    let hydrated = Releases::hydrate(cx.db, &ids)?
        .into_iter()
        .filter_map(|release| Some((DbId::from(release.db_id.clone()?), release)))
        .collect::<HashMap<_, _>>();
    Ok(owners
        .into_iter()
        .map(|(owner, releases)| {
            let releases = releases
                .iter()
                .filter_map(|release| hydrated.get(&index.releases.id(*release)).cloned())
                .collect();
            (owner, releases)
        })
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
    if owners.is_empty() {
        return Ok(Vec::new());
    }
    visible_in_order_in::<C>(&Context::new(db, viewer)?, owners)
}

fn visible_in_order_in<C: Catalog>(
    cx: &Context<'_>,
    owners: &[DbId],
) -> Result<Vec<DbId>, CatalogError> {
    let visible = visible_in::<C>(cx, owners)?;
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
) -> Result<HashMap<DbId, Vec<Listed<Track>>>, CatalogError> {
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
) -> Result<HashMap<DbId, Vec<Listed<Track>>>, CatalogError> {
    let mut tracks = HashMap::new();
    for entry in crate::services::entries::visible_ids(db, viewer, entries)? {
        tracks.insert(entry, lyra_db::tracks::get_by_entry(db, entry)?);
    }
    Ok(super::listed_groups(
        db,
        tracks,
        |track| track.db_id.clone().map(DbId::from),
        Snapshot::track_available,
    )?)
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
) -> Result<HashMap<DbId, Vec<lyra_db::chains::ReleaseChain>>, CatalogError> {
    let mut chains = HashMap::new();
    for release in visible_in_order::<Releases>(db, viewer, releases)? {
        chains.insert(release, lyra_db::chains::get_for_release(db, release)?);
    }
    Ok(chains)
}

/// The chain place of each visible track that is in a chain.
pub(crate) fn chain_places(
    db: &DbAny,
    viewer: &Viewer,
    tracks: &[DbId],
) -> Result<HashMap<DbId, TrackChain>, CatalogError> {
    if tracks.is_empty() {
        return Ok(HashMap::new());
    }
    let cx = Context::new(db, viewer)?;
    let links = cx.links();
    let tracks = visible_in_order_in::<Tracks>(&cx, tracks)?;
    Ok(lyra_db::chains::links_for_tracks(db, &tracks)?
        .into_iter()
        .filter(|(_, link)| links.sees_release(link.release_db_id))
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
) -> Result<HashMap<DbId, Vec<Listed<Release>>>, CatalogError> {
    if tracks.is_empty() {
        return Ok(HashMap::new());
    }
    let cx = Context::new(db, viewer)?;
    let index = &cx.index;
    let links = cx.links();
    let tracks = visible_in_order_in::<Tracks>(&cx, tracks)?;
    releases_by_owner(
        &cx,
        tracks
            .into_iter()
            .filter_map(|track| {
                Some((
                    track,
                    links.releases_of_track(index.tracks.slot(track)?).collect(),
                ))
            })
            .collect(),
    )
}

/// The visible releases in each visible genre.
pub(crate) fn releases_by_genre(
    db: &DbAny,
    viewer: &Viewer,
    genres: &[DbId],
) -> Result<HashMap<DbId, Vec<Listed<Release>>>, CatalogError> {
    if genres.is_empty() {
        return Ok(HashMap::new());
    }
    let cx = Context::new(db, viewer)?;
    let index = &cx.index;
    let links = cx.links();
    let genres = visible_in_order_in::<Genres>(&cx, genres)?;
    releases_by_owner(
        &cx,
        genres
            .into_iter()
            .filter_map(|genre| {
                Some((
                    genre,
                    links.releases_of_genre(index.genres.slot(genre)?).collect(),
                ))
            })
            .collect(),
    )
}

/// The artists credited on each visible owner, in credit order. `C` is the owners' catalog.
pub(crate) fn artists_by_owner<C: Catalog>(
    db: &DbAny,
    viewer: &Viewer,
    owners: &[DbId],
) -> Result<HashMap<DbId, Vec<Listed<Artist>>>, CatalogError> {
    let owners = visible_in_order::<C>(db, viewer, owners)?;
    Ok(super::listed_groups(
        db,
        lyra_db::artists::get_many_by_owner(db, &owners)?,
        |artist| artist.db_id.clone().map(DbId::from),
        Snapshot::artist_available,
    )?)
}

/// The genres of each visible release.
pub(crate) fn genres_by_release(
    db: &DbAny,
    viewer: &Viewer,
    releases: &[DbId],
) -> Result<HashMap<DbId, Vec<Listed<Genre>>>, CatalogError> {
    let releases = visible_in_order::<Releases>(db, viewer, releases)?;
    Ok(super::listed_groups(
        db,
        lyra_db::genres::get_for_releases_many(db, &releases)?,
        |genre| genre.db_id.clone().map(DbId::from),
        Snapshot::genre_available,
    )?)
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
