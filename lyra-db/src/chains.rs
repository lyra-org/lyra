// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Track chains: ordered groups of tracks from one release that play together.
//!
//! A `Chain` node hangs off its release by an owned edge and reaches each
//! member track by an edge holding the member's 0-based [`POSITION_KEY`].

use std::collections::{
    HashMap,
    HashSet,
};

use crate::nanoid;
use agdb::{
    CountComparison,
    DbAny,
    DbElement,
    DbId,
    DbValue,
    QueryBuilder,
};

use super::DbAccess;
use super::NodeId;

const POSITION_KEY: &str = "chain_position";

#[derive(DbElement, Clone, Debug)]
pub struct Chain {
    pub db_id: Option<NodeId>,
    pub id: String,
}

/// A chain with its member tracks in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReleaseChain {
    pub id: String,
    pub track_db_ids: Vec<DbId>,
}

/// A track's place in its chain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainLink {
    pub chain_db_id: DbId,
    pub chain_id: String,
    /// The release the chain hangs off.
    pub release_db_id: DbId,
    pub position: u64,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ChainError {
    #[error("release not found")]
    ReleaseNotFound,
    #[error("a chain needs at least two tracks")]
    TooShort,
    #[error("track {} is listed more than once", .0.0)]
    DuplicateTrack(DbId),
    #[error("track {} is not on the release", .0.0)]
    TrackNotOnRelease(DbId),
    #[error("track {} is already in a chain on another release", .0.0)]
    ChainedElsewhere(DbId),
}

/// The release's chains, ordered by where their first track sits on the release.
pub fn get_for_release(
    db: &impl DbAccess,
    release_db_id: DbId,
) -> anyhow::Result<Vec<ReleaseChain>> {
    let chain_db_ids = super::graph::neighbor_ids(db, release_db_id, "Chain")?;
    if chain_db_ids.is_empty() {
        return Ok(Vec::new());
    }
    let chains: HashMap<DbId, Chain> =
        super::graph::bulk_fetch_typed(db, chain_db_ids.clone(), "Chain")?;
    let mut release_chains = Vec::with_capacity(chain_db_ids.len());
    for chain_db_id in chain_db_ids {
        let Some(chain) = chains.get(&chain_db_id) else {
            continue;
        };
        let mut members = member_edges(db, chain_db_id)?;
        members.sort_by_key(|(_, position)| *position);
        release_chains.push(ReleaseChain {
            id: chain.id.clone(),
            track_db_ids: members.into_iter().map(|(track, _)| track).collect(),
        });
    }

    let first_tracks = release_chains
        .iter()
        .filter_map(|chain| chain.track_db_ids.first().copied())
        .collect::<Vec<_>>();
    let tracks = super::tracks::get_by_ids(db, &first_tracks)?;
    let place = |chain: &ReleaseChain| {
        let track = chain
            .track_db_ids
            .first()
            .and_then(|track_db_id| tracks.get(track_db_id));
        (
            track.and_then(|track| track.disc),
            track.and_then(|track| track.track),
        )
    };
    release_chains.sort_by(|left, right| {
        let (left_disc, left_track) = place(left);
        let (right_disc, right_track) = place(right);
        super::compare_option(&left_disc, &right_disc)
            .then_with(|| super::compare_option(&left_track, &right_track))
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(release_chains)
}

/// The chain place of each of `track_db_ids` that is chained.
pub fn links_for_tracks(
    db: &impl DbAccess,
    track_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, ChainLink>> {
    if track_db_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let wanted: HashSet<DbId> = track_db_ids.iter().copied().collect();
    // Chains are few and hand-made, so one walk over every member edge beats a
    // search per track.
    let edges = db.exec(
        QueryBuilder::select()
            .search()
            .from("chains")
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(3))
            .and()
            .keys(POSITION_KEY)
            .query(),
    )?;
    let mut positions = Vec::new();
    for edge in edges.elements {
        if !wanted.contains(&edge.to) {
            continue;
        }
        if let Some(position) = edge_position(&edge.values) {
            positions.push((edge.to, edge.from, position));
        }
    }
    if positions.is_empty() {
        return Ok(HashMap::new());
    }
    let chain_db_ids = super::dedup_positive_ids(
        &positions
            .iter()
            .map(|(_, chain, _)| *chain)
            .collect::<Vec<_>>(),
    );
    let mut releases = HashMap::new();
    for chain_db_id in &chain_db_ids {
        if let Some(release) =
            super::graph::inbound_neighbor_ids(db, *chain_db_id, "Release")?.first()
        {
            releases.insert(*chain_db_id, *release);
        }
    }
    let chains: HashMap<DbId, Chain> = super::graph::bulk_fetch_typed(db, chain_db_ids, "Chain")?;
    Ok(positions
        .into_iter()
        .filter_map(|(track_db_id, chain_db_id, position)| {
            let chain = chains.get(&chain_db_id)?;
            Some((
                track_db_id,
                ChainLink {
                    chain_db_id,
                    chain_id: chain.id.clone(),
                    release_db_id: *releases.get(&chain_db_id)?,
                    position,
                },
            ))
        })
        .collect())
}

/// Replace every chain on the release with `chains`, each a list of its
/// member tracks in order. An empty list clears the release's chains.
pub fn replace_for_release(
    db: &mut DbAny,
    release_db_id: DbId,
    chains: &[Vec<DbId>],
) -> anyhow::Result<Result<Vec<ReleaseChain>, ChainError>> {
    db.transaction_mut(|t| replace_for_release_inside_tx(t, release_db_id, chains))
}

fn replace_for_release_inside_tx(
    db: &mut impl DbAccess,
    release_db_id: DbId,
    chains: &[Vec<DbId>],
) -> anyhow::Result<Result<Vec<ReleaseChain>, ChainError>> {
    if super::releases::get_by_id(db, release_db_id)?.is_none() {
        return Ok(Err(ChainError::ReleaseNotFound));
    }
    if let Err(error) = validate(db, release_db_id, chains)? {
        return Ok(Err(error));
    }

    remove_ids(db, &super::graph::neighbor_ids(db, release_db_id, "Chain")?)?;
    for track_db_ids in chains {
        let chain_db_id = db
            .exec_mut(
                QueryBuilder::insert()
                    .element(&Chain {
                        db_id: None,
                        id: nanoid(),
                    })
                    .query(),
            )?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("chains")
                .to(chain_db_id)
                .query(),
        )?;
        super::graph::ensure_owned_edge(db, release_db_id, chain_db_id)?;
        let positions = (0_u64..)
            .take(track_db_ids.len())
            .map(|position| vec![(POSITION_KEY, position).into()])
            .collect::<Vec<_>>();
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(chain_db_id)
                .to(track_db_ids.as_slice())
                .values(positions)
                .query(),
        )?;
    }
    Ok(Ok(get_for_release(db, release_db_id)?))
}

fn validate(
    db: &impl DbAccess,
    release_db_id: DbId,
    chains: &[Vec<DbId>],
) -> anyhow::Result<Result<(), ChainError>> {
    let release_tracks: HashSet<DbId> = super::graph::neighbor_ids(db, release_db_id, "Track")?
        .into_iter()
        .collect();
    let release_chains: HashSet<DbId> = super::graph::neighbor_ids(db, release_db_id, "Chain")?
        .into_iter()
        .collect();
    let mut seen = HashSet::new();
    for track_db_ids in chains {
        if track_db_ids.len() < 2 {
            return Ok(Err(ChainError::TooShort));
        }
        for &track_db_id in track_db_ids {
            if !seen.insert(track_db_id) {
                return Ok(Err(ChainError::DuplicateTrack(track_db_id)));
            }
            if !release_tracks.contains(&track_db_id) {
                return Ok(Err(ChainError::TrackNotOnRelease(track_db_id)));
            }
            if super::graph::inbound_neighbor_ids(db, track_db_id, "Chain")?
                .iter()
                .any(|chain_db_id| !release_chains.contains(chain_db_id))
            {
                return Ok(Err(ChainError::ChainedElsewhere(track_db_id)));
            }
        }
    }
    Ok(Ok(()))
}

/// Hand every chain the `from` release owns to the `to` release, members and places unchanged,
/// for a merge that moves all of `from`'s tracks to `to`.
pub fn move_release_chains(
    db: &mut impl DbAccess,
    from_release_db_id: DbId,
    to_release_db_id: DbId,
) -> anyhow::Result<()> {
    for chain_db_id in super::graph::neighbor_ids(db, from_release_db_id, "Chain")? {
        super::graph::remove_edges_between(db, from_release_db_id, chain_db_id)?;
        super::graph::ensure_owned_edge(db, to_release_db_id, chain_db_id)?;
    }
    Ok(())
}

/// Remove the chains the track belongs to on the release it is leaving.
pub(crate) fn remove_for_departing_track(
    db: &mut impl DbAccess,
    release_db_id: DbId,
    track_db_id: DbId,
) -> anyhow::Result<()> {
    let owned: HashSet<DbId> = super::graph::neighbor_ids(db, release_db_id, "Chain")?
        .into_iter()
        .collect();
    let departing = super::graph::inbound_neighbor_ids(db, track_db_id, "Chain")?
        .into_iter()
        .filter(|chain_db_id| owned.contains(chain_db_id))
        .collect::<Vec<_>>();
    remove_ids(db, &departing)
}

/// Hands `from`'s place in its chains to `to`, and drops `to`'s own places, renumbering what
/// remains; a chain left with fewer than two tracks goes.
pub fn hand_over(db: &mut impl DbAccess, from: DbId, to: DbId) -> anyhow::Result<()> {
    let mut chain_db_ids = super::graph::inbound_neighbor_ids(db, to, "Chain")?;
    chain_db_ids.extend(super::graph::inbound_neighbor_ids(db, from, "Chain")?);
    chain_db_ids.sort_unstable();
    chain_db_ids.dedup();
    for chain_db_id in chain_db_ids {
        let mut members = member_edges(db, chain_db_id)?;
        members.sort_by_key(|(_, position)| *position);
        let tracks = members
            .into_iter()
            .map(|(track, _)| track)
            .filter(|track| *track != to)
            .map(|track| if track == from { to } else { track })
            .collect::<Vec<_>>();
        super::graph::remove_edges_between(db, chain_db_id, to)?;
        super::graph::remove_edges_between(db, chain_db_id, from)?;
        if tracks.len() < 2 {
            remove_ids(db, &[chain_db_id])?;
            continue;
        }
        if tracks.contains(&to) {
            db.exec_mut(
                QueryBuilder::insert()
                    .edges()
                    .from(chain_db_id)
                    .to(to)
                    .values_uniform([(POSITION_KEY, 0_u64).into()])
                    .query(),
            )?;
        }
        renumber(db, chain_db_id, &tracks)?;
    }
    Ok(())
}

/// Numbers the chain's member edges in the order of `tracks`.
fn renumber(db: &mut impl DbAccess, chain_db_id: DbId, tracks: &[DbId]) -> anyhow::Result<()> {
    for (position, &track) in (0_u64..).zip(tracks) {
        for edge in super::graph::direct_edge_ids(db, chain_db_id, track)? {
            db.exec_mut(
                QueryBuilder::insert()
                    .values_uniform([(POSITION_KEY, position).into()])
                    .ids(edge)
                    .query(),
            )?;
        }
    }
    Ok(())
}

/// Remove the chains owned by, or holding, a node that is about to be deleted.
pub(crate) fn remove_for_entity(db: &mut impl DbAccess, node_db_id: DbId) -> anyhow::Result<()> {
    let mut chain_db_ids = super::graph::neighbor_ids(db, node_db_id, "Chain")?;
    chain_db_ids.extend(super::graph::inbound_neighbor_ids(db, node_db_id, "Chain")?);
    remove_ids(db, &chain_db_ids)
}

fn remove_ids(db: &mut impl DbAccess, chain_db_ids: &[DbId]) -> anyhow::Result<()> {
    if !chain_db_ids.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(chain_db_ids).query())?;
    }
    Ok(())
}

fn member_edges(db: &impl DbAccess, chain_db_id: DbId) -> anyhow::Result<Vec<(DbId, u64)>> {
    Ok(db
        .exec(
            QueryBuilder::select()
                .search()
                .from(chain_db_id)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .and()
                .keys(POSITION_KEY)
                .query(),
        )?
        .elements
        .into_iter()
        .filter_map(|edge| Some((edge.to, edge_position(&edge.values)?)))
        .collect())
}

fn edge_position(values: &[agdb::DbKeyValue]) -> Option<u64> {
    values.iter().find_map(|kv| match (&kv.key, &kv.value) {
        (DbValue::String(key), DbValue::U64(position)) if key == POSITION_KEY => Some(*position),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::{
        insert_release,
        insert_track,
    };
    use crate::test_db::new_test_db;

    struct Urn {
        release: DbId,
        tracks: Vec<DbId>,
    }

    fn release_with_tracks(db: &mut DbAny, title: &str, count: u32) -> anyhow::Result<Urn> {
        let release = insert_release(db, title)?;
        let mut tracks = Vec::new();
        for number in 1..=count {
            let track_db_id = insert_track(db, &format!("{title} {number}"))?;
            let mut track = crate::tracks::get_by_id(db, track_db_id)?.expect("track");
            track.disc = Some(1);
            track.track = Some(number);
            crate::tracks::update(db, &track)?;
            crate::releases::link_track(db, release, track_db_id)?;
            tracks.push(track_db_id);
        }
        Ok(Urn { release, tracks })
    }

    fn members(chains: &[ReleaseChain]) -> Vec<Vec<DbId>> {
        chains
            .iter()
            .map(|chain| chain.track_db_ids.clone())
            .collect()
    }

    fn chain_count(db: &DbAny) -> anyhow::Result<usize> {
        Ok(crate::graph::neighbor_ids(db, "chains", "Chain")?.len())
    }

    #[test]
    fn handing_over_within_one_chain_keeps_it_valid() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let urn = release_with_tracks(&mut db, "Urn", 4)?;
        let t = &urn.tracks;
        replace_for_release(&mut db, urn.release, &[vec![t[0], t[1], t[2], t[3]]])??;

        // t[1] takes t[3]'s place and leaves its own.
        db.transaction_mut(|tx| hand_over(tx, t[3], t[1]))?;
        let _ = crate::releases::unlink_track(&mut db, urn.release, t[3])?;
        assert_eq!(
            members(&get_for_release(&db, urn.release)?),
            vec![vec![t[0], t[2], t[1]]]
        );
        assert_eq!(links_for_tracks(&db, &[t[1]])?[&t[1]].position, 2);

        // A chain left with one track goes.
        let pair = release_with_tracks(&mut db, "Pair", 2)?;
        let p = &pair.tracks;
        replace_for_release(&mut db, pair.release, &[vec![p[0], p[1]]])??;
        db.transaction_mut(|tx| hand_over(tx, p[1], p[0]))?;
        assert!(get_for_release(&db, pair.release)?.is_empty());
        Ok(())
    }

    #[test]
    fn handing_over_takes_the_place_in_another_chain() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let old = release_with_tracks(&mut db, "Old", 3)?;
        let new = release_with_tracks(&mut db, "New", 2)?;
        replace_for_release(&mut db, old.release, std::slice::from_ref(&old.tracks))??;
        replace_for_release(&mut db, new.release, std::slice::from_ref(&new.tracks))??;

        db.transaction_mut(|tx| hand_over(tx, new.tracks[1], old.tracks[0]))?;
        assert_eq!(
            members(&get_for_release(&db, old.release)?),
            vec![vec![old.tracks[1], old.tracks[2]]]
        );
        let links = links_for_tracks(&db, &[old.tracks[0]])?;
        assert_eq!(links[&old.tracks[0]].position, 1);
        assert_eq!(links[&old.tracks[0]].release_db_id, new.release);
        Ok(())
    }

    #[test]
    fn chains_read_back_in_member_and_release_order() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let urn = release_with_tracks(&mut db, "Urn", 6)?;
        let t = &urn.tracks;

        let stored =
            replace_for_release(&mut db, urn.release, &[vec![t[5], t[4]], vec![t[1], t[2]]])??;

        assert_eq!(members(&stored), vec![vec![t[1], t[2]], vec![t[5], t[4]]]);
        assert_eq!(get_for_release(&db, urn.release)?, stored);
        let links = links_for_tracks(&db, &[t[0], t[2], t[5]])?;
        assert_eq!(links.len(), 2);
        assert_eq!(links[&t[2]].position, 1);
        assert_eq!(links[&t[2]].chain_id, stored[0].id);
        assert_eq!(links[&t[5]].position, 0);
        assert_eq!(links[&t[5]].chain_id, stored[1].id);
        Ok(())
    }

    #[test]
    fn replacing_swaps_the_whole_set_and_an_empty_set_clears_it() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let urn = release_with_tracks(&mut db, "Urn", 4)?;
        let t = &urn.tracks;
        replace_for_release(&mut db, urn.release, &[vec![t[0], t[1]]])??;

        let second = replace_for_release(&mut db, urn.release, &[vec![t[1], t[2], t[3]]])??;
        assert_eq!(members(&second), vec![vec![t[1], t[2], t[3]]]);
        assert_eq!(chain_count(&db)?, 1);
        assert!(!links_for_tracks(&db, &[t[0]])?.contains_key(&t[0]));

        assert!(replace_for_release(&mut db, urn.release, &[])??.is_empty());
        assert!(get_for_release(&db, urn.release)?.is_empty());
        assert_eq!(chain_count(&db)?, 0);
        Ok(())
    }

    #[test]
    fn invalid_sets_are_rejected_and_keep_the_stored_chains() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let urn = release_with_tracks(&mut db, "Urn", 4)?;
        let other = release_with_tracks(&mut db, "Other", 1)?;
        let t = &urn.tracks;
        let stored = replace_for_release(&mut db, urn.release, &[vec![t[0], t[1]]])??;

        let cases = [
            (vec![vec![t[2]]], ChainError::TooShort),
            (vec![vec![t[2], t[2]]], ChainError::DuplicateTrack(t[2])),
            (
                vec![vec![t[0], t[1]], vec![t[1], t[2]]],
                ChainError::DuplicateTrack(t[1]),
            ),
            (
                vec![vec![t[2], other.tracks[0]]],
                ChainError::TrackNotOnRelease(other.tracks[0]),
            ),
        ];
        for (chains, expected) in cases {
            assert_eq!(
                replace_for_release(&mut db, urn.release, &chains)?,
                Err(expected)
            );
            assert_eq!(get_for_release(&db, urn.release)?, stored);
        }
        assert_eq!(
            replace_for_release(&mut db, t[0], &[])?,
            Err(ChainError::ReleaseNotFound)
        );
        Ok(())
    }

    #[test]
    fn a_track_chained_on_one_release_cannot_join_a_chain_on_another() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let urn = release_with_tracks(&mut db, "Urn", 2)?;
        let other = release_with_tracks(&mut db, "Urn (Deluxe)", 1)?;
        let t = &urn.tracks;
        crate::releases::link_track(&mut db, other.release, t[1])?;
        let stored = replace_for_release(&mut db, urn.release, std::slice::from_ref(t))??;

        assert_eq!(
            replace_for_release(&mut db, other.release, &[vec![other.tracks[0], t[1]]])?,
            Err(ChainError::ChainedElsewhere(t[1]))
        );
        assert_eq!(get_for_release(&db, urn.release)?, stored);
        assert!(get_for_release(&db, other.release)?.is_empty());
        Ok(())
    }

    #[test]
    fn deleting_a_member_removes_its_whole_chain() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let urn = release_with_tracks(&mut db, "Urn", 5)?;
        let t = &urn.tracks;
        replace_for_release(
            &mut db,
            urn.release,
            &[vec![t[0], t[1], t[2]], vec![t[3], t[4]]],
        )??;

        crate::metadata::cascade_remove_entities(&mut db, &[t[1]])?;

        assert_eq!(chain_count(&db)?, 1);
        assert_eq!(
            members(&get_for_release(&db, urn.release)?),
            vec![vec![t[3], t[4]]]
        );
        assert!(links_for_tracks(&db, &[t[0], t[2]])?.is_empty());
        Ok(())
    }

    #[test]
    fn a_member_leaving_the_release_removes_its_whole_chain() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let urn = release_with_tracks(&mut db, "Urn", 4)?;
        let other = release_with_tracks(&mut db, "Other", 0)?;
        let t = &urn.tracks;
        replace_for_release(&mut db, urn.release, &[vec![t[0], t[1]], vec![t[2], t[3]]])??;

        let _ = crate::releases::replace_track_release(&mut db, other.release, t[1])?;
        assert_eq!(
            members(&get_for_release(&db, urn.release)?),
            vec![vec![t[2], t[3]]]
        );

        let _ = crate::releases::unlink_track(&mut db, urn.release, t[3])?;
        assert!(get_for_release(&db, urn.release)?.is_empty());
        assert!(links_for_tracks(&db, t)?.is_empty());
        Ok(())
    }

    #[test]
    fn keeping_a_member_on_its_release_keeps_the_chain() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let urn = release_with_tracks(&mut db, "Urn", 2)?;
        let t = &urn.tracks;
        let stored = replace_for_release(&mut db, urn.release, std::slice::from_ref(t))??;

        let _ = crate::releases::replace_track_release(&mut db, urn.release, t[0])?;
        crate::releases::link_track(&mut db, urn.release, t[1])?;

        assert_eq!(get_for_release(&db, urn.release)?, stored);
        Ok(())
    }

    #[test]
    fn moved_chains_keep_their_members_and_places_on_the_new_release() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let disc_two = release_with_tracks(&mut db, "Urn (Disc 2)", 3)?;
        let urn = release_with_tracks(&mut db, "Urn", 2)?;
        let t = &disc_two.tracks;
        let own = replace_for_release(&mut db, urn.release, std::slice::from_ref(&urn.tracks))??;
        let moving = replace_for_release(&mut db, disc_two.release, &[vec![t[2], t[0]]])??;

        for &track in t {
            crate::releases::link_track(&mut db, urn.release, track)?;
        }
        move_release_chains(&mut db, disc_two.release, urn.release)?;
        for &track in t {
            let _ = crate::releases::unlink_track(&mut db, disc_two.release, track)?;
        }

        assert!(get_for_release(&db, disc_two.release)?.is_empty());
        let stored = get_for_release(&db, urn.release)?;
        assert_eq!(stored.len(), 2);
        assert!(stored.contains(&own[0]));
        assert!(stored.contains(&moving[0]));
        assert_eq!(chain_count(&db)?, 2);
        let links = links_for_tracks(&db, t)?;
        assert_eq!(links[&t[2]].position, 0);
        assert_eq!(links[&t[0]].position, 1);
        Ok(())
    }

    #[test]
    fn deleting_a_release_removes_its_chains() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let urn = release_with_tracks(&mut db, "Urn", 2)?;
        replace_for_release(&mut db, urn.release, std::slice::from_ref(&urn.tracks))??;

        crate::metadata::cascade_remove_entities(&mut db, &[urn.release])?;

        assert_eq!(chain_count(&db)?, 0);
        assert!(links_for_tracks(&db, &urn.tracks)?.is_empty());
        Ok(())
    }
}
