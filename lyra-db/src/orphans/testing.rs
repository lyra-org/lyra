// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The orphan rules as one read of the whole catalog, for tests to hold the collection to: a
//! write that forgets to collect what it left, or to mark an artist it left, fails the test that
//! asserts one of these after it. Nothing checks them outside the tests that call them.

use std::collections::HashSet;

use agdb::{
    DbAny,
    DbId,
    QueryBuilder,
};

use crate::{
    Artist,
    graph::{
        inbound_neighbor_ids,
        neighbor_ids,
    },
    metadata::layers,
    roots::{
        self,
        Root,
    },
};

/// The artists any path of outgoing edges reaches from a release or track.
fn artists_reached_from_catalog(db: &DbAny) -> anyhow::Result<HashSet<DbId>> {
    let mut reached = HashSet::new();
    for root in [Root::Releases, Root::Tracks] {
        reached.extend(
            db.exec(
                QueryBuilder::search()
                    .from(root.alias())
                    .where_()
                    .key("db_element_id")
                    .value("Artist")
                    .query(),
            )?
            .ids(),
        );
    }
    Ok(reached)
}

/// The artists among `unreached` that stay all the same: a layer of an entity that stays
/// references them, or a walk from an artist that stays reaches them.
fn artists_kept_by_layers(db: &DbAny, unreached: &HashSet<DbId>) -> anyhow::Result<HashSet<DbId>> {
    let mut kept = HashSet::new();
    loop {
        let mut newly_kept = Vec::new();
        for &artist_id in unreached {
            if kept.contains(&artist_id) {
                continue;
            }
            for layer_id in layers::edges::layers_referencing_artist(db, artist_id)? {
                if layers::owner_id(db, layer_id)?
                    .is_some_and(|owner| !unreached.contains(&owner) || kept.contains(&owner))
                {
                    newly_kept.push(artist_id);
                    break;
                }
            }
        }
        if newly_kept.is_empty() {
            return Ok(kept);
        }
        for artist_id in newly_kept {
            if !kept.insert(artist_id) {
                continue;
            }
            let reachable: Vec<Artist> = db
                .exec(
                    QueryBuilder::select()
                        .elements::<Artist>()
                        .search()
                        .from(artist_id)
                        .query(),
                )?
                .try_into()?;
            kept.extend(
                reachable
                    .into_iter()
                    .filter_map(|artist| artist.db_id.map(DbId::from))
                    .filter(|id| unreached.contains(id)),
            );
        }
    }
}

/// The artists nothing references.
pub fn unreferenced_artists(db: &DbAny) -> anyhow::Result<HashSet<DbId>> {
    let reached = artists_reached_from_catalog(db)?;
    let unreached: HashSet<DbId> = roots::member_ids(db, Root::Artists)?
        .into_iter()
        .filter(|artist_id| !reached.contains(artist_id))
        .collect();
    let kept = artists_kept_by_layers(db, &unreached)?;
    Ok(unreached
        .into_iter()
        .filter(|artist_id| !kept.contains(artist_id))
        .collect())
}

fn sorted(ids: impl IntoIterator<Item = DbId>) -> Vec<i64> {
    let mut ids: Vec<i64> = ids.into_iter().map(|id| id.0).collect();
    ids.sort_unstable();
    ids
}

/// The artists among `unreferenced` that collecting the candidates removes: the candidates,
/// the artists referencing them, and the artists those reference in turn.
fn artists_a_collection_reaches(
    db: &DbAny,
    unreferenced: &HashSet<DbId>,
) -> anyhow::Result<HashSet<DbId>> {
    let mut reached = HashSet::new();
    let mut pending: Vec<DbId> = roots::member_ids(db, Root::ArtistCandidates)?;
    while let Some(artist_id) = pending.pop() {
        if !unreferenced.contains(&artist_id) || !reached.insert(artist_id) {
            continue;
        }
        pending.extend(inbound_neighbor_ids(db, artist_id, "Artist")?);
        pending.extend(neighbor_ids(db, artist_id, "Artist")?);
        for layer_id in layers::edges::layers_referencing_artist(db, artist_id)? {
            pending.extend(layers::owner_id(db, layer_id)?);
        }
        pending.extend(layers::edges::artists_referenced_by(
            db,
            &layers::ids_for_entity(db, artist_id)?,
        )?);
    }
    Ok(reached)
}

/// Fails unless collecting the candidates would remove every unreferenced artist: each is a
/// candidate, or one of the artists a candidate goes with or leaves behind. Every write is meant
/// to leave this so.
pub fn assert_artists_tracked(db: &DbAny) -> anyhow::Result<()> {
    let unreferenced = unreferenced_artists(db)?;
    let reached = artists_a_collection_reaches(db, &unreferenced)?;
    let untracked = sorted(
        unreferenced
            .into_iter()
            .filter(|artist_id| !reached.contains(artist_id)),
    );
    anyhow::ensure!(
        untracked.is_empty(),
        "no candidate leads to the unreferenced artists {untracked:?}"
    );
    Ok(())
}

/// Fails unless nothing but artists is unreferenced and the candidates lead to every
/// unreferenced artist, as every write is meant to leave it.
pub fn assert_collected(db: &DbAny) -> anyhow::Result<()> {
    let mut tracks = Vec::new();
    for track in roots::member_ids(db, Root::Tracks)? {
        if crate::entries::get_by_track(db, track)?.is_empty() {
            tracks.push(track);
        }
    }
    let mut releases = Vec::new();
    for release in roots::member_ids(db, Root::Releases)? {
        let tracks: Vec<crate::Track> = db
            .exec(
                QueryBuilder::select()
                    .elements::<crate::Track>()
                    .search()
                    .from(release)
                    .query(),
            )?
            .try_into()?;
        if tracks.is_empty() {
            releases.push(release);
        }
    }
    let mut track_sources = Vec::new();
    for source in roots::member_ids(db, Root::TrackSources)? {
        if inbound_neighbor_ids(db, source, "Track")?.is_empty() {
            track_sources.push(source);
        }
    }
    let mut cue_tracks = Vec::new();
    for cue_track in roots::member_ids(db, Root::CueTracks)? {
        if inbound_neighbor_ids(db, cue_track, "TrackSource")?.is_empty() {
            cue_tracks.push(cue_track);
        }
    }
    let mut cue_sheets = Vec::new();
    for sheet in roots::member_ids(db, Root::CueSheets)? {
        if neighbor_ids(db, sheet, "CueTrack")?.is_empty() {
            cue_sheets.push(sheet);
        }
    }
    for (kind, orphans) in [
        ("tracks without an entry", tracks),
        ("releases without a track", releases),
        ("track sources without a track", track_sources),
        ("cue tracks without a source", cue_tracks),
        ("cue sheets without a cue track", cue_sheets),
    ] {
        anyhow::ensure!(orphans.is_empty(), "{kind}: {:?}", sorted(orphans));
    }
    assert_artists_tracked(db)
}

/// Fails unless nothing at all is unreferenced, as a collection of the artist candidates that
/// nothing held back leaves it.
pub fn assert_drained(db: &DbAny) -> anyhow::Result<()> {
    assert_collected(db)?;
    let artists = sorted(unreferenced_artists(db)?);
    anyhow::ensure!(artists.is_empty(), "unreferenced artists: {artists:?}");
    anyhow::ensure!(
        !super::has_artist_candidates(db)?,
        "artist candidates are left"
    );
    Ok(())
}

/// Collects the artist candidates until none is left, as a scan's end does.
pub fn drain_artists(db: &mut DbAny) -> anyhow::Result<usize> {
    let mut removed = 0;
    loop {
        let pass = super::collect_artist_candidates(db, 64)?;
        removed += pass.removed;
        if !pass.remaining {
            return Ok(removed);
        }
    }
}
