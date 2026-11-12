// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Collecting what a write leaves unreferenced.
//!
//! A track with no entry to play, a release with no track, a track source no track plays, a cue
//! track no source plays and a cue sheet with no cue track go in the write that leaves them so:
//! that write names them as [`Candidates`] and [`collect`] removes the ones nothing holds.
//!
//! Artists wait. Several writes build one up before anything references it, and a refresh drops
//! and re-adds them across writes, so a write that may leave an artist unreferenced only marks it
//! a member of [`Root::ArtistCandidates`], and [`collect_artist_candidates`] alone removes artists.

use std::collections::HashSet;

use agdb::{
    DbAny,
    DbId,
    QueryBuilder,
};

use super::{
    DbAccess,
    dedup_positive_ids,
    graph::{
        self,
        inbound_neighbor_ids,
        neighbor_ids,
    },
    metadata::{
        self,
        layers,
    },
    roots::{
        self,
        Root,
    },
};

/// What a write may have left unreferenced, of every kind collected in that write. Artists are
/// not among them; theirs is [`Root::ArtistCandidates`].
#[derive(Debug, Default)]
pub struct Candidates {
    pub tracks: Vec<DbId>,
    pub releases: Vec<DbId>,
    pub track_sources: Vec<DbId>,
    pub cue_tracks: Vec<DbId>,
    pub cue_sheets: Vec<DbId>,
}

impl Candidates {
    pub fn extend(&mut self, other: Self) {
        self.tracks.extend(other.tracks);
        self.releases.extend(other.releases);
        self.track_sources.extend(other.track_sources);
        self.cue_tracks.extend(other.cue_tracks);
        self.cue_sheets.extend(other.cue_sheets);
    }

    /// What removing the tracks among `ids` may leave: their releases and their sources.
    pub(crate) fn around(db: &impl DbAccess, ids: &[DbId]) -> anyhow::Result<Self> {
        let mut candidates = Self::default();
        for id in graph::existing_ids(db, &dedup_positive_ids(ids), "Track")? {
            candidates
                .releases
                .extend(inbound_neighbor_ids(db, id, "Release")?);
            candidates
                .track_sources
                .extend(neighbor_ids(db, id, "TrackSource")?);
        }
        Ok(candidates)
    }
}

/// Whether any path from the track reaches a file entry.
fn has_entry(db: &impl DbAccess, track_id: DbId) -> anyhow::Result<bool> {
    Ok(db
        .exec(
            QueryBuilder::search()
                .from(track_id)
                .limit(1)
                .where_()
                .key("db_element_id")
                .value("Entry")
                .and()
                .beyond()
                .where_()
                .not()
                .key("db_element_id")
                .value("Entry")
                .end_where()
                .query(),
        )?
        .result
        > 0)
}

fn has_track(db: &impl DbAccess, release_id: DbId) -> anyhow::Result<bool> {
    Ok(db
        .exec(
            QueryBuilder::search()
                .from(release_id)
                .limit(1)
                .where_()
                .key("db_element_id")
                .value("Track")
                .query(),
        )?
        .result
        > 0)
}

/// The `kind` nodes among `ids` that `held` does not hold.
fn unheld(
    db: &impl DbAccess,
    ids: &[DbId],
    kind: &str,
    held: impl Fn(DbId) -> anyhow::Result<bool>,
) -> anyhow::Result<Vec<DbId>> {
    let mut unheld = Vec::new();
    // A freed id may name another element by now, so only its kind makes it a candidate still.
    for id in graph::existing_ids(db, &dedup_positive_ids(ids), kind)? {
        if !held(id)? {
            unheld.push(id);
        }
    }
    Ok(unheld)
}

fn remove_elements(db: &mut impl DbAccess, ids: &[DbId]) -> anyhow::Result<()> {
    if !ids.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(ids).query())?;
    }
    Ok(())
}

/// Removes the candidates nothing holds, and what removing them leaves in turn: an emptied
/// release after its last track, a cue sheet after its last cue track. The artists they
/// referenced become candidates of their own, and the files of the covers no remaining owner
/// shares are listed for discarding. Runs inside the caller's transaction.
pub fn collect(db: &mut impl DbAccess, candidates: Candidates) -> anyhow::Result<()> {
    let Candidates {
        tracks,
        mut releases,
        mut track_sources,
        mut cue_tracks,
        mut cue_sheets,
    } = candidates;

    let tracks = unheld(db, &tracks, "Track", |track| has_entry(db, track))?;
    let left = Candidates::around(db, &tracks)?;
    releases.extend(left.releases);
    track_sources.extend(left.track_sources);
    metadata::remove_entities(db, &tracks)?;

    let releases = unheld(db, &releases, "Release", |release| has_track(db, release))?;
    metadata::remove_entities(db, &releases)?;

    let track_sources = unheld(db, &track_sources, "TrackSource", |source| {
        Ok(!inbound_neighbor_ids(db, source, "Track")?.is_empty())
    })?;
    for &source in &track_sources {
        cue_tracks.extend(neighbor_ids(db, source, "CueTrack")?);
    }
    remove_elements(db, &track_sources)?;

    let cue_tracks = unheld(db, &cue_tracks, "CueTrack", |cue_track| {
        Ok(!inbound_neighbor_ids(db, cue_track, "TrackSource")?.is_empty())
    })?;
    for &cue_track in &cue_tracks {
        cue_sheets.extend(inbound_neighbor_ids(db, cue_track, "CueSheet")?);
    }
    remove_elements(db, &cue_tracks)?;

    let cue_sheets = unheld(db, &cue_sheets, "CueSheet", |sheet| {
        Ok(!neighbor_ids(db, sheet, "CueTrack")?.is_empty())
    })?;
    remove_elements(db, &cue_sheets)
}

/// Marks the artists among `ids` as possibly unreferenced, for the next
/// [`collect_artist_candidates`] to check.
pub(crate) fn mark_artists(db: &mut impl DbAccess, ids: &[DbId]) -> anyhow::Result<()> {
    let artists = graph::existing_ids(db, &dedup_positive_ids(ids), "Artist")?;
    roots::link_once(db, Root::ArtistCandidates, &artists)
}

pub fn is_artist_candidate(db: &impl DbAccess, artist_id: DbId) -> anyhow::Result<bool> {
    roots::is_member(db, Root::ArtistCandidates, artist_id)
}

pub fn has_artist_candidates(db: &impl DbAccess) -> anyhow::Result<bool> {
    roots::has_members(db, Root::ArtistCandidates)
}

/// The artists nothing references, `artist_id` first and each after one it references, or `None`
/// when something references it. A release or track with an edge to an artist references it, as
/// does the owner of a layer crediting or relating it, and an artist with a relation to it; an
/// artist counts only while something references it in turn, so artists referencing each other
/// alone go together. `referenced` holds the artists already found referenced, and gains the ones
/// this walk finds so.
fn unreferenced_with(
    db: &impl DbAccess,
    artist_id: DbId,
    referenced: &mut HashSet<DbId>,
) -> anyhow::Result<Option<Vec<DbId>>> {
    // Each artist walked, with the place of the one it references that led the walk to it.
    let mut walked: Vec<(DbId, Option<usize>)> = vec![(artist_id, None)];
    let mut seen = HashSet::from([artist_id]);
    let mut next = 0;
    while let Some(&(artist, _)) = walked.get(next) {
        let mut is_referenced = referenced.contains(&artist)
            || db
                .exec(
                    QueryBuilder::search()
                        .to(artist)
                        .limit(1)
                        .where_()
                        .distance(roots::MEMBERS)
                        .and()
                        .where_()
                        .key("db_element_id")
                        .value("Release")
                        .or()
                        .key("db_element_id")
                        .value("Track")
                        .end_where()
                        .query(),
                )?
                .result
                > 0;
        let mut referencing = Vec::new();
        if !is_referenced {
            for layer_id in layers::edges::layers_referencing_artist(db, artist)? {
                let Some(owner) = layers::owner_id(db, layer_id)? else {
                    continue;
                };
                if !graph::is_kind(db, owner, "Artist")? {
                    is_referenced = true;
                    break;
                }
                referencing.push(owner);
            }
        }
        if is_referenced {
            // Only the artists between this one and the first are known to be referenced; the
            // others walked reference them, which says nothing of what references those.
            let mut link = Some(next);
            while let Some(index) = link {
                let (artist, led_by) = walked[index];
                referenced.insert(artist);
                link = led_by;
            }
            return Ok(None);
        }
        referencing.extend(inbound_neighbor_ids(db, artist, "Artist")?);
        for other in referencing {
            if seen.insert(other) {
                walked.push((other, Some(next)));
            }
        }
        next += 1;
    }
    Ok(Some(walked.into_iter().map(|(artist, _)| artist).collect()))
}

/// What one pass over the artist candidates did.
#[derive(Debug, Default)]
pub struct ArtistCollection {
    pub removed: usize,
    /// Whether candidates are left for another pass.
    pub remaining: bool,
}

/// Checks up to `limit` artist candidates, newest first, in one transaction: a referenced one
/// stops being a candidate, and an unreferenced one goes with every artist that only it and its
/// like reference. The artists those referenced become candidates for a later pass.
///
/// `limit` also bounds the artists one pass removes. An unreferenced candidate with more artists
/// behind it than that loses the furthest of them and stays a candidate for the rest, which the
/// next pass still reaches through it. Reading is not bounded: each candidate's walk covers the
/// artists referencing it until it finds one something else references.
pub fn collect_artist_candidates(db: &mut DbAny, limit: usize) -> anyhow::Result<ArtistCollection> {
    db.transaction_mut(|t| {
        let mut settled = Vec::new();
        let mut unreferenced = Vec::new();
        let mut removing = HashSet::new();
        let mut referenced = HashSet::new();
        for (edge, candidate) in roots::newest_member_edges(t, Root::ArtistCandidates, limit)? {
            if removing.contains(&candidate) {
                continue;
            }
            if unreferenced.len() >= limit {
                break;
            }
            if !graph::is_kind(t, candidate, "Artist")? {
                settled.push(edge);
                continue;
            }
            let Some(artists) = unreferenced_with(t, candidate, &mut referenced)? else {
                settled.push(edge);
                continue;
            };
            let artists: Vec<DbId> = artists
                .into_iter()
                .filter(|artist| !removing.contains(artist))
                .collect();
            let room = limit - unreferenced.len();
            // Past the room left, the candidate stays with the artists nearest it, each still
            // referencing one that stays.
            let kept = artists.len().saturating_sub(room);
            for &artist in &artists[kept..] {
                removing.insert(artist);
                unreferenced.push(artist);
            }
        }
        remove_elements(t, &settled)?;
        metadata::remove_entities(t, &unreferenced)?;
        Ok(ArtistCollection {
            removed: unreferenced.len(),
            remaining: has_artist_candidates(t)?,
        })
    })
}

#[cfg(any(test, feature = "testing"))]
pub mod testing;

#[cfg(test)]
mod tests;
