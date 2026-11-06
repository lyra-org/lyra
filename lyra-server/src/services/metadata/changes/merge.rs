// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Merging duplicate artists, releases, genres and labels by hand.

use std::collections::{
    BTreeMap,
    BTreeSet,
    HashMap,
    HashSet,
};

use agdb::{
    DbAnyTransactionMut,
    DbId,
};
use anyhow::Context;
use lyra_db::{
    DbAccess,
    metadata::layers::edges,
};
use serde_json::{
    Value,
    json,
};

use super::{
    ChangeContext,
    ChangeError,
    model::{
        KeptExternalId,
        ManualFill,
        MergeCounts,
        MergeKind,
        MergeOutcome,
        MergePreview,
        TrackCollision,
    },
};
use crate::services::{
    metadata::{
        cleanup::{
            merge_artist_into,
            migrate_metadata,
        },
        layers::{
            MANUAL_SOURCE_ID,
            json::value_json,
        },
        merging::{
            LayerKeys,
            apply_merged_metadata_to_entity_inside_tx,
        },
    },
    providers::{
        collect_track_unique_ids,
        merge_release_into_inside_tx,
        release_library_ids,
    },
};

impl MergeKind {
    fn of(db: &impl DbAccess, entity: DbId) -> anyhow::Result<Option<Self>> {
        Ok(match lyra_db::graph::element_kind(db, entity)?.as_deref() {
            Some("Artist") => Some(Self::Artist),
            Some("Release") => Some(Self::Release),
            Some("Genre") => Some(Self::Genre),
            Some("Label") => Some(Self::Label),
            _ => None,
        })
    }
}

/// The entities of a merge.
struct Targets {
    kind: MergeKind,
    winner: DbId,
    losers: Vec<DbId>,
}

/// The public IDs of the request's losers without repeats, once the request itself makes sense.
fn checked_loser_ids(winner_id: &str, loser_ids: &[String]) -> Result<Vec<String>, ChangeError> {
    if loser_ids.is_empty() {
        return Err(ChangeError::Invalid(
            "loser_ids must name at least one entity".to_string(),
        ));
    }
    if loser_ids.iter().any(|id| id == winner_id) {
        return Err(ChangeError::Invalid(
            "an entity can't be merged into itself".to_string(),
        ));
    }
    let mut seen = HashSet::new();
    Ok(loser_ids
        .iter()
        .filter(|id| seen.insert(id.as_str()))
        .cloned()
        .collect())
}

/// The entities of a merge, which the caller may merge, on libraries free of runs.
fn targets(
    db: &impl DbAccess,
    context: &ChangeContext,
    winner_id: &str,
    loser_ids: &[String],
) -> Result<Targets, ChangeError> {
    let loser_ids = checked_loser_ids(winner_id, loser_ids)?;
    let (winner, mut libraries) = context.reached(db, winner_id)?;
    let kind = MergeKind::of(db, winner)?.ok_or_else(|| {
        ChangeError::Invalid("only artists, releases, genres and labels merge".to_string())
    })?;
    let mut losers = Vec::with_capacity(loser_ids.len());
    for loser_id in &loser_ids {
        let (loser, loser_libraries) = context.reached(db, loser_id)?;
        if MergeKind::of(db, loser)? != Some(kind) {
            return Err(ChangeError::Invalid(format!(
                "{loser_id} is not a {}",
                kind.as_str()
            )));
        }
        libraries.extend(loser_libraries);
        losers.push(loser);
    }
    context.require_every_library(&libraries)?;
    context.check_runs(&libraries)?;
    Ok(Targets {
        kind,
        winner,
        losers,
    })
}

/// What the data itself allows of the merge.
fn check_merge(
    db: &impl DbAccess,
    context: &ChangeContext,
    targets: &Targets,
) -> Result<(), ChangeError> {
    match targets.kind {
        MergeKind::Release => check_releases(
            db,
            targets.winner,
            &targets.losers,
            &context.unique_track_ids,
        ),
        MergeKind::Genre => check_genre_name(db, targets.winner),
        MergeKind::Artist | MergeKind::Label => Ok(()),
    }
}

fn check_releases(
    db: &impl DbAccess,
    winner: DbId,
    losers: &[DbId],
    unique_track_ids: &HashSet<(String, String)>,
) -> Result<(), ChangeError> {
    let winner_libraries = release_library_ids(db, winner)?;
    let mut track_ids = collect_track_unique_ids(db, winner, unique_track_ids)?;
    for &loser in losers {
        if release_library_ids(db, loser)? != winner_libraries {
            return Err(ChangeError::Conflict(
                "releases merge only within the same libraries".to_string(),
            ));
        }
        let loser_track_ids = collect_track_unique_ids(db, loser, unique_track_ids)?;
        if !track_ids.is_disjoint(&loser_track_ids) {
            return Err(ChangeError::Conflict(
                "these releases hold the same track".to_string(),
            ));
        }
        track_ids.extend(loser_track_ids);
    }
    Ok(())
}

fn check_genre_name(db: &impl DbAccess, winner: DbId) -> Result<(), ChangeError> {
    let genre = lyra_db::genres::get_by_id(db, winner)?
        .ok_or_else(|| anyhow::anyhow!("genre {} missing", winner.0))?;
    if genre.name.trim().is_empty() {
        return Err(ChangeError::Invalid(
            "a genre with a blank name can't win a merge".to_string(),
        ));
    }
    Ok(())
}

fn counts(db: &impl DbAccess, targets: &Targets) -> anyhow::Result<MergeCounts> {
    let mut counts = MergeCounts {
        losers: targets.losers.len(),
        ..MergeCounts::default()
    };
    for &loser in &targets.losers {
        match targets.kind {
            MergeKind::Artist => {
                counts.releases += lyra_db::releases::get_by_artist(db, loser)?.len();
                counts.tracks += lyra_db::tracks::get_by_artist(db, loser)?.len();
            }
            MergeKind::Release => {
                counts.tracks += lyra_db::tracks::get_direct(db, loser)?.len();
            }
            MergeKind::Genre => {
                counts.releases += lyra_db::genres::get_releases(db, loser)?.len();
                counts.relatives += lyra_db::genres::get_parents(db, loser)?.len()
                    + lyra_db::genres::get_children(db, loser)?.len();
            }
            MergeKind::Label => {
                counts.releases += lyra_db::labels::get_releases(db, loser)?.len();
            }
        }
    }
    Ok(counts)
}

/// What a merge of `loser_ids` into `winner_id` would do to the data as `db` holds it.
pub(super) fn preview(
    db: &impl DbAccess,
    context: &ChangeContext,
    winner_id: &str,
    loser_ids: &[String],
) -> Result<MergePreview, ChangeError> {
    let targets = targets(db, context, winner_id, loser_ids)?;
    check_merge(db, context, &targets)?;
    Ok(MergePreview {
        outcome: MergeOutcome {
            winner_id: winner_id.to_string(),
            kind: targets.kind,
            counts: counts(db, &targets)?,
        },
        kept_external_ids: lyra_db::external_ids::absorb_plan(db, targets.winner, &targets.losers)?
            .into_iter()
            .map(|alias| KeptExternalId {
                provider_id: alias.provider_id,
                id_type: alias.id_type,
                id_value: alias.id_value,
            })
            .collect(),
        track_collisions: track_collisions(db, &targets)?,
        locked_ids: locked_ids(db, &targets)?,
        manual_fills: manual_fills(db, &targets)?,
    })
}

/// Merges each of `loser_ids` into `winner_id`: what is attached to them moves to the winner,
/// their names and differing external IDs keep finding it, and they are gone.
pub(super) fn apply(
    t: &mut DbAnyTransactionMut<'_>,
    context: &ChangeContext,
    winner_id: &str,
    loser_ids: &[String],
    expected: &[ManualFill],
) -> Result<MergeOutcome, ChangeError> {
    let targets = targets(t, context, winner_id, loser_ids)?;
    let fills = manual_fills(t, &targets)?;
    if fills != expected {
        return Err(ChangeError::stale(
            "the manual values a loser would fill changed after preview",
            &fills,
        )?);
    }
    check_merge(t, context, &targets)?;
    let counts = counts(t, &targets)?;
    for &loser in &targets.losers {
        merge_into(t, targets.kind, targets.winner, loser)?;
    }
    Ok(MergeOutcome {
        winner_id: winner_id.to_string(),
        kind: targets.kind,
        counts,
    })
}

fn track_collisions(db: &impl DbAccess, targets: &Targets) -> anyhow::Result<Vec<TrackCollision>> {
    if targets.kind != MergeKind::Release {
        return Ok(Vec::new());
    }
    let mut seen: HashMap<(u32, u32), DbId> = HashMap::new();
    let mut collisions = BTreeSet::new();
    for &release in std::iter::once(&targets.winner).chain(&targets.losers) {
        for track in lyra_db::tracks::get_direct(db, release)? {
            let Some(number) = track.track else {
                continue;
            };
            let position = (track.disc.unwrap_or(1), number);
            if *seen.entry(position).or_insert(release) != release {
                collisions.insert(position);
            }
        }
    }
    Ok(collisions
        .into_iter()
        .map(|(disc, track)| TrackCollision { disc, track })
        .collect())
}

fn locked_ids(db: &impl DbAccess, targets: &Targets) -> anyhow::Result<Vec<String>> {
    let mut locked = Vec::new();
    for &entity in std::iter::once(&targets.winner).chain(&targets.losers) {
        let id = match targets.kind {
            MergeKind::Artist => lyra_db::artists::get_by_id(db, entity)?
                .filter(|artist| artist.locked.unwrap_or(false))
                .map(|artist| artist.id),
            MergeKind::Release => lyra_db::releases::get_by_id(db, entity)?
                .filter(|release| release.locked.unwrap_or(false))
                .map(|release| release.id),
            MergeKind::Genre | MergeKind::Label => None,
        };
        locked.extend(id);
    }
    Ok(locked)
}

fn artist_public_id(db: &impl DbAccess, artist: DbId) -> anyhow::Result<String> {
    lyra_db::lookup::find_id_by_db_id(db, artist)?
        .with_context(|| format!("artist {} is gone", artist.0))
}

/// The manual values the losers fill in for the keys the winner leaves out, ordered by key. The
/// first loser supplying a key fills it, as the merge goes through them in order.
fn manual_fills(db: &impl DbAccess, targets: &Targets) -> anyhow::Result<Vec<ManualFill>> {
    let winner = lyra_db::metadata::layers::find(db, targets.winner, MANUAL_SOURCE_ID)?
        .as_ref()
        .map(LayerKeys::supplied_by)
        .unwrap_or_default();
    let mut fills: BTreeMap<String, Value> = BTreeMap::new();
    for &loser in &targets.losers {
        let Some(layer) = lyra_db::metadata::layers::find(db, loser, MANUAL_SOURCE_ID)? else {
            continue;
        };
        for (field, value) in &layer.fields {
            if !winner.fields.contains(field) && !fills.contains_key(&field.to_string()) {
                fills.insert(field.to_string(), value_json(db, value.as_ref())?);
            }
        }
        for (credit_type, inputs) in &layer.credits {
            if !winner.credits.contains(credit_type)
                && !fills.contains_key(&credit_type.to_string())
            {
                let credits = inputs
                    .iter()
                    .map(|input| {
                        let artist_id = artist_public_id(db, input.artist_id)?;
                        Ok(json!({"artist_id": artist_id, "detail": input.detail}))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                fills.insert(credit_type.to_string(), Value::Array(credits));
            }
        }
        for (relation_type, inputs) in &layer.relations {
            if !winner.relations.contains(relation_type)
                && !fills.contains_key(&relation_type.to_string())
            {
                let relations = inputs
                    .iter()
                    .map(|input| {
                        let artist_id = artist_public_id(db, input.artist_id)?;
                        Ok(json!({"artist_id": artist_id, "attributes": input.attributes}))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                fills.insert(relation_type.to_string(), Value::Array(relations));
            }
        }
    }
    Ok(fills
        .into_iter()
        .map(|(key, value)| ManualFill { key, value })
        .collect())
}

fn merge_into(
    t: &mut DbAnyTransactionMut<'_>,
    kind: MergeKind,
    winner: DbId,
    loser: DbId,
) -> anyhow::Result<()> {
    match kind {
        MergeKind::Artist => {
            migrate_metadata(t, winner, loser)?;
            lyra_db::external_ids::absorb_inside_tx(t, winner, loser)?;
            merge_artist_into(t, winner, loser)
        }
        MergeKind::Release => merge_release_into_inside_tx(t, winner, loser),
        MergeKind::Genre => merge_genre_into(t, winner, loser),
        MergeKind::Label => merge_label_into(t, winner, loser),
    }
}

/// The entities whose stored values name `loser`, by a layer or as `linked`.
fn owners(t: &DbAnyTransactionMut<'_>, loser: DbId, linked: &[DbId]) -> anyhow::Result<Vec<DbId>> {
    let mut owners = edges::referencing_owners(t, loser)?;
    for &release in linked {
        if !owners.contains(&release) {
            owners.push(release);
        }
    }
    Ok(owners)
}

fn release_locked(t: &DbAnyTransactionMut<'_>, release: DbId) -> anyhow::Result<bool> {
    Ok(lyra_db::releases::get_by_id(t, release)?
        .is_some_and(|release| release.locked.unwrap_or(false)))
}

fn merge_genre_into(
    t: &mut DbAnyTransactionMut<'_>,
    winner: DbId,
    loser: DbId,
) -> anyhow::Result<()> {
    let linked = lyra_db::genres::get_releases(t, loser)?;
    let owners = owners(t, loser, &linked)?;
    edges::retarget_genre(t, winner, loser)?;
    // A locked release keeps what is stored through the apply below, so its link moves.
    for &release in &linked {
        if release_locked(t, release)? {
            lyra_db::genres::move_release_link(t, release, winner, loser)?;
        }
    }
    for owner in owners {
        apply_merged_metadata_to_entity_inside_tx(t, owner)?;
    }
    lyra_db::genres::absorb_relatives(t, winner, loser)?;
    lyra_db::external_ids::absorb_inside_tx(t, winner, loser)?;
    lyra_db::genres::absorb_aliases(t, winner, loser)?;
    lyra_db::genres::remove_inside_tx(t, loser)
}

fn merge_label_into(
    t: &mut DbAnyTransactionMut<'_>,
    winner: DbId,
    loser: DbId,
) -> anyhow::Result<()> {
    let linked = lyra_db::labels::get_releases(t, loser)?;
    let owners = owners(t, loser, &linked)?;
    edges::retarget_label(t, winner, loser)?;
    // The apply below can collect the loser with its last link, so what it holds moves first.
    lyra_db::external_ids::absorb_inside_tx(t, winner, loser)?;
    lyra_db::labels::absorb_aliases(t, winner, loser)?;
    for &release in &linked {
        if release_locked(t, release)? {
            lyra_db::labels::move_release_link(t, release, winner, loser)?;
        }
    }
    for owner in owners {
        apply_merged_metadata_to_entity_inside_tx(t, owner)?;
    }
    lyra_db::labels::collect_unreferenced(t, &[loser])?;
    anyhow::ensure!(
        lyra_db::labels::get_by_id(t, loser)?.is_none(),
        "merged label {} is still referenced",
        loser.0
    );
    Ok(())
}

#[cfg(test)]
mod tests;
