// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    cmp::Ordering,
    collections::{
        HashMap,
        HashSet,
    },
};

use agdb::{
    DbAny,
    DbId,
};

use crate::db;

pub(super) fn select_release_merge_winner(
    db: &DbAny,
    release_ids: &[DbId],
) -> anyhow::Result<DbId> {
    let mut winner = release_ids
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("select_release_merge_winner called with empty list"))?;

    let mut winner_release = db::releases::get_by_id(db, winner)?
        .ok_or_else(|| anyhow::anyhow!("winner release {} missing", winner.0))?;
    let mut winner_tracks = db::tracks::get(db, winner)?;
    let mut winner_min_disc = winner_tracks.iter().filter_map(|track| track.disc).min();
    let mut winner_track_count = winner_tracks.len();
    let mut winner_created_at = winner_release.created_at.unwrap_or(u64::MAX);

    for candidate in release_ids.iter().copied().skip(1) {
        let Some(candidate_release) = db::releases::get_by_id(db, candidate)? else {
            continue;
        };
        let candidate_tracks = db::tracks::get(db, candidate)?;
        let candidate_min_disc = candidate_tracks.iter().filter_map(|track| track.disc).min();
        let candidate_track_count = candidate_tracks.len();
        let candidate_created_at = candidate_release.created_at.unwrap_or(u64::MAX);

        let ordering = db::compare_option(&candidate_min_disc, &winner_min_disc)
            .then_with(|| winner_track_count.cmp(&candidate_track_count))
            .then_with(|| winner_created_at.cmp(&candidate_created_at))
            .then_with(|| winner.0.cmp(&candidate.0));

        if ordering == Ordering::Less {
            winner = candidate;
            winner_release = candidate_release;
            winner_tracks = candidate_tracks;
            winner_min_disc = candidate_min_disc;
            winner_track_count = winner_tracks.len();
            winner_created_at = winner_release.created_at.unwrap_or(u64::MAX);
        }
    }

    Ok(winner)
}

fn merge_release_into(db: &mut DbAny, winner: DbId, loser: DbId) -> anyhow::Result<()> {
    if winner == loser {
        return Ok(());
    }

    for library in db::libraries::get_by_release(db, loser)? {
        let Some(library_db_id) = library.db_id else {
            continue;
        };
        db::graph::ensure_owned_edge(db, library_db_id, winner)?;
    }

    for track in db::tracks::get(db, loser)? {
        let Some(track_db_id) = track.db_id.map(Into::into) else {
            continue;
        };
        db::graph::ensure_owned_edge(db, winner, track_db_id)?;
        db::graph::remove_edges_between(db, loser, track_db_id)?;
    }

    // Migrate Credit nodes from loser to winner.
    // First, collect artist IDs already credited on the winner to avoid duplicates.
    let winner_artists: std::collections::HashSet<agdb::DbId> = db::artists::get(db, winner)?
        .into_iter()
        .filter_map(|p| p.db_id.map(Into::into))
        .collect();

    let loser_credits: Vec<db::Credit> = db
        .exec(
            agdb::QueryBuilder::select()
                .elements::<db::Credit>()
                .search()
                .from(loser)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    // Read loser→credit edge order values before removing edges.
    let loser_edge_orders = db::artists::artist_edge_orders_raw(db, loser)?;

    for credit in &loser_credits {
        let Some(credit_db_id) = credit.db_id.clone().map(agdb::DbId::from) else {
            continue;
        };
        let credit_targets = db::graph::direct_edges_from(db, credit_db_id)?;
        let artist_db_id = credit_targets
            .iter()
            .find_map(|e| e.to.filter(|id| id.0 > 0));

        let should_migrate = match artist_db_id {
            Some(pid) => !winner_artists.contains(&pid),
            None => false,
        };

        if should_migrate {
            let order = loser_edge_orders.get(&credit_db_id).copied().unwrap_or(0);
            db.exec_mut(
                agdb::QueryBuilder::insert()
                    .edges()
                    .from(winner)
                    .to(credit_db_id)
                    .values_uniform([
                        ("owned", 1).into(),
                        (db::credits::EDGE_ORDER_KEY, order).into(),
                    ])
                    .query(),
            )?;
        }
        // Remove loser→credit edge (credit itself is deleted if not migrated).
        db::graph::remove_edges_between(db, loser, credit_db_id)?;
        if !should_migrate {
            db.exec_mut(agdb::QueryBuilder::remove().ids(credit_db_id).query())?;
        }
    }

    for external_id in db::external_ids::get_for_entity(db, loser)? {
        let id_value = external_id.id_value.trim();
        if id_value.is_empty() {
            continue;
        }

        db::external_ids::upsert(
            db,
            winner,
            &external_id.provider_id,
            &external_id.id_type,
            id_value,
            external_id.source,
        )?;
    }

    let mut winner_layers_by_provider = HashMap::new();
    for layer in db::metadata::layers::get_for_entity(db, winner)? {
        winner_layers_by_provider.insert(layer.provider_id.clone(), layer);
    }
    let mut wrote_layer = false;
    for layer in db::metadata::layers::get_for_entity(db, loser)? {
        let should_upsert = winner_layers_by_provider
            .get(&layer.provider_id)
            .is_none_or(|existing| layer.updated_at > existing.updated_at);
        if !should_upsert {
            continue;
        }

        let mut layer_to_upsert = layer.clone();
        layer_to_upsert.db_id = None;
        db::metadata::layers::upsert(db, winner, &layer_to_upsert)?;
        winner_layers_by_provider.insert(layer.provider_id.clone(), layer);
        wrote_layer = true;
    }
    if wrote_layer {
        crate::services::metadata::merging::apply_merged_metadata_to_entity(db, winner)?;
    }

    if db::covers::get(db, winner)?.is_none()
        && let Some(cover) = db::covers::get(db, loser)?
        && let Some(cover_id) = cover.db_id
    {
        db::graph::ensure_owned_edge(db, winner, cover_id)?;
    }

    // Migrate label pairings onto the winner before cascade. Cascade calls
    // `cascade_remove_release_labels_for_owner` on the loser, which would GC
    // orphaned Labels; doing the winner-side upsert first keeps each Label
    // referenced by at least one RL through the deletion.
    db::labels::migrate_release_labels(db, winner, loser)?;

    db::metadata::cascade_remove_entities(db, &[loser])?;

    Ok(())
}

fn collect_track_unique_ids(
    db: &DbAny,
    release_db_id: DbId,
    unique_track_id_pairs: &HashSet<(String, String)>,
) -> anyhow::Result<HashSet<String>> {
    let mut ids = HashSet::new();
    if unique_track_id_pairs.is_empty() {
        return Ok(ids);
    }
    let ext_ids = db::external_ids::get_for_album_tracks(db, release_db_id)?;
    for ext_id in ext_ids {
        if !unique_track_id_pairs.contains(&(ext_id.provider_id.clone(), ext_id.id_type.clone())) {
            continue;
        }
        let val = ext_id.id_value.trim();
        if val.is_empty() {
            continue;
        }
        ids.insert(format!("{}:{}:{}", ext_id.provider_id, ext_id.id_type, val));
    }
    Ok(ids)
}

pub(super) fn deduplicate_releases_by_external_id(
    db: &mut DbAny,
    library_db_id: DbId,
    unique_release_id_pairs: &HashSet<(String, String)>,
    unique_track_id_pairs: &HashSet<(String, String)>,
    provider_scope: Option<&HashSet<String>>,
) -> anyhow::Result<u32> {
    if unique_release_id_pairs.is_empty() {
        return Ok(0);
    }

    let releases = db::releases::get(db, library_db_id)?;
    let mut groups: HashMap<String, Vec<DbId>> = HashMap::new();
    for release in releases {
        let Some(release_db_id) = release.db_id.map(Into::into) else {
            continue;
        };

        for external_id in db::external_ids::get_for_entity(db, release_db_id)? {
            if let Some(scope) = provider_scope
                && !scope.contains(&external_id.provider_id)
            {
                continue;
            }

            if !unique_release_id_pairs
                .contains(&(external_id.provider_id.clone(), external_id.id_type.clone()))
            {
                continue;
            }

            let id_value = external_id.id_value.trim();
            if id_value.is_empty() {
                continue;
            }

            let key = format!(
                "{}:{}:{}",
                external_id.provider_id, external_id.id_type, id_value
            );
            groups.entry(key).or_default().push(release_db_id);
        }
    }

    let mut releases_in_merge_groups = HashSet::new();
    for (_, release_ids) in &groups {
        if release_ids.len() > 1 {
            for release_db_id in release_ids {
                releases_in_merge_groups.insert(*release_db_id);
            }
        }
    }

    let mut track_ids_cache: HashMap<DbId, HashSet<String>> = HashMap::new();
    if !unique_track_id_pairs.is_empty() {
        for &release_db_id in &releases_in_merge_groups {
            let ids = collect_track_unique_ids(db, release_db_id, unique_track_id_pairs)?;
            track_ids_cache.insert(release_db_id, ids);
        }
    }

    let mut live_releases: HashSet<DbId> = releases_in_merge_groups.clone();
    let mut merged_count = 0;
    for (key, release_ids) in groups {
        let mut unique_release_ids = Vec::new();
        let mut seen = HashSet::new();
        for release_db_id in release_ids {
            if seen.insert(release_db_id) && live_releases.contains(&release_db_id) {
                unique_release_ids.push(release_db_id);
            }
        }
        if unique_release_ids.len() <= 1 {
            continue;
        }

        let winner = select_release_merge_winner(db, &unique_release_ids)?;
        let winner_track_ids = track_ids_cache.get(&winner);
        for loser in unique_release_ids
            .into_iter()
            .filter(|release_db_id| *release_db_id != winner)
        {
            if !live_releases.contains(&loser) {
                continue;
            }

            if let Some(w_ids) = winner_track_ids
                && !w_ids.is_empty()
                && let Some(l_ids) = track_ids_cache.get(&loser)
                && !l_ids.is_empty()
                && !w_ids.is_disjoint(l_ids)
            {
                tracing::debug!(
                    library_db_id = library_db_id.0,
                    winner = winner.0,
                    loser = loser.0,
                    unique_id = %key,
                    "skipping release merge due to overlapping track unique ids"
                );
                continue;
            }

            tracing::info!(
                library_db_id = library_db_id.0,
                winner = winner.0,
                loser = loser.0,
                unique_id = %key,
                "merging releases by provider unique id"
            );
            merge_release_into(db, winner, loser)?;
            live_releases.remove(&loser);
            merged_count += 1;
        }
    }

    Ok(merged_count)
}
