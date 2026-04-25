// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::cmp::Ordering;
use std::collections::{
    HashMap,
    HashSet,
};

use agdb::{
    CountComparison,
    DbAny,
    DbId,
    QueryBuilder,
};

use super::verification::recompute_artist_verified;
use crate::db::{
    self,
    Artist,
    CueSheet,
    CueTrack,
    Release,
    Track,
    TrackSource,
    graph::{
        edge_count_map,
        ensure_owned_edge,
    },
    metadata::cascade_remove_entities,
};

pub(crate) fn cleanup_orphaned_metadata(db: &mut DbAny) -> anyhow::Result<()> {
    let all_tracks: Vec<Track> = db
        .exec(
            QueryBuilder::select()
                .elements::<Track>()
                .search()
                .from("tracks")
                .query(),
        )?
        .try_into()?;

    let mut orphan_track_ids: Vec<DbId> = Vec::new();
    for track in all_tracks {
        let track_db_id: DbId = match track.db_id {
            Some(id) => id.into(),
            None => continue,
        };
        if db::entries::get_by_track(db, track_db_id)?.is_empty() {
            orphan_track_ids.push(track_db_id);
        }
    }

    cascade_remove_entities(db, &orphan_track_ids)?;

    let releases: Vec<Release> = db
        .exec(
            QueryBuilder::select()
                .elements::<Release>()
                .search()
                .from("releases")
                .query(),
        )?
        .try_into()?;
    let mut orphan_release_ids = Vec::new();
    for release in releases {
        let release_db_id: DbId = match release.db_id {
            Some(id) => id.into(),
            None => continue,
        };
        let tracks: Vec<Track> = db
            .exec(
                QueryBuilder::select()
                    .elements::<Track>()
                    .search()
                    .from(release_db_id)
                    .query(),
            )?
            .try_into()?;
        if tracks.is_empty() {
            orphan_release_ids.push(release_db_id);
        }
    }

    cascade_remove_entities(db, &orphan_release_ids)?;

    let mut connected_artist_ids: HashSet<DbId> = HashSet::new();
    for origin in ["releases", "tracks"] {
        let connected_artists: Vec<Artist> = db
            .exec(
                QueryBuilder::select()
                    .elements::<Artist>()
                    .search()
                    .from(origin)
                    .query(),
            )?
            .try_into()?;
        for artist in connected_artists {
            if let Some(artist_db_id) = artist.db_id {
                connected_artist_ids.insert(artist_db_id.into());
            }
        }
    }
    let all_artists: Vec<Artist> = db
        .exec(
            QueryBuilder::select()
                .elements::<Artist>()
                .search()
                .from("artists")
                .query(),
        )?
        .try_into()?;
    let orphan_artist_ids: Vec<DbId> = all_artists
        .into_iter()
        .filter_map(|artist| {
            let artist_db_id: DbId = artist.db_id?.into();
            if connected_artist_ids.contains(&artist_db_id) {
                None
            } else {
                Some(artist_db_id)
            }
        })
        .collect();

    cascade_remove_entities(db, &orphan_artist_ids)?;

    // Cleanup order matters for cue/source graph nodes:
    // 1) remove orphan track_sources, 2) then orphan cue_tracks no longer referenced
    // by any source, 3) then orphan cue_sheets with no remaining cue_track children.
    let all_track_sources: Vec<TrackSource> = db
        .exec(
            QueryBuilder::select()
                .elements::<TrackSource>()
                .search()
                .from("track_sources")
                .query(),
        )?
        .try_into()?;
    let mut orphan_track_source_ids = Vec::new();
    for source in all_track_sources {
        let Some(source_db_id) = source.db_id else {
            continue;
        };
        let incoming = db.exec(
            QueryBuilder::search()
                .to(source_db_id)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .query(),
        )?;
        let mut has_track_ref = false;
        for edge in incoming.elements {
            if edge.to != Some(source_db_id) {
                continue;
            }
            let Some(from_id) = edge.from else {
                continue;
            };
            if db::tracks::get_by_id(db, from_id)?.is_some() {
                has_track_ref = true;
                break;
            }
        }
        if !has_track_ref {
            orphan_track_source_ids.push(source_db_id);
        }
    }
    cascade_remove_entities(db, &orphan_track_source_ids)?;

    let all_cue_tracks: Vec<CueTrack> = db
        .exec(
            QueryBuilder::select()
                .elements::<CueTrack>()
                .search()
                .from("cue_tracks")
                .query(),
        )?
        .try_into()?;
    let mut orphan_cue_track_ids = Vec::new();
    for cue_track in all_cue_tracks {
        let Some(cue_track_db_id) = cue_track.db_id else {
            continue;
        };
        let incoming = db.exec(
            QueryBuilder::search()
                .to(cue_track_db_id)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .query(),
        )?;
        let mut has_source_ref = false;
        for edge in incoming.elements {
            if edge.to != Some(cue_track_db_id) {
                continue;
            }
            let Some(from_id) = edge.from else {
                continue;
            };
            if db::track_sources::get_by_id(db, from_id)?.is_some() {
                has_source_ref = true;
                break;
            }
        }
        if !has_source_ref {
            orphan_cue_track_ids.push(cue_track_db_id);
        }
    }
    cascade_remove_entities(db, &orphan_cue_track_ids)?;

    let all_cue_sheets: Vec<CueSheet> = db
        .exec(
            QueryBuilder::select()
                .elements::<CueSheet>()
                .search()
                .from("cue_sheets")
                .query(),
        )?
        .try_into()?;
    let mut orphan_cue_sheet_ids = Vec::new();
    for cue_sheet in all_cue_sheets {
        let Some(cue_sheet_db_id) = cue_sheet.db_id else {
            continue;
        };
        let outgoing = db.exec(
            QueryBuilder::search()
                .from(cue_sheet_db_id)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .query(),
        )?;
        let mut has_cue_track_ref = false;
        for edge in outgoing.elements {
            if edge.from != Some(cue_sheet_db_id) {
                continue;
            }
            let Some(to_id) = edge.to else {
                continue;
            };
            if db::cue::tracks::get_by_id(db, to_id)?.is_some() {
                has_cue_track_ref = true;
                break;
            }
        }
        if !has_cue_track_ref {
            orphan_cue_sheet_ids.push(cue_sheet_db_id);
        }
    }
    cascade_remove_entities(db, &orphan_cue_sheet_ids)?;

    Ok(())
}

/// Merge artists that share the same external ID (e.g. MusicBrainz MBID) but were
/// created as separate nodes due to different file tag spellings.
pub(crate) fn deduplicate_artists_by_external_id(db: &mut DbAny) -> anyhow::Result<u32> {
    let all_artists: Vec<Artist> = db
        .exec(
            QueryBuilder::select()
                .elements::<Artist>()
                .search()
                .from("artists")
                .query(),
        )?
        .try_into()?;

    let mut groups: HashMap<String, Vec<DbId>> = HashMap::new();
    let mut artist_lookup: HashMap<DbId, Artist> = HashMap::new();
    for artist in &all_artists {
        let Some(artist_db_id) = artist.db_id.clone().map(DbId::from) else {
            continue;
        };
        artist_lookup.insert(artist_db_id, artist.clone());
        let external_ids = db::external_ids::get_for_entity(db, artist_db_id)?;
        for ext_id in external_ids {
            if ext_id.id_type == "artist_id" {
                let id_value = ext_id.id_value.trim();
                if id_value.is_empty() {
                    continue;
                }
                let key = format!("{}:{}", ext_id.provider_id, id_value);
                groups.entry(key).or_default().push(artist_db_id);
            }
        }
    }

    let mut merged_count: u32 = 0;
    let mut winners: Vec<DbId> = Vec::new();
    for (key, artist_ids) in &groups {
        let mut unique_artist_ids = Vec::new();
        let mut seen_artist_ids: HashSet<DbId> = HashSet::new();
        for &artist_id in artist_ids {
            if seen_artist_ids.insert(artist_id) {
                unique_artist_ids.push(artist_id);
            }
        }

        if unique_artist_ids.len() <= 1 {
            continue;
        }

        let winner = select_merge_winner(db, &artist_lookup, &unique_artist_ids)?;
        for loser in unique_artist_ids.into_iter().filter(|id| *id != winner) {
            tracing::info!(
                winner = winner.0,
                loser = loser.0,
                external_id = %key,
                "merging duplicate artist"
            );
            migrate_metadata(db, winner, loser)?;
            db.transaction_mut(|t| merge_artist_into(t, winner, loser))?;
            merged_count += 1;
        }
        winners.push(winner);
    }

    if merged_count > 0 {
        for &winner in &winners {
            let _ = recompute_artist_verified(db, winner);
        }
        tracing::info!(merged_count, "deduplicated artists by external id");
    }

    Ok(merged_count)
}

fn select_merge_winner(
    db: &DbAny,
    artist_lookup: &HashMap<DbId, Artist>,
    artist_ids: &[DbId],
) -> anyhow::Result<DbId> {
    let edge_counts = edge_count_map(db, artist_ids)?;
    let mut ordered_ids = artist_ids.to_vec();

    ordered_ids.sort_by(|a, b| {
        let a_artist = artist_lookup.get(a);
        let b_artist = artist_lookup.get(b);

        let a_verified = a_artist.is_some_and(|artist| artist.verified);
        let b_verified = b_artist.is_some_and(|artist| artist.verified);
        match b_verified.cmp(&a_verified) {
            Ordering::Equal => {}
            ord => return ord,
        }

        let a_edges = edge_counts.get(a).copied().unwrap_or(0);
        let b_edges = edge_counts.get(b).copied().unwrap_or(0);
        match b_edges.cmp(&a_edges) {
            Ordering::Equal => {}
            ord => return ord,
        }

        let a_created_at = a_artist
            .and_then(|artist| artist.created_at)
            .unwrap_or(u64::MAX);
        let b_created_at = b_artist
            .and_then(|artist| artist.created_at)
            .unwrap_or(u64::MAX);
        match a_created_at.cmp(&b_created_at) {
            Ordering::Equal => {}
            ord => return ord,
        }

        a.0.cmp(&b.0)
    });

    let winner = ordered_ids[0];
    let winner_artist = artist_lookup.get(&winner);
    tracing::debug!(
        winner = winner.0,
        winner_verified = winner_artist.is_some_and(|artist| artist.verified),
        winner_created_at = winner_artist.and_then(|artist| artist.created_at),
        winner_edges = edge_counts.get(&winner).copied().unwrap_or(0),
        "selected artist merge winner"
    );

    Ok(winner)
}

fn merge_artist_into(
    db: &mut impl crate::db::DbAccess,
    winner: DbId,
    loser: DbId,
) -> anyhow::Result<()> {
    let incoming = db.exec(
        QueryBuilder::search()
            .to(loser)
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(1))
            .query(),
    )?;

    for element in &incoming.elements {
        if let Some(from_id) = element.from {
            ensure_owned_edge(db, from_id, winner)?;
        }
    }

    // `merge_artist_into` runs inside `deduplicate_artists_by_external_id`'s outer
    // `transaction_mut`; use the in-txn variant — agdb does not support reentrant transactions.
    crate::db::metadata::cascade_remove_entities_in_txn(db, &[loser])?;

    Ok(())
}

fn migrate_metadata(db: &mut DbAny, winner: DbId, loser: DbId) -> anyhow::Result<()> {
    let loser_layers = db::metadata::layers::get_for_entity(db, loser)?;
    let winner_layers = db::metadata::layers::get_for_entity(db, winner)?;
    let winner_provider_ids: HashSet<&str> = winner_layers
        .iter()
        .map(|l| l.provider_id.as_str())
        .collect();

    for layer in &loser_layers {
        if winner_provider_ids.contains(layer.provider_id.as_str()) {
            continue;
        }
        let mut migrated = layer.clone();
        migrated.db_id = None;
        db::metadata::layers::upsert(db, winner, &migrated)?;
    }

    let loser_ext_ids = db::external_ids::get_for_entity(db, loser)?;
    for ext_id in &loser_ext_ids {
        let existing = db::external_ids::get(db, winner, &ext_id.provider_id, &ext_id.id_type)?;
        if existing.is_some() {
            continue;
        }
        db::external_ids::upsert(
            db,
            winner,
            &ext_id.provider_id,
            &ext_id.id_type,
            &ext_id.id_value,
            ext_id.source,
        )?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{
        self,
        providers::external_ids::IdSource,
        test_db,
    };

    #[test]
    fn dedup_drops_loser_metadata_layers_instead_of_migrating() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;

        db::providers::upsert(
            &mut db,
            &db::ProviderConfig {
                db_id: None,
                id: nanoid::nanoid!(),
                provider_id: "musicbrainz".to_string(),
                display_name: "MusicBrainz".to_string(),
                priority: 100,
                enabled: true,
            },
        )?;

        let artist_a = test_db::insert_artist(&mut db, "Radiohead")?;
        let artist_b = test_db::insert_artist(&mut db, "Radiohead")?;

        // Give artist A more edges so it wins the merge
        let release = test_db::insert_release(&mut db, "OK Computer")?;
        test_db::connect(&mut db, release, artist_a)?;
        let track = test_db::insert_track(&mut db, "Paranoid Android")?;
        test_db::connect(&mut db, track, artist_a)?;

        db::external_ids::upsert(
            &mut db,
            artist_a,
            "musicbrainz",
            "artist_id",
            "mbid-123",
            IdSource::Plugin,
        )?;
        db::external_ids::upsert(
            &mut db,
            artist_b,
            "musicbrainz",
            "artist_id",
            "mbid-123",
            IdSource::Plugin,
        )?;

        // Artist B (the loser) has a layer from a different provider
        db::metadata::layers::upsert(
            &mut db,
            artist_b,
            &db::MetadataLayer {
                db_id: None,
                id: nanoid::nanoid!(),
                provider_id: "local-scanner".to_string(),
                fields: r#"{"artist_name": "Radiohead", "description": "English rock band"}"#
                    .to_string(),
                updated_at: 1000,
            },
        )?;

        let merged_count = deduplicate_artists_by_external_id(&mut db)?;
        assert_eq!(merged_count, 1);

        let a_exists = db::artists::get_by_id(&db, artist_a)?.is_some();
        assert!(a_exists, "artist A should win (more edges)");

        let winner_layers = db::metadata::layers::get_for_entity(&db, artist_a)?;
        let has_local_scanner_layer = winner_layers
            .iter()
            .any(|l| l.provider_id == "local-scanner");

        assert!(
            has_local_scanner_layer,
            "winner should have inherited loser's local-scanner layer"
        );

        Ok(())
    }

    #[test]
    fn dedup_drops_loser_external_ids_from_other_providers() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;

        db::providers::upsert(
            &mut db,
            &db::ProviderConfig {
                db_id: None,
                id: nanoid::nanoid!(),
                provider_id: "provider-a".to_string(),
                display_name: "Provider A".to_string(),
                priority: 100,
                enabled: true,
            },
        )?;
        db::providers::upsert(
            &mut db,
            &db::ProviderConfig {
                db_id: None,
                id: nanoid::nanoid!(),
                provider_id: "provider-b".to_string(),
                display_name: "Provider B".to_string(),
                priority: 50,
                enabled: true,
            },
        )?;

        let artist_a = test_db::insert_artist(&mut db, "Radiohead")?;
        let artist_b = test_db::insert_artist(&mut db, "Radiohead")?;

        // Give artist A more edges so it wins the merge
        let release = test_db::insert_release(&mut db, "OK Computer")?;
        test_db::connect(&mut db, release, artist_a)?;
        let track = test_db::insert_track(&mut db, "Paranoid Android")?;
        test_db::connect(&mut db, track, artist_a)?;

        // Both artists share the same ID from provider A (triggers dedup)
        db::external_ids::upsert(
            &mut db,
            artist_a,
            "provider-a",
            "artist_id",
            "shared-id",
            IdSource::Plugin,
        )?;
        db::external_ids::upsert(
            &mut db,
            artist_b,
            "provider-a",
            "artist_id",
            "shared-id",
            IdSource::Plugin,
        )?;

        // Artist B (the loser) also has an ID from provider B
        db::external_ids::upsert(
            &mut db,
            artist_b,
            "provider-b",
            "artist_id",
            "provider-b-id",
            IdSource::Plugin,
        )?;

        let merged_count = deduplicate_artists_by_external_id(&mut db)?;
        assert_eq!(merged_count, 1);

        let a_exists = db::artists::get_by_id(&db, artist_a)?.is_some();
        assert!(a_exists, "artist A should win (more edges)");

        let winner_ext_ids = db::external_ids::get_for_entity(&db, artist_a)?;
        let has_provider_b_id = winner_ext_ids
            .iter()
            .any(|id| id.provider_id == "provider-b");

        assert!(
            has_provider_b_id,
            "winner should have inherited loser's provider-b external ID"
        );

        Ok(())
    }
}
