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
    DbAnyTransactionMut,
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
    let mut orphan_artist_ids: Vec<DbId> = all_artists
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
    // Layer edges point from an artist into the layer, so the walk from the
    // roots misses artists that only layers reference.
    let spared = db::metadata::layers::edges::spared_artists(
        db,
        &orphan_artist_ids.iter().copied().collect(),
    )?;
    orphan_artist_ids.retain(|artist_id| !spared.contains(artist_id));

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
            if edge.to != source_db_id {
                continue;
            }
            let from_id = edge.from;
            if from_id.0 == 0 {
                continue;
            }
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
            if edge.to != cue_track_db_id {
                continue;
            }
            let from_id = edge.from;
            if from_id.0 == 0 {
                continue;
            }
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
            if edge.from != cue_sheet_db_id {
                continue;
            }
            let to_id = edge.to;
            if to_id.0 == 0 {
                continue;
            }
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
            let winner_relations_are_manual = db::metadata::manual_overrides::owns_field(
                db,
                winner,
                db::MetadataField::Relations,
            )?;
            let loser_relations_are_manual = db::metadata::manual_overrides::owns_field(
                db,
                loser,
                db::MetadataField::Relations,
            )?;
            db.transaction_mut(|t| {
                migrate_metadata(t, winner, loser)?;
                merge_artist_into(
                    t,
                    winner,
                    loser,
                    winner_relations_are_manual,
                    loser_relations_are_manual,
                )
            })?;
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
    // Layer edges are not effective references, so they never pick the winner.
    let mut edge_counts = edge_count_map(db, artist_ids)?;
    for (&artist_id, count) in &mut edge_counts {
        *count = count.saturating_sub(db::metadata::layers::edges::reference_count(db, artist_id)?);
    }
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
    db: &mut DbAnyTransactionMut<'_>,
    winner: DbId,
    loser: DbId,
    winner_relations_are_manual: bool,
    loser_relations_are_manual: bool,
) -> anyhow::Result<()> {
    let incoming_relations = db::artists::relations::get_relations_to(db, loser, None)?;
    let outgoing_relations = db::artists::relations::get_relations_from(db, loser, None)?;
    let relation_edge_ids = db::artists::relations::incoming_edge_ids(db, loser)?;

    // Layers own the other incoming relations; re-applying the winner below
    // rebuilds them. Manual relations live only on the effective graph.
    for (relation, source_id) in incoming_relations {
        if source_id != winner
            && db::metadata::manual_overrides::owns_field(
                db,
                source_id,
                db::MetadataField::Relations,
            )?
            && !db::artists::relations::exists(db, source_id, winner, relation.relation_type)?
        {
            db::artists::relations::link(
                db,
                source_id,
                winner,
                relation.relation_type,
                relation.attributes,
            )?;
        }
    }
    if !winner_relations_are_manual {
        if loser_relations_are_manual {
            db::artists::relations::replace_from(db, winner, &[])?;
        }
        for (relation, target_id) in outgoing_relations {
            if target_id != winner
                && !db::artists::relations::exists(db, winner, target_id, relation.relation_type)?
            {
                db::artists::relations::link(
                    db,
                    winner,
                    target_id,
                    relation.relation_type,
                    relation.attributes,
                )?;
            }
        }
    }

    db::metadata::layers::edges::retarget_artist(db, winner, loser)?;

    // Layers own the other credits; re-applying their owners below rebuilds
    // them. Manual and locked owners keep what is stored.
    let winner_credits = db::credits::CreditSet::new(&db::credits::links_to_artist(db, winner)?);
    let credit_links = db::credits::links_to_artist(db, loser)?;
    let mut reapplied_owners = Vec::new();
    for link in &credit_links {
        if !credits_stay_as_stored(db, link.owner_id)? {
            if !reapplied_owners.contains(&link.owner_id) {
                reapplied_owners.push(link.owner_id);
            }
            continue;
        }
        if !winner_credits.contains(link.owner_id, winner, link.credit.role()) {
            db::credits::link(db, link.owner_id, winner, &link.credit)?;
        }
    }
    let credit_edge_ids: HashSet<DbId> = credit_links.iter().map(|link| link.edge_id).collect();

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
        if relation_edge_ids.contains(&element.id) || credit_edge_ids.contains(&element.id) {
            continue;
        }
        let from_id = element.from;
        if from_id.0 != 0 {
            ensure_owned_edge(db, from_id, winner)?;
        }
    }

    // `merge_artist_into` runs inside `deduplicate_artists_by_external_id`'s outer
    // `transaction_mut`; use the in-txn variant — agdb does not support reentrant transactions.
    crate::db::metadata::cascade_remove_entities_in_txn(db, &[loser])?;
    for owner_id in reapplied_owners {
        crate::services::metadata::merging::apply_merged_metadata_to_entity_inside_tx(
            db, owner_id,
        )?;
    }
    crate::services::metadata::merging::apply_merged_metadata_to_entity_inside_tx(db, winner)
}

/// Whether a release's or track's effective credits are kept rather than
/// rebuilt from its layers: manual edits own them, or a lock freezes them.
fn credits_stay_as_stored(db: &impl crate::db::DbAccess, owner_id: DbId) -> anyhow::Result<bool> {
    let locked = match db::releases::get_by_id(db, owner_id)? {
        Some(release) => release.locked.unwrap_or(false),
        None => {
            db::tracks::get_by_id(db, owner_id)?.is_some_and(|track| track.locked.unwrap_or(false))
        }
    };
    Ok(locked
        || db::metadata::manual_overrides::owns_field(db, owner_id, db::MetadataField::Credits)?)
}

fn migrate_metadata(
    db: &mut DbAnyTransactionMut<'_>,
    winner: DbId,
    loser: DbId,
) -> anyhow::Result<()> {
    let loser_layers = db::metadata::layers::get_for_entity(db, loser)?;
    let winner_layers = db::metadata::layers::get_for_entity(db, winner)?;
    let winner_source_ids: HashSet<&str> =
        winner_layers.iter().map(|l| l.source_id.as_str()).collect();

    for layer in &loser_layers {
        if !winner_source_ids.contains(layer.source_id.as_str()) {
            db::metadata::layers::copy_inside_tx(db, loser, winner, &layer.source_id)?;
            continue;
        }
        // Both duplicates heard from this source; neither one's relations
        // are wrong, so the winner's layer keeps both.
        let Some(winner_layer) = winner_layers
            .iter()
            .find(|winner_layer| winner_layer.source_id == layer.source_id)
        else {
            continue;
        };
        let mut loser_relations = layer.relations.clone();
        for inputs in loser_relations.values_mut() {
            inputs.retain(|input| input.artist_id != winner);
        }
        if loser_relations.is_empty() {
            continue;
        }
        let relations =
            db::metadata::layers::union_relations(&winner_layer.relations, loser_relations);
        let mut merged = winner_layer.clone().with_relations(relations);
        merged.updated_at = merged.updated_at.max(layer.updated_at);
        db::metadata::layers::upsert_inside_tx(db, winner, &merged)?;
    }

    let loser_ext_ids = db::external_ids::get_for_entity_inside_tx(db, loser)?;
    for ext_id in &loser_ext_ids {
        let existing =
            db::external_ids::get_inside_tx(db, winner, &ext_id.provider_id, &ext_id.id_type)?;
        if existing.is_some() {
            continue;
        }
        db::external_ids::copy_inside_tx(db, winner, ext_id)?;
    }

    db::metadata::custom_fields::copy_between_entities(db, loser, winner)?;
    db::metadata::manual_overrides::merge_into(db, winner, loser)?;
    if db::metadata::manual_overrides::get(db, winner)?.is_some() {
        crate::services::metadata::merging::apply_merged_metadata_to_entity_inside_tx(db, winner)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{
        self,
        IdSource,
        test_db,
    };

    #[test]
    fn dedup_drops_loser_metadata_layers_instead_of_migrating() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;

        db::providers::upsert(
            &mut db,
            &db::ProviderConfig {
                db_id: None,
                provider_id: "musicbrainz".to_string(),
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
            IdSource::Resolved,
        )?;
        db::external_ids::upsert(
            &mut db,
            artist_b,
            "musicbrainz",
            "artist_id",
            "mbid-123",
            IdSource::Resolved,
        )?;

        // Artist B (the loser) has a layer from a different provider
        db::metadata::layers::upsert(
            &mut db,
            artist_b,
            &db::MetadataLayer::new(
                "local-scanner",
                db::metadata::layers::fields_from_json(serde_json::json!({
                    "artist_name": "Radiohead",
                    "description": "English rock band",
                })),
                1000,
            ),
        )?;

        let merged_count = deduplicate_artists_by_external_id(&mut db)?;
        assert_eq!(merged_count, 1);

        let a_exists = db::artists::get_by_id(&db, artist_a)?.is_some();
        assert!(a_exists, "artist A should win (more edges)");

        let winner_layers = db::metadata::layers::get_for_entity(&db, artist_a)?;
        let has_local_scanner_layer = winner_layers.iter().any(|l| l.source_id == "local-scanner");

        assert!(
            has_local_scanner_layer,
            "winner should have inherited loser's local-scanner layer"
        );

        Ok(())
    }

    #[test]
    fn artist_merge_retargets_layer_edges_before_removing_the_loser() -> anyhow::Result<()> {
        use crate::db::metadata::layers::{
            self,
            edges,
        };

        let mut db = test_db::new_test_db()?;
        let credited = test_db::insert_artist(&mut db, "Radiohead")?;
        let layer_only = test_db::insert_artist(&mut db, "Radiohead")?;
        let member = test_db::insert_artist(&mut db, "Thom Yorke")?;
        let track = test_db::insert_track(&mut db, "Creep")?;
        test_db::connect(&mut db, track, credited)?;
        for artist in [credited, layer_only] {
            db::external_ids::upsert(
                &mut db,
                artist,
                "musicbrainz",
                "artist_id",
                "mbid-123",
                IdSource::Resolved,
            )?;
        }
        for node_id in [track, member] {
            layers::upsert(
                &mut db,
                node_id,
                &db::MetadataLayer::new("musicbrainz", Default::default(), 1),
            )?;
        }
        let track_layer = layers::layer_id(&db, track, "musicbrainz")?.expect("track layer");
        let member_layer = layers::layer_id(&db, member, "musicbrainz")?.expect("member layer");
        db.transaction_mut(|t| {
            edges::replace_credits(
                t,
                track_layer,
                db::CreditType::Vocalist,
                &[edges::LayerCreditInput {
                    artist_id: layer_only,
                    detail: Some("lead".to_string()),
                }],
            )?;
            edges::replace_relations(
                t,
                member_layer,
                db::ArtistRelationType::MemberOf,
                &[edges::LayerRelationInput {
                    artist_id: layer_only,
                    attributes: Some("vocals".to_string()),
                }],
            )
        })?;

        assert_eq!(deduplicate_artists_by_external_id(&mut db)?, 1);

        let (winner, loser) = (credited, layer_only);
        assert!(db::artists::get_by_id(&db, loser)?.is_none());
        let credits = edges::credits(&db, track_layer)?;
        assert_eq!(credits.len(), 1);
        assert_eq!(credits[0].target_id, winner);
        assert_eq!(credits[0].edge.credit_type, db::CreditType::Vocalist);
        assert_eq!(credits[0].edge.detail.as_deref(), Some("lead"));
        let relations = edges::relations(&db, member_layer)?;
        assert_eq!(relations.len(), 1);
        assert_eq!(relations[0].target_id, winner);
        assert_eq!(relations[0].edge.attributes.as_deref(), Some("vocals"));
        Ok(())
    }

    #[test]
    fn merging_duplicate_characters_keeps_both_voice_actors() -> anyhow::Result<()> {
        use crate::db::metadata::layers::{
            self,
            edges::LayerRelationInput,
        };
        use crate::services::metadata::layers::LOCAL_SOURCE_ID;

        let mut db = test_db::new_test_db()?;
        let first = test_db::insert_artist(&mut db, "Character")?;
        let second = test_db::insert_artist(&mut db, "Character")?;
        let first_actor = test_db::insert_artist(&mut db, "First Actor")?;
        let second_actor = test_db::insert_artist(&mut db, "Second Actor")?;
        for (character, actor, updated_at) in [(first, first_actor, 1), (second, second_actor, 2)] {
            db::external_ids::upsert(
                &mut db,
                character,
                "musicbrainz",
                "artist_id",
                "character-mbid",
                IdSource::Resolved,
            )?;
            layers::upsert(
                &mut db,
                character,
                &db::MetadataLayer::new(
                    LOCAL_SOURCE_ID,
                    layers::fields_from_json(serde_json::json!({"artist_name": "Character"})),
                    updated_at,
                )
                .with_relations(std::collections::BTreeMap::from([(
                    db::ArtistRelationType::VoiceActor,
                    vec![LayerRelationInput {
                        artist_id: actor,
                        attributes: None,
                    }],
                )])),
            )?;
            crate::services::metadata::merging::apply_merged_metadata_to_entity(
                &mut db, character,
            )?;
        }

        assert_eq!(deduplicate_artists_by_external_id(&mut db)?, 1);
        let winner = if db::artists::get_by_id(&db, first)?.is_some() {
            first
        } else {
            second
        };
        let voice_actors = |db: &DbAny| -> anyhow::Result<Vec<DbId>> {
            let mut actors: Vec<DbId> = db::artists::relations::get_relations_to(
                db,
                winner,
                Some(db::ArtistRelationType::VoiceActor),
            )?
            .into_iter()
            .map(|(_, actor)| actor)
            .collect();
            actors.sort();
            Ok(actors)
        };
        let mut expected = vec![first_actor, second_actor];
        expected.sort();
        assert_eq!(voice_actors(&db)?, expected);
        let local = layers::get_for_entity(&db, winner)?
            .into_iter()
            .find(|layer| layer.source_id == LOCAL_SOURCE_ID)
            .expect("winner keeps its layer");
        assert_eq!(
            local.updated_at, 2,
            "the combined layer is as recent as its newer half"
        );

        crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, winner)?;
        assert_eq!(voice_actors(&db)?, expected, "a later apply keeps both");
        Ok(())
    }

    #[test]
    fn dedup_drops_loser_external_ids_from_other_providers() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;

        db::providers::upsert(
            &mut db,
            &db::ProviderConfig {
                db_id: None,
                provider_id: "provider-a".to_string(),
                priority: 100,
                enabled: true,
            },
        )?;
        db::providers::upsert(
            &mut db,
            &db::ProviderConfig {
                db_id: None,
                provider_id: "provider-b".to_string(),
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
            IdSource::Resolved,
        )?;
        db::external_ids::upsert(
            &mut db,
            artist_b,
            "provider-a",
            "artist_id",
            "shared-id",
            IdSource::Resolved,
        )?;

        // Artist B (the loser) also has an ID from provider B
        db::external_ids::upsert(
            &mut db,
            artist_b,
            "provider-b",
            "artist_id",
            "provider-b-id",
            IdSource::Resolved,
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

    #[test]
    fn artist_merge_retargets_relations_without_losing_metadata() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;
        let source = test_db::insert_artist(&mut db, "Source")?;
        let winner = test_db::insert_artist(&mut db, "Winner")?;
        let loser = test_db::insert_artist(&mut db, "Loser")?;
        let target = test_db::insert_artist(&mut db, "Target")?;
        db::artists::relations::link(
            &mut db,
            source,
            winner,
            db::ArtistRelationType::MemberOf,
            Some("existing incoming".to_string()),
        )?;
        db::artists::relations::link(
            &mut db,
            winner,
            target,
            db::ArtistRelationType::VoiceActor,
            Some("existing outgoing".to_string()),
        )?;
        db::artists::relations::link(
            &mut db,
            source,
            loser,
            db::ArtistRelationType::MemberOf,
            Some("incoming".to_string()),
        )?;
        db::artists::relations::link(
            &mut db,
            loser,
            target,
            db::ArtistRelationType::VoiceActor,
            Some("outgoing".to_string()),
        )?;
        // Incoming relations survive a merge only as manual edits; layers own the rest.
        db::metadata::manual_overrides::replace(
            &mut db,
            source,
            &std::collections::BTreeMap::from([(
                db::MetadataField::Relations,
                serde_json::Value::Bool(true),
            )]),
        )?;

        db.transaction_mut(|transaction| {
            merge_artist_into(transaction, winner, loser, false, false)
        })?;

        let incoming = db::artists::relations::get_relations_from(&db, source, None)?;
        assert_eq!(incoming.len(), 1);
        assert_eq!(incoming[0].1, winner);
        assert_eq!(
            incoming[0].0.attributes.as_deref(),
            Some("existing incoming")
        );
        let outgoing = db::artists::relations::get_relations_from(&db, winner, None)?;
        assert_eq!(outgoing.len(), 1);
        assert_eq!(outgoing[0].1, target);
        assert_eq!(
            outgoing[0].0.attributes.as_deref(),
            Some("existing outgoing")
        );
        Ok(())
    }

    #[test]
    fn artist_merge_does_not_turn_relations_into_ownership_edges() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;
        let winner = test_db::insert_artist(&mut db, "Winner")?;
        let loser = test_db::insert_artist(&mut db, "Loser")?;
        db::artists::relations::link(
            &mut db,
            winner,
            loser,
            db::ArtistRelationType::MemberOf,
            None,
        )?;

        db.transaction_mut(|transaction| {
            merge_artist_into(transaction, winner, loser, false, false)
        })?;

        assert!(db::graph::direct_edge_ids(&db, winner, winner)?.is_empty());
        Ok(())
    }

    #[test]
    fn artist_merge_moves_credits_with_their_values() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;
        let track = test_db::insert_track(&mut db, "Track")?;
        let winner = test_db::insert_artist(&mut db, "Winner")?;
        let loser = test_db::insert_artist(&mut db, "Loser")?;
        let credit =
            |artist_id, detail: Option<&str>| db::metadata::layers::edges::LayerCreditInput {
                artist_id,
                detail: detail.map(str::to_string),
            };
        db::metadata::layers::upsert(
            &mut db,
            track,
            &db::MetadataLayer::new("local", Default::default(), 1).with_credits(
                std::collections::BTreeMap::from([
                    (
                        db::CreditType::Artist,
                        vec![credit(winner, None), credit(loser, None)],
                    ),
                    (
                        db::CreditType::Producer,
                        vec![credit(loser, Some("co-producer"))],
                    ),
                ]),
            ),
        )?;
        crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, track)?;
        assert_eq!(db::credits::links_for_owner(&db, track)?.len(), 3);

        db.transaction_mut(|transaction| {
            merge_artist_into(transaction, winner, loser, false, false)
        })?;

        let mut credits: Vec<db::Credit> = db::credits::links_for_owner(&db, track)?
            .into_iter()
            .inspect(|link| assert_eq!(link.artist_id, winner))
            .map(|link| link.credit)
            .collect();
        credits.sort_by_key(|credit| credit.artist_order);
        assert_eq!(
            credits,
            [
                db::Credit {
                    credit_type: db::CreditType::Artist,
                    detail: None,
                    artist_order: 0,
                },
                db::Credit {
                    credit_type: db::CreditType::Producer,
                    detail: Some("co-producer".to_string()),
                    artist_order: 1,
                },
            ]
        );
        let before = db::credits::links_for_owner(&db, track)?;
        crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, track)?;
        assert_eq!(
            db::credits::links_for_owner(&db, track)?,
            before,
            "a later apply keeps the merged credits"
        );
        Ok(())
    }
}
