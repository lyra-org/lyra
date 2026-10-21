// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};
use std::time::{
    SystemTime,
    UNIX_EPOCH,
};

use agdb::{
    DbAny,
    DbAnyTransactionMut,
    DbId,
};

use crate::db::{
    self,
    ArtistRelationType,
    CreditType,
    DbAccess,
    IdSource,
    MetadataField,
    MetadataLayer,
    ProviderCustomFields,
    metadata::layers::{
        LayerCredits,
        LayerFields,
        LayerRelations,
        edges::{
            LayerCreditInput,
            LayerRelationInput,
        },
        normalize_value,
    },
};
use crate::services::EntityType;

/// Reserved source for file-derived metadata; plugin registration rejects it.
pub(crate) const LOCAL_SOURCE_ID: &str = "local";

/// Reserved source for manual edits, ranked above every provider; plugin
/// registration rejects it.
pub(crate) const MANUAL_SOURCE_ID: &str = "manual";

/// Whether a merge of `loser` into `winner` can carry their manual layers; one
/// that no longer validates keeps both entities apart until it is repaired.
pub(crate) fn manual_layers_mergeable(db: &impl DbAccess, winner: DbId, loser: DbId) -> bool {
    for node_id in [winner, loser] {
        if let Err(error) = db::metadata::layers::find_valid(db, node_id, MANUAL_SOURCE_ID) {
            tracing::warn!(
                winner = winner.0,
                loser = loser.0,
                invalid = node_id.0,
                error = %format!("{error:#}"),
                "skipping merge over an invalid manual metadata layer"
            );
            return false;
        }
    }
    true
}

/// File-derived metadata; omitted fields express no opinion, while null clears.
#[derive(Default)]
pub(crate) struct LocalLayer {
    fields: LayerFields,
    relations: LayerRelations,
    credits: LayerCredits,
}

impl LocalLayer {
    pub(crate) fn supply<T: Into<serde_json::Value>>(
        &mut self,
        field: MetadataField,
        value: Option<T>,
    ) -> anyhow::Result<()> {
        if let Some(value) = value {
            self.fields
                .insert(field, normalize_value(field, value.into())?);
        }
        Ok(())
    }

    /// Supplies the primary artist credits the tags name, in order. Tags only
    /// yield primary credits; the other roles belong to providers and manual edits.
    pub(crate) fn supply_artists(&mut self, artist_ids: &[DbId]) {
        self.supply_credits(
            CreditType::Artist,
            artist_ids
                .iter()
                .map(|&artist_id| LayerCreditInput {
                    artist_id,
                    detail: None,
                })
                .collect(),
        );
    }

    pub(crate) fn supply_credits(
        &mut self,
        credit_type: CreditType,
        inputs: Vec<LayerCreditInput>,
    ) {
        self.credits.insert(credit_type, inputs);
    }

    pub(crate) fn supply_relations(
        &mut self,
        relation_type: ArtistRelationType,
        inputs: Vec<LayerRelationInput>,
    ) {
        self.relations.insert(relation_type, inputs);
    }

    pub(crate) fn save(
        self,
        db: &mut DbAnyTransactionMut<'_>,
        node_id: DbId,
    ) -> anyhow::Result<()> {
        let layer = MetadataLayer::new(LOCAL_SOURCE_ID, self.fields, now_secs())
            .with_relations(self.relations)
            .with_credits(self.credits);
        db::metadata::layers::upsert_inside_tx(db, node_id, &layer)?;
        super::merging::apply_merged_metadata_to_entity_inside_tx(db, node_id)
    }
}

/// Validates provider-supplied fields for the entity they describe.
fn validated_fields(
    db: &DbAny,
    node_id: DbId,
    fields: &LayerFields,
) -> anyhow::Result<LayerFields> {
    let entity_type = db::entities::metadata_entity_type(db, node_id)?
        .ok_or_else(|| anyhow::anyhow!("Entity not found: {}", node_id.0))?;
    fields
        .iter()
        .map(|(&field, value)| {
            anyhow::ensure!(
                entity_type.supports(field),
                "a {} has no '{field}' field",
                entity_type.as_str()
            );
            Ok((field, normalize_value(field, value.clone())?))
        })
        .collect()
}

pub(super) fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn artist_type_conflicts_with_layer(
    artist: Option<&db::Artist>,
    fields: &LayerFields,
    node_id: DbId,
    provider_id: &str,
) -> anyhow::Result<bool> {
    let Some(existing_type) = artist.and_then(|artist| artist.artist_type) else {
        return Ok(false);
    };
    let Some(incoming) = fields
        .get(&MetadataField::ArtistType)
        .filter(|value| !value.is_null())
    else {
        return Ok(false);
    };
    let incoming_type: db::ArtistType =
        db::metadata::layers::decode(MetadataField::ArtistType, incoming)?;
    if existing_type == incoming_type {
        return Ok(false);
    }

    tracing::warn!(
        node_id = node_id.0,
        provider_id,
        existing_artist_type = %existing_type,
        incoming_artist_type = %incoming_type,
        "skipping provider artist identity update with conflicting artist_type"
    );
    Ok(true)
}

/// What a provider supplies for one entity in a layer save.
#[derive(Clone, Default)]
pub(crate) struct ProviderValues {
    pub(crate) fields: LayerFields,
    pub(crate) relations: LayerRelations,
    pub(crate) credits: LayerCredits,
}

/// Custom field versions a provider sets or removes on one entity.
#[derive(Clone, Default)]
pub(crate) struct CustomFieldChanges {
    pub(crate) set: HashMap<u64, HashMap<String, serde_json::Value>>,
    pub(crate) remove: HashSet<u64>,
}

fn save_provider_custom_fields(
    db: &mut DbAny,
    node_id: DbId,
    provider_id: &str,
    changes: &CustomFieldChanges,
) -> anyhow::Result<()> {
    let custom_fields = &changes.set;
    let mut sorted_remove_versions: Vec<u64> = changes.remove.iter().copied().collect();
    sorted_remove_versions.sort_unstable();
    for version in sorted_remove_versions {
        db::metadata::custom_fields::remove(db, node_id, provider_id, version)?;
    }

    if custom_fields.is_empty() {
        return Ok(());
    }

    let now = now_secs();
    let mut sorted_versions: Vec<u64> = custom_fields.keys().copied().collect();
    sorted_versions.sort_unstable();
    for version in sorted_versions {
        let Some(fields) = custom_fields.get(&version) else {
            continue;
        };
        let fields_json = serde_json::to_string(fields)?;
        let row = ProviderCustomFields {
            db_id: None,
            provider_id: provider_id.to_string(),
            version,
            fields: fields_json,
            updated_at: now,
        };
        db::metadata::custom_fields::upsert(db, node_id, &row)?;
    }

    Ok(())
}

pub(crate) fn save_provider_layer(
    db: &mut DbAny,
    node_id: DbId,
    provider_id: &str,
    values: &ProviderValues,
    external_ids: &HashMap<String, String>,
    custom_fields: &CustomFieldChanges,
) -> anyhow::Result<()> {
    let fields = validated_fields(db, node_id, &values.fields)?;
    let relations = &values.relations;
    let artist = db::artists::get_by_id(db, node_id)?;
    let artist_type_conflict =
        artist_type_conflicts_with_layer(artist.as_ref(), &fields, node_id, provider_id)?;

    // A conflicting artist type keeps the source's stored fields, but its
    // relations still save. Layers save while locked; only application is
    // suppressed.
    let fields = if artist_type_conflict {
        db::metadata::layers::get_for_entity(db, node_id)?
            .into_iter()
            .find(|stored| stored.source_id == provider_id)
            .map(|stored| stored.fields)
            .unwrap_or_default()
    } else {
        fields
    };
    let has_content = !relations.is_empty()
        || !values.credits.is_empty()
        || (!artist_type_conflict && !fields.is_empty());
    if has_content
        && db::metadata::layers::upsert(
            db,
            node_id,
            &MetadataLayer::new(provider_id, fields, now_secs())
                .with_relations(relations.clone())
                .with_credits(values.credits.clone()),
        )?
    {
        super::merging::apply_merged_metadata_to_entity(db, node_id)?;
    }

    save_provider_custom_fields(db, node_id, provider_id, custom_fields)?;

    if artist_type_conflict {
        return Ok(());
    }

    if external_ids.is_empty() {
        return Ok(());
    }

    let is_artist_entity = artist.is_some();
    let artist_is_verified = artist.as_ref().is_some_and(|a| a.verified);
    let mut should_recompute_artist_verification = false;

    for (id_type, id_value) in external_ids {
        let existing_id = db::external_ids::get(db, node_id, provider_id, id_type)?;

        if is_artist_entity && id_type == "artist_id" {
            let incoming_artist_id = id_value.trim();
            if !incoming_artist_id.is_empty()
                && artist_is_verified
                && let Some(existing_artist_id) =
                    existing_id.as_ref().and_then(|e| e.resolved_value())
            {
                let existing_artist_id = existing_artist_id.trim();
                if !existing_artist_id.is_empty() && existing_artist_id != incoming_artist_id {
                    tracing::warn!(
                        node_id = node_id.0,
                        provider_id,
                        existing_artist_id = %existing_artist_id,
                        incoming_artist_id = %incoming_artist_id,
                        "skipping conflicting artist_id update for verified artist"
                    );
                    continue;
                }
            }
        }

        let external_id_changed = existing_id
            .as_ref()
            .is_none_or(|existing| existing.resolved_value() != Some(id_value.as_str()));
        if !external_id_changed {
            continue;
        }

        db::external_ids::upsert(
            db,
            node_id,
            provider_id,
            id_type,
            id_value,
            IdSource::Resolved,
        )?;

        if is_artist_entity && id_type == "artist_db_id" {
            should_recompute_artist_verification = true;
        }
    }

    if is_artist_entity && should_recompute_artist_verification {
        super::verification::recompute_artist_verified(db, node_id)?;
    }

    Ok(())
}

/// Records that `provider_id` found no match for an entity: blanks `id_types`
/// and clears the provider's layer. Artists are shared across releases, so one
/// release failing to match says nothing about them: an artist keeps its layer
/// and any ID the provider already set.
pub(crate) fn mark_unmatched(
    db: &mut DbAny,
    node_id: DbId,
    provider_id: &str,
    entity_type: EntityType,
    id_types: Vec<String>,
) -> anyhow::Result<()> {
    let is_artist = entity_type == EntityType::Artist;
    let mut external_ids = HashMap::new();
    for id_type in id_types {
        if is_artist
            && db::external_ids::get(db, node_id, provider_id, &id_type)?
                .is_some_and(|id| id.resolved_value().is_some_and(|value| !value.is_empty()))
        {
            continue;
        }
        external_ids.insert(id_type, String::new());
    }
    save_provider_layer(
        db,
        node_id,
        provider_id,
        &ProviderValues::default(),
        &external_ids,
        &Default::default(),
    )?;
    if !is_artist {
        clear_provider_layer(db, node_id, provider_id)?;
    }
    Ok(())
}

/// Removes everything `provider_id` contributed to an entity's metadata, its
/// layer and custom fields, and re-applies what the other layers resolve to.
/// External IDs are left to the caller.
pub(crate) fn clear_provider_layer(
    db: &mut DbAny,
    node_id: DbId,
    provider_id: &str,
) -> anyhow::Result<()> {
    db.transaction_mut(|t| -> anyhow::Result<()> {
        let removed_layer = db::metadata::layers::remove_inside_tx(t, node_id, provider_id)?;
        let mut removed_custom_fields = false;
        for row in db::metadata::custom_fields::get_for_entity(t, node_id)? {
            if row.provider_id == provider_id {
                removed_custom_fields |=
                    db::metadata::custom_fields::remove(t, node_id, provider_id, row.version)?;
            }
        }
        if removed_layer || removed_custom_fields {
            super::merging::apply_merged_metadata_to_entity_inside_tx(t, node_id)?;
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::*;
    use crate::db::test_db::{
        insert_artist,
        insert_release,
        new_test_db,
    };

    #[test]
    fn clearing_a_provider_layer_drops_its_fields_and_custom_fields() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Local Title")?;
        db::providers::upsert(
            &mut db,
            &db::ProviderConfig {
                db_id: None,
                provider_id: "provider".to_string(),
                priority: 100,
                enabled: true,
            },
        )?;
        let mut local = LocalLayer::default();
        local.supply(MetadataField::ReleaseTitle, Some("Local Title"))?;
        db.transaction_mut(|t| local.save(t, release_id))?;
        save_provider_layer(
            &mut db,
            release_id,
            "provider",
            &ProviderValues {
                fields: LayerFields::from([
                    (MetadataField::ReleaseTitle, json!("Wrong Title")),
                    (MetadataField::ReleaseDate, json!("1999")),
                ]),
                ..Default::default()
            },
            &HashMap::new(),
            &CustomFieldChanges {
                set: HashMap::from([(1, HashMap::from([("barcode".to_string(), json!("123"))]))]),
                ..Default::default()
            },
        )?;
        let matched = db::releases::get_by_id(&db, release_id)?.expect("release exists");
        assert_eq!(matched.release_title, "Wrong Title");
        assert_eq!(matched.release_date.as_deref(), Some("1999"));

        clear_provider_layer(&mut db, release_id, "provider")?;

        let cleared = db::releases::get_by_id(&db, release_id)?.expect("release exists");
        assert_eq!(cleared.release_title, "Local Title");
        assert_eq!(cleared.release_date, None);
        assert!(
            db::metadata::layers::get_for_entity(&db, release_id)?
                .iter()
                .all(|layer| layer.source_id != "provider")
        );
        assert!(db::metadata::custom_fields::get_for_entity(&db, release_id)?.is_empty());
        Ok(())
    }

    #[test]
    fn a_conflicting_artist_type_still_saves_relations() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let character = insert_artist(&mut db, "Character")?;
        let actor = insert_artist(&mut db, "Actor")?;
        let mut local = LocalLayer::default();
        local.supply(MetadataField::ArtistName, Some("Character"))?;
        local.supply(MetadataField::ArtistType, Some("character"))?;
        db.transaction_mut(|t| local.save(t, character))?;
        db::providers::upsert(
            &mut db,
            &db::ProviderConfig {
                db_id: None,
                provider_id: "provider".to_string(),
                priority: 100,
                enabled: true,
            },
        )?;

        save_provider_layer(
            &mut db,
            character,
            "provider",
            &ProviderValues {
                fields: LayerFields::from([
                    (MetadataField::ArtistName, json!("Renamed")),
                    (MetadataField::ArtistType, json!("person")),
                ]),
                relations: LayerRelations::from([(
                    db::ArtistRelationType::VoiceActor,
                    vec![LayerRelationInput {
                        artist_id: actor,
                        attributes: None,
                    }],
                )]),
                ..Default::default()
            },
            &HashMap::new(),
            &Default::default(),
        )?;

        let artist = db::artists::get_by_id(&db, character)?.expect("artist exists");
        assert_eq!(artist.artist_type, Some(db::ArtistType::Character));
        assert_eq!(
            artist.artist_name, "Character",
            "conflicting fields are skipped"
        );
        let relations = db::artists::relations::get_relations_to(
            &db,
            character,
            Some(db::ArtistRelationType::VoiceActor),
        )?;
        assert_eq!(relations.len(), 1);
        assert_eq!(relations[0].1, actor);
        Ok(())
    }

    fn musicbrainz_id(db: &DbAny, node_id: DbId, id_type: &str) -> anyhow::Result<Option<String>> {
        Ok(db::external_ids::get(db, node_id, "musicbrainz", id_type)?.map(|id| id.id_value))
    }

    #[test]
    fn unmatching_an_artist_keeps_the_id_the_provider_set() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let matched = insert_artist(&mut db, "Matched")?;
        let unknown = insert_artist(&mut db, "Unknown")?;
        save_provider_layer(
            &mut db,
            matched,
            "musicbrainz",
            &ProviderValues {
                fields: LayerFields::from([(MetadataField::ArtistName, json!("Matched"))]),
                ..Default::default()
            },
            &HashMap::from([("artist_id".to_string(), "artist-1".to_string())]),
            &Default::default(),
        )?;

        for artist in [matched, unknown] {
            mark_unmatched(
                &mut db,
                artist,
                "musicbrainz",
                EntityType::Artist,
                vec!["artist_id".to_string()],
            )?;
        }

        assert_eq!(
            musicbrainz_id(&db, matched, "artist_id")?.as_deref(),
            Some("artist-1")
        );
        assert!(
            db::metadata::layers::get_for_entity(&db, matched)?
                .iter()
                .any(|layer| layer.source_id == "musicbrainz")
        );
        assert_eq!(
            musicbrainz_id(&db, unknown, "artist_id")?.as_deref(),
            Some("")
        );
        Ok(())
    }

    #[test]
    fn unmatching_a_release_blanks_its_ids() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;
        save_provider_layer(
            &mut db,
            release_id,
            "musicbrainz",
            &ProviderValues::default(),
            &HashMap::from([("release_id".to_string(), "release-1".to_string())]),
            &Default::default(),
        )?;

        mark_unmatched(
            &mut db,
            release_id,
            "musicbrainz",
            EntityType::Release,
            vec!["release_id".to_string()],
        )?;

        assert_eq!(
            musicbrainz_id(&db, release_id, "release_id")?.as_deref(),
            Some("")
        );
        Ok(())
    }
}
