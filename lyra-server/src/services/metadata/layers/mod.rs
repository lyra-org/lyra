// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub(crate) mod json;

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

use crate::services::EntityType;
use lyra_db::{
    ArtistRelationType,
    CreditType,
    IdSource,
    MetadataField,
    MetadataLayer,
    ProviderCustomFields,
    metadata::layers::{
        FieldInput,
        FieldInputs,
        FieldValue,
        LayerCredits,
        LayerRelations,
        edges::{
            LayerCreditInput,
            LayerRelationInput,
        },
        resolve_inputs,
    },
};

/// Reserved source for file-derived metadata; plugin registration rejects it.
pub(crate) const LOCAL_SOURCE_ID: &str = "local";

/// Reserved source for manual edits, ranked above every provider; plugin
/// registration rejects it.
pub(crate) const MANUAL_SOURCE_ID: &str = "manual";

/// File-derived metadata; omitted fields express no opinion, while `None`
/// clears.
#[derive(Default)]
pub(crate) struct LocalLayer {
    fields: FieldInputs,
    relations: LayerRelations,
    credits: LayerCredits,
}

impl LocalLayer {
    pub(crate) fn supply(&mut self, field: MetadataField, value: Option<impl Into<FieldValue>>) {
        if let Some(value) = value {
            self.fields
                .insert(field, Some(FieldInput::Value(value.into())));
        }
    }

    /// Supplies an input that may name genres or labels, found or created on
    /// save; `Some(None)` clears the field.
    pub(crate) fn supply_input(&mut self, field: MetadataField, input: Option<Option<FieldInput>>) {
        if let Some(input) = input {
            self.fields.insert(field, input);
        }
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
        let fields = resolve_inputs(db, self.fields)?;
        let layer = MetadataLayer::new(LOCAL_SOURCE_ID, fields, now_secs())
            .with_relations(self.relations)
            .with_credits(self.credits);
        lyra_db::metadata::layers::upsert_inside_tx(db, node_id, &layer)?;
        super::merging::apply_merged_metadata_to_entity_inside_tx(db, node_id)
    }
}

/// Checks that the entity holds every field a provider supplies.
fn ensure_supported(db: &DbAny, node_id: DbId, fields: &FieldInputs) -> anyhow::Result<()> {
    let entity_type = lyra_db::entities::metadata_entity_type(db, node_id)?
        .ok_or_else(|| anyhow::anyhow!("Entity not found: {}", node_id.0))?;
    for &field in fields.keys() {
        anyhow::ensure!(
            entity_type.supports(field),
            "a {} has no '{field}' field",
            entity_type.as_str()
        );
    }
    Ok(())
}

pub(super) fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn artist_type_conflicts_with_layer(
    artist: Option<&lyra_db::Artist>,
    fields: &FieldInputs,
    node_id: DbId,
    provider_id: &str,
) -> bool {
    let Some(existing_type) = artist.and_then(|artist| artist.artist_type) else {
        return false;
    };
    let Some(Some(FieldInput::Value(FieldValue::ArtistType(incoming_type)))) =
        fields.get(&MetadataField::ArtistType)
    else {
        return false;
    };
    if existing_type == *incoming_type {
        return false;
    }

    tracing::warn!(
        node_id = node_id.0,
        provider_id,
        existing_artist_type = %existing_type,
        incoming_artist_type = %incoming_type,
        "skipping provider artist identity update with conflicting artist_type"
    );
    true
}

/// What a provider supplies for one entity in a layer save.
#[derive(Clone, Default)]
pub(crate) struct ProviderValues {
    pub(crate) fields: FieldInputs,
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
        lyra_db::metadata::custom_fields::remove(db, node_id, provider_id, version)?;
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
        lyra_db::metadata::custom_fields::upsert(db, node_id, &row)?;
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
    ensure_supported(db, node_id, &values.fields)?;
    let artist = lyra_db::artists::get_by_id(db, node_id)?;
    let artist_type_conflict =
        artist_type_conflicts_with_layer(artist.as_ref(), &values.fields, node_id, provider_id);

    let has_content = !values.relations.is_empty()
        || !values.credits.is_empty()
        || (!artist_type_conflict && !values.fields.is_empty());
    if has_content {
        // One transaction, so a failed apply also undoes the save that an
        // identical later save would skip as unchanged.
        db.transaction_mut(|t| -> anyhow::Result<()> {
            // A conflicting artist type keeps the source's stored fields, but
            // its relations still save. Layers save while locked; only
            // application is suppressed.
            let fields = if artist_type_conflict {
                lyra_db::metadata::layers::get_for_entity(t, node_id)?
                    .into_iter()
                    .find(|stored| stored.source_id == provider_id)
                    .map(|stored| stored.fields)
                    .unwrap_or_default()
            } else {
                resolve_inputs(t, values.fields.clone())?
            };
            let layer = MetadataLayer::new(provider_id, fields, now_secs())
                .with_relations(values.relations.clone())
                .with_credits(values.credits.clone());
            if lyra_db::metadata::layers::upsert_inside_tx(t, node_id, &layer)? {
                super::merging::apply_merged_metadata_to_entity_inside_tx(t, node_id)?;
            }
            Ok(())
        })?;
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
        let existing_id = lyra_db::external_ids::get(db, node_id, provider_id, id_type)?;

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

        lyra_db::external_ids::upsert(
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
            && lyra_db::external_ids::get(db, node_id, provider_id, &id_type)?
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
        let removed_layer = lyra_db::metadata::layers::remove_inside_tx(t, node_id, provider_id)?;
        let mut removed_custom_fields = false;
        for row in lyra_db::metadata::custom_fields::get_for_entity(t, node_id)? {
            if row.provider_id == provider_id {
                removed_custom_fields |=
                    lyra_db::metadata::custom_fields::remove(t, node_id, provider_id, row.version)?;
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
    use crate::services::metadata::layers::json::inputs_from_json;
    use lyra_db::fixtures::{
        insert_artist,
        insert_release,
    };
    use lyra_db::test_db::new_test_db;

    #[test]
    fn clearing_a_provider_layer_drops_its_fields_and_custom_fields() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Local Title")?;
        lyra_db::providers::upsert(
            &mut db,
            &lyra_db::ProviderConfig {
                db_id: None,
                provider_id: "provider".to_string(),
                position: 0,
                enabled: true,
            },
        )?;
        let mut local = LocalLayer::default();
        local.supply(MetadataField::ReleaseTitle, Some("Local Title"));
        db.transaction_mut(|t| local.save(t, release_id))?;
        save_provider_layer(
            &mut db,
            release_id,
            "provider",
            &ProviderValues {
                fields: inputs_from_json(json!({
                    "release_title": "Wrong Title",
                    "release_date": "1999",
                })),
                ..Default::default()
            },
            &HashMap::new(),
            &CustomFieldChanges {
                set: HashMap::from([(1, HashMap::from([("barcode".to_string(), json!("123"))]))]),
                ..Default::default()
            },
        )?;
        let matched = lyra_db::releases::get_by_id(&db, release_id)?.expect("release exists");
        assert_eq!(matched.release_title, "Wrong Title");
        assert_eq!(matched.release_date.as_deref(), Some("1999"));

        clear_provider_layer(&mut db, release_id, "provider")?;

        let cleared = lyra_db::releases::get_by_id(&db, release_id)?.expect("release exists");
        assert_eq!(cleared.release_title, "Local Title");
        assert_eq!(cleared.release_date, None);
        assert!(
            lyra_db::metadata::layers::get_for_entity(&db, release_id)?
                .iter()
                .all(|layer| layer.source_id != "provider")
        );
        assert!(lyra_db::metadata::custom_fields::get_for_entity(&db, release_id)?.is_empty());
        Ok(())
    }

    #[test]
    fn a_failed_apply_does_not_keep_the_saved_layer() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;
        // A stored value of the wrong type makes the release unreadable, so applying fails.
        db.exec_mut(
            agdb::QueryBuilder::insert()
                .values_uniform([("release_type", 7_i64).into()])
                .ids(release_id)
                .query(),
        )?;
        let save = |db: &mut DbAny| {
            save_provider_layer(
                db,
                release_id,
                "provider",
                &ProviderValues {
                    fields: inputs_from_json(json!({"release_title": "Title"})),
                    ..Default::default()
                },
                &HashMap::new(),
                &CustomFieldChanges::default(),
            )
        };

        assert!(save(&mut db).is_err());
        assert!(lyra_db::metadata::layers::layer_id(&db, release_id, "provider")?.is_none());
        assert!(save(&mut db).is_err(), "a repeated save applies again");
        Ok(())
    }

    #[test]
    fn a_conflicting_artist_type_still_saves_relations() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let character = insert_artist(&mut db, "Character")?;
        let actor = insert_artist(&mut db, "Actor")?;
        let mut local = LocalLayer::default();
        local.supply(MetadataField::ArtistName, Some("Character"));
        local.supply(
            MetadataField::ArtistType,
            Some(lyra_db::ArtistType::Character),
        );
        db.transaction_mut(|t| local.save(t, character))?;
        lyra_db::providers::upsert(
            &mut db,
            &lyra_db::ProviderConfig {
                db_id: None,
                provider_id: "provider".to_string(),
                position: 0,
                enabled: true,
            },
        )?;

        save_provider_layer(
            &mut db,
            character,
            "provider",
            &ProviderValues {
                fields: inputs_from_json(json!({
                    "artist_name": "Renamed",
                    "artist_type": "person",
                })),
                relations: LayerRelations::from([(
                    lyra_db::ArtistRelationType::VoiceActor,
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

        let artist = lyra_db::artists::get_by_id(&db, character)?.expect("artist exists");
        assert_eq!(artist.artist_type, Some(lyra_db::ArtistType::Character));
        assert_eq!(
            artist.artist_name, "Character",
            "conflicting fields are skipped"
        );
        let relations = lyra_db::artists::relations::get_relations_to(
            &db,
            character,
            Some(lyra_db::ArtistRelationType::VoiceActor),
        )?;
        assert_eq!(relations.len(), 1);
        assert_eq!(relations[0].1, actor);
        Ok(())
    }

    fn musicbrainz_id(db: &DbAny, node_id: DbId, id_type: &str) -> anyhow::Result<Option<String>> {
        Ok(lyra_db::external_ids::get(db, node_id, "musicbrainz", id_type)?.map(|id| id.id_value))
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
                fields: inputs_from_json(json!({"artist_name": "Matched"})),
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
            lyra_db::metadata::layers::get_for_entity(&db, matched)?
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
