// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
    hash_map::Entry,
};

use agdb::{
    DbAny,
    DbAnyTransactionMut,
    DbId,
};
use serde::de::DeserializeOwned;

use crate::db::{
    self,
    Artist,
    ArtistRelationType,
    CreditType,
    DbAccess,
    MetadataField,
    MetadataLayer,
    ProviderConfig,
    Release,
    Track,
    labels::LabelLinkInput,
    metadata::layers::{
        decode,
        edges::{
            self as layer_edges,
            LayerCreditInput,
            LayerRelationInput,
        },
        normalize_value,
    },
};
use crate::services::metadata::layers::LOCAL_SOURCE_ID;

#[derive(Debug, Clone)]
pub(crate) struct MergedMetadata {
    pub(crate) fields: HashMap<MetadataField, serde_json::Value>,
    pub(crate) provenance: HashMap<MetadataField, String>,
    /// Each relation type's whole set, from the highest-priority layer supplying it.
    pub(crate) relations: HashMap<ArtistRelationType, Vec<LayerRelationInput>>,
    /// Each credit role's whole ordered list, from the highest-priority layer supplying it.
    pub(crate) credits: HashMap<CreditType, Vec<LayerCreditInput>>,
    manual_fields: HashSet<MetadataField>,
}

impl MergedMetadata {
    /// The resolved value, or `None` when no source supplies one or it was cleared.
    fn value<T: DeserializeOwned>(&self, field: MetadataField) -> anyhow::Result<Option<T>> {
        match self.fields.get(&field) {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(value) => decode(field, value).map(Some),
        }
    }

    /// The layer whose whole list wins a graph field, or `None` when no layer
    /// supplies it. A layer that clears the field links nothing.
    fn winning_layer(
        &self,
        db: &impl DbAccess,
        node_id: DbId,
        field: MetadataField,
    ) -> anyhow::Result<Option<DbId>> {
        match self.provenance.get(&field) {
            Some(source_id) => db::metadata::layers::layer_id(db, node_id, source_id),
            None => Ok(None),
        }
    }
}

fn assign<T: PartialEq>(target: &mut Option<T>, next: Option<T>) -> bool {
    if *target == next {
        return false;
    }
    *target = next;
    true
}

fn overlay_manual_metadata(
    db: &impl DbAccess,
    node_id: DbId,
    mut merged: MergedMetadata,
) -> anyhow::Result<MergedMetadata> {
    let Some(manual) = db::metadata::manual_overrides::get(db, node_id)? else {
        return Ok(merged);
    };
    for (field, value) in manual.parsed_fields()? {
        if field.is_graph() {
            merged.fields.remove(&field);
        } else {
            merged.fields.insert(field, normalize_value(field, value)?);
        }
        merged.provenance.remove(&field);
        merged.manual_fields.insert(field);
    }
    Ok(merged)
}

pub(crate) fn merge_layers(
    layers: Vec<MetadataLayer>,
    providers: &[ProviderConfig],
) -> MergedMetadata {
    let mut priority_map: HashMap<&str, u32> = HashMap::new();
    for provider in providers.iter().filter(|provider| provider.enabled) {
        priority_map.insert(&provider.provider_id, provider.priority);
    }

    // None sorts local below even a priority-0 provider.
    let layer_priority = |layer: &MetadataLayer| -> Option<u32> {
        if layer.source_id == LOCAL_SOURCE_ID {
            None
        } else {
            priority_map.get(layer.source_id.as_str()).copied()
        }
    };

    let mut sorted_layers: Vec<MetadataLayer> = layers
        .into_iter()
        .filter(|layer| {
            layer.source_id == LOCAL_SOURCE_ID
                || priority_map.contains_key(layer.source_id.as_str())
        })
        .collect();
    sorted_layers.sort_by(|a, b| {
        layer_priority(b)
            .cmp(&layer_priority(a))
            .then_with(|| b.updated_at.cmp(&a.updated_at))
            .then_with(|| a.source_id.cmp(&b.source_id))
    });

    let mut merged_fields = HashMap::new();
    let mut provenance = HashMap::new();
    let mut relations = HashMap::new();
    let mut credits = HashMap::new();

    for layer in sorted_layers {
        for (field, value) in layer.fields {
            // The highest-priority layer mentioning a field wins, including explicit null.
            if let Entry::Vacant(entry) = merged_fields.entry(field) {
                entry.insert(value);
                provenance.insert(field, layer.source_id.clone());
            }
        }
        for (relation_type, inputs) in layer.relations {
            relations.entry(relation_type).or_insert(inputs);
        }
        for (credit_type, inputs) in layer.credits {
            credits.entry(credit_type).or_insert(inputs);
        }
    }

    MergedMetadata {
        fields: merged_fields,
        provenance,
        relations,
        credits,
        manual_fields: HashSet::new(),
    }
}

pub(crate) fn apply_merged_metadata_to_entity(db: &mut DbAny, node_id: DbId) -> anyhow::Result<()> {
    db.transaction_mut(|transaction| {
        apply_merged_metadata_to_entity_inside_tx(transaction, node_id)
    })
}

/// Applies the complete resolved snapshot; locks preserve stored values.
pub(crate) fn apply_merged_metadata_to_entity_inside_tx(
    db: &mut DbAnyTransactionMut<'_>,
    node_id: DbId,
) -> anyhow::Result<()> {
    let layers = db::metadata::layers::get_for_entity(db, node_id)?;
    let providers = db::providers::get(db)?;
    let merged = overlay_manual_metadata(db, node_id, merge_layers(layers, &providers))?;

    if let Some(mut release) = db::releases::get_by_id(db, node_id)? {
        if release.locked.unwrap_or(false) {
            return Ok(());
        }
        if apply_to_release(&mut release, &merged)? {
            db::releases::update_in_transaction(db, &release)?;
        }
        // Manually owned graph fields keep their stored edges.
        if !merged.manual_fields.contains(&MetadataField::Genres) {
            let genre_ids: Vec<DbId> =
                match merged.winning_layer(db, node_id, MetadataField::Genres)? {
                    Some(layer_id) => layer_edges::genres(db, layer_id)?
                        .into_iter()
                        .map(|link| link.target_id)
                        .collect(),
                    None => Vec::new(),
                };
            db::genres::sync_release_genre_ids(db, node_id, &genre_ids)?;
        }
        if !merged.manual_fields.contains(&MetadataField::Labels) {
            let label_links: Vec<LabelLinkInput> =
                match merged.winning_layer(db, node_id, MetadataField::Labels)? {
                    Some(layer_id) => layer_edges::labels(db, layer_id)?
                        .into_iter()
                        .map(|link| LabelLinkInput {
                            label_id: link.target_id,
                            catalog_number: link.edge.catalog_number,
                        })
                        .collect(),
                    None => Vec::new(),
                };
            db::labels::sync_release_label_links_inside_tx(db, node_id, &label_links)?;
        }
        sync_credits(db, node_id, &merged)?;
    } else if let Some(mut track) = db::tracks::get_by_id(db, node_id)? {
        if track.locked.unwrap_or(false) {
            return Ok(());
        }
        if apply_to_track(&mut track, &merged)? {
            db::tracks::update_in_transaction(db, &track)?;
        }
        sync_credits(db, node_id, &merged)?;
    } else if let Some(mut artist) = db::artists::get_by_id(db, node_id)? {
        if artist.locked.unwrap_or(false) {
            return Ok(());
        }
        if apply_to_artist(&mut artist, &merged)? {
            db::artists::update_in_transaction(db, &artist)?;
        }
        sync_incoming_relations(db, node_id, &merged)?;
    }

    Ok(())
}

/// Rewrites a release's or track's credits from its resolved layers, each role
/// in its winning layer's order. Manually owned credits stay as stored.
fn sync_credits(
    db: &mut DbAnyTransactionMut<'_>,
    owner_id: DbId,
    merged: &MergedMetadata,
) -> anyhow::Result<()> {
    if db::metadata::manual_overrides::owns_field(db, owner_id, MetadataField::Credits)? {
        return Ok(());
    }
    let desired: Vec<db::credits::CreditLinkInput> = resolved_credits(merged)
        .into_iter()
        .map(|(credit_type, input)| db::credits::CreditLinkInput {
            artist_id: input.artist_id,
            credit_type,
            detail: input.detail,
        })
        .collect();
    let mut stored = db::credits::links_for_owner(db, owner_id)?;
    stored.sort_by_key(|link| link.credit.artist_order);
    let unchanged = stored
        .iter()
        .map(|link| {
            (
                link.artist_id,
                link.credit.credit_type,
                link.credit.detail.as_deref(),
            )
        })
        .eq(desired
            .iter()
            .map(|input| (input.artist_id, input.credit_type, input.detail.as_deref())));
    if !unchanged {
        db::credits::replace_for_owner(db, owner_id, &desired)?;
    }
    Ok(())
}

/// The resolved credits, roles in their canonical order.
pub(crate) fn resolved_credits(merged: &MergedMetadata) -> Vec<(CreditType, LayerCreditInput)> {
    CreditType::ALL
        .into_iter()
        .flat_map(|credit_type| {
            merged
                .credits
                .get(&credit_type)
                .into_iter()
                .flatten()
                .map(move |input| (credit_type, input.clone()))
        })
        .collect()
}

/// The relations from `artist_id` that its targets' resolved layers supply:
/// each target's winning set for a type that lists this artist.
pub(crate) fn resolved_relations_from(
    db: &impl DbAccess,
    artist_id: DbId,
) -> anyhow::Result<Vec<(DbId, ArtistRelationType, Option<String>)>> {
    let providers = db::providers::get(db)?;
    let mut targets = HashSet::new();
    for link in layer_edges::relations_naming(db, artist_id)? {
        targets.extend(db::metadata::layers::owner_id(db, link.layer_id)?);
    }
    let mut relations = Vec::new();
    for target_id in targets {
        let merged = merge_layers(
            db::metadata::layers::get_for_entity(db, target_id)?,
            &providers,
        );
        for (relation_type, inputs) in merged.relations {
            relations.extend(
                inputs
                    .into_iter()
                    .filter(|input| input.artist_id == artist_id)
                    .map(|input| (target_id, relation_type, input.attributes)),
            );
        }
    }
    Ok(relations)
}

/// Rewrites the relations into an artist from its resolved layers, one whole set
/// per type. Relations from an artist whose relations are manually owned keep
/// what is stored: manual edits own an artist's outgoing relations.
fn sync_incoming_relations(
    db: &mut DbAnyTransactionMut<'_>,
    artist_id: DbId,
    merged: &MergedMetadata,
) -> anyhow::Result<()> {
    let mut manually_owned = HashMap::new();
    let mut owned = |db: &DbAnyTransactionMut<'_>, source_id: DbId| -> anyhow::Result<bool> {
        if let Some(&owned) = manually_owned.get(&source_id) {
            return Ok(owned);
        }
        let owned =
            db::metadata::manual_overrides::owns_field(db, source_id, MetadataField::Relations)?;
        manually_owned.insert(source_id, owned);
        Ok(owned)
    };
    for relation_type in ArtistRelationType::ALL {
        let desired: HashMap<DbId, Option<String>> = merged
            .relations
            .get(&relation_type)
            .into_iter()
            .flatten()
            .map(|input| (input.artist_id, input.attributes.clone()))
            .collect();
        for (relation, source_id) in
            db::artists::relations::get_relations_to(db, artist_id, Some(relation_type))?
        {
            let wanted = desired.get(&source_id);
            if wanted == Some(&relation.attributes) || owned(db, source_id)? {
                continue;
            }
            if wanted.is_none() {
                db::artists::relations::unlink(db, source_id, artist_id, relation_type)?;
            }
        }
        for (source_id, attributes) in desired {
            if !owned(db, source_id)?
                && !db::artists::relations::get_relations_from(db, source_id, Some(relation_type))?
                    .iter()
                    .any(|(relation, target)| {
                        *target == artist_id && relation.attributes == attributes
                    })
            {
                db::artists::relations::link(db, source_id, artist_id, relation_type, attributes)?;
            }
        }
    }
    Ok(())
}

fn apply_to_release(release: &mut Release, merged: &MergedMetadata) -> anyhow::Result<bool> {
    let mut changed = false;

    // Required titles keep their stored value when unresolved.
    if let Some(title) = merged.value::<String>(MetadataField::ReleaseTitle)?
        && release.release_title != title
    {
        release.set_release_title(title);
        changed = true;
    }
    changed |= assign(
        &mut release.sort_title,
        merged.value(MetadataField::SortTitle)?,
    );
    changed |= assign(
        &mut release.release_type,
        merged.value(MetadataField::ReleaseType)?,
    );
    changed |= assign(
        &mut release.release_date,
        merged.value(MetadataField::ReleaseDate)?,
    );

    Ok(changed)
}

fn apply_to_track(track: &mut Track, merged: &MergedMetadata) -> anyhow::Result<bool> {
    let mut changed = false;

    if let Some(title) = merged.value::<String>(MetadataField::TrackTitle)?
        && track.track_title != title
    {
        track.set_track_title(title);
        changed = true;
    }
    changed |= assign(
        &mut track.sort_title,
        merged.value(MetadataField::SortTitle)?,
    );
    for (field, target) in [
        (MetadataField::Year, &mut track.year),
        (MetadataField::Disc, &mut track.disc),
        (MetadataField::DiscTotal, &mut track.disc_total),
        (MetadataField::Track, &mut track.track),
        (MetadataField::TrackTotal, &mut track.track_total),
    ] {
        changed |= assign(target, merged.value(field)?);
    }

    Ok(changed)
}

fn apply_to_artist(artist: &mut Artist, merged: &MergedMetadata) -> anyhow::Result<bool> {
    let mut changed = false;

    if let Some(name) = merged.value::<String>(MetadataField::ArtistName)?
        && artist.artist_name != name
    {
        artist.set_artist_name(name);
        changed = true;
    }
    changed |= assign(
        &mut artist.artist_type,
        merged.value(MetadataField::ArtistType)?,
    );
    changed |= assign(
        &mut artist.sort_name,
        merged.value(MetadataField::SortName)?,
    );
    changed |= assign(
        &mut artist.description,
        merged.value(MetadataField::Description)?,
    );

    Ok(changed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_layer(source_id: &str, fields_json: &str, updated_at: u64) -> MetadataLayer {
        MetadataLayer::new(
            source_id,
            serde_json::from_str(fields_json).expect("test layer fields parse"),
            updated_at,
        )
    }

    fn make_provider(provider_id: &str, priority: u32, enabled: bool) -> ProviderConfig {
        ProviderConfig {
            db_id: None,
            provider_id: provider_id.to_string(),
            priority,
            enabled,
        }
    }

    #[test]
    fn merge_layers_higher_priority_provider_wins() {
        let layers = vec![
            make_layer("low", r#"{"track_title": "Low Priority Title"}"#, 1000),
            make_layer("high", r#"{"track_title": "High Priority Title"}"#, 1000),
        ];
        let providers = vec![
            make_provider("low", 10, true),
            make_provider("high", 100, true),
        ];

        let merged = merge_layers(layers, &providers);

        assert_eq!(
            merged
                .fields
                .get(&MetadataField::TrackTitle)
                .and_then(|v| v.as_str()),
            Some("High Priority Title")
        );
        assert_eq!(
            merged
                .provenance
                .get(&MetadataField::TrackTitle)
                .map(String::as_str),
            Some("high")
        );
    }

    #[test]
    fn merge_layers_null_clears_while_an_absent_key_falls_through() {
        let layers = vec![
            make_layer("high", r#"{"sort_title": null}"#, 1000),
            make_layer(
                "low",
                r#"{"sort_title": "Low Sort", "release_date": "1999"}"#,
                1000,
            ),
        ];
        let providers = vec![
            make_provider("high", 100, true),
            make_provider("low", 10, true),
        ];

        let merged = merge_layers(layers, &providers);

        assert_eq!(
            merged.fields.get(&MetadataField::SortTitle),
            Some(&serde_json::Value::Null)
        );
        assert_eq!(
            merged
                .provenance
                .get(&MetadataField::SortTitle)
                .map(String::as_str),
            Some("high")
        );
        assert_eq!(
            merged
                .fields
                .get(&MetadataField::ReleaseDate)
                .and_then(|v| v.as_str()),
            Some("1999")
        );
        assert_eq!(
            merged
                .provenance
                .get(&MetadataField::ReleaseDate)
                .map(String::as_str),
            Some("low")
        );
    }

    #[test]
    fn apply_to_release_clears_a_field_no_layer_supplies() -> anyhow::Result<()> {
        let mut release = Release {
            db_id: None,
            id: "release".to_string(),
            release_title: "Stored Title".to_string(),
            sort_title: Some("Stored Sort".to_string()),
            release_type: Some(db::releases::ReleaseType::Album),
            release_date: Some("1999".to_string()),
            media_formats: None,
            barcode: None,
            locked: None,
            created_at: None,
            ctime: None,
        };
        let merged = merge_layers(
            vec![make_layer("provider", r#"{"release_title": "Layer"}"#, 1)],
            &[make_provider("provider", 10, true)],
        );

        assert!(apply_to_release(&mut release, &merged)?);

        assert_eq!(release.release_title, "Layer");
        assert!(release.sort_title.is_none());
        assert!(release.release_type.is_none());
        assert!(release.release_date.is_none());
        Ok(())
    }

    #[test]
    fn apply_to_release_keeps_the_title_when_no_layer_supplies_one() -> anyhow::Result<()> {
        let mut release = Release {
            db_id: None,
            id: "release".to_string(),
            release_title: "Stored Title".to_string(),
            sort_title: None,
            release_type: None,
            release_date: None,
            media_formats: None,
            barcode: None,
            locked: None,
            created_at: None,
            ctime: None,
        };
        let merged = merge_layers(Vec::new(), &[]);

        assert!(!apply_to_release(&mut release, &merged)?);
        assert_eq!(release.release_title, "Stored Title");
        Ok(())
    }

    #[test]
    fn merge_layers_keeps_local_source_without_a_provider() {
        let layers = vec![make_layer(
            LOCAL_SOURCE_ID,
            r#"{"track_title": "Tagged Title"}"#,
            1000,
        )];

        let merged = merge_layers(layers, &[]);

        assert_eq!(
            merged
                .fields
                .get(&MetadataField::TrackTitle)
                .and_then(|v| v.as_str()),
            Some("Tagged Title")
        );
        assert_eq!(
            merged
                .provenance
                .get(&MetadataField::TrackTitle)
                .map(String::as_str),
            Some(LOCAL_SOURCE_ID)
        );
    }

    #[test]
    fn merge_layers_ranks_local_source_below_every_enabled_provider() {
        let layers = vec![
            make_layer(LOCAL_SOURCE_ID, r#"{"track_title": "Tagged Title"}"#, 2000),
            make_layer("provider", r#"{"track_title": "Provider Title"}"#, 1000),
        ];
        let providers = vec![make_provider("provider", 0, true)];

        let merged = merge_layers(layers, &providers);

        assert_eq!(
            merged
                .fields
                .get(&MetadataField::TrackTitle)
                .and_then(|v| v.as_str()),
            Some("Provider Title")
        );
        assert_eq!(
            merged
                .provenance
                .get(&MetadataField::TrackTitle)
                .map(String::as_str),
            Some("provider")
        );
    }

    #[test]
    fn merge_layers_disabled_provider_is_excluded() {
        let layers = vec![
            make_layer("active", r#"{"track_title": "Active Title"}"#, 1000),
            make_layer("disabled", r#"{"track_title": "Disabled Title"}"#, 1000),
        ];
        let providers = vec![
            make_provider("active", 10, true),
            make_provider("disabled", 100, false),
        ];

        let merged = merge_layers(layers, &providers);

        assert_eq!(
            merged
                .fields
                .get(&MetadataField::TrackTitle)
                .and_then(|v| v.as_str()),
            Some("Active Title")
        );
        assert_eq!(
            merged
                .provenance
                .get(&MetadataField::TrackTitle)
                .map(String::as_str),
            Some("active")
        );
    }
    mod snapshot {
        use std::collections::BTreeMap;

        use agdb::DbAny;

        use super::*;
        use crate::db::test_db::{
            insert_artist,
            insert_release,
            insert_track,
            new_test_db,
            stored_keys,
        };

        fn enable_provider(db: &mut DbAny, provider_id: &str, priority: u32) -> anyhow::Result<()> {
            db::providers::upsert(db, &make_provider(provider_id, priority, true))?;
            Ok(())
        }

        fn save_layer(
            db: &mut DbAny,
            node_id: DbId,
            source_id: &str,
            fields: serde_json::Value,
            updated_at: u64,
        ) -> anyhow::Result<()> {
            db::metadata::layers::upsert(
                db,
                node_id,
                &make_layer(source_id, &fields.to_string(), updated_at),
            )?;
            Ok(())
        }

        fn provider_save(
            db: &mut DbAny,
            node_id: DbId,
            provider_id: &str,
            fields: serde_json::Value,
        ) -> anyhow::Result<()> {
            crate::services::metadata::layers::save_provider_layer(
                db,
                node_id,
                provider_id,
                &crate::services::metadata::layers::ProviderValues {
                    fields: db::metadata::layers::fields_from_json(fields),
                    ..Default::default()
                },
                &HashMap::new(),
                &Default::default(),
            )
        }

        fn genre_names(db: &DbAny, release_id: DbId) -> anyhow::Result<Vec<String>> {
            Ok(db::genres::get_for_release(db, release_id)?
                .into_iter()
                .map(|genre| genre.name)
                .collect())
        }

        fn label_names(db: &DbAny, release_id: DbId) -> anyhow::Result<Vec<String>> {
            Ok(db::labels::get_for_release(db, release_id)?
                .into_iter()
                .map(|label| label.label.name)
                .collect())
        }

        fn listed_genres(db: &DbAny) -> anyhow::Result<Vec<String>> {
            use crate::services::catalog::{
                Viewer,
                genres::{
                    GenreFilter,
                    Genres,
                },
                pipeline::Catalog,
            };
            let ids = Genres::candidates(db, &Viewer::System, &GenreFilter::default())
                .map_err(|error| anyhow::anyhow!("{error:?}"))?;
            let mut names: Vec<String> = ids
                .into_iter()
                .filter_map(|id| db::genres::get_by_id(db, id).transpose())
                .map(|genre| genre.map(|genre| genre.name))
                .collect::<anyhow::Result<_>>()?;
            names.sort();
            Ok(names)
        }

        fn save_relations(
            db: &mut DbAny,
            artist_id: DbId,
            provider_id: &str,
            relations: &[(ArtistRelationType, &[DbId])],
        ) -> anyhow::Result<()> {
            crate::services::metadata::layers::save_provider_layer(
                db,
                artist_id,
                provider_id,
                &crate::services::metadata::layers::ProviderValues {
                    fields: db::metadata::layers::fields_from_json(serde_json::json!({})),
                    relations: relations
                        .iter()
                        .map(|(relation_type, artist_ids)| {
                            (
                                *relation_type,
                                artist_ids
                                    .iter()
                                    .map(|&artist_id| LayerRelationInput {
                                        artist_id,
                                        attributes: None,
                                    })
                                    .collect(),
                            )
                        })
                        .collect(),
                    ..Default::default()
                },
                &HashMap::new(),
                &Default::default(),
            )
        }

        fn related_into(
            db: &DbAny,
            artist_id: DbId,
            relation_type: ArtistRelationType,
        ) -> anyhow::Result<Vec<DbId>> {
            let mut sources: Vec<DbId> =
                db::artists::relations::get_relations_to(db, artist_id, Some(relation_type))?
                    .into_iter()
                    .map(|(_, source_id)| source_id)
                    .collect();
            sources.sort();
            Ok(sources)
        }

        #[test]
        fn each_relation_type_follows_the_layer_that_supplies_it() -> anyhow::Result<()> {
            let mut db = new_test_db()?;
            let character = insert_artist(&mut db, "Character")?;
            let low_actor = insert_artist(&mut db, "Low Actor")?;
            let high_actor = insert_artist(&mut db, "High Actor")?;
            let member = insert_artist(&mut db, "Member")?;
            enable_provider(&mut db, "low", 10)?;
            enable_provider(&mut db, "high", 100)?;
            save_relations(
                &mut db,
                character,
                "low",
                &[
                    (ArtistRelationType::VoiceActor, &[low_actor]),
                    (ArtistRelationType::MemberOf, &[member]),
                ],
            )?;
            save_relations(
                &mut db,
                character,
                "high",
                &[(ArtistRelationType::VoiceActor, &[high_actor])],
            )?;
            assert_eq!(
                related_into(&db, character, ArtistRelationType::VoiceActor)?,
                vec![high_actor]
            );
            assert_eq!(
                related_into(&db, character, ArtistRelationType::MemberOf)?,
                vec![member]
            );

            provider_save(
                &mut db,
                character,
                "high",
                serde_json::json!({"artist_name": "Character"}),
            )?;
            assert_eq!(
                related_into(&db, character, ArtistRelationType::VoiceActor)?,
                vec![high_actor],
                "a save that does not set a type keeps it"
            );

            crate::services::metadata::layers::clear_provider_layer(&mut db, character, "high")?;
            assert_eq!(
                related_into(&db, character, ArtistRelationType::VoiceActor)?,
                vec![low_actor],
                "removing a layer falls back to the next one"
            );

            save_relations(
                &mut db,
                character,
                "low",
                &[(ArtistRelationType::VoiceActor, &[])],
            )?;
            assert!(related_into(&db, character, ArtistRelationType::VoiceActor)?.is_empty());
            assert_eq!(
                related_into(&db, character, ArtistRelationType::MemberOf)?,
                vec![member]
            );

            crate::services::metadata::layers::clear_provider_layer(&mut db, character, "low")?;
            assert!(
                related_into(&db, character, ArtistRelationType::MemberOf)?.is_empty(),
                "with no layer left, the relations go"
            );
            Ok(())
        }

        #[test]
        fn a_locked_artist_keeps_its_relations_until_unlocked() -> anyhow::Result<()> {
            let mut db = new_test_db()?;
            let character = insert_artist(&mut db, "Character")?;
            let actor = insert_artist(&mut db, "Actor")?;
            let mut locked = db::artists::get_by_id(&db, character)?.expect("artist exists");
            locked.locked = Some(true);
            db::artists::update(&mut db, &locked)?;
            enable_provider(&mut db, "provider", 10)?;

            save_relations(
                &mut db,
                character,
                "provider",
                &[(ArtistRelationType::VoiceActor, &[actor])],
            )?;
            assert!(related_into(&db, character, ArtistRelationType::VoiceActor)?.is_empty());
            assert_eq!(
                db::metadata::layers::get_for_entity(&db, character)?.len(),
                1
            );

            let mut unlocked = db::artists::get_by_id(&db, character)?.expect("artist exists");
            unlocked.locked = None;
            db::artists::update(&mut db, &unlocked)?;
            apply_merged_metadata_to_entity(&mut db, character)?;
            assert_eq!(
                related_into(&db, character, ArtistRelationType::VoiceActor)?,
                vec![actor]
            );
            Ok(())
        }

        #[test]
        fn relations_from_a_manually_owned_artist_stay_as_stored() -> anyhow::Result<()> {
            let mut db = new_test_db()?;
            let character = insert_artist(&mut db, "Character")?;
            let manual_actor = insert_artist(&mut db, "Manual Actor")?;
            let unwanted_actor = insert_artist(&mut db, "Unwanted Actor")?;
            let provider_actor = insert_artist(&mut db, "Provider Actor")?;
            db::artists::relations::link(
                &mut db,
                manual_actor,
                character,
                ArtistRelationType::VoiceActor,
                Some("manual".to_string()),
            )?;
            for actor in [manual_actor, unwanted_actor] {
                db::metadata::manual_overrides::replace(
                    &mut db,
                    actor,
                    &BTreeMap::from([(MetadataField::Relations, serde_json::Value::Bool(true))]),
                )?;
            }
            enable_provider(&mut db, "provider", 10)?;

            save_relations(
                &mut db,
                character,
                "provider",
                &[(
                    ArtistRelationType::VoiceActor,
                    &[unwanted_actor, provider_actor],
                )],
            )?;

            assert_eq!(
                related_into(&db, character, ArtistRelationType::VoiceActor)?,
                {
                    let mut expected = vec![manual_actor, provider_actor];
                    expected.sort();
                    expected
                },
                "a manually owned artist's relations are neither removed nor added"
            );
            let kept = db::artists::relations::get_relations_to(
                &db,
                character,
                Some(ArtistRelationType::VoiceActor),
            )?;
            assert!(kept.iter().any(|(relation, source)| *source == manual_actor
                && relation.attributes.as_deref() == Some("manual")));
            Ok(())
        }

        fn credits_of(db: &DbAny, owner: DbId) -> anyhow::Result<Vec<(db::CreditType, DbId)>> {
            let mut links = db::credits::links_for_owner(db, owner)?;
            links.sort_by_key(|link| link.credit.artist_order);
            Ok(links
                .into_iter()
                .map(|link| (link.credit.credit_type, link.artist_id))
                .collect())
        }

        fn credit_list(artist_ids: &[DbId]) -> Vec<LayerCreditInput> {
            artist_ids
                .iter()
                .map(|&artist_id| LayerCreditInput {
                    artist_id,
                    detail: None,
                })
                .collect()
        }

        fn save_credits(
            db: &mut DbAny,
            owner: DbId,
            provider_id: &str,
            credits: &[(db::CreditType, &[DbId])],
        ) -> anyhow::Result<()> {
            crate::services::metadata::layers::save_provider_layer(
                db,
                owner,
                provider_id,
                &crate::services::metadata::layers::ProviderValues {
                    credits: credits
                        .iter()
                        .map(|(role, artist_ids)| (*role, credit_list(artist_ids)))
                        .collect(),
                    ..Default::default()
                },
                &HashMap::new(),
                &Default::default(),
            )
        }

        #[test]
        fn each_credit_role_follows_the_layer_that_supplies_it() -> anyhow::Result<()> {
            let mut db = new_test_db()?;
            let track = insert_track(&mut db, "Track")?;
            let local_artist = insert_artist(&mut db, "Local Artist")?;
            let composer = insert_artist(&mut db, "Composer")?;
            let [first, second] = [
                insert_artist(&mut db, "First")?,
                insert_artist(&mut db, "Second")?,
            ];
            db::metadata::layers::upsert(
                &mut db,
                track,
                &MetadataLayer::new(LOCAL_SOURCE_ID, Default::default(), 1).with_credits(
                    BTreeMap::from([
                        (db::CreditType::Artist, credit_list(&[local_artist])),
                        (db::CreditType::Composer, credit_list(&[composer])),
                    ]),
                ),
            )?;
            enable_provider(&mut db, "provider", 10)?;
            save_credits(
                &mut db,
                track,
                "provider",
                &[(db::CreditType::Artist, &[second, first])],
            )?;
            assert_eq!(
                credits_of(&db, track)?,
                vec![
                    (db::CreditType::Artist, second),
                    (db::CreditType::Artist, first),
                    (db::CreditType::Composer, composer),
                ],
                "the provider wins the artist role; the local composer stays"
            );

            save_credits(
                &mut db,
                track,
                "provider",
                &[(db::CreditType::Composer, &[])],
            )?;
            assert_eq!(
                credits_of(&db, track)?,
                vec![
                    (db::CreditType::Artist, second),
                    (db::CreditType::Artist, first)
                ],
                "an empty set clears its role and leaves the others"
            );

            crate::services::metadata::layers::clear_provider_layer(&mut db, track, "provider")?;
            assert_eq!(
                credits_of(&db, track)?,
                vec![
                    (db::CreditType::Artist, local_artist),
                    (db::CreditType::Composer, composer),
                ]
            );
            Ok(())
        }

        #[test]
        fn manual_and_locked_owners_keep_their_credits() -> anyhow::Result<()> {
            let mut db = new_test_db()?;
            let manual_track = insert_track(&mut db, "Manual")?;
            let locked_track = insert_track(&mut db, "Locked")?;
            let stored = insert_artist(&mut db, "Stored")?;
            let provider_artist = insert_artist(&mut db, "Provider")?;
            for track in [manual_track, locked_track] {
                db::credits::replace_for_owner(
                    &mut db,
                    track,
                    &[db::credits::CreditLinkInput {
                        artist_id: stored,
                        credit_type: db::CreditType::Artist,
                        detail: None,
                    }],
                )?;
            }
            db::metadata::manual_overrides::replace(
                &mut db,
                manual_track,
                &BTreeMap::from([(MetadataField::Credits, serde_json::Value::Bool(true))]),
            )?;
            let mut locked = db::tracks::get_by_id(&db, locked_track)?.expect("track exists");
            locked.locked = Some(true);
            db::tracks::update(&mut db, &locked)?;
            enable_provider(&mut db, "provider", 10)?;

            for track in [manual_track, locked_track] {
                save_credits(
                    &mut db,
                    track,
                    "provider",
                    &[(db::CreditType::Artist, &[provider_artist])],
                )?;
                assert_eq!(
                    credits_of(&db, track)?,
                    vec![(db::CreditType::Artist, stored)]
                );
            }

            let mut unlocked = db::tracks::get_by_id(&db, locked_track)?.expect("track exists");
            unlocked.locked = None;
            db::tracks::update(&mut db, &unlocked)?;
            apply_merged_metadata_to_entity(&mut db, locked_track)?;
            assert_eq!(
                credits_of(&db, locked_track)?,
                vec![(db::CreditType::Artist, provider_artist)]
            );
            Ok(())
        }

        #[test]
        fn credits_no_layer_supplies_go_on_the_next_apply() -> anyhow::Result<()> {
            let mut db = new_test_db()?;
            let track = insert_track(&mut db, "Track")?;
            let artist = insert_artist(&mut db, "Artist")?;
            db::credits::replace_for_owner(
                &mut db,
                track,
                &[db::credits::CreditLinkInput {
                    artist_id: artist,
                    credit_type: db::CreditType::Artist,
                    detail: None,
                }],
            )?;

            apply_merged_metadata_to_entity(&mut db, track)?;

            assert!(credits_of(&db, track)?.is_empty());
            Ok(())
        }

        #[test]
        fn genres_that_no_applied_layer_wins_stay_out_of_genre_listings() -> anyhow::Result<()> {
            let mut db = new_test_db()?;
            let release_id = insert_release(&mut db, "Release")?;
            let locked_id = insert_release(&mut db, "Locked")?;
            let mut locked = db::releases::get_by_id(&db, locked_id)?.expect("release exists");
            locked.locked = Some(true);
            db::releases::update(&mut db, &locked)?;
            enable_provider(&mut db, "low", 10)?;
            enable_provider(&mut db, "high", 100)?;

            provider_save(
                &mut db,
                release_id,
                "low",
                serde_json::json!({"genres": ["Losing"]}),
            )?;
            provider_save(
                &mut db,
                release_id,
                "high",
                serde_json::json!({"genres": ["Winning"]}),
            )?;
            provider_save(
                &mut db,
                locked_id,
                "high",
                serde_json::json!({"genres": ["Locked"]}),
            )?;

            assert!(db::genres::find_by_name(&db, "Losing")?.is_some());
            assert!(db::genres::find_by_name(&db, "Locked")?.is_some());
            assert_eq!(genre_names(&db, release_id)?, vec!["Winning"]);
            assert!(
                genre_names(&db, locked_id)?.is_empty(),
                "a locked release is not re-applied"
            );
            assert_eq!(
                db::metadata::layers::get_for_entity(&db, locked_id)?.len(),
                1,
                "a locked release still records its layer"
            );
            assert_eq!(listed_genres(&db)?, vec!["Winning"]);
            Ok(())
        }

        #[test]
        fn genres_and_labels_follow_the_layer_that_supplies_them() -> anyhow::Result<()> {
            let mut db = new_test_db()?;
            let release_id = insert_release(&mut db, "Release")?;
            enable_provider(&mut db, "low", 10)?;
            enable_provider(&mut db, "high", 100)?;
            provider_save(
                &mut db,
                release_id,
                "low",
                serde_json::json!({"genres": ["Low Genre"], "labels": [{"name": "Low Label"}]}),
            )?;
            provider_save(
                &mut db,
                release_id,
                "high",
                serde_json::json!({"genres": ["High Genre"], "labels": [{"name": "High Label"}]}),
            )?;
            assert_eq!(genre_names(&db, release_id)?, vec!["High Genre"]);
            assert_eq!(label_names(&db, release_id)?, vec!["High Label"]);

            provider_save(
                &mut db,
                release_id,
                "high",
                serde_json::json!({"labels": [{"name": "High Label"}]}),
            )?;
            assert_eq!(
                genre_names(&db, release_id)?,
                vec!["Low Genre"],
                "no opinion falls through"
            );
            assert_eq!(label_names(&db, release_id)?, vec!["High Label"]);

            provider_save(
                &mut db,
                release_id,
                "high",
                serde_json::json!({"genres": null, "labels": [{"name": "High Label"}]}),
            )?;
            assert!(genre_names(&db, release_id)?.is_empty(), "a clear wins");

            crate::services::metadata::layers::clear_provider_layer(&mut db, release_id, "high")?;
            assert_eq!(genre_names(&db, release_id)?, vec!["Low Genre"]);
            assert_eq!(label_names(&db, release_id)?, vec!["Low Label"]);
            assert!(
                db::labels::find_by_name(&db, "High Label")?.is_none(),
                "a label nothing names any more is collected"
            );
            Ok(())
        }

        #[test]
        fn a_property_no_layer_supplies_is_removed_from_storage() -> anyhow::Result<()> {
            let mut db = new_test_db()?;
            let track_id = insert_track(&mut db, "Track")?;
            let mut track =
                db::tracks::get_by_id(&db, track_id)?.expect("track exists after insert");
            track.year = Some(1999);
            track.disc = Some(2);
            db::tracks::update(&mut db, &track)?;
            assert!(stored_keys(&db, track_id)?.contains("year"));

            enable_provider(&mut db, "provider", 10)?;
            save_layer(
                &mut db,
                track_id,
                "provider",
                serde_json::json!({"disc": 2}),
                1,
            )?;
            apply_merged_metadata_to_entity(&mut db, track_id)?;

            let stored = stored_keys(&db, track_id)?;
            assert!(
                !stored.contains("year"),
                "year must be removed from storage, not merely left unchanged"
            );
            assert!(stored.contains("disc"));
            let track = db::tracks::get_by_id(&db, track_id)?.expect("track still exists");
            assert_eq!(track.year, None);
            assert_eq!(track.disc, Some(2));
            Ok(())
        }

        #[test]
        fn a_higher_priority_null_clears_a_lower_priority_value() -> anyhow::Result<()> {
            let mut db = new_test_db()?;
            let artist_id = insert_artist(&mut db, "Artist")?;

            enable_provider(&mut db, "low", 10)?;
            enable_provider(&mut db, "high", 100)?;
            save_layer(
                &mut db,
                artist_id,
                "low",
                serde_json::json!({"description": "Low description"}),
                1,
            )?;
            apply_merged_metadata_to_entity(&mut db, artist_id)?;
            assert_eq!(
                db::artists::get_by_id(&db, artist_id)?
                    .expect("artist exists")
                    .description
                    .as_deref(),
                Some("Low description")
            );

            save_layer(
                &mut db,
                artist_id,
                "high",
                serde_json::json!({"description": null}),
                2,
            )?;
            apply_merged_metadata_to_entity(&mut db, artist_id)?;

            assert!(
                db::artists::get_by_id(&db, artist_id)?
                    .expect("artist exists")
                    .description
                    .is_none()
            );
            assert!(!stored_keys(&db, artist_id)?.contains("description"));
            Ok(())
        }

        #[test]
        fn disabling_a_provider_restores_the_local_artist_type() -> anyhow::Result<()> {
            let mut db = new_test_db()?;
            let artist_id = insert_artist(&mut db, "Artist")?;

            save_layer(
                &mut db,
                artist_id,
                LOCAL_SOURCE_ID,
                serde_json::json!({"artist_type": "group"}),
                1,
            )?;
            enable_provider(&mut db, "provider", 10)?;
            save_layer(
                &mut db,
                artist_id,
                "provider",
                serde_json::json!({"artist_type": "person"}),
                2,
            )?;
            apply_merged_metadata_to_entity(&mut db, artist_id)?;
            assert_eq!(
                db::artists::get_by_id(&db, artist_id)?
                    .expect("artist exists")
                    .artist_type,
                Some(db::ArtistType::Person)
            );

            db::providers::upsert(&mut db, &make_provider("provider", 10, false))?;
            apply_merged_metadata_to_entity(&mut db, artist_id)?;

            assert_eq!(
                db::artists::get_by_id(&db, artist_id)?
                    .expect("artist exists")
                    .artist_type,
                Some(db::ArtistType::Group)
            );
            Ok(())
        }

        #[test]
        fn unsupplied_genres_and_labels_drop_their_relationships() -> anyhow::Result<()> {
            let mut db = new_test_db()?;
            let release_id = insert_release(&mut db, "Release")?;
            db::genres::sync_release_genres(&mut db, release_id, &["Jazz".to_string()])?;
            db::labels::sync_release_labels(
                &mut db,
                release_id,
                &[db::labels::LabelInput {
                    name: "Blue Note".to_string(),
                    catalog_number: None,
                    external_id: None,
                }],
            )?;

            enable_provider(&mut db, "provider", 10)?;
            save_layer(
                &mut db,
                release_id,
                "provider",
                serde_json::json!({"release_title": "Release"}),
                1,
            )?;
            apply_merged_metadata_to_entity(&mut db, release_id)?;

            assert!(db::genres::get_for_release(&db, release_id)?.is_empty());
            assert!(db::labels::get_for_release(&db, release_id)?.is_empty());
            Ok(())
        }

        #[test]
        fn dropping_a_manual_override_falls_back_to_the_next_layer() -> anyhow::Result<()> {
            let mut db = new_test_db()?;
            let release_id = insert_release(&mut db, "Release")?;

            enable_provider(&mut db, "provider", 10)?;
            save_layer(
                &mut db,
                release_id,
                "provider",
                serde_json::json!({
                    "sort_title": "Provider Sort",
                    "genres": ["Provider Genre"],
                }),
                1,
            )?;
            db::metadata::manual_overrides::replace(
                &mut db,
                release_id,
                &BTreeMap::from([
                    (MetadataField::SortTitle, serde_json::json!("Manual Sort")),
                    (MetadataField::Genres, serde_json::Value::Bool(true)),
                ]),
            )?;
            db::genres::sync_release_genres(&mut db, release_id, &["Manual Genre".to_string()])?;
            apply_merged_metadata_to_entity(&mut db, release_id)?;

            assert_eq!(
                db::releases::get_by_id(&db, release_id)?
                    .expect("release exists")
                    .sort_title
                    .as_deref(),
                Some("Manual Sort")
            );
            let genres = db::genres::get_for_release(&db, release_id)?;
            assert_eq!(genres.len(), 1);
            assert_eq!(genres[0].name, "Manual Genre");

            db::metadata::manual_overrides::replace(&mut db, release_id, &BTreeMap::new())?;
            apply_merged_metadata_to_entity(&mut db, release_id)?;

            assert_eq!(
                db::releases::get_by_id(&db, release_id)?
                    .expect("release exists")
                    .sort_title
                    .as_deref(),
                Some("Provider Sort")
            );
            let genres = db::genres::get_for_release(&db, release_id)?;
            assert_eq!(genres.len(), 1);
            assert_eq!(genres[0].name, "Provider Genre");
            Ok(())
        }

        #[test]
        fn a_locked_entity_records_its_layer_without_applying_it() -> anyhow::Result<()> {
            let mut db = new_test_db()?;
            let track_id = insert_track(&mut db, "Curated Title")?;
            let mut track = db::tracks::get_by_id(&db, track_id)?.expect("track exists");
            track.year = Some(1999);
            track.locked = Some(true);
            db::tracks::update(&mut db, &track)?;

            enable_provider(&mut db, "provider", 10)?;
            crate::services::metadata::layers::save_provider_layer(
                &mut db,
                track_id,
                "provider",
                &crate::services::metadata::layers::ProviderValues {
                    fields: std::collections::BTreeMap::from([(
                        MetadataField::TrackTitle,
                        serde_json::json!("Provider Title"),
                    )]),
                    ..Default::default()
                },
                &HashMap::new(),
                &Default::default(),
            )?;

            let layers = db::metadata::layers::get_for_entity(&db, track_id)?;
            assert_eq!(layers.len(), 1, "a locked entity still records its layer");
            assert_eq!(layers[0].source_id, "provider");

            let track = db::tracks::get_by_id(&db, track_id)?.expect("track exists");
            assert_eq!(track.track_title, "Curated Title");
            assert_eq!(track.year, Some(1999));

            let mut track = track;
            track.locked = None;
            db::tracks::update(&mut db, &track)?;
            apply_merged_metadata_to_entity(&mut db, track_id)?;

            let track = db::tracks::get_by_id(&db, track_id)?.expect("track exists");
            assert_eq!(track.track_title, "Provider Title");
            assert_eq!(track.year, None);
            Ok(())
        }
    }
}
