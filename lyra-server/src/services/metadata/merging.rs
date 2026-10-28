// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};

use crate::services::{
    metadata::layers::{
        LOCAL_SOURCE_ID,
        MANUAL_SOURCE_ID,
    },
    providers::{
        ActiveProviders,
        active_providers,
    },
};
use agdb::{
    DbAny,
    DbAnyTransactionMut,
    DbId,
};
use lyra_db::{
    Artist,
    ArtistRelationType,
    CreditType,
    DbAccess,
    MetadataField,
    MetadataLayer,
    Release,
    Track,
    labels::LabelLinkInput,
    metadata::layers::{
        FieldType,
        LayerFields,
        edges::{
            LayerCreditInput,
            LayerRelationInput,
        },
        field_value,
    },
};

#[derive(Debug, Clone)]
pub(crate) struct MergedMetadata {
    pub(crate) fields: LayerFields,
    /// Each relation type's whole set, from the highest-priority layer supplying it.
    pub(crate) relations: HashMap<ArtistRelationType, Vec<LayerRelationInput>>,
    /// Each credit role's whole ordered list, from the highest-priority layer supplying it.
    pub(crate) credits: HashMap<CreditType, Vec<LayerCreditInput>>,
}

/// The keys a layer supplies.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct LayerKeys {
    pub(crate) fields: HashSet<MetadataField>,
    pub(crate) credits: HashSet<CreditType>,
    pub(crate) relations: HashSet<ArtistRelationType>,
}

impl LayerKeys {
    pub(crate) fn supplied_by(layer: &MetadataLayer) -> Self {
        Self {
            fields: layer.fields.keys().copied().collect(),
            credits: layer.credits.keys().copied().collect(),
            relations: layer.relations.keys().copied().collect(),
        }
    }

    fn extend(&mut self, other: &Self) {
        self.fields.extend(&other.fields);
        self.credits.extend(&other.credits);
        self.relations.extend(&other.relations);
    }
}

/// The resolved keys an apply writes: all of them, or on a locked entity only
/// the ones the manual layer supplies, so everything else keeps its stored value.
enum Scope {
    All,
    Only(LayerKeys),
}

impl Scope {
    fn new(locked: Option<bool>, unlocked: LayerKeys) -> Self {
        if locked.unwrap_or(false) {
            Self::Only(unlocked)
        } else {
            Self::All
        }
    }

    fn field(&self, field: MetadataField) -> bool {
        match self {
            Self::All => true,
            Self::Only(keys) => keys.fields.contains(&field),
        }
    }

    fn credit(&self, credit_type: CreditType) -> bool {
        match self {
            Self::All => true,
            Self::Only(keys) => keys.credits.contains(&credit_type),
        }
    }

    fn relation(&self, relation_type: ArtistRelationType) -> bool {
        match self {
            Self::All => true,
            Self::Only(keys) => keys.relations.contains(&relation_type),
        }
    }
}

fn assign<T: PartialEq + FieldType>(
    target: &mut Option<T>,
    merged: &MergedMetadata,
    scope: &Scope,
    field: MetadataField,
) -> anyhow::Result<bool> {
    if !scope.field(field) {
        return Ok(false);
    }
    let next = field_value(&merged.fields, field)?;
    if *target == next {
        return Ok(false);
    }
    *target = next;
    Ok(true)
}

/// Layers from providers outside `providers` are dropped.
pub(crate) fn merge_layers(
    layers: Vec<MetadataLayer>,
    providers: &ActiveProviders,
) -> MergedMetadata {
    // Manual ranks above every provider, and local below the last one.
    let layer_rank = |layer: &MetadataLayer| -> (u8, u32) {
        match layer.source_id.as_str() {
            MANUAL_SOURCE_ID => (0, 0),
            LOCAL_SOURCE_ID => (2, 0),
            source_id => (1, providers.position(source_id).unwrap_or_default()),
        }
    };

    let mut sorted_layers: Vec<MetadataLayer> = layers
        .into_iter()
        .filter(|layer| {
            matches!(layer.source_id.as_str(), LOCAL_SOURCE_ID | MANUAL_SOURCE_ID)
                || providers.contains(&layer.source_id)
        })
        .collect();
    sorted_layers.sort_by(|a, b| {
        layer_rank(a)
            .cmp(&layer_rank(b))
            .then_with(|| b.updated_at.cmp(&a.updated_at))
            .then_with(|| a.source_id.cmp(&b.source_id))
    });

    let mut merged_fields = LayerFields::new();
    let mut relations = HashMap::new();
    let mut credits = HashMap::new();

    for layer in sorted_layers {
        for (field, value) in layer.fields {
            // The highest-priority layer mentioning a field wins, including explicit null.
            merged_fields.entry(field).or_insert(value);
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
        relations,
        credits,
    }
}

/// Resolves an entity's layers, leaving out `excluded_source`'s.
pub(crate) fn resolve_without(
    db: &impl DbAccess,
    node_id: DbId,
    excluded_source: &str,
) -> anyhow::Result<MergedMetadata> {
    let layers = lyra_db::metadata::layers::get_for_entity(db, node_id)?
        .into_iter()
        .filter(|layer| layer.source_id != excluded_source)
        .collect();
    Ok(merge_layers(layers, &active_providers(db)?))
}

pub(crate) fn apply_merged_metadata_to_entity(db: &mut DbAny, node_id: DbId) -> anyhow::Result<()> {
    db.transaction_mut(|transaction| {
        apply_merged_metadata_to_entity_inside_tx(transaction, node_id)
    })
}

/// Applies the complete resolved snapshot. A lock keeps every stored value
/// the manual layer does not supply.
pub(crate) fn apply_merged_metadata_to_entity_inside_tx(
    db: &mut DbAnyTransactionMut<'_>,
    node_id: DbId,
) -> anyhow::Result<()> {
    apply_through_lock(db, node_id, &LayerKeys::default())
}

/// Like [`apply_merged_metadata_to_entity_inside_tx`], but a lock also lets
/// `released` through.
pub(crate) fn apply_through_lock(
    db: &mut DbAnyTransactionMut<'_>,
    node_id: DbId,
    released: &LayerKeys,
) -> anyhow::Result<()> {
    let layers = lyra_db::metadata::layers::get_for_entity(db, node_id)?;
    let mut unlocked = layers
        .iter()
        .find(|layer| layer.source_id == MANUAL_SOURCE_ID)
        .map(LayerKeys::supplied_by)
        .unwrap_or_default();
    unlocked.extend(released);
    let providers = active_providers(db)?;
    let merged = merge_layers(layers, &providers);

    if let Some(mut release) = lyra_db::releases::get_by_id(db, node_id)? {
        let scope = Scope::new(release.locked, unlocked);
        if apply_to_release(&mut release, &merged, &scope)? {
            lyra_db::releases::update_in_transaction(db, &release)?;
        }
        if scope.field(MetadataField::Genres) {
            let genre_ids: Vec<DbId> =
                field_value(&merged.fields, MetadataField::Genres)?.unwrap_or_default();
            lyra_db::genres::sync_release_genre_ids(db, node_id, &genre_ids)?;
        }
        if scope.field(MetadataField::Labels) {
            let label_links: Vec<LabelLinkInput> =
                field_value(&merged.fields, MetadataField::Labels)?.unwrap_or_default();
            lyra_db::labels::sync_release_label_links_inside_tx(db, node_id, &label_links)?;
        }
        sync_credits(db, node_id, &merged, &scope)?;
    } else if let Some(mut track) = lyra_db::tracks::get_by_id(db, node_id)? {
        let scope = Scope::new(track.locked, unlocked);
        if apply_to_track(&mut track, &merged, &scope)? {
            lyra_db::tracks::update_in_transaction(db, &track)?;
        }
        sync_credits(db, node_id, &merged, &scope)?;
    } else if let Some(mut artist) = lyra_db::artists::get_by_id(db, node_id)? {
        let scope = Scope::new(artist.locked, unlocked);
        if apply_to_artist(&mut artist, &merged, &scope)? {
            lyra_db::artists::update_in_transaction(db, &artist)?;
        }
        sync_incoming_relations(db, node_id, &merged, &scope)?;
    }

    Ok(())
}

/// Rewrites a release's or track's credits from its resolved layers, each role
/// in its winning layer's order. Roles out of scope keep their stored credits.
fn sync_credits(
    db: &mut DbAnyTransactionMut<'_>,
    owner_id: DbId,
    merged: &MergedMetadata,
    scope: &Scope,
) -> anyhow::Result<()> {
    if !CreditType::ALL
        .into_iter()
        .any(|credit_type| scope.credit(credit_type))
    {
        return Ok(());
    }
    let mut stored = lyra_db::credits::links_for_owner(db, owner_id)?;
    stored.sort_by_key(|link| link.credit.artist_order);
    let desired: Vec<lyra_db::credits::CreditLinkInput> = CreditType::ALL
        .into_iter()
        .flat_map(|credit_type| -> Vec<lyra_db::credits::CreditLinkInput> {
            if scope.credit(credit_type) {
                merged
                    .credits
                    .get(&credit_type)
                    .into_iter()
                    .flatten()
                    .map(|input| lyra_db::credits::CreditLinkInput {
                        artist_id: input.artist_id,
                        credit_type,
                        detail: input.detail.clone(),
                    })
                    .collect()
            } else {
                stored
                    .iter()
                    .filter(|link| link.credit.credit_type == credit_type)
                    .map(|link| lyra_db::credits::CreditLinkInput {
                        artist_id: link.artist_id,
                        credit_type,
                        detail: link.credit.detail.clone(),
                    })
                    .collect()
            }
        })
        .collect();
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
        lyra_db::credits::replace_for_owner(db, owner_id, &desired)?;
    }
    Ok(())
}

/// Rewrites the relations into an artist from its resolved layers, one whole
/// set per type. Types out of scope keep their stored relations.
fn sync_incoming_relations(
    db: &mut DbAnyTransactionMut<'_>,
    artist_id: DbId,
    merged: &MergedMetadata,
    scope: &Scope,
) -> anyhow::Result<()> {
    for relation_type in ArtistRelationType::ALL {
        if !scope.relation(relation_type) {
            continue;
        }
        let desired: HashMap<DbId, Option<String>> = merged
            .relations
            .get(&relation_type)
            .into_iter()
            .flatten()
            .map(|input| (input.artist_id, input.attributes.clone()))
            .collect();
        let stored =
            lyra_db::artists::relations::get_relations_to(db, artist_id, Some(relation_type))?;
        for (_, source_id) in &stored {
            if !desired.contains_key(source_id) {
                lyra_db::artists::relations::unlink(db, *source_id, artist_id, relation_type)?;
            }
        }
        for (source_id, attributes) in desired {
            if !stored.iter().any(|(relation, stored_source)| {
                *stored_source == source_id && relation.attributes == attributes
            }) {
                lyra_db::artists::relations::link(
                    db,
                    source_id,
                    artist_id,
                    relation_type,
                    attributes,
                )?;
            }
        }
    }
    Ok(())
}

fn apply_to_release(
    release: &mut Release,
    merged: &MergedMetadata,
    scope: &Scope,
) -> anyhow::Result<bool> {
    let mut changed = false;

    // Required titles keep their stored value when unresolved.
    if scope.field(MetadataField::ReleaseTitle)
        && let Some(title) = field_value::<String>(&merged.fields, MetadataField::ReleaseTitle)?
        && release.release_title != title
    {
        release.set_release_title(title);
        changed = true;
    }
    changed |= assign(
        &mut release.sort_title,
        merged,
        scope,
        MetadataField::SortTitle,
    )?;
    changed |= assign(
        &mut release.release_type,
        merged,
        scope,
        MetadataField::ReleaseType,
    )?;
    changed |= assign(
        &mut release.release_date,
        merged,
        scope,
        MetadataField::ReleaseDate,
    )?;

    Ok(changed)
}

fn apply_to_track(
    track: &mut Track,
    merged: &MergedMetadata,
    scope: &Scope,
) -> anyhow::Result<bool> {
    let mut changed = false;

    if scope.field(MetadataField::TrackTitle)
        && let Some(title) = field_value::<String>(&merged.fields, MetadataField::TrackTitle)?
        && track.track_title != title
    {
        track.set_track_title(title);
        changed = true;
    }
    changed |= assign(
        &mut track.sort_title,
        merged,
        scope,
        MetadataField::SortTitle,
    )?;
    for (field, target) in [
        (MetadataField::Year, &mut track.year),
        (MetadataField::Disc, &mut track.disc),
        (MetadataField::DiscTotal, &mut track.disc_total),
        (MetadataField::Track, &mut track.track),
        (MetadataField::TrackTotal, &mut track.track_total),
    ] {
        changed |= assign(target, merged, scope, field)?;
    }

    Ok(changed)
}

fn apply_to_artist(
    artist: &mut Artist,
    merged: &MergedMetadata,
    scope: &Scope,
) -> anyhow::Result<bool> {
    let mut changed = false;

    if scope.field(MetadataField::ArtistName)
        && let Some(name) = field_value::<String>(&merged.fields, MetadataField::ArtistName)?
        && artist.artist_name != name
    {
        artist.set_artist_name(name);
        changed = true;
    }
    changed |= assign(
        &mut artist.artist_type,
        merged,
        scope,
        MetadataField::ArtistType,
    )?;
    changed |= assign(
        &mut artist.sort_name,
        merged,
        scope,
        MetadataField::SortName,
    )?;
    changed |= assign(
        &mut artist.description,
        merged,
        scope,
        MetadataField::Description,
    )?;

    Ok(changed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_layer(source_id: &str, fields_json: &str, updated_at: u64) -> MetadataLayer {
        MetadataLayer::new(
            source_id,
            crate::services::metadata::layers::json::fields_from_json(
                serde_json::from_str(fields_json).expect("test layer fields parse"),
            ),
            updated_at,
        )
    }

    fn text(merged: &MergedMetadata, field: MetadataField) -> Option<String> {
        field_value(&merged.fields, field).expect("resolved text")
    }

    fn make_provider(provider_id: &str, position: u32, enabled: bool) -> lyra_db::ProviderConfig {
        lyra_db::ProviderConfig {
            db_id: None,
            provider_id: provider_id.to_string(),
            position,
            enabled,
        }
    }

    #[test]
    fn merge_layers_higher_priority_provider_wins() {
        let layers = vec![
            make_layer("low", r#"{"track_title": "Low Priority Title"}"#, 1000),
            make_layer("high", r#"{"track_title": "High Priority Title"}"#, 1000),
        ];
        let providers = ActiveProviders::from_configs(vec![
            make_provider("low", 1, true),
            make_provider("high", 0, true),
        ]);

        let merged = merge_layers(layers, &providers);

        assert_eq!(
            text(&merged, MetadataField::TrackTitle).as_deref(),
            Some("High Priority Title")
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
        let providers = ActiveProviders::from_configs(vec![
            make_provider("high", 0, true),
            make_provider("low", 1, true),
        ]);

        let merged = merge_layers(layers, &providers);

        assert_eq!(merged.fields.get(&MetadataField::SortTitle), Some(&None));
        assert_eq!(
            text(&merged, MetadataField::ReleaseDate).as_deref(),
            Some("1999")
        );
    }

    #[test]
    fn apply_to_release_clears_a_field_no_layer_supplies() -> anyhow::Result<()> {
        let mut release = Release {
            db_id: None,
            id: "release".to_string(),
            release_title: "Stored Title".to_string(),
            sort_title: Some("Stored Sort".to_string()),
            release_type: Some(lyra_db::releases::ReleaseType::Album),
            release_date: Some("1999".to_string()),
            media_formats: None,
            barcode: None,
            locked: None,
            created_at: None,
            ctime: None,
        };
        let merged = merge_layers(
            vec![make_layer("provider", r#"{"release_title": "Layer"}"#, 1)],
            &ActiveProviders::from_configs(vec![make_provider("provider", 0, true)]),
        );

        assert!(apply_to_release(&mut release, &merged, &Scope::All)?);

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
        let merged = merge_layers(Vec::new(), &ActiveProviders::from_configs(Vec::new()));

        assert!(!apply_to_release(&mut release, &merged, &Scope::All)?);
        assert_eq!(release.release_title, "Stored Title");
        Ok(())
    }

    #[test]
    fn merge_layers_unlisted_provider_is_excluded() {
        let layers = vec![
            make_layer("off", r#"{"track_title": "Unlisted Title"}"#, 2000),
            make_layer("live", r#"{"track_title": "Listed Title"}"#, 1000),
        ];
        let providers = ActiveProviders::from_configs(vec![make_provider("live", 1, true)]);

        let merged = merge_layers(layers, &providers);

        assert_eq!(
            text(&merged, MetadataField::TrackTitle).as_deref(),
            Some("Listed Title")
        );
    }

    #[test]
    fn merge_layers_keeps_local_source_without_a_provider() {
        let layers = vec![make_layer(
            LOCAL_SOURCE_ID,
            r#"{"track_title": "Tagged Title"}"#,
            1000,
        )];

        let merged = merge_layers(layers, &ActiveProviders::from_configs(Vec::new()));

        assert_eq!(
            text(&merged, MetadataField::TrackTitle).as_deref(),
            Some("Tagged Title")
        );
    }

    #[test]
    fn merge_layers_ranks_local_source_below_every_enabled_provider() {
        let layers = vec![
            make_layer(LOCAL_SOURCE_ID, r#"{"track_title": "Tagged Title"}"#, 2000),
            make_layer("provider", r#"{"track_title": "Provider Title"}"#, 1000),
        ];
        let providers = ActiveProviders::from_configs(vec![make_provider("provider", 0, true)]);

        let merged = merge_layers(layers, &providers);

        assert_eq!(
            text(&merged, MetadataField::TrackTitle).as_deref(),
            Some("Provider Title")
        );
    }

    mod snapshot {
        use std::collections::BTreeMap;

        use agdb::DbAny;

        use super::*;
        use lyra_db::fixtures::{
            insert_artist,
            insert_release,
            insert_track,
        };
        use lyra_db::test_db::{
            new_test_db,
            stored_keys,
        };

        fn enable_provider(db: &mut DbAny, provider_id: &str, position: u32) -> anyhow::Result<()> {
            lyra_db::providers::upsert(db, &make_provider(provider_id, position, true))?;
            Ok(())
        }

        fn save_layer(
            db: &mut DbAny,
            node_id: DbId,
            source_id: &str,
            fields: serde_json::Value,
            updated_at: u64,
        ) -> anyhow::Result<()> {
            let fields =
                crate::services::metadata::layers::json::resolved_fields_from_json(db, fields);
            lyra_db::metadata::layers::upsert(
                db,
                node_id,
                &MetadataLayer::new(source_id, fields, updated_at),
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
                    fields: crate::services::metadata::layers::json::inputs_from_json(fields),
                    ..Default::default()
                },
                &HashMap::new(),
                &Default::default(),
            )
        }

        fn genre_names(db: &DbAny, release_id: DbId) -> anyhow::Result<Vec<String>> {
            Ok(lyra_db::genres::get_for_release(db, release_id)?
                .into_iter()
                .map(|genre| genre.name)
                .collect())
        }

        fn label_names(db: &DbAny, release_id: DbId) -> anyhow::Result<Vec<String>> {
            Ok(lyra_db::labels::get_for_release(db, release_id)?
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
                .filter_map(|id| lyra_db::genres::get_by_id(db, id).transpose())
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
                lyra_db::artists::relations::get_relations_to(db, artist_id, Some(relation_type))?
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
            enable_provider(&mut db, "low", 1)?;
            enable_provider(&mut db, "high", 0)?;
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
            let mut locked = lyra_db::artists::get_by_id(&db, character)?.expect("artist exists");
            locked.locked = Some(true);
            lyra_db::artists::update(&mut db, &locked)?;
            enable_provider(&mut db, "provider", 0)?;

            save_relations(
                &mut db,
                character,
                "provider",
                &[(ArtistRelationType::VoiceActor, &[actor])],
            )?;
            assert!(related_into(&db, character, ArtistRelationType::VoiceActor)?.is_empty());
            assert_eq!(
                lyra_db::metadata::layers::get_for_entity(&db, character)?.len(),
                1
            );

            let mut unlocked = lyra_db::artists::get_by_id(&db, character)?.expect("artist exists");
            unlocked.locked = None;
            lyra_db::artists::update(&mut db, &unlocked)?;
            apply_merged_metadata_to_entity(&mut db, character)?;
            assert_eq!(
                related_into(&db, character, ArtistRelationType::VoiceActor)?,
                vec![actor]
            );
            Ok(())
        }

        #[test]
        fn a_manual_relation_set_outranks_every_provider() -> anyhow::Result<()> {
            let mut db = new_test_db()?;
            let character = insert_artist(&mut db, "Character")?;
            let manual_actor = insert_artist(&mut db, "Manual Actor")?;
            let provider_actor = insert_artist(&mut db, "Provider Actor")?;
            let member = insert_artist(&mut db, "Member")?;
            enable_provider(&mut db, "provider", 0)?;
            lyra_db::metadata::layers::upsert(
                &mut db,
                character,
                &MetadataLayer::new(MANUAL_SOURCE_ID, Default::default(), 1).with_relations(
                    BTreeMap::from([(
                        ArtistRelationType::VoiceActor,
                        vec![LayerRelationInput {
                            artist_id: manual_actor,
                            attributes: None,
                        }],
                    )]),
                ),
            )?;

            save_relations(
                &mut db,
                character,
                "provider",
                &[
                    (ArtistRelationType::VoiceActor, &[provider_actor]),
                    (ArtistRelationType::MemberOf, &[member]),
                ],
            )?;

            assert_eq!(
                related_into(&db, character, ArtistRelationType::VoiceActor)?,
                vec![manual_actor]
            );
            assert_eq!(
                related_into(&db, character, ArtistRelationType::MemberOf)?,
                vec![member],
                "a type the manual layer leaves out falls through"
            );
            Ok(())
        }

        fn credits_of(db: &DbAny, owner: DbId) -> anyhow::Result<Vec<(lyra_db::CreditType, DbId)>> {
            let mut links = lyra_db::credits::links_for_owner(db, owner)?;
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
            credits: &[(lyra_db::CreditType, &[DbId])],
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
            lyra_db::metadata::layers::upsert(
                &mut db,
                track,
                &MetadataLayer::new(LOCAL_SOURCE_ID, Default::default(), 1).with_credits(
                    BTreeMap::from([
                        (lyra_db::CreditType::Artist, credit_list(&[local_artist])),
                        (lyra_db::CreditType::Composer, credit_list(&[composer])),
                    ]),
                ),
            )?;
            enable_provider(&mut db, "provider", 0)?;
            save_credits(
                &mut db,
                track,
                "provider",
                &[(lyra_db::CreditType::Artist, &[second, first])],
            )?;
            assert_eq!(
                credits_of(&db, track)?,
                vec![
                    (lyra_db::CreditType::Artist, second),
                    (lyra_db::CreditType::Artist, first),
                    (lyra_db::CreditType::Composer, composer),
                ],
                "the provider wins the artist role; the local composer stays"
            );

            save_credits(
                &mut db,
                track,
                "provider",
                &[(lyra_db::CreditType::Composer, &[])],
            )?;
            assert_eq!(
                credits_of(&db, track)?,
                vec![
                    (lyra_db::CreditType::Artist, second),
                    (lyra_db::CreditType::Artist, first)
                ],
                "an empty set clears its role and leaves the others"
            );

            crate::services::metadata::layers::clear_provider_layer(&mut db, track, "provider")?;
            assert_eq!(
                credits_of(&db, track)?,
                vec![
                    (lyra_db::CreditType::Artist, local_artist),
                    (lyra_db::CreditType::Composer, composer),
                ]
            );
            Ok(())
        }

        #[test]
        fn a_provider_split_hides_the_combined_artist_from_browsing() -> anyhow::Result<()> {
            use crate::services::catalog::{
                self,
                artists::{
                    ArtistFilter,
                    Artists,
                },
            };

            let mut db = new_test_db()?;
            let track = insert_track(&mut db, "Track")?;
            let combined = insert_artist(&mut db, "Alpha & Beta")?;
            let [alpha, beta] = [
                insert_artist(&mut db, "Alpha")?,
                insert_artist(&mut db, "Beta")?,
            ];
            lyra_db::metadata::layers::upsert(
                &mut db,
                track,
                &MetadataLayer::new(LOCAL_SOURCE_ID, Default::default(), 1).with_credits(
                    BTreeMap::from([(lyra_db::CreditType::Artist, credit_list(&[combined]))]),
                ),
            )?;
            apply_merged_metadata_to_entity(&mut db, track)?;
            let browsed = |db: &DbAny, filter: ArtistFilter| -> anyhow::Result<Vec<DbId>> {
                let query = catalog::Query::<Artists>::new(filter);
                let mut ids = catalog::order(db, &catalog::Viewer::System, &query)?;
                ids.sort();
                Ok(ids)
            };
            let searched = |db: &DbAny| -> anyhow::Result<Vec<DbId>> {
                let mut ids: Vec<DbId> = lyra_db::search::collect_candidates(db, None)?
                    .artists
                    .into_iter()
                    .map(|candidate| candidate.db_id)
                    .collect();
                ids.sort();
                Ok(ids)
            };
            assert_eq!(browsed(&db, ArtistFilter::default())?, vec![combined]);
            assert_eq!(searched(&db)?, vec![combined]);

            enable_provider(&mut db, "provider", 0)?;
            save_credits(
                &mut db,
                track,
                "provider",
                &[(lyra_db::CreditType::Artist, &[alpha, beta])],
            )?;
            assert_eq!(browsed(&db, ArtistFilter::default())?, vec![alpha, beta]);
            assert_eq!(searched(&db)?, vec![alpha, beta]);
            assert!(
                lyra_db::artists::get_by_id(&db, combined)?.is_some_and(|artist| !artist.credited),
                "the combined artist survives, uncredited"
            );
            let by_id = ArtistFilter {
                ids: Some(vec![combined]),
                ..ArtistFilter::default()
            };
            assert_eq!(browsed(&db, by_id)?, vec![combined]);

            crate::services::metadata::layers::mark_unmatched(
                &mut db,
                track,
                "provider",
                crate::services::EntityType::Track,
                Vec::new(),
            )?;
            assert_eq!(browsed(&db, ArtistFilter::default())?, vec![combined]);
            assert_eq!(searched(&db)?, vec![combined]);
            Ok(())
        }

        #[test]
        fn a_locked_owner_applies_only_what_its_manual_layer_supplies() -> anyhow::Result<()> {
            let mut db = new_test_db()?;
            let track = insert_track(&mut db, "Stored Title")?;
            let stored = insert_artist(&mut db, "Stored")?;
            let provider_artist = insert_artist(&mut db, "Provider")?;
            let manual_composer = insert_artist(&mut db, "Manual Composer")?;
            lyra_db::credits::replace_for_owner(
                &mut db,
                track,
                &[lyra_db::credits::CreditLinkInput {
                    artist_id: stored,
                    credit_type: lyra_db::CreditType::Artist,
                    detail: None,
                }],
            )?;
            let mut locked = lyra_db::tracks::get_by_id(&db, track)?.expect("track exists");
            locked.locked = Some(true);
            lyra_db::tracks::update(&mut db, &locked)?;
            enable_provider(&mut db, "provider", 0)?;

            save_credits(
                &mut db,
                track,
                "provider",
                &[(lyra_db::CreditType::Artist, &[provider_artist])],
            )?;
            provider_save(
                &mut db,
                track,
                "provider",
                serde_json::json!({"track_title": "Provider Title"}),
            )?;
            assert_eq!(
                credits_of(&db, track)?,
                vec![(lyra_db::CreditType::Artist, stored)]
            );

            lyra_db::metadata::layers::upsert(
                &mut db,
                track,
                &MetadataLayer::new(
                    MANUAL_SOURCE_ID,
                    crate::services::metadata::layers::json::fields_from_json(
                        serde_json::json!({"year": 2001}),
                    ),
                    1,
                )
                .with_credits(BTreeMap::from([(
                    lyra_db::CreditType::Composer,
                    credit_list(&[manual_composer]),
                )])),
            )?;
            apply_merged_metadata_to_entity(&mut db, track)?;
            let applied = lyra_db::tracks::get_by_id(&db, track)?.expect("track exists");
            assert_eq!(applied.track_title, "Stored Title");
            assert_eq!(applied.year, Some(2001));
            assert_eq!(
                credits_of(&db, track)?,
                vec![
                    (lyra_db::CreditType::Artist, stored),
                    (lyra_db::CreditType::Composer, manual_composer),
                ]
            );

            let mut unlocked = lyra_db::tracks::get_by_id(&db, track)?.expect("track exists");
            unlocked.locked = None;
            lyra_db::tracks::update(&mut db, &unlocked)?;
            apply_merged_metadata_to_entity(&mut db, track)?;
            assert_eq!(
                lyra_db::tracks::get_by_id(&db, track)?
                    .expect("track exists")
                    .track_title,
                "Provider Title"
            );
            assert_eq!(
                credits_of(&db, track)?,
                vec![
                    (lyra_db::CreditType::Artist, provider_artist),
                    (lyra_db::CreditType::Composer, manual_composer),
                ]
            );
            Ok(())
        }

        #[test]
        fn credits_no_layer_supplies_go_on_the_next_apply() -> anyhow::Result<()> {
            let mut db = new_test_db()?;
            let track = insert_track(&mut db, "Track")?;
            let artist = insert_artist(&mut db, "Artist")?;
            lyra_db::credits::replace_for_owner(
                &mut db,
                track,
                &[lyra_db::credits::CreditLinkInput {
                    artist_id: artist,
                    credit_type: lyra_db::CreditType::Artist,
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
            let mut locked = lyra_db::releases::get_by_id(&db, locked_id)?.expect("release exists");
            locked.locked = Some(true);
            lyra_db::releases::update(&mut db, &locked)?;
            enable_provider(&mut db, "low", 1)?;
            enable_provider(&mut db, "high", 0)?;

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

            assert!(lyra_db::genres::find_by_name(&db, "Losing")?.is_some());
            assert!(lyra_db::genres::find_by_name(&db, "Locked")?.is_some());
            assert_eq!(genre_names(&db, release_id)?, vec!["Winning"]);
            assert!(
                genre_names(&db, locked_id)?.is_empty(),
                "a locked release is not re-applied"
            );
            assert_eq!(
                lyra_db::metadata::layers::get_for_entity(&db, locked_id)?.len(),
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
            enable_provider(&mut db, "low", 1)?;
            enable_provider(&mut db, "high", 0)?;
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
                lyra_db::labels::find_by_name(&db, "High Label")?.is_none(),
                "a label nothing names any more is collected"
            );
            Ok(())
        }

        #[test]
        fn a_property_no_layer_supplies_is_removed_from_storage() -> anyhow::Result<()> {
            let mut db = new_test_db()?;
            let track_id = insert_track(&mut db, "Track")?;
            let mut track =
                lyra_db::tracks::get_by_id(&db, track_id)?.expect("track exists after insert");
            track.year = Some(1999);
            track.disc = Some(2);
            lyra_db::tracks::update(&mut db, &track)?;
            assert!(stored_keys(&db, track_id)?.contains("year"));

            enable_provider(&mut db, "provider", 0)?;
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
            let track = lyra_db::tracks::get_by_id(&db, track_id)?.expect("track still exists");
            assert_eq!(track.year, None);
            assert_eq!(track.disc, Some(2));
            Ok(())
        }

        #[test]
        fn a_higher_priority_null_clears_a_lower_priority_value() -> anyhow::Result<()> {
            let mut db = new_test_db()?;
            let artist_id = insert_artist(&mut db, "Artist")?;

            enable_provider(&mut db, "low", 1)?;
            enable_provider(&mut db, "high", 0)?;
            save_layer(
                &mut db,
                artist_id,
                "low",
                serde_json::json!({"description": "Low description"}),
                1,
            )?;
            apply_merged_metadata_to_entity(&mut db, artist_id)?;
            assert_eq!(
                lyra_db::artists::get_by_id(&db, artist_id)?
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
                lyra_db::artists::get_by_id(&db, artist_id)?
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
            enable_provider(&mut db, "provider", 0)?;
            save_layer(
                &mut db,
                artist_id,
                "provider",
                serde_json::json!({"artist_type": "person"}),
                2,
            )?;
            apply_merged_metadata_to_entity(&mut db, artist_id)?;
            assert_eq!(
                lyra_db::artists::get_by_id(&db, artist_id)?
                    .expect("artist exists")
                    .artist_type,
                Some(lyra_db::ArtistType::Person)
            );

            lyra_db::providers::upsert(&mut db, &make_provider("provider", 0, false))?;
            apply_merged_metadata_to_entity(&mut db, artist_id)?;

            assert_eq!(
                lyra_db::artists::get_by_id(&db, artist_id)?
                    .expect("artist exists")
                    .artist_type,
                Some(lyra_db::ArtistType::Group)
            );
            Ok(())
        }

        #[test]
        fn unsupplied_genres_and_labels_drop_their_relationships() -> anyhow::Result<()> {
            let mut db = new_test_db()?;
            let release_id = insert_release(&mut db, "Release")?;
            lyra_db::genres::sync_release_genres(&mut db, release_id, &["Jazz".to_string()])?;
            lyra_db::labels::sync_release_labels(
                &mut db,
                release_id,
                &[lyra_db::labels::LabelInput {
                    name: "Blue Note".to_string(),
                    catalog_number: None,
                    external_id: None,
                }],
            )?;

            enable_provider(&mut db, "provider", 0)?;
            save_layer(
                &mut db,
                release_id,
                "provider",
                serde_json::json!({"release_title": "Release"}),
                1,
            )?;
            apply_merged_metadata_to_entity(&mut db, release_id)?;

            assert!(lyra_db::genres::get_for_release(&db, release_id)?.is_empty());
            assert!(lyra_db::labels::get_for_release(&db, release_id)?.is_empty());
            Ok(())
        }

        #[test]
        fn dropping_the_manual_layer_falls_back_to_the_next_layer() -> anyhow::Result<()> {
            let mut db = new_test_db()?;
            let release_id = insert_release(&mut db, "Release")?;

            enable_provider(&mut db, "provider", 0)?;
            save_layer(
                &mut db,
                release_id,
                "provider",
                serde_json::json!({
                    "sort_title": "Provider Sort",
                    "genres": ["Provider Genre"],
                }),
                2,
            )?;
            save_layer(
                &mut db,
                release_id,
                MANUAL_SOURCE_ID,
                serde_json::json!({
                    "sort_title": "Manual Sort",
                    "genres": ["Manual Genre"],
                }),
                1,
            )?;
            apply_merged_metadata_to_entity(&mut db, release_id)?;

            assert_eq!(
                lyra_db::releases::get_by_id(&db, release_id)?
                    .expect("release exists")
                    .sort_title
                    .as_deref(),
                Some("Manual Sort")
            );
            assert_eq!(genre_names(&db, release_id)?, vec!["Manual Genre"]);

            db.transaction_mut(|t| {
                lyra_db::metadata::layers::remove_inside_tx(t, release_id, MANUAL_SOURCE_ID)
            })?;
            apply_merged_metadata_to_entity(&mut db, release_id)?;

            assert_eq!(
                lyra_db::releases::get_by_id(&db, release_id)?
                    .expect("release exists")
                    .sort_title
                    .as_deref(),
                Some("Provider Sort")
            );
            assert_eq!(genre_names(&db, release_id)?, vec!["Provider Genre"]);
            Ok(())
        }

        #[test]
        fn a_locked_entity_records_its_layer_without_applying_it() -> anyhow::Result<()> {
            let mut db = new_test_db()?;
            let track_id = insert_track(&mut db, "Curated Title")?;
            let mut track = lyra_db::tracks::get_by_id(&db, track_id)?.expect("track exists");
            track.year = Some(1999);
            track.locked = Some(true);
            lyra_db::tracks::update(&mut db, &track)?;

            enable_provider(&mut db, "provider", 0)?;
            crate::services::metadata::layers::save_provider_layer(
                &mut db,
                track_id,
                "provider",
                &crate::services::metadata::layers::ProviderValues {
                    fields: crate::services::metadata::layers::json::inputs_from_json(
                        serde_json::json!({"track_title": "Provider Title"}),
                    ),
                    ..Default::default()
                },
                &HashMap::new(),
                &Default::default(),
            )?;

            let layers = lyra_db::metadata::layers::get_for_entity(&db, track_id)?;
            assert_eq!(layers.len(), 1, "a locked entity still records its layer");
            assert_eq!(layers[0].source_id, "provider");

            let track = lyra_db::tracks::get_by_id(&db, track_id)?.expect("track exists");
            assert_eq!(track.track_title, "Curated Title");
            assert_eq!(track.year, Some(1999));

            let mut track = track;
            track.locked = None;
            lyra_db::tracks::update(&mut db, &track)?;
            apply_merged_metadata_to_entity(&mut db, track_id)?;

            let track = lyra_db::tracks::get_by_id(&db, track_id)?.expect("track exists");
            assert_eq!(track.track_title, "Provider Title");
            assert_eq!(track.year, None);
            Ok(())
        }
    }
}
