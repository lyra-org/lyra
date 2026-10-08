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
    DbAccess,
    MetadataField,
    MetadataLayer,
    ProviderConfig,
    Release,
    Track,
    metadata::layers::{
        LayerFields,
        decode,
        label_inputs,
        normalize_value,
    },
};
use crate::services::metadata::layers::LOCAL_SOURCE_ID;

#[derive(Debug, Clone)]
pub(crate) struct MergedMetadata {
    pub(crate) fields: HashMap<MetadataField, serde_json::Value>,
    pub(crate) provenance: HashMap<MetadataField, String>,
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

    /// The resolved graph value; manually owned graph fields keep their stored edges.
    fn graph_value<T>(
        &self,
        field: MetadataField,
        decode: impl FnOnce(&serde_json::Value) -> anyhow::Result<T>,
    ) -> anyhow::Result<Option<T>>
    where
        T: Default,
    {
        if self.manual_fields.contains(&field) {
            return Ok(None);
        }
        match self.fields.get(&field) {
            None | Some(serde_json::Value::Null) => Ok(Some(T::default())),
            Some(value) => decode(value).map(Some),
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

    for layer in sorted_layers {
        let fields: LayerFields = match layer.parsed_fields() {
            Ok(fields) => fields,
            Err(err) => {
                tracing::warn!(
                    source_id = %layer.source_id,
                    error = %format!("{err:#}"),
                    "skipping metadata layer that no longer validates"
                );
                continue;
            }
        };

        for (field, value) in fields {
            // The highest-priority layer mentioning a field wins, including explicit null.
            if let Entry::Vacant(entry) = merged_fields.entry(field) {
                entry.insert(value);
                provenance.insert(field, layer.source_id.clone());
            }
        }
    }

    MergedMetadata {
        fields: merged_fields,
        provenance,
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
        if let Some(genre_names) = merged.graph_value(MetadataField::Genres, |value| {
            decode::<Vec<String>>(MetadataField::Genres, value)
        })? {
            db::genres::sync_release_genres(db, node_id, &genre_names)?;
        }
        if let Some(label_inputs) = merged.graph_value(MetadataField::Labels, label_inputs)? {
            db::labels::sync_release_labels_inside_tx(db, node_id, &label_inputs)?;
        }
    } else if let Some(mut track) = db::tracks::get_by_id(db, node_id)? {
        if track.locked.unwrap_or(false) {
            return Ok(());
        }
        if apply_to_track(&mut track, &merged)? {
            db::tracks::update_in_transaction(db, &track)?;
        }
    } else if let Some(mut artist) = db::artists::get_by_id(db, node_id)?
        && !artist.locked.unwrap_or(false)
        && apply_to_artist(&mut artist, &merged)?
    {
        db::artists::update_in_transaction(db, &artist)?;
    }

    Ok(())
}

pub(crate) fn materialize_provider_labels_if_manually_owned(
    db: &mut DbAny,
    node_id: DbId,
) -> anyhow::Result<()> {
    if db::releases::get_by_id(db, node_id)?.is_none()
        || !db::metadata::manual_overrides::owns_field(db, node_id, MetadataField::Labels)?
    {
        return Ok(());
    }
    let layers = db::metadata::layers::get_for_entity(db, node_id)?;
    let providers = db::providers::get(db)?;
    let merged = merge_layers(layers, &providers);
    if let Some(value) = merged
        .fields
        .get(&MetadataField::Labels)
        .filter(|value| !value.is_null())
    {
        db::labels::materialize_labels(db, &label_inputs(value)?)?;
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
        MetadataLayer {
            db_id: None,
            source_id: source_id.to_string(),
            fields: fields_json.to_string(),
            updated_at,
        }
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

    fn merged_with(field: MetadataField, value: serde_json::Value) -> MergedMetadata {
        MergedMetadata {
            fields: HashMap::from([(field, value)]),
            provenance: HashMap::new(),
            manual_fields: HashSet::new(),
        }
    }

    fn genres(merged: &MergedMetadata) -> Option<Vec<String>> {
        merged
            .graph_value(MetadataField::Genres, |value| {
                decode(MetadataField::Genres, value)
            })
            .expect("validated genres decode")
    }

    #[test]
    fn graph_value_covers_every_resolution_state() {
        let mut merged = merge_layers(Vec::new(), &[]);
        assert_eq!(genres(&merged), Some(Vec::new()), "absent clears");

        merged = merged_with(MetadataField::Genres, serde_json::Value::Null);
        assert_eq!(genres(&merged), Some(Vec::new()), "null clears");

        merged = merged_with(MetadataField::Genres, serde_json::json!(["Jazz"]));
        assert_eq!(genres(&merged), Some(vec!["Jazz".to_string()]));

        merged.manual_fields.insert(MetadataField::Genres);
        assert_eq!(genres(&merged), None, "manual ownership keeps stored edges");
    }

    #[test]
    fn merge_layers_skips_a_layer_that_no_longer_validates() {
        let layers = vec![
            make_layer("high", r#"{"track_title": "High", "work_id": "w"}"#, 1000),
            make_layer("low", r#"{"track_title": "Low"}"#, 1000),
        ];
        let providers = vec![
            make_provider("high", 100, true),
            make_provider("low", 10, true),
        ];

        let merged = merge_layers(layers, &providers);

        assert_eq!(
            merged
                .provenance
                .get(&MetadataField::TrackTitle)
                .map(String::as_str),
            Some("low")
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
                &std::collections::BTreeMap::from([(
                    MetadataField::TrackTitle,
                    serde_json::json!("Provider Title"),
                )]),
                &HashMap::new(),
                &HashMap::new(),
                &HashSet::new(),
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
