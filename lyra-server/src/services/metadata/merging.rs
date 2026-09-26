// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};

use agdb::{
    DbAny,
    DbAnyTransactionMut,
    DbId,
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::db::{
    self,
    Artist,
    DbAccess,
    MetadataLayer,
    ProviderConfig,
    Release,
    Track,
    metadata::manual_overrides::ManualMetadataField,
};
use crate::services::metadata::layers::LOCAL_SOURCE_ID;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MergedMetadata {
    pub(crate) fields: HashMap<String, serde_json::Value>,
    pub(crate) provenance: HashMap<String, String>,
    manual_fields: HashSet<ManualMetadataField>,
}

/// A field update, explicit clear, or instruction to preserve the stored value.
enum Resolution<T> {
    Value(T),
    Cleared,
    Keep,
}

impl<T: Default> Resolution<T> {
    fn value_to_write(self) -> Option<T> {
        match self {
            Self::Value(value) => Some(value),
            Self::Cleared => Some(T::default()),
            Self::Keep => None,
        }
    }
}

fn resolve_field<T>(
    merged: &MergedMetadata,
    key: &str,
    parse: impl FnOnce(&serde_json::Value) -> Option<T>,
) -> Resolution<T> {
    let Some(value) = merged.fields.get(key) else {
        return Resolution::Cleared;
    };
    if value.is_null() {
        return Resolution::Cleared;
    }

    match parse(value) {
        Some(parsed) => Resolution::Value(parsed),
        None => {
            tracing::warn!(
                field = key,
                %value,
                "keeping stored value: merged metadata field has an unusable shape"
            );
            Resolution::Keep
        }
    }
}

/// Manual ownership preserves graph fields because the overlay removes their values.
fn resolve_graph_field<T>(
    merged: &MergedMetadata,
    field: ManualMetadataField,
    parse: impl FnOnce(&serde_json::Value) -> Option<T>,
) -> Resolution<T> {
    if merged.manual_fields.contains(&field) {
        return Resolution::Keep;
    }

    resolve_field(merged, field.as_str(), parse)
}

fn value_as_string(value: &serde_json::Value) -> Option<String> {
    value.as_str().map(str::to_string)
}

fn value_as_u32(value: &serde_json::Value) -> Option<u32> {
    value.as_u64().and_then(|value| u32::try_from(value).ok())
}

fn assign<T: PartialEq>(target: &mut Option<T>, resolved: Resolution<T>) -> bool {
    let next = match resolved {
        Resolution::Value(value) => Some(value),
        Resolution::Cleared => None,
        Resolution::Keep => return false,
    };
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
            merged.fields.remove(field.as_str());
        } else {
            merged.fields.insert(field.as_str().to_string(), value);
        }
        merged.provenance.remove(field.as_str());
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

    let mut merged_fields: HashMap<String, serde_json::Value> = HashMap::new();
    let mut provenance: HashMap<String, String> = HashMap::new();

    for layer in sorted_layers {
        let fields: HashMap<String, serde_json::Value> = match serde_json::from_str(&layer.fields) {
            Ok(f) => f,
            Err(err) => {
                tracing::warn!(
                    source_id = %layer.source_id,
                    error = %err,
                    "failed to parse metadata layer fields as JSON, skipping layer"
                );
                HashMap::new()
            }
        };

        for (key, value) in fields {
            // The highest-priority layer mentioning a key wins, including explicit null.
            if !merged_fields.contains_key(&key) {
                merged_fields.insert(key.clone(), value);
                provenance.insert(key, layer.source_id.clone());
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
        if apply_to_release(&mut release, &merged) {
            db::releases::update_in_transaction(db, &release)?;
        }
        if let Some(genre_names) = resolve_genres(&merged).value_to_write() {
            db::genres::sync_release_genres(db, node_id, &genre_names)?;
        }
        if let Some(label_inputs) = resolve_labels(&merged).value_to_write() {
            db::labels::sync_release_labels_inside_tx(db, node_id, &label_inputs)?;
        }
    } else if let Some(mut track) = db::tracks::get_by_id(db, node_id)? {
        if track.locked.unwrap_or(false) {
            return Ok(());
        }
        if apply_to_track(&mut track, &merged) {
            db::tracks::update_in_transaction(db, &track)?;
        }
    } else if let Some(mut artist) = db::artists::get_by_id(db, node_id)?
        && !artist.locked.unwrap_or(false)
        && apply_to_artist(&mut artist, &merged)
    {
        db::artists::update_in_transaction(db, &artist)?;
    }

    Ok(())
}

fn resolve_genres(merged: &MergedMetadata) -> Resolution<Vec<String>> {
    resolve_graph_field(merged, ManualMetadataField::Genres, |value| {
        let names = value.as_array()?;
        Some(
            names
                .iter()
                .filter_map(|name| name.as_str().map(String::from))
                .collect(),
        )
    })
}

/// Schema: JSON array of objects with required `name: String` and optional
/// `catalog_number: String` and `external_id: { provider_id, id_type,
/// id_value }` (all three inner fields required when present).
///
/// Fail-closed on schema violation is deliberate: silently treating malformed
/// input as "zero labels" would unlink every label and cascade-delete
/// orphan Labels on every ingestion.
fn resolve_labels(merged: &MergedMetadata) -> Resolution<Vec<db::labels::LabelInput>> {
    resolve_graph_field(merged, ManualMetadataField::Labels, parse_label_inputs)
}

pub(crate) fn materialize_provider_labels_if_manually_owned(
    db: &mut DbAny,
    node_id: DbId,
) -> anyhow::Result<()> {
    if db::releases::get_by_id(db, node_id)?.is_none()
        || !db::metadata::manual_overrides::owns_field(db, node_id, ManualMetadataField::Labels)?
    {
        return Ok(());
    }
    let layers = db::metadata::layers::get_for_entity(db, node_id)?;
    let providers = db::providers::get(db)?;
    let merged = merge_layers(layers, &providers);
    if let Resolution::Value(inputs) = resolve_field(
        &merged,
        ManualMetadataField::Labels.as_str(),
        parse_label_inputs,
    ) {
        db::labels::materialize_labels(db, &inputs)?;
    }
    Ok(())
}

pub(crate) fn parse_label_inputs(raw: &serde_json::Value) -> Option<Vec<db::labels::LabelInput>> {
    let Some(arr) = raw.as_array() else {
        tracing::warn!(
            ?raw,
            "labels field is not a JSON array; skipping label sync to avoid destructive rescan"
        );
        return None;
    };

    let mut inputs = Vec::with_capacity(arr.len());
    for entry in arr {
        let Some(obj) = entry.as_object() else {
            tracing::warn!(
                ?entry,
                "labels entry is not a JSON object; skipping label sync (expected {{name, catalog_number?, external_id?}})"
            );
            return None;
        };
        let Some(name) = obj.get("name").and_then(|v| v.as_str()) else {
            tracing::warn!(?obj, "labels entry missing `name`; skipping label sync");
            return None;
        };
        let trimmed = name.trim();
        if trimmed.is_empty() {
            tracing::warn!(?obj, "labels entry `name` is blank; skipping label sync");
            return None;
        }

        let catalog_number = match obj.get("catalog_number") {
            None | Some(serde_json::Value::Null) => None,
            Some(raw) => {
                let Some(s) = raw.as_str() else {
                    // Coercing a non-string scalar to None would overwrite an
                    // existing cat# on sync. Fail-closed, same discipline as
                    // top-level shape.
                    tracing::warn!(
                        ?raw,
                        "labels entry `catalog_number` is not a string; skipping label sync"
                    );
                    return None;
                };
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            }
        };

        let external_id = match obj.get("external_id") {
            None | Some(serde_json::Value::Null) => None,
            Some(ext_raw) => {
                let Some(ext) = ext_raw.as_object() else {
                    tracing::warn!(
                        ?ext_raw,
                        "labels entry `external_id` is not an object; skipping label sync"
                    );
                    return None;
                };
                // Trim first: a whitespace-only id_value would pass a bare
                // non-empty check and persist a garbage ExternalId that
                // blocks future real enrichment.
                let provider_id = ext
                    .get("provider_id")
                    .and_then(|v| v.as_str())
                    .map(str::trim);
                let id_type = ext.get("id_type").and_then(|v| v.as_str()).map(str::trim);
                let id_value = ext.get("id_value").and_then(|v| v.as_str()).map(str::trim);
                match (provider_id, id_type, id_value) {
                    (Some(p), Some(t), Some(v))
                        if !p.is_empty() && !t.is_empty() && !v.is_empty() =>
                    {
                        Some(db::labels::LabelExternalIdInput {
                            provider_id: p.to_string(),
                            id_type: t.to_string(),
                            id_value: v.to_string(),
                        })
                    }
                    _ => {
                        tracing::warn!(
                            ?ext,
                            "labels entry `external_id` missing required fields or has blank values; skipping label sync"
                        );
                        return None;
                    }
                }
            }
        };

        inputs.push(db::labels::LabelInput {
            name: trimmed.to_string(),
            catalog_number,
            external_id,
        });
    }
    Some(inputs)
}

fn apply_to_release(release: &mut Release, merged: &MergedMetadata) -> bool {
    let mut changed = false;

    // Required titles keep their stored value when unresolved.
    if let Resolution::Value(title) = resolve_field(merged, "release_title", value_as_string)
        && release.release_title != title
    {
        release.set_release_title(title);
        changed = true;
    }
    changed |= assign(
        &mut release.sort_title,
        resolve_field(merged, "sort_title", value_as_string),
    );
    changed |= assign(
        &mut release.release_type,
        resolve_field(merged, "release_type", |value| {
            value
                .as_str()
                .and_then(|value| db::releases::ReleaseType::from_db_str(value).ok())
        }),
    );
    changed |= assign(
        &mut release.release_date,
        resolve_field(merged, "release_date", |value| {
            value
                .as_str()
                .and_then(db::releases::normalize_release_date)
        }),
    );

    changed
}

fn apply_to_track(track: &mut Track, merged: &MergedMetadata) -> bool {
    let mut changed = false;

    if let Resolution::Value(title) = resolve_field(merged, "track_title", value_as_string)
        && track.track_title != title
    {
        track.set_track_title(title);
        changed = true;
    }
    changed |= assign(
        &mut track.sort_title,
        resolve_field(merged, "sort_title", value_as_string),
    );
    for (field, target) in [
        ("year", &mut track.year),
        ("disc", &mut track.disc),
        ("disc_total", &mut track.disc_total),
        ("track", &mut track.track),
        ("track_total", &mut track.track_total),
    ] {
        changed |= assign(target, resolve_field(merged, field, value_as_u32));
    }

    changed
}

fn apply_to_artist(artist: &mut Artist, merged: &MergedMetadata) -> bool {
    let mut changed = false;

    if let Resolution::Value(name) = resolve_field(merged, "artist_name", value_as_string)
        && artist.artist_name != name
    {
        artist.set_artist_name(name);
        changed = true;
    }

    let artist_type = resolve_field(merged, "artist_type", |value| {
        value
            .as_str()
            .and_then(|value| crate::db::ArtistType::from_db_str(value).ok())
    });
    changed |= assign(&mut artist.artist_type, artist_type);

    changed |= assign(
        &mut artist.sort_name,
        resolve_field(merged, "sort_name", value_as_string),
    );
    changed |= assign(
        &mut artist.description,
        resolve_field(merged, "description", value_as_string),
    );

    changed
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
            merged.fields.get("track_title").and_then(|v| v.as_str()),
            Some("High Priority Title")
        );
        assert_eq!(
            merged.provenance.get("track_title").map(String::as_str),
            Some("high")
        );
    }

    fn merged_with_labels(labels: serde_json::Value) -> MergedMetadata {
        let mut fields = HashMap::new();
        fields.insert("labels".to_string(), labels);
        MergedMetadata {
            fields,
            provenance: HashMap::new(),
            manual_fields: HashSet::new(),
        }
    }

    fn resolved_labels(labels: serde_json::Value) -> Resolution<Vec<db::labels::LabelInput>> {
        resolve_labels(&merged_with_labels(labels))
    }

    #[track_caller]
    fn expect_value<T>(resolution: Resolution<T>) -> T {
        match resolution {
            Resolution::Value(value) => value,
            Resolution::Cleared => panic!("expected a resolved value, got Cleared"),
            Resolution::Keep => panic!("expected a resolved value, got Keep"),
        }
    }

    #[track_caller]
    fn assert_keep<T>(resolution: Resolution<T>) {
        assert!(
            matches!(resolution, Resolution::Keep),
            "expected the stored value to be kept"
        );
    }

    #[track_caller]
    fn assert_cleared<T>(resolution: Resolution<T>) {
        assert!(
            matches!(resolution, Resolution::Cleared),
            "expected the field to be cleared"
        );
    }

    #[test]
    fn resolve_labels_parses_full_shape() {
        let inputs = expect_value(resolved_labels(serde_json::json!([
            {
                "name": "Blue Note",
                "catalog_number": "BN-1577",
                "external_id": {
                    "provider_id": "musicbrainz",
                    "id_type": "label_id",
                    "id_value": "abc"
                }
            },
            {
                "name": "Impulse!"
            }
        ])));
        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[0].name, "Blue Note");
        assert_eq!(inputs[0].catalog_number.as_deref(), Some("BN-1577"));
        assert!(inputs[0].external_id.is_some());
        assert_eq!(inputs[1].name, "Impulse!");
        assert!(inputs[1].catalog_number.is_none());
        assert!(inputs[1].external_id.is_none());
    }

    #[test]
    fn resolve_labels_absent_clears() {
        let merged = MergedMetadata {
            fields: HashMap::new(),
            provenance: HashMap::new(),
            manual_fields: HashSet::new(),
        };
        assert_cleared(resolve_labels(&merged));
    }

    #[test]
    fn resolve_labels_null_clears() {
        assert_cleared(resolved_labels(serde_json::Value::Null));
    }

    #[test]
    fn resolve_labels_manual_ownership_keeps_stored_links() {
        let mut merged = merged_with_labels(serde_json::json!([{"name": "Blue Note"}]));
        merged.manual_fields.insert(ManualMetadataField::Labels);
        assert_keep(resolve_labels(&merged));
    }

    #[test]
    fn resolve_labels_empty_array_is_an_authoritative_empty_set() {
        let inputs = expect_value(resolved_labels(serde_json::json!([])));
        assert!(inputs.is_empty());
    }

    #[test]
    fn resolve_labels_wrong_top_level_shape_keeps_stored_links() {
        assert_keep(resolved_labels(serde_json::json!("Blue Note")));
        assert_keep(resolved_labels(serde_json::json!({"name": "Blue Note"})));
        assert_keep(resolved_labels(serde_json::json!(42)));
    }

    #[test]
    fn resolve_labels_malformed_entry_keeps_stored_links() {
        // Bare strings or entries missing `name` poison the whole batch.
        assert_keep(resolved_labels(serde_json::json!(["Blue Note"])));
        assert_keep(resolved_labels(
            serde_json::json!([{"catalog_number": "BN-1"}]),
        ));
        assert_keep(resolved_labels(serde_json::json!([{"name": "   "}])));
    }

    #[test]
    fn resolve_labels_partial_external_id_keeps_stored_links() {
        assert_keep(resolved_labels(serde_json::json!([{
            "name": "Blue Note",
            "external_id": { "provider_id": "mb", "id_type": "label_id" }
        }])));
    }

    #[test]
    fn resolve_labels_non_string_cat_number_keeps_stored_links() {
        // Fail-closed on non-string scalars: silently coercing to None would
        // overwrite an existing cat# with null on sync.
        assert_keep(resolved_labels(serde_json::json!([
            {"name": "Blue Note", "catalog_number": 12345}
        ])));
        assert_keep(resolved_labels(serde_json::json!([
            {"name": "Blue Note", "catalog_number": true}
        ])));
    }

    #[test]
    fn resolve_labels_blank_external_id_value_keeps_stored_links() {
        // Whitespace-only id_value passes is_empty but is not a real claim.
        // Accepting it would create a garbage ExternalId that blocks later
        // real enrichment via the "zero ext_ids" guard.
        assert_keep(resolved_labels(serde_json::json!([{
            "name": "Blue Note",
            "external_id": {
                "provider_id": "mb",
                "id_type": "label_id",
                "id_value": "   "
            }
        }])));
    }

    #[test]
    fn resolve_labels_trims_external_id_components() {
        let inputs = expect_value(resolved_labels(serde_json::json!([{
            "name": "Blue Note",
            "external_id": {
                "provider_id": "  mb  ",
                "id_type": " label_id ",
                "id_value": " bn-001 "
            }
        }])));
        let ext = inputs[0].external_id.as_ref().expect("external_id present");
        assert_eq!(ext.provider_id, "mb");
        assert_eq!(ext.id_type, "label_id");
        assert_eq!(ext.id_value, "bn-001");
    }

    #[test]
    fn resolve_labels_null_external_id_is_ok() {
        let inputs = expect_value(resolved_labels(serde_json::json!([
            {"name": "Blue Note", "external_id": null}
        ])));
        assert_eq!(inputs.len(), 1);
        assert!(inputs[0].external_id.is_none());
    }

    #[test]
    fn resolve_genres_covers_every_resolution_state() {
        let merged = MergedMetadata {
            fields: HashMap::new(),
            provenance: HashMap::new(),
            manual_fields: HashSet::new(),
        };
        assert_cleared(resolve_genres(&merged));

        let mut merged = merged;
        merged
            .fields
            .insert("genres".to_string(), serde_json::json!("Jazz"));
        assert_keep(resolve_genres(&merged));

        merged
            .fields
            .insert("genres".to_string(), serde_json::Value::Null);
        assert_cleared(resolve_genres(&merged));

        merged
            .fields
            .insert("genres".to_string(), serde_json::json!(["Jazz"]));
        merged.manual_fields.insert(ManualMetadataField::Genres);
        assert_keep(resolve_genres(&merged));
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
            merged.fields.get("sort_title"),
            Some(&serde_json::Value::Null)
        );
        assert_eq!(
            merged.provenance.get("sort_title").map(String::as_str),
            Some("high")
        );
        assert_eq!(
            merged.fields.get("release_date").and_then(|v| v.as_str()),
            Some("1999")
        );
        assert_eq!(
            merged.provenance.get("release_date").map(String::as_str),
            Some("low")
        );
    }

    #[test]
    fn apply_to_release_clears_a_field_no_layer_supplies() {
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

        assert!(apply_to_release(&mut release, &merged));

        assert_eq!(release.release_title, "Layer");
        assert!(release.sort_title.is_none());
        assert!(release.release_type.is_none());
        assert!(release.release_date.is_none());
    }

    #[test]
    fn apply_to_release_keeps_the_title_when_no_layer_supplies_one() {
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

        assert!(!apply_to_release(&mut release, &merged));
        assert_eq!(release.release_title, "Stored Title");
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
            merged.fields.get("track_title").and_then(|v| v.as_str()),
            Some("Tagged Title")
        );
        assert_eq!(
            merged.provenance.get("track_title").map(String::as_str),
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
            merged.fields.get("track_title").and_then(|v| v.as_str()),
            Some("Provider Title")
        );
        assert_eq!(
            merged.provenance.get("track_title").map(String::as_str),
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
            merged.fields.get("track_title").and_then(|v| v.as_str()),
            Some("Active Title")
        );
        assert_eq!(
            merged.provenance.get("track_title").map(String::as_str),
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
                    (
                        ManualMetadataField::SortTitle,
                        serde_json::json!("Manual Sort"),
                    ),
                    (ManualMetadataField::Genres, serde_json::Value::Bool(true)),
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
                &HashMap::from([(
                    "track_title".to_string(),
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
