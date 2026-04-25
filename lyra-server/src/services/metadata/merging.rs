// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use agdb::{
    DbAny,
    DbId,
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::db::{
    self,
    Artist,
    MetadataLayer,
    ProviderConfig,
    Release,
    Track,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MergedMetadata {
    pub(crate) fields: HashMap<String, serde_json::Value>,
    pub(crate) provenance: HashMap<String, String>,
}

pub(crate) fn merge_layers(
    layers: Vec<MetadataLayer>,
    providers: &[ProviderConfig],
) -> MergedMetadata {
    let mut priority_map: HashMap<&str, u32> = HashMap::new();
    for provider in providers.iter().filter(|provider| provider.enabled) {
        priority_map.insert(&provider.provider_id, provider.priority);
    }

    let mut sorted_layers: Vec<MetadataLayer> = layers
        .into_iter()
        .filter(|layer| priority_map.contains_key(layer.provider_id.as_str()))
        .collect();
    sorted_layers.sort_by(|a, b| {
        let a_priority = priority_map
            .get(a.provider_id.as_str())
            .copied()
            .unwrap_or(0);
        let b_priority = priority_map
            .get(b.provider_id.as_str())
            .copied()
            .unwrap_or(0);
        b_priority
            .cmp(&a_priority)
            .then_with(|| b.updated_at.cmp(&a.updated_at))
            .then_with(|| a.provider_id.cmp(&b.provider_id))
    });

    let mut merged_fields: HashMap<String, serde_json::Value> = HashMap::new();
    let mut provenance: HashMap<String, String> = HashMap::new();

    for layer in sorted_layers {
        let fields: HashMap<String, serde_json::Value> = match serde_json::from_str(&layer.fields) {
            Ok(f) => f,
            Err(err) => {
                tracing::warn!(
                    provider_id = %layer.provider_id,
                    error = %err,
                    "failed to parse metadata layer fields as JSON, skipping layer"
                );
                HashMap::new()
            }
        };

        for (key, value) in fields {
            if !merged_fields.contains_key(&key) && !value.is_null() {
                merged_fields.insert(key.clone(), value);
                provenance.insert(key, layer.provider_id.clone());
            }
        }
    }

    MergedMetadata {
        fields: merged_fields,
        provenance,
    }
}

/// Apply merged metadata from all layers to an entity.
/// Call this after saving a layer to update the entity with merged values.
pub(crate) fn apply_merged_metadata_to_entity(db: &mut DbAny, node_id: DbId) -> anyhow::Result<()> {
    let layers = db::metadata::layers::get_for_entity(db, node_id)?;
    let providers = db::providers::get(db)?;
    let merged = merge_layers(layers, &providers);

    if let Some(mut release) = db::releases::get_by_id(db, node_id)? {
        let is_locked = release.locked.unwrap_or(false);
        if apply_to_release(&mut release, &merged) {
            db::releases::update(db, &release)?;
        }
        let genre_names = extract_genre_names(&merged);
        if !genre_names.is_empty() {
            db::genres::sync_release_genres(db, node_id, &genre_names)?;
        }
        // Locked releases skip the sync: a merge triggered by e.g. the dedup
        // path migrating a loser's layer onto a locked winner would wipe the
        // curated labels — the lock exists to prevent exactly that. Mirrors
        // the gate in `plugins/labels.rs::sync_for_release`.
        if !is_locked && let Some(label_inputs) = extract_label_inputs(&merged) {
            db::labels::sync_release_labels(db, node_id, &label_inputs)?;
        }
    } else if let Some(mut track) = db::tracks::get_by_id(db, node_id)? {
        if apply_to_track(&mut track, &merged) {
            db::tracks::update(db, &track)?;
        }
    } else if let Some(mut artist) = db::artists::get_by_id(db, node_id)? {
        if apply_to_artist(&mut artist, &merged) {
            db::artists::update(db, &artist)?;
        }
    }

    Ok(())
}

fn extract_genre_names(merged: &MergedMetadata) -> Vec<String> {
    merged
        .fields
        .get("genres")
        .and_then(|v| v.as_array())
        .map(|genres| {
            genres
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// Parse the merged `labels` field into `LabelInput`s.
///
/// Schema: JSON array of objects with required `name: String` and optional
/// `catalog_number: String` and `external_id: { provider_id, id_type,
/// id_value }` (all three inner fields required when present).
///
/// - `None` — field absent or any schema violation; callers skip the sync.
/// - `Some([])` — provider authoritatively asserts no labels; sync drops stale.
/// - `Some([..])` — authoritative replacement set.
///
/// Fail-closed on schema violation is deliberate: silently treating malformed
/// input as "zero labels" would wipe every ReleaseLabel and cascade-delete
/// orphan Labels on every ingestion.
fn extract_label_inputs(merged: &MergedMetadata) -> Option<Vec<db::labels::LabelInput>> {
    let raw = merged.fields.get("labels")?;
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

    if let Some(title) = merged.fields.get("release_title").and_then(|v| v.as_str()) {
        if release.release_title != title {
            release.release_title = title.to_string();
            changed = true;
        }
    }
    if let Some(sort_title) = merged.fields.get("sort_title").and_then(|v| v.as_str()) {
        if release.sort_title.as_deref() != Some(sort_title) {
            release.sort_title = Some(sort_title.to_string());
            changed = true;
        }
    }
    if let Some(release_type_str) = merged.fields.get("release_type").and_then(|v| v.as_str()) {
        let next = db::releases::ReleaseType::from_db_str(release_type_str)
            .ok()
            .map(Some)
            .unwrap_or(release.release_type);
        if release.release_type != next {
            release.release_type = next;
            changed = true;
        }
    }
    if let Some(release_date) = merged
        .fields
        .get("release_date")
        .and_then(|v| v.as_str())
        .and_then(db::releases::normalize_release_date)
    {
        let next = Some(release_date);
        if release.release_date != next {
            release.release_date = next;
            changed = true;
        }
    }

    changed
}

fn apply_to_track(track: &mut Track, merged: &MergedMetadata) -> bool {
    let mut changed = false;

    if let Some(title) = merged.fields.get("track_title").and_then(|v| v.as_str()) {
        if track.track_title != title {
            track.track_title = title.to_string();
            changed = true;
        }
    }
    if let Some(sort_title) = merged.fields.get("sort_title").and_then(|v| v.as_str()) {
        if track.sort_title.as_deref() != Some(sort_title) {
            track.sort_title = Some(sort_title.to_string());
            changed = true;
        }
    }
    if let Some(year) = merged.fields.get("year").and_then(|v| v.as_u64()) {
        let next = Some(year as u32);
        if track.year != next {
            track.year = next;
            changed = true;
        }
    }
    if let Some(disc) = merged.fields.get("disc").and_then(|v| v.as_u64()) {
        let next = Some(disc as u32);
        if track.disc != next {
            track.disc = next;
            changed = true;
        }
    }
    if let Some(disc_total) = merged.fields.get("disc_total").and_then(|v| v.as_u64()) {
        let next = Some(disc_total as u32);
        if track.disc_total != next {
            track.disc_total = next;
            changed = true;
        }
    }
    if let Some(track_num) = merged.fields.get("track").and_then(|v| v.as_u64()) {
        let next = Some(track_num as u32);
        if track.track != next {
            track.track = next;
            changed = true;
        }
    }
    if let Some(track_total) = merged.fields.get("track_total").and_then(|v| v.as_u64()) {
        let next = Some(track_total as u32);
        if track.track_total != next {
            track.track_total = next;
            changed = true;
        }
    }

    changed
}

fn apply_to_artist(artist: &mut Artist, merged: &MergedMetadata) -> bool {
    let mut changed = false;

    if let Some(name) = merged.fields.get("artist_name").and_then(|v| v.as_str()) {
        if artist.artist_name != name {
            artist.artist_name = name.to_string();
            changed = true;
        }
    }
    if let Some(at_str) = merged.fields.get("artist_type").and_then(|v| v.as_str()) {
        match crate::db::ArtistType::from_db_str(at_str) {
            Ok(at) => {
                if artist.artist_type != Some(at) {
                    artist.artist_type = Some(at);
                    changed = true;
                }
            }
            Err(_) => {
                tracing::warn!(
                    artist_type = at_str,
                    "ignoring unrecognized artist_type in merged metadata"
                );
            }
        }
    }
    if let Some(sort_name) = merged.fields.get("sort_name").and_then(|v| v.as_str()) {
        if artist.sort_name.as_deref() != Some(sort_name) {
            artist.sort_name = Some(sort_name.to_string());
            changed = true;
        }
    }
    if let Some(description) = merged.fields.get("description").and_then(|v| v.as_str()) {
        if artist.description.as_deref() != Some(description) {
            artist.description = Some(description.to_string());
            changed = true;
        }
    }

    changed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_layer(provider_id: &str, fields_json: &str, updated_at: u64) -> MetadataLayer {
        MetadataLayer {
            db_id: None,
            id: format!("layer-{provider_id}"),
            provider_id: provider_id.to_string(),
            fields: fields_json.to_string(),
            updated_at,
        }
    }

    fn make_provider(provider_id: &str, priority: u32, enabled: bool) -> ProviderConfig {
        ProviderConfig {
            db_id: None,
            id: format!("config-{provider_id}"),
            provider_id: provider_id.to_string(),
            display_name: provider_id.to_string(),
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
        }
    }

    #[test]
    fn extract_label_inputs_parses_full_shape() {
        let merged = merged_with_labels(serde_json::json!([
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
        ]));

        let inputs = extract_label_inputs(&merged).expect("valid shape parses");
        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[0].name, "Blue Note");
        assert_eq!(inputs[0].catalog_number.as_deref(), Some("BN-1577"));
        assert!(inputs[0].external_id.is_some());
        assert_eq!(inputs[1].name, "Impulse!");
        assert!(inputs[1].catalog_number.is_none());
        assert!(inputs[1].external_id.is_none());
    }

    #[test]
    fn extract_label_inputs_absent_returns_none() {
        let merged = MergedMetadata {
            fields: HashMap::new(),
            provenance: HashMap::new(),
        };
        assert!(extract_label_inputs(&merged).is_none());
    }

    #[test]
    fn extract_label_inputs_empty_array_returns_some_empty() {
        let merged = merged_with_labels(serde_json::json!([]));
        let inputs = extract_label_inputs(&merged).expect("empty array is legitimate");
        assert!(inputs.is_empty());
    }

    #[test]
    fn extract_label_inputs_wrong_top_level_shape_returns_none() {
        // Fail-closed: provider emits a string/object instead of an array.
        // Skipping the sync is the safe default — treating it as empty would
        // wipe every existing ReleaseLabel on ingestion.
        assert!(
            extract_label_inputs(&merged_with_labels(serde_json::json!("Blue Note"))).is_none()
        );
        assert!(
            extract_label_inputs(&merged_with_labels(
                serde_json::json!({"name": "Blue Note"})
            ))
            .is_none()
        );
        assert!(extract_label_inputs(&merged_with_labels(serde_json::json!(42))).is_none());
    }

    #[test]
    fn extract_label_inputs_malformed_entry_returns_none() {
        // Bare strings or entries missing `name` poison the whole batch.
        assert!(
            extract_label_inputs(&merged_with_labels(serde_json::json!(["Blue Note"]))).is_none()
        );
        assert!(
            extract_label_inputs(&merged_with_labels(
                serde_json::json!([{"catalog_number": "BN-1"}])
            ))
            .is_none()
        );
        assert!(
            extract_label_inputs(&merged_with_labels(serde_json::json!([{"name": "   "}])))
                .is_none()
        );
    }

    #[test]
    fn extract_label_inputs_partial_external_id_returns_none() {
        let merged = merged_with_labels(serde_json::json!([{
            "name": "Blue Note",
            "external_id": { "provider_id": "mb", "id_type": "label_id" }
        }]));
        assert!(extract_label_inputs(&merged).is_none());
    }

    #[test]
    fn extract_label_inputs_non_string_cat_number_returns_none() {
        // Fail-closed on non-string scalars: silently coercing to None would
        // overwrite an existing cat# with null on sync.
        assert!(
            extract_label_inputs(&merged_with_labels(serde_json::json!([
                {"name": "Blue Note", "catalog_number": 12345}
            ])))
            .is_none()
        );
        assert!(
            extract_label_inputs(&merged_with_labels(serde_json::json!([
                {"name": "Blue Note", "catalog_number": true}
            ])))
            .is_none()
        );
    }

    #[test]
    fn extract_label_inputs_blank_external_id_value_returns_none() {
        // Whitespace-only id_value passes is_empty but is not a real claim.
        // Accepting it would create a garbage ExternalId that blocks later
        // real enrichment via the "zero ext_ids" guard.
        let merged = merged_with_labels(serde_json::json!([{
            "name": "Blue Note",
            "external_id": {
                "provider_id": "mb",
                "id_type": "label_id",
                "id_value": "   "
            }
        }]));
        assert!(extract_label_inputs(&merged).is_none());
    }

    #[test]
    fn extract_label_inputs_trims_external_id_components() {
        let merged = merged_with_labels(serde_json::json!([{
            "name": "Blue Note",
            "external_id": {
                "provider_id": "  mb  ",
                "id_type": " label_id ",
                "id_value": " bn-001 "
            }
        }]));
        let inputs = extract_label_inputs(&merged).expect("trimmed non-empty values are valid");
        let ext = inputs[0].external_id.as_ref().expect("external_id present");
        assert_eq!(ext.provider_id, "mb");
        assert_eq!(ext.id_type, "label_id");
        assert_eq!(ext.id_value, "bn-001");
    }

    #[test]
    fn extract_label_inputs_null_external_id_is_ok() {
        let merged = merged_with_labels(serde_json::json!([
            {"name": "Blue Note", "external_id": null}
        ]));
        let inputs = extract_label_inputs(&merged).expect("null external_id is legitimate");
        assert_eq!(inputs.len(), 1);
        assert!(inputs[0].external_id.is_none());
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
}
