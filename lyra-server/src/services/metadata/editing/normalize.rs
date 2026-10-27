// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    BTreeMap,
    HashSet,
};

use agdb::DbId;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::db::{
    DbAccess,
    MetadataField,
    entities::MetadataEntityType,
};

use super::{
    MetadataEditingError,
    model::{
        MetadataCreditValue,
        MetadataFieldDiff,
        MetadataLabelEditValue,
        MetadataLabelValue,
        MetadataRelationValue,
    },
    state::EntityState,
};

fn normalize_nonempty_string(
    value: &Value,
    field: MetadataField,
) -> Result<String, MetadataEditingError> {
    let raw = value.as_str().ok_or_else(|| {
        MetadataEditingError::BadRequest(format!("field '{}' must be a string", field.as_str()))
    })?;
    let normalized = lyra_metadata::normalize_unicode_nfc(raw).trim().to_string();
    if normalized.is_empty() {
        return Err(MetadataEditingError::BadRequest(format!(
            "field '{}' cannot be empty",
            field.as_str(),
        )));
    }
    Ok(normalized)
}

fn decode_value<T: DeserializeOwned>(
    value: &Value,
    field: MetadataField,
) -> Result<T, MetadataEditingError> {
    serde_json::from_value(value.clone()).map_err(|error| {
        MetadataEditingError::BadRequest(format!(
            "invalid value for field '{}': {error}",
            field.as_str(),
        ))
    })
}

fn normalize_positive_u32(
    value: &Value,
    field: MetadataField,
    max: u32,
) -> Result<Value, MetadataEditingError> {
    let raw = value.as_u64().ok_or_else(|| {
        MetadataEditingError::BadRequest(format!(
            "field '{}' must be a positive integer",
            field.as_str(),
        ))
    })?;
    let number = u32::try_from(raw)
        .ok()
        .filter(|number| *number > 0 && *number <= max)
        .ok_or_else(|| {
            MetadataEditingError::BadRequest(format!(
                "field '{}' must be between 1 and {max}",
                field.as_str(),
            ))
        })?;
    Ok(Value::from(number))
}

fn normalize_genres(value: &Value) -> Result<Value, MetadataEditingError> {
    let raw: Vec<String> = decode_value(value, MetadataField::Genres)?;
    let mut seen = HashSet::new();
    let mut normalized = Vec::with_capacity(raw.len());
    for name in raw {
        let name = lyra_metadata::normalize_unicode_nfc(&name)
            .trim()
            .to_string();
        if name.is_empty() {
            return Err(MetadataEditingError::BadRequest(
                "genre names cannot be empty".to_string(),
            ));
        }
        let key = name.to_lowercase();
        if !seen.insert(key) {
            return Err(MetadataEditingError::BadRequest(format!(
                "duplicate genre '{name}'",
            )));
        }
        normalized.push(name);
    }
    normalized.sort_by_key(|name| name.to_lowercase());
    Ok(serde_json::to_value(normalized)?)
}

fn normalized_catalog_number(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = lyra_metadata::normalize_unicode_nfc(&value)
            .trim()
            .to_string();
        (!value.is_empty()).then_some(value)
    })
}

fn finish_normalized_labels(
    mut labels: Vec<MetadataLabelValue>,
) -> Result<Value, MetadataEditingError> {
    let mut seen = HashSet::new();
    for label in &labels {
        if !seen.insert(label.id.clone()) {
            return Err(MetadataEditingError::BadRequest(format!(
                "duplicate label '{}'",
                label.id,
            )));
        }
    }
    labels.sort_by(|a, b| {
        crate::db::labels::normalize_label_name(&a.name)
            .cmp(&crate::db::labels::normalize_label_name(&b.name))
            .then_with(|| a.id.cmp(&b.id))
            .then_with(|| a.catalog_number.cmp(&b.catalog_number))
    });
    Ok(serde_json::to_value(labels)?)
}

fn normalize_label_edits(db: &impl DbAccess, value: &Value) -> Result<Value, MetadataEditingError> {
    let raw: Vec<MetadataLabelEditValue> = decode_value(value, MetadataField::Labels)?;
    let mut labels = Vec::with_capacity(raw.len());
    for label in raw {
        let id = label.id.trim().to_string();
        if id.is_empty() {
            return Err(MetadataEditingError::BadRequest(
                "label IDs cannot be empty".to_string(),
            ));
        }
        let label_id = crate::db::lookup::find_node_id_by_id(db, &id)?
            .ok_or_else(|| MetadataEditingError::BadRequest(format!("unknown label '{id}'")))?;
        let stored = crate::db::labels::get_by_id(db, label_id)?
            .ok_or_else(|| MetadataEditingError::BadRequest(format!("unknown label '{id}'")))?;
        labels.push(MetadataLabelValue {
            id,
            name: stored.name,
            catalog_number: normalized_catalog_number(label.catalog_number),
        });
    }
    finish_normalized_labels(labels)
}

/// The labels a provider layer links, as the editing value of an inherited field.
pub(super) fn layer_labels(
    db: &impl DbAccess,
    layer_id: DbId,
) -> Result<Value, MetadataEditingError> {
    let mut labels = Vec::new();
    for link in crate::db::metadata::layers::edges::labels(db, layer_id)? {
        let stored = crate::db::labels::get_by_id(db, link.target_id)?.ok_or_else(|| {
            MetadataEditingError::Internal(anyhow::anyhow!("a layer's label disappeared"))
        })?;
        labels.push(MetadataLabelValue {
            id: stored.id,
            name: stored.name,
            catalog_number: normalized_catalog_number(link.edge.catalog_number),
        });
    }
    finish_normalized_labels(labels)
}

fn require_artist_id(
    db: &impl DbAccess,
    public_id: &str,
    field: MetadataField,
) -> Result<DbId, MetadataEditingError> {
    let unknown = || {
        MetadataEditingError::BadRequest(format!(
            "field '{}' references unknown artist '{}'",
            field.as_str(),
            public_id,
        ))
    };
    let db_id = crate::db::lookup::find_node_id_by_id(db, public_id)?.ok_or_else(unknown)?;
    if crate::db::artists::get_by_id(db, db_id)?.is_none() {
        return Err(unknown());
    }
    Ok(db_id)
}

fn normalize_credits(db: &impl DbAccess, value: &Value) -> Result<Value, MetadataEditingError> {
    let raw: Vec<MetadataCreditValue> = decode_value(value, MetadataField::Credits)?;
    let mut seen = HashSet::new();
    let mut normalized = Vec::with_capacity(raw.len());
    for credit in raw {
        let artist_id = credit.artist_id.trim().to_string();
        require_artist_id(db, &artist_id, MetadataField::Credits)?;
        let detail = credit.detail.and_then(|value| {
            let value = lyra_metadata::normalize_unicode_nfc(&value)
                .trim()
                .to_string();
            (!value.is_empty()).then_some(value)
        });
        let key = (
            artist_id.clone(),
            credit.credit_type.to_string(),
            detail.clone(),
        );
        if !seen.insert(key) {
            return Err(MetadataEditingError::BadRequest(format!(
                "duplicate credit for artist '{artist_id}'",
            )));
        }
        normalized.push(MetadataCreditValue {
            artist_id,
            credit_type: credit.credit_type,
            detail,
        });
    }
    // Credits resolve role by role, so they apply in role order.
    normalized.sort_by_key(|credit| {
        crate::db::CreditType::ALL
            .iter()
            .position(|credit_type| *credit_type == credit.credit_type)
    });
    Ok(serde_json::to_value(normalized)?)
}

fn normalize_relations(
    db: &impl DbAccess,
    artist_id: DbId,
    value: &Value,
) -> Result<Value, MetadataEditingError> {
    let raw: Vec<MetadataRelationValue> = decode_value(value, MetadataField::Relations)?;
    let mut seen = HashSet::new();
    let mut normalized = Vec::with_capacity(raw.len());
    for relation in raw {
        let related_id = relation.artist_id.trim().to_string();
        let related_db_id = require_artist_id(db, &related_id, MetadataField::Relations)?;
        if related_db_id == artist_id {
            return Err(MetadataEditingError::BadRequest(
                "an artist cannot relate to itself".to_string(),
            ));
        }
        let key = (related_id.clone(), relation.relation_type.to_string());
        if !seen.insert(key) {
            return Err(MetadataEditingError::BadRequest(format!(
                "duplicate relation from artist '{related_id}'",
            )));
        }
        let attributes = relation.attributes.and_then(|value| {
            let value = lyra_metadata::normalize_unicode_nfc(&value)
                .trim()
                .to_string();
            (!value.is_empty()).then_some(value)
        });
        normalized.push(MetadataRelationValue {
            artist_id: related_id,
            relation_type: relation.relation_type,
            attributes,
        });
    }
    sort_relations(&mut normalized);
    Ok(serde_json::to_value(normalized)?)
}

pub(super) fn sort_relations(relations: &mut [MetadataRelationValue]) {
    relations.sort_by(|a, b| {
        a.artist_id
            .cmp(&b.artist_id)
            .then_with(|| {
                a.relation_type
                    .to_string()
                    .cmp(&b.relation_type.to_string())
            })
            .then_with(|| a.attributes.cmp(&b.attributes))
    });
}

pub(super) fn normalized_set_value(
    db: &impl DbAccess,
    state: &EntityState,
    field: MetadataField,
    value: &Value,
) -> Result<Value, MetadataEditingError> {
    if value.is_null() {
        return if field.is_required() {
            Err(MetadataEditingError::BadRequest(format!(
                "field '{}' cannot be cleared",
                field.as_str(),
            )))
        } else if field.is_graph() {
            Err(MetadataEditingError::BadRequest(format!(
                "field '{}' must be a list; use [] to clear it",
                field.as_str(),
            )))
        } else {
            Ok(Value::Null)
        };
    }
    match field {
        MetadataField::ReleaseTitle
        | MetadataField::TrackTitle
        | MetadataField::ArtistName
        | MetadataField::SortTitle
        | MetadataField::SortName
        | MetadataField::Description => Ok(Value::String(normalize_nonempty_string(value, field)?)),
        MetadataField::ReleaseType => {
            let value: crate::db::releases::ReleaseType = decode_value(value, field)?;
            Ok(serde_json::to_value(value)?)
        }
        MetadataField::ReleaseDate => {
            let value = normalize_nonempty_string(value, field)?;
            let normalized =
                crate::db::releases::normalize_release_date(&value).ok_or_else(|| {
                    MetadataEditingError::BadRequest(
                        "release_date must use YYYY, YYYY-MM, or YYYY-MM-DD".to_string(),
                    )
                })?;
            Ok(Value::String(normalized))
        }
        MetadataField::Genres => normalize_genres(value),
        MetadataField::Labels => normalize_label_edits(db, value),
        MetadataField::Credits => normalize_credits(db, value),
        MetadataField::Year => normalize_positive_u32(value, field, 9999),
        MetadataField::Disc
        | MetadataField::DiscTotal
        | MetadataField::Track
        | MetadataField::TrackTotal => normalize_positive_u32(value, field, u32::MAX),
        MetadataField::ArtistType => {
            let value: crate::db::ArtistType = decode_value(value, field)?;
            Ok(serde_json::to_value(value)?)
        }
        MetadataField::Relations => normalize_relations(db, state.db_id, value),
    }
}

fn optional_u32(fields: &BTreeMap<MetadataField, Value>, field: MetadataField) -> Option<u32> {
    fields
        .get(&field)
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
}

pub(super) fn validate_target(
    state: &EntityState,
    diff: &[MetadataFieldDiff],
) -> Result<(), MetadataEditingError> {
    if state.entity_type != MetadataEntityType::Track {
        return Ok(());
    }
    let mut fields: BTreeMap<MetadataField, Value> = state
        .fields
        .iter()
        .map(|(field, state)| (*field, state.value.clone()))
        .collect();
    for entry in diff {
        fields.insert(entry.field, entry.after.value.clone());
    }
    if let (Some(disc), Some(total)) = (
        optional_u32(&fields, MetadataField::Disc),
        optional_u32(&fields, MetadataField::DiscTotal),
    ) && disc > total
    {
        return Err(MetadataEditingError::BadRequest(
            "disc cannot exceed disc_total".to_string(),
        ));
    }
    if let (Some(track), Some(total)) = (
        optional_u32(&fields, MetadataField::Track),
        optional_u32(&fields, MetadataField::TrackTotal),
    ) && track > total
    {
        return Err(MetadataEditingError::BadRequest(
            "track cannot exceed track_total".to_string(),
        ));
    }
    Ok(())
}
