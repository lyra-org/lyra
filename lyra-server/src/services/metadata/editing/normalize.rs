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

use crate::{
    db::{
        self,
        DbAccess,
    },
    services::auth::{
        Principal,
        access,
    },
};

use super::{
    MetadataEditingError,
    model::{
        MetadataCreditValue,
        MetadataEntityType,
        MetadataField,
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

fn label_accessible(
    db: &impl DbAccess,
    principal: &Principal,
    label_id: DbId,
) -> Result<bool, MetadataEditingError> {
    let release_ids = db::labels::get_releases_with_catalog(db, label_id)?
        .into_iter()
        .map(|(release_id, _)| release_id)
        .collect::<Vec<_>>();
    Ok(!access::accessible_entities(db, principal, &release_ids)?.is_empty())
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
        db::labels::normalize_label_name(&a.name)
            .cmp(&db::labels::normalize_label_name(&b.name))
            .then_with(|| a.id.cmp(&b.id))
            .then_with(|| a.catalog_number.cmp(&b.catalog_number))
    });
    Ok(serde_json::to_value(labels)?)
}

pub(super) fn normalize_label_edits(
    db: &impl DbAccess,
    principal: &Principal,
    value: &Value,
) -> Result<Value, MetadataEditingError> {
    let raw: Vec<MetadataLabelEditValue> = decode_value(value, MetadataField::Labels)?;
    let mut labels = Vec::with_capacity(raw.len());
    for label in raw {
        let id = label.id.trim().to_string();
        if id.is_empty() {
            return Err(MetadataEditingError::BadRequest(
                "label IDs cannot be empty".to_string(),
            ));
        }
        let label_id = db::lookup::find_node_id_by_id(db, &id)?
            .ok_or_else(|| MetadataEditingError::BadRequest(format!("unknown label '{id}'")))?;
        let stored = db::labels::get_by_id(db, label_id)?
            .ok_or_else(|| MetadataEditingError::BadRequest(format!("unknown label '{id}'")))?;
        if !label_accessible(db, principal, label_id)? {
            return Err(MetadataEditingError::BadRequest(format!(
                "unknown label '{id}'"
            )));
        }
        labels.push(MetadataLabelValue {
            id,
            name: stored.name,
            catalog_number: normalized_catalog_number(label.catalog_number),
        });
    }
    finish_normalized_labels(labels)
}

pub(super) fn normalize_provider_labels(
    db: &impl DbAccess,
    value: &Value,
) -> Result<Value, MetadataEditingError> {
    let inputs =
        crate::services::metadata::merging::parse_label_inputs(value).ok_or_else(|| {
            MetadataEditingError::BadRequest(
                "resolved provider labels have an invalid schema".to_string(),
            )
        })?;
    let mut labels = Vec::with_capacity(inputs.len());
    let mut external_keys = HashSet::new();
    let mut name_keys = HashSet::new();
    for input in inputs {
        let label_id = if let Some(external_id) = &input.external_id {
            let key = (
                external_id.provider_id.clone(),
                external_id.id_type.clone(),
                external_id.id_value.clone(),
            );
            if !external_keys.insert(key) {
                return Err(MetadataEditingError::BadRequest(
                    "resolved provider labels contain a duplicate identity".to_string(),
                ));
            }
            db::labels::find_by_external_id(
                db,
                &external_id.provider_id,
                &external_id.id_type,
                &external_id.id_value,
            )?
        } else {
            let key = db::labels::normalize_label_name(&input.name);
            if !name_keys.insert(key) {
                return Err(MetadataEditingError::BadRequest(
                    "resolved provider labels contain a duplicate identity".to_string(),
                ));
            }
            db::labels::find_by_name(db, &input.name)?
        };
        let label_id = label_id.ok_or_else(|| {
            MetadataEditingError::BadRequest(format!(
                "resolved provider label '{}' is not present locally; resolve it before inheriting labels",
                input.name,
            ))
        })?;
        let stored = db::labels::get_by_id(db, label_id)?.ok_or_else(|| {
            MetadataEditingError::Internal(anyhow::anyhow!("resolved provider label disappeared"))
        })?;
        labels.push(MetadataLabelValue {
            id: stored.id,
            name: stored.name,
            catalog_number: normalized_catalog_number(input.catalog_number),
        });
    }
    finish_normalized_labels(labels)
}

fn require_artist_id(
    db: &impl DbAccess,
    principal: &Principal,
    public_id: &str,
    field: MetadataField,
) -> Result<DbId, MetadataEditingError> {
    let db_id = db::lookup::find_node_id_by_id(db, public_id)?.ok_or_else(|| {
        MetadataEditingError::BadRequest(format!(
            "field '{}' references unknown artist '{}'",
            field.as_str(),
            public_id,
        ))
    })?;
    if db::artists::get_by_id(db, db_id)?.is_none() {
        return Err(MetadataEditingError::BadRequest(format!(
            "field '{}' reference '{}' is not an artist",
            field.as_str(),
            public_id,
        )));
    }
    if !access::artist_accessible(db, principal, db_id)? {
        return Err(MetadataEditingError::BadRequest(format!(
            "field '{}' references unknown artist '{}'",
            field.as_str(),
            public_id,
        )));
    }
    Ok(db_id)
}

fn normalize_credits(
    db: &impl DbAccess,
    principal: &Principal,
    value: &Value,
) -> Result<Value, MetadataEditingError> {
    let raw: Vec<MetadataCreditValue> = decode_value(value, MetadataField::Credits)?;
    let mut seen = HashSet::new();
    let mut normalized = Vec::with_capacity(raw.len());
    for credit in raw {
        let artist_id = credit.artist_id.trim().to_string();
        require_artist_id(db, principal, &artist_id, MetadataField::Credits)?;
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
    Ok(serde_json::to_value(normalized)?)
}

fn normalize_relations(
    db: &impl DbAccess,
    principal: &Principal,
    source_artist_id: DbId,
    value: &Value,
) -> Result<Value, MetadataEditingError> {
    let raw: Vec<MetadataRelationValue> = decode_value(value, MetadataField::Relations)?;
    let mut seen = HashSet::new();
    let mut normalized = Vec::with_capacity(raw.len());
    for relation in raw {
        let target_artist_id = relation.target_artist_id.trim().to_string();
        let target_db_id =
            require_artist_id(db, principal, &target_artist_id, MetadataField::Relations)?;
        if target_db_id == source_artist_id {
            return Err(MetadataEditingError::BadRequest(
                "an artist cannot relate to itself".to_string(),
            ));
        }
        let key = (target_artist_id.clone(), relation.relation_type.to_string());
        if !seen.insert(key) {
            return Err(MetadataEditingError::BadRequest(format!(
                "duplicate relation to artist '{target_artist_id}'",
            )));
        }
        let attributes = relation.attributes.and_then(|value| {
            let value = lyra_metadata::normalize_unicode_nfc(&value)
                .trim()
                .to_string();
            (!value.is_empty()).then_some(value)
        });
        normalized.push(MetadataRelationValue {
            target_artist_id,
            relation_type: relation.relation_type,
            attributes,
        });
    }
    normalized.sort_by(|a, b| {
        a.target_artist_id
            .cmp(&b.target_artist_id)
            .then_with(|| {
                a.relation_type
                    .to_string()
                    .cmp(&b.relation_type.to_string())
            })
            .then_with(|| a.attributes.cmp(&b.attributes))
    });
    Ok(serde_json::to_value(normalized)?)
}

pub(super) fn normalized_set_value(
    db: &impl DbAccess,
    principal: &Principal,
    state: &EntityState,
    field: MetadataField,
    value: &Value,
) -> Result<Value, MetadataEditingError> {
    if value.is_null() {
        return match field {
            MetadataField::Title | MetadataField::Name => Err(MetadataEditingError::BadRequest(
                format!("field '{}' cannot be cleared", field.as_str()),
            )),
            MetadataField::Genres
            | MetadataField::Labels
            | MetadataField::Credits
            | MetadataField::Relations => Err(MetadataEditingError::BadRequest(format!(
                "field '{}' must be a list; use [] to clear it",
                field.as_str(),
            ))),
            _ => Ok(Value::Null),
        };
    }
    match field {
        MetadataField::Title | MetadataField::Name => {
            Ok(Value::String(normalize_nonempty_string(value, field)?))
        }
        MetadataField::SortTitle | MetadataField::SortName | MetadataField::Description => {
            Ok(Value::String(normalize_nonempty_string(value, field)?))
        }
        MetadataField::ReleaseType => {
            let value: db::releases::ReleaseType = decode_value(value, field)?;
            Ok(serde_json::to_value(value)?)
        }
        MetadataField::ReleaseDate => {
            let value = normalize_nonempty_string(value, field)?;
            let normalized = db::releases::normalize_release_date(&value).ok_or_else(|| {
                MetadataEditingError::BadRequest(
                    "release_date must use YYYY, YYYY-MM, or YYYY-MM-DD".to_string(),
                )
            })?;
            Ok(Value::String(normalized))
        }
        MetadataField::Genres => normalize_genres(value),
        MetadataField::Labels => Err(MetadataEditingError::Internal(anyhow::anyhow!(
            "label edits require identity-aware normalization"
        ))),
        MetadataField::Credits => normalize_credits(db, principal, value),
        MetadataField::Year => normalize_positive_u32(value, field, 9999),
        MetadataField::Disc
        | MetadataField::DiscTotal
        | MetadataField::Track
        | MetadataField::TrackTotal => normalize_positive_u32(value, field, u32::MAX),
        MetadataField::ArtistType => {
            let value: db::ArtistType = decode_value(value, field)?;
            Ok(serde_json::to_value(value)?)
        }
        MetadataField::Relations => normalize_relations(db, principal, state.db_id, value),
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
    targets: &BTreeMap<MetadataField, Value>,
) -> Result<(), MetadataEditingError> {
    if state.entity_type != MetadataEntityType::Track {
        return Ok(());
    }
    let mut fields: BTreeMap<MetadataField, Value> = state
        .fields
        .iter()
        .map(|(field, state)| (*field, state.value.clone()))
        .collect();
    for (field, value) in targets {
        fields.insert(*field, value.clone());
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
