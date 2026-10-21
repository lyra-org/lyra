// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    BTreeMap,
    BTreeSet,
};

use agdb::DbId;
use serde_json::Value;

use crate::{
    db::{
        self,
        DbAccess,
        MetadataField,
    },
    services::metadata::{
        layers::MANUAL_SOURCE_ID,
        merging,
    },
};

use super::{
    MetadataEditingError,
    model::{
        FieldState,
        MetadataChangeRequest,
        MetadataCreditValue,
        MetadataEditOperation,
        MetadataFieldDiff,
        MetadataRelationValue,
        MetadataValueSource,
    },
    normalize::{
        layer_labels,
        normalized_set_value,
        validate_target,
    },
    state::EntityState,
};

fn cleared_value(state: &EntityState, field: MetadataField) -> Result<Value, MetadataEditingError> {
    if field.is_graph() {
        Ok(Value::Array(Vec::new()))
    } else if field.is_required() {
        // Required titles and names keep their stored values when unresolved.
        Ok(state.field_state(field)?.value.clone())
    } else {
        Ok(Value::Null)
    }
}

/// The value a field resolves to from every layer but the manual one.
fn inherited_value(
    db: &impl DbAccess,
    state: &EntityState,
    field: MetadataField,
) -> Result<Value, MetadataEditingError> {
    let merged = merging::resolve_without(db, state.db_id, MANUAL_SOURCE_ID)?;
    let artist_public_id = |artist_id: DbId| -> Result<String, MetadataEditingError> {
        Ok(db::artists::get_by_id(db, artist_id)?
            .ok_or_else(|| {
                MetadataEditingError::Internal(anyhow::anyhow!("a referenced artist disappeared"))
            })?
            .id)
    };
    let value = match field {
        MetadataField::Credits => {
            let mut credits = Vec::new();
            for credit_type in db::CreditType::ALL {
                for input in merged.credits.get(&credit_type).into_iter().flatten() {
                    credits.push(MetadataCreditValue {
                        artist_id: artist_public_id(input.artist_id)?,
                        credit_type,
                        detail: input.detail.clone(),
                    });
                }
            }
            serde_json::to_value(credits)?
        }
        MetadataField::Relations => {
            let mut relations = Vec::new();
            for relation_type in db::ArtistRelationType::ALL {
                for input in merged.relations.get(&relation_type).into_iter().flatten() {
                    relations.push(MetadataRelationValue {
                        artist_id: artist_public_id(input.artist_id)?,
                        relation_type,
                        attributes: input.attributes.clone(),
                    });
                }
            }
            serde_json::to_value(relations)?
        }
        field => {
            let Some(value) = merged.fields.get(&field).filter(|value| !value.is_null()) else {
                return cleared_value(state, field);
            };
            if field == MetadataField::Labels {
                // The winning layer's own labels, not ones that share their names.
                let source_id = merged.provenance.get(&field).ok_or_else(|| {
                    MetadataEditingError::Internal(anyhow::anyhow!(
                        "resolved labels have no source"
                    ))
                })?;
                let layer_id = db::metadata::layers::layer_id(db, state.db_id, source_id)?
                    .ok_or_else(|| {
                        MetadataEditingError::Internal(anyhow::anyhow!(
                            "the labels' layer disappeared"
                        ))
                    })?;
                return layer_labels(db, layer_id);
            }
            value.clone()
        }
    };
    normalized_set_value(db, state, field, &value)
}

pub(super) fn build_diff(
    db: &impl DbAccess,
    state: &EntityState,
    changes: &[MetadataChangeRequest],
) -> Result<Vec<MetadataFieldDiff>, MetadataEditingError> {
    if changes.is_empty() {
        return Err(MetadataEditingError::BadRequest(
            "no metadata changes provided".to_string(),
        ));
    }

    let mut fields = BTreeMap::new();
    for change in changes {
        if fields.contains_key(&change.field) {
            return Err(MetadataEditingError::BadRequest(format!(
                "duplicate metadata field '{}'",
                change.field.as_str(),
            )));
        }
        let before = state.field_state(change.field)?.clone();
        let (value, source) = match &change.edit {
            MetadataEditOperation::Set { value } => (
                normalized_set_value(db, state, change.field, value)?,
                MetadataValueSource::Manual,
            ),
            MetadataEditOperation::Inherit => {
                if before.source != MetadataValueSource::Manual {
                    return Err(MetadataEditingError::BadRequest(format!(
                        "field '{}' is not manually owned",
                        change.field.as_str(),
                    )));
                }
                let target = inherited_value(db, state, change.field)?;
                (target, MetadataValueSource::Resolved)
            }
        };
        fields.insert(
            change.field,
            MetadataFieldDiff {
                field: change.field,
                before,
                after: FieldState { value, source },
            },
        );
    }
    let diff: Vec<MetadataFieldDiff> = fields
        .into_values()
        .filter(|diff| diff.before != diff.after)
        .collect();
    validate_target(state, &diff)?;
    Ok(diff)
}

pub(super) fn check_expected(
    changes: &[MetadataChangeRequest],
    expected: &[MetadataFieldDiff],
    current: &[MetadataFieldDiff],
) -> Result<(), MetadataEditingError> {
    if expected.is_empty() {
        return Err(MetadataEditingError::BadRequest(
            "no expected metadata diff provided".to_string(),
        ));
    }
    let mut seen = BTreeSet::new();
    for diff in expected {
        if !seen.insert(diff.field) {
            return Err(MetadataEditingError::BadRequest(format!(
                "duplicate expected metadata field '{}'",
                diff.field.as_str(),
            )));
        }
        if !changes.iter().any(|change| change.field == diff.field) {
            return Err(MetadataEditingError::BadRequest(format!(
                "expected metadata field '{}' is not in changes",
                diff.field.as_str(),
            )));
        }
    }
    if expected.len() == current.len() && expected.iter().all(|diff| current.contains(diff)) {
        Ok(())
    } else {
        Err(MetadataEditingError::Conflict(current.to_vec()))
    }
}
