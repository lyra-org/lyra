// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    BTreeMap,
    BTreeSet,
};

use serde_json::Value;

use crate::{
    db::{
        self,
        DbAccess,
    },
    services::auth::Principal,
};

use super::{
    MetadataEditingError,
    model::{
        FieldState,
        MetadataChangeRequest,
        MetadataEditOperation,
        MetadataField,
        MetadataFieldDiff,
        MetadataValueSource,
    },
    normalize::{
        normalize_provider_labels,
        normalized_set_value,
        validate_target,
    },
    state::{
        EntityState,
        internal_field_name,
    },
};

fn provider_value(
    db: &impl DbAccess,
    state: &EntityState,
    field: MetadataField,
) -> Result<Option<Value>, MetadataEditingError> {
    let internal_field = internal_field_name(state.entity_type, field)
        .ok_or_else(|| MetadataEditingError::BadRequest("invalid metadata field".to_string()))?;
    let layers = db::metadata::layers::get_for_entity(db, state.db_id)?;
    let providers = db::providers::get(db)?;
    let merged = crate::services::metadata::merging::merge_layers(layers, &providers);
    Ok(merged.fields.get(internal_field.as_str()).cloned())
}

fn inherited_value(
    db: &impl DbAccess,
    principal: &Principal,
    state: &EntityState,
    field: MetadataField,
    provider_value: Option<&Value>,
) -> Result<Value, MetadataEditingError> {
    let Some(value) = provider_value else {
        if matches!(
            field,
            MetadataField::Genres
                | MetadataField::Labels
                | MetadataField::Credits
                | MetadataField::Relations
        ) {
            return Ok(Value::Array(Vec::new()));
        }
        return Ok(state.field_state(field)?.value.clone());
    };
    if field == MetadataField::Labels {
        normalize_provider_labels(db, value)
    } else {
        normalized_set_value(db, principal, state, field, value)
    }
}

pub(super) fn build_diff(
    db: &impl DbAccess,
    principal: &Principal,
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
                normalized_set_value(db, principal, state, change.field, value)?,
                MetadataValueSource::Manual,
            ),
            MetadataEditOperation::Inherit => {
                if before.source != MetadataValueSource::Manual {
                    return Err(MetadataEditingError::BadRequest(format!(
                        "field '{}' is not manually owned",
                        change.field.as_str(),
                    )));
                }
                let provider_value = provider_value(db, state, change.field)?;
                let target =
                    inherited_value(db, principal, state, change.field, provider_value.as_ref())?;
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
