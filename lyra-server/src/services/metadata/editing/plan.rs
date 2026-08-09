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
    EditPlan,
    MetadataEditingError,
    PlannedField,
    model::{
        MetadataChangeRequest,
        MetadataEditOperation,
        MetadataField,
        MetadataFieldConflict,
        MetadataFieldDiff,
        MetadataValueSource,
    },
    normalize::{
        normalize_label_edits,
        normalize_provider_labels,
        normalized_clear_value,
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
            return normalized_clear_value(field);
        }
        return Ok(state.value(field)?.clone());
    };
    if field == MetadataField::Labels {
        normalize_provider_labels(db, value)
    } else {
        normalized_set_value(db, principal, state, field, value)
    }
}

pub(super) fn build_plan(
    db: &impl DbAccess,
    principal: &Principal,
    state: &EntityState,
    changes: &[MetadataChangeRequest],
) -> Result<EditPlan, MetadataEditingError> {
    if changes.is_empty() {
        return Err(MetadataEditingError::BadRequest(
            "no metadata changes provided".to_string(),
        ));
    }

    let mut seen = BTreeSet::new();
    let mut targets = BTreeMap::new();
    let mut fields = BTreeMap::new();
    for change in changes {
        if !seen.insert(change.field) {
            return Err(MetadataEditingError::BadRequest(format!(
                "duplicate metadata field '{}'",
                change.field.as_str(),
            )));
        }
        state.value(change.field)?;
        let (after, source_after) = match &change.edit {
            MetadataEditOperation::Set { value } if change.field == MetadataField::Labels => (
                normalize_label_edits(db, principal, value)?,
                MetadataValueSource::Manual,
            ),
            MetadataEditOperation::Set { value } => (
                normalized_set_value(db, principal, state, change.field, value)?,
                MetadataValueSource::Manual,
            ),
            MetadataEditOperation::Clear => (
                normalized_clear_value(change.field)?,
                MetadataValueSource::Manual,
            ),
            MetadataEditOperation::Inherit => {
                if !state.manual_fields.contains(&change.field) {
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
        targets.insert(change.field, after.clone());
        fields.insert(
            change.field,
            PlannedField {
                before: state.value(change.field)?.clone(),
                source_before: state.source(change.field),
                after,
                source_after,
            },
        );
    }
    validate_target(state, &targets)?;

    Ok(EditPlan {
        entity_type: state.entity_type,
        fields,
    })
}

pub(super) fn targets(plan: &EditPlan) -> BTreeMap<MetadataField, Value> {
    plan.fields
        .iter()
        .map(|(field, planned)| (*field, planned.after.clone()))
        .collect()
}

pub(super) fn inherited_fields(plan: &EditPlan) -> BTreeSet<MetadataField> {
    plan.fields
        .iter()
        .filter_map(|(field, planned)| {
            (planned.source_after == MetadataValueSource::Resolved).then_some(*field)
        })
        .collect()
}

pub(super) fn plan_diff(plan: &EditPlan) -> Vec<MetadataFieldDiff> {
    let mut diff = Vec::with_capacity(plan.fields.len());
    for (field, planned) in &plan.fields {
        if planned.before != planned.after || planned.source_before != planned.source_after {
            diff.push(MetadataFieldDiff {
                field: *field,
                before: planned.before.clone(),
                after: planned.after.clone(),
                source_before: planned.source_before,
                source_after: planned.source_after,
            });
        }
    }
    diff
}

pub(super) fn check_preconditions(
    state: &EntityState,
    plan: &EditPlan,
) -> Result<(), MetadataEditingError> {
    let mut conflicts = Vec::new();
    for (field, planned) in &plan.fields {
        let Some(current) = state.fields.get(field).cloned() else {
            conflicts.push(MetadataFieldConflict {
                field: *field,
                expected: planned.before.clone(),
                current: Value::Null,
                expected_source: planned.source_before,
                current_source: state.source(*field),
            });
            continue;
        };
        let current_source = state.source(*field);
        if planned.before != current || planned.source_before != current_source {
            conflicts.push(MetadataFieldConflict {
                field: *field,
                expected: planned.before.clone(),
                current,
                expected_source: planned.source_before,
                current_source,
            });
        }
    }
    if conflicts.is_empty() {
        Ok(())
    } else {
        Err(MetadataEditingError::Conflict(conflicts))
    }
}

pub(super) fn check_inherited_preconditions(
    db: &impl DbAccess,
    principal: &Principal,
    state: &EntityState,
    plan: &EditPlan,
) -> Result<(), MetadataEditingError> {
    let mut conflicts = Vec::new();
    for (field, planned) in &plan.fields {
        if planned.source_after != MetadataValueSource::Resolved {
            continue;
        }
        let current_provider_value = provider_value(db, state, *field)?;
        let current = match inherited_value(
            db,
            principal,
            state,
            *field,
            current_provider_value.as_ref(),
        ) {
            Ok(current) => current,
            Err(MetadataEditingError::BadRequest(_)) => {
                conflicts.push(MetadataFieldConflict {
                    field: *field,
                    expected: planned.after.clone(),
                    current: Value::Null,
                    expected_source: MetadataValueSource::Resolved,
                    current_source: MetadataValueSource::Resolved,
                });
                continue;
            }
            Err(error) => return Err(error),
        };
        if planned.after != current {
            conflicts.push(MetadataFieldConflict {
                field: *field,
                expected: planned.after.clone(),
                current,
                expected_source: MetadataValueSource::Resolved,
                current_source: MetadataValueSource::Resolved,
            });
        }
    }
    if conflicts.is_empty() {
        Ok(())
    } else {
        Err(MetadataEditingError::Conflict(conflicts))
    }
}
