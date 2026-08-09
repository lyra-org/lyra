// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod apply;
pub(crate) mod model;
mod normalize;
mod plan;
mod previews;
mod state;

use std::collections::BTreeMap;

use agdb::{
    DbAny,
    DbId,
};
use serde_json::Value;

use crate::services::auth::Principal;

use apply::{
    apply_plan,
    prepare_references,
};
use normalize::validate_target;

use model::{
    MetadataApplyRequest,
    MetadataEntityType,
    MetadataField,
    MetadataFieldConflict,
    MetadataPreviewRequest,
    MetadataPreviewResponse,
    MetadataSnapshot,
};
use plan::{
    build_plan,
    check_inherited_preconditions,
    check_preconditions,
    plan_diff,
};
use state::load_entity_state;

#[derive(Debug, thiserror::Error)]
pub(crate) enum MetadataEditingError {
    #[error("{0}")]
    BadRequest(String),
    #[error("entity not found: {0}")]
    EntityNotFound(String),
    #[error("metadata changed after preview")]
    Conflict(Vec<MetadataFieldConflict>),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<serde_json::Error> for MetadataEditingError {
    fn from(error: serde_json::Error) -> Self {
        Self::Internal(error.into())
    }
}

struct EditPlan {
    entity_type: MetadataEntityType,
    fields: BTreeMap<MetadataField, PlannedField>,
}

struct PlannedField {
    before: Value,
    source_before: model::MetadataValueSource,
    after: Value,
    source_after: model::MetadataValueSource,
}

pub(crate) fn get_snapshot(
    db: &DbAny,
    principal: &Principal,
    db_id: DbId,
) -> Result<MetadataSnapshot, MetadataEditingError> {
    Ok(load_entity_state(db, principal, db_id)?.response())
}

pub(crate) fn preview(
    db: &DbAny,
    principal: &Principal,
    db_id: DbId,
    request: &MetadataPreviewRequest,
) -> Result<MetadataPreviewResponse, MetadataEditingError> {
    let state = load_entity_state(db, principal, db_id)?;
    let plan = build_plan(db, principal, &state, &request.changes)?;
    let diff = plan_diff(&plan);
    if diff.is_empty() {
        return Err(MetadataEditingError::BadRequest(
            "metadata edit has no effect".to_string(),
        ));
    }
    let preview_id = previews::issue(&principal.user_public_id, &state.public_id, plan)?;
    Ok(MetadataPreviewResponse { preview_id, diff })
}

pub(crate) fn apply(
    db: &mut DbAny,
    principal: &Principal,
    db_id: DbId,
    request: &MetadataApplyRequest,
) -> Result<MetadataSnapshot, MetadataEditingError> {
    let state = load_entity_state(db, principal, db_id)?;
    let plan = previews::take(
        &request.preview_id,
        &principal.user_public_id,
        &state.public_id,
    )?;
    if state.entity_type != plan.entity_type {
        return Err(MetadataEditingError::BadRequest(
            "preview_id no longer matches the entity type".to_string(),
        ));
    }
    let targets = plan::targets(&plan);
    check_preconditions(&state, &plan)?;
    check_inherited_preconditions(db, principal, &state, &plan)?;
    validate_target(&state, &targets)?;
    let references = prepare_references(db, principal, &state, &plan)?;
    apply_plan(db, &state, &plan, &references)?;
    Ok(load_entity_state(db, principal, db_id)?.response())
}

#[cfg(test)]
mod tests;
