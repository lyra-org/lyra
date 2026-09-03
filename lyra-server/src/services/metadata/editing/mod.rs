// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod apply;
pub(crate) mod model;
mod normalize;
mod plan;
mod state;

use agdb::{
    DbAny,
    DbId,
};

use crate::services::auth::Principal;

use apply::{
    apply_plan,
    prepare_references,
};
use model::{
    MetadataApplyRequest,
    MetadataFieldDiff,
    MetadataPreviewRequest,
    MetadataSnapshot,
};
use plan::{
    build_plan,
    check_expected,
};
use state::load_entity_state;

#[derive(Debug, thiserror::Error)]
pub(crate) enum MetadataEditingError {
    #[error("{0}")]
    BadRequest(String),
    #[error("entity not found: {0}")]
    EntityNotFound(String),
    #[error("metadata changed after preview")]
    Conflict(Vec<MetadataFieldDiff>),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<serde_json::Error> for MetadataEditingError {
    fn from(error: serde_json::Error) -> Self {
        Self::Internal(error.into())
    }
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
) -> Result<Vec<MetadataFieldDiff>, MetadataEditingError> {
    let state = load_entity_state(db, principal, db_id)?;
    let diff = build_plan(db, principal, &state, &request.changes)?;
    if diff.is_empty() {
        return Err(MetadataEditingError::BadRequest(
            "metadata edit has no effect".to_string(),
        ));
    }
    Ok(diff)
}

pub(crate) fn apply(
    db: &mut DbAny,
    principal: &Principal,
    db_id: DbId,
    request: &MetadataApplyRequest,
) -> Result<MetadataSnapshot, MetadataEditingError> {
    let state = load_entity_state(db, principal, db_id)?;
    let plan = build_plan(db, principal, &state, &request.changes)?;
    check_expected(&request.changes, &request.expected, &plan)?;
    let references = prepare_references(db, &plan)?;
    apply_plan(db, &state, &plan, &references)?;
    Ok(load_entity_state(db, principal, db_id)?.response())
}

#[cfg(test)]
mod tests;
