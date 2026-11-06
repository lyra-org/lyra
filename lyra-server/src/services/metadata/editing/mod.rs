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
    DbAnyTransactionMut,
    DbError,
    DbId,
};
use lyra_db::DbAccess;

use apply::apply_inside_tx;
use model::{
    MetadataChangeRequest,
    MetadataFieldDiff,
    MetadataSnapshot,
};
use plan::{
    build_diff,
    check_expected,
    check_inherits_manual,
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

impl From<DbError> for MetadataEditingError {
    fn from(error: DbError) -> Self {
        Self::Internal(error.into())
    }
}

pub(crate) fn get_snapshot(
    db: &DbAny,
    db_id: DbId,
) -> Result<MetadataSnapshot, MetadataEditingError> {
    Ok(load_entity_state(db, db_id)?.response())
}

pub(crate) fn preview(
    db: &impl DbAccess,
    db_id: DbId,
    changes: &[MetadataChangeRequest],
) -> Result<Vec<MetadataFieldDiff>, MetadataEditingError> {
    let state = load_entity_state(db, db_id)?;
    let diff = build_diff(db, &state, changes)?;
    check_inherits_manual(&state, changes)?;
    if diff.is_empty() {
        return Err(MetadataEditingError::BadRequest(
            "metadata edit has no effect".to_string(),
        ));
    }
    Ok(diff)
}

pub(crate) fn apply(
    t: &mut DbAnyTransactionMut<'_>,
    db_id: DbId,
    changes: &[MetadataChangeRequest],
    expected: &[MetadataFieldDiff],
) -> Result<MetadataSnapshot, MetadataEditingError> {
    let state = load_entity_state(t, db_id)?;
    let diff = build_diff(t, &state, changes)?;
    check_expected(changes, expected, &diff)?;
    check_inherits_manual(&state, changes)?;
    apply_inside_tx(t, &state, &diff)?;
    Ok(load_entity_state(t, db_id)?.response())
}

#[cfg(test)]
mod tests;
