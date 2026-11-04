// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Public ids of the shared entities that caller state such as favorites, ratings and tags
//! attaches to.

use std::collections::HashMap;

use agdb::{
    DbAny,
    DbId,
};
use axum::http::HeaderMap;

use crate::{
    routes::{
        AppError,
        request::bearer_credential,
    },
    services::{
        STATE,
        auth::{
            Principal,
            require_authenticated,
        },
    },
};

/// The most ids a batch lookup of caller state takes.
pub(super) const CHECK_TARGET_CAP: usize = 500;

pub(super) fn validate_target_id(target_id: &str) -> Result<(), AppError> {
    if crate::services::opaque_ids::is_public_id(target_id) {
        return Ok(());
    }
    Err(AppError::bad_request(format!(
        "malformed target id: {target_id}"
    )))
}

pub(super) fn resolve_target(
    db: &impl lyra_db::DbAccess,
    target_id: &str,
    not_found: impl FnOnce() -> AppError,
) -> Result<DbId, AppError> {
    lyra_db::lookup::find_node_id_by_id(db, target_id)?.ok_or_else(not_found)
}

/// A value from `lookup` for each submitted id, `None` where it has none. Errs above
/// [`CHECK_TARGET_CAP`] submitted ids.
pub(super) async fn dense_lookup<T>(
    headers: &HeaderMap,
    submitted: Vec<String>,
    label: &str,
    lookup: impl FnOnce(&DbAny, &Principal, &[DbId]) -> anyhow::Result<HashMap<DbId, T>>,
) -> Result<HashMap<String, Option<T>>, AppError> {
    let principal = require_authenticated(bearer_credential(headers)).await?;
    if submitted.len() > CHECK_TARGET_CAP {
        return Err(AppError::bad_request(format!(
            "{label} cap exceeded: {} > {CHECK_TARGET_CAP}",
            submitted.len(),
        )));
    }

    let db = STATE.db.read().await;
    let mut values: HashMap<String, Option<T>> = submitted
        .into_iter()
        .map(|target_id| (target_id, None))
        .collect();
    let target_ids: Vec<&str> = values.keys().map(String::as_str).collect();
    let resolved = lyra_db::lookup::find_node_ids_by_ids(&*db, &target_ids)?;
    let target_db_ids: Vec<DbId> = resolved.values().copied().collect();
    let mut found = lookup(&db, &principal, &target_db_ids)?;
    for (target_id, target_db_id) in resolved {
        if let Some(value) = found.remove(&target_db_id) {
            values.insert(target_id, Some(value));
        }
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_id_validation_matches_public_id_shape() {
        assert!(validate_target_id("abc123").is_ok());
        assert!(validate_target_id("V1StGXR8_Z5jdHi6B-myT").is_ok());
        assert!(validate_target_id("short").is_err());
        assert!(validate_target_id("has/slash").is_err());
    }
}
