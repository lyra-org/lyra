// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Public ids of the shared entities that caller state such as favorites, ratings and tags
//! attaches to.

use agdb::DbId;

use crate::routes::AppError;

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
