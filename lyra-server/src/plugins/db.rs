// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub(crate) use crate::db::metadata::manual_overrides::ManualMetadataField;
pub(crate) use crate::db::plugin::*;

pub(crate) fn manual_metadata_owns_field(
    db: &impl crate::db::DbAccess,
    entity_id: agdb::DbId,
    field: ManualMetadataField,
) -> anyhow::Result<bool> {
    crate::db::metadata::manual_overrides::owns_field(db, entity_id, field)
}

#[cfg(test)]
pub(crate) use crate::db::test_db;
