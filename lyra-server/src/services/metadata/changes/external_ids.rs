// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Hand edits of an entity's external IDs.

use agdb::{
    DbAnyTransactionMut,
    DbId,
};
use lyra_db::{
    DbAccess,
    IdSource,
    external_ids,
};

use super::{
    ChangeContext,
    ChangeError,
    model::ExternalIdState,
};

/// The external ID of an entity a change works on.
#[derive(Clone, Copy)]
pub(super) struct IdKey<'a> {
    pub(super) entity_id: &'a str,
    pub(super) provider_id: &'a str,
    pub(super) id_type: &'a str,
}

impl IdKey<'_> {
    fn label(&self) -> String {
        format!("{}/{}", self.provider_id, self.id_type)
    }

    fn current(&self, db: &impl DbAccess, entity: DbId) -> anyhow::Result<Option<ExternalIdState>> {
        Ok(
            external_ids::get_inside_tx(db, entity, self.provider_id, self.id_type)?
                .map(Into::into),
        )
    }
}

fn reachable(
    db: &impl DbAccess,
    context: &ChangeContext,
    entity_id: &str,
) -> Result<DbId, ChangeError> {
    let (entity, libraries) = context.reached(db, entity_id)?;
    context.check_runs(&libraries)?;
    Ok(entity)
}

fn set_target(
    db: &impl DbAccess,
    context: &ChangeContext,
    key: IdKey<'_>,
    id_value: &str,
) -> Result<(DbId, Option<ExternalIdState>), ChangeError> {
    if id_value.is_empty() {
        return Err(ChangeError::Invalid(format!(
            "External ID value is blank: {}",
            key.label()
        )));
    }
    let entity = reachable(db, context, key.entity_id)?;
    if !lyra_db::entities::exists(db, entity)? {
        return Err(ChangeError::entity_not_found(key.entity_id));
    }
    Ok((entity, key.current(db, entity)?))
}

fn check_set_changes(
    key: IdKey<'_>,
    current: Option<&ExternalIdState>,
    id_value: &str,
) -> Result<(), ChangeError> {
    if current.is_some_and(|id| id.source == IdSource::Manual && id.id_value == id_value) {
        return Err(ChangeError::Invalid(format!(
            "external ID edit has no effect: {}",
            key.label()
        )));
    }
    Ok(())
}

/// The record a set of `id_value` would replace.
pub(super) fn preview_set(
    db: &impl DbAccess,
    context: &ChangeContext,
    key: IdKey<'_>,
    id_value: &str,
) -> Result<Option<ExternalIdState>, ChangeError> {
    let id_value = id_value.trim();
    let (_, current) = set_target(db, context, key, id_value)?;
    check_set_changes(key, current.as_ref(), id_value)?;
    Ok(current)
}

pub(super) fn apply_set(
    t: &mut DbAnyTransactionMut<'_>,
    context: &ChangeContext,
    key: IdKey<'_>,
    id_value: &str,
    expected: Option<&ExternalIdState>,
) -> Result<ExternalIdState, ChangeError> {
    let id_value = id_value.trim();
    let (entity, current) = set_target(t, context, key, id_value)?;
    check_expected(current.as_ref(), expected)?;
    check_set_changes(key, current.as_ref(), id_value)?;
    external_ids::upsert_inside_tx(
        t,
        entity,
        key.provider_id,
        key.id_type,
        id_value,
        IdSource::Manual,
    )?;
    Ok(key
        .current(t, entity)?
        .ok_or_else(|| anyhow::anyhow!("external id missing after it was set"))?)
}

fn manual_record(
    key: IdKey<'_>,
    current: Option<ExternalIdState>,
) -> Result<ExternalIdState, ChangeError> {
    current
        .filter(|id| id.source == IdSource::Manual)
        .ok_or_else(|| ChangeError::NotFound(format!("No manual external ID: {}", key.label())))
}

/// The record an inherit would return to its resolved value.
pub(super) fn preview_inherit(
    db: &impl DbAccess,
    context: &ChangeContext,
    key: IdKey<'_>,
) -> Result<ExternalIdState, ChangeError> {
    let entity = reachable(db, context, key.entity_id)?;
    manual_record(key, key.current(db, entity)?)
}

pub(super) fn apply_inherit(
    t: &mut DbAnyTransactionMut<'_>,
    context: &ChangeContext,
    key: IdKey<'_>,
    expected: &ExternalIdState,
) -> Result<(), ChangeError> {
    let entity = reachable(t, context, key.entity_id)?;
    let current = key.current(t, entity)?;
    check_expected(current.as_ref(), Some(expected))?;
    manual_record(key, current)?;
    external_ids::remove_manual_inside_tx(t, entity, key.provider_id, key.id_type)?;
    Ok(())
}

fn check_expected(
    current: Option<&ExternalIdState>,
    expected: Option<&ExternalIdState>,
) -> Result<(), ChangeError> {
    if current == expected {
        Ok(())
    } else {
        Err(ChangeError::stale(
            "the external ID changed after preview",
            &current,
        )?)
    }
}

/// The entity holding an alias, under a merge's access rule: every library sees where the value
/// routes.
fn alias_owner(
    db: &impl DbAccess,
    context: &ChangeContext,
    entity_id: &str,
) -> Result<DbId, ChangeError> {
    let (entity, libraries) = context.reached(db, entity_id)?;
    context.require_every_library(&libraries)?;
    context.check_runs(&libraries)?;
    Ok(entity)
}

fn alias_not_found(key: IdKey<'_>, id_value: &str) -> ChangeError {
    ChangeError::NotFound(format!("No external ID alias: {}/{id_value}", key.label()))
}

pub(super) fn preview_remove_alias(
    db: &impl DbAccess,
    context: &ChangeContext,
    key: IdKey<'_>,
    id_value: &str,
) -> Result<(), ChangeError> {
    let entity = alias_owner(db, context, key.entity_id)?;
    let held = external_ids::get_aliases_for_entity(db, entity)?
        .iter()
        .any(|alias| {
            alias.provider_id == key.provider_id
                && alias.id_type == key.id_type
                && alias.id_value == id_value
        });
    if held {
        Ok(())
    } else {
        Err(alias_not_found(key, id_value))
    }
}

/// Removes an external ID alias, so a wrong merge stops routing the value to the entity.
pub(super) fn apply_remove_alias(
    t: &mut DbAnyTransactionMut<'_>,
    context: &ChangeContext,
    key: IdKey<'_>,
    id_value: &str,
) -> Result<(), ChangeError> {
    let entity = alias_owner(t, context, key.entity_id)?;
    if external_ids::remove_alias(t, entity, key.provider_id, key.id_type, id_value)? {
        Ok(())
    } else {
        Err(alias_not_found(key, id_value))
    }
}
