// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Curation changes to metadata, previewed and applied as one ordered set.
//!
//! A preview is a read: it reports each operation against the data as it stands, so it doesn't
//! see what an earlier operation of the set would do. Applying is authoritative: one transaction
//! runs the operations in order, each resolving and checking what it names against what the ones
//! before it staged, and any failure rolls the whole set back.

mod external_ids;
mod merge;
pub(crate) mod model;
mod rename_genre;

use std::collections::HashSet;

use agdb::{
    DbAnyTransactionMut,
    DbError,
    DbId,
};
use lyra_db::{
    DbAccess,
    Permission,
    libraries::Library,
};
use serde::Serialize;
use serde_json::Value;

use self::{
    external_ids::IdKey,
    model::{
        ChangePreview,
        ChangeResult,
        MetadataChange,
    },
};
use super::editing::{
    self,
    MetadataEditingError,
};
use super::merge_journal::Merge;
use crate::services::{
    STATE,
    auth::{
        Principal,
        Recheck,
        access,
    },
    libraries::{
        library_has_active_run,
        library_mutation_guard,
    },
    providers::provider_registry,
};

/// The most operations a set takes, all of them under one hold of the database's write lock.
const MAX_CHANGES: usize = 50;
/// The most losers a set's merges take together, as each one costs a merge under that lock.
const MAX_LOSERS: usize = 50;

/// Why one operation of a set failed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ChangeError {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Forbidden(String),
    #[error("{0}")]
    Conflict(String),
    /// What the operation expected no longer holds; `current` is what a new preview would report.
    #[error("{message}")]
    Stale { message: String, current: Value },
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl ChangeError {
    fn entity_not_found(id: &str) -> Self {
        Self::NotFound(format!("Entity not found: {id}"))
    }

    fn stale(message: &str, current: &impl Serialize) -> anyhow::Result<Self> {
        Ok(Self::Stale {
            message: message.to_string(),
            current: serde_json::to_value(current)?,
        })
    }
}

impl From<DbError> for ChangeError {
    fn from(error: DbError) -> Self {
        Self::Internal(error.into())
    }
}

impl From<MetadataEditingError> for ChangeError {
    fn from(error: MetadataEditingError) -> Self {
        match error {
            MetadataEditingError::BadRequest(message) => Self::Invalid(message),
            MetadataEditingError::EntityNotFound(id) => Self::entity_not_found(&id),
            MetadataEditingError::Conflict(current) => {
                Self::stale("metadata changed after preview", &current)
                    .unwrap_or_else(Self::Internal)
            }
            MetadataEditingError::Internal(error) => Self::Internal(error),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ChangeSetError {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Forbidden(String),
    #[error("operation {index} failed: {error}")]
    Failed { index: usize, error: ChangeError },
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<DbError> for ChangeSetError {
    fn from(error: DbError) -> Self {
        Self::Internal(error.into())
    }
}

impl From<Recheck> for ChangeSetError {
    fn from(error: Recheck) -> Self {
        match error {
            Recheck::Internal(error) => Self::Internal(error),
            other => Self::Forbidden(other.to_string()),
        }
    }
}

/// The runs looked up before the database guard, under which a lookup can't run: the libraries the
/// set's entities reached, and those of them with a run on them.
struct CheckedRuns {
    checked: HashSet<String>,
    running: HashSet<String>,
}

/// What every operation of a set works with. Without `runs`, nothing in the set needs a run check.
struct ChangeContext {
    principal: Principal,
    unique_track_ids: HashSet<(String, String)>,
    runs: Option<CheckedRuns>,
}

impl ChangeContext {
    /// The entity `public_id` names and its libraries. A hidden entity reads as a missing one.
    fn reached(
        &self,
        db: &impl DbAccess,
        public_id: &str,
    ) -> Result<(DbId, Vec<Library>), ChangeError> {
        access::reachable_entity(db, &self.principal, public_id)?
            .ok_or_else(|| ChangeError::entity_not_found(public_id))
    }

    fn require_every_library(&self, libraries: &[Library]) -> Result<(), ChangeError> {
        if access::every_library_accessible(&self.principal, libraries) {
            Ok(())
        } else {
            Err(ChangeError::Forbidden(
                "this change reaches a library you can't access".to_string(),
            ))
        }
    }

    fn check_runs(&self, libraries: &[Library]) -> Result<(), ChangeError> {
        let Some(runs) = &self.runs else {
            return Ok(());
        };
        // A run can start on a library reached only now, so its check would have been missed.
        if libraries
            .iter()
            .any(|library| !runs.checked.contains(&library.id))
        {
            return Err(ChangeError::Conflict(
                "the entities reached another library while this was checked".to_string(),
            ));
        }
        if libraries
            .iter()
            .any(|library| runs.running.contains(&library.id))
        {
            return Err(ChangeError::Conflict(
                "a sync, refresh or purge of an affected library is running".to_string(),
            ));
        }
        Ok(())
    }
}

fn check_set_size(changes: &[MetadataChange]) -> Result<(), ChangeSetError> {
    if changes.is_empty() {
        return Err(ChangeSetError::Invalid(
            "changes must hold at least one operation".to_string(),
        ));
    }
    if changes.len() > MAX_CHANGES {
        return Err(ChangeSetError::Invalid(format!(
            "a change set takes at most {MAX_CHANGES} operations"
        )));
    }
    let losers: usize = changes
        .iter()
        .map(|change| match change {
            MetadataChange::Merge { loser_ids, .. } => loser_ids.len(),
            _ => 0,
        })
        .sum();
    if losers > MAX_LOSERS {
        return Err(ChangeSetError::Invalid(format!(
            "a change set merges at most {MAX_LOSERS} losers"
        )));
    }
    Ok(())
}

/// The ID types a merge of releases holds unique, for a set that merges.
async fn unique_track_ids(changes: &[MetadataChange]) -> HashSet<(String, String)> {
    if !changes.iter().any(MetadataChange::is_merge) {
        return HashSet::new();
    }
    provider_registry()
        .read_owned()
        .await
        .unique_track_id_pairs()
}

/// Looks up the runs on what the set reaches, when an operation needs that. It takes the database
/// lock itself, so it can't run under a guard of the caller's. It reports nothing: an operation
/// does, once its caller may know of the library.
async fn checked_runs(changes: &[MetadataChange]) -> anyhow::Result<Option<CheckedRuns>> {
    if !changes.iter().any(MetadataChange::needs_run_check) {
        return Ok(None);
    }
    let mut checked = HashSet::new();
    {
        let db = STATE.db.read().await;
        for public_id in changes.iter().flat_map(MetadataChange::entity_ids) {
            if let Some(entity) = lyra_db::lookup::find_node_id_by_id(&*db, public_id)? {
                checked.extend(
                    access::affected_libraries(&*db, entity)?
                        .into_iter()
                        .map(|library| library.id),
                );
            }
        }
    }
    let mut running = HashSet::new();
    for library in &checked {
        if library_has_active_run(library).await? {
            running.insert(library.clone());
        }
    }
    Ok(Some(CheckedRuns { checked, running }))
}

fn edited_entity(
    db: &impl DbAccess,
    context: &ChangeContext,
    entity_id: &str,
) -> Result<DbId, ChangeError> {
    let entity = access::metadata_entity(db, &context.principal, entity_id)?
        .ok_or_else(|| ChangeError::entity_not_found(entity_id))?;
    if context.runs.is_some() {
        context.check_runs(&access::affected_libraries(db, entity)?)?;
    }
    Ok(entity)
}

fn preview_change(
    db: &impl DbAccess,
    context: &ChangeContext,
    change: &MetadataChange,
) -> Result<ChangePreview, ChangeError> {
    Ok(match change {
        MetadataChange::EditMetadata {
            entity_id, edits, ..
        } => {
            let entity = edited_entity(db, context, entity_id)?;
            ChangePreview::EditMetadata {
                entity_id: entity_id.clone(),
                diff: editing::preview(db, entity, edits)?,
            }
        }
        MetadataChange::SetExternalId {
            entity_id,
            provider_id,
            id_type,
            id_value,
            ..
        } => {
            let key = IdKey {
                entity_id,
                provider_id,
                id_type,
            };
            ChangePreview::SetExternalId {
                entity_id: entity_id.clone(),
                provider_id: provider_id.clone(),
                id_type: id_type.clone(),
                id_value: id_value.trim().to_string(),
                current: external_ids::preview_set(db, context, key, id_value)?,
            }
        }
        MetadataChange::InheritExternalId {
            entity_id,
            provider_id,
            id_type,
            ..
        } => {
            let key = IdKey {
                entity_id,
                provider_id,
                id_type,
            };
            ChangePreview::InheritExternalId {
                entity_id: entity_id.clone(),
                provider_id: provider_id.clone(),
                id_type: id_type.clone(),
                current: external_ids::preview_inherit(db, context, key)?,
            }
        }
        MetadataChange::RemoveExternalIdAlias {
            entity_id,
            provider_id,
            id_type,
            id_value,
        } => {
            let key = IdKey {
                entity_id,
                provider_id,
                id_type,
            };
            external_ids::preview_remove_alias(db, context, key, id_value)?;
            ChangePreview::RemoveExternalIdAlias {
                entity_id: entity_id.clone(),
                provider_id: provider_id.clone(),
                id_type: id_type.clone(),
                id_value: id_value.clone(),
            }
        }
        MetadataChange::Merge {
            winner_id,
            loser_ids,
            ..
        } => ChangePreview::Merge(merge::preview(db, context, winner_id, loser_ids)?),
        MetadataChange::RenameGenre { genre_id, name, .. } => ChangePreview::RenameGenre {
            genre_id: genre_id.clone(),
            name: name.trim().to_string(),
            current_name: rename_genre::preview(db, context, genre_id, name)?,
        },
    })
}

fn require_expected<T>(expected: Option<T>) -> Result<T, ChangeError> {
    expected.ok_or_else(|| ChangeError::Invalid("expected is required".to_string()))
}

fn apply_change(
    t: &mut DbAnyTransactionMut<'_>,
    context: &ChangeContext,
    change: &MetadataChange,
    merges: &mut Vec<Merge>,
) -> Result<ChangeResult, ChangeError> {
    Ok(match change {
        MetadataChange::EditMetadata {
            entity_id,
            edits,
            expected,
        } => {
            let expected = require_expected(expected.as_deref())?;
            let entity = edited_entity(t, context, entity_id)?;
            ChangeResult::EditMetadata(editing::apply(t, entity, edits, expected)?)
        }
        MetadataChange::SetExternalId {
            entity_id,
            provider_id,
            id_type,
            id_value,
            expected,
        } => {
            let expected = require_expected(expected.as_ref())?.as_ref();
            let key = IdKey {
                entity_id,
                provider_id,
                id_type,
            };
            ChangeResult::SetExternalId {
                entity_id: entity_id.clone(),
                provider_id: provider_id.clone(),
                id_type: id_type.clone(),
                record: external_ids::apply_set(t, context, key, id_value, expected)?,
            }
        }
        MetadataChange::InheritExternalId {
            entity_id,
            provider_id,
            id_type,
            expected,
        } => {
            let expected = require_expected(expected.as_ref())?;
            let key = IdKey {
                entity_id,
                provider_id,
                id_type,
            };
            external_ids::apply_inherit(t, context, key, expected)?;
            ChangeResult::InheritExternalId {
                entity_id: entity_id.clone(),
                provider_id: provider_id.clone(),
                id_type: id_type.clone(),
            }
        }
        MetadataChange::RemoveExternalIdAlias {
            entity_id,
            provider_id,
            id_type,
            id_value,
        } => {
            let key = IdKey {
                entity_id,
                provider_id,
                id_type,
            };
            external_ids::apply_remove_alias(t, context, key, id_value)?;
            ChangeResult::RemoveExternalIdAlias {
                entity_id: entity_id.clone(),
                provider_id: provider_id.clone(),
                id_type: id_type.clone(),
                id_value: id_value.clone(),
            }
        }
        MetadataChange::Merge {
            winner_id,
            loser_ids,
            expected,
        } => {
            let expected = require_expected(expected.as_deref())?;
            ChangeResult::Merge(merge::apply(
                t, context, winner_id, loser_ids, expected, merges,
            )?)
        }
        MetadataChange::RenameGenre {
            genre_id,
            name,
            expected,
        } => {
            let expected = require_expected(expected.as_deref())?;
            ChangeResult::RenameGenre {
                genre_id: genre_id.clone(),
                name: rename_genre::apply(t, context, genre_id, name, expected)?,
            }
        }
    })
}

/// Reports what each operation would do to the data as it stands, without seeing what an earlier
/// one would change. It binds nothing: [`apply`] checks everything again.
pub(crate) async fn preview(
    principal: &Principal,
    changes: &[MetadataChange],
) -> Result<Vec<ChangePreview>, ChangeSetError> {
    check_set_size(changes)?;
    let unique_track_ids = unique_track_ids(changes).await;
    let runs = checked_runs(changes).await?;

    let db = STATE.db.read().await;
    let context = ChangeContext {
        principal: principal.current_with(&db, Permission::ManageMetadata)?,
        unique_track_ids,
        runs,
    };
    changes
        .iter()
        .enumerate()
        .map(|(index, change)| {
            preview_change(&*db, &context, change)
                .map_err(|error| ChangeSetError::Failed { index, error })
        })
        .collect()
}

/// Applies the operations in order in one transaction, or none of them. For a set that checks
/// runs, the library mutation guard keeps one from starting on what it checked.
pub(crate) async fn apply(
    principal: &Principal,
    changes: &[MetadataChange],
) -> Result<Vec<ChangeResult>, ChangeSetError> {
    check_set_size(changes)?;
    let unique_track_ids = unique_track_ids(changes).await;
    let _guard = if changes.iter().any(MetadataChange::needs_run_check) {
        Some(library_mutation_guard().await)
    } else {
        None
    };
    let runs = checked_runs(changes).await?;

    let mut db = STATE.db.write().await;
    let context = ChangeContext {
        principal: principal.current_with(&db, Permission::ManageMetadata)?,
        unique_track_ids,
        runs,
    };
    let mut merges = Vec::new();
    let results = db.transaction_mut(|t| {
        changes
            .iter()
            .enumerate()
            .map(|(index, change)| {
                apply_change(t, &context, change, &mut merges)
                    .map_err(|error| ChangeSetError::Failed { index, error })
            })
            .collect::<Result<Vec<_>, _>>()
    })?;
    STATE.generation().merge_journal.record(merges);
    Ok(results)
}

#[cfg(test)]
mod tests;
