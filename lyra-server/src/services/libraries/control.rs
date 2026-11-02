// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Syncing, refreshing and cancelling library runs on a user's behalf, with the one check of
//! who may manage a library that the REST routes and plugins share.

use std::collections::HashMap;

use agdb::DbAny;
use lyra_db::{
    Library,
    Permission,
};

use super::orchestrator::{
    self,
    LibraryRefreshRunOptions,
    SyncRunStartResponse,
    SyncRunSummary,
};
use crate::services::{
    STATE,
    auth::{
        AuthError,
        Principal,
        access,
    },
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum LibraryControlError {
    #[error("{0}")]
    Forbidden(String),
    #[error("library not found: {0}")]
    NotFound(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<LibraryControlError> for AuthError {
    fn from(error: LibraryControlError) -> Self {
        match error {
            LibraryControlError::Forbidden(message) => Self::Forbidden(message),
            LibraryControlError::NotFound(id) => Self::NotFound(format!("library not found: {id}")),
            LibraryControlError::Internal(error) => Self::Internal(error),
        }
    }
}

/// The principal as the database has it now, holding `ManageLibraries`. A request outlives the
/// moment it was authorized, and a plugin may act for a user long after.
fn current_manager(db: &DbAny, principal: &Principal) -> Result<Principal, LibraryControlError> {
    let current = principal.current(db).map_err(|error| match error {
        AuthError::Internal(error) => LibraryControlError::Internal(error),
        _ => LibraryControlError::Forbidden("the caller is no longer authorized".to_string()),
    })?;
    if !lyra_db::roles::has_permission(&current.permissions, Permission::ManageLibraries) {
        return Err(LibraryControlError::Forbidden(
            "ManageLibraries permission required".to_string(),
        ));
    }
    Ok(current)
}

/// The library `library_id` names, when `principal` holds `ManageLibraries` and can see the
/// library. A missing library, or one hidden from the principal, is not found.
pub(crate) fn managed_library(
    db: &DbAny,
    principal: &Principal,
    library_id: &str,
) -> Result<Library, LibraryControlError> {
    let current = current_manager(db, principal)?;
    library_of(db, &current, library_id)?
        .ok_or_else(|| LibraryControlError::NotFound(library_id.to_string()))
}

fn library_of(
    db: &DbAny,
    manager: &Principal,
    library_id: &str,
) -> Result<Option<Library>, LibraryControlError> {
    let library_db_id = match access::resolve_library_db_id(db, manager, library_id) {
        Ok(library_db_id) => library_db_id,
        Err(access::AccessError::LibraryNotFound(_)) => return Ok(None),
        Err(access::AccessError::InvalidRequest(message)) => {
            return Err(LibraryControlError::Forbidden(message));
        }
        Err(access::AccessError::Internal(error)) => return Err(error.into()),
    };
    Ok(lyra_db::libraries::get_by_id(db, library_db_id)?)
}

/// Starts a sync of the library. While a run of either kind is active, starts nothing and
/// returns that run with `started` false.
pub(crate) async fn sync(
    principal: &Principal,
    library_id: &str,
) -> Result<SyncRunStartResponse, LibraryControlError> {
    let library = managed_library(&*STATE.db.read().await, principal, library_id)?;
    Ok(orchestrator::start_library_sync(STATE.db.get(), library).await?)
}

/// Starts a metadata refresh of the library, as [`sync`] starts a sync.
pub(crate) async fn refresh(
    principal: &Principal,
    library_id: &str,
    options: LibraryRefreshRunOptions,
) -> Result<SyncRunStartResponse, LibraryControlError> {
    let library = managed_library(&*STATE.db.read().await, principal, library_id)?;
    Ok(orchestrator::start_library_refresh(STATE.db.get(), library, options).await?)
}

/// The active or latest run of each library, or an idle summary for one that never ran, keyed by
/// library id. Libraries that are missing or hidden from the principal are absent.
pub(crate) async fn sync_status(
    principal: &Principal,
    library_ids: &[String],
) -> Result<HashMap<String, SyncRunSummary>, LibraryControlError> {
    let managed = {
        let db = STATE.db.read().await;
        let current = current_manager(&db, principal)?;
        let mut managed = Vec::with_capacity(library_ids.len());
        for library_id in library_ids {
            if let Some(library) = library_of(&db, &current, library_id)? {
                managed.push(library.id);
            }
        }
        managed
    };
    Ok(orchestrator::library_sync_statuses(&managed).await?)
}

#[cfg(test)]
mod tests {
    use agdb::DbId;

    use super::*;
    use crate::services::testing::{
        init_default_test_state,
        runtime_test_lock,
        user_principal,
    };

    fn user_with(db: &mut DbAny, name: &str, permissions: Vec<Permission>) -> anyhow::Result<DbId> {
        let user = lyra_db::users::create(db, &lyra_db::fixtures::test_user(name)?)?;
        let role = lyra_db::roles::create(
            db,
            &lyra_db::roles::Role {
                db_id: None,
                id: nanoid::nanoid!(),
                name: format!("{name}-role"),
                permissions,
            },
        )?;
        lyra_db::roles::assign_role_to_user(db, user, role)?;
        Ok(user)
    }

    fn library(db: &mut DbAny, name: &str, path: &str) -> anyhow::Result<(DbId, String)> {
        let library = lyra_db::test_db::insert_library(db, name, path)?;
        let id = lyra_db::libraries::get_by_id(db, library)?
            .expect("library exists")
            .id;
        Ok((library, id))
    }

    async fn managed(principal: &Principal, id: &str) -> Result<String, LibraryControlError> {
        managed_library(&*STATE.db.read().await, principal, id).map(|library| library.id)
    }

    #[tokio::test]
    async fn managing_a_library_takes_the_permission_and_access_the_database_has_now()
    -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let (manager, listener, shown, hidden) = {
            let mut db = STATE.db.write().await;
            let manager = user_with(&mut db, "manager", vec![Permission::ManageLibraries])?;
            let listener = user_with(&mut db, "listener", Vec::new())?;
            let (shown_db_id, shown) = library(&mut db, "Shown", "/control/shown")?;
            let (_, hidden) = library(&mut db, "Hidden", "/control/hidden")?;
            for user in [manager, listener] {
                lyra_db::libraries::grant_access(
                    &mut db,
                    user,
                    shown_db_id,
                    lyra_db::libraries::AccessKind::ReadWrite,
                )?;
            }
            (manager, listener, shown, hidden)
        };
        let manager_principal = user_principal(manager).await?;
        let listener_principal = user_principal(listener).await?;

        assert_eq!(managed(&manager_principal, &shown).await?, shown);
        assert!(matches!(
            managed(&manager_principal, &hidden).await,
            Err(LibraryControlError::NotFound(_))
        ));
        assert!(matches!(
            managed(&manager_principal, "missing").await,
            Err(LibraryControlError::NotFound(_))
        ));
        assert!(matches!(
            managed(&listener_principal, &shown).await,
            Err(LibraryControlError::Forbidden(_))
        ));

        let statuses = sync_status(
            &manager_principal,
            &[shown.clone(), hidden.clone(), "missing".to_string()],
        )
        .await?;
        assert_eq!(statuses.len(), 1);
        let idle = &statuses[&shown];
        assert_eq!(idle.run.status, orchestrator::SyncRunStatus::Idle);
        assert_eq!(idle.run.id, None);
        assert!(matches!(
            sync_status(&listener_principal, std::slice::from_ref(&shown)).await,
            Err(LibraryControlError::Forbidden(_))
        ));

        // The principal the caller holds keeps its grants, but the database's role decides.
        {
            let mut db = STATE.db.write().await;
            let role = lyra_db::roles::get_role_for_user(&*db, manager)?.expect("manager role");
            lyra_db::roles::update(
                &mut *db,
                &lyra_db::roles::Role {
                    permissions: Vec::new(),
                    ..role
                },
            )?;
        }
        assert!(matches!(
            managed(&manager_principal, &shown).await,
            Err(LibraryControlError::Forbidden(_))
        ));
        Ok(())
    }
}
