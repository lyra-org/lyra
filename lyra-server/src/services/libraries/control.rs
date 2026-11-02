// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Syncing, refreshing and cancelling library runs on a user's behalf, with the one check of
//! who may manage a library that the REST routes and plugins share, and reading run status for
//! whoever can see a library.

use std::collections::HashMap;

use agdb::DbAny;
use lyra_db::{
    Library,
    Permission,
};

use super::orchestrator::{
    self,
    LibraryRun,
    RunFilter,
    SyncRunStartResponse,
    SyncRunSummary,
};
use crate::services::{
    STATE,
    auth::{
        AuthError,
        Principal,
        Recheck,
        access,
        require_authenticated,
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

impl From<Recheck> for LibraryControlError {
    fn from(error: Recheck) -> Self {
        match error {
            Recheck::Internal(error) => Self::Internal(error),
            other => Self::Forbidden(other.to_string()),
        }
    }
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

/// The library `library_id` names, with the principal as the database has it now, when the
/// principal holds `ManageLibraries` and can see the library. A missing library, or one hidden
/// from the principal, is not found. A request outlives the moment it was authorized, and a
/// plugin may act for a user long after.
pub(crate) fn managed_library(
    db: &DbAny,
    principal: &Principal,
    library_id: &str,
) -> Result<(Principal, Library), LibraryControlError> {
    let current = principal.current_with(db, Permission::ManageLibraries)?;
    let library = library_of(db, &current, library_id)?
        .ok_or_else(|| LibraryControlError::NotFound(library_id.to_string()))?;
    Ok((current, library))
}

fn library_of(
    db: &DbAny,
    principal: &Principal,
    library_id: &str,
) -> Result<Option<Library>, LibraryControlError> {
    match access::resolve_library(db, principal, library_id) {
        Ok(library) => Ok(Some(library)),
        Err(access::AccessError::LibraryNotFound(_)) => Ok(None),
        Err(error) => Err(anyhow::Error::from(error).into()),
    }
}

/// The principal of `bearer` as the database has it now, when it may manage the library
/// `library_id`, as [`managed_library`] decides.
pub(crate) async fn require_manage_libraries_on(
    bearer: Option<&str>,
    library_id: &str,
) -> Result<Principal, AuthError> {
    let principal = require_authenticated(bearer).await?;
    let db = STATE.db.read().await;
    let (current, _) = managed_library(&db, &principal, library_id)?;
    Ok(current)
}

/// Starts `run` on the library, checking the principal under the lock that serializes starts.
/// While a run of either kind is active, starts nothing and returns that run with `started`
/// false.
pub(crate) async fn start(
    principal: &Principal,
    library_id: &str,
    run: LibraryRun,
) -> Result<SyncRunStartResponse, LibraryControlError> {
    orchestrator::start_library_run(STATE.db.get(), run, |db| {
        managed_library(db, principal, library_id).map(|(_, library)| library)
    })
    .await
}

/// The run `filter` picks of each library, or an idle summary for a library without one, keyed
/// by library id. Libraries that are missing or hidden from the principal, as the database has it
/// now, are absent; without a principal, as for the system, none is hidden. A principal without
/// `ManageLibraries` gets no run error or current subject, since they can name paths.
pub(crate) async fn sync_status(
    principal: Option<&Principal>,
    library_ids: &[String],
    filter: RunFilter,
) -> Result<HashMap<String, SyncRunSummary>, LibraryControlError> {
    let (visible, shows_details) = {
        let db = STATE.db.read().await;
        match principal {
            Some(principal) => {
                let current = principal.recheck(&db)?;
                let mut visible = Vec::with_capacity(library_ids.len());
                for library_id in library_ids {
                    if let Some(library) = library_of(&db, &current, library_id)? {
                        visible.push(library);
                    }
                }
                let manages = lyra_db::roles::has_permission(
                    &current.permissions,
                    Permission::ManageLibraries,
                );
                (visible, manages)
            }
            None => {
                let visible = access::system_libraries(&*db, &super::system_context())?
                    .into_iter()
                    .filter(|library| library_ids.contains(&library.id))
                    .collect();
                (visible, true)
            }
        }
    };
    let mut statuses = orchestrator::library_sync_statuses(&visible, filter).await?;
    if !shows_details {
        for summary in statuses.values_mut() {
            summary.run.error = None;
            if let Some(current) = &mut summary.current {
                current.subject = None;
            }
        }
    }
    Ok(statuses)
}

/// Cancels the library's active run and returns its summary; without one, returns what
/// [`sync_status`] does for the library.
pub(crate) async fn cancel_sync(
    principal: &Principal,
    library_id: &str,
) -> Result<SyncRunSummary, LibraryControlError> {
    let (_, library) = managed_library(&*STATE.db.read().await, principal, library_id)?;
    Ok(orchestrator::cancel_library_run(&library).await?)
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
        managed_library(&*STATE.db.read().await, principal, id).map(|(_, library)| library.id)
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
            Some(&manager_principal),
            &[shown.clone(), hidden.clone(), "missing".to_string()],
            RunFilter::default(),
        )
        .await?;
        assert_eq!(statuses.len(), 1);
        let idle = &statuses[&shown];
        assert_eq!(idle.run.status, orchestrator::SyncRunStatus::Idle);
        assert_eq!(idle.run.id, None);
        let listener_idle = sync_status(
            Some(&listener_principal),
            &[shown.clone(), hidden.clone()],
            RunFilter::default(),
        )
        .await?;
        assert_eq!(
            listener_idle.len(),
            1,
            "a listener sees the libraries it can see"
        );
        assert_eq!(
            listener_idle[&shown].run.status,
            orchestrator::SyncRunStatus::Idle
        );

        let system = sync_status(
            None,
            &[shown.clone(), hidden.clone(), "missing".to_string()],
            RunFilter::default(),
        )
        .await?;
        assert_eq!(system.len(), 2, "the system sees every library");

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
        assert!(matches!(
            cancel_sync(&manager_principal, &shown).await,
            Err(LibraryControlError::Forbidden(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn a_status_names_errors_and_subjects_only_to_managers() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let (manager, listener, library) = {
            let mut db = STATE.db.write().await;
            let manager = user_with(&mut db, "manager", vec![Permission::ManageLibraries])?;
            let listener = user_with(&mut db, "listener", Vec::new())?;
            let (library_db_id, _) = library(&mut db, "Shown", "/control/shown")?;
            for user in [manager, listener] {
                lyra_db::libraries::grant_access(
                    &mut db,
                    user,
                    library_db_id,
                    lyra_db::libraries::AccessKind::ReadWrite,
                )?;
            }
            let library = lyra_db::libraries::get_by_id(&*db, library_db_id)?.expect("library");
            (manager, listener, library)
        };
        let progress = orchestrator::begin_test_run(&library).await;
        progress
            .set_test_error("cannot read /control/shown/a")
            .await;
        progress
            .start_work(
                orchestrator::SyncStageKey::EntrySync,
                orchestrator::SyncWorkDetails::new("entry_group_sync")
                    .source_dir(Some("/control/shown/a".to_string())),
            )
            .await;
        let status = |principal: Option<Principal>| {
            let id = library.id.clone();
            async move {
                sync_status(
                    principal.as_ref(),
                    std::slice::from_ref(&id),
                    RunFilter::default(),
                )
                .await
                .map(|mut statuses| statuses.remove(&id).expect("status"))
            }
        };

        let system = status(None).await?;
        assert_eq!(
            system.run.error.as_deref(),
            Some("cannot read /control/shown/a"),
            "the system sees every library and its run's error"
        );

        let managed = status(Some(user_principal(manager).await?)).await?;
        assert_eq!(
            managed.run.error.as_deref(),
            Some("cannot read /control/shown/a")
        );
        let current = managed.current.expect("current work");
        assert_eq!(current.subject.as_deref(), Some("/control/shown/a"));

        let listened = status(Some(user_principal(listener).await?)).await?;
        assert_eq!(listened.run.id, managed.run.id);
        assert_eq!(listened.run.error, None);
        let current = listened.current.expect("current work");
        assert_eq!(current.stage, orchestrator::SyncStageKey::EntrySync);
        assert_eq!(current.subject, None);
        orchestrator::end_test_run(&progress).await;
        Ok(())
    }

    #[tokio::test]
    async fn an_admin_manages_a_library_created_after_its_request_began() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let admin = user_with(
            &mut *STATE.db.write().await,
            "admin",
            vec![Permission::Admin],
        )?;
        let principal = user_principal(admin).await?;
        let (_, created) = library(&mut *STATE.db.write().await, "Later", "/control/later")?;

        assert_eq!(managed(&principal, &created).await?, created);
        let (current, _) = managed_library(&*STATE.db.read().await, &principal, &created)?;
        assert!(current.accessible_library_ids.contains(&created));
        Ok(())
    }
}
