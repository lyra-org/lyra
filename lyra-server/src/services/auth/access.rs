// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashMap,
    path::PathBuf,
};

use agdb::DbId;

use crate::{
    db::{
        self,
        libraries::Library,
    },
    services::SystemContext,
};

use super::Principal;

#[derive(Debug, thiserror::Error)]
pub(crate) enum AccessError {
    #[error("{0}")]
    InvalidRequest(String),
    #[error("library not found: {0}")]
    LibraryNotFound(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LibraryView {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) language: Option<String>,
    pub(crate) country: Option<String>,
}

impl From<Library> for LibraryView {
    fn from(library: Library) -> Self {
        Self {
            id: library.id,
            name: library.name,
            language: library.language,
            country: library.country,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LibraryFull {
    pub(crate) db_id: Option<DbId>,
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) language: Option<String>,
    pub(crate) country: Option<String>,
    pub(crate) path: PathBuf,
}

impl From<Library> for LibraryFull {
    fn from(library: Library) -> Self {
        Self {
            db_id: library.db_id,
            id: library.id,
            name: library.name,
            language: library.language,
            country: library.country,
            path: library.path,
        }
    }
}

fn principal_can_access_library(principal: &Principal, library: &Library) -> bool {
    principal.accessible_library_ids.contains(&library.id)
}

fn filter_accessible_libraries(
    libraries: impl IntoIterator<Item = Library>,
    principal: &Principal,
) -> Vec<LibraryView> {
    libraries
        .into_iter()
        .filter(|library| principal_can_access_library(principal, library))
        .map(LibraryView::from)
        .collect()
}

pub(crate) fn libraries(
    db: &impl db::DbAccess,
    principal: &Principal,
) -> anyhow::Result<Vec<LibraryView>> {
    Ok(filter_accessible_libraries(
        db::libraries::get(db)?,
        principal,
    ))
}

pub(crate) fn system_libraries(
    db: &impl db::DbAccess,
    _ctx: &SystemContext,
) -> anyhow::Result<Vec<LibraryFull>> {
    Ok(db::libraries::get(db)?
        .into_iter()
        .map(LibraryFull::from)
        .collect())
}

pub(crate) fn library_by_id(
    db: &impl db::DbAccess,
    principal: &Principal,
    library_db_id: DbId,
) -> anyhow::Result<Option<LibraryView>> {
    Ok(db::libraries::get_by_id(db, library_db_id)?
        .filter(|library| principal_can_access_library(principal, library))
        .map(LibraryView::from))
}

pub(crate) fn system_library_by_id(
    db: &impl db::DbAccess,
    _ctx: &SystemContext,
    library_db_id: DbId,
) -> anyhow::Result<Option<LibraryFull>> {
    Ok(db::libraries::get_by_id(db, library_db_id)?.map(LibraryFull::from))
}

pub(crate) fn libraries_by_alias(
    db: &impl db::DbAccess,
    principal: &Principal,
    alias: &str,
) -> anyhow::Result<Vec<LibraryView>> {
    Ok(filter_accessible_libraries(
        db::libraries::get_by_alias(db, alias)?,
        principal,
    ))
}

pub(crate) fn system_libraries_by_alias(
    db: &impl db::DbAccess,
    _ctx: &SystemContext,
    alias: &str,
) -> anyhow::Result<Vec<LibraryFull>> {
    Ok(db::libraries::get_by_alias(db, alias)?
        .into_iter()
        .map(LibraryFull::from)
        .collect())
}

pub(crate) fn libraries_for_entity(
    db: &impl db::DbAccess,
    principal: &Principal,
    entity_db_id: DbId,
) -> anyhow::Result<Vec<LibraryView>> {
    Ok(filter_accessible_libraries(
        db::libraries::get_for_entity(db, entity_db_id)?,
        principal,
    ))
}

pub(crate) fn system_libraries_for_entity(
    db: &impl db::DbAccess,
    _ctx: &SystemContext,
    entity_db_id: DbId,
) -> anyhow::Result<Vec<LibraryFull>> {
    Ok(db::libraries::get_for_entity(db, entity_db_id)?
        .into_iter()
        .map(LibraryFull::from)
        .collect())
}

pub(crate) fn libraries_for_entities(
    db: &impl db::DbAccess,
    principal: &Principal,
    entity_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, LibraryView>> {
    Ok(db::libraries::get_for_entities(db, entity_ids)?
        .into_iter()
        .filter(|(_, library)| principal_can_access_library(principal, library))
        .map(|(entity_id, library)| (entity_id, LibraryView::from(library)))
        .collect())
}

pub(crate) fn system_libraries_for_entities(
    db: &impl db::DbAccess,
    _ctx: &SystemContext,
    entity_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, LibraryFull>> {
    Ok(db::libraries::get_for_entities(db, entity_ids)?
        .into_iter()
        .map(|(entity_id, library)| (entity_id, LibraryFull::from(library)))
        .collect())
}

pub(crate) fn entity_accessible(
    db: &impl db::DbAccess,
    principal: &Principal,
    entity_db_id: DbId,
) -> anyhow::Result<bool> {
    if principal.permissions.contains(&db::Permission::Admin) {
        return Ok(true);
    }
    Ok(db::libraries::get_for_entity(db, entity_db_id)?
        .into_iter()
        .any(|library| principal_can_access_library(principal, &library)))
}

pub(crate) fn require_entity_accessible<E>(
    db: &impl db::DbAccess,
    principal: &Principal,
    entity_db_id: DbId,
    not_accessible: impl FnOnce() -> E,
) -> Result<(), E>
where
    E: From<anyhow::Error>,
{
    if entity_accessible(db, principal, entity_db_id).map_err(E::from)? {
        Ok(())
    } else {
        Err(not_accessible())
    }
}

pub(crate) fn playlist_accessible(
    db: &impl db::DbAccess,
    principal: &Principal,
    playlist_db_id: DbId,
) -> anyhow::Result<bool> {
    if principal.permissions.contains(&db::Permission::Admin) {
        return Ok(true);
    }
    let Some(playlist) = db::playlists::get_by_id(db, playlist_db_id)? else {
        return Ok(false);
    };
    if playlist.is_public.unwrap_or(false) {
        return Ok(true);
    }
    Ok(db::playlists::get_owner(db, playlist_db_id)? == Some(principal.user_db_id))
}

pub(crate) fn resolve_library_db_id(
    db: &impl db::DbAccess,
    principal: &Principal,
    library_id: &str,
) -> Result<DbId, AccessError> {
    if principal.permissions.contains(&db::Permission::Admin) {
        if !principal.accessible_library_ids.contains(library_id) {
            return Err(AccessError::LibraryNotFound(library_id.to_string()));
        }
        let library_db_id = db::lookup::find_node_id_by_id(db, library_id)?
            .ok_or_else(|| AccessError::LibraryNotFound(library_id.to_string()))?;
        db::libraries::get_by_id(db, library_db_id)?
            .ok_or_else(|| AccessError::LibraryNotFound(library_id.to_string()))?;
        return Ok(library_db_id);
    }

    db::libraries::find_node_id_accessible_to_user(db, principal.user_db_id, library_id)?
        .ok_or_else(|| AccessError::LibraryNotFound(library_id.to_string()))
}

pub(crate) fn resolve_optional_library_filter(
    db: &impl db::DbAccess,
    principal: &Principal,
    library_id: Option<&str>,
) -> Result<Option<DbId>, AccessError> {
    let Some(library_id) = library_id else {
        return Ok(None);
    };
    let library_id = library_id.trim();
    if library_id.is_empty() {
        return Err(AccessError::InvalidRequest(
            "library_id cannot be empty".to_string(),
        ));
    }

    resolve_library_db_id(db, principal, library_id).map(Some)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use agdb::DbAny;
    use nanoid::nanoid;

    use super::*;
    use crate::db::{
        libraries::LibraryInsert,
        playlists::Playlist,
        test_db::{
            new_test_db,
            test_user,
        },
    };

    fn principal(user_db_id: DbId, permissions: Vec<db::Permission>) -> Principal {
        Principal {
            user_db_id,
            user_public_id: format!("user-{}", user_db_id.0),
            username: format!("user-{}", user_db_id.0),
            permissions,
            role_name: None,
            accessible_library_ids: HashSet::new(),
        }
    }

    fn create_user(db: &mut DbAny, username: &str) -> anyhow::Result<DbId> {
        db::users::create(db, &test_user(username)?)
    }

    fn create_library(
        db: &mut DbAny,
        name: &str,
        creator_user_db_id: Option<DbId>,
    ) -> anyhow::Result<Library> {
        let path = PathBuf::from(format!("/tmp/lyra-access-test-{}", nanoid!()));
        let request = LibraryInsert {
            id: nanoid!(),
            name: name.to_string(),
            path: path.clone(),
            path_key: db::libraries::path_key_for(&path),
            language: None,
            country: None,
        };
        db.transaction_mut(|transaction| -> anyhow::Result<Library> {
            if let Some(user_db_id) = creator_user_db_id {
                Ok(db::libraries::create_with_creator(
                    transaction,
                    request,
                    user_db_id,
                )?)
            } else {
                Ok(db::libraries::create_system(transaction, request)?)
            }
        })
    }

    #[test]
    fn library_views_enforce_principal_scope_and_system_shape() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_user(&mut db, "viewer")?;
        let visible = create_library(&mut db, "Visible", Some(user_db_id))?;
        let hidden = create_library(&mut db, "Hidden", None)?;
        let mut principal = principal(user_db_id, vec![]);
        principal.accessible_library_ids = db::libraries::accessible_library_ids(&db, user_db_id)?;

        let visible_libraries = libraries(&db, &principal)?;
        assert_eq!(visible_libraries.len(), 1);
        assert_eq!(visible_libraries[0].id, visible.id);

        let all_libraries = system_libraries(&db, &crate::services::libraries::system_context())?;
        assert_eq!(all_libraries.len(), 2);
        assert!(
            all_libraries
                .iter()
                .any(|library| { library.id == hidden.id && library.path == hidden.path })
        );
        Ok(())
    }

    #[test]
    fn library_resolution_enforces_access_for_users_and_admins() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let owner_db_id = create_user(&mut db, "owner")?;
        let viewer_db_id = create_user(&mut db, "viewer")?;
        let library = create_library(&mut db, "Music", Some(owner_db_id))?;

        let owner = principal(owner_db_id, vec![]);
        assert_eq!(
            resolve_library_db_id(&db, &owner, &library.id)?,
            library.db_id.unwrap()
        );

        let viewer = principal(viewer_db_id, vec![]);
        assert!(matches!(
            resolve_library_db_id(&db, &viewer, &library.id),
            Err(AccessError::LibraryNotFound(_))
        ));

        let mut admin = principal(viewer_db_id, vec![db::Permission::Admin]);
        admin.accessible_library_ids.insert(library.id.clone());
        assert_eq!(
            resolve_library_db_id(&db, &admin, &library.id)?,
            library.db_id.unwrap()
        );
        Ok(())
    }

    #[test]
    fn playlist_access_honors_admin_public_and_owner_rules() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let owner_db_id = create_user(&mut db, "owner")?;
        let viewer_db_id = create_user(&mut db, "viewer")?;
        let private_id = db::playlists::create(
            &mut db,
            &Playlist {
                db_id: None,
                id: nanoid!(),
                name: "Private".to_string(),
                description: None,
                is_public: Some(false),
                created_at: None,
                updated_at: None,
            },
            owner_db_id,
        )?;
        let public_id = db::playlists::create(
            &mut db,
            &Playlist {
                db_id: None,
                id: nanoid!(),
                name: "Public".to_string(),
                description: None,
                is_public: Some(true),
                created_at: None,
                updated_at: None,
            },
            owner_db_id,
        )?;

        let owner = principal(owner_db_id, vec![]);
        let viewer = principal(viewer_db_id, vec![]);
        let admin = principal(viewer_db_id, vec![db::Permission::Admin]);
        assert!(playlist_accessible(&db, &owner, private_id)?);
        assert!(!playlist_accessible(&db, &viewer, private_id)?);
        assert!(playlist_accessible(&db, &viewer, public_id)?);
        assert!(playlist_accessible(&db, &admin, private_id)?);
        Ok(())
    }

    #[test]
    fn empty_library_filter_is_invalid() {
        let db = new_test_db().unwrap();
        let principal = principal(DbId(1), vec![]);
        assert!(matches!(
            resolve_optional_library_filter(&db, &principal, Some("  ")),
            Err(AccessError::InvalidRequest(_))
        ));
    }
}
