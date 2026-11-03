// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashSet;

use agdb::DbId;

use crate::services::SystemContext;
use lyra_db::libraries::Library;

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

fn principal_can_access_library(principal: &Principal, library: &Library) -> bool {
    principal.accessible_library_ids.contains(&library.id)
}

/// The libraries the principal can see, by database id.
pub(crate) fn libraries(
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
) -> anyhow::Result<Vec<Library>> {
    Ok(lyra_db::libraries::get(db)?
        .into_iter()
        .filter(|library| principal_can_access_library(principal, library))
        .collect())
}

/// Every library, by database id.
pub(crate) fn system_libraries(
    db: &impl lyra_db::DbAccess,
    _ctx: &SystemContext,
) -> anyhow::Result<Vec<Library>> {
    lyra_db::libraries::get(db)
}

/// The libraries holding the entity that the principal can see, the one the server picks for the
/// entity first.
pub(crate) fn libraries_for_entity(
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
    entity_db_id: DbId,
) -> anyhow::Result<Vec<Library>> {
    Ok(lyra_db::libraries::get_for_entity(db, entity_db_id)?
        .into_iter()
        .filter(|library| principal_can_access_library(principal, library))
        .collect())
}

/// Every library holding the entity, the one the server picks for the entity first.
pub(crate) fn system_libraries_for_entity(
    db: &impl lyra_db::DbAccess,
    _ctx: &SystemContext,
    entity_db_id: DbId,
) -> anyhow::Result<Vec<Library>> {
    lyra_db::libraries::get_for_entity(db, entity_db_id)
}

/// Return the unique entity ids accessible to the principal.
pub(crate) fn accessible_entities(
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
    entity_db_ids: &[DbId],
) -> anyhow::Result<HashSet<DbId>> {
    if principal.permissions.contains(&lyra_db::Permission::Admin) {
        return Ok(lyra_db::dedup_positive_ids(entity_db_ids)
            .into_iter()
            .collect());
    }

    let mut accessible = HashSet::new();
    for entity_db_id in lyra_db::dedup_positive_ids(entity_db_ids) {
        if entity_accessible(db, principal, entity_db_id)? {
            accessible.insert(entity_db_id);
        }
    }
    Ok(accessible)
}

pub(crate) fn entity_accessible(
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
    entity_db_id: DbId,
) -> anyhow::Result<bool> {
    if principal.permissions.contains(&lyra_db::Permission::Admin) {
        return Ok(true);
    }
    Ok(lyra_db::libraries::get_for_entity(db, entity_db_id)?
        .into_iter()
        .any(|library| principal_can_access_library(principal, &library)))
}

pub(crate) fn artist_accessible(
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
    artist_db_id: DbId,
) -> anyhow::Result<bool> {
    if principal.permissions.contains(&lyra_db::Permission::Admin) {
        return Ok(true);
    }
    for release in lyra_db::releases::get_by_artist(db, artist_db_id)? {
        let Some(release_db_id) = release.db_id.clone().map(DbId::from) else {
            continue;
        };
        if lyra_db::libraries::get_by_release(db, release_db_id)?
            .into_iter()
            .any(|library| principal_can_access_library(principal, &library))
        {
            return Ok(true);
        }
    }
    for track in lyra_db::tracks::get_by_artist(db, artist_db_id)? {
        let Some(track_db_id) = track.db_id.clone().map(DbId::from) else {
            continue;
        };
        for release in lyra_db::releases::get_by_track(db, track_db_id)? {
            let Some(release_db_id) = release.db_id.clone().map(DbId::from) else {
                continue;
            };
            if lyra_db::libraries::get_by_release(db, release_db_id)?
                .into_iter()
                .any(|library| principal_can_access_library(principal, &library))
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub(crate) fn require_entity_accessible<E>(
    db: &impl lyra_db::DbAccess,
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
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
    playlist_db_id: DbId,
) -> anyhow::Result<bool> {
    playlist_accessible_as(db, principal, principal.require(db)?, playlist_db_id)
}

/// [`playlist_accessible`] for callers checking many playlists, with `user_db_id` taken from
/// [`Principal::require`] under the same guard as `db`.
pub(crate) fn playlist_accessible_as(
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
    user_db_id: DbId,
    playlist_db_id: DbId,
) -> anyhow::Result<bool> {
    if principal.permissions.contains(&lyra_db::Permission::Admin) {
        return Ok(true);
    }
    playlist_visible_to_user(db, user_db_id, playlist_db_id)
}

/// Whether the user can see the playlist as a listener: a public playlist, or its own.
pub(crate) fn playlist_visible_to_user(
    db: &impl lyra_db::DbAccess,
    user_db_id: DbId,
    playlist_db_id: DbId,
) -> anyhow::Result<bool> {
    let Some(playlist) = lyra_db::playlists::get_by_id(db, playlist_db_id)? else {
        return Ok(false);
    };
    if playlist.is_public.unwrap_or(false) {
        return Ok(true);
    }
    Ok(lyra_db::playlists::get_owner(db, playlist_db_id)? == Some(user_db_id))
}

/// Whether the user can see the playlist as the plugin API shows playlists: its own, or a public
/// one that still has an owner. A deleted user's playlists stay behind without one.
pub(crate) fn playlist_visible_to_listener(
    db: &impl lyra_db::DbAccess,
    user_db_id: DbId,
    playlist_db_id: DbId,
) -> anyhow::Result<bool> {
    let Some(playlist) = lyra_db::playlists::get_by_id(db, playlist_db_id)? else {
        return Ok(false);
    };
    Ok(match lyra_db::playlists::get_owner(db, playlist_db_id)? {
        Some(owner_db_id) => owner_db_id == user_db_id || playlist.is_public.unwrap_or(false),
        None => false,
    })
}

/// Which playlists a caller sees.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum PlaylistView {
    /// [`playlist_accessible_as`]: its own and the public ones, and every one for an admin.
    #[default]
    Role,
    /// [`playlist_visible_to_listener`], whatever the caller's role.
    Listener,
}

impl PlaylistView {
    pub(crate) fn sees(
        self,
        db: &impl lyra_db::DbAccess,
        principal: &Principal,
        user_db_id: DbId,
        playlist_db_id: DbId,
    ) -> anyhow::Result<bool> {
        match self {
            Self::Role => playlist_accessible_as(db, principal, user_db_id, playlist_db_id),
            Self::Listener => playlist_visible_to_listener(db, user_db_id, playlist_db_id),
        }
    }
}

/// Whether the principal owns the playlist, verified under the guard of `db`. Anything but an
/// existing playlist is unowned; a graph search from a missing id would fail.
pub(crate) fn playlist_owned(
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
    playlist_db_id: DbId,
) -> anyhow::Result<bool> {
    let user_db_id = principal.require(db)?;
    if lyra_db::playlists::get_by_id(db, playlist_db_id)?.is_none() {
        return Ok(false);
    }
    Ok(lyra_db::playlists::get_owner(db, playlist_db_id)? == Some(user_db_id))
}

/// Whether the principal may delete the playlist: its owner, or an admin, for any user's
/// playlist. Changing a playlist's contents stays with its owner.
pub(crate) fn playlist_deletable(
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
    playlist_db_id: DbId,
) -> anyhow::Result<bool> {
    if !principal.permissions.contains(&lyra_db::Permission::Admin) {
        return playlist_owned(db, principal, playlist_db_id);
    }
    principal.require(db)?;
    Ok(lyra_db::playlists::get_by_id(db, playlist_db_id)?.is_some())
}

pub(crate) fn resolve_library_db_id(
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
    library_id: &str,
) -> Result<DbId, AccessError> {
    resolve_library(db, principal, library_id)?
        .db_id
        .ok_or_else(|| AccessError::LibraryNotFound(library_id.to_string()))
}

/// The library `library_id` names, when the principal can see it.
pub(crate) fn resolve_library(
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
    library_id: &str,
) -> Result<Library, AccessError> {
    let not_found = || AccessError::LibraryNotFound(library_id.to_string());
    let library_db_id = if principal.permissions.contains(&lyra_db::Permission::Admin) {
        if !principal.accessible_library_ids.contains(library_id) {
            return Err(not_found());
        }
        lyra_db::lookup::find_node_id_by_id(db, library_id)?.ok_or_else(not_found)?
    } else {
        let user_db_id = principal.require(db).map_err(anyhow::Error::from)?;
        lyra_db::libraries::find_node_id_accessible_to_user(db, user_db_id, library_id)?
            .ok_or_else(not_found)?
    };
    lyra_db::libraries::get_by_id(db, library_db_id)?.ok_or_else(not_found)
}

pub(crate) fn resolve_optional_library_filter(
    db: &impl lyra_db::DbAccess,
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
    use std::path::PathBuf;

    use agdb::DbAny;
    use nanoid::nanoid;

    use super::*;
    use lyra_db::{
        fixtures::{
            connect,
            insert_track,
            test_user,
        },
        libraries::LibraryInsert,
        playlists::Playlist,
        test_db::new_test_db,
    };

    #[test]
    fn a_shared_entity_reports_only_the_libraries_the_viewer_can_see() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let shown = lyra_db::test_db::insert_library(&mut db, "Shown", "/access/shown")?;
        let hidden = lyra_db::test_db::insert_library(&mut db, "Hidden", "/access/hidden")?;
        let release = lyra_db::fixtures::insert_release(&mut db, "Shared")?;
        let track = insert_track(&mut db, "Song")?;
        connect(&mut db, shown, release)?;
        connect(&mut db, release, track)?;
        let public = |db: &DbAny, library| -> anyhow::Result<String> {
            Ok(lyra_db::libraries::get_by_id(db, library)?
                .expect("library exists")
                .id)
        };
        let (shown_id, hidden_id) = (public(&db, shown)?, public(&db, hidden)?);
        let user = create_user(&mut db, "listener")?;
        let viewer = Principal::for_user(&db, user, Vec::new(), HashSet::from([shown_id.clone()]));
        let nobody = Principal::for_user(&db, user, Vec::new(), HashSet::new());
        let named = |db: &DbAny, principal: &Principal| -> anyhow::Result<Vec<Vec<String>>> {
            [release, track]
                .into_iter()
                .map(|entity| {
                    Ok(libraries_for_entity(db, principal, entity)?
                        .into_iter()
                        .map(|library| library.id)
                        .collect())
                })
                .collect()
        };
        let in_shown = vec![vec![shown_id.clone()], vec![shown_id.clone()]];

        assert_eq!(named(&db, &viewer)?, in_shown);
        connect(&mut db, hidden, release)?;
        assert_eq!(
            named(&db, &viewer)?,
            in_shown,
            "the hidden library changes nothing"
        );
        assert_eq!(named(&db, &nobody)?, vec![Vec::<String>::new(); 2]);

        let admin = Principal::for_user(
            &db,
            user,
            vec![lyra_db::Permission::Admin],
            HashSet::from([shown_id.clone(), hidden_id.clone()]),
        );
        let both = vec![shown_id, hidden_id];
        assert_eq!(named(&db, &admin)?, vec![both.clone(), both]);
        Ok(())
    }

    fn principal(db: &DbAny, user_db_id: DbId, permissions: Vec<lyra_db::Permission>) -> Principal {
        Principal::for_user(db, user_db_id, permissions, HashSet::new())
    }

    fn create_user(db: &mut DbAny, username: &str) -> anyhow::Result<DbId> {
        lyra_db::users::create(db, &test_user(username)?)
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
            path_key: lyra_db::libraries::path_key_for(&path),
            language: None,
            country: None,
        };
        db.transaction_mut(|transaction| -> anyhow::Result<Library> {
            if let Some(user_db_id) = creator_user_db_id {
                Ok(lyra_db::libraries::create_with_creator(
                    transaction,
                    request,
                    user_db_id,
                )?)
            } else {
                Ok(lyra_db::libraries::create_system(transaction, request)?)
            }
        })
    }

    #[test]
    fn library_views_enforce_principal_scope_and_system_shape() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_user(&mut db, "viewer")?;
        let visible = create_library(&mut db, "Visible", Some(user_db_id))?;
        let hidden = create_library(&mut db, "Hidden", None)?;
        let mut principal = principal(&db, user_db_id, vec![]);
        principal.accessible_library_ids =
            lyra_db::libraries::accessible_library_ids(&db, user_db_id)?;

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
    fn accessible_entities_accepts_any_visible_library_path() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_user(&mut db, "shared-track-viewer")?;
        let visible = create_library(&mut db, "Visible Shared", None)?;
        let hidden = create_library(&mut db, "Hidden Shared", None)?;
        let track_db_id = insert_track(&mut db, "Shared Track")?;

        connect(&mut db, visible.db_id.unwrap(), track_db_id)?;
        connect(&mut db, hidden.db_id.unwrap(), track_db_id)?;

        let mut principal = principal(&db, user_db_id, vec![]);
        principal.accessible_library_ids.insert(visible.id);
        assert!(entity_accessible(&db, &principal, track_db_id)?);
        assert_eq!(
            accessible_entities(&db, &principal, &[track_db_id])?,
            HashSet::from([track_db_id])
        );

        Ok(())
    }

    #[test]
    fn activity_on_entities_never_reveals_a_library_the_viewer_lacks() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let shown = lyra_db::test_db::insert_library(&mut db, "Shown", "/access/activity-shown")?;
        let hidden =
            lyra_db::test_db::insert_library(&mut db, "Hidden", "/access/activity-hidden")?;
        let mut catalog = |library| -> anyhow::Result<(DbId, DbId, DbId)> {
            let release = lyra_db::fixtures::insert_release(&mut db, "Release")?;
            let track = insert_track(&mut db, "Song")?;
            let artist = lyra_db::fixtures::insert_artist(&mut db, "Artist")?;
            connect(&mut db, library, release)?;
            connect(&mut db, release, track)?;
            lyra_db::fixtures::connect_credit(
                &mut db,
                release,
                artist,
                lyra_db::CreditType::Artist,
                None,
                0,
            )?;
            Ok((release, track, artist))
        };
        let shown_entities = catalog(shown)?;
        let hidden_entities = catalog(hidden)?;
        let shown_id = lyra_db::libraries::get_by_id(&db, shown)?
            .expect("library exists")
            .id;
        let listener = create_user(&mut db, "listener")?;
        let insider = create_user(&mut db, "insider")?;
        lyra_db::libraries::grant_access(
            &mut db,
            insider,
            hidden,
            lyra_db::libraries::AccessKind::ReadWrite,
        )?;
        for user in [listener, insider] {
            for (release, track, _) in [shown_entities, hidden_entities] {
                for target in [release, track] {
                    lyra_db::favorites::add(
                        &mut db,
                        user,
                        target,
                        lyra_db::favorites::FavoriteKind::Track,
                        1,
                    )?;
                    lyra_db::ratings::upsert(
                        &mut db,
                        user,
                        target,
                        lyra_db::ratings::RatingValue::new(5).expect("valid rating"),
                        1,
                    )?;
                }
                lyra_db::fixtures::insert_listen(&mut db, user, track, 1)?;
                lyra_db::tags::create(&mut db, user, track, "mood", "blue", 1)?;
            }
        }
        let principal = Principal::for_user(&db, listener, Vec::new(), HashSet::from([shown_id]));

        let (release, track, artist) = shown_entities;
        for entity in [release, track, artist] {
            assert!(entity_accessible(&db, &principal, entity)?);
        }
        let (release, track, artist) = hidden_entities;
        for entity in [release, track, artist] {
            assert!(!entity_accessible(&db, &principal, entity)?);
            assert!(libraries_for_entity(&db, &principal, entity)?.is_empty());
        }
        assert!(!entity_accessible(&db, &principal, listener)?);
        Ok(())
    }

    #[test]
    fn library_resolution_enforces_access_for_users_and_admins() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let owner_db_id = create_user(&mut db, "owner")?;
        let viewer_db_id = create_user(&mut db, "viewer")?;
        let library = create_library(&mut db, "Music", Some(owner_db_id))?;

        let owner = principal(&db, owner_db_id, vec![]);
        assert_eq!(
            resolve_library_db_id(&db, &owner, &library.id)?,
            library.db_id.unwrap()
        );

        let viewer = principal(&db, viewer_db_id, vec![]);
        assert!(matches!(
            resolve_library_db_id(&db, &viewer, &library.id),
            Err(AccessError::LibraryNotFound(_))
        ));

        let mut admin = principal(&db, viewer_db_id, vec![lyra_db::Permission::Admin]);
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
        let private_id = lyra_db::playlists::create(
            &mut db,
            &Playlist {
                db_id: None,
                id: nanoid!(),
                name: "Private".to_string(),
                description: None,
                is_public: Some(false),
                created_at_ms: None,
                updated_at_ms: None,
            },
            owner_db_id,
        )?;
        let public_id = lyra_db::playlists::create(
            &mut db,
            &Playlist {
                db_id: None,
                id: nanoid!(),
                name: "Public".to_string(),
                description: None,
                is_public: Some(true),
                created_at_ms: None,
                updated_at_ms: None,
            },
            owner_db_id,
        )?;

        let owner = principal(&db, owner_db_id, vec![]);
        let viewer = principal(&db, viewer_db_id, vec![]);
        let admin = principal(&db, viewer_db_id, vec![lyra_db::Permission::Admin]);
        assert!(playlist_accessible(&db, &owner, private_id)?);
        assert!(!playlist_accessible(&db, &viewer, private_id)?);
        assert!(playlist_accessible(&db, &viewer, public_id)?);
        assert!(playlist_accessible(&db, &admin, private_id)?);
        Ok(())
    }

    #[test]
    fn admins_may_delete_any_playlist_but_edit_only_their_own() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let owner_db_id = create_user(&mut db, "owner")?;
        let viewer_db_id = create_user(&mut db, "viewer")?;
        let admin_db_id = create_user(&mut db, "admin")?;
        let private_id = lyra_db::playlists::create(
            &mut db,
            &Playlist {
                db_id: None,
                id: nanoid!(),
                name: "Private".to_string(),
                description: None,
                is_public: Some(false),
                created_at_ms: None,
                updated_at_ms: None,
            },
            owner_db_id,
        )?;

        let owner = principal(&db, owner_db_id, vec![]);
        let viewer = principal(&db, viewer_db_id, vec![]);
        let admin = principal(&db, admin_db_id, vec![lyra_db::Permission::Admin]);
        assert!(playlist_deletable(&db, &owner, private_id)?);
        assert!(!playlist_deletable(&db, &viewer, private_id)?);
        assert!(playlist_deletable(&db, &admin, private_id)?);
        assert!(!playlist_owned(&db, &admin, private_id)?);
        assert!(
            !playlist_deletable(&db, &admin, viewer_db_id)?,
            "a user is no playlist"
        );
        Ok(())
    }

    #[test]
    fn empty_library_filter_is_invalid() {
        let db = new_test_db().unwrap();
        let principal = Principal::from_parts(
            DbId(1),
            "missing".to_string(),
            "missing".to_string(),
            Vec::new(),
            None,
            HashSet::new(),
        );
        assert!(matches!(
            resolve_optional_library_filter(&db, &principal, Some("  ")),
            Err(AccessError::InvalidRequest(_))
        ));
    }
}
