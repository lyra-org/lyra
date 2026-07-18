// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;
use std::time::{
    SystemTime,
    UNIX_EPOCH,
};

use agdb::{
    DbAny,
    DbId,
};
use anyhow::bail;

use crate::db::{
    self,
    favorites::{
        FavoriteKind,
        HAS_MANY_CAP,
    },
};
use crate::services::auth::Principal;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MutationOutcome {
    /// Whitelist passed; mutation applied.
    Applied(FavoriteKind),
    /// Target didn't resolve to a whitelisted kind, or a visibility gate rejected it.
    NotTargetable,
}

#[derive(Clone, Debug)]
pub(crate) struct ListItem {
    pub(crate) edge_db_id: DbId,
    pub(crate) target_id: String,
    pub(crate) kind: FavoriteKind,
    pub(crate) first_favorited_at_ms: i64,
    pub(crate) last_refreshed_at_ms: i64,
}

impl ListItem {
    pub(crate) fn snapshot_id(&self) -> String {
        format!("{}:{}", self.edge_db_id.0, self.target_id)
    }
}

/// Add or refresh a favorite. Atomic over resolve + whitelist + visibility + write.
pub(crate) fn add_for_principal(
    db: &mut DbAny,
    principal: &Principal,
    public_target_id: &str,
) -> anyhow::Result<MutationOutcome> {
    let now_ms = now_ms()?;
    db.transaction_mut(|t| -> anyhow::Result<MutationOutcome> {
        let Some((target_db_id, kind)) =
            resolve_targetable_for_principal(t, principal, public_target_id)?
        else {
            return Ok(MutationOutcome::NotTargetable);
        };
        db::favorites::add(t, principal.user_db_id, target_db_id, kind, now_ms)?;
        Ok(MutationOutcome::Applied(kind))
    })
}

/// Remove a favorite. No visibility gate — a caller must always be able to evict their own
/// edge, else a "ghost favorite" reappears if the target becomes visible again.
pub(crate) fn remove(
    db: &mut DbAny,
    user_db_id: DbId,
    public_target_id: &str,
) -> anyhow::Result<MutationOutcome> {
    db.transaction_mut(|t| -> anyhow::Result<MutationOutcome> {
        let Some((target_db_id, kind)) = resolve_whitelisted(t, public_target_id)? else {
            return Ok(MutationOutcome::NotTargetable);
        };
        db::favorites::remove(t, user_db_id, target_db_id)?;
        Ok(MutationOutcome::Applied(kind))
    })
}

pub(crate) fn has_for_principal(
    db: &DbAny,
    principal: &Principal,
    public_target_id: &str,
) -> anyhow::Result<bool> {
    let Some((target_db_id, _kind)) =
        resolve_targetable_for_principal(db, principal, public_target_id)?
    else {
        return Ok(false);
    };
    db::favorites::has(db, principal.user_db_id, target_db_id)
}

pub(crate) fn has_many_for_principal(
    db: &DbAny,
    principal: &Principal,
    public_target_ids: &[String],
) -> anyhow::Result<HashMap<String, bool>> {
    if public_target_ids.len() > HAS_MANY_CAP {
        bail!(
            "has_many cap exceeded: {} > {HAS_MANY_CAP}",
            public_target_ids.len(),
        );
    }

    let mut resolved: Vec<(String, DbId)> = Vec::with_capacity(public_target_ids.len());
    let mut response: HashMap<String, bool> = HashMap::with_capacity(public_target_ids.len());
    for public_id in public_target_ids {
        match resolve_targetable_for_principal(db, principal, public_id)? {
            Some((target_db_id, _)) => {
                resolved.push((public_id.clone(), target_db_id));
            }
            None => {
                response.insert(public_id.clone(), false);
            }
        }
    }

    if !resolved.is_empty() {
        let db_ids: Vec<DbId> = resolved.iter().map(|(_, id)| *id).collect();
        let states = db::favorites::has_many(db, principal.user_db_id, &db_ids)?;
        for (public_id, db_id) in resolved {
            let is_fav = states.get(&db_id).copied().unwrap_or(false);
            response.insert(public_id, is_fav);
        }
    }

    Ok(response)
}

/// Favorites list for one `kind`, with target visibility filtered on hydration.
pub(crate) fn list(
    db: &DbAny,
    principal: &Principal,
    kind: FavoriteKind,
) -> anyhow::Result<Vec<ListItem>> {
    let edges = db::favorites::list(db, principal.user_db_id, kind)?;
    let target_db_ids: Vec<DbId> = edges.iter().map(|edge| edge.target_db_id).collect();
    let target_ids = db::lookup::find_ids_by_db_ids(db, &target_db_ids)?;

    let mut items = Vec::with_capacity(edges.len());
    for edge in edges {
        if target_visible_to_principal(db, principal, edge.target_db_id, edge.kind)?
            && let Some(target_id) = target_ids.get(&edge.target_db_id)
        {
            items.push(ListItem {
                edge_db_id: edge.db_id,
                target_id: target_id.clone(),
                kind: edge.kind,
                first_favorited_at_ms: edge.first_favorited_at_ms,
                last_refreshed_at_ms: edge.last_refreshed_at_ms,
            });
        }
    }
    Ok(items)
}

pub(crate) fn hydrate_snapshot(
    db: &DbAny,
    principal: &Principal,
    kind: FavoriteKind,
    snapshot_ids: &[String],
) -> anyhow::Result<Vec<ListItem>> {
    let identities = snapshot_ids
        .iter()
        .filter_map(|snapshot_id| {
            let (edge_db_id, target_id) = snapshot_id.split_once(':')?;
            Some((DbId(edge_db_id.parse::<i64>().ok()?), target_id))
        })
        .collect::<Vec<_>>();
    let edge_ids = identities
        .iter()
        .map(|(edge_db_id, _)| *edge_db_id)
        .collect::<Vec<_>>();
    let mut edges = db::favorites::get_by_ids(db, principal.user_db_id, kind, &edge_ids)?;
    let target_db_ids = edges
        .values()
        .map(|edge| edge.target_db_id)
        .collect::<Vec<_>>();
    let target_ids = db::lookup::find_ids_by_db_ids(db, &target_db_ids)?;

    let mut items = Vec::with_capacity(identities.len());
    for (edge_db_id, expected_target_id) in identities {
        let Some(edge) = edges.remove(&edge_db_id) else {
            continue;
        };
        if !target_visible_to_principal(db, principal, edge.target_db_id, edge.kind)? {
            continue;
        }
        let Some(target_id) = target_ids.get(&edge.target_db_id) else {
            continue;
        };
        if target_id != expected_target_id {
            continue;
        }
        items.push(ListItem {
            edge_db_id,
            target_id: target_id.clone(),
            kind: edge.kind,
            first_favorited_at_ms: edge.first_favorited_at_ms,
            last_refreshed_at_ms: edge.last_refreshed_at_ms,
        });
    }
    Ok(items)
}

/// Flat target DbIds for user+kind, with playlist visibility applied. Errs above the DB cap.
pub(crate) fn list_ids(
    db: &DbAny,
    user_db_id: DbId,
    kind: FavoriteKind,
) -> anyhow::Result<Vec<DbId>> {
    let ids = db::favorites::list_ids(db, user_db_id, kind)?;
    if kind == FavoriteKind::Playlist {
        let mut visible = Vec::with_capacity(ids.len());
        for id in ids {
            if playlist_is_visible(db, user_db_id, id)? {
                visible.push(id);
            }
        }
        Ok(visible)
    } else {
        Ok(ids)
    }
}

fn resolve_targetable_for_principal(
    db: &impl db::DbAccess,
    principal: &Principal,
    public_target_id: &str,
) -> anyhow::Result<Option<(DbId, FavoriteKind)>> {
    let Some(target_db_id) = db::lookup::find_node_id_by_id(db, public_target_id)? else {
        return Ok(None);
    };
    let Some((target_db_id, kind)) = resolve_whitelisted_by_db_id(db, target_db_id)? else {
        return Ok(None);
    };
    if !target_visible_to_principal(db, principal, target_db_id, kind)? {
        return Ok(None);
    }
    Ok(Some((target_db_id, kind)))
}

fn target_visible_to_principal(
    db: &impl db::DbAccess,
    principal: &Principal,
    target_db_id: DbId,
    kind: FavoriteKind,
) -> anyhow::Result<bool> {
    match kind {
        FavoriteKind::Playlist => playlist_is_visible(db, principal.user_db_id, target_db_id),
        FavoriteKind::Track | FavoriteKind::Release | FavoriteKind::Artist => {
            Ok(db::libraries::get_for_entity(db, target_db_id)?
                .into_iter()
                .any(|library| principal.accessible_library_ids.contains(&library.id)))
        }
    }
}

/// No visibility gate.
fn resolve_whitelisted(
    db: &impl db::DbAccess,
    public_target_id: &str,
) -> anyhow::Result<Option<(DbId, FavoriteKind)>> {
    let Some(target_db_id) = db::lookup::find_node_id_by_id(db, public_target_id)? else {
        return Ok(None);
    };
    resolve_whitelisted_by_db_id(db, target_db_id)
}

fn resolve_whitelisted_by_db_id(
    db: &impl db::DbAccess,
    target_db_id: DbId,
) -> anyhow::Result<Option<(DbId, FavoriteKind)>> {
    let Some(kind) = db::favorites::target_kind(db, target_db_id)? else {
        return Ok(None);
    };
    Ok(Some((target_db_id, kind)))
}

pub(crate) fn remove_by_db_id(
    db: &mut DbAny,
    user_db_id: DbId,
    target_db_id: DbId,
) -> anyhow::Result<MutationOutcome> {
    db.transaction_mut(|t| -> anyhow::Result<MutationOutcome> {
        let Some((target_db_id, kind)) = resolve_whitelisted_by_db_id(t, target_db_id)? else {
            return Ok(MutationOutcome::NotTargetable);
        };
        db::favorites::remove(t, user_db_id, target_db_id)?;
        Ok(MutationOutcome::Applied(kind))
    })
}

fn playlist_is_visible(
    db: &impl db::DbAccess,
    user_db_id: DbId,
    playlist_db_id: DbId,
) -> anyhow::Result<bool> {
    let Some(playlist) = db::playlists::get_by_id(db, playlist_db_id)? else {
        return Ok(false);
    };
    if playlist.is_public.unwrap_or(false) {
        return Ok(true);
    }
    let owner = db::playlists::get_owner(db, playlist_db_id)?;
    Ok(owner == Some(user_db_id))
}

fn now_ms() -> anyhow::Result<i64> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    Ok(nanos as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    use crate::db::test_db::{
        connect,
        insert_library,
        new_test_db,
        test_user,
    };
    use crate::db::{
        Playlist,
        users,
    };
    use agdb::{
        DbAny,
        QueryBuilder,
    };
    use nanoid::nanoid;

    fn add(
        db: &mut DbAny,
        user_db_id: DbId,
        public_target_id: &str,
    ) -> anyhow::Result<MutationOutcome> {
        let now_ms = now_ms()?;
        db.transaction_mut(|t| -> anyhow::Result<MutationOutcome> {
            let Some((target_db_id, kind)) = resolve_targetable(t, user_db_id, public_target_id)?
            else {
                return Ok(MutationOutcome::NotTargetable);
            };
            db::favorites::add(t, user_db_id, target_db_id, kind, now_ms)?;
            Ok(MutationOutcome::Applied(kind))
        })
    }

    fn has(db: &DbAny, user_db_id: DbId, public_target_id: &str) -> anyhow::Result<bool> {
        let Some((target_db_id, _kind)) = resolve_targetable(db, user_db_id, public_target_id)?
        else {
            return Ok(false);
        };
        db::favorites::has(db, user_db_id, target_db_id)
    }

    fn has_many(
        db: &DbAny,
        user_db_id: DbId,
        public_target_ids: &[String],
    ) -> anyhow::Result<HashMap<String, bool>> {
        if public_target_ids.len() > HAS_MANY_CAP {
            bail!(
                "has_many cap exceeded: {} > {HAS_MANY_CAP}",
                public_target_ids.len(),
            );
        }

        let mut resolved: Vec<(String, DbId)> = Vec::with_capacity(public_target_ids.len());
        let mut response: HashMap<String, bool> = HashMap::with_capacity(public_target_ids.len());
        for public_id in public_target_ids {
            match resolve_targetable(db, user_db_id, public_id)? {
                Some((target_db_id, _)) => {
                    resolved.push((public_id.clone(), target_db_id));
                }
                None => {
                    response.insert(public_id.clone(), false);
                }
            }
        }

        if !resolved.is_empty() {
            let db_ids: Vec<DbId> = resolved.iter().map(|(_, id)| *id).collect();
            let states = db::favorites::has_many(db, user_db_id, &db_ids)?;
            for (public_id, db_id) in resolved {
                let is_fav = states.get(&db_id).copied().unwrap_or(false);
                response.insert(public_id, is_fav);
            }
        }

        Ok(response)
    }

    fn resolve_targetable(
        db: &impl db::DbAccess,
        user_db_id: DbId,
        public_target_id: &str,
    ) -> anyhow::Result<Option<(DbId, FavoriteKind)>> {
        let Some(target_db_id) = db::lookup::find_node_id_by_id(db, public_target_id)? else {
            return Ok(None);
        };
        resolve_targetable_by_db_id(db, user_db_id, target_db_id)
    }

    fn resolve_targetable_by_db_id(
        db: &impl db::DbAccess,
        user_db_id: DbId,
        target_db_id: DbId,
    ) -> anyhow::Result<Option<(DbId, FavoriteKind)>> {
        let Some((target_db_id, kind)) = resolve_whitelisted_by_db_id(db, target_db_id)? else {
            return Ok(None);
        };
        if kind == FavoriteKind::Playlist && !playlist_is_visible(db, user_db_id, target_db_id)? {
            return Ok(None);
        }
        Ok(Some((target_db_id, kind)))
    }

    fn create_user(db: &mut DbAny, username: &str) -> anyhow::Result<DbId> {
        users::create(db, &test_user(username)?)
    }

    fn create_track(db: &mut DbAny, public_id: &str) -> anyhow::Result<DbId> {
        let track_db_id = db
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values([[("id", public_id).into(), ("track_title", "Track").into()]])
                    .query(),
            )?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("tracks")
                .to(track_db_id)
                .query(),
        )?;
        Ok(track_db_id)
    }

    fn create_playlist(
        db: &mut DbAny,
        owner_db_id: DbId,
        public_id: &str,
        is_public: bool,
    ) -> anyhow::Result<DbId> {
        let playlist = Playlist {
            db_id: None,
            id: public_id.to_string(),
            name: "p".to_string(),
            description: None,
            is_public: Some(is_public),
            created_at: None,
            updated_at: None,
        };
        db::playlists::create(db, &playlist, owner_db_id)
    }

    fn setup_id_index(db: &mut DbAny) -> anyhow::Result<()> {
        db::indexes::ensure_index(db, "id")?;
        Ok(())
    }

    fn principal_for(user_db_id: DbId, accessible_library_ids: HashSet<String>) -> Principal {
        Principal {
            user_db_id,
            user_public_id: format!("user-{}", user_db_id.0),
            username: format!("user-{}", user_db_id.0),
            permissions: Vec::new(),
            role_name: None,
            accessible_library_ids,
        }
    }

    #[test]
    fn add_applies_for_whitelisted_track() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;
        let track_id = nanoid!();
        create_track(&mut db, &track_id)?;

        let outcome = add(&mut db, user, &track_id)?;
        assert!(matches!(
            outcome,
            MutationOutcome::Applied(FavoriteKind::Track)
        ));
        assert!(has(&db, user, &track_id)?);

        Ok(())
    }

    #[test]
    fn add_returns_not_targetable_for_unknown_nanoid() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;

        let outcome = add(&mut db, user, "unknown-id-string")?;
        assert_eq!(outcome, MutationOutcome::NotTargetable);

        Ok(())
    }

    #[test]
    fn add_returns_not_targetable_for_non_whitelisted_kind() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;
        let other_user_id = nanoid!();
        let other_user = db.exec_mut(
            QueryBuilder::insert()
                .nodes()
                .values([[("id", other_user_id.as_str()).into()]])
                .query(),
        )?;
        let _ = (user, other_user);

        let outcome = add(&mut db, user, &other_user_id)?;
        assert_eq!(outcome, MutationOutcome::NotTargetable);

        Ok(())
    }

    #[test]
    fn add_rejects_whitelist_bypass_via_real_user_nanoid() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let caller = create_user(&mut db, "alice")?;
        let bob_db_id = create_user(&mut db, "bob")?;
        let bob_public_id = crate::db::users::get_by_id(&db, bob_db_id)?
            .expect("bob should exist")
            .id;

        let outcome = add(&mut db, caller, &bob_public_id)?;
        assert_eq!(
            outcome,
            MutationOutcome::NotTargetable,
            "caller must not be able to favorite a user node via its real public nanoid",
        );

        Ok(())
    }

    #[test]
    fn playlist_visibility_gate_blocks_non_owner_non_public() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let owner = create_user(&mut db, "alice")?;
        let intruder = create_user(&mut db, "bob")?;
        let pub_id = nanoid!();
        create_playlist(&mut db, owner, &pub_id, /* is_public= */ false)?;

        let outcome = add(&mut db, intruder, &pub_id)?;
        assert_eq!(
            outcome,
            MutationOutcome::NotTargetable,
            "non-owner non-public PUT on private playlist must be opaque 404",
        );

        assert!(
            !has(&db, intruder, &pub_id)?,
            "has must return false for a non-visible private playlist",
        );

        Ok(())
    }

    #[test]
    fn playlist_visibility_gate_allows_owner() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let owner = create_user(&mut db, "alice")?;
        let pub_id = nanoid!();
        create_playlist(&mut db, owner, &pub_id, false)?;

        let outcome = add(&mut db, owner, &pub_id)?;
        assert!(matches!(
            outcome,
            MutationOutcome::Applied(FavoriteKind::Playlist)
        ));
        assert!(has(&db, owner, &pub_id)?);

        Ok(())
    }

    #[test]
    fn playlist_visibility_gate_allows_public_non_owner() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let owner = create_user(&mut db, "alice")?;
        let other = create_user(&mut db, "bob")?;
        let pub_id = nanoid!();
        create_playlist(&mut db, owner, &pub_id, true)?;

        let outcome = add(&mut db, other, &pub_id)?;
        assert!(matches!(
            outcome,
            MutationOutcome::Applied(FavoriteKind::Playlist)
        ));
        Ok(())
    }

    #[test]
    fn list_filters_non_visible_playlists_after_flip() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let owner = create_user(&mut db, "alice")?;
        let other = create_user(&mut db, "bob")?;
        let pub_id = nanoid!();
        let playlist_db_id = create_playlist(&mut db, owner, &pub_id, true)?;

        add(&mut db, other, &pub_id)?;
        let other_principal = principal_for(other, HashSet::new());
        assert_eq!(
            list(&db, &other_principal, FavoriteKind::Playlist)?.len(),
            1,
            "public playlist should hydrate for non-owner",
        );

        db::playlists::update(
            &mut db,
            &Playlist {
                db_id: Some(playlist_db_id.into()),
                id: pub_id.clone(),
                name: "p".to_string(),
                description: None,
                is_public: Some(false),
                created_at: None,
                updated_at: None,
            },
        )?;

        assert!(
            list(&db, &other_principal, FavoriteKind::Playlist)?.is_empty(),
            "flipped-to-private playlist must be dropped from non-owner's list",
        );
        let owner_principal = principal_for(owner, HashSet::new());
        assert_eq!(
            list(&db, &owner_principal, FavoriteKind::Playlist)?.len(),
            0,
            "owner never favorited it, so it shouldn't appear on their list either",
        );

        Ok(())
    }

    #[test]
    fn list_filters_tracks_without_library_access() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;
        let library_db_id = insert_library(&mut db, "Favorites", "/tmp/lyra-favorites-list")?;
        let library_public_id = db::lookup::find_id_by_db_id(&db, library_db_id)?
            .expect("library should have a public id");
        let track_public_id = nanoid!();
        let track_db_id = create_track(&mut db, &track_public_id)?;
        connect(&mut db, library_db_id, track_db_id)?;

        let visible_principal = principal_for(user, HashSet::from([library_public_id]));
        let hidden_principal = principal_for(user, HashSet::new());

        let added = add_for_principal(&mut db, &visible_principal, &track_public_id)?;
        assert!(matches!(
            added,
            MutationOutcome::Applied(FavoriteKind::Track)
        ));
        let visible_items = list(&db, &visible_principal, FavoriteKind::Track)?;
        assert_eq!(
            visible_items.len(),
            1,
            "track favorite should list while the library is accessible",
        );
        assert_eq!(visible_items[0].target_id, track_public_id);
        assert!(
            list(&db, &hidden_principal, FavoriteKind::Track)?.is_empty(),
            "track favorite must be hidden without access to the containing library",
        );

        Ok(())
    }

    #[test]
    fn has_many_dense_response_false_for_invalid_and_visible_for_favorited() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;
        let track_id = nanoid!();
        create_track(&mut db, &track_id)?;
        add(&mut db, user, &track_id)?;

        let bad_id = "nonexistent".to_string();
        let result = has_many(&db, user, &[track_id.clone(), bad_id.clone()])?;
        assert_eq!(result.len(), 2);
        assert_eq!(result.get(&track_id), Some(&true));
        assert_eq!(
            result.get(&bad_id),
            Some(&false),
            "invalid nanoid must map to false, not omitted",
        );

        Ok(())
    }

    #[test]
    fn remove_bypasses_visibility_gate_for_private_playlist() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let owner = create_user(&mut db, "alice")?;
        let other = create_user(&mut db, "bob")?;
        let pub_id = nanoid!();
        let playlist_db_id = create_playlist(&mut db, owner, &pub_id, /* is_public= */ true)?;

        let added = add(&mut db, other, &pub_id)?;
        assert!(matches!(
            added,
            MutationOutcome::Applied(FavoriteKind::Playlist)
        ));

        db::playlists::update(
            &mut db,
            &db::Playlist {
                db_id: Some(playlist_db_id.into()),
                id: pub_id.clone(),
                name: "p".to_string(),
                description: None,
                is_public: Some(false),
                created_at: None,
                updated_at: None,
            },
        )?;

        assert!(!has(&db, other, &pub_id)?);

        let removed = remove(&mut db, other, &pub_id)?;
        assert!(
            matches!(removed, MutationOutcome::Applied(FavoriteKind::Playlist)),
            "DELETE on non-visible playlist must still be Applied (idempotent)",
        );

        db::playlists::update(
            &mut db,
            &db::Playlist {
                db_id: Some(playlist_db_id.into()),
                id: pub_id.clone(),
                name: "p".to_string(),
                description: None,
                is_public: Some(true),
                created_at: None,
                updated_at: None,
            },
        )?;
        assert!(
            !has(&db, other, &pub_id)?,
            "after DELETE, re-visibility must not resurrect a ghost favorite",
        );

        Ok(())
    }

    #[test]
    fn remove_still_rejects_non_whitelisted_nanoid() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let caller = create_user(&mut db, "alice")?;
        let victim_db_id = create_user(&mut db, "victim")?;
        let victim_public_id = db::users::get_by_id(&db, victim_db_id)?
            .expect("victim exists")
            .id;

        let outcome = remove(&mut db, caller, &victim_public_id)?;
        assert_eq!(
            outcome,
            MutationOutcome::NotTargetable,
            "DELETE must still reject nanoids that resolve to non-whitelisted kinds",
        );

        Ok(())
    }

    #[test]
    fn remove_idempotent_over_whitelisted_kind() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;
        let track_id = nanoid!();
        create_track(&mut db, &track_id)?;

        let outcome = remove(&mut db, user, &track_id)?;
        assert!(
            matches!(outcome, MutationOutcome::Applied(FavoriteKind::Track)),
            "remove on valid kind without an existing edge is still Applied",
        );

        Ok(())
    }
}
