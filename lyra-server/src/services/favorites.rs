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
        Cursor,
        FavoriteEdge,
        FavoriteKind,
        HAS_MANY_CAP,
        LIST_HARD_LIMIT,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MutationOutcome {
    /// Whitelist passed; mutation applied.
    Applied(FavoriteKind),
    /// Target didn't resolve to a whitelisted kind, or a visibility gate rejected it.
    NotTargetable,
}

#[derive(Clone, Debug)]
pub(crate) struct ListPage {
    pub(crate) edges: Vec<FavoriteEdge>,
    pub(crate) next_cursor: Option<Cursor>,
}

/// Add or refresh a favorite. Atomic over resolve + whitelist + visibility + write.
pub(crate) fn add(
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

/// Opaque check — `false` for missing, non-whitelisted, or non-visible targets.
pub(crate) fn has(db: &DbAny, user_db_id: DbId, public_target_id: &str) -> anyhow::Result<bool> {
    let Some((target_db_id, _kind)) = resolve_targetable(db, user_db_id, public_target_id)? else {
        return Ok(false);
    };
    db::favorites::has(db, user_db_id, target_db_id)
}

/// Batch check. Missing / non-visible targets map to `false`. Errs above [`HAS_MANY_CAP`].
pub(crate) fn has_many(
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
        let favored = db::favorites::has_many(db, user_db_id, &db_ids)?;
        for (public_id, db_id) in resolved {
            let is_fav = favored.get(&db_id).copied().unwrap_or(false);
            response.insert(public_id, is_fav);
        }
    }

    Ok(response)
}

/// Paginated favorites list for one `kind`, with playlist visibility filtered on hydration.
/// `next_cursor.is_none()` is the sole termination signal.
pub(crate) fn list(
    db: &DbAny,
    user_db_id: DbId,
    kind: FavoriteKind,
    limit: u64,
    cursor: Option<Cursor>,
) -> anyhow::Result<ListPage> {
    let clamped_limit = limit.min(LIST_HARD_LIMIT).max(1);
    let page = db::favorites::list(db, user_db_id, kind, clamped_limit, cursor)?;

    let edges = if kind == FavoriteKind::Playlist {
        let mut visible = Vec::with_capacity(page.edges.len());
        for edge in page.edges {
            if playlist_is_visible(db, user_db_id, edge.target_db_id)? {
                visible.push(edge);
            }
        }
        visible
    } else {
        page.edges
    };

    Ok(ListPage {
        edges,
        next_cursor: page.next_cursor,
    })
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

pub(crate) fn add_by_db_id(
    db: &mut DbAny,
    user_db_id: DbId,
    target_db_id: DbId,
) -> anyhow::Result<MutationOutcome> {
    let now_ms = now_ms()?;
    db.transaction_mut(|t| -> anyhow::Result<MutationOutcome> {
        let Some((target_db_id, kind)) = resolve_targetable_by_db_id(t, user_db_id, target_db_id)?
        else {
            return Ok(MutationOutcome::NotTargetable);
        };
        db::favorites::add(t, user_db_id, target_db_id, kind, now_ms)?;
        Ok(MutationOutcome::Applied(kind))
    })
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

pub(crate) fn has_by_db_id(
    db: &DbAny,
    user_db_id: DbId,
    target_db_id: DbId,
) -> anyhow::Result<bool> {
    let Some((target_db_id, _)) = resolve_targetable_by_db_id(db, user_db_id, target_db_id)? else {
        return Ok(false);
    };
    db::favorites::has(db, user_db_id, target_db_id)
}

pub(crate) fn has_many_by_db_id(
    db: &DbAny,
    user_db_id: DbId,
    target_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, bool>> {
    if target_db_ids.len() > HAS_MANY_CAP {
        bail!(
            "has_many cap exceeded: {} > {HAS_MANY_CAP}",
            target_db_ids.len(),
        );
    }

    let mut resolved: Vec<DbId> = Vec::with_capacity(target_db_ids.len());
    let mut response: HashMap<DbId, bool> = HashMap::with_capacity(target_db_ids.len());
    for &id in target_db_ids {
        match resolve_targetable_by_db_id(db, user_db_id, id)? {
            Some((valid_db_id, _)) => {
                resolved.push(valid_db_id);
            }
            None => {
                response.insert(id, false);
            }
        }
    }
    if !resolved.is_empty() {
        let favored = db::favorites::has_many(db, user_db_id, &resolved)?;
        for id in resolved {
            response.insert(id, favored.get(&id).copied().unwrap_or(false));
        }
    }
    Ok(response)
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
    use crate::db::test_db::new_test_db;
    use crate::db::{
        Playlist,
        users,
    };
    use agdb::{
        DbAny,
        QueryBuilder,
    };
    use nanoid::nanoid;

    fn create_user(db: &mut DbAny, username: &str) -> anyhow::Result<DbId> {
        users::create(db, &users::test_user(username)?)
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
        assert_eq!(
            list(&db, other, FavoriteKind::Playlist, 10, None)?
                .edges
                .len(),
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
            list(&db, other, FavoriteKind::Playlist, 10, None)?
                .edges
                .is_empty(),
            "flipped-to-private playlist must be dropped from non-owner's list",
        );
        assert_eq!(
            list(&db, owner, FavoriteKind::Playlist, 10, None)?
                .edges
                .len(),
            0,
            "owner never favorited it, so it shouldn't appear on their list either",
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
