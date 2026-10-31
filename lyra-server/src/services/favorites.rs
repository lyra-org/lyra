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

use crate::services::auth::{
    Principal,
    access,
};
use lyra_db::favorites::FavoriteKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MutationOutcome {
    Applied,
    /// The target is not a favoritable kind, or a visibility gate rejected it.
    NotTargetable,
}

/// Add or refresh a favorite. Atomic over whitelist + visibility + write.
pub(crate) fn add_for_principal(
    db: &mut DbAny,
    principal: &Principal,
    target_db_id: DbId,
) -> anyhow::Result<MutationOutcome> {
    let now_ms = now_ms()?;
    db.transaction_mut(|t| -> anyhow::Result<MutationOutcome> {
        let user_db_id = principal.require(t)?;
        let Some(kind) = targetable_kind(t, principal, user_db_id, target_db_id)? else {
            return Ok(MutationOutcome::NotTargetable);
        };
        lyra_db::favorites::add(t, user_db_id, target_db_id, kind, now_ms)?;
        Ok(MutationOutcome::Applied)
    })
}

/// Remove a favorite. No visibility gate — a caller must always be able to evict their own
/// edge, else a "ghost favorite" reappears if the target becomes visible again.
pub(crate) fn remove(
    db: &mut DbAny,
    user_db_id: DbId,
    target_db_id: DbId,
) -> anyhow::Result<MutationOutcome> {
    db.transaction_mut(|t| -> anyhow::Result<MutationOutcome> {
        if lyra_db::favorites::target_kind(t, target_db_id)?.is_none() {
            return Ok(MutationOutcome::NotTargetable);
        }
        lyra_db::favorites::remove(t, user_db_id, target_db_id)?;
        Ok(MutationOutcome::Applied)
    })
}

pub(crate) fn has_for_principal(
    db: &DbAny,
    principal: &Principal,
    target_db_id: DbId,
) -> anyhow::Result<bool> {
    let user_db_id = principal.require(db)?;
    if targetable_kind(db, principal, user_db_id, target_db_id)?.is_none() {
        return Ok(false);
    }
    lyra_db::favorites::has(db, user_db_id, target_db_id)
}

/// When the caller first favorited each target it can see, in Unix milliseconds; unfavorited
/// targets are absent.
pub(crate) fn favorited_at_for_principal(
    db: &DbAny,
    principal: &Principal,
    target_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, i64>> {
    let user_db_id = principal.require(db)?;
    let mut targetable = Vec::with_capacity(target_db_ids.len());
    for &target_db_id in target_db_ids {
        if targetable_kind(db, principal, user_db_id, target_db_id)?.is_some() {
            targetable.push(target_db_id);
        }
    }
    Ok(
        lyra_db::favorites::for_targets(db, user_db_id, &targetable)?
            .into_iter()
            .map(|(target_db_id, edge)| (target_db_id, edge.first_favorited_at_ms))
            .collect(),
    )
}

/// Flat target DbIds for user+kind, with playlist visibility applied. Errs above the DB cap.
pub(crate) fn list_ids(
    db: &DbAny,
    user_db_id: DbId,
    kind: FavoriteKind,
) -> anyhow::Result<Vec<DbId>> {
    let ids = lyra_db::favorites::list_ids(db, user_db_id, kind)?;
    if kind == FavoriteKind::Playlist {
        let mut visible = Vec::with_capacity(ids.len());
        for id in ids {
            if access::playlist_visible_to_user(db, user_db_id, id)? {
                visible.push(id);
            }
        }
        Ok(visible)
    } else {
        Ok(ids)
    }
}

/// `user_db_id` comes from [`Principal::require`] under the same guard as `db`.
fn targetable_kind(
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
    user_db_id: DbId,
    target_db_id: DbId,
) -> anyhow::Result<Option<FavoriteKind>> {
    let Some(kind) = lyra_db::favorites::target_kind(db, target_db_id)? else {
        return Ok(None);
    };
    if !target_visible_to_principal(db, principal, user_db_id, target_db_id, kind)? {
        return Ok(None);
    }
    Ok(Some(kind))
}

/// `user_db_id` comes from [`Principal::require`] under the same guard as `db`.
fn target_visible_to_principal(
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
    user_db_id: DbId,
    target_db_id: DbId,
    kind: FavoriteKind,
) -> anyhow::Result<bool> {
    match kind {
        FavoriteKind::Playlist => access::playlist_visible_to_user(db, user_db_id, target_db_id),
        FavoriteKind::Artist => access::artist_accessible(db, principal, target_db_id),
        FavoriteKind::Track | FavoriteKind::Release => {
            access::entity_accessible(db, principal, target_db_id)
        }
    }
}

fn now_ms() -> anyhow::Result<i64> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    Ok(nanos as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    use agdb::{
        DbAny,
        QueryBuilder,
    };
    use lyra_db::fixtures::{
        connect,
        test_user,
    };
    use lyra_db::test_db::{
        insert_library,
        new_test_db,
    };
    use lyra_db::{
        Playlist,
        users,
    };
    use nanoid::nanoid;

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
            created_at_ms: None,
            updated_at_ms: None,
        };
        lyra_db::playlists::create(db, &playlist, owner_db_id)
    }

    fn setup_id_index(db: &mut DbAny) -> anyhow::Result<()> {
        lyra_db::indexes::ensure_index(db, "id")?;
        Ok(())
    }

    fn principal_for(
        db: &DbAny,
        user_db_id: DbId,
        accessible_library_ids: HashSet<String>,
    ) -> Principal {
        Principal::for_user(db, user_db_id, Vec::new(), accessible_library_ids)
    }

    fn set_playlist_public(
        db: &mut DbAny,
        playlist_db_id: DbId,
        public_id: &str,
        is_public: bool,
    ) -> anyhow::Result<()> {
        lyra_db::playlists::update(
            db,
            &Playlist {
                db_id: Some(playlist_db_id.into()),
                id: public_id.to_string(),
                name: "p".to_string(),
                description: None,
                is_public: Some(is_public),
                created_at_ms: None,
                updated_at_ms: None,
            },
        )?;
        Ok(())
    }

    #[test]
    fn add_applies_for_whitelisted_track() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;
        let library_db_id = insert_library(&mut db, "Favorites", "/tmp/lyra-favorites-add")?;
        let library_public_id = lyra_db::lookup::find_id_by_db_id(&db, library_db_id)?
            .expect("library should have a public id");
        let track_db_id = create_track(&mut db, &nanoid!())?;
        connect(&mut db, library_db_id, track_db_id)?;

        let principal = principal_for(&db, user, HashSet::from([library_public_id]));
        let outcome = add_for_principal(&mut db, &principal, track_db_id)?;
        assert_eq!(outcome, MutationOutcome::Applied);
        assert!(has_for_principal(&db, &principal, track_db_id)?);

        Ok(())
    }

    #[test]
    fn missing_db_ids_are_not_targetable() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;

        let principal = principal_for(&db, user, HashSet::new());
        for target in [DbId(999_999_999), DbId(0), DbId(-5)] {
            assert_eq!(
                add_for_principal(&mut db, &principal, target)?,
                MutationOutcome::NotTargetable,
            );
            assert_eq!(
                remove(&mut db, user, target)?,
                MutationOutcome::NotTargetable
            );
            assert!(!has_for_principal(&db, &principal, target)?);
        }

        Ok(())
    }

    #[test]
    fn add_returns_not_targetable_for_non_whitelisted_kind() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;
        let other_node = db
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values([[("id", nanoid!().as_str()).into()]])
                    .query(),
            )?
            .ids()[0];

        let principal = principal_for(&db, user, HashSet::new());
        let outcome = add_for_principal(&mut db, &principal, other_node)?;
        assert_eq!(outcome, MutationOutcome::NotTargetable);

        Ok(())
    }

    #[test]
    fn add_rejects_whitelist_bypass_via_real_user() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let caller = create_user(&mut db, "alice")?;
        let bob_db_id = create_user(&mut db, "bob")?;

        let principal = principal_for(&db, caller, HashSet::new());
        let outcome = add_for_principal(&mut db, &principal, bob_db_id)?;
        assert_eq!(
            outcome,
            MutationOutcome::NotTargetable,
            "caller must not be able to favorite a user node",
        );

        Ok(())
    }

    #[test]
    fn playlist_visibility_gate_blocks_non_owner_non_public() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let owner = create_user(&mut db, "alice")?;
        let intruder = create_user(&mut db, "bob")?;
        let playlist = create_playlist(&mut db, owner, &nanoid!(), /* is_public= */ false)?;

        let principal = principal_for(&db, intruder, HashSet::new());
        let outcome = add_for_principal(&mut db, &principal, playlist)?;
        assert_eq!(
            outcome,
            MutationOutcome::NotTargetable,
            "non-owner non-public PUT on private playlist must be opaque 404",
        );

        assert!(
            !has_for_principal(&db, &principal, playlist)?,
            "has must return false for a non-visible private playlist",
        );

        Ok(())
    }

    #[test]
    fn playlist_visibility_gate_allows_owner() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let owner = create_user(&mut db, "alice")?;
        let playlist = create_playlist(&mut db, owner, &nanoid!(), false)?;

        let principal = principal_for(&db, owner, HashSet::new());
        let outcome = add_for_principal(&mut db, &principal, playlist)?;
        assert_eq!(outcome, MutationOutcome::Applied);
        assert!(has_for_principal(&db, &principal, playlist)?);

        Ok(())
    }

    #[test]
    fn playlist_visibility_gate_allows_public_non_owner() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let owner = create_user(&mut db, "alice")?;
        let other = create_user(&mut db, "bob")?;
        let playlist = create_playlist(&mut db, owner, &nanoid!(), true)?;

        let principal = principal_for(&db, other, HashSet::new());
        let outcome = add_for_principal(&mut db, &principal, playlist)?;
        assert_eq!(outcome, MutationOutcome::Applied);
        Ok(())
    }

    #[test]
    fn favorited_at_for_principal_returns_visible_favorites_and_caps_existing_ids()
    -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;
        let unsupported_user = create_user(&mut db, "unsupported")?;
        let visible_library = insert_library(&mut db, "Visible", "/tmp/lyra-favorites-check")?;
        let hidden_library = insert_library(&mut db, "Hidden", "/tmp/lyra-favorites-check-hidden")?;
        let visible_library_id = lyra_db::lookup::find_id_by_db_id(&db, visible_library)?.unwrap();
        let principal = principal_for(&db, user, HashSet::from([visible_library_id]));
        let favorited = create_track(&mut db, &nanoid!())?;
        let unfavorited = create_track(&mut db, &nanoid!())?;
        let hidden = create_track(&mut db, &nanoid!())?;
        connect(&mut db, visible_library, favorited)?;
        connect(&mut db, visible_library, unfavorited)?;
        connect(&mut db, hidden_library, hidden)?;
        add_for_principal(&mut db, &principal, favorited)?;
        lyra_db::favorites::add(&mut db, user, hidden, FavoriteKind::Track, now_ms()?)?;

        let result = favorited_at_for_principal(
            &db,
            &principal,
            &[
                favorited,
                unfavorited,
                hidden,
                unsupported_user,
                DbId(999_999_999),
            ],
        )?;
        assert_eq!(result.keys().copied().collect::<Vec<_>>(), vec![favorited]);
        assert!(favorited_at_for_principal(&db, &principal, &[])?.is_empty());

        let others = db
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values(
                        (0..2000)
                            .map(|_| vec![("id", nanoid!().as_str()).into()])
                            .collect::<Vec<_>>(),
                    )
                    .query(),
            )?
            .ids();
        let many = [others.as_slice(), &[favorited]].concat();
        let result = favorited_at_for_principal(&db, &principal, &many)?;
        assert_eq!(result.keys().copied().collect::<Vec<_>>(), vec![favorited]);

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

        let principal = principal_for(&db, other, HashSet::new());
        let added = add_for_principal(&mut db, &principal, playlist_db_id)?;
        assert_eq!(added, MutationOutcome::Applied);

        set_playlist_public(&mut db, playlist_db_id, &pub_id, false)?;
        assert!(!has_for_principal(&db, &principal, playlist_db_id)?);

        let removed = remove(&mut db, other, playlist_db_id)?;
        assert_eq!(
            removed,
            MutationOutcome::Applied,
            "DELETE on non-visible playlist must still be Applied (idempotent)",
        );

        set_playlist_public(&mut db, playlist_db_id, &pub_id, true)?;
        assert!(
            !has_for_principal(&db, &principal, playlist_db_id)?,
            "after DELETE, re-visibility must not resurrect a ghost favorite",
        );

        Ok(())
    }

    #[test]
    fn remove_still_rejects_non_whitelisted_node() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let caller = create_user(&mut db, "alice")?;
        let victim_db_id = create_user(&mut db, "victim")?;

        let outcome = remove(&mut db, caller, victim_db_id)?;
        assert_eq!(
            outcome,
            MutationOutcome::NotTargetable,
            "DELETE must still reject nodes of non-whitelisted kinds",
        );

        Ok(())
    }

    #[test]
    fn remove_idempotent_over_whitelisted_kind() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;
        let track = create_track(&mut db, &nanoid!())?;

        let outcome = remove(&mut db, user, track)?;
        assert_eq!(
            outcome,
            MutationOutcome::Applied,
            "remove on valid kind without an existing edge is still Applied",
        );

        Ok(())
    }
}
