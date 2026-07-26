// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashMap,
    time::{
        SystemTime,
        UNIX_EPOCH,
    },
};

use agdb::{
    DbAny,
    DbId,
};

use crate::{
    db::{
        self,
        ratings::{
            RatingKind,
            RatingValue,
        },
    },
    services::auth::Principal,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MutationOutcome {
    Applied(RatingKind),
    NotTargetable,
}

pub(crate) fn set_for_principal(
    db: &mut DbAny,
    principal: &Principal,
    public_target_id: &str,
    value: RatingValue,
) -> anyhow::Result<MutationOutcome> {
    let now_ms = now_ms()?;
    db.transaction_mut(|t| -> anyhow::Result<MutationOutcome> {
        let Some((target_db_id, kind)) =
            resolve_targetable_for_principal(t, principal, public_target_id)?
        else {
            return Ok(MutationOutcome::NotTargetable);
        };
        db::ratings::upsert(t, principal.user_db_id, target_db_id, kind, value, now_ms)?;
        Ok(MutationOutcome::Applied(kind))
    })
}

/// No visibility gate: callers must be able to remove their own stored state after access changes.
pub(crate) fn remove(
    db: &mut DbAny,
    user_db_id: DbId,
    public_target_id: &str,
) -> anyhow::Result<MutationOutcome> {
    db.transaction_mut(|t| -> anyhow::Result<MutationOutcome> {
        let Some((target_db_id, kind)) = resolve_whitelisted(t, public_target_id)? else {
            return Ok(MutationOutcome::NotTargetable);
        };
        db::ratings::remove(t, user_db_id, target_db_id)?;
        Ok(MutationOutcome::Applied(kind))
    })
}

pub(crate) fn get_for_principal(
    db: &DbAny,
    principal: &Principal,
    public_target_id: &str,
) -> anyhow::Result<Option<RatingValue>> {
    let Some((target_db_id, kind)) =
        resolve_targetable_for_principal(db, principal, public_target_id)?
    else {
        return Ok(None);
    };
    let Some(edge) = db::ratings::get(db, principal.user_db_id, target_db_id)? else {
        return Ok(None);
    };
    if edge.kind != kind {
        return Ok(None);
    }
    Ok(Some(edge.value))
}

pub(crate) fn values_for_principal(
    db: &DbAny,
    principal: &Principal,
    public_target_ids: &[String],
) -> anyhow::Result<HashMap<String, Option<RatingValue>>> {
    let mut response: HashMap<String, Option<RatingValue>> = public_target_ids
        .iter()
        .cloned()
        .map(|public_id| (public_id, None))
        .collect();
    let unique_public_ids: Vec<String> = response.keys().cloned().collect();
    let public_id_refs: Vec<&str> = unique_public_ids.iter().map(String::as_str).collect();
    let resolved_ids = db::lookup::find_node_ids_by_ids(db, &public_id_refs)?;
    let target_db_ids: Vec<DbId> = resolved_ids.values().copied().collect();
    let stored = db::ratings::values_for_targets(db, principal.user_db_id, &target_db_ids)?;

    for public_id in unique_public_ids {
        let Some(target_db_id) = resolved_ids.get(&public_id).copied() else {
            continue;
        };
        let Some(edge) = stored.get(&target_db_id) else {
            continue;
        };
        let Some(actual_kind) = target_kind(db, target_db_id)? else {
            continue;
        };
        if !target_visible_to_principal(db, principal, target_db_id)? {
            continue;
        }
        if edge.kind == actual_kind {
            response.insert(public_id, Some(edge.value));
        }
    }

    Ok(response)
}

fn resolve_targetable_for_principal(
    db: &impl db::DbAccess,
    principal: &Principal,
    public_target_id: &str,
) -> anyhow::Result<Option<(DbId, RatingKind)>> {
    let Some((target_db_id, kind)) = resolve_whitelisted(db, public_target_id)? else {
        return Ok(None);
    };
    if !target_visible_to_principal(db, principal, target_db_id)? {
        return Ok(None);
    }
    Ok(Some((target_db_id, kind)))
}

fn resolve_whitelisted(
    db: &impl db::DbAccess,
    public_target_id: &str,
) -> anyhow::Result<Option<(DbId, RatingKind)>> {
    let Some(target_db_id) = db::lookup::find_node_id_by_id(db, public_target_id)? else {
        return Ok(None);
    };
    Ok(target_kind(db, target_db_id)?.map(|kind| (target_db_id, kind)))
}

fn target_kind(db: &impl db::DbAccess, target_db_id: DbId) -> anyhow::Result<Option<RatingKind>> {
    Ok(db::entities::metadata_entity_type(db, target_db_id)?.map(RatingKind::from))
}

fn target_visible_to_principal(
    db: &impl db::DbAccess,
    principal: &Principal,
    target_db_id: DbId,
) -> anyhow::Result<bool> {
    Ok(db::libraries::get_for_entity(db, target_db_id)?
        .into_iter()
        .any(|library| principal.accessible_library_ids.contains(&library.id)))
}

fn now_ms() -> anyhow::Result<i64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    use crate::db::{
        test_db::{
            connect,
            insert_artist,
            insert_library,
            insert_release,
            insert_track,
            new_test_db,
            test_user,
        },
        users,
    };

    fn principal(user_db_id: DbId, accessible_library_ids: HashSet<String>) -> Principal {
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
    fn set_get_update_and_remove_rating() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = users::create(&mut db, &test_user("alice")?)?;
        let library = insert_library(&mut db, "Music", "/tmp/lyra-ratings-service")?;
        let library_public_id = db::lookup::find_id_by_db_id(&db, library)?.unwrap();
        let track = insert_track(&mut db, "Track")?;
        let track_public_id = db::lookup::find_id_by_db_id(&db, track)?.unwrap();
        connect(&mut db, library, track)?;
        let principal = principal(user, HashSet::from([library_public_id]));

        let outcome = set_for_principal(
            &mut db,
            &principal,
            &track_public_id,
            RatingValue::new(2).unwrap(),
        )?;
        assert_eq!(outcome, MutationOutcome::Applied(RatingKind::Track));
        assert_eq!(
            get_for_principal(&db, &principal, &track_public_id)?
                .unwrap()
                .get(),
            2,
        );

        set_for_principal(
            &mut db,
            &principal,
            &track_public_id,
            RatingValue::new(5).unwrap(),
        )?;
        assert_eq!(
            get_for_principal(&db, &principal, &track_public_id)?
                .unwrap()
                .get(),
            5,
        );

        assert_eq!(
            remove(&mut db, user, &track_public_id)?,
            MutationOutcome::Applied(RatingKind::Track),
        );
        assert!(get_for_principal(&db, &principal, &track_public_id)?.is_none());
        Ok(())
    }

    #[test]
    fn values_for_principal_returns_dense_mixed_kind_state_with_opaque_nulls() -> anyhow::Result<()>
    {
        let mut db = new_test_db()?;
        let user = users::create(&mut db, &test_user("alice")?)?;
        let other_user = users::create(&mut db, &test_user("bob")?)?;
        let visible_library = insert_library(&mut db, "Visible", "/tmp/lyra-ratings-check")?;
        let hidden_library = insert_library(&mut db, "Hidden", "/tmp/lyra-ratings-check-hidden")?;
        let visible_library_id = db::lookup::find_id_by_db_id(&db, visible_library)?.unwrap();
        let principal = principal(user, HashSet::from([visible_library_id]));

        let track = insert_track(&mut db, "Rated Track")?;
        let release = insert_release(&mut db, "Rated Release")?;
        let artist = insert_artist(&mut db, "Rated Artist")?;
        let unrated = insert_track(&mut db, "Unrated Track")?;
        let other_user_rating = insert_track(&mut db, "Other User Rating")?;
        let hidden = insert_track(&mut db, "Hidden Rating")?;
        let kind_mismatch = insert_track(&mut db, "Kind Mismatch")?;
        for target in [
            track,
            release,
            artist,
            unrated,
            other_user_rating,
            kind_mismatch,
        ] {
            connect(&mut db, visible_library, target)?;
        }
        connect(&mut db, hidden_library, hidden)?;

        for (target, kind, value) in [
            (track, RatingKind::Track, 2),
            (release, RatingKind::Release, 3),
            (artist, RatingKind::Artist, 4),
            (hidden, RatingKind::Track, 5),
            (kind_mismatch, RatingKind::Artist, 5),
        ] {
            db::ratings::upsert(
                &mut db,
                user,
                target,
                kind,
                RatingValue::new(value).unwrap(),
                100,
            )?;
        }
        db::ratings::upsert(
            &mut db,
            other_user,
            other_user_rating,
            RatingKind::Track,
            RatingValue::new(5).unwrap(),
            100,
        )?;

        let public_id = |target| db::lookup::find_id_by_db_id(&db, target).map(Option::unwrap);
        let track_id = public_id(track)?;
        let release_id = public_id(release)?;
        let artist_id = public_id(artist)?;
        let unrated_id = public_id(unrated)?;
        let other_user_rating_id = public_id(other_user_rating)?;
        let hidden_id = public_id(hidden)?;
        let kind_mismatch_id = public_id(kind_mismatch)?;
        let unsupported_id = db::users::get_by_id(&db, other_user)?.unwrap().id;
        let missing_id = "missing-id".to_string();
        let malformed_id = "bad/id".to_string();
        let submitted = vec![
            track_id.clone(),
            track_id.clone(),
            release_id.clone(),
            artist_id.clone(),
            unrated_id.clone(),
            other_user_rating_id.clone(),
            hidden_id.clone(),
            kind_mismatch_id.clone(),
            unsupported_id.clone(),
            missing_id.clone(),
            malformed_id.clone(),
        ];

        let values = values_for_principal(&db, &principal, &submitted)?;
        assert_eq!(values.len(), submitted.iter().collect::<HashSet<_>>().len());
        assert_eq!(
            values
                .get(&track_id)
                .copied()
                .flatten()
                .map(RatingValue::get),
            Some(2)
        );
        assert_eq!(
            values
                .get(&release_id)
                .copied()
                .flatten()
                .map(RatingValue::get),
            Some(3),
        );
        assert_eq!(
            values
                .get(&artist_id)
                .copied()
                .flatten()
                .map(RatingValue::get),
            Some(4),
        );
        for opaque_id in [
            unrated_id,
            other_user_rating_id,
            hidden_id,
            kind_mismatch_id,
            unsupported_id,
            missing_id,
            malformed_id,
        ] {
            assert_eq!(values.get(&opaque_id), Some(&None), "{opaque_id}");
        }
        assert!(values_for_principal(&db, &principal, &[])?.is_empty());

        Ok(())
    }

    #[test]
    fn inaccessible_media_is_opaque_but_rating_can_be_removed() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = users::create(&mut db, &test_user("alice")?)?;
        let library = insert_library(&mut db, "Music", "/tmp/lyra-ratings-hidden")?;
        let library_public_id = db::lookup::find_id_by_db_id(&db, library)?.unwrap();
        let track = insert_track(&mut db, "Track")?;
        let track_public_id = db::lookup::find_id_by_db_id(&db, track)?.unwrap();
        connect(&mut db, library, track)?;
        let visible = principal(user, HashSet::from([library_public_id]));
        set_for_principal(
            &mut db,
            &visible,
            &track_public_id,
            RatingValue::new(4).unwrap(),
        )?;

        let hidden = principal(user, HashSet::new());
        assert!(get_for_principal(&db, &hidden, &track_public_id)?.is_none());
        assert_eq!(
            remove(&mut db, user, &track_public_id)?,
            MutationOutcome::Applied(RatingKind::Track),
        );
        Ok(())
    }

    #[test]
    fn rejects_non_media_targets() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = users::create(&mut db, &test_user("alice")?)?;
        let other_user = users::create(&mut db, &test_user("bob")?)?;
        let other_user_public_id = db::lookup::find_id_by_db_id(&db, other_user)?.unwrap();
        let principal = principal(user, HashSet::new());

        assert_eq!(
            set_for_principal(
                &mut db,
                &principal,
                &other_user_public_id,
                RatingValue::new(4).unwrap(),
            )?,
            MutationOutcome::NotTargetable,
        );
        Ok(())
    }
}
