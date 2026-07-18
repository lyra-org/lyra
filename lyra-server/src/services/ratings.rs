// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::time::{
    SystemTime,
    UNIX_EPOCH,
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
            insert_library,
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
