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

use crate::services::auth::Principal;
use lyra_db::{
    entities::MetadataEntityType,
    ratings::RatingValue,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MutationOutcome {
    Applied,
    NotTargetable,
}

pub(crate) fn set_for_principal(
    db: &mut DbAny,
    principal: &Principal,
    target_db_id: DbId,
    value: RatingValue,
) -> anyhow::Result<MutationOutcome> {
    let now_ms = now_ms()?;
    db.transaction_mut(|t| -> anyhow::Result<MutationOutcome> {
        if !targetable_by_principal(t, principal, target_db_id)? {
            return Ok(MutationOutcome::NotTargetable);
        }
        lyra_db::ratings::upsert(t, principal.require(t)?, target_db_id, value, now_ms)?;
        Ok(MutationOutcome::Applied)
    })
}

/// No visibility gate: callers must be able to remove their own stored state after access changes.
pub(crate) fn remove(
    db: &mut DbAny,
    user_db_id: DbId,
    target_db_id: DbId,
) -> anyhow::Result<MutationOutcome> {
    db.transaction_mut(|t| -> anyhow::Result<MutationOutcome> {
        if target_kind(t, target_db_id)?.is_none() {
            return Ok(MutationOutcome::NotTargetable);
        }
        lyra_db::ratings::remove(t, user_db_id, target_db_id)?;
        Ok(MutationOutcome::Applied)
    })
}

pub(crate) fn get_for_principal(
    db: &DbAny,
    principal: &Principal,
    target_db_id: DbId,
) -> anyhow::Result<Option<RatingValue>> {
    if !targetable_by_principal(db, principal, target_db_id)? {
        return Ok(None);
    }
    Ok(lyra_db::ratings::get(db, principal.require(db)?, target_db_id)?.map(|edge| edge.value))
}

/// The caller's ratings of the targets it can see; unrated targets are absent.
pub(crate) fn values_for_principal(
    db: &DbAny,
    principal: &Principal,
    target_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, RatingValue>> {
    let mut values = HashMap::new();
    for (target_db_id, edge) in
        lyra_db::ratings::values_for_targets(db, principal.require(db)?, target_db_ids)?
    {
        if targetable_by_principal(db, principal, target_db_id)? {
            values.insert(target_db_id, edge.value);
        }
    }
    Ok(values)
}

fn targetable_by_principal(
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
    target_db_id: DbId,
) -> anyhow::Result<bool> {
    match target_kind(db, target_db_id)? {
        None => Ok(false),
        Some(MetadataEntityType::Artist) => {
            crate::services::auth::access::artist_accessible(db, principal, target_db_id)
        }
        Some(MetadataEntityType::Track | MetadataEntityType::Release) => {
            crate::services::auth::access::entity_accessible(db, principal, target_db_id)
        }
    }
}

/// Plugins pass raw ids, so an id naming no node is untargetable rather than an error.
fn target_kind(
    db: &impl lyra_db::DbAccess,
    target_db_id: DbId,
) -> anyhow::Result<Option<MetadataEntityType>> {
    if lyra_db::lookup::find_id_by_db_id(db, target_db_id)?.is_none() {
        return Ok(None);
    }
    lyra_db::entities::metadata_entity_type(db, target_db_id)
}

fn now_ms() -> anyhow::Result<i64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    use lyra_db::{
        fixtures::{
            connect,
            insert_artist,
            insert_release,
            insert_track,
            test_user,
        },
        test_db::{
            connect_artist,
            insert_library,
            new_test_db,
        },
        users,
    };

    fn principal(
        db: &DbAny,
        user_db_id: DbId,
        accessible_library_ids: HashSet<String>,
    ) -> Principal {
        Principal::for_user(db, user_db_id, Vec::new(), accessible_library_ids)
    }

    fn rate(db: &mut DbAny, user: DbId, target: DbId, value: u8) -> anyhow::Result<()> {
        lyra_db::ratings::upsert(db, user, target, RatingValue::new(value).unwrap(), 100)
    }

    #[test]
    fn set_get_update_and_remove_rating() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = users::create(&mut db, &test_user("alice")?)?;
        let library = insert_library(&mut db, "Music", "/tmp/lyra-ratings-service")?;
        let library_public_id = lyra_db::lookup::find_id_by_db_id(&db, library)?.unwrap();
        let track = insert_track(&mut db, "Track")?;
        connect(&mut db, library, track)?;
        let principal = principal(&db, user, HashSet::from([library_public_id]));

        let outcome = set_for_principal(&mut db, &principal, track, RatingValue::new(2).unwrap())?;
        assert_eq!(outcome, MutationOutcome::Applied);
        assert_eq!(get_for_principal(&db, &principal, track)?.unwrap().get(), 2);

        set_for_principal(&mut db, &principal, track, RatingValue::new(5).unwrap())?;
        assert_eq!(get_for_principal(&db, &principal, track)?.unwrap().get(), 5);

        assert_eq!(remove(&mut db, user, track)?, MutationOutcome::Applied);
        assert!(get_for_principal(&db, &principal, track)?.is_none());
        Ok(())
    }

    #[test]
    fn values_for_principal_returns_visible_mixed_kind_ratings() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = users::create(&mut db, &test_user("alice")?)?;
        let other_user = users::create(&mut db, &test_user("bob")?)?;
        let visible_library = insert_library(&mut db, "Visible", "/tmp/lyra-ratings-check")?;
        let hidden_library = insert_library(&mut db, "Hidden", "/tmp/lyra-ratings-check-hidden")?;
        let visible_library_id = lyra_db::lookup::find_id_by_db_id(&db, visible_library)?.unwrap();
        let principal = principal(&db, user, HashSet::from([visible_library_id]));

        let track = insert_track(&mut db, "Rated Track")?;
        let release = insert_release(&mut db, "Rated Release")?;
        let artist = insert_artist(&mut db, "Rated Artist")?;
        let unrated = insert_track(&mut db, "Unrated Track")?;
        let other_user_rating = insert_track(&mut db, "Other User Rating")?;
        let hidden = insert_track(&mut db, "Hidden Rating")?;
        for target in [track, release, unrated, other_user_rating] {
            connect(&mut db, visible_library, target)?;
        }
        connect_artist(&mut db, release, artist)?;
        connect(&mut db, hidden_library, hidden)?;

        for (target, value) in [(track, 2), (release, 3), (artist, 4), (hidden, 5)] {
            rate(&mut db, user, target, value)?;
        }
        rate(&mut db, other_user, other_user_rating, 5)?;

        let values = values_for_principal(
            &db,
            &principal,
            &[
                track,
                release,
                artist,
                unrated,
                other_user_rating,
                hidden,
                other_user,
                DbId(999_999_999),
            ],
        )?;
        let values: HashMap<DbId, u8> = values
            .into_iter()
            .map(|(id, value)| (id, value.get()))
            .collect();
        assert_eq!(
            values,
            HashMap::from([(track, 2), (release, 3), (artist, 4)])
        );
        assert!(values_for_principal(&db, &principal, &[])?.is_empty());
        Ok(())
    }

    #[test]
    fn inaccessible_media_is_opaque_but_rating_can_be_removed() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = users::create(&mut db, &test_user("alice")?)?;
        let library = insert_library(&mut db, "Music", "/tmp/lyra-ratings-hidden")?;
        let library_public_id = lyra_db::lookup::find_id_by_db_id(&db, library)?.unwrap();
        let track = insert_track(&mut db, "Track")?;
        connect(&mut db, library, track)?;
        let visible = principal(&db, user, HashSet::from([library_public_id]));
        set_for_principal(&mut db, &visible, track, RatingValue::new(4).unwrap())?;

        let hidden = principal(&db, user, HashSet::new());
        assert!(get_for_principal(&db, &hidden, track)?.is_none());
        assert_eq!(remove(&mut db, user, track)?, MutationOutcome::Applied);
        Ok(())
    }

    #[test]
    fn rejects_non_media_targets() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = users::create(&mut db, &test_user("alice")?)?;
        let other_user = users::create(&mut db, &test_user("bob")?)?;
        let principal = principal(&db, user, HashSet::new());

        for target in [other_user, DbId(999_999_999)] {
            assert_eq!(
                set_for_principal(&mut db, &principal, target, RatingValue::new(4).unwrap())?,
                MutationOutcome::NotTargetable,
            );
            assert_eq!(
                remove(&mut db, user, target)?,
                MutationOutcome::NotTargetable
            );
        }
        Ok(())
    }
}
