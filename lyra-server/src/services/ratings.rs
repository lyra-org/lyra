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

use serde::Serialize;

use crate::services::{
    STATE,
    auth::Principal,
    plugin_dispatch::send_update,
};
use lyra_db::{
    entities::MetadataEntityType,
    ratings::RatingValue,
};

/// One change to a user's ratings, as plugins see it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct RatingUpdatePayload {
    pub event: RatingEvent,
    pub user_id: String,
    pub target_id: String,
    pub target_type: &'static str,
    /// The rating a `set` left.
    pub rating: Option<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RatingEvent {
    Set,
    Removed,
}

fn update(
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
    target_db_id: DbId,
    kind: MetadataEntityType,
    rating: Option<RatingValue>,
) -> anyhow::Result<Option<RatingUpdatePayload>> {
    Ok(
        lyra_db::lookup::find_id_by_db_id(db, target_db_id)?.map(|target_id| RatingUpdatePayload {
            event: if rating.is_some() {
                RatingEvent::Set
            } else {
                RatingEvent::Removed
            },
            user_id: principal.user_public_id.clone(),
            target_id,
            target_type: kind.as_str(),
            rating: rating.map(RatingValue::get),
        }),
    )
}

/// Announces the change a committed write made, if it made one.
fn announce<T>(written: (T, Option<RatingUpdatePayload>)) -> T {
    let (outcome, update) = written;
    if let Some(update) = update {
        send_update("rating", |dispatch| dispatch.dispatch_rating_update(update));
    }
    outcome
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MutationOutcome {
    Applied,
    NotTargetable,
}

/// Sets the caller's rating of a target it can see, announcing it to plugins.
pub(crate) fn set_for_principal(
    db: &mut DbAny,
    principal: &Principal,
    target_db_id: DbId,
    value: RatingValue,
) -> anyhow::Result<MutationOutcome> {
    let now_ms = now_ms()?;
    db.transaction_mut(|t| -> anyhow::Result<_> {
        let Some(kind) = targetable_kind(t, principal, target_db_id)? else {
            return Ok((MutationOutcome::NotTargetable, None));
        };
        lyra_db::ratings::upsert(t, principal.require(t)?, target_db_id, value, now_ms)?;
        let update = update(t, principal, target_db_id, kind, Some(value))?;
        Ok((MutationOutcome::Applied, update))
    })
    .map(announce)
}

/// Clears the caller's rating even of a target it can no longer see, so stored state can be
/// removed after access changes. A hidden target still answers `NotTargetable`, so removal does
/// not reveal that it exists. A rating that was cleared is announced to plugins.
pub(crate) fn remove_for_principal(
    db: &mut DbAny,
    principal: &Principal,
    target_db_id: DbId,
) -> anyhow::Result<MutationOutcome> {
    db.transaction_mut(|t| -> anyhow::Result<_> {
        let Some(kind) = target_kind(t, target_db_id)? else {
            return Ok((MutationOutcome::NotTargetable, None));
        };
        let update = if lyra_db::ratings::remove(t, principal.require(t)?, target_db_id)? {
            update(t, principal, target_db_id, kind, None)?
        } else {
            None
        };
        let outcome = if targetable_by_principal(t, principal, target_db_id)? {
            MutationOutcome::Applied
        } else {
            MutationOutcome::NotTargetable
        };
        Ok((outcome, update))
    })
    .map(announce)
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

/// Every user's average rating of the targets the caller can see, or nothing while the
/// `instance_stats` setting is off; unrated targets are absent.
pub(crate) fn averages_for_principal(
    db: &DbAny,
    principal: &Principal,
    target_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, f64>> {
    if !STATE.config().instance_stats {
        return Ok(HashMap::new());
    }
    let existing: Vec<DbId> = lyra_db::lookup::find_ids_by_db_ids(db, target_db_ids)?
        .into_keys()
        .collect();
    let mut averages = HashMap::new();
    for (target_db_id, average) in lyra_db::ratings::averages_for_targets(db, &existing)? {
        if targetable_by_principal(db, principal, target_db_id)? {
            averages.insert(target_db_id, average);
        }
    }
    Ok(averages)
}

fn targetable_by_principal(
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
    target_db_id: DbId,
) -> anyhow::Result<bool> {
    Ok(targetable_kind(db, principal, target_db_id)?.is_some())
}

/// What the target is, when the caller can rate it.
fn targetable_kind(
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
    target_db_id: DbId,
) -> anyhow::Result<Option<MetadataEntityType>> {
    let Some(kind) = target_kind(db, target_db_id)? else {
        return Ok(None);
    };
    let accessible = match kind {
        MetadataEntityType::Artist => {
            crate::services::auth::access::artist_accessible(db, principal, target_db_id)?
        }
        MetadataEntityType::Track | MetadataEntityType::Release => {
            crate::services::auth::access::entity_accessible(db, principal, target_db_id)?
        }
    };
    Ok(accessible.then_some(kind))
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
    fn every_change_to_a_rating_is_announced_once() -> anyhow::Result<()> {
        let (mut db, _state) = crate::services::testing::publishing_test_db()?;
        let dispatch = crate::services::testing::record_plugin_dispatch();
        let user = users::create(&mut db, &test_user("alice")?)?;
        let library = insert_library(&mut db, "Music", "/tmp/lyra-ratings-announced")?;
        let library_id = lyra_db::lookup::find_id_by_db_id(&db, library)?.unwrap();
        let release = insert_release(&mut db, "Release")?;
        connect(&mut db, library, release)?;
        let release_id = lyra_db::lookup::find_id_by_db_id(&db, release)?.unwrap();
        let hidden = insert_track(&mut db, "Hidden")?;
        let principal = principal(&db, user, HashSet::from([library_id]));
        let announced = || dispatch.rating_updates();

        // Nothing changes: a target that can't be rated, and no rating to clear.
        set_for_principal(&mut db, &principal, hidden, RatingValue::new(3).unwrap())?;
        remove_for_principal(&mut db, &principal, release)?;
        assert_eq!(announced(), vec![]);

        set_for_principal(&mut db, &principal, release, RatingValue::new(2).unwrap())?;
        set_for_principal(&mut db, &principal, release, RatingValue::new(5).unwrap())?;
        remove_for_principal(&mut db, &principal, release)?;

        let update = |event, rating| RatingUpdatePayload {
            event,
            user_id: principal.user_public_id.clone(),
            target_id: release_id.clone(),
            target_type: "release",
            rating,
        };
        assert_eq!(
            announced(),
            vec![
                update(RatingEvent::Set, Some(2)),
                update(RatingEvent::Set, Some(5)),
                update(RatingEvent::Removed, None),
            ]
        );
        Ok(())
    }

    #[test]
    fn set_get_update_and_remove_rating() -> anyhow::Result<()> {
        let (mut db, _state) = crate::services::testing::publishing_test_db()?;
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

        assert_eq!(
            remove_for_principal(&mut db, &principal, track)?,
            MutationOutcome::Applied
        );
        assert!(get_for_principal(&db, &principal, track)?.is_none());
        Ok(())
    }

    #[test]
    fn values_for_principal_returns_visible_mixed_kind_ratings() -> anyhow::Result<()> {
        let (mut db, _state) = crate::services::testing::publishing_test_db()?;
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

    #[tokio::test]
    async fn averages_cover_every_user_for_visible_targets_while_instance_stats_is_on()
    -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;

        let mut db = lyra_db::test_db::new_test_db()?;
        let user = users::create(&mut db, &test_user("alice")?)?;
        let other_user = users::create(&mut db, &test_user("bob")?)?;
        let outsider = users::create(&mut db, &test_user("carol")?)?;
        let visible_library = insert_library(&mut db, "Visible", "/tmp/lyra-ratings-average")?;
        let hidden_library = insert_library(&mut db, "Hidden", "/tmp/lyra-ratings-average-hidden")?;
        let visible_library_id = lyra_db::lookup::find_id_by_db_id(&db, visible_library)?.unwrap();
        let principal = principal(&db, user, HashSet::from([visible_library_id]));

        let track = insert_track(&mut db, "Shared")?;
        let unrated = insert_track(&mut db, "Unrated")?;
        let hidden = insert_track(&mut db, "Hidden")?;
        connect(&mut db, visible_library, track)?;
        connect(&mut db, visible_library, unrated)?;
        connect(&mut db, hidden_library, hidden)?;
        rate(&mut db, user, track, 2)?;
        rate(&mut db, other_user, track, 3)?;
        rate(&mut db, outsider, track, 5)?;
        rate(&mut db, other_user, hidden, 4)?;

        let targets = [track, unrated, hidden, DbId(999_999_999)];
        assert_eq!(
            averages_for_principal(&db, &principal, &targets)?,
            HashMap::from([(track, 10.0 / 3.0)]),
        );

        let mut config = STATE.config().as_ref().clone();
        config.instance_stats = false;
        crate::services::testing::publish_config(config);
        assert!(averages_for_principal(&db, &principal, &targets)?.is_empty());
        Ok(())
    }

    #[test]
    fn inaccessible_media_is_opaque_but_its_rating_is_still_cleared() -> anyhow::Result<()> {
        let (mut db, _state) = crate::services::testing::publishing_test_db()?;
        let user = users::create(&mut db, &test_user("alice")?)?;
        let library = insert_library(&mut db, "Music", "/tmp/lyra-ratings-hidden")?;
        let library_public_id = lyra_db::lookup::find_id_by_db_id(&db, library)?.unwrap();
        let track = insert_track(&mut db, "Track")?;
        connect(&mut db, library, track)?;
        let visible = principal(&db, user, HashSet::from([library_public_id]));
        set_for_principal(&mut db, &visible, track, RatingValue::new(4).unwrap())?;

        let hidden = principal(&db, user, HashSet::new());
        assert!(get_for_principal(&db, &hidden, track)?.is_none());
        assert_eq!(
            remove_for_principal(&mut db, &hidden, track)?,
            MutationOutcome::NotTargetable
        );
        assert!(get_for_principal(&db, &visible, track)?.is_none());
        Ok(())
    }

    #[test]
    fn rejects_non_media_targets() -> anyhow::Result<()> {
        let (mut db, _state) = crate::services::testing::publishing_test_db()?;
        let user = users::create(&mut db, &test_user("alice")?)?;
        let other_user = users::create(&mut db, &test_user("bob")?)?;
        let principal = principal(&db, user, HashSet::new());

        for target in [other_user, DbId(999_999_999)] {
            assert_eq!(
                set_for_principal(&mut db, &principal, target, RatingValue::new(4).unwrap())?,
                MutationOutcome::NotTargetable,
            );
            assert_eq!(
                remove_for_principal(&mut db, &principal, target)?,
                MutationOutcome::NotTargetable
            );
        }
        Ok(())
    }
}
