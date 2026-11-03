// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Deleting playbacks: one at a time, as a client asks, and the inactive queue-backed ones a user
//! keeps past the retention target.

use agdb::{
    DbAny,
    DbId,
};

use super::{
    PlaybackError,
    bindings,
};
use crate::services::{
    playback_sessions::ACTIVE_SESSION_TTL_MS,
    remote::handoffs,
};

/// The inactive queue-backed playbacks each user keeps; the maintenance sweep deletes the oldest
/// beyond them.
pub(crate) const RETAINED_QUEUE_PLAYBACKS_PER_USER: usize = 100;
/// The most playbacks one sweep deletes, since deleting a hub's old neighbours grows quadratically.
const RETENTION_BATCH: usize = 100;

/// A deleted playback, whose pending handoffs and connection binding [`Self::finish`] settles once
/// its transaction commits, while the caller still holds the write guard it deleted under.
#[derive(Debug)]
pub(crate) struct DeletedPlayback {
    pub(crate) playback_id: String,
    current_session: Option<(DbId, String)>,
}

impl DeletedPlayback {
    pub(crate) async fn finish(self) {
        handoffs::fail_for_playback(&self.playback_id).await;
        if let Some((playback_session_id, playback_session_public_id)) = self.current_session {
            bindings::clear_session(playback_session_id, &playback_session_public_id);
        }
    }
}

/// Deletes the playback. Its sessions stay until they go stale, and listens recorded from its
/// plays stay.
pub(crate) fn delete_in_transaction(
    db: &mut impl lyra_db::DbAccess,
    playback_db_id: DbId,
) -> Result<Option<DeletedPlayback>, PlaybackError> {
    let Some(playback) = lyra_db::playbacks::get_by_id(db, playback_db_id)? else {
        return Ok(None);
    };
    let current_session = match lyra_db::playbacks::get_current_session_id(db, playback_db_id)? {
        Some(session_id) => lyra_db::playback_sessions::get_by_id(db, session_id)?
            .map(|session| (session_id, session)),
        None => None,
    };
    lyra_db::playbacks::delete_in_transaction(db, playback_db_id)?;
    Ok(Some(DeletedPlayback {
        playback_id: playback.id,
        current_session: current_session.map(|(session_id, session)| (session_id, session.id)),
    }))
}

pub(crate) fn delete(
    db: &mut DbAny,
    playback_db_id: DbId,
) -> Result<Option<DeletedPlayback>, PlaybackError> {
    db.transaction_mut(|t| delete_in_transaction(t, playback_db_id))
}

/// Deletes the oldest inactive queue-backed playbacks of each user beyond
/// [`RETAINED_QUEUE_PLAYBACKS_PER_USER`], at most a bounded batch at a time, so a user far past
/// the target drains over several sweeps. A playback is inactive while its current session isn't
/// a non-terminal one reported within the active window and no connection drives it.
pub(crate) fn sweep_retained(
    db: &mut DbAny,
    now_ms: u64,
) -> Result<Vec<DeletedPlayback>, PlaybackError> {
    let mut doomed = Vec::new();
    for user in lyra_db::users::get(db)? {
        if doomed.len() >= RETENTION_BATCH {
            break;
        }
        let Some(user_db_id) = user.db_id else {
            continue;
        };
        if lyra_db::playbacks::count_queue_for_user_up_to(
            db,
            user_db_id,
            RETAINED_QUEUE_PLAYBACKS_PER_USER + 1,
        )? <= RETAINED_QUEUE_PLAYBACKS_PER_USER
        {
            continue;
        }
        let mut inactive = inactive_queue_playbacks(db, user_db_id, now_ms)?;
        if inactive.len() <= RETAINED_QUEUE_PLAYBACKS_PER_USER {
            continue;
        }
        inactive.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.0.cmp(&right.0.0)));
        let room = RETENTION_BATCH - doomed.len();
        doomed.extend(
            inactive
                .into_iter()
                .skip(RETAINED_QUEUE_PLAYBACKS_PER_USER)
                .rev()
                .take(room)
                .map(|(playback_db_id, _)| playback_db_id),
        );
    }
    if doomed.is_empty() {
        return Ok(Vec::new());
    }
    db.transaction_mut(|t| {
        let mut deleted = Vec::with_capacity(doomed.len());
        for playback_db_id in doomed {
            deleted.extend(delete_in_transaction(t, playback_db_id)?);
        }
        Ok(deleted)
    })
}

/// The user's inactive queue-backed playbacks, with when each was last updated.
fn inactive_queue_playbacks(
    db: &DbAny,
    user_db_id: DbId,
    now_ms: u64,
) -> Result<Vec<(DbId, u64)>, PlaybackError> {
    let projections = lyra_db::playbacks::list_projections_for_user(db, user_db_id)?
        .into_iter()
        .filter(|projection| projection.queue_revision.is_some())
        .collect::<Vec<_>>();
    let playback_db_ids = projections
        .iter()
        .map(|projection| projection.db_id)
        .collect::<Vec<_>>();
    let current_ids = lyra_db::playbacks::current_session_ids(db, &playback_db_ids)?;
    let sessions = lyra_db::playback_sessions::get_list_projections_by_ids(
        db,
        current_ids.values().copied().collect(),
    )?;
    let active_cutoff_ms = now_ms.saturating_sub(ACTIVE_SESSION_TTL_MS);
    let driven = bindings::driven_playback_ids();
    Ok(projections
        .into_iter()
        .filter(|projection| {
            let live = current_ids
                .get(&projection.db_id)
                .and_then(|session_id| sessions.get(session_id))
                .is_some_and(|session| {
                    !session.state.is_terminal() && session.updated_at_ms >= active_cutoff_ms
                });
            !live && !driven.contains(&projection.id)
        })
        .map(|projection| (projection.db_id, projection.updated_at_ms))
        .collect())
}

#[cfg(test)]
mod tests {
    use lyra_db::{
        PlaybackState,
        fixtures::{
            insert_track,
            test_user,
        },
        test_db::new_test_db,
    };

    use super::*;
    use crate::services::playbacks::tests::store_queue_playback;

    #[test]
    fn the_sweep_deletes_the_oldest_inactive_queue_playbacks_in_batches() -> anyhow::Result<()> {
        let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
        crate::services::testing::init_default_test_state()?;
        let mut db = new_test_db()?;
        let user_db_id = lyra_db::users::create(&mut db, &test_user("keeper")?)?;
        let other_db_id = lyra_db::users::create(&mut db, &test_user("other")?)?;
        let track_db_id = insert_track(&mut db, "Track")?;
        let track_id = lyra_db::lookup::find_id_by_db_id(&db, track_db_id)?.unwrap();
        let now_ms = 10 * ACTIVE_SESSION_TTL_MS;
        let mut store = |user_db_id, state, updated_at_ms| -> anyhow::Result<DbId> {
            let playback =
                store_queue_playback(&mut db, user_db_id, &[&track_id], 0, track_db_id, state, 0)?;
            let playback_db_id = playback.db_id.unwrap();
            let session_db_id =
                lyra_db::playbacks::get_current_session_id(&db, playback_db_id)?.unwrap();
            let mut session = lyra_db::playback_sessions::get_by_id(&db, session_db_id)?.unwrap();
            session.updated_at_ms = updated_at_ms;
            db.transaction_mut(|t| {
                lyra_db::playback_sessions::update(t, &session)?;
                lyra_db::playbacks::touch(t, playback_db_id, updated_at_ms)
            })?;
            Ok(playback_db_id)
        };

        let extra = RETENTION_BATCH + 5;
        let mut inactive = Vec::new();
        for index in 0..RETAINED_QUEUE_PLAYBACKS_PER_USER + extra {
            inactive.push(store(user_db_id, PlaybackState::Paused, 1 + index as u64)?);
        }
        // A play still reported is active, however old its playback.
        let live = store(user_db_id, PlaybackState::Playing, now_ms)?;
        let mut others = Vec::new();
        for index in 0..RETAINED_QUEUE_PLAYBACKS_PER_USER + 1 {
            others.push(store(
                other_db_id,
                PlaybackState::Stopped,
                1 + index as u64,
            )?);
        }
        let exists = |db: &DbAny, playback_db_id| {
            lyra_db::playbacks::get_by_id(db, playback_db_id).map(|found| found.is_some())
        };

        let first = sweep_retained(&mut db, now_ms)?;
        assert_eq!(first.len(), RETENTION_BATCH, "one sweep deletes one batch");
        assert!(!exists(&db, inactive[0])?, "the oldest go first");
        assert!(exists(&db, inactive[extra])?);

        let second = sweep_retained(&mut db, now_ms)?;
        assert_eq!(
            second.len(),
            extra + 1 - RETENTION_BATCH,
            "the rest, of both users"
        );
        for (index, playback_db_id) in inactive.iter().enumerate() {
            assert_eq!(
                exists(&db, *playback_db_id)?,
                index >= extra,
                "playback {index}"
            );
        }
        assert!(exists(&db, live)?);
        assert!(!exists(&db, others[0])?);
        assert!(exists(&db, others[1])?);
        assert!(sweep_retained(&mut db, now_ms)?.is_empty());
        Ok(())
    }
}
