// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Deleting playbacks.

use agdb::{
    DbAny,
    DbId,
};

use super::{
    PlaybackError,
    bindings,
};
use crate::services::remote::handoffs;

/// A deleted playback, whose pending handoffs and connection binding [`Self::finish`] settles once
/// its transaction commits.
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
