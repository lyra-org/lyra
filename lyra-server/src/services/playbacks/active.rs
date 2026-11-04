// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! What users are playing now, whoever reports it.

use std::collections::HashMap;

use agdb::{
    DbAny,
    DbId,
};
use lyra_db::{
    PlaybackSession,
    playbacks::PlaybackSource,
};

use super::PlaybackError;
use crate::services::auth::{
    Principal,
    access,
};

/// A playback whose current track session is live.
#[derive(Clone, Debug)]
pub(crate) struct ActivePlayback {
    pub(crate) playback_id: String,
    pub(crate) user_id: String,
    pub(crate) username: String,
    pub(crate) track_id: String,
    pub(crate) session: PlaybackSession,
    /// The plugin's session key, for a playback a plugin reports.
    pub(crate) reported_session_key: Option<String>,
}

/// The active playbacks of `owners`, most recently reported first, leaving out those whose
/// current track the viewer can't see. A reported playback with a live current play hasn't
/// lapsed.
/// With `all_users`, `owners` covers every user the viewer may read for, so every user's playback
/// is read in one pass rather than owner by owner.
pub(crate) fn list_active(
    db: &DbAny,
    viewer: &Principal,
    owners: &[lyra_db::User],
    all_users: bool,
    now_ms: u64,
) -> Result<Vec<ActivePlayback>, PlaybackError> {
    let owners = owners
        .iter()
        .filter_map(|user| Some((user.db_id?, user)))
        .collect::<HashMap<DbId, _>>();
    let owner_db_ids = owners.keys().copied().collect::<Vec<_>>();
    let current =
        lyra_db::playbacks::list_live(db, (!all_users).then_some(owner_db_ids.as_slice()), now_ms)?
            .into_iter()
            .filter(|current| owners.contains_key(&current.owner_db_id))
            .collect::<Vec<_>>();
    let track_db_ids = current
        .iter()
        .map(|current| current.track_db_id)
        .collect::<Vec<_>>();
    let visible = access::accessible_entities(db, viewer, &track_db_ids)?;

    let mut active = Vec::with_capacity(current.len());
    for current in current {
        if !visible.contains(&current.track_db_id) {
            continue;
        }
        let Some(track_id) = lyra_db::lookup::find_id_by_db_id(db, current.track_db_id)? else {
            continue;
        };
        let owner = owners[&current.owner_db_id];
        active.push(ActivePlayback {
            reported_session_key: match &current.playback.source {
                PlaybackSource::Reported(source) => Some(source.session_key().to_string()),
                PlaybackSource::Queue(_) => None,
            },
            playback_id: current.playback.id,
            user_id: owner.id.clone(),
            username: owner.username.clone(),
            track_id,
            session: current.session,
        });
    }
    active.sort_by(|left, right| {
        right
            .session
            .updated_at_ms
            .cmp(&left.session.updated_at_ms)
            .then_with(|| left.playback_id.cmp(&right.playback_id))
    });
    Ok(active)
}
