// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use agdb::{
    DbAny,
    DbId,
};

use crate::{
    db::{
        self,
        covers::display::{
            self as db_display,
            DisplayCoverScope,
            DisplayCoverTargetKind,
            DisplayCoverWinnerKind,
        },
    },
    services::auth::Principal,
};

/// A playlist's display cover: the deterministically chosen release cover from
/// its membership. Unlike genres there is no listen-driven personal or instance
/// winner — a playlist's identity is its curation, not aggregate listening.
pub(crate) fn cover_for_playlist(
    db: &DbAny,
    principal: &Principal,
    playlist: &db::Playlist,
) -> anyhow::Result<Option<db::Cover>> {
    let Some((_, profile)) = db_display::get_profile(
        db,
        DisplayCoverScope::Instance,
        DisplayCoverTargetKind::Playlist,
        None,
        &playlist.id,
    )?
    else {
        return Ok(None);
    };
    if !super::profile_is_current(&profile) {
        return Ok(None);
    }
    let Some(winner) = db_display::get_winner(db, &profile, DisplayCoverWinnerKind::Random)? else {
        return Ok(None);
    };
    super::cover_for_accessible_release(db, principal, winner.release_db_id)
}

pub(crate) fn covers_for_playlists(
    db: &DbAny,
    principal: &Principal,
    playlists: &[db::Playlist],
) -> anyhow::Result<HashMap<DbId, db::Cover>> {
    let mut covers = HashMap::new();
    for playlist in playlists {
        let Some(playlist_db_id) = playlist.db_id.clone().map(DbId::from) else {
            continue;
        };
        let Some(cover) = cover_for_playlist(db, principal, playlist)? else {
            continue;
        };
        covers.insert(playlist_db_id, cover);
    }
    Ok(covers)
}
