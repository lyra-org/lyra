// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The cover each item shows, for REST responses and plugins alike. Covers are read from the
//! stored records alone, never from the files; whoever serves the image checks its file.

use std::collections::HashMap;

use agdb::{
    DbAny,
    DbId,
};

use super::{
    ResolvedCover,
    display::{
        self,
        CoverFilter,
    },
};
use lyra_db::{
    Playlist,
    genres::Genre,
};

/// An item whose cover to resolve. The caller vouches that the viewer sees the item itself.
#[derive(Clone, Copy)]
pub(crate) enum CoverTarget<'a> {
    Release(DbId),
    Artist(DbId),
    Track(DbId),
    Genre(&'a Genre),
    Playlist(&'a Playlist),
}

impl CoverTarget<'_> {
    /// Whether the target's cover may come from releases the filter judges; releases and artists
    /// only show their own.
    pub(crate) fn derived(&self) -> bool {
        !matches!(self, Self::Release(_) | Self::Artist(_))
    }
}

/// The covers `targets` show, keyed by target id. An owner's own cover is its pinned source's
/// record, else the first by precedence (manual, provider, local). Releases and artists show their
/// own cover; a track its own, else that of the first of its releases, by public id, that `filter`
/// lets through; a playlist its own, else its display cover; a genre its display cover. Display
/// covers are picked among the releases `filter` lets through. Targets without a cover are absent.
pub(crate) fn resolve(
    db: &DbAny,
    filter: &CoverFilter<'_>,
    targets: &[CoverTarget<'_>],
) -> anyhow::Result<HashMap<DbId, ResolvedCover>> {
    let mut covers = HashMap::with_capacity(targets.len());
    let mut uncovered_tracks = Vec::new();
    for target in targets {
        match *target {
            CoverTarget::Release(db_id) | CoverTarget::Artist(db_id) => {
                own_cover(db, &mut covers, db_id)?;
            }
            CoverTarget::Track(db_id) => {
                if !own_cover(db, &mut covers, db_id)? {
                    uncovered_tracks.push(db_id);
                }
            }
            CoverTarget::Genre(genre) => {
                if let Some(db_id) = genre.db_id.clone().map(DbId::from)
                    && let Some(cover) = display::genres::cover_for_genre(db, filter, genre)?
                {
                    covers.insert(db_id, cover);
                }
            }
            CoverTarget::Playlist(playlist) => {
                if let Some(db_id) = playlist.db_id.clone().map(DbId::from)
                    && let Some(cover) =
                        display::playlists::cover_for_playlist(db, filter, playlist)?
                {
                    covers.insert(db_id, cover);
                }
            }
        }
    }
    if uncovered_tracks.is_empty() {
        return Ok(covers);
    }

    for (track_db_id, mut releases) in lyra_db::releases::get_by_tracks(db, &uncovered_tracks)? {
        releases.sort_by(|a, b| a.id.cmp(&b.id));
        let release_db_ids = releases
            .into_iter()
            .filter_map(|release| release.db_id.map(DbId::from));
        if let Some(cover) = filter.first_release_cover(db, release_db_ids)? {
            covers.insert(track_db_id, cover);
        }
    }
    Ok(covers)
}

/// Whether `db_id` has a cover of its own, which then goes into `covers`.
fn own_cover(
    db: &DbAny,
    covers: &mut HashMap<DbId, ResolvedCover>,
    db_id: DbId,
) -> anyhow::Result<bool> {
    if covers.contains_key(&db_id) {
        return Ok(true);
    }
    let Some(cover) = lyra_db::covers::get(db, db_id)? else {
        return Ok(false);
    };
    covers.insert(
        db_id,
        ResolvedCover {
            owner_db_id: db_id,
            cover,
        },
    );
    Ok(true)
}
