// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Covers by item id for callers that read the image file itself, so a cover whose file is gone
//! is left out and reported for removal.

use std::collections::{
    HashMap,
    HashSet,
};

use agdb::{
    DbAny,
    DbId,
};

use super::{
    ResolvedCover,
    display::{
        self,
        CoverFilter,
        Listens,
    },
};
use crate::services::catalog::Viewer;
use lyra_db::Cover;

pub(crate) struct CoverLookup {
    pub(crate) covers: HashMap<DbId, ResolvedCover>,
    /// Owners whose cover file is gone, for [`remove_stale`] once the read is over.
    pub(crate) stale_owners: Vec<DbId>,
}

enum CoverFile {
    Present,
    Gone,
    Unreadable,
}

fn cover_file(cover: &Cover) -> CoverFile {
    match std::fs::metadata(&cover.path) {
        Ok(meta) if meta.is_file() => CoverFile::Present,
        Ok(_) => CoverFile::Gone,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => CoverFile::Gone,
        Err(_) => CoverFile::Unreadable,
    }
}

/// Whether `cover`'s file is there, noting `owner_db_id` as stale when it is gone.
fn cover_file_present(stale_owners: &mut Vec<DbId>, owner_db_id: DbId, cover: &Cover) -> bool {
    match cover_file(cover) {
        CoverFile::Present => true,
        CoverFile::Gone => {
            stale_owners.push(owner_db_id);
            false
        }
        CoverFile::Unreadable => false,
    }
}

/// The covers of the items the viewer can see, keyed by item id. Releases and artists have their
/// own cover, a track falls back to its release's, and playlists and genres get their display
/// cover, picked from `listens` for genres. A release whose cover file is gone is passed over.
pub(crate) fn resolve(
    db: &DbAny,
    viewer: &Viewer,
    item_ids: &[DbId],
    listens: Listens,
) -> anyhow::Result<CoverLookup> {
    let mut covers = HashMap::new();
    let mut stale_owners = Vec::new();
    let mut owned_ids = Vec::new();
    let mut seen = HashSet::new();
    {
        let mut check =
            |owner_db_id, cover: &Cover| cover_file_present(&mut stale_owners, owner_db_id, cover);
        let mut filter = CoverFilter::new(viewer).checked(&mut check);
        for item_id in item_ids.iter().copied() {
            if !seen.insert(item_id) {
                continue;
            }
            let display_cover = match lyra_db::entities::get_element_type(db, item_id)?.as_deref() {
                Some("Release" | "Artist" | "Track") => {
                    if viewer.sees(db, item_id)? {
                        owned_ids.push(item_id);
                    }
                    continue;
                }
                Some("Playlist") => {
                    if !viewer.sees_playlist(db, item_id)? {
                        continue;
                    }
                    let Some(playlist) = lyra_db::playlists::get_by_id(db, item_id)? else {
                        continue;
                    };
                    display::playlists::cover_for_playlist(db, &mut filter, &playlist)?
                }
                Some("Genre") => {
                    let Some(genre) = lyra_db::genres::get_by_id(db, item_id)? else {
                        continue;
                    };
                    display::genres::cover_for_genre(db, &mut filter, &genre, listens)?
                }
                _ => continue,
            };
            if let Some(display_cover) = display_cover {
                covers.insert(item_id, display_cover);
            }
        }
    }
    let mut lookup = CoverLookup {
        covers,
        stale_owners,
    };
    lookup.resolve_owned(db, &owned_ids)?;
    Ok(lookup)
}

impl CoverLookup {
    fn keep_valid(&mut self, item_id: DbId, resolved: ResolvedCover) -> bool {
        if !cover_file_present(
            &mut self.stale_owners,
            resolved.owner_db_id,
            &resolved.cover,
        ) {
            return false;
        }
        self.covers.insert(item_id, resolved);
        true
    }

    /// Own covers, with tracks lacking one falling back to their releases' in id order.
    fn resolve_owned(&mut self, db: &DbAny, item_ids: &[DbId]) -> anyhow::Result<()> {
        let direct_covers = lyra_db::covers::get_many(db, item_ids)?;

        let mut unresolved = Vec::new();
        for item_id in item_ids.iter().copied() {
            if let Some(cover) = direct_covers.get(&item_id)
                && self.keep_valid(
                    item_id,
                    ResolvedCover {
                        owner_db_id: item_id,
                        cover: cover.clone(),
                    },
                )
            {
                continue;
            }
            unresolved.push(item_id);
        }
        if unresolved.is_empty() {
            return Ok(());
        }

        let track_releases = lyra_db::releases::get_by_tracks(db, &unresolved)?;
        let mut release_ids = Vec::new();
        let mut seen_release_ids = HashSet::new();
        for related_releases in track_releases.values() {
            for release in related_releases {
                let Some(release_id) = release.db_id.clone().map(DbId::from) else {
                    continue;
                };
                if seen_release_ids.insert(release_id) {
                    release_ids.push(release_id);
                }
            }
        }
        let covers_by_release = lyra_db::covers::get_many(db, &release_ids)?;

        for item_id in unresolved {
            let Some(releases) = track_releases.get(&item_id) else {
                continue;
            };
            let mut release_ids = releases
                .iter()
                .filter_map(|release| release.db_id.clone().map(DbId::from))
                .collect::<Vec<_>>();
            release_ids.sort_by_key(|release_id| release_id.0);
            for release_id in release_ids {
                let Some(cover) = covers_by_release.get(&release_id) else {
                    continue;
                };
                if self.keep_valid(
                    item_id,
                    ResolvedCover {
                        owner_db_id: release_id,
                        cover: cover.clone(),
                    },
                ) {
                    break;
                }
            }
        }
        Ok(())
    }
}

/// Removes the covers of `owner_ids` whose files are still gone.
pub(crate) fn remove_stale(db: &mut DbAny, owner_ids: &[DbId]) -> anyhow::Result<()> {
    let mut seen = HashSet::new();
    for owner_id in owner_ids.iter().copied() {
        if !seen.insert(owner_id) {
            continue;
        }
        let Some(cover) = lyra_db::covers::get(db, owner_id)? else {
            continue;
        };
        if matches!(cover_file(&cover), CoverFile::Gone) {
            lyra_db::covers::remove(db, owner_id)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lyra_db::{
        covers::display as db_display,
        fixtures::{
            connect,
            insert_release,
            insert_track,
            test_user,
        },
        test_db::new_test_db,
    };

    /// Three releases in Rock, each with a track and a cover file in `dir`.
    fn covered_releases(
        db: &mut DbAny,
        dir: &std::path::Path,
    ) -> anyhow::Result<Vec<(DbId, DbId)>> {
        let mut releases = Vec::new();
        for title in ["first", "second", "third"] {
            let release = insert_release(db, title)?;
            let track = insert_track(db, title)?;
            connect(db, release, track)?;
            lyra_db::genres::sync_release_genres(db, release, &["Rock".to_string()])?;
            let path = dir.join(format!("{title}.png"));
            std::fs::write(&path, b"cover")?;
            db.transaction_mut(|t| {
                lyra_db::covers::upsert(
                    t,
                    release,
                    Cover {
                        db_id: None,
                        id: title.to_string(),
                        path: path.to_string_lossy().into_owned(),
                        mime_type: "image/png".to_string(),
                        hash: title.to_string(),
                        blurhash: None,
                    },
                )
            })?;
            releases.push((release, track));
        }
        Ok(releases)
    }

    fn remove_cover_file(db: &DbAny, release: DbId) -> anyhow::Result<()> {
        let cover = lyra_db::covers::get(db, release)?.expect("cover exists");
        std::fs::remove_file(cover.path)?;
        Ok(())
    }

    #[tokio::test]
    async fn a_genre_whose_winner_file_is_gone_shows_the_next_candidate() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let dir = tempfile::tempdir()?;
        let mut db = new_test_db()?;
        covered_releases(&mut db, dir.path())?;
        let genre_db_id = lyra_db::genres::resolve_by_name(&mut db, "Rock")?;
        let genre = lyra_db::genres::get_by_id(&db, genre_db_id)?.expect("genre exists");
        let (profile_db_id, profile) = db_display::get_profile(
            &db,
            db_display::DisplayCoverScope::Instance,
            db_display::DisplayCoverTargetKind::Genre,
            None,
            &genre.id,
        )?
        .expect("instance profile");
        let candidates = db_display::candidates_by_random_score(&db, profile_db_id)?;
        let winner =
            db_display::get_winner(&db, &profile, db_display::DisplayCoverWinnerKind::Random)?
                .expect("random winner")
                .release_db_id;
        assert_eq!(candidates[0], winner);
        remove_cover_file(&db, winner)?;

        let found = resolve(&db, &Viewer::System, &[genre_db_id], Listens::Instance)?;
        assert_eq!(
            found
                .covers
                .get(&genre_db_id)
                .map(|cover| cover.owner_db_id),
            Some(candidates[1])
        );
        assert_eq!(found.stale_owners, vec![winner]);
        Ok(())
    }

    #[test]
    fn a_playlist_whose_winner_file_is_gone_shows_the_next_source() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let mut db = new_test_db()?;
        let releases = covered_releases(&mut db, dir.path())?;
        let owner = lyra_db::users::create(&mut db, &test_user("owner")?)?;
        let playlist_db_id = lyra_db::playlists::create(
            &mut db,
            &lyra_db::Playlist {
                db_id: None,
                id: nanoid::nanoid!(),
                name: "Mix".to_string(),
                description: None,
                is_public: Some(false),
                created_at_ms: None,
                updated_at_ms: None,
            },
            owner,
        )?;
        for (_, track) in &releases {
            db.transaction_mut(|t| lyra_db::playlists::add_track(t, playlist_db_id, *track))?;
        }
        db_display::sync_playlist_cover(&mut db, playlist_db_id)?;
        let sources = db_display::playlist_cover_sources(&db, playlist_db_id)?;
        remove_cover_file(&db, sources[0].1)?;

        let found = resolve(&db, &Viewer::System, &[playlist_db_id], Listens::Viewer)?;
        assert_eq!(
            found
                .covers
                .get(&playlist_db_id)
                .map(|cover| cover.owner_db_id),
            Some(sources[1].1)
        );
        assert_eq!(found.stale_owners, vec![sources[0].1]);
        Ok(())
    }
}
