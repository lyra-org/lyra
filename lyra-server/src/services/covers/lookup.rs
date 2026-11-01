// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The covers items show, for callers that serve the image file itself. A stored cover whose
//! file is missing is passed over for the next one the owner has, and its record kept.

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

/// Whether the cover's image file can be served.
pub(crate) fn cover_file_available(cover: &Cover) -> bool {
    std::fs::metadata(&cover.path).is_ok_and(|meta| meta.is_file())
}

/// The cover each owner shows: its pinned source's, else the first by precedence, passing over
/// those whose file is missing. Owners without one are left out.
pub(crate) fn shown_covers(
    db: &impl lyra_db::DbAccess,
    owner_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Cover>> {
    Ok(lyra_db::covers::sources_many(db, owner_db_ids)?
        .into_iter()
        .filter_map(|(owner_db_id, sources)| {
            lyra_db::covers::shown(&sources, cover_file_available)
                .map(|sourced| (owner_db_id, sourced.cover.clone()))
        })
        .collect())
}

/// [`shown_covers`] for one owner.
pub(crate) fn shown_cover(
    db: &impl lyra_db::DbAccess,
    owner_db_id: DbId,
) -> anyhow::Result<Option<Cover>> {
    Ok(shown_covers(db, &[owner_db_id])?.remove(&owner_db_id))
}

pub(crate) struct CoverLookup {
    pub(crate) covers: HashMap<DbId, ResolvedCover>,
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
    let mut owned_ids = Vec::new();
    let mut track_ids = HashSet::new();
    let mut seen = HashSet::new();
    let mut filter = CoverFilter::new(viewer);
    for item_id in item_ids.iter().copied() {
        if !seen.insert(item_id) {
            continue;
        }
        let display_cover = match lyra_db::graph::element_kind(db, item_id)?.as_deref() {
            Some(kind @ ("Release" | "Artist" | "Track")) => {
                if viewer.sees(db, item_id)? {
                    owned_ids.push(item_id);
                    if kind == "Track" {
                        track_ids.insert(item_id);
                    }
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
    let mut lookup = CoverLookup { covers };
    lookup.resolve_owned(db, &owned_ids, &track_ids)?;
    Ok(lookup)
}

impl CoverLookup {
    /// Own covers, with tracks lacking one falling back to their releases' in id order.
    fn resolve_owned(
        &mut self,
        db: &DbAny,
        item_ids: &[DbId],
        track_ids: &HashSet<DbId>,
    ) -> anyhow::Result<()> {
        let mut direct_covers = shown_covers(db, item_ids)?;

        let mut unresolved_tracks = Vec::new();
        for item_id in item_ids.iter().copied() {
            if let Some(cover) = direct_covers.remove(&item_id) {
                self.covers.insert(
                    item_id,
                    ResolvedCover {
                        owner_db_id: item_id,
                        cover,
                    },
                );
                continue;
            }
            if track_ids.contains(&item_id) {
                unresolved_tracks.push(item_id);
            }
        }
        if unresolved_tracks.is_empty() {
            return Ok(());
        }

        let track_releases = lyra_db::releases::get_by_tracks(db, &unresolved_tracks)?;
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
        let covers_by_release = shown_covers(db, &release_ids)?;

        for item_id in unresolved_tracks {
            let Some(releases) = track_releases.get(&item_id) else {
                continue;
            };
            let mut release_ids = releases
                .iter()
                .filter_map(|release| release.db_id.clone().map(DbId::from))
                .collect::<Vec<_>>();
            release_ids.sort_by_key(|release_id| release_id.0);
            if let Some((release_id, cover)) = release_ids.into_iter().find_map(|release_id| {
                covers_by_release
                    .get(&release_id)
                    .map(|cover| (release_id, cover.clone()))
            }) {
                self.covers.insert(
                    item_id,
                    ResolvedCover {
                        owner_db_id: release_id,
                        cover,
                    },
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lyra_db::{
        covers::display as db_display,
        fixtures::{
            connect,
            connect_credit,
            insert_artist,
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
                    lyra_db::covers::CoverSource::Local,
                    Cover {
                        db_id: None,
                        id: title.to_string(),
                        path: path.to_string_lossy().into_owned(),
                        mime_type: "image/png".to_string(),
                        hash: title.to_string(),
                        blurhash: None,
                        provider_id: None,
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
        assert!(
            lyra_db::covers::get(&db, winner)?.is_some(),
            "the record stays"
        );
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
        assert!(
            lyra_db::covers::get(&db, sources[0].1)?.is_some(),
            "the record stays"
        );
        Ok(())
    }

    #[test]
    fn an_artist_without_a_cover_does_not_take_its_release_cover() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let mut db = new_test_db()?;
        let (release, track) = covered_releases(&mut db, dir.path())?[0];
        let artist = insert_artist(&mut db, "Artist")?;
        connect_credit(
            &mut db,
            release,
            artist,
            lyra_db::CreditType::Artist,
            None,
            0,
        )?;

        let found = resolve(&db, &Viewer::System, &[artist, track], Listens::Viewer)?;
        assert!(!found.covers.contains_key(&artist));
        assert_eq!(
            found.covers.get(&track).map(|cover| cover.owner_db_id),
            Some(release)
        );
        Ok(())
    }
}
