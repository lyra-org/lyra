// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Covers by item id, for callers holding only ids.

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
    display::CoverFilter,
    shown::{
        self,
        CoverTarget,
    },
};

/// The covers of the items of `item_ids` the filter's viewer can see, keyed by item id, as
/// [`shown::resolve`] picks them. Items of other types are absent.
pub(crate) fn resolve(
    db: &DbAny,
    filter: &CoverFilter<'_>,
    item_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, ResolvedCover>> {
    let viewer = filter.viewer();
    let mut owned = Vec::new();
    let mut genres = Vec::new();
    let mut playlists = Vec::new();
    let mut seen = HashSet::new();
    for item_id in item_ids.iter().copied() {
        if !seen.insert(item_id) {
            continue;
        }
        match lyra_db::graph::element_kind(db, item_id)?.as_deref() {
            Some("Release") if viewer.sees_entity(db, item_id)? => {
                owned.push(CoverTarget::Release(item_id));
            }
            Some("Track") if viewer.sees_entity(db, item_id)? => {
                owned.push(CoverTarget::Track(item_id));
            }
            Some("Artist") if viewer.sees_artist(db, item_id)? => {
                owned.push(CoverTarget::Artist(item_id));
            }
            Some("Playlist") if viewer.sees_playlist(db, item_id)? => {
                playlists.extend(lyra_db::playlists::get_by_id(db, item_id)?);
            }
            Some("Genre") => genres.extend(lyra_db::genres::get_by_id(db, item_id)?),
            _ => {}
        }
    }
    let mut targets = owned;
    targets.extend(genres.iter().map(CoverTarget::Genre));
    targets.extend(playlists.iter().map(CoverTarget::Playlist));
    shown::resolve(db, filter, &targets)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::services::{
        auth::Principal,
        catalog::Viewer,
        covers::display::Listens,
    };
    use lyra_db::{
        Cover,
        covers::{
            CoverSource,
            display as db_display,
        },
        fixtures::{
            connect,
            connect_credit,
            insert_artist,
            insert_release,
            insert_track,
            test_user,
        },
        test_db::{
            insert_library,
            new_test_db,
        },
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
                    CoverSource::Local,
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

    fn owner(covers: &HashMap<DbId, ResolvedCover>, item: DbId) -> Option<DbId> {
        covers.get(&item).map(|cover| cover.owner_db_id)
    }

    fn genre_winner(db: &DbAny, genre: &lyra_db::genres::Genre) -> anyhow::Result<DbId> {
        let (_, profile) = db_display::get_profile(
            db,
            db_display::DisplayCoverScope::Instance,
            db_display::DisplayCoverTargetKind::Genre,
            None,
            &genre.id,
        )?
        .expect("instance profile");
        Ok(
            db_display::get_winner(db, &profile, db_display::DisplayCoverWinnerKind::Random)?
                .expect("random winner")
                .release_db_id,
        )
    }

    #[tokio::test]
    async fn a_genre_listing_reads_its_winner_without_looking_at_the_file() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let dir = tempfile::tempdir()?;
        let mut db = new_test_db()?;
        covered_releases(&mut db, dir.path())?;
        let genre_db_id = lyra_db::genres::resolve_by_name(&mut db, "Rock")?;
        let genre = lyra_db::genres::get_by_id(&db, genre_db_id)?.expect("genre exists");
        let winner = genre_winner(&db, &genre)?;
        remove_cover_file(&db, winner)?;
        let filter = CoverFilter::new(&Viewer::System).listening(Listens::Instance);

        let listed = resolve(&db, &filter, &[genre_db_id])?;
        assert_eq!(owner(&listed, genre_db_id), Some(winner));
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

        let found = resolve(&db, &CoverFilter::new(&Viewer::System), &[artist, track])?;
        assert!(!found.contains_key(&artist));
        assert_eq!(owner(&found, track), Some(release));
        Ok(())
    }

    #[test]
    fn a_track_shows_the_first_release_by_public_id_the_viewer_sees() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let mut db = new_test_db()?;
        let releases = covered_releases(&mut db, dir.path())?;
        let track = releases[0].1;
        for (release, _) in &releases[1..] {
            connect(&mut db, *release, track)?;
        }
        let mut by_public_id = releases
            .iter()
            .map(|(release, _)| {
                let public_id = lyra_db::releases::get_by_id(&db, *release)?
                    .expect("release exists")
                    .id;
                Ok((public_id, *release))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        by_public_id.sort();
        let (first, second) = (by_public_id[0].1, by_public_id[1].1);
        let shown = insert_library(&mut db, "Shown", "/shown")?;
        let hidden = insert_library(&mut db, "Hidden", "/hidden")?;
        connect(&mut db, hidden, first)?;
        for (_, release) in &by_public_id[1..] {
            connect(&mut db, shown, *release)?;
        }
        let user_db_id = lyra_db::users::create(&mut db, &test_user("alice")?)?;
        let shown_public_id = lyra_db::libraries::get_by_id(&db, shown)?
            .expect("library exists")
            .id;
        let principal = Principal::for_user(
            &db,
            user_db_id,
            Vec::new(),
            HashSet::from([shown_public_id]),
        );
        let viewer = Viewer::user(&db, principal)?;

        let system = resolve(&db, &CoverFilter::new(&Viewer::System), &[track])?;
        assert_eq!(owner(&system, track), Some(first));
        let restricted = resolve(&db, &CoverFilter::new(&viewer), &[track])?;
        assert_eq!(owner(&restricted, track), Some(second));
        Ok(())
    }
}
