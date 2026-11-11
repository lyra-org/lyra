// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    DbId,
};

use super::{
    CoverFilter,
    ResolvedCover,
};
use lyra_db::covers::display::{
    self as db_display,
    DisplayCoverScope,
    DisplayCoverTargetKind,
    DisplayCoverWinnerKind,
};

/// A playlist's cover: the one its owner uploaded, else its generated display cover.
pub(crate) fn cover_for_playlist(
    db: &DbAny,
    filter: &CoverFilter<'_>,
    playlist: &lyra_db::Playlist,
) -> anyhow::Result<Option<ResolvedCover>> {
    let Some(playlist_db_id) = playlist.db_id.clone().map(DbId::from) else {
        return Ok(None);
    };
    if let Some(cover) = filter.owned_cover(db, playlist_db_id)? {
        return Ok(Some(cover));
    }
    generated_cover_for_playlist(db, filter, playlist)
}

/// A playlist's generated display cover: the deterministically chosen release cover from
/// its membership. Unlike genres there is no listen-driven personal or instance
/// winner — a playlist's identity is its curation, not aggregate listening.
/// When the filter rejects the stored winner, the next source it lets through stands in.
pub(crate) fn generated_cover_for_playlist(
    db: &DbAny,
    filter: &CoverFilter<'_>,
    playlist: &lyra_db::Playlist,
) -> anyhow::Result<Option<ResolvedCover>> {
    let Some(playlist_db_id) = playlist.db_id.clone().map(DbId::from) else {
        return Ok(None);
    };
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
    if let Some(cover) = filter.release_cover(db, winner.release_db_id)? {
        return Ok(Some(cover));
    }
    filter.first_release_cover(
        db,
        db_display::playlist_cover_sources(db, playlist_db_id)?
            .into_iter()
            .map(|(_, release_db_id)| release_db_id)
            .filter(|release_db_id| *release_db_id != winner.release_db_id),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::services::{
        auth::Principal,
        catalog::Viewer,
    };
    use lyra_db::fixtures::{
        connect,
        insert_release,
        insert_track,
        test_user,
    };
    use lyra_db::test_db::{
        insert_library,
        new_test_db,
    };

    fn covered_track(db: &mut DbAny, title: &str) -> anyhow::Result<(DbId, DbId)> {
        let release = insert_release(db, title)?;
        let track = insert_track(db, title)?;
        lyra_db::fixtures::link_track(db, release, track)?;
        db.transaction_mut(|t| {
            lyra_db::covers::upsert(
                t,
                release,
                lyra_db::covers::CoverSource::Local,
                lyra_db::Cover {
                    db_id: None,
                    id: title.to_string(),
                    path: crate::services::testing::existing_cover_path(),
                    mime_type: "image/jpeg".to_string(),
                    hash: "a".repeat(64),
                    blurhash: None,
                    provider_id: None,
                },
            )
        })?;
        Ok((release, track))
    }

    /// A private playlist of `owner` holding `tracks`, with its display cover synced.
    fn playlist(db: &mut DbAny, owner: DbId, tracks: &[DbId]) -> anyhow::Result<lyra_db::Playlist> {
        let playlist_db_id = lyra_db::playlists::create(
            db,
            &lyra_db::Playlist {
                db_id: None,
                id: lyra_db::nanoid(),
                name: "Mix".to_string(),
                description: None,
                is_public: Some(false),
                created_at_ms: None,
                updated_at_ms: None,
            },
            owner,
        )?;
        for track in tracks {
            db.transaction_mut(|t| lyra_db::playlists::add_track(t, playlist_db_id, *track))?;
        }
        db_display::sync_playlist_cover(db, playlist_db_id)?;
        Ok(lyra_db::playlists::get_by_id(db, playlist_db_id)?.expect("playlist exists"))
    }

    #[test]
    fn the_system_sees_a_private_playlists_cover() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let owner = lyra_db::users::create(&mut db, &test_user("owner")?)?;
        let (release, track) = covered_track(&mut db, "only")?;
        let playlist = playlist(&mut db, owner, &[track])?;

        let cover = cover_for_playlist(&db, &CoverFilter::new(&Viewer::System), &playlist)?;
        assert_eq!(cover.map(|cover| cover.owner_db_id), Some(release));
        Ok(())
    }

    #[test]
    fn a_hidden_winner_falls_back_to_the_next_visible_source() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let owner = lyra_db::users::create(&mut db, &test_user("owner")?)?;
        let mut tracks = Vec::new();
        for title in ["first", "second", "third"] {
            tracks.push(covered_track(&mut db, title)?);
        }
        let playlist = playlist(
            &mut db,
            owner,
            &tracks.iter().map(|(_, track)| *track).collect::<Vec<_>>(),
        )?;
        let playlist_db_id = playlist.db_id.clone().map(DbId::from).expect("playlist id");
        let sources = db_display::playlist_cover_sources(&db, playlist_db_id)?
            .into_iter()
            .map(|(_, release)| release)
            .collect::<Vec<_>>();
        let shown = insert_library(&mut db, "Shown", "/shown")?;
        let hidden = insert_library(&mut db, "Hidden", "/hidden")?;
        for (release, _) in &tracks {
            let library = if *release == sources[0] {
                hidden
            } else {
                shown
            };
            connect(&mut db, library, *release)?;
        }
        let shown_public_id = lyra_db::libraries::get_by_id(&db, shown)?
            .expect("library exists")
            .id;
        let principal =
            Principal::for_user(&db, owner, Vec::new(), HashSet::from([shown_public_id]));
        let viewer = Viewer::user(&db, principal)?;

        let cover = cover_for_playlist(&db, &CoverFilter::new(&viewer), &playlist)?;
        assert_eq!(cover.map(|cover| cover.owner_db_id), Some(sources[1]));
        Ok(())
    }
}
