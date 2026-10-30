// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use agdb::{
    DbAny,
    DbId,
};

use super::{
    CoverFilter,
    Listens,
    ResolvedCover,
};
use crate::services::{
    STATE,
    catalog::Viewer,
};
use lyra_db::covers::display::{
    self as db_display,
    DisplayCoverScope,
    DisplayCoverTargetKind,
    DisplayCoverWinnerKind,
};

fn profile_for_genre(
    db: &DbAny,
    scope: DisplayCoverScope,
    user_public_id: Option<&str>,
    genre_public_id: &str,
) -> anyhow::Result<Option<(DbId, db_display::DisplayCoverProfile)>> {
    db_display::get_profile(
        db,
        scope,
        DisplayCoverTargetKind::Genre,
        user_public_id,
        genre_public_id,
    )
}

fn winner_cover(
    db: &DbAny,
    filter: &mut CoverFilter<'_>,
    profile: &db_display::DisplayCoverProfile,
    kind: DisplayCoverWinnerKind,
) -> anyhow::Result<Option<(db_display::DisplayCoverWinner, ResolvedCover)>> {
    let Some(winner) = db_display::get_winner(db, profile, kind)? else {
        return Ok(None);
    };
    let Some(cover) = filter.release_cover(db, winner.release_db_id)? else {
        return Ok(None);
    };
    Ok(Some((winner, cover)))
}

/// The user whose listens pick the cover, if any.
fn listening_user(viewer: &Viewer, listens: Listens) -> Option<&str> {
    match (viewer, listens) {
        (Viewer::User { principal, .. }, Listens::Viewer) => Some(&principal.user_public_id),
        _ => None,
    }
}

pub(crate) fn cover_for_genre(
    db: &DbAny,
    filter: &mut CoverFilter<'_>,
    genre: &lyra_db::genres::Genre,
    listens: Listens,
) -> anyhow::Result<Option<ResolvedCover>> {
    select_genre_cover(db, filter, genre, listens, STATE.config().instance_stats)
}

/// Without `instance_stats`, other users' listens neither pick the cover nor raise the bar the
/// user's own listens must clear; the instance profile only supplies the deterministic fallback.
fn select_genre_cover(
    db: &DbAny,
    filter: &mut CoverFilter<'_>,
    genre: &lyra_db::genres::Genre,
    listens: Listens,
    instance_stats: bool,
) -> anyhow::Result<Option<ResolvedCover>> {
    let instance_profile = profile_for_genre(db, DisplayCoverScope::Instance, None, &genre.id)?;
    let instance_signals = instance_profile
        .as_ref()
        .map(|(_, profile)| profile)
        .filter(|_| instance_stats);

    if let Some(user_public_id) = listening_user(filter.viewer(), listens)
        && let Some((_, user_profile)) =
            profile_for_genre(db, DisplayCoverScope::User, Some(user_public_id), &genre.id)?
        && super::profile_is_current(&user_profile)
        && let Some((personal_winner, cover)) =
            winner_cover(db, filter, &user_profile, DisplayCoverWinnerKind::Personal)?
        && super::user_signal_is_enough(&user_profile, &personal_winner, instance_signals)
    {
        return Ok(Some(cover));
    }

    let Some((instance_profile_db_id, instance_profile)) = instance_profile.as_ref() else {
        return Ok(None);
    };
    if !super::profile_is_current(instance_profile) {
        return Ok(None);
    }

    if instance_stats
        && let Some((_instance_winner, cover)) = winner_cover(
            db,
            filter,
            instance_profile,
            DisplayCoverWinnerKind::Instance,
        )?
        && super::instance_signal_is_enough(instance_profile)
    {
        return Ok(Some(cover));
    }

    let random_winner =
        db_display::get_winner(db, instance_profile, DisplayCoverWinnerKind::Random)?;
    let Some(random_winner) = random_winner else {
        return Ok(None);
    };
    if let Some(cover) = filter.release_cover(db, random_winner.release_db_id)? {
        return Ok(Some(cover));
    }
    filter.first_release_cover(
        db,
        db_display::candidates_by_random_score(db, *instance_profile_db_id)?
            .into_iter()
            .filter(|release_db_id| *release_db_id != random_winner.release_db_id),
    )
}

pub(crate) fn covers_for_genres(
    db: &DbAny,
    filter: &mut CoverFilter<'_>,
    genres: &[lyra_db::genres::Genre],
    listens: Listens,
) -> anyhow::Result<HashMap<DbId, ResolvedCover>> {
    let instance_stats = STATE.config().instance_stats;
    let mut covers = HashMap::new();
    for genre in genres {
        let Some(genre_db_id) = genre.db_id.clone().map(DbId::from) else {
            continue;
        };
        let Some(cover) = select_genre_cover(db, filter, genre, listens, instance_stats)? else {
            continue;
        };
        covers.insert(genre_db_id, cover);
    }
    Ok(covers)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::services::auth::Principal;
    use lyra_db::{
        Permission,
        fixtures::{
            connect,
            insert_listen,
            insert_release,
            insert_track,
            test_user,
        },
        test_db::{
            insert_library,
            new_test_db,
        },
    };

    fn covered_release(db: &mut DbAny, title: &str) -> anyhow::Result<(DbId, DbId)> {
        let release = insert_release(db, title)?;
        let track = insert_track(db, title)?;
        connect(db, release, track)?;
        lyra_db::genres::sync_release_genres(db, release, &["Rock".to_string()])?;
        db.transaction_mut(|t| {
            lyra_db::covers::upsert(
                t,
                release,
                lyra_db::Cover {
                    db_id: None,
                    id: title.to_string(),
                    path: format!("/music/{title}.jpg"),
                    mime_type: "image/jpeg".to_string(),
                    hash: "a".repeat(64),
                    blurhash: None,
                },
            )
        })?;
        Ok((release, track))
    }

    fn rock(db: &mut DbAny) -> anyhow::Result<lyra_db::genres::Genre> {
        let genre_db_id = lyra_db::genres::resolve_by_name(db, "Rock")?;
        Ok(lyra_db::genres::get_by_id(db, genre_db_id)?.expect("genre exists"))
    }

    fn random_winner(db: &DbAny, genre: &lyra_db::genres::Genre) -> anyhow::Result<DbId> {
        let (_, profile) = profile_for_genre(db, DisplayCoverScope::Instance, None, &genre.id)?
            .expect("instance profile");
        Ok(
            db_display::get_winner(db, &profile, DisplayCoverWinnerKind::Random)?
                .expect("random winner")
                .release_db_id,
        )
    }

    fn viewer(
        db: &DbAny,
        user_db_id: DbId,
        permissions: Vec<Permission>,
        libraries: &[DbId],
    ) -> anyhow::Result<Viewer> {
        let mut library_ids = HashSet::new();
        for library in libraries {
            library_ids.insert(
                lyra_db::libraries::get_by_id(db, *library)?
                    .expect("library exists")
                    .id,
            );
        }
        let principal = Principal::for_user(db, user_db_id, permissions, library_ids);
        Ok(Viewer::user(db, principal)?)
    }

    /// `releases` in the order the genre's random pick ranks them.
    fn by_score(
        db: &DbAny,
        genre: &lyra_db::genres::Genre,
        releases: impl IntoIterator<Item = DbId>,
    ) -> anyhow::Result<Vec<DbId>> {
        let mut scored = releases
            .into_iter()
            .map(|release| {
                let release_public_id = lyra_db::releases::get_by_id(db, release)?
                    .expect("release exists")
                    .id;
                let score = db_display::deterministic_random_score(
                    DisplayCoverTargetKind::Genre,
                    &genre.id,
                    &release_public_id,
                );
                Ok((score, release))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        scored.sort();
        Ok(scored.into_iter().map(|(_, release)| release).collect())
    }

    fn picked(cover: Option<ResolvedCover>) -> Option<DbId> {
        cover.map(|cover| cover.owner_db_id)
    }

    #[tokio::test]
    async fn other_users_listens_only_pick_the_cover_with_instance_stats() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;

        let mut db = new_test_db()?;
        let (popular, popular_track) = covered_release(&mut db, "popular")?;
        let (own, own_track) = covered_release(&mut db, "own")?;
        let mut listened_at = 1_000;
        for name in ["bob", "carol", "dave"] {
            let other = lyra_db::users::create(&mut db, &test_user(name)?)?;
            for _ in 0..20 {
                listened_at += 1;
                insert_listen(&mut db, other, popular_track, listened_at)?;
            }
        }
        let user_db_id = lyra_db::users::create(&mut db, &test_user("alice")?)?;
        for _ in 0..3 {
            listened_at += 1;
            insert_listen(&mut db, user_db_id, own_track, listened_at)?;
        }
        let genre = rock(&mut db)?;
        let viewer = viewer(&db, user_db_id, vec![Permission::Admin], &[])?;

        let cover = || {
            cover_for_genre(&db, &mut CoverFilter::new(&viewer), &genre, Listens::Viewer)
                .map(picked)
        };
        assert_eq!(cover()?, Some(popular));

        let mut config = STATE.config().as_ref().clone();
        config.instance_stats = false;
        crate::services::testing::publish_config(config);
        assert_eq!(cover()?, Some(own));
        Ok(())
    }

    #[tokio::test]
    async fn without_a_listening_user_the_random_winner_shows() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;

        let mut db = new_test_db()?;
        let releases = [
            covered_release(&mut db, "first")?,
            covered_release(&mut db, "second")?,
        ];
        let genre = rock(&mut db)?;
        let winner = random_winner(&db, &genre)?;
        let (other, other_track) = releases
            .into_iter()
            .find(|(release, _)| *release != winner)
            .expect("a release besides the winner");
        let user_db_id = lyra_db::users::create(&mut db, &test_user("alice")?)?;
        for listened_at in 1_000..1_003 {
            insert_listen(&mut db, user_db_id, other_track, listened_at)?;
        }
        let viewer = viewer(&db, user_db_id, vec![Permission::Admin], &[])?;

        let pick = |viewer: &Viewer, listens| {
            cover_for_genre(&db, &mut CoverFilter::new(viewer), &genre, listens).map(picked)
        };
        assert_eq!(pick(&viewer, Listens::Viewer)?, Some(other));
        assert_eq!(pick(&viewer, Listens::Instance)?, Some(winner));
        assert_eq!(pick(&Viewer::System, Listens::Viewer)?, Some(winner));
        Ok(())
    }

    #[tokio::test]
    async fn a_hidden_random_winner_falls_back_to_a_visible_candidate() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;

        let mut db = new_test_db()?;
        let releases = [
            covered_release(&mut db, "first")?.0,
            covered_release(&mut db, "second")?.0,
            covered_release(&mut db, "third")?.0,
        ];
        let genre = rock(&mut db)?;
        let winner = random_winner(&db, &genre)?;
        let shown = insert_library(&mut db, "Shown", "/shown")?;
        let hidden = insert_library(&mut db, "Hidden", "/hidden")?;
        for release in releases {
            connect(
                &mut db,
                if release == winner { hidden } else { shown },
                release,
            )?;
        }
        let user_db_id = lyra_db::users::create(&mut db, &test_user("alice")?)?;
        let viewer = viewer(&db, user_db_id, Vec::new(), &[shown])?;

        let visible = by_score(
            &db,
            &genre,
            releases.into_iter().filter(|release| *release != winner),
        )?;

        let genre_db_id = genre.db_id.clone().map(DbId::from).expect("genre id");
        let listed = covers_for_genres(
            &db,
            &mut CoverFilter::new(&viewer),
            &[genre],
            Listens::Viewer,
        )?;
        assert_eq!(
            listed.get(&genre_db_id).map(|cover| cover.owner_db_id),
            Some(visible[0])
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_library_scoped_cover_comes_from_that_library() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;

        let mut db = new_test_db()?;
        let releases = [
            covered_release(&mut db, "first")?.0,
            covered_release(&mut db, "second")?.0,
            covered_release(&mut db, "third")?.0,
        ];
        let genre = rock(&mut db)?;
        let winner = random_winner(&db, &genre)?;
        let scoped = insert_library(&mut db, "Scoped", "/scoped")?;
        let other = insert_library(&mut db, "Other", "/other")?;
        for release in releases {
            connect(
                &mut db,
                if release == winner { other } else { scoped },
                release,
            )?;
        }
        let in_scope = by_score(
            &db,
            &genre,
            releases.into_iter().filter(|release| *release != winner),
        )?;

        let pick = |library| {
            cover_for_genre(
                &db,
                &mut CoverFilter::new(&Viewer::System).in_library(library),
                &genre,
                Listens::Viewer,
            )
            .map(picked)
        };
        assert_eq!(pick(None)?, Some(winner));
        assert_eq!(pick(Some(scoped))?, Some(in_scope[0]));
        Ok(())
    }
}
