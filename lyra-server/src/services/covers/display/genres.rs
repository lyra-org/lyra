// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};

use agdb::{
    DbAny,
    DbId,
};

use crate::services::STATE;
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
    profile: &db_display::DisplayCoverProfile,
    kind: DisplayCoverWinnerKind,
    visible_release_ids: Option<&HashSet<DbId>>,
) -> anyhow::Result<Option<(db_display::DisplayCoverWinner, lyra_db::Cover)>> {
    let Some(winner) = db_display::get_winner(db, profile, kind)? else {
        return Ok(None);
    };
    let Some(cover) = super::cover_for_release(db, winner.release_db_id, visible_release_ids)?
    else {
        return Ok(None);
    };
    Ok(Some((winner, cover)))
}

pub(crate) fn cover_for_genre(
    db: &DbAny,
    genre: &lyra_db::genres::Genre,
    user_public_id: &str,
    visible_release_ids: Option<&HashSet<DbId>>,
) -> anyhow::Result<Option<lyra_db::Cover>> {
    select_genre_cover(
        db,
        genre,
        user_public_id,
        visible_release_ids,
        STATE.config().instance_stats,
    )
}

/// Without `instance_stats`, other users' listens neither pick the cover nor raise the bar the
/// user's own listens must clear; the instance profile only supplies the deterministic fallback.
fn select_genre_cover(
    db: &DbAny,
    genre: &lyra_db::genres::Genre,
    user_public_id: &str,
    visible_release_ids: Option<&HashSet<DbId>>,
    instance_stats: bool,
) -> anyhow::Result<Option<lyra_db::Cover>> {
    let user_profile =
        profile_for_genre(db, DisplayCoverScope::User, Some(user_public_id), &genre.id)?;
    let instance_profile = profile_for_genre(db, DisplayCoverScope::Instance, None, &genre.id)?;
    let instance_signals = instance_profile
        .as_ref()
        .map(|(_, profile)| profile)
        .filter(|_| instance_stats);

    if let Some((_, user_profile)) = user_profile.as_ref()
        && super::profile_is_current(user_profile)
        && let Some((personal_winner, cover)) = winner_cover(
            db,
            user_profile,
            DisplayCoverWinnerKind::Personal,
            visible_release_ids,
        )?
        && super::user_signal_is_enough(user_profile, &personal_winner, instance_signals)
    {
        return Ok(Some(cover));
    }

    if let Some((_, instance_profile)) = instance_profile.as_ref()
        && super::profile_is_current(instance_profile)
    {
        if instance_stats
            && let Some((_instance_winner, cover)) = winner_cover(
                db,
                instance_profile,
                DisplayCoverWinnerKind::Instance,
                visible_release_ids,
            )?
            && super::instance_signal_is_enough(instance_profile)
        {
            return Ok(Some(cover));
        }

        if let Some((_, cover)) = winner_cover(
            db,
            instance_profile,
            DisplayCoverWinnerKind::Random,
            visible_release_ids,
        )? {
            return Ok(Some(cover));
        }
    }

    Ok(None)
}

pub(crate) fn covers_for_genres(
    db: &DbAny,
    genres: &[lyra_db::genres::Genre],
    user_public_id: &str,
    visible_release_ids: Option<&HashSet<DbId>>,
) -> anyhow::Result<HashMap<DbId, lyra_db::Cover>> {
    let instance_stats = STATE.config().instance_stats;
    let mut covers = HashMap::new();
    for genre in genres {
        let Some(genre_db_id) = genre.db_id.clone().map(DbId::from) else {
            continue;
        };
        let Some(cover) = select_genre_cover(
            db,
            genre,
            user_public_id,
            visible_release_ids,
            instance_stats,
        )?
        else {
            continue;
        };
        covers.insert(genre_db_id, cover);
    }
    Ok(covers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lyra_db::{
        fixtures::{
            connect,
            insert_listen,
            insert_release,
            insert_track,
            test_user,
        },
        test_db::new_test_db,
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

    #[tokio::test]
    async fn other_users_listens_only_pick_the_cover_with_instance_stats() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;

        let mut db = new_test_db()?;
        let (_, popular_track) = covered_release(&mut db, "popular")?;
        let (_, own_track) = covered_release(&mut db, "own")?;
        let mut listened_at = 1_000;
        for name in ["bob", "carol", "dave"] {
            let other = lyra_db::users::create(&mut db, &test_user(name)?)?;
            for _ in 0..20 {
                listened_at += 1;
                insert_listen(&mut db, other, popular_track, listened_at)?;
            }
        }
        let user = test_user("alice")?;
        let user_public_id = user.id.clone();
        let user_db_id = lyra_db::users::create(&mut db, &user)?;
        for _ in 0..3 {
            listened_at += 1;
            insert_listen(&mut db, user_db_id, own_track, listened_at)?;
        }
        let genre_db_id = lyra_db::genres::resolve_by_name(&mut db, "Rock")?;
        let genre = lyra_db::genres::get_by_id(&db, genre_db_id)?.expect("genre exists");

        let cover = || {
            cover_for_genre(&db, &genre, &user_public_id, None)
                .map(|cover| cover.map(|cover| cover.id))
        };
        assert_eq!(cover()?.as_deref(), Some("popular"));

        let mut config = STATE.config().as_ref().clone();
        config.instance_stats = false;
        crate::services::testing::publish_config(config);
        assert_eq!(cover()?.as_deref(), Some("own"));
        Ok(())
    }
}
