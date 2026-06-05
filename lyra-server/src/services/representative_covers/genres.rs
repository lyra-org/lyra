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

use crate::db::{
    self,
    representative_covers::{
        self,
        RepresentativeCoverScope,
        RepresentativeCoverTargetKind,
        RepresentativeCoverWinnerKind,
    },
};

fn profile_for_genre(
    db: &DbAny,
    scope: RepresentativeCoverScope,
    user_public_id: Option<&str>,
    genre_public_id: &str,
) -> anyhow::Result<Option<(DbId, representative_covers::RepresentativeCoverProfile)>> {
    representative_covers::get_profile(
        db,
        scope,
        RepresentativeCoverTargetKind::Genre,
        user_public_id,
        genre_public_id,
    )
}

fn winner_cover(
    db: &DbAny,
    profile: &representative_covers::RepresentativeCoverProfile,
    kind: RepresentativeCoverWinnerKind,
    visible_release_ids: Option<&HashSet<DbId>>,
) -> anyhow::Result<Option<(representative_covers::RepresentativeCoverWinner, db::Cover)>> {
    let Some(winner) = representative_covers::get_winner(db, profile, kind)? else {
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
    genre: &db::genres::Genre,
    user_public_id: &str,
    visible_release_ids: Option<&HashSet<DbId>>,
) -> anyhow::Result<Option<db::Cover>> {
    let user_profile = profile_for_genre(
        db,
        RepresentativeCoverScope::User,
        Some(user_public_id),
        &genre.id,
    )?;
    let instance_profile =
        profile_for_genre(db, RepresentativeCoverScope::Instance, None, &genre.id)?;

    if let Some((_, user_profile)) = user_profile.as_ref()
        && super::profile_is_current(user_profile)
        && let Some((personal_winner, cover)) = winner_cover(
            db,
            user_profile,
            RepresentativeCoverWinnerKind::Personal,
            visible_release_ids,
        )?
        && super::user_signal_is_enough(
            user_profile,
            &personal_winner,
            instance_profile.as_ref().map(|(_, profile)| profile),
        )
    {
        return Ok(Some(cover));
    }

    if let Some((_, instance_profile)) = instance_profile.as_ref()
        && super::profile_is_current(instance_profile)
    {
        if let Some((_instance_winner, cover)) = winner_cover(
            db,
            instance_profile,
            RepresentativeCoverWinnerKind::Instance,
            visible_release_ids,
        )? && super::instance_signal_is_enough(instance_profile)
        {
            return Ok(Some(cover));
        }

        if let Some((_, cover)) = winner_cover(
            db,
            instance_profile,
            RepresentativeCoverWinnerKind::Random,
            visible_release_ids,
        )? {
            return Ok(Some(cover));
        }
    }

    Ok(None)
}

pub(crate) fn covers_for_genres(
    db: &DbAny,
    genres: &[db::genres::Genre],
    user_public_id: &str,
    visible_release_ids: Option<&HashSet<DbId>>,
) -> anyhow::Result<HashMap<DbId, db::Cover>> {
    let mut covers = HashMap::new();
    for genre in genres {
        let Some(genre_db_id) = genre.db_id.clone().map(DbId::from) else {
            continue;
        };
        let Some(cover) = cover_for_genre(db, genre, user_public_id, visible_release_ids)? else {
            continue;
        };
        covers.insert(genre_db_id, cover);
    }
    Ok(covers)
}
