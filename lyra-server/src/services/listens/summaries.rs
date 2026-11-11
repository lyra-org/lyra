// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Each user's listens to each track, as the listens lists filter, sort and page them: by
//! internal ids throughout, so only the rows of the page shown need their public ids.

use std::{
    cmp::Ordering,
    collections::{
        HashMap,
        HashSet,
    },
};

use agdb::{
    DbAny,
    DbId,
};

use super::ListenStats;
use crate::services::{
    auth::{
        Principal,
        access::entity_accessible,
    },
    catalog::{
        self,
        CatalogError,
        Direction,
        tracks::Tracks,
    },
    playback_sessions::resolve_merged_track_ids_for_play_count,
};

/// One user's listens to one track.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Summary {
    pub(crate) user_db_id: DbId,
    pub(crate) track_db_id: DbId,
    pub(crate) stats: ListenStats,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SortKey {
    ListenCount,
    LastPlayedAt,
    /// The user's public id, which [`sort`] is given.
    UserId,
}

/// The tracks whose listens count as listens to `track_db_id`: the track itself and, with
/// `unique_track_id_pairs`, every track sharing one of those external ids with it. Only the ones
/// `principal` can access.
pub(crate) fn counted_tracks(
    db: &DbAny,
    principal: &Principal,
    track_db_id: DbId,
    unique_track_id_pairs: Option<&HashSet<(String, String)>>,
) -> anyhow::Result<Vec<DbId>> {
    let tracks = match unique_track_id_pairs {
        Some(pairs) => resolve_merged_track_ids_for_play_count(db, track_db_id, pairs)?,
        None => vec![track_db_id],
    };
    let mut counted = Vec::with_capacity(tracks.len());
    for track in tracks {
        if entity_accessible(db, principal, track)? {
            counted.push(track);
        }
    }
    Ok(counted)
}

/// A summary for each track one of `users` listened to that `principal` sees, as the catalog
/// lists tracks for it, whether or not they can play.
pub(crate) fn of_users(
    db: &DbAny,
    principal: &Principal,
    users: &[DbId],
) -> Result<Vec<Summary>, CatalogError> {
    let mut summaries = Vec::new();
    for &user_db_id in users {
        summaries.extend(lyra_db::listens::totals(db, user_db_id)?.into_iter().map(
            |(track_db_id, total)| Summary {
                user_db_id,
                track_db_id,
                stats: ListenStats::over([total]),
            },
        ));
    }
    let tracks = summaries
        .iter()
        .map(|summary| summary.track_db_id)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if let Some(seen) = seen(db, principal, &tracks)? {
        summaries.retain(|summary| seen.contains(&summary.track_db_id));
    }
    Ok(summaries)
}

/// The user's listens to the `counted` tracks as one summary of `track_db_id`, which a user
/// without any still gets.
pub(crate) fn of_track(
    db: &DbAny,
    user_db_id: DbId,
    track_db_id: DbId,
    counted: &[DbId],
) -> anyhow::Result<Summary> {
    let mut totals = Vec::with_capacity(counted.len());
    for &track in counted {
        totals.extend(lyra_db::listens::total(db, user_db_id, track)?);
    }
    Ok(Summary {
        user_db_id,
        track_db_id,
        stats: ListenStats::over(totals),
    })
}

/// [`of_track`] for each of `users` who listened to one of the `counted` tracks `principal` sees
/// in the catalog.
pub(crate) fn of_track_by_users(
    db: &DbAny,
    principal: &Principal,
    users: &[DbId],
    track_db_id: DbId,
    counted: &[DbId],
) -> Result<Vec<Summary>, CatalogError> {
    let mut counted = counted.to_vec();
    if let Some(seen) = seen(db, principal, &counted)? {
        counted.retain(|track| seen.contains(track));
    }
    let mut summaries = Vec::new();
    for &user_db_id in users {
        let summary = of_track(db, user_db_id, track_db_id, &counted)?;
        if summary.stats.count > 0 {
            summaries.push(summary);
        }
    }
    Ok(summaries)
}

/// The tracks among `tracks` the catalog shows `principal`, or `None` when it shows them all.
fn seen(
    db: &DbAny,
    principal: &Principal,
    tracks: &[DbId],
) -> Result<Option<HashSet<DbId>>, CatalogError> {
    let viewer = catalog::Viewer::user(db, principal.clone())?;
    if viewer.sees_everything() {
        return Ok(None);
    }
    catalog::lookups::visible::<Tracks>(db, &viewer, tracks).map(Some)
}

/// Sorts by `keys` in `direction`, ascending unless given; without keys, by listen count then
/// last play, descending unless given. A summary never played sorts last either way. Ties
/// break by internal user id, then internal track id. `user_ids` holds the public ids
/// [`SortKey::UserId`] compares.
pub(crate) fn sort(
    summaries: &mut [Summary],
    keys: &[SortKey],
    direction: Option<Direction>,
    user_ids: &HashMap<DbId, String>,
) {
    let (keys, direction) = if keys.is_empty() {
        (
            &[SortKey::ListenCount, SortKey::LastPlayedAt][..],
            direction.unwrap_or(Direction::Descending),
        )
    } else {
        (keys, direction.unwrap_or(Direction::Ascending))
    };
    summaries.sort_unstable_by(|a, b| {
        keys.iter()
            .map(|key| match key {
                SortKey::ListenCount => direction.apply(a.stats.count.cmp(&b.stats.count)),
                SortKey::LastPlayedAt => {
                    match (a.stats.last_played_at_ms, b.stats.last_played_at_ms) {
                        (Some(a), Some(b)) => direction.apply(a.cmp(&b)),
                        (Some(_), None) => Ordering::Less,
                        (None, Some(_)) => Ordering::Greater,
                        (None, None) => Ordering::Equal,
                    }
                }
                SortKey::UserId => direction.apply(
                    user_ids
                        .get(&a.user_db_id)
                        .cmp(&user_ids.get(&b.user_db_id)),
                ),
            })
            .find(|ordering| ordering.is_ne())
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.user_db_id.0.cmp(&b.user_db_id.0))
            .then_with(|| a.track_db_id.0.cmp(&b.track_db_id.0))
    });
}

#[cfg(test)]
mod tests {
    use lyra_db::fixtures::{
        connect,
        insert_listen,
        insert_release,
        insert_track,
    };
    use lyra_db::test_db::{
        insert_library,
        insert_user,
        new_test_db,
    };

    use super::*;

    fn summary(user: i64, track: i64, count: u64, last_played_at_ms: Option<u64>) -> Summary {
        Summary {
            user_db_id: DbId(user),
            track_db_id: DbId(track),
            stats: ListenStats {
                count,
                last_played_at_ms,
            },
        }
    }

    fn sorted(
        mut summaries: Vec<Summary>,
        keys: &[SortKey],
        direction: Option<Direction>,
        user_ids: &[(i64, &str)],
    ) -> Vec<(i64, i64)> {
        let user_ids = user_ids
            .iter()
            .map(|(user, id)| (DbId(*user), id.to_string()))
            .collect();
        sort(&mut summaries, keys, direction, &user_ids);
        summaries
            .into_iter()
            .map(|summary| (summary.user_db_id.0, summary.track_db_id.0))
            .collect()
    }

    #[test]
    fn without_keys_the_most_and_latest_played_come_first() {
        let summaries = vec![
            summary(1, 12, 2, Some(2_000)),
            summary(1, 13, 1, Some(9_000)),
            summary(1, 11, 2, Some(3_000)),
        ];

        assert_eq!(
            sorted(summaries.clone(), &[], None, &[]),
            vec![(1, 11), (1, 12), (1, 13)]
        );
        assert_eq!(
            sorted(summaries, &[], Some(Direction::Ascending), &[]),
            vec![(1, 13), (1, 12), (1, 11)]
        );
    }

    #[test]
    fn ties_break_by_internal_user_then_track_id_in_both_directions() {
        let summaries = vec![
            summary(2, 11, 1, Some(1_000)),
            summary(1, 12, 1, Some(1_000)),
            summary(2, 10, 1, Some(1_000)),
            summary(1, 11, 1, Some(1_000)),
        ];
        let expected = vec![(1, 11), (1, 12), (2, 10), (2, 11)];

        for direction in [
            None,
            Some(Direction::Ascending),
            Some(Direction::Descending),
        ] {
            assert_eq!(sorted(summaries.clone(), &[], direction, &[]), expected);
            assert_eq!(
                sorted(summaries.clone(), &[SortKey::ListenCount], direction, &[]),
                expected
            );
        }
    }

    #[test]
    fn never_played_sorts_last_in_both_directions() {
        for (direction, expected) in [
            (Direction::Ascending, [(1, 11), (1, 12), (1, 13)]),
            (Direction::Descending, [(1, 12), (1, 11), (1, 13)]),
        ] {
            let summaries = vec![
                summary(1, 13, 0, None),
                summary(1, 12, 1, Some(2_000)),
                summary(1, 11, 1, Some(1_000)),
            ];
            assert_eq!(
                sorted(summaries, &[SortKey::LastPlayedAt], Some(direction), &[]),
                expected,
                "{direction:?}"
            );
        }
    }

    #[test]
    fn the_user_id_key_compares_public_ids() {
        let summaries = vec![summary(1, 11, 1, Some(1)), summary(2, 11, 1, Some(1))];
        let user_ids = [(1, "user-b"), (2, "user-a")];

        assert_eq!(
            sorted(summaries.clone(), &[SortKey::UserId], None, &user_ids),
            vec![(2, 11), (1, 11)]
        );
        assert_eq!(
            sorted(
                summaries,
                &[SortKey::UserId],
                Some(Direction::Descending),
                &user_ids
            ),
            vec![(1, 11), (2, 11)]
        );
    }

    /// A release holding one track, in `library`.
    fn track_in(db: &mut DbAny, library: DbId, title: &str) -> anyhow::Result<DbId> {
        let release = insert_release(db, title)?;
        let track = insert_track(db, title)?;
        connect(db, library, release)?;
        lyra_db::fixtures::link_track(db, release, track)?;
        Ok(track)
    }

    fn tracks(summaries: Vec<Summary>) -> HashSet<DbId> {
        summaries
            .into_iter()
            .map(|summary| summary.track_db_id)
            .collect()
    }

    #[test]
    fn summaries_hold_only_tracks_the_catalog_shows_the_viewer() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = insert_user(&mut db, "limited-listener")?;
        let shown = insert_library(&mut db, "Shown", "/listens/shown")?;
        let hidden = insert_library(&mut db, "Hidden", "/listens/hidden")?;
        let seen = track_in(&mut db, shown, "Seen")?;
        let unseen = track_in(&mut db, hidden, "Unseen")?;
        let orphan = insert_track(&mut db, "Orphan")?;
        for track in [seen, unseen, orphan] {
            insert_listen(&mut db, user, track, 1_700_000_000_000)?;
        }
        let shown_id = lyra_db::libraries::get_by_id(&db, shown)?
            .expect("library exists")
            .id;
        let listed = |db: &DbAny, permissions, libraries| {
            let principal = Principal::for_user(db, user, permissions, libraries);
            of_users(db, &principal, &[user]).map(tracks)
        };

        assert_eq!(
            listed(&db, Vec::new(), HashSet::from([shown_id]))?,
            HashSet::from([seen])
        );
        assert!(listed(&db, Vec::new(), HashSet::new())?.is_empty());
        assert_eq!(
            listed(&db, vec![lyra_db::Permission::Admin], HashSet::new())?,
            HashSet::from([seen, unseen, orphan])
        );

        // A caller who no longer exists is refused, as the catalog lists refuse it.
        let caller = insert_user(&mut db, "deleted-caller")?;
        let principal = Principal::for_user(
            &db,
            caller,
            vec![lyra_db::Permission::Admin],
            HashSet::new(),
        );
        lyra_db::test_db::recycle_user(&mut db, caller, "replacement")?;
        assert!(matches!(
            of_users(&db, &principal, &[user]),
            Err(CatalogError::Auth(_))
        ));
        Ok(())
    }

    #[test]
    fn a_track_summary_counts_every_counted_track_and_exists_without_listens() -> anyhow::Result<()>
    {
        let mut db = new_test_db()?;
        let user = insert_user(&mut db, "listener")?;
        let other = insert_user(&mut db, "other")?;
        let first = insert_track(&mut db, "First")?;
        let second = insert_track(&mut db, "Second")?;
        insert_listen(&mut db, user, first, 3_000)?;
        insert_listen(&mut db, user, second, 5_000)?;
        insert_listen(&mut db, user, second, 4_000)?;

        let merged = of_track(&db, user, first, &[first, second])?;
        assert_eq!(merged, summary(user.0, first.0, 3, Some(5_000)));
        assert_eq!(
            of_track(&db, user, first, &[first])?,
            summary(user.0, first.0, 1, Some(3_000))
        );
        assert_eq!(
            of_track(&db, other, first, &[first, second])?,
            summary(other.0, first.0, 0, None)
        );

        let admin =
            Principal::for_user(&db, user, vec![lyra_db::Permission::Admin], HashSet::new());
        assert_eq!(
            of_track_by_users(&db, &admin, &[user, other], first, &[first, second])?,
            vec![merged],
            "a user without listens has no row among everyone's"
        );
        Ok(())
    }
}
