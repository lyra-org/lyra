// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Listens were counted by reading every one of them under the `listens` root and matching the
//! `track_public_id` each kept. Each user now has a `ListenTotals` node with an edge to every
//! track they listened to, holding `listen_count` and `last_played_at_ms`; the root and the
//! key go. A listen counts for the track and user its edges lead to, so listens of a track
//! remapped before this step count again. Listens without a user go; those without a track
//! stay, counted nowhere.

use std::collections::{
    BTreeMap,
    HashMap,
    HashSet,
};

use agdb::{
    CountComparison,
    DbAnyTransactionMut,
    DbId,
    DbKeyValue,
    QueryBuilder,
};

pub(super) fn run(t: &mut DbAnyTransactionMut<'_>) -> anyhow::Result<()> {
    let users = nodes(t, "User")?;
    let tracks = nodes(t, "Track")?;
    // Every listen, whether or not the root still holds it.
    let listens = t
        .exec(
            QueryBuilder::select()
                .values(["started_at_ms"])
                .search()
                .elements()
                .where_()
                .node()
                .and()
                .key("db_element_id")
                .value("Listen")
                .query(),
        )?
        .elements;

    let mut listened_tracks = HashMap::<DbId, HashSet<DbId>>::new();
    let mut listeners = HashMap::<DbId, HashSet<DbId>>::new();
    for listen in &listens {
        for edge in t
            .exec(
                QueryBuilder::search()
                    .from(listen.id)
                    .where_()
                    .edge()
                    .and()
                    .distance(CountComparison::Equal(1))
                    .query(),
            )?
            .elements
        {
            if tracks.contains(&edge.to) {
                listened_tracks
                    .entry(listen.id)
                    .or_default()
                    .insert(edge.to);
            } else if users.contains(&edge.to) {
                listeners.entry(listen.id).or_default().insert(edge.to);
            }
        }
    }

    let mut totals = BTreeMap::<DbId, BTreeMap<DbId, (u64, u64)>>::new();
    let mut kept = Vec::new();
    let mut ownerless = Vec::new();
    for listen in listens {
        let one = |targets: Option<&HashSet<DbId>>, kind: &str| -> anyhow::Result<Option<DbId>> {
            let mut targets = targets.into_iter().flatten().copied();
            let target = targets.next();
            let more = targets.count();
            anyhow::ensure!(
                more == 0,
                "listen {} is of {} {kind}s",
                listen.id.0,
                more + 1
            );
            Ok(target)
        };
        let track = one(listened_tracks.get(&listen.id), "track")?;
        let Some(user) = one(listeners.get(&listen.id), "user")? else {
            ownerless.push(listen.id);
            continue;
        };
        kept.push(listen.id);
        let Some(started_at_ms) = listen.values.first() else {
            anyhow::bail!("listen {} has no start", listen.id.0);
        };
        let started_at_ms = started_at_ms.value.to_u64()?;
        let Some(track) = track else {
            continue;
        };
        totals
            .entry(user)
            .or_default()
            .entry(track)
            .and_modify(|(count, last_played_at_ms)| {
                *count = count.saturating_add(1);
                *last_played_at_ms = (*last_played_at_ms).max(started_at_ms);
            })
            .or_insert((1, started_at_ms));
    }

    if !totals.is_empty() {
        let nodes = t
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values(vec![
                        vec![DbKeyValue::from((
                            "db_element_id",
                            "ListenTotals"
                        ))];
                        totals.len()
                    ])
                    .query(),
            )?
            .ids();
        t.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(totals.keys().copied().collect::<Vec<_>>())
                .to(&nodes)
                .query(),
        )?;
        let mut from = Vec::new();
        let mut to = Vec::new();
        let mut values = Vec::new();
        for (node, tracks) in nodes.iter().zip(totals.values()) {
            for (track, (count, last_played_at_ms)) in tracks {
                from.push(*node);
                to.push(*track);
                values.push(vec![
                    DbKeyValue::from(("listen_count", *count)),
                    ("last_played_at_ms", *last_played_at_ms).into(),
                ]);
            }
        }
        t.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(from)
                .to(to)
                .values(values)
                .query(),
        )?;
    }

    if !kept.is_empty() {
        t.exec_mut(
            QueryBuilder::remove()
                .values(["track_public_id"])
                .ids(kept)
                .query(),
        )?;
    }
    // The root goes first, so removing a listen no longer walks the root's other members.
    t.exec_mut(QueryBuilder::remove().ids("listens").query())?;
    if !ownerless.is_empty() {
        t.exec_mut(QueryBuilder::remove().ids(ownerless).query())?;
    }
    Ok(())
}

/// Every node stored as a `kind` element.
fn nodes(t: &DbAnyTransactionMut<'_>, kind: &str) -> anyhow::Result<HashSet<DbId>> {
    Ok(t.exec(
        QueryBuilder::search()
            .elements()
            .where_()
            .node()
            .and()
            .key("db_element_id")
            .value(kind)
            .query(),
    )?
    .ids()
    .into_iter()
    .collect())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use agdb::{
        DbAny,
        DbId,
        QueryBuilder,
    };

    use crate::{
        fixtures::insert_track,
        listens::{
            ListenTotal,
            get_by_play_id,
            totals,
            track_of,
        },
        test_db::{
            insert_user,
            new_test_db,
            stored_keys,
        },
    };

    fn old_db() -> anyhow::Result<DbAny> {
        let mut db = new_test_db()?;
        db.exec_mut(QueryBuilder::insert().nodes().aliases("listens").query())?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("root")
                .to("listens")
                .query(),
        )?;
        Ok(db)
    }

    /// A listen as the previous release stored it, with an edge to each of `targets`.
    fn seed(
        db: &mut DbAny,
        play_id: &str,
        track_public_id: &str,
        started_at_ms: u64,
        targets: &[DbId],
    ) -> anyhow::Result<DbId> {
        let listen = db
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values([[
                        ("db_element_id", "Listen").into(),
                        ("id", format!("listen-{play_id}")).into(),
                        ("play_id", play_id).into(),
                        ("track_public_id", track_public_id).into(),
                        ("started_at_ms", started_at_ms).into(),
                        ("played_ms", 180_000_u64).into(),
                        ("created_at_ms", started_at_ms).into(),
                    ]])
                    .query(),
            )?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("listens")
                .to(listen)
                .query(),
        )?;
        for target in targets {
            db.exec_mut(
                QueryBuilder::insert()
                    .edges()
                    .from(listen)
                    .to(*target)
                    .query(),
            )?;
        }
        Ok(listen)
    }

    fn total(count: u64, last_played_at_ms: u64) -> ListenTotal {
        ListenTotal {
            count,
            last_played_at_ms,
        }
    }

    fn exists(db: &DbAny, id: DbId) -> bool {
        db.exec(QueryBuilder::select().ids(id).query()).is_ok()
    }

    #[test]
    fn listens_become_each_users_totals_by_track() -> anyhow::Result<()> {
        let mut db = old_db()?;
        let user = insert_user(&mut db, "listener")?;
        let other = insert_user(&mut db, "other")?;
        let silent = insert_user(&mut db, "silent")?;
        let first = insert_track(&mut db, "First")?;
        let second = insert_track(&mut db, "Second")?;
        let listen = seed(
            &mut db,
            "play-0000000000001",
            "tr-first",
            3_000,
            &[first, user],
        )?;
        seed(
            &mut db,
            "play-0000000000002",
            "tr-first",
            9_000,
            &[first, user],
        )?;
        seed(
            &mut db,
            "play-0000000000003",
            "tr-first",
            5_000,
            &[user, first],
        )?;
        seed(
            &mut db,
            "play-0000000000004",
            "tr-second",
            0,
            &[second, user],
        )?;
        seed(
            &mut db,
            "play-0000000000005",
            "tr-first",
            7_000,
            &[first, other],
        )?;

        db.transaction_mut(super::run)?;

        assert_eq!(
            totals(&db, user)?,
            HashMap::from([(first, total(3, 9_000)), (second, total(1, 0))]),
            "a start at 0 is a time"
        );
        assert_eq!(
            totals(&db, other)?,
            HashMap::from([(first, total(1, 7_000))])
        );
        assert!(totals(&db, silent)?.is_empty());
        assert!(!stored_keys(&db, listen)?.contains("track_public_id"));
        let stored = get_by_play_id(&db, user, "play-0000000000001")?.expect("listen stays");
        assert_eq!(
            (stored.started_at_ms, stored.played_ms),
            (3_000, Some(180_000))
        );
        assert_eq!(track_of(&db, listen)?, Some(first));
        assert!(
            db.exec(QueryBuilder::select().aliases().ids(listen).query())
                .is_err()
                && db
                    .exec(QueryBuilder::search().from("listens").query())
                    .is_err(),
            "the listens root is gone"
        );

        // The next listen joins the migrated total.
        crate::fixtures::insert_listen(&mut db, user, first, 4_000)?;
        assert_eq!(totals(&db, user)?[&first], total(4, 9_000));
        Ok(())
    }

    #[test]
    fn a_listen_counts_for_the_track_its_edge_leads_to() -> anyhow::Result<()> {
        let mut db = old_db()?;
        let user = insert_user(&mut db, "listener")?;
        let survivor = insert_track(&mut db, "Survivor")?;
        // A remap moved the edge of the second listen and left the removed track's id on it.
        seed(
            &mut db,
            "play-0000000000001",
            "tr-survivor",
            1_000,
            &[survivor, user],
        )?;
        seed(
            &mut db,
            "play-0000000000002",
            "tr-removed",
            2_000,
            &[survivor, user],
        )?;
        seed(&mut db, "play-0000000000003", "", 3_000, &[survivor, user])?;

        db.transaction_mut(super::run)?;

        assert_eq!(
            totals(&db, user)?,
            HashMap::from([(survivor, total(3, 3_000))])
        );
        Ok(())
    }

    #[test]
    fn duplicate_edges_count_once() -> anyhow::Result<()> {
        let mut db = old_db()?;
        let user = insert_user(&mut db, "listener")?;
        let track = insert_track(&mut db, "Track")?;
        seed(
            &mut db,
            "play-0000000000001",
            "tr-track",
            1_000,
            &[track, user, track, user, track],
        )?;

        db.transaction_mut(super::run)?;

        assert_eq!(
            totals(&db, user)?,
            HashMap::from([(track, total(1, 1_000))])
        );
        Ok(())
    }

    #[test]
    fn a_listen_of_several_tracks_or_users_fails_the_step() -> anyhow::Result<()> {
        for several_users in [false, true] {
            let mut db = old_db()?;
            let user = insert_user(&mut db, "listener")?;
            let other = insert_user(&mut db, "other")?;
            let track = insert_track(&mut db, "Track")?;
            let second = insert_track(&mut db, "Second")?;
            seed(
                &mut db,
                "play-0000000000001",
                "tr-track",
                1_000,
                &[track, user],
            )?;
            let (targets, what) = if several_users {
                (vec![track, user, other], "2 users")
            } else {
                (vec![track, second, user], "2 tracks")
            };
            let listen = seed(&mut db, "play-0000000000002", "tr-track", 2_000, &targets)?;

            let error = db
                .transaction_mut(super::run)
                .expect_err("an ambiguous listen fails the step");

            assert_eq!(
                error.to_string(),
                format!("listen {} is of {what}", listen.0)
            );
            assert!(
                stored_keys(&db, listen)?.contains("track_public_id"),
                "a failed step changes nothing"
            );
        }
        Ok(())
    }

    #[test]
    fn a_listen_without_a_user_goes_and_one_without_a_track_stays_uncounted() -> anyhow::Result<()>
    {
        let mut db = old_db()?;
        let user = insert_user(&mut db, "listener")?;
        let track = insert_track(&mut db, "Track")?;
        let ownerless = seed(&mut db, "play-0000000000001", "tr-track", 1_000, &[track])?;
        let adrift = seed(&mut db, "play-0000000000002", "tr-gone", 2_000, &[])?;
        let trackless = seed(&mut db, "play-0000000000003", "tr-gone", 3_000, &[user])?;

        db.transaction_mut(super::run)?;

        assert!(!exists(&db, ownerless));
        assert!(!exists(&db, adrift));
        assert!(exists(&db, trackless));
        assert!(!stored_keys(&db, trackless)?.contains("track_public_id"));
        assert!(get_by_play_id(&db, user, "play-0000000000003")?.is_some());
        assert!(totals(&db, user)?.is_empty());
        Ok(())
    }

    /// A node of `kind` that no root holds.
    fn unrooted(db: &mut DbAny, kind: &str) -> anyhow::Result<DbId> {
        Ok(db
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values([[("db_element_id", kind).into()]])
                    .query(),
            )?
            .ids()[0])
    }

    #[test]
    fn tracks_and_users_count_by_what_they_are_not_by_their_roots() -> anyhow::Result<()> {
        let mut db = old_db()?;
        let user = unrooted(&mut db, "User")?;
        let track = unrooted(&mut db, "Track")?;
        let listen = seed(
            &mut db,
            "play-0000000000001",
            "tr-track",
            1_000,
            &[track, user],
        )?;

        db.transaction_mut(super::run)?;

        assert!(
            exists(&db, listen),
            "a listen of an unrooted user was deleted"
        );
        assert_eq!(
            totals(&db, user)?,
            HashMap::from([(track, total(1, 1_000))])
        );
        Ok(())
    }

    #[test]
    fn a_second_unrooted_track_or_user_fails_the_step() -> anyhow::Result<()> {
        for kind in ["Track", "User"] {
            let mut db = old_db()?;
            let user = insert_user(&mut db, "listener")?;
            let track = insert_track(&mut db, "Track")?;
            let extra = unrooted(&mut db, kind)?;
            seed(
                &mut db,
                "play-0000000000001",
                "tr-track",
                1_000,
                &[track, user, extra],
            )?;

            assert!(db.transaction_mut(super::run).is_err(), "{kind}");
        }
        Ok(())
    }

    #[test]
    fn a_listen_the_root_does_not_hold_is_migrated_too() -> anyhow::Result<()> {
        let mut db = old_db()?;
        let user = insert_user(&mut db, "listener")?;
        let track = insert_track(&mut db, "Track")?;
        let rooted = seed(
            &mut db,
            "play-0000000000001",
            "tr-track",
            1_000,
            &[track, user],
        )?;
        let stray = seed(
            &mut db,
            "play-0000000000002",
            "tr-track",
            2_000,
            &[track, user],
        )?;
        let ownerless = seed(&mut db, "play-0000000000003", "tr-track", 3_000, &[track])?;
        for listen in [stray, ownerless] {
            db.exec_mut(
                QueryBuilder::remove()
                    .search()
                    .to(listen)
                    .where_()
                    .edge()
                    .and()
                    .distance(agdb::CountComparison::Equal(1))
                    .query(),
            )?;
        }

        db.transaction_mut(super::run)?;

        assert_eq!(
            totals(&db, user)?,
            HashMap::from([(track, total(2, 2_000))])
        );
        for listen in [rooted, stray] {
            assert!(!stored_keys(&db, listen)?.contains("track_public_id"));
        }
        assert!(!exists(&db, ownerless));
        Ok(())
    }

    #[test]
    fn a_database_without_the_root_migrates() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = insert_user(&mut db, "listener")?;
        let track = insert_track(&mut db, "Track")?;
        let listen = db
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values([[
                        ("db_element_id", "Listen").into(),
                        ("id", "listen-rootless").into(),
                        ("play_id", "play-0000000000001").into(),
                        ("track_public_id", "tr-track").into(),
                        ("started_at_ms", 1_000_u64).into(),
                        ("created_at_ms", 1_000_u64).into(),
                    ]])
                    .query(),
            )?
            .ids()[0];
        for target in [track, user] {
            db.exec_mut(
                QueryBuilder::insert()
                    .edges()
                    .from(listen)
                    .to(target)
                    .query(),
            )?;
        }

        db.transaction_mut(super::run)?;

        assert_eq!(
            totals(&db, user)?,
            HashMap::from([(track, total(1, 1_000))])
        );
        assert!(!stored_keys(&db, listen)?.contains("track_public_id"));
        Ok(())
    }

    #[test]
    fn a_database_without_listens_only_loses_the_root() -> anyhow::Result<()> {
        let mut db = old_db()?;
        let user = insert_user(&mut db, "listener")?;

        db.transaction_mut(super::run)?;

        assert!(totals(&db, user)?.is_empty());
        assert!(
            db.exec(QueryBuilder::search().from("listens").query())
                .is_err()
        );
        Ok(())
    }
}
