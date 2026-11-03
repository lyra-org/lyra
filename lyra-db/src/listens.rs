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
    DbElement,
    DbId,
    QueryBuilder,
};

#[derive(DbElement, Clone, Debug, PartialEq, Eq)]
pub struct Listen {
    pub db_id: Option<DbId>,
    pub id: String,
    pub play_id: String,
    pub track_public_id: String,
    pub started_at_ms: u64,
    /// Absent when the player declared the listen without a playing time.
    pub played_ms: Option<u64>,
    pub created_at_ms: u64,
}

/// Inserts the user's listen of the track; the caller owns the transaction.
pub fn insert(
    db: &mut impl super::DbAccess,
    listen: &Listen,
    track_db_id: DbId,
    user_db_id: DbId,
) -> anyhow::Result<DbId> {
    let listen_id = db
        .exec_mut(QueryBuilder::insert().element(listen).query())?
        .ids()[0];

    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from("listens")
            .to(listen_id)
            .query(),
    )?;
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(listen_id)
            .to(track_db_id)
            .query(),
    )?;
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(listen_id)
            .to(user_db_id)
            .query(),
    )?;
    super::covers::display::record_genre_listen(db, track_db_id, user_db_id, listen.started_at_ms)?;

    Ok(listen_id)
}

/// The user's listen of the play, if one was recorded.
pub fn find_by_play_id(
    db: &impl super::DbAccess,
    user_db_id: DbId,
    play_id: &str,
) -> anyhow::Result<Option<DbId>> {
    super::graph::find_owned_by_index(db, "play_id", play_id, "Listen", user_db_id)
}

/// The user's listen of the play, with its stored values.
pub fn get_by_play_id(
    db: &impl super::DbAccess,
    user_db_id: DbId,
    play_id: &str,
) -> anyhow::Result<Option<Listen>> {
    let Some(listen_db_id) = find_by_play_id(db, user_db_id, play_id)? else {
        return Ok(None);
    };
    super::graph::fetch_typed_by_id(db, listen_db_id, "Listen")
}

pub fn set_played_ms(
    db: &mut impl super::DbAccess,
    listen_db_id: DbId,
    played_ms: u64,
) -> anyhow::Result<()> {
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform([("played_ms", played_ms).into()])
            .ids(listen_db_id)
            .query(),
    )?;
    Ok(())
}

fn get_listen_ids_for_target(db: &DbAny, target_id: DbId) -> anyhow::Result<Vec<DbId>> {
    let result = db.exec(
        QueryBuilder::search()
            .to(target_id)
            .where_()
            .node()
            .and()
            .distance(agdb::CountComparison::Equal(2))
            .query(),
    )?;
    Ok(result
        .elements
        .into_iter()
        .filter(|e| e.id.0 > 0)
        .map(|e| e.id)
        .collect())
}

pub struct ListenStats {
    pub db_id: DbId,
    pub count: u64,
    pub last_played: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct ListenSummary {
    pub user_public_id: String,
    pub track_db_id: DbId,
    pub track_public_id: String,
    pub count: u64,
    pub last_played: Option<u64>,
}

/// Whose listens a query counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ListenOwners {
    User(DbId),
    All,
}

/// The user's listen stats for each requested track, from one read of the user's listens.
pub fn get_stats(
    db: &DbAny,
    track_db_ids: &[DbId],
    user_db_id: DbId,
) -> anyhow::Result<Vec<ListenStats>> {
    let unique_ids = super::dedup_positive_ids(track_db_ids);
    if unique_ids.is_empty() {
        return Ok(Vec::new());
    }

    let track_public_ids = super::lookup::find_ids_by_db_ids(db, &unique_ids)?;
    let track_ids_by_public_id = track_public_ids
        .iter()
        .map(|(db_id, public_id)| (public_id.clone(), *db_id))
        .collect::<HashMap<_, _>>();
    if track_ids_by_public_id.is_empty() {
        return Ok(unique_ids
            .into_iter()
            .map(|db_id| ListenStats {
                db_id,
                count: 0,
                last_played: None,
            })
            .collect());
    }

    let listen_ids = get_listen_ids_for_target(db, user_db_id)?;
    let listens = super::graph::bulk_fetch_typed::<Listen>(db, listen_ids, "Listen")?;
    let mut stats_by_track = HashMap::<DbId, ListenStats>::new();
    for listen in listens.into_values() {
        if listen.track_public_id.is_empty() {
            continue;
        }
        let Some(track_db_id) = track_ids_by_public_id.get(&listen.track_public_id).copied() else {
            continue;
        };
        let entry = stats_by_track.entry(track_db_id).or_insert(ListenStats {
            db_id: track_db_id,
            count: 0,
            last_played: None,
        });
        entry.count = entry.count.saturating_add(1);
        if listen.started_at_ms > entry.last_played.unwrap_or(0) {
            entry.last_played = Some(listen.started_at_ms);
        }
    }

    Ok(unique_ids
        .into_iter()
        .map(|db_id| {
            stats_by_track.remove(&db_id).unwrap_or(ListenStats {
                db_id,
                count: 0,
                last_played: None,
            })
        })
        .collect())
}

/// The tracks the user has listened to at least once.
pub fn listened_track_ids(db: &DbAny, user_db_id: DbId) -> anyhow::Result<HashSet<DbId>> {
    let listen_ids = get_listen_ids_for_target(db, user_db_id)?;
    let listens = super::graph::bulk_fetch_typed::<Listen>(db, listen_ids, "Listen")?;
    let public_ids = listens
        .values()
        .map(|listen| listen.track_public_id.as_str())
        .filter(|public_id| !public_id.is_empty())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    Ok(super::lookup::find_node_ids_by_ids(db, &public_ids)?
        .into_values()
        .collect())
}

pub fn get_counts(
    db: &DbAny,
    track_db_ids: &[DbId],
    user_db_id: DbId,
) -> anyhow::Result<HashMap<DbId, u64>> {
    Ok(get_stats(db, track_db_ids, user_db_id)?
        .into_iter()
        .map(|s| (s.db_id, s.count))
        .collect())
}

fn resolve_track_ids_by_public_id(
    db: &DbAny,
    track_public_ids: &HashSet<String>,
) -> anyhow::Result<HashMap<String, DbId>> {
    let mut resolved = HashMap::with_capacity(track_public_ids.len());
    for track_public_id in track_public_ids {
        if let Some(track_db_id) =
            super::lookup::find_id_by_index(db, "id", track_public_id, "Track")?
        {
            resolved.insert(track_public_id.clone(), track_db_id);
        }
    }
    Ok(resolved)
}

fn resolve_listen_user_ids(db: &DbAny, listens: &[Listen]) -> anyhow::Result<HashMap<DbId, DbId>> {
    let mut targets_by_listen: HashMap<DbId, Vec<DbId>> = HashMap::new();
    let mut target_ids = Vec::new();
    let mut seen_targets = HashSet::new();

    for listen in listens {
        let Some(listen_db_id) = listen.db_id else {
            continue;
        };
        for edge in super::graph::direct_edges_from(db, listen_db_id)? {
            let target_id = edge.to;
            if target_id.0 == 0 {
                continue;
            }
            targets_by_listen
                .entry(listen_db_id)
                .or_default()
                .push(target_id);
            if target_id.0 > 0 && seen_targets.insert(target_id) {
                target_ids.push(target_id);
            }
        }
    }

    let users = super::graph::bulk_fetch_typed::<super::users::User>(db, target_ids, "User")?;
    let mut user_by_listen = HashMap::new();
    for (listen_db_id, target_ids) in targets_by_listen {
        let Some(user_db_id) = target_ids
            .into_iter()
            .find(|target_id| users.contains_key(target_id))
        else {
            continue;
        };
        user_by_listen.insert(listen_db_id, user_db_id);
    }
    Ok(user_by_listen)
}

struct ListenSummaryAccumulator {
    user_public_id: String,
    count: u64,
    last_played: Option<u64>,
}

pub fn list_summaries(
    db: &DbAny,
    owners: ListenOwners,
    track_public_ids: Option<&HashSet<String>>,
) -> anyhow::Result<Vec<ListenSummary>> {
    let listens: Vec<Listen> = if let ListenOwners::User(user_db_id) = owners {
        let listen_ids = get_listen_ids_for_target(db, user_db_id)?;
        if listen_ids.is_empty() {
            return Ok(Vec::new());
        }
        super::graph::bulk_fetch_typed::<Listen>(db, listen_ids, "Listen")?
            .into_values()
            .collect()
    } else {
        db.exec(
            QueryBuilder::select()
                .elements::<Listen>()
                .search()
                .from("listens")
                .query(),
        )?
        .try_into()?
    };
    if listens.is_empty() {
        return Ok(Vec::new());
    }

    let owners_by_listen: HashMap<DbId, (DbId, String)> =
        if let ListenOwners::User(user_db_id) = owners {
            let Some(user) = super::users::get_by_id(db, user_db_id)? else {
                return Ok(Vec::new());
            };
            listens
                .iter()
                .filter_map(|listen| {
                    listen
                        .db_id
                        .map(|listen_db_id| (listen_db_id, (user_db_id, user.id.clone())))
                })
                .collect()
        } else {
            let user_ids_by_listen = resolve_listen_user_ids(db, &listens)?;
            let mut unique_user_ids = Vec::new();
            let mut seen_user_ids = HashSet::new();
            for user_db_id in user_ids_by_listen.values() {
                if seen_user_ids.insert(*user_db_id) {
                    unique_user_ids.push(*user_db_id);
                }
            }

            let users =
                super::graph::bulk_fetch_typed::<super::users::User>(db, unique_user_ids, "User")?;
            user_ids_by_listen
                .into_iter()
                .filter_map(|(listen_db_id, user_db_id)| {
                    users
                        .get(&user_db_id)
                        .map(|user| (listen_db_id, (user_db_id, user.id.clone())))
                })
                .collect()
        };

    let mut by_user_track: HashMap<(DbId, String), ListenSummaryAccumulator> = HashMap::new();
    for listen in listens {
        if listen.track_public_id.is_empty() {
            continue;
        }
        if let Some(track_public_ids) = track_public_ids
            && !track_public_ids.contains(&listen.track_public_id)
        {
            continue;
        }
        let Some(listen_db_id) = listen.db_id else {
            continue;
        };
        let Some((user_db_id, user_public_id)) = owners_by_listen.get(&listen_db_id) else {
            continue;
        };

        let entry = by_user_track
            .entry((*user_db_id, listen.track_public_id))
            .or_insert_with(|| ListenSummaryAccumulator {
                user_public_id: user_public_id.clone(),
                count: 0,
                last_played: None,
            });
        entry.count = entry.count.saturating_add(1);
        if listen.started_at_ms > entry.last_played.unwrap_or(0) {
            entry.last_played = Some(listen.started_at_ms);
        }
    }

    let requested_track_public_ids = by_user_track
        .keys()
        .map(|(_, track_public_id)| track_public_id.clone())
        .collect::<HashSet<_>>();
    let track_db_ids = resolve_track_ids_by_public_id(db, &requested_track_public_ids)?;

    let mut summaries = Vec::with_capacity(by_user_track.len());
    for ((_, track_public_id), accumulator) in by_user_track {
        let Some(track_db_id) = track_db_ids.get(&track_public_id).copied() else {
            continue;
        };
        summaries.push(ListenSummary {
            user_public_id: accumulator.user_public_id,
            track_db_id,
            track_public_id,
            count: accumulator.count,
            last_played: accumulator.last_played,
        });
    }

    Ok(summaries)
}

pub fn list_summaries_for_user(db: &DbAny, user_db_id: DbId) -> anyhow::Result<Vec<ListenSummary>> {
    list_summaries(db, ListenOwners::User(user_db_id), None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::roots::{
        self,
        Root,
    };
    use crate::test_db::new_test_db;
    use agdb::QueryBuilder;
    use nanoid::nanoid;

    fn create_user(db: &mut DbAny) -> anyhow::Result<DbId> {
        let user = crate::users::User {
            db_id: None,
            id: nanoid!(),
            username: format!("user-{}", nanoid!()),
            password: "hash".to_string(),
        };
        crate::users::create(db, &user)
    }

    fn create_track(db: &mut DbAny, public_id: &str) -> anyhow::Result<DbId> {
        let track = crate::tracks::Track {
            db_id: None,
            id: public_id.to_string(),
            track_title: "track".to_string(),
            sort_title: None,
            year: None,
            disc: None,
            disc_total: None,
            track: None,
            track_total: None,
            duration_ms: None,
            track_gain_db: None,
            album_gain_db: None,
            locked: None,
            created_at_ms: None,
            file_created_at_ms: None,
        };
        let track_db_id = db
            .exec_mut(QueryBuilder::insert().element(&track).query())?
            .ids()[0];
        roots::link(db, Root::Tracks, track_db_id)?;
        Ok(track_db_id)
    }

    fn insert_listen_with_snapshot(
        db: &mut DbAny,
        user_db_id: DbId,
        track_db_id: DbId,
        track_public_id: &str,
        play_id: &str,
        started_at_ms: u64,
    ) -> anyhow::Result<DbId> {
        let listen = Listen {
            db_id: None,
            id: nanoid!(),
            play_id: play_id.to_string(),
            track_public_id: track_public_id.to_string(),
            started_at_ms,
            played_ms: Some(180_000),
            created_at_ms: started_at_ms,
        };
        db.transaction_mut(|t| insert(t, &listen, track_db_id, user_db_id))
    }

    #[test]
    fn listen_persists_track_public_id_snapshot_on_recorded_listen() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_user(&mut db)?;
        let track_db_id = create_track(&mut db, "tr-original")?;

        insert_listen_with_snapshot(&mut db, user, track_db_id, "tr-original", &nanoid!(), 1)?;

        let listens: Vec<Listen> = db
            .exec(
                QueryBuilder::select()
                    .elements::<Listen>()
                    .search()
                    .from("listens")
                    .query(),
            )?
            .try_into()?;
        assert_eq!(listens.len(), 1);
        assert_eq!(listens[0].track_public_id, "tr-original");
        Ok(())
    }

    #[test]
    fn find_by_play_id_matches_only_the_users_listen() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_user(&mut db)?;
        let other_user = create_user(&mut db)?;
        let track = create_track(&mut db, "tr-one")?;

        let listen =
            insert_listen_with_snapshot(&mut db, user, track, "tr-one", "play-shared-0001", 1_000)?;
        let other_listen = insert_listen_with_snapshot(
            &mut db,
            other_user,
            track,
            "tr-one",
            "play-shared-0001",
            2_000,
        )?;

        assert_eq!(
            find_by_play_id(&db, user, "play-shared-0001")?,
            Some(listen)
        );
        assert_eq!(
            find_by_play_id(&db, other_user, "play-shared-0001")?,
            Some(other_listen)
        );
        assert_eq!(find_by_play_id(&db, user, "play-unknown-0001")?, None);
        Ok(())
    }

    #[test]
    fn get_by_play_id_reads_the_stored_listen() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_user(&mut db)?;
        let track = create_track(&mut db, "tr-one")?;
        let listen =
            insert_listen_with_snapshot(&mut db, user, track, "tr-one", "play-stored-00001", 7)?;

        set_played_ms(&mut db, listen, 200_000)?;

        let stored = get_by_play_id(&db, user, "play-stored-00001")?.expect("listen exists");
        assert_eq!(stored.db_id, Some(listen));
        assert_eq!(stored.started_at_ms, 7);
        assert_eq!(stored.played_ms, Some(200_000));
        assert!(get_by_play_id(&db, user, "play-unknown-0001")?.is_none());
        Ok(())
    }

    #[test]
    fn a_listen_without_playing_time_stores_no_key_and_reads_as_none() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_user(&mut db)?;
        let track = create_track(&mut db, "tr-one")?;
        let listen = Listen {
            db_id: None,
            id: nanoid!(),
            play_id: "play-untimed-0001".to_string(),
            track_public_id: "tr-one".to_string(),
            started_at_ms: 7,
            played_ms: None,
            created_at_ms: 7,
        };
        let listen_db_id = db.transaction_mut(|t| insert(t, &listen, track, user))?;

        let keys = db.exec(QueryBuilder::select().keys().ids(listen_db_id).query())?;
        assert!(
            !keys.elements[0]
                .values
                .iter()
                .any(|value| value.key == "played_ms".into()),
            "no played_ms key"
        );
        let stored = get_by_play_id(&db, user, "play-untimed-0001")?.expect("listen exists");
        assert_eq!(stored.played_ms, None);

        set_played_ms(&mut db, listen_db_id, 40_000)?;
        let stored = get_by_play_id(&db, user, "play-untimed-0001")?.expect("listen exists");
        assert_eq!(stored.played_ms, Some(40_000));
        Ok(())
    }

    fn run_get_stats_with_listen_snapshot(snapshot: &str) -> anyhow::Result<ListenStats> {
        let mut db = new_test_db()?;
        let user = create_user(&mut db)?;
        let track_db_id = create_track(&mut db, "tr-current")?;

        insert_listen_with_snapshot(&mut db, user, track_db_id, snapshot, &nanoid!(), 1)?;

        let mut stats = get_stats(&db, &[track_db_id], user)?;
        anyhow::ensure!(stats.len() == 1, "expected exactly one stats row");
        Ok(stats.remove(0))
    }

    #[test]
    fn list_summaries_for_user_groups_by_track_and_user() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_user(&mut db)?;
        let other_user = create_user(&mut db)?;
        let track_one = create_track(&mut db, "tr-one")?;
        let track_two = create_track(&mut db, "tr-two")?;

        insert_listen_with_snapshot(&mut db, user, track_one, "tr-one", &nanoid!(), 1_000)?;
        insert_listen_with_snapshot(&mut db, user, track_one, "tr-one", &nanoid!(), 3_000)?;
        insert_listen_with_snapshot(&mut db, user, track_two, "tr-two", &nanoid!(), 2_000)?;
        insert_listen_with_snapshot(&mut db, other_user, track_one, "tr-one", &nanoid!(), 9_000)?;

        let user_public_id = crate::users::get_by_id(&db, user)?
            .expect("user should exist")
            .id;
        let summaries = list_summaries_for_user(&db, user)?;
        assert_eq!(summaries.len(), 2);
        let by_track = summaries
            .into_iter()
            .map(|summary| (summary.track_public_id.clone(), summary))
            .collect::<HashMap<_, _>>();

        let one = by_track.get("tr-one").expect("track one summary");
        assert_eq!(one.user_public_id, user_public_id);
        assert_eq!(one.count, 2);
        assert_eq!(one.last_played, Some(3_000));
        assert_eq!(one.track_db_id, track_one);

        let two = by_track.get("tr-two").expect("track two summary");
        assert_eq!(two.user_public_id, user_public_id);
        assert_eq!(two.count, 1);
        assert_eq!(two.last_played, Some(2_000));
        assert_eq!(two.track_db_id, track_two);
        Ok(())
    }

    #[test]
    fn list_summaries_groups_by_user_and_track() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_user(&mut db)?;
        let other_user = create_user(&mut db)?;
        let track_one = create_track(&mut db, "tr-one")?;
        let track_two = create_track(&mut db, "tr-two")?;

        insert_listen_with_snapshot(&mut db, user, track_one, "tr-one", &nanoid!(), 1_000)?;
        insert_listen_with_snapshot(&mut db, user, track_one, "tr-one", &nanoid!(), 3_000)?;
        insert_listen_with_snapshot(&mut db, other_user, track_one, "tr-one", &nanoid!(), 9_000)?;
        insert_listen_with_snapshot(&mut db, other_user, track_two, "tr-two", &nanoid!(), 4_000)?;

        let user_public_id = crate::users::get_by_id(&db, user)?
            .expect("user should exist")
            .id;
        let other_user_public_id = crate::users::get_by_id(&db, other_user)?
            .expect("other user should exist")
            .id;
        let summaries = list_summaries(&db, ListenOwners::All, None)?;
        assert_eq!(summaries.len(), 3);
        let by_user_track = summaries
            .into_iter()
            .map(|summary| {
                (
                    (
                        summary.user_public_id.clone(),
                        summary.track_public_id.clone(),
                    ),
                    summary,
                )
            })
            .collect::<HashMap<_, _>>();

        let user_one = by_user_track
            .get(&(user_public_id, "tr-one".to_string()))
            .expect("user track one summary");
        assert_eq!(user_one.count, 2);
        assert_eq!(user_one.last_played, Some(3_000));
        assert!(!user_one.user_public_id.is_empty());

        let other_one = by_user_track
            .get(&(other_user_public_id.clone(), "tr-one".to_string()))
            .expect("other user track one summary");
        assert_eq!(other_one.count, 1);
        assert_eq!(other_one.last_played, Some(9_000));

        let other_two = by_user_track
            .get(&(other_user_public_id, "tr-two".to_string()))
            .expect("other user track two summary");
        assert_eq!(other_two.count, 1);
        assert_eq!(other_two.last_played, Some(4_000));
        Ok(())
    }

    #[test]
    fn list_summaries_for_user_skips_unresolved_snapshots() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_user(&mut db)?;
        let track = create_track(&mut db, "tr-current")?;

        insert_listen_with_snapshot(&mut db, user, track, "tr-current", &nanoid!(), 1_000)?;
        insert_listen_with_snapshot(&mut db, user, track, "tr-missing", &nanoid!(), 2_000)?;

        let summaries = list_summaries_for_user(&db, user)?;
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].track_public_id, "tr-current");
        assert_eq!(summaries[0].count, 1);
        assert_eq!(summaries[0].last_played, Some(1_000));
        Ok(())
    }

    #[test]
    fn get_stats_skips_listens_with_disagreeing_or_empty_snapshot() -> anyhow::Result<()> {
        for snapshot in ["", "tr-original"] {
            let stats = run_get_stats_with_listen_snapshot(snapshot)?;
            assert_eq!(stats.count, 0, "snapshot {snapshot:?} should elide");
            assert_eq!(stats.last_played, None, "snapshot {snapshot:?}");
        }
        Ok(())
    }

    #[test]
    fn get_stats_preserves_requested_track_rows() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_user(&mut db)?;
        let other_user = create_user(&mut db)?;
        let track_one = create_track(&mut db, "tr-one")?;
        let track_two = create_track(&mut db, "tr-two")?;

        insert_listen_with_snapshot(&mut db, user, track_one, "tr-one", &nanoid!(), 1_000)?;
        insert_listen_with_snapshot(&mut db, user, track_one, "tr-one", &nanoid!(), 3_000)?;
        insert_listen_with_snapshot(&mut db, user, track_two, "tr-old", &nanoid!(), 5_000)?;
        insert_listen_with_snapshot(&mut db, other_user, track_two, "tr-two", &nanoid!(), 7_000)?;

        let stats = get_stats(&db, &[track_one, track_two, track_one], user)?;
        assert_eq!(stats.len(), 2);
        let by_track = stats
            .into_iter()
            .map(|stat| (stat.db_id, stat))
            .collect::<HashMap<_, _>>();

        let one = by_track.get(&track_one).expect("track one stats");
        assert_eq!(one.count, 2);
        assert_eq!(one.last_played, Some(3_000));

        let two = by_track.get(&track_two).expect("track two stats");
        assert_eq!(two.count, 0);
        assert_eq!(two.last_played, None);
        Ok(())
    }
}
