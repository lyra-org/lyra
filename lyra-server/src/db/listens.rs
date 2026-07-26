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

#[derive(DbElement, Clone, Debug)]
pub(crate) struct Listen {
    pub(crate) db_id: Option<DbId>,
    pub(crate) id: String,
    pub(crate) track_public_id: String,
    pub(crate) position_ms: u64,
    pub(crate) duration_ms: Option<u64>,
    pub(crate) activity_ms: u64,
    pub(crate) state: super::playback_sessions::PlaybackState,
    pub(crate) listened_at_ms: u64,
    pub(crate) created_at_ms: u64,
}

pub(crate) fn create_and_mark_recorded(
    db: &mut DbAny,
    listen: &Listen,
    track_db_id: DbId,
    user_db_id: DbId,
    playback_session: &super::playback_sessions::PlaybackSession,
) -> anyhow::Result<()> {
    db.transaction_mut(|t| -> anyhow::Result<()> {
        let listen_id = t
            .exec_mut(QueryBuilder::insert().element(listen).query())?
            .ids()[0];

        t.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("listens")
                .to(listen_id)
                .query(),
        )?;
        t.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(listen_id)
                .to(track_db_id)
                .query(),
        )?;
        t.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(listen_id)
                .to(user_db_id)
                .query(),
        )?;
        super::covers::display::record_genre_listen(
            t,
            track_db_id,
            user_db_id,
            listen.listened_at_ms,
        )?;
        t.exec_mut(QueryBuilder::insert().element(playback_session).query())?;

        Ok(())
    })
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

pub(crate) struct ListenStats {
    pub(crate) db_id: DbId,
    pub(crate) count: u64,
    pub(crate) last_played: Option<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct ListenSummary {
    pub(crate) user_public_id: String,
    pub(crate) track_db_id: DbId,
    pub(crate) track_public_id: String,
    pub(crate) count: u64,
    pub(crate) last_played: Option<u64>,
}

/// Returns listen stats (count + last played) for each track.
pub(crate) fn get_stats(
    db: &DbAny,
    track_db_ids: &[DbId],
    user_db_id: Option<DbId>,
) -> anyhow::Result<Vec<ListenStats>> {
    let mut unique_ids = Vec::new();
    let mut seen = HashSet::new();
    for track_db_id in track_db_ids {
        if track_db_id.0 <= 0 {
            continue;
        }
        if seen.insert(*track_db_id) {
            unique_ids.push(*track_db_id);
        }
    }

    let mut stats: Vec<ListenStats> = Vec::with_capacity(unique_ids.len());
    if unique_ids.is_empty() {
        return Ok(stats);
    }

    let user_listen_ids: Option<HashSet<DbId>> = user_db_id
        .map(|uid| get_listen_ids_for_target(db, uid))
        .transpose()?
        .map(|ids| ids.into_iter().collect());

    let track_public_ids = super::lookup::find_ids_by_db_ids(db, &unique_ids)?;

    for track_id in unique_ids {
        let track_public_id = track_public_ids.get(&track_id);
        let listens: Vec<Listen> = db
            .exec(
                QueryBuilder::select()
                    .elements::<Listen>()
                    .search()
                    .to(track_id)
                    .where_()
                    .neighbor()
                    .end_where()
                    .query(),
            )?
            .try_into()?;

        let mut count: u64 = 0;
        let mut last_played: Option<u64> = None;
        for listen in &listens {
            if listen.track_public_id.is_empty()
                || track_public_id.map(String::as_str) != Some(listen.track_public_id.as_str())
            {
                continue;
            }
            if let Some(user_ids) = &user_listen_ids {
                let Some(listen_id) = listen.db_id else {
                    continue;
                };
                if !user_ids.contains(&listen_id) {
                    continue;
                }
            }
            count = count.saturating_add(1);
            if listen.listened_at_ms > last_played.unwrap_or(0) {
                last_played = Some(listen.listened_at_ms);
            }
        }

        stats.push(ListenStats {
            db_id: track_id,
            count,
            last_played,
        });
    }

    Ok(stats)
}

pub(crate) fn get_stats_for_user_tracks(
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
        if listen.listened_at_ms > entry.last_played.unwrap_or(0) {
            entry.last_played = Some(listen.listened_at_ms);
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

pub(crate) fn get_counts(
    db: &DbAny,
    track_db_ids: &[DbId],
    user_db_id: Option<DbId>,
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
    if super::indexes::has_index(db, "id")? {
        let mut resolved = HashMap::with_capacity(track_public_ids.len());
        for track_public_id in track_public_ids {
            let Some(track_db_id) = super::lookup::find_node_id_by_id(db, track_public_id)? else {
                continue;
            };
            if super::tracks::get_by_id(db, track_db_id)?.is_some() {
                resolved.insert(track_public_id.clone(), track_db_id);
            }
        }
        return Ok(resolved);
    }

    let tracks = super::tracks::get_direct(db, "tracks")?;
    Ok(tracks
        .into_iter()
        .filter(|track| track_public_ids.contains(&track.id))
        .filter_map(|track| {
            let track_id = track.id;
            let track_db_id = track.db_id.map(DbId::from)?;
            Some((track_id, track_db_id))
        })
        .collect())
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

pub(crate) fn list_summaries(
    db: &DbAny,
    user_db_id: Option<DbId>,
    track_public_ids: Option<&HashSet<String>>,
) -> anyhow::Result<Vec<ListenSummary>> {
    let listens: Vec<Listen> = if let Some(user_db_id) = user_db_id {
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

    let owners_by_listen: HashMap<DbId, (DbId, String)> = if let Some(user_db_id) = user_db_id {
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
        if listen.listened_at_ms > entry.last_played.unwrap_or(0) {
            entry.last_played = Some(listen.listened_at_ms);
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

pub(crate) fn list_summaries_for_user(
    db: &DbAny,
    user_db_id: DbId,
) -> anyhow::Result<Vec<ListenSummary>> {
    list_summaries(db, Some(user_db_id), None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::new_test_db;
    use agdb::QueryBuilder;
    use nanoid::nanoid;

    fn create_user(db: &mut DbAny) -> anyhow::Result<DbId> {
        let user = crate::db::users::User {
            db_id: None,
            id: nanoid!(),
            username: format!("user-{}", nanoid!()),
            password: "hash".to_string(),
        };
        crate::db::users::create(db, &user)
    }

    fn create_track(db: &mut DbAny, public_id: &str) -> anyhow::Result<DbId> {
        let track = crate::db::tracks::Track {
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
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
            locked: None,
            created_at: None,
            ctime: None,
        };
        let track_db_id = db
            .exec_mut(QueryBuilder::insert().element(&track).query())?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("tracks")
                .to(track_db_id)
                .query(),
        )?;
        Ok(track_db_id)
    }

    fn record_listen(
        db: &mut DbAny,
        user_db_id: DbId,
        track_db_id: DbId,
        track_public_id: &str,
        listened_at_ms: u64,
    ) -> anyhow::Result<()> {
        let listen = Listen {
            db_id: None,
            id: nanoid!(),
            track_public_id: track_public_id.to_string(),
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: 180_000,
            state: crate::db::PlaybackState::Completed,
            listened_at_ms,
            created_at_ms: listened_at_ms,
        };
        let session = crate::db::PlaybackSession {
            db_id: None,
            id: nanoid!(),
            client_name: None,
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: Some(180_000),
            last_position_ms: None,
            state: crate::db::PlaybackState::Completed,
            listen_recorded: Some(true),
            updated_at_ms: listened_at_ms,
            created_at_ms: listened_at_ms,
        };
        create_and_mark_recorded(db, &listen, track_db_id, user_db_id, &session)
    }

    #[test]
    fn listen_persists_track_public_id_snapshot_on_recorded_listen() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_user(&mut db)?;
        let track_db_id = create_track(&mut db, "tr-original")?;

        let listen = Listen {
            db_id: None,
            id: nanoid!(),
            track_public_id: "tr-original".to_string(),
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: 180_000,
            state: crate::db::PlaybackState::Completed,
            listened_at_ms: 1,
            created_at_ms: 1,
        };
        let session = crate::db::PlaybackSession {
            db_id: None,
            id: nanoid!(),
            client_name: None,
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: Some(180_000),
            last_position_ms: None,
            state: crate::db::PlaybackState::Completed,
            listen_recorded: Some(true),
            updated_at_ms: 1,
            created_at_ms: 1,
        };
        create_and_mark_recorded(&mut db, &listen, track_db_id, user, &session)?;

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

    fn run_get_stats_with_listen_snapshot(snapshot: &str) -> anyhow::Result<ListenStats> {
        let mut db = new_test_db()?;
        let user = create_user(&mut db)?;
        let track_db_id = create_track(&mut db, "tr-current")?;

        let listen = Listen {
            db_id: None,
            id: nanoid!(),
            track_public_id: snapshot.to_string(),
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: 180_000,
            state: crate::db::PlaybackState::Completed,
            listened_at_ms: 1,
            created_at_ms: 1,
        };
        let session = crate::db::PlaybackSession {
            db_id: None,
            id: nanoid!(),
            client_name: None,
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: Some(180_000),
            last_position_ms: None,
            state: crate::db::PlaybackState::Completed,
            listen_recorded: Some(true),
            updated_at_ms: 1,
            created_at_ms: 1,
        };
        create_and_mark_recorded(&mut db, &listen, track_db_id, user, &session)?;

        let mut stats = get_stats(&db, &[track_db_id], Some(user))?;
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

        record_listen(&mut db, user, track_one, "tr-one", 1_000)?;
        record_listen(&mut db, user, track_one, "tr-one", 3_000)?;
        record_listen(&mut db, user, track_two, "tr-two", 2_000)?;
        record_listen(&mut db, other_user, track_one, "tr-one", 9_000)?;

        let user_public_id = crate::db::users::get_by_id(&db, user)?
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

        record_listen(&mut db, user, track_one, "tr-one", 1_000)?;
        record_listen(&mut db, user, track_one, "tr-one", 3_000)?;
        record_listen(&mut db, other_user, track_one, "tr-one", 9_000)?;
        record_listen(&mut db, other_user, track_two, "tr-two", 4_000)?;

        let user_public_id = crate::db::users::get_by_id(&db, user)?
            .expect("user should exist")
            .id;
        let other_user_public_id = crate::db::users::get_by_id(&db, other_user)?
            .expect("other user should exist")
            .id;
        let summaries = list_summaries(&db, None, None)?;
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

        record_listen(&mut db, user, track, "tr-current", 1_000)?;
        record_listen(&mut db, user, track, "tr-missing", 2_000)?;

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
    fn get_stats_for_user_tracks_preserves_requested_track_rows() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_user(&mut db)?;
        let other_user = create_user(&mut db)?;
        let track_one = create_track(&mut db, "tr-one")?;
        let track_two = create_track(&mut db, "tr-two")?;

        record_listen(&mut db, user, track_one, "tr-one", 1_000)?;
        record_listen(&mut db, user, track_one, "tr-one", 3_000)?;
        record_listen(&mut db, user, track_two, "tr-old", 5_000)?;
        record_listen(&mut db, other_user, track_two, "tr-two", 7_000)?;

        let stats = get_stats_for_user_tracks(&db, &[track_one, track_two, track_one], user)?;
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
