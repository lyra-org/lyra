// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};

use agdb::{
    CountComparison,
    DbElement,
    DbId,
    DbValue,
    QueryBuilder,
};

use super::DbAccess;

const TOTALS_KIND: &str = "ListenTotals";
const COUNT_KEY: &str = "listen_count";
const LAST_PLAYED_KEY: &str = "last_played_at_ms";

/// One counted play. It goes with its user; a removed track leaves its listens behind, as
/// removing them costs too much until agnesoft/agdb#2030 is fixed.
#[derive(DbElement, Clone, Debug, PartialEq, Eq)]
pub struct Listen {
    pub db_id: Option<DbId>,
    pub id: String,
    pub play_id: String,
    pub started_at_ms: u64,
    /// Absent when the player declared the listen without a playing time.
    pub played_ms: Option<u64>,
    pub created_at_ms: u64,
}

/// A user's listens of one track. Each user has one `ListenTotals` node, with an edge holding
/// this to every track they listened to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ListenTotal {
    pub count: u64,
    /// When the latest of the listens started.
    pub last_played_at_ms: u64,
}

/// Inserts the user's listen of the track and counts it in the user's totals; the caller owns
/// the transaction, and whatever else a listen updates.
pub fn insert(
    db: &mut impl DbAccess,
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
            .from([listen_id, listen_id])
            .to([track_db_id, user_db_id])
            .query(),
    )?;
    count(db, user_db_id, track_db_id, listen.started_at_ms)?;

    Ok(listen_id)
}

/// The user's listen of the play, if one was recorded.
pub fn find_by_play_id(
    db: &impl DbAccess,
    user_db_id: DbId,
    play_id: &str,
) -> anyhow::Result<Option<DbId>> {
    super::graph::find_owned_by_index(db, "play_id", play_id, "Listen", user_db_id)
}

/// The user's listen of the play, with its stored values.
pub fn get_by_play_id(
    db: &impl DbAccess,
    user_db_id: DbId,
    play_id: &str,
) -> anyhow::Result<Option<Listen>> {
    let Some(listen_db_id) = find_by_play_id(db, user_db_id, play_id)? else {
        return Ok(None);
    };
    super::graph::fetch_typed_by_id(db, listen_db_id, "Listen")
}

/// The track the listen is of, or none once that track was removed.
pub fn track_of(db: &impl DbAccess, listen_db_id: DbId) -> anyhow::Result<Option<DbId>> {
    Ok(super::graph::neighbor_ids(db, listen_db_id, "Track")?
        .into_iter()
        .next())
}

pub fn set_played_ms(
    db: &mut impl DbAccess,
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

/// The user's totals for every track they listened to.
pub fn totals(db: &impl DbAccess, user_db_id: DbId) -> anyhow::Result<HashMap<DbId, ListenTotal>> {
    let Some(node) = totals_node(db, user_db_id)? else {
        return Ok(HashMap::new());
    };
    db.exec(
        QueryBuilder::select()
            .search()
            .from(node)
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(1))
            .query(),
    )?
    .elements
    .iter()
    .map(|edge| Ok((edge.to, stored_total(edge)?)))
    .collect()
}

/// The tracks the user has listened to at least once.
pub fn listened_track_ids(db: &impl DbAccess, user_db_id: DbId) -> anyhow::Result<HashSet<DbId>> {
    let Some(node) = totals_node(db, user_db_id)? else {
        return Ok(HashSet::new());
    };
    Ok(db
        .exec(
            QueryBuilder::search()
                .from(node)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .query(),
        )?
        .elements
        .into_iter()
        .map(|edge| edge.to)
        .collect())
}

/// The user's total for one track, without reading their other totals.
pub fn total(
    db: &impl DbAccess,
    user_db_id: DbId,
    track_db_id: DbId,
) -> anyhow::Result<Option<ListenTotal>> {
    let Some(node) = totals_node(db, user_db_id)? else {
        return Ok(None);
    };
    match total_edge(db, node, track_db_id)? {
        Some(edge) => read_total(db, edge).map(Some),
        None => Ok(None),
    }
}

/// Moves every user's total of `from` onto `to`. A user with a total on both keeps their sum
/// and the later last play.
pub fn retarget(db: &mut impl DbAccess, from: DbId, to: DbId) -> anyhow::Result<()> {
    let moved = inbound_totals(db, from)?;
    if moved.is_empty() {
        return Ok(());
    }
    let kept = inbound_totals(db, to)?
        .into_iter()
        .map(|(edge, total)| (edge.from, (edge.id, total)))
        .collect::<HashMap<_, _>>();
    for (edge, moved) in &moved {
        match kept.get(&edge.from) {
            Some((kept_edge, kept)) => write_total(
                db,
                *kept_edge,
                ListenTotal {
                    count: kept.count.saturating_add(moved.count),
                    last_played_at_ms: kept.last_played_at_ms.max(moved.last_played_at_ms),
                },
            )?,
            None => insert_total(db, edge.from, to, *moved)?,
        }
    }
    db.exec_mut(
        QueryBuilder::remove()
            .ids(moved.iter().map(|(edge, _)| edge.id).collect::<Vec<_>>())
            .query(),
    )?;
    Ok(())
}

/// The user's listens.
pub(crate) fn ids_for_user(db: &impl DbAccess, user_db_id: DbId) -> anyhow::Result<Vec<DbId>> {
    super::graph::inbound_neighbor_ids(db, user_db_id, "Listen")
}

pub(crate) fn remove_totals_for_user(
    db: &mut impl DbAccess,
    user_db_id: DbId,
) -> anyhow::Result<()> {
    if let Some(node) = totals_node(db, user_db_id)? {
        db.exec_mut(QueryBuilder::remove().ids(node).query())?;
    }
    Ok(())
}

fn count(
    db: &mut impl DbAccess,
    user_db_id: DbId,
    track_db_id: DbId,
    started_at_ms: u64,
) -> anyhow::Result<()> {
    let first = ListenTotal {
        count: 1,
        last_played_at_ms: started_at_ms,
    };
    let Some(node) = totals_node(db, user_db_id)? else {
        let node = db
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values([[("db_element_id", TOTALS_KIND).into()]])
                    .query(),
            )?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(user_db_id)
                .to(node)
                .query(),
        )?;
        return insert_total(db, node, track_db_id, first);
    };
    let Some(edge) = total_edge(db, node, track_db_id)? else {
        return insert_total(db, node, track_db_id, first);
    };
    let total = read_total(db, edge)?;
    write_total(
        db,
        edge,
        ListenTotal {
            count: total.count.saturating_add(1),
            // Submitted plays arrive out of order.
            last_played_at_ms: total.last_played_at_ms.max(started_at_ms),
        },
    )
}

fn totals_node(db: &impl DbAccess, user_db_id: DbId) -> anyhow::Result<Option<DbId>> {
    Ok(super::graph::neighbor_ids(db, user_db_id, TOTALS_KIND)?
        .into_iter()
        .next())
}

/// The totals node's edge to the track, found among the track's inbound edges, which include
/// every listen of it. Measured at 3-6 µs for a typical track and about 450 µs at 10,000 plays,
/// against 0.25-2.2 ms to scan a totals node of 5,000-50,000 edges.
fn total_edge(db: &impl DbAccess, node: DbId, track_db_id: DbId) -> anyhow::Result<Option<DbId>> {
    Ok(db
        .exec(
            QueryBuilder::search()
                .to(track_db_id)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .query(),
        )?
        .elements
        .into_iter()
        .find(|edge| edge.from == node)
        .map(|edge| edge.id))
}

fn inbound_totals(
    db: &impl DbAccess,
    track_db_id: DbId,
) -> anyhow::Result<Vec<(DbElement, ListenTotal)>> {
    Ok(db
        .exec(
            QueryBuilder::select()
                .search()
                .to(track_db_id)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .query(),
        )?
        .elements
        .into_iter()
        .filter_map(|edge| {
            let total = parse_total(&edge)?;
            Some((edge, total))
        })
        .collect())
}

fn read_total(db: &impl DbAccess, edge: DbId) -> anyhow::Result<ListenTotal> {
    let result = db.exec(QueryBuilder::select().ids(edge).query())?;
    match result.elements.first() {
        Some(edge) => stored_total(edge),
        None => anyhow::bail!("listen total {} is missing", edge.0),
    }
}

/// The total a totals node's edge holds.
fn stored_total(edge: &DbElement) -> anyhow::Result<ListenTotal> {
    parse_total(edge)
        .ok_or_else(|| anyhow::anyhow!("listen total {} holds no count or last play", edge.id.0))
}

fn parse_total(edge: &DbElement) -> Option<ListenTotal> {
    let mut count = None;
    let mut last_played_at_ms = None;
    for kv in &edge.values {
        match (&kv.key, &kv.value) {
            (DbValue::String(key), DbValue::U64(value)) if key == COUNT_KEY => count = Some(*value),
            (DbValue::String(key), DbValue::U64(value)) if key == LAST_PLAYED_KEY => {
                last_played_at_ms = Some(*value);
            }
            _ => {}
        }
    }
    Some(ListenTotal {
        count: count?,
        last_played_at_ms: last_played_at_ms?,
    })
}

fn total_values(total: ListenTotal) -> [agdb::DbKeyValue; 2] {
    [
        (COUNT_KEY, total.count).into(),
        (LAST_PLAYED_KEY, total.last_played_at_ms).into(),
    ]
}

fn insert_total(
    db: &mut impl DbAccess,
    node: DbId,
    track_db_id: DbId,
    total: ListenTotal,
) -> anyhow::Result<()> {
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(node)
            .to(track_db_id)
            .values_uniform(total_values(total))
            .query(),
    )?;
    Ok(())
}

fn write_total(db: &mut impl DbAccess, edge: DbId, total: ListenTotal) -> anyhow::Result<()> {
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform(total_values(total))
            .ids(edge)
            .query(),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::{
        insert_listen,
        insert_track,
    };
    use crate::nanoid;
    use crate::test_db::{
        insert_user,
        new_test_db,
    };
    use agdb::DbAny;

    fn listen(play_id: &str, started_at_ms: u64, played_ms: Option<u64>) -> Listen {
        Listen {
            db_id: None,
            id: nanoid(),
            play_id: play_id.to_string(),
            started_at_ms,
            played_ms,
            created_at_ms: started_at_ms,
        }
    }

    fn insert_play(
        db: &mut DbAny,
        user_db_id: DbId,
        track_db_id: DbId,
        play_id: &str,
        started_at_ms: u64,
    ) -> anyhow::Result<DbId> {
        let listen = listen(play_id, started_at_ms, Some(180_000));
        db.transaction_mut(|t| insert(t, &listen, track_db_id, user_db_id))
    }

    fn of(count: u64, last_played_at_ms: u64) -> ListenTotal {
        ListenTotal {
            count,
            last_played_at_ms,
        }
    }

    #[test]
    fn find_by_play_id_matches_only_the_users_listen() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = insert_user(&mut db, "listener")?;
        let other_user = insert_user(&mut db, "other")?;
        let track = insert_track(&mut db, "Track")?;

        let listen = insert_play(&mut db, user, track, "play-shared-0001", 1_000)?;
        let other_listen = insert_play(&mut db, other_user, track, "play-shared-0001", 2_000)?;

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
        let user = insert_user(&mut db, "listener")?;
        let track = insert_track(&mut db, "Track")?;
        let listen = insert_play(&mut db, user, track, "play-stored-00001", 7)?;

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
        let user = insert_user(&mut db, "listener")?;
        let track = insert_track(&mut db, "Track")?;
        let listen = listen("play-untimed-0001", 7, None);
        let listen_db_id = db.transaction_mut(|t| insert(t, &listen, track, user))?;

        assert!(
            !crate::test_db::stored_keys(&db, listen_db_id)?.contains("played_ms"),
            "no played_ms key"
        );
        let stored = get_by_play_id(&db, user, "play-untimed-0001")?.expect("listen exists");
        assert_eq!(stored.played_ms, None);

        set_played_ms(&mut db, listen_db_id, 40_000)?;
        let stored = get_by_play_id(&db, user, "play-untimed-0001")?.expect("listen exists");
        assert_eq!(stored.played_ms, Some(40_000));
        Ok(())
    }

    #[test]
    fn a_listen_leads_only_to_its_track_and_user() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = insert_user(&mut db, "listener")?;
        let track = insert_track(&mut db, "Track")?;
        let listen = insert_listen(&mut db, user, track, 1_000)?;

        assert_eq!(track_of(&db, listen)?, Some(track));
        assert_eq!(ids_for_user(&db, user)?, vec![listen]);
        assert_eq!(
            db.exec(QueryBuilder::select().edge_count().ids(listen).query())?
                .elements[0]
                .values[0]
                .value
                .to_u64()?,
            2,
            "no root holds a listen"
        );
        Ok(())
    }

    #[test]
    fn inserting_a_listen_leaves_genre_display_covers_alone() -> anyhow::Result<()> {
        use crate::covers::display::{
            DisplayCoverScope,
            DisplayCoverTargetKind,
            get_profile,
        };

        let mut db = new_test_db()?;
        let user = insert_user(&mut db, "listener")?;
        let track = insert_track(&mut db, "Track")?;
        let release = crate::fixtures::insert_release(&mut db, "Release")?;
        crate::fixtures::link_track(&mut db, release, track)?;
        let genre_db_id = crate::genres::resolve_by_name(&mut db, "Rock")?;
        crate::genres::link_to_release(&mut db, genre_db_id, release)?;
        let genre = crate::genres::get_by_id(&db, genre_db_id)?.expect("genre exists");
        let instance_profile = |db: &DbAny| {
            get_profile(
                db,
                DisplayCoverScope::Instance,
                DisplayCoverTargetKind::Genre,
                None,
                &genre.id,
            )
        };

        insert_play(&mut db, user, track, "play-stored-00001", 1_000)?;
        assert_eq!(total(&db, user, track)?, Some(of(1, 1_000)));
        assert!(instance_profile(&db)?.is_none());

        insert_listen(&mut db, user, track, 2_000)?;
        let (_, profile) = instance_profile(&db)?.expect("the fixture counts its listen");
        assert_eq!(profile.instance_total_listens, 1);
        Ok(())
    }

    #[test]
    fn totals_count_each_users_listens_by_track() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = insert_user(&mut db, "listener")?;
        let other_user = insert_user(&mut db, "other")?;
        let silent = insert_user(&mut db, "silent")?;
        let one = insert_track(&mut db, "One")?;
        let two = insert_track(&mut db, "Two")?;
        let unplayed = insert_track(&mut db, "Unplayed")?;

        insert_listen(&mut db, user, one, 1_000)?;
        insert_listen(&mut db, user, one, 3_000)?;
        insert_listen(&mut db, user, two, 2_000)?;
        insert_listen(&mut db, other_user, one, 9_000)?;

        assert_eq!(
            totals(&db, user)?,
            HashMap::from([(one, of(2, 3_000)), (two, of(1, 2_000))])
        );
        assert_eq!(
            totals(&db, other_user)?,
            HashMap::from([(one, of(1, 9_000))])
        );
        assert!(totals(&db, silent)?.is_empty());
        assert_eq!(listened_track_ids(&db, user)?, HashSet::from([one, two]));
        assert!(listened_track_ids(&db, silent)?.is_empty());
        assert_eq!(total(&db, user, one)?, Some(of(2, 3_000)));
        assert_eq!(total(&db, other_user, two)?, None);
        assert_eq!(total(&db, user, unplayed)?, None);
        assert_eq!(total(&db, silent, one)?, None);
        Ok(())
    }

    #[test]
    fn a_listen_that_started_earlier_keeps_the_later_last_play() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = insert_user(&mut db, "listener")?;
        let track = insert_track(&mut db, "Track")?;

        insert_listen(&mut db, user, track, 5_000)?;
        insert_listen(&mut db, user, track, 2_000)?;
        assert_eq!(total(&db, user, track)?, Some(of(2, 5_000)));
        insert_listen(&mut db, user, track, 8_000)?;
        assert_eq!(total(&db, user, track)?, Some(of(3, 8_000)));
        Ok(())
    }

    #[test]
    fn a_first_listen_at_zero_is_a_last_play() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = insert_user(&mut db, "listener")?;
        let track = insert_track(&mut db, "Track")?;

        insert_listen(&mut db, user, track, 0)?;

        assert_eq!(totals(&db, user)?, HashMap::from([(track, of(1, 0))]));
        Ok(())
    }

    #[test]
    fn a_count_at_the_limit_stays_there() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = insert_user(&mut db, "listener")?;
        let track = insert_track(&mut db, "Track")?;
        insert_listen(&mut db, user, track, 1_000)?;
        let node = totals_node(&db, user)?.expect("totals node");
        let edge = total_edge(&db, node, track)?.expect("total edge");
        write_total(&mut db, edge, of(u64::MAX, 1_000))?;

        insert_listen(&mut db, user, track, 2_000)?;

        assert_eq!(total(&db, user, track)?, Some(of(u64::MAX, 2_000)));
        Ok(())
    }

    #[test]
    fn a_total_without_its_values_fails_every_read_of_it() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = insert_user(&mut db, "listener")?;
        let track = insert_track(&mut db, "Track")?;
        insert_listen(&mut db, user, track, 1_000)?;
        let node = totals_node(&db, user)?.expect("totals node");
        let edge = total_edge(&db, node, track)?.expect("total edge");
        db.exec_mut(
            QueryBuilder::remove()
                .values([LAST_PLAYED_KEY])
                .ids(edge)
                .query(),
        )?;

        assert!(totals(&db, user).is_err());
        assert!(total(&db, user, track).is_err());
        Ok(())
    }

    #[test]
    fn totals_read_nothing_past_their_edges() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = insert_user(&mut db, "listener")?;
        let track = insert_track(&mut db, "Track")?;
        let listen = insert_listen(&mut db, user, track, 1_000)?;
        // A second user's totals and a library reach the same track.
        let other_user = insert_user(&mut db, "other")?;
        insert_listen(&mut db, other_user, track, 2_000)?;
        crate::test_db::hang_in_users_library(&mut db, user, listen)?;

        assert_eq!(totals(&db, user)?, HashMap::from([(track, of(1, 1_000))]));
        assert_eq!(listened_track_ids(&db, user)?, HashSet::from([track]));
        assert_eq!(ids_for_user(&db, user)?, vec![listen]);
        Ok(())
    }

    #[test]
    fn retargeting_moves_totals_and_sums_a_user_who_has_both() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let both = insert_user(&mut db, "both")?;
        let from_only = insert_user(&mut db, "from-only")?;
        let to_only = insert_user(&mut db, "to-only")?;
        let from = insert_track(&mut db, "From")?;
        let to = insert_track(&mut db, "To")?;
        let other = insert_track(&mut db, "Other")?;
        insert_listen(&mut db, both, from, 9_000)?;
        insert_listen(&mut db, both, from, 4_000)?;
        insert_listen(&mut db, both, to, 6_000)?;
        insert_listen(&mut db, both, other, 1_000)?;
        insert_listen(&mut db, from_only, from, 0)?;
        insert_listen(&mut db, to_only, to, 3_000)?;

        db.transaction_mut(|t| retarget(t, from, to))?;

        assert_eq!(
            totals(&db, both)?,
            HashMap::from([(to, of(3, 9_000)), (other, of(1, 1_000))])
        );
        assert_eq!(totals(&db, from_only)?, HashMap::from([(to, of(1, 0))]));
        assert_eq!(totals(&db, to_only)?, HashMap::from([(to, of(1, 3_000))]));

        db.transaction_mut(|t| retarget(t, from, to))?;
        assert_eq!(
            totals(&db, both)?[&to],
            of(3, 9_000),
            "nothing left to move"
        );
        Ok(())
    }

    #[test]
    fn a_removed_track_takes_its_totals_and_leaves_its_listens() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = insert_user(&mut db, "listener")?;
        let gone = insert_track(&mut db, "Gone")?;
        let kept = insert_track(&mut db, "Kept")?;
        let listen = insert_play(&mut db, user, gone, "play-of-gone-0001", 1_000)?;
        insert_listen(&mut db, user, kept, 2_000)?;

        db.exec_mut(QueryBuilder::remove().ids(gone).query())?;

        assert_eq!(totals(&db, user)?, HashMap::from([(kept, of(1, 2_000))]));
        assert_eq!(
            find_by_play_id(&db, user, "play-of-gone-0001")?,
            Some(listen)
        );
        assert_eq!(track_of(&db, listen)?, None);
        Ok(())
    }
}
