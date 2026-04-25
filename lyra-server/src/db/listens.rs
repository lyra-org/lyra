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

    for track_id in unique_ids {
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
