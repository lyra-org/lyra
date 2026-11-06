// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Periodic upkeep: reclaims expired datastore entries, deletes the oldest
//! inactive playbacks past the retention target, and evicts stale playback
//! sessions.

use std::{
    sync::Arc,
    time::{
        SystemTime,
        UNIX_EPOCH,
    },
};

use tokio::sync::Notify;
use tokio::time::{
    Duration,
    sleep,
};

use crate::services::playback_sessions;
use lyra_db::DbAsync;

const SWEEP_INTERVAL: Duration = Duration::from_secs(60);
/// Reads already hide expired datastore entries, so their removal only
/// reclaims space.
const DATASTORE_SWEEP_EVERY: u32 = 5;
/// Expired entries removed per write lock, keeping each hold short.
const DATASTORE_SWEEP_CHUNK: u64 = 256;
/// Expired entries removed per run; the rest wait for the next one.
const DATASTORE_SWEEP_MAX_PER_RUN: usize = 4096;

pub(crate) fn spawn(db: DbAsync) -> Arc<Notify> {
    let shutdown = Arc::new(Notify::new());
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        run(db, &shutdown_clone).await;
    });
    shutdown
}

async fn run(db: DbAsync, shutdown: &Notify) {
    let mut runs: u32 = 0;
    let mut datastore_sweep = DatastoreSweep::default();
    loop {
        tokio::select! {
            _ = sleep(SWEEP_INTERVAL) => {}
            _ = shutdown.notified() => {
                tracing::info!("maintenance sweeper shutting down");
                return;
            }
        }

        let Some(now_ms) = now_ms() else { continue };

        runs = runs.wrapping_add(1);
        let sweep_datastore = runs.is_multiple_of(DATASTORE_SWEEP_EVERY);
        // Its own task, so a sweep that panics leaves the next one to run.
        match tokio::spawn(sweep(db.clone(), now_ms, sweep_datastore, datastore_sweep)).await {
            Ok(swept) => datastore_sweep = swept,
            Err(err) => tracing::error!(error = %err, "maintenance sweep failed"),
        }
    }
}

async fn sweep(
    db: DbAsync,
    now_ms: u64,
    sweep_datastore: bool,
    mut datastore_sweep: DatastoreSweep,
) -> DatastoreSweep {
    if sweep_datastore {
        datastore_sweep
            .run(
                &db,
                now_ms,
                DATASTORE_SWEEP_CHUNK,
                DATASTORE_SWEEP_MAX_PER_RUN,
            )
            .await;
    }
    sweep_retained_playbacks(&db, now_ms).await;
    sweep_stale_playback_sessions(&db, now_ms).await;
    datastore_sweep
}

/// Reclaims expired datastore entries a chunk per store in turn, so a backlog
/// in one store doesn't starve the others; each run starts one store further
/// on than the last.
#[derive(Clone, Copy, Default)]
struct DatastoreSweep {
    next_store: usize,
}

impl DatastoreSweep {
    /// Returns the number of entries removed.
    async fn run(&mut self, db: &DbAsync, now_ms: u64, chunk: u64, max_per_run: usize) -> usize {
        let datastore_ids = match lyra_db::datastore::list_ids(&*db.read().await) {
            Ok(ids) => ids,
            Err(err) => {
                tracing::warn!(error = %err, "plugin datastore sweep failed");
                return 0;
            }
        };
        if datastore_ids.is_empty() {
            return 0;
        }

        let start = self.next_store % datastore_ids.len();
        self.next_store = start + 1;
        let mut pending: Vec<_> = datastore_ids[start..]
            .iter()
            .chain(&datastore_ids[..start])
            .copied()
            .collect();
        let mut removed = 0;
        while !pending.is_empty() && removed < max_per_run {
            let mut index = 0;
            while index < pending.len() && removed < max_per_run {
                let limit = chunk.min((max_per_run - removed) as u64);
                // Removes plugin datastore entries only.
                let mut db_write = db.write_outside_catalog().await;
                let result = lyra_db::datastore::remove_expired_entries(
                    &mut db_write,
                    pending[index],
                    now_ms,
                    limit,
                );
                drop(db_write);
                match result {
                    Ok(count) if count as u64 == limit => {
                        removed += count;
                        index += 1;
                    }
                    Ok(count) => {
                        removed += count;
                        pending.remove(index);
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "plugin datastore sweep failed");
                        pending.remove(index);
                    }
                }
            }
        }

        if removed > 0 {
            tracing::info!(removed, "swept expired plugin datastore entries");
        }
        removed
    }
}

async fn sweep_retained_playbacks(db: &DbAsync, now_ms: u64) {
    // Removes playbacks and playback sessions only.
    let mut db_write = db.write_outside_catalog().await;
    let deleted = match crate::services::playbacks::sweep_retained(&mut db_write, now_ms) {
        Ok(deleted) => deleted,
        Err(err) => {
            tracing::warn!(error = %err, "playback retention sweep failed");
            return;
        }
    };
    if deleted.is_empty() {
        return;
    }
    tracing::info!(
        deleted = deleted.len(),
        "deleted playbacks past the retention target"
    );
    // Under the write guard, as every playback deletion settles its handoffs.
    for playback in deleted {
        playback.finish().await;
    }
    drop(db_write);
}

async fn sweep_stale_playback_sessions(db: &DbAsync, now_ms: u64) {
    // Drop the write guard before dispatch to cut contention; the sends
    // themselves are non-blocking.
    let evicted = {
        // Removes playbacks and playback sessions only.
        let mut db_write = db.write_outside_catalog().await;
        if let Err(err) = crate::services::playbacks::expire_all_lapsed(&mut db_write, now_ms) {
            tracing::warn!(error = %err, "reported playback sweep failed");
        }
        match crate::services::playbacks::cleanup_evicted_playbacks(&mut db_write, now_ms) {
            Ok(evicted) => evicted,
            Err(err) => {
                tracing::warn!(error = %err, "playback session sweep failed");
                return;
            }
        }
    };

    if evicted.is_empty() {
        return;
    }
    tracing::info!(evicted = evicted.len(), "swept stale playback sessions");
    playback_sessions::dispatch_evicted_updates(evicted);
}

fn now_ms() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|d| d.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::services::{
        STATE,
        catalog::index::snapshot,
        testing::{
            init_default_test_state,
            runtime_test_lock,
        },
    };

    #[tokio::test]
    async fn an_idle_sweep_keeps_the_catalog_index() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        lyra_db::fixtures::insert_release(&mut *STATE.db.write().await, "Kept")?;
        let before = snapshot(&*STATE.db.read().await)?;

        let db = STATE.db.get();
        let now = now_ms().expect("a clock");
        DatastoreSweep::default()
            .run(&db, now, DATASTORE_SWEEP_CHUNK, DATASTORE_SWEEP_MAX_PER_RUN)
            .await;
        sweep_retained_playbacks(&db, now).await;
        sweep_stale_playback_sessions(&db, now).await;
        assert!(Arc::ptr_eq(&before, &snapshot(&*STATE.db.read().await)?));

        lyra_db::fixtures::insert_release(&mut *STATE.db.write().await, "Added")?;
        assert!(!Arc::ptr_eq(&before, &snapshot(&*STATE.db.read().await)?));
        Ok(())
    }

    #[tokio::test]
    async fn the_datastore_sweep_removes_expired_entries_of_every_store() -> anyhow::Result<()> {
        use lyra_db::datastore::{
            EntryWrite,
            find,
            get_entry,
            get_or_create,
            upsert_many,
        };

        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let write = |key: &str, expires_at_ms| EntryWrite {
            key: key.to_string(),
            value: "1".to_string(),
            expires_at_ms,
        };
        let store_ids = {
            let mut db = STATE.db.write().await;
            let mut ids = Vec::new();
            for plugin_id in ["first", "second"] {
                let store = get_or_create(&mut db, plugin_id, "cache".to_string())?;
                let id = store.db_id.map(agdb::DbId::from).expect("persisted");
                upsert_many(
                    &mut db,
                    id,
                    vec![write("expired", Some(1)), write("kept", None)],
                )?;
                ids.push(id);
            }
            ids
        };

        DatastoreSweep::default()
            .run(&STATE.db.get(), 1000, 256, 4096)
            .await;

        let db = STATE.db.read().await;
        for id in store_ids {
            assert!(get_entry(&*db, id, "expired", 0)?.is_none());
            assert!(get_entry(&*db, id, "kept", 0)?.is_some());
        }
        assert!(find(&db, "first", "cache")?.is_some());
        Ok(())
    }

    /// A standalone database holding stores with `expired[i]` expired entries
    /// each, keyed `e0`, `e1`, ..., in `list_ids` order.
    fn stores_with_expired(expired: &[usize]) -> anyhow::Result<(DbAsync, Vec<agdb::DbId>)> {
        use lyra_db::datastore::{
            EntryWrite,
            get_or_create,
            list_ids,
            upsert_many,
        };

        let mut db = lyra_db::test_db::new_test_db()?;
        for index in 0..expired.len() {
            get_or_create(&mut db, "demo", format!("store-{index}"))?;
        }
        let ids = list_ids(&db)?;
        for (id, count) in ids.iter().zip(expired) {
            let writes = (0..*count)
                .map(|key| EntryWrite {
                    key: format!("e{key}"),
                    value: "1".to_string(),
                    expires_at_ms: Some(1),
                })
                .collect();
            upsert_many(&mut db, *id, writes)?;
        }
        Ok((DbAsync::new(db), ids))
    }

    async fn remaining(db: &DbAsync, id: agdb::DbId, keys: usize) -> anyhow::Result<usize> {
        let db = db.read().await;
        let mut count = 0;
        for key in 0..keys {
            if lyra_db::datastore::get_entry(&db, id, &format!("e{key}"), 0)?.is_some() {
                count += 1;
            }
        }
        Ok(count)
    }

    #[tokio::test]
    async fn the_datastore_sweep_spreads_its_budget_across_stores() -> anyhow::Result<()> {
        let (db, ids) = stores_with_expired(&[10, 10, 2])?;

        let removed = DatastoreSweep::default().run(&db, 1000, 2, 8).await;

        assert_eq!(removed, 8);
        assert_eq!(remaining(&db, ids[0], 10).await?, 6);
        assert_eq!(remaining(&db, ids[1], 10).await?, 8);
        assert_eq!(remaining(&db, ids[2], 2).await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn each_datastore_sweep_starts_at_the_next_store() -> anyhow::Result<()> {
        let (db, ids) = stores_with_expired(&[5, 5, 5])?;
        let mut sweep = DatastoreSweep::default();

        for expected in [[4, 5, 5], [4, 4, 5], [4, 4, 4], [3, 4, 4]] {
            assert_eq!(sweep.run(&db, 1000, 1, 1).await, 1);
            let mut left = [0; 3];
            for (slot, id) in left.iter_mut().zip(&ids) {
                *slot = remaining(&db, *id, 5).await?;
            }
            assert_eq!(left, expected);
        }
        Ok(())
    }
}
