// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Periodic upkeep: sweeps expired datastore entries, evicts stale playback
//! sessions, compacts agdb when fragmentation crosses a threshold.
//! https://github.com/agnesoft/agdb/discussions/1781

use std::{
    path::{
        Path,
        PathBuf,
    },
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

use crate::{
    db::{
        self,
        DbAsync,
    },
    services::playback_sessions,
};

const SWEEP_INTERVAL: Duration = Duration::from_secs(60);
const FRAGMENTATION_RATIO_THRESHOLD: f64 = 1.3;
const MIN_WASTE_BYTES: u64 = 50 * 1024 * 1024;

/// `storage_path` is `None` for in-memory DBs.
pub(crate) fn spawn(db: DbAsync, storage_path: Option<PathBuf>) -> Arc<Notify> {
    let shutdown = Arc::new(Notify::new());
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        run(db, storage_path, &shutdown_clone).await;
    });
    shutdown
}

async fn run(db: DbAsync, storage_path: Option<PathBuf>, shutdown: &Notify) {
    loop {
        tokio::select! {
            _ = sleep(SWEEP_INTERVAL) => {}
            _ = shutdown.notified() => {
                tracing::debug!("maintenance sweeper shutting down");
                return;
            }
        }

        let Some(now_ms) = now_ms() else { continue };

        sweep_expired_datastore_entries(&db, now_ms).await;
        sweep_stale_playback_sessions(&db, now_ms).await;
        if let Some(path) = storage_path.as_deref() {
            check_fragmentation(&db, path).await;
        }
    }
}

async fn sweep_expired_datastore_entries(db: &DbAsync, now_ms: u64) {
    let mut db_write = db.write().await;
    match db::datastore::sweep_expired_entries(&mut db_write, now_ms) {
        Ok(0) => {}
        Ok(removed) => {
            tracing::info!(removed, "swept expired plugin datastore entries");
        }
        Err(err) => {
            tracing::warn!(error = %err, "plugin datastore sweep failed");
        }
    }
}

async fn sweep_stale_playback_sessions(db: &DbAsync, now_ms: u64) {
    // Drop the write guard before dispatch — handlers may re-enter the DB.
    let evicted = {
        let mut db_write = db.write().await;
        match playback_sessions::cleanup_evicted_playbacks(&mut db_write, now_ms) {
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

async fn check_fragmentation(db: &DbAsync, db_path: &Path) {
    let Some(file_bytes) = file_size(db_path) else {
        return;
    };
    let logical_bytes = db.read().await.size();
    if logical_bytes == 0 {
        return;
    }

    let waste_bytes = file_bytes.saturating_sub(logical_bytes);
    if waste_bytes < MIN_WASTE_BYTES {
        return;
    }
    let ratio = file_bytes as f64 / logical_bytes as f64;
    if ratio < FRAGMENTATION_RATIO_THRESHOLD {
        return;
    }

    tracing::info!(
        file_bytes,
        logical_bytes,
        waste_bytes,
        ratio = format!("{ratio:.2}x"),
        "storage fragmentation exceeded threshold, optimizing"
    );

    let mut db_write = db.write().await;
    match db_write.optimize_storage() {
        Ok(()) => {
            let new_logical = db_write.size();
            drop(db_write);
            let new_file_bytes = file_size(db_path).unwrap_or(file_bytes);
            tracing::info!(
                new_file_bytes,
                new_logical_bytes = new_logical,
                reclaimed_bytes = file_bytes.saturating_sub(new_file_bytes),
                "storage optimization complete"
            );
        }
        Err(err) => {
            tracing::warn!(error = %err, "background storage optimization failed");
        }
    }
}

fn file_size(path: &Path) -> Option<u64> {
    std::fs::metadata(path).ok().map(|m| m.len())
}

fn now_ms() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|d| d.as_millis() as u64)
}
