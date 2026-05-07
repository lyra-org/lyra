// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Periodic upkeep: sweeps expired datastore entries and evicts stale playback
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

use crate::{
    db::{
        self,
        DbAsync,
    },
    services::playback_sessions,
};

const SWEEP_INTERVAL: Duration = Duration::from_secs(60);

pub(crate) fn spawn(db: DbAsync) -> Arc<Notify> {
    let shutdown = Arc::new(Notify::new());
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        run(db, &shutdown_clone).await;
    });
    shutdown
}

async fn run(db: DbAsync, shutdown: &Notify) {
    loop {
        tokio::select! {
            _ = sleep(SWEEP_INTERVAL) => {}
            _ = shutdown.notified() => {
                tracing::info!("maintenance sweeper shutting down");
                return;
            }
        }

        let Some(now_ms) = now_ms() else { continue };

        sweep_expired_datastore_entries(&db, now_ms).await;
        sweep_stale_playback_sessions(&db, now_ms).await;
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

fn now_ms() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|d| d.as_millis() as u64)
}
