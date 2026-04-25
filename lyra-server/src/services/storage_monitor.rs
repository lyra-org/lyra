// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::atomic::{
    AtomicU64,
    Ordering,
};
use std::{
    collections::HashMap,
    path::{
        Path,
        PathBuf,
    },
    sync::{
        Arc,
        LazyLock,
    },
};

use tokio::sync::Notify;
use tokio::time::{
    Duration,
    sleep,
};

use crate::db::DbAsync;

const GROWTH_THRESHOLD: f64 = 2.0;
const CHECK_INTERVAL: Duration = Duration::from_secs(10);
const MIN_GROWTH_BYTES: u64 = 50 * 1024 * 1024;

static BASELINE_SIZES: LazyLock<std::sync::Mutex<HashMap<PathBuf, Arc<AtomicU64>>>> =
    LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

fn file_size(path: &Path) -> Option<u64> {
    std::fs::metadata(path).ok().map(|m| m.len())
}

fn baseline_slot(path: &Path) -> Arc<AtomicU64> {
    let mut baselines = BASELINE_SIZES
        .lock()
        .expect("storage monitor baseline registry poisoned");
    baselines
        .entry(path.to_path_buf())
        .or_insert_with(|| Arc::new(AtomicU64::new(0)))
        .clone()
}

fn record_baseline_for(path: &Path, baseline: &AtomicU64) {
    if let Some(size) = file_size(path) {
        baseline.store(size, Ordering::Relaxed);
        tracing::debug!(baseline_bytes = size, "recorded storage baseline");
    }
}

pub(crate) fn record_baseline(path: &PathBuf) {
    let baseline = baseline_slot(path);
    record_baseline_for(path, baseline.as_ref());
}

/// Spawn the storage monitor. Notify the returned handle to stop it.
pub(crate) fn spawn(db: DbAsync, db_path: PathBuf) -> Arc<Notify> {
    let baseline = baseline_slot(&db_path);
    record_baseline_for(&db_path, baseline.as_ref());

    let shutdown = Arc::new(Notify::new());
    let shutdown_clone = shutdown.clone();

    tokio::spawn(async move {
        run(db, db_path, baseline, &shutdown_clone).await;
    });

    shutdown
}

async fn run(db: DbAsync, db_path: PathBuf, baseline: Arc<AtomicU64>, shutdown: &Notify) {
    loop {
        tokio::select! {
            _ = sleep(CHECK_INTERVAL) => {}
            _ = shutdown.notified() => {
                tracing::debug!("storage monitor shutting down");
                return;
            }
        }

        let current_size = match file_size(&db_path) {
            Some(size) => size,
            None => continue,
        };

        let baseline_bytes = baseline.load(Ordering::Relaxed);
        if baseline_bytes == 0 {
            baseline.store(current_size, Ordering::Relaxed);
            continue;
        }

        let growth = current_size.saturating_sub(baseline_bytes);
        let ratio = current_size as f64 / baseline_bytes as f64;

        if ratio >= GROWTH_THRESHOLD && growth >= MIN_GROWTH_BYTES {
            tracing::info!(
                current_bytes = current_size,
                baseline_bytes = baseline_bytes,
                ratio = format!("{ratio:.1}x"),
                "storage fragmentation exceeded threshold, optimizing"
            );

            let mut db_write = db.write().await;
            match db_write.optimize_storage() {
                Ok(()) => {
                    drop(db_write);
                    record_baseline_for(&db_path, baseline.as_ref());
                    tracing::info!(
                        new_baseline_bytes = baseline.load(Ordering::Relaxed),
                        "storage optimization complete"
                    );
                }
                Err(err) => {
                    tracing::warn!(error = %err, "background storage optimization failed");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::Ordering,
        time::{
            SystemTime,
            UNIX_EPOCH,
        },
    };

    use super::{
        baseline_slot,
        record_baseline,
    };

    #[test]
    fn record_baseline_tracks_each_path_independently() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "lyra-storage-monitor-{}-{}",
            std::process::id(),
            unique
        ));
        let first = root.join("first.db");
        let second = root.join("second.db");

        fs::create_dir_all(&root).unwrap();
        fs::write(&first, vec![0u8; 3]).unwrap();
        fs::write(&second, vec![0u8; 7]).unwrap();

        record_baseline(&first);
        record_baseline(&second);

        assert_eq!(baseline_slot(&first).load(Ordering::Relaxed), 3);
        assert_eq!(baseline_slot(&second).load(Ordering::Relaxed), 7);

        let _ = fs::remove_dir_all(root);
    }
}
