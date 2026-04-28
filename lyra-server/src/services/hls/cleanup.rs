// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashSet,
    io::ErrorKind,
    path::{
        Path as FsPath,
        PathBuf,
    },
    sync::{
        LazyLock,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
    time::{
        Duration,
        Instant,
    },
};
use tokio::{
    sync::Mutex,
    time::sleep,
};

use crate::STATE;

use super::state::{
    HLS_JOB_TEMP_DIR_PREFIX,
    HLS_JOBS,
    HLS_SESSIONS,
    HlsJobKey,
    cleanup_hls_dir,
    detach_hls_session_with_timestamp,
    hls_registry_counts,
    teardown_hls_job,
};

const HLS_SESSION_TTL: Duration = Duration::from_secs(15 * 60);
const HLS_JOB_IDLE_GRACE: Duration = Duration::from_secs(30);
const HLS_CLEANUP_INTERVAL: Duration = Duration::from_secs(30);
const HLS_CLEANUP_STARTUP_PURGE_DEFAULT: bool = true;

static HLS_CLEANUP_WORKER_STARTED: AtomicBool = AtomicBool::new(false);
static HLS_CLEANUP_WORKER_START_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

pub(crate) fn reset_cleanup_worker_state() {
    HLS_CLEANUP_WORKER_STARTED.store(false, Ordering::Release);
}

#[derive(Clone)]
struct HlsJobDiskSnapshot {
    job_key: HlsJobKey,
    dir_path: PathBuf,
    idle_since: Option<Instant>,
    has_active_sessions: bool,
}

#[derive(Clone)]
pub(crate) struct InactiveHlsJobUsage {
    pub(crate) job_key: HlsJobKey,
    pub(crate) idle_since: Instant,
    pub(crate) size_bytes: u64,
}

fn hls_temp_disk_budget_bytes() -> Option<u64> {
    STATE
        .config
        .get()
        .hls
        .temp_disk_budget_bytes
        .filter(|budget| *budget > 0)
}

fn hls_cleanup_startup_purge_enabled() -> bool {
    STATE
        .config
        .get()
        .hls
        .cleanup_startup_purge
        .unwrap_or(HLS_CLEANUP_STARTUP_PURGE_DEFAULT)
}

fn is_hls_temp_job_dir_name(name: &str) -> bool {
    name.starts_with(HLS_JOB_TEMP_DIR_PREFIX) && name.len() > HLS_JOB_TEMP_DIR_PREFIX.len()
}

/// Collects `(path, metadata)` pairs from a single directory level, logging
/// and skipping entries that vanish or fail.  Shared by orphan-purge and
/// disk-budget accounting so the read_dir/next_entry error handling lives in
/// one place.
async fn collect_dir_entries(dir: &FsPath, context: &str) -> Vec<(PathBuf, std::fs::Metadata)> {
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Vec::new(),
        Err(err) => {
            tracing::warn!(
                path = %dir.display(),
                error = %err,
                "failed to read directory for {context}"
            );
            return Vec::new();
        }
    };

    let mut result = Vec::new();
    loop {
        match entries.next_entry().await {
            Ok(Some(entry)) => {
                let entry_path = entry.path();
                match entry.metadata().await {
                    Ok(metadata) => result.push((entry_path, metadata)),
                    Err(err) if err.kind() == ErrorKind::NotFound => {}
                    Err(err) => {
                        tracing::warn!(
                            path = %entry_path.display(),
                            error = %err,
                            "failed to read entry metadata for {context}"
                        );
                    }
                }
            }
            Ok(None) => break,
            Err(err) => {
                tracing::warn!(
                    path = %dir.display(),
                    error = %err,
                    "failed to iterate directory for {context}"
                );
                break;
            }
        }
    }

    result
}

pub(crate) async fn purge_orphaned_hls_temp_dirs(
    temp_dir: &FsPath,
    active_job_dirs: &HashSet<PathBuf>,
) -> usize {
    let mut purged_dirs = 0usize;

    for (entry_path, metadata) in collect_dir_entries(temp_dir, "HLS startup purge").await {
        let Some(entry_name) = entry_path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !is_hls_temp_job_dir_name(entry_name) || active_job_dirs.contains(&entry_path) {
            continue;
        }
        if metadata.is_dir() {
            cleanup_hls_dir(&entry_path).await;
            purged_dirs = purged_dirs.saturating_add(1);
        }
    }

    purged_dirs
}

async fn cleanup_hls_startup_orphaned_dirs() {
    if !hls_cleanup_startup_purge_enabled() {
        return;
    }

    let active_job_dirs = {
        let jobs = HLS_JOBS.read().await;
        jobs.values()
            .map(|job| job.dir_path.clone())
            .collect::<HashSet<_>>()
    };

    let temp_dir = std::env::temp_dir();
    let purged_dirs = purge_orphaned_hls_temp_dirs(&temp_dir, &active_job_dirs).await;
    if purged_dirs > 0 {
        tracing::info!(
            purged_dirs,
            path = %temp_dir.display(),
            "purged orphaned HLS temp outputs on startup"
        );
    }
}

async fn hls_dir_size_bytes(dir_path: &FsPath) -> u64 {
    let mut total_bytes = 0u64;
    let mut stack = vec![dir_path.to_path_buf()];

    while let Some(path) = stack.pop() {
        for (entry_path, metadata) in collect_dir_entries(&path, "HLS disk budget accounting").await
        {
            if metadata.is_dir() {
                stack.push(entry_path);
            } else if metadata.is_file() {
                total_bytes = total_bytes.saturating_add(metadata.len());
            }
        }
    }

    total_bytes
}

pub(crate) fn pick_jobs_to_evict_for_budget(
    mut inactive_jobs: Vec<InactiveHlsJobUsage>,
    total_bytes: u64,
    budget_bytes: u64,
) -> Vec<InactiveHlsJobUsage> {
    if total_bytes <= budget_bytes {
        return Vec::new();
    }

    inactive_jobs.sort_by_key(|job| job.idle_since);

    let mut remaining_bytes = total_bytes;
    let mut selected = Vec::new();
    for job in inactive_jobs {
        if remaining_bytes <= budget_bytes {
            break;
        }

        remaining_bytes = remaining_bytes.saturating_sub(job.size_bytes);
        selected.push(job);
    }

    selected
}

pub(crate) async fn enforce_hls_temp_disk_budget_with_limit(budget_bytes: Option<u64>) {
    let Some(budget_bytes) = budget_bytes else {
        return;
    };

    let snapshots = {
        let jobs = HLS_JOBS.read().await;
        jobs.iter()
            .map(|(job_key, job)| HlsJobDiskSnapshot {
                job_key: job_key.clone(),
                dir_path: job.dir_path.clone(),
                idle_since: job.idle_since,
                has_active_sessions: !job.session_ids.is_empty(),
            })
            .collect::<Vec<_>>()
    };

    if snapshots.is_empty() {
        return;
    }

    let mut total_bytes = 0u64;
    let mut inactive_jobs = Vec::new();
    let now = Instant::now();

    for snapshot in snapshots {
        let size_bytes = hls_dir_size_bytes(&snapshot.dir_path).await;
        total_bytes = total_bytes.saturating_add(size_bytes);

        if !snapshot.has_active_sessions {
            inactive_jobs.push(InactiveHlsJobUsage {
                job_key: snapshot.job_key,
                idle_since: snapshot.idle_since.unwrap_or(now),
                size_bytes,
            });
        }
    }

    if total_bytes <= budget_bytes {
        return;
    }

    let eviction_candidates =
        pick_jobs_to_evict_for_budget(inactive_jobs, total_bytes, budget_bytes);
    if eviction_candidates.is_empty() {
        tracing::warn!(
            budget_bytes,
            total_bytes,
            "temp disk budget for HLS exceeded but no inactive jobs are available for eviction"
        );
        return;
    }

    let mut evicted_jobs = Vec::new();
    let mut evicted_bytes = 0u64;

    for candidate in eviction_candidates {
        let removed_job = {
            let mut jobs = HLS_JOBS.write().await;
            let should_remove = jobs
                .get(&candidate.job_key)
                .is_some_and(|job| job.session_ids.is_empty());

            should_remove
                .then(|| jobs.remove(&candidate.job_key))
                .flatten()
        };

        if let Some(job) = removed_job {
            evicted_bytes = evicted_bytes.saturating_add(candidate.size_bytes);
            evicted_jobs.push(job);
        }
    }

    if evicted_jobs.is_empty() {
        tracing::warn!(
            budget_bytes,
            total_bytes,
            "temp disk budget for HLS exceeded but inactive eviction candidates became active"
        );
        return;
    }

    let evicted_job_count = evicted_jobs.len();
    for job in evicted_jobs {
        teardown_hls_job(job).await;
    }

    let remaining_bytes = total_bytes.saturating_sub(evicted_bytes);
    let (active_jobs, active_sessions) = hls_registry_counts().await;
    if remaining_bytes > budget_bytes {
        tracing::warn!(
            budget_bytes,
            total_bytes,
            evicted_bytes,
            evicted_job_count,
            remaining_bytes,
            active_jobs,
            active_sessions,
            "temp disk budget for HLS still exceeded after evicting inactive outputs"
        );
    } else {
        tracing::info!(
            budget_bytes,
            total_bytes,
            evicted_bytes,
            evicted_job_count,
            remaining_bytes,
            active_jobs,
            active_sessions,
            "evicted inactive HLS outputs to enforce temp disk budget"
        );
    }
}

async fn enforce_hls_temp_disk_budget() {
    enforce_hls_temp_disk_budget_with_limit(hls_temp_disk_budget_bytes()).await;
}

async fn cleanup_idle_hls_jobs(now: Instant) {
    let idle_jobs = {
        let mut jobs = HLS_JOBS.write().await;
        let stale_keys = jobs
            .iter()
            .filter_map(|(job_key, job)| {
                if !job.session_ids.is_empty() {
                    return None;
                }

                match job.idle_since {
                    Some(idle_since) if now.duration_since(idle_since) >= HLS_JOB_IDLE_GRACE => {
                        Some(job_key.clone())
                    }
                    _ => None,
                }
            })
            .collect::<Vec<_>>();

        stale_keys
            .into_iter()
            .filter_map(|job_key| jobs.remove(&job_key))
            .collect::<Vec<_>>()
    };

    let mut removed_jobs = 0usize;
    let mut reclaimed_bytes = 0u64;

    for job in idle_jobs {
        reclaimed_bytes = reclaimed_bytes.saturating_add(hls_dir_size_bytes(&job.dir_path).await);
        teardown_hls_job(job).await;
        removed_jobs = removed_jobs.saturating_add(1);
    }

    if removed_jobs > 0 {
        let (active_jobs, active_sessions) = hls_registry_counts().await;
        tracing::info!(
            removed_jobs,
            reclaimed_bytes,
            active_jobs,
            active_sessions,
            "cleaned up idle HLS jobs"
        );
    }
}

pub(crate) async fn cleanup_stale_hls_sessions() {
    let now = Instant::now();
    let stale_session_ids = {
        let sessions = HLS_SESSIONS.read().await;
        sessions
            .iter()
            .filter_map(|(session_id, session)| {
                (now.duration_since(session.last_access) >= HLS_SESSION_TTL)
                    .then_some(session_id.clone())
            })
            .collect::<Vec<_>>()
    };

    for session_id in stale_session_ids {
        let _ = detach_hls_session_with_timestamp(&session_id, now).await;
    }

    cleanup_idle_hls_jobs(now).await;
    enforce_hls_temp_disk_budget().await;
}

pub(crate) async fn ensure_hls_cleanup_worker_started() {
    if HLS_CLEANUP_WORKER_STARTED.load(Ordering::Acquire) {
        return;
    }

    let _start_guard = HLS_CLEANUP_WORKER_START_LOCK.lock().await;
    if HLS_CLEANUP_WORKER_STARTED.load(Ordering::Acquire) {
        return;
    }

    cleanup_hls_startup_orphaned_dirs().await;

    tokio::spawn(async {
        loop {
            cleanup_stale_hls_sessions().await;
            sleep(HLS_CLEANUP_INTERVAL).await;
        }
    });

    HLS_CLEANUP_WORKER_STARTED.store(true, Ordering::Release);
}

#[cfg(test)]
mod tests {
    use super::super::codec::{
        HLS_AUDIO_BITRATE_KBPS,
        HlsCodecProfile,
        HlsOutputConfig,
    };
    use super::super::state::{
        HlsJobKey,
        test_helpers::*,
    };
    use super::*;
    use agdb::DbId;
    use lyra_ffmpeg::AudioCodec;
    use std::time::{
        Duration,
        Instant,
        SystemTime,
        UNIX_EPOCH,
    };

    #[test]
    fn pick_jobs_to_evict_for_budget_prefers_oldest_inactive_jobs() {
        let profile = HlsCodecProfile::from_requested(Some(AudioCodec::Aac)).expect("aac profile");

        let oldest_key = HlsJobKey::new(
            DbId(1),
            DbId(1001),
            None,
            None,
            HlsOutputConfig::new(profile, Some(HLS_AUDIO_BITRATE_KBPS), None, None, false),
        );
        let middle_key = HlsJobKey::new(
            DbId(2),
            DbId(1002),
            None,
            None,
            HlsOutputConfig::new(profile, Some(HLS_AUDIO_BITRATE_KBPS), None, None, false),
        );
        let newest_key = HlsJobKey::new(
            DbId(3),
            DbId(1003),
            None,
            None,
            HlsOutputConfig::new(profile, Some(HLS_AUDIO_BITRATE_KBPS), None, None, false),
        );

        let now = Instant::now();
        let selected = pick_jobs_to_evict_for_budget(
            vec![
                InactiveHlsJobUsage {
                    job_key: newest_key.clone(),
                    idle_since: now - Duration::from_secs(10),
                    size_bytes: 20,
                },
                InactiveHlsJobUsage {
                    job_key: oldest_key.clone(),
                    idle_since: now - Duration::from_secs(45),
                    size_bytes: 45,
                },
                InactiveHlsJobUsage {
                    job_key: middle_key.clone(),
                    idle_since: now - Duration::from_secs(25),
                    size_bytes: 30,
                },
            ],
            95,
            25,
        );

        let selected_keys = selected
            .into_iter()
            .map(|usage| usage.job_key)
            .collect::<Vec<_>>();

        assert_eq!(selected_keys, vec![oldest_key, middle_key]);
    }

    #[tokio::test]
    async fn cleanup_stale_hls_sessions_detaches_expired_session_and_marks_job_idle() {
        let _guard = HLS_TEST_MUTEX.lock().await;
        reset_hls_state_for_test().await;

        let track_db_id = DbId(333);
        let session_id = "stale-session".to_string();
        let test_dir = unique_test_dir("lyra-hls-cleanup-stale-session-test");
        tokio::fs::create_dir_all(&test_dir)
            .await
            .expect("test dir created");

        let profile = HlsCodecProfile::from_requested(Some(AudioCodec::Aac)).expect("aac profile");
        let job_key = HlsJobKey::new(
            track_db_id,
            DbId(3331),
            None,
            None,
            HlsOutputConfig::new(profile, Some(HLS_AUDIO_BITRATE_KBPS), None, None, false),
        );
        let mut job = build_test_job(test_dir.clone(), test_dir.join("index.m3u8"));
        job.session_ids.insert(session_id.clone());
        {
            let mut jobs = HLS_JOBS.write().await;
            jobs.insert(job_key.clone(), job);
        }
        {
            let mut sessions = HLS_SESSIONS.write().await;
            sessions.insert(
                session_id.clone(),
                super::super::state::HlsSession {
                    user_db_id: DbId(7),
                    track_db_id,
                    playlist_segment_count: 1,
                    job_key: job_key.clone(),
                    last_access: Instant::now() - HLS_SESSION_TTL - Duration::from_secs(1),
                },
            );
        }

        cleanup_stale_hls_sessions().await;

        assert!(
            HLS_SESSIONS.read().await.get(&session_id).is_none(),
            "stale session should be removed"
        );
        let jobs = HLS_JOBS.read().await;
        let job_after = jobs
            .get(&job_key)
            .expect("job should remain for idle-grace handling");
        assert!(
            job_after.session_ids.is_empty(),
            "stale session should detach from job"
        );
        assert!(
            job_after.idle_since.is_some(),
            "job should be marked idle after stale detach"
        );
        drop(jobs);

        reset_hls_state_for_test().await;
    }

    #[tokio::test]
    async fn enforce_hls_temp_disk_budget_evicts_oldest_inactive_job() {
        let _guard = HLS_TEST_MUTEX.lock().await;
        reset_hls_state_for_test().await;

        let older_dir = unique_test_dir("lyra-hls-budget-older");
        let newer_dir = unique_test_dir("lyra-hls-budget-newer");
        tokio::fs::create_dir_all(&older_dir)
            .await
            .expect("older dir created");
        tokio::fs::create_dir_all(&newer_dir)
            .await
            .expect("newer dir created");
        tokio::fs::write(older_dir.join("segment-00001.ts"), vec![0u8; 40])
            .await
            .expect("older file written");
        tokio::fs::write(newer_dir.join("segment-00001.ts"), vec![0u8; 20])
            .await
            .expect("newer file written");

        let profile = HlsCodecProfile::from_requested(Some(AudioCodec::Aac)).expect("aac profile");
        let older_key = HlsJobKey::new(
            DbId(901),
            DbId(9011),
            None,
            None,
            HlsOutputConfig::new(profile, Some(HLS_AUDIO_BITRATE_KBPS), None, None, false),
        );
        let newer_key = HlsJobKey::new(
            DbId(902),
            DbId(9021),
            None,
            None,
            HlsOutputConfig::new(profile, Some(HLS_AUDIO_BITRATE_KBPS), None, None, false),
        );

        let mut older_job = build_test_job(older_dir.clone(), older_dir.join("index.m3u8"));
        older_job.idle_since = Some(Instant::now() - Duration::from_secs(50));
        let mut newer_job = build_test_job(newer_dir.clone(), newer_dir.join("index.m3u8"));
        newer_job.idle_since = Some(Instant::now() - Duration::from_secs(10));

        {
            let mut jobs = HLS_JOBS.write().await;
            jobs.insert(older_key.clone(), older_job);
            jobs.insert(newer_key.clone(), newer_job);
        }

        enforce_hls_temp_disk_budget_with_limit(Some(30)).await;

        let jobs = HLS_JOBS.read().await;
        assert!(
            !jobs.contains_key(&older_key),
            "oldest inactive job should be evicted first"
        );
        assert!(
            jobs.contains_key(&newer_key),
            "newer inactive job should remain"
        );
        drop(jobs);

        assert!(tokio::fs::metadata(&older_dir).await.is_err());
        assert!(tokio::fs::metadata(&newer_dir).await.is_ok());

        reset_hls_state_for_test().await;
    }

    #[tokio::test]
    async fn purge_orphaned_hls_temp_dirs_removes_only_managed_inactive_dirs() {
        let test_root = std::env::temp_dir().join(format!(
            "lyra-hls-startup-purge-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("valid timestamp")
                .as_nanos()
        ));
        tokio::fs::create_dir_all(&test_root)
            .await
            .expect("test root created");

        let orphan_dir = test_root.join("lyra-hls-job-1-orphan");
        let active_dir = test_root.join("lyra-hls-job-1-active");
        let unmanaged_dir = test_root.join("not-hls-job-dir");

        tokio::fs::create_dir_all(&orphan_dir)
            .await
            .expect("orphan dir created");
        tokio::fs::create_dir_all(&active_dir)
            .await
            .expect("active dir created");
        tokio::fs::create_dir_all(&unmanaged_dir)
            .await
            .expect("unmanaged dir created");

        tokio::fs::write(orphan_dir.join("segment-00001.ts"), b"x")
            .await
            .expect("orphan file created");
        tokio::fs::write(active_dir.join("segment-00001.ts"), b"x")
            .await
            .expect("active file created");
        tokio::fs::write(unmanaged_dir.join("tmp.txt"), b"x")
            .await
            .expect("unmanaged file created");

        let mut active_job_dirs = std::collections::HashSet::new();
        active_job_dirs.insert(active_dir.clone());

        purge_orphaned_hls_temp_dirs(&test_root, &active_job_dirs).await;

        assert!(tokio::fs::metadata(&orphan_dir).await.is_err());
        assert!(tokio::fs::metadata(&active_dir).await.is_ok());
        assert!(tokio::fs::metadata(&unmanaged_dir).await.is_ok());

        let _ = tokio::fs::remove_dir_all(&test_root).await;
    }
}
