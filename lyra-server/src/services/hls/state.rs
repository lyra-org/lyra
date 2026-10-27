// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use argon2::password_hash::rand_core::{
    OsRng,
    RngCore,
};
use base64::{
    Engine,
    alphabet,
    engine::{
        GeneralPurpose,
        general_purpose,
    },
};
use lyra_ffmpeg::{
    FfmpegContext,
    FfmpegHandle,
};
use std::{
    collections::{
        HashMap,
        HashSet,
    },
    io::ErrorKind,
    path::{
        Path as FsPath,
        PathBuf,
    },
    sync::{
        Arc,
        LazyLock,
    },
    time::{
        Duration,
        Instant,
    },
};
use tokio::sync::{
    Mutex,
    Notify,
    RwLock,
};

use super::HlsError;
use super::codec::{
    HlsOutputConfig,
    build_hls_output,
};
use crate::services::playback_sources::SourceVersion;

pub(crate) const HLS_JOB_TEMP_DIR_PREFIX: &str = "lyra-hls-job-";

type HlsSessions = Arc<RwLock<HashMap<String, HlsSession>>>;
type HlsJobs = Arc<RwLock<HashMap<HlsJobKey, HlsJob>>>;
type HlsJobWaiters = Arc<Mutex<HashMap<HlsJobKey, Arc<Notify>>>>;

pub(crate) static HLS_SESSIONS: LazyLock<HlsSessions> =
    LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));
pub(crate) static HLS_JOBS: LazyLock<HlsJobs> =
    LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));
pub(crate) static HLS_JOB_CREATING: LazyLock<HlsJobWaiters> =
    LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));
#[derive(Clone)]
pub(crate) struct HlsSession {
    pub(crate) playlist_segment_count: u64,
    pub(crate) job_key: HlsJobKey,
    pub(crate) last_access: Instant,
    pub(crate) holder: HlsSessionHolder,
}

/// Who an HLS session was created for. Segments are only served while they still may be.
#[derive(Clone, Debug)]
pub(crate) enum HlsSessionHolder {
    Principal(crate::services::auth::Principal),
    MediaToken(crate::services::auth::media_tokens::MediaTokenHolder),
    /// A public plugin route, which serves anyone.
    Public,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct HlsJobKey {
    track_public_id: String,
    source: SourceVersion,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    output: HlsOutputConfig,
}

pub(crate) struct HlsJob {
    pub(crate) dir_path: PathBuf,
    pub(crate) playlist_path: PathBuf,
    pub(crate) transcode_handle: Arc<Mutex<Option<FfmpegHandle>>>,
    pub(crate) session_ids: HashSet<String>,
    pub(crate) idle_since: Option<Instant>,
}

/// How often a running HLS transcode is checked for having stopped, to release its permit.
const TRANSCODE_PERMIT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Holds `permit` until the transcode in `handle` stops or is taken away, so an HLS job that is
/// only serving finished segments no longer counts against the transcoding limit.
async fn release_permit_when_stopped(
    permit: crate::services::transcoding::capacity::TranscodePermit,
    handle: std::sync::Weak<Mutex<Option<FfmpegHandle>>>,
) {
    loop {
        tokio::time::sleep(TRANSCODE_PERMIT_POLL_INTERVAL).await;
        let Some(handle) = handle.upgrade() else {
            break;
        };
        if handle
            .lock()
            .await
            .as_ref()
            .is_none_or(FfmpegHandle::is_finished)
        {
            break;
        }
    }
    drop(permit);
}

pub(crate) fn generate_hls_session_id() -> String {
    let mut bytes = [0u8; 16];
    OsRng.fill_bytes(&mut bytes);

    let base = GeneralPurpose::new(&alphabet::URL_SAFE, general_purpose::NO_PAD);
    base.encode(bytes)
}

impl HlsJobKey {
    pub(crate) fn new(
        track_public_id: String,
        source: SourceVersion,
        start_ms: Option<u64>,
        end_ms: Option<u64>,
        output: HlsOutputConfig,
    ) -> Self {
        Self {
            track_public_id,
            source,
            start_ms,
            end_ms,
            output,
        }
    }

    pub(crate) fn track_public_id(&self) -> &str {
        &self.track_public_id
    }
}

pub(crate) async fn cleanup_hls_dir(dir_path: &FsPath) {
    if let Err(err) = tokio::fs::remove_dir_all(dir_path).await
        && err.kind() != ErrorKind::NotFound
    {
        tracing::warn!(
            path = %dir_path.display(),
            error = %err,
            "failed to clean up HLS temp directory"
        );
    }
}

pub(crate) async fn stop_hls_transcode(transcode_handle: Arc<Mutex<Option<FfmpegHandle>>>) {
    let handle = {
        let mut guard = transcode_handle.lock().await;
        guard.take()
    };

    if let Some(handle) = handle {
        match tokio::task::spawn_blocking(move || {
            handle.abort_with_timeout(crate::services::shutdown::TRANSCODE_ABORT_TIMEOUT)
        })
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                tracing::warn!(
                    error = %err,
                    timeout_ms = crate::services::shutdown::TRANSCODE_ABORT_TIMEOUT.as_millis() as u64,
                    "HLS transcode did not stop cleanly"
                );
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "failed to join HLS transcode stop task"
                );
            }
        }
    }
}

pub(crate) async fn teardown_hls_job(job: HlsJob) {
    stop_hls_transcode(job.transcode_handle).await;
    cleanup_hls_dir(&job.dir_path).await;
}

pub(crate) async fn teardown_hls_jobs_for_tracks(track_ids: &HashSet<String>) {
    let removed = {
        let mut jobs = HLS_JOBS.write().await;
        let keys: Vec<_> = jobs
            .keys()
            .filter(|key| track_ids.contains(key.track_public_id()))
            .cloned()
            .collect();
        keys.into_iter()
            .filter_map(|key| jobs.remove(&key))
            .collect::<Vec<_>>()
    };
    HLS_SESSIONS
        .write()
        .await
        .retain(|_, session| !track_ids.contains(session.job_key.track_public_id()));
    for job in removed {
        teardown_hls_job(job).await;
    }
}

pub(crate) async fn teardown_all_hls_jobs() {
    let jobs = {
        let mut jobs = HLS_JOBS.write().await;
        jobs.drain().map(|(_, job)| job).collect::<Vec<_>>()
    };
    HLS_SESSIONS.write().await.clear();
    let creating_notifies = {
        let mut creating = HLS_JOB_CREATING.lock().await;
        creating
            .drain()
            .map(|(_, notify)| notify)
            .collect::<Vec<_>>()
    };
    for notify in creating_notifies {
        notify.notify_waiters();
    }

    for job in jobs {
        teardown_hls_job(job).await;
    }
}

pub(crate) async fn hls_registry_counts() -> (usize, usize) {
    let job_count = HLS_JOBS.read().await.len();
    let session_count = HLS_SESSIONS.read().await.len();
    (job_count, session_count)
}

async fn create_hls_job(input_path: &str, job_key: &HlsJobKey) -> Result<HlsJob, HlsError> {
    let transcode_permit = crate::services::transcoding::capacity::acquire_shared()
        .await
        .map_err(|_| HlsError::Saturated)?;

    let job_id = generate_hls_session_id();
    let job_dir = std::env::temp_dir().join(format!(
        "{HLS_JOB_TEMP_DIR_PREFIX}{}-{}-{job_id}",
        job_key.track_public_id, job_key.source.source_public_id
    ));
    let playlist_path = job_dir.join("index.m3u8");
    let segment_pattern = job_dir.join(format!(
        "segment-%05d.{}",
        job_key.output.profile.segment_extension
    ));

    tokio::fs::create_dir_all(&job_dir)
        .await
        .map_err(anyhow::Error::from)?;

    let output = build_hls_output(&playlist_path, &segment_pattern, job_key.output);
    let context = FfmpegContext::builder()
        .input(input_path)
        .start_ms(job_key.start_ms)
        .end_ms(job_key.end_ms)
        .output(output)
        .build()
        .map_err(anyhow::Error::from)?;

    let transcode_handle = match context.start() {
        Ok(handle) => handle,
        Err(err) => {
            cleanup_hls_dir(&job_dir).await;
            return Err(anyhow::Error::from(err).into());
        }
    };

    let transcode_handle = Arc::new(Mutex::new(Some(transcode_handle)));
    tokio::spawn(release_permit_when_stopped(
        transcode_permit,
        Arc::downgrade(&transcode_handle),
    ));
    Ok(HlsJob {
        dir_path: job_dir,
        playlist_path,
        transcode_handle,
        session_ids: HashSet::new(),
        idle_since: Some(Instant::now()),
    })
}

/// Marks a key's job as being created. Dropping it clears the marker and wakes the requests
/// waiting on it, however creation ends, so a creator cancelled mid-way, such as by its client
/// disconnecting while waiting for a permit, never leaves them waiting forever.
struct CreationGuard {
    job_key: HlsJobKey,
}

impl Drop for CreationGuard {
    fn drop(&mut self) {
        let notify = match HLS_JOB_CREATING.try_lock() {
            Ok(mut creating) => creating.remove(&self.job_key),
            Err(_) => {
                let job_key = self.job_key.clone();
                if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                    runtime.spawn(async move { finish_hls_job_creation(&job_key).await });
                }
                return;
            }
        };
        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }
}

async fn finish_hls_job_creation(job_key: &HlsJobKey) {
    let notify = {
        let mut creating = HLS_JOB_CREATING.lock().await;
        creating.remove(job_key)
    };
    if let Some(notify) = notify {
        notify.notify_waiters();
    }
}

pub(crate) async fn get_or_create_hls_job(
    job_key: &HlsJobKey,
    input_path: &str,
) -> Result<bool, HlsError> {
    loop {
        if HLS_JOBS.read().await.contains_key(job_key) {
            return Ok(true);
        }

        let (notify, is_creator) = {
            let mut creating = HLS_JOB_CREATING.lock().await;
            if let Some(notify) = creating.get(job_key) {
                (Arc::clone(notify), false)
            } else {
                let notify = Arc::new(Notify::new());
                creating.insert(job_key.clone(), Arc::clone(&notify));
                (notify, true)
            }
        };

        if !is_creator {
            notify.notified().await;
            continue;
        }

        let creation = CreationGuard {
            job_key: job_key.clone(),
        };
        let job = create_hls_job(input_path, job_key).await?;
        // Registering runs to the end even if this request is cancelled, so a started job is
        // either registered or torn down, never dropped with its transcode still running.
        return tokio::spawn(register_hls_job(job_key.clone(), job, creation))
            .await
            .map_err(anyhow::Error::from)?;
    }
}

async fn register_hls_job(
    job_key: HlsJobKey,
    job: HlsJob,
    _creation: CreationGuard,
) -> Result<bool, HlsError> {
    let replaced_job = {
        let db = crate::services::STATE.db.read().await;
        let exists = crate::db::lookup::find_node_id_by_id(&*db, job_key.track_public_id());
        let source_exists =
            crate::db::lookup::find_node_id_by_id(&*db, &job_key.source.source_public_id);
        if !matches!(exists, Ok(Some(_))) || !matches!(source_exists, Ok(Some(_))) {
            drop(db);
            teardown_hls_job(job).await;
            return Err(HlsError::JobNotFound);
        }
        HLS_JOBS.write().await.insert(job_key, job)
    };
    if let Some(replaced_job) = replaced_job {
        teardown_hls_job(replaced_job).await;
    }
    Ok(false)
}

pub(crate) async fn attach_session_to_job(
    session_id: &str,
    playlist_segment_count: u64,
    job_key: HlsJobKey,
    holder: HlsSessionHolder,
) -> Result<PathBuf, HlsError> {
    let playlist_path = {
        let mut jobs = HLS_JOBS.write().await;
        let job = jobs.get_mut(&job_key).ok_or(HlsError::JobNotFound)?;
        job.session_ids.insert(session_id.to_string());
        job.idle_since = None;
        job.playlist_path.clone()
    };

    let mut sessions = HLS_SESSIONS.write().await;
    sessions.insert(
        session_id.to_string(),
        HlsSession {
            playlist_segment_count,
            job_key,
            last_access: Instant::now(),
            holder,
        },
    );

    Ok(playlist_path)
}

pub(crate) fn mark_job_session_detached(job: &mut HlsJob, session_id: &str, now: Instant) {
    if job.session_ids.remove(session_id) && job.session_ids.is_empty() {
        job.idle_since.get_or_insert(now);
    }
}

pub(crate) async fn detach_hls_session_with_timestamp(
    session_id: &str,
    now: Instant,
) -> Option<HlsSession> {
    let detached = {
        let mut sessions = HLS_SESSIONS.write().await;
        sessions.remove(session_id)
    }?;

    let mut jobs = HLS_JOBS.write().await;
    if let Some(job) = jobs.get_mut(&detached.job_key) {
        mark_job_session_detached(job, session_id, now);
    }

    Some(detached)
}

#[cfg(test)]
pub(crate) mod test_helpers {
    use super::*;
    use std::{
        path::PathBuf,
        time::{
            SystemTime,
            UNIX_EPOCH,
        },
    };
    use tokio::sync::Mutex as TokioMutex;

    pub(crate) static HLS_TEST_MUTEX: LazyLock<TokioMutex<()>> =
        LazyLock::new(|| TokioMutex::new(()));

    pub(crate) fn unique_test_dir(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "{prefix}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("valid timestamp")
                .as_nanos()
        ))
    }

    pub(crate) async fn reset_hls_state_for_test() {
        teardown_all_hls_jobs().await;
    }

    pub(crate) fn build_test_job(dir_path: PathBuf, playlist_path: PathBuf) -> HlsJob {
        HlsJob {
            dir_path,
            playlist_path,
            transcode_handle: Arc::new(Mutex::new(None)),
            session_ids: HashSet::new(),
            idle_since: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::codec::HLS_AUDIO_BITRATE_KBPS;
    use super::super::codec::{
        HlsCodecProfile,
        HlsOutputConfig,
    };
    use super::test_helpers::*;
    use super::*;
    use lyra_ffmpeg::AudioCodec;

    #[tokio::test]
    async fn attach_session_to_job_keeps_single_shared_job_for_concurrent_sessions() {
        let _guard = HLS_TEST_MUTEX.lock().await;
        reset_hls_state_for_test().await;

        let test_dir = unique_test_dir("lyra-hls-shared-job-test");
        tokio::fs::create_dir_all(&test_dir)
            .await
            .expect("test dir created");

        let profile = HlsCodecProfile::from_requested(Some(AudioCodec::Aac)).expect("aac profile");
        let job_key = HlsJobKey::new(
            "track-pub-601".to_string(),
            crate::services::playback_sources::SourceVersion::fixed("source-pub-6011"),
            None,
            None,
            HlsOutputConfig::new(profile, Some(HLS_AUDIO_BITRATE_KBPS), None, None, false),
        );
        {
            let mut jobs = HLS_JOBS.write().await;
            jobs.insert(
                job_key.clone(),
                build_test_job(test_dir.clone(), test_dir.join("index.m3u8")),
            );
        }

        let key_a = job_key.clone();
        let key_b = job_key.clone();
        let attach_a = tokio::spawn(async move {
            attach_session_to_job("session-a", 1, key_a, HlsSessionHolder::Public).await
        });
        let attach_b = tokio::spawn(async move {
            attach_session_to_job("session-b", 1, key_b, HlsSessionHolder::Public).await
        });

        let attach_a_result = attach_a.await.expect("session-a task should finish");
        let attach_b_result = attach_b.await.expect("session-b task should finish");
        assert!(
            attach_a_result.is_ok(),
            "session-a should attach to shared job"
        );
        assert!(
            attach_b_result.is_ok(),
            "session-b should attach to shared job"
        );

        let jobs = HLS_JOBS.read().await;
        assert_eq!(jobs.len(), 1, "job registry should still contain one job");
        let shared_job = jobs.get(&job_key).expect("shared job should exist");
        assert_eq!(shared_job.session_ids.len(), 2);
        assert!(shared_job.session_ids.contains("session-a"));
        assert!(shared_job.session_ids.contains("session-b"));
        drop(jobs);

        let sessions = HLS_SESSIONS.read().await;
        assert_eq!(sessions.len(), 2, "two sessions should be tracked");
        assert_eq!(
            sessions
                .get("session-a")
                .expect("session-a should exist")
                .job_key,
            job_key
        );
        drop(sessions);

        reset_hls_state_for_test().await;
    }
}
