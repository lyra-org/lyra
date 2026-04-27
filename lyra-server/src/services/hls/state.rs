// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::DbId;
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
    time::Instant,
};
use tokio::sync::{
    Mutex,
    Notify,
    OwnedSemaphorePermit,
    RwLock,
    Semaphore,
};

use crate::config::Config;

use super::HlsError;
use super::codec::{
    HLS_AUDIO_BITRATE_KBPS,
    HLS_SEGMENT_TIME_SECONDS,
    HlsCodecProfile,
    build_hls_output,
};

pub(crate) const HLS_JOB_TEMP_DIR_PREFIX: &str = "lyra-hls-job-";

pub(crate) static HLS_SESSIONS: LazyLock<Arc<RwLock<HashMap<String, HlsSession>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));
pub(crate) static HLS_JOBS: LazyLock<Arc<RwLock<HashMap<HlsJobKey, HlsJob>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));
pub(crate) static HLS_JOB_CREATING: LazyLock<Arc<Mutex<HashMap<HlsJobKey, Arc<Notify>>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));
static TRANSCODE_SEMAPHORE: LazyLock<RwLock<Option<Arc<Semaphore>>>> =
    LazyLock::new(|| RwLock::new(None));

pub(crate) async fn refresh_hls_transcode_semaphore(config: &Config) {
    let mut guard = TRANSCODE_SEMAPHORE.write().await;
    *guard = config
        .hls
        .max_concurrent_transcodes
        .filter(|n| *n > 0)
        .map(|n| Arc::new(Semaphore::new(n as usize)));
}

pub(crate) async fn acquire_hls_transcode_permit() -> Result<Option<OwnedSemaphorePermit>, HlsError>
{
    let guard = TRANSCODE_SEMAPHORE.read().await;
    if let Some(semaphore) = guard.as_ref() {
        let permit = Arc::clone(semaphore)
            .acquire_owned()
            .await
            .map_err(|_| HlsError::TranscodeCapacityUnavailable)?;
        Ok(Some(permit))
    } else {
        Ok(None)
    }
}

#[derive(Clone)]
pub(crate) struct HlsSession {
    pub(crate) user_db_id: DbId,
    pub(crate) track_db_id: DbId,
    pub(crate) playlist_segment_count: u64,
    pub(crate) job_key: HlsJobKey,
    pub(crate) last_access: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct HlsJobKey {
    track_db_id: DbId,
    source_db_id: DbId,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    codec: &'static str,
    segment_type: &'static str,
    segment_extension: &'static str,
    init_filename: Option<&'static str>,
    pub(crate) audio_bitrate_kbps: Option<u32>,
    segment_time_seconds: u32,
}

pub(crate) struct HlsJob {
    pub(crate) dir_path: PathBuf,
    pub(crate) playlist_path: PathBuf,
    pub(crate) transcode_handle: Arc<Mutex<Option<FfmpegHandle>>>,
    pub(crate) session_ids: HashSet<String>,
    pub(crate) idle_since: Option<Instant>,
    _transcode_permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

pub(crate) fn generate_hls_session_id() -> String {
    let mut bytes = [0u8; 16];
    OsRng.fill_bytes(&mut bytes);

    let base = GeneralPurpose::new(&alphabet::URL_SAFE, general_purpose::NO_PAD);
    base.encode(bytes)
}

pub(crate) fn hls_job_key(
    track_db_id: DbId,
    source_db_id: DbId,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    profile: HlsCodecProfile,
) -> HlsJobKey {
    HlsJobKey {
        track_db_id,
        source_db_id,
        start_ms,
        end_ms,
        codec: profile.ffmpeg_codec_str,
        segment_type: profile.segment_type,
        segment_extension: profile.segment_extension,
        init_filename: profile.init_filename,
        audio_bitrate_kbps: matches!(profile.codec, lyra_ffmpeg::AudioCodec::Aac)
            .then_some(HLS_AUDIO_BITRATE_KBPS),
        segment_time_seconds: HLS_SEGMENT_TIME_SECONDS,
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
        let _ = tokio::task::spawn_blocking(move || drop(handle)).await;
    }
}

pub(crate) async fn teardown_hls_job(job: HlsJob) {
    stop_hls_transcode(job.transcode_handle).await;
    cleanup_hls_dir(&job.dir_path).await;
}

pub(crate) async fn hls_registry_counts() -> (usize, usize) {
    let job_count = HLS_JOBS.read().await.len();
    let session_count = HLS_SESSIONS.read().await.len();
    (job_count, session_count)
}

async fn create_hls_job(
    input_path: &str,
    job_key: &HlsJobKey,
    profile: HlsCodecProfile,
) -> Result<HlsJob, HlsError> {
    let transcode_permit = acquire_hls_transcode_permit().await?;

    let job_id = generate_hls_session_id();
    let job_dir = std::env::temp_dir().join(format!(
        "{HLS_JOB_TEMP_DIR_PREFIX}{}-{}-{job_id}",
        job_key.track_db_id.0, job_key.source_db_id.0
    ));
    let playlist_path = job_dir.join("index.m3u8");
    let segment_pattern = job_dir.join(format!("segment-%05d.{}", profile.segment_extension));

    tokio::fs::create_dir_all(&job_dir)
        .await
        .map_err(anyhow::Error::from)?;

    let output = build_hls_output(&playlist_path, &segment_pattern, profile);
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

    Ok(HlsJob {
        dir_path: job_dir,
        playlist_path,
        transcode_handle: Arc::new(Mutex::new(Some(transcode_handle))),
        session_ids: HashSet::new(),
        idle_since: Some(Instant::now()),
        _transcode_permit: transcode_permit,
    })
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
    profile: HlsCodecProfile,
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

        let create_result = create_hls_job(input_path, job_key, profile).await;
        let replaced_job = match create_result {
            Ok(job) => {
                let mut jobs = HLS_JOBS.write().await;
                jobs.insert(job_key.clone(), job)
            }
            Err(err) => {
                finish_hls_job_creation(job_key).await;
                return Err(err);
            }
        };

        finish_hls_job_creation(job_key).await;

        if let Some(replaced_job) = replaced_job {
            teardown_hls_job(replaced_job).await;
        }

        return Ok(false);
    }
}

pub(crate) async fn attach_session_to_job(
    session_id: &str,
    user_db_id: DbId,
    track_db_id: DbId,
    playlist_segment_count: u64,
    job_key: HlsJobKey,
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
            user_db_id,
            track_db_id,
            playlist_segment_count,
            job_key,
            last_access: Instant::now(),
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

pub(crate) fn authorize_hls_segment_session(
    session: &HlsSession,
    principal_user_id: Option<DbId>,
) -> Result<(), HlsError> {
    if let Some(principal_user_id) = principal_user_id
        && session.user_db_id != principal_user_id
    {
        return Err(HlsError::SessionForbidden);
    }

    Ok(())
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
        let jobs = {
            let mut jobs = HLS_JOBS.write().await;
            jobs.drain().map(|(_, job)| job).collect::<Vec<_>>()
        };
        HLS_SESSIONS.write().await.clear();
        HLS_JOB_CREATING.lock().await.clear();

        for job in jobs {
            teardown_hls_job(job).await;
        }
    }

    pub(crate) fn build_test_job(dir_path: PathBuf, playlist_path: PathBuf) -> HlsJob {
        HlsJob {
            dir_path,
            playlist_path,
            transcode_handle: Arc::new(Mutex::new(None)),
            session_ids: HashSet::new(),
            idle_since: None,
            _transcode_permit: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_helpers::*;
    use super::*;
    use agdb::DbId;
    use lyra_ffmpeg::AudioCodec;

    #[test]
    fn hls_job_key_tracks_profile_parameters() {
        let aac_profile =
            HlsCodecProfile::from_requested(Some(AudioCodec::Aac)).expect("aac profile");
        let alac_profile =
            HlsCodecProfile::from_requested(Some(AudioCodec::Alac)).expect("alac profile");

        let aac_key = hls_job_key(DbId(7), DbId(701), None, None, aac_profile);
        let alac_key = hls_job_key(DbId(7), DbId(701), None, None, alac_profile);

        assert_eq!(aac_key.audio_bitrate_kbps, Some(HLS_AUDIO_BITRATE_KBPS));
        assert_eq!(alac_key.audio_bitrate_kbps, None);
        assert_ne!(aac_key, alac_key);
    }

    #[test]
    fn authorize_hls_segment_session_enforces_track_and_owner() {
        let profile = HlsCodecProfile::from_requested(Some(AudioCodec::Aac)).expect("aac profile");
        let session = HlsSession {
            user_db_id: DbId(10),
            track_db_id: DbId(77),
            playlist_segment_count: 1,
            job_key: hls_job_key(DbId(77), DbId(7701), None, None, profile),
            last_access: Instant::now(),
        };

        assert!(authorize_hls_segment_session(&session, Some(DbId(10))).is_ok());
        assert!(authorize_hls_segment_session(&session, None).is_ok());

        let owner_err = authorize_hls_segment_session(&session, Some(DbId(11)))
            .expect_err("owner mismatch should be forbidden");
        assert!(matches!(owner_err, HlsError::SessionForbidden));
    }

    #[tokio::test]
    async fn attach_session_to_job_keeps_single_shared_job_for_concurrent_sessions() {
        let _guard = HLS_TEST_MUTEX.lock().await;
        reset_hls_state_for_test().await;

        let track_db_id = DbId(601);
        let test_dir = unique_test_dir("lyra-hls-shared-job-test");
        tokio::fs::create_dir_all(&test_dir)
            .await
            .expect("test dir created");

        let profile = HlsCodecProfile::from_requested(Some(AudioCodec::Aac)).expect("aac profile");
        let job_key = hls_job_key(track_db_id, DbId(6011), None, None, profile);
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
            attach_session_to_job("session-a", DbId(1), track_db_id, 1, key_a).await
        });
        let attach_b = tokio::spawn(async move {
            attach_session_to_job("session-b", DbId(2), track_db_id, 1, key_b).await
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
