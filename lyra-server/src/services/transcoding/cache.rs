// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Transcodes are encoded into files under the cache directory and served from there, so a
//! finished transcode answers range requests, reports its length, and survives a reconnect.
//!
//! A key names everything that decides a transcode's bytes. At most one encode runs per key:
//! requests that arrive while it runs read the same file as it grows. An encode writes to a
//! uniquely named part file and is renamed into place only once it succeeds; a failed or
//! abandoned encode removes its part file. Because the output is seekable, muxers that finish
//! their header at the end, such as FLAC's STREAMINFO, leave a complete header in the cached file.
//!
//! Finished and running transcodes together stay within the configured size where they can:
//! running encodes evict finished ones as they grow, and a transcode larger than the size is
//! served to its readers but not kept. An encode that stops making progress is stopped, and its
//! source gets no new encode for a while, since FFmpeg's thread may still be stuck reading it. No
//! encode starts while the cache's disk is nearly full.
//!
//! Every method here does blocking file I/O; call them off the async workers.

use std::{
    collections::HashMap,
    fmt::Debug,
    fs::{
        File,
        OpenOptions,
    },
    io::{
        self,
        Seek,
        SeekFrom,
        Write,
    },
    path::{
        Path,
        PathBuf,
    },
    sync::{
        Arc,
        Mutex,
        MutexGuard,
        Weak,
        atomic::{
            AtomicBool,
            AtomicU64,
            Ordering,
        },
    },
    time::{
        Duration,
        Instant,
    },
};

use bytes::Bytes;
use futures::Stream;
use lyra_ffmpeg::{
    FfmpegContext,
    Output,
    SeekRequest,
    SeekResult,
    WriteResult,
};
use tokio::{
    io::AsyncReadExt,
    sync::watch,
};

use super::capacity::{
    Saturated,
    TranscodePermit,
};

const ENCODE_POLL_INTERVAL: Duration = Duration::from_millis(100);
/// How long an encode nobody is reading keeps running, so a client that drops and reconnects
/// picks it up again.
const ABANDONED_ENCODE_GRACE: Duration = Duration::from_secs(5);
const SWEEP_INTERVAL: Duration = Duration::from_secs(60);
/// How long an encode may write nothing before it is taken for stalled, such as on a hung
/// network mount, and stopped.
const STALLED_ENCODE_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a source whose encode stalled gets no new one.
const STALLED_SOURCE_COOLDOWN: Duration = Duration::from_secs(300);
/// The free space the cache's disk must have for an encode to start or keep writing. The data
/// directory also holds the database, which must not be starved by transcodes.
const MIN_FREE_DISK_BYTES: u64 = 1024 * 1024 * 1024;
/// How much a running encode writes between checks of its disk's free space.
const FREE_SPACE_CHECK_BYTES: u64 = 8 * 1024 * 1024;
/// How long a request refused for one of the reasons above is told to wait.
pub(crate) const UNAVAILABLE_RETRY_AFTER: Duration = Duration::from_secs(30);
const READ_CHUNK_SIZE: u64 = 64 * 1024;

/// Names a transcode by everything that decides its bytes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct CacheKey(String);

impl CacheKey {
    /// `parts` is formatted with `Debug`, so it must spell out every input of the encode.
    pub(crate) fn of(parts: &impl Debug) -> Self {
        Self(
            blake3::hash(format!("{parts:?}").as_bytes())
                .to_hex()
                .to_string(),
        )
    }
}

/// The encode to run when a key has no cached or running transcode.
pub(crate) struct EncodeSpec {
    pub(crate) input_path: String,
    pub(crate) start_ms: Option<u64>,
    pub(crate) end_ms: Option<u64>,
    pub(crate) extension: &'static str,
    /// Completes the output, whose writes already land in the cache.
    pub(crate) configure: Box<dyn FnOnce(Output) -> Output + Send>,
    /// Whether the input is still the file the key was made for. Checked once the encode
    /// finishes, so output of a file replaced meanwhile is never cached under the old key.
    pub(crate) input_unchanged: Box<dyn Fn() -> bool + Send>,
}

/// How long finished transcodes are kept and how many bytes the cache may hold.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Budget {
    pub(crate) ttl: Duration,
    pub(crate) max_bytes: u64,
}

impl Budget {
    fn configured() -> Self {
        let config = crate::services::STATE.config();
        Self {
            ttl: Duration::from_secs(config.transcoding.cache_ttl_seconds),
            max_bytes: config.transcoding.cache_max_bytes,
        }
    }
}

pub(crate) struct TranscodeCache {
    entries: Mutex<HashMap<CacheKey, Entry>>,
    next_part_id: AtomicU64,
    /// The bytes of finished entries; changed only under `entries`.
    ready_bytes: AtomicU64,
    /// The bytes running encodes have written so far.
    encoding_bytes: AtomicU64,
    /// `None` reads the configured budget each time.
    budget: Option<Budget>,
    abandoned_grace: Duration,
    stalled_timeout: Duration,
    /// Sources whose last encode stalled, with when it did.
    stalled_sources: Mutex<HashMap<String, Instant>>,
    min_free_bytes: u64,
}

impl Default for TranscodeCache {
    fn default() -> Self {
        Self {
            entries: Mutex::default(),
            next_part_id: AtomicU64::new(0),
            ready_bytes: AtomicU64::new(0),
            encoding_bytes: AtomicU64::new(0),
            budget: None,
            abandoned_grace: ABANDONED_ENCODE_GRACE,
            stalled_timeout: STALLED_ENCODE_TIMEOUT,
            stalled_sources: Mutex::default(),
            min_free_bytes: MIN_FREE_DISK_BYTES,
        }
    }
}

enum Entry {
    Encoding(Arc<Job>),
    Ready(ReadyEntry),
}

struct ReadyEntry {
    path: PathBuf,
    size: u64,
    last_access: Instant,
}

#[derive(Clone, Debug)]
enum Progress {
    Encoding { written: u64 },
    Ready { size: u64 },
    Failed(Arc<str>),
}

struct Job {
    part_path: PathBuf,
    progress: watch::Sender<Progress>,
    readers: Mutex<Readers>,
    abort: AtomicBool,
    /// Set once something waits for the whole file, such as a download: the encode then runs to
    /// the end even when its readers leave.
    keep_running: AtomicBool,
    /// The bytes this encode counts in the cache's `encoding_bytes`.
    accounted: AtomicU64,
    /// Set once the encode is finished or failed. Writes after that, from an FFmpeg thread that
    /// outlived its abort, are refused.
    settled: AtomicBool,
    /// Held while the encode runs, and given up as soon as it is asked to stop.
    permit: Mutex<Option<TranscodePermit>>,
}

impl Job {
    /// Asks the encode to stop; a new request for its key starts afresh from here on. Its permit
    /// goes at once, so that new request is not refused while this one winds down.
    fn stop(&self) {
        self.abort.store(true, Ordering::Release);
        self.release_permit();
    }

    fn release_permit(&self) {
        drop(
            self.permit
                .lock()
                .expect("transcode permit poisoned")
                .take(),
        );
    }
}

#[derive(Default)]
struct Readers {
    count: usize,
    /// Bumped whenever the last reader leaves, so a pending abandonment check can tell whether a
    /// reader came and went in the meantime.
    abandoned: u64,
}

/// What a lookup found for a key.
pub(crate) enum Lookup {
    Ready(ReadyTranscode),
    Encoding(EncodingTranscode),
}

/// A finished transcode, opened for one request.
pub(crate) struct ReadyTranscode {
    pub(crate) file: tokio::fs::File,
    pub(crate) size: u64,
}

/// A running encode, opened for one request. Reading follows the encode as it writes, and the
/// encode stops a short while after its last reader is dropped.
pub(crate) struct EncodingTranscode {
    file: tokio::fs::File,
    progress: watch::Receiver<Progress>,
    reader: ReaderGuard,
}

#[derive(Debug, thiserror::Error)]
#[error("transcode failed: {0}")]
pub(crate) struct TranscodeFailed(Arc<str>);

/// A new encode cannot start for now; retry after [`UNAVAILABLE_RETRY_AFTER`].
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct TranscodeUnavailable(&'static str);

/// The bytes free for unprivileged use on the filesystem holding `dir`.
#[cfg(unix)]
fn free_bytes(dir: &Path) -> io::Result<u64> {
    use std::os::unix::ffi::OsStrExt;

    let path = std::ffi::CString::new(dir.as_os_str().as_bytes())?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is NUL-terminated and `stat` is valid for writes of a `statvfs`.
    if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: statvfs succeeded, so it filled `stat`.
    let stat = unsafe { stat.assume_init() };
    #[allow(clippy::unnecessary_cast)]
    Ok((stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
}

#[cfg(not(unix))]
fn free_bytes(_dir: &Path) -> io::Result<u64> {
    Ok(u64::MAX)
}

impl TranscodeCache {
    #[cfg(test)]
    fn with_budget(budget: Budget, abandoned_grace: Duration) -> Self {
        Self {
            budget: Some(budget),
            abandoned_grace,
            ..Self::default()
        }
    }

    #[cfg(test)]
    fn with_min_free_bytes(min_free_bytes: u64) -> Self {
        Self {
            budget: Some(Budget {
                ttl: Duration::MAX,
                max_bytes: u64::MAX,
            }),
            min_free_bytes,
            ..Self::default()
        }
    }

    #[cfg(test)]
    fn with_stalled_timeout(stalled_timeout: Duration) -> Self {
        Self {
            budget: Some(Budget {
                ttl: Duration::MAX,
                max_bytes: u64::MAX,
            }),
            stalled_timeout,
            ..Self::default()
        }
    }

    fn budget(&self) -> Budget {
        self.budget.unwrap_or_else(Budget::configured)
    }

    fn entries(&self) -> MutexGuard<'_, HashMap<CacheKey, Entry>> {
        self.entries.lock().expect("transcode cache poisoned")
    }

    /// The finished transcode for `key`, if there is one. Never starts or joins an encode.
    pub(crate) fn find_ready(
        self: &Arc<Self>,
        key: &CacheKey,
    ) -> io::Result<Option<ReadyTranscode>> {
        let mut entries = self.entries();
        match self.open(&mut entries, key)? {
            Some(Lookup::Ready(ready)) => Ok(Some(ready)),
            Some(Lookup::Encoding(_)) | None => Ok(None),
        }
    }

    /// The finished or running transcode for `key`, starting `spec`'s encode into `dir` when
    /// there is neither. Starting needs a permit from `admit`; when it refuses, the refusal is
    /// the error and nothing starts.
    pub(crate) fn get_or_start(
        self: &Arc<Self>,
        key: CacheKey,
        dir: &Path,
        spec: EncodeSpec,
        admit: impl FnOnce() -> Result<TranscodePermit, Saturated>,
    ) -> anyhow::Result<Lookup> {
        let (job, write_file, reader) = {
            let mut entries = self.entries();
            if let Some(found) = self.open(&mut entries, &key)? {
                return Ok(found);
            }
            let permit = admit()?;
            std::fs::create_dir_all(dir)?;
            self.refuse_unless_startable(dir, &spec.input_path)?;
            let part_id = self.next_part_id.fetch_add(1, Ordering::Relaxed);
            let part_path = dir.join(format!("{}-{part_id}.part", key.0));
            let write_file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&part_path)?;
            let job = Arc::new(Job {
                part_path,
                progress: watch::Sender::new(Progress::Encoding { written: 0 }),
                readers: Mutex::default(),
                abort: AtomicBool::new(false),
                keep_running: AtomicBool::new(false),
                accounted: AtomicU64::new(0),
                settled: AtomicBool::new(false),
                permit: Mutex::new(Some(permit)),
            });
            let reader = EncodingTranscode::attach(self, &job)?;
            // An entry still here is an encode being stopped; it cleans up after itself.
            entries.insert(key.clone(), Entry::Encoding(Arc::clone(&job)));
            (job, write_file, reader)
        };

        let final_path = dir.join(format!("{}.{}", key.0, spec.extension));
        let input_unchanged = spec.input_unchanged;
        let input_path = spec.input_path.clone();
        let context = match build_encode(
            self,
            &job,
            write_file,
            spec.input_path,
            spec.start_ms,
            spec.end_ms,
            spec.configure,
        ) {
            Ok(context) => context,
            Err(err) => {
                self.fail(&key, &job, err.to_string());
                return Err(err);
            }
        };
        let outcome = EncodeOutcome {
            cache: Arc::downgrade(self),
            key,
            job,
            settled: false,
        };
        let stalled_timeout = self.stalled_timeout;
        tokio::task::spawn_blocking(move || {
            run_encode(
                outcome,
                context,
                final_path,
                input_unchanged,
                stalled_timeout,
                input_path,
            )
        });
        Ok(Lookup::Encoding(reader))
    }

    /// Refuses a new encode of `input_path` while its last encode's stall cools down, or while the
    /// disk holding `dir` is nearly full.
    fn refuse_unless_startable(&self, dir: &Path, input_path: &str) -> anyhow::Result<()> {
        {
            let mut stalled = self
                .stalled_sources
                .lock()
                .expect("stalled sources poisoned");
            stalled.retain(|_, since| since.elapsed() < STALLED_SOURCE_COOLDOWN);
            if stalled.contains_key(input_path) {
                return Err(TranscodeUnavailable(
                    "the track's source stopped responding; retry later",
                )
                .into());
            }
        }
        if free_bytes(dir)? < self.min_free_bytes {
            tracing::warn!(
                path = %dir.display(),
                min_free_bytes = self.min_free_bytes,
                "refusing a transcode: the transcode cache's disk is nearly full"
            );
            return Err(
                TranscodeUnavailable("the server is low on disk space; retry later").into(),
            );
        }
        Ok(())
    }

    fn record_stalled(&self, input_path: String) {
        self.stalled_sources
            .lock()
            .expect("stalled sources poisoned")
            .insert(input_path, Instant::now());
    }

    fn open(
        self: &Arc<Self>,
        entries: &mut HashMap<CacheKey, Entry>,
        key: &CacheKey,
    ) -> io::Result<Option<Lookup>> {
        match entries.get_mut(key) {
            Some(Entry::Ready(ready)) => match File::open(&ready.path) {
                Ok(file) => {
                    ready.last_access = Instant::now();
                    Ok(Some(Lookup::Ready(ReadyTranscode {
                        file: file.into(),
                        size: ready.size,
                    })))
                }
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    if let Some(Entry::Ready(entry)) = entries.remove(key) {
                        self.ready_bytes.fetch_sub(entry.size, Ordering::Relaxed);
                    }
                    Ok(None)
                }
                Err(err) => Err(err),
            },
            // An encode being stopped is about to fail; a new request starts afresh instead of
            // reading a truncated file.
            Some(Entry::Encoding(job)) if job.abort.load(Ordering::Acquire) => Ok(None),
            Some(Entry::Encoding(job)) => Ok(Some(Lookup::Encoding(EncodingTranscode::attach(
                self, job,
            )?))),
            None => Ok(None),
        }
    }

    /// Settles `job` and releases what it held while it ran: its transcoding permit, and its
    /// bytes in the running encodes' count.
    fn release(&self, job: &Job) {
        job.settled.store(true, Ordering::Release);
        job.release_permit();
        let accounted = job.accounted.swap(0, Ordering::AcqRel);
        self.encoding_bytes.fetch_sub(accounted, Ordering::AcqRel);
    }

    /// Hands the finished file to `job`'s readers, and keeps it when `keep` holds and it fits the
    /// cache.
    fn finish(&self, key: &CacheKey, job: &Arc<Job>, final_path: PathBuf, keep: bool) {
        let size = match std::fs::metadata(&job.part_path) {
            Ok(metadata) => metadata.len(),
            Err(err) => return self.fail(key, job, err.to_string()),
        };
        let keep = keep && size <= self.budget().max_bytes;
        {
            let mut entries = self.entries();
            self.release(job);
            let current =
                matches!(entries.get(key), Some(Entry::Encoding(entry)) if Arc::ptr_eq(entry, job));
            if current && !keep {
                entries.remove(key);
                remove_file(&job.part_path);
            } else if current {
                match std::fs::rename(&job.part_path, &final_path) {
                    Ok(()) => {
                        self.ready_bytes.fetch_add(size, Ordering::Relaxed);
                        entries.insert(
                            key.clone(),
                            Entry::Ready(ReadyEntry {
                                path: final_path,
                                size,
                                last_access: Instant::now(),
                            }),
                        );
                    }
                    Err(err) => {
                        tracing::warn!(
                            path = %final_path.display(),
                            error = %err,
                            "failed to move a finished transcode into the cache"
                        );
                        entries.remove(key);
                        remove_file(&job.part_path);
                    }
                }
            } else {
                remove_file(&job.part_path);
            }
        }
        // Readers hold their own descriptors, so they finish reading even when the file is gone.
        job.progress.send_replace(Progress::Ready { size });
        self.sweep();
    }

    fn fail(&self, key: &CacheKey, job: &Arc<Job>, message: String) {
        {
            let mut entries = self.entries();
            self.release(job);
            if matches!(entries.get(key), Some(Entry::Encoding(entry)) if Arc::ptr_eq(entry, job)) {
                entries.remove(key);
            }
        }
        remove_file(&job.part_path);
        job.progress
            .send_replace(Progress::Failed(Arc::from(message)));
    }

    /// Counts `grown` more bytes written by `job`, evicting finished transcodes to make room.
    /// Refuses writes from a job already settled, and from one whose disk runs low on space.
    fn grow(&self, job: &Job, grown: u64) -> io::Result<()> {
        if job.settled.load(Ordering::Acquire) {
            return Err(io::Error::other("the transcode has already ended"));
        }
        let accounted = job.accounted.fetch_add(grown, Ordering::AcqRel);
        if accounted / FREE_SPACE_CHECK_BYTES != (accounted + grown) / FREE_SPACE_CHECK_BYTES
            && let Some(dir) = job.part_path.parent()
        {
            self.keep_free_space(dir)?;
        }
        let encoding = self.encoding_bytes.fetch_add(grown, Ordering::AcqRel) + grown;
        let budget = self.budget();
        // Running encodes that alone exceed the size will not be kept, so evicting finished
        // transcodes for them would gain nothing.
        if encoding <= budget.max_bytes
            && self.ready_bytes.load(Ordering::Relaxed) + encoding > budget.max_bytes
        {
            self.sweep_with(Budget {
                max_bytes: budget.max_bytes - encoding,
                ..budget
            });
        }
        Ok(())
    }

    /// Evicts finished transcodes, least recently used first, until the disk holding `dir` has
    /// the free space it must keep; fails when evicting all of them leaves it short.
    fn keep_free_space(&self, dir: &Path) -> io::Result<()> {
        while free_bytes(dir)? < self.min_free_bytes {
            let oldest = {
                let mut entries = self.entries();
                let oldest = entries
                    .iter()
                    .filter_map(|(key, entry)| match entry {
                        Entry::Ready(ready) => Some((key.clone(), ready.last_access)),
                        Entry::Encoding(_) => None,
                    })
                    .min_by_key(|(_, last_access)| *last_access)
                    .map(|(key, _)| key);
                match oldest.and_then(|key| entries.remove(&key)) {
                    Some(Entry::Ready(entry)) => {
                        self.ready_bytes.fetch_sub(entry.size, Ordering::Relaxed);
                        Some(entry.path)
                    }
                    _ => None,
                }
            };
            let Some(path) = oldest else {
                tracing::warn!(
                    path = %dir.display(),
                    min_free_bytes = self.min_free_bytes,
                    "stopping a transcode: the transcode cache's disk is nearly full"
                );
                return Err(io::Error::new(
                    io::ErrorKind::StorageFull,
                    "the server is low on disk space",
                ));
            };
            remove_file(&path);
        }
        Ok(())
    }

    /// Removes finished transcodes unused for longer than the configured lifetime, then the least
    /// recently used until they and the running encodes fit the configured size. Running encodes
    /// are never removed.
    pub(crate) fn sweep(&self) {
        let budget = self.budget();
        let encoding = self.encoding_bytes.load(Ordering::Acquire);
        self.sweep_with(Budget {
            max_bytes: budget.max_bytes.saturating_sub(encoding),
            ..budget
        });
    }

    /// Readers that already opened a removed transcode keep reading it through their descriptor.
    fn sweep_with(&self, budget: Budget) {
        let now = Instant::now();
        let removed = {
            let mut entries = self.entries();
            let mut ready = entries
                .iter()
                .filter_map(|(key, entry)| match entry {
                    Entry::Ready(ready) => Some((key.clone(), ready.last_access)),
                    Entry::Encoding(_) => None,
                })
                .collect::<Vec<_>>();
            ready.sort_by_key(|(_, last_access)| *last_access);
            let mut removed = Vec::new();
            for (key, last_access) in ready {
                if self.ready_bytes.load(Ordering::Relaxed) <= budget.max_bytes
                    && now.duration_since(last_access) < budget.ttl
                {
                    continue;
                }
                if let Some(Entry::Ready(entry)) = entries.remove(&key) {
                    self.ready_bytes.fetch_sub(entry.size, Ordering::Relaxed);
                    removed.push(entry.path);
                }
            }
            removed
        };
        for path in removed {
            remove_file(&path);
        }
    }
}

/// Sweeps `cache` periodically for as long as it lives.
pub(crate) fn spawn_sweeper(cache: &Arc<TranscodeCache>) {
    let cache = Arc::downgrade(cache);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(SWEEP_INTERVAL).await;
            let Some(cache) = cache.upgrade() else {
                return;
            };
            if let Err(err) = tokio::task::spawn_blocking(move || cache.sweep()).await {
                tracing::warn!(error = %err, "transcode cache sweep failed");
            }
        }
    });
}

fn remove_file(path: &Path) {
    if let Err(err) = std::fs::remove_file(path)
        && err.kind() != io::ErrorKind::NotFound
    {
        tracing::warn!(path = %path.display(), error = %err, "failed to remove a cached transcode");
    }
}

/// Where the encoder's writes land: the part file, with the furthest byte written published to
/// readers. Muxers that finish their header seek back to rewrite it, which never moves that mark.
struct PartWriter {
    file: File,
    position: u64,
    written: u64,
    job: Arc<Job>,
    cache: Weak<TranscodeCache>,
}

impl PartWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let end = self.position + buf.len() as u64;
        if end > self.written {
            let cache = self
                .cache
                .upgrade()
                .ok_or_else(|| io::Error::other("transcode cache was replaced"))?;
            cache.grow(&self.job, end - self.written)?;
        }
        self.file.write_all(buf)?;
        self.position = end;
        if self.position > self.written {
            self.written = self.position;
            let written = self.written;
            self.job.progress.send_modify(|progress| {
                if let Progress::Encoding { written: published } = progress {
                    *published = written;
                }
            });
        }
        Ok(buf.len())
    }

    fn seek(&mut self, request: SeekRequest) -> io::Result<u64> {
        let target = match request {
            SeekRequest::Size => return Ok(self.file.metadata()?.len()),
            SeekRequest::Start(offset) => SeekFrom::Start(u64::try_from(offset).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "negative seek offset")
            })?),
            SeekRequest::Current(offset) => SeekFrom::Current(offset),
            SeekRequest::End(offset) => SeekFrom::End(offset),
        };
        self.position = self.file.seek(target)?;
        Ok(self.position)
    }
}

fn build_encode(
    cache: &Arc<TranscodeCache>,
    job: &Arc<Job>,
    file: File,
    input_path: String,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    configure: Box<dyn FnOnce(Output) -> Output + Send>,
) -> anyhow::Result<FfmpegContext> {
    let writer = Arc::new(Mutex::new(PartWriter {
        file,
        position: 0,
        written: 0,
        job: Arc::clone(job),
        cache: Arc::downgrade(cache),
    }));
    let seek_writer = Arc::clone(&writer);
    let output = Output::with_callback(move |buf: &[u8]| -> WriteResult {
        writer
            .lock()
            .expect("transcode writer poisoned")
            .write(buf)
            .into()
    })
    .set_seek_callback(move |request| -> SeekResult {
        seek_writer
            .lock()
            .expect("transcode writer poisoned")
            .seek(request)
            .into()
    });
    Ok(FfmpegContext::builder()
        .input(input_path)
        .start_ms(start_ms)
        .end_ms(end_ms)
        .output(configure(output))
        .build()?)
}

/// Settles an encode's entry exactly once: finished, failed, or, if the encode thread unwinds
/// before either, failed on drop so readers and the entry are not left waiting forever.
struct EncodeOutcome {
    cache: Weak<TranscodeCache>,
    key: CacheKey,
    job: Arc<Job>,
    settled: bool,
}

impl EncodeOutcome {
    fn finish(mut self, final_path: PathBuf, keep: bool) {
        self.settled = true;
        match self.cache.upgrade() {
            Some(cache) => cache.finish(&self.key, &self.job, final_path, keep),
            None => self.orphan(),
        }
    }

    fn fail(mut self, message: String) {
        self.settled = true;
        match self.cache.upgrade() {
            Some(cache) => cache.fail(&self.key, &self.job, message),
            None => self.orphan(),
        }
    }

    fn orphan(&self) {
        self.job.settled.store(true, Ordering::Release);
        self.job.release_permit();
        remove_file(&self.job.part_path);
        self.job
            .progress
            .send_replace(Progress::Failed(Arc::from("transcode cache was replaced")));
    }
}

impl Drop for EncodeOutcome {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        match self.cache.upgrade() {
            Some(cache) => cache.fail(&self.key, &self.job, "transcode crashed".to_string()),
            None => self.orphan(),
        }
    }
}

fn written(job: &Job) -> u64 {
    match *job.progress.borrow() {
        Progress::Encoding { written } => written,
        Progress::Ready { size } => size,
        Progress::Failed(_) => 0,
    }
}

fn run_encode(
    outcome: EncodeOutcome,
    context: FfmpegContext,
    final_path: PathBuf,
    input_unchanged: Box<dyn Fn() -> bool + Send>,
    stalled_timeout: Duration,
    input_path: String,
) {
    let job = Arc::clone(&outcome.job);
    let shutdown = crate::services::shutdown::token();
    let mut progress = (written(&job), Instant::now());
    let result = match context.start() {
        Ok(mut handle) => loop {
            let now_written = written(&job);
            if now_written != progress.0 {
                progress = (now_written, Instant::now());
            }
            let stalled = progress.1.elapsed() >= stalled_timeout;
            if shutdown.is_cancelled() || job.abort.load(Ordering::Acquire) || stalled {
                if let Err(err) =
                    handle.abort_with_timeout(crate::services::shutdown::TRANSCODE_ABORT_TIMEOUT)
                {
                    tracing::debug!(error = %err, "stopped transcode reported an error");
                }
                if stalled && let Some(cache) = outcome.cache.upgrade() {
                    cache.record_stalled(input_path);
                }
                break Err(if stalled {
                    "the transcode stopped making progress".to_string()
                } else {
                    "transcode stopped".to_string()
                });
            }
            if let Some(result) = handle.wait_completion_timeout(ENCODE_POLL_INTERVAL) {
                break result.map_err(|err| err.to_string());
            }
        },
        Err(err) => Err(err.to_string()),
    };
    match result {
        // Output of a source replaced meanwhile still reaches the readers that asked for it, but
        // is never kept under the old source's key.
        Ok(()) => outcome.finish(final_path, input_unchanged()),
        Err(message) => {
            if !job.abort.load(Ordering::Acquire) && !shutdown.is_cancelled() {
                tracing::warn!(error = %message, "transcode failed");
            }
            outcome.fail(message);
        }
    }
}

struct ReaderGuard {
    job: Arc<Job>,
    abandoned_grace: Duration,
}

impl ReaderGuard {
    fn new(cache: &TranscodeCache, job: &Arc<Job>) -> Self {
        job.readers
            .lock()
            .expect("transcode readers poisoned")
            .count += 1;
        Self {
            job: Arc::clone(job),
            abandoned_grace: cache.abandoned_grace,
        }
    }
}

impl Drop for ReaderGuard {
    fn drop(&mut self) {
        let abandoned = {
            let mut readers = self.job.readers.lock().expect("transcode readers poisoned");
            readers.count -= 1;
            if readers.count > 0 {
                return;
            }
            readers.abandoned += 1;
            readers.abandoned
        };
        if self.job.keep_running.load(Ordering::Acquire)
            || !matches!(*self.job.progress.borrow(), Progress::Encoding { .. })
        {
            return;
        }
        let job = Arc::clone(&self.job);
        let abort_if_still_abandoned = move || {
            let readers = job.readers.lock().expect("transcode readers poisoned");
            if readers.count == 0
                && readers.abandoned == abandoned
                && !job.keep_running.load(Ordering::Acquire)
            {
                job.stop();
            }
        };
        let grace = self.abandoned_grace;
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move {
                    tokio::time::sleep(grace).await;
                    abort_if_still_abandoned();
                });
            }
            Err(_) => abort_if_still_abandoned(),
        }
    }
}

impl EncodingTranscode {
    fn attach(cache: &TranscodeCache, job: &Arc<Job>) -> io::Result<Self> {
        let file = File::open(&job.part_path)?;
        Ok(Self {
            file: file.into(),
            progress: job.progress.subscribe(),
            reader: ReaderGuard::new(cache, job),
        })
    }

    /// Lets the encode run to the end even if every reader leaves, for a caller that will not
    /// get its response until it does.
    pub(crate) fn keep_running(&self) {
        self.reader.job.keep_running.store(true, Ordering::Release);
    }

    /// Waits until the encode has written something, so a failure to start is reported before a
    /// response commits to success.
    pub(crate) async fn started(&mut self) -> Result<(), TranscodeFailed> {
        loop {
            match &*self.progress.borrow_and_update() {
                Progress::Encoding { written: 0 } => {}
                Progress::Encoding { .. } | Progress::Ready { .. } => return Ok(()),
                Progress::Failed(message) => return Err(TranscodeFailed(Arc::clone(message))),
            }
            if self.progress.changed().await.is_err() {
                return Err(TranscodeFailed(Arc::from("transcode ended unexpectedly")));
            }
        }
    }

    /// Waits for the encode to finish and hands back the finished file.
    pub(crate) async fn finished(mut self) -> Result<ReadyTranscode, TranscodeFailed> {
        loop {
            match &*self.progress.borrow_and_update() {
                Progress::Encoding { .. } => {}
                Progress::Ready { size } => {
                    return Ok(ReadyTranscode {
                        file: self.file,
                        size: *size,
                    });
                }
                Progress::Failed(message) => return Err(TranscodeFailed(Arc::clone(message))),
            }
            if self.progress.changed().await.is_err() {
                return Err(TranscodeFailed(Arc::from("transcode ended unexpectedly")));
            }
        }
    }

    /// The transcode's bytes from the start, as the encode writes them.
    pub(crate) fn into_stream(self) -> impl Stream<Item = io::Result<Bytes>> + Send {
        futures::stream::unfold(Some((self, 0_u64)), |state| async move {
            let (mut this, position) = state?;
            loop {
                let progress = this.progress.borrow_and_update().clone();
                let available = match &progress {
                    Progress::Encoding { written } => *written,
                    Progress::Ready { size } => *size,
                    Progress::Failed(message) => {
                        return Some((Err(io::Error::other(message.to_string())), None));
                    }
                };
                if position < available {
                    let len = (available - position).min(READ_CHUNK_SIZE);
                    let mut chunk = vec![0; len as usize];
                    return match this.file.read_exact(&mut chunk).await {
                        Ok(_) => Some((Ok(Bytes::from(chunk)), Some((this, position + len)))),
                        Err(err) => Some((Err(err), None)),
                    };
                }
                if matches!(progress, Progress::Ready { .. }) {
                    return None;
                }
                if this.progress.changed().await.is_err() {
                    return Some((Err(io::Error::other("transcode ended unexpectedly")), None));
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::{
        auth::caller::Caller,
        transcoding::capacity::{
            Limits,
            TranscodeCapacity,
        },
    };
    use lyra_ffmpeg::{
        AudioCodec,
        AudioFormat,
    };

    const LARGE: Budget = Budget {
        ttl: Duration::MAX,
        max_bytes: u64::MAX,
    };

    fn fixture() -> String {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/assets/metadata/integration_track.flac")
            .to_string_lossy()
            .into_owned()
    }

    /// A WAV of `seconds` of stereo noise, long enough that its encode is still running when a
    /// test looks.
    fn long_wav(dir: &Path, seconds: u32) -> anyhow::Result<String> {
        let rate = 44_100_u32;
        let data_len = rate * 4 * seconds;
        let mut file = b"RIFF".to_vec();
        file.extend((36 + data_len).to_le_bytes());
        file.extend(b"WAVEfmt ");
        file.extend(16_u32.to_le_bytes());
        file.extend(1_u16.to_le_bytes());
        file.extend(2_u16.to_le_bytes());
        file.extend(rate.to_le_bytes());
        file.extend((rate * 4).to_le_bytes());
        file.extend(4_u16.to_le_bytes());
        file.extend(16_u16.to_le_bytes());
        file.extend(b"data");
        file.extend(data_len.to_le_bytes());
        let mut state = 0x2545_f491_u32;
        file.extend((0..data_len).map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        }));
        let path = dir.join("long.wav");
        std::fs::write(&path, file)?;
        Ok(path.to_string_lossy().into_owned())
    }

    fn mp3_spec(input_path: String, bitrate_kbps: u32) -> EncodeSpec {
        EncodeSpec {
            input_path,
            start_ms: None,
            end_ms: None,
            extension: "mp3",
            configure: Box::new(move |output| {
                output
                    .audio_format(AudioFormat::Mp3)
                    .codec(AudioCodec::Mp3)
                    .audio_bitrate_kbps(bitrate_kbps)
            }),
            input_unchanged: Box::new(|| true),
        }
    }

    fn flac_spec(input_path: String) -> EncodeSpec {
        EncodeSpec {
            extension: "flac",
            configure: Box::new(|output| {
                output
                    .audio_format(AudioFormat::Flac)
                    .codec(AudioCodec::Flac)
            }),
            ..mp3_spec(input_path, 128)
        }
    }

    fn dir_entries(dir: &Path) -> Vec<PathBuf> {
        std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .filter_map(|entry| Some(entry.ok()?.path()))
                    .filter(|path| path.extension().is_none_or(|extension| extension != "wav"))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn start(
        cache: &Arc<TranscodeCache>,
        dir: &Path,
        key: &str,
        spec: EncodeSpec,
    ) -> EncodingTranscode {
        match cache
            .get_or_start(CacheKey::of(&key), dir, spec, admit)
            .expect("lookup")
        {
            Lookup::Encoding(encoding) => encoding,
            Lookup::Ready(_) => panic!("a new key must start an encode"),
        }
    }

    async fn encode(
        cache: &Arc<TranscodeCache>,
        dir: &Path,
        key: &str,
        spec: EncodeSpec,
    ) -> Result<ReadyTranscode, TranscodeFailed> {
        start(cache, dir, key, spec).finished().await
    }

    async fn read_all(lookup: Lookup) -> io::Result<Vec<u8>> {
        use futures::TryStreamExt;

        match lookup {
            Lookup::Encoding(encoding) => {
                encoding
                    .into_stream()
                    .try_fold(Vec::new(), |mut all, chunk| async move {
                        all.extend_from_slice(&chunk);
                        Ok(all)
                    })
                    .await
            }
            Lookup::Ready(mut ready) => {
                let mut all = Vec::new();
                ready.file.read_to_end(&mut all).await?;
                Ok(all)
            }
        }
    }

    fn test_job(dir: &Path, name: &str) -> anyhow::Result<Arc<Job>> {
        let part_path = dir.join(name);
        std::fs::write(&part_path, b"")?;
        Ok(Arc::new(Job {
            part_path,
            progress: watch::Sender::new(Progress::Encoding { written: 0 }),
            readers: Mutex::default(),
            abort: AtomicBool::new(false),
            keep_running: AtomicBool::new(false),
            accounted: AtomicU64::new(0),
            settled: AtomicBool::new(false),
            permit: Mutex::new(None),
        }))
    }

    fn admit() -> Result<TranscodePermit, Saturated> {
        Arc::new(TranscodeCapacity::default()).try_acquire(
            Limits {
                global: 0,
                per_user: 0,
            },
            &Caller::User("listener".to_string()),
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_encode_leaves_nothing_behind() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let cache = Arc::new(TranscodeCache::with_budget(LARGE, ABANDONED_ENCODE_GRACE));

        let missing = dir.path().join("missing.flac");
        let result = encode(
            &cache,
            dir.path(),
            "missing",
            mp3_spec(missing.to_string_lossy().into_owned(), 128),
        )
        .await;

        assert!(result.is_err());
        assert!(cache.find_ready(&CacheKey::of(&"missing"))?.is_none());
        assert!(cache.entries().is_empty());
        assert_eq!(cache.encoding_bytes.load(Ordering::Acquire), 0);
        assert_eq!(dir_entries(dir.path()), Vec::<PathBuf>::new());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sweeping_removes_expired_and_least_recently_used_transcodes() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let cache = Arc::new(TranscodeCache::with_budget(LARGE, ABANDONED_ENCODE_GRACE));

        let older = encode(&cache, dir.path(), "older", mp3_spec(fixture(), 96)).await?;
        let newer = encode(&cache, dir.path(), "newer", mp3_spec(fixture(), 128)).await?;
        assert_eq!(dir_entries(dir.path()).len(), 2);

        cache.sweep_with(Budget {
            ttl: Duration::MAX,
            max_bytes: older.size.max(newer.size),
        });
        assert!(cache.find_ready(&CacheKey::of(&"older"))?.is_none());
        assert!(cache.find_ready(&CacheKey::of(&"newer"))?.is_some());
        assert_eq!(dir_entries(dir.path()).len(), 1);

        cache.sweep_with(Budget {
            ttl: Duration::ZERO,
            max_bytes: u64::MAX,
        });
        assert!(cache.entries().is_empty());
        assert_eq!(cache.ready_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(dir_entries(dir.path()), Vec::<PathBuf>::new());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_second_request_joins_the_running_encode() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let cache = Arc::new(TranscodeCache::with_budget(LARGE, ABANDONED_ENCODE_GRACE));
        let input = long_wav(dir.path(), 60)?;
        let key = CacheKey::of(&"shared");

        let first = start(&cache, dir.path(), "shared", flac_spec(input.clone()));
        let joined = cache.get_or_start(key.clone(), dir.path(), flac_spec(input), admit)?;
        assert!(
            matches!(joined, Lookup::Encoding(_)),
            "a minute of audio is still encoding, so the second request joins it"
        );
        assert_eq!(
            cache.next_part_id.load(Ordering::Relaxed),
            1,
            "one encode ran"
        );

        let (first, joined) = tokio::join!(read_all(Lookup::Encoding(first)), read_all(joined));
        let (first, joined) = (first?, joined?);
        assert!(!first.is_empty());
        assert_eq!(first, joined);
        let Some(ready) = cache.find_ready(&key)? else {
            panic!("the finished encode is cached");
        };
        assert_eq!(ready.size, first.len() as u64);
        let entries = dir_entries(dir.path());
        assert_eq!(entries.len(), 1, "no part files left: {entries:?}");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_open_transcode_keeps_serving_after_it_is_swept() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let cache = Arc::new(TranscodeCache::with_budget(LARGE, ABANDONED_ENCODE_GRACE));
        let finished = encode(&cache, dir.path(), "swept", mp3_spec(fixture(), 128)).await?;

        let Some(open) = cache.find_ready(&CacheKey::of(&"swept"))? else {
            panic!("finished transcode is cached");
        };
        cache.sweep_with(Budget {
            ttl: Duration::ZERO,
            max_bytes: 0,
        });
        assert_eq!(dir_entries(dir.path()), Vec::<PathBuf>::new());

        let bytes = read_all(Lookup::Ready(open)).await?;
        assert_eq!(bytes.len() as u64, finished.size);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_transcode_larger_than_the_cache_is_served_but_not_kept() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let cache = Arc::new(TranscodeCache::with_budget(
            Budget {
                ttl: Duration::MAX,
                max_bytes: 1_000,
            },
            ABANDONED_ENCODE_GRACE,
        ));

        let streamed = read_all(Lookup::Encoding(start(
            &cache,
            dir.path(),
            "huge",
            mp3_spec(fixture(), 128),
        )))
        .await?;
        let downloaded = encode(
            &cache,
            dir.path(),
            "huge-download",
            mp3_spec(fixture(), 128),
        )
        .await?;

        assert!(streamed.len() > 1_000);
        assert_eq!(downloaded.size, streamed.len() as u64);
        assert!(cache.entries().is_empty());
        assert_eq!(cache.encoding_bytes.load(Ordering::Acquire), 0);
        assert_eq!(cache.ready_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(dir_entries(dir.path()), Vec::<PathBuf>::new());
        Ok(())
    }

    #[test]
    fn a_settled_encode_refuses_further_writes() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let cache = TranscodeCache::with_budget(LARGE, ABANDONED_ENCODE_GRACE);
        let job = test_job(dir.path(), "settled.part")?;
        cache.grow(&job, 10)?;
        cache.release(&job);

        assert!(cache.grow(&job, 10).is_err());
        assert_eq!(cache.encoding_bytes.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stalled_encode_is_stopped() -> anyhow::Result<()> {
        use std::os::unix::ffi::OsStrExt;

        let dir = tempfile::tempdir()?;
        let fifo = dir.path().join("stalled.wav");
        let fifo_c = std::ffi::CString::new(fifo.as_os_str().as_bytes())?;
        // SAFETY: a valid NUL-terminated path; mkfifo has no other preconditions.
        assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0);
        let cache = Arc::new(TranscodeCache::with_stalled_timeout(Duration::from_millis(
            200,
        )));

        let encoding = start(
            &cache,
            dir.path(),
            "stalled",
            mp3_spec(fifo.to_string_lossy().into_owned(), 128),
        );
        encoding.keep_running();
        let result = tokio::time::timeout(Duration::from_secs(10), encoding.finished()).await?;
        // Unblock FFmpeg's open of the input so its thread can end.
        drop(std::fs::OpenOptions::new().write(true).open(&fifo));

        assert!(
            result.is_err(),
            "a stalled encode fails rather than waiting forever"
        );
        assert!(cache.entries().is_empty());

        let retried = cache.get_or_start(
            CacheKey::of(&"stalled-again"),
            dir.path(),
            mp3_spec(fifo.to_string_lossy().into_owned(), 128),
            admit,
        );
        assert!(
            retried.is_err_and(|err| err.is::<TranscodeUnavailable>()),
            "a stalled source gets no new encode while it cools down"
        );
        encode(&cache, dir.path(), "other", mp3_spec(fixture(), 128)).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_encode_starts_on_a_nearly_full_disk() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let roomy = Arc::new(TranscodeCache::with_min_free_bytes(0));
        encode(&roomy, dir.path(), "cached", mp3_spec(fixture(), 128)).await?;
        let full = Arc::new(TranscodeCache {
            entries: Mutex::new(std::mem::take(&mut *roomy.entries())),
            ready_bytes: AtomicU64::new(roomy.ready_bytes.load(Ordering::Relaxed)),
            ..TranscodeCache::with_min_free_bytes(u64::MAX)
        });

        let refused = full.get_or_start(
            CacheKey::of(&"new"),
            dir.path(),
            mp3_spec(fixture(), 96),
            admit,
        );
        assert!(refused.is_err_and(|err| err.is::<TranscodeUnavailable>()));
        assert!(
            full.find_ready(&CacheKey::of(&"cached"))?.is_some(),
            "finished transcodes are still served"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_running_encode_stops_once_the_disk_runs_low() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let roomy = Arc::new(TranscodeCache::with_min_free_bytes(0));
        encode(&roomy, dir.path(), "cached", mp3_spec(fixture(), 128)).await?;
        let full = Arc::new(TranscodeCache {
            entries: Mutex::new(std::mem::take(&mut *roomy.entries())),
            ready_bytes: AtomicU64::new(roomy.ready_bytes.load(Ordering::Relaxed)),
            ..TranscodeCache::with_min_free_bytes(u64::MAX)
        });
        let job = test_job(dir.path(), "growing.part")?;

        full.grow(&job, FREE_SPACE_CHECK_BYTES - 1)?;
        let refused = full
            .grow(&job, 1)
            .expect_err("the disk is too full to keep writing");

        assert_eq!(refused.kind(), io::ErrorKind::StorageFull);
        assert!(
            full.find_ready(&CacheKey::of(&"cached"))?.is_none(),
            "finished transcodes are evicted to make room first"
        );
        assert_eq!(full.ready_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(
            dir_entries(dir.path()),
            vec![dir.path().join("growing.part")]
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_encode_already_too_large_to_keep_evicts_nothing() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let small = {
            let probe = Arc::new(TranscodeCache::with_budget(LARGE, ABANDONED_ENCODE_GRACE));
            let probe_dir = tempfile::tempdir()?;
            encode(&probe, probe_dir.path(), "probe", mp3_spec(fixture(), 128))
                .await?
                .size
        };
        let cache = Arc::new(TranscodeCache::with_budget(
            Budget {
                ttl: Duration::MAX,
                max_bytes: small * 2,
            },
            ABANDONED_ENCODE_GRACE,
        ));
        encode(&cache, dir.path(), "small", mp3_spec(fixture(), 128)).await?;
        let job = test_job(dir.path(), "large.part")?;

        cache.grow(&job, small * 2 + 1)?;
        cache.grow(&job, small)?;

        assert!(
            cache.find_ready(&CacheKey::of(&"small"))?.is_some(),
            "an encode that will not be kept does not evict the transcodes that are"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn running_encodes_evict_finished_ones_to_fit() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let size = {
            let probe = Arc::new(TranscodeCache::with_budget(LARGE, ABANDONED_ENCODE_GRACE));
            let probe_dir = tempfile::tempdir()?;
            encode(&probe, probe_dir.path(), "probe", mp3_spec(fixture(), 128))
                .await?
                .size
        };
        let cache = Arc::new(TranscodeCache::with_budget(
            Budget {
                ttl: Duration::MAX,
                max_bytes: size + size / 2,
            },
            ABANDONED_ENCODE_GRACE,
        ));

        encode(&cache, dir.path(), "first", mp3_spec(fixture(), 128)).await?;
        encode(&cache, dir.path(), "second", mp3_spec(fixture(), 128)).await?;

        assert!(cache.find_ready(&CacheKey::of(&"first"))?.is_none());
        assert!(cache.find_ready(&CacheKey::of(&"second"))?.is_some());
        assert_eq!(cache.ready_bytes.load(Ordering::Relaxed), size);
        assert_eq!(dir_entries(dir.path()).len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn an_abandoned_encode_stops_unless_a_reader_returns_or_it_must_finish()
    -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let grace = Duration::from_millis(50);
        let cache = TranscodeCache::with_budget(LARGE, grace);

        let abandoned = test_job(dir.path(), "abandoned")?;
        drop(EncodingTranscode::attach(&cache, &abandoned)?);
        let returned = test_job(dir.path(), "returned")?;
        drop(EncodingTranscode::attach(&cache, &returned)?);
        let reader = EncodingTranscode::attach(&cache, &returned)?;
        let downloaded = test_job(dir.path(), "downloaded")?;
        let download = EncodingTranscode::attach(&cache, &downloaded)?;
        download.keep_running();
        drop(download);

        tokio::time::sleep(grace * 4).await;
        assert!(abandoned.abort.load(Ordering::Acquire));
        assert!(!returned.abort.load(Ordering::Acquire));
        assert!(!downloaded.abort.load(Ordering::Acquire));
        drop(reader);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_request_for_a_stopping_encode_starts_a_new_one() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let cache = Arc::new(TranscodeCache::with_budget(LARGE, ABANDONED_ENCODE_GRACE));
        let stopping = test_job(dir.path(), "stopping.part")?;
        stopping.abort.store(true, Ordering::Release);
        cache
            .entries()
            .insert(CacheKey::of(&"key"), Entry::Encoding(Arc::clone(&stopping)));

        let bytes = read_all(Lookup::Encoding(start(
            &cache,
            dir.path(),
            "key",
            mp3_spec(fixture(), 128),
        )))
        .await?;

        assert!(!bytes.is_empty());
        assert!(cache.find_ready(&CacheKey::of(&"key"))?.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn an_encode_that_never_settles_fails_its_readers() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let cache = Arc::new(TranscodeCache::with_budget(LARGE, ABANDONED_ENCODE_GRACE));
        let job = test_job(dir.path(), "crashed.part")?;
        let key = CacheKey::of(&"crashed");
        cache
            .entries()
            .insert(key.clone(), Entry::Encoding(Arc::clone(&job)));
        let reader = EncodingTranscode::attach(&cache, &job)?;

        drop(EncodeOutcome {
            cache: Arc::downgrade(&cache),
            key,
            job,
            settled: false,
        });

        assert!(reader.finished().await.is_err());
        assert!(cache.entries().is_empty());
        assert_eq!(dir_entries(dir.path()), Vec::<PathBuf>::new());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn output_of_a_changed_input_is_served_but_not_cached() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let cache = Arc::new(TranscodeCache::with_budget(LARGE, ABANDONED_ENCODE_GRACE));

        let bytes = read_all(Lookup::Encoding(start(
            &cache,
            dir.path(),
            "changed",
            EncodeSpec {
                input_unchanged: Box::new(|| false),
                ..mp3_spec(fixture(), 128)
            },
        )))
        .await?;

        assert!(
            !bytes.is_empty(),
            "its readers still get the whole transcode"
        );
        assert!(cache.entries().is_empty());
        assert_eq!(dir_entries(dir.path()), Vec::<PathBuf>::new());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_encode_holds_its_permit_only_while_it_runs() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let cache = Arc::new(TranscodeCache::with_budget(LARGE, ABANDONED_ENCODE_GRACE));
        let capacity = Arc::new(TranscodeCapacity::default());
        let one = Limits {
            global: 1,
            per_user: 0,
        };
        let input = long_wav(dir.path(), 60)?;

        let running = cache.get_or_start(
            CacheKey::of(&"running"),
            dir.path(),
            flac_spec(input),
            || capacity.try_acquire(one, &Caller::User("listener".to_string())),
        )?;
        let refused = cache.get_or_start(
            CacheKey::of(&"refused"),
            dir.path(),
            mp3_spec(fixture(), 128),
            || capacity.try_acquire(one, &Caller::User("listener".to_string())),
        );
        assert!(refused.is_err_and(|err| err.is::<Saturated>()));
        assert!(cache.find_ready(&CacheKey::of(&"refused"))?.is_none());

        let Lookup::Encoding(running) = running else {
            panic!("a new key starts an encode");
        };
        running.finished().await?;
        drop(capacity.try_acquire(one, &Caller::User("listener".to_string()))?);

        let failed = cache.get_or_start(
            CacheKey::of(&"failed"),
            dir.path(),
            mp3_spec(
                dir.path().join("missing").to_string_lossy().into_owned(),
                128,
            ),
            || capacity.try_acquire(one, &Caller::User("listener".to_string())),
        )?;
        let Lookup::Encoding(failed) = failed else {
            panic!("a new key starts an encode");
        };
        assert!(failed.finished().await.is_err());
        drop(capacity.try_acquire(one, &Caller::User("listener".to_string()))?);
        Ok(())
    }

    #[test]
    fn a_stopped_encode_gives_up_its_permit_at_once() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let capacity = Arc::new(TranscodeCapacity::default());
        let one = Limits {
            global: 1,
            per_user: 0,
        };
        let job = test_job(dir.path(), "stopping.part")?;
        *job.permit.lock().expect("permit") =
            Some(capacity.try_acquire(one, &Caller::User("listener".to_string()))?);
        assert!(
            capacity
                .try_acquire(one, &Caller::User("listener".to_string()))
                .is_err()
        );

        job.stop();

        drop(capacity.try_acquire(one, &Caller::User("listener".to_string()))?);
        Ok(())
    }
}
