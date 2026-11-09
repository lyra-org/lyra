// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Transformed covers, kept on disk and served from there.
//!
//! A variant's path names everything that decides its bytes: the cover's content hash, then the
//! format, bounds and quality its options snap to, so nearby requests share one file. At most one
//! transform runs per variant; requests that arrive while it runs wait for its file. A variant is
//! written to a temporary file and renamed into place. One that fails to transform is remembered
//! for the generation and not tried again; after a failed write, no variant is transformed for a
//! while. Past the size cap, the oldest variants are evicted. Variants are served from handles
//! opened while they exist, so an eviction never fails a request that found its file.
//!
//! Methods that touch files block; call them off the async workers.

use std::{
    collections::{
        HashMap,
        HashSet,
        VecDeque,
    },
    fs::{
        self,
        File,
    },
    io::{
        self,
        Seek,
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
    },
    time::{
        Duration,
        Instant,
        SystemTime,
    },
};

use super::{
    extension_from_image_format,
    header::CoverHeader,
};
use crate::services::{
    STATE,
    covers::CoverTransformOptions,
};

/// The bounds a requested width or height snaps up to. Larger requests are left unbounded.
const SIZE_STEPS: [u32; 6] = [64, 128, 256, 512, 1024, 2048];
/// The JPEG qualities a requested quality snaps up to; the other encoders are lossless.
const JPEG_QUALITY_STEPS: [u8; 4] = [50, 75, 90, 100];
const DEFAULT_JPEG_QUALITY: u8 = 90;
const TEMP_PREFIX: &str = ".tmp";
/// How long transforms stop after a variant fails to be written, as when the disk is full.
const STORAGE_BACKOFF: Duration = Duration::from_secs(60);

/// A transform as the cache keys it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Variant {
    pub(super) format: image::ImageFormat,
    max_width: Option<u32>,
    max_height: Option<u32>,
    quality: Option<u8>,
}

impl Variant {
    /// What `options` ask of `source`, snapped to the cached steps, or `None` when that changes
    /// nothing. A bound at or past the source's size is no bound.
    pub(super) fn of(source: &CoverHeader, options: &CoverTransformOptions) -> Option<Self> {
        let format = options.format.unwrap_or(source.format);
        let max_width = snap_bound(options.max_width, source.width);
        let max_height = snap_bound(options.max_height, source.height);
        if format == source.format && max_width.is_none() && max_height.is_none() {
            return None;
        }
        let quality = (format == image::ImageFormat::Jpeg).then(|| {
            let requested = options.quality.unwrap_or(DEFAULT_JPEG_QUALITY);
            JPEG_QUALITY_STEPS
                .into_iter()
                .find(|step| *step >= requested)
                .unwrap_or(100)
        });
        Some(Self {
            format,
            max_width,
            max_height,
            quality,
        })
    }

    pub(super) fn options(&self) -> CoverTransformOptions {
        CoverTransformOptions {
            format: Some(self.format),
            quality: self.quality,
            max_width: self.max_width,
            max_height: self.max_height,
        }
    }

    fn file_name(&self) -> String {
        let quality = self.quality.map(|q| format!("-q{q}")).unwrap_or_default();
        format!(
            "{}x{}{quality}.{}",
            self.max_width.unwrap_or(0),
            self.max_height.unwrap_or(0),
            extension_from_image_format(self.format).expect("cover formats have extensions")
        )
    }
}

fn snap_bound(requested: Option<u32>, source: u32) -> Option<u32> {
    let requested = requested?;
    let step = SIZE_STEPS.into_iter().find(|step| *step >= requested)?;
    (step < source).then_some(step)
}

pub(crate) struct TransformCache {
    dir: PathBuf,
    /// How much the cache keeps before it evicts its oldest variants; `None` follows the
    /// `covers.transform_cache_max_bytes` setting.
    max_bytes: Option<u64>,
    /// The stored variants, oldest first, read from the directory on first use.
    entries: Mutex<Option<Entries>>,
    failed: Mutex<HashSet<PathBuf>>,
    flights: Mutex<HashMap<PathBuf, FlightTurn>>,
    storage_backoff: Duration,
    /// Until when nothing is stored, after a write failed.
    unwritable_until: Mutex<Option<Instant>>,
}

/// A variant's turn to transform, and how many requests hold or wait for it.
#[derive(Default)]
struct FlightTurn {
    turn: Arc<tokio::sync::Mutex<()>>,
    holders: usize,
}

#[derive(Default)]
struct Entries {
    files: VecDeque<(PathBuf, u64)>,
    bytes: u64,
}

/// The one transform of a variant that may run. Waiting for it waits for that transform.
pub(super) struct Flight {
    _turn: tokio::sync::OwnedMutexGuard<()>,
    _holder: FlightHolder,
}

/// A request's hold on a variant's turn, from before it waits for the turn: one that stops
/// waiting lets go of it as one that ran does.
struct FlightHolder {
    cache: Arc<TransformCache>,
    path: PathBuf,
}

impl Drop for FlightHolder {
    fn drop(&mut self) {
        let mut flights = lock(&self.cache.flights);
        if let Some(turn) = flights.get_mut(&self.path) {
            turn.holders -= 1;
            if turn.holders == 0 {
                flights.remove(&self.path);
            }
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl TransformCache {
    fn new(dir: PathBuf, max_bytes: Option<u64>, storage_backoff: Duration) -> Self {
        Self {
            dir,
            max_bytes,
            entries: Mutex::default(),
            failed: Mutex::default(),
            flights: Mutex::default(),
            storage_backoff,
            unwritable_until: Mutex::default(),
        }
    }

    /// The generation's cache, in the cover cache directory the server started with.
    pub(crate) fn current() -> Arc<Self> {
        Arc::clone(STATE.generation().cover_transforms.get_or_init(|| {
            Arc::new(Self::new(
                STATE.boot.get().cover_cache_dir.clone(),
                None,
                STORAGE_BACKOFF,
            ))
        }))
    }

    fn max_bytes(&self) -> u64 {
        self.max_bytes
            .unwrap_or_else(|| STATE.config().covers.transform_cache_max_bytes)
    }

    fn cover_dir(&self, hash: &str) -> Option<PathBuf> {
        (!hash.is_empty() && hash.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .then(|| self.dir.join(hash))
    }

    /// Where the variant of the cover with content `hash` is kept, unless the hash is not one.
    pub(super) fn path(&self, hash: &str, variant: &Variant) -> Option<PathBuf> {
        self.cover_dir(hash)
            .map(|dir| dir.join(variant.file_name()))
    }

    pub(super) fn failed(&self, path: &Path) -> bool {
        lock(&self.failed).contains(path)
    }

    pub(super) fn record_failure(&self, path: &Path) {
        lock(&self.failed).insert(path.to_path_buf());
    }

    /// Whether a write failed too recently for a transform to be worth its cost.
    pub(super) fn unwritable(&self) -> bool {
        lock(&self.unwritable_until).is_some_and(|until| Instant::now() < until)
    }

    /// Opens the variant at `path`, if it is stored.
    pub(super) fn open(&self, path: &Path) -> io::Result<Option<(File, u64)>> {
        let file = match open_variant(path) {
            Ok(file) => file,
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
                ) =>
            {
                return Ok(None);
            }
            Err(err) => return Err(err),
        };
        let meta = file.metadata()?;
        Ok(meta.is_file().then_some((file, meta.len())))
    }

    /// Waits until no other transform of the variant at `path` runs.
    pub(super) async fn flight(self: &Arc<Self>, path: &Path) -> Flight {
        let turn = {
            let mut flights = lock(&self.flights);
            let turn = flights.entry(path.to_path_buf()).or_default();
            turn.holders += 1;
            Arc::clone(&turn.turn)
        };
        let holder = FlightHolder {
            cache: Arc::clone(self),
            path: path.to_path_buf(),
        };
        Flight {
            _turn: turn.lock_owned().await,
            _holder: holder,
        }
    }

    /// Stores `bytes` as the variant at `path`, then evicts the oldest variants past the cap, and
    /// returns the stored file opened at its start. The newest stays even when it alone is past
    /// the cap, since its caller serves it. A failed write stops transforms for the backoff, and
    /// is logged once for it.
    pub(super) fn store(&self, path: &Path, bytes: &[u8]) -> io::Result<File> {
        self.write(path, bytes).inspect_err(|err| {
            let mut until = lock(&self.unwritable_until);
            let now = Instant::now();
            if until.is_none_or(|until| until <= now) {
                tracing::warn!(
                    path = %path.display(),
                    error = %err,
                    backoff_secs = self.storage_backoff.as_secs(),
                    "failed to cache a transformed cover; serving covers untransformed for a while"
                );
                *until = Some(now + self.storage_backoff);
            }
        })
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<File> {
        let mut guard = lock(&self.entries);
        let entries = guard.get_or_insert_with(|| scan(&self.dir));
        let dir = path
            .parent()
            .expect("variant paths are in a cover directory");
        fs::create_dir_all(dir)?;
        let mut file = tempfile::Builder::new()
            .prefix(TEMP_PREFIX)
            .tempfile_in(dir)?;
        file.write_all(bytes)?;
        file.as_file().sync_data()?;
        let mut stored = file.persist(path).map_err(|err| err.error)?;
        stored.rewind()?;

        if let Some(index) = entries.files.iter().position(|(stored, _)| stored == path) {
            let (_, len) = entries.files.remove(index).expect("index is in range");
            entries.bytes -= len;
        }
        entries
            .files
            .push_back((path.to_path_buf(), bytes.len() as u64));
        entries.bytes += bytes.len() as u64;
        entries.evict(self.max_bytes());
        Ok(stored)
    }

    /// Deletes every variant of the cover with content `hash`.
    pub(crate) fn forget(&self, hash: &str) {
        let Some(dir) = self.cover_dir(hash) else {
            return;
        };
        let mut entries = lock(&self.entries);
        if let Some(entries) = entries.as_mut() {
            entries.files.retain(|(path, len)| {
                let kept = !path.starts_with(&dir);
                if !kept {
                    entries.bytes -= len;
                }
                kept
            });
        }
        match fs::remove_dir_all(&dir) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => tracing::warn!(
                path = %dir.display(),
                error = %err,
                "failed to remove transformed covers"
            ),
        }
        drop(entries);
        lock(&self.failed).retain(|path| !path.starts_with(&dir));
    }
}

impl Entries {
    /// A variant that cannot be removed yet, as one open elsewhere on Windows, stays counted and
    /// oldest, to be evicted by a later store.
    fn evict(&mut self, max_bytes: u64) {
        let mut kept = Vec::new();
        while self.bytes > max_bytes && self.files.len() > 1 {
            let (path, len) = self
                .files
                .pop_front()
                .expect("more than one file is stored");
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => {
                    tracing::debug!(path = %path.display(), error = %err, "failed to evict a transformed cover");
                    kept.push((path, len));
                    continue;
                }
            }
            self.bytes -= len;
            if let Some(dir) = path.parent() {
                let _ = fs::remove_dir(dir);
            }
        }
        for entry in kept.into_iter().rev() {
            self.files.push_front(entry);
        }
    }
}

/// Opens a variant so that it can still be evicted while it is served.
fn open_variant(path: &Path) -> io::Result<File> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE
        options.share_mode(0x1 | 0x2 | 0x4);
    }
    options.open(path)
}

/// The variants a previous run left, oldest first. Temporary files of writes it never finished
/// are removed.
fn scan(dir: &Path) -> Entries {
    let mut found = Vec::new();
    for cover_dir in fs::read_dir(dir).into_iter().flatten().flatten() {
        for file in fs::read_dir(cover_dir.path())
            .into_iter()
            .flatten()
            .flatten()
        {
            let path = file.path();
            if file.file_name().to_string_lossy().starts_with(TEMP_PREFIX) {
                let _ = fs::remove_file(&path);
                continue;
            }
            let Ok(meta) = file.metadata() else {
                continue;
            };
            if meta.is_file() {
                let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                found.push((modified, path, meta.len()));
            }
        }
    }
    found.sort();
    Entries {
        bytes: found.iter().map(|(_, _, len)| len).sum(),
        files: found
            .into_iter()
            .map(|(_, path, len)| (path, len))
            .collect(),
    }
}

/// Deletes the transformed variants of the cover with content `hash`. Without server state there
/// is no cache to delete from.
pub(crate) fn forget_cover_transforms(hash: &str) {
    if STATE.get().is_some() {
        TransformCache::current().forget(hash);
    }
}

#[cfg(test)]
pub(super) mod testing {
    use super::*;

    pub(in super::super) fn cache(dir: &Path, max_bytes: u64) -> Arc<TransformCache> {
        Arc::new(TransformCache::new(
            dir.to_path_buf(),
            Some(max_bytes),
            STORAGE_BACKOFF,
        ))
    }

    pub(in super::super) fn cache_with_backoff(
        dir: &Path,
        storage_backoff: Duration,
    ) -> Arc<TransformCache> {
        Arc::new(TransformCache::new(
            dir.to_path_buf(),
            Some(1 << 20),
            storage_backoff,
        ))
    }

    pub(in super::super) fn flights(cache: &TransformCache) -> usize {
        lock(&cache.flights).len()
    }

    pub(in super::super) fn flight_holders(cache: &TransformCache) -> usize {
        lock(&cache.flights).values().map(|turn| turn.holders).sum()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::services::covers::image::header::{
        fixtures::jpeg_header,
        read_cover_header,
    };

    const HASH: &str = "0123456789abcdef0123456789abcdef";

    fn source(width: u16, height: u16) -> CoverHeader {
        read_cover_header(&mut Cursor::new(jpeg_header(width, height))).expect("header")
    }

    fn options(
        format: Option<image::ImageFormat>,
        quality: Option<u8>,
        max_width: Option<u32>,
        max_height: Option<u32>,
    ) -> CoverTransformOptions {
        CoverTransformOptions {
            format,
            quality,
            max_width,
            max_height,
        }
    }

    fn name(source: &CoverHeader, options: &CoverTransformOptions) -> Option<String> {
        Variant::of(source, options).map(|variant| variant.file_name())
    }

    #[test]
    fn bounds_snap_up_to_steps_within_the_source() {
        let large = source(3000, 2000);
        for (requested, snapped) in [
            (1, Some("64x0-q90.jpg")),
            (64, Some("64x0-q90.jpg")),
            (65, Some("128x0-q90.jpg")),
            (300, Some("512x0-q90.jpg")),
            (2048, Some("2048x0-q90.jpg")),
            (2049, None),
            (u32::MAX, None),
        ] {
            assert_eq!(
                name(&large, &options(None, None, Some(requested), None)).as_deref(),
                snapped,
                "{requested}"
            );
        }
        let small = source(200, 100);
        assert_eq!(name(&small, &options(None, None, Some(150), None)), None);
        assert_eq!(name(&small, &options(None, None, None, Some(100))), None);
        assert_eq!(
            name(&small, &options(None, None, Some(150), Some(50))).as_deref(),
            Some("0x64-q90.jpg")
        );
    }

    #[test]
    fn formats_and_qualities_collapse_to_one_variant() {
        let large = source(3000, 2000);
        let jpeg = Some(image::ImageFormat::Jpeg);
        let png = Some(image::ImageFormat::Png);
        for (options, snapped) in [
            (options(jpeg, None, None, None), None),
            (options(None, Some(10), None, None), None),
            (options(png, None, None, None), Some("0x0.png")),
            (options(png, Some(10), Some(100), None), Some("128x0.png")),
            (
                options(jpeg, Some(10), Some(100), None),
                Some("128x0-q50.jpg"),
            ),
            (
                options(None, Some(76), Some(100), None),
                Some("128x0-q90.jpg"),
            ),
            (
                options(None, Some(90), Some(100), None),
                Some("128x0-q90.jpg"),
            ),
            (
                options(None, Some(95), Some(100), None),
                Some("128x0-q100.jpg"),
            ),
        ] {
            assert_eq!(name(&large, &options).as_deref(), snapped, "{options:?}");
        }
    }

    #[test]
    fn eviction_removes_the_oldest_variants_past_the_cap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = testing::cache(dir.path(), 10);
        let path = |name: &str| dir.path().join(HASH).join(name);

        let served = cache.store(&path("a.png"), &[1; 4]).expect("stores");
        cache.store(&path("b.png"), &[0; 4]).expect("stores");
        cache.store(&path("c.png"), &[0; 4]).expect("stores");
        assert!(!path("a.png").exists());
        assert!(path("b.png").exists() && path("c.png").exists());
        assert_eq!(io::read_to_string(served).ok(), Some("\u{1}".repeat(4)));

        cache.store(&path("d.png"), &[0; 20]).expect("stores");
        assert!(!path("b.png").exists() && !path("c.png").exists());
        assert!(path("d.png").exists());
    }

    #[test]
    fn a_restarted_cache_counts_what_it_finds_and_drops_unfinished_writes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cover_dir = dir.path().join(HASH);
        fs::create_dir_all(&cover_dir).expect("cover dir");
        fs::write(cover_dir.join("old.png"), [0; 8]).expect("write");
        fs::write(cover_dir.join(".tmpabc"), [0; 8]).expect("write");
        let cache = testing::cache(dir.path(), 10);

        cache
            .store(&cover_dir.join("new.png"), &[0; 4])
            .expect("stores");
        assert!(!cover_dir.join(".tmpabc").exists());
        assert!(!cover_dir.join("old.png").exists());
        assert!(cover_dir.join("new.png").exists());
    }

    #[test]
    fn forgetting_a_cover_removes_its_variants_and_failures() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = testing::cache(dir.path(), 1024);
        let other = "fedcba9876543210fedcba9876543210";
        let stored = dir.path().join(HASH).join("a.png");
        let failed = dir.path().join(HASH).join("b.png");
        let kept = dir.path().join(other).join("a.png");
        cache.store(&stored, &[0; 4]).expect("stores");
        cache.store(&kept, &[0; 4]).expect("stores");
        cache.record_failure(&failed);

        cache.forget(HASH);
        cache.forget("../escape");
        assert!(!dir.path().join(HASH).exists());
        assert!(!cache.failed(&failed));
        assert!(kept.exists());
        assert_eq!(
            lock(&cache.entries).as_ref().map(|entries| entries.bytes),
            Some(4)
        );
    }

    #[tokio::test]
    async fn requests_that_stop_waiting_for_a_flight_let_go_of_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = testing::cache(dir.path(), 1024);
        let path = dir.path().join(HASH).join("a.png");
        let wait = || {
            let (cache, path) = (Arc::clone(&cache), path.clone());
            tokio::spawn(async move {
                let _flight = cache.flight(&path).await;
                std::future::pending::<()>().await;
            })
        };

        // A waiter cancelled while the flight runs.
        let running = cache.flight(&path).await;
        let waiter = wait();
        tokio::task::yield_now().await;
        waiter.abort();
        assert!(waiter.await.is_err_and(|err| err.is_cancelled()));
        drop(running);
        assert_eq!(testing::flights(&cache), 0);

        // A waiter cancelled after the flight ended, before it took its turn.
        let running = cache.flight(&path).await;
        let waiter = wait();
        tokio::task::yield_now().await;
        drop(running);
        waiter.abort();
        assert!(waiter.await.is_err_and(|err| err.is_cancelled()));
        assert_eq!(testing::flights(&cache), 0);
    }

    #[test]
    fn only_hashes_name_cover_directories() {
        let cache = testing::cache(Path::new("/cache"), 1024);
        let variant =
            Variant::of(&source(600, 600), &options(None, None, Some(64), None)).expect("variant");

        assert_eq!(
            cache.path(HASH, &variant),
            Some(PathBuf::from(format!("/cache/{HASH}/64x0-q90.jpg")))
        );
        for hash in ["", "..", "../x", "a/b", "abc.def"] {
            assert_eq!(cache.path(hash, &variant), None, "{hash}");
        }
    }
}
