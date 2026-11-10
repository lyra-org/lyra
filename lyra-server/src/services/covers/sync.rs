// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    path::{
        Path,
        PathBuf,
    },
    sync::Arc,
};

use agdb::DbId;
use anyhow::{
    Context,
    Result,
    anyhow,
};
use lyra_db::nanoid;
use rayon::iter::{
    IntoParallelIterator,
    ParallelIterator,
};
use tokio::{
    fs,
    io::AsyncWriteExt,
    sync::Semaphore,
    task::JoinSet,
};

use tokio_util::sync::CancellationToken;

use crate::services::libraries::MediaRoots;
use crate::services::plugin_dispatch::{
    CallSource,
    Published,
};
use crate::services::providers::{
    active_providers,
    id_schemes,
    provider_registry,
    registry_for,
};
use lyra_db::{
    Artist,
    Cover,
    Release,
    Track,
    covers::CoverSource,
};

use super::{
    CoverPaths,
    CoverSyncOptions,
    CoverUpdate,
    folders::AudioFolders,
    image::{
        MAX_COVER_BYTES,
        cover_blurhash,
        cover_mime_from_path,
        read_cover_hash,
        validate_cover_image,
    },
    providers::{
        EntityType,
        artist_context_value,
        cover_external_ids,
        library_for_release,
        release_context_value,
        resolve_provider_artist_cover_url,
        resolve_provider_release_cover_url,
    },
    resolve::{
        CoverDirs,
        any_cover_file,
        configured_cover_dir_for_artist,
        configured_cover_dir_for_release,
        cover_dirs_for_release,
        covers_locked,
        has_stored_cover,
        search_cover_files,
        search_local_cover,
        search_local_covers,
        stored_cover_files,
    },
    stale::{
        StoredCover,
        confirmed_stale,
        lists_fully,
    },
};

/// Downloads the cover at `url` that the provider's cover handler for
/// `entity_type` returned, on behalf of the plugin that still owns that
/// handler, with the extension its bytes call for.
///
/// Registering a cover handler is the `harmony.http` scope gate, so the
/// download needs no scope check of its own. The owner is looked up here,
/// not when the URL was produced, so that URLs queued by a batch sync are
/// skipped once their plugin unloads or loses the handler.
pub(super) async fn download_cover(
    provider_id: &str,
    entity_type: EntityType,
    url: &str,
) -> Result<(Vec<u8>, &'static str)> {
    let plugin_id = provider_registry()
        .read()
        .await
        .cover_handler_owner(provider_id, entity_type)
        .cloned()
        .ok_or_else(|| {
            anyhow!(
                "provider '{provider_id}' no longer has a {} cover handler",
                entity_type.as_str()
            )
        })?;
    let request = harmony_http::Request::get(url).max_body_bytes(MAX_COVER_BYTES);
    let response = harmony_http::request_as(plugin_id.as_str(), request)
        .await
        .with_context(|| format!("failed to download cover from {url}"))?;
    if !(200..300).contains(&response.status) {
        return Err(anyhow!(
            "cover download failed with status {} for {url}",
            response.status
        ));
    }
    let (bytes, extension) = tokio::task::spawn_blocking(move || {
        validate_cover_image(&response.body).map(|ext| (response.body, ext))
    })
    .await
    .context("cover validation task panicked")?
    .with_context(|| format!("cover from {url} is not a usable image"))?;
    Ok((bytes, extension))
}

async fn write_cover_image(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create cover directory {}", parent.display()))?;
    }

    let temp = path.with_file_name(format!(".lyra-cover-tmp-{}", nanoid()));
    let written = async {
        let mut file = fs::File::create(&temp)
            .await
            .with_context(|| format!("failed to create temporary cover file {}", temp.display()))?;
        file.write_all(bytes)
            .await
            .with_context(|| format!("failed to write temporary cover file {}", temp.display()))?;
        file.flush().await?;
        fs::rename(&temp, path)
            .await
            .with_context(|| format!("failed to finalize cover file {}", path.display()))
    }
    .await;
    if written.is_err() {
        let _ = fs::remove_file(&temp).await;
    }
    written
}

/// Deletes a cover file Lyra wrote. Only a regular file whose directory, resolved on disk, lies
/// inside the covers root is deleted, so library files and anything a symlink leads to are never
/// touched. A missing path has nothing to delete.
fn discard_cover_file(covers_root: &Path, path: &str) {
    let path = Path::new(path);
    let (Some(dir), Some(name)) = (path.parent(), path.file_name()) else {
        return;
    };
    let (Ok(root), Ok(dir)) = (
        std::fs::canonicalize(covers_root),
        std::fs::canonicalize(dir),
    ) else {
        return;
    };
    if !dir.starts_with(&root) {
        return;
    }
    let target = dir.join(name);
    if !std::fs::symlink_metadata(&target).is_ok_and(|meta| meta.file_type().is_file()) {
        return;
    }
    if let Err(err) = std::fs::remove_file(&target)
        && err.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(path = %target.display(), error = %err, "failed to remove replaced cover file");
    }
}

/// Discards a cover no owner refers to anymore: its file under the covers root, and its
/// transformed variants.
pub(crate) fn discard_cover(covers_root: &Path, cover: &Cover) {
    discard_cover_file(covers_root, &cover.path);
    super::forget_cover_transforms(&cover.hash);
}

/// Discards the files of covers detached inside a transaction, once that transaction is over
/// and only if it committed: a cover whose record survived, as after a rollback, keeps its file.
/// Runs in the background, since the caller may still hold the database write guard.
pub(crate) fn discard_unreferenced_cover_files(covers: Vec<Cover>) {
    if covers.is_empty() {
        return;
    }
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let db = crate::services::STATE.db.get();
    let covers_root = super::configured_covers_root();
    runtime.spawn(async move {
        let unreferenced = {
            let db = db.read().await;
            covers
                .into_iter()
                .filter(|cover| {
                    lyra_db::covers::get_by_public_id(&*db, &cover.id)
                        .is_ok_and(|found| found.is_none())
                })
                .collect::<Vec<_>>()
        };
        for cover in unreferenced {
            discard_cover(&covers_root, &cover);
        }
    });
}

/// A new image file under the covers root, not yet recorded as anyone's cover. Dropped before
/// it is recorded, it deletes its file. Every stored file gets its own name, so a replaced cover
/// never changes under another owner.
pub(super) struct PreparedCover {
    source: CoverSource,
    pending: Option<Cover>,
}

impl PreparedCover {
    pub(super) async fn write(
        dir: &Path,
        source: CoverSource,
        ext: &str,
        bytes: &[u8],
        provider_id: Option<&str>,
    ) -> Result<Self> {
        let target = dir.join(format!("{}-{}.{ext}", source.as_str(), nanoid()));
        write_cover_image(&target, bytes).await?;
        let mut prepared = Self {
            source,
            pending: Some(Cover {
                db_id: None,
                id: nanoid(),
                path: target.to_string_lossy().into_owned(),
                mime_type: String::new(),
                hash: String::new(),
                blurhash: None,
                provider_id: provider_id.map(str::to_string),
            }),
        };
        let (hash, blurhash, mime_type) = cover_file_metadata(&target).await?;
        if let Some(cover) = prepared.pending.as_mut() {
            cover.hash = hash;
            cover.blurhash = blurhash;
            cover.mime_type = mime_type;
        }
        Ok(prepared)
    }

    /// Records the file as the owner's cover from its source, under the write guard in which the
    /// caller confirmed the write still applies, and discards the file of the cover it replaces
    /// once nothing refers to it.
    pub(super) fn record(
        mut self,
        db: &mut agdb::DbAny,
        covers_root: &Path,
        owner_id: DbId,
        select: bool,
    ) -> Result<Cover> {
        let cover = self
            .pending
            .clone()
            .ok_or_else(|| anyhow!("prepared cover already recorded"))?;
        let source = self.source;
        let upserted = db.transaction_mut(|t| {
            let upserted = lyra_db::covers::upsert(t, owner_id, source, cover)?;
            if select {
                lyra_db::covers::select(t, owner_id, Some(source))?;
            }
            anyhow::Ok(upserted)
        })?;
        self.pending = None;
        if let Some(replaced) = upserted.replaced
            && replaced.orphaned
        {
            discard_cover(covers_root, &replaced.cover);
        }
        Ok(upserted.cover)
    }
}

impl Drop for PreparedCover {
    fn drop(&mut self) {
        if let Some(cover) = self.pending.take() {
            let _ = std::fs::remove_file(cover.path);
        }
    }
}

/// What an automatic provider sync saw when it decided to fetch a cover, checked again under the
/// write guard before the download is recorded.
#[derive(Clone)]
struct ProviderSyncBasis {
    options: CoverSyncOptions,
    owner_id: DbId,
    owner_public_id: String,
    /// The stored provider cover's file, which a user's choice made meanwhile would change.
    provider_cover: Option<String>,
    /// A stored local cover that didn't stand in for a cover when the sync decided to fetch.
    discounted_local: Option<DiscountedLocal>,
}

impl ProviderSyncBasis {
    fn discounting_local(mut self, discounted_local: Option<DiscountedLocal>) -> Self {
        self.discounted_local = discounted_local;
        self
    }
}

/// A local cover rejected when a provider sync started, with what it takes to judge it again once
/// the download is in, as a merge meanwhile can make it the release's after all.
#[derive(Clone)]
struct DiscountedLocal {
    path: PathBuf,
    folders: Arc<AudioFolders>,
    library_root: Option<PathBuf>,
}

impl DiscountedLocal {
    fn of(
        path: Option<PathBuf>,
        folders: &Arc<AudioFolders>,
        library_root: Option<&Path>,
    ) -> Option<Self> {
        path.map(|path| Self {
            path,
            folders: folders.clone(),
            library_root: library_root.map(Path::to_path_buf),
        })
    }

    fn still_rejected(&self, db: &agdb::DbAny, owner_id: DbId) -> Result<bool> {
        let tracks = lyra_db::tracks::get_direct(db, owner_id)?;
        let dirs = cover_dirs_for_release(
            db,
            &self.folders,
            owner_id,
            &tracks,
            self.library_root.as_deref(),
        )?;
        Ok(dirs.rejects(&self.folders, &self.path))
    }
}

/// The basis on which an automatic provider sync may fetch a cover for the owner: never for a
/// locked owner or when skipping covers, and otherwise only when it has no cover at all, as
/// `has_cover` tells from a look at the files, unless the stored provider cover is to be replaced.
fn provider_sync_basis(
    db: &agdb::DbAny,
    owner_id: DbId,
    owner_public_id: &str,
    has_cover: bool,
    options: CoverSyncOptions,
) -> Result<Option<ProviderSyncBasis>> {
    if covers_locked(db, owner_id)? {
        return Ok(None);
    }
    match options.update {
        CoverUpdate::Skip => return Ok(None),
        CoverUpdate::Missing if has_cover => return Ok(None),
        CoverUpdate::Missing | CoverUpdate::Replace => {}
    }
    Ok(Some(ProviderSyncBasis {
        options,
        owner_id,
        owner_public_id: owner_public_id.to_string(),
        provider_cover: lyra_db::covers::get_source(db, owner_id, CoverSource::Provider)?
            .map(|cover| cover.path),
        discounted_local: None,
    }))
}

/// Whether an automatic provider sync would fetch a cover for the artist now: it is unlocked,
/// and the options replace its cover or it has none.
pub(crate) async fn artist_cover_wanted(
    db: &lyra_db::DbAsync,
    artist_id: DbId,
    artist_public_id: &str,
    options: CoverSyncOptions,
) -> Result<bool> {
    let files = stored_cover_files(&*db.read().await, artist_id, None)?;
    let has_cover = search_cover_files(files).await?;
    let db_read = db.read().await;
    Ok(provider_sync_basis(&db_read, artist_id, artist_public_id, has_cover, options)?.is_some())
}

/// Whether the basis still holds: the same entity, still unlocked, with no cover arrived
/// meanwhile unless replacing, and the provider cover as the sync found it.
fn provider_sync_still_allowed(db: &agdb::DbAny, basis: &ProviderSyncBasis) -> Result<bool> {
    if lyra_db::lookup::find_node_id_by_id(db, &basis.owner_public_id)? != Some(basis.owner_id)
        || covers_locked(db, basis.owner_id)?
    {
        return Ok(false);
    }
    // Under the write guard, since a cover arriving during the download must win.
    if basis.options.update != CoverUpdate::Replace {
        let rejected_local = match &basis.discounted_local {
            Some(discounted) if discounted.still_rejected(db, basis.owner_id)? => {
                Some(discounted.path.as_path())
            }
            _ => None,
        };
        if has_stored_cover(db, basis.owner_id, rejected_local)? {
            return Ok(false);
        }
    }
    let provider_cover = lyra_db::covers::get_source(db, basis.owner_id, CoverSource::Provider)?
        .map(|cover| cover.path);
    Ok(provider_cover == basis.provider_cover)
}

/// Records a downloaded provider cover when the sync's basis still holds. Returns the stored
/// file, or none when the entity changed meanwhile.
async fn save_provider_cover(
    db: &lyra_db::DbAsync,
    covers_root: &Path,
    basis: &ProviderSyncBasis,
    target_dir: &Path,
    provider_id: &str,
    ext: &str,
    bytes: &[u8],
) -> Result<Option<PathBuf>> {
    let prepared = PreparedCover::write(
        target_dir,
        CoverSource::Provider,
        ext,
        bytes,
        Some(provider_id),
    )
    .await?;
    let mut db_write = db.write().await;
    if !provider_sync_still_allowed(&db_write, basis)? {
        return Ok(None);
    }
    let cover = prepared.record(&mut db_write, covers_root, basis.owner_id, false)?;
    Ok(Some(PathBuf::from(cover.path)))
}

/// The release a cover sync is for, with the tracks and artists it reads around it.
pub(crate) struct ReleaseCoverSubject<'a> {
    pub(crate) release: &'a Release,
    pub(crate) tracks: &'a [Track],
    pub(crate) artists: &'a [Artist],
}

pub(crate) async fn sync_release_cover_for_tracks<S: CallSource>(
    source: &S,
    db: &lyra_db::DbAsync,
    folders: &Arc<AudioFolders>,
    subject: ReleaseCoverSubject<'_>,
    paths: CoverPaths<'_>,
    options: CoverSyncOptions,
) -> Result<bool> {
    let ReleaseCoverSubject {
        release,
        tracks,
        artists,
    } = subject;
    let Some(release_id) = release.db_id.clone().map(Into::<DbId>::into) else {
        return Ok(false);
    };

    let schemes = registry_for(source).await.read().await.id_schemes();
    let (dirs, files, rejected_local, roots) = {
        let db_read = db.read().await;
        let dirs =
            cover_dirs_for_release(&db_read, folders, release_id, tracks, paths.library_root)?;
        let rejected_local = rejected_local_file(&*db_read, release_id, &dirs, folders)?;
        let files = stored_cover_files(&*db_read, release_id, rejected_local.as_deref())?;
        (
            dirs.owned,
            files,
            rejected_local,
            MediaRoots::for_principal(&*db_read, None)?.for_covers(),
        )
    };
    let has_cover =
        search_local_cover(dirs, roots).await?.is_some() || search_cover_files(files).await?;
    let (basis, provider_contexts) = {
        let db_read = db.read().await;
        let sorted_providers = active_providers(&db_read)?;
        let library = library_for_release(&db_read, release_id)?;
        let external_ids = cover_external_ids(&db_read, release_id, &schemes)?;
        let basis = provider_sync_basis(&db_read, release_id, &release.id, has_cover, options)?
            .map(|basis| {
                basis.discounting_local(DiscountedLocal::of(
                    rejected_local,
                    folders,
                    paths.library_root,
                ))
            });

        let release_context =
            release_context_value(release, tracks, artists, &external_ids, library.as_ref())?;
        let provider_contexts = sorted_providers
            .into_iter()
            .map(|provider| (provider.provider_id, release_context.clone()))
            .collect::<Vec<_>>();

        (basis, provider_contexts)
    };

    let Some(basis) = basis.filter(|_| !provider_contexts.is_empty()) else {
        return Ok(false);
    };

    let target_dir = configured_cover_dir_for_release(paths.covers_root, release_id);

    for (provider_id, release_context) in provider_contexts {
        let cover_url = match resolve_provider_release_cover_url(
            source,
            &provider_id,
            &release_context,
            options.bypass_cache,
        )
        .await
        {
            Ok(Some(url)) => url,
            Ok(None) => continue,
            Err(err) => {
                tracing::warn!(
                    provider = %provider_id,
                    error = %err,
                    "provider cover lookup failed while syncing release cover"
                );
                continue;
            }
        };

        let (bytes, ext) = match download_cover(&provider_id, EntityType::Release, &cover_url).await
        {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(
                    provider = %provider_id,
                    cover_url = %cover_url,
                    error = %err,
                    "failed to download cover"
                );
                continue;
            }
        };

        let Some(target) = save_provider_cover(
            db,
            paths.covers_root,
            &basis,
            &target_dir,
            &provider_id,
            ext,
            &bytes,
        )
        .await?
        else {
            return Ok(false);
        };
        tracing::info!(
            provider = %provider_id,
            release = %release.release_title,
            cover_path = %target.display(),
            "synced release cover from provider cover handler"
        );
        return Ok(true);
    }

    Ok(false)
}

pub(crate) async fn sync_artist_cover<S: CallSource>(
    source: &S,
    db: &lyra_db::DbAsync,
    artist: &Artist,
    paths: CoverPaths<'_>,
    options: CoverSyncOptions,
    provider_filter: Option<&str>,
) -> Result<bool> {
    let Some(artist_id) = artist.db_id.clone().map(Into::<DbId>::into) else {
        return Ok(false);
    };

    let schemes = registry_for(source).await.read().await.id_schemes();
    let files = stored_cover_files(&*db.read().await, artist_id, None)?;
    let has_cover = search_cover_files(files).await?;
    let (basis, provider_contexts) = {
        let db_read = db.read().await;
        let sorted_providers = active_providers(&db_read)?.only(provider_filter);
        let basis = provider_sync_basis(&db_read, artist_id, &artist.id, has_cover, options)?;
        let artist_context =
            artist_context_value(artist, &cover_external_ids(&db_read, artist_id, &schemes)?)?;
        let provider_contexts = sorted_providers
            .into_iter()
            .map(|provider| (provider.provider_id, artist_context.clone()))
            .collect::<Vec<_>>();

        (basis, provider_contexts)
    };

    let Some(basis) = basis.filter(|_| !provider_contexts.is_empty()) else {
        return Ok(false);
    };

    let target_dir = configured_cover_dir_for_artist(paths.covers_root, artist_id);

    for (provider_id, artist_context) in provider_contexts {
        let cover_url = match resolve_provider_artist_cover_url(
            source,
            &provider_id,
            &artist_context,
            options.bypass_cache,
        )
        .await
        {
            Ok(Some(url)) => url,
            Ok(None) => continue,
            Err(err) => {
                tracing::warn!(
                    provider = %provider_id,
                    error = %err,
                    "provider cover lookup failed while syncing artist cover"
                );
                continue;
            }
        };

        let (bytes, ext) = match download_cover(&provider_id, EntityType::Artist, &cover_url).await
        {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(
                    provider = %provider_id,
                    cover_url = %cover_url,
                    error = %err,
                    "failed to download cover"
                );
                continue;
            }
        };

        let Some(target) = save_provider_cover(
            db,
            paths.covers_root,
            &basis,
            &target_dir,
            &provider_id,
            ext,
            &bytes,
        )
        .await?
        else {
            return Ok(false);
        };
        tracing::info!(
            provider = %provider_id,
            artist = %artist.artist_name,
            cover_path = %target.display(),
            "synced artist cover from provider cover handler"
        );
        return Ok(true);
    }

    Ok(false)
}

struct CoverResolveContext {
    basis: ProviderSyncBasis,
    release_title: String,
    target_dir: PathBuf,
    provider_contexts: Vec<(String, serde_json::Value)>,
}

struct CoverDownloadTask {
    basis: ProviderSyncBasis,
    release_title: String,
    provider_id: String,
    cover_url: String,
    covers_root: PathBuf,
    target_dir: PathBuf,
}

async fn execute_cover_download(db: lyra_db::DbAsync, task: CoverDownloadTask) -> bool {
    let (bytes, ext) =
        match download_cover(&task.provider_id, EntityType::Release, &task.cover_url).await {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(
                    provider = %task.provider_id,
                    cover_url = %task.cover_url,
                    error = %err,
                    "failed to download cover"
                );
                return false;
            }
        };

    let target = match save_provider_cover(
        &db,
        &task.covers_root,
        &task.basis,
        &task.target_dir,
        &task.provider_id,
        ext,
        &bytes,
    )
    .await
    {
        Ok(Some(target)) => target,
        Ok(None) => return false,
        Err(err) => {
            tracing::warn!(
                provider = %task.provider_id,
                cover_url = %task.cover_url,
                error = %err,
                "failed to save cover image"
            );
            return false;
        }
    };

    tracing::info!(
        provider = %task.provider_id,
        release = %task.release_title,
        cover_path = %target.display(),
        "synced release cover from provider cover handler"
    );
    true
}

/// A release's local cover as a search found it, identified by a public id as well, since by the
/// time it is recorded the database id can name another node.
struct FoundCover {
    owner_id: DbId,
    owner_public_id: String,
    path: PathBuf,
    /// The stored local cover this replaces because the folders reject it, whose replacing takes
    /// the same evidence as dropping it would.
    replaces: Option<RejectedLocalCover>,
}

/// Which of a library's releases a local cover search looks at.
#[derive(Clone, Copy, PartialEq, Eq)]
enum LocalCoverScope {
    Every,
    /// Only those whose stored local cover the folders reject, sparing every other cover a read
    /// of its image.
    Rejected,
}

/// How many releases a library's local cover pass judges under one read guard, so that writers
/// get in between.
const LOCAL_COVER_BATCH: usize = 64;

/// What searching a library's releases for local covers came to.
struct ResolvedCovers {
    found: Vec<FoundCover>,
    /// Stored local covers of releases where nothing replaced them.
    rejected: Vec<RejectedLocalCover>,
    /// The errors of the searches that failed.
    failed: Vec<anyhow::Error>,
    folders: AudioFolders,
}

/// A stored local cover that the folder rule no longer allows, with the folders that must list
/// before it is dropped.
struct RejectedLocalCover {
    stored: StoredCover,
    evidence: Vec<PathBuf>,
}

/// The file of the release's stored local cover, if `dirs` rejects it.
fn rejected_local_file(
    db: &impl lyra_db::DbAccess,
    release_id: DbId,
    dirs: &CoverDirs,
    folders: &AudioFolders,
) -> Result<Option<PathBuf>> {
    Ok(
        lyra_db::covers::get_source(db, release_id, CoverSource::Local)?
            .map(|cover| PathBuf::from(cover.path))
            .filter(|path| dirs.rejects(folders, path)),
    )
}

impl RejectedLocalCover {
    /// The release's stored local cover, if `dirs` rejects it.
    fn find(
        db: &impl lyra_db::DbAccess,
        release_id: DbId,
        dirs: &CoverDirs,
        folders: &AudioFolders,
    ) -> Result<Option<Self>> {
        Ok(StoredCover::candidates(db, release_id)?
            .into_iter()
            .find(|stored| stored.source() == CoverSource::Local)
            .filter(|stored| dirs.rejects(folders, stored.path()))
            .map(|stored| Self {
                evidence: dirs.evidence_for(stored.path()),
                stored,
            }))
    }

    /// Whether every folder the rejection rests on lists, read off the async threads.
    fn evidence_lists(&self) -> bool {
        self.evidence.iter().all(|dir| lists_fully(dir))
    }

    /// The release's folders as they are now, if the record may still be acted on: the same
    /// cover, an unlocked release, still rejected, and resting on no folder that wasn't listed.
    /// To be called under the write guard.
    fn still_stands(
        &self,
        db: &agdb::DbAny,
        folders: &AudioFolders,
        library_root: Option<&Path>,
    ) -> Result<Option<CoverDirs>> {
        let owner_id = self.stored.owner_db_id();
        if self.stored.current(db)?.is_none() || covers_locked(db, owner_id)? {
            return Ok(None);
        }
        let tracks = lyra_db::tracks::get_direct(db, owner_id)?;
        let dirs = cover_dirs_for_release(db, folders, owner_id, &tracks, library_root)?;
        let path = self.stored.path();
        let listed = dirs
            .evidence_for(path)
            .iter()
            .all(|dir| self.evidence.contains(dir));
        Ok((listed && dirs.rejects(folders, path)).then_some(dirs))
    }
}

/// Whether automatic local cover writes may touch the owner read as `owner_public_id`: it is the
/// same entity and unlocked.
fn local_cover_writable(
    db: &impl lyra_db::DbAccess,
    owner_id: DbId,
    owner_public_id: &str,
) -> Result<bool> {
    Ok(
        lyra_db::lookup::find_id_by_db_id(db, owner_id)?.as_deref() == Some(owner_public_id)
            && !covers_locked(db, owner_id)?,
    )
}

/// The local covers of the library's unlocked releases, as far as `scope` goes.
async fn resolve_local_covers_for_library(
    db: &lyra_db::DbAsync,
    library_id: DbId,
    library_root: Option<&Path>,
    scope: LocalCoverScope,
) -> Result<ResolvedCovers> {
    let (mut searches, mut failed) = (Vec::new(), Vec::new());
    let (releases, folders, roots) = {
        let db_read = db.read().await;
        (
            lyra_db::releases::get(&db_read, library_id)?,
            AudioFolders::of_library(&*db_read, library_id)?,
            MediaRoots::for_principal(&*db_read, None)?.for_covers(),
        )
    };
    for batch in releases.chunks(LOCAL_COVER_BATCH) {
        let db_read = db.read().await;
        for release in batch {
            let Some(release_id) = release.db_id.clone().map(Into::<DbId>::into) else {
                continue;
            };
            if release.locked.unwrap_or(false)
                || lyra_db::lookup::find_id_by_db_id(&*db_read, release_id)?.as_deref()
                    != Some(release.id.as_str())
                || (scope == LocalCoverScope::Rejected
                    && lyra_db::covers::get_source(&*db_read, release_id, CoverSource::Local)?
                        .is_none())
            {
                continue;
            }
            let tracks = lyra_db::tracks::get_direct(&db_read, release_id)?;
            match cover_dirs_for_release(&db_read, &folders, release_id, &tracks, library_root) {
                Ok(dirs) => {
                    let rejected =
                        RejectedLocalCover::find(&*db_read, release_id, &dirs, &folders)?;
                    if scope == LocalCoverScope::Every || rejected.is_some() {
                        searches.push(((release_id, release.id.clone(), rejected), dirs.owned));
                    }
                }
                Err(err) => failed.push(err),
            }
        }
    }
    let mut resolved = ResolvedCovers {
        found: Vec::new(),
        rejected: Vec::new(),
        failed,
        folders,
    };
    for ((owner_id, owner_public_id, rejected), found) in
        search_local_covers(searches, roots).await?
    {
        match found {
            Ok(Some(path)) => resolved.found.push(FoundCover {
                owner_id,
                owner_public_id,
                path,
                replaces: rejected,
            }),
            Ok(None) => resolved.rejected.extend(rejected),
            Err(err) => resolved.failed.push(err),
        }
    }
    Ok(resolved)
}

/// Records the local covers `resolved` found and drops those it rejected, returning how many it
/// recorded, or an error naming the searches that failed once the rest are done.
async fn persist_resolved_covers(
    db: &lyra_db::DbAsync,
    library_id: DbId,
    library_root: Option<&Path>,
    resolved: ResolvedCovers,
) -> Result<usize> {
    let ResolvedCovers {
        found,
        rejected,
        failed: mut unresolved,
        folders,
    } = resolved;
    let persisted = if found.is_empty() {
        Ok(0)
    } else {
        sync_local_cover_metadata_from_resolved(db, found, &folders, library_root).await
    };
    match drop_rejected_local_covers(db, rejected, &folders, library_root).await {
        Ok(removed) => tracing::debug!(
            library_id = library_id.0,
            removed,
            "dropped local covers from folders that aren't their release's"
        ),
        Err(err) => unresolved.push(err),
    }
    match (persisted, unresolved.first()) {
        (persisted, None) => persisted,
        (persisted, Some(first)) => {
            let unresolved = anyhow::anyhow!(
                "{} release covers could not be resolved; first: {first:#}",
                unresolved.len()
            );
            Err(match persisted {
                Ok(_) => unresolved,
                Err(err) => err.context(unresolved),
            })
        }
    }
}

/// Checks the library's stored local covers against who its folders belong to now, as a scan's
/// last step, since a scan passes over releases whose audio is unchanged. A cover that no longer
/// passes gives way to one in the release's own folders, or is dropped. Returns how many were
/// replaced.
pub(crate) async fn revalidate_local_covers_for_library(
    db: &lyra_db::DbAsync,
    library_id: DbId,
    library_root: Option<&Path>,
) -> Result<usize> {
    let resolved =
        resolve_local_covers_for_library(db, library_id, library_root, LocalCoverScope::Rejected)
            .await?;
    persist_resolved_covers(db, library_id, library_root, resolved).await
}

/// Drops the stored local covers `rejected` names, provided the folders that decide it all list,
/// and returns how many it removed. A cover changed since it was read, an owner locked since, or
/// one whose folders turned out to be the release's after all or now include one not listed keeps
/// its record. Only the record goes: the image is a file of the library's.
async fn drop_rejected_local_covers(
    db: &lyra_db::DbAsync,
    rejected: Vec<RejectedLocalCover>,
    folders: &AudioFolders,
    library_root: Option<&Path>,
) -> Result<usize> {
    if rejected.is_empty() {
        return Ok(0);
    }
    let listed = tokio::task::spawn_blocking(move || {
        rejected
            .into_par_iter()
            .filter(RejectedLocalCover::evidence_lists)
            .collect::<Vec<_>>()
    })
    .await
    .context("cover folder listing task panicked")?;

    let mut db_write = db.write().await;
    let mut removed = 0;
    let mut orphaned = Vec::new();
    for rejected in listed {
        if rejected
            .still_stands(&db_write, folders, library_root)?
            .is_none()
        {
            continue;
        }
        let owner_id = rejected.stored.owner_db_id();
        let detached = db_write
            .transaction_mut(|t| lyra_db::covers::remove(t, owner_id, CoverSource::Local))?;
        if let Some(detached) = detached {
            removed += 1;
            if detached.orphaned {
                orphaned.push(detached.cover.hash);
            }
        }
    }
    drop(db_write);
    for hash in orphaned {
        super::forget_cover_transforms(&hash);
    }
    Ok(removed)
}

/// A cancelled pass stops early and returns what it did so far, leaving the caller to check.
pub(crate) async fn sync_release_covers_for_library(
    db: &lyra_db::DbAsync,
    paths: CoverPaths<'_>,
    library_id: DbId,
    options: CoverSyncOptions,
    provider_filter: Option<&str>,
    cancel: &CancellationToken,
) -> Result<usize> {
    let schemes = id_schemes().await;
    let (sorted_providers, searches, roots, folders) = {
        let db_read = db.read().await;
        let sorted_providers = active_providers(&db_read)?.only(provider_filter);
        if sorted_providers.is_empty() {
            return Ok(0);
        }

        let folders = Arc::new(AudioFolders::of_library(&*db_read, library_id)?);
        let mut searches = Vec::new();
        for release in lyra_db::releases::get(&db_read, library_id)? {
            let Some(release_id) = release.db_id.map(Into::<DbId>::into) else {
                continue;
            };

            let tracks = lyra_db::tracks::get_direct(&db_read, release_id)?;
            // The metadata pass after this one reports a cover search that fails.
            let Ok(dirs) =
                cover_dirs_for_release(&db_read, &folders, release_id, &tracks, paths.library_root)
            else {
                continue;
            };
            let rejected_local = rejected_local_file(&*db_read, release_id, &dirs, &folders)?;
            let files = stored_cover_files(&*db_read, release_id, rejected_local.as_deref())?;
            searches.push(((release_id, release.id, files, rejected_local), dirs.owned));
        }

        let roots = MediaRoots::for_principal(&*db_read, None)?.for_covers();
        (sorted_providers, searches, roots, folders)
    };
    let searched = search_local_covers(searches, roots).await?;
    let searched = tokio::task::spawn_blocking(move || {
        searched
            .into_par_iter()
            .filter_map(
                |((release_id, public_id, files, rejected_local), local_cover)| {
                    let local_cover = local_cover.ok()?;
                    let has_cover = local_cover.is_some() || any_cover_file(&files);
                    Some((release_id, public_id, has_cover, rejected_local))
                },
            )
            .collect::<Vec<_>>()
    })
    .await
    .context("cover file search task panicked")?;

    let resolve_contexts = {
        let db_read = db.read().await;
        let mut contexts = Vec::new();
        for (release_id, public_id, has_cover, rejected_local) in searched {
            let Some(release) = lyra_db::releases::get_by_id(&db_read, release_id)?
                .filter(|release| release.id == public_id)
            else {
                continue;
            };
            let Some(basis) =
                provider_sync_basis(&db_read, release_id, &release.id, has_cover, options)?.map(
                    |basis| {
                        basis.discounting_local(DiscountedLocal::of(
                            rejected_local,
                            &folders,
                            paths.library_root,
                        ))
                    },
                )
            else {
                continue;
            };

            let tracks = lyra_db::tracks::get_direct(&db_read, release_id)?;
            let artists = lyra_db::artists::get(&db_read, release_id)?;
            let library = library_for_release(&db_read, release_id)?;
            let target_dir = configured_cover_dir_for_release(paths.covers_root, release_id);
            let external_ids = cover_external_ids(&db_read, release_id, &schemes)?;
            let ctx = release_context_value(
                &release,
                &tracks,
                &artists,
                &external_ids,
                library.as_ref(),
            )?;
            let provider_contexts = sorted_providers
                .iter()
                .map(|provider| (provider.provider_id.clone(), ctx.clone()))
                .collect();

            contexts.push(CoverResolveContext {
                basis,
                release_title: release.release_title.clone(),
                target_dir,
                provider_contexts,
            });
        }

        contexts
    };

    if resolve_contexts.is_empty() {
        return Ok(0);
    }

    let mut downloads = Vec::new();
    for ctx in resolve_contexts {
        if cancel.is_cancelled() {
            return Ok(0);
        }
        for (provider_id, release_context) in &ctx.provider_contexts {
            let cover_url = match resolve_provider_release_cover_url(
                &Published::background(cancel.clone()),
                provider_id,
                release_context,
                options.bypass_cache,
            )
            .await
            {
                Ok(Some(url)) => url,
                Ok(None) => continue,
                Err(err) => {
                    tracing::warn!(
                        provider = %provider_id,
                        error = %err,
                        "provider cover lookup failed while syncing release cover"
                    );
                    continue;
                }
            };

            downloads.push(CoverDownloadTask {
                basis: ctx.basis.clone(),
                release_title: ctx.release_title.clone(),
                provider_id: provider_id.clone(),
                cover_url,
                covers_root: paths.covers_root.to_path_buf(),
                target_dir: ctx.target_dir.clone(),
            });
            break; // first provider with a URL wins
        }
    }

    if downloads.is_empty() {
        return Ok(0);
    }

    let concurrency = Arc::new(Semaphore::new(8));
    let mut tasks = JoinSet::new();

    for dl in downloads {
        let permit = tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            permit = concurrency.clone().acquire_owned() => {
                permit.map_err(|_| anyhow!("download semaphore closed"))?
            }
        };
        let db = db.clone();
        tasks.spawn(async move {
            let result = execute_cover_download(db, dl).await;
            drop(permit);
            result
        });
    }

    let mut synced = 0usize;
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(true) => synced += 1,
            Ok(false) => {}
            Err(err) => {
                tracing::warn!(error = %err, "cover download task panicked");
            }
        }
    }

    Ok(synced)
}

async fn cover_file_metadata(path: &Path) -> Result<(String, Option<String>, String)> {
    let path = path.to_path_buf();
    let (hash, blurhash, mime_type) = tokio::task::spawn_blocking(move || {
        (
            read_cover_hash(&path),
            cover_blurhash(&path),
            cover_mime_from_path(&path).to_string(),
        )
    })
    .await
    .context("cover metadata computation task panicked")?;
    Ok((hash?, blurhash, mime_type))
}

/// Records the image at `cover_path` as the local cover of the owner read as `owner_public_id`,
/// unless it was removed or locked since. Returns whether the record changed.
pub(super) async fn upsert_local_cover_metadata(
    db: &lyra_db::DbAsync,
    owner_id: DbId,
    owner_public_id: &str,
    cover_path: &Path,
) -> Result<bool> {
    if !cover_path.is_file() {
        return Ok(false);
    }

    let (hash, blurhash, mime_type) = cover_file_metadata(cover_path).await?;
    let path_str = cover_path.to_string_lossy().into_owned();
    let mut db_write = db.write().await;
    if !local_cover_writable(&*db_write, owner_id, owner_public_id)? {
        return Ok(false);
    }
    // An unchanged cover needs no mutable access, which would count as a catalog change.
    if let Some(existing) = lyra_db::covers::get_source(&*db_write, owner_id, CoverSource::Local)?
        && existing.hash == hash
        && existing.path == path_str
    {
        return Ok(false);
    }
    db_write.transaction_mut(|t| {
        let cover = Cover {
            db_id: None,
            id: nanoid(),
            path: path_str,
            mime_type,
            hash,
            blurhash,
            provider_id: None,
        };
        lyra_db::covers::upsert(t, owner_id, CoverSource::Local, cover)?;
        Ok(true)
    })
}

/// Records the release's library cover as its local cover, leaving a locked release alone. With
/// none to record, drops a stored local cover that isn't the release's, then removes its covers
/// from any source whose file is confirmed gone. Returns the cover found and whether its record
/// changed. Judging folders takes every album in them to have been applied, as a scan's have by
/// its artifact stage.
pub(crate) async fn record_local_release_cover(
    db: &lyra_db::DbAsync,
    folders: &AudioFolders,
    release_id: DbId,
    library_root: Option<&Path>,
) -> Result<Option<(PathBuf, bool)>> {
    let (public_id, dirs, rejected, roots) = {
        let db_read = db.read().await;
        let Some(release) = lyra_db::releases::get_by_id(&db_read, release_id)? else {
            return Ok(None);
        };
        if release.locked.unwrap_or(false) {
            return Ok(None);
        }
        let tracks = lyra_db::tracks::get_direct(&db_read, release_id)?;
        let dirs = cover_dirs_for_release(&db_read, folders, release_id, &tracks, library_root)?;
        let rejected = RejectedLocalCover::find(&*db_read, release_id, &dirs, folders)?;
        let roots = MediaRoots::for_principal(&*db_read, None)?.for_covers();
        (release.id, dirs.owned, rejected, roots)
    };
    let recorded = match search_local_cover(dirs, roots).await? {
        Some(cover_path) => {
            let changed = match rejected {
                Some(rejected) => {
                    let found = FoundCover {
                        owner_id: release_id,
                        owner_public_id: public_id,
                        path: cover_path.clone(),
                        replaces: Some(rejected),
                    };
                    sync_local_cover_metadata_from_resolved(db, vec![found], folders, library_root)
                        .await?
                        > 0
                }
                None => {
                    upsert_local_cover_metadata(db, release_id, &public_id, &cover_path).await?
                }
            };
            Some((cover_path, changed))
        }
        None => {
            drop_rejected_local_covers(db, rejected.into_iter().collect(), folders, library_root)
                .await?;
            None
        }
    };
    forget_gone_covers(db, release_id).await?;
    Ok(recorded)
}

/// Removes the owner's covers from every source whose file is confirmed gone, unless the owner is
/// locked, so it shows its next cover. Returns how many were removed.
pub(crate) async fn forget_gone_covers(db: &lyra_db::DbAsync, owner_id: DbId) -> Result<usize> {
    let candidates = {
        let db_read = db.read().await;
        if covers_locked(&db_read, owner_id)? {
            return Ok(0);
        }
        StoredCover::candidates(&*db_read, owner_id)?
    };
    let stale = confirmed_stale(candidates).await?;
    if stale.is_empty() {
        return Ok(0);
    }
    let mut db_write = db.write().await;
    // An owner locked since the read keeps its covers.
    if covers_locked(&db_write, owner_id)? {
        return Ok(0);
    }
    super::stale::remove_stale(&mut db_write, &stale)
}

/// A library's stored covers whose file is confirmed gone, as the sweep found them.
#[derive(Default)]
struct GoneCovers {
    /// Local covers, each with the library file found to replace it.
    replaced: Vec<(StoredCover, PathBuf)>,
    /// Those with no file to replace them.
    removed: Vec<StoredCover>,
    /// The errors of the searches that failed.
    failed: Vec<anyhow::Error>,
}

async fn find_gone_covers(
    db: &lyra_db::DbAsync,
    library_id: DbId,
    library_root: Option<&Path>,
) -> Result<GoneCovers> {
    let (release_covers, artist_covers) = {
        let db_read = db.read().await;
        // A missing release's or artist's cover is as likely gone with its files as replaced.
        let availability = crate::services::catalog::Availability::of(&db_read)?;
        let mut release_covers = Vec::new();
        for release in lyra_db::releases::get(&db_read, library_id)? {
            let Some(release_id) = release.db_id.map(DbId::from) else {
                continue;
            };
            if release.locked.unwrap_or(false) || !availability.release(release_id) {
                continue;
            }
            release_covers.extend(StoredCover::candidates(&*db_read, release_id)?);
        }
        let mut artist_covers = Vec::new();
        for artist in lyra_db::artists::get_by_library(&db_read, library_id)? {
            let Some(artist_id) = artist.db_id.map(DbId::from) else {
                continue;
            };
            if artist.locked.unwrap_or(false) || !availability.artist(artist_id) {
                continue;
            }
            artist_covers.extend(StoredCover::candidates(&*db_read, artist_id)?);
        }
        (release_covers, artist_covers)
    };

    let mut gone = GoneCovers {
        removed: confirmed_stale(artist_covers).await?,
        ..GoneCovers::default()
    };
    let (local, other): (Vec<_>, Vec<_>) = confirmed_stale(release_covers)
        .await?
        .into_iter()
        .partition(|stale| stale.source() == CoverSource::Local);
    gone.removed.extend(other);

    let mut searches = Vec::new();
    let roots = {
        let db_read = db.read().await;
        let folders = if local.is_empty() {
            AudioFolders::default()
        } else {
            AudioFolders::of_library(&*db_read, library_id)?
        };
        for stale in local {
            if stale.current(&*db_read)?.is_none() {
                continue;
            }
            let owner_id = stale.owner_db_id();
            let tracks = lyra_db::tracks::get_direct(&db_read, owner_id)?;
            match cover_dirs_for_release(&db_read, &folders, owner_id, &tracks, library_root) {
                Ok(dirs) => searches.push((stale, dirs.owned)),
                Err(err) => gone.failed.push(err),
            }
        }
        MediaRoots::for_principal(&*db_read, None)?.for_covers()
    };
    for (stale, found) in search_local_covers(searches, roots).await? {
        match found {
            Ok(Some(path)) => gone.replaced.push((stale, path)),
            Ok(None) => gone.removed.push(stale),
            Err(err) => gone.failed.push(err),
        }
    }
    Ok(gone)
}

/// Searches again for the covers of `library_id`'s releases and artists whose file is confirmed
/// gone, as a library sync's last step, since a sync passes over releases whose audio is
/// unchanged. A local cover found again in the library replaces the record; any other is removed,
/// so the owner shows its next cover. Locked owners, and covers changed since they were found gone,
/// are left alone, and failures are logged rather than stopping the rest. Returns how many covers
/// were removed.
pub(crate) async fn refresh_gone_covers_for_library(
    db: &lyra_db::DbAsync,
    library_id: DbId,
    library_root: Option<&Path>,
) -> Result<usize> {
    let gone = find_gone_covers(db, library_id, library_root).await?;
    Ok(apply_gone_covers(db, library_id, gone).await)
}

async fn apply_gone_covers(db: &lyra_db::DbAsync, library_id: DbId, gone: GoneCovers) -> usize {
    let GoneCovers {
        replaced,
        removed,
        mut failed,
    } = gone;
    let computed = tokio::task::spawn_blocking(move || {
        replaced
            .into_par_iter()
            .map(|(stale, path)| {
                let hash = read_cover_hash(&path)?;
                let cover = Cover {
                    db_id: None,
                    id: nanoid(),
                    mime_type: cover_mime_from_path(&path).to_string(),
                    blurhash: cover_blurhash(&path),
                    path: path.to_string_lossy().into_owned(),
                    hash,
                    provider_id: None,
                };
                Ok((stale, cover))
            })
            .collect::<Vec<Result<_>>>()
    })
    .await;
    let computed = match computed {
        Ok(computed) => computed,
        Err(err) => vec![Err(anyhow!("cover metadata task failed: {err}"))],
    };

    let mut db_write = db.write().await;
    for replacement in computed {
        let result = replacement.and_then(|(stale, cover)| {
            db_write.transaction_mut(|t| -> Result<()> {
                // A cover changed, or whose file came back, since the read keeps its record.
                let still_gone = stale
                    .current(t)?
                    .is_some_and(|current| super::stale::confirmed_gone(Path::new(&current.path)));
                if still_gone && !covers_locked(t, stale.owner_db_id())? {
                    lyra_db::covers::upsert(t, stale.owner_db_id(), CoverSource::Local, cover)?;
                }
                Ok(())
            })
        });
        if let Err(err) = result {
            failed.push(err);
        }
    }
    let mut removed_count = 0;
    for stale in &removed {
        let result = covers_locked(&db_write, stale.owner_db_id()).and_then(|locked| {
            if locked {
                return Ok(0);
            }
            super::stale::remove_stale(&mut db_write, std::slice::from_ref(stale))
        });
        match result {
            Ok(count) => removed_count += count,
            Err(err) => failed.push(err),
        }
    }
    drop(db_write);

    if let Some(first) = failed.first() {
        tracing::warn!(
            library_id = library_id.0,
            failed = failed.len(),
            error = %format_args!("{first:#}"),
            "covers whose file is gone could not all be searched for, replaced or removed"
        );
    }
    removed_count
}

/// The file and hash of a stored cover, to tell whether it changed since a read.
type StoredFile = Option<(String, String)>;

fn stored_local_file(db: &impl lyra_db::DbAccess, owner_id: DbId) -> Result<StoredFile> {
    Ok(
        lyra_db::covers::get_source(db, owner_id, CoverSource::Local)?
            .map(|cover| (cover.path, cover.hash)),
    )
}

struct PendingCover {
    found: FoundCover,
    stored: StoredFile,
}

struct ComputedCover {
    owner_id: DbId,
    owner_public_id: String,
    replaces: Option<RejectedLocalCover>,
    stored: StoredFile,
    path: String,
    mime_type: String,
    hash: String,
    blurhash: Option<String>,
}

async fn sync_local_cover_metadata_from_resolved(
    db: &lyra_db::DbAsync,
    resolved: Vec<FoundCover>,
    folders: &AudioFolders,
    library_root: Option<&Path>,
) -> Result<usize> {
    let pending = {
        let db_read = db.read().await;
        resolved
            .into_iter()
            .map(|found| {
                let stored = stored_local_file(&*db_read, found.owner_id)?;
                Ok(PendingCover { found, stored })
            })
            .collect::<Result<Vec<_>>>()?
    };

    let (computed, unreadable): (Vec<_>, Vec<_>) = tokio::task::spawn_blocking(move || {
        pending
            .into_par_iter()
            .filter_map(|PendingCover { found, stored }| {
                if found
                    .replaces
                    .as_ref()
                    .is_some_and(|rejected| !rejected.evidence_lists())
                {
                    return None;
                }
                let hash = match read_cover_hash(&found.path) {
                    Ok(hash) => hash,
                    Err(err) => return Some(Err(err)),
                };
                let path = found.path.to_string_lossy().into_owned();
                if stored
                    .as_ref()
                    .is_some_and(|known| known.0 == path && known.1 == hash)
                {
                    return None;
                }
                Some(Ok(ComputedCover {
                    owner_id: found.owner_id,
                    owner_public_id: found.owner_public_id,
                    replaces: found.replaces,
                    stored,
                    mime_type: cover_mime_from_path(&found.path).to_string(),
                    blurhash: cover_blurhash(&found.path),
                    path,
                    hash,
                }))
            })
            .partition_map(|computed: Result<ComputedCover>| match computed {
                Ok(computed) => rayon::iter::Either::Left(computed),
                Err(err) => rayon::iter::Either::Right(err),
            })
    })
    .await
    .context("cover metadata task panicked")?;
    if computed.is_empty() && unreadable.is_empty() {
        return Ok(0);
    }

    let mut db_write = db.write().await;
    let mut allowed = Vec::with_capacity(computed.len());
    for c in computed {
        if let Some(rejected) = &c.replaces {
            let Some(dirs) = rejected.still_stands(&db_write, folders, library_root)? else {
                continue;
            };
            if !Path::new(&c.path)
                .parent()
                .is_some_and(|dir| dirs.owns(dir))
            {
                continue;
            }
        }
        allowed.push(c);
    }
    let count = db_write.transaction_mut(|t| {
        let mut count = 0;
        for c in allowed {
            // A release removed or locked since the scan read it, or whose local cover changed,
            // keeps what it has.
            if !local_cover_writable(t, c.owner_id, &c.owner_public_id)?
                || stored_local_file(t, c.owner_id)? != c.stored
            {
                continue;
            }
            let cover = Cover {
                db_id: None,
                id: nanoid(),
                path: c.path,
                mime_type: c.mime_type,
                hash: c.hash,
                blurhash: c.blurhash,
                provider_id: None,
            };
            lyra_db::covers::upsert(t, c.owner_id, CoverSource::Local, cover)?;
            count += 1;
        }
        anyhow::Ok(count)
    })?;
    drop(db_write);

    tracing::info!(count, "persisted release cover metadata");
    if let Some(first) = unreadable.first() {
        anyhow::bail!(
            "{} release covers could not be read; first: {first:#}",
            unreadable.len()
        );
    }
    Ok(count)
}

/// Fetches provider covers for the library's releases that may get one, then records the local
/// covers found beside their audio files.
/// A cancelled pass stops early and returns what it did so far, leaving the caller to check.
pub(crate) async fn sync_and_persist_release_covers_for_library(
    db: &lyra_db::DbAsync,
    paths: CoverPaths<'_>,
    library_id: DbId,
    options: CoverSyncOptions,
    provider_filter: Option<&str>,
    cancel: &CancellationToken,
) -> Result<usize> {
    sync_release_covers_for_library(db, paths, library_id, options, provider_filter, cancel)
        .await?;
    if cancel.is_cancelled() {
        return Ok(0);
    }

    let resolved = resolve_local_covers_for_library(
        db,
        library_id,
        paths.library_root,
        LocalCoverScope::Every,
    )
    .await?;
    let persisted = persist_resolved_covers(db, library_id, paths.library_root, resolved).await?;
    tracing::debug!(
        library_id = library_id.0,
        persisted,
        "synced library release cover metadata"
    );
    Ok(persisted)
}

#[cfg(test)]
mod tests {
    use std::fs as std_fs;

    use super::*;
    use lyra_db::fixtures::insert_release;
    use lyra_db::test_db::new_test_db;

    const PNG_1X1: &[u8] = &[
        0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44,
        0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x04, 0x00, 0x00, 0x00, 0xB5,
        0x1C, 0x0C, 0x02, 0x00, 0x00, 0x00, 0x0B, 0x49, 0x44, 0x41, 0x54, 0x78, 0xDA, 0x63, 0xFC,
        0xFF, 0x1F, 0x00, 0x03, 0x03, 0x02, 0x00, 0xEF, 0xBF, 0x55, 0x3A, 0x00, 0x00, 0x00, 0x00,
        0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
    ];

    fn write_test_cover() -> anyhow::Result<PathBuf> {
        let path = std::env::temp_dir().join(format!("lyra-test-cover-{}.png", nanoid()));
        std_fs::write(&path, PNG_1X1)?;
        Ok(path)
    }

    #[tokio::test]
    async fn sync_release_cover_for_tracks_returns_false_without_providers() -> anyhow::Result<()> {
        let db = lyra_db::DbAsync::new(new_test_db()?);
        let release = Release {
            db_id: None,
            id: nanoid(),
            release_title: "Release".to_string(),
            sort_title: None,
            release_type: None,
            release_date: None,
            media_formats: None,
            barcode: None,
            description: None,
            locked: None,
            created_at_ms: None,
            file_created_at_ms: None,
        };

        let synced = sync_release_cover_for_tracks(
            &crate::services::plugin_dispatch::Published::interactive(),
            &db,
            &Arc::default(),
            ReleaseCoverSubject {
                release: &release,
                tracks: &[],
                artists: &[],
            },
            CoverPaths {
                library_root: None,
                covers_root: Path::new("covers"),
            },
            CoverSyncOptions::default(),
        )
        .await?;

        assert!(!synced);
        Ok(())
    }

    #[tokio::test]
    async fn download_cover_refuses_a_provider_without_a_registered_cover_handler()
    -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;

        let error = download_cover(
            "unregistered-cover-provider",
            EntityType::Release,
            "http://127.0.0.1:9/cover.png",
        )
        .await
        .expect_err("no plugin owns the handler the URL came from");

        assert!(
            error
                .to_string()
                .contains("no longer has a release cover handler"),
            "{error:#}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn recording_the_same_local_cover_twice_changes_nothing() -> anyhow::Result<()> {
        let db = lyra_db::DbAsync::new(new_test_db()?);
        let release_id = {
            let mut db_write = db.write().await;
            insert_release(&mut db_write, "Release")?
        };
        let cover_path = write_test_cover()?;

        let public_id = release_public_id(&db, release_id).await?;
        let inserted =
            upsert_local_cover_metadata(&db, release_id, &public_id, &cover_path).await?;
        let inserted_again =
            upsert_local_cover_metadata(&db, release_id, &public_id, &cover_path).await?;

        assert!(inserted);
        assert!(!inserted_again);

        let _ = std_fs::remove_file(cover_path);
        Ok(())
    }

    async fn release_public_id(db: &lyra_db::DbAsync, release_id: DbId) -> anyhow::Result<String> {
        Ok(lyra_db::releases::get_by_id(&*db.read().await, release_id)?
            .expect("release exists")
            .id)
    }

    async fn found_cover(
        db: &lyra_db::DbAsync,
        owner_id: DbId,
        path: &Path,
    ) -> anyhow::Result<FoundCover> {
        Ok(FoundCover {
            owner_id,
            owner_public_id: release_public_id(db, owner_id).await?,
            path: path.to_path_buf(),
            replaces: None,
        })
    }

    #[tokio::test]
    async fn resolved_covers_are_recorded_only_when_they_changed() -> anyhow::Result<()> {
        let db = lyra_db::DbAsync::new(new_test_db()?);
        let release_id = {
            let mut db_write = db.write().await;
            insert_release(&mut db_write, "Release")?
        };
        let root = tempfile::tempdir()?;
        let cover_path = root.path().join("cover.png");
        std_fs::write(&cover_path, PNG_1X1)?;
        let sync = || async {
            let found = found_cover(&db, release_id, &cover_path).await?;
            sync_local_cover_metadata_from_resolved(
                &db,
                vec![found],
                &AudioFolders::default(),
                None,
            )
            .await
        };

        assert_eq!(sync().await?, 1);
        let changes = db.stamp();
        assert_eq!(sync().await?, 0);
        assert_eq!(db.stamp(), changes, "an unchanged cover takes no write");

        std_fs::write(&cover_path, [PNG_1X1, b"changed"].concat())?;
        assert_eq!(sync().await?, 1);

        std_fs::remove_file(&cover_path)?;
        let err = sync()
            .await
            .expect_err("a file gone since it was found can't be read");
        assert!(err.to_string().contains("could not be read"), "{err:#}");
        Ok(())
    }

    #[tokio::test]
    async fn a_local_cover_stays_off_an_id_reused_since_the_read() -> anyhow::Result<()> {
        let db = lyra_db::DbAsync::new(new_test_db()?);
        let release_id = {
            let mut db_write = db.write().await;
            insert_release(&mut db_write, "Release")?
        };
        let root = tempfile::tempdir()?;
        let cover_path = root.path().join("cover.png");
        std_fs::write(&cover_path, PNG_1X1)?;
        let found = found_cover(&db, release_id, &cover_path).await?;
        let public_id = found.owner_public_id.clone();

        {
            let mut db_write = db.write().await;
            db_write.exec_mut(agdb::QueryBuilder::remove().ids(release_id).query())?;
            let reused = insert_release(&mut db_write, "Reused")?;
            assert_eq!(
                reused, release_id,
                "agdb hands the freed id to the next node"
            );
        }

        assert_eq!(
            sync_local_cover_metadata_from_resolved(
                &db,
                vec![found],
                &AudioFolders::default(),
                None
            )
            .await?,
            0
        );
        assert!(!upsert_local_cover_metadata(&db, release_id, &public_id, &cover_path).await?);
        assert!(
            lyra_db::covers::get(&*db.read().await, release_id)?.is_none(),
            "the new release took the old one's cover"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_release_locked_since_the_read_keeps_its_covers() -> anyhow::Result<()> {
        let db = lyra_db::DbAsync::new(new_test_db()?);
        let release_id = {
            let mut db_write = db.write().await;
            insert_release(&mut db_write, "Release")?
        };
        let root = tempfile::tempdir()?;
        let cover_path = root.path().join("cover.png");
        std_fs::write(&cover_path, PNG_1X1)?;
        let found = found_cover(&db, release_id, &cover_path).await?;
        let public_id = found.owner_public_id.clone();

        {
            let mut db_write = db.write().await;
            let mut release =
                lyra_db::releases::get_by_id(&*db_write, release_id)?.expect("release exists");
            release.locked = Some(true);
            db_write.transaction_mut(|t| lyra_db::releases::update_in_transaction(t, &release))?;
        }

        assert_eq!(
            sync_local_cover_metadata_from_resolved(
                &db,
                vec![found],
                &AudioFolders::default(),
                None
            )
            .await?,
            0
        );
        assert!(!upsert_local_cover_metadata(&db, release_id, &public_id, &cover_path).await?);
        assert!(lyra_db::covers::get(&*db.read().await, release_id)?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn a_provider_cover_is_kept_beside_the_library_cover() -> anyhow::Result<()> {
        let db = lyra_db::DbAsync::new(new_test_db()?);
        let release_id = {
            let mut db_write = db.write().await;
            insert_release(&mut db_write, "Release")?
        };
        let root = tempfile::tempdir()?;
        let library_cover = root
            .path()
            .join("library")
            .join("Release")
            .join("cover.png");
        std_fs::create_dir_all(library_cover.parent().unwrap())?;
        std_fs::write(&library_cover, PNG_1X1)?;
        let public_id = release_public_id(&db, release_id).await?;
        upsert_local_cover_metadata(&db, release_id, &public_id, &library_cover).await?;

        let covers_root = root.path().join("covers");
        let target_dir = configured_cover_dir_for_release(&covers_root, release_id);
        let replace = CoverSyncOptions {
            update: CoverUpdate::Replace,
            bypass_cache: false,
        };
        let mut saved = Vec::new();
        for _ in 0..2 {
            let basis = {
                let db_read = db.read().await;
                let public_id = lyra_db::releases::get_by_id(&*db_read, release_id)?
                    .expect("release exists")
                    .id;
                provider_sync_basis(&db_read, release_id, &public_id, false, replace)?
                    .expect("replacing is allowed")
            };
            saved.push(
                save_provider_cover(
                    &db,
                    &covers_root,
                    &basis,
                    &target_dir,
                    "provider",
                    "png",
                    PNG_1X1,
                )
                .await?
                .expect("the basis still holds"),
            );
        }
        let (first, second) = (&saved[0], &saved[1]);

        assert!(library_cover.is_file());
        assert!(!first.exists(), "the replaced provider file is discarded");
        assert!(second.is_file());
        let db_read = db.read().await;
        let local = lyra_db::covers::get_source(&*db_read, release_id, CoverSource::Local)?
            .expect("the local cover stays recorded");
        assert_eq!(PathBuf::from(local.path), library_cover);
        let provider = lyra_db::covers::get_source(&*db_read, release_id, CoverSource::Provider)?
            .expect("the provider cover is recorded");
        assert_eq!(&PathBuf::from(&provider.path), second);
        assert_eq!(provider.provider_id.as_deref(), Some("provider"));
        Ok(())
    }

    #[test]
    fn discarding_never_touches_files_outside_the_covers_root() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let covers_root = root.path().join("covers");
        std_fs::create_dir_all(&covers_root)?;
        let outside = root.path().join("library");
        std_fs::create_dir_all(&outside)?;
        let library_cover = outside.join("cover.png");
        std_fs::write(&library_cover, PNG_1X1)?;

        let escaping = covers_root.join("..").join("library").join("cover.png");
        discard_cover_file(&covers_root, &library_cover.to_string_lossy());
        discard_cover_file(&covers_root, &escaping.to_string_lossy());

        // A directory symlink under the covers root that leads outside it.
        std::os::unix::fs::symlink(&outside, covers_root.join("linked"))?;
        discard_cover_file(
            &covers_root,
            &covers_root
                .join("linked")
                .join("cover.png")
                .to_string_lossy(),
        );
        // A file symlink under the covers root.
        std::os::unix::fs::symlink(&library_cover, covers_root.join("manual-link.png"))?;
        discard_cover_file(
            &covers_root,
            &covers_root.join("manual-link.png").to_string_lossy(),
        );

        assert!(library_cover.is_file());
        assert!(
            covers_root
                .join("manual-link.png")
                .symlink_metadata()
                .is_ok()
        );
        Ok(())
    }

    #[test]
    fn discarding_works_under_a_covers_root_with_dot_dot() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        std_fs::create_dir_all(root.path().join("data"))?;
        let covers_root = root.path().join("data").join("..").join("covers");
        let dir = covers_root.join("1");
        std_fs::create_dir_all(&dir)?;
        let stored = dir.join("manual-x.png");
        std_fs::write(&stored, PNG_1X1)?;

        discard_cover_file(&covers_root, &stored.to_string_lossy());
        assert!(!stored.exists());
        discard_cover_file(&covers_root, &dir.join("missing.png").to_string_lossy());
        Ok(())
    }

    #[tokio::test]
    async fn a_locked_release_gets_no_local_cover_recorded() -> anyhow::Result<()> {
        let db = lyra_db::DbAsync::new(new_test_db()?);
        let release_id = {
            let mut db_write = db.write().await;
            let release_id = insert_release(&mut db_write, "Release")?;
            let mut release =
                lyra_db::releases::get_by_id(&*db_write, release_id)?.expect("release exists");
            release.locked = Some(true);
            lyra_db::releases::update(&mut db_write, &release)?;
            release_id
        };

        assert!(
            record_local_release_cover(&db, &AudioFolders::default(), release_id, None)
                .await?
                .is_none()
        );
        let db_read = db.read().await;
        assert!(lyra_db::covers::sources(&*db_read, release_id)?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn provider_sync_records_only_while_its_basis_holds() -> anyhow::Result<()> {
        let db = lyra_db::DbAsync::new(new_test_db()?);
        let release_id = insert_release(&mut *db.write().await, "Release")?;
        let public_id = lyra_db::releases::get_by_id(&*db.read().await, release_id)?
            .expect("release exists")
            .id;
        let root = tempfile::tempdir()?;
        let covers_root = root.path().join("covers");
        let target_dir = configured_cover_dir_for_release(&covers_root, release_id);
        let fetch_missing = CoverSyncOptions::default();
        let replace = CoverSyncOptions {
            update: CoverUpdate::Replace,
            bypass_cache: false,
        };
        let basis = |options| {
            let db = db.clone();
            let public_id = public_id.clone();
            async move {
                let db_read = db.read().await;
                provider_sync_basis(&db_read, release_id, &public_id, false, options)
            }
        };
        let save = |basis: ProviderSyncBasis| {
            let db = db.clone();
            let covers_root = covers_root.clone();
            let target_dir = target_dir.clone();
            async move {
                save_provider_cover(
                    &db,
                    &covers_root,
                    &basis,
                    &target_dir,
                    "provider",
                    "png",
                    PNG_1X1,
                )
                .await
            }
        };

        assert!(
            provider_sync_basis(
                &*db.read().await,
                release_id,
                &public_id,
                true,
                fetch_missing
            )?
            .is_none(),
            "a local cover means no fetch"
        );
        assert!(
            basis(CoverSyncOptions::SKIP).await?.is_none(),
            "skipping covers fetches none, even for a cover-less release"
        );

        // A cover arrives while a fetch for a cover-less release is under way.
        let missing = basis(fetch_missing).await?.expect("no cover yet");
        let replacing = basis(replace).await?.expect("replacing is allowed");
        save(replacing).await?.expect("the first replace records");
        assert!(save(missing).await?.is_none());

        // The provider cover changes, as a user's choice would, while a replace is under way.
        let stale = basis(replace).await?.expect("replacing is allowed");
        let current = basis(replace).await?.expect("replacing is allowed");
        save(current).await?.expect("recorded");
        assert!(save(stale).await?.is_none());

        // The release is locked while a replace is under way.
        let unlocked = basis(replace).await?.expect("replacing is allowed");
        {
            let mut db_write = db.write().await;
            let mut release =
                lyra_db::releases::get_by_id(&*db_write, release_id)?.expect("release exists");
            release.locked = Some(true);
            lyra_db::releases::update(&mut db_write, &release)?;
        }
        assert!(save(unlocked).await?.is_none());
        assert!(basis(replace).await?.is_none());
        Ok(())
    }

    /// A release whose local cover is recorded at `cover` and pinned, with the file removed.
    async fn pinned_local_cover(db: &lyra_db::DbAsync, cover: &Path) -> anyhow::Result<DbId> {
        let release_id = insert_release(&mut *db.write().await, "Release")?;
        std_fs::create_dir_all(cover.parent().expect("parent"))?;
        std_fs::write(cover, PNG_1X1)?;
        let public_id = release_public_id(db, release_id).await?;
        upsert_local_cover_metadata(db, release_id, &public_id, cover).await?;
        db.write().await.transaction_mut(|t| {
            lyra_db::covers::select(t, release_id, Some(CoverSource::Local))
        })?;
        Ok(release_id)
    }

    async fn local_pin(db: &lyra_db::DbAsync, release_id: DbId) -> anyhow::Result<bool> {
        let db_read = db.read().await;
        Ok(
            lyra_db::covers::get_source(&*db_read, release_id, CoverSource::Local)?.is_some()
                && lyra_db::covers::selection(&*db_read, release_id)? == Some(CoverSource::Local),
        )
    }

    #[tokio::test]
    async fn a_missing_disc_folder_keeps_the_local_cover_and_pin() -> anyhow::Result<()> {
        let db = lyra_db::DbAsync::new(new_test_db()?);
        let root = tempfile::tempdir()?;
        let album = root.path().join("Album");
        std_fs::create_dir_all(album.join("CD1"))?;
        let release_id = pinned_local_cover(&db, &album.join("CD2").join("cover.png")).await?;
        // The disc folder goes away, as an unmounted disc would, while its sibling and parent
        // stay reachable.
        std_fs::remove_dir_all(album.join("CD2"))?;

        assert!(
            record_local_release_cover(
                &db,
                &AudioFolders::default(),
                release_id,
                Some(root.path())
            )
            .await?
            .is_none()
        );
        assert!(local_pin(&db, release_id).await?);
        Ok(())
    }

    #[tokio::test]
    async fn a_folder_that_fails_to_list_keeps_the_local_cover() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let db = lyra_db::DbAsync::new(new_test_db()?);
        let root = tempfile::tempdir()?;
        let folder = root.path().join("Album");
        let release_id = pinned_local_cover(&db, &folder.join("cover.png")).await?;
        std_fs::remove_file(folder.join("cover.png"))?;
        std_fs::set_permissions(&folder, std_fs::Permissions::from_mode(0o000))?;
        if std_fs::read_dir(&folder).is_ok() {
            std_fs::set_permissions(&folder, std_fs::Permissions::from_mode(0o755))?;
            return Ok(());
        }

        let result = record_local_release_cover(
            &db,
            &AudioFolders::default(),
            release_id,
            Some(root.path()),
        )
        .await;
        std_fs::set_permissions(&folder, std_fs::Permissions::from_mode(0o755))?;
        assert!(result?.is_none());
        assert!(local_pin(&db, release_id).await?);
        Ok(())
    }

    /// A read file at `path` in the library, on a new track of `release`, which is returned.
    fn insert_library_track(
        db: &mut agdb::DbAny,
        library: DbId,
        release: DbId,
        path: &Path,
    ) -> anyhow::Result<DbId> {
        use agdb::QueryBuilder;

        let name = path.file_name().expect("file").to_string_lossy();
        let entry = lyra_db::Entry {
            db_id: None,
            id: nanoid(),
            full_path: path.to_path_buf(),
            kind: lyra_db::entries::EntryKind::File,
            file_kind: Some("audio".to_string()),
            name: name.into_owned(),
            hash: None,
            size: 1,
            file_modified_at_ms: 1,
            file_created_at_ms: 1,
            synced_at_ms: Some(1),
            missing_at_ms: None,
        };
        let entry = db
            .exec_mut(QueryBuilder::insert().element(&entry).query())?
            .ids()[0];
        let source = lyra_db::TrackSource {
            db_id: None,
            id: nanoid(),
            source_kind: "embedded_tags".to_string(),
            source_key: format!("entry:{}:embedded", entry.0),
            identity: format!("entry:{}:embedded", entry.0),
            is_primary: true,
            start_ms: None,
            end_ms: None,
            container: None,
            codec: None,
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
        };
        let source = db
            .exec_mut(QueryBuilder::insert().element(&source).query())?
            .ids()[0];
        let track = lyra_db::fixtures::insert_track(db, "Track")?;
        for (from, to) in [
            (library, entry),
            (release, track),
            (track, source),
            (source, entry),
        ] {
            lyra_db::fixtures::connect(db, from, to)?;
        }
        Ok(track)
    }

    /// A release with tracks in `Set/A` and `Disc/B`, the latter not there, and a pinned local
    /// cover at `Set/cover.png`, a folder it shares with another release. `Set/A` holds an image
    /// that could replace it.
    struct SplitRelease {
        db: lyra_db::DbAsync,
        root: tempfile::TempDir,
        library: DbId,
        release: DbId,
        other: DbId,
        wrong: PathBuf,
        replacement: PathBuf,
        set_a_track: DbId,
    }

    impl SplitRelease {
        async fn new() -> anyhow::Result<Self> {
            let db = lyra_db::DbAsync::new(new_test_db()?);
            let root = tempfile::tempdir()?;
            let wrong = root.path().join("Set/cover.png");
            let replacement = root.path().join("Set/A/cover.png");
            std_fs::create_dir_all(replacement.parent().expect("parent"))?;
            std_fs::write(&wrong, PNG_1X1)?;
            std_fs::write(&replacement, PNG_1X1)?;
            let (library, release, other, set_a_track) = {
                let mut db_write = db.write().await;
                let library = lyra_db::test_db::insert_library(
                    &mut db_write,
                    "Music",
                    root.path().to_str().expect("UTF-8 temp path"),
                )?;
                let release = insert_release(&mut db_write, "Release")?;
                let other = insert_release(&mut db_write, "Other")?;
                for owner in [release, other] {
                    lyra_db::fixtures::connect(&mut db_write, library, owner)?;
                }
                let set_a_track = insert_library_track(
                    &mut db_write,
                    library,
                    release,
                    &root.path().join("Set/A/one.flac"),
                )?;
                insert_library_track(
                    &mut db_write,
                    library,
                    release,
                    &root.path().join("Disc/B/two.flac"),
                )?;
                insert_library_track(
                    &mut db_write,
                    library,
                    other,
                    &root.path().join("Set/C/three.flac"),
                )?;
                (library, release, other, set_a_track)
            };
            stored(&db, release, CoverSource::Local, &wrong).await?;
            db.write().await.transaction_mut(|t| {
                lyra_db::covers::select(t, release, Some(CoverSource::Local))
            })?;
            Ok(Self {
                db,
                root,
                library,
                release,
                other,
                wrong,
                replacement,
                set_a_track,
            })
        }

        async fn local_path(&self) -> anyhow::Result<Option<PathBuf>> {
            Ok(lyra_db::covers::get_source(
                &*self.db.read().await,
                self.release,
                CoverSource::Local,
            )?
            .map(|cover| PathBuf::from(cover.path)))
        }

        async fn resolve(&self) -> anyhow::Result<ResolvedCovers> {
            resolve_local_covers_for_library(
                &self.db,
                self.library,
                Some(self.root.path()),
                LocalCoverScope::Rejected,
            )
            .await
        }

        async fn persist(&self, resolved: ResolvedCovers) -> anyhow::Result<usize> {
            persist_resolved_covers(&self.db, self.library, Some(self.root.path()), resolved).await
        }
    }

    #[tokio::test]
    async fn a_replacement_waits_for_every_folder_as_a_drop_does() -> anyhow::Result<()> {
        let split = SplitRelease::new().await?;

        let resolved = split.resolve().await?;
        assert_eq!(resolved.found.len(), 1, "the image in Set/A is found");
        split.persist(resolved).await?;
        assert_eq!(split.local_path().await?, Some(split.wrong.clone()));
        assert!(local_pin(&split.db, split.release).await?);

        std_fs::create_dir_all(split.root.path().join("Disc/B"))?;
        let resolved = split.resolve().await?;
        split.persist(resolved).await?;
        assert_eq!(split.local_path().await?, Some(split.replacement.clone()));
        Ok(())
    }

    #[tokio::test]
    async fn per_release_recording_defers_a_replacement_too() -> anyhow::Result<()> {
        let split = SplitRelease::new().await?;
        let folders = {
            let db_read = split.db.read().await;
            AudioFolders::of_library(&*db_read, split.library)?
        };
        let record = || {
            record_local_release_cover(&split.db, &folders, split.release, Some(split.root.path()))
        };

        let recorded = record().await?.expect("the image in Set/A is found");
        assert_eq!(recorded, (split.replacement.clone(), false));
        assert_eq!(split.local_path().await?, Some(split.wrong.clone()));
        assert!(local_pin(&split.db, split.release).await?);

        std_fs::create_dir_all(split.root.path().join("Disc/B"))?;
        let recorded = record().await?.expect("the image in Set/A is found");
        assert_eq!(recorded, (split.replacement.clone(), true));
        assert_eq!(split.local_path().await?, Some(split.replacement.clone()));
        Ok(())
    }

    #[tokio::test]
    async fn a_replacement_is_judged_again_by_current_ownership() -> anyhow::Result<()> {
        let split = SplitRelease::new().await?;
        std_fs::create_dir_all(split.root.path().join("Disc/B"))?;

        // A merge brings the release a track on a disc that isn't mounted.
        let resolved = split.resolve().await?;
        insert_library_track(
            &mut *split.db.write().await,
            split.library,
            split.release,
            &split.root.path().join("Unmounted/four.flac"),
        )?;
        split.persist(resolved).await?;
        assert_eq!(split.local_path().await?, Some(split.wrong.clone()));

        // The folder holding the replacement stops being the release's alone.
        let split = SplitRelease::new().await?;
        std_fs::create_dir_all(split.root.path().join("Disc/B"))?;
        let resolved = split.resolve().await?;
        lyra_db::fixtures::connect(&mut *split.db.write().await, split.other, split.set_a_track)?;
        split.persist(resolved).await?;
        assert_eq!(split.local_path().await?, Some(split.wrong.clone()));
        Ok(())
    }

    #[tokio::test]
    async fn a_nested_library_with_no_entries_for_a_folder_cannot_reject_its_cover()
    -> anyhow::Result<()> {
        let db = lyra_db::DbAsync::new(new_test_db()?);
        let root = tempfile::tempdir()?;
        let inner = root.path().join("inner");
        let cover = inner.join("Album/cover.png");
        std_fs::create_dir_all(cover.parent().expect("parent"))?;
        std_fs::write(&cover, PNG_1X1)?;
        std_fs::create_dir_all(root.path().join("Elsewhere"))?;
        let (outer_library, release) = {
            let mut db_write = db.write().await;
            let inner_library = lyra_db::test_db::insert_library(
                &mut db_write,
                "Inner",
                inner.to_str().expect("UTF-8 temp path"),
            )?;
            let outer_library = lyra_db::test_db::insert_library(
                &mut db_write,
                "Outer",
                root.path().to_str().expect("UTF-8 temp path"),
            )?;
            let release = insert_release(&mut db_write, "Release")?;
            for library in [inner_library, outer_library] {
                lyra_db::fixtures::connect(&mut db_write, library, release)?;
            }
            insert_library_track(
                &mut db_write,
                inner_library,
                release,
                &inner.join("Album/one.flac"),
            )?;
            insert_library_track(
                &mut db_write,
                outer_library,
                release,
                &root.path().join("Elsewhere/two.flac"),
            )?;
            (outer_library, release)
        };
        stored(&db, release, CoverSource::Local, &cover).await?;

        for scope in [LocalCoverScope::Every, LocalCoverScope::Rejected] {
            let resolved =
                resolve_local_covers_for_library(&db, outer_library, Some(root.path()), scope)
                    .await?;
            persist_resolved_covers(&db, outer_library, Some(root.path()), resolved).await?;
            assert!(
                lyra_db::covers::get_source(&*db.read().await, release, CoverSource::Local)?
                    .is_some(),
                "a folder this library has no entries for is no evidence"
            );
        }
        Ok(())
    }

    /// Two releases sharing `Mix`, the winner with a local cover from it, which the folders reject.
    struct DisputedCover {
        db: lyra_db::DbAsync,
        root: tempfile::TempDir,
        library: DbId,
        winner: DbId,
        loser: DbId,
        public_id: String,
        cover: PathBuf,
    }

    impl DisputedCover {
        async fn new() -> anyhow::Result<Self> {
            let db = lyra_db::DbAsync::new(new_test_db()?);
            let root = tempfile::tempdir()?;
            let cover = root.path().join("Mix/cover.png");
            std_fs::create_dir_all(cover.parent().expect("parent"))?;
            std_fs::write(&cover, PNG_1X1)?;
            let (library, winner, loser) = {
                let mut db_write = db.write().await;
                let library = lyra_db::test_db::insert_library(
                    &mut db_write,
                    "Music",
                    root.path().to_str().expect("UTF-8 temp path"),
                )?;
                let winner = insert_release(&mut db_write, "Winner")?;
                let loser = insert_release(&mut db_write, "Loser")?;
                for (release, name) in [(winner, "one.flac"), (loser, "two.flac")] {
                    lyra_db::fixtures::connect(&mut db_write, library, release)?;
                    insert_library_track(
                        &mut db_write,
                        library,
                        release,
                        &root.path().join("Mix").join(name),
                    )?;
                }
                (library, winner, loser)
            };
            let public_id = release_public_id(&db, winner).await?;
            upsert_local_cover_metadata(&db, winner, &public_id, &cover).await?;
            Ok(Self {
                db,
                root,
                library,
                winner,
                loser,
                public_id,
                cover,
            })
        }

        /// What a sync decides on before downloading, discounting the cover if the folders do.
        async fn basis(&self) -> anyhow::Result<ProviderSyncBasis> {
            let db_read = self.db.read().await;
            let folders = Arc::new(AudioFolders::of_library(&*db_read, self.library)?);
            let tracks = lyra_db::tracks::get_direct(&db_read, self.winner)?;
            let root = Some(self.root.path());
            let dirs = cover_dirs_for_release(&db_read, &folders, self.winner, &tracks, root)?;
            let rejected = rejected_local_file(&*db_read, self.winner, &dirs, &folders)?;
            assert_eq!(rejected.as_deref(), Some(self.cover.as_path()));
            Ok(provider_sync_basis(
                &db_read,
                self.winner,
                &self.public_id,
                false,
                CoverSyncOptions::default(),
            )?
            .expect("no cover seen")
            .discounting_local(DiscountedLocal::of(rejected, &folders, root)))
        }

        async fn save(&self, basis: &ProviderSyncBasis) -> anyhow::Result<Option<PathBuf>> {
            let covers_root = self.root.path().join("covers");
            save_provider_cover(
                &self.db,
                &covers_root,
                basis,
                &configured_cover_dir_for_release(&covers_root, self.winner),
                "provider",
                "png",
                PNG_1X1,
            )
            .await
        }

        async fn merge(&self) -> anyhow::Result<()> {
            self.db.write().await.transaction_mut(|t| {
                crate::services::providers::merge_release_into_inside_tx(t, self.winner, self.loser)
            })?;
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_rejected_local_cover_gives_way_to_a_provider_download() -> anyhow::Result<()> {
        let disputed = DisputedCover::new().await?;
        let mut undiscounted = disputed.basis().await?;
        undiscounted.discounted_local = None;
        assert!(
            disputed.save(&undiscounted).await?.is_none(),
            "a local cover not discounted wins over the download"
        );

        let basis = disputed.basis().await?;
        assert!(disputed.save(&basis).await?.is_some());
        assert!(
            disputed.save(&basis).await?.is_none(),
            "the provider cover now stored still counts"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_merge_during_a_download_makes_the_local_cover_count_again() -> anyhow::Result<()> {
        let disputed = DisputedCover::new().await?;
        let basis = disputed.basis().await?;

        disputed.merge().await?;
        let saved = disputed.save(&basis).await?;

        assert!(saved.is_none(), "the local cover is valid again");
        assert!(
            lyra_db::covers::get_source(
                &*disputed.db.read().await,
                disputed.winner,
                CoverSource::Provider
            )?
            .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_folder_relevant_only_since_the_listing_defers_the_drop() -> anyhow::Result<()> {
        let db = lyra_db::DbAsync::new(new_test_db()?);
        let root = tempfile::tempdir()?;
        let wrong = root.path().join("Mix/cover.png");
        std_fs::create_dir_all(wrong.parent().expect("parent"))?;
        std_fs::write(&wrong, PNG_1X1)?;
        let (library, release) = {
            let mut db_write = db.write().await;
            let library = lyra_db::test_db::insert_library(
                &mut db_write,
                "Music",
                root.path().to_str().expect("UTF-8 temp path"),
            )?;
            let release = insert_release(&mut db_write, "Release")?;
            let other = insert_release(&mut db_write, "Other")?;
            lyra_db::fixtures::connect(&mut db_write, library, release)?;
            lyra_db::fixtures::connect(&mut db_write, library, other)?;
            insert_library_track(
                &mut db_write,
                library,
                release,
                &root.path().join("Mix/one.flac"),
            )?;
            insert_library_track(
                &mut db_write,
                library,
                other,
                &root.path().join("Mix/two.flac"),
            )?;
            (library, release)
        };
        stored(&db, release, CoverSource::Local, &wrong).await?;
        let judged = async || -> anyhow::Result<(AudioFolders, Option<RejectedLocalCover>)> {
            let db_read = db.read().await;
            let folders = AudioFolders::of_library(&*db_read, library)?;
            let tracks = lyra_db::tracks::get_direct(&db_read, release)?;
            let dirs =
                cover_dirs_for_release(&db_read, &folders, release, &tracks, Some(root.path()))?;
            let rejected = RejectedLocalCover::find(&*db_read, release, &dirs, &folders)?;
            Ok((folders, rejected))
        };
        let (folders, rejected) = judged().await?;
        let rejected = rejected.expect("the shared folder rejects the record");

        // A merge brings the release tracks on a disc that isn't mounted.
        insert_library_track(
            &mut *db.write().await,
            library,
            release,
            &root.path().join("Unmounted/three.flac"),
        )?;
        let removed =
            drop_rejected_local_covers(&db, vec![rejected], &folders, Some(root.path())).await?;
        assert_eq!(removed, 0, "the new folder was never listed");
        assert!(
            lyra_db::covers::get_source(&*db.read().await, release, CoverSource::Local)?.is_some()
        );

        std_fs::create_dir_all(root.path().join("Unmounted"))?;
        let (folders, rejected) = judged().await?;
        let rejected = rejected.expect("still not the release's");
        let removed =
            drop_rejected_local_covers(&db, vec![rejected], &folders, Some(root.path())).await?;
        assert_eq!(removed, 1, "with every folder listed it goes");
        Ok(())
    }

    #[tokio::test]
    async fn a_cover_gone_from_a_listed_folder_is_forgotten() -> anyhow::Result<()> {
        let db = lyra_db::DbAsync::new(new_test_db()?);
        let root = tempfile::tempdir()?;
        let folder = root.path().join("Album");
        let release_id = pinned_local_cover(&db, &folder.join("cover.png")).await?;
        std_fs::remove_file(folder.join("cover.png"))?;

        assert!(
            record_local_release_cover(
                &db,
                &AudioFolders::default(),
                release_id,
                Some(root.path())
            )
            .await?
            .is_none()
        );
        assert!(
            lyra_db::covers::sources(&*db.read().await, release_id)?.is_empty(),
            "the record and its pin go"
        );
        Ok(())
    }

    /// Stores a cover for `owner` from `source` at `path`, which may not exist.
    async fn stored(
        db: &lyra_db::DbAsync,
        owner: DbId,
        source: CoverSource,
        path: &Path,
    ) -> anyhow::Result<Cover> {
        let mut db_write = db.write().await;
        Ok(db_write
            .transaction_mut(|t| {
                lyra_db::covers::upsert(
                    t,
                    owner,
                    source,
                    Cover {
                        db_id: None,
                        id: nanoid(),
                        path: path.to_string_lossy().into_owned(),
                        mime_type: "image/png".to_string(),
                        hash: source.as_str().to_string(),
                        blurhash: None,
                        provider_id: None,
                    },
                )
            })?
            .cover)
    }

    async fn shown_source(
        db: &lyra_db::DbAsync,
        owner: DbId,
    ) -> anyhow::Result<Option<CoverSource>> {
        let sources = lyra_db::covers::sources(&*db.read().await, owner)?;
        Ok(lyra_db::covers::shown(&sources).map(|sourced| sourced.source))
    }

    #[tokio::test]
    async fn an_entity_sync_forgets_every_source_confirmed_gone() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let dir = tempfile::tempdir()?;
        let db = lyra_db::DbAsync::new(new_test_db()?);
        let (release, locked) = {
            let mut db_write = db.write().await;
            let release = insert_release(&mut db_write, "Album")?;
            let locked = lyra_db::fixtures::insert_artist(&mut db_write, "Locked")?;
            let mut artist = lyra_db::artists::get_by_id(&db_write, locked)?.expect("artist");
            artist.locked = Some(true);
            lyra_db::artists::update(&mut db_write, &artist)?;
            (release, locked)
        };
        let local = write_test_cover()?;
        stored(&db, release, CoverSource::Local, &local).await?;
        stored(
            &db,
            release,
            CoverSource::Manual,
            &dir.path().join("manual.png"),
        )
        .await?;
        stored(
            &db,
            release,
            CoverSource::Provider,
            &dir.path().join("unmounted/provider.png"),
        )
        .await?;
        stored(
            &db,
            locked,
            CoverSource::Provider,
            &dir.path().join("gone.png"),
        )
        .await?;

        assert_eq!(forget_gone_covers(&db, release).await?, 1);
        assert_eq!(
            shown_source(&db, release).await?,
            Some(CoverSource::Provider),
            "the manual cover is gone, the unmounted provider cover is no evidence"
        );
        assert_eq!(
            forget_gone_covers(&db, locked).await?,
            0,
            "a lock keeps covers"
        );
        let _ = std_fs::remove_file(local);
        Ok(())
    }

    #[tokio::test]
    async fn a_library_sweep_forgets_gone_covers_of_releases_and_artists() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let dir = tempfile::tempdir()?;
        let db = lyra_db::DbAsync::new(new_test_db()?);
        let (library, release, unmounted, artist) = {
            let mut db_write = db.write().await;
            let library = lyra_db::test_db::insert_library(&mut db_write, "Music", "/music")?;
            let release = insert_release(&mut db_write, "Album")?;
            let unmounted = insert_release(&mut db_write, "Unmounted")?;
            for owner in [release, unmounted] {
                lyra_db::fixtures::connect(&mut db_write, library, owner)?;
            }
            let artist = lyra_db::fixtures::insert_artist(&mut db_write, "Artist")?;
            lyra_db::fixtures::connect_credit(
                &mut db_write,
                release,
                artist,
                lyra_db::CreditType::Artist,
                None,
                0,
            )?;
            (library, release, unmounted, artist)
        };
        let local = write_test_cover()?;
        stored(&db, release, CoverSource::Local, &local).await?;
        stored(
            &db,
            release,
            CoverSource::Provider,
            &dir.path().join("gone.png"),
        )
        .await?;
        stored(
            &db,
            unmounted,
            CoverSource::Local,
            &dir.path().join("unmounted/cover.png"),
        )
        .await?;
        stored(
            &db,
            artist,
            CoverSource::Provider,
            &dir.path().join("artist.png"),
        )
        .await?;

        let removed = refresh_gone_covers_for_library(&db, library, None).await?;
        assert_eq!(removed, 2);
        assert_eq!(shown_source(&db, release).await?, Some(CoverSource::Local));
        assert_eq!(shown_source(&db, artist).await?, None);
        assert_eq!(
            shown_source(&db, unmounted).await?,
            Some(CoverSource::Local),
            "an unmounted folder is no evidence"
        );
        let _ = std_fs::remove_file(local);
        Ok(())
    }

    /// A release with a local cover whose file is confirmed gone, as the sweep reads it.
    async fn release_with_gone_local_cover(
        db: &lyra_db::DbAsync,
        dir: &Path,
        title: &str,
    ) -> anyhow::Result<(DbId, StoredCover)> {
        let release = {
            let mut db_write = db.write().await;
            insert_release(&mut db_write, title)?
        };
        stored(
            db,
            release,
            CoverSource::Local,
            &dir.join(format!("{title}.png")),
        )
        .await?;
        let candidates = StoredCover::candidates(&*db.read().await, release)?;
        let mut stale = confirmed_stale(candidates).await?;
        assert_eq!(stale.len(), 1);
        Ok((release, stale.remove(0)))
    }

    #[tokio::test]
    async fn a_cover_stored_since_the_sweep_read_is_not_overwritten() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let dir = tempfile::tempdir()?;
        let db = lyra_db::DbAsync::new(new_test_db()?);
        let (release, stale) = release_with_gone_local_cover(&db, dir.path(), "Album").await?;
        let found = write_test_cover()?;

        let refreshed = write_test_cover()?;
        stored(&db, release, CoverSource::Local, &refreshed).await?;
        let gone = GoneCovers {
            replaced: vec![(stale, found.clone())],
            ..GoneCovers::default()
        };
        apply_gone_covers(&db, DbId(0), gone).await;

        let stored = lyra_db::covers::get(&*db.read().await, release)?.map(|cover| cover.path);
        assert_eq!(stored, Some(refreshed.to_string_lossy().into_owned()));
        let _ = std_fs::remove_file(found);
        let _ = std_fs::remove_file(refreshed);
        Ok(())
    }

    #[tokio::test]
    async fn a_cover_whose_file_came_back_is_not_replaced() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let dir = tempfile::tempdir()?;
        let db = lyra_db::DbAsync::new(new_test_db()?);
        let (release, stale) = release_with_gone_local_cover(&db, dir.path(), "Album").await?;
        let found = write_test_cover()?;

        let original = dir.path().join("Album.png");
        std_fs::write(&original, PNG_1X1)?;
        let gone = GoneCovers {
            replaced: vec![(stale, found.clone())],
            ..GoneCovers::default()
        };
        apply_gone_covers(&db, DbId(0), gone).await;

        let stored = lyra_db::covers::get(&*db.read().await, release)?.map(|cover| cover.path);
        assert_eq!(stored, Some(original.to_string_lossy().into_owned()));
        let _ = std_fs::remove_file(found);
        Ok(())
    }

    #[tokio::test]
    async fn an_unreadable_replacement_does_not_stop_the_other_removals() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let dir = tempfile::tempdir()?;
        let db = lyra_db::DbAsync::new(new_test_db()?);
        let (unreadable, unreadable_stale) =
            release_with_gone_local_cover(&db, dir.path(), "Unreadable").await?;
        let (removed, removed_stale) =
            release_with_gone_local_cover(&db, dir.path(), "Removed").await?;
        let gone = GoneCovers {
            replaced: vec![(unreadable_stale, dir.path().join("vanished.png"))],
            removed: vec![removed_stale],
            failed: Vec::new(),
        };

        assert_eq!(apply_gone_covers(&db, DbId(0), gone).await, 1);
        let db_read = db.read().await;
        assert!(lyra_db::covers::get(&*db_read, removed)?.is_none());
        assert!(
            lyra_db::covers::get(&*db_read, unreadable)?.is_some(),
            "the stale cover stays for the next sync"
        );
        Ok(())
    }
}
