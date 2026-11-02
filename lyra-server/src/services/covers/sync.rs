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
use nanoid::nanoid;
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

use crate::services::providers::{
    active_providers,
    provider_registry,
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
    image::{
        cover_blurhash,
        cover_hash,
        cover_mime_from_path,
        read_cover_hash,
        validate_cover_image,
    },
    providers::{
        EntityType,
        artist_context_value,
        library_for_release,
        provider_external_ids_for_entity,
        release_context_value,
        resolve_provider_artist_cover_url,
        resolve_provider_release_cover_url,
    },
    resolve::{
        configured_cover_dir_for_artist,
        configured_cover_dir_for_release,
        covers_locked,
        has_stored_cover,
        resolve_local_cover_for_release,
    },
    stale::StaleCover,
};

pub(crate) const MAX_COVER_BYTES: usize = 20 * 1024 * 1024;

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

    let temp = path.with_file_name(format!(".lyra-cover-tmp-{}", nanoid!()));
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
pub(crate) fn discard_cover_file(covers_root: &Path, path: &str) {
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
            discard_cover_file(&covers_root, &cover.path);
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
        let target = dir.join(format!("{}-{}.{ext}", source.as_str(), nanoid!()));
        write_cover_image(&target, bytes).await?;
        let mut prepared = Self {
            source,
            pending: Some(Cover {
                db_id: None,
                id: nanoid!(),
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
            discard_cover_file(covers_root, &replaced.cover.path);
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
}

/// The basis on which an automatic provider sync may fetch a cover for the owner: never for a
/// locked owner, and otherwise only when it has no cover at all unless the stored provider
/// cover is to be replaced.
fn provider_sync_basis(
    db: &agdb::DbAny,
    owner_id: DbId,
    owner_public_id: &str,
    local_cover: Option<&Path>,
    options: CoverSyncOptions,
) -> Result<Option<ProviderSyncBasis>> {
    if covers_locked(db, owner_id)? {
        return Ok(None);
    }
    if !options.replace_existing && (local_cover.is_some() || has_stored_cover(db, owner_id)?) {
        return Ok(None);
    }
    Ok(Some(ProviderSyncBasis {
        options,
        owner_id,
        owner_public_id: owner_public_id.to_string(),
        provider_cover: lyra_db::covers::get_source(db, owner_id, CoverSource::Provider)?
            .map(|cover| cover.path),
    }))
}

/// Whether the basis still holds: the same entity, still unlocked, with no cover arrived
/// meanwhile unless replacing, and the provider cover as the sync found it.
fn provider_sync_still_allowed(db: &agdb::DbAny, basis: &ProviderSyncBasis) -> Result<bool> {
    if lyra_db::lookup::find_node_id_by_id(db, &basis.owner_public_id)? != Some(basis.owner_id)
        || covers_locked(db, basis.owner_id)?
    {
        return Ok(false);
    }
    if !basis.options.replace_existing && has_stored_cover(db, basis.owner_id)? {
        return Ok(false);
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

pub(crate) async fn sync_release_cover_for_tracks(
    db: &lyra_db::DbAsync,
    tracks: &[Track],
    release: &Release,
    artists: &[Artist],
    paths: CoverPaths<'_>,
    options: CoverSyncOptions,
) -> Result<bool> {
    let Some(release_id) = release.db_id.clone().map(Into::<DbId>::into) else {
        return Ok(false);
    };

    let (basis, provider_contexts) = {
        let db_read = db.read().await;
        let sorted_providers = active_providers(&db_read)?;
        let library = library_for_release(&db_read, release_id)?;
        let local_cover = resolve_local_cover_for_release(&db_read, tracks, paths.library_root)?;
        let basis = provider_sync_basis(
            &db_read,
            release_id,
            &release.id,
            local_cover.as_deref(),
            options,
        )?;

        let mut provider_contexts = Vec::new();
        for provider in sorted_providers {
            let provider_ids =
                provider_external_ids_for_entity(&db_read, release_id, &provider.provider_id)?;
            let release_context =
                release_context_value(release, tracks, artists, &provider_ids, library.as_ref())?;
            provider_contexts.push((provider.provider_id, release_context));
        }

        (basis, provider_contexts)
    };

    let Some(basis) = basis.filter(|_| !provider_contexts.is_empty()) else {
        return Ok(false);
    };

    let target_dir = configured_cover_dir_for_release(paths.covers_root, release_id);

    for (provider_id, release_context) in provider_contexts {
        let cover_url = match resolve_provider_release_cover_url(
            &provider_id,
            &release_context,
            options.force_refresh,
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

pub(crate) async fn sync_artist_cover(
    db: &lyra_db::DbAsync,
    artist: &Artist,
    paths: CoverPaths<'_>,
    options: CoverSyncOptions,
    provider_filter: Option<&str>,
) -> Result<bool> {
    let Some(artist_id) = artist.db_id.clone().map(Into::<DbId>::into) else {
        return Ok(false);
    };

    let (basis, provider_contexts) = {
        let db_read = db.read().await;
        let sorted_providers = active_providers(&db_read)?.only(provider_filter);
        let basis = provider_sync_basis(&db_read, artist_id, &artist.id, None, options)?;

        let mut provider_contexts = Vec::new();
        for provider in sorted_providers {
            let provider_ids =
                provider_external_ids_for_entity(&db_read, artist_id, &provider.provider_id)?;
            let artist_context = artist_context_value(artist, &provider_ids)?;
            provider_contexts.push((provider.provider_id, artist_context));
        }

        (basis, provider_contexts)
    };

    let Some(basis) = basis.filter(|_| !provider_contexts.is_empty()) else {
        return Ok(false);
    };

    let target_dir = configured_cover_dir_for_artist(paths.covers_root, artist_id);

    for (provider_id, artist_context) in provider_contexts {
        let cover_url = match resolve_provider_artist_cover_url(
            &provider_id,
            &artist_context,
            options.force_refresh,
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

/// Local covers found for a library's releases, and the errors of those whose search failed.
type ResolvedCovers = (Vec<(DbId, PathBuf)>, Vec<anyhow::Error>);

/// The local covers of the library's unlocked releases.
fn resolve_local_covers_for_library(
    db: &agdb::DbAny,
    library_id: DbId,
    paths: CoverPaths<'_>,
) -> Result<ResolvedCovers> {
    let (mut resolved, mut failed) = (Vec::new(), Vec::new());
    for release in &lyra_db::releases::get(db, library_id)? {
        let Some(release_id) = release.db_id.clone().map(Into::<DbId>::into) else {
            continue;
        };
        if release.locked.unwrap_or(false) {
            continue;
        }
        let tracks = lyra_db::tracks::get_direct(db, release_id)?;
        match resolve_local_cover_for_release(db, &tracks, paths.library_root) {
            Ok(Some(cover_path)) => resolved.push((release_id, cover_path)),
            Ok(None) => {}
            Err(err) => failed.push(err),
        }
    }
    Ok((resolved, failed))
}

pub(crate) async fn sync_release_covers_for_library(
    db: &lyra_db::DbAsync,
    paths: CoverPaths<'_>,
    library_id: DbId,
    options: CoverSyncOptions,
    provider_filter: Option<&str>,
) -> Result<usize> {
    let resolve_contexts = {
        let db_read = db.read().await;
        let releases = lyra_db::releases::get(&db_read, library_id)?;

        let sorted_providers = active_providers(&db_read)?.only(provider_filter);
        if sorted_providers.is_empty() {
            return Ok(0);
        }

        let mut contexts = Vec::new();
        for release in &releases {
            let Some(release_id) = release.db_id.clone().map(Into::<DbId>::into) else {
                continue;
            };

            let tracks = lyra_db::tracks::get_direct(&db_read, release_id)?;
            // The metadata pass after this one reports a cover search that fails.
            let Ok(local_cover) =
                resolve_local_cover_for_release(&db_read, &tracks, paths.library_root)
            else {
                continue;
            };
            let Some(basis) = provider_sync_basis(
                &db_read,
                release_id,
                &release.id,
                local_cover.as_deref(),
                options,
            )?
            else {
                continue;
            };

            let artists = lyra_db::artists::get(&db_read, release_id)?;
            let library = library_for_release(&db_read, release_id)?;
            let target_dir = configured_cover_dir_for_release(paths.covers_root, release_id);

            let mut provider_contexts = Vec::new();
            for provider in &sorted_providers {
                let ext_ids =
                    provider_external_ids_for_entity(&db_read, release_id, &provider.provider_id)?;
                let ctx =
                    release_context_value(release, &tracks, &artists, &ext_ids, library.as_ref())?;
                provider_contexts.push((provider.provider_id.clone(), ctx));
            }

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
        for (provider_id, release_context) in &ctx.provider_contexts {
            let cover_url = match resolve_provider_release_cover_url(
                provider_id,
                release_context,
                options.force_refresh,
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
        let permit = concurrency
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow!("download semaphore closed"))?;
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

pub(crate) async fn sync_artist_covers_for_library(
    db: &lyra_db::DbAsync,
    paths: CoverPaths<'_>,
    library_id: DbId,
    options: CoverSyncOptions,
    provider_filter: Option<&str>,
) -> Result<usize> {
    let artists = {
        let db_read = db.read().await;
        lyra_db::artists::get_by_library(&db_read, library_id)?
    };

    let mut synced = 0usize;
    for artist in &artists {
        if sync_artist_cover(db, artist, paths, options, provider_filter).await? {
            synced += 1;
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

/// Records the image at `cover_path` as the owner's local cover. Returns whether the record
/// changed.
pub(super) async fn upsert_local_cover_metadata(
    db: &lyra_db::DbAsync,
    owner_id: DbId,
    cover_path: &Path,
) -> Result<bool> {
    if !cover_path.is_file() {
        return Ok(false);
    }

    let (hash, blurhash, mime_type) = cover_file_metadata(cover_path).await?;
    let path_str = cover_path.to_string_lossy().into_owned();
    let mut db_write = db.write().await;
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
            id: nanoid!(),
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

/// Records the release's library cover as its local cover, leaving a locked release alone, then
/// removes its covers from any source whose file is confirmed gone. Returns the cover found and
/// whether its record changed.
pub(crate) async fn record_local_release_cover(
    db: &lyra_db::DbAsync,
    release_id: DbId,
    library_root: Option<&Path>,
) -> Result<Option<(PathBuf, bool)>> {
    let scan = {
        let db_read = db.read().await;
        let Some(release) = lyra_db::releases::get_by_id(&db_read, release_id)? else {
            return Ok(None);
        };
        if release.locked.unwrap_or(false) {
            return Ok(None);
        }
        let tracks = lyra_db::tracks::get_direct(&db_read, release_id)?;
        resolve_local_cover_for_release(&db_read, &tracks, library_root)?
    };
    let recorded = match scan {
        Some(cover_path) => {
            let changed = upsert_local_cover_metadata(db, release_id, &cover_path).await?;
            Some((cover_path, changed))
        }
        None => None,
    };
    forget_gone_covers(db, release_id).await?;
    Ok(recorded)
}

/// Removes the owner's covers from every source whose file is confirmed gone, unless the owner is
/// locked, so it shows its next cover. Returns how many were removed.
pub(crate) async fn forget_gone_covers(db: &lyra_db::DbAsync, owner_id: DbId) -> Result<usize> {
    let stale = {
        let db_read = db.read().await;
        if covers_locked(&db_read, owner_id)? {
            return Ok(0);
        }
        StaleCover::read_all(&*db_read, owner_id)?
    };
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

/// A library's stored covers whose file is confirmed gone, as the sweep found them under one read.
#[derive(Default)]
struct GoneCovers {
    /// Local covers, each with the library file found to replace it.
    replaced: Vec<(StaleCover, PathBuf)>,
    /// Those with no file to replace them.
    removed: Vec<StaleCover>,
    /// The errors of the searches that failed.
    failed: Vec<anyhow::Error>,
}

fn find_gone_covers(
    db: &agdb::DbAny,
    library_id: DbId,
    library_root: Option<&Path>,
) -> Result<GoneCovers> {
    let mut gone = GoneCovers::default();
    for release in lyra_db::releases::get(db, library_id)? {
        let Some(release_id) = release.db_id.map(DbId::from) else {
            continue;
        };
        if release.locked.unwrap_or(false) {
            continue;
        }
        for stale in StaleCover::read_all(db, release_id)? {
            if stale.source() != CoverSource::Local {
                gone.removed.push(stale);
                continue;
            }
            let tracks = lyra_db::tracks::get_direct(db, release_id)?;
            match resolve_local_cover_for_release(db, &tracks, library_root) {
                Ok(Some(path)) => gone.replaced.push((stale, path)),
                Ok(None) => gone.removed.push(stale),
                Err(err) => gone.failed.push(err),
            }
        }
    }
    for artist in lyra_db::artists::get_by_library(db, library_id)? {
        let Some(artist_id) = artist.db_id.map(DbId::from) else {
            continue;
        };
        if artist.locked.unwrap_or(false) {
            continue;
        }
        gone.removed.extend(StaleCover::read_all(db, artist_id)?);
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
    let gone = find_gone_covers(&*db.read().await, library_id, library_root)?;
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
                    id: nanoid!(),
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

struct PendingCover {
    owner_id: DbId,
    path: PathBuf,
    mime_type: String,
}

struct ComputedCover {
    owner_id: DbId,
    path: String,
    mime_type: String,
    hash: String,
    blurhash: Option<String>,
}

async fn sync_local_cover_metadata_from_resolved(
    db: &lyra_db::DbAsync,
    resolved: &[(DbId, PathBuf)],
) -> Result<usize> {
    let mut pending: Vec<PendingCover> = Vec::new();

    for (owner_id, cover_path) in resolved {
        let mime_type = cover_mime_from_path(cover_path).to_string();

        let skip = {
            let db_read = db.read().await;
            if let Some(existing) =
                lyra_db::covers::get_source(&db_read, *owner_id, CoverSource::Local)?
            {
                existing.path == cover_path.to_string_lossy()
                    && cover_hash(cover_path).is_some_and(|hash| existing.hash == hash)
            } else {
                false
            }
        };
        if skip {
            continue;
        }

        pending.push(PendingCover {
            owner_id: *owner_id,
            path: cover_path.clone(),
            mime_type,
        });
    }

    if pending.is_empty() {
        return Ok(0);
    }

    let (computed, unreadable): (Vec<_>, Vec<_>) = pending
        .into_par_iter()
        .map(|p| {
            let hash = read_cover_hash(&p.path)?;
            let blurhash = cover_blurhash(&p.path);
            Ok(ComputedCover {
                owner_id: p.owner_id,
                path: p.path.to_string_lossy().into_owned(),
                mime_type: p.mime_type,
                hash,
                blurhash,
            })
        })
        .partition_map(|computed: Result<ComputedCover>| match computed {
            Ok(computed) => rayon::iter::Either::Left(computed),
            Err(err) => rayon::iter::Either::Right(err),
        });

    let mut db_write = db.write().await;
    let count = db_write.transaction_mut(|t| {
        let mut count = 0;
        for c in computed {
            // A release locked since the scan read it keeps its covers.
            if covers_locked(t, c.owner_id)? {
                continue;
            }
            let cover = Cover {
                db_id: None,
                id: nanoid!(),
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
pub(crate) async fn sync_and_persist_release_covers_for_library(
    db: &lyra_db::DbAsync,
    paths: CoverPaths<'_>,
    library_id: DbId,
    options: CoverSyncOptions,
    provider_filter: Option<&str>,
) -> Result<usize> {
    sync_release_covers_for_library(db, paths, library_id, options, provider_filter).await?;

    let (resolved, unresolved) = {
        let db_read = db.read().await;
        resolve_local_covers_for_library(&db_read, library_id, paths)?
    };
    let persisted = if resolved.is_empty() {
        Ok(0)
    } else {
        sync_local_cover_metadata_from_resolved(db, &resolved).await
    };
    let persisted = match (persisted, unresolved.first()) {
        (persisted, None) => persisted?,
        (persisted, Some(first)) => {
            let unresolved = anyhow::anyhow!(
                "{} release covers could not be resolved; first: {first:#}",
                unresolved.len()
            );
            return Err(match persisted {
                Ok(_) => unresolved,
                Err(err) => err.context(unresolved),
            });
        }
    };
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
        let path = std::env::temp_dir().join(format!("lyra-test-cover-{}.png", nanoid!()));
        std_fs::write(&path, PNG_1X1)?;
        Ok(path)
    }

    #[tokio::test]
    async fn sync_release_cover_for_tracks_returns_false_without_providers() -> anyhow::Result<()> {
        let db = lyra_db::DbAsync::new(new_test_db()?);
        let release = Release {
            db_id: None,
            id: nanoid!(),
            release_title: "Release".to_string(),
            sort_title: None,
            release_type: None,
            release_date: None,
            media_formats: None,
            barcode: None,
            locked: None,
            created_at_ms: None,
            file_created_at_ms: None,
        };

        let synced = sync_release_cover_for_tracks(
            &db,
            &[],
            &release,
            &[],
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

        let inserted = upsert_local_cover_metadata(&db, release_id, &cover_path).await?;
        let inserted_again = upsert_local_cover_metadata(&db, release_id, &cover_path).await?;

        assert!(inserted);
        assert!(!inserted_again);

        let _ = std_fs::remove_file(cover_path);
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
        upsert_local_cover_metadata(&db, release_id, &library_cover).await?;

        let covers_root = root.path().join("covers");
        let target_dir = configured_cover_dir_for_release(&covers_root, release_id);
        let replace = CoverSyncOptions {
            replace_existing: true,
            force_refresh: false,
        };
        let mut saved = Vec::new();
        for _ in 0..2 {
            let basis = {
                let db_read = db.read().await;
                let public_id = lyra_db::releases::get_by_id(&*db_read, release_id)?
                    .expect("release exists")
                    .id;
                provider_sync_basis(&db_read, release_id, &public_id, None, replace)?
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
            record_local_release_cover(&db, release_id, None)
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
            replace_existing: true,
            force_refresh: false,
        };
        let basis = |options| {
            let db = db.clone();
            let public_id = public_id.clone();
            async move {
                let db_read = db.read().await;
                provider_sync_basis(&db_read, release_id, &public_id, None, options)
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
                Some(Path::new("/music/cover.jpg")),
                fetch_missing
            )?
            .is_none(),
            "a local cover means no fetch"
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
        upsert_local_cover_metadata(db, release_id, cover).await?;
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
            record_local_release_cover(&db, release_id, Some(root.path()))
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

        let result = record_local_release_cover(&db, release_id, Some(root.path())).await;
        std_fs::set_permissions(&folder, std_fs::Permissions::from_mode(0o755))?;
        assert!(result?.is_none());
        assert!(local_pin(&db, release_id).await?);
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
            record_local_release_cover(&db, release_id, Some(root.path()))
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
                        id: nanoid!(),
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
    ) -> anyhow::Result<(DbId, StaleCover)> {
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
        let mut stale = StaleCover::read_all(&*db.read().await, release)?;
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
