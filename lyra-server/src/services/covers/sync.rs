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

/// Deletes a cover file Lyra wrote. Files outside the covers root, such as library covers, are
/// never touched.
pub(crate) fn discard_cover_file(covers_root: &Path, path: &str) {
    let path = Path::new(path);
    let inside_root = path.starts_with(covers_root)
        && path
            .components()
            .all(|component| !matches!(component, std::path::Component::ParentDir));
    if !inside_root {
        return;
    }
    if let Err(err) = std::fs::remove_file(path)
        && err.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(path = %path.display(), error = %err, "failed to remove replaced cover file");
    }
}

/// A cover image to store under the covers root as an owner's cover from one source.
pub(super) struct NewCoverFile<'a> {
    pub(super) owner_id: DbId,
    pub(super) source: CoverSource,
    pub(super) dir: &'a Path,
    pub(super) ext: &'a str,
    pub(super) bytes: &'a [u8],
    pub(super) provider_id: Option<&'a str>,
    /// Whether to pin the source as the one the owner shows.
    pub(super) select: bool,
}

/// Writes the image as a new file in its directory and records it as the owner's cover from
/// its source, discarding the file of the cover it replaces once nothing refers to it. Every
/// stored file gets its own name, so a replaced cover never changes under another owner.
pub(super) async fn store_cover_file(
    db: &lyra_db::DbAsync,
    covers_root: &Path,
    new: NewCoverFile<'_>,
) -> Result<Cover> {
    let target = new
        .dir
        .join(format!("{}-{}.{}", new.source.as_str(), nanoid!(), new.ext));
    write_cover_image(&target, new.bytes).await?;
    let recorded = async {
        let (hash, blurhash, mime_type) = cover_file_metadata(&target).await?;
        let cover = Cover {
            db_id: None,
            id: nanoid!(),
            path: target.to_string_lossy().into_owned(),
            mime_type,
            hash,
            blurhash,
            provider_id: new.provider_id.map(str::to_string),
        };
        let mut db_write = db.write().await;
        db_write.transaction_mut(|t| {
            let upserted = lyra_db::covers::upsert(t, new.owner_id, new.source, cover)?;
            if new.select {
                lyra_db::covers::select(t, new.owner_id, Some(new.source))?;
            }
            anyhow::Ok(upserted)
        })
    }
    .await;
    let upserted = match recorded {
        Ok(upserted) => upserted,
        Err(err) => {
            let _ = fs::remove_file(&target).await;
            return Err(err);
        }
    };
    if let Some(replaced) = upserted.replaced
        && replaced.orphaned
    {
        discard_cover_file(covers_root, &replaced.cover.path);
    }
    Ok(upserted.cover)
}

async fn save_provider_cover(
    db: &lyra_db::DbAsync,
    covers_root: &Path,
    owner_id: DbId,
    target_dir: &Path,
    provider_id: &str,
    ext: &str,
    bytes: &[u8],
) -> Result<PathBuf> {
    let cover = store_cover_file(
        db,
        covers_root,
        NewCoverFile {
            owner_id,
            source: CoverSource::Provider,
            dir: target_dir,
            ext,
            bytes,
            provider_id: Some(provider_id),
            select: false,
        },
    )
    .await?;
    Ok(PathBuf::from(cover.path))
}

/// Whether an automatic provider sync may fetch a cover for the owner: never for a locked owner,
/// and otherwise only when it has no cover at all unless the stored provider cover is to be
/// replaced.
fn provider_sync_allowed(
    db: &agdb::DbAny,
    owner_id: DbId,
    local_cover: Option<&Path>,
    options: CoverSyncOptions,
) -> Result<bool> {
    if covers_locked(db, owner_id)? {
        return Ok(false);
    }
    if options.replace_existing {
        return Ok(true);
    }
    Ok(local_cover.is_none() && !has_stored_cover(db, owner_id)?)
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

    let (allowed, provider_contexts) = {
        let db_read = db.read().await;
        let sorted_providers = active_providers(&db_read)?;
        let library = library_for_release(&db_read, release_id)?;
        let local_cover = resolve_local_cover_for_release(&db_read, tracks, paths.library_root)?;
        let allowed = provider_sync_allowed(&db_read, release_id, local_cover.as_deref(), options)?;

        let mut provider_contexts = Vec::new();
        for provider in sorted_providers {
            let provider_ids =
                provider_external_ids_for_entity(&db_read, release_id, &provider.provider_id)?;
            let release_context =
                release_context_value(release, tracks, artists, &provider_ids, library.as_ref())?;
            provider_contexts.push((provider.provider_id, release_context));
        }

        (allowed, provider_contexts)
    };

    if provider_contexts.is_empty() || !allowed {
        return Ok(false);
    }

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

        let target = save_provider_cover(
            db,
            paths.covers_root,
            release_id,
            &target_dir,
            &provider_id,
            ext,
            &bytes,
        )
        .await?;
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

    let (allowed, provider_contexts) = {
        let db_read = db.read().await;
        let sorted_providers = active_providers(&db_read)?.only(provider_filter);
        let allowed = provider_sync_allowed(&db_read, artist_id, None, options)?;

        let mut provider_contexts = Vec::new();
        for provider in sorted_providers {
            let provider_ids =
                provider_external_ids_for_entity(&db_read, artist_id, &provider.provider_id)?;
            let artist_context = artist_context_value(artist, &provider_ids)?;
            provider_contexts.push((provider.provider_id, artist_context));
        }

        (allowed, provider_contexts)
    };

    if provider_contexts.is_empty() || !allowed {
        return Ok(false);
    }

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

        let target = save_provider_cover(
            db,
            paths.covers_root,
            artist_id,
            &target_dir,
            &provider_id,
            ext,
            &bytes,
        )
        .await?;
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
    release_id: DbId,
    release_title: String,
    target_dir: PathBuf,
    provider_contexts: Vec<(String, serde_json::Value)>,
}

struct CoverDownloadTask {
    release_id: DbId,
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
        task.release_id,
        &task.target_dir,
        &task.provider_id,
        ext,
        &bytes,
    )
    .await
    {
        Ok(target) => target,
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

#[derive(Clone, Copy, Debug)]
pub(crate) enum CoverScope {
    Release,
    Artist,
}

impl CoverScope {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Release => "release",
            Self::Artist => "artist",
        }
    }
}

/// Local covers found for a library's releases, and the errors of those whose search failed.
type ResolvedCovers = (Vec<(DbId, PathBuf)>, Vec<anyhow::Error>);

/// The local covers of the library's unlocked releases. Artists have no local covers.
fn resolve_local_covers_for_library(
    scope: CoverScope,
    db: &agdb::DbAny,
    library_id: DbId,
    paths: CoverPaths<'_>,
) -> Result<ResolvedCovers> {
    let (mut resolved, mut failed) = (Vec::new(), Vec::new());
    if matches!(scope, CoverScope::Artist) {
        return Ok((resolved, failed));
    }
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
            if !provider_sync_allowed(&db_read, release_id, local_cover.as_deref(), options)? {
                continue;
            }

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
                release_id,
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
                release_id: ctx.release_id,
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

async fn sync_covers_for_library_scope(
    scope: CoverScope,
    db: &lyra_db::DbAsync,
    paths: CoverPaths<'_>,
    library_id: DbId,
    options: CoverSyncOptions,
    provider_filter: Option<&str>,
) -> Result<usize> {
    match scope {
        CoverScope::Release => {
            sync_release_covers_for_library(db, paths, library_id, options, provider_filter).await
        }
        CoverScope::Artist => {
            sync_artist_covers_for_library(db, paths, library_id, options, provider_filter).await
        }
    }
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

/// Records the release's library cover as its local cover, leaving a locked release alone.
/// Returns the cover found and whether its record changed.
pub(crate) async fn record_local_release_cover(
    db: &lyra_db::DbAsync,
    release_id: DbId,
    library_root: Option<&Path>,
) -> Result<Option<(PathBuf, bool)>> {
    let local_cover = {
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
    let Some(cover_path) = local_cover else {
        return Ok(None);
    };
    let changed = upsert_local_cover_metadata(db, release_id, &cover_path).await?;
    Ok(Some((cover_path, changed)))
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
    entity_type: &'static str,
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

    let count = computed.len();
    for c in computed {
        let cover = Cover {
            db_id: None,
            id: nanoid!(),
            path: c.path,
            mime_type: c.mime_type,
            hash: c.hash,
            blurhash: c.blurhash,
            provider_id: None,
        };
        let mut db_write = db.write().await;
        db_write.transaction_mut(|t| {
            lyra_db::covers::upsert(t, c.owner_id, CoverSource::Local, cover)
        })?;
    }

    tracing::info!(count, entity_type, "persisted cover metadata");
    if let Some(first) = unreadable.first() {
        anyhow::bail!(
            "{} {entity_type} covers could not be read; first: {first:#}",
            unreadable.len()
        );
    }
    Ok(count)
}

pub(crate) async fn sync_and_persist_covers_for_library(
    db: &lyra_db::DbAsync,
    paths: CoverPaths<'_>,
    library_id: DbId,
    options: CoverSyncOptions,
    provider_filter: Option<&str>,
    scope: CoverScope,
) -> Result<usize> {
    let scope_name = scope.as_str();
    let _synced =
        sync_covers_for_library_scope(scope, db, paths, library_id, options, provider_filter)
            .await?;

    let (resolved, unresolved) = {
        let db_read = db.read().await;
        resolve_local_covers_for_library(scope, &db_read, library_id, paths)?
    };
    let persisted = if resolved.is_empty() {
        Ok(0)
    } else {
        sync_local_cover_metadata_from_resolved(db, &resolved, scope_name).await
    };
    let persisted = match (persisted, unresolved.first()) {
        (persisted, None) => persisted?,
        (persisted, Some(first)) => {
            let unresolved = anyhow::anyhow!(
                "{} {scope_name} covers could not be resolved; first: {first:#}",
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
        scope = scope_name,
        persisted,
        "synced library cover metadata"
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
        let first = save_provider_cover(
            &db,
            &covers_root,
            release_id,
            &target_dir,
            "provider",
            "png",
            PNG_1X1,
        )
        .await?;
        let second = save_provider_cover(
            &db,
            &covers_root,
            release_id,
            &target_dir,
            "provider",
            "png",
            PNG_1X1,
        )
        .await?;

        assert!(library_cover.is_file());
        assert!(!first.exists(), "the replaced provider file is discarded");
        assert!(second.is_file());
        let db_read = db.read().await;
        let local = lyra_db::covers::get_source(&*db_read, release_id, CoverSource::Local)?
            .expect("the local cover stays recorded");
        assert_eq!(PathBuf::from(local.path), library_cover);
        let provider = lyra_db::covers::get_source(&*db_read, release_id, CoverSource::Provider)?
            .expect("the provider cover is recorded");
        assert_eq!(PathBuf::from(&provider.path), second);
        assert_eq!(provider.provider_id.as_deref(), Some("provider"));
        Ok(())
    }

    #[test]
    fn discarding_never_touches_files_outside_the_covers_root() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let covers_root = root.path().join("covers");
        let library_cover = root.path().join("library").join("cover.png");
        std_fs::create_dir_all(library_cover.parent().unwrap())?;
        std_fs::write(&library_cover, PNG_1X1)?;
        let escaping = covers_root.join("..").join("library").join("cover.png");

        discard_cover_file(&covers_root, &library_cover.to_string_lossy());
        discard_cover_file(&covers_root, &escaping.to_string_lossy());

        assert!(library_cover.is_file());
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

    #[test]
    fn provider_sync_skips_locked_and_covered_owners() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;
        let replace = CoverSyncOptions {
            replace_existing: true,
            force_refresh: false,
        };
        assert!(provider_sync_allowed(
            &db,
            release_id,
            None,
            CoverSyncOptions::default()
        )?);
        assert!(!provider_sync_allowed(
            &db,
            release_id,
            Some(Path::new("/music/cover.jpg")),
            CoverSyncOptions::default()
        )?);
        assert!(provider_sync_allowed(
            &db,
            release_id,
            Some(Path::new("/music/cover.jpg")),
            replace
        )?);

        let mut release = lyra_db::releases::get_by_id(&db, release_id)?.expect("release exists");
        release.locked = Some(true);
        lyra_db::releases::update(&mut db, &release)?;
        assert!(!provider_sync_allowed(&db, release_id, None, replace)?);
        Ok(())
    }
}
