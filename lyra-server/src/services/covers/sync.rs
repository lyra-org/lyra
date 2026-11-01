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
};

use super::{
    CoverPaths,
    CoverSyncOptions,
    image::{
        COVER_EXTENSIONS,
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
        cover_path_from_db,
        resolve_cover_for_artist_id,
        resolve_cover_for_release,
    },
};

const MAX_COVER_BYTES: usize = 20 * 1024 * 1024;

/// Downloads the cover at `url` that the provider's cover handler for
/// `entity_type` returned, on behalf of the plugin that still owns that
/// handler, with the extension its bytes call for.
///
/// Registering a cover handler is the `harmony.http` scope gate, so the
/// download needs no scope check of its own. The owner is looked up here,
/// not when the URL was produced, so that URLs queued by a batch sync are
/// skipped once their plugin unloads or loses the handler.
async fn download_cover(
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

    let temp = path.with_file_name(".lyra-cover-tmp");
    let mut file = fs::File::create(&temp)
        .await
        .with_context(|| format!("failed to create temporary cover file {}", temp.display()))?;
    file.write_all(bytes)
        .await
        .with_context(|| format!("failed to write temporary cover file {}", temp.display()))?;
    file.flush().await?;
    fs::rename(&temp, path)
        .await
        .with_context(|| format!("failed to finalize cover file {}", path.display()))?;

    Ok(())
}

/// Saves a provider cover in the entity's directory under the covers root and records it as the
/// entity's cover. Replacing removes only covers in that directory, never files in a library.
async fn save_provider_cover(
    db: &lyra_db::DbAsync,
    owner_id: DbId,
    target_dir: &Path,
    ext: &str,
    bytes: &[u8],
    replace_existing: bool,
) -> Result<PathBuf> {
    let target = target_dir.join(format!("cover.{ext}"));
    if replace_existing {
        for other_ext in COVER_EXTENSIONS {
            let other = target_dir.join(format!("cover.{other_ext}"));
            if other != target && other.exists() {
                let _ = fs::remove_file(&other).await;
            }
        }
    }

    write_cover_image(&target, bytes).await?;
    upsert_cover_metadata(db, owner_id, &target).await?;
    Ok(target)
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

    let (existing_cover, provider_contexts) = {
        let db_read = db.read().await;
        let sorted_providers = active_providers(&db_read)?;
        let library = library_for_release(&db_read, release_id)?;
        let existing_cover = resolve_cover_for_release(&db_read, release_id, tracks, paths)?;

        let mut provider_contexts = Vec::new();
        for provider in sorted_providers {
            let provider_ids =
                provider_external_ids_for_entity(&db_read, release_id, &provider.provider_id)?;
            let release_context =
                release_context_value(release, tracks, artists, &provider_ids, library.as_ref())?;
            provider_contexts.push((provider.provider_id, release_context));
        }

        (existing_cover, provider_contexts)
    };

    if provider_contexts.is_empty() {
        return Ok(false);
    }
    if existing_cover.is_some() && !options.replace_existing {
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
            release_id,
            &target_dir,
            ext,
            &bytes,
            options.replace_existing,
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

    let (existing_cover, provider_contexts) = {
        let db_read = db.read().await;
        let sorted_providers = active_providers(&db_read)?.only(provider_filter);
        let existing_cover = resolve_cover_for_artist_id(&db_read, artist_id, paths)?;

        let mut provider_contexts = Vec::new();
        for provider in sorted_providers {
            let provider_ids =
                provider_external_ids_for_entity(&db_read, artist_id, &provider.provider_id)?;
            let artist_context = artist_context_value(artist, &provider_ids)?;
            provider_contexts.push((provider.provider_id, artist_context));
        }

        (existing_cover, provider_contexts)
    };

    if provider_contexts.is_empty() {
        return Ok(false);
    }
    if existing_cover.is_some() && !options.replace_existing {
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
            artist_id,
            &target_dir,
            ext,
            &bytes,
            options.replace_existing,
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
    existing_cover: Option<PathBuf>,
    target_dir: PathBuf,
    provider_contexts: Vec<(String, serde_json::Value)>,
}

struct CoverDownloadTask {
    release_id: DbId,
    release_title: String,
    provider_id: String,
    cover_url: String,
    target_dir: PathBuf,
    replace_existing: bool,
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
        task.release_id,
        &task.target_dir,
        ext,
        &bytes,
        task.replace_existing,
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

/// Covers found for a library's releases or artists, and the errors of those whose search failed.
type ResolvedCovers = (Vec<(DbId, PathBuf)>, Vec<anyhow::Error>);

fn resolve_covers_for_library(
    scope: CoverScope,
    db: &agdb::DbAny,
    library_id: DbId,
    paths: CoverPaths<'_>,
) -> Result<ResolvedCovers> {
    match scope {
        CoverScope::Release => resolve_release_covers(db, library_id, paths),
        CoverScope::Artist => resolve_artist_covers(db, library_id, paths),
    }
}

pub(crate) fn resolve_release_covers(
    db: &agdb::DbAny,
    library_id: DbId,
    paths: CoverPaths<'_>,
) -> Result<ResolvedCovers> {
    let releases = lyra_db::releases::get(db, library_id)?;
    let (mut resolved, mut failed) = (Vec::new(), Vec::new());
    for release in &releases {
        let Some(release_id) = release.db_id.clone().map(Into::<DbId>::into) else {
            continue;
        };
        let tracks = lyra_db::tracks::get_direct(db, release_id)?;
        match resolve_cover_for_release(db, release_id, &tracks, paths) {
            Ok(Some(cover_path)) => resolved.push((release_id, cover_path)),
            Ok(None) => {}
            Err(err) => failed.push(err),
        }
    }
    Ok((resolved, failed))
}

pub(crate) fn resolve_artist_covers(
    db: &agdb::DbAny,
    library_id: DbId,
    paths: CoverPaths<'_>,
) -> Result<ResolvedCovers> {
    let artists = lyra_db::artists::get_by_library(db, library_id)?;
    let (mut resolved, mut failed) = (Vec::new(), Vec::new());
    for artist in &artists {
        let Some(artist_id) = artist.db_id.clone().map(Into::<DbId>::into) else {
            continue;
        };
        match resolve_cover_for_artist_id(db, artist_id, paths) {
            Ok(Some(cover_path)) => resolved.push((artist_id, cover_path)),
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

            if !options.replace_existing && cover_path_from_db(&db_read, release_id)?.is_some() {
                continue;
            }

            let tracks = lyra_db::tracks::get_direct(&db_read, release_id)?;
            let artists = lyra_db::artists::get(&db_read, release_id)?;
            let library = library_for_release(&db_read, release_id)?;
            // The metadata pass after this one reports a cover search that fails.
            let Ok(existing_cover) =
                resolve_cover_for_release(&db_read, release_id, &tracks, paths)
            else {
                continue;
            };
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
                existing_cover,
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
        if ctx.existing_cover.is_some() && !options.replace_existing {
            continue;
        }
        let target_dir = ctx.target_dir;

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
                target_dir: target_dir.clone(),
                replace_existing: options.replace_existing,
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

pub(crate) async fn upsert_cover_metadata(
    db: &lyra_db::DbAsync,
    owner_id: DbId,
    cover_path: &Path,
) -> Result<bool> {
    if !cover_path.is_file() {
        return Ok(false);
    }

    let path = cover_path.to_path_buf();
    let (hash, blurhash, mime_type) = tokio::task::spawn_blocking({
        let path = path.clone();
        move || {
            (
                read_cover_hash(&path),
                cover_blurhash(&path),
                cover_mime_from_path(&path).to_string(),
            )
        }
    })
    .await
    .context("cover metadata computation task panicked")?;
    let hash = hash?;

    let path_str = path.to_string_lossy().into_owned();
    let mut db_write = db.write().await;
    // An unchanged cover needs no mutable access, which would count as a catalog change.
    if let Some(existing) = lyra_db::covers::get(&*db_write, owner_id)?
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
        };
        lyra_db::covers::upsert(t, owner_id, cover)?;
        Ok(true)
    })
}

pub(crate) async fn upsert_release_cover_metadata(
    db: &lyra_db::DbAsync,
    release_id: DbId,
    cover_path: &Path,
) -> Result<bool> {
    upsert_cover_metadata(db, release_id, cover_path).await
}

pub(crate) async fn upsert_artist_cover_metadata(
    db: &lyra_db::DbAsync,
    artist_id: DbId,
    cover_path: &Path,
) -> Result<bool> {
    upsert_cover_metadata(db, artist_id, cover_path).await
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

async fn sync_cover_metadata_from_resolved(
    db: &lyra_db::DbAsync,
    resolved: &[(DbId, PathBuf)],
    entity_type: &'static str,
) -> Result<usize> {
    let mut pending: Vec<PendingCover> = Vec::new();

    for (owner_id, cover_path) in resolved {
        let mime_type = cover_mime_from_path(cover_path).to_string();

        let skip = {
            let db_read = db.read().await;
            if let Some(existing) = lyra_db::covers::get(&db_read, *owner_id)? {
                cover_hash(cover_path).is_some_and(|hash| existing.hash == hash)
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
        };
        let mut db_write = db.write().await;
        db_write.transaction_mut(|t| lyra_db::covers::upsert(t, c.owner_id, cover))?;
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

pub(crate) async fn sync_release_cover_metadata_from_resolved(
    db: &lyra_db::DbAsync,
    resolved: &[(DbId, PathBuf)],
) -> Result<usize> {
    sync_cover_metadata_from_resolved(db, resolved, "release").await
}

pub(crate) async fn sync_artist_cover_metadata_from_resolved(
    db: &lyra_db::DbAsync,
    resolved: &[(DbId, PathBuf)],
) -> Result<usize> {
    sync_cover_metadata_from_resolved(db, resolved, "artist").await
}

async fn sync_cover_metadata_for_scope(
    scope: CoverScope,
    db: &lyra_db::DbAsync,
    resolved: &[(DbId, PathBuf)],
) -> Result<usize> {
    match scope {
        CoverScope::Release => sync_release_cover_metadata_from_resolved(db, resolved).await,
        CoverScope::Artist => sync_artist_cover_metadata_from_resolved(db, resolved).await,
    }
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
        resolve_covers_for_library(scope, &db_read, library_id, paths)?
    };
    let persisted = if resolved.is_empty() {
        Ok(0)
    } else {
        sync_cover_metadata_for_scope(scope, db, &resolved).await
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
    use crate::services::covers::resolve_cover_for_release_id;
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
    async fn upsert_release_cover_metadata_is_idempotent_for_same_file() -> anyhow::Result<()> {
        let db = lyra_db::DbAsync::new(new_test_db()?);
        let release_id = {
            let mut db_write = db.write().await;
            insert_release(&mut db_write, "Release")?
        };
        let cover_path = write_test_cover()?;

        let inserted = upsert_release_cover_metadata(&db, release_id, &cover_path).await?;
        let inserted_again = upsert_release_cover_metadata(&db, release_id, &cover_path).await?;

        assert!(inserted);
        assert!(!inserted_again);

        let _ = std_fs::remove_file(cover_path);
        Ok(())
    }

    #[tokio::test]
    async fn replacing_a_scanned_cover_keeps_the_library_file() -> anyhow::Result<()> {
        let db = lyra_db::DbAsync::new(new_test_db()?);
        let release_id = {
            let mut db_write = db.write().await;
            insert_release(&mut db_write, "Release")?
        };
        let root = std::env::temp_dir().join(format!("lyra-test-covers-{}", nanoid!()));
        let library_cover = root.join("library").join("Release").join("cover.png");
        std_fs::create_dir_all(library_cover.parent().unwrap())?;
        std_fs::write(&library_cover, PNG_1X1)?;
        upsert_release_cover_metadata(&db, release_id, &library_cover).await?;

        let covers_root = root.join("covers");
        let target_dir = configured_cover_dir_for_release(&covers_root, release_id);
        std_fs::create_dir_all(&target_dir)?;
        std_fs::write(target_dir.join("cover.jpg"), PNG_1X1)?;

        let saved = save_provider_cover(&db, release_id, &target_dir, "png", PNG_1X1, true).await?;

        assert!(library_cover.is_file());
        assert!(!target_dir.join("cover.jpg").exists());
        let resolved = resolve_cover_for_release_id(
            &*db.read().await,
            release_id,
            CoverPaths {
                library_root: Some(&root.join("library")),
                covers_root: &covers_root,
            },
        )?;
        assert_eq!(resolved, Some(saved));

        let _ = std_fs::remove_dir_all(root);
        Ok(())
    }
}
