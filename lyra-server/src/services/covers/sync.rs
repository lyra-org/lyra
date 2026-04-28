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
use axum::http::header::CONTENT_TYPE;
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
use url::Url;

use nanoid::nanoid;

use crate::db::{
    self,
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
        normalize_extension,
    },
    providers::{
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
        cover_dirs_for_release,
        cover_path_from_db,
        resolve_cover_for_artist_id,
        resolve_cover_for_release,
    },
};

const MAX_COVER_BYTES: usize = 20 * 1024 * 1024;

fn extension_from_url(url: &str) -> Option<&'static str> {
    let parsed = Url::parse(url).ok()?;
    let last_segment = parsed.path_segments()?.next_back()?;
    let extension = std::path::Path::new(last_segment)
        .extension()
        .and_then(|s| s.to_str())?;
    normalize_extension(extension)
}

fn extension_from_content_type(content_type: &str) -> Option<&'static str> {
    match content_type
        .split(';')
        .next()?
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "image/jpeg" | "image/jpg" => Some("jpg"),
        "image/png" => Some("png"),
        "image/webp" => Some("webp"),
        _ => None,
    }
}

async fn download_cover(url: &str) -> Result<(Vec<u8>, Option<String>)> {
    let response = harmony_http::shared_client()
        .get(url)
        .send()
        .await
        .with_context(|| format!("failed to download cover from {url}"))?;

    if !response.status().is_success() {
        return Err(anyhow!(
            "cover download failed with status {} for {}",
            response.status(),
            url
        ));
    }

    if let Some(size) = response.content_length() {
        if size > MAX_COVER_BYTES as u64 {
            return Err(anyhow!(
                "cover download from {url} exceeded max size {MAX_COVER_BYTES}"
            ));
        }
    }

    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string);
    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("failed to read cover response from {url}"))?
        .to_vec();

    if bytes.len() > MAX_COVER_BYTES {
        return Err(anyhow!(
            "cover download from {url} exceeded max size {MAX_COVER_BYTES}"
        ));
    }

    Ok((bytes, content_type))
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

async fn prune_other_covers(dirs: &[PathBuf], target_path: Option<&Path>) -> Result<()> {
    for dir in dirs {
        for ext in COVER_EXTENSIONS {
            let cover = dir.join(format!("cover.{ext}"));
            if target_path == Some(cover.as_path()) {
                continue;
            }

            if cover.exists() {
                let _ = fs::remove_file(&cover).await;
            }
        }
    }

    Ok(())
}

pub(crate) async fn sync_release_cover_for_tracks(
    db: &crate::db::DbAsync,
    tracks: &[Track],
    release: &Release,
    artists: &[Artist],
    paths: CoverPaths<'_>,
    options: CoverSyncOptions,
) -> Result<bool> {
    let Some(release_id) = release.db_id.clone().map(Into::<DbId>::into) else {
        return Ok(false);
    };

    let (existing_cover, candidate_dirs, provider_contexts) = {
        let db_read = db.read().await;
        let providers = db::providers::get(&db_read)?;
        let library = library_for_release(&db_read, release_id)?;
        let existing_cover = resolve_cover_for_release(&db_read, release_id, tracks, paths)?;
        let configured_target_dir = configured_cover_dir_for_release(paths.covers_root, release_id);
        let candidate_dirs = if let Some(ref configured_dir) = configured_target_dir {
            vec![configured_dir.clone()]
        } else {
            cover_dirs_for_release(&db_read, tracks, paths.library_root)?
        };

        let mut sorted_providers = providers;
        sorted_providers.retain(|provider| provider.enabled);
        sorted_providers.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then(a.provider_id.cmp(&b.provider_id))
        });
        let mut provider_contexts = Vec::new();
        for provider in sorted_providers {
            let provider_ids =
                provider_external_ids_for_entity(&db_read, release_id, &provider.provider_id)?;
            let release_context =
                release_context_value(release, tracks, artists, &provider_ids, library.as_ref())?;
            provider_contexts.push((provider.provider_id, release_context));
        }

        (existing_cover, candidate_dirs, provider_contexts)
    };

    if provider_contexts.is_empty() {
        return Ok(false);
    }
    if existing_cover.is_some() && !options.replace_existing {
        return Ok(false);
    }
    if candidate_dirs.is_empty() {
        return Ok(false);
    }

    let configured_target_dir = configured_cover_dir_for_release(paths.covers_root, release_id);
    let target_dir = if let Some(configured_dir) = configured_target_dir {
        configured_dir
    } else if options.replace_existing {
        existing_cover
            .as_ref()
            .and_then(|existing| existing.parent().map(Path::to_path_buf))
            .or_else(|| candidate_dirs.first().cloned())
            .ok_or_else(|| anyhow!("unable to determine cover directory"))?
    } else {
        candidate_dirs
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("unable to determine cover directory"))?
    };

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

        if Url::parse(&cover_url).is_err() {
            tracing::warn!(
                provider = %provider_id,
                cover_url = %cover_url,
                "invalid cover_url from provider cover handler"
            );
            continue;
        }

        let (bytes, content_type) = match download_cover(&cover_url).await {
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

        let ext = extension_from_url(&cover_url).or_else(|| {
            content_type
                .as_deref()
                .and_then(extension_from_content_type)
        });
        let Some(ext) = ext else {
            tracing::warn!(
                provider = %provider_id,
                cover_url = %cover_url,
                "unsupported cover extension or mime"
            );
            continue;
        };

        let target = target_dir.join(format!("cover.{ext}"));
        if options.replace_existing {
            if let Some(existing) = existing_cover.as_ref() {
                if existing != &target {
                    let _ = fs::remove_file(existing).await;
                }
            }

            let mut prune_dirs = candidate_dirs.clone();
            if !prune_dirs.iter().any(|dir| dir == &target_dir) {
                prune_dirs.push(target_dir.clone());
            }
            prune_other_covers(&prune_dirs, Some(&target)).await?;
        }

        write_cover_image(&target, &bytes).await?;
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
    db: &crate::db::DbAsync,
    artist: &Artist,
    paths: CoverPaths<'_>,
    options: CoverSyncOptions,
) -> Result<bool> {
    let Some(artist_id) = artist.db_id.clone().map(Into::<DbId>::into) else {
        return Ok(false);
    };

    let (existing_cover, candidate_dirs, provider_contexts) = {
        let db_read = db.read().await;
        let providers = db::providers::get(&db_read)?;
        let existing_cover = resolve_cover_for_artist_id(&db_read, artist_id, paths)?;
        let configured_target_dir = configured_cover_dir_for_artist(paths.covers_root, artist_id);
        let candidate_dirs = if let Some(ref configured_dir) = configured_target_dir {
            vec![configured_dir.clone()]
        } else {
            existing_cover
                .as_ref()
                .and_then(|existing| existing.parent().map(Path::to_path_buf))
                .into_iter()
                .collect()
        };

        let mut sorted_providers = providers;
        sorted_providers.retain(|provider| provider.enabled);
        sorted_providers.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then(a.provider_id.cmp(&b.provider_id))
        });
        let mut provider_contexts = Vec::new();
        for provider in sorted_providers {
            let provider_ids =
                provider_external_ids_for_entity(&db_read, artist_id, &provider.provider_id)?;
            let artist_context = artist_context_value(artist, &provider_ids)?;
            provider_contexts.push((provider.provider_id, artist_context));
        }

        (existing_cover, candidate_dirs, provider_contexts)
    };

    if provider_contexts.is_empty() {
        return Ok(false);
    }
    if existing_cover.is_some() && !options.replace_existing {
        return Ok(false);
    }
    if candidate_dirs.is_empty() {
        return Ok(false);
    }

    let configured_target_dir = configured_cover_dir_for_artist(paths.covers_root, artist_id);
    let target_dir = if let Some(configured_dir) = configured_target_dir {
        configured_dir
    } else {
        existing_cover
            .as_ref()
            .and_then(|existing| existing.parent().map(Path::to_path_buf))
            .or_else(|| candidate_dirs.first().cloned())
            .ok_or_else(|| anyhow!("unable to determine artist cover directory"))?
    };

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

        if Url::parse(&cover_url).is_err() {
            tracing::warn!(
                provider = %provider_id,
                cover_url = %cover_url,
                "invalid cover_url from provider cover handler"
            );
            continue;
        }

        let (bytes, content_type) = match download_cover(&cover_url).await {
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

        let ext = extension_from_url(&cover_url).or_else(|| {
            content_type
                .as_deref()
                .and_then(extension_from_content_type)
        });
        let Some(ext) = ext else {
            tracing::warn!(
                provider = %provider_id,
                cover_url = %cover_url,
                "unsupported cover extension or mime"
            );
            continue;
        };

        let target = target_dir.join(format!("cover.{ext}"));
        if options.replace_existing {
            if let Some(existing) = existing_cover.as_ref() {
                if existing != &target {
                    let _ = fs::remove_file(existing).await;
                }
            }

            let mut prune_dirs = candidate_dirs.clone();
            if !prune_dirs.iter().any(|dir| dir == &target_dir) {
                prune_dirs.push(target_dir.clone());
            }
            prune_other_covers(&prune_dirs, Some(&target)).await?;
        }

        write_cover_image(&target, &bytes).await?;
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
    release_title: String,
    existing_cover: Option<PathBuf>,
    candidate_dirs: Vec<PathBuf>,
    configured_target_dir: Option<PathBuf>,
    provider_contexts: Vec<(String, serde_json::Value)>,
}

struct CoverDownloadTask {
    release_title: String,
    provider_id: String,
    cover_url: String,
    target_dir: PathBuf,
    candidate_dirs: Vec<PathBuf>,
    existing_cover: Option<PathBuf>,
    replace_existing: bool,
}

async fn execute_cover_download(task: CoverDownloadTask) -> bool {
    let (bytes, content_type) = match download_cover(&task.cover_url).await {
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

    let ext = extension_from_url(&task.cover_url).or_else(|| {
        content_type
            .as_deref()
            .and_then(extension_from_content_type)
    });
    let Some(ext) = ext else {
        tracing::warn!(
            provider = %task.provider_id,
            cover_url = %task.cover_url,
            "unsupported cover extension or mime"
        );
        return false;
    };

    let target = task.target_dir.join(format!("cover.{ext}"));
    if task.replace_existing {
        if let Some(existing) = task.existing_cover.as_ref() {
            if existing != &target {
                let _ = fs::remove_file(existing).await;
            }
        }

        let mut prune_dirs = task.candidate_dirs.clone();
        if !prune_dirs.iter().any(|dir| dir == &task.target_dir) {
            prune_dirs.push(task.target_dir.clone());
        }
        if let Err(err) = prune_other_covers(&prune_dirs, Some(&target)).await {
            tracing::warn!(
                release = %task.release_title,
                error = %err,
                "failed to prune other covers"
            );
        }
    }

    if let Err(err) = write_cover_image(&target, &bytes).await {
        tracing::warn!(
            provider = %task.provider_id,
            cover_url = %task.cover_url,
            error = %err,
            "failed to write cover image"
        );
        return false;
    }

    tracing::info!(
        provider = %task.provider_id,
        release = %task.release_title,
        cover_path = %target.display(),
        "synced release cover from provider cover handler"
    );
    true
}

pub(crate) fn resolve_release_covers(
    db: &agdb::DbAny,
    library_id: DbId,
    paths: CoverPaths<'_>,
) -> Result<Vec<(DbId, PathBuf)>> {
    let releases = db::releases::get(db, library_id)?;
    let mut resolved = Vec::new();
    for release in &releases {
        let Some(release_id) = release.db_id.clone().map(Into::<DbId>::into) else {
            continue;
        };
        let tracks = db::tracks::get_direct(db, release_id)?;
        if let Some(cover_path) = resolve_cover_for_release(db, release_id, &tracks, paths)? {
            resolved.push((release_id, cover_path));
        }
    }
    Ok(resolved)
}

pub(crate) async fn sync_release_covers_for_library(
    db: &crate::db::DbAsync,
    paths: CoverPaths<'_>,
    library_id: DbId,
    options: CoverSyncOptions,
) -> Result<usize> {
    let resolve_contexts = {
        let db_read = db.read().await;
        let releases = db::releases::get(&db_read, library_id)?;

        let mut sorted_providers: Vec<_> = db::providers::get(&db_read)?
            .into_iter()
            .filter(|p| p.enabled)
            .collect();
        if sorted_providers.is_empty() {
            return Ok(0);
        }
        sorted_providers.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then(a.provider_id.cmp(&b.provider_id))
        });

        let mut contexts = Vec::new();
        for release in &releases {
            let Some(release_id) = release.db_id.clone().map(Into::<DbId>::into) else {
                continue;
            };

            if !options.replace_existing && cover_path_from_db(&db_read, release_id)?.is_some() {
                continue;
            }

            let tracks = db::tracks::get_direct(&db_read, release_id)?;
            let artists = db::artists::get(&db_read, release_id)?;
            let library = library_for_release(&db_read, release_id)?;
            let existing_cover = resolve_cover_for_release(&db_read, release_id, &tracks, paths)?;
            let configured_target_dir =
                configured_cover_dir_for_release(paths.covers_root, release_id);
            let candidate_dirs = if let Some(ref dir) = configured_target_dir {
                vec![dir.clone()]
            } else {
                cover_dirs_for_release(&db_read, &tracks, paths.library_root)?
            };

            let mut provider_contexts = Vec::new();
            for provider in &sorted_providers {
                let ext_ids =
                    provider_external_ids_for_entity(&db_read, release_id, &provider.provider_id)?;
                let ctx =
                    release_context_value(release, &tracks, &artists, &ext_ids, library.as_ref())?;
                provider_contexts.push((provider.provider_id.clone(), ctx));
            }

            contexts.push(CoverResolveContext {
                release_title: release.release_title.clone(),
                existing_cover,
                candidate_dirs,
                configured_target_dir,
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
        if ctx.candidate_dirs.is_empty() {
            continue;
        }

        let target_dir = if let Some(dir) = ctx.configured_target_dir {
            dir
        } else if options.replace_existing {
            ctx.existing_cover
                .as_ref()
                .and_then(|p| p.parent().map(Path::to_path_buf))
                .or_else(|| ctx.candidate_dirs.first().cloned())
                .ok_or_else(|| anyhow!("unable to determine cover directory"))?
        } else {
            ctx.candidate_dirs
                .first()
                .cloned()
                .ok_or_else(|| anyhow!("unable to determine cover directory"))?
        };

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

            if Url::parse(&cover_url).is_err() {
                tracing::warn!(
                    provider = %provider_id,
                    cover_url = %cover_url,
                    "invalid cover_url from provider cover handler"
                );
                continue;
            }

            downloads.push(CoverDownloadTask {
                release_title: ctx.release_title.clone(),
                provider_id: provider_id.clone(),
                cover_url,
                target_dir: target_dir.clone(),
                candidate_dirs: ctx.candidate_dirs.clone(),
                existing_cover: ctx.existing_cover.clone(),
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
        tasks.spawn(async move {
            let result = execute_cover_download(dl).await;
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

pub(crate) async fn upsert_cover_metadata(
    db: &crate::db::DbAsync,
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
                cover_hash(&path),
                cover_blurhash(&path),
                cover_mime_from_path(&path).to_string(),
            )
        }
    })
    .await
    .context("cover metadata computation task panicked")?;

    let Some(hash) = hash else {
        return Ok(false);
    };

    let path_str = path.to_string_lossy().into_owned();
    let mut db_write = db.write().await;
    db_write.transaction_mut(|t| {
        if let Some(existing) = db::covers::get(t, owner_id)?
            && existing.hash == hash
            && existing.path == path_str
        {
            return Ok(false);
        }

        let cover = Cover {
            db_id: None,
            id: nanoid!(),
            path: path_str,
            mime_type,
            hash,
            blurhash,
        };
        db::covers::upsert(t, owner_id, cover)?;
        Ok(true)
    })
}

pub(crate) async fn upsert_release_cover_metadata(
    db: &crate::db::DbAsync,
    release_id: DbId,
    cover_path: &Path,
) -> Result<bool> {
    upsert_cover_metadata(db, release_id, cover_path).await
}

pub(crate) async fn upsert_artist_cover_metadata(
    db: &crate::db::DbAsync,
    artist_id: DbId,
    cover_path: &Path,
) -> Result<bool> {
    upsert_cover_metadata(db, artist_id, cover_path).await
}

struct PendingCover {
    release_id: DbId,
    path: PathBuf,
    mime_type: String,
}

struct ComputedCover {
    release_id: DbId,
    path: String,
    mime_type: String,
    hash: String,
    blurhash: Option<String>,
}

pub(crate) async fn sync_release_cover_metadata_from_resolved(
    db: &crate::db::DbAsync,
    resolved: &[(DbId, PathBuf)],
) -> Result<usize> {
    let mut pending: Vec<PendingCover> = Vec::new();

    for (release_id, cover_path) in resolved {
        let mime_type = cover_mime_from_path(cover_path).to_string();

        let skip = {
            let db_read = db.read().await;
            if let Some(existing) = db::covers::get(&db_read, *release_id)? {
                cover_hash(cover_path).is_some_and(|hash| existing.hash == hash)
            } else {
                false
            }
        };
        if skip {
            continue;
        }

        pending.push(PendingCover {
            release_id: *release_id,
            path: cover_path.clone(),
            mime_type,
        });
    }

    if pending.is_empty() {
        return Ok(0);
    }

    let computed: Vec<ComputedCover> = pending
        .into_par_iter()
        .filter_map(|p| {
            let hash = cover_hash(&p.path)?;
            let blurhash = cover_blurhash(&p.path);
            Some(ComputedCover {
                release_id: p.release_id,
                path: p.path.to_string_lossy().into_owned(),
                mime_type: p.mime_type,
                hash,
                blurhash,
            })
        })
        .collect();

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
        db_write.transaction_mut(|t| db::covers::upsert(t, c.release_id, cover))?;
    }

    tracing::info!(count, "persisted release cover metadata");
    Ok(count)
}

pub(crate) async fn eager_sync_cover_metadata(
    db: &crate::db::DbAsync,
    library_db_id: DbId,
    library_dir: &Path,
) {
    let covers_root = super::resolve::configured_covers_root();
    let cover_paths = super::CoverPaths {
        library_root: Some(library_dir),
        covers_root: covers_root.as_deref(),
    };
    let resolved = {
        let db_read = db.read().await;
        match resolve_release_covers(&db_read, library_db_id, cover_paths) {
            Ok(r) => r,
            Err(err) => {
                tracing::warn!(
                    library_db_id = library_db_id.0,
                    error = %err,
                    "eager cover resolution failed during library sync"
                );
                return;
            }
        }
    };
    if !resolved.is_empty() {
        if let Err(err) = sync_release_cover_metadata_from_resolved(db, &resolved).await {
            tracing::warn!(
                library_db_id = library_db_id.0,
                error = %err,
                "eager cover metadata sync failed during library sync"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs as std_fs,
        sync::Arc,
    };

    use super::*;
    use crate::db::test_db::{
        insert_release,
        new_test_db,
    };
    use tokio::sync::RwLock;

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
        let db = Arc::new(RwLock::new(new_test_db()?));
        let release = Release {
            db_id: None,
            id: nanoid!(),
            release_title: "Release".to_string(),
            sort_title: None,
            release_type: None,
            release_date: None,
            locked: None,
            created_at: None,
            ctime: None,
        };

        let synced = sync_release_cover_for_tracks(
            &db,
            &[],
            &release,
            &[],
            CoverPaths {
                library_root: None,
                covers_root: None,
            },
            CoverSyncOptions::default(),
        )
        .await?;

        assert!(!synced);
        Ok(())
    }

    #[tokio::test]
    async fn upsert_release_cover_metadata_is_idempotent_for_same_file() -> anyhow::Result<()> {
        let db = Arc::new(RwLock::new(new_test_db()?));
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
}
