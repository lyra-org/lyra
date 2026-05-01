// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use nanoid::nanoid;

use crate::{
    STATE,
    config::Config,
    db::{
        self,
        Library,
    },
};

use super::{
    StartLibrarySyncResult,
    start_library_sync,
    sync_library,
};

/// Override `library.name` in config.json if another library already uses
/// this default — names must be unique.
const DEFAULT_CONFIGURED_LIBRARY_NAME: &str = "Music";

pub(crate) async fn prepare_configured_library(
    config: &Config,
    capture_mode: bool,
) -> anyhow::Result<Option<Library>> {
    let Some(library) = resolve_configured_library(config).await? else {
        return Ok(None);
    };

    sync_configured_library(STATE.db.get(), &library, capture_mode).await?;
    Ok(Some(library))
}

async fn resolve_configured_library(config: &Config) -> anyhow::Result<Option<Library>> {
    let Some(library_config) = &config.library else {
        return Ok(None);
    };
    let Some(path) = library_config.path.as_ref() else {
        return Ok(None);
    };

    // Store the user's raw path so symlink retargeting still works; canonical
    // form is the comparison key only. `canonicalize` is a syscall — keep it
    // off the write lock.
    let stored_path = path.clone();
    let path_key = {
        let candidate = stored_path.clone();
        tokio::task::spawn_blocking(move || db::libraries::path_key_for(&candidate))
            .await
            .map_err(|e| anyhow::anyhow!("path canonicalize task panicked: {e}"))?
    };

    let display_name = match library_config.name.as_deref() {
        Some(raw) => db::libraries::normalize_library_name_display(raw)
            .map_err(|e| anyhow::anyhow!("invalid config library.name '{raw}': {e}"))?,
        None => DEFAULT_CONFIGURED_LIBRARY_NAME.to_string(),
    };
    let language = library_config.language.clone();
    let country = library_config.country.clone();

    // Lookup-or-create in one txn so a crash between the node and edge
    // inserts can't orphan a Library invisible to subsequent `find`s.
    let db = STATE.db.get();
    let mut db_write = db.write().await;
    let lookup_path = stored_path.clone();
    let lookup_key = path_key.clone();
    let outcome =
        db_write.transaction_mut(|t| -> Result<Library, db::libraries::LibraryCreateError> {
            if let Some(existing) = db::libraries::find_by_path_key(t, &lookup_key)? {
                return Ok(existing);
            }
            db::libraries::create(
                t,
                db::libraries::LibraryInsert {
                    id: nanoid!(),
                    name: display_name,
                    path: lookup_path,
                    path_key: lookup_key,
                    language,
                    country,
                },
            )
        });

    match outcome {
        Ok(library) => Ok(Some(library)),
        Err(db::libraries::LibraryCreateError::NameInUse(name)) => Err(anyhow::anyhow!(
            "configured library name '{name}' is already in use; set `library.name` in \
             config.json to a unique value"
        )),
        Err(db::libraries::LibraryCreateError::PathInUse(conflicting_path)) => {
            // Unreachable unless `find_by_path_key` and `create` disagree on
            // normalization — log the inputs so the divergence is debuggable.
            tracing::error!(
                conflicting = %conflicting_path.display(),
                configured = %stored_path.display(),
                path_key = %path_key,
                "configured-library path_key divergence"
            );
            Err(anyhow::anyhow!(
                "configured library path {} is already in use by another library; \
                 schema invariant violated",
                conflicting_path.display()
            ))
        }
        Err(db::libraries::LibraryCreateError::InvalidName(e)) => {
            Err(anyhow::anyhow!("invalid configured library name: {e}"))
        }
        Err(db::libraries::LibraryCreateError::Db(e)) => Err(e),
    }
}

async fn sync_configured_library(
    db: crate::db::DbAsync,
    library: &Library,
    capture_mode: bool,
) -> anyhow::Result<()> {
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("configured library missing db_id"))?;

    if capture_mode {
        sync_library(&db, library).await?;
        return Ok(());
    }

    match start_library_sync(db, library.clone()).await? {
        StartLibrarySyncResult::Started { run_id } => {
            tracing::info!(
                library_id = library_db_id.0,
                run_id,
                "started background library sync"
            );
        }
        StartLibrarySyncResult::AlreadyRunning { run_id } => {
            tracing::info!(
                library_id = library_db_id.0,
                run_id,
                "library sync already running"
            );
        }
    }

    Ok(())
}
