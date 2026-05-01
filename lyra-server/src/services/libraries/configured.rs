// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::QueryBuilder;
use nanoid::nanoid;

use crate::{
    STATE,
    config::Config,
    db::Library,
};

use super::{
    StartLibrarySyncResult,
    start_library_sync,
    sync_library,
};

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
    let Some(library) = &config.library else {
        return Ok(None);
    };
    let Some(path) = library.path.as_ref() else {
        return Ok(None);
    };

    let db = STATE.db.get();
    let mut library = {
        let existing = {
            let db_read = db.read().await;
            let libraries: Vec<Library> = db_read
                .exec(
                    QueryBuilder::select()
                        .elements::<Library>()
                        .search()
                        .from("libraries")
                        .query(),
                )?
                .try_into()?;
            let mut matching: Vec<Library> = libraries
                .into_iter()
                .filter(|lib| lib.directory == *path)
                .collect();
            if matching.len() > 1 {
                tracing::warn!(
                    path = %path.display(),
                    count = matching.len(),
                    "multiple libraries found for configured path"
                );
            }
            matching.pop()
        };

        existing.unwrap_or_else(|| Library {
            db_id: None,
            id: nanoid!(),
            name: "Music".to_string(),
            directory: path.clone(),
            language: library.language.clone(),
            country: library.country.clone(),
        })
    };

    if library.db_id.is_none() {
        let library_db_id =
            db.write()
                .await
                .transaction_mut(|t| -> anyhow::Result<agdb::DbId> {
                    let qr = t.exec_mut(QueryBuilder::insert().element(&library).query())?;
                    let id = qr
                        .ids()
                        .first()
                        .copied()
                        .ok_or_else(|| anyhow::anyhow!("library insert missing id"))?;
                    t.exec_mut(
                        QueryBuilder::insert()
                            .edges()
                            .from("libraries")
                            .to(id)
                            .query(),
                    )?;
                    Ok(id)
                })?;
        library.db_id = Some(library_db_id);
    }

    Ok(Some(library))
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
