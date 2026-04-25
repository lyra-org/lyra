// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use tokio::{
    sync::{
        Notify,
        Semaphore,
    },
    task::JoinSet,
};

use crate::{
    STATE,
    db,
    services::EntityType,
    services::libraries::{
        running_library_sync_count,
        wait_for_running_library_syncs,
    },
    services::providers::{
        PROVIDER_REGISTRY,
        SYNC_LOCKS,
    },
};

use super::{
    ProviderServiceError,
    refresh::LibraryRefreshOptions,
};

pub(crate) async fn run_provider_sync(provider_id: &str) -> Result<(), ProviderServiceError> {
    {
        let registry = PROVIDER_REGISTRY.read().await;
        if registry
            .get_refresh_handler(provider_id, EntityType::Release)
            .is_none()
        {
            return Err(ProviderServiceError::NoRefreshHandler(
                provider_id.to_string(),
            ));
        }
    }

    {
        let mut locks = SYNC_LOCKS.lock().await;
        if !locks.insert(provider_id.to_string()) {
            return Err(ProviderServiceError::SyncAlreadyRunning(
                provider_id.to_string(),
            ));
        }
    }

    let result = run_provider_sync_inner(provider_id).await;

    SYNC_LOCKS.lock().await.remove(provider_id);

    result
}

async fn run_provider_sync_inner(provider_id: &str) -> Result<(), ProviderServiceError> {
    let libraries = {
        let db = STATE.db.read().await;
        db::libraries::get(&db)?
    };

    let library_db_ids: Vec<_> = libraries
        .into_iter()
        .filter_map(|library| library.db_id)
        .collect();

    let provider_id_owned: Arc<str> = Arc::from(provider_id);
    let concurrency = Arc::new(Semaphore::new(4));
    let mut tasks = JoinSet::new();
    for library_db_id in library_db_ids {
        let provider_id = Arc::clone(&provider_id_owned);
        let permit = concurrency
            .clone()
            .acquire_owned()
            .await
            .expect("library refresh semaphore closed");
        tasks.spawn(async move {
            let options = LibraryRefreshOptions {
                replace_cover: false,
                force_refresh: false,
                apply_sync_filters: true,
                provider_id: Some(&provider_id),
            };
            if let Err(err) =
                super::refresh::refresh_library_metadata(library_db_id, &options).await
            {
                tracing::warn!(
                    library_db_id = library_db_id.0,
                    provider_id = provider_id.as_ref(),
                    error = %err,
                    "library refresh failed during provider sync"
                );
            }
            drop(permit);
        });
    }

    while let Some(result) = tasks.join_next().await {
        if let Err(err) = result {
            tracing::warn!(error = %err, "library refresh task panicked during provider sync");
        }
    }

    Ok(())
}

pub(crate) async fn run_provider_sync_loop(interval_secs: u64, shutdown: Arc<Notify>) {
    run_all_provider_syncs().await;

    if interval_secs == 0 {
        return;
    }

    let interval = std::time::Duration::from_secs(interval_secs);
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {
                tracing::info!("running scheduled provider sync");
                run_all_provider_syncs().await;
            }
            _ = shutdown.notified() => {
                tracing::info!("background sync loop shutting down");
                break;
            }
        }
    }
}

async fn run_all_provider_syncs() {
    let running_library_syncs = running_library_sync_count().await;
    if running_library_syncs > 0 {
        tracing::info!(
            running_library_syncs,
            "waiting for library syncs to finish before provider syncs"
        );
        wait_for_running_library_syncs().await;
    }

    let provider_ids = {
        let registry = PROVIDER_REGISTRY.read().await;
        registry.providers_with_refresh_handler(EntityType::Release)
    };

    if provider_ids.is_empty() {
        return;
    }

    tracing::info!(count = provider_ids.len(), "running provider syncs");

    for provider_id in &provider_ids {
        if let Err(err) = run_provider_sync(provider_id).await {
            tracing::warn!(provider_id, error = %err, "provider sync failed");
        }
    }
}
