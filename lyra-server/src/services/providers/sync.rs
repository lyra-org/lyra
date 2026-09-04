// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    sync::Arc,
    time::Duration,
};

use tokio::{
    sync::{
        Notify,
        Semaphore,
    },
    task::JoinSet,
    time::Instant,
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
        provider_registry,
        sync_locks,
    },
};

use super::{
    ProviderServiceError,
    refresh::LibraryRefreshOptions,
};

pub(crate) async fn run_provider_sync(provider_id: &str) -> Result<(), ProviderServiceError> {
    {
        let registry = provider_registry().read_owned().await;
        if registry
            .get_refresh_callback(provider_id, EntityType::Release)
            .is_none()
        {
            return Err(ProviderServiceError::NoRefreshHandler(
                provider_id.to_string(),
            ));
        }
    }

    {
        let mut locks = sync_locks().lock_owned().await;
        if !locks.insert(provider_id.to_string()) {
            return Err(ProviderServiceError::SyncAlreadyRunning(
                provider_id.to_string(),
            ));
        }
    }

    let result = run_provider_sync_inner(provider_id).await;

    sync_locks().lock_owned().await.remove(provider_id);

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

/// When the next sync is due for the interval currently configured, or
/// `None` while syncing is disabled or the interval is too far out to
/// represent.
fn next_sync_due(interval_secs: u64, last_run: Instant) -> Option<Instant> {
    if interval_secs == 0 {
        return None;
    }
    last_run.checked_add(Duration::from_secs(interval_secs))
}

async fn sleep_until_due(due: Option<Instant>) {
    match due {
        Some(due) => tokio::time::sleep_until(due).await,
        None => std::future::pending().await,
    }
}

/// Reads `sync.interval_secs` from the live config: every settings
/// publish wakes the loop so a changed interval reschedules immediately.
pub(crate) async fn run_provider_sync_loop(shutdown: Arc<Notify>) {
    run_all_provider_syncs().await;
    let mut last_run = Instant::now();

    loop {
        let due = next_sync_due(STATE.config().sync.interval_secs, last_run);
        tokio::select! {
            _ = sleep_until_due(due) => {
                tracing::info!("running scheduled provider sync");
                run_all_provider_syncs().await;
                last_run = Instant::now();
            }
            _ = STATE.settings_changed.notified() => {}
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
        let registry = provider_registry().read_owned().await;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_sync_due_is_none_when_disabled_and_set_when_enabled() {
        let last_run = Instant::now();

        assert!(next_sync_due(0, last_run).is_none());
        assert_eq!(
            next_sync_due(30, last_run),
            Some(last_run + Duration::from_secs(30))
        );
        assert!(next_sync_due(0, last_run).is_none());
    }

    #[test]
    fn next_sync_due_is_none_when_the_interval_overflows() {
        assert!(next_sync_due(u64::MAX, Instant::now()).is_none());
    }
}
