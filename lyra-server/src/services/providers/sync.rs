// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashSet,
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
    services::CoverSyncOptions,
    services::EntityType,
    services::STATE,
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

    let _lock = ProviderSyncLock::acquire(provider_id)?;
    run_provider_sync_inner(provider_id).await
}

/// Marks a provider's sync as running until dropped, however the sync ends.
struct ProviderSyncLock {
    locks: Arc<std::sync::Mutex<HashSet<String>>>,
    provider_id: String,
}

impl ProviderSyncLock {
    fn acquire(provider_id: &str) -> Result<Self, ProviderServiceError> {
        let locks = sync_locks();
        if !locks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(provider_id.to_string())
        {
            return Err(ProviderServiceError::SyncAlreadyRunning(
                provider_id.to_string(),
            ));
        }
        Ok(Self {
            locks,
            provider_id: provider_id.to_string(),
        })
    }
}

impl Drop for ProviderSyncLock {
    fn drop(&mut self) {
        self.locks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.provider_id);
    }
}

async fn run_provider_sync_inner(provider_id: &str) -> Result<(), ProviderServiceError> {
    let libraries = {
        let db = STATE.db.read().await;
        lyra_db::libraries::get(&db)?
    };

    let library_db_ids: Vec<_> = libraries
        .into_iter()
        .filter_map(|library| library.db_id)
        .collect();

    let provider_id_owned: Arc<str> = Arc::from(provider_id);
    let concurrency = Arc::new(Semaphore::new(4));
    let shutdown = crate::services::shutdown::token();
    let mut tasks = JoinSet::new();
    let mut interrupted = false;
    for library_db_id in library_db_ids {
        let provider_id = Arc::clone(&provider_id_owned);
        let permit = tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                interrupted = true;
                break;
            }
            permit = concurrency.clone().acquire_owned() => {
                permit.expect("library refresh semaphore closed")
            }
        };
        tasks.spawn(async move {
            let options = LibraryRefreshOptions {
                covers: CoverSyncOptions::default(),
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

    if interrupted {
        return Err(ProviderServiceError::ShuttingDown);
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
    let shutdown = crate::services::shutdown::token();
    let running_library_syncs = running_library_sync_count().await;
    if running_library_syncs > 0 {
        tracing::info!(
            running_library_syncs,
            "waiting for library syncs to finish before provider syncs"
        );
        tokio::select! {
            () = wait_for_running_library_syncs() => {}
            () = shutdown.cancelled() => return,
        }
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
        if shutdown.is_cancelled() {
            return;
        }
        if let Err(err) = run_provider_sync(provider_id).await {
            tracing::warn!(provider_id, error = %err, "provider sync failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_provider_sync_lock_is_released_when_its_sync_is_dropped() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;

        let held = ProviderSyncLock::acquire("provider")?;
        assert!(matches!(
            ProviderSyncLock::acquire("provider"),
            Err(ProviderServiceError::SyncAlreadyRunning(_))
        ));
        let sync = tokio::spawn(async move {
            let _held = held;
            panic!("sync panicked");
        });
        assert!(sync.await.unwrap_err().is_panic());

        drop(ProviderSyncLock::acquire("provider")?);
        Ok(())
    }

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

    struct ShutdownResetGuard;

    impl Drop for ShutdownResetGuard {
        fn drop(&mut self) {
            crate::services::shutdown::reset();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_provider_sync_starts_no_library_refresh_once_shutdown_began() -> anyhow::Result<()> {
        use crate::services::testing::{
            FakePluginDispatch,
            FakeResponse,
        };

        let _guard = crate::services::testing::runtime_test_lock().await;
        let _shutdown = ShutdownResetGuard;
        crate::services::testing::init_default_test_state()?;
        crate::services::shutdown::reset();
        let dispatch = FakePluginDispatch::install();
        let gate = Arc::new(Semaphore::new(0));
        let handler = dispatch.add(FakeResponse::Held(
            gate.clone(),
            Box::new(FakeResponse::Value(serde_json::Value::Null)),
        ));
        lyra_db::providers::register(&mut *STATE.db.write().await, "held")?;
        {
            let registry = provider_registry();
            let mut registry = registry.write().await;
            registry.register(
                crate::services::plugin_scope::PluginId::new("held_plugin")?,
                "held".to_string(),
                None,
            )?;
            registry.set_refresh_callback("held", EntityType::Release, handler);
        }
        {
            let mut db = STATE.db.write().await;
            for index in 0..5 {
                let library = lyra_db::test_db::insert_library(
                    &mut db,
                    &format!("Library {index}"),
                    &format!("/library-{index}"),
                )?;
                let release = lyra_db::fixtures::insert_release(&mut db, "Release")?;
                lyra_db::fixtures::connect(&mut db, library, release)?;
            }
        }

        let sync = tokio::spawn(async { run_provider_sync("held").await });
        // Four refreshes hold every permit, one of them calling the provider, leaving the fifth
        // library waiting for a permit.
        for _ in 0..500 {
            if !dispatch.calls(handler).is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(dispatch.calls(handler).len(), 1);
        crate::services::shutdown::cancel();
        gate.add_permits(16);
        let result = tokio::time::timeout(Duration::from_secs(5), sync).await??;
        assert!(matches!(result, Err(ProviderServiceError::ShuttingDown)));

        assert_eq!(dispatch.calls(handler).len(), 4);
        Ok(())
    }
}
