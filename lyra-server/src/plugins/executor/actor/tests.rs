use super::*;
use std::time::Instant;

use harmony_core::{
    FunctionSpec,
    ModuleExport,
    luau::RequireRuntime,
    plugin::PluginManifest,
};
use tokio::sync::{
    Notify,
    mpsc as async_mpsc,
};

struct PendingCall(async_mpsc::UnboundedSender<()>);

impl Drop for PendingCall {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

struct TestExecutor {
    handle: PluginExecutorHandle,
    slow: u64,
    fast: u64,
    api: u64,
    started: async_mpsc::UnboundedReceiver<()>,
    dropped: async_mpsc::UnboundedReceiver<()>,
    release: Arc<Notify>,
}

fn test_executor() -> Result<TestExecutor> {
    let _ = futures::executor::block_on(crate::plugins::api::install(
        axum::Router::new(),
        std::collections::HashSet::new(),
    ))?;
    let (tx, rx) = plugin_executor_channel(4);
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let (started_tx, started) = async_mpsc::unbounded_channel();
    let (dropped_tx, dropped) = async_mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let release_for_thread = release.clone();
    thread::spawn(move || {
        let setup = (|| -> Result<_> {
            let runtime = PluginExecutor::with_manifests(Arc::from(vec![PluginManifest {
                schema_version: 1,
                id: "dispatch-test".into(),
                name: "Dispatch test".into(),
                version: "1.0.0".into(),
                description: String::new(),
                entrypoint: Some("init.luau".into()),
                scopes: vec![
                    "lyra.metadata".into(),
                    "lyra.api".into(),
                    "test.gate".into(),
                ],
                dependencies: Vec::new(),
            }]))?;
            runtime.vm.data().get::<RequireRuntime>()?.register(
                ModuleSpec::new("test/gate")
                    .capability("test.gate")
                    .function(FunctionSpec::async_fn("wait").returns::<bool>().call_async(
                        Arc::new(move |_| {
                            let _ = started_tx.send(());
                            let guard = PendingCall(dropped_tx.clone());
                            let release = release_for_thread.clone();
                            Ok(harmony_luau::ScheduledFuture::new(async move {
                                let _guard = guard;
                                release.notified().await;
                                Ok(true)
                            }))
                        }),
                    ))
                    .install(|_| Ok(ModuleExport::new(()))),
            )?;
            runtime.eval_plugin_source("dispatch-test", "init.luau", &br#"
                local metadata = require("@lyra/metadata")
                local gate = require("@test/gate")
                metadata.Provider.new("dispatch-slow"):refresh(metadata.EntityType.Release, function(ctx)
                    gate.wait()
                    return ctx.value
                end)
                metadata.Provider.new("dispatch-fast"):refresh(metadata.EntityType.Release, function(ctx)
                    return ctx.value
                end)
                local api = require("@lyra/api")
                api.get("/dispatch-wait", function(_ctx)
                    gate.wait()
                    return api.response.json(200, { value = 42 })
                end, "public")
            "#[..])?;
            let registry = futures::executor::block_on(
                crate::services::providers::provider_registry().read_owned(),
            );
            let slow = registry
                .get_refresh_callback("dispatch-slow", crate::services::EntityType::Release)
                .context("slow callback")?
                .handler_id;
            let fast = registry
                .get_refresh_callback("dispatch-fast", crate::services::EntityType::Release)
                .context("fast callback")?
                .handler_id;
            let api = futures::executor::block_on(crate::plugins::api::tests::registered_handler(
                "GET",
                "/dispatch-wait",
            ))
            .context("API callback")?;
            Ok((runtime, slow, fast, api))
        })();
        match setup {
            Ok((runtime, slow, fast, api)) => {
                if ready_tx
                    .send(Ok((runtime.vm.id(), slow, fast, api)))
                    .is_ok()
                {
                    run_plugin_executor_thread(runtime, rx);
                }
            }
            Err(error) => {
                let _ = ready_tx.send(Err(error));
            }
        }
    });
    let (id, slow, fast, api) = ready_rx.recv()??;
    Ok(TestExecutor {
        handle: PluginExecutorHandle { id, tx },
        slow,
        fast,
        api,
        started,
        dropped,
        release,
    })
}

fn request(handler_id: u64, timeout: Duration) -> MetadataRefreshRequest {
    MetadataRefreshRequest {
        handler_id,
        context: serde_json::json!({ "value": 42 }),
        deadline: Instant::now() + timeout,
    }
}

async fn signal(receiver: &mut async_mpsc::UnboundedReceiver<()>) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await?
        .context("signal channel closed")
}

#[tokio::test]
async fn slow_metadata_does_not_block_other_callbacks_and_dropped_call_is_cancelled() -> Result<()>
{
    let _guard = crate::testing::runtime_test_lock().await;
    crate::testing::init_default_test_state()?;
    let mut executor = test_executor()?;
    let slow = tokio::spawn({
        let handle = executor.handle.clone();
        let request = request(executor.slow, Duration::from_secs(10));
        async move { handle.dispatch_metadata_refresh(request).await }
    });
    signal(&mut executor.started).await?;
    let fast = tokio::time::timeout(
        Duration::from_secs(2),
        executor
            .handle
            .dispatch_metadata_refresh(request(executor.fast, Duration::from_secs(2))),
    )
    .await??;
    assert_eq!(fast.values, vec![serde_json::json!(42)]);
    assert!(!slow.is_finished());
    slow.abort();
    assert!(slow.await.unwrap_err().is_cancelled());
    signal(&mut executor.dropped).await?;
    let fast = executor
        .handle
        .dispatch_metadata_refresh(request(executor.fast, Duration::from_secs(2)))
        .await?;
    assert_eq!(fast.values, vec![serde_json::json!(42)]);
    Ok(())
}

#[tokio::test]
async fn metadata_deadline_cancels_waiting_host_future() -> Result<()> {
    let _guard = crate::testing::runtime_test_lock().await;
    crate::testing::init_default_test_state()?;
    let mut executor = test_executor()?;
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        executor
            .handle
            .dispatch_metadata_refresh(request(executor.slow, Duration::from_millis(100))),
    )
    .await?;
    assert!(result.is_err());
    signal(&mut executor.started).await?;
    signal(&mut executor.dropped).await?;
    Ok(())
}

#[tokio::test]
async fn expired_metadata_never_starts_and_completed_callback_returns_its_values() -> Result<()> {
    let _guard = crate::testing::runtime_test_lock().await;
    crate::testing::init_default_test_state()?;
    let mut executor = test_executor()?;
    let mut expired = request(executor.slow, Duration::ZERO);
    expired.deadline = Instant::now() - Duration::from_secs(1);
    assert!(
        executor
            .handle
            .dispatch_metadata_refresh(expired)
            .await
            .is_err()
    );
    assert!(executor.started.try_recv().is_err());
    let slow = tokio::spawn({
        let handle = executor.handle.clone();
        let request = request(executor.slow, Duration::from_secs(2));
        async move { handle.dispatch_metadata_refresh(request).await }
    });
    signal(&mut executor.started).await?;
    executor.release.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(2), slow).await???;
    assert_eq!(result.values, vec![serde_json::json!(42)]);
    signal(&mut executor.dropped).await?;
    Ok(())
}

#[tokio::test]
async fn active_callbacks_retain_capacity_until_cancelled() -> Result<()> {
    let _guard = crate::testing::runtime_test_lock().await;
    crate::testing::init_default_test_state()?;
    let mut executor = test_executor()?;
    let mut calls = Vec::new();
    for _ in 0..4 {
        let handle = executor.handle.clone();
        let request = request(executor.slow, Duration::from_secs(10));
        calls.push(tokio::spawn(async move {
            handle.dispatch_metadata_refresh(request).await
        }));
        signal(&mut executor.started).await?;
    }
    let handle = executor.handle.clone();
    let request = request(executor.slow, Duration::from_secs(10));
    let waiting = tokio::spawn(async move { handle.dispatch_metadata_refresh(request).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), executor.started.recv())
            .await
            .is_err()
    );
    let cancelled = calls.pop().unwrap();
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
    signal(&mut executor.dropped).await?;
    signal(&mut executor.started).await?;
    calls.push(waiting);
    for call in calls {
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        signal(&mut executor.dropped).await?;
    }
    Ok(())
}

#[tokio::test]
async fn waiting_api_callback_does_not_delay_metadata_deadlines() -> Result<()> {
    let _guard = crate::testing::runtime_test_lock().await;
    crate::testing::init_default_test_state()?;
    let mut executor = test_executor()?;
    let api = tokio::spawn({
        let handle = executor.handle.clone();
        let handler_id = executor.api;
        async move {
            handle
                .dispatch_api_handler(ApiHandlerRequest {
                    handler_id,
                    plugin_id: "dispatch-test".into(),
                    method: "GET".into(),
                    path: "/dispatch-wait".into(),
                    headers: Vec::new(),
                    query: Default::default(),
                    params: Default::default(),
                    body: Vec::new(),
                    auth: None,
                    client_key: None,
                })
                .await
        }
    });
    signal(&mut executor.started).await?;
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        executor
            .handle
            .dispatch_metadata_refresh(request(executor.slow, Duration::from_millis(100))),
    )
    .await?;
    assert!(result.is_err());
    signal(&mut executor.started).await?;
    signal(&mut executor.dropped).await?;
    assert!(!api.is_finished());
    executor.release.notify_one();
    let response = tokio::time::timeout(Duration::from_secs(2), api).await???;
    assert_eq!(response.status, 200);
    signal(&mut executor.dropped).await?;
    Ok(())
}
