use super::*;
use std::time::Instant;

use crate::services::plugin_dispatch::PluginCallbackHandle;

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

/// Held across a handler's wait; its drop is the handler's cancellation. While `gate` is closed
/// that cancellation can't finish, keeping the executor thread in it.
struct PendingCall(async_mpsc::UnboundedSender<()>, Arc<CancelGate>);

impl Drop for PendingCall {
    fn drop(&mut self) {
        let _ = self.0.send(());
        self.1.pass();
    }
}

#[derive(Default)]
struct CancelGate {
    closed: std::sync::Mutex<bool>,
    opened: std::sync::Condvar,
}

impl CancelGate {
    fn close(&self) {
        *self.closed.lock().expect("gate poisoned") = true;
    }

    fn open(&self) {
        *self.closed.lock().expect("gate poisoned") = false;
        self.opened.notify_all();
    }

    fn pass(&self) {
        let mut closed = self.closed.lock().expect("gate poisoned");
        while *closed {
            closed = self.opened.wait(closed).expect("gate poisoned");
        }
    }
}

struct TestExecutor {
    cancel_gate: Arc<CancelGate>,
    handle: PluginExecutorHandle,
    slow: PluginCallbackHandle,
    fast: PluginCallbackHandle,
    api: PluginCallbackHandle,
    started: async_mpsc::UnboundedReceiver<()>,
    dropped: async_mpsc::UnboundedReceiver<()>,
    release: Arc<Notify>,
}

fn test_executor() -> Result<TestExecutor> {
    futures::executor::block_on(crate::plugins::api::initialize_registry(
        std::collections::HashSet::new(),
        None,
    ));
    let (tx, rx) = plugin_executor_channel(4);
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let (crashed, exit) = exit_channel();
    let (started_tx, started) = async_mpsc::unbounded_channel();
    let (dropped_tx, dropped) = async_mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let release_for_thread = release.clone();
    let cancel_gate = Arc::new(CancelGate::default());
    let cancel_gate_for_thread = cancel_gate.clone();
    thread::spawn(move || {
        let setup = (|| -> Result<_> {
            let runtime = PluginExecutor::with_manifests(Arc::from(vec![PluginManifest {
                schema_version: 1,
                id: "dispatch-test".into(),
                name: "Dispatch test".into(),
                version: semver::Version::new(1, 0, 0),
                host_requirement: semver::VersionReq::STAR,
                description: String::new(),
                entrypoint: Some("init.luau".into()),
                scopes: vec![
                    "lyra.metadata".into(),
                    "lyra.api".into(),
                    "test.gate".into(),
                ],
                scope_reasons: Default::default(),
                dependencies: Default::default(),
            }]))?;
            runtime
                .vm
                .data()
                .get::<RequireRuntime>()?
                .register(harmony_core::GatedModule::new(
                    "test.gate",
                    ModuleSpec::new("test/gate")
                        .function(FunctionSpec::new("wait").call_async(Arc::new(move |_| {
                            let _ = started_tx.send(());
                            let guard =
                                PendingCall(dropped_tx.clone(), cancel_gate_for_thread.clone());
                            let release = release_for_thread.clone();
                            Ok(harmony_luau::ScheduledFuture::new(async move {
                                let _guard = guard;
                                release.notified().await;
                                Ok(true)
                            }))
                        })))
                        .install(|_| Ok(ModuleExport::new(()))),
                ))?;
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
                end, { auth = "public" })
            "#[..])?;
            let registry = futures::executor::block_on(
                crate::services::providers::provider_registry().read_owned(),
            );
            let slow = registry
                .get_refresh_callback("dispatch-slow", crate::services::EntityType::Release)
                .copied()
                .context("slow callback")?;
            let fast = registry
                .get_refresh_callback("dispatch-fast", crate::services::EntityType::Release)
                .copied()
                .context("fast callback")?;
            let api = futures::executor::block_on(crate::plugins::api::tests::registered_handler(
                "GET",
                "/dispatch-wait",
            ))
            .context("API callback")?;
            Ok((runtime, slow, fast, api))
        })();
        match setup {
            Ok((runtime, slow, fast, api)) => {
                if ready_tx.send(Ok((slow, fast, api))).is_ok() {
                    run_plugin_executor_thread(runtime, rx, crashed);
                }
            }
            Err(error) => {
                let _ = ready_tx.send(Err(error));
            }
        }
    });
    let (slow, fast, api) = ready_rx.recv()??;
    Ok(TestExecutor {
        handle: PluginExecutorHandle {
            tx,
            streams: Default::default(),
            entrypoints: Default::default(),
            exit,
            scope: Default::default(),
            running: Default::default(),
        },
        slow,
        fast,
        api,
        started,
        dropped,
        cancel_gate,
        release,
    })
}

fn request(handler: PluginCallbackHandle, timeout: Duration) -> MetadataRefreshRequest {
    MetadataRefreshRequest {
        handler,
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
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
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
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
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
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
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
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
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
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = test_executor()?;
    let api = tokio::spawn({
        let handle = executor.handle.clone();
        let handler = executor.api;
        async move {
            handle
                .dispatch_api_handler(ApiHandlerRequest {
                    handler,
                    plugin_id: "dispatch-test".into(),
                    method: "GET".into(),
                    path: "/dispatch-wait".into(),
                    headers: Vec::new(),
                    base_url: None,
                    query: Default::default(),
                    params: Default::default(),
                    body: Vec::new(),
                    auth: None,
                    client: Default::default(),
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

/// An executor running no plugins.
fn empty_executor() -> Result<PluginExecutorHandle> {
    let (handle, _) = PluginExecutorHandle::spawn(|| {
        Ok((PluginExecutor::with_manifests(Arc::from([]))?, Vec::new()))
    })?;
    Ok(handle)
}

#[tokio::test]
async fn a_panic_ends_the_executor_as_a_crash_and_fails_its_callers() -> Result<()> {
    let handle = empty_executor()?;
    let exit = handle.exit();

    handle
        .inject_panic()
        .await
        .expect_err("a panicking executor drops the reply");
    assert_eq!(exit.wait().await, ExecutorExitKind::Crashed);
    let error = handle
        .has_plugin("demo")
        .await
        .expect_err("a crashed executor serves no requests");
    assert!(format!("{error:#}").contains("unavailable"), "{error:#}");
    Ok(())
}

#[tokio::test]
async fn shutdown_and_dropped_handles_end_the_executor_as_a_stop() -> Result<()> {
    let shut_down = empty_executor()?;
    let exit = shut_down.exit();
    shut_down.shutdown().await?;
    assert_eq!(exit.wait().await, ExecutorExitKind::Stopped);

    let dropped = empty_executor()?;
    let exit = dropped.exit();
    drop(dropped);
    assert_eq!(exit.wait().await, ExecutorExitKind::Stopped);
    Ok(())
}

/// Starts the slow handler and returns once it holds its slot.
async fn hold_slot(
    executor: &mut TestExecutor,
) -> Result<tokio::task::JoinHandle<Result<MetadataRefreshResult>>> {
    let running = tokio::spawn({
        let handle = executor.handle.clone();
        let request = request(executor.slow, Duration::from_secs(30));
        async move { handle.dispatch_metadata_refresh(request).await }
    });
    signal(&mut executor.started).await?;
    Ok(running)
}

/// Starts a drain and returns once it stands in line for the slots.
async fn start_drain(
    handle: &PluginExecutorHandle,
    limit: Duration,
) -> Result<tokio::task::JoinHandle<DrainOutcome>> {
    let drain = tokio::spawn({
        let handle = handle.clone();
        async move { handle.drain(limit, &CancellationToken::new()).await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while handle.tx.slots.available_permits() > 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(drain)
}

const LONG_LIMIT: Duration = Duration::from_secs(60);

#[tokio::test]
async fn a_drain_waits_for_the_running_call_and_fails_the_one_behind_it() -> Result<()> {
    use crate::services::providers::{
        ProviderCallError,
        ProviderCallStage,
        ProviderLookup,
        with_provider_call,
    };

    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = test_executor()?;
    let running = hold_slot(&mut executor).await?;
    let drain = start_drain(&executor.handle, LONG_LIMIT).await?;

    let provider_call = tokio::spawn({
        let handle = executor.handle.clone();
        let request = request(executor.fast, Duration::from_secs(30));
        async move {
            with_provider_call(
                "dispatch-fast",
                ProviderCallStage::MetadataRefresh,
                crate::services::plugin_dispatch::TransitionWait::interactive(),
                |_| Some(ProviderLookup::Call(())),
                |(), _| {
                    let (handle, request) = (handle.clone(), request.clone());
                    async move { handle.dispatch_metadata_refresh(request).await }
                },
            )
            .await
        }
    });
    let direct_call = tokio::spawn({
        let handle = executor.handle.clone();
        let request = request(executor.fast, Duration::from_secs(30));
        async move { handle.dispatch_metadata_refresh(request).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!provider_call.is_finished());
    assert!(!drain.is_finished());

    executor.release.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(2), running).await???;
    assert_eq!(result.values, vec![serde_json::json!(42)]);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), drain).await??,
        DrainOutcome::Drained
    );

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), provider_call).await??,
        Err(ProviderCallError::NotRegistered)
    ));
    let error = direct_call.await?.expect_err("the call never started");
    assert!(DispatchNotStarted::caused(&error), "{error:#}");
    Ok(())
}

#[tokio::test]
async fn a_dropped_caller_does_not_let_a_drain_finish_while_its_handler_runs() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = test_executor()?;
    let running = hold_slot(&mut executor).await?;
    let drain = start_drain(&executor.handle, LONG_LIMIT).await?;

    executor.cancel_gate.close();
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    signal(&mut executor.dropped).await?;

    // The handler's cancellation has begun and can't finish, so its slot is still held.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !drain.is_finished(),
        "a drain must wait for the cancelled handler"
    );

    executor.cancel_gate.open();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), drain).await??,
        DrainOutcome::Drained
    );
    Ok(())
}

#[tokio::test]
async fn a_handler_outlasting_the_limit_does_not_hold_up_the_shutdown() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = test_executor()?;
    let running = hold_slot(&mut executor).await?;

    let outcome = tokio::time::timeout(
        Duration::from_secs(2),
        executor
            .handle
            .drain(Duration::from_millis(50), &CancellationToken::new()),
    )
    .await?;
    assert_eq!(outcome, DrainOutcome::TimedOut);

    let error = executor
        .handle
        .dispatch_metadata_refresh(request(executor.fast, Duration::from_secs(2)))
        .await
        .expect_err("a drained runtime admits nothing");
    assert!(DispatchNotStarted::caused(&error), "{error:#}");

    tokio::time::timeout(Duration::from_secs(2), executor.handle.shutdown()).await??;
    let error = tokio::time::timeout(Duration::from_secs(2), running)
        .await??
        .expect_err("shutdown cancels the running handler");
    assert!(!DispatchNotStarted::caused(&error), "{error:#}");
    Ok(())
}

#[tokio::test]
async fn a_nested_call_queued_behind_the_drain_ends_it_at_its_limit() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = test_executor()?;
    let running = hold_slot(&mut executor).await?;
    let drain = start_drain(&executor.handle, Duration::from_millis(100)).await?;

    // The running handler can't finish until the call it made does.
    let nested = tokio::spawn({
        let handle = executor.handle.clone();
        let request = request(executor.fast, Duration::from_secs(30));
        let release = executor.release.clone();
        async move {
            let result = handle.dispatch_metadata_refresh(request).await;
            release.notify_one();
            result
        }
    });

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), drain).await??,
        DrainOutcome::TimedOut
    );
    let error = tokio::time::timeout(Duration::from_secs(2), nested)
        .await??
        .expect_err("the nested call never started");
    assert!(DispatchNotStarted::caused(&error), "{error:#}");
    tokio::time::timeout(Duration::from_secs(2), running).await???;
    Ok(())
}

#[tokio::test]
async fn a_crash_ends_a_drain_at_once() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;

    let crashed = empty_executor()?;
    crashed.inject_panic().await.expect_err("it crashed");
    let outcome = tokio::time::timeout(
        Duration::from_secs(2),
        crashed.drain(LONG_LIMIT, &CancellationToken::new()),
    )
    .await?;
    assert_ne!(outcome, DrainOutcome::TimedOut);

    let mut executor = test_executor()?;
    let running = hold_slot(&mut executor).await?;
    let drain = start_drain(&executor.handle, LONG_LIMIT).await?;
    executor
        .handle
        .inject_panic()
        .await
        .expect_err("it crashed");
    let outcome = tokio::time::timeout(Duration::from_secs(2), drain).await??;
    assert_ne!(outcome, DrainOutcome::TimedOut);
    running.await?.expect_err("the crash ended the handler");
    Ok(())
}

#[tokio::test]
async fn cancelling_ends_a_drain_and_closes_the_slots() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = test_executor()?;
    let running = hold_slot(&mut executor).await?;
    let cancelled = CancellationToken::new();
    cancelled.cancel();

    let outcome = tokio::time::timeout(
        Duration::from_secs(2),
        executor.handle.drain(LONG_LIMIT, &cancelled),
    )
    .await?;
    assert_eq!(outcome, DrainOutcome::Cancelled);
    assert!(executor.handle.tx.slots.is_closed());
    running.abort();
    Ok(())
}

#[tokio::test]
async fn lossy_updates_report_never_started_while_draining() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = test_executor()?;
    let running = hold_slot(&mut executor).await?;
    let drain = start_drain(&executor.handle, LONG_LIMIT).await?;
    let update = crate::services::listens::ListenUpdatePayload {
        event: "recorded".to_string(),
        user_id: "user".to_string(),
        listens: Vec::new(),
    };

    let error = executor
        .handle
        .dispatch_listen_update(update.clone())
        .expect_err("no slot is free during a drain");
    assert!(DispatchNotStarted::caused(&error), "{error:#}");

    executor.release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), running).await???;
    drain.await?;
    let error = executor
        .handle
        .dispatch_listen_update(update)
        .expect_err("a closed runtime admits nothing");
    assert!(DispatchNotStarted::caused(&error), "{error:#}");
    Ok(())
}
