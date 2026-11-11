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
    rounds: Arc<std::sync::atomic::AtomicU64>,
}

/// What the test plugin's `@test/gate` reports, and what holds its `wait` back.
struct Gate {
    started: async_mpsc::UnboundedSender<()>,
    dropped: async_mpsc::UnboundedSender<()>,
    /// The slow handler went on after its wait.
    resumed: async_mpsc::UnboundedSender<()>,
    release: Arc<Notify>,
    cancel_gate: Arc<CancelGate>,
}

struct DispatchHandlers {
    slow: PluginCallbackHandle,
    fast: PluginCallbackHandle,
    api: PluginCallbackHandle,
}

fn manifest(id: &str, scopes: &[&str]) -> PluginManifest {
    PluginManifest {
        schema_version: 1,
        id: id.into(),
        name: id.into(),
        version: semver::Version::new(1, 0, 0),
        host_requirement: semver::VersionReq::STAR,
        description: String::new(),
        entrypoint: Some("init.luau".into()),
        scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
        scope_reasons: Default::default(),
        scope_grants: Default::default(),
        dependencies: Default::default(),
    }
}

/// An executor on the calling thread running the test plugin, whose slow handlers wait at `gate`.
fn dispatch_runtime(gate: Gate) -> Result<(PluginExecutor, DispatchHandlers)> {
    let Gate {
        started,
        dropped,
        resumed,
        release,
        cancel_gate,
    } = gate;
    let runtime = PluginExecutor::with_manifests(Arc::from(vec![manifest(
        "dispatch-test",
        &[
            "lyra.metadata",
            "lyra.api",
            "lyra.listens",
            "lyra.listens.all",
            "harmony.task",
            "test.gate",
        ],
    )]))?;
    runtime
        .vm
        .data()
        .get::<RequireRuntime>()?
        .register(harmony_core::GatedModule::new(
            "test.gate",
            ModuleSpec::new("test/gate")
                .function(FunctionSpec::new("wait").call_async(Arc::new(move |_| {
                    let _ = started.send(());
                    let guard = PendingCall(dropped.clone(), cancel_gate.clone());
                    let release = release.clone();
                    Ok(harmony_luau::ScheduledFuture::new(async move {
                        let _guard = guard;
                        release.notified().await;
                        Ok(true)
                    }))
                })))
                .function(FunctionSpec::new("mark").call(move |_| {
                    let _ = resumed.send(());
                    Ok(())
                }))
                .install(|_| Ok(ModuleExport::new(()))),
        ))?;
    runtime.eval_plugin_source(
        "dispatch-test",
        "init.luau",
        &br#"
        local metadata = require("@lyra/metadata")
        local gate = require("@test/gate")
        metadata.Provider.new("dispatch-slow"):refresh(metadata.EntityType.Release, function(ctx)
            gate.wait()
            gate.mark()
            return ctx.value
        end)
        metadata.Provider.new("dispatch-fast"):refresh(metadata.EntityType.Release, function(ctx)
            return ctx.value
        end)
        local listens = require("@lyra/listens")
        local task = require("@harmony/task")
        listens.on_update(function(update)
            if update.event == "hold" then
                task.wait(1)
            end
        end)
        local api = require("@lyra/api")
        api.get("/dispatch-wait", function(_ctx)
            gate.wait()
            return api.response.json(200, { value = 42 })
        end, { auth = "public" })
    "#[..],
    )?;
    let registry =
        futures::executor::block_on(crate::services::providers::provider_registry().read_owned());
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
    Ok((runtime, DispatchHandlers { slow, fast, api }))
}

fn test_executor() -> Result<TestExecutor> {
    futures::executor::block_on(crate::plugins::api::initialize_registry(
        std::collections::HashSet::new(),
        None,
    ));
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let (crashed, exit) = exit_channel();
    let (started_tx, started) = async_mpsc::unbounded_channel();
    let (dropped_tx, dropped) = async_mpsc::unbounded_channel();
    let (resumed_tx, _resumed) = async_mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let cancel_gate = Arc::new(CancelGate::default());
    let gate = Gate {
        started: started_tx,
        dropped: dropped_tx,
        resumed: resumed_tx,
        release: release.clone(),
        cancel_gate: cancel_gate.clone(),
    };
    thread::spawn(move || {
        let setup = dispatch_runtime(gate).map(|(runtime, handlers)| {
            let (tx, rx) = plugin_executor_channel(4, runtime.wake_handle());
            let entrypoints = runtime.entrypoint_cancellation();
            let rounds = runtime.rounds.clone();
            (runtime, rx, (tx, entrypoints, rounds, handlers))
        });
        match setup {
            Ok((runtime, rx, ready)) => {
                if ready_tx.send(Ok(ready)).is_ok() {
                    run_plugin_executor_thread(runtime, rx, crashed);
                }
            }
            Err(error) => {
                let _ = ready_tx.send(Err(error));
            }
        }
    });
    let (tx, entrypoints, rounds, DispatchHandlers { slow, fast, api }) = ready_rx.recv()??;
    Ok(TestExecutor {
        handle: PluginExecutorHandle {
            tx,
            streams: Default::default(),
            entrypoints,
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
        rounds,
    })
}

/// The test plugin on the calling thread, where a test decides when each round runs, with what
/// its gate reports.
struct StagedExecutor {
    runtime: PluginExecutor,
    slow: PluginCallbackHandle,
    started: async_mpsc::UnboundedReceiver<()>,
    dropped: async_mpsc::UnboundedReceiver<()>,
    resumed: async_mpsc::UnboundedReceiver<()>,
    release: Arc<Notify>,
}

type RefreshAnswer = tokio::sync::oneshot::Receiver<Result<MetadataRefreshResult>>;

impl StagedExecutor {
    fn new() -> Result<Self> {
        futures::executor::block_on(crate::plugins::api::initialize_registry(
            std::collections::HashSet::new(),
            None,
        ));
        let (started_tx, started) = async_mpsc::unbounded_channel();
        let (dropped_tx, dropped) = async_mpsc::unbounded_channel();
        let (resumed_tx, resumed) = async_mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());
        let (runtime, handlers) = dispatch_runtime(Gate {
            started: started_tx,
            dropped: dropped_tx,
            resumed: resumed_tx,
            release: release.clone(),
            cancel_gate: Arc::default(),
        })?;
        Ok(Self {
            runtime,
            slow: handlers.slow,
            started,
            dropped,
            resumed,
            release,
        })
    }

    /// Starts the slow handler as its command does, running no round.
    fn start_slow(&self, timeout: Duration) -> Result<RefreshAnswer> {
        let (reply, answer) = tokio::sync::oneshot::channel();
        let permit = Arc::new(tokio::sync::Semaphore::new(1)).try_acquire_owned()?;
        self.runtime
            .start_metadata_refresh(request(self.slow, timeout), reply, permit);
        Ok(answer)
    }

    /// Starts the slow handler and runs rounds until it waits at the gate.
    fn start_slow_waiting(&mut self, timeout: Duration) -> Result<RefreshAnswer> {
        let answer = self.start_slow(timeout)?;
        self.runtime.poll_background_tasks();
        self.started.try_recv().context("the handler waits")?;
        self.runtime.poll_background_tasks();
        Ok(answer)
    }

    fn run_rounds(&self) {
        for _ in 0..4 {
            self.runtime.poll_background_tasks();
        }
    }
}

fn deadline_expired(mut answer: RefreshAnswer) -> Result<()> {
    let error = answer
        .try_recv()
        .context("the handler is answered for")?
        .expect_err("a handler past its deadline fails");
    assert!(error.to_string().contains("deadline expired"), "{error:#}");
    Ok(())
}

#[tokio::test]
async fn a_handler_whose_caller_left_before_its_first_round_never_starts() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = StagedExecutor::new()?;

    drop(executor.start_slow(Duration::from_secs(30))?);
    executor.run_rounds();

    assert!(executor.started.try_recv().is_err(), "the handler ran");
    Ok(())
}

#[tokio::test]
async fn a_handler_whose_deadline_passed_before_its_first_round_never_starts() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = StagedExecutor::new()?;

    let mut answer = executor.start_slow(Duration::from_millis(300))?;
    assert!(
        answer.try_recv().is_err(),
        "the handler was refused, not scheduled"
    );
    thread::sleep(Duration::from_millis(400));
    executor.run_rounds();

    assert!(executor.started.try_recv().is_err(), "the handler ran");
    deadline_expired(answer)
}

/// The handler's call is ready, and its wake is queued ahead of the one its caller's leaving
/// sends its watcher.
#[tokio::test]
async fn a_handler_ready_as_its_caller_leaves_is_not_resumed() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = StagedExecutor::new()?;
    let answer = executor.start_slow_waiting(Duration::from_secs(30))?;

    executor.release.notify_one();
    drop(answer);
    executor.run_rounds();

    assert!(executor.resumed.try_recv().is_err(), "the handler went on");
    assert!(executor.dropped.try_recv().is_ok(), "its call is dropped");
    Ok(())
}

#[tokio::test]
async fn a_handler_ready_after_its_deadline_is_not_resumed() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = StagedExecutor::new()?;
    let answer = executor.start_slow_waiting(Duration::from_millis(300))?;

    thread::sleep(Duration::from_millis(400));
    executor.release.notify_one();
    executor.run_rounds();

    assert!(executor.resumed.try_recv().is_err(), "the handler went on");
    assert!(executor.dropped.try_recv().is_ok(), "its call is dropped");
    deadline_expired(answer)
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

/// The rounds the executor has run, once it has gone a while without running one: it sleeps, and
/// only a wake or a deadline will bring on the next.
async fn rounds_once_asleep(executor: &TestExecutor) -> Result<u64> {
    use std::sync::atomic::Ordering;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let rounds = executor.rounds.load(Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(100)).await;
            if executor.rounds.load(Ordering::Relaxed) == rounds {
                return rounds;
            }
        }
    })
    .await
    .context("the executor kept polling with nothing to do")
}

/// A handler parked on a host call runs on because the call's readiness wakes the executor, which
/// polls nothing while the call is pending.
#[tokio::test]
async fn a_parked_handler_resumes_on_the_wake_of_its_host_call() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = test_executor()?;
    let running = hold_slot(&mut executor).await?;
    let asleep = rounds_once_asleep(&executor).await?;

    executor.release.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(10), running).await???;
    assert_eq!(result.values, vec![serde_json::json!(42)]);
    assert!(executor.rounds.load(std::sync::atomic::Ordering::Relaxed) > asleep);
    Ok(())
}

/// A handler that ended is answered before the next command, which holds the thread past the
/// handler's deadline.
#[tokio::test]
async fn a_finished_handler_is_answered_before_a_command_that_outlasts_its_deadline() -> Result<()>
{
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let executor = test_executor()?;
    let hold = || crate::services::listens::ListenUpdatePayload {
        event: "hold".to_string(),
        user_id: "user-public".to_string(),
        listens: Vec::new(),
    };

    // While the first update holds the thread, the handler and a second update queue behind it.
    executor.handle.dispatch_listen_update(hold())?;
    let fast = tokio::spawn({
        let handle = executor.handle.clone();
        let request = request(executor.fast, Duration::from_millis(1500));
        async move { handle.dispatch_metadata_refresh(request).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    executor.handle.dispatch_listen_update(hold())?;

    let result = tokio::time::timeout(Duration::from_secs(10), fast).await???;
    assert_eq!(result.values, vec![serde_json::json!(42)]);
    Ok(())
}

/// An executor running one plugin, `demo`, on the calling thread, and a producer `source` returns.
fn stream_executor(
    source: &'static str,
) -> Result<(
    PluginExecutor,
    harmony_core::CallContext,
    harmony_luau::Function,
)> {
    let runtime = PluginExecutor::with_manifests(Arc::from(vec![manifest("demo", &[])]))?;
    let context = harmony_core::CallContext {
        origin: super::super::plugin_origin("demo", "init.luau"),
        ..Default::default()
    };
    let values =
        runtime.eval_plugin_source_with_call_context(source.as_bytes(), context.clone())?;
    let Some(harmony_luau::Value::Function(produce)) = values.into_iter().next() else {
        anyhow::bail!("the chunk returns the producer");
    };
    Ok((runtime, context, produce))
}

fn queue_demo_stream(
    vm: &harmony_luau::Vm,
    produce: harmony_luau::Function,
    context: harmony_core::CallContext,
    timeout: Duration,
) -> Result<super::super::ApiResponseStream> {
    Ok(super::super::queue_stream(
        vm,
        produce,
        context,
        Instant::now() + timeout,
        super::super::StreamOrigin {
            plugin_id: "demo".to_string(),
            method: "GET".to_string(),
            path: "/stream".to_string(),
        },
    )?)
}

/// Serves `runtime` with no command ever sent, until `read` returns on another thread: whatever
/// `read` waits for, the executor has to wake for by itself. `None` if `read` ran out of time.
fn serve_until<T: Send + 'static>(
    runtime: &PluginExecutor,
    read: impl Future<Output = T> + Send + 'static,
) -> Option<T> {
    let (tx, rx) = plugin_executor_channel(4, runtime.wake_handle());
    let reader = thread::spawn(move || {
        let read = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("reader runtime")
            .block_on(async { tokio::time::timeout(Duration::from_secs(10), read).await });
        // Ends the executor's loop, whatever it was waiting for.
        drop(tx);
        read.ok()
    });
    assert!(serve_plugin_executor_commands(runtime, &rx).is_none());
    reader.join().expect("reader thread")
}

/// The producer waits on nothing, so only its deadline ends the executor's wait.
#[test]
fn a_stream_s_deadline_wakes_the_executor_to_end_it() -> Result<()> {
    use futures::StreamExt;

    let (runtime, context, produce) =
        stream_executor("return function(_writer) coroutine.yield() end")?;
    let mut body = queue_demo_stream(&runtime.vm, produce, context, Duration::from_millis(100))?;

    let error = serve_until(&runtime, async move { body.next().await })
        .context("nothing woke the executor at the stream's deadline")?
        .context("a stream past its deadline is aborted")?
        .expect_err("a stream past its deadline is aborted");
    assert!(error.to_string().contains("deadline expired"), "{error}");
    Ok(())
}

/// Every place is taken by a producer that never ends, so the stream behind them never starts,
/// and only its deadline ends the executor's wait.
#[test]
fn a_stream_waiting_for_a_place_ends_at_its_deadline() -> Result<()> {
    use futures::StreamExt;

    let (runtime, context, produce) = stream_executor(
        "started = 0; return function(_writer) started += 1; coroutine.yield() end",
    )?;
    let started =
        || runtime.eval_plugin_source_with_call_context(&b"return started"[..], context.clone());
    let mut producing = Vec::new();
    while producing.len() < 16 {
        producing.push(queue_demo_stream(
            &runtime.vm,
            produce.clone(),
            context.clone(),
            Duration::from_secs(600),
        )?);
    }
    for _ in 0..4 {
        runtime.poll_background_tasks();
    }
    assert_eq!(started()?, [harmony_luau::Value::Number(16.0)]);

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut waiting = queue_demo_stream(
        &runtime.vm,
        produce.clone(),
        context.clone(),
        Duration::from_secs(2),
    )?;
    // Its first round finds no place and leaves it waiting for one, well before its deadline.
    runtime.poll_background_tasks();
    assert!(
        Instant::now() < deadline,
        "too slow to tell a wait from an expiry"
    );
    let error = serve_until(&runtime, async move {
        let end = waiting.next().await;
        // The producing streams' clients stay until the waiting one has ended.
        drop(producing);
        end
    })
    .context("nothing woke the executor at the waiting stream's deadline")?
    .context("a stream past its deadline is aborted")?
    .expect_err("a stream past its deadline is aborted");
    assert!(error.to_string().contains("deadline expired"), "{error}");
    assert_eq!(
        started()?,
        [harmony_luau::Value::Number(16.0)],
        "the stream behind the full places still produced"
    );
    Ok(())
}

/// A task queues the stream while the scheduler polls, as a host call or a reply does, and nothing
/// else reaches the executor: the stream has to start, produce and end on its own wakes.
#[test]
fn a_stream_queued_while_the_scheduler_polls_is_produced() -> Result<()> {
    use futures::StreamExt;

    let (runtime, context, produce) =
        stream_executor("return function(writer) writer:write('chunk') end")?;
    let (queued, body) = std::sync::mpsc::channel();
    let vm = runtime.vm.clone();
    vm.data()
        .get::<harmony_core::LocalScheduler>()?
        .schedule_future(harmony_core::CallContext::default(), async move {
            let body = queue_demo_stream(&vm, produce, context, Duration::from_secs(60))?;
            let _ = queued.send(body);
            Ok(())
        });

    let chunks = serve_until(&runtime, async move {
        let mut body = tokio::task::spawn_blocking(move || body.recv())
            .await
            .expect("the task queued the stream")
            .expect("the task queued the stream");
        let mut chunks = Vec::new();
        while let Some(chunk) = body.next().await {
            chunks.push(chunk);
        }
        chunks
    })
    .context("nothing woke the executor for the queued stream")?;
    let chunks = chunks.into_iter().collect::<std::io::Result<Vec<_>>>()?;
    assert_eq!(chunks, ["chunk"]);
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
