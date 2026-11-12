use super::*;
use std::time::Instant;

use crate::services::plugin_dispatch::PluginCallbackHandle;

use harmony_core::{
    FunctionSpec,
    ModuleExport,
    luau::RequireRuntime,
    plugin::PluginManifest,
    testing::ManualClock,
};
use tokio::sync::{
    Notify,
    mpsc as async_mpsc,
    oneshot::error::TryRecvError,
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
    wake: SchedulerWake,
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

/// How long the slow handler takes, by its executor's manual clock, to go on after its wait: it
/// moves the clock as it runs, once, after the watchers of that round.
struct AfterWait {
    clock: ManualClock,
    takes: std::cell::Cell<Duration>,
}

struct DispatchHandlers {
    slow: PluginCallbackHandle,
    fast: PluginCallbackHandle,
    /// Waits on a call it makes to the slow handler.
    outer: PluginCallbackHandle,
    /// Waits at the gate, then never yields again.
    spinning: PluginCallbackHandle,
    /// Waits on a call it makes to the spinning handler.
    outer_spinning: PluginCallbackHandle,
    /// Waits at the gate, as does a coroutine it started before.
    with_coroutine: PluginCallbackHandle,
    api: PluginCallbackHandle,
}

/// Calls `provider`'s release refresh from the VM a handler runs on, as a host function reaches a
/// provider, giving it [`NESTED_TIMEOUT`].
async fn nested_refresh(
    calls: &super::super::VmCalls,
    provider: &str,
) -> Result<(), crate::services::providers::ProviderCallError> {
    use crate::services::plugin_dispatch::RuntimeCalls;
    use crate::services::providers::{
        ProviderCallStage,
        ProviderLookup,
        with_provider_call_on,
    };

    with_provider_call_on(
        calls,
        provider,
        ProviderCallStage::MetadataRefresh,
        |registry| {
            registry
                .get_refresh_callback(provider, crate::services::EntityType::Release)
                .copied()
                .map(ProviderLookup::Call)
        },
        |handler: PluginCallbackHandle, calls| async move {
            let request = MetadataRefreshRequest {
                handler,
                context: serde_json::json!({ "value": 42 }),
                timeout: NESTED_TIMEOUT,
                until_settled: Default::default(),
            };
            calls.metadata_refresh(request).await.map(|_| ())
        },
    )
    .await
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
/// With a `clock`, its scheduler runs by it.
fn dispatch_runtime(
    gate: Gate,
    clock: Option<ManualClock>,
) -> Result<(PluginExecutor, DispatchHandlers)> {
    let Gate {
        started,
        dropped,
        resumed,
        release,
        cancel_gate,
    } = gate;
    let manifests = Arc::from(vec![manifest(
        "dispatch-test",
        &["lyra.metadata", "lyra.api", "test.gate"],
    )]);
    let runtime = match clock {
        Some(clock) => {
            let runtime = PluginExecutor::with_manual_clock(manifests, clock.clone())?;
            runtime.vm.data().insert(AfterWait {
                clock,
                takes: Default::default(),
            })?;
            runtime
        }
        None => PluginExecutor::with_manifests(manifests)?,
    };
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
                .function(FunctionSpec::new("mark").call(move |frame| {
                    if let Ok(after_wait) = frame.vm.data().get::<AfterWait>() {
                        after_wait.clock.advance(after_wait.takes.take());
                    }
                    let _ = resumed.send(());
                    Ok(())
                }))
                .function(
                    FunctionSpec::new("refresh").call_async(Arc::new(|mut frame| {
                        let provider: String = frame.args.read_named("provider")?;
                        let calls = super::super::VmCalls::of(frame.vm, &frame.context)?;
                        Ok(harmony_luau::ScheduledFuture::new(async move {
                            nested_refresh(&calls, &provider)
                                .await
                                .map_err(crate::plugins::runtime_error)?;
                            Ok(true)
                        }))
                    })),
                )
                .install(|_| Ok(ModuleExport::new(()))),
        ))?;
    runtime.eval_source_while_ready(
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
        metadata.Provider.new("dispatch-outer"):refresh(metadata.EntityType.Release, function(ctx)
            gate.refresh("dispatch-slow")
            return ctx.value
        end)
        metadata.Provider.new("dispatch-spinning"):refresh(metadata.EntityType.Release, function()
            gate.wait()
            while true do end
        end)
        metadata.Provider.new("dispatch-outer-spinning"):refresh(metadata.EntityType.Release, function(ctx)
            gate.refresh("dispatch-spinning")
            return ctx.value
        end)
        metadata.Provider.new("dispatch-with-coroutine"):refresh(metadata.EntityType.Release, function(ctx)
            coroutine.resume(coroutine.create(function()
                gate.wait()
                gate.mark()
            end))
            gate.wait()
            return ctx.value
        end)
        local api = require("@lyra/api")
        api.get("/dispatch-wait", function(_ctx)
            gate.wait()
            return api.response.json(200, { value = 42 })
        end, { auth = "public" })
    "#[..],
        harmony_core::CallContext {
            origin: super::super::plugin_origin("dispatch-test", "init.luau"),
            ..Default::default()
        },
    )?;
    let registry =
        futures::executor::block_on(crate::services::providers::provider_registry().read_owned());
    let refresh = |provider: &str| {
        registry
            .get_refresh_callback(provider, crate::services::EntityType::Release)
            .copied()
            .with_context(|| format!("{provider} callback"))
    };
    let slow = refresh("dispatch-slow")?;
    let fast = refresh("dispatch-fast")?;
    let outer = refresh("dispatch-outer")?;
    let spinning = refresh("dispatch-spinning")?;
    let outer_spinning = refresh("dispatch-outer-spinning")?;
    let with_coroutine = refresh("dispatch-with-coroutine")?;
    let api = futures::executor::block_on(crate::plugins::api::tests::registered_handler(
        "GET",
        "/dispatch-wait",
    ))
    .context("API callback")?;
    Ok((
        runtime,
        DispatchHandlers {
            slow,
            fast,
            outer,
            spinning,
            outer_spinning,
            with_coroutine,
            api,
        },
    ))
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
        let setup = dispatch_runtime(gate, None).map(|(runtime, handlers)| {
            let wake = runtime.wake_handle();
            let (tx, rx) = plugin_executor_channel(4, wake.clone());
            let entrypoints = runtime.entrypoint_cancellation();
            (runtime, rx, (tx, entrypoints, wake, handlers))
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
    let (
        tx,
        entrypoints,
        wake,
        DispatchHandlers {
            slow, fast, api, ..
        },
    ) = ready_rx.recv()??;
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
        wake,
    })
}

/// The test plugin on the calling thread and a manual clock, where a test decides when each round
/// runs and when time passes, with what its gate reports.
struct StagedExecutor {
    runtime: PluginExecutor,
    clock: ManualClock,
    slow: PluginCallbackHandle,
    fast: PluginCallbackHandle,
    outer: PluginCallbackHandle,
    spinning: PluginCallbackHandle,
    outer_spinning: PluginCallbackHandle,
    with_coroutine: PluginCallbackHandle,
    started: async_mpsc::UnboundedReceiver<()>,
    dropped: async_mpsc::UnboundedReceiver<()>,
    resumed: async_mpsc::UnboundedReceiver<()>,
    release: Arc<Notify>,
}

type RefreshAnswer = tokio::sync::oneshot::Receiver<Result<MetadataRefreshResult>>;

/// How long the staged handlers have. Nothing waits it out.
const STAGED_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a call the test plugin makes to a provider has: longer than the handler making it, so
/// that only what ends that handler ends the call.
const NESTED_TIMEOUT: Duration = Duration::from_secs(120);

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
        let clock = ManualClock::new();
        let (runtime, handlers) = dispatch_runtime(
            Gate {
                started: started_tx,
                dropped: dropped_tx,
                resumed: resumed_tx,
                release: release.clone(),
                cancel_gate: Arc::default(),
            },
            Some(clock.clone()),
        )?;
        Ok(Self {
            runtime,
            clock,
            slow: handlers.slow,
            fast: handlers.fast,
            outer: handlers.outer,
            spinning: handlers.spinning,
            outer_spinning: handlers.outer_spinning,
            with_coroutine: handlers.with_coroutine,
            started,
            dropped,
            resumed,
            release,
        })
    }

    /// Starts `handler` as its command does, with [`STAGED_TIMEOUT`] by the clock, running no
    /// round.
    fn start(&self, handler: PluginCallbackHandle) -> Result<RefreshAnswer> {
        self.start_for(handler, STAGED_TIMEOUT)
    }

    /// [`Self::start`] with `timeout` by the clock.
    fn start_for(&self, handler: PluginCallbackHandle, timeout: Duration) -> Result<RefreshAnswer> {
        self.start_with(handler, timeout, Default::default())
    }

    /// [`Self::start`], with what the caller wants kept until the handler has settled.
    fn start_keeping(
        &self,
        handler: PluginCallbackHandle,
        until_settled: crate::services::plugin_dispatch::UntilSettled,
    ) -> Result<RefreshAnswer> {
        self.start_with(handler, STAGED_TIMEOUT, until_settled)
    }

    fn start_with(
        &self,
        handler: PluginCallbackHandle,
        timeout: Duration,
        until_settled: crate::services::plugin_dispatch::UntilSettled,
    ) -> Result<RefreshAnswer> {
        let (reply, answer) = tokio::sync::oneshot::channel();
        let permit = Arc::new(tokio::sync::Semaphore::new(1)).try_acquire_owned()?;
        let request = MetadataRefreshRequest {
            handler,
            context: serde_json::json!({ "value": 42 }),
            timeout,
            until_settled,
        };
        self.runtime.start_metadata_refresh(request, reply, permit);
        Ok(answer)
    }

    /// Starts the slow handler and runs it until it waits at the gate.
    fn start_slow_waiting(&mut self) -> Result<RefreshAnswer> {
        let answer = self.start(self.slow)?;
        self.runtime.run_until_idle()?;
        self.started.try_recv().context("the handler waits")?;
        Ok(answer)
    }

    /// Starts the outer handler and runs it until the slow handler it calls waits at the gate.
    fn start_outer_waiting(&mut self) -> Result<RefreshAnswer> {
        let answer = self.start(self.outer)?;
        self.runtime.run_until_idle()?;
        self.started
            .try_recv()
            .context("the nested handler waits")?;
        Ok(answer)
    }

    /// Has the slow handler take `duration`, by the clock, to go on after its wait.
    fn slow_handler_takes(&self, duration: Duration) -> Result<()> {
        self.runtime
            .vm
            .data()
            .get::<AfterWait>()?
            .takes
            .set(duration);
        Ok(())
    }

    fn scheduler(&self) -> Result<std::rc::Rc<harmony_core::Scheduler>> {
        Ok(self.runtime.vm.data().get()?)
    }

    /// How many tasks the scheduler holds that haven't ended.
    fn pending_tasks(&self) -> Result<usize> {
        Ok(self
            .scheduler()?
            .snapshots()
            .iter()
            .filter(|task| task.state == harmony_core::TaskState::Pending)
            .count())
    }
}

fn unanswered(answer: &mut RefreshAnswer) -> bool {
    matches!(answer.try_recv(), Err(TryRecvError::Empty))
}

fn answered(mut answer: RefreshAnswer) -> Result<MetadataRefreshResult> {
    answer.try_recv().context("the handler is answered for")?
}

fn deadline_expired(answer: RefreshAnswer) -> Result<()> {
    let error = answered(answer).expect_err("a handler past its deadline fails");
    assert!(error.to_string().contains("deadline expired"), "{error:#}");
    Ok(())
}

#[tokio::test]
async fn a_handler_whose_caller_left_before_its_first_round_never_starts() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = StagedExecutor::new()?;

    drop(executor.start(executor.slow)?);
    assert_eq!(
        executor.pending_tasks()?,
        2,
        "the handler and its watcher are scheduled"
    );
    executor.runtime.run_until_idle()?;

    assert!(executor.started.try_recv().is_err(), "the handler ran");
    assert_eq!(executor.pending_tasks()?, 0);
    Ok(())
}

#[tokio::test]
async fn a_handler_whose_deadline_passed_before_its_first_round_never_starts() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = StagedExecutor::new()?;

    let mut answer = executor.start(executor.slow)?;
    assert!(
        unanswered(&mut answer),
        "the handler was refused, not scheduled"
    );
    assert_eq!(
        executor.pending_tasks()?,
        2,
        "the handler and its watcher are scheduled"
    );
    // To the deadline exactly, which is the first instant that is too late.
    executor.clock.advance(STAGED_TIMEOUT);
    executor.runtime.run_until_idle()?;

    assert!(executor.started.try_recv().is_err(), "the handler ran");
    assert_eq!(executor.pending_tasks()?, 0);
    deadline_expired(answer)
}

/// The handler's call is ready, and its wake is queued ahead of the one its caller's leaving
/// sends its watcher.
#[tokio::test]
async fn a_handler_ready_as_its_caller_leaves_is_not_resumed() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = StagedExecutor::new()?;
    let answer = executor.start_slow_waiting()?;

    executor.release.notify_one();
    drop(answer);
    executor.runtime.run_until_idle()?;

    assert!(executor.resumed.try_recv().is_err(), "the handler went on");
    assert!(executor.dropped.try_recv().is_ok(), "its call is dropped");
    Ok(())
}

/// A caller that leaves stops waiting at once, while its handler goes on until a round of the
/// executor finds it gone.
#[tokio::test]
async fn what_a_caller_keeps_until_settled_outlasts_its_leaving() -> Result<()> {
    use crate::services::plugin_dispatch::UntilSettled;

    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = StagedExecutor::new()?;
    let kept = Arc::new(());
    let start = |executor: &mut StagedExecutor| -> Result<RefreshAnswer> {
        let answer =
            executor.start_keeping(executor.slow, UntilSettled::keeping(Arc::clone(&kept)))?;
        executor.runtime.run_until_idle()?;
        executor.started.try_recv().context("the handler waits")?;
        Ok(answer)
    };

    let answer = start(&mut executor)?;
    drop(answer);
    assert_eq!(
        Arc::strong_count(&kept),
        2,
        "the caller left before its handler settled"
    );
    executor.runtime.run_until_idle()?;
    assert!(executor.dropped.try_recv().is_ok(), "its call is dropped");
    assert_eq!(Arc::strong_count(&kept), 1);

    let answer = start(&mut executor)?;
    executor.clock.advance(STAGED_TIMEOUT);
    assert_eq!(Arc::strong_count(&kept), 2);
    executor.runtime.run_until_idle()?;
    assert!(executor.dropped.try_recv().is_ok(), "its call is dropped");
    assert_eq!(Arc::strong_count(&kept), 1);
    deadline_expired(answer)?;

    let answer = start(&mut executor)?;
    executor.release.notify_one();
    executor.runtime.run_until_idle()?;
    assert!(executor.resumed.try_recv().is_ok(), "the handler ended");
    assert_eq!(Arc::strong_count(&kept), 1);
    answered(answer)?;

    // A handler that never starts keeps nothing.
    drop(executor.start_keeping(executor.slow, UntilSettled::keeping(Arc::clone(&kept)))?);
    executor.runtime.run_until_idle()?;
    assert_eq!(Arc::strong_count(&kept), 1);
    Ok(())
}

#[tokio::test]
async fn a_handler_ready_after_its_deadline_is_not_resumed() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = StagedExecutor::new()?;
    let mut answer = executor.start_slow_waiting()?;

    executor
        .clock
        .advance(STAGED_TIMEOUT - Duration::from_nanos(1));
    executor.runtime.run_until_idle()?;
    assert!(unanswered(&mut answer), "expired before its deadline");
    assert!(executor.dropped.try_recv().is_err());

    executor.clock.advance(Duration::from_nanos(1));
    executor.release.notify_one();
    executor.runtime.run_until_idle()?;

    assert!(executor.resumed.try_recv().is_err(), "the handler went on");
    assert!(executor.dropped.try_recv().is_ok(), "its call is dropped");
    deadline_expired(answer)
}

/// The outer handler waits on its call to the slow one, which is ready to go on in the round that
/// finds the outer's caller gone.
#[tokio::test]
async fn a_nested_handler_ready_as_its_caller_s_caller_leaves_is_not_resumed() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = StagedExecutor::new()?;
    let answer = executor.start_outer_waiting()?;

    executor.release.notify_one();
    drop(answer);
    executor.runtime.poll_background_tasks();

    assert!(
        executor.resumed.try_recv().is_err(),
        "the nested handler went on"
    );
    assert!(executor.dropped.try_recv().is_ok(), "its call is dropped");
    executor.runtime.run_until_idle()?;
    assert!(executor.resumed.try_recv().is_err());
    assert_eq!(executor.pending_tasks()?, 0);
    Ok(())
}

/// As above, in the round that finds the outer handler's deadline passed, well before the nested
/// call's own.
#[tokio::test]
async fn a_nested_handler_ready_after_its_caller_s_deadline_is_not_resumed() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = StagedExecutor::new()?;
    let mut answer = executor.start_outer_waiting()?;

    executor
        .clock
        .advance(STAGED_TIMEOUT - Duration::from_nanos(1));
    executor.runtime.run_until_idle()?;
    assert!(unanswered(&mut answer), "expired before its deadline");
    assert!(executor.dropped.try_recv().is_err());

    executor.clock.advance(Duration::from_nanos(1));
    executor.release.notify_one();
    executor.runtime.poll_background_tasks();

    assert!(
        executor.resumed.try_recv().is_err(),
        "the nested handler went on"
    );
    assert!(executor.dropped.try_recv().is_ok(), "its call is dropped");
    executor.runtime.run_until_idle()?;
    assert!(executor.resumed.try_recv().is_err());
    assert_eq!(executor.pending_tasks()?, 0);
    deadline_expired(answer)
}

/// A request carries how long its handler has, not until when: the time counts from when the
/// executor takes the request up, by its clock, however long ago the request was made.
#[tokio::test]
async fn a_handler_s_timeout_counts_from_when_the_executor_takes_its_request() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = StagedExecutor::new()?;
    let (reply, mut answer) = tokio::sync::oneshot::channel();
    let permit = Arc::new(tokio::sync::Semaphore::new(1)).try_acquire_owned()?;
    let request = request(executor.slow, STAGED_TIMEOUT);

    executor.clock.advance(STAGED_TIMEOUT * 2);
    executor
        .runtime
        .start_metadata_refresh(request, reply, permit);
    executor.runtime.run_until_idle()?;
    executor.started.try_recv().context("the handler waits")?;

    executor
        .clock
        .advance(STAGED_TIMEOUT - Duration::from_nanos(1));
    executor.runtime.run_until_idle()?;
    assert!(unanswered(&mut answer), "expired before its deadline");

    executor.clock.advance(Duration::from_nanos(1));
    executor.runtime.run_until_idle()?;
    deadline_expired(answer)
}

/// How long the spinning handler has left, by the clock, when it goes on after its wait.
const LEFT_TO_SPIN: Duration = Duration::from_millis(50);

/// Lets the spinning handler go on with [`LEFT_TO_SPIN`] of `timeout` left and returns how the
/// call that began with `timeout` was answered. The clock stands still while it spins, so only
/// the limit on its resume ends it: what was left when the resume began, counted in real time.
fn answer_for_a_handler_spinning_late(
    executor: &mut StagedExecutor,
    mut answer: RefreshAnswer,
    timeout: Duration,
) -> Result<anyhow::Error> {
    executor.started.try_recv().context("the handler waits")?;
    executor.clock.advance(timeout - LEFT_TO_SPIN);
    executor.release.notify_one();
    let resumed = Instant::now();
    executor.runtime.run_until_idle()?;
    let spun = resumed.elapsed();

    assert!(
        spun < timeout / 2,
        "it spun for {spun:?}, as if each resume had the whole timeout"
    );
    let error = answer
        .try_recv()
        .context("the handler is answered for")?
        .expect_err("an interrupted handler fails");
    assert!(
        error.to_string().contains(harmony_luau::INTERRUPTED_ERROR),
        "{error:#}"
    );
    assert_eq!(executor.pending_tasks()?, 0);
    Ok(error)
}

/// The watcher ends a handler only between two resumes, so one that waits until late and then
/// never yields again is ended by its resume's own limit, which is its deadline.
#[tokio::test]
async fn a_handler_that_spins_after_a_late_wait_is_interrupted_at_its_deadline() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = StagedExecutor::new()?;
    let answer = executor.start(executor.spinning)?;
    executor.runtime.run_until_idle()?;

    answer_for_a_handler_spinning_late(&mut executor, answer, STAGED_TIMEOUT)?;
    Ok(())
}

/// As above for a handler run on the calling VM, by the nested call's own deadline: its caller
/// has longer.
#[tokio::test]
async fn a_nested_handler_that_spins_after_a_late_wait_is_interrupted_at_its_deadline() -> Result<()>
{
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = StagedExecutor::new()?;
    let answer = executor.start_for(executor.outer_spinning, NESTED_TIMEOUT * 2)?;
    executor.runtime.run_until_idle()?;

    answer_for_a_handler_spinning_late(&mut executor, answer, NESTED_TIMEOUT)?;
    Ok(())
}

/// A nested call has its own, longer time, but answers to the handler that made it: its handler's
/// resume ends at that handler's deadline.
#[tokio::test]
async fn a_nested_handler_that_spins_late_is_interrupted_at_its_caller_s_deadline() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = StagedExecutor::new()?;
    assert!(STAGED_TIMEOUT < NESTED_TIMEOUT);
    let answer = executor.start(executor.outer_spinning)?;
    executor.runtime.run_until_idle()?;

    answer_for_a_handler_spinning_late(&mut executor, answer, STAGED_TIMEOUT)?;
    Ok(())
}

/// A handler runs in its plugin's run, with no group of its own, so cancelling it reaches its own
/// thread only: a coroutine it started that waits on a call of its own is a task of the run, and
/// goes on until the run ends.
#[tokio::test]
async fn a_coroutine_a_handler_started_goes_on_after_the_handler_s_deadline() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = StagedExecutor::new()?;
    let answer = executor.start(executor.with_coroutine)?;
    executor.runtime.run_until_idle()?;
    assert!(executor.started.try_recv().is_ok(), "the coroutine waits");
    assert!(executor.started.try_recv().is_ok(), "the handler waits");

    executor.clock.advance(STAGED_TIMEOUT);
    executor.runtime.run_until_idle()?;
    deadline_expired(answer)?;
    assert!(executor.dropped.try_recv().is_ok(), "the handler's call");
    assert!(executor.dropped.try_recv().is_err(), "the coroutine's call");

    executor.release.notify_one();
    executor.runtime.run_until_idle()?;
    assert!(executor.resumed.try_recv().is_ok(), "the coroutine went on");
    Ok(())
}

/// The handler ends in a round, and the round in which its watcher reads that comes only after
/// its deadline, as when a command holds the thread in between.
#[tokio::test]
async fn a_finished_handler_is_answered_before_a_command_that_outlasts_its_deadline() -> Result<()>
{
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let executor = StagedExecutor::new()?;
    let mut answer = executor.start(executor.fast)?;

    executor.runtime.poll_background_tasks();
    assert!(unanswered(&mut answer), "its watcher has yet to read it");
    executor.clock.advance(STAGED_TIMEOUT * 2);
    executor.runtime.run_until_idle()?;

    assert_eq!(answered(answer)?.values, vec![serde_json::json!(42)]);
    Ok(())
}

/// Starts the slow handler and lets it go on and end `after` into its time, in a round whose
/// watchers ran before then, so that its own saw neither an end nor a deadline. Returns how it
/// was answered.
fn answer_for_a_handler_ending(
    executor: &mut StagedExecutor,
    after: Duration,
) -> Result<Result<MetadataRefreshResult>> {
    let mut answer = executor.start_slow_waiting()?;

    executor.slow_handler_takes(after)?;
    executor.release.notify_one();
    executor.runtime.poll_background_tasks();
    assert!(executor.resumed.try_recv().is_ok(), "the handler went on");
    assert!(unanswered(&mut answer), "its watcher has yet to read it");
    executor.runtime.run_until_idle()?;
    answer.try_recv().context("the handler is answered for")
}

/// A handler has to end before its deadline: one that ends at it is as late as the timer that
/// fires at it says, though its watcher never cancelled it.
#[tokio::test]
async fn a_handler_is_answered_by_when_it_ended_against_its_deadline() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = StagedExecutor::new()?;

    let in_time =
        answer_for_a_handler_ending(&mut executor, STAGED_TIMEOUT - Duration::from_nanos(1))??;
    assert_eq!(in_time.values, vec![serde_json::json!(42)]);

    for late in [STAGED_TIMEOUT, STAGED_TIMEOUT * 2] {
        let error = answer_for_a_handler_ending(&mut executor, late)?
            .expect_err("a handler that ended at or past its deadline fails");
        assert!(error.to_string().contains("deadline expired"), "{error:#}");
    }
    Ok(())
}

/// Calls the slow handler as a host function does, with [`NESTED_TIMEOUT`], and lets the handler
/// go on and end `after` into that. Returns how the call was answered.
fn answer_for_a_nested_call_ending(
    executor: &mut StagedExecutor,
    after: Duration,
) -> Result<Result<(), crate::services::providers::ProviderCallError>> {
    use futures::FutureExt;

    let calls = super::super::VmCalls::of(&executor.runtime.vm, &Default::default())?;
    let mut call = std::pin::pin!(nested_refresh(&calls, "dispatch-slow"));
    assert!(
        call.as_mut().now_or_never().is_none(),
        "the call ended before its handler ran"
    );
    executor.runtime.run_until_idle()?;
    executor.started.try_recv().context("the handler waits")?;

    executor.slow_handler_takes(after)?;
    executor.release.notify_one();
    executor.runtime.run_until_idle()?;
    assert!(executor.resumed.try_recv().is_ok(), "the handler went on");
    call.as_mut()
        .now_or_never()
        .context("the call still waits after its handler ended")
}

/// A nested call's handler has to end before the call's deadline, as one the executor starts has
/// to end before its own, though nothing cancelled it.
#[tokio::test]
async fn a_nested_call_is_answered_by_when_its_handler_ended_against_its_deadline() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = StagedExecutor::new()?;

    answer_for_a_nested_call_ending(&mut executor, NESTED_TIMEOUT - Duration::from_nanos(1))??;

    for late in [NESTED_TIMEOUT, NESTED_TIMEOUT * 2] {
        let error = answer_for_a_nested_call_ending(&mut executor, late)?
            .expect_err("a handler that ended at or past the call's deadline fails it");
        assert!(
            error
                .to_string()
                .contains("did not complete within its timeout"),
            "{error:#}"
        );
    }
    Ok(())
}

fn request(handler: PluginCallbackHandle, timeout: Duration) -> MetadataRefreshRequest {
    MetadataRefreshRequest {
        handler,
        context: serde_json::json!({ "value": 42 }),
        timeout,
        until_settled: Default::default(),
    }
}

/// How long a test waits for what should come at once before it takes it as never coming.
const WATCHDOG: Duration = Duration::from_secs(60);
/// A deadline no test here reaches.
const AMPLE: Duration = Duration::from_secs(30);

async fn signal(receiver: &mut async_mpsc::UnboundedReceiver<()>) -> Result<()> {
    tokio::time::timeout(WATCHDOG, receiver.recv())
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
        let request = request(executor.slow, AMPLE);
        async move { handle.dispatch_metadata_refresh(request).await }
    });
    signal(&mut executor.started).await?;
    let fast = tokio::time::timeout(
        WATCHDOG,
        executor
            .handle
            .dispatch_metadata_refresh(request(executor.fast, AMPLE)),
    )
    .await??;
    assert_eq!(fast.values, vec![serde_json::json!(42)]);
    assert!(!slow.is_finished());
    slow.abort();
    assert!(slow.await.unwrap_err().is_cancelled());
    signal(&mut executor.dropped).await?;
    let fast = executor
        .handle
        .dispatch_metadata_refresh(request(executor.fast, AMPLE))
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
        WATCHDOG,
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
    let expired = request(executor.slow, Duration::ZERO);
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
        let request = request(executor.slow, AMPLE);
        async move { handle.dispatch_metadata_refresh(request).await }
    });
    signal(&mut executor.started).await?;
    executor.release.notify_one();
    let result = tokio::time::timeout(WATCHDOG, slow).await???;
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
        let request = request(executor.slow, AMPLE);
        calls.push(tokio::spawn(async move {
            handle.dispatch_metadata_refresh(request).await
        }));
        signal(&mut executor.started).await?;
    }
    let handle = executor.handle.clone();
    let request = request(executor.slow, AMPLE);
    let waiting = tokio::spawn(async move { handle.dispatch_metadata_refresh(request).await });
    // Runs the call as far as it gets: to its wait for a slot, as every slot is still held.
    tokio::task::yield_now().await;
    assert_eq!(executor.handle.slots_in_use(), 4);
    assert!(!waiting.is_finished());
    assert!(executor.started.try_recv().is_err());
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
        WATCHDOG,
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
    let response = tokio::time::timeout(WATCHDOG, api).await???;
    assert_eq!(response.status, 200);
    signal(&mut executor.dropped).await?;
    Ok(())
}

/// Returns once the executor thread is asleep in its wait, where only a wake or a deadline brings
/// on its next round.
async fn asleep(executor: &TestExecutor) -> Result<()> {
    tokio::time::timeout(WATCHDOG, async {
        while !executor.wake.owner_is_waiting() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("the executor kept polling with nothing to do")
}

/// A handler parked on a host call runs on because the call's readiness wakes the executor, which
/// is asleep while the call is pending. On the real clock and the real wait.
#[tokio::test]
async fn a_parked_handler_resumes_on_the_wake_of_its_host_call() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut executor = test_executor()?;
    let running = hold_slot(&mut executor).await?;
    asleep(&executor).await?;

    executor.release.notify_one();
    let result = tokio::time::timeout(WATCHDOG, running).await???;
    assert_eq!(result.values, vec![serde_json::json!(42)]);
    Ok(())
}

/// An executor running one plugin, `demo`, on the calling thread, and a producer `source` returns.
/// With a `clock`, its scheduler runs by it.
fn stream_executor(
    source: &'static str,
    clock: Option<ManualClock>,
) -> Result<(
    PluginExecutor,
    harmony_core::CallContext,
    harmony_luau::Function,
)> {
    let manifests = Arc::from(vec![manifest("demo", &[])]);
    let runtime = match clock {
        Some(clock) => PluginExecutor::with_manual_clock(manifests, clock)?,
        None => PluginExecutor::with_manifests(manifests)?,
    };
    let context = harmony_core::CallContext {
        origin: super::super::plugin_origin("demo", "init.luau"),
        ..Default::default()
    };
    let values = runtime.eval_source_while_ready(source.as_bytes(), context.clone())?;
    let Some(harmony_luau::Value::Function(produce)) = values.into_iter().next() else {
        anyhow::bail!("the chunk returns the producer");
    };
    Ok((runtime, context, produce))
}

/// Queues a stream that has `timeout` by the executor's clock.
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
        super::super::now(vm)? + timeout,
        super::super::StreamOrigin {
            plugin_id: "demo".to_string(),
            method: "GET".to_string(),
            path: "/stream".to_string(),
        },
    )?)
}

/// As many streams as there are places for, each produced by `produce`, which must not end, so
/// that the next stream queued waits for a place.
fn fill_the_places(
    runtime: &PluginExecutor,
    produce: &harmony_luau::Function,
    context: &harmony_core::CallContext,
) -> Result<Vec<super::super::ApiResponseStream>> {
    let producing = (0..16)
        .map(|_| {
            queue_demo_stream(
                &runtime.vm,
                produce.clone(),
                context.clone(),
                Duration::from_secs(600),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    runtime.run_until_idle()?;
    Ok(producing)
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
            .block_on(async { tokio::time::timeout(WATCHDOG, read).await });
        // Ends the executor's loop, whatever it was waiting for.
        drop(tx);
        read.ok()
    });
    assert!(serve_plugin_executor_commands(runtime, &rx).is_none());
    reader.join().expect("reader thread")
}

/// The producer waits on nothing, so only its deadline ends the executor's wait. On the real
/// clock and the real wait.
#[test]
fn a_stream_s_deadline_wakes_the_executor_to_end_it() -> Result<()> {
    use futures::StreamExt;

    let (runtime, context, produce) =
        stream_executor("return function(_writer) coroutine.yield() end", None)?;
    let mut body = queue_demo_stream(&runtime.vm, produce, context, Duration::from_millis(100))?;

    let error = serve_until(&runtime, async move { body.next().await })
        .context("nothing woke the executor at the stream's deadline")?
        .context("a stream past its deadline is aborted")?
        .expect_err("a stream past its deadline is aborted");
    assert!(error.to_string().contains("deadline expired"), "{error}");
    Ok(())
}

const COUNTING_PRODUCER: &str =
    "started = 0; return function(_writer) started += 1; coroutine.yield() end";

/// Every place is taken by a producer that never ends, so the stream behind them never starts,
/// and ends at its deadline.
#[test]
fn a_stream_waiting_for_a_place_ends_at_its_deadline() -> Result<()> {
    use futures::{
        FutureExt,
        StreamExt,
    };

    let clock = ManualClock::new();
    let (runtime, context, produce) = stream_executor(COUNTING_PRODUCER, Some(clock.clone()))?;
    let scheduler = runtime.vm.data().get::<harmony_core::Scheduler>()?;
    let started = || runtime.eval_source_while_ready(&b"return started"[..], context.clone());
    let _producing = fill_the_places(&runtime, &produce, &context)?;
    assert_eq!(started()?, [harmony_luau::Value::Number(16.0)]);

    let timeout = Duration::from_secs(2);
    let mut waiting = queue_demo_stream(&runtime.vm, produce.clone(), context.clone(), timeout)?;
    runtime.run_until_idle()?;
    assert!(waiting.next().now_or_never().is_none(), "the stream ended");
    assert_eq!(started()?, [harmony_luau::Value::Number(16.0)]);
    assert_eq!(
        scheduler.next_wake_delay(),
        Some(timeout),
        "nothing ends the executor's wait at the stream's deadline"
    );

    clock.advance(timeout - Duration::from_nanos(1));
    runtime.run_until_idle()?;
    assert!(
        waiting.next().now_or_never().is_none(),
        "ended before its deadline"
    );

    clock.advance(Duration::from_nanos(1));
    runtime.run_until_idle()?;
    let error = waiting
        .next()
        .now_or_never()
        .context("the stream is still waiting at its deadline")?
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

/// The stream waits for a place, on nothing that would wake the executor, so only the deadline
/// it registered while waiting ends the executor's wait. On the real clock and the real wait.
#[test]
fn the_deadline_of_a_stream_waiting_for_a_place_wakes_the_executor() -> Result<()> {
    use futures::StreamExt;

    let (runtime, context, produce) = stream_executor(COUNTING_PRODUCER, None)?;
    let producing = fill_the_places(&runtime, &produce, &context)?;
    let mut waiting = queue_demo_stream(
        &runtime.vm,
        produce.clone(),
        context.clone(),
        Duration::from_millis(200),
    )?;

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
    Ok(())
}

/// A producer has to end before its stream's deadline, like a handler before its own.
#[test]
fn a_stream_ends_by_when_its_producer_ended_against_its_deadline() -> Result<()> {
    use futures::{
        FutureExt,
        StreamExt,
    };

    let timeout = Duration::from_secs(30);
    // The clock moves in the stream's first round, after the stream took its place and before
    // the producer it started then runs.
    let end_of_a_producer_ending = |after: Duration| -> Result<Option<std::io::Result<_>>> {
        let clock = ManualClock::new();
        let (runtime, context, produce) =
            stream_executor("return function(_writer) end", Some(clock.clone()))?;
        let mut body = queue_demo_stream(&runtime.vm, produce, context, timeout)?;
        runtime
            .vm
            .data()
            .get::<harmony_core::Scheduler>()?
            .spawn_future(harmony_core::CallContext::default(), {
                let clock = clock.clone();
                async move {
                    clock.advance(after);
                    Ok(())
                }
            });
        runtime.run_until_idle()?;
        body.next()
            .now_or_never()
            .context("the stream is still open after its producer ended")
    };

    // Short of the deadline by what the producer's resume, limited to what is left until it in
    // real time, needs to run.
    assert!(
        end_of_a_producer_ending(timeout - Duration::from_secs(10))?.is_none(),
        "a producer that ended before its deadline ends the body whole"
    );
    for late in [timeout, timeout * 2] {
        let error = end_of_a_producer_ending(late)?
            .context("a producer that ended at or past its deadline aborts the body")?
            .expect_err("a producer that ended at or past its deadline aborts the body");
        assert!(error.to_string().contains("deadline expired"), "{error}");
    }
    Ok(())
}

/// A task queues the stream while the scheduler polls, as a host call or a reply does, and nothing
/// else reaches the executor: the stream has to start, produce and end on its own wakes.
#[test]
fn a_stream_queued_while_the_scheduler_polls_is_produced() -> Result<()> {
    use futures::StreamExt;

    let (runtime, context, produce) =
        stream_executor("return function(writer) writer:write('chunk') end", None)?;
    let (queued, body) = std::sync::mpsc::channel();
    let vm = runtime.vm.clone();
    vm.data().get::<harmony_core::Scheduler>()?.spawn_future(
        harmony_core::CallContext::default(),
        async move {
            let body = queue_demo_stream(&vm, produce, context, Duration::from_secs(60))?;
            let _ = queued.send(body);
            Ok(())
        },
    );

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
        let request = request(executor.slow, AMPLE);
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
    tokio::time::timeout(WATCHDOG, async {
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
        let request = request(executor.fast, AMPLE);
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
        let request = request(executor.fast, AMPLE);
        async move { handle.dispatch_metadata_refresh(request).await }
    });
    // Runs each call as far as it gets: to its wait behind the drain.
    tokio::task::yield_now().await;
    assert!(!provider_call.is_finished());
    assert!(!direct_call.is_finished());
    assert!(!drain.is_finished());

    executor.release.notify_one();
    let result = tokio::time::timeout(WATCHDOG, running).await???;
    assert_eq!(result.values, vec![serde_json::json!(42)]);
    assert_eq!(
        tokio::time::timeout(WATCHDOG, drain).await??,
        DrainOutcome::Drained
    );

    assert!(matches!(
        tokio::time::timeout(WATCHDOG, provider_call).await??,
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

    // The handler's cancellation has begun and can't finish before the gate opens, so its slot
    // is still held.
    tokio::task::yield_now().await;
    assert!(
        !drain.is_finished(),
        "a drain must wait for the cancelled handler"
    );

    executor.cancel_gate.open();
    assert_eq!(
        tokio::time::timeout(WATCHDOG, drain).await??,
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
        WATCHDOG,
        executor
            .handle
            .drain(Duration::from_millis(50), &CancellationToken::new()),
    )
    .await?;
    assert_eq!(outcome, DrainOutcome::TimedOut);

    let error = executor
        .handle
        .dispatch_metadata_refresh(request(executor.fast, AMPLE))
        .await
        .expect_err("a drained runtime admits nothing");
    assert!(DispatchNotStarted::caused(&error), "{error:#}");

    tokio::time::timeout(WATCHDOG, executor.handle.shutdown()).await??;
    let error = tokio::time::timeout(WATCHDOG, running)
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
        let request = request(executor.fast, AMPLE);
        let release = executor.release.clone();
        async move {
            let result = handle.dispatch_metadata_refresh(request).await;
            release.notify_one();
            result
        }
    });

    assert_eq!(
        tokio::time::timeout(WATCHDOG, drain).await??,
        DrainOutcome::TimedOut
    );
    let error = tokio::time::timeout(WATCHDOG, nested)
        .await??
        .expect_err("the nested call never started");
    assert!(DispatchNotStarted::caused(&error), "{error:#}");
    tokio::time::timeout(WATCHDOG, running).await???;
    Ok(())
}

#[tokio::test]
async fn a_crash_ends_a_drain_at_once() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;

    let crashed = empty_executor()?;
    crashed.inject_panic().await.expect_err("it crashed");
    let outcome = tokio::time::timeout(
        WATCHDOG,
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
    let outcome = tokio::time::timeout(WATCHDOG, drain).await??;
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

    let outcome =
        tokio::time::timeout(WATCHDOG, executor.handle.drain(LONG_LIMIT, &cancelled)).await?;
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
    tokio::time::timeout(WATCHDOG, running).await???;
    drain.await?;
    let error = executor
        .handle
        .dispatch_listen_update(update)
        .expect_err("a closed runtime admits nothing");
    assert!(DispatchNotStarted::caused(&error), "{error:#}");
    Ok(())
}
