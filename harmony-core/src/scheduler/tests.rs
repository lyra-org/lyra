use std::{
    future::{
        pending,
        poll_fn,
    },
    panic::{
        AssertUnwindSafe,
        catch_unwind,
    },
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            AtomicUsize,
            Ordering,
        },
    },
    task::Poll,
};

use super::local::Polling;
use super::*;
use crate::testing::ManualClock;
use harmony_luau::{
    CapabilityId,
    ModuleId,
};

#[test]
fn ready_future_completes_with_origin_snapshot() {
    let mut scheduler = Scheduler::new();
    let group = scheduler.create_group();
    let context = CallContext {
        origin: ChunkOrigin {
            module: Some(ModuleId(Arc::from("harmony/task"))),
            plugin: Some(Arc::from("demo")),
            path: Some(Arc::from("plugins/demo/init.luau")),
        },
        capability: Some(CapabilityId(Arc::from("harmony.task"))),
        task_group: group,
        ..CallContext::default()
    };

    let handle = scheduler.spawn(context, async { Ok(()) });

    assert_eq!(scheduler.poll_ready(), 1);
    let snapshot = scheduler.snapshot(handle.id()).expect("task snapshot");
    assert_eq!(snapshot.state, TaskState::Completed);
    assert_eq!(snapshot.group, group);
    assert_eq!(snapshot.origin.plugin.as_deref(), Some("demo"));
}

#[test]
fn panicking_future_fails_only_its_own_task() {
    let mut scheduler = Scheduler::new();
    let panicking = scheduler.spawn(CallContext::default(), async {
        panic!("upstream sent nonsense");
    });
    let healthy = scheduler.spawn(CallContext::default(), async { Ok(()) });

    assert_eq!(scheduler.poll_ready(), 2);
    let failed = scheduler.snapshot(panicking.id()).expect("panicking task");
    assert_eq!(failed.state, TaskState::Failed);
    assert!(
        failed
            .error
            .as_deref()
            .is_some_and(|error| error.contains("upstream sent nonsense")),
        "{:?}",
        failed.error
    );
    assert_eq!(
        scheduler
            .snapshot(healthy.id())
            .expect("healthy task")
            .state,
        TaskState::Completed
    );
}

#[test]
fn future_panicking_again_when_dropped_fails_only_its_own_task() {
    struct PanicsTwice;
    impl Future for PanicsTwice {
        type Output = Result<()>;
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
            panic!("poll panicked");
        }
    }
    impl Drop for PanicsTwice {
        fn drop(&mut self) {
            if !std::thread::panicking() {
                panic!("drop panicked");
            }
        }
    }

    let mut scheduler = Scheduler::new();
    let panicking = scheduler.spawn(CallContext::default(), PanicsTwice);
    let healthy = scheduler.spawn(CallContext::default(), async { Ok(()) });

    assert_eq!(scheduler.poll_ready(), 2);
    assert_eq!(
        scheduler
            .snapshot(panicking.id())
            .expect("panicking task")
            .state,
        TaskState::Failed
    );
    assert_eq!(
        scheduler
            .snapshot(healthy.id())
            .expect("healthy task")
            .state,
        TaskState::Completed
    );
}

#[test]
fn removing_one_finished_task_preserves_other_finished_tasks() {
    let mut scheduler = Scheduler::new();
    let first = scheduler.spawn(CallContext::default(), async { Ok(()) });
    let second = scheduler.spawn(CallContext::default(), async { Ok(()) });

    assert_eq!(scheduler.poll_ready(), 2);
    assert!(scheduler.remove(first.id()));
    assert!(scheduler.snapshot(first.id()).is_none());
    assert_eq!(
        scheduler.snapshot(second.id()).expect("second task").state,
        TaskState::Completed
    );
}

#[test]
fn cancelling_group_cancels_pending_tasks() {
    let mut scheduler = Scheduler::new();
    let group = scheduler.create_group();
    let context = CallContext {
        task_group: group,
        ..CallContext::default()
    };

    let first = scheduler.spawn(context, pending());
    let second = scheduler.spawn(
        CallContext {
            task_group: group,
            ..CallContext::default()
        },
        pending(),
    );

    assert_eq!(scheduler.cancel_group(group), 2);
    assert_eq!(
        scheduler.snapshot(first.id()).expect("first task").state,
        TaskState::Cancelled
    );
    assert_eq!(
        scheduler.snapshot(second.id()).expect("second task").state,
        TaskState::Cancelled
    );
}

#[test]
fn local_and_inner_scheduler_groups_never_collide() {
    let scheduler = LocalScheduler::new();
    let groups = [
        scheduler.create_group(),
        scheduler.scheduler.borrow().create_group(),
        scheduler.create_group(),
        scheduler.scheduler.borrow().create_group(),
    ];

    let distinct: HashSet<_> = groups.iter().collect();
    assert_eq!(distinct.len(), groups.len());
    assert!(!groups.contains(&TaskGroupId::default()));
}

#[test]
fn cancelled_group_cancels_tasks_spawned_into_it_later() {
    let mut scheduler = Scheduler::new();
    let group = scheduler.create_group();
    let other = scheduler.create_group();
    scheduler.cancel_group(group);

    let late = scheduler.spawn(
        CallContext {
            task_group: group,
            ..CallContext::default()
        },
        async { Ok(()) },
    );
    let unaffected = scheduler.spawn(
        CallContext {
            task_group: other,
            ..CallContext::default()
        },
        async { Ok(()) },
    );

    assert_eq!(scheduler.poll_ready(), 1);
    assert_eq!(
        scheduler.snapshot(late.id()).expect("late task").state,
        TaskState::Cancelled
    );
    assert_eq!(
        scheduler
            .snapshot(unaffected.id())
            .expect("other task")
            .state,
        TaskState::Completed
    );
}

/// A VM and a thread whose function returns nil.
fn nil_thread() -> luau::runtime::Result<(luau::Vm, luau::Thread)> {
    let vm = luau::Vm::new()?;
    let function = vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(&b"return nil"[..]),
        luau::ChunkOrigin::default(),
    ))?;
    let thread = vm.create_thread(&function)?;
    Ok((vm, thread))
}

/// What a parked async call leaves: a pending task with no work.
fn park(scheduler: &mut Scheduler, id: TaskId) {
    scheduler.tasks.get_mut(&id).expect("task").work = None;
}

#[test]
fn only_a_parked_task_takes_a_future() -> luau::runtime::Result<()> {
    let mut scheduler = Scheduler::new();
    let task = scheduler.spawn(CallContext::default(), async { Ok(()) });
    scheduler.poll_ready();
    let (vm, thread) = nil_thread()?;

    assert!(!scheduler.replace_with_luau_future(
        task.id(),
        TaskGroupId::default(),
        vm,
        thread,
        luau::ScheduledFuture::new(std::future::pending::<luau::runtime::Result<()>>()),
        None,
    ));
    assert_eq!(
        scheduler.snapshot(task.id()).expect("task").state,
        TaskState::Completed
    );
    Ok(())
}

#[test]
fn a_task_awaiting_another_group_s_future_belongs_to_both_groups() -> luau::runtime::Result<()> {
    for cancel_callee in [true, false] {
        let mut scheduler = Scheduler::new();
        let caller = scheduler.create_group();
        let callee = scheduler.create_group();
        let task = scheduler.spawn(
            CallContext {
                task_group: caller,
                ..CallContext::default()
            },
            pending(),
        );
        let (vm, thread) = nil_thread()?;
        park(&mut scheduler, task.id());

        assert!(scheduler.replace_with_luau_future(
            task.id(),
            callee,
            vm,
            thread,
            luau::ScheduledFuture::new(std::future::pending::<luau::runtime::Result<()>>()),
            None,
        ));
        for group in [caller, callee] {
            let snapshots = scheduler.group_snapshots(group);
            assert_eq!(snapshots.len(), 1);
            assert_eq!(snapshots[0].id, task.id());
            assert_eq!(snapshots[0].group, caller);
        }

        let cancelled = if cancel_callee { callee } else { caller };
        assert_eq!(scheduler.cancel_group(cancelled), 1);
        assert_eq!(
            scheduler.snapshot(task.id()).expect("task").state,
            TaskState::Cancelled
        );
        scheduler.remove(task.id());
        assert!(scheduler.group_snapshots(caller).is_empty());
        assert!(scheduler.group_snapshots(callee).is_empty());
    }
    Ok(())
}

#[test]
fn a_task_leaves_the_awaited_group_once_its_future_completes() -> luau::runtime::Result<()> {
    let mut scheduler = Scheduler::new();
    let caller = scheduler.create_group();
    let callee = scheduler.create_group();
    let (vm, thread) = nil_thread()?;
    let task = scheduler.spawn(
        CallContext {
            task_group: caller,
            ..CallContext::default()
        },
        pending(),
    );
    park(&mut scheduler, task.id());
    assert!(scheduler.replace_with_luau_future(
        task.id(),
        callee,
        vm,
        thread,
        luau::ScheduledFuture::new(async { luau::runtime::Result::Ok(()) }),
        None,
    ));

    assert_eq!(scheduler.group_snapshots(callee).len(), 1);
    scheduler.poll_ready();
    assert!(scheduler.group_snapshots(callee).is_empty());
    assert_eq!(scheduler.group_snapshots(caller).len(), 1);
    Ok(())
}

#[test]
fn default_group_is_never_cancelled() {
    let mut scheduler = Scheduler::new();
    let pending_task = scheduler.spawn(CallContext::default(), pending());

    assert_eq!(scheduler.cancel_group(TaskGroupId::default()), 0);
    let later = scheduler.spawn(CallContext::default(), async { Ok(()) });

    assert_eq!(scheduler.poll_ready(), 1);
    assert_eq!(
        scheduler
            .snapshot(pending_task.id())
            .expect("pending task")
            .state,
        TaskState::Pending
    );
    assert_eq!(
        scheduler.snapshot(later.id()).expect("later task").state,
        TaskState::Completed
    );
}

#[test]
fn failed_future_preserves_error_for_diagnostics() {
    let mut scheduler = Scheduler::new();
    let handle = scheduler.spawn(CallContext::default(), async { anyhow::bail!("boom") });

    assert_eq!(scheduler.poll_ready(), 1);
    let snapshot = scheduler.snapshot(handle.id()).expect("task snapshot");
    assert_eq!(snapshot.state, TaskState::Failed);
    assert!(
        snapshot
            .error
            .as_deref()
            .is_some_and(|error| error.contains("boom"))
    );
}

#[test]
fn pending_future_is_polled_again_after_waker_signal() {
    let mut scheduler = Scheduler::new();
    let polls = Arc::new(AtomicUsize::new(0));
    let handle = scheduler.spawn(CallContext::default(), {
        let polls = polls.clone();
        poll_fn(move |cx| {
            if polls.fetch_add(1, Ordering::SeqCst) == 0 {
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        })
    });

    assert_eq!(scheduler.poll_ready(), 0);
    assert_eq!(
        scheduler
            .snapshot(handle.id())
            .expect("task snapshot")
            .state,
        TaskState::Pending
    );
    assert_eq!(scheduler.next_wake_delay(), Some(Duration::ZERO));

    assert_eq!(scheduler.poll_ready(), 1);
    assert_eq!(
        scheduler
            .snapshot(handle.id())
            .expect("task snapshot")
            .state,
        TaskState::Completed
    );
    assert_eq!(polls.load(Ordering::SeqCst), 2);
}

#[test]
fn delayed_task_waits_until_wake_time() {
    let clock = ManualClock::new();
    let mut scheduler = Scheduler::with_manual_clock(clock.clone());
    let handle = scheduler.spawn_boxed_after(
        CallContext::default(),
        Duration::from_secs(5),
        Box::pin(async { Ok(()) }),
    );
    let state = |scheduler: &Scheduler| {
        scheduler
            .snapshot(handle.id())
            .expect("task snapshot")
            .state
    };

    assert_eq!(scheduler.poll_ready(), 0);
    assert_eq!(scheduler.next_wake_delay(), Some(Duration::from_secs(5)));

    clock.advance(Duration::from_secs(5) - Duration::from_nanos(1));
    assert_eq!(scheduler.poll_ready(), 0);
    assert_eq!(state(&scheduler), TaskState::Pending);
    assert_eq!(scheduler.next_wake_delay(), Some(Duration::from_nanos(1)));

    clock.advance(Duration::from_nanos(1));
    assert_eq!(scheduler.poll_ready(), 1);
    assert_eq!(state(&scheduler), TaskState::Completed);
}

#[test]
fn a_cancelled_delayed_task_leaves_nothing_to_wait_for() {
    let mut scheduler = Scheduler::with_manual_clock(ManualClock::new());
    let handle = scheduler.spawn_boxed_after(
        CallContext::default(),
        Duration::from_secs(5),
        Box::pin(async { Ok(()) }),
    );
    assert_eq!(scheduler.next_wake_delay(), Some(Duration::from_secs(5)));

    assert!(scheduler.cancel(handle.id()));
    assert_eq!(scheduler.next_wake_delay(), None);
}

#[test]
fn luau_root_thread_completes_on_scheduler() -> luau::runtime::Result<()> {
    let vm = luau::Vm::new()?;
    let function = vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(&b"local value = ...; return value + 1"[..]),
        luau::ChunkOrigin {
            module: Some(ModuleId(Arc::from("demo/root"))),
            plugin: Some(Arc::from("demo")),
            path: Some(Arc::from("plugins/demo/init.luau")),
        },
    ))?;
    let mut scheduler = Scheduler::new();
    let group = scheduler.create_group();
    let context = CallContext {
        origin: ChunkOrigin {
            module: Some(ModuleId(Arc::from("demo/root"))),
            plugin: Some(Arc::from("demo")),
            path: Some(Arc::from("plugins/demo/init.luau")),
        },
        task_group: group,
        ..CallContext::default()
    };

    let handle =
        scheduler.spawn_luau_function(context, vm, &function, vec![luau::Value::Number(41.0)])?;

    assert_eq!(scheduler.poll_ready(), 1);
    let snapshot = scheduler.snapshot(handle.id()).expect("task snapshot");
    assert_eq!(snapshot.state, TaskState::Completed);
    assert_eq!(snapshot.origin.plugin.as_deref(), Some("demo"));
    Ok(())
}

#[test]
fn luau_root_thread_failure_is_scheduler_failure() -> luau::runtime::Result<()> {
    let vm = luau::Vm::new()?;
    vm.open_standard_libraries(luau::StandardLibraries {
        base: true,
        ..luau::StandardLibraries::none()
    })?;
    let function = vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(&b"error('root failed')"[..]),
        luau::ChunkOrigin::default(),
    ))?;
    let mut scheduler = Scheduler::new();
    let handle = scheduler.spawn_luau_function(CallContext::default(), vm, &function, vec![])?;

    assert_eq!(scheduler.poll_ready(), 1);
    let snapshot = scheduler.snapshot(handle.id()).expect("task snapshot");
    assert_eq!(snapshot.state, TaskState::Failed);
    assert!(
        snapshot
            .error
            .as_deref()
            .is_some_and(|error| error.contains("root failed"))
    );
    Ok(())
}

#[test]
fn per_task_luau_budget_does_not_leak_to_other_tasks() -> luau::runtime::Result<()> {
    let vm = luau::Vm::new()?;
    let timed_function = vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(&b"while true do end"[..]),
        luau::ChunkOrigin::default(),
    ))?;
    let ordinary_function = vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(
            &b"local total = 0; for value = 1, 100000 do total += value end; return total"[..],
        ),
        luau::ChunkOrigin::default(),
    ))?;
    let timed_thread = vm.create_thread(&timed_function)?;
    let ordinary_thread = vm.create_thread(&ordinary_function)?;
    let mut scheduler = Scheduler::new();
    let timed = scheduler.spawn_luau_thread_with_options(
        CallContext::default(),
        vm.clone(),
        timed_thread,
        Vec::new(),
        LuauTaskOptions {
            resume_limit: Some(ResumeLimit::budget(Duration::ZERO)),
            completion: None,
        },
    );
    let ordinary =
        scheduler.spawn_luau_thread(CallContext::default(), vm, ordinary_thread, Vec::new());

    assert_eq!(scheduler.poll_ready(), 2);
    assert_eq!(
        scheduler.snapshot(timed.id()).expect("timed task").state,
        TaskState::Failed
    );
    assert_eq!(
        scheduler
            .snapshot(ordinary.id())
            .expect("ordinary task")
            .state,
        TaskState::Completed
    );
    Ok(())
}

#[test]
fn a_resume_limited_until_a_deadline_stops_at_it_or_at_its_signal() -> luau::runtime::Result<()> {
    let vm = luau::Vm::new()?;
    let function = vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(&b"while true do end"[..]),
        luau::ChunkOrigin::default(),
    ))?;
    let raised = luau::InterruptSignal::new();
    raised.raise();
    let mut scheduler = Scheduler::new();
    let far = Instant::now() + Duration::from_secs(600);
    let limits = [
        (ResumeLimit::until(far).or_on(raised), ResumeBound::Signal),
        (ResumeLimit::until(Instant::now()), ResumeBound::Deadline),
        // Each resume gets the smaller of its budget and what is left until its deadline.
        (
            ResumeLimit::until(far).with_budget(Duration::ZERO),
            ResumeBound::Budget,
        ),
        (
            ResumeLimit::until(Instant::now()).with_budget(Duration::from_secs(600)),
            ResumeBound::Deadline,
        ),
        (
            ResumeLimit::budget(Duration::from_secs(600)).not_past(Instant::now()),
            ResumeBound::Deadline,
        ),
        (
            ResumeLimit::until(far).not_past(Instant::now()),
            ResumeBound::Deadline,
        ),
    ];
    let limited = limits.len();
    let tasks: Vec<_> = limits
        .into_iter()
        .map(|(limit, bound)| -> luau::runtime::Result<_> {
            let completion = Rc::new(LocalLuauTaskCompletion::default());
            let task = scheduler.spawn_luau_thread_with_options(
                CallContext::default(),
                vm.clone(),
                vm.create_thread(&function)?,
                Vec::new(),
                LuauTaskOptions {
                    resume_limit: Some(limit),
                    completion: Some(completion.clone()),
                },
            );
            Ok((task, completion, bound))
        })
        .collect::<luau::runtime::Result<_>>()?;

    let started = Instant::now();
    assert_eq!(scheduler.poll_ready(), limited);
    assert!(started.elapsed() < Duration::from_secs(60));
    for (task, completion, bound) in tasks {
        assert_eq!(
            scheduler.snapshot(task.id()).expect("task").state,
            TaskState::Failed
        );
        assert_eq!(completion.interrupted_by(), Some(bound));
    }
    Ok(())
}

/// Which bound cut a resume short is read as the resume starts, from the scheduler's clock,
/// and not from the time left once it has failed: a budget that runs out just before the
/// deadline is the budget's doing, and on a clock that stands still the deadline's remains
/// the deadline's.
#[test]
fn a_task_tells_which_bound_of_its_limit_cut_it_short() -> luau::runtime::Result<()> {
    const BUDGET: Duration = Duration::from_millis(40);

    let vm = luau::Vm::new()?;
    vm.open_standard_libraries(luau::StandardLibraries::all_supported())?;
    let run = |source: &'static str, limit: fn(Instant) -> ResumeLimit| {
        let clock = ManualClock::new();
        let scheduler = LocalScheduler::with_manual_clock(clock.clone());
        let completion = Rc::new(LocalLuauTaskCompletion::default());
        scheduler.schedule_luau_thread_with_limit_and_completion(
            CallContext::default(),
            vm.clone(),
            chunk_thread(&vm, source)?,
            Vec::new(),
            limit(clock.now()),
            completion.clone(),
        );
        scheduler.poll_ready();
        assert!(completion.ended_at().is_some(), "{source} ended");
        luau::runtime::Result::Ok(completion.interrupted_by())
    };

    assert_eq!(
        run("while true do end", |now| {
            ResumeLimit::until(now + BUDGET + Duration::from_millis(5)).with_budget(BUDGET)
        })?,
        Some(ResumeBound::Budget)
    );
    assert_eq!(
        run("while true do end", |now| {
            ResumeLimit::until(now + BUDGET).with_budget(BUDGET * 2)
        })?,
        Some(ResumeBound::Deadline)
    );
    assert_eq!(
        run("while true do end", |now| {
            ResumeLimit::until(now + BUDGET).with_budget(BUDGET)
        })?,
        Some(ResumeBound::Budget),
        "a tie is the budget's"
    );
    let ample = |now| ResumeLimit::until(now + Duration::from_secs(600));
    assert_eq!(run("error('boom')", ample)?, None);
    assert_eq!(run("return 42", ample)?, None);

    // A failure of its own is no interruption, though the limit passed before it came, and
    // though it reads like one.
    let outlast: luau::NativeFn = Arc::new(|_| {
        std::thread::sleep(BUDGET * 2);
        Err(luau::Error::Runtime("failed on its own".to_string()))
    });
    let outlast = vm.create_function_with_options(
        luau::NativeFunctionOptions::new(luau::ChunkOrigin::default()).function_name("outlast"),
        outlast,
    )?;
    vm.set_global_function("outlast", &outlast)?;
    for limit in [
        (|_| ResumeLimit::budget(BUDGET)) as fn(Instant) -> ResumeLimit,
        |now| ResumeLimit::until(now + BUDGET),
    ] {
        assert_eq!(run("outlast()", limit)?, None);
        // The interrupt raised, and was caught: what the thread then failed with is its own.
        assert_eq!(
            run(
                "pcall(function() pcall(outlast) while true do end end) local x = nil + 1",
                limit
            )?,
            None
        );
        assert!(
            run(
                "pcall(function() pcall(outlast) while true do end end) while true do end",
                limit
            )?
            .is_some(),
            "caught once, the interrupt's own error still ends it"
        );
    }
    assert_eq!(run("error('Luau execution interrupted')", ample)?, None);
    Ok(())
}

#[test]
fn pending_budgeted_luau_thread_preserves_budget_and_completion() -> luau::runtime::Result<()> {
    let vm = luau::Vm::new()?;
    let function = vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(&b"return 42"[..]),
        luau::ChunkOrigin::default(),
    ))?;
    let thread = vm.create_thread(&function)?;
    let scheduler = LocalScheduler::new();
    let completion = Rc::new(LocalLuauTaskCompletion::default());

    scheduler.polling.set(true);
    scheduler.schedule_luau_thread_with_limit_and_completion(
        CallContext::default(),
        vm,
        thread.clone(),
        Vec::new(),
        ResumeLimit::budget(Duration::from_secs(1)),
        completion.clone(),
    );
    scheduler.polling.set(false);
    scheduler.flush_pending_luau_threads();

    let handle = scheduler
        .luau_thread_handle(&thread)
        .expect("scheduled thread handle");
    let inner = scheduler.scheduler.borrow();
    let task = inner.tasks.get(&handle.id()).expect("scheduled task");
    assert!(matches!(
        task.resume_limit,
        Some(ResumeLimit { budget: Some(budget), deadline: None, signal: None })
            if budget == Duration::from_secs(1)
    ));
    assert!(Rc::ptr_eq(
        task.completion.as_ref().expect("task completion"),
        &completion
    ));
    drop(inner);

    assert_eq!(scheduler.poll_ready(), 1);
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(Ok(values)) = completion.poll(&mut cx) else {
        panic!("completion should contain the Luau result");
    };
    assert!(matches!(values.as_slice(), [luau::Value::Number(42.0)]));
    Ok(())
}

/// Runs `test` on a thread of its own and fails if it is still running after a long while,
/// as one whose wait missed its wake would be for good.
pub(crate) fn ending_without_a_missed_wake(test: impl FnOnce() + Send + 'static) {
    let (done, ended) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        test();
        let _ = done.send(());
    });
    match ended.recv_timeout(Duration::from_secs(60)) {
        Ok(()) => thread.join().expect("test thread"),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("nothing ended the scheduler's wait")
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            std::panic::resume_unwind(thread.join().expect_err("the test panicked"))
        }
    }
}

/// Returns once the scheduler's owner is asleep in a wait.
fn once_waiting(wake: &SchedulerWake) {
    while !wake.owner_is_waiting() {
        std::thread::yield_now();
    }
}

/// Runs rounds that sleep in between, as a host does, until `done`. Only a wake or a due
/// task ends each sleep.
fn run_until(scheduler: &LocalScheduler, done: impl Fn() -> bool) {
    loop {
        scheduler.poll_ready();
        if done() {
            return;
        }
        scheduler.wait_for_work(None);
    }
}

#[test]
fn a_wake_handle_ends_a_wait_from_another_thread() {
    ending_without_a_missed_wake(|| {
        let scheduler = Scheduler::new();
        let wake = scheduler.wake_handle();
        let waker = std::thread::spawn(move || {
            once_waiting(&wake);
            wake.wake();
        });

        scheduler.wait_for_wake(None);
        waker.join().expect("waker thread");
    });
}

/// The task's own waker, called from another thread while the owner sleeps with nothing due,
/// is all that ends the wait.
#[test]
fn a_task_s_waker_ends_a_wait_from_another_thread() {
    ending_without_a_missed_wake(|| {
        let scheduler = LocalScheduler::new();
        let (ready, readied) = tokio::sync::oneshot::channel::<()>();
        let seen = Rc::new(Cell::new(false));
        scheduler.schedule_future(CallContext::default(), {
            let seen = seen.clone();
            async move {
                let _ = readied.await;
                seen.set(true);
                Ok(())
            }
        });
        scheduler.poll_ready();
        assert!(!seen.get());

        let waker = std::thread::spawn({
            let wake = scheduler.wake_handle();
            move || {
                once_waiting(&wake);
                let _ = ready.send(());
            }
        });
        run_until(&scheduler, || seen.get());
        waker.join().expect("waking thread");
    });
}

#[test]
fn a_wake_before_the_wait_is_kept_for_it() {
    ending_without_a_missed_wake(|| {
        let scheduler = Scheduler::new();
        scheduler.wake_handle().waker().wake();

        scheduler.wait_for_wake(None);
    });
}

#[test]
fn a_cancellation_ends_a_wait_for_work_without_a_deadline() {
    ending_without_a_missed_wake(|| {
        let scheduler = LocalScheduler::new();
        let cancellation = Cancellation::new(scheduler.wake_handle());
        let cancel = std::thread::spawn({
            let (cancellation, wake) = (cancellation.clone(), scheduler.wake_handle());
            move || {
                once_waiting(&wake);
                cancellation.cancel();
            }
        });

        scheduler.wait_for_work(None);
        assert!(cancellation.is_cancelled());
        cancel.join().expect("cancelling thread");
    });
}

#[test]
fn a_task_spawns_another_while_the_scheduler_polls() {
    let scheduler = Rc::new(LocalScheduler::new());
    let ran = Rc::new(Cell::new(false));
    scheduler.schedule_future(CallContext::default(), {
        let (scheduler, ran) = (Rc::downgrade(&scheduler), ran.clone());
        async move {
            let scheduler = scheduler.upgrade().expect("the scheduler is polling");
            scheduler.schedule_future(CallContext::default(), async move {
                ran.set(true);
                Ok(())
            });
            Ok(())
        }
    });

    scheduler.poll_ready();
    assert!(!ran.get(), "a task spawned in a poll starts after it");
    assert_eq!(scheduler.next_wake_delay(), Some(Duration::ZERO));
    scheduler.poll_ready();
    assert!(ran.get());
}

/// On the real clock and the real wait: nothing but the sleep's deadline ends the wait.
#[test]
fn a_sleep_ends_the_wait_and_wakes_its_task_at_the_deadline() {
    ending_without_a_missed_wake(|| {
        let scheduler = LocalScheduler::new();
        let woke = Rc::new(Cell::new(None));
        let deadline = Instant::now() + Duration::from_millis(50);
        let sleep = scheduler.sleep_until(deadline);
        scheduler.schedule_future(CallContext::default(), {
            let woke = woke.clone();
            async move {
                sleep.await;
                woke.set(Some(Instant::now()));
                Ok(())
            }
        });

        run_until(&scheduler, || woke.get().is_some());
        let woke = woke.get().expect("the task woke");
        assert!(woke >= deadline, "woken {:?} early", deadline - woke);
    });
}

/// A task waiting on a sleep, and whether it has woken.
fn sleeper(scheduler: &LocalScheduler, deadline: Instant) -> Rc<Cell<bool>> {
    let woke = Rc::new(Cell::new(false));
    let sleep = scheduler.sleep_until(deadline);
    scheduler.schedule_future(CallContext::default(), {
        let woke = woke.clone();
        async move {
            sleep.await;
            woke.set(true);
            Ok(())
        }
    });
    woke
}

/// A deadline is the first instant that is too late: a sleep is over at it, not after it.
#[test]
fn a_sleep_is_over_at_its_deadline_and_not_before() {
    let clock = ManualClock::new();
    let scheduler = LocalScheduler::with_manual_clock(clock.clone());
    let woke = sleeper(&scheduler, clock.now() + Duration::from_secs(5));

    scheduler.poll_ready();
    assert!(!woke.get());
    assert_eq!(scheduler.next_wake_delay(), Some(Duration::from_secs(5)));

    clock.advance(Duration::from_secs(5) - Duration::from_nanos(1));
    scheduler.poll_ready();
    scheduler.poll_ready();
    assert!(!woke.get(), "woken before the deadline");
    assert_eq!(scheduler.next_wake_delay(), Some(Duration::from_nanos(1)));

    clock.advance(Duration::from_nanos(1));
    assert!(!woke.get(), "advancing the clock ran the task");
    scheduler.poll_ready();
    assert!(woke.get(), "still asleep at the deadline");
    assert_eq!(scheduler.next_wake_delay(), None);

    let late = sleeper(&scheduler, clock.now() - Duration::from_secs(1));
    scheduler.poll_ready();
    assert!(late.get(), "a sleep past its deadline waits");
}

#[test]
fn a_dropped_sleep_leaves_nothing_to_wait_for() {
    let clock = ManualClock::new();
    let scheduler = LocalScheduler::with_manual_clock(clock.clone());
    let mut sleep = scheduler.sleep_until(clock.now() + Duration::from_secs(3600));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(Pin::new(&mut sleep).poll(&mut cx).is_pending());
    assert_eq!(scheduler.next_wake_delay(), Some(Duration::from_secs(3600)));

    drop(sleep);
    assert_eq!(scheduler.next_wake_delay(), None);
}

#[test]
#[should_panic(expected = "manual clock can't block")]
fn a_scheduler_on_a_manual_clock_refuses_to_wait_for_work() {
    let clock = ManualClock::new();
    let scheduler = LocalScheduler::with_manual_clock(clock.clone());
    // Even with work due at once, which a real wait would return for.
    scheduler.schedule_future(CallContext::default(), async { Ok(()) });
    scheduler.wait_for_work(Some(clock.now()));
}

#[test]
#[should_panic(expected = "manual clock can't block")]
fn a_scheduler_on_a_manual_clock_refuses_to_wait_for_a_wake() {
    let scheduler = Scheduler::with_manual_clock(ManualClock::new());
    scheduler.wake_handle().wake();
    scheduler.wait_for_wake(Some(Duration::ZERO));
}

fn chunk_thread(vm: &luau::Vm, source: &'static str) -> luau::runtime::Result<luau::Thread> {
    vm.create_thread(&vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(source.as_bytes()),
        luau::ChunkOrigin::default(),
    ))?)
}

/// Schedules a thread that returns at once, and a task that moves the clock on by a second,
/// in the order given, and runs them in one round. Returns when the thread ended, as its
/// completion has it, counted from the round's start.
fn ended_after(advance_first: bool) -> luau::runtime::Result<Duration> {
    let vm = luau::Vm::new()?;
    let clock = ManualClock::new();
    let scheduler = LocalScheduler::with_manual_clock(clock.clone());
    let completion = Rc::new(LocalLuauTaskCompletion::default());
    let advance = |scheduler: &LocalScheduler| {
        let clock = clock.clone();
        scheduler.schedule_future(CallContext::default(), async move {
            clock.advance(Duration::from_secs(1));
            Ok(())
        });
    };
    if advance_first {
        advance(&scheduler);
    }
    scheduler.schedule_luau_thread_with_completion(
        CallContext::default(),
        vm.clone(),
        chunk_thread(&vm, "return 42")?,
        Vec::new(),
        completion.clone(),
    );
    if !advance_first {
        advance(&scheduler);
    }

    let start = clock.now();
    assert!(completion.ended_at().is_none());
    scheduler.poll_ready();
    assert_eq!(clock.now(), start + Duration::from_secs(1));
    Ok(completion.ended_at().expect("the thread ended") - start)
}

/// Watches a thread that ends at its deadline, before the watcher sees the deadline pass.
fn watched_ending_at_its_deadline(
    late: LateEnd,
) -> luau::runtime::Result<Watched<std::convert::Infallible>> {
    let vm = luau::Vm::new()?;
    let clock = ManualClock::new();
    let scheduler = Rc::new(LocalScheduler::with_manual_clock(clock.clone()));
    let deadline = clock.now() + Duration::from_secs(1);
    scheduler.schedule_future(CallContext::default(), {
        let clock = clock.clone();
        async move {
            clock.advance(Duration::from_secs(1));
            Ok(())
        }
    });
    let watched = scheduler.schedule_watched_luau_thread(
        CallContext::default(),
        vm.clone(),
        chunk_thread(&vm, "return 42")?,
        Vec::new(),
        None,
    );
    let completion = watched.completion.clone();
    let outcome = Rc::new(RefCell::new(None));
    scheduler.schedule_watcher(CallContext::default(), {
        let outcome = outcome.clone();
        async move {
            let watched = watched.watch(std::future::pending(), deadline, late).await;
            *outcome.borrow_mut() = Some(watched);
            Ok(())
        }
    });

    scheduler.poll_ready();
    assert_eq!(completion.ended_at(), Some(deadline));
    assert!(outcome.borrow().is_none(), "the watch ended in the round");
    scheduler.poll_ready();
    Ok(outcome.take().expect("the watch ended"))
}

#[test]
fn a_watch_takes_an_end_at_the_deadline_as_its_rule_says() -> luau::runtime::Result<()> {
    assert!(matches!(
        watched_ending_at_its_deadline(LateEnd::Expired)?,
        Watched::Expired
    ));
    assert!(matches!(
        watched_ending_at_its_deadline(LateEnd::Ended)?,
        Watched::Ended { result: Ok(values), .. }
            if matches!(values[..], [luau::Value::Number(n)] if n == 42.0)
    ));
    Ok(())
}

#[test]
fn a_watch_cancels_a_thread_still_running_once_stopped() -> luau::runtime::Result<()> {
    let vm = luau::Vm::new()?;
    vm.open_standard_libraries(luau::StandardLibraries::all_supported())?;
    let clock = ManualClock::new();
    let scheduler = Rc::new(LocalScheduler::with_manual_clock(clock.clone()));
    let watched = scheduler.schedule_watched_luau_thread(
        CallContext::default(),
        vm.clone(),
        chunk_thread(&vm, "coroutine.yield()")?,
        Vec::new(),
        None,
    );
    let completion = watched.completion.clone();
    let stop = Cancellation::new(scheduler.wake_handle());
    let deadline = clock.now() + Duration::from_secs(3600);
    let outcome = Rc::new(RefCell::new(None));
    scheduler.schedule_watcher(CallContext::default(), {
        let (outcome, stop) = (outcome.clone(), stop.clone());
        async move {
            let watched = watched
                .watch(stop.cancelled(), deadline, LateEnd::Expired)
                .await;
            *outcome.borrow_mut() = Some(watched);
            Ok(())
        }
    });
    scheduler.poll_ready();
    assert!(completion.ended_at().is_none(), "a yield ended the thread");

    stop.cancel();
    scheduler.poll_ready();
    let ended = completion_message(&completion).expect("the thread is cancelled");
    assert!(ended.is_err_and(|error| error.contains("cancelled")));
    assert!(
        outcome.borrow().is_none(),
        "the watch ended before the cancel settled"
    );
    scheduler.poll_ready();
    assert!(matches!(outcome.take(), Some(Watched::Stopped(()))));
    Ok(())
}

/// The end is timed when it happens, not when its round began or ended.
#[test]
fn a_completion_records_the_time_its_thread_ended() -> luau::runtime::Result<()> {
    assert_eq!(ended_after(false)?, Duration::ZERO);
    assert_eq!(ended_after(true)?, Duration::from_secs(1));
    Ok(())
}

#[test]
fn a_completion_records_the_time_its_thread_was_cancelled() -> luau::runtime::Result<()> {
    let vm = luau::Vm::new()?;
    vm.open_standard_libraries(luau::StandardLibraries::all_supported())?;
    let clock = ManualClock::new();
    let scheduler = LocalScheduler::with_manual_clock(clock.clone());
    let start = clock.now();
    let completion = Rc::new(LocalLuauTaskCompletion::default());
    let thread = chunk_thread(&vm, "coroutine.yield()")?;
    scheduler.schedule_luau_thread_with_completion(
        CallContext::default(),
        vm.clone(),
        thread.clone(),
        Vec::new(),
        completion.clone(),
    );
    scheduler.poll_ready();
    assert!(completion.ended_at().is_none(), "a yield ended the thread");

    clock.advance(Duration::from_secs(3));
    assert!(completion.ended_at().is_none());
    clock.advance(Duration::from_secs(4));
    assert!(scheduler.cancel_luau_thread(&thread));
    clock.advance(Duration::from_secs(5));

    assert_eq!(completion.ended_at(), Some(start + Duration::from_secs(7)));
    let ended = completion_message(&completion).expect("the thread is cancelled");
    assert!(ended.is_err_and(|error| error.contains("cancelled")));
    Ok(())
}

/// The deadline is read by the scheduler's clock, and what is left until it is enforced in
/// real time: a clock that stands still doesn't let a resume run for good.
#[test]
fn a_frozen_clock_still_interrupts_a_resume_at_its_limit() -> luau::runtime::Result<()> {
    let vm = luau::Vm::new()?;
    let clock = ManualClock::new();
    let scheduler = LocalScheduler::with_manual_clock(clock.clone());
    let limits = [
        ResumeLimit::budget(Duration::from_millis(20)),
        ResumeLimit::until(clock.now() + Duration::from_millis(20)),
    ];
    let completions = limits.map(|limit| -> luau::runtime::Result<_> {
        let completion = Rc::new(LocalLuauTaskCompletion::default());
        scheduler.schedule_luau_thread_with_limit_and_completion(
            CallContext::default(),
            vm.clone(),
            chunk_thread(&vm, "while true do end")?,
            Vec::new(),
            limit,
            completion.clone(),
        );
        Ok(completion)
    });

    let start = clock.now();
    assert_eq!(scheduler.poll_ready(), 2);
    assert_eq!(clock.now(), start);
    for completion in completions {
        let completion = completion?;
        let ended = completion_message(&completion).expect("the thread ended");
        assert!(
            ended.is_err_and(|error| error.contains(luau::INTERRUPTED_ERROR)),
            "the resume wasn't interrupted"
        );
    }
    Ok(())
}

#[test]
fn a_resume_limited_until_a_deadline_already_passed_by_the_clock_is_interrupted_at_once()
-> luau::runtime::Result<()> {
    let vm = luau::Vm::new()?;
    let clock = ManualClock::new();
    let scheduler = LocalScheduler::with_manual_clock(clock.clone());
    // An hour away in real time, and passed once the clock says so.
    let deadline = clock.now() + Duration::from_secs(3600);
    clock.advance(Duration::from_secs(3600));
    let completion = Rc::new(LocalLuauTaskCompletion::default());
    scheduler.schedule_luau_thread_with_limit_and_completion(
        CallContext::default(),
        vm.clone(),
        chunk_thread(&vm, "while true do end")?,
        Vec::new(),
        ResumeLimit::until(deadline),
        completion.clone(),
    );

    assert_eq!(scheduler.poll_ready(), 1);
    let ended = completion_message(&completion).expect("the thread ended");
    assert!(ended.is_err_and(|error| error.contains(luau::INTERRUPTED_ERROR)));
    Ok(())
}

#[test]
fn a_cancellation_wakes_the_task_waiting_on_it() {
    ending_without_a_missed_wake(|| {
        let scheduler = LocalScheduler::new();
        let cancellation = Cancellation::new(scheduler.wake_handle());
        let seen = Rc::new(Cell::new(false));
        scheduler.schedule_future(CallContext::default(), {
            let (cancellation, seen) = (cancellation.clone(), seen.clone());
            async move {
                cancellation.cancelled().await;
                seen.set(true);
                Ok(())
            }
        });
        scheduler.poll_ready();
        assert!(!seen.get());

        let cancel = std::thread::spawn({
            let wake = scheduler.wake_handle();
            move || {
                once_waiting(&wake);
                cancellation.cancel();
            }
        });
        run_until(&scheduler, || seen.get());
        cancel.join().expect("cancelling thread");
    });
}

/// A thread that waits forever on a host call, whose future raises `dropped` when dropped.
fn waiting_thread(
    vm: &luau::Vm,
    scheduler: &LocalScheduler,
    context: CallContext,
    dropped: Arc<AtomicBool>,
) -> luau::runtime::Result<luau::Thread> {
    struct RaiseOnDrop(Arc<AtomicBool>);
    impl Drop for RaiseOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    let thread = vm.create_thread(&vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(&b"coroutine.yield()"[..]),
        luau::ChunkOrigin::default(),
    ))?)?;
    scheduler.spawn_luau_thread(context.clone(), vm.clone(), thread.clone(), Vec::new());
    scheduler.poll_ready();
    let guard = RaiseOnDrop(dropped);
    scheduler.schedule_luau_future(
        context,
        vm.clone(),
        thread.clone(),
        luau::ScheduledFuture::new(async move {
            let _guard = guard;
            pending::<luau::runtime::Result<()>>().await
        }),
    );
    Ok(thread)
}

fn completion_message(completion: &LocalLuauTaskCompletion) -> Option<Result<(), String>> {
    match completion.poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(result) => Some(result.map(|_| ()).map_err(|error| error.message)),
        Poll::Pending => None,
    }
}

#[test]
fn a_watcher_scheduled_after_a_thread_cancels_it_before_it_starts() -> luau::runtime::Result<()> {
    let vm = luau::Vm::new()?;
    let scheduler = Rc::new(LocalScheduler::new());
    let thread = vm.create_thread(&vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(&b"ran = true"[..]),
        luau::ChunkOrigin::default(),
    ))?)?;
    let completion = Rc::new(LocalLuauTaskCompletion::default());
    scheduler.schedule_luau_thread_with_completion(
        CallContext::default(),
        vm.clone(),
        thread.clone(),
        Vec::new(),
        completion.clone(),
    );
    scheduler.schedule_watcher(CallContext::default(), {
        let (scheduler, thread) = (Rc::downgrade(&scheduler), thread.clone());
        async move {
            scheduler
                .upgrade()
                .expect("the scheduler is polling")
                .cancel_luau_thread(&thread);
            Ok(())
        }
    });

    scheduler.poll_ready();
    assert!(!ran(&vm)?, "the cancelled thread still ran");
    let ended = completion_message(&completion).expect("the thread is cancelled");
    assert!(ended.is_err_and(|error| error.contains("cancelled")));
    assert!(completion.ended_at().is_some());
    Ok(())
}

#[test]
fn a_watcher_woken_after_its_thread_still_cancels_it_before_it_resumes() -> luau::runtime::Result<()>
{
    let vm = luau::Vm::new()?;
    let scheduler = Rc::new(LocalScheduler::new());
    let thread = vm.create_thread(&vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(&b"coroutine.yield()"[..]),
        luau::ChunkOrigin::default(),
    ))?)?;
    scheduler.spawn_luau_thread(
        CallContext::default(),
        vm.clone(),
        thread.clone(),
        Vec::new(),
    );
    scheduler.poll_ready();
    let (ready, readied) = tokio::sync::oneshot::channel::<()>();
    let resumed = Arc::new(AtomicBool::new(false));
    scheduler.schedule_luau_future(
        CallContext::default(),
        vm,
        thread.clone(),
        luau::ScheduledFuture::new({
            let resumed = resumed.clone();
            async move {
                let _ = readied.await;
                resumed.store(true, Ordering::Release);
                Ok(())
            }
        }),
    );
    let (leave, left) = tokio::sync::oneshot::channel::<()>();
    scheduler.schedule_watcher(CallContext::default(), {
        let scheduler = Rc::downgrade(&scheduler);
        async move {
            let _ = left.await;
            scheduler
                .upgrade()
                .expect("the scheduler is polling")
                .cancel_luau_thread(&thread);
            Ok(())
        }
    });
    scheduler.poll_ready();
    assert!(scheduler.has_pending());

    // The thread's wake is queued ahead of its watcher's.
    let _ = ready.send(());
    let _ = leave.send(());
    scheduler.poll_ready();
    assert!(
        !resumed.load(Ordering::Acquire),
        "the cancelled thread's call still completed"
    );
    scheduler.poll_ready();
    assert!(!resumed.load(Ordering::Acquire));
    Ok(())
}

/// The outer thread waits on a call that runs an inner thread, which the call cancels when
/// dropped. The inner thread is ready in the round the outer one's watcher cancels it.
#[test]
fn a_watcher_s_cancel_reaches_the_threads_its_thread_was_waiting_on() -> luau::runtime::Result<()> {
    struct CancelOnDrop(std::rc::Weak<LocalScheduler>, luau::Thread);
    impl Drop for CancelOnDrop {
        fn drop(&mut self) {
            if let Some(scheduler) = self.0.upgrade() {
                scheduler.schedule_cancel_luau_thread(self.1.clone());
            }
        }
    }

    let vm = luau::Vm::new()?;
    let scheduler = Rc::new(LocalScheduler::new());
    let chunk = |source: &'static [u8]| {
        vm.create_thread(&vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(source),
            luau::ChunkOrigin::default(),
        ))?)
    };
    let outer = chunk(b"coroutine.yield()")?;
    scheduler.spawn_luau_thread(
        CallContext::default(),
        vm.clone(),
        outer.clone(),
        Vec::new(),
    );
    scheduler.poll_ready();
    let (leave, left) = tokio::sync::oneshot::channel::<()>();
    scheduler.schedule_watcher(CallContext::default(), {
        let scheduler = Rc::downgrade(&scheduler);
        let outer = outer.clone();
        async move {
            let _ = left.await;
            scheduler
                .upgrade()
                .expect("the scheduler is polling")
                .cancel_luau_thread(&outer);
            Ok(())
        }
    });
    scheduler.poll_ready();

    let inner = chunk(b"ran = true")?;
    scheduler.spawn_luau_thread(
        CallContext::default(),
        vm.clone(),
        inner.clone(),
        Vec::new(),
    );
    let guard = CancelOnDrop(Rc::downgrade(&scheduler), inner.clone());
    scheduler.schedule_luau_future(
        CallContext::default(),
        vm.clone(),
        outer,
        luau::ScheduledFuture::new(async move {
            let _guard = guard;
            pending::<luau::runtime::Result<()>>().await
        }),
    );
    let _ = leave.send(());
    scheduler.poll_ready();

    assert!(!ran(&vm)?, "the inner thread ran");
    Ok(())
}

/// A task's own panic fails the task alone. What unwinds a poll is a panic outside the tasks'
/// code, such as one from the waker a completion wakes.
/// A VM whose scheduler is its own, with `wait()`, a host call that never returns, `pause()`,
/// one that returns in the next round, `cancel(thread)` and `cancel_group(id)`, and
/// `cancel_as_it_wakes()` and `cancel_group_as_it_wakes()`, waits that cancel the thread they
/// are about to wake, or its group.
fn vm_with_waits() -> luau::runtime::Result<(luau::Vm, Rc<LocalScheduler>)> {
    vm_with_waits_on(LocalScheduler::new())
}

/// [`vm_with_waits`] with `scheduler` as the VM's.
fn vm_with_waits_on(
    scheduler: LocalScheduler,
) -> luau::runtime::Result<(luau::Vm, Rc<LocalScheduler>)> {
    use crate::modules::{
        FunctionSpec,
        ModuleSpec,
        install_luau_module,
    };

    let vm = luau::Vm::new()?;
    vm.open_standard_libraries(luau::StandardLibraries::all_supported())?;
    vm.data().insert(scheduler)?;
    let module = ModuleSpec::new("demo/waits")
        .function(FunctionSpec::new("wait").call_async(Arc::new(|_frame| {
            Ok(luau::ScheduledFuture::new(pending::<
                luau::runtime::Result<()>,
            >()))
        })))
        .function(FunctionSpec::new("pause").call_async(Arc::new(|_frame| {
            Ok(luau::ScheduledFuture::after(Duration::ZERO, async {
                Ok(())
            }))
        })))
        .function(
            FunctionSpec::new("cancel_as_it_wakes").call_async(Arc::new(|frame| {
                let scheduler = Rc::downgrade(&frame.vm.data().get::<LocalScheduler>()?);
                let thread = frame.thread.clone();
                Ok(luau::ScheduledFuture::after(Duration::ZERO, async move {
                    scheduler
                        .upgrade()
                        .expect("the scheduler is polling")
                        .cancel_luau_thread(&thread);
                    Ok(())
                }))
            })),
        )
        .function(
            FunctionSpec::new("cancel_group_as_it_wakes").call_async(Arc::new(|frame| {
                let scheduler = Rc::downgrade(&frame.vm.data().get::<LocalScheduler>()?);
                let group = frame.context.task_group;
                Ok(luau::ScheduledFuture::after(Duration::ZERO, async move {
                    scheduler
                        .upgrade()
                        .expect("the scheduler is polling")
                        .cancel_group(group);
                    Ok(())
                }))
            })),
        )
        .function(FunctionSpec::new("cancel_group").call(|mut frame| {
            let group: f64 = frame.args.read()?;
            frame
                .vm
                .data()
                .get::<LocalScheduler>()?
                .cancel_group(TaskGroupId(group as u64));
            Ok(())
        }))
        .function(FunctionSpec::new("cancel").call(|mut frame| {
            let thread: luau::Thread = frame.args.read()?;
            frame
                .vm
                .data()
                .get::<LocalScheduler>()?
                .cancel_luau_thread(&thread);
            Ok(())
        }));
    let table = install_luau_module(&vm, &luau::ChunkOrigin::default(), &module)?;
    for name in [
        "wait",
        "pause",
        "cancel",
        "cancel_group",
        "cancel_as_it_wakes",
        "cancel_group_as_it_wakes",
    ] {
        let luau::Value::Function(function) = table.get(name)? else {
            panic!("the module has no {name}");
        };
        vm.set_global_function(name, &function)?;
    }
    let scheduler = vm.data().get::<LocalScheduler>()?;
    Ok((vm, scheduler))
}

fn spawn_chunk(
    vm: &luau::Vm,
    scheduler: &LocalScheduler,
    context: CallContext,
    source: &'static str,
) -> luau::runtime::Result<luau::Thread> {
    let thread = chunk_thread(vm, source)?;
    scheduler.spawn_luau_thread(context, vm.clone(), thread.clone(), Vec::new());
    Ok(thread)
}

/// Whether a chunk that sets `ran` has run in `vm`.
fn ran(vm: &luau::Vm) -> luau::runtime::Result<bool> {
    Ok(eval(vm, "return ran")? == [luau::Value::Boolean(true)])
}

fn eval(vm: &luau::Vm, source: &'static str) -> luau::runtime::Result<Vec<luau::Value>> {
    vm.eval(
        Arc::<[u8]>::from(source.as_bytes()),
        luau::ChunkOrigin::default(),
    )
}

/// What Luau code holding `thread` makes of it: its status, and what resuming it returns.
fn status_and_resume(
    vm: &luau::Vm,
    thread: &luau::Thread,
) -> luau::runtime::Result<Vec<luau::Value>> {
    vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(
            &b"local co = ...; return coroutine.status(co), coroutine.resume(co)"[..],
        ),
        luau::ChunkOrigin::default(),
    ))?
    .call(&[luau::Value::Thread(thread.clone())])
}

#[track_caller]
fn assert_closed(vm: &luau::Vm, thread: &luau::Thread) {
    assert_eq!(
        status_and_resume(vm, thread).expect("Luau reads the thread"),
        vec![
            luau::Value::String(b"dead".to_vec()),
            luau::Value::Boolean(false),
            luau::Value::String(b"cannot resume dead coroutine".to_vec()),
        ]
    );
    assert_eq!(
        eval(vm, "return went_on").expect("Luau reads the global"),
        vec![luau::Value::Nil],
        "a cancelled thread went on"
    );
}

#[test]
fn a_cancelled_thread_is_closed_wherever_it_waited() -> luau::runtime::Result<()> {
    let (vm, scheduler) = vm_with_waits()?;
    let spawn = |source| spawn_chunk(&vm, &scheduler, CallContext::default(), source);
    let yielded = spawn("coroutine.yield() went_on = true")?;
    let waiting = spawn("wait() went_on = true")?;
    scheduler.poll_ready();
    let unstarted = spawn("went_on = true")?;
    let delayed = chunk_thread(&vm, "went_on = true")?;
    scheduler.spawn_luau_thread_after(
        CallContext::default(),
        Duration::ZERO,
        vm.clone(),
        delayed.clone(),
        Vec::new(),
    );

    for thread in [&yielded, &waiting, &unstarted, &delayed] {
        assert!(scheduler.cancel_luau_thread(thread));
        assert_closed(&vm, thread);
    }
    scheduler.poll_ready();
    assert_closed(&vm, &delayed);
    Ok(())
}

#[test]
fn a_thread_that_ended_or_was_never_scheduled_is_not_closed() -> luau::runtime::Result<()> {
    let (vm, scheduler) = vm_with_waits()?;
    let unscheduled = chunk_thread(&vm, "return 42")?;
    let Some(luau::Value::Thread(handed_back)) = eval(
        &vm,
        "return coroutine.create(function() coroutine.yield() return 42 end)",
    )?
    .pop() else {
        panic!("expected a thread");
    };
    // Its task ends as it yields, which hands the coroutine back to the code that made it.
    scheduler.schedule_luau_future(
        CallContext::default(),
        vm.clone(),
        handed_back.clone(),
        luau::ScheduledFuture::new(async { Ok(()) }),
    );
    scheduler.poll_ready();

    for thread in [&unscheduled, &handed_back] {
        scheduler.cancel_luau_thread(thread);
        assert_eq!(
            thread.resume(&[])?,
            luau::ThreadStatus::Completed(vec![luau::Value::Number(42.0)])
        );
    }
    Ok(())
}

#[test]
fn cancelling_a_group_closes_its_threads_and_no_others() -> luau::runtime::Result<()> {
    let (vm, scheduler) = vm_with_waits()?;
    let group = scheduler.create_group();
    let in_group = || CallContext {
        task_group: group,
        ..CallContext::default()
    };
    let yielded = spawn_chunk(
        &vm,
        &scheduler,
        in_group(),
        "coroutine.yield() went_on = true",
    )?;
    let waiting = spawn_chunk(&vm, &scheduler, in_group(), "wait() went_on = true")?;
    let other = spawn_chunk(
        &vm,
        &scheduler,
        CallContext::default(),
        "coroutine.yield() return 42",
    )?;
    scheduler.poll_ready();

    scheduler.cancel_group(group);

    assert_closed(&vm, &yielded);
    assert_closed(&vm, &waiting);
    let late = spawn_chunk(&vm, &scheduler, in_group(), "went_on = true")?;
    assert_closed(&vm, &late);
    assert_eq!(
        other.resume(&[])?,
        luau::ThreadStatus::Completed(vec![luau::Value::Number(42.0)])
    );
    Ok(())
}

/// The watcher cancels one thread that waits and one that is ready in the same round.
#[test]
fn a_thread_a_watcher_cancels_during_a_poll_is_closed_before_the_poll_ends()
-> luau::runtime::Result<()> {
    let (vm, scheduler) = vm_with_waits()?;
    let waiting = spawn_chunk(
        &vm,
        &scheduler,
        CallContext::default(),
        "wait() went_on = true",
    )?;
    scheduler.poll_ready();
    let ready = spawn_chunk(&vm, &scheduler, CallContext::default(), "went_on = true")?;
    scheduler.schedule_watcher(CallContext::default(), {
        let scheduler = Rc::downgrade(&scheduler);
        let (waiting, ready) = (waiting.clone(), ready.clone());
        async move {
            let scheduler = scheduler.upgrade().expect("the scheduler is polling");
            scheduler.cancel_luau_thread(&waiting);
            scheduler.cancel_luau_thread(&ready);
            Ok(())
        }
    });

    scheduler.poll_ready();

    assert_closed(&vm, &waiting);
    assert_closed(&vm, &ready);
    Ok(())
}

/// The task after them holds both threads and could resume them in the same round: each is
/// closed as it yields, not when the poll ends, and leaves nothing behind.
#[test]
fn a_thread_that_cancels_itself_runs_to_its_next_yield_and_is_closed() -> luau::runtime::Result<()>
{
    let (vm, scheduler) = vm_with_waits()?;
    let spawn = |source| spawn_chunk(&vm, &scheduler, CallContext::default(), source);
    let yielding = spawn(
        r#"
            yielding = coroutine.running()
            cancel(yielding)
            reached = 1
            coroutine.yield()
            went_on = true
        "#,
    )?;
    let waiting = spawn(
        r#"
            waiting = coroutine.running()
            cancel(waiting)
            reached += 1
            wait()
            went_on = true
        "#,
    )?;
    spawn(
        r#"
            statuses = coroutine.status(yielding) .. " " .. coroutine.status(waiting)
            resumed = coroutine.resume(yielding) or coroutine.resume(waiting)
        "#,
    )?;
    let tasks = [&yielding, &waiting]
        .map(|thread| scheduler.luau_thread_handle(thread).expect("its task").id());

    scheduler.poll_ready();

    assert_eq!(
        eval(&vm, "return reached, statuses, resumed")?,
        vec![
            luau::Value::Number(2.0),
            luau::Value::String(b"dead dead".to_vec()),
            luau::Value::Boolean(false),
        ]
    );
    assert_closed(&vm, &yielding);
    assert_closed(&vm, &waiting);
    assert!(holds_nothing(&scheduler));
    for task in tasks {
        assert_eq!(
            scheduler.snapshot(task).map(|task| task.state),
            Some(TaskState::Cancelled)
        );
    }
    Ok(())
}

/// Cancelled by `Thread::cancel` alone, which the scheduler doesn't hear of: the thread's
/// task still ends, as cancelled, once the thread is closed where it yields.
#[test]
fn a_thread_closed_where_it_yields_ends_its_task() -> luau::runtime::Result<()> {
    let (vm, scheduler) = vm_with_waits()?;
    let cancel_thread: luau::NativeFn = Arc::new(|frame| {
        frame.thread.cancel()?;
        Ok(())
    });
    let cancel_thread = vm.create_function_with_options(
        luau::NativeFunctionOptions::new(luau::ChunkOrigin::default())
            .function_name("cancel_thread"),
        cancel_thread,
    )?;
    vm.set_global_function("cancel_thread", &cancel_thread)?;
    let completion = Rc::new(LocalLuauTaskCompletion::default());
    let thread = chunk_thread(&vm, "cancel_thread() coroutine.yield() went_on = true")?;
    scheduler.schedule_luau_thread_with_completion(
        CallContext::default(),
        vm.clone(),
        thread.clone(),
        Vec::new(),
        completion.clone(),
    );

    scheduler.poll_ready();

    let ended = completion_message(&completion).expect("the thread's task ended");
    assert!(ended.is_err_and(|error| error.contains("cancelled")));
    assert_closed(&vm, &thread);
    assert!(holds_nothing(&scheduler));
    Ok(())
}

/// The task borrows the coroutine for the one wait: cancelling it leaves the coroutine with
/// nothing to resume it, so it is closed like a thread the scheduler runs.
#[test]
fn a_coroutine_cancelled_in_a_host_wait_is_closed() -> luau::runtime::Result<()> {
    let (vm, scheduler) = vm_with_waits()?;
    let group = scheduler.create_group();
    spawn_chunk(
        &vm,
        &scheduler,
        CallContext {
            task_group: group,
            ..CallContext::default()
        },
        r#"
            by_thread = coroutine.create(function() wait() went_on = true end)
            by_group = coroutine.create(function() wait() went_on = true end)
            coroutine.resume(by_thread)
            coroutine.resume(by_group)
        "#,
    )?;
    scheduler.poll_ready();
    let coroutine = |name| -> luau::runtime::Result<luau::Thread> {
        match eval(&vm, name)?.pop() {
            Some(luau::Value::Thread(thread)) => Ok(thread),
            other => panic!("expected a thread, got {other:?}"),
        }
    };
    let (by_thread, by_group) = (
        coroutine("return by_thread")?,
        coroutine("return by_group")?,
    );
    assert_eq!(
        eval(
            &vm,
            "return coroutine.status(by_thread), coroutine.status(by_group)"
        )?,
        vec![
            luau::Value::String(b"suspended".to_vec()),
            luau::Value::String(b"suspended".to_vec()),
        ]
    );

    assert!(scheduler.cancel_luau_thread(&by_thread));
    assert_closed(&vm, &by_thread);
    scheduler.cancel_group(group);
    assert_closed(&vm, &by_group);
    Ok(())
}

/// Schedules `source` under `limit` and returns its thread and what its task ends with.
fn spawn_limited(
    vm: &luau::Vm,
    scheduler: &LocalScheduler,
    source: &'static str,
    limit: ResumeLimit,
) -> luau::runtime::Result<(luau::Thread, Rc<LocalLuauTaskCompletion>)> {
    let thread = chunk_thread(vm, source)?;
    let completion = Rc::new(LocalLuauTaskCompletion::default());
    scheduler.schedule_luau_thread_with_limit_and_completion(
        CallContext::default(),
        vm.clone(),
        thread.clone(),
        Vec::new(),
        limit,
        completion.clone(),
    );
    Ok((thread, completion))
}

#[track_caller]
fn assert_interrupted(completion: &LocalLuauTaskCompletion) {
    let ended = completion_message(completion).expect("the task ended");
    assert!(
        ended
            .as_ref()
            .is_err_and(|error| error.contains(luau::INTERRUPTED_ERROR)),
        "{ended:?}"
    );
}

/// Luau looks for an interrupt only at calls, loops and returns, so a resume given no time
/// would still run what comes before the first of them. A deadline is the first instant that
/// is too late: a thread is resumed just before it and not at it.
#[test]
fn a_thread_is_not_started_once_its_limit_s_deadline_is_reached() -> luau::runtime::Result<()> {
    const TIMEOUT: Duration = Duration::from_secs(30);

    for (late_by, started) in [
        (None, luau::Value::Boolean(true)),
        (Some(Duration::ZERO), luau::Value::Nil),
        (Some(TIMEOUT), luau::Value::Nil),
    ] {
        let clock = ManualClock::new();
        let (vm, scheduler) = vm_with_waits_on(LocalScheduler::with_manual_clock(clock.clone()))?;
        let (thread, completion) = spawn_limited(
            &vm,
            &scheduler,
            "started = true",
            ResumeLimit::until(clock.now() + TIMEOUT),
        )?;
        match late_by {
            None => clock.advance(TIMEOUT - Duration::from_nanos(1)),
            Some(late_by) => clock.advance(TIMEOUT + late_by),
        }

        scheduler.poll_ready();

        assert_eq!(eval(&vm, "return started")?, vec![started], "{late_by:?}");
        if late_by.is_some() {
            assert_interrupted(&completion);
            assert_closed(&vm, &thread);
        }
    }
    Ok(())
}

#[test]
fn a_thread_is_not_resumed_from_a_wait_that_outlasted_its_limit_s_deadline()
-> luau::runtime::Result<()> {
    const TIMEOUT: Duration = Duration::from_secs(30);

    let clock = ManualClock::new();
    let (vm, scheduler) = vm_with_waits_on(LocalScheduler::with_manual_clock(clock.clone()))?;
    let (thread, completion) = spawn_limited(
        &vm,
        &scheduler,
        "started = true pause() went_on = true",
        ResumeLimit::until(clock.now() + TIMEOUT),
    )?;
    scheduler.poll_ready();
    assert_eq!(
        eval(&vm, "return started")?,
        vec![luau::Value::Boolean(true)]
    );
    assert!(completion_message(&completion).is_none());

    clock.advance(TIMEOUT);
    scheduler.poll_ready();

    assert_interrupted(&completion);
    assert_closed(&vm, &thread);
    Ok(())
}

/// A budget of no time is no resume, not one that ends at its first call, loop or return.
#[test]
fn a_thread_is_not_started_on_a_limit_that_gives_it_no_time() -> luau::runtime::Result<()> {
    let far = Instant::now() + Duration::from_secs(600);
    for limit in [
        ResumeLimit::budget(Duration::ZERO),
        ResumeLimit::until(far).with_budget(Duration::ZERO),
    ] {
        let (vm, scheduler) = vm_with_waits()?;
        let (thread, completion) = spawn_limited(&vm, &scheduler, "went_on = true", limit)?;

        scheduler.poll_ready();

        assert_interrupted(&completion);
        assert_eq!(completion.interrupted_by(), Some(ResumeBound::Budget));
        assert_closed(&vm, &thread);
    }
    Ok(())
}

#[test]
fn a_thread_is_not_started_once_its_limit_s_signal_is_raised() -> luau::runtime::Result<()> {
    let (vm, scheduler) = vm_with_waits()?;
    let signal = luau::InterruptSignal::new();
    let (thread, completion) = spawn_limited(
        &vm,
        &scheduler,
        "went_on = true",
        ResumeLimit::budget(Duration::from_secs(600)).or_on(signal.clone()),
    )?;
    signal.raise();

    scheduler.poll_ready();

    assert_interrupted(&completion);
    assert_closed(&vm, &thread);
    Ok(())
}

/// The coroutine's wait is a task of its own, in its group and not under the thread that
/// started it.
#[test]
fn cancelling_a_thread_leaves_a_coroutine_it_started_waiting_on_a_call_of_its_own()
-> luau::runtime::Result<()> {
    let (vm, scheduler) = vm_with_waits()?;
    let thread = spawn_chunk(
        &vm,
        &scheduler,
        CallContext::default(),
        r#"
            local started = coroutine.create(function() pause() outlived = true end)
            coroutine.resume(started)
            wait()
            went_on = true
        "#,
    )?;
    scheduler.poll_ready();

    assert!(scheduler.cancel_luau_thread(&thread));
    scheduler.poll_ready();

    assert_closed(&vm, &thread);
    assert_eq!(
        eval(&vm, "return outlived")?,
        vec![luau::Value::Boolean(true)]
    );
    Ok(())
}

/// The thread parks on a call in the round its group is cancelled, after it ran: the call's
/// future never becomes its work.
#[test]
fn a_thread_whose_wait_is_refused_for_a_cancelled_group_is_closed() -> luau::runtime::Result<()> {
    let (vm, scheduler) = vm_with_waits()?;
    let group = scheduler.create_group();
    let thread = spawn_chunk(
        &vm,
        &scheduler,
        CallContext {
            task_group: group,
            ..CallContext::default()
        },
        "wait() went_on = true",
    )?;
    scheduler.schedule_future(CallContext::default(), {
        let scheduler = Rc::downgrade(&scheduler);
        async move {
            scheduler
                .upgrade()
                .expect("the scheduler is polling")
                .cancel_group(group);
            Ok(())
        }
    });

    scheduler.poll_ready();

    assert_closed(&vm, &thread);
    assert!(!scheduler.has_pending());
    Ok(())
}

/// Work a poll put off and never took up, as when it unwound, is cancelled where it lies.
#[test]
fn cancelling_work_the_scheduler_has_not_taken_up_closes_its_thread() -> luau::runtime::Result<()> {
    let (vm, scheduler) = vm_with_waits()?;
    let unstarted = chunk_thread(&vm, "went_on = true")?;
    let suspended = chunk_thread(&vm, "coroutine.yield() went_on = true")?;
    suspended.resume(&[])?;
    let dropped = Rc::new(Cell::new(false));
    {
        let _polling = Polling::begin(&scheduler.polling);
        scheduler.schedule_luau_thread(
            CallContext::default(),
            vm.clone(),
            unstarted.clone(),
            Vec::new(),
        );
        let held = OnDrop({
            let (suspended, dropped) = (suspended.clone(), dropped.clone());
            move || {
                assert!(
                    suspended.resume(&[]).is_err(),
                    "closed after its work dropped"
                );
                dropped.set(true);
            }
        });
        scheduler.schedule_luau_future(
            CallContext::default(),
            vm.clone(),
            suspended.clone(),
            luau::ScheduledFuture::new(async move {
                let _held = held;
                pending::<luau::runtime::Result<()>>().await
            }),
        );
    }

    assert!(scheduler.cancel_luau_thread(&unstarted));
    assert!(scheduler.cancel_luau_thread(&suspended));

    assert!(dropped.get());
    assert_closed(&vm, &unstarted);
    assert_closed(&vm, &suspended);
    scheduler.poll_ready();
    assert!(scheduler.snapshots().is_empty());
    Ok(())
}

/// Whether the scheduler holds nothing for any thread: no pending task, no wait parked or put
/// off.
fn holds_nothing(scheduler: &LocalScheduler) -> bool {
    !scheduler.has_pending()
        && scheduler.parked_threads.borrow().is_empty()
        && scheduler.pending_luau_work.borrow().is_empty()
}

/// Luau code resumed the coroutine itself while it was parked in a host wait, and the
/// coroutine is cancelled inside that resume, outside any poll. It can't be closed while it
/// runs, and its next yield would hand it back to Luau code and not to the scheduler: it runs
/// on to that yield and is closed there, with nothing scheduled for a wait it asked for.
#[test]
fn a_thread_cancelled_while_luau_code_runs_it_is_closed_where_it_yields()
-> luau::runtime::Result<()> {
    for yields in [
        "coroutine.yield('lost')",
        "wait()",
        "pause()",
        "pcall(coroutine.yield)",
        "pcall(wait)",
        "pcall(pause)",
        "while true do pcall(wait) end",
    ] {
        let (vm, scheduler) = vm_with_waits()?;
        let source = format!(
            r#"
                co = coroutine.create(function()
                    wait()
                    cancel(coroutine.running())
                    reached = true
                    {yields}
                    went_on = true
                end)
                coroutine.resume(co)
            "#
        );
        vm.eval(
            Arc::<[u8]>::from(source.as_bytes()),
            luau::ChunkOrigin::default(),
        )?;
        assert!(scheduler.has_pending());

        let resumed = eval(
            &vm,
            "local resumed = table.pack(coroutine.resume(co)) return resumed.n, resumed[1], reached",
        )?;

        assert_eq!(
            resumed,
            [
                luau::Value::Number(1.0),
                luau::Value::Boolean(true),
                luau::Value::Boolean(true)
            ],
            "{yields}"
        );
        let Some(luau::Value::Thread(co)) = eval(&vm, "return co")?.pop() else {
            panic!("expected a thread");
        };
        assert_closed(&vm, &co);
        assert!(holds_nothing(&scheduler), "{yields}");
        scheduler.poll_ready();
        assert!(
            scheduler
                .snapshots()
                .iter()
                .all(|task| task.state == TaskState::Cancelled),
            "{yields}: the wait left work behind"
        );
    }
    Ok(())
}

/// As above inside a poll, from the thread the scheduler resumes: it resumes the parked
/// coroutine, which is cancelled there and would yield back to that thread, which goes on.
#[test]
fn a_thread_cancelled_while_another_task_s_code_runs_it_is_closed_where_it_yields()
-> luau::runtime::Result<()> {
    for yields in ["coroutine.yield('lost')", "while true do pcall(wait) end"] {
        let (vm, scheduler) = vm_with_waits()?;
        let completion = Rc::new(LocalLuauTaskCompletion::default());
        let source = format!(
            r#"
                co = coroutine.create(function()
                    wait()
                    cancel(coroutine.running())
                    {yields}
                    went_on = true
                end)
                coroutine.resume(co)
                pause()
                resumed = table.pack(coroutine.resume(co))
                again = coroutine.resume(co)
                resumer_went_on = true
            "#
        );
        scheduler.schedule_luau_thread_with_completion(
            CallContext::default(),
            vm.clone(),
            vm.create_thread(&vm.load_chunk(&luau::Chunk::new(
                Arc::<[u8]>::from(source.as_bytes()),
                luau::ChunkOrigin::default(),
            ))?)?,
            Vec::new(),
            completion.clone(),
        );

        scheduler.poll_ready();
        scheduler.poll_ready();

        assert_eq!(completion_message(&completion), Some(Ok(())), "{yields}");
        assert_eq!(
            eval(
                &vm,
                "return resumed.n, resumed[1], again, coroutine.status(co), went_on, resumer_went_on"
            )?,
            vec![
                luau::Value::Number(1.0),
                luau::Value::Boolean(true),
                luau::Value::Boolean(false),
                luau::Value::String(b"dead".to_vec()),
                luau::Value::Nil,
                luau::Value::Boolean(true),
            ],
            "{yields}"
        );
        assert!(holds_nothing(&scheduler), "{yields}");
    }
    Ok(())
}

/// The scheduler resumes the coroutine it borrowed for a wait, which cancels itself and
/// yields: it is not handed back to the code that made it.
#[test]
fn a_borrowed_coroutine_that_cancels_itself_is_closed_as_it_yields() -> luau::runtime::Result<()> {
    let (vm, scheduler) = vm_with_waits()?;
    eval(
        &vm,
        r#"
            co = coroutine.create(function()
                pause()
                cancel(coroutine.running())
                reached = true
                coroutine.yield()
                went_on = true
            end)
            coroutine.resume(co)
        "#,
    )?;

    scheduler.poll_ready();

    assert_eq!(
        eval(&vm, "return reached")?,
        vec![luau::Value::Boolean(true)]
    );
    let Some(luau::Value::Thread(co)) = eval(&vm, "return co")?.pop() else {
        panic!("expected a thread");
    };
    assert_closed(&vm, &co);
    let cancelled = scheduler
        .snapshots()
        .iter()
        .filter(|task| task.state == TaskState::Cancelled)
        .count();
    assert_eq!(cancelled, 1);
    Ok(())
}

/// The canceller holds the thread and could resume it in the same breath: the thread is
/// closed as it is cancelled, not when the poll ends.
#[test]
fn a_thread_cancelled_from_another_task_is_closed_before_that_task_goes_on()
-> luau::runtime::Result<()> {
    let (vm, scheduler) = vm_with_waits()?;
    let waiting = spawn_chunk(
        &vm,
        &scheduler,
        CallContext::default(),
        "held = coroutine.running() wait() went_on = true",
    )?;
    scheduler.poll_ready();
    spawn_chunk(
        &vm,
        &scheduler,
        CallContext::default(),
        "cancel(held) status = coroutine.status(held) resumed = coroutine.resume(held)",
    )?;

    scheduler.poll_ready();

    assert_eq!(
        eval(&vm, "return status, resumed")?,
        vec![
            luau::Value::String(b"dead".to_vec()),
            luau::Value::Boolean(false),
        ]
    );
    assert_closed(&vm, &waiting);
    assert!(!scheduler.has_pending());
    Ok(())
}

/// The wait the thread is parked in cancels it, or its group, as it completes: the poll that
/// finds the wait over must not go on to resume the thread.
#[test]
fn a_thread_cancelled_by_the_wait_that_wakes_it_is_not_resumed() -> luau::runtime::Result<()> {
    for wait in [
        "cancel_as_it_wakes() went_on = true",
        "cancel_group_as_it_wakes() went_on = true",
    ] {
        let (vm, scheduler) = vm_with_waits()?;
        let context = CallContext {
            task_group: scheduler.create_group(),
            ..CallContext::default()
        };
        let thread = spawn_chunk(&vm, &scheduler, context, wait)?;
        let task = scheduler.luau_thread_handle(&thread).expect("its task");

        scheduler.poll_ready();
        scheduler.poll_ready();

        assert_closed(&vm, &thread);
        assert_eq!(
            scheduler.snapshot(task.id()).map(|task| task.state),
            Some(TaskState::Cancelled),
            "{wait}"
        );
    }
    Ok(())
}

/// Sets the global `group` to `group`'s id, for `cancel_group`.
fn set_group_global(vm: &luau::Vm, group: TaskGroupId) -> luau::runtime::Result<()> {
    vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(&b"group = ..."[..]),
        luau::ChunkOrigin::default(),
    ))?
    .call(&[luau::Value::Number(group.0 as f64)])?;
    Ok(())
}

/// The canceller holds a thread of the group and could resume it in the same breath: the
/// group's threads are closed as it is cancelled, not when the poll ends.
#[test]
fn a_group_cancelled_from_a_task_closes_its_threads_before_that_task_goes_on()
-> luau::runtime::Result<()> {
    let (vm, scheduler) = vm_with_waits()?;
    let group = scheduler.create_group();
    set_group_global(&vm, group)?;
    let in_group = || CallContext {
        task_group: group,
        ..CallContext::default()
    };
    let waiting = spawn_chunk(
        &vm,
        &scheduler,
        in_group(),
        "held = coroutine.running() wait() went_on = true",
    )?;
    let yielded = spawn_chunk(
        &vm,
        &scheduler,
        in_group(),
        "also_held = coroutine.running() coroutine.yield() went_on = true",
    )?;
    scheduler.poll_ready();
    spawn_chunk(
        &vm,
        &scheduler,
        CallContext::default(),
        r#"
            cancel_group(group)
            statuses = coroutine.status(held) .. " " .. coroutine.status(also_held)
            resumed = coroutine.resume(held) or coroutine.resume(also_held)
        "#,
    )?;

    scheduler.poll_ready();

    assert_eq!(
        eval(&vm, "return statuses, resumed")?,
        vec![
            luau::Value::String(b"dead dead".to_vec()),
            luau::Value::Boolean(false),
        ]
    );
    assert_closed(&vm, &waiting);
    assert_closed(&vm, &yielded);
    assert!(!scheduler.has_pending());
    Ok(())
}

/// The thread that cancels its own group is the one the scheduler is resuming: it goes on to
/// where it yields, or calls what would wait, as one that cancels itself does. The task after
/// it holds the thread and could resume it in the same round: it is closed by then.
#[test]
fn a_thread_that_cancels_its_group_runs_to_its_next_yield_and_is_closed()
-> luau::runtime::Result<()> {
    for yields in [
        "coroutine.yield()",
        "wait()",
        "pcall(wait)",
        "while true do pcall(wait) end",
    ] {
        let (vm, scheduler) = vm_with_waits()?;
        let group = scheduler.create_group();
        set_group_global(&vm, group)?;
        let source = format!(
            r#"
                held = coroutine.running()
                cancel_group(group)
                reached = true
                {yields}
                went_on = true
            "#
        );
        let thread = vm.create_thread(&vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(source.as_bytes()),
            luau::ChunkOrigin::default(),
        ))?)?;
        let completion = Rc::new(LocalLuauTaskCompletion::default());
        scheduler.schedule_luau_thread_with_completion(
            CallContext {
                task_group: group,
                ..CallContext::default()
            },
            vm.clone(),
            thread.clone(),
            Vec::new(),
            completion.clone(),
        );
        spawn_chunk(
            &vm,
            &scheduler,
            CallContext::default(),
            "status = coroutine.status(held) resumed = coroutine.resume(held)",
        )?;

        scheduler.poll_ready();

        assert_eq!(
            eval(&vm, "return reached, status, resumed")?,
            vec![
                luau::Value::Boolean(true),
                luau::Value::String(b"dead".to_vec()),
                luau::Value::Boolean(false),
            ],
            "{yields}"
        );
        let ended = completion_message(&completion).expect("the thread's task ended");
        assert!(
            ended.is_err_and(|error| error.contains("cancelled")),
            "{yields}"
        );
        assert_closed(&vm, &thread);
        assert!(holds_nothing(&scheduler), "{yields}");
    }
    Ok(())
}

/// The cancelled thread is cancelled by a coroutine it resumed. That coroutine, and what
/// else runs beneath the cancelled thread, is not cancelled with it: it yields back, waits on
/// a host call of its own and is woken from it.
#[test]
fn the_coroutines_a_cancelled_thread_resumed_go_on() -> luau::runtime::Result<()> {
    let (vm, scheduler) = vm_with_waits()?;
    eval(
        &vm,
        r#"
            co = coroutine.create(function()
                wait()
                worker = coroutine.create(function()
                    cancel(co)
                    coroutine.yield("yielded")
                    pause()
                    worker_went_on = true
                end)
                local _, yielded = coroutine.resume(worker)
                reached = yielded
                coroutine.resume(worker)
                wait()
                went_on = true
            end)
            coroutine.resume(co)
        "#,
    )?;

    let resumed = eval(
        &vm,
        "return coroutine.resume(co), reached, coroutine.status(worker)",
    )?;

    assert_eq!(resumed[0], luau::Value::Boolean(true), "{resumed:?}");
    assert_eq!(
        resumed[1..],
        [
            luau::Value::String(b"yielded".to_vec()),
            luau::Value::String(b"suspended".to_vec()),
        ]
    );
    let Some(luau::Value::Thread(co)) = eval(&vm, "return co")?.pop() else {
        panic!("expected a thread");
    };
    assert_closed(&vm, &co);
    assert!(scheduler.has_pending(), "the worker's wait went with it");

    scheduler.poll_ready();

    assert_eq!(
        eval(&vm, "return worker_went_on, coroutine.status(worker)")?,
        vec![
            luau::Value::Boolean(true),
            luau::Value::String(b"dead".to_vec())
        ]
    );
    Ok(())
}

/// A poll that unwinds leaves a thread parked with its wait not yet taken up. Cancelling the
/// thread closes it and drops the wait, which would otherwise start it again as a new task.
#[test]
fn a_thread_left_parked_by_a_poll_that_unwound_is_closed_with_its_wait() -> luau::runtime::Result<()>
{
    struct PanickingWake;
    impl std::task::Wake for PanickingWake {
        fn wake(self: Arc<Self>) {
            panic!("the waker panics");
        }
    }

    let (vm, scheduler) = vm_with_waits()?;
    let parked = spawn_chunk(
        &vm,
        &scheduler,
        CallContext::default(),
        "wait() went_on = true",
    )?;
    let completion = Rc::new(LocalLuauTaskCompletion::default());
    scheduler.schedule_luau_thread_with_completion(
        CallContext::default(),
        vm.clone(),
        chunk_thread(&vm, "return 42")?,
        Vec::new(),
        completion.clone(),
    );
    let waker = Waker::from(Arc::new(PanickingWake));
    assert!(
        completion
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let poll = catch_unwind(AssertUnwindSafe(|| scheduler.poll_ready()));
    assert!(poll.is_err(), "the poll didn't unwind");
    assert_eq!(scheduler.pending_luau_work.borrow().len(), 1);

    let task = scheduler.luau_thread_handle(&parked).expect("its task");

    assert!(scheduler.cancel_luau_thread(&parked));

    assert_closed(&vm, &parked);
    assert!(scheduler.pending_luau_work.borrow().is_empty());
    let tasks = scheduler.snapshots().len();
    scheduler.poll_ready();
    assert_eq!(scheduler.snapshots().len(), tasks, "its wait became a task");
    assert_eq!(
        scheduler.snapshot(task.id()).map(|task| task.state),
        Some(TaskState::Cancelled)
    );
    Ok(())
}

/// Runs a round of `scheduler` that unwinds after the tasks scheduled so far ran, as one does
/// when a completion's waker panics. What they put off is left for a later round.
fn unwind_a_round(vm: &luau::Vm, scheduler: &LocalScheduler) -> luau::runtime::Result<()> {
    struct PanickingWake;
    impl std::task::Wake for PanickingWake {
        fn wake(self: Arc<Self>) {
            panic!("the waker panics");
        }
    }

    let completion = Rc::new(LocalLuauTaskCompletion::default());
    scheduler.schedule_luau_thread_with_completion(
        CallContext::default(),
        vm.clone(),
        chunk_thread(vm, "return 42")?,
        Vec::new(),
        completion.clone(),
    );
    let waker = Waker::from(Arc::new(PanickingWake));
    assert!(
        completion
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let poll = catch_unwind(AssertUnwindSafe(|| scheduler.poll_ready()));
    assert!(poll.is_err(), "the poll didn't unwind");
    Ok(())
}

/// A waker that notes it was woken.
#[derive(Default)]
struct NotedWake(AtomicBool);

impl std::task::Wake for NotedWake {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::Release);
    }
}

/// Waits on `completion`, which is pending, and returns what notes its wake.
fn wait_on(completion: &LocalLuauTaskCompletion) -> Arc<NotedWake> {
    let noted = Arc::new(NotedWake::default());
    let waker = Waker::from(noted.clone());
    assert!(
        completion
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    noted
}

/// As above for a group: what the unwound poll put off for it, a parked thread's wait,
/// threads not yet started and a future, goes with it, and its threads are closed, at once.
/// A start with a completion tells it the thread was cancelled, as one cancelled by itself
/// does.
#[test]
fn cancelling_a_group_closes_and_drops_the_work_a_poll_that_unwound_put_off()
-> luau::runtime::Result<()> {
    let (vm, scheduler) = vm_with_waits()?;
    let group = scheduler.create_group();
    let in_group = CallContext {
        task_group: group,
        ..CallContext::default()
    };
    let parked = spawn_chunk(&vm, &scheduler, in_group.clone(), "wait() went_on = true")?;
    let [unstarted, completed, limited, alone] =
        ["went_on = true"; 4].map(|source| chunk_thread(&vm, source).expect("a thread"));
    let completions: [Rc<LocalLuauTaskCompletion>; 3] = Default::default();
    let other = chunk_thread(&vm, "return 42")?;
    let dropped = Rc::new(Cell::new(false));
    scheduler.schedule_future(CallContext::default(), {
        let scheduler = Rc::downgrade(&scheduler);
        let (vm, other) = (vm.clone(), other.clone());
        let threads = [&unstarted, &completed, &limited, &alone].map(Clone::clone);
        let completions = completions.clone();
        let dropped = OnDrop({
            let dropped = dropped.clone();
            move || dropped.set(true)
        });
        async move {
            let scheduler = scheduler.upgrade().expect("the scheduler is polling");
            let [unstarted, completed, limited, alone] = threads;
            let [to_completed, to_limited, to_alone] = completions;
            scheduler.schedule_luau_thread(in_group.clone(), vm.clone(), unstarted, Vec::new());
            scheduler.schedule_luau_thread_with_completion(
                in_group.clone(),
                vm.clone(),
                completed,
                Vec::new(),
                to_completed,
            );
            scheduler.schedule_luau_thread_with_limit_and_completion(
                in_group.clone(),
                vm.clone(),
                limited,
                Vec::new(),
                ResumeLimit::budget(Duration::from_secs(60)),
                to_limited,
            );
            scheduler.schedule_luau_thread_with_completion(
                CallContext::default(),
                vm.clone(),
                alone,
                Vec::new(),
                to_alone,
            );
            scheduler.schedule_luau_thread(CallContext::default(), vm, other, Vec::new());
            scheduler.schedule_future(in_group, async move {
                let _dropped = dropped;
                Ok(())
            });
            Ok(())
        }
    });
    unwind_a_round(&vm, &scheduler)?;
    assert_eq!(scheduler.pending_luau_work.borrow().len(), 6);
    assert_eq!(scheduler.pending_futures.borrow().len(), 1);
    let woken = completions.each_ref().map(|completion| wait_on(completion));

    assert!(scheduler.cancel_luau_thread(&alone));
    assert_closed(&vm, &alone);
    scheduler.cancel_group(group);

    for thread in [&parked, &unstarted, &completed, &limited] {
        assert_closed(&vm, thread);
    }
    for (completion, woken) in completions.iter().zip(&woken) {
        assert!(woken.0.load(Ordering::Acquire), "a waiter wasn't woken");
        let ended = completion_message(completion).expect("the start was cancelled");
        assert!(ended.is_err_and(|error| error.contains("cancelled")));
    }
    assert!(dropped.get(), "the group's future was kept");
    assert!(scheduler.pending_futures.borrow().is_empty());
    assert_eq!(scheduler.pending_luau_work.borrow().len(), 1);
    scheduler.poll_ready();
    assert_eq!(
        scheduler
            .luau_thread_handle(&other)
            .and_then(|task| scheduler.snapshot(task.id()))
            .map(|task| task.state),
        Some(TaskState::Completed),
        "another group's thread went with it"
    );
    Ok(())
}

/// Two of the waits the unwound poll put off for the group panic as they drop: each fails
/// alone, and the rest of the group's work is still closed and dropped.
#[test]
fn cancelling_a_group_outlives_put_off_work_that_panics_as_it_drops() -> luau::runtime::Result<()> {
    let (vm, scheduler) = vm_with_waits()?;
    let group = scheduler.create_group();
    let in_group = CallContext {
        task_group: group,
        ..CallContext::default()
    };
    let suspended = [(); 2].map(|()| {
        let Some(luau::Value::Thread(thread)) = eval(
            &vm,
            "local co = coroutine.create(coroutine.yield) coroutine.resume(co) return co",
        )
        .expect("a coroutine")
        .pop() else {
            panic!("expected a thread");
        };
        thread
    });
    let unstarted = chunk_thread(&vm, "went_on = true")?;
    let dropped = Rc::new(Cell::new(false));
    scheduler.schedule_future(CallContext::default(), {
        let scheduler = Rc::downgrade(&scheduler);
        let (vm, suspended, unstarted) = (vm.clone(), suspended.clone(), unstarted.clone());
        let dropped = OnDrop({
            let dropped = dropped.clone();
            move || dropped.set(true)
        });
        async move {
            let scheduler = scheduler.upgrade().expect("the scheduler is polling");
            for thread in suspended {
                let held = OnDrop(|| panic!("drop panicked"));
                scheduler.schedule_luau_future(
                    in_group.clone(),
                    vm.clone(),
                    thread,
                    luau::ScheduledFuture::new(async move {
                        let _held = held;
                        pending::<luau::runtime::Result<()>>().await
                    }),
                );
            }
            scheduler.schedule_luau_thread(in_group.clone(), vm, unstarted, Vec::new());
            scheduler.schedule_future(in_group, async move {
                let _dropped = dropped;
                Ok(())
            });
            Ok(())
        }
    });
    unwind_a_round(&vm, &scheduler)?;
    assert_eq!(scheduler.pending_luau_work.borrow().len(), 3);

    scheduler.cancel_group(group);

    for thread in suspended.iter().chain([&unstarted]) {
        assert_closed(&vm, thread);
    }
    assert!(scheduler.pending_luau_work.borrow().is_empty());
    assert!(dropped.get(), "the group's future was kept");
    Ok(())
}

#[test]
fn a_poll_that_unwinds_leaves_the_scheduler_taking_work_at_once() -> luau::runtime::Result<()> {
    struct PanickingWake;
    impl std::task::Wake for PanickingWake {
        fn wake(self: Arc<Self>) {
            panic!("the waker panics");
        }
    }

    let vm = luau::Vm::new()?;
    let function = vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(&b"return 42"[..]),
        luau::ChunkOrigin::default(),
    ))?;
    let thread = vm.create_thread(&function)?;
    let scheduler = LocalScheduler::new();
    let completion = Rc::new(LocalLuauTaskCompletion::default());
    scheduler.schedule_luau_thread_with_completion(
        CallContext::default(),
        vm,
        thread,
        Vec::new(),
        completion.clone(),
    );
    let waker = Waker::from(Arc::new(PanickingWake));
    assert!(
        completion
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let poll = catch_unwind(AssertUnwindSafe(|| scheduler.poll_ready()));
    assert!(poll.is_err(), "the poll didn't unwind");

    scheduler.schedule_future(CallContext::default(), async { Ok(()) });
    assert_eq!(
        scheduler.snapshots().len(),
        2,
        "the scheduler put the task off as if it were still polling"
    );
    Ok(())
}

#[test]
#[should_panic(expected = "a task's result was already read")]
fn a_completion_read_twice_panics() {
    let vm = luau::Vm::new().expect("vm");
    let function = vm
        .load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"return 42"[..]),
            luau::ChunkOrigin::default(),
        ))
        .expect("chunk");
    let thread = vm.create_thread(&function).expect("thread");
    let scheduler = LocalScheduler::new();
    let completion = Rc::new(LocalLuauTaskCompletion::default());
    scheduler.schedule_luau_thread_with_completion(
        CallContext::default(),
        vm,
        thread,
        Vec::new(),
        completion.clone(),
    );
    scheduler.poll_ready();

    let mut cx = Context::from_waker(Waker::noop());
    assert!(completion.poll(&mut cx).is_ready());
    let _ = completion.poll(&mut cx);
}

struct OnDrop<F: FnMut()>(F);

impl<F: FnMut()> Drop for OnDrop<F> {
    fn drop(&mut self) {
        (self.0)();
    }
}

/// A thread that waits forever on a host call, whose future holds `held`.
fn thread_holding(
    vm: &luau::Vm,
    scheduler: &LocalScheduler,
    held: impl Sized + 'static,
) -> luau::runtime::Result<luau::Thread> {
    let thread = vm.create_thread(&vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(&b"coroutine.yield()"[..]),
        luau::ChunkOrigin::default(),
    ))?)?;
    scheduler.spawn_luau_thread(
        CallContext::default(),
        vm.clone(),
        thread.clone(),
        Vec::new(),
    );
    scheduler.poll_ready();
    scheduler.schedule_luau_future(
        CallContext::default(),
        vm.clone(),
        thread.clone(),
        luau::ScheduledFuture::new(async move {
            let _held = held;
            pending::<luau::runtime::Result<()>>().await
        }),
    );
    Ok(thread)
}

#[test]
fn cancelled_work_may_call_the_scheduler_as_it_drops() -> luau::runtime::Result<()> {
    let vm = luau::Vm::new()?;
    let scheduler = Rc::new(LocalScheduler::new());
    let other = vm.create_thread(&vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(&b"ran = true"[..]),
        luau::ChunkOrigin::default(),
    ))?)?;
    let group = scheduler.create_group();
    let spawned = Rc::new(Cell::new(false));
    let thread = thread_holding(
        &vm,
        &scheduler,
        OnDrop({
            let scheduler = Rc::downgrade(&scheduler);
            let (other, spawned) = (other.clone(), spawned.clone());
            move || {
                let scheduler = scheduler.upgrade().expect("the scheduler is alive");
                scheduler.cancel_luau_thread(&other);
                scheduler.cancel_group(group);
                let spawned = spawned.clone();
                scheduler.schedule_future(CallContext::default(), async move {
                    spawned.set(true);
                    Ok(())
                });
            }
        }),
    )?;
    scheduler.poll_ready();
    scheduler.spawn_luau_thread(
        CallContext::default(),
        vm.clone(),
        other.clone(),
        Vec::new(),
    );
    scheduler.schedule_watcher(CallContext::default(), {
        let scheduler = Rc::downgrade(&scheduler);
        async move {
            scheduler
                .upgrade()
                .expect("the scheduler is polling")
                .cancel_luau_thread(&thread);
            Ok(())
        }
    });

    scheduler.poll_ready();
    assert!(!ran(&vm)?, "the thread its drop cancelled ran");
    assert!(scheduler.is_group_cancelled(group));
    assert!(spawned.get(), "the task its drop spawned didn't run");
    Ok(())
}

#[test]
fn cancelled_work_panicking_as_it_drops_fails_nothing_else() -> luau::runtime::Result<()> {
    let vm = luau::Vm::new()?;
    let scheduler = Rc::new(LocalScheduler::new());
    let panicking = thread_holding(&vm, &scheduler, OnDrop(|| panic!("drop panicked")))?;
    let dropped = Rc::new(Cell::new(false));
    let quiet = thread_holding(
        &vm,
        &scheduler,
        OnDrop({
            let dropped = dropped.clone();
            move || dropped.set(true)
        }),
    )?;
    scheduler.poll_ready();
    let settled = Rc::new(Cell::new(false));
    scheduler.schedule_watcher(CallContext::default(), {
        let scheduler = Rc::downgrade(&scheduler);
        let settled = settled.clone();
        async move {
            let scheduler = scheduler.upgrade().expect("the scheduler is polling");
            let gone = scheduler.cancel_luau_thread_and_settle(&panicking);
            scheduler.cancel_luau_thread(&quiet);
            drop(scheduler);
            gone.await;
            settled.set(true);
            Ok(())
        }
    });

    run_until(&scheduler, || settled.get());
    assert!(dropped.get(), "the cancel after the panicking one was lost");
    assert!(scheduler.cancelling.borrow().is_empty());
    Ok(())
}

#[test]
fn a_task_cancelling_a_thread_goes_on_once_the_thread_s_work_is_dropped()
-> luau::runtime::Result<()> {
    let vm = luau::Vm::new()?;
    let scheduler = Rc::new(LocalScheduler::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let thread = waiting_thread(&vm, &scheduler, CallContext::default(), dropped.clone())?;
    scheduler.poll_ready();
    assert!(!dropped.load(Ordering::Acquire));

    let dropped_when_settled = Rc::new(Cell::new(None));
    scheduler.schedule_future(CallContext::default(), {
        let scheduler = Rc::downgrade(&scheduler);
        let (dropped, dropped_when_settled) = (dropped.clone(), dropped_when_settled.clone());
        async move {
            let settled = scheduler
                .upgrade()
                .expect("the scheduler is polling")
                .cancel_luau_thread_and_settle(&thread);
            assert!(
                !dropped.load(Ordering::Acquire),
                "the poll defers the cancel"
            );
            settled.await;
            dropped_when_settled.set(Some(dropped.load(Ordering::Acquire)));
            Ok(())
        }
    });

    run_until(&scheduler, || dropped_when_settled.get().is_some());
    assert_eq!(dropped_when_settled.get(), Some(true));
    Ok(())
}

#[test]
fn a_group_cancelled_from_a_task_runs_no_further() -> luau::runtime::Result<()> {
    let vm = luau::Vm::new()?;
    let scheduler = Rc::new(LocalScheduler::new());
    let group = scheduler.create_group();
    let context = CallContext {
        task_group: group,
        ..CallContext::default()
    };
    let dropped = Arc::new(AtomicBool::new(false));
    waiting_thread(&vm, &scheduler, context.clone(), dropped.clone())?;
    scheduler.poll_ready();

    // The canceller runs first, and the group's own task is ready behind it.
    let ran = Rc::new(Cell::new(false));
    scheduler.schedule_future(CallContext::default(), {
        let scheduler = Rc::downgrade(&scheduler);
        async move {
            scheduler
                .upgrade()
                .expect("the scheduler is polling")
                .cancel_group(group);
            Ok(())
        }
    });
    scheduler.schedule_future(context, {
        let ran = ran.clone();
        async move {
            ran.set(true);
            Ok(())
        }
    });

    scheduler.poll_ready();
    assert!(!ran.get(), "a task of the cancelled group still ran");
    assert!(dropped.load(Ordering::Acquire));
    assert!(scheduler.is_group_cancelled(group));
    assert!(!scheduler.has_pending());
    Ok(())
}
