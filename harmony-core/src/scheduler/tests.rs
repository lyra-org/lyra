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

use super::testing::*;
use super::*;
use harmony_luau::{
    CapabilityId,
    ModuleId,
};

#[test]
fn ready_future_completes_with_origin_snapshot() {
    let scheduler = Scheduler::new();
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

    let handle = scheduler.spawn_future(context, async { Ok(()) });

    assert_eq!(scheduler.poll_ready(), 1);
    let snapshot = scheduler.snapshot(handle.id()).expect("task snapshot");
    assert_eq!(snapshot.state, TaskState::Completed);
    assert_eq!(snapshot.group, group);
    assert_eq!(snapshot.origin.plugin.as_deref(), Some("demo"));
}

#[test]
fn pending_future_is_polled_again_after_waker_signal() {
    let scheduler = Scheduler::new();
    let polls = Arc::new(AtomicUsize::new(0));
    let handle = scheduler.spawn_future(CallContext::default(), {
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
fn a_task_spawns_another_while_the_scheduler_polls() {
    let scheduler = Rc::new(Scheduler::new());
    let ran = Rc::new(Cell::new(false));
    scheduler.spawn_future(CallContext::default(), {
        let (scheduler, ran) = (Rc::downgrade(&scheduler), ran.clone());
        async move {
            let scheduler = scheduler.upgrade().expect("the scheduler is polling");
            scheduler.spawn_future(CallContext::default(), async move {
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

/// Each round polls the tasks ready as it began: a task that spawns its successor, or wakes
/// itself, even twice, runs once a round, and the task behind it still gets its turn.
#[test]
fn a_task_that_keeps_itself_ready_runs_once_a_round() {
    fn respawn(scheduler: std::rc::Weak<Scheduler>, runs: Rc<Cell<usize>>) {
        let Some(strong) = scheduler.upgrade() else {
            return;
        };
        strong.spawn_future(CallContext::default(), async move {
            runs.set(runs.get() + 1);
            respawn(scheduler, runs);
            Ok(())
        });
    }

    let scheduler = Rc::new(Scheduler::new());
    let respawned = Rc::new(Cell::new(0));
    respawn(Rc::downgrade(&scheduler), respawned.clone());
    let woken = Rc::new(Cell::new(0));
    scheduler.spawn_future(CallContext::default(), {
        let woken = woken.clone();
        poll_fn(move |cx| {
            woken.set(woken.get() + 1);
            cx.waker().wake_by_ref();
            cx.waker().wake_by_ref();
            Poll::Pending
        })
    });
    let other = Rc::new(Cell::new(false));
    scheduler.spawn_future(CallContext::default(), {
        let other = other.clone();
        async move {
            other.set(true);
            Ok(())
        }
    });

    for round in 1..=3 {
        scheduler.poll_ready();
        assert_eq!((respawned.get(), woken.get()), (round, round));
        assert!(other.get());
    }
}

/// A watcher starts a thread and a watcher that cancels it: both first run in the next round,
/// whose watchers run before the thread can start.
#[test]
fn what_a_watcher_starts_first_runs_in_the_next_round() -> luau::runtime::Result<()> {
    let vm = luau::Vm::new()?;
    let scheduler = Rc::new(Scheduler::new());
    let thread = chunk_thread(&vm, "ran = true")?;
    scheduler.spawn_watcher(CallContext::default(), {
        let (scheduler, thread) = (Rc::downgrade(&scheduler), thread.clone());
        async move {
            let scheduler = scheduler.upgrade().expect("the scheduler is polling");
            scheduler.spawn_luau_thread(CallContext::default(), thread.clone(), Vec::new());
            let canceller = Rc::downgrade(&scheduler);
            scheduler.spawn_watcher(CallContext::default(), async move {
                canceller
                    .upgrade()
                    .expect("the scheduler is polling")
                    .cancel_luau_thread(&thread);
                Ok(())
            });
            Ok(())
        }
    });

    scheduler.poll_ready();
    assert!(
        !ran(&vm)?,
        "a thread a watcher started ran in the same round"
    );
    scheduler.poll_ready();
    assert!(
        !ran(&vm)?,
        "the thread ran before the watcher started with it"
    );
    Ok(())
}

/// A thread that waits forever on a host call, whose future raises `dropped` when dropped.
fn waiting_thread(
    vm: &luau::Vm,
    scheduler: &Scheduler,
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
    scheduler.spawn_luau_thread(context.clone(), thread.clone(), Vec::new());
    scheduler.poll_ready();
    let guard = RaiseOnDrop(dropped);
    scheduler.wait_on_future(
        context,
        thread.clone(),
        luau::ScheduledFuture::new(async move {
            let _guard = guard;
            pending::<luau::runtime::Result<()>>().await
        }),
        None,
    );
    Ok(thread)
}

#[test]
fn a_watcher_scheduled_after_a_thread_cancels_it_before_it_starts() -> luau::runtime::Result<()> {
    let vm = luau::Vm::new()?;
    let scheduler = Rc::new(Scheduler::new());
    let thread = vm.create_thread(&vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(&b"ran = true"[..]),
        luau::ChunkOrigin::default(),
    ))?)?;
    let completion = Rc::new(LocalLuauTaskCompletion::default());
    scheduler.spawn_luau_thread_with_completion(
        CallContext::default(),
        thread.clone(),
        Vec::new(),
        completion.clone(),
    );
    scheduler.spawn_watcher(CallContext::default(), {
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
    let scheduler = Rc::new(Scheduler::new());
    let thread = vm.create_thread(&vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(&b"coroutine.yield()"[..]),
        luau::ChunkOrigin::default(),
    ))?)?;
    scheduler.spawn_luau_thread(CallContext::default(), thread.clone(), Vec::new());
    scheduler.poll_ready();
    let (ready, readied) = tokio::sync::oneshot::channel::<()>();
    let resumed = Arc::new(AtomicBool::new(false));
    scheduler.wait_on_future(
        CallContext::default(),
        thread.clone(),
        luau::ScheduledFuture::new({
            let resumed = resumed.clone();
            async move {
                let _ = readied.await;
                resumed.store(true, Ordering::Release);
                Ok(())
            }
        }),
        None,
    );
    let (leave, left) = tokio::sync::oneshot::channel::<()>();
    scheduler.spawn_watcher(CallContext::default(), {
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
    struct CancelOnDrop(std::rc::Weak<Scheduler>, luau::Thread);
    impl Drop for CancelOnDrop {
        fn drop(&mut self) {
            if let Some(scheduler) = self.0.upgrade() {
                scheduler.cancel_luau_thread(&self.1);
            }
        }
    }

    let vm = luau::Vm::new()?;
    let scheduler = Rc::new(Scheduler::new());
    let chunk = |source: &'static [u8]| {
        vm.create_thread(&vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(source),
            luau::ChunkOrigin::default(),
        ))?)
    };
    let outer = chunk(b"coroutine.yield()")?;
    scheduler.spawn_luau_thread(CallContext::default(), outer.clone(), Vec::new());
    scheduler.poll_ready();
    let (leave, left) = tokio::sync::oneshot::channel::<()>();
    scheduler.spawn_watcher(CallContext::default(), {
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
    scheduler.spawn_luau_thread(CallContext::default(), inner.clone(), Vec::new());
    let guard = CancelOnDrop(Rc::downgrade(&scheduler), inner.clone());
    scheduler.wait_on_future(
        CallContext::default(),
        outer,
        luau::ScheduledFuture::new(async move {
            let _guard = guard;
            pending::<luau::runtime::Result<()>>().await
        }),
        None,
    );
    let _ = leave.send(());
    scheduler.poll_ready();

    assert!(!ran(&vm)?, "the inner thread ran");
    Ok(())
}

fn spawn_chunk(
    vm: &luau::Vm,
    scheduler: &Scheduler,
    context: CallContext,
    source: &'static str,
) -> luau::runtime::Result<luau::Thread> {
    let thread = chunk_thread(vm, source)?;
    scheduler.spawn_luau_thread(context, thread.clone(), Vec::new());
    Ok(thread)
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
    scheduler.wait_on_future(
        CallContext::default(),
        handed_back.clone(),
        luau::ScheduledFuture::new(async { Ok(()) }),
        None,
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
    scheduler.spawn_watcher(CallContext::default(), {
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
    assert!(!scheduler.has_pending());
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
    scheduler.spawn_luau_thread_with_completion(
        CallContext::default(),
        thread.clone(),
        Vec::new(),
        completion.clone(),
    );

    scheduler.poll_ready();

    let ended = completion_message(&completion).expect("the thread's task ended");
    assert!(ended.is_err_and(|error| error.contains("cancelled")));
    assert_closed(&vm, &thread);
    assert!(!scheduler.has_pending());
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

/// The thread asks to wait on a call in the round its group is cancelled, after it ran: the
/// call's future never becomes its work.
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
    scheduler.spawn_future(CallContext::default(), {
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

/// Luau code resumed the coroutine itself while it was waiting on a host call, and the
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
        assert!(!scheduler.has_pending(), "{yields}");
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

/// As above inside a poll, from the thread the scheduler resumes: it resumes the waiting
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
        scheduler.spawn_luau_thread_with_completion(
            CallContext::default(),
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
        assert!(!scheduler.has_pending(), "{yields}");
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

/// The wait the thread is in cancels it, or its group, as it completes: the poll that
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
        scheduler.spawn_luau_thread_with_completion(
            CallContext {
                task_group: group,
                ..CallContext::default()
            },
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
        assert!(!scheduler.has_pending(), "{yields}");
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

/// A round unwinds after a thread asked to wait on a call: the thread waits as its task, and
/// cancelling it closes it and drops the wait, which starts no other task.
#[test]
fn a_thread_waiting_as_a_round_unwound_is_closed_with_its_wait() -> luau::runtime::Result<()> {
    struct PanickingWake;
    impl std::task::Wake for PanickingWake {
        fn wake(self: Arc<Self>) {
            panic!("the waker panics");
        }
    }

    let (vm, scheduler) = vm_with_waits()?;
    let waiting = spawn_chunk(
        &vm,
        &scheduler,
        CallContext::default(),
        "wait() went_on = true",
    )?;
    let completion = Rc::new(LocalLuauTaskCompletion::default());
    scheduler.spawn_luau_thread_with_completion(
        CallContext::default(),
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

    let task = scheduler.luau_thread_handle(&waiting).expect("its task");

    assert!(scheduler.cancel_luau_thread(&waiting));

    assert_closed(&vm, &waiting);
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
/// when a completion's waker panics.
fn unwind_a_round(vm: &luau::Vm, scheduler: &Scheduler) -> luau::runtime::Result<()> {
    struct PanickingWake;
    impl std::task::Wake for PanickingWake {
        fn wake(self: Arc<Self>) {
            panic!("the waker panics");
        }
    }

    let completion = Rc::new(LocalLuauTaskCompletion::default());
    scheduler.spawn_luau_thread_with_completion(
        CallContext::default(),
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

/// As above for a group: a wait one of its threads asked for in the unwound round goes with it,
/// as do the threads and the future a task started for it there, and its threads are closed,
/// at once. A start with a completion tells it the thread was cancelled.
#[test]
fn cancelling_a_group_after_a_round_unwound_closes_and_drops_what_it_started()
-> luau::runtime::Result<()> {
    let (vm, scheduler) = vm_with_waits()?;
    let group = scheduler.create_group();
    let in_group = CallContext {
        task_group: group,
        ..CallContext::default()
    };
    let waiting = spawn_chunk(&vm, &scheduler, in_group.clone(), "wait() went_on = true")?;
    let [unstarted, completed, limited, alone] =
        ["went_on = true"; 4].map(|source| chunk_thread(&vm, source).expect("a thread"));
    let completions: [Rc<LocalLuauTaskCompletion>; 3] = Default::default();
    let other = chunk_thread(&vm, "return 42")?;
    let dropped = Rc::new(Cell::new(false));
    scheduler.spawn_future(CallContext::default(), {
        let scheduler = Rc::downgrade(&scheduler);
        let other = other.clone();
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
            scheduler.spawn_luau_thread(in_group.clone(), unstarted, Vec::new());
            scheduler.spawn_luau_thread_with_completion(
                in_group.clone(),
                completed,
                Vec::new(),
                to_completed,
            );
            scheduler.spawn_luau_thread_with_limit_and_completion(
                in_group.clone(),
                limited,
                Vec::new(),
                ResumeLimit::budget(Duration::from_secs(60)),
                to_limited,
            );
            scheduler.spawn_luau_thread_with_completion(
                CallContext::default(),
                alone,
                Vec::new(),
                to_alone,
            );
            scheduler.spawn_luau_thread(CallContext::default(), other, Vec::new());
            scheduler.spawn_future(in_group, async move {
                let _dropped = dropped;
                Ok(())
            });
            Ok(())
        }
    });
    unwind_a_round(&vm, &scheduler)?;
    let woken = completions.each_ref().map(|completion| wait_on(completion));

    assert!(scheduler.cancel_luau_thread(&alone));
    assert_closed(&vm, &alone);
    scheduler.cancel_group(group);

    for thread in [&waiting, &unstarted, &completed, &limited] {
        assert_closed(&vm, thread);
    }
    for (completion, woken) in completions.iter().zip(&woken) {
        assert!(woken.0.load(Ordering::Acquire), "a waiter wasn't woken");
        let ended = completion_message(completion).expect("the start was cancelled");
        assert!(ended.is_err_and(|error| error.contains("cancelled")));
    }
    assert!(dropped.get(), "the group's future was kept");
    scheduler.poll_ready();
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

/// Two of the group's waits a task started in the unwound round panic as they drop: each fails
/// alone, and the rest of the group's work is still closed and dropped.
#[test]
fn cancelling_a_group_outlives_waits_that_panic_as_they_drop() -> luau::runtime::Result<()> {
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
    scheduler.spawn_future(CallContext::default(), {
        let scheduler = Rc::downgrade(&scheduler);
        let (suspended, unstarted) = (suspended.clone(), unstarted.clone());
        let dropped = OnDrop({
            let dropped = dropped.clone();
            move || dropped.set(true)
        });
        async move {
            let scheduler = scheduler.upgrade().expect("the scheduler is polling");
            for thread in suspended {
                let held = OnDrop(|| panic!("drop panicked"));
                scheduler.wait_on_future(
                    in_group.clone(),
                    thread,
                    luau::ScheduledFuture::new(async move {
                        let _held = held;
                        pending::<luau::runtime::Result<()>>().await
                    }),
                    None,
                );
            }
            scheduler.spawn_luau_thread(in_group.clone(), unstarted, Vec::new());
            scheduler.spawn_future(in_group, async move {
                let _dropped = dropped;
                Ok(())
            });
            Ok(())
        }
    });
    unwind_a_round(&vm, &scheduler)?;

    scheduler.cancel_group(group);

    for thread in suspended.iter().chain([&unstarted]) {
        assert_closed(&vm, thread);
    }
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
    let scheduler = Scheduler::new();
    let completion = Rc::new(LocalLuauTaskCompletion::default());
    scheduler.spawn_luau_thread_with_completion(
        CallContext::default(),
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

    scheduler.spawn_future(CallContext::default(), async { Ok(()) });
    assert_eq!(
        scheduler.snapshots().len(),
        2,
        "the scheduler put the task off as if it were still polling"
    );
    Ok(())
}

/// The poll unwinds while the task's work is out of its slot: the task fails, and the round's
/// other ready tasks wait for the next one.
#[test]
fn a_poll_that_unwinds_with_the_work_out_fails_its_task() {
    let scheduler = Scheduler::new();
    let completion = Rc::new(LocalLuauTaskCompletion::default());
    let vm = luau::Vm::new().expect("vm");
    scheduler.spawn_luau_thread_with_completion(
        CallContext::default(),
        chunk_thread(&vm, "coroutine.yield()").expect("thread"),
        Vec::new(),
        completion.clone(),
    );
    let task = scheduler.snapshots()[0].id;
    let unwound = catch_unwind(AssertUnwindSafe(|| {
        let _taken = scheduler.take_work(task).expect("the task is ready");
        panic!("the poll unwinds");
    }));
    assert!(unwound.is_err());

    assert_eq!(
        scheduler.snapshot(task).map(|task| task.state),
        Some(TaskState::Failed)
    );
    let ended = completion_message(&completion).expect("the task's completion was told");
    assert!(ended.is_err_and(|error| error.contains("unwound")));
    let later = scheduler.spawn_future(CallContext::default(), async { Ok(()) });
    assert_eq!(scheduler.poll_ready(), 1);
    assert_eq!(
        scheduler.snapshot(later.id()).map(|task| task.state),
        Some(TaskState::Completed)
    );
}

/// A task can't drive the scheduler it runs on: the round it is in would poll it again.
#[test]
fn a_task_that_polls_its_own_scheduler_fails_with_a_clear_panic() {
    let scheduler = Rc::new(Scheduler::new());
    let task = scheduler.spawn_future(CallContext::default(), {
        let scheduler = Rc::downgrade(&scheduler);
        async move {
            scheduler
                .upgrade()
                .expect("the scheduler is polling")
                .poll_ready();
            Ok(())
        }
    });

    scheduler.poll_ready();

    let failed = scheduler.snapshot(task.id()).expect("the task");
    assert_eq!(failed.state, TaskState::Failed);
    assert!(
        failed
            .error
            .as_deref()
            .is_some_and(|error| error.contains("during a round of the same scheduler")),
        "{:?}",
        failed.error
    );
    let later = scheduler.spawn_future(CallContext::default(), async { Ok(()) });
    assert_eq!(scheduler.poll_ready(), 1);
    assert_eq!(
        scheduler.snapshot(later.id()).map(|task| task.state),
        Some(TaskState::Completed)
    );
}

/// The work of a task that ended is dropped with the scheduler free, so its destructor may call
/// it.
#[test]
fn finished_work_may_call_the_scheduler_as_it_drops() {
    let scheduler = Rc::new(Scheduler::new());
    let spawned = Rc::new(Cell::new(false));
    let guard = OnDrop({
        let scheduler = Rc::downgrade(&scheduler);
        let spawned = spawned.clone();
        move || {
            let scheduler = scheduler.upgrade().expect("the scheduler is alive");
            assert!(scheduler.has_pending());
            let spawned = spawned.clone();
            scheduler.spawn_future(CallContext::default(), async move {
                spawned.set(true);
                Ok(())
            });
        }
    });
    scheduler.spawn_future(CallContext::default(), async move {
        let _guard = guard;
        Ok(())
    });
    scheduler.spawn_future(CallContext::default(), pending());

    scheduler.poll_ready();
    scheduler.poll_ready();
    assert!(spawned.get(), "the task its drop spawned didn't run");
}

/// Starts `source` in `group` with a completion, and polls it to where it waits.
fn waiting_in_group(
    vm: &luau::Vm,
    scheduler: &Scheduler,
    group: TaskGroupId,
    source: &'static str,
) -> luau::runtime::Result<(luau::Thread, Rc<LocalLuauTaskCompletion>)> {
    let thread = chunk_thread(vm, source)?;
    let completion = Rc::new(LocalLuauTaskCompletion::default());
    scheduler.spawn_luau_thread_with_completion(
        CallContext {
            task_group: group,
            ..CallContext::default()
        },
        thread.clone(),
        Vec::new(),
        completion.clone(),
    );
    scheduler.poll_ready();
    Ok((thread, completion))
}

thread_local! {
    static SIBLING: RefCell<Option<luau::Thread>> = const { RefCell::new(None) };
    static SIBLING_RESUMED: Cell<Option<bool>> = const { Cell::new(None) };
}

/// Resumes the thread in `SIBLING` when woken, as code a completion wakes may.
struct ResumesSibling;

impl std::task::Wake for ResumesSibling {
    fn wake(self: Arc<Self>) {
        let resumed = SIBLING
            .with_borrow(|sibling| sibling.as_ref().map(|thread| thread.resume(&[]).is_ok()));
        SIBLING_RESUMED.set(resumed);
    }
}

/// The group's first task's completion wakes code that resumes the second's thread: it finds the
/// thread closed, as cancelling the group closes every thread before telling any completion.
#[test]
fn a_group_s_threads_are_closed_before_any_of_its_completions_is_told() -> luau::runtime::Result<()>
{
    let (vm, scheduler) = vm_with_waits()?;
    let group = scheduler.create_group();
    let (_, first) = waiting_in_group(&vm, &scheduler, group, "wait() went_on = true")?;
    let (second, _) = waiting_in_group(&vm, &scheduler, group, "wait() went_on = true")?;
    SIBLING.set(Some(second.clone()));
    let waker = Waker::from(Arc::new(ResumesSibling));
    assert!(first.poll(&mut Context::from_waker(&waker)).is_pending());

    scheduler.cancel_group(group);

    assert_eq!(
        SIBLING_RESUMED.get(),
        Some(false),
        "the second thread was open"
    );
    SIBLING.set(None);
    assert_closed(&vm, &second);
    Ok(())
}

thread_local! {
    static HELD_DROPPED: Cell<bool> = const { Cell::new(false) };
    static DROPPED_WHEN_WOKEN: Cell<Option<bool>> = const { Cell::new(None) };
}

/// Notes whether `HELD_DROPPED` was set when woken.
struct NotesDrop;

impl std::task::Wake for NotesDrop {
    fn wake(self: Arc<Self>) {
        DROPPED_WHEN_WOKEN.set(Some(HELD_DROPPED.get()));
    }
}

/// The group's first task's completion wakes code that looks at the work of another task in the
/// group: it finds it dropped, as cancelling the group drops every task's work before telling any
/// completion.
#[test]
fn a_group_s_work_is_dropped_before_any_of_its_completions_is_told() -> luau::runtime::Result<()> {
    let (vm, scheduler) = vm_with_waits()?;
    let group = scheduler.create_group();
    let (_, first) = waiting_in_group(&vm, &scheduler, group, "wait() went_on = true")?;
    let held = OnDrop(|| HELD_DROPPED.set(true));
    scheduler.spawn_future(
        CallContext {
            task_group: group,
            ..CallContext::default()
        },
        async move {
            let _held = held;
            pending::<Result<()>>().await
        },
    );
    let waker = Waker::from(Arc::new(NotesDrop));
    assert!(first.poll(&mut Context::from_waker(&waker)).is_pending());

    scheduler.cancel_group(group);

    assert_eq!(
        DROPPED_WHEN_WOKEN.get(),
        Some(true),
        "the group's future was still held"
    );
    Ok(())
}

/// The group's first task's completion wakes a waker that panics: the others are still closed,
/// dropped and told, and the panic carries on from there.
#[test]
fn a_completion_s_waker_that_panics_leaves_the_rest_of_a_group_cancelled()
-> luau::runtime::Result<()> {
    struct PanickingWake;
    impl std::task::Wake for PanickingWake {
        fn wake(self: Arc<Self>) {
            panic!("the waker panics");
        }
    }

    let (vm, scheduler) = vm_with_waits()?;
    let group = scheduler.create_group();
    let (_, first) = waiting_in_group(&vm, &scheduler, group, "wait() went_on = true")?;
    let (second, told) = waiting_in_group(&vm, &scheduler, group, "wait() went_on = true")?;
    let dropped = Rc::new(Cell::new(false));
    let held = OnDrop({
        let dropped = dropped.clone();
        move || dropped.set(true)
    });
    scheduler.spawn_future(
        CallContext {
            task_group: group,
            ..CallContext::default()
        },
        async move {
            let _held = held;
            pending::<Result<()>>().await
        },
    );
    let waker = Waker::from(Arc::new(PanickingWake));
    assert!(first.poll(&mut Context::from_waker(&waker)).is_pending());
    let noted = wait_on(&told);

    let cancelled = catch_unwind(AssertUnwindSafe(|| scheduler.cancel_group(group)));

    assert!(cancelled.is_err(), "the waker's panic was lost");
    assert_closed(&vm, &second);
    assert!(
        noted.0.load(Ordering::Acquire),
        "the second completion wasn't told"
    );
    assert!(dropped.get(), "the group's future wasn't dropped");
    assert!(!scheduler.has_pending());
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
    let scheduler = Scheduler::new();
    let completion = Rc::new(LocalLuauTaskCompletion::default());
    scheduler.spawn_luau_thread_with_completion(
        CallContext::default(),
        thread,
        Vec::new(),
        completion.clone(),
    );
    scheduler.poll_ready();

    let mut cx = Context::from_waker(Waker::noop());
    assert!(completion.poll(&mut cx).is_ready());
    let _ = completion.poll(&mut cx);
}

/// A thread that waits forever on a host call, whose future holds `held`.
fn thread_holding(
    vm: &luau::Vm,
    scheduler: &Scheduler,
    held: impl Sized + 'static,
) -> luau::runtime::Result<luau::Thread> {
    let thread = vm.create_thread(&vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(&b"coroutine.yield()"[..]),
        luau::ChunkOrigin::default(),
    ))?)?;
    scheduler.spawn_luau_thread(CallContext::default(), thread.clone(), Vec::new());
    scheduler.poll_ready();
    scheduler.wait_on_future(
        CallContext::default(),
        thread.clone(),
        luau::ScheduledFuture::new(async move {
            let _held = held;
            pending::<luau::runtime::Result<()>>().await
        }),
        None,
    );
    Ok(thread)
}

#[test]
fn cancelled_work_may_call_the_scheduler_as_it_drops() -> luau::runtime::Result<()> {
    let vm = luau::Vm::new()?;
    let scheduler = Rc::new(Scheduler::new());
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
                scheduler.spawn_future(CallContext::default(), async move {
                    spawned.set(true);
                    Ok(())
                });
            }
        }),
    )?;
    scheduler.poll_ready();
    scheduler.spawn_luau_thread(CallContext::default(), other.clone(), Vec::new());
    scheduler.spawn_watcher(CallContext::default(), {
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
    scheduler.poll_ready();
    assert!(spawned.get(), "the task its drop spawned didn't run");
    Ok(())
}

#[test]
fn cancelled_work_panicking_as_it_drops_fails_nothing_else() -> luau::runtime::Result<()> {
    let vm = luau::Vm::new()?;
    let scheduler = Rc::new(Scheduler::new());
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
    scheduler.spawn_watcher(CallContext::default(), {
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
    Ok(())
}

/// Another task's thread is cancelled, and its work dropped, before the canceller goes on.
#[test]
fn a_task_cancelling_another_s_thread_finds_its_work_dropped() -> luau::runtime::Result<()> {
    let vm = luau::Vm::new()?;
    let scheduler = Rc::new(Scheduler::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let thread = waiting_thread(&vm, &scheduler, CallContext::default(), dropped.clone())?;
    scheduler.poll_ready();
    assert!(!dropped.load(Ordering::Acquire));

    let dropped_when_cancelled = Rc::new(Cell::new(None));
    scheduler.spawn_future(CallContext::default(), {
        let scheduler = Rc::downgrade(&scheduler);
        let (dropped, dropped_when_cancelled) = (dropped.clone(), dropped_when_cancelled.clone());
        async move {
            let settled = scheduler
                .upgrade()
                .expect("the scheduler is polling")
                .cancel_luau_thread_and_settle(&thread);
            dropped_when_cancelled.set(Some(dropped.load(Ordering::Acquire)));
            settled.await;
            Ok(())
        }
    });

    scheduler.poll_ready();
    assert_eq!(dropped_when_cancelled.get(), Some(true));
    Ok(())
}

/// The thread cancels itself and asks to hear once that has settled: its task is the one being
/// polled, so the wait ends once the poll has returned and the task is cancelled.
#[test]
fn settling_the_cancel_of_the_task_being_polled_waits_for_its_poll() -> luau::runtime::Result<()> {
    let (vm, scheduler) = vm_with_waits()?;
    // 1: the settling was still to come as the thread cancelled itself; 2: and it came after.
    let seen = Arc::new(AtomicUsize::new(0));
    let cancel_and_settle: luau::NativeFn = Arc::new({
        let seen = seen.clone();
        move |frame| {
            let scheduler = frame.vm.data().get::<Scheduler>()?;
            let task = scheduler
                .luau_thread_handle(&frame.thread)
                .expect("the thread's task")
                .id();
            let mut settled = Box::pin(scheduler.cancel_luau_thread_and_settle(&frame.thread));
            if settled
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
            {
                seen.store(1, Ordering::Release);
            }
            let (weak, seen) = (Rc::downgrade(&scheduler), seen.clone());
            scheduler.spawn_future(CallContext::default(), async move {
                settled.await;
                let state = weak
                    .upgrade()
                    .and_then(|scheduler| scheduler.snapshot(task))
                    .map(|task| task.state);
                if state == Some(TaskState::Cancelled) && seen.load(Ordering::Acquire) == 1 {
                    seen.store(2, Ordering::Release);
                }
                Ok(())
            });
            Ok(())
        }
    });
    let cancel_and_settle = vm.create_function_with_options(
        luau::NativeFunctionOptions::new(luau::ChunkOrigin::default())
            .function_name("cancel_and_settle"),
        cancel_and_settle,
    )?;
    vm.set_global_function("cancel_and_settle", &cancel_and_settle)?;
    let thread = spawn_chunk(
        &vm,
        &scheduler,
        CallContext::default(),
        "cancel_and_settle() coroutine.yield() went_on = true",
    )?;

    scheduler.poll_ready();
    scheduler.poll_ready();

    assert_eq!(seen.load(Ordering::Acquire), 2);
    assert_closed(&vm, &thread);
    Ok(())
}

/// As above, after the thread already cancelled itself, and asking twice: each wait still ends
/// only once the poll has returned and the task is cancelled.
#[test]
fn settling_a_repeated_cancel_of_the_task_being_polled_waits_for_its_poll()
-> luau::runtime::Result<()> {
    let (vm, scheduler) = vm_with_waits()?;
    // 1: both settlings were still to come as the thread cancelled itself; 2: and both came after.
    let seen = Arc::new(AtomicUsize::new(0));
    let cancel_again_and_settle: luau::NativeFn = Arc::new({
        let seen = seen.clone();
        move |frame| {
            let scheduler = frame.vm.data().get::<Scheduler>()?;
            let task = scheduler
                .luau_thread_handle(&frame.thread)
                .expect("the thread's task")
                .id();
            scheduler.cancel_luau_thread(&frame.thread);
            let mut first = Box::pin(scheduler.cancel_luau_thread_and_settle(&frame.thread));
            let mut second = Box::pin(scheduler.cancel_luau_thread_and_settle(&frame.thread));
            let mut cx = Context::from_waker(Waker::noop());
            if first.as_mut().poll(&mut cx).is_pending()
                && second.as_mut().poll(&mut cx).is_pending()
            {
                seen.store(1, Ordering::Release);
            }
            let (weak, seen) = (Rc::downgrade(&scheduler), seen.clone());
            scheduler.spawn_future(CallContext::default(), async move {
                first.await;
                second.await;
                let state = weak
                    .upgrade()
                    .and_then(|scheduler| scheduler.snapshot(task))
                    .map(|task| task.state);
                if state == Some(TaskState::Cancelled) && seen.load(Ordering::Acquire) == 1 {
                    seen.store(2, Ordering::Release);
                }
                Ok(())
            });
            Ok(())
        }
    });
    let cancel_again_and_settle = vm.create_function_with_options(
        luau::NativeFunctionOptions::new(luau::ChunkOrigin::default())
            .function_name("cancel_again_and_settle"),
        cancel_again_and_settle,
    )?;
    vm.set_global_function("cancel_again_and_settle", &cancel_again_and_settle)?;
    let thread = spawn_chunk(
        &vm,
        &scheduler,
        CallContext::default(),
        "cancel_again_and_settle() coroutine.yield() went_on = true",
    )?;

    scheduler.poll_ready();
    scheduler.poll_ready();

    assert_eq!(seen.load(Ordering::Acquire), 2);
    assert_closed(&vm, &thread);
    Ok(())
}

#[test]
fn a_group_cancelled_from_a_task_runs_no_further() -> luau::runtime::Result<()> {
    let vm = luau::Vm::new()?;
    let scheduler = Rc::new(Scheduler::new());
    let group = scheduler.create_group();
    let context = CallContext {
        task_group: group,
        ..CallContext::default()
    };
    let dropped = Arc::new(AtomicBool::new(false));
    waiting_thread(&vm, &scheduler, context.clone(), dropped.clone())?;
    scheduler.poll_ready();

    // The canceller runs first, and the group's own task is ready behind it. The group's tasks
    // are cancelled, and their work dropped, before the canceller goes on.
    let ran = Rc::new(Cell::new(false));
    let dropped_when_cancelled = Rc::new(Cell::new(None));
    scheduler.spawn_future(CallContext::default(), {
        let scheduler = Rc::downgrade(&scheduler);
        let (dropped, dropped_when_cancelled) = (dropped.clone(), dropped_when_cancelled.clone());
        async move {
            let scheduler = scheduler.upgrade().expect("the scheduler is polling");
            scheduler.cancel_group(group);
            dropped_when_cancelled.set(Some((
                dropped.load(Ordering::Acquire),
                scheduler
                    .group_snapshots(group)
                    .iter()
                    .all(|task| task.state != TaskState::Pending),
            )));
            Ok(())
        }
    });
    scheduler.spawn_future(context, {
        let ran = ran.clone();
        async move {
            ran.set(true);
            Ok(())
        }
    });

    scheduler.poll_ready();
    assert_eq!(dropped_when_cancelled.get(), Some((true, true)));
    assert!(!ran.get(), "a task of the cancelled group still ran");
    assert!(dropped.load(Ordering::Acquire));
    assert!(scheduler.is_group_cancelled(group));
    assert!(!scheduler.has_pending());
    Ok(())
}
