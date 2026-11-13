use std::{
    future::pending,
    sync::Arc,
    task::Poll,
};

use super::*;
use crate::scheduler::testing::*;
use crate::scheduler::work::WorkPoll;
use crate::scheduler::*;

#[test]
fn removing_one_finished_task_preserves_other_finished_tasks() {
    let scheduler = Scheduler::new();
    let first = scheduler.spawn_future(CallContext::default(), async { Ok(()) });
    let second = scheduler.spawn_future(CallContext::default(), async { Ok(()) });

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
    let scheduler = Scheduler::new();
    let group = scheduler.create_group();
    let context = CallContext {
        task_group: group,
        ..CallContext::default()
    };

    let first = scheduler.spawn_future(context, pending());
    let second = scheduler.spawn_future(
        CallContext {
            task_group: group,
            ..CallContext::default()
        },
        pending(),
    );

    scheduler.cancel_group(group);
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
fn cancelled_group_cancels_tasks_spawned_into_it_later() {
    let scheduler = Scheduler::new();
    let group = scheduler.create_group();
    let other = scheduler.create_group();
    scheduler.cancel_group(group);

    let late = scheduler.spawn_future(
        CallContext {
            task_group: group,
            ..CallContext::default()
        },
        async { Ok(()) },
    );
    let unaffected = scheduler.spawn_future(
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

/// A thread whose function returns nil.
fn nil_thread() -> luau::runtime::Result<luau::Thread> {
    let vm = luau::Vm::new()?;
    let function = vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(&b"return nil"[..]),
        luau::ChunkOrigin::default(),
    ))?;
    vm.create_thread(&function)
}

fn never_ready() -> luau::ScheduledFuture {
    luau::ScheduledFuture::new(pending::<luau::runtime::Result<()>>())
}

/// Polls task `id`, whose thread is `thread`, as if the thread asked to wait on `future`, for a
/// call of `awaiting`'s run, and yielded.
fn wait_in_poll(
    scheduler: &Scheduler,
    id: TaskId,
    thread: &luau::Thread,
    awaiting: TaskGroupId,
    future: luau::ScheduledFuture,
) {
    let taken = scheduler.take_work(id).expect("the task is ready");
    let waiting = scheduler
        .state
        .borrow_mut()
        .wait_in_polled_task(thread, awaiting, future, None);
    assert!(matches!(waiting, Ok(None)));
    let (context, work) = taken.put_back();
    let (_, ended, _) =
        scheduler
            .state
            .borrow_mut()
            .after_poll(id, context, work, WorkPoll::Yielded, None);
    scheduler.finish(ended);
}

#[test]
fn only_the_thread_of_the_task_being_polled_waits_in_that_task() -> luau::runtime::Result<()> {
    let scheduler = Scheduler::new();
    let (thread, other) = (nil_thread()?, nil_thread()?);
    let task = scheduler.spawn_luau_thread(CallContext::default(), thread.clone(), Vec::new());
    let wait = |thread: &luau::Thread| {
        scheduler
            .state
            .borrow_mut()
            .wait_in_polled_task(thread, TaskGroupId::default(), never_ready(), None)
            .is_ok()
    };
    assert!(!wait(&thread), "no task is being polled");

    let taken = scheduler.take_work(task.id()).expect("the task is ready");
    assert!(!wait(&other));
    assert!(wait(&thread));
    let (context, work) = taken.put_back();
    let (_, ended, _) =
        scheduler
            .state
            .borrow_mut()
            .after_poll(task.id(), context, work, WorkPoll::Yielded, None);
    scheduler.finish(ended);

    let state = scheduler.state.borrow();
    let task = state.tasks.get(&task.id()).expect("the task");
    assert!(matches!(task.work, Some(TaskWork::LuauFuture { .. })));
    Ok(())
}

#[test]
fn a_task_awaiting_another_group_s_future_belongs_to_both_groups() -> luau::runtime::Result<()> {
    for cancel_callee in [true, false] {
        let scheduler = Scheduler::new();
        let caller = scheduler.create_group();
        let callee = scheduler.create_group();
        let thread = nil_thread()?;
        let task = scheduler.spawn_luau_thread(
            CallContext {
                task_group: caller,
                ..CallContext::default()
            },
            thread.clone(),
            Vec::new(),
        );
        wait_in_poll(&scheduler, task.id(), &thread, callee, never_ready());
        for group in [caller, callee] {
            let snapshots = scheduler.group_snapshots(group);
            assert_eq!(snapshots.len(), 1);
            assert_eq!(snapshots[0].id, task.id());
            assert_eq!(snapshots[0].group, caller);
        }

        let cancelled = if cancel_callee { callee } else { caller };
        scheduler.cancel_group(cancelled);
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
    let scheduler = Scheduler::new();
    let caller = scheduler.create_group();
    let callee = scheduler.create_group();
    let thread = nil_thread()?;
    let task = scheduler.spawn_luau_thread(
        CallContext {
            task_group: caller,
            ..CallContext::default()
        },
        thread.clone(),
        Vec::new(),
    );
    wait_in_poll(
        &scheduler,
        task.id(),
        &thread,
        callee,
        luau::ScheduledFuture::new(async { luau::runtime::Result::Ok(()) }),
    );

    assert_eq!(scheduler.group_snapshots(callee).len(), 1);
    scheduler.poll_ready();
    assert!(scheduler.group_snapshots(callee).is_empty());
    assert_eq!(scheduler.group_snapshots(caller).len(), 1);
    Ok(())
}

/// The thread asks to wait for a run that has ended: nothing will resume it, so its task is
/// cancelled and the wait dropped.
#[test]
fn a_wait_for_a_cancelled_run_cancels_the_task() -> luau::runtime::Result<()> {
    let scheduler = Scheduler::new();
    let callee = scheduler.create_group();
    scheduler.cancel_group(callee);
    let thread = nil_thread()?;
    let task = scheduler.spawn_luau_thread(CallContext::default(), thread.clone(), Vec::new());
    let dropped = Rc::new(Cell::new(false));
    let held = OnDrop({
        let dropped = dropped.clone();
        move || dropped.set(true)
    });
    wait_in_poll(
        &scheduler,
        task.id(),
        &thread,
        callee,
        luau::ScheduledFuture::new(async move {
            let _held = held;
            pending::<luau::runtime::Result<()>>().await
        }),
    );

    assert_eq!(
        scheduler.snapshot(task.id()).map(|task| task.state),
        Some(TaskState::Cancelled)
    );
    assert!(dropped.get());
    assert!(thread.resume(&[]).is_err(), "the thread was left open");
    Ok(())
}

#[test]
fn default_group_is_never_cancelled() {
    let scheduler = Scheduler::new();
    let pending_task = scheduler.spawn_future(CallContext::default(), pending());

    scheduler.cancel_group(TaskGroupId::default());
    let later = scheduler.spawn_future(CallContext::default(), async { Ok(()) });

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
fn pending_budgeted_luau_thread_preserves_budget_and_completion() -> luau::runtime::Result<()> {
    let vm = luau::Vm::new()?;
    let function = vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(&b"return 42"[..]),
        luau::ChunkOrigin::default(),
    ))?;
    let thread = vm.create_thread(&function)?;
    let scheduler = Scheduler::new();
    let completion = Rc::new(LocalLuauTaskCompletion::default());

    scheduler.spawn_luau_thread_with_limit_and_completion(
        CallContext::default(),
        thread.clone(),
        Vec::new(),
        ResumeLimit::budget(Duration::from_secs(1)),
        completion.clone(),
    );

    let handle = scheduler
        .luau_thread_handle(&thread)
        .expect("scheduled thread handle");
    let inner = scheduler.state.borrow();
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
