//! Helpers shared by the scheduler's tests.

use std::{
    future::pending,
    sync::Arc,
    task::Poll,
};

use super::*;

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

/// Runs rounds that sleep in between, as a host does, until `done`. Only a wake or a due
/// task ends each sleep.
pub(super) fn run_until(scheduler: &Scheduler, done: impl Fn() -> bool) {
    loop {
        scheduler.poll_ready();
        if done() {
            return;
        }
        scheduler.wait_for_work(None);
    }
}

pub(super) fn chunk_thread(
    vm: &luau::Vm,
    source: &'static str,
) -> luau::runtime::Result<luau::Thread> {
    vm.create_thread(&vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(source.as_bytes()),
        luau::ChunkOrigin::default(),
    ))?)
}

pub(super) fn completion_message(
    completion: &LocalLuauTaskCompletion,
) -> Option<Result<(), String>> {
    match completion.poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(result) => Some(result.map(|_| ()).map_err(|error| error.message)),
        Poll::Pending => None,
    }
}

/// A task's own panic fails the task alone. What unwinds a poll is a panic outside the tasks'
/// code, such as one from the waker a completion wakes.
/// A VM whose scheduler is its own, with `wait()`, a host call that never returns, `pause()`,
/// one that returns in the next round, `cancel(thread)` and `cancel_group(id)`, and
/// `cancel_as_it_wakes()` and `cancel_group_as_it_wakes()`, waits that cancel the thread they
/// are about to wake, or its group.
pub(super) fn vm_with_waits() -> luau::runtime::Result<(luau::Vm, Rc<Scheduler>)> {
    vm_with_waits_on(Scheduler::new())
}

/// [`vm_with_waits`] with `scheduler` as the VM's.
pub(super) fn vm_with_waits_on(
    scheduler: Scheduler,
) -> luau::runtime::Result<(luau::Vm, Rc<Scheduler>)> {
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
                let scheduler = Rc::downgrade(&frame.vm.data().get::<Scheduler>()?);
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
                let scheduler = Rc::downgrade(&frame.vm.data().get::<Scheduler>()?);
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
                .get::<Scheduler>()?
                .cancel_group(TaskGroupId(group as u64));
            Ok(())
        }))
        .function(FunctionSpec::new("cancel").call(|mut frame| {
            let thread: luau::Thread = frame.args.read()?;
            frame
                .vm
                .data()
                .get::<Scheduler>()?
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
    let scheduler = vm.data().get::<Scheduler>()?;
    Ok((vm, scheduler))
}

/// Whether a chunk that sets `ran` has run in `vm`.
pub(super) fn ran(vm: &luau::Vm) -> luau::runtime::Result<bool> {
    Ok(eval(vm, "return ran")? == [luau::Value::Boolean(true)])
}

pub(super) fn eval(vm: &luau::Vm, source: &'static str) -> luau::runtime::Result<Vec<luau::Value>> {
    vm.eval(
        Arc::<[u8]>::from(source.as_bytes()),
        luau::ChunkOrigin::default(),
    )
}

/// What Luau code holding `thread` makes of it: its status, and what resuming it returns.
pub(super) fn status_and_resume(
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
pub(super) fn assert_closed(vm: &luau::Vm, thread: &luau::Thread) {
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

pub(super) struct OnDrop<F: FnMut()>(pub(super) F);

impl<F: FnMut()> Drop for OnDrop<F> {
    fn drop(&mut self) {
        (self.0)();
    }
}
