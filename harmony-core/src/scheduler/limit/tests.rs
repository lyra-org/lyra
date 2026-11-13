use std::sync::Arc;

use crate::testing::ManualClock;

use super::*;
use crate::scheduler::testing::*;
use crate::scheduler::*;

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
    let scheduler = Scheduler::new();
    let timed = scheduler.spawn_luau_thread_with_limit_and_completion(
        CallContext::default(),
        timed_thread,
        Vec::new(),
        ResumeLimit::budget(Duration::ZERO),
        Rc::default(),
    );
    let ordinary = scheduler.spawn_luau_thread(CallContext::default(), ordinary_thread, Vec::new());

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
    let scheduler = Scheduler::new();
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
            let task = scheduler.spawn_luau_thread_with_limit_and_completion(
                CallContext::default(),
                vm.create_thread(&function)?,
                Vec::new(),
                limit,
                completion.clone(),
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
        let scheduler = Scheduler::with_manual_clock(clock.clone());
        let completion = Rc::new(LocalLuauTaskCompletion::default());
        scheduler.spawn_luau_thread_with_limit_and_completion(
            CallContext::default(),
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

/// The deadline is read by the scheduler's clock, and what is left until it is enforced in
/// real time: a clock that stands still doesn't let a resume run for good.
#[test]
fn a_frozen_clock_still_interrupts_a_resume_at_its_limit() -> luau::runtime::Result<()> {
    let vm = luau::Vm::new()?;
    let clock = ManualClock::new();
    let scheduler = Scheduler::with_manual_clock(clock.clone());
    let limits = [
        ResumeLimit::budget(Duration::from_millis(20)),
        ResumeLimit::until(clock.now() + Duration::from_millis(20)),
    ];
    let completions = limits.map(|limit| -> luau::runtime::Result<_> {
        let completion = Rc::new(LocalLuauTaskCompletion::default());
        scheduler.spawn_luau_thread_with_limit_and_completion(
            CallContext::default(),
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
    let scheduler = Scheduler::with_manual_clock(clock.clone());
    // An hour away in real time, and passed once the clock says so.
    let deadline = clock.now() + Duration::from_secs(3600);
    clock.advance(Duration::from_secs(3600));
    let completion = Rc::new(LocalLuauTaskCompletion::default());
    scheduler.spawn_luau_thread_with_limit_and_completion(
        CallContext::default(),
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

/// Schedules `source` under `limit` and returns its thread and what its task ends with.
fn spawn_limited(
    vm: &luau::Vm,
    scheduler: &Scheduler,
    source: &'static str,
    limit: ResumeLimit,
) -> luau::runtime::Result<(luau::Thread, Rc<LocalLuauTaskCompletion>)> {
    let thread = chunk_thread(vm, source)?;
    let completion = Rc::new(LocalLuauTaskCompletion::default());
    scheduler.spawn_luau_thread_with_limit_and_completion(
        CallContext::default(),
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
        let (vm, scheduler) = vm_with_waits_on(Scheduler::with_manual_clock(clock.clone()))?;
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
    let (vm, scheduler) = vm_with_waits_on(Scheduler::with_manual_clock(clock.clone()))?;
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
