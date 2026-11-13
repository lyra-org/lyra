# Harmony Reference

Lyra embeds Luau through its own crates, not a third-party binding: `harmony-luau-sys` builds Luau,
`harmony-luau` wraps it, `harmony-core` adds modules, scheduling and the plugin runtime, and
`harmony-macros` generates bindings. This covers what isn't obvious from their signatures. For the
shape of the modules plugins see, follow [the plugin API conventions](../plugin-api.md).

## The VM

- All plugins share one `Vm`, with a memory limit of 256 MiB by default. Each plugin's chunks get
  their own global scope, which reads through to the shared globals and keeps the plugin's writes.
- `Vm` is `Rc`-based and not `Send`, as are its handles. The server's VM lives on the
  `lyra-plugin-executor` thread, and everything else reaches it through the executor's queue.
  `InterruptSignal` is what crosses threads to stop running code.
- `NativeFn` closures must still be `Send + Sync`. Keep VM handles out of them; read what a call
  needs from its `CallFrame`.
- `Function`, `Table`, `Thread` and `UserData` pin their value in the registry until the last clone
  drops, and keep the `Vm` alive. Their methods act on that VM, and passing one to another VM
  fails with `Error::VmMismatch`.
- `Thread::resume` and `Thread::resume_error` resume only a suspended coroutine. They fail, and
  leave the thread alone, for the main thread, a dead one, and one that runs or has resumed
  another, such as the thread a native function's `CallFrame` names.
- `VmData` and `ThreadData` hold typed host state, one value per type.
- `harmony-luau` keeps a thread's cancelled mark in Luau's per-thread data pointer, and closes a
  marked thread from Luau's `postresume` callback, so a host must set neither itself.

## Sandboxing

- `Runtime` opens the libraries, registers modules and installs globals, then calls `Vm::sandbox`
  last. Globals and libraries are read-only from then on, so install anything shared before it.
- Every thread that runs plugin code needs `Thread::sandbox`.
- `open_standard_libraries` removes `getfenv` and `setfenv`, which would reach any function's
  globals, including its `require`. `os` is a safe subset.

## The Gateway

Luau raises errors by longjmp, which aborts the process outside a protected call and skips Rust
destructors inside one. `harmony-luau/src/runtime/gateway.rs` is the only caller of `harmony_luau_sys`
functions that can raise: each runs under a protected call in `csrc/gateway.cpp` and returns a
status. `clippy.toml` disallows those functions everywhere else.

- A newly used raising function needs a shim, a gateway wrapper and a `clippy.toml` entry.
- When upgrading Luau, check for functions that have started to raise.
- `Vm::with_options` asserts that Luau still defines the `LuauTableRobustOom` flag.
- `harmony-luau` denies `unsafe` code outside four modules of `harmony-luau/src/runtime/`, and the
  other workspace crates but `lyra-ffmpeg` forbid it:
  - `gateway.rs`, above.
  - `stack.rs` owns access to Luau stacks, the registry and userdata. Its `Stack` checks the
    indexes and value types it is given, so the handles and argument reading hold no `unsafe`.
    Stacks on one thread drop innermost first, and one dropped out of turn panics rather than
    pop what a later one reads. A `Thread` gets its state only from a `Stack` holding the thread.
  - `state.rs` opens and closes the Luau state, holds the allocator and the hooks Luau calls on
    it (`userthread`, `postresume` and the interrupt), and compiles source.
  - `native.rs` is Luau's entry into native functions, `HostError.__tostring` and userdata
    destructors.

  What has no `Vm` to make a `Stack` with works on the raw state: the state, its hooks and
  destructors, a native function's entry until its slot gives it the VM and its exit by raising
  or yielding, `HostError.__tostring`, and the gateway reading a raised error. Clippy requires
  each `unsafe` block to hold one unsafe operation and a `SAFETY:` comment; the gateway's
  comments build on the contract in its module documentation.

## Values

- Luau strings are bytes. `String` and `read_str` fail on invalid UTF-8; `ByteString` and
  `read_string_bytes` don't.
- `Value` has no null: `None` and unit serialize to `nil`, and a table can't hold `nil`, so such a
  field is absent in Luau.
- Numbers are exact to 2^53. `serializable_to_luau_owned` fails past that; `Value::from(i64)`
  silently rounds. Reading an `i64` argument rejects a number that isn't a whole `i64`.
- `Option<T>` reads a missing argument and `nil` alike as `None`.
- `OwnedTable` builds a table in Rust without touching the VM, for returning from a call. `Table`
  is a live handle.
- Reading a string can borrow its bytes from the VM (`read_str`, `ValueRef::String`). Reading a
  buffer copies them, since Luau code a callback runs can write to the buffer.
- `Table` access is raw: `get`, `set`, `len` and `pairs` skip metamethods.

## Userdata

- Each Rust type's userdata gets a Luau tag of its own, and `UserData::borrow` reads a userdata
  only as the type it holds. A host can't choose tags, and Luau code can't make userdata with a
  host's tag: `newproxy` tags its userdata outside their range.
- Luau aligns userdata to 8 bytes, or 16 for userdata of 16 bytes or more, so `create_userdata`
  rejects a type that needs more when it compiles.
- `UserData::borrow` gives shared access only, so state that changes needs interior mutability.
  Bound userdata is `Clone`, and methods receive a clone.
- Luau can't be entered while it collects garbage, so collecting a userdata moves its value
  aside, as it does a collected native function's state and a freed thread's call context once
  no other thread shares it (a coroutine shares its creator's). The VM drops them where Rust
  code next enters it: a new stack, a native call, a call into the VM returning, or closing the
  VM. A panic there aborts the process.
- Userdata types must be `Send`, which keeps VM handles out of them, so a value waiting to drop
  can't keep its VM alive. Its `Drop` may still reach the VM some other way, as through a
  thread-local handle.

## Errors and Panics

- A native function returning `Err` raises into Luau. `Error::Kinded` becomes a `HostError` value
  that Luau code can branch on; any other error raises its message.
- Converting from `anyhow::Error` keeps `OutOfMemory` and `Kinded`, and turns the rest into
  `Runtime`.
- An error value exactly equal to Luau's out-of-memory message reads as `Error::OutOfMemory`, so a
  plugin can raise one.
- A panic in a native function or destructor aborts the process, since unwinding into Luau's C++
  frames would corrupt it. Callbacks must return errors rather than panic.
- Closing a thread, by `Thread::close` or `Thread::cancel` or as a cancelled one yields, aborts the
  process if the allocator itself, not the VM's memory limit, fails while Luau frees its stack:
  the thread would be left suspended with no frame, and resuming it would run garbage.

## Async and Yielding

- An `async fn` in a module yields to the `Scheduler`, which resumes the thread when its
  future completes. One that is ready at once returns without yielding. The future becomes the
  work of the task the scheduler was resuming the thread as, once the thread yields; a thread no
  task is resuming, such as a coroutine Luau code resumed, waits as a task of its own.
- Code can't yield inside a metamethod, a `table.sort` comparator or a host `pcall`. A call that
  must wait there fails with "attempt to yield from a non-yieldable Harmony callback".
- `Thread::cancel` closes a thread as `coroutine.close` does. One that runs, or has resumed
  another, can't be closed yet, and is marked instead: it runs on, calls included, to where it
  next yields, by `coroutine.yield` or a native function that asks to, and is closed there before
  what resumed it runs again. Nothing after that yield runs, and the resume returns no values:
  `coroutine.resume` returns just `true`, and `Thread::resume` returns `ThreadStatus::Closed`.
  One that returns or fails first ends as any other. `coroutine.isyieldable` still returns `true`
  in a marked thread.
- A `ResumeLimit` bounds each resume of a thread: by a budget, by a deadline no resume runs
  past, or by both, when a resume gets the smaller of its budget and what is left until the
  deadline. It can also hold an `InterruptSignal`, which ends a resume once it is raised, from
  any OS thread. Past any of them the thread raises `INTERRUPTED_ERROR`, and
  `LocalLuauTaskCompletion::interrupted_by` tells which one it was: the budget when it was no
  longer than what was left until the deadline, and none for a thread that failed on its own
  after its limit had passed. Luau looks for an interrupt only at calls, loops and returns, so
  a thread that is to be resumed with no time left, at or after the deadline, on a budget of
  none or with the signal raised, isn't resumed at all: it is closed, and its task fails as
  interrupted.
- A host that polls a scheduler sleeps between polls in `Scheduler::wait_for_work`, which a
  woken or due task ends, as does the deadline it is given. For anything else the host polls
  there, it takes a `SchedulerWake` and fires it from any thread, directly or as a `Waker`; a
  wake that arrives between two waits ends the next one. Nothing ticks, so work with no wake and
  no deadline is not looked at again.
- `Cancellation` is a flag that fires such a wake when set. `drive_thread` and a runtime's
  entrypoints take theirs from the scheduler they run on.
- Host work that watches a Luau thread belongs in a task of its own rather than in the host's
  loop, so that the scheduler wakes it: `Scheduler::spawn_watcher` runs a future, which
  need not be `Send`. There, `LocalLuauTaskCompletion::wait` waits for the thread's end,
  `Scheduler::sleep_until` for a deadline and `Cancellation::cancelled` for a cancellation.
  Every round runs the woken watchers first and drops what they cancelled before any other task
  runs, so a thread whose watcher cancels it, for a caller gone or a deadline passed before the
  round began, isn't resumed again, and neither are the threads its work was waiting on.
  `LocalLuauTaskCompletion::ended_at` tells a thread that ended in time from one read late.
- A round polls each task ready as it begins, once, with the scheduler free; a task that polls
  the scheduler it runs on panics. A thread or future a task spawns during a round is a task
  at once, and, like any task started or woken during a round, first runs in the next one. A
  thread or group a task cancels is cancelled at once, and the work of its tasks dropped before
  the task goes on, except the task being polled, which is cancelled as its poll returns:
  `cancel_luau_thread_and_settle` returns a future for the moment its work is gone. A task's
  future must not hold the scheduler's `Rc` across an await, which would keep the scheduler alive
  through itself.
- Cancelling a thread, or a group, closes each thread as `coroutine.close` does, at once and
  before its work is dropped, also while the scheduler polls. A thread that isn't running is
  `dead` from then on: the Luau code holding it can't resume it, and neither does a wait that
  cancelled the thread it was about to wake. That covers a thread that hasn't started, and a
  coroutine cancelled in a host wait, which only Luau code resumed before. A thread that ended,
  or yielded back to the Luau code that resumed it, isn't the scheduler's to close. Cancelling
  reaches the thread's own task only: a coroutine it started that waits on a call of its own
  goes on, until its group is cancelled.
- A cancelled thread never suspends again. One that is suspended, or hasn't started, is closed
  on the spot, as above. One that runs, or has resumed another, whether the scheduler or Luau
  code resumed it, goes on to where it next yields and is closed there, as `Thread::cancel` has
  it. A call that must wait there, or that the run of a cancelled group refuses, yields and
  schedules nothing. The scheduler's own thread is closed before any other task runs, and its
  task is cancelled. The threads a cancelled thread has resumed are not cancelled with it, and
  yield and wait as before.

## Time

- A scheduler reads time from one clock, `Scheduler::now`: delayed tasks and futures,
  `Sleep`, `ResumeLimit::until`, the deadlines of entrypoints and of `drive_thread`, and
  `LocalLuauTaskCompletion::ended_at`, which is read as the thread ends or is cancelled. A host
  makes the deadlines it hands the scheduler from `now`, not from `Instant::now()`.
- A deadline is the first instant that is too late: a `Sleep` is over once the clock reaches it,
  and work is in time only if it ended before it.
- The clock isn't Tokio's, since the drivers poll outside a Tokio context and `wait_for_work`
  blocks the thread. A `tokio::time` wait doesn't follow it and stays real on any clock, so a
  host that must wait in Tokio for a deadline made from `now` waits for what is left until it,
  the deadline less `now`, from Tokio's own time.
- The `testing` feature adds `testing::ManualClock`, which `Scheduler::with_manual_clock` and
  `RuntimeBuilder::manual_clock` install for the scheduler's whole life. It starts at the real
  time and only moves forward, by `advance`, which runs nothing: the next `poll_ready` wakes what
  came due.
- A scheduler on a manual clock never blocks. `wait_for_work` and `wait_for_wake` panic, and what
  runs code to its end on the calling thread fails before running any: `drive_thread`, and a
  runtime's `exec_all`, `exec_plugin`, `restart_plugin`, `run_plugin_source` and `eval_source_*`.
  A test polls instead, with `poll_ready`, `Runtime::start_plugin_restart` and
  `testing::drive_thread_while_ready`.
- Resume budgets stay real on any clock. `ResumeLimit::budget` is real time, and
  `ResumeLimit::until` takes what is left until its deadline from the clock as each resume starts,
  then counts it in real time, so a clock that stands still doesn't let `while true do end` run on.
- A manually polled test can't see a missed wake. Tests of what ends a wait run on the real clock
  and `wait_for_work`, and send the wake once `SchedulerWake::owner_is_waiting`, also under
  `testing`, says the owner is asleep. Keep the watchdog off the thread that polls, or fail on
  it before polling again: a round run because the watchdog ended the wait finds what the
  missing wake left ready.

## Writing Bindings

Bindings live in the crate that owns the exposed module, and for Lyra's types in the plugin layer.

- `#[harmony_macros::module]` turns an impl of associated functions into a module. Parameters are
  read as Luau arguments by name, and `#[context]` parameters come from the call through
  `FromCallFrame`.
- `#[harmony_macros::userdata]` and `#[harmony_macros::userdata_methods]` expose a type.
  `#[harmony_macros::string_enum]` exposes an enum as a string union.
- Option tables and other hand-read arguments implement `FromLuau` and `LuauTypeInfo` side by side,
  so what a function reads and what its definition says stay in step.
- The macros generate the runtime binding and the definitions from one source. Run
  `cargo run -p lyra-docgen -- setup` after changing a module surface.

## Naming

- `raw` is reserved for Luau raw access, such as table operations that bypass metamethods.
- Rust functions installed as native callback entrypoints take a `*_callback` suffix.
- VM-injected module state uses `*ModuleStore`, not `Raw*Store`, unless it is genuinely unprocessed
  input.
