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
  drops, and keep the `Vm` alive. Using one with another VM fails with `Error::VmMismatch`.
- `VmData` and `ThreadData` hold typed host state, one value per type.

## Sandboxing

- `Runtime` opens the libraries, registers modules and installs globals, then calls `Vm::sandbox`
  last. Globals and libraries are read-only from then on, so install anything shared before it.
- Every thread that runs plugin code needs `Vm::sandbox_thread`.
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
- `Table` access is raw: `get`, `set`, `len` and `pairs` skip metamethods.

## Userdata

- A tag binds a Rust type to its userdata; `create_userdata_auto` assigns one per type.
- `UserData::borrow` gives shared access only, so state that changes needs interior mutability.
  Bound userdata is `Clone`, and methods receive a clone.
- Luau runs the value's `Drop` when it collects the userdata. A panic there aborts the process.

## Errors and Panics

- A native function returning `Err` raises into Luau. `Error::Kinded` becomes a `HostError` value
  that Luau code can branch on; any other error raises its message.
- Converting from `anyhow::Error` keeps `OutOfMemory` and `Kinded`, and turns the rest into
  `Runtime`.
- An error value exactly equal to Luau's out-of-memory message reads as `Error::OutOfMemory`, so a
  plugin can raise one.
- A panic in a native function or destructor aborts the process, since unwinding into Luau's C++
  frames would corrupt it. Callbacks must return errors rather than panic.

## Async and Yielding

- An `async fn` in a module yields to the `LocalScheduler`, which resumes the thread when its
  future completes. One that is ready at once returns without yielding.
- Code can't yield inside a metamethod, a `table.sort` comparator or a host `pcall`. A call that
  must wait there fails with "attempt to yield from a non-yieldable Harmony callback".
- A scheduler with a resume limit runs each resume under an interrupt budget. Past it, the thread
  raises `INTERRUPTED_ERROR`.

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
  `cargo run -p lyra-docs -- setup` after changing a module surface.

## Naming

- `raw` is reserved for Luau raw access, such as table operations that bypass metamethods.
- Rust functions installed as native callback entrypoints take a `*_callback` suffix.
- VM-injected module state uses `*ModuleStore`, not `Raw*Store`, unless it is genuinely unprocessed
  input.
