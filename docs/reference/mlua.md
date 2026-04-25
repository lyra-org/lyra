# mlua/mluau Consumer Reference

This document summarizes how mlua behaves for library consumers (app developers and Lua module
authors). It focuses on observable behavior, required configuration, and common pitfalls so you can
use the API correctly without guessing. It is not a maintainer guide.

## mluau

This project depends on [mluau](https://github.com/mluau/mluau), a Luau-focused fork of mlua. In
`Cargo.toml` the dependency is declared as `mlua = { package = "mluau", ... }`, so all Rust code
imports it as `mlua`. The companion scheduler crate
[mluau/scheduler](https://github.com/mluau/scheduler) is imported as `mlua_scheduler`. When
searching for upstream docs or issues, look at the mluau repositories first; behavior may diverge
from mainline mlua.

## Build And Feature Selection
- You must enable exactly one Lua backend: `lua54`, `lua53`, `lua52`, `lua51`, `luajit`, `luajit52`,
  or `luau`. No backend is enabled by default.
- `vendored` builds the chosen Lua or LuaJIT from source and is the easiest way to link.
- `module` builds a `cdylib` for `require()`-style Lua modules; it is not compatible with `send`.
- `async` enables async/await support and `create_async_function` / `call_async`.
- `serde` enables serialization of `mlua::Value` and `LuaSerdeExt` helpers.
- `send` makes `Lua`, `Function`, and `UserData` `Send + Sync` and enforces `Send` bounds.

Other useful feature flags:
- `macros` enables `chunk!` and `FromLua` derive.
- `anyhow` allows `anyhow::Error` to be converted into Lua errors.
- `userdata-wrappers` enables `UserData` impls for common `Rc`/`Arc` wrappers.
- `luau-jit` enables the Luau JIT backend.
- `luau-vector4` makes Luau vectors 4D instead of 3D.

Example (Cargo.toml, standalone):
```toml
[dependencies]
mlua = { version = "0.11", features = ["lua54", "vendored", "macros"] }
```

Example (Cargo.toml, module):
```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
mlua = { version = "0.11", features = ["lua54", "module"] }
```

## Core Model And Ownership Rules
- `Lua` is a handle to a single VM; cloning `Lua` shares the same VM and does not create a new one.
- Handles (`Table`, `Function`, `Thread`, `String`, `AnyUserData`, `Buffer`, `Vector`) are tied to one
  Lua state. Mixing handles across different `Lua` instances will panic or error.
- `RegistryKey` also belongs to one Lua state; using it with another state yields
  `Error::MismatchedRegistryKey`.
- `Lua` is `!Send` by default. Use the `send` feature if you need cross-thread access.
- Use `WeakLua` when you must avoid reference cycles; `WeakLua::upgrade` panics if the VM is gone,
  so prefer `try_upgrade` when destruction is possible.

Example:
```rust
use mlua::{Lua, Result};

fn main() -> Result<()> {
    let lua = Lua::new();
    let lua2 = lua.clone();
    lua.globals().set("answer", 42)?;
    let v: i32 = lua2.globals().get("answer")?;
    assert_eq!(v, 42);
    Ok(())
}
```

## Safe Vs Unsafe States
- `Lua::new()` loads the safe subset of standard libraries and blocks unsafe ones.
- `Lua::new_with(StdLib::..., LuaOptions)` is safe and rejects `StdLib::DEBUG` and (LuaJIT) `FFI`.
- In a safe state, loading `StdLib::PACKAGE` disables C module loading by overriding
  `package.loadlib` and C searchers.
- `Lua::unsafe_new()` or `unsafe_new_with()` loads all standard libraries and permits C modules.
- `Lua::load_std_libs()` obeys the same safety checks if the state was created in safe mode.

Example:
```rust
use mlua::{Lua, LuaOptions, Result, StdLib};

fn main() -> Result<()> {
    let lua = Lua::new_with(StdLib::BASE | StdLib::TABLE, LuaOptions::new())?;
    lua.load_std_libs(StdLib::STRING)?;

    let lua_unsafe = unsafe { Lua::unsafe_new() };
    lua_unsafe.load("return package").eval::<mlua::Table>()?;
    Ok(())
}
```

## Values And Conversions
- `IntoLua` / `FromLua` map common Rust types to Lua values; `Option<T>` maps to `nil`.
- `Vec<T>` and arrays map to sequence tables (`t[1]..t[n]`), maps/sets map to key/value tables.
- `Value::NULL` is a lightuserdata sentinel (not `nil`) used for "null" values, especially in serde.
- Lua strings are raw bytes and may not be UTF-8. Use `mlua::String`, `BorrowedStr`, or
  `BorrowedBytes` and handle invalid UTF-8.
- Unknown VM types (e.g. LuaJIT cdata) show up as `Value::Other`; treat as opaque passthroughs.
- `String` conversion uses Lua coercion rules (strings or numbers only), not arbitrary types.

Example:
```rust
use mlua::{Lua, Result};
use std::collections::HashMap;

fn main() -> Result<()> {
    let lua = Lua::new();
    lua.globals().set("nums", vec![1, 2, 3])?;
    let vals: Vec<i32> = lua.load("return nums").eval()?;
    assert_eq!(vals, vec![1, 2, 3]);

    let none: Option<String> = lua.load("return nil").eval()?;
    assert!(none.is_none());

    let mut map = HashMap::new();
    map.insert("a", 1);
    lua.globals().set("map", map)?;
    let v: i32 = lua.load("return map['a']").eval()?;
    assert_eq!(v, 1);

    let s: mlua::String = lua.load(r#""test\255""#).eval()?;
    assert!(s.to_str().is_err());
    Ok(())
}
```

## Loading And Executing Code
- `Lua::load()` returns a `Chunk`; call `.exec()`, `.eval()`, `.call()`, or `.into_function()`.
- `Chunk::eval()` first tries to parse as an expression by prepending `return`, then falls back.
- `Chunk::set_name()` controls error trace labels; use `@path` for files and `=name` for labels.
- `Chunk::set_environment()` sets `_ENV` for that chunk; you must populate it with needed globals.
- `Chunk::set_mode(ChunkMode::Binary)` runs bytecode; do not run untrusted bytecode.
- `Chunk::try_cache()` compiles and caches bytecode per Lua state to speed repeated loads.

Example:
```rust
use mlua::{Lua, Result};

fn main() -> Result<()> {
    let lua = Lua::new();

    lua.load("x = 2 + 3").set_name("=init").exec()?;
    let v: i32 = lua.load("return x * 2").eval()?;
    assert_eq!(v, 10);

    let f = lua.load("return function(a, b) return a + b end").into_function()?;
    let sum: i32 = f.call((1, 2))?;
    assert_eq!(sum, 3);

    let env = lua.create_table()?;
    env.set("x", 10)?;
    let v2: i32 = lua.load("return x + 1").set_environment(env).eval()?;
    assert_eq!(v2, 11);
    Ok(())
}
```

## Globals And Environments
- `Lua::globals()` returns the `_G` table. In Lua 5.2+ it is shared across threads; in Lua 5.1 and
  Luau it is per-thread.
- `Lua::set_globals()` replaces the global environment for future code, not for existing functions.
- `Function::set_environment()` updates `_ENV` for a Lua function; it returns `false` for Rust/C
  functions.
- Luau sandbox mode blocks `set_globals()` while sandboxed.

Example:
```rust
use mlua::{Function, Lua, Result};

fn main() -> Result<()> {
    let lua = Lua::new();
    lua.globals().set("answer", 42)?;

    let f: Function = lua.load("return function() return answer end").eval()?;
    let env = lua.create_table()?;
    env.set("answer", 7)?;
    assert!(f.set_environment(env)?);

    let v: i32 = f.call(())?;
    assert_eq!(v, 7);
    Ok(())
}
```

## Tables
- `Table::get` / `set` respect `__index` / `__newindex`; use `raw_get` / `raw_set` to bypass.
- Setting a table key to `nil` removes the key.
- `Table::pairs()` does not call `__pairs`; it iterates raw keys like `next`.
- `Table::sequence_values()` yields `t[1]..t[n]` until the first `nil`, without metamethods.
- `Table::len()` uses `__len`; `raw_len()` is the raw length.

Example:
```rust
use mlua::{Lua, Result, Value};

fn main() -> Result<()> {
    let lua = Lua::new();
    let t = lua.create_table()?;
    let mt = lua.create_table()?;
    mt.set("__index", lua.create_function(|_, _k: String| Ok("fallback"))?)?;
    t.set_metatable(Some(mt))?;
    assert_eq!(t.get::<String>("missing")?, "fallback");
    assert!(matches!(t.raw_get::<_, Value>("missing")?, Value::Nil));

    let list = lua.create_sequence_from([1, 2, 3])?;
    let vals: Vec<i32> = list.sequence_values::<i32>().collect::<Result<_>>()?;
    assert_eq!(vals, vec![1, 2, 3]);
    Ok(())
}
```

## Functions And Callbacks
- `create_function` expects `Fn(&Lua, A) -> mlua::Result<R>`; `Err` becomes a Lua error.
- `create_function_mut` uses runtime borrowing and returns `Error::RecursiveMutCallback` if reentered.
- Use tuples or `Variadic<T>` for varargs, and tuples or `MultiValue` for multiple returns.
- To return "nil, err" without raising, return `Ok(Err(err))` from Rust; `Result<T, E>` maps to
  `(value)` or `(nil, err)` as Lua values.
- `Function::bind()` captures arguments as upvalues; binding too many returns `Error::BindError`.
- `create_c_function` is unsafe; only wrap trusted C APIs.

Example:
```rust
use mlua::{Function, Lua, Result, Value, Variadic};

fn main() -> Result<()> {
    let lua = Lua::new();

    let add = lua.create_function(|_, (a, b): (i32, i32)| Ok(a + b))?;
    lua.globals().set("add", add)?;
    let v: i32 = lua.load("return add(2, 3)").eval()?;
    assert_eq!(v, 5);

    let join = lua.create_function(|_, args: Variadic<String>| {
        Ok(args.into_iter().collect::<Vec<_>>().join(","))
    })?;
    lua.globals().set("join", join)?;
    let s: String = lua.load(r#"return join("a","b","c")"#).eval()?;
    assert_eq!(s, "a,b,c");

    let f = lua.create_function(|_, ()| -> Result<std::result::Result<i32, String>> {
        Ok(Err("oops".to_string()))
    })?;
    lua.globals().set("f", f)?;
    let (val, err): (Value, String) = lua.load("return f()").eval()?;
    assert!(val.is_nil());
    assert_eq!(err, "oops");

    let sum: Function = lua.load("return function(a, b, c) return a + b + c end").eval()?;
    let bound = sum.bind(1)?.bind(2)?;
    assert_eq!(bound.call::<i32>(3)?, 6);

    Ok(())
}
```

## Userdata
- Implement `UserData` to expose Rust types; add fields and methods via `UserDataFields` and
  `UserDataMethods`.
- You cannot override `__gc` or `__metatable`; use `Drop` for cleanup.
- `AnyUserData::borrow` / `borrow_mut` enforce runtime borrowing; reentering Lua while holding a
  borrow can cause `UserDataBorrowError` / `UserDataBorrowMutError`.
- `AnyUserData::take` or `destroy` invalidates the userdata; future access yields
  `Error::UserDataDestructed`.
- `create_userdata` requires `'static` (and `Send` if `send` is enabled).
- `create_proxy::<T>()` creates a proxy userdata to expose constructors like `T.new(...)`.

Example:
```rust
use mlua::{Lua, Result, UserData, UserDataMethods};

struct Counter(i32);

impl UserData for Counter {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method_mut("inc", |_, this, ()| {
            this.0 += 1;
            Ok(this.0)
        });
    }
}

fn main() -> Result<()> {
    let lua = Lua::new();
    lua.globals().set("counter", Counter(0))?;
    let v: i32 = lua.load("return counter:inc()").eval()?;
    assert_eq!(v, 1);
    Ok(())
}
```

## Harmony macros (`harmony_macros`)
This repo now uses the local `harmony_macros` crate to generate `UserData` glue and Luau doc metadata
for Lua-facing types like `Album`, `Artist`, `Track`, and `DataStore`.
- `#[harmony_macros::structure]` on a struct exposes all named fields (including private ones) as
  Lua properties with getters and setters. Getters clone the field value, so fields must implement
  `Clone`.
- `#[harmony_macros::implementation]` on an `impl` block exposes methods: static functions become
  `Type.method()` via `add_function`, `&self` methods become `obj:method()` via `add_method`,
  `&mut self` methods become `obj:method()` via `add_method_mut`, and `async fn` uses `add_async_*`.
- `harmony_macros::compile!(type_path = ..., fields = ..., methods = ...)` generates the final
  `impl mlua::UserData` and `impl mlua::FromLua`. `FromLua` clones the underlying userdata and
  errors if the value is not userdata of the exact Rust type.
- `#[harmony_macros::module(...)]` additionally generates Luau module descriptors and, when `path =`
  is provided, the runtime Harmony module wrapper.
- In this repo, `fields = true, methods = true` is used for `Album`/`Artist`/`Track` so Lua can read
  and write fields like `track.track_title`, and call setters like `track:set_track_title("...")`.
  `DataStore`, `Provider`, and `Layer` use `fields = false, methods = true` so only methods are exposed.

Example (from this repo):
```rust
#[derive(DbElement, Serialize, Clone, Debug)]
#[harmony_macros::structure]
pub(crate) struct Track {
    pub(crate) db_id: Option<EntityId>,
    pub(crate) track_title: String,
}

#[harmony_macros::implementation]
impl Track {
    pub(crate) fn set_track_title(&mut self, track_title: String) {
        self.track_title = track_title;
    }
}

harmony_macros::compile!(type_path = Track, fields = true, methods = true);
```

## Scoped Lifetimes (Lua::scope)
- Use `Lua::scope` to create callbacks or userdata that are not `'static` or not `Send`.
- Values created inside a scope become invalid once the scope ends; Lua access then raises an error.
- Scoped userdata must be accessed with `AnyUserData::borrow_scoped` / `borrow_mut_scoped`.
- `create_userdata_ref` is read-only from Lua; `create_userdata_ref_mut` allows mutation.

Example:
```rust
use mlua::{Lua, Result};

fn main() -> Result<()> {
    let lua = Lua::new();
    let mut total = 0;

    lua.scope(|scope| {
        let add = scope.create_function_mut(|_, n: i32| {
            total += n;
            Ok(())
        })?;
        lua.globals().set("add", add)?;
        lua.load("add(3)").exec()?;
        Ok(())
    })?;

    assert_eq!(total, 3);
    assert!(lua.load("add(1)").exec().is_err());
    Ok(())
}
```

## Errors And Panics
- Rust panics inside callbacks are caught and converted to Lua errors by default.
- Set `LuaOptions::catch_rust_panics(false)` if you need `pcall`/`xpcall` to stop catching panics.
- `Error::SyntaxError` includes `incomplete_input` to support REPL-style input.
- `Error::CallbackError` wraps a Rust-side error and includes a Lua traceback.
- If you see `Error::MismatchedRegistryKey`, you used a key from a different Lua state.

Example:
```rust
use mlua::{Error, Lua, Result};

fn main() -> Result<()> {
    let lua = Lua::new();
    match lua.load("function(").eval::<()>() {
        Err(Error::SyntaxError {
            incomplete_input: true,
            ..
        }) => {
            // Ask for more input
        }
        Err(err) => return Err(err),
        Ok(_) => {}
    }
    Ok(())
}
```

## Coroutines And Async
- `Lua::create_thread` creates a coroutine; `Thread::resume` works only when `status()` is
  `Resumable`.
- Calling `resume` on a finished or running coroutine yields `Error::CoroutineUnresumable`.
- Luau adds `Thread::resume_error` to resume and immediately raise an error.
- Async functions must run inside a coroutine; use `Function::call_async` or `AsyncThread` to drive.
- `LuaOptions::thread_pool_size` controls coroutine reuse for async calls.

Example (coroutine):
```rust
use mlua::{Lua, Result};

fn main() -> Result<()> {
    let lua = Lua::new();
    let func = lua.load("return function(x) return x + 1 end").eval()?;
    let thread = lua.create_thread(func)?;
    let v: i32 = thread.resume(1)?;
    assert_eq!(v, 2);
    Ok(())
}
```

Example (async):
```rust
use mlua::{Lua, Result};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let lua = Lua::new();
    let sleep = lua.create_async_function(|_, ms: u64| async move {
        tokio::time::sleep(Duration::from_millis(ms)).await;
        Ok("done")
    })?;
    let v: String = sleep.call_async(10).await?;
    assert_eq!(v, "done");
    Ok(())
}
```

## Registry And App Data
- Named registry values are set via `set_named_registry_value` and retrieved via
  `named_registry_value`.
- `create_registry_value` stores a value with a `RegistryKey`. These entries are not freed
  automatically; call `remove_registry_value` or `expire_registry_values`.
- Dropped `RegistryKey`s can be cleaned via `expire_registry_values`; otherwise they leak.
- `set_app_data` stores typed Rust data in the VM; borrows are dynamic and can fail on reentry.

Example:
```rust
use mlua::{Lua, Result};

fn main() -> Result<()> {
    let lua = Lua::new();
    lua.set_app_data(String::from("prefix"));
    let v = lua.app_data_ref::<String>().unwrap();
    assert_eq!(&*v, "prefix");

    let key = lua.create_registry_value(42)?;
    let val: i32 = lua.registry_value(&key)?;
    assert_eq!(val, 42);
    lua.remove_registry_value(key)?;
    Ok(())
}
```

## Memory And GC
- `set_memory_limit` enforces an allocation cap and returns `Error::MemoryControlNotAvailable` if
  the VM is externally managed (module mode or external Lua state).
- `used_memory` reports total bytes used by the VM (custom allocator or GC counters).
- GC controls include `gc_stop`, `gc_restart`, `gc_collect`, `gc_step`, `gc_inc`, and `gc_gen`.
- Luau supports `set_memory_category` and `heap_dump` for per-category tracking.

Example:
```rust
use mlua::{Lua, Result};

fn main() -> Result<()> {
    let lua = Lua::new();
    lua.set_memory_limit(10 * 1024 * 1024)?;
    let before = lua.used_memory();
    lua.gc_collect()?;
    let after = lua.used_memory();
    assert!(after <= before);
    Ok(())
}
```

## Module Mode (cdylib)
- Enable `feature = "module"` and set `crate-type = ["cdylib"]` in `Cargo.toml`.
- Use `#[mlua::lua_module]` to export `luaopen_<name>` entrypoints.
- `module` is incompatible with `send` (compile-time error).
- Memory limits are unavailable; `#[mlua::lua_module(skip_memory_check)]` trades safety for speed.
- On macOS use `-C link-arg=-undefined -C link-arg=dynamic_lookup`. Windows links to `lua5x.dll`.

Example:
```rust
use mlua::prelude::*;

#[mlua::lua_module]
fn my_module(lua: &Lua) -> LuaResult<LuaTable> {
    let exports = lua.create_table()?;
    exports.set("sum", lua.create_function(|_, (a, b): (i64, i64)| Ok(a + b))?)?;
    Ok(exports)
}
```

## Luau-Specific Behavior
- `Lua::sandbox(true)` makes globals and metatables read-only and enables safeenv; `sandbox(false)`
  restores the original globals and clears sandbox changes.
- `Lua::set_interrupt` is Luau's execution hook; it can yield with `VmState::Yield` at yieldable
  points and ignores recursion.
- `set_thread_collection_callback` must not panic; a panic will abort the process.
- `create_require_function` + `Require` let you customize Luau module resolution; default is
  `TextRequirer`.
- `register_module` requires module names to start with `@` and lowercases them internally.
- `Compiler` can compile Luau source to bytecode; set a default via `Lua::set_compiler`.

Example:
```rust
use mlua::{Lua, Result, VmState};

#[cfg(feature = "luau")]
fn main() -> Result<()> {
    let lua = Lua::new();
    lua.sandbox(true)?;
    lua.load("x = 1").exec()?;
    lua.sandbox(false)?;
    assert!(lua.globals().get::<Option<i32>>("x")?.is_none());

    lua.set_interrupt(|_| Ok(VmState::Continue));
    Ok(())
}

#[cfg(not(feature = "luau"))]
fn main() {}
```

## Macros (feature = "macros")
- `chunk!` embeds Lua in Rust and captures variables with `$name`; captures move into the chunk.
- Because Rust tokenizes the chunk, use double-quoted strings and avoid Lua `--` comments on stable
  Rust (use `//` instead).
- The `//` operator is unusable in `chunk!` because it parses as a comment.
- Some Lua string escapes are unsupported in `chunk!` due to Rust tokenizer limitations.

Example:
```rust
use mlua::{chunk, Lua, Result};

fn main() -> Result<()> {
    let lua = Lua::new();
    let name = "world";
    lua.load(chunk! {
        print("hello, " .. $name)
    }).exec()?;
    Ok(())
}
```

## Mistake-Proof Checklist
- Enable one Lua backend feature and verify your chosen version at build time.
- Do not mix handles or registry keys across different `Lua` instances.
- Use `call_async` or coroutines for async Lua functions; do not call them synchronously.
- Avoid reentrant mutable borrows in userdata or `create_function_mut` callbacks.
- Do not use scoped callbacks or userdata outside `Lua::scope`.
- Treat Lua strings as bytes; validate or convert to UTF-8 when needed.
