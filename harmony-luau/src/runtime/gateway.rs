//! The only caller of Luau API functions that can raise an error.
//!
//! Luau raises by longjmp: outside a protected call that aborts the process, and inside one it
//! jumps over every frame in between without running destructors. The C++ shims in
//! `csrc/gateway.cpp` run each raising operation under a protected call, so a raise lands in C++
//! and comes back here as a status, which these wrappers turn into an [`Error`] after popping the
//! error value. The workspace `clippy.toml` disallows the raising `harmony_luau_sys` functions
//! everywhere else.
//!
//! # Safety
//!
//! Every function here is `unsafe` under one contract, which the `SAFETY:` comments take as given
//! and add to only where a block relies on more:
//!
//! - Each thread passed is a live thread of one Luau state, used on the OS thread that owns it.
//! - The indexes passed name values on its stack, and the values an operation takes from the top
//!   are there, of the types Luau's API requires of them.
//! - A `harmony_luau_gateway_*` shim runs its operation under a protected call and returns its
//!   status. A failed one leaves the stack at its height before the call with the error value on
//!   top, which [`check`] pops.

#![allow(unsafe_code)]
#![allow(clippy::disallowed_methods)]

use std::{
    ffi::{
        CStr,
        c_char,
        c_int,
        c_void,
    },
    ptr,
};

use harmony_luau_sys::{
    self as sys,
    lua_State,
};

use super::{
    Error,
    Result,
};

unsafe extern "C-unwind" {
    fn harmony_luau_gateway_createtable(L: *mut lua_State, narray: c_int, nrec: c_int) -> c_int;
    fn harmony_luau_gateway_newbuffer(
        L: *mut lua_State,
        size: usize,
        data: *mut *mut c_void,
    ) -> c_int;
    fn harmony_luau_gateway_newuserdatatagged(
        L: *mut lua_State,
        size: usize,
        tag: c_int,
        data: *mut *mut c_void,
    ) -> c_int;
    fn harmony_luau_gateway_newuserdatataggedwithmetatable(
        L: *mut lua_State,
        size: usize,
        tag: c_int,
        data: *mut *mut c_void,
    ) -> c_int;
    fn harmony_luau_gateway_newuserdatadtor(
        L: *mut lua_State,
        size: usize,
        dtor: sys::lua_Destructor,
        data: *mut *mut c_void,
    ) -> c_int;
    fn harmony_luau_gateway_newthread(L: *mut lua_State, thread: *mut *mut lua_State) -> c_int;
    fn harmony_luau_gateway_pushcclosurek(
        L: *mut lua_State,
        func: sys::lua_CFunction,
        debugname: *const c_char,
        nup: c_int,
        cont: sys::lua_Continuation,
    ) -> c_int;
    fn harmony_luau_gateway_pushlstring(L: *mut lua_State, s: *const c_char, len: usize) -> c_int;
    fn harmony_luau_gateway_pushnil(L: *mut lua_State) -> c_int;
    fn harmony_luau_gateway_pushboolean(L: *mut lua_State, b: c_int) -> c_int;
    fn harmony_luau_gateway_pushnumber(L: *mut lua_State, n: sys::lua_Number) -> c_int;
    fn harmony_luau_gateway_pushinteger64(L: *mut lua_State, n: i64) -> c_int;
    fn harmony_luau_gateway_pushthread(L: *mut lua_State) -> c_int;
    fn harmony_luau_gateway_pushvalue(L: *mut lua_State, idx: c_int) -> c_int;
    fn harmony_luau_gateway_rawgeti(L: *mut lua_State, idx: c_int, n: c_int) -> c_int;
    fn harmony_luau_gateway_rawgetfield(
        L: *mut lua_State,
        idx: c_int,
        k: *const c_char,
        len: usize,
    ) -> c_int;
    fn harmony_luau_gateway_getmetatable(L: *mut lua_State, idx: c_int, found: *mut c_int)
    -> c_int;
    fn harmony_luau_gateway_ref(L: *mut lua_State, idx: c_int, reference: *mut c_int) -> c_int;
    fn harmony_luau_gateway_rawsetfield(
        L: *mut lua_State,
        idx: c_int,
        k: *const c_char,
        len: usize,
    ) -> c_int;
    fn harmony_luau_gateway_rawseti(L: *mut lua_State, idx: c_int, n: c_int) -> c_int;
    fn harmony_luau_gateway_rawset(L: *mut lua_State, idx: c_int) -> c_int;
    fn harmony_luau_gateway_setfield(
        L: *mut lua_State,
        idx: c_int,
        k: *const c_char,
        len: usize,
    ) -> c_int;
    fn harmony_luau_gateway_setmetatable(L: *mut lua_State, idx: c_int) -> c_int;
    fn harmony_luau_gateway_setreadonly(L: *mut lua_State, idx: c_int, enabled: c_int) -> c_int;
    fn harmony_luau_gateway_next(L: *mut lua_State, idx: c_int, more: *mut c_int) -> c_int;
    fn harmony_luau_gateway_xmove(L: *mut lua_State, to: *mut lua_State, n: c_int) -> c_int;
    fn harmony_luau_gateway_topointer(L: *mut lua_State, idx: c_int) -> *const c_void;
    fn harmony_luau_gateway_tolstring(
        L: *mut lua_State,
        idx: c_int,
        s: *mut *const c_char,
        len: *mut usize,
    ) -> c_int;
    fn harmony_luau_gateway_load(
        L: *mut lua_State,
        chunkname: *const c_char,
        data: *const c_char,
        size: usize,
        result: *mut c_int,
    ) -> c_int;
    fn harmony_luau_gateway_resume(
        L: *mut lua_State,
        co: *mut lua_State,
        nargs: c_int,
        status: *mut c_int,
    ) -> c_int;
    fn harmony_luau_gateway_resumeerror(
        L: *mut lua_State,
        co: *mut lua_State,
        status: *mut c_int,
    ) -> c_int;
    fn harmony_luau_gateway_closethread(
        L: *mut lua_State,
        co: *mut lua_State,
        costatus: *mut c_int,
    ) -> c_int;
    fn harmony_luau_gateway_costatus(L: *mut lua_State, co: *mut lua_State) -> c_int;
    fn harmony_luau_gateway_closeyielded(co: *mut lua_State);
    fn harmony_luau_gateway_cancelthread(co: *mut lua_State);
    fn harmony_luau_gateway_iscancelled(L: *mut lua_State) -> c_int;
    fn harmony_luau_gateway_openlibrary(L: *mut lua_State, name: *const c_char) -> c_int;
    fn harmony_luau_gateway_sandbox(L: *mut lua_State) -> c_int;
    fn harmony_luau_gateway_sandboxthread(L: *mut lua_State) -> c_int;
}

/// Luau's error value for running out of memory, `LUA_MEMERRMSG` in its internal `ldebug.h`.
pub(super) const MEMORY_ERROR: &str = "not enough memory";

const UNKNOWN_ERROR: &str = "unknown Luau error";

/// Calls a shim, whose first argument is the thread it runs on, and returns its status as a
/// result. The arguments are evaluated first, outside the call's `unsafe` block.
macro_rules! protected {
    ($shim:ident($($arg:expr),+ $(,)?)) => {
        protected!(@bind $shim, [$($arg),+], [])
    };
    (@bind $shim:ident, [$next:expr $(, $rest:expr)*], [$($bound:ident)*]) => {{
        let argument = $next;
        protected!(@bind $shim, [$($rest),*], [$($bound)* argument])
    }};
    (@bind $shim:ident, [], [$state:ident $($bound:ident)*]) => {{
        // SAFETY: the module contract, which the caller of the function using this upholds.
        let status = unsafe { $shim($state $(, $bound)*) };
        // SAFETY: a shim's status for the thread it ran on.
        unsafe { check($state, status) }
    }};
}

/// Pops the error value a failed `status` left on top of `state` and returns it as an [`Error`].
unsafe fn take_error(state: *mut lua_State, status: c_int) -> Error {
    // SAFETY: the error value is on top.
    let out_of_memory = status == sys::LUA_ERRMEM || unsafe { is_memory_error(state, -1) };
    // SAFETY: as above.
    let host_error = unsafe { super::stack::host_error_at(state, -1) };
    let error = match host_error {
        _ if out_of_memory => Error::OutOfMemory,
        Some(error) => Error::Kinded {
            kind: error.kind,
            message: error.message.clone(),
        },
        // SAFETY: as above.
        None => Error::Runtime(unsafe { error_message(state, -1) }),
    };
    // SAFETY: as above.
    unsafe { sys::lua_pop(state, 1) };
    error
}

/// `Ok` for a shim's successful `status`; else pops the error it left and returns it.
unsafe fn check(state: *mut lua_State, status: c_int) -> Result<()> {
    if status == sys::LUA_OK {
        Ok(())
    } else {
        // SAFETY: a failed shim left its error value on top.
        Err(unsafe { take_error(state, status) })
    }
}

pub(super) unsafe fn create_table(
    state: *mut lua_State,
    array: c_int,
    records: c_int,
) -> Result<()> {
    protected!(harmony_luau_gateway_createtable(state, array, records))
}

pub(super) unsafe fn new_buffer(state: *mut lua_State, bytes: &[u8]) -> Result<()> {
    let mut data = ptr::null_mut();
    protected!(harmony_luau_gateway_newbuffer(
        state,
        bytes.len(),
        &mut data
    ))?;
    // SAFETY: the new buffer's `bytes.len()` writable bytes are at `data`, apart from `bytes`.
    unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), data.cast(), bytes.len()) };
    Ok(())
}

/// Pushes a userdata of `size` uninitialized bytes and returns them.
pub(super) unsafe fn new_userdata_tagged(
    state: *mut lua_State,
    size: usize,
    tag: c_int,
) -> Result<*mut c_void> {
    let mut data = ptr::null_mut();
    protected!(harmony_luau_gateway_newuserdatatagged(
        state, size, tag, &mut data
    ))?;
    Ok(data)
}

/// Pushes a userdata of `size` uninitialized bytes with the metatable Luau holds for `tag`, and
/// returns them.
pub(super) unsafe fn new_userdata_tagged_with_metatable(
    state: *mut lua_State,
    size: usize,
    tag: c_int,
) -> Result<*mut c_void> {
    let mut data = ptr::null_mut();
    protected!(harmony_luau_gateway_newuserdatataggedwithmetatable(
        state, size, tag, &mut data
    ))?;
    Ok(data)
}

/// Pushes a userdata of `size` uninitialized bytes, finalized by `dtor`, and returns them.
pub(super) unsafe fn new_userdata_dtor(
    state: *mut lua_State,
    size: usize,
    dtor: sys::lua_Destructor,
) -> Result<*mut c_void> {
    let mut data = ptr::null_mut();
    protected!(harmony_luau_gateway_newuserdatadtor(
        state, size, dtor, &mut data
    ))?;
    Ok(data)
}

pub(super) unsafe fn new_thread(state: *mut lua_State) -> Result<*mut lua_State> {
    let mut thread = ptr::null_mut();
    protected!(harmony_luau_gateway_newthread(state, &mut thread))?;
    Ok(thread)
}

pub(super) unsafe fn push_cclosure(
    state: *mut lua_State,
    func: sys::lua_CFunction,
    debug_name: &CStr,
    upvalues: c_int,
) -> Result<()> {
    protected!(harmony_luau_gateway_pushcclosurek(
        state,
        func,
        debug_name.as_ptr(),
        upvalues,
        None
    ))
}

pub(super) unsafe fn push_bytes(state: *mut lua_State, bytes: &[u8]) -> Result<()> {
    protected!(harmony_luau_gateway_pushlstring(
        state,
        bytes.as_ptr().cast(),
        bytes.len()
    ))
}

pub(super) unsafe fn push_nil(state: *mut lua_State) -> Result<()> {
    protected!(harmony_luau_gateway_pushnil(state))
}

pub(super) unsafe fn push_boolean(state: *mut lua_State, value: bool) -> Result<()> {
    protected!(harmony_luau_gateway_pushboolean(state, c_int::from(value)))
}

pub(super) unsafe fn push_number(state: *mut lua_State, value: f64) -> Result<()> {
    protected!(harmony_luau_gateway_pushnumber(state, value))
}

pub(super) unsafe fn push_integer(state: *mut lua_State, value: i64) -> Result<()> {
    protected!(harmony_luau_gateway_pushinteger64(state, value))
}

pub(super) unsafe fn push_thread(state: *mut lua_State) -> Result<()> {
    protected!(harmony_luau_gateway_pushthread(state))
}

/// Pushes a copy of the value at `index`.
pub(super) unsafe fn push_value(state: *mut lua_State, index: c_int) -> Result<()> {
    protected!(harmony_luau_gateway_pushvalue(state, index))
}

pub(super) unsafe fn raw_get_index(state: *mut lua_State, index: c_int, key: c_int) -> Result<()> {
    protected!(harmony_luau_gateway_rawgeti(state, index, key))
}

pub(super) unsafe fn get_ref(state: *mut lua_State, reference: c_int) -> Result<()> {
    // SAFETY: the registry pseudo-index always names a table.
    unsafe { raw_get_index(state, sys::LUA_REGISTRYINDEX, reference) }
}

pub(super) unsafe fn raw_get_field(state: *mut lua_State, index: c_int, key: &[u8]) -> Result<()> {
    protected!(harmony_luau_gateway_rawgetfield(
        state,
        index,
        key.as_ptr().cast(),
        key.len()
    ))
}

/// Pushes the metatable of the value at `index` and returns whether it has one; pushes nothing
/// when it does not.
pub(super) unsafe fn get_metatable(state: *mut lua_State, index: c_int) -> Result<bool> {
    let mut found = 0;
    protected!(harmony_luau_gateway_getmetatable(state, index, &mut found))?;
    Ok(found != 0)
}

pub(super) unsafe fn reference(state: *mut lua_State, index: c_int) -> Result<c_int> {
    let mut reference = 0;
    protected!(harmony_luau_gateway_ref(state, index, &mut reference))?;
    Ok(reference)
}

pub(super) unsafe fn raw_set_field(state: *mut lua_State, index: c_int, key: &[u8]) -> Result<()> {
    protected!(harmony_luau_gateway_rawsetfield(
        state,
        index,
        key.as_ptr().cast(),
        key.len()
    ))
}

pub(super) unsafe fn raw_set_index(state: *mut lua_State, index: c_int, key: c_int) -> Result<()> {
    protected!(harmony_luau_gateway_rawseti(state, index, key))
}

pub(super) unsafe fn raw_set(state: *mut lua_State, index: c_int) -> Result<()> {
    protected!(harmony_luau_gateway_rawset(state, index))
}

pub(super) unsafe fn set_global(state: *mut lua_State, name: &[u8]) -> Result<()> {
    protected!(harmony_luau_gateway_setfield(
        state,
        sys::LUA_GLOBALSINDEX,
        name.as_ptr().cast(),
        name.len()
    ))
}

pub(super) unsafe fn set_metatable(state: *mut lua_State, index: c_int) -> Result<()> {
    protected!(harmony_luau_gateway_setmetatable(state, index))
}

pub(super) unsafe fn set_readonly(
    state: *mut lua_State,
    index: c_int,
    readonly: bool,
) -> Result<()> {
    protected!(harmony_luau_gateway_setreadonly(
        state,
        index,
        c_int::from(readonly)
    ))
}

/// Replaces the key on top with the next key and value of the table at `index`, or pops the key
/// and returns false once the table is exhausted.
pub(super) unsafe fn next(state: *mut lua_State, index: c_int) -> Result<bool> {
    let mut more = 0;
    protected!(harmony_luau_gateway_next(state, index, &mut more))?;
    Ok(more != 0)
}

/// The address of the object at `index`, which identifies it while it lives; never raises.
pub(super) unsafe fn to_pointer(state: *mut lua_State, index: c_int) -> usize {
    // SAFETY: the shim makes no protected call, as `lua_topointer` never raises.
    unsafe { harmony_luau_gateway_topointer(state, index) as usize }
}

pub(super) unsafe fn xmove(from: *mut lua_State, to: *mut lua_State, count: c_int) -> Result<()> {
    protected!(harmony_luau_gateway_xmove(from, to, count))
}

/// Pops values down to `top`, which never raises; a `top` above the current one is left alone.
pub(super) unsafe fn truncate(state: *mut lua_State, top: c_int) {
    // SAFETY: never raises.
    let current = unsafe { sys::lua_gettop(state) };
    debug_assert!(top <= current, "truncating the Luau stack must not grow it");
    if top < current {
        // SAFETY: shrinking the stack only clears slots, which never raises.
        unsafe { sys::lua_settop(state, top) };
    }
}

/// Loads bytecode and pushes the resulting function.
pub(super) unsafe fn load(state: *mut lua_State, chunk_name: &CStr, bytecode: &[u8]) -> Result<()> {
    // luau_load pushes its out-of-memory message outside any protected call; a raise there would
    // skip its C++ cleanup and leave the GC paused, so that slot is reserved up front.
    // SAFETY: `lua_checkstack` reports failing to grow the stack rather than raising.
    if unsafe { sys::lua_checkstack(state, 1) } == 0 {
        return Err(Error::Runtime("stack overflow".to_string()));
    }
    let mut result = 0;
    protected!(harmony_luau_gateway_load(
        state,
        chunk_name.as_ptr(),
        bytecode.as_ptr().cast(),
        bytecode.len(),
        &mut result
    ))?;
    if result == 0 {
        return Ok(());
    }
    // luau_load reports running out of memory only through its message.
    // SAFETY: a failed load leaves its message on top.
    let error = if unsafe { is_memory_error(state, -1) } {
        Error::OutOfMemory
    } else {
        // SAFETY: as above.
        Error::Load(unsafe { error_message(state, -1) })
    };
    // SAFETY: as above.
    unsafe { sys::lua_pop(state, 1) };
    Err(error)
}

/// Calls the function below the top `args` values, leaving all its results. Asking for every
/// result keeps `lua_pcall` from growing the stack, so it cannot raise.
pub(super) unsafe fn pcall(state: *mut lua_State, args: c_int) -> Result<()> {
    // SAFETY: the function and its arguments are on top, and `lua_pcall` is a protected call of
    // its own.
    let status = unsafe { sys::lua_pcall(state, args, sys::LUA_MULTRET, 0) };
    // SAFETY: a failed `lua_pcall` leaves its error on top, as a shim does.
    unsafe { check(state, status) }
}

/// Resumes `thread` from `from` and returns whether it yielded rather than finished.
pub(super) unsafe fn resume(
    from: *mut lua_State,
    thread: *mut lua_State,
    args: c_int,
) -> Result<bool> {
    let mut status = 0;
    protected!(harmony_luau_gateway_resume(from, thread, args, &mut status))?;
    // SAFETY: the status of the resume, which leaves its error on top of `thread`.
    unsafe { resume_status(thread, status) }
}

/// Resumes `thread` from `from` by raising the error value on top of `thread` where it yielded,
/// so the nearest `pcall` in that thread catches it; with none, the thread finishes with the error.
/// Returns whether it yielded again rather than finished.
pub(super) unsafe fn resume_error(from: *mut lua_State, thread: *mut lua_State) -> Result<bool> {
    let mut status = 0;
    protected!(harmony_luau_gateway_resumeerror(from, thread, &mut status))?;
    // SAFETY: the status of the resume, which leaves its error on top of `thread`.
    unsafe { resume_status(thread, status) }
}

/// A thread's state as `coroutine.status` reports it from the thread `from`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CoStatus {
    Running,
    Suspended,
    /// It has resumed another thread. A running thread reads so too from any thread but itself.
    Normal,
    Dead,
}

impl CoStatus {
    /// On the stack of what runs now.
    pub(super) fn is_live(self) -> bool {
        matches!(self, Self::Running | Self::Normal)
    }
}

// Luau's `lua_CoStatus`, which `harmony_luau_sys` leaves out; the rest are dead.
const LUA_CORUN: c_int = 0;
const LUA_COSUS: c_int = 1;
const LUA_CONOR: c_int = 2;

/// Reads a value of Luau's `lua_CoStatus`.
fn co_status_from(costatus: c_int) -> CoStatus {
    match costatus {
        LUA_CORUN => CoStatus::Running,
        LUA_COSUS => CoStatus::Suspended,
        LUA_CONOR => CoStatus::Normal,
        _ => CoStatus::Dead,
    }
}

/// `thread`'s state as `coroutine.status` reports it from `from`; never raises.
pub(super) unsafe fn co_status(from: *mut lua_State, thread: *mut lua_State) -> CoStatus {
    // SAFETY: the shim makes no protected call, as `lua_costatus` never raises.
    co_status_from(unsafe { harmony_luau_gateway_costatus(from, thread) })
}

/// Resets a suspended `thread` as `coroutine.close` does, leaving it dead, and leaves a dead one
/// as it is. Returns whether it is dead: a live one is left alone.
///
/// The reset allocates to shrink the stack. Failing there before the thread reads as dead leaves
/// a suspended thread with no frame, which a resume would run as garbage, so the process aborts.
pub(super) unsafe fn close_thread(from: *mut lua_State, thread: *mut lua_State) -> bool {
    let mut costatus = 0;
    // SAFETY: the shim protects only the reset, and reports the status it reads either way.
    let status = unsafe { harmony_luau_gateway_closethread(from, thread, &mut costatus) };
    if status != sys::LUA_OK {
        // SAFETY: a failed reset left its error on top of `from`.
        unsafe { sys::lua_pop(from, 1) };
    }
    match co_status_from(costatus) {
        CoStatus::Suspended => std::process::abort(),
        status => !status.is_live(),
    }
}

/// Resets `thread`, which a resume has just left suspended, as `coroutine.close` does. A reset
/// that fails aborts the process, as [`close_thread`] does.
///
/// # Safety
///
/// The thread is one a resume has just yielded, which isn't running.
pub(super) unsafe fn close_yielded(thread: *mut lua_State) {
    // SAFETY: a thread that isn't running, as the caller guarantees; the shim aborts rather than
    // return a failure.
    unsafe { harmony_luau_gateway_closeyielded(thread) }
}

/// Marks `thread` as cancelled, for as long as it exists: it runs on, but is closed where it next
/// yields.
pub(super) unsafe fn cancel_thread(thread: *mut lua_State) {
    // SAFETY: setting thread data never raises.
    unsafe { harmony_luau_gateway_cancelthread(thread) }
}

/// Whether [`cancel_thread`] marked `thread`.
pub(super) unsafe fn is_cancelled(thread: *mut lua_State) -> bool {
    // SAFETY: reading thread data never raises.
    unsafe { harmony_luau_gateway_iscancelled(thread) != 0 }
}

/// Reads the status a resume of `thread` ended with, popping the error a failed one left.
unsafe fn resume_status(thread: *mut lua_State, status: c_int) -> Result<bool> {
    match status {
        sys::LUA_OK => Ok(false),
        sys::LUA_YIELD => Ok(true),
        // SAFETY: a failed resume leaves its error on top of the thread.
        _ => Err(unsafe { take_error(thread, status) }),
    }
}

/// Opens one of the standard libraries Harmony exposes, by its global name (`""` for base).
pub(super) unsafe fn open_library(state: *mut lua_State, name: &CStr) -> Result<()> {
    protected!(harmony_luau_gateway_openlibrary(state, name.as_ptr()))
}

/// Freezes the globals and every table in them, as `luaL_sandbox` does.
pub(super) unsafe fn sandbox(state: *mut lua_State) -> Result<()> {
    protected!(harmony_luau_gateway_sandbox(state))
}

/// Gives the thread a fresh global table that reads through to the frozen globals, as
/// `luaL_sandboxthread` does.
pub(super) unsafe fn sandbox_thread(state: *mut lua_State) -> Result<()> {
    protected!(harmony_luau_gateway_sandboxthread(state))
}

/// The bytes of the string at `index`, or `None` when the value is not a string. Numbers are not
/// converted, so this never raises.
pub(super) unsafe fn string_bytes<'a>(state: *mut lua_State, index: c_int) -> Option<&'a [u8]> {
    // SAFETY: `lua_type` never raises.
    if unsafe { sys::lua_type(state, index) } != sys::LUA_TSTRING {
        return None;
    }
    let mut len = 0;
    // SAFETY: a string at `index`, which `lua_tolstring` reads without converting or raising.
    let ptr = unsafe { sys::lua_tolstring(state, index, &mut len) };
    // SAFETY: the string's `len` bytes, which live while it stays on the stack.
    Some(unsafe { std::slice::from_raw_parts(ptr.cast(), len) })
}

/// Whether the error value at `index` is Luau's out-of-memory message, which is how running out
/// of memory reaches us once it has been raised again as an ordinary error.
unsafe fn is_memory_error(state: *mut lua_State, index: c_int) -> bool {
    // SAFETY: needs nothing beyond the contract.
    unsafe { string_bytes(state, index) == Some(MEMORY_ERROR.as_bytes()) }
}

/// Describes the error value at `index`, converting a number in place as `tostring` would.
unsafe fn error_message(state: *mut lua_State, index: c_int) -> String {
    let mut ptr = ptr::null();
    let mut len = 0;
    // SAFETY: `lua_absindex` never raises.
    let index = unsafe { sys::lua_absindex(state, index) };
    // SAFETY: the index stays put while the shim pushes an error on failure.
    let status = unsafe { harmony_luau_gateway_tolstring(state, index, &mut ptr, &mut len) };
    if status != sys::LUA_OK {
        // SAFETY: the failed shim left its error on top.
        unsafe { sys::lua_pop(state, 1) };
        return UNKNOWN_ERROR.to_string();
    }
    if ptr.is_null() {
        return UNKNOWN_ERROR.to_string();
    }
    // SAFETY: the string's `len` bytes, which live while it stays on the stack.
    let bytes = unsafe { std::slice::from_raw_parts(ptr.cast(), len) };
    String::from_utf8_lossy(bytes).into_owned()
}

/// Pushes `message` as an error value. If that fails, the shim's own error value is on the stack
/// instead, so exactly one value is pushed either way.
pub(super) unsafe fn push_error(state: *mut lua_State, message: &str) {
    // SAFETY: `message` is readable for its length, and Luau copies it. Either status leaves one
    // value pushed.
    unsafe { harmony_luau_gateway_pushlstring(state, message.as_ptr().cast(), message.len()) };
}

/// Raises the value on top of the stack as an error.
///
/// # Safety
///
/// The longjmp skips every Rust frame up to the enclosing protected call, so none of them may hold
/// a value with a destructor.
pub(super) unsafe fn raise(state: *mut lua_State) -> ! {
    // SAFETY: the frames up to the protected call hold nothing with a destructor, as the caller
    // guarantees.
    unsafe { sys::lua_error(state) }
}

/// Yields the top `results` values from a native callback.
///
/// # Safety
///
/// The thread must be yieldable, which keeps `lua_yield` from raising.
pub(super) unsafe fn yield_results(state: *mut lua_State, results: c_int) -> c_int {
    // SAFETY: `lua_isyieldable` never raises.
    debug_assert!(unsafe { sys::lua_isyieldable(state) } != 0);
    // SAFETY: a yieldable thread, as the caller guarantees, with the results on top.
    unsafe { sys::lua_yield(state, results) }
}
