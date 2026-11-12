//! Luau's calls into the Rust code its values hold: native functions, `HostError.__tostring`,
//! and userdata destructors, which leave what they free for the VM to drop outside the
//! collection.

#![allow(unsafe_code)]

use std::{
    ffi::{
        c_int,
        c_void,
    },
    mem,
    ptr::{
        self,
        NonNull,
    },
};

use harmony_luau_sys as sys;

use super::{
    CallbackSlot,
    RaisedError,
    ReturnState,
    Vm,
    abort_on_panic,
    gateway,
    run_native_function,
    stack::{
        self,
        Stack,
    },
    state,
};

/// Takes the `CallbackSlot` of a native function Luau collects, for the VM to drop.
pub(super) unsafe extern "C-unwind" fn drop_callback_slot(
    state: *mut sys::lua_State,
    userdata: *mut c_void,
) {
    // SAFETY: Luau runs this once, on a thread of the state collecting it, as the destructor of a
    // userdata `Stack::push_native_function` wrote a `CallbackSlot` into.
    abort_on_panic(|| unsafe { defer_drop::<CallbackSlot>(state, userdata) });
}

/// Takes the `T` of a userdata Luau collects, for the VM to drop.
///
/// # Safety
///
/// Luau runs it only for userdata holding a `T`, once, on a thread of the state collecting it.
pub(super) unsafe extern "C-unwind" fn drop_userdata<T>(
    state: *mut sys::lua_State,
    userdata: *mut c_void,
) where
    T: 'static,
{
    // SAFETY: as the caller guarantees.
    abort_on_panic(|| unsafe { defer_drop::<T>(state, userdata) });
}

/// Moves the `T` out of `userdata`, which Luau frees, for its VM to drop outside the collection.
///
/// # Safety
///
/// `userdata` holds a `T`, which nothing reads after this, and `state` is a live thread of a state
/// `LuauState::open` made.
unsafe fn defer_drop<T>(state: *mut sys::lua_State, userdata: *mut c_void)
where
    T: 'static,
{
    if !mem::needs_drop::<T>() {
        return;
    }
    // SAFETY: a `T`, which nothing reads after this, as the caller guarantees.
    let value = unsafe { ptr::read(userdata.cast::<T>()) };
    // SAFETY: as the caller guarantees.
    let control = unsafe { state::control_of(state) };
    control
        .expect("a state LuauState::open made has a control block")
        .defer_drop(Box::new(value));
}

enum CallbackExit {
    Return(c_int),
    Yield(c_int),
    Raise,
}

/// Luau's entry into a native function. Everything with a destructor lives in
/// [`call_native_function`], so this frame holds nothing a raise would skip.
pub(super) unsafe extern "C-unwind" fn native_callback(state: *mut sys::lua_State) -> c_int {
    // SAFETY: Luau calls this only as the closure `Stack::push_native_function` makes, on the
    // thread that runs it.
    let exit = abort_on_panic(|| unsafe { call_native_function(state) });
    match exit {
        CallbackExit::Return(count) => count,
        // SAFETY: `call_native_function` asks to yield only from a yieldable thread, with the
        // results on top.
        CallbackExit::Yield(count) => unsafe { gateway::yield_results(state, count) },
        // SAFETY: the error is on top, and this frame holds nothing with a destructor.
        CallbackExit::Raise => unsafe { gateway::raise(state) },
    }
}

/// Runs a native function and leaves its results on top of the stack, or the error to raise.
///
/// # Safety
///
/// `state` is a live thread running a closure `Stack::push_native_function` made.
unsafe fn call_native_function(state: *mut sys::lua_State) -> CallbackExit {
    let state = NonNull::new(state).expect("Luau calls a native function on a thread");
    // SAFETY: a thread running such a closure, as the caller guarantees.
    let slot = unsafe { stack::callback_slot(state) };
    let Some((slot, vm)) = slot.and_then(|slot| {
        Some((
            slot,
            Vm {
                inner: slot.vm.upgrade()?,
            },
        ))
    }) else {
        // SAFETY: a live state.
        unsafe { gateway::push_error(state.as_ptr(), "Harmony VM is no longer available") };
        return CallbackExit::Raise;
    };
    vm.drop_collected();
    // SAFETY: a live thread of `vm`, which runs the function until it returns.
    let args = unsafe { Stack::from_raw(&vm, state) };
    let exit = run_native_function(slot, args).and_then(|returns| {
        // SAFETY: as for the arguments' stack, which is gone.
        let results = unsafe { Stack::from_raw(&vm, state) };
        push_results(results, returns)
    });
    exit.unwrap_or_else(|error| {
        // SAFETY: as for the arguments' stack, which is gone, as is the results' one.
        let mut stack = unsafe { Stack::from_raw(&vm, state) };
        stack.push_raised(&error);
        stack.keep();
        CallbackExit::Raise
    })
}

/// Leaves what a native function returned on top of `results`, for Luau to return or yield.
fn push_results(
    mut results: Stack<'_>,
    returns: ReturnState,
) -> std::result::Result<CallbackExit, RaisedError> {
    for value in &returns.values {
        results
            .push_value(value)
            .map_err(|error| error.to_string())?;
    }
    let count = results.top() - results.base();
    if returns.yield_requested && !results.is_yieldable() {
        return Err("attempt to yield from a non-yieldable Harmony callback".into());
    }
    results.keep();
    Ok(if returns.yield_requested {
        CallbackExit::Yield(count)
    } else {
        CallbackExit::Return(count)
    })
}

/// `HostError.__tostring`: the error's message. Luau gives a C function enough stack for it, so
/// pushing it fails only by running out of memory.
pub(super) unsafe extern "C-unwind" fn host_error_tostring(state: *mut sys::lua_State) -> c_int {
    let pushed = abort_on_panic(|| {
        // SAFETY: Luau passes the live thread that calls the metamethod, with its argument at 1.
        match unsafe { stack::host_error_at(state, 1) } {
            // SAFETY: a live state.
            Some(error) => match unsafe { gateway::push_bytes(state, error.message.as_bytes()) } {
                Ok(()) => true,
                Err(error) => {
                    // SAFETY: a live state.
                    unsafe { gateway::push_error(state, &error.to_string()) };
                    false
                }
            },
            None => {
                // SAFETY: a live state.
                unsafe { gateway::push_error(state, "expected a host error") };
                false
            }
        }
    });
    if pushed {
        1
    } else {
        // SAFETY: the error is on top, and this frame holds nothing with a destructor.
        unsafe { gateway::raise(state) }
    }
}
