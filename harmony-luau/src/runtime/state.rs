//! The Luau state behind a [`Vm`](super::Vm) and Luau's library calls outside its stacks:
//! opening and closing the state with its [`VmControl`], the allocator and hooks Luau calls on it,
//! and compiling the bytecode it loads.

#![allow(unsafe_code)]

use std::{
    alloc::Layout,
    ffi::{
        c_int,
        c_void,
    },
    ptr::{
        self,
        NonNull,
    },
    sync::{
        Once,
        atomic::Ordering,
    },
};

use harmony_luau_sys as sys;

use super::{
    CompileOptions,
    Error,
    INTERRUPTED_ERROR,
    Result,
    VmControl,
    abort_on_panic,
    gateway,
};

static LUAU_FLAGS: Once = Once::new();

/// A Luau state and the control block its callbacks read, which live and die together.
pub(super) struct LuauState {
    main: NonNull<sys::lua_State>,
    control: NonNull<VmControl>,
}

impl LuauState {
    pub(super) fn open(control: VmControl) -> Result<Self> {
        LUAU_FLAGS.call_once(|| {
            // Keeps tables intact when growing or freezing them runs out of memory.
            // SAFETY: a nul-terminated name, set before this crate opens any state.
            let set = unsafe { sys::harmony_luau_setfflag(c"LuauTableRobustOom".as_ptr(), 1) };
            assert!(
                set != 0,
                "Luau no longer defines the LuauTableRobustOom flag"
            );
        });
        let control = NonNull::from(Box::leak(Box::new(control)));
        // SAFETY: `alloc` takes the control block as its userdata, which outlives the state.
        let main = unsafe { sys::lua_newstate(Some(alloc), control.as_ptr().cast()) };
        let Some(main) = NonNull::new(main) else {
            // SAFETY: the block came from `Box::leak` above, and no state holds it.
            drop(unsafe { Box::from_raw(control.as_ptr()) });
            return Err(Error::VmCreate);
        };
        // SAFETY: a live state, given the control block its callbacks read.
        unsafe { sys::lua_set_callback_userdata(main.as_ptr(), control.as_ptr().cast()) };
        // SAFETY: a live state.
        let callbacks = unsafe { sys::lua_callbacks(main.as_ptr()) };
        // SAFETY: the state's callbacks, which live as long as it does and which nothing else
        // borrows here.
        let callbacks = unsafe { &mut *callbacks };
        callbacks.userthread = Some(userthread);
        callbacks.postresume = Some(postresume);
        Ok(Self { main, control })
    }

    /// The main thread, which lives as long as `self`.
    pub(super) fn main(&self) -> NonNull<sys::lua_State> {
        self.main
    }

    pub(super) fn control(&self) -> &VmControl {
        // SAFETY: the block lives until `self` drops, and is only ever borrowed shared.
        unsafe { self.control.as_ref() }
    }

    /// Has Luau call the interrupt at its safepoints, or stop calling it.
    pub(super) fn set_interrupt(&self, enabled: bool) {
        let callback: sys::lua_InterruptCallback = if enabled { Some(interrupt) } else { None };
        // SAFETY: a live state.
        unsafe { sys::lua_set_interrupt_callback(self.main.as_ptr(), callback) };
    }

    #[cfg(test)]
    pub(super) fn has_interrupt(&self) -> bool {
        // SAFETY: a live state.
        let callbacks = unsafe { sys::lua_callbacks(self.main.as_ptr()) };
        // SAFETY: the state's callbacks, which live as long as it does.
        unsafe { (*callbacks).interrupt }.is_some()
    }

    /// Runs Luau's `lua_gc` with `what` and no data.
    #[cfg(test)]
    pub(super) fn gc(&self, what: c_int) {
        // SAFETY: a live state, with no Rust frames between here and Luau to skip.
        unsafe { sys::lua_gc(self.main.as_ptr(), what, 0) };
    }
}

impl Drop for LuauState {
    /// Closes the state, then drops what closing it collected.
    fn drop(&mut self) {
        // SAFETY: a live state, closed once; its callbacks read the control block until then.
        unsafe { sys::lua_close(self.main.as_ptr()) };
        self.control().drop_collected();
        // SAFETY: the block came from `Box::leak` in `open`, and the closed state no longer
        // reads it.
        drop(unsafe { Box::from_raw(self.control.as_ptr()) });
    }
}

/// The control block a callback's `state` was opened with, if any.
///
/// # Safety
///
/// `state` is a live thread of a state [`LuauState::open`] made.
pub(super) unsafe fn control_of<'a>(state: *mut sys::lua_State) -> Option<&'a VmControl> {
    // SAFETY: a live state, as the caller guarantees.
    let control = unsafe { sys::lua_callback_userdata(state) }.cast::<VmControl>();
    // SAFETY: the state's control block, which lives as long as it does.
    unsafe { control.as_ref() }
}

/// A new thread, such as a coroutine, starts with its creator's call context;
/// a freed thread's context goes with it.
unsafe extern "C-unwind" fn userthread(parent: *mut sys::lua_State, thread: *mut sys::lua_State) {
    // SAFETY: Luau passes a live thread.
    if let Some(control) = unsafe { control_of(thread) } {
        control.track_thread(
            (!parent.is_null()).then_some(parent as usize),
            thread as usize,
        );
    }
}

/// Closes a thread [`Thread::cancel`](super::Thread::cancel) found live as soon as a resume leaves
/// it suspended, before whatever resumed it runs again.
unsafe extern "C-unwind" fn postresume(thread: *mut sys::lua_State) {
    // SAFETY: Luau passes the live thread a resume has just left.
    if unsafe { sys::lua_status(thread) } != sys::LUA_YIELD {
        return;
    }
    // SAFETY: as above.
    if !unsafe { gateway::is_cancelled(thread) } {
        return;
    }
    // SAFETY: a thread the resume has just yielded, which isn't running.
    let close = || unsafe { gateway::close_yielded(thread) };
    // SAFETY: as above.
    match unsafe { control_of(thread) } {
        Some(control) => control.without_memory_limit(close),
        None => close(),
    }
}

unsafe extern "C-unwind" fn interrupt(state: *mut sys::lua_State, gc: c_int) {
    if state.is_null() || gc >= 0 {
        return;
    }
    // SAFETY: Luau passes the live thread it runs.
    let control = unsafe { control_of(state) };
    if abort_on_panic(|| control.is_some_and(VmControl::interrupt_due)) {
        // SAFETY: a live state.
        unsafe { gateway::push_error(state, INTERRUPTED_ERROR) };
        // SAFETY: Luau calls the interrupt from its interpreter, under the protected call that
        // runs the thread, and this frame holds nothing with a destructor.
        unsafe { gateway::raise(state) }
    }
}

/// Luau keeps 16-byte-aligned data at the start of its blocks (`lua_Page::data`, `LG`).
const LUAU_ALIGN: usize = 16;

unsafe extern "C-unwind" fn alloc(
    ud: *mut c_void,
    ptr: *mut c_void,
    osize: usize,
    nsize: usize,
) -> *mut c_void {
    // SAFETY: Luau passes the control block the state was opened with, and a block of its own.
    abort_on_panic(|| unsafe { alloc_within_limit(ud, ptr, osize, nsize) })
}

/// # Safety
///
/// `ud` is null or a live [`VmControl`], and a non-null `ptr` is a live block of `osize` bytes
/// from [`reallocate`].
unsafe fn alloc_within_limit(
    ud: *mut c_void,
    ptr: *mut c_void,
    osize: usize,
    nsize: usize,
) -> *mut c_void {
    // SAFETY: null or a live control block, as the caller guarantees.
    let control = unsafe { ud.cast::<VmControl>().as_ref() };
    if nsize == 0 {
        // SAFETY: as the caller guarantees.
        unsafe { reallocate(ptr, osize, 0) };
        if let Some(control) = control {
            let allocated = control.allocated.load(Ordering::Relaxed);
            control
                .allocated
                .store(allocated.saturating_sub(osize), Ordering::Relaxed);
        }
        ptr::null_mut()
    } else if let Some(control) = control {
        let allocated = control.allocated.load(Ordering::Relaxed);
        let next_allocated = match allocated.saturating_sub(osize).checked_add(nsize) {
            Some(value) => value,
            None => return ptr::null_mut(),
        };
        let limit = control.memory_limit.load(Ordering::Relaxed);
        if limit != 0 && next_allocated > limit {
            return ptr::null_mut();
        }
        // SAFETY: as the caller guarantees.
        let result = unsafe { reallocate(ptr, osize, nsize) };
        if !result.is_null() {
            control.allocated.store(next_allocated, Ordering::Relaxed);
        }
        result
    } else {
        // SAFETY: as the caller guarantees.
        unsafe { reallocate(ptr, osize, nsize) }
    }
}

/// Frees `ptr` when `nsize` is zero, allocates when `ptr` is null, and reallocates otherwise.
/// Returns null when freeing or on failure.
///
/// # Safety
///
/// A non-null `ptr` is a live block from this function of `osize` bytes, which Luau guarantees.
unsafe fn reallocate(ptr: *mut c_void, osize: usize, nsize: usize) -> *mut c_void {
    // SAFETY: a live block was allocated with a valid layout of `osize` bytes and `LUAU_ALIGN`.
    let old =
        (!ptr.is_null()).then(|| unsafe { Layout::from_size_align_unchecked(osize, LUAU_ALIGN) });
    if nsize == 0 {
        if let Some(old) = old {
            // SAFETY: `ptr` is live with layout `old`.
            unsafe { std::alloc::dealloc(ptr.cast(), old) };
        }
        return ptr::null_mut();
    }
    let Ok(layout) = Layout::from_size_align(nsize, LUAU_ALIGN) else {
        return ptr::null_mut();
    };
    match old {
        // SAFETY: `layout` has a nonzero size.
        None => unsafe { std::alloc::alloc(layout).cast() },
        // SAFETY: `ptr` is live with layout `old`, and `layout` proves the new size is nonzero
        // and does not overflow `isize` once rounded up to the alignment.
        Some(old) => unsafe { std::alloc::realloc(ptr.cast(), old, layout.size()).cast() },
    }
}

pub(super) fn compile(source: &[u8], options: CompileOptions) -> Result<CompiledBytecode> {
    let mut compile_options = sys::lua_CompileOptions {
        optimizationLevel: options.optimization_level,
        debugLevel: options.debug_level,
        typeInfoLevel: options.type_info_level,
        mutableGlobals: ptr::null(),
        disabledBuiltins: ptr::null(),
    };
    let mut len = 0usize;
    // SAFETY: `source` is `source.len()` readable bytes, and the options and length outlive the
    // call.
    let ptr = unsafe {
        sys::luau_compile(
            source.as_ptr().cast(),
            source.len(),
            &mut compile_options,
            &mut len,
        )
    };
    let ptr = NonNull::new(ptr).ok_or(Error::Compile)?;
    Ok(CompiledBytecode { ptr, len })
}

/// Bytecode `luau_compile` made, which this owns.
pub(super) struct CompiledBytecode {
    ptr: NonNull<std::ffi::c_char>,
    len: usize,
}

impl CompiledBytecode {
    pub(super) fn as_bytes(&self) -> &[u8] {
        // SAFETY: `luau_compile` returned `len` bytes at `ptr`, which live until `self` drops.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr().cast(), self.len) }
    }
}

impl Drop for CompiledBytecode {
    fn drop(&mut self) {
        // SAFETY: bytecode `luau_compile` returned, freed once.
        unsafe { sys::luau_free_compiled_bytecode(self.ptr.as_ptr()) };
    }
}
