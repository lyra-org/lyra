// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{
    CStr,
    CString,
    NulError,
    c_int,
    c_void,
};
use std::future::Future;
use std::marker::PhantomData;
use std::mem;
use std::panic::{
    AssertUnwindSafe,
    catch_unwind,
};
use std::pin::Pin;
use std::ptr::{
    self,
    NonNull,
};
use std::rc::Rc;
use std::str::Utf8Error;
use std::task::{
    Context,
    Poll,
};

use harmony_luau_sys as sys;

/// Re-evaluate before stabilizing. The final error model may split compiler,
/// loader, runtime, and conversion failures into more detailed variants once
/// callbacks, userdata, and async execution are implemented.
#[derive(Debug)]
pub enum LuauError {
    StateAllocation,
    CallbackAllocation,
    InvalidChunkName(NulError),
    InvalidFunctionName(NulError),
    InvalidGlobalName(NulError),
    InvalidTableKey(NulError),
    StateMismatch,
    ThreadAllocation,
    ThreadMissing,
    ThreadYieldWithoutFuture,
    SchedulerMissing,
    CompileAllocation,
    Load(String),
    Runtime(String),
    Type {
        expected: &'static str,
        actual: String,
    },
    Utf8(Utf8Error),
}

impl std::fmt::Display for LuauError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StateAllocation => f.write_str("failed to allocate Luau state"),
            Self::CallbackAllocation => f.write_str("failed to allocate Luau callback state"),
            Self::InvalidChunkName(error) => write!(f, "invalid Luau chunk name: {error}"),
            Self::InvalidFunctionName(error) => write!(f, "invalid Luau function name: {error}"),
            Self::InvalidGlobalName(error) => write!(f, "invalid Luau global name: {error}"),
            Self::InvalidTableKey(error) => write!(f, "invalid Luau table key: {error}"),
            Self::StateMismatch => f.write_str("cannot move Luau values between states"),
            Self::ThreadAllocation => f.write_str("failed to allocate Luau thread"),
            Self::ThreadMissing => f.write_str("Luau thread reference is no longer valid"),
            Self::ThreadYieldWithoutFuture => {
                f.write_str("Luau thread yielded without a scheduled Rust future")
            }
            Self::SchedulerMissing => f.write_str("Luau value is not associated with a scheduler"),
            Self::CompileAllocation => f.write_str("Luau compiler returned a null bytecode buffer"),
            Self::Load(message) => write!(f, "failed to load Luau chunk: {message}"),
            Self::Runtime(message) => write!(f, "Luau runtime error: {message}"),
            Self::Type { expected, actual } => {
                write!(f, "expected Luau {expected}, got {actual}")
            }
            Self::Utf8(error) => write!(f, "Luau string is not valid UTF-8: {error}"),
        }
    }
}

impl std::error::Error for LuauError {}

/// Re-evaluate before stabilizing. This is the minimal compiler option set
/// needed for the initial direct-Luau path; the final API may group defaults or
/// expose additional Luau compile settings.
#[derive(Clone, Debug)]
pub struct CompileOptions {
    pub optimization_level: c_int,
    pub debug_level: c_int,
    pub type_info_level: c_int,
    pub coverage_level: c_int,
}

impl Default for CompileOptions {
    fn default() -> Self {
        Self {
            optimization_level: 1,
            debug_level: 1,
            type_info_level: 0,
            coverage_level: 0,
        }
    }
}

impl CompileOptions {
    fn to_sys(&self) -> sys::lua_CompileOptions {
        sys::lua_CompileOptions {
            optimizationLevel: self.optimization_level,
            debugLevel: self.debug_level,
            typeInfoLevel: self.type_info_level,
            coverageLevel: self.coverage_level,
            vectorLib: ptr::null(),
            vectorCtor: ptr::null(),
            vectorType: ptr::null(),
            mutableGlobals: ptr::null(),
            userdataTypes: ptr::null(),
            librariesWithKnownMembers: ptr::null(),
            libraryMemberTypeCb: None,
            libraryMemberConstantCb: None,
            disabledBuiltins: ptr::null(),
        }
    }
}

/// Re-evaluate before stabilizing. This borrowed state handle is the current
/// way to share stack operations between owned `Luau` values and callback
/// trampolines; the final public API may keep it mostly internal.
#[derive(Clone, Copy)]
pub struct LuauState<'lua> {
    state: NonNull<sys::lua_State>,
    _marker: PhantomData<&'lua sys::lua_State>,
}

impl<'lua> LuauState<'lua> {
    unsafe fn from_ptr(state: *mut sys::lua_State) -> Self {
        Self {
            state: unsafe { NonNull::new_unchecked(state) },
            _marker: PhantomData,
        }
    }

    pub fn as_ptr(self) -> *mut sys::lua_State {
        self.state.as_ptr()
    }

    fn same_state(self, other: Self) -> bool {
        self.as_ptr() == other.as_ptr()
    }
}

pub struct Luau {
    state: NonNull<sys::lua_State>,
    scheduler: Rc<RefCell<Scheduler>>,
}

impl Luau {
    pub fn new() -> Result<Self, LuauError> {
        let state = unsafe { sys::luaL_newstate() };
        let Some(state) = NonNull::new(state) else {
            return Err(LuauError::StateAllocation);
        };

        Ok(Self {
            state,
            scheduler: Rc::default(),
        })
    }

    pub fn with_standard_libraries() -> Result<Self, LuauError> {
        let luau = Self::new()?;
        luau.open_standard_libraries();
        Ok(luau)
    }

    pub fn open_standard_libraries(&self) {
        unsafe { sys::luaL_openlibs(self.as_ptr()) };
    }

    pub fn sandbox(&self) {
        unsafe { sys::luaL_sandbox(self.as_ptr()) };
    }

    pub fn sandbox_thread(&self) {
        unsafe { sys::luaL_sandboxthread(self.as_ptr()) };
    }

    pub fn state(&self) -> LuauState<'_> {
        LuauState {
            state: self.state,
            _marker: PhantomData,
        }
    }

    /// Re-evaluate before stabilizing. Raw-state escape hatches are useful
    /// during migration but should stay narrowly documented.
    pub fn as_ptr(&self) -> *mut sys::lua_State {
        self.state().as_ptr()
    }

    pub fn load(
        &self,
        chunk_name: &str,
        source: impl AsRef<[u8]>,
    ) -> Result<LuauFunction<'_>, LuauError> {
        self.load_with_options(chunk_name, source, &CompileOptions::default())
    }

    pub fn load_with_options(
        &self,
        chunk_name: &str,
        source: impl AsRef<[u8]>,
        options: &CompileOptions,
    ) -> Result<LuauFunction<'_>, LuauError> {
        let bytecode = compile(source.as_ref(), options)?;
        self.load_bytecode(chunk_name, &bytecode)
    }

    pub fn exec(&self, chunk_name: &str, source: impl AsRef<[u8]>) -> Result<(), LuauError> {
        self.eval(chunk_name, source)
    }

    /// Re-evaluate before stabilizing. This is a convenient `load` + `call`
    /// path for early typed-return work; the final API may route evaluation
    /// through a richer chunk builder.
    pub fn eval<'lua, R>(
        &'lua self,
        chunk_name: &str,
        source: impl AsRef<[u8]>,
    ) -> Result<R, LuauError>
    where
        R: FromLuauMulti<'lua>,
    {
        let function = self.load(chunk_name, source)?;
        function.call(())
    }

    /// Re-evaluate before stabilizing. Direct global mutation is useful for
    /// tests and bootstrapping, but module installation will likely get a
    /// narrower table/module builder API.
    pub fn set_global<V>(&self, name: &str, value: V) -> Result<(), LuauError>
    where
        V: IntoLuau,
    {
        let name = CString::new(name).map_err(LuauError::InvalidGlobalName)?;
        let top = unsafe { sys::lua_gettop(self.as_ptr()) };
        if let Err(error) = value.push_to_stack(self.state()) {
            unsafe { sys::lua_settop(self.as_ptr(), top) };
            return Err(error);
        }

        unsafe { sys::lua_setglobal(self.as_ptr(), name.as_ptr()) };
        Ok(())
    }

    /// Re-evaluate before stabilizing. This is intentionally small while the
    /// value model is still scalar-only.
    pub fn global<'lua, T>(&'lua self, name: &str) -> Result<T, LuauError>
    where
        T: FromLuau<'lua>,
    {
        let name = CString::new(name).map_err(LuauError::InvalidGlobalName)?;
        unsafe { sys::lua_getglobal(self.as_ptr(), name.as_ptr()) };

        let value = unsafe { T::from_stack(self.state(), -1) };
        unsafe { sys::lua_pop(self.as_ptr(), 1) };
        value
    }

    /// Re-evaluate before stabilizing. Tables need array/meta-table/userdata
    /// support before this becomes the main module construction path.
    pub fn create_table(&self) -> LuauTable<'_> {
        unsafe { sys::lua_newtable(self.as_ptr()) };
        let reference = unsafe { sys::lua_ref(self.as_ptr(), -1) };
        unsafe { sys::lua_pop(self.as_ptr(), 1) };

        LuauTable {
            state: self.state(),
            reference,
            scheduler: Some(Rc::clone(&self.scheduler)),
        }
    }

    /// Re-evaluate before stabilizing. This is synchronous-only and intentionally
    /// avoids userdata/upvalue conveniences until the callback ABI settles.
    pub fn create_function<A, F, R>(
        &self,
        debug_name: &str,
        callback: F,
    ) -> Result<LuauFunction<'_>, LuauError>
    where
        A: 'static,
        F: for<'lua> Fn(LuauState<'lua>, A) -> Result<R, LuauError> + 'static,
        R: IntoLuauMulti + 'static,
        for<'lua> A: FromLuauMulti<'lua>,
    {
        create_callback(
            self.state(),
            debug_name,
            CallbackKind::Sync(Box::new(TypedCallback::<A, F, R> {
                callback,
                _marker: PhantomData,
            })),
        )
    }

    /// Re-evaluate before stabilizing. This is the first direct scheduler
    /// entrypoint: it erases scheduler/yield plumbing but still uses a typed
    /// adapter for argument and result conversion.
    pub fn create_async_function<A, F, R, Fut>(
        &self,
        debug_name: &str,
        callback: F,
    ) -> Result<LuauFunction<'_>, LuauError>
    where
        A: 'static,
        F: for<'lua> Fn(LuauState<'lua>, A) -> Fut + 'static,
        R: IntoLuauMulti + 'static,
        Fut: Future<Output = Result<R, LuauError>> + 'static,
        for<'lua> A: FromLuauMulti<'lua>,
    {
        create_callback(
            self.state(),
            debug_name,
            CallbackKind::Async {
                scheduler: Rc::clone(&self.scheduler),
                callback: Box::new(TypedAsyncCallback::<A, F, R, Fut> {
                    callback,
                    _marker: PhantomData,
                }),
            },
        )
    }

    /// Re-evaluate before stabilizing. This coroutine future is intentionally
    /// minimal; cancellation, timeouts, and host executor integration should
    /// build on top of it rather than duplicating scheduler paths.
    pub fn spawn<'lua, R>(
        &'lua self,
        chunk_name: &str,
        source: impl AsRef<[u8]>,
    ) -> Result<LuauThread<'lua, R>, LuauError>
    where
        R: FromLuauMulti<'lua>,
    {
        let function = self.load(chunk_name, source)?;
        self.spawn_function(function, ())
    }

    /// Re-evaluate before stabilizing. Argument support is currently routed
    /// through the same tuple conversion as direct calls.
    pub fn spawn_function<'lua, A, R>(
        &'lua self,
        function: LuauFunction<'lua>,
        args: A,
    ) -> Result<LuauThread<'lua, R>, LuauError>
    where
        A: IntoLuauMulti,
        R: FromLuauMulti<'lua>,
    {
        let thread = unsafe { sys::lua_newthread(self.as_ptr()) };
        let Some(thread) = NonNull::new(thread) else {
            return Err(LuauError::ThreadAllocation);
        };

        let reference = unsafe { sys::lua_ref(self.as_ptr(), -1) };
        unsafe { sys::lua_pop(self.as_ptr(), 1) };

        function.push_to_thread(self.state(), thread.as_ptr())?;
        args.push_to_stack(unsafe { LuauState::from_ptr(thread.as_ptr()) })?;
        Ok(LuauThread {
            luau: self,
            thread,
            reference,
            next_arg_count: A::VALUE_COUNT,
            waiting: false,
            done: false,
            _marker: PhantomData,
        })
    }

    fn load_bytecode(
        &self,
        chunk_name: &str,
        bytecode: &[u8],
    ) -> Result<LuauFunction<'_>, LuauError> {
        let chunk_name = CString::new(chunk_name).map_err(LuauError::InvalidChunkName)?;
        let status = unsafe {
            sys::luau_load(
                self.as_ptr(),
                chunk_name.as_ptr(),
                bytecode.as_ptr().cast(),
                bytecode.len(),
                0,
            )
        };

        if status != sys::LUA_OK {
            return Err(LuauError::Load(unsafe { pop_string(self.as_ptr()) }));
        }

        let reference = unsafe { sys::lua_ref(self.as_ptr(), -1) };
        unsafe { sys::lua_pop(self.as_ptr(), 1) };

        Ok(LuauFunction {
            state: self.state(),
            reference,
        })
    }
}

impl Drop for Luau {
    fn drop(&mut self) {
        unsafe { sys::lua_close(self.as_ptr()) };
    }
}

pub struct LuauFunction<'lua> {
    state: LuauState<'lua>,
    reference: c_int,
}

impl<'lua> LuauFunction<'lua> {
    /// Re-evaluate as argument and multi-return conversion matures. The final
    /// call API may use a richer stack/value abstraction.
    pub fn call<A, R>(&self, args: A) -> Result<R, LuauError>
    where
        A: IntoLuauMulti,
        R: FromLuauMulti<'lua>,
    {
        let top = unsafe { sys::lua_gettop(self.state.as_ptr()) };
        unsafe { sys::lua_getref(self.state.as_ptr(), self.reference) };

        if let Err(error) = args.push_to_stack(self.state) {
            unsafe { sys::lua_settop(self.state.as_ptr(), top) };
            return Err(error);
        }

        let status =
            unsafe { sys::lua_pcall(self.state.as_ptr(), A::VALUE_COUNT, R::VALUE_COUNT, 0) };
        if status != sys::LUA_OK {
            return Err(LuauError::Runtime(unsafe {
                pop_string(self.state.as_ptr())
            }));
        }

        let result = unsafe { R::from_stack(self.state, -R::VALUE_COUNT) };
        if R::VALUE_COUNT > 0 {
            unsafe { sys::lua_pop(self.state.as_ptr(), R::VALUE_COUNT) };
        }
        result
    }

    fn push_to_thread(
        &self,
        source_state: LuauState<'_>,
        thread: *mut sys::lua_State,
    ) -> Result<(), LuauError> {
        if !self.state.same_state(source_state) {
            return Err(LuauError::StateMismatch);
        }

        unsafe { sys::lua_getref(source_state.as_ptr(), self.reference) };
        unsafe { sys::lua_xmove(source_state.as_ptr(), thread, 1) };
        Ok(())
    }
}

/// Re-evaluate before stabilizing. This is the first coroutine/scheduler
/// bridge; the final version should expose cancellation and host wake
/// integration deliberately.
pub struct LuauThread<'lua, R> {
    luau: &'lua Luau,
    thread: NonNull<sys::lua_State>,
    reference: c_int,
    next_arg_count: c_int,
    waiting: bool,
    done: bool,
    _marker: PhantomData<R>,
}

impl<R> Unpin for LuauThread<'_, R> {}

impl<'lua, R> Future for LuauThread<'lua, R>
where
    R: FromLuauMulti<'lua>,
{
    type Output = Result<R, LuauError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(Err(LuauError::ThreadMissing));
        }

        loop {
            if this.waiting {
                match this
                    .luau
                    .scheduler
                    .borrow_mut()
                    .poll_thread(this.thread.as_ptr(), context)
                {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(count)) => {
                        this.waiting = false;
                        this.next_arg_count = count;
                    }
                    Poll::Ready(Err(error)) => {
                        this.waiting = false;
                        push_error_string(this.thread.as_ptr(), error);
                        let status = unsafe {
                            sys::lua_resumeerror(this.thread.as_ptr(), this.luau.as_ptr())
                        };
                        match status {
                            sys::LUA_OK => {
                                this.done = true;
                                let state = unsafe { LuauState::from_ptr(this.thread.as_ptr()) };
                                let result = unsafe { R::from_stack(state, -R::VALUE_COUNT) };
                                if R::VALUE_COUNT > 0 {
                                    unsafe { sys::lua_pop(this.thread.as_ptr(), R::VALUE_COUNT) };
                                }
                                return Poll::Ready(result);
                            }
                            sys::LUA_YIELD => {
                                this.waiting = true;
                                continue;
                            }
                            _ => {
                                this.done = true;
                                return Poll::Ready(Err(LuauError::Runtime(unsafe {
                                    pop_string(this.thread.as_ptr())
                                })));
                            }
                        }
                    }
                }
            }

            let status = unsafe {
                sys::lua_resume(
                    this.thread.as_ptr(),
                    this.luau.as_ptr(),
                    this.next_arg_count,
                )
            };
            this.next_arg_count = 0;

            match status {
                sys::LUA_OK => {
                    this.done = true;
                    let state = unsafe { LuauState::from_ptr(this.thread.as_ptr()) };
                    let result = unsafe { R::from_stack(state, -R::VALUE_COUNT) };
                    if R::VALUE_COUNT > 0 {
                        unsafe { sys::lua_pop(this.thread.as_ptr(), R::VALUE_COUNT) };
                    }
                    return Poll::Ready(result);
                }
                sys::LUA_YIELD => {
                    this.waiting = true;
                    match this
                        .luau
                        .scheduler
                        .borrow_mut()
                        .poll_thread(this.thread.as_ptr(), context)
                    {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Ok(count)) => {
                            this.waiting = false;
                            this.next_arg_count = count;
                        }
                        Poll::Ready(Err(error)) => {
                            this.waiting = false;
                            push_error_string(this.thread.as_ptr(), error);
                            let status = unsafe {
                                sys::lua_resumeerror(this.thread.as_ptr(), this.luau.as_ptr())
                            };
                            match status {
                                sys::LUA_OK => {
                                    this.done = true;
                                    let state =
                                        unsafe { LuauState::from_ptr(this.thread.as_ptr()) };
                                    let result = unsafe { R::from_stack(state, -R::VALUE_COUNT) };
                                    if R::VALUE_COUNT > 0 {
                                        unsafe {
                                            sys::lua_pop(this.thread.as_ptr(), R::VALUE_COUNT)
                                        };
                                    }
                                    return Poll::Ready(result);
                                }
                                sys::LUA_YIELD => continue,
                                _ => {
                                    this.done = true;
                                    return Poll::Ready(Err(LuauError::Runtime(unsafe {
                                        pop_string(this.thread.as_ptr())
                                    })));
                                }
                            }
                        }
                    }
                }
                _ => {
                    this.done = true;
                    return Poll::Ready(Err(LuauError::Runtime(unsafe {
                        pop_string(this.thread.as_ptr())
                    })));
                }
            }
        }
    }
}

impl<R> Drop for LuauThread<'_, R> {
    fn drop(&mut self) {
        self.luau
            .scheduler
            .borrow_mut()
            .cancel_thread(self.thread.as_ptr());
        unsafe { sys::lua_unref(self.luau.as_ptr(), self.reference) };
    }
}

pub struct LuauTable<'lua> {
    state: LuauState<'lua>,
    reference: c_int,
    scheduler: Option<Rc<RefCell<Scheduler>>>,
}

impl<'lua> LuauTable<'lua> {
    /// Re-evaluate before stabilizing. String-only table keys are enough for
    /// module bootstrapping, but array keys and metatable operations will be
    /// needed before this can replace all existing table usage.
    pub fn set<V>(&self, key: &str, value: V) -> Result<(), LuauError>
    where
        V: IntoLuau,
    {
        let key = CString::new(key).map_err(LuauError::InvalidTableKey)?;
        let top = unsafe { sys::lua_gettop(self.state.as_ptr()) };
        unsafe { sys::lua_getref(self.state.as_ptr(), self.reference) };

        if let Err(error) = value.push_to_stack(self.state) {
            unsafe { sys::lua_settop(self.state.as_ptr(), top) };
            return Err(error);
        }

        unsafe { sys::lua_setfield(self.state.as_ptr(), -2, key.as_ptr()) };
        unsafe { sys::lua_settop(self.state.as_ptr(), top) };
        Ok(())
    }

    /// Re-evaluate before stabilizing. This mirrors `set` and is limited to
    /// string-keyed fields while the table API is still intentionally small.
    pub fn get<T>(&self, key: &str) -> Result<T, LuauError>
    where
        T: FromLuau<'lua>,
    {
        let key = CString::new(key).map_err(LuauError::InvalidTableKey)?;
        let top = unsafe { sys::lua_gettop(self.state.as_ptr()) };
        unsafe { sys::lua_getref(self.state.as_ptr(), self.reference) };
        unsafe { sys::lua_getfield(self.state.as_ptr(), -1, key.as_ptr()) };

        let value = unsafe { T::from_stack(self.state, -1) };
        unsafe { sys::lua_settop(self.state.as_ptr(), top) };
        value
    }

    /// Re-evaluate before stabilizing. This mirrors `Luau::create_function`
    /// for early module-building experiments; nested-path installation will
    /// likely live above this primitive.
    pub fn set_function<A, F, R>(
        &self,
        key: &str,
        debug_name: &str,
        callback: F,
    ) -> Result<(), LuauError>
    where
        A: 'static,
        F: for<'state> Fn(LuauState<'state>, A) -> Result<R, LuauError> + 'static,
        R: IntoLuauMulti + 'static,
        for<'state> A: FromLuauMulti<'state>,
    {
        let function = create_callback(
            self.state,
            debug_name,
            CallbackKind::Sync(Box::new(TypedCallback::<A, F, R> {
                callback,
                _marker: PhantomData,
            })),
        )?;
        self.set(key, function)
    }

    /// Re-evaluate before stabilizing. This is table-level async callback
    /// installation over the same erased scheduler path as `Luau`.
    pub fn set_async_function<A, F, R, Fut>(
        &self,
        key: &str,
        debug_name: &str,
        callback: F,
    ) -> Result<(), LuauError>
    where
        A: 'static,
        F: for<'state> Fn(LuauState<'state>, A) -> Fut + 'static,
        R: IntoLuauMulti + 'static,
        Fut: Future<Output = Result<R, LuauError>> + 'static,
        for<'state> A: FromLuauMulti<'state>,
    {
        let Some(scheduler) = &self.scheduler else {
            return Err(LuauError::SchedulerMissing);
        };

        let function = create_callback(
            self.state,
            debug_name,
            CallbackKind::Async {
                scheduler: Rc::clone(scheduler),
                callback: Box::new(TypedAsyncCallback::<A, F, R, Fut> {
                    callback,
                    _marker: PhantomData,
                }),
            },
        )?;
        self.set(key, function)
    }
}

/// Re-evaluate before stabilizing. This is the first pass at stack conversion;
/// it may be replaced by a broader value/stack API when tables, functions, and
/// userdata are represented directly.
pub trait FromLuau<'lua>: Sized {
    /// # Safety
    ///
    /// `index` must refer to a valid stack slot in `state`, and the value at
    /// that slot must remain on the stack for the duration of conversion.
    unsafe fn from_stack(state: LuauState<'lua>, index: c_int) -> Result<Self, LuauError>;
}

/// Re-evaluate before stabilizing. This tuple-based multi-value conversion is
/// deliberately small while callbacks, variadics, and richer value references
/// are still being designed.
pub trait FromLuauMulti<'lua>: Sized {
    const VALUE_COUNT: c_int;

    /// # Safety
    ///
    /// `index` must refer to the first stack slot in the value sequence, and
    /// the following `VALUE_COUNT` slots must remain on the stack for the
    /// duration of conversion.
    unsafe fn from_stack(state: LuauState<'lua>, index: c_int) -> Result<Self, LuauError>;
}

impl<'lua> FromLuauMulti<'lua> for () {
    const VALUE_COUNT: c_int = 0;

    unsafe fn from_stack(_state: LuauState<'lua>, _index: c_int) -> Result<Self, LuauError> {
        Ok(())
    }
}

impl<'lua, T> FromLuauMulti<'lua> for T
where
    T: FromLuau<'lua>,
{
    const VALUE_COUNT: c_int = 1;

    unsafe fn from_stack(state: LuauState<'lua>, index: c_int) -> Result<Self, LuauError> {
        unsafe { T::from_stack(state, index) }
    }
}

macro_rules! impl_from_luau_multi_tuple {
    ($count:expr, $first:ident $(, $rest:ident)+) => {
        impl<'lua, $first, $($rest),+> FromLuauMulti<'lua> for ($first, $($rest,)+)
        where
            $first: FromLuau<'lua>,
            $($rest: FromLuau<'lua>),+
        {
            const VALUE_COUNT: c_int = $count;

            #[allow(non_snake_case)]
            unsafe fn from_stack(
                state: LuauState<'lua>,
                index: c_int,
            ) -> Result<Self, LuauError> {
                let mut next_index = index;
                let $first = unsafe { $first::from_stack(state, next_index)? };
                next_index += 1;
                $(
                    let $rest = unsafe { $rest::from_stack(state, next_index)? };
                    next_index += 1;
                )+
                let _ = next_index;
                Ok(($first, $($rest,)+))
            }
        }
    };
}

impl_from_luau_multi_tuple!(2, A, B);
impl_from_luau_multi_tuple!(3, A, B, C);
impl_from_luau_multi_tuple!(4, A, B, C, D);

impl<'lua> FromLuau<'lua> for bool {
    unsafe fn from_stack(state: LuauState<'lua>, index: c_int) -> Result<Self, LuauError> {
        unsafe { expect_type(state.as_ptr(), index, sys::LUA_TBOOLEAN, "boolean")? };
        Ok(unsafe { sys::lua_toboolean(state.as_ptr(), index) } != 0)
    }
}

impl<'lua> FromLuau<'lua> for f64 {
    unsafe fn from_stack(state: LuauState<'lua>, index: c_int) -> Result<Self, LuauError> {
        let mut is_number = 0;
        let value = unsafe { sys::lua_tonumberx(state.as_ptr(), index, &mut is_number) };
        if is_number == 0 {
            return Err(unsafe { type_error(state.as_ptr(), index, "number") });
        }
        Ok(value)
    }
}

impl<'lua> FromLuau<'lua> for i32 {
    unsafe fn from_stack(state: LuauState<'lua>, index: c_int) -> Result<Self, LuauError> {
        let mut is_integer = 0;
        let value = unsafe { sys::lua_tointegerx(state.as_ptr(), index, &mut is_integer) };
        if is_integer == 0 {
            return Err(unsafe { type_error(state.as_ptr(), index, "integer") });
        }
        Ok(value)
    }
}

impl<'lua> FromLuau<'lua> for i64 {
    unsafe fn from_stack(state: LuauState<'lua>, index: c_int) -> Result<Self, LuauError> {
        let mut is_integer = 0;
        let value = unsafe { sys::lua_tointeger64(state.as_ptr(), index, &mut is_integer) };
        if is_integer == 0 {
            return Err(unsafe { type_error(state.as_ptr(), index, "integer") });
        }
        Ok(value)
    }
}

impl<'lua> FromLuau<'lua> for String {
    unsafe fn from_stack(state: LuauState<'lua>, index: c_int) -> Result<Self, LuauError> {
        let bytes = unsafe { stack_bytes(state.as_ptr(), index)? };
        String::from_utf8(bytes).map_err(|error| LuauError::Utf8(error.utf8_error()))
    }
}

impl<'lua> FromLuau<'lua> for Vec<u8> {
    unsafe fn from_stack(state: LuauState<'lua>, index: c_int) -> Result<Self, LuauError> {
        unsafe { stack_bytes(state.as_ptr(), index) }
    }
}

impl<'lua, T> FromLuau<'lua> for Option<T>
where
    T: FromLuau<'lua>,
{
    unsafe fn from_stack(state: LuauState<'lua>, index: c_int) -> Result<Self, LuauError> {
        if unsafe { sys::lua_type(state.as_ptr(), index) } == sys::LUA_TNIL {
            Ok(None)
        } else {
            unsafe { T::from_stack(state, index) }.map(Some)
        }
    }
}

impl<'lua> FromLuau<'lua> for LuauFunction<'lua> {
    unsafe fn from_stack(state: LuauState<'lua>, index: c_int) -> Result<Self, LuauError> {
        unsafe { expect_type(state.as_ptr(), index, sys::LUA_TFUNCTION, "function")? };
        let reference = unsafe { sys::lua_ref(state.as_ptr(), index) };
        Ok(Self { state, reference })
    }
}

impl<'lua> FromLuau<'lua> for LuauTable<'lua> {
    unsafe fn from_stack(state: LuauState<'lua>, index: c_int) -> Result<Self, LuauError> {
        unsafe { expect_type(state.as_ptr(), index, sys::LUA_TTABLE, "table")? };
        let reference = unsafe { sys::lua_ref(state.as_ptr(), index) };
        Ok(Self {
            state,
            reference,
            scheduler: None,
        })
    }
}

/// Re-evaluate before stabilizing. This mirrors the initial return-conversion
/// traits but only covers simple scalar pushes for now.
pub trait IntoLuau: Sized {
    fn push_to_stack(self, state: LuauState<'_>) -> Result<(), LuauError>;
}

/// Re-evaluate before stabilizing. Tuple-based argument pushing is enough for
/// the first direct call path, but callbacks and variadics may need a more
/// explicit multi-value container.
pub trait IntoLuauMulti: Sized {
    const VALUE_COUNT: c_int;

    fn push_to_stack(self, state: LuauState<'_>) -> Result<(), LuauError>;
}

impl IntoLuauMulti for () {
    const VALUE_COUNT: c_int = 0;

    fn push_to_stack(self, _state: LuauState<'_>) -> Result<(), LuauError> {
        Ok(())
    }
}

impl<T> IntoLuauMulti for T
where
    T: IntoLuau,
{
    const VALUE_COUNT: c_int = 1;

    fn push_to_stack(self, state: LuauState<'_>) -> Result<(), LuauError> {
        IntoLuau::push_to_stack(self, state)
    }
}

impl IntoLuau for bool {
    fn push_to_stack(self, state: LuauState<'_>) -> Result<(), LuauError> {
        unsafe { sys::lua_pushboolean(state.as_ptr(), i32::from(self)) };
        Ok(())
    }
}

impl IntoLuau for f64 {
    fn push_to_stack(self, state: LuauState<'_>) -> Result<(), LuauError> {
        unsafe { sys::lua_pushnumber(state.as_ptr(), self) };
        Ok(())
    }
}

impl IntoLuau for i32 {
    fn push_to_stack(self, state: LuauState<'_>) -> Result<(), LuauError> {
        unsafe { sys::lua_pushinteger(state.as_ptr(), self) };
        Ok(())
    }
}

impl IntoLuau for i64 {
    fn push_to_stack(self, state: LuauState<'_>) -> Result<(), LuauError> {
        unsafe { sys::lua_pushinteger64(state.as_ptr(), self) };
        Ok(())
    }
}

impl IntoLuau for &str {
    fn push_to_stack(self, state: LuauState<'_>) -> Result<(), LuauError> {
        unsafe { sys::lua_pushlstring(state.as_ptr(), self.as_ptr().cast(), self.len()) };
        Ok(())
    }
}

impl IntoLuau for String {
    fn push_to_stack(self, state: LuauState<'_>) -> Result<(), LuauError> {
        IntoLuau::push_to_stack(self.as_str(), state)
    }
}

impl IntoLuau for &[u8] {
    fn push_to_stack(self, state: LuauState<'_>) -> Result<(), LuauError> {
        unsafe { sys::lua_pushlstring(state.as_ptr(), self.as_ptr().cast(), self.len()) };
        Ok(())
    }
}

impl IntoLuau for Vec<u8> {
    fn push_to_stack(self, state: LuauState<'_>) -> Result<(), LuauError> {
        IntoLuau::push_to_stack(self.as_slice(), state)
    }
}

impl<T> IntoLuau for Option<T>
where
    T: IntoLuau,
{
    fn push_to_stack(self, state: LuauState<'_>) -> Result<(), LuauError> {
        match self {
            Some(value) => IntoLuau::push_to_stack(value, state),
            None => {
                unsafe { sys::lua_pushnil(state.as_ptr()) };
                Ok(())
            }
        }
    }
}

impl IntoLuau for &LuauFunction<'_> {
    fn push_to_stack(self, state: LuauState<'_>) -> Result<(), LuauError> {
        if !self.state.same_state(state) {
            return Err(LuauError::StateMismatch);
        }

        unsafe { sys::lua_getref(state.as_ptr(), self.reference) };
        Ok(())
    }
}

impl IntoLuau for LuauFunction<'_> {
    fn push_to_stack(self, state: LuauState<'_>) -> Result<(), LuauError> {
        IntoLuau::push_to_stack(&self, state)
    }
}

impl IntoLuau for &LuauTable<'_> {
    fn push_to_stack(self, state: LuauState<'_>) -> Result<(), LuauError> {
        if !self.state.same_state(state) {
            return Err(LuauError::StateMismatch);
        }

        unsafe { sys::lua_getref(state.as_ptr(), self.reference) };
        Ok(())
    }
}

impl IntoLuau for LuauTable<'_> {
    fn push_to_stack(self, state: LuauState<'_>) -> Result<(), LuauError> {
        IntoLuau::push_to_stack(&self, state)
    }
}

macro_rules! impl_into_luau_multi_tuple {
    ($count:expr, $($name:ident),+) => {
        impl<$($name),+> IntoLuauMulti for ($($name,)+)
        where
            $($name: IntoLuau),+
        {
            const VALUE_COUNT: c_int = $count;

            #[allow(non_snake_case)]
            fn push_to_stack(self, state: LuauState<'_>) -> Result<(), LuauError> {
                let ($($name,)+) = self;
                $(IntoLuau::push_to_stack($name, state)?;)+
                Ok(())
            }
        }
    };
}

impl_into_luau_multi_tuple!(2, A, B);
impl_into_luau_multi_tuple!(3, A, B, C);
impl_into_luau_multi_tuple!(4, A, B, C, D);

impl Drop for LuauFunction<'_> {
    fn drop(&mut self) {
        unsafe { sys::lua_unref(self.state.as_ptr(), self.reference) };
    }
}

impl Drop for LuauTable<'_> {
    fn drop(&mut self) {
        unsafe { sys::lua_unref(self.state.as_ptr(), self.reference) };
    }
}

trait Callback: 'static {
    fn call<'lua>(&self, state: LuauState<'lua>) -> Result<c_int, LuauError>;
}

trait AsyncCallback: 'static {
    fn call<'lua>(&self, state: LuauState<'lua>) -> Result<ScheduledFuture, LuauError>;
}

trait AsyncResult: 'static {
    fn push_to_stack(self: Box<Self>, state: LuauState<'_>) -> Result<c_int, LuauError>;
}

struct TypedCallback<A, F, R> {
    callback: F,
    _marker: PhantomData<fn(A) -> R>,
}

impl<A, F, R> Callback for TypedCallback<A, F, R>
where
    A: 'static,
    F: for<'lua> Fn(LuauState<'lua>, A) -> Result<R, LuauError> + 'static,
    R: IntoLuauMulti + 'static,
    for<'lua> A: FromLuauMulti<'lua>,
{
    fn call<'lua>(&self, state: LuauState<'lua>) -> Result<c_int, LuauError> {
        let args = unsafe { A::from_stack(state, 1)? };
        let value = (self.callback)(state, args)?;
        value.push_to_stack(state)?;
        Ok(R::VALUE_COUNT)
    }
}

struct TypedAsyncCallback<A, F, R, Fut> {
    callback: F,
    _marker: PhantomData<fn(A) -> (R, Fut)>,
}

impl<A, F, R, Fut> AsyncCallback for TypedAsyncCallback<A, F, R, Fut>
where
    A: 'static,
    F: for<'lua> Fn(LuauState<'lua>, A) -> Fut + 'static,
    R: IntoLuauMulti + 'static,
    Fut: Future<Output = Result<R, LuauError>> + 'static,
    for<'lua> A: FromLuauMulti<'lua>,
{
    fn call<'lua>(&self, state: LuauState<'lua>) -> Result<ScheduledFuture, LuauError> {
        let args = unsafe { A::from_stack(state, 1)? };
        let future = (self.callback)(state, args);
        Ok(Box::pin(async move {
            let value = future.await?;
            Ok(Box::new(TypedAsyncResult(value)) as Box<dyn AsyncResult>)
        }))
    }
}

struct TypedAsyncResult<R>(R);

impl<R> AsyncResult for TypedAsyncResult<R>
where
    R: IntoLuauMulti + 'static,
{
    fn push_to_stack(self: Box<Self>, state: LuauState<'_>) -> Result<c_int, LuauError> {
        let value = self.0;
        value.push_to_stack(state)?;
        Ok(R::VALUE_COUNT)
    }
}

type ScheduledFuture =
    Pin<Box<dyn Future<Output = Result<Box<dyn AsyncResult>, LuauError>> + 'static>>;

#[derive(Default)]
struct Scheduler {
    tasks: HashMap<usize, ScheduledFuture>,
}

impl Scheduler {
    fn schedule(&mut self, thread: *mut sys::lua_State, future: ScheduledFuture) {
        self.tasks.insert(thread as usize, future);
    }

    fn poll_thread(
        &mut self,
        thread: *mut sys::lua_State,
        context: &mut Context<'_>,
    ) -> Poll<Result<c_int, LuauError>> {
        let key = thread as usize;
        let Some(future) = self.tasks.get_mut(&key) else {
            return Poll::Ready(Err(LuauError::ThreadYieldWithoutFuture));
        };

        match future.as_mut().poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.tasks.remove(&key);
                Poll::Ready(
                    result.and_then(|value| {
                        value.push_to_stack(unsafe { LuauState::from_ptr(thread) })
                    }),
                )
            }
        }
    }

    fn cancel_thread(&mut self, thread: *mut sys::lua_State) {
        self.tasks.remove(&(thread as usize));
    }
}

enum CallbackKind {
    Sync(Box<dyn Callback>),
    Async {
        scheduler: Rc<RefCell<Scheduler>>,
        callback: Box<dyn AsyncCallback>,
    },
}

struct CallbackHolder {
    debug_name: CString,
    callback: CallbackKind,
}

fn create_callback<'lua>(
    state: LuauState<'lua>,
    debug_name: &str,
    callback: CallbackKind,
) -> Result<LuauFunction<'lua>, LuauError> {
    let debug_name = CString::new(debug_name).map_err(LuauError::InvalidFunctionName)?;
    let holder = CallbackHolder {
        debug_name,
        callback,
    };

    let storage = unsafe {
        sys::lua_newuserdatadtor(
            state.as_ptr(),
            mem::size_of::<CallbackHolder>(),
            Some(callback_drop),
        )
    };
    let Some(storage) = NonNull::new(storage.cast::<CallbackHolder>()) else {
        return Err(LuauError::CallbackAllocation);
    };

    unsafe { storage.as_ptr().write(holder) };
    let debug_name = unsafe { (*storage.as_ptr()).debug_name.as_ptr() };
    unsafe { sys::lua_pushcclosure(state.as_ptr(), Some(callback_trampoline), debug_name, 1) };
    let reference = unsafe { sys::lua_ref(state.as_ptr(), -1) };
    unsafe { sys::lua_pop(state.as_ptr(), 1) };

    Ok(LuauFunction { state, reference })
}

unsafe extern "C-unwind" fn callback_drop(userdata: *mut c_void) {
    unsafe { ptr::drop_in_place(userdata.cast::<CallbackHolder>()) };
}

unsafe extern "C-unwind" fn callback_trampoline(raw_state: *mut sys::lua_State) -> c_int {
    let state = unsafe { LuauState::from_ptr(raw_state) };
    let holder = unsafe {
        sys::lua_touserdata(raw_state, sys::lua_upvalueindex(1)).cast::<CallbackHolder>()
    };
    if holder.is_null() {
        unsafe { raise_luau_error(raw_state, "missing Rust callback state") };
    }

    let result = catch_unwind(AssertUnwindSafe(|| unsafe {
        match &(*holder).callback {
            CallbackKind::Sync(callback) => callback.call(state),
            CallbackKind::Async {
                scheduler,
                callback,
            } => {
                let future = callback.call(state)?;
                scheduler.borrow_mut().schedule(raw_state, future);
                Ok(sys::lua_yield(raw_state, 0))
            }
        }
    }));
    match result {
        Ok(Ok(count)) => count,
        Ok(Err(error)) => unsafe { raise_luau_error(raw_state, error) },
        Err(_) => unsafe { raise_luau_error(raw_state, "Rust callback panicked") },
    }
}

/// Re-evaluate before stabilizing. Raising callback errors through Luau's C API
/// currently relies on Luau copying the pushed error string before the long jump.
unsafe fn raise_luau_error(state: *mut sys::lua_State, error: impl std::fmt::Display) -> ! {
    push_error_string(state, error);
    unsafe { sys::lua_error(state) }
}

fn push_error_string(state: *mut sys::lua_State, error: impl std::fmt::Display) {
    let message = error.to_string();
    unsafe { sys::lua_pushlstring(state, message.as_ptr().cast(), message.len()) };
}

fn compile(source: &[u8], options: &CompileOptions) -> Result<Vec<u8>, LuauError> {
    let mut options = options.to_sys();
    let mut bytecode_len = 0usize;
    let bytecode = unsafe {
        sys::luau_compile(
            source.as_ptr().cast(),
            source.len(),
            &mut options,
            &mut bytecode_len,
        )
    };

    if bytecode.is_null() {
        return Err(LuauError::CompileAllocation);
    }

    let bytes = unsafe { std::slice::from_raw_parts(bytecode.cast::<u8>(), bytecode_len) }.to_vec();
    unsafe { sys::luau_free_compiled_bytecode(bytecode) };
    Ok(bytes)
}

unsafe fn pop_string(state: *mut sys::lua_State) -> String {
    let value = unsafe { stack_string(state, -1) };
    unsafe { sys::lua_pop(state, 1) };
    value
}

unsafe fn stack_string(state: *mut sys::lua_State, index: c_int) -> String {
    let mut len = 0usize;
    let ptr = unsafe { sys::lua_tolstring(state, index, &mut len) };
    if ptr.is_null() {
        return unsafe { type_name(state, index) };
    }

    let bytes = unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len) };
    String::from_utf8_lossy(bytes).into_owned()
}

unsafe fn stack_bytes(state: *mut sys::lua_State, index: c_int) -> Result<Vec<u8>, LuauError> {
    unsafe { expect_type(state, index, sys::LUA_TSTRING, "string")? };

    let mut len = 0usize;
    let ptr = unsafe { sys::lua_tolstring(state, index, &mut len) };
    if ptr.is_null() {
        return Err(unsafe { type_error(state, index, "string") });
    }

    Ok(unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len) }.to_vec())
}

unsafe fn expect_type(
    state: *mut sys::lua_State,
    index: c_int,
    expected_type: c_int,
    expected: &'static str,
) -> Result<(), LuauError> {
    let actual_type = unsafe { sys::lua_type(state, index) };
    if actual_type == expected_type {
        Ok(())
    } else {
        Err(unsafe { type_error(state, index, expected) })
    }
}

unsafe fn type_error(
    state: *mut sys::lua_State,
    index: c_int,
    expected: &'static str,
) -> LuauError {
    LuauError::Type {
        expected,
        actual: unsafe { type_name(state, index) },
    }
}

unsafe fn type_name(state: *mut sys::lua_State, index: c_int) -> String {
    let ty = unsafe { sys::lua_type(state, index) };
    let name = unsafe { sys::lua_typename(state, ty) };
    if name.is_null() {
        return format!("type {ty}");
    }
    unsafe { CStr::from_ptr(name) }
        .to_string_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{
        Context,
        Poll,
        Waker,
    };

    use super::{
        Luau,
        LuauError,
    };

    struct PendingOnce {
        pending: bool,
        value: f64,
    }

    impl Future for PendingOnce {
        type Output = Result<f64, LuauError>;

        fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            if self.pending {
                self.pending = false;
                Poll::Pending
            } else {
                Poll::Ready(Ok(self.value))
            }
        }
    }

    fn poll_once<T>(
        future: Pin<&mut impl Future<Output = Result<T, LuauError>>>,
    ) -> Poll<Result<T, LuauError>> {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        future.poll(&mut context)
    }

    #[test]
    fn evaluates_number_expression() {
        let luau = Luau::with_standard_libraries().expect("create Luau state");
        let value: f64 = luau
            .eval("unit/eval", "return 40 + 2")
            .expect("evaluate number");

        assert_eq!(value, 42.0);
    }

    #[test]
    fn evaluates_string_expression() {
        let luau = Luau::new().expect("create Luau state");
        let value: String = luau
            .eval("unit/string", "return 'harmony'")
            .expect("evaluate string");

        assert_eq!(value, "harmony");
    }

    #[test]
    fn evaluates_boolean_expression() {
        let luau = Luau::new().expect("create Luau state");
        let value: bool = luau
            .eval("unit/bool", "return true")
            .expect("evaluate bool");

        assert!(value);
    }

    #[test]
    fn reports_compile_error_from_load() {
        let luau = Luau::new().expect("create Luau state");
        let error = match luau.load("unit/syntax", "return function(") {
            Ok(_) => panic!("syntax error should fail load"),
            Err(error) => error,
        };

        assert!(matches!(error, LuauError::Load(_)));
        assert!(error.to_string().contains("Expected identifier"));
    }

    #[test]
    fn reports_runtime_error() {
        let luau = Luau::with_standard_libraries().expect("create Luau state");
        let error = luau
            .exec("unit/runtime", "error('boom')")
            .expect_err("runtime error should fail");

        assert!(matches!(error, LuauError::Runtime(_)));
        assert!(error.to_string().contains("boom"));
    }

    #[test]
    fn reads_and_writes_globals() {
        let luau = Luau::new().expect("create Luau state");
        luau.set_global("answer", 42.0).expect("set global");

        let value: f64 = luau.global("answer").expect("read global");

        assert_eq!(value, 42.0);
    }

    #[test]
    fn calls_function_with_arguments() {
        let luau = Luau::new().expect("create Luau state");
        luau.exec("unit/add-definition", "function add(a, b) return a + b end")
            .expect("define function");
        let add = luau
            .global::<super::LuauFunction<'_>>("add")
            .expect("read function");

        let value: f64 = add.call((20.0, 22.0)).expect("call function");

        assert_eq!(value, 42.0);
    }

    #[test]
    fn converts_nil_to_option() {
        let luau = Luau::new().expect("create Luau state");

        let value: Option<String> = luau.eval("unit/nil", "return nil").expect("evaluate nil");

        assert_eq!(value, None);
    }

    #[test]
    fn round_trips_byte_strings() {
        let luau = Luau::new().expect("create Luau state");
        luau.set_global("payload", vec![0, 1, 2, 255])
            .expect("set byte string");

        let value: Vec<u8> = luau.global("payload").expect("read byte string");

        assert_eq!(value, vec![0, 1, 2, 255]);
    }

    #[test]
    fn creates_and_reads_tables() {
        let luau = Luau::new().expect("create Luau state");
        let table = luau.create_table();
        table.set("name", "harmony").expect("set table field");
        table.set("enabled", true).expect("set table field");
        luau.set_global("module", &table).expect("set global");

        let name: String = table.get("name").expect("read table field");
        let enabled: bool = luau
            .eval("unit/table-field", "return module.enabled")
            .expect("read global table field");

        assert_eq!(name, "harmony");
        assert!(enabled);
    }

    #[test]
    fn calls_rust_function_from_luau() {
        let luau = Luau::new().expect("create Luau state");
        let add = luau
            .create_function("add", |_, (a, b): (f64, f64)| Ok(a + b))
            .expect("create Rust function");
        luau.set_global("add", &add).expect("set global");

        let value: f64 = luau
            .eval("unit/rust-callback", "return add(20, 22)")
            .expect("call Rust function");

        assert_eq!(value, 42.0);
    }

    #[test]
    fn sets_rust_function_on_table() {
        let luau = Luau::new().expect("create Luau state");
        let api = luau.create_table();
        api.set_function("answer", "api.answer", |_, ()| Ok(42.0))
            .expect("set table function");
        luau.set_global("api", &api).expect("set global");

        let value: f64 = luau
            .eval("unit/table-callback", "return api.answer()")
            .expect("call table function");

        assert_eq!(value, 42.0);
    }

    #[test]
    fn propagates_rust_callback_errors() {
        let luau = Luau::new().expect("create Luau state");
        let fail = luau
            .create_function("fail", |_, ()| -> Result<(), LuauError> {
                Err(LuauError::Runtime("boom".to_string()))
            })
            .expect("create Rust function");
        luau.set_global("fail", &fail).expect("set global");

        let error = luau
            .exec("unit/callback-error", "fail()")
            .expect_err("callback error should fail");

        assert!(matches!(error, LuauError::Runtime(_)));
        assert!(error.to_string().contains("boom"));
    }

    #[test]
    fn evaluates_multiple_returns() {
        let luau = Luau::new().expect("create Luau state");

        let value: (String, f64) = luau
            .eval("unit/multiple-returns", "return 'ok', 42")
            .expect("evaluate multiple returns");

        assert_eq!(value, ("ok".to_string(), 42.0));
    }

    #[test]
    fn runs_ready_async_rust_function_from_luau_thread() {
        let luau = Luau::new().expect("create Luau state");
        let add = luau
            .create_async_function(
                "add_async",
                |_, (a, b): (f64, f64)| async move { Ok(a + b) },
            )
            .expect("create async Rust function");
        luau.set_global("add_async", &add).expect("set global");
        let mut thread = Box::pin(
            luau.spawn::<f64>("unit/async-callback", "return add_async(20, 22)")
                .expect("spawn Luau thread"),
        );

        let value = match poll_once(thread.as_mut()) {
            Poll::Ready(Ok(value)) => value,
            Poll::Ready(Err(error)) => panic!("async callback failed: {error}"),
            Poll::Pending => panic!("ready async callback should complete in one poll"),
        };

        assert_eq!(value, 42.0);
    }

    #[test]
    fn keeps_luau_thread_yielded_while_async_callback_is_pending() {
        let luau = Luau::new().expect("create Luau state");
        let add = luau
            .create_async_function("add_async", |_, (a, b): (f64, f64)| PendingOnce {
                pending: true,
                value: a + b,
            })
            .expect("create async Rust function");
        luau.set_global("add_async", &add).expect("set global");
        let mut thread = Box::pin(
            luau.spawn::<f64>("unit/pending-async-callback", "return add_async(20, 22)")
                .expect("spawn Luau thread"),
        );

        assert!(matches!(poll_once(thread.as_mut()), Poll::Pending));
        let value = match poll_once(thread.as_mut()) {
            Poll::Ready(Ok(value)) => value,
            Poll::Ready(Err(error)) => panic!("async callback failed: {error}"),
            Poll::Pending => panic!("async callback should complete on second poll"),
        };

        assert_eq!(value, 42.0);
    }

    #[test]
    fn propagates_async_callback_errors() {
        let luau = Luau::new().expect("create Luau state");
        let fail = luau
            .create_async_function("fail_async", |_, ()| async {
                Err::<(), _>(LuauError::Runtime("boom".to_string()))
            })
            .expect("create async Rust function");
        luau.set_global("fail_async", &fail).expect("set global");
        let mut thread = Box::pin(
            luau.spawn::<()>("unit/async-callback-error", "fail_async()")
                .expect("spawn Luau thread"),
        );

        let error = match poll_once(thread.as_mut()) {
            Poll::Ready(Ok(())) => panic!("async callback error should fail"),
            Poll::Ready(Err(error)) => error,
            Poll::Pending => panic!("ready async callback should complete in one poll"),
        };

        assert!(matches!(error, LuauError::Runtime(_)));
        assert!(error.to_string().contains("boom"));
    }

    #[test]
    fn sets_async_rust_function_on_table() {
        let luau = Luau::new().expect("create Luau state");
        let api = luau.create_table();
        api.set_async_function("answer", "api.answer", |_, ()| async { Ok(42.0) })
            .expect("set async table function");
        luau.set_global("api", &api).expect("set global");
        let mut thread = Box::pin(
            luau.spawn::<f64>("unit/table-async-callback", "return api.answer()")
                .expect("spawn Luau thread"),
        );

        let value = match poll_once(thread.as_mut()) {
            Poll::Ready(Ok(value)) => value,
            Poll::Ready(Err(error)) => panic!("async callback failed: {error}"),
            Poll::Pending => panic!("ready async callback should complete in one poll"),
        };

        assert_eq!(value, 42.0);
    }
}
