use std::{
    alloc::Layout,
    any::{
        Any,
        TypeId,
        type_name,
    },
    cell::Cell,
    collections::HashMap,
    ffi::{
        CString,
        c_int,
        c_void,
    },
    fmt,
    future::Future,
    marker::PhantomData,
    mem,
    panic::{
        AssertUnwindSafe,
        catch_unwind,
    },
    ptr::{
        self,
        NonNull,
    },
    rc::{
        Rc,
        Weak as RcWeak,
    },
    sync::{
        Arc,
        Once,
        RwLock,
        atomic::{
            AtomicU64,
            AtomicUsize,
            Ordering,
        },
    },
    time::{
        Duration,
        Instant,
        SystemTime,
        UNIX_EPOCH,
    },
};

use harmony_luau_sys as sys;
use time::OffsetDateTime;

mod gateway;

pub type Result<T> = std::result::Result<T, Error>;

static NEXT_VM_ID: AtomicU64 = AtomicU64::new(1);
static LUAU_FLAGS: Once = Once::new();

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to create Luau VM")]
    VmCreate,
    #[error("string contains an interior nul byte")]
    Nul(#[from] std::ffi::NulError),
    #[error("Luau compile failed")]
    Compile,
    #[error("Luau load failed: {0}")]
    Load(String),
    #[error("Luau runtime failed: {0}")]
    Runtime(String),
    /// The VM's allocator failed, or an error value was exactly Luau's out-of-memory message, as
    /// when a native callback ran out of memory. Failing to grow the stack is a `Runtime` error,
    /// which Luau reports as "stack overflow".
    #[error("{}", gateway::MEMORY_ERROR)]
    OutOfMemory,
    #[error("registry reference belongs to VM {reference_vm}, not VM {actual_vm}")]
    VmMismatch { reference_vm: u64, actual_vm: u64 },
    #[error("argument `{name}` expected {expected}, got {actual}")]
    ArgumentType {
        name: Arc<str>,
        expected: &'static str,
        actual: &'static str,
    },
    #[error("argument `{0}` is missing")]
    MissingArgument(Arc<str>),
    #[error("argument `{name}` expected valid UTF-8: {error}")]
    InvalidUtf8Argument {
        name: Arc<str>,
        error: std::string::FromUtf8Error,
    },
    #[error("argument `{name}` expected valid UTF-8: {error}")]
    InvalidUtf8BorrowedArgument {
        name: Arc<str>,
        error: std::str::Utf8Error,
    },
    #[error("call context does not contain {}", type_name)]
    MissingContext { type_name: &'static str },
    #[error("VM data lock is poisoned")]
    VmDataPoisoned,
    #[error("thread data lock is poisoned")]
    ThreadDataPoisoned,
    #[error("userdata tag {tag} is outside Luau's supported tag range")]
    InvalidUserDataTag { tag: i32 },
    #[error("userdata tag {tag} is already registered for another Rust type")]
    UserDataTagMismatch { tag: i32 },
    #[error("no Luau userdata tags are available")]
    UserDataTagExhausted,
    #[error("userdata has tag {actual}, expected {expected}")]
    UserDataTypeMismatch { expected: i32, actual: i32 },
    #[error("Luau serialization failed: {0}")]
    Serialize(String),
}

/// A host error raised into Luau as a runtime error with its message. Running out of memory stays
/// [`Error::OutOfMemory`], even when the error carries context.
#[cfg(feature = "anyhow")]
impl From<anyhow::Error> for Error {
    fn from(error: anyhow::Error) -> Self {
        match error.downcast_ref::<Error>() {
            Some(Error::OutOfMemory) => Error::OutOfMemory,
            _ => Error::Runtime(error.to_string()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ModuleId(pub Arc<str>);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CapabilityId(pub Arc<str>);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct TaskGroupId(pub u64);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChunkOrigin {
    pub module: Option<ModuleId>,
    pub plugin: Option<Arc<str>>,
    pub path: Option<Arc<str>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceBytes(pub Arc<[u8]>);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct UserDataTag(i32);

impl UserDataTag {
    pub fn new(tag: i32) -> Result<Self> {
        if (0..sys::LUA_UTAG_LIMIT).contains(&tag) {
            Ok(Self(tag))
        } else {
            Err(Error::InvalidUserDataTag { tag })
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VmOptions {
    pub memory_limit: Option<usize>,
}

impl VmOptions {
    pub fn memory_limit(mut self, bytes: usize) -> Self {
        self.memory_limit = Some(bytes);
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompileOptions {
    pub optimization_level: i32,
    pub debug_level: i32,
    pub type_info_level: i32,
}

impl Default for CompileOptions {
    fn default() -> Self {
        Self {
            optimization_level: 1,
            debug_level: 1,
            type_info_level: 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chunk {
    pub source: SourceBytes,
    pub origin: ChunkOrigin,
    pub compile: CompileOptions,
}

impl Chunk {
    pub fn new(source: impl Into<Arc<[u8]>>, origin: ChunkOrigin) -> Self {
        Self {
            source: SourceBytes(source.into()),
            origin,
            compile: CompileOptions::default(),
        }
    }

    fn chunk_name(&self) -> CString {
        let label = self
            .origin
            .path
            .as_deref()
            .or(self.origin.plugin.as_deref())
            .or_else(|| self.origin.module.as_ref().map(|id| id.0.as_ref()))
            .unwrap_or("harmony-chunk");
        CString::new(format!("={label}")).expect("formatted chunk name has no nul")
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StandardLibraries {
    pub base: bool,
    pub coroutine: bool,
    pub table: bool,
    pub string: bool,
    pub math: bool,
    pub bit32: bool,
    pub os: bool,
}

impl StandardLibraries {
    pub fn none() -> Self {
        Self::default()
    }

    pub fn all_supported() -> Self {
        Self {
            base: true,
            coroutine: true,
            table: true,
            string: true,
            math: true,
            bit32: true,
            os: true,
        }
    }
}

#[derive(Clone)]
pub struct Vm {
    inner: Rc<VmInner>,
}

struct VmInner {
    id: u64,
    state: NonNull<sys::lua_State>,
    control: NonNull<VmControl>,
    data: VmData,
    userdata_tags: RwLock<HashMap<i32, TypeId>>,
    started_at: Instant,
}

impl Vm {
    pub fn new() -> Result<Self> {
        Self::with_options(VmOptions::default())
    }

    pub fn with_options(options: VmOptions) -> Result<Self> {
        LUAU_FLAGS.call_once(|| {
            // Keeps tables intact when growing or freezing them runs out of memory.
            let set = unsafe { sys::harmony_luau_setfflag(c"LuauTableRobustOom".as_ptr(), 1) };
            assert!(
                set != 0,
                "Luau no longer defines the LuauTableRobustOom flag"
            );
        });
        let control = Box::into_raw(Box::new(VmControl::new(options)));
        let state = unsafe { sys::lua_newstate(Some(alloc), control.cast()) };
        let state = match NonNull::new(state) {
            Some(state) => state,
            None => {
                unsafe {
                    drop(Box::from_raw(control));
                }
                return Err(Error::VmCreate);
            }
        };
        let control = NonNull::new(control).expect("Box::into_raw never returns null");
        unsafe {
            sys::lua_set_callback_userdata(state.as_ptr(), control.as_ptr().cast());
            (*sys::lua_callbacks(state.as_ptr())).userthread = Some(userthread);
        }
        Ok(Self {
            inner: Rc::new(VmInner {
                id: NEXT_VM_ID.fetch_add(1, Ordering::Relaxed),
                state,
                control,
                data: VmData::default(),
                userdata_tags: RwLock::new(HashMap::new()),
                started_at: Instant::now(),
            }),
        })
    }

    pub fn id(&self) -> u64 {
        self.inner.id
    }

    pub fn state_id(&self) -> usize {
        self.inner.state.as_ptr() as usize
    }

    pub fn data(&self) -> &VmData {
        &self.inner.data
    }

    pub fn memory_used(&self) -> usize {
        self.inner.control().allocated.load(Ordering::Relaxed)
    }

    pub fn memory_limit(&self) -> Option<usize> {
        let limit = self.inner.control().memory_limit.load(Ordering::Relaxed);
        (limit != 0).then_some(limit)
    }

    pub fn interrupt_after(&self, budget: Duration) -> InterruptBudgetGuard {
        let deadline = current_unix_millis()
            .saturating_add(budget.as_millis().min(u128::from(u64::MAX)) as u64);
        let previous_deadline = self.set_interrupt_deadline_millis(deadline);
        InterruptBudgetGuard {
            vm: self.clone(),
            previous_deadline,
        }
    }

    pub fn open_standard_libraries(&self, libraries: StandardLibraries) -> Result<()> {
        let opened = [
            (libraries.base, c""),
            (libraries.coroutine, c"coroutine"),
            (libraries.table, c"table"),
            (libraries.string, c"string"),
            (libraries.math, c"math"),
            (libraries.bit32, c"bit32"),
        ];
        for (_, name) in opened.into_iter().filter(|(open, _)| *open) {
            unsafe { gateway::open_library(self.inner.state.as_ptr(), name)? };
        }
        if libraries.base {
            let _guard = self.stack_guard();
            // Environment access would reach any function's globals,
            // including the `require` a host gives a chunk.
            for name in [c"getfenv", c"setfenv"] {
                unsafe {
                    gateway::push_nil(self.inner.state.as_ptr())?;
                    gateway::set_global(self.inner.state.as_ptr(), name)?;
                }
            }
        }
        if libraries.os {
            self.install_safe_os()?;
        }
        Ok(())
    }

    pub fn sandbox(&self) -> Result<()> {
        unsafe { gateway::sandbox(self.inner.state.as_ptr()) }
    }

    pub fn sandbox_thread(&self, thread: &Thread) -> Result<()> {
        if thread.vm_id() != self.inner.id {
            return Err(Error::VmMismatch {
                reference_vm: thread.vm_id(),
                actual_vm: self.inner.id,
            });
        }
        unsafe { gateway::sandbox_thread(thread.state().as_ptr()) }
    }

    pub fn load_chunk(&self, chunk: &Chunk) -> Result<Function> {
        let bytecode = compile(&chunk.source.0, chunk.compile)?;
        let guard = self.stack_guard();
        unsafe {
            gateway::load(
                self.inner.state.as_ptr(),
                &chunk.chunk_name(),
                bytecode.as_bytes(),
            )?
        };
        let reference = self.ref_top()?;
        drop(guard);
        Ok(Function {
            reference,
            origin: chunk.origin.clone(),
        })
    }

    /// Loads `chunk` with an environment of its own holding `globals`. Every
    /// other global falls through to the shared table, and writes go there,
    /// so only the named globals differ from a [`Vm::load_chunk`] chunk.
    /// Functions the chunk creates share its environment.
    pub fn load_chunk_with_globals(
        &self,
        chunk: &Chunk,
        globals: &[(&str, &Value)],
    ) -> Result<Function> {
        let bytecode = compile(&chunk.source.0, chunk.compile)?;
        let names = globals
            .iter()
            .map(|(name, _)| CString::new(*name))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let guard = self.stack_guard();
        let main = self.inner.state;
        // Luau gives a loaded chunk the environment of the state loading it.
        let loader =
            NonNull::new(unsafe { gateway::new_thread(main.as_ptr())? }).ok_or(Error::VmCreate)?;
        unsafe { gateway::create_table(loader.as_ptr(), 0, globals.len() as c_int)? };
        for ((_, value), name) in globals.iter().zip(&names) {
            self.push_value_to(loader, value)?;
            unsafe { gateway::raw_set_field(loader.as_ptr(), -2, name)? };
        }
        unsafe {
            let loader = loader.as_ptr();
            gateway::create_table(loader, 0, 2)?;
            gateway::push_value(loader, sys::LUA_GLOBALSINDEX)?;
            gateway::raw_set_field(loader, -2, c"__index")?;
            gateway::push_value(loader, sys::LUA_GLOBALSINDEX)?;
            gateway::raw_set_field(loader, -2, c"__newindex")?;
            gateway::set_readonly(loader, -1, true)?;
            gateway::set_metatable(loader, -2)?;
            sys::lua_replace(loader, sys::LUA_GLOBALSINDEX);
            gateway::load(loader, &chunk.chunk_name(), bytecode.as_bytes())?;
            gateway::xmove(loader, main.as_ptr(), 1)?;
        }
        let reference = self.ref_top()?;
        drop(guard);
        Ok(Function {
            reference,
            origin: chunk.origin.clone(),
        })
    }

    pub fn eval(&self, source: impl Into<Arc<[u8]>>, origin: ChunkOrigin) -> Result<Vec<Value>> {
        let function = self.load_chunk(&Chunk::new(source, origin))?;
        function.call(self, &[])
    }

    pub fn create_table(&self) -> Result<Table> {
        self.create_table_with_capacity(0, 0)
    }

    pub fn create_table_with_capacity(&self, array: i32, records: i32) -> Result<Table> {
        let guard = self.stack_guard();
        unsafe { gateway::create_table(self.inner.state.as_ptr(), array, records)? };
        let reference = self.ref_top()?;
        drop(guard);
        Ok(Table { reference })
    }

    pub fn create_buffer(&self, bytes: &[u8]) -> Result<Buffer> {
        let guard = self.stack_guard();
        unsafe { gateway::new_buffer(self.inner.state.as_ptr(), bytes)? };
        let reference = self.ref_top()?;
        drop(guard);
        Ok(Buffer { reference })
    }

    pub fn create_userdata<T>(&self, tag: UserDataTag, value: T) -> Result<UserData>
    where
        T: 'static,
    {
        self.register_userdata_tag::<T>(tag)?;
        let guard = self.stack_guard();
        unsafe {
            let ptr = gateway::new_userdata_tagged(
                self.inner.state.as_ptr(),
                mem::size_of::<T>(),
                tag.0,
            )?;
            ptr::write(ptr.cast::<T>(), value);
        }
        let reference = self.ref_top()?;
        drop(guard);
        Ok(UserData { reference, tag })
    }

    pub fn create_userdata_auto<T>(&self, value: T) -> Result<UserData>
    where
        T: 'static,
    {
        let tag = self.userdata_tag::<T>()?;
        self.create_userdata(tag, value)
    }

    pub fn create_function(&self, origin: ChunkOrigin, callback: NativeFn) -> Result<Function> {
        self.create_function_with_options(NativeFunctionOptions::new(origin), callback)
    }

    pub fn create_function_with_options(
        &self,
        options: NativeFunctionOptions,
        callback: NativeFn,
    ) -> Result<Function> {
        let guard = self.stack_guard();
        self.push_native_function_to(self.inner.state, &options, callback)?;
        let reference = self.ref_top()?;
        drop(guard);
        Ok(Function {
            reference,
            origin: options.origin,
        })
    }

    pub fn create_thread(&self, function: &Function) -> Result<Thread> {
        let guard = self.stack_guard();
        let state = unsafe { gateway::new_thread(self.inner.state.as_ptr())? };
        let state = NonNull::new(state).ok_or(Error::VmCreate)?;
        let reference = self.ref_top()?;
        self.push_registry(&function.reference)?;
        unsafe { gateway::xmove(self.inner.state.as_ptr(), state.as_ptr(), 1)? };
        drop(guard);
        Ok(Thread::new(
            Some(reference),
            state,
            self.inner.id,
            function.origin.clone(),
            false,
            false,
        ))
    }

    pub fn set_thread_call_context(
        &self,
        thread: &Thread,
        context: CallContext,
    ) -> Result<Option<CallContext>> {
        if thread.vm_id() != self.inner.id {
            return Err(Error::VmMismatch {
                reference_vm: thread.vm_id(),
                actual_vm: self.inner.id,
            });
        }
        self.replace_state_context(thread.state(), Some(context))
    }

    pub fn clear_thread_call_context(&self, thread: &Thread) -> Result<Option<CallContext>> {
        if thread.vm_id() != self.inner.id {
            return Err(Error::VmMismatch {
                reference_vm: thread.vm_id(),
                actual_vm: self.inner.id,
            });
        }
        self.replace_state_context(thread.state(), None)
    }

    /// The call context native calls on `thread` see.
    pub fn thread_call_context(&self, thread: &Thread) -> Result<Option<CallContext>> {
        if thread.vm_id() != self.inner.id {
            return Err(Error::VmMismatch {
                reference_vm: thread.vm_id(),
                actual_vm: self.inner.id,
            });
        }
        self.call_context_for_state(thread.state())
    }

    /// Runs `source` as [`Vm::eval`] does, as a call made in `context`.
    pub fn eval_with_context(
        &self,
        source: impl Into<Arc<[u8]>>,
        origin: ChunkOrigin,
        context: CallContext,
    ) -> Result<Vec<Value>> {
        let function = self.load_chunk(&Chunk::new(source, origin))?;
        function.call_with_context(self, &[], context)
    }

    fn replace_state_context(
        &self,
        state: NonNull<sys::lua_State>,
        context: Option<CallContext>,
    ) -> Result<Option<CallContext>> {
        let mut contexts = self
            .inner
            .control()
            .thread_contexts
            .write()
            .map_err(|_| Error::VmDataPoisoned)?;
        let key = state.as_ptr() as usize;
        Ok(match context {
            Some(context) => contexts.insert(key, context),
            None => contexts.remove(&key),
        })
    }

    fn call_context_for_state(
        &self,
        state: NonNull<sys::lua_State>,
    ) -> Result<Option<CallContext>> {
        self.inner
            .control()
            .thread_contexts
            .read()
            .map_err(|_| Error::VmDataPoisoned)
            .map(|contexts| contexts.get(&(state.as_ptr() as usize)).cloned())
    }

    pub fn set_global_table(&self, name: &str, table: &Table) -> Result<()> {
        let name = CString::new(name)?;
        let _guard = self.stack_guard();
        self.push_registry(&table.reference)?;
        unsafe { gateway::set_global(self.inner.state.as_ptr(), &name) }
    }

    pub fn set_global_function(&self, name: &str, function: &Function) -> Result<()> {
        let name = CString::new(name)?;
        let _guard = self.stack_guard();
        self.push_registry(&function.reference)?;
        unsafe { gateway::set_global(self.inner.state.as_ptr(), &name) }
    }

    fn top(&self) -> i32 {
        unsafe { sys::lua_gettop(self.inner.state.as_ptr()) }
    }

    fn stack_guard(&self) -> StackGuard<'_> {
        StackGuard::new(self.inner.state)
    }

    fn set_interrupt_deadline_millis(&self, deadline: u64) -> u64 {
        let previous = self
            .inner
            .control()
            .interrupt_deadline_millis
            .swap(deadline, Ordering::Release);
        unsafe {
            sys::lua_set_interrupt_callback(
                self.inner.state.as_ptr(),
                if deadline == 0 { None } else { Some(interrupt) },
            );
        }
        previous
    }

    fn register_userdata_tag<T>(&self, tag: UserDataTag) -> Result<()>
    where
        T: 'static,
    {
        let mut tags = self
            .inner
            .userdata_tags
            .write()
            .map_err(|_| Error::VmDataPoisoned)?;
        match tags.get(&tag.0) {
            Some(type_id) if *type_id == TypeId::of::<T>() => Ok(()),
            Some(_) => Err(Error::UserDataTagMismatch { tag: tag.0 }),
            None => {
                unsafe {
                    sys::lua_setuserdatadtor(
                        self.inner.state.as_ptr(),
                        tag.0,
                        Some(drop_userdata::<T>),
                    );
                }
                tags.insert(tag.0, TypeId::of::<T>());
                Ok(())
            }
        }
    }

    pub fn userdata_tag<T>(&self) -> Result<UserDataTag>
    where
        T: 'static,
    {
        let type_id = TypeId::of::<T>();
        let mut tags = self
            .inner
            .userdata_tags
            .write()
            .map_err(|_| Error::VmDataPoisoned)?;
        if let Some(tag) = tags
            .iter()
            .find_map(|(tag, existing)| (*existing == type_id).then_some(*tag))
        {
            return Ok(UserDataTag(tag));
        }
        let tag = (0..sys::LUA_UTAG_LIMIT)
            .find(|tag| !tags.contains_key(tag))
            .ok_or(Error::UserDataTagExhausted)?;
        unsafe {
            sys::lua_setuserdatadtor(self.inner.state.as_ptr(), tag, Some(drop_userdata::<T>));
        }
        tags.insert(tag, type_id);
        Ok(UserDataTag(tag))
    }

    fn install_safe_os(&self) -> Result<()> {
        let table = self.create_table()?;
        let origin = ChunkOrigin {
            module: Some(ModuleId(Arc::from("luau/os"))),
            ..ChunkOrigin::default()
        };

        let time = self.create_function_with_options(
            NativeFunctionOptions::new(origin.clone())
                .function_name("os.time")
                .argument_names(Vec::<Arc<str>>::new()),
            Arc::new(|mut frame| {
                frame.returns.write(current_unix_seconds() as f64)?;
                Ok(())
            }),
        )?;
        table.set_function_raw(self, "time", &time)?;

        let started_at = self.inner.started_at;
        let clock = self.create_function_with_options(
            NativeFunctionOptions::new(origin.clone())
                .function_name("os.clock")
                .argument_names(Vec::<Arc<str>>::new()),
            Arc::new(move |mut frame| {
                frame.returns.write(started_at.elapsed().as_secs_f64())?;
                Ok(())
            }),
        )?;
        table.set_function_raw(self, "clock", &clock)?;

        let date = self.create_function_with_options(
            NativeFunctionOptions::new(origin)
                .function_name("os.date")
                .argument_names([Arc::from("format"), Arc::from("time")]),
            Arc::new(|mut frame| {
                let format = frame
                    .args
                    .read_optional_named::<String>("format")?
                    .unwrap_or_else(|| "%c".to_string());
                let timestamp = frame
                    .args
                    .read_optional_named::<i64>("time")?
                    .unwrap_or_else(current_unix_seconds);
                frame.returns.write(format_os_date(&format, timestamp)?)?;
                Ok(())
            }),
        )?;
        table.set_function_raw(self, "date", &date)?;

        self.set_global_table("os", &table)?;
        Ok(())
    }

    fn ref_top(&self) -> Result<RegistryRef> {
        self.ref_value_from(self.inner.state, StackIndex::TOP)
    }

    fn ref_value_from(
        &self,
        state: NonNull<sys::lua_State>,
        index: StackIndex,
    ) -> Result<RegistryRef> {
        let reference = unsafe { gateway::reference(state.as_ptr(), index.raw())? };
        Ok(RegistryRef::new(self.clone(), reference))
    }

    fn ref_current_thread_from(&self, state: NonNull<sys::lua_State>) -> Result<RegistryRef> {
        let state = state.as_ptr();
        let reference = unsafe {
            gateway::push_thread(state)?;
            let reference = gateway::reference(state, -1);
            sys::lua_pop(state, 1);
            reference?
        };
        Ok(RegistryRef::new(self.clone(), reference))
    }

    fn push_registry(&self, reference: &RegistryRef) -> Result<()> {
        self.push_registry_to(self.inner.state, reference)
    }

    fn push_registry_to(
        &self,
        state: NonNull<sys::lua_State>,
        reference: &RegistryRef,
    ) -> Result<()> {
        if reference.vm().id() != self.inner.id {
            return Err(Error::VmMismatch {
                reference_vm: reference.vm().id(),
                actual_vm: self.inner.id,
            });
        }
        unsafe { gateway::get_ref(state.as_ptr(), reference.raw()) }
    }

    fn push_metatable(&self, metatable: Option<&Table>) -> Result<()> {
        match metatable {
            Some(metatable) => self.push_registry(&metatable.reference),
            None => unsafe { gateway::push_nil(self.inner.state.as_ptr()) },
        }
    }

    fn push_value(&self, value: &Value) -> Result<()> {
        self.push_value_to(self.inner.state, value)
    }

    fn push_value_to(&self, state: NonNull<sys::lua_State>, value: &Value) -> Result<()> {
        let state_ptr = state.as_ptr();
        unsafe {
            match value {
                Value::Nil => gateway::push_nil(state_ptr),
                Value::Boolean(value) => gateway::push_boolean(state_ptr, *value),
                Value::Integer(value) => gateway::push_integer(state_ptr, *value),
                Value::Number(value) => gateway::push_number(state_ptr, *value),
                Value::String(value) => gateway::push_bytes(state_ptr, value),
                Value::Buffer(value) => gateway::new_buffer(state_ptr, value),
                Value::TableData(value) => value.push_to(self, state),
                Value::NativeFunction(value) => {
                    self.push_native_function_to(state, &value.options, value.callback.clone())
                }
                Value::Table(value) => self.push_registry_to(state, &value.reference),
                Value::Function(value) => self.push_registry_to(state, &value.reference),
                Value::Thread(value) => self.push_thread_to(state, value),
                Value::UserData(value) => self.push_registry_to(state, &value.reference),
            }
        }
    }

    fn push_native_function_to(
        &self,
        state: NonNull<sys::lua_State>,
        options: &NativeFunctionOptions,
        callback: NativeFn,
    ) -> Result<()> {
        unsafe {
            let slot = gateway::new_userdata_dtor(
                state.as_ptr(),
                std::mem::size_of::<CallbackSlot>(),
                Some(drop_callback_slot),
            )?
            .cast::<CallbackSlot>();
            ptr::write(
                slot,
                CallbackSlot {
                    vm: Rc::downgrade(&self.inner),
                    vm_id: self.inner.id,
                    callback,
                    origin: options.origin.clone(),
                    capability: options.capability.clone(),
                    task_group: options.task_group,
                    function_name: options.function_name.clone(),
                    argument_names: options.argument_names.clone(),
                    use_thread_context_origin: options.use_thread_context_origin,
                },
            );
            gateway::push_cclosure(
                state.as_ptr(),
                Some(native_callback),
                c"harmony_native_callback",
                1,
            )
        }
    }

    fn push_thread_to(&self, state: NonNull<sys::lua_State>, thread: &Thread) -> Result<()> {
        if thread.vm_id() != self.inner.id {
            return Err(Error::VmMismatch {
                reference_vm: thread.vm_id(),
                actual_vm: self.inner.id,
            });
        }

        if let Some(reference) = thread.reference() {
            self.push_registry_to(state, reference)?;
            return Ok(());
        }

        if thread.state() == state {
            return unsafe { gateway::push_thread(state.as_ptr()) };
        }

        Err(Error::Runtime(
            "callback thread cannot be pushed to a different Luau stack".to_string(),
        ))
    }

    fn read_value(&self, index: StackIndex) -> Result<Value> {
        self.read_value_from(self.inner.state, index)
    }

    fn read_value_from(&self, state: NonNull<sys::lua_State>, index: StackIndex) -> Result<Value> {
        let state_ptr = state.as_ptr();
        let raw_index = index.raw();
        let value = unsafe {
            match sys::lua_type(state_ptr, raw_index) {
                sys::LUA_TNIL | sys::LUA_TNONE => Value::Nil,
                sys::LUA_TBOOLEAN => Value::Boolean(sys::lua_toboolean(state_ptr, raw_index) != 0),
                sys::LUA_TINTEGER => {
                    let mut is_integer = 0;
                    Value::Integer(sys::lua_tointeger64(state_ptr, raw_index, &mut is_integer))
                }
                sys::LUA_TNUMBER => {
                    let mut is_number = 0;
                    Value::Number(sys::lua_tonumberx(state_ptr, raw_index, &mut is_number))
                }
                sys::LUA_TSTRING => Value::String(
                    gateway::string_bytes(state_ptr, raw_index)
                        .expect("string value has bytes")
                        .to_vec(),
                ),
                sys::LUA_TBUFFER => {
                    let mut len = 0usize;
                    let ptr = sys::lua_tobuffer(state_ptr, raw_index, &mut len);
                    Value::Buffer(std::slice::from_raw_parts(ptr.cast(), len).to_vec())
                }
                sys::LUA_TTABLE => Value::Table(Table {
                    reference: self.ref_value_from(state, index)?,
                }),
                sys::LUA_TFUNCTION => Value::Function(Function {
                    reference: self.ref_value_from(state, index)?,
                    origin: ChunkOrigin::default(),
                }),
                sys::LUA_TTHREAD => {
                    let thread_state = sys::lua_tothread(state_ptr, raw_index);
                    let thread_state =
                        NonNull::new(thread_state).expect("thread value is non-null");
                    let reference = self.ref_value_from(state, index)?;
                    let status = sys::lua_status(thread_state.as_ptr());
                    let top = sys::lua_gettop(thread_state.as_ptr());
                    Value::Thread(Thread::new(
                        Some(reference),
                        thread_state,
                        self.inner.id,
                        ChunkOrigin::default(),
                        status != sys::LUA_OK || top != 1,
                        status == sys::LUA_OK && top == 0,
                    ))
                }
                sys::LUA_TUSERDATA => Value::UserData(UserData {
                    reference: self.ref_value_from(state, index)?,
                    tag: UserDataTag(sys::lua_userdatatag(state_ptr, raw_index)),
                }),
                _ => Value::Nil,
            }
        };
        Ok(value)
    }
}

impl Drop for VmInner {
    fn drop(&mut self) {
        unsafe {
            sys::lua_close(self.state.as_ptr());
            drop(Box::from_raw(self.control.as_ptr()));
        }
    }
}

impl VmInner {
    fn control(&self) -> &VmControl {
        unsafe { self.control.as_ref() }
    }
}

struct VmControl {
    allocated: AtomicUsize,
    memory_limit: AtomicUsize,
    interrupt_deadline_millis: AtomicU64,
    /// Keyed by `lua_State` address; [`userthread`] keeps it in step with
    /// the threads Luau creates and frees.
    thread_contexts: RwLock<HashMap<usize, CallContext>>,
}

impl VmControl {
    fn new(options: VmOptions) -> Self {
        Self {
            allocated: AtomicUsize::new(0),
            memory_limit: AtomicUsize::new(options.memory_limit.unwrap_or(0)),
            interrupt_deadline_millis: AtomicU64::new(0),
            thread_contexts: RwLock::new(HashMap::new()),
        }
    }
}

pub struct InterruptBudgetGuard {
    vm: Vm,
    previous_deadline: u64,
}

impl Drop for InterruptBudgetGuard {
    fn drop(&mut self) {
        self.vm
            .set_interrupt_deadline_millis(self.previous_deadline);
    }
}

#[derive(Default)]
pub struct VmData {
    values: RwLock<LocalContextBag>,
}

impl VmData {
    pub fn insert<T>(&self, value: T) -> Result<()>
    where
        T: 'static,
    {
        let mut values = self.values.write().map_err(|_| Error::VmDataPoisoned)?;
        values.insert(value);
        Ok(())
    }

    pub fn get<T>(&self) -> Result<Rc<T>>
    where
        T: 'static,
    {
        let values = self.values.read().map_err(|_| Error::VmDataPoisoned)?;
        values.get::<T>()
    }

    /// Drops every value. Values that hold Luau references keep their VM
    /// alive, so a host clears them before letting the VM go.
    pub fn clear(&self) -> Result<()> {
        let values = std::mem::take(&mut *self.values.write().map_err(|_| Error::VmDataPoisoned)?);
        drop(values);
        Ok(())
    }
}

struct StackGuard<'vm> {
    state: NonNull<sys::lua_State>,
    top: i32,
    _vm: PhantomData<&'vm Vm>,
}

impl StackGuard<'_> {
    fn new(state: NonNull<sys::lua_State>) -> Self {
        Self {
            state,
            top: unsafe { sys::lua_gettop(state.as_ptr()) },
            _vm: PhantomData,
        }
    }
}

impl Drop for StackGuard<'_> {
    fn drop(&mut self) {
        unsafe { gateway::truncate(self.state.as_ptr(), self.top) };
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StackIndex(c_int);

impl StackIndex {
    const TOP: Self = Self(-1);

    fn absolute(index: c_int) -> Self {
        debug_assert!(index > 0, "absolute Luau stack indexes must be positive");
        Self(index)
    }

    fn relative(index: c_int) -> Self {
        debug_assert!(index < 0, "relative Luau stack indexes must be negative");
        Self(index)
    }

    fn argument(index: usize) -> Self {
        debug_assert!(
            index < c_int::MAX as usize,
            "Luau argument index must fit in c_int"
        );
        Self::absolute(index as c_int + 1)
    }

    fn raw(self) -> c_int {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AbsStackIndex(c_int);

impl AbsStackIndex {
    fn from_stack(state: NonNull<sys::lua_State>, index: StackIndex) -> Self {
        let absolute = if index.raw() > 0 {
            index.raw()
        } else {
            unsafe { sys::lua_absindex(state.as_ptr(), index.raw()) }
        };
        debug_assert!(absolute > 0, "absolute Luau stack indexes must be positive");
        Self(absolute)
    }

    fn raw(self) -> c_int {
        self.0
    }
}

/// A Luau value pinned in the registry. Clones share the pin, which the last one releases.
#[derive(Clone)]
pub struct RegistryRef {
    slot: Rc<RegistrySlot>,
}

struct RegistrySlot {
    vm: Vm,
    reference: c_int,
}

impl RegistryRef {
    fn new(vm: Vm, reference: c_int) -> Self {
        Self {
            slot: Rc::new(RegistrySlot { vm, reference }),
        }
    }

    fn vm(&self) -> &Vm {
        &self.slot.vm
    }

    fn raw(&self) -> c_int {
        self.slot.reference
    }
}

impl fmt::Debug for RegistryRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistryRef")
            .field("vm_id", &self.vm().id())
            .field("reference", &self.raw())
            .finish()
    }
}

impl PartialEq for RegistryRef {
    fn eq(&self, other: &Self) -> bool {
        if Rc::ptr_eq(&self.slot, &other.slot) {
            return true;
        }
        if self.vm().id() != other.vm().id() {
            return false;
        }

        let vm = self.vm();
        let _guard = vm.stack_guard();
        let state = vm.inner.state.as_ptr();
        // A stack too full to hold both values cannot show them equal.
        unsafe {
            gateway::get_ref(state, self.raw()).is_ok()
                && gateway::get_ref(state, other.raw()).is_ok()
                && sys::lua_rawequal(state, -1, -2) != 0
        }
    }
}

impl Drop for RegistrySlot {
    fn drop(&mut self) {
        unsafe {
            sys::lua_unref(self.vm.inner.state.as_ptr(), self.reference);
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Function {
    reference: RegistryRef,
    pub origin: ChunkOrigin,
}

impl Function {
    pub fn vm_id(&self) -> u64 {
        self.reference.vm().id()
    }

    pub fn call(&self, vm: &Vm, args: &[Value]) -> Result<Vec<Value>> {
        let guard = vm.stack_guard();
        vm.push_registry(&self.reference)?;
        for arg in args {
            vm.push_value(arg)?;
        }
        unsafe { gateway::pcall(vm.inner.state.as_ptr(), args.len() as i32)? };
        let returned = vm.top() - guard.top;
        let mut values = Vec::with_capacity(returned.max(0) as usize);
        for offset in 0..returned {
            values.push(vm.read_value(StackIndex::absolute(guard.top + 1 + offset))?);
        }
        Ok(values)
    }

    /// Calls the function as [`Function::call`] does, with `context` as the
    /// call context of the native functions it reaches and the coroutines it
    /// creates.
    pub fn call_with_context(
        &self,
        vm: &Vm,
        args: &[Value],
        context: CallContext,
    ) -> Result<Vec<Value>> {
        let previous = vm.replace_state_context(vm.inner.state, Some(context))?;
        let result = self.call(vm, args);
        vm.replace_state_context(vm.inner.state, previous)?;
        result
    }
}

#[derive(Clone)]
pub struct Thread {
    inner: Rc<ThreadInner>,
}

struct ThreadInner {
    _reference: Option<RegistryRef>,
    state: NonNull<sys::lua_State>,
    vm_id: u64,
    origin: ChunkOrigin,
    data: ThreadData,
    started: Cell<bool>,
    finished: Cell<bool>,
}

impl Thread {
    /// Whether code running on this thread may yield here; it can't inside
    /// a metamethod, a `table.sort` comparator, or a host `pcall`.
    pub fn is_yieldable(&self) -> bool {
        unsafe { sys::lua_isyieldable(self.inner.state.as_ptr()) != 0 }
    }

    fn new(
        reference: Option<RegistryRef>,
        state: NonNull<sys::lua_State>,
        vm_id: u64,
        origin: ChunkOrigin,
        started: bool,
        finished: bool,
    ) -> Self {
        Self {
            inner: Rc::new(ThreadInner {
                _reference: reference,
                state,
                vm_id,
                origin,
                data: ThreadData::default(),
                started: Cell::new(started),
                finished: Cell::new(finished),
            }),
        }
    }

    pub fn vm_id(&self) -> u64 {
        self.inner.vm_id
    }

    pub fn state_id(&self) -> usize {
        self.inner.state.as_ptr() as usize
    }

    pub fn origin(&self) -> &ChunkOrigin {
        &self.inner.origin
    }

    pub fn data(&self) -> &ThreadData {
        &self.inner.data
    }

    pub fn is_started(&self) -> bool {
        self.inner.started.get() || self.has_started_state()
    }

    fn reference(&self) -> Option<&RegistryRef> {
        self.inner._reference.as_ref()
    }

    fn state(&self) -> NonNull<sys::lua_State> {
        self.inner.state
    }

    fn has_started_state(&self) -> bool {
        unsafe {
            sys::lua_status(self.inner.state.as_ptr()) != sys::LUA_OK
                || sys::lua_gettop(self.inner.state.as_ptr()) != 1
        }
    }

    pub fn resume(&self, vm: &Vm, args: &[Value]) -> Result<ThreadStatus> {
        if self.vm_id() != vm.inner.id {
            return Err(Error::VmMismatch {
                reference_vm: self.vm_id(),
                actual_vm: vm.inner.id,
            });
        }
        if self.inner.finished.get() {
            return Err(Error::Runtime("thread already completed".to_string()));
        }

        // A thread that has not started holds just its function, which stays below the args.
        let state = self.inner.state.as_ptr();
        if self.is_started() {
            unsafe { gateway::truncate(state, 0) };
        }
        let base = unsafe { sys::lua_gettop(state) };
        for arg in args {
            if let Err(error) = vm.push_value_to(self.inner.state, arg) {
                // Leaves an unstarted thread with just its function, so it still reads as unstarted.
                unsafe { gateway::truncate(state, base) };
                return Err(error);
            }
        }
        self.inner.started.set(true);

        let resumed = unsafe { gateway::resume(vm.inner.state.as_ptr(), state, args.len() as i32) };
        let result = resumed.and_then(|yielded| {
            let top = unsafe { sys::lua_gettop(state) };
            let values = (1..=top)
                .map(|index| vm.read_value_from(self.inner.state, StackIndex::absolute(index)))
                .collect::<Result<Vec<_>>>()?;
            if yielded {
                Ok(ThreadStatus::Yielded(values))
            } else {
                self.inner.finished.set(true);
                Ok(ThreadStatus::Completed(values))
            }
        });
        unsafe { gateway::truncate(state, 0) };
        result
    }
}

impl fmt::Debug for Thread {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Thread")
            .field("vm_id", &self.vm_id())
            .field("state", &self.inner.state)
            .field("origin", &self.inner.origin)
            .finish()
    }
}

impl PartialEq for Thread {
    fn eq(&self, other: &Self) -> bool {
        self.vm_id() == other.vm_id() && self.inner.state == other.inner.state
    }
}

#[derive(Default)]
pub struct ThreadData {
    values: RwLock<LocalContextBag>,
}

impl ThreadData {
    pub fn insert<T>(&self, value: T) -> Result<()>
    where
        T: 'static,
    {
        let mut values = self.values.write().map_err(|_| Error::ThreadDataPoisoned)?;
        values.insert(value);
        Ok(())
    }

    pub fn get<T>(&self) -> Result<Rc<T>>
    where
        T: 'static,
    {
        let values = self.values.read().map_err(|_| Error::ThreadDataPoisoned)?;
        values.get::<T>()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ThreadStatus {
    Yielded(Vec<Value>),
    Completed(Vec<Value>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Table {
    reference: RegistryRef,
}

impl Table {
    pub fn vm_id(&self) -> u64 {
        self.reference.vm().id()
    }

    // `raw` table APIs intentionally mirror Lua raw access: they bypass
    // metamethods and operate directly on table storage.
    pub fn set_raw(&self, vm: &Vm, key: &str, value: Value) -> Result<()> {
        let key = CString::new(key)?;
        let _guard = vm.stack_guard();
        vm.push_registry(&self.reference)?;
        vm.push_value(&value)?;
        unsafe { gateway::raw_set_field(vm.inner.state.as_ptr(), -2, &key) }
    }

    pub fn set_integer_raw(&self, vm: &Vm, key: i32, value: Value) -> Result<()> {
        let _guard = vm.stack_guard();
        vm.push_registry(&self.reference)?;
        vm.push_value(&value)?;
        unsafe { gateway::raw_set_index(vm.inner.state.as_ptr(), -2, key) }
    }

    pub fn set_key_raw(&self, vm: &Vm, key: Value, value: Value) -> Result<()> {
        let _guard = vm.stack_guard();
        vm.push_registry(&self.reference)?;
        vm.push_value(&key)?;
        vm.push_value(&value)?;
        unsafe { gateway::raw_set(vm.inner.state.as_ptr(), -3) }
    }

    pub fn set_function_raw(&self, vm: &Vm, key: &str, function: &Function) -> Result<()> {
        let key = CString::new(key)?;
        let _guard = vm.stack_guard();
        vm.push_registry(&self.reference)?;
        vm.push_registry(&function.reference)?;
        unsafe { gateway::raw_set_field(vm.inner.state.as_ptr(), -2, &key) }
    }

    pub fn set_table_raw(&self, vm: &Vm, key: &str, table: &Table) -> Result<()> {
        let key = CString::new(key)?;
        let _guard = vm.stack_guard();
        vm.push_registry(&self.reference)?;
        vm.push_registry(&table.reference)?;
        unsafe { gateway::raw_set_field(vm.inner.state.as_ptr(), -2, &key) }
    }

    pub fn get_raw(&self, vm: &Vm, key: &str) -> Result<Value> {
        let key = CString::new(key)?;
        let _guard = vm.stack_guard();
        vm.push_registry(&self.reference)?;
        unsafe { gateway::raw_get_field(vm.inner.state.as_ptr(), -1, &key)? };
        vm.read_value(StackIndex::TOP)
    }

    pub fn get_integer_raw(&self, vm: &Vm, key: i32) -> Result<Value> {
        let _guard = vm.stack_guard();
        vm.push_registry(&self.reference)?;
        unsafe { gateway::raw_get_index(vm.inner.state.as_ptr(), -1, key)? };
        vm.read_value(StackIndex::TOP)
    }

    pub fn raw_len(&self, vm: &Vm) -> Result<usize> {
        let reader = self.reader(vm)?;
        Ok(reader.raw_len())
    }

    pub fn array_values_raw(&self, vm: &Vm) -> Result<Vec<Value>> {
        let _guard = vm.stack_guard();
        vm.push_registry(&self.reference)?;

        let state = vm.inner.state.as_ptr();
        let mut values = Vec::new();
        for index in 1..=i32::MAX {
            unsafe { gateway::raw_get_index(state, -1, index)? };
            let value = vm.read_value(StackIndex::TOP)?;
            unsafe {
                sys::lua_pop(state, 1);
            }
            if value == Value::Nil {
                return Ok(values);
            }
            values.push(value);
        }

        Err(Error::Runtime(
            "table array part exceeded supported integer indexes".to_string(),
        ))
    }

    pub fn pairs_raw(&self, vm: &Vm) -> Result<Vec<(Value, Value)>> {
        let _guard = vm.stack_guard();
        let mut pairs = Vec::new();
        vm.push_registry(&self.reference)?;
        let state = vm.inner.state.as_ptr();
        unsafe {
            gateway::push_nil(state)?;
            while gateway::next(state, -2)? {
                let key = vm.read_value(StackIndex::relative(-2))?;
                let value = vm.read_value(StackIndex::TOP)?;
                pairs.push((key, value));
                sys::lua_pop(state, 1);
            }
        }
        Ok(pairs)
    }

    pub fn reader<'vm>(&self, vm: &'vm Vm) -> Result<TableReader<'vm>> {
        let guard = vm.stack_guard();
        vm.push_registry(&self.reference)?;
        let table_index = AbsStackIndex::from_stack(vm.inner.state, StackIndex::TOP);
        Ok(TableReader {
            vm,
            state: vm.inner.state,
            table_index,
            _guard: guard,
        })
    }

    pub fn set_metatable_raw(&self, vm: &Vm, metatable: Option<&Table>) -> Result<()> {
        let _guard = vm.stack_guard();
        vm.push_registry(&self.reference)?;
        vm.push_metatable(metatable)?;
        unsafe { gateway::set_metatable(vm.inner.state.as_ptr(), -2) }
    }

    pub fn metatable_raw(&self, vm: &Vm) -> Result<Option<Table>> {
        let _guard = vm.stack_guard();
        vm.push_registry(&self.reference)?;
        let has_metatable = unsafe { gateway::get_metatable(vm.inner.state.as_ptr(), -1)? };
        if !has_metatable {
            return Ok(None);
        }
        let reference = vm.ref_top()?;
        Ok(Some(Table { reference }))
    }

    pub fn set_readonly(&self, vm: &Vm, readonly: bool) -> Result<()> {
        let _guard = vm.stack_guard();
        vm.push_registry(&self.reference)?;
        unsafe { gateway::set_readonly(vm.inner.state.as_ptr(), -1, readonly) }
    }
}

pub struct TableReader<'vm> {
    vm: &'vm Vm,
    state: NonNull<sys::lua_State>,
    table_index: AbsStackIndex,
    _guard: StackGuard<'vm>,
}

impl TableReader<'_> {
    pub fn raw_len(&self) -> usize {
        let len = unsafe { sys::lua_objlen(self.state.as_ptr(), self.table_index.raw()) };
        debug_assert!(len >= 0);
        len as usize
    }

    pub fn get_raw(&self, key: &str) -> Result<Value> {
        let key = CString::new(key)?;
        unsafe { gateway::raw_get_field(self.state.as_ptr(), self.table_index.raw(), &key)? };
        self.pop_value()
    }

    pub fn get_integer_raw(&self, key: i32) -> Result<Value> {
        unsafe { gateway::raw_get_index(self.state.as_ptr(), self.table_index.raw(), key)? };
        self.pop_value()
    }

    pub fn get_fields_raw(&self, keys: &[&str]) -> Result<Vec<Value>> {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(self.get_raw(key)?);
        }
        Ok(values)
    }

    pub fn array_values_raw(&self) -> Result<Vec<Value>> {
        let len = self.raw_len();
        if len > i32::MAX as usize {
            return Err(Error::Runtime(
                "table array part exceeded supported integer indexes".to_string(),
            ));
        }

        let mut values = Vec::with_capacity(len);
        for index in 1..=len as i32 {
            values.push(self.get_integer_raw(index)?);
        }
        Ok(values)
    }

    fn pop_value(&self) -> Result<Value> {
        let value = self.vm.read_value_from(self.state, StackIndex::TOP);
        unsafe {
            sys::lua_pop(self.state.as_ptr(), 1);
        }
        value
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Buffer {
    reference: RegistryRef,
}

impl Buffer {
    pub fn vm_id(&self) -> u64 {
        self.reference.vm().id()
    }

    pub fn to_vec(&self, vm: &Vm) -> Result<Vec<u8>> {
        let _guard = vm.stack_guard();
        vm.push_registry(&self.reference)?;
        match vm.read_value(StackIndex::TOP)? {
            Value::Buffer(bytes) => Ok(bytes),
            other => Err(Error::ArgumentType {
                name: Arc::from("buffer"),
                expected: "buffer",
                actual: other.type_name(),
            }),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct UserData {
    reference: RegistryRef,
    tag: UserDataTag,
}

impl UserData {
    pub fn vm_id(&self) -> u64 {
        self.reference.vm().id()
    }

    pub fn tag(&self) -> UserDataTag {
        self.tag
    }

    pub fn set_metatable_raw(&self, vm: &Vm, metatable: Option<&Table>) -> Result<()> {
        let _guard = vm.stack_guard();
        vm.push_registry(&self.reference)?;
        vm.push_metatable(metatable)?;
        unsafe { gateway::set_metatable(vm.inner.state.as_ptr(), -2) }
    }

    pub fn borrow<T>(&self, vm: &Vm, tag: UserDataTag) -> Result<UserDataRef<'_, T>>
    where
        T: 'static,
    {
        let _guard = vm.stack_guard();
        vm.push_registry(&self.reference)?;
        let actual = unsafe { sys::lua_userdatatag(vm.inner.state.as_ptr(), -1) };
        if actual != tag.0 {
            return Err(Error::UserDataTypeMismatch {
                expected: tag.0,
                actual,
            });
        }
        let ptr = unsafe { sys::lua_touserdatatagged(vm.inner.state.as_ptr(), -1, tag.0) };
        let ptr = NonNull::new(ptr.cast::<T>()).ok_or(Error::UserDataTypeMismatch {
            expected: tag.0,
            actual,
        })?;
        Ok(UserDataRef {
            ptr,
            _owner: PhantomData,
        })
    }
}

pub struct UserDataRef<'a, T> {
    ptr: NonNull<T>,
    _owner: PhantomData<&'a UserData>,
}

impl<T> std::ops::Deref for UserDataRef<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref() }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ByteString(pub Vec<u8>);

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Nil,
    Boolean(bool),
    Integer(i64),
    Number(f64),
    String(Vec<u8>),
    Buffer(Vec<u8>),
    TableData(OwnedTable),
    NativeFunction(NativeFunctionValue),
    Table(Table),
    Function(Function),
    Thread(Thread),
    UserData(UserData),
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Number(value as f64)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Self::Number(value as f64)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::Number(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::String(value.into_bytes())
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.as_bytes().to_vec())
    }
}

impl From<OwnedTable> for Value {
    fn from(value: OwnedTable) -> Self {
        Self::TableData(value)
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(value: Option<T>) -> Self {
        value.map_or(Self::Nil, Into::into)
    }
}

#[derive(Clone)]
pub struct NativeFunctionValue {
    options: NativeFunctionOptions,
    callback: NativeFn,
}

impl NativeFunctionValue {
    pub fn new(options: NativeFunctionOptions, callback: NativeFn) -> Self {
        Self { options, callback }
    }
}

impl fmt::Debug for NativeFunctionValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeFunctionValue")
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl PartialEq for NativeFunctionValue {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.callback, &other.callback)
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct OwnedTable {
    array: Vec<Value>,
    fields: Vec<(String, Value)>,
    entries: Vec<(Value, Value)>,
}

impl OwnedTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(array: usize, fields: usize) -> Self {
        Self {
            array: Vec::with_capacity(array),
            fields: Vec::with_capacity(fields),
            entries: Vec::new(),
        }
    }

    pub fn with_entry_capacity(array: usize, fields: usize, entries: usize) -> Self {
        Self {
            array: Vec::with_capacity(array),
            fields: Vec::with_capacity(fields),
            entries: Vec::with_capacity(entries),
        }
    }

    /// An array of `values` in order.
    pub fn from_array<V: Into<Value>>(values: impl IntoIterator<Item = V>) -> Self {
        Self {
            array: values.into_iter().map(Into::into).collect(),
            ..Self::default()
        }
    }

    /// A table of `entries`, keyed by any value.
    pub fn from_entries<K: Into<Value>, V: Into<Value>>(
        entries: impl IntoIterator<Item = (K, V)>,
    ) -> Self {
        Self {
            entries: entries
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
            ..Self::default()
        }
    }

    pub fn push_array(&mut self, value: impl Into<Value>) {
        self.array.push(value.into());
    }

    pub fn set_field(&mut self, key: impl Into<String>, value: impl Into<Value>) {
        self.fields.push((key.into(), value.into()));
    }

    pub fn set_key(&mut self, key: impl Into<Value>, value: impl Into<Value>) {
        self.entries.push((key.into(), value.into()));
    }

    pub fn array(&self) -> &[Value] {
        &self.array
    }

    pub fn fields(&self) -> &[(String, Value)] {
        &self.fields
    }

    pub fn entries(&self) -> &[(Value, Value)] {
        &self.entries
    }

    fn push_to(&self, vm: &Vm, state: NonNull<sys::lua_State>) -> Result<()> {
        let state_ptr = state.as_ptr();
        unsafe {
            gateway::create_table(
                state_ptr,
                self.array.len() as i32,
                (self.fields.len() + self.entries.len()) as i32,
            )?;
        }
        for (index, value) in self.array.iter().enumerate() {
            vm.push_value_to(state, value)?;
            unsafe { gateway::raw_set_index(state_ptr, -2, index as i32 + 1)? };
        }
        for (key, value) in &self.fields {
            let key = CString::new(key.as_str())?;
            vm.push_value_to(state, value)?;
            unsafe { gateway::raw_set_field(state_ptr, -2, &key)? };
        }
        for (key, value) in &self.entries {
            vm.push_value_to(state, key)?;
            vm.push_value_to(state, value)?;
            unsafe { gateway::raw_set(state_ptr, -3)? };
        }
        Ok(())
    }
}

pub struct ArgReader<'vm> {
    vm: &'vm Vm,
    state: NonNull<sys::lua_State>,
    cursor: ArgCursor,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ArgCursor {
    argc: usize,
    index: usize,
    names: Vec<Arc<str>>,
}

impl ArgCursor {
    fn new(argc: i32, names: impl IntoIterator<Item = Arc<str>>) -> Self {
        Self {
            argc: argc.max(0) as usize,
            index: 0,
            names: names.into_iter().collect(),
        }
    }

    fn len(&self) -> usize {
        self.argc
    }

    fn index(&self) -> usize {
        self.index
    }

    fn is_finished(&self) -> bool {
        self.index >= self.len()
    }

    fn current_name(&self) -> Arc<str> {
        self.names
            .get(self.index)
            .cloned()
            .unwrap_or_else(|| Arc::from(format!("arg{}", self.index + 1)))
    }

    fn label_current(&mut self, name: &'static str) {
        if self.names.len() <= self.index {
            self.names.resize(self.index + 1, Arc::from(name));
        }
    }

    fn advance(&mut self) {
        self.index += 1;
    }

    fn drain_remaining(&mut self) -> std::ops::Range<usize> {
        let remaining = self.index..self.argc;
        self.index = self.argc;
        remaining
    }
}

impl<'vm> ArgReader<'vm> {
    fn stack(
        vm: &'vm Vm,
        state: NonNull<sys::lua_State>,
        argc: i32,
        names: impl IntoIterator<Item = Arc<str>>,
    ) -> Self {
        Self {
            vm,
            state,
            cursor: ArgCursor::new(argc, names),
        }
    }

    pub fn vm(&self) -> &'vm Vm {
        self.vm
    }

    /// The name of the argument the next read takes, for a reader's own error messages.
    pub fn next_name(&self) -> Arc<str> {
        self.current_name()
    }

    pub fn read<T: FromLuau<'vm>>(&mut self) -> Result<T> {
        T::read(self)
    }

    pub fn read_named<T: FromLuau<'vm>>(&mut self, name: &'static str) -> Result<T> {
        self.cursor.label_current(name);
        T::read(self)
    }

    pub fn read_optional_named<T: FromLuau<'vm>>(
        &mut self,
        name: &'static str,
    ) -> Result<Option<T>> {
        if self.cursor.is_finished() {
            return Ok(None);
        }
        if self.is_nil(self.cursor.index()) {
            self.cursor.advance();
            return Ok(None);
        }
        self.read_named(name).map(Some)
    }

    pub fn drain_remaining(&mut self) -> Result<Vec<Value>> {
        let remaining = self.cursor.drain_remaining();
        remaining.map(|index| self.value_at(index)).collect()
    }

    pub fn read_string_bytes(&mut self) -> Result<&[u8]> {
        self.next_borrowed_bytes("string")
    }

    pub fn read_named_string_bytes(&mut self, name: &'static str) -> Result<&[u8]> {
        self.cursor.label_current(name);
        self.next_borrowed_bytes("string")
    }

    pub fn read_str(&mut self) -> Result<&str> {
        let (name, bytes) = self.next_named_borrowed_bytes("string")?;
        std::str::from_utf8(bytes)
            .map_err(|error| Error::InvalidUtf8BorrowedArgument { name, error })
    }

    pub fn read_named_str(&mut self, name: &'static str) -> Result<&str> {
        self.cursor.label_current(name);
        self.read_str()
    }

    pub fn read_buffer_bytes(&mut self) -> Result<&[u8]> {
        self.next_borrowed_bytes("buffer")
    }

    pub fn read_named_buffer_bytes(&mut self, name: &'static str) -> Result<&[u8]> {
        self.cursor.label_current(name);
        self.next_borrowed_bytes("buffer")
    }

    fn next_value(&mut self, expected: &'static str) -> Result<Value> {
        self.next_named_value(expected).map(|(_, value)| value)
    }

    fn next_bool(&mut self) -> Result<bool> {
        let name = self.current_name();
        if self.cursor.is_finished() {
            return Err(Error::MissingArgument(name));
        }
        let index = StackIndex::argument(self.cursor.index()).raw();
        let actual_type = unsafe { sys::lua_type(self.state.as_ptr(), index) };
        if actual_type != sys::LUA_TBOOLEAN {
            return Err(Error::ArgumentType {
                name,
                expected: "boolean",
                actual: stack_type_name(actual_type),
            });
        }
        let value = unsafe { sys::lua_toboolean(self.state.as_ptr(), index) != 0 };
        self.cursor.advance();
        Ok(value)
    }

    fn next_i64(&mut self) -> Result<i64> {
        let name = self.current_name();
        if self.cursor.is_finished() {
            return Err(Error::MissingArgument(name));
        }
        let index = StackIndex::argument(self.cursor.index()).raw();
        let actual_type = unsafe { sys::lua_type(self.state.as_ptr(), index) };
        let value = match actual_type {
            sys::LUA_TINTEGER => {
                let mut is_integer = 0;
                unsafe { sys::lua_tointeger64(self.state.as_ptr(), index, &mut is_integer) }
            }
            sys::LUA_TNUMBER => {
                let mut is_number = 0;
                let value =
                    unsafe { sys::lua_tonumberx(self.state.as_ptr(), index, &mut is_number) };
                checked_i64_arg(name.clone(), value)?
            }
            _ => {
                return Err(Error::ArgumentType {
                    name,
                    expected: "number",
                    actual: stack_type_name(actual_type),
                });
            }
        };
        self.cursor.advance();
        Ok(value)
    }

    fn next_f64(&mut self) -> Result<f64> {
        let name = self.current_name();
        if self.cursor.is_finished() {
            return Err(Error::MissingArgument(name));
        }
        let index = StackIndex::argument(self.cursor.index()).raw();
        let actual_type = unsafe { sys::lua_type(self.state.as_ptr(), index) };
        let value = match actual_type {
            sys::LUA_TINTEGER => {
                let mut is_integer = 0;
                unsafe { sys::lua_tointeger64(self.state.as_ptr(), index, &mut is_integer) as f64 }
            }
            sys::LUA_TNUMBER => {
                let mut is_number = 0;
                unsafe { sys::lua_tonumberx(self.state.as_ptr(), index, &mut is_number) }
            }
            _ => {
                return Err(Error::ArgumentType {
                    name,
                    expected: "number",
                    actual: stack_type_name(actual_type),
                });
            }
        };
        self.cursor.advance();
        Ok(value)
    }

    fn next_named_value(&mut self, expected: &'static str) -> Result<(Arc<str>, Value)> {
        let name = self.current_name();
        if self.cursor.is_finished() {
            return Err(Error::MissingArgument(name));
        }
        let actual = self.type_name_at(self.cursor.index());
        if actual != expected && expected != "any" {
            return Err(Error::ArgumentType {
                name,
                expected,
                actual,
            });
        }
        let value = self.value_at(self.cursor.index())?;
        self.cursor.advance();
        Ok((name, value))
    }

    fn next_borrowed_bytes(&mut self, expected: &'static str) -> Result<&[u8]> {
        self.next_named_borrowed_bytes(expected)
            .map(|(_, bytes)| bytes)
    }

    fn next_named_borrowed_bytes(&mut self, expected: &'static str) -> Result<(Arc<str>, &[u8])> {
        let name = self.current_name();
        if self.cursor.is_finished() {
            return Err(Error::MissingArgument(name));
        }
        let index = self.cursor.index();
        let actual = self.type_name_at(index);
        if actual != expected {
            return Err(Error::ArgumentType {
                name,
                expected,
                actual,
            });
        }
        self.cursor.advance();
        let bytes = self.bytes_at(index, expected);
        Ok((name, bytes))
    }

    fn current_name(&self) -> Arc<str> {
        self.cursor.current_name()
    }

    fn is_nil(&self, index: usize) -> bool {
        self.type_name_at(index) == "nil"
    }

    fn value_at(&self, index: usize) -> Result<Value> {
        self.vm
            .read_value_from(self.state, StackIndex::argument(index))
    }

    fn bytes_at(&self, index: usize, expected: &'static str) -> &[u8] {
        let state = self.state.as_ptr();
        let index = StackIndex::argument(index).raw();
        if expected != "buffer" {
            return unsafe { gateway::string_bytes(state, index) }.unwrap_or_default();
        }
        unsafe {
            let mut len = 0usize;
            let ptr = sys::lua_tobuffer(state, index, &mut len).cast::<u8>();
            if ptr.is_null() {
                &[]
            } else {
                std::slice::from_raw_parts(ptr, len)
            }
        }
    }

    fn type_name_at(&self, index: usize) -> &'static str {
        unsafe {
            stack_type_name(sys::lua_type(
                self.state.as_ptr(),
                StackIndex::argument(index).raw(),
            ))
        }
    }
}

fn stack_type_name(type_id: i32) -> &'static str {
    match type_id {
        sys::LUA_TNIL | sys::LUA_TNONE => "nil",
        sys::LUA_TBOOLEAN => "boolean",
        sys::LUA_TINTEGER | sys::LUA_TNUMBER => "number",
        sys::LUA_TSTRING => "string",
        sys::LUA_TTABLE => "table",
        sys::LUA_TFUNCTION => "function",
        sys::LUA_TTHREAD => "thread",
        sys::LUA_TBUFFER => "buffer",
        sys::LUA_TUSERDATA => "userdata",
        _ => "unknown",
    }
}

fn checked_i64_arg(name: Arc<str>, value: f64) -> Result<i64> {
    if value.is_finite()
        && value.fract() == 0.0
        && value >= i64::MIN as f64
        && value < 9_223_372_036_854_775_808.0
    {
        Ok(value as i64)
    } else {
        Err(Error::ArgumentType {
            name,
            expected: "integer",
            actual: "number",
        })
    }
}

#[derive(Clone)]
pub struct ReturnWriter<'vm> {
    state: NonNull<ReturnState>,
    _marker: PhantomData<&'vm ReturnState>,
}

impl<'vm> ReturnWriter<'vm> {
    fn borrowed(state: &'vm mut ReturnState) -> Self {
        Self {
            state: NonNull::from(state),
            _marker: PhantomData,
        }
    }

    pub fn write<T: ToLuau>(&mut self, value: T) -> Result<()> {
        value.write(self)
    }

    pub fn request_yield(&mut self) {
        self.with_state_mut(|state| state.yield_requested = true);
    }

    fn push(&mut self, value: Value) {
        self.with_state_mut(|state| state.values.push(value));
    }

    fn with_state_mut<T>(&mut self, callback: impl FnOnce(&mut ReturnState) -> T) -> T {
        unsafe { callback(self.state.as_mut()) }
    }
}

#[derive(Clone, Default)]
struct ReturnState {
    values: Vec<Value>,
    yield_requested: bool,
}

pub trait FromLuau<'vm>: Sized {
    fn read(reader: &mut ArgReader<'vm>) -> Result<Self>;
}

pub trait ToLuau {
    fn write(self, writer: &mut ReturnWriter<'_>) -> Result<()>;
}

#[derive(Clone, Debug, Default, PartialEq)]
pub enum ReturnValues {
    #[default]
    None,
    One(Value),
    Many(Vec<Value>),
}

impl ReturnValues {
    pub fn none() -> Self {
        Self::None
    }

    pub fn one(value: Value) -> Self {
        Self::One(value)
    }

    pub fn many(values: Vec<Value>) -> Self {
        match values.len() {
            0 => Self::None,
            1 => Self::One(values.into_iter().next().expect("single return value")),
            _ => Self::Many(values),
        }
    }

    pub fn as_slice(&self) -> &[Value] {
        match self {
            Self::None => &[],
            Self::One(value) => std::slice::from_ref(value),
            Self::Many(values) => values,
        }
    }

    pub fn write_to(self, writer: &mut ReturnWriter<'_>) -> Result<()> {
        match self {
            Self::None => {}
            Self::One(value) => writer.write(value)?,
            Self::Many(values) => {
                for value in values {
                    writer.write(value)?;
                }
            }
        }
        Ok(())
    }

    fn extend_into(self, values: &mut Vec<Value>) {
        match self {
            Self::None => {}
            Self::One(value) => values.push(value),
            Self::Many(mut many) => values.append(&mut many),
        }
    }
}

pub trait IntoLuauReturn {
    fn into_luau_return(self) -> Result<ReturnValues>;
}

impl IntoLuauReturn for ReturnValues {
    fn into_luau_return(self) -> Result<ReturnValues> {
        Ok(self)
    }
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Nil => "nil",
            Self::Boolean(_) => "boolean",
            Self::Integer(_) | Self::Number(_) => "number",
            Self::String(_) => "string",
            Self::Buffer(_) => "buffer",
            Self::TableData(_) => "table",
            Self::NativeFunction(_) => "function",
            Self::Table(_) => "table",
            Self::Function(_) => "function",
            Self::Thread(_) => "thread",
            Self::UserData(_) => "userdata",
        }
    }
}

impl<'vm> FromLuau<'vm> for Value {
    fn read(reader: &mut ArgReader<'vm>) -> Result<Self> {
        reader.next_value("any")
    }
}

impl ToLuau for Value {
    fn write(self, writer: &mut ReturnWriter<'_>) -> Result<()> {
        writer.push(self);
        Ok(())
    }
}

impl IntoLuauReturn for Value {
    fn into_luau_return(self) -> Result<ReturnValues> {
        Ok(ReturnValues::one(self))
    }
}

impl ToLuau for OwnedTable {
    fn write(self, writer: &mut ReturnWriter<'_>) -> Result<()> {
        writer.push(Value::TableData(self));
        Ok(())
    }
}

impl IntoLuauReturn for OwnedTable {
    fn into_luau_return(self) -> Result<ReturnValues> {
        Ok(ReturnValues::one(Value::TableData(self)))
    }
}

impl ToLuau for NativeFunctionValue {
    fn write(self, writer: &mut ReturnWriter<'_>) -> Result<()> {
        writer.push(Value::NativeFunction(self));
        Ok(())
    }
}

impl IntoLuauReturn for NativeFunctionValue {
    fn into_luau_return(self) -> Result<ReturnValues> {
        Ok(ReturnValues::one(Value::NativeFunction(self)))
    }
}

impl<'vm> FromLuau<'vm> for Table {
    fn read(reader: &mut ArgReader<'vm>) -> Result<Self> {
        match reader.next_value("table")? {
            Value::Table(value) => Ok(value),
            other => Err(Error::ArgumentType {
                name: Arc::from("value"),
                expected: "table",
                actual: other.type_name(),
            }),
        }
    }
}

impl ToLuau for Table {
    fn write(self, writer: &mut ReturnWriter<'_>) -> Result<()> {
        writer.push(Value::Table(self));
        Ok(())
    }
}

impl IntoLuauReturn for Table {
    fn into_luau_return(self) -> Result<ReturnValues> {
        Ok(ReturnValues::one(Value::Table(self)))
    }
}

impl<'vm> FromLuau<'vm> for Function {
    fn read(reader: &mut ArgReader<'vm>) -> Result<Self> {
        match reader.next_value("function")? {
            Value::Function(value) => Ok(value),
            other => Err(Error::ArgumentType {
                name: Arc::from("value"),
                expected: "function",
                actual: other.type_name(),
            }),
        }
    }
}

impl ToLuau for Function {
    fn write(self, writer: &mut ReturnWriter<'_>) -> Result<()> {
        writer.push(Value::Function(self));
        Ok(())
    }
}

impl IntoLuauReturn for Function {
    fn into_luau_return(self) -> Result<ReturnValues> {
        Ok(ReturnValues::one(Value::Function(self)))
    }
}

impl<'vm> FromLuau<'vm> for Thread {
    fn read(reader: &mut ArgReader<'vm>) -> Result<Self> {
        match reader.next_value("thread")? {
            Value::Thread(value) => Ok(value),
            other => Err(Error::ArgumentType {
                name: Arc::from("value"),
                expected: "thread",
                actual: other.type_name(),
            }),
        }
    }
}

impl ToLuau for Thread {
    fn write(self, writer: &mut ReturnWriter<'_>) -> Result<()> {
        writer.push(Value::Thread(self));
        Ok(())
    }
}

impl IntoLuauReturn for Thread {
    fn into_luau_return(self) -> Result<ReturnValues> {
        Ok(ReturnValues::one(Value::Thread(self)))
    }
}

impl<'vm> FromLuau<'vm> for UserData {
    fn read(reader: &mut ArgReader<'vm>) -> Result<Self> {
        match reader.next_value("userdata")? {
            Value::UserData(value) => Ok(value),
            other => Err(Error::ArgumentType {
                name: Arc::from("value"),
                expected: "userdata",
                actual: other.type_name(),
            }),
        }
    }
}

impl ToLuau for UserData {
    fn write(self, writer: &mut ReturnWriter<'_>) -> Result<()> {
        writer.push(Value::UserData(self));
        Ok(())
    }
}

impl IntoLuauReturn for UserData {
    fn into_luau_return(self) -> Result<ReturnValues> {
        Ok(ReturnValues::one(Value::UserData(self)))
    }
}

impl<'vm> FromLuau<'vm> for bool {
    fn read(reader: &mut ArgReader<'vm>) -> Result<Self> {
        reader.next_bool()
    }
}

impl ToLuau for bool {
    fn write(self, writer: &mut ReturnWriter<'_>) -> Result<()> {
        writer.push(Value::Boolean(self));
        Ok(())
    }
}

impl IntoLuauReturn for bool {
    fn into_luau_return(self) -> Result<ReturnValues> {
        Ok(ReturnValues::one(Value::Boolean(self)))
    }
}

impl<'vm> FromLuau<'vm> for i64 {
    fn read(reader: &mut ArgReader<'vm>) -> Result<Self> {
        reader.next_i64()
    }
}

impl ToLuau for i64 {
    fn write(self, writer: &mut ReturnWriter<'_>) -> Result<()> {
        writer.push(Value::from(self));
        Ok(())
    }
}

impl IntoLuauReturn for i64 {
    fn into_luau_return(self) -> Result<ReturnValues> {
        Ok(ReturnValues::one(Value::from(self)))
    }
}

impl<'vm> FromLuau<'vm> for f64 {
    fn read(reader: &mut ArgReader<'vm>) -> Result<Self> {
        reader.next_f64()
    }
}

impl ToLuau for f64 {
    fn write(self, writer: &mut ReturnWriter<'_>) -> Result<()> {
        writer.push(Value::Number(self));
        Ok(())
    }
}

impl IntoLuauReturn for f64 {
    fn into_luau_return(self) -> Result<ReturnValues> {
        Ok(ReturnValues::one(Value::Number(self)))
    }
}

impl<'vm> FromLuau<'vm> for ByteString {
    fn read(reader: &mut ArgReader<'vm>) -> Result<Self> {
        reader
            .next_borrowed_bytes("string")
            .map(|bytes| Self(bytes.to_vec()))
    }
}

impl ToLuau for ByteString {
    fn write(self, writer: &mut ReturnWriter<'_>) -> Result<()> {
        writer.push(Value::String(self.0));
        Ok(())
    }
}

impl IntoLuauReturn for ByteString {
    fn into_luau_return(self) -> Result<ReturnValues> {
        Ok(ReturnValues::one(Value::String(self.0)))
    }
}

impl ToLuau for Vec<u8> {
    fn write(self, writer: &mut ReturnWriter<'_>) -> Result<()> {
        writer.push(Value::String(self));
        Ok(())
    }
}

impl IntoLuauReturn for Vec<u8> {
    fn into_luau_return(self) -> Result<ReturnValues> {
        Ok(ReturnValues::one(Value::String(self)))
    }
}

impl<'vm> FromLuau<'vm> for String {
    fn read(reader: &mut ArgReader<'vm>) -> Result<Self> {
        let (name, bytes) = reader.next_named_borrowed_bytes("string")?;
        String::from_utf8(bytes.to_vec())
            .map_err(|error| Error::InvalidUtf8Argument { name, error })
    }
}

impl<'vm, T> FromLuau<'vm> for Option<T>
where
    T: FromLuau<'vm>,
{
    fn read(reader: &mut ArgReader<'vm>) -> Result<Self> {
        if reader.cursor.is_finished() {
            return Ok(None);
        }
        if reader.is_nil(reader.cursor.index()) {
            reader.cursor.advance();
            return Ok(None);
        }
        T::read(reader).map(Some)
    }
}

impl<T> ToLuau for Option<T>
where
    T: ToLuau,
{
    fn write(self, writer: &mut ReturnWriter<'_>) -> Result<()> {
        match self {
            Some(value) => value.write(writer),
            None => {
                writer.push(Value::Nil);
                Ok(())
            }
        }
    }
}

impl<T> IntoLuauReturn for Option<T>
where
    T: IntoLuauReturn,
{
    fn into_luau_return(self) -> Result<ReturnValues> {
        match self {
            Some(value) => value.into_luau_return(),
            None => Ok(ReturnValues::one(Value::Nil)),
        }
    }
}

impl<'vm> FromLuau<'vm> for Vec<u8> {
    fn read(reader: &mut ArgReader<'vm>) -> Result<Self> {
        reader.next_borrowed_bytes("string").map(ToOwned::to_owned)
    }
}

impl ToLuau for String {
    fn write(self, writer: &mut ReturnWriter<'_>) -> Result<()> {
        writer.push(Value::String(self.into_bytes()));
        Ok(())
    }
}

impl IntoLuauReturn for String {
    fn into_luau_return(self) -> Result<ReturnValues> {
        Ok(ReturnValues::one(Value::String(self.into_bytes())))
    }
}

impl ToLuau for &str {
    fn write(self, writer: &mut ReturnWriter<'_>) -> Result<()> {
        writer.push(Value::String(self.as_bytes().to_vec()));
        Ok(())
    }
}

impl IntoLuauReturn for &str {
    fn into_luau_return(self) -> Result<ReturnValues> {
        Ok(ReturnValues::one(Value::String(self.as_bytes().to_vec())))
    }
}

impl ToLuau for () {
    fn write(self, _writer: &mut ReturnWriter<'_>) -> Result<()> {
        Ok(())
    }
}

impl IntoLuauReturn for () {
    fn into_luau_return(self) -> Result<ReturnValues> {
        Ok(ReturnValues::none())
    }
}

macro_rules! impl_into_luau_return_tuple {
    ($($name:ident),+ $(,)?) => {
        impl<$($name),+> IntoLuauReturn for ($($name,)+)
        where
            $($name: IntoLuauReturn,)+
        {
            #[allow(non_snake_case)]
            fn into_luau_return(self) -> Result<ReturnValues> {
                let ($($name,)+) = self;
                let mut values = Vec::new();
                $(
                    $name.into_luau_return()?.extend_into(&mut values);
                )+
                Ok(ReturnValues::many(values))
            }
        }
    };
}

impl_into_luau_return_tuple!(A, B);
impl_into_luau_return_tuple!(A, B, C);
impl_into_luau_return_tuple!(A, B, C, D);

pub struct ScheduledFuture {
    start_delay: Option<Duration>,
    state: ScheduledFutureState,
}

enum ScheduledFutureState {
    Future(std::pin::Pin<Box<dyn Future<Output = Result<ReturnValues>> + 'static>>),
}

impl ScheduledFuture {
    pub fn new<F, T>(future: F) -> Self
    where
        F: Future<Output = Result<T>> + 'static,
        T: IntoLuauReturn + 'static,
    {
        Self::with_start_delay(None, future)
    }

    pub fn after<F, T>(delay: Duration, future: F) -> Self
    where
        F: Future<Output = Result<T>> + 'static,
        T: IntoLuauReturn + 'static,
    {
        Self::with_start_delay(Some(delay), future)
    }

    pub fn take_start_delay(&mut self) -> Option<Duration> {
        self.start_delay.take()
    }

    fn with_start_delay<F, T>(start_delay: Option<Duration>, future: F) -> Self
    where
        F: Future<Output = Result<T>> + 'static,
        T: IntoLuauReturn + 'static,
    {
        Self {
            start_delay,
            state: ScheduledFutureState::Future(Box::pin(async move {
                future.await?.into_luau_return()
            })),
        }
    }
}

impl Future for ScheduledFuture {
    type Output = Result<ReturnValues>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match &mut self.state {
            ScheduledFutureState::Future(future) => future.as_mut().poll(cx),
        }
    }
}

pub type NativeFn = Arc<dyn for<'vm> Fn(CallFrame<'vm>) -> Result<()> + Send + Sync + 'static>;
pub type NativeAsyncFn =
    Arc<dyn for<'vm> Fn(AsyncCallFrame<'vm>) -> Result<ScheduledFuture> + Send + Sync + 'static>;

#[derive(Clone, Debug)]
pub struct NativeFunctionOptions {
    pub origin: ChunkOrigin,
    pub capability: Option<CapabilityId>,
    pub task_group: TaskGroupId,
    pub function_name: Option<Arc<str>>,
    pub argument_names: Vec<Arc<str>>,
    pub use_thread_context_origin: bool,
}

impl NativeFunctionOptions {
    pub fn new(origin: ChunkOrigin) -> Self {
        Self {
            origin,
            capability: None,
            task_group: TaskGroupId(0),
            function_name: None,
            argument_names: Vec::new(),
            use_thread_context_origin: false,
        }
    }

    pub fn capability(mut self, capability: CapabilityId) -> Self {
        self.capability = Some(capability);
        self
    }

    pub fn task_group(mut self, task_group: TaskGroupId) -> Self {
        self.task_group = task_group;
        self
    }

    pub fn function_name(mut self, function_name: impl Into<Arc<str>>) -> Self {
        self.function_name = Some(function_name.into());
        self
    }

    pub fn argument_names(mut self, names: impl IntoIterator<Item = Arc<str>>) -> Self {
        self.argument_names = names.into_iter().collect();
        self
    }

    pub fn use_thread_context_origin(mut self, enabled: bool) -> Self {
        self.use_thread_context_origin = enabled;
        self
    }
}

struct CallbackSlot {
    vm: RcWeak<VmInner>,
    vm_id: u64,
    callback: NativeFn,
    origin: ChunkOrigin,
    capability: Option<CapabilityId>,
    task_group: TaskGroupId,
    function_name: Option<Arc<str>>,
    argument_names: Vec<Arc<str>>,
    use_thread_context_origin: bool,
}

#[derive(Clone, Default)]
pub struct CallContext {
    pub origin: ChunkOrigin,
    pub capability: Option<CapabilityId>,
    pub caller: ContextBag,
    pub task_group: TaskGroupId,
}

#[derive(Clone, Default)]
pub struct ContextBag {
    values: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl ContextBag {
    pub fn insert<T>(&mut self, value: T)
    where
        T: Send + Sync + 'static,
    {
        self.values.insert(TypeId::of::<T>(), Arc::new(value));
    }

    pub fn get<T>(&self) -> Result<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.values
            .get(&TypeId::of::<T>())
            .cloned()
            .and_then(|value| value.downcast::<T>().ok())
            .ok_or(Error::MissingContext {
                type_name: type_name::<T>(),
            })
    }
}

#[derive(Clone, Default)]
struct LocalContextBag {
    values: HashMap<TypeId, Rc<dyn Any>>,
}

impl LocalContextBag {
    fn insert<T>(&mut self, value: T)
    where
        T: 'static,
    {
        self.values.insert(TypeId::of::<T>(), Rc::new(value));
    }

    fn get<T>(&self) -> Result<Rc<T>>
    where
        T: 'static,
    {
        self.values
            .get(&TypeId::of::<T>())
            .cloned()
            .and_then(|value| value.downcast::<T>().ok())
            .ok_or(Error::MissingContext {
                type_name: type_name::<T>(),
            })
    }
}

/// A value a native function takes from its call, rather than from its arguments.
pub trait FromCallFrame: Sized {
    fn from_call_frame(vm: &Vm, context: &CallContext) -> Result<Self>;
}

pub struct CallFrame<'vm> {
    pub vm: &'vm Vm,
    pub thread: Thread,
    pub context: CallContext,
    pub args: ArgReader<'vm>,
    pub returns: ReturnWriter<'vm>,
}

impl<'vm> CallFrame<'vm> {
    pub fn into_async(self) -> AsyncCallFrame<'vm> {
        AsyncCallFrame {
            vm: self.vm,
            thread: self.thread,
            context: self.context,
            args: self.args,
        }
    }

    pub fn yield_now(&mut self) {
        self.returns.request_yield();
    }
}

pub struct AsyncCallFrame<'vm> {
    pub vm: &'vm Vm,
    pub thread: Thread,
    pub context: CallContext,
    pub args: ArgReader<'vm>,
}

/// Runs Rust code that Luau called. A panic aborts instead of unwinding into Luau's C++ frames,
/// which would leave the VM's protected-call state pointing at frames that no longer exist.
fn abort_on_panic<T>(callback: impl FnOnce() -> T) -> T {
    catch_unwind(AssertUnwindSafe(callback)).unwrap_or_else(|_| std::process::abort())
}

unsafe extern "C-unwind" fn drop_callback_slot(_: *mut sys::lua_State, userdata: *mut c_void) {
    abort_on_panic(|| unsafe { ptr::drop_in_place(userdata.cast::<CallbackSlot>()) });
}

unsafe extern "C-unwind" fn drop_userdata<T>(_: *mut sys::lua_State, userdata: *mut c_void) {
    abort_on_panic(|| unsafe { ptr::drop_in_place(userdata.cast::<T>()) });
}

enum CallbackExit {
    Return(c_int),
    Yield(c_int),
    Raise,
}

/// Luau's entry into a native function. Everything with a destructor lives in
/// [`call_native_function`], so this frame holds nothing a raise would skip.
unsafe extern "C-unwind" fn native_callback(state: *mut sys::lua_State) -> c_int {
    let exit = abort_on_panic(|| match unsafe { call_native_function(state) } {
        Ok(exit) => exit,
        Err(message) => {
            unsafe { gateway::push_error(state, &message) };
            CallbackExit::Raise
        }
    });
    match exit {
        CallbackExit::Return(count) => count,
        CallbackExit::Yield(count) => unsafe { gateway::yield_results(state, count) },
        CallbackExit::Raise => unsafe { gateway::raise(state) },
    }
}

/// Runs a native function and leaves its results on top of the stack, or returns the message to
/// raise.
unsafe fn call_native_function(
    state_ptr: *mut sys::lua_State,
) -> std::result::Result<CallbackExit, String> {
    let slot = unsafe {
        sys::lua_touserdata(state_ptr, sys::lua_upvalueindex(1))
            .cast::<CallbackSlot>()
            .as_ref()
    }
    .ok_or("missing Harmony callback slot")?;
    let inner = slot
        .vm
        .upgrade()
        .ok_or("Harmony VM is no longer available")?;
    let state = NonNull::new(state_ptr).ok_or("missing Luau state")?;
    let vm = Vm { inner };
    let thread_context = vm
        .call_context_for_state(state)
        .map_err(|error| error.to_string())?;
    let thread_reference = vm
        .ref_current_thread_from(state)
        .map_err(|error| error.to_string())?;
    let mut return_state = ReturnState::default();
    let result = {
        let returns = ReturnWriter::borrowed(&mut return_state);
        let argc = unsafe { sys::lua_gettop(state_ptr) };
        let caller = thread_context
            .as_ref()
            .map(|context| context.caller.clone())
            .unwrap_or_default();
        // A function installed for another plugin acts in that plugin's run.
        let task_group = match &thread_context {
            Some(context)
                if slot.use_thread_context_origin
                    || slot.origin.plugin.is_none()
                    || slot.origin.plugin == context.origin.plugin =>
            {
                context.task_group
            }
            _ => slot.task_group,
        };
        let origin = if slot.use_thread_context_origin {
            thread_context
                .as_ref()
                .map(|context| context.origin.clone())
                .unwrap_or_else(|| slot.origin.clone())
        } else {
            slot.origin.clone()
        };
        let frame = CallFrame {
            vm: &vm,
            thread: Thread::new(
                Some(thread_reference),
                state,
                slot.vm_id,
                origin.clone(),
                true,
                false,
            ),
            context: CallContext {
                origin,
                capability: slot.capability.clone(),
                caller,
                task_group,
            },
            args: ArgReader::stack(&vm, state, argc, slot.argument_names.clone()),
            returns: returns.clone(),
        };

        catch_unwind(AssertUnwindSafe(|| (slot.callback)(frame)))
    };
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => return Err(format_callback_error(slot, &error)),
        Err(_) => return Err("Harmony callback panicked".to_string()),
    }
    for value in &return_state.values {
        vm.push_value_to(state, value)
            .map_err(|error| error.to_string())?;
    }
    let count = return_state.values.len() as c_int;
    if !return_state.yield_requested {
        return Ok(CallbackExit::Return(count));
    }
    if unsafe { sys::lua_isyieldable(state_ptr) } == 0 {
        return Err("attempt to yield from a non-yieldable Harmony callback".to_string());
    }
    Ok(CallbackExit::Yield(count))
}

/// Prefixes a callback's error with where it came from, except running out of memory, which keeps
/// Luau's own message so callers further out still see [`Error::OutOfMemory`].
fn format_callback_error(slot: &CallbackSlot, error: &Error) -> String {
    if matches!(error, Error::OutOfMemory) {
        return error.to_string();
    }
    let module = slot
        .origin
        .module
        .as_ref()
        .map(|module| module.0.as_ref())
        .unwrap_or("<unknown module>");
    let function = slot
        .function_name
        .as_deref()
        .unwrap_or("<unknown function>");
    let plugin = slot.origin.plugin.as_deref().unwrap_or("<non-plugin>");
    format!("module '{module}' function '{function}' plugin '{plugin}': {error}")
}

fn current_unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs().min(i64::MAX as u64) as i64)
        .unwrap_or(0)
}

/// Error message raised in a thread that exceeds its `interrupt_after` budget.
pub const INTERRUPTED_ERROR: &str = "Luau execution interrupted";

fn current_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

fn format_os_date(format: &str, timestamp: i64) -> Result<String> {
    if format != "!%Y-%m-%dT%H:%M:%SZ" {
        return Err(Error::Runtime(format!(
            "unsupported os.date format: {format}"
        )));
    }

    let dt = OffsetDateTime::from_unix_timestamp(timestamp)
        .map_err(|error| Error::Runtime(format!("invalid os.date timestamp: {error}")))?;
    Ok(format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        dt.year(),
        u8::from(dt.month()),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    ))
}

/// A new thread, such as a coroutine, starts with its creator's call context;
/// a freed thread's context goes with it.
unsafe extern "C-unwind" fn userthread(parent: *mut sys::lua_State, thread: *mut sys::lua_State) {
    let control = unsafe {
        sys::lua_callback_userdata(thread)
            .cast::<VmControl>()
            .as_ref()
    };
    let Some(control) = control else {
        return;
    };
    let Ok(mut contexts) = control.thread_contexts.write() else {
        return;
    };
    if parent.is_null() {
        contexts.remove(&(thread as usize));
    } else if let Some(context) = contexts.get(&(parent as usize)).cloned() {
        contexts.insert(thread as usize, context);
    }
}

unsafe extern "C-unwind" fn interrupt(state: *mut sys::lua_State, gc: c_int) {
    if state.is_null() || gc >= 0 {
        return;
    }
    let expired = abort_on_panic(|| {
        let control = unsafe {
            sys::lua_callback_userdata(state)
                .cast::<VmControl>()
                .as_ref()
        };
        control.is_some_and(|control| {
            let deadline = control.interrupt_deadline_millis.load(Ordering::Acquire);
            deadline != 0 && current_unix_millis() >= deadline
        })
    });
    if expired {
        unsafe {
            gateway::push_error(state, INTERRUPTED_ERROR);
            gateway::raise(state);
        }
    }
}

fn compile(source: &[u8], options: CompileOptions) -> Result<CompiledBytecode> {
    let mut compile_options = sys::lua_CompileOptions {
        optimizationLevel: options.optimization_level,
        debugLevel: options.debug_level,
        typeInfoLevel: options.type_info_level,
        mutableGlobals: ptr::null(),
        disabledBuiltins: ptr::null(),
    };
    let mut len = 0usize;
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

struct CompiledBytecode {
    ptr: NonNull<std::ffi::c_char>,
    len: usize,
}

impl CompiledBytecode {
    fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr().cast(), self.len) }
    }
}

impl Drop for CompiledBytecode {
    fn drop(&mut self) {
        unsafe {
            sys::luau_free_compiled_bytecode(self.ptr.as_ptr());
        }
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
    abort_on_panic(|| unsafe { alloc_within_limit(ud, ptr, osize, nsize) })
}

unsafe fn alloc_within_limit(
    ud: *mut c_void,
    ptr: *mut c_void,
    osize: usize,
    nsize: usize,
) -> *mut c_void {
    let control = unsafe { ud.cast::<VmControl>().as_ref() };
    if nsize == 0 {
        unsafe { reallocate(ptr, osize, 0) };
        if let Some(control) = control {
            let allocated = control.allocated.load(Ordering::Relaxed);
            control
                .allocated
                .store(allocated.saturating_sub(osize), Ordering::Relaxed);
        }
        ptr::null_mut()
    } else {
        if let Some(control) = control {
            let allocated = control.allocated.load(Ordering::Relaxed);
            let next_allocated = match allocated.saturating_sub(osize).checked_add(nsize) {
                Some(value) => value,
                None => return ptr::null_mut(),
            };
            let limit = control.memory_limit.load(Ordering::Relaxed);
            if limit != 0 && next_allocated > limit {
                return ptr::null_mut();
            }
            let result = unsafe { reallocate(ptr, osize, nsize) };
            if !result.is_null() {
                control.allocated.store(next_allocated, Ordering::Relaxed);
            }
            result
        } else {
            unsafe { reallocate(ptr, osize, nsize) }
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn stack_top(vm: &Vm) -> i32 {
        vm.top()
    }

    #[test]
    fn owned_tables_build_from_rust_values() {
        let array = OwnedTable::from_array(["a", "b"]);
        assert_eq!(
            array.array(),
            [Value::String(b"a".to_vec()), Value::String(b"b".to_vec())]
        );

        let map = OwnedTable::from_entries([(1_u64, Some(true)), (2_u64, None)]);
        assert_eq!(
            map.entries(),
            [
                (Value::Number(1.0), Value::Boolean(true)),
                (Value::Number(2.0), Value::Nil),
            ]
        );

        let mut record = OwnedTable::new();
        record.set_field("name", String::from("demo"));
        record.set_field("tags", array);
        assert_eq!(
            record.fields(),
            [
                ("name".to_string(), Value::String(b"demo".to_vec())),
                (
                    "tags".to_string(),
                    Value::TableData(OwnedTable::from_array(["a", "b"]))
                ),
            ]
        );
    }

    #[cfg(feature = "anyhow")]
    #[test]
    fn host_errors_keep_their_message_and_running_out_of_memory() {
        let error = Error::from(anyhow::anyhow!("track not found").context("loading track"));
        assert!(matches!(error, Error::Runtime(message) if message == "loading track"));

        let error = Error::from(anyhow::Error::new(Error::OutOfMemory).context("loading track"));
        assert!(matches!(error, Error::OutOfMemory));
    }

    #[test]
    fn userdata_tags_accept_only_luau_tag_range() -> Result<()> {
        assert_eq!(UserDataTag::new(0)?, UserDataTag(0));
        assert_eq!(
            UserDataTag::new(sys::LUA_UTAG_LIMIT - 1)?,
            UserDataTag(sys::LUA_UTAG_LIMIT - 1)
        );

        assert!(matches!(
            UserDataTag::new(-1),
            Err(Error::InvalidUserDataTag { tag: -1 })
        ));
        assert!(matches!(
            UserDataTag::new(sys::LUA_UTAG_LIMIT),
            Err(Error::InvalidUserDataTag { tag }) if tag == sys::LUA_UTAG_LIMIT
        ));
        Ok(())
    }

    #[test]
    fn local_metadata_stores_typed_values_without_vm() -> Result<()> {
        #[derive(Debug, PartialEq, Eq)]
        struct Label(&'static str);

        let vm_data = VmData::default();
        vm_data.insert(Label("vm"))?;
        assert_eq!(*vm_data.get::<Label>()?, Label("vm"));
        assert!(matches!(
            vm_data.get::<String>(),
            Err(Error::MissingContext { .. })
        ));

        let thread_data = ThreadData::default();
        thread_data.insert(Label("thread"))?;
        assert_eq!(*thread_data.get::<Label>()?, Label("thread"));
        assert!(matches!(
            thread_data.get::<String>(),
            Err(Error::MissingContext { .. })
        ));
        Ok(())
    }

    #[test]
    fn arg_cursor_tracks_labels_and_remaining_arguments() {
        let mut cursor = ArgCursor::new(3, [Arc::from("given")]);

        assert_eq!(cursor.len(), 3);
        assert_eq!(cursor.index(), 0);
        assert_eq!(cursor.current_name().as_ref(), "given");

        cursor.advance();
        assert_eq!(cursor.current_name().as_ref(), "arg2");
        cursor.label_current("named");
        assert_eq!(cursor.current_name().as_ref(), "named");

        cursor.advance();
        assert_eq!(cursor.drain_remaining().collect::<Vec<_>>(), vec![2]);
        assert!(cursor.is_finished());

        assert_eq!(ArgCursor::new(-1, []).len(), 0);
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn vm_compiles_and_runs_chunk_with_origin() -> Result<()> {
        let vm = Vm::new()?;
        let values = vm.eval(
            Arc::<[u8]>::from(&b"return 40 + 2"[..]),
            ChunkOrigin {
                module: Some(ModuleId(Arc::from("test/module"))),
                plugin: Some(Arc::from("demo")),
                path: Some(Arc::from("plugins/demo/init.luau")),
            },
        )?;

        assert_eq!(values, vec![Value::Number(42.0)]);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn chunks_return_table_and_function_handles() -> Result<()> {
        let vm = Vm::new()?;
        let values = vm.eval(
            Arc::<[u8]>::from(
                &b"local t = { answer = 42 }; return t, function(x) return x + t.answer end"[..],
            ),
            ChunkOrigin::default(),
        )?;

        assert_eq!(values.len(), 2);
        let Value::Table(table) = values[0].clone() else {
            panic!("expected first return value to be a table");
        };
        assert_eq!(table.get_raw(&vm, "answer")?, Value::Number(42.0));

        let Value::Function(function) = values[1].clone() else {
            panic!("expected second return value to be a function");
        };
        assert_eq!(
            function.call(&vm, &[Value::Number(8.0)])?,
            vec![Value::Number(50.0)]
        );
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn vm_opens_only_requested_standard_libraries() -> Result<()> {
        let vm = Vm::new()?;
        let missing_string = vm.eval(
            Arc::<[u8]>::from(&b"return string.upper('lyra')"[..]),
            ChunkOrigin::default(),
        );
        assert!(matches!(missing_string, Err(Error::Runtime(_))));

        vm.open_standard_libraries(StandardLibraries {
            string: true,
            table: true,
            ..StandardLibraries::none()
        })?;
        let values = vm.eval(
            Arc::<[u8]>::from(&b"return string ~= nil, table ~= nil, math == nil"[..]),
            ChunkOrigin::default(),
        )?;

        assert_eq!(
            values,
            vec![
                Value::Boolean(true),
                Value::Boolean(true),
                Value::Boolean(true)
            ]
        );
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn safe_os_library_exposes_only_plugin_required_functions() -> Result<()> {
        let vm = Vm::new()?;
        let missing_os = vm.eval(
            Arc::<[u8]>::from(&b"return os.time()"[..]),
            ChunkOrigin::default(),
        );
        assert!(matches!(missing_os, Err(Error::Runtime(_))));

        vm.open_standard_libraries(StandardLibraries {
            base: true,
            os: true,
            ..StandardLibraries::none()
        })?;
        let values = vm.eval(
            Arc::<[u8]>::from(
                &br#"
                return
                    type(os.time()),
                    type(os.clock()),
                    os.date("!%Y-%m-%dT%H:%M:%SZ", 0),
                    os.execute,
                    os.getenv,
                    os.remove
                "#[..],
            ),
            ChunkOrigin::default(),
        )?;

        assert_eq!(
            values,
            vec![
                Value::String(b"number".to_vec()),
                Value::String(b"number".to_vec()),
                Value::String(b"1970-01-01T00:00:00Z".to_vec()),
                Value::Nil,
                Value::Nil,
                Value::Nil
            ]
        );

        let unsupported_date = vm.eval(
            Arc::<[u8]>::from(&br#"return os.date("%x", 0)"#[..]),
            ChunkOrigin::default(),
        );
        assert!(matches!(
            unsupported_date,
            Err(Error::Runtime(message)) if message.contains("unsupported os.date format")
        ));
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn sandbox_freezes_opened_standard_libraries_and_globals() -> Result<()> {
        let vm = Vm::new()?;
        vm.open_standard_libraries(StandardLibraries::all_supported())?;
        vm.sandbox()?;

        assert!(matches!(
            vm.eval(
                Arc::<[u8]>::from(&b"string.extra = true"[..]),
                ChunkOrigin::default(),
            ),
            Err(Error::Runtime(_))
        ));
        assert!(matches!(
            vm.eval(
                Arc::<[u8]>::from(&b"_G.extra = true"[..]),
                ChunkOrigin::default(),
            ),
            Err(Error::Runtime(_))
        ));
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn stack_guard_restores_after_load_and_runtime_errors() -> Result<()> {
        let vm = Vm::new()?;

        let bad_load = vm.eval(
            Arc::<[u8]>::from(&b"local ="[..]),
            ChunkOrigin {
                path: Some(Arc::from("plugins/demo/bad.luau")),
                ..ChunkOrigin::default()
            },
        );
        assert!(matches!(bad_load, Err(Error::Load(_))));
        assert_eq!(stack_top(&vm), 0);

        let bad_runtime = vm.eval(
            Arc::<[u8]>::from(&b"return missing + 1"[..]),
            ChunkOrigin {
                path: Some(Arc::from("plugins/demo/runtime.luau")),
                ..ChunkOrigin::default()
            },
        );
        assert!(matches!(bad_runtime, Err(Error::Runtime(_))));
        assert_eq!(stack_top(&vm), 0);

        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn table_raw_access_uses_vm_checked_handles() -> Result<()> {
        let vm = Vm::new()?;
        let table = vm.create_table()?;

        table.set_raw(&vm, "name", Value::String(b"demo".to_vec()))?;
        table.set_raw(&vm, "count", Value::Integer(7))?;
        table.set_integer_raw(&vm, 1, Value::String(b"first".to_vec()))?;
        table.set_integer_raw(&vm, 2, Value::Integer(9))?;

        assert_eq!(table.vm_id(), vm.id());
        assert_eq!(table.get_raw(&vm, "name")?, Value::String(b"demo".to_vec()));
        assert_eq!(table.get_raw(&vm, "count")?, Value::Integer(7));
        assert_eq!(table.raw_len(&vm)?, 2);
        assert_eq!(
            table.get_integer_raw(&vm, 1)?,
            Value::String(b"first".to_vec())
        );
        assert_eq!(
            table.array_values_raw(&vm)?,
            vec![Value::String(b"first".to_vec()), Value::Integer(9)]
        );
        {
            let reader = table.reader(&vm)?;
            assert_eq!(reader.raw_len(), 2);
            assert_eq!(reader.get_raw("name")?, Value::String(b"demo".to_vec()));
            assert_eq!(reader.get_integer_raw(2)?, Value::Integer(9));
            assert_eq!(
                reader.get_fields_raw(&["name", "count"])?,
                vec![Value::String(b"demo".to_vec()), Value::Integer(7)]
            );
            assert_eq!(
                reader.array_values_raw()?,
                vec![Value::String(b"first".to_vec()), Value::Integer(9)]
            );
        }
        assert!(table.pairs_raw(&vm)?.contains(&(
            Value::String(b"name".to_vec()),
            Value::String(b"demo".to_vec())
        )));
        assert!(table.pairs_raw(&vm)?.iter().any(|(key, value)| {
            matches!(key, Value::Integer(1) | Value::Number(1.0))
                && *value == Value::String(b"first".to_vec())
        }));
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn table_metatable_raw_access_uses_vm_checked_handles() -> Result<()> {
        let vm = Vm::new()?;
        let table = vm.create_table()?;
        let metatable = vm.create_table()?;

        metatable.set_raw(&vm, "marker", Value::Boolean(true))?;
        table.set_metatable_raw(&vm, Some(&metatable))?;
        let returned = table
            .metatable_raw(&vm)?
            .expect("table should have metatable");

        assert_eq!(returned.get_raw(&vm, "marker")?, Value::Boolean(true));
        table.set_metatable_raw(&vm, None)?;
        assert!(table.metatable_raw(&vm)?.is_none());
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn table_handles_fail_fast_across_vms() -> Result<()> {
        let owner = Vm::new()?;
        let other = Vm::new()?;
        let table = owner.create_table()?;

        let error = table
            .get_raw(&other, "name")
            .expect_err("cross-VM table access must fail");

        assert!(matches!(error, Error::VmMismatch { .. }));
        assert_eq!(stack_top(&owner), 0);
        assert_eq!(stack_top(&other), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn buffer_handles_round_trip_bytes_and_fail_fast_across_vms() -> Result<()> {
        let owner = Vm::new()?;
        let other = Vm::new()?;
        let buffer = owner.create_buffer(b"abc")?;

        assert_eq!(buffer.vm_id(), owner.id());
        assert_eq!(buffer.to_vec(&owner)?, b"abc".to_vec());
        assert!(matches!(
            buffer
                .to_vec(&other)
                .expect_err("cross-VM buffer access must fail"),
            Error::VmMismatch { .. }
        ));
        assert_eq!(stack_top(&owner), 0);
        assert_eq!(stack_top(&other), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn userdata_handles_store_vm_owned_rust_values() -> Result<()> {
        #[derive(Debug, PartialEq)]
        struct PluginObject {
            value: i64,
        }

        let vm = Vm::new()?;
        let tag = UserDataTag::new(1)?;
        let other_tag = UserDataTag::new(2)?;
        let userdata = vm.create_userdata(tag, PluginObject { value: 42 })?;

        assert_eq!(userdata.vm_id(), vm.id());
        assert_eq!(userdata.tag(), tag);
        assert_eq!(userdata.borrow::<PluginObject>(&vm, tag)?.value, 42);
        assert!(matches!(
            userdata.borrow::<PluginObject>(&vm, other_tag),
            Err(Error::UserDataTypeMismatch { .. })
        ));

        let callback_tag = tag;
        let callback: NativeFn = Arc::new(move |mut frame| {
            let userdata: UserData = frame.args.read_named("object")?;
            let object = userdata.borrow::<PluginObject>(frame.vm, callback_tag)?;
            frame.returns.write(object.value)?;
            Ok(())
        });
        let function = vm.create_function_with_options(
            NativeFunctionOptions::new(ChunkOrigin::default())
                .function_name("value")
                .argument_names([Arc::from("object")]),
            callback,
        )?;

        assert_eq!(
            function.call(&vm, &[Value::UserData(userdata)])?,
            vec![Value::Number(42.0)]
        );
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn clearing_vm_data_frees_a_vm_its_values_reference() -> Result<()> {
        struct DropFlag(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        struct Holder(#[allow(dead_code)] Function);

        let run = |clear: bool| -> Result<bool> {
            let freed = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let vm = Vm::new()?;
            let userdata = vm.create_userdata(UserDataTag::new(1)?, DropFlag(freed.clone()))?;
            let function = vm.load_chunk(&Chunk::new(
                Arc::<[u8]>::from(&b"local held = ...; return function() return held end"[..]),
                ChunkOrigin::default(),
            ))?;
            let Some(Value::Function(closure)) = function
                .call(&vm, &[Value::UserData(userdata)])?
                .into_iter()
                .next()
            else {
                panic!("the chunk returns a closure");
            };
            vm.data().insert(Holder(closure))?;
            if clear {
                vm.data().clear()?;
            }
            drop(function);
            drop(vm);
            Ok(freed.load(Ordering::SeqCst))
        };

        assert!(!run(false)?, "a VM its own data references stays alive");
        assert!(run(true)?, "clearing the data lets the VM close");
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn thread_resumes_loaded_function_with_owned_args() -> Result<()> {
        let vm = Vm::new()?;
        let function = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(&b"local lhs, rhs = ...; return lhs + rhs"[..]),
            ChunkOrigin {
                module: Some(ModuleId(Arc::from("test/thread"))),
                ..ChunkOrigin::default()
            },
        ))?;
        let thread = vm.create_thread(&function)?;

        assert_eq!(
            thread.resume(&vm, &[Value::Number(20.0), Value::Number(22.0)])?,
            ThreadStatus::Completed(vec![Value::Number(42.0)])
        );
        assert!(matches!(
            thread.resume(&vm, &[]),
            Err(Error::Runtime(message)) if message.contains("already completed")
        ));
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn interrupt_budget_stops_cpu_bound_thread() -> Result<()> {
        let vm = Vm::new()?;
        let function = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(&b"while true do end"[..]),
            ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&function)?;

        let _guard = vm.interrupt_after(Duration::ZERO);
        let error = thread
            .resume(&vm, &[])
            .expect_err("interrupt budget should stop the loop");

        assert!(matches!(
            error,
            Error::Runtime(message) if message.contains("interrupted")
        ));
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn thread_handles_fail_fast_across_vms() -> Result<()> {
        let owner = Vm::new()?;
        let other = Vm::new()?;
        let function = owner.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(&b"return 1"[..]),
            ChunkOrigin::default(),
        ))?;
        let thread = owner.create_thread(&function)?;

        assert!(matches!(
            thread
                .resume(&other, &[])
                .expect_err("cross-VM thread access must fail"),
            Error::VmMismatch { .. }
        ));
        assert_eq!(stack_top(&owner), 0);
        assert_eq!(stack_top(&other), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn lua_thread_values_are_preserved_and_resumable() -> Result<()> {
        let vm = Vm::new()?;
        vm.open_standard_libraries(StandardLibraries {
            coroutine: true,
            ..StandardLibraries::none()
        })?;

        let values = vm.eval(
            Arc::<[u8]>::from(
                &b"return coroutine.create(function(value) return value + 1 end)"[..],
            ),
            ChunkOrigin::default(),
        )?;
        let [Value::Thread(thread)] = values.as_slice() else {
            panic!("expected coroutine.create to return a thread");
        };

        assert_eq!(
            thread.resume(&vm, &[Value::Number(41.0)])?,
            ThreadStatus::Completed(vec![Value::Number(42.0)])
        );
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    fn task_group_probe(vm: &Vm) -> Result<()> {
        let probe: NativeFn = Arc::new(|mut frame| {
            frame.returns.write(frame.context.task_group.0 as f64)?;
            Ok(())
        });
        let probe = vm.create_function_with_options(
            NativeFunctionOptions::new(ChunkOrigin::default()).function_name("probe"),
            probe,
        )?;
        vm.set_global_function("probe", &probe)
    }

    fn context_in_group(group: u64) -> CallContext {
        CallContext {
            task_group: TaskGroupId(group),
            ..CallContext::default()
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn base_library_leaves_out_environment_access() -> Result<()> {
        let vm = Vm::new()?;
        vm.open_standard_libraries(StandardLibraries {
            base: true,
            ..StandardLibraries::none()
        })?;
        assert_eq!(
            vm.eval(
                Arc::<[u8]>::from(&b"return getfenv, setfenv"[..]),
                ChunkOrigin::default()
            )?,
            vec![Value::Nil, Value::Nil]
        );
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn chunks_loaded_with_globals_keep_their_own_and_share_the_rest() -> Result<()> {
        let vm = Vm::new()?;
        vm.open_standard_libraries(StandardLibraries {
            base: true,
            ..StandardLibraries::none()
        })?;
        let function = vm.load_chunk_with_globals(
            &Chunk::new(
                Arc::<[u8]>::from(
                    &b"--!strict\nshared = own\nlocal function read() return own end\nreturn read(), select('#', ...), ..."[..],
                ),
                ChunkOrigin::default(),
            ),
            &[("own", &Value::Number(7.0))],
        )?;
        assert_eq!(
            function.call(&vm, &[Value::Number(1.0), Value::Number(2.0)])?,
            vec![
                Value::Number(7.0),
                Value::Number(2.0),
                Value::Number(1.0),
                Value::Number(2.0)
            ]
        );
        assert_eq!(
            vm.eval(
                Arc::<[u8]>::from(&b"return shared, own"[..]),
                ChunkOrigin::default()
            )?,
            vec![Value::Number(7.0), Value::Nil]
        );
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn coroutines_inherit_their_creators_call_context() -> Result<()> {
        let vm = Vm::new()?;
        vm.open_standard_libraries(StandardLibraries {
            coroutine: true,
            ..StandardLibraries::none()
        })?;
        task_group_probe(&vm)?;
        let function = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(
                &br#"
                    local created = coroutine.create(function()
                        return probe()
                    end)
                    local _, from_created = coroutine.resume(created)
                    local from_wrapped = coroutine.wrap(function()
                        return coroutine.wrap(function()
                            return probe()
                        end)()
                    end)()
                    later = coroutine.create(function()
                        return probe()
                    end)
                    return from_created, from_wrapped
                "#[..],
            ),
            ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&function)?;
        vm.set_thread_call_context(&thread, context_in_group(7))?;

        assert_eq!(
            thread.resume(&vm, &[])?,
            ThreadStatus::Completed(vec![Value::Number(7.0), Value::Number(7.0)])
        );
        vm.clear_thread_call_context(&thread)?;
        let values = vm.eval(
            Arc::<[u8]>::from(&b"local _, group = coroutine.resume(later); return group"[..]),
            ChunkOrigin::default(),
        )?;
        assert_eq!(
            values,
            vec![Value::Number(7.0)],
            "a coroutine keeps the context it was created with"
        );
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn freed_threads_drop_their_call_context() -> Result<()> {
        let vm = Vm::new()?;
        vm.open_standard_libraries(StandardLibraries {
            coroutine: true,
            ..StandardLibraries::none()
        })?;
        let function = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(&b"for _ = 1, 10 do coroutine.create(function() end) end"[..]),
            ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&function)?;
        vm.set_thread_call_context(&thread, context_in_group(3))?;
        thread.resume(&vm, &[])?;
        vm.clear_thread_call_context(&thread)?;
        drop(thread);
        drop(function);
        unsafe {
            sys::lua_gc(vm.inner.state.as_ptr(), sys::LUA_GCCOLLECT, 0);
        }

        let contexts = vm.inner.control().thread_contexts.read().expect("contexts");
        assert!(contexts.is_empty(), "{} contexts left", contexts.len());
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn calls_with_a_context_restore_the_previous_one() -> Result<()> {
        let vm = Vm::new()?;
        vm.open_standard_libraries(StandardLibraries {
            coroutine: true,
            ..StandardLibraries::none()
        })?;
        task_group_probe(&vm)?;
        let nested = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(&b"return probe()"[..]),
            ChunkOrigin::default(),
        ))?;
        vm.data().insert(nested)?;
        let inner: NativeFn = Arc::new(|mut frame| {
            let nested = frame.vm.data().get::<Function>()?;
            let values = nested.call_with_context(frame.vm, &[], context_in_group(5))?;
            for value in values {
                frame.returns.write(value)?;
            }
            Ok(())
        });
        let inner = vm.create_function(ChunkOrigin::default(), inner)?;
        vm.set_global_function("inner", &inner)?;

        let values = vm.eval_with_context(
            Arc::<[u8]>::from(
                &b"local nested = inner(); local wrapped = coroutine.wrap(probe)(); return probe(), nested, wrapped"[..],
            ),
            ChunkOrigin::default(),
            context_in_group(4),
        )?;
        assert_eq!(
            values,
            vec![Value::Number(4.0), Value::Number(5.0), Value::Number(4.0)]
        );
        assert_eq!(
            vm.eval(
                Arc::<[u8]>::from(&b"return probe()"[..]),
                ChunkOrigin::default()
            )?,
            vec![Value::Number(0.0)]
        );
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn native_function_receives_owned_args_context_and_returns_values() -> Result<()> {
        let vm = Vm::new()?;
        let callback: NativeFn = Arc::new(|mut frame| {
            assert_eq!(frame.vm.id(), frame.thread.vm_id());
            assert_eq!(
                frame.context.origin.module.as_ref().map(|id| id.0.as_ref()),
                Some("test/native")
            );
            assert_eq!(
                frame.context.capability.as_ref().map(|id| id.0.as_ref()),
                Some("test.native")
            );
            assert_eq!(frame.context.task_group, TaskGroupId(9));

            let lhs: f64 = frame.args.read()?;
            let rhs: f64 = frame.args.read()?;
            frame.returns.write(lhs + rhs)?;
            Ok(())
        });
        let function = vm.create_function_with_options(
            NativeFunctionOptions::new(ChunkOrigin {
                module: Some(ModuleId(Arc::from("test/native"))),
                ..ChunkOrigin::default()
            })
            .capability(CapabilityId(Arc::from("test.native")))
            .task_group(TaskGroupId(9))
            .argument_names([Arc::from("lhs"), Arc::from("rhs")]),
            callback,
        )?;

        assert_eq!(
            function.call(&vm, &[Value::Number(20.0), Value::Number(22.0)])?,
            vec![Value::Number(42.0)]
        );
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn owned_table_supports_arbitrary_raw_keys() -> Result<()> {
        let vm = Vm::new()?;
        let mut table = OwnedTable::with_entry_capacity(0, 0, 2);
        table.set_key(Value::Number(42.0), Value::String(b"answer".to_vec()));
        table.set_key(
            Value::String(b"named".to_vec()),
            Value::String(b"value".to_vec()),
        );

        let function = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(&b"local table = ...; return table[42], table.named"[..]),
            ChunkOrigin::default(),
        ))?;
        assert_eq!(
            function.call(&vm, &[Value::TableData(table)])?,
            vec![
                Value::String(b"answer".to_vec()),
                Value::String(b"value".to_vec())
            ]
        );
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn native_function_errors_are_luau_runtime_errors_with_argument_names() -> Result<()> {
        let vm = Vm::new()?;
        let callback: NativeFn = Arc::new(|mut frame| {
            let _: f64 = frame.args.read()?;
            Ok(())
        });
        let function = vm.create_function_with_options(
            NativeFunctionOptions::new(ChunkOrigin {
                module: Some(ModuleId(Arc::from("test/native"))),
                plugin: Some(Arc::from("demo")),
                ..ChunkOrigin::default()
            })
            .function_name("scale")
            .argument_names([Arc::from("amount")]),
            callback,
        )?;

        let error = function
            .call(&vm, &[Value::String(b"bad".to_vec())])
            .expect_err("native callback argument errors should cross pcall");

        assert!(matches!(
            error,
            Error::Runtime(message)
                if message.contains("test/native")
                    && message.contains("scale")
                    && message.contains("demo")
                    && message.contains("amount")
                    && message.contains("expected number")
                    && message.contains("got string")
        ));
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn string_args_require_utf8_and_byte_string_preserves_raw_bytes() -> Result<()> {
        let vm = Vm::new()?;
        let utf8_callback: NativeFn = Arc::new(|mut frame| {
            let _: String = frame.args.read_named("text")?;
            Ok(())
        });
        let bytes_callback: NativeFn = Arc::new(|mut frame| {
            let bytes: ByteString = frame.args.read_named("data")?;
            frame.returns.write(bytes)?;
            Ok(())
        });

        let utf8 = vm.create_function_with_options(
            NativeFunctionOptions::new(ChunkOrigin::default())
                .function_name("utf8")
                .argument_names([Arc::from("text")]),
            utf8_callback,
        )?;
        let bytes = vm.create_function_with_options(
            NativeFunctionOptions::new(ChunkOrigin::default())
                .function_name("bytes")
                .argument_names([Arc::from("data")]),
            bytes_callback,
        )?;

        assert!(matches!(
            utf8.call(&vm, &[Value::String(vec![0xff])]),
            Err(Error::Runtime(message))
                if message.contains("text") && message.contains("valid UTF-8")
        ));
        assert_eq!(
            bytes.call(&vm, &[Value::String(vec![0xff, b'a'])])?,
            vec![Value::String(vec![0xff, b'a'])]
        );
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn native_callbacks_can_borrow_string_and_buffer_args() -> Result<()> {
        let vm = Vm::new()?;
        let callback: NativeFn = Arc::new(|mut frame| {
            let text_len = frame.args.read_named_str("text")?.len() as i64;
            let data_len = frame.args.read_named_buffer_bytes("data")?.len() as i64;
            frame.returns.write(text_len)?;
            frame.returns.write(data_len)?;
            Ok(())
        });
        let function = vm.create_function_with_options(
            NativeFunctionOptions::new(ChunkOrigin::default())
                .function_name("borrowed")
                .argument_names([Arc::from("text"), Arc::from("data")]),
            callback,
        )?;

        assert_eq!(
            function.call(
                &vm,
                &[
                    Value::String(b"hello".to_vec()),
                    Value::Buffer(vec![1, 2, 3, 4])
                ],
            )?,
            vec![Value::Number(5.0), Value::Number(4.0)]
        );
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn native_callbacks_reject_fractional_integer_args() -> Result<()> {
        let vm = Vm::new()?;
        let callback: NativeFn = Arc::new(|mut frame| {
            let _: i64 = frame.args.read_named("track_id")?;
            Ok(())
        });
        let function = vm.create_function_with_options(
            NativeFunctionOptions::new(ChunkOrigin::default())
                .function_name("integer_arg")
                .argument_names([Arc::from("track_id")]),
            callback,
        )?;
        let error = function
            .call(&vm, &[Value::Number(1.5)])
            .expect_err("fractional Luau number must not read as integer");

        assert!(error.to_string().contains("track_id"));
        assert!(error.to_string().contains("expected integer"));
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    struct DropCounter(Arc<AtomicUsize>);

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn set_memory_limit(vm: &Vm, bytes: usize) {
        vm.inner
            .control()
            .memory_limit
            .store(bytes, Ordering::Relaxed);
    }

    fn limited_vm(bytes: usize) -> Result<Vm> {
        let vm = Vm::with_options(VmOptions::default().memory_limit(bytes))?;
        vm.open_standard_libraries(StandardLibraries {
            base: true,
            table: true,
            ..StandardLibraries::none()
        })?;
        Ok(vm)
    }

    fn set_gc_running(vm: &Vm, running: bool) {
        let what = if running {
            sys::LUA_GCRESTART
        } else {
            sys::LUA_GCSTOP
        };
        unsafe { sys::lua_gc(vm.inner.state.as_ptr(), what, 0) };
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn value_creation_reports_out_of_memory_and_vm_recovers() -> Result<()> {
        type Attempt<'a> = Box<dyn Fn(&Vm, usize) -> Result<()> + 'a>;

        let vm = limited_vm(1 << 20)?;
        let table = vm.create_table()?;
        let function = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(&b"return ..."[..]),
            ChunkOrigin::default(),
        ))?;
        let allocate = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(&b"return table.create(...)"[..]),
            ChunkOrigin::default(),
        ))?;
        let dropped = Arc::new(AtomicUsize::new(0));
        let attempts: Vec<(&str, Attempt<'_>)> = vec![
            (
                "create_table",
                Box::new(|vm, _| vm.create_table().map(drop)),
            ),
            (
                "create_table_with_capacity",
                Box::new(|vm, _| vm.create_table_with_capacity(8, 8).map(drop)),
            ),
            (
                "create_buffer",
                Box::new(|vm, _| vm.create_buffer(b"bytes").map(drop)),
            ),
            (
                "create_userdata",
                Box::new(|vm, _| {
                    vm.create_userdata_auto(DropCounter(dropped.clone()))
                        .map(drop)
                }),
            ),
            (
                "create_function",
                Box::new(|vm, _| {
                    vm.create_function(ChunkOrigin::default(), Arc::new(|_| Ok(())))
                        .map(drop)
                }),
            ),
            (
                "create_thread",
                Box::new(|vm, _| vm.create_thread(&function).map(drop)),
            ),
            (
                "load_chunk",
                Box::new(|vm, i| {
                    vm.load_chunk(&Chunk::new(
                        format!("return {i}").into_bytes(),
                        ChunkOrigin::default(),
                    ))
                    .map(drop)
                }),
            ),
            (
                "eval",
                Box::new(|vm, i| {
                    vm.eval(
                        format!("return table.create({})", 1 + i % 64).into_bytes(),
                        ChunkOrigin::default(),
                    )
                    .map(drop)
                }),
            ),
            (
                "Function::call allocating in Luau",
                Box::new(|vm, _| allocate.call(vm, &[Value::Number(64.0)]).map(drop)),
            ),
            (
                "set_global_table",
                Box::new(|vm, i| vm.set_global_table(&format!("global{i}"), &table)),
            ),
            (
                "Table::set_raw",
                Box::new(|vm, i| table.set_raw(vm, &format!("key{i}"), Value::Boolean(true))),
            ),
            (
                "Table::set_integer_raw",
                Box::new(|vm, i| table.set_integer_raw(vm, i as i32 + 1, Value::Boolean(true))),
            ),
            (
                "Table::set_key_raw",
                Box::new(|vm, i| {
                    table.set_key_raw(vm, Value::Number(i as f64 + 0.5), Value::Integer(1))
                }),
            ),
            (
                "Function::call with a string",
                Box::new(|vm, i| {
                    function
                        .call(vm, &[Value::String(format!("argument{i}").into_bytes())])
                        .map(drop)
                }),
            ),
            (
                "Function::call with table data",
                Box::new(|vm, i| {
                    let mut data = OwnedTable::new();
                    data.set_field(format!("field{i}"), Value::Integer(1));
                    function.call(vm, &[Value::TableData(data)]).map(drop)
                }),
            ),
        ];

        vm.eval(
            Arc::<[u8]>::from(
                &b"hold = {}
                local size = 4096
                while size > 0 do
                    local ok, value = pcall(table.create, size)
                    if not (ok and pcall(table.insert, hold, value)) then
                        size //= 2
                    end
                end"[..],
            ),
            ChunkOrigin::default(),
        )?;
        // Nothing is freed while the attempts run, so each repeats until memory runs out.
        set_gc_running(&vm, false);
        for (name, attempt) in &attempts {
            let error = (0..1_000_000)
                .find_map(|i| attempt(&vm, i).err())
                .unwrap_or_else(|| panic!("{name} never ran out of memory"));
            assert!(
                matches!(error, Error::OutOfMemory),
                "{name} failed with {error}"
            );
            assert_eq!(stack_top(&vm), 0, "{name} left values on the stack");
        }
        assert_eq!(dropped.load(Ordering::SeqCst), 1);

        set_memory_limit(&vm, 0);
        set_gc_running(&vm, true);
        for (name, attempt) in &attempts {
            attempt(&vm, 1_000_000)
                .unwrap_or_else(|error| panic!("{name} failed after recovery: {error}"));
        }
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn callbacks_out_of_memory_raise_catchable_errors_after_dropping_rust_values() -> Result<()> {
        let vm = limited_vm(1 << 20)?;
        let dropped = Arc::new(AtomicUsize::new(0));

        let held = dropped.clone();
        let allocate = vm.create_function(
            ChunkOrigin::default(),
            Arc::new(move |frame| {
                let _held = DropCounter(held.clone());
                frame.vm.create_buffer(&vec![0; 2 << 20])?;
                Ok(())
            }),
        )?;
        let returned = dropped.clone();
        let oversized = vm.create_function(
            ChunkOrigin::default(),
            Arc::new(move |mut frame| {
                let held = DropCounter(returned.clone());
                frame.returns.write(Value::Buffer(vec![0; 2 << 20]))?;
                frame.returns.write(NativeFunctionValue::new(
                    NativeFunctionOptions::new(ChunkOrigin::default()),
                    Arc::new(move |_| {
                        let _ = &held;
                        Ok(())
                    }),
                ))?;
                Ok(())
            }),
        )?;
        let probe = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(
                &b"local allocate, oversized = ...
                local allocated, allocate_error = pcall(allocate)
                local returned, return_error = pcall(oversized)
                return allocated, allocate_error, returned, return_error"[..],
            ),
            ChunkOrigin::default(),
        ))?;

        let values = probe.call(
            &vm,
            &[Value::Function(allocate), Value::Function(oversized)],
        )?;
        drop(probe);
        let [
            Value::Boolean(false),
            Value::String(allocate_error),
            Value::Boolean(false),
            Value::String(return_error),
        ] = values.as_slice()
        else {
            panic!("expected both callbacks to fail, got {values:?}");
        };
        assert_eq!(allocate_error.as_slice(), gateway::MEMORY_ERROR.as_bytes());
        assert_eq!(return_error.as_slice(), gateway::MEMORY_ERROR.as_bytes());
        assert_eq!(dropped.load(Ordering::SeqCst), 2);
        assert_eq!(Rc::strong_count(&vm.inner), 1);
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn callback_errors_reach_luau_after_dropping_rust_values() -> Result<()> {
        let vm = limited_vm(16 << 20)?;
        let dropped = Arc::new(AtomicUsize::new(0));

        let held = dropped.clone();
        let failing = vm.create_function(
            ChunkOrigin::default(),
            Arc::new(move |mut frame| {
                let pending = DropCounter(held.clone());
                frame.returns.write(NativeFunctionValue::new(
                    NativeFunctionOptions::new(ChunkOrigin::default()),
                    Arc::new(move |_| {
                        let _ = &pending;
                        Ok(())
                    }),
                ))?;
                Err(Error::Runtime("callback failed".to_string()))
            }),
        )?;
        let probe = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(&b"return pcall(...)"[..]),
            ChunkOrigin::default(),
        ))?;

        let values = probe.call(&vm, &[Value::Function(failing)])?;
        drop(probe);
        let [Value::Boolean(false), Value::String(message)] = values.as_slice() else {
            panic!("expected the callback to fail, got {values:?}");
        };
        assert!(String::from_utf8_lossy(message).contains("callback failed"));
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(Rc::strong_count(&vm.inner), 1);
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn callback_out_of_memory_reaches_outer_callers_as_out_of_memory() -> Result<()> {
        let vm = limited_vm(1 << 20)?;
        let allocate = vm.create_function(
            ChunkOrigin::default(),
            Arc::new(|frame| {
                frame.vm.create_buffer(&vec![0; 2 << 20])?;
                Ok(())
            }),
        )?;
        let through_luau = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(&b"local allocate = ... return allocate()"[..]),
            ChunkOrigin::default(),
        ))?;

        let direct = allocate.call(&vm, &[]);
        assert!(matches!(direct, Err(Error::OutOfMemory)), "{direct:?}");
        let called = through_luau.call(&vm, &[Value::Function(allocate.clone())]);
        assert!(matches!(called, Err(Error::OutOfMemory)), "{called:?}");

        let thread = vm.create_thread(&through_luau)?;
        let resumed = thread.resume(&vm, &[Value::Function(allocate)]);
        assert!(matches!(resumed, Err(Error::OutOfMemory)), "{resumed:?}");
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    const PANICKING_DESTRUCTOR_CHILD: &str = "HARMONY_LUAU_PANICKING_DESTRUCTOR_CHILD";

    /// Runs itself again in a child process, where the destructor's panic must abort the process
    /// rather than unwind through `lua_close`.
    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn panicking_destructor_aborts_instead_of_unwinding_into_luau() -> Result<()> {
        struct PanicsOnDrop;

        impl Drop for PanicsOnDrop {
            fn drop(&mut self) {
                panic!("userdata destructor panicked");
            }
        }

        if std::env::var_os(PANICKING_DESTRUCTOR_CHILD).is_some() {
            let vm = Vm::new()?;
            vm.create_userdata_auto(PanicsOnDrop)?;
            drop(vm);
            return Ok(());
        }

        let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                "runtime::tests::panicking_destructor_aborts_instead_of_unwinding_into_luau",
                "--nocapture",
            ])
            .env(PANICKING_DESTRUCTOR_CHILD, "1")
            .output()
            .expect("test binary runs");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("userdata destructor panicked"),
            "child stderr: {stderr}"
        );
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(
                output.status.signal(),
                Some(6),
                "child exited with {}",
                output.status
            );
        }
        #[cfg(not(unix))]
        assert!(!output.status.success());
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn failed_resume_arguments_leave_an_unstarted_thread_resumable() -> Result<()> {
        let vm = limited_vm(1 << 20)?;
        let function = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(&b"local count, data = ... return count, type(data)"[..]),
            ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&function)?;

        let failed = thread.resume(&vm, &[Value::Number(1.0), Value::Buffer(vec![0; 2 << 20])]);
        assert!(matches!(failed, Err(Error::OutOfMemory)), "{failed:?}");
        assert!(!thread.is_started());

        set_memory_limit(&vm, 0);
        assert_eq!(
            thread.resume(&vm, &[Value::Number(2.0), Value::Buffer(vec![0; 2 << 20])])?,
            ThreadStatus::Completed(vec![Value::Number(2.0), Value::String(b"buffer".to_vec())])
        );
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }
}
