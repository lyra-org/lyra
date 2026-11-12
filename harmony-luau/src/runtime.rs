use std::{
    alloc::Layout,
    any::{
        Any,
        TypeId,
        type_name,
    },
    cell::RefCell,
    collections::HashMap,
    ffi::{
        CString,
        c_int,
        c_void,
    },
    fmt,
    future::Future,
    marker::PhantomData,
    ops::ControlFlow,
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
            AtomicBool,
            AtomicPtr,
            AtomicU8,
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
mod stack;

pub use stack::UserDataRef;
use stack::{
    RegistryRef,
    Stack,
    StackIndex,
    ThreadRef,
};

pub type Result<T> = std::result::Result<T, Error>;

static NEXT_VM_ID: AtomicU64 = AtomicU64::new(1);
static LUAU_FLAGS: Once = Once::new();

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to create Luau VM")]
    VmCreate,
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
    #[error(
        "argument `{name}` expected one of {}, got {value:?}",
        quoted_list(expected)
    )]
    UnknownEnumValue {
        name: Arc<str>,
        expected: &'static [&'static str],
        value: String,
    },
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
    #[error("no Luau userdata tags are available")]
    UserDataTagExhausted,
    #[error("expected userdata of type {expected}")]
    UserDataTypeMismatch { expected: &'static str },
    #[error("Luau serialization failed: {0}")]
    Serialize(String),
    /// A host error of a fixed kind, which Luau code can branch on rather than match its message.
    /// Raised into Luau, it is a `HostError` value whose `tostring` is the message.
    #[error("{message}")]
    Kinded { kind: &'static str, message: String },
}

/// An error as it is raised into Luau: a `HostError` value when it has a kind, else its message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaisedError {
    pub kind: Option<&'static str>,
    pub message: String,
}

impl RaisedError {
    fn of(error: &Error, message: String) -> Self {
        let kind = match error {
            Error::Kinded { kind, .. } => Some(*kind),
            _ => None,
        };
        Self { kind, message }
    }
}

impl From<&Error> for RaisedError {
    fn from(error: &Error) -> Self {
        Self::of(error, error.to_string())
    }
}

impl From<&str> for RaisedError {
    fn from(message: &str) -> Self {
        Self {
            kind: None,
            message: message.to_string(),
        }
    }
}

impl Error {
    /// Whether this is the error the interrupt raises, as it left the thread it was raised in:
    /// [`INTERRUPTED_ERROR`] itself, and not an error of the thread's own that came after it
    /// caught that one.
    pub fn is_interruption(&self) -> bool {
        matches!(self, Self::Runtime(message) if message == INTERRUPTED_ERROR)
    }
}

impl From<String> for RaisedError {
    fn from(message: String) -> Self {
        Self {
            kind: None,
            message,
        }
    }
}

/// A host error raised into Luau as a runtime error with its message. Running out of memory stays
/// [`Error::OutOfMemory`], even when the error carries context.
#[cfg(feature = "anyhow")]
impl From<anyhow::Error> for Error {
    fn from(error: anyhow::Error) -> Self {
        match error.downcast_ref::<Error>() {
            Some(Error::OutOfMemory) => Error::OutOfMemory,
            Some(Error::Kinded { kind, .. }) => Error::Kinded {
                kind,
                message: error.to_string(),
            },
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

/// The Luau tag of the userdata holding one Rust type, which also picks the destructor Luau runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct UserDataTag(i32);

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
    userdata_tags: RefCell<stack::UserDataTags>,
    stacks: stack::LiveStacks,
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
            let callbacks = sys::lua_callbacks(state.as_ptr());
            (*callbacks).userthread = Some(userthread);
            (*callbacks).postresume = Some(postresume);
        }
        let vm = Self {
            inner: Rc::new(VmInner {
                id: NEXT_VM_ID.fetch_add(1, Ordering::Relaxed),
                state,
                control,
                data: VmData::default(),
                userdata_tags: RefCell::default(),
                stacks: stack::LiveStacks::default(),
                started_at: Instant::now(),
            }),
        };
        vm.install_host_errors()?;
        Ok(vm)
    }

    /// The kind of a `HostError` value; `None` for any other value.
    pub fn error_kind(&self, value: &Value) -> Option<&'static str> {
        self.host_error(value).map(|error| error.kind)
    }

    /// The message of a `HostError` value, as `tostring` gives it; `None` for any other value.
    pub fn error_message(&self, value: &Value) -> Option<String> {
        self.host_error(value).map(|error| error.message.clone())
    }

    fn host_error<'a>(&self, value: &'a Value) -> Option<UserDataRef<'a, HostError>> {
        match value {
            Value::UserData(userdata)
                if userdata.tag == HOST_ERROR_TAG && userdata.vm_id() == self.inner.id =>
            {
                userdata.borrow().ok()
            }
            _ => None,
        }
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
        self.interrupt_after_or_on(budget, None)
    }

    /// Interrupts the running thread once `budget` runs out or, sooner, once `signal` is raised,
    /// even from another OS thread.
    pub fn interrupt_after_or_on(
        &self,
        budget: Duration,
        signal: Option<&InterruptSignal>,
    ) -> InterruptBudgetGuard {
        let deadline_millis = current_unix_millis()
            .saturating_add(budget.as_millis().min(u128::from(u64::MAX)) as u64);
        let token = {
            let mut budgets = self.interrupt_budgets();
            budgets.next_token += 1;
            let token = budgets.next_token;
            budgets.active.push(InterruptBudget {
                token,
                deadline_millis,
                signal: signal.cloned(),
                interrupted: Arc::default(),
            });
            self.apply_innermost_budget(&budgets);
            token
        };
        InterruptBudgetGuard {
            vm: self.clone(),
            token,
        }
    }

    fn interrupt_budgets(&self) -> std::sync::MutexGuard<'_, InterruptBudgets> {
        self.inner
            .control()
            .interrupt_budgets
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    /// Makes the innermost live budget the one the interrupt checks. Its signal stays owned by
    /// `budgets` for as long as the interrupt can read it.
    fn apply_innermost_budget(&self, budgets: &InterruptBudgets) {
        let control = self.inner.control();
        let innermost = budgets.active.last();
        let signal = innermost
            .and_then(|budget| budget.signal.as_ref())
            .map_or(std::ptr::null_mut(), |signal| {
                Arc::as_ptr(&signal.0).cast_mut()
            });
        control.interrupt_signal.store(signal, Ordering::Release);
        let interrupted = innermost.map_or(std::ptr::null_mut(), |budget| {
            Arc::as_ptr(&budget.interrupted).cast_mut()
        });
        control
            .interrupt_interrupted
            .store(interrupted, Ordering::Release);
        self.set_interrupt_deadline_millis(innermost.map_or(0, |budget| budget.deadline_millis));
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
        let mut stack = Stack::main(self);
        for (_, name) in opened.into_iter().filter(|(open, _)| *open) {
            stack.open_library(name)?;
        }
        if libraries.base {
            // Environment access would reach any function's globals,
            // including the `require` a host gives a chunk.
            for name in [b"getfenv", b"setfenv"] {
                stack.push_nil()?;
                stack.set_global(name)?;
            }
        }
        if libraries.os {
            self.install_safe_os()?;
        }
        Ok(())
    }

    pub fn sandbox(&self) -> Result<()> {
        Stack::main(self).sandbox_globals()
    }

    pub fn load_chunk(&self, chunk: &Chunk) -> Result<Function> {
        let bytecode = compile(&chunk.source.0, chunk.compile)?;
        let mut stack = Stack::main(self);
        stack.load(&chunk.chunk_name(), bytecode.as_bytes())?;
        Ok(Function {
            reference: stack.reference(StackIndex::TOP)?,
            origin: chunk.origin.clone(),
        })
    }

    /// A table for [`Vm::load_chunk_with_globals`] that reads through to the
    /// shared globals and keeps the writes of the chunks loaded with it.
    pub fn create_global_scope(&self) -> Result<Table> {
        let mut stack = Stack::main(self);
        stack.create_table(0, 0)?;
        stack.create_table(0, 1)?;
        stack.push_globals()?;
        stack.raw_set_field(StackIndex::relative(-2), b"__index")?;
        stack.set_readonly(StackIndex::TOP, true)?;
        stack.set_metatable(StackIndex::relative(-2))?;
        Ok(Table {
            reference: stack.reference(StackIndex::TOP)?,
        })
    }

    /// Loads `chunk` with an environment of its own holding `globals`. Every
    /// other global is read from and written to `scope`, so chunks sharing a
    /// scope share their globals. Functions the chunk creates share its
    /// environment.
    pub fn load_chunk_with_globals(
        &self,
        chunk: &Chunk,
        globals: &[(&str, &Value)],
        scope: &Table,
    ) -> Result<Function> {
        let bytecode = compile(&chunk.source.0, chunk.compile)?;
        let mut stack = Stack::main(self);
        stack.load_with_globals(
            &chunk.chunk_name(),
            bytecode.as_bytes(),
            globals,
            &scope.reference,
        )?;
        Ok(Function {
            reference: stack.reference(StackIndex::TOP)?,
            origin: chunk.origin.clone(),
        })
    }

    pub fn eval(&self, source: impl Into<Arc<[u8]>>, origin: ChunkOrigin) -> Result<Vec<Value>> {
        let function = self.load_chunk(&Chunk::new(source, origin))?;
        function.call(&[])
    }

    pub fn create_table(&self) -> Result<Table> {
        self.create_table_with_capacity(0, 0)
    }

    pub fn create_table_with_capacity(&self, array: i32, records: i32) -> Result<Table> {
        let mut stack = Stack::main(self);
        stack.create_table(array, records)?;
        Ok(Table {
            reference: stack.reference(StackIndex::TOP)?,
        })
    }

    pub fn create_buffer(&self, bytes: &[u8]) -> Result<Buffer> {
        let mut stack = Stack::main(self);
        stack.new_buffer(bytes)?;
        Ok(Buffer {
            reference: stack.reference(StackIndex::TOP)?,
        })
    }

    /// A userdata holding `value`, which only [`UserData::borrow`] as a `T` reads.
    pub fn create_userdata<T>(&self, value: T) -> Result<UserData>
    where
        T: 'static,
    {
        let mut stack = Stack::main(self);
        let tag = stack.new_userdata(value)?;
        Ok(UserData {
            reference: stack.reference(StackIndex::TOP)?,
            tag,
        })
    }

    pub fn create_function(&self, origin: ChunkOrigin, callback: NativeFn) -> Result<Function> {
        self.create_function_with_options(NativeFunctionOptions::new(origin), callback)
    }

    pub fn create_function_with_options(
        &self,
        options: NativeFunctionOptions,
        callback: NativeFn,
    ) -> Result<Function> {
        let mut stack = Stack::main(self);
        stack.push_native_function(&options, callback)?;
        Ok(Function {
            reference: stack.reference(StackIndex::TOP)?,
            origin: options.origin,
        })
    }

    pub fn create_thread(&self, function: &Function) -> Result<Thread> {
        let thread = Stack::main(self).new_thread(&function.reference)?;
        Ok(Thread::new(thread, function.origin.clone()))
    }

    /// Runs `source` as [`Vm::eval`] does, as a call made in `context`.
    pub fn eval_with_context(
        &self,
        source: impl Into<Arc<[u8]>>,
        origin: ChunkOrigin,
        context: CallContext,
    ) -> Result<Vec<Value>> {
        let function = self.load_chunk(&Chunk::new(source, origin))?;
        function.call_with_context(&[], context)
    }

    /// Replaces the call context of the thread whose state is `state_id`.
    fn replace_state_context(
        &self,
        state_id: usize,
        context: Option<CallContext>,
    ) -> Result<Option<CallContext>> {
        let mut contexts = self
            .inner
            .control()
            .thread_contexts
            .write()
            .map_err(|_| Error::VmDataPoisoned)?;
        Ok(match context {
            Some(context) => contexts.insert(state_id, context),
            None => contexts.remove(&state_id),
        })
    }

    fn call_context_for_state(&self, state_id: usize) -> Result<Option<CallContext>> {
        self.inner
            .control()
            .thread_contexts
            .read()
            .map_err(|_| Error::VmDataPoisoned)
            .map(|contexts| contexts.get(&state_id).cloned())
    }

    pub fn set_global_table(&self, name: &str, table: &Table) -> Result<()> {
        self.set_global(name, &table.reference)
    }

    pub fn set_global_function(&self, name: &str, function: &Function) -> Result<()> {
        self.set_global(name, &function.reference)
    }

    fn set_global(&self, name: &str, value: &RegistryRef) -> Result<()> {
        let mut stack = Stack::main(self);
        stack.push_ref(value)?;
        stack.set_global(name.as_bytes())
    }

    fn set_interrupt_deadline_millis(&self, deadline: u64) {
        self.inner
            .control()
            .interrupt_deadline_millis
            .store(deadline, Ordering::Release);
        unsafe {
            sys::lua_set_interrupt_callback(
                self.inner.state.as_ptr(),
                if deadline == 0 { None } else { Some(interrupt) },
            );
        }
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
        table.set("time", &time)?;

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
        table.set("clock", &clock)?;

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
        table.set("date", &date)?;

        self.set_global_table("os", &table)?;
        Ok(())
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
    /// The innermost live budget's deadline and signal, as the interrupt reads them.
    interrupt_deadline_millis: AtomicU64,
    interrupt_signal: AtomicPtr<AtomicBool>,
    /// Where the interrupt notes that it raised on that budget, and for which reason.
    interrupt_interrupted: AtomicPtr<AtomicU8>,
    /// Every live [`InterruptBudgetGuard`]'s budget, which owns the signal the interrupt reads.
    interrupt_budgets: std::sync::Mutex<InterruptBudgets>,
    /// Keyed by `lua_State` address; [`userthread`] keeps it in step with
    /// the threads Luau creates and frees.
    thread_contexts: RwLock<HashMap<usize, CallContext>>,
}

impl VmControl {
    /// Runs `close` outside the memory limit, which shrinking a closed thread's stack could
    /// otherwise hit partway.
    fn without_memory_limit<T>(&self, close: impl FnOnce() -> T) -> T {
        let limit = self.memory_limit.swap(0, Ordering::Relaxed);
        let closed = close();
        self.memory_limit.store(limit, Ordering::Relaxed);
        closed
    }

    fn new(options: VmOptions) -> Self {
        Self {
            allocated: AtomicUsize::new(0),
            memory_limit: AtomicUsize::new(options.memory_limit.unwrap_or(0)),
            interrupt_deadline_millis: AtomicU64::new(0),
            interrupt_signal: AtomicPtr::new(std::ptr::null_mut()),
            interrupt_interrupted: AtomicPtr::new(std::ptr::null_mut()),
            interrupt_budgets: std::sync::Mutex::default(),
            thread_contexts: RwLock::new(HashMap::new()),
        }
    }
}

#[derive(Default)]
struct InterruptBudgets {
    next_token: u64,
    /// In the order they were set; the last is the innermost.
    active: Vec<InterruptBudget>,
}

struct InterruptBudget {
    token: u64,
    deadline_millis: u64,
    signal: Option<InterruptSignal>,
    /// An [`InterruptCause`] once the interrupt has raised on this budget, 0 before.
    interrupted: Arc<AtomicU8>,
}

/// Why the interrupt raised on a budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InterruptCause {
    /// The budget ran out.
    Time = 1,
    /// The budget's signal was raised.
    Signal = 2,
}

/// Holds an interrupt budget until dropped. Guards may drop in any order; the innermost live one
/// applies.
pub struct InterruptBudgetGuard {
    vm: Vm,
    token: u64,
}

impl InterruptBudgetGuard {
    /// Why the interrupt raised in what ran under this budget, if it did. A budget that ran out
    /// while a native function ran, which then failed on its own, interrupted nothing.
    pub fn interrupted(&self) -> Option<InterruptCause> {
        let budgets = self.vm.interrupt_budgets();
        let budget = budgets
            .active
            .iter()
            .find(|budget| budget.token == self.token)?;
        match budget.interrupted.load(Ordering::Acquire) {
            0 => None,
            cause if cause == InterruptCause::Signal as u8 => Some(InterruptCause::Signal),
            _ => Some(InterruptCause::Time),
        }
    }
}

impl Drop for InterruptBudgetGuard {
    fn drop(&mut self) {
        let mut budgets = self.vm.interrupt_budgets();
        let Some(index) = budgets
            .active
            .iter()
            .position(|budget| budget.token == self.token)
        else {
            return;
        };
        let removed = budgets.active.remove(index);
        self.vm.apply_innermost_budget(&budgets);
        drop(budgets);
        drop(removed);
    }
}

/// A flag that interrupts the thread a [`Vm::interrupt_after_or_on`] budget runs, once raised.
#[derive(Clone, Debug, Default)]
pub struct InterruptSignal(Arc<AtomicBool>);

impl InterruptSignal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn raise(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn is_raised(&self) -> bool {
        self.0.load(Ordering::Acquire)
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

#[derive(Clone, Debug, PartialEq)]
pub struct Function {
    reference: RegistryRef,
    pub origin: ChunkOrigin,
}

impl Function {
    pub fn vm_id(&self) -> u64 {
        self.reference.vm().id()
    }

    pub fn call(&self, args: &[Value]) -> Result<Vec<Value>> {
        let mut stack = Stack::main(self.reference.vm());
        stack.push_ref(&self.reference)?;
        for arg in args {
            stack.push_value(arg)?;
        }
        stack.pcall(args.len() as c_int)?;
        (stack.base() + 1..=stack.top())
            .map(|index| stack.read_value(StackIndex::absolute(index)))
            .collect()
    }

    /// Calls the function as [`Function::call`] does, with `context` as the
    /// call context of the native functions it reaches and the coroutines it
    /// creates.
    pub fn call_with_context(&self, args: &[Value], context: CallContext) -> Result<Vec<Value>> {
        let vm = self.reference.vm();
        let previous = vm.replace_state_context(vm.state_id(), Some(context))?;
        let result = self.call(args);
        vm.replace_state_context(vm.state_id(), previous)?;
        result
    }
}

/// How [`Thread::cancel`] left a thread.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThreadEnd {
    /// Dead.
    Closed,
    /// Live still, and closed where it next yields.
    ClosedAtYield,
}

#[derive(Clone)]
pub struct Thread {
    inner: Rc<ThreadInner>,
}

struct ThreadInner {
    thread: ThreadRef,
    origin: ChunkOrigin,
    data: ThreadData,
}

impl Thread {
    /// Whether code running on this thread may yield here; it can't inside
    /// a metamethod, a `table.sort` comparator, or a host `pcall`.
    pub fn is_yieldable(&self) -> bool {
        Stack::of_thread(self).is_yieldable()
    }

    /// Whether [`Self::cancel`] found the thread live, so that it is closed where it next yields.
    pub fn is_cancelled(&self) -> bool {
        Stack::of_thread(self).is_cancelled()
    }

    fn new(thread: ThreadRef, origin: ChunkOrigin) -> Self {
        Self {
            inner: Rc::new(ThreadInner {
                thread,
                origin,
                data: ThreadData::default(),
            }),
        }
    }

    pub fn vm_id(&self) -> u64 {
        self.vm().id()
    }

    fn vm(&self) -> &Vm {
        self.inner.thread.vm()
    }

    pub fn state_id(&self) -> usize {
        self.inner.thread.state_id()
    }

    pub fn origin(&self) -> &ChunkOrigin {
        &self.inner.origin
    }

    pub fn data(&self) -> &ThreadData {
        &self.inner.data
    }

    /// Whether the thread has run, or was closed and never will.
    pub fn is_started(&self) -> bool {
        !Stack::of_thread(self).is_unstarted()
    }

    pub fn sandbox(&self) -> Result<()> {
        Stack::of_thread(self).sandbox_thread()
    }

    /// Makes `context` the call context native calls on the thread see, and returns the one it
    /// replaces.
    pub fn set_call_context(&self, context: CallContext) -> Result<Option<CallContext>> {
        self.vm()
            .replace_state_context(self.state_id(), Some(context))
    }

    pub fn clear_call_context(&self) -> Result<Option<CallContext>> {
        self.vm().replace_state_context(self.state_id(), None)
    }

    /// The call context native calls on the thread see.
    pub fn call_context(&self) -> Result<Option<CallContext>> {
        self.vm().call_context_for_state(self.state_id())
    }

    pub fn resume(&self, args: &[Value]) -> Result<ThreadStatus> {
        let mut stack = self.prepare_resume()?;
        for arg in args {
            // Failing leaves an unstarted thread with just its function, so it still reads as
            // unstarted.
            stack.push_value(arg)?;
        }
        let resumed = stack.resume(args.len() as c_int);
        self.finish_resume(stack, resumed)
    }

    /// Resumes the thread by raising `error` where it yielded, as if the call it yielded in had
    /// raised it: the nearest `pcall` in this thread catches it, and with none the thread fails.
    pub fn resume_error(&self, error: &RaisedError) -> Result<ThreadStatus> {
        let mut stack = self.prepare_resume()?;
        let resumed = stack.resume_error(error);
        self.finish_resume(stack, resumed)
    }

    /// Closes the thread as `coroutine.close` does: Luau reports it as `dead` and nothing resumes
    /// it again. One that hasn't started never runs, and one that has ended stays as it is. Fails,
    /// leaving the thread alone, for one that is running or has resumed another, and for the VM's
    /// main thread, which is no coroutine.
    ///
    /// Closing frees the thread's stack, which allocates a smaller one, outside the VM's memory
    /// limit. If the allocator itself fails there, the process aborts: the thread is left
    /// suspended with no frame, and resuming it would run garbage.
    pub fn close(&self) -> Result<()> {
        if self.try_close()? {
            return Ok(());
        }
        Err(Error::Runtime(
            "cannot close a coroutine that is running or has resumed another".to_string(),
        ))
    }

    /// [`Self::close`], or for a thread that is running or has resumed another, as soon as it
    /// can be: where it next yields, before whatever resumed it runs again. Until then it runs
    /// on, calls included. Nothing after that yield runs, and the resume returns no values, or
    /// [`ThreadStatus::Closed`] from [`Self::resume`]. One that returns or fails first ends as any
    /// other. The threads it has resumed are left as they are.
    pub fn cancel(&self) -> Result<ThreadEnd> {
        if self.try_close()? {
            return Ok(ThreadEnd::Closed);
        }
        Stack::of_thread(self).cancel();
        Ok(ThreadEnd::ClosedAtYield)
    }

    /// Closes a thread that isn't live, and returns whether it is dead.
    fn try_close(&self) -> Result<bool> {
        if self.state_id() == self.vm().state_id() {
            return Err(Error::Runtime("cannot close the main thread".to_string()));
        }
        Ok(Stack::of_thread(self).close())
    }

    /// Checks the thread can resume and clears what its last resume left on its stack. A thread
    /// that has not started holds just its function, which stays below what is pushed.
    fn prepare_resume(&self) -> Result<Stack<'_>> {
        let mut stack = Stack::of_thread(self);
        stack.check_resumable()?;
        if self.is_started() {
            stack.clear();
        }
        Ok(stack)
    }

    fn finish_resume(&self, mut stack: Stack<'_>, resumed: Result<bool>) -> Result<ThreadStatus> {
        let result = resumed.and_then(|yielded| {
            // Cancelled as it ran, and closed as it yielded.
            if yielded && stack.status() != sys::LUA_YIELD {
                return Ok(ThreadStatus::Closed);
            }
            let values = (1..=stack.top())
                .map(|index| stack.read_value(StackIndex::absolute(index)))
                .collect::<Result<Vec<_>>>()?;
            if yielded {
                Ok(ThreadStatus::Yielded(values))
            } else {
                Ok(ThreadStatus::Completed(values))
            }
        });
        stack.clear();
        result
    }
}

impl fmt::Debug for Thread {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Thread")
            .field("vm_id", &self.vm_id())
            .field("state_id", &self.state_id())
            .field("origin", &self.inner.origin)
            .finish()
    }
}

impl PartialEq for Thread {
    fn eq(&self, other: &Self) -> bool {
        self.vm_id() == other.vm_id() && self.state_id() == other.state_id()
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
    /// Cancelled while it ran, and closed where it yielded: see [`Thread::cancel`].
    Closed,
}

/// A table pinned in its VM's registry. Every access is raw: it bypasses
/// metamethods and works directly on the table's storage.
#[derive(Clone, Debug, PartialEq)]
pub struct Table {
    reference: RegistryRef,
}

impl Table {
    pub fn vm_id(&self) -> u64 {
        self.reference.vm().id()
    }

    /// A stack with the table on top.
    fn stack(&self) -> Result<Stack<'_>> {
        let mut stack = Stack::main(self.reference.vm());
        stack.push_ref(&self.reference)?;
        Ok(stack)
    }

    pub fn set(&self, key: &str, value: impl Into<Value>) -> Result<()> {
        let mut stack = self.stack()?;
        stack.push_value(&value.into())?;
        stack.raw_set_field(StackIndex::relative(-2), key.as_bytes())
    }

    pub fn set_integer(&self, key: i32, value: impl Into<Value>) -> Result<()> {
        let mut stack = self.stack()?;
        stack.push_value(&value.into())?;
        stack.raw_set_index(StackIndex::relative(-2), key)
    }

    pub fn set_key(&self, key: impl Into<Value>, value: impl Into<Value>) -> Result<()> {
        let mut stack = self.stack()?;
        stack.push_value(&key.into())?;
        stack.push_value(&value.into())?;
        stack.raw_set(StackIndex::relative(-3))
    }

    pub fn get(&self, key: &str) -> Result<Value> {
        let mut stack = self.stack()?;
        stack.raw_get_field(StackIndex::TOP, key.as_bytes())?;
        stack.read_value(StackIndex::TOP)
    }

    pub fn get_integer(&self, key: i32) -> Result<Value> {
        let mut stack = self.stack()?;
        stack.raw_get_index(StackIndex::TOP, key)?;
        stack.read_value(StackIndex::TOP)
    }

    /// The table's border, as the `#` operator finds it without `__len`.
    #[expect(
        clippy::len_without_is_empty,
        reason = "a zero border does not mean the table has no entries"
    )]
    pub fn len(&self) -> Result<usize> {
        Ok(self.stack()?.raw_len(StackIndex::TOP))
    }

    /// The values at `1, 2, ...` up to the first nil.
    pub fn array_values(&self) -> Result<Vec<Value>> {
        let mut values = Vec::new();
        self.for_each_array_value(|value| {
            values.push(value.into_value());
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(values)
    }

    /// Visits the values at `1, 2, ...` up to the first nil, or until `visit` breaks, reading each
    /// in place.
    pub fn for_each_array_value(
        &self,
        mut visit: impl FnMut(ValueRef<'_>) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        let mut stack = self.stack()?;
        let table = StackIndex::absolute(stack.top());
        for index in 1..=i32::MAX {
            stack.raw_get_index(table, index)?;
            let value = stack.read_value_ref(StackIndex::TOP)?;
            if matches!(value, ValueRef::Nil) {
                return Ok(());
            }
            let flow = visit(value)?;
            stack.pop(1);
            if flow.is_break() {
                return Ok(());
            }
        }

        Err(Error::Runtime(
            "table array part exceeded supported integer indexes".to_string(),
        ))
    }

    pub fn pairs(&self) -> Result<Vec<(Value, Value)>> {
        let mut pairs = Vec::new();
        self.for_each_pair(|key, value| {
            pairs.push((key.into_value(), value.into_value()));
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(pairs)
    }

    /// Visits each key and value as `next` finds them, or until `visit` breaks, reading each in
    /// place.
    pub fn for_each_pair(
        &self,
        mut visit: impl FnMut(ValueRef<'_>, ValueRef<'_>) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        let mut stack = self.stack()?;
        let table = StackIndex::absolute(stack.top());
        stack.push_nil()?;
        while stack.next(table)? {
            let key = stack.read_value_ref(StackIndex::relative(-2))?;
            let value = stack.read_value_ref(StackIndex::TOP)?;
            if visit(key, value)?.is_break() {
                return Ok(());
            }
            stack.pop(1);
        }
        Ok(())
    }

    /// What tells this table apart from every other live one: two handles to the same table
    /// share it.
    pub fn identity(&self) -> Result<usize> {
        Ok(self.stack()?.to_pointer(StackIndex::TOP))
    }

    pub fn set_metatable(&self, metatable: Option<&Table>) -> Result<()> {
        let mut stack = self.stack()?;
        push_metatable(&mut stack, metatable)?;
        stack.set_metatable(StackIndex::relative(-2))
    }

    pub fn metatable(&self) -> Result<Option<Table>> {
        let mut stack = self.stack()?;
        if !stack.get_metatable(StackIndex::TOP)? {
            return Ok(None);
        }
        Ok(Some(Table {
            reference: stack.reference(StackIndex::TOP)?,
        }))
    }

    pub fn set_readonly(&self, readonly: bool) -> Result<()> {
        self.stack()?.set_readonly(StackIndex::TOP, readonly)
    }
}

fn push_metatable(stack: &mut Stack<'_>, metatable: Option<&Table>) -> Result<()> {
    match metatable {
        Some(metatable) => stack.push_ref(&metatable.reference),
        None => stack.push_nil(),
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

    pub fn to_vec(&self) -> Result<Vec<u8>> {
        let mut stack = Stack::main(self.reference.vm());
        stack.push_ref(&self.reference)?;
        match stack.read_value(StackIndex::TOP)? {
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

    /// Whether the userdata holds a `T`.
    pub fn is<T>(&self) -> bool
    where
        T: 'static,
    {
        self.reference.vm().userdata_tag::<T>() == Some(self.tag)
    }

    pub fn set_metatable(&self, metatable: Option<&Table>) -> Result<()> {
        let mut stack = Stack::main(self.reference.vm());
        stack.push_ref(&self.reference)?;
        push_metatable(&mut stack, metatable)?;
        stack.set_metatable(StackIndex::relative(-2))
    }

    /// The `T` the userdata holds; fails for userdata holding any other type.
    pub fn borrow<T>(&self) -> Result<UserDataRef<'_, T>>
    where
        T: 'static,
    {
        UserDataRef::new(self)
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

impl From<Table> for Value {
    fn from(value: Table) -> Self {
        Self::Table(value)
    }
}

impl From<&Table> for Value {
    fn from(value: &Table) -> Self {
        Self::Table(value.clone())
    }
}

impl From<Function> for Value {
    fn from(value: Function) -> Self {
        Self::Function(value)
    }
}

impl From<&Function> for Value {
    fn from(value: &Function) -> Self {
        Self::Function(value.clone())
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
}

pub struct ArgReader<'vm> {
    /// The native function's arguments are what is on it below its base.
    stack: Stack<'vm>,
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
    fn new(stack: Stack<'vm>, names: impl IntoIterator<Item = Arc<str>>) -> Self {
        Self {
            cursor: ArgCursor::new(stack.base(), names),
            stack,
        }
    }

    pub fn vm(&self) -> &'vm Vm {
        self.stack.vm()
    }

    /// The name of the argument the next read takes, for a reader's own error messages.
    pub fn next_name(&self) -> Arc<str> {
        self.current_name()
    }

    pub fn read<T: FromLuau<'vm>>(&mut self) -> Result<T> {
        T::read(self)
    }

    /// Reads one of `T`'s strings, failing with the accepted ones for any other string.
    pub fn read_string_enum<T: crate::StringEnum>(&mut self) -> Result<T> {
        let (name, bytes) = self.next_named_borrowed_bytes("string")?;
        let value = String::from_utf8_lossy(bytes);
        T::from_str(&value).ok_or_else(|| Error::UnknownEnumValue {
            name,
            expected: T::VALUES,
            value: value.into_owned(),
        })
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
        let index = StackIndex::argument(self.cursor.index());
        let actual_type = self.stack.type_at(index);
        if actual_type != sys::LUA_TBOOLEAN {
            return Err(Error::ArgumentType {
                name,
                expected: "boolean",
                actual: stack_type_name(actual_type),
            });
        }
        let value = self.stack.read_bool(index);
        self.cursor.advance();
        Ok(value)
    }

    fn next_i64(&mut self) -> Result<i64> {
        let name = self.current_name();
        if self.cursor.is_finished() {
            return Err(Error::MissingArgument(name));
        }
        let index = StackIndex::argument(self.cursor.index());
        let actual_type = self.stack.type_at(index);
        let value = match actual_type {
            sys::LUA_TINTEGER => self.stack.read_integer(index),
            sys::LUA_TNUMBER => checked_i64_arg(name.clone(), self.stack.read_number(index))?,
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
        let index = StackIndex::argument(self.cursor.index());
        let actual_type = self.stack.type_at(index);
        let value = match actual_type {
            sys::LUA_TINTEGER => self.stack.read_integer(index) as f64,
            sys::LUA_TNUMBER => self.stack.read_number(index),
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
        self.stack.read_value(StackIndex::argument(index))
    }

    fn bytes_at(&self, index: usize, expected: &'static str) -> &[u8] {
        let index = StackIndex::argument(index);
        let bytes = if expected == "buffer" {
            self.stack.buffer_bytes(index)
        } else {
            self.stack.string_bytes(index)
        };
        bytes.unwrap_or_default()
    }

    fn type_name_at(&self, index: usize) -> &'static str {
        stack_type_name(self.stack.type_at(StackIndex::argument(index)))
    }
}

fn quoted_list(values: &[&str]) -> String {
    values
        .iter()
        .map(|value| format!("{value:?}"))
        .collect::<Vec<_>>()
        .join(", ")
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

/// A value read in place: a string's or buffer's bytes are the VM's own, borrowed for as long as
/// whatever read the value keeps it.
#[derive(Debug)]
pub enum ValueRef<'a> {
    Nil,
    Boolean(bool),
    Integer(i64),
    Number(f64),
    String(&'a [u8]),
    Buffer(&'a [u8]),
    Table(Table),
    Function(Function),
    Thread(Thread),
    UserData(UserData),
}

impl ValueRef<'_> {
    /// The value, with its bytes copied.
    pub fn into_value(self) -> Value {
        match self {
            Self::Nil => Value::Nil,
            Self::Boolean(value) => Value::Boolean(value),
            Self::Integer(value) => Value::Integer(value),
            Self::Number(value) => Value::Number(value),
            Self::String(bytes) => Value::String(bytes.to_vec()),
            Self::Buffer(bytes) => Value::Buffer(bytes.to_vec()),
            Self::Table(table) => Value::Table(table),
            Self::Function(function) => Value::Function(function),
            Self::Thread(thread) => Value::Thread(thread),
            Self::UserData(userdata) => Value::UserData(userdata),
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Nil => "nil",
            Self::Boolean(_) => "boolean",
            Self::Integer(_) | Self::Number(_) => "number",
            Self::String(_) => "string",
            Self::Buffer(_) => "buffer",
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

/// The remaining arguments of a call, each read as `T`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Variadic<T>(pub Vec<T>);

impl<'vm, T> FromLuau<'vm> for Variadic<T>
where
    T: FromLuau<'vm>,
{
    fn read(reader: &mut ArgReader<'vm>) -> Result<Self> {
        let mut values = Vec::new();
        while !reader.cursor.is_finished() {
            values.push(T::read(reader)?);
        }
        Ok(Self(values))
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
    site: Option<CallSite>,
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

    /// Marks the future as finishing a call to `site`, whose errors it raises as that call would.
    pub fn for_call(mut self, site: CallSite) -> Self {
        self.site = Some(site);
        self
    }

    /// The error to raise in the waiting thread for the future's `error`.
    pub fn raised_error(&self, error: &Error) -> RaisedError {
        match &self.site {
            Some(site) => site.raised_error(error),
            None => RaisedError::of(error, error.to_string()),
        }
    }

    fn with_start_delay<F, T>(start_delay: Option<Duration>, future: F) -> Self
    where
        F: Future<Output = Result<T>> + 'static,
        T: IntoLuauReturn + 'static,
    {
        Self {
            start_delay,
            site: None,
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
    callback: NativeFn,
    site: CallSite,
    capability: Option<CapabilityId>,
    task_group: TaskGroupId,
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

impl FromCallFrame for Vm {
    fn from_call_frame(vm: &Vm, _context: &CallContext) -> Result<Self> {
        Ok(vm.clone())
    }
}

impl FromCallFrame for CallContext {
    fn from_call_frame(_vm: &Vm, context: &CallContext) -> Result<Self> {
        Ok(context.clone())
    }
}

impl FromCallFrame for ChunkOrigin {
    fn from_call_frame(_vm: &Vm, context: &CallContext) -> Result<Self> {
        Ok(context.origin.clone())
    }
}

pub struct CallFrame<'vm> {
    pub vm: &'vm Vm,
    pub thread: Thread,
    pub context: CallContext,
    pub args: ArgReader<'vm>,
    pub returns: ReturnWriter<'vm>,
    pub site: CallSite,
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
    let exit = abort_on_panic(|| unsafe { call_native_function(state) });
    match exit {
        CallbackExit::Return(count) => count,
        CallbackExit::Yield(count) => unsafe { gateway::yield_results(state, count) },
        CallbackExit::Raise => unsafe { gateway::raise(state) },
    }
}

/// Runs a native function and leaves its results on top of the stack, or the error to raise.
unsafe fn call_native_function(state: *mut sys::lua_State) -> CallbackExit {
    let state = NonNull::new(state).expect("Luau calls a native function on a thread");
    // SAFETY: Luau calls `native_callback` only as the closure `Stack::push_native_function`
    // makes.
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
    // SAFETY: as for `run_native_function`, whose stacks are gone.
    match unsafe { run_native_function(&vm, slot, state) } {
        Ok(exit) => exit,
        Err(error) => {
            // SAFETY: as for `run_native_function`, whose stacks are gone.
            let mut stack = unsafe { Stack::from_raw(&vm, state) };
            stack.push_raised(&error);
            stack.keep();
            CallbackExit::Raise
        }
    }
}

/// # Safety
///
/// Luau calls the native function `slot` holds on `state`, a live thread of `vm`.
unsafe fn run_native_function(
    vm: &Vm,
    slot: &CallbackSlot,
    state: NonNull<sys::lua_State>,
) -> std::result::Result<CallbackExit, RaisedError> {
    // SAFETY: a live thread of `vm`, as the caller guarantees.
    let mut args = unsafe { Stack::from_raw(vm, state) };
    let thread = args.own_thread().map_err(|error| error.to_string())?;
    let thread_context = vm
        .call_context_for_state(thread.state_id())
        .map_err(|error| error.to_string())?;
    let mut return_state = ReturnState::default();
    let result = {
        let returns = ReturnWriter::borrowed(&mut return_state);
        let caller = thread_context
            .as_ref()
            .map(|context| context.caller.clone())
            .unwrap_or_default();
        // A function installed for another plugin acts in that plugin's run.
        let task_group = match &thread_context {
            Some(context)
                if slot.use_thread_context_origin
                    || slot.site.origin.plugin.is_none()
                    || slot.site.origin.plugin == context.origin.plugin =>
            {
                context.task_group
            }
            _ => slot.task_group,
        };
        let origin = if slot.use_thread_context_origin {
            thread_context
                .as_ref()
                .map(|context| context.origin.clone())
                .unwrap_or_else(|| slot.site.origin.clone())
        } else {
            slot.site.origin.clone()
        };
        let frame = CallFrame {
            vm,
            thread: Thread::new(thread, origin.clone()),
            context: CallContext {
                origin,
                capability: slot.capability.clone(),
                caller,
                task_group,
            },
            args: ArgReader::new(args, slot.argument_names.clone()),
            returns: returns.clone(),
            site: slot.site.clone(),
        };

        catch_unwind(AssertUnwindSafe(|| (slot.callback)(frame)))
    };
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => return Err(slot.site.raised_error(&error)),
        Err(_) => return Err("Harmony callback panicked".into()),
    }
    // SAFETY: as for the arguments' stack, which is gone.
    let mut results = unsafe { Stack::from_raw(vm, state) };
    for value in &return_state.values {
        results
            .push_value(value)
            .map_err(|error| error.to_string())?;
    }
    let count = return_state.values.len() as c_int;
    if return_state.yield_requested && !results.is_yieldable() {
        return Err("attempt to yield from a non-yieldable Harmony callback".into());
    }
    results.keep();
    Ok(if return_state.yield_requested {
        CallbackExit::Yield(count)
    } else {
        CallbackExit::Return(count)
    })
}

/// The userdata tag of `HostError` values, kept from the tags [`Vm::userdata_tag`] hands out.
const HOST_ERROR_TAG: UserDataTag = UserDataTag(sys::LUA_UTAG_LIMIT - 1);

/// What a `HostError` value holds: an [`Error::Kinded`] raised into Luau.
struct HostError {
    kind: &'static str,
    message: String,
}

/// `HostError.__tostring`: the error's message. Luau gives a C function enough stack for it, so
/// pushing it fails only by running out of memory.
unsafe extern "C-unwind" fn host_error_tostring(state: *mut sys::lua_State) -> c_int {
    let pushed = abort_on_panic(|| unsafe {
        match stack::host_error_at(state, 1) {
            Some(error) => match gateway::push_bytes(state, error.message.as_bytes()) {
                Ok(()) => true,
                Err(error) => {
                    gateway::push_error(state, &error.to_string());
                    false
                }
            },
            None => {
                gateway::push_error(state, "expected a host error");
                false
            }
        }
    });
    if pushed {
        1
    } else {
        unsafe { gateway::raise(state) }
    }
}

/// The native function a call went to, which names where its errors came from.
#[derive(Clone, Debug)]
pub struct CallSite {
    origin: ChunkOrigin,
    function_name: Option<Arc<str>>,
}

impl CallSite {
    /// The error the call raises for `error`: its message prefixed with where it came from, except
    /// running out of memory, which keeps Luau's own message so callers further out still see
    /// [`Error::OutOfMemory`].
    pub fn raised_error(&self, error: &Error) -> RaisedError {
        if matches!(error, Error::OutOfMemory) {
            return error.to_string().into();
        }
        let module = self
            .origin
            .module
            .as_ref()
            .map(|module| module.0.as_ref())
            .unwrap_or("<unknown module>");
        let function = self
            .function_name
            .as_deref()
            .unwrap_or("<unknown function>");
        let plugin = self.origin.plugin.as_deref().unwrap_or("<non-plugin>");
        RaisedError::of(
            error,
            format!("module '{module}' function '{function}' plugin '{plugin}': {error}"),
        )
    }
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

/// Closes a thread [`Thread::cancel`] found live as soon as a resume leaves it suspended, before
/// whatever resumed it runs again.
unsafe extern "C-unwind" fn postresume(thread: *mut sys::lua_State) {
    unsafe {
        if sys::lua_status(thread) != sys::LUA_YIELD || !gateway::is_cancelled(thread) {
            return;
        }
        match sys::lua_callback_userdata(thread)
            .cast::<VmControl>()
            .as_ref()
        {
            Some(control) => control.without_memory_limit(|| gateway::close_yielded(thread)),
            None => gateway::close_yielded(thread),
        }
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
            let signal = control.interrupt_signal.load(Ordering::Acquire);
            // SAFETY: the signal and the note belong to a live budget in `interrupt_budgets`,
            // which stops pointing the interrupt at them before dropping them, on this same
            // thread.
            let cause = if !signal.is_null() && unsafe { (*signal).load(Ordering::Acquire) } {
                InterruptCause::Signal
            } else {
                let deadline = control.interrupt_deadline_millis.load(Ordering::Acquire);
                if deadline == 0 || current_unix_millis() < deadline {
                    return false;
                }
                InterruptCause::Time
            };
            let interrupted = control.interrupt_interrupted.load(Ordering::Acquire);
            if !interrupted.is_null() {
                unsafe { (*interrupted).store(cause as u8, Ordering::Release) };
            }
            true
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
        Stack::main(vm).top()
    }

    /// Dropping a stack out of turn panics before cutting its thread back, which would pop what
    /// the later stack reads.
    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn a_stack_dropped_before_a_later_one_on_its_thread_panics() -> Result<()> {
        let vm = Vm::new()?;
        let mut outer = Stack::main(&vm);
        outer.push_bytes(b"outer")?;
        let mut inner = Stack::main(&vm);
        inner.push_bytes(b"inner")?;
        let bytes = inner.string_bytes(StackIndex::TOP);

        let panic = catch_unwind(AssertUnwindSafe(move || drop(outer)))
            .expect_err("dropping the outer stack first panics");
        assert_eq!(
            panic.downcast_ref::<&str>(),
            Some(&"a Luau stack dropped before one made on its thread after it")
        );
        assert_eq!(bytes, Some(b"inner".as_slice()));
        assert_eq!(inner.top(), 2, "both values are still on the stack");
        drop(inner);
        assert_eq!(stack_top(&vm), 1, "the outer stack's value stays");
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn stacks_on_different_threads_drop_in_any_order() -> Result<()> {
        let vm = Vm::new()?;
        let function = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(&b"return 1"[..]),
            ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&function)?;
        let mut main = Stack::main(&vm);
        main.push_bytes(b"main")?;
        let mut coroutine = Stack::of_thread(&thread);
        coroutine.push_bytes(b"coroutine")?;
        drop(main);
        assert_eq!(
            coroutine.string_bytes(StackIndex::TOP),
            Some(b"coroutine".as_slice())
        );
        drop(coroutine);
        assert_eq!(stack_top(&vm), 0);
        Ok(())
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

        let kinded = || Error::Kinded {
            kind: "missing",
            message: "track not found".to_string(),
        };
        let error = Error::from(anyhow::Error::new(kinded()));
        assert!(
            matches!(error, Error::Kinded { kind: "missing", message } if message == "track not found")
        );
        let error = Error::from(anyhow::Error::new(kinded()).context("loading track"));
        assert!(
            matches!(error, Error::Kinded { kind: "missing", message } if message == "loading track")
        );
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
        assert_eq!(table.get("answer")?, Value::Number(42.0));

        let Value::Function(function) = values[1].clone() else {
            panic!("expected second return value to be a function");
        };
        assert_eq!(
            function.call(&[Value::Number(8.0)])?,
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
    fn table_visits_leave_the_stack_and_table_as_they_found_them() -> Result<()> {
        let vm = Vm::new()?;
        let table = vm.create_table()?;
        for index in 1..=4 {
            table.set_integer(index, Value::String(format!("v{index}").into_bytes()))?;
        }
        table.set("name", "lyra")?;
        let nested = vm.create_table()?;
        nested.set("inner", true)?;
        table.set("nested", Value::Table(nested))?;

        let mut seen = 0;
        let failed = table.for_each_pair(|_, value| {
            seen += 1;
            if let ValueRef::Table(nested) = value {
                nested.for_each_pair(|_, _| Ok(ControlFlow::Continue(())))?;
            }
            if seen == 3 {
                return Err(Error::Runtime("stop".into()));
            }
            Ok(ControlFlow::Continue(()))
        });
        assert!(matches!(failed, Err(Error::Runtime(message)) if message == "stop"));
        assert_eq!(stack_top(&vm), 0);

        let mut visited = 0;
        table.for_each_pair(|_, _| {
            visited += 1;
            Ok(if visited == 2 {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            })
        })?;
        assert_eq!(visited, 2);
        assert_eq!(stack_top(&vm), 0);

        let failed = table.for_each_array_value(|value| match value {
            ValueRef::String(b"v2") => Err(Error::Runtime("stop".into())),
            _ => Ok(ControlFlow::Continue(())),
        });
        assert!(failed.is_err());
        assert_eq!(stack_top(&vm), 0);

        let mut first = Vec::new();
        table.for_each_array_value(|value| {
            first.push(value.into_value());
            Ok(ControlFlow::Break(()))
        })?;
        assert_eq!(first, vec![Value::String(b"v1".to_vec())]);
        assert_eq!(stack_top(&vm), 0);

        assert_eq!(table.pairs()?.len(), 6);
        assert_eq!(table.array_values()?.len(), 4);
        table.set("after", 1.0)?;
        assert_eq!(table.get("after")?, Value::Number(1.0));
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn table_raw_access_uses_vm_checked_handles() -> Result<()> {
        let vm = Vm::new()?;
        let table = vm.create_table()?;

        table.set("name", Value::String(b"demo".to_vec()))?;
        table.set("count", Value::Integer(7))?;
        table.set_integer(1, Value::String(b"first".to_vec()))?;
        table.set_integer(2, Value::Integer(9))?;

        assert_eq!(table.vm_id(), vm.id());
        assert_eq!(table.get("name")?, Value::String(b"demo".to_vec()));
        assert_eq!(table.get("count")?, Value::Integer(7));
        assert_eq!(table.len()?, 2);
        assert_eq!(table.get_integer(1)?, Value::String(b"first".to_vec()));
        assert_eq!(
            table.array_values()?,
            vec![Value::String(b"first".to_vec()), Value::Integer(9)]
        );
        assert!(table.pairs()?.contains(&(
            Value::String(b"name".to_vec()),
            Value::String(b"demo".to_vec())
        )));
        assert!(table.pairs()?.iter().any(|(key, value)| {
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

        metatable.set("marker", Value::Boolean(true))?;
        table.set_metatable(Some(&metatable))?;
        let returned = table.metatable()?.expect("table should have metatable");

        assert_eq!(returned.get("marker")?, Value::Boolean(true));
        table.set_metatable(None)?;
        assert!(table.metatable()?.is_none());
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn userdata_metatable_rejects_tables_from_another_vm() -> Result<()> {
        let owner = Vm::new()?;
        let other = Vm::new()?;
        let userdata = owner.create_userdata(0_u8)?;
        let foreign = other.create_table()?;

        let error = userdata
            .set_metatable(Some(&foreign))
            .expect_err("a metatable from another VM must fail");
        assert!(matches!(error, Error::VmMismatch { .. }));
        assert_eq!(stack_top(&owner), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn table_handles_fail_fast_across_vms() -> Result<()> {
        let owner = Vm::new()?;
        let other = Vm::new()?;
        let table = owner.create_table()?;
        let foreign = other.create_table()?;

        let error = table
            .set("child", &foreign)
            .expect_err("storing a table from another VM must fail");
        assert!(matches!(error, Error::VmMismatch { .. }));
        assert_eq!(stack_top(&owner), 0);
        assert_eq!(stack_top(&other), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn buffer_handles_round_trip_bytes() -> Result<()> {
        let vm = Vm::new()?;
        let buffer = vm.create_buffer(b"abc")?;

        assert_eq!(buffer.vm_id(), vm.id());
        assert_eq!(buffer.to_vec()?, b"abc".to_vec());
        assert_eq!(stack_top(&vm), 0);
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
        let userdata = vm.create_userdata(PluginObject { value: 42 })?;

        assert_eq!(userdata.vm_id(), vm.id());
        assert!(userdata.is::<PluginObject>());
        assert_eq!(userdata.borrow::<PluginObject>()?.value, 42);

        let callback: NativeFn = Arc::new(move |mut frame| {
            let userdata: UserData = frame.args.read_named("object")?;
            let object = userdata.borrow::<PluginObject>()?;
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
            function.call(&[Value::UserData(userdata)])?,
            vec![Value::Number(42.0)]
        );
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn userdata_borrows_only_as_the_type_it_holds() -> Result<()> {
        let vm = Vm::new()?;
        let byte = vm.create_userdata(42_u8)?;

        assert!(!byte.is::<String>());
        let mismatch = byte
            .borrow::<String>()
            .err()
            .expect("a u8 borrowed as a String");
        assert_eq!(
            mismatch.to_string(),
            "expected userdata of type alloc::string::String"
        );
        assert_eq!(vm.userdata_tag::<String>(), None);
        let text = vm.create_userdata(String::from("text"))?;
        assert!(matches!(
            byte.borrow::<String>(),
            Err(Error::UserDataTypeMismatch { .. })
        ));
        assert!(matches!(
            text.borrow::<u8>(),
            Err(Error::UserDataTypeMismatch { .. })
        ));
        assert_eq!(*byte.borrow::<u8>()?, 42);
        assert_eq!(*text.borrow::<String>()?, "text");
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
            let userdata = vm.create_userdata(DropFlag(freed.clone()))?;
            let function = vm.load_chunk(&Chunk::new(
                Arc::<[u8]>::from(&b"local held = ...; return function() return held end"[..]),
                ChunkOrigin::default(),
            ))?;
            let Some(Value::Function(closure)) = function
                .call(&[Value::UserData(userdata)])?
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
            thread.resume(&[Value::Number(20.0), Value::Number(22.0)])?,
            ThreadStatus::Completed(vec![Value::Number(42.0)])
        );
        assert!(matches!(
            thread.resume(&[]),
            Err(Error::Runtime(message)) if message.contains("dead coroutine")
        ));
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn thread_resume_error_raises_where_the_thread_yielded() -> Result<()> {
        let vm = Vm::new()?;
        vm.open_standard_libraries(StandardLibraries::all_supported())?;
        let function = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(
                &br#"
                    local ok, err = pcall(function()
                        return coroutine.yield("waiting")
                    end)
                    local value = coroutine.yield("caught")
                    return ok, err, value
                "#[..],
            ),
            ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&function)?;

        assert_eq!(
            thread.resume(&[])?,
            ThreadStatus::Yielded(vec![Value::String(b"waiting".to_vec())])
        );
        assert_eq!(
            thread.resume_error(&"boom".into())?,
            ThreadStatus::Yielded(vec![Value::String(b"caught".to_vec())])
        );
        assert_eq!(
            thread.resume(&[Value::Number(1.0)])?,
            ThreadStatus::Completed(vec![
                Value::Boolean(false),
                Value::String(b"boom".to_vec()),
                Value::Number(1.0),
            ])
        );
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    /// What Luau makes of `thread`: its status, and what resuming it returns.
    fn status_and_resume(vm: &Vm, thread: &Thread) -> Result<Vec<Value>> {
        vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(
                &b"local co = ...; return coroutine.status(co), coroutine.resume(co)"[..],
            ),
            ChunkOrigin::default(),
        ))?
        .call(&[Value::Thread(thread.clone())])
    }

    fn dead() -> Vec<Value> {
        vec![
            Value::String(b"dead".to_vec()),
            Value::Boolean(false),
            Value::String(b"cannot resume dead coroutine".to_vec()),
        ]
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn a_closed_thread_is_dead_to_luau_whatever_it_was_doing() -> Result<()> {
        let vm = Vm::new()?;
        vm.open_standard_libraries(StandardLibraries::all_supported())?;
        let values = vm.eval(
            Arc::<[u8]>::from(
                &br#"
                    local function descend(depth)
                        if depth == 0 then
                            coroutine.yield()
                            ran_on = true
                        else
                            descend(depth - 1)
                        end
                        return depth
                    end
                    local suspended = coroutine.create(function() descend(100) end)
                    coroutine.resume(suspended)
                    local unstarted = coroutine.create(function() ran_on = true end)
                    local finished = coroutine.create(function() end)
                    coroutine.resume(finished)
                    local failed = coroutine.create(function() error("boom") end)
                    coroutine.resume(failed)
                    return suspended, unstarted, finished, failed
                "#[..],
            ),
            ChunkOrigin::default(),
        )?;
        assert_eq!(values.len(), 4);

        for value in &values {
            let Value::Thread(thread) = value else {
                panic!("expected a thread");
            };
            thread.close()?;
            thread.close()?;
            assert_eq!(status_and_resume(&vm, thread)?, dead());
            assert!(thread.resume(&[]).is_err());
        }
        assert_eq!(
            vm.eval(
                Arc::<[u8]>::from(&b"return ran_on"[..]),
                ChunkOrigin::default()
            )?,
            vec![Value::Nil]
        );
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn a_thread_that_is_running_or_has_resumed_another_refuses_to_close() -> Result<()> {
        let vm = Vm::new()?;
        vm.open_standard_libraries(StandardLibraries::all_supported())?;
        let close: NativeFn = Arc::new(|mut frame| {
            let thread: Thread = frame.args.read()?;
            frame.returns.write(thread.close().is_err())?;
            Ok(())
        });
        let close = vm.create_function_with_options(
            NativeFunctionOptions::new(ChunkOrigin::default()).function_name("close"),
            close,
        )?;
        vm.set_global_function("close", &close)?;

        assert_eq!(
            vm.eval(
                Arc::<[u8]>::from(
                    &br#"
                        return coroutine.wrap(function()
                            local outer = coroutine.running()
                            local inner = coroutine.create(function()
                                local closed_self = close(coroutine.running())
                                local closed_outer = close(outer)
                                coroutine.yield(closed_self, closed_outer)
                                return "went on"
                            end)
                            local _, closed_self, closed_outer = coroutine.resume(inner)
                            return closed_self, closed_outer, select(2, coroutine.resume(inner))
                        end)()
                    "#[..],
                ),
                ChunkOrigin::default(),
            )?,
            vec![
                Value::Boolean(true),
                Value::Boolean(true),
                Value::String(b"went on".to_vec()),
            ]
        );
        Ok(())
    }

    /// A VM with natives for a thread to be cancelled live: `cancel(thread)` another, or its own
    /// without one, `cancel_and_wait()` its own before asking to yield, and `wait()`, which asks
    /// to yield.
    fn vm_that_cancels() -> Result<Vm> {
        let vm = Vm::new()?;
        vm.open_standard_libraries(StandardLibraries::all_supported())?;
        let cancel_live = |thread: &Thread| -> Result<()> {
            assert_eq!(thread.cancel()?, ThreadEnd::ClosedAtYield);
            Ok(())
        };
        let cancel: NativeFn = Arc::new(move |mut frame| {
            let thread: Option<Thread> = frame.args.read()?;
            cancel_live(thread.as_ref().unwrap_or(&frame.thread))
        });
        let cancel_and_wait: NativeFn = Arc::new(move |mut frame| {
            cancel_live(&frame.thread)?;
            frame.returns.request_yield();
            Ok(())
        });
        let wait: NativeFn = Arc::new(|mut frame| {
            frame.returns.request_yield();
            Ok(())
        });
        for (name, function) in [
            ("cancel", cancel),
            ("cancel_and_wait", cancel_and_wait),
            ("wait", wait),
        ] {
            let function = vm.create_function_with_options(
                NativeFunctionOptions::new(ChunkOrigin::default()).function_name(name),
                function,
            )?;
            vm.set_global_function(name, &function)?;
        }
        Ok(vm)
    }

    fn eval_str(vm: &Vm, source: &str) -> Result<Vec<Value>> {
        vm.eval(Arc::<[u8]>::from(source.as_bytes()), ChunkOrigin::default())
    }

    fn string(value: &str) -> Value {
        Value::String(value.as_bytes().to_vec())
    }

    /// Runs `body` as the coroutine `co`, resumed by Luau code, and returns how many values that
    /// resume returned and the first two, then `ran_on` and `went_on`, then the coroutine's
    /// status and what resuming it again returns.
    fn resume_body(vm: &Vm, body: &str) -> Vec<Value> {
        eval_str(
            vm,
            &format!(
                r#"
                    ran_on, went_on = nil, nil
                    co = coroutine.create(function() {body} end)
                    local resumed = table.pack(coroutine.resume(co))
                    return resumed.n, resumed[1], resumed[2], ran_on, went_on,
                        coroutine.status(co), coroutine.resume(co)
                "#
            ),
        )
        .expect("the resuming chunk runs")
    }

    /// [`resume_body`] through `coroutine.wrap`: how many values the call returned, then
    /// `ran_on` and `went_on`, then what calling it again raises.
    fn wrap_body(vm: &Vm, body: &str) -> Vec<Value> {
        eval_str(
            vm,
            &format!(
                r#"
                    ran_on, went_on = nil, nil
                    local wrapped = coroutine.wrap(function() {body} end)
                    local returned = table.pack(wrapped())
                    return returned.n, ran_on, went_on, pcall(wrapped)
                "#
            ),
        )
        .expect("the calling chunk runs")
    }

    fn interrupt_is_installed(vm: &Vm) -> bool {
        unsafe { (*sys::lua_callbacks(vm.inner.state.as_ptr())).interrupt }.is_some()
    }

    /// Each body runs on past the cancel, calls included, to where it yields, and is closed
    /// there: the resume returns no values, and nothing after the yield runs.
    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn a_thread_cancelled_while_it_runs_is_closed_where_it_next_yields() -> Result<()> {
        let vm = vm_that_cancels()?;
        for body in [
            "cancel() ran_on = true coroutine.yield('lost') went_on = true",
            "cancel() ran_on = true wait() went_on = true",
            "ran_on = true cancel_and_wait() went_on = true",
            "cancel() ran_on = true pcall(coroutine.yield) went_on = true",
            "cancel() ran_on = true pcall(wait) went_on = true",
            "cancel() ran_on = true xpcall(coroutine.yield, error) went_on = true",
            "cancel() ran_on = true while true do pcall(coroutine.yield) end",
            // Luau's own, which still holds the thread yieldable.
            "cancel() ran_on = coroutine.isyieldable() coroutine.yield() went_on = true",
            // Cancelled by a coroutine it resumed, while it wasn't the one running.
            r#"
                local outer = coroutine.running()
                coroutine.resume(coroutine.create(function() cancel(outer) end))
                ran_on = true
                coroutine.yield()
                went_on = true
            "#,
        ] {
            assert_eq!(
                resume_body(&vm, body),
                vec![
                    Value::Number(1.0),
                    Value::Boolean(true),
                    Value::Nil,
                    Value::Boolean(true),
                    Value::Nil,
                    string("dead"),
                    Value::Boolean(false),
                    string("cannot resume dead coroutine"),
                ],
                "{body}"
            );
            assert_eq!(
                wrap_body(&vm, body),
                vec![
                    Value::Number(0.0),
                    Value::Boolean(true),
                    Value::Nil,
                    Value::Boolean(false),
                    string("cannot resume dead coroutine"),
                ],
                "{body}"
            );
        }
        assert!(!interrupt_is_installed(&vm));
        Ok(())
    }

    /// The reset closes the thread's upvalues first, so a closure it made outlives it with the
    /// values it saw.
    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn a_closure_made_in_a_thread_closed_where_it_yields_keeps_its_upvalues() -> Result<()> {
        let vm = vm_that_cancels()?;
        let resumed = resume_body(
            &vm,
            r#"
                local kept = "kept"
                read_kept = function()
                    return kept
                end
                cancel()
                ran_on = true
                kept = "changed"
                coroutine.yield()
                went_on = true
            "#,
        );
        assert_eq!(resumed[5], string("dead"));
        assert_eq!(
            eval_str(&vm, "return read_kept()")?,
            vec![string("changed")]
        );
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn a_cancelled_thread_that_returns_or_fails_first_ends_as_any_other() -> Result<()> {
        let vm = vm_that_cancels()?;
        for (body, ok, result) in [
            ("cancel() ran_on = true return 'returned'", true, "returned"),
            ("cancel() ran_on = true error('failed', 0)", false, "failed"),
        ] {
            assert_eq!(
                resume_body(&vm, body),
                vec![
                    Value::Number(2.0),
                    Value::Boolean(ok),
                    string(result),
                    Value::Boolean(true),
                    Value::Nil,
                    string("dead"),
                    Value::Boolean(false),
                    string("cannot resume dead coroutine"),
                ],
                "{body}"
            );
        }
        Ok(())
    }

    /// Harmony's own resume, by value and by error, tells a thread closed as it yielded from one
    /// that is waiting.
    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn resuming_a_thread_cancelled_as_it_runs_reads_as_closed_once_it_yields() -> Result<()> {
        let vm = vm_that_cancels()?;
        let thread = |source: &'static str| {
            vm.create_thread(&vm.load_chunk(&Chunk::new(
                Arc::<[u8]>::from(source.as_bytes()),
                ChunkOrigin::default(),
            ))?)
        };
        let yields = thread("cancel() coroutine.yield(1) went_on = true")?;
        assert_eq!(yields.resume(&[])?, ThreadStatus::Closed);
        let caught = thread("pcall(coroutine.yield) cancel() wait() went_on = true")?;
        assert_eq!(caught.resume(&[])?, ThreadStatus::Yielded(vec![]));
        assert_eq!(caught.resume_error(&"boom".into())?, ThreadStatus::Closed);

        for thread in [&yields, &caught] {
            assert_eq!(status_and_resume(&vm, thread)?, dead());
            assert!(thread.resume(&[]).is_err());
        }
        assert_eq!(eval_str(&vm, "return went_on")?, vec![Value::Nil]);
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    /// The cancelled thread is the one that resumed the coroutine asking. That coroutine, beneath
    /// it, yields back to it and is resumed again later, as if nothing had happened.
    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn the_threads_a_cancelled_thread_resumed_are_left_as_they_are() -> Result<()> {
        let vm = vm_that_cancels()?;
        assert_eq!(
            eval_str(
                &vm,
                r#"
                    local outer, inner, inner_status
                    outer = coroutine.create(function()
                        inner = coroutine.create(function()
                            cancel(outer)
                            local back = coroutine.yield("yielded")
                            wait()
                            return "returned", back
                        end)
                        local _, yielded = coroutine.resume(inner)
                        inner_status = coroutine.status(inner)
                        return yielded
                    end)
                    local ok, yielded = coroutine.resume(outer)
                    local resumed, waiting = coroutine.resume(inner, "back")
                    return ok, yielded, coroutine.status(outer), inner_status, resumed, waiting,
                        coroutine.status(inner), coroutine.resume(inner)
                "#,
            )?,
            vec![
                Value::Boolean(true),
                string("yielded"),
                string("dead"),
                string("suspended"),
                Value::Boolean(true),
                Value::Nil,
                string("suspended"),
                Value::Boolean(true),
                string("returned"),
                string("back"),
            ]
        );
        Ok(())
    }

    /// Luau gives a new thread the memory of one it freed. The thread that takes a cancelled
    /// thread's place yields as any other.
    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn a_thread_made_where_a_cancelled_one_was_freed_can_yield() -> Result<()> {
        let vm = vm_that_cancels()?;
        let function = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(&b"local back = coroutine.yield(1) wait() return back"[..]),
            ChunkOrigin::default(),
        ))?;
        // Keeps the page of the thread to free in use, so that Luau keeps the page.
        let _neighbour = vm.create_thread(&function)?;
        let Some(Value::Thread(cancelled)) = eval_str(
            &vm,
            "local co = coroutine.create(function() cancel() end) coroutine.resume(co) return co",
        )?
        .pop() else {
            panic!("expected a thread");
        };
        let freed = cancelled.state_id();
        assert!(Stack::of_thread(&cancelled).is_cancelled());
        drop(cancelled);
        unsafe { sys::lua_gc(vm.inner.state.as_ptr(), sys::LUA_GCCOLLECT, 0) };

        let mut made = Vec::new();
        let reused = loop {
            let thread = vm.create_thread(&function)?;
            if thread.state_id() == freed {
                break thread;
            }
            made.push(thread);
            assert!(made.len() < 4096, "no thread reused the freed one's memory");
        };

        assert_eq!(
            reused.resume(&[])?,
            ThreadStatus::Yielded(vec![Value::Number(1.0)])
        );
        assert_eq!(
            reused.resume(&[Value::Number(2.0)])?,
            ThreadStatus::Yielded(vec![])
        );
        assert_eq!(
            reused.resume(&[])?,
            ThreadStatus::Completed(vec![Value::Number(2.0)])
        );
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn cancel_closes_a_suspended_thread_and_neither_closes_the_main_one() -> Result<()> {
        let vm = vm_that_cancels()?;
        let Some(Value::Thread(suspended)) = eval_str(
            &vm,
            "local co = coroutine.create(function() coroutine.yield() went_on = true end)
             coroutine.resume(co) return co",
        )?
        .pop() else {
            panic!("expected a thread");
        };
        assert_eq!(suspended.cancel()?, ThreadEnd::Closed);
        assert_eq!(status_and_resume(&vm, &suspended)?, dead());

        let main: NativeFn = Arc::new(|frame| {
            for refusal in [
                frame.thread.close().map(|()| ThreadEnd::Closed),
                frame.thread.cancel(),
            ] {
                let refusal = refusal.expect_err("the main thread closed");
                assert!(refusal.to_string().contains("main thread"), "{refusal}");
            }
            Ok(())
        });
        let main = vm.create_function_with_options(
            NativeFunctionOptions::new(ChunkOrigin::default()).function_name("main"),
            main,
        )?;
        main.call(&[])?;
        assert_eq!(eval_str(&vm, "return went_on")?, vec![Value::Nil]);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn a_thread_read_while_it_runs_a_native_function_reads_as_running() -> Result<()> {
        let vm = Vm::new()?;
        vm.open_standard_libraries(StandardLibraries::all_supported())?;
        let check: NativeFn = Arc::new(|mut frame| {
            let thread: Thread = frame.args.read()?;
            frame.returns.write(thread.is_started())?;
            Ok(())
        });
        let capture: NativeFn = Arc::new(|frame| {
            let Some(Value::Thread(thread)) = frame
                .vm
                .eval(Arc::<[u8]>::from(&b"return co"[..]), ChunkOrigin::default())?
                .pop()
            else {
                return Err(Error::Runtime("expected a thread".to_string()));
            };
            frame.vm.data().insert(thread)?;
            Ok(())
        });
        for (name, callback) in [("check", check), ("capture", capture)] {
            let function = vm.create_function_with_options(
                NativeFunctionOptions::new(ChunkOrigin::default()).function_name(name),
                callback,
            )?;
            vm.set_global_function(name, &function)?;
        }

        assert_eq!(
            eval_str(
                &vm,
                "co = coroutine.create(function()
                     local started = check(coroutine.running())
                     capture()
                     coroutine.yield(started)
                     return 'done'
                 end)
                 return coroutine.resume(co)"
            )?,
            vec![Value::Boolean(true), Value::Boolean(true)]
        );
        let captured = vm.data().get::<Thread>()?;
        assert!(captured.is_started());
        assert_eq!(
            captured.resume(&[])?,
            ThreadStatus::Completed(vec![string("done")])
        );
        vm.data().clear()?;
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn a_thread_closed_before_it_ran_reads_as_started() -> Result<()> {
        let vm = Vm::new()?;
        vm.open_standard_libraries(StandardLibraries::all_supported())?;
        let function = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(&b"return 1"[..]),
            ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&function)?;
        assert!(!thread.is_started());

        thread.close()?;

        assert!(thread.is_started());
        // As does a handle made from the thread afterwards, which shares no flags with the first.
        let Some(Value::Thread(read_back)) = vm
            .load_chunk(&Chunk::new(
                Arc::<[u8]>::from(&b"return ..."[..]),
                ChunkOrigin::default(),
            ))?
            .call(&[Value::Thread(thread)])?
            .pop()
        else {
            panic!("expected a thread");
        };
        assert!(read_back.is_started());
        assert!(read_back.resume(&[]).is_err());
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn closing_threads_ignores_the_memory_limit() -> Result<()> {
        let vm = Vm::new()?;
        vm.open_standard_libraries(StandardLibraries::all_supported())?;
        let function = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(
                &br#"
                    local function descend(depth)
                        if depth == 0 then
                            coroutine.yield()
                            coroutine.yield()
                        else
                            descend(depth - 1)
                        end
                        return depth
                    end
                    descend(200)
                "#[..],
            ),
            ChunkOrigin::default(),
        ))?;
        // Enough of them that shrinking their stacks needs new pages. Half are closed where they
        // wait, and half where they next yield, as cancelled threads that run.
        let threads = (0..1000)
            .map(|_| {
                let thread = vm.create_thread(&function)?;
                assert_eq!(thread.resume(&[])?, ThreadStatus::Yielded(vec![]));
                Ok(thread)
            })
            .collect::<Result<Vec<_>>>()?;
        let (closed, closed_at_yield) = threads.split_at(500);
        for thread in closed_at_yield {
            Stack::of_thread(thread).cancel();
        }
        set_gc_running(&vm, false);

        set_memory_limit(&vm, 1);
        for thread in closed {
            thread.close()?;
        }
        for thread in closed_at_yield {
            assert_eq!(thread.resume(&[])?, ThreadStatus::Closed);
        }

        assert_eq!(vm.memory_limit(), Some(1));
        set_memory_limit(&vm, 0);
        for thread in &threads {
            assert_eq!(status_and_resume(&vm, thread)?, dead());
        }
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn thread_resume_error_fails_a_thread_without_pcall() -> Result<()> {
        let vm = Vm::new()?;
        vm.open_standard_libraries(StandardLibraries::all_supported())?;
        let function = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(&b"coroutine.yield()"[..]),
            ChunkOrigin::default(),
        ))?;

        for (message, expected) in [
            ("boom", Error::Runtime("boom".to_string())),
            (gateway::MEMORY_ERROR, Error::OutOfMemory),
        ] {
            let thread = vm.create_thread(&function)?;
            assert_eq!(thread.resume(&[])?, ThreadStatus::Yielded(vec![]));
            let failed = thread.resume_error(&message.into());
            assert_eq!(
                failed.map_err(|error| error.to_string()),
                Err(expected.to_string())
            );
            assert!(thread.resume(&[]).is_err());
        }
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn guards_dropped_out_of_order_leave_the_innermost_live_budget() -> Result<()> {
        let vm = Vm::new()?;
        let function = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(&b"while true do end"[..]),
            ChunkOrigin::default(),
        ))?;
        let outer_signal = InterruptSignal::new();
        let inner_signal = InterruptSignal::new();
        let outer = vm.interrupt_after_or_on(Duration::from_secs(600), Some(&outer_signal));
        let inner = vm.interrupt_after_or_on(Duration::from_secs(600), Some(&inner_signal));
        drop(outer_signal);
        // The outer guard goes first; the inner one's budget and signal still apply.
        drop(outer);
        let control = vm.inner.control();
        assert_eq!(
            control.interrupt_signal.load(Ordering::Acquire),
            Arc::as_ptr(&inner_signal.0).cast_mut()
        );
        inner_signal.raise();
        let error = vm
            .create_thread(&function)?
            .resume(&[])
            .expect_err("the inner signal should stop the loop");
        assert!(matches!(error, Error::Runtime(message) if message.contains("interrupted")));
        drop(inner);
        assert!(control.interrupt_signal.load(Ordering::Acquire).is_null());
        assert_eq!(control.interrupt_deadline_millis.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn raised_signal_interrupts_before_the_budget_runs_out() -> Result<()> {
        let vm = Vm::new()?;
        let function = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(&b"while true do end"[..]),
            ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&function)?;
        let signal = InterruptSignal::new();
        let raiser = {
            let signal = signal.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(20));
                signal.raise();
            })
        };

        let started = Instant::now();
        let guard = vm.interrupt_after_or_on(Duration::from_secs(60), Some(&signal));
        let error = thread
            .resume(&[])
            .expect_err("the raised signal should stop the loop");
        drop(guard);
        raiser.join().expect("raiser thread");

        assert!(matches!(
            error,
            Error::Runtime(message) if message.contains("interrupted")
        ));
        assert!(started.elapsed() < Duration::from_secs(30));
        assert!(
            vm.inner
                .control()
                .interrupt_signal
                .load(Ordering::Acquire)
                .is_null(),
            "the guard restores the signal before it"
        );
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

        let signal = InterruptSignal::new();
        signal.raise();
        let on_signal = vm.interrupt_after_or_on(Duration::from_secs(600), Some(&signal));
        assert_eq!(on_signal.interrupted(), None, "nothing ran under it yet");
        vm.create_thread(&function)?
            .resume(&[])
            .expect_err("the raised signal should stop the loop");
        assert_eq!(on_signal.interrupted(), Some(InterruptCause::Signal));
        drop(on_signal);

        // The budget ran out, but what failed under it failed on its own.
        let outlasted = vm.interrupt_after(Duration::ZERO);
        let failing: NativeFn = Arc::new(|_| Err(Error::Runtime("on its own".to_string())));
        vm.create_function_with_options(
            NativeFunctionOptions::new(ChunkOrigin::default()).function_name("failing"),
            failing,
        )?
        .call(&[])
        .expect_err("the function fails");
        assert_eq!(outlasted.interrupted(), None);
        drop(outlasted);

        let guard = vm.interrupt_after(Duration::ZERO);
        let error = thread
            .resume(&[])
            .expect_err("interrupt budget should stop the loop");
        assert_eq!(guard.interrupted(), Some(InterruptCause::Time));
        assert!(error.is_interruption());
        assert!(!Error::Runtime(format!("chunk:1: {INTERRUPTED_ERROR}")).is_interruption());

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
        let foreign = other.create_table()?;

        assert!(matches!(
            foreign
                .set("thread", Value::Thread(thread.clone()))
                .expect_err("a thread stored in another VM"),
            Error::VmMismatch { .. }
        ));
        assert_eq!(
            thread.resume(&[])?,
            ThreadStatus::Completed(vec![Value::Number(1.0)])
        );
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
            thread.resume(&[Value::Number(41.0)])?,
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
    fn chunks_loaded_with_globals_share_writes_only_within_their_scope() -> Result<()> {
        let vm = Vm::new()?;
        vm.open_standard_libraries(StandardLibraries {
            base: true,
            ..StandardLibraries::none()
        })?;
        let scope = vm.create_global_scope()?;
        let function = vm.load_chunk_with_globals(
            &Chunk::new(
                Arc::<[u8]>::from(
                    &b"--!strict\nwritten = own\nlocal function read() return written end\nreturn read(), select('#', ...), ..."[..],
                ),
                ChunkOrigin::default(),
            ),
            &[("own", &Value::Number(7.0))],
            &scope,
        )?;
        assert_eq!(
            function.call(&[Value::Number(1.0), Value::Number(2.0)])?,
            vec![
                Value::Number(7.0),
                Value::Number(2.0),
                Value::Number(1.0),
                Value::Number(2.0)
            ]
        );
        let read = |scope: &Table| -> Result<Vec<Value>> {
            vm.load_chunk_with_globals(
                &Chunk::new(
                    Arc::<[u8]>::from(&b"return written, own"[..]),
                    ChunkOrigin::default(),
                ),
                &[],
                scope,
            )?
            .call(&[])
        };
        assert_eq!(read(&scope)?, vec![Value::Number(7.0), Value::Nil]);
        assert_eq!(
            read(&vm.create_global_scope()?)?,
            vec![Value::Nil, Value::Nil]
        );
        assert_eq!(
            vm.eval(
                Arc::<[u8]>::from(&b"return written, own"[..]),
                ChunkOrigin::default()
            )?,
            vec![Value::Nil, Value::Nil]
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
        thread.set_call_context(context_in_group(7))?;

        assert_eq!(
            thread.resume(&[])?,
            ThreadStatus::Completed(vec![Value::Number(7.0), Value::Number(7.0)])
        );
        thread.clear_call_context()?;
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
        thread.set_call_context(context_in_group(3))?;
        thread.resume(&[])?;
        thread.clear_call_context()?;
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
            let values = nested.call_with_context(&[], context_in_group(5))?;
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
            function.call(&[Value::Number(20.0), Value::Number(22.0)])?,
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
            function.call(&[Value::TableData(table)])?,
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
    fn string_keys_may_hold_nul_bytes() -> Result<()> {
        let vm = Vm::new()?;
        vm.open_standard_libraries(StandardLibraries::all_supported())?;
        let table = vm.create_table()?;
        table.set("a\0b", Value::Integer(1))?;
        table.set("a", Value::Integer(2))?;
        assert_eq!(table.get("a\0b")?, Value::Integer(1));
        assert_eq!(table.get("a")?, Value::Integer(2));
        vm.set_global_table("g\0lobal", &table)?;

        let mut owned = OwnedTable::new();
        owned.set_field("x\0y", Value::Integer(3));
        owned.set_field("x", Value::Integer(4));

        let function = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(
                &br#"local owned = ...
                local table = _G["g\0lobal"]
                return table["a\0b"], table.a, owned["x\0y"], owned.x"#[..],
            ),
            ChunkOrigin::default(),
        ))?;
        assert_eq!(
            function.call(&[Value::TableData(owned)])?,
            vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
                Value::Integer(4)
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
            .call(&[Value::String(b"bad".to_vec())])
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
            utf8.call(&[Value::String(vec![0xff])]),
            Err(Error::Runtime(message))
                if message.contains("text") && message.contains("valid UTF-8")
        ));
        assert_eq!(
            bytes.call(&[Value::String(vec![0xff, b'a'])])?,
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
            function.call(&[
                Value::String(b"hello".to_vec()),
                Value::Buffer(vec![1, 2, 3, 4])
            ],)?,
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
            .call(&[Value::Number(1.5)])
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
                Box::new(|vm, _| vm.create_userdata(DropCounter(dropped.clone())).map(drop)),
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
                Box::new(|_, _| allocate.call(&[Value::Number(64.0)]).map(drop)),
            ),
            (
                "set_global_table",
                Box::new(|vm, i| vm.set_global_table(&format!("global{i}"), &table)),
            ),
            (
                "Table::set",
                Box::new(|_, i| table.set(&format!("key{i}"), Value::Boolean(true))),
            ),
            (
                "Table::set_integer",
                Box::new(|_, i| table.set_integer(i as i32 + 1, Value::Boolean(true))),
            ),
            (
                "Table::set_key",
                Box::new(|_, i| table.set_key(Value::Number(i as f64 + 0.5), Value::Integer(1))),
            ),
            (
                "Function::call with a string",
                Box::new(|_, i| {
                    function
                        .call(&[Value::String(format!("argument{i}").into_bytes())])
                        .map(drop)
                }),
            ),
            (
                "Function::call with table data",
                Box::new(|_, i| {
                    let mut data = OwnedTable::new();
                    data.set_field(format!("field{i}"), Value::Integer(1));
                    function.call(&[Value::TableData(data)]).map(drop)
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

        let values = probe.call(&[Value::Function(allocate), Value::Function(oversized)])?;
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

        let values = probe.call(&[Value::Function(failing)])?;
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

        let direct = allocate.call(&[]);
        assert!(matches!(direct, Err(Error::OutOfMemory)), "{direct:?}");
        let called = through_luau.call(&[Value::Function(allocate.clone())]);
        assert!(matches!(called, Err(Error::OutOfMemory)), "{called:?}");

        let thread = vm.create_thread(&through_luau)?;
        let resumed = thread.resume(&[Value::Function(allocate)]);
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
            vm.create_userdata(PanicsOnDrop)?;
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

        let failed = thread.resume(&[Value::Number(1.0), Value::Buffer(vec![0; 2 << 20])]);
        assert!(matches!(failed, Err(Error::OutOfMemory)), "{failed:?}");
        assert!(!thread.is_started());

        set_memory_limit(&vm, 0);
        assert_eq!(
            thread.resume(&[Value::Number(2.0), Value::Buffer(vec![0; 2 << 20])])?,
            ThreadStatus::Completed(vec![Value::Number(2.0), Value::String(b"buffer".to_vec())])
        );
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn a_thread_resuming_itself_fails_and_keeps_its_arguments() -> Result<()> {
        let vm = Vm::new()?;
        vm.open_standard_libraries(StandardLibraries::all_supported())?;
        let resume_self: NativeFn = Arc::new(|mut frame| {
            let first = frame.args.read_str()?;
            let refusal = frame
                .thread
                .resume(&[])
                .expect_err("a running thread resumed");
            assert!(refusal.to_string().contains("non-suspended"), "{refusal}");
            frame.vm.eval(
                Arc::<[u8]>::from(
                    &b"local t = {} for i = 1, 20000 do t[i % 100 + 1] = string.rep(tostring(i), 8) end"[..],
                ),
                ChunkOrigin::default(),
            )?;
            let first = first.to_string();
            let second: String = frame.args.read()?;
            frame.returns.write(first + &second)?;
            Ok(())
        });
        let resume_self = vm.create_function_with_options(
            NativeFunctionOptions::new(ChunkOrigin::default()).function_name("resume_self"),
            resume_self,
        )?;
        vm.set_global_function("resume_self", &resume_self)?;
        let body = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(
                &b"return resume_self(string.rep('a', 64) .. tostring(math.random()), 'b')"[..],
            ),
            ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&body)?;

        let ThreadStatus::Completed(values) = thread.resume(&[])? else {
            panic!("expected the thread to complete");
        };
        let [Value::String(joined)] = values.as_slice() else {
            panic!("expected one string, got {values:?}");
        };
        assert!(joined.starts_with(&[b'a'; 64]) && joined.ends_with(b"b"));
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn resuming_the_main_thread_fails_and_leaves_the_vm_working() -> Result<()> {
        let vm = Vm::new()?;
        vm.open_standard_libraries(StandardLibraries::all_supported())?;
        let grab: NativeFn = Arc::new(|frame| {
            frame.vm.data().insert(frame.thread.clone())?;
            Ok(())
        });
        let grab = vm.create_function_with_options(
            NativeFunctionOptions::new(ChunkOrigin::default()).function_name("grab"),
            grab,
        )?;
        grab.call(&[])?;
        let main = vm.data().get::<Thread>()?;

        for _ in 0..2 {
            let refusal = main.resume(&[]).expect_err("the main thread resumed");
            assert!(refusal.to_string().contains("main thread"), "{refusal}");
            let refusal = main
                .resume_error(&RaisedError::from("stop"))
                .expect_err("the main thread resumed");
            assert!(refusal.to_string().contains("main thread"), "{refusal}");
        }
        assert_eq!(
            eval_str(
                &vm,
                "local co = coroutine.create(function() coroutine.yield(1) end) return coroutine.resume(co)"
            )?,
            vec![Value::Boolean(true), Value::Number(1.0)]
        );
        assert_eq!(stack_top(&vm), 0);
        vm.data().clear()?;
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn a_thread_that_has_resumed_another_refuses_to_resume() -> Result<()> {
        let vm = Vm::new()?;
        vm.open_standard_libraries(StandardLibraries::all_supported())?;
        let resume: NativeFn = Arc::new(|mut frame| {
            let thread: Thread = frame.args.read()?;
            let refusal = thread
                .resume(&[Value::from("again")])
                .expect_err("a thread that has resumed another resumed");
            assert!(refusal.to_string().contains("non-suspended"), "{refusal}");
            let refusal = thread
                .resume_error(&RaisedError::from("stop"))
                .expect_err("a thread that has resumed another resumed");
            assert!(refusal.to_string().contains("non-suspended"), "{refusal}");
            Ok(())
        });
        let hold: NativeFn = Arc::new(|mut frame| {
            let inner: Thread = frame.args.read()?;
            let text = frame.args.read_str()?;
            inner.resume(&[])?;
            let text = text.to_string();
            let tail: String = frame.args.read()?;
            frame.returns.write(text + &tail)?;
            Ok(())
        });
        for (name, callback) in [("resume", resume), ("hold", hold)] {
            let function = vm.create_function_with_options(
                NativeFunctionOptions::new(ChunkOrigin::default()).function_name(name),
                callback,
            )?;
            vm.set_global_function(name, &function)?;
        }
        let body = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(
                &br#"
                    local outer = coroutine.running()
                    local inner = coroutine.create(function()
                        assert(coroutine.status(outer) == "normal")
                        resume(outer)
                    end)
                    return hold(inner, string.rep("a", 64), "b")
                "#[..],
            ),
            ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&body)?;

        assert_eq!(
            thread.resume(&[])?,
            ThreadStatus::Completed(vec![Value::String([[b'a'; 64].as_slice(), b"b"].concat())])
        );
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    const KINDED_FAILURE: &str = "module 'test/native' function 'fail' plugin 'demo': gone";

    fn kinded_failure(vm: &Vm) -> Result<Function> {
        vm.create_function_with_options(
            NativeFunctionOptions::new(ChunkOrigin {
                module: Some(ModuleId(Arc::from("test/native"))),
                plugin: Some(Arc::from("demo")),
                ..ChunkOrigin::default()
            })
            .function_name("fail"),
            Arc::new(|_| {
                Err(Error::Kinded {
                    kind: "missing",
                    message: "gone".to_string(),
                })
            }),
        )
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn kinded_errors_raise_host_errors_that_keep_their_kind() -> Result<()> {
        let vm = Vm::new()?;
        vm.open_standard_libraries(StandardLibraries::all_supported())?;
        let probe = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(
                &br#"
                    local fail = ...
                    local ok, err = pcall(fail)
                    local rethrown_ok, rethrown = pcall(function()
                        error(err, 0)
                    end)
                    local _, plain = pcall(error, "plain", 0)
                    return ok, err, typeof(err), tostring(err), `{err}`, rethrown_ok, rawequal(rethrown, err), plain, {}
                "#[..],
            ),
            ChunkOrigin::default(),
        ))?;

        let values = probe.call(&[Value::Function(kinded_failure(&vm)?)])?;
        let [
            Value::Boolean(false),
            error @ Value::UserData(_),
            Value::String(type_name),
            Value::String(tostring),
            Value::String(interpolated),
            Value::Boolean(false),
            Value::Boolean(true),
            plain,
            table,
        ] = values.as_slice()
        else {
            panic!("expected a host error, got {values:?}");
        };
        assert_eq!(type_name.as_slice(), b"userdata");
        assert_eq!(tostring.as_slice(), KINDED_FAILURE.as_bytes());
        assert_eq!(interpolated.as_slice(), KINDED_FAILURE.as_bytes());
        assert_eq!(vm.error_kind(error), Some("missing"));
        assert_eq!(vm.error_message(error).as_deref(), Some(KINDED_FAILURE));

        let other = vm.create_userdata(1_u8)?;
        for value in [plain, table, &Value::UserData(other), &Value::Nil] {
            assert_eq!(vm.error_kind(value), None, "{value:?}");
            assert_eq!(vm.error_message(value), None, "{value:?}");
        }
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn kinded_errors_reach_rust_callers_with_their_kind() -> Result<()> {
        let vm = Vm::new()?;
        vm.open_standard_libraries(StandardLibraries::all_supported())?;
        let fail = kinded_failure(&vm)?;
        let through_luau = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(&b"local fail = ... fail()"[..]),
            ChunkOrigin::default(),
        ))?;
        let kinded = |message: &str| Error::Kinded {
            kind: "missing",
            message: message.to_string(),
        };

        let direct = fail.call(&[]);
        assert_eq!(
            direct.map_err(|error| error.to_string()),
            Err(kinded(KINDED_FAILURE).to_string())
        );
        let called = through_luau.call(&[Value::Function(fail.clone())]);
        assert!(
            matches!(&called, Err(Error::Kinded { kind: "missing", message }) if message == KINDED_FAILURE),
            "{called:?}"
        );

        // A native function passing on the error of Luau it called raises it with the same kind.
        let relay = vm.create_function(
            ChunkOrigin::default(),
            Arc::new(|mut frame| {
                let through_luau: Function = frame.args.read()?;
                let fail: Value = frame.args.read()?;
                through_luau.call(&[fail])?;
                Ok(())
            }),
        )?;
        let probe = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(&b"local _, err = pcall(...) return err"[..]),
            ChunkOrigin::default(),
        ))?;
        let relayed = probe.call(&[
            Value::Function(relay),
            Value::Function(through_luau),
            Value::Function(fail),
        ])?;
        assert_eq!(vm.error_kind(&relayed[0]), Some("missing"));
        assert!(
            vm.error_message(&relayed[0])
                .is_some_and(|message| message.ends_with(KINDED_FAILURE))
        );
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn thread_resume_error_raises_kinded_errors_as_host_errors() -> Result<()> {
        let vm = Vm::new()?;
        vm.open_standard_libraries(StandardLibraries::all_supported())?;
        let caught = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(
                &br#"
                    local ok, err = pcall(coroutine.yield)
                    return ok, err, tostring(err)
                "#[..],
            ),
            ChunkOrigin::default(),
        ))?;
        let uncaught = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(&b"coroutine.yield()"[..]),
            ChunkOrigin::default(),
        ))?;
        let error = RaisedError {
            kind: Some("missing"),
            message: "gone".to_string(),
        };

        let thread = vm.create_thread(&caught)?;
        thread.resume(&[])?;
        let ThreadStatus::Completed(values) = thread.resume_error(&error)? else {
            panic!("expected the thread to finish");
        };
        assert_eq!(values[0], Value::Boolean(false));
        assert_eq!(vm.error_kind(&values[1]), Some("missing"));
        assert_eq!(values[2], Value::String(b"gone".to_vec()));

        let thread = vm.create_thread(&uncaught)?;
        thread.resume(&[])?;
        let failed = thread.resume_error(&error);
        assert!(
            matches!(&failed, Err(Error::Kinded { kind: "missing", message }) if message == "gone"),
            "{failed:?}"
        );
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn kinded_errors_fall_back_to_their_message_when_memory_runs_out() -> Result<()> {
        let vm = limited_vm(1 << 20)?;
        let fail = kinded_failure(&vm)?;
        let probe = vm.load_chunk(&Chunk::new(
            Arc::<[u8]>::from(
                &br#"
                    local fail = ...
                    for _ = 1, 1000000 do
                        local _, err = pcall(fail)
                        if type(err) == "string" then
                            return err
                        end
                    end
                    return nil
                "#[..],
            ),
            ChunkOrigin::default(),
        ))?;

        // Nothing is freed, so each raise holds on to its error until one can't be made.
        set_gc_running(&vm, false);
        let values = probe.call(&[Value::Function(fail)])?;
        set_gc_running(&vm, true);
        let [Value::String(message)] = values.as_slice() else {
            panic!("expected a string error, got {values:?}");
        };
        assert!(
            [KINDED_FAILURE.as_bytes(), gateway::MEMORY_ERROR.as_bytes()]
                .contains(&message.as_slice()),
            "{}",
            String::from_utf8_lossy(message)
        );
        assert_eq!(stack_top(&vm), 0);
        Ok(())
    }
}
