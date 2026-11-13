//! The Luau stack and registry as the handles use them.
//!
//! A [`Stack`] is one thread's stack from where it stood when the `Stack` was made, its base,
//! to which it is cut back when dropped. Its methods check the indexes they take against the
//! values on the stack, and the types of what they work on, so the handles call them as safe
//! code. They pop only what was pushed above their base, and stacks on one thread drop in the
//! reverse order they were made in, which dropping one out of turn panics for, so a value a stack
//! reads stays where it is while the stack is borrowed.

#![allow(unsafe_code)]

use std::{
    any::{
        TypeId,
        type_name,
    },
    cell::RefCell,
    collections::HashMap,
    ffi::{
        CStr,
        c_int,
        c_void,
    },
    fmt,
    marker::PhantomData,
    mem,
    ptr::{
        self,
        NonNull,
    },
    rc::Rc,
};

use harmony_luau_sys as sys;

use super::{
    CallSite,
    CallbackSlot,
    ChunkOrigin,
    Error,
    Function,
    HOST_ERROR_TAG,
    HostError,
    NativeFn,
    NativeFunctionOptions,
    OwnedTable,
    RaisedError,
    Result,
    Table,
    Thread,
    UserData,
    UserDataTag,
    Value,
    ValueRef,
    Vm,
    gateway::{
        self,
        CoStatus,
    },
    native::{
        drop_callback_slot,
        drop_userdata,
        host_error_tostring,
        native_callback,
    },
};

/// `len` as the size of a new table, which fails as Luau does for a table too large for it.
fn table_size(len: usize) -> Result<c_int> {
    c_int::try_from(len).map_err(|_| Error::Runtime("table overflow".to_string()))
}

/// Whether Luau aligns a userdata's data for `T`: to 8 bytes, and to 16 for 16 bytes or more
/// (`Udata`).
const fn fits_userdata<T>() -> bool {
    mem::align_of::<T>() <= 8 || (mem::align_of::<T>() <= 16 && mem::size_of::<T>() >= 16)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct StackIndex(c_int);

impl StackIndex {
    pub(super) const TOP: Self = Self(-1);

    pub(super) fn absolute(index: c_int) -> Self {
        debug_assert!(index > 0, "absolute Luau stack indexes must be positive");
        Self(index)
    }

    pub(super) fn relative(index: c_int) -> Self {
        debug_assert!(index < 0, "relative Luau stack indexes must be negative");
        Self(index)
    }

    pub(super) fn argument(index: usize) -> Self {
        let index = c_int::try_from(index)
            .ok()
            .and_then(|index| index.checked_add(1))
            .expect("Luau argument index must fit in c_int");
        Self::absolute(index)
    }
}

/// One thread's stack above its base. See the module documentation.
pub(super) struct Stack<'a> {
    vm: &'a Vm,
    /// A thread of `vm`, alive for as long as the stack is.
    state: NonNull<sys::lua_State>,
    base: c_int,
    /// How many stacks on the thread are live with this one, which is the innermost while they
    /// are this many.
    depth: usize,
}

impl<'a> Stack<'a> {
    /// The stack of the VM's main thread.
    pub(super) fn main(vm: &'a Vm) -> Self {
        vm.drop_collected();
        // SAFETY: the main thread lives as long as the VM, which `vm` keeps alive.
        unsafe { Self::from_raw(vm, vm.inner.state.main()) }
    }

    /// The stack of `thread`, whose registry reference keeps it alive.
    pub(super) fn of_thread(thread: &'a Thread) -> Self {
        let thread_ref = &thread.inner.thread;
        thread_ref.vm().drop_collected();
        // SAFETY: the thread's registry reference pins it, and `thread` is borrowed for as long
        // as the stack lives.
        unsafe { Self::from_raw(thread_ref.vm(), thread_ref.state) }
    }

    /// # Safety
    ///
    /// `state` is a thread of `vm` that stays alive for as long as the stack does.
    pub(super) unsafe fn from_raw(vm: &'a Vm, state: NonNull<sys::lua_State>) -> Self {
        // SAFETY: `state` is live, as the caller guarantees.
        let base = unsafe { sys::lua_gettop(state.as_ptr()) };
        let depth = vm.inner.stacks.enter(state);
        Self {
            vm,
            state,
            base,
            depth,
        }
    }

    pub(super) fn vm(&self) -> &'a Vm {
        self.vm
    }

    pub(super) fn base(&self) -> c_int {
        self.base
    }

    pub(super) fn top(&self) -> c_int {
        // SAFETY: a live state.
        unsafe { sys::lua_gettop(self.raw()) }
    }

    fn raw(&self) -> *mut sys::lua_State {
        self.state.as_ptr()
    }

    fn is_main(&self) -> bool {
        self.state == self.vm.inner.state.main()
    }

    /// `index` as Luau takes it, after checking that it names a value on the stack.
    fn slot(&self, index: StackIndex) -> c_int {
        let top = self.top();
        let raw = index.0;
        assert!(
            (1..=top).contains(&raw) || (-top..=-1).contains(&raw),
            "Luau stack index {raw} is outside the {top} values on the stack"
        );
        raw
    }

    fn type_of(&self, raw: c_int) -> c_int {
        // SAFETY: a live state, and `raw` comes from `slot`.
        unsafe { sys::lua_type(self.raw(), raw) }
    }

    /// `index`, after checking that it names a table.
    fn table_slot(&self, index: StackIndex) -> c_int {
        let raw = self.slot(index);
        assert_eq!(
            self.type_of(raw),
            sys::LUA_TTABLE,
            "Luau stack index {raw} holds no table"
        );
        raw
    }

    /// Checks that the top `count` values were pushed above the base.
    fn own(&self, count: c_int) {
        assert!(
            count >= 0 && self.top() - count >= self.base,
            "{count} values aren't on the stack above its base"
        );
    }

    pub(super) fn pop(&mut self, count: c_int) {
        self.own(count);
        // SAFETY: a live state; the values were pushed above the base.
        unsafe { gateway::truncate(self.raw(), self.top() - count) };
    }

    /// Leaves what was pushed on the stack, for Luau to take from a native function.
    pub(super) fn keep(self) {
        self.vm.inner.stacks.leave(self.state, self.depth);
        mem::forget(self);
    }

    pub(super) fn type_at(&self, index: StackIndex) -> c_int {
        self.type_of(self.slot(index))
    }

    pub(super) fn push_nil(&mut self) -> Result<()> {
        // SAFETY: a live state.
        unsafe { gateway::push_nil(self.raw()) }
    }

    pub(super) fn push_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        // SAFETY: a live state.
        unsafe { gateway::push_bytes(self.raw(), bytes) }
    }

    pub(super) fn push_ref(&mut self, reference: &RegistryRef) -> Result<()> {
        let actual_vm = self.vm.id();
        if reference.vm().id() != actual_vm {
            return Err(Error::VmMismatch {
                reference_vm: reference.vm().id(),
                actual_vm,
            });
        }
        // SAFETY: a live state, and the reference belongs to its VM.
        unsafe { gateway::get_ref(self.raw(), reference.raw()) }
    }

    /// The shared globals, which a chunk's environment reads through to.
    pub(super) fn push_globals(&mut self) -> Result<()> {
        // SAFETY: a live state, and the globals pseudo-index always names a table.
        unsafe { gateway::push_value(self.raw(), sys::LUA_GLOBALSINDEX) }
    }

    pub(super) fn push_value(&mut self, value: &Value) -> Result<()> {
        let state = self.raw();
        match value {
            Value::TableData(value) => self.push_owned_table(value),
            Value::NativeFunction(value) => {
                self.push_native_function(&value.options, value.callback.clone())
            }
            Value::Table(value) => self.push_ref(&value.reference),
            Value::Function(value) => self.push_ref(&value.reference),
            Value::Thread(value) => self.push_ref(&value.inner.thread.reference),
            Value::UserData(value) => self.push_ref(&value.reference),
            Value::String(value) => self.push_bytes(value),
            Value::Buffer(value) => self.new_buffer(value),
            Value::Nil => self.push_nil(),
            // SAFETY: a live state.
            Value::Boolean(value) => unsafe { gateway::push_boolean(state, *value) },
            // SAFETY: a live state.
            Value::Integer(value) => unsafe { gateway::push_integer(state, *value) },
            // SAFETY: a live state.
            Value::Number(value) => unsafe { gateway::push_number(state, *value) },
        }
    }

    fn push_owned_table(&mut self, table: &OwnedTable) -> Result<()> {
        self.create_table(
            table_size(table.array.len())?,
            table_size(table.fields.len() + table.entries.len())?,
        )?;
        for (value, key) in table.array.iter().zip(1..) {
            self.push_value(value)?;
            self.raw_set_index(StackIndex::relative(-2), key)?;
        }
        for (key, value) in &table.fields {
            self.push_value(value)?;
            self.raw_set_field(StackIndex::relative(-2), key.as_bytes())?;
        }
        for (key, value) in &table.entries {
            self.push_value(key)?;
            self.push_value(value)?;
            self.raw_set(StackIndex::relative(-3))?;
        }
        Ok(())
    }

    pub(super) fn push_native_function(
        &mut self,
        options: &NativeFunctionOptions,
        callback: NativeFn,
    ) -> Result<()> {
        const { assert!(fits_userdata::<CallbackSlot>()) };
        // SAFETY: a live state. `drop_callback_slot` drops the `CallbackSlot` written below.
        let slot_data = unsafe {
            gateway::new_userdata_dtor(
                self.raw(),
                mem::size_of::<CallbackSlot>(),
                Some(drop_callback_slot),
            )?
        };
        let slot = CallbackSlot {
            vm: Rc::downgrade(&self.vm.inner),
            callback,
            site: CallSite {
                origin: options.origin.clone(),
                function_name: options.function_name.clone(),
            },
            capability: options.capability.clone(),
            task_group: options.task_group,
            argument_names: options.argument_names.clone(),
            use_thread_context_origin: options.use_thread_context_origin,
        };
        // SAFETY: the userdata's `size_of::<CallbackSlot>()` bytes, aligned for it, which nothing
        // reads before this.
        unsafe { ptr::write(slot_data.cast::<CallbackSlot>(), slot) };
        // SAFETY: a live state, with the slot on top as the closure's upvalue.
        unsafe {
            gateway::push_cclosure(
                self.raw(),
                Some(native_callback),
                c"harmony_native_callback",
                1,
            )
        }
    }

    pub(super) fn create_table(&mut self, array: c_int, records: c_int) -> Result<()> {
        // SAFETY: a live state.
        unsafe { gateway::create_table(self.raw(), array, records) }
    }

    pub(super) fn new_buffer(&mut self, bytes: &[u8]) -> Result<()> {
        // SAFETY: a live state.
        unsafe { gateway::new_buffer(self.raw(), bytes) }
    }

    /// Pushes a userdata holding `value`, tagged for `T`, and returns its tag.
    pub(super) fn new_userdata<T>(&mut self, value: T) -> Result<UserDataTag>
    where
        T: Send + 'static,
    {
        const { assert!(fits_userdata::<T>()) };
        let tag = self.vm.assign_userdata_tag::<T>()?;
        // SAFETY: a live state. The tag's destructor drops the `T` written below.
        let data = unsafe { gateway::new_userdata_tagged(self.raw(), mem::size_of::<T>(), tag.0)? };
        // SAFETY: the userdata's `size_of::<T>()` bytes, which Luau aligns for `T` and nothing reads
        // before this.
        unsafe { ptr::write(data.cast::<T>(), value) };
        Ok(tag)
    }

    /// Pushes `error` as the value to raise: a `HostError` when it has a kind, else its message.
    /// A `HostError` that can't be made falls back to the message, so exactly one value is pushed.
    pub(super) fn push_raised(&mut self, error: &RaisedError) {
        const { assert!(fits_userdata::<HostError>()) };
        if let Some(kind) = error.kind {
            // SAFETY: a live state.
            let pushed = unsafe {
                gateway::new_userdata_tagged_with_metatable(
                    self.raw(),
                    mem::size_of::<HostError>(),
                    HOST_ERROR_TAG.0,
                )
            };
            if let Ok(data) = pushed {
                let error = HostError {
                    kind,
                    message: error.message.clone(),
                };
                // SAFETY: the userdata holds `size_of::<HostError>()` bytes, which Luau aligns for
                // it, and its tag's destructor drops a `HostError`.
                unsafe { ptr::write(data.cast::<HostError>(), error) };
                return;
            }
        }
        // SAFETY: a live state.
        unsafe { gateway::push_error(self.raw(), &error.message) };
    }

    /// Pushes `HostError.__tostring`.
    fn push_host_error_tostring(&mut self) -> Result<()> {
        // SAFETY: a live state, and Luau calls the function only with what it is given.
        unsafe { gateway::push_cclosure(self.raw(), Some(host_error_tostring), c"__tostring", 0) }
    }

    /// Makes the table on top, which it pops, the metatable of new userdata with `tag`.
    fn set_userdata_metatable(&mut self, tag: UserDataTag) {
        self.own(1);
        assert_eq!(self.type_of(-1), sys::LUA_TTABLE, "a metatable is a table");
        // SAFETY: a live state, with a table on top.
        unsafe { sys::lua_setuserdatametatable(self.raw(), tag.0) };
    }

    /// Pushes a new thread and returns its state, which lives while it is on the stack.
    fn push_new_thread(&mut self) -> Result<NonNull<sys::lua_State>> {
        // SAFETY: a live state.
        let thread = unsafe { gateway::new_thread(self.raw())? };
        NonNull::new(thread).ok_or(Error::VmCreate)
    }

    /// A new thread that is to call `function`.
    pub(super) fn new_thread(&mut self, function: &RegistryRef) -> Result<ThreadRef> {
        let thread = self.push_new_thread()?;
        let thread_ref = self.thread_ref(StackIndex::TOP)?;
        self.push_ref(function)?;
        // SAFETY: a live state, and `thread` is a thread of the same VM, which the stack holds.
        unsafe { gateway::xmove(self.raw(), thread.as_ptr(), 1)? };
        self.pop(1);
        Ok(thread_ref)
    }

    pub(super) fn load(&mut self, chunk_name: &CStr, bytecode: &[u8]) -> Result<()> {
        // SAFETY: a live state.
        unsafe { gateway::load(self.raw(), chunk_name, bytecode) }
    }

    /// Loads a chunk whose environment holds `globals`, and reads and writes every other global
    /// in the table `scope`.
    pub(super) fn load_with_globals(
        &mut self,
        chunk_name: &CStr,
        bytecode: &[u8],
        globals: &[(&str, &Value)],
        scope: &RegistryRef,
    ) -> Result<()> {
        // Luau gives a loaded chunk the environment of the state loading it.
        let loader = self.push_new_thread()?;
        // SAFETY: the loader is on this stack, which keeps it alive, until the loader's stack is
        // dropped at the end of this function.
        let mut loader = unsafe { Stack::from_raw(self.vm, loader) };
        loader.create_table(0, table_size(globals.len())?)?;
        for (name, value) in globals {
            loader.push_value(value)?;
            loader.raw_set_field(StackIndex::relative(-2), name.as_bytes())?;
        }
        loader.create_table(0, 2)?;
        for field in [b"__index".as_slice(), b"__newindex"] {
            loader.push_ref(scope)?;
            loader.raw_set_field(StackIndex::relative(-2), field)?;
        }
        loader.set_readonly(StackIndex::TOP, true)?;
        loader.set_metatable(StackIndex::relative(-2))?;
        loader.own(1);
        // SAFETY: a live state, with the environment table on top.
        unsafe { sys::lua_replace(loader.raw(), sys::LUA_GLOBALSINDEX) };
        loader.load(chunk_name, bytecode)?;
        loader.own(1);
        // SAFETY: both are live threads of one VM, and the chunk is on top of the loader.
        unsafe { gateway::xmove(loader.raw(), self.raw(), 1) }
    }

    /// Sets the global `name` to the value on top, which it pops.
    pub(super) fn set_global(&mut self, name: &[u8]) -> Result<()> {
        self.own(1);
        // SAFETY: a live state, with a value on top.
        unsafe { gateway::set_global(self.raw(), name) }
    }

    /// Sets `key` in the table at `table` to the value on top, which it pops.
    pub(super) fn raw_set_field(&mut self, table: StackIndex, key: &[u8]) -> Result<()> {
        let table = self.table_slot(table);
        self.own(1);
        // SAFETY: a live state, a table at `table`, and a value on top.
        unsafe { gateway::raw_set_field(self.raw(), table, key) }
    }

    /// Sets `key` in the table at `table` to the value on top, which it pops.
    pub(super) fn raw_set_index(&mut self, table: StackIndex, key: c_int) -> Result<()> {
        let table = self.table_slot(table);
        self.own(1);
        // SAFETY: a live state, a table at `table`, and a value on top.
        unsafe { gateway::raw_set_index(self.raw(), table, key) }
    }

    /// Sets the key below the top in the table at `table` to the value on top, and pops both.
    pub(super) fn raw_set(&mut self, table: StackIndex) -> Result<()> {
        let table = self.table_slot(table);
        self.own(2);
        // SAFETY: a live state, a table at `table`, and a key and a value on top.
        unsafe { gateway::raw_set(self.raw(), table) }
    }

    /// Pushes the value of `key` in the table at `table`.
    pub(super) fn raw_get_field(&mut self, table: StackIndex, key: &[u8]) -> Result<()> {
        let table = self.table_slot(table);
        // SAFETY: a live state, and a table at `table`.
        unsafe { gateway::raw_get_field(self.raw(), table, key) }
    }

    /// Pushes the value of `key` in the table at `table`.
    pub(super) fn raw_get_index(&mut self, table: StackIndex, key: c_int) -> Result<()> {
        let table = self.table_slot(table);
        // SAFETY: a live state, and a table at `table`.
        unsafe { gateway::raw_get_index(self.raw(), table, key) }
    }

    /// Replaces the key on top with the next key and value of the table at `table`, or pops the
    /// key and returns false once the table is exhausted.
    pub(super) fn next(&mut self, table: StackIndex) -> Result<bool> {
        let table = self.table_slot(table);
        self.own(1);
        // SAFETY: a live state, a table at `table`, and a key on top.
        unsafe { gateway::next(self.raw(), table) }
    }

    /// Sets the metatable of the table or userdata at `index` to the table or nil on top, which
    /// it pops.
    pub(super) fn set_metatable(&mut self, index: StackIndex) -> Result<()> {
        let index = self.slot(index);
        assert!(
            matches!(self.type_of(index), sys::LUA_TTABLE | sys::LUA_TUSERDATA),
            "only tables and userdata take a metatable here"
        );
        self.own(1);
        assert!(
            matches!(self.type_of(-1), sys::LUA_TTABLE | sys::LUA_TNIL),
            "a metatable is a table or nil"
        );
        // SAFETY: a live state, a table or userdata at `index`, and a table or nil on top.
        unsafe { gateway::set_metatable(self.raw(), index) }
    }

    /// Pushes the metatable of the value at `index` and returns whether it has one; pushes
    /// nothing when it does not.
    pub(super) fn get_metatable(&mut self, index: StackIndex) -> Result<bool> {
        let index = self.slot(index);
        // SAFETY: a live state, and a value at `index`.
        unsafe { gateway::get_metatable(self.raw(), index) }
    }

    pub(super) fn set_readonly(&mut self, table: StackIndex, readonly: bool) -> Result<()> {
        let table = self.table_slot(table);
        // SAFETY: a live state, and a table at `table`.
        unsafe { gateway::set_readonly(self.raw(), table, readonly) }
    }

    /// The length of the value at `index`, as the `#` operator finds it without `__len`.
    pub(super) fn raw_len(&self, index: StackIndex) -> usize {
        let index = self.slot(index);
        // SAFETY: a live state, and a value at `index`.
        let len = unsafe { sys::lua_objlen(self.raw(), index) };
        len.max(0) as usize
    }

    /// The address of the object at `index`, which identifies it while it lives.
    pub(super) fn to_pointer(&self, index: StackIndex) -> usize {
        let index = self.slot(index);
        // SAFETY: a live state, and a value at `index`.
        unsafe { gateway::to_pointer(self.raw(), index) }
    }

    pub(super) fn raw_equal(&self, lhs: StackIndex, rhs: StackIndex) -> bool {
        let (lhs, rhs) = (self.slot(lhs), self.slot(rhs));
        // SAFETY: a live state, and values at `lhs` and `rhs`.
        unsafe { sys::lua_rawequal(self.raw(), lhs, rhs) != 0 }
    }

    /// Calls the function below the top `args` values, leaving all its results.
    pub(super) fn pcall(&mut self, args: c_int) -> Result<()> {
        self.own(args + 1);
        // SAFETY: a live state, with the function and its arguments above the base.
        unsafe { gateway::pcall(self.raw(), args) }
    }

    pub(super) fn read_bool(&self, index: StackIndex) -> bool {
        let index = self.slot(index);
        // SAFETY: a live state, and a value at `index`.
        unsafe { sys::lua_toboolean(self.raw(), index) != 0 }
    }

    pub(super) fn read_integer(&self, index: StackIndex) -> i64 {
        let index = self.slot(index);
        let mut is_integer = 0;
        // SAFETY: a live state, and a value at `index`.
        unsafe { sys::lua_tointeger64(self.raw(), index, &mut is_integer) }
    }

    pub(super) fn read_number(&self, index: StackIndex) -> f64 {
        let index = self.slot(index);
        let mut is_number = 0;
        // SAFETY: a live state, and a value at `index`.
        unsafe { sys::lua_tonumberx(self.raw(), index, &mut is_number) }
    }

    /// The bytes of the string at `index`, or `None` for any other value.
    pub(super) fn string_bytes(&self, index: StackIndex) -> Option<&[u8]> {
        let index = self.slot(index);
        // SAFETY: a live state, and a value at `index`. The string stays where it is, and so
        // alive, while `self` is borrowed (see the module documentation).
        unsafe { gateway::string_bytes(self.raw(), index) }
    }

    /// A copy of the bytes of the buffer at `index`, or `None` for any other value. Luau code can
    /// write to a buffer, so they aren't lent out.
    pub(super) fn buffer_bytes(&self, index: StackIndex) -> Option<Vec<u8>> {
        let index = self.slot(index);
        let mut len = 0usize;
        // SAFETY: a live state, and a value at `index`.
        let data = unsafe { sys::lua_tobuffer(self.raw(), index, &mut len) };
        if data.is_null() {
            return None;
        }
        // SAFETY: a buffer of `len` bytes at `data`, which no Luau code runs to write to or free
        // before the copy returns.
        Some(unsafe { std::slice::from_raw_parts(data.cast(), len) }.to_vec())
    }

    /// The data of the userdata at `index` if it has `tag`.
    pub(super) fn userdata_data(
        &self,
        index: StackIndex,
        tag: UserDataTag,
    ) -> Option<NonNull<c_void>> {
        let index = self.slot(index);
        // SAFETY: a live state, and a value at `index`.
        NonNull::new(unsafe { sys::lua_touserdatatagged(self.raw(), index, tag.0) })
    }

    pub(super) fn read_value(&self, index: StackIndex) -> Result<Value> {
        self.read_value_ref(index).map(ValueRef::into_value)
    }

    /// Reads the value at `index`, borrowing a string's bytes from the VM.
    pub(super) fn read_value_ref(&self, index: StackIndex) -> Result<ValueRef<'_>> {
        let raw = self.slot(index);
        Ok(match self.type_of(raw) {
            sys::LUA_TNIL | sys::LUA_TNONE => ValueRef::Nil,
            sys::LUA_TBOOLEAN => ValueRef::Boolean(self.read_bool(index)),
            sys::LUA_TINTEGER => ValueRef::Integer(self.read_integer(index)),
            sys::LUA_TNUMBER => ValueRef::Number(self.read_number(index)),
            sys::LUA_TSTRING => {
                ValueRef::String(self.string_bytes(index).expect("string value has bytes"))
            }
            sys::LUA_TBUFFER => {
                ValueRef::Buffer(self.buffer_bytes(index).expect("buffer value has bytes"))
            }
            sys::LUA_TTABLE => ValueRef::Table(Table {
                reference: self.reference(index)?,
            }),
            sys::LUA_TFUNCTION => ValueRef::Function(Function {
                reference: self.reference(index)?,
                origin: ChunkOrigin::default(),
            }),
            sys::LUA_TTHREAD => {
                ValueRef::Thread(Thread::new(self.thread_ref(index)?, ChunkOrigin::default()))
            }
            sys::LUA_TUSERDATA => ValueRef::UserData(UserData {
                reference: self.reference(index)?,
                // SAFETY: a live state, and a userdata at `raw`.
                tag: UserDataTag(unsafe { sys::lua_userdatatag(self.raw(), raw) }),
            }),
            _ => ValueRef::Nil,
        })
    }

    /// A reference that pins the value at `index` in the registry.
    pub(super) fn reference(&self, index: StackIndex) -> Result<RegistryRef> {
        let index = self.slot(index);
        // SAFETY: a live state, and a value at `index`.
        let reference = unsafe { gateway::reference(self.raw(), index)? };
        Ok(RegistryRef::new(self.vm.clone(), reference))
    }

    /// The thread at `index`, pinned in the registry.
    fn thread_ref(&self, index: StackIndex) -> Result<ThreadRef> {
        let raw = self.slot(index);
        // SAFETY: a live state, and a value at `raw`.
        let state = unsafe { sys::lua_tothread(self.raw(), raw) };
        let state =
            NonNull::new(state).unwrap_or_else(|| panic!("Luau stack index {raw} holds no thread"));
        Ok(ThreadRef {
            reference: self.reference(index)?,
            state,
        })
    }

    /// The thread this stack belongs to, pinned in the registry.
    pub(super) fn own_thread(&mut self) -> Result<ThreadRef> {
        // SAFETY: a live state.
        unsafe { gateway::push_thread(self.raw())? };
        let thread = self.thread_ref(StackIndex::TOP);
        self.pop(1);
        thread
    }

    pub(super) fn is_yieldable(&self) -> bool {
        // SAFETY: a live state.
        unsafe { sys::lua_isyieldable(self.raw()) != 0 }
    }

    /// The thread's status, as `lua_status` gives it.
    pub(super) fn status(&self) -> c_int {
        // SAFETY: a live state.
        unsafe { sys::lua_status(self.raw()) }
    }

    /// The thread's state as `coroutine.status` reports it from the main thread.
    pub(super) fn co_status(&self) -> CoStatus {
        // SAFETY: both are live threads of one VM.
        unsafe { gateway::co_status(self.vm.inner.state.main().as_ptr(), self.raw()) }
    }

    /// Whether the thread is a coroutine that has neither run nor been closed.
    pub(super) fn is_unstarted(&self) -> bool {
        self.co_status() == CoStatus::Suspended && self.status() == sys::LUA_OK
    }

    /// Whether Luau would resume the thread: it is a suspended coroutine, one that yielded or
    /// hasn't started.
    pub(super) fn check_resumable(&self) -> Result<()> {
        if self.is_main() {
            return Err(Error::Runtime("cannot resume the main thread".to_string()));
        }
        match self.co_status() {
            CoStatus::Suspended => Ok(()),
            CoStatus::Dead => Err(Error::Runtime("cannot resume dead coroutine".to_string())),
            CoStatus::Running | CoStatus::Normal => Err(Error::Runtime(
                "cannot resume non-suspended coroutine".to_string(),
            )),
        }
    }

    /// Empties the stack of a thread that isn't live, below its base too, which becomes the
    /// bottom of the stack.
    pub(super) fn clear(&mut self) {
        assert!(
            !self.is_main() && !self.co_status().is_live(),
            "only a thread that isn't live can be emptied"
        );
        // SAFETY: a live state, whose values nothing else uses while it isn't running.
        unsafe { gateway::truncate(self.raw(), 0) };
        self.base = 0;
    }

    /// Resumes the thread with the top `args` values, and returns whether it yielded rather than
    /// finished.
    pub(super) fn resume(&mut self, args: c_int) -> Result<bool> {
        self.check_resumable()?;
        self.own(args);
        // SAFETY: a suspended coroutine of the VM, with `args` values on top.
        unsafe { gateway::resume(self.vm.inner.state.main().as_ptr(), self.raw(), args) }
    }

    /// Resumes the thread by raising `error` where it yielded, and returns whether it yielded
    /// again rather than finished.
    pub(super) fn resume_error(&mut self, error: &RaisedError) -> Result<bool> {
        self.check_resumable()?;
        // Failing to make the error leaves the out-of-memory error in its place, which is raised
        // instead.
        self.push_raised(error);
        // SAFETY: a suspended coroutine of the VM, with the error on top.
        unsafe { gateway::resume_error(self.vm.inner.state.main().as_ptr(), self.raw()) }
    }

    /// Closes a thread that isn't live, outside the memory limit, and returns whether it is
    /// dead.
    pub(super) fn close(&mut self) -> bool {
        let main = self.vm.inner.state.main().as_ptr();
        let state = self.raw();
        self.vm.inner.control().without_memory_limit(|| {
            // SAFETY: both are live threads of one VM.
            unsafe { gateway::close_thread(main, state) }
        })
    }

    /// Marks the thread to be closed where it next yields.
    pub(super) fn cancel(&mut self) {
        // SAFETY: a live state.
        unsafe { gateway::cancel_thread(self.raw()) }
    }

    pub(super) fn is_cancelled(&self) -> bool {
        // SAFETY: a live state.
        unsafe { gateway::is_cancelled(self.raw()) }
    }

    /// Gives the thread a global table of its own that reads through to the frozen globals.
    pub(super) fn sandbox_thread(&mut self) -> Result<()> {
        // SAFETY: a live state.
        unsafe { gateway::sandbox_thread(self.raw()) }
    }

    /// Freezes the globals and every table in them.
    pub(super) fn sandbox_globals(&mut self) -> Result<()> {
        // SAFETY: a live state.
        unsafe { gateway::sandbox(self.raw()) }
    }

    pub(super) fn open_library(&mut self, name: &CStr) -> Result<()> {
        // SAFETY: a live state.
        unsafe { gateway::open_library(self.raw(), name) }
    }
}

impl Drop for Stack<'_> {
    /// Cuts the stack back to its base, unless closing its thread emptied it. Once no stack of the
    /// VM is left, as when a call into it returns, drops what Luau collected meanwhile.
    fn drop(&mut self) {
        let stacks = &self.vm.inner.stacks;
        stacks.leave(self.state, self.depth);
        if self.top() > self.base {
            // SAFETY: a live state, with values above the base.
            unsafe { gateway::truncate(self.raw(), self.base) };
        }
        if stacks.is_empty() && !std::thread::panicking() {
            self.vm.drop_collected();
        }
    }
}

/// The live [`Stack`]s of a VM: how many each thread has.
#[derive(Default)]
pub(super) struct LiveStacks {
    depths: RefCell<Vec<(NonNull<sys::lua_State>, usize)>>,
}

impl LiveStacks {
    /// Counts a new stack on `state`, and returns how many it has with it.
    fn enter(&self, state: NonNull<sys::lua_State>) -> usize {
        let mut depths = self.depths.borrow_mut();
        if let Some((_, depth)) = depths.iter_mut().find(|(live, _)| *live == state) {
            *depth += 1;
            return *depth;
        }
        depths.push((state, 1));
        1
    }

    /// Stops counting the stack on `state` that made it `depth` deep. Panics unless it is the
    /// innermost: cutting the thread back to its base would pop what a later stack reads.
    fn leave(&self, state: NonNull<sys::lua_State>, depth: usize) {
        let mut depths = self.depths.borrow_mut();
        let position = depths.iter().position(|(live, _)| *live == state);
        let innermost = position.filter(|position| depths[*position].1 == depth);
        let Some(position) = innermost else {
            drop(depths);
            panic!("a Luau stack dropped before one made on its thread after it");
        };
        if depth == 1 {
            depths.swap_remove(position);
        } else {
            depths[position].1 -= 1;
        }
    }

    fn is_empty(&self) -> bool {
        self.depths.borrow().is_empty()
    }
}

/// The userdata tag of each Rust type a VM has made userdata of.
#[derive(Default)]
pub(super) struct UserDataTags {
    by_type: HashMap<TypeId, UserDataTag>,
    /// The next tag to assign; [`HOST_ERROR_TAG`] is the last one.
    next: c_int,
}

impl Vm {
    /// The tag of `T`'s userdata, or `None` before the VM has made any.
    pub(super) fn userdata_tag<T>(&self) -> Option<UserDataTag>
    where
        T: 'static,
    {
        self.inner
            .userdata_tags
            .borrow()
            .by_type
            .get(&TypeId::of::<T>())
            .copied()
    }

    /// The tag of `T`'s userdata, assigned on first use.
    fn assign_userdata_tag<T>(&self) -> Result<UserDataTag>
    where
        T: 'static,
    {
        if let Some(tag) = self.userdata_tag::<T>() {
            return Ok(tag);
        }
        let mut tags = self.inner.userdata_tags.borrow_mut();
        let tag = UserDataTag(tags.next);
        if tag == HOST_ERROR_TAG {
            return Err(Error::UserDataTagExhausted);
        }
        // SAFETY: a live state, and only userdata holding a `T` get the new tag.
        unsafe {
            sys::lua_setuserdatadtor(
                self.inner.state.main().as_ptr(),
                tag.0,
                Some(drop_userdata::<T>),
            );
        }
        tags.next += 1;
        tags.by_type.insert(TypeId::of::<T>(), tag);
        Ok(tag)
    }

    /// Gives `HostError` values their tag and metatable. Luau holds the metatable for the tag, so
    /// it lives as long as the state and holds no reference back to the [`Vm`].
    pub(super) fn install_host_errors(&self) -> Result<()> {
        self.inner
            .userdata_tags
            .borrow_mut()
            .by_type
            .insert(TypeId::of::<HostError>(), HOST_ERROR_TAG);
        // SAFETY: a live state, and only `HostError` values get their tag.
        unsafe {
            sys::lua_setuserdatadtor(
                self.inner.state.main().as_ptr(),
                HOST_ERROR_TAG.0,
                Some(drop_userdata::<HostError>),
            )
        };
        let mut stack = Stack::main(self);
        stack.create_table(0, 1)?;
        stack.push_host_error_tostring()?;
        stack.raw_set_field(StackIndex::relative(-2), b"__tostring")?;
        stack.set_readonly(StackIndex::TOP, true)?;
        stack.set_userdata_metatable(HOST_ERROR_TAG);
        Ok(())
    }
}

/// The `HostError` at `index`, or `None` for any other value. It lives as long as the value does.
///
/// # Safety
///
/// `state` is a live thread and `index` names a value on its stack. This serves the code that has
/// no [`Stack`]: reading a raised error, and `HostError.__tostring`.
pub(super) unsafe fn host_error_at<'a>(
    state: *mut sys::lua_State,
    index: c_int,
) -> Option<&'a HostError> {
    // SAFETY: a live state and a value at `index`, as the caller guarantees.
    let data = unsafe { sys::lua_touserdatatagged(state, index, HOST_ERROR_TAG.0) };
    // SAFETY: null, or the data of a userdata with the tag, which holds a `HostError`.
    unsafe { data.cast::<HostError>().as_ref() }
}

/// The `CallbackSlot` of the native function running on `state`, which has no [`Stack`] before
/// the slot gives it its VM.
///
/// # Safety
///
/// `state` is a live thread running a closure [`Stack::push_native_function`] made.
pub(super) unsafe fn callback_slot<'a>(state: NonNull<sys::lua_State>) -> Option<&'a CallbackSlot> {
    // SAFETY: a running closure's upvalue, as the caller guarantees.
    let data = unsafe {
        sys::lua_touserdatatagged(
            state.as_ptr(),
            sys::lua_upvalueindex(1),
            sys::LUA_UTAG_LIMIT,
        )
    };
    // SAFETY: null, or the data of a userdata with a destructor of its own, which Luau tags
    // `LUA_UTAG_LIMIT` and only `CallbackSlot`s are; the slot lives while the closure runs.
    unsafe { data.cast::<CallbackSlot>().as_ref() }
}

/// A shared borrow of the `T` a userdata holds.
pub struct UserDataRef<'a, T> {
    ptr: NonNull<T>,
    _owner: PhantomData<&'a UserData>,
}

impl<'a, T> UserDataRef<'a, T>
where
    T: 'static,
{
    pub(super) fn new(userdata: &'a UserData) -> Result<Self> {
        let mismatch = || Error::UserDataTypeMismatch {
            expected: type_name::<T>(),
        };
        let vm = userdata.reference.vm();
        let tag = vm
            .userdata_tag::<T>()
            .filter(|tag| *tag == userdata.tag)
            .ok_or_else(mismatch)?;
        let mut stack = Stack::main(vm);
        stack.push_ref(&userdata.reference)?;
        let ptr = stack
            .userdata_data(StackIndex::TOP, tag)
            .ok_or_else(mismatch)?;
        Ok(Self {
            ptr: ptr.cast(),
            _owner: PhantomData,
        })
    }
}

impl<T> std::ops::Deref for UserDataRef<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: the userdata's tag is `T`'s, so it holds a `T`, which lives while the borrowed
        // `UserData` pins the userdata, and is only ever borrowed shared.
        unsafe { self.ptr.as_ref() }
    }
}

/// A thread pinned in the registry, and its state, which the pin keeps alive. Only a [`Stack`]
/// makes one, from a thread on it.
pub(super) struct ThreadRef {
    reference: RegistryRef,
    state: NonNull<sys::lua_State>,
}

impl ThreadRef {
    pub(super) fn vm(&self) -> &Vm {
        self.reference.vm()
    }

    /// The address of the thread's state, which identifies it while it lives.
    pub(super) fn state_id(&self) -> usize {
        self.state.as_ptr() as usize
    }
}

/// A Luau value pinned in the registry. Clones share the pin, which the last one releases.
#[derive(Clone)]
pub(super) struct RegistryRef {
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

    pub(super) fn vm(&self) -> &Vm {
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
        let mut stack = Stack::main(self.vm());
        // A stack too full to hold both values cannot show them equal.
        stack.push_ref(self).is_ok()
            && stack.push_ref(other).is_ok()
            && stack.raw_equal(StackIndex::TOP, StackIndex::relative(-2))
    }
}

impl Drop for RegistrySlot {
    fn drop(&mut self) {
        // SAFETY: a live state, which the slot's `Vm` keeps alive, and a reference it made.
        unsafe { sys::lua_unref(self.vm.inner.state.main().as_ptr(), self.reference) };
    }
}
