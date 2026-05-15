// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::ffi::{
    CStr,
    CString,
    NulError,
    c_int,
};
use std::marker::PhantomData;
use std::ptr::{
    self,
    NonNull,
};
use std::str::Utf8Error;

use harmony_luau_sys as sys;

/// Re-evaluate before stabilizing. The final error model may split compiler,
/// loader, runtime, and conversion failures into more detailed variants once
/// callbacks, userdata, and async execution are implemented.
#[derive(Debug)]
pub enum LuauError {
    StateAllocation,
    InvalidChunkName(NulError),
    InvalidGlobalName(NulError),
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
            Self::InvalidChunkName(error) => write!(f, "invalid Luau chunk name: {error}"),
            Self::InvalidGlobalName(error) => write!(f, "invalid Luau global name: {error}"),
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

pub struct Luau {
    state: NonNull<sys::lua_State>,
}

impl Luau {
    pub fn new() -> Result<Self, LuauError> {
        let state = unsafe { sys::luaL_newstate() };
        let Some(state) = NonNull::new(state) else {
            return Err(LuauError::StateAllocation);
        };

        Ok(Self { state })
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

    pub fn as_ptr(&self) -> *mut sys::lua_State {
        self.state.as_ptr()
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

    pub fn set_global<V>(&self, name: &str, value: V) -> Result<(), LuauError>
    where
        V: IntoLuau,
    {
        let name = CString::new(name).map_err(LuauError::InvalidGlobalName)?;
        let top = unsafe { sys::lua_gettop(self.as_ptr()) };
        if let Err(error) = value.push_to_stack(self) {
            unsafe { sys::lua_settop(self.as_ptr(), top) };
            return Err(error);
        }

        unsafe { sys::lua_setglobal(self.as_ptr(), name.as_ptr()) };
        Ok(())
    }

    pub fn global<'lua, T>(&'lua self, name: &str) -> Result<T, LuauError>
    where
        T: FromLuau<'lua>,
    {
        let name = CString::new(name).map_err(LuauError::InvalidGlobalName)?;
        unsafe { sys::lua_getglobal(self.as_ptr(), name.as_ptr()) };

        let value = unsafe { T::from_stack(self, -1) };
        unsafe { sys::lua_pop(self.as_ptr(), 1) };
        value
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
            state: self,
            reference,
            _marker: PhantomData,
        })
    }
}

impl Drop for Luau {
    fn drop(&mut self) {
        unsafe { sys::lua_close(self.as_ptr()) };
    }
}

pub struct LuauFunction<'lua> {
    state: &'lua Luau,
    reference: c_int,
    _marker: PhantomData<&'lua Luau>,
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
            unsafe { sys::lua_pcall(self.state.as_ptr(), A::ARG_COUNT, R::RETURN_COUNT, 0) };
        if status != sys::LUA_OK {
            return Err(LuauError::Runtime(unsafe {
                pop_string(self.state.as_ptr())
            }));
        }

        let result = unsafe { R::from_stack(self.state) };
        if R::RETURN_COUNT > 0 {
            unsafe { sys::lua_pop(self.state.as_ptr(), R::RETURN_COUNT) };
        }
        result
    }
}

/// Re-evaluate before stabilizing. This is the first pass at stack conversion;
/// it may be replaced by a broader value/stack API when tables, functions, and
/// userdata are represented directly.
pub trait FromLuau<'lua>: Sized {
    /// # Safety
    ///
    /// `index` must refer to a valid stack slot in `luau`, and the value at
    /// that slot must remain on the stack for the duration of conversion.
    unsafe fn from_stack(luau: &'lua Luau, index: c_int) -> Result<Self, LuauError>;
}

/// Re-evaluate before stabilizing. Multi-value conversion will likely need a
/// tuple implementation strategy once function arguments and multiple returns
/// are fully supported.
pub trait FromLuauMulti<'lua>: Sized {
    const RETURN_COUNT: c_int;

    /// # Safety
    ///
    /// The top `RETURN_COUNT` stack slots in `luau` must contain this return
    /// value sequence and must remain on the stack for the duration of
    /// conversion.
    unsafe fn from_stack(luau: &'lua Luau) -> Result<Self, LuauError>;
}

impl<'lua> FromLuauMulti<'lua> for () {
    const RETURN_COUNT: c_int = 0;

    unsafe fn from_stack(_luau: &'lua Luau) -> Result<Self, LuauError> {
        Ok(())
    }
}

impl<'lua, T> FromLuauMulti<'lua> for T
where
    T: FromLuau<'lua>,
{
    const RETURN_COUNT: c_int = 1;

    unsafe fn from_stack(luau: &'lua Luau) -> Result<Self, LuauError> {
        unsafe { T::from_stack(luau, -1) }
    }
}

impl<'lua> FromLuau<'lua> for bool {
    unsafe fn from_stack(luau: &'lua Luau, index: c_int) -> Result<Self, LuauError> {
        unsafe { expect_type(luau.as_ptr(), index, sys::LUA_TBOOLEAN, "boolean")? };
        Ok(unsafe { sys::lua_toboolean(luau.as_ptr(), index) } != 0)
    }
}

impl<'lua> FromLuau<'lua> for f64 {
    unsafe fn from_stack(luau: &'lua Luau, index: c_int) -> Result<Self, LuauError> {
        let mut is_number = 0;
        let value = unsafe { sys::lua_tonumberx(luau.as_ptr(), index, &mut is_number) };
        if is_number == 0 {
            return Err(unsafe { type_error(luau.as_ptr(), index, "number") });
        }
        Ok(value)
    }
}

impl<'lua> FromLuau<'lua> for i32 {
    unsafe fn from_stack(luau: &'lua Luau, index: c_int) -> Result<Self, LuauError> {
        let mut is_integer = 0;
        let value = unsafe { sys::lua_tointegerx(luau.as_ptr(), index, &mut is_integer) };
        if is_integer == 0 {
            return Err(unsafe { type_error(luau.as_ptr(), index, "integer") });
        }
        Ok(value)
    }
}

impl<'lua> FromLuau<'lua> for i64 {
    unsafe fn from_stack(luau: &'lua Luau, index: c_int) -> Result<Self, LuauError> {
        let mut is_integer = 0;
        let value = unsafe { sys::lua_tointeger64(luau.as_ptr(), index, &mut is_integer) };
        if is_integer == 0 {
            return Err(unsafe { type_error(luau.as_ptr(), index, "integer") });
        }
        Ok(value)
    }
}

impl<'lua> FromLuau<'lua> for String {
    unsafe fn from_stack(luau: &'lua Luau, index: c_int) -> Result<Self, LuauError> {
        let bytes = unsafe { stack_bytes(luau.as_ptr(), index)? };
        String::from_utf8(bytes).map_err(|error| LuauError::Utf8(error.utf8_error()))
    }
}

impl<'lua> FromLuau<'lua> for Vec<u8> {
    unsafe fn from_stack(luau: &'lua Luau, index: c_int) -> Result<Self, LuauError> {
        unsafe { stack_bytes(luau.as_ptr(), index) }
    }
}

impl<'lua, T> FromLuau<'lua> for Option<T>
where
    T: FromLuau<'lua>,
{
    unsafe fn from_stack(luau: &'lua Luau, index: c_int) -> Result<Self, LuauError> {
        if unsafe { sys::lua_type(luau.as_ptr(), index) } == sys::LUA_TNIL {
            Ok(None)
        } else {
            unsafe { T::from_stack(luau, index) }.map(Some)
        }
    }
}

impl<'lua> FromLuau<'lua> for LuauFunction<'lua> {
    unsafe fn from_stack(luau: &'lua Luau, index: c_int) -> Result<Self, LuauError> {
        unsafe { expect_type(luau.as_ptr(), index, sys::LUA_TFUNCTION, "function")? };
        let reference = unsafe { sys::lua_ref(luau.as_ptr(), index) };
        Ok(Self {
            state: luau,
            reference,
            _marker: PhantomData,
        })
    }
}

/// Re-evaluate before stabilizing. This mirrors the initial return-conversion
/// traits but only covers simple scalar pushes for now.
pub trait IntoLuau: Sized {
    fn push_to_stack(self, luau: &Luau) -> Result<(), LuauError>;
}

pub trait IntoLuauMulti: Sized {
    const ARG_COUNT: c_int;

    fn push_to_stack(self, luau: &Luau) -> Result<(), LuauError>;
}

impl IntoLuauMulti for () {
    const ARG_COUNT: c_int = 0;

    fn push_to_stack(self, _luau: &Luau) -> Result<(), LuauError> {
        Ok(())
    }
}

impl<T> IntoLuauMulti for T
where
    T: IntoLuau,
{
    const ARG_COUNT: c_int = 1;

    fn push_to_stack(self, luau: &Luau) -> Result<(), LuauError> {
        IntoLuau::push_to_stack(self, luau)
    }
}

impl IntoLuau for bool {
    fn push_to_stack(self, luau: &Luau) -> Result<(), LuauError> {
        unsafe { sys::lua_pushboolean(luau.as_ptr(), i32::from(self)) };
        Ok(())
    }
}

impl IntoLuau for f64 {
    fn push_to_stack(self, luau: &Luau) -> Result<(), LuauError> {
        unsafe { sys::lua_pushnumber(luau.as_ptr(), self) };
        Ok(())
    }
}

impl IntoLuau for i32 {
    fn push_to_stack(self, luau: &Luau) -> Result<(), LuauError> {
        unsafe { sys::lua_pushinteger(luau.as_ptr(), self) };
        Ok(())
    }
}

impl IntoLuau for i64 {
    fn push_to_stack(self, luau: &Luau) -> Result<(), LuauError> {
        unsafe { sys::lua_pushinteger64(luau.as_ptr(), self) };
        Ok(())
    }
}

impl IntoLuau for &str {
    fn push_to_stack(self, luau: &Luau) -> Result<(), LuauError> {
        unsafe { sys::lua_pushlstring(luau.as_ptr(), self.as_ptr().cast(), self.len()) };
        Ok(())
    }
}

impl IntoLuau for String {
    fn push_to_stack(self, luau: &Luau) -> Result<(), LuauError> {
        self.as_str().push_to_stack(luau)
    }
}

impl IntoLuau for &[u8] {
    fn push_to_stack(self, luau: &Luau) -> Result<(), LuauError> {
        unsafe { sys::lua_pushlstring(luau.as_ptr(), self.as_ptr().cast(), self.len()) };
        Ok(())
    }
}

impl IntoLuau for Vec<u8> {
    fn push_to_stack(self, luau: &Luau) -> Result<(), LuauError> {
        self.as_slice().push_to_stack(luau)
    }
}

impl<T> IntoLuau for Option<T>
where
    T: IntoLuau,
{
    fn push_to_stack(self, luau: &Luau) -> Result<(), LuauError> {
        match self {
            Some(value) => value.push_to_stack(luau),
            None => {
                unsafe { sys::lua_pushnil(luau.as_ptr()) };
                Ok(())
            }
        }
    }
}

macro_rules! impl_into_luau_multi_tuple {
    ($count:expr, $($name:ident),+) => {
        impl<$($name),+> IntoLuauMulti for ($($name,)+)
        where
            $($name: IntoLuau),+
        {
            const ARG_COUNT: c_int = $count;

            #[allow(non_snake_case)]
            fn push_to_stack(self, luau: &Luau) -> Result<(), LuauError> {
                let ($($name,)+) = self;
                $($name.push_to_stack(luau)?;)+
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
    use super::{
        Luau,
        LuauError,
    };

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
}
