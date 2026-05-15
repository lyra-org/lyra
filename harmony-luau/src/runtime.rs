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

use harmony_luau_sys as sys;

#[derive(Debug)]
pub enum LuauError {
    StateAllocation,
    InvalidChunkName(NulError),
    CompileAllocation,
    Load(String),
    Runtime(String),
    Type {
        expected: &'static str,
        actual: String,
    },
}

impl std::fmt::Display for LuauError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StateAllocation => f.write_str("failed to allocate Luau state"),
            Self::InvalidChunkName(error) => write!(f, "invalid Luau chunk name: {error}"),
            Self::CompileAllocation => f.write_str("Luau compiler returned a null bytecode buffer"),
            Self::Load(message) => write!(f, "failed to load Luau chunk: {message}"),
            Self::Runtime(message) => write!(f, "Luau runtime error: {message}"),
            Self::Type { expected, actual } => {
                write!(f, "expected Luau {expected}, got {actual}")
            }
        }
    }
}

impl std::error::Error for LuauError {}

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
        let function = self.load(chunk_name, source)?;
        function.call()
    }

    pub fn eval_number(
        &self,
        chunk_name: &str,
        source: impl AsRef<[u8]>,
    ) -> Result<f64, LuauError> {
        let function = self.load(chunk_name, source)?;
        function.call_no_args(1)?;

        let mut is_number = 0;
        let value = unsafe { sys::lua_tonumberx(self.as_ptr(), -1, &mut is_number) };
        if is_number == 0 {
            let actual = unsafe { type_name(self.as_ptr(), -1) };
            unsafe { sys::lua_pop(self.as_ptr(), 1) };
            return Err(LuauError::Type {
                expected: "number",
                actual,
            });
        }

        unsafe { sys::lua_pop(self.as_ptr(), 1) };
        Ok(value)
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

impl LuauFunction<'_> {
    pub fn call(&self) -> Result<(), LuauError> {
        self.call_no_args(0)
    }

    fn call_no_args(&self, results: c_int) -> Result<(), LuauError> {
        unsafe { sys::lua_getref(self.state.as_ptr(), self.reference) };
        let status = unsafe { sys::lua_pcall(self.state.as_ptr(), 0, results, 0) };

        if status == sys::LUA_OK {
            Ok(())
        } else {
            Err(LuauError::Runtime(unsafe {
                pop_string(self.state.as_ptr())
            }))
        }
    }
}

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
        let value = luau
            .eval_number("unit/eval", "return 40 + 2")
            .expect("evaluate number");

        assert_eq!(value, 42.0);
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
