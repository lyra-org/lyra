use std::{
    any::Any,
    collections::HashMap,
    fmt,
    fs,
    path::{
        Component,
        Path,
        PathBuf,
    },
    sync::Arc,
};

use anyhow::Result;
use harmony_luau as luau;

use crate::{
    CapabilityId,
    ChunkOrigin,
    ModuleId,
};

pub type NativeModuleInstaller =
    Arc<dyn for<'a> Fn(ModuleLoadContext<'a>) -> Result<ModuleExport> + Send + Sync + 'static>;

pub type LuauModuleInitializer = Arc<
    dyn Fn(&luau::Vm, &ChunkOrigin, &luau::Table) -> luau::runtime::Result<()>
        + Send
        + Sync
        + 'static,
>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ModuleCacheKey(pub Arc<str>);

pub trait CapabilityPolicy {
    fn is_allowed(&self, origin: &ChunkOrigin, capability: &CapabilityId) -> bool;
}

pub struct AllowAllCapabilities;

impl CapabilityPolicy for AllowAllCapabilities {
    fn is_allowed(&self, _origin: &ChunkOrigin, _capability: &CapabilityId) -> bool {
        true
    }
}

pub trait SourceLoader {
    fn resolve(&self, request: SourceRequest<'_>) -> Result<ResolvedSource, ModuleLoadError>;
}

#[derive(Clone, Copy)]
pub struct SourceRequest<'a> {
    pub specifier: &'a str,
    pub origin: &'a ChunkOrigin,
}

#[derive(Clone, Debug)]
pub struct ResolvedSource {
    pub bytes: Arc<[u8]>,
    pub origin: ChunkOrigin,
    pub cache_key: ModuleCacheKey,
}

#[derive(Default)]
pub struct MemorySourceLoader {
    sources: HashMap<ModuleCacheKey, Arc<[u8]>>,
}

impl MemorySourceLoader {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(
        &mut self,
        cache_key: impl Into<Arc<str>>,
        bytes: impl Into<Arc<[u8]>>,
    ) -> &mut Self {
        self.sources
            .insert(ModuleCacheKey(cache_key.into()), bytes.into());
        self
    }

    fn key_for(request: SourceRequest<'_>) -> Result<(ModuleCacheKey, ChunkOrigin)> {
        if let Some(path) = request.specifier.strip_prefix("@self/") {
            let plugin = request
                .origin
                .plugin
                .clone()
                .ok_or_else(|| anyhow::anyhow!("@self require needs plugin origin"))?;
            let path = normalize_logical_path(path)?;
            let origin = ChunkOrigin {
                plugin: Some(plugin.clone()),
                path: Some(Arc::from(format!("plugins/{plugin}/{path}"))),
                ..ChunkOrigin::default()
            };
            return Ok((
                ModuleCacheKey(Arc::from(format!("plugin:{plugin}/{path}"))),
                origin,
            ));
        }

        if request.specifier.starts_with("./") || request.specifier.starts_with("../") {
            let base_path = request
                .origin
                .path
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("relative require needs source path origin"))?;
            let parent = Path::new(base_path)
                .parent()
                .unwrap_or_else(|| Path::new(""));
            let path = normalize_logical_path(parent.join(request.specifier))?;
            let cache_path = if let Some(plugin) = request.origin.plugin.as_deref() {
                let plugin_root = format!("plugins/{plugin}/");
                path.strip_prefix(&plugin_root).unwrap_or(&path).to_string()
            } else {
                path.clone()
            };
            let cache_prefix = request
                .origin
                .plugin
                .as_deref()
                .map(|plugin| format!("plugin:{plugin}/"))
                .unwrap_or_else(|| "source:".to_string());
            let origin = ChunkOrigin {
                plugin: request.origin.plugin.clone(),
                path: Some(Arc::from(path.clone())),
                ..ChunkOrigin::default()
            };
            return Ok((
                ModuleCacheKey(Arc::from(format!("{cache_prefix}{cache_path}"))),
                origin,
            ));
        }

        if let Some(path) = request.specifier.strip_prefix('@') {
            let path = normalize_logical_path(path)?;
            let origin = ChunkOrigin {
                module: Some(ModuleId(Arc::from(path.clone()))),
                path: Some(Arc::from(path.clone())),
                ..ChunkOrigin::default()
            };
            return Ok((ModuleCacheKey(Arc::from(format!("alias:{path}"))), origin));
        }

        let path = normalize_logical_path(request.specifier)?;
        let origin = ChunkOrigin {
            path: Some(Arc::from(path.clone())),
            ..ChunkOrigin::default()
        };
        Ok((ModuleCacheKey(Arc::from(format!("source:{path}"))), origin))
    }
}

impl SourceLoader for MemorySourceLoader {
    fn resolve(&self, request: SourceRequest<'_>) -> Result<ResolvedSource, ModuleLoadError> {
        let (cache_key, origin) =
            Self::key_for(request).map_err(ModuleLoadError::SourceResolveFailed)?;
        let bytes = self.sources.get(&cache_key).cloned().ok_or_else(|| {
            ModuleLoadError::UnknownSource {
                cache_key: cache_key.clone(),
            }
        })?;

        Ok(ResolvedSource {
            bytes,
            origin,
            cache_key,
        })
    }
}

pub struct FilesystemSourceLoader {
    source_root: PathBuf,
    plugins_dir: PathBuf,
}

impl FilesystemSourceLoader {
    pub fn new(source_root: impl Into<PathBuf>, plugins_dir: impl Into<PathBuf>) -> Self {
        Self {
            source_root: source_root.into(),
            plugins_dir: plugins_dir.into(),
        }
    }

    fn path_for(&self, cache_key: &ModuleCacheKey) -> Result<PathBuf> {
        let key = cache_key.0.as_ref();
        if let Some(path) = key.strip_prefix("plugin:") {
            let (plugin, relative_path) = path
                .split_once('/')
                .ok_or_else(|| anyhow::anyhow!("plugin source key is missing a path"))?;
            return Ok(resolve_luau_source_path(
                self.plugins_dir.join(plugin).join(relative_path),
            ));
        }

        if let Some(path) = key.strip_prefix("alias:") {
            return Ok(resolve_luau_source_path(self.source_root.join(path)));
        }

        if let Some(path) = key.strip_prefix("source:") {
            return Ok(resolve_luau_source_path(self.source_root.join(path)));
        }

        anyhow::bail!("unsupported source cache key '{}'", key)
    }
}

fn resolve_luau_source_path(path: PathBuf) -> PathBuf {
    if path.extension().is_none() {
        let with_luau = path.with_extension("luau");
        if with_luau.is_file() {
            return with_luau;
        }
    }
    path
}

impl SourceLoader for FilesystemSourceLoader {
    fn resolve(&self, request: SourceRequest<'_>) -> Result<ResolvedSource, ModuleLoadError> {
        let (cache_key, origin) =
            MemorySourceLoader::key_for(request).map_err(ModuleLoadError::SourceResolveFailed)?;
        let path = self
            .path_for(&cache_key)
            .map_err(ModuleLoadError::SourceResolveFailed)?;
        let bytes = fs::read(&path).map(Arc::<[u8]>::from).map_err(|error| {
            ModuleLoadError::SourceLoadFailed(anyhow::anyhow!(
                "failed to read Luau source '{}': {error}",
                path.display()
            ))
        })?;

        Ok(ResolvedSource {
            bytes,
            origin,
            cache_key,
        })
    }
}

pub struct ModuleLoadContext<'a> {
    pub origin: &'a ChunkOrigin,
    pub module: &'a ModuleSpec,
}

pub struct ModuleExport {
    value: Arc<dyn Any + Send + Sync>,
}

impl fmt::Debug for ModuleExport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModuleExport").finish_non_exhaustive()
    }
}

impl ModuleExport {
    pub fn new<T>(value: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self {
            value: Arc::new(value),
        }
    }

    pub fn downcast<T>(self: Arc<Self>) -> std::result::Result<Arc<T>, Arc<dyn Any + Send + Sync>>
    where
        T: Send + Sync + 'static,
    {
        self.value.clone().downcast::<T>()
    }
}

pub struct ModuleSpec {
    pub id: ModuleId,
    pub capability: Option<CapabilityId>,
    pub functions: Vec<FunctionSpec>,
    pub userdata: Vec<UserDataSpec>,
    pub install: Option<NativeModuleInstaller>,
    pub luau_initializer: Option<LuauModuleInitializer>,
}

impl ModuleSpec {
    pub fn new(id: impl Into<Arc<str>>) -> Self {
        Self {
            id: ModuleId(id.into()),
            capability: None,
            functions: Vec::new(),
            userdata: Vec::new(),
            install: None,
            luau_initializer: None,
        }
    }

    pub fn capability(mut self, capability: impl Into<Arc<str>>) -> Self {
        self.capability = Some(CapabilityId(capability.into()));
        self
    }

    pub fn function(mut self, function: FunctionSpec) -> Self {
        self.functions.push(function);
        self
    }

    pub fn userdata(mut self, userdata: UserDataSpec) -> Self {
        self.userdata.push(userdata);
        self
    }

    pub fn install<F>(mut self, installer: F) -> Self
    where
        F: for<'a> Fn(ModuleLoadContext<'a>) -> Result<ModuleExport> + Send + Sync + 'static,
    {
        self.install = Some(Arc::new(installer));
        self
    }

    pub fn luau_initializer<F>(mut self, initializer: F) -> Self
    where
        F: Fn(&luau::Vm, &ChunkOrigin, &luau::Table) -> luau::runtime::Result<()>
            + Send
            + Sync
            + 'static,
    {
        self.luau_initializer = Some(Arc::new(initializer));
        self
    }
}

#[derive(Clone, Debug)]
pub struct UserDataSpec {
    pub name: Arc<str>,
    pub methods: Vec<FunctionSpec>,
}

impl UserDataSpec {
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        Self {
            name: name.into(),
            methods: Vec::new(),
        }
    }

    pub fn method(mut self, method: FunctionSpec) -> Self {
        self.methods.push(method);
        self
    }
}

#[derive(Clone, Debug)]
pub struct GlobalSpec {
    pub name: Arc<str>,
    pub functions: Vec<FunctionSpec>,
}

impl GlobalSpec {
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        Self {
            name: name.into(),
            functions: Vec::new(),
        }
    }

    pub fn function(mut self, function: FunctionSpec) -> Self {
        self.functions.push(function);
        self
    }
}

#[derive(Clone)]
pub enum FunctionCallback {
    Sync(luau::NativeFn),
    Async(luau::NativeAsyncFn),
}

impl fmt::Debug for FunctionCallback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sync(_) => f.write_str("FunctionCallback::Sync(..)"),
            Self::Async(_) => f.write_str("FunctionCallback::Async(..)"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct FunctionSpec {
    pub name: Arc<str>,
    pub yields: bool,
    pub context_type: Option<&'static str>,
    pub arg_types: Vec<&'static str>,
    pub arg_names: Vec<Arc<str>>,
    pub variadic: bool,
    pub return_types: Vec<&'static str>,
    pub callback: Option<FunctionCallback>,
}

impl FunctionSpec {
    pub fn sync_fn(name: impl Into<Arc<str>>) -> Self {
        Self::new(name, false)
    }

    pub fn async_fn(name: impl Into<Arc<str>>) -> Self {
        Self::new(name, true)
    }

    fn new(name: impl Into<Arc<str>>, yields: bool) -> Self {
        Self {
            name: name.into(),
            yields,
            context_type: None,
            arg_types: Vec::new(),
            arg_names: Vec::new(),
            variadic: false,
            return_types: Vec::new(),
            callback: None,
        }
    }

    pub fn context<T: 'static>(mut self) -> Self {
        self.context_type = Some(std::any::type_name::<T>());
        self
    }

    pub fn args<T: 'static>(mut self) -> Self {
        self.arg_types.push(std::any::type_name::<T>());
        self
    }

    pub fn named_arg<T: 'static>(mut self, name: impl Into<Arc<str>>) -> Self {
        self.arg_types.push(std::any::type_name::<T>());
        self.arg_names.push(name.into());
        self
    }

    pub fn arg_name(mut self, name: impl Into<Arc<str>>) -> Self {
        self.arg_names.push(name.into());
        self
    }

    pub fn variadic_args<T: 'static>(mut self) -> Self {
        self.arg_types.push(std::any::type_name::<T>());
        self.variadic = true;
        self
    }

    pub fn returns<T: 'static>(mut self) -> Self {
        self.return_types.push(std::any::type_name::<T>());
        self
    }

    pub fn call<F>(mut self, callback: F) -> Self
    where
        F: for<'vm> Fn(luau::CallFrame<'vm>) -> luau::runtime::Result<()> + Send + Sync + 'static,
    {
        self.callback = Some(FunctionCallback::Sync(Arc::new(callback)));
        self
    }

    pub fn call_native(mut self, callback: luau::NativeFn) -> Self {
        self.callback = Some(FunctionCallback::Sync(callback));
        self
    }

    pub fn call_async_native(mut self, callback: luau::NativeAsyncFn) -> Self {
        self.callback = Some(FunctionCallback::Async(callback));
        self
    }
}

#[derive(Debug)]
pub enum ModuleLoadError {
    DuplicateModule { module_id: ModuleId },
    UnknownModule { module_id: ModuleId },
    MissingInstaller { module_id: ModuleId },
    CapabilityDenied(ModuleCapabilityDenied),
    InstallFailed(anyhow::Error),
    SourceResolveFailed(anyhow::Error),
    SourceLoadFailed(anyhow::Error),
    UnknownSource { cache_key: ModuleCacheKey },
}

impl fmt::Display for ModuleLoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateModule { module_id } => {
                write!(f, "module '{}' is already registered", module_id.0)
            }
            Self::UnknownModule { module_id } => {
                write!(f, "module '{}' is not registered", module_id.0)
            }
            Self::MissingInstaller { module_id } => {
                write!(f, "module '{}' has no installer", module_id.0)
            }
            Self::CapabilityDenied(error) => error.fmt(f),
            Self::InstallFailed(error) => write!(f, "module install failed: {error}"),
            Self::SourceResolveFailed(error) => write!(f, "source resolve failed: {error}"),
            Self::SourceLoadFailed(error) => write!(f, "source load failed: {error}"),
            Self::UnknownSource { cache_key } => {
                write!(f, "source '{}' is not registered", cache_key.0)
            }
        }
    }
}

fn normalize_logical_path(path: impl AsRef<Path>) -> Result<String> {
    let mut output = PathBuf::new();

    for component in path.as_ref().components() {
        match component {
            Component::Prefix(_) | Component::RootDir => {
                anyhow::bail!("absolute paths are not valid module specifiers")
            }
            Component::CurDir => {}
            Component::ParentDir => {
                if !output.pop() {
                    anyhow::bail!("module specifier escapes its root")
                }
            }
            Component::Normal(part) => output.push(part),
        }
    }

    let path = output.to_string_lossy();
    if path.is_empty() {
        anyhow::bail!("module specifier is empty");
    }

    Ok(path.into_owned())
}

impl std::error::Error for ModuleLoadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::CapabilityDenied(error) => Some(error),
            Self::InstallFailed(error)
            | Self::SourceResolveFailed(error)
            | Self::SourceLoadFailed(error) => error.source(),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ModuleCapabilityDenied {
    pub origin: ChunkOrigin,
    pub module_id: ModuleId,
    pub capability: CapabilityId,
}

impl fmt::Display for ModuleCapabilityDenied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let caller = self
            .origin
            .plugin
            .as_deref()
            .unwrap_or("<non-plugin caller>");
        write!(
            f,
            "origin '{}' required '{}' without capability '{}'",
            caller, self.module_id.0, self.capability.0
        )
    }
}

impl std::error::Error for ModuleCapabilityDenied {}

#[derive(Default)]
pub struct ModuleRegistry {
    modules: HashMap<ModuleId, ModuleSpec>,
    cache: HashMap<ModuleId, Arc<ModuleExport>>,
    luau_cache: HashMap<LuauModuleCacheKey, Arc<luau::Table>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct LuauModuleCacheKey {
    module_id: ModuleId,
    plugin: Option<Arc<str>>,
}

impl LuauModuleCacheKey {
    fn new(module_id: &ModuleId, origin: &ChunkOrigin) -> Self {
        Self {
            module_id: module_id.clone(),
            plugin: origin.plugin.clone(),
        }
    }
}

#[derive(Default)]
pub struct LuauSourceCache {
    cache: HashMap<ModuleCacheKey, Arc<[luau::Value]>>,
}

impl LuauSourceCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, cache_key: &ModuleCacheKey) -> Option<Arc<[luau::Value]>> {
        self.cache.get(cache_key).cloned()
    }

    pub fn insert(
        &mut self,
        cache_key: ModuleCacheKey,
        values: Arc<[luau::Value]>,
    ) -> Option<Arc<[luau::Value]>> {
        self.cache.insert(cache_key, values)
    }

    pub fn require(
        &mut self,
        vm: &luau::Vm,
        loader: &dyn SourceLoader,
        specifier: &str,
        origin: &ChunkOrigin,
    ) -> std::result::Result<Arc<[luau::Value]>, ModuleLoadError> {
        let source = loader.resolve(SourceRequest { specifier, origin })?;
        if let Some(values) = self.cache.get(&source.cache_key) {
            return Ok(values.clone());
        }

        let values = vm
            .eval(source.bytes.clone(), luau_origin(&source.origin))
            .map_err(|error| ModuleLoadError::SourceLoadFailed(anyhow::Error::new(error)))?;
        let values = Arc::<[luau::Value]>::from(values.into_boxed_slice());
        self.cache.insert(source.cache_key, values.clone());
        Ok(values)
    }

    pub fn invalidate(&mut self, cache_key: &ModuleCacheKey) -> bool {
        self.cache.remove(cache_key).is_some()
    }
}

impl ModuleRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn contains(&self, module_id: &ModuleId) -> bool {
        self.modules.contains_key(module_id)
    }

    pub fn register(&mut self, spec: ModuleSpec) -> std::result::Result<(), ModuleLoadError> {
        if self.modules.contains_key(&spec.id) {
            return Err(ModuleLoadError::DuplicateModule {
                module_id: spec.id.clone(),
            });
        }

        self.modules.insert(spec.id.clone(), spec);
        Ok(())
    }

    pub fn require(
        &mut self,
        origin: &ChunkOrigin,
        module_id: &ModuleId,
        policy: &dyn CapabilityPolicy,
    ) -> std::result::Result<Arc<ModuleExport>, ModuleLoadError> {
        let module = self
            .modules
            .get(module_id)
            .ok_or_else(|| ModuleLoadError::UnknownModule {
                module_id: module_id.clone(),
            })?;

        if let Some(capability) = &module.capability
            && !policy.is_allowed(origin, capability)
        {
            return Err(ModuleLoadError::CapabilityDenied(ModuleCapabilityDenied {
                origin: origin.clone(),
                module_id: module_id.clone(),
                capability: capability.clone(),
            }));
        }

        if let Some(export) = self.cache.get(module_id) {
            return Ok(export.clone());
        }

        let installer =
            module
                .install
                .as_ref()
                .ok_or_else(|| ModuleLoadError::MissingInstaller {
                    module_id: module_id.clone(),
                })?;
        let export = Arc::new(
            installer(ModuleLoadContext { origin, module })
                .map_err(ModuleLoadError::InstallFailed)?,
        );
        self.cache.insert(module_id.clone(), export.clone());
        Ok(export)
    }

    pub fn require_luau_module(
        &mut self,
        vm: &luau::Vm,
        origin: &ChunkOrigin,
        module_id: &ModuleId,
        policy: &dyn CapabilityPolicy,
    ) -> std::result::Result<Arc<luau::Table>, ModuleLoadError> {
        let module = self
            .modules
            .get(module_id)
            .ok_or_else(|| ModuleLoadError::UnknownModule {
                module_id: module_id.clone(),
            })?;

        if let Some(capability) = &module.capability
            && !policy.is_allowed(origin, capability)
        {
            return Err(ModuleLoadError::CapabilityDenied(ModuleCapabilityDenied {
                origin: origin.clone(),
                module_id: module_id.clone(),
                capability: capability.clone(),
            }));
        }

        let cache_key = LuauModuleCacheKey::new(module_id, origin);
        if let Some(export) = self.luau_cache.get(&cache_key) {
            return Ok(export.clone());
        }

        let export = Arc::new(
            install_luau_module(vm, origin, module)
                .map_err(|error| ModuleLoadError::InstallFailed(anyhow::Error::new(error)))?,
        );
        self.luau_cache.insert(cache_key, export.clone());
        Ok(export)
    }

    pub fn invalidate(&mut self, module_id: &ModuleId) -> bool {
        let removed = self.cache.remove(module_id).is_some();
        let removed = {
            let before = self.luau_cache.len();
            self.luau_cache.retain(|key, _| key.module_id != *module_id);
            self.luau_cache.len() != before || removed
        };
        removed
    }
}

pub struct LuauRequireRuntime {
    registry: std::cell::RefCell<ModuleRegistry>,
    source_cache: std::cell::RefCell<LuauSourceCache>,
    loader: Box<dyn SourceLoader>,
    policy: Box<dyn CapabilityPolicy>,
}

impl LuauRequireRuntime {
    pub fn new<L, P>(loader: L, policy: P) -> Self
    where
        L: SourceLoader + 'static,
        P: CapabilityPolicy + 'static,
    {
        Self {
            registry: std::cell::RefCell::new(ModuleRegistry::new()),
            source_cache: std::cell::RefCell::new(LuauSourceCache::new()),
            loader: Box::new(loader),
            policy: Box::new(policy),
        }
    }

    pub fn register(&self, spec: ModuleSpec) -> std::result::Result<(), ModuleLoadError> {
        self.registry.borrow_mut().register(spec)
    }
}

pub fn install_luau_require(vm: &luau::Vm, origin: &ChunkOrigin) -> luau::runtime::Result<()> {
    let function = vm.create_function_with_options(
        luau::NativeFunctionOptions::new(luau::ChunkOrigin {
            module: Some(luau::ModuleId(Arc::from("harmony/require"))),
            plugin: origin.plugin.clone(),
            path: origin.path.clone(),
        })
        .function_name("require")
        .argument_names([Arc::from("specifier")]),
        Arc::new(require_luau_callback),
    )?;
    vm.set_global_function("require", &function)
}

fn require_luau_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let specifier: String = frame.args.read_named("specifier")?;
    let runtime = frame.vm.data().get::<LuauRequireRuntime>()?;
    let origin = core_origin(&frame.context.origin);

    if let Some(module_id) = specifier
        .strip_prefix('@')
        .map(|path| ModuleId(Arc::from(path)))
        .filter(|module_id| runtime.registry.borrow().contains(module_id))
    {
        let module = runtime
            .registry
            .borrow_mut()
            .require_luau_module(frame.vm, &origin, &module_id, runtime.policy.as_ref())
            .map_err(module_load_runtime_error)?;
        frame.returns.write((*module).clone())?;
        return Ok(());
    }

    let values = runtime
        .require_source(frame.vm, &specifier, &origin)
        .map_err(module_load_runtime_error)?;
    for value in values.iter().cloned() {
        frame.returns.write(value)?;
    }
    Ok(())
}

impl LuauRequireRuntime {
    fn require_source(
        &self,
        vm: &luau::Vm,
        specifier: &str,
        origin: &ChunkOrigin,
    ) -> std::result::Result<Arc<[luau::Value]>, ModuleLoadError> {
        let source = self.loader.resolve(SourceRequest { specifier, origin })?;
        if let Some(values) = self.source_cache.borrow().get(&source.cache_key) {
            return Ok(values);
        }

        install_luau_require(vm, &source.origin)
            .map_err(|error| ModuleLoadError::SourceLoadFailed(anyhow::Error::new(error)))?;
        let result = vm.eval(
            source_with_local_require(source.bytes.as_ref()),
            luau_origin(&source.origin),
        );
        let restore_result = install_luau_require(vm, origin);
        if let Err(error) = restore_result {
            return Err(ModuleLoadError::SourceLoadFailed(anyhow::Error::new(error)));
        }
        let values =
            result.map_err(|error| ModuleLoadError::SourceLoadFailed(anyhow::Error::new(error)))?;
        let values = Arc::<[luau::Value]>::from(values.into_boxed_slice());
        self.source_cache
            .borrow_mut()
            .insert(source.cache_key, values.clone());
        Ok(values)
    }
}

fn source_with_local_require(source: &[u8]) -> Arc<[u8]> {
    const PREFIX: &[u8] = b"local require = require\n";
    let mut wrapped = Vec::with_capacity(PREFIX.len() + source.len());
    wrapped.extend_from_slice(PREFIX);
    wrapped.extend_from_slice(source);
    Arc::from(wrapped)
}

fn module_load_runtime_error(error: ModuleLoadError) -> luau::Error {
    luau::Error::Runtime(error.to_string())
}

fn core_origin(origin: &luau::ChunkOrigin) -> ChunkOrigin {
    ChunkOrigin {
        module: origin
            .module
            .as_ref()
            .map(|module| ModuleId(module.0.clone())),
        plugin: origin.plugin.clone(),
        path: origin.path.clone(),
    }
}

pub fn install_luau_module(
    vm: &luau::Vm,
    origin: &ChunkOrigin,
    module: &ModuleSpec,
) -> luau::runtime::Result<luau::Table> {
    #[derive(Clone, Copy)]
    enum TableHandle {
        Root,
        Nested(usize),
    }

    fn table_for<'a>(
        root: &'a luau::Table,
        nested: &'a [(String, luau::Table)],
        handle: TableHandle,
    ) -> &'a luau::Table {
        match handle {
            TableHandle::Root => root,
            TableHandle::Nested(index) => &nested[index].1,
        }
    }

    fn function_origin(origin: &ChunkOrigin, module_id: &ModuleId) -> luau::ChunkOrigin {
        luau::ChunkOrigin {
            module: Some(luau::ModuleId(module_id.0.clone())),
            plugin: origin.plugin.clone(),
            path: origin.path.clone(),
        }
    }

    fn capability(capability: &CapabilityId) -> luau::CapabilityId {
        luau::CapabilityId(capability.0.clone())
    }

    let root = vm.create_table()?;
    let mut nested = Vec::<(String, luau::Table)>::new();
    let mut nested_indices = HashMap::<String, usize>::new();

    for spec in &module.functions {
        let Some(callback) = spec.callback.clone() else {
            continue;
        };
        let segments = spec.name.split('.').collect::<Vec<_>>();
        if segments.iter().any(|segment| segment.is_empty()) {
            return Err(luau::Error::Runtime(format!(
                "module '{}' function path '{}' contains an empty segment",
                module.id.0, spec.name
            )));
        }
        let Some((leaf, parents)) = segments.split_last() else {
            continue;
        };

        let mut parent = TableHandle::Root;
        let mut key = String::new();
        for segment in parents {
            if !key.is_empty() {
                key.push('.');
            }
            key.push_str(segment);

            if let Some(index) = nested_indices.get(&key).copied() {
                parent = TableHandle::Nested(index);
                continue;
            }

            let table = vm.create_table()?;
            table_for(&root, &nested, parent).set_table_raw(vm, segment, &table)?;
            let index = nested.len();
            nested.push((key.clone(), table));
            nested_indices.insert(key.clone(), index);
            parent = TableHandle::Nested(index);
        }

        let mut options = luau::NativeFunctionOptions::new(function_origin(origin, &module.id))
            .function_name(spec.name.clone())
            .argument_names(spec.arg_names.clone());
        if let Some(capability_id) = &module.capability {
            options = options.capability(capability(capability_id));
        }
        let callback = match callback {
            FunctionCallback::Sync(callback) => callback,
            FunctionCallback::Async(callback) => async_luau_callback(callback),
        };
        let function = vm.create_function_with_options(options, callback)?;
        table_for(&root, &nested, parent).set_function_raw(vm, leaf, &function)?;
    }

    if let Some(initializer) = &module.luau_initializer {
        initializer(vm, origin, &root)?;
    }

    for (_, table) in &nested {
        table.set_readonly(vm, true)?;
    }
    root.set_readonly(vm, true)?;
    Ok(root)
}

pub fn async_luau_callback(callback: luau::NativeAsyncFn) -> luau::NativeFn {
    Arc::new(move |mut frame| {
        let vm = frame.vm.clone();
        let thread = frame.thread.clone();
        let context = core_call_context(&frame.context);
        let scheduler = frame.vm.data().get::<crate::LocalScheduler>()?;
        frame.yield_now();
        let future = callback(frame)?;
        scheduler.park_luau_thread(&thread);
        scheduler.schedule_luau_future(context, vm, thread, future);
        Ok(())
    })
}

fn core_call_context(context: &luau::CallContext) -> crate::CallContext {
    let mut caller = crate::ContextBag::default();
    for (type_id, value) in context.caller.cloned_entries() {
        caller.insert_shared(type_id, value);
    }
    crate::CallContext {
        origin: core_origin(&context.origin),
        capability: context
            .capability
            .as_ref()
            .map(|capability| CapabilityId(capability.0.clone())),
        caller,
        task_group: crate::TaskGroupId(context.task_group.0),
    }
}

pub fn install_luau_globals(
    vm: &luau::Vm,
    origin: &ChunkOrigin,
    globals: &GlobalSpec,
) -> luau::runtime::Result<()> {
    fn function_origin(origin: &ChunkOrigin, module_id: &str) -> luau::ChunkOrigin {
        luau::ChunkOrigin {
            module: Some(luau::ModuleId(Arc::from(module_id))),
            plugin: origin.plugin.clone(),
            path: origin.path.clone(),
        }
    }

    for spec in &globals.functions {
        let Some(callback) = spec.callback.clone() else {
            continue;
        };
        let FunctionCallback::Sync(callback) = callback else {
            return Err(luau::Error::Runtime(format!(
                "global '{}' function '{}' requires async scheduler support",
                globals.name, spec.name
            )));
        };

        if spec.name.contains('.') {
            return Err(luau::Error::Runtime(format!(
                "global function '{}' must be a direct global name",
                spec.name
            )));
        }

        let function = vm.create_function_with_options(
            luau::NativeFunctionOptions::new(function_origin(origin, &globals.name))
                .function_name(spec.name.clone())
                .argument_names(spec.arg_names.clone()),
            callback,
        )?;
        vm.set_global_function(&spec.name, &function)?;
    }

    Ok(())
}

fn luau_origin(origin: &ChunkOrigin) -> luau::ChunkOrigin {
    luau::ChunkOrigin {
        module: origin
            .module
            .as_ref()
            .map(|id| luau::ModuleId(id.0.clone())),
        plugin: origin.plugin.clone(),
        path: origin.path.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handwritten_function_specs_do_not_need_macros() {
        struct RequestCaller;
        struct TrackQueryOptions;
        struct TrackQueryResult;

        let spec = ModuleSpec::new("lyra/tracks")
            .capability("lyra.tracks")
            .function(
                FunctionSpec::async_fn("query")
                    .context::<RequestCaller>()
                    .named_arg::<TrackQueryOptions>("opts")
                    .returns::<TrackQueryResult>(),
            );

        assert_eq!(spec.id.0.as_ref(), "lyra/tracks");
        assert_eq!(spec.capability.as_ref().unwrap().0.as_ref(), "lyra.tracks");
        assert_eq!(spec.functions[0].name.as_ref(), "query");
        assert_eq!(spec.functions[0].arg_names[0].as_ref(), "opts");
        assert!(spec.functions[0].yields);
        assert!(
            spec.functions[0]
                .context_type
                .is_some_and(|name| name.contains("RequestCaller"))
        );
    }

    #[test]
    fn handwritten_global_specs_do_not_need_macros() {
        struct LogValue;

        let spec = GlobalSpec::new("harmony.globals")
            .function(FunctionSpec::sync_fn("warn").variadic_args::<LogValue>());

        assert_eq!(spec.name.as_ref(), "harmony.globals");
        assert_eq!(spec.functions[0].name.as_ref(), "warn");
        assert!(spec.functions[0].variadic);
        assert!(
            spec.functions[0]
                .arg_types
                .iter()
                .any(|name| name.contains("LogValue"))
        );
    }

    #[test]
    fn handwritten_userdata_specs_do_not_need_macros() {
        struct DataStore;
        struct JsonValue;

        let spec = ModuleSpec::new("lyra/datastore").userdata(
            UserDataSpec::new("DataStore").method(
                FunctionSpec::async_fn("get")
                    .args::<String>()
                    .returns::<Option<JsonValue>>(),
            ),
        );

        assert_eq!(spec.userdata.len(), 1);
        assert_eq!(spec.userdata[0].name.as_ref(), "DataStore");
        assert_eq!(spec.userdata[0].methods[0].name.as_ref(), "get");
        assert!(spec.userdata[0].methods[0].yields);
        assert!(
            spec.userdata[0].methods[0]
                .return_types
                .iter()
                .any(|name| name.contains("JsonValue"))
        );
        let _ = std::any::type_name::<DataStore>();
    }

    struct PluginPolicy {
        allowed_plugin: &'static str,
    }

    impl CapabilityPolicy for PluginPolicy {
        fn is_allowed(&self, origin: &ChunkOrigin, _capability: &CapabilityId) -> bool {
            origin.plugin.as_deref() == Some(self.allowed_plugin)
        }
    }

    #[test]
    fn registry_installs_rust_module_once_and_returns_cached_export() {
        let module_id = ModuleId(Arc::from("harmony/json"));
        let mut registry = ModuleRegistry::new();
        registry
            .register(
                ModuleSpec::new(module_id.0.clone())
                    .capability("harmony.json")
                    .install(|_| Ok(ModuleExport::new(42_u64))),
            )
            .expect("register module");

        let origin = ChunkOrigin {
            plugin: Some(Arc::from("demo")),
            ..ChunkOrigin::default()
        };
        let first = registry
            .require(&origin, &module_id, &AllowAllCapabilities)
            .expect("first require");
        let second = registry
            .require(&origin, &module_id, &AllowAllCapabilities)
            .expect("cached require");

        assert!(Arc::ptr_eq(&first, &second));
        let value = first.downcast::<u64>().expect("u64 export");
        assert_eq!(*value, 42);
    }

    #[test]
    fn luau_module_installer_materializes_sync_callbacks_in_readonly_tables()
    -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        let origin = ChunkOrigin {
            plugin: Some(Arc::from("demo")),
            path: Some(Arc::from("plugins/demo/init.luau")),
            ..ChunkOrigin::default()
        };
        let module = ModuleSpec::new("demo/math")
            .capability("demo.math")
            .function(
                FunctionSpec::sync_fn("add")
                    .arg_name("lhs")
                    .arg_name("rhs")
                    .call(|mut frame| {
                        assert_eq!(
                            frame.context.origin.module.as_ref().map(|id| id.0.as_ref()),
                            Some("demo/math")
                        );
                        assert_eq!(frame.context.origin.plugin.as_deref(), Some("demo"));
                        assert_eq!(
                            frame.context.capability.as_ref().map(|id| id.0.as_ref()),
                            Some("demo.math")
                        );
                        let lhs: f64 = frame.args.read()?;
                        let rhs: f64 = frame.args.read()?;
                        frame.returns.write(lhs + rhs)?;
                        Ok(())
                    }),
            )
            .function(
                FunctionSpec::sync_fn("nested.double")
                    .arg_name("value")
                    .call(|mut frame| {
                        let value: f64 = frame.args.read()?;
                        frame.returns.write(value * 2.0)?;
                        Ok(())
                    }),
            );
        let table = install_luau_module(&vm, &origin, &module)?;
        vm.set_global_table("module", &table)?;

        let values = vm.eval(
            Arc::<[u8]>::from(&b"return module.add(20, 22), module.nested.double(21)"[..]),
            luau::ChunkOrigin::default(),
        )?;
        assert_eq!(
            values,
            vec![luau::Value::Number(42.0), luau::Value::Number(42.0)]
        );
        assert!(matches!(
            vm.eval(
                Arc::<[u8]>::from(&b"module.nested.double = nil"[..]),
                luau::ChunkOrigin::default(),
            ),
            Err(luau::Error::Runtime(_))
        ));
        Ok(())
    }

    #[test]
    fn luau_module_installer_schedules_async_callbacks() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        vm.data().insert(crate::LocalScheduler::new())?;
        let scheduler = vm.data().get::<crate::LocalScheduler>()?;
        let module = ModuleSpec::new("demo/async").function(
            FunctionSpec::async_fn("add_one")
                .arg_name("value")
                .call_async_native(Arc::new(|mut frame| {
                    let value: i64 = frame.args.read_named("value")?;
                    Ok(Box::pin(async move {
                        Ok(vec![luau::Value::Integer(value + 1)])
                    }))
                })),
        );
        let table = install_luau_module(&vm, &ChunkOrigin::default(), &module)?;
        vm.set_global_table("module", &table)?;
        let root = vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"async_result = module.add_one(41)"[..]),
            luau::ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&root)?;

        scheduler.spawn_luau_thread(crate::CallContext::default(), vm.clone(), thread, vec![]);
        assert_eq!(scheduler.poll_ready(), 1);
        assert_eq!(scheduler.poll_ready(), 1);
        assert_eq!(
            vm.eval(
                Arc::<[u8]>::from(&b"return async_result"[..]),
                luau::ChunkOrigin::default(),
            )?,
            vec![luau::Value::Integer(42)]
        );
        Ok(())
    }

    #[test]
    fn luau_global_installer_materializes_sync_callbacks() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        let origin = ChunkOrigin {
            plugin: Some(Arc::from("demo")),
            ..ChunkOrigin::default()
        };
        let globals = GlobalSpec::new("demo.globals").function(
            FunctionSpec::sync_fn("demo_global")
                .arg_name("value")
                .call(|mut frame| {
                    assert_eq!(
                        frame.context.origin.module.as_ref().map(|id| id.0.as_ref()),
                        Some("demo.globals")
                    );
                    assert_eq!(frame.context.origin.plugin.as_deref(), Some("demo"));
                    let value: f64 = frame.args.read()?;
                    frame.returns.write(value + 1.0)?;
                    Ok(())
                }),
        );

        install_luau_globals(&vm, &origin, &globals)?;
        assert_eq!(
            vm.eval(
                Arc::<[u8]>::from(&b"return demo_global(41)"[..]),
                luau::ChunkOrigin::default(),
            )?,
            vec![luau::Value::Number(42.0)]
        );
        Ok(())
    }

    #[test]
    fn registry_requires_luau_modules_with_capability_check_before_cache()
    -> luau::runtime::Result<()> {
        let module_id = ModuleId(Arc::from("demo/math"));
        let vm = luau::Vm::new()?;
        let mut registry = ModuleRegistry::new();
        registry
            .register(
                ModuleSpec::new(module_id.0.clone())
                    .capability("demo.math")
                    .function(FunctionSpec::sync_fn("answer").call(|mut frame| {
                        frame.returns.write(42_i64)?;
                        Ok(())
                    })),
            )
            .expect("register module");

        let allowed_origin = ChunkOrigin {
            plugin: Some(Arc::from("allowed")),
            ..ChunkOrigin::default()
        };
        let first = registry
            .require_luau_module(
                &vm,
                &allowed_origin,
                &module_id,
                &PluginPolicy {
                    allowed_plugin: "allowed",
                },
            )
            .expect("allowed require");
        let second = registry
            .require_luau_module(
                &vm,
                &allowed_origin,
                &module_id,
                &PluginPolicy {
                    allowed_plugin: "allowed",
                },
            )
            .expect("cached require");
        assert!(Arc::ptr_eq(&first, &second));
        vm.set_global_table("module", &first)?;
        assert_eq!(
            vm.eval(
                Arc::<[u8]>::from(&b"return module.answer()"[..]),
                luau::ChunkOrigin::default(),
            )?,
            vec![luau::Value::Integer(42)]
        );

        let denied_origin = ChunkOrigin {
            plugin: Some(Arc::from("denied")),
            ..ChunkOrigin::default()
        };
        let denied = registry.require_luau_module(
            &vm,
            &denied_origin,
            &module_id,
            &PluginPolicy {
                allowed_plugin: "allowed",
            },
        );
        let Err(error) = denied else {
            panic!("capability denial must run before cached return");
        };
        assert!(matches!(error, ModuleLoadError::CapabilityDenied(_)));
        assert!(registry.invalidate(&module_id));
        Ok(())
    }

    #[test]
    fn registry_caches_luau_modules_per_plugin_origin() -> luau::runtime::Result<()> {
        let module_id = ModuleId(Arc::from("lyra/plugins"));
        let vm = luau::Vm::new()?;
        let mut registry = ModuleRegistry::new();
        registry
            .register(ModuleSpec::new(module_id.0.clone()).function(
                FunctionSpec::sync_fn("plugin_id").call(|mut frame| {
                    let plugin_id = frame.context.origin.plugin.as_deref().unwrap_or("");
                    frame.returns.write(plugin_id)?;
                    Ok(())
                }),
            ))
            .expect("register module");

        let alpha_origin = ChunkOrigin {
            plugin: Some(Arc::from("alpha")),
            ..ChunkOrigin::default()
        };
        let beta_origin = ChunkOrigin {
            plugin: Some(Arc::from("beta")),
            ..ChunkOrigin::default()
        };

        let alpha_first = registry
            .require_luau_module(&vm, &alpha_origin, &module_id, &AllowAllCapabilities)
            .expect("alpha require");
        let alpha_second = registry
            .require_luau_module(&vm, &alpha_origin, &module_id, &AllowAllCapabilities)
            .expect("alpha cached require");
        let beta = registry
            .require_luau_module(&vm, &beta_origin, &module_id, &AllowAllCapabilities)
            .expect("beta require");

        assert!(Arc::ptr_eq(&alpha_first, &alpha_second));
        assert!(!Arc::ptr_eq(&alpha_first, &beta));

        vm.set_global_table("alpha", &alpha_first)?;
        vm.set_global_table("beta", &beta)?;
        assert_eq!(
            vm.eval(
                Arc::<[u8]>::from(&b"return alpha.plugin_id(), beta.plugin_id()"[..]),
                luau::ChunkOrigin::default(),
            )?,
            vec![
                luau::Value::String(b"alpha".to_vec()),
                luau::Value::String(b"beta".to_vec()),
            ]
        );
        Ok(())
    }

    #[test]
    fn source_cache_executes_resolved_luau_source_by_typed_cache_key() -> luau::runtime::Result<()>
    {
        let vm = luau::Vm::new()?;
        let mut loader = MemorySourceLoader::new();
        loader.insert("plugin:demo/lib/util.luau", b"return 40 + 2".as_slice());
        let origin = ChunkOrigin {
            plugin: Some(Arc::from("demo")),
            path: Some(Arc::from("plugins/demo/init.luau")),
            ..ChunkOrigin::default()
        };
        let mut cache = LuauSourceCache::new();

        let first = cache
            .require(&vm, &loader, "@self/lib/util.luau", &origin)
            .expect("first require");
        assert_eq!(first.as_ref(), [luau::Value::Number(42.0)]);

        loader.insert("plugin:demo/lib/util.luau", b"return 99".as_slice());
        let second = cache
            .require(&vm, &loader, "@self/lib/util.luau", &origin)
            .expect("cached require");
        assert!(Arc::ptr_eq(&first, &second));

        assert!(cache.invalidate(&ModuleCacheKey(Arc::from("plugin:demo/lib/util.luau"))));
        let reloaded = cache
            .require(&vm, &loader, "@self/lib/util.luau", &origin)
            .expect("reloaded require");
        assert_eq!(reloaded.as_ref(), [luau::Value::Number(99.0)]);
        Ok(())
    }

    #[test]
    fn source_cache_preserves_module_table_exports() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        let mut loader = MemorySourceLoader::new();
        loader.insert(
            "plugin:demo/lib/module.luau",
            b"return { answer = 42, get = function() return 7 end }".as_slice(),
        );
        let origin = ChunkOrigin {
            plugin: Some(Arc::from("demo")),
            path: Some(Arc::from("plugins/demo/init.luau")),
            ..ChunkOrigin::default()
        };
        let mut cache = LuauSourceCache::new();

        let values = cache
            .require(&vm, &loader, "@self/lib/module.luau", &origin)
            .expect("module require");
        let [luau::Value::Table(module)] = values.as_ref() else {
            panic!("module source should return its table export");
        };
        assert_eq!(module.get_raw(&vm, "answer")?, luau::Value::Number(42.0));
        let luau::Value::Function(get) = module.get_raw(&vm, "get")? else {
            panic!("module table should preserve function exports");
        };
        assert_eq!(get.call(&vm, &[])?, vec![luau::Value::Number(7.0)]);
        Ok(())
    }

    #[test]
    fn luau_require_global_loads_registered_and_source_modules() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        let mut loader = MemorySourceLoader::new();
        loader.insert(
            "plugin:demo/lib/util.luau",
            b"return { source_answer = 7 }".as_slice(),
        );
        let origin = ChunkOrigin {
            plugin: Some(Arc::from("demo")),
            path: Some(Arc::from("plugins/demo/init.luau")),
            ..ChunkOrigin::default()
        };
        let runtime = LuauRequireRuntime::new(loader, AllowAllCapabilities);
        runtime
            .register(
                ModuleSpec::new("harmony/math")
                    .capability("harmony.math")
                    .function(FunctionSpec::sync_fn("answer").call(|mut frame| {
                        frame.returns.write(42_i64)?;
                        Ok(())
                    })),
            )
            .expect("register module");
        vm.data().insert(runtime)?;
        install_luau_require(&vm, &origin)?;

        let values = vm.eval(
            Arc::<[u8]>::from(
                &br#"
                    local math = require("@harmony/math")
                    local util = require("@self/lib/util.luau")
                    return math.answer(), util.source_answer
                "#[..],
            ),
            luau::ChunkOrigin::default(),
        )?;

        assert_eq!(
            values,
            vec![luau::Value::Integer(42), luau::Value::Number(7.0)]
        );
        Ok(())
    }

    #[test]
    fn capability_check_runs_before_cache_return() {
        let module_id = ModuleId(Arc::from("lyra/datastore"));
        let mut registry = ModuleRegistry::new();
        registry
            .register(
                ModuleSpec::new(module_id.0.clone())
                    .capability("lyra.datastore")
                    .install(|_| Ok(ModuleExport::new("cached"))),
            )
            .expect("register module");

        let allowed_origin = ChunkOrigin {
            plugin: Some(Arc::from("allowed")),
            ..ChunkOrigin::default()
        };
        registry
            .require(
                &allowed_origin,
                &module_id,
                &PluginPolicy {
                    allowed_plugin: "allowed",
                },
            )
            .expect("allowed require warms cache");

        let denied_origin = ChunkOrigin {
            plugin: Some(Arc::from("denied")),
            ..ChunkOrigin::default()
        };
        let error = registry
            .require(
                &denied_origin,
                &module_id,
                &PluginPolicy {
                    allowed_plugin: "allowed",
                },
            )
            .expect_err("denied require must not use cache");

        assert!(matches!(error, ModuleLoadError::CapabilityDenied(_)));
        assert!(error.to_string().contains("lyra.datastore"));
    }

    #[test]
    fn source_loader_resolves_self_alias_with_typed_plugin_origin() {
        let mut loader = MemorySourceLoader::new();
        loader.insert("plugin:demo/lib/util.luau", b"return 1".as_slice());
        let origin = ChunkOrigin {
            plugin: Some(Arc::from("demo")),
            path: Some(Arc::from("plugins/demo/init.luau")),
            ..ChunkOrigin::default()
        };

        let resolved = loader
            .resolve(SourceRequest {
                specifier: "@self/lib/util.luau",
                origin: &origin,
            })
            .expect("resolve @self");

        assert_eq!(resolved.cache_key.0.as_ref(), "plugin:demo/lib/util.luau");
        assert_eq!(resolved.origin.plugin.as_deref(), Some("demo"));
        assert_eq!(
            resolved.origin.path.as_deref(),
            Some("plugins/demo/lib/util.luau")
        );
    }

    #[test]
    fn source_loader_resolves_relative_paths_without_source_parsing() {
        let mut loader = MemorySourceLoader::new();
        loader.insert("plugin:demo/lib/shared.luau", b"return 2".as_slice());
        let origin = ChunkOrigin {
            plugin: Some(Arc::from("demo")),
            path: Some(Arc::from("plugins/demo/pages/home.luau")),
            ..ChunkOrigin::default()
        };

        let resolved = loader
            .resolve(SourceRequest {
                specifier: "../lib/shared.luau",
                origin: &origin,
            })
            .expect("resolve relative path");

        assert_eq!(resolved.cache_key.0.as_ref(), "plugin:demo/lib/shared.luau");
        assert_eq!(
            resolved.origin.path.as_deref(),
            Some("plugins/demo/lib/shared.luau")
        );
    }

    #[test]
    fn filesystem_source_loader_reads_plugin_sources_by_typed_cache_key()
    -> luau::runtime::Result<()> {
        let test_dir = std::env::temp_dir().join(format!(
            "harmony-core-fs-loader-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time before epoch")
                .as_nanos()
        ));
        let plugin_dir = test_dir.join("plugins").join("demo").join("lib");
        std::fs::create_dir_all(&plugin_dir).expect("create plugin source dir");
        std::fs::write(plugin_dir.join("util.luau"), b"return 40 + 2")
            .expect("write plugin source");

        let loader = FilesystemSourceLoader::new(test_dir.join("root"), test_dir.join("plugins"));
        let origin = ChunkOrigin {
            plugin: Some(Arc::from("demo")),
            path: Some(Arc::from("plugins/demo/init.luau")),
            ..ChunkOrigin::default()
        };
        let vm = luau::Vm::new()?;
        let mut cache = LuauSourceCache::new();

        let values = cache
            .require(&vm, &loader, "@self/lib/util.luau", &origin)
            .expect("load plugin source from filesystem");

        assert_eq!(values.as_ref(), [luau::Value::Number(42.0)]);
        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[test]
    fn source_loader_resolves_root_alias_to_module_origin() {
        let mut loader = MemorySourceLoader::new();
        loader.insert("alias:harmony/json", b"return {}".as_slice());

        let resolved = loader
            .resolve(SourceRequest {
                specifier: "@harmony/json",
                origin: &ChunkOrigin::default(),
            })
            .expect("resolve alias");

        assert_eq!(resolved.cache_key.0.as_ref(), "alias:harmony/json");
        assert_eq!(
            resolved
                .origin
                .module
                .as_ref()
                .map(|module| module.0.as_ref()),
            Some("harmony/json")
        );
    }
}
