use std::{
    any::Any,
    collections::{
        HashMap,
        HashSet,
    },
    fmt,
    fs,
    path::{
        Component,
        Path,
        PathBuf,
    },
    rc::Rc,
    sync::Arc,
    task::{
        Context,
        Poll,
        Waker,
    },
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

pub type ModuleInitializer = Arc<
    dyn Fn(&luau::Vm, &ChunkOrigin, &luau::Table) -> luau::runtime::Result<()>
        + Send
        + Sync
        + 'static,
>;
pub type UserDataInitializer = Arc<
    dyn Fn(&luau::Vm, &luau::ChunkOrigin, &luau::Table) -> luau::runtime::Result<()>
        + Send
        + Sync
        + 'static,
>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ModuleCacheKey(pub Arc<str>);

pub trait CapabilityPolicy {
    fn is_allowed(&self, origin: &ChunkOrigin, capability: &CapabilityId) -> bool;

    /// Whether `requester` may require Luau sources from plugin `target`.
    fn allows_plugin_dependency(&self, requester: &str, target: &str) -> bool {
        let _ = (requester, target);
        true
    }
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
        if request.specifier.starts_with("./") || request.specifier.starts_with("../") {
            let base_path = request
                .origin
                .path
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("relative require needs source path origin"))?;
            let parent = Path::new(base_path)
                .parent()
                .unwrap_or_else(|| Path::new(""));
            let path = canonical_module_path(parent.join(request.specifier))?;
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

        if let Some(target) = request.specifier.strip_prefix('@') {
            let (plugin_id, module_path) = match target.split_once('/') {
                Some((plugin_id, module_path)) => (plugin_id, module_path),
                None => (target, "init"),
            };
            let plugin_id = if plugin_id == "self" {
                request
                    .origin
                    .plugin
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("@self require needs plugin origin"))?
            } else {
                plugin_id
            };
            if let Err(reason) = crate::plugin::validate_plugin_id(plugin_id) {
                anyhow::bail!("invalid plugin id in require '@{target}': {reason}");
            }
            return plugin_source_key(plugin_id, module_path);
        }

        if let Some(plugin) = request.origin.plugin.as_deref() {
            anyhow::bail!(
                "plugin '{plugin}' require '{}' must start with './', '../', '@self', or '@<plugin-id>'",
                request.specifier
            );
        }

        let path = canonical_module_path(request.specifier)?;
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

/// A module and the capability a plugin's manifest must declare to `require`
/// it. The host decides the capability; the module only describes itself.
pub struct GatedModule {
    pub capability: CapabilityId,
    pub spec: ModuleSpec,
}

impl GatedModule {
    pub fn new(capability: impl Into<Arc<str>>, spec: ModuleSpec) -> Self {
        Self {
            capability: CapabilityId(capability.into()),
            spec,
        }
    }
}

pub struct ModuleSpec {
    pub id: ModuleId,
    pub functions: Vec<FunctionSpec>,
    pub userdata: Vec<UserDataSpec>,
    pub install: Option<NativeModuleInstaller>,
    pub initializer: Option<ModuleInitializer>,
}

impl ModuleSpec {
    pub fn new(id: impl Into<Arc<str>>) -> Self {
        Self {
            id: ModuleId(id.into()),
            functions: Vec::new(),
            userdata: Vec::new(),
            install: None,
            initializer: None,
        }
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

    pub fn initializer<F>(mut self, initializer: F) -> Self
    where
        F: Fn(&luau::Vm, &ChunkOrigin, &luau::Table) -> luau::runtime::Result<()>
            + Send
            + Sync
            + 'static,
    {
        self.initializer = Some(Arc::new(initializer));
        self
    }
}

#[derive(Clone)]
pub struct UserDataSpec {
    pub name: Arc<str>,
    pub initializer: Option<UserDataInitializer>,
}

impl fmt::Debug for UserDataSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UserDataSpec")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl UserDataSpec {
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        Self {
            name: name.into(),
            initializer: None,
        }
    }

    pub fn initializer<F>(mut self, initializer: F) -> Self
    where
        F: Fn(&luau::Vm, &luau::ChunkOrigin, &luau::Table) -> luau::runtime::Result<()>
            + Send
            + Sync
            + 'static,
    {
        self.initializer = Some(Arc::new(initializer));
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
    pub arg_names: Vec<Arc<str>>,
    pub callback: Option<FunctionCallback>,
}

impl FunctionSpec {
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        Self {
            name: name.into(),
            arg_names: Vec::new(),
            callback: None,
        }
    }

    pub fn arg_name(mut self, name: impl Into<Arc<str>>) -> Self {
        self.arg_names.push(name.into());
        self
    }

    pub fn call<F>(mut self, callback: F) -> Self
    where
        F: for<'vm> Fn(luau::CallFrame<'vm>) -> luau::runtime::Result<()> + Send + Sync + 'static,
    {
        self.callback = Some(FunctionCallback::Sync(Arc::new(callback)));
        self
    }

    pub fn call_async(mut self, callback: luau::NativeAsyncFn) -> Self {
        self.callback = Some(FunctionCallback::Async(callback));
        self
    }
}

#[derive(Debug)]
pub enum ModuleLoadError {
    DuplicateModule {
        module_id: ModuleId,
    },
    UnknownModule {
        module_id: ModuleId,
    },
    MissingInstaller {
        module_id: ModuleId,
    },
    CapabilityDenied(ModuleCapabilityDenied),
    InstallFailed(anyhow::Error),
    SourceResolveFailed(anyhow::Error),
    SourceLoadFailed(anyhow::Error),
    UnknownSource {
        cache_key: ModuleCacheKey,
    },
    PluginDependencyDenied {
        requester: Arc<str>,
        target: Arc<str>,
    },
    RequireCycle {
        cache_key: ModuleCacheKey,
    },
    PluginNotRunning {
        plugin_id: Arc<str>,
    },
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
            Self::PluginDependencyDenied { requester, target } => {
                write!(
                    f,
                    "plugin '{requester}' requires plugin '{target}', which is not a loaded \
                     plugin it declares as a dependency in plugin.json"
                )
            }
            Self::RequireCycle { cache_key } => {
                write!(f, "require cycle detected at '{}'", cache_key.0)
            }
            Self::PluginNotRunning { plugin_id } => {
                write!(f, "plugin '{plugin_id}' is not running")
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

/// Canonical logical module path for cache keys: normalized, with the
/// implicit `.luau` extension stripped so `foo` and `foo.luau` share one
/// cache entry (the filesystem loader re-appends it on read).
fn canonical_module_path(path: impl AsRef<Path>) -> Result<String> {
    let path = normalize_logical_path(path)?;
    Ok(match path.strip_suffix(".luau") {
        Some(stem) => stem.to_owned(),
        None => path,
    })
}

fn plugin_source_key(
    plugin: &str,
    path: impl AsRef<Path>,
) -> Result<(ModuleCacheKey, ChunkOrigin)> {
    let path = canonical_module_path(path)?;
    let origin = ChunkOrigin {
        plugin: Some(Arc::from(plugin)),
        path: Some(Arc::from(format!("plugins/{plugin}/{path}"))),
        ..ChunkOrigin::default()
    };
    Ok((
        ModuleCacheKey(Arc::from(format!("plugin:{plugin}/{path}"))),
        origin,
    ))
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
    modules: HashMap<ModuleId, GatedModule>,
    cache: HashMap<ModuleId, Arc<ModuleExport>>,
    luau_cache: HashMap<LuauModuleCacheKey, Rc<luau::Table>>,
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
struct LuauSourceCache {
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

    /// Drops every cached source under `plugin:<plugin_id>/`.
    pub fn invalidate_plugin(&mut self, plugin_id: &str) -> usize {
        let prefix = format!("plugin:{plugin_id}/");
        let before = self.cache.len();
        self.cache.retain(|key, _| !key.0.starts_with(&prefix));
        before - self.cache.len()
    }
}

impl ModuleRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn contains(&self, module_id: &ModuleId) -> bool {
        self.modules.contains_key(module_id)
    }

    /// Whether any registered module lives under `namespace` (the first
    /// `/`-separated component of its id). Registered namespaces are
    /// reserved: they never fall through to plugin source resolution.
    pub fn contains_namespace(&self, namespace: &str) -> bool {
        self.modules.keys().any(|module_id| {
            let id = module_id.0.as_ref();
            id == namespace
                || id
                    .strip_prefix(namespace)
                    .is_some_and(|rest| rest.starts_with('/'))
        })
    }

    pub fn register(&mut self, module: GatedModule) -> std::result::Result<(), ModuleLoadError> {
        if self.modules.contains_key(&module.spec.id) {
            return Err(ModuleLoadError::DuplicateModule {
                module_id: module.spec.id.clone(),
            });
        }

        self.modules.insert(module.spec.id.clone(), module);
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

        if !policy.is_allowed(origin, &module.capability) {
            return Err(ModuleLoadError::CapabilityDenied(ModuleCapabilityDenied {
                origin: origin.clone(),
                module_id: module_id.clone(),
                capability: module.capability.clone(),
            }));
        }

        if let Some(export) = self.cache.get(module_id) {
            return Ok(export.clone());
        }

        let installer =
            module
                .spec
                .install
                .as_ref()
                .ok_or_else(|| ModuleLoadError::MissingInstaller {
                    module_id: module_id.clone(),
                })?;
        let export = Arc::new(
            installer(ModuleLoadContext {
                origin,
                module: &module.spec,
            })
            .map_err(ModuleLoadError::InstallFailed)?,
        );
        self.cache.insert(module_id.clone(), export.clone());
        Ok(export)
    }

    /// `task_group` is the run of the plugin `origin` belongs to; the
    /// module's functions act in it when another plugin calls them.
    pub fn require_luau_module(
        &mut self,
        vm: &luau::Vm,
        origin: &ChunkOrigin,
        module_id: &ModuleId,
        policy: &dyn CapabilityPolicy,
        task_group: crate::TaskGroupId,
    ) -> std::result::Result<Rc<luau::Table>, ModuleLoadError> {
        let module = self
            .modules
            .get(module_id)
            .ok_or_else(|| ModuleLoadError::UnknownModule {
                module_id: module_id.clone(),
            })?;

        if !policy.is_allowed(origin, &module.capability) {
            return Err(ModuleLoadError::CapabilityDenied(ModuleCapabilityDenied {
                origin: origin.clone(),
                module_id: module_id.clone(),
                capability: module.capability.clone(),
            }));
        }

        let cache_key = LuauModuleCacheKey::new(module_id, origin);
        if let Some(export) = self.luau_cache.get(&cache_key) {
            return Ok(export.clone());
        }

        let export = Rc::new(
            install_gated_luau_module(
                vm,
                origin,
                &module.spec,
                Some(&module.capability),
                task_group,
            )
            .map_err(|error| ModuleLoadError::InstallFailed(anyhow::Error::new(error)))?,
        );
        self.luau_cache.insert(cache_key, export.clone());
        Ok(export)
    }

    pub fn invalidate(&mut self, module_id: &ModuleId) -> bool {
        let removed = self.cache.remove(module_id).is_some();
        let before = self.luau_cache.len();
        self.luau_cache.retain(|key, _| key.module_id != *module_id);
        self.luau_cache.len() != before || removed
    }

    /// Drops the Luau module tables installed for `plugin_id`'s origin; the
    /// shared native exports in `cache` are not plugin-owned and stay.
    pub fn invalidate_plugin(&mut self, plugin_id: &str) -> usize {
        let before = self.luau_cache.len();
        self.luau_cache
            .retain(|key, _| key.plugin.as_deref() != Some(plugin_id));
        before - self.luau_cache.len()
    }
}

pub struct LuauRequireRuntime {
    registry: std::cell::RefCell<ModuleRegistry>,
    source_cache: std::cell::RefCell<LuauSourceCache>,
    /// Bumped per plugin by `invalidate_plugin`; a source whose plugin was
    /// invalidated while it evaluated must not land in `source_cache`.
    plugin_generations: std::cell::RefCell<HashMap<Arc<str>, u64>>,
    in_flight_sources: std::cell::RefCell<HashSet<Arc<str>>>,
    /// The task group of each plugin's current run. Only the host starts a
    /// run; a plugin's sources load only while it has one.
    plugin_groups: std::cell::RefCell<HashMap<Arc<str>, crate::TaskGroupId>>,
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
            plugin_generations: std::cell::RefCell::new(HashMap::new()),
            in_flight_sources: std::cell::RefCell::new(HashSet::new()),
            plugin_groups: std::cell::RefCell::new(HashMap::new()),
            loader: Box::new(loader),
            policy: Box::new(policy),
        }
    }

    pub fn register(&self, module: GatedModule) -> std::result::Result<(), ModuleLoadError> {
        self.registry.borrow_mut().register(module)
    }

    /// The task group of `plugin_id`'s current run, starting a run when it
    /// has none. Only the host calls this; a require never starts a run.
    pub fn plugin_group(
        &self,
        scheduler: &crate::LocalScheduler,
        plugin_id: &str,
    ) -> crate::TaskGroupId {
        *self
            .plugin_groups
            .borrow_mut()
            .entry(Arc::from(plugin_id))
            .or_insert_with(|| scheduler.create_group())
    }

    /// Ends `plugin_id`'s current run, returning its task group.
    pub fn end_plugin_run(&self, plugin_id: &str) -> Option<crate::TaskGroupId> {
        self.plugin_groups.borrow_mut().remove(plugin_id)
    }

    fn current_plugin_group(&self, plugin_id: &str) -> Option<crate::TaskGroupId> {
        self.plugin_groups.borrow().get(plugin_id).copied()
    }

    /// Whether `origin` holds `capability`, by the policy that gates its
    /// requires. Lets a module gate part of what it offers the same way.
    pub fn allows(&self, origin: &ChunkOrigin, capability: &str) -> bool {
        self.policy
            .is_allowed(origin, &CapabilityId(Arc::from(capability)))
    }

    /// Forgets every evaluated source and installed module table owned by
    /// `plugin_id`, so its next load resolves through the loader again.
    pub fn invalidate_plugin(&self, plugin_id: &str) {
        self.source_cache.borrow_mut().invalidate_plugin(plugin_id);
        self.registry.borrow_mut().invalidate_plugin(plugin_id);
        *self
            .plugin_generations
            .borrow_mut()
            .entry(Arc::from(plugin_id))
            .or_default() += 1;
    }

    fn plugin_generation(&self, plugin: Option<&str>) -> u64 {
        plugin
            .and_then(|plugin| self.plugin_generations.borrow().get(plugin).copied())
            .unwrap_or_default()
    }
}

/// The `require` plugin code gets, as a global of its chunk's own
/// environment: it resolves from `origin` and, like the source's other
/// functions, acts for the run it was made for, even when another plugin
/// calls it. There is no shared global `require`.
fn bound_require(
    vm: &luau::Vm,
    origin: &ChunkOrigin,
    task_group: crate::TaskGroupId,
) -> luau::runtime::Result<luau::Function> {
    vm.create_function_with_options(
        luau::NativeFunctionOptions::new(origin.clone())
            .function_name("require")
            .argument_names([Arc::from("specifier")])
            .task_group(task_group),
        Arc::new(|mut frame| {
            let specifier: String = frame.args.read_named("specifier")?;
            let origin = frame.context.origin.clone();
            require_from(frame, &specifier, origin)
        }),
    )
}

/// Fails for a call from a plugin run that has ended, such as one through
/// an export a restarted plugin left behind in another plugin.
pub fn ensure_current_plugin_run(
    vm: &luau::Vm,
    context: &luau::CallContext,
) -> luau::runtime::Result<()> {
    let Some(plugin_id) = context.origin.plugin.as_deref() else {
        return Ok(());
    };
    let current = vm
        .data()
        .get::<LuauRequireRuntime>()?
        .current_plugin_group(plugin_id);
    if current == Some(context.task_group) {
        return Ok(());
    }
    Err(luau::Error::Runtime(format!(
        "plugin '{plugin_id}' code from a run that has ended can't act for its current run"
    )))
}

/// Loads a chunk of `origin`'s code with its own `require`. All plugin code
/// is loaded through here. The `require` acts in `task_group`, so the caller
/// runs the chunk in that group too.
pub(crate) fn load_chunk_with_require(
    vm: &luau::Vm,
    source: impl Into<Arc<[u8]>>,
    origin: &ChunkOrigin,
    task_group: crate::TaskGroupId,
) -> luau::runtime::Result<luau::Function> {
    let require = luau::Value::Function(bound_require(vm, origin, task_group)?);
    vm.load_chunk_with_globals(
        &luau::Chunk::new(source, origin.clone()),
        &[("require", &require)],
    )
}

/// A require acts for the run of the code that makes it, so code from a run
/// that has ended can load nothing, the same rule its registrations follow.
/// Past that check, a plugin origin's context group is its current run.
fn require_from(
    mut frame: luau::CallFrame<'_>,
    specifier: &str,
    origin: ChunkOrigin,
) -> luau::runtime::Result<()> {
    ensure_current_plugin_run(frame.vm, &frame.context)?;
    let runtime = frame.vm.data().get::<LuauRequireRuntime>()?;

    if let Some(module_id) = specifier
        .strip_prefix('@')
        .map(|path| ModuleId(Arc::from(path)))
        .filter(|module_id| runtime.registry.borrow().contains(module_id))
    {
        let task_group = match origin.plugin {
            Some(_) => frame.context.task_group,
            None => crate::TaskGroupId::default(),
        };
        let module = runtime
            .registry
            .borrow_mut()
            .require_luau_module(
                frame.vm,
                &origin,
                &module_id,
                runtime.policy.as_ref(),
                task_group,
            )
            .map_err(module_load_runtime_error)?;
        frame.returns.write((*module).clone())?;
        return Ok(());
    }

    let values = runtime
        .require_source(frame.vm, specifier, &origin, &frame.context)
        .map_err(module_load_runtime_error)?;
    for value in values.iter().cloned() {
        frame.returns.write(value)?;
    }
    Ok(())
}

impl LuauRequireRuntime {
    /// Evaluates a source in its plugin's current run, with the requiring
    /// call's caller data. A source outside any plugin is shared, so it runs
    /// in the default group rather than its first requester's.
    fn require_source(
        &self,
        vm: &luau::Vm,
        specifier: &str,
        origin: &ChunkOrigin,
        requester: &luau::CallContext,
    ) -> std::result::Result<Arc<[luau::Value]>, ModuleLoadError> {
        self.load_source_cached(specifier, origin, |source, task_group| {
            let function =
                load_chunk_with_require(vm, source.bytes.clone(), &source.origin, task_group)?;
            function
                .call_with_context(
                    vm,
                    &[],
                    luau::CallContext {
                        origin: source.origin.clone(),
                        capability: None,
                        caller: requester.caller.clone(),
                        task_group,
                    },
                )
                .map_err(anyhow::Error::new)
        })
    }

    /// Resolves, gates, and caches a Luau source module; `eval` supplies the
    /// evaluation strategy (a direct call for require, a scheduler-driven
    /// thread for plugin entrypoints) so both share one cache identity. A
    /// plugin's source loads only during its current run, which `eval` gets
    /// as the group to evaluate it in, cached or not.
    pub(crate) fn load_source_cached(
        &self,
        specifier: &str,
        origin: &ChunkOrigin,
        eval: impl FnOnce(&ResolvedSource, crate::TaskGroupId) -> anyhow::Result<Vec<luau::Value>>,
    ) -> std::result::Result<Arc<[luau::Value]>, ModuleLoadError> {
        // Registered module namespaces are reserved: `@<ns>/typo` must fail
        // as an unknown module instead of resolving to a plugin named `<ns>`.
        if let Some(target) = specifier.strip_prefix('@') {
            let namespace = target.split('/').next().unwrap_or(target);
            if namespace != "self" && self.registry.borrow().contains_namespace(namespace) {
                return Err(ModuleLoadError::UnknownModule {
                    module_id: ModuleId(Arc::from(target)),
                });
            }
        }

        let source = self.loader.resolve(SourceRequest { specifier, origin })?;

        // Dependency gate runs before the cache so an undeclared dependency
        // fails even when another plugin already materialized the module.
        if let (Some(requester), Some(target)) =
            (origin.plugin.as_deref(), source.origin.plugin.as_deref())
            && requester != target
            && !self.policy.allows_plugin_dependency(requester, target)
        {
            return Err(ModuleLoadError::PluginDependencyDenied {
                requester: Arc::from(requester),
                target: Arc::from(target),
            });
        }

        let task_group = match source.origin.plugin.as_deref() {
            Some(plugin_id) => self.current_plugin_group(plugin_id).ok_or_else(|| {
                ModuleLoadError::PluginNotRunning {
                    plugin_id: Arc::from(plugin_id),
                }
            })?,
            None => crate::TaskGroupId::default(),
        };

        if let Some(values) = self.source_cache.borrow().get(&source.cache_key) {
            return Ok(values);
        }

        if !self
            .in_flight_sources
            .borrow_mut()
            .insert(source.cache_key.0.clone())
        {
            return Err(ModuleLoadError::RequireCycle {
                cache_key: source.cache_key,
            });
        }

        let generation = self.plugin_generation(source.origin.plugin.as_deref());
        let result = eval(&source, task_group);
        self.in_flight_sources
            .borrow_mut()
            .remove(&source.cache_key.0);
        let values = result.map_err(ModuleLoadError::SourceLoadFailed)?;
        let values = Arc::<[luau::Value]>::from(values.into_boxed_slice());
        if generation == self.plugin_generation(source.origin.plugin.as_deref()) {
            self.source_cache
                .borrow_mut()
                .insert(source.cache_key, values.clone());
        }
        Ok(values)
    }
}

fn module_load_runtime_error(error: ModuleLoadError) -> luau::Error {
    luau::Error::Runtime(error.to_string())
}

/// Installs `module` directly, outside any registry, so its functions carry
/// no capability.
pub fn install_luau_module(
    vm: &luau::Vm,
    origin: &ChunkOrigin,
    module: &ModuleSpec,
) -> luau::runtime::Result<luau::Table> {
    install_gated_luau_module(vm, origin, module, None, crate::TaskGroupId::default())
}

fn install_gated_luau_module(
    vm: &luau::Vm,
    origin: &ChunkOrigin,
    module: &ModuleSpec,
    capability_id: Option<&CapabilityId>,
    task_group: crate::TaskGroupId,
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

    fn function_origin(origin: &ChunkOrigin, module_id: &ModuleId) -> ChunkOrigin {
        ChunkOrigin {
            module: Some(module_id.clone()),
            ..origin.clone()
        }
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
            table_for(&root, &nested, parent).set(segment, &table)?;
            let index = nested.len();
            nested.push((key.clone(), table));
            nested_indices.insert(key.clone(), index);
            parent = TableHandle::Nested(index);
        }

        let mut options = luau::NativeFunctionOptions::new(function_origin(origin, &module.id))
            .function_name(spec.name.clone())
            .argument_names(spec.arg_names.clone())
            .task_group(task_group);
        if let Some(capability_id) = capability_id {
            options = options.capability(capability_id.clone());
        }
        let callback = match callback {
            FunctionCallback::Sync(callback) => callback,
            FunctionCallback::Async(callback) => async_luau_callback(callback),
        };
        let function = vm.create_function_with_options(options, callback)?;
        table_for(&root, &nested, parent).set(leaf, &function)?;
    }

    let module_origin = function_origin(origin, &module.id);
    for spec in &module.userdata {
        if let Some(initializer) = &spec.initializer {
            initializer(vm, &module_origin, &root)?;
        }
    }

    if let Some(initializer) = &module.initializer {
        initializer(vm, origin, &root)?;
    }

    for (_, table) in &nested {
        table.set_readonly(true)?;
    }
    root.set_readonly(true)?;
    Ok(root)
}

pub fn async_luau_callback(callback: luau::NativeAsyncFn) -> luau::NativeFn {
    Arc::new(move |frame| {
        let vm = frame.vm.clone();
        let thread = frame.thread.clone();
        let context = frame.context.clone();
        let scheduler = frame.vm.data().get::<crate::LocalScheduler>()?;
        // Refused before the thread parks: nothing would resume it.
        if scheduler.is_group_cancelled(context.task_group) {
            return Err(luau::Error::Runtime(
                "this call acts for a plugin run that has ended".to_string(),
            ));
        }
        let mut returns = frame.returns.clone();
        let site = frame.site.clone();
        let mut future = callback(frame.into_async())?;
        if let Some(delay) = future.take_start_delay() {
            ensure_yieldable(&thread)?;
            returns.request_yield();
            scheduler.park_luau_thread(&thread);
            scheduler.schedule_luau_future_after(context, delay, vm, thread, future.for_call(site));
            return Ok(());
        }
        match poll_luau_future_once(&mut future) {
            Poll::Ready(Ok(values)) => values.write_to(&mut returns)?,
            Poll::Ready(Err(error)) => return Err(error),
            Poll::Pending => {
                ensure_yieldable(&thread)?;
                returns.request_yield();
                scheduler.park_luau_thread(&thread);
                scheduler.schedule_luau_future(context, vm, thread, future.for_call(site));
            }
        }
        Ok(())
    })
}

/// A call that must wait fails here when its thread can't yield, before
/// anything is parked or scheduled for it.
fn ensure_yieldable(thread: &luau::Thread) -> luau::runtime::Result<()> {
    if thread.is_yieldable() {
        return Ok(());
    }
    Err(luau::Error::Runtime(
        "attempt to yield from a non-yieldable Harmony callback".to_string(),
    ))
}

fn poll_luau_future_once(
    future: &mut luau::ScheduledFuture,
) -> Poll<luau::runtime::Result<luau::ReturnValues>> {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    std::pin::Pin::new(future).poll(&mut cx)
}

pub fn install_luau_globals(
    vm: &luau::Vm,
    origin: &ChunkOrigin,
    globals: &GlobalSpec,
) -> luau::runtime::Result<()> {
    fn function_origin(origin: &ChunkOrigin, module_id: &str) -> ChunkOrigin {
        ChunkOrigin {
            module: Some(ModuleId(Arc::from(module_id))),
            ..origin.clone()
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

#[cfg(test)]
mod tests {
    use super::*;

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
        let module_id = ModuleId(Arc::from("harmony/serde"));
        let mut registry = ModuleRegistry::new();
        registry
            .register(GatedModule::new(
                "harmony.serde",
                ModuleSpec::new(module_id.0.clone()).install(|_| Ok(ModuleExport::new(42_u64))),
            ))
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
        let module =
            ModuleSpec::new("demo/math")
                .function(
                    FunctionSpec::new("add")
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
                .function(FunctionSpec::new("nested.double").arg_name("value").call(
                    |mut frame| {
                        let value: f64 = frame.args.read()?;
                        frame.returns.write(value * 2.0)?;
                        Ok(())
                    },
                ));
        let capability = CapabilityId(Arc::from("demo.math"));
        let table = install_gated_luau_module(
            &vm,
            &origin,
            &module,
            Some(&capability),
            crate::TaskGroupId::default(),
        )?;
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
    fn luau_module_installer_completes_ready_async_callbacks_without_yielding()
    -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        vm.data().insert(crate::LocalScheduler::new())?;
        let scheduler = vm.data().get::<crate::LocalScheduler>()?;
        let module = ModuleSpec::new("demo/async").function(
            FunctionSpec::new("add_one")
                .arg_name("value")
                .call_async(Arc::new(|mut frame| {
                    let value: i64 = frame.args.read_named("value")?;
                    Ok(luau::ScheduledFuture::new(async move { Ok(value + 1) }))
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
        assert_eq!(scheduler.poll_ready(), 0);
        assert_eq!(
            vm.eval(
                Arc::<[u8]>::from(&b"return async_result"[..]),
                luau::ChunkOrigin::default(),
            )?,
            vec![luau::Value::Number(42.0)]
        );
        Ok(())
    }

    #[test]
    fn luau_module_installer_schedules_pending_async_callbacks() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        vm.data().insert(crate::LocalScheduler::new())?;
        let scheduler = vm.data().get::<crate::LocalScheduler>()?;
        let pending_once = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let pending_once_for_callback = pending_once.clone();
        let module = ModuleSpec::new("demo/async").function(
            FunctionSpec::new("add_one")
                .arg_name("value")
                .call_async(Arc::new(move |mut frame| {
                    let value: i64 = frame.args.read_named("value")?;
                    let pending_once = pending_once_for_callback.clone();
                    Ok(luau::ScheduledFuture::new(std::future::poll_fn(
                        move |_cx| {
                            if pending_once.swap(false, std::sync::atomic::Ordering::SeqCst) {
                                std::task::Poll::Pending
                            } else {
                                std::task::Poll::Ready(Ok(value + 1))
                            }
                        },
                    )))
                })),
        );
        let table = install_luau_module(&vm, &ChunkOrigin::default(), &module)?;
        vm.set_global_table("module", &table)?;
        let root = vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"async_result = module.add_one(41)"[..]),
            luau::ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&root)?;

        let handle = scheduler.spawn_luau_thread(
            crate::CallContext::default(),
            vm.clone(),
            thread.clone(),
            vec![],
        );
        assert_eq!(scheduler.luau_thread_handle(&thread), Some(handle));
        assert_eq!(scheduler.poll_ready(), 1);
        assert_eq!(scheduler.luau_thread_handle(&thread), Some(handle));
        assert_eq!(
            scheduler
                .snapshot(handle.id())
                .expect("task snapshot")
                .state,
            crate::TaskState::Pending
        );
        assert_eq!(scheduler.poll_ready(), 1);
        assert_eq!(
            scheduler
                .snapshot(handle.id())
                .expect("task snapshot")
                .state,
            crate::TaskState::Completed
        );
        assert_eq!(
            vm.eval(
                Arc::<[u8]>::from(&b"return async_result"[..]),
                luau::ChunkOrigin::default(),
            )?,
            vec![luau::Value::Number(42.0)]
        );
        Ok(())
    }

    /// Calls that answer or fail after waiting once; `fail_now` fails without waiting and
    /// `fail_later` after a start delay.
    fn waiting_module() -> ModuleSpec {
        fn wait_once<T: 'static>(
            output: impl FnOnce() -> luau::runtime::Result<T> + 'static,
        ) -> impl std::future::Future<Output = luau::runtime::Result<T>> {
            let mut output = Some(output);
            let mut waited = false;
            std::future::poll_fn(move |cx| {
                if !std::mem::replace(&mut waited, true) {
                    cx.waker().wake_by_ref();
                    return std::task::Poll::Pending;
                }
                std::task::Poll::Ready(output.take().expect("polled after completion")())
            })
        }
        fn boom() -> luau::runtime::Result<()> {
            Err(luau::Error::Runtime("boom".to_string()))
        }

        ModuleSpec::new("demo/async")
            .function(FunctionSpec::new("fail_now").call_async(Arc::new(|_| {
                boom().map(|()| luau::ScheduledFuture::new(async { Ok(()) }))
            })))
            .function(FunctionSpec::new("fail").call_async(Arc::new(|_| {
                Ok(luau::ScheduledFuture::new(wait_once(boom)))
            })))
            .function(FunctionSpec::new("fail_later").call_async(Arc::new(|_| {
                Ok(luau::ScheduledFuture::after(
                    std::time::Duration::ZERO,
                    async { boom() },
                ))
            })))
            .function(
                FunctionSpec::new("fail_out_of_memory").call_async(Arc::new(|_| {
                    Ok(luau::ScheduledFuture::new(wait_once(|| {
                        Err::<(), _>(luau::Error::OutOfMemory)
                    })))
                })),
            )
            .function(
                FunctionSpec::new("add_one")
                    .arg_name("value")
                    .call_async(Arc::new(|mut frame| {
                        let value: i64 = frame.args.read_named("value")?;
                        Ok(luau::ScheduledFuture::new(wait_once(move || Ok(value + 1))))
                    })),
            )
    }

    /// Runs `source` as a task of plugin `demo` until it settles.
    fn run_waiting_module_task(
        source: &'static str,
    ) -> luau::runtime::Result<(luau::Vm, crate::TaskSnapshot)> {
        let vm = luau::Vm::new()?;
        vm.open_standard_libraries(luau::StandardLibraries::all_supported())?;
        vm.data().insert(crate::LocalScheduler::new())?;
        let scheduler = vm.data().get::<crate::LocalScheduler>()?;
        let origin = ChunkOrigin {
            plugin: Some(Arc::from("demo")),
            ..ChunkOrigin::default()
        };
        let table = install_luau_module(&vm, &origin, &waiting_module())?;
        vm.set_global_table("module", &table)?;
        let root = vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(source.as_bytes()),
            luau::ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&root)?;
        let handle =
            scheduler.spawn_luau_thread(crate::CallContext::default(), vm.clone(), thread, vec![]);
        for _ in 0..16 {
            scheduler.poll_ready();
            let snapshot = scheduler.snapshot(handle.id()).expect("task snapshot");
            if snapshot.state != crate::TaskState::Pending {
                return Ok((vm, snapshot));
            }
        }
        panic!("task did not settle");
    }

    const WAITING_FAILURE: &str =
        "module 'demo/async' function 'fail' plugin 'demo': Luau runtime failed: boom";

    #[test]
    fn waiting_async_callback_errors_are_caught_by_pcall() -> luau::runtime::Result<()> {
        let (vm, snapshot) = run_waiting_module_task(
            r#"
                local ok, err = pcall(module.fail)
                local now_ok, now_err = pcall(module.fail_now)
                local later_ok, later_err = pcall(module.fail_later)
                local memory_ok, memory_err = pcall(module.fail_out_of_memory)
                result = { ok, err, now_ok, now_err, later_ok, later_err, memory_ok, memory_err, module.add_one(41) }
            "#,
        )?;
        assert_eq!(
            snapshot.state,
            crate::TaskState::Completed,
            "{:?}",
            snapshot.error
        );
        let result = vm.eval(
            Arc::<[u8]>::from(&b"return table.unpack(result, 1, 9)"[..]),
            luau::ChunkOrigin::default(),
        )?;
        let message = |function: &str| {
            luau::Value::String(
                WAITING_FAILURE
                    .replace("'fail'", &format!("'{function}'"))
                    .into_bytes(),
            )
        };
        assert_eq!(
            result,
            vec![
                luau::Value::Boolean(false),
                message("fail"),
                luau::Value::Boolean(false),
                message("fail_now"),
                luau::Value::Boolean(false),
                message("fail_later"),
                luau::Value::Boolean(false),
                luau::Value::String(b"not enough memory".to_vec()),
                luau::Value::Number(42.0),
            ]
        );
        Ok(())
    }

    #[test]
    fn waiting_async_callback_errors_are_caught_by_pcall_in_their_coroutine()
    -> luau::runtime::Result<()> {
        let (vm, snapshot) = run_waiting_module_task(
            r#"
                coroutine.resume(coroutine.create(function()
                    result = { pcall(module.fail) }
                end))
            "#,
        )?;
        assert_eq!(
            snapshot.state,
            crate::TaskState::Completed,
            "{:?}",
            snapshot.error
        );
        vm.data().get::<crate::LocalScheduler>()?.poll_ready();
        assert_eq!(
            vm.eval(
                Arc::<[u8]>::from(&b"return table.unpack(result, 1, 2)"[..]),
                luau::ChunkOrigin::default(),
            )?,
            vec![
                luau::Value::Boolean(false),
                luau::Value::String(WAITING_FAILURE.as_bytes().to_vec()),
            ]
        );
        Ok(())
    }

    #[test]
    fn waiting_async_callback_errors_fail_the_task_without_pcall() -> luau::runtime::Result<()> {
        let (_, snapshot) = run_waiting_module_task("module.fail()")?;
        assert_eq!(snapshot.state, crate::TaskState::Failed);
        assert_eq!(
            snapshot.error.as_deref(),
            Some(format!("Luau runtime failed: {WAITING_FAILURE}").as_str())
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
            FunctionSpec::new("demo_global")
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
            .register(GatedModule::new(
                "demo.math",
                ModuleSpec::new(module_id.0.clone()).function(FunctionSpec::new("answer").call(
                    |mut frame| {
                        frame.returns.write(42_i64)?;
                        Ok(())
                    },
                )),
            ))
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
                crate::TaskGroupId::default(),
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
                crate::TaskGroupId::default(),
            )
            .expect("cached require");
        assert!(Rc::ptr_eq(&first, &second));
        vm.set_global_table("module", &first)?;
        assert_eq!(
            vm.eval(
                Arc::<[u8]>::from(&b"return module.answer()"[..]),
                luau::ChunkOrigin::default(),
            )?,
            vec![luau::Value::Number(42.0)]
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
            crate::TaskGroupId::default(),
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
            .register(GatedModule::new(
                "lyra.plugins",
                ModuleSpec::new(module_id.0.clone()).function(FunctionSpec::new("plugin_id").call(
                    |mut frame| {
                        let plugin_id = frame.context.origin.plugin.as_deref().unwrap_or("");
                        frame.returns.write(plugin_id)?;
                        Ok(())
                    },
                )),
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
            .require_luau_module(
                &vm,
                &alpha_origin,
                &module_id,
                &AllowAllCapabilities,
                crate::TaskGroupId::default(),
            )
            .expect("alpha require");
        let alpha_second = registry
            .require_luau_module(
                &vm,
                &alpha_origin,
                &module_id,
                &AllowAllCapabilities,
                crate::TaskGroupId::default(),
            )
            .expect("alpha cached require");
        let beta = registry
            .require_luau_module(
                &vm,
                &beta_origin,
                &module_id,
                &AllowAllCapabilities,
                crate::TaskGroupId::default(),
            )
            .expect("beta require");

        assert!(Rc::ptr_eq(&alpha_first, &alpha_second));
        assert!(!Rc::ptr_eq(&alpha_first, &beta));

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
    fn required_sources_evaluate_once_and_keep_their_table_exports() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        let mut loader = MemorySourceLoader::new();
        loader.insert(
            "plugin:demo/lib/module",
            b"return { answer = 42, get = function() return 7 end }".as_slice(),
        );
        let origin = ChunkOrigin {
            plugin: Some(Arc::from("demo")),
            path: Some(Arc::from("plugins/demo/init.luau")),
            ..ChunkOrigin::default()
        };
        let runtime = LuauRequireRuntime::new(loader, AllowAllCapabilities);
        let scheduler = crate::LocalScheduler::new();
        let run = runtime.plugin_group(&scheduler, "demo");
        vm.data().insert(runtime)?;
        vm.data().insert(scheduler)?;
        vm.set_global_function("require", &bound_require(&vm, &origin, run)?)?;

        let values = vm.eval(
            Arc::<[u8]>::from(
                &br#"
                    local first = require("@self/lib/module.luau")
                    local second = require("@self/lib/module.luau")
                    return first == second, first.answer, first.get()
                "#[..],
            ),
            luau::ChunkOrigin::default(),
        )?;

        assert_eq!(
            values,
            vec![
                luau::Value::Boolean(true),
                luau::Value::Number(42.0),
                luau::Value::Number(7.0)
            ]
        );
        Ok(())
    }

    #[test]
    fn luau_require_global_loads_registered_and_source_modules() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        let mut loader = MemorySourceLoader::new();
        loader.insert(
            "plugin:demo/lib/util",
            b"local child = require(\"./child.luau\"); return { source_answer = child.answer }"
                .as_slice(),
        );
        loader.insert("plugin:demo/lib/child", b"return { answer = 7 }".as_slice());
        let origin = ChunkOrigin {
            plugin: Some(Arc::from("demo")),
            path: Some(Arc::from("plugins/demo/init.luau")),
            ..ChunkOrigin::default()
        };
        let runtime = LuauRequireRuntime::new(loader, AllowAllCapabilities);
        runtime
            .register(GatedModule::new(
                "harmony.math",
                ModuleSpec::new("harmony/math").function(FunctionSpec::new("answer").call(
                    |mut frame| {
                        frame.returns.write(42_i64)?;
                        Ok(())
                    },
                )),
            ))
            .expect("register module");
        let scheduler = crate::LocalScheduler::new();
        let run = runtime.plugin_group(&scheduler, "demo");
        vm.data().insert(runtime)?;
        vm.data().insert(scheduler)?;
        vm.set_global_function("require", &bound_require(&vm, &origin, run)?)?;

        let values = vm.eval_with_context(
            Arc::<[u8]>::from(
                &br#"
                    local math = require("@harmony/math")
                    local util = require("@self/lib/util.luau")
                    return math.answer(), util.source_answer
                "#[..],
            ),
            luau::ChunkOrigin::default(),
            luau::CallContext {
                origin: origin.clone(),
                task_group: run,
                ..luau::CallContext::default()
            },
        )?;

        assert_eq!(
            values,
            vec![luau::Value::Number(42.0), luau::Value::Number(7.0)]
        );
        Ok(())
    }

    #[test]
    fn invalidation_during_evaluation_keeps_stale_source_out_of_cache() -> anyhow::Result<()> {
        let mut loader = MemorySourceLoader::new();
        loader.insert("plugin:demo/lib", b"return 1".as_slice());
        loader.insert("plugin:other/lib", b"return 1".as_slice());
        let runtime = LuauRequireRuntime::new(loader, AllowAllCapabilities);
        let scheduler = crate::LocalScheduler::new();
        runtime.plugin_group(&scheduler, "demo");
        runtime.plugin_group(&scheduler, "other");
        let origin = ChunkOrigin::default();
        let key = |plugin: &str| ModuleCacheKey(Arc::from(format!("plugin:{plugin}/lib")));

        runtime.load_source_cached("@demo/lib", &origin, |_, _| {
            runtime.invalidate_plugin("demo");
            Ok(vec![luau::Value::Number(1.0)])
        })?;
        assert!(runtime.source_cache.borrow().get(&key("demo")).is_none());

        runtime.load_source_cached("@other/lib", &origin, |_, _| {
            runtime.invalidate_plugin("demo");
            Ok(vec![luau::Value::Number(1.0)])
        })?;
        assert!(runtime.source_cache.borrow().get(&key("other")).is_some());

        runtime.load_source_cached("@demo/lib", &origin, |_, _| {
            Ok(vec![luau::Value::Number(2.0)])
        })?;
        assert_eq!(
            runtime.source_cache.borrow().get(&key("demo")).as_deref(),
            Some(&[luau::Value::Number(2.0)][..])
        );
        Ok(())
    }

    #[test]
    fn capability_check_runs_before_cache_return() {
        let module_id = ModuleId(Arc::from("lyra/datastore"));
        let mut registry = ModuleRegistry::new();
        registry
            .register(GatedModule::new(
                "lyra.datastore",
                ModuleSpec::new(module_id.0.clone()).install(|_| Ok(ModuleExport::new("cached"))),
            ))
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
        loader.insert("plugin:demo/lib/util", b"return 1".as_slice());
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

        assert_eq!(resolved.cache_key.0.as_ref(), "plugin:demo/lib/util");
        assert_eq!(resolved.origin.plugin.as_deref(), Some("demo"));
        assert_eq!(
            resolved.origin.path.as_deref(),
            Some("plugins/demo/lib/util")
        );
    }

    #[test]
    fn source_loader_resolves_relative_paths_without_source_parsing() {
        let mut loader = MemorySourceLoader::new();
        loader.insert("plugin:demo/lib/shared", b"return 2".as_slice());
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

        assert_eq!(resolved.cache_key.0.as_ref(), "plugin:demo/lib/shared");
        assert_eq!(
            resolved.origin.path.as_deref(),
            Some("plugins/demo/lib/shared")
        );
    }

    #[test]
    fn filesystem_source_loader_reads_plugin_sources_by_typed_cache_key() {
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

        let source = loader
            .resolve(SourceRequest {
                specifier: "@self/lib/util.luau",
                origin: &origin,
            })
            .expect("load plugin source from filesystem");

        assert_eq!(source.cache_key.0.as_ref(), "plugin:demo/lib/util");
        assert_eq!(source.bytes.as_ref(), b"return 40 + 2");
        let _ = std::fs::remove_dir_all(test_dir);
    }

    #[test]
    fn source_loader_resolves_plugin_id_alias_to_plugin_origin() {
        let mut loader = MemorySourceLoader::new();
        loader.insert("plugin:demo/util", b"return {}".as_slice());

        let resolved = loader
            .resolve(SourceRequest {
                specifier: "@demo/util",
                origin: &ChunkOrigin::default(),
            })
            .expect("resolve plugin alias");

        assert_eq!(resolved.cache_key.0.as_ref(), "plugin:demo/util");
        assert_eq!(resolved.origin.plugin.as_deref(), Some("demo"));
        assert_eq!(resolved.origin.path.as_deref(), Some("plugins/demo/util"));
    }

    #[test]
    fn source_loader_resolves_bare_plugin_id_to_init_module() {
        let mut loader = MemorySourceLoader::new();
        loader.insert("plugin:demo/init", b"return {}".as_slice());

        let resolved = loader
            .resolve(SourceRequest {
                specifier: "@demo",
                origin: &ChunkOrigin::default(),
            })
            .expect("resolve bare plugin alias");

        assert_eq!(resolved.cache_key.0.as_ref(), "plugin:demo/init");
    }

    #[test]
    fn source_loader_rejects_bare_source_paths_from_plugin_origins() {
        let mut loader = MemorySourceLoader::new();
        loader.insert("source:lib/util", b"return 1".as_slice());
        let origin = ChunkOrigin {
            plugin: Some(Arc::from("demo")),
            path: Some(Arc::from("plugins/demo/init")),
            ..ChunkOrigin::default()
        };

        let error = loader
            .resolve(SourceRequest {
                specifier: "lib/util",
                origin: &origin,
            })
            .expect_err("plugin origins must not require bare source paths");

        assert!(error.to_string().contains("must start with"));
    }
}
