// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod luaurc;
mod modules;
mod plugin;
mod scheduler;

pub use luaurc::LuaurcConfig;
pub use modules::{
    AllowAllCapabilities,
    CapabilityPolicy,
    FilesystemSourceLoader,
    FunctionSpec,
    GlobalSpec,
    MemorySourceLoader,
    ModuleCacheKey,
    ModuleCapabilityDenied,
    ModuleExport,
    ModuleLoadContext,
    ModuleLoadError,
    ModuleRegistry,
    ModuleSpec,
    NativeModuleInstaller,
    ResolvedSource,
    SourceLoader,
    SourceRequest,
    UserDataSpec,
};
pub use modules::{
    FunctionCallback,
    LuauRequireRuntime,
    LuauSourceCache,
    async_luau_callback,
    install_luau_globals,
    install_luau_module,
    install_luau_require,
};
pub use plugin::{
    LoadedPlugin,
    PluginLoadError,
    PluginManager,
    PluginManifest,
};
pub use scheduler::LocalScheduler;
pub use scheduler::{
    CallContext,
    CapabilityId,
    ChunkOrigin,
    ContextBag,
    ModuleId,
    ScheduledFuture,
    Scheduler,
    TaskGroupId,
    TaskHandle,
    TaskId,
    TaskSnapshot,
    TaskState,
};
