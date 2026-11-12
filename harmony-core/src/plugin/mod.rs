// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use semver::{
    Version,
    VersionReq,
};
use std::fmt;
use std::path::PathBuf;

mod discovery;
mod manifest;
mod runtime;

pub use discovery::{
    LoadedPlugin,
    PluginDiscoveryError,
    PluginManager,
};
pub use manifest::{
    Dependency,
    InvalidHostName,
    LoadedManifest,
    PLUGIN_CONFIG_FILENAME,
    PLUGIN_CONFIG_MAX_BYTES,
    PLUGIN_CONFIG_MAX_SCOPE_GRANT_ITEMS,
    PLUGIN_SCHEMA_VERSION,
    PluginHost,
    PluginManifest,
    RESERVED_HOST_NAMES,
    validate_plugin_id,
};
pub use runtime::{
    ManifestCapabilityPolicy,
    Runtime,
    RuntimeBuilder,
};

#[derive(Debug)]
pub enum PluginLoadError {
    ConfigNotFound(PathBuf),
    ConfigTooLarge {
        path: PathBuf,
        bytes: u64,
        max: u64,
    },
    ConfigParseError {
        path: PathBuf,
        error: String,
    },
    EntrypointNotFound {
        plugin_id: String,
        path: PathBuf,
    },
    DuplicateId(String),
    InvalidSchemaVersion {
        plugin_id: String,
        version: u32,
    },
    InvalidPluginId {
        plugin_id: String,
        reason: String,
    },
    IncompatibleHost {
        plugin_id: String,
        requirement: VersionReq,
        host: String,
        host_version: Version,
    },
    DirectoryIdMismatch {
        plugin_id: String,
        directory: String,
    },
    ScopeEntryTooLong {
        plugin_id: String,
        scope: String,
        max: usize,
    },
    UnknownScope {
        plugin_id: String,
        scope: String,
    },
    ScopeWithoutParent {
        plugin_id: String,
        scope: String,
        parent: String,
    },
    /// The plugin declares `scope` without the grant, under `key` of the
    /// scope's entry, that the scope requires.
    MissingScopeGrant {
        plugin_id: String,
        scope: String,
        key: String,
    },
    TooManyDependencies {
        plugin_id: String,
        count: usize,
        max: usize,
    },
    InvalidDependencyId {
        plugin_id: String,
        dep_id: String,
        reason: String,
    },
    SelfDependency {
        plugin_id: String,
    },
    MissingDependency {
        plugin_id: String,
        dependency: String,
        requirement: VersionReq,
    },
    IncompatibleDependency {
        plugin_id: String,
        dependency: String,
        requirement: VersionReq,
        installed: Version,
    },
    DependencyCycle {
        plugins: Vec<String>,
    },
}

impl fmt::Display for PluginLoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PluginLoadError::ConfigNotFound(path) => {
                write!(
                    f,
                    "{PLUGIN_CONFIG_FILENAME} not found at {}",
                    path.display()
                )
            }
            PluginLoadError::ConfigTooLarge { path, bytes, max } => write!(
                f,
                "{PLUGIN_CONFIG_FILENAME} at {} is {bytes} bytes, exceeds cap of {max}",
                path.display(),
            ),
            PluginLoadError::ConfigParseError { path, error } => {
                write!(f, "failed to parse {}: {error}", path.display())
            }
            PluginLoadError::EntrypointNotFound { plugin_id, path } => {
                write!(
                    f,
                    "entrypoint not found for plugin '{}' at {}",
                    plugin_id,
                    path.display()
                )
            }
            PluginLoadError::DuplicateId(id) => {
                write!(f, "duplicate plugin id: {}", id)
            }
            PluginLoadError::InvalidSchemaVersion { plugin_id, version } => {
                write!(
                    f,
                    "invalid schema_version {version} for plugin '{plugin_id}' (expected \
                     {PLUGIN_SCHEMA_VERSION})"
                )
            }
            PluginLoadError::InvalidPluginId { plugin_id, reason } => {
                write!(f, "invalid plugin id '{}': {}", plugin_id, reason)
            }
            PluginLoadError::IncompatibleHost {
                plugin_id,
                requirement,
                host,
                host_version,
            } => write!(
                f,
                "plugin '{plugin_id}' requires {host} {requirement}, but this is {host} \
                 {host_version}"
            ),
            PluginLoadError::DirectoryIdMismatch {
                plugin_id,
                directory,
            } => write!(
                f,
                "plugin id '{}' does not match directory basename '{}'; runtime identity is \
                 derived from the directory for required modules, so they must match",
                plugin_id, directory
            ),
            PluginLoadError::ScopeEntryTooLong {
                plugin_id,
                scope,
                max,
            } => write!(
                f,
                "plugin '{}' scope entry is {} bytes, exceeds cap of {}: {:?}",
                plugin_id,
                scope.len(),
                max,
                truncate_for_error(scope)
            ),
            PluginLoadError::UnknownScope { plugin_id, scope } => write!(
                f,
                "plugin '{}' declares unknown scope '{}'",
                plugin_id, scope
            ),
            PluginLoadError::ScopeWithoutParent {
                plugin_id,
                scope,
                parent,
            } => write!(
                f,
                "plugin '{plugin_id}' declares scope '{scope}' without '{parent}', the scope it \
                 narrows"
            ),
            PluginLoadError::MissingScopeGrant {
                plugin_id,
                scope,
                key,
            } => write!(
                f,
                "plugin '{plugin_id}' declares scope '{scope}' without `{key}`, which the scope \
                 requires"
            ),
            PluginLoadError::TooManyDependencies {
                plugin_id,
                count,
                max,
            } => write!(
                f,
                "plugin '{}' declares {} dependencies, exceeds cap of {}",
                plugin_id, count, max
            ),
            PluginLoadError::InvalidDependencyId {
                plugin_id,
                dep_id,
                reason,
            } => write!(
                f,
                "plugin '{}' declares invalid dependency id '{}': {}",
                plugin_id, dep_id, reason
            ),
            PluginLoadError::SelfDependency { plugin_id } => {
                write!(f, "plugin '{}' lists itself as a dependency", plugin_id)
            }
            PluginLoadError::MissingDependency {
                plugin_id,
                dependency,
                requirement,
            } => write!(
                f,
                "plugin '{plugin_id}' requires plugin '{dependency}' {requirement}, but it is not \
                 installed or failed to load"
            ),
            PluginLoadError::IncompatibleDependency {
                plugin_id,
                dependency,
                requirement,
                installed,
            } => write!(
                f,
                "plugin '{plugin_id}' requires plugin '{dependency}' {requirement}, but \
                 {installed} is installed"
            ),
            PluginLoadError::DependencyCycle { plugins } => write!(
                f,
                "dependency cycle detected; the following plugins are in a cycle and were \
                 skipped: {:?}",
                plugins
            ),
        }
    }
}

impl std::error::Error for PluginLoadError {}

fn truncate_for_error(s: &str) -> String {
    const LIMIT: usize = 80;
    if s.len() <= LIMIT {
        s.to_string()
    } else {
        format!(
            "{}…",
            &s[..s.char_indices().nth(LIMIT).map(|(i, _)| i).unwrap_or(LIMIT)]
        )
    }
}

#[cfg(test)]
pub(crate) mod tests;
