// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use serde::Deserialize;
use std::collections::{
    BTreeSet,
    HashMap,
    HashSet,
};
use std::fmt;
use std::path::{
    Path,
    PathBuf,
};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use harmony_luau as luau;

use crate::{
    CallContext,
    GlobalSpec,
    LocalScheduler,
    ModuleId,
    ModuleSpec,
    SourceLoader,
    TokioRuntimeContext,
    luau::{
        RequireRuntime,
        ThreadDriveOptions,
        drive_thread,
        install_globals,
        install_require,
    },
    modules::CapabilityPolicy,
    scheduler::{
        CapabilityId,
        ChunkOrigin,
    },
};

pub const PLUGIN_CONFIG_FILENAME: &str = "plugin.json";
pub(crate) const PLUGIN_SCHEMA_VERSION: u32 = 1;

/// Hard caps applied to `plugin.json` before `serde_json` parses it.
/// Prevents a hostile or malformed plugin config from OOMing `discover_plugins`.
pub(crate) const PLUGIN_CONFIG_MAX_BYTES: u64 = 64 * 1024;
pub(crate) const PLUGIN_CONFIG_MAX_SCOPES: usize = 32;
pub(crate) const PLUGIN_CONFIG_MAX_SCOPE_LEN: usize = 128;
pub(crate) const PLUGIN_CONFIG_MAX_DEPENDENCIES: usize = 32;
pub(crate) const PLUGIN_CONFIG_MAX_DEPENDENCY_ALTERNATIVES: usize = 16;

#[derive(Debug, Clone, Deserialize)]
pub struct PluginManifest {
    pub schema_version: u32,
    pub id: String,
    pub name: String,
    pub version: String,
    pub description: String,
    pub entrypoint: String,
    /// Scopes (capability ids) the plugin declares. Every gated module
    /// the plugin `require`s must have its scope id listed here.
    /// Required on `schema_version: 1`.
    pub scopes: Vec<String>,
    #[serde(default)]
    pub dependencies: Vec<DependencyEntry>,
}

/// Bare-string variant is shorthand for a single-alternative required
/// dependency; the group form expresses alternatives and/or `required: false`.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum DependencyEntry {
    Id(String),
    Group(DependencyGroup),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyGroup {
    pub any_of: Vec<String>,
    /// When `false`, missing alternatives don't block load — but any
    /// installed alternative still constrains load order.
    #[serde(default = "default_required")]
    pub required: bool,
}

fn default_required() -> bool {
    true
}

/// Validated, uniform form of a `DependencyEntry` built at load time.
#[derive(Debug, Clone)]
pub struct NormalizedDependency {
    pub alternatives: Vec<String>,
    pub required: bool,
}

#[derive(Debug, Clone)]
pub struct LoadedPlugin {
    pub manifest: PluginManifest,
    pub directory: PathBuf,
    pub entrypoint_path: PathBuf,
    /// Deduplicated, validated copy of `manifest.scopes` kept as `Arc<str>`
    /// so the runtime gate can share allocations across lookups.
    pub declared_scopes: HashSet<Arc<str>>,
    pub dependencies: Vec<NormalizedDependency>,
}

pub struct ManifestCapabilityPolicy {
    scopes_by_plugin: HashMap<Arc<str>, HashSet<Arc<str>>>,
}

impl ManifestCapabilityPolicy {
    pub fn from_manifests(manifests: Arc<[PluginManifest]>) -> Self {
        let scopes_by_plugin = manifests
            .iter()
            .map(|manifest| {
                let scopes = manifest
                    .scopes
                    .iter()
                    .map(|scope| Arc::<str>::from(scope.as_str()))
                    .collect::<HashSet<_>>();
                (Arc::<str>::from(manifest.id.as_str()), scopes)
            })
            .collect();

        Self { scopes_by_plugin }
    }
}

impl CapabilityPolicy for ManifestCapabilityPolicy {
    fn is_allowed(&self, origin: &ChunkOrigin, capability: &CapabilityId) -> bool {
        let Some(plugin_id) = origin.plugin.as_ref() else {
            return false;
        };
        self.scopes_by_plugin
            .get(plugin_id)
            .is_some_and(|scopes| scopes.contains(&capability.0))
    }
}

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
    DirectoryIdMismatch {
        plugin_id: String,
        directory: String,
    },
    TooManyScopes {
        plugin_id: String,
        count: usize,
        max: usize,
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
    TooManyDependencies {
        plugin_id: String,
        count: usize,
        max: usize,
    },
    TooManyDependencyAlternatives {
        plugin_id: String,
        group_index: usize,
        count: usize,
        max: usize,
    },
    EmptyDependencyGroup {
        plugin_id: String,
        group_index: usize,
    },
    InvalidDependencyId {
        plugin_id: String,
        dep_id: String,
        reason: String,
    },
    SelfDependency {
        plugin_id: String,
    },
    DuplicateDependencyAlternative {
        plugin_id: String,
        group_index: usize,
        alternative: String,
    },
    UnsatisfiedRequiredDependency {
        plugin_id: String,
        alternatives: Vec<String>,
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
            PluginLoadError::DirectoryIdMismatch {
                plugin_id,
                directory,
            } => write!(
                f,
                "plugin id '{}' does not match directory basename '{}'; runtime identity is \
                 derived from the directory for required modules, so they must match",
                plugin_id, directory
            ),
            PluginLoadError::TooManyScopes {
                plugin_id,
                count,
                max,
            } => write!(
                f,
                "plugin '{}' declares {} scopes, exceeds cap of {}",
                plugin_id, count, max
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
            PluginLoadError::TooManyDependencies {
                plugin_id,
                count,
                max,
            } => write!(
                f,
                "plugin '{}' declares {} dependency entries, exceeds cap of {}",
                plugin_id, count, max
            ),
            PluginLoadError::TooManyDependencyAlternatives {
                plugin_id,
                group_index,
                count,
                max,
            } => write!(
                f,
                "plugin '{}' dependency entry #{} declares {} alternatives, exceeds cap of {}",
                plugin_id, group_index, count, max
            ),
            PluginLoadError::EmptyDependencyGroup {
                plugin_id,
                group_index,
            } => write!(
                f,
                "plugin '{}' dependency entry #{} has an empty any_of list",
                plugin_id, group_index
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
            PluginLoadError::DuplicateDependencyAlternative {
                plugin_id,
                group_index,
                alternative,
            } => write!(
                f,
                "plugin '{}' dependency entry #{} lists '{}' more than once",
                plugin_id, group_index, alternative
            ),
            PluginLoadError::UnsatisfiedRequiredDependency {
                plugin_id,
                alternatives,
            } => write!(
                f,
                "plugin '{}' requires any of {:?} but none are installed",
                plugin_id, alternatives
            ),
            PluginLoadError::DependencyCycle { plugins } => write!(
                f,
                "dependency cycle detected; the following plugins are in or downstream of a \
                 cycle and were skipped: {:?}",
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

// Cross-plugin resolution (required deps satisfied, no cycles) lives
// in `resolve_dependencies` after all manifests are loaded.
fn normalize_dependencies(
    manifest_id: &str,
    raw: &[DependencyEntry],
) -> Result<Vec<NormalizedDependency>, PluginLoadError> {
    if raw.len() > PLUGIN_CONFIG_MAX_DEPENDENCIES {
        return Err(PluginLoadError::TooManyDependencies {
            plugin_id: manifest_id.to_string(),
            count: raw.len(),
            max: PLUGIN_CONFIG_MAX_DEPENDENCIES,
        });
    }

    let mut result = Vec::with_capacity(raw.len());
    for (index, entry) in raw.iter().enumerate() {
        let (alternatives, required) = match entry {
            DependencyEntry::Id(id) => (vec![id.clone()], true),
            DependencyEntry::Group(group) => (group.any_of.clone(), group.required),
        };

        if alternatives.is_empty() {
            return Err(PluginLoadError::EmptyDependencyGroup {
                plugin_id: manifest_id.to_string(),
                group_index: index,
            });
        }

        if alternatives.len() > PLUGIN_CONFIG_MAX_DEPENDENCY_ALTERNATIVES {
            return Err(PluginLoadError::TooManyDependencyAlternatives {
                plugin_id: manifest_id.to_string(),
                group_index: index,
                count: alternatives.len(),
                max: PLUGIN_CONFIG_MAX_DEPENDENCY_ALTERNATIVES,
            });
        }

        let mut seen: HashSet<&str> = HashSet::with_capacity(alternatives.len());
        for dep_id in &alternatives {
            if let Err(reason) = validate_plugin_id(dep_id) {
                return Err(PluginLoadError::InvalidDependencyId {
                    plugin_id: manifest_id.to_string(),
                    dep_id: dep_id.clone(),
                    reason,
                });
            }
            if dep_id == manifest_id {
                return Err(PluginLoadError::SelfDependency {
                    plugin_id: manifest_id.to_string(),
                });
            }
            if !seen.insert(dep_id.as_str()) {
                return Err(PluginLoadError::DuplicateDependencyAlternative {
                    plugin_id: manifest_id.to_string(),
                    group_index: index,
                    alternative: dep_id.clone(),
                });
            }
        }

        result.push(NormalizedDependency {
            alternatives,
            required,
        });
    }
    Ok(result)
}

fn validate_plugin_id(id: &str) -> Result<(), String> {
    if id.is_empty() {
        return Err("must not be empty".into());
    }
    for c in id.chars() {
        if !(c.is_ascii_alphanumeric() || c == '_' || c == '-') {
            return Err(format!(
                "must match [A-Za-z0-9_-]+ (invalid character: {c:?})"
            ));
        }
    }
    Ok(())
}

pub struct PluginManager {
    plugins: HashMap<String, LoadedPlugin>,
    /// `LoadedPlugin.directory` → plugin id. Populated at insert time.
    /// Lets the runtime gate resolve a chunk's plugin root to an identity
    /// without re-parsing chunk names (which was the forgery oracle).
    by_root: HashMap<PathBuf, String>,
    plugins_dir: PathBuf,
}

impl PluginManager {
    pub fn new(plugins_dir: PathBuf) -> Self {
        Self {
            plugins: HashMap::new(),
            by_root: HashMap::new(),
            plugins_dir,
        }
    }

    /// `valid_scope_ids` is the set of scope ids the running workspace
    /// actually registers (derived from `Module.scope.id`). A manifest
    /// naming a scope outside this set fails load — the plugin is asking
    /// for something the runtime cannot grant.
    pub fn discover_plugins(
        &mut self,
        valid_scope_ids: &HashSet<Arc<str>>,
    ) -> Result<Vec<PluginLoadError>, std::io::Error> {
        let mut errors = Vec::new();

        if !self.plugins_dir.exists() {
            return Ok(errors);
        }

        for entry in std::fs::read_dir(&self.plugins_dir)? {
            let entry = entry?;
            let path = entry.path();

            if !path.is_dir() {
                continue;
            }

            // Dot-directories (VCS metadata, install staging) are not
            // plugin candidates.
            if entry.file_name().to_string_lossy().starts_with('.') {
                continue;
            }

            match self.load_plugin(&path, valid_scope_ids) {
                Ok(plugin) => {
                    if self.plugins.contains_key(&plugin.manifest.id) {
                        errors.push(PluginLoadError::DuplicateId(plugin.manifest.id));
                    } else {
                        tracing::info!(
                            "loaded plugin '{}' v{} from {}",
                            plugin.manifest.name,
                            plugin.manifest.version,
                            path.display()
                        );
                        self.by_root
                            .insert(plugin.directory.clone(), plugin.manifest.id.clone());
                        self.plugins.insert(plugin.manifest.id.clone(), plugin);
                    }
                }
                Err(e) => errors.push(e),
            }
        }

        errors.extend(self.resolve_dependencies());

        Ok(errors)
    }

    /// Validate cross-plugin invariants and prune offenders so the
    /// runtime never sees a half-resolved set.
    fn resolve_dependencies(&mut self) -> Vec<PluginLoadError> {
        let mut errors = Vec::new();

        // Cascading prunes: dropping A can unsatisfy B's required dep
        // on A.
        loop {
            let mut to_remove: Vec<(String, Vec<String>)> = Vec::new();
            let mut ids: Vec<&String> = self.plugins.keys().collect();
            ids.sort();
            for id in ids {
                let plugin = &self.plugins[id];
                for dep in &plugin.dependencies {
                    if !dep.required {
                        continue;
                    }
                    let satisfied = dep
                        .alternatives
                        .iter()
                        .any(|alt| self.plugins.contains_key(alt));
                    if !satisfied {
                        to_remove.push((plugin.manifest.id.clone(), dep.alternatives.clone()));
                        break;
                    }
                }
            }

            if to_remove.is_empty() {
                break;
            }

            for (id, alternatives) in to_remove {
                if let Some(removed) = self.plugins.remove(&id) {
                    self.by_root.remove(&removed.directory);
                    errors.push(PluginLoadError::UnsatisfiedRequiredDependency {
                        plugin_id: id,
                        alternatives,
                    });
                }
            }
        }

        let (_, cycle_members) = self.kahn_visit();
        if !cycle_members.is_empty() {
            for plugin_id in &cycle_members {
                if let Some(removed) = self.plugins.remove(plugin_id) {
                    self.by_root.remove(&removed.directory);
                }
            }
            errors.push(PluginLoadError::DependencyCycle {
                plugins: cycle_members,
            });
        }

        errors
    }

    // Kahn's algorithm. `unscheduled` is non-empty iff the graph has a
    // cycle, and lists ids in or downstream of one.
    fn kahn_visit(&self) -> (Vec<String>, Vec<String>) {
        let (mut in_degree, adjacency) = self.build_dependency_graph();

        let mut ready: BTreeSet<String> = in_degree
            .iter()
            .filter_map(|(id, deg)| if *deg == 0 { Some(id.clone()) } else { None })
            .collect();

        let mut scheduled: Vec<String> = Vec::with_capacity(self.plugins.len());
        while let Some(id) = ready.iter().next().cloned() {
            ready.remove(&id);
            scheduled.push(id.clone());
            if let Some(successors) = adjacency.get(&id) {
                for successor in successors {
                    if let Some(deg) = in_degree.get_mut(successor) {
                        *deg -= 1;
                        if *deg == 0 {
                            ready.insert(successor.clone());
                        }
                    }
                }
            }
        }

        let mut unscheduled: Vec<String> = in_degree
            .into_iter()
            .filter_map(|(id, deg)| if deg > 0 { Some(id) } else { None })
            .collect();
        unscheduled.sort();
        (scheduled, unscheduled)
    }

    // Edges fan out to every installed alternative — required or not,
    // once a dep is present the dependent runs after it.
    fn build_dependency_graph(&self) -> (HashMap<String, usize>, HashMap<String, Vec<String>>) {
        let mut in_degree: HashMap<String, usize> =
            self.plugins.keys().map(|id| (id.clone(), 0)).collect();
        let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();

        for plugin in self.plugins.values() {
            let dependent = &plugin.manifest.id;
            let mut already_edged: HashSet<&str> = HashSet::new();
            for dep in &plugin.dependencies {
                for alt in &dep.alternatives {
                    if !self.plugins.contains_key(alt) {
                        continue;
                    }
                    if !already_edged.insert(alt.as_str()) {
                        continue;
                    }
                    adjacency
                        .entry(alt.clone())
                        .or_default()
                        .push(dependent.clone());
                    if let Some(deg) = in_degree.get_mut(dependent) {
                        *deg += 1;
                    }
                }
            }
        }

        (in_degree, adjacency)
    }

    pub fn reload_plugin(
        &mut self,
        plugin_id: &str,
        valid_scope_ids: &HashSet<Arc<str>>,
    ) -> Result<(), PluginLoadError> {
        if let Err(reason) = validate_plugin_id(plugin_id) {
            return Err(PluginLoadError::InvalidPluginId {
                plugin_id: plugin_id.to_string(),
                reason,
            });
        }

        let plugin_dir = self.plugins_dir.join(plugin_id);
        let plugin = self.load_plugin(&plugin_dir, valid_scope_ids)?;

        // Check deps before mutating — a failing reload must leave the
        // previous version intact.
        for dep in &plugin.dependencies {
            if !dep.required {
                continue;
            }
            let satisfied = dep
                .alternatives
                .iter()
                .any(|alt| self.plugins.contains_key(alt));
            if !satisfied {
                return Err(PluginLoadError::UnsatisfiedRequiredDependency {
                    plugin_id: plugin.manifest.id.clone(),
                    alternatives: dep.alternatives.clone(),
                });
            }
        }

        let previous = self
            .plugins
            .insert(plugin.manifest.id.clone(), plugin.clone());
        if let Some(prev) = &previous {
            self.by_root.remove(&prev.directory);
        }
        self.by_root
            .insert(plugin.directory.clone(), plugin.manifest.id.clone());

        // The new manifest could introduce a cycle — probe the updated
        // graph and roll back if so.
        let (_, cycle_members) = self.kahn_visit();
        if !cycle_members.is_empty() {
            self.by_root.remove(&plugin.directory);
            match previous {
                Some(prev) => {
                    self.by_root
                        .insert(prev.directory.clone(), prev.manifest.id.clone());
                    self.plugins.insert(prev.manifest.id.clone(), prev);
                }
                None => {
                    self.plugins.remove(&plugin.manifest.id);
                }
            }
            return Err(PluginLoadError::DependencyCycle {
                plugins: cycle_members,
            });
        }

        tracing::info!(
            "reloaded plugin '{}' v{} from {}",
            plugin.manifest.name,
            plugin.manifest.version,
            plugin.directory.display()
        );
        Ok(())
    }

    fn load_plugin(
        &self,
        dir: &Path,
        valid_scope_ids: &HashSet<Arc<str>>,
    ) -> Result<LoadedPlugin, PluginLoadError> {
        let plugin = Self::load_plugin_dir(dir, valid_scope_ids)?;

        // Runtime identity for required modules comes from the directory
        // basename (see `parse_plugin_id` in lyra-server). Entrypoint
        // chunks use `manifest.id`. If these disagree, top-level code
        // attributes to manifest.id but `require("sub")` modules attribute
        // to the directory — teardown clears one bucket and the other
        // leaks forever. Enforce the constraint here rather than audit
        // every chunk-name call site.
        let directory_basename = dir.file_name().and_then(|s| s.to_str()).unwrap_or_default();
        if directory_basename != plugin.manifest.id {
            return Err(PluginLoadError::DirectoryIdMismatch {
                plugin_id: plugin.manifest.id,
                directory: directory_basename.to_string(),
            });
        }

        Ok(plugin)
    }

    /// Parses and validates a plugin directory without binding it to its
    /// installed location, so install tooling can vet staged candidates
    /// before they land under `plugins_dir`.
    pub fn load_plugin_dir(
        dir: &Path,
        valid_scope_ids: &HashSet<Arc<str>>,
    ) -> Result<LoadedPlugin, PluginLoadError> {
        let config_path = dir.join(PLUGIN_CONFIG_FILENAME);

        if !config_path.exists() {
            return Err(PluginLoadError::ConfigNotFound(config_path));
        }

        // DoS cap — check size before reading the whole file into memory.
        match std::fs::metadata(&config_path) {
            Ok(meta) if meta.len() > PLUGIN_CONFIG_MAX_BYTES => {
                return Err(PluginLoadError::ConfigTooLarge {
                    path: config_path,
                    bytes: meta.len(),
                    max: PLUGIN_CONFIG_MAX_BYTES,
                });
            }
            Ok(_) => {}
            Err(e) => {
                return Err(PluginLoadError::ConfigParseError {
                    path: config_path,
                    error: e.to_string(),
                });
            }
        }

        let config_str = std::fs::read_to_string(&config_path).map_err(|e| {
            PluginLoadError::ConfigParseError {
                path: config_path.clone(),
                error: e.to_string(),
            }
        })?;

        let manifest: PluginManifest =
            serde_json::from_str(&config_str).map_err(|e| PluginLoadError::ConfigParseError {
                path: config_path,
                error: e.to_string(),
            })?;

        if manifest.schema_version != PLUGIN_SCHEMA_VERSION {
            return Err(PluginLoadError::InvalidSchemaVersion {
                plugin_id: manifest.id,
                version: manifest.schema_version,
            });
        }

        if let Err(reason) = validate_plugin_id(&manifest.id) {
            return Err(PluginLoadError::InvalidPluginId {
                plugin_id: manifest.id,
                reason,
            });
        }

        let entrypoint_path = dir.join(&manifest.entrypoint);
        if !entrypoint_path.exists() {
            return Err(PluginLoadError::EntrypointNotFound {
                plugin_id: manifest.id,
                path: entrypoint_path,
            });
        }

        if manifest.scopes.len() > PLUGIN_CONFIG_MAX_SCOPES {
            return Err(PluginLoadError::TooManyScopes {
                plugin_id: manifest.id,
                count: manifest.scopes.len(),
                max: PLUGIN_CONFIG_MAX_SCOPES,
            });
        }

        let mut declared_scopes: HashSet<Arc<str>> = HashSet::new();
        for scope in &manifest.scopes {
            if scope.len() > PLUGIN_CONFIG_MAX_SCOPE_LEN {
                return Err(PluginLoadError::ScopeEntryTooLong {
                    plugin_id: manifest.id.clone(),
                    scope: scope.clone(),
                    max: PLUGIN_CONFIG_MAX_SCOPE_LEN,
                });
            }
            // Intern via valid_scope_ids so declared_scopes entries share
            // allocations with the module registry — clones in the runtime
            // gate are ref-count bumps, not string copies.
            let Some(interned) = valid_scope_ids.get(scope.as_str()) else {
                return Err(PluginLoadError::UnknownScope {
                    plugin_id: manifest.id.clone(),
                    scope: scope.clone(),
                });
            };
            declared_scopes.insert(interned.clone());
        }

        let dependencies = normalize_dependencies(&manifest.id, &manifest.dependencies)?;

        Ok(LoadedPlugin {
            manifest,
            directory: dir.to_path_buf(),
            entrypoint_path,
            declared_scopes,
            dependencies,
        })
    }

    pub fn get_plugin(&self, id: &str) -> Option<&LoadedPlugin> {
        self.plugins.get(id)
    }

    /// Resolve a filesystem path to the plugin that owns it. Path must
    /// be the canonical directory under `plugins_dir` (not a child file).
    pub fn get_by_root(&self, root: &Path) -> Option<&LoadedPlugin> {
        self.by_root.get(root).and_then(|id| self.plugins.get(id))
    }

    pub fn list_plugins(&self) -> impl Iterator<Item = &LoadedPlugin> {
        self.plugins.values()
    }

    /// Plugins in execution order. Ties break by id so the order is
    /// stable (the underlying `HashMap` is not). Discovery and reload
    /// reject cycles, which is what the `debug_assert!` relies on.
    pub fn topological_order(&self) -> Vec<LoadedPlugin> {
        let (scheduled, unscheduled) = self.kahn_visit();
        debug_assert!(
            unscheduled.is_empty(),
            "topological_order missed plugins — cycle invariant broken: {unscheduled:?}"
        );
        scheduled
            .into_iter()
            .map(|id| {
                self.plugins
                    .get(&id)
                    .cloned()
                    .expect("kahn_visit produced id not present in self.plugins")
            })
            .collect()
    }
}

type VmConfigurator = Box<dyn FnOnce(&luau::Vm) -> anyhow::Result<()> + 'static>;

pub struct RuntimeBuilder<L> {
    loader: L,
    manifests: Arc<[PluginManifest]>,
    plugins: Arc<[LoadedPlugin]>,
    module_specs: Vec<ModuleSpec>,
    global_specs: Vec<GlobalSpec>,
    memory_limit: usize,
    luau_resume_budget: Option<Duration>,
    configure_vm: Vec<VmConfigurator>,
}

impl<L> RuntimeBuilder<L>
where
    L: SourceLoader + 'static,
{
    pub fn new(loader: L, manifests: Arc<[PluginManifest]>) -> Self {
        Self {
            loader,
            manifests,
            plugins: Arc::from(Vec::<LoadedPlugin>::new()),
            module_specs: Vec::new(),
            global_specs: Vec::new(),
            memory_limit: 256 * 1024 * 1024,
            luau_resume_budget: Some(Duration::from_secs(300)),
            configure_vm: Vec::new(),
        }
    }

    pub fn plugins(mut self, plugins: Arc<[LoadedPlugin]>) -> Self {
        self.plugins = plugins;
        self
    }

    pub fn module_specs(mut self, specs: Vec<ModuleSpec>) -> Self {
        self.module_specs = specs;
        self
    }

    pub fn global_specs(mut self, specs: Vec<GlobalSpec>) -> Self {
        self.global_specs = specs;
        self
    }

    pub fn memory_limit(mut self, bytes: usize) -> Self {
        self.memory_limit = bytes;
        self
    }

    pub fn luau_resume_budget(mut self, budget: Option<Duration>) -> Self {
        self.luau_resume_budget = budget;
        self
    }

    pub fn configure_vm<F>(mut self, configure: F) -> Self
    where
        F: FnOnce(&luau::Vm) -> anyhow::Result<()> + 'static,
    {
        self.configure_vm.push(Box::new(configure));
        self
    }

    pub fn build(self) -> anyhow::Result<Runtime> {
        let vm =
            luau::Vm::with_options(luau::VmOptions::default().memory_limit(self.memory_limit))?;
        vm.open_standard_libraries(luau::StandardLibraries::all_supported())?;

        let scheduler = LocalScheduler::new();
        scheduler.set_luau_resume_budget(self.luau_resume_budget);
        vm.data().insert(scheduler)?;

        for configure in self.configure_vm {
            configure(&vm)?;
        }

        let require = RequireRuntime::new(
            self.loader,
            ManifestCapabilityPolicy::from_manifests(self.manifests),
        );
        for spec in self.module_specs {
            require.register(spec)?;
        }
        vm.data().insert(require)?;

        for globals in self.global_specs {
            install_globals(&vm, &ChunkOrigin::default(), &globals)?;
        }
        install_require(&vm, &ChunkOrigin::default())?;

        Ok(Runtime {
            vm,
            plugins: self.plugins,
            tokio_runtime: TokioRuntimeContext::new()?,
        })
    }
}

pub struct Runtime {
    pub vm: luau::Vm,
    pub plugins: Arc<[LoadedPlugin]>,
    pub tokio_runtime: TokioRuntimeContext,
}

impl Runtime {
    pub fn plugin_manifests(&self) -> Vec<PluginManifest> {
        let mut manifests = self
            .plugins
            .iter()
            .map(|plugin| plugin.manifest.clone())
            .collect::<Vec<_>>();
        manifests.sort_by(|left, right| left.id.cmp(&right.id));
        manifests
    }

    pub fn has_plugin(&self, plugin_id: &str) -> bool {
        self.plugins
            .iter()
            .any(|plugin| plugin.manifest.id == plugin_id)
    }

    pub fn exec_plugin(&self, plugin_id: &str) -> anyhow::Result<()> {
        let plugin = self
            .plugins
            .iter()
            .find(|plugin| plugin.manifest.id == plugin_id)
            .ok_or_else(|| anyhow::anyhow!("plugin not found: {plugin_id}"))?;
        self.exec_loaded_plugin(plugin)
    }

    pub fn exec_all(&self) -> anyhow::Result<()> {
        for plugin in self.plugins.iter() {
            match self.exec_loaded_plugin(plugin) {
                Ok(()) => tracing::debug!("plugin '{}' executed", plugin.manifest.id),
                Err(error) => tracing::warn!("plugin '{}' error: {error}", plugin.manifest.id),
            }
        }
        Ok(())
    }

    fn exec_loaded_plugin(&self, plugin: &LoadedPlugin) -> anyhow::Result<()> {
        let bytes = std::fs::read(&plugin.entrypoint_path).with_context(|| {
            format!(
                "load plugin '{}' entrypoint from {}",
                plugin.manifest.id,
                plugin.entrypoint_path.display()
            )
        })?;
        self.run_plugin_source(
            plugin.manifest.id.as_str(),
            plugin.manifest.entrypoint.as_str(),
            bytes,
        )
    }

    pub fn run_plugin_source(
        &self,
        plugin_id: impl Into<Arc<str>>,
        path: impl Into<Arc<str>>,
        source: impl Into<Arc<[u8]>>,
    ) -> anyhow::Result<()> {
        let origin = plugin_origin(plugin_id, path);
        self.eval_source_with_context(
            source,
            CallContext {
                origin,
                ..CallContext::default()
            },
        )
        .map(|_| ())
    }

    pub fn eval_source_with_context(
        &self,
        source: impl Into<Arc<[u8]>>,
        context: CallContext,
    ) -> anyhow::Result<Vec<luau::Value>> {
        let origin = context.origin.clone();
        let function = self
            .vm
            .load_chunk(&luau::Chunk::new(source, luau_origin(&origin)))?;
        let thread = self.vm.create_thread(&function)?;
        self.vm.sandbox_thread(&thread)?;
        let scheduler = self.vm.data().get::<LocalScheduler>()?;
        scheduler.spawn_luau_thread(context, self.vm.clone(), thread.clone(), Vec::new());
        drive_thread(
            &self.tokio_runtime,
            &scheduler,
            &thread,
            ThreadDriveOptions::default(),
        )
    }

    pub fn poll_background_tasks(&self) -> usize {
        let Ok(scheduler) = self.vm.data().get::<LocalScheduler>() else {
            return 0;
        };
        {
            let _guard = self.tokio_runtime.enter();
            scheduler.poll_ready();
        }
        scheduler.remove_finished()
    }

    pub fn next_scheduler_delay(&self) -> Option<Duration> {
        let scheduler = self.vm.data().get::<LocalScheduler>().ok()?;
        if !scheduler.has_pending() {
            return None;
        }
        scheduler.next_wake_delay()
    }
}

fn plugin_origin(plugin_id: impl Into<Arc<str>>, path: impl Into<Arc<str>>) -> ChunkOrigin {
    let plugin = plugin_id.into();
    let path = path.into();
    ChunkOrigin {
        module: Some(ModuleId(Arc::from(format!("plugins/{plugin}/{path}")))),
        plugin: Some(plugin.clone()),
        path: Some(Arc::from(format!("plugins/{plugin}/{path}"))),
    }
}

fn luau_origin(origin: &ChunkOrigin) -> luau::ChunkOrigin {
    luau::ChunkOrigin {
        module: origin
            .module
            .as_ref()
            .map(|module| luau::ModuleId(module.0.clone())),
        plugin: origin.plugin.clone(),
        path: origin.path.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DependencyEntry,
        DependencyGroup,
        LoadedPlugin,
        NormalizedDependency,
        PluginLoadError,
        PluginManager,
        PluginManifest,
        RuntimeBuilder,
        normalize_dependencies,
        validate_plugin_id,
    };
    use crate::{
        CallContext,
        ChunkOrigin,
        FunctionSpec,
        GlobalSpec,
        MemorySourceLoader,
        ModuleExport,
        ModuleSpec,
    };
    use harmony_luau as luau;
    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::sync::{
        Arc,
        Mutex,
        atomic::{
            AtomicUsize,
            Ordering,
        },
    };

    #[derive(Clone)]
    struct TestStore(&'static str);

    fn manifest(id: &str, scopes: &[&str]) -> PluginManifest {
        PluginManifest {
            schema_version: super::PLUGIN_SCHEMA_VERSION,
            id: id.to_string(),
            name: id.to_string(),
            version: "1.0.0".to_string(),
            description: String::new(),
            entrypoint: "init.luau".to_string(),
            scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
            dependencies: Vec::new(),
        }
    }

    fn manifest_arc(manifests: Vec<PluginManifest>) -> Arc<[PluginManifest]> {
        Arc::from(manifests)
    }

    #[test]
    fn accepts_well_formed_ids() {
        for id in ["demo", "Demo_Plugin", "my-plugin", "plugin123", "a"] {
            assert!(validate_plugin_id(id).is_ok(), "rejected {id:?}");
        }
    }

    #[test]
    fn rejects_empty() {
        assert!(validate_plugin_id("").is_err());
    }

    #[test]
    fn rejects_slash() {
        assert!(validate_plugin_id("foo/bar").is_err());
    }

    #[test]
    fn rejects_whitespace_dots_and_other_symbols() {
        for id in [
            "foo bar", "foo.bar", "foo:bar", "foo@bar", "foo/..", "foo\\bar",
        ] {
            assert!(validate_plugin_id(id).is_err(), "accepted {id:?}");
        }
    }

    #[test]
    fn parses_string_shorthand_as_required_single_alternative() {
        let json = r#"["musicbrainz", "lrclib"]"#;
        let entries: Vec<DependencyEntry> = serde_json::from_str(json).unwrap();
        let normalized = normalize_dependencies("dependent", &entries).unwrap();
        assert_eq!(normalized.len(), 2);
        assert_eq!(normalized[0].alternatives, vec!["musicbrainz"]);
        assert!(normalized[0].required);
        assert_eq!(normalized[1].alternatives, vec!["lrclib"]);
        assert!(normalized[1].required);
    }

    #[test]
    fn parses_group_with_required_default_true() {
        let json = r#"[{"any_of": ["musicbrainz", "discogs"]}]"#;
        let entries: Vec<DependencyEntry> = serde_json::from_str(json).unwrap();
        let normalized = normalize_dependencies("dependent", &entries).unwrap();
        assert_eq!(normalized.len(), 1);
        assert_eq!(normalized[0].alternatives, vec!["musicbrainz", "discogs"]);
        assert!(normalized[0].required);
    }

    #[test]
    fn parses_group_with_required_false() {
        let json = r#"[{"any_of": ["theaudiodb"], "required": false}]"#;
        let entries: Vec<DependencyEntry> = serde_json::from_str(json).unwrap();
        let normalized = normalize_dependencies("dependent", &entries).unwrap();
        assert_eq!(normalized.len(), 1);
        assert_eq!(normalized[0].alternatives, vec!["theaudiodb"]);
        assert!(!normalized[0].required);
    }

    #[test]
    fn rejects_unknown_group_keys() {
        let json = r#"[{"any_off": ["musicbrainz"]}]"#;
        let parsed: Result<Vec<DependencyEntry>, _> = serde_json::from_str(json);
        assert!(parsed.is_err(), "expected error for unknown key");
    }

    #[test]
    fn rejects_empty_any_of() {
        let entries = vec![DependencyEntry::Group(DependencyGroup {
            any_of: vec![],
            required: true,
        })];
        let err = normalize_dependencies("dependent", &entries).unwrap_err();
        assert!(matches!(err, PluginLoadError::EmptyDependencyGroup { .. }));
    }

    #[test]
    fn rejects_self_dependency() {
        let entries = vec![DependencyEntry::Id("dependent".to_string())];
        let err = normalize_dependencies("dependent", &entries).unwrap_err();
        assert!(matches!(err, PluginLoadError::SelfDependency { .. }));
    }

    #[test]
    fn rejects_duplicate_alternative_within_group() {
        let entries = vec![DependencyEntry::Group(DependencyGroup {
            any_of: vec!["musicbrainz".to_string(), "musicbrainz".to_string()],
            required: true,
        })];
        let err = normalize_dependencies("dependent", &entries).unwrap_err();
        assert!(matches!(
            err,
            PluginLoadError::DuplicateDependencyAlternative { .. }
        ));
    }

    #[test]
    fn rejects_invalid_dependency_id() {
        let entries = vec![DependencyEntry::Id("not/valid".to_string())];
        let err = normalize_dependencies("dependent", &entries).unwrap_err();
        assert!(matches!(err, PluginLoadError::InvalidDependencyId { .. }));
    }

    #[test]
    fn rejects_too_many_dependency_entries() {
        let entries: Vec<DependencyEntry> = (0..super::PLUGIN_CONFIG_MAX_DEPENDENCIES + 1)
            .map(|i| DependencyEntry::Id(format!("dep{i}")))
            .collect();
        let err = normalize_dependencies("dependent", &entries).unwrap_err();
        assert!(matches!(err, PluginLoadError::TooManyDependencies { .. }));
    }

    #[test]
    fn rejects_too_many_dependency_alternatives() {
        let alts: Vec<String> = (0..super::PLUGIN_CONFIG_MAX_DEPENDENCY_ALTERNATIVES + 1)
            .map(|i| format!("alt{i}"))
            .collect();
        let entries = vec![DependencyEntry::Group(DependencyGroup {
            any_of: alts,
            required: true,
        })];
        let err = normalize_dependencies("dependent", &entries).unwrap_err();
        assert!(matches!(
            err,
            PluginLoadError::TooManyDependencyAlternatives { .. }
        ));
    }

    fn loaded_plugin(id: &str, deps: Vec<NormalizedDependency>) -> LoadedPlugin {
        LoadedPlugin {
            manifest: PluginManifest {
                schema_version: super::PLUGIN_SCHEMA_VERSION,
                id: id.to_string(),
                name: id.to_string(),
                version: "0.0.0".to_string(),
                description: String::new(),
                entrypoint: "init.luau".to_string(),
                scopes: Vec::new(),
                dependencies: Vec::new(),
            },
            directory: PathBuf::from(format!("plugins/{id}")),
            entrypoint_path: PathBuf::from(format!("plugins/{id}/init.luau")),
            declared_scopes: HashSet::new(),
            dependencies: deps,
        }
    }

    fn manager_with(plugins: Vec<LoadedPlugin>) -> PluginManager {
        let mut mgr = PluginManager::new(PathBuf::from("plugins"));
        for plugin in plugins {
            mgr.by_root
                .insert(plugin.directory.clone(), plugin.manifest.id.clone());
            mgr.plugins.insert(plugin.manifest.id.clone(), plugin);
        }
        mgr
    }

    fn required(alts: &[&str]) -> NormalizedDependency {
        NormalizedDependency {
            alternatives: alts.iter().map(|s| s.to_string()).collect(),
            required: true,
        }
    }

    fn optional(alts: &[&str]) -> NormalizedDependency {
        NormalizedDependency {
            alternatives: alts.iter().map(|s| s.to_string()).collect(),
            required: false,
        }
    }

    #[test]
    fn topological_order_orders_after_every_installed_alternative() {
        let mgr = manager_with(vec![
            loaded_plugin("consumer", vec![required(&["a", "b"])]),
            loaded_plugin("a", vec![]),
            loaded_plugin("b", vec![]),
        ]);
        let order: Vec<String> = mgr
            .topological_order()
            .into_iter()
            .map(|p| p.manifest.id)
            .collect();
        let consumer_pos = order.iter().position(|id| id == "consumer").unwrap();
        let a_pos = order.iter().position(|id| id == "a").unwrap();
        let b_pos = order.iter().position(|id| id == "b").unwrap();
        assert!(a_pos < consumer_pos);
        assert!(b_pos < consumer_pos);
    }

    #[test]
    fn topological_order_is_stable_via_id_sort() {
        let mgr = manager_with(vec![
            loaded_plugin("zeta", vec![]),
            loaded_plugin("alpha", vec![]),
            loaded_plugin("mu", vec![]),
        ]);
        let order: Vec<String> = mgr
            .topological_order()
            .into_iter()
            .map(|p| p.manifest.id)
            .collect();
        assert_eq!(order, vec!["alpha", "mu", "zeta"]);
    }

    #[test]
    fn topological_order_respects_optional_edges_when_provider_installed() {
        let mgr = manager_with(vec![
            loaded_plugin("consumer", vec![optional(&["provider"])]),
            loaded_plugin("provider", vec![]),
        ]);
        let order: Vec<String> = mgr
            .topological_order()
            .into_iter()
            .map(|p| p.manifest.id)
            .collect();
        assert_eq!(order, vec!["provider", "consumer"]);
    }

    #[test]
    fn resolve_prunes_plugins_with_missing_required_dep() {
        let mut mgr = manager_with(vec![loaded_plugin(
            "consumer",
            vec![required(&["missing"])],
        )]);
        let errors = mgr.resolve_dependencies();
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0],
            PluginLoadError::UnsatisfiedRequiredDependency { .. }
        ));
        assert!(mgr.get_plugin("consumer").is_none());
    }

    #[test]
    fn resolve_keeps_plugins_with_satisfied_or_group() {
        let mut mgr = manager_with(vec![
            loaded_plugin("consumer", vec![required(&["missing", "provider"])]),
            loaded_plugin("provider", vec![]),
        ]);
        let errors = mgr.resolve_dependencies();
        assert!(errors.is_empty());
        assert!(mgr.get_plugin("consumer").is_some());
    }

    #[test]
    fn resolve_keeps_plugins_with_missing_optional_dep() {
        let mut mgr = manager_with(vec![loaded_plugin(
            "consumer",
            vec![optional(&["missing"])],
        )]);
        let errors = mgr.resolve_dependencies();
        assert!(errors.is_empty());
        assert!(mgr.get_plugin("consumer").is_some());
    }

    #[test]
    fn resolve_cascades_pruning_through_chain() {
        // c → b → a; a missing → b dropped → c dropped.
        let mut mgr = manager_with(vec![
            loaded_plugin("c", vec![required(&["b"])]),
            loaded_plugin("b", vec![required(&["a"])]),
        ]);
        let errors = mgr.resolve_dependencies();
        assert_eq!(errors.len(), 2);
        assert!(mgr.get_plugin("b").is_none());
        assert!(mgr.get_plugin("c").is_none());
    }

    #[test]
    fn resolve_detects_and_prunes_cycle() {
        let mut mgr = manager_with(vec![
            loaded_plugin("a", vec![required(&["b"])]),
            loaded_plugin("b", vec![required(&["a"])]),
        ]);
        let errors = mgr.resolve_dependencies();
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, PluginLoadError::DependencyCycle { .. })),
            "expected DependencyCycle error, got {errors:?}",
        );
        assert!(mgr.get_plugin("a").is_none());
        assert!(mgr.get_plugin("b").is_none());
    }

    #[test]
    fn runtime_builder_installs_require_and_loads_memory_sources() -> anyhow::Result<()> {
        let mut loader = MemorySourceLoader::new();
        loader.insert("alias:demo/util", b"return { answer = 42 }".as_slice());
        let runtime = RuntimeBuilder::new(loader, manifest_arc(Vec::new())).build()?;

        let values = runtime.eval_source_with_context(
            Arc::<[u8]>::from(
                &br#"
                    local util = require("@demo/util")
                    return util.answer
                "#[..],
            ),
            CallContext::default(),
        )?;

        assert_eq!(values, vec![luau::Value::Number(42.0)]);
        Ok(())
    }

    #[test]
    fn runtime_builder_enforces_manifest_capability_policy() -> anyhow::Result<()> {
        let runtime = RuntimeBuilder::new(
            MemorySourceLoader::new(),
            manifest_arc(vec![manifest("demo", &[])]),
        )
        .module_specs(vec![
            ModuleSpec::new("demo/secret")
                .capability("demo.secret")
                .function(FunctionSpec::sync_fn("value").call(|mut frame| {
                    frame.returns.write("secret")?;
                    Ok(())
                })),
        ])
        .build()?;

        let error = runtime
            .eval_source_with_context(
                Arc::<[u8]>::from(&b"return require('@demo/secret').value()"[..]),
                CallContext {
                    origin: ChunkOrigin {
                        plugin: Some(Arc::from("demo")),
                        ..ChunkOrigin::default()
                    },
                    ..CallContext::default()
                },
            )
            .expect_err("capability should be denied");

        assert!(error.to_string().contains("demo.secret"));
        Ok(())
    }

    #[test]
    fn runtime_builder_registers_native_modules() -> anyhow::Result<()> {
        let runtime = RuntimeBuilder::new(
            MemorySourceLoader::new(),
            manifest_arc(vec![manifest("demo", &["demo.math"])]),
        )
        .module_specs(vec![
            ModuleSpec::new("demo/math")
                .capability("demo.math")
                .function(FunctionSpec::sync_fn("answer").call(|mut frame| {
                    frame.returns.write(42_i64)?;
                    Ok(())
                })),
        ])
        .build()?;

        let values = runtime.eval_source_with_context(
            Arc::<[u8]>::from(&b"return require('@demo/math').answer()"[..]),
            CallContext {
                origin: ChunkOrigin {
                    plugin: Some(Arc::from("demo")),
                    ..ChunkOrigin::default()
                },
                ..CallContext::default()
            },
        )?;

        assert_eq!(values, vec![luau::Value::Integer(42)]);
        Ok(())
    }

    #[test]
    fn runtime_builder_configure_vm_inserts_host_data() -> anyhow::Result<()> {
        let runtime = RuntimeBuilder::new(
            MemorySourceLoader::new(),
            manifest_arc(vec![manifest("demo", &["demo.store"])]),
        )
        .configure_vm(|vm| {
            vm.data().insert(TestStore("configured"))?;
            Ok(())
        })
        .module_specs(vec![
            ModuleSpec::new("demo/store")
                .capability("demo.store")
                .function(FunctionSpec::sync_fn("value").call(|mut frame| {
                    let store = frame.vm.data().get::<TestStore>()?;
                    frame.returns.write(store.0)?;
                    Ok(())
                })),
        ])
        .build()?;

        let values = runtime.eval_source_with_context(
            Arc::<[u8]>::from(&b"return require('@demo/store').value()"[..]),
            CallContext {
                origin: ChunkOrigin {
                    plugin: Some(Arc::from("demo")),
                    ..ChunkOrigin::default()
                },
                ..CallContext::default()
            },
        )?;

        assert_eq!(values, vec![luau::Value::String(b"configured".to_vec())]);
        Ok(())
    }

    #[test]
    fn runtime_eval_source_returns_values_and_propagates_errors() -> anyhow::Result<()> {
        let runtime = RuntimeBuilder::new(MemorySourceLoader::new(), manifest_arc(Vec::new()))
            .global_specs(vec![GlobalSpec::new("test/noop")])
            .build()?;

        let values = runtime.eval_source_with_context(
            Arc::<[u8]>::from(&b"return 'ok'"[..]),
            CallContext::default(),
        )?;
        assert_eq!(values, vec![luau::Value::String(b"ok".to_vec())]);

        let error = runtime
            .eval_source_with_context(
                Arc::<[u8]>::from(&b"error('source failed')"[..]),
                CallContext::default(),
            )
            .expect_err("source should fail");
        assert!(error.to_string().contains("source failed"));
        Ok(())
    }

    #[test]
    fn runtime_exec_all_runs_loaded_plugins_in_runtime_order() -> anyhow::Result<()> {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

        let order = Arc::new(Mutex::new(Vec::<String>::new()));
        let order_for_module = order.clone();
        let module = ModuleSpec::new("test/order")
            .capability("test.order")
            .function(FunctionSpec::sync_fn("record").call(move |mut frame| {
                let id: String = frame.args.read_named("id")?;
                order_for_module
                    .lock()
                    .expect("order mutex poisoned")
                    .push(id);
                Ok(())
            }))
            .install(|_| Ok(ModuleExport::new(())));

        let root = std::env::temp_dir().join(format!(
            "harmony-core-runtime-test-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let alpha_dir = root.join("alpha");
        let beta_dir = root.join("beta");
        std::fs::create_dir_all(&alpha_dir)?;
        std::fs::create_dir_all(&beta_dir)?;
        let alpha_entrypoint = alpha_dir.join("init.luau");
        let beta_entrypoint = beta_dir.join("init.luau");
        std::fs::write(&alpha_entrypoint, "require('@test/order').record('alpha')")?;
        std::fs::write(&beta_entrypoint, "require('@test/order').record('beta')")?;

        let alpha = LoadedPlugin {
            manifest: manifest("alpha", &["test.order"]),
            directory: alpha_dir,
            entrypoint_path: alpha_entrypoint,
            declared_scopes: HashSet::new(),
            dependencies: Vec::new(),
        };
        let beta = LoadedPlugin {
            manifest: manifest("beta", &["test.order"]),
            directory: beta_dir,
            entrypoint_path: beta_entrypoint,
            declared_scopes: HashSet::new(),
            dependencies: Vec::new(),
        };
        let runtime = RuntimeBuilder::new(
            MemorySourceLoader::new(),
            manifest_arc(vec![alpha.manifest.clone(), beta.manifest.clone()]),
        )
        .plugins(Arc::from(vec![alpha, beta]))
        .module_specs(vec![module])
        .build()?;

        runtime.exec_all()?;

        assert_eq!(
            *order.lock().expect("order mutex poisoned"),
            vec!["alpha".to_string(), "beta".to_string()]
        );
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }
}
