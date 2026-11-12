// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use semver::Version;
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

use super::PluginLoadError;
use super::manifest::{
    LoadedManifest,
    PLUGIN_CONFIG_FILENAME,
    PLUGIN_CONFIG_MAX_BYTES,
    PluginHost,
    PluginManifest,
    validate_plugin_id,
};

#[derive(Debug, Clone)]
pub struct LoadedPlugin {
    pub manifest: PluginManifest,
    pub directory: PathBuf,
    /// Deduplicated, validated copy of `manifest.scopes` kept as `Arc<str>`
    /// so the runtime gate can share allocations across lookups.
    pub declared_scopes: HashSet<Arc<str>>,
}

/// Why a discovery left plugins out.
#[derive(Debug)]
pub struct PluginDiscoveryError {
    /// The names of the plugin directories left out: one, or every member
    /// of a dependency cycle.
    pub directories: Vec<String>,
    pub error: PluginLoadError,
}

impl fmt::Display for PluginDiscoveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for PluginDiscoveryError {}

/// The first dependency of `plugin` that `installed` leaves unmet: a
/// required one that is missing, or any whose installed version is outside
/// its requirement.
fn unmet_dependency<'a>(
    plugin: &PluginManifest,
    installed: impl Fn(&str) -> Option<&'a Version>,
) -> Option<PluginLoadError> {
    plugin
        .dependencies
        .iter()
        .find_map(|(dep_id, dependency)| match installed(dep_id) {
            None if dependency.optional => None,
            None => Some(PluginLoadError::MissingDependency {
                plugin_id: plugin.id.clone(),
                dependency: dep_id.clone(),
                requirement: dependency.version.clone(),
            }),
            Some(version) if dependency.version.matches(version) => None,
            Some(version) => Some(PluginLoadError::IncompatibleDependency {
                plugin_id: plugin.id.clone(),
                dependency: dep_id.clone(),
                requirement: dependency.version.clone(),
                installed: version.clone(),
            }),
        })
}

pub struct PluginManager {
    pub(super) plugins: HashMap<String, LoadedPlugin>,
    /// `LoadedPlugin.directory` → plugin id. Populated at insert time.
    /// Lets the runtime gate resolve a chunk's plugin root to an identity
    /// without re-parsing chunk names (which was the forgery oracle).
    pub(super) by_root: HashMap<PathBuf, String>,
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

    /// A manifest naming a scope outside `host.scope_ids` fails load — the
    /// plugin is asking for something the runtime cannot grant — as does one
    /// whose requirement on the host excludes `host.version`.
    pub fn discover_plugins(
        &mut self,
        host: &PluginHost,
    ) -> Result<Vec<PluginDiscoveryError>, std::io::Error> {
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
            let directory = entry.file_name().to_string_lossy().into_owned();
            if directory.starts_with('.') {
                continue;
            }

            match self.load_plugin(&path, host) {
                Ok(plugin) => {
                    if self.plugins.contains_key(&plugin.manifest.id) {
                        errors.push(PluginDiscoveryError {
                            directories: vec![directory],
                            error: PluginLoadError::DuplicateId(plugin.manifest.id),
                        });
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
                Err(error) => errors.push(PluginDiscoveryError {
                    directories: vec![directory],
                    error,
                }),
            }
        }

        errors.extend(self.resolve_dependencies());

        Ok(errors)
    }

    /// Validate cross-plugin invariants and prune offenders so the
    /// runtime never sees a half-resolved set.
    pub(super) fn resolve_dependencies(&mut self) -> Vec<PluginDiscoveryError> {
        let mut errors = Vec::new();

        // Cascading prunes: dropping A can unsatisfy B's dependency on A,
        // whether A had an unmet dependency itself or sat in a cycle.
        loop {
            let mut ids: Vec<&String> = self.plugins.keys().collect();
            ids.sort();
            let unmet: Vec<(String, PluginLoadError)> = ids
                .into_iter()
                .filter_map(|id| {
                    unmet_dependency(&self.plugins[id].manifest, |dep_id| {
                        self.installed_version(dep_id)
                    })
                    .map(|error| (id.clone(), error))
                })
                .collect();

            if !unmet.is_empty() {
                for (id, error) in unmet {
                    self.remove_plugin(&id);
                    errors.push(PluginDiscoveryError {
                        directories: vec![id],
                        error,
                    });
                }
                continue;
            }

            let cycle_members = self.cycle_members();
            if cycle_members.is_empty() {
                break;
            }
            for plugin_id in &cycle_members {
                self.remove_plugin(plugin_id);
            }
            errors.push(PluginDiscoveryError {
                directories: cycle_members.clone(),
                error: PluginLoadError::DependencyCycle {
                    plugins: cycle_members,
                },
            });
        }

        errors
    }

    fn remove_plugin(&mut self, plugin_id: &str) {
        if let Some(removed) = self.plugins.remove(plugin_id) {
            self.by_root.remove(&removed.directory);
        }
    }

    /// Plugins that reach themselves through installed dependencies, sorted.
    /// Plugins only downstream of a cycle are left out.
    fn cycle_members(&self) -> Vec<String> {
        let (_, unscheduled) = kahn_visit(&self.plugins);
        unscheduled
            .into_iter()
            .filter(|id| self.reaches_itself(id))
            .collect()
    }

    fn reaches_itself(&self, plugin_id: &str) -> bool {
        let mut stack = vec![plugin_id];
        let mut seen = HashSet::new();
        while let Some(current) = stack.pop() {
            for dep_id in self.plugins[current].manifest.dependencies.keys() {
                if dep_id == plugin_id {
                    return true;
                }
                if self.plugins.contains_key(dep_id) && seen.insert(dep_id.as_str()) {
                    stack.push(dep_id);
                }
            }
        }
        false
    }

    pub fn reload_plugin(
        &mut self,
        plugin_id: &str,
        host: &PluginHost,
    ) -> Result<(), PluginLoadError> {
        if let Err(reason) = validate_plugin_id(plugin_id) {
            return Err(PluginLoadError::InvalidPluginId {
                plugin_id: plugin_id.to_string(),
                reason,
            });
        }

        let plugin_dir = self.plugins_dir.join(plugin_id);
        let plugin = self.load_plugin(&plugin_dir, host)?;

        // Check deps before mutating — a failing reload must leave the
        // previous version intact, and so must one whose new version a
        // loaded dependent no longer accepts.
        let installed = |dep_id: &str| {
            if dep_id == plugin.manifest.id {
                Some(&plugin.manifest.version)
            } else {
                self.installed_version(dep_id)
            }
        };
        if let Some(error) = unmet_dependency(&plugin.manifest, installed) {
            return Err(error);
        }
        let mut dependents: Vec<&LoadedPlugin> = self
            .plugins
            .values()
            .filter(|other| {
                other
                    .manifest
                    .dependencies
                    .contains_key(&plugin.manifest.id)
            })
            .collect();
        dependents.sort_by(|left, right| left.manifest.id.cmp(&right.manifest.id));
        if let Some(error) = dependents
            .into_iter()
            .find_map(|dependent| unmet_dependency(&dependent.manifest, installed))
        {
            return Err(error);
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
        let (_, cycle_members) = kahn_visit(&self.plugins);
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
}

pub(super) fn read_manifest_bytes(config_path: &Path) -> Result<Vec<u8>, PluginLoadError> {
    if !config_path.exists() {
        return Err(PluginLoadError::ConfigNotFound(config_path.to_path_buf()));
    }

    // DoS cap — check size before reading the whole file into memory.
    match std::fs::metadata(config_path) {
        Ok(meta) if meta.len() > PLUGIN_CONFIG_MAX_BYTES => {
            return Err(PluginLoadError::ConfigTooLarge {
                path: config_path.to_path_buf(),
                bytes: meta.len(),
                max: PLUGIN_CONFIG_MAX_BYTES,
            });
        }
        Ok(_) => {}
        Err(e) => {
            return Err(PluginLoadError::ConfigParseError {
                path: config_path.to_path_buf(),
                error: e.to_string(),
            });
        }
    }

    std::fs::read(config_path).map_err(|e| PluginLoadError::ConfigParseError {
        path: config_path.to_path_buf(),
        error: e.to_string(),
    })
}

impl PluginManager {
    fn installed_version(&self, plugin_id: &str) -> Option<&Version> {
        self.plugins
            .get(plugin_id)
            .map(|plugin| &plugin.manifest.version)
    }

    fn load_plugin(&self, dir: &Path, host: &PluginHost) -> Result<LoadedPlugin, PluginLoadError> {
        let plugin = Self::load_plugin_dir(dir, host)?;

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
    /// before they land under `plugins_dir`. Runs [`LoadedManifest::load`],
    /// then the checks that need the directory's files.
    pub fn load_plugin_dir(dir: &Path, host: &PluginHost) -> Result<LoadedPlugin, PluginLoadError> {
        let config_path = dir.join(PLUGIN_CONFIG_FILENAME);
        let bytes = read_manifest_bytes(&config_path)?;
        let LoadedManifest {
            manifest,
            declared_scopes,
        } = LoadedManifest::load(&config_path, &bytes, host)?;

        if let Some(entrypoint) = manifest.entrypoint.as_deref() {
            let path = dir.join(entrypoint);
            if !path.exists() {
                return Err(PluginLoadError::EntrypointNotFound {
                    plugin_id: manifest.id,
                    path,
                });
            }
        }

        Ok(LoadedPlugin {
            manifest,
            directory: dir.to_path_buf(),
            declared_scopes,
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
}

// Kahn's algorithm. `unscheduled` is non-empty iff the graph has a
// cycle, and lists ids in or downstream of one.
fn kahn_visit(plugins: &HashMap<String, LoadedPlugin>) -> (Vec<String>, Vec<String>) {
    let (mut in_degree, adjacency) = build_dependency_graph(plugins);

    let mut ready: BTreeSet<String> = in_degree
        .iter()
        .filter_map(|(id, deg)| if *deg == 0 { Some(id.clone()) } else { None })
        .collect();

    let mut scheduled: Vec<String> = Vec::with_capacity(plugins.len());
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

// Required or not, once a dep is present the dependent runs after it.
fn build_dependency_graph(
    plugins: &HashMap<String, LoadedPlugin>,
) -> (HashMap<String, usize>, HashMap<String, Vec<String>>) {
    let mut in_degree: HashMap<String, usize> = plugins.keys().map(|id| (id.clone(), 0)).collect();
    let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();

    for plugin in plugins.values() {
        let dependent = &plugin.manifest.id;
        for dep_id in plugin.manifest.dependencies.keys() {
            if !plugins.contains_key(dep_id) {
                continue;
            }
            adjacency
                .entry(dep_id.clone())
                .or_default()
                .push(dependent.clone());
            if let Some(deg) = in_degree.get_mut(dependent) {
                *deg += 1;
            }
        }
    }

    (in_degree, adjacency)
}

/// `plugins` in execution order: each runs after every installed plugin it
/// depends on, and ties break by id so the order is stable.
pub(super) fn execution_order(
    plugins: impl IntoIterator<Item = LoadedPlugin>,
) -> anyhow::Result<Vec<LoadedPlugin>> {
    let mut by_id = HashMap::new();
    for plugin in plugins {
        let id = plugin.manifest.id.clone();
        if by_id.insert(id.clone(), plugin).is_some() {
            anyhow::bail!("plugin '{id}' is listed more than once");
        }
    }
    let (scheduled, unscheduled) = kahn_visit(&by_id);
    if !unscheduled.is_empty() {
        anyhow::bail!("plugin dependencies form a cycle: {unscheduled:?}");
    }
    Ok(scheduled
        .into_iter()
        .filter_map(|id| by_id.remove(&id))
        .collect())
}
