// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use serde::Deserialize;
use std::collections::{
    HashMap,
    HashSet,
};
use std::fmt;
use std::path::{
    Path,
    PathBuf,
};
use std::sync::Arc;

pub(crate) const PLUGIN_CONFIG_FILENAME: &str = "plugin.json";
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

        Ok(errors)
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
        if let Some(previous) = self
            .plugins
            .insert(plugin.manifest.id.clone(), plugin.clone())
        {
            self.by_root.remove(&previous.directory);
        }
        self.by_root
            .insert(plugin.directory.clone(), plugin.manifest.id.clone());
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

        // Runtime identity for required modules comes from the directory
        // basename (see `parse_plugin_id` in lyra-server). Entrypoint
        // chunks use `manifest.id`. If these disagree, top-level code
        // attributes to manifest.id but `require("sub")` modules attribute
        // to the directory — teardown clears one bucket and the other
        // leaks forever. Enforce the constraint here rather than audit
        // every chunk-name call site.
        let directory_basename = dir.file_name().and_then(|s| s.to_str()).unwrap_or_default();
        if directory_basename != manifest.id {
            return Err(PluginLoadError::DirectoryIdMismatch {
                plugin_id: manifest.id,
                directory: directory_basename.to_string(),
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
}

#[cfg(test)]
mod tests {
    use super::{
        DependencyEntry,
        DependencyGroup,
        PluginLoadError,
        normalize_dependencies,
        validate_plugin_id,
    };

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

}
