// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use semver::{
    Version,
    VersionReq,
};
use serde::de::{
    self,
    DeserializeSeed,
    MapAccess,
    Visitor,
};
use serde::{
    Deserialize,
    Deserializer,
};
use std::collections::{
    BTreeMap,
    BTreeSet,
    HashMap,
    HashSet,
};
use std::fmt;
use std::path::{
    Path,
    PathBuf,
};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{
    Duration,
    Instant,
};

use anyhow::Context;
use harmony_luau as luau;

use crate::{
    CallContext,
    GatedModule,
    GlobalSpec,
    LocalScheduler,
    ModuleId,
    SourceLoader,
    TokioRuntimeContext,
    luau::{
        RequireRuntime,
        ThreadDriveOptions,
        drive_thread,
        install_globals,
    },
    modules::{
        CapabilityPolicy,
        load_chunk_with_require,
    },
    scopes::ScopeGrant,
};
use harmony_luau::{
    CapabilityId,
    ChunkOrigin,
    TaskGroupId,
};

pub const PLUGIN_CONFIG_FILENAME: &str = "plugin.json";
pub const PLUGIN_SCHEMA_VERSION: u32 = 1;

/// Hard caps applied to `plugin.json` before `serde_json` parses it.
/// Prevents a hostile or malformed plugin config from OOMing `discover_plugins`.
pub const PLUGIN_CONFIG_MAX_BYTES: u64 = 64 * 1024;
pub(crate) const PLUGIN_CONFIG_MAX_SCOPE_LEN: usize = 128;
pub(crate) const PLUGIN_CONFIG_MAX_SCOPE_REASON_CHARS: usize = 200;
pub(crate) const PLUGIN_CONFIG_MAX_DEPENDENCIES: usize = 32;
/// The most items a scope's grant may list.
pub const PLUGIN_CONFIG_MAX_SCOPE_GRANT_ITEMS: usize = 32;

/// The fixed top-level fields of `plugin.json` and of the release entries
/// a repository's `versions.json` lists, which a host's name must not
/// shadow.
pub const RESERVED_HOST_NAMES: &[&str] = &[
    "schema_version",
    "id",
    "name",
    "version",
    "description",
    "entrypoint",
    "scopes",
    "dependencies",
    "commit",
];

/// What manifests are checked against: the running host's name, which is
/// the top-level key holding each manifest's requirement on the host, the
/// host's version, which that requirement must match, the scope ids its
/// modules register, the scopes that only narrow another, the grants some
/// scopes take, and the plugin ids it addresses something else by.
#[derive(Debug, Clone)]
pub struct PluginHost {
    name: String,
    version: Version,
    scope_ids: HashSet<Arc<str>>,
    scope_parents: HashMap<Arc<str>, Arc<str>>,
    scope_grants: HashMap<Arc<str>, Arc<dyn ScopeGrant>>,
    reserved_ids: HashSet<String>,
}

impl PluginHost {
    /// Fails when `name` is empty or one of [`RESERVED_HOST_NAMES`].
    pub fn new(
        name: impl Into<String>,
        version: Version,
        scope_ids: HashSet<Arc<str>>,
    ) -> Result<Self, InvalidHostName> {
        let name = name.into();
        if name.is_empty() || RESERVED_HOST_NAMES.contains(&name.as_str()) {
            return Err(InvalidHostName(name));
        }
        Ok(Self {
            name,
            version,
            scope_ids,
            scope_parents: HashMap::new(),
            scope_grants: HashMap::new(),
            reserved_ids: HashSet::new(),
        })
    }

    /// Has manifests declaring `scope` say what it lets them reach, in the
    /// key of the scope's entry that `grant` names. A manifest declaring
    /// the scope without that key still parses, and is refused at load.
    pub fn with_scope_grant(
        mut self,
        scope: impl Into<Arc<str>>,
        grant: Arc<dyn ScopeGrant>,
    ) -> Self {
        self.scope_grants.insert(scope.into(), grant);
        self
    }

    /// The grant `scope` takes, when it takes one.
    pub fn scope_grant(&self, scope: &str) -> Option<&dyn ScopeGrant> {
        self.scope_grants.get(scope).map(Arc::as_ref)
    }

    /// Scopes, each paired with its parent, that gate part of what the
    /// parent's module offers: a manifest declaring one must declare its
    /// parent too.
    pub fn with_scope_parents<I, S, P>(mut self, parents: I) -> Self
    where
        I: IntoIterator<Item = (S, P)>,
        S: Into<Arc<str>>,
        P: Into<Arc<str>>,
    {
        self.scope_parents.extend(
            parents
                .into_iter()
                .map(|(scope, parent)| (scope.into(), parent.into())),
        );
        self
    }

    /// Ids a manifest may not take, such as names the host's own interfaces
    /// would read as something other than a plugin.
    pub fn with_reserved_ids<I>(mut self, ids: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        self.reserved_ids.extend(ids.into_iter().map(Into::into));
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn version(&self) -> &Version {
        &self.version
    }
}

/// A host name that is empty or shadows a fixed manifest or release field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidHostName(pub String);

impl fmt::Display for InvalidHostName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            f.write_str("the host name is empty")
        } else {
            write!(
                f,
                "host name `{}` is a fixed {PLUGIN_CONFIG_FILENAME} or versions.json field",
                self.0
            )
        }
    }
}

impl std::error::Error for InvalidHostName {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginManifest {
    pub schema_version: u32,
    pub id: String,
    pub name: String,
    pub version: Version,
    /// Host versions the plugin runs on, read from the top-level key the
    /// host names.
    pub host_requirement: VersionReq,
    pub description: String,
    /// Library-only plugins omit the entrypoint: nothing runs at startup and
    /// their modules load lazily via `require("@<plugin-id>/...")`.
    pub entrypoint: Option<String>,
    /// Scopes (capability ids) the plugin declares. Every gated module
    /// the plugin `require`s must have its scope id listed here.
    /// Required on `schema_version: 1`.
    pub scopes: Vec<String>,
    /// What the plugin's author says each scope is for, by scope id, holding
    /// only the scopes given a reason. Unverified text, for hosts to show.
    pub scope_reasons: BTreeMap<String, String>,
    /// What each scope given a grant lets the plugin reach, by scope id: the
    /// grant's items as [`ScopeGrant::parse`] returned them.
    pub scope_grants: BTreeMap<String, Vec<String>>,
    /// Keyed by plugin id.
    pub dependencies: BTreeMap<String, Dependency>,
}

impl PluginManifest {
    /// Parses `plugin.json` contents, reading the requirement on `host`
    /// from the top-level key [`PluginHost::name`] names.
    pub fn from_json(json: &str, host: &PluginHost) -> Result<Self, serde_json::Error> {
        #[derive(Deserialize)]
        struct Fields {
            schema_version: u32,
            id: String,
            name: String,
            #[serde(deserialize_with = "deserialize_version")]
            version: Version,
            description: String,
            entrypoint: Option<String>,
            scopes: Vec<ScopeEntry>,
            #[serde(default, deserialize_with = "deserialize_dependencies")]
            dependencies: BTreeMap<String, Dependency>,
            #[serde(flatten, deserialize_with = "deserialize_other_fields")]
            other: serde_json::Map<String, serde_json::Value>,
        }

        let mut fields: Fields = serde_json::from_str(json)?;
        let host_requirement = match fields.other.remove(host.name()) {
            Some(requirement) => {
                parse_requirement(host.name(), &String::deserialize(requirement)?)?
            }
            None => {
                return Err(de::Error::custom(format!(
                    "missing field `{}`",
                    host.name()
                )));
            }
        };
        let DeclaredScopes {
            scopes,
            reasons: scope_reasons,
            grants: scope_grants,
        } = declared_scopes(&fields.id, fields.scopes, host)?;
        Ok(Self {
            schema_version: fields.schema_version,
            id: fields.id,
            name: fields.name,
            version: fields.version,
            host_requirement,
            description: fields.description,
            entrypoint: fields.entrypoint,
            scopes,
            scope_reasons,
            scope_grants,
            dependencies: fields.dependencies,
        })
    }
}

/// The fields a manifest has beyond the fixed ones, among them the host's
/// requirement. One named twice is rejected rather than read as either.
fn deserialize_other_fields<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<serde_json::Map<String, serde_json::Value>, D::Error> {
    struct OtherFieldsVisitor;

    impl<'de> Visitor<'de> for OtherFieldsVisitor {
        type Value = serde_json::Map<String, serde_json::Value>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("manifest fields")
        }

        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            let mut fields = serde_json::Map::new();
            while let Some(name) = map.next_key::<String>()? {
                if fields.contains_key(&name) {
                    return Err(de::Error::custom(format!("duplicate field `{name}`")));
                }
                let value = map.next_value()?;
                fields.insert(name, value);
            }
            Ok(fields)
        }
    }

    deserializer.deserialize_map(OtherFieldsVisitor)
}

/// One entry of a manifest's `scopes`: a bare scope id, or
/// `{ "id": "<scope>", "reason": "<why the plugin needs it>" }` with, for a
/// scope that takes a grant, the grant under its key.
struct ScopeEntry {
    id: String,
    reason: Option<String>,
    /// The entry's other keys, each once, in the order written.
    other: Vec<(String, serde_json::Value)>,
}

impl<'de> Deserialize<'de> for ScopeEntry {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ScopeEntryVisitor;

        impl<'de> Visitor<'de> for ScopeEntryVisitor {
            type Value = ScopeEntry;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a scope id or an object with `id` and `reason`")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<ScopeEntry, E> {
                Ok(ScopeEntry {
                    id: value.to_string(),
                    reason: None,
                    other: Vec::new(),
                })
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<ScopeEntry, A::Error> {
                let mut id: Option<String> = None;
                let mut reason: Option<String> = None;
                let mut other: Vec<(String, serde_json::Value)> = Vec::new();
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "id" => {
                            if id.replace(map.next_value()?).is_some() {
                                return Err(de::Error::duplicate_field("id"));
                            }
                        }
                        "reason" => {
                            if reason.replace(map.next_value()?).is_some() {
                                return Err(de::Error::duplicate_field("reason"));
                            }
                        }
                        _ => {
                            if other.iter().any(|(seen, _)| *seen == key) {
                                return Err(de::Error::custom(format!("duplicate field `{key}`")));
                            }
                            other.push((key, map.next_value()?));
                        }
                    }
                }
                Ok(ScopeEntry {
                    id: id.ok_or_else(|| de::Error::missing_field("id"))?,
                    reason: Some(reason.ok_or_else(|| de::Error::missing_field("reason"))?),
                    other,
                })
            }
        }

        deserializer.deserialize_any(ScopeEntryVisitor)
    }
}

struct DeclaredScopes {
    /// The scope ids, in the author's order.
    scopes: Vec<String>,
    reasons: BTreeMap<String, String>,
    grants: BTreeMap<String, Vec<String>>,
}

/// Splits plugin `plugin_id`'s `scopes` into the scope ids, the reasons
/// given for some of them and the grants `host`'s scopes take. Checked here
/// so no parsed manifest carries a reason that isn't fit to show, or a grant
/// its scope did not accept.
fn declared_scopes<E: de::Error>(
    plugin_id: &str,
    entries: Vec<ScopeEntry>,
    host: &PluginHost,
) -> Result<DeclaredScopes, E> {
    let plugin = truncate_for_error(plugin_id);
    let mut scopes = Vec::with_capacity(entries.len());
    let mut reasons = BTreeMap::new();
    let mut grants = BTreeMap::new();
    let mut listed = HashSet::with_capacity(entries.len());
    for ScopeEntry { id, reason, other } in entries {
        let scope = truncate_for_error(&id);
        if !listed.insert(id.clone()) {
            return Err(E::custom(format!(
                "plugin '{plugin}' lists scope '{scope}' more than once"
            )));
        }
        if let Some(reason) = reason {
            if let Some(problem) = scope_reason_problem(&reason) {
                return Err(E::custom(format!(
                    "plugin '{plugin}' gives scope '{scope}' an invalid reason: {problem}"
                )));
            }
            reasons.insert(id.clone(), reason);
        }
        let grant = host.scope_grant(&id);
        for (key, value) in other {
            let Some(grant) = grant.filter(|grant| grant.key() == key) else {
                return Err(E::custom(format!(
                    "plugin '{plugin}' gives scope '{scope}' an unknown field `{}`",
                    truncate_for_error(&key)
                )));
            };
            let items = grant
                .parse(&value)
                .and_then(|items| match items.len() {
                    count if count > PLUGIN_CONFIG_MAX_SCOPE_GRANT_ITEMS => Err(format!(
                        "it lists {count} items, exceeds cap of \
                         {PLUGIN_CONFIG_MAX_SCOPE_GRANT_ITEMS}"
                    )),
                    _ => Ok(items),
                })
                .map_err(|problem| {
                    E::custom(format!(
                        "plugin '{plugin}' gives scope '{scope}' an invalid `{key}`: {problem}"
                    ))
                })?;
            grants.insert(id.clone(), items);
        }
        scopes.push(id);
    }
    Ok(DeclaredScopes {
        scopes,
        reasons,
        grants,
    })
}

fn scope_reason_problem(reason: &str) -> Option<String> {
    if reason.trim().is_empty() {
        return Some("it is empty".to_string());
    }
    if reason.chars().any(breaks_single_line_text) {
        return Some("it contains a control character or line break".to_string());
    }
    (reason.chars().count() > PLUGIN_CONFIG_MAX_SCOPE_REASON_CHARS)
        .then(|| format!("it is longer than {PLUGIN_CONFIG_MAX_SCOPE_REASON_CHARS} characters"))
}

/// Control characters, including Unicode's line separators and
/// bidirectional overrides, which could disturb the text displayed around
/// a reason.
fn breaks_single_line_text(c: char) -> bool {
    c.is_control()
        || matches!(
            c,
            '\u{2028}' | '\u{2029}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}'
        )
}

/// Written as a bare version requirement, or as
/// `{ "version": "<requirement>", "optional": true }`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dependency {
    pub version: VersionReq,
    /// A missing optional dependency doesn't block load; an installed one
    /// must still match `version`, and loads first.
    pub optional: bool,
}

fn deserialize_dependencies<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<String, Dependency>, D::Error> {
    struct DependenciesVisitor;

    impl<'de> Visitor<'de> for DependenciesVisitor {
        type Value = BTreeMap<String, Dependency>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("an object mapping plugin ids to dependencies")
        }

        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            let mut dependencies = BTreeMap::new();
            while let Some(id) = map.next_key::<String>()? {
                if dependencies.contains_key(&id) {
                    return Err(de::Error::custom(format!(
                        "dependency '{id}' is listed more than once"
                    )));
                }
                let dependency = map.next_value_seed(DependencySeed(&id))?;
                dependencies.insert(id, dependency);
            }
            Ok(dependencies)
        }
    }

    deserializer.deserialize_map(DependenciesVisitor)
}

/// Deserializes the dependency on plugin `.0`, naming it in errors.
struct DependencySeed<'a>(&'a str);

impl<'de> DeserializeSeed<'de> for DependencySeed<'_> {
    type Value = Dependency;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Dependency, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for DependencySeed<'_> {
    type Value = Dependency;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a version requirement or an object with `version` and `optional`")
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Dependency, E> {
        Ok(Dependency {
            version: parse_requirement(&format!("dependency '{}'", self.0), value)?,
            optional: false,
        })
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Dependency, A::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Table {
            version: String,
            #[serde(default)]
            optional: bool,
        }

        let table = Table::deserialize(de::value::MapAccessDeserializer::new(map))?;
        Ok(Dependency {
            version: parse_requirement(&format!("dependency '{}'", self.0), &table.version)?,
            optional: table.optional,
        })
    }
}

fn deserialize_version<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Version, D::Error> {
    let value = String::deserialize(deserializer)?;
    Version::parse(&value)
        .map_err(|error| de::Error::custom(format!("invalid version {value:?}: {error}")))
}

fn parse_requirement<E: de::Error>(field: &str, value: &str) -> Result<VersionReq, E> {
    VersionReq::parse(value).map_err(|error| {
        E::custom(format!(
            "invalid {field} version requirement {value:?}: {error}"
        ))
    })
}

/// A `plugin.json` that passed every check its contents alone can answer.
/// [`PluginManager::load_manifest`] produces it without a plugin directory.
#[derive(Debug, Clone)]
pub struct LoadedManifest {
    pub manifest: PluginManifest,
    /// Deduplicated, validated copy of `manifest.scopes` kept as `Arc<str>`
    /// so the runtime gate can share allocations across lookups.
    pub declared_scopes: HashSet<Arc<str>>,
}

#[derive(Debug, Clone)]
pub struct LoadedPlugin {
    pub manifest: PluginManifest,
    pub directory: PathBuf,
    /// Deduplicated, validated copy of `manifest.scopes` kept as `Arc<str>`
    /// so the runtime gate can share allocations across lookups.
    pub declared_scopes: HashSet<Arc<str>>,
}

pub struct ManifestCapabilityPolicy {
    scopes_by_plugin: HashMap<Arc<str>, HashSet<Arc<str>>>,
    dependencies_by_plugin: HashMap<Arc<str>, HashSet<Arc<str>>>,
}

impl ManifestCapabilityPolicy {
    /// `manifests` are the loaded plugins: a declared dependency outside
    /// them, such as an optional one that failed to load, is not requirable
    /// even when its files are on disk.
    pub fn from_manifests(manifests: Arc<[PluginManifest]>) -> Self {
        let loaded: HashSet<&str> = manifests
            .iter()
            .map(|manifest| manifest.id.as_str())
            .collect();
        let mut scopes_by_plugin = HashMap::new();
        let mut dependencies_by_plugin = HashMap::new();
        for manifest in manifests.iter() {
            let id = Arc::<str>::from(manifest.id.as_str());
            let scopes = manifest
                .scopes
                .iter()
                .map(|scope| Arc::<str>::from(scope.as_str()))
                .collect::<HashSet<_>>();
            let dependencies = manifest
                .dependencies
                .keys()
                .filter(|dependency| loaded.contains(dependency.as_str()))
                .map(|dependency| Arc::<str>::from(dependency.as_str()))
                .collect::<HashSet<_>>();
            scopes_by_plugin.insert(id.clone(), scopes);
            dependencies_by_plugin.insert(id, dependencies);
        }

        Self {
            scopes_by_plugin,
            dependencies_by_plugin,
        }
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

    fn allows_plugin_dependency(&self, requester: &str, target: &str) -> bool {
        self.dependencies_by_plugin
            .get(requester)
            .is_some_and(|dependencies| dependencies.contains(target))
    }
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

// Cross-plugin resolution (dependencies satisfied, no cycles) lives
// in `resolve_dependencies` after all manifests are loaded.
fn validate_dependencies(manifest: &PluginManifest) -> Result<(), PluginLoadError> {
    if manifest.dependencies.len() > PLUGIN_CONFIG_MAX_DEPENDENCIES {
        return Err(PluginLoadError::TooManyDependencies {
            plugin_id: manifest.id.clone(),
            count: manifest.dependencies.len(),
            max: PLUGIN_CONFIG_MAX_DEPENDENCIES,
        });
    }

    for dep_id in manifest.dependencies.keys() {
        if let Err(reason) = validate_plugin_id(dep_id) {
            return Err(PluginLoadError::InvalidDependencyId {
                plugin_id: manifest.id.clone(),
                dep_id: dep_id.clone(),
                reason,
            });
        }
        if *dep_id == manifest.id {
            return Err(PluginLoadError::SelfDependency {
                plugin_id: manifest.id.clone(),
            });
        }
    }
    Ok(())
}

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

pub fn validate_plugin_id(id: &str) -> Result<(), String> {
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
    // `@self` is the require keyword for the requesting plugin's own root;
    // a plugin with this id could never be addressed.
    if id == "self" {
        return Err("'self' is a reserved id".into());
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
    fn resolve_dependencies(&mut self) -> Vec<PluginDiscoveryError> {
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

fn read_manifest_bytes(config_path: &Path) -> Result<Vec<u8>, PluginLoadError> {
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

fn parse_manifest(
    path: &Path,
    bytes: &[u8],
    host: &PluginHost,
) -> Result<PluginManifest, PluginLoadError> {
    if bytes.len() as u64 > PLUGIN_CONFIG_MAX_BYTES {
        return Err(PluginLoadError::ConfigTooLarge {
            path: path.to_path_buf(),
            bytes: bytes.len() as u64,
            max: PLUGIN_CONFIG_MAX_BYTES,
        });
    }

    // Same message `std::fs::read_to_string` gives for invalid UTF-8.
    let text = std::str::from_utf8(bytes).map_err(|_| PluginLoadError::ConfigParseError {
        path: path.to_path_buf(),
        error: "stream did not contain valid UTF-8".into(),
    })?;
    PluginManifest::from_json(text, host).map_err(|e| PluginLoadError::ConfigParseError {
        path: path.to_path_buf(),
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
    /// before they land under `plugins_dir`. Runs [`Self::load_manifest`],
    /// then the checks that need the directory's files.
    pub fn load_plugin_dir(dir: &Path, host: &PluginHost) -> Result<LoadedPlugin, PluginLoadError> {
        let config_path = dir.join(PLUGIN_CONFIG_FILENAME);
        let bytes = read_manifest_bytes(&config_path)?;
        let LoadedManifest {
            manifest,
            declared_scopes,
        } = Self::load_manifest(&config_path, &bytes, host)?;

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

    /// Parses and validates `plugin.json` contents on their own, so a plugin
    /// can be vetted before any of its files are downloaded. `path` labels
    /// where the bytes came from, such as a file path or a repository URL
    /// path, and appears in errors; it is never read.
    pub fn load_manifest(
        path: &Path,
        bytes: &[u8],
        host: &PluginHost,
    ) -> Result<LoadedManifest, PluginLoadError> {
        let manifest = parse_manifest(path, bytes, host)?;

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

        if host.reserved_ids.contains(&manifest.id) {
            return Err(PluginLoadError::InvalidPluginId {
                reason: format!("'{}' is reserved by {}", manifest.id, host.name),
                plugin_id: manifest.id,
            });
        }

        if !manifest.host_requirement.matches(&host.version) {
            return Err(PluginLoadError::IncompatibleHost {
                plugin_id: manifest.id,
                requirement: manifest.host_requirement,
                host: host.name.clone(),
                host_version: host.version.clone(),
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
            // Intern via host.scope_ids so declared_scopes entries share
            // allocations with the module registry — clones in the runtime
            // gate are ref-count bumps, not string copies.
            let Some(interned) = host.scope_ids.get(scope.as_str()) else {
                return Err(PluginLoadError::UnknownScope {
                    plugin_id: manifest.id.clone(),
                    scope: scope.clone(),
                });
            };
            declared_scopes.insert(interned.clone());
        }
        for scope in &manifest.scopes {
            if let Some(parent) = host.scope_parents.get(scope.as_str())
                && !declared_scopes.contains(parent)
            {
                return Err(PluginLoadError::ScopeWithoutParent {
                    plugin_id: manifest.id.clone(),
                    scope: scope.clone(),
                    parent: parent.to_string(),
                });
            }
        }
        for scope in &manifest.scopes {
            if let Some(grant) = host.scope_grant(scope)
                && !manifest.scope_grants.contains_key(scope)
            {
                return Err(PluginLoadError::MissingScopeGrant {
                    plugin_id: manifest.id.clone(),
                    scope: scope.clone(),
                    key: grant.key().to_string(),
                });
            }
        }

        validate_dependencies(&manifest)?;

        Ok(LoadedManifest {
            manifest,
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
fn execution_order(
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

type VmConfigurator = Box<dyn FnOnce(&luau::Vm) -> anyhow::Result<()> + 'static>;
type RunEndHook = Box<dyn Fn(&luau::Vm, &str) + 'static>;

pub struct RuntimeBuilder<L> {
    loader: L,
    manifests: Arc<[PluginManifest]>,
    plugins: Arc<[LoadedPlugin]>,
    modules: Vec<GatedModule>,
    global_specs: Vec<GlobalSpec>,
    memory_limit: usize,
    luau_resume_budget: Option<Duration>,
    clock: crate::scheduler::Clock,
    configure_vm: Vec<VmConfigurator>,
    run_end_hooks: Vec<RunEndHook>,
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
            modules: Vec::new(),
            global_specs: Vec::new(),
            memory_limit: 256 * 1024 * 1024,
            luau_resume_budget: Some(Duration::from_secs(300)),
            clock: crate::scheduler::Clock::default(),
            configure_vm: Vec::new(),
            run_end_hooks: Vec::new(),
        }
    }

    pub fn plugins(mut self, plugins: Arc<[LoadedPlugin]>) -> Self {
        self.plugins = plugins;
        self
    }

    pub fn modules(mut self, modules: Vec<GatedModule>) -> Self {
        self.modules = modules;
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

    /// Runs the runtime's scheduler by `clock`, for a test that polls it; see
    /// [`LocalScheduler::with_manual_clock`]. What runs a plugin's code to its end on the calling
    /// thread, such as [`Runtime::exec_all`], fails on such a runtime.
    #[cfg(any(test, feature = "testing"))]
    pub fn manual_clock(mut self, clock: crate::testing::ManualClock) -> Self {
        self.clock = crate::scheduler::Clock::Manual(clock);
        self
    }

    pub fn configure_vm<F>(mut self, configure: F) -> Self
    where
        F: FnOnce(&luau::Vm) -> anyhow::Result<()> + 'static,
    {
        self.configure_vm.push(Box::new(configure));
        self
    }

    /// Runs with the plugin's id after each of its runs ends, whatever ended
    /// it: a stop, a restart or a failed entrypoint.
    pub fn on_run_end<F>(mut self, hook: F) -> Self
    where
        F: Fn(&luau::Vm, &str) + 'static,
    {
        self.run_end_hooks.push(Box::new(hook));
        self
    }

    pub fn build(self) -> anyhow::Result<Runtime> {
        let vm =
            luau::Vm::with_options(luau::VmOptions::default().memory_limit(self.memory_limit))?;
        vm.open_standard_libraries(luau::StandardLibraries::all_supported())?;

        let scheduler = LocalScheduler::with_clock(self.clock);
        scheduler.set_luau_resume_budget(self.luau_resume_budget);
        let scheduler_wake = scheduler.wake_handle();
        vm.data().insert(scheduler)?;

        for configure in self.configure_vm {
            configure(&vm)?;
        }

        let require = RequireRuntime::new(
            self.loader,
            ManifestCapabilityPolicy::from_manifests(self.manifests),
        );
        for module in self.modules {
            require.register(module)?;
        }
        vm.data().insert(require)?;

        for globals in self.global_specs {
            install_globals(&vm, &ChunkOrigin::default(), &globals)?;
        }
        // Every plugin shares these globals and libraries, so none may change them.
        vm.sandbox()?;

        Ok(Runtime {
            vm,
            plugins: Arc::from(execution_order(self.plugins.iter().cloned())?),
            tokio_runtime: TokioRuntimeContext::new()?,
            run_end_hooks: Rc::from(self.run_end_hooks),
            entrypoints_cancelled: crate::Cancellation::new(scheduler_wake),
        })
    }
}

/// What starting a plugin's entrypoint came to.
enum ScheduledEntrypoint {
    NoEntrypoint,
    /// Already evaluated, by an earlier run or a dependent's `require`.
    Cached,
    Running(RunningEntrypoint),
}

/// A plugin's entrypoint running on the scheduler.
struct RunningEntrypoint {
    vm: luau::Vm,
    plugin_id: Arc<str>,
    entrypoint: Arc<str>,
    load: crate::modules::SourceLoad,
    thread: luau::Thread,
    completion: Rc<crate::LocalLuauTaskCompletion>,
    deadline: Instant,
    cancelled: crate::Cancellation,
    run_end_hooks: Rc<[RunEndHook]>,
}

impl RunningEntrypoint {
    /// Waits for the entrypoint to end, at its deadline at the latest, and records how it did: a
    /// failed entrypoint ends the run.
    async fn finish(self) -> anyhow::Result<()> {
        let result = self.ended().await;
        let finished = self
            .vm
            .data()
            .get::<RequireRuntime>()?
            .finish_source_load(self.load, result)
            .map_err(anyhow::Error::new)
            .with_context(|| {
                format!(
                    "run plugin '{}' entrypoint '{}'",
                    self.plugin_id, self.entrypoint
                )
            });
        if finished.is_err() {
            end_plugin_run(&self.vm, &self.run_end_hooks, &self.plugin_id)?;
        }
        finished.map(|_| ())
    }

    /// Cancellation wins over a completion that landed; a completion wins over the deadline,
    /// which only keeps an entrypoint from holding up its runtime for good: nobody was promised
    /// an answer by then, so a plugin that loaded isn't unloaded for having loaded late. An
    /// entrypoint that didn't complete is cancelled.
    async fn ended(&self) -> anyhow::Result<Vec<luau::Value>> {
        let mut expired = self
            .vm
            .data()
            .get::<LocalScheduler>()?
            .sleep_until(self.deadline);
        let mut cancelled = std::pin::pin!(self.cancelled.cancelled());
        let failure = std::future::poll_fn(|cx| {
            if cancelled.as_mut().poll(cx).is_ready() {
                return std::task::Poll::Ready(Err("entrypoint was cancelled"));
            }
            if let std::task::Poll::Ready(result) = self.completion.poll(cx) {
                return std::task::Poll::Ready(Ok(result));
            }
            std::pin::Pin::new(&mut expired)
                .poll(cx)
                .map(|()| Err("entrypoint did not complete"))
        })
        .await;
        match failure {
            Ok(result) => result.map_err(|error| anyhow::Error::msg(error.message)),
            Err(failure) => {
                self.vm
                    .data()
                    .get::<LocalScheduler>()?
                    .cancel_luau_thread(&self.thread);
                Err(anyhow::anyhow!(failure))
            }
        }
    }
}

/// Cancels every task of the plugin's current run, if it has one, then runs the run-end hooks.
fn end_plugin_run(vm: &luau::Vm, hooks: &[RunEndHook], plugin_id: &str) -> anyhow::Result<()> {
    let Some(group) = vm.data().get::<RequireRuntime>()?.end_plugin_run(plugin_id) else {
        return Ok(());
    };
    vm.data().get::<LocalScheduler>()?.cancel_group(group);
    for hook in hooks {
        hook(vm, plugin_id);
    }
    Ok(())
}

pub struct Runtime {
    pub vm: luau::Vm,
    /// In execution order, dependencies first.
    pub plugins: Arc<[LoadedPlugin]>,
    pub tokio_runtime: TokioRuntimeContext,
    run_end_hooks: Rc<[RunEndHook]>,
    entrypoints_cancelled: crate::Cancellation,
}

impl Runtime {
    fn scheduler(&self) -> anyhow::Result<Rc<LocalScheduler>> {
        Ok(self.vm.data().get::<LocalScheduler>()?)
    }

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

    fn loaded_plugin(&self, plugin_id: &str) -> anyhow::Result<&LoadedPlugin> {
        self.plugins
            .iter()
            .find(|plugin| plugin.manifest.id == plugin_id)
            .ok_or_else(|| anyhow::anyhow!("plugin not found: {plugin_id}"))
    }

    /// Fails when `plugin.json` on disk no longer matches the loaded
    /// manifest. Scopes and dependencies are fixed when the runtime is
    /// built, so manifest changes need a full reload rather than a restart.
    pub fn verify_plugin_manifest(&self, plugin_id: &str, host: &PluginHost) -> anyhow::Result<()> {
        let plugin = self.loaded_plugin(plugin_id)?;
        let config_path = plugin.directory.join(PLUGIN_CONFIG_FILENAME);
        let on_disk = read_manifest_bytes(&config_path)
            .and_then(|bytes| parse_manifest(&config_path, &bytes, host))
            .map_err(|error| anyhow::anyhow!("{error}"))
            .with_context(|| format!("read plugin '{plugin_id}' manifest"))?;
        if on_disk != plugin.manifest {
            anyhow::bail!(
                "plugin '{plugin_id}' manifest changed on disk; reload plugins to apply it"
            );
        }
        Ok(())
    }

    /// Drops every cached artifact owned by `plugin_id` (its evaluated
    /// sources and the module tables installed for its origin) and re-runs
    /// its entrypoint from the files on disk. Other plugins that already
    /// required its modules keep the exports they have until they restart.
    pub fn restart_plugin(&self, plugin_id: &str) -> anyhow::Result<()> {
        self.scheduler()?.ensure_can_wait()?;
        match self.start_restart_entrypoint(plugin_id)? {
            Some(entrypoint) => self.drive_entrypoint(entrypoint),
            None => Ok(()),
        }
    }

    /// [`Self::restart_plugin`] without waiting for the new entrypoint: its thread is scheduled,
    /// and the returned future, for a watcher of the runtime's scheduler to await, completes with
    /// how the restart went once the entrypoint has run. `None` when there was nothing to run.
    /// A failure to start leaves the run ended.
    pub fn start_plugin_restart(
        &self,
        plugin_id: &str,
    ) -> anyhow::Result<Option<impl Future<Output = anyhow::Result<()>> + use<>>> {
        Ok(self
            .start_restart_entrypoint(plugin_id)?
            .map(RunningEntrypoint::finish))
    }

    fn start_restart_entrypoint(
        &self,
        plugin_id: &str,
    ) -> anyhow::Result<Option<RunningEntrypoint>> {
        let plugin = self.loaded_plugin(plugin_id)?;
        self.stop_plugin(plugin_id)?;
        let require = self.vm.data().get::<RequireRuntime>()?;
        require.invalidate_plugin(plugin_id);
        let scheduler = self.vm.data().get::<LocalScheduler>()?;
        require.plugin_group(&scheduler, plugin_id);
        let started = match self.schedule_entrypoint(plugin, &require, &scheduler) {
            Ok(ScheduledEntrypoint::Running(entrypoint)) => Ok(Some(entrypoint)),
            Ok(ScheduledEntrypoint::NoEntrypoint) => Ok(None),
            // Invalidated just above, so nothing may have cached it since.
            Ok(ScheduledEntrypoint::Cached) => Err(anyhow::anyhow!(
                "plugin '{plugin_id}' entrypoint was already loaded after its sources were \
                 invalidated"
            )),
            Err(error) => Err(error),
        };
        if started.is_err() {
            self.stop_plugin(plugin_id)?;
        }
        started
    }

    fn schedule_entrypoint(
        &self,
        plugin: &LoadedPlugin,
        require: &RequireRuntime,
        scheduler: &Rc<LocalScheduler>,
    ) -> anyhow::Result<ScheduledEntrypoint> {
        let Some(entrypoint) = plugin.manifest.entrypoint.as_deref() else {
            return Ok(ScheduledEntrypoint::NoEntrypoint);
        };
        let plugin_id = plugin.manifest.id.as_str();
        anyhow::ensure!(
            !self.entrypoints_cancelled.is_cancelled(),
            "plugin '{plugin_id}' entrypoint was cancelled"
        );
        let context = || format!("run plugin '{plugin_id}' entrypoint '{entrypoint}'");
        let specifier = format!("@{plugin_id}/{entrypoint}");
        let load = match require
            .start_source_load(&specifier, &ChunkOrigin::default())
            .map_err(anyhow::Error::new)
            .with_context(context)?
        {
            crate::modules::SourceLoadStart::Cached(_) => return Ok(ScheduledEntrypoint::Cached),
            crate::modules::SourceLoadStart::Started(load) => load,
        };
        let timeout = ThreadDriveOptions::default().timeout;
        let completion = Rc::new(crate::LocalLuauTaskCompletion::default());
        let thread = (|| {
            let function = load_chunk_with_require(
                &self.vm,
                load.source.bytes.clone(),
                &load.source.origin,
                load.task_group,
            )?;
            let thread = self.vm.create_thread(&function)?;
            self.vm.sandbox_thread(&thread)?;
            scheduler.schedule_luau_thread_with_completion(
                CallContext {
                    origin: load.source.origin.clone(),
                    task_group: load.task_group,
                    ..CallContext::default()
                },
                self.vm.clone(),
                thread.clone(),
                Vec::new(),
                completion.clone(),
            );
            anyhow::Ok(thread)
        })();
        match thread {
            Ok(thread) => Ok(ScheduledEntrypoint::Running(RunningEntrypoint {
                vm: self.vm.clone(),
                cancelled: self.entrypoints_cancelled.clone(),
                run_end_hooks: self.run_end_hooks.clone(),
                plugin_id: Arc::from(plugin_id),
                entrypoint: Arc::from(entrypoint),
                load,
                thread,
                completion,
                deadline: scheduler.now() + timeout,
            })),
            Err(error) => {
                let failed = require.finish_source_load(load, Err(error));
                failed
                    .map(|_| ScheduledEntrypoint::NoEntrypoint)
                    .map_err(anyhow::Error::new)
                    .with_context(context)
            }
        }
    }

    /// Runs `entrypoint` to its end on this thread, driving the scheduler meanwhile.
    fn drive_entrypoint(&self, entrypoint: RunningEntrypoint) -> anyhow::Result<()> {
        let scheduler = self.vm.data().get::<LocalScheduler>()?;
        let task = scheduler.luau_thread_handle(&entrypoint.thread);
        let waker = scheduler.wake_handle().waker();
        let mut cx = std::task::Context::from_waker(&waker);
        let mut finish = std::pin::pin!(entrypoint.finish());
        let result = loop {
            // Before resuming anything: a cancelled entrypoint must not run further.
            if let std::task::Poll::Ready(result) = finish.as_mut().poll(&mut cx) {
                break result;
            }
            {
                let _guard = self.tokio_runtime.enter();
                scheduler.poll_ready();
            }
            scheduler.wait_for_work(None);
        };
        if let Some(task) = task {
            scheduler.remove(task.id());
        }
        result
    }

    /// Cancels every task of the plugin's current run, including handler
    /// calls running in it and tasks spawned into it later, then runs the
    /// run-end hooks.
    pub fn stop_plugin(&self, plugin_id: &str) -> anyhow::Result<()> {
        self.loaded_plugin(plugin_id)?;
        end_plugin_run(&self.vm, &self.run_end_hooks, plugin_id)
    }

    /// Runs every plugin's entrypoint, dependencies first, and returns the
    /// ids of the plugins whose entrypoint failed.
    pub fn exec_all(&self) -> Vec<String> {
        let mut failed = Vec::new();
        for plugin in self.plugins.iter() {
            match self.exec_loaded_plugin(plugin) {
                Ok(()) => tracing::debug!("plugin '{}' executed", plugin.manifest.id),
                Err(error) => {
                    tracing::warn!("plugin '{}' error: {error:#}", plugin.manifest.id);
                    failed.push(plugin.manifest.id.clone());
                }
            }
        }
        failed
    }

    /// Starts the plugin's run, libraries included, so its sources can be
    /// required. A failed entrypoint ends the run, so the tasks it spawned
    /// before failing stop with it.
    fn exec_loaded_plugin(&self, plugin: &LoadedPlugin) -> anyhow::Result<()> {
        self.scheduler()?.ensure_can_wait()?;
        match self.start_entrypoint(plugin)? {
            Some(entrypoint) => self.drive_entrypoint(entrypoint),
            None => Ok(()),
        }
    }

    /// Starts the plugin's run and schedules its entrypoint. `None` when there is nothing to
    /// run. A failure to start leaves the run ended.
    fn start_entrypoint(&self, plugin: &LoadedPlugin) -> anyhow::Result<Option<RunningEntrypoint>> {
        let scheduler = self.scheduler()?;
        let require = self.vm.data().get::<RequireRuntime>()?;
        require.plugin_group(&scheduler, &plugin.manifest.id);
        match self.schedule_entrypoint(plugin, &require, &scheduler) {
            Ok(ScheduledEntrypoint::Running(entrypoint)) => Ok(Some(entrypoint)),
            Ok(ScheduledEntrypoint::NoEntrypoint | ScheduledEntrypoint::Cached) => Ok(None),
            Err(error) => {
                self.stop_plugin(&plugin.manifest.id)?;
                Err(error)
            }
        }
    }

    /// Once cancelled, from any thread, every entrypoint run fails as
    /// cancelled: the running one at its next yield and later ones before
    /// they start. One-way, for a runtime about to shut down, so a long
    /// entrypoint can't hold up its shutdown.
    pub fn entrypoint_cancellation(&self) -> crate::Cancellation {
        self.entrypoints_cancelled.clone()
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
        self.eval_source_with_args(source, context, Vec::new())
    }

    /// Evaluates `source` with `args` as the chunk's `...`.
    pub fn eval_source_with_args(
        &self,
        source: impl Into<Arc<[u8]>>,
        context: CallContext,
        args: Vec<luau::Value>,
    ) -> anyhow::Result<Vec<luau::Value>> {
        let scheduler = self.scheduler()?;
        scheduler.ensure_can_wait()?;
        let mut context = context;
        let thread = self.load_source_thread(source, &mut context)?;
        scheduler.spawn_luau_thread(context, self.vm.clone(), thread.clone(), args);
        drive_thread(
            &self.tokio_runtime,
            &scheduler,
            &thread,
            ThreadDriveOptions::default(),
        )
    }

    /// Loads `source` as a sandboxed thread for the caller to schedule under `context`. Plugin
    /// code runs in its plugin's current run, never the default group, so a default group in
    /// `context` becomes the plugin's.
    pub fn load_source_thread(
        &self,
        source: impl Into<Arc<[u8]>>,
        context: &mut CallContext,
    ) -> anyhow::Result<luau::Thread> {
        if let Some(plugin_id) = context.origin.plugin.as_deref()
            && context.task_group == TaskGroupId::default()
        {
            let scheduler = self.vm.data().get::<LocalScheduler>()?;
            context.task_group = self
                .vm
                .data()
                .get::<RequireRuntime>()?
                .plugin_group(&scheduler, plugin_id);
        }
        let function =
            load_chunk_with_require(&self.vm, source, &context.origin, context.task_group)?;
        let thread = self.vm.create_thread(&function)?;
        self.vm.sandbox_thread(&thread)?;
        Ok(thread)
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

#[cfg(test)]
mod tests {
    use super::{
        Dependency,
        LoadedManifest,
        LoadedPlugin,
        PluginDiscoveryError,
        PluginHost,
        PluginLoadError,
        PluginManager,
        PluginManifest,
        RuntimeBuilder,
        execution_order,
        plugin_origin,
        validate_plugin_id,
    };
    use crate::{
        CallContext,
        ChunkOrigin,
        FunctionSpec,
        GatedModule,
        GlobalSpec,
        MemorySourceLoader,
        ModuleExport,
        ModuleSpec,
    };
    use crate::{
        scheduler::tests::ending_without_a_missed_wake,
        testing::ManualClock,
    };
    use harmony_luau as luau;
    use semver::{
        Version,
        VersionReq,
    };
    use std::collections::{
        BTreeMap,
        HashSet,
    };
    use std::path::PathBuf;
    use std::sync::{
        Arc,
        Mutex,
    };

    #[derive(Clone)]
    struct TestStore(&'static str);

    fn manifest(id: &str, scopes: &[&str]) -> PluginManifest {
        PluginManifest {
            schema_version: super::PLUGIN_SCHEMA_VERSION,
            id: id.to_string(),
            name: id.to_string(),
            version: Version::new(1, 0, 0),
            host_requirement: VersionReq::STAR,
            description: String::new(),
            entrypoint: Some("init.luau".to_string()),
            scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
            scope_reasons: BTreeMap::new(),
            scope_grants: BTreeMap::new(),
            dependencies: BTreeMap::new(),
        }
    }

    fn dependency(requirement: &str) -> Dependency {
        Dependency {
            version: VersionReq::parse(requirement).unwrap(),
            optional: false,
        }
    }

    fn optional(requirement: &str) -> Dependency {
        Dependency {
            optional: true,
            ..dependency(requirement)
        }
    }

    fn dependencies(entries: &[(&str, Dependency)]) -> BTreeMap<String, Dependency> {
        entries
            .iter()
            .map(|(id, dependency)| (id.to_string(), dependency.clone()))
            .collect()
    }

    fn manifest_arc(manifests: Vec<PluginManifest>) -> Arc<[PluginManifest]> {
        Arc::from(manifests)
    }

    /// `manifest` as discovery would load it from `plugins/<id>`.
    fn loaded(manifest: PluginManifest) -> LoadedPlugin {
        LoadedPlugin {
            directory: PathBuf::from(format!("plugins/{}", manifest.id)),
            manifest,
            declared_scopes: HashSet::new(),
        }
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

    fn parse_dependencies(json: &str) -> serde_json::Result<BTreeMap<String, Dependency>> {
        super::deserialize_dependencies(&mut serde_json::Deserializer::from_str(json))
    }

    #[test]
    fn parses_dependency_short_and_long_forms() {
        let json = r#"{"a": "0.2", "b": {"version": "^1.4"}, "c": {"version": "=0.0.3", "optional": true}}"#;
        assert_eq!(
            parse_dependencies(json).unwrap(),
            dependencies(&[
                ("a", dependency("0.2")),
                ("b", dependency("^1.4")),
                ("c", optional("=0.0.3")),
            ])
        );
    }

    #[test]
    fn rejects_malformed_dependencies() {
        for (json, expected) in [
            (
                r#"{"a": "nope"}"#,
                "invalid dependency 'a' version requirement \"nope\"",
            ),
            (
                r#"{"a": "1", "b": {"version": "nope"}}"#,
                "invalid dependency 'b' version requirement \"nope\"",
            ),
            (
                r#"{"a": "1", "b": "1", "a": {"version": "2"}}"#,
                "dependency 'a' is listed more than once",
            ),
            (r#"{"a": {"optional": true}}"#, "missing field `version`"),
            (
                r#"{"a": {"version": "1", "required": false}}"#,
                "unknown field `required`",
            ),
            (r#"{"a": ["b"]}"#, "a version requirement or an object"),
            (r#"["a"]"#, "an object mapping plugin ids to dependencies"),
        ] {
            let error = parse_dependencies(json).unwrap_err();
            assert!(error.to_string().contains(expected), "{json}: {error}");
        }
    }

    fn loaded_plugin(
        id: &str,
        version: &str,
        dependencies: BTreeMap<String, Dependency>,
    ) -> LoadedPlugin {
        LoadedPlugin {
            manifest: PluginManifest {
                version: Version::parse(version).unwrap(),
                dependencies,
                ..manifest(id, &[])
            },
            directory: PathBuf::from(format!("plugins/{id}")),
            declared_scopes: HashSet::new(),
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

    fn order(mgr: &PluginManager) -> Vec<String> {
        execution_order_ids(mgr.list_plugins().cloned().collect())
    }

    fn execution_order_ids(plugins: Vec<LoadedPlugin>) -> Vec<String> {
        execution_order(plugins)
            .expect("plugins have an execution order")
            .into_iter()
            .map(|plugin| plugin.manifest.id)
            .collect()
    }

    #[test]
    fn execution_order_orders_after_every_installed_dependency() {
        let order = execution_order_ids(vec![
            loaded_plugin(
                "consumer",
                "1.0.0",
                dependencies(&[("a", dependency("1")), ("b", optional("1"))]),
            ),
            loaded_plugin("b", "1.0.0", BTreeMap::new()),
            loaded_plugin("a", "1.0.0", BTreeMap::new()),
        ]);
        assert_eq!(order, vec!["a", "b", "consumer"]);
    }

    #[test]
    fn execution_order_is_stable_via_id_sort() {
        let order = execution_order_ids(vec![
            loaded_plugin("zeta", "1.0.0", BTreeMap::new()),
            loaded_plugin("alpha", "1.0.0", BTreeMap::new()),
            loaded_plugin("mu", "1.0.0", BTreeMap::new()),
        ]);
        assert_eq!(order, vec!["alpha", "mu", "zeta"]);
    }

    #[test]
    fn execution_order_rejects_cycles() {
        let error = execution_order(vec![
            loaded_plugin("a", "1.0.0", dependencies(&[("b", dependency("1"))])),
            loaded_plugin("b", "1.0.0", dependencies(&[("a", dependency("1"))])),
        ])
        .expect_err("a cycle has no execution order");
        assert!(error.to_string().contains("cycle"), "{error}");
    }

    #[test]
    fn resolve_prunes_plugins_with_missing_required_dep() {
        let mut mgr = manager_with(vec![loaded_plugin(
            "consumer",
            "1.0.0",
            dependencies(&[("missing", dependency("1"))]),
        )]);
        let errors = mgr.resolve_dependencies();
        assert_eq!(errors.len(), 1);
        assert_eq!(
            errors[0].to_string(),
            "plugin 'consumer' requires plugin 'missing' ^1, but it is not installed or failed \
             to load"
        );
        assert!(mgr.get_plugin("consumer").is_none());
    }

    #[test]
    fn resolve_keeps_plugins_with_missing_optional_dep() {
        let mut mgr = manager_with(vec![loaded_plugin(
            "consumer",
            "1.0.0",
            dependencies(&[("missing", optional("1"))]),
        )]);
        let errors = mgr.resolve_dependencies();
        assert!(errors.is_empty());
        assert!(mgr.get_plugin("consumer").is_some());
    }

    #[test]
    fn resolve_checks_installed_dependency_versions() {
        let mut mgr = manager_with(vec![
            loaded_plugin("lib", "0.2.5", BTreeMap::new()),
            loaded_plugin("extra", "0.0.4", BTreeMap::new()),
            loaded_plugin(
                "matching",
                "1.0.0",
                dependencies(&[("lib", dependency("0.2")), ("extra", optional("0.0.4"))]),
            ),
            loaded_plugin(
                "too_old",
                "1.0.0",
                dependencies(&[("lib", dependency("0.3"))]),
            ),
            loaded_plugin(
                "optional_mismatch",
                "1.0.0",
                dependencies(&[("extra", optional("0.0.3"))]),
            ),
        ]);
        let errors = mgr.resolve_dependencies();
        let messages: Vec<String> = errors.iter().map(ToString::to_string).collect();
        assert_eq!(
            messages,
            vec![
                "plugin 'optional_mismatch' requires plugin 'extra' ^0.0.3, but 0.0.4 is installed",
                "plugin 'too_old' requires plugin 'lib' ^0.3, but 0.2.5 is installed",
            ]
        );
        assert!(mgr.get_plugin("matching").is_some());
        assert!(mgr.get_plugin("too_old").is_none());
        assert!(mgr.get_plugin("optional_mismatch").is_none());
    }

    #[test]
    fn resolve_cascades_pruning_through_chain() {
        // c → b → a with a missing: b is dropped, and c only when it
        // requires b.
        let mut mgr = manager_with(vec![
            loaded_plugin("c", "1.0.0", dependencies(&[("b", optional("1"))])),
            loaded_plugin("b", "1.0.0", dependencies(&[("a", dependency("1"))])),
        ]);
        let errors = mgr.resolve_dependencies();
        assert_eq!(errors.len(), 1);
        assert!(mgr.get_plugin("b").is_none());
        assert!(mgr.get_plugin("c").is_some());

        let mut mgr = manager_with(vec![
            loaded_plugin("c", "1.0.0", dependencies(&[("b", dependency("1"))])),
            loaded_plugin("b", "1.0.0", dependencies(&[("a", dependency("1"))])),
        ]);
        let errors = mgr.resolve_dependencies();
        let directories: Vec<_> = errors.iter().map(|error| &error.directories).collect();
        assert_eq!(directories, [&["b"], &["c"]]);
        assert!(mgr.get_plugin("b").is_none());
        assert!(mgr.get_plugin("c").is_none());
    }

    #[test]
    fn resolve_prunes_cycles_and_rechecks_what_depends_on_them() {
        let mut mgr = manager_with(vec![
            loaded_plugin("a", "1.0.0", dependencies(&[("b", dependency("1"))])),
            loaded_plugin("b", "1.0.0", dependencies(&[("a", optional("1"))])),
            loaded_plugin("optional", "1.0.0", dependencies(&[("a", optional("1"))])),
            loaded_plugin("required", "1.0.0", dependencies(&[("a", dependency("1"))])),
        ]);
        let errors = mgr.resolve_dependencies();
        let messages: Vec<String> = errors.iter().map(ToString::to_string).collect();
        assert_eq!(
            messages,
            vec![
                r#"dependency cycle detected; the following plugins are in a cycle and were skipped: ["a", "b"]"#,
                "plugin 'required' requires plugin 'a' ^1, but it is not installed or failed to \
                 load",
            ]
        );
        assert_eq!(order(&mgr), vec!["optional"]);
    }

    #[test]
    fn runtime_builder_installs_require_and_loads_memory_sources() -> anyhow::Result<()> {
        let mut loader = MemorySourceLoader::new();
        loader.insert("plugin:demo/util", b"return { answer = 42 }".as_slice());
        let mut demo = loaded_plugin("demo", "1.0.0", BTreeMap::new());
        demo.manifest.entrypoint = None;
        let runtime = RuntimeBuilder::new(loader, manifest_arc(vec![demo.manifest.clone()]))
            .plugins(Arc::from(vec![demo]))
            .build()?;
        assert!(runtime.exec_all().is_empty());

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
    fn eval_passes_args_as_chunk_varargs() -> anyhow::Result<()> {
        let runtime =
            RuntimeBuilder::new(MemorySourceLoader::new(), manifest_arc(Vec::new())).build()?;

        let values = runtime.eval_source_with_args(
            Arc::<[u8]>::from(&b"local first, second = ...\nreturn first + second"[..]),
            CallContext::default(),
            vec![luau::Value::Number(40.0), luau::Value::Number(2.0)],
        )?;

        assert_eq!(values, vec![luau::Value::Number(42.0)]);
        Ok(())
    }

    fn temp_plugin_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "harmony-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("create temp plugin dir");
        dir
    }

    fn write_demo_plugin(plugin_dir: &std::path::Path, answer: &str) {
        std::fs::write(
            plugin_dir.join("lib.luau"),
            format!("return {{ answer = {answer} }}"),
        )
        .expect("write lib");
        std::fs::write(
            plugin_dir.join("init.luau"),
            "demo_answer = require('./lib').answer",
        )
        .expect("write init");
    }

    #[test]
    fn restart_plugin_reloads_sources_from_disk_and_verify_rejects_manifest_changes()
    -> anyhow::Result<()> {
        let plugins_dir = temp_plugin_dir("invalidate");
        let plugin_dir = plugins_dir.join("demo");
        std::fs::create_dir_all(&plugin_dir)?;
        std::fs::write(
            plugin_dir.join(super::PLUGIN_CONFIG_FILENAME),
            r#"{"schema_version":1,"id":"demo","name":"demo","version":"1.0.0","host":"*","description":"","entrypoint":"init.luau","scopes":[]}"#,
        )?;
        write_demo_plugin(&plugin_dir, "1");

        let manifest = manifest("demo", &[]);
        let plugin = LoadedPlugin {
            manifest: manifest.clone(),
            directory: plugin_dir.clone(),
            declared_scopes: HashSet::new(),
        };
        let runtime = RuntimeBuilder::new(
            crate::FilesystemSourceLoader::new(&plugins_dir, &plugins_dir),
            manifest_arc(vec![manifest]),
        )
        .plugins(Arc::from(vec![plugin]))
        .build()?;
        let read_answer =
            |runtime: &super::Runtime| eval_as(runtime, "demo", b"return demo_answer");

        runtime.exec_plugin("demo")?;
        assert_eq!(read_answer(&runtime)?, vec![luau::Value::Number(1.0)]);

        write_demo_plugin(&plugin_dir, "2");
        runtime.exec_plugin("demo")?;
        assert_eq!(
            read_answer(&runtime)?,
            vec![luau::Value::Number(1.0)],
            "exec without invalidation serves the cached entrypoint"
        );

        runtime.verify_plugin_manifest("demo", &host())?;
        runtime.restart_plugin("demo")?;
        assert_eq!(read_answer(&runtime)?, vec![luau::Value::Number(2.0)]);

        std::fs::write(
            plugin_dir.join(super::PLUGIN_CONFIG_FILENAME),
            r#"{"schema_version":1,"id":"demo","name":"demo","version":"1.1.0","host":"*","description":"","entrypoint":"init.luau","scopes":[]}"#,
        )?;
        let error = runtime
            .verify_plugin_manifest("demo", &host())
            .expect_err("changed manifest must be rejected");
        assert!(error.to_string().contains("reload plugins"), "{error}");

        std::fs::write(
            plugin_dir.join(super::PLUGIN_CONFIG_FILENAME),
            "0".repeat(super::PLUGIN_CONFIG_MAX_BYTES as usize + 1),
        )?;
        let error = runtime
            .verify_plugin_manifest("demo", &host())
            .expect_err("oversized manifest must be rejected");
        assert!(format!("{error:#}").contains("exceeds cap"), "{error:#}");

        let _ = std::fs::remove_dir_all(plugins_dir);
        Ok(())
    }

    #[derive(Default)]
    struct TestHandlers(std::cell::RefCell<Vec<(luau::Function, CallContext)>>);

    /// `later(seconds, fn)` runs `fn` after a delay in the caller's task
    /// group, `register(fn)` keeps `fn` with the caller's group for the test
    /// to call, `run()` numbers each call and `record(value)` logs a value.
    fn task_test_module(recorded: Arc<Mutex<Vec<String>>>) -> ModuleSpec {
        let runs = Arc::new(Mutex::new(0usize));
        let eventually_recorded = recorded.clone();
        let read_function = |frame: &mut luau::CallFrame<'_>| match frame.args.read()? {
            luau::Value::Function(function) => Ok(function),
            other => Err(luau::Error::Runtime(format!(
                "expected a function, got {}",
                other.type_name()
            ))),
        };
        let group_context = |frame: &luau::CallFrame<'_>| CallContext {
            task_group: frame.context.task_group,
            ..CallContext::default()
        };
        ModuleSpec::new("test/tasks")
            .function(FunctionSpec::new("wake").call(|frame| {
                frame
                    .vm
                    .data()
                    .get::<crate::LocalScheduler>()?
                    .wake_handle()
                    .wake();
                Ok(())
            }))
            .function(FunctionSpec::new("later").call(move |mut frame| {
                let seconds: f64 = frame.args.read()?;
                let function = read_function(&mut frame)?;
                let thread = frame.vm.create_thread(&function)?;
                frame
                    .vm
                    .data()
                    .get::<crate::LocalScheduler>()?
                    .schedule_luau_thread_after(
                        group_context(&frame),
                        std::time::Duration::from_secs_f64(seconds),
                        frame.vm.clone(),
                        thread,
                        Vec::new(),
                    );
                Ok(())
            }))
            .function(FunctionSpec::new("register").call(move |mut frame| {
                let function = read_function(&mut frame)?;
                let context = group_context(&frame);
                frame
                    .vm
                    .data()
                    .get::<TestHandlers>()?
                    .0
                    .borrow_mut()
                    .push((function, context));
                Ok(())
            }))
            .function(FunctionSpec::new("run").call(move |mut frame| {
                let mut runs = runs.lock().expect("runs mutex poisoned");
                *runs += 1;
                frame.returns.write(*runs as i64)?;
                Ok(())
            }))
            .function(FunctionSpec::new("record").call(move |mut frame| {
                let value: String = frame.args.read()?;
                recorded
                    .lock()
                    .expect("recorded mutex poisoned")
                    .push(value);
                Ok(())
            }))
            .function(FunctionSpec::new("check").call(|mut frame| {
                let checked = crate::ensure_current_plugin_run(frame.vm, &frame.context);
                frame
                    .returns
                    .write(checked.err().map(|error| error.to_string()))?;
                Ok(())
            }))
            .function(FunctionSpec::new("forever").call_async(Arc::new(|_frame| {
                Ok(luau::ScheduledFuture::new(std::future::pending::<
                    luau::runtime::Result<()>,
                >()))
            })))
            .function(FunctionSpec::new("soon").call_async(Arc::new(|_frame| {
                Ok(luau::ScheduledFuture::after(
                    std::time::Duration::from_millis(10),
                    async { Ok(()) },
                ))
            })))
            .function(
                FunctionSpec::new("eventually").call_async(Arc::new(move |mut frame| {
                    let value: String = frame.args.read()?;
                    let recorded = eventually_recorded.clone();
                    Ok(luau::ScheduledFuture::after(
                        std::time::Duration::from_millis(10),
                        async move {
                            recorded
                                .lock()
                                .expect("recorded mutex poisoned")
                                .push(value);
                            Ok(())
                        },
                    ))
                })),
            )
            .function(FunctionSpec::new("thread_plugin").call(|mut frame| {
                let plugin = frame
                    .vm
                    .thread_call_context(&frame.thread)?
                    .and_then(|context| context.origin.plugin)
                    .map(|plugin| plugin.to_string());
                frame.returns.write(plugin)?;
                Ok(())
            }))
            .install(|_| Ok(ModuleExport::new(())))
    }

    /// A capability only `lib` holds in the task test runtimes.
    fn secret_module() -> ModuleSpec {
        ModuleSpec::new("test/secret")
            .function(FunctionSpec::new("get").call(|mut frame| {
                frame.returns.write("secret")?;
                Ok(())
            }))
            .install(|_| Ok(ModuleExport::new(())))
    }

    /// How long `later` in the tests' sources and the module's `soon` and `eventually` wait.
    const LATER: std::time::Duration = std::time::Duration::from_millis(50);
    const SOON: std::time::Duration = std::time::Duration::from_millis(10);

    /// The entrypoint of a plugin that numbers its run, records it later and registers a handler
    /// that never ends.
    fn numbered_run_source(plugin: &str) -> String {
        r#"
            local tasks = require('@test/tasks')
            local run = tasks.run()
            tasks.later(0.05, function()
                tasks.record(`{PLUGIN}:{run}`)
            end)
            tasks.register(function()
                tasks.forever()
            end)
        "#
        .replace("{PLUGIN}", plugin)
    }

    /// Plugins as `(id, dependencies, entrypoint source)` and extra plugin
    /// modules as `(source key, source)`.
    fn task_test_runtime_with(
        recorded: Arc<Mutex<Vec<String>>>,
        sources: &[(&str, &[&str], &str)],
        modules: &[(&str, &str)],
    ) -> anyhow::Result<super::Runtime> {
        let (loader, plugins) = task_test_plugins(sources, modules);
        task_test_runtime_from(recorded, loader, plugins)
    }

    fn task_test_plugins(
        sources: &[(&str, &[&str], &str)],
        modules: &[(&str, &str)],
    ) -> (MemorySourceLoader, Vec<LoadedPlugin>) {
        let mut loader = MemorySourceLoader::new();
        for (key, source) in modules {
            loader.insert(key.to_string(), source.as_bytes());
        }
        let mut plugins = Vec::new();
        for (id, dependencies, source) in sources {
            loader.insert(format!("plugin:{id}/init"), source.as_bytes());
            let scopes: &[&str] = if *id == "lib" {
                &["test.tasks", "test.secret"]
            } else {
                &["test.tasks"]
            };
            let mut plugin_manifest = manifest(id, scopes);
            plugin_manifest.dependencies = dependencies
                .iter()
                .map(|dependency| (dependency.to_string(), self::dependency("1")))
                .collect();
            plugins.push(loaded(plugin_manifest));
        }
        (loader, plugins)
    }

    fn task_test_runtime_from(
        recorded: Arc<Mutex<Vec<String>>>,
        loader: MemorySourceLoader,
        plugins: Vec<LoadedPlugin>,
    ) -> anyhow::Result<super::Runtime> {
        task_test_builder(recorded, loader, plugins).build()
    }

    fn task_test_builder(
        recorded: Arc<Mutex<Vec<String>>>,
        loader: MemorySourceLoader,
        plugins: Vec<LoadedPlugin>,
    ) -> RuntimeBuilder<MemorySourceLoader> {
        RuntimeBuilder::new(
            loader,
            manifest_arc(
                plugins
                    .iter()
                    .map(|plugin| plugin.manifest.clone())
                    .collect(),
            ),
        )
        .plugins(Arc::from(plugins))
        .modules(vec![
            GatedModule::new("test.tasks", task_test_module(recorded)),
            GatedModule::new("test.secret", secret_module()),
        ])
        .configure_vm(|vm| {
            vm.data().insert(TestHandlers::default())?;
            Ok(())
        })
    }

    /// A task test runtime on a manual clock: the delays its plugins wait out pass when the test
    /// advances it, and nothing here waits for them. Its methods stand in for the runtime's own
    /// that would.
    struct StagedRuntime {
        runtime: super::Runtime,
        clock: ManualClock,
        recorded: Arc<Mutex<Vec<String>>>,
    }

    impl std::ops::Deref for StagedRuntime {
        type Target = super::Runtime;

        fn deref(&self) -> &super::Runtime {
            &self.runtime
        }
    }

    impl StagedRuntime {
        fn new(
            sources: &[(&str, &[&str], &str)],
            modules: &[(&str, &str)],
        ) -> anyhow::Result<Self> {
            let clock = ManualClock::new();
            let recorded = Arc::<Mutex<Vec<String>>>::default();
            let (loader, plugins) = task_test_plugins(sources, modules);
            let runtime = task_test_builder(recorded.clone(), loader, plugins)
                .manual_clock(clock.clone())
                .build()?;
            Ok(Self {
                runtime,
                clock,
                recorded,
            })
        }

        /// Two plugins, `demo` and `other`, of [`numbered_run_source`].
        fn numbered_runs() -> anyhow::Result<Self> {
            Self::new(
                &[
                    ("demo", &[], &numbered_run_source("demo")),
                    ("other", &[], &numbered_run_source("other")),
                ],
                &[],
            )
        }

        fn scheduler(&self) -> std::rc::Rc<crate::LocalScheduler> {
            self.runtime.scheduler().expect("the runtime's scheduler")
        }

        /// Runs `entrypoint` to its end, which it must reach without waiting.
        fn drive(&self, entrypoint: Option<super::RunningEntrypoint>) -> anyhow::Result<()> {
            let Some(entrypoint) = entrypoint else {
                return Ok(());
            };
            let scheduler = self.scheduler();
            let task = scheduler.luau_thread_handle(&entrypoint.thread);
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            let mut finish = std::pin::pin!(entrypoint.finish());
            let result = loop {
                {
                    let _guard = self.runtime.tokio_runtime.enter();
                    scheduler.poll_ready();
                }
                if let std::task::Poll::Ready(result) = finish.as_mut().poll(&mut cx) {
                    break result;
                }
                anyhow::ensure!(
                    scheduler.next_wake_delay() == Some(std::time::Duration::ZERO),
                    "the entrypoint waits"
                );
            };
            if let Some(task) = task {
                scheduler.remove(task.id());
            }
            result
        }

        /// As [`super::Runtime::exec_all`].
        fn exec_all(&self) -> Vec<String> {
            self.runtime
                .plugins
                .iter()
                .filter(|plugin| {
                    self.runtime
                        .start_entrypoint(plugin)
                        .and_then(|entrypoint| self.drive(entrypoint))
                        .is_err()
                })
                .map(|plugin| plugin.manifest.id.clone())
                .collect()
        }

        /// As [`super::Runtime::restart_plugin`].
        fn restart_plugin(&self, plugin_id: &str) -> anyhow::Result<()> {
            self.drive(self.runtime.start_restart_entrypoint(plugin_id)?)
        }

        /// As [`eval_as`], for a chunk that ends without waiting.
        fn eval_as(&self, plugin: &str, source: &'static [u8]) -> anyhow::Result<Vec<luau::Value>> {
            let mut context = CallContext {
                origin: plugin_origin(plugin, "check.luau"),
                ..CallContext::default()
            };
            let thread = self
                .runtime
                .load_source_thread(Arc::<[u8]>::from(source), &mut context)?;
            let scheduler = self.scheduler();
            scheduler.spawn_luau_thread(context, self.vm.clone(), thread.clone(), Vec::new());
            crate::testing::drive_thread_while_ready(
                &self.runtime.tokio_runtime,
                &scheduler,
                &thread,
            )
        }

        /// Runs rounds until no task is ready.
        fn run_ready(&self) {
            let scheduler = self.scheduler();
            loop {
                self.runtime.poll_background_tasks();
                if scheduler.next_wake_delay() != Some(std::time::Duration::ZERO) {
                    return;
                }
            }
        }

        /// Lets `duration` pass, then runs what came due and what that made ready.
        fn advance(&self, duration: std::time::Duration) {
            self.clock.advance(duration);
            self.run_ready();
        }

        /// How long until the next delayed task is due.
        fn next_delay(&self) -> Option<std::time::Duration> {
            self.scheduler().next_wake_delay()
        }

        fn recorded(&self) -> Vec<String> {
            self.recorded
                .lock()
                .expect("recorded mutex poisoned")
                .clone()
        }
    }

    /// An entrypoint exporting these `@test/tasks` functions.
    fn lib_exporting(functions: &[&str]) -> String {
        let exports = functions
            .iter()
            .map(|function| format!("{function} = tasks.{function}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("local tasks = require('@test/tasks')\nreturn {{ {exports} }}")
    }

    fn leftover_tasks(runtime: &super::Runtime) -> anyhow::Result<Vec<crate::TaskSnapshot>> {
        let scheduler = runtime.vm.data().get::<crate::LocalScheduler>()?;
        scheduler.remove_finished();
        Ok(scheduler.snapshots())
    }

    /// Evaluates `source` as plugin code of `plugin`.
    fn eval_as(
        runtime: &super::Runtime,
        plugin: &str,
        source: &'static [u8],
    ) -> anyhow::Result<Vec<luau::Value>> {
        runtime.eval_source_with_context(
            Arc::<[u8]>::from(source),
            CallContext {
                origin: plugin_origin(plugin, "check.luau"),
                ..CallContext::default()
            },
        )
    }

    fn call_handler(
        runtime: &super::Runtime,
        index: usize,
    ) -> anyhow::Result<std::rc::Rc<crate::LocalLuauTaskCompletion>> {
        let (function, context) =
            runtime.vm.data().get::<TestHandlers>()?.0.borrow()[index].clone();
        let completion = std::rc::Rc::new(crate::LocalLuauTaskCompletion::default());
        let scheduler = runtime.vm.data().get::<crate::LocalScheduler>()?;
        scheduler.schedule_luau_thread_with_limit_and_completion(
            context,
            runtime.vm.clone(),
            runtime.vm.create_thread(&function)?,
            Vec::new(),
            crate::ResumeLimit::budget(std::time::Duration::from_secs(5)),
            completion.clone(),
        );
        Ok(completion)
    }

    fn completion_result(
        completion: &crate::LocalLuauTaskCompletion,
    ) -> Option<Result<Vec<luau::Value>, String>> {
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        match completion.poll(&mut context) {
            std::task::Poll::Ready(result) => Some(result.map_err(|error| error.message)),
            std::task::Poll::Pending => None,
        }
    }

    fn recording_entrypoint(recorded: Arc<Mutex<Vec<String>>>) -> anyhow::Result<super::Runtime> {
        task_test_runtime_with(
            recorded,
            &[("demo", &[], "require('@test/tasks').record('ran')")],
            &[],
        )
    }

    #[test]
    fn a_host_call_takes_the_wake_handle_while_the_scheduler_polls() -> anyhow::Result<()> {
        let runtime = task_test_runtime_with(
            Arc::default(),
            &[("demo", &[], "require('@test/tasks').wake()")],
            &[],
        )?;
        assert!(runtime.exec_all().is_empty());
        Ok(())
    }

    #[test]
    fn a_cancelled_entrypoint_is_not_resumed_by_the_driver() -> anyhow::Result<()> {
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let runtime = recording_entrypoint(recorded.clone())?;
        assert!(runtime.exec_all().is_empty());
        recorded.lock().expect("recorded").clear();

        let entrypoint = runtime
            .start_restart_entrypoint("demo")?
            .ok_or_else(|| anyhow::anyhow!("the plugin has an entrypoint"))?;
        runtime.entrypoint_cancellation().cancel();
        let error = runtime
            .drive_entrypoint(entrypoint)
            .expect_err("a cancelled entrypoint fails");

        assert!(format!("{error:#}").contains("cancelled"), "{error:#}");
        assert!(recorded.lock().expect("recorded").is_empty());
        Ok(())
    }

    #[test]
    fn cancellation_wins_over_a_completion_that_already_landed() -> anyhow::Result<()> {
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let runtime = recording_entrypoint(recorded.clone())?;
        assert!(runtime.exec_all().is_empty());
        recorded.lock().expect("recorded").clear();

        let entrypoint = runtime
            .start_restart_entrypoint("demo")?
            .ok_or_else(|| anyhow::anyhow!("the plugin has an entrypoint"))?;
        // The entrypoint is ready, so one round lands its completion.
        runtime.poll_background_tasks();
        assert_eq!(*recorded.lock().expect("recorded"), ["ran"]);
        runtime.entrypoint_cancellation().cancel();

        let error = runtime
            .drive_entrypoint(entrypoint)
            .expect_err("cancellation takes precedence");
        assert!(format!("{error:#}").contains("cancelled"), "{error:#}");
        Ok(())
    }

    /// On the real clock and the real wait, as the runtime's owner runs a started restart: as a
    /// task, between rounds that sleep until something wakes the scheduler.
    #[test]
    fn a_started_restart_runs_as_a_task_and_ends_like_a_restart() {
        ending_without_a_missed_wake(|| {
            let recorded = Arc::new(Mutex::new(Vec::new()));
            let runtime = recording_entrypoint(recorded.clone()).expect("runtime");
            assert!(runtime.exec_all().is_empty());
            recorded.lock().expect("recorded").clear();

            let restart = runtime
                .start_plugin_restart("demo")
                .expect("the restart starts")
                .expect("the plugin has an entrypoint");
            let scheduler = runtime.scheduler().expect("scheduler");
            let outcome = std::rc::Rc::new(std::cell::RefCell::new(None));
            scheduler.schedule_watcher(CallContext::default(), {
                let outcome = outcome.clone();
                async move {
                    *outcome.borrow_mut() = Some(restart.await);
                    Ok(())
                }
            });
            let outcome = loop {
                runtime.poll_background_tasks();
                if let Some(outcome) = outcome.borrow_mut().take() {
                    break outcome;
                }
                scheduler.wait_for_work(None);
            };
            outcome.expect("the restart succeeds");
            assert_eq!(*recorded.lock().expect("recorded"), ["ran"]);
        });
    }

    #[test]
    fn a_restart_whose_entrypoint_fails_ends_the_run_from_its_task() -> anyhow::Result<()> {
        let runtime = StagedRuntime::new(
            &[(
                "demo",
                &[],
                r#"
                    local tasks = require('@test/tasks')
                    local run = tasks.run()
                    tasks.later(0.05, function() tasks.record('late:' .. run) end)
                    if run > 1 then error('boom') end
                "#,
            )],
            &[],
        )?;
        assert!(runtime.exec_all().is_empty());
        assert_eq!(runtime.next_delay(), Some(LATER), "the first run waits");

        let restart = runtime
            .start_plugin_restart("demo")?
            .ok_or_else(|| anyhow::anyhow!("the plugin has an entrypoint"))?;
        let outcome = std::rc::Rc::new(std::cell::RefCell::new(None));
        runtime
            .scheduler()
            .schedule_watcher(CallContext::default(), {
                let outcome = outcome.clone();
                async move {
                    *outcome.borrow_mut() = Some(restart.await);
                    Ok(())
                }
            });
        runtime.run_ready();
        let error = outcome
            .borrow_mut()
            .take()
            .expect("the restart ended")
            .expect_err("the entrypoint fails");
        assert!(format!("{error:#}").contains("boom"), "{error:#}");

        assert!(
            leftover_tasks(&runtime)?.is_empty(),
            "a run's task is left waiting"
        );
        runtime.advance(LATER);
        assert!(
            runtime.recorded().is_empty(),
            "neither run's task outlives it"
        );
        Ok(())
    }

    #[test]
    fn run_end_hooks_run_whenever_a_run_ends() -> anyhow::Result<()> {
        let mut loader = MemorySourceLoader::new();
        loader.insert("plugin:demo/init".to_string(), &b"return nil"[..]);
        loader.insert("plugin:broken/init".to_string(), &b"error('boom')"[..]);
        let manifests = vec![manifest("demo", &[]), manifest("broken", &[])];
        let plugins = manifests.iter().cloned().map(loaded).collect::<Vec<_>>();
        let ended = Arc::new(Mutex::new(Vec::new()));
        let hook_ended = ended.clone();
        let runtime = RuntimeBuilder::new(loader, manifest_arc(manifests))
            .plugins(Arc::from(plugins))
            .on_run_end(move |_vm, plugin_id| {
                hook_ended.lock().unwrap().push(plugin_id.to_string());
            })
            .build()?;
        let ended = || ended.lock().unwrap().clone();

        assert_eq!(runtime.exec_all(), ["broken"]);
        assert_eq!(ended(), ["broken"], "a failed entrypoint ends its run");

        runtime.restart_plugin("demo")?;
        assert_eq!(ended(), ["broken", "demo"]);

        runtime.stop_plugin("demo")?;
        runtime.stop_plugin("demo")?;
        assert_eq!(
            ended(),
            ["broken", "demo", "demo"],
            "stopping a stopped plugin ends no run"
        );
        Ok(())
    }

    #[test]
    fn restart_plugin_cancels_tasks_of_the_previous_run_only() -> anyhow::Result<()> {
        let runtime = StagedRuntime::numbered_runs()?;
        assert!(runtime.exec_all().is_empty());
        assert_eq!(leftover_tasks(&runtime)?.len(), 2, "each run waits");

        runtime.restart_plugin("demo")?;
        assert_eq!(
            leftover_tasks(&runtime)?.len(),
            2,
            "the restarted plugin's first run still waits"
        );
        assert!(runtime.recorded().is_empty());

        runtime.advance(LATER);
        let mut recorded = runtime.recorded();
        recorded.sort();
        assert_eq!(recorded, ["demo:3", "other:2"]);
        assert!(leftover_tasks(&runtime)?.is_empty());
        Ok(())
    }

    #[test]
    fn restart_plugin_cancels_tasks_spawned_inside_coroutines() -> anyhow::Result<()> {
        let runtime = StagedRuntime::new(
            &[(
                "demo",
                &[],
                r#"
                    local tasks = require('@test/tasks')
                    local run = tasks.run()
                    coroutine.wrap(function()
                        tasks.later(0.05, function()
                            tasks.record(`coroutine:{run}`)
                        end)
                    end)()
                "#,
            )],
            &[],
        )?;
        assert!(runtime.exec_all().is_empty());
        assert_eq!(leftover_tasks(&runtime)?.len(), 1, "the first run waits");

        runtime.restart_plugin("demo")?;
        assert_eq!(leftover_tasks(&runtime)?.len(), 1);

        runtime.advance(LATER);
        assert_eq!(runtime.recorded(), ["coroutine:2"]);
        Ok(())
    }

    #[test]
    fn exec_all_runs_dependencies_before_their_dependents() -> anyhow::Result<()> {
        let runtime = StagedRuntime::new(
            &[
                (
                    "app",
                    &["lib"],
                    r#"
                        require('@lib')
                        require('@test/tasks').record("app")
                    "#,
                ),
                (
                    "lib",
                    &[],
                    r#"
                        local tasks = require('@test/tasks')
                        tasks.record("lib")
                        tasks.later(0.05, function()
                            tasks.record("lib later")
                        end)
                        return {}
                    "#,
                ),
            ],
            &[],
        )?;
        assert!(runtime.exec_all().is_empty());
        assert_eq!(runtime.recorded(), ["lib", "app"]);
        assert_eq!(runtime.next_delay(), Some(LATER), "lib's task waits");

        runtime.stop_plugin("lib")?;
        assert!(leftover_tasks(&runtime)?.is_empty());

        runtime.advance(LATER);
        assert_eq!(
            runtime.recorded(),
            ["lib", "app"],
            "stopping lib cancels the task its entrypoint spawned"
        );
        Ok(())
    }

    /// On the real clock and the real wait: the entrypoint waits for good, so only the
    /// cancellation's wake ends the wait its driver sleeps in.
    #[test]
    fn cancelled_entrypoints_end_the_running_one_and_skip_the_rest() {
        ending_without_a_missed_wake(|| {
            let recorded = Arc::new(Mutex::new(Vec::new()));
            let runtime = task_test_runtime_with(
                recorded.clone(),
                &[
                    (
                        "a",
                        &[],
                        r#"
                            local tasks = require('@test/tasks')
                            tasks.record("a")
                            tasks.forever()
                        "#,
                    ),
                    ("b", &["a"], "require('@test/tasks').record('b')"),
                ],
                &[],
            )
            .expect("runtime");
            let cancellation = runtime.entrypoint_cancellation();
            let reached = recorded.clone();
            // Cancels once `a` has started, whether or not it has yielded yet.
            let cancel = std::thread::spawn(move || {
                while reached.lock().expect("recorded mutex poisoned").is_empty() {
                    std::thread::yield_now();
                }
                cancellation.cancel();
            });

            assert_eq!(runtime.exec_all(), ["a", "b"]);
            assert_eq!(*recorded.lock().expect("recorded mutex poisoned"), ["a"]);
            cancel.join().expect("cancelling thread");
        });
    }

    /// The entrypoint's driver is asleep, with nothing due, when the cancellation comes.
    #[test]
    fn a_cancellation_wakes_the_driver_of_an_entrypoint_that_waits() {
        ending_without_a_missed_wake(|| {
            let runtime = task_test_runtime_with(
                Arc::default(),
                &[("demo", &[], "require('@test/tasks').forever()")],
                &[],
            )
            .expect("runtime");
            let cancellation = runtime.entrypoint_cancellation();
            let wake = runtime.scheduler().expect("scheduler").wake_handle();
            let cancel = std::thread::spawn(move || {
                while !wake.owner_is_waiting() {
                    std::thread::yield_now();
                }
                cancellation.cancel();
            });

            assert_eq!(runtime.exec_all(), ["demo"]);
            cancel.join().expect("cancelling thread");
        });
    }

    #[test]
    fn a_runtime_on_a_manual_clock_runs_no_code_to_its_end_on_the_calling_thread()
    -> anyhow::Result<()> {
        let runtime = StagedRuntime::new(
            &[("demo", &[], "require('@test/tasks').record('ran')")],
            &[],
        )?;
        let refused = |error: anyhow::Error| {
            assert!(
                format!("{error:#}").contains("manual clock can't block"),
                "{error:#}"
            );
        };

        assert_eq!(runtime.runtime.exec_all(), ["demo"]);
        refused(runtime.exec_plugin("demo").expect_err("refused"));
        refused(runtime.runtime.restart_plugin("demo").expect_err("refused"));
        refused(
            eval_as(&runtime, "demo", b"require('@test/tasks').record('ran')")
                .expect_err("refused"),
        );
        runtime.run_ready();
        assert!(runtime.recorded().is_empty(), "refused code still ran");
        assert!(leftover_tasks(&runtime)?.is_empty());
        Ok(())
    }

    #[test]
    fn a_plugin_cannot_change_the_globals_or_libraries_another_plugin_sees() -> anyhow::Result<()> {
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let runtime = task_test_runtime_with(
            recorded.clone(),
            &[
                (
                    "first",
                    &[],
                    r#"
                        local tasks = require('@test/tasks')
                        written = "first"
                        print = nil
                        tasks.record(tostring(pcall(function() string.upper = string.lower end)))
                        tasks.record(tostring(pcall(function() _G.written = "first" end)))
                        return {}
                    "#,
                ),
                (
                    "second",
                    &["first"],
                    r#"
                        require('@first')
                        local tasks = require('@test/tasks')
                        tasks.record(tostring(written))
                        tasks.record(type(print))
                        tasks.record(string.upper("a"))
                    "#,
                ),
            ],
            &[],
        )?;
        assert!(runtime.exec_all().is_empty());
        assert_eq!(
            *recorded.lock().expect("recorded mutex poisoned"),
            ["false", "false", "nil", "function", "A"]
        );
        Ok(())
    }

    #[test]
    fn a_stopped_plugin_s_cached_sources_are_not_served() -> anyhow::Result<()> {
        let runtime = task_test_runtime_with(
            Arc::new(Mutex::new(Vec::new())),
            &[
                ("lib", &[], "return { answer = 42 }"),
                ("app", &["lib"], ""),
            ],
            &[],
        )?;
        assert!(runtime.exec_all().is_empty());
        assert_eq!(
            eval_as(&runtime, "app", b"return require('@lib').answer")?,
            [luau::Value::Number(42.0)]
        );

        runtime.stop_plugin("lib")?;

        let error = eval_as(&runtime, "app", b"return require('@lib').answer")
            .expect_err("lib's run ended, so its cached exports aren't served");
        assert!(
            format!("{error:#}").contains("plugin 'lib' is not running"),
            "{error:#}"
        );
        Ok(())
    }

    #[test]
    fn a_restarted_library_plugin_can_still_be_required() -> anyhow::Result<()> {
        let mut loader = MemorySourceLoader::new();
        loader.insert("plugin:lib/util", b"return 7".as_slice());
        let mut library = manifest("lib", &[]);
        library.entrypoint = None;
        let mut app = manifest("app", &[]);
        app.dependencies = dependencies(&[("lib", dependency("1"))]);
        let plugins = [library, app].into_iter().map(loaded).collect();
        loader.insert("plugin:app/init", b"".as_slice());
        let runtime = task_test_runtime_from(Arc::new(Mutex::new(Vec::new())), loader, plugins)?;
        assert!(runtime.exec_all().is_empty());

        runtime.restart_plugin("lib")?;

        assert_eq!(
            eval_as(&runtime, "app", b"return require('@lib/util')")?,
            [luau::Value::Number(7.0)]
        );
        Ok(())
    }

    #[test]
    fn tasks_a_live_export_spawns_stop_when_its_plugin_restarts() -> anyhow::Result<()> {
        let runtime = StagedRuntime::new(
            &[
                ("lib", &[], &lib_exporting(&["later"])),
                (
                    "app",
                    &["lib"],
                    r#"
                        local tasks = require('@test/tasks')
                        require('@lib').later(0.05, function()
                            tasks.record("through lib")
                        end)
                        tasks.later(0.05, function()
                            tasks.record("app")
                        end)
                    "#,
                ),
            ],
            &[],
        )?;
        assert!(runtime.exec_all().is_empty());
        assert_eq!(leftover_tasks(&runtime)?.len(), 2, "both tasks wait");

        runtime.restart_plugin("lib")?;
        assert_eq!(leftover_tasks(&runtime)?.len(), 1);

        runtime.advance(LATER);
        assert_eq!(runtime.recorded(), ["app"]);
        Ok(())
    }

    #[test]
    fn ending_either_plugin_cancels_an_async_call_between_them() -> anyhow::Result<()> {
        // Stopping the callee, and restarting the caller while it awaits.
        for (callee, ended) in [("forever", "lib"), ("soon", "app")] {
            let app = format!(
                r#"
                    local tasks = require('@test/tasks')
                    local lib = require('@lib')
                    tasks.register(function()
                        lib.{callee}()
                        tasks.record("resumed")
                    end)
                "#
            );
            let runtime = StagedRuntime::new(
                &[
                    ("lib", &[], &lib_exporting(&[callee])),
                    ("app", &["lib"], &app),
                ],
                &[],
            )?;
            assert!(runtime.exec_all().is_empty());
            let call = call_handler(&runtime, 0)?;
            runtime.run_ready();
            assert!(completion_result(&call).is_none(), "{ended}");
            assert_eq!(
                leftover_tasks(&runtime)?.len(),
                1,
                "{ended}: the call waits"
            );

            if ended == "lib" {
                runtime.stop_plugin("lib")?;
            } else {
                runtime.restart_plugin("app")?;
            }

            let error = completion_result(&call)
                .unwrap_or_else(|| panic!("the call must finish when {ended} ends"))
                .expect_err("the call must fail");
            assert!(error.contains("cancelled"), "{ended}: {error}");
            assert!(leftover_tasks(&runtime)?.is_empty(), "{ended}");
            runtime.advance(SOON);
            assert!(runtime.recorded().is_empty(), "{ended}");
        }
        Ok(())
    }

    #[test]
    fn a_kept_wrapper_cannot_require_for_a_restarted_plugin() -> anyhow::Result<()> {
        let runtime = StagedRuntime::new(
            &[
                (
                    "lib",
                    &[],
                    r#"
                        return {
                            lazy = function()
                                return require('./late')
                            end,
                        }
                    "#,
                ),
                (
                    "app",
                    &["lib"],
                    r#"
                        kept_lazy = require('@lib').lazy
                    "#,
                ),
            ],
            &[(
                "plugin:lib/late",
                r#"
                    local tasks = require('@test/tasks')
                    tasks.later(0.05, function()
                        tasks.record("late")
                    end)
                    return { check = tasks.check() }
                "#,
            )],
        )?;
        assert!(runtime.exec_all().is_empty());

        runtime.restart_plugin("lib")?;
        let error = runtime
            .eval_as("app", b"return kept_lazy()")
            .expect_err("a wrapper from lib's ended run must not load lib's modules");
        assert!(
            error.to_string().contains("run that has ended"),
            "{error:#}"
        );
        assert!(
            leftover_tasks(&runtime)?.is_empty(),
            "the refused wrapper loaded the module"
        );

        assert_eq!(
            runtime.eval_as("app", b"return require('@lib').lazy().check")?,
            vec![luau::Value::Nil],
            "the current run's wrapper loads modules that act for that run"
        );
        assert!(runtime.recorded().is_empty());
        runtime.advance(LATER);
        assert_eq!(runtime.recorded(), ["late"]);
        Ok(())
    }

    #[test]
    fn app_code_run_on_another_plugin_s_thread_has_no_require_of_that_plugin() -> anyhow::Result<()>
    {
        let runtime = StagedRuntime::new(
            &[
                (
                    "lib",
                    &[],
                    r#"
                        local tasks = require('@test/tasks')
                        local kept
                        tasks.register(function()
                            return kept()
                        end)
                        return {
                            keep = function(callback)
                                kept = callback
                            end,
                        }
                    "#,
                ),
                (
                    "app",
                    &["lib"],
                    r#"
                        local tasks = require('@test/tasks')
                        require('@lib').keep(function()
                            tasks.record("getfenv:" .. tostring(getfenv))
                            tasks.record("capability:" .. tostring((pcall(require, "@test/secret"))))
                        end)
                    "#,
                ),
            ],
            &[],
        )?;
        assert!(runtime.exec_all().is_empty());

        let call = call_handler(&runtime, 0)?;
        runtime.run_ready();
        assert_eq!(runtime.recorded(), ["getfenv:nil", "capability:false"]);
        assert!(matches!(completion_result(&call), Some(Ok(_))));
        Ok(())
    }

    #[test]
    fn a_stale_async_export_fails_its_caller_at_once() -> anyhow::Result<()> {
        let runtime = StagedRuntime::new(
            &[
                ("lib", &[], &lib_exporting(&["soon"])),
                (
                    "app",
                    &["lib"],
                    r#"
                        local tasks = require('@test/tasks')
                        local lib = require('@lib')
                        tasks.register(function()
                            lib.soon()
                            tasks.record("resumed")
                        end)
                    "#,
                ),
            ],
            &[],
        )?;
        assert!(runtime.exec_all().is_empty());
        runtime.restart_plugin("lib")?;

        let call = call_handler(&runtime, 0)?;
        // With no time passing, so not once the export's wait would have ended.
        runtime.run_ready();
        let error = completion_result(&call)
            .expect("a call through a stale async export must finish")
            .expect_err("a call through a stale async export must fail");
        assert!(error.contains("run that has ended"), "{error}");
        assert!(leftover_tasks(&runtime)?.is_empty());
        runtime.advance(SOON);
        assert!(runtime.recorded().is_empty());
        Ok(())
    }

    #[test]
    fn async_calls_where_luau_cannot_yield_fail_and_schedule_nothing() -> anyhow::Result<()> {
        // A `table.sort` comparator, and a module's top level during `require`.
        let runtime = StagedRuntime::new(
            &[
                ("lib", &[], &lib_exporting(&["eventually"])),
                (
                    "app",
                    &["lib"],
                    r#"
                        local tasks = require('@test/tasks')
                        local lib = require('@lib')
                        tasks.record("require:" .. tostring((pcall(require, "./helper"))))
                        tasks.register(function()
                            local ok, error = pcall(function()
                                table.sort({ 3, 1, 2 }, function(a, b)
                                    lib.eventually("sorted")
                                    return a < b
                                end)
                            end)
                            tasks.record("sort:" .. tostring(ok))
                            assert(string.find(error, "non-yieldable", 1, true), error)
                            return "done"
                        end)
                    "#,
                ),
            ],
            &[(
                "plugin:app/helper",
                r#"
                    require('@lib').eventually("module")
                    return 1
                "#,
            )],
        )?;
        assert!(runtime.exec_all().is_empty());
        let call = call_handler(&runtime, 0)?;
        runtime.run_ready();

        assert_eq!(
            completion_result(&call).expect("the call finishes"),
            Ok(vec![luau::Value::String(b"done".to_vec())])
        );
        assert!(leftover_tasks(&runtime)?.is_empty());
        assert_eq!(runtime.next_delay(), None, "a refused call still waits");
        runtime.advance(SOON);
        assert_eq!(runtime.recorded(), ["require:false", "sort:false"]);
        Ok(())
    }

    #[test]
    fn a_coroutine_keeps_its_context_across_scheduler_resumes() -> anyhow::Result<()> {
        let runtime = StagedRuntime::new(
            &[
                ("lib", &[], &lib_exporting(&["soon"])),
                (
                    "app",
                    &["lib"],
                    r#"
                        local tasks = require('@test/tasks')
                        local lib = require('@lib')
                        local co = coroutine.create(function()
                            tasks.record("before:" .. tostring(tasks.thread_plugin()))
                            lib.soon()
                            tasks.record("resumed:" .. tostring(tasks.thread_plugin()))
                            tasks.record("getfenv:" .. tostring(getfenv))
                            tasks.record("private:" .. tostring((pcall(require, "./secret"))))
                            tasks.record("capability:" .. tostring((pcall(require, "@test/secret"))))
                            coroutine.yield()
                            tasks.record("manual:" .. tostring(tasks.thread_plugin()))
                            lib.soon()
                            tasks.record("again:" .. tostring(tasks.thread_plugin()))
                        end)
                        coroutine.resume(co)
                        tasks.register(function()
                            tasks.record("handler:" .. coroutine.status(co))
                            coroutine.resume(co)
                        end)
                    "#,
                ),
            ],
            &[("plugin:lib/secret", r#"return "lib secret""#)],
        )?;
        assert!(runtime.exec_all().is_empty());
        assert_eq!(runtime.recorded(), ["before:app"]);
        assert_eq!(runtime.next_delay(), Some(SOON), "the coroutine waits");

        runtime.advance(SOON);
        assert_eq!(
            runtime.recorded()[1..],
            [
                "resumed:app",
                "getfenv:nil",
                "private:false",
                "capability:false"
            ]
        );
        assert!(
            leftover_tasks(&runtime)?.is_empty(),
            "a plain yield hands the coroutine back to the code that resumes it"
        );

        let call = call_handler(&runtime, 0)?;
        runtime.run_ready();
        assert_eq!(runtime.recorded()[5..], ["handler:suspended", "manual:app"]);
        assert_eq!(
            runtime.next_delay(),
            Some(SOON),
            "the coroutine waits again"
        );
        assert!(matches!(completion_result(&call), Some(Ok(_))));

        runtime.advance(SOON);
        assert_eq!(runtime.recorded()[7..], ["again:app"]);
        assert!(leftover_tasks(&runtime)?.is_empty());
        Ok(())
    }

    #[test]
    fn eval_sources_run_as_their_own_chunk() -> anyhow::Result<()> {
        let runtime = task_test_runtime_with(
            Arc::new(Mutex::new(Vec::new())),
            &[("lib", &[], "return {}")],
            &[],
        )?;
        assert!(runtime.exec_all().is_empty());

        let escape = eval_as(
            &runtime,
            "lib",
            b"end end and (function() error('escaped') end)() and function(require) return function(...) return 1",
        )
        .expect_err("source can't close a wrapper around it");
        assert!(!escape.to_string().contains("escaped"), "{escape:#}");

        let error = eval_as(&runtime, "lib", b"local x = 1\nerror('boom')")
            .expect_err("the error propagates");
        assert!(error.to_string().contains(":2:"), "{error:#}");
        Ok(())
    }

    #[test]
    fn failed_entrypoint_ends_its_run() -> anyhow::Result<()> {
        let runtime = StagedRuntime::new(
            &[(
                "demo",
                &[],
                r#"
                    local tasks = require('@test/tasks')
                    tasks.later(0.05, function()
                        tasks.record("demo")
                    end)
                    error("entrypoint failed")
                "#,
            )],
            &[],
        )?;

        assert_eq!(runtime.exec_all(), ["demo"]);
        assert!(
            leftover_tasks(&runtime)?.is_empty(),
            "the failed run's task is left waiting"
        );
        runtime.advance(LATER);
        assert!(runtime.recorded().is_empty());
        Ok(())
    }

    #[test]
    fn a_require_cannot_start_a_plugin_run() -> anyhow::Result<()> {
        let runtime = StagedRuntime::new(
            &[
                (
                    "lib",
                    &[],
                    r#"
                        require('@test/tasks').record("lib")
                        error("entrypoint failed")
                    "#,
                ),
                ("app", &["lib"], "require('@lib')"),
            ],
            &[(
                "plugin:lib/extra",
                r#"require('@test/tasks').record("extra")"#,
            )],
        )?;
        let assert_not_running = |error: anyhow::Error| {
            assert!(
                format!("{error:#}").contains("plugin 'lib' is not running"),
                "{error:#}"
            );
        };

        assert_not_running(
            runtime
                .eval_as("app", b"require('@lib')")
                .expect_err("lib has no run until the host starts one"),
        );
        runtime.run_ready();
        assert!(runtime.recorded().is_empty());
        assert!(leftover_tasks(&runtime)?.is_empty());

        assert_eq!(runtime.exec_all(), ["lib", "app"]);
        assert_not_running(
            runtime
                .eval_as("app", b"require('@lib/extra')")
                .expect_err("lib's run ended, so requiring its code fails"),
        );
        runtime.run_ready();
        assert_eq!(runtime.recorded(), ["lib"]);
        Ok(())
    }

    #[test]
    fn exports_left_by_a_restarted_plugin_cannot_act_for_it() -> anyhow::Result<()> {
        let runtime = task_test_runtime_with(
            Arc::new(Mutex::new(Vec::new())),
            &[
                (
                    "lib",
                    &[],
                    r#"
                        local tasks = require('@test/tasks')
                        own_check = tasks.check()
                        return { check = tasks.check }
                    "#,
                ),
                (
                    "app",
                    &["lib"],
                    r#"
                        kept_check = require('@lib').check
                    "#,
                ),
            ],
            &[],
        )?;
        assert!(runtime.exec_all().is_empty());
        assert_eq!(
            eval_as(&runtime, "lib", b"return own_check")?,
            vec![luau::Value::Nil]
        );
        assert_eq!(
            eval_as(&runtime, "app", b"return kept_check()")?,
            vec![luau::Value::Nil],
            "a live export acts as its plugin's current run"
        );

        runtime.restart_plugin("lib")?;
        let values = eval_as(&runtime, "app", b"return kept_check()")?;
        let [luau::Value::String(error)] = values.as_slice() else {
            panic!("a stale export must be refused, got {values:?}");
        };
        assert!(
            String::from_utf8_lossy(error).contains("run that has ended"),
            "{}",
            String::from_utf8_lossy(error)
        );
        assert_eq!(
            eval_as(&runtime, "lib", b"return own_check")?,
            vec![luau::Value::Nil]
        );
        Ok(())
    }

    #[test]
    fn restart_plugin_cancels_handler_calls_of_the_previous_run() -> anyhow::Result<()> {
        let runtime = StagedRuntime::numbered_runs()?;
        assert!(runtime.exec_all().is_empty());
        let demo_call = call_handler(&runtime, 0)?;
        let other_call = call_handler(&runtime, 1)?;
        runtime.run_ready();
        assert!(completion_result(&demo_call).is_none());

        runtime.restart_plugin("demo")?;

        let error = completion_result(&demo_call)
            .expect("in-flight call must finish when its plugin restarts")
            .expect_err("in-flight call must fail");
        assert!(error.contains("cancelled"), "{error}");
        let late_call = call_handler(&runtime, 0)?;
        runtime.run_ready();
        let error = completion_result(&late_call)
            .expect("a call into the previous run must finish")
            .expect_err("a call into the previous run must fail");
        assert!(error.contains("cancelled"), "{error}");
        assert!(completion_result(&other_call).is_none());
        Ok(())
    }

    #[test]
    fn runtime_builder_enforces_manifest_capability_policy() -> anyhow::Result<()> {
        let runtime = RuntimeBuilder::new(
            MemorySourceLoader::new(),
            manifest_arc(vec![manifest("demo", &[])]),
        )
        .modules(vec![GatedModule::new(
            "demo.secret",
            ModuleSpec::new("demo/secret").function(FunctionSpec::new("value").call(
                |mut frame| {
                    frame.returns.write("secret")?;
                    Ok(())
                },
            )),
        )])
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
        .modules(vec![GatedModule::new(
            "demo.math",
            ModuleSpec::new("demo/math").function(FunctionSpec::new("answer").call(|mut frame| {
                frame.returns.write(42_i64)?;
                Ok(())
            })),
        )])
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

        assert_eq!(values, vec![luau::Value::Number(42.0)]);
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
        .modules(vec![GatedModule::new(
            "demo.store",
            ModuleSpec::new("demo/store").function(FunctionSpec::new("value").call(|mut frame| {
                let store = frame.vm.data().get::<TestStore>()?;
                frame.returns.write(store.0)?;
                Ok(())
            })),
        )])
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
        let order = Arc::new(Mutex::new(Vec::<String>::new()));
        let order_for_module = order.clone();
        let module = ModuleSpec::new("test/order")
            .function(FunctionSpec::new("record").call(move |mut frame| {
                let id: String = frame.args.read_named("id")?;
                order_for_module
                    .lock()
                    .expect("order mutex poisoned")
                    .push(id);
                Ok(())
            }))
            .install(|_| Ok(ModuleExport::new(())));

        let mut loader = MemorySourceLoader::new();
        loader.insert(
            "plugin:alpha/init",
            b"require('@test/order').record('alpha')".as_slice(),
        );
        loader.insert(
            "plugin:beta/init",
            b"require('@test/order').record('beta')".as_slice(),
        );

        let alpha = LoadedPlugin {
            manifest: manifest("alpha", &["test.order"]),
            directory: PathBuf::from("plugins/alpha"),
            declared_scopes: HashSet::new(),
        };
        let beta = LoadedPlugin {
            manifest: manifest("beta", &["test.order"]),
            directory: PathBuf::from("plugins/beta"),
            declared_scopes: HashSet::new(),
        };
        let runtime = RuntimeBuilder::new(
            loader,
            manifest_arc(vec![alpha.manifest.clone(), beta.manifest.clone()]),
        )
        .plugins(Arc::from(vec![alpha, beta]))
        .modules(vec![GatedModule::new("test.order", module)])
        .build()?;

        assert!(runtime.exec_all().is_empty());

        assert_eq!(
            *order.lock().expect("order mutex poisoned"),
            vec!["alpha".to_string(), "beta".to_string()]
        );
        Ok(())
    }

    fn manifest_json(id: &str, extra: &str) -> String {
        format!(
            r#"{{"schema_version":1,"id":"{id}","name":"Demo","version":"1.0.0","host":"0.0.1","description":"d","entrypoint":"init.luau","scopes":["net"]{extra}}}"#
        )
    }

    fn host() -> PluginHost {
        PluginHost::new(
            "host",
            Version::new(0, 0, 1),
            HashSet::from([Arc::<str>::from("net")]),
        )
        .unwrap()
    }

    fn load_manifest(bytes: &[u8]) -> Result<LoadedManifest, PluginLoadError> {
        PluginManager::load_manifest(std::path::Path::new("plugin.json"), bytes, &host())
    }

    #[test]
    fn load_manifest_accepts_valid_manifests_without_a_directory() {
        let json = manifest_json(
            "demo",
            r#","dependencies":{"other":"0.2","extra":{"version":"1","optional":true}},"unknown":1"#,
        );
        let loaded = load_manifest(json.as_bytes()).unwrap();

        assert_eq!(loaded.manifest.id, "demo");
        assert_eq!(loaded.manifest.name, "Demo");
        assert_eq!(loaded.manifest.version, Version::new(1, 0, 0));
        assert_eq!(
            loaded.manifest.host_requirement,
            VersionReq::parse("0.0.1").unwrap()
        );
        assert_eq!(loaded.manifest.description, "d");
        assert_eq!(loaded.manifest.scopes, vec!["net"]);
        assert!(loaded.declared_scopes.contains("net"));
        assert_eq!(
            loaded.manifest.dependencies,
            dependencies(&[("other", dependency("0.2")), ("extra", optional("1"))])
        );
    }

    #[test]
    fn load_manifest_rejects_a_narrow_scope_without_its_parent() {
        let host = PluginHost::new(
            "host",
            Version::new(0, 0, 1),
            HashSet::from([Arc::<str>::from("net"), Arc::<str>::from("net.all")]),
        )
        .unwrap()
        .with_scope_parents([("net.all", "net")]);
        let load = |scopes: &str| {
            let json = manifest_json("demo", "").replace(r#"["net"]"#, scopes);
            PluginManager::load_manifest(
                std::path::Path::new("plugin.json"),
                json.as_bytes(),
                &host,
            )
        };

        assert!(load(r#"["net.all","net"]"#).is_ok());
        let error = load(r#"["net.all"]"#).unwrap_err();
        assert!(matches!(
            &error,
            PluginLoadError::ScopeWithoutParent { scope, parent, .. }
                if scope == "net.all" && parent == "net"
        ));
        assert_eq!(
            error.to_string(),
            "plugin 'demo' declares scope 'net.all' without 'net', the scope it narrows"
        );
    }

    fn load_scopes(scopes: serde_json::Value) -> Result<LoadedManifest, PluginLoadError> {
        let host = PluginHost::new(
            "host",
            Version::new(0, 0, 1),
            HashSet::from([
                Arc::<str>::from("net"),
                Arc::<str>::from("net.all"),
                Arc::<str>::from("dns"),
            ]),
        )
        .unwrap()
        .with_scope_parents([("net.all", "net")]);
        let json = manifest_json("demo", "").replace(r#"["net"]"#, &scopes.to_string());
        PluginManager::load_manifest(std::path::Path::new("plugin.json"), json.as_bytes(), &host)
    }

    fn scope_parse_error(scopes: serde_json::Value) -> String {
        match load_scopes(scopes) {
            Err(PluginLoadError::ConfigParseError { error, .. }) => error,
            other => panic!("expected a parse error: {other:?}"),
        }
    }

    #[test]
    fn scopes_are_plain_ids_or_objects_with_a_reason_in_the_order_written() {
        let plain = load_scopes(serde_json::json!(["net", "dns"]))
            .unwrap()
            .manifest;
        assert_eq!(plain.scopes, ["net", "dns"]);
        assert!(plain.scope_reasons.is_empty());

        let objects = load_scopes(serde_json::json!([
            { "id": "net", "reason": "Fetches things." },
            { "id": "dns", "reason": "Resolves things." },
        ]))
        .unwrap();
        assert_eq!(objects.manifest.scopes, ["net", "dns"]);
        assert_eq!(objects.manifest.scope_reasons.len(), 2);
        assert!(objects.declared_scopes.contains("dns"));

        let mixed = load_scopes(serde_json::json!([
            "net.all",
            { "id": "dns", "reason": "Resolves things." },
            "net",
        ]))
        .unwrap()
        .manifest;
        assert_eq!(mixed.scopes, ["net.all", "dns", "net"]);
        assert_eq!(
            mixed.scope_reasons,
            BTreeMap::from([("dns".to_string(), "Resolves things.".to_string())])
        );
    }

    #[test]
    fn scope_objects_hold_exactly_a_string_id_and_reason() {
        for (scopes, expected) in [
            (
                serde_json::json!([{ "id": "net", "reason": "r", "extra": 1 }]),
                "unknown field `extra`",
            ),
            (
                serde_json::json!([{ "id": "net" }]),
                "missing field `reason`",
            ),
            (serde_json::json!([{ "reason": "r" }]), "missing field `id`"),
            (
                serde_json::json!([{ "id": 1, "reason": "r" }]),
                "invalid type",
            ),
            (
                serde_json::json!([{ "id": "net", "reason": 1 }]),
                "invalid type",
            ),
            (
                serde_json::json!([{ "id": "net", "reason": null }]),
                "invalid type",
            ),
            (serde_json::json!([1]), "a scope id or an object"),
            (
                serde_json::json!(["net", ["dns"]]),
                "a scope id or an object",
            ),
            (serde_json::json!("net"), "invalid type"),
        ] {
            let error = scope_parse_error(scopes.clone());
            assert!(error.contains(expected), "{scopes}: {error}");
        }
        let missing = manifest_json("demo", "").replace(r#""scopes":["net"]"#, r#""z":0"#);
        assert!(matches!(
            load_manifest(missing.as_bytes()),
            Err(PluginLoadError::ConfigParseError { error, .. }) if error.contains("scopes")
        ));
    }

    #[test]
    fn scope_reasons_are_short_single_lines_of_text() {
        let reason = |reason: &str| serde_json::json!([{ "id": "net", "reason": reason }]);
        for bad in [
            "",
            "   ",
            "first\nsecond",
            "tab\there",
            "bell\u{7}",
            "delete\u{7f}",
            "next line\u{85}",
            "line\u{2028}separator",
            "paragraph\u{2029}separator",
            "\u{202e}reversed",
            "\u{2067}isolated",
        ] {
            let error = scope_parse_error(reason(bad));
            assert!(
                error.contains("plugin 'demo' gives scope 'net' an invalid reason"),
                "{bad:?}: {error}"
            );
        }
        assert!(
            scope_parse_error(reason(&"x".repeat(201)))
                .ends_with("an invalid reason: it is longer than 200 characters")
        );

        for accepted in ["x".repeat(200), "é".repeat(200)] {
            let manifest = load_scopes(reason(&accepted)).unwrap().manifest;
            assert_eq!(manifest.scope_reasons["net"], accepted);
        }
    }

    /// Items are lowercase words, written as an array, in the key `words`.
    #[derive(Debug)]
    struct Words;

    impl crate::scopes::ScopeGrant for Words {
        fn key(&self) -> &str {
            "words"
        }

        fn parse(&self, value: &serde_json::Value) -> Result<Vec<String>, String> {
            let words: Vec<String> = serde_json::from_value(value.clone())
                .map_err(|error| format!("it is not a list of words: {error}"))?;
            let mut items = std::collections::BTreeSet::new();
            for word in words {
                if word.is_empty() || !word.chars().all(|c| c.is_ascii_lowercase()) {
                    return Err(format!("'{word}' is not a lowercase word"));
                }
                if !items.insert(word.clone()) {
                    return Err(format!("'{word}' is listed more than once"));
                }
            }
            Ok(items.into_iter().collect())
        }

        fn covers(&self, granted: &[String], item: &str) -> bool {
            granted.iter().any(|word| word == item)
        }
    }

    fn load_granted_scopes(scopes: serde_json::Value) -> Result<LoadedManifest, PluginLoadError> {
        let host = PluginHost::new(
            "host",
            Version::new(0, 0, 1),
            HashSet::from([Arc::<str>::from("net"), Arc::<str>::from("dns")]),
        )
        .unwrap()
        .with_scope_grant("net", Arc::new(Words));
        let json = manifest_json("demo", "").replace(r#"["net"]"#, &scopes.to_string());
        PluginManager::load_manifest(std::path::Path::new("plugin.json"), json.as_bytes(), &host)
    }

    fn grant_parse_error(scopes: serde_json::Value) -> String {
        match load_granted_scopes(scopes) {
            Err(PluginLoadError::ConfigParseError { error, .. }) => error,
            other => panic!("expected a parse error: {other:?}"),
        }
    }

    #[test]
    fn a_scope_entry_holds_the_grant_its_scope_takes() {
        let manifest = load_granted_scopes(serde_json::json!([
            "dns",
            { "id": "net", "reason": "Fetches things.", "words": ["b", "a"] },
        ]))
        .unwrap()
        .manifest;

        assert_eq!(manifest.scopes, ["dns", "net"]);
        assert_eq!(
            manifest.scope_grants,
            BTreeMap::from([("net".to_string(), vec!["a".to_string(), "b".to_string()])])
        );

        let empty = load_granted_scopes(
            serde_json::json!([{ "id": "net", "reason": "Encodes things.", "words": [] }]),
        )
        .unwrap()
        .manifest;
        assert_eq!(empty.scope_grants["net"], Vec::<String>::new());
    }

    #[test]
    fn a_scope_entry_takes_no_key_but_its_scopes_grant() {
        for (scopes, expected) in [
            (
                serde_json::json!([{ "id": "net", "reason": "r", "words": [], "extra": 1 }]),
                "plugin 'demo' gives scope 'net' an unknown field `extra`",
            ),
            (
                serde_json::json!([{ "id": "dns", "reason": "r", "words": [] }]),
                "plugin 'demo' gives scope 'dns' an unknown field `words`",
            ),
            (
                serde_json::json!([{ "id": "net", "words": [] }]),
                "missing field `reason`",
            ),
            (
                serde_json::json!([{ "id": "net", "reason": "r", "words": "a" }]),
                "plugin 'demo' gives scope 'net' an invalid `words`: it is not a list of words",
            ),
            (
                serde_json::json!([{ "id": "net", "reason": "r", "words": ["A"] }]),
                "an invalid `words`: 'A' is not a lowercase word",
            ),
            (
                serde_json::json!([{ "id": "net", "reason": "r", "words": ["a", "a"] }]),
                "an invalid `words`: 'a' is listed more than once",
            ),
        ] {
            let error = grant_parse_error(scopes.clone());
            assert!(error.contains(expected), "{scopes}: {error}");
        }

        for (field, twice) in [
            ("id", r#"{"id":"net","id":"net","reason":"r","words":[]}"#),
            (
                "reason",
                r#"{"id":"net","reason":"r","reason":"r","words":[]}"#,
            ),
            (
                "words",
                r#"{"id":"net","reason":"r","words":[],"words":[]}"#,
            ),
        ] {
            let json = manifest_json("demo", "").replace(r#""net""#, twice);
            let error = match load_manifest(json.as_bytes()) {
                Err(PluginLoadError::ConfigParseError { error, .. }) => error,
                other => panic!("expected a parse error: {other:?}"),
            };
            assert!(
                error.contains(&format!("duplicate field `{field}`")),
                "{error}"
            );
        }
    }

    #[test]
    fn a_grant_lists_no_more_items_than_its_cap() {
        let words = |count: usize| -> Vec<String> {
            (0..count)
                .map(|index| {
                    let first = char::from(b'a' + (index / 26) as u8);
                    let second = char::from(b'a' + (index % 26) as u8);
                    format!("{first}{second}")
                })
                .collect()
        };
        let scopes =
            |count| serde_json::json!([{ "id": "net", "reason": "r", "words": words(count) }]);
        let cap = super::PLUGIN_CONFIG_MAX_SCOPE_GRANT_ITEMS;

        let manifest = load_granted_scopes(scopes(cap)).unwrap().manifest;
        assert_eq!(manifest.scope_grants["net"].len(), cap);
        assert!(
            grant_parse_error(scopes(cap + 1))
                .ends_with("an invalid `words`: it lists 33 items, exceeds cap of 32")
        );
    }

    #[test]
    fn a_scope_without_its_required_grant_parses_but_does_not_load() {
        let object = serde_json::json!([{ "id": "net", "reason": "Fetches things." }]);
        for scopes in [serde_json::json!(["net"]), object.clone()] {
            let error = load_granted_scopes(scopes.clone()).unwrap_err();
            assert!(
                matches!(
                    &error,
                    PluginLoadError::MissingScopeGrant { plugin_id, scope, key }
                        if plugin_id == "demo" && scope == "net" && key == "words"
                ),
                "{scopes}: {error}"
            );
            assert_eq!(
                error.to_string(),
                "plugin 'demo' declares scope 'net' without `words`, which the scope requires"
            );
        }

        let host = PluginHost::new(
            "host",
            Version::new(0, 0, 1),
            HashSet::from([Arc::<str>::from("net")]),
        )
        .unwrap()
        .with_scope_grant("net", Arc::new(Words));
        let json = manifest_json("demo", "").replace(r#"["net"]"#, &object.to_string());
        let parsed = PluginManifest::from_json(&json, &host).expect("the manifest still parses");
        assert_eq!(parsed.scopes, ["net"]);
        assert!(parsed.scope_grants.is_empty());
    }

    #[test]
    fn a_host_mismatch_is_reported_before_a_missing_grant() {
        let host = PluginHost::new(
            "host",
            Version::new(0, 0, 2),
            HashSet::from([Arc::<str>::from("net")]),
        )
        .unwrap()
        .with_scope_grant("net", Arc::new(Words));
        let error = PluginManager::load_manifest(
            std::path::Path::new("plugin.json"),
            manifest_json("demo", "").as_bytes(),
            &host,
        )
        .unwrap_err();
        assert!(matches!(error, PluginLoadError::IncompatibleHost { .. }));
    }

    #[test]
    fn a_scope_may_be_listed_once_in_either_form() {
        let object = serde_json::json!({ "id": "net", "reason": "Fetches things." });
        for scopes in [
            serde_json::json!(["net", "net"]),
            serde_json::json!(["net", object]),
            serde_json::json!([object, "net"]),
            serde_json::json!([object, object]),
        ] {
            assert_eq!(
                scope_parse_error(scopes),
                "plugin 'demo' lists scope 'net' more than once"
            );
        }
    }

    #[test]
    fn scope_objects_pass_the_same_checks_as_plain_ids() {
        let explained = |id: &str| serde_json::json!([{ "id": id, "reason": "Needs it." }]);
        assert!(matches!(
            load_scopes(explained("fs")),
            Err(PluginLoadError::UnknownScope { scope, .. }) if scope == "fs"
        ));
        assert!(matches!(
            load_scopes(explained("net.all")),
            Err(PluginLoadError::ScopeWithoutParent { scope, parent, .. })
                if scope == "net.all" && parent == "net"
        ));
        assert!(matches!(
            load_scopes(explained(
                &"s".repeat(super::PLUGIN_CONFIG_MAX_SCOPE_LEN + 1)
            )),
            Err(PluginLoadError::ScopeEntryTooLong { .. })
        ));
    }

    #[test]
    fn load_manifest_rejects_manifest_level_errors() {
        let long_scope = format!(
            r#"{{"schema_version":1,"id":"demo","name":"d","version":"1.0.0","host":"*","description":"","scopes":["{}"]}}"#,
            "s".repeat(super::PLUGIN_CONFIG_MAX_SCOPE_LEN + 1)
        );
        let err = |bytes: &[u8]| load_manifest(bytes).unwrap_err();

        assert!(matches!(
            err(&vec![b' '; super::PLUGIN_CONFIG_MAX_BYTES as usize + 1]),
            PluginLoadError::ConfigTooLarge { .. }
        ));
        assert!(matches!(
            err(&[0xff, 0xfe]),
            PluginLoadError::ConfigParseError { error, .. }
                if error == "stream did not contain valid UTF-8"
        ));
        assert!(matches!(
            err(b"{"),
            PluginLoadError::ConfigParseError { .. }
        ));
        assert!(matches!(
            err(manifest_json("demo", "")
                .replace(r#""schema_version":1"#, r#""schema_version":2"#)
                .as_bytes()),
            PluginLoadError::InvalidSchemaVersion { version: 2, .. }
        ));
        assert!(matches!(
            err(manifest_json("not/valid", "").as_bytes()),
            PluginLoadError::InvalidPluginId { .. }
        ));
        assert!(matches!(
            err(long_scope.as_bytes()),
            PluginLoadError::ScopeEntryTooLong { .. }
        ));
        assert!(matches!(
            err(manifest_json("demo", "").replace(r#"["net"]"#, r#"["fs"]"#).as_bytes()),
            PluginLoadError::UnknownScope { scope, .. } if scope == "fs"
        ));
        assert!(matches!(
            err(manifest_json("demo", r#","dependencies":{"other":"1","other":"2"}"#).as_bytes()),
            PluginLoadError::ConfigParseError { error, .. }
                if error.starts_with("dependency 'other' is listed more than once")
        ));
        assert!(matches!(
            err(manifest_json("demo", r#","dependencies":{"demo":"1"}"#).as_bytes()),
            PluginLoadError::SelfDependency { .. }
        ));
        assert!(matches!(
            err(manifest_json("demo", r#","dependencies":{"not/valid":"1"}"#).as_bytes()),
            PluginLoadError::InvalidDependencyId { .. }
        ));
        let too_many = (0..=super::PLUGIN_CONFIG_MAX_DEPENDENCIES)
            .map(|i| format!(r#""dep{i}":"1""#))
            .collect::<Vec<_>>()
            .join(",");
        assert!(matches!(
            err(manifest_json("demo", &format!(r#","dependencies":{{{too_many}}}"#)).as_bytes()),
            PluginLoadError::TooManyDependencies { .. }
        ));
    }

    #[test]
    fn load_manifest_parses_versions_and_checks_the_host() {
        let err = |json: String| load_manifest(json.as_bytes()).unwrap_err().to_string();

        for (from, to, expected) in [
            (
                r#""version":"1.0.0""#,
                r#""version":"1""#,
                r#"invalid version "1""#,
            ),
            (r#""version":"1.0.0","#, "", "missing field `version`"),
            (
                r#""host":"0.0.1""#,
                r#""host":"latest""#,
                r#"invalid host version requirement "latest""#,
            ),
            (r#""host":"0.0.1","#, "", "missing field `host`"),
        ] {
            let message = err(manifest_json("demo", "").replace(from, to));
            assert!(message.contains(expected), "{message}");
        }

        assert_eq!(
            err(manifest_json("demo", "").replace(r#""host":"0.0.1""#, r#""host":"0.0.2""#)),
            "plugin 'demo' requires host ^0.0.2, but this is host 0.0.1"
        );
        for requirement in ["0.0", ">=0.0.1, <0.1.0", "*"] {
            let json = manifest_json("demo", "")
                .replace(r#""host":"0.0.1""#, &format!(r#""host":"{requirement}""#));
            assert!(load_manifest(json.as_bytes()).is_ok(), "{requirement}");
        }
    }

    #[test]
    fn load_manifest_reads_the_requirement_under_the_host_name() {
        let other = PluginHost::new(
            "other",
            Version::new(2, 0, 0),
            HashSet::from([Arc::<str>::from("net")]),
        )
        .unwrap();
        let load = |json: String| {
            PluginManager::load_manifest(
                std::path::Path::new("plugin.json"),
                json.as_bytes(),
                &other,
            )
        };
        let named =
            |requirement: &str| manifest_json("demo", &format!(r#","other":"{requirement}""#));

        let loaded = load(named("2")).unwrap();
        assert_eq!(
            loaded.manifest.host_requirement,
            VersionReq::parse("2").unwrap()
        );
        assert_eq!(
            load(named("1")).unwrap_err().to_string(),
            "plugin 'demo' requires other ^1, but this is other 2.0.0"
        );
        let message = load(manifest_json("demo", "")).unwrap_err().to_string();
        assert!(message.contains("missing field `other`"), "{message}");
    }

    #[test]
    fn load_manifest_rejects_a_host_requirement_named_twice() {
        let json = manifest_json("demo", "")
            .replace(r#""host":"0.0.1""#, r#""host":"0.0.1","host":"0.0.2""#);

        let message = load_manifest(json.as_bytes()).unwrap_err().to_string();

        assert!(message.contains("duplicate field `host`"), "{message}");
    }

    #[test]
    fn hosts_reserve_plugin_ids_at_manifest_and_directory_load() {
        let reserving = host().with_reserved_ids(["update"]);
        let json = manifest_json("update", "");

        assert!(load_manifest(json.as_bytes()).is_ok());
        let err = PluginManager::load_manifest(
            std::path::Path::new("plugin.json"),
            json.as_bytes(),
            &reserving,
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid plugin id 'update': 'update' is reserved by host"
        );

        let plugins_dir = temp_plugin_dir("reserved-ids");
        for id in ["update", "demo"] {
            let dir = plugins_dir.join(id);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("init.luau"), "").unwrap();
            std::fs::write(
                dir.join(super::PLUGIN_CONFIG_FILENAME),
                manifest_json(id, ""),
            )
            .unwrap();
        }
        let mut mgr = PluginManager::new(plugins_dir);
        let errors = mgr.discover_plugins(&reserving).unwrap();
        assert!(
            matches!(errors.as_slice(), [PluginDiscoveryError { directories, error: PluginLoadError::InvalidPluginId { plugin_id, .. } }] if plugin_id == "update" && directories == &["update"]),
            "{errors:?}"
        );
        assert_eq!(order(&mgr), vec!["demo"]);
        assert!(matches!(
            mgr.reload_plugin("update", &reserving),
            Err(PluginLoadError::InvalidPluginId { .. })
        ));
    }

    #[test]
    fn host_names_must_not_shadow_fixed_fields() {
        for name in ["", "id", "version", "dependencies", "commit"] {
            assert_eq!(
                PluginHost::new(name, Version::new(0, 0, 1), HashSet::new()).unwrap_err(),
                super::InvalidHostName(name.to_string()),
                "{name:?}"
            );
        }
    }

    #[test]
    fn missing_entrypoint_passes_the_manifest_but_fails_the_directory() {
        let dir = temp_plugin_dir("missing-entrypoint");
        let json = manifest_json("demo", "");
        std::fs::write(dir.join(super::PLUGIN_CONFIG_FILENAME), &json).unwrap();
        assert!(load_manifest(json.as_bytes()).is_ok());
        let err = PluginManager::load_plugin_dir(&dir, &host()).unwrap_err();
        assert!(
            matches!(&err, PluginLoadError::EntrypointNotFound { path, .. } if *path == dir.join("init.luau")),
            "{err}"
        );

        std::fs::write(dir.join("init.luau"), "").unwrap();
        let loaded = PluginManager::load_plugin_dir(&dir, &host()).unwrap();
        assert_eq!(loaded.directory, dir);
        assert!(loaded.declared_scopes.contains("net"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_plugin_dir_reports_config_errors_against_the_config_path() {
        let dir = temp_plugin_dir("config-errors");
        let config = dir.join(super::PLUGIN_CONFIG_FILENAME);

        let err = PluginManager::load_plugin_dir(&dir, &host()).unwrap_err();
        assert!(
            matches!(&err, PluginLoadError::ConfigNotFound(path) if *path == config),
            "{err}"
        );

        std::fs::write(&config, [0xff, 0xfe]).unwrap();
        let err = PluginManager::load_plugin_dir(&dir, &host()).unwrap_err();
        assert_eq!(
            err.to_string(),
            format!(
                "failed to parse {}: stream did not contain valid UTF-8",
                config.display()
            )
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn discover_and_reload_check_host_and_dependency_versions() {
        let plugins_dir = temp_plugin_dir("versions");
        let write = |id: &str, version: &str, extra: &str| {
            let dir = plugins_dir.join(id);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("init.luau"), "").unwrap();
            std::fs::write(
                dir.join(super::PLUGIN_CONFIG_FILENAME),
                manifest_json(id, extra).replace("1.0.0", version),
            )
            .unwrap();
        };
        write("lib", "0.1.2", "");
        write("consumer", "1.0.0", r#","dependencies":{"lib":"0.1"}"#);
        write("future", "1.0.0", "");
        std::fs::write(
            plugins_dir
                .join("future")
                .join(super::PLUGIN_CONFIG_FILENAME),
            manifest_json("future", "").replace(r#""host":"0.0.1""#, r#""host":"0.1""#),
        )
        .unwrap();

        let mut mgr = PluginManager::new(plugins_dir.clone());
        let errors = mgr.discover_plugins(&host()).unwrap();
        let messages: Vec<String> = errors.iter().map(ToString::to_string).collect();
        assert_eq!(
            messages,
            vec!["plugin 'future' requires host ^0.1, but this is host 0.0.1"]
        );
        assert_eq!(errors[0].directories, ["future"]);
        assert_eq!(order(&mgr), vec!["lib", "consumer"]);

        write("lib", "0.2.0", "");
        let err = mgr.reload_plugin("lib", &host()).unwrap_err();
        assert_eq!(
            err.to_string(),
            "plugin 'consumer' requires plugin 'lib' ^0.1, but 0.2.0 is installed"
        );
        assert_eq!(
            mgr.get_plugin("lib").unwrap().manifest.version,
            Version::new(0, 1, 2)
        );

        write("lib", "0.1.3", "");
        mgr.reload_plugin("lib", &host()).unwrap();
        assert_eq!(
            mgr.get_plugin("lib").unwrap().manifest.version,
            Version::new(0, 1, 3)
        );
        let _ = std::fs::remove_dir_all(&plugins_dir);
    }

    #[test]
    fn optional_dependency_that_failed_to_load_cannot_be_required() -> anyhow::Result<()> {
        let plugins_dir = temp_plugin_dir("failed-optional");
        for (id, extra) in [
            (
                "consumer",
                r#","dependencies":{"helper":{"version":"1","optional":true}}"#,
            ),
            ("helper", ""),
        ] {
            let dir = plugins_dir.join(id);
            std::fs::create_dir_all(&dir)?;
            std::fs::write(dir.join("init.luau"), "")?;
            std::fs::write(
                dir.join(super::PLUGIN_CONFIG_FILENAME),
                manifest_json(id, extra),
            )?;
        }
        std::fs::write(
            plugins_dir.join("helper").join("lib.luau"),
            "helper_ran = true\nreturn {}",
        )?;
        std::fs::write(
            plugins_dir
                .join("helper")
                .join(super::PLUGIN_CONFIG_FILENAME),
            manifest_json("helper", "").replace(r#""host":"0.0.1""#, r#""host":"0.1""#),
        )?;

        let mut mgr = PluginManager::new(plugins_dir.clone());
        let errors = mgr.discover_plugins(&host())?;
        assert!(
            matches!(
                errors[..],
                [PluginDiscoveryError {
                    error: PluginLoadError::IncompatibleHost { .. },
                    ..
                }]
            ),
            "{errors:?}"
        );
        let plugins: Vec<_> = mgr.list_plugins().cloned().collect();
        let manifests = plugins
            .iter()
            .map(|plugin| plugin.manifest.clone())
            .collect();
        let runtime = RuntimeBuilder::new(
            crate::FilesystemSourceLoader::new(&plugins_dir, &plugins_dir),
            manifest_arc(manifests),
        )
        .plugins(Arc::from(plugins))
        .build()?;

        let error = runtime
            .eval_source_with_context(
                Arc::<[u8]>::from(&b"return require('@helper/lib')"[..]),
                CallContext {
                    origin: plugin_origin("consumer", "init.luau"),
                    ..CallContext::default()
                },
            )
            .expect_err("a dependency that failed to load must not be requirable");
        assert!(
            error.to_string().contains("which is not a loaded plugin"),
            "{error}"
        );
        let ran = runtime.eval_source_with_context(
            Arc::<[u8]>::from(&b"return helper_ran"[..]),
            CallContext::default(),
        )?;
        assert_eq!(ran, vec![luau::Value::Nil]);
        let _ = std::fs::remove_dir_all(&plugins_dir);
        Ok(())
    }

    #[test]
    fn rejects_reserved_self_id() {
        assert!(validate_plugin_id("self").is_err());
    }

    #[test]
    fn cross_plugin_require_needs_declared_dependency() -> anyhow::Result<()> {
        let mut loader = MemorySourceLoader::new();
        loader.insert("plugin:lib/init", b"return { answer = 42 }".as_slice());

        let mut lib = manifest("lib", &[]);
        lib.entrypoint = None;
        let mut consumer = manifest("consumer", &[]);
        consumer.dependencies = dependencies(&[("lib", dependency("1"))]);
        let outsider = manifest("outsider", &[]);

        let library_plugin = LoadedPlugin {
            manifest: lib.clone(),
            directory: PathBuf::from("plugins/lib"),
            declared_scopes: HashSet::new(),
        };
        let runtime = RuntimeBuilder::new(loader, manifest_arc(vec![lib, consumer, outsider]))
            .plugins(Arc::from(vec![library_plugin]))
            .build()?;
        // Library plugins have no entrypoint; exec_all must skip them.
        assert!(runtime.exec_all().is_empty());

        let values = runtime.eval_source_with_context(
            Arc::<[u8]>::from(&b"return require('@lib').answer"[..]),
            CallContext {
                origin: plugin_origin("consumer", "init.luau"),
                ..CallContext::default()
            },
        )?;
        assert_eq!(values, vec![luau::Value::Number(42.0)]);

        let error = runtime
            .eval_source_with_context(
                Arc::<[u8]>::from(&b"return require('@lib').answer"[..]),
                CallContext {
                    origin: plugin_origin("outsider", "init.luau"),
                    ..CallContext::default()
                },
            )
            .expect_err("undeclared dependency must be denied");
        assert!(
            error
                .to_string()
                .contains("which is not a loaded plugin it declares as a dependency"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[test]
    fn require_of_executed_plugin_returns_entrypoint_exports_without_rerunning()
    -> anyhow::Result<()> {
        let runs = Arc::new(Mutex::new(0usize));
        let runs_for_module = runs.clone();
        let module = ModuleSpec::new("test/order")
            .function(FunctionSpec::new("record").call(move |_frame| {
                *runs_for_module.lock().expect("runs mutex poisoned") += 1;
                Ok(())
            }))
            .install(|_| Ok(ModuleExport::new(())));

        let mut loader = MemorySourceLoader::new();
        loader.insert(
            "plugin:dep/init",
            b"require('@test/order').record()\nreturn { answer = 7 }".as_slice(),
        );

        let dep = manifest("dep", &["test.order"]);
        let mut consumer = manifest("consumer", &[]);
        consumer.dependencies = dependencies(&[("dep", dependency("1"))]);

        let dep_plugin = LoadedPlugin {
            manifest: dep.clone(),
            directory: PathBuf::from("plugins/dep"),
            declared_scopes: HashSet::new(),
        };
        let runtime = RuntimeBuilder::new(loader, manifest_arc(vec![dep, consumer]))
            .plugins(Arc::from(vec![dep_plugin]))
            .modules(vec![GatedModule::new("test.order", module)])
            .build()?;
        assert!(runtime.exec_all().is_empty());

        let values = runtime.eval_source_with_context(
            Arc::<[u8]>::from(&b"return require('@dep').answer"[..]),
            CallContext {
                origin: plugin_origin("consumer", "init.luau"),
                ..CallContext::default()
            },
        )?;

        assert_eq!(values, vec![luau::Value::Number(7.0)]);
        assert_eq!(
            *runs.lock().expect("runs mutex poisoned"),
            1,
            "entrypoint side effects must not re-run on require"
        );
        Ok(())
    }

    #[test]
    fn require_in_registered_namespace_fails_as_unknown_module() -> anyhow::Result<()> {
        let mut loader = MemorySourceLoader::new();
        // Even a plugin named like a registered namespace must not resolve.
        loader.insert("plugin:test/x", b"return 1".as_slice());
        let runtime = RuntimeBuilder::new(loader, manifest_arc(Vec::new()))
            .modules(vec![GatedModule::new(
                "test.order",
                ModuleSpec::new("test/order").install(|_| Ok(ModuleExport::new(()))),
            )])
            .build()?;

        let error = runtime
            .eval_source_with_context(
                Arc::<[u8]>::from(&b"return require('@test/x')"[..]),
                CallContext::default(),
            )
            .expect_err("registered namespaces must not fall through to plugin sources");
        assert!(
            error.to_string().contains("is not registered"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[test]
    fn require_cycle_fails_fast() -> anyhow::Result<()> {
        let mut loader = MemorySourceLoader::new();
        loader.insert("plugin:demo/a", b"return require('@self/b')".as_slice());
        loader.insert("plugin:demo/b", b"return require('@self/a')".as_slice());
        let runtime =
            RuntimeBuilder::new(loader, manifest_arc(vec![manifest("demo", &[])])).build()?;

        let error = runtime
            .eval_source_with_context(
                Arc::<[u8]>::from(&b"return require('@self/a')"[..]),
                CallContext {
                    origin: plugin_origin("demo", "init.luau"),
                    ..CallContext::default()
                },
            )
            .expect_err("require cycles must fail fast");
        assert!(
            error.to_string().contains("require cycle"),
            "unexpected error: {error}"
        );
        Ok(())
    }
}
