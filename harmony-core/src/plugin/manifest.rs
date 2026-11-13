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
    HashMap,
    HashSet,
};
use std::fmt;
use std::path::Path;
use std::sync::Arc;

use super::{
    PluginLoadError,
    truncate_for_error,
};
use crate::scopes::ScopeGrant;

pub const PLUGIN_CONFIG_FILENAME: &str = "plugin.json";
pub const PLUGIN_SCHEMA_VERSION: u32 = 1;

/// Hard caps applied to `plugin.json` before `serde_json` parses it.
/// Prevents a hostile or malformed plugin config from OOMing `discover_plugins`.
pub const PLUGIN_CONFIG_MAX_BYTES: u64 = 64 * 1024;
const PLUGIN_CONFIG_MAX_SCOPE_LEN: usize = 128;
const PLUGIN_CONFIG_MAX_SCOPE_REASON_CHARS: usize = 200;
const PLUGIN_CONFIG_MAX_DEPENDENCIES: usize = 32;
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
/// [`Self::load`] produces it without a plugin directory.
#[derive(Debug, Clone)]
pub struct LoadedManifest {
    pub manifest: PluginManifest,
    /// Deduplicated, validated copy of `manifest.scopes` kept as `Arc<str>`
    /// so the runtime gate can share allocations across lookups.
    pub declared_scopes: HashSet<Arc<str>>,
}

impl LoadedManifest {
    /// Parses and validates `plugin.json` contents on their own, so a plugin
    /// can be vetted before any of its files are downloaded. `path` labels
    /// where the bytes came from, such as a file path or a repository URL
    /// path, and appears in errors; it is never read.
    pub fn load(path: &Path, bytes: &[u8], host: &PluginHost) -> Result<Self, PluginLoadError> {
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

        Ok(Self {
            manifest,
            declared_scopes,
        })
    }
}

pub(super) fn parse_manifest(
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

#[cfg(test)]
mod tests;
