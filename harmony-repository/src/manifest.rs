// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::path::Path;

use serde::{
    Deserialize,
    Serialize,
};

use crate::fetch::{
    FetchError,
    Forge,
    RepoSpec,
};

pub const REPOSITORY_MANIFEST_FILENAME: &str = "repository.json";
pub const REPOSITORY_SCHEMA_VERSION: u32 = 1;
pub const REPOSITORY_MANIFEST_MAX_BYTES: u64 = 64 * 1024;
pub const REPOSITORY_MANIFEST_MAX_ENTRIES: usize = 64;

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("{REPOSITORY_MANIFEST_FILENAME} is {bytes} bytes, exceeds cap of {max}")]
    TooLarge { bytes: u64, max: u64 },
    #[error("failed to parse {REPOSITORY_MANIFEST_FILENAME}: {reason}")]
    Parse { reason: String },
    #[error("invalid schema_version {version} (expected {REPOSITORY_SCHEMA_VERSION})")]
    InvalidSchemaVersion { version: u32 },
    #[error("repository name must not be empty")]
    EmptyName,
    #[error("repository lists {count} plugin entries, exceeds cap of {max}")]
    TooManyEntries { count: usize, max: usize },
    #[error("plugin entry #{index} must set exactly one of 'path' or 'url'")]
    AmbiguousEntry { index: usize },
    #[error("plugin entry #{index}: 'ref' is only valid together with 'url'")]
    RefWithoutUrl { index: usize },
    #[error("plugin entry #{index} has invalid path '{path}': {reason}")]
    InvalidPath {
        index: usize,
        path: String,
        reason: String,
    },
    #[error("plugin entry #{index} has an invalid url: {source}")]
    InvalidUrl {
        index: usize,
        #[source]
        source: FetchError,
    },
    #[error("duplicate plugin entry '{value}'")]
    DuplicateEntry { value: String },
    #[error("plugin path '{child}' is nested inside plugin path '{parent}'")]
    NestedPath { parent: String, child: String },
    #[error("plugin entry #{index} references its own repository")]
    SelfReference { index: usize },
}

/// Validated `repository.json`: a multi-plugin repository's index.
///
/// Every entry names either a directory in the same repository or another
/// Git repository whose root is a single plugin (`plugin.json`). Remote
/// entries may not point at another repository index, which keeps the
/// reference graph one level deep by construction.
#[derive(Debug, Clone)]
pub struct RepositoryManifest {
    pub schema_version: u32,
    pub name: String,
    pub description: String,
    pub entries: Vec<RepositoryEntry>,
}

#[derive(Debug, Clone)]
pub enum RepositoryEntry {
    /// Directory inside the repository, normalized to `a/b` form, holding a
    /// `plugin.json` directly.
    Path { path: String },
    /// Another Git repository with a `plugin.json` at its root. A pinned
    /// ref, when present, is carried as `spec.explicit_ref`.
    Remote { spec: RepoSpec },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawManifest {
    schema_version: u32,
    name: String,
    #[serde(default)]
    description: String,
    plugins: Vec<RawEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawEntry {
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default, rename = "ref")]
    git_ref: Option<String>,
}

impl RepositoryManifest {
    /// Parses and validates a `repository.json`. `origin` is the repository
    /// the manifest was fetched from, used to reject self-references; pass
    /// `None` when validating outside that context (e.g. an author lint).
    pub fn parse(json: &str, origin: Option<&RepoSpec>) -> Result<Self, ManifestError> {
        if json.len() as u64 > REPOSITORY_MANIFEST_MAX_BYTES {
            return Err(ManifestError::TooLarge {
                bytes: json.len() as u64,
                max: REPOSITORY_MANIFEST_MAX_BYTES,
            });
        }

        let raw: RawManifest = serde_json::from_str(json).map_err(|e| ManifestError::Parse {
            reason: e.to_string(),
        })?;

        if raw.schema_version != REPOSITORY_SCHEMA_VERSION {
            return Err(ManifestError::InvalidSchemaVersion {
                version: raw.schema_version,
            });
        }

        if raw.name.trim().is_empty() {
            return Err(ManifestError::EmptyName);
        }

        if raw.plugins.len() > REPOSITORY_MANIFEST_MAX_ENTRIES {
            return Err(ManifestError::TooManyEntries {
                count: raw.plugins.len(),
                max: REPOSITORY_MANIFEST_MAX_ENTRIES,
            });
        }

        let mut entries = Vec::with_capacity(raw.plugins.len());
        let mut paths: Vec<String> = Vec::new();
        let mut remotes: Vec<RepoSpec> = Vec::new();

        for (index, entry) in raw.plugins.iter().enumerate() {
            match (&entry.path, &entry.url) {
                (Some(path), None) => {
                    if entry.git_ref.is_some() {
                        return Err(ManifestError::RefWithoutUrl { index });
                    }

                    let normalized = normalize_entry_path(path).map_err(|reason| {
                        ManifestError::InvalidPath {
                            index,
                            path: path.clone(),
                            reason,
                        }
                    })?;

                    let lowered = normalized.to_ascii_lowercase();
                    if paths.contains(&lowered) {
                        return Err(ManifestError::DuplicateEntry { value: normalized });
                    }
                    paths.push(lowered);

                    entries.push(RepositoryEntry::Path { path: normalized });
                }
                (None, Some(url)) => {
                    let spec = RepoSpec::parse(url, entry.git_ref.as_deref())
                        .map_err(|source| ManifestError::InvalidUrl { index, source })?;

                    if let Some(origin) = origin
                        && spec.is_same_repo(origin)
                    {
                        return Err(ManifestError::SelfReference { index });
                    }

                    if remotes.iter().any(|seen| seen.is_same_repo(&spec)) {
                        return Err(ManifestError::DuplicateEntry {
                            value: spec.human_id(),
                        });
                    }
                    remotes.push(spec.clone());

                    entries.push(RepositoryEntry::Remote { spec });
                }
                _ => return Err(ManifestError::AmbiguousEntry { index }),
            }
        }

        reject_nested_paths(&entries)?;

        Ok(Self {
            schema_version: raw.schema_version,
            name: raw.name,
            description: raw.description,
            entries,
        })
    }
}

fn normalize_entry_path(path: &str) -> Result<String, String> {
    if path.is_empty() {
        return Err("must not be empty".into());
    }
    if path.contains('\\') {
        return Err("must use forward slashes".into());
    }
    if path.starts_with('/') {
        return Err("must be relative".into());
    }

    let segments: Vec<&str> = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();

    if segments.is_empty() {
        return Err("must name a directory".into());
    }
    for segment in &segments {
        if *segment == "." || *segment == ".." {
            return Err("must not contain '.' or '..' segments".into());
        }
        if segment.contains(':') {
            return Err("must not contain ':'".into());
        }
        if segment.chars().any(char::is_control) {
            return Err("must not contain control characters".into());
        }
    }

    Ok(segments.join("/"))
}

/// A plugin directory may not live inside another entry's directory; ids
/// would collide with files of the enclosing plugin and installs would
/// copy one plugin into another.
fn reject_nested_paths(entries: &[RepositoryEntry]) -> Result<(), ManifestError> {
    let paths: Vec<&String> = entries
        .iter()
        .filter_map(|entry| match entry {
            RepositoryEntry::Path { path } => Some(path),
            RepositoryEntry::Remote { .. } => None,
        })
        .collect();

    for parent in &paths {
        for child in &paths {
            if parent != child && child.starts_with(&format!("{parent}/")) {
                return Err(ManifestError::NestedPath {
                    parent: (*parent).clone(),
                    child: (*child).clone(),
                });
            }
        }
    }

    Ok(())
}

pub const SOURCE_RECORD_FILENAME: &str = ".harmony-source.json";
pub const SOURCE_RECORD_SCHEMA_VERSION: u32 = 1;

/// Provenance of an installed plugin, written next to its `plugin.json`.
/// Plugins without a record are local (bundled or hand-copied) and are
/// not managed by repository installs or updates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceRecord {
    pub schema_version: u32,
    /// Canonical URL of the repository the plugin was installed from.
    pub origin: String,
    pub forge: Forge,
    /// Ref the install tracks. `None` follows the default branch. Branch
    /// refs track new commits; tag and SHA refs are pinned.
    pub git_ref: Option<String>,
    /// Commit the installed tree came from, when the forge API was
    /// reachable at install time.
    pub commit: Option<String>,
    /// Directory within `origin` for plugins installed through a
    /// multi-plugin repository's path entry.
    pub subpath: Option<String>,
    /// Canonical URL of the repository whose `repository.json` referenced
    /// this plugin, when it differs from `origin`.
    pub via_repository: Option<String>,
    /// RFC 3339 timestamp supplied by the host.
    pub installed_at: Option<String>,
}

impl SourceRecord {
    /// Human-readable source location: the origin, plus the subpath for
    /// plugins installed from a repository index's path entry.
    pub fn location(&self) -> String {
        match &self.subpath {
            Some(subpath) => format!("{}/{subpath}", self.origin),
            None => self.origin.clone(),
        }
    }

    /// Reads the record from a plugin directory. `Ok(None)` means the
    /// plugin is local.
    pub fn load(plugin_dir: &Path) -> Result<Option<Self>, std::io::Error> {
        let path = plugin_dir.join(SOURCE_RECORD_FILENAME);
        let json = match std::fs::read_to_string(&path) {
            Ok(json) => json,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        serde_json::from_str(&json)
            .map(Some)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    pub fn store(&self, plugin_dir: &Path) -> Result<(), std::io::Error> {
        let json = serde_json::to_string_pretty(self).expect("source record serializes");
        std::fs::write(plugin_dir.join(SOURCE_RECORD_FILENAME), json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn origin() -> RepoSpec {
        RepoSpec::parse("https://github.com/lyra/plugins", None).unwrap()
    }

    #[test]
    fn parses_mixed_entries() {
        let manifest = RepositoryManifest::parse(
            r#"{
                "schema_version": 1,
                "name": "Lyra Plugins",
                "description": "First-party plugins",
                "plugins": [
                    { "path": "musicbrainz" },
                    { "path": "metadata/theaudiodb/" },
                    { "url": "https://codeberg.org/someone/lyra-foo" },
                    { "url": "https://github.com/someone/lyra-bar", "ref": "v2" }
                ]
            }"#,
            Some(&origin()),
        )
        .unwrap();

        assert_eq!(manifest.name, "Lyra Plugins");
        assert_eq!(manifest.entries.len(), 4);

        let RepositoryEntry::Path { path } = &manifest.entries[1] else {
            panic!("expected path entry");
        };
        assert_eq!(path, "metadata/theaudiodb");

        let RepositoryEntry::Remote { spec } = &manifest.entries[3] else {
            panic!("expected remote entry");
        };
        assert_eq!(spec.explicit_ref.as_deref(), Some("v2"));
    }

    #[test]
    fn allows_empty_plugin_lists() {
        let manifest = RepositoryManifest::parse(
            r#"{ "schema_version": 1, "name": "Empty", "plugins": [] }"#,
            None,
        )
        .unwrap();
        assert!(manifest.entries.is_empty());
    }

    #[test]
    fn rejects_unknown_fields() {
        let err = RepositoryManifest::parse(
            r#"{ "schema_version": 1, "name": "X", "plugins": [], "extra": true }"#,
            None,
        )
        .unwrap_err();
        assert!(matches!(err, ManifestError::Parse { .. }), "{err}");
    }

    #[test]
    fn rejects_wrong_schema_version() {
        let err = RepositoryManifest::parse(
            r#"{ "schema_version": 2, "name": "X", "plugins": [] }"#,
            None,
        )
        .unwrap_err();
        assert!(
            matches!(err, ManifestError::InvalidSchemaVersion { version: 2 }),
            "{err}"
        );
    }

    #[test]
    fn rejects_blank_names() {
        let err = RepositoryManifest::parse(
            r#"{ "schema_version": 1, "name": " ", "plugins": [] }"#,
            None,
        )
        .unwrap_err();
        assert!(matches!(err, ManifestError::EmptyName), "{err}");
    }

    #[test]
    fn rejects_oversized_manifests() {
        let padding = "x".repeat(REPOSITORY_MANIFEST_MAX_BYTES as usize);
        let json = format!(r#"{{ "schema_version": 1, "name": "{padding}", "plugins": [] }}"#);
        let err = RepositoryManifest::parse(&json, None).unwrap_err();
        assert!(matches!(err, ManifestError::TooLarge { .. }), "{err}");
    }

    #[test]
    fn rejects_too_many_entries() {
        let entries: Vec<String> = (0..=REPOSITORY_MANIFEST_MAX_ENTRIES)
            .map(|i| format!(r#"{{ "path": "plugin{i}" }}"#))
            .collect();
        let json = format!(
            r#"{{ "schema_version": 1, "name": "X", "plugins": [{}] }}"#,
            entries.join(",")
        );
        let err = RepositoryManifest::parse(&json, None).unwrap_err();
        assert!(matches!(err, ManifestError::TooManyEntries { .. }), "{err}");
    }

    #[test]
    fn rejects_entries_with_both_or_neither_target() {
        for entry in [
            r#"{ "path": "a", "url": "https://github.com/o/r" }"#,
            r#"{}"#,
        ] {
            let json = format!(r#"{{ "schema_version": 1, "name": "X", "plugins": [{entry}] }}"#);
            let err = RepositoryManifest::parse(&json, None).unwrap_err();
            assert!(
                matches!(err, ManifestError::AmbiguousEntry { index: 0 }),
                "{err}"
            );
        }
    }

    #[test]
    fn rejects_ref_on_path_entries() {
        let err = RepositoryManifest::parse(
            r#"{ "schema_version": 1, "name": "X", "plugins": [{ "path": "a", "ref": "v1" }] }"#,
            None,
        )
        .unwrap_err();
        assert!(
            matches!(err, ManifestError::RefWithoutUrl { index: 0 }),
            "{err}"
        );
    }

    #[test]
    fn rejects_traversal_paths() {
        for path in [
            "../escape",
            "a/../../b",
            "/absolute",
            ".",
            "a/./b",
            "c:evil",
        ] {
            let json = format!(
                r#"{{ "schema_version": 1, "name": "X", "plugins": [{{ "path": "{path}" }}] }}"#
            );
            let err = RepositoryManifest::parse(&json, None).unwrap_err();
            assert!(
                matches!(err, ManifestError::InvalidPath { .. }),
                "path {path:?}: {err}"
            );
        }
    }

    #[test]
    fn rejects_duplicate_paths_ignoring_case() {
        let err = RepositoryManifest::parse(
            r#"{
                "schema_version": 1,
                "name": "X",
                "plugins": [{ "path": "Foo" }, { "path": "foo/" }]
            }"#,
            None,
        )
        .unwrap_err();
        assert!(matches!(err, ManifestError::DuplicateEntry { .. }), "{err}");
    }

    #[test]
    fn rejects_nested_paths() {
        let err = RepositoryManifest::parse(
            r#"{
                "schema_version": 1,
                "name": "X",
                "plugins": [{ "path": "plugins" }, { "path": "plugins/foo" }]
            }"#,
            None,
        )
        .unwrap_err();
        assert!(matches!(err, ManifestError::NestedPath { .. }), "{err}");
    }

    #[test]
    fn rejects_duplicate_remotes() {
        let err = RepositoryManifest::parse(
            r#"{
                "schema_version": 1,
                "name": "X",
                "plugins": [
                    { "url": "https://github.com/o/r" },
                    { "url": "https://github.com/O/R.git", "ref": "v1" }
                ]
            }"#,
            None,
        )
        .unwrap_err();
        assert!(matches!(err, ManifestError::DuplicateEntry { .. }), "{err}");
    }

    #[test]
    fn rejects_self_references() {
        let err = RepositoryManifest::parse(
            r#"{
                "schema_version": 1,
                "name": "X",
                "plugins": [{ "url": "https://github.com/Lyra/Plugins" }]
            }"#,
            Some(&origin()),
        )
        .unwrap_err();
        assert!(
            matches!(err, ManifestError::SelfReference { index: 0 }),
            "{err}"
        );
    }

    #[test]
    fn rejects_invalid_remote_urls() {
        let err = RepositoryManifest::parse(
            r#"{ "schema_version": 1, "name": "X", "plugins": [{ "url": "ssh://git@github.com/o/r" }] }"#,
            None,
        )
        .unwrap_err();
        assert!(
            matches!(err, ManifestError::InvalidUrl { index: 0, .. }),
            "{err}"
        );
    }

    #[test]
    fn source_records_round_trip() {
        let dir = tempfile::TempDir::new().unwrap();
        let record = SourceRecord {
            schema_version: SOURCE_RECORD_SCHEMA_VERSION,
            origin: "https://github.com/o/r".into(),
            forge: Forge::GitHub,
            git_ref: Some("main".into()),
            commit: Some("a".repeat(40)),
            subpath: Some("musicbrainz".into()),
            via_repository: Some("https://github.com/lyra/plugins".into()),
            installed_at: Some("2026-06-09T12:00:00Z".into()),
        };

        record.store(dir.path()).unwrap();
        let loaded = SourceRecord::load(dir.path()).unwrap();
        assert_eq!(loaded.as_ref(), Some(&record));
    }

    #[test]
    fn missing_source_records_load_as_local() {
        let dir = tempfile::TempDir::new().unwrap();
        assert_eq!(SourceRecord::load(dir.path()).unwrap(), None);
    }
}
