// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use harmony_core::plugin::PluginHost;
use semver::{
    Version,
    VersionReq,
};
use serde::Deserialize;

pub const RELEASE_INDEX_FILENAME: &str = "versions.json";

/// Why a repository offers no release this host can install.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ReleaseError {
    #[error("no releases: the repository has no {RELEASE_INDEX_FILENAME}")]
    Missing,
    #[error("failed to parse {RELEASE_INDEX_FILENAME}: {reason}")]
    Parse { reason: String },
    #[error("no releases: {RELEASE_INDEX_FILENAME} lists none")]
    Empty,
    #[error("no release runs on {host} {host_version}; the newest needs {host} {requirement}")]
    Incompatible {
        host: String,
        host_version: Version,
        requirement: VersionReq,
    },
    #[error(
        "{RELEASE_INDEX_FILENAME} lists commit {commit}, but the repository has no such commit or \
         no plugin at it"
    )]
    Unavailable { commit: String },
}

/// One entry of a repository's `versions.json`.
#[derive(Debug)]
pub(crate) struct Release {
    /// Full commit id, lowercased.
    pub(crate) commit: String,
    /// Host versions the release is offered to, read from the key the
    /// host names.
    pub(crate) host_requirement: VersionReq,
}

/// A repository's validated `versions.json`: its releases, newest first.
#[derive(Debug)]
pub(crate) struct ReleaseIndex {
    releases: Vec<Release>,
}

impl ReleaseIndex {
    /// Parses `versions.json`, whose entries hold a `commit` and the
    /// requirement on `host` under the key [`PluginHost::name`] names, and
    /// nothing else.
    pub(crate) fn parse(bytes: &[u8], host: &PluginHost) -> Result<Self, ReleaseError> {
        #[derive(Deserialize)]
        struct Entry {
            commit: String,
            #[serde(flatten)]
            other: serde_json::Map<String, serde_json::Value>,
        }

        let parse_error = |reason: String| ReleaseError::Parse { reason };
        let entries: Vec<Entry> =
            serde_json::from_slice(bytes).map_err(|error| parse_error(error.to_string()))?;
        let mut releases: Vec<Release> = Vec::with_capacity(entries.len());
        for (index, mut entry) in entries.into_iter().enumerate() {
            let name = host.name();
            let requirement = match entry.other.remove(name) {
                Some(serde_json::Value::String(requirement)) => requirement,
                Some(other) => {
                    return Err(parse_error(format!(
                        "release #{index} has {name} requirement {other}, which is not a string"
                    )));
                }
                None => {
                    return Err(parse_error(format!(
                        "release #{index} is missing field `{name}`"
                    )));
                }
            };
            if let Some(field) = entry.other.keys().next() {
                return Err(parse_error(format!(
                    "release #{index} has unknown field `{field}`, expected `commit` or `{name}`"
                )));
            }
            if entry.commit.len() != 40 || !entry.commit.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(parse_error(format!(
                    "release #{index} has commit {:?}, which is not a full 40-character commit id",
                    entry.commit
                )));
            }
            let commit = entry.commit.to_ascii_lowercase();
            if releases.iter().any(|release| release.commit == commit) {
                return Err(parse_error(format!(
                    "commit {commit} is listed more than once"
                )));
            }
            let host_requirement = VersionReq::parse(&requirement).map_err(|error| {
                parse_error(format!(
                    "release #{index} has invalid {name} version requirement {requirement:?}: \
                     {error}"
                ))
            })?;
            releases.push(Release {
                commit,
                host_requirement,
            });
        }
        Ok(Self { releases })
    }

    /// The first, so newest, release whose requirement on `host` matches
    /// its version.
    pub(crate) fn select(&self, host: &PluginHost) -> Result<&Release, ReleaseError> {
        if let Some(release) = self
            .releases
            .iter()
            .find(|release| release.host_requirement.matches(host.version()))
        {
            return Ok(release);
        }
        let newest = self.releases.first().ok_or(ReleaseError::Empty)?;
        Err(ReleaseError::Incompatible {
            host: host.name().to_string(),
            host_version: host.version().clone(),
            requirement: newest.host_requirement.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn host_at(name: &str, version: &str) -> PluginHost {
        PluginHost::new(name, Version::parse(version).unwrap(), Default::default()).unwrap()
    }

    fn index(json: &str) -> Result<ReleaseIndex, ReleaseError> {
        ReleaseIndex::parse(json.as_bytes(), &host_at("host", "0.0.1"))
    }

    fn entry(commit: char, requirement: &str) -> String {
        format!(
            r#"{{ "commit": "{}", "host": "{requirement}" }}"#,
            commit.to_string().repeat(40)
        )
    }

    fn selected(index: &ReleaseIndex, version: &str) -> Result<String, ReleaseError> {
        index
            .select(&host_at("host", version))
            .map(|release| release.commit.clone())
    }

    #[test]
    fn rejects_invalid_indexes() {
        let a = entry('a', "0.0.1");
        for (json, reason) in [
            (
                format!("[{a}, {}]", entry('A', "0.0.2")),
                "is listed more than once",
            ),
            (
                r#"[{ "commit": "abc", "host": "0.0.1" }]"#.to_string(),
                "not a full 40-character commit id",
            ),
            (
                format!(r#"[{{ "commit": "{}", "host": "0.0.1" }}]"#, "g".repeat(40)),
                "not a full 40-character commit id",
            ),
            (
                format!(r#"[{{ "commit": "{}", "host": "0.0.1" }}]"#, "a".repeat(64)),
                "not a full 40-character commit id",
            ),
            (
                format!(
                    r#"[{{ "commit": "{}", "host": "latest" }}]"#,
                    "a".repeat(40)
                ),
                "invalid host version requirement \"latest\"",
            ),
            (
                format!(
                    r#"[{{ "commit": "{}", "host": "0.0.1", "version": "1.0.0" }}]"#,
                    "a".repeat(40)
                ),
                "unknown field `version`",
            ),
            (
                format!(r#"[{{ "commit": "{}" }}]"#, "a".repeat(40)),
                "missing field `host`",
            ),
            (
                format!(r#"[{{ "commit": "{}", "host": 1 }}]"#, "a".repeat(40)),
                "has host requirement 1, which is not a string",
            ),
            (format!(r#"{{ "1.0.0": {a} }}"#), "expected a sequence"),
            (format!("[{a}] []"), "trailing characters"),
        ] {
            match index(&json) {
                Err(ReleaseError::Parse { reason: error }) => {
                    assert!(error.contains(reason), "{json}: {error}")
                }
                other => panic!("{json}: expected a parse error, got {other:?}"),
            }
        }
        assert!(matches!(
            ReleaseIndex::parse(&[0xff], &host_at("host", "0.0.1")),
            Err(ReleaseError::Parse { .. })
        ));
    }

    #[test]
    fn selects_the_first_compatible_release() {
        let parsed = index(&format!(
            "[{}, {}, {}]",
            entry('C', "0.3"),
            entry('b', ">=0.1, <0.3"),
            entry('a', "0.1"),
        ))
        .unwrap();

        assert_eq!(selected(&parsed, "0.3.1").unwrap(), "c".repeat(40));
        assert_eq!(selected(&parsed, "0.2.0").unwrap(), "b".repeat(40));
        assert_eq!(selected(&parsed, "0.1.5").unwrap(), "b".repeat(40));
        let error = selected(&parsed, "0.5.0").unwrap_err();
        assert_eq!(
            error.to_string(),
            "no release runs on host 0.5.0; the newest needs host ^0.3"
        );
        assert!(matches!(
            index("[]").unwrap().select(&host_at("host", "0.1.0")),
            Err(ReleaseError::Empty)
        ));
    }

    #[test]
    fn reads_requirements_under_the_host_name() {
        let other = host_at("other", "2.0.0");
        let json = format!(r#"[{{ "commit": "{}", "other": "2" }}]"#, "a".repeat(40));
        let parsed = ReleaseIndex::parse(json.as_bytes(), &other).unwrap();

        assert_eq!(parsed.select(&other).unwrap().commit, "a".repeat(40));
        assert_eq!(
            parsed
                .select(&host_at("other", "3.0.0"))
                .unwrap_err()
                .to_string(),
            "no release runs on other 3.0.0; the newest needs other ^2"
        );
        assert!(matches!(
            index(&json),
            Err(ReleaseError::Parse { reason }) if reason.contains("missing field `host`")
        ));
    }

    #[test]
    fn pre_release_hosts_match_only_requirements_naming_their_pre_release() {
        let parsed = index(&format!(
            "[{}, {}]",
            entry('b', ">=0.0.6-dev"),
            entry('a', "*"),
        ))
        .unwrap();

        assert_eq!(selected(&parsed, "0.0.5").unwrap(), "a".repeat(40));
        assert_eq!(selected(&parsed, "0.0.6-dev").unwrap(), "b".repeat(40));
        assert!(matches!(
            selected(&parsed, "0.0.7-dev"),
            Err(ReleaseError::Incompatible { .. })
        ));
    }
}
