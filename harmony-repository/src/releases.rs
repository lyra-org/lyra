// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

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
    #[error("no release runs on lyra {host}; the newest needs lyra {requirement}")]
    Incompatible {
        host: Version,
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
    /// Host versions the release is offered to.
    pub(crate) lyra: VersionReq,
}

/// A repository's validated `versions.json`: its releases, newest first.
#[derive(Debug)]
pub(crate) struct ReleaseIndex {
    releases: Vec<Release>,
}

impl ReleaseIndex {
    pub(crate) fn parse(bytes: &[u8]) -> Result<Self, ReleaseError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Entry {
            commit: String,
            lyra: String,
        }

        let parse_error = |reason: String| ReleaseError::Parse { reason };
        let entries: Vec<Entry> =
            serde_json::from_slice(bytes).map_err(|error| parse_error(error.to_string()))?;
        let mut releases: Vec<Release> = Vec::with_capacity(entries.len());
        for (index, entry) in entries.into_iter().enumerate() {
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
            let lyra = VersionReq::parse(&entry.lyra).map_err(|error| {
                parse_error(format!(
                    "release #{index} has invalid lyra version requirement {:?}: {error}",
                    entry.lyra
                ))
            })?;
            releases.push(Release { commit, lyra });
        }
        Ok(Self { releases })
    }

    /// The first, so newest, release whose `lyra` requirement matches
    /// `host`.
    pub(crate) fn select(&self, host: &Version) -> Result<&Release, ReleaseError> {
        if let Some(release) = self
            .releases
            .iter()
            .find(|release| release.lyra.matches(host))
        {
            return Ok(release);
        }
        let newest = self.releases.first().ok_or(ReleaseError::Empty)?;
        Err(ReleaseError::Incompatible {
            host: host.clone(),
            requirement: newest.lyra.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index(json: &str) -> Result<ReleaseIndex, ReleaseError> {
        ReleaseIndex::parse(json.as_bytes())
    }

    fn entry(commit: char, lyra: &str) -> String {
        format!(
            r#"{{ "commit": "{}", "lyra": "{lyra}" }}"#,
            commit.to_string().repeat(40)
        )
    }

    fn selected(index: &ReleaseIndex, host: &str) -> Result<String, ReleaseError> {
        index
            .select(&Version::parse(host).unwrap())
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
                r#"[{ "commit": "abc", "lyra": "0.0.1" }]"#.to_string(),
                "not a full 40-character commit id",
            ),
            (
                format!(r#"[{{ "commit": "{}", "lyra": "0.0.1" }}]"#, "g".repeat(40)),
                "not a full 40-character commit id",
            ),
            (
                format!(r#"[{{ "commit": "{}", "lyra": "0.0.1" }}]"#, "a".repeat(64)),
                "not a full 40-character commit id",
            ),
            (
                format!(
                    r#"[{{ "commit": "{}", "lyra": "latest" }}]"#,
                    "a".repeat(40)
                ),
                "invalid lyra version requirement \"latest\"",
            ),
            (
                format!(
                    r#"[{{ "commit": "{}", "lyra": "0.0.1", "version": "1.0.0" }}]"#,
                    "a".repeat(40)
                ),
                "unknown field `version`",
            ),
            (
                format!(r#"[{{ "commit": "{}" }}]"#, "a".repeat(40)),
                "missing field `lyra`",
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
            ReleaseIndex::parse(&[0xff]),
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
            "no release runs on lyra 0.5.0; the newest needs lyra ^0.3"
        );
        assert!(matches!(
            index("[]").unwrap().select(&Version::new(0, 1, 0)),
            Err(ReleaseError::Empty)
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
