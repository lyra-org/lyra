// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use serde::Deserialize;

use crate::expect::Expectations;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fixture {
    #[serde(default)]
    pub run: RunMode,
    pub library: LibraryConfig,
    /// `None` skips the version check; `Some` must match
    /// [`lyra_metadata::DEFAULT_MAPPING_VERSION`] or the runner fails
    /// rather than compare against drifted extraction rules.
    #[serde(default)]
    pub mapping_version: Option<u64>,
    pub raw_tags: Vec<lyra_metadata::RawTrackTags>,
    #[serde(default)]
    pub expect: Expectations,
}

impl Fixture {
    pub fn check_mapping_version(&self) -> Result<(), MappingVersionMismatch> {
        match self.mapping_version {
            None => Ok(()),
            Some(v) if v == lyra_metadata::DEFAULT_MAPPING_VERSION => Ok(()),
            Some(found) => Err(MappingVersionMismatch {
                expected: lyra_metadata::DEFAULT_MAPPING_VERSION,
                found,
            }),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MappingVersionMismatch {
    pub expected: u64,
    pub found: u64,
}

impl std::fmt::Display for MappingVersionMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "fixture mapping_version {} does not match server \
             DEFAULT_MAPPING_VERSION {}; regenerate the fixture",
            self.found, self.expected,
        )
    }
}

impl std::error::Error for MappingVersionMismatch {}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum RunMode {
    #[default]
    Refresh,
    Sync,
}

#[derive(Debug, Deserialize)]
pub struct LibraryConfig {
    pub directory: String,
    pub language: Option<String>,
    pub country: Option<String>,
}
