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
    pub raw_tags: Vec<lyra_metadata::RawTrackTags>,
    #[serde(default)]
    pub expect: Expectations,
}

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
