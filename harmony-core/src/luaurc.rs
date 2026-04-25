// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value;
use std::collections::{
    BTreeMap,
    BTreeSet,
};

/// Parsed `.luaurc` contents.
///
/// The config is read and written as JSON5, but rewriting it still normalizes formatting and drops
/// comments and other source-level details from the original file.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct LuaurcConfig {
    #[serde(default)]
    pub aliases: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub globals: BTreeSet<String>,
    #[serde(flatten)]
    pub other: BTreeMap<String, Value>,
}

impl LuaurcConfig {
    pub fn from_json5_str(text: &str) -> Result<Self, json5::Error> {
        json5::from_str(text)
    }

    pub fn from_slice(bytes: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(bytes).ok()?;
        Self::from_json5_str(text).ok()
    }

    pub fn to_pretty_json5_string(&self) -> Result<String, json5::Error> {
        json5::to_string_with_options(self, luaurc_serializer_options())
    }

    pub fn to_json5_bytes(&self) -> Result<Vec<u8>, json5::Error> {
        let mut output = Vec::new();
        json5::to_writer_with_options(&mut output, self, luaurc_serializer_options())?;
        Ok(output)
    }

    pub fn from_aliases<I>(aliases: I) -> Self
    where
        I: IntoIterator<Item = (String, String)>,
    {
        Self {
            aliases: aliases.into_iter().collect(),
            globals: BTreeSet::new(),
            other: BTreeMap::new(),
        }
    }

    pub fn insert_alias(&mut self, alias: impl Into<String>, path: impl Into<String>) {
        self.aliases.insert(alias.into(), path.into());
    }

    pub fn merge_missing_aliases<I>(&mut self, aliases: I)
    where
        I: IntoIterator<Item = (String, String)>,
    {
        for (alias, path) in aliases {
            self.aliases.entry(alias).or_insert(path);
        }
    }

    pub fn insert_global(&mut self, global: impl Into<String>) {
        self.globals.insert(global.into());
    }

    pub fn remove_global(&mut self, global: &str) {
        self.globals.remove(global);
    }

    pub fn merge_globals<I, S>(&mut self, globals: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for global in globals {
            self.insert_global(global);
        }
    }
}

fn luaurc_serializer_options() -> json5::SerializerOptions {
    json5::SerializerOptions::new().property_name_style(json5::PropertyNameStyle::DoubleQuoted)
}
