// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::BTreeMap;

use serde::{
    Deserialize,
    Deserializer,
    Serialize,
    Serializer,
    de,
    ser,
};

#[derive(Debug, Deserialize)]
pub struct TestCase {
    pub plugin: String,
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
    pub expect: ExpectedExpectations,
}

impl TestCase {
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

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ExpectedExpectations {
    #[serde(default)]
    pub release: Option<ExpectedEntity>,
    #[serde(default)]
    pub artists: BTreeMap<String, ExpectedEntity>,
    #[serde(default)]
    pub tracks: BTreeMap<String, ExpectedEntity>,
}

impl ExpectedExpectations {
    pub fn is_empty(&self) -> bool {
        self.release.is_none() && self.artists.is_empty() && self.tracks.is_empty()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ExpectedEntity {
    #[serde(default)]
    pub ids: BTreeMap<String, AcceptedValues>,
    #[serde(default)]
    pub fields: BTreeMap<String, toml::Value>,
    #[serde(default)]
    pub credits: BTreeMap<String, ExpectedCredit>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ExpectedCredit {
    pub credit_type: Option<String>,
    pub detail: Option<String>,
}

/// A value that accepts either a single string or multiple alternatives.
/// Deserializes from `"value"` or `["value1", "value2"]`.
/// Serializes back to a single string when there's only one value.
#[derive(Debug, Clone)]
pub enum AcceptedValues {
    Single(String),
    Multiple(Vec<String>),
}

impl AcceptedValues {
    pub fn contains(&self, value: &str) -> bool {
        match self {
            AcceptedValues::Single(s) => s == value,
            AcceptedValues::Multiple(v) => v.iter().any(|s| s == value),
        }
    }

    pub fn display(&self) -> String {
        match self {
            AcceptedValues::Single(s) => s.clone(),
            AcceptedValues::Multiple(v) => {
                let mut out = String::new();
                for (i, s) in v.iter().enumerate() {
                    if i > 0 {
                        out.push_str("\n              ");
                    }
                    out.push_str(s);
                }
                out
            }
        }
    }
}

impl<'de> Deserialize<'de> for AcceptedValues {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct AcceptedValuesVisitor;

        impl<'de> de::Visitor<'de> for AcceptedValuesVisitor {
            type Value = AcceptedValues;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string or array of strings")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(AcceptedValues::Single(v.to_string()))
            }

            fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(v) = seq.next_element::<String>()? {
                    values.push(v);
                }
                Ok(AcceptedValues::Multiple(values))
            }
        }

        deserializer.deserialize_any(AcceptedValuesVisitor)
    }
}

impl Serialize for AcceptedValues {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            AcceptedValues::Single(s) => serializer.serialize_str(s),
            AcceptedValues::Multiple(v) => {
                use ser::SerializeSeq;
                let mut seq = serializer.serialize_seq(Some(v.len()))?;
                for s in v {
                    seq.serialize_element(s)?;
                }
                seq.end()
            }
        }
    }
}
