// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::BTreeMap;

use lyra_server::testing::{
    EntitySnapshot,
    FixtureSnapshot,
};
use serde::{
    Deserialize,
    Deserializer,
    Serialize,
    Serializer,
    de,
    ser,
};

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Expectations {
    #[serde(default)]
    pub release: Option<ExpectedEntity>,
    #[serde(default)]
    pub artists: BTreeMap<String, ExpectedEntity>,
    #[serde(default)]
    pub tracks: BTreeMap<String, ExpectedEntity>,
}

impl Expectations {
    pub fn is_empty(&self) -> bool {
        self.release.is_none() && self.artists.is_empty() && self.tracks.is_empty()
    }

    /// Appends one failure per expectation the final DB state does not meet.
    pub fn check(&self, snapshot: &FixtureSnapshot, failures: &mut Vec<String>) {
        if let Some(expected_release) = &self.release {
            match snapshot.release.as_ref() {
                Some(actual) => {
                    compare_expected_entity("release", expected_release, actual, failures)
                }
                None => failures.push("  release: entity missing from final DB state".to_string()),
            }
        }
        compare_expected_entities("artist", &self.artists, &snapshot.artists, failures);
        compare_expected_entities("track", &self.tracks, &snapshot.tracks, failures);
    }

    /// The expectations that the final DB state would satisfy, for `--record`.
    pub fn capture(snapshot: &FixtureSnapshot) -> Self {
        let keyed = |entities: &[EntitySnapshot]| {
            entities
                .iter()
                .filter_map(|entity| {
                    let (_, ext_id) = entity.external_ids.first_key_value()?;
                    Some((ext_id.clone(), captured_entity(entity)))
                })
                .collect()
        };
        Self {
            release: snapshot.release.as_ref().map(captured_entity),
            artists: keyed(&snapshot.artists),
            tracks: keyed(&snapshot.tracks),
        }
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

fn resolve_entity_by_id_values<'a>(
    entities: &'a [EntitySnapshot],
    id_type: &str,
    accepted: &AcceptedValues,
) -> Option<&'a EntitySnapshot> {
    entities.iter().find(|entity| {
        entity
            .external_ids
            .get(id_type)
            .is_some_and(|value| accepted.contains(value))
    })
}

fn compare_expected_entity(
    label: &str,
    expected: &ExpectedEntity,
    actual: &EntitySnapshot,
    failures: &mut Vec<String>,
) {
    for (id_type, accepted) in &expected.ids {
        match actual.external_ids.get(id_type) {
            None => failures.push(format!(
                "  {label}: id '{id_type}' missing\n    expected: {}",
                accepted.display()
            )),
            Some(actual_value) if !accepted.contains(actual_value) => failures.push(format!(
                "  {label}: id '{id_type}' mismatch\n    expected: {}\n    actual:   {actual_value}",
                accepted.display()
            )),
            _ => {}
        }
    }

    for (field_name, expected_value) in &expected.fields {
        let expected_json = toml_to_json(expected_value);
        match actual.fields.get(field_name) {
            None => failures.push(format!(
                "  {label}: field '{field_name}' missing\n    expected: {expected_value}"
            )),
            Some(actual_value) if *actual_value != expected_json => {
                let actual_toml = json_to_toml(actual_value);
                failures.push(format!(
                    "  {label}: field '{field_name}' mismatch\n    expected: {expected_value}\n    actual:   {actual_toml}"
                ));
            }
            _ => {}
        }
    }

    for (artist_ext_id, expected_credit) in &expected.credits {
        let matching = actual.credits.iter().find(|c| {
            c.artist_id == *artist_ext_id
                && expected_credit
                    .credit_type
                    .as_ref()
                    .is_none_or(|ct| ct == &c.credit_type)
                && expected_credit.detail == c.detail
        });
        if matching.is_none() {
            let mut desc = format!("  {label}: credit for artist '{artist_ext_id}' missing");
            if let Some(ct) = &expected_credit.credit_type {
                desc.push_str(&format!("\n    expected credit_type: {ct}"));
            }
            if let Some(d) = &expected_credit.detail {
                desc.push_str(&format!("\n    expected detail: {d}"));
            }
            failures.push(desc);
        }
    }
}

fn compare_expected_entities(
    label_prefix: &str,
    expected_map: &BTreeMap<String, ExpectedEntity>,
    actual_list: &[EntitySnapshot],
    failures: &mut Vec<String>,
) {
    for (ext_id_value, expected) in expected_map {
        let resolved = resolve_entity_by_any_id(actual_list, ext_id_value, expected);

        match resolved {
            Some(actual) => {
                compare_expected_entity(
                    &format!("{label_prefix} {ext_id_value}"),
                    expected,
                    actual,
                    failures,
                );
            }
            None => failures.push(format!(
                "  {label_prefix} {ext_id_value}: entity missing from final DB state"
            )),
        }
    }
}

fn resolve_entity_by_any_id<'a>(
    entities: &'a [EntitySnapshot],
    ext_id_value: &str,
    expected: &ExpectedEntity,
) -> Option<&'a EntitySnapshot> {
    // If the expected entity specifies ID types, match on those.
    for (id_type, accepted) in &expected.ids {
        if let Some(entity) = resolve_entity_by_id_values(entities, id_type, accepted) {
            return Some(entity);
        }
    }
    // Otherwise, match the key against any external ID value on any entity.
    entities
        .iter()
        .find(|entity| entity.external_ids.values().any(|v| v == ext_id_value))
}

fn captured_entity(entity: &EntitySnapshot) -> ExpectedEntity {
    let ids = entity
        .external_ids
        .iter()
        .map(|(key, value)| (key.clone(), AcceptedValues::Single(value.clone())))
        .collect();
    let fields = entity
        .fields
        .iter()
        .map(|(key, value)| (key.clone(), json_to_toml(value)))
        .collect();
    ExpectedEntity {
        ids,
        fields,
        credits: BTreeMap::new(),
    }
}

pub(crate) fn json_to_toml(value: &serde_json::Value) -> toml::Value {
    match value {
        serde_json::Value::Null => toml::Value::String("null".to_string()),
        serde_json::Value::Bool(value) => toml::Value::Boolean(*value),
        serde_json::Value::Number(value) => {
            if let Some(integer) = value.as_i64() {
                toml::Value::Integer(integer)
            } else if let Some(float) = value.as_f64() {
                toml::Value::Float(float)
            } else {
                toml::Value::String(value.to_string())
            }
        }
        serde_json::Value::String(value) => toml::Value::String(value.clone()),
        serde_json::Value::Array(values) => {
            toml::Value::Array(values.iter().map(json_to_toml).collect())
        }
        serde_json::Value::Object(values) => {
            let mut table = toml::map::Map::new();
            for (key, value) in values {
                table.insert(key.clone(), json_to_toml(value));
            }
            toml::Value::Table(table)
        }
    }
}

fn toml_to_json(value: &toml::Value) -> serde_json::Value {
    match value {
        toml::Value::String(value) if value == "null" => serde_json::Value::Null,
        toml::Value::String(value) => serde_json::Value::String(value.clone()),
        toml::Value::Integer(value) => serde_json::Value::Number((*value).into()),
        toml::Value::Float(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        toml::Value::Boolean(value) => serde_json::Value::Bool(*value),
        toml::Value::Datetime(value) => serde_json::Value::String(value.to_string()),
        toml::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(toml_to_json).collect())
        }
        toml::Value::Table(values) => {
            let mut object = serde_json::Map::new();
            for (key, value) in values {
                object.insert(key.clone(), toml_to_json(value));
            }
            serde_json::Value::Object(object)
        }
    }
}
