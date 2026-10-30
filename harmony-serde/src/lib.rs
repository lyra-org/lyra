// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::fmt;

use harmony_luau as luau;
use harmony_luau::{
    DescribeModule,
    DescribeTypeAlias,
    JsonValue,
    render_definition_file_with_support,
};

const EMPTY_OBJECT_MARKER: &str = "__harmony_serde_empty_object";

pub struct SerdeModule;

/// The capability hosts conventionally gate this module behind.
pub const SCOPE: &str = "harmony.serde";

#[harmony_macros::module(
    id = "harmony/serde",
    name = "Serde",
    local = "serde",
    description = "Serde-backed JSON, YAML, and XML encoding and decoding helpers."
)]
impl SerdeModule {
    #[harmony(
        name = "json.encode",
        description = "Encodes a Lua JSON-compatible value into a JSON string.",
        args(input: JsonValue)
    )]
    fn json_encode(input: luau::Value) -> luau::runtime::Result<String> {
        let json = luau_to_json(&input, 0)?;
        serde_json::to_string(&json).map_err(|error| runtime_error("JSON encode failed", error))
    }

    #[harmony(
        name = "json.decode",
        description = "Decodes a JSON string into a Lua JSON-compatible value.",
        returns(JsonValue)
    )]
    fn json_decode(vm: &luau::Vm, input: String) -> luau::runtime::Result<luau::Value> {
        let json: serde_json::Value = serde_json::from_str(&input)
            .map_err(|error| runtime_error("JSON decode failed", error))?;
        json_to_luau(vm, json, 0)
    }

    #[harmony(
        name = "yaml.encode",
        description = "Encodes a Lua JSON-compatible value into a YAML string.",
        args(input: JsonValue)
    )]
    fn yaml_encode(input: luau::Value) -> luau::runtime::Result<String> {
        let json = luau_to_json(&input, 0)?;
        serde_saphyr::to_string(&json).map_err(|error| runtime_error("YAML encode failed", error))
    }

    #[harmony(
        name = "yaml.decode",
        description = "Decodes a YAML string into a Lua JSON-compatible value.",
        returns(JsonValue)
    )]
    fn yaml_decode(vm: &luau::Vm, input: String) -> luau::runtime::Result<luau::Value> {
        let json: serde_json::Value = serde_saphyr::from_str(&input)
            .map_err(|error| runtime_error("YAML decode failed", error))?;
        json_to_luau(vm, json, 0)
    }

    #[harmony(
        name = "xml.encode",
        description = "Encodes a Lua JSON-compatible value into an XML string using the given root element name.",
        args(root: String, input: JsonValue)
    )]
    fn xml_encode(root: String, input: luau::Value) -> luau::runtime::Result<String> {
        let json = luau_to_json(&input, 0)?;
        quick_xml::se::to_string_with_root(&root, &json)
            .map_err(|error| runtime_error("XML encode failed", error))
    }

    #[harmony(
        name = "xml.decode",
        description = "Decodes an XML string into a Lua JSON-compatible value using quick-xml conventions.",
        returns(JsonValue)
    )]
    fn xml_decode(vm: &luau::Vm, input: String) -> luau::runtime::Result<luau::Value> {
        let json: serde_json::Value = quick_xml::de::from_str(&input)
            .map_err(|error| runtime_error("XML decode failed", error))?;
        json_to_luau(vm, json, 0)
    }

    #[harmony(
        name = "empty_object",
        description = "Returns a value that serializes as an empty object `{}`. Use this \
                       instead of `{}` when an object (not array) is required, since empty \
                       Lua tables serialize as arrays by default.",
        returns(JsonValue)
    )]
    fn empty_object(vm: &luau::Vm) -> luau::runtime::Result<luau::Table> {
        let table = vm.create_table()?;
        let metatable = vm.create_table()?;
        metatable.set(EMPTY_OBJECT_MARKER, luau::Value::Boolean(true))?;
        metatable.set("__metatable", luau::Value::Boolean(false))?;
        metatable.set_readonly(true)?;
        table.set_metatable(Some(&metatable))?;
        table.set_readonly(true)?;
        Ok(table)
    }
}

pub fn luau_to_json(value: &luau::Value, depth: usize) -> luau::runtime::Result<serde_json::Value> {
    if depth > 128 {
        return Err(luau::Error::Runtime(
            "JSON value is too deeply nested".into(),
        ));
    }

    match value {
        luau::Value::Nil => Ok(serde_json::Value::Null),
        luau::Value::Boolean(value) => Ok(serde_json::Value::Bool(*value)),
        luau::Value::Integer(value) => Ok(serde_json::Value::Number((*value).into())),
        luau::Value::Number(value) => json_number_from_f64(*value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| luau::Error::Runtime("JSON numbers must be finite".into())),
        luau::Value::String(value) => String::from_utf8(value.clone())
            .map(serde_json::Value::String)
            .map_err(|error| runtime_error("JSON strings must be valid UTF-8", error)),
        luau::Value::TableData(table) => owned_table_to_json(table, depth + 1),
        luau::Value::Table(table) => table_to_json(table, depth + 1),
        luau::Value::Buffer(_) => Err(luau::Error::Runtime(
            "buffers cannot be encoded as JSON".into(),
        )),
        luau::Value::NativeFunction(_) => Err(luau::Error::Runtime(
            "functions cannot be encoded as JSON".into(),
        )),
        luau::Value::Function(_) => Err(luau::Error::Runtime(
            "functions cannot be encoded as JSON".into(),
        )),
        luau::Value::Thread(_) => Err(luau::Error::Runtime(
            "threads cannot be encoded as JSON".into(),
        )),
        luau::Value::UserData(_) => Err(luau::Error::Runtime(
            "userdata cannot be encoded as JSON".into(),
        )),
    }
}

pub fn optional_string_field(
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Option<String>> {
    match table.get(key)? {
        luau::Value::String(value) => String::from_utf8(value)
            .map(Some)
            .map_err(|error| luau::Error::Runtime(format!("{key} must be valid UTF-8: {error}"))),
        luau::Value::Nil => Ok(None),
        other => Err(luau::Error::Runtime(format!(
            "{key} must be a string or nil, got {}",
            other.type_name()
        ))),
    }
}

pub fn optional_i64_field(table: &luau::Table, key: &str) -> luau::runtime::Result<Option<i64>> {
    match table.get(key)? {
        luau::Value::Integer(value) => Ok(Some(value)),
        luau::Value::Number(value)
            if value.is_finite()
                && value.fract() == 0.0
                && value >= i64::MIN as f64
                && value < 9_223_372_036_854_775_808.0 =>
        {
            Ok(Some(value as i64))
        }
        luau::Value::Number(_) => Err(luau::Error::Runtime(format!(
            "{key} must be an integer or nil"
        ))),
        luau::Value::Nil => Ok(None),
        other => Err(luau::Error::Runtime(format!(
            "{key} must be a number or nil, got {}",
            other.type_name()
        ))),
    }
}

pub fn optional_json_field(
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Option<serde_json::Value>> {
    let value = table.get(key)?;
    if matches!(value, luau::Value::Nil) {
        Ok(None)
    } else {
        luau_to_json(&value, 0).map(Some)
    }
}

pub fn optional_string_pairs_field(
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Vec<(String, String)>> {
    let pairs = match table.get(key)? {
        luau::Value::Table(pairs) => pairs,
        luau::Value::Nil => return Ok(Vec::new()),
        other => {
            return Err(luau::Error::Runtime(format!(
                "{key} must be a table or nil, got {}",
                other.type_name()
            )));
        }
    };
    let mut result = Vec::new();
    for (key, value) in pairs.pairs()? {
        let (luau::Value::String(key), luau::Value::String(value)) = (key, value) else {
            continue;
        };
        let key = String::from_utf8(key)
            .map_err(|error| runtime_error("table keys must be valid UTF-8", error))?;
        let value = String::from_utf8(value)
            .map_err(|error| runtime_error("table values must be valid UTF-8", error))?;
        result.push((key, value));
    }
    Ok(result)
}

fn json_number_from_f64(value: f64) -> Option<serde_json::Number> {
    if !value.is_finite() {
        return None;
    }
    if value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
        return Some(serde_json::Number::from(value as i64));
    }
    serde_json::Number::from_f64(value)
}

fn owned_table_to_json(
    table: &luau::OwnedTable,
    depth: usize,
) -> luau::runtime::Result<serde_json::Value> {
    if !table.array().is_empty() && !table.fields().is_empty() {
        return Err(luau::Error::Runtime(
            "JSON tables cannot mix array and object keys".into(),
        ));
    }

    if !table.array().is_empty() {
        return Ok(serde_json::Value::Array(
            table
                .array()
                .iter()
                .map(|value| luau_to_json(value, depth + 1))
                .collect::<luau::runtime::Result<Vec<_>>>()?,
        ));
    }

    let mut object = serde_json::Map::new();
    for (key, value) in table.fields() {
        object.insert(key.clone(), luau_to_json(value, depth + 1)?);
    }
    Ok(serde_json::Value::Object(object))
}

fn table_to_json(table: &luau::Table, depth: usize) -> luau::runtime::Result<serde_json::Value> {
    if table_is_empty_object_marker(table)? {
        return Ok(serde_json::Value::Object(serde_json::Map::new()));
    }

    let pairs = table.pairs()?;
    if pairs.is_empty() {
        return Ok(serde_json::Value::Array(Vec::new()));
    }

    enum TableShape {
        Unknown,
        Array(Vec<(usize, serde_json::Value)>),
        Object(serde_json::Map<String, serde_json::Value>),
    }

    let mut shape = TableShape::Unknown;
    for (key, value) in pairs {
        match (table_array_index(&key), key) {
            (Some(index), _) => {
                let json = luau_to_json(&value, depth)?;
                match &mut shape {
                    TableShape::Unknown => shape = TableShape::Array(vec![(index, json)]),
                    TableShape::Array(values) => values.push((index, json)),
                    TableShape::Object(_) => {
                        return Err(luau::Error::Runtime(
                            "JSON tables cannot mix array and object keys".into(),
                        ));
                    }
                }
            }
            (None, luau::Value::String(key)) => {
                let key = String::from_utf8(key).map_err(|error| {
                    runtime_error("JSON object keys must be valid UTF-8", error)
                })?;
                let json = luau_to_json(&value, depth)?;
                match &mut shape {
                    TableShape::Unknown => {
                        let mut object = serde_json::Map::new();
                        object.insert(key, json);
                        shape = TableShape::Object(object);
                    }
                    TableShape::Object(object) => {
                        object.insert(key, json);
                    }
                    TableShape::Array(_) => {
                        return Err(luau::Error::Runtime(
                            "JSON tables cannot mix array and object keys".into(),
                        ));
                    }
                }
            }
            (None, other) => {
                return Err(luau::Error::Runtime(format!(
                    "JSON object keys must be strings or positive integers, got {}",
                    other.type_name()
                )));
            }
        }
    }

    match shape {
        TableShape::Unknown => Ok(serde_json::Value::Array(Vec::new())),
        TableShape::Object(object) => Ok(serde_json::Value::Object(object)),
        TableShape::Array(mut entries) => {
            entries.sort_by_key(|(index, _)| *index);
            if entries
                .iter()
                .enumerate()
                .any(|(offset, (index, _))| *index != offset + 1)
            {
                return Err(luau::Error::Runtime(
                    "JSON array tables must use contiguous 1-based integer keys".into(),
                ));
            }
            Ok(serde_json::Value::Array(
                entries.into_iter().map(|(_, value)| value).collect(),
            ))
        }
    }
}

fn table_array_index(key: &luau::Value) -> Option<usize> {
    match key {
        luau::Value::Integer(value) if *value > 0 => Some(*value as usize),
        luau::Value::Number(value) if value.is_finite() && *value > 0.0 && value.fract() == 0.0 => {
            Some(*value as usize)
        }
        _ => None,
    }
}

fn table_is_empty_object_marker(table: &luau::Table) -> luau::runtime::Result<bool> {
    let Some(metatable) = table.metatable()? else {
        return Ok(false);
    };
    Ok(matches!(
        metatable.get(EMPTY_OBJECT_MARKER)?,
        luau::Value::Boolean(true)
    ))
}

pub fn json_to_luau(
    vm: &luau::Vm,
    value: serde_json::Value,
    depth: usize,
) -> luau::runtime::Result<luau::Value> {
    if depth > 128 {
        return Err(luau::Error::Runtime(
            "JSON value is too deeply nested".into(),
        ));
    }

    match value {
        serde_json::Value::Null => Ok(luau::Value::Nil),
        serde_json::Value::Bool(value) => Ok(luau::Value::Boolean(value)),
        serde_json::Value::Number(value) => json_number_to_luau(value),
        serde_json::Value::String(value) => Ok(luau::Value::String(value.into_bytes())),
        serde_json::Value::Array(values) => {
            let table = vm.create_table_with_capacity(values.len() as i32, 0)?;
            for (index, value) in values.into_iter().enumerate() {
                table.set_integer(index as i32 + 1, json_to_luau(vm, value, depth + 1)?)?;
            }
            Ok(luau::Value::Table(table))
        }
        serde_json::Value::Object(values) => {
            let table = vm.create_table_with_capacity(0, values.len() as i32)?;
            for (key, value) in values {
                table.set(&key, json_to_luau(vm, value, depth + 1)?)?;
            }
            Ok(luau::Value::Table(table))
        }
    }
}

pub fn json_to_luau_owned(
    value: serde_json::Value,
    depth: usize,
) -> luau::runtime::Result<luau::Value> {
    if depth > 128 {
        return Err(luau::Error::Runtime(
            "JSON value is too deeply nested".into(),
        ));
    }

    match value {
        serde_json::Value::Null => Ok(luau::Value::Nil),
        serde_json::Value::Bool(value) => Ok(luau::Value::Boolean(value)),
        serde_json::Value::Number(value) => json_number_to_luau(value),
        serde_json::Value::String(value) => Ok(luau::Value::String(value.into_bytes())),
        serde_json::Value::Array(values) => {
            let mut table = luau::OwnedTable::with_capacity(values.len(), 0);
            for value in values {
                table.push_array(json_to_luau_owned(value, depth + 1)?);
            }
            Ok(luau::Value::TableData(table))
        }
        serde_json::Value::Object(values) => {
            let mut table = luau::OwnedTable::with_capacity(0, values.len());
            for (key, value) in values {
                table.set_field(key, json_to_luau_owned(value, depth + 1)?);
            }
            Ok(luau::Value::TableData(table))
        }
    }
}

fn json_number_to_luau(value: serde_json::Number) -> luau::runtime::Result<luau::Value> {
    value
        .as_f64()
        .filter(|value| value.is_finite())
        .map(luau::Value::Number)
        .ok_or_else(|| luau::Error::Runtime("JSON number is out of range".into()))
}

fn runtime_error(context: &str, error: impl fmt::Display) -> luau::Error {
    luau::Error::Runtime(format!("{context}: {error}"))
}

pub fn render_luau_definition() -> std::result::Result<String, fmt::Error> {
    render_definition_file_with_support(
        &SerdeModule::module_descriptor(),
        &[JsonValue::type_alias_descriptor()],
        &[],
        &[],
    )
}

#[cfg(test)]
mod tests {
    use super::SerdeModule;

    #[test]
    fn luau_module_encodes_and_decodes_json_values() -> harmony_luau::runtime::Result<()> {
        let vm = harmony_luau::Vm::new()?;
        let spec = SerdeModule::module_spec();
        let origin = harmony_core::ChunkOrigin {
            plugin: Some(std::sync::Arc::from("demo")),
            path: Some(std::sync::Arc::from("plugins/demo/init.luau")),
            ..harmony_core::ChunkOrigin::default()
        };
        let table = harmony_core::luau::install_module(&vm, &origin, &spec)?;
        vm.set_global_table("serde", &table)?;

        let encoded = vm.eval(
            std::sync::Arc::<[u8]>::from(
                &br#"
                return serde.json.encode({
                    name = "Lyra",
                    count = 2,
                    tags = { "server", "music" },
                    active = true,
                })
                "#[..],
            ),
            harmony_luau::ChunkOrigin::default(),
        )?;
        let [harmony_luau::Value::String(encoded)] = encoded.as_slice() else {
            panic!("json.encode should return a string");
        };
        let parsed: serde_json::Value =
            serde_json::from_slice(encoded).expect("encoded JSON should parse");
        assert_eq!(parsed["name"], "Lyra");
        assert_eq!(parsed["count"].as_f64(), Some(2.0));
        assert_eq!(parsed["tags"], serde_json::json!(["server", "music"]));
        assert_eq!(parsed["active"], true);

        let decoded = vm.eval(
            std::sync::Arc::<[u8]>::from(
                &br#"
                local value = serde.json.decode('{"name":"Lyra","tags":["server"],"active":true}')
                return value.name, value.tags[1], value.active
                "#[..],
            ),
            harmony_luau::ChunkOrigin::default(),
        )?;
        assert_eq!(
            decoded,
            vec![
                harmony_luau::Value::String(b"Lyra".to_vec()),
                harmony_luau::Value::String(b"server".to_vec()),
                harmony_luau::Value::Boolean(true),
            ]
        );
        Ok(())
    }

    #[test]
    fn luau_module_encodes_and_decodes_yaml_values() -> harmony_luau::runtime::Result<()> {
        let vm = harmony_luau::Vm::new()?;
        let spec = SerdeModule::module_spec();
        let table =
            harmony_core::luau::install_module(&vm, &harmony_core::ChunkOrigin::default(), &spec)?;
        vm.set_global_table("serde", &table)?;

        let encoded = vm.eval(
            std::sync::Arc::<[u8]>::from(
                &br#"
                return serde.yaml.encode({
                    name = "Lyra",
                    tags = { "server", "music" },
                })
                "#[..],
            ),
            harmony_luau::ChunkOrigin::default(),
        )?;
        let [harmony_luau::Value::String(encoded)] = encoded.as_slice() else {
            panic!("serde.yaml.encode should return a string");
        };
        let encoded = String::from_utf8(encoded.clone()).expect("YAML should be UTF-8");
        assert!(encoded.contains("name: Lyra"), "{encoded}");
        assert!(encoded.contains("server"), "{encoded}");

        let decoded = vm.eval(
            std::sync::Arc::<[u8]>::from(
                &br#"
                local value = serde.yaml.decode("name: Lyra\ntags:\n  - server\n")
                return value.name, value.tags[1]
                "#[..],
            ),
            harmony_luau::ChunkOrigin::default(),
        )?;
        assert_eq!(
            decoded,
            vec![
                harmony_luau::Value::String(b"Lyra".to_vec()),
                harmony_luau::Value::String(b"server".to_vec()),
            ]
        );
        Ok(())
    }

    #[test]
    fn luau_module_encodes_and_decodes_xml_values() -> harmony_luau::runtime::Result<()> {
        let vm = harmony_luau::Vm::new()?;
        let spec = SerdeModule::module_spec();
        let table =
            harmony_core::luau::install_module(&vm, &harmony_core::ChunkOrigin::default(), &spec)?;
        vm.set_global_table("serde", &table)?;

        let encoded = vm.eval(
            std::sync::Arc::<[u8]>::from(
                &br#"
                return serde.xml.encode("release", {
                    ["@id"] = "abc",
                    title = "Kind of Blue",
                })
                "#[..],
            ),
            harmony_luau::ChunkOrigin::default(),
        )?;
        let [harmony_luau::Value::String(encoded)] = encoded.as_slice() else {
            panic!("serde.xml.encode should return a string");
        };
        let encoded = String::from_utf8(encoded.clone()).expect("XML should be UTF-8");
        assert!(encoded.contains("<release"), "{encoded}");
        assert!(encoded.contains("id=\"abc\""), "{encoded}");
        assert!(encoded.contains("<title>Kind of Blue</title>"), "{encoded}");

        let decoded = vm.eval(
            std::sync::Arc::<[u8]>::from(
                &br#"
                local value = serde.xml.decode('<release id="abc"><title>Kind of Blue</title></release>')
                return value["@id"], value.title["$text"]
                "#[..],
            ),
            harmony_luau::ChunkOrigin::default(),
        )?;
        assert_eq!(
            decoded,
            vec![
                harmony_luau::Value::String(b"abc".to_vec()),
                harmony_luau::Value::String(b"Kind of Blue".to_vec()),
            ]
        );
        Ok(())
    }

    #[test]
    fn luau_empty_object_encodes_as_json_object() -> harmony_luau::runtime::Result<()> {
        let vm = harmony_luau::Vm::new()?;
        let spec = SerdeModule::module_spec();
        let table =
            harmony_core::luau::install_module(&vm, &harmony_core::ChunkOrigin::default(), &spec)?;
        vm.set_global_table("serde", &table)?;

        let values = vm.eval(
            std::sync::Arc::<[u8]>::from(
                &br#"
                return
                    serde.json.encode({ image_tags = serde.empty_object() }),
                    serde.json.encode({ image_tags = {} })
                "#[..],
            ),
            harmony_luau::ChunkOrigin::default(),
        )?;
        let [
            harmony_luau::Value::String(empty_object),
            harmony_luau::Value::String(empty_array),
        ] = values.as_slice()
        else {
            panic!("json.encode should return strings");
        };

        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(empty_object).expect("empty object JSON"),
            serde_json::json!({ "image_tags": {} })
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(empty_array).expect("empty array JSON"),
            serde_json::json!({ "image_tags": [] })
        );
        assert!(matches!(
            vm.eval(
                std::sync::Arc::<[u8]>::from(&b"serde.empty_object().extra = true"[..]),
                harmony_luau::ChunkOrigin::default(),
            ),
            Err(harmony_luau::Error::Runtime(_))
        ));
        Ok(())
    }
}
