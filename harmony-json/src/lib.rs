// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::fmt;

use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;
use harmony_luau::{
    DescribeModule,
    DescribeTypeAlias,
    JsonValue,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
};

const EMPTY_OBJECT_MARKER: &str = "__harmony_json_empty_object";

struct JsonModuleDocs;
struct JsonModule;

pub fn module_spec() -> ModuleSpec {
    ModuleSpec::new("harmony/json")
        .capability("harmony.json")
        .function(encode_spec())
        .function(decode_spec())
        .function(empty_object_spec())
        .install(|_| Ok(ModuleExport::new(JsonModule)))
}

fn encode_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("encode")
        .named_arg::<JsonValue>("input")
        .returns::<String>();
    spec.call(encode_callback)
}

fn decode_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("decode")
        .named_arg::<String>("input")
        .returns::<JsonValue>();
    spec.call(decode_callback)
}

fn empty_object_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("empty_object").returns::<JsonValue>();
    spec.call(empty_object_callback)
}

fn encode_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let value: luau::Value = frame.args.read_named("input")?;
    let json = luau_to_json(frame.vm, &value, 0)?;
    let encoded =
        serde_json::to_string(&json).map_err(|error| runtime_error("JSON encode failed", error))?;
    frame.returns.write(encoded)?;
    Ok(())
}

fn decode_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let input: String = frame.args.read_named("input")?;
    let json: serde_json::Value =
        serde_json::from_str(&input).map_err(|error| runtime_error("JSON decode failed", error))?;
    let value = json_to_luau(frame.vm, json, 0)?;
    frame.returns.write(value)?;
    Ok(())
}

fn empty_object_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let table = frame.vm.create_table()?;
    let metatable = frame.vm.create_table()?;
    metatable.set_raw(frame.vm, EMPTY_OBJECT_MARKER, luau::Value::Boolean(true))?;
    metatable.set_raw(frame.vm, "__metatable", luau::Value::Boolean(false))?;
    metatable.set_readonly(frame.vm, true)?;
    table.set_metatable_raw(frame.vm, Some(&metatable))?;
    table.set_readonly(frame.vm, true)?;
    frame.returns.write(table)?;
    Ok(())
}

pub fn luau_to_json(
    vm: &luau::Vm,
    value: &luau::Value,
    depth: usize,
) -> luau::runtime::Result<serde_json::Value> {
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
        luau::Value::TableData(table) => owned_table_to_json(vm, table, depth + 1),
        luau::Value::Table(table) => table_to_json(vm, table, depth + 1),
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
    }
}

pub fn optional_string_field(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Option<String>> {
    match table.get_raw(vm, key)? {
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

pub fn optional_i64_field(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Option<i64>> {
    match table.get_raw(vm, key)? {
        luau::Value::Integer(value) => Ok(Some(value)),
        luau::Value::Number(value) => Ok(Some(value as i64)),
        luau::Value::Nil => Ok(None),
        other => Err(luau::Error::Runtime(format!(
            "{key} must be a number or nil, got {}",
            other.type_name()
        ))),
    }
}

pub fn optional_json_field(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Option<serde_json::Value>> {
    let value = table.get_raw(vm, key)?;
    if matches!(value, luau::Value::Nil) {
        Ok(None)
    } else {
        luau_to_json(vm, &value, 0).map(Some)
    }
}

pub fn optional_string_pairs_field(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Vec<(String, String)>> {
    let pairs = match table.get_raw(vm, key)? {
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
    for (key, value) in pairs.pairs_raw(vm)? {
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
    vm: &luau::Vm,
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
                .map(|value| luau_to_json(vm, value, depth + 1))
                .collect::<luau::runtime::Result<Vec<_>>>()?,
        ));
    }

    let mut object = serde_json::Map::new();
    for (key, value) in table.fields() {
        object.insert(key.clone(), luau_to_json(vm, value, depth + 1)?);
    }
    Ok(serde_json::Value::Object(object))
}

fn table_to_json(
    vm: &luau::Vm,
    table: &luau::Table,
    depth: usize,
) -> luau::runtime::Result<serde_json::Value> {
    if table_is_empty_object_marker(vm, table)? {
        return Ok(serde_json::Value::Object(serde_json::Map::new()));
    }

    let pairs = table.pairs_raw(vm)?;
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
                let json = luau_to_json(vm, &value, depth)?;
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
                let json = luau_to_json(vm, &value, depth)?;
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

fn table_is_empty_object_marker(vm: &luau::Vm, table: &luau::Table) -> luau::runtime::Result<bool> {
    let Some(metatable) = table.metatable_raw(vm)? else {
        return Ok(false);
    };
    Ok(matches!(
        metatable.get_raw(vm, EMPTY_OBJECT_MARKER)?,
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
        serde_json::Value::Number(value) => value
            .as_f64()
            .filter(|value| value.is_finite())
            .map(luau::Value::Number)
            .ok_or_else(|| luau::Error::Runtime("JSON number is out of range".into())),
        serde_json::Value::String(value) => Ok(luau::Value::String(value.into_bytes())),
        serde_json::Value::Array(values) => {
            let table = vm.create_table_with_capacity(values.len() as i32, 0)?;
            for (index, value) in values.into_iter().enumerate() {
                table.set_integer_raw(vm, index as i32 + 1, json_to_luau(vm, value, depth + 1)?)?;
            }
            Ok(luau::Value::Table(table))
        }
        serde_json::Value::Object(values) => {
            let table = vm.create_table_with_capacity(0, values.len() as i32)?;
            for (key, value) in values {
                table.set_raw(vm, &key, json_to_luau(vm, value, depth + 1)?)?;
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
        serde_json::Value::Number(value) => value
            .as_f64()
            .filter(|value| value.is_finite())
            .map(luau::Value::Number)
            .ok_or_else(|| luau::Error::Runtime("JSON number is out of range".into())),
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

fn runtime_error(context: &str, error: impl fmt::Display) -> luau::Error {
    luau::Error::Runtime(format!("{context}: {error}"))
}

pub fn render_luau_definition() -> std::result::Result<String, fmt::Error> {
    render_definition_file_with_support(
        &JsonModuleDocs::module_descriptor(),
        &[JsonValue::type_alias_descriptor()],
        &[],
        &[],
    )
}

impl DescribeModule for JsonModuleDocs {
    fn module_descriptor() -> ModuleDescriptor {
        ModuleDescriptor {
            name: "Json",
            local_name: "json",
            description: Some("JSON encoding and decoding helpers."),
            fields: Vec::new(),
            functions: vec![
                ModuleFunctionDescriptor {
                    path: vec!["encode"],
                    description: Some("Encodes a Lua JSON-compatible value into a JSON string."),
                    params: vec![ParameterDescriptor {
                        name: "input",
                        ty: JsonValue::luau_type(),
                        description: None,
                        variadic: false,
                    }],
                    returns: vec![String::luau_type()],
                    yields: false,
                },
                ModuleFunctionDescriptor {
                    path: vec!["decode"],
                    description: Some("Decodes a JSON string into a Lua JSON-compatible value."),
                    params: vec![ParameterDescriptor {
                        name: "input",
                        ty: String::luau_type(),
                        description: None,
                        variadic: false,
                    }],
                    returns: vec![JsonValue::luau_type()],
                    yields: false,
                },
                ModuleFunctionDescriptor {
                    path: vec!["empty_object"],
                    description: Some(
                        "Returns a value that serializes as an empty JSON object `{}`. \
                         Use this instead of `{}` when a JSON object (not array) is required, \
                         since empty Lua tables serialize as arrays by default.",
                    ),
                    params: vec![],
                    returns: vec![JsonValue::luau_type()],
                    yields: false,
                },
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        module_spec,
        render_luau_definition,
    };

    #[test]
    fn renders_json_module_definition() {
        let rendered = render_luau_definition().expect("render harmony/json docs");

        assert!(rendered.contains("@class Json"));
        assert!(rendered.contains("@type JsonValue"));
        assert!(rendered.contains("export type JsonValue = (boolean | number | string | {JsonValue} | { [string]: JsonValue })?"));
        assert!(rendered.contains("function json.encode(input: JsonValue): string"));
        assert!(rendered.contains("function json.decode(input: string): JsonValue"));
        assert!(rendered.contains("function json.empty_object(): JsonValue"));
    }

    #[test]
    fn exposes_handwritten_module_spec() {
        let spec = module_spec();

        assert_eq!(spec.id.0.as_ref(), "harmony/json");
        assert_eq!(spec.capability.as_ref().unwrap().0.as_ref(), "harmony.json");
        assert_eq!(spec.functions.len(), 3);
        assert!(spec.functions.iter().all(|function| !function.yields));
        assert_eq!(spec.functions[0].name.as_ref(), "encode");
        assert_eq!(spec.functions[1].name.as_ref(), "decode");
        assert_eq!(spec.functions[2].name.as_ref(), "empty_object");
    }

    #[test]
    fn luau_module_encodes_and_decodes_json_values() -> harmony_luau::runtime::Result<()> {
        let vm = harmony_luau::Vm::new()?;
        let spec = module_spec();
        let origin = harmony_core::ChunkOrigin {
            plugin: Some(std::sync::Arc::from("demo")),
            path: Some(std::sync::Arc::from("plugins/demo/init.luau")),
            ..harmony_core::ChunkOrigin::default()
        };
        let table = harmony_core::install_luau_module(&vm, &origin, &spec)?;
        vm.set_global_table("json", &table)?;

        let encoded = vm.eval(
            std::sync::Arc::<[u8]>::from(
                &br#"
                return json.encode({
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
                local value = json.decode('{"name":"Lyra","tags":["server"],"active":true}')
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
    fn luau_empty_object_encodes_as_json_object() -> harmony_luau::runtime::Result<()> {
        let vm = harmony_luau::Vm::new()?;
        let spec = module_spec();
        let table =
            harmony_core::install_luau_module(&vm, &harmony_core::ChunkOrigin::default(), &spec)?;
        vm.set_global_table("json", &table)?;

        let values = vm.eval(
            std::sync::Arc::<[u8]>::from(
                &br#"
                return
                    json.encode({ image_tags = json.empty_object() }),
                    json.encode({ image_tags = {} })
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
                std::sync::Arc::<[u8]>::from(&b"json.empty_object().extra = true"[..]),
                harmony_luau::ChunkOrigin::default(),
            ),
            Err(harmony_luau::Error::Runtime(_))
        ));
        Ok(())
    }
}
