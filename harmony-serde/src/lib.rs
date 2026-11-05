// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod encoder;
mod node;
mod write;

use std::fmt;

use harmony_luau as luau;
use harmony_luau::{
    DescribeModule,
    DescribeTypeAlias,
    DescribeUserData,
    JsonValue,
    render_definition_file_with_support,
};

pub use encoder::{
    JsonEncoder,
    MAX_BUFFERED_BYTES,
    XmlEncoder,
};

const EMPTY_OBJECT_MARKER: &str = "__harmony_serde_empty_object";

pub struct SerdeModule;

/// The capability hosts conventionally gate this module behind.
pub const SCOPE: &str = "harmony.serde";

#[harmony_macros::module(
    id = "harmony/serde",
    name = "Serde",
    local = "serde",
    description = "Serde-backed JSON, YAML, and XML encoding and decoding helpers.",
    extend = install_limits,
    describe = describe_limits,
    userdata(JsonEncoder, XmlEncoder)
)]
impl SerdeModule {
    #[harmony(
        name = "json.encode",
        description = "Encodes a Lua JSON-compatible value into a JSON string.",
        args(input: JsonValue)
    )]
    fn json_encode(input: luau::Value) -> luau::runtime::Result<String> {
        let node = node::Reader::new(MAX_ENCODED_BYTES, &too_large).value(&input)?;
        let mut out = Vec::new();
        write::json(&mut out, &node, MAX_ENCODED_BYTES, &too_large)?;
        String::from_utf8(out).map_err(|error| runtime_error("JSON encode failed", error))
    }

    #[harmony(
        name = "json.encoder",
        description = "Creates an encoder that writes one JSON document a piece at a time, each piece encoded as `json.encode` would.",
        returns(JsonEncoder)
    )]
    fn json_encoder(
        vm: &luau::Vm,
        origin: &luau::ChunkOrigin,
    ) -> luau::runtime::Result<luau::Value> {
        <JsonEncoder as harmony_core::UserDataBinding>::class().create_value(
            vm,
            origin,
            JsonEncoder::default(),
        )
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
        let node = node::Reader::new(MAX_ENCODED_BYTES, &too_large).value(&input)?;
        write::yaml(&node, MAX_ENCODED_BYTES, &too_large)
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
        let node = node::Reader::new(MAX_ENCODED_BYTES, &too_large).value(&input)?;
        write::xml(&root, &node, MAX_ENCODED_BYTES, &too_large)
    }

    #[harmony(
        name = "xml.encoder",
        description = "Creates an encoder that writes one XML document a piece at a time, each element encoded as `xml.encode` would.",
        returns(XmlEncoder)
    )]
    fn xml_encoder(
        vm: &luau::Vm,
        origin: &luau::ChunkOrigin,
    ) -> luau::runtime::Result<luau::Value> {
        <XmlEncoder as harmony_core::UserDataBinding>::class().create_value(
            vm,
            origin,
            XmlEncoder::default(),
        )
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

const MAX_BUFFERED_BYTES_FIELD: &str = "MAX_BUFFERED_BYTES";
const MAX_ENCODED_BYTES_FIELD: &str = "MAX_ENCODED_BYTES";

/// Exports the limits the module enforces, so callers can plan against them.
fn install_limits(spec: harmony_core::ModuleSpec) -> harmony_core::ModuleSpec {
    spec.initializer(|_vm, _origin, root| {
        root.set(
            MAX_BUFFERED_BYTES_FIELD,
            luau::Value::Number(MAX_BUFFERED_BYTES as f64),
        )?;
        root.set(
            MAX_ENCODED_BYTES_FIELD,
            luau::Value::Number(MAX_ENCODED_BYTES as f64),
        )
    })
}

fn describe_limits(descriptor: &mut harmony_luau::ModuleDescriptor) {
    use harmony_luau::LuauTypeInfo;

    descriptor.fields.push(harmony_luau::ModuleFieldDescriptor {
        path: vec![MAX_BUFFERED_BYTES_FIELD],
        description: Some("The most an encoder holds unflushed, in bytes."),
        ty: u64::luau_type(),
    });
    descriptor.fields.push(harmony_luau::ModuleFieldDescriptor {
        path: vec![MAX_ENCODED_BYTES_FIELD],
        description: Some(
            "The most `json.encode`, `yaml.encode` and `xml.encode` encode a value to, in bytes.",
        ),
        ty: u64::luau_type(),
    });
}

/// The most a one-shot encode, or a value converted for the host, may encode to, in bytes.
pub const MAX_ENCODED_BYTES: usize = 16 * 1024 * 1024;

/// The most values a value converted for the host may hold. A `serde_json::Value` takes tens of
/// bytes for each, however few its JSON takes.
pub const MAX_CONVERTED_VALUES: usize = 1024 * 1024;

fn too_large() -> String {
    format!("the value encodes to more than {MAX_ENCODED_BYTES} bytes")
}

/// Converts `value` as `json.encode` does, refusing one that encodes to more than
/// [`MAX_ENCODED_BYTES`] or holds more than [`MAX_CONVERTED_VALUES`] values.
pub fn luau_to_json(value: &luau::Value) -> luau::runtime::Result<serde_json::Value> {
    luau_to_json_within(value, MAX_ENCODED_BYTES)
}

/// Converts `value` as `json.encode` does, refusing one that encodes to more than `max_bytes` or
/// holds more than [`MAX_CONVERTED_VALUES`] values.
///
/// A table can hold the same subtable or string many times over, so the converted value can be
/// far larger than the Luau one. The value is measured as JSON before any of it is built.
pub fn luau_to_json_within(
    value: &luau::Value,
    max_bytes: usize,
) -> luau::runtime::Result<serde_json::Value> {
    let too_large = || format!("the value encodes to more than {max_bytes} bytes");
    let node = node::Reader::new(max_bytes, &too_large).value(value)?;
    let meter = write::Meter::new(MAX_CONVERTED_VALUES);
    let mut sink = write::Sink::new((), max_bytes);
    if let Err(error) = serde_json::to_writer(&mut sink, &write::Encoded::new(&node, &meter, None))
    {
        return Err(luau::Error::Runtime(match meter.spent() {
            Some(write::Spent::Values) => {
                format!("the value holds more than {MAX_CONVERTED_VALUES} values")
            }
            Some(write::Spent::Bytes) => too_large(),
            None if error.is_io() => too_large(),
            None => format!("JSON encode failed: {error}"),
        }));
    }
    Ok(node.to_json())
}

pub fn optional_json_field(
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Option<serde_json::Value>> {
    let value = table.get(key)?;
    if matches!(value, luau::Value::Nil) {
        Ok(None)
    } else {
        luau_to_json(&value).map(Some)
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
        &[
            JsonEncoder::class_descriptor(),
            XmlEncoder::class_descriptor(),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::SerdeModule;

    fn eval_value(source: &str) -> harmony_luau::runtime::Result<harmony_luau::Value> {
        let vm = harmony_luau::Vm::new()?;
        let mut values = vm.eval(
            std::sync::Arc::<[u8]>::from(source.as_bytes()),
            harmony_luau::ChunkOrigin::default(),
        )?;
        Ok(values.remove(0))
    }

    #[test]
    fn conversion_refuses_shared_tables_that_encode_too_large() -> harmony_luau::runtime::Result<()>
    {
        let chain = eval_value(
            r#"
            local chain = { "leaf" }
            for _ = 1, 40 do
                chain = { a = chain, b = chain }
            end
            return chain
            "#,
        )?;
        let error = super::luau_to_json_within(&chain, 1024 * 1024)
            .expect_err("the chain encodes to 2^40 leaves");
        assert!(
            error
                .to_string()
                .contains("encodes to more than 1048576 bytes"),
            "{error}"
        );
        Ok(())
    }

    #[test]
    fn conversion_refuses_values_too_many_to_build() -> harmony_luau::runtime::Result<()> {
        // Fits in MAX_ENCODED_BYTES as JSON, but would build 2^23 values.
        let chain = eval_value(
            r#"
            local chain = 0
            for _ = 1, 22 do
                chain = { chain, chain }
            end
            return chain
            "#,
        )?;
        let error = super::luau_to_json(&chain).expect_err("the chain holds 2^23 values");
        assert!(
            error.to_string().contains(&format!(
                "holds more than {} values",
                super::MAX_CONVERTED_VALUES
            )),
            "{error}"
        );
        Ok(())
    }

    #[test]
    fn writing_a_node_matches_writing_its_json() -> harmony_luau::runtime::Result<()> {
        let value = eval_value(
            r#"
            local shared = { name = "a&b<c>\"d\"", score = -0.125, n = 7, big = 2^70 }
            return {
                ["@id"] = "x",
                ["$text"] = "body",
                items = { shared, shared, { true, false } },
                zeta = { 1, 2.5, -3 },
                alpha = "first",
            }
            "#,
        )?;
        let too_large = || String::from("too large");
        let node = super::node::Reader::new(usize::MAX, &too_large).value(&value)?;
        let json = node.to_json();

        let mut written = Vec::new();
        super::write::json(&mut written, &node, usize::MAX, &too_large)?;
        assert_eq!(written, serde_json::to_vec(&json).expect("JSON"));
        assert_eq!(
            super::write::yaml(&node, usize::MAX, &too_large)?,
            serde_saphyr::to_string(&json).expect("YAML")
        );
        assert_eq!(
            super::write::xml("root", &node, usize::MAX, &too_large)?,
            quick_xml::se::to_string_with_root("root", &json).expect("XML")
        );
        Ok(())
    }

    #[test]
    fn conversion_charges_what_json_writes() -> harmony_luau::runtime::Result<()> {
        let value = eval_value(
            r#"
            local shared = { name = "a\"b\n\1", score = -0.125, tiny = 1.5e-7, count = -42 }
            return { items = { shared, shared, true, false }, big = 2^70, half = 0.5 }
            "#,
        )?;
        let json = super::luau_to_json(&value)?;
        let written = serde_json::to_string(&json)
            .expect("the value encodes")
            .len();
        super::luau_to_json_within(&value, written)?;
        assert!(super::luau_to_json_within(&value, written - 1).is_err());
        Ok(())
    }

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
