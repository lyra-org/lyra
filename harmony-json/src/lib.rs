// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use mlua::{
    ExternalResult,
    LuaSerdeExt,
    Result,
    SerializeOptions,
    UserData,
    Value,
};
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;

use harmony_core::Module;
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
use mlua::{
    DeserializeOptions,
    Lua,
};
use std::fmt;

fn encode(lua: &Lua, input: Value) -> Result<String> {
    let json: serde_json::Value = lua
        .from_value_with(
            input,
            DeserializeOptions::new().encode_empty_tables_as_array(true),
        )
        .into_lua_err()?;
    let json_str = serde_json::to_string(&json).into_lua_err()?;

    Ok(json_str)
}

#[derive(Clone, Serialize)]
#[serde(transparent)]
struct EmptyObject(BTreeMap<String, serde_json::Value>);

impl UserData for EmptyObject {}

fn empty_object(lua: &Lua, _: ()) -> Result<Value> {
    let ud = lua
        .create_ser_userdata(EmptyObject(BTreeMap::new()))
        .map_err(mlua::Error::external)?;
    Ok(Value::UserData(ud))
}

fn decode(lua: &Lua, input: String) -> Result<Value> {
    let json: serde_json::Value = serde_json::from_str(&input).into_lua_err()?;

    lua.to_value_with(
        &json,
        SerializeOptions::new()
            .serialize_none_to_null(false)
            .serialize_unit_to_null(false),
    )
}

struct JsonModuleDocs;

pub fn get_module() -> Module {
    Module {
        path: "harmony/json".into(),
        setup: Arc::new(|lua: &Lua| -> anyhow::Result<mlua::Table> {
            let table = lua.create_table()?;

            table.set("encode", lua.create_function(encode)?)?;
            table.set("decode", lua.create_function(decode)?)?;
            table.set("empty_object", lua.create_function(empty_object)?)?;

            Ok(table)
        }),
        scope: harmony_core::Scope {
            id: "harmony.json".into(),
            description: "Encode and decode JSON.",
            danger: harmony_core::Danger::Negligible,
        },
    }
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
    use super::render_luau_definition;

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
}
