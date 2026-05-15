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

use harmony_core::{
    Danger,
    Module,
    ModuleBuilder,
    ModuleExport,
};
use harmony_luau::{
    JsonValue,
    LuauTypeInfo,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
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

fn module_export() -> ModuleExport {
    ModuleBuilder::empty("harmony/json", "Json", "json")
        .description("JSON encoding and decoding helpers.")
        .scope_id(
            "harmony.json",
            "Encode and decode JSON.",
            Danger::Negligible,
        )
        .type_alias::<JsonValue>()
        .sync_function(
            ModuleFunctionDescriptor::new("encode")
                .description("Encodes a Lua JSON-compatible value into a JSON string.")
                .param(ParameterDescriptor::new("input", JsonValue::luau_type()))
                .returns::<String>(),
            encode,
        )
        .sync_function(
            ModuleFunctionDescriptor::new("decode")
                .description("Decodes a JSON string into a Lua JSON-compatible value.")
                .param_type::<String>("input")
                .return_type(JsonValue::luau_type()),
            decode,
        )
        .sync_function(
            ModuleFunctionDescriptor::new("empty_object")
                .description(
                    "Returns a value that serializes as an empty JSON object `{}`. \
                 Use this instead of `{}` when a JSON object (not array) is required, \
                 since empty Lua tables serialize as arrays by default.",
                )
                .return_type(JsonValue::luau_type()),
            empty_object,
        )
        .build()
}

pub fn get_module() -> Module {
    module_export().into_module()
}

pub fn render_luau_definition() -> std::result::Result<String, fmt::Error> {
    module_export().render_luau_definition()
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
