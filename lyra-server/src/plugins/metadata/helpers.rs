// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use mlua::{
    Result,
    Table,
    Value,
};

pub(super) fn required_spec_string(spec: &Table, field: &str) -> Result<String> {
    match spec.get::<Value>(field)? {
        Value::String(value) => {
            let value = value
                .to_str()
                .map_err(|_| {
                    mlua::Error::runtime(format!("provider:id spec.{field} must be utf-8"))
                })?
                .trim()
                .to_string();
            if value.is_empty() {
                return Err(mlua::Error::runtime(format!(
                    "provider:id spec.{field} must be a non-empty string"
                )));
            }
            Ok(value)
        }
        Value::Nil => Err(mlua::Error::runtime(format!(
            "provider:id spec.{field} is required"
        ))),
        _ => Err(mlua::Error::runtime(format!(
            "provider:id spec.{field} must be a string"
        ))),
    }
}

pub(super) fn required_non_empty_request_string(
    request: &Table,
    field: &str,
    method: &str,
) -> Result<String> {
    match request.get::<Value>(field)? {
        Value::String(value) => {
            let value = value
                .to_str()
                .map_err(|_| mlua::Error::runtime(format!("{method}: {field} must be utf-8")))?
                .trim()
                .to_string();
            if value.is_empty() {
                return Err(mlua::Error::runtime(format!(
                    "{method}: {field} must be a non-empty string"
                )));
            }
            Ok(value)
        }
        Value::Nil => Err(mlua::Error::runtime(format!(
            "{method}: missing required field '{field}'"
        ))),
        _ => Err(mlua::Error::runtime(format!(
            "{method}: {field} must be a string"
        ))),
    }
}

pub(super) fn optional_trimmed_request_string(
    request: &Table,
    field: &str,
    method: &str,
) -> Result<Option<String>> {
    match request.get::<Value>(field)? {
        Value::Nil => Ok(None),
        Value::String(value) => {
            let value = value
                .to_str()
                .map_err(|_| mlua::Error::runtime(format!("{method}: {field} must be utf-8")))?
                .trim()
                .to_string();
            if value.is_empty() {
                Ok(None)
            } else {
                Ok(Some(value))
            }
        }
        _ => Err(mlua::Error::runtime(format!(
            "{method}: {field} must be a string when provided"
        ))),
    }
}
