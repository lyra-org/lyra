// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Readers for the Luau arguments shared by the plugin modules.

use std::collections::HashSet;

use agdb::DbId;
use harmony_luau as luau;
use harmony_luau::{
    JsonValue,
    LuauType,
    LuauTypeInfo,
};
use serde::de::DeserializeOwned;

use lyra_db::ResolveId;

use crate::plugins::types::LuauNodeId;

/// The ids of a Luau array in array order, each once.
pub(crate) fn unique_ids(table: &luau::Table) -> luau::runtime::Result<Vec<DbId>> {
    let mut seen = HashSet::new();
    Ok(id_sequence(table)?
        .into_iter()
        .filter(|id| seen.insert(*id))
        .collect())
}

/// The ids of a Luau array in array order, repeats included. Every entry must be a positive
/// integer.
pub(crate) fn id_sequence(table: &luau::Table) -> luau::runtime::Result<Vec<DbId>> {
    array_values(table)?
        .into_iter()
        .map(|(index, value)| match value {
            luau::Value::Integer(value) if value > 0 => Ok(DbId(value)),
            luau::Value::Number(value)
                if value.is_finite() && value.fract() == 0.0 && value > 0.0 =>
            {
                Ok(DbId(value as i64))
            }
            other => Err(crate::plugins::runtime_error(format!(
                "id entry {index} must be a positive integer, got {}",
                describe(&other)
            ))),
        })
        .collect()
}

/// The integers of a Luau array in array order, each within the range of `T`.
pub(crate) fn integers<T: TryFrom<i64>>(table: &luau::Table) -> luau::runtime::Result<Vec<T>> {
    array_values(table)?
        .into_iter()
        .map(|(index, value)| {
            let integer = match value {
                luau::Value::Integer(value) => Some(value),
                luau::Value::Number(value) if value.is_finite() && value.fract() == 0.0 => {
                    Some(value as i64)
                }
                _ => None,
            };
            integer
                .and_then(|integer| T::try_from(integer).ok())
                .ok_or_else(|| {
                    crate::plugins::runtime_error(format!(
                        "entry {index} must be an integer in range, got {}",
                        describe(&value)
                    ))
                })
        })
        .collect()
}

/// [`unique_ids`] of the optional array field `key`, empty when absent.
pub(crate) fn optional_unique_ids(
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Vec<DbId>> {
    optional_id_array(table, key, unique_ids)
}

/// [`id_sequence`] of the optional array field `key`, empty when absent.
pub(crate) fn optional_id_sequence(
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Vec<DbId>> {
    optional_id_array(table, key, id_sequence)
}

fn optional_id_array(
    table: &luau::Table,
    key: &str,
    read: fn(&luau::Table) -> luau::runtime::Result<Vec<DbId>>,
) -> luau::runtime::Result<Vec<DbId>> {
    match table.get(key)? {
        luau::Value::Nil => Ok(Vec::new()),
        luau::Value::Table(table) => read(&table),
        other => Err(crate::plugins::runtime_error(format!(
            "{key} must be an array of ids, got {}",
            other.type_name()
        ))),
    }
}

pub(crate) fn positive_id(value: i64, name: &str) -> luau::runtime::Result<DbId> {
    if value <= 0 {
        return Err(crate::plugins::runtime_error(format!(
            "{name} must be a positive id, got {value}"
        )));
    }
    Ok(DbId(value))
}

/// A [`positive_id`] argument.
pub(crate) struct PositiveId(pub(crate) DbId);

impl<'vm> luau::FromLuau<'vm> for PositiveId {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let name = reader.next_name();
        let value: i64 = reader.read()?;
        positive_id(value, &name).map(Self)
    }
}

impl LuauTypeInfo for PositiveId {
    fn luau_type() -> LuauType {
        LuauNodeId::luau_type()
    }
}

/// A node id argument, taken as given.
pub(crate) struct NodeIdArg(pub(crate) DbId);

impl<'vm> luau::FromLuau<'vm> for NodeIdArg {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        Ok(Self(DbId(reader.read()?)))
    }
}

impl LuauTypeInfo for NodeIdArg {
    fn luau_type() -> LuauType {
        LuauNodeId::luau_type()
    }
}

/// A [`unique_ids`] argument.
pub(crate) struct UniqueIds(pub(crate) Vec<DbId>);

impl<'vm> luau::FromLuau<'vm> for UniqueIds {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let table: luau::Table = reader.read()?;
        unique_ids(&table).map(Self)
    }
}

impl LuauTypeInfo for UniqueIds {
    fn luau_type() -> LuauType {
        Vec::<LuauNodeId>::luau_type()
    }
}

pub(crate) fn optional_positive_id(
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Option<DbId>> {
    table
        .optional_i64_field(key)?
        .map(|value| positive_id(value, key))
        .transpose()
}

/// The strings of a Luau array in array order.
pub(crate) fn strings(table: &luau::Table) -> luau::runtime::Result<Vec<String>> {
    array_values(table)?
        .into_iter()
        .map(|(index, value)| match value {
            luau::Value::String(bytes) => {
                String::from_utf8(bytes).map_err(crate::plugins::runtime_error)
            }
            other => Err(crate::plugins::runtime_error(format!(
                "entry {index} must be a string, got {}",
                other.type_name()
            ))),
        })
        .collect()
}

pub(crate) fn resolve_id(value: luau::Value) -> luau::runtime::Result<ResolveId> {
    match value {
        luau::Value::Integer(value) => Ok(ResolveId::DbId(DbId(value))),
        luau::Value::Number(value) if value.is_finite() && value.fract() == 0.0 => {
            Ok(ResolveId::DbId(DbId(value as i64)))
        }
        luau::Value::String(bytes) => {
            let text = String::from_utf8(bytes).map_err(crate::plugins::runtime_error)?;
            if lyra_db::bootstrap::ROOT_COLLECTION_ALIASES.contains(&text.as_str()) {
                Ok(ResolveId::Alias(text))
            } else {
                Ok(ResolveId::Nanoid(text))
            }
        }
        other => Err(crate::plugins::runtime_error(format!(
            "expected integer or string id, got {}",
            other.type_name()
        ))),
    }
}

pub(crate) fn optional_u64(table: &luau::Table, key: &str) -> luau::runtime::Result<Option<u64>> {
    match table.get(key)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Integer(value) if value >= 0 => Ok(Some(value as u64)),
        luau::Value::Number(value) if value.is_finite() && value.fract() == 0.0 && value >= 0.0 => {
            Ok(Some(value as u64))
        }
        other => Err(crate::plugins::runtime_error(format!(
            "{key} must be a non-negative integer when provided, got {}",
            describe(&other)
        ))),
    }
}

/// `table[key]` as a boolean; `owner` names the table in errors, as `provider:id spec`.
pub(crate) fn optional_bool(
    table: &luau::Table,
    key: &str,
    owner: &str,
) -> luau::runtime::Result<Option<bool>> {
    match table.get(key)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Boolean(value) => Ok(Some(value)),
        other => Err(crate::plugins::runtime_error(format!(
            "{owner}.{key} must be a boolean, got {}",
            other.type_name()
        ))),
    }
}

/// `table[key]` as an integer; `owner` names the table in errors.
pub(crate) fn optional_i64(
    table: &luau::Table,
    key: &str,
    owner: &str,
) -> luau::runtime::Result<Option<i64>> {
    match table.get(key)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Integer(value) => Ok(Some(value)),
        luau::Value::Number(value) if value.is_finite() && value.fract() == 0.0 => {
            Ok(Some(value as i64))
        }
        other => Err(crate::plugins::runtime_error(format!(
            "{owner}.{key} must be an integer number, got {}",
            describe(&other)
        ))),
    }
}

/// `table[key]` as a timeout in milliseconds, `default` without one; `owner` names the table in
/// errors.
pub(crate) fn optional_timeout(
    table: &luau::Table,
    key: &str,
    owner: &str,
    default: std::time::Duration,
) -> luau::runtime::Result<std::time::Duration> {
    let Some(value) = optional_i64(table, key, owner)? else {
        return Ok(default);
    };
    if value < 1 {
        return Err(crate::plugins::runtime_error(format!(
            "{owner}.{key} must be an integer >= 1"
        )));
    }
    Ok(std::time::Duration::from_millis(value as u64))
}

/// A value for an error message: numbers as themselves, anything else by type.
fn describe(value: &luau::Value) -> String {
    match value {
        luau::Value::Integer(value) => value.to_string(),
        luau::Value::Number(value) => value.to_string(),
        other => other.type_name().to_string(),
    }
}

/// The entries of a Luau array, each with its index. A table with any other key, or with a
/// hole, is not an array.
pub(crate) fn array_values(table: &luau::Table) -> luau::runtime::Result<Vec<(i64, luau::Value)>> {
    let mut values = table
        .pairs()?
        .into_iter()
        .map(|(key, value)| array_index(key).map(|index| (index, value)))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| crate::plugins::runtime_error("expected an array, got a table with keys"))?;
    values.sort_by_key(|(index, _)| *index);
    if values
        .iter()
        .enumerate()
        .any(|(position, (index, _))| *index != position as i64 + 1)
    {
        return Err(crate::plugins::runtime_error(
            "expected an array, got a table with holes",
        ));
    }
    Ok(values)
}

fn array_index(value: luau::Value) -> Option<i64> {
    match value {
        luau::Value::Integer(value) if value > 0 => Some(value),
        luau::Value::Number(value) if value.is_finite() && value.fract() == 0.0 && value > 0.0 => {
            Some(value as i64)
        }
        _ => None,
    }
}

pub(crate) fn resolve_id_type() -> LuauType {
    LuauType::union(vec![i64::luau_type(), String::luau_type()])
}

#[cfg(feature = "docgen")]
pub(crate) fn sort_order_type() -> LuauType {
    LuauType::union(vec![
        LuauType::string_literal("ascending"),
        LuauType::string_literal("descending"),
    ])
}

/// A [`resolve_id`] argument.
pub(crate) struct ResolveIdArg(pub(crate) ResolveId);

impl<'vm> luau::FromLuau<'vm> for ResolveIdArg {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let value: luau::Value = reader.read()?;
        resolve_id(value).map(Self)
    }
}

impl LuauTypeInfo for ResolveIdArg {
    fn luau_type() -> LuauType {
        resolve_id_type()
    }
}

/// A Luau value converted to JSON.
pub(crate) struct JsonArg(pub(crate) serde_json::Value);

impl<'vm> luau::FromLuau<'vm> for JsonArg {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let value: luau::Value = reader.read()?;
        harmony_serde::luau_to_json(reader.vm(), &value, 0).map(Self)
    }
}

impl LuauTypeInfo for JsonArg {
    fn luau_type() -> LuauType {
        JsonValue::luau_type()
    }
}

/// A Luau value read as the JSON-deserializable `T`.
pub(crate) fn from_json<T: DeserializeOwned>(
    vm: &luau::Vm,
    value: &luau::Value,
) -> luau::runtime::Result<T> {
    serde_json::from_value(harmony_serde::luau_to_json(vm, value, 0)?)
        .map_err(crate::plugins::runtime_error)
}

/// A [`from_json`] argument, documented as `T`.
pub(crate) struct Json<T>(pub(crate) T);

impl<'vm, T: DeserializeOwned> luau::FromLuau<'vm> for Json<T> {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let value: luau::Value = reader.read()?;
        from_json(reader.vm(), &value).map(Self)
    }
}

impl<T: LuauTypeInfo> LuauTypeInfo for Json<T> {
    fn luau_type() -> LuauType {
        T::luau_type()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn array(vm: &luau::Vm, values: Vec<luau::Value>) -> luau::runtime::Result<luau::Table> {
        let table = vm.create_table()?;
        for (index, value) in values.into_iter().enumerate() {
            table.set_key(luau::Value::Integer(index as i64 + 1), value)?;
        }
        Ok(table)
    }

    #[test]
    fn id_readers_keep_array_order() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        let table = array(
            &vm,
            vec![
                luau::Value::Integer(3),
                luau::Value::Number(1.0),
                luau::Value::Integer(3),
                luau::Value::Integer(2),
            ],
        )?;

        assert_eq!(
            id_sequence(&table)?,
            vec![DbId(3), DbId(1), DbId(3), DbId(2)]
        );
        assert_eq!(unique_ids(&table)?, vec![DbId(3), DbId(1), DbId(2)]);
        Ok(())
    }

    #[test]
    fn array_readers_reject_tables_that_are_not_arrays() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        let keyed = vm.create_table()?;
        keyed.set("foo", luau::Value::Integer(5))?;
        let mixed = array(&vm, vec![luau::Value::Integer(1)])?;
        mixed.set("foo", luau::Value::Integer(5))?;
        let holed = vm.create_table()?;
        holed.set_key(luau::Value::Integer(2), luau::Value::Integer(5))?;

        for table in [keyed, mixed, holed] {
            assert!(id_sequence(&table).is_err());
        }
        assert!(id_sequence(&vm.create_table()?)?.is_empty());
        Ok(())
    }

    #[test]
    fn id_readers_reject_entries_that_are_not_positive_integers() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        for invalid in [
            luau::Value::String(b"7".to_vec()),
            luau::Value::Number(1.5),
            luau::Value::Boolean(true),
            luau::Value::Integer(0),
            luau::Value::Integer(-1),
        ] {
            let table = array(&vm, vec![luau::Value::Integer(1), invalid])?;
            let error = unique_ids(&table).expect_err("invalid id entry");
            assert!(
                error
                    .to_string()
                    .contains("id entry 2 must be a positive integer")
            );
        }
        Ok(())
    }
}
