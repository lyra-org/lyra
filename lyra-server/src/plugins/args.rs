// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Readers for the Luau arguments shared by the plugin modules.

use std::collections::HashSet;

use agdb::DbId;
use harmony_luau as luau;
use harmony_luau::{
    LuauType,
    LuauTypeInfo,
};
use serde::Serialize;

use crate::plugins::db::{
    self,
    ListOptions,
    ResolveId,
    parse_sort_direction,
    parse_sort_specs_tokens,
};

/// The ids of a Luau array in array order, each once.
pub(crate) fn unique_ids(vm: &luau::Vm, table: &luau::Table) -> luau::runtime::Result<Vec<DbId>> {
    let mut seen = HashSet::new();
    Ok(id_sequence(vm, table)?
        .into_iter()
        .filter(|id| seen.insert(*id))
        .collect())
}

/// The ids of a Luau array in array order, repeats included. Every entry must be a positive
/// integer.
pub(crate) fn id_sequence(vm: &luau::Vm, table: &luau::Table) -> luau::runtime::Result<Vec<DbId>> {
    array_values(vm, table)?
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
                match other {
                    luau::Value::Integer(value) => value.to_string(),
                    luau::Value::Number(value) => value.to_string(),
                    other => other.type_name().to_string(),
                }
            ))),
        })
        .collect()
}

/// [`unique_ids`] of the optional array field `key`, empty when absent.
pub(crate) fn optional_unique_ids(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Vec<DbId>> {
    optional_id_array(vm, table, key, unique_ids)
}

/// [`id_sequence`] of the optional array field `key`, empty when absent.
pub(crate) fn optional_id_sequence(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Vec<DbId>> {
    optional_id_array(vm, table, key, id_sequence)
}

fn optional_id_array(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
    read: fn(&luau::Vm, &luau::Table) -> luau::runtime::Result<Vec<DbId>>,
) -> luau::runtime::Result<Vec<DbId>> {
    match table.get_raw(vm, key)? {
        luau::Value::Nil => Ok(Vec::new()),
        luau::Value::Table(table) => read(vm, &table),
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

pub(crate) fn optional_positive_id(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Option<DbId>> {
    luau::table::optional_i64_field(vm, table, key)?
        .map(|value| positive_id(value, key))
        .transpose()
}

/// The strings of a Luau array in array order.
pub(crate) fn strings(vm: &luau::Vm, table: &luau::Table) -> luau::runtime::Result<Vec<String>> {
    array_values(vm, table)?
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
            if db::ROOT_COLLECTION_ALIASES.contains(&text.as_str()) {
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

pub(crate) fn optional_u64(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Option<u64>> {
    match table.get_raw(vm, key)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Integer(value) if value >= 0 => Ok(Some(value as u64)),
        luau::Value::Number(value) if value.is_finite() && value.fract() == 0.0 && value >= 0.0 => {
            Ok(Some(value as u64))
        }
        other => Err(crate::plugins::runtime_error(format!(
            "{key} must be a non-negative integer when provided, got {}",
            match other {
                luau::Value::Integer(value) => value.to_string(),
                luau::Value::Number(value) => value.to_string(),
                other => other.type_name().to_string(),
            }
        ))),
    }
}

pub(crate) fn list_options(
    vm: &luau::Vm,
    table: &luau::Table,
) -> luau::runtime::Result<ListOptions> {
    let direction = parse_sort_direction(
        luau::table::optional_string_field(vm, table, "sort_order")?,
        true,
    )
    .map_err(crate::plugins::runtime_error)?;
    let sort_by = luau::table::optional_table_field(vm, table, "sort_by")?
        .map(|sort_by| strings(vm, &sort_by))
        .transpose()?;
    let sort = parse_sort_specs_tokens(sort_by, direction, |_| true, false)
        .map_err(crate::plugins::runtime_error)?;

    Ok(ListOptions {
        sort,
        offset: optional_u64(vm, table, "offset")?,
        limit: optional_u64(vm, table, "limit")?,
        search_term: luau::table::optional_string_field(vm, table, "search_term")?,
    })
}

pub(crate) fn page_table<T: Serialize>(
    entries: Vec<T>,
    total_count: u64,
    offset: u64,
) -> luau::runtime::Result<luau::OwnedTable> {
    let mut table = luau::OwnedTable::with_capacity(0, 3);
    table.set_field(
        "entities",
        harmony_luau::serializable_to_luau_owned(entries)?,
    );
    table.set_field("total_count", luau::Value::from(total_count as i64));
    table.set_field("offset", luau::Value::from(offset as i64));
    Ok(table)
}

/// The entries of a Luau array, each with its index. A table with any other key, or with a
/// hole, is not an array.
pub(crate) fn array_values(
    vm: &luau::Vm,
    table: &luau::Table,
) -> luau::runtime::Result<Vec<(i64, luau::Value)>> {
    let mut values = table
        .pairs_raw(vm)?
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

#[cfg(test)]
mod tests {
    use super::*;

    fn array(vm: &luau::Vm, values: Vec<luau::Value>) -> luau::runtime::Result<luau::Table> {
        let table = vm.create_table()?;
        for (index, value) in values.into_iter().enumerate() {
            table.set_key_raw(vm, luau::Value::Integer(index as i64 + 1), value)?;
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
            id_sequence(&vm, &table)?,
            vec![DbId(3), DbId(1), DbId(3), DbId(2)]
        );
        assert_eq!(unique_ids(&vm, &table)?, vec![DbId(3), DbId(1), DbId(2)]);
        Ok(())
    }

    #[test]
    fn array_readers_reject_tables_that_are_not_arrays() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        let keyed = vm.create_table()?;
        keyed.set_raw(&vm, "foo", luau::Value::Integer(5))?;
        let mixed = array(&vm, vec![luau::Value::Integer(1)])?;
        mixed.set_raw(&vm, "foo", luau::Value::Integer(5))?;
        let holed = vm.create_table()?;
        holed.set_key_raw(&vm, luau::Value::Integer(2), luau::Value::Integer(5))?;

        for table in [keyed, mixed, holed] {
            assert!(id_sequence(&vm, &table).is_err());
        }
        assert!(id_sequence(&vm, &vm.create_table()?)?.is_empty());
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
            let error = unique_ids(&vm, &table).expect_err("invalid id entry");
            assert!(
                error
                    .to_string()
                    .contains("id entry 2 must be a positive integer")
            );
        }
        Ok(())
    }
}
