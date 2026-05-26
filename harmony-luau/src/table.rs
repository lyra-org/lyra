use crate::{
    Error,
    Table,
    Value,
    Vm,
    runtime::Result,
};

pub fn required_string_field(vm: &Vm, table: &Table, key: &str) -> Result<String> {
    match table.get_raw(vm, key)? {
        Value::String(value) => String::from_utf8(value)
            .map_err(|error| Error::Runtime(format!("{key} must be valid UTF-8: {error}"))),
        Value::Nil => Err(Error::Runtime(format!("missing required field: {key}"))),
        other => Err(Error::Runtime(format!(
            "{key} must be a string, got {}",
            other.type_name()
        ))),
    }
}

pub fn optional_string_field(vm: &Vm, table: &Table, key: &str) -> Result<Option<String>> {
    match table.get_raw(vm, key)? {
        Value::Nil => Ok(None),
        Value::String(value) => String::from_utf8(value)
            .map(Some)
            .map_err(|error| Error::Runtime(format!("{key} must be valid UTF-8: {error}"))),
        other => Err(Error::Runtime(format!(
            "{key} must be a string or nil, got {}",
            other.type_name()
        ))),
    }
}

pub fn required_bool_field(vm: &Vm, table: &Table, key: &str) -> Result<bool> {
    match table.get_raw(vm, key)? {
        Value::Boolean(value) => Ok(value),
        Value::Nil => Err(Error::Runtime(format!("missing required field: {key}"))),
        other => Err(Error::Runtime(format!(
            "{key} must be a boolean, got {}",
            other.type_name()
        ))),
    }
}

pub fn optional_bool_field(vm: &Vm, table: &Table, key: &str) -> Result<Option<bool>> {
    match table.get_raw(vm, key)? {
        Value::Nil => Ok(None),
        Value::Boolean(value) => Ok(Some(value)),
        other => Err(Error::Runtime(format!(
            "{key} must be a boolean or nil, got {}",
            other.type_name()
        ))),
    }
}

pub fn required_i64_field(vm: &Vm, table: &Table, key: &str) -> Result<i64> {
    match table.get_raw(vm, key)? {
        Value::Integer(value) => Ok(value),
        Value::Number(value) => {
            number_to_i64(value).ok_or_else(|| Error::Runtime(format!("{key} must be an integer")))
        }
        Value::Nil => Err(Error::Runtime(format!("missing required field: {key}"))),
        other => Err(Error::Runtime(format!(
            "{key} must be an integer, got {}",
            other.type_name()
        ))),
    }
}

pub fn optional_i64_field(vm: &Vm, table: &Table, key: &str) -> Result<Option<i64>> {
    match table.get_raw(vm, key)? {
        Value::Nil => Ok(None),
        Value::Integer(value) => Ok(Some(value)),
        Value::Number(value) => number_to_i64(value)
            .map(Some)
            .ok_or_else(|| Error::Runtime(format!("{key} must be an integer or nil"))),
        other => Err(Error::Runtime(format!(
            "{key} must be an integer or nil, got {}",
            other.type_name()
        ))),
    }
}

pub fn required_table_field(vm: &Vm, table: &Table, key: &str) -> Result<Table> {
    match table.get_raw(vm, key)? {
        Value::Table(value) => Ok(value),
        Value::Nil => Err(Error::Runtime(format!("missing required field: {key}"))),
        other => Err(Error::Runtime(format!(
            "{key} must be a table, got {}",
            other.type_name()
        ))),
    }
}

pub fn optional_table_field(vm: &Vm, table: &Table, key: &str) -> Result<Option<Table>> {
    match table.get_raw(vm, key)? {
        Value::Nil => Ok(None),
        Value::Table(value) => Ok(Some(value)),
        other => Err(Error::Runtime(format!(
            "{key} must be a table or nil, got {}",
            other.type_name()
        ))),
    }
}

fn number_to_i64(value: f64) -> Option<i64> {
    if value.is_finite()
        && value.fract() == 0.0
        && value >= i64::MIN as f64
        && value < 9_223_372_036_854_775_808.0
    {
        Some(value as i64)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_required_and_optional_string_fields() -> Result<()> {
        let vm = Vm::new()?;
        let table = vm.create_table()?;
        table.set_raw(&vm, "name", Value::String(b"Lyra".to_vec()))?;

        assert_eq!(required_string_field(&vm, &table, "name")?, "Lyra");
        assert_eq!(optional_string_field(&vm, &table, "missing")?, None);
        Ok(())
    }

    #[test]
    fn required_field_errors_when_missing() -> Result<()> {
        let vm = Vm::new()?;
        let table = vm.create_table()?;

        let error = required_string_field(&vm, &table, "name").expect_err("field is missing");

        assert!(error.to_string().contains("missing required field: name"));
        Ok(())
    }

    #[test]
    fn string_field_rejects_wrong_type_and_invalid_utf8() -> Result<()> {
        let vm = Vm::new()?;
        let table = vm.create_table()?;
        table.set_raw(&vm, "name", Value::Boolean(true))?;
        table.set_raw(&vm, "bad", Value::String(vec![0xff]))?;

        let error = optional_string_field(&vm, &table, "name").expect_err("wrong type");
        assert!(error.to_string().contains("name must be a string or nil"));

        let error = required_string_field(&vm, &table, "bad").expect_err("invalid UTF-8");
        assert!(error.to_string().contains("bad must be valid UTF-8"));
        Ok(())
    }

    #[test]
    fn reads_bool_fields_without_coercion() -> Result<()> {
        let vm = Vm::new()?;
        let table = vm.create_table()?;
        table.set_raw(&vm, "enabled", Value::Boolean(true))?;
        table.set_raw(&vm, "coerce", Value::Integer(1))?;

        assert!(required_bool_field(&vm, &table, "enabled")?);
        assert_eq!(optional_bool_field(&vm, &table, "missing")?, None);
        assert!(optional_bool_field(&vm, &table, "coerce").is_err());
        Ok(())
    }

    #[test]
    fn reads_i64_fields_without_lossy_numbers() -> Result<()> {
        let vm = Vm::new()?;
        let table = vm.create_table()?;
        table.set_raw(&vm, "integer", Value::Integer(7))?;
        table.set_raw(&vm, "number", Value::Number(8.0))?;
        table.set_raw(&vm, "fraction", Value::Number(8.5))?;
        table.set_raw(&vm, "nan", Value::Number(f64::NAN))?;

        assert_eq!(required_i64_field(&vm, &table, "integer")?, 7);
        assert_eq!(optional_i64_field(&vm, &table, "number")?, Some(8));
        assert!(optional_i64_field(&vm, &table, "fraction").is_err());
        assert!(optional_i64_field(&vm, &table, "nan").is_err());
        Ok(())
    }

    #[test]
    fn reads_table_fields() -> Result<()> {
        let vm = Vm::new()?;
        let table = vm.create_table()?;
        let child = vm.create_table()?;
        child.set_raw(&vm, "value", Value::String(b"child".to_vec()))?;
        table.set_table_raw(&vm, "child", &child)?;

        let read = required_table_field(&vm, &table, "child")?;
        assert_eq!(
            read.get_raw(&vm, "value")?,
            Value::String(b"child".to_vec())
        );
        assert!(optional_table_field(&vm, &table, "missing")?.is_none());
        Ok(())
    }
}
