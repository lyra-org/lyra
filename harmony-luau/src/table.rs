use crate::{
    Error,
    Table,
    Value,
    runtime::Result,
};

impl Table {
    pub fn required_string_field(&self, key: &str) -> Result<String> {
        match self.get(key)? {
            Value::String(value) => String::from_utf8(value)
                .map_err(|error| Error::Runtime(format!("{key} must be valid UTF-8: {error}"))),
            Value::Nil => Err(Error::Runtime(format!("missing required field: {key}"))),
            other => Err(Error::Runtime(format!(
                "{key} must be a string, got {}",
                other.type_name()
            ))),
        }
    }

    pub fn optional_string_field(&self, key: &str) -> Result<Option<String>> {
        match self.get(key)? {
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

    pub fn required_bool_field(&self, key: &str) -> Result<bool> {
        match self.get(key)? {
            Value::Boolean(value) => Ok(value),
            Value::Nil => Err(Error::Runtime(format!("missing required field: {key}"))),
            other => Err(Error::Runtime(format!(
                "{key} must be a boolean, got {}",
                other.type_name()
            ))),
        }
    }

    pub fn optional_bool_field(&self, key: &str) -> Result<Option<bool>> {
        match self.get(key)? {
            Value::Nil => Ok(None),
            Value::Boolean(value) => Ok(Some(value)),
            other => Err(Error::Runtime(format!(
                "{key} must be a boolean or nil, got {}",
                other.type_name()
            ))),
        }
    }

    pub fn required_i64_field(&self, key: &str) -> Result<i64> {
        match self.get(key)? {
            Value::Integer(value) => Ok(value),
            Value::Number(value) => number_to_i64(value)
                .ok_or_else(|| Error::Runtime(format!("{key} must be an integer"))),
            Value::Nil => Err(Error::Runtime(format!("missing required field: {key}"))),
            other => Err(Error::Runtime(format!(
                "{key} must be an integer, got {}",
                other.type_name()
            ))),
        }
    }

    pub fn optional_i64_field(&self, key: &str) -> Result<Option<i64>> {
        match self.get(key)? {
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

    pub fn required_table_field(&self, key: &str) -> Result<Table> {
        match self.get(key)? {
            Value::Table(value) => Ok(value),
            Value::Nil => Err(Error::Runtime(format!("missing required field: {key}"))),
            other => Err(Error::Runtime(format!(
                "{key} must be a table, got {}",
                other.type_name()
            ))),
        }
    }

    pub fn optional_table_field(&self, key: &str) -> Result<Option<Table>> {
        match self.get(key)? {
            Value::Nil => Ok(None),
            Value::Table(value) => Ok(Some(value)),
            other => Err(Error::Runtime(format!(
                "{key} must be a table or nil, got {}",
                other.type_name()
            ))),
        }
    }
}

/// `value` as an `i64`, if it is a whole number in its range.
#[expect(
    clippy::cast_possible_truncation,
    reason = "a whole number in `i64`'s range converts exactly"
)]
pub(crate) fn number_to_i64(value: f64) -> Option<i64> {
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
    use crate::Vm;

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn reads_required_and_optional_string_fields() -> Result<()> {
        let vm = Vm::new()?;
        let table = vm.create_table()?;
        table.set("name", Value::String(b"Lyra".to_vec()))?;

        assert_eq!(table.required_string_field("name")?, "Lyra");
        assert_eq!(table.optional_string_field("missing")?, None);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn required_field_errors_when_missing() -> Result<()> {
        let vm = Vm::new()?;
        let table = vm.create_table()?;

        let error = table
            .required_string_field("name")
            .expect_err("field is missing");

        assert!(error.to_string().contains("missing required field: name"));
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn string_field_rejects_wrong_type_and_invalid_utf8() -> Result<()> {
        let vm = Vm::new()?;
        let table = vm.create_table()?;
        table.set("name", Value::Boolean(true))?;
        table.set("bad", Value::String(vec![0xff]))?;

        let error = table.optional_string_field("name").expect_err("wrong type");
        assert!(error.to_string().contains("name must be a string or nil"));

        let error = table
            .required_string_field("bad")
            .expect_err("invalid UTF-8");
        assert!(error.to_string().contains("bad must be valid UTF-8"));
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn reads_bool_fields_without_coercion() -> Result<()> {
        let vm = Vm::new()?;
        let table = vm.create_table()?;
        table.set("enabled", Value::Boolean(true))?;
        table.set("coerce", Value::Integer(1))?;

        assert!(table.required_bool_field("enabled")?);
        assert_eq!(table.optional_bool_field("missing")?, None);
        assert!(table.optional_bool_field("coerce").is_err());
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn reads_i64_fields_without_lossy_numbers() -> Result<()> {
        let vm = Vm::new()?;
        let table = vm.create_table()?;
        table.set("integer", Value::Integer(7))?;
        table.set("number", Value::Number(8.0))?;
        table.set("fraction", Value::Number(8.5))?;
        table.set("nan", Value::Number(f64::NAN))?;

        assert_eq!(table.required_i64_field("integer")?, 7);
        assert_eq!(table.optional_i64_field("number")?, Some(8));
        assert!(table.optional_i64_field("fraction").is_err());
        assert!(table.optional_i64_field("nan").is_err());
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires Luau C VM")]
    fn reads_table_fields() -> Result<()> {
        let vm = Vm::new()?;
        let table = vm.create_table()?;
        let child = vm.create_table()?;
        child.set("value", Value::String(b"child".to_vec()))?;
        table.set("child", &child)?;

        let read = table.required_table_field("child")?;
        assert_eq!(read.get("value")?, Value::String(b"child".to_vec()));
        assert!(table.optional_table_field("missing")?.is_none());
        Ok(())
    }
}
