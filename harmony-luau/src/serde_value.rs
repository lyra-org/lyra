use std::{
    fmt,
    result,
};

use serde::{
    Serialize,
    ser::{
        self,
        Impossible,
        SerializeMap,
        SerializeSeq,
        SerializeStruct,
        SerializeStructVariant,
        SerializeTuple,
        SerializeTupleStruct,
        SerializeTupleVariant,
        Serializer,
    },
};

use crate::{
    OwnedTable,
    Value,
    runtime::{
        Error,
        Result,
    },
};

const MAX_DEPTH: usize = 128;

pub fn serializable_to_luau_owned<T>(value: T) -> Result<Value>
where
    T: Serialize,
{
    serialize_with_depth(&value, 0).map_err(Into::into)
}

fn serialize_with_depth<T>(value: &T, depth: usize) -> SerializeResult<Value>
where
    T: Serialize + ?Sized,
{
    if depth > MAX_DEPTH {
        return Err(LuauSerializeError::custom(
            "Luau value is too deeply nested",
        ));
    }
    value.serialize(LuauValueSerializer { depth })
}

fn nested_depth(depth: usize) -> SerializeResult<usize> {
    depth
        .checked_add(1)
        .filter(|depth| *depth <= MAX_DEPTH)
        .ok_or_else(|| LuauSerializeError::custom("Luau value is too deeply nested"))
}

type SerializeResult<T> = result::Result<T, LuauSerializeError>;

#[derive(Debug)]
struct LuauSerializeError {
    message: String,
}

impl LuauSerializeError {
    fn custom<T>(message: T) -> Self
    where
        T: fmt::Display,
    {
        Self {
            message: message.to_string(),
        }
    }
}

impl ser::Error for LuauSerializeError {
    fn custom<T>(message: T) -> Self
    where
        T: fmt::Display,
    {
        Self::custom(message)
    }
}

impl fmt::Display for LuauSerializeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for LuauSerializeError {}

impl From<LuauSerializeError> for Error {
    fn from(error: LuauSerializeError) -> Self {
        Self::Serialize(error.to_string())
    }
}

struct LuauValueSerializer {
    depth: usize,
}

impl LuauValueSerializer {
    fn serialize_i64_value(value: i64) -> SerializeResult<Value> {
        Ok(Value::Integer(value))
    }

    fn serialize_u64_value(value: u64) -> SerializeResult<Value> {
        if value <= i64::MAX as u64 {
            Ok(Value::Integer(value as i64))
        } else {
            Err(LuauSerializeError::custom(
                "unsigned integer is out of Luau integer range",
            ))
        }
    }

    fn serialize_f64_value(value: f64) -> SerializeResult<Value> {
        if value.is_finite() {
            Ok(Value::Number(value))
        } else {
            Err(LuauSerializeError::custom("Luau numbers must be finite"))
        }
    }

    fn serialize_bytes_value(value: &[u8]) -> Value {
        Value::Buffer(value.to_vec())
    }
}

impl Serializer for LuauValueSerializer {
    type Ok = Value;
    type Error = LuauSerializeError;
    type SerializeSeq = LuauSequenceSerializer;
    type SerializeTuple = LuauSequenceSerializer;
    type SerializeTupleStruct = LuauSequenceSerializer;
    type SerializeTupleVariant = LuauTupleVariantSerializer;
    type SerializeMap = LuauMapSerializer;
    type SerializeStruct = LuauStructSerializer;
    type SerializeStructVariant = LuauStructVariantSerializer;

    fn serialize_bool(self, value: bool) -> SerializeResult<Value> {
        Ok(Value::Boolean(value))
    }

    fn serialize_i8(self, value: i8) -> SerializeResult<Value> {
        Self::serialize_i64_value(i64::from(value))
    }

    fn serialize_i16(self, value: i16) -> SerializeResult<Value> {
        Self::serialize_i64_value(i64::from(value))
    }

    fn serialize_i32(self, value: i32) -> SerializeResult<Value> {
        Self::serialize_i64_value(i64::from(value))
    }

    fn serialize_i64(self, value: i64) -> SerializeResult<Value> {
        Self::serialize_i64_value(value)
    }

    fn serialize_i128(self, value: i128) -> SerializeResult<Value> {
        i64::try_from(value)
            .map(Value::Integer)
            .map_err(|_| LuauSerializeError::custom("integer is out of Luau integer range"))
    }

    fn serialize_u8(self, value: u8) -> SerializeResult<Value> {
        Self::serialize_u64_value(u64::from(value))
    }

    fn serialize_u16(self, value: u16) -> SerializeResult<Value> {
        Self::serialize_u64_value(u64::from(value))
    }

    fn serialize_u32(self, value: u32) -> SerializeResult<Value> {
        Self::serialize_u64_value(u64::from(value))
    }

    fn serialize_u64(self, value: u64) -> SerializeResult<Value> {
        Self::serialize_u64_value(value)
    }

    fn serialize_u128(self, value: u128) -> SerializeResult<Value> {
        u64::try_from(value)
            .map_err(|_| LuauSerializeError::custom("integer is out of Luau integer range"))
            .and_then(Self::serialize_u64_value)
    }

    fn serialize_f32(self, value: f32) -> SerializeResult<Value> {
        Self::serialize_f64_value(f64::from(value))
    }

    fn serialize_f64(self, value: f64) -> SerializeResult<Value> {
        Self::serialize_f64_value(value)
    }

    fn serialize_char(self, value: char) -> SerializeResult<Value> {
        let mut buffer = [0; 4];
        Ok(Value::String(
            value.encode_utf8(&mut buffer).as_bytes().to_vec(),
        ))
    }

    fn serialize_str(self, value: &str) -> SerializeResult<Value> {
        Ok(Value::String(value.as_bytes().to_vec()))
    }

    fn serialize_bytes(self, value: &[u8]) -> SerializeResult<Value> {
        Ok(Self::serialize_bytes_value(value))
    }

    fn serialize_none(self) -> SerializeResult<Value> {
        Ok(Value::Nil)
    }

    fn serialize_some<T>(self, value: &T) -> SerializeResult<Value>
    where
        T: Serialize + ?Sized,
    {
        serialize_with_depth(value, self.depth)
    }

    fn serialize_unit(self) -> SerializeResult<Value> {
        Ok(Value::Nil)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> SerializeResult<Value> {
        Ok(Value::Nil)
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> SerializeResult<Value> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> SerializeResult<Value>
    where
        T: Serialize + ?Sized,
    {
        serialize_with_depth(value, self.depth)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> SerializeResult<Value>
    where
        T: Serialize + ?Sized,
    {
        let mut table = OwnedTable::with_capacity(0, 1);
        table.set_field(
            variant,
            serialize_with_depth(value, nested_depth(self.depth)?)?,
        );
        Ok(Value::TableData(table))
    }

    fn serialize_seq(self, len: Option<usize>) -> SerializeResult<LuauSequenceSerializer> {
        Ok(LuauSequenceSerializer {
            table: OwnedTable::with_capacity(len.unwrap_or(0), 0),
            depth: self.depth,
        })
    }

    fn serialize_tuple(self, len: usize) -> SerializeResult<LuauSequenceSerializer> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> SerializeResult<LuauSequenceSerializer> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> SerializeResult<LuauTupleVariantSerializer> {
        Ok(LuauTupleVariantSerializer {
            variant,
            table: OwnedTable::with_capacity(len, 0),
            depth: self.depth,
        })
    }

    fn serialize_map(self, len: Option<usize>) -> SerializeResult<LuauMapSerializer> {
        let len = len.unwrap_or(0);
        Ok(LuauMapSerializer {
            table: OwnedTable::with_entry_capacity(0, len, len),
            next_key: None,
            depth: self.depth,
        })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> SerializeResult<LuauStructSerializer> {
        Ok(LuauStructSerializer {
            table: OwnedTable::with_capacity(0, len),
            depth: self.depth,
        })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> SerializeResult<LuauStructVariantSerializer> {
        Ok(LuauStructVariantSerializer {
            variant,
            table: OwnedTable::with_capacity(0, len),
            depth: self.depth,
        })
    }
}

struct LuauSequenceSerializer {
    table: OwnedTable,
    depth: usize,
}

impl LuauSequenceSerializer {
    fn push<T>(&mut self, value: &T) -> SerializeResult<()>
    where
        T: Serialize + ?Sized,
    {
        self.table
            .push_array(serialize_with_depth(value, nested_depth(self.depth)?)?);
        Ok(())
    }

    fn finish(self) -> Value {
        Value::TableData(self.table)
    }
}

impl SerializeSeq for LuauSequenceSerializer {
    type Ok = Value;
    type Error = LuauSerializeError;

    fn serialize_element<T>(&mut self, value: &T) -> SerializeResult<()>
    where
        T: Serialize + ?Sized,
    {
        self.push(value)
    }

    fn end(self) -> SerializeResult<Value> {
        Ok(self.finish())
    }
}

impl SerializeTuple for LuauSequenceSerializer {
    type Ok = Value;
    type Error = LuauSerializeError;

    fn serialize_element<T>(&mut self, value: &T) -> SerializeResult<()>
    where
        T: Serialize + ?Sized,
    {
        self.push(value)
    }

    fn end(self) -> SerializeResult<Value> {
        Ok(self.finish())
    }
}

impl SerializeTupleStruct for LuauSequenceSerializer {
    type Ok = Value;
    type Error = LuauSerializeError;

    fn serialize_field<T>(&mut self, value: &T) -> SerializeResult<()>
    where
        T: Serialize + ?Sized,
    {
        self.push(value)
    }

    fn end(self) -> SerializeResult<Value> {
        Ok(self.finish())
    }
}

struct LuauTupleVariantSerializer {
    variant: &'static str,
    table: OwnedTable,
    depth: usize,
}

impl SerializeTupleVariant for LuauTupleVariantSerializer {
    type Ok = Value;
    type Error = LuauSerializeError;

    fn serialize_field<T>(&mut self, value: &T) -> SerializeResult<()>
    where
        T: Serialize + ?Sized,
    {
        self.table
            .push_array(serialize_with_depth(value, nested_depth(self.depth)?)?);
        Ok(())
    }

    fn end(self) -> SerializeResult<Value> {
        let mut table = OwnedTable::with_capacity(0, 1);
        table.set_field(self.variant, Value::TableData(self.table));
        Ok(Value::TableData(table))
    }
}

#[derive(Debug)]
enum LuauMapKey {
    Field(String),
    Value(Value),
}

struct LuauMapSerializer {
    table: OwnedTable,
    next_key: Option<LuauMapKey>,
    depth: usize,
}

impl LuauMapSerializer {
    fn insert(&mut self, key: LuauMapKey, value: Value) {
        match key {
            LuauMapKey::Field(key) if !key.as_bytes().contains(&0) => {
                self.table.set_field(key, value);
            }
            LuauMapKey::Field(key) => {
                self.table.set_key(Value::String(key.into_bytes()), value);
            }
            LuauMapKey::Value(key) => {
                self.table.set_key(key, value);
            }
        }
    }
}

impl SerializeMap for LuauMapSerializer {
    type Ok = Value;
    type Error = LuauSerializeError;

    fn serialize_key<T>(&mut self, key: &T) -> SerializeResult<()>
    where
        T: Serialize + ?Sized,
    {
        if self.next_key.is_some() {
            return Err(LuauSerializeError::custom(
                "serialize_key called before previous map value",
            ));
        }
        self.next_key = Some(key.serialize(LuauKeySerializer)?);
        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> SerializeResult<()>
    where
        T: Serialize + ?Sized,
    {
        let key = self
            .next_key
            .take()
            .ok_or_else(|| LuauSerializeError::custom("serialize_value called before map key"))?;
        let value = serialize_with_depth(value, nested_depth(self.depth)?)?;
        self.insert(key, value);
        Ok(())
    }

    fn serialize_entry<K, V>(&mut self, key: &K, value: &V) -> SerializeResult<()>
    where
        K: Serialize + ?Sized,
        V: Serialize + ?Sized,
    {
        let key = key.serialize(LuauKeySerializer)?;
        let value = serialize_with_depth(value, nested_depth(self.depth)?)?;
        self.insert(key, value);
        Ok(())
    }

    fn end(self) -> SerializeResult<Value> {
        if self.next_key.is_some() {
            return Err(LuauSerializeError::custom(
                "map serialization ended with a key but no value",
            ));
        }
        Ok(Value::TableData(self.table))
    }
}

struct LuauStructSerializer {
    table: OwnedTable,
    depth: usize,
}

impl SerializeStruct for LuauStructSerializer {
    type Ok = Value;
    type Error = LuauSerializeError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> SerializeResult<()>
    where
        T: Serialize + ?Sized,
    {
        self.table
            .set_field(key, serialize_with_depth(value, nested_depth(self.depth)?)?);
        Ok(())
    }

    fn end(self) -> SerializeResult<Value> {
        Ok(Value::TableData(self.table))
    }
}

struct LuauStructVariantSerializer {
    variant: &'static str,
    table: OwnedTable,
    depth: usize,
}

impl SerializeStructVariant for LuauStructVariantSerializer {
    type Ok = Value;
    type Error = LuauSerializeError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> SerializeResult<()>
    where
        T: Serialize + ?Sized,
    {
        self.table
            .set_field(key, serialize_with_depth(value, nested_depth(self.depth)?)?);
        Ok(())
    }

    fn end(self) -> SerializeResult<Value> {
        let mut table = OwnedTable::with_capacity(0, 1);
        table.set_field(self.variant, Value::TableData(self.table));
        Ok(Value::TableData(table))
    }
}

struct LuauKeySerializer;

impl LuauKeySerializer {
    fn unsupported(kind: &'static str) -> LuauSerializeError {
        LuauSerializeError::custom(format!("Luau table keys cannot be serialized from {kind}"))
    }
}

impl Serializer for LuauKeySerializer {
    type Ok = LuauMapKey;
    type Error = LuauSerializeError;
    type SerializeSeq = Impossible<LuauMapKey, LuauSerializeError>;
    type SerializeTuple = Impossible<LuauMapKey, LuauSerializeError>;
    type SerializeTupleStruct = Impossible<LuauMapKey, LuauSerializeError>;
    type SerializeTupleVariant = Impossible<LuauMapKey, LuauSerializeError>;
    type SerializeMap = Impossible<LuauMapKey, LuauSerializeError>;
    type SerializeStruct = Impossible<LuauMapKey, LuauSerializeError>;
    type SerializeStructVariant = Impossible<LuauMapKey, LuauSerializeError>;

    fn serialize_bool(self, value: bool) -> SerializeResult<LuauMapKey> {
        Ok(LuauMapKey::Value(Value::Boolean(value)))
    }

    fn serialize_i8(self, value: i8) -> SerializeResult<LuauMapKey> {
        self.serialize_i64(i64::from(value))
    }

    fn serialize_i16(self, value: i16) -> SerializeResult<LuauMapKey> {
        self.serialize_i64(i64::from(value))
    }

    fn serialize_i32(self, value: i32) -> SerializeResult<LuauMapKey> {
        self.serialize_i64(i64::from(value))
    }

    fn serialize_i64(self, value: i64) -> SerializeResult<LuauMapKey> {
        Ok(LuauMapKey::Value(Value::Integer(value)))
    }

    fn serialize_i128(self, value: i128) -> SerializeResult<LuauMapKey> {
        i64::try_from(value)
            .map(Value::Integer)
            .map(LuauMapKey::Value)
            .map_err(|_| LuauSerializeError::custom("integer key is out of Luau integer range"))
    }

    fn serialize_u8(self, value: u8) -> SerializeResult<LuauMapKey> {
        self.serialize_u64(u64::from(value))
    }

    fn serialize_u16(self, value: u16) -> SerializeResult<LuauMapKey> {
        self.serialize_u64(u64::from(value))
    }

    fn serialize_u32(self, value: u32) -> SerializeResult<LuauMapKey> {
        self.serialize_u64(u64::from(value))
    }

    fn serialize_u64(self, value: u64) -> SerializeResult<LuauMapKey> {
        if value <= i64::MAX as u64 {
            Ok(LuauMapKey::Value(Value::Integer(value as i64)))
        } else {
            Err(LuauSerializeError::custom(
                "unsigned integer key is out of Luau integer range",
            ))
        }
    }

    fn serialize_u128(self, value: u128) -> SerializeResult<LuauMapKey> {
        u64::try_from(value)
            .map_err(|_| LuauSerializeError::custom("integer key is out of Luau integer range"))
            .and_then(|value| self.serialize_u64(value))
    }

    fn serialize_f32(self, value: f32) -> SerializeResult<LuauMapKey> {
        self.serialize_f64(f64::from(value))
    }

    fn serialize_f64(self, value: f64) -> SerializeResult<LuauMapKey> {
        if value.is_finite() {
            Ok(LuauMapKey::Value(Value::Number(value)))
        } else {
            Err(LuauSerializeError::custom(
                "Luau number keys must be finite",
            ))
        }
    }

    fn serialize_char(self, value: char) -> SerializeResult<LuauMapKey> {
        Ok(LuauMapKey::Field(value.to_string()))
    }

    fn serialize_str(self, value: &str) -> SerializeResult<LuauMapKey> {
        Ok(LuauMapKey::Field(value.to_owned()))
    }

    fn serialize_bytes(self, _value: &[u8]) -> SerializeResult<LuauMapKey> {
        Err(Self::unsupported("bytes"))
    }

    fn serialize_none(self) -> SerializeResult<LuauMapKey> {
        Err(Self::unsupported("none"))
    }

    fn serialize_some<T>(self, value: &T) -> SerializeResult<LuauMapKey>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> SerializeResult<LuauMapKey> {
        Err(Self::unsupported("unit"))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> SerializeResult<LuauMapKey> {
        Err(Self::unsupported("unit struct"))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> SerializeResult<LuauMapKey> {
        Ok(LuauMapKey::Field(variant.to_owned()))
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        value: &T,
    ) -> SerializeResult<LuauMapKey>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> SerializeResult<LuauMapKey>
    where
        T: Serialize + ?Sized,
    {
        Err(Self::unsupported("newtype variant"))
    }

    fn serialize_seq(self, _len: Option<usize>) -> SerializeResult<Self::SerializeSeq> {
        Err(Self::unsupported("sequence"))
    }

    fn serialize_tuple(self, _len: usize) -> SerializeResult<Self::SerializeTuple> {
        Err(Self::unsupported("tuple"))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> SerializeResult<Self::SerializeTupleStruct> {
        Err(Self::unsupported("tuple struct"))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> SerializeResult<Self::SerializeTupleVariant> {
        Err(Self::unsupported("tuple variant"))
    }

    fn serialize_map(self, _len: Option<usize>) -> SerializeResult<Self::SerializeMap> {
        Err(Self::unsupported("map"))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> SerializeResult<Self::SerializeStruct> {
        Err(Self::unsupported("struct"))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> SerializeResult<Self::SerializeStructVariant> {
        Err(Self::unsupported("struct variant"))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde::{
        Serialize,
        Serializer,
    };

    use super::serializable_to_luau_owned;
    use crate::{
        Error,
        Value,
    };

    #[derive(Serialize)]
    struct Sample<'a> {
        id: i64,
        name: &'a str,
        tags: Vec<&'a str>,
        missing: Option<i64>,
    }

    struct Bytes<'a>(&'a [u8]);

    impl Serialize for Bytes<'_> {
        fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_bytes(self.0)
        }
    }

    #[test]
    fn serializes_structs_directly_to_luau_values() -> crate::runtime::Result<()> {
        let value = serializable_to_luau_owned(Sample {
            id: 42,
            name: "Lyra",
            tags: vec!["music", "plugins"],
            missing: None,
        })?;

        let Value::TableData(table) = value else {
            panic!("struct should serialize as a table");
        };

        assert_eq!(
            table.fields(),
            &[
                ("id".to_string(), Value::Integer(42)),
                ("name".to_string(), Value::String(b"Lyra".to_vec())),
                (
                    "tags".to_string(),
                    Value::TableData({
                        let mut tags = crate::OwnedTable::with_capacity(2, 0);
                        tags.push_array(Value::String(b"music".to_vec()));
                        tags.push_array(Value::String(b"plugins".to_vec()));
                        tags
                    })
                ),
                ("missing".to_string(), Value::Nil),
            ]
        );
        Ok(())
    }

    #[test]
    fn serializes_luau_map_keys_without_json_stringification() -> crate::runtime::Result<()> {
        let value = serializable_to_luau_owned(BTreeMap::from([(7_i64, "seven")]))?;

        let Value::TableData(table) = value else {
            panic!("map should serialize as a table");
        };

        assert!(table.fields().is_empty());
        assert_eq!(
            table.entries(),
            &[(Value::Integer(7), Value::String(b"seven".to_vec()))]
        );
        Ok(())
    }

    #[test]
    fn serializes_bytes_as_luau_buffers() -> crate::runtime::Result<()> {
        assert_eq!(
            serializable_to_luau_owned(Bytes(&[1, 2, 3]))?,
            Value::Buffer(vec![1, 2, 3])
        );
        Ok(())
    }

    #[test]
    fn rejects_values_luau_cannot_represent_safely() {
        assert!(matches!(
            serializable_to_luau_owned(u64::MAX),
            Err(Error::Serialize(message)) if message.contains("out of Luau integer range")
        ));
        assert!(matches!(
            serializable_to_luau_owned(f64::NAN),
            Err(Error::Serialize(message)) if message.contains("finite")
        ));
    }
}
