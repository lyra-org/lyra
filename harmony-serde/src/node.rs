// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! A Luau value read for encoding. A table is read once however often it appears and a string
//! copied once, so reading costs what the Luau value holds rather than what it encodes to.

use std::{
    collections::HashMap,
    ops::ControlFlow,
    rc::Rc,
};

use harmony_luau as luau;

use crate::{
    EMPTY_OBJECT_MARKER,
    MAX_CONVERTED_VALUES,
    runtime_error,
};

const MAX_DEPTH: usize = 128;

/// A JSON-compatible value whose shared parts stay shared.
#[derive(Clone, Debug)]
pub(crate) enum Node {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(Rc<str>),
    Array(Rc<[Node]>),
    /// Entries in key order, as `serde_json::Map` keeps them.
    Object(Rc<[(Rc<str>, Node)]>),
}

impl Node {
    pub(crate) fn to_json(&self) -> serde_json::Value {
        match self {
            Self::Null => serde_json::Value::Null,
            Self::Bool(value) => serde_json::Value::Bool(*value),
            Self::Number(value) => serde_json::Value::Number(value.clone()),
            Self::String(value) => serde_json::Value::String(value.to_string()),
            Self::Array(items) => {
                serde_json::Value::Array(items.iter().map(Self::to_json).collect())
            }
            Self::Object(entries) => serde_json::Value::Object(
                entries
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_json()))
                    .collect(),
            ),
        }
    }
}

/// A read node and how many tables deep it goes, itself included.
struct Read {
    node: Node,
    height: usize,
}

impl Read {
    fn leaf(node: Node) -> Self {
        Self { node, height: 0 }
    }
}

/// Reads Luau values into [`Node`]s. It refuses once the distinct strings they hold outgrow what
/// the encoding may take, each reaching the output at least once, and once it would hold more
/// than [`MAX_CONVERTED_VALUES`] values, each charged before anything grows to hold it.
pub(crate) struct Reader<'a> {
    depth: usize,
    remaining: usize,
    values: usize,
    too_large: &'a dyn Fn() -> String,
    tables: HashMap<usize, (Node, usize)>,
    strings: HashMap<usize, Rc<str>>,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(max_bytes: usize, too_large: &'a dyn Fn() -> String) -> Self {
        Self {
            depth: 0,
            remaining: max_bytes,
            values: MAX_CONVERTED_VALUES,
            too_large,
            tables: HashMap::new(),
            strings: HashMap::new(),
        }
    }

    pub(crate) fn value(&mut self, value: &luau::Value) -> luau::runtime::Result<Node> {
        self.hold(1)?;
        self.read_value(value).map(|read| read.node)
    }

    pub(crate) fn value_ref(&mut self, value: luau::ValueRef<'_>) -> luau::runtime::Result<Node> {
        self.hold(1)?;
        self.read_value_ref(value).map(|read| read.node)
    }

    /// Charges `count` more values, before anything grows to hold them.
    fn hold(&mut self, count: usize) -> luau::runtime::Result<()> {
        self.values = self.values.checked_sub(count).ok_or_else(too_many_values)?;
        Ok(())
    }

    /// A string the output holds `charge` bytes of, copied the first time the VM's string is
    /// seen. `shared` says the bytes are the VM's, whose address identifies the string.
    pub(crate) fn string(
        &mut self,
        bytes: &[u8],
        shared: bool,
        charge: usize,
        invalid: &str,
    ) -> luau::runtime::Result<Rc<str>> {
        let identity = bytes.as_ptr() as usize;
        if shared && let Some(string) = self.strings.get(&identity) {
            return Ok(string.clone());
        }
        self.remaining = self
            .remaining
            .checked_sub(charge)
            .ok_or_else(|| luau::Error::Runtime((self.too_large)()))?;
        let string: Rc<str> = std::str::from_utf8(bytes)
            .map_err(|error| runtime_error(invalid, error))?
            .into();
        if shared {
            self.strings.insert(identity, string.clone());
        }
        Ok(string)
    }

    /// An object key, charged for what every format writes of it: XML writes `@name` as
    /// `name` and leaves `$text` and `$value` out. What goes uncharged is at most six bytes a
    /// key, and keys are values the reader holds a bounded number of.
    fn key(&mut self, bytes: &[u8], shared: bool) -> luau::runtime::Result<Rc<str>> {
        let charge = match bytes {
            b"$text" | b"$value" => 0,
            [b'@', name @ ..] => name.len(),
            _ => bytes.len(),
        };
        self.string(
            bytes,
            shared,
            charge,
            "JSON object keys must be valid UTF-8",
        )
    }

    fn text(&mut self, bytes: &[u8], shared: bool) -> luau::runtime::Result<Read> {
        self.string(
            bytes,
            shared,
            bytes.len(),
            "JSON strings must be valid UTF-8",
        )
        .map(|string| Read::leaf(Node::String(string)))
    }

    fn read_value(&mut self, value: &luau::Value) -> luau::runtime::Result<Read> {
        match value {
            luau::Value::Nil => Ok(Read::leaf(Node::Null)),
            luau::Value::Boolean(value) => Ok(Read::leaf(Node::Bool(*value))),
            luau::Value::Integer(value) => Ok(Read::leaf(Node::Number((*value).into()))),
            luau::Value::Number(value) => number(*value),
            luau::Value::String(bytes) => self.text(bytes, false),
            luau::Value::TableData(table) => self.nested(|reader| reader.owned_table(table)),
            luau::Value::Table(table) => self.table(table),
            other => Err(unsupported(other.type_name())),
        }
    }

    fn read_value_ref(&mut self, value: luau::ValueRef<'_>) -> luau::runtime::Result<Read> {
        match value {
            luau::ValueRef::Nil => Ok(Read::leaf(Node::Null)),
            luau::ValueRef::Boolean(value) => Ok(Read::leaf(Node::Bool(value))),
            luau::ValueRef::Integer(value) => Ok(Read::leaf(Node::Number(value.into()))),
            luau::ValueRef::Number(value) => number(value),
            luau::ValueRef::String(bytes) => self.text(bytes, true),
            luau::ValueRef::Table(table) => self.table(&table),
            other => Err(unsupported(other.type_name())),
        }
    }

    fn nested(
        &mut self,
        read: impl FnOnce(&mut Self) -> luau::runtime::Result<Read>,
    ) -> luau::runtime::Result<Read> {
        if self.depth >= MAX_DEPTH {
            return Err(too_deep());
        }
        self.depth += 1;
        let result = read(self);
        self.depth -= 1;
        let mut read = result?;
        read.height += 1;
        Ok(read)
    }

    fn owned_table(&mut self, table: &luau::OwnedTable) -> luau::runtime::Result<Read> {
        if !table.array().is_empty() && !table.fields().is_empty() {
            return Err(mixed_keys());
        }

        let mut height = 0;
        if !table.array().is_empty() {
            self.hold(table.array().len())?;
            let mut items = Vec::with_capacity(table.array().len());
            for value in table.array() {
                let read = self.read_value(value)?;
                height = height.max(read.height);
                items.push(read.node);
            }
            return Ok(Read {
                node: Node::Array(items.into()),
                height,
            });
        }

        self.hold(table.fields().len())?;
        let mut entries = Vec::with_capacity(table.fields().len());
        for (key, value) in table.fields() {
            let key = self.key(key.as_bytes(), false)?;
            let read = self.read_value(value)?;
            height = height.max(read.height);
            entries.push((key, read.node));
        }
        Ok(Read {
            node: object(entries),
            height,
        })
    }

    fn table(&mut self, table: &luau::Table) -> luau::runtime::Result<Read> {
        let identity = table.identity()?;
        if let Some((node, height)) = self.tables.get(&identity) {
            if self.depth + height > MAX_DEPTH {
                return Err(too_deep());
            }
            return Ok(Read {
                node: node.clone(),
                height: *height,
            });
        }
        let read = self.nested(|reader| reader.table_entries(table))?;
        self.tables
            .insert(identity, (read.node.clone(), read.height));
        Ok(read)
    }

    fn table_entries(&mut self, table: &luau::Table) -> luau::runtime::Result<Read> {
        if is_empty_object_marker(table)? {
            return Ok(Read::leaf(Node::Object(Rc::new([]))));
        }

        enum Shape {
            Unknown,
            Array(Vec<(usize, Node)>),
            Object(Vec<(Rc<str>, Node)>),
        }

        let mut shape = Shape::Unknown;
        let mut height = 0;
        table.for_each_pair(|key, value| {
            self.hold(1)?;
            match (array_index(&key), key) {
                (Some(index), _) => {
                    let read = self.read_value_ref(value)?;
                    height = height.max(read.height);
                    match &mut shape {
                        Shape::Unknown => shape = Shape::Array(vec![(index, read.node)]),
                        Shape::Array(items) => items.push((index, read.node)),
                        Shape::Object(_) => return Err(mixed_keys()),
                    }
                }
                (None, luau::ValueRef::String(key)) => {
                    let key = self.key(key, true)?;
                    let read = self.read_value_ref(value)?;
                    height = height.max(read.height);
                    match &mut shape {
                        Shape::Unknown => shape = Shape::Object(vec![(key, read.node)]),
                        Shape::Object(entries) => entries.push((key, read.node)),
                        Shape::Array(_) => return Err(mixed_keys()),
                    }
                }
                (None, other) => {
                    return Err(luau::Error::Runtime(format!(
                        "JSON object keys must be strings or positive integers, got {}",
                        other.type_name()
                    )));
                }
            }
            Ok(ControlFlow::Continue(()))
        })?;

        let node = match shape {
            Shape::Unknown => Node::Array(Rc::new([])),
            Shape::Object(entries) => object(entries),
            Shape::Array(mut items) => {
                items.sort_by_key(|(index, _)| *index);
                if items
                    .iter()
                    .enumerate()
                    .any(|(offset, (index, _))| *index != offset + 1)
                {
                    return Err(luau::Error::Runtime(
                        "JSON array tables must use contiguous 1-based integer keys".into(),
                    ));
                }
                Node::Array(items.into_iter().map(|(_, node)| node).collect())
            }
        };
        Ok(Read { node, height })
    }
}

/// An object of `entries` in key order, the last of equal keys kept, as `serde_json::Map` keeps
/// them.
fn object(mut entries: Vec<(Rc<str>, Node)>) -> Node {
    entries.reverse();
    entries.sort_by(|(left, _), (right, _)| left.cmp(right));
    entries.dedup_by(|(next, _), (kept, _)| next == kept);
    Node::Object(entries.into())
}

fn number(value: f64) -> luau::runtime::Result<Read> {
    json_number_from_f64(value)
        .map(|number| Read::leaf(Node::Number(number)))
        .ok_or_else(|| luau::Error::Runtime("JSON numbers must be finite".into()))
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

fn array_index(key: &luau::ValueRef<'_>) -> Option<usize> {
    match key {
        luau::ValueRef::Integer(value) if *value > 0 => Some(*value as usize),
        luau::ValueRef::Number(value)
            if value.is_finite() && *value > 0.0 && value.fract() == 0.0 =>
        {
            Some(*value as usize)
        }
        _ => None,
    }
}

fn is_empty_object_marker(table: &luau::Table) -> luau::runtime::Result<bool> {
    let Some(metatable) = table.metatable()? else {
        return Ok(false);
    };
    Ok(matches!(
        metatable.get(EMPTY_OBJECT_MARKER)?,
        luau::Value::Boolean(true)
    ))
}

fn unsupported(type_name: &str) -> luau::Error {
    let what = match type_name {
        "buffer" => "buffers",
        "function" => "functions",
        "thread" => "threads",
        _ => "userdata",
    };
    luau::Error::Runtime(format!("{what} cannot be encoded as JSON"))
}

fn mixed_keys() -> luau::Error {
    luau::Error::Runtime("JSON tables cannot mix array and object keys".into())
}

fn too_many_values() -> luau::Error {
    luau::Error::Runtime(format!(
        "the value holds more than {MAX_CONVERTED_VALUES} values"
    ))
}

fn too_deep() -> luau::Error {
    luau::Error::Runtime("JSON value is too deeply nested".into())
}

#[cfg(test)]
mod tests {
    use harmony_luau as luau;

    use super::Reader;

    fn too_large() -> String {
        String::from("too large")
    }

    #[test]
    fn only_text_and_value_keys_go_uncharged() {
        let mut table = luau::OwnedTable::default();
        table.set_field(format!("${}", "x".repeat(2048)), 0.0);
        let error = Reader::new(1024, &too_large)
            .value(&luau::Value::TableData(table))
            .expect_err("the key alone outgrows the limit");
        assert_eq!(error.to_string(), "Luau runtime failed: too large");

        let mut table = luau::OwnedTable::default();
        table.set_field("$text", "body");
        table.set_field("$value", "body");
        assert!(
            Reader::new(8, &too_large)
                .value(&luau::Value::TableData(table))
                .is_ok()
        );
    }

    #[test]
    fn values_are_charged_before_a_table_is_held() {
        let table = luau::OwnedTable::from_array(std::iter::repeat_n(0.0, 1 << 21));
        let error = Reader::new(usize::MAX, &too_large)
            .value(&luau::Value::TableData(table))
            .expect_err("the table holds more values than a reader may");
        assert!(
            error.to_string().contains("holds more than 1048576 values"),
            "{error}"
        );
    }
}
