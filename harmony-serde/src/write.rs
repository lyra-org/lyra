// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Writes [`Node`]s as JSON, YAML, or XML into sinks that refuse past a byte limit, so a shared
//! table repeated in the output stops being written the moment the output grows too large.

use std::{
    cell::Cell,
    fmt,
    io,
};

use harmony_luau as luau;
use serde::{
    Serialize,
    Serializer,
    ser::{
        Error as _,
        SerializeMap,
        SerializeSeq,
    },
};

use crate::{
    node::Node,
    runtime_error,
};

/// What a write ran out of.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Spent {
    /// More than the byte limit, written or held.
    Bytes,
    /// More values than the meter allows.
    Values,
}

/// Counts what a write visits. Every format writes at least a byte for each value but XML, which
/// can write none for an empty list, so the meter also bounds the values visited. quick-xml holds
/// an element's children until the element closes, so for XML the meter also charges, as it
/// visits them, a lower bound of the markup and text it writes.
pub(crate) struct Meter {
    max_values: usize,
    values: Cell<usize>,
    markup: Option<Cell<usize>>,
    spent: Cell<Option<Spent>>,
}

impl Meter {
    pub(crate) fn new(max_values: usize) -> Self {
        Self {
            max_values,
            values: Cell::new(max_values),
            markup: None,
            spent: Cell::new(None),
        }
    }

    pub(crate) fn xml(max_values: usize, max_bytes: usize) -> Self {
        Self {
            markup: Some(Cell::new(max_bytes)),
            ..Self::new(max_values)
        }
    }

    pub(crate) fn spent(&self) -> Option<Spent> {
        self.spent.get()
    }

    fn take(&self, cell: &Cell<usize>, amount: usize, spent: Spent) -> Result<(), MeterSpent> {
        match cell.get().checked_sub(amount) {
            Some(left) => {
                cell.set(left);
                Ok(())
            }
            None => {
                self.spent.set(Some(spent));
                Err(MeterSpent)
            }
        }
    }

    fn value(&self) -> Result<(), MeterSpent> {
        self.take(&self.values, 1, Spent::Values)
    }

    fn markup(&self, bytes: usize) -> Result<(), MeterSpent> {
        match &self.markup {
            Some(cell) => self.take(cell, bytes, Spent::Bytes),
            None => Ok(()),
        }
    }
}

struct MeterSpent;

impl fmt::Display for MeterSpent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the value is too large")
    }
}

/// A node as a format writes it. `element` names the XML element the node is written as, which
/// a list repeats for each of its items.
pub(crate) struct Encoded<'a> {
    node: &'a Node,
    meter: &'a Meter,
    element: Option<&'a str>,
}

impl<'a> Encoded<'a> {
    pub(crate) fn new(node: &'a Node, meter: &'a Meter, element: Option<&'a str>) -> Self {
        Self {
            node,
            meter,
            element,
        }
    }
}

/// The least XML writes for `text`: `&` and `<` are always escaped.
fn xml_text_width(text: &str) -> usize {
    text.len()
        + text
            .bytes()
            .map(|byte| match byte {
                b'&' => 4,
                b'<' => 3,
                _ => 0,
            })
            .sum::<usize>()
}

/// How wide quick-xml writes `number`: with `Display`, as the primitive it serializes as.
fn display_width(number: &serde_json::Number) -> usize {
    struct Width(usize);

    impl fmt::Write for Width {
        fn write_str(&mut self, text: &str) -> fmt::Result {
            self.0 += text.len();
            Ok(())
        }
    }

    let mut width = Width(0);
    let written = if let Some(value) = number.as_u64() {
        fmt::write(&mut width, format_args!("{value}"))
    } else if let Some(value) = number.as_i64() {
        fmt::write(&mut width, format_args!("{value}"))
    } else {
        fmt::write(
            &mut width,
            format_args!("{}", number.as_f64().unwrap_or_default()),
        )
    };
    written.expect("counting never fails");
    width.0
}

impl Serialize for Encoded<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let charge = |result: Result<(), MeterSpent>| result.map_err(S::Error::custom);
        let meter = self.meter;
        charge(meter.value())?;
        if let Some(name) = self.element
            && !matches!(self.node, Node::Array(_))
        {
            // `<name/>`, the least an element takes.
            charge(meter.markup(name.len() + 3))?;
        }
        match self.node {
            Node::Null => serializer.serialize_unit(),
            Node::Bool(value) => {
                charge(meter.markup(if *value { 4 } else { 5 }))?;
                serializer.serialize_bool(*value)
            }
            Node::Number(number) => {
                if meter.markup.is_some() {
                    charge(meter.markup(display_width(number)))?;
                }
                number.serialize(serializer)
            }
            Node::String(text) => {
                charge(meter.markup(xml_text_width(text)))?;
                serializer.serialize_str(text)
            }
            Node::Array(items) => {
                let mut seq = serializer.serialize_seq(Some(items.len()))?;
                for item in items.iter() {
                    seq.serialize_element(&Encoded::new(item, meter, self.element))?;
                }
                seq.end()
            }
            Node::Object(entries) => {
                let mut map = serializer.serialize_map(Some(entries.len()))?;
                for (key, value) in entries.iter() {
                    let element = match key.as_bytes().first() {
                        Some(b'@') => {
                            // ` name=""`.
                            charge(meter.markup(key.len() + 3))?;
                            None
                        }
                        Some(b'$') => None,
                        _ => Some(&**key),
                    };
                    map.serialize_entry(&**key, &Encoded::new(value, meter, element))?;
                }
                map.end()
            }
        }
    }
}

/// Bytes written up to a limit; a write past it fails and marks the sink full.
pub(crate) struct Sink<B> {
    out: B,
    remaining: usize,
    full: bool,
}

impl<B> Sink<B> {
    pub(crate) fn new(out: B, max_bytes: usize) -> Self {
        Self {
            out,
            remaining: max_bytes,
            full: false,
        }
    }

    fn take(&mut self, bytes: usize) -> bool {
        match self.remaining.checked_sub(bytes) {
            Some(left) => {
                self.remaining = left;
                true
            }
            None => {
                self.full = true;
                false
            }
        }
    }
}

impl io::Write for Sink<&mut Vec<u8>> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if !self.take(buf.len()) {
            return Err(io::Error::other("the output is full"));
        }
        self.out.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Counts what is written without keeping it.
impl io::Write for Sink<()> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if !self.take(buf.len()) {
            return Err(io::Error::other("the output is full"));
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl fmt::Write for Sink<String> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        if !self.take(text.len()) {
            return Err(fmt::Error);
        }
        self.out.push_str(text);
        Ok(())
    }
}

/// Settles a write: a full sink or spent markup fails with `too_large`, too many steps with
/// its own message, and any other failure as the format reported it.
fn settle<T, E: fmt::Display>(
    result: Result<T, E>,
    full: bool,
    meter: &Meter,
    too_large: &dyn Fn() -> String,
    context: &str,
) -> luau::runtime::Result<T> {
    result.map_err(|error| match meter.spent() {
        Some(Spent::Values) => luau::Error::Runtime(format!(
            "the value takes more than {} steps to encode",
            meter.max_values
        )),
        Some(Spent::Bytes) => luau::Error::Runtime(too_large()),
        None if full => luau::Error::Runtime(too_large()),
        None => runtime_error(context, error),
    })
}

/// Appends `node` to `out` as JSON, failing once `out` would grow past `max_len`.
pub(crate) fn json(
    out: &mut Vec<u8>,
    node: &Node,
    max_len: usize,
    too_large: &dyn Fn() -> String,
) -> luau::runtime::Result<()> {
    let room = max_len.saturating_sub(out.len());
    let meter = Meter::new(room);
    let mut sink = Sink::new(out, room);
    let result = serde_json::to_writer(&mut sink, &Encoded::new(node, &meter, None));
    settle(result, sink.full, &meter, too_large, "JSON encode failed")
}

pub(crate) fn yaml(
    node: &Node,
    max_bytes: usize,
    too_large: &dyn Fn() -> String,
) -> luau::runtime::Result<String> {
    let meter = Meter::new(max_bytes);
    let mut sink = Sink::new(String::new(), max_bytes);
    let result = serde_saphyr::to_fmt_writer(&mut sink, &Encoded::new(node, &meter, None));
    settle(result, sink.full, &meter, too_large, "YAML encode failed")?;
    Ok(sink.out)
}

pub(crate) fn xml(
    root: &str,
    node: &Node,
    max_bytes: usize,
    too_large: &dyn Fn() -> String,
) -> luau::runtime::Result<String> {
    let meter = Meter::xml(max_bytes, max_bytes);
    let mut sink = Sink::new(String::new(), max_bytes);
    let result = quick_xml::se::to_writer_with_root(
        &mut sink,
        root,
        &Encoded::new(node, &meter, Some(root)),
    );
    settle(result, sink.full, &meter, too_large, "XML encode failed")?;
    Ok(sink.out)
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use super::{
        Encoded,
        Meter,
        Sink,
        Spent,
    };
    use crate::node::Node;

    #[test]
    fn xml_meter_charges_numbers_as_written() {
        let mut node = Node::Number(serde_json::Number::from_f64(1e-300).expect("finite"));
        for _ in 0..12 {
            node = Node::Object(Rc::from([
                (Rc::from("a"), node.clone()),
                (Rc::from("b"), node),
            ]));
        }
        // 4096 numbers of over 300 bytes each: well past the meter, though only 4096 bytes if
        // each were charged one.
        let meter = Meter::xml(usize::MAX, 64 * 1024);
        let mut sink = Sink::new(String::new(), usize::MAX);
        let result = quick_xml::se::to_writer_with_root(
            &mut sink,
            "root",
            &Encoded::new(&node, &meter, Some("root")),
        );
        assert!(result.is_err());
        assert!(meter.spent() == Some(Spent::Bytes));
        assert!(
            sink.out.len() < 1024,
            "the meter stops XML before it buffers much: {} bytes",
            sink.out.len()
        );
    }
}
