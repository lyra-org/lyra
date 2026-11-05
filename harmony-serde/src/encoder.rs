// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Encoders that write one JSON or XML document a piece at a time, so a large document never has
//! to exist whole as Luau values or as one string.

use std::{
    ops::ControlFlow,
    rc::Rc,
    sync::{
        Arc,
        Mutex,
    },
};

use harmony_luau as luau;

use crate::{
    node::{
        Node,
        Reader,
    },
    runtime_error,
    write,
};

/// The most an encoder holds before it must be flushed.
pub const MAX_BUFFERED_BYTES: usize = 4 * 1024 * 1024;

fn overfull() -> String {
    format!("the encoder holds more than {MAX_BUFFERED_BYTES} bytes; flush it more often")
}

/// What every encoder shares: its bytes, and whether it can still go on. Any failed operation
/// poisons the encoder, so a caller that catches the error can't go on to finish a document that
/// looks whole but lacks what failed.
#[derive(Default)]
struct Output {
    bytes: Vec<u8>,
    failure: Option<String>,
    finished: bool,
    complete: bool,
}

impl Output {
    fn usable(&self) -> luau::runtime::Result<()> {
        if let Some(failure) = &self.failure {
            return Err(luau::Error::Runtime(format!(
                "the encoder failed earlier: {failure}"
            )));
        }
        if self.finished {
            return Err(luau::Error::Runtime("the encoder is finished".into()));
        }
        Ok(())
    }

    /// What the encoder can still take before it holds too much.
    fn room(&self) -> usize {
        MAX_BUFFERED_BYTES.saturating_sub(self.bytes.len())
    }

    /// A reader for one call's values, refusing once they could not fit.
    fn reader(&self) -> Reader<'static> {
        Reader::new(self.room(), &overfull)
    }

    /// Poisons the encoder when `result` failed or left too much buffered.
    fn settle<T>(&mut self, result: luau::runtime::Result<T>) -> luau::runtime::Result<T> {
        let result = result.and_then(|value| {
            if self.bytes.len() > MAX_BUFFERED_BYTES {
                return Err(luau::Error::Runtime(overfull()));
            }
            Ok(value)
        });
        if let Err(error) = &result {
            self.failure = Some(error.to_string());
            self.bytes = Vec::new();
        }
        result
    }

    fn flush(&mut self) -> luau::runtime::Result<luau::ByteString> {
        self.usable()?;
        Ok(luau::ByteString(std::mem::take(&mut self.bytes)))
    }

    fn finish(&mut self, open: usize) -> luau::runtime::Result<luau::ByteString> {
        self.usable()?;
        if open > 0 || !self.complete {
            return self.settle(Err(luau::Error::Runtime(
                "the document is not complete: a container is still open".into(),
            )));
        }
        self.finished = true;
        Ok(luau::ByteString(std::mem::take(&mut self.bytes)))
    }
}

/// The parts of an encoder's state an operation works on.
trait EncoderState {
    fn output(&mut self) -> &mut Output;
}

/// Runs `operation` on the encoder's state unless the encoder failed, poisoning it when the
/// operation fails.
fn apply<S: EncoderState, T>(
    state: &Mutex<S>,
    operation: impl FnOnce(&mut S) -> luau::runtime::Result<T>,
) -> luau::runtime::Result<T> {
    let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
    state.output().usable()?;
    let result = operation(&mut state);
    state.output().settle(result)
}

fn no_document_after_the_root(output: &Output) -> luau::runtime::Result<()> {
    if output.complete {
        return Err(luau::Error::Runtime(
            "the document already has its root".into(),
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum JsonContainer {
    Object,
    Array,
}

struct JsonFrame {
    container: JsonContainer,
    empty: bool,
}

#[derive(Default)]
struct JsonState {
    output: Output,
    stack: Vec<JsonFrame>,
}

impl EncoderState for JsonState {
    fn output(&mut self) -> &mut Output {
        &mut self.output
    }
}

impl JsonState {
    /// Writes what goes before a value: a separator, and the key inside an object.
    fn begin_value(&mut self, key: Option<&str>) -> luau::runtime::Result<()> {
        let Some(frame) = self.stack.last_mut() else {
            if key.is_some() {
                return Err(luau::Error::Runtime("a key needs an open object".into()));
            }
            return no_document_after_the_root(&self.output);
        };
        match (frame.container, key) {
            (JsonContainer::Object, None) => {
                return Err(luau::Error::Runtime(
                    "a value inside an object needs a key".into(),
                ));
            }
            (JsonContainer::Array, Some(_)) => {
                return Err(luau::Error::Runtime(
                    "a value inside an array takes no key".into(),
                ));
            }
            _ => {}
        }
        if !std::mem::replace(&mut frame.empty, false) {
            self.output.bytes.push(b',');
        }
        if let Some(key) = key {
            write::json(
                &mut self.output.bytes,
                &Node::String(Rc::from(key)),
                MAX_BUFFERED_BYTES,
                &overfull,
            )?;
            self.output.bytes.push(b':');
        }
        Ok(())
    }

    fn value(&mut self, key: Option<&str>, node: &Node) -> luau::runtime::Result<()> {
        self.begin_value(key)?;
        write::json(&mut self.output.bytes, node, MAX_BUFFERED_BYTES, &overfull)?;
        if self.stack.is_empty() {
            self.output.complete = true;
        }
        Ok(())
    }

    fn begin(&mut self, key: Option<&str>, container: JsonContainer) -> luau::runtime::Result<()> {
        self.begin_value(key)?;
        self.output.bytes.push(match container {
            JsonContainer::Object => b'{',
            JsonContainer::Array => b'[',
        });
        self.stack.push(JsonFrame {
            container,
            empty: true,
        });
        Ok(())
    }

    fn end(&mut self, container: JsonContainer) -> luau::runtime::Result<()> {
        match self.stack.last() {
            Some(frame) if frame.container == container => {}
            _ => {
                return Err(luau::Error::Runtime(match container {
                    JsonContainer::Object => "no object is open".into(),
                    JsonContainer::Array => "no array is open".into(),
                }));
            }
        }
        self.stack.pop();
        self.output.bytes.push(match container {
            JsonContainer::Object => b'}',
            JsonContainer::Array => b']',
        });
        if self.stack.is_empty() {
            self.output.complete = true;
        }
        Ok(())
    }

    fn require(&self, container: JsonContainer) -> luau::runtime::Result<()> {
        match self.stack.last() {
            Some(frame) if frame.container == container => Ok(()),
            _ => Err(luau::Error::Runtime(match container {
                JsonContainer::Object => "fields need an open object".into(),
                JsonContainer::Array => "items need an open array".into(),
            })),
        }
    }
}

fn require_list(values: &luau::Value) -> luau::runtime::Result<()> {
    match values {
        luau::Value::Table(_) | luau::Value::TableData(_) => Ok(()),
        other => Err(luau::Error::Runtime(format!(
            "expected a list of values, got {}",
            other.type_name()
        ))),
    }
}

/// Reads each value of a sequence table, in order, and hands it to `visit` until it breaks.
fn each_item(
    values: &luau::Value,
    reader: &mut Reader<'_>,
    mut visit: impl FnMut(Node) -> luau::runtime::Result<ControlFlow<()>>,
) -> luau::runtime::Result<()> {
    match values {
        luau::Value::Table(table) => {
            table.for_each_array_value(|value| visit(reader.value_ref(value)?))
        }
        luau::Value::TableData(table) => {
            for value in table.array() {
                if visit(reader.value(value)?)?.is_break() {
                    break;
                }
            }
            Ok(())
        }
        other => require_list(other),
    }
}

/// Goes on to the next item while the encoder can still take more.
fn while_room(output: &Output) -> ControlFlow<()> {
    if output.bytes.len() > MAX_BUFFERED_BYTES {
        ControlFlow::Break(())
    } else {
        ControlFlow::Continue(())
    }
}

/// A call's arguments, taken whole so that reading them, a missing one included, fails inside the
/// encoder's poisoning like the rest of the call.
struct Arguments(Vec<luau::Value>);

impl Arguments {
    fn required(&self, index: usize, name: &str) -> luau::runtime::Result<&luau::Value> {
        self.0
            .get(index)
            .ok_or_else(|| luau::Error::Runtime(format!("argument `{name}` is missing")))
    }

    fn optional(&self, index: usize) -> &luau::Value {
        self.0.get(index).unwrap_or(&luau::Value::Nil)
    }
}

/// A key or name argument, read inside the encoder's poisoning like the rest of the call.
fn text(value: &luau::Value, what: &str) -> luau::runtime::Result<String> {
    match value {
        luau::Value::String(bytes) => String::from_utf8(bytes.clone())
            .map_err(|error| runtime_error(&format!("{what} must be valid UTF-8"), error)),
        other => Err(luau::Error::Runtime(format!(
            "{what} must be a string, got {}",
            other.type_name()
        ))),
    }
}

fn optional_text(value: &luau::Value, what: &str) -> luau::runtime::Result<Option<String>> {
    match value {
        luau::Value::Nil => Ok(None),
        value => text(value, what).map(Some),
    }
}

#[harmony_macros::userdata(
    name = "JsonEncoder",
    description = "Writes one JSON document a piece at a time. Containers open and close in order, \
                   values inside an object take a key, and the encoder separates and escapes them. \
                   `flush` hands over what it has written so far; it raises once more than \
                   `MAX_BUFFERED_BYTES` is left unflushed. A failed call leaves the encoder failed: every later call \
                   raises, so a caught error can't lead to a document that looks whole."
)]
#[derive(Clone, Default)]
pub struct JsonEncoder {
    state: Arc<Mutex<JsonState>>,
}

#[harmony_macros::userdata_methods]
impl JsonEncoder {
    #[harmony(
        description = "Opens an object: the document's root without a key, under `key` inside an object, or as an item inside an array.",
        args(key: Option<String>)
    )]
    fn begin_object(&self, args: luau::Variadic<luau::Value>) -> luau::runtime::Result<()> {
        let args = Arguments(args.0);
        apply(&self.state, |state| {
            let key = optional_text(args.optional(0), "an object key")?;
            state.begin(key.as_deref(), JsonContainer::Object)
        })
    }

    #[harmony(description = "Closes the open object.")]
    fn end_object(&self) -> luau::runtime::Result<()> {
        apply(&self.state, |state| state.end(JsonContainer::Object))
    }

    #[harmony(
        description = "Opens an array: the document's root without a key, under `key` inside an object, or as an item inside an array.",
        args(key: Option<String>)
    )]
    fn begin_array(&self, args: luau::Variadic<luau::Value>) -> luau::runtime::Result<()> {
        let args = Arguments(args.0);
        apply(&self.state, |state| {
            let key = optional_text(args.optional(0), "an object key")?;
            state.begin(key.as_deref(), JsonContainer::Array)
        })
    }

    #[harmony(description = "Closes the open array.")]
    fn end_array(&self) -> luau::runtime::Result<()> {
        apply(&self.state, |state| state.end(JsonContainer::Array))
    }

    #[harmony(
        description = "Writes `value` under `key` in the open object.",
        args(key: String, value: luau::JsonValue)
    )]
    fn field(&self, args: luau::Variadic<luau::Value>) -> luau::runtime::Result<()> {
        let args = Arguments(args.0);
        apply(&self.state, |state| {
            let key = text(args.required(0, "key")?, "an object key")?;
            let value = args.required(1, "value")?;
            state.require(JsonContainer::Object)?;
            let node = state.output.reader().value(value)?;
            state.value(Some(&key), &node)
        })
    }

    #[harmony(
        description = "Writes each of `values`, in order, as the next items of the open array.",
        args(values: Vec<luau::JsonValue>)
    )]
    fn items(&self, args: luau::Variadic<luau::Value>) -> luau::runtime::Result<()> {
        let args = Arguments(args.0);
        apply(&self.state, |state| {
            let values = args.required(0, "values")?;
            require_list(values)?;
            state.require(JsonContainer::Array)?;
            let mut reader = state.output.reader();
            each_item(values, &mut reader, |node| {
                state.value(None, &node)?;
                Ok(while_room(&state.output))
            })
        })
    }

    #[harmony(
        description = "Hands over what the encoder has written since the last flush, and forgets it."
    )]
    fn flush(&self) -> luau::runtime::Result<luau::ByteString> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.output.flush()
    }

    #[harmony(
        description = "Hands over the rest of the document, raising when it isn't complete. The encoder takes no more calls after."
    )]
    fn finish(&self) -> luau::runtime::Result<luau::ByteString> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let open = state.stack.len();
        state.output.finish(open)
    }
}

struct XmlFrame {
    name: String,
    /// The start tag still lacks its `>`, so the element can still close as `<name/>`.
    start_open: bool,
}

#[derive(Default)]
struct XmlState {
    output: Output,
    stack: Vec<XmlFrame>,
}

impl EncoderState for XmlState {
    fn output(&mut self) -> &mut Output {
        &mut self.output
    }
}

impl XmlState {
    /// Makes room for a child: no second root, and the parent's start tag closed.
    fn begin_child(&mut self) -> luau::runtime::Result<()> {
        match self.stack.last_mut() {
            None => no_document_after_the_root(&self.output),
            Some(frame) => {
                if std::mem::replace(&mut frame.start_open, false) {
                    self.output.bytes.push(b'>');
                }
                Ok(())
            }
        }
    }

    fn element(&mut self, name: &str, node: &Node) -> luau::runtime::Result<()> {
        let xml = write::xml(name, node, self.output.room(), &overfull)?;
        self.begin_child()?;
        self.output.bytes.extend_from_slice(xml.as_bytes());
        if self.stack.is_empty() {
            self.output.complete = true;
        }
        Ok(())
    }

    fn begin_element(
        &mut self,
        name: String,
        attributes: &luau::Value,
    ) -> luau::runtime::Result<()> {
        let fields = attributes_node(attributes, &mut self.output.reader())?;
        // The serializer writes the start tag as it would inside a whole document, escaping
        // included, and closes it as an empty element.
        let empty = write::xml(&name, &fields, self.output.room(), &overfull)?;
        let start = empty.strip_suffix("/>").ok_or_else(|| {
            luau::Error::Runtime(format!("XML encode failed: unexpected start tag {empty}"))
        })?;
        self.begin_child()?;
        self.output.bytes.extend_from_slice(start.as_bytes());
        self.stack.push(XmlFrame {
            name,
            start_open: true,
        });
        Ok(())
    }

    fn end_element(&mut self) -> luau::runtime::Result<()> {
        let frame = self
            .stack
            .pop()
            .ok_or_else(|| luau::Error::Runtime("no element is open".into()))?;
        if frame.start_open {
            self.output.bytes.extend_from_slice(b"/>");
        } else {
            self.output.bytes.extend_from_slice(b"</");
            self.output.bytes.extend_from_slice(frame.name.as_bytes());
            self.output.bytes.push(b'>');
        }
        if self.stack.is_empty() {
            self.output.complete = true;
        }
        Ok(())
    }
}

/// An attribute table as the object `xml.encode` writes as attributes: each name prefixed with
/// `@`, in name order. Each value must be a string, number, or boolean, and is checked before it
/// is read.
fn attributes_node(
    attributes: &luau::Value,
    reader: &mut Reader<'_>,
) -> luau::runtime::Result<Node> {
    let mut fields: Vec<(Rc<str>, Node)> = Vec::new();
    let mut attribute = |reader: &mut Reader<'_>,
                         name: luau::ValueRef<'_>,
                         value: luau::ValueRef<'_>|
     -> luau::runtime::Result<()> {
        let luau::ValueRef::String(name) = name else {
            return Err(luau::Error::Runtime(format!(
                "attribute names must be strings, got {}",
                name.type_name()
            )));
        };
        let name = reader.string(
            name,
            true,
            name.len() + 4,
            "attribute names must be valid UTF-8",
        )?;
        if !matches!(
            value,
            luau::ValueRef::String(_)
                | luau::ValueRef::Integer(_)
                | luau::ValueRef::Number(_)
                | luau::ValueRef::Boolean(_)
        ) {
            return Err(luau::Error::Runtime(format!(
                "attribute {name} must be a string, number or boolean"
            )));
        }
        fields.push((Rc::from(format!("@{name}")), reader.value_ref(value)?));
        Ok(())
    };
    match attributes {
        luau::Value::Nil => {}
        luau::Value::Table(table) => table.for_each_pair(|name, value| {
            attribute(reader, name, value)?;
            Ok(ControlFlow::Continue(()))
        })?,
        luau::Value::TableData(table) => {
            for (name, value) in table.fields() {
                attribute(
                    reader,
                    luau::ValueRef::String(name.as_bytes()),
                    owned_scalar(value),
                )?;
            }
        }
        other => {
            return Err(luau::Error::Runtime(format!(
                "attributes must be a table, got {}",
                other.type_name()
            )));
        }
    }
    fields.sort_by(|(left, _), (right, _)| left.cmp(right));
    Ok(Node::Object(fields.into()))
}

/// An owned attribute value as a [`luau::ValueRef`], for the scalar check: anything else reads
/// as the table it cannot be.
fn owned_scalar(value: &luau::Value) -> luau::ValueRef<'_> {
    match value {
        luau::Value::String(bytes) => luau::ValueRef::String(bytes),
        luau::Value::Integer(value) => luau::ValueRef::Integer(*value),
        luau::Value::Number(value) => luau::ValueRef::Number(*value),
        luau::Value::Boolean(value) => luau::ValueRef::Boolean(*value),
        _ => luau::ValueRef::Nil,
    }
}

#[harmony_macros::userdata(
    name = "XmlEncoder",
    description = "Writes one XML document a piece at a time. Elements open and close in order; an \
                   element closed with nothing inside it closes as `<name/>`. Values convert as \
                   `xml.encode` converts them. `flush` hands over what it has written so far; it \
                   raises once more than `MAX_BUFFERED_BYTES` is left unflushed. A failed call leaves the encoder \
                   failed: every later call raises, so a caught error can't lead to a document that \
                   looks whole."
)]
#[derive(Clone, Default)]
pub struct XmlEncoder {
    state: Arc<Mutex<XmlState>>,
}

#[harmony_macros::userdata_methods]
impl XmlEncoder {
    #[harmony(
        description = "Opens the element `name` with `attributes`, in name order.",
        args(name: String, attributes: Option<std::collections::HashMap<String, luau::JsonValue>>)
    )]
    fn begin_element(&self, args: luau::Variadic<luau::Value>) -> luau::runtime::Result<()> {
        let args = Arguments(args.0);
        apply(&self.state, |state| {
            let name = text(args.required(0, "name")?, "an element name")?;
            state.begin_element(name, args.optional(1))
        })
    }

    #[harmony(description = "Closes the open element.")]
    fn end_element(&self) -> luau::runtime::Result<()> {
        apply(&self.state, XmlState::end_element)
    }

    #[harmony(
        description = "Writes each of `values`, in order, as the element `name`, as `xml.encode(name, value)` would.",
        args(name: String, values: Vec<luau::JsonValue>)
    )]
    fn elements(&self, args: luau::Variadic<luau::Value>) -> luau::runtime::Result<()> {
        let args = Arguments(args.0);
        apply(&self.state, |state| {
            let name = text(args.required(0, "name")?, "an element name")?;
            let values = args.required(1, "values")?;
            let mut reader = state.output.reader();
            each_item(values, &mut reader, |node| {
                state.element(&name, &node)?;
                Ok(while_room(&state.output))
            })
        })
    }

    #[harmony(
        description = "Hands over what the encoder has written since the last flush, and forgets it."
    )]
    fn flush(&self) -> luau::runtime::Result<luau::ByteString> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.output.flush()
    }

    #[harmony(
        description = "Hands over the rest of the document, raising when it isn't complete. The encoder takes no more calls after."
    )]
    fn finish(&self) -> luau::runtime::Result<luau::ByteString> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let open = state.stack.len();
        state.output.finish(open)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use harmony_luau as luau;

    use crate::SerdeModule;

    fn eval(source: &str) -> luau::runtime::Result<Vec<luau::Value>> {
        let vm = luau::Vm::new()?;
        vm.open_standard_libraries(luau::StandardLibraries::all_supported())?;
        let spec = SerdeModule::module_spec();
        let table =
            harmony_core::luau::install_module(&vm, &harmony_core::ChunkOrigin::default(), &spec)?;
        vm.set_global_table("serde", &table)?;
        vm.eval(
            Arc::<[u8]>::from(source.as_bytes()),
            luau::ChunkOrigin::default(),
        )
    }

    fn strings(values: Vec<luau::Value>) -> Vec<String> {
        values
            .into_iter()
            .map(|value| match value {
                luau::Value::String(bytes) => String::from_utf8(bytes).expect("UTF-8"),
                other => panic!("expected a string, got {}", other.type_name()),
            })
            .collect()
    }

    #[test]
    fn json_pieces_match_a_whole_encode() -> luau::runtime::Result<()> {
        let [pieces, whole] = <[String; 2]>::try_from(strings(eval(
            r#"
            local items = {
                { id = "a", n = 1, tags = { "x", "y" }, empty = serde.empty_object() },
                { id = "b\"\n", n = 2.5, tags = {} },
                { id = "c" },
            }
            local enc = serde.json.encoder()
            local out = {}
            enc:begin_object()
            enc:field("before", true)
            enc:begin_object("list")
            enc:begin_array("song")
            enc:items({ items[1] })
            table.insert(out, enc:flush())
            enc:items({ items[2], items[3] })
            enc:end_array()
            enc:end_object()
            enc:field("z", "end")
            enc:end_object()
            table.insert(out, enc:finish())
            local whole = serde.json.encode({ before = true, list = { song = items }, z = "end" })
            return table.concat(out), whole
            "#,
        )?))
        .expect("two strings");
        assert_eq!(pieces, whole);
        Ok(())
    }

    #[test]
    fn xml_pieces_match_a_whole_encode() -> luau::runtime::Result<()> {
        let [pieces, whole, empty] = <[String; 3]>::try_from(strings(eval(
            r#"
            local items = {
                { ["@id"] = "a&b", ["@n"] = 1, genre = { { ["$text"] = "Rock" } } },
                { ["@id"] = "<c>", empty = serde.empty_object() },
            }
            local enc = serde.xml.encoder()
            local out = {}
            enc:begin_element("root", { xmlns = "urn:x", status = "ok", flag = true })
            enc:begin_element("result")
            enc:elements("song", { items[1] })
            table.insert(out, enc:flush())
            enc:elements("song", { items[2] })
            enc:end_element()
            enc:end_element()
            table.insert(out, enc:finish())
            local whole = serde.xml.encode("root", {
                ["@xmlns"] = "urn:x",
                ["@status"] = "ok",
                ["@flag"] = true,
                result = { song = items },
            })

            local empty = serde.xml.encoder()
            empty:begin_element("root", { status = "ok" })
            empty:begin_element("result")
            local first = empty:flush()
            empty:end_element()
            empty:end_element()
            return table.concat(out), whole, first .. empty:finish()
            "#,
        )?))
        .expect("three strings");
        assert_eq!(pieces, whole);
        assert_eq!(empty, r#"<root status="ok"><result/></root>"#);
        Ok(())
    }

    #[test]
    fn a_failed_call_poisons_the_encoder() -> luau::runtime::Result<()> {
        let messages = strings(eval(
            r#"
            local results = {}
            local function try(f, ...)
                local ok, err = pcall(f, ...)
                table.insert(results, if ok then "ok" else tostring(err))
            end
            local enc = serde.json.encoder()
            enc:begin_object()
            enc:begin_array("items")
            try(enc.items, enc, { 0 / 0 })
            try(enc.end_array, enc)
            try(enc.flush, enc)

            local unfinished = serde.json.encoder()
            unfinished:begin_array()
            try(unfinished.finish, unfinished)
            try(unfinished.end_array, unfinished)

            local wrong = serde.json.encoder()
            wrong:begin_object()
            try(wrong.items, wrong, { 1 })

            local deep = {}
            local node = deep
            for _ = 1, 200 do
                node.child = {}
                node = node.child
            end
            local xml = serde.xml.encoder()
            xml:begin_element("root")
            try(xml.elements, xml, "deep", { deep })
            try(xml.end_element, xml)

            local big = serde.json.encoder()
            big:begin_array()
            try(big.items, big, { string.rep("x", 5 * 1024 * 1024) })
            try(big.end_array, big)

            local not_a_list = serde.json.encoder()
            not_a_list:begin_array()
            try(not_a_list.items, not_a_list, "invalid")
            try(not_a_list.end_array, not_a_list)

            local bad_key = serde.json.encoder()
            bad_key:begin_object()
            try(bad_key.field, bad_key, "\255", 1)
            try(bad_key.end_object, bad_key)

            local bad_name = serde.xml.encoder()
            try(bad_name.begin_element, bad_name, "\255")
            try(bad_name.elements, bad_name, "ok", { {} })
            return table.unpack(results)
            "#,
        )?);
        assert!(messages[0].contains("finite"), "{}", messages[0]);
        for message in [&messages[1], &messages[2]] {
            assert!(message.contains("failed earlier"), "{message}");
        }
        assert!(messages[3].contains("not complete"), "{}", messages[3]);
        assert!(messages[4].contains("failed earlier"), "{}", messages[4]);
        assert!(
            messages[5].contains("items need an open array"),
            "{}",
            messages[5]
        );
        assert!(messages[6].contains("too deeply nested"), "{}", messages[6]);
        assert!(messages[7].contains("failed earlier"), "{}", messages[7]);
        assert!(
            messages[8].contains("flush it more often"),
            "{}",
            messages[8]
        );
        assert!(messages[9].contains("failed earlier"), "{}", messages[9]);
        assert!(
            messages[10].contains("a list of values"),
            "{}",
            messages[10]
        );
        assert!(messages[11].contains("failed earlier"), "{}", messages[11]);
        assert!(messages[12].contains("valid UTF-8"), "{}", messages[12]);
        assert!(messages[13].contains("failed earlier"), "{}", messages[13]);
        assert!(messages[14].contains("valid UTF-8"), "{}", messages[14]);
        assert!(messages[15].contains("failed earlier"), "{}", messages[15]);
        assert_eq!(messages.len(), 16);
        Ok(())
    }

    #[test]
    fn a_missing_argument_poisons_the_encoder() -> luau::runtime::Result<()> {
        let messages = strings(eval(
            r#"
            local results = {}
            local function try(f, ...)
                local ok, err = pcall(f, ...)
                table.insert(results, if ok then "ok" else tostring(err))
            end
            -- Each call leaves out a required argument, then the document's end is tried.
            local function json(container, call)
                local enc = serde.json.encoder()
                enc[`begin_{container}`](enc)
                try(call, enc)
                try(enc[`end_{container}`], enc)
                try(enc.flush, enc)
                try(enc.finish, enc)
            end
            json("array", function(enc) enc:items() end)
            json("object", function(enc) enc:field() end)
            json("object", function(enc) enc:field("key") end)
            local function xml(call)
                local enc = serde.xml.encoder()
                enc:begin_element("root")
                try(call, enc)
                try(enc.end_element, enc)
                try(enc.flush, enc)
                try(enc.finish, enc)
            end
            xml(function(enc) enc:begin_element() end)
            xml(function(enc) enc:elements() end)
            xml(function(enc) enc:elements("item") end)
            return table.unpack(results)
            "#,
        )?);
        let missing = ["values", "key", "value", "name", "name", "values"];
        assert_eq!(messages.len(), missing.len() * 4);
        for (calls, argument) in messages.chunks(4).zip(missing) {
            assert!(
                calls[0].contains(&format!("argument `{argument}` is missing")),
                "{}",
                calls[0]
            );
            for later in &calls[1..] {
                assert!(later.contains("failed earlier"), "{later}");
            }
        }
        Ok(())
    }

    #[test]
    fn the_module_states_the_buffer_limit() -> luau::runtime::Result<()> {
        assert_eq!(
            eval("return serde.MAX_BUFFERED_BYTES")?,
            vec![luau::Value::Number(super::MAX_BUFFERED_BYTES as f64)]
        );
        Ok(())
    }

    #[test]
    fn a_document_has_one_root() -> luau::runtime::Result<()> {
        let messages = strings(eval(
            r#"
            local enc = serde.json.encoder()
            enc:begin_array()
            enc:end_array()
            local ok, err = pcall(enc.begin_object, enc)
            local xml = serde.xml.encoder()
            xml:elements("a", { { ["@x"] = 1 } })
            local xml_ok, xml_err = pcall(xml.elements, xml, "b", { {} })
            return tostring(err), tostring(xml_err)
            "#,
        )?);
        for message in &messages {
            assert!(message.contains("already has its root"), "{message}");
        }
        Ok(())
    }
}
