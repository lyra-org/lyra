// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! `.luaurc` files, read and written the way Luau reads them.
//!
//! Luau's grammar is its own JSON-like subset rather than JSON5: keys are
//! quoted strings, values are strings, `true`, `false`, objects, or arrays of
//! strings, and `//` comments and trailing commas are allowed. Strings may use
//! either quote and are taken verbatim, without decoding escapes, so every
//! string held here is exactly the text between its quotes.

use std::{
    collections::{
        BTreeMap,
        BTreeSet,
        btree_map::Entry,
    },
    fmt,
};

/// Objects nested deeper than this are refused; Luau gives no key below the
/// second level a meaning.
const MAX_DEPTH: usize = 64;

/// Parsed `.luaurc` contents.
///
/// Rewriting a file normalizes its formatting and drops its comments.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LuaurcConfig {
    pub aliases: BTreeMap<String, String>,
    pub globals: BTreeSet<String>,
    pub other: BTreeMap<String, LuaurcValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LuaurcValue {
    String(String),
    Bool(bool),
    Array(Vec<String>),
    Object(BTreeMap<String, LuaurcValue>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LuaurcError {
    Syntax {
        line: usize,
        message: String,
    },
    Shape {
        key: &'static str,
        expected: &'static str,
    },
    /// A string that no quote can hold, since Luau decodes no escapes.
    Unquotable(String),
}

impl fmt::Display for LuaurcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Syntax { line, message } => write!(f, "line {line}: {message}"),
            Self::Shape { key, expected } => write!(f, "`{key}` must be {expected}"),
            Self::Unquotable(text) => write!(f, "{text:?} cannot be written as a .luaurc string"),
        }
    }
}

impl std::error::Error for LuaurcError {}

impl LuaurcConfig {
    pub fn parse(text: &str) -> Result<Self, LuaurcError> {
        let mut root = Parser::new(text)?.parse_document()?;
        let aliases = match root.remove("aliases") {
            None => BTreeMap::new(),
            Some(LuaurcValue::Object(aliases)) => aliases
                .into_iter()
                .map(|(alias, path)| match path {
                    LuaurcValue::String(path) => Ok((alias, path)),
                    _ => Err(ALIASES_SHAPE),
                })
                .collect::<Result<_, _>>()?,
            Some(_) => return Err(ALIASES_SHAPE),
        };
        let globals = match root.remove("globals") {
            None => BTreeSet::new(),
            Some(LuaurcValue::Array(globals)) => globals.into_iter().collect(),
            Some(_) => {
                return Err(LuaurcError::Shape {
                    key: "globals",
                    expected: "an array of strings",
                });
            }
        };
        Ok(Self {
            aliases,
            globals,
            other: root,
        })
    }

    /// Renders the file with every key quoted and a trailing newline.
    pub fn render(&self) -> Result<String, LuaurcError> {
        let mut root = self.other.clone();
        if !self.aliases.is_empty() {
            let aliases = self
                .aliases
                .iter()
                .map(|(alias, path)| (alias.clone(), LuaurcValue::String(path.clone())))
                .collect();
            root.insert("aliases".to_owned(), LuaurcValue::Object(aliases));
        }
        if !self.globals.is_empty() {
            let globals = self.globals.iter().cloned().collect();
            root.insert("globals".to_owned(), LuaurcValue::Array(globals));
        }
        let mut out = String::new();
        write_object(&mut out, &root, 0)?;
        out.push('\n');
        Ok(out)
    }

    pub fn insert_alias(&mut self, alias: impl Into<String>, path: impl Into<String>) {
        self.aliases.insert(alias.into(), path.into());
    }

    pub fn remove_global(&mut self, global: &str) {
        self.globals.remove(global);
    }

    pub fn merge_globals<I, S>(&mut self, globals: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.globals.extend(globals.into_iter().map(Into::into));
    }
}

const ALIASES_SHAPE: LuaurcError = LuaurcError::Shape {
    key: "aliases",
    expected: "an object of strings",
};

#[derive(Clone, Copy)]
enum Token<'a> {
    Eof,
    Punct(u8),
    String(&'a str),
    Bool(bool),
    Other(&'a str),
}

/// A port of Luau's `parseJson` (`Config/src/Config.cpp`) over the parts of
/// its lexer that grammar reaches.
struct Parser<'a> {
    text: &'a str,
    pos: usize,
    start: usize,
    token: Token<'a>,
}

impl<'a> Parser<'a> {
    fn new(text: &'a str) -> Result<Self, LuaurcError> {
        let mut parser = Self {
            text,
            pos: 0,
            start: 0,
            token: Token::Eof,
        };
        parser.advance()?;
        Ok(parser)
    }

    fn parse_document(&mut self) -> Result<BTreeMap<String, LuaurcValue>, LuaurcError> {
        if !matches!(self.token, Token::Punct(b'{')) {
            return Err(self.expected("'{'"));
        }
        let root = self.parse_object(1)?;
        if !matches!(self.token, Token::Eof) {
            return Err(self.expected("end of file"));
        }
        Ok(root)
    }

    fn parse_object(&mut self, depth: usize) -> Result<BTreeMap<String, LuaurcValue>, LuaurcError> {
        if depth > MAX_DEPTH {
            return Err(self.error(format!("objects nested more than {MAX_DEPTH} deep")));
        }
        self.advance()?;
        let mut members = BTreeMap::new();
        loop {
            let key = match self.token {
                Token::Punct(b'}') => {
                    self.advance()?;
                    return Ok(members);
                }
                Token::String(key) => key.to_owned(),
                _ => return Err(self.expected("field key")),
            };
            self.advance()?;
            if !matches!(self.token, Token::Punct(b':')) {
                return Err(self.expected("':'"));
            }
            self.advance()?;
            let value = match self.token {
                Token::Punct(b'{') => LuaurcValue::Object(self.parse_object(depth + 1)?),
                Token::Punct(b'[') => LuaurcValue::Array(self.parse_array()?),
                Token::String(value) => {
                    self.advance()?;
                    LuaurcValue::String(value.to_owned())
                }
                Token::Bool(value) => {
                    self.advance()?;
                    LuaurcValue::Bool(value)
                }
                _ => return Err(self.expected("field value")),
            };
            insert_merged(&mut members, key, value);
            self.separator(b'}')?;
        }
    }

    fn parse_array(&mut self) -> Result<Vec<String>, LuaurcError> {
        self.advance()?;
        let mut items = Vec::new();
        loop {
            match self.token {
                Token::Punct(b']') => {
                    self.advance()?;
                    return Ok(items);
                }
                Token::String(item) => {
                    items.push(item.to_owned());
                    self.advance()?;
                }
                _ => return Err(self.expected("array element or ']'")),
            }
            self.separator(b']')?;
        }
    }

    fn separator(&mut self, close: u8) -> Result<(), LuaurcError> {
        match self.token {
            Token::Punct(b',') => self.advance(),
            Token::Punct(punct) if punct == close => Ok(()),
            _ => Err(self.expected(if close == b'}' {
                "',' or '}'"
            } else {
                "',' or ']'"
            })),
        }
    }

    fn peek(&self, ahead: usize) -> u8 {
        byte_at(self.text.as_bytes(), self.pos + ahead)
    }

    fn advance(&mut self) -> Result<(), LuaurcError> {
        loop {
            while is_space(self.peek(0)) {
                self.pos += 1;
            }
            // `//=` lexes as one operator in Luau, so it starts no comment.
            if self.peek(0) == b'/' && self.peek(1) == b'/' && self.peek(2) != b'=' {
                while !matches!(self.peek(0), 0 | b'\n' | b'\r') {
                    self.pos += 1;
                }
                continue;
            }
            break;
        }
        self.start = self.pos;
        self.token = match self.peek(0) {
            // Luau's lexer reads a NUL byte as the end of its input.
            0 => Token::Eof,
            punct @ (b'{' | b'}' | b'[' | b']' | b':' | b',') => {
                self.pos += 1;
                Token::Punct(punct)
            }
            delimiter @ (b'"' | b'\'') => {
                let body = self.pos + 1;
                let Some(len) = string_len(&self.text.as_bytes()[body..], delimiter) else {
                    return Err(self.error("unterminated string".to_owned()));
                };
                self.pos = body + len + 1;
                Token::String(&self.text[body..body + len])
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                while self.peek(0).is_ascii_alphanumeric() || self.peek(0) == b'_' {
                    self.pos += 1;
                }
                match &self.text[self.start..self.pos] {
                    "true" => Token::Bool(true),
                    "false" => Token::Bool(false),
                    word => Token::Other(word),
                }
            }
            _ => {
                let len = self.text[self.pos..]
                    .chars()
                    .next()
                    .map_or(1, char::len_utf8);
                self.pos += len;
                Token::Other(&self.text[self.start..self.pos])
            }
        };
        Ok(())
    }

    fn expected(&self, expected: &str) -> LuaurcError {
        let found = match self.token {
            Token::Eof => "end of file".to_owned(),
            Token::Punct(punct) => format!("'{}'", punct as char),
            Token::String(text) => format!("string {text:?}"),
            Token::Bool(value) => format!("'{value}'"),
            Token::Other(text) => format!("'{text}'"),
        };
        self.error(format!("expected {expected}, found {found}"))
    }

    fn error(&self, message: String) -> LuaurcError {
        let line = 1 + self.text.as_bytes()[..self.start]
            .iter()
            .filter(|&&byte| byte == b'\n')
            .count();
        LuaurcError::Syntax { line, message }
    }
}

/// Repeated keys apply in order in Luau: objects merge, arrays append, and
/// any other value replaces the earlier one.
fn insert_merged(members: &mut BTreeMap<String, LuaurcValue>, key: String, value: LuaurcValue) {
    match members.entry(key) {
        Entry::Vacant(entry) => {
            entry.insert(value);
        }
        Entry::Occupied(mut entry) => match (entry.get_mut(), value) {
            (LuaurcValue::Object(existing), LuaurcValue::Object(more)) => {
                for (key, value) in more {
                    insert_merged(existing, key, value);
                }
            }
            (LuaurcValue::Array(existing), LuaurcValue::Array(more)) => existing.extend(more),
            (existing, value) => *existing = value,
        },
    }
}

fn byte_at(bytes: &[u8], index: usize) -> u8 {
    bytes.get(index).copied().unwrap_or(0)
}

fn is_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\r' | b'\n' | 0x0b | 0x0c)
}

/// The length of a string body up to its closing `delimiter`, following
/// Luau's `readQuotedString`: a backslash skips the byte after it (a CRLF or,
/// for `\z`, a run of whitespace), and a bare line break or NUL ends the
/// string unterminated.
fn string_len(body: &[u8], delimiter: u8) -> Option<usize> {
    let mut i = 0;
    loop {
        match byte_at(body, i) {
            byte if byte == delimiter => return Some(i),
            0 | b'\r' | b'\n' => return None,
            b'\\' => {
                i += 1;
                match byte_at(body, i) {
                    0 => {}
                    b'\r' => {
                        i += 1;
                        if byte_at(body, i) == b'\n' {
                            i += 1;
                        }
                    }
                    b'z' => {
                        i += 1;
                        while is_space(byte_at(body, i)) {
                            i += 1;
                        }
                    }
                    _ => i += 1,
                }
            }
            _ => i += 1,
        }
    }
}

fn write_string(out: &mut String, text: &str) -> Result<(), LuaurcError> {
    for delimiter in [b'"', b'\''] {
        let mut quoted = Vec::with_capacity(text.len() + 1);
        quoted.extend_from_slice(text.as_bytes());
        quoted.push(delimiter);
        if string_len(&quoted, delimiter) == Some(text.len()) {
            let delimiter = delimiter as char;
            out.push(delimiter);
            out.push_str(text);
            out.push(delimiter);
            return Ok(());
        }
    }
    Err(LuaurcError::Unquotable(text.to_owned()))
}

fn write_object(
    out: &mut String,
    members: &BTreeMap<String, LuaurcValue>,
    indent: usize,
) -> Result<(), LuaurcError> {
    if members.is_empty() {
        out.push_str("{}");
        return Ok(());
    }
    out.push_str("{\n");
    for (key, value) in members {
        push_indent(out, indent + 1);
        write_string(out, key)?;
        out.push_str(": ");
        match value {
            LuaurcValue::String(text) => write_string(out, text)?,
            LuaurcValue::Bool(value) => out.push_str(if *value { "true" } else { "false" }),
            LuaurcValue::Array(items) => write_array(out, items, indent + 1)?,
            LuaurcValue::Object(members) => write_object(out, members, indent + 1)?,
        }
        out.push_str(",\n");
    }
    push_indent(out, indent);
    out.push('}');
    Ok(())
}

fn write_array(out: &mut String, items: &[String], indent: usize) -> Result<(), LuaurcError> {
    if items.is_empty() {
        out.push_str("[]");
        return Ok(());
    }
    out.push_str("[\n");
    for item in items {
        push_indent(out, indent + 1);
        write_string(out, item)?;
        out.push_str(",\n");
    }
    push_indent(out, indent);
    out.push(']');
    Ok(())
}

fn push_indent(out: &mut String, indent: usize) {
    for _ in 0..indent {
        out.push_str("  ");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> LuaurcConfig {
        LuaurcConfig::parse(text).unwrap_or_else(|err| panic!("{text:?}: {err}"))
    }

    fn syntax_error(text: &str) -> (usize, String) {
        match LuaurcConfig::parse(text) {
            Err(LuaurcError::Syntax { line, message }) => (line, message),
            other => panic!("{text:?}: expected a syntax error, got {other:?}"),
        }
    }

    fn aliases(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(alias, path)| ((*alias).to_owned(), (*path).to_owned()))
            .collect()
    }

    #[test]
    fn accepts_the_syntax_luau_accepts() {
        let config = parse(
            "// leading\r\n{\n  'aliases': {\"a\": './a',}, // trailing\n  // own line\n\x0c\x0b  \"globals\": [\"g\",],\n  \"typeErrors\": false,\n}\n",
        );
        assert_eq!(config.aliases, aliases(&[("a", "./a")]));
        assert_eq!(config.globals, BTreeSet::from(["g".to_owned()]));
        assert_eq!(
            config.other,
            BTreeMap::from([("typeErrors".to_owned(), LuaurcValue::Bool(false))])
        );
    }

    #[test]
    fn rejects_what_luau_rejects() {
        for text in [
            "{ /* c */ }",
            "{ -- c\n}",
            "{ aliases: {} }",
            "{ \"languageMode\": 1 }",
            "{ \"languageMode\": null }",
            "{ \"languageMode\": Infinity }",
            "{ \"globals\": [[\"g\"]] }",
            "{ \"globals\": [{}] }",
            "{ \"a\": [[./a]] }",
            "{ \"a\": `./a` }",
            "{ \"a\": \"b\" } //= c",
            "\u{feff}{}",
            "{\u{a0}}",
            "[\"g\"]",
            "{ , }",
            "{} {}",
            "{ \"a\": \"b\n\" }",
        ] {
            syntax_error(text);
        }
    }

    #[test]
    fn reports_the_line_and_token() {
        assert_eq!(
            syntax_error("{\n  aliases: {}\n}"),
            (2, "expected field key, found 'aliases'".to_owned())
        );
        assert_eq!(
            syntax_error("{\n  \"a\": \"b\"\n"),
            (3, "expected ',' or '}', found end of file".to_owned())
        );
    }

    #[test]
    fn keeps_escapes_verbatim() {
        for raw in [
            r#"./a\"b"#,
            r"./a\nb",
            r"./\u0041",
            "./a\\\nb",
            "./a\\z\n  b",
        ] {
            let config = parse(&format!("{{ \"aliases\": {{ \"a\": \"{raw}\" }} }}"));
            assert_eq!(config.aliases, aliases(&[("a", raw)]));
        }
    }

    #[test]
    fn repeated_keys_apply_in_order() {
        let config = parse(
            r#"{
                "aliases": { "a": "./old", "b": "./b" },
                "globals": ["one"],
                "aliases": { "a": "./new" },
                "globals": ["two"],
                "languageMode": "nonstrict",
                "languageMode": "strict",
            }"#,
        );
        assert_eq!(config.aliases, aliases(&[("a", "./new"), ("b", "./b")]));
        assert_eq!(
            config.globals,
            BTreeSet::from(["one".to_owned(), "two".to_owned()])
        );
        assert_eq!(
            config.other["languageMode"],
            LuaurcValue::String("strict".to_owned())
        );
    }

    #[test]
    fn refuses_deep_nesting_and_misshapen_known_keys() {
        let deep = "{\"a\":".repeat(MAX_DEPTH) + "{" + &"}".repeat(MAX_DEPTH + 1);
        syntax_error(&deep);
        assert_eq!(
            LuaurcConfig::parse(r#"{ "aliases": { "a": true } }"#),
            Err(ALIASES_SHAPE)
        );
        assert!(matches!(
            LuaurcConfig::parse(r#"{ "globals": "g" }"#),
            Err(LuaurcError::Shape { key: "globals", .. })
        ));
    }

    #[test]
    fn renders_quoted_keys_with_trailing_commas() {
        let mut config = parse(r#"{ "lint": { "*": true }, "languageMode": "strict" }"#);
        config.insert_alias("lyra", "./.lyra/luau/lyra");
        config.merge_globals(["warn"]);
        assert_eq!(
            config.render().unwrap(),
            r#"{
  "aliases": {
    "lyra": "./.lyra/luau/lyra",
  },
  "globals": [
    "warn",
  ],
  "languageMode": "strict",
  "lint": {
    "*": true,
  },
}
"#
        );
        assert_eq!(LuaurcConfig::default().render().unwrap(), "{}\n");
    }

    #[test]
    fn renders_strings_in_a_quote_that_keeps_them() {
        let mut config = LuaurcConfig::default();
        config.insert_alias("double", r#"./say "hi""#);
        config.insert_alias("escaped", r#"./a\"b"#);
        config.insert_alias("backslash", r"./a\b");
        let rendered = config.render().unwrap();
        assert!(rendered.contains(r#""double": './say "hi"',"#));
        assert!(rendered.contains(r#""escaped": "./a\"b","#));
        assert_eq!(parse(&rendered), config);

        for unquotable in ["./a\\", "./a\nb", r#"./"both'"#] {
            let mut config = LuaurcConfig::default();
            config.insert_alias("a", unquotable);
            assert_eq!(
                config.render(),
                Err(LuaurcError::Unquotable(unquotable.to_owned()))
            );
        }
    }
}
