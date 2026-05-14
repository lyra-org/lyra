// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use harmony_core::Global;
use harmony_luau::{
    GlobalFunctionDescriptor,
    LuauType,
    ParameterDescriptor,
    render_globals_definition,
};
use mlua::{
    Lua,
    MultiValue,
    Value,
};
use tracing::Level;

const MAX_LINE_BYTES: usize = 8 * 1024;
const MAX_STACK_FRAMES: usize = 64;
const STACK_TRUNCATED_MARKER: &str = "...truncated";
const NO_FRAME_SOURCE: &str = "<?>";
const LOG_GLOBAL_SCOPE_NAME: &str = "harmony.globals";
const PRINT_GLOBAL_NAME: &str = "print";
const WARN_GLOBAL_NAME: &str = "warn";
const LUAURC_GLOBAL_NAMES: &[&str] = &[WARN_GLOBAL_NAME];

pub fn plugin_log_luaurc_global_names() -> &'static [&'static str] {
    LUAURC_GLOBAL_NAMES
}

pub fn plugin_log_globals() -> Vec<Global> {
    vec![Global {
        name: LOG_GLOBAL_SCOPE_NAME.into(),
        install: Arc::new(|lua| install_plugin_log_globals(lua).map_err(Into::into)),
    }]
}

pub fn install_plugin_log_globals(lua: &Lua) -> mlua::Result<()> {
    install_log_global(lua, PRINT_GLOBAL_NAME, Level::INFO)?;
    install_log_global(lua, WARN_GLOBAL_NAME, Level::WARN)?;
    Ok(())
}

pub fn render_plugin_log_globals_luau_definition() -> Result<String, std::fmt::Error> {
    render_globals_definition(&[GlobalFunctionDescriptor {
        name: WARN_GLOBAL_NAME,
        description: Some("Logs at WARN."),
        params: vec![ParameterDescriptor {
            name: "values",
            ty: LuauType::any(),
            description: None,
            variadic: true,
        }],
        returns: Vec::new(),
        yields: false,
    }])
}

fn install_log_global(lua: &Lua, name: &'static str, level: Level) -> mlua::Result<()> {
    let function = lua.create_function(move |lua, args: MultiValue| emit(level, lua, args))?;
    lua.globals().set(name, function)
}

fn emit(level: Level, lua: &Lua, args: MultiValue) -> mlua::Result<()> {
    let frame = walk_caller_stack(lua);
    let joined = join_args(&args);
    let sanitized = sanitize_message(&joined);
    let final_msg = truncate_to_byte_cap(&sanitized, MAX_LINE_BYTES);
    let log_msg = format_log_message(&frame, &final_msg);

    match level {
        Level::ERROR => tracing::event!(parent: None, Level::ERROR, "{log_msg}"),
        Level::WARN => tracing::event!(parent: None, Level::WARN, "{log_msg}"),
        Level::INFO => tracing::event!(parent: None, Level::INFO, "{log_msg}"),
        Level::DEBUG => tracing::event!(parent: None, Level::DEBUG, "{log_msg}"),
        Level::TRACE => tracing::event!(parent: None, Level::TRACE, "{log_msg}"),
    }

    Ok(())
}

#[derive(Debug, Default)]
struct StackFrame {
    chain: Vec<Arc<str>>,
    chain_truncated: bool,
    immediate_source: Option<String>,
    immediate_line: Option<u32>,
}

fn walk_caller_stack(lua: &Lua) -> StackFrame {
    let mut frame = StackFrame::default();

    for level in 1..=MAX_STACK_FRAMES {
        let info = lua.inspect_stack(level, |debug| {
            let source = debug.source().source.map(|cow| cow.into_owned());
            let line = debug.current_line();
            (source, line)
        });
        let Some((source, line)) = info else {
            return frame;
        };
        if level == 1 {
            frame.immediate_source = source.clone();
            frame.immediate_line = line.and_then(|n| u32::try_from(n).ok());
        }
        if let Some(source) = source.as_deref()
            && let Some(plugin_id) = harmony_core::parse_plugin_id(source)
        {
            let id = Arc::<str>::from(plugin_id);
            if frame
                .chain
                .last()
                .is_none_or(|last| last.as_ref() != id.as_ref())
            {
                frame.chain.push(id);
            }
        }
    }

    frame.chain_truncated = true;
    frame
}

fn format_log_message(frame: &StackFrame, message: &str) -> String {
    let source = frame.immediate_source.as_deref().unwrap_or(NO_FRAME_SOURCE);
    let line = frame.immediate_line.unwrap_or(0);
    let chain = render_chain(&frame.chain, frame.chain_truncated);

    if chain.is_empty() {
        format!("{source}:{line}: {message}")
    } else {
        format!("{chain} {source}:{line}: {message}")
    }
}

fn render_chain(chain: &[Arc<str>], truncated: bool) -> String {
    if chain.is_empty() {
        return if truncated {
            STACK_TRUNCATED_MARKER.to_string()
        } else {
            String::new()
        };
    }
    let mut out = chain
        .iter()
        .map(|id| id.as_ref())
        .collect::<Vec<_>>()
        .join("->");
    if truncated {
        out.push_str("->");
        out.push_str(STACK_TRUNCATED_MARKER);
    }
    out
}

fn join_args(args: &MultiValue) -> String {
    let mut parts = Vec::with_capacity(args.len());
    for value in args.iter() {
        parts.push(coerce_to_display_string(value));
    }
    parts.join("\t")
}

fn coerce_to_display_string(value: &Value) -> String {
    match value.to_string() {
        Ok(s) => s,
        Err(_) => format!("<error: {}>", value.type_name()),
    }
}

fn sanitize_message(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        let is_c0 = (ch as u32) < 0x20;
        let is_del = ch == '\u{7f}';
        let is_c1 = matches!(ch, '\u{80}'..='\u{9f}');
        if (is_c0 && ch != '\t') || is_del || is_c1 {
            continue;
        }
        out.push(ch);
    }
    out
}

fn truncate_to_byte_cap(input: &str, cap: usize) -> String {
    if input.len() <= cap {
        return input.to_string();
    }
    let boundary = floor_char_boundary(input, cap);
    let dropped = input.len() - boundary;
    let mut out = String::with_capacity(boundary + 32);
    out.push_str(&input[..boundary]);
    out.push_str(&format!("...[truncated {dropped} bytes]"));
    out
}

fn floor_char_boundary(s: &str, index: usize) -> usize {
    let mut idx = index.min(s.len());
    while idx > 0 && !s.is_char_boundary(idx) {
        idx -= 1;
    }
    idx
}
