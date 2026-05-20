// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use harmony_core::{
    FunctionSpec,
    GlobalSpec,
};
use harmony_luau as luau;
use harmony_luau::{
    GlobalFunctionDescriptor,
    LuauType,
    ParameterDescriptor,
    render_globals_definition,
};
use tracing::Level;

const MAX_LINE_BYTES: usize = 8 * 1024;
const LOG_GLOBAL_SCOPE_NAME: &str = "harmony.globals";
const WARN_GLOBAL_NAME: &str = "warn";
const LUAURC_GLOBAL_NAMES: &[&str] = &[WARN_GLOBAL_NAME];

struct LogValue;

pub fn plugin_log_global_specs() -> Vec<GlobalSpec> {
    vec![
        GlobalSpec::new(LOG_GLOBAL_SCOPE_NAME)
            .function(log_function_spec(WARN_GLOBAL_NAME, Level::WARN)),
    ]
}

pub fn plugin_log_luaurc_global_names() -> &'static [&'static str] {
    LUAURC_GLOBAL_NAMES
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

fn log_function_spec(name: &'static str, level: Level) -> FunctionSpec {
    let spec = FunctionSpec::sync_fn(name).variadic_args::<LogValue>();
    spec.call(move |frame| emit_callback(level, frame))
}

fn emit_callback(level: Level, mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let joined = join_log_values(&frame.args.drain_remaining());
    let sanitized = sanitize_message(&joined);
    let final_msg = truncate_to_byte_cap(&sanitized, MAX_LINE_BYTES);

    match level {
        Level::ERROR => tracing::event!(parent: None, Level::ERROR, "{final_msg}"),
        Level::WARN => tracing::event!(parent: None, Level::WARN, "{final_msg}"),
        Level::INFO => tracing::event!(parent: None, Level::INFO, "{final_msg}"),
        Level::DEBUG => tracing::event!(parent: None, Level::DEBUG, "{final_msg}"),
        Level::TRACE => tracing::event!(parent: None, Level::TRACE, "{final_msg}"),
    }

    Ok(())
}

fn join_log_values(args: &[luau::Value]) -> String {
    args.iter()
        .map(log_value_to_display_string)
        .collect::<Vec<_>>()
        .join("\t")
}

fn log_value_to_display_string(value: &luau::Value) -> String {
    match value {
        luau::Value::Nil => "nil".to_string(),
        luau::Value::Boolean(value) => value.to_string(),
        luau::Value::Integer(value) => value.to_string(),
        luau::Value::Number(value) => value.to_string(),
        luau::Value::String(value) => String::from_utf8_lossy(value).into_owned(),
        luau::Value::Buffer(_) => "<buffer>".to_string(),
        luau::Value::TableData(_) => "<table>".to_string(),
        luau::Value::NativeFunction(_) => "<function>".to_string(),
        luau::Value::Table(_) => "<table>".to_string(),
        luau::Value::Function(_) => "<function>".to_string(),
        luau::Value::Thread(_) => "<thread>".to_string(),
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
