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

const MAX_LINE_BYTES: usize = 8 * 1024;
const LOG_TARGET: &str = "harmony::plugin";
const LOG_GLOBAL_SCOPE_NAME: &str = "harmony.globals";
const PRINT_GLOBAL_NAME: &str = "print";
const WARN_GLOBAL_NAME: &str = "warn";
const LUAURC_GLOBAL_NAMES: &[&str] = &[WARN_GLOBAL_NAME];

pub fn plugin_log_global_specs() -> Vec<GlobalSpec> {
    vec![
        GlobalSpec::new(LOG_GLOBAL_SCOPE_NAME)
            .function(log_function_spec(PRINT_GLOBAL_NAME, |line| {
                tracing::info!(
                    target: LOG_TARGET,
                    parent: None,
                    plugin = line.plugin(),
                    source = line.source(),
                    line = line.line(),
                    "{}",
                    line.message,
                );
            }))
            .function(log_function_spec(WARN_GLOBAL_NAME, |line| {
                tracing::warn!(
                    target: LOG_TARGET,
                    parent: None,
                    plugin = line.plugin(),
                    source = line.source(),
                    line = line.line(),
                    "{}",
                    line.message,
                );
            })),
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

/// A line a plugin logs, with where in its code it was logged from when that is known.
struct LogLine {
    message: String,
    location: Option<luau::CallerLocation>,
}

impl LogLine {
    fn plugin(&self) -> Option<&str> {
        self.location.as_ref()?.origin.as_ref()?.plugin.as_deref()
    }

    fn source(&self) -> Option<&str> {
        self.location.as_ref().map(|location| &*location.chunk)
    }

    fn line(&self) -> Option<u32> {
        self.location.as_ref()?.line
    }
}

fn log_function_spec(name: &'static str, log: fn(&LogLine)) -> FunctionSpec {
    FunctionSpec::new(name).call(move |frame| emit_callback(log, frame))
}

fn emit_callback(log: fn(&LogLine), mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let location = frame.caller_location();
    let joined = join_log_values(&frame.args.drain_remaining()?);
    let sanitized = sanitize_message(&joined);
    let message = truncate_to_byte_cap(&sanitized, MAX_LINE_BYTES);

    log(&LogLine { message, location });
    Ok(())
}

fn join_log_values(args: &[luau::Value]) -> String {
    args.iter()
        .map(log_value_to_display_string)
        .collect::<Vec<_>>()
        .join("\t")
}

fn log_value_to_display_string(value: &luau::Value) -> String {
    if let Some(message) = value.error_message() {
        return message;
    }
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
        luau::Value::UserData(_) => "<userdata>".to_string(),
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

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::{
            Arc,
            Mutex,
        },
    };

    use tracing_subscriber::fmt::MakeWriter;

    use super::*;

    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Captured {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Captured {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    fn capture_log<T>(run: impl FnOnce() -> T) -> (T, String) {
        let captured = Captured::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(captured.clone())
            .with_max_level(tracing::Level::TRACE)
            .with_ansi(false)
            .without_time()
            .finish();
        let result = tracing::subscriber::with_default(subscriber, run);
        let log = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
        (result, log)
    }

    #[test]
    fn print_and_warn_log_where_they_were_called_from() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        vm.open_standard_libraries(luau::StandardLibraries::all_supported())?;
        for globals in plugin_log_global_specs() {
            harmony_core::luau::install_globals(&vm, &luau::ChunkOrigin::default(), &globals)?;
        }
        let chunk = vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(
                &b"print(\"hello\", 1)\nwarn(\"careful\")\ncoroutine.wrap(print)(\"native\")"[..],
            ),
            luau::ChunkOrigin {
                plugin: Some(Arc::from("demo")),
                path: Some(Arc::from("plugins/demo/init.luau")),
                ..luau::ChunkOrigin::default()
            },
        ))?;

        let (result, log) = capture_log(|| chunk.call(&[]));
        result?;

        assert_eq!(
            log,
            concat!(
                " INFO harmony::plugin: hello\t1 plugin=\"demo\" source=\"plugins/demo/init.luau\" line=1\n",
                " WARN harmony::plugin: careful plugin=\"demo\" source=\"plugins/demo/init.luau\" line=2\n",
                " INFO harmony::plugin: native plugin=\"demo\" source=\"plugins/demo/init.luau\" line=3\n",
            )
        );
        Ok(())
    }

    fn plugin(id: &str, dependencies: &[&str]) -> harmony_core::plugin::LoadedPlugin {
        harmony_core::plugin::LoadedPlugin {
            manifest: harmony_core::plugin::PluginManifest {
                schema_version: harmony_core::plugin::PLUGIN_SCHEMA_VERSION,
                id: id.to_string(),
                name: id.to_string(),
                version: semver::Version::new(1, 0, 0),
                host_requirement: semver::VersionReq::STAR,
                description: String::new(),
                entrypoint: Some("init.luau".to_string()),
                scopes: Vec::new(),
                scope_reasons: Default::default(),
                scope_grants: Default::default(),
                dependencies: dependencies
                    .iter()
                    .map(|dependency| {
                        (
                            dependency.to_string(),
                            harmony_core::plugin::Dependency {
                                version: semver::VersionReq::STAR,
                                optional: false,
                            },
                        )
                    })
                    .collect(),
            },
            directory: format!("plugins/{id}").into(),
            declared_scopes: Default::default(),
        }
    }

    /// A plugin's code logs as that plugin, also when another plugin that required it calls it.
    #[test]
    fn plugin_logs_name_the_plugin_whose_code_logged() -> anyhow::Result<()> {
        let mut loader = harmony_core::MemorySourceLoader::new();
        loader.insert(
            "plugin:lib/init",
            b"return {\n\tlog = function(message)\n\t\twarn(message)\n\tend,\n}".as_slice(),
        );
        loader.insert(
            "plugin:app/init",
            b"local lib = require(\"@lib\")\nprint(\"app\")\nlib.log(\"lib\")".as_slice(),
        );
        let plugins = [plugin("lib", &[]), plugin("app", &["lib"])];
        let runtime = harmony_core::plugin::RuntimeBuilder::new(
            loader,
            plugins
                .iter()
                .map(|plugin| plugin.manifest.clone())
                .collect(),
        )
        .plugins(Arc::from(plugins))
        .global_specs(plugin_log_global_specs())
        .build()?;

        let (failed, log) = capture_log(|| runtime.exec_all());

        assert!(failed.is_empty(), "{failed:?}");
        let plugin_lines = log
            .lines()
            .filter(|line| line.contains("harmony::plugin"))
            .collect::<Vec<_>>();
        assert_eq!(
            plugin_lines,
            [
                " INFO harmony::plugin: app plugin=\"app\" source=\"plugins/app/init\" line=2",
                " WARN harmony::plugin: lib plugin=\"lib\" source=\"plugins/lib/init\" line=3",
            ],
            "{log}"
        );
        Ok(())
    }

    #[test]
    fn host_errors_log_their_message() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        vm.open_standard_libraries(luau::StandardLibraries::all_supported())?;
        let fail = vm.create_function(
            luau::ChunkOrigin::default(),
            Arc::new(|_| {
                Err(luau::Error::Kinded {
                    kind: "missing",
                    message: "gone".to_string(),
                })
            }),
        )?;
        let caught = vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"local _, err = pcall(...) return err"[..]),
            luau::ChunkOrigin::default(),
        ))?;
        let error = caught.call(&[luau::Value::Function(fail)])?.remove(0);
        let other = vm.create_userdata(1_u8)?;

        assert_eq!(
            join_log_values(&[error, luau::Value::UserData(other)]),
            "module '<unknown module>' function '<unknown function>' plugin '<non-plugin>': gone\t<userdata>"
        );
        Ok(())
    }
}
