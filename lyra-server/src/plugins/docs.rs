// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use anyhow::{
    Context,
    Result,
    bail,
};
use harmony_core::LuaurcConfig;
use std::{
    env,
    fs,
    io::ErrorKind,
    path::{
        Path,
        PathBuf,
    },
};

use crate::{
    routes,
    services::remote::messages::WsApiSpec,
};

use super::{
    lyra_doc_source_ids,
    render_lyra_doc_source,
};

pub(crate) const DEFAULT_SETUP_DOCS_OUT_DIR: &str = ".lyra/luau";
pub(crate) const DEFAULT_SETUP_DEFS_DIR: &str = ".lyra/defs";
pub(crate) const DEFAULT_SETUP_API_DOCS_DIR: &str = ".lyra/api";
pub(crate) const GLOBALS_DEFINITIONS_FILENAME: &str = "globals.d.luau";

const OPENAPI_FILENAME: &str = "openapi.json";
const ASYNCAPI_FILENAME: &str = "asyncapi.json";
const LUAURC_FILENAME: &str = ".luaurc";
const LUAUCONFIG_FILENAME: &str = ".config.luau";
const GITIGNORE_FILENAME: &str = ".gitignore";
const GITIGNORE_ENTRY: &str = ".lyra/";

type RenderDocsFn = fn() -> Result<String>;

#[derive(Clone, Copy)]
enum DocOutputLayout {
    Declaration,
    Module,
}

#[derive(Clone, Copy)]
enum ApiDocKind {
    OpenApi,
    AsyncApi,
}

struct Surface {
    id: &'static str,
    render_docs: RenderDocsFn,
}

macro_rules! surface {
    ($id:literal, $module:path, $render:path) => {
        Surface {
            id: $id,
            render_docs: || $render().map_err(anyhow::Error::from),
        }
    };
}

pub(crate) fn doc_source_ids() -> impl Iterator<Item = &'static str> {
    harmony_surfaces()
        .iter()
        .map(|surface| surface.id)
        .chain(lyra_doc_source_ids())
}

pub(crate) fn render_doc_source(id: &str) -> Result<String> {
    if let Some(surface) = harmony_surfaces().iter().find(|surface| surface.id == id) {
        return (surface.render_docs)();
    }

    if let Some(rendered) = render_lyra_doc_source(id)? {
        return Ok(rendered);
    }

    bail!("unknown docs source `{id}`")
}

fn render_api_doc(kind: ApiDocKind) -> Result<String> {
    match kind {
        ApiDocKind::OpenApi => render_pretty_json(&routes::build_openapi_spec(), "OpenAPI docs"),
        ApiDocKind::AsyncApi => render_pretty_json(&WsApiSpec::asyncapi_spec(), "AsyncAPI docs"),
    }
}

pub(crate) fn generate_docs(out_dir: &Path) -> Result<()> {
    write_docs(out_dir, DocOutputLayout::Declaration, true)?;
    let globals_path = out_dir.join(GLOBALS_DEFINITIONS_FILENAME);
    write_globals_definition(&globals_path)?;
    println!("{}", globals_path.display());
    write_api_docs(&out_dir.join("api"), true)?;
    Ok(())
}

pub(crate) fn setup_docs(project_root: &Path) -> Result<()> {
    let config_luau_path = project_root.join(LUAUCONFIG_FILENAME);
    if config_luau_path.is_file() {
        bail!(
            "cannot run docs setup while {} exists",
            config_luau_path.display()
        );
    }

    let output_dir = project_root.join(DEFAULT_SETUP_DOCS_OUT_DIR);
    generate_setup_docs(&output_dir)?;

    let defs_dir = project_root.join(DEFAULT_SETUP_DEFS_DIR);
    let globals_path = defs_dir.join(GLOBALS_DEFINITIONS_FILENAME);
    write_globals_definition(&globals_path)?;

    write_api_docs(&project_root.join(DEFAULT_SETUP_API_DOCS_DIR), false)?;

    let luaurc_path = project_root.join(LUAURC_FILENAME);
    let mut luaurc = read_or_create_luaurc(&luaurc_path)?;
    merge_setup_aliases(&mut luaurc);
    apply_setup_globals(&mut luaurc);
    write_luaurc(&luaurc_path, &luaurc)?;

    ensure_gitignore_entry(&project_root.join(GITIGNORE_FILENAME), GITIGNORE_ENTRY)?;

    println!(
        "Luau docs have been set up successfully in {}.",
        output_dir.display()
    );
    println!("Pass this flag to `luau-lsp analyze` so plugins can see the bare global `warn`:");
    println!("  --definitions={}", globals_path.display());
    println!("You may need to restart your editor for the changes to take effect.");

    Ok(())
}

fn write_globals_definition(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .context("globals definition path should have a parent directory")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create globals definition directory {}", parent.display()))?;
    let contents = harmony_globals::render_plugin_log_globals_luau_definition()
        .context("render plugin log globals definition")?;
    fs::write(path, contents)
        .with_context(|| format!("write globals definition {}", path.display()))
}

pub(crate) fn run_command(args: &[String]) -> Result<()> {
    match args {
        [command] if command == "list" => {
            for source in doc_source_ids() {
                println!("{source}");
            }
            Ok(())
        }
        [command, source] if command == "print" => {
            let contents = match ApiDocKind::parse(source) {
                Some(kind) => render_api_doc(kind)?,
                None => render_doc_source(source)?,
            };
            print!("{contents}");
            Ok(())
        }
        [command, flag, path] if command == "generate" && flag == "--out-dir" => {
            generate_docs(Path::new(path))
        }
        [command] if command == "setup" => setup_docs(&env::current_dir()?),
        _ => bail!(docs_command_usage()),
    }
}

fn docs_command_usage() -> &'static str {
    "usage:\n  lyra-docs list\n  lyra-docs print <source|openapi|asyncapi>\n  lyra-docs generate --out-dir <dir>\n  lyra-docs setup"
}

fn write_api_docs(out_dir: &Path, print_paths: bool) -> Result<()> {
    fs::create_dir_all(out_dir)
        .with_context(|| format!("create API docs output directory {}", out_dir.display()))?;

    let openapi_path = out_dir.join(OPENAPI_FILENAME);
    let openapi = routes::build_openapi_spec();
    write_pretty_json(&openapi_path, &openapi)?;
    if print_paths {
        println!("{}", openapi_path.display());
    }

    let asyncapi_path = out_dir.join(ASYNCAPI_FILENAME);
    let asyncapi = WsApiSpec::asyncapi_spec();
    write_pretty_json(&asyncapi_path, &asyncapi)?;
    if print_paths {
        println!("{}", asyncapi_path.display());
    }

    Ok(())
}

impl ApiDocKind {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "openapi" | OPENAPI_FILENAME => Some(Self::OpenApi),
            "asyncapi" | ASYNCAPI_FILENAME => Some(Self::AsyncApi),
            _ => None,
        }
    }
}

fn render_pretty_json(value: &impl serde::Serialize, description: &str) -> Result<String> {
    let contents =
        serde_json::to_string_pretty(value).with_context(|| format!("serialize {description}"))?;
    Ok(format!("{contents}\n"))
}

fn write_pretty_json(path: &Path, value: &impl serde::Serialize) -> Result<()> {
    let contents = render_pretty_json(value, &path.display().to_string())?;
    fs::write(path, contents).with_context(|| format!("write API docs output {}", path.display()))
}

fn generate_setup_docs(out_dir: &Path) -> Result<()> {
    write_docs(out_dir, DocOutputLayout::Module, false)
}

fn write_docs(out_dir: &Path, layout: DocOutputLayout, print_paths: bool) -> Result<()> {
    for source_id in doc_source_ids() {
        let contents = render_doc_source(source_id)?;
        let output_path = output_path(out_dir, source_id, layout);
        let parent = output_path
            .parent()
            .context("generated docs output path should have a parent directory")?;
        fs::create_dir_all(parent)
            .with_context(|| format!("create docs output directory {}", parent.display()))?;
        fs::write(&output_path, contents)
            .with_context(|| format!("write docs output {}", output_path.display()))?;
        if print_paths {
            println!("{}", output_path.display());
        }
    }

    Ok(())
}

fn output_path(out_dir: &Path, source_id: &str, layout: DocOutputLayout) -> PathBuf {
    let mut output = out_dir.to_path_buf();
    let mut segments = source_id.split('/').peekable();

    while let Some(segment) = segments.next() {
        if segments.peek().is_some() {
            output.push(segment);
        } else {
            match layout {
                DocOutputLayout::Declaration => output.push(format!("{segment}.d.luau")),
                DocOutputLayout::Module => output.push(format!("{segment}.luau")),
            }
        }
    }

    output
}

fn harmony_surfaces() -> &'static [Surface] {
    &[
        surface!(
            "harmony/crypt",
            harmony_crypt::get_module,
            harmony_crypt::render_luau_definition
        ),
        surface!(
            "harmony/serde",
            harmony_serde::get_module,
            harmony_serde::render_luau_definition
        ),
        surface!(
            "harmony/http",
            harmony_http::get_module,
            harmony_http::render_luau_definition
        ),
        surface!(
            "harmony/task",
            harmony_task::get_module,
            harmony_task::render_luau_definition
        ),
        surface!(
            "harmony/net",
            harmony_net::get_module,
            harmony_net::render_luau_definition
        ),
    ]
}

fn read_or_create_luaurc(path: &Path) -> Result<LuaurcConfig> {
    match fs::read_to_string(path) {
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(LuaurcConfig::default()),
        Err(err) => {
            Err(err).with_context(|| format!("failed to read .luaurc at {}", path.display()))
        }
        Ok(contents) => LuaurcConfig::from_json5_str(&contents)
            .with_context(|| format!("failed to parse .luaurc at {}", path.display())),
    }
}

fn write_luaurc(path: &Path, luaurc: &LuaurcConfig) -> Result<()> {
    let mut serialized = luaurc
        .to_pretty_json5_string()
        .with_context(|| format!("failed to serialize .luaurc for {}", path.display()))?;
    serialized.push('\n');
    fs::write(path, serialized).with_context(|| format!("failed to write {}", path.display()))
}

fn merge_setup_aliases(luaurc: &mut LuaurcConfig) {
    luaurc.insert_alias("harmony", format!("./{DEFAULT_SETUP_DOCS_OUT_DIR}/harmony"));
    luaurc.insert_alias("lyra", format!("./{DEFAULT_SETUP_DOCS_OUT_DIR}/lyra"));
}

fn apply_setup_globals(luaurc: &mut LuaurcConfig) {
    luaurc.remove_global("print");
    luaurc.merge_globals(
        harmony_globals::plugin_log_luaurc_global_names()
            .iter()
            .copied(),
    );
}

fn ensure_gitignore_entry(path: &Path, entry: &str) -> Result<()> {
    match fs::read_to_string(path) {
        Err(err) if err.kind() == ErrorKind::NotFound => fs::write(path, format!("{entry}\n"))
            .with_context(|| format!("failed to write {}", path.display())),
        Err(err) => Err(err).with_context(|| format!("failed to read {}", path.display())),
        Ok(mut contents) => {
            if contents
                .lines()
                .map(str::trim)
                .any(|line| line == entry || line == format!("/{entry}"))
            {
                return Ok(());
            }

            if !contents.is_empty() && !contents.ends_with('\n') {
                contents.push('\n');
            }
            contents.push_str(entry);
            contents.push('\n');

            fs::write(path, contents).with_context(|| format!("failed to write {}", path.display()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn render_api_doc_json(kind: ApiDocKind) -> serde_json::Value {
        let output = render_api_doc(kind).expect("render API docs");
        assert!(output.ends_with('\n'));
        serde_json::from_str(&output).expect("valid API docs JSON")
    }

    #[test]
    fn render_api_doc_outputs_openapi_json() {
        let json = render_api_doc_json(ApiDocKind::OpenApi);

        assert!(json.get("openapi").is_some());
        assert_eq!(json["info"]["title"], "Lyra Server REST API");
    }

    #[test]
    fn render_api_doc_outputs_asyncapi_json() {
        let json = render_api_doc_json(ApiDocKind::AsyncApi);

        assert!(json.get("asyncapi").is_some());
        assert_eq!(json["info"]["title"], "Lyra WebSocket Remote Control");
    }
}
