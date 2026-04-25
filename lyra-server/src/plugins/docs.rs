// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use anyhow::{
    Context,
    Result,
    bail,
};
use harmony_core::{
    LuaurcConfig,
    Module,
};
use std::{
    env,
    fs,
    io::ErrorKind,
    path::{
        Path,
        PathBuf,
    },
};

use super::{
    lyra_doc_source_ids,
    lyra_modules,
    render_lyra_doc_source,
};

pub(crate) const DEFAULT_SETUP_DOCS_OUT_DIR: &str = ".lyra/luau";
pub(crate) const DEFAULT_SETUP_DEFS_DIR: &str = ".lyra/defs";
pub(crate) const GLOBALS_DEFINITIONS_FILENAME: &str = "globals.d.luau";

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

struct Surface {
    id: &'static str,
    module: fn() -> Module,
    render_docs: RenderDocsFn,
}

macro_rules! surface {
    ($id:literal, $module:path, $render:path) => {
        Surface {
            id: $id,
            module: $module,
            render_docs: || $render().map_err(anyhow::Error::from),
        }
    };
}

pub(crate) fn runtime_modules() -> Vec<Module> {
    let mut modules = harmony_surfaces()
        .iter()
        .map(|surface| (surface.module)())
        .collect::<Vec<_>>();
    modules.extend(lyra_modules());
    modules
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

pub(crate) fn generate_docs(out_dir: &Path) -> Result<()> {
    write_docs(out_dir, DocOutputLayout::Declaration, true)?;
    let globals_path = out_dir.join(GLOBALS_DEFINITIONS_FILENAME);
    write_globals_definition(&globals_path)?;
    println!("{}", globals_path.display());
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
            print!("{}", render_doc_source(source)?);
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
    "usage:\n  lyra docs list\n  lyra docs print <source>\n  lyra docs generate --out-dir <dir>\n  lyra docs setup"
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
            "harmony/json",
            harmony_json::get_module,
            harmony_json::render_luau_definition
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
    use super::{
        DEFAULT_SETUP_DOCS_OUT_DIR,
        DocOutputLayout,
        GITIGNORE_ENTRY,
        LuaurcConfig,
        doc_source_ids,
        generate_docs,
        generate_setup_docs,
        output_path,
        render_doc_source,
        runtime_modules,
        setup_docs,
    };
    use serde_json::Value as JsonValue;
    use std::{
        env,
        fs,
        path::{
            Path,
            PathBuf,
        },
        process,
        time::{
            SystemTime,
            UNIX_EPOCH,
        },
    };

    struct TestOutDir(PathBuf);

    impl TestOutDir {
        fn new() -> Self {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("current time should be after unix epoch")
                .as_nanos();
            Self(env::temp_dir().join(format!("lyra-surfaces-docs-{}-{unique}", process::id())))
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestOutDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    struct TestProject {
        root: PathBuf,
    }

    impl TestProject {
        fn new() -> Self {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("current time should be after unix epoch")
                .as_nanos();
            let root = env::temp_dir().join(format!("lyra-docs-setup-{}-{unique}", process::id()));
            fs::create_dir_all(&root).expect("create temp project");
            Self { root }
        }

        fn path(&self) -> &Path {
            &self.root
        }

        fn write(&self, relative: &str, contents: &str) {
            let path = self.root.join(relative);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).expect("create parent dirs");
            }
            fs::write(path, contents).expect("write temp file");
        }

        fn read(&self, relative: &str) -> String {
            fs::read_to_string(self.root.join(relative)).expect("read temp file")
        }
    }

    impl Drop for TestProject {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn runtime_modules_include_harmony_and_lyra_modules() {
        let module_paths = runtime_modules()
            .into_iter()
            .map(|module| module.path.to_owned())
            .collect::<Vec<_>>();

        assert!(
            module_paths
                .iter()
                .any(|path| path.as_ref() == "harmony/http")
        );
        assert!(module_paths.iter().any(|path| path.as_ref() == "lyra/api"));
    }

    #[test]
    fn doc_output_path_uses_module_like_layout() {
        let path = output_path(
            Path::new("generated/luau"),
            "harmony/http",
            DocOutputLayout::Declaration,
        );
        assert_eq!(path, Path::new("generated/luau/harmony/http.d.luau"));
    }

    #[test]
    fn setup_doc_output_path_uses_module_file_layout() {
        let path = output_path(
            Path::new(".lyra/luau"),
            "harmony/http",
            DocOutputLayout::Module,
        );
        assert_eq!(path, Path::new(".lyra/luau/harmony/http.luau"));
    }

    #[test]
    fn every_doc_source_renders_non_empty_definition() {
        let source_ids = doc_source_ids().collect::<Vec<_>>();

        assert!(!source_ids.is_empty(), "expected at least one doc source");
        assert!(source_ids.contains(&"harmony/http"));
        assert!(source_ids.contains(&"lyra/metadata"));

        for source_id in source_ids {
            let rendered =
                render_doc_source(source_id).unwrap_or_else(|_| panic!("render {source_id} docs"));
            assert!(
                !rendered.trim().is_empty(),
                "{source_id} rendered an empty definition file"
            );
        }
    }

    #[test]
    fn generate_docs_writes_each_rendered_source_to_its_expected_path() {
        let out_dir = TestOutDir::new();
        let source_ids = doc_source_ids().collect::<Vec<_>>();

        generate_docs(out_dir.path()).expect("generate doc files");

        for source_id in source_ids {
            let output_path = output_path(out_dir.path(), source_id, DocOutputLayout::Declaration);
            let written = fs::read_to_string(&output_path)
                .unwrap_or_else(|_| panic!("read generated docs at {}", output_path.display()));
            let rendered =
                render_doc_source(source_id).unwrap_or_else(|_| panic!("render {source_id} docs"));

            assert!(
                output_path.is_file(),
                "{source_id} should generate {}",
                output_path.display()
            );
            assert_eq!(
                written, rendered,
                "{source_id} should write the same content it renders"
            );
        }
    }

    #[test]
    fn generate_setup_docs_writes_module_file_layout() {
        let out_dir = TestOutDir::new();
        let source_ids = doc_source_ids().collect::<Vec<_>>();

        generate_setup_docs(out_dir.path()).expect("generate setup doc files");

        for source_id in source_ids {
            let output_path = output_path(out_dir.path(), source_id, DocOutputLayout::Module);
            let written = fs::read_to_string(&output_path).unwrap_or_else(|_| {
                panic!("read generated setup docs at {}", output_path.display())
            });
            let rendered =
                render_doc_source(source_id).unwrap_or_else(|_| panic!("render {source_id} docs"));

            assert!(
                output_path.is_file(),
                "{source_id} should generate {}",
                output_path.display()
            );
            assert_eq!(
                written, rendered,
                "{source_id} should write the same content it renders"
            );
        }
    }

    fn assert_doc_shape(source_id: &str, required: &[&str], forbidden: &[&str]) {
        let rendered =
            render_doc_source(source_id).unwrap_or_else(|_| panic!("render {source_id} docs"));

        for fragment in required {
            assert!(
                rendered.contains(fragment),
                "{source_id} should contain `{fragment}`"
            );
        }

        for fragment in forbidden {
            assert!(
                !rendered.contains(fragment),
                "{source_id} should not contain `{fragment}`"
            );
        }
    }

    #[test]
    fn targeted_doc_sources_preserve_known_output_shapes() {
        assert_doc_shape(
            "lyra/server",
            &[
                "@class Server",
                "export type ServerInfo = {",
                "function server.info(): ServerInfo",
                "commit_hash: string",
            ],
            &[],
        );
        assert_doc_shape(
            "lyra/metadata",
            &[
                "@class Metadata",
                "@type ProviderCoverHandler (ctx: ProviderCoverContext) -> JsonValue",
                "@interface ProviderCoverOptions",
                ".force_refresh boolean?",
                "@interface ProviderCoverContext",
                ".cover_options ProviderCoverOptions?",
                "metadata.Provider = {}",
                "function metadata.Provider.new(id: string): Provider",
                "@interface Provider",
                "@interface Layer",
            ],
            &[],
        );
    }

    #[test]
    fn setup_docs_creates_editor_docs_and_is_idempotent() {
        let project = TestProject::new();

        setup_docs(project.path()).expect("run docs setup");
        setup_docs(project.path()).expect("repeat docs setup without changing output");

        let luaurc: LuaurcConfig = LuaurcConfig::from_json5_str(&project.read(".luaurc"))
            .expect("parse serialized .luaurc");
        assert_eq!(
            luaurc.aliases.get("harmony"),
            Some(&format!("./{DEFAULT_SETUP_DOCS_OUT_DIR}/harmony"))
        );
        assert_eq!(
            luaurc.aliases.get("lyra"),
            Some(&format!("./{DEFAULT_SETUP_DOCS_OUT_DIR}/lyra"))
        );
        assert_eq!(luaurc.globals.len(), 1);
        assert!(luaurc.globals.contains("warn"));
        assert_eq!(project.read(".gitignore"), format!("{GITIGNORE_ENTRY}\n"));
        assert!(
            project
                .path()
                .join(Path::new(DEFAULT_SETUP_DOCS_OUT_DIR))
                .join("harmony/http.luau")
                .is_file()
        );
        assert!(
            project
                .path()
                .join(Path::new(DEFAULT_SETUP_DOCS_OUT_DIR))
                .join("lyra/api.luau")
                .is_file()
        );
        let globals_path = project
            .path()
            .join(Path::new(super::DEFAULT_SETUP_DEFS_DIR))
            .join(super::GLOBALS_DEFINITIONS_FILENAME);
        assert!(
            globals_path.is_file(),
            "globals definition file should exist"
        );
        let globals_contents = fs::read_to_string(&globals_path).expect("read globals.d.luau");
        assert!(globals_contents.contains("declare function warn(...: any)"));
        assert!(!globals_contents.contains("declare function print(...: any)"));
    }

    #[test]
    fn setup_docs_merges_existing_json5_luaurc_and_preserves_unrelated_values() {
        let project = TestProject::new();
        project.write(
            ".luaurc",
            r#"{
                typeErrors: true,
                globals: ["project_defined_global", "print"],
                aliases: {
                    foo: "./foo",
                    lyra: "./old/location",
                },
            }"#,
        );
        project.write(".gitignore", "/target\n");

        setup_docs(project.path()).expect("merge into existing docs setup");

        let luaurc: LuaurcConfig = LuaurcConfig::from_json5_str(&project.read(".luaurc"))
            .expect("parse rewritten .luaurc");
        assert_eq!(luaurc.other["typeErrors"], JsonValue::Bool(true));
        assert_eq!(luaurc.aliases.get("foo"), Some(&"./foo".to_string()));
        assert_eq!(
            luaurc.aliases.get("harmony"),
            Some(&format!("./{DEFAULT_SETUP_DOCS_OUT_DIR}/harmony"))
        );
        assert_eq!(
            luaurc.aliases.get("lyra"),
            Some(&format!("./{DEFAULT_SETUP_DOCS_OUT_DIR}/lyra"))
        );
        assert!(luaurc.globals.contains("project_defined_global"));
        assert!(!luaurc.globals.contains("print"));
        assert!(luaurc.globals.contains("warn"));
        assert_eq!(project.read(".gitignore"), "/target\n.lyra/\n");
    }

    #[test]
    fn setup_docs_rejects_config_luau() {
        let project = TestProject::new();
        project.write(".config.luau", "return {}");

        let error = setup_docs(project.path()).expect_err("reject conflicting config");

        assert!(error.to_string().contains(".config.luau"));
        assert!(!project.path().join(".luaurc").exists());
    }
}
