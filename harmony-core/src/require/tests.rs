// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::{
    CapabilityDenied,
    setup,
};
use crate::{
    Danger,
    Module,
    PluginManager,
    Scope,
};
use mlua::Lua;
use std::collections::HashSet;
use std::path::{
    Path,
    PathBuf,
};
use std::sync::{
    Arc,
    Mutex,
    RwLock,
};
use std::time::{
    SystemTime,
    UNIX_EPOCH,
};

static CWD_LOCK: Mutex<()> = Mutex::new(());

struct CwdGuard(PathBuf);

impl Drop for CwdGuard {
    fn drop(&mut self) {
        let _ = std::env::set_current_dir(&self.0);
    }
}

/// Build a scoped module. Every module is gated now, so pass the
/// scope id the plugin manifest will need to declare.
fn test_module(path: &str, scope_id: &str, value: i64) -> Module {
    Module {
        path: path.into(),
        setup: Arc::new(move |lua: &Lua| -> anyhow::Result<mlua::Table> {
            let table = lua.create_table()?;
            table.set("value", value)?;
            Ok(table)
        }),
        scope: Scope {
            id: scope_id.into(),
            description: "test module",
            danger: Danger::Negligible,
        },
    }
}

/// Empty `PluginManager` for tests that only exercise file-based
/// require (`./foo`, `@self/bar`) — the gate only fires on
/// alias-prefixed paths bound to a registered `Module`, so tests
/// without `@harmony/` or `@lyra/` requires don't need a plugin.
fn empty_plugin_manager() -> Arc<RwLock<PluginManager>> {
    Arc::new(RwLock::new(PluginManager::new(PathBuf::from("plugins"))))
}

fn temp_workspace(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("lyra-require-{name}-{nanos}"));
    std::fs::create_dir_all(&dir).expect("create temp workspace");
    dir
}

fn enter_cwd(path: &Path) -> CwdGuard {
    let original = std::env::current_dir().expect("cwd should exist");
    std::env::set_current_dir(path).expect("set cwd");
    CwdGuard(original)
}

/// Set up a two-plugin workspace and write each plugin's `init.luau`
/// with the provided body. Returns the workspace directory so the
/// caller can clean up after the test.
fn two_plugin_workspace(
    name: &str,
    plugin_a_scopes: &[&str],
    plugin_a_body: &str,
    plugin_b_scopes: &[&str],
    plugin_b_body: &str,
) -> (PathBuf, Arc<RwLock<PluginManager>>) {
    let workspace = temp_workspace(name);
    let plugins_dir = workspace.join("plugins");
    std::fs::create_dir_all(&plugins_dir).expect("create plugins dir");

    for (id, scopes, body) in [
        ("alpha", plugin_a_scopes, plugin_a_body),
        ("beta", plugin_b_scopes, plugin_b_body),
    ] {
        let plugin_dir = plugins_dir.join(id);
        std::fs::create_dir_all(&plugin_dir).expect("create plugin dir");
        let scopes_json = scopes
            .iter()
            .map(|s| format!("\"{s}\""))
            .collect::<Vec<_>>()
            .join(", ");
        std::fs::write(
            plugin_dir.join("plugin.json"),
            format!(
                r#"{{
                  "schema_version": 1,
                  "id": "{id}",
                  "name": "{id}",
                  "version": "0.0.1",
                  "description": "test",
                  "entrypoint": "init.luau",
                  "scopes": [{scopes_json}]
                }}"#
            ),
        )
        .expect("write manifest");
        std::fs::write(plugin_dir.join("init.luau"), body).expect("write init");
    }

    let mut manager = PluginManager::new(plugins_dir);
    // Build the valid-scope-id set from the caller's lists so `load_plugin`
    // accepts the manifests.
    let all_scopes: HashSet<Arc<str>> = plugin_a_scopes
        .iter()
        .chain(plugin_b_scopes.iter())
        .map(|s| Arc::from(*s))
        .collect();
    let errors = manager
        .discover_plugins(&all_scopes)
        .expect("discover plugins");
    assert!(errors.is_empty(), "unexpected manifest errors: {errors:?}");
    (workspace, Arc::new(RwLock::new(manager)))
}

/// Execute `init.luau` for `plugin_id` under a chunk name that the
/// gate's stack walk will recognize — `plugins/<id>/init`, same
/// pattern `Harmony::exec` uses.
fn exec_plugin(lua: &Lua, workspace: &Path, plugin_id: &str) -> mlua::Result<()> {
    let init = workspace.join("plugins").join(plugin_id).join("init.luau");
    let source = std::fs::read_to_string(&init).expect("read init");
    let chunk_name = format!("plugins/{plugin_id}/init");
    lua.load(&source).set_name(&chunk_name).exec()
}

#[test]
fn gated_require_allowed_when_scope_declared() {
    let _guard = CWD_LOCK.lock().unwrap_or_else(|err| err.into_inner());

    let (workspace, manager) = two_plugin_workspace(
        "gate-allowed",
        &["test.datastore"],
        "local m = require('@test/datastore'); _G.alpha_value = m.value",
        &[],
        "",
    );
    let _cwd = enter_cwd(&workspace);

    let lua = Lua::new();
    let modules: Arc<[Module]> =
        Arc::from(vec![test_module("test/datastore", "test.datastore", 42)]);
    setup(&lua, &modules, manager).expect("setup");

    exec_plugin(&lua, &workspace, "alpha").expect("alpha exec");
    let value: i64 = lua.globals().get("alpha_value").expect("read global");
    assert_eq!(value, 42);

    let _ = std::fs::remove_dir_all(&workspace);
}

#[test]
fn gated_require_denied_when_scope_missing_and_error_downcasts() {
    let _guard = CWD_LOCK.lock().unwrap_or_else(|err| err.into_inner());

    let (workspace, manager) =
        two_plugin_workspace("gate-denied", &[], "require('@test/datastore')", &[], "");
    let _cwd = enter_cwd(&workspace);

    let lua = Lua::new();
    let modules: Arc<[Module]> =
        Arc::from(vec![test_module("test/datastore", "test.datastore", 42)]);
    setup(&lua, &modules, manager).expect("setup");

    let err = exec_plugin(&lua, &workspace, "alpha").expect_err("should deny");
    // Walk the error chain (mlua wraps external errors) to find the
    // typed `CapabilityDenied`. If the denial is stringified somewhere
    // along the chain this downcast fails — that's the regression
    // guard against reverting to `Error::runtime(format!(...))`.
    let denied: Option<&CapabilityDenied> = find_capability_denied(&err);
    let denied = denied.expect("error must downcast to CapabilityDenied");
    assert_eq!(denied.plugin_id, "alpha");
    assert_eq!(denied.scope_id, "test.datastore");

    let _ = std::fs::remove_dir_all(&workspace);
}

#[test]
fn manifest_reload_updates_declared_scopes_without_rebuilding_require() {
    let _guard = CWD_LOCK.lock().unwrap_or_else(|err| err.into_inner());

    let (workspace, manager) = two_plugin_workspace(
        "scope-reload",
        &[],
        "local m = require('@test/datastore'); _G.reloaded_value = m.value",
        &[],
        "",
    );
    let _cwd = enter_cwd(&workspace);

    let lua = Lua::new();
    let modules: Arc<[Module]> =
        Arc::from(vec![test_module("test/datastore", "test.datastore", 42)]);
    setup(&lua, &modules, manager.clone()).expect("setup");

    let err = exec_plugin(&lua, &workspace, "alpha").expect_err("should deny before reload");
    let denied = find_capability_denied(&err).expect("error must downcast to CapabilityDenied");
    assert_eq!(denied.scope_id, "test.datastore");

    std::fs::write(
        workspace.join("plugins").join("alpha").join("plugin.json"),
        r#"{
          "schema_version": 1,
          "id": "alpha",
          "name": "alpha",
          "version": "0.0.1",
          "description": "test",
          "entrypoint": "init.luau",
          "scopes": ["test.datastore"]
        }"#,
    )
    .expect("rewrite manifest");
    let valid_scope_ids: HashSet<Arc<str>> = [Arc::from("test.datastore")].into_iter().collect();
    manager
        .write()
        .expect("plugin manager lock")
        .reload_plugin("alpha", &valid_scope_ids)
        .expect("reload manifest");

    exec_plugin(&lua, &workspace, "alpha").expect("alpha exec after manifest reload");
    let value: i64 = lua.globals().get("reloaded_value").expect("read global");
    assert_eq!(value, 42);

    let _ = std::fs::remove_dir_all(&workspace);
}

#[test]
fn cache_bypass_regression_second_plugin_still_denied() {
    let _guard = CWD_LOCK.lock().unwrap_or_else(|err| err.into_inner());

    // Alpha declares the scope and pre-warms the module cache. Beta
    // declares no scopes and must still be denied — a pre-warmed cache
    // must not mask the gate.
    let (workspace, manager) = two_plugin_workspace(
        "cache-bypass",
        &["test.datastore"],
        "local m = require('@test/datastore'); _G.alpha_value = m.value",
        &[],
        "require('@test/datastore')",
    );
    let _cwd = enter_cwd(&workspace);

    let lua = Lua::new();
    let modules: Arc<[Module]> =
        Arc::from(vec![test_module("test/datastore", "test.datastore", 7)]);
    setup(&lua, &modules, manager).expect("setup");

    exec_plugin(&lua, &workspace, "alpha").expect("alpha should succeed");
    let alpha_val: i64 = lua.globals().get("alpha_value").expect("alpha global");
    assert_eq!(alpha_val, 7);

    let err = exec_plugin(&lua, &workspace, "beta").expect_err("beta must be denied");
    let denied =
        find_capability_denied(&err).expect("beta error must downcast to CapabilityDenied");
    assert_eq!(denied.plugin_id, "beta");
    assert_eq!(denied.scope_id, "test.datastore");

    let _ = std::fs::remove_dir_all(&workspace);
}

#[test]
fn negligible_scope_still_requires_declaration() {
    // Every module is gated (even `Danger::Negligible` ones). The
    // install UI collapses negligible-danger scopes into a utility
    // summary, but the plugin still has to list them in its manifest.
    let _guard = CWD_LOCK.lock().unwrap_or_else(|err| err.into_inner());

    let (workspace, manager) = two_plugin_workspace(
        "negligible-declared",
        &["test.helper"],
        "local m = require('@test/helper'); _G.result = m.value",
        &[],
        "",
    );
    let _cwd = enter_cwd(&workspace);

    let lua = Lua::new();
    let modules: Arc<[Module]> = Arc::from(vec![test_module("test/helper", "test.helper", 99)]);
    setup(&lua, &modules, manager).expect("setup");

    exec_plugin(&lua, &workspace, "alpha").expect("alpha exec");
    let v: i64 = lua.globals().get("result").expect("result");
    assert_eq!(v, 99);

    let _ = std::fs::remove_dir_all(&workspace);
}

#[test]
fn forged_chunk_name_for_unknown_plugin_is_denied() {
    // A chunk name like `plugins/ghost/init` that doesn't correspond
    // to any PluginManager-registered plugin resolves to None in the
    // gate's identity resolver — fail-closed denies the require.
    let lua = Lua::new();
    let modules: Arc<[Module]> =
        Arc::from(vec![test_module("test/datastore", "test.datastore", 1)]);
    let manager = Arc::new(RwLock::new(PluginManager::new(PathBuf::from(
        "/tmp/nonexistent-plugins-dir",
    ))));
    setup(&lua, &modules, manager).expect("setup");

    let err = lua
        .load("require('@test/datastore')")
        .set_name("plugins/ghost/init")
        .exec()
        .expect_err("forged chunk name should be denied");
    let denied =
        find_capability_denied(&err).expect("forged-chunk error must downcast to CapabilityDenied");
    // `<non-plugin caller>` is the sentinel the wrapper uses when
    // the stack walk finds no PluginManager-registered identity.
    assert_eq!(denied.plugin_id, "<non-plugin caller>");
}

fn find_capability_denied(err: &mlua::Error) -> Option<&CapabilityDenied> {
    // mlua wraps Rust callback errors in `CallbackError { cause: ExternalError(..) }`.
    // `Error::chain()` walks those wrappers so we can reach the typed payload.
    err.chain()
        .find_map(|e| e.downcast_ref::<CapabilityDenied>())
}

#[test]
fn require_imports_modules() {
    let _guard = CWD_LOCK.lock().unwrap_or_else(|err| err.into_inner());

    let (workspace, manager) = two_plugin_workspace(
        "imports",
        &["harmony.test", "lyra.test"],
        r#"
            local a = require("@harmony/test")
            local b = require("@lyra/test")
            _G.a_value = a.value
            _G.b_value = b.value
        "#,
        &[],
        "",
    );
    let _cwd = enter_cwd(&workspace);

    let lua = Lua::new();
    let modules = Arc::from(vec![
        test_module("harmony/test", "harmony.test", 10),
        test_module("lyra/test", "lyra.test", 20),
    ]);
    setup(&lua, &modules, manager).unwrap();

    exec_plugin(&lua, &workspace, "alpha").unwrap();
    assert_eq!(lua.globals().get::<i64>("a_value").unwrap(), 10);
    assert_eq!(lua.globals().get::<i64>("b_value").unwrap(), 20);

    let _ = std::fs::remove_dir_all(&workspace);
}

#[test]
fn require_caches_modules() {
    let _guard = CWD_LOCK.lock().unwrap_or_else(|err| err.into_inner());

    let (workspace, manager) = two_plugin_workspace(
        "cache",
        &["harmony.cache"],
        r#"
            local a = require("@harmony/cache")
            local b = require("@harmony/cache")
            _G.cached = (a == b)
        "#,
        &[],
        "",
    );
    let _cwd = enter_cwd(&workspace);

    let lua = Lua::new();
    let modules = Arc::from(vec![test_module("harmony/cache", "harmony.cache", 1)]);
    setup(&lua, &modules, manager).unwrap();

    exec_plugin(&lua, &workspace, "alpha").unwrap();
    assert!(lua.globals().get::<bool>("cached").unwrap());

    let _ = std::fs::remove_dir_all(&workspace);
}

#[test]
fn plugin_file_require_cache_can_be_invalidated_for_reload() {
    let _guard = CWD_LOCK.lock().unwrap_or_else(|err| err.into_inner());

    let (workspace, manager) = two_plugin_workspace(
        "plugin-file-cache-invalidate",
        &[],
        "local value = require('@self/value'); _G.loaded_value = value.value",
        &[],
        "",
    );
    let alpha_dir = workspace.join("plugins").join("alpha");
    let value_path = alpha_dir.join("value.luau");
    std::fs::write(&value_path, "return { value = 1 }").expect("write initial helper");

    let _cwd = enter_cwd(&workspace);

    let lua = Lua::new();
    let modules: Arc<[Module]> = Arc::from(Vec::<Module>::new());
    let cache = setup(&lua, &modules, manager).expect("setup require");

    exec_plugin(&lua, &workspace, "alpha").expect("alpha initial exec");
    assert_eq!(lua.globals().get::<i64>("loaded_value").unwrap(), 1);

    std::fs::write(&value_path, "return { value = 2 }").expect("rewrite helper");
    exec_plugin(&lua, &workspace, "alpha").expect("alpha cached exec");
    assert_eq!(lua.globals().get::<i64>("loaded_value").unwrap(), 1);

    cache.invalidate_plugin_root(&alpha_dir);
    exec_plugin(&lua, &workspace, "alpha").expect("alpha invalidated exec");
    assert_eq!(lua.globals().get::<i64>("loaded_value").unwrap(), 2);

    let _ = std::fs::remove_dir_all(&workspace);
}

#[test]
fn relative_plugin_root_invalidation_reloads_cached_absolute_require_root() {
    let _guard = CWD_LOCK.lock().unwrap_or_else(|err| err.into_inner());

    let workspace = temp_workspace("relative-plugin-cache-invalidate");
    let plugins_dir = workspace.join("plugins");
    let alpha_dir = plugins_dir.join("alpha");
    std::fs::create_dir_all(&alpha_dir).expect("create alpha plugin dir");
    std::fs::write(
        alpha_dir.join("plugin.json"),
        r#"{
		  "schema_version": 1,
		  "id": "alpha",
		  "name": "alpha",
		  "version": "0.0.1",
		  "description": "test",
		  "entrypoint": "init.luau",
		  "scopes": []
		}"#,
    )
    .expect("write manifest");
    std::fs::write(
        alpha_dir.join("init.luau"),
        "local value = require('@self/value'); _G.loaded_value = value.value",
    )
    .expect("write init");
    let value_path = alpha_dir.join("value.luau");
    std::fs::write(&value_path, "return { value = 1 }").expect("write initial helper");

    let _cwd = enter_cwd(&workspace);

    let mut manager = PluginManager::new(PathBuf::from("plugins"));
    let errors = manager
        .discover_plugins(&HashSet::new())
        .expect("discover plugins");
    assert!(errors.is_empty(), "unexpected manifest errors: {errors:?}");
    let manager = Arc::new(RwLock::new(manager));

    let lua = Lua::new();
    let modules: Arc<[Module]> = Arc::from(Vec::<Module>::new());
    let cache = setup(&lua, &modules, manager).expect("setup require");

    exec_plugin(&lua, &workspace, "alpha").expect("alpha initial exec");
    assert_eq!(lua.globals().get::<i64>("loaded_value").unwrap(), 1);

    std::fs::write(&value_path, "return { value = 2 }").expect("rewrite helper");
    exec_plugin(&lua, &workspace, "alpha").expect("alpha cached exec");
    assert_eq!(lua.globals().get::<i64>("loaded_value").unwrap(), 1);

    cache.invalidate_plugin_root(&PathBuf::from("plugins").join("alpha"));
    exec_plugin(&lua, &workspace, "alpha").expect("alpha relative invalidated exec");
    assert_eq!(lua.globals().get::<i64>("loaded_value").unwrap(), 2);

    let _ = std::fs::remove_dir_all(&workspace);
}

#[test]
fn required_plugin_files_use_plugin_chunk_names() {
    let _guard = CWD_LOCK.lock().unwrap_or_else(|err| err.into_inner());

    let (workspace, manager) = two_plugin_workspace(
        "plugin-file-chunk-name",
        &[],
        "local helper = require('@self/helper'); _G.helper_sources = helper.sources",
        &[],
        "",
    );
    let alpha_dir = workspace.join("plugins").join("alpha");
    std::fs::write(
        alpha_dir.join("helper.luau"),
        "return { sources = caller_sources() }",
    )
    .expect("write helper");

    let _cwd = enter_cwd(&workspace);

    let lua = Lua::new();
    let caller_sources = lua
        .create_function(|lua, ()| {
            let mut sources = Vec::new();
            for level in 1..=8 {
                if let Some(source) = lua
                    .inspect_stack(level, |debug| {
                        debug.source().source.map(|cow| cow.into_owned())
                    })
                    .flatten()
                {
                    sources.push(source);
                }
            }
            Ok(sources.join("\n"))
        })
        .expect("create caller source function");
    lua.globals()
        .set("caller_sources", caller_sources)
        .expect("install caller source function");

    let modules: Arc<[Module]> = Arc::from(Vec::<Module>::new());
    setup(&lua, &modules, manager).expect("setup require");

    exec_plugin(&lua, &workspace, "alpha").expect("alpha exec");
    let sources: String = lua.globals().get("helper_sources").expect("helper sources");
    assert!(
        sources.contains("plugins/alpha/helper.luau"),
        "expected plugin-relative chunk name in sources:\n{sources}"
    );

    let _ = std::fs::remove_dir_all(&workspace);
}

#[test]
fn require_resolves_relative_paths_from_non_init_files() {
    let _guard = CWD_LOCK.lock().unwrap_or_else(|err| err.into_inner());

    let workspace = temp_workspace("resolve");
    let package_dir = workspace.join("package");
    std::fs::create_dir_all(package_dir.join("nested")).expect("create package directories");
    std::fs::write(
        package_dir.join("main.luau"),
        "local a = require('./token')\nlocal b = require('./nested')\nreturn { a = a.value, b = b.value }",
    )
    .expect("write main script");
    std::fs::write(package_dir.join("token.lua"), "return { value = 10 }").expect("write lua file");
    std::fs::write(
        package_dir.join("nested/init.luau"),
        "return { value = 20 }",
    )
    .expect("write nested init");

    let _cwd = enter_cwd(&workspace);

    let lua = Lua::new();
    let modules = Arc::from(Vec::<Module>::new());
    setup(&lua, &modules, empty_plugin_manager()).expect("setup require");

    let result: mlua::Table = lua
        .load("return require('./package/main')")
        .set_name("entry")
        .eval()
        .expect("evaluate require");
    assert_eq!(result.get::<i64>("a").expect("table field a"), 10);
    assert_eq!(result.get::<i64>("b").expect("table field b"), 20);

    let _ = std::fs::remove_dir_all(&workspace);
}

#[test]
fn require_treats_init_as_abstract_module_and_supports_self_alias() {
    let _guard = CWD_LOCK.lock().unwrap_or_else(|err| err.into_inner());

    let workspace = temp_workspace("init-abstract");
    let package_dir = workspace.join("package");
    std::fs::create_dir_all(&package_dir).expect("create package directory");
    std::fs::write(workspace.join("foo.luau"), "return { value = 10 }").expect("write outer foo");
    std::fs::write(package_dir.join("foo.luau"), "return { value = 20 }").expect("write inner foo");
    std::fs::write(
        package_dir.join("init.luau"),
        "local outer = require('./foo')\nlocal inner = require('@self/foo')\nreturn { outer = outer.value, inner = inner.value }",
    )
    .expect("write init script");

    let _cwd = enter_cwd(&workspace);

    let lua = Lua::new();
    let modules = Arc::from(Vec::<Module>::new());
    setup(&lua, &modules, empty_plugin_manager()).expect("setup require");

    let result: mlua::Table = lua
        .load("return require('./package')")
        .set_name("entry")
        .eval()
        .expect("evaluate require");
    assert_eq!(result.get::<i64>("outer").expect("table field outer"), 10);
    assert_eq!(result.get::<i64>("inner").expect("table field inner"), 20);

    let _ = std::fs::remove_dir_all(&workspace);
}

#[test]
fn require_reports_ambiguous_modules() {
    let _guard = CWD_LOCK.lock().unwrap_or_else(|err| err.into_inner());

    let workspace = temp_workspace("ambiguous");
    let plugin_dir = workspace.join("plugins/demo");
    std::fs::create_dir_all(&plugin_dir).expect("create plugin directory");

    std::fs::write(plugin_dir.join("init.luau"), "return require('./dup')")
        .expect("write init script");
    std::fs::write(plugin_dir.join("dup.luau"), "return 1").expect("write dup luau");
    std::fs::write(plugin_dir.join("dup.lua"), "return 2").expect("write dup lua");

    let _cwd = enter_cwd(&workspace);

    let lua = Lua::new();
    let modules = Arc::from(Vec::<Module>::new());
    setup(&lua, &modules, empty_plugin_manager()).expect("setup require");

    let err = lua
        .load("return require('./plugins/demo')")
        .set_name("entry")
        .eval::<mlua::Value>()
        .expect_err("ambiguous require should fail");
    assert!(err.to_string().contains("ambiguous"));

    let _ = std::fs::remove_dir_all(&workspace);
}

#[test]
fn plugin_require_cannot_escape_plugin_directory() {
    let _guard = CWD_LOCK.lock().unwrap_or_else(|err| err.into_inner());

    let workspace = temp_workspace("scope");
    let plugin_dir = workspace.join("plugins/demo");
    std::fs::create_dir_all(&plugin_dir).expect("create plugin directory");
    std::fs::create_dir_all(workspace.join("plugins/shared")).expect("create shared directory");

    std::fs::write(
        plugin_dir.join("init.luau"),
        "return require('../shared/secret')",
    )
    .expect("write init script");
    std::fs::write(workspace.join("plugins/shared/secret.luau"), "return 1")
        .expect("write escape target");

    let _cwd = enter_cwd(&workspace);

    let lua = Lua::new();
    let modules = Arc::from(Vec::<Module>::new());
    setup(&lua, &modules, empty_plugin_manager()).expect("setup require");

    let err = lua
        .load("return require('./plugins/demo')")
        .set_name("entry")
        .eval::<mlua::Value>()
        .expect_err("escape should fail");
    let message = err.to_string();
    assert!(
        message.contains("could not resolve")
            || message.contains("not found")
            || message.contains("could not get parent")
    );

    let _ = std::fs::remove_dir_all(&workspace);
}
