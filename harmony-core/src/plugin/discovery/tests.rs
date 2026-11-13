use super::execution_order;

use crate::plugin::testing::*;
use crate::plugin::{
    LoadedPlugin,
    PluginLoadError,
    PluginManager,
};

use semver::Version;
use std::collections::BTreeMap;
use std::path::PathBuf;

fn manager_with(plugins: Vec<LoadedPlugin>) -> PluginManager {
    let mut mgr = PluginManager::new(PathBuf::from("plugins"));
    for plugin in plugins {
        mgr.by_root
            .insert(plugin.directory.clone(), plugin.manifest.id.clone());
        mgr.plugins.insert(plugin.manifest.id.clone(), plugin);
    }
    mgr
}

#[test]
fn execution_order_orders_after_every_installed_dependency() {
    let order = execution_order_ids(vec![
        loaded_plugin(
            "consumer",
            "1.0.0",
            dependencies(&[("a", dependency("1")), ("b", optional("1"))]),
        ),
        loaded_plugin("b", "1.0.0", BTreeMap::new()),
        loaded_plugin("a", "1.0.0", BTreeMap::new()),
    ]);
    assert_eq!(order, vec!["a", "b", "consumer"]);
}

#[test]
fn execution_order_is_stable_via_id_sort() {
    let order = execution_order_ids(vec![
        loaded_plugin("zeta", "1.0.0", BTreeMap::new()),
        loaded_plugin("alpha", "1.0.0", BTreeMap::new()),
        loaded_plugin("mu", "1.0.0", BTreeMap::new()),
    ]);
    assert_eq!(order, vec!["alpha", "mu", "zeta"]);
}

#[test]
fn execution_order_rejects_cycles() {
    let error = execution_order(vec![
        loaded_plugin("a", "1.0.0", dependencies(&[("b", dependency("1"))])),
        loaded_plugin("b", "1.0.0", dependencies(&[("a", dependency("1"))])),
    ])
    .expect_err("a cycle has no execution order");
    assert!(error.to_string().contains("cycle"), "{error}");
}

#[test]
fn resolve_prunes_plugins_with_missing_required_dep() {
    let mut mgr = manager_with(vec![loaded_plugin(
        "consumer",
        "1.0.0",
        dependencies(&[("missing", dependency("1"))]),
    )]);
    let errors = mgr.resolve_dependencies();
    assert_eq!(errors.len(), 1);
    assert_eq!(
        errors[0].to_string(),
        "plugin 'consumer' requires plugin 'missing' ^1, but it is not installed or failed \
         to load"
    );
    assert!(mgr.get_plugin("consumer").is_none());
}

#[test]
fn resolve_keeps_plugins_with_missing_optional_dep() {
    let mut mgr = manager_with(vec![loaded_plugin(
        "consumer",
        "1.0.0",
        dependencies(&[("missing", optional("1"))]),
    )]);
    let errors = mgr.resolve_dependencies();
    assert!(errors.is_empty());
    assert!(mgr.get_plugin("consumer").is_some());
}

#[test]
fn resolve_checks_installed_dependency_versions() {
    let mut mgr = manager_with(vec![
        loaded_plugin("lib", "0.2.5", BTreeMap::new()),
        loaded_plugin("extra", "0.0.4", BTreeMap::new()),
        loaded_plugin(
            "matching",
            "1.0.0",
            dependencies(&[("lib", dependency("0.2")), ("extra", optional("0.0.4"))]),
        ),
        loaded_plugin(
            "too_old",
            "1.0.0",
            dependencies(&[("lib", dependency("0.3"))]),
        ),
        loaded_plugin(
            "optional_mismatch",
            "1.0.0",
            dependencies(&[("extra", optional("0.0.3"))]),
        ),
    ]);
    let errors = mgr.resolve_dependencies();
    let messages: Vec<String> = errors.iter().map(ToString::to_string).collect();
    assert_eq!(
        messages,
        vec![
            "plugin 'optional_mismatch' requires plugin 'extra' ^0.0.3, but 0.0.4 is installed",
            "plugin 'too_old' requires plugin 'lib' ^0.3, but 0.2.5 is installed",
        ]
    );
    assert!(mgr.get_plugin("matching").is_some());
    assert!(mgr.get_plugin("too_old").is_none());
    assert!(mgr.get_plugin("optional_mismatch").is_none());
}

#[test]
fn resolve_cascades_pruning_through_chain() {
    // c → b → a with a missing: b is dropped, and c only when it
    // requires b.
    let mut mgr = manager_with(vec![
        loaded_plugin("c", "1.0.0", dependencies(&[("b", optional("1"))])),
        loaded_plugin("b", "1.0.0", dependencies(&[("a", dependency("1"))])),
    ]);
    let errors = mgr.resolve_dependencies();
    assert_eq!(errors.len(), 1);
    assert!(mgr.get_plugin("b").is_none());
    assert!(mgr.get_plugin("c").is_some());

    let mut mgr = manager_with(vec![
        loaded_plugin("c", "1.0.0", dependencies(&[("b", dependency("1"))])),
        loaded_plugin("b", "1.0.0", dependencies(&[("a", dependency("1"))])),
    ]);
    let errors = mgr.resolve_dependencies();
    let directories: Vec<_> = errors.iter().map(|error| &error.directories).collect();
    assert_eq!(directories, [&["b"], &["c"]]);
    assert!(mgr.get_plugin("b").is_none());
    assert!(mgr.get_plugin("c").is_none());
}

#[test]
fn resolve_prunes_cycles_and_rechecks_what_depends_on_them() {
    let mut mgr = manager_with(vec![
        loaded_plugin("a", "1.0.0", dependencies(&[("b", dependency("1"))])),
        loaded_plugin("b", "1.0.0", dependencies(&[("a", optional("1"))])),
        loaded_plugin("optional", "1.0.0", dependencies(&[("a", optional("1"))])),
        loaded_plugin("required", "1.0.0", dependencies(&[("a", dependency("1"))])),
    ]);
    let errors = mgr.resolve_dependencies();
    let messages: Vec<String> = errors.iter().map(ToString::to_string).collect();
    assert_eq!(
        messages,
        vec![
            r#"dependency cycle detected; the following plugins are in a cycle and were skipped: ["a", "b"]"#,
            "plugin 'required' requires plugin 'a' ^1, but it is not installed or failed to \
             load",
        ]
    );
    assert_eq!(order(&mgr), vec!["optional"]);
}

#[test]
fn missing_entrypoint_passes_the_manifest_but_fails_the_directory() {
    let dir = temp_plugin_dir("missing-entrypoint");
    let json = manifest_json("demo", "");
    std::fs::write(dir.join(crate::plugin::PLUGIN_CONFIG_FILENAME), &json).unwrap();
    assert!(load_manifest(json.as_bytes()).is_ok());
    let err = PluginManager::load_plugin_dir(&dir, &host()).unwrap_err();
    assert!(
        matches!(&err, PluginLoadError::EntrypointNotFound { path, .. } if *path == dir.join("init.luau")),
        "{err}"
    );

    std::fs::write(dir.join("init.luau"), "").unwrap();
    let loaded = PluginManager::load_plugin_dir(&dir, &host()).unwrap();
    assert_eq!(loaded.directory, dir);
    assert!(loaded.declared_scopes.contains("net"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn load_plugin_dir_reports_config_errors_against_the_config_path() {
    let dir = temp_plugin_dir("config-errors");
    let config = dir.join(crate::plugin::PLUGIN_CONFIG_FILENAME);

    let err = PluginManager::load_plugin_dir(&dir, &host()).unwrap_err();
    assert!(
        matches!(&err, PluginLoadError::ConfigNotFound(path) if *path == config),
        "{err}"
    );

    std::fs::write(&config, [0xff, 0xfe]).unwrap();
    let err = PluginManager::load_plugin_dir(&dir, &host()).unwrap_err();
    assert_eq!(
        err.to_string(),
        format!(
            "failed to parse {}: stream did not contain valid UTF-8",
            config.display()
        )
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn discover_and_reload_check_host_and_dependency_versions() {
    let plugins_dir = temp_plugin_dir("versions");
    let write = |id: &str, version: &str, extra: &str| {
        let dir = plugins_dir.join(id);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("init.luau"), "").unwrap();
        std::fs::write(
            dir.join(crate::plugin::PLUGIN_CONFIG_FILENAME),
            manifest_json(id, extra).replace("1.0.0", version),
        )
        .unwrap();
    };
    write("lib", "0.1.2", "");
    write("consumer", "1.0.0", r#","dependencies":{"lib":"0.1"}"#);
    write("future", "1.0.0", "");
    std::fs::write(
        plugins_dir
            .join("future")
            .join(crate::plugin::PLUGIN_CONFIG_FILENAME),
        manifest_json("future", "").replace(r#""host":"0.0.1""#, r#""host":"0.1""#),
    )
    .unwrap();

    let mut mgr = PluginManager::new(plugins_dir.clone());
    let errors = mgr.discover_plugins(&host()).unwrap();
    let messages: Vec<String> = errors.iter().map(ToString::to_string).collect();
    assert_eq!(
        messages,
        vec!["plugin 'future' requires host ^0.1, but this is host 0.0.1"]
    );
    assert_eq!(errors[0].directories, ["future"]);
    assert_eq!(order(&mgr), vec!["lib", "consumer"]);

    write("lib", "0.2.0", "");
    let err = mgr.reload_plugin("lib", &host()).unwrap_err();
    assert_eq!(
        err.to_string(),
        "plugin 'consumer' requires plugin 'lib' ^0.1, but 0.2.0 is installed"
    );
    assert_eq!(
        mgr.get_plugin("lib").unwrap().manifest.version,
        Version::new(0, 1, 2)
    );

    write("lib", "0.1.3", "");
    mgr.reload_plugin("lib", &host()).unwrap();
    assert_eq!(
        mgr.get_plugin("lib").unwrap().manifest.version,
        Version::new(0, 1, 3)
    );
    let _ = std::fs::remove_dir_all(&plugins_dir);
}
