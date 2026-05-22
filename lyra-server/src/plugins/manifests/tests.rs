use std::sync::Arc;

use anyhow::Result;
use harmony_luau as luau;

use crate::plugins::executor::PluginExecutor;

fn manifest(id: &str, scopes: &[&str]) -> harmony_core::PluginManifest {
    harmony_core::PluginManifest {
        schema_version: 1,
        id: id.to_string(),
        name: format!("{id} Plugin"),
        version: "1.0.0".to_string(),
        description: "Test manifest".to_string(),
        entrypoint: "init.luau".to_string(),
        scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
        dependencies: Vec::new(),
    }
}

#[test]
fn plugins_manifest_access_reads_vm_context_store() -> Result<()> {
    let runtime = PluginExecutor::with_manifests(Arc::from(vec![
        manifest("demo", &["lyra.plugins"]),
        harmony_core::PluginManifest {
            schema_version: 1,
            id: "other".to_string(),
            name: "Other Plugin".to_string(),
            version: "2.0.0".to_string(),
            description: "Other manifest".to_string(),
            entrypoint: "main.luau".to_string(),
            scopes: Vec::new(),
            dependencies: Vec::new(),
        },
    ]))?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local plugins = require("@lyra/plugins")
            return plugins.manifest().id, plugins.get("other").name, #plugins.list(), plugins.get("missing") == nil
        "#[..],
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::String(b"demo".to_vec()),
            luau::Value::String(b"Other Plugin".to_vec()),
            luau::Value::Number(2.0),
            luau::Value::Boolean(true),
        ]
    );
    Ok(())
}
