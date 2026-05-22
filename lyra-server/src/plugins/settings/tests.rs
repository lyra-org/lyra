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

fn server_info() -> crate::plugins::server::ServerInfo {
    crate::plugins::server::ServerInfo {
        id: "settings-test".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        commit_hash: env!("LYRA_GIT_HASH").to_string(),
        hostname: "localhost".to_string(),
        port: 0,
        published_url: None,
        setup_complete: false,
    }
}

#[test]
fn declare_settings_registers_global_and_user_schemas() -> Result<()> {
    futures::executor::block_on(super::REGISTRY.write()).clear();
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let user_db_id = crate::plugins::db::users::create(
        &mut db,
        &crate::plugins::db::users::test_user("settings-test")?,
    )?;
    let db = Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.plugins"])]),
        server_info(),
        db,
    )?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local plugins = require("@lyra/plugins")

                executor_config = plugins.declare_settings(function(builder)
                    builder:group("connection", "Connection")
                    builder:string("token", {{
                        label = "Token",
                        default = "global-token",
                    }})
                    builder:bool("enabled", {{
                        label = "Enabled",
                        default = true,
                    }})
                    builder:choice("mode", {{
                        label = "Mode",
                        default = "fast",
                        options = {{
                            {{ value = "fast", label = "Fast" }},
                            {{ value = "safe", label = "Safe" }},
                        }},
                    }})
                end)

                local user_settings = plugins.declare_user_settings(function(builder)
                    builder:group("authentication", "Authentication")
                    builder:string("token", {{
                        label = "API Token",
                        default = "user-token",
                    }})
                end)
                executor_user_config = user_settings:get({user_db_id})
            "#,
            user_db_id = user_db_id.0,
        )
        .into_bytes(),
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        &br#"
            return executor_config.token,
                executor_config.enabled,
                executor_config.mode,
                executor_user_config.token
        "#[..],
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::String(b"global-token".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::String(b"fast".to_vec()),
            luau::Value::String(b"user-token".to_vec()),
        ]
    );

    let registry = futures::executor::block_on(super::REGISTRY.read());
    assert!(
        registry
            .get_schema("demo", super::SettingsScope::Global)
            .is_some()
    );
    assert!(
        registry
            .get_schema("demo", super::SettingsScope::User)
            .is_some()
    );
    Ok(())
}
