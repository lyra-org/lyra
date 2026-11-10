// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashMap,
    path::Path,
    sync::Arc,
};

use anyhow::Context;
use tokio_util::sync::CancellationToken;

use super::{
    PublishOutcome,
    reentry_tests::admin_principal,
};
use crate::{
    plugins::{
        bootstrap::PluginRuntime,
        executor::PluginSourceRun,
        settings::{
            SettingsScope,
            settings_registry,
        },
        testing::{
            HOST_REQUIREMENT,
            init_default_test_state,
            start_runtime,
            use_plugins_dir,
        },
    },
    services::{
        STATE,
        plugin_scope::PluginId,
    },
};

/// Writes plugin `id` into `plugins_dir`, declaring the outbound HTTP scope for `hosts` and
/// depending on `dependencies`. With `entrypoint` it runs `init`; without, `init` is the module
/// its dependents require.
fn write_plugin(
    plugins_dir: &Path,
    id: &str,
    hosts: serde_json::Value,
    dependencies: &[&str],
    entrypoint: bool,
    init: &str,
) -> anyhow::Result<()> {
    let directory = plugins_dir.join(id);
    std::fs::create_dir_all(&directory)?;
    let mut manifest = serde_json::json!({
        "schema_version": 1,
        "id": id,
        "name": id,
        "version": "1.0.0",
        "lyra": HOST_REQUIREMENT,
        "description": "",
        "scopes": [
            { "id": "harmony.http", "reason": "Sends requests.", "hosts": hosts },
            "lyra.plugins",
        ],
        "dependencies": dependencies
            .iter()
            .map(|dependency| (dependency.to_string(), "*".into()))
            .collect::<serde_json::Map<String, serde_json::Value>>(),
    });
    if entrypoint {
        manifest["entrypoint"] = "init.luau".into();
    }
    std::fs::write(directory.join("plugin.json"), manifest.to_string())?;
    std::fs::write(directory.join("init.luau"), init)?;
    Ok(())
}

/// Fresh state whose runtime starts, as a server's does, from the plugins in `plugins_dir`.
async fn start(plugins_dir: &Path) -> anyhow::Result<PluginRuntime> {
    init_default_test_state()?;
    start_runtime().await?;
    use_plugins_dir(plugins_dir);
    let runtime = crate::plugins::bootstrap::initialize_harmony().await?;
    let outcome = crate::plugins::STATE
        .generation()
        .registries
        .start_plugins(runtime.clone(), CancellationToken::new())
        .await?;
    assert_eq!(outcome, PublishOutcome::Published);
    Ok(runtime)
}

async fn reload() -> anyhow::Result<()> {
    crate::plugins::STATE
        .generation()
        .registries
        .reload_all_plugins()
        .await
}

async fn restart(id: &str) -> anyhow::Result<()> {
    let plugin_id = PluginId::new(id)?;
    crate::plugins::STATE
        .generation()
        .registries
        .restart_plugin(&plugin_id)
        .await?;
    Ok(())
}

fn reaches(plugin_id: &str, url: &str) -> bool {
    harmony_http::test_plugin_reaches(plugin_id, url)
}

/// Answers every request with 200 on a local port, until dropped.
async fn serve() -> anyhow::Result<(u16, tokio::task::JoinHandle<()>)> {
    use tokio::io::{
        AsyncReadExt,
        AsyncWriteExt,
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let server = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buffer = vec![0; 4096];
                let _ = stream.read(&mut buffer).await;
                let _ = stream
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                    .await;
                let _ = stream.shutdown().await;
            });
        }
    });
    Ok((port, server))
}

/// A module whose `fetch` requests a URL and gives the response's error kind, or `ok`.
const FETCH_MODULE: &str = r#"
    local http = require("@harmony/http")
    return {
        fetch = function(url: string): string
            local response = http.request({ url = url, method = http.HttpMethod.Get })
            return response.error_kind or "ok"
        end,
    }
"#;

#[tokio::test(flavor = "multi_thread")]
async fn a_request_is_bound_by_the_hosts_of_the_plugin_whose_code_makes_it() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    let (port, server) = serve().await?;
    // `127.0.0.1` and `localhost` are two hosts.
    write_plugin(
        plugins_dir.path(),
        "helper",
        serde_json::json!(["localhost"]),
        &[],
        false,
        FETCH_MODULE,
    )?;
    write_plugin(
        plugins_dir.path(),
        "caller",
        serde_json::json!(["127.0.0.1"]),
        &["helper"],
        true,
        "",
    )?;
    let runtime = start(plugins_dir.path()).await?;

    let source = format!(
        r#"
            local http = require("@harmony/http")
            local helper = require("@helper")
            local own, others = "http://127.0.0.1:{port}/", "http://localhost:{port}/"
            local function fetch(url: string): string
                local response = http.request({{ url = url, method = http.HttpMethod.Get }})
                return response.error_kind or "ok"
            end
            local seen = table.concat({{ fetch(own), fetch(others), helper.fetch(own), helper.fetch(others) }}, " ")
            assert(seen == "ok host_not_allowed host_not_allowed ok", seen)
        "#
    );
    runtime
        .run_source(PluginSourceRun {
            plugin_id: "caller".to_string(),
            path: "chunk.luau".to_string(),
            source: Arc::from(source.as_bytes()),
            args: Box::new(|_, _| Ok(Vec::new())),
            auth: crate::plugins::testing::acting_as(admin_principal().await?),
        })
        .await
        .context("each request follows the list of the plugin whose code sent it")?;
    server.abort();
    Ok(())
}

/// Stores `value` as plugin `id`'s server-level setting `key`, as saving it does.
async fn store_setting(id: &str, key: &str, value: &str) -> anyhow::Result<()> {
    use crate::services::settings::plugins::{
        apply_updates,
        validate_updates,
    };

    let schema = settings_registry()
        .read()
        .await
        .get_schema(id, SettingsScope::Global)
        .cloned()
        .context("the plugin declared its settings")?;
    let changes = validate_updates(&schema, &HashMap::from([(key.to_string(), value.into())]))?;
    apply_updates(&mut *STATE.db.write().await, id, &schema, &changes)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_host_named_by_a_setting_is_the_stored_values_read_when_the_plugin_starts()
-> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    write_plugin(
        plugins_dir.path(),
        "named",
        serde_json::json!([{ "setting": "base_url" }, "fixed.example"]),
        &[],
        true,
        r#"
            require("@lyra/plugins").declare_settings(function(builder)
                builder:group("connection", "Connection")
                builder:string("base_url", { label = "Base URL", default = "https://declared.example" })
            end)
        "#,
    )?;
    start(plugins_dir.path()).await?;

    assert!(reaches("named", "https://fixed.example/"));
    assert!(
        !reaches("named", "https://declared.example/"),
        "a default the plugin declares names no host"
    );

    store_setting("named", "base_url", "http://Music.Example:8096/api").await?;
    assert!(
        !reaches("named", "http://music.example:8096/"),
        "a saved setting waits for the plugin's next start"
    );
    restart("named").await?;
    assert!(reaches("named", "http://music.example:8096/api/health"));
    assert!(reaches("named", "https://music.example/"), "any port");
    assert!(!reaches("named", "https://declared.example/"));
    assert!(reaches("named", "https://fixed.example/"));

    store_setting("named", "base_url", "music.example:8096").await?;
    reload().await?;
    assert!(
        reaches("named", "http://music.example:8096/"),
        "a bare host names itself"
    );

    for unusable in ["", "not a host"] {
        store_setting("named", "base_url", unusable).await?;
        restart("named").await?;
        assert!(
            !reaches("named", "http://music.example:8096/"),
            "{unusable:?}"
        );
        assert!(!reaches("named", "https://declared.example/"));
        assert!(reaches("named", "https://fixed.example/"));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn hosts_follow_the_manifest_each_run_loaded_and_go_with_the_plugin() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    let write = |hosts: &[&str], init: &str| {
        write_plugin(
            plugins_dir.path(),
            "listed",
            serde_json::json!(hosts),
            &[],
            true,
            init,
        )
    };
    write(&["first.example"], "")?;
    start(plugins_dir.path()).await?;
    assert!(reaches("listed", "https://first.example/"));
    assert!(!reaches("listed", "https://second.example/"));

    write(&["second.example"], "")?;
    restart("listed")
        .await
        .expect_err("a restart refuses a changed manifest");
    assert!(
        reaches("listed", "https://first.example/")
            && !reaches("listed", "https://second.example/"),
        "the hosts stay those of the manifest that is loaded"
    );

    reload().await?;
    assert!(!reaches("listed", "https://first.example/"));
    assert!(reaches("listed", "https://second.example/"));

    write(&["second.example"], r#"error("the entrypoint fails")"#)?;
    reload().await?;
    assert!(
        !reaches("listed", "https://second.example/"),
        "a plugin whose entrypoint failed is left no host"
    );

    write(&["second.example"], "")?;
    reload().await?;
    assert!(reaches("listed", "https://second.example/"));
    std::fs::remove_dir_all(plugins_dir.path().join("listed"))?;
    reload().await?;
    assert!(
        !reaches("listed", "https://second.example/"),
        "an uninstalled plugin is left no host"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_runtime_that_is_never_published_leaves_no_hosts_behind() -> anyhow::Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let plugins_dir = tempfile::tempdir()?;
    write_plugin(
        plugins_dir.path(),
        "unpublished",
        serde_json::json!(["listed.example"]),
        &[],
        true,
        "",
    )?;
    init_default_test_state()?;
    start_runtime().await?;
    use_plugins_dir(plugins_dir.path());
    let registries = crate::plugins::STATE.generation().registries.clone();
    let url = "https://listed.example/";

    let built = crate::plugins::bootstrap::initialize_harmony().await?;
    assert!(!reaches("unpublished", url), "building a runtime sets none");

    let shutdown = CancellationToken::new();
    shutdown.cancel();
    let outcome = registries.replace_runtime(&built, &shutdown).await?;
    assert_eq!(outcome, PublishOutcome::ShuttingDown);
    assert!(!reaches("unpublished", url), "an abandoned runtime's go");

    let published = crate::plugins::bootstrap::initialize_harmony().await?;
    registries
        .start_plugins(published, CancellationToken::new())
        .await?;
    assert!(reaches("unpublished", url));
    let late = crate::plugins::bootstrap::initialize_harmony().await?;
    let outcome = registries
        .start_plugins(late, CancellationToken::new())
        .await?;
    assert_eq!(outcome, PublishOutcome::Replaced);
    assert!(
        reaches("unpublished", url),
        "a runtime that lost the start keeps the published one's hosts"
    );
    Ok(())
}
