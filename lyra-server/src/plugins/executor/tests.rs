use super::*;
use anyhow::{
    Context,
    Result,
};
use harmony_core::{
    CallContext,
    MemorySourceLoader,
    ModuleSpec,
    luau::RequireRuntime,
    plugin::{
        PluginLoadError,
        PluginManifest,
    },
};

fn default_server_info() -> crate::plugins::server::ServerInfo {
    crate::plugins::server::ServerInfo {
        id: "raw-runtime".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        commit_hash: env!("LYRA_GIT_HASH").to_string(),
        hostname: "localhost".to_string(),
        port: 0,
        published_url: None,
        setup_complete: false,
    }
}

impl PluginExecutor {
    pub(crate) fn with_manifests(manifests: Arc<[PluginManifest]>) -> Result<Self> {
        Self::with_runtime_state(manifests, default_server_info())
    }

    pub(crate) fn with_runtime_state(
        manifests: Arc<[PluginManifest]>,
        server_info: crate::plugins::server::ServerInfo,
    ) -> Result<Self> {
        Self::with_loader(
            manifests,
            server_info,
            stores::PluginModuleStores::empty(),
            MemorySourceLoader::new(),
        )
    }

    pub(crate) fn with_database(
        manifests: Arc<[PluginManifest]>,
        server_info: crate::plugins::server::ServerInfo,
        db: crate::plugins::db::DbAsync,
    ) -> Result<Self> {
        Self::with_loader(
            manifests,
            server_info,
            stores::PluginModuleStores::with_db(db),
            MemorySourceLoader::new(),
        )
    }

    pub(crate) fn discover_from_plugins_dir(
        plugins_dir: impl Into<std::path::PathBuf>,
        server_info: crate::plugins::server::ServerInfo,
    ) -> Result<(Self, Vec<PluginLoadError>)> {
        Self::discover_from_plugins_dir_with_stores(
            plugins_dir,
            server_info,
            stores::PluginModuleStores::empty(),
            Vec::new(),
        )
    }

    pub(crate) fn discover_from_plugins_dir_with_db(
        plugins_dir: impl Into<std::path::PathBuf>,
        server_info: crate::plugins::server::ServerInfo,
        db: crate::plugins::db::DbAsync,
    ) -> Result<(Self, Vec<PluginLoadError>)> {
        Self::discover_from_plugins_dir_with_db_and_modules(
            plugins_dir,
            server_info,
            db,
            Vec::new(),
        )
    }

    pub(crate) fn eval_plugin_source(
        &self,
        plugin_id: impl Into<Arc<str>>,
        path: impl Into<Arc<str>>,
        source: impl Into<Arc<[u8]>>,
    ) -> Result<Vec<luau::Value>> {
        let origin = plugin_origin(plugin_id, path);
        self.eval_plugin_source_with_call_context(
            source,
            CallContext {
                origin,
                ..CallContext::default()
            },
        )
    }
}

fn runtime_with_scopes(scopes: &[&str]) -> Result<PluginExecutor> {
    PluginExecutor::with_manifests(Arc::from(vec![manifest("demo", scopes)]))
}

fn manifest(id: &str, scopes: &[&str]) -> PluginManifest {
    PluginManifest {
        schema_version: 1,
        id: id.to_string(),
        name: format!("{id} Plugin"),
        version: "1.0.0".to_string(),
        description: "Test manifest".to_string(),
        entrypoint: Some("init.luau".to_string()),
        scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
        dependencies: Vec::new(),
    }
}

#[test]
fn plugin_executor_preserves_typed_call_context_across_luau_yield() -> Result<()> {
    let runtime = runtime_with_scopes(&["harmony.task", "test.context"])?;
    let require = runtime.vm.data().get::<RequireRuntime>()?;
    require.register(
        ModuleSpec::new("test/context")
            .capability("test.context")
            .function(
                harmony_core::FunctionSpec::sync_fn("username")
                    .returns::<String>()
                    .call(|mut frame| {
                        let principal = frame
                            .context
                            .caller
                            .get::<crate::services::auth::Principal>()?;
                        frame.returns.write(principal.username.as_str())
                    }),
            )
            .install(|_| Ok(harmony_core::ModuleExport::new(()))),
    )?;

    let mut context = CallContext {
        origin: plugin_origin("demo", "init.luau"),
        ..CallContext::default()
    };
    context.caller.insert(crate::services::auth::Principal {
        user_db_id: agdb::DbId(7),
        user_public_id: "user-public-id".to_string(),
        username: "raw-user".to_string(),
        permissions: vec![crate::plugins::db::Permission::Admin],
        role_name: Some("admin".to_string()),
        accessible_library_ids: std::collections::HashSet::new(),
    });
    runtime.run_plugin_source_with_call_context(
        br#"
            local task = require("@harmony/task")
            local context = require("@test/context")
            task.wait()
            executor_context_username = context.username()
        "# as &[u8],
        context,
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        &b"return executor_context_username"[..],
    )?;
    assert_eq!(values, vec![luau::Value::String(b"raw-user".to_vec())]);
    Ok(())
}

#[test]
fn plugin_executor_declares_metadata_provider_ids_and_options() -> Result<()> {
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
    crate::testing::init_default_test_state()?;
    futures::executor::block_on(crate::services::providers::reset_provider_registry_for_test());
    let runtime = runtime_with_scopes(&["lyra.metadata"])?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local metadata = require("@lyra/metadata")
            local provider = metadata.Provider.new("raw-provider")
            provider:id({
                id = "release_id",
                entity = metadata.EntityType.Release,
                unique = true,
            }, "https://example.test/release/{id}")
            provider:declare_option({
                name = "force",
                label = "Force refresh",
                type = "boolean",
                default = true,
                requires_settings = { "token" },
            })

            local ids = metadata.ids.for_provider({
                ["raw-provider"] = {
                    release_id = "abc123",
                },
            }, "raw-provider")

            return metadata.EntityType.Release, ids.release_id
        "#[..],
    )?;

    assert_eq!(values.len(), 2);
    let entity = crate::services::EntityType::_harmony_userdata_class().read_value(
        &runtime.vm,
        "entity",
        values[0].clone(),
    )?;
    assert_eq!(entity, crate::services::EntityType::Release);
    assert_eq!(values[1], luau::Value::String(b"abc123".to_vec()));

    let registry =
        futures::executor::block_on(crate::services::providers::PROVIDER_REGISTRY.read());
    let (id_spec, has_generator) = crate::services::providers::registry_tests::id_registration(
        &registry,
        "raw-provider",
        "release_id",
    )
    .context("provider id registration")?;
    assert_eq!(id_spec.id, "release_id");
    assert_eq!(id_spec.entity, crate::services::EntityType::Release);
    assert!(id_spec.unique);
    assert!(has_generator);
    assert_eq!(
        crate::services::providers::registry_tests::id_url_template(
            &registry,
            "raw-provider",
            "release_id",
        )
        .as_deref(),
        Some("https://example.test/release/{id}")
    );
    let option = registry
        .get_options("raw-provider")
        .iter()
        .find(|option| option.name == "force")
        .context("provider option")?;
    assert_eq!(option.label, "Force refresh");
    Ok(())
}

#[test]
fn plugin_executor_reads_server_info_from_vm_context() -> Result<()> {
    let runtime = PluginExecutor::with_runtime_state(
        Arc::from(vec![manifest("demo", &["lyra.server"])]),
        crate::plugins::server::ServerInfo {
            id: "server-1".to_string(),
            version: "9.8.7".to_string(),
            commit_hash: "abc123".to_string(),
            hostname: "test-host".to_string(),
            port: 3210,
            published_url: Some("https://lyra.example".to_string()),
            setup_complete: true,
        },
    )?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local server = require("@lyra/server")
            local info = server.info()
            executor_server_id = info.id
            executor_server_port = info.port
            executor_server_url = info.published_url
            executor_server_setup = info.setup_complete
        "#[..],
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        &b"return executor_server_id, executor_server_port, executor_server_url, executor_server_setup"[..],
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::String(b"server-1".to_vec()),
            luau::Value::Integer(3210),
            luau::Value::String(b"https://lyra.example".to_vec()),
            luau::Value::Boolean(true),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_lyra_playback_sessions_on_update() -> Result<()> {
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
    crate::testing::init_default_test_state()?;
    let runtime = runtime_with_scopes(&["lyra.playback_sessions", "harmony.task"])?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local task = require("@harmony/task")
            local playbacks = require("@lyra/playback_sessions")

            playbacks.on_update(function(update)
                task.wait()
                executor_playback_update = update.event .. ":" .. update.track_public_id
            end)
        "#[..],
    )?;

    runtime.dispatch_playback_update(
        crate::services::playback_sessions::PlaybackUpdatePayload {
            event: "started".to_string(),
            state: crate::plugins::db::PlaybackState::Playing,
            playback_session_public_id: "playback-public".to_string(),
            track_public_id: "track-public".to_string(),
            user_public_id: "user-public".to_string(),
            library_public_id: None,
            position_ms: 42,
            duration_ms: Some(100),
            activity_ms: 42,
            qualifies_single_listen: false,
            updated_at_ms: 10,
        },
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        &b"return executor_playback_update"[..],
    )?;
    assert_eq!(
        values,
        vec![luau::Value::String(b"started:track-public".to_vec())]
    );
    Ok(())
}

#[test]
fn plugin_executor_dispatches_registered_api_handler() -> Result<()> {
    let _ = futures::executor::block_on(crate::plugins::api::install(
        axum::Router::new(),
        std::collections::HashSet::new(),
    ))?;
    let runtime = runtime_with_scopes(&["lyra.api", "harmony.task"])?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local api = require("@lyra/api")
            local task = require("@harmony/task")

            api.post("/demo/{id}", function(ctx)
                task.wait()
                return api.response.json(201, {
                    id = ctx.params.id,
                    flag = ctx.request.query.flag[1],
                    body = ctx.request.json.name,
                }, {
                    ["x-demo"] = "ok",
                })
            end, "public")
        "#[..],
    )?;

    let handler_id = futures::executor::block_on(crate::plugins::api::tests::registered_handler(
        "POST",
        "/demo/{id}",
    ))
    .context("registered API handler")?;
    let result = runtime.dispatch_api_handler(ApiHandlerRequest {
        handler_id,
        plugin_id: "demo".to_string(),
        method: "POST".to_string(),
        path: "/demo/abc".to_string(),
        headers: vec![("content-type".to_string(), "application/json".to_string())],
        query: HashMap::from([("flag".to_string(), vec!["yes".to_string()])]),
        params: HashMap::from([("id".to_string(), "abc".to_string())]),
        body: br#"{"name":"raw"}"#.to_vec(),
        auth: None,
    })?;

    assert_eq!(result.kind, ApiResponseKind::Json);
    assert_eq!(result.status, 201);
    assert!(
        result
            .headers
            .contains(&("content-type".to_string(), "application/json".to_string()))
    );
    assert!(
        result
            .headers
            .contains(&("x-demo".to_string(), "ok".to_string()))
    );
    let Some(ApiResponseBody::Json(body)) = result.body else {
        anyhow::bail!("expected JSON response body");
    };
    assert_eq!(body["id"], "abc");
    assert_eq!(body["flag"], "yes");
    assert_eq!(body["body"], "raw");
    Ok(())
}

#[test]
fn plugin_executor_dispatches_registered_websocket_handler() -> Result<()> {
    let _ = futures::executor::block_on(crate::plugins::api::install(
        axum::Router::new(),
        std::collections::HashSet::new(),
    ))?;
    let runtime = runtime_with_scopes(&["lyra.api"])?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local api = require("@lyra/api")

            api.websocket("/socket/{id}", function(reader, sender, ctx)
                local frame = reader:recv()
                sender:send(ctx.params.id .. ":" .. frame)
                sender:close()
            end, "public")
        "#[..],
    )?;

    let handler_id = futures::executor::block_on(crate::plugins::api::tests::registered_handler(
        "GET",
        "/socket/{id}",
    ))
    .context("registered websocket handler")?;
    let (inbound_tx, inbound_rx) = tokio::sync::mpsc::channel(4);
    let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::channel(4);
    inbound_tx.try_send("hello".to_string())?;
    let state = WebSocketState::new();

    runtime.start_websocket(WebSocketStartRequest {
        handler_id,
        plugin_id: "demo".to_string(),
        method: "GET".to_string(),
        path: "/socket/abc".to_string(),
        headers: Vec::new(),
        query: HashMap::new(),
        params: HashMap::from([("id".to_string(), "abc".to_string())]),
        auth: None,
        inbound: Arc::new(tokio::sync::Mutex::new(inbound_rx)),
        outbound: outbound_tx,
        state,
    })?;

    let mut outbound = None;
    for _ in 0..100 {
        runtime.poll_background_tasks();
        match outbound_rx.try_recv() {
            Ok(text) => {
                outbound = Some(text);
                break;
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(error) => return Err(error).context("raw websocket outbound channel closed"),
        }
    }
    assert_eq!(outbound.as_deref(), Some("abc:hello"));

    for _ in 0..100 {
        runtime.poll_background_tasks();
        if runtime.websocket_tasks.borrow().is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(runtime.websocket_tasks.borrow().is_empty());
    Ok(())
}

#[test]
fn foreground_dispatch_does_not_hide_finished_websocket_cleanup() -> Result<()> {
    let _ = futures::executor::block_on(crate::plugins::api::install(
        axum::Router::new(),
        std::collections::HashSet::new(),
    ))?;
    let runtime = runtime_with_scopes(&["lyra.api"])?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local api = require("@lyra/api")

            api.websocket("/socket", function(reader, sender)
                local frame = reader:recv()
                sender:send(frame)
                sender:close()
            end, "public")

            api.get("/ping", function()
                return api.response.empty(204)
            end, "public")
        "#[..],
    )?;

    let websocket_handler_id = futures::executor::block_on(
        crate::plugins::api::tests::registered_handler("GET", "/socket"),
    )
    .context("registered websocket handler")?;
    let api_handler_id = futures::executor::block_on(
        crate::plugins::api::tests::registered_handler("GET", "/ping"),
    )
    .context("registered API handler")?;
    let (inbound_tx, inbound_rx) = tokio::sync::mpsc::channel(4);
    let (outbound_tx, _outbound_rx) = tokio::sync::mpsc::channel(4);
    inbound_tx.try_send("done".to_string())?;
    let state = WebSocketState::new();

    runtime.start_websocket(WebSocketStartRequest {
        handler_id: websocket_handler_id,
        plugin_id: "demo".to_string(),
        method: "GET".to_string(),
        path: "/socket".to_string(),
        headers: Vec::new(),
        query: HashMap::new(),
        params: HashMap::new(),
        auth: None,
        inbound: Arc::new(tokio::sync::Mutex::new(inbound_rx)),
        outbound: outbound_tx,
        state,
    })?;
    assert_eq!(runtime.websocket_tasks.borrow().len(), 1);

    let response = runtime.dispatch_api_handler(ApiHandlerRequest {
        handler_id: api_handler_id,
        plugin_id: "demo".to_string(),
        method: "GET".to_string(),
        path: "/ping".to_string(),
        headers: Vec::new(),
        query: HashMap::new(),
        params: HashMap::new(),
        body: Vec::new(),
        auth: None,
    })?;
    assert_eq!(response.kind, ApiResponseKind::Empty);

    for _ in 0..100 {
        runtime.poll_background_tasks();
        if runtime.websocket_tasks.borrow().is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(runtime.websocket_tasks.borrow().is_empty());
    Ok(())
}

#[test]
fn plugin_executor_dispatches_registered_mix_handler() -> Result<()> {
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
    crate::testing::init_default_test_state()?;
    futures::executor::block_on(crate::services::mix::reset_mix_registry_for_test());
    let runtime = runtime_with_scopes(&["lyra.mix", "harmony.task"])?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local mix = require("@lyra/mix")
            local task = require("@harmony/task")

            local mixer = mix.Mixer.new("demo-mixer")
            mixer:declare_option({
                name = "boost",
                label = "Boost",
                type = "boolean",
            })
            mixer:from_track(function(ctx)
                task.wait()
                return {
                    tracks = {
                        { track_id = ctx.options.boost and 42 or 41 },
                    },
                }
            end)
        "#[..],
    )?;

    let handler_id = futures::executor::block_on(async {
        crate::services::mix::MIX_REGISTRY
            .read()
            .await
            .get_seed_callback("demo-mixer", crate::services::mix::MixSeedType::Track)
    })
    .context("registered mix callback")?;
    let result = runtime.dispatch_mix_handler(MixHandlerRequest {
        handler_id,
        seed_id: 40,
        limit: Some(10),
        user_id: None,
        recent_track_ids: Vec::new(),
        options: serde_json::Map::from_iter([("boost".to_string(), serde_json::Value::Bool(true))]),
    })?;

    assert_eq!(result.track_ids, vec![42]);
    Ok(())
}

#[path = "tests/db_modules.rs"]
mod db_modules;

#[tokio::test]
async fn plugin_executor_drives_async_lyra_images_compose() -> Result<()> {
    let test_dir = std::env::temp_dir().join(format!(
        "lyra-raw-images-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    std::fs::create_dir_all(&test_dir)?;
    let source_path = test_dir.join("source.png");
    image::RgbImage::from_pixel(2, 2, image::Rgb([255, 0, 0])).save(&source_path)?;

    let runtime = runtime_with_scopes(&["lyra.images"])?;
    let source = source_path.to_string_lossy();
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local images = require("@lyra/images")
                local result = images.compose({{
                    sources = {{ "{source}" }},
                    width = 2,
                    height = 2,
                    quality = 80,
                }})

                executor_image_path = result.path
                executor_image_hash_len = #result.hash
                executor_image_mime = result.mime_type
            "#
        )
        .into_bytes(),
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        &b"return type(executor_image_path), executor_image_hash_len, executor_image_mime"[..],
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::String(b"string".to_vec()),
            luau::Value::Number(64.0),
            luau::Value::String(b"image/jpeg".to_vec()),
        ]
    );

    let _ = std::fs::remove_file(source_path);
    let _ = std::fs::remove_dir(test_dir);
    Ok(())
}

#[test]
fn plugin_executor_denies_undeclared_capability_before_cached_module_return() -> Result<()> {
    let runtime = PluginExecutor::with_manifests(Arc::from(vec![
        manifest("demo", &["harmony.serde"]),
        manifest("denied", &[]),
    ]))?;

    let allowed = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local serde = require("@harmony/serde")
            local json = serde.json
            return json.encode({ answer = 42 })
        "#[..],
    )?;
    assert_eq!(
        allowed,
        vec![luau::Value::String(br#"{"answer":42}"#.to_vec())]
    );

    let denied = runtime
        .eval_plugin_source(
            "denied",
            "init.luau",
            &br#"
                local serde = require("@harmony/serde")
                local json = serde.json
                return json.encode({ answer = 42 })
            "#[..],
        )
        .expect_err("undeclared capability should be denied");

    assert!(
        denied
            .to_string()
            .contains("without capability 'harmony.serde'"),
        "{denied}"
    );
    Ok(())
}

#[test]
fn plugin_executor_discovers_and_executes_plugins_from_directory() -> Result<()> {
    let test_dir = std::env::temp_dir().join(format!(
        "lyra-raw-discover-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    let plugin_dir = test_dir.join("plugins").join("demo");
    std::fs::create_dir_all(&plugin_dir)?;
    std::fs::write(
        plugin_dir.join("plugin.json"),
        r#"{
            "schema_version": 1,
            "id": "demo",
            "name": "Demo",
            "version": "1.0.0",
            "description": "Demo plugin",
            "entrypoint": "init.luau",
            "scopes": ["harmony.serde"]
        }"#,
    )?;
    std::fs::write(
        plugin_dir.join("init.luau"),
        br#"
            local serde = require("@harmony/serde")
            local json = serde.json
            executor_discovered_output = json.encode({ answer = 42 })
        "#,
    )?;

    let (runtime, errors) =
        PluginExecutor::discover_from_plugins_dir(test_dir.join("plugins"), default_server_info())?;
    assert!(errors.is_empty(), "{errors:?}");
    assert!(runtime.has_plugin("demo"));
    assert_eq!(runtime.plugin_manifests()[0].id, "demo");

    runtime.exec_all()?;
    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        &b"return executor_discovered_output"[..],
    )?;
    assert_eq!(
        values,
        vec![luau::Value::String(br#"{"answer":42}"#.to_vec())]
    );

    let _ = std::fs::remove_dir_all(test_dir);
    Ok(())
}

#[test]
fn plugin_executor_executes_checked_in_plugins_from_repo() -> Result<()> {
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
    crate::testing::init_default_test_state()?;
    futures::executor::block_on(crate::plugins::settings::REGISTRY.write()).clear();
    futures::executor::block_on(crate::services::providers::reset_provider_registry_for_test());
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(
        crate::plugins::db::test_db::new_test_db()?,
    ));
    let plugins_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .context("lyra-server manifest directory has parent")?
        .join("plugins");
    let (runtime, errors) =
        PluginExecutor::discover_from_plugins_dir_with_db(plugins_dir, default_server_info(), db)?;
    assert!(
        errors.is_empty(),
        "unexpected plugin discovery errors: {errors:?}"
    );

    for plugin in runtime.plugins.iter() {
        runtime
            .exec_plugin(&plugin.manifest.id)
            .with_context(|| format!("execute checked-in plugin '{}'", plugin.manifest.id))?;
    }
    Ok(())
}

#[test]
fn plugin_executor_handle_discovers_and_executes_on_runtime_thread() -> Result<()> {
    let test_dir = std::env::temp_dir().join(format!(
        "lyra-raw-handle-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    let plugin_dir = test_dir.join("plugins").join("demo");
    std::fs::create_dir_all(&plugin_dir)?;
    std::fs::write(
        plugin_dir.join("plugin.json"),
        r#"{
            "schema_version": 1,
            "id": "demo",
            "name": "Demo",
            "version": "1.0.0",
            "description": "Demo plugin",
            "entrypoint": "init.luau",
            "scopes": ["harmony.serde"]
        }"#,
    )?;
    std::fs::write(
        plugin_dir.join("init.luau"),
        br#"
            local serde = require("@harmony/serde")
            local json = serde.json
            executor_handle_output = json.encode({ answer = 42 })
        "#,
    )?;

    let db = crate::plugins::db::test_db::new_test_db()?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));
    let (runtime, errors) = PluginExecutorHandle::discover_from_plugins_dir_with_db_and_modules(
        test_dir.join("plugins"),
        default_server_info(),
        db,
        Vec::new(),
    )?;
    assert!(errors.is_empty(), "{errors:?}");
    assert!(futures::executor::block_on(runtime.has_plugin("demo"))?);
    assert_eq!(
        futures::executor::block_on(runtime.plugin_manifests())?[0].id,
        "demo"
    );

    futures::executor::block_on(runtime.exec_all())?;
    futures::executor::block_on(runtime.exec_plugin("demo"))?;

    let _ = std::fs::remove_dir_all(test_dir);
    Ok(())
}
