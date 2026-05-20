use super::*;
use anyhow::Context;

fn runtime_with_scopes(scopes: &[&str]) -> Result<PluginExecutor> {
    PluginExecutor::with_manifests(Arc::from(vec![manifest("demo", scopes)]))
}

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
fn plugin_executor_requires_generic_harmony_modules() -> Result<()> {
    let runtime = runtime_with_scopes(&["harmony.crypt", "harmony.json", "harmony.task"])?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local crypt = require("@harmony/crypt")
            local json = require("@harmony/json")
            local task = require("@harmony/task")

            return json.encode({ answer = 42 }), #crypt.random(4), type(task.spawn)
        "#[..],
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::String(br#"{"answer":42}"#.to_vec()),
            luau::Value::Number(8.0),
            luau::Value::String(b"function".to_vec()),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_requires_lyra_module_with_plugin_origin() -> Result<()> {
    let runtime = runtime_with_scopes(&["lyra.plugins"])?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local plugins = require("@lyra/plugins")
            return plugins.id()
        "#[..],
    )?;

    assert_eq!(values, vec![luau::Value::String(b"demo".to_vec())]);
    Ok(())
}

#[test]
fn plugin_executor_preserves_typed_call_context_across_luau_yield() -> Result<()> {
    let runtime = runtime_with_scopes(&["harmony.task", "test.context"])?;
    let require = runtime.vm.data().get::<LuauRequireRuntime>()?;
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
fn plugin_executor_reads_plugin_manifests_from_vm_context() -> Result<()> {
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

#[test]
fn plugin_executor_declares_plugin_settings() -> Result<()> {
    futures::executor::block_on(crate::plugins::runtime::REGISTRY.write()).clear();
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let user_db_id = crate::plugins::db::users::create(
        &mut db,
        &crate::plugins::db::users::test_user("raw-settings")?,
    )?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.plugins"])]),
        default_server_info(),
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

    let registry = futures::executor::block_on(crate::plugins::runtime::REGISTRY.read());
    assert!(
        registry
            .get_schema("demo", crate::plugins::runtime::SettingsScope::Global)
            .is_some()
    );
    assert!(
        registry
            .get_schema("demo", crate::plugins::runtime::SettingsScope::User)
            .is_some()
    );
    Ok(())
}

#[test]
fn plugin_executor_declares_metadata_provider_ids_and_options() -> Result<()> {
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
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

    assert_eq!(
        values,
        vec![
            luau::Value::String(b"release".to_vec()),
            luau::Value::String(b"abc123".to_vec()),
        ]
    );

    let registry =
        futures::executor::block_on(crate::services::providers::PROVIDER_REGISTRY.read());
    let (id_spec, has_generator) = registry
        .id_registration("raw-provider", "release_id")
        .context("provider id registration")?;
    assert_eq!(id_spec.id, "release_id");
    assert_eq!(id_spec.entity, crate::services::EntityType::Release);
    assert!(id_spec.unique);
    assert!(has_generator);
    assert_eq!(
        registry
            .id_url_template("raw-provider", "release_id")
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
    let runtime = runtime_with_scopes(&["lyra.playback_sessions", "harmony.task"])?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local task = require("@harmony/task")
            local playbacks = require("@lyra/playback_sessions")

            executor_playback_exports =
                type(playbacks.report) .. ":" ..
                type(playbacks.start) .. ":" ..
                type(playbacks.report_session) .. ":" ..
                type(playbacks.clear_session) .. ":" ..
                type(playbacks.list_connections) .. ":" ..
                type(playbacks.send_command)

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
        &b"return executor_playback_update, executor_playback_exports"[..],
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::String(b"started:track-public".to_vec()),
            luau::Value::String(b"function:function:function:function:function:function".to_vec()),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_lyra_api_module_shape() -> Result<()> {
    let _ = futures::executor::block_on(crate::plugins::api::install(
        axum::Router::new(),
        std::collections::HashSet::new(),
    ))?;
    let runtime = runtime_with_scopes(&["lyra.api"])?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local api = require("@lyra/api")
            api.get("/demo/{id}", function(ctx)
                return api.response.json(200, { id = ctx.params.id })
            end, "public", true)
            api.route("post", "/demo", function()
                return api.response.empty(204)
            end)
            api.websocket("/socket", function()
            end, "public")

            local response = api.response.json(201, { ok = true }, {
                ["x-test"] = "1",
            })
            local query = {
                flag = { "yes" },
                count = { "12" },
                csv = { "a,b", "c" },
            }
            return
                type(api.get),
                type(api.response.stream_track),
                response.kind,
                response.status,
                response.headers["content-type"],
                response.headers["x-test"],
                api.query.bool(query, "flag", false),
                api.query.int(query, "count", nil, 1, 20),
                table.concat(api.query.csv(query, "csv"), "|")
        "#[..],
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::String(b"function".to_vec()),
            luau::Value::String(b"function".to_vec()),
            luau::Value::String(b"json".to_vec()),
            luau::Value::Integer(201),
            luau::Value::String(b"application/json".to_vec()),
            luau::Value::String(b"1".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::Integer(12),
            luau::Value::String(b"a|b|c".to_vec()),
        ]
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

    let handler_id = futures::executor::block_on(crate::plugins::api::registered_handler_for_test(
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

    assert_eq!(result.kind, "json");
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

    let handler_id = futures::executor::block_on(crate::plugins::api::registered_handler_for_test(
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
fn plugin_executor_exposes_lyra_mix_module_shape() -> Result<()> {
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
    futures::executor::block_on(crate::services::mix::reset_mix_registry_for_test());
    let runtime = runtime_with_scopes(&["lyra.mix"])?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local mix = require("@lyra/mix")
            local mixer = mix.Mixer.new("demo-mixer")
            mixer:from_track(function(ctx)
                return { tracks = {} }
            end)
            mixer:from_release(function(ctx)
                return { tracks = {} }
            end)
            mixer:from_artist(function(ctx)
                return { tracks = {} }
            end)
            mixer:from_recent_listens(function(ctx)
                return { tracks = {} }
            end)
            mixer:from_genre(function(ctx)
                return { tracks = {} }
            end)
            mixer:from_playlist(function(ctx)
                return { tracks = {} }
            end)
            mixer:declare_option({
                name = "source",
                label = "Source",
                type = "string",
            })

            return
                type(mix.from_track),
                type(mix.from_release),
                type(mix.from_artist),
                type(mix.from_genre),
                type(mix.from_playlist),
                type(mix.instant_mix_from_audio),
                type(mixer.from_track),
                type(mixer.from_recent_listens),
                type(mixer.declare_option)
        "#[..],
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::String(b"function".to_vec()),
            luau::Value::String(b"function".to_vec()),
            luau::Value::String(b"function".to_vec()),
            luau::Value::String(b"function".to_vec()),
            luau::Value::String(b"function".to_vec()),
            luau::Value::String(b"function".to_vec()),
            luau::Value::String(b"function".to_vec()),
            luau::Value::String(b"function".to_vec()),
            luau::Value::String(b"function".to_vec()),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_dispatches_registered_mix_handler() -> Result<()> {
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
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

#[test]
fn plugin_executor_reads_auth_capabilities_from_vm_context() -> Result<()> {
    let runtime = runtime_with_scopes(&["lyra.auth"])?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local auth = require("@lyra/auth")
            local capabilities = auth.capabilities()
            return capabilities.enabled, capabilities.allow_default_login_when_disabled, capabilities.default_username, auth.login == nil
        "#[..],
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::Boolean(false),
            luau::Value::Boolean(true),
            luau::Value::String(b"default".to_vec()),
            luau::Value::Boolean(true),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_lyra_ids_module() -> Result<()> {
    let runtime = runtime_with_scopes(&["lyra.ids"])?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local ids = require("@lyra/ids")
            executor_id_missing = ids.get_id(999999)
            executor_db_id_missing = ids.get_db_id("missing-public-id")
            executor_ids_missing = ids.get_ids({ 999999 })
            executor_db_ids_missing = ids.get_db_ids({ "missing-public-id" })
        "#[..],
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        &br#"
            return executor_id_missing == nil,
                executor_db_id_missing == nil,
                executor_ids_missing[999999] == nil,
                executor_db_ids_missing["missing-public-id"] == nil
        "#[..],
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::Boolean(true),
            luau::Value::Boolean(true),
            luau::Value::Boolean(true),
            luau::Value::Boolean(true),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_ids_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw ID Track")?;
    let track_public_id = crate::plugins::db::lookup::find_id_by_db_id(&db, track_db_id)?
        .context("inserted track has public id")?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.ids"])]),
        default_server_info(),
        db,
    )?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local ids = require("@lyra/ids")
                local track_db_id = {track_db_id}
                local track_public_id = "{track_public_id}"

                executor_public_id = ids.get_id(track_db_id)
                executor_public_ids = ids.get_ids({{ track_db_id, -1, track_db_id, 999999 }})
                executor_db_id = ids.get_db_id(track_public_id)
                executor_db_ids = ids.get_db_ids({{ track_public_id, " ", track_public_id, "missing-public-id" }})
            "#,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        format!(
            r#"
                local track_db_id = {track_db_id}
                local track_public_id = "{track_public_id}"

                return executor_public_id,
                    executor_public_ids[track_db_id],
                    executor_public_ids[999999] == nil,
                    executor_db_id,
                    executor_db_ids[track_public_id],
                    executor_db_ids["missing-public-id"] == nil
            "#,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::String(track_public_id.as_bytes().to_vec()),
            luau::Value::String(track_public_id.as_bytes().to_vec()),
            luau::Value::Boolean(true),
            luau::Value::Integer(track_db_id.0),
            luau::Value::Integer(track_db_id.0),
            luau::Value::Boolean(true),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_entities_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw Entity Track")?;
    let track_public_id = crate::plugins::db::lookup::find_id_by_db_id(&db, track_db_id)?
        .context("inserted track has public id")?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.entities"])]),
        default_server_info(),
        db,
    )?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local entities = require("@lyra/entities")
                local track_db_id = {track_db_id}
                local track_public_id = "{track_public_id}"

                local by_db = entities.query_track({{ id = track_db_id }})
                local by_public = entities.query_track({{ id = track_public_id }})
                local many = entities.query_many({{ ids = {{ track_public_id, track_db_id }} }})

                return by_db.entity.track_title,
                    by_public.entity.id,
                    many[track_public_id].entity.track_title,
                    many[tostring(track_db_id)].entity.track_title,
                    entities.get_type(track_public_id),
                    entities.CreditType.Artist
            "#,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::String(b"Raw Entity Track".to_vec()),
            luau::Value::String(track_public_id.as_bytes().to_vec()),
            luau::Value::String(b"Raw Entity Track".to_vec()),
            luau::Value::String(b"Raw Entity Track".to_vec()),
            luau::Value::String(b"Track".to_vec()),
            luau::Value::String(b"artist".to_vec()),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_entries_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw Entry Track")?;
    let entry = crate::plugins::db::entries::Entry {
        db_id: None,
        id: nanoid::nanoid!(),
        full_path: std::path::PathBuf::from("/music/raw-entry.flac"),
        kind: crate::plugins::db::entries::EntryKind::File,
        file_kind: Some("audio".to_string()),
        name: "raw-entry.flac".to_string(),
        hash: Some("raw-entry-hash".to_string()),
        size: 123,
        mtime: 456,
        ctime: 789,
    };
    let entry_public_id = entry.id.clone();
    let entry_db_id = db
        .exec_mut(agdb::QueryBuilder::insert().element(&entry).query())?
        .ids()[0];
    crate::plugins::db::track_sources::upsert(
        &mut db,
        track_db_id,
        entry_db_id,
        crate::plugins::db::track_sources::TrackSourceUpsert {
            source_kind: "embedded_tags".to_string(),
            source_key: format!("entry:{}:embedded", entry_db_id.0),
            is_primary: true,
            start_ms: None,
            end_ms: None,
        },
        None,
    )?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.entries"])]),
        default_server_info(),
        db,
    )?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local entries = require("@lyra/entries")
                local rows = entries.get({track_db_id})
                return rows[1].db_id,
                    rows[1].id,
                    rows[1].kind,
                    rows[1].name,
                    rows[1].hash,
                    rows[1].size,
                    rows[1].mtime,
                    rows[1].full_path == nil
            "#,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::Number(entry_db_id.0 as f64),
            luau::Value::String(entry_public_id.into_bytes()),
            luau::Value::String(b"file".to_vec()),
            luau::Value::String(b"raw-entry.flac".to_vec()),
            luau::Value::String(b"raw-entry-hash".to_vec()),
            luau::Value::Number(123.0),
            luau::Value::Number(456.0),
            luau::Value::Boolean(true),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_chromaprint_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let entry = crate::plugins::db::entries::Entry {
        db_id: None,
        id: nanoid::nanoid!(),
        full_path: std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/assets/metadata/integration_track.flac"),
        kind: crate::plugins::db::entries::EntryKind::File,
        file_kind: Some("audio".to_string()),
        name: "integration_track.flac".to_string(),
        hash: None,
        size: 1,
        mtime: 1,
        ctime: 1,
    };
    let entry_db_id = db
        .exec_mut(agdb::QueryBuilder::insert().element(&entry).query())?
        .ids()[0];
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.chromaprint"])]),
        default_server_info(),
        db,
    )?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local chromaprint = require("@lyra/chromaprint")
                local result = chromaprint.compute({entry_db_id})
                executor_chromaprint_result =
                    type(result.fingerprint) == "string"
                    and type(result.duration) == "number"
                    and result.duration > 0
            "#,
            entry_db_id = entry_db_id.0,
        )
        .into_bytes(),
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        &b"return executor_chromaprint_result"[..],
    )?;
    assert_eq!(values, vec![luau::Value::Boolean(true)]);
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_users_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    crate::plugins::db::roles::ensure_builtin_roles(&mut db)?;
    crate::plugins::db::users::create(&mut db, &crate::plugins::db::users::test_user("bob")?)?;
    let alice_id = crate::plugins::db::users::create(
        &mut db,
        &crate::plugins::db::users::test_user("alice")?,
    )?;
    crate::plugins::db::roles::ensure_user_has_role(
        &mut db,
        alice_id,
        crate::plugins::db::roles::BUILTIN_ADMIN_ROLE,
    )?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.users"])]),
        default_server_info(),
        db,
    )?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        r#"
            local users = require("@lyra/users")
            local listed = users.list()
            executor_users_count = #listed
            executor_users_first = listed[1].username
            executor_users_first_role = listed[1].role
            executor_users_second = listed[2].username
            executor_users_second_role = listed[2].role
        "#
        .as_bytes()
        .to_vec(),
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        &b"return executor_users_count, executor_users_first, executor_users_first_role, executor_users_second, executor_users_second_role"[..],
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::Number(2.0),
            luau::Value::String(b"alice".to_vec()),
            luau::Value::String(b"admin".to_vec()),
            luau::Value::String(b"bob".to_vec()),
            luau::Value::Nil,
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_listens_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let user = crate::plugins::db::users::test_user("listener")?;
    let user_public_id = user.id.clone();
    let user_db_id = crate::plugins::db::users::create(&mut db, &user)?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw Listen Track")?;
    let track_public_id = crate::plugins::db::lookup::find_id_by_db_id(&db, track_db_id)?
        .context("inserted track has public id")?;
    for listened_at_ms in [1000_u64, 2500] {
        let listen = crate::plugins::db::listens::Listen {
            db_id: None,
            id: nanoid::nanoid!(),
            track_public_id: track_public_id.clone(),
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: 180_000,
            state: crate::plugins::db::PlaybackState::Completed,
            listened_at_ms,
            created_at_ms: listened_at_ms,
        };
        let session = crate::services::playback_sessions::PlaybackSession {
            db_id: None,
            id: nanoid::nanoid!(),
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: Some(180_000),
            last_position_ms: None,
            state: crate::plugins::db::PlaybackState::Completed,
            listen_recorded: Some(true),
            updated_at_ms: listened_at_ms,
            created_at_ms: listened_at_ms,
        };
        crate::plugins::db::listens::create_and_mark_recorded(
            &mut db,
            &listen,
            track_db_id,
            user_db_id,
            &session,
        )?;
    }
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.listens"])]),
        default_server_info(),
        db,
    )?;
    let mut context = CallContext {
        origin: plugin_origin("demo", "init.luau"),
        ..CallContext::default()
    };
    context.caller.insert(crate::services::auth::Principal {
        user_db_id,
        user_public_id,
        username: "listener".to_string(),
        permissions: vec![crate::plugins::db::Permission::Admin],
        role_name: Some("admin".to_string()),
        accessible_library_ids: std::collections::HashSet::new(),
    });
    runtime.run_plugin_source_with_call_context(
        format!(
            r#"
                local listens = require("@lyra/listens")
                local track_db_id = {track_db_id}
                executor_listen_count = listens.get_count(track_db_id, {user_db_id})
                executor_listen_counts = listens.get_counts({{ track_db_id, -1, track_db_id }}, {user_db_id})
                executor_listen_stats = listens.get_stats({{ track_db_id }}, {user_db_id})
            "#,
            track_db_id = track_db_id.0,
            user_db_id = user_db_id.0,
        )
        .into_bytes(),
        context,
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        format!(
            r#"
                local track_db_id = {track_db_id}
                return executor_listen_count,
                    executor_listen_counts[track_db_id],
                    executor_listen_stats.counts[track_db_id],
                    executor_listen_stats.last_played[track_db_id]
            "#,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::Integer(2),
            luau::Value::Integer(2),
            luau::Value::Integer(2),
            luau::Value::Integer(2500),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_favorites_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let user = crate::plugins::db::users::test_user("favorite-user")?;
    let user_public_id = user.id.clone();
    let user_db_id = crate::plugins::db::users::create(&mut db, &user)?;
    let library_db_id =
        crate::plugins::db::test_db::insert_library(&mut db, "Raw Favorites", "/tmp/raw-fav")?;
    let library_public_id = crate::plugins::db::lookup::find_id_by_db_id(&db, library_db_id)?
        .context("inserted library has public id")?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw Favorite Track")?;
    crate::plugins::db::test_db::connect(&mut db, library_db_id, track_db_id)?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.favorites"])]),
        default_server_info(),
        db,
    )?;
    let mut context = CallContext {
        origin: plugin_origin("demo", "init.luau"),
        ..CallContext::default()
    };
    context.caller.insert(crate::services::auth::Principal {
        user_db_id,
        user_public_id,
        username: "favorite-user".to_string(),
        permissions: Vec::new(),
        role_name: None,
        accessible_library_ids: std::collections::HashSet::from([library_public_id]),
    });
    runtime.run_plugin_source_with_call_context(
        format!(
            r#"
                local favorites = require("@lyra/favorites")
                local user_db_id = {user_db_id}
                local track_db_id = {track_db_id}

                executor_favorite_add = favorites.add(user_db_id, track_db_id)
                executor_favorite_has = favorites.has(user_db_id, track_db_id)
                executor_favorite_many = favorites.has_many(user_db_id, {{ track_db_id, -1, track_db_id, 999999 }})
                executor_favorite_ids = favorites.list_ids(user_db_id, "track")
                executor_favorite_remove = favorites.remove(user_db_id, track_db_id)
                executor_favorite_has_after_remove = favorites.has(user_db_id, track_db_id)
            "#,
            user_db_id = user_db_id.0,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
        context,
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        format!(
            r#"
                local track_db_id = {track_db_id}
                return executor_favorite_add,
                    executor_favorite_has,
                    executor_favorite_many[track_db_id],
                    executor_favorite_many[999999],
                    executor_favorite_ids[1],
                    executor_favorite_remove,
                    executor_favorite_has_after_remove
            "#,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::Boolean(true),
            luau::Value::Boolean(true),
            luau::Value::Boolean(true),
            luau::Value::Boolean(false),
            luau::Value::Integer(track_db_id.0),
            luau::Value::Boolean(true),
            luau::Value::Boolean(false),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_artists_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let library_db_id = crate::plugins::db::test_db::insert_library(
        &mut db,
        "Raw Artists Library",
        "/tmp/raw-artists",
    )?;
    let release_db_id =
        crate::plugins::db::test_db::insert_release(&mut db, "Raw Artists Release")?;
    crate::plugins::db::test_db::connect(&mut db, library_db_id, release_db_id)?;
    let artist_db_id = crate::plugins::db::test_db::insert_artist(&mut db, "Raw Artist Module")?;
    let mut artist = crate::plugins::db::artists::get_by_id(&db, artist_db_id)?
        .context("inserted artist exists")?;
    artist.set_artist_type(crate::plugins::db::ArtistType::Person);
    crate::plugins::db::artists::update(&mut db, &artist)?;
    crate::plugins::db::test_db::connect_artist(&mut db, release_db_id, artist_db_id)?;
    let actor_db_id = crate::plugins::db::test_db::insert_artist(&mut db, "Raw Voice Actor")?;
    let character_db_id = crate::plugins::db::test_db::insert_artist(&mut db, "Raw Character")?;
    crate::plugins::db::artists::relations::link(
        &mut db,
        actor_db_id,
        character_db_id,
        crate::plugins::db::ArtistRelationType::VoiceActor,
        Some("Lead".to_string()),
    )?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.artists"])]),
        default_server_info(),
        db,
    )?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local artists = require("@lyra/artists")
                local library_db_id = {library_db_id}
                local release_db_id = {release_db_id}
                local actor_db_id = {actor_db_id}

                local listed = artists.list()
                local by_library = artists.list_by_library(library_db_id)
                local many = artists.list_many({{ release_db_id }})
                local relations = artists.list_relations_many({{ actor_db_id }})
                local queried = artists.query({{
                    search_term = "Module",
                    artist_type = artists.ArtistType.Person,
                    sort_by = {{ "name" }},
                    sort_order = "ascending",
                }})
                local credited = artists.query_credited({{
                    scope = release_db_id,
                    artist_type = artists.ArtistType.Person,
                    credit_types = {{ artists.CreditType.Artist }},
                }})

                return artists.ArtistType.Person,
                    artists.ArtistRelationType.VoiceActor,
                    artists.CreditType.Artist,
                    listed[1] ~= nil,
                    by_library[1].artist_name,
                    many[release_db_id][1].artist_name,
                    relations[actor_db_id][1].relation_type,
                    relations[actor_db_id][1].direction,
                    relations[actor_db_id][1].artist.artist_name,
                    queried.entities[1].artist_name,
                    credited.entities[1].artist_name,
                    queried.total_count,
                    credited.total_count
            "#,
            library_db_id = library_db_id.0,
            release_db_id = release_db_id.0,
            actor_db_id = actor_db_id.0,
        )
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::String(b"person".to_vec()),
            luau::Value::String(b"voice_actor".to_vec()),
            luau::Value::String(b"artist".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::String(b"Raw Artist Module".to_vec()),
            luau::Value::String(b"Raw Artist Module".to_vec()),
            luau::Value::String(b"voice_actor".to_vec()),
            luau::Value::String(b"outgoing".to_vec()),
            luau::Value::String(b"Raw Character".to_vec()),
            luau::Value::String(b"Raw Artist Module".to_vec()),
            luau::Value::String(b"Raw Artist Module".to_vec()),
            luau::Value::Integer(1),
            luau::Value::Integer(1),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_tracks_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let release_db_id = crate::plugins::db::test_db::insert_release(&mut db, "Raw Track Release")?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw Track Module Song")?;
    db.exec_mut(
        agdb::QueryBuilder::insert()
            .edges()
            .from(release_db_id)
            .to(track_db_id)
            .query(),
    )?;
    let track_public_id = crate::plugins::db::lookup::find_id_by_db_id(&db, track_db_id)?
        .context("inserted track has public id")?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.tracks"])]),
        default_server_info(),
        db,
    )?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local tracks = require("@lyra/tracks")
                local release_db_id = {release_db_id}
                local track_db_id = {track_db_id}

                local listed = tracks.list(release_db_id)
                local all = tracks.list()
                local fetched = tracks.get_by_ids({{ track_db_id, -1, track_db_id }})
                local related = tracks.list_many({{ release_db_id }})
                local queried = tracks.query({{
                    scope = release_db_id,
                    search_term = "Module",
                    sort_by = {{ "name" }},
                    sort_order = "ascending",
                    limit = 5,
                }})

                return listed[1].track_title,
                    all[1] ~= nil,
                    fetched[track_db_id].id,
                    related[release_db_id][1].track_title,
                    queried.entities[1].track_title,
                    queried.total_count,
                    queried.offset
            "#,
            release_db_id = release_db_id.0,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::String(b"Raw Track Module Song".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::String(track_public_id.as_bytes().to_vec()),
            luau::Value::String(b"Raw Track Module Song".to_vec()),
            luau::Value::String(b"Raw Track Module Song".to_vec()),
            luau::Value::Integer(1),
            luau::Value::Integer(0),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_track_sources_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw Source Track")?;
    let entry = crate::plugins::db::entries::Entry {
        db_id: None,
        id: nanoid::nanoid!(),
        full_path: std::path::PathBuf::from("/music/raw-source.FLAC"),
        kind: crate::plugins::db::entries::EntryKind::File,
        file_kind: Some("audio".to_string()),
        name: "raw-source.FLAC".to_string(),
        hash: None,
        size: 1,
        mtime: 1,
        ctime: 1,
    };
    let entry_db_id = db
        .exec_mut(agdb::QueryBuilder::insert().element(&entry).query())?
        .ids()[0];
    let source_key = format!("entry:{}:embedded", entry_db_id.0);
    crate::plugins::db::track_sources::upsert(
        &mut db,
        track_db_id,
        entry_db_id,
        crate::plugins::db::track_sources::TrackSourceUpsert {
            source_kind: "embedded_tags".to_string(),
            source_key: source_key.clone(),
            is_primary: true,
            start_ms: None,
            end_ms: None,
        },
        None,
    )?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.track_sources"])]),
        default_server_info(),
        db,
    )?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local track_sources = require("@lyra/track_sources")
                local track_db_id = {track_db_id}

                local many = track_sources.get_primary_containers({{ track_db_id, -1, track_db_id }})
                return track_sources.get_primary_source_key(track_db_id),
                    track_sources.get_primary_container(track_db_id),
                    many[track_db_id],
                    track_sources.get_primary_source_key(-1) == nil
            "#,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::String(source_key.into_bytes()),
            luau::Value::String(b"flac".to_vec()),
            luau::Value::String(b"flac".to_vec()),
            luau::Value::Boolean(true),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_playback_sources_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let user = crate::plugins::db::users::test_user("playback-source-user")?;
    let user_public_id = user.id.clone();
    let user_db_id = crate::plugins::db::users::create(&mut db, &user)?;
    let track_db_id =
        crate::plugins::db::test_db::insert_track(&mut db, "Raw Playback Source Track")?;
    let entry_path = std::env::temp_dir().join(format!(
        "lyra-raw-playback-source-{}.flac",
        nanoid::nanoid!()
    ));
    std::fs::write(&entry_path, b"plugin playback source")?;
    let entry = crate::plugins::db::entries::Entry {
        db_id: None,
        id: nanoid::nanoid!(),
        full_path: entry_path.clone(),
        kind: crate::plugins::db::entries::EntryKind::File,
        file_kind: Some("audio".to_string()),
        name: "raw-playback-source.flac".to_string(),
        hash: Some("raw-playback-hash".to_string()),
        size: 19,
        mtime: 321,
        ctime: 654,
    };
    let entry_db_id = db
        .exec_mut(agdb::QueryBuilder::insert().element(&entry).query())?
        .ids()[0];
    let source_key = format!("entry:{}:embedded", entry_db_id.0);
    let source_id = crate::plugins::db::track_sources::upsert(
        &mut db,
        track_db_id,
        entry_db_id,
        crate::plugins::db::track_sources::TrackSourceUpsert {
            source_kind: "embedded_tags".to_string(),
            source_key: source_key.clone(),
            is_primary: true,
            start_ms: Some(100),
            end_ms: Some(200),
        },
        None,
    )?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.playback_sources"])]),
        default_server_info(),
        db,
    )?;
    let mut context = CallContext {
        origin: plugin_origin("demo", "init.luau"),
        ..CallContext::default()
    };
    context.caller.insert(crate::services::auth::Principal {
        user_db_id,
        user_public_id,
        username: "playback-source-user".to_string(),
        permissions: vec![
            crate::plugins::db::Permission::Admin,
            crate::plugins::db::Permission::ManageLibraries,
        ],
        role_name: Some("admin".to_string()),
        accessible_library_ids: std::collections::HashSet::new(),
    });
    runtime.run_plugin_source_with_call_context(
        format!(
            r#"
                local playback_sources = require("@lyra/playback_sources")
                local track_db_id = {track_db_id}

                executor_playback_source_rows = playback_sources.get(track_db_id, true)
                executor_playback_source_many = playback_sources.get_many({{ track_db_id, -1, track_db_id, 999999 }}, true)
            "#,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
        context,
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        format!(
            r#"
                local track_db_id = {track_db_id}
                local first = executor_playback_source_rows[1]
                local many = executor_playback_source_many[track_db_id]

                return first.track_id,
                    first.source_id,
                    first.source_kind,
                    first.source_key,
                    first.is_primary,
                    first.start_ms,
                    first.end_ms,
                    first.is_virtual,
                    first.entry.name,
                    first.entry.hash,
                    first.entry.full_path,
                    many.entry.name,
                    executor_playback_source_many[999999] == nil
            "#,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;
    let _ = std::fs::remove_file(&entry_path);

    assert_eq!(
        values,
        vec![
            luau::Value::Integer(track_db_id.0),
            luau::Value::Integer(source_id.0),
            luau::Value::String(b"embedded_tags".to_vec()),
            luau::Value::String(source_key.into_bytes()),
            luau::Value::Boolean(true),
            luau::Value::Integer(100),
            luau::Value::Integer(200),
            luau::Value::Boolean(true),
            luau::Value::String(b"raw-playback-source.flac".to_vec()),
            luau::Value::String(b"raw-playback-hash".to_vec()),
            luau::Value::String(entry_path.to_string_lossy().into_owned().into_bytes()),
            luau::Value::String(b"raw-playback-source.flac".to_vec()),
            luau::Value::Boolean(true),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_playlists_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let user = crate::plugins::db::users::test_user("playlist-user")?;
    let user_public_id = user.id.clone();
    let user_db_id = crate::plugins::db::users::create(&mut db, &user)?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw Playlist Track")?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.playlists"])]),
        default_server_info(),
        db,
    )?;
    let mut context = CallContext {
        origin: plugin_origin("demo", "init.luau"),
        ..CallContext::default()
    };
    context.caller.insert(crate::services::auth::Principal {
        user_db_id,
        user_public_id,
        username: "playlist-user".to_string(),
        permissions: vec![crate::plugins::db::Permission::Admin],
        role_name: Some("admin".to_string()),
        accessible_library_ids: std::collections::HashSet::new(),
    });
    runtime.run_plugin_source_with_call_context(
        format!(
            r#"
                local playlists = require("@lyra/playlists")
                local user_db_id = {user_db_id}
                local track_db_id = {track_db_id}

                executor_playlist_id = playlists.create({{
                    user_id = user_db_id,
                    name = "Raw Playlist",
                    description = "seed",
                    is_public = false,
                    created_at = 10,
                    updated_at = 20,
                }})
                executor_playlist = playlists.get_by_id(executor_playlist_id)
                executor_playlist_owner = playlists.get_owner(executor_playlist_id)
                executor_user_playlists = playlists.get_by_user(user_db_id)
                executor_playlist_entry_id = playlists.add_track(executor_playlist_id, track_db_id)
                executor_playlist_tracks = playlists.get_tracks(executor_playlist_id)
                executor_playlist_tracks_many = playlists.get_tracks_many({{ executor_playlist_id, -1, executor_playlist_id }})
                executor_updated_playlist = playlists.update({{
                    playlist_id = executor_playlist_id,
                    name = "Raw Updated Playlist",
                    description = "changed",
                    is_public = true,
                    updated_at = 30,
                }})
                playlists.remove_track(executor_playlist_entry_id)
                executor_playlist_tracks_after_remove = playlists.get_tracks(executor_playlist_id)
            "#,
            user_db_id = user_db_id.0,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
        context,
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        format!(
            r#"
                return executor_playlist_id ~= nil,
                    executor_playlist.name,
                    executor_playlist.db_id == executor_playlist_id,
                    executor_playlist_owner,
                    executor_user_playlists[1].name,
                    executor_playlist_entry_id ~= nil,
                    executor_playlist_tracks[1].track_id,
                    executor_playlist_tracks[1].entry_id == executor_playlist_entry_id,
                    executor_playlist_tracks_many[executor_playlist_id][1].track_id,
                    executor_updated_playlist.name,
                    executor_updated_playlist.is_public,
                    executor_playlist_tracks_after_remove[1] == nil
            "#,
        )
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::Boolean(true),
            luau::Value::String(b"Raw Playlist".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::Integer(user_db_id.0),
            luau::Value::String(b"Raw Playlist".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::Number(track_db_id.0 as f64),
            luau::Value::Boolean(true),
            luau::Value::Number(track_db_id.0 as f64),
            luau::Value::String(b"Raw Updated Playlist".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::Boolean(true),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_covers_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let user = crate::plugins::db::users::test_user("cover-user")?;
    let user_public_id = user.id.clone();
    let user_db_id = crate::plugins::db::users::create(&mut db, &user)?;
    let release_db_id =
        crate::plugins::db::test_db::insert_release(&mut db, "Raw Covered Release")?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw Covered Track")?;
    crate::plugins::db::test_db::connect(&mut db, release_db_id, track_db_id)?;
    let cover_path = std::env::temp_dir().join(format!("lyra-raw-cover-{}.jpg", nanoid::nanoid!()));
    std::fs::write(&cover_path, b"raw cover")?;
    let cover_path_string = cover_path.to_string_lossy().into_owned();
    crate::plugins::db::covers::upsert(
        &mut db,
        release_db_id,
        crate::plugins::db::Cover {
            db_id: None,
            id: nanoid::nanoid!(),
            path: cover_path_string.clone(),
            mime_type: "image/jpeg".to_string(),
            hash: "raw-cover-hash".to_string(),
            blurhash: Some("raw-blurhash".to_string()),
        },
    )?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.covers"])]),
        default_server_info(),
        db,
    )?;
    let mut context = CallContext {
        origin: plugin_origin("demo", "init.luau"),
        ..CallContext::default()
    };
    context.caller.insert(crate::services::auth::Principal {
        user_db_id,
        user_public_id,
        username: "cover-user".to_string(),
        permissions: vec![crate::plugins::db::Permission::Admin],
        role_name: Some("admin".to_string()),
        accessible_library_ids: std::collections::HashSet::new(),
    });
    runtime.run_plugin_source_with_call_context(
        format!(
            r#"
                local covers = require("@lyra/covers")
                local release_db_id = {release_db_id}
                local track_db_id = {track_db_id}

                executor_cover_release = covers.get(release_db_id)
                executor_cover_track = covers.get(track_db_id)
                executor_covers_many = covers.get_many({{ release_db_id, track_db_id, -1, release_db_id, 999999 }})
            "#,
            release_db_id = release_db_id.0,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
        context,
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        format!(
            r#"
                local release_db_id = {release_db_id}
                local track_db_id = {track_db_id}

                return executor_cover_release.path,
                    executor_cover_release.mime_type,
                    executor_cover_release.hash,
                    executor_cover_release.blurhash,
                    executor_cover_release.release_id,
                    executor_cover_track.release_id,
                    executor_covers_many[release_db_id].hash,
                    executor_covers_many[track_db_id].release_id,
                    executor_covers_many[999999] == nil
            "#,
            release_db_id = release_db_id.0,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;
    let _ = std::fs::remove_file(&cover_path);

    assert_eq!(
        values,
        vec![
            luau::Value::String(cover_path_string.into_bytes()),
            luau::Value::String(b"image/jpeg".to_vec()),
            luau::Value::String(b"raw-cover-hash".to_vec()),
            luau::Value::String(b"raw-blurhash".to_vec()),
            luau::Value::Integer(release_db_id.0),
            luau::Value::Integer(release_db_id.0),
            luau::Value::String(b"raw-cover-hash".to_vec()),
            luau::Value::Integer(release_db_id.0),
            luau::Value::Boolean(true),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_releases_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let release_db_id = crate::plugins::db::test_db::insert_release(&mut db, "Raw Release Module")?;
    crate::plugins::db::test_db::insert_release(&mut db, "Other Release")?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw Release Track")?;
    crate::plugins::db::test_db::connect(&mut db, release_db_id, track_db_id)?;
    let album_artist_id = crate::plugins::db::test_db::insert_artist(&mut db, "Raw Album Artist")?;
    crate::plugins::db::test_db::connect_artist(&mut db, release_db_id, album_artist_id)?;
    let guest_artist_id = crate::plugins::db::test_db::insert_artist(&mut db, "Raw Guest Artist")?;
    crate::plugins::db::test_db::connect_artist(&mut db, track_db_id, guest_artist_id)?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.releases"])]),
        default_server_info(),
        db,
    )?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local releases = require("@lyra/releases")
                local release_db_id = {release_db_id}
                local track_db_id = {track_db_id}
                local album_artist_id = {album_artist_id}
                local guest_artist_id = {guest_artist_id}

                local listed = releases.list(track_db_id)
                local all = releases.list()
                local by_artist = releases.get_by_artist(album_artist_id)
                local appearances = releases.get_appearances(guest_artist_id)
                local many = releases.list_many({{ track_db_id, -1, track_db_id }})
                local queried = releases.query({{
                    scope = "releases",
                    search_term = "Module",
                    sort_by = {{ "name" }},
                    sort_order = "ascending",
                    limit = 5,
                }})

                return listed[1].release_title,
                    all[1] ~= nil,
                    by_artist[1].release_title,
                    appearances[1].release_title,
                    many[track_db_id][1].release_title,
                    queried.entities[1].release_title,
                    queried.total_count,
                    queried.offset
            "#,
            release_db_id = release_db_id.0,
            track_db_id = track_db_id.0,
            album_artist_id = album_artist_id.0,
            guest_artist_id = guest_artist_id.0,
        )
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::String(b"Raw Release Module".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::String(b"Raw Release Module".to_vec()),
            luau::Value::String(b"Raw Release Module".to_vec()),
            luau::Value::String(b"Raw Release Module".to_vec()),
            luau::Value::String(b"Raw Release Module".to_vec()),
            luau::Value::Integer(1),
            luau::Value::Integer(0),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_libraries_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let library_db_id =
        crate::plugins::db::test_db::insert_library(&mut db, "Raw Library", "/tmp/raw-lib")?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw Library Track")?;
    crate::plugins::db::test_db::connect(&mut db, library_db_id, track_db_id)?;
    let library_public_id = crate::plugins::db::lookup::find_id_by_db_id(&db, library_db_id)?
        .context("inserted library has public id")?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.libraries"])]),
        default_server_info(),
        db,
    )?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local libraries = require("@lyra/libraries")
                local library_db_id = {library_db_id}
                local library_public_id = "{library_public_id}"
                local track_db_id = {track_db_id}

                local all = libraries.list()
                local by_id = libraries.list(library_db_id)
                local by_public = libraries.list(library_public_id)
                local for_entity = libraries.get_for_entity(track_db_id)
                local many = libraries.get_for_entities({{ track_db_id, library_db_id }})

                return all[1].name,
                    by_id[1].path,
                    by_public[1].db_id,
                    for_entity[1].name,
                    many[track_db_id].name,
                    many[library_db_id].name
            "#,
            library_db_id = library_db_id.0,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::String(b"Raw Library".to_vec()),
            luau::Value::String(b"/tmp/raw-lib".to_vec()),
            luau::Value::Number(library_db_id.0 as f64),
            luau::Value::String(b"Raw Library".to_vec()),
            luau::Value::String(b"Raw Library".to_vec()),
            luau::Value::String(b"Raw Library".to_vec()),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_genres_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let release_db_id = crate::plugins::db::test_db::insert_release(&mut db, "Raw Genre Release")?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.genres"])]),
        default_server_info(),
        db,
    )?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local genres = require("@lyra/genres")
                local release_db_id = {release_db_id}

                local parent_id = genres.resolve({{
                    name = "Electronic",
                    aliases = {{ {{ name = "Electronica", locale = "en" }} }},
                }})
                local child_id = genres.add(release_db_id, {{
                    name = "Synthpop",
                    aliases = {{ {{ name = "Synth Pop" }} }},
                    external_id = {{
                        provider_id = "wikidata",
                        id_type = "qid",
                        id = "Q12345",
                    }},
                }})
                genres.add_parent(child_id, parent_id)

                local by_id = genres.get_by_id(child_id)
                local by_name = genres.find_by_name(" synthpop ")
                local parents = genres.get_parents(child_id)
                local children = genres.get_children(parent_id)
                local releases = genres.get_releases(child_id)
                local releases_many = genres.get_releases_many({{ child_id, parent_id, child_id }})
                local for_release = genres.get_for_release(release_db_id)
                local for_many = genres.get_for_releases_many({{ release_db_id }})

                return by_id.name,
                    by_name.name,
                    parents[1].name,
                    children[1].name,
                    releases[1],
                    releases_many[child_id][1],
                    for_release[1].name,
                    for_many[release_db_id][1].name,
                    #releases_many[parent_id]
            "#,
            release_db_id = release_db_id.0,
        )
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::String(b"Synthpop".to_vec()),
            luau::Value::String(b"Synthpop".to_vec()),
            luau::Value::String(b"Electronic".to_vec()),
            luau::Value::String(b"Synthpop".to_vec()),
            luau::Value::Number(release_db_id.0 as f64),
            luau::Value::Number(release_db_id.0 as f64),
            luau::Value::String(b"Synthpop".to_vec()),
            luau::Value::String(b"Synthpop".to_vec()),
            luau::Value::Number(0.0),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_tags_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let user_db_id = crate::plugins::db::users::create(
        &mut db,
        &crate::plugins::db::users::test_user("raw-tags")?,
    )?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw Tag Track")?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.tags"])]),
        default_server_info(),
        db,
    )?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        format!(
            r##"
                local tags = require("@lyra/tags")
                local user_id = {user_db_id}
                local track_id = {track_db_id}

                executor_tag_name = tags.add(user_id, track_id, " Workout ", "#335577")
                executor_has = tags.has(user_id, track_id, "Workout")
                executor_has_many = tags.has_many(user_id, {{ track_id, -1, track_id, 999999 }}, "Workout")
                executor_for_target = tags.get_for_target(user_id, track_id)
                executor_for_targets = tags.get_for_targets_many(user_id, {{ track_id, 999999 }})
                executor_tagged = tags.get_tagged(user_id, "Workout")
                tags.remove(user_id, track_id, "Workout")
                executor_has_after_remove = tags.has(user_id, track_id, "Workout")
            "##,
            user_db_id = user_db_id.0,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        format!(
            r#"
                local track_id = {track_db_id}

                return executor_tag_name,
                    executor_has,
                    executor_has_many[track_id],
                    executor_has_many[999999] == false,
                    executor_for_target[1].tag,
                    executor_for_target[1].color,
                    executor_for_targets[track_id][1].tag,
                    executor_for_targets[999999] ~= nil and #executor_for_targets[999999] == 0,
                    executor_tagged[1],
                    executor_has_after_remove
            "#,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::String(b"Workout".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::Boolean(true),
            luau::Value::Boolean(true),
            luau::Value::String(b"Workout".to_vec()),
            luau::Value::String(b"#335577".to_vec()),
            luau::Value::String(b"Workout".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::Integer(track_db_id.0),
            luau::Value::Boolean(false),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_datastore_module() -> Result<()> {
    let db = crate::plugins::db::test_db::new_test_db()?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.datastore"])]),
        default_server_info(),
        db,
    )?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local datastore = require("@lyra/datastore")
            local store = datastore.get_or_create("raw-store")

            store:set("answer", { value = 42, tags = { "a", "b" } })
            executor_answer = store:get("answer")
            store:set_many({ alpha = true, beta = "two" })
            executor_many = store:get_many({ "alpha", "missing", "beta" })
            executor_removed = store:remove("beta")
            executor_cleared = store:clear()
        "#[..],
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        &br#"
            return executor_answer.value == 42,
                executor_answer.tags[2],
                executor_many[1],
                executor_many[2] == nil,
                executor_many[3],
                executor_removed,
                executor_cleared
        "#[..],
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::Boolean(true),
            luau::Value::String(b"b".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::Boolean(true),
            luau::Value::String(b"two".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::Integer(2),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_lyra_lyrics_parse_lrc() -> Result<()> {
    let runtime = runtime_with_scopes(&["lyra.lyrics"])?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local lyrics = require("@lyra/lyrics")
            local parsed = lyrics.parse_lrc("[00:01.500]Hello\n[00:02.000]<00:02.100>Wide <00:02.500>World", " ")
            local exports =
                type(lyrics.get) .. ":" ..
                type(lyrics.parse_lrc) .. ":" ..
                type(lyrics.upsert) .. ":" ..
                type(lyrics.upsert_user_override) .. ":" ..
                type(lyrics.delete_user_override_for_track) .. ":" ..
                type(lyrics.delete_for_track) .. ":" ..
                type(lyrics.has) .. ":" ..
                type(lyrics.has_many)

            return exports,
                parsed.id,
                parsed.language,
                parsed.plain_text,
                #parsed.lines,
                parsed.lines[1].ts_ms,
                parsed.lines[1].text,
                parsed.lines[2].text,
                #parsed.lines[2].words,
                parsed.lines[2].words[1].ts_ms
        "#[..],
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::String(
                b"function:function:function:function:function:function:function:function".to_vec(),
            ),
            luau::Value::String(Vec::new()),
            luau::Value::String(b"und".to_vec()),
            luau::Value::String(b"Hello\nWide World".to_vec()),
            luau::Value::Number(2.0),
            luau::Value::Number(1_500.0),
            luau::Value::String(b"Hello".to_vec()),
            luau::Value::String(b"Wide World".to_vec()),
            luau::Value::Number(2.0),
            luau::Value::Number(2_100.0),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_drives_task_scheduler_for_plugin_source() -> Result<()> {
    let runtime = runtime_with_scopes(&["harmony.task"])?;
    runtime
        .run_plugin_source(
            "demo",
            "init.luau",
            &br#"
                local task = require("@harmony/task")
                executor_total = 0

                task.spawn(function(value)
                    executor_total = executor_total + value
                end, 41)

                task.wait(0)
                executor_total = executor_total + 1
            "#[..],
        )
        .context("run plugin source")?;

    let values = runtime.eval_plugin_source("demo", "check.luau", &b"return executor_total"[..])?;
    assert_eq!(values, vec![luau::Value::Number(42.0)]);
    Ok(())
}

#[test]
fn plugin_executor_loads_plugin_self_and_relative_sources_from_filesystem() -> Result<()> {
    let test_dir = std::env::temp_dir().join(format!(
        "lyra-raw-fs-loader-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    let plugin_dir = test_dir.join("plugins").join("demo").join("lib");
    std::fs::create_dir_all(&plugin_dir)?;
    std::fs::write(
        plugin_dir.join("main.luau"),
        b"local util = require(\"./util.luau\"); return { value = util.value + 1 }",
    )?;
    std::fs::write(plugin_dir.join("util.luau"), b"return { value = 41 }")?;

    let runtime = PluginExecutor::with_filesystem_sources(
        Arc::from(Vec::<harmony_core::PluginManifest>::new()),
        default_server_info(),
        test_dir.join("root"),
        test_dir.join("plugins"),
    )?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local main = require("@self/lib/main.luau")
            executor_fs_value = main.value
        "#[..],
    )?;

    let values =
        runtime.eval_plugin_source("demo", "check.luau", &b"return executor_fs_value"[..])?;
    assert_eq!(values, vec![luau::Value::Number(42.0)]);

    let _ = std::fs::remove_dir_all(test_dir);
    Ok(())
}

#[test]
fn plugin_executor_keeps_relative_require_origin_for_returned_source_functions() -> Result<()> {
    let test_dir = std::env::temp_dir().join(format!(
        "lyra-raw-late-require-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    let plugin_dir = test_dir.join("plugins").join("demo").join("lib");
    std::fs::create_dir_all(&plugin_dir)?;
    std::fs::write(
        plugin_dir.join("main.luau"),
        b"return { load = function() return require(\"./late.luau\").value end }",
    )?;
    std::fs::write(plugin_dir.join("late.luau"), b"return { value = 42 }")?;

    let runtime = PluginExecutor::with_filesystem_sources(
        Arc::from(Vec::<harmony_core::PluginManifest>::new()),
        default_server_info(),
        test_dir.join("root"),
        test_dir.join("plugins"),
    )?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local main = require("@self/lib/main.luau")
            executor_late_require_value = main.load()
        "#[..],
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        &b"return executor_late_require_value"[..],
    )?;
    assert_eq!(values, vec![luau::Value::Number(42.0)]);

    let _ = std::fs::remove_dir_all(test_dir);
    Ok(())
}

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
        manifest("demo", &["harmony.json"]),
        manifest("denied", &[]),
    ]))?;

    let allowed = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local json = require("@harmony/json")
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
                local json = require("@harmony/json")
                return json.encode({ answer = 42 })
            "#[..],
        )
        .expect_err("undeclared capability should be denied");

    assert!(
        denied
            .to_string()
            .contains("without capability 'harmony.json'"),
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
            "scopes": ["harmony.json"]
        }"#,
    )?;
    std::fs::write(
        plugin_dir.join("init.luau"),
        br#"
            local json = require("@harmony/json")
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
    futures::executor::block_on(crate::plugins::runtime::REGISTRY.write()).clear();
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
            "scopes": ["harmony.json"]
        }"#,
    )?;
    std::fs::write(
        plugin_dir.join("init.luau"),
        br#"
            local json = require("@harmony/json")
            executor_handle_output = json.encode({ answer = 42 })
        "#,
    )?;

    let db = crate::plugins::db::test_db::new_test_db()?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));
    let (runtime, errors) = PluginExecutorHandle::discover_from_plugins_dir_with_db(
        test_dir.join("plugins"),
        default_server_info(),
        db,
    )?;
    assert!(errors.is_empty(), "{errors:?}");
    assert!(runtime.has_plugin("demo")?);
    assert_eq!(runtime.plugin_manifests()?[0].id, "demo");

    runtime.exec_all()?;
    runtime.exec_plugin("demo")?;

    let _ = std::fs::remove_dir_all(test_dir);
    Ok(())
}
