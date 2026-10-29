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
        commit_hash: "test".to_string(),
        hostname: "localhost".to_string(),
        port: 0,
        published_url: None,
        setup: crate::services::setup::Status {
            account_required: true,
            plugin_selection_required: true,
        },
    }
}

fn default_auth_capabilities() -> crate::plugins::auth::AuthCapabilities {
    crate::plugins::auth::AuthCapabilities {
        enabled: false,
        allow_default_login_when_disabled: true,
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
            default_auth_capabilities(),
            stores::PluginModuleStores::empty(),
            MemorySourceLoader::new(),
        )
    }

    pub(crate) fn with_database(
        manifests: Arc<[PluginManifest]>,
        server_info: crate::plugins::server::ServerInfo,
        db: lyra_db::DbAsync,
    ) -> Result<Self> {
        Self::with_loader(
            manifests,
            server_info,
            default_auth_capabilities(),
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
            default_auth_capabilities(),
            stores::PluginModuleStores::empty(),
            Vec::new(),
        )
    }

    pub(crate) fn discover_from_plugins_dir_with_db(
        plugins_dir: impl Into<std::path::PathBuf>,
        server_info: crate::plugins::server::ServerInfo,
        db: lyra_db::DbAsync,
    ) -> Result<(Self, Vec<PluginLoadError>)> {
        Self::discover_from_plugins_dir_with_db_and_modules(
            plugins_dir,
            server_info,
            default_auth_capabilities(),
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

fn seed_caller_principal(context: &mut CallContext, principal: crate::services::auth::Principal) {
    let dispatch_auth = crate::plugins::auth::DispatchAuth::default();
    dispatch_auth.record(principal);
    context.caller.insert(dispatch_auth);
}

/// A call context acting as an admin who exists in the shared test DB.
fn admin_call_context(path: &str) -> Result<CallContext> {
    let mut principal = futures::executor::block_on(async {
        let user_db_id = {
            let mut db = crate::services::STATE.db.write().await;
            lyra_db::test_db::insert_user(&mut db, "dispatch-admin")?
        };
        crate::services::testing::user_principal(user_db_id).await
    })?;
    principal.permissions = vec![lyra_db::Permission::Admin];
    let mut context = CallContext {
        origin: plugin_origin("demo", path.to_string()),
        ..CallContext::default()
    };
    seed_caller_principal(&mut context, principal);
    Ok(context)
}

/// A plugins directory holding plugin `id`, with `scopes` and `init` as its
/// entrypoint source. Removed when dropped.
struct TempPlugin {
    dir: tempfile::TempDir,
    id: String,
}

impl TempPlugin {
    fn plugins_dir(&self) -> std::path::PathBuf {
        self.dir.path().join("plugins")
    }

    fn plugin_dir(&self) -> std::path::PathBuf {
        self.plugins_dir().join(&self.id)
    }

    fn write(&self, file: &str, contents: impl AsRef<[u8]>) -> Result<()> {
        Ok(std::fs::write(self.plugin_dir().join(file), contents)?)
    }
}

/// Default test state with an empty plugin API route registry.
fn init_api_test_state() -> Result<()> {
    crate::plugins::testing::init_default_test_state()?;
    futures::executor::block_on(crate::plugins::api::initialize_registry(
        std::collections::HashSet::new(),
        None,
    ));
    Ok(())
}

/// An unauthenticated `GET` to `path` with no query, params or body.
fn api_request(
    handler: crate::services::plugin_dispatch::PluginCallbackHandle,
    path: &str,
) -> ApiHandlerRequest {
    ApiHandlerRequest {
        handler,
        plugin_id: "demo".to_string(),
        method: "GET".to_string(),
        path: path.to_string(),
        headers: Vec::new(),
        query: HashMap::new(),
        params: HashMap::new(),
        body: Vec::new(),
        auth: None,
        client: Default::default(),
    }
}

/// An unauthenticated websocket start for `path` and the ends of its
/// inbound and outbound channels.
fn websocket_request(
    handler: crate::services::plugin_dispatch::PluginCallbackHandle,
    path: &str,
) -> (
    WebSocketStartRequest,
    tokio::sync::mpsc::Sender<String>,
    tokio::sync::mpsc::Receiver<String>,
) {
    let (inbound_tx, inbound_rx) = tokio::sync::mpsc::channel(4);
    let (outbound_tx, outbound_rx) = tokio::sync::mpsc::channel(4);
    let request = WebSocketStartRequest {
        handler,
        plugin_id: "demo".to_string(),
        method: "GET".to_string(),
        path: path.to_string(),
        headers: Vec::new(),
        query: HashMap::new(),
        params: HashMap::new(),
        auth: None,
        dispatch_auth: crate::plugins::auth::DispatchAuth::default(),
        inbound: Arc::new(tokio::sync::Mutex::new(inbound_rx)),
        outbound: outbound_tx,
        state: WebSocketState::new(),
    };
    (request, inbound_tx, outbound_rx)
}

fn temp_plugin(id: &str, scopes: &[&str], init: impl AsRef<[u8]>) -> Result<TempPlugin> {
    let plugin = TempPlugin {
        dir: tempfile::tempdir()?,
        id: id.to_string(),
    };
    std::fs::create_dir_all(plugin.plugin_dir())?;
    plugin.write(
        "plugin.json",
        serde_json::json!({
            "schema_version": 1,
            "id": id,
            "name": id,
            "version": "1.0.0",
            "lyra": crate::plugins::testing::HOST_REQUIREMENT,
            "description": "",
            "entrypoint": "init.luau",
            "scopes": scopes,
        })
        .to_string(),
    )?;
    plugin.write("init.luau", init)?;
    Ok(plugin)
}

fn manifest(id: &str, scopes: &[&str]) -> PluginManifest {
    PluginManifest {
        schema_version: 1,
        id: id.to_string(),
        name: format!("{id} Plugin"),
        version: semver::Version::new(1, 0, 0),
        host_requirement: semver::VersionReq::STAR,
        description: "Test manifest".to_string(),
        entrypoint: Some("init.luau".to_string()),
        scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
        dependencies: Default::default(),
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
                        let principal =
                            crate::plugins::auth::require_dispatch_principal(&frame.context)?;
                        frame.returns.write(principal.username.as_str())
                    }),
            )
            .install(|_| Ok(harmony_core::ModuleExport::new(()))),
    )?;

    let mut context = CallContext {
        origin: plugin_origin("demo", "init.luau"),
        ..CallContext::default()
    };
    seed_caller_principal(
        &mut context,
        crate::services::auth::Principal::from_parts(
            agdb::DbId(7),
            "user-public-id".to_string(),
            "raw-user".to_string(),
            vec![lyra_db::Permission::Admin],
            Some("admin".to_string()),
            std::collections::HashSet::new(),
        ),
    );
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
fn plugin_executor_keeps_dispatch_auth_inside_coroutines() -> Result<()> {
    let runtime = runtime_with_scopes(&["test.context"])?;
    let require = runtime.vm.data().get::<RequireRuntime>()?;
    require.register(
        ModuleSpec::new("test/context")
            .capability("test.context")
            .function(
                harmony_core::FunctionSpec::sync_fn("username")
                    .returns::<String>()
                    .call(|mut frame| {
                        let principal =
                            crate::plugins::auth::require_dispatch_principal(&frame.context)?;
                        frame.returns.write(principal.username.as_str())
                    }),
            )
            .install(|_| Ok(harmony_core::ModuleExport::new(()))),
    )?;
    let mut context = CallContext {
        origin: plugin_origin("demo", "init.luau"),
        ..CallContext::default()
    };
    seed_caller_principal(
        &mut context,
        crate::services::auth::Principal::from_parts(
            agdb::DbId(7),
            "user-public-id".to_string(),
            "raw-user".to_string(),
            vec![lyra_db::Permission::Admin],
            None,
            std::collections::HashSet::new(),
        ),
    );

    let values = runtime.eval_plugin_source_with_call_context(
        br#"
            local context = require("@test/context")
            return coroutine.wrap(function()
                return context.username()
            end)()
        "# as &[u8],
        context,
    )?;
    assert_eq!(values, vec![luau::Value::String(b"raw-user".to_vec())]);
    Ok(())
}

#[test]
fn plugin_executor_refuses_registrations_from_an_ended_run() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    init_api_test_state()?;
    let runtime = runtime_with_scopes(&["lyra.api"])?;
    runtime.run_plugin_source("demo", "init.luau", &b""[..])?;
    let error = runtime
        .run_plugin_source_with_call_context(
            br#"
                local api = require("@lyra/api")
                api.get("/ended", function()
                    return api.response.empty(204)
                end, "public")
            "# as &[u8],
            CallContext {
                origin: plugin_origin("demo", "ended.luau"),
                task_group: harmony_core::TaskGroupId(u64::MAX),
                ..CallContext::default()
            },
        )
        .expect_err("a registration from a run that has ended must be refused");
    assert!(
        format!("{error:#}").contains("run that has ended"),
        "{error:#}"
    );
    Ok(())
}

#[test]
fn plugin_executor_scopes_personal_lyrics_to_the_dispatch_principal() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;

    let (
        alice_db_id,
        alice_public_id,
        bob_db_id,
        bob_public_id,
        library_public_id,
        track_db_id,
        track_public_id,
    ) = futures::executor::block_on(async {
        let mut db = crate::services::STATE.db.write().await;
        let alice = lyra_db::fixtures::test_user("lyrics-alice")?;
        let alice_public_id = alice.id.clone();
        let alice_db_id = lyra_db::users::create(&mut db, &alice)?;
        let bob = lyra_db::fixtures::test_user("lyrics-bob")?;
        let bob_public_id = bob.id.clone();
        let bob_db_id = lyra_db::users::create(&mut db, &bob)?;
        let library_db_id =
            lyra_db::test_db::insert_library(&mut db, "Plugin Lyrics", "/tmp/plugin-lyrics")?;
        let library_public_id = lyra_db::lookup::find_id_by_db_id(&db, library_db_id)?
            .context("inserted library has public id")?;
        let track_db_id = lyra_db::fixtures::insert_track(&mut db, "Plugin Lyrics Track")?;
        let track_public_id = lyra_db::lookup::find_id_by_db_id(&db, track_db_id)?
            .context("inserted track has public id")?;
        lyra_db::fixtures::connect(&mut db, library_db_id, track_db_id)?;
        lyra_db::providers::upsert(
            &mut db,
            &lyra_db::ProviderConfig {
                db_id: None,
                provider_id: "test_provider".to_string(),
                position: 0,
                enabled: true,
            },
        )?;
        lyra_db::lyrics::upsert_from_plugin(
            &mut db,
            track_db_id,
            lyra_db::lyrics::LyricsInput {
                language: "eng".to_string(),
                plain_text: "provider lyrics".to_string(),
                lines: Vec::new(),
                last_checked_at: 1,
            },
            "remote-one".to_string(),
            "test_provider",
            None,
        )?;
        Ok::<_, anyhow::Error>((
            alice_db_id,
            alice_public_id,
            bob_db_id,
            bob_public_id,
            library_public_id,
            track_db_id,
            track_public_id,
        ))
    })?;

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.lyrics"])]),
        default_server_info(),
        crate::services::STATE.db.get(),
    )?;
    let principal = |user_db_id, user_public_id: String, can_access: bool| {
        crate::services::auth::Principal::from_parts(
            user_db_id,
            user_public_id,
            "lyrics-user".to_string(),
            Vec::new(),
            None,
            if can_access {
                std::collections::HashSet::from([library_public_id.clone()])
            } else {
                Default::default()
            },
        )
    };

    let mut alice_context = CallContext {
        origin: plugin_origin("demo", "alice.luau"),
        ..CallContext::default()
    };
    seed_caller_principal(
        &mut alice_context,
        principal(alice_db_id, alice_public_id.clone(), true),
    );
    let alice_values = runtime.eval_plugin_source_with_call_context(
        format!(
            r#"
                local lyrics = require("@lyra/lyrics")
                local track_id = {track_id}
                local provider = lyrics.get(track_id, nil, false, "test_provider")
                local personal = lyrics.upsert_personal(track_id, {track_public_id:?}, {{
                    content_type = "text/plain",
                    body = "alice lyrics",
                    language = "eng",
                }})
                local automatic = lyrics.get(track_id, nil, false, nil)
                local selected_provider = lyrics.get(track_id, nil, false, "test_provider")
                return provider.plain_text,
                    provider.provider_id,
                    provider.scope,
                    provider.source,
                    personal.plain_text,
                    personal.scope,
                    personal.source,
                    automatic.plain_text,
                    selected_provider.plain_text
            "#,
            track_id = track_db_id.0,
            track_public_id = track_public_id,
        )
        .into_bytes(),
        alice_context,
    )?;
    assert_eq!(
        alice_values,
        vec![
            luau::Value::String(b"provider lyrics".to_vec()),
            luau::Value::String(b"test_provider".to_vec()),
            luau::Value::String(b"shared".to_vec()),
            luau::Value::String(b"provider".to_vec()),
            luau::Value::String(b"alice lyrics".to_vec()),
            luau::Value::String(b"personal".to_vec()),
            luau::Value::String(b"manual".to_vec()),
            luau::Value::String(b"alice lyrics".to_vec()),
            luau::Value::String(b"provider lyrics".to_vec()),
        ]
    );

    let mut bob_context = CallContext {
        origin: plugin_origin("demo", "bob.luau"),
        ..CallContext::default()
    };
    seed_caller_principal(
        &mut bob_context,
        principal(bob_db_id, bob_public_id.clone(), true),
    );
    let bob_values = runtime.eval_plugin_source_with_call_context(
        format!(
            r#"
                local lyrics = require("@lyra/lyrics")
                local track_id = {track_id}
                local before = lyrics.get(track_id, nil, false, nil)
                lyrics.upsert_personal(track_id, {track_public_id:?}, {{
                    content_type = "text/plain",
                    body = "bob lyrics",
                    language = "eng",
                }})
                local after = lyrics.get(track_id, nil, false, nil)
                return before.plain_text, after.plain_text
            "#,
            track_id = track_db_id.0,
            track_public_id = track_public_id,
        )
        .into_bytes(),
        bob_context,
    )?;
    assert_eq!(
        bob_values,
        vec![
            luau::Value::String(b"provider lyrics".to_vec()),
            luau::Value::String(b"bob lyrics".to_vec()),
        ]
    );

    let mut inaccessible_alice_context = CallContext {
        origin: plugin_origin("demo", "alice-delete.luau"),
        ..CallContext::default()
    };
    seed_caller_principal(
        &mut inaccessible_alice_context,
        principal(alice_db_id, alice_public_id.clone(), false),
    );
    let deleted = runtime.eval_plugin_source_with_call_context(
        format!(
            r#"
                local lyrics = require("@lyra/lyrics")
                return lyrics.delete_personal_for_track({track_id})
            "#,
            track_id = track_db_id.0,
        )
        .into_bytes(),
        inaccessible_alice_context,
    )?;
    assert_eq!(deleted, vec![luau::Value::Boolean(true)]);

    let (alice_remaining, bob_remaining) = futures::executor::block_on(async {
        let db = crate::services::STATE.db.read().await;
        Ok::<_, anyhow::Error>((
            lyra_db::lyrics::find_personal(&db, track_db_id, alice_db_id)?,
            lyra_db::lyrics::find_personal(&db, track_db_id, bob_db_id)?,
        ))
    })?;
    assert!(alice_remaining.is_none());
    assert_eq!(
        bob_remaining.map(|lyrics| lyrics.plain_text).as_deref(),
        Some("bob lyrics")
    );
    Ok(())
}

#[test]
fn plugin_executor_declares_metadata_provider_ids_and_options() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let runtime = runtime_with_scopes(&["lyra.metadata"])?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local metadata = require("@lyra/metadata")
            local provider = metadata.Provider.new("raw-provider")
            provider:id({
                id_type = "release_id",
                entity = metadata.EntityType.Release,
                unique = true,
                scheme = "example:release",
            }, "https://example.test/release/{id}")
            provider:declare_option({
                name = "force",
                label = "Force refresh",
                type = "boolean",
                default = true,
                requires_settings = { "token" },
            })

            return metadata.EntityType.Release
        "#[..],
    )?;

    assert_eq!(values.len(), 1);
    let entity = crate::plugins::types::LuauEntityType::_harmony_userdata_class().read_value(
        &runtime.vm,
        "entity",
        values[0].clone(),
    )?;
    assert_eq!(
        crate::services::EntityType::from(entity),
        crate::services::EntityType::Release
    );

    let registry =
        futures::executor::block_on(crate::services::providers::provider_registry().read_owned());
    let (id_spec, has_generator) = crate::services::providers::registry_tests::id_registration(
        &registry,
        "raw-provider",
        crate::services::EntityType::Release,
        "release_id",
    )
    .context("provider id registration")?;
    assert_eq!(id_spec.id_type, "release_id");
    assert_eq!(id_spec.entity, crate::services::EntityType::Release);
    assert!(id_spec.unique);
    assert_eq!(id_spec.scheme.as_deref(), Some("example:release"));
    assert!(has_generator);
    assert_eq!(
        crate::services::providers::registry_tests::id_url_template(
            &registry,
            "raw-provider",
            crate::services::EntityType::Release,
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
fn plugin_executor_registers_similar_releases_handler() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let runtime = runtime_with_scopes(&["lyra.metadata"])?;
    runtime.eval_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local metadata = require("@lyra/metadata")
            local provider = metadata.Provider.new("similar-provider")
            provider:similar_releases({
                timeout_ms = 4200,
                require = {
                    all_of = { "external_ids.musicbrainz.release_group_id" },
                    any_of = { "genres", "artist_names" },
                },
            }, function(_ctx)
                return { candidates = {} }
            end)

            local default_provider = metadata.Provider.new("default-similar-provider")
            default_provider:similar_releases({}, function(_ctx)
                return nil
            end)
        "#[..],
    )?;

    let registry =
        futures::executor::block_on(crate::services::providers::provider_registry().read_owned());
    let spec = registry
        .get_similar_releases_handler("similar-provider")
        .context("similar releases handler")?;
    assert_eq!(spec.timeout, std::time::Duration::from_millis(4200));
    assert_eq!(
        spec.require.all_of,
        ["external_ids.musicbrainz.release_group_id"]
    );
    assert_eq!(spec.require.any_of, ["genres", "artist_names"]);
    assert!(spec.handler.handler_id > 0);

    let default_spec = registry
        .get_similar_releases_handler("default-similar-provider")
        .context("default similar releases handler")?;
    assert_eq!(default_spec.timeout, std::time::Duration::from_secs(10));
    assert!(default_spec.require.all_of.is_empty());
    assert!(default_spec.require.any_of.is_empty());
    Ok(())
}

#[test]
fn provider_id_keeps_one_generator_handler_per_registration() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let runtime = runtime_with_scopes(&["lyra.metadata"])?;
    let handlers = runtime
        .vm
        .data()
        .get::<crate::plugins::metadata::MetadataCallbackRegistry>()?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local metadata = require("@lyra/metadata")
            local ET = metadata.EntityType
            provider = metadata.Provider.new("replaced-generator-provider")
            provider:id({ id_type = "thing_id", entity = ET.Release, scheme = "example:thing" }, function()
                return nil
            end)
        "#[..],
    )?;
    assert_eq!(handlers.len(), 1);

    runtime.run_plugin_source(
        "demo",
        "replace.luau",
        &br#"
            local metadata = require("@lyra/metadata")
            provider:id({ id_type = "thing_id", entity = metadata.EntityType.Release, scheme = "example:thing" }, function()
                return nil
            end)
        "#[..],
    )?;
    assert_eq!(handlers.len(), 1, "the replaced handler is removed");

    let error = runtime
        .run_plugin_source(
            "demo",
            "conflict.luau",
            &br#"
                local metadata = require("@lyra/metadata")
                provider:id({ id_type = "thing_id", entity = metadata.EntityType.Artist, scheme = "example:other" }, function()
                    return nil
                end)
            "#[..],
        )
        .expect_err("conflicting scheme must be rejected");
    assert!(error.to_string().contains("same scheme"), "{error:#}");
    assert_eq!(
        handlers.len(),
        1,
        "a rejected registration keeps no handler"
    );
    Ok(())
}

#[test]
fn plugin_executor_rejects_invalid_id_scheme() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let runtime = runtime_with_scopes(&["lyra.metadata"])?;
    let error = runtime
        .eval_plugin_source(
            "demo",
            "init.luau",
            &br#"
                local metadata = require("@lyra/metadata")
                local provider = metadata.Provider.new("invalid-scheme-provider")
                provider:id({
                    id_type = "thing_id",
                    entity = metadata.EntityType.Release,
                    scheme = "Example:Thing",
                })
            "#[..],
        )
        .expect_err("uppercase scheme must be rejected");

    assert!(
        error
            .to_string()
            .contains("provider:id: scheme 'Example:Thing' must be lowercase")
    );
    Ok(())
}

#[test]
fn plugin_executor_rejects_invalid_similar_releases_config() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let runtime = runtime_with_scopes(&["lyra.metadata"])?;
    let error = runtime
        .eval_plugin_source(
            "demo",
            "init.luau",
            &br#"
                local metadata = require("@lyra/metadata")
                local provider = metadata.Provider.new("invalid-similar-provider")
                provider:similar_releases({ timeout_ms = 0 }, function(_ctx)
                    return { candidates = {} }
                end)
            "#[..],
        )
        .expect_err("zero timeout must be rejected");

    assert!(
        error
            .to_string()
            .contains("provider:similar_releases config.timeout_ms must be an integer >= 1")
    );

    let error = runtime
        .eval_plugin_source(
            "demo",
            "too-long.luau",
            &br#"
                local metadata = require("@lyra/metadata")
                local provider = metadata.Provider.new("too-long-similar-provider")
                provider:similar_releases({ timeout_ms = 10001 }, function(_ctx)
                    return { candidates = {} }
                end)
            "#[..],
        )
        .expect_err("timeout above the maximum must be rejected");
    assert!(
        error
            .to_string()
            .contains("provider:similar_releases config.timeout_ms must be <= 10000")
    );
    Ok(())
}

#[test]
fn plugin_executor_bounds_similar_release_result_before_json_conversion() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let runtime = runtime_with_scopes(&["lyra.metadata"])?;
    runtime.eval_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local metadata = require("@lyra/metadata")
            local provider = metadata.Provider.new("bounded-similar-provider")
            provider:similar_releases({}, function(_ctx)
                local candidates = {}
                for index = 1, 1000 do
                    candidates[index] = {
                        external_id = {
                            provider_id = "bounded-similar-provider",
                            id_type = "release_group_id",
                            id_value = tostring(index),
                        },
                    }
                end
                return { candidates = candidates }
            end)
        "#[..],
    )?;
    let handler = {
        let registry = futures::executor::block_on(
            crate::services::providers::provider_registry().read_owned(),
        );
        registry
            .get_similar_releases_handler("bounded-similar-provider")
            .context("similar releases handler")?
            .handler
    };

    let result = runtime.dispatch_similar_releases(SimilarReleasesDispatchRequest {
        provider_id: "bounded-similar-provider".to_string(),
        handler,
        context: serde_json::json!({}),
        timeout: std::time::Duration::from_secs(1),
        cancellation: DispatchCancellation::default(),
        max_candidates: 3,
    })?;

    assert_eq!(result.candidates.len(), 3);
    Ok(())
}

#[test]
fn releases_similar_dispatches_provider_on_current_executor() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let (seed, candidate, candidate_public_id) = futures::executor::block_on(async {
        let mut db = crate::services::STATE.db.write().await;
        let seed = lyra_db::fixtures::insert_release(&mut db, "Seed")?;
        let candidate = lyra_db::fixtures::insert_release(&mut db, "Candidate")?;
        let public_id = lyra_db::releases::get_by_id(&db, candidate)?
            .context("candidate release")?
            .id;
        Ok::<_, anyhow::Error>((seed, candidate, public_id))
    })?;
    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.metadata", "lyra.releases"])]),
        default_server_info(),
        crate::services::STATE.db.get(),
    )?;
    let source = format!(
        r#"
            local metadata = require("@lyra/metadata")
            local releases = require("@lyra/releases")
            local provider = metadata.Provider.new("in-vm-similar-provider")
            provider:similar_releases({{}}, function()
                return {{ candidates = {{{{
                    release_db_id = {},
                    release_id = {:?},
                }}}} }}
            end)
            local matches = releases.similar({})
            return matches[1].id
        "#,
        candidate.0, candidate_public_id, seed.0
    );

    let values = runtime.eval_plugin_source_with_call_context(
        source.into_bytes(),
        admin_call_context("init.luau")?,
    )?;

    assert_eq!(
        values,
        vec![luau::Value::String(candidate_public_id.into_bytes())]
    );
    Ok(())
}

#[test]
fn releases_similar_requires_a_dispatch_principal() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let seed = futures::executor::block_on(async {
        let mut db = crate::services::STATE.db.write().await;
        lyra_db::fixtures::insert_release(&mut db, "Seed")
    })?;
    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.releases"])]),
        default_server_info(),
        crate::services::STATE.db.get(),
    )?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local releases = require("@lyra/releases")
                local ok, err = pcall(releases.similar, {})
                return ok, string.find(tostring(err), "no authenticated caller", 1, true) ~= nil
            "#,
            seed.0
        )
        .as_bytes(),
    )?;

    assert_eq!(
        values,
        vec![luau::Value::Boolean(false), luau::Value::Boolean(true)]
    );
    Ok(())
}

#[test]
fn awaited_background_similar_provider_result_survives_executor_polling() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let (seed, candidate, candidate_public_id) = futures::executor::block_on(async {
        let mut db = crate::services::STATE.db.write().await;
        let seed = lyra_db::fixtures::insert_release(&mut db, "Seed")?;
        let candidate = lyra_db::fixtures::insert_release(&mut db, "Candidate")?;
        let public_id = lyra_db::releases::get_by_id(&db, candidate)?
            .context("candidate release")?
            .id;
        Ok::<_, anyhow::Error>((seed, candidate, public_id))
    })?;
    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest(
            "demo",
            &["harmony.task", "lyra.metadata", "lyra.releases"],
        )]),
        default_server_info(),
        crate::services::STATE.db.get(),
    )?;
    let source = format!(
        r#"
            local task = require("@harmony/task")
            local metadata = require("@lyra/metadata")
            local releases = require("@lyra/releases")
            local provider = metadata.Provider.new("background-similar-provider")
            provider:similar_releases({{}}, function()
                task.wait(0)
                return {{ candidates = {{{{
                    release_db_id = {},
                    release_id = {:?},
                }}}} }}
            end)
            similar_done = false
            similar_result_id = nil
            task.spawn(function()
                local matches = releases.similar({})
                similar_result_id = matches[1].id
                similar_done = true
            end)
        "#,
        candidate.0, candidate_public_id, seed.0
    );
    runtime.eval_plugin_source_with_call_context(
        source.into_bytes(),
        admin_call_context("init.luau")?,
    )?;

    for _ in 0..50 {
        runtime.poll_background_tasks();
        runtime.poll_background_tasks();
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        &b"return similar_done, similar_result_id"[..],
    )?;

    assert_eq!(values[0], luau::Value::Boolean(true));
    assert_eq!(
        values[1],
        luau::Value::String(candidate_public_id.into_bytes())
    );
    Ok(())
}

#[test]
fn cancelling_awaited_similar_release_call_keeps_executor_usable() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let seed = futures::executor::block_on(async {
        let mut db = crate::services::STATE.db.write().await;
        lyra_db::fixtures::insert_release(&mut db, "Seed")
    })?;
    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest(
            "demo",
            &["harmony.task", "lyra.metadata", "lyra.releases"],
        )]),
        default_server_info(),
        crate::services::STATE.db.get(),
    )?;
    let source = format!(
        r#"
            local task = require("@harmony/task")
            local metadata = require("@lyra/metadata")
            local releases = require("@lyra/releases")
            local provider = metadata.Provider.new("cancelled-similar-provider")
            provider_started = false
            provider_release = false
            provider:similar_releases({{}}, function()
                provider_started = true
                while not provider_release do
                    task.wait(0.01)
                end
                return {{ candidates = {{}} }}
            end)
            similar_resumed = false
            similar_thread = task.spawn(function()
                releases.similar({})
                similar_resumed = true
            end)
        "#,
        seed.0
    );
    runtime.eval_plugin_source_with_call_context(
        source.into_bytes(),
        admin_call_context("init.luau")?,
    )?;
    let mut provider_started = false;
    for _ in 0..10 {
        runtime.poll_background_tasks();
        let values = runtime.eval_plugin_source(
            "demo",
            "started.luau",
            &b"return provider_started, similar_resumed"[..],
        )?;
        provider_started = values[0] == luau::Value::Boolean(true);
        assert_eq!(values[1], luau::Value::Boolean(false));
        if provider_started {
            break;
        }
    }
    assert!(provider_started, "similar releases provider did not start");

    runtime.eval_plugin_source(
        "demo",
        "cancel.luau",
        &br#"
            local task = require("@harmony/task")
            task.cancel(similar_thread)
        "#[..],
    )?;
    runtime.poll_background_tasks();
    let values =
        runtime.eval_plugin_source("demo", "check.luau", &b"return similar_resumed, 42"[..])?;

    assert_eq!(
        values,
        vec![luau::Value::Boolean(false), luau::Value::Number(42.0)]
    );
    Ok(())
}

#[test]
fn plugin_executor_reads_live_settings_through_injected_handle() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let mut stores = stores::PluginModuleStores::empty();
    stores.server_settings = Some(crate::services::STATE.settings.clone());
    let runtime = PluginExecutor::with_loader(
        Arc::from(vec![manifest("demo", &["lyra.server", "lyra.auth"])]),
        default_server_info(),
        default_auth_capabilities(),
        stores,
        MemorySourceLoader::new(),
    )?;
    let source = &b"return require('@lyra/server').info().published_url, require('@lyra/auth').capabilities().enabled"[..];
    let before = runtime.eval_plugin_source("demo", "check.luau", source)?;
    let mut config = (*crate::services::STATE.config()).clone();
    config.published_url = Some("https://updated.example".to_string());
    config.auth.enabled = !config.auth.enabled;
    let enabled = config.auth.enabled;
    crate::services::testing::publish_config(config);
    let after = runtime.eval_plugin_source("demo", "check.luau", source)?;
    assert_ne!(before, after);
    assert_eq!(
        after,
        vec![
            luau::Value::String(b"https://updated.example".to_vec()),
            luau::Value::Boolean(enabled)
        ]
    );
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
            setup: crate::services::setup::Status {
                account_required: false,
                plugin_selection_required: true,
            },
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
            executor_server_account_required = info.setup.account_required
            executor_server_plugin_selection_required = info.setup.plugin_selection_required
        "#[..],
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        &b"return executor_server_id, executor_server_port, executor_server_url, executor_server_account_required, executor_server_plugin_selection_required"[..],
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::String(b"server-1".to_vec()),
            luau::Value::Number(3210.0),
            luau::Value::String(b"https://lyra.example".to_vec()),
            luau::Value::Boolean(false),
            luau::Value::Boolean(true),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_lyra_playback_sessions_on_update() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
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
            state: lyra_db::PlaybackState::Playing,
            playback_session_public_id: "playback-public".to_string(),
            track_public_id: "track-public".to_string(),
            user_public_id: "user-public".to_string(),
            library_public_id: None,
            position_ms: 42,
            duration_ms: Some(100),
            played_ms: 42,
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
fn plugin_executor_exposes_lyra_listens_on_update() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let runtime = runtime_with_scopes(&["lyra.listens", "harmony.task"])?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local task = require("@harmony/task")
            local listens = require("@lyra/listens")

            executor_listen_updates = {}
            listens.on_update(function(update)
                task.wait()
                for _, listen in update.listens do
                    table.insert(
                        executor_listen_updates,
                        `{update.event}:{update.user_id}:{listen.listen_id}:{listen.track_id}:{listen.started_at_ms}:{listen.played_ms}`
                    )
                end
            end)
        "#[..],
    )?;

    let listen = |listen_id: &str| crate::services::listens::ListenPayload {
        listen_id: listen_id.to_string(),
        track_id: "track-public".to_string(),
        started_at_ms: 1_000,
        played_ms: 42_000,
    };
    runtime.dispatch_listen_update(crate::services::listens::ListenUpdatePayload {
        event: "recorded".to_string(),
        user_id: "user-public".to_string(),
        listens: vec![listen("listen-a"), listen("listen-b")],
    })?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        &b"return table.concat(executor_listen_updates, ',')"[..],
    )?;
    assert_eq!(
        values,
        vec![luau::Value::String(
            b"recorded:user-public:listen-a:track-public:1000:42000,recorded:user-public:listen-b:track-public:1000:42000".to_vec()
        )]
    );
    Ok(())
}

#[test]
fn plugin_executor_rejects_handles_from_another_runtime() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    init_api_test_state()?;
    struct Handlers {
        refresh: crate::services::plugin_dispatch::PluginCallbackHandle,
        route: crate::services::plugin_dispatch::PluginCallbackHandle,
        websocket: crate::services::plugin_dispatch::PluginCallbackHandle,
        mix: crate::services::plugin_dispatch::PluginCallbackHandle,
        id_link: crate::services::plugin_dispatch::PluginCallbackHandle,
    }
    let register = |name: &str| -> Result<_> {
        let runtime = PluginExecutor::with_database(
            Arc::from(vec![manifest(
                "demo",
                &["lyra.metadata", "lyra.api", "lyra.mix"],
            )]),
            default_server_info(),
            crate::services::STATE.db.get(),
        )?;
        runtime.run_plugin_source(
            "demo",
            "init.luau",
            format!(
                r#"
                    local metadata = require("@lyra/metadata")
                    local api = require("@lyra/api")
                    local mix = require("@lyra/mix")
                    local provider = metadata.Provider.new("{name}")
                    provider:refresh(metadata.EntityType.Release, function()
                        return "{name}"
                    end)
                    provider:id({{ id_type = "thing_id", entity = metadata.EntityType.Release }}, function()
                        return "https://example.com/{name}"
                    end)
                    api.get("/{name}", function()
                        return api.response.json(200, {{}})
                    end, "public")
                    api.websocket("/{name}/socket", function(_reader, sender)
                        sender:close()
                    end, "public")
                    mix.Mixer.new("{name}"):from_track(function()
                        return {{ tracks = {{}} }}
                    end)
                "#
            )
            .into_bytes(),
        )?;
        let (refresh, id_link) = futures::executor::block_on(async {
            let registry = crate::services::providers::provider_registry()
                .read_owned()
                .await;
            let refresh = registry
                .get_refresh_callback(name, crate::services::EntityType::Release)
                .copied();
            let id_link = registry
                .id_link_generators()
                .get(name, crate::services::EntityType::Release, "thing_id")
                .and_then(|generator| match generator.generator {
                    crate::services::providers::ProviderIdUrlGenerator::Function(handler) => {
                        Some(handler)
                    }
                    crate::services::providers::ProviderIdUrlGenerator::Template(_) => None,
                });
            (refresh, id_link)
        });
        let handlers = Handlers {
            refresh: refresh.context("registered refresh callback")?,
            route: futures::executor::block_on(crate::plugins::api::tests::registered_handler(
                "GET",
                &format!("/{name}"),
            ))
            .context("registered API handler")?,
            websocket: futures::executor::block_on(crate::plugins::api::tests::registered_handler(
                "GET",
                &format!("/{name}/socket"),
            ))
            .context("registered websocket handler")?,
            mix: futures::executor::block_on(async {
                crate::services::mix::mix_registry()
                    .read_owned()
                    .await
                    .get_seed_callback(name, crate::services::mix::MixSeedType::Track)
            })
            .context("registered mix callback")?,
            id_link: id_link.context("registered id link generator")?,
        };
        Ok((runtime, handlers))
    };
    let (_stale_runtime, stale) = register("stale-a")?;
    let (runtime, own) = register("stale-b")?;
    assert_eq!(stale.refresh.handler_id, own.refresh.handler_id);
    let assert_stale = |error: anyhow::Error| {
        assert!(
            format!("{error:#}").contains("belongs to plugin runtime"),
            "{error:#}"
        );
    };
    let refresh_request = |handler| MetadataRefreshRequest {
        handler,
        context: serde_json::json!({}),
        deadline: std::time::Instant::now() + Duration::from_secs(5),
    };

    let result = runtime.dispatch_metadata_refresh(refresh_request(own.refresh))?;
    assert_eq!(result.values, vec![serde_json::json!("stale-b")]);
    assert_stale(
        runtime
            .dispatch_metadata_refresh(refresh_request(stale.refresh))
            .expect_err("a provider handle from another runtime must be rejected"),
    );
    assert_stale(
        runtime
            .dispatch_api_handler(api_request(stale.route, "/stale-a"))
            .expect_err("an API route handle from another runtime must be rejected"),
    );
    let (websocket, _inbound_tx, _outbound_rx) =
        websocket_request(stale.websocket, "/stale-a/socket");
    assert_stale(
        runtime
            .start_websocket(websocket)
            .expect_err("a websocket handle from another runtime must be rejected"),
    );
    assert_stale(
        runtime
            .dispatch_mix_handler(MixHandlerRequest {
                handler: stale.mix,
                seed_id: Some(1),
                limit: Some(1),
                recent_track_ids: Vec::new(),
                options: serde_json::Map::new(),
            })
            .expect_err("a mix handle from another runtime must be rejected"),
    );
    let id_link_call = |handler| crate::services::providers::IdLinkCall {
        handler,
        entity: crate::services::EntityType::Release,
        id_type: "thing_id".to_string(),
        id: "1".to_string(),
        library: Default::default(),
        external_ids: Default::default(),
    };
    let tokio_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let results = tokio_runtime.block_on(super::dispatch_id_links_in_vm(
        runtime.vm.clone(),
        "stale-b".to_string(),
        vec![id_link_call(stale.id_link)],
    ))?;
    match &results[0] {
        Err(crate::services::providers::IdLinkCallError::Failed(error)) => {
            assert!(error.contains("belongs to plugin runtime"), "{error}");
        }
        other => panic!("a stale id link handle must fail, got {other:?}"),
    }
    Ok(())
}

#[test]
fn plugin_executor_dispatches_registered_api_handler() -> Result<()> {
    // api::install resets the shared API route registry; serialize with the
    // other global-registry mutators.
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    init_api_test_state()?;
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

    let handler = futures::executor::block_on(crate::plugins::api::tests::registered_handler(
        "POST",
        "/demo/{id}",
    ))
    .context("registered API handler")?;
    let result = runtime.dispatch_api_handler(ApiHandlerRequest {
        method: "POST".to_string(),
        headers: vec![("content-type".to_string(), "application/json".to_string())],
        query: HashMap::from([("flag".to_string(), vec!["yes".to_string()])]),
        params: HashMap::from([("id".to_string(), "abc".to_string())]),
        body: br#"{"name":"raw"}"#.to_vec(),
        ..api_request(handler, "/demo/abc")
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

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn plugin_executor_binds_host_resolved_principal_to_api_responses() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let test_dir = std::env::temp_dir().join(format!(
        "lyra-dispatch-auth-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    std::fs::create_dir_all(&test_dir)?;
    crate::plugins::testing::init_library_test_state(test_dir.clone()).await?;

    let user = lyra_db::fixtures::test_user("dispatch-auth-user")?;
    let user_db_id = {
        let mut db = crate::services::STATE.db.write().await;
        lyra_db::users::create(&mut db, &user)?
    };
    let token = crate::services::testing::create_session(user_db_id, Default::default())
        .await?
        .token;

    crate::plugins::api::initialize_registry(std::collections::HashSet::new(), None).await;
    let runtime = runtime_with_scopes(&["lyra.api", "lyra.auth"])?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local api = require("@lyra/api")
                local auth = require("@lyra/auth")

                api.get("/whoami", function(ctx)
                    local resolved = auth.resolve_auth("{token}")
                    return api.response.json(200, {{ resolved = resolved ~= nil }})
                end, "public")

                api.get("/anon", function(ctx)
                    return api.response.json(200, {{}})
                end, "public")
            "#
        )
        .into_bytes(),
    )?;

    let whoami_handler = crate::plugins::api::tests::registered_handler("GET", "/whoami")
        .await
        .context("registered /whoami handler")?;
    let response = runtime.dispatch_api_handler(api_request(whoami_handler, "/whoami"))?;
    let principal = response
        .principal
        .context("resolve_auth during dispatch should bind the principal to the response")?;
    assert_eq!(principal.user_public_id, user.id);

    let anon_handler = crate::plugins::api::tests::registered_handler("GET", "/anon")
        .await
        .context("registered /anon handler")?;
    let response = runtime.dispatch_api_handler(api_request(anon_handler, "/anon"))?;
    assert!(
        response.principal.is_none(),
        "dispatches that never resolve auth must not carry a principal"
    );

    let boundary_auth = crate::services::auth::resolve_auth_from_bearer(Some(&token))
        .await?
        .context("bearer token should resolve at the boundary")?;
    let mut seeded = api_request(anon_handler, "/anon");
    seeded.auth = Some(boundary_auth);
    let response = runtime.dispatch_api_handler(seeded)?;
    let principal = response
        .principal
        .context("boundary auth should seed the dispatch principal")?;
    assert_eq!(principal.user_public_id, user.id);

    let _ = std::fs::remove_dir_all(test_dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn plugin_executor_treats_expired_session_as_unauthenticated() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let mut config = crate::services::STATE.config().as_ref().clone();
    config.auth.enabled = true;
    crate::services::testing::publish_config(config);

    let (token, session_id) = {
        let user_id = {
            let mut db = crate::services::STATE.db.write().await;
            lyra_db::users::create(
                &mut db,
                &lyra_db::fixtures::test_user("expired-plugin-session")?,
            )?
        };
        let session = crate::services::testing::create_session(user_id, Default::default()).await?;
        let db = crate::services::STATE.db.read().await;
        let (_, _, session_id) = lyra_db::users::find_by_session_token_hash(
            &db,
            &crate::services::auth::hash_secret(&session.token),
        )?
        .context("created session")?;
        (session.token, session_id)
    };

    crate::plugins::api::initialize_registry(std::collections::HashSet::new(), None).await;
    let runtime = runtime_with_scopes(&["lyra.api", "lyra.auth"])?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local api = require("@lyra/api")
            local auth = require("@lyra/auth")

            api.get("/protected", function(ctx)
                local resolved = auth.resolve_auth(ctx.request.query.api_key[1])
                if resolved == nil then
                    return api.response.empty(401)
                end
                return api.response.empty(204)
            end, "public")
        "#[..],
    )?;
    let handler = crate::plugins::api::tests::registered_handler("GET", "/protected")
        .await
        .context("registered protected handler")?;
    let request = || ApiHandlerRequest {
        query: HashMap::from([("api_key".to_string(), vec![token.clone()])]),
        ..api_request(handler, "/protected")
    };
    let response = runtime.dispatch_api_handler(request())?;
    assert_eq!(response.status, 204);
    assert!(response.principal.is_some());

    crate::services::STATE.db.write().await.exec_mut(
        agdb::QueryBuilder::insert()
            .values_uniform([("expires_at", lyra_db::users::now_secs() - 1).into()])
            .ids(session_id)
            .query(),
    )?;
    let response = runtime.dispatch_api_handler(request())?;
    assert_eq!(response.status, 401);
    assert!(response.principal.is_none());
    {
        let db = crate::services::STATE.db.read().await;
        assert!(
            lyra_db::users::find_by_session_token_hash(
                &db,
                &crate::services::auth::hash_secret(&token),
            )?
            .is_none()
        );
    }
    Ok(())
}

#[test]
fn plugin_executor_dispatches_registered_websocket_handler() -> Result<()> {
    // api::install resets the shared API route registry; serialize with the
    // other global-registry mutators.
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    init_api_test_state()?;
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

    let handler = futures::executor::block_on(crate::plugins::api::tests::registered_handler(
        "GET",
        "/socket/{id}",
    ))
    .context("registered websocket handler")?;
    let (mut request, inbound_tx, mut outbound_rx) = websocket_request(handler, "/socket/abc");
    request.params = HashMap::from([("id".to_string(), "abc".to_string())]);
    inbound_tx.try_send("hello".to_string())?;

    runtime.start_websocket(request)?;

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
    // api::install resets the shared API route registry; serialize with the
    // other global-registry mutators.
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    init_api_test_state()?;
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

    let websocket_handler = futures::executor::block_on(
        crate::plugins::api::tests::registered_handler("GET", "/socket"),
    )
    .context("registered websocket handler")?;
    let api_handler = futures::executor::block_on(crate::plugins::api::tests::registered_handler(
        "GET", "/ping",
    ))
    .context("registered API handler")?;
    let (request, inbound_tx, _outbound_rx) = websocket_request(websocket_handler, "/socket");
    inbound_tx.try_send("done".to_string())?;

    runtime.start_websocket(request)?;
    assert_eq!(runtime.websocket_tasks.borrow().len(), 1);

    let response = runtime.dispatch_api_handler(api_request(api_handler, "/ping"))?;
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

async fn next_websocket_frame(
    runtime: &PluginExecutor,
    outbound: &mut tokio::sync::mpsc::Receiver<String>,
) -> Result<String> {
    for _ in 0..200 {
        runtime.poll_background_tasks();
        match outbound.try_recv() {
            Ok(text) => return Ok(text),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(error) => return Err(error).context("websocket outbound channel closed"),
        }
    }
    anyhow::bail!("no websocket frame arrived")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn websocket_spawned_work_acts_as_the_verified_socket_principal() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    crate::plugins::api::initialize_registry(std::collections::HashSet::new(), None).await;
    let user_db_id = {
        let mut db = crate::services::STATE.db.write().await;
        lyra_db::test_db::insert_user(&mut db, "socket-owner")?
    };
    let principal = crate::services::testing::user_principal(user_db_id).await?;
    let connection = crate::services::remote::registry::register(
        principal.user_public_id.clone(),
        None,
        "socket-device".to_string(),
        tokio::sync::watch::channel(None).0,
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error}"))?;

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest(
            "demo",
            &["harmony.task", "lyra.api", "lyra.playback_sessions"],
        )]),
        default_server_info(),
        crate::services::STATE.db.get(),
    )?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local api = require("@lyra/api")
            local playback_sessions = require("@lyra/playback_sessions")
            local task = require("@harmony/task")

            api.websocket("/socket", function(reader, sender)
                while reader:recv() ~= nil do
                    task.spawn(function()
                        local ok, connections = pcall(playback_sessions.list_connections)
                        sender:send(if ok then `connections:{#connections}` else "denied")
                    end)
                end
            end, "public")
        "#[..],
    )?;
    let handler = crate::plugins::api::tests::registered_handler("GET", "/socket")
        .await
        .context("registered websocket handler")?;
    let (mut request, inbound_tx, mut outbound_rx) = websocket_request(handler, "/socket");
    request.auth = Some(crate::services::auth::ResolvedAuth {
        principal: principal.clone(),
        credential: crate::services::auth::AuthCredential::Default,
        client_name: None,
    });
    request.dispatch_auth.record(principal);
    let state = request.state.clone();
    runtime.start_websocket(request)?;

    inbound_tx.send("probe".to_string()).await?;
    let allowed = next_websocket_frame(&runtime, &mut outbound_rx).await;
    {
        let mut db = crate::services::STATE.db.write().await;
        lyra_db::users::delete_user(&mut db, user_db_id)?;
    }
    inbound_tx.send("probe".to_string()).await?;
    let stale = next_websocket_frame(&runtime, &mut outbound_rx).await;
    state.request_close();
    crate::services::remote::registry::unregister(connection.connection_id).await;

    assert_eq!(allowed?, "connections:1");
    assert_eq!(stale?, "denied");
    Ok(())
}

#[test]
fn plugin_executor_dispatches_registered_mix_handler() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.mix", "harmony.task"])]),
        default_server_info(),
        crate::services::STATE.db.get(),
    )?;
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

    let handler = futures::executor::block_on(async {
        crate::services::mix::mix_registry()
            .read_owned()
            .await
            .get_seed_callback("demo-mixer", crate::services::mix::MixSeedType::Track)
    })
    .context("registered mix callback")?;
    let result = runtime.dispatch_mix_handler(MixHandlerRequest {
        handler,
        seed_id: Some(40),
        limit: Some(10),
        recent_track_ids: Vec::new(),
        options: serde_json::Map::from_iter([("boost".to_string(), serde_json::Value::Bool(true))]),
    })?;

    assert_eq!(result.track_ids, vec![42]);
    Ok(())
}

#[path = "tests/catalog_queries.rs"]
mod catalog_queries;
#[path = "tests/db_contention.rs"]
mod db_contention;
#[path = "tests/db_modules.rs"]
mod db_modules;

#[test]
fn plugin_executor_exposes_lyra_locale_module() -> Result<()> {
    let runtime = runtime_with_scopes(&["lyra.locale"])?;
    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        &br#"
            local locale = require("@lyra/locale")
            return type(locale.language("en"))
        "#[..],
    )?;
    assert_eq!(values, vec![luau::Value::String(b"table".to_vec())]);
    Ok(())
}

#[test]
fn plugin_executor_exposes_lyra_editions_module() -> Result<()> {
    let runtime = runtime_with_scopes(&["lyra.editions"])?;
    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        &br#"
            local editions = require("@lyra/editions")
            return editions.media_formats("WEB")[1], editions.barcode("0199957588430")
        "#[..],
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::String(b"digital".to_vec()),
            luau::Value::String(b"00199957588430".to_vec()),
        ]
    );
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
    let plugin = temp_plugin(
        "demo",
        &["harmony.serde"],
        r#"
            local serde = require("@harmony/serde")
            local json = serde.json
            executor_discovered_output = json.encode({ answer = 42 })
        "#,
    )?;

    let (runtime, errors) =
        PluginExecutor::discover_from_plugins_dir(plugin.plugins_dir(), default_server_info())?;
    assert!(errors.is_empty(), "{errors:?}");
    assert!(runtime.has_plugin("demo"));
    assert_eq!(runtime.plugin_manifests()[0].id, "demo");

    assert!(runtime.exec_all().is_empty());
    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        &b"return executor_discovered_output"[..],
    )?;
    assert_eq!(
        values,
        vec![luau::Value::String(br#"{"answer":42}"#.to_vec())]
    );
    Ok(())
}

#[test]
fn plugin_executor_executes_checked_in_plugins_from_repo() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(lyra_db::test_db::new_test_db()?));
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
    let plugin = temp_plugin(
        "demo",
        &["harmony.serde"],
        r#"
            local serde = require("@harmony/serde")
            local json = serde.json
            executor_handle_output = json.encode({ answer = 42 })
        "#,
    )?;

    let db = lyra_db::test_db::new_test_db()?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));
    let (runtime, errors) = PluginExecutorHandle::discover_from_plugins_dir_with_db_and_modules(
        plugin.plugins_dir(),
        default_server_info(),
        default_auth_capabilities(),
        db,
        Vec::new(),
        None,
    )?;
    assert!(errors.is_empty(), "{errors:?}");
    assert!(futures::executor::block_on(runtime.has_plugin("demo"))?);
    assert_eq!(
        futures::executor::block_on(runtime.plugin_manifests())?[0].id,
        "demo"
    );

    futures::executor::block_on(runtime.exec_all())?;
    futures::executor::block_on(runtime.exec_plugin("demo"))?;
    Ok(())
}

#[test]
fn plugin_executor_restart_reloads_plugin_sources_from_disk() -> Result<()> {
    let init = r#"
        local task = require("@harmony/task")
        local answer = require('./lib').answer
        restart_reload_output = answer
        task.delay(0.05, function()
            delayed_runs = (delayed_runs or "") .. answer
        end)
    "#;
    let plugin = temp_plugin("demo", &["harmony.task"], init)?;
    let write_lib =
        |answer: &str| plugin.write("lib.luau", format!("return {{ answer = {answer} }}"));
    write_lib("1")?;

    let (runtime, errors) =
        PluginExecutor::discover_from_plugins_dir(plugin.plugins_dir(), default_server_info())?;
    assert!(errors.is_empty(), "{errors:?}");
    let read = |global: &'static str| {
        runtime.eval_plugin_source(
            "demo",
            "check.luau",
            format!("return {global}").into_bytes(),
        )
    };

    runtime.exec_plugin("demo")?;
    assert_eq!(
        read("restart_reload_output")?,
        vec![luau::Value::Number(1.0)]
    );

    write_lib("2")?;
    runtime.verify_plugin_manifest("demo")?;
    runtime.restart_plugin("demo")?;
    assert_eq!(
        read("restart_reload_output")?,
        vec![luau::Value::Number(2.0)]
    );

    let deadline = std::time::Instant::now() + Duration::from_millis(200);
    while std::time::Instant::now() < deadline {
        runtime.poll_background_tasks();
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        read("delayed_runs")?,
        vec![luau::Value::String(b"2".to_vec())],
        "the previous run's delayed task never fires"
    );
    Ok(())
}

#[test]
fn plugin_executor_restart_keeps_one_registration_of_each_update_handler() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let test_dir = std::env::temp_dir().join(format!(
        "lyra-restart-handlers-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    let plugin_dir = test_dir.join("plugins").join("demo");
    std::fs::create_dir_all(&plugin_dir)?;
    std::fs::write(
        plugin_dir.join("plugin.json"),
        serde_json::json!({
            "schema_version": 1,
            "id": "demo",
            "name": "Demo",
            "version": "1.0.0",
            "lyra": crate::plugins::testing::HOST_REQUIREMENT,
            "description": "Demo plugin",
            "entrypoint": "init.luau",
            "scopes": ["lyra.listens", "lyra.playback_sessions"]
        })
        .to_string(),
    )?;
    std::fs::write(
        plugin_dir.join("init.luau"),
        r#"
            local listens = require("@lyra/listens")
            local playbacks = require("@lyra/playback_sessions")
            listen_calls = 0
            playback_calls = 0
            listens.on_update(function() listen_calls += 1 end)
            playbacks.on_update(function() playback_calls += 1 end)
        "#,
    )?;
    let (runtime, errors) =
        PluginExecutor::discover_from_plugins_dir(test_dir.join("plugins"), default_server_info())?;
    assert!(errors.is_empty(), "{errors:?}");

    runtime.exec_plugin("demo")?;
    runtime.restart_plugin("demo")?;
    runtime.restart_plugin("demo")?;
    runtime.dispatch_listen_update(crate::services::listens::ListenUpdatePayload {
        event: "recorded".to_string(),
        user_id: "user-public".to_string(),
        listens: Vec::new(),
    })?;
    runtime.dispatch_playback_update(
        crate::services::playback_sessions::PlaybackUpdatePayload {
            event: "progress".to_string(),
            state: lyra_db::PlaybackState::Playing,
            playback_session_public_id: "playback-public".to_string(),
            track_public_id: "track-public".to_string(),
            user_public_id: "user-public".to_string(),
            library_public_id: None,
            position_ms: 0,
            duration_ms: None,
            played_ms: 0,
            updated_at_ms: 0,
        },
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        &b"return listen_calls, playback_calls"[..],
    )?;
    assert_eq!(
        values,
        vec![luau::Value::Number(1.0), luau::Value::Number(1.0)]
    );
    let _ = std::fs::remove_dir_all(test_dir);
    Ok(())
}

#[test]
fn plugin_executor_failed_restart_keeps_no_update_handler() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let test_dir = std::env::temp_dir().join(format!(
        "lyra-failed-restart-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    let plugin_dir = test_dir.join("plugins").join("demo");
    std::fs::create_dir_all(&plugin_dir)?;
    std::fs::write(
        plugin_dir.join("plugin.json"),
        serde_json::json!({
            "schema_version": 1,
            "id": "demo",
            "name": "Demo",
            "version": "1.0.0",
            "lyra": crate::plugins::testing::HOST_REQUIREMENT,
            "description": "Demo plugin",
            "entrypoint": "init.luau",
            "scopes": ["lyra.listens"]
        })
        .to_string(),
    )?;
    let write_entrypoint = |fail: bool| {
        std::fs::write(
            plugin_dir.join("init.luau"),
            format!(
                r#"
                    local listens = require("@lyra/listens")
                    listen_calls = 0
                    listens.on_update(function() listen_calls += 1 end)
                    if {fail} then error("entrypoint fails after registering") end
                "#
            ),
        )
    };
    write_entrypoint(false)?;
    let (runtime, errors) =
        PluginExecutor::discover_from_plugins_dir(test_dir.join("plugins"), default_server_info())?;
    assert!(errors.is_empty(), "{errors:?}");
    runtime.exec_plugin("demo")?;

    write_entrypoint(true)?;
    assert!(runtime.restart_plugin("demo").is_err());
    runtime.dispatch_listen_update(crate::services::listens::ListenUpdatePayload {
        event: "recorded".to_string(),
        user_id: "user-public".to_string(),
        listens: Vec::new(),
    })?;

    let values = runtime.eval_plugin_source("demo", "check.luau", &b"return listen_calls"[..])?;
    assert_eq!(values, vec![luau::Value::Number(0.0)]);
    let _ = std::fs::remove_dir_all(test_dir);
    Ok(())
}

#[test]
fn plugin_executor_stop_drops_the_plugins_callbacks() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    init_api_test_state()?;
    let plugin = temp_plugin(
        "demo",
        &[
            "lyra.metadata",
            "lyra.api",
            "lyra.mix",
            "lyra.playback_sessions",
            "lyra.listens",
        ],
        r#"
            local metadata = require("@lyra/metadata")
            local api = require("@lyra/api")
            local mix = require("@lyra/mix")
            local playbacks = require("@lyra/playback_sessions")
            local listens = require("@lyra/listens")
            metadata.Provider.new("stopped-provider"):refresh(metadata.EntityType.Release, function()
                return nil
            end)
            api.get("/stopped", function()
                return api.response.json(200, {})
            end, "public")
            mix.Mixer.new("stopped-mixer"):from_track(function()
                return { tracks = {} }
            end)
            playbacks.on_update(function() end)
            listens.on_update(function() end)
        "#,
    )?;
    let (runtime, errors) = PluginExecutor::discover_from_plugins_dir_with_db(
        plugin.plugins_dir(),
        default_server_info(),
        crate::services::STATE.db.get(),
    )?;
    assert!(errors.is_empty(), "{errors:?}");
    runtime.exec_plugin("demo")?;
    let data = runtime.vm.data();
    let lens = || -> Result<[usize; 5]> {
        Ok([
            data.get::<crate::plugins::metadata::MetadataCallbackRegistry>()?
                .len(),
            data.get::<crate::plugins::api::ApiRouteStore>()?.len(),
            data.get::<crate::plugins::mix::MixCallbackRegistry>()?
                .len(),
            data.get::<crate::plugins::update_callbacks::UpdateCallbackStore<
                crate::services::playback_sessions::PlaybackUpdatePayload,
            >>()?
            .len(),
            data.get::<crate::plugins::update_callbacks::UpdateCallbackStore<
                crate::services::listens::ListenUpdatePayload,
            >>()?
            .len(),
        ])
    };
    assert_eq!(lens()?, [1, 1, 1, 1, 1]);

    runtime.stop_plugin("demo")?;
    assert_eq!(lens()?, [0, 0, 0, 0, 0]);
    Ok(())
}

#[test]
fn plugin_executor_handle_shutdown_stops_lua_and_the_executor_thread() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    init_api_test_state()?;
    let plugin = temp_plugin(
        "demo",
        &["harmony.task", "lyra.api"],
        r#"
            local task = require("@harmony/task")
            local api = require("@lyra/api")
            local ticks = require("@test/ticks")
            task.spawn(function()
                while true do
                    ticks.tick()
                    task.wait(0.005)
                end
            end)
            api.websocket("/socket", function(reader, sender)
                kept_sender = sender
                reader:recv()
            end, "public")
        "#,
    )?;
    let ticks = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let module_ticks = ticks.clone();
    let ticks_module = ModuleSpec::new("test/ticks")
        .function(
            harmony_core::FunctionSpec::sync_fn("tick").call(move |_frame| {
                module_ticks.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }),
        )
        .install(|_| Ok(harmony_core::ModuleExport::new(())));

    let (runtime, errors) = PluginExecutorHandle::discover_from_plugins_dir_with_db_and_modules(
        plugin.plugins_dir(),
        default_server_info(),
        default_auth_capabilities(),
        crate::services::STATE.db.get(),
        vec![ticks_module],
        None,
    )?;
    assert!(errors.is_empty(), "{errors:?}");
    futures::executor::block_on(runtime.exec_all())?;
    let handler = futures::executor::block_on(crate::plugins::api::tests::registered_handler(
        "GET", "/socket",
    ))
    .context("registered websocket handler")?;
    let (request, _inbound_tx, mut outbound_rx) = websocket_request(handler, "/socket");
    futures::executor::block_on(runtime.start_websocket(request))?;
    std::thread::sleep(Duration::from_millis(50));
    assert!(ticks.load(std::sync::atomic::Ordering::SeqCst) > 0);
    assert!(matches!(
        outbound_rx.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));

    let stale = runtime.clone();
    futures::executor::block_on(runtime.shutdown())?;
    let stopped_at = ticks.load(std::sync::atomic::Ordering::SeqCst);
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(ticks.load(std::sync::atomic::Ordering::SeqCst), stopped_at);
    assert_eq!(
        std::sync::Arc::strong_count(&ticks),
        1,
        "the executor thread drops its runtime and the VM's native callbacks",
    );
    while let Ok(_message) = outbound_rx.try_recv() {}
    assert!(
        matches!(
            outbound_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
        ),
        "every websocket sender, even one a plugin kept, is dropped",
    );
    let error = futures::executor::block_on(stale.has_plugin("demo"))
        .expect_err("a stopped executor must not serve requests");
    assert!(format!("{error:#}").contains("unavailable"), "{error:#}");
    Ok(())
}

#[test]
fn plugin_executor_handle_restart_runs_new_entrypoint_on_runtime_thread() -> Result<()> {
    let plugin = temp_plugin("demo", &[], "return nil")?;

    let db = lyra_db::test_db::new_test_db()?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));
    let (runtime, errors) = PluginExecutorHandle::discover_from_plugins_dir_with_db_and_modules(
        plugin.plugins_dir(),
        default_server_info(),
        default_auth_capabilities(),
        db,
        Vec::new(),
        None,
    )?;
    assert!(errors.is_empty(), "{errors:?}");
    futures::executor::block_on(runtime.exec_all())?;

    plugin.write("init.luau", "error('second version ran')")?;
    futures::executor::block_on(runtime.verify_plugin_manifest("demo"))?;
    let error = futures::executor::block_on(runtime.restart_plugin("demo"))
        .expect_err("restart must run the rewritten entrypoint");
    assert!(
        format!("{error:#}").contains("second version ran"),
        "{error:#}"
    );

    plugin.write(
        "plugin.json",
        serde_json::json!({
            "schema_version": 1,
            "id": "demo",
            "name": "Demo",
            "version": "2.0.0",
            "lyra": crate::plugins::testing::HOST_REQUIREMENT,
            "description": "Demo plugin",
            "entrypoint": "init.luau",
            "scopes": []
        })
        .to_string(),
    )?;
    let error = futures::executor::block_on(runtime.verify_plugin_manifest("demo"))
        .expect_err("changed manifest must be rejected");
    assert!(format!("{error:#}").contains("reload plugins"), "{error:#}");
    Ok(())
}

#[test]
fn playlist_bulk_bindings_refuse_inaccessible_tracks() -> Result<()> {
    let mut db = lyra_db::test_db::TestDb::initialized()?.into_inner();
    let owner = lyra_db::fixtures::test_user("playlist-viewer")?;
    let owner_id = lyra_db::users::create(&mut db, &owner)?;
    let hidden_track = lyra_db::fixtures::insert_track(&mut db, "Hidden Track")?;
    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.playlists"])]),
        default_server_info(),
        Arc::new(tokio::sync::RwLock::new(db)),
    )?;
    let mut context = CallContext {
        origin: plugin_origin("demo", "playlist-test.luau"),
        ..CallContext::default()
    };
    seed_caller_principal(
        &mut context,
        crate::services::auth::Principal::from_parts(
            owner_id,
            owner.id,
            "playlist-viewer".into(),
            Vec::new(),
            None,
            Default::default(),
        ),
    );
    let values = runtime.eval_plugin_source_with_call_context(
        format!(
            r#"
                local playlists = require("@lyra/playlists")
                local id = playlists.create({{ name = "Viewer" }})
                local added = pcall(playlists.add_tracks, id, {{ {hidden_track} }})
                local replaced = pcall(playlists.replace, id, {{ track_ids = {{ {hidden_track} }} }})
                return added, replaced, #playlists.get_tracks(id)
            "#,
            hidden_track = hidden_track.0,
        )
        .into_bytes(),
        context,
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::Boolean(false),
            luau::Value::Boolean(false),
            luau::Value::Number(0.0),
        ]
    );
    Ok(())
}

/// A user who can see the `shown` library and not the `hidden` one.
struct LibraryAccessFixture {
    db: agdb::DbAny,
    principal: crate::services::auth::Principal,
    shown: agdb::DbId,
    hidden: agdb::DbId,
}

fn library_access_fixture() -> Result<LibraryAccessFixture> {
    let mut db = lyra_db::test_db::TestDb::initialized()?.into_inner();
    let user = lyra_db::fixtures::test_user("library-viewer")?;
    let user_db_id = lyra_db::users::create(&mut db, &user)?;
    let shown = lyra_db::test_db::insert_library(&mut db, "Shown", "/music/shown")?;
    let hidden = lyra_db::test_db::insert_library(&mut db, "Hidden", "/music/hidden")?;
    let shown_public_id = lyra_db::libraries::get_by_id(&db, shown)?
        .context("shown library exists")?
        .id;
    let principal = crate::services::auth::Principal::from_parts(
        user_db_id,
        user.id,
        "library-viewer".into(),
        Vec::new(),
        None,
        std::collections::HashSet::from([shown_public_id]),
    );
    Ok(LibraryAccessFixture {
        db,
        principal,
        shown,
        hidden,
    })
}

impl LibraryAccessFixture {
    /// Runs `source` as the restricted user, with `scopes` granted.
    fn eval(self, scopes: &[&str], source: String) -> Result<Vec<luau::Value>> {
        let runtime = PluginExecutor::with_database(
            Arc::from(vec![manifest("demo", scopes)]),
            default_server_info(),
            Arc::new(tokio::sync::RwLock::new(self.db)),
        )?;
        let mut context = CallContext {
            origin: plugin_origin("demo", "library-access.luau"),
            ..CallContext::default()
        };
        seed_caller_principal(&mut context, self.principal);
        runtime.eval_plugin_source_with_call_context(source.into_bytes(), context)
    }
}

#[test]
fn cover_bindings_hide_inaccessible_releases() -> Result<()> {
    let mut fixture = library_access_fixture()?;
    let image = tempfile::NamedTempFile::new()?;
    let mut releases = Vec::new();
    for library in [fixture.shown, fixture.hidden] {
        let release = lyra_db::fixtures::insert_release(&mut fixture.db, "Release")?;
        lyra_db::fixtures::connect(&mut fixture.db, library, release)?;
        fixture.db.transaction_mut(|t| -> Result<()> {
            lyra_db::covers::upsert(
                t,
                release,
                lyra_db::Cover {
                    db_id: None,
                    id: nanoid::nanoid!(),
                    path: image.path().to_string_lossy().into_owned(),
                    mime_type: "image/png".into(),
                    hash: "hash".into(),
                    blurhash: None,
                },
            )?;
            Ok(())
        })?;
        releases.push(release.0);
    }
    let [shown, hidden] = releases[..] else {
        unreachable!()
    };
    let values = fixture.eval(
        &["lyra.covers"],
        format!(
            r#"
                local covers = require("@lyra/covers")
                local missing = 987654
                local many = covers.get_many({{ {shown}, {hidden}, missing }})
                return covers.get({shown}) ~= nil, covers.get({hidden}) == nil,
                    many[{shown}] ~= nil, many[{hidden}] == nil,
                    covers.get(missing) == nil, many[missing] == nil
            "#
        ),
    )?;
    assert_eq!(values, vec![luau::Value::Boolean(true); 6]);
    Ok(())
}

#[test]
fn cover_artwork_bindings_serve_anonymous_public_routes() -> Result<()> {
    // api::install resets the shared API route registry; serialize with the
    // other global-registry mutators.
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let mut database = lyra_db::test_db::TestDb::initialized()?.into_inner();
    let owner = lyra_db::users::create(
        &mut database,
        &lyra_db::fixtures::test_user("playlist-owner")?,
    )?;
    let library = lyra_db::test_db::insert_library(&mut database, "Library", "/music")?;
    let release = lyra_db::fixtures::insert_release(&mut database, "Release")?;
    let track = lyra_db::fixtures::insert_track(&mut database, "Track")?;
    lyra_db::fixtures::connect(&mut database, library, release)?;
    lyra_db::fixtures::connect(&mut database, release, track)?;
    let image = tempfile::NamedTempFile::new()?;
    database.transaction_mut(|t| -> Result<()> {
        lyra_db::covers::upsert(
            t,
            release,
            lyra_db::Cover {
                db_id: None,
                id: nanoid::nanoid!(),
                path: image.path().to_string_lossy().into_owned(),
                mime_type: "image/png".into(),
                hash: "hash".into(),
                blurhash: None,
            },
        )?;
        Ok(())
    })?;
    let playlist = lyra_db::playlists::create(
        &mut database,
        &lyra_db::Playlist {
            db_id: None,
            id: nanoid::nanoid!(),
            name: "Private".into(),
            description: None,
            is_public: Some(false),
            created_at: None,
            updated_at: None,
        },
        owner,
    )?;
    database.transaction_mut(|t| lyra_db::playlists::add_track(t, playlist, track))?;

    futures::executor::block_on(crate::plugins::api::initialize_registry(
        std::collections::HashSet::new(),
        None,
    ));
    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.api", "lyra.covers"])]),
        default_server_info(),
        Arc::new(tokio::sync::RwLock::new(database)),
    )?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local api = require("@lyra/api")
            local covers = require("@lyra/covers")

            api.get("/artwork/{release}/{playlist}", function(ctx)
                local release = tonumber(ctx.params.release)
                local playlist = tonumber(ctx.params.playlist)
                return api.response.json(200, {
                    cover = covers.artwork(release) ~= nil,
                    many = covers.artwork_many({ release })[release] ~= nil,
                    playlist_sources = #covers.artwork_playlist_sources(playlist),
                    scoped = pcall(covers.get, release),
                    scoped_playlist = pcall(covers.get_playlist_sources, playlist),
                })
            end, "public")
        "#[..],
    )?;

    let handler = futures::executor::block_on(crate::plugins::api::tests::registered_handler(
        "GET",
        "/artwork/{release}/{playlist}",
    ))
    .context("registered API handler")?;
    let result = runtime.dispatch_api_handler(ApiHandlerRequest {
        params: HashMap::from([
            ("release".to_string(), release.0.to_string()),
            ("playlist".to_string(), playlist.0.to_string()),
        ]),
        ..api_request(handler, &format!("/artwork/{}/{}", release.0, playlist.0))
    })?;

    assert_eq!(result.status, 200);
    let Some(ApiResponseBody::Json(body)) = result.body else {
        anyhow::bail!("expected JSON response body");
    };
    assert_eq!(body["cover"], true);
    assert_eq!(body["many"], true);
    assert_eq!(body["playlist_sources"], 1);
    assert_eq!(body["scoped"], false);
    assert_eq!(body["scoped_playlist"], false);
    Ok(())
}

#[test]
fn streamable_bindings_serve_anonymous_public_routes() -> Result<()> {
    // api::install resets the shared API route registry; serialize with the
    // other global-registry mutators.
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let mut database = lyra_db::test_db::TestDb::initialized()?.into_inner();
    let library = lyra_db::test_db::insert_library(&mut database, "Library", "/music")?;
    let release = lyra_db::fixtures::insert_release(&mut database, "Release")?;
    let track = lyra_db::fixtures::insert_track(&mut database, "Track")?;
    lyra_db::fixtures::connect(&mut database, library, release)?;
    lyra_db::fixtures::connect(&mut database, release, track)?;
    let audio = tempfile::Builder::new().suffix(".flac").tempfile()?;
    let entry = lyra_db::Entry {
        db_id: None,
        id: nanoid::nanoid!(),
        full_path: audio.path().to_path_buf(),
        kind: lyra_db::entries::EntryKind::File,
        file_kind: Some("audio".into()),
        name: "track.flac".into(),
        hash: None,
        size: 1,
        mtime: 1,
        ctime: 1,
    };
    let entry_id = database
        .exec_mut(agdb::QueryBuilder::insert().element(&entry).query())?
        .ids()[0];
    lyra_db::fixtures::connect(&mut database, library, entry_id)?;
    database.transaction_mut(|t| -> Result<()> {
        lyra_db::track_sources::upsert(
            t,
            track,
            entry_id,
            lyra_db::track_sources::TrackSourceUpsert {
                source_kind: "embedded_tags".into(),
                source_key: "track".into(),
                is_primary: true,
                start_ms: None,
                end_ms: None,
                format: Default::default(),
            },
            None,
        )?;
        Ok(())
    })?;

    futures::executor::block_on(crate::plugins::api::initialize_registry(
        std::collections::HashSet::new(),
        None,
    ));
    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest(
            "demo",
            &["lyra.api", "lyra.tracks", "lyra.playback_sources"],
        )]),
        default_server_info(),
        Arc::new(tokio::sync::RwLock::new(database)),
    )?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local api = require("@lyra/api")
            local playback_sources = require("@lyra/playback_sources")
            local tracks = require("@lyra/tracks")

            api.get("/stream/{track}/{release}", function(ctx)
                local track = tonumber(ctx.params.track)
                local release = tonumber(ctx.params.release)
                local source = playback_sources.streamable(track, true)
                return api.response.json(200, {
                    track = tracks.streamable({ track, release })[track] ~= nil,
                    release = tracks.streamable({ release })[release] ~= nil,
                    entry_name = source and source.entry and source.entry.name,
                    full_path = source and source.entry and source.entry.full_path,
                    release_source = playback_sources.streamable(release) ~= nil,
                    scoped = pcall(tracks.get, { track }),
                    scoped_source = pcall(playback_sources.get, track),
                })
            end, "public")
        "#[..],
    )?;

    let handler = futures::executor::block_on(crate::plugins::api::tests::registered_handler(
        "GET",
        "/stream/{track}/{release}",
    ))
    .context("registered API handler")?;
    let result = runtime.dispatch_api_handler(ApiHandlerRequest {
        params: HashMap::from([
            ("track".to_string(), track.0.to_string()),
            ("release".to_string(), release.0.to_string()),
        ]),
        ..api_request(handler, &format!("/stream/{}/{}", track.0, release.0))
    })?;

    assert_eq!(result.status, 200);
    let Some(ApiResponseBody::Json(body)) = result.body else {
        anyhow::bail!("expected JSON response body");
    };
    assert_eq!(body["track"], true);
    assert_eq!(body["release"], false);
    assert_eq!(body["entry_name"], "track.flac");
    assert_eq!(body["full_path"], serde_json::Value::Null);
    assert_eq!(body["release_source"], false);
    assert_eq!(body["scoped"], false);
    assert_eq!(body["scoped_source"], false);
    Ok(())
}

#[test]
fn label_bindings_hide_inaccessible_releases() -> Result<()> {
    let mut fixture = library_access_fixture()?;
    let mut releases = Vec::new();
    for library in [fixture.shown, fixture.hidden] {
        let release = lyra_db::fixtures::insert_release(&mut fixture.db, "Release")?;
        lyra_db::fixtures::connect(&mut fixture.db, library, release)?;
        lyra_db::labels::sync_release_labels(
            &mut fixture.db,
            release,
            &[lyra_db::labels::LabelInput {
                name: "Label".into(),
                catalog_number: None,
                external_id: None,
            }],
        )?;
        releases.push(release.0);
    }
    let label = lyra_db::labels::find_by_name(&fixture.db, "Label")?.context("label exists")?;
    let [shown, hidden] = releases[..] else {
        unreachable!()
    };
    let values = fixture.eval(
        &["lyra.labels"],
        format!(
            r#"
                local labels = require("@lyra/labels")
                local releases = labels.get_releases({label})
                local many = labels.get_releases_many({{ {label} }})[{label}]
                return #releases, releases[1], #many, many[1],
                    #labels.get_for_release({hidden}), #labels.get_for_release({shown})
            "#,
            label = label.0,
        ),
    )?;
    let shown = luau::Value::Number(shown as f64);
    assert_eq!(
        values,
        vec![
            luau::Value::Number(1.0),
            shown.clone(),
            luau::Value::Number(1.0),
            shown,
            luau::Value::Number(0.0),
            luau::Value::Number(1.0),
        ]
    );
    Ok(())
}

#[test]
fn track_source_bindings_hide_inaccessible_tracks() -> Result<()> {
    let mut fixture = library_access_fixture()?;
    let mut ids = Vec::new();
    for (library, name) in [(fixture.shown, "shown"), (fixture.hidden, "hidden")] {
        let track = lyra_db::fixtures::insert_track(&mut fixture.db, name)?;
        let entry = lyra_db::Entry {
            db_id: None,
            id: nanoid::nanoid!(),
            full_path: std::path::PathBuf::from(format!("/music/{name}.flac")),
            kind: lyra_db::entries::EntryKind::File,
            file_kind: Some("audio".into()),
            name: format!("{name}.flac"),
            hash: None,
            size: 1,
            mtime: 1,
            ctime: 1,
        };
        let entry_id = fixture
            .db
            .exec_mut(agdb::QueryBuilder::insert().element(&entry).query())?
            .ids()[0];
        lyra_db::fixtures::connect(&mut fixture.db, library, track)?;
        lyra_db::fixtures::connect(&mut fixture.db, library, entry_id)?;
        fixture.db.transaction_mut(|t| -> Result<()> {
            lyra_db::track_sources::upsert(
                t,
                track,
                entry_id,
                lyra_db::track_sources::TrackSourceUpsert {
                    source_kind: "embedded_tags".into(),
                    source_key: name.into(),
                    is_primary: true,
                    start_ms: None,
                    end_ms: None,
                    format: Default::default(),
                },
                None,
            )?;
            Ok(())
        })?;
        ids.push((track.0, entry_id.0));
    }
    let [(shown, _), (hidden, hidden_entry)] = ids[..] else {
        unreachable!()
    };
    let values = fixture.eval(
        &["lyra.track_sources", "lyra.chromaprint"],
        format!(
            r#"
                local track_sources = require("@lyra/track_sources")
                local chromaprint = require("@lyra/chromaprint")
                local _, fingerprint_error = pcall(chromaprint.compute, {hidden_entry})
                return track_sources.get_primary_source_key({shown}),
                    track_sources.get_primary_source_key({hidden}) == nil,
                    string.find(tostring(fingerprint_error), "entry not found", 1, true) ~= nil
            "#
        ),
    )?;
    let text = |value: &str| luau::Value::String(value.as_bytes().to_vec());
    let yes = luau::Value::Boolean(true);
    assert_eq!(values, vec![text("shown"), yes.clone(), yes]);
    Ok(())
}
