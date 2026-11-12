use super::*;
use anyhow::{
    Context,
    Result,
};
use harmony_core::{
    CallContext,
    MemorySourceLoader,
    ModuleSpec,
    UserDataBinding,
    luau::RequireRuntime,
    plugin::{
        PluginDiscoveryError,
        PluginManifest,
    },
};
use harmony_luau as luau;
use std::time::Duration;

fn default_server_info() -> crate::plugins::server::ServerInfo {
    crate::plugins::server::ServerInfo {
        id: "raw-runtime".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        commit_hash: "test".to_string(),
        port: 0,
        tls: false,
        published_url: None,
        setup: crate::services::setup::Status {
            account_required: true,
            setup_code_required: false,
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

    /// An executor whose scheduler runs by `clock`, for a test that runs its rounds itself: it
    /// has no thread, and what blocks on a real clock fails here.
    pub(crate) fn with_manual_clock(
        manifests: Arc<[PluginManifest]>,
        clock: harmony_core::testing::ManualClock,
    ) -> Result<Self> {
        Self::with_runtime(
            harmony_core::plugin::RuntimeBuilder::new(MemorySourceLoader::new(), manifests.clone())
                .manual_clock(clock),
            manifests,
            default_server_info(),
            default_auth_capabilities(),
            stores::PluginModuleStores::empty(),
            Vec::new(),
        )
    }

    pub(super) fn drive_callback<T>(
        &self,
        start: impl FnOnce(tokio::sync::oneshot::Sender<Result<T>>, tokio::sync::OwnedSemaphorePermit),
    ) -> Result<T> {
        let (reply, mut result) = tokio::sync::oneshot::channel();
        let permit = std::sync::Arc::new(tokio::sync::Semaphore::new(1)).try_acquire_owned()?;
        start(reply, permit);
        self.run_until("the callback is answered", || match result.try_recv() {
            Ok(result) => Some(result),
            Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                Some(Err(anyhow::anyhow!("callback dropped response")))
            }
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => None,
        })?
    }

    /// Serves as the executor thread does, in rounds with a sleep in between that only a wake or
    /// something due ends, until `done` returns a value. Fails if that takes a wake that never
    /// comes: a sleep that lasts to the watchdog is the failure, and no round follows it to find
    /// what the missing wake left ready.
    pub(super) fn run_until<T>(&self, what: &str, done: impl FnMut() -> Option<T>) -> Result<T> {
        self.run_until_within(Duration::from_secs(60), what, done)
    }

    /// [`Self::run_until`] with `watchdog` as the longest sleep that isn't yet a failure.
    pub(super) fn run_until_within<T>(
        &self,
        watchdog: Duration,
        what: &str,
        mut done: impl FnMut() -> Option<T>,
    ) -> Result<T> {
        let scheduler = self.vm.data().get::<LocalScheduler>()?;
        let give_up = std::time::Instant::now() + watchdog;
        loop {
            self.poll_background_tasks();
            if let Some(value) = done() {
                return Ok(value);
            }
            scheduler.wait_for_work(Some(give_up));
            if std::time::Instant::now() >= give_up {
                anyhow::bail!("nothing woke the executor before {what}");
            }
        }
    }

    /// Runs rounds for as long as one has something to do, which on a manual clock is until the
    /// test moves the clock or wakes a task. Fails on work that never settles.
    pub(super) fn run_until_idle(&self) -> Result<()> {
        let scheduler = self.vm.data().get::<LocalScheduler>()?;
        for _ in 0..1000 {
            if scheduler.next_wake_delay() != Some(Duration::ZERO) {
                return Ok(());
            }
            self.poll_background_tasks();
        }
        anyhow::bail!("the executor still had a round to run after 1000")
    }

    /// As [`Self::eval_plugin_source_with_call_context`] on a manual clock, for a chunk that
    /// ends without waiting.
    pub(crate) fn eval_source_while_ready(
        &self,
        source: impl Into<Arc<[u8]>>,
        mut context: CallContext,
    ) -> Result<Vec<luau::Value>> {
        let thread = self.runtime.load_source_thread(source, &mut context)?;
        let scheduler = self.vm.data().get::<LocalScheduler>()?;
        scheduler.spawn_luau_thread(context, thread.clone(), Vec::new());
        harmony_core::testing::drive_thread_while_ready(&self.tokio_runtime, &scheduler, &thread)
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
            stores::PluginModuleStores::with_db(db).sharing_published_scope(),
            MemorySourceLoader::new(),
        )
    }

    pub(crate) fn discover_from_plugins_dir(
        plugins_dir: impl Into<std::path::PathBuf>,
        server_info: crate::plugins::server::ServerInfo,
    ) -> Result<(Self, Vec<PluginDiscoveryError>)> {
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
    ) -> Result<(Self, Vec<PluginDiscoveryError>)> {
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

/// The public id plugins name a node by.
fn public_id(db: &impl lyra_db::DbAccess, db_id: agdb::DbId) -> Result<String> {
    lyra_db::lookup::find_id_by_db_id(db, db_id)?.context("node has a public id")
}

fn seed_caller_principal(context: &mut CallContext, principal: crate::services::auth::Principal) {
    let dispatch_auth = crate::plugins::auth::DispatchAuth::default();
    dispatch_auth.record(crate::plugins::testing::acting_as(principal));
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
        base_url: None,
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
        scope_reasons: Default::default(),
        scope_grants: Default::default(),
        dependencies: Default::default(),
    }
}

#[test]
fn plugin_executor_preserves_typed_call_context_across_luau_yield() -> Result<()> {
    let runtime = runtime_with_scopes(&["harmony.task", "test.context"])?;
    let require = runtime.vm.data().get::<RequireRuntime>()?;
    require.register(harmony_core::GatedModule::new(
        "test.context",
        ModuleSpec::new("test/context")
            .function(
                harmony_core::FunctionSpec::new("username").call(|mut frame| {
                    let principal =
                        crate::plugins::auth::require_dispatch_principal(&frame.context)?;
                    frame.returns.write(principal.username.as_str())
                }),
            )
            .install(|_| Ok(harmony_core::ModuleExport::new(()))),
    ))?;

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
    require.register(harmony_core::GatedModule::new(
        "test.context",
        ModuleSpec::new("test/context")
            .function(
                harmony_core::FunctionSpec::new("username").call(|mut frame| {
                    let principal =
                        crate::plugins::auth::require_dispatch_principal(&frame.context)?;
                    frame.returns.write(principal.username.as_str())
                }),
            )
            .install(|_| Ok(harmony_core::ModuleExport::new(()))),
    ))?;
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
                end, { auth = "public" })
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
fn module_functions_answer_what_they_can_before_reading_the_database() -> Result<()> {
    let runtime = runtime_with_scopes(&[
        "lyra.genres",
        "lyra.genres.manage",
        "lyra.labels",
        "lyra.labels.manage",
    ])?;
    let mut viewer = CallContext {
        origin: plugin_origin("demo", "init.luau"),
        ..CallContext::default()
    };
    seed_caller_principal(
        &mut viewer,
        crate::services::auth::Principal::from_parts(
            agdb::DbId(1),
            "viewer".to_string(),
            "viewer".to_string(),
            Vec::new(),
            None,
            Default::default(),
        ),
    );
    let values = runtime.eval_plugin_source_with_call_context(
        br#"
            local genres = require("@lyra/genres")
            local labels = require("@lyra/labels")
            return labels.resolve({ name = "Label" }) == nil, genres.resolve({ name = "Genre" }) == nil
        "# as &[u8],
        viewer,
    )?;
    assert_eq!(
        values,
        vec![luau::Value::Boolean(true), luau::Value::Boolean(true)]
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
                last_checked_at_ms: 1,
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
                local track_id = {track_public_id:?}
                local function best(options)
                    local id = lyrics.by_track({{ track_id }}, options)[track_id][1].id
                    return lyrics.get({{ id }})[id]
                end
                local provider = best({{ provider_id = "test_provider" }})
                local personal = lyrics.set_personal(track_id, {{
                    content_type = "text/plain",
                    body = "alice lyrics",
                    language = "eng",
                }})
                local automatic = best()
                local selected_provider = best({{ provider_id = "test_provider" }})
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
                local track_id = {track_public_id:?}
                local function best()
                    local id = lyrics.by_track({{ track_id }})[track_id][1].id
                    return lyrics.get({{ id }})[id]
                end
                local before = best()
                lyrics.set_personal(track_id, {{
                    content_type = "text/plain",
                    body = "bob lyrics",
                    language = "eng",
                }})
                local after = best()
                return before.plain_text, after.plain_text
            "#,
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
                return lyrics.remove_personal({track_public_id:?})
            "#,
        )
        .into_bytes(),
        inaccessible_alice_context,
    )?;
    assert_eq!(
        deleted,
        vec![luau::Value::Boolean(false)],
        "a track the caller can no longer see answers false"
    );

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
    let entity =
        crate::plugins::types::LuauEntityType::class().read_value("entity", values[0].clone())?;
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
    assert_eq!(id_spec.scheme, "example:release");
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
                    all_of = { "external_ids.musicbrainz:release-group" },
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
        ["external_ids.musicbrainz:release-group"]
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
    let (seed, candidate_public_id) = futures::executor::block_on(async {
        let mut db = crate::services::STATE.db.write().await;
        let seed = lyra_db::fixtures::insert_release(&mut db, "Seed")?;
        let candidate = lyra_db::fixtures::insert_release(&mut db, "Candidate")?;
        Ok::<_, anyhow::Error>((public_id(&*db, seed)?, public_id(&*db, candidate)?))
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
                return {{ candidates = {{{{ release_id = {candidate_public_id:?} }}}} }}
            end)
            local matches = releases.similar({seed:?})
            return matches[1].id
        "#,
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
        let seed = lyra_db::fixtures::insert_release(&mut db, "Seed")?;
        public_id(&*db, seed)
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
                local ok, err = pcall(releases.similar, {seed:?})
                return ok, string.find(tostring(err), "no authenticated caller", 1, true) ~= nil
            "#,
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
    let (seed, candidate_public_id) = futures::executor::block_on(async {
        let mut db = crate::services::STATE.db.write().await;
        let seed = lyra_db::fixtures::insert_release(&mut db, "Seed")?;
        let candidate = lyra_db::fixtures::insert_release(&mut db, "Candidate")?;
        Ok::<_, anyhow::Error>((public_id(&*db, seed)?, public_id(&*db, candidate)?))
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
                return {{ candidates = {{{{ release_id = {candidate_public_id:?} }}}} }}
            end)
            similar_done = false
            similar_result_id = nil
            task.spawn(function()
                local matches = releases.similar({seed:?})
                similar_result_id = matches[1].id
                similar_done = true
            end)
        "#,
    );
    runtime.eval_plugin_source_with_call_context(
        source.into_bytes(),
        admin_call_context("init.luau")?,
    )?;

    let scheduler = runtime.vm.data().get::<LocalScheduler>()?;
    runtime.run_until("the background task ends", || {
        (!scheduler.has_pending()).then_some(())
    })?;
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
        let seed = lyra_db::fixtures::insert_release(&mut db, "Seed")?;
        public_id(&*db, seed)
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
                releases.similar({seed:?})
                similar_resumed = true
            end)
        "#,
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
            port: 3210,
            tls: false,
            published_url: Some("https://lyra.example".to_string()),
            setup: crate::services::setup::Status {
                account_required: false,
                setup_code_required: false,
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
    let runtime = runtime_with_scopes(&[
        "lyra.playback_sessions",
        "lyra.playback_sessions.all",
        "harmony.task",
    ])?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local task = require("@harmony/task")
            local playbacks = require("@lyra/playback_sessions")

            playbacks.on_update(function(update)
                task.wait()
                executor_playback_update = update.event .. ":" .. update.track_id
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
            plugin_id: None,
            client_name: None,
            position_ms: 42,
            duration_ms: Some(100),
            rate: 1.0,
            counts_as_listen: true,
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
    let runtime = runtime_with_scopes(&["lyra.listens", "lyra.listens.all", "harmony.task"])?;
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
        played_ms: Some(42_000),
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
                    provider:id({{ id_type = "thing_id", entity = metadata.EntityType.Release, scheme = "test:{name}" }}, function()
                        return "https://example.com/{name}"
                    end)
                    api.get("/{name}", function()
                        return api.response.json(200, {{}})
                    end, {{ auth = "public" }})
                    api.websocket("/{name}/socket", function(_reader, sender)
                        sender:close()
                    end, {{ auth = "public" }})
                    mix.Mixer.new("{name}"):blend({{ seeds = {{ mix.SeedType.Track }} }}, function()
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
                    .handler(name, crate::services::mix::MixShape::Blend)
                    .map(|registration| registration.handler)
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
        timeout: Duration::from_secs(5),
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
                seeds: Vec::new(),
                query: None,
                limit: Some(1),
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
        super::CallChain::root(std::time::Instant::now() + Duration::from_secs(5), None),
        "stale-b".to_string(),
        vec![id_link_call(stale.id_link)],
    ))?;
    assert_eq!(
        results[0],
        Err(crate::services::providers::IdLinkCallError::NotRun),
        "a stale id link handle runs nothing, and nothing is remembered for it"
    );
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
            end, { auth = "public" })
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

#[test]
fn plugin_executor_passes_request_keys_holding_nul_bytes() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    init_api_test_state()?;
    let runtime = runtime_with_scopes(&["lyra.api"])?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local api = require("@lyra/api")

            api.post("/demo/nul", function(ctx)
                return api.response.json(200, {
                    query = ctx.request.query["a\0b"][1],
                    body = ctx.request.json["c\0d"],
                })
            end, { auth = "public" })
        "#[..],
    )?;

    let handler = futures::executor::block_on(crate::plugins::api::tests::registered_handler(
        "POST",
        "/demo/nul",
    ))
    .context("registered API handler")?;
    let result = runtime.dispatch_api_handler(ApiHandlerRequest {
        method: "POST".to_string(),
        headers: vec![("content-type".to_string(), "application/json".to_string())],
        query: HashMap::from([("a\0b".to_string(), vec!["from query".to_string()])]),
        body: br#"{"c\u0000d":"from body"}"#.to_vec(),
        ..api_request(handler, "/demo/nul")
    })?;

    let Some(ApiResponseBody::Json(body)) = result.body else {
        anyhow::bail!("expected JSON response body");
    };
    assert_eq!(body["query"], "from query");
    assert_eq!(body["body"], "from body");
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
                end, {{ auth = "public" }})

                api.get("/anon", function(ctx)
                    return api.response.json(200, {{}})
                end, {{ auth = "public" }})
            "#
        )
        .into_bytes(),
    )?;

    let whoami_handler = crate::plugins::api::tests::registered_handler("GET", "/whoami")
        .await
        .context("registered /whoami handler")?;
    let response = runtime.dispatch_api_handler(api_request(whoami_handler, "/whoami"))?;
    let auth = response
        .auth
        .context("resolve_auth during dispatch should bind whom it resolved to the response")?;
    assert_eq!(auth.principal.user_public_id, user.id);
    assert!(
        matches!(
            auth.credential,
            crate::services::auth::AuthCredential::Session { .. }
        ),
        "the principal comes with the credential that authenticated it"
    );

    let anon_handler = crate::plugins::api::tests::registered_handler("GET", "/anon")
        .await
        .context("registered /anon handler")?;
    let response = runtime.dispatch_api_handler(api_request(anon_handler, "/anon"))?;
    assert!(
        response.auth.is_none(),
        "dispatches that never resolve auth must not carry a principal"
    );

    let boundary_auth = crate::services::auth::resolve_auth_from_bearer(Some(&token))
        .await?
        .context("bearer token should resolve at the boundary")?;
    let mut seeded = api_request(anon_handler, "/anon");
    seeded.auth = Some(boundary_auth);
    let response = runtime.dispatch_api_handler(seeded)?;
    let auth = response
        .auth
        .context("boundary auth should seed the dispatch")?;
    assert_eq!(auth.principal.user_public_id, user.id);

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
            end, { auth = "public" })
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
    assert!(response.auth.is_some());

    crate::services::STATE.db.write().await.exec_mut(
        agdb::QueryBuilder::insert()
            .values_uniform([("expires_at_ms", lyra_db::users::now_ms() - 1).into()])
            .ids(session_id)
            .query(),
    )?;
    let response = runtime.dispatch_api_handler(request())?;
    assert_eq!(response.status, 401);
    assert!(response.auth.is_none());
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
            end, { auth = "public" })
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

    assert_eq!(
        next_websocket_frame(&runtime, &mut outbound_rx)?,
        "abc:hello"
    );
    runtime.run_until("the websocket's task is cleaned up", || {
        runtime.websocket_tasks.borrow().is_empty().then_some(())
    })
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
            end, { auth = "public" })

            api.get("/ping", function()
                return api.response.empty(204)
            end, { auth = "public" })
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

    runtime.run_until("the websocket's task is cleaned up", || {
        runtime.websocket_tasks.borrow().is_empty().then_some(())
    })
}

/// The next frame a websocket handler of `runtime` sends, serving the runtime until it does.
fn next_websocket_frame(
    runtime: &PluginExecutor,
    outbound: &mut tokio::sync::mpsc::Receiver<String>,
) -> Result<String> {
    runtime.run_until("a websocket frame arrives", || match outbound.try_recv() {
        Ok(text) => Some(Ok(text)),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => None,
        Err(error) => Some(Err(error).context("websocket outbound channel closed")),
    })?
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
            &[
                "harmony.task",
                "lyra.api",
                "lyra.playback_sessions",
                "lyra.playback_sessions.all",
            ],
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
            end, { auth = "public" })
        "#[..],
    )?;
    let handler = crate::plugins::api::tests::registered_handler("GET", "/socket")
        .await
        .context("registered websocket handler")?;
    let (mut request, inbound_tx, mut outbound_rx) = websocket_request(handler, "/socket");
    let auth = crate::plugins::testing::acting_as(principal);
    request.auth = Some(auth.clone());
    request.dispatch_auth.record(auth);
    let state = request.state.clone();
    runtime.start_websocket(request)?;

    inbound_tx.send("probe".to_string()).await?;
    let allowed = next_websocket_frame(&runtime, &mut outbound_rx);
    {
        let mut db = crate::services::STATE.db.write().await;
        lyra_db::users::delete_user(&mut db, user_db_id)?;
    }
    inbound_tx.send("probe".to_string()).await?;
    let stale = next_websocket_frame(&runtime, &mut outbound_rx);
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
            mixer:blend({ seeds = { mix.SeedType.Track, mix.SeedType.Genre }, max_seeds = 2, exclude = true }, function(ctx)
                task.wait()
                local track, genre = ctx.seeds[1], ctx.seeds[2]
                assert(track.id == "track-40" and track.type == mix.SeedType.Track and not track.exclude, "track seed")
                assert(genre.id == "genre-7" and genre.type == mix.SeedType.Genre and genre.exclude, "genre seed")
                assert(ctx.q == "rain" and ctx.limit == 10, "query and limit")
                return {
                    tracks = {
                        { track_id = ctx.options.boost and "track-42" or "track-41", score = 0.75 },
                        { track_id = "track-43" },
                    },
                }
            end)
        "#[..],
    )?;

    let handler = futures::executor::block_on(async {
        crate::services::mix::mix_registry()
            .read_owned()
            .await
            .handler("demo-mixer", crate::services::mix::MixShape::Blend)
            .map(|registration| registration.handler)
    })
    .context("registered mix callback")?;
    let seed = |id: &str, seed_type, exclude| crate::services::mix::MixSeed {
        db_id: agdb::DbId(1),
        id: id.to_string(),
        seed_type,
        exclude,
    };
    let result = runtime.dispatch_mix_handler(MixHandlerRequest {
        handler,
        seeds: vec![
            seed("track-40", crate::services::mix::MixSeedType::Track, false),
            seed("genre-7", crate::services::mix::MixSeedType::Genre, true),
        ],
        query: Some("rain".to_string()),
        limit: Some(10),
        options: serde_json::Map::from_iter([("boost".to_string(), serde_json::Value::Bool(true))]),
    })?;

    assert_eq!(
        result.tracks,
        vec![
            crate::services::plugin_dispatch::MixHandlerTrack {
                track_id: "track-42".to_string(),
                score: Some(0.75),
            },
            crate::services::plugin_dispatch::MixHandlerTrack {
                track_id: "track-43".to_string(),
                score: None,
            },
        ]
    );
    Ok(())
}

#[test]
fn mix_results_take_scores_from_0_to_1_and_refuse_others() -> Result<()> {
    let runtime = runtime_with_scopes(&[])?;
    let parse = |tracks: &str| {
        let values = runtime.eval_plugin_source(
            "demo",
            "result.luau",
            format!("return {{ tracks = {tracks} }}").into_bytes(),
        )?;
        super::mix::parse_mix_result("demo-mixer", values)
    };

    let result = parse(
        r#"{ { track_id = "a", score = 0 }, { track_id = "b" }, { track_id = "c", score = 1 }, { track_id = "d", score = 0.25 } }"#,
    )?;
    assert_eq!(
        result
            .tracks
            .iter()
            .map(|track| (track.track_id.as_str(), track.score))
            .collect::<Vec<_>>(),
        vec![
            ("a", Some(0.0)),
            ("b", None),
            ("c", Some(1.0)),
            ("d", Some(0.25))
        ]
    );

    for (score, message) in [
        ("1.5", "returned score 1.5 for track 'a', expected 0 to 1"),
        ("-0.1", "returned score -0.1 for track 'a', expected 0 to 1"),
        ("0/0", "expected 0 to 1"),
        (
            "\"0.5\"",
            "returned score string for track 'a', expected number",
        ),
        (
            "true",
            "returned score boolean for track 'a', expected number",
        ),
    ] {
        let error = parse(&format!(r#"{{ {{ track_id = "a", score = {score} }} }}"#))
            .expect_err(&format!("score {score} must be refused"));
        assert!(
            error.to_string().contains(message),
            "score {score}: {error:#}"
        );
    }
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
    let db = lyra_db::DbAsync::new(lyra_db::test_db::new_test_db()?);
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
    let db = lyra_db::DbAsync::new(db);
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

/// Restarts `plugin_id` as the executor's command does, on a manual clock: its entrypoint must end
/// without waiting.
fn restart_while_ready(runtime: &PluginExecutor, plugin_id: &str) -> Result<()> {
    let (reply, mut answer) = tokio::sync::oneshot::channel();
    runtime.start_plugin_restart(plugin_id.to_string(), reply);
    runtime.run_until_idle()?;
    answer
        .try_recv()
        .context("the restarted entrypoint waits")?
}

#[test]
fn plugin_executor_restart_reloads_plugin_sources_from_disk() -> Result<()> {
    const DELAY: Duration = Duration::from_millis(50);
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

    let clock = harmony_core::testing::ManualClock::new();
    let discovered = vm::DiscoveredPlugins::in_dir(plugin.plugins_dir())?;
    assert!(discovered.errors.is_empty(), "{:?}", discovered.errors);
    let runtime = PluginExecutor::with_runtime(
        discovered.builder.manual_clock(clock.clone()),
        discovered.manifests,
        default_server_info(),
        default_auth_capabilities(),
        stores::PluginModuleStores::empty(),
        Vec::new(),
    )?;
    let scheduler = runtime.vm.data().get::<LocalScheduler>()?;
    let read = |global: &'static str| {
        runtime.eval_source_while_ready(
            format!("return {global}").into_bytes(),
            CallContext {
                origin: plugin_origin("demo", "check.luau"),
                ..CallContext::default()
            },
        )
    };

    restart_while_ready(&runtime, "demo")?;
    assert_eq!(
        read("restart_reload_output")?,
        vec![luau::Value::Number(1.0)]
    );
    assert_eq!(
        scheduler.next_wake_delay(),
        Some(DELAY),
        "the first run's delayed task waits"
    );

    write_lib("2")?;
    runtime.verify_plugin_manifest("demo")?;
    restart_while_ready(&runtime, "demo")?;
    assert_eq!(
        read("restart_reload_output")?,
        vec![luau::Value::Number(2.0)]
    );
    assert_eq!(read("delayed_runs")?, vec![luau::Value::Nil]);
    assert_eq!(scheduler.next_wake_delay(), Some(DELAY));

    clock.advance(DELAY);
    runtime.run_until_idle()?;
    assert_eq!(
        read("delayed_runs")?,
        vec![luau::Value::String(b"2".to_vec())],
        "the previous run's delayed task never fires"
    );
    assert_eq!(scheduler.next_wake_delay(), None);
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
            "scopes": [
                "lyra.listens",
                "lyra.listens.all",
                "lyra.playback_sessions",
                "lyra.playback_sessions.all"
            ]
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
            plugin_id: None,
            client_name: None,
            position_ms: 0,
            duration_ms: None,
            rate: 1.0,
            counts_as_listen: true,
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
            "scopes": ["lyra.listens", "lyra.listens.all"]
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
            "lyra.playback_sessions.all",
            "lyra.listens",
            "lyra.listens.all",
            "lyra.favorites",
            "lyra.favorites.all",
            "lyra.ratings",
            "lyra.ratings.all",
            "lyra.bookmarks",
            "lyra.bookmarks.all",
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
            end, { auth = "public" })
            mix.Mixer.new("stopped-mixer"):blend({ seeds = { mix.SeedType.Track } }, function()
                return { tracks = {} }
            end)
            playbacks.on_update(function() end)
            listens.on_update(function() end)
            require("@lyra/favorites").on_update(function() end)
            require("@lyra/ratings").on_update(function() end)
            require("@lyra/bookmarks").on_update(function() end)
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
    let lens = || -> Result<[usize; 8]> {
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
            data.get::<crate::plugins::update_callbacks::UpdateCallbackStore<
                crate::services::favorites::FavoriteUpdatePayload,
            >>()?
            .len(),
            data.get::<crate::plugins::update_callbacks::UpdateCallbackStore<
                crate::services::ratings::RatingUpdatePayload,
            >>()?
            .len(),
            data.get::<crate::plugins::update_callbacks::UpdateCallbackStore<
                crate::services::bookmarks::BookmarkUpdatePayload,
            >>()?
            .len(),
        ])
    };
    assert_eq!(lens()?, [1; 8]);

    runtime.stop_plugin("demo")?;
    assert_eq!(lens()?, [0; 8]);
    Ok(())
}

#[test]
fn plugin_executor_handle_shutdown_stops_lua_and_the_executor_thread() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    init_api_test_state()?;
    let plugin = temp_plugin(
        "demo",
        &["harmony.task", "lyra.api", "harmony.crypt"],
        r#"
            local task = require("@harmony/task")
            local api = require("@lyra/api")
            local ticks = require("@harmony/crypt")
            task.spawn(function()
                while true do
                    ticks.tick()
                    task.wait(0.005)
                end
            end)
            api.websocket("/socket", function(reader, sender)
                kept_sender = sender
                reader:recv()
            end, { auth = "public" })
        "#,
    )?;
    let ticks = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let module_ticks = ticks.clone();
    // Overrides replace a module, so the counter stands in for one this
    // plugin does not otherwise use.
    let ticks_module = ModuleSpec::new("harmony/crypt")
        .function(harmony_core::FunctionSpec::new("tick").call(move |_frame| {
            module_ticks.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }))
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
    let handler = futures::executor::block_on(crate::plugins::api::tests::registered_handler_of(
        &runtime, "GET", "/socket",
    ))
    .context("registered websocket handler")?;
    let (request, _inbound_tx, mut outbound_rx) = websocket_request(handler, "/socket");
    futures::executor::block_on(runtime.start_websocket(request))?;
    until("the plugin's task ticks", || {
        ticks.load(std::sync::atomic::Ordering::SeqCst) > 0
    })?;
    assert!(matches!(
        outbound_rx.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));

    let stale = runtime.clone();
    futures::executor::block_on(runtime.shutdown())?;
    // Nothing is left to tick: the counter's only other holder was the VM's native callback.
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

/// Returns once `reached`, which another thread brings about. Nothing is timed by this: it only
/// keeps a test from waiting for good on what never comes.
fn until(what: &str, reached: impl Fn() -> bool) -> Result<()> {
    let give_up = std::time::Instant::now() + Duration::from_secs(60);
    while !reached() {
        anyhow::ensure!(std::time::Instant::now() < give_up, "never saw {what}");
        std::thread::sleep(Duration::from_millis(1));
    }
    Ok(())
}

/// What an entrypoint that calls `wait()` of [`entrypoint_gate`]'s module reports and waits for.
struct EntrypointGate {
    reached: Arc<std::sync::atomic::AtomicUsize>,
    release: Arc<tokio::sync::Notify>,
}

impl EntrypointGate {
    /// How many entrypoints have reached their wait.
    fn reached(&self) -> usize {
        self.reached.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// A module whose `wait()` waits until released. Overrides replace a module, so it stands in
/// for `@harmony/crypt`, which these plugins do not otherwise use.
fn entrypoint_gate() -> (ModuleSpec, EntrypointGate) {
    let gate = EntrypointGate {
        reached: Arc::default(),
        release: Arc::default(),
    };
    let (reached, release) = (gate.reached.clone(), gate.release.clone());
    let module = ModuleSpec::new("harmony/crypt")
        .function(
            harmony_core::FunctionSpec::new("wait").call_async(Arc::new(move |_frame| {
                reached.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let release = release.clone();
                Ok(luau::ScheduledFuture::new(async move {
                    release.notified().await;
                    Ok(())
                }))
            })),
        )
        .install(|_| Ok(harmony_core::ModuleExport::new(())));
    (module, gate)
}

/// An executor on its own thread running `demo`, whose entrypoint returns at once until it is
/// rewritten.
fn gated_executor() -> Result<(TempPlugin, PluginExecutorHandle, EntrypointGate)> {
    let plugin = temp_plugin("demo", &["harmony.crypt"], "return nil")?;
    let (module, gate) = entrypoint_gate();
    let (runtime, errors) = PluginExecutorHandle::discover_from_plugins_dir_with_db_and_modules(
        plugin.plugins_dir(),
        default_server_info(),
        default_auth_capabilities(),
        lyra_db::DbAsync::new(lyra_db::test_db::new_test_db()?),
        vec![module],
        None,
    )?;
    assert!(errors.is_empty(), "{errors:?}");
    Ok((plugin, runtime, gate))
}

const WAITING_ENTRYPOINT: &str = r#"require("@harmony/crypt").wait()"#;

/// On the real clock and the real wait: the entrypoint waits for good and the executor is asleep,
/// so only the cancellation's wake ends the restart.
#[tokio::test(flavor = "multi_thread")]
async fn cancelling_entrypoints_ends_a_restart_whose_entrypoint_waits() -> Result<()> {
    let (plugin, runtime, gate) = gated_executor()?;
    runtime.exec_all().await?;

    plugin.write("init.luau", WAITING_ENTRYPOINT)?;
    let restart = tokio::spawn({
        let runtime = runtime.clone();
        async move { runtime.restart_plugin("demo").await }
    });
    until("the entrypoint wait", || gate.reached() == 1)?;
    until("the executor sleep", || runtime.is_asleep())?;
    assert!(!restart.is_finished());

    runtime.cancel_entrypoints();
    let error = tokio::time::timeout(Duration::from_secs(60), restart)
        .await??
        .expect_err("a cancelled entrypoint fails its restart");
    assert!(format!("{error:#}").contains("cancelled"), "{error:#}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_is_refused_only_while_another_runs() -> Result<()> {
    let (plugin, runtime, gate) = gated_executor()?;
    runtime.exec_all().await?;

    plugin.write("init.luau", WAITING_ENTRYPOINT)?;
    let restart = tokio::spawn({
        let runtime = runtime.clone();
        async move { runtime.restart_plugin("demo").await }
    });
    until("the entrypoint wait", || gate.reached() == 1)?;
    let error = runtime
        .restart_plugin("demo")
        .await
        .expect_err("a restart is running");
    assert!(
        format!("{error:#}").contains("already running"),
        "{error:#}"
    );
    assert_eq!(gate.reached(), 1, "the refused restart ran the entrypoint");

    // The entrypoint's wait ending is all that wakes the sleeping executor to answer.
    until("the executor sleep", || runtime.is_asleep())?;
    gate.release.notify_one();
    tokio::time::timeout(Duration::from_secs(60), restart).await???;
    plugin.write("init.luau", "return nil")?;
    tokio::time::timeout(Duration::from_secs(60), runtime.restart_plugin("demo")).await??;
    Ok(())
}

#[test]
fn plugin_executor_handle_restart_runs_new_entrypoint_on_runtime_thread() -> Result<()> {
    let plugin = temp_plugin("demo", &[], "return nil")?;

    let db = lyra_db::test_db::new_test_db()?;
    let db = lyra_db::DbAsync::new(db);
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
    let hidden_track = public_id(&db, hidden_track)?;
    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.playlists"])]),
        default_server_info(),
        lyra_db::DbAsync::new(db),
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
                local id = playlists.create({{ name = "Viewer" }}).id
                local added = pcall(playlists.add_tracks, id, {{ "{hidden_track}" }})
                local replaced = pcall(playlists.replace, id, {{ track_ids = {{ "{hidden_track}" }} }})
                return added, replaced, #playlists.entries({{ id }})[id]
            "#,
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
            lyra_db::DbAsync::new(self.db),
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
                lyra_db::covers::CoverSource::Local,
                lyra_db::Cover {
                    db_id: None,
                    id: lyra_db::nanoid(),
                    path: image.path().to_string_lossy().into_owned(),
                    mime_type: "image/png".into(),
                    hash: "hash".into(),
                    blurhash: None,
                    provider_id: None,
                },
            )?;
            Ok(())
        })?;
        releases.push(public_id(&fixture.db, release)?);
    }
    let [shown, hidden] = &releases[..] else {
        unreachable!()
    };
    let values = fixture.eval(
        &["lyra.covers"],
        format!(
            r#"
                local covers = require("@lyra/covers")
                local missing = "missing"
                local found = covers.get({{ "{shown}", "{hidden}", missing }})
                return found["{shown}"] ~= nil, found["{hidden}"] == nil, found[missing] == nil
            "#
        ),
    )?;
    assert_eq!(values, vec![luau::Value::Boolean(true); 3]);
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
    lyra_db::fixtures::link_track(&mut database, release, track)?;
    let image = tempfile::NamedTempFile::new()?;
    database.transaction_mut(|t| -> Result<()> {
        lyra_db::covers::upsert(
            t,
            release,
            lyra_db::covers::CoverSource::Local,
            lyra_db::Cover {
                db_id: None,
                id: lyra_db::nanoid(),
                path: image.path().to_string_lossy().into_owned(),
                mime_type: "image/png".into(),
                hash: "hash".into(),
                blurhash: None,
                provider_id: None,
            },
        )?;
        Ok(())
    })?;
    let playlist = lyra_db::playlists::create(
        &mut database,
        &lyra_db::Playlist {
            db_id: None,
            id: lyra_db::nanoid(),
            name: "Private".into(),
            description: None,
            is_public: Some(false),
            created_at_ms: None,
            updated_at_ms: None,
        },
        owner,
    )?;
    database.transaction_mut(|t| lyra_db::playlists::add_track(t, playlist, track))?;
    lyra_db::covers::display::sync_playlist_cover(&mut database, playlist)?;
    let release = public_id(&database, release)?;
    let playlist = public_id(&database, playlist)?;

    futures::executor::block_on(crate::plugins::api::initialize_registry(
        std::collections::HashSet::new(),
        None,
    ));
    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.api", "lyra.covers"])]),
        default_server_info(),
        lyra_db::DbAsync::new(database),
    )?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local api = require("@lyra/api")
            local covers = require("@lyra/covers")

            api.get("/artwork/{release}/{playlist}", function(ctx)
                local release = ctx.params.release
                local playlist = ctx.params.playlist
                return api.response.json(200, {
                    cover = covers.artwork({ release })[release] ~= nil,
                    playlist = covers.artwork({ playlist })[playlist].owner_id == release,
                    scoped = pcall(covers.get, { release, playlist }),
                })
            end, { auth = "public" })
        "#[..],
    )?;

    let handler = futures::executor::block_on(crate::plugins::api::tests::registered_handler(
        "GET",
        "/artwork/{release}/{playlist}",
    ))
    .context("registered API handler")?;
    let result = runtime.dispatch_api_handler(ApiHandlerRequest {
        params: HashMap::from([
            ("release".to_string(), release.clone()),
            ("playlist".to_string(), playlist.clone()),
        ]),
        ..api_request(handler, &format!("/artwork/{release}/{playlist}"))
    })?;

    assert_eq!(result.status, 200);
    let Some(ApiResponseBody::Json(body)) = result.body else {
        anyhow::bail!("expected JSON response body");
    };
    assert_eq!(body["cover"], true);
    assert_eq!(body["playlist"], true);
    assert_eq!(body["scoped"], false);
    Ok(())
}

#[test]
fn streamable_bindings_serve_anonymous_public_routes() -> Result<()> {
    // api::install resets the shared API route registry; serialize with the
    // other global-registry mutators.
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let mut database = lyra_db::test_db::TestDb::initialized()?.into_inner();
    let directory = tempfile::tempdir()?;
    let library = lyra_db::test_db::insert_library(
        &mut database,
        "Library",
        directory.path().to_str().expect("UTF-8 temp path"),
    )?;
    let release = lyra_db::fixtures::insert_release(&mut database, "Release")?;
    let track = lyra_db::fixtures::insert_track(&mut database, "Track")?;
    lyra_db::fixtures::connect(&mut database, library, release)?;
    lyra_db::fixtures::link_track(&mut database, release, track)?;
    let audio = tempfile::Builder::new()
        .suffix(".flac")
        .tempfile_in(directory.path())?;
    let entry = lyra_db::Entry {
        db_id: None,
        id: lyra_db::nanoid(),
        full_path: audio.path().to_path_buf(),
        kind: lyra_db::entries::EntryKind::File,
        file_kind: Some("audio".into()),
        name: "track.flac".into(),
        hash: None,
        size: 1,
        file_modified_at_ms: 1,
        file_created_at_ms: 1,
        synced_at_ms: None,
        missing_at_ms: None,
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
    let track = public_id(&database, track)?;
    let release = public_id(&database, release)?;

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
        lyra_db::DbAsync::new(database),
    )?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local api = require("@lyra/api")
            local playback_sources = require("@lyra/playback_sources")
            local tracks = require("@lyra/tracks")

            api.get("/stream/{track}/{release}", function(ctx)
                local track = ctx.params.track
                local release = ctx.params.release
                local source = playback_sources.streamable(track, { include_entry = true })
                return api.response.json(200, {
                    track = tracks.streamable({ track, release })[track] ~= nil,
                    release = tracks.streamable({ release })[release] ~= nil,
                    entry_name = source and source.entry and source.entry.name,
                    full_path = source and source.entry and source.entry.full_path,
                    release_source = playback_sources.streamable(release) ~= nil,
                    scoped = pcall(tracks.get, { track }),
                    scoped_source = pcall(playback_sources.get, track),
                })
            end, { auth = "public" })
        "#[..],
    )?;

    let handler = futures::executor::block_on(crate::plugins::api::tests::registered_handler(
        "GET",
        "/stream/{track}/{release}",
    ))
    .context("registered API handler")?;
    let result = runtime.dispatch_api_handler(ApiHandlerRequest {
        params: HashMap::from([
            ("track".to_string(), track.clone()),
            ("release".to_string(), release.clone()),
        ]),
        ..api_request(handler, &format!("/stream/{track}/{release}"))
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
        releases.push(public_id(&fixture.db, release)?);
    }
    let label = lyra_db::labels::find_by_name(&fixture.db, "Label")?.context("label exists")?;
    let label = public_id(&fixture.db, label)?;
    let [shown, hidden] = &releases[..] else {
        unreachable!()
    };
    let values = fixture.eval(
        &["lyra.labels"],
        format!(
            r#"
                local labels = require("@lyra/labels")
                local releases = labels.get_releases("{label}")
                local many = labels.get_releases_many({{ "{label}" }})["{label}"]
                local by_release = labels.by_release({{ "{hidden}", "{shown}" }})
                return #releases, releases[1], #many, many[1],
                    by_release["{hidden}"] == nil, #by_release["{shown}"]
            "#,
        ),
    )?;
    let shown = luau::Value::String(shown.clone().into_bytes());
    assert_eq!(
        values,
        vec![
            luau::Value::Number(1.0),
            shown.clone(),
            luau::Value::Number(1.0),
            shown,
            luau::Value::Boolean(true),
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
            id: lyra_db::nanoid(),
            full_path: std::path::PathBuf::from(format!("/music/{name}.flac")),
            kind: lyra_db::entries::EntryKind::File,
            file_kind: Some("audio".into()),
            name: format!("{name}.flac"),
            hash: None,
            size: 1,
            file_modified_at_ms: 1,
            file_created_at_ms: 1,
            synced_at_ms: None,
            missing_at_ms: None,
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
        ids.push((public_id(&fixture.db, track)?, entry.id));
    }
    let [(shown, _), (hidden, hidden_entry)] = &ids[..] else {
        unreachable!()
    };
    let values = fixture.eval(
        &["lyra.track_sources", "lyra.chromaprint"],
        format!(
            r#"
                local track_sources = require("@lyra/track_sources")
                local chromaprint = require("@lyra/chromaprint")
                local _, fingerprint_error = pcall(chromaprint.compute, "{hidden_entry}")
                return track_sources.get_primary_source_key("{shown}"),
                    track_sources.get_primary_source_key("{hidden}") == nil,
                    string.find(tostring(fingerprint_error), "entry not found", 1, true) ~= nil
            "#
        ),
    )?;
    let text = |value: &str| luau::Value::String(value.as_bytes().to_vec());
    let yes = luau::Value::Boolean(true);
    assert_eq!(values, vec![text("shown"), yes.clone(), yes]);
    Ok(())
}

#[test]
fn api_responses_take_only_body_fallbacks_on_served_bodies() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    init_api_test_state()?;
    let runtime = runtime_with_scopes(&["lyra.api"])?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local api = require("@lyra/api")

            api.get("/fallback", function()
                return api.response.with_fallback(
                    api.response.stream_track("track"),
                    api.response.text(200, "refused", { ["x-fallback"] = "yes" })
                )
            end, { auth = "public" })

            api.get("/cover-fallback", function()
                return { kind = "stream_track", track_id = "track", fallback = { kind = "cover", cover_id = "x" } }
            end, { auth = "public" })

            api.get("/json-with-fallback", function()
                return { kind = "json", body = {}, fallback = api.response.empty(204) }
            end, { auth = "public" })
        "#[..],
    )?;
    let dispatch = |path: &str| {
        let handler = futures::executor::block_on(crate::plugins::api::tests::registered_handler(
            "GET", path,
        ))
        .context("registered API handler")?;
        runtime.dispatch_api_handler(api_request(handler, path))
    };

    let response = dispatch("/fallback")?;
    assert_eq!(response.kind, ApiResponseKind::StreamTrack);
    let fallback = response.fallback.context("the fallback is kept")?;
    assert_eq!(fallback.kind, ApiResponseKind::Text);
    assert_eq!(fallback.status, 200);
    assert!(
        fallback
            .headers
            .contains(&("x-fallback".to_string(), "yes".to_string()))
    );
    assert!(matches!(fallback.body, Some(ApiResponseBody::Bytes(ref body)) if body == b"refused"));

    let error = dispatch("/cover-fallback").expect_err("a cover fallback is refused");
    assert!(
        error
            .to_string()
            .contains("must be a json, empty, text or bytes response"),
        "{error}"
    );
    let error = dispatch("/json-with-fallback").expect_err("a json response takes no fallback");
    assert!(error.to_string().contains("take no fallback"), "{error}");
    Ok(())
}

#[tokio::test]
async fn a_crash_closes_the_websockets_of_its_runtime() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    init_api_test_state()?;
    let (handle, _) = PluginExecutorHandle::spawn(|| {
        let runtime = runtime_with_scopes(&["lyra.api"])?;
        runtime.run_plugin_source(
            "demo",
            "init.luau",
            &br#"
                local api = require("@lyra/api")

                api.websocket("/socket", function(reader)
                    while reader:recv() do
                    end
                end, { auth = "public" })
            "#[..],
        )?;
        Ok((runtime, Vec::new()))
    })?;
    let handler = crate::plugins::api::tests::registered_handler("GET", "/socket")
        .await
        .context("registered websocket handler")?;
    let (request, _inbound_tx, _outbound_rx) = websocket_request(handler, "/socket");
    let state = request.state.clone();
    handle.start_websocket(request).await?;

    handle
        .inject_panic()
        .await
        .expect_err("a panicking executor drops the reply");
    tokio::time::timeout(Duration::from_secs(5), state.closed())
        .await
        .context("the crashed runtime's websocket was never closed")?;
    Ok(())
}

#[tokio::test]
async fn nested_calls_wait_by_their_chains_deadline() -> Result<()> {
    use crate::services::plugin_dispatch::CallSource;
    use crate::services::state::Transition;

    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let runtime = PluginExecutor::with_manifests(Arc::from(Vec::new()))?;
    let generation = crate::services::STATE.generation();
    let _transition = generation.begin_transition(Transition::Plugin {
        plugin: crate::services::plugin_scope::PluginId::new("target")?,
        targets: Arc::new(std::collections::HashSet::from(["provider".to_string()])),
    });

    let mut context = CallContext::default();
    context.caller.insert(super::CallChain::root(
        std::time::Instant::now() + Duration::from_millis(60),
        None,
    ));
    let chained = super::VmCalls::of(&runtime.vm, &context)?;
    let started = std::time::Instant::now();
    assert!(chained.reach("provider").await.is_none());
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the chain's deadline bounds the wait"
    );

    let unchained = super::VmCalls::of(&runtime.vm, &CallContext::default())?;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), unchained.reach("provider"))
            .await
            .is_err(),
        "a task with no handler in its chain has the interactive budget"
    );
    Ok(())
}

/// What is waited for comes about with nothing waking the executor, as when a wake goes missing:
/// the sleep runs into the watchdog, and no round or check that would find it done may follow.
#[test]
fn a_missed_wake_fails_a_served_wait_at_its_watchdog() -> Result<()> {
    let runtime = PluginExecutor::with_manifests(Arc::from(Vec::new()))?;
    let mut asked = 0;

    let error = runtime
        .run_until_within(Duration::from_millis(50), "the second ask", || {
            asked += 1;
            (asked > 1).then_some(())
        })
        .expect_err("the watchdog's own round rescued the missed wake");
    assert!(error.to_string().contains("nothing woke"), "{error:#}");
    Ok(())
}

#[tokio::test]
async fn a_chain_past_its_deadline_holds_no_exemptions() -> Result<()> {
    use crate::services::plugin_dispatch::CallSource;
    use crate::services::state::Transition;

    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_default_test_state()?;
    let runtime = PluginExecutor::with_manifests(Arc::from(Vec::new()))?;
    let generation = crate::services::STATE.generation();
    let _transition = generation.begin_transition(Transition::Plugin {
        plugin: crate::services::plugin_scope::PluginId::new("target")?,
        targets: Arc::new(std::collections::HashSet::from(["provider".to_string()])),
    });
    let chained = |deadline: std::time::Instant| -> Result<super::VmCalls> {
        let mut context = CallContext::default();
        context
            .caller
            .insert(super::CallChain::root(deadline, Some(Arc::from("target"))));
        Ok(super::VmCalls::of(&runtime.vm, &context)?)
    };

    let live = chained(std::time::Instant::now() + Duration::from_secs(30))?;
    assert!(
        live.reach("provider").await.is_some(),
        "a chain inside its deadline is exempt from its own plugin's transition"
    );

    let expired = chained(std::time::Instant::now() - Duration::from_millis(1))?;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), expired.reach("provider"))
            .await
            .is_err(),
        "past its deadline the task waits like one with no chain"
    );
    Ok(())
}
