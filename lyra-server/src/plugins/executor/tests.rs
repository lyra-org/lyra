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

fn default_auth_capabilities() -> crate::plugins::auth::AuthCapabilities {
    crate::plugins::auth::AuthCapabilities {
        enabled: false,
        allow_default_login_when_disabled: true,
        default_username: "default".to_string(),
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
        db: crate::plugins::db::DbAsync,
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
        db: crate::plugins::db::DbAsync,
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
        crate::services::auth::Principal {
            user_db_id: agdb::DbId(7),
            user_public_id: "user-public-id".to_string(),
            username: "raw-user".to_string(),
            permissions: vec![crate::plugins::db::Permission::Admin],
            role_name: Some("admin".to_string()),
            accessible_library_ids: std::collections::HashSet::new(),
        },
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
fn plugin_executor_scopes_personal_lyrics_to_the_dispatch_principal() -> Result<()> {
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
    crate::testing::init_default_test_state()?;

    let (
        alice_db_id,
        alice_public_id,
        bob_db_id,
        bob_public_id,
        library_public_id,
        track_db_id,
        track_public_id,
    ) = futures::executor::block_on(async {
        let mut db = crate::STATE.db.write().await;
        let alice = crate::plugins::db::test_db::test_user("lyrics-alice")?;
        let alice_public_id = alice.id.clone();
        let alice_db_id = crate::plugins::db::users::create(&mut db, &alice)?;
        let bob = crate::plugins::db::test_db::test_user("lyrics-bob")?;
        let bob_public_id = bob.id.clone();
        let bob_db_id = crate::plugins::db::users::create(&mut db, &bob)?;
        let library_db_id = crate::plugins::db::test_db::insert_library(
            &mut db,
            "Plugin Lyrics",
            "/tmp/plugin-lyrics",
        )?;
        let library_public_id = crate::plugins::db::lookup::find_id_by_db_id(&db, library_db_id)?
            .context("inserted library has public id")?;
        let track_db_id =
            crate::plugins::db::test_db::insert_track(&mut db, "Plugin Lyrics Track")?;
        let track_public_id = crate::plugins::db::lookup::find_id_by_db_id(&db, track_db_id)?
            .context("inserted track has public id")?;
        crate::plugins::db::test_db::connect(&mut db, library_db_id, track_db_id)?;
        crate::plugins::db::providers::upsert(
            &mut db,
            &crate::plugins::db::ProviderConfig {
                db_id: None,
                provider_id: "test_provider".to_string(),
                display_name: "Test Provider".to_string(),
                priority: 10,
                enabled: true,
            },
        )?;
        crate::plugins::db::lyrics::upsert_from_plugin(
            &mut db,
            track_db_id,
            crate::plugins::db::lyrics::LyricsInput {
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

    let runtime = runtime_with_scopes(&["lyra.lyrics"])?;
    let principal =
        |user_db_id, user_public_id: String, can_access: bool| crate::services::auth::Principal {
            user_db_id,
            user_public_id,
            username: "lyrics-user".to_string(),
            permissions: Vec::new(),
            role_name: None,
            accessible_library_ids: can_access
                .then(|| std::collections::HashSet::from([library_public_id.clone()]))
                .unwrap_or_default(),
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

    let remaining_personal_owners = futures::executor::block_on(async {
        let db = crate::STATE.db.read().await;
        Ok::<_, anyhow::Error>(
            crate::plugins::db::lyrics::get_for_track(&db, track_db_id)?
                .into_iter()
                .filter_map(|lyrics| lyrics.owner_user_id)
                .collect::<Vec<_>>(),
        )
    })?;
    assert_eq!(remaining_personal_owners, vec![bob_public_id]);
    Ok(())
}

#[test]
fn plugin_executor_declares_metadata_provider_ids_and_options() -> Result<()> {
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
    crate::testing::init_default_test_state()?;
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
        futures::executor::block_on(crate::services::providers::provider_registry().read_owned());
    let (id_spec, has_generator) = crate::services::providers::registry_tests::id_registration(
        &registry,
        "raw-provider",
        "release_id",
    )
    .context("provider id registration")?;
    assert_eq!(id_spec.id_type, "release_id");
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
fn plugin_executor_registers_similar_releases_handler() -> Result<()> {
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
    crate::testing::init_default_test_state()?;
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
fn plugin_executor_rejects_invalid_similar_releases_config() -> Result<()> {
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
    crate::testing::init_default_test_state()?;
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
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
    crate::testing::init_default_test_state()?;
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
    let handler_id = {
        let registry = futures::executor::block_on(
            crate::services::providers::provider_registry().read_owned(),
        );
        registry
            .get_similar_releases_handler("bounded-similar-provider")
            .context("similar releases handler")?
            .handler
            .handler_id
    };

    let result = runtime.dispatch_similar_releases(SimilarReleasesDispatchRequest {
        provider_id: "bounded-similar-provider".to_string(),
        handler_id,
        context: serde_json::json!({}),
        timeout: std::time::Duration::from_secs(1),
        cancellation: MetadataRefreshCancellation::default(),
        max_candidates: 3,
    })?;

    assert_eq!(result.candidates.len(), 3);
    Ok(())
}

#[test]
fn releases_similar_dispatches_provider_on_current_executor() -> Result<()> {
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
    crate::testing::init_default_test_state()?;
    let (seed, candidate, candidate_public_id) = futures::executor::block_on(async {
        let mut db = crate::STATE.db.write().await;
        let seed = crate::plugins::db::test_db::insert_release(&mut db, "Seed")?;
        let candidate = crate::plugins::db::test_db::insert_release(&mut db, "Candidate")?;
        let public_id = crate::plugins::db::releases::get_by_id(&db, candidate)?
            .context("candidate release")?
            .id;
        Ok::<_, anyhow::Error>((seed, candidate, public_id))
    })?;
    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.metadata", "lyra.releases"])]),
        default_server_info(),
        crate::STATE.db.get(),
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

    let values = runtime.eval_plugin_source("demo", "init.luau", source.as_bytes())?;

    assert_eq!(
        values,
        vec![luau::Value::String(candidate_public_id.into_bytes())]
    );
    Ok(())
}

#[test]
fn awaited_background_similar_provider_result_survives_executor_polling() -> Result<()> {
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
    crate::testing::init_default_test_state()?;
    let (seed, candidate, candidate_public_id) = futures::executor::block_on(async {
        let mut db = crate::STATE.db.write().await;
        let seed = crate::plugins::db::test_db::insert_release(&mut db, "Seed")?;
        let candidate = crate::plugins::db::test_db::insert_release(&mut db, "Candidate")?;
        let public_id = crate::plugins::db::releases::get_by_id(&db, candidate)?
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
        crate::STATE.db.get(),
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
    runtime.eval_plugin_source("demo", "init.luau", source.as_bytes())?;

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
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
    crate::testing::init_default_test_state()?;
    let seed = futures::executor::block_on(async {
        let mut db = crate::STATE.db.write().await;
        crate::plugins::db::test_db::insert_release(&mut db, "Seed")
    })?;
    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest(
            "demo",
            &["harmony.task", "lyra.metadata", "lyra.releases"],
        )]),
        default_server_info(),
        crate::STATE.db.get(),
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
    runtime.eval_plugin_source("demo", "init.luau", source.as_bytes())?;
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
    // api::install resets the shared API route registry; serialize with the
    // other global-registry mutators.
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
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
        client_key: None,
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
fn plugin_executor_binds_host_resolved_principal_to_api_responses() -> Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    let _guard = rt.block_on(crate::testing::runtime_test_lock());
    let test_dir = std::env::temp_dir().join(format!(
        "lyra-dispatch-auth-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    std::fs::create_dir_all(&test_dir)?;
    rt.block_on(crate::testing::initialize_runtime(
        &crate::testing::LibraryFixtureConfig {
            directory: test_dir.clone(),
            language: None,
            country: None,
        },
    ))?;

    let (user_db_id, token) = rt.block_on(async {
        let user_db_id = {
            let mut db = crate::STATE.db.write().await;
            crate::plugins::db::users::create(
                &mut db,
                &crate::plugins::db::test_db::test_user("dispatch-auth-user")?,
            )?
        };
        let session = crate::services::auth::sessions::create_session_for_user(
            user_db_id,
            Default::default(),
        )
        .await?;
        Ok::<_, anyhow::Error>((user_db_id, session.token))
    })?;

    let _ = rt.block_on(crate::plugins::api::install(
        axum::Router::new(),
        std::collections::HashSet::new(),
    ))?;
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

    let request = |handler_id: u64, path: &str| ApiHandlerRequest {
        handler_id,
        plugin_id: "demo".to_string(),
        method: "GET".to_string(),
        path: path.to_string(),
        headers: Vec::new(),
        query: HashMap::new(),
        params: HashMap::new(),
        body: Vec::new(),
        auth: None,
        client_key: None,
    };

    let whoami_handler = rt
        .block_on(crate::plugins::api::tests::registered_handler(
            "GET", "/whoami",
        ))
        .context("registered /whoami handler")?;
    let response = runtime.dispatch_api_handler(request(whoami_handler, "/whoami"))?;
    let principal = response
        .principal
        .context("resolve_auth during dispatch should bind the principal to the response")?;
    assert_eq!(principal.user_db_id, user_db_id);

    let anon_handler = rt
        .block_on(crate::plugins::api::tests::registered_handler(
            "GET", "/anon",
        ))
        .context("registered /anon handler")?;
    let response = runtime.dispatch_api_handler(request(anon_handler, "/anon"))?;
    assert!(
        response.principal.is_none(),
        "dispatches that never resolve auth must not carry a principal"
    );

    let boundary_auth = rt
        .block_on(crate::services::auth::resolve_auth_from_bearer(Some(
            &token,
        )))?
        .context("bearer token should resolve at the boundary")?;
    let mut seeded = request(anon_handler, "/anon");
    seeded.auth = Some(boundary_auth);
    let response = runtime.dispatch_api_handler(seeded)?;
    let principal = response
        .principal
        .context("boundary auth should seed the dispatch principal")?;
    assert_eq!(principal.user_db_id, user_db_id);

    let _ = std::fs::remove_dir_all(test_dir);
    Ok(())
}

#[test]
fn plugin_executor_dispatches_registered_websocket_handler() -> Result<()> {
    // api::install resets the shared API route registry; serialize with the
    // other global-registry mutators.
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
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
    // api::install resets the shared API route registry; serialize with the
    // other global-registry mutators.
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
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
        client_key: None,
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
        crate::services::mix::mix_registry()
            .read_owned()
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
        default_auth_capabilities(),
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
