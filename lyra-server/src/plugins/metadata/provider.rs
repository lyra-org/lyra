// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashSet,
    sync::Arc,
};

use anyhow::Context;
use harmony_core::FunctionSpec;
use harmony_luau as luau;

use crate::plugins::args;
use crate::plugins::db;
use crate::services::plugin_dispatch::MetadataRefreshRequest;
use crate::services::plugin_scope::PluginId;
use crate::services::{
    EntityType,
    metadata::{
        layers::{
            EnsureArtistError,
            ProviderArtist,
            ensure_provider_artist,
        },
        lyrics::{
            providers as lyrics_dispatcher,
            providers::RegisteredHandler,
        },
    },
    providers::{
        ProviderCallbackHandle,
        ProviderCoverSpec,
        ProviderIdUrlGenerator,
        ProviderSimilarReleasesSpec,
        provider_registry,
    },
};

use super::MetadataCallbackRegistry;
use super::layer::MetadataLayer;
use super::parsing::{
    core_call_context,
    ensure_provider_owner,
    entity_type_for_node,
    optional_artist_type,
    optional_table_string,
    parse_cover_spec,
    parse_id_spec,
    parse_id_url_template,
    parse_lyrics_spec,
    parse_option_declaration,
    parse_similar_releases_spec,
    required_table_string,
    string_array_from_table,
};

pub(super) fn provider_new_spec() -> FunctionSpec {
    FunctionSpec::async_fn("Provider.new")
        .named_arg::<String>("id")
        .named_arg::<Option<luau::Table>>("options")
        .returns::<MetadataProvider>()
        .call_async(Arc::new(provider_new_callback))
}

fn provider_new_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let provider_id: String = frame.args.read_named("id")?;
    let options: Option<luau::Table> = frame.args.read_optional_named("options")?;
    let display_name = match &options {
        Some(options) => {
            optional_table_string(frame.vm, options, "display_name", "metadata.Provider.new")?
        }
        None => None,
    };
    let plugin_id = frame.context.origin.plugin.clone().ok_or_else(|| {
        crate::plugins::runtime_error("metadata.Provider.new must be called from plugin Lua code")
    })?;
    let plugin_id = PluginId::new(plugin_id.to_string()).map_err(crate::plugins::runtime_error)?;
    let db = db::get_optional(frame.vm)?;

    let vm = frame.vm.clone();
    let origin = frame.context.origin.clone();
    Ok(luau::ScheduledFuture::new(async move {
        let generation = crate::plugins::STATE.generation();
        let _registration = generation
            .registries
            .ensure_registrations_open(&plugin_id)
            .await
            .map_err(crate::plugins::runtime_error)?;
        let mut registry = provider_registry().write_owned().await;
        registry
            .register(plugin_id.clone(), provider_id.clone(), display_name)
            .map_err(crate::plugins::runtime_error)?;
        drop(registry);

        if let Some(db) = db {
            let mut db_write = db.write().await;
            lyra_db::providers::register(&mut db_write, &provider_id)
                .map_err(crate::plugins::runtime_error)?;
        }

        let provider = MetadataProvider {
            plugin_id,
            provider_id,
        };
        let provider =
            MetadataProvider::_harmony_userdata_class().create_value(&vm, &origin, provider)?;
        Ok(provider)
    }))
}

#[harmony_macros::userdata(
    name = "Provider",
    description = "Metadata provider registration object."
)]
#[derive(Clone)]
pub(super) struct MetadataProvider {
    plugin_id: PluginId,
    provider_id: String,
}

#[harmony_macros::userdata_methods]
impl MetadataProvider {
    fn id(
        &self,
        vm: &luau::Vm,
        context: &luau::CallContext,
        spec: luau::Table,
        generator: Option<luau::Value>,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        provider_id_callback(self, vm, context, spec, generator)
    }

    fn search(
        &self,
        vm: &luau::Vm,
        context: &luau::CallContext,
        entity: crate::plugins::types::LuauEntityType,
        handler: luau::Function,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        search_callback(self, vm, context, entity.into(), handler)
    }

    fn cover(
        &self,
        vm: &luau::Vm,
        context: &luau::CallContext,
        entity: crate::plugins::types::LuauEntityType,
        config: luau::Table,
        handler: luau::Function,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        cover_callback(self, vm, context, entity.into(), config, handler)
    }

    fn lyrics(
        &self,
        vm: &luau::Vm,
        context: &luau::CallContext,
        config: luau::Table,
        handler: luau::Function,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        lyrics_callback(self, vm, context, config, handler)
    }

    fn similar_releases(
        &self,
        vm: &luau::Vm,
        context: &luau::CallContext,
        config: luau::Table,
        handler: luau::Function,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        similar_releases_callback(self, vm, context, config, handler)
    }

    fn refresh(
        &self,
        vm: &luau::Vm,
        context: &luau::CallContext,
        entity: crate::plugins::types::LuauEntityType,
        handler: luau::Function,
        filter: Option<luau::Function>,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        refresh_callback(self, vm, context, entity.into(), handler, filter)
    }

    fn declare_option(
        &self,
        vm: &luau::Vm,
        context: &luau::CallContext,
        config: luau::Table,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        declare_option_callback(self, vm, context, config)
    }

    #[harmony(returns(i64))]
    fn ensure_artist(
        &self,
        vm: &luau::Vm,
        context: &luau::CallContext,
        request: luau::Table,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        ensure_artist_callback(self, vm, context, request)
    }

    fn mark_unmatched(
        &self,
        vm: &luau::Vm,
        context: &luau::CallContext,
        node_id: i64,
        id_types: luau::Table,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        mark_unmatched_callback(self, vm, context, node_id, id_types)
    }

    #[harmony(returns(MetadataLayer))]
    fn layer(
        &self,
        vm: &luau::Vm,
        context: &luau::CallContext,
        origin: &luau::ChunkOrigin,
        node_id: i64,
    ) -> luau::runtime::Result<luau::Value> {
        layer_callback(self, vm, context, origin, node_id)
    }
}

fn provider_id_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    spec: luau::Table,
    generator: Option<luau::Value>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    ensure_provider_owner(context, &provider.plugin_id, &provider.provider_id)?;

    let id_spec = parse_id_spec(vm, &spec)?;
    let generator = match generator {
        Some(luau::Value::String(bytes)) => {
            Some(IdGenerator::Template(parse_id_url_template(bytes)?))
        }
        Some(luau::Value::Function(function)) => Some(IdGenerator::Function(function)),
        Some(luau::Value::Nil) | None => None,
        Some(other) => {
            return Err(crate::plugins::runtime_error(format!(
                "provider:id generator must be a function, string template, or nil; got {}",
                other.type_name()
            )));
        }
    };
    let handlers = vm.data().get::<MetadataCallbackRegistry>()?;
    let context = core_call_context(context);
    let vm_id = vm.id();
    let provider = provider.clone();
    Ok(luau::ScheduledFuture::new(async move {
        let generation = crate::plugins::STATE.generation();
        let _registration = generation
            .registries
            .ensure_registrations_open(&provider.plugin_id)
            .await
            .map_err(crate::plugins::runtime_error)?;
        let mut registry = provider_registry().write_owned().await;
        let entity = id_spec.entity;
        let replaced = registry
            .set_id_registration(&provider.provider_id, id_spec, || {
                generator.map(|generator| match generator {
                    IdGenerator::Template(template) => ProviderIdUrlGenerator::Template(template),
                    IdGenerator::Function(function) => ProviderIdUrlGenerator::Function {
                        handler: ProviderCallbackHandle {
                            handler_id: handlers.register(
                                provider.provider_id.clone(),
                                entity,
                                function,
                                context,
                            ),
                        },
                        vm_id,
                    },
                })
            })
            .map_err(|err| crate::plugins::runtime_error(format!("provider:id: {err}")))?;
        if let Some(ProviderIdUrlGenerator::Function {
            handler,
            vm_id: replaced_vm_id,
        }) = replaced
            && replaced_vm_id == vm_id
        {
            handlers.remove(handler.handler_id);
        }
        Ok(())
    }))
}

enum IdGenerator {
    Template(String),
    Function(luau::Function),
}

fn declare_option_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    config: luau::Table,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    ensure_provider_owner(context, &provider.plugin_id, &provider.provider_id)?;
    let option = parse_option_declaration(vm, &config)?;

    let provider = provider.clone();
    Ok(luau::ScheduledFuture::new(async move {
        let generation = crate::plugins::STATE.generation();
        let _registration = generation
            .registries
            .ensure_registrations_open(&provider.plugin_id)
            .await
            .map_err(crate::plugins::runtime_error)?;
        let mut registry = provider_registry().write_owned().await;
        registry
            .declare_option(&provider.provider_id, option)
            .map_err(crate::plugins::runtime_error)?;
        Ok(())
    }))
}

fn search_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    entity_type: EntityType,
    handler: luau::Function,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    ensure_provider_owner(context, &provider.plugin_id, &provider.provider_id)?;
    let handlers = vm.data().get::<MetadataCallbackRegistry>()?;
    let context = core_call_context(context);
    let provider = provider.clone();
    Ok(luau::ScheduledFuture::new(async move {
        let generation = crate::plugins::STATE.generation();
        let _registration = generation
            .registries
            .ensure_registrations_open(&provider.plugin_id)
            .await
            .map_err(crate::plugins::runtime_error)?;
        let mut registry = provider_registry().write_owned().await;
        let handler_id =
            handlers.register(provider.provider_id.clone(), entity_type, handler, context);

        registry.set_search_callback(
            &provider.provider_id,
            entity_type,
            ProviderCallbackHandle { handler_id },
        );
        Ok(())
    }))
}

fn cover_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    entity_type: EntityType,
    config: luau::Table,
    handler: luau::Function,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    ensure_provider_owner(context, &provider.plugin_id, &provider.provider_id)?;
    if !matches!(entity_type, EntityType::Release | EntityType::Artist) {
        return Err(crate::plugins::runtime_error(
            "provider:cover entity_type must be EntityType.Release or EntityType.Artist",
        ));
    }

    let (timeout, require) = parse_cover_spec(vm, &config)?;
    let handlers = vm.data().get::<MetadataCallbackRegistry>()?;
    let context = core_call_context(context);
    let provider = provider.clone();
    Ok(luau::ScheduledFuture::new(async move {
        let generation = crate::plugins::STATE.generation();
        let _registration = generation
            .registries
            .ensure_registrations_open(&provider.plugin_id)
            .await
            .map_err(crate::plugins::runtime_error)?;
        if !harmony_http::has_rate_limit_for_plugin(provider.plugin_id.as_ref()).await {
            return Err(crate::plugins::runtime_error(format!(
                "provider:cover requires http.set_rate_limit to be configured for at least one domain before registration; call set_rate_limit in plugin init for plugin '{}'",
                provider.plugin_id
            )));
        }
        let mut registry = provider_registry().write_owned().await;
        let handler_id =
            handlers.register(provider.provider_id.clone(), entity_type, handler, context);

        registry.set_cover_handler(
            &provider.provider_id,
            entity_type,
            ProviderCoverSpec {
                timeout,
                require,
                handler: ProviderCallbackHandle { handler_id },
            },
        );
        Ok(())
    }))
}

fn lyrics_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    config: luau::Table,
    handler: luau::Function,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    ensure_provider_owner(context, &provider.plugin_id, &provider.provider_id)?;
    validate_lyrics_provider_id(&provider.provider_id)?;

    let (timeout, require) = parse_lyrics_spec(vm, &config)?;
    let handlers = vm.data().get::<MetadataCallbackRegistry>()?;
    let context = core_call_context(context);
    let provider = provider.clone();
    Ok(luau::ScheduledFuture::new(async move {
        let generation = crate::plugins::STATE.generation();
        let _registration = generation
            .registries
            .ensure_registrations_open(&provider.plugin_id)
            .await
            .map_err(crate::plugins::runtime_error)?;
        if !harmony_http::has_rate_limit_for_plugin(provider.plugin_id.as_ref()).await {
            return Err(crate::plugins::runtime_error(format!(
                "provider:lyrics requires http.set_rate_limit to be configured for at least one domain before registration; call set_rate_limit in plugin init for plugin '{}'",
                provider.plugin_id
            )));
        }
        let handler_id = handlers.register(
            provider.provider_id.clone(),
            EntityType::Track,
            handler,
            context,
        );
        let provider_id = provider.provider_id.clone();
        let provider_id_for_handler = provider_id.clone();
        let plugin_id = provider.plugin_id.clone();
        let handler_fn: lyrics_dispatcher::HandlerFn = Arc::new(move |context| {
            let provider_id = provider_id_for_handler.clone();
            Box::pin(async move {
                let runtime = crate::services::STATE
                    .generation()
                    .plugin_dispatch
                    .get()
                    .context("plugin runtime is not initialized")?;
                let context = lyrics_dispatcher::track_context_to_json(&context);
                let result = runtime
                    .dispatch_metadata_refresh(MetadataRefreshRequest {
                        handler_id,
                        context,
                        deadline: std::time::Instant::now() + timeout,
                    })
                    .await
                    .with_context(|| {
                        format!("provider lyrics handler failed for '{provider_id}'")
                    })?;
                let value =
                    result.values.into_iter().next().ok_or_else(|| {
                        anyhow::anyhow!("provider lyrics handler returned no value")
                    })?;
                lyrics_dispatcher::parse_handler_result(value)
            })
        });

        let cancel = lyrics_dispatcher::make_plugin_cancellation_child(&plugin_id).await;
        lyrics_dispatcher::register_handler(RegisteredHandler {
            provider_id: Arc::from(provider_id.as_str()),
            plugin_id,
            timeout,
            require,
            handler: handler_fn,
            cancel,
        })
        .await;
        Ok(())
    }))
}

fn similar_releases_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    config: luau::Table,
    handler: luau::Function,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    ensure_provider_owner(context, &provider.plugin_id, &provider.provider_id)?;
    let (timeout, require) = parse_similar_releases_spec(vm, &config)?;
    let handlers = vm.data().get::<MetadataCallbackRegistry>()?;
    let context = core_call_context(context);
    let provider = provider.clone();
    Ok(luau::ScheduledFuture::new(async move {
        let generation = crate::plugins::STATE.generation();
        let _registration = generation
            .registries
            .ensure_registrations_open(&provider.plugin_id)
            .await
            .map_err(crate::plugins::runtime_error)?;
        let mut registry = provider_registry().write_owned().await;
        let handler_id = handlers.register(
            provider.provider_id.clone(),
            EntityType::Release,
            handler,
            context,
        );

        registry.set_similar_releases_handler(
            &provider.provider_id,
            ProviderSimilarReleasesSpec {
                timeout,
                require,
                handler: ProviderCallbackHandle { handler_id },
            },
        );
        Ok(())
    }))
}

fn validate_lyrics_provider_id(provider_id: &str) -> luau::runtime::Result<()> {
    lyra_db::lyrics::validate_provider_id(provider_id).map_err(crate::plugins::runtime_error)
}

fn refresh_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    entity_type: EntityType,
    handler: luau::Function,
    filter: Option<luau::Function>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    ensure_provider_owner(context, &provider.plugin_id, &provider.provider_id)?;
    let handlers = vm.data().get::<MetadataCallbackRegistry>()?;
    let context = core_call_context(context);
    let provider = provider.clone();
    Ok(luau::ScheduledFuture::new(async move {
        let generation = crate::plugins::STATE.generation();
        let _registration = generation
            .registries
            .ensure_registrations_open(&provider.plugin_id)
            .await
            .map_err(crate::plugins::runtime_error)?;
        let mut registry = provider_registry().write_owned().await;
        let handler_id = handlers.register(
            provider.provider_id.clone(),
            entity_type,
            handler,
            context.clone(),
        );
        let filter_id = filter.map(|filter| {
            handlers.register(provider.provider_id.clone(), entity_type, filter, context)
        });

        registry.set_refresh_callback(
            &provider.provider_id,
            entity_type,
            ProviderCallbackHandle { handler_id },
        );
        if let Some(handler_id) = filter_id {
            registry.set_sync_filter_callback(
                &provider.provider_id,
                entity_type,
                ProviderCallbackHandle { handler_id },
            );
        }
        Ok(())
    }))
}

fn ensure_artist_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    request: luau::Table,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    ensure_provider_owner(context, &provider.plugin_id, &provider.provider_id)?;
    let artist = ProviderArtist {
        id_type: required_table_string(vm, &request, "id_type", "provider:ensure_artist")?,
        id_value: required_table_string(vm, &request, "id_value", "provider:ensure_artist")?,
        artist_name: optional_table_string(vm, &request, "artist_name", "provider:ensure_artist")?,
        sort_name: optional_table_string(vm, &request, "sort_name", "provider:ensure_artist")?,
        artist_type: optional_artist_type(vm, &request, "artist_type")?,
        description: optional_table_string(vm, &request, "description", "provider:ensure_artist")?,
    };
    let provider_id = provider.provider_id.clone();
    let db = db::get_optional(vm)?;
    Ok(luau::ScheduledFuture::new(async move {
        let is_registered_artist_id = {
            let registry = provider_registry().read_owned().await;
            registry.id_spec_matches_entity(&provider_id, &artist.id_type, EntityType::Artist)
        };
        if !is_registered_artist_id {
            return Err(crate::plugins::runtime_error(format!(
                "provider:ensure_artist: id_type '{}' is not registered for artist on provider '{provider_id}'",
                artist.id_type
            )));
        }

        let Some(db) = db else {
            return Err(crate::plugins::runtime_error(
                "provider:ensure_artist requires a database-backed plugin executor",
            ));
        };
        let mut db_write = db.write().await;
        let artist_db_id = ensure_provider_artist(&mut db_write, &provider_id, artist).map_err(
            |error| match error {
                EnsureArtistError::ScannedTypeConflict { .. } => {
                    crate::plugins::runtime_error(format!("provider:ensure_artist: {error}"))
                }
                EnsureArtistError::Internal(error) => crate::plugins::runtime_error(error),
            },
        )?;
        Ok(artist_db_id.0)
    }))
}

fn mark_unmatched_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    node_id: i64,
    id_types_table: luau::Table,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    ensure_provider_owner(context, &provider.plugin_id, &provider.provider_id)?;
    let node_id = args::positive_id(node_id, "node_id")?;
    let id_types = string_array_from_table(vm, &id_types_table)?;
    let provider_id = provider.provider_id.clone();
    if id_types.is_empty() {
        return Err(crate::plugins::runtime_error(
            "provider:mark_unmatched: id_types must contain at least one id type",
        ));
    }

    let mut seen = HashSet::new();
    let mut normalized_id_types = Vec::new();
    for raw_id_type in id_types {
        let id_type = raw_id_type.trim().to_string();
        if id_type.is_empty() {
            return Err(crate::plugins::runtime_error(
                "provider:mark_unmatched: id_types must contain non-empty strings",
            ));
        }
        if seen.insert(id_type.clone()) {
            normalized_id_types.push(id_type);
        }
    }

    let db = db::get_optional(vm)?;
    Ok(luau::ScheduledFuture::new(async move {
        let Some(db) = db else {
            return Err(crate::plugins::runtime_error(
                "provider:mark_unmatched requires a database-backed plugin executor",
            ));
        };
        let registered_entities = {
            let registry = provider_registry().read_owned().await;
            normalized_id_types
                .iter()
                .map(|id_type| registry.id_spec_entities(&provider_id, id_type))
                .collect::<Vec<_>>()
        };
        let mut db_write = db.write().await;
        let entity_type = entity_type_for_node(&db_write, node_id)
            .map_err(crate::plugins::runtime_error)?
            .ok_or_else(|| {
                crate::plugins::runtime_error(format!(
                    "provider:mark_unmatched: node_id {} does not reference a release, artist, or track",
                    node_id.0
                ))
            })?;

        {
            for (id_type, entities) in normalized_id_types.iter().zip(registered_entities) {
                if !entities.contains(&entity_type) {
                    return Err(crate::plugins::runtime_error(format!(
                        "provider:mark_unmatched: id_type '{id_type}' is not registered for {entity_type} on provider '{provider_id}'"
                    )));
                }
            }
        }

        crate::services::metadata::layers::mark_unmatched(
            &mut db_write,
            node_id,
            &provider_id,
            entity_type,
            normalized_id_types,
        )
        .map_err(crate::plugins::runtime_error)
    }))
}

fn layer_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    origin: &luau::ChunkOrigin,
    node_id: i64,
) -> luau::runtime::Result<luau::Value> {
    ensure_provider_owner(context, &provider.plugin_id, &provider.provider_id)?;
    let node_id = args::positive_id(node_id, "node_id")?;
    let db = db::get_optional(vm)?;
    MetadataLayer::new_value(vm, origin, db, node_id, provider.provider_id.clone())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{
        SystemTime,
        UNIX_EPOCH,
    };

    use super::validate_lyrics_provider_id;
    use crate::services::EntityType;
    use crate::services::STATE;
    use crate::services::covers::providers::{
        NormalizedProviderSearchResult,
        ProviderSearchRequest,
        search_provider,
    };

    struct TempSearchPluginDir {
        plugins_dir: PathBuf,
    }

    impl TempSearchPluginDir {
        fn new() -> anyhow::Result<Self> {
            let root = std::env::temp_dir().join(format!(
                "lyra-search-plugin-test-{}-{}",
                std::process::id(),
                SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
            ));
            let plugin_dir = root.join("searchtest");
            std::fs::create_dir_all(&plugin_dir)?;
            std::fs::write(
                plugin_dir.join("plugin.json"),
                r#"{
                    "schema_version": 1,
                    "id": "searchtest",
                    "name": "Search Test",
                    "version": "1.0.0",
                    "description": "Search callback contract test",
                    "entrypoint": "init.luau",
                    "scopes": ["lyra.metadata"]
                }"#,
            )?;
            std::fs::write(
                plugin_dir.join("init.luau"),
                r#"
                    local metadata = require("@lyra/metadata")
                    local provider = metadata.Provider.new("searchtest")

                    provider:search(metadata.EntityType.Release, function(query)
                        if type(query) ~= "string" then
                            error("expected string query, got " .. type(query))
                        end
                        return {
                            {
                                title = "Search: " .. query,
                                redirect_url = "https://example.test/releases/" .. query,
                                ids = {
                                    release_id = query,
                                },
                            },
                        }
                    end)
                "#,
            )?;
            Ok(Self { plugins_dir: root })
        }
    }

    impl Drop for TempSearchPluginDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.plugins_dir);
        }
    }

    #[tokio::test]
    async fn provider_search_dispatches_query_string_to_handler() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_library_test_state(std::env::temp_dir()).await?;
        let plugins = TempSearchPluginDir::new()?;
        let server_info = crate::plugins::server::load_server_info().await?;
        let auth_capabilities =
            crate::plugins::auth::AuthCapabilities::from_config(&STATE.config().auth);
        let (runtime, errors) =
            crate::plugins::executor::PluginExecutorHandle::discover_from_plugins_dir_with_db_and_modules(
                plugins.plugins_dir.clone(),
                server_info,
                auth_capabilities,
                STATE.db.get(),
                Vec::new(),
                Some(STATE.settings.clone()),
            )?;
        assert!(errors.is_empty(), "{errors:?}");
        runtime.exec_plugin("searchtest").await?;
        crate::plugins::STATE.publish_runtime(runtime.clone());

        let results = search_provider(ProviderSearchRequest {
            provider_id: "searchtest",
            entity_type: EntityType::Release,
            query: "loveless",
            include_cover_urls: false,
            force_refresh: false,
        })
        .await?;

        assert_eq!(results.len(), 1);
        let NormalizedProviderSearchResult::Release(result) = &results[0] else {
            panic!("expected release search result");
        };
        assert_eq!(result.title, "Search: loveless");
        assert_eq!(
            result.ids.as_ref().and_then(|ids| ids.get("release_id")),
            Some(&"loveless".to_string())
        );

        crate::plugins::STATE.clear_runtime();
        Ok(())
    }

    #[test]
    fn lyrics_registration_uses_database_provider_id_rules() {
        for provider_id in ["lrclib", "manual", "provider-with-punctuation_1"] {
            assert!(
                validate_lyrics_provider_id(provider_id).is_ok(),
                "expected {provider_id:?} to be accepted"
            );
        }
        for provider_id in ["", "two words", " leading", "trailing\t", "métadata"] {
            assert!(
                validate_lyrics_provider_id(provider_id).is_err(),
                "expected {provider_id:?} to be rejected"
            );
        }
    }
}
