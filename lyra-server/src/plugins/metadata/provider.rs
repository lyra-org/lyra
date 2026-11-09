// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashSet;

use harmony_core::UserDataBinding;
use harmony_luau as luau;

use crate::plugins::db;
use crate::services::plugin_scope::PluginId;
use crate::services::{
    EntityType,
    metadata::{
        layers::{
            EnsureArtistError,
            ProviderArtist,
            ensure_provider_artist,
        },
        lyrics::providers as lyrics_dispatcher,
    },
    providers::{
        ProviderCoverSpec,
        ProviderIdUrlGenerator,
        ProviderLyricsSpec,
        ProviderSimilarReleasesSpec,
    },
};

use super::MetadataCallbackRegistry;
use super::layer::{
    LayerOptions,
    MetadataLayer,
    write_target,
};
use super::parsing::{
    ensure_provider_owner,
    entity_type_for_node,
    optional_artist_type,
    optional_table_string,
    parse_cover_spec,
    parse_id_spec,
    parse_id_url_template,
    parse_lyrics_spec,
    parse_similar_releases_spec,
    required_table_string,
    string_array_from_table,
};

#[cfg(feature = "docgen")]
pub(super) struct ProviderOptions;

#[cfg(feature = "docgen")]
impl luau::LuauTypeInfo for ProviderOptions {
    fn luau_type() -> luau::LuauType {
        luau::LuauType::literal("ProviderOptions")
    }
}

pub(super) async fn provider_new(
    provider_id: String,
    options: Option<luau::Table>,
    vm: luau::Vm,
    context: luau::CallContext,
) -> luau::runtime::Result<luau::Value> {
    let plugin_id = crate::plugins::current_plugin(&vm, &context, "metadata.Provider.new")?;
    let db = db::get_optional(&vm)?;
    let origin = context.origin;
    let display_name = match &options {
        Some(options) => optional_table_string(options, "display_name", "metadata.Provider.new")?,
        None => None,
    };
    let generation = crate::plugins::STATE.generation();
    let _registration = generation
        .registries
        .ensure_registrations_open(&plugin_id)
        .await?;
    let scope = crate::plugins::state::VmScope::of(&vm)?;
    let mut registry = scope.providers_for(&plugin_id).write_owned().await;
    registry.register(plugin_id.clone(), provider_id.clone(), display_name)?;
    drop(registry);

    if let Some(db) = db {
        let mut db_write = db.write().await;
        lyra_db::providers::register(&mut db_write, &provider_id)?;
    }

    let provider = MetadataProvider {
        plugin_id,
        provider_id,
    };
    MetadataProvider::class().create_value(&vm, &origin, provider)
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

    #[harmony(returns(String))]
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
        node_id: String,
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
        node_id: String,
        options: Option<luau::Value>,
    ) -> luau::runtime::Result<luau::Value> {
        layer_callback(self, vm, context, origin, node_id, options)
    }
}

fn provider_id_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    spec: luau::Table,
    generator: Option<luau::Value>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    ensure_provider_owner(vm, context, &provider.plugin_id, &provider.provider_id)?;

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
    let context = context.clone();
    let provider = provider.clone();
    let scope = crate::plugins::state::VmScope::of(vm)?;
    Ok(luau::ScheduledFuture::new(async move {
        let generation = crate::plugins::STATE.generation();
        let _registration = generation
            .registries
            .ensure_registrations_open(&provider.plugin_id)
            .await?;
        let mut registry = scope.providers_for(&provider.plugin_id).write_owned().await;
        let entity = id_spec.entity;
        let replaced = registry
            .set_id_registration(&provider.provider_id, id_spec, || {
                generator.map(|generator| match generator {
                    IdGenerator::Template(template) => ProviderIdUrlGenerator::Template(template),
                    IdGenerator::Function(function) => ProviderIdUrlGenerator::Function(
                        handlers.register(provider.provider_id.clone(), entity, function, context),
                    ),
                })
            })
            .map_err(|err| crate::plugins::runtime_error(format!("provider:id: {err}")))?;
        if let Some(ProviderIdUrlGenerator::Function(handler)) = replaced {
            handlers.remove(handler);
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
    ensure_provider_owner(vm, context, &provider.plugin_id, &provider.provider_id)?;
    let option = crate::plugins::options::parse_option_declaration(&config)?;

    let provider = provider.clone();
    let scope = crate::plugins::state::VmScope::of(vm)?;
    Ok(luau::ScheduledFuture::new(async move {
        let generation = crate::plugins::STATE.generation();
        let _registration = generation
            .registries
            .ensure_registrations_open(&provider.plugin_id)
            .await?;
        let mut registry = scope.providers_for(&provider.plugin_id).write_owned().await;
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
    ensure_provider_owner(vm, context, &provider.plugin_id, &provider.provider_id)?;
    let handlers = vm.data().get::<MetadataCallbackRegistry>()?;
    let context = context.clone();
    let provider = provider.clone();
    let scope = crate::plugins::state::VmScope::of(vm)?;
    Ok(luau::ScheduledFuture::new(async move {
        let generation = crate::plugins::STATE.generation();
        let _registration = generation
            .registries
            .ensure_registrations_open(&provider.plugin_id)
            .await?;
        let mut registry = scope.providers_for(&provider.plugin_id).write_owned().await;
        let handler =
            handlers.register(provider.provider_id.clone(), entity_type, handler, context);

        registry.set_search_callback(&provider.provider_id, entity_type, handler);
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
    ensure_provider_owner(vm, context, &provider.plugin_id, &provider.provider_id)?;
    if !matches!(entity_type, EntityType::Release | EntityType::Artist) {
        return Err(crate::plugins::runtime_error(
            "provider:cover entity_type must be EntityType.Release or EntityType.Artist",
        ));
    }

    // Cover sync downloads the URLs the handler returns, so they reach the
    // network on the plugin's behalf.
    if !vm
        .data()
        .get::<harmony_core::luau::RequireRuntime>()?
        .allows(&context.origin, harmony_http::SCOPE)
    {
        return Err(crate::plugins::runtime_error(format!(
            "provider:cover requires the {} scope",
            harmony_http::SCOPE
        )));
    }

    let (timeout, require) = parse_cover_spec(&config)?;
    let handlers = vm.data().get::<MetadataCallbackRegistry>()?;
    let context = context.clone();
    let provider = provider.clone();
    let scope = crate::plugins::state::VmScope::of(vm)?;
    Ok(luau::ScheduledFuture::new(async move {
        let generation = crate::plugins::STATE.generation();
        let _registration = generation
            .registries
            .ensure_registrations_open(&provider.plugin_id)
            .await?;
        if !harmony_http::has_rate_limit_for_plugin(provider.plugin_id.as_ref()).await {
            return Err(crate::plugins::runtime_error(format!(
                "provider:cover requires http.set_rate_limit to be configured for at least one domain before registration; call set_rate_limit in plugin init for plugin '{}'",
                provider.plugin_id
            )));
        }
        let mut registry = scope.providers_for(&provider.plugin_id).write_owned().await;
        let handler =
            handlers.register(provider.provider_id.clone(), entity_type, handler, context);

        registry.set_cover_handler(
            &provider.provider_id,
            entity_type,
            ProviderCoverSpec {
                timeout,
                require,
                handler,
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
    ensure_provider_owner(vm, context, &provider.plugin_id, &provider.provider_id)?;

    let (timeout, require) = parse_lyrics_spec(&config)?;
    let handlers = vm.data().get::<MetadataCallbackRegistry>()?;
    let context = context.clone();
    let provider = provider.clone();
    let scope = crate::plugins::state::VmScope::of(vm)?;
    Ok(luau::ScheduledFuture::new(async move {
        let generation = crate::plugins::STATE.generation();
        let _registration = generation
            .registries
            .ensure_registrations_open(&provider.plugin_id)
            .await?;
        if !harmony_http::has_rate_limit_for_plugin(provider.plugin_id.as_ref()).await {
            return Err(crate::plugins::runtime_error(format!(
                "provider:lyrics requires http.set_rate_limit to be configured for at least one domain before registration; call set_rate_limit in plugin init for plugin '{}'",
                provider.plugin_id
            )));
        }
        let handler = handlers.register(
            provider.provider_id.clone(),
            EntityType::Track,
            handler,
            context,
        );
        lyrics_dispatcher::register_handler(
            &scope.providers_for(&provider.plugin_id),
            &provider.provider_id,
            ProviderLyricsSpec::new(timeout, require, handler),
        )
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
    ensure_provider_owner(vm, context, &provider.plugin_id, &provider.provider_id)?;
    let (timeout, require) = parse_similar_releases_spec(&config)?;
    let handlers = vm.data().get::<MetadataCallbackRegistry>()?;
    let context = context.clone();
    let provider = provider.clone();
    let scope = crate::plugins::state::VmScope::of(vm)?;
    Ok(luau::ScheduledFuture::new(async move {
        let generation = crate::plugins::STATE.generation();
        let _registration = generation
            .registries
            .ensure_registrations_open(&provider.plugin_id)
            .await?;
        let mut registry = scope.providers_for(&provider.plugin_id).write_owned().await;
        let handler = handlers.register(
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
                handler,
            },
        );
        Ok(())
    }))
}

fn refresh_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    entity_type: EntityType,
    handler: luau::Function,
    filter: Option<luau::Function>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    ensure_provider_owner(vm, context, &provider.plugin_id, &provider.provider_id)?;
    let handlers = vm.data().get::<MetadataCallbackRegistry>()?;
    let context = context.clone();
    let provider = provider.clone();
    let scope = crate::plugins::state::VmScope::of(vm)?;
    Ok(luau::ScheduledFuture::new(async move {
        let generation = crate::plugins::STATE.generation();
        let _registration = generation
            .registries
            .ensure_registrations_open(&provider.plugin_id)
            .await?;
        let mut registry = scope.providers_for(&provider.plugin_id).write_owned().await;
        let handler = handlers.register(
            provider.provider_id.clone(),
            entity_type,
            handler,
            context.clone(),
        );
        let filter = filter.map(|filter| {
            handlers.register(provider.provider_id.clone(), entity_type, filter, context)
        });

        registry.set_refresh_callback(&provider.provider_id, entity_type, handler);
        if let Some(filter) = filter {
            registry.set_sync_filter_callback(&provider.provider_id, entity_type, filter);
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
    ensure_provider_owner(vm, context, &provider.plugin_id, &provider.provider_id)?;
    let artist = ProviderArtist {
        id_type: required_table_string(&request, "id_type", "provider:ensure_artist")?,
        id_value: required_table_string(&request, "id_value", "provider:ensure_artist")?,
        artist_name: optional_table_string(&request, "artist_name", "provider:ensure_artist")?,
        sort_name: optional_table_string(&request, "sort_name", "provider:ensure_artist")?,
        artist_type: optional_artist_type(vm, &request, "artist_type")?,
        description: optional_table_string(&request, "description", "provider:ensure_artist")?,
    };
    let provider_id = provider.provider_id.clone();
    let plugin_id = provider.plugin_id.clone();
    let db = db::get_optional(vm)?;
    let scope = crate::plugins::state::VmScope::of(vm)?;
    Ok(luau::ScheduledFuture::new(async move {
        let is_registered_artist_id = {
            let registry = scope.providers_for(&plugin_id).read_owned().await;
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
        crate::plugins::ids::public_id(&*db_write, artist_db_id)
    }))
}

fn mark_unmatched_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    node_id: String,
    id_types_table: luau::Table,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    ensure_provider_owner(vm, context, &provider.plugin_id, &provider.provider_id)?;
    let id_types = string_array_from_table(&id_types_table)?;
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
    let plugin_id = provider.plugin_id.clone();
    let scope = crate::plugins::state::VmScope::of(vm)?;
    Ok(luau::ScheduledFuture::new(async move {
        let Some(db) = db else {
            return Err(crate::plugins::runtime_error(
                "provider:mark_unmatched requires a database-backed plugin executor",
            ));
        };
        let registered_entities = {
            let registry = scope.providers_for(&plugin_id).read_owned().await;
            normalized_id_types
                .iter()
                .map(|id_type| registry.id_spec_entities(&provider_id, id_type))
                .collect::<Vec<_>>()
        };
        let mut db_write = db.write().await;
        let not_an_entity = || {
            format!(
                "provider:mark_unmatched: node_id {node_id} does not reference a release, artist, or track"
            )
        };
        let generation = crate::services::STATE.generation();
        // The kind is what makes a refresh count as skipped rather than failed.
        let target =
            write_target(&db_write, &generation.merge_journal, &node_id)?.ok_or_else(|| {
                crate::plugins::errors::kinded(
                    crate::plugins::errors::ErrorKind::NotFound,
                    not_an_entity(),
                )
            })?;
        // A failed lookup of the loser says nothing about the winner.
        if target.merged {
            return Ok(());
        }
        let node_id = target.db_id;
        let entity_type = entity_type_for_node(&db_write, node_id)?
            .ok_or_else(|| crate::plugins::runtime_error(not_an_entity()))?;

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
    node_id: String,
    options: Option<luau::Value>,
) -> luau::runtime::Result<luau::Value> {
    ensure_provider_owner(vm, context, &provider.plugin_id, &provider.provider_id)?;
    let options = LayerOptions::from_luau(options)?;
    let db = db::get_optional(vm)?;
    MetadataLayer::new_value(
        vm,
        origin,
        db,
        node_id,
        provider.provider_id.clone(),
        options,
    )
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{
        SystemTime,
        UNIX_EPOCH,
    };

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
        fn with_plugin(id: &str, source: &str) -> anyhow::Result<Self> {
            let root = std::env::temp_dir().join(format!(
                "lyra-search-plugin-test-{}-{}",
                std::process::id(),
                SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
            ));
            let plugin_dir = root.join(id);
            std::fs::create_dir_all(&plugin_dir)?;
            std::fs::write(
                plugin_dir.join("plugin.json"),
                serde_json::json!({
                    "schema_version": 1,
                    "id": id,
                    "name": "Test Plugin",
                    "version": "1.0.0",
                    "lyra": crate::plugins::testing::HOST_REQUIREMENT,
                    "description": "Provider contract test",
                    "entrypoint": "init.luau",
                    "scopes": ["lyra.metadata"]
                })
                .to_string(),
            )?;
            std::fs::write(plugin_dir.join("init.luau"), source)?;
            Ok(Self { plugins_dir: root })
        }

        fn new() -> anyhow::Result<Self> {
            Self::with_plugin(
                "searchtest",
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
            )
        }
    }

    impl Drop for TempSearchPluginDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.plugins_dir);
        }
    }

    /// Writes a release's title and then, by its title, raises through a write to something gone.
    const GONE_WRITES: &str = r#"
        local metadata = require("@lyra/metadata")
        local provider = metadata.Provider.new("gonetest")

        provider:refresh(metadata.EntityType.Release, function(context)
            local layer = provider:layer(context.id)
            layer:set_field("release_title", "Written first")
            layer:save()
            if context.release_title == "Target Gone" then
                local gone = provider:layer("no-such-node")
                gone:set_field("track_title", "Nowhere")
                gone:save()
            elseif context.release_title == "Artist Gone" then
                local credited = provider:layer(context.id)
                credited:set_credits(metadata.CreditType.Artist, { { artist_id = "no-such-artist" } })
                credited:save()
            end
            return nil
        end)
    "#;

    #[tokio::test(flavor = "multi_thread")]
    async fn a_gone_target_is_skipped_and_a_gone_credited_artist_fails() -> anyhow::Result<()> {
        use crate::services::covers::CoverSyncOptions;
        use crate::services::providers::{
            LibraryRefreshOptions,
            refresh_library_metadata,
        };

        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_library_test_state(std::env::temp_dir()).await?;
        let plugins = TempSearchPluginDir::with_plugin("gonetest", GONE_WRITES)?;
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
        runtime.exec_plugin("gonetest").await?;
        crate::plugins::STATE
            .publish_runtime(&crate::services::STATE.generation(), runtime.clone());

        let (library, target_gone, artist_gone) = {
            let mut db = STATE.db.write().await;
            let library = lyra_db::test_db::insert_library(&mut db, "Gone", "/gone-writes")?;
            let target_gone = lyra_db::fixtures::insert_release(&mut db, "Target Gone")?;
            let artist_gone = lyra_db::fixtures::insert_release(&mut db, "Artist Gone")?;
            for release in [target_gone, artist_gone] {
                lyra_db::fixtures::connect(&mut db, library, release)?;
            }
            (
                lyra_db::libraries::get_by_id(&db, library)?.expect("library"),
                target_gone,
                artist_gone,
            )
        };
        let progress = crate::services::libraries::begin_test_run(&library).await;
        let options = LibraryRefreshOptions {
            covers: CoverSyncOptions::SKIP,
            apply_sync_filters: false,
            provider_id: None,
        };

        refresh_library_metadata(library.db_id.expect("db id"), &options, progress.clone())
            .await
            .map_err(|err| err.into_anyhow())?;
        let run = crate::services::libraries::get_sync_run(progress.run_id())
            .await?
            .expect("run");
        crate::services::libraries::end_test_run(&progress).await;

        assert_eq!(
            (
                run.progress.skipped_units,
                run.progress.failed_units,
                run.progress.completed_units
            ),
            (1, 1, 0),
            "the gone target is skipped, the gone credited artist fails"
        );
        let db = STATE.db.read().await;
        for release in [target_gone, artist_gone] {
            let saved = lyra_db::releases::get_by_id(&db, release)?.expect("release");
            assert_eq!(saved.release_title, "Written first", "the first save stays");
        }
        drop(db);
        crate::plugins::STATE.clear_runtime();
        Ok(())
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
        crate::plugins::STATE
            .publish_runtime(&crate::services::STATE.generation(), runtime.clone());

        let results = search_provider(ProviderSearchRequest {
            provider_id: "searchtest",
            entity_type: EntityType::Release,
            query: "loveless",
            include_cover_urls: false,
            bypass_cache: false,
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
}
