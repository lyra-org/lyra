// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        HashMap,
        HashSet,
    },
    sync::Arc,
};

use agdb::{
    DbId,
    QueryBuilder,
};
use anyhow::Context;
use harmony_core::FunctionSpec;
use harmony_luau as luau;
use nanoid::nanoid;
use serde_json::Value as JsonValue;

use crate::STATE;
use crate::plugins::db;
use crate::plugins::db as server_db;
use crate::plugins::db::ProviderConfig;
use crate::plugins::executor::MetadataRefreshRequest;
use crate::plugins::lifecycle::PluginId;
use crate::services::{
    EntityType,
    metadata::lyrics::{
        providers as lyrics_dispatcher,
        providers::RegisteredHandler,
    },
    providers::{
        ProviderCallbackHandle,
        ProviderCoverSpec,
        ProviderIdUrlGenerator,
        ProviderSimilarReleasesSpec,
        provider_registry,
    },
};

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
    require_positive_id,
    required_table_string,
    string_array_from_table,
};
use super::{
    MetadataCallbackRegistry,
    MetadataModuleStore,
};

pub(super) fn provider_new_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("Provider.new")
        .arg_name("id")
        .args::<String>()
        .returns::<MetadataProvider>()
        .call(provider_new_callback)
}

pub(super) fn ids_for_provider_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("ids.for_provider")
        .arg_name("external_ids")
        .arg_name("provider_id")
        .args::<luau::Table>()
        .args::<String>()
        .returns::<Option<luau::Table>>()
        .call(ids_for_provider_callback)
}

fn provider_new_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let provider_id: String = frame.args.read_named("id")?;
    let plugin_id = frame.context.origin.plugin.clone().ok_or_else(|| {
        crate::plugins::runtime_error("metadata.Provider.new must be called from plugin Lua code")
    })?;
    let plugin_id = PluginId::new(plugin_id.to_string()).map_err(crate::plugins::runtime_error)?;
    let store = frame
        .vm
        .data()
        .get::<MetadataModuleStore>()?
        .as_ref()
        .clone();

    futures::executor::block_on(async {
        let generation = STATE.generation();
        let _registration = generation
            .plugin_registries
            .ensure_registrations_open(&plugin_id)
            .await
            .map_err(crate::plugins::runtime_error)?;
        let mut registry = provider_registry().write_owned().await;
        registry
            .register(plugin_id.clone(), provider_id.clone())
            .map_err(crate::plugins::runtime_error)?;
        drop(registry);

        if let Some(db) = store.db {
            let mut db_write = db.write().await;
            if db::providers::get_by_provider_id(&db_write, &provider_id)
                .map_err(crate::plugins::runtime_error)?
                .is_none()
            {
                let provider_config = ProviderConfig {
                    db_id: None,
                    provider_id: provider_id.clone(),
                    display_name: provider_id.clone(),
                    priority: 50,
                    enabled: true,
                };
                db::providers::upsert(&mut db_write, &provider_config)
                    .map_err(crate::plugins::runtime_error)?;
            }
        }

        Ok::<(), luau::Error>(())
    })?;

    let provider = MetadataProvider {
        plugin_id,
        provider_id,
    };
    let provider = MetadataProvider::_harmony_userdata_class().create_value(
        frame.vm,
        &frame.context.origin,
        provider,
    )?;
    frame.returns.write(provider)
}

fn ensure_registration_open(plugin_id: &PluginId) -> luau::runtime::Result<()> {
    futures::executor::block_on(async {
        let generation = STATE.generation();
        let _registration = generation
            .plugin_registries
            .ensure_registrations_open(plugin_id)
            .await
            .map_err(crate::plugins::runtime_error)?;
        Ok(())
    })
}

fn ids_for_provider_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let external_ids: luau::Table = frame.args.read_named("external_ids")?;
    let provider_id: String = frame.args.read_named("provider_id")?;
    match external_ids.get_raw(frame.vm, &provider_id)? {
        luau::Value::Table(table) => frame.returns.write(Some(table)),
        luau::Value::TableData(data) => frame.returns.write(luau::Value::TableData(data)),
        luau::Value::Nil => frame.returns.write(luau::Value::Nil),
        _ => frame.returns.write(luau::Value::Nil),
    }
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
    ) -> luau::runtime::Result<()> {
        provider_id_callback(self, vm, context, spec, generator)
    }

    fn search(
        &self,
        vm: &luau::Vm,
        context: &luau::CallContext,
        entity: EntityType,
        handler: luau::Function,
    ) -> luau::runtime::Result<()> {
        search_callback(self, vm, context, entity, handler)
    }

    fn cover(
        &self,
        vm: &luau::Vm,
        context: &luau::CallContext,
        entity: EntityType,
        config: luau::Table,
        handler: luau::Function,
    ) -> luau::runtime::Result<()> {
        cover_callback(self, vm, context, entity, config, handler)
    }

    fn lyrics(
        &self,
        vm: &luau::Vm,
        context: &luau::CallContext,
        config: luau::Table,
        handler: luau::Function,
    ) -> luau::runtime::Result<()> {
        lyrics_callback(self, vm, context, config, handler)
    }

    fn similar_releases(
        &self,
        vm: &luau::Vm,
        context: &luau::CallContext,
        config: luau::Table,
        handler: luau::Function,
    ) -> luau::runtime::Result<()> {
        similar_releases_callback(self, vm, context, config, handler)
    }

    fn refresh(
        &self,
        vm: &luau::Vm,
        context: &luau::CallContext,
        entity: EntityType,
        handler: luau::Function,
        filter: Option<luau::Function>,
    ) -> luau::runtime::Result<()> {
        refresh_callback(self, vm, context, entity, handler, filter)
    }

    fn declare_option(
        &self,
        vm: &luau::Vm,
        context: &luau::CallContext,
        config: luau::Table,
    ) -> luau::runtime::Result<()> {
        declare_option_callback(self, vm, context, config)
    }

    fn ensure_artist(
        &self,
        vm: &luau::Vm,
        context: &luau::CallContext,
        request: luau::Table,
    ) -> luau::runtime::Result<i64> {
        ensure_artist_callback(self, vm, context, request)
    }

    fn mark_unmatched(
        &self,
        vm: &luau::Vm,
        context: &luau::CallContext,
        node_id: i64,
        id_types: luau::Table,
    ) -> luau::runtime::Result<()> {
        mark_unmatched_callback(self, vm, context, node_id, id_types)
    }

    fn link_credit(
        &self,
        vm: &luau::Vm,
        context: &luau::CallContext,
        owner_id: i64,
        artist_id: i64,
        credit_type: Option<server_db::CreditType>,
        detail: Option<String>,
    ) -> luau::runtime::Result<()> {
        link_credit_callback(self, vm, context, owner_id, artist_id, credit_type, detail)
    }

    fn link_artist_relation(
        &self,
        vm: &luau::Vm,
        context: &luau::CallContext,
        from_artist_id: i64,
        to_artist_id: i64,
        relation_type: server_db::ArtistRelationType,
        attributes: Option<String>,
    ) -> luau::runtime::Result<()> {
        link_artist_relation_callback(
            self,
            vm,
            context,
            from_artist_id,
            to_artist_id,
            relation_type,
            attributes,
        )
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
) -> luau::runtime::Result<()> {
    ensure_provider_owner(context, &provider.plugin_id, &provider.provider_id)?;
    ensure_registration_open(&provider.plugin_id)?;

    let id_spec = parse_id_spec(vm, &spec)?;
    let generator = match generator {
        Some(luau::Value::String(bytes)) => Some(ProviderIdUrlGenerator::Template(
            parse_id_url_template(bytes)?,
        )),
        Some(luau::Value::Function(_)) | Some(luau::Value::NativeFunction(_)) => None,
        Some(luau::Value::Nil) | None => None,
        Some(other) => {
            return Err(crate::plugins::runtime_error(format!(
                "provider:id generator must be a function, string template, or nil; got {}",
                other.type_name()
            )));
        }
    };

    futures::executor::block_on(async {
        let mut registry = provider_registry().write_owned().await;
        registry.set_id_registration(&provider.provider_id, id_spec, generator);
    });
    Ok(())
}

fn declare_option_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    config: luau::Table,
) -> luau::runtime::Result<()> {
    ensure_provider_owner(context, &provider.plugin_id, &provider.provider_id)?;
    ensure_registration_open(&provider.plugin_id)?;
    let option = parse_option_declaration(vm, &config)?;

    futures::executor::block_on(async {
        let mut registry = provider_registry().write_owned().await;
        registry
            .declare_option(&provider.provider_id, option)
            .map_err(crate::plugins::runtime_error)
    })?;
    Ok(())
}

fn search_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    entity_type: EntityType,
    handler: luau::Function,
) -> luau::runtime::Result<()> {
    ensure_provider_owner(context, &provider.plugin_id, &provider.provider_id)?;
    ensure_registration_open(&provider.plugin_id)?;
    let handlers = vm.data().get::<MetadataCallbackRegistry>()?;
    let context = core_call_context(context);
    let handler_id = handlers.register(provider.provider_id.clone(), entity_type, handler, context);

    futures::executor::block_on(async {
        let mut registry = provider_registry().write_owned().await;
        registry.set_search_callback(
            &provider.provider_id,
            entity_type,
            ProviderCallbackHandle { handler_id },
        );
    });
    Ok(())
}

fn cover_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    entity_type: EntityType,
    config: luau::Table,
    handler: luau::Function,
) -> luau::runtime::Result<()> {
    ensure_provider_owner(context, &provider.plugin_id, &provider.provider_id)?;
    ensure_registration_open(&provider.plugin_id)?;
    if !matches!(entity_type, EntityType::Release | EntityType::Artist) {
        return Err(crate::plugins::runtime_error(
            "provider:cover entity_type must be EntityType.Release or EntityType.Artist",
        ));
    }
    if !futures::executor::block_on(harmony_http::has_rate_limit_for_plugin(
        provider.plugin_id.as_ref(),
    )) {
        return Err(crate::plugins::runtime_error(format!(
            "provider:cover requires http.set_rate_limit to be configured for at least one domain before registration; call set_rate_limit in plugin init for plugin '{}'",
            provider.plugin_id
        )));
    }

    let (priority, timeout, require) = parse_cover_spec(vm, &config)?;
    let handlers = vm.data().get::<MetadataCallbackRegistry>()?;
    let context = core_call_context(context);
    let handler_id = handlers.register(provider.provider_id.clone(), entity_type, handler, context);

    futures::executor::block_on(async {
        let mut registry = provider_registry().write_owned().await;
        registry.set_cover_handler(
            &provider.provider_id,
            entity_type,
            ProviderCoverSpec {
                priority,
                timeout,
                require,
                handler: ProviderCallbackHandle { handler_id },
            },
        );
    });
    Ok(())
}

fn lyrics_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    config: luau::Table,
    handler: luau::Function,
) -> luau::runtime::Result<()> {
    ensure_provider_owner(context, &provider.plugin_id, &provider.provider_id)?;
    ensure_registration_open(&provider.plugin_id)?;
    validate_lyrics_provider_id(&provider.provider_id)?;
    if !futures::executor::block_on(harmony_http::has_rate_limit_for_plugin(
        provider.plugin_id.as_ref(),
    )) {
        return Err(crate::plugins::runtime_error(format!(
            "provider:lyrics requires http.set_rate_limit to be configured for at least one domain before registration; call set_rate_limit in plugin init for plugin '{}'",
            provider.plugin_id
        )));
    }

    let (priority, timeout, require) = parse_lyrics_spec(vm, &config)?;
    let handlers = vm.data().get::<MetadataCallbackRegistry>()?;
    let context = core_call_context(context);
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
            let runtime = STATE
                .generation()
                .plugin_runtime
                .get()
                .context("plugin runtime is not initialized")?;
            let context = lyrics_dispatcher::track_context_to_json(&context);
            let result = runtime
                .dispatch_metadata_refresh(MetadataRefreshRequest {
                    handler_id,
                    context,
                })
                .await
                .with_context(|| format!("provider lyrics handler failed for '{provider_id}'"))?;
            let value = result
                .values
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("provider lyrics handler returned no value"))?;
            lyrics_dispatcher::parse_handler_result(value)
        })
    });

    futures::executor::block_on(async {
        let cancel = lyrics_dispatcher::make_plugin_cancellation_child(&plugin_id).await;
        lyrics_dispatcher::register_handler(RegisteredHandler {
            provider_id: Arc::from(provider_id.as_str()),
            plugin_id,
            priority,
            timeout,
            require,
            handler: handler_fn,
            cancel,
        })
        .await;
    });
    Ok(())
}

fn similar_releases_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    config: luau::Table,
    handler: luau::Function,
) -> luau::runtime::Result<()> {
    ensure_provider_owner(context, &provider.plugin_id, &provider.provider_id)?;
    ensure_registration_open(&provider.plugin_id)?;
    let (timeout, require) = parse_similar_releases_spec(vm, &config)?;
    let handlers = vm.data().get::<MetadataCallbackRegistry>()?;
    let context = core_call_context(context);
    let handler_id = handlers.register(
        provider.provider_id.clone(),
        EntityType::Release,
        handler,
        context,
    );

    futures::executor::block_on(async {
        let mut registry = provider_registry().write_owned().await;
        registry.set_similar_releases_handler(
            &provider.provider_id,
            ProviderSimilarReleasesSpec {
                timeout,
                require,
                handler: ProviderCallbackHandle { handler_id },
            },
        );
    });
    Ok(())
}

fn validate_lyrics_provider_id(provider_id: &str) -> luau::runtime::Result<()> {
    db::lyrics::validate_provider_id(provider_id).map_err(crate::plugins::runtime_error)
}

fn refresh_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    entity_type: EntityType,
    handler: luau::Function,
    filter: Option<luau::Function>,
) -> luau::runtime::Result<()> {
    ensure_provider_owner(context, &provider.plugin_id, &provider.provider_id)?;
    ensure_registration_open(&provider.plugin_id)?;
    let handlers = vm.data().get::<MetadataCallbackRegistry>()?;
    let context = core_call_context(context);
    let handler_id = handlers.register(
        provider.provider_id.clone(),
        entity_type,
        handler,
        context.clone(),
    );
    let filter_id = filter.map(|filter| {
        handlers.register(provider.provider_id.clone(), entity_type, filter, context)
    });

    futures::executor::block_on(async {
        let mut registry = provider_registry().write_owned().await;
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
    });
    Ok(())
}

fn ensure_artist_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    request: luau::Table,
) -> luau::runtime::Result<i64> {
    ensure_provider_owner(context, &provider.plugin_id, &provider.provider_id)?;
    let id_type = required_table_string(vm, &request, "id_type", "provider:ensure_artist")?;
    let id_value = required_table_string(vm, &request, "id_value", "provider:ensure_artist")?;
    let artist_name = optional_table_string(vm, &request, "artist_name", "provider:ensure_artist")?;
    let sort_name = optional_table_string(vm, &request, "sort_name", "provider:ensure_artist")?;
    let artist_type = optional_artist_type(vm, &request, "artist_type")?;
    let description = optional_table_string(vm, &request, "description", "provider:ensure_artist")?;
    let provider_id = provider.provider_id.clone();
    futures::executor::block_on(async {
        let is_registered_artist_id = {
            let registry = provider_registry().read_owned().await;
            registry.id_spec_matches_entity(&provider_id, &id_type, EntityType::Artist)
        };
        if !is_registered_artist_id {
            return Err(crate::plugins::runtime_error(format!(
                "provider:ensure_artist: id_type '{id_type}' is not registered for artist on provider '{provider_id}'"
            )));
        }

        let store = vm.data().get::<MetadataModuleStore>()?.as_ref().clone();
        let Some(db) = store.db else {
            return Err(crate::plugins::runtime_error(
                "provider:ensure_artist requires a database-backed plugin executor",
            ));
        };
        let mut db_write = db.write().await;
        let mut resolved_artist_id: Option<DbId> = None;

        if let Some(owner_id) = server_db::external_ids::get_owner(
            &db_write,
            &provider_id,
            &id_type,
            &id_value,
            Some("Artist"),
        )
        .map_err(crate::plugins::runtime_error)?
            && let Some(artist) = server_db::artists::get_by_id(&db_write, owner_id)
                .map_err(crate::plugins::runtime_error)?
        {
            if let (Some(existing_type), Some(incoming_type)) = (artist.artist_type, artist_type)
                && existing_type != incoming_type
            {
                return Err(crate::plugins::runtime_error(format!(
                    "provider:ensure_artist: {id_type} '{id_value}' is already attached to artist type '{existing_type}', not '{incoming_type}'"
                )));
            }
            resolved_artist_id = Some(owner_id);
        }

        if resolved_artist_id.is_none()
            && let Some(scan_name) = artist_name.as_deref()
        {
            let existing_name_matches = db_write
                .exec(
                    QueryBuilder::search()
                        .index("scan_name")
                        .value(scan_name)
                        .query(),
                )
                .map_err(crate::plugins::runtime_error)?;
            let mut candidate_ids = existing_name_matches.ids().to_vec();
            candidate_ids.sort_by_key(|id| id.0);

            for candidate_id in candidate_ids {
                let Some(candidate_artist) = server_db::artists::get_by_id(&db_write, candidate_id)
                    .map_err(crate::plugins::runtime_error)?
                else {
                    continue;
                };
                if candidate_artist.scan_name != scan_name {
                    continue;
                }
                if let (Some(existing_type), Some(incoming_type)) =
                    (candidate_artist.artist_type, artist_type)
                    && existing_type != incoming_type
                {
                    continue;
                }

                if let Some(existing_for_provider) =
                    server_db::external_ids::get(&db_write, candidate_id, &provider_id, &id_type)
                        .map_err(crate::plugins::runtime_error)?
                {
                    let existing_value = existing_for_provider.id_value.trim();
                    if !existing_value.is_empty() && existing_value != id_value {
                        continue;
                    }
                }

                resolved_artist_id = Some(candidate_id);
                break;
            }
        }

        let artist_db_id = if let Some(existing) = resolved_artist_id {
            existing
        } else {
            let fallback_name = artist_name.clone().unwrap_or_else(|| id_value.clone());
            let created_artist = server_db::Artist {
                db_id: None,
                id: nanoid!(),
                artist_name: fallback_name.clone(),
                scan_name: fallback_name,
                sort_name: None,
                artist_type,
                description: None,
                verified: false,
                locked: None,
                created_at: Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                ),
            };
            let insert_result = db_write
                .exec_mut(QueryBuilder::insert().element(&created_artist).query())
                .map_err(crate::plugins::runtime_error)?;
            let inner_artist_db_id = insert_result
                .elements
                .first()
                .map(|element| element.id)
                .ok_or_else(|| {
                    crate::plugins::runtime_error(
                        "provider:ensure_artist: artist insert missing id",
                    )
                })?;
            db_write
                .exec_mut(
                    QueryBuilder::insert()
                        .edges()
                        .from("artists")
                        .to(inner_artist_db_id)
                        .query(),
                )
                .map_err(crate::plugins::runtime_error)?;
            inner_artist_db_id
        };

        let mut fields = HashMap::new();
        if let Some(name) = artist_name {
            fields.insert("artist_name".to_string(), JsonValue::String(name));
        }
        if let Some(artist_type) = &artist_type {
            fields.insert(
                "artist_type".to_string(),
                JsonValue::String(artist_type.to_string()),
            );
        }
        if let Some(sort_name) = sort_name {
            fields.insert("sort_name".to_string(), JsonValue::String(sort_name));
        }
        if let Some(description) = description {
            fields.insert("description".to_string(), JsonValue::String(description));
        }

        let mut external_ids = HashMap::new();
        external_ids.insert(id_type, id_value);
        crate::services::metadata::layers::save_provider_layer(
            &mut db_write,
            artist_db_id,
            &provider_id,
            &fields,
            &external_ids,
            &HashMap::new(),
            &HashSet::new(),
        )
        .map_err(crate::plugins::runtime_error)?;

        Ok(artist_db_id.0)
    })
}

fn mark_unmatched_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    node_id: i64,
    id_types_table: luau::Table,
) -> luau::runtime::Result<()> {
    ensure_provider_owner(context, &provider.plugin_id, &provider.provider_id)?;
    let node_id = require_positive_id(node_id, "node_id")?;
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

    futures::executor::block_on(async {
        let store = vm.data().get::<MetadataModuleStore>()?.as_ref().clone();
        let Some(db) = store.db else {
            return Err(crate::plugins::runtime_error(
                "provider:mark_unmatched requires a database-backed plugin executor",
            ));
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
            let registry = provider_registry().read_owned().await;
            for id_type in &normalized_id_types {
                if !registry.id_spec_matches_entity(&provider_id, id_type, entity_type) {
                    return Err(crate::plugins::runtime_error(format!(
                        "provider:mark_unmatched: id_type '{id_type}' is not registered for {entity_type} on provider '{provider_id}'"
                    )));
                }
            }
        }

        let external_ids = normalized_id_types
            .into_iter()
            .map(|id_type| (id_type, String::new()))
            .collect::<HashMap<_, _>>();
        crate::services::metadata::layers::save_provider_layer(
            &mut db_write,
            node_id,
            &provider_id,
            &HashMap::new(),
            &external_ids,
            &HashMap::new(),
            &HashSet::new(),
        )
        .map_err(crate::plugins::runtime_error)
    })
}

fn link_credit_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    owner_id: i64,
    artist_id: i64,
    credit_type: Option<server_db::CreditType>,
    detail: Option<String>,
) -> luau::runtime::Result<()> {
    ensure_provider_owner(context, &provider.plugin_id, &provider.provider_id)?;
    let owner_id = require_positive_id(owner_id, "owner_id")?;
    let artist_id = require_positive_id(artist_id, "artist_id")?;
    let credit_type = credit_type.unwrap_or(server_db::CreditType::Artist);
    let detail = detail.and_then(|value| {
        let trimmed = value.trim().to_string();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    });

    futures::executor::block_on(async {
        let store = vm.data().get::<MetadataModuleStore>()?.as_ref().clone();
        let Some(db) = store.db else {
            return Err(crate::plugins::runtime_error(
                "provider:link_credit requires a database-backed plugin executor",
            ));
        };
        let mut db_write = db.write().await;

        let owner_is_release = server_db::releases::get_by_id(&db_write, owner_id)
            .map_err(crate::plugins::runtime_error)?
            .is_some();
        let owner_is_track = server_db::tracks::get_by_id(&db_write, owner_id)
            .map_err(crate::plugins::runtime_error)?
            .is_some();
        if !owner_is_release && !owner_is_track {
            return Err(crate::plugins::runtime_error(
                "provider:link_credit: owner_id must reference a release or track",
            ));
        }
        if server_db::artists::get_by_id(&db_write, artist_id)
            .map_err(crate::plugins::runtime_error)?
            .is_none()
        {
            return Err(crate::plugins::runtime_error(
                "provider:link_credit: artist_id does not reference an artist",
            ));
        }
        if db::manual_metadata_owns_field(&db_write, owner_id, db::ManualMetadataField::Credits)
            .map_err(crate::plugins::runtime_error)?
        {
            return Ok(());
        }

        let existing_credits: Vec<server_db::Credit> = db_write
            .exec(
                QueryBuilder::select()
                    .elements::<server_db::Credit>()
                    .search()
                    .from(owner_id)
                    .where_()
                    .neighbor()
                    .end_where()
                    .query(),
            )
            .map_err(crate::plugins::runtime_error)?
            .try_into()
            .map_err(crate::plugins::runtime_error)?;

        let already_linked = existing_credits.iter().any(|credit| {
            if credit.credit_type != credit_type || credit.detail != detail {
                return false;
            }
            let Some(credit_id) = credit.db_id.clone().map(DbId::from) else {
                return false;
            };
            server_db::graph::direct_edges_from(&db_write, credit_id)
                .ok()
                .is_some_and(|edges| edges.iter().any(|edge| edge.to == artist_id))
        });

        if already_linked {
            return Ok(());
        }

        db_write
            .transaction_mut(|transaction| -> anyhow::Result<()> {
                let credit = server_db::Credit {
                    db_id: None,
                    id: nanoid!(),
                    credit_type,
                    detail,
                };
                let credit_id = transaction
                    .exec_mut(QueryBuilder::insert().element(&credit).query())?
                    .ids()[0];
                transaction.exec_mut(
                    QueryBuilder::insert()
                        .edges()
                        .from("credits")
                        .to(credit_id)
                        .query(),
                )?;
                transaction.exec_mut(
                    QueryBuilder::insert()
                        .edges()
                        .from(owner_id)
                        .to(credit_id)
                        .values_uniform([("owned", 1).into()])
                        .query(),
                )?;
                transaction.exec_mut(
                    QueryBuilder::insert()
                        .edges()
                        .from(credit_id)
                        .to(artist_id)
                        .query(),
                )?;
                Ok(())
            })
            .map_err(crate::plugins::runtime_error)
    })
}

fn link_artist_relation_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    from_artist_id: i64,
    to_artist_id: i64,
    relation_type: server_db::ArtistRelationType,
    attributes: Option<String>,
) -> luau::runtime::Result<()> {
    ensure_provider_owner(context, &provider.plugin_id, &provider.provider_id)?;
    let from_artist_id = require_positive_id(from_artist_id, "from_artist_id")?;
    let to_artist_id = require_positive_id(to_artist_id, "to_artist_id")?;
    let attributes = attributes.and_then(|value| {
        let trimmed = value.trim().to_string();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    });

    futures::executor::block_on(async {
        let store = vm.data().get::<MetadataModuleStore>()?.as_ref().clone();
        let Some(db) = store.db else {
            return Err(crate::plugins::runtime_error(
                "provider:link_artist_relation requires a database-backed plugin executor",
            ));
        };
        let mut db_write = db.write().await;

        if server_db::artists::get_by_id(&db_write, from_artist_id)
            .map_err(crate::plugins::runtime_error)?
            .is_none()
        {
            return Err(crate::plugins::runtime_error(
                "provider:link_artist_relation: from_artist_id does not reference an artist",
            ));
        }
        if server_db::artists::get_by_id(&db_write, to_artist_id)
            .map_err(crate::plugins::runtime_error)?
            .is_none()
        {
            return Err(crate::plugins::runtime_error(
                "provider:link_artist_relation: to_artist_id does not reference an artist",
            ));
        }
        if db::manual_metadata_owns_field(
            &db_write,
            from_artist_id,
            db::ManualMetadataField::Relations,
        )
        .map_err(crate::plugins::runtime_error)?
        {
            return Ok(());
        }

        server_db::artists::relations::link(
            &mut db_write,
            from_artist_id,
            to_artist_id,
            relation_type,
            attributes,
        )
        .map(|_| ())
        .map_err(crate::plugins::runtime_error)
    })
}

fn layer_callback(
    provider: &MetadataProvider,
    vm: &luau::Vm,
    context: &luau::CallContext,
    origin: &luau::ChunkOrigin,
    node_id: i64,
) -> luau::runtime::Result<luau::Value> {
    ensure_provider_owner(context, &provider.plugin_id, &provider.provider_id)?;
    let node_id = require_positive_id(node_id, "node_id")?;
    let store = vm.data().get::<MetadataModuleStore>()?.as_ref().clone();
    MetadataLayer::new_value(vm, origin, store, node_id, provider.provider_id.clone())
}

#[cfg(test)]
mod tests {
    use super::validate_lyrics_provider_id;

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
