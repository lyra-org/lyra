// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    cell::{
        Cell,
        RefCell,
    },
    collections::{
        HashMap,
        HashSet,
    },
    sync::{
        Arc,
        Mutex,
    },
    time::Duration,
};

use agdb::DbId;
use agdb::QueryBuilder;
use anyhow::Context;
use harmony_core::{
    CallContext,
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::{
    ClassDescriptor,
    FieldDescriptor,
    FunctionParameter,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
    MethodDescriptor,
    MethodKind,
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    TypeAliasDescriptor,
    render_definition_file_with_support,
};
use nanoid::nanoid;
use serde_json::Value as JsonValue;

use crate::STATE;
use crate::plugins::db;
use crate::plugins::db as server_db;
use crate::plugins::db::{
    DbAsync,
    ProviderConfig,
};
use crate::plugins::executor::MetadataRefreshRequest;
use crate::plugins::lifecycle::PluginId;
use crate::services::{
    EntityType,
    covers::providers::DEFAULT_COVER_HANDLER_TIMEOUT,
    metadata::lyrics::{
        providers as lyrics_dispatcher,
        providers::{
            DEFAULT_HANDLER_TIMEOUT,
            LyricsRequireSpec,
            RegisteredHandler,
        },
    },
    options::{
        OptionDeclaration,
        OptionType,
    },
    providers::{
        PROVIDER_REGISTRY,
        ProviderCallbackHandle,
        ProviderCoverRequireSpec,
        ProviderCoverSpec,
        ProviderIdSpec,
        ProviderIdUrlGenerator,
    },
};

#[derive(Clone, Default)]
pub(crate) struct MetadataModuleStore {
    db: Option<DbAsync>,
}

impl MetadataModuleStore {
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: DbAsync) -> Self {
        Self { db: Some(db) }
    }
}

#[derive(Default)]
pub(crate) struct MetadataCallbackRegistry {
    next_handler_id: Cell<u64>,
    handlers: RefCell<HashMap<u64, MetadataCallback>>,
}

impl MetadataCallbackRegistry {
    pub(crate) fn new() -> Self {
        Self {
            next_handler_id: Cell::new(1),
            handlers: RefCell::new(HashMap::new()),
        }
    }

    fn register(
        &self,
        _provider_id: String,
        _entity_type: EntityType,
        function: luau::Function,
        context: CallContext,
    ) -> u64 {
        let id = self.next_handler_id.get();
        self.next_handler_id.set(id.saturating_add(1));
        self.handlers
            .borrow_mut()
            .insert(id, MetadataCallback { function, context });
        id
    }

    pub(crate) fn get(&self, id: u64) -> Option<MetadataCallback> {
        self.handlers.borrow().get(&id).cloned()
    }
}

#[derive(Clone)]
pub(crate) struct MetadataCallback {
    pub(crate) function: luau::Function,
    pub(crate) context: CallContext,
}

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/metadata")
        .capability("lyra.metadata")
        .function(provider_new_spec())
        .function(ids_for_provider_spec())
        .userdata(EntityType::_harmony_userdata_spec())
        .userdata(server_db::ArtistType::_harmony_userdata_spec())
        .userdata(server_db::CreditType::_harmony_userdata_spec())
        .userdata(server_db::ArtistRelationType::_harmony_userdata_spec())
        .userdata(MetadataProvider::_harmony_userdata_spec())
        .userdata(MetadataLayer::_harmony_userdata_spec())
        .install(|_| Ok(ModuleExport::new(MetadataModule)))
}

struct MetadataModule;

fn provider_new_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("Provider.new")
        .arg_name("id")
        .args::<String>()
        .returns::<MetadataProvider>()
        .call(provider_new_callback)
}

fn ids_for_provider_spec() -> FunctionSpec {
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
        let _registration = STATE
            .plugin_registries
            .ensure_registrations_open(&plugin_id)
            .await
            .map_err(crate::plugins::runtime_error)?;
        let mut registry = PROVIDER_REGISTRY.write().await;
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
                    id: nanoid::nanoid!(),
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
        let _registration = STATE
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
struct MetadataProvider {
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
        let mut registry = PROVIDER_REGISTRY.write().await;
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
        let mut registry = PROVIDER_REGISTRY.write().await;
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
        let mut registry = PROVIDER_REGISTRY.write().await;
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
        let mut registry = PROVIDER_REGISTRY.write().await;
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
        let mut registry = PROVIDER_REGISTRY.write().await;
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
            let registry = PROVIDER_REGISTRY.read().await;
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
            let registry = PROVIDER_REGISTRY.read().await;
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
                .is_some_and(|edges| edges.iter().any(|edge| edge.to == Some(artist_id)))
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
    let layer = MetadataLayer {
        store,
        state: Arc::new(Mutex::new(LayerBuilderState {
            node_id,
            provider_id: provider.provider_id.clone(),
            fields: HashMap::new(),
            external_ids: HashMap::new(),
            custom_fields: HashMap::new(),
            remove_custom_field_versions: HashSet::new(),
        })),
    };
    MetadataLayer::_harmony_userdata_class().create_value(vm, origin, layer)
}

#[harmony_macros::userdata(name = "Layer", description = "Metadata layer builder.")]
#[derive(Clone)]
struct MetadataLayer {
    store: MetadataModuleStore,
    state: Arc<Mutex<LayerBuilderState>>,
}

#[derive(Clone)]
struct LayerBuilderState {
    node_id: DbId,
    provider_id: String,
    fields: HashMap<String, JsonValue>,
    external_ids: HashMap<String, String>,
    custom_fields: HashMap<u64, HashMap<String, JsonValue>>,
    remove_custom_field_versions: HashSet<u64>,
}

#[harmony_macros::userdata_methods]
impl MetadataLayer {
    fn set_id(&self, id_type: String, id_value: String) -> luau::runtime::Result<()> {
        if id_type.trim().is_empty() {
            return Err(crate::plugins::runtime_error(
                "metadata layer id key must not be empty",
            ));
        }
        self.state
            .lock()
            .expect("metadata layer mutex poisoned")
            .external_ids
            .insert(id_type, id_value);
        Ok(())
    }

    #[harmony(args(name: String, value: luau::JsonValue))]
    fn set_field(
        &self,
        vm: &luau::Vm,
        name: String,
        value: luau::Value,
    ) -> luau::runtime::Result<()> {
        if name.trim().is_empty() {
            return Err(crate::plugins::runtime_error(
                "metadata layer field key must not be empty",
            ));
        }
        if name == "duration_ms" {
            return Err(crate::plugins::runtime_error(
                "duration_ms is read-only and cannot be set by plugins",
            ));
        }
        let value = harmony_json::luau_to_json(vm, &value, 0)?;
        self.state
            .lock()
            .expect("metadata layer mutex poisoned")
            .fields
            .insert(name, value);
        Ok(())
    }

    fn save(&self) -> luau::runtime::Result<()> {
        let Some(db) = self.store.db.clone() else {
            return Err(crate::plugins::runtime_error(
                "metadata layer save requires a database-backed plugin executor",
            ));
        };
        let layer = self
            .state
            .lock()
            .expect("metadata layer mutex poisoned")
            .clone();
        futures::executor::block_on(async {
            let mut db = db.write().await;
            crate::services::metadata::layers::save_provider_layer(
                &mut db,
                layer.node_id,
                &layer.provider_id,
                &layer.fields,
                &layer.external_ids,
                &layer.custom_fields,
                &layer.remove_custom_field_versions,
            )
            .map_err(crate::plugins::runtime_error)
        })
    }

    #[harmony(args(version: String, name: String, value: luau::JsonValue))]
    fn set_custom_field(
        &self,
        vm: &luau::Vm,
        version: String,
        name: String,
        value: luau::Value,
    ) -> luau::runtime::Result<()> {
        let version = parse_custom_field_version(&version)?;
        let name = name.trim().to_string();
        if name.is_empty() {
            return Err(crate::plugins::runtime_error(
                "custom field name must be a non-empty string",
            ));
        }
        let value = harmony_json::luau_to_json(vm, &value, 0)?;
        let mut layer = self.state.lock().expect("metadata layer mutex poisoned");
        layer.remove_custom_field_versions.remove(&version);
        layer
            .custom_fields
            .entry(version)
            .or_default()
            .insert(name, value);
        Ok(())
    }

    #[harmony(args(version: String, fields: luau::JsonValue))]
    fn set_custom_fields(
        &self,
        vm: &luau::Vm,
        version: String,
        fields: luau::Value,
    ) -> luau::runtime::Result<()> {
        let version = parse_custom_field_version(&version)?;
        let JsonValue::Object(fields) = harmony_json::luau_to_json(vm, &fields, 0)? else {
            return Err(crate::plugins::runtime_error(
                "custom fields payload must be a JSON object",
            ));
        };
        let mut layer = self.state.lock().expect("metadata layer mutex poisoned");
        layer.remove_custom_field_versions.remove(&version);
        layer
            .custom_fields
            .insert(version, fields.into_iter().collect());
        Ok(())
    }

    fn clear_custom_fields(&self, version: String) -> luau::runtime::Result<()> {
        let version = parse_custom_field_version(&version)?;
        let mut layer = self.state.lock().expect("metadata layer mutex poisoned");
        layer.custom_fields.remove(&version);
        layer.remove_custom_field_versions.insert(version);
        Ok(())
    }
}

fn core_call_context(context: &luau::CallContext) -> CallContext {
    let mut caller = harmony_core::ContextBag::default();
    for (type_id, value) in context.caller.cloned_entries() {
        caller.insert_shared(type_id, value);
    }

    CallContext {
        origin: harmony_core::ChunkOrigin {
            module: context
                .origin
                .module
                .as_ref()
                .map(|module| harmony_core::ModuleId(module.0.clone())),
            plugin: context.origin.plugin.clone(),
            path: context.origin.path.clone(),
        },
        capability: context
            .capability
            .as_ref()
            .map(|capability| harmony_core::CapabilityId(capability.0.clone())),
        caller,
        task_group: harmony_core::TaskGroupId(context.task_group.0),
    }
}

fn require_positive_id(value: i64, label: &str) -> luau::runtime::Result<DbId> {
    if value <= 0 {
        return Err(crate::plugins::runtime_error(format!(
            "{label} must be a positive integer, got {value}"
        )));
    }
    Ok(DbId(value))
}

fn entity_type_for_node(db: &agdb::DbAny, node_id: DbId) -> anyhow::Result<Option<EntityType>> {
    if server_db::releases::get_by_id(db, node_id)?.is_some() {
        return Ok(Some(EntityType::Release));
    }
    if server_db::artists::get_by_id(db, node_id)?.is_some() {
        return Ok(Some(EntityType::Artist));
    }
    if server_db::tracks::get_by_id(db, node_id)?.is_some() {
        return Ok(Some(EntityType::Track));
    }

    Ok(None)
}

fn required_table_string(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
    method: &str,
) -> luau::runtime::Result<String> {
    optional_table_string(vm, table, key, method)?.ok_or_else(|| {
        crate::plugins::runtime_error(format!("{method}: {key} must be a non-empty string"))
    })
}

fn optional_table_string(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
    method: &str,
) -> luau::runtime::Result<Option<String>> {
    match table.get_raw(vm, key)? {
        luau::Value::Nil => Ok(None),
        luau::Value::String(bytes) => {
            let value = String::from_utf8(bytes).map_err(crate::plugins::runtime_error)?;
            let trimmed = value.trim().to_string();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed))
            }
        }
        other => Err(crate::plugins::runtime_error(format!(
            "{method}: {key} must be a string, got {}",
            other.type_name()
        ))),
    }
}

fn parse_custom_field_version(raw: &str) -> luau::runtime::Result<u64> {
    let version = raw.trim();
    let Some(number) = version.strip_prefix('v') else {
        return Err(crate::plugins::runtime_error(
            "custom fields version must be formatted as vN with N a positive integer",
        ));
    };
    if number.is_empty()
        || (number.len() > 1 && number.starts_with('0'))
        || !number.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(crate::plugins::runtime_error(
            "custom fields version must be formatted as vN with N a positive integer",
        ));
    }
    let parsed = number.parse::<u64>().map_err(|_| {
        crate::plugins::runtime_error(
            "custom fields version must be formatted as vN with N a positive integer",
        )
    })?;
    if parsed == 0 {
        return Err(crate::plugins::runtime_error(
            "custom fields version must be formatted as vN with N a positive integer",
        ));
    }
    Ok(parsed)
}

fn ensure_provider_owner(
    context: &luau::CallContext,
    plugin_id: &PluginId,
    provider_id: &str,
) -> luau::runtime::Result<()> {
    let caller = context.origin.plugin.as_deref();
    if caller == Some(plugin_id.as_ref()) {
        return Ok(());
    }

    Err(crate::plugins::runtime_error(format!(
        "provider '{provider_id}' method must be called by owning plugin '{plugin_id}'"
    )))
}

fn parse_id_spec(vm: &luau::Vm, spec: &luau::Table) -> luau::runtime::Result<ProviderIdSpec> {
    let id = required_string(vm, spec, "id")?;
    let entity = required_entity_type(vm, spec, "entity")?;
    let unique = optional_bool(vm, spec, "unique")?.unwrap_or(false);
    Ok(ProviderIdSpec { id, entity, unique })
}

fn parse_cover_spec(
    vm: &luau::Vm,
    config: &luau::Table,
) -> luau::runtime::Result<(i64, Duration, ProviderCoverRequireSpec)> {
    let priority = optional_i64(vm, config, "priority", "provider:cover")?.unwrap_or(50);
    let timeout = optional_timeout(
        vm,
        config,
        "timeout_ms",
        "provider:cover",
        DEFAULT_COVER_HANDLER_TIMEOUT,
    )?;
    let require = parse_cover_require(vm, config, "provider:cover")?;
    Ok((priority, timeout, require))
}

fn parse_lyrics_spec(
    vm: &luau::Vm,
    config: &luau::Table,
) -> luau::runtime::Result<(i32, Duration, LyricsRequireSpec)> {
    let priority = optional_i64(vm, config, "priority", "provider:lyrics")?.unwrap_or(50);
    let priority = i32::try_from(priority).map_err(|_| {
        crate::plugins::runtime_error("provider:lyrics config.priority must fit in i32")
    })?;
    let timeout = optional_timeout(
        vm,
        config,
        "timeout_ms",
        "provider:lyrics",
        DEFAULT_HANDLER_TIMEOUT,
    )?;
    let require = parse_lyrics_require(vm, config)?;
    Ok((priority, timeout, require))
}

fn optional_i64(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
    method: &str,
) -> luau::runtime::Result<Option<i64>> {
    match table.get_raw(vm, key)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Integer(value) => Ok(Some(value)),
        luau::Value::Number(value) if value.is_finite() && value.fract() == 0.0 => {
            Ok(Some(value as i64))
        }
        other => Err(crate::plugins::runtime_error(format!(
            "{method} config.{key} must be an integer number, got {}",
            other.type_name()
        ))),
    }
}

fn optional_timeout(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
    method: &str,
    default: Duration,
) -> luau::runtime::Result<Duration> {
    let Some(value) = optional_i64(vm, table, key, method)? else {
        return Ok(default);
    };
    if value < 1 {
        return Err(crate::plugins::runtime_error(format!(
            "{method} config.{key} must be an integer >= 1"
        )));
    }
    Ok(Duration::from_millis(value as u64))
}

fn parse_cover_require(
    vm: &luau::Vm,
    config: &luau::Table,
    method: &str,
) -> luau::runtime::Result<ProviderCoverRequireSpec> {
    match config.get_raw(vm, "require")? {
        luau::Value::Nil => Ok(ProviderCoverRequireSpec::default()),
        luau::Value::Table(require) => Ok(ProviderCoverRequireSpec {
            all_of: parse_require_paths(vm, &require, "all_of", method)?,
            any_of: parse_require_paths(vm, &require, "any_of", method)?,
        }),
        other => Err(crate::plugins::runtime_error(format!(
            "{method} config.require must be a table, got {}",
            other.type_name()
        ))),
    }
}

fn parse_lyrics_require(
    vm: &luau::Vm,
    config: &luau::Table,
) -> luau::runtime::Result<LyricsRequireSpec> {
    match config.get_raw(vm, "require")? {
        luau::Value::Nil => Ok(LyricsRequireSpec::default()),
        luau::Value::Table(require) => Ok(LyricsRequireSpec {
            all_of: parse_require_paths(vm, &require, "all_of", "provider:lyrics")?,
            any_of: parse_require_paths(vm, &require, "any_of", "provider:lyrics")?,
        }),
        other => Err(crate::plugins::runtime_error(format!(
            "provider:lyrics config.require must be a table, got {}",
            other.type_name()
        ))),
    }
}

fn parse_require_paths(
    vm: &luau::Vm,
    require: &luau::Table,
    key: &str,
    method: &str,
) -> luau::runtime::Result<Vec<String>> {
    match require.get_raw(vm, key)? {
        luau::Value::Nil => Ok(Vec::new()),
        luau::Value::Table(table) => {
            let mut parsed = Vec::new();
            for (_, value) in table.pairs_raw(vm)? {
                let luau::Value::String(bytes) = value else {
                    return Err(crate::plugins::runtime_error(format!(
                        "{method} require.{key} must contain only strings"
                    )));
                };
                let path = String::from_utf8(bytes)
                    .map_err(crate::plugins::runtime_error)?
                    .trim()
                    .to_string();
                if path.is_empty() {
                    return Err(crate::plugins::runtime_error(format!(
                        "{method} require.{key} must contain non-empty strings"
                    )));
                }
                if !parsed.contains(&path) {
                    parsed.push(path);
                }
            }
            Ok(parsed)
        }
        other => Err(crate::plugins::runtime_error(format!(
            "{method} require.{key} must be an array of strings, got {}",
            other.type_name()
        ))),
    }
}

fn parse_id_url_template(bytes: Vec<u8>) -> luau::runtime::Result<String> {
    let template = String::from_utf8(bytes)
        .map_err(|_| crate::plugins::runtime_error("provider:id URL template must be utf-8"))?
        .trim()
        .to_string();
    if template.is_empty() {
        return Err(crate::plugins::runtime_error(
            "provider:id URL template must be a non-empty string",
        ));
    }
    if template.matches("{id}").count() != 1 {
        return Err(crate::plugins::runtime_error(
            "provider:id URL template must contain exactly one {id} placeholder",
        ));
    }
    Ok(template)
}

fn parse_option_declaration(
    vm: &luau::Vm,
    config: &luau::Table,
) -> luau::runtime::Result<OptionDeclaration> {
    let name = required_string(vm, config, "name")?;
    let label = required_string(vm, config, "label")?;
    let option_type = match required_string(vm, config, "type")?.as_str() {
        "boolean" => OptionType::Boolean,
        "string" => OptionType::String,
        "number" => OptionType::Number,
        other => {
            return Err(crate::plugins::runtime_error(format!(
                "declare_option: unsupported type '{other}', expected 'boolean', 'string', or 'number'"
            )));
        }
    };
    let default = match (option_type.clone(), config.get_raw(vm, "default")?) {
        (_, luau::Value::Nil) => JsonValue::Null,
        (OptionType::Boolean, luau::Value::Boolean(value)) => JsonValue::Bool(value),
        (OptionType::Number, luau::Value::Integer(value)) => serde_json::json!(value),
        (OptionType::Number, luau::Value::Number(value)) => serde_json::json!(value),
        (OptionType::String, luau::Value::String(bytes)) => {
            JsonValue::String(String::from_utf8(bytes).map_err(crate::plugins::runtime_error)?)
        }
        (expected, actual) => {
            return Err(crate::plugins::runtime_error(format!(
                "declare_option: default value type mismatch for '{name}': expected {expected:?}, got {}",
                actual.type_name()
            )));
        }
    };
    let requires_settings = match config.get_raw(vm, "requires_settings")? {
        luau::Value::Nil => Vec::new(),
        luau::Value::Table(table) => string_array_from_table(vm, &table)?,
        other => {
            return Err(crate::plugins::runtime_error(format!(
                "declare_option: requires_settings must be a string array, got {}",
                other.type_name()
            )));
        }
    };

    Ok(OptionDeclaration {
        name,
        label,
        option_type,
        default,
        requires_settings,
    })
}

fn required_string(vm: &luau::Vm, table: &luau::Table, key: &str) -> luau::runtime::Result<String> {
    match table.get_raw(vm, key)? {
        luau::Value::String(bytes) => {
            let value = String::from_utf8(bytes).map_err(crate::plugins::runtime_error)?;
            let trimmed = value.trim().to_string();
            if trimmed.is_empty() {
                Err(crate::plugins::runtime_error(format!(
                    "{key} must be non-empty"
                )))
            } else {
                Ok(trimmed)
            }
        }
        other => Err(crate::plugins::runtime_error(format!(
            "{key} must be a non-empty string, got {}",
            other.type_name()
        ))),
    }
}

fn required_entity_type(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &'static str,
) -> luau::runtime::Result<EntityType> {
    let value = table.get_raw(vm, key)?;
    EntityType::_harmony_userdata_class().read_value(vm, key, value)
}

fn optional_artist_type(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &'static str,
) -> luau::runtime::Result<Option<server_db::ArtistType>> {
    let value = table.get_raw(vm, key)?;
    if matches!(value, luau::Value::Nil) {
        return Ok(None);
    }
    server_db::ArtistType::_harmony_userdata_class()
        .read_value(vm, key, value)
        .map(Some)
}

fn optional_bool(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Option<bool>> {
    match table.get_raw(vm, key)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Boolean(value) => Ok(Some(value)),
        other => Err(crate::plugins::runtime_error(format!(
            "{key} must be a boolean, got {}",
            other.type_name()
        ))),
    }
}

fn string_array_from_table(
    vm: &luau::Vm,
    table: &luau::Table,
) -> luau::runtime::Result<Vec<String>> {
    let mut values = Vec::new();
    for (_, value) in table.pairs_raw(vm)? {
        let luau::Value::String(bytes) = value else {
            return Err(crate::plugins::runtime_error(
                "array values must be strings",
            ));
        };
        values.push(String::from_utf8(bytes).map_err(crate::plugins::runtime_error)?);
    }
    Ok(values)
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &metadata_module_descriptor(),
        &metadata_type_aliases(),
        &metadata_interfaces(),
        &metadata_classes(),
    )
}

#[cfg(feature = "docgen")]
fn metadata_module_descriptor() -> ModuleDescriptor {
    let mut descriptor = ModuleDescriptor::new("Metadata", "metadata", None);
    descriptor.functions.extend([
        ModuleFunctionDescriptor {
            path: vec!["Provider", "new"],
            description: Some("Creates a metadata provider registration object."),
            params: vec![param("id", string())],
            returns: vec![ty("Provider")],
            yields: false,
        },
        ModuleFunctionDescriptor {
            path: vec!["ids", "for_provider"],
            description: Some("Returns external IDs for a single provider."),
            params: vec![
                param("external_ids", opt(ty("ExternalIdsByProvider"))),
                param("provider_id", string()),
            ],
            returns: vec![opt(ty("ProviderExternalIdMap"))],
            yields: false,
        },
    ]);
    descriptor
}

#[cfg(feature = "docgen")]
fn metadata_type_aliases() -> Vec<TypeAliasDescriptor> {
    vec![
        alias(
            "JsonValue",
            ty("(boolean | number | string | { JsonValue } | { [string]: JsonValue })?"),
        ),
        alias("ProviderExternalIdMap", map(string(), string())),
        alias(
            "ExternalIdsByProvider",
            map(string(), ty("ProviderExternalIdMap")),
        ),
        alias("ProviderCustomFieldMap", map(string(), ty("JsonValue"))),
        alias(
            "ProviderCustomFieldsByVersion",
            map(string(), ty("ProviderCustomFieldMap")),
        ),
        alias(
            "CustomFieldsByProvider",
            map(string(), ty("ProviderCustomFieldsByVersion")),
        ),
        alias("OptionValue", union([boolean(), string(), number()])),
        alias("ProviderSearchResult", map(string(), ty("JsonValue"))),
        alias(
            "ProviderSearchHandlerResult",
            opt(union([
                ty("ProviderSearchResult"),
                array(ty("ProviderSearchResult")),
            ])),
        ),
        alias(
            "ProviderSearchHandler",
            LuauType::function(
                vec![fn_param("query", string())],
                vec![ty("ProviderSearchHandlerResult")],
            ),
        ),
        alias(
            "ProviderCoverCandidate",
            union([
                string(),
                LuauType::object(vec![
                    field("url", opt(string())),
                    field("cover_url", opt(string())),
                    field("cover_image_url", opt(string())),
                    field("cover", opt(string())),
                    field("width", opt(number())),
                    field("height", opt(number())),
                ]),
            ]),
        ),
        alias(
            "ProviderCoverResult",
            union([
                ty("ProviderCoverCandidate"),
                LuauType::object(vec![
                    field("candidates", array(ty("ProviderCoverCandidate"))),
                    field("selected_index", opt(number())),
                ]),
            ]),
        ),
        alias(
            "ProviderCoverHandler",
            LuauType::function(
                vec![fn_param("ctx", ty("ProviderCoverContext"))],
                vec![opt(ty("ProviderCoverResult"))],
            ),
        ),
        alias(
            "ProviderLyricsHitResult",
            LuauType::object(vec![
                field("kind", LuauType::string_literal("hit")),
                field("candidates", array(ty("ProviderLyricsCandidate"))),
            ]),
        ),
        alias(
            "ProviderLyricsMissResult",
            LuauType::object(vec![field("kind", LuauType::string_literal("miss"))]),
        ),
        alias(
            "ProviderLyricsInstrumentalResult",
            LuauType::object(vec![field(
                "kind",
                LuauType::string_literal("instrumental"),
            )]),
        ),
        alias(
            "ProviderLyricsRateLimitedResult",
            LuauType::object(vec![
                field("kind", LuauType::string_literal("rate_limited")),
                field("retry_after_ms", opt(number())),
            ]),
        ),
        alias(
            "ProviderLyricsResult",
            union([
                ty("ProviderLyricsHitResult"),
                ty("ProviderLyricsMissResult"),
                ty("ProviderLyricsInstrumentalResult"),
                ty("ProviderLyricsRateLimitedResult"),
            ]),
        ),
        alias(
            "ProviderLyricsHandler",
            LuauType::function(
                vec![fn_param("ctx", ty("ProviderLyricsContext"))],
                vec![ty("ProviderLyricsResult")],
            ),
        ),
        alias(
            "ProviderRefreshContext",
            union([
                ty("ReleaseRefreshContext"),
                ty("ArtistRefreshContext"),
                ty("TrackRefreshContext"),
            ]),
        ),
        alias(
            "ProviderRefreshHandler",
            union([
                LuauType::function(
                    vec![fn_param("ctx", ty("ReleaseRefreshContext"))],
                    vec![ty("nil")],
                ),
                LuauType::function(
                    vec![fn_param("ctx", ty("ArtistRefreshContext"))],
                    vec![ty("nil")],
                ),
                LuauType::function(
                    vec![fn_param("ctx", ty("TrackRefreshContext"))],
                    vec![ty("nil")],
                ),
            ]),
        ),
        alias(
            "ProviderRefreshFilter",
            union([
                LuauType::function(
                    vec![fn_param("ctx", ty("ReleaseRefreshContext"))],
                    vec![boolean()],
                ),
                LuauType::function(
                    vec![fn_param("ctx", ty("ArtistRefreshContext"))],
                    vec![boolean()],
                ),
                LuauType::function(
                    vec![fn_param("ctx", ty("TrackRefreshContext"))],
                    vec![boolean()],
                ),
            ]),
        ),
    ]
}

#[cfg(feature = "docgen")]
fn metadata_interfaces() -> Vec<InterfaceDescriptor> {
    vec![
        interface(
            "MetadataIdRow",
            vec![
                field("provider_id", string()),
                field("id_type", string()),
                field("id_value", string()),
            ],
        ),
        interface(
            "ProviderIdRegistration",
            vec![
                field("id", string()),
                field("entity", ty("EntityType")),
                field("unique", opt(boolean())),
            ],
        ),
        interface(
            "OptionConfig",
            vec![
                field("name", string()),
                field("label", string()),
                field("type", string()),
                field("default", opt(ty("OptionValue"))),
                field("requires_settings", opt(array(string()))),
            ],
        ),
        interface(
            "ReleaseRefreshLookupHints",
            vec![
                field("artist_name", opt(string())),
                field("release_title", opt(string())),
                field("year", opt(number())),
            ],
        ),
        interface(
            "ReleaseRefreshArtist",
            vec![
                field("db_id", opt(number())),
                field("artist_name", string()),
                field("sort_name", opt(string())),
                field("custom_fields", opt(ty("CustomFieldsByProvider"))),
            ],
        ),
        interface(
            "ReleaseRefreshTrackArtist",
            vec![
                field("db_id", opt(number())),
                field("artist_name", string()),
                field("custom_fields", opt(ty("CustomFieldsByProvider"))),
            ],
        ),
        interface(
            "ReleaseRefreshTrack",
            vec![
                field("db_id", opt(number())),
                field("track_title", string()),
                field("sort_title", opt(string())),
                field("disc", opt(number())),
                field("track", opt(number())),
                field("track_total", opt(number())),
                field("duration_ms", opt(number())),
                field("external_ids", ty("ExternalIdsByProvider")),
                field("custom_fields", opt(ty("CustomFieldsByProvider"))),
                field("artists", array(ty("ReleaseRefreshTrackArtist"))),
            ],
        ),
        interface(
            "ReleaseRefreshContext",
            vec![
                field("db_id", opt(number())),
                field("id", opt(string())),
                field("release_title", opt(string())),
                field("sort_title", opt(string())),
                field("release_date", opt(string())),
                field("locked", opt(boolean())),
                field("created_at", opt(number())),
                field("ctime", opt(number())),
                field("lookup_hints", opt(ty("ReleaseRefreshLookupHints"))),
                field("external_ids", opt(ty("ExternalIdsByProvider"))),
                field("custom_fields", opt(ty("CustomFieldsByProvider"))),
                field("artists", opt(array(ty("ReleaseRefreshArtist")))),
                field("tracks", opt(array(ty("ReleaseRefreshTrack")))),
                field("library_id", opt(number())),
                field("options", opt(map(string(), ty("OptionValue")))),
            ],
        ),
        interface(
            "ArtistRefreshContext",
            vec![
                field("db_id", opt(number())),
                field("id", opt(string())),
                field("artist_name", opt(string())),
                field("sort_name", opt(string())),
                field("artist_type", opt(string())),
                field("description", opt(string())),
                field("external_ids", opt(ty("ExternalIdsByProvider"))),
                field("custom_fields", opt(ty("CustomFieldsByProvider"))),
                field("options", opt(map(string(), ty("OptionValue")))),
            ],
        ),
        interface(
            "TrackRefreshRelease",
            vec![
                field("db_id", opt(number())),
                field("id", opt(string())),
                field("release_title", opt(string())),
                field("sort_title", opt(string())),
                field("release_date", opt(string())),
                field("custom_fields", opt(ty("CustomFieldsByProvider"))),
            ],
        ),
        interface(
            "TrackRefreshContext",
            vec![
                field("db_id", opt(number())),
                field("id", opt(string())),
                field("track_title", opt(string())),
                field("sort_title", opt(string())),
                field("year", opt(number())),
                field("disc", opt(number())),
                field("disc_total", opt(number())),
                field("track", opt(number())),
                field("track_total", opt(number())),
                field("duration_ms", opt(number())),
                field("external_ids", opt(ty("ExternalIdsByProvider"))),
                field("custom_fields", opt(ty("CustomFieldsByProvider"))),
                field("artists", opt(array(ty("ReleaseRefreshTrackArtist")))),
                field("releases", opt(array(ty("TrackRefreshRelease")))),
                field("options", opt(map(string(), ty("OptionValue")))),
            ],
        ),
        interface(
            "ProviderCoverRequire",
            vec![
                field("all_of", opt(array(string()))),
                field("any_of", opt(array(string()))),
            ],
        ),
        interface(
            "ProviderCoverConfig",
            vec![
                field("priority", opt(number())),
                field("timeout_ms", opt(number())),
                field("require", opt(ty("ProviderCoverRequire"))),
            ],
        ),
        interface(
            "ProviderCoverOptions",
            vec![field("force_refresh", opt(boolean()))],
        ),
        interface(
            "ProviderCoverLibrary",
            vec![
                field("db_id", opt(number())),
                field("name", opt(string())),
                field("directory", opt(string())),
                field("language", opt(string())),
                field("country", opt(string())),
            ],
        ),
        interface(
            "ProviderCoverArtist",
            vec![
                field("db_id", opt(number())),
                field("artist_name", opt(string())),
                field("sort_name", opt(string())),
            ],
        ),
        interface(
            "ProviderCoverTrack",
            vec![
                field("db_id", opt(number())),
                field("track_title", string()),
                field("sort_title", opt(string())),
                field("disc", opt(number())),
                field("track", opt(number())),
                field("track_total", opt(number())),
                field("duration_ms", opt(number())),
            ],
        ),
        interface(
            "ProviderCoverContext",
            vec![
                field("db_id", opt(number())),
                field("release_title", opt(string())),
                field("sort_title", opt(string())),
                field("release_date", opt(string())),
                field("tracks", opt(array(ty("ProviderCoverTrack")))),
                field("artists", opt(array(ty("ProviderCoverArtist")))),
                field("artist_names", opt(array(string()))),
                field("ids", opt(ty("ProviderExternalIdMap"))),
                field("library", opt(ty("ProviderCoverLibrary"))),
                field("cover_options", opt(ty("ProviderCoverOptions"))),
            ],
        ),
        interface(
            "ProviderLyricsRequire",
            vec![
                field("all_of", opt(array(string()))),
                field("any_of", opt(array(string()))),
            ],
        ),
        interface(
            "ProviderLyricsConfig",
            vec![
                field("priority", opt(number())),
                field("timeout_ms", opt(number())),
                field("require", opt(ty("ProviderLyricsRequire"))),
            ],
        ),
        interface(
            "ProviderLyricsContext",
            vec![
                field("track_db_id", number()),
                field("track_name", string()),
                field("artist_name", string()),
                field("album_name", opt(string())),
                field("duration_ms", opt(number())),
                field("external_ids", opt(ty("ExternalIdsByProvider"))),
                field("force_refresh", boolean()),
            ],
        ),
        interface(
            "ProviderLyricWordInput",
            vec![
                field("ts_ms", number()),
                field("char_start", number()),
                field("char_end", number()),
            ],
        ),
        interface(
            "ProviderLyricLineInput",
            vec![
                field("ts_ms", number()),
                field("text", string()),
                field("words", array(ty("ProviderLyricWordInput"))),
            ],
        ),
        interface(
            "ProviderLyricsInput",
            vec![
                field("id", string()),
                field("language", string()),
                field("plain_text", string()),
                field("lines", array(ty("ProviderLyricLineInput"))),
            ],
        ),
        interface(
            "ProviderLyricsCandidate",
            vec![
                field("lyrics", ty("ProviderLyricsInput")),
                field("title", string()),
                field("artist", string()),
                field("duration_ms", opt(number())),
                field("language", opt(string())),
            ],
        ),
        interface(
            "EnsureArtistRequest",
            vec![
                field("id_type", string()),
                field("id_value", string()),
                field("artist_name", opt(string())),
                field("sort_name", opt(string())),
                field("artist_type", opt(ty("ArtistType"))),
                field("description", opt(string())),
            ],
        ),
    ]
}

#[cfg(feature = "docgen")]
fn metadata_classes() -> Vec<ClassDescriptor> {
    vec![
        <EntityType as harmony_luau::DescribeUserData>::class_descriptor(),
        <server_db::ArtistType as harmony_luau::DescribeUserData>::class_descriptor(),
        <server_db::CreditType as harmony_luau::DescribeUserData>::class_descriptor(),
        <server_db::ArtistRelationType as harmony_luau::DescribeUserData>::class_descriptor(),
        layer_class(),
        provider_class(),
    ]
}

#[cfg(feature = "docgen")]
fn layer_class() -> ClassDescriptor {
    let mut class = ClassDescriptor::new("Layer", None);
    class.methods.extend([
        method(
            "set_field",
            vec![param("name", string()), param("value", ty("JsonValue"))],
            vec![],
        ),
        method(
            "set_id",
            vec![param("id_type", string()), param("id_value", string())],
            vec![],
        ),
        method(
            "set_custom_field",
            vec![
                param("version", string()),
                param("name", string()),
                param("value", ty("JsonValue")),
            ],
            vec![],
        ),
        method(
            "set_custom_fields",
            vec![param("version", string()), param("fields", ty("JsonValue"))],
            vec![],
        ),
        method(
            "clear_custom_fields",
            vec![param("version", string())],
            vec![],
        ),
        method("save", vec![], vec![]),
    ]);
    class
}

#[cfg(feature = "docgen")]
fn provider_class() -> ClassDescriptor {
    let mut class = ClassDescriptor::new("Provider", None);
    class.methods.extend([
        method(
            "id",
            vec![
                param("spec", ty("ProviderIdRegistration")),
                param(
                    "generator",
                    opt(union([
                        string(),
                        LuauType::function(vec![fn_param("id", string())], vec![string()]),
                    ])),
                ),
            ],
            vec![],
        ),
        method(
            "search",
            vec![
                param("entity", ty("EntityType")),
                param("handler", ty("ProviderSearchHandler")),
            ],
            vec![],
        ),
        method(
            "cover",
            vec![
                param("entity", ty("EntityType")),
                param("config", ty("ProviderCoverConfig")),
                param("handler", ty("ProviderCoverHandler")),
            ],
            vec![],
        ),
        method(
            "lyrics",
            vec![
                param("config", ty("ProviderLyricsConfig")),
                param("handler", ty("ProviderLyricsHandler")),
            ],
            vec![],
        ),
        method(
            "refresh",
            vec![
                param("entity", ty("EntityType")),
                param("handler", ty("ProviderRefreshHandler")),
                param("filter", opt(ty("ProviderRefreshFilter"))),
            ],
            vec![],
        ),
        method(
            "declare_option",
            vec![param("config", ty("OptionConfig"))],
            vec![],
        ),
        method(
            "ensure_artist",
            vec![param("request", ty("EnsureArtistRequest"))],
            vec![opt(number())],
        ),
        method(
            "mark_unmatched",
            vec![
                param("node_id", number()),
                param("id_types", array(string())),
            ],
            vec![],
        ),
        method(
            "link_credit",
            vec![
                param("owner_id", number()),
                param("artist_id", number()),
                param("credit_type", opt(ty("CreditType"))),
                param("detail", opt(string())),
            ],
            vec![],
        ),
        method(
            "link_artist_relation",
            vec![
                param("from_artist_id", number()),
                param("to_artist_id", number()),
                param("relation_type", ty("ArtistRelationType")),
                param("attributes", opt(string())),
            ],
            vec![],
        ),
        method("layer", vec![param("node_id", number())], vec![ty("Layer")]),
    ]);
    class
}

#[cfg(feature = "docgen")]
fn alias(name: &'static str, ty: LuauType) -> TypeAliasDescriptor {
    TypeAliasDescriptor::new(name, ty, None)
}

#[cfg(feature = "docgen")]
fn interface(name: &'static str, fields: Vec<FieldDescriptor>) -> InterfaceDescriptor {
    InterfaceDescriptor {
        name,
        description: None,
        fields,
    }
}

#[cfg(feature = "docgen")]
fn field(name: &'static str, ty: LuauType) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description: None,
    }
}

#[cfg(feature = "docgen")]
fn method(
    name: &'static str,
    params: Vec<ParameterDescriptor>,
    returns: Vec<LuauType>,
) -> MethodDescriptor {
    MethodDescriptor {
        name,
        description: None,
        params,
        returns,
        yields: false,
        kind: MethodKind::Instance,
    }
}

#[cfg(feature = "docgen")]
fn param(name: &'static str, ty: LuauType) -> ParameterDescriptor {
    ParameterDescriptor {
        name,
        ty,
        description: None,
        variadic: false,
    }
}

#[cfg(feature = "docgen")]
fn fn_param(name: &'static str, ty: LuauType) -> FunctionParameter {
    FunctionParameter {
        name: Some(name),
        ty,
        variadic: false,
    }
}

#[cfg(feature = "docgen")]
fn boolean() -> LuauType {
    bool::luau_type()
}

#[cfg(feature = "docgen")]
fn string() -> LuauType {
    String::luau_type()
}

#[cfg(feature = "docgen")]
fn number() -> LuauType {
    LuauType::literal("number")
}

#[cfg(feature = "docgen")]
fn ty(name: &'static str) -> LuauType {
    LuauType::literal(name)
}

#[cfg(feature = "docgen")]
fn opt(ty: LuauType) -> LuauType {
    LuauType::optional(ty)
}

#[cfg(feature = "docgen")]
fn array(ty: LuauType) -> LuauType {
    LuauType::array(ty)
}

#[cfg(feature = "docgen")]
fn map(key: LuauType, value: LuauType) -> LuauType {
    LuauType::map(key, value)
}

#[cfg(feature = "docgen")]
fn union<const N: usize>(types: [LuauType; N]) -> LuauType {
    LuauType::union(types.into())
}
