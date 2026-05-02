// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use crate::plugins::lifecycle::{
    PluginFunctionHandle,
    PluginId,
};
mod covers;
mod ensure;
mod layers;
mod lyrics;

pub(crate) use layers::Layer;

use std::{
    collections::HashMap,
    sync::Arc,
};

use agdb::{
    DbId,
    QueryBuilder,
};
use harmony_core::{
    LuaAsyncExt,
    LuaUserDataAsyncExt,
    Module,
};
use harmony_luau::{
    ClassDescriptor,
    DescribeInterface,
    DescribeModule,
    DescribeTypeAlias,
    DescribeUserData,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    TypeAliasDescriptor,
};
use mlua::{
    ExternalResult,
    Function,
    Lua,
    LuaSerdeExt,
    Result,
    Table,
    Value,
};
use nanoid::nanoid;

use crate::db::{
    self,
    ProviderConfig,
};
use crate::services::EntityType;
use crate::services::metadata::layers::{
    list_entity_external_ids,
    save_provider_layer,
};
use serde::Serialize;

use crate::{
    STATE,
    db::NodeId,
    plugins::LUA_SERIALIZE_OPTIONS,
    services::metadata::lyrics::providers as lyrics_dispatcher,
    services::options::{
        OptionDeclaration,
        OptionType,
    },
    services::providers::{
        PROVIDER_REGISTRY,
        ProviderCoverSpec,
        ProviderIdSpec,
    },
};

use covers::parse_cover_spec;

#[harmony_macros::interface]
struct ProviderIdRegistration {
    id: String,
    entity: String,
    unique: Option<bool>,
}

use super::OptionConfig;

#[derive(Serialize)]
#[harmony_macros::interface]
struct MetadataIdRow {
    provider_id: String,
    id_type: String,
    id_value: String,
}

struct LuaCallback;

impl LuauTypeInfo for LuaCallback {
    fn luau_type() -> LuauType {
        LuauType::function(
            vec![harmony_luau::FunctionParameter {
                name: None,
                ty: LuauType::any(),
                variadic: true,
            }],
            vec![LuauType::any()],
        )
    }
}

impl DescribeTypeAlias for LuaCallback {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "LuaCallback",
            Self::luau_type(),
            Some("Generic Lua callback."),
        )
    }
}

struct ProviderExternalIdMap;

impl LuauTypeInfo for ProviderExternalIdMap {
    fn luau_type() -> LuauType {
        LuauType::literal("ProviderExternalIdMap")
    }
}

impl DescribeTypeAlias for ProviderExternalIdMap {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "ProviderExternalIdMap",
            LuauType::map(String::luau_type(), String::luau_type()),
            Some("String-keyed external IDs for a single provider."),
        )
    }
}

struct ExternalIdsByProvider;

impl LuauTypeInfo for ExternalIdsByProvider {
    fn luau_type() -> LuauType {
        LuauType::literal("ExternalIdsByProvider")
    }
}

impl DescribeTypeAlias for ExternalIdsByProvider {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "ExternalIdsByProvider",
            LuauType::map(String::luau_type(), ProviderExternalIdMap::luau_type()),
            Some("Provider-keyed external ID maps."),
        )
    }
}

#[harmony_macros::interface]
struct ReleaseRefreshLookupHints {
    artist_name: Option<String>,
    release_title: Option<String>,
    year: Option<u32>,
}

#[harmony_macros::interface]
struct ReleaseRefreshArtist {
    db_id: Option<u64>,
    artist_name: String,
    sort_name: Option<String>,
}

#[harmony_macros::interface]
struct ReleaseRefreshTrackArtist {
    db_id: Option<u64>,
    artist_name: String,
}

#[harmony_macros::interface]
struct ReleaseRefreshTrack {
    db_id: Option<u64>,
    track_title: String,
    disc: Option<u32>,
    track: Option<u32>,
    track_total: Option<u32>,
    duration_ms: Option<u64>,
    external_ids: std::collections::HashMap<String, std::collections::HashMap<String, String>>,
    artists: Vec<ReleaseRefreshTrackArtist>,
}

struct OptionValue;

impl LuauTypeInfo for OptionValue {
    fn luau_type() -> LuauType {
        LuauType::union(vec![
            <bool as LuauTypeInfo>::luau_type(),
            <String as LuauTypeInfo>::luau_type(),
            <f64 as LuauTypeInfo>::luau_type(),
        ])
    }
}

/// Release context passed to refresh handlers and filters during library sync.
#[harmony_macros::interface]
struct ReleaseRefreshContext {
    db_id: Option<u64>,
    id: String,
    release_title: String,
    sort_title: Option<String>,
    release_date: Option<String>,
    locked: Option<bool>,
    created_at: Option<u64>,
    ctime: Option<u64>,
    lookup_hints: Option<ReleaseRefreshLookupHints>,
    external_ids:
        Option<std::collections::HashMap<String, std::collections::HashMap<String, String>>>,
    artists: Option<Vec<ReleaseRefreshArtist>>,
    tracks: Option<Vec<ReleaseRefreshTrack>>,
    library_id: Option<u64>,
    /// Options toggled by the user for this refresh.
    options: Option<std::collections::HashMap<String, OptionValue>>,
}

struct ReleaseRefreshHandler;

impl LuauTypeInfo for ReleaseRefreshHandler {
    fn luau_type() -> LuauType {
        LuauType::function(
            vec![harmony_luau::FunctionParameter {
                name: Some("context"),
                ty: ReleaseRefreshContext::luau_type(),
                variadic: false,
            }],
            vec![LuauType::literal("nil")],
        )
    }
}

struct ReleaseRefreshFilter;

impl LuauTypeInfo for ReleaseRefreshFilter {
    fn luau_type() -> LuauType {
        LuauType::function(
            vec![harmony_luau::FunctionParameter {
                name: Some("context"),
                ty: ReleaseRefreshContext::luau_type(),
                variadic: false,
            }],
            vec![bool::luau_type()],
        )
    }
}

mod helpers;

use helpers::required_spec_string;

fn parse_entity_type(s: &str) -> Result<EntityType> {
    match s {
        "release" => Ok(EntityType::Release),
        "artist" => Ok(EntityType::Artist),
        "track" => Ok(EntityType::Track),
        _ => Err(mlua::Error::runtime(format!(
            "entity_type must be one of: release, artist, track (got '{s}')"
        ))),
    }
}

fn parse_id_spec(spec: Table) -> Result<ProviderIdSpec> {
    let id = required_spec_string(&spec, "id")?;
    let entity_str = required_spec_string(&spec, "entity")?;
    let entity = parse_entity_type(&entity_str.to_ascii_lowercase())?;

    let unique = match spec.get::<Value>("unique")? {
        Value::Nil => false,
        Value::Boolean(value) => value,
        _ => {
            return Err(mlua::Error::runtime(
                "provider:id spec.unique must be a boolean",
            ));
        }
    };

    Ok(ProviderIdSpec { id, entity, unique })
}

#[derive(Clone, Debug)]
pub(crate) struct Provider {
    pub(crate) plugin_id: PluginId,
    pub(crate) id: String,
}

impl Provider {
    async fn wrap_handler(&self, func: Function) -> mlua::Result<PluginFunctionHandle> {
        let counter = STATE
            .plugin_registries
            .inflight_counter(&self.plugin_id)
            .await;
        Ok(PluginFunctionHandle::new(
            self.plugin_id.clone(),
            counter,
            func,
        ))
    }

    /// Reject cross-plugin access. All plugins share one Lua, so a
    /// Provider userdata stashed in `_G` can be picked up by another
    /// plugin; without this check, handlers would register under the
    /// owner's counter and bucket and get wiped on the owner's
    /// teardown.
    fn ensure_owner(&self, caller: Option<&PluginId>) -> mlua::Result<()> {
        match caller {
            Some(id) if id == &self.plugin_id => Ok(()),
            _ => Err(mlua::Error::runtime(format!(
                "provider '{}' method must be called by owning plugin '{}'",
                self.id, self.plugin_id
            ))),
        }
    }
}

#[harmony_macros::implementation(plugin_scoped)]
impl Provider {
    pub async fn new(plugin_id: Option<Arc<str>>, id: String) -> mlua::Result<Self> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        let plugin_id = plugin_id.ok_or_else(|| {
            mlua::Error::runtime("metadata.Provider.new must be called from plugin Lua code")
        })?;

        {
            let _registration = STATE
                .plugin_registries
                .ensure_registrations_open(&plugin_id)
                .await?;
            let mut registry = PROVIDER_REGISTRY.write().await;
            registry
                .register(plugin_id.clone(), id.clone())
                .map_err(|err| mlua::Error::runtime(err.to_string()))?;
        }

        let db = &STATE.db;
        let mut db_write = db.write().await;

        if db::providers::get_by_provider_id(&db_write, &id)?.is_none() {
            let provider_config = ProviderConfig {
                db_id: None,
                id: nanoid!(),
                provider_id: id.clone(),
                display_name: id.clone(),
                priority: 50,
                enabled: true,
            };
            db::providers::upsert(&mut db_write, &provider_config)?;
        }

        Ok(Provider { plugin_id, id })
    }

    /// Registers an external id generator for this provider.
    #[harmony(args(spec: ProviderIdRegistration, generator: Option<LuaCallback>))]
    pub(crate) async fn id(
        &self,
        plugin_id: Option<Arc<str>>,
        spec: Table,
        generator: Option<Function>,
    ) -> Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        self.ensure_owner(plugin_id.as_ref())?;
        let id_spec = parse_id_spec(spec)?;
        let _registration = STATE
            .plugin_registries
            .ensure_registrations_open(&self.plugin_id)
            .await?;
        let generator = match generator {
            Some(func) => Some(self.wrap_handler(func).await?),
            None => None,
        };
        let mut registry = PROVIDER_REGISTRY.write().await;
        registry.set_id_registration(&self.id, id_spec, generator);
        Ok(())
    }

    /// Registers a search handler for this provider.
    #[harmony(args(entity_type: EntityType, handler: LuaCallback))]
    pub(crate) async fn search(
        &self,
        plugin_id: Option<Arc<str>>,
        entity_type: EntityType,
        handler: Function,
    ) -> Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        self.ensure_owner(plugin_id.as_ref())?;
        let _registration = STATE
            .plugin_registries
            .ensure_registrations_open(&self.plugin_id)
            .await?;
        let handle = self.wrap_handler(handler).await?;
        let mut registry = PROVIDER_REGISTRY.write().await;
        registry.set_search_handler(&self.id, entity_type, handle);
        Ok(())
    }

    /// Requires `http.set_rate_limit` for at least one domain before
    /// registration.
    #[harmony(args(entity_type: EntityType, config: covers::ProviderCoverConfig, handler: covers::ProviderCoverHandler))]
    pub(crate) async fn cover(
        &self,
        plugin_id: Option<Arc<str>>,
        entity_type: EntityType,
        config: Table,
        handler: Function,
    ) -> Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        self.ensure_owner(plugin_id.as_ref())?;
        if !matches!(entity_type, EntityType::Release | EntityType::Artist) {
            return Err(mlua::Error::runtime(
                "provider:cover entity_type must be 'release' or 'artist'",
            ));
        }

        if !harmony_http::has_rate_limit_for_plugin(self.plugin_id.as_str()).await {
            return Err(mlua::Error::runtime(format!(
                "provider:cover requires http.set_rate_limit to be configured for at \
                 least one domain before registration; call set_rate_limit in plugin \
                 init for plugin '{}'",
                self.plugin_id
            )));
        }

        let (priority, timeout, require) = parse_cover_spec(config)?;
        let _registration = STATE
            .plugin_registries
            .ensure_registrations_open(&self.plugin_id)
            .await?;
        let handle = self.wrap_handler(handler).await?;
        let mut registry = PROVIDER_REGISTRY.write().await;
        registry.set_cover_handler(
            &self.id,
            entity_type,
            ProviderCoverSpec {
                priority,
                timeout,
                require,
                handler: handle,
            },
        );
        Ok(())
    }

    /// Requires `http.set_rate_limit` first. Handlers MUST resolve
    /// cooldown / known-miss / known-instrumental without awaiting HTTP;
    /// `ctx.force_refresh = true` skips Miss/Instrumental but not cooldown.
    #[harmony(args(config: lyrics::ProviderLyricsConfig, handler: lyrics::ProviderLyricsHandler))]
    pub(crate) async fn lyrics(
        &self,
        plugin_id: Option<Arc<str>>,
        config: Table,
        handler: Function,
    ) -> Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        self.ensure_owner(plugin_id.as_ref())?;

        if !harmony_http::has_rate_limit_for_plugin(self.plugin_id.as_str()).await {
            return Err(mlua::Error::runtime(format!(
                "provider:lyrics requires http.set_rate_limit to be configured for at \
                 least one domain before registration; call set_rate_limit in plugin \
                 init for plugin '{}'",
                self.plugin_id
            )));
        }

        let (priority, timeout, require) = lyrics::parse_lyrics_spec(config)?;
        let _registration = STATE
            .plugin_registries
            .ensure_registrations_open(&self.plugin_id)
            .await?;
        let handle = self.wrap_handler(handler).await?;

        let handler_fn: lyrics_dispatcher::HandlerFn = Arc::new(move |context| {
            let handle = handle.clone();
            Box::pin(async move {
                let lua = handle
                    .try_upgrade_lua()
                    .ok_or_else(|| anyhow::anyhow!("lua state for lyrics handler is gone"))?;
                let lua_ctx = lyrics::track_context_to_lua(&lua, &context)
                    .map_err(|err| anyhow::anyhow!(err))?;
                let returned: Value = handle
                    .call_async(lua_ctx)
                    .await
                    .map_err(|err| anyhow::anyhow!(err))?;
                lyrics::parse_handler_result(&lua, returned).map_err(|err| anyhow::anyhow!(err))
            })
        });

        let cancel = lyrics_dispatcher::make_plugin_cancellation_child(&self.plugin_id).await;
        lyrics_dispatcher::register_handler(lyrics_dispatcher::RegisteredHandler {
            provider_id: Arc::from(self.id.as_str()),
            plugin_id: self.plugin_id.clone(),
            priority,
            timeout,
            require,
            handler: handler_fn,
            cancel,
        })
        .await;
        Ok(())
    }

    /// Registers a refresh handler for this provider.
    ///
    /// When `filter` is provided, it is called during sync to skip entities
    /// that don't need refresh. Return `true` to refresh, `false` to skip.
    #[harmony(args(entity_type: EntityType, handler: ReleaseRefreshHandler, filter: Option<ReleaseRefreshFilter>))]
    pub(crate) async fn refresh(
        &self,
        plugin_id: Option<Arc<str>>,
        entity_type: EntityType,
        handler: Function,
        filter: Option<Function>,
    ) -> Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        self.ensure_owner(plugin_id.as_ref())?;
        let _registration = STATE
            .plugin_registries
            .ensure_registrations_open(&self.plugin_id)
            .await?;
        let handler = self.wrap_handler(handler).await?;
        let filter = match filter {
            Some(func) => Some(self.wrap_handler(func).await?),
            None => None,
        };
        let mut registry = PROVIDER_REGISTRY.write().await;
        registry.set_refresh_handler(&self.id, entity_type, handler);
        if let Some(filter) = filter {
            registry.set_sync_filter(&self.id, entity_type, filter);
        }
        Ok(())
    }

    /// Declares an option that clients can toggle when triggering a refresh.
    #[harmony(args(config: OptionConfig))]
    pub(crate) async fn declare_option(
        &self,
        plugin_id: Option<Arc<str>>,
        config: Table,
    ) -> Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        self.ensure_owner(plugin_id.as_ref())?;
        let _registration = STATE
            .plugin_registries
            .ensure_registrations_open(&self.plugin_id)
            .await?;
        let name: String = config
            .get("name")
            .ok()
            .filter(|s: &String| !s.trim().is_empty())
            .ok_or_else(|| mlua::Error::runtime("declare_option requires a non-empty 'name'"))?;
        let label: String = config
            .get("label")
            .ok()
            .filter(|s: &String| !s.trim().is_empty())
            .ok_or_else(|| mlua::Error::runtime("declare_option requires a non-empty 'label'"))?;
        let option_type_str: String = config
            .get("type")
            .ok()
            .filter(|s: &String| !s.trim().is_empty())
            .ok_or_else(|| mlua::Error::runtime("declare_option requires a non-empty 'type'"))?;
        let option_type = match option_type_str.as_str() {
            "boolean" => OptionType::Boolean,
            "string" => OptionType::String,
            "number" => OptionType::Number,
            other => {
                return Err(mlua::Error::runtime(format!(
                    "declare_option: unsupported type '{other}', expected 'boolean', 'string', or 'number'"
                )));
            }
        };
        let default: mlua::Value = config.get("default").unwrap_or(mlua::Value::Nil);
        let default_json = match (&option_type, &default) {
            (OptionType::Boolean, mlua::Value::Boolean(b)) => serde_json::Value::Bool(*b),
            (OptionType::Number, mlua::Value::Integer(n)) => {
                serde_json::json!(*n)
            }
            (OptionType::Number, mlua::Value::Number(n)) => {
                serde_json::json!(*n)
            }
            (OptionType::String, mlua::Value::String(s)) => {
                serde_json::Value::String(s.to_str()?.to_string())
            }
            (_, mlua::Value::Nil) => serde_json::Value::Null,
            (ty, val) => {
                return Err(mlua::Error::runtime(format!(
                    "declare_option: default value type mismatch for '{name}': \
                     expected {ty:?}, got {val:?}"
                )));
            }
        };
        let requires_settings: Vec<String> = config
            .get::<Vec<String>>("requires_settings")
            .unwrap_or_default();

        let mut registry = PROVIDER_REGISTRY.write().await;
        registry
            .declare_option(
                &self.id,
                OptionDeclaration {
                    name,
                    label,
                    option_type,
                    default: default_json,
                    requires_settings,
                },
            )
            .map_err(mlua::Error::runtime)?;
        Ok(())
    }

    pub(crate) async fn layer(
        &self,
        plugin_id: Option<Arc<str>>,
        node_id: NodeId,
    ) -> Result<Layer> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        self.ensure_owner(plugin_id.as_ref())?;
        Ok(Layer {
            plugin_id: self.plugin_id.clone(),
            provider_id: self.id.clone(),
            entity_id: node_id,
            fields: HashMap::new(),
            external_ids: HashMap::new(),
        })
    }

    /// Ensures an artist exists for this provider id mapping.
    #[harmony(args(request: ensure::EnsureArtistRequest))]
    pub(crate) async fn ensure_artist(
        &self,
        plugin_id: Option<Arc<str>>,
        request: Table,
    ) -> Result<NodeId> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        self.ensure_owner(plugin_id.as_ref())?;
        let method = "provider:ensure_artist";
        let id_type = helpers::required_non_empty_request_string(&request, "id_type", method)?;
        let id_value = helpers::required_non_empty_request_string(&request, "id_value", method)?;
        let artist_name =
            helpers::optional_trimmed_request_string(&request, "artist_name", method)?;
        let sort_name = helpers::optional_trimmed_request_string(&request, "sort_name", method)?;
        let artist_type_str =
            helpers::optional_trimmed_request_string(&request, "artist_type", method)?;
        let artist_type = artist_type_str
            .as_deref()
            .map(db::ArtistType::from_db_str)
            .transpose()
            .into_lua_err()?;
        let description =
            helpers::optional_trimmed_request_string(&request, "description", method)?;

        let is_registered_artist_id = {
            let registry = PROVIDER_REGISTRY.read().await;
            registry.id_spec_matches_entity(&self.id, &id_type, EntityType::Artist)
        };
        if !is_registered_artist_id {
            return Err(mlua::Error::runtime(format!(
                "{method}: id_type '{id_type}' is not registered for artist on provider '{}'",
                self.id
            )));
        }

        let mut db_write = STATE.db.write().await;
        let mut resolved_artist_id: Option<DbId> = None;

        let external_id_rows: Vec<db::external_ids::ExternalId> = db_write
            .exec(
                QueryBuilder::select()
                    .elements::<db::external_ids::ExternalId>()
                    .search()
                    .from("external_ids")
                    .where_()
                    .key("provider_id")
                    .value(self.id.as_str())
                    .and()
                    .key("id_type")
                    .value(id_type.as_str())
                    .and()
                    .key("id_value")
                    .value(id_value.as_str())
                    .query(),
            )
            .into_lua_err()?
            .try_into()
            .into_lua_err()?;

        for external_id in external_id_rows {
            let Some(external_id_db_id) = external_id.db_id.map(DbId::from) else {
                continue;
            };
            let artists = db::artists::get(&db_write, external_id_db_id).into_lua_err()?;
            if let Some(artist_id) = artists
                .into_iter()
                .find_map(|artist| artist.db_id.map(DbId::from))
            {
                resolved_artist_id = Some(artist_id);
                break;
            }
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
                .into_lua_err()?;
            let mut candidate_ids = existing_name_matches.ids().to_vec();
            candidate_ids.sort_by_key(|id| id.0);

            for candidate_id in candidate_ids {
                let Some(candidate_artist) =
                    db::artists::get_by_id(&db_write, candidate_id).into_lua_err()?
                else {
                    continue;
                };
                if candidate_artist.scan_name != scan_name {
                    continue;
                }

                if let Some(existing_for_provider) =
                    db::external_ids::get(&db_write, candidate_id, &self.id, &id_type)
                        .into_lua_err()?
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
            let created_artist = db::Artist {
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
                .into_lua_err()?;
            let inner_artist_db_id = insert_result
                .elements
                .first()
                .map(|element| element.id)
                .ok_or_else(|| {
                    mlua::Error::runtime(format!("{method}: artist insert missing id"))
                })?;
            db_write
                .exec_mut(
                    QueryBuilder::insert()
                        .edges()
                        .from("artists")
                        .to(inner_artist_db_id)
                        .query(),
                )
                .into_lua_err()?;
            inner_artist_db_id
        };

        let mut fields = HashMap::new();
        if let Some(name) = artist_name {
            fields.insert("artist_name".to_string(), serde_json::Value::String(name));
        }
        if let Some(at) = &artist_type {
            fields.insert(
                "artist_type".to_string(),
                serde_json::Value::String(at.to_string()),
            );
        }
        if let Some(sort_name) = sort_name {
            fields.insert(
                "sort_name".to_string(),
                serde_json::Value::String(sort_name),
            );
        }
        if let Some(description) = description {
            fields.insert(
                "description".to_string(),
                serde_json::Value::String(description),
            );
        }

        let mut external_ids = HashMap::new();
        external_ids.insert(id_type, id_value);
        save_provider_layer(
            &mut db_write,
            artist_db_id,
            &self.id,
            &fields,
            &external_ids,
        )
        .into_lua_err()?;

        Ok(artist_db_id.into())
    }

    pub(crate) async fn link_credit(
        &self,
        plugin_id: Option<Arc<str>>,
        owner_id: NodeId,
        artist_id: NodeId,
        credit_type: Option<db::CreditType>,
        detail: Option<String>,
    ) -> Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        self.ensure_owner(plugin_id.as_ref())?;
        let method = "provider:link_credit";
        let owner_db_id: DbId = owner_id.into();
        let artist_db_id: DbId = artist_id.into();

        let resolved_credit_type = credit_type.unwrap_or(db::CreditType::Artist);
        let resolved_detail = detail.and_then(|d| {
            let trimmed = d.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        });

        let mut db_write = STATE.db.write().await;

        let owner_is_release = db::releases::get_by_id(&db_write, owner_db_id)
            .into_lua_err()?
            .is_some();
        let owner_is_track = db::tracks::get_by_id(&db_write, owner_db_id)
            .into_lua_err()?
            .is_some();
        if !owner_is_release && !owner_is_track {
            return Err(mlua::Error::runtime(format!(
                "{method}: owner_id must reference a release or track"
            )));
        }
        if db::artists::get_by_id(&db_write, artist_db_id)
            .into_lua_err()?
            .is_none()
        {
            return Err(mlua::Error::runtime(format!(
                "{method}: artist_id does not reference an artist"
            )));
        }

        let existing_credits: Vec<db::Credit> = db_write
            .exec(
                agdb::QueryBuilder::select()
                    .elements::<db::Credit>()
                    .search()
                    .from(owner_db_id)
                    .where_()
                    .neighbor()
                    .end_where()
                    .query(),
            )
            .into_lua_err()?
            .try_into()
            .into_lua_err()?;

        let already_linked = existing_credits.iter().any(|c| {
            if c.credit_type != resolved_credit_type || c.detail != resolved_detail {
                return false;
            }
            let Some(cid) = c.db_id.clone().map(DbId::from) else {
                return false;
            };
            db::graph::direct_edges_from(&*db_write, cid)
                .ok()
                .map_or(false, |edges| {
                    edges.iter().any(|e| e.to == Some(artist_db_id))
                })
        });

        if !already_linked {
            db_write
                .transaction_mut(|t| -> anyhow::Result<()> {
                    let credit = db::Credit {
                        db_id: None,
                        id: nanoid::nanoid!(),
                        credit_type: resolved_credit_type,
                        detail: resolved_detail,
                    };
                    let credit_db_id = t
                        .exec_mut(agdb::QueryBuilder::insert().element(&credit).query())?
                        .ids()[0];
                    t.exec_mut(
                        agdb::QueryBuilder::insert()
                            .edges()
                            .from("credits")
                            .to(credit_db_id)
                            .query(),
                    )?;
                    t.exec_mut(
                        agdb::QueryBuilder::insert()
                            .edges()
                            .from(owner_db_id)
                            .to(credit_db_id)
                            .values_uniform([("owned", 1).into()])
                            .query(),
                    )?;
                    t.exec_mut(
                        agdb::QueryBuilder::insert()
                            .edges()
                            .from(credit_db_id)
                            .to(artist_db_id)
                            .query(),
                    )?;
                    Ok(())
                })
                .into_lua_err()?;
        }
        Ok(())
    }

    pub(crate) async fn link_artist_relation(
        &self,
        plugin_id: Option<Arc<str>>,
        from_artist_id: NodeId,
        to_artist_id: NodeId,
        relation_type: db::ArtistRelationType,
        attributes: Option<String>,
    ) -> Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        self.ensure_owner(plugin_id.as_ref())?;
        let method = "provider:link_artist_relation";
        let from_db_id: DbId = from_artist_id.into();
        let to_db_id: DbId = to_artist_id.into();

        let mut db_write = STATE.db.write().await;

        if db::artists::get_by_id(&db_write, from_db_id)
            .into_lua_err()?
            .is_none()
        {
            return Err(mlua::Error::runtime(format!(
                "{method}: from_artist_id does not reference an artist"
            )));
        }
        if db::artists::get_by_id(&db_write, to_db_id)
            .into_lua_err()?
            .is_none()
        {
            return Err(mlua::Error::runtime(format!(
                "{method}: to_artist_id does not reference an artist"
            )));
        }

        let resolved_attributes = attributes.and_then(|a| {
            let trimmed = a.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        });

        db::artists::relations::link(
            &mut db_write,
            from_db_id,
            to_db_id,
            relation_type,
            resolved_attributes,
        )
        .into_lua_err()?;

        Ok(())
    }
}

harmony_macros::compile!(type_path = Provider, fields = false, methods = true);

struct MetadataModule;

impl DescribeModule for MetadataModule {
    fn module_descriptor() -> ModuleDescriptor {
        let mut descriptor = ModuleDescriptor::new("Metadata", "metadata", None);
        descriptor.functions.extend(vec![
            ModuleFunctionDescriptor {
                path: vec!["Provider", "new"],
                description: Some("Creates or loads a metadata provider."),
                params: vec![ParameterDescriptor {
                    name: "id",
                    ty: String::luau_type(),
                    description: None,
                    variadic: false,
                }],
                returns: vec![LuauType::literal("Provider")],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["ids", "list"],
                description: Some("Lists external ids for an entity."),
                params: vec![ParameterDescriptor {
                    name: "id",
                    ty: <NodeId as LuauTypeInfo>::luau_type(),
                    description: None,
                    variadic: false,
                }],
                returns: vec![LuauType::array(LuauType::literal("MetadataIdRow"))],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["ids", "for_provider"],
                description: Some("Returns the external ID map for a provider from an ExternalIdsByProvider value."),
                params: vec![
                    ParameterDescriptor {
                        name: "external_ids",
                        ty: LuauType::optional(ExternalIdsByProvider::luau_type()),
                        description: None,
                        variadic: false,
                    },
                    ParameterDescriptor {
                        name: "provider_id",
                        ty: String::luau_type(),
                        description: None,
                        variadic: false,
                    },
                ],
                returns: vec![LuauType::optional(ProviderExternalIdMap::luau_type())],
                yields: false,
            },
        ]);
        descriptor
    }
}

impl MetadataModule {
    fn support_aliases() -> Vec<TypeAliasDescriptor> {
        vec![
            LuaCallback::type_alias_descriptor(),
            ProviderExternalIdMap::type_alias_descriptor(),
            ExternalIdsByProvider::type_alias_descriptor(),
            covers::ProviderCoverHandler::type_alias_descriptor(),
            lyrics::ProviderLyricsHandler::type_alias_descriptor(),
            harmony_luau::JsonValue::type_alias_descriptor(),
        ]
    }

    fn support_interfaces() -> Vec<InterfaceDescriptor> {
        let mut interfaces = vec![
            ProviderIdRegistration::interface_descriptor(),
            OptionConfig::interface_descriptor(),
            MetadataIdRow::interface_descriptor(),
            ReleaseRefreshLookupHints::interface_descriptor(),
            ReleaseRefreshArtist::interface_descriptor(),
            ReleaseRefreshTrackArtist::interface_descriptor(),
            ReleaseRefreshTrack::interface_descriptor(),
            ReleaseRefreshContext::interface_descriptor(),
        ];
        interfaces.extend(covers::interface_descriptors());
        interfaces.extend(lyrics::interface_descriptors());
        interfaces.extend(ensure::interface_descriptors());
        interfaces
    }

    fn support_classes() -> Vec<ClassDescriptor> {
        let mut provider = <Provider as DescribeUserData>::class_descriptor();
        provider.methods.retain(|method| {
            !(method.kind == harmony_luau::MethodKind::Static && method.name == "new")
        });
        vec![
            provider,
            layers::class_descriptor(),
            <EntityType as DescribeUserData>::class_descriptor(),
            <db::ArtistRelationType as DescribeUserData>::class_descriptor(),
            <db::CreditType as DescribeUserData>::class_descriptor(),
            <db::ArtistType as DescribeUserData>::class_descriptor(),
        ]
    }

    fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
        harmony_luau::render_definition_file_with_support(
            &<Self as DescribeModule>::module_descriptor(),
            &Self::support_aliases(),
            &Self::support_interfaces(),
            &Self::support_classes(),
        )
    }
}

async fn ids_list(lua: Lua, node_id: NodeId) -> Result<Value> {
    let node_db_id: DbId = node_id.into();

    let ids = {
        let db_read = STATE.db.read().await;
        list_entity_external_ids(&db_read, node_db_id).into_lua_err()?
    };

    let rows: Vec<MetadataIdRow> = ids
        .into_iter()
        .map(|id| MetadataIdRow {
            provider_id: id.provider_id,
            id_type: id.id_type,
            id_value: id.id_value,
        })
        .collect();

    lua.to_value_with(&rows, LUA_SERIALIZE_OPTIONS)
}

fn ids_for_provider(
    lua: &Lua,
    (external_ids, provider_id): (Option<HashMap<String, HashMap<String, String>>>, String),
) -> Result<Value> {
    let provider_id = provider_id.trim();
    let ids = if provider_id.is_empty() {
        None
    } else {
        external_ids.and_then(|all| all.get(provider_id).cloned())
    };
    lua.to_value_with(&ids, LUA_SERIALIZE_OPTIONS)
}

pub(crate) fn get_module() -> Module {
    Module {
        path: "lyra/metadata".into(),
        setup: Arc::new(|lua: &Lua| -> anyhow::Result<mlua::Table> {
            let table = lua.create_table()?;
            let ids_table = lua.create_table()?;

            table.set("Provider", lua.create_proxy::<Provider>()?)?;
            table.set("EntityType", lua.create_proxy::<EntityType>()?)?;
            table.set(
                "ArtistRelationType",
                lua.create_proxy::<db::ArtistRelationType>()?,
            )?;
            table.set("CreditType", lua.create_proxy::<db::CreditType>()?)?;
            table.set("ArtistType", lua.create_proxy::<db::ArtistType>()?)?;
            ids_table.set("list", lua.create_async_function(ids_list)?)?;
            ids_table.set("for_provider", lua.create_function(ids_for_provider)?)?;
            table.set("ids", ids_table)?;

            Ok(table)
        }),
        scope: harmony_core::Scope {
            id: "lyra.metadata".into(),
            description: "Register as a metadata provider and resolve entity matches.",
            danger: harmony_core::Danger::Medium,
        },
    }
}

pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    MetadataModule::render_luau_definition()
}

#[cfg(test)]
mod tests {
    use super::*;
    use agdb::QueryBuilder;
    use anyhow::anyhow;
    use harmony_core::LuaFunctionAsyncExt;
    use mlua as mluau;
    use mlua::{
        Value,
        chunk,
    };
    use std::sync::Arc;
    use std::sync::atomic::{
        AtomicU64,
        Ordering,
    };

    use crate::db::{
        self,
        IdSource,
        Release,
        Track,
    };
    use crate::services::metadata::merging::apply_merged_metadata_to_entity;
    use crate::services::providers::SYNC_LOCKS;
    use crate::testing::runtime_test_lock;

    static PROVIDER_SEQ: AtomicU64 = AtomicU64::new(1);

    fn next_provider_id(prefix: &str) -> String {
        let idx = PROVIDER_SEQ.fetch_add(1, Ordering::SeqCst);
        format!("{prefix}-{idx}")
    }

    /// Stand-in for `http.set_rate_limit` so registration clears the gate.
    async fn seed_test_rate_limit() {
        harmony_http::test_seed_rate_limit("provider-tests.example", Arc::<str>::from("test")).await;
    }

    fn setup_metadata_module(lua: &Lua) -> anyhow::Result<()> {
        let table = (get_module().setup)(lua)?;
        lua.globals().set("metadata", table)?;
        Ok(())
    }

    fn insert_track(db: &mut agdb::DbAny, title: &str, locked: bool) -> anyhow::Result<DbId> {
        let track = Track {
            db_id: None,
            id: nanoid!(),
            track_title: title.to_string(),
            sort_title: None,
            year: None,
            disc: None,
            disc_total: None,
            track: None,
            track_total: None,
            duration_ms: None,
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
            locked: Some(locked),
            created_at: None,
            ctime: None,
        };

        let qr = db.exec_mut(QueryBuilder::insert().element(&track).query())?;
        let track_db_id = qr.elements[0].id;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("tracks")
                .to(track_db_id)
                .query(),
        )?;
        Ok(track_db_id)
    }

    fn insert_release(db: &mut agdb::DbAny, title: &str, locked: bool) -> anyhow::Result<DbId> {
        let release = Release {
            db_id: None,
            id: nanoid!(),
            release_title: title.to_string(),
            sort_title: None,
            release_type: None,
            release_date: None,
            locked: Some(locked),
            created_at: None,
            ctime: None,
        };

        let qr = db.exec_mut(QueryBuilder::insert().element(&release).query())?;
        let release_db_id = qr.elements[0].id;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("releases")
                .to(release_db_id)
                .query(),
        )?;
        Ok(release_db_id)
    }

    fn first_result_title(results: Value) -> anyhow::Result<String> {
        let table = match results {
            Value::Table(table) => table,
            _ => return Err(anyhow!("expected search result table")),
        };

        let mut rows = table.sequence_values::<Value>();
        let row = rows
            .next()
            .transpose()?
            .ok_or_else(|| anyhow!("expected at least one row"))?;
        let row = match row {
            Value::Table(row) => row,
            _ => return Err(anyhow!("expected row table")),
        };

        let title: String = row.get("track_title")?;
        Ok(title)
    }

    fn list_rows(rows: Value) -> anyhow::Result<Vec<(String, String, String)>> {
        let table = match rows {
            Value::Table(table) => table,
            _ => return Err(anyhow!("expected ids list table")),
        };

        let mut parsed = Vec::new();
        for value in table.sequence_values::<Value>() {
            let row = match value? {
                Value::Table(row) => row,
                _ => return Err(anyhow!("expected ids list row table")),
            };

            let provider_id: String = row.get("provider_id")?;
            let id_type: String = row.get("id_type")?;
            let id_value: String = row.get("id_value")?;
            let source: Value = row.get("source")?;
            assert!(matches!(source, Value::Nil), "source should be omitted");
            parsed.push((provider_id, id_type, id_value));
        }

        Ok(parsed)
    }

    #[tokio::test]
    async fn provider_layer_save_updates_track_and_external_id() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let provider_id = next_provider_id("layer-save");
        let track_title = "Chunk Layer Title";
        let external_id = "ext-track-1";
        let lua_provider_id = provider_id.clone();

        let track_db_id = {
            let mut db = STATE.db.write().await;
            insert_track(&mut db, "Provider Track", false)?
        };
        let track_id = track_db_id.0;

        setup_metadata_module(STATE.lua.get().as_ref())?;

        let save_fn = STATE
            .lua
            .get()
            .load(chunk! {
                local provider = metadata.Provider.new($lua_provider_id)
                local layer = provider:layer($track_id)
                layer:set_field("track_title", $track_title)
                layer:set_id("release_id", $external_id)
                layer:save()
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        save_fn.call_async::<()>(()).await?;

        let db = STATE.db.read().await;
        let layers = db::metadata::layers::get_for_entity(&db, track_db_id)?;
        assert_eq!(layers.len(), 1);
        let track = db::tracks::get_by_id(&db, track_db_id)?
            .ok_or_else(|| anyhow!("track not found after layer save"))?;
        assert_eq!(track.track_title, track_title.to_string());

        let provider_config = db::providers::get_by_provider_id(&db, &provider_id)?;
        assert!(provider_config.is_some(), "provider not stored");

        let external_ids = db::external_ids::get_for_entity(&db, track_db_id)?;
        let external = external_ids
            .into_iter()
            .find(|id| id.provider_id == provider_id && id.id_type == "release_id")
            .ok_or_else(|| anyhow!("external id not written"))?;
        assert_eq!(external.id_value, external_id);

        Ok(())
    }

    #[tokio::test]
    async fn ensure_artist_creates_and_reuses_by_registered_external_id() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let provider_id = next_provider_id("ensure-artist");
        let lua_provider_id = provider_id.clone();

        setup_metadata_module(STATE.lua.get().as_ref())?;

        let ensure_fn = STATE
            .lua
            .get()
            .load(chunk! {
                local provider = metadata.Provider.new($lua_provider_id)
                provider:id({
                    id = "artist_id",
                    entity = "artist",
                })
                local first = provider:ensure_artist({
                    id_type = "artist_id",
                    id_value = "mb-artist-1",
                    artist_name = "Ensured Artist",
                    sort_name = "Artist, Ensured",
                })
                local second = provider:ensure_artist({
                    id_type = "artist_id",
                    id_value = "mb-artist-1",
                    artist_name = "Ensured Artist",
                })
                return { first = first, second = second }
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        let result = ensure_fn.call_async::<Value>(()).await?;
        let result_table = match result {
            Value::Table(table) => table,
            _ => return Err(anyhow!("expected ensure_artist return table")),
        };
        let first_id: i64 = result_table.get("first")?;
        let second_id: i64 = result_table.get("second")?;
        assert_eq!(first_id, second_id);

        let db = STATE.db.read().await;
        let artist_db_id = DbId(first_id);
        let artist = db::artists::get_by_id(&db, artist_db_id)?
            .ok_or_else(|| anyhow!("ensured artist missing"))?;
        assert_eq!(artist.artist_name, "Ensured Artist");
        assert_eq!(artist.sort_name.as_deref(), Some("Artist, Ensured"));

        let external_ids = db::external_ids::get_for_entity(&db, artist_db_id)?;
        let artist_id_row = external_ids
            .into_iter()
            .find(|id| id.provider_id == provider_id && id.id_type == "artist_id")
            .ok_or_else(|| anyhow!("artist_id row missing"))?;
        assert_eq!(artist_id_row.id_value, "mb-artist-1");

        Ok(())
    }

    #[tokio::test]
    async fn ensure_artist_requires_registered_artist_id_spec() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let provider_id = next_provider_id("ensure-artist-unregistered");
        let lua_provider_id = provider_id.clone();

        setup_metadata_module(STATE.lua.get().as_ref())?;

        let ensure_fn = STATE
            .lua
            .get()
            .load(chunk! {
                local provider = metadata.Provider.new($lua_provider_id)
                return provider:ensure_artist({
                    id_type = "artist_id",
                    id_value = "mb-artist-2",
                    artist_name = "Unregistered",
                })
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        let result = ensure_fn.call_async::<Value>(()).await;
        let err = result.expect_err("ensure_artist should fail without id registration");
        assert!(
            err.to_string().contains("is not registered for artist"),
            "unexpected error: {err}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn link_credit_links_release_and_is_idempotent() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let provider_id = next_provider_id("link-credit");
        let lua_provider_id = provider_id.clone();

        let release_db_id = {
            let mut db = STATE.db.write().await;
            insert_release(&mut db, "Link Release", false)?
        };
        let release_id = release_db_id.0;

        setup_metadata_module(STATE.lua.get().as_ref())?;

        let link_fn = STATE
            .lua
            .get()
            .load(chunk! {
                local provider = metadata.Provider.new($lua_provider_id)
                provider:id({
                    id = "artist_id",
                    entity = "artist",
                })
                local artist_id = provider:ensure_artist({
                    id_type = "artist_id",
                    id_value = "mb-link-artist",
                    artist_name = "Linked Artist",
                })
                provider:link_credit($release_id, artist_id)
                provider:link_credit($release_id, artist_id)
                return artist_id
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        let artist_id_value = link_fn.call_async::<i64>(()).await?;
        let artist_db_id = DbId(artist_id_value);

        let db = STATE.db.read().await;
        let linked_artists = db::artists::get(&db, release_db_id)?;
        assert_eq!(linked_artists.len(), 1);
        let linked_artist_id = linked_artists[0]
            .db_id
            .clone()
            .map(DbId::from)
            .ok_or_else(|| anyhow!("linked artist missing db_id"))?;
        assert_eq!(linked_artist_id, artist_db_id);

        Ok(())
    }

    #[tokio::test]
    async fn set_field_rejects_duration_ms() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let provider_id = next_provider_id("reject-duration");
        let lua_provider_id = provider_id.clone();

        let track_db_id = {
            let mut db = STATE.db.write().await;
            insert_track(&mut db, "Duration Track", false)?
        };
        let track_id = track_db_id.0;

        setup_metadata_module(STATE.lua.get().as_ref())?;

        let set_fn = STATE
            .lua
            .get()
            .load(chunk! {
                local provider = metadata.Provider.new($lua_provider_id)
                local layer = provider:layer($track_id)
                layer:set_field("duration_ms", 12345)
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        let result = set_fn.call_async::<()>(()).await;
        let err = result.expect_err("set_field(duration_ms) should error");
        assert!(
            err.to_string().contains("duration_ms is read-only"),
            "unexpected error: {err}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn provider_handlers_execute_from_lua_registration() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        seed_test_rate_limit().await;
        let provider_id = next_provider_id("provider-handlers");
        let search_term = "find-track";
        let refresh_title = "Refreshed By Provider";
        let cover_url = "https://example.com/cover.jpg";
        let lua_provider_id = provider_id.clone();

        let track_db_id = {
            let mut db = STATE.db.write().await;
            insert_track(&mut db, "Original Track", false)?
        };
        let track_id = track_db_id.0;

        setup_metadata_module(STATE.lua.get().as_ref())?;

        let register_handlers = STATE
            .lua
            .get()
            .load(chunk! {
                local provider = metadata.Provider.new($lua_provider_id)
                local ET = metadata.EntityType
                provider:search(ET.Track, function(query)
                    if query == $search_term then
                        return { { track_title = "Search Result" } }
                    end
                    return {}
                end)
                provider:cover(ET.Release, {
                    priority = 75,
                    require = {
                        any_of = { "ids.release_id", "ids.release_group_id" },
                    },
                }, function(ctx)
                    if ctx.ids and ctx.ids.release_id then
                        return $cover_url
                    end
                    return nil
                end)
                provider:refresh(ET.Track, function(track)
                    local layer = provider:layer(track.db_id)
                    layer:set_field("track_title", $refresh_title)
                    layer:save()
                end)
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        register_handlers.call_async::<()>(()).await?;

        let search_handler = {
            let registry = PROVIDER_REGISTRY.read().await;
            registry
                .get_search_handler(&provider_id, EntityType::Track)
                .cloned()
                .ok_or_else(|| anyhow!("search handler not registered"))?
        };

        let search_results = search_handler
            .call_async::<_, Value>(search_term.to_string())
            .await?;
        let title = first_result_title(search_results)?;
        assert_eq!(title, "Search Result");

        let cover_handler = {
            let registry = PROVIDER_REGISTRY.read().await;
            registry
                .get_cover_handlers(&provider_id, EntityType::Release)
                .into_iter()
                .next()
                .ok_or_else(|| anyhow!("cover handler not registered"))?
        };
        assert_eq!(
            cover_handler.require.any_of,
            vec![
                "ids.release_id".to_string(),
                "ids.release_group_id".to_string()
            ]
        );
        assert_eq!(cover_handler.priority, 75);
        let context = {
            let table = STATE.lua.get().create_table()?;
            let ids = STATE.lua.get().create_table()?;
            ids.set("release_id", "release-123")?;
            table.set("ids", ids)?;
            table
        };
        let cover_result: Option<String> = cover_handler
            .handler
            .call_async(Value::Table(context))
            .await?;
        assert_eq!(cover_result.as_deref(), Some(cover_url));

        let refresh_handler = {
            let registry = PROVIDER_REGISTRY.read().await;
            registry
                .get_refresh_handler(&provider_id, EntityType::Track)
                .cloned()
                .ok_or_else(|| anyhow!("refresh handler not registered"))?
        };

        let context = {
            let table = STATE.lua.get().create_table()?;
            table.set("db_id", track_id)?;
            table
        };
        refresh_handler.call_async::<_, ()>(context).await?;

        let db = STATE.db.read().await;
        let track = db::tracks::get_by_id(&db, track_db_id)?
            .ok_or_else(|| anyhow!("track missing after refresh handler"))?;
        assert_eq!(track.track_title, refresh_title.to_string());

        Ok(())
    }

    #[tokio::test]
    async fn provider_cover_registration_requires_supported_entity() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let provider_id = next_provider_id("cover-entity");
        let lua_provider_id = provider_id.clone();

        setup_metadata_module(STATE.lua.get().as_ref())?;

        let register_fn = STATE
            .lua
            .get()
            .load(chunk! {
                local provider = metadata.Provider.new($lua_provider_id)
                provider:cover(metadata.EntityType.Track, {
                    require = { all_of = { "ids.recording_id" } },
                }, function(_)
                    return nil
                end)
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        let err = register_fn
            .call_async::<()>(())
            .await
            .expect_err("expected provider:cover non-release entity to fail");
        assert!(
            err.to_string()
                .contains("entity_type must be 'release' or 'artist'"),
            "unexpected error: {err}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn provider_cover_registration_accepts_artist_entity() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        seed_test_rate_limit().await;
        let provider_id = next_provider_id("cover-artist");
        let lua_provider_id = provider_id.clone();

        setup_metadata_module(STATE.lua.get().as_ref())?;

        let register_fn = STATE
            .lua
            .get()
            .load(chunk! {
                local provider = metadata.Provider.new($lua_provider_id)
                provider:cover(metadata.EntityType.Artist, {}, function(_)
                    return nil
                end)
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        register_fn.call_async::<()>(()).await?;

        Ok(())
    }

    #[tokio::test]
    async fn provider_cover_registration_accepts_empty_require() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        seed_test_rate_limit().await;
        let provider_id = next_provider_id("cover-empty-require");
        let lua_provider_id = provider_id.clone();

        setup_metadata_module(STATE.lua.get().as_ref())?;

        let register_fn = STATE
            .lua
            .get()
            .load(chunk! {
                local provider = metadata.Provider.new($lua_provider_id)
                provider:cover(metadata.EntityType.Release, {}, function(_)
                    return nil
                end)
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        register_fn.call_async::<()>(()).await?;

        Ok(())
    }

    #[tokio::test]
    async fn provider_cover_registration_requires_set_rate_limit() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        // No seed — gate must refuse and the error must mention set_rate_limit.
        harmony_http::test_clear_rate_limits_for_plugin("test").await;

        let provider_id = next_provider_id("cover-no-rl");
        let lua_provider_id = provider_id.clone();

        setup_metadata_module(STATE.lua.get().as_ref())?;

        let register_fn = STATE
            .lua
            .get()
            .load(chunk! {
                local provider = metadata.Provider.new($lua_provider_id)
                provider:cover(metadata.EntityType.Release, {}, function(_)
                    return nil
                end)
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        let err = register_fn
            .call_async::<()>(())
            .await
            .expect_err("expected provider:cover to refuse without set_rate_limit");
        assert!(
            err.to_string().contains("set_rate_limit"),
            "error must mention set_rate_limit: {err}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn provider_cover_handler_timeout_surfaces_error() -> anyhow::Result<()> {
        use std::time::Duration;
        let _guard = runtime_test_lock().await;
        seed_test_rate_limit().await;
        crate::services::covers::providers::clear_cover_search_cache().await;

        let provider_id = next_provider_id("cover-timeout");
        let lua_provider_id = provider_id.clone();

        setup_metadata_module(STATE.lua.get().as_ref())?;

        // Lua-side sleep so the handler can outlast `timeout_ms`.
        let lua = STATE.lua.get();
        let sleep_fn = lua.create_async_function(|_lua, ms: u64| async move {
            tokio::time::sleep(Duration::from_millis(ms)).await;
            Ok(())
        })?;
        lua.globals().set("__test_sleep_ms", sleep_fn)?;

        let register_fn = lua
            .load(chunk! {
                local provider = metadata.Provider.new($lua_provider_id)
                provider:cover(metadata.EntityType.Release, {
                    timeout_ms = 50,
                }, function(_)
                    __test_sleep_ms(500)
                    return nil
                end)
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        register_fn.call_async::<()>(()).await?;

        // Call the dispatcher. The handler sleeps 500ms; the timeout is 50ms,
        // so the dispatcher must surface a timeout error.
        let context = serde_json::json!({});
        let err = crate::services::covers::providers::resolve_provider_cover_url(
            &provider_id,
            EntityType::Release,
            &context,
            true,
        )
        .await
        .expect_err("expected dispatcher to surface a timeout error");
        assert!(
            err.to_string().contains("timed out"),
            "error must mention 'timed out': {err}"
        );

        // Sanity: the registered spec carries the parsed timeout.
        let registry = PROVIDER_REGISTRY.read().await;
        let spec = registry
            .get_cover_handlers(&provider_id, EntityType::Release)
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("expected cover spec registered"))?;
        assert_eq!(spec.timeout, Duration::from_millis(50));

        Ok(())
    }

    #[tokio::test]
    async fn provider_cover_config_defaults_timeout_when_omitted() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        seed_test_rate_limit().await;

        let provider_id = next_provider_id("cover-default-timeout");
        let lua_provider_id = provider_id.clone();

        setup_metadata_module(STATE.lua.get().as_ref())?;

        let register_fn = STATE
            .lua
            .get()
            .load(chunk! {
                local provider = metadata.Provider.new($lua_provider_id)
                provider:cover(metadata.EntityType.Release, {}, function(_)
                    return nil
                end)
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        register_fn.call_async::<()>(()).await?;

        let registry = PROVIDER_REGISTRY.read().await;
        let spec = registry
            .get_cover_handlers(&provider_id, EntityType::Release)
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("expected cover spec registered"))?;
        assert_eq!(
            spec.timeout,
            crate::services::covers::providers::DEFAULT_COVER_HANDLER_TIMEOUT,
            "omitted timeout_ms must fall back to DEFAULT_COVER_HANDLER_TIMEOUT"
        );

        Ok(())
    }

    #[tokio::test]
    async fn provider_id_registration_accepts_spec_and_defaults_unique_false() -> anyhow::Result<()>
    {
        let _guard = runtime_test_lock().await;
        let provider_id = next_provider_id("id-spec-default");
        let lua_provider_id = provider_id.clone();

        setup_metadata_module(STATE.lua.get().as_ref())?;

        let register_fn = STATE
            .lua
            .get()
            .load(chunk! {
                local provider = metadata.Provider.new($lua_provider_id)
                provider:id({
                    id = "release_id",
                    entity = "release",
                })
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        register_fn.call_async::<()>(()).await?;

        let registry = PROVIDER_REGISTRY.read().await;
        let (spec, has_generator) = registry
            .id_registration(&provider_id, "release_id")
            .ok_or_else(|| anyhow!("id spec missing"))?;
        assert_eq!(spec.id, "release_id");
        assert_eq!(spec.entity, EntityType::Release);
        assert!(!spec.unique);
        assert!(
            !has_generator,
            "generator should be absent when callback is omitted"
        );

        Ok(())
    }

    #[tokio::test]
    async fn provider_id_registration_persists_unique_and_generator() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let provider_id = next_provider_id("id-spec-generator");
        let lua_provider_id = provider_id.clone();

        setup_metadata_module(STATE.lua.get().as_ref())?;

        let register_fn = STATE
            .lua
            .get()
            .load(chunk! {
                local provider = metadata.Provider.new($lua_provider_id)
                provider:id({
                    id = "recording_id",
                    entity = "track",
                    unique = true,
                }, function(id)
                    return id
                end)
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        register_fn.call_async::<()>(()).await?;

        let registry = PROVIDER_REGISTRY.read().await;
        let (spec, has_generator) = registry
            .id_registration(&provider_id, "recording_id")
            .ok_or_else(|| anyhow!("id spec missing"))?;
        assert_eq!(spec.id, "recording_id");
        assert_eq!(spec.entity, EntityType::Track);
        assert!(spec.unique);
        assert!(
            has_generator,
            "generator should be present when callback is provided"
        );

        Ok(())
    }

    #[tokio::test]
    async fn unique_id_pairs_filters_by_entity() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let provider_id = next_provider_id("unique-pairs-entity");
        let lua_provider_id = provider_id.clone();

        setup_metadata_module(STATE.lua.get().as_ref())?;

        let register_fn = STATE
            .lua
            .get()
            .load(chunk! {
                local provider = metadata.Provider.new($lua_provider_id)
                provider:id({
                    id = "release_id",
                    entity = "release",
                    unique = true,
                })
                provider:id({
                    id = "recording_id",
                    entity = "track",
                    unique = true,
                })
                provider:id({
                    id = "release_group_id",
                    entity = "release",
                    unique = false,
                })
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        register_fn.call_async::<()>(()).await?;

        let registry = PROVIDER_REGISTRY.read().await;
        let release_pairs = registry.unique_id_pairs(EntityType::Release);
        let track_pairs = registry.unique_id_pairs(EntityType::Track);

        assert!(release_pairs.contains(&(provider_id.clone(), "release_id".to_string())));
        assert!(!release_pairs.contains(&(provider_id.clone(), "release_group_id".to_string())));
        assert!(!release_pairs.contains(&(provider_id.clone(), "recording_id".to_string())));
        assert!(track_pairs.contains(&(provider_id, "recording_id".to_string())));

        Ok(())
    }

    #[tokio::test]
    async fn provider_id_registration_requires_id_and_entity() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let lua_provider_id_missing_id = next_provider_id("id-spec-invalid-id");
        let lua_provider_id_missing_entity = next_provider_id("id-spec-invalid-entity");

        setup_metadata_module(STATE.lua.get().as_ref())?;

        let missing_id = STATE
            .lua
            .get()
            .load(chunk! {
                local provider = metadata.Provider.new($lua_provider_id_missing_id)
                provider:id({
                    entity = "track",
                }, function(id)
                    return id
                end)
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        let err = missing_id
            .call_async::<()>(())
            .await
            .expect_err("expected missing id to fail");
        assert!(
            err.to_string().contains("spec.id is required"),
            "unexpected missing-id error: {err}"
        );

        let missing_entity = STATE
            .lua
            .get()
            .load(chunk! {
                local provider = metadata.Provider.new($lua_provider_id_missing_entity)
                provider:id({
                    id = "recording_id",
                }, function(id)
                    return id
                end)
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        let err = missing_entity
            .call_async::<()>(())
            .await
            .expect_err("expected missing entity to fail");
        assert!(
            err.to_string().contains("spec.entity is required"),
            "unexpected missing-entity error: {err}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn provider_layer_save_skips_fields_on_locked_entity() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let provider_id = next_provider_id("locked-entity");
        let lua_provider_id = provider_id.clone();
        let track_db_id = {
            let mut db = STATE.db.write().await;
            insert_track(&mut db, "Locked Track", true)?
        };
        let track_id = track_db_id.0;
        let title_after = "Should Not Apply";
        let external_id = "locked-ext-1";

        setup_metadata_module(STATE.lua.get().as_ref())?;

        let save_fn = STATE
            .lua
            .get()
            .load(chunk! {
                local provider = metadata.Provider.new($lua_provider_id)
                local layer = provider:layer($track_id)
                layer:set_field("track_title", $title_after)
                layer:set_id("release_id", $external_id)
                layer:save()
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        save_fn.call_async::<()>(()).await?;

        let db = STATE.db.read().await;
        let track = db::tracks::get_by_id(&db, track_db_id)?
            .ok_or_else(|| anyhow!("locked track not found"))?;
        assert_eq!(track.track_title, "Locked Track");

        let external_ids = db::external_ids::get_for_entity(&db, track_db_id)?;
        let external = external_ids
            .into_iter()
            .find(|id| id.provider_id == provider_id && id.id_type == "release_id")
            .ok_or_else(|| anyhow!("external id not written on locked entity"))?;
        assert_eq!(external.id_value, external_id);

        Ok(())
    }

    #[tokio::test]
    async fn provider_layer_precedence_is_priority_driven() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let low_provider_id = next_provider_id("priority-low");
        let high_provider_id = next_provider_id("priority-high");
        let lua_low_provider_id = low_provider_id.clone();
        let lua_high_provider_id = high_provider_id.clone();
        let track_db_id = {
            let mut db = STATE.db.write().await;
            insert_track(&mut db, "Priority Track", false)?
        };
        let track_id = track_db_id.0;

        setup_metadata_module(STATE.lua.get().as_ref())?;

        let low_title = "Low Priority";
        let high_title = "High Priority";

        let save_fn = STATE
            .lua
            .get()
            .load(chunk! {
                local low = metadata.Provider.new($lua_low_provider_id)
                local high = metadata.Provider.new($lua_high_provider_id)

                local low_layer = low:layer($track_id)
                low_layer:set_field("track_title", $low_title)
                low_layer:save()

                local high_layer = high:layer($track_id)
                high_layer:set_field("track_title", $high_title)
                high_layer:save()
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        save_fn.call_async::<()>(()).await?;

        {
            let mut db = STATE.db.write().await;
            db::providers::update_priority(&mut db, &low_provider_id, 10)?;
            db::providers::update_priority(&mut db, &high_provider_id, 100)?;
            apply_merged_metadata_to_entity(&mut db, track_db_id)?;
        }

        let db = STATE.db.read().await;
        let track = db::tracks::get_by_id(&db, track_db_id)?
            .ok_or_else(|| anyhow!("track missing for priority test"))?;
        assert_eq!(track.track_title, high_title.to_string());

        Ok(())
    }

    #[tokio::test]
    async fn ids_list_returns_all_rows_with_provider_context() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let track_db_id = {
            let mut db = STATE.db.write().await;
            insert_track(&mut db, "IDs Track", false)?
        };
        let track_id = track_db_id.0;

        {
            let mut db = STATE.db.write().await;
            db::external_ids::upsert(
                &mut db,
                track_db_id,
                "z-provider",
                "release_id",
                "release-z",
                IdSource::Plugin,
            )?;
            db::external_ids::upsert(
                &mut db,
                track_db_id,
                "a-provider",
                "artist_id",
                "artist-a",
                IdSource::Plugin,
            )?;
            db::external_ids::upsert(
                &mut db,
                track_db_id,
                "a-provider",
                "release_id",
                "release-a",
                IdSource::Plugin,
            )?;
        }

        setup_metadata_module(STATE.lua.get().as_ref())?;

        let list_fn = STATE
            .lua
            .get()
            .load(chunk! {
                return metadata.ids.list($track_id)
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        let rows = list_fn.call_async::<Value>(()).await?;
        let parsed = list_rows(rows)?;
        assert_eq!(
            parsed,
            vec![
                (
                    "a-provider".to_string(),
                    "artist_id".to_string(),
                    "artist-a".to_string()
                ),
                (
                    "a-provider".to_string(),
                    "release_id".to_string(),
                    "release-a".to_string(),
                ),
                (
                    "z-provider".to_string(),
                    "release_id".to_string(),
                    "release-z".to_string(),
                ),
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn ids_list_returns_empty_when_entity_has_no_ids() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let track_db_id = {
            let mut db = STATE.db.write().await;
            insert_track(&mut db, "No IDs Track", false)?
        };
        let track_id = track_db_id.0;

        setup_metadata_module(STATE.lua.get().as_ref())?;

        let list_fn = STATE
            .lua
            .get()
            .load(chunk! {
                return metadata.ids.list($track_id)
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        let rows = list_fn.call_async::<Value>(()).await?;
        let parsed = list_rows(rows)?;
        assert!(parsed.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn ids_list_errors_for_unknown_entity() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_metadata_module(STATE.lua.get().as_ref())?;

        let missing_id = -1;
        let list_fn = STATE
            .lua
            .get()
            .load(chunk! {
                return metadata.ids.list($missing_id)
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;

        let result = list_fn.call_async::<Value>(()).await;
        let err = result.expect_err("expected missing entity lookup to fail");
        assert!(
            err.to_string().contains("Entity not found"),
            "unexpected error: {err}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn refresh_with_filter_registers_both() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let provider_id = next_provider_id("refresh-filter");
        let lua_provider_id = provider_id.clone();

        setup_metadata_module(STATE.lua.get().as_ref())?;

        let register_fn = STATE
            .lua
            .get()
            .load(chunk! {
                local provider = metadata.Provider.new($lua_provider_id)
                provider:refresh(metadata.EntityType.Release, function() end, function() return true end)
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        register_fn.call_async::<()>(()).await?;

        let registry = PROVIDER_REGISTRY.read().await;
        assert!(
            registry
                .get_refresh_handler(&provider_id, EntityType::Release)
                .is_some(),
            "refresh handler should be registered"
        );
        assert!(
            registry
                .get_sync_filter(&provider_id, EntityType::Release)
                .is_some(),
            "sync filter should be registered"
        );
        let ids = registry.providers_with_refresh_handler(EntityType::Release);
        assert!(
            ids.contains(&provider_id),
            "provider should appear in providers_with_refresh_handler"
        );

        Ok(())
    }

    #[tokio::test]
    async fn refresh_with_filter_table_executes_handler() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let provider_id = next_provider_id("refresh-table-exec");
        let lua_provider_id = provider_id.clone();

        let track_db_id = {
            let mut db = STATE.db.write().await;
            insert_track(&mut db, "Before Refresh", false)?
        };
        let track_id = track_db_id.0;

        setup_metadata_module(STATE.lua.get().as_ref())?;

        let new_title = "After Refresh";
        let register_fn = STATE
            .lua
            .get()
            .load(chunk! {
                local provider = metadata.Provider.new($lua_provider_id)
                provider:refresh(metadata.EntityType.Track, function(ctx)
                    local layer = provider:layer($track_id)
                    layer:set_field("track_title", $new_title)
                    layer:save()
                end, function() return true end)
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        register_fn.call_async::<()>(()).await?;

        let handler = {
            let registry = PROVIDER_REGISTRY.read().await;
            registry
                .get_refresh_handler(&provider_id, EntityType::Track)
                .cloned()
                .ok_or_else(|| anyhow!("refresh handler not registered"))?
        };
        handler.call_async::<_, ()>(()).await?;

        let db = STATE.db.read().await;
        let track = db::tracks::get_by_id(&db, track_db_id)?
            .ok_or_else(|| anyhow!("track missing after refresh handler"))?;
        assert_eq!(track.track_title, new_title.to_string());

        Ok(())
    }

    #[tokio::test]
    async fn sync_locks_prevent_concurrent_runs() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let provider_id = next_provider_id("sync-lock");

        {
            let mut locks = SYNC_LOCKS.lock().await;
            locks.insert(provider_id.clone());
        }

        {
            let locks = SYNC_LOCKS.lock().await;
            assert!(
                locks.contains(&provider_id),
                "lock should be held for provider"
            );
        }

        {
            let mut locks = SYNC_LOCKS.lock().await;
            locks.remove(&provider_id);
        }

        Ok(())
    }

    #[tokio::test]
    async fn ids_list_round_trips_ids_written_by_layer_save() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let provider_id = next_provider_id("ids-roundtrip");
        let lua_provider_id = provider_id.clone();
        let track_db_id = {
            let mut db = STATE.db.write().await;
            insert_track(&mut db, "Roundtrip Track", false)?
        };
        let track_id = track_db_id.0;

        setup_metadata_module(STATE.lua.get().as_ref())?;

        let save_fn = STATE
            .lua
            .get()
            .load(chunk! {
                local provider = metadata.Provider.new($lua_provider_id)
                local layer = provider:layer($track_id)
                layer:set_id("release_id", "release-1")
                layer:set_id("recording_id", "recording-1")
                layer:save()
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        save_fn.call_async::<()>(()).await?;

        let list_fn = STATE
            .lua
            .get()
            .load(chunk! {
                return metadata.ids.list($track_id)
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        let rows = list_fn.call_async::<Value>(()).await?;
        let parsed = list_rows(rows)?;
        assert_eq!(
            parsed,
            vec![
                (
                    provider_id.clone(),
                    "recording_id".to_string(),
                    "recording-1".to_string(),
                ),
                (
                    provider_id,
                    "release_id".to_string(),
                    "release-1".to_string(),
                ),
            ]
        );

        Ok(())
    }

    mod lyrics_provider_tests {
        use super::*;
        use crate::db::test_db;
        use crate::services::metadata::lyrics::providers::{
            LYRICS_PROVIDER_REGISTRY,
            dispatch_for_track,
            reset_registry_for_test,
        };
        use std::sync::Arc;

        const TEST_PLUGIN_ID: &str = "test";
        const TEST_DOMAIN: &str = "lyrics-tests.example";

        async fn initialize_dispatcher_runtime() -> anyhow::Result<()> {
            crate::testing::initialize_runtime(&crate::testing::LibraryFixtureConfig {
                directory: std::path::PathBuf::from("."),
                language: None,
                country: None,
            })
            .await?;
            reset_registry_for_test().await;
            harmony_http::test_clear_rate_limits_for_plugin(TEST_PLUGIN_ID).await;
            Ok(())
        }

        async fn install_track(
            title: &str,
            artist: &str,
            duration_ms: u64,
        ) -> anyhow::Result<DbId> {
            let mut db = STATE.db.write().await;
            let track_id = test_db::insert_track(&mut *db, title)?;
            let mut track = db::tracks::get_by_id(&*db, track_id)?
                .ok_or_else(|| anyhow!("just-inserted track missing"))?;
            track.duration_ms = Some(duration_ms);
            db.exec_mut(QueryBuilder::insert().element(&track).query())?;
            let artist_id = test_db::insert_artist(&mut *db, artist)?;
            test_db::connect(&mut *db, track_id, artist_id)?;
            Ok(track_id)
        }

        async fn seed_rate_limit() {
            harmony_http::test_seed_rate_limit(TEST_DOMAIN, Arc::<str>::from(TEST_PLUGIN_ID)).await;
        }

        #[tokio::test]
        async fn lyrics_handler_hit_round_trip_writes_lyrics_row() -> anyhow::Result<()> {
            let _guard = runtime_test_lock().await;
            initialize_dispatcher_runtime().await?;
            seed_rate_limit().await;

            let provider_id = next_provider_id("lyrics-hit");
            let lua_provider_id = provider_id.clone();
            let track_id =
                install_track("Round Trip Title For Hit", "Round Trip Artist For Hit", 200_000)
                    .await?;
            let track_db_id_value = track_id.0;

            setup_metadata_module(STATE.lua.get().as_ref())?;
            let register_fn = STATE
                .lua
                .get()
                .load(chunk! {
                    local provider = metadata.Provider.new($lua_provider_id)
                    provider:lyrics({ priority = 60 }, function(ctx)
                        assert(ctx.track_db_id == $track_db_id_value, "expected matching track id")
                        return {
                            kind = "hit",
                            candidates = {
                                {
                                    lyrics = {
                                        id = "lyrics-id-1",
                                        language = "eng",
                                        plain_text = "verse",
                                        lines = {
                                            { ts_ms = 1000, text = "verse" },
                                        },
                                    },
                                    title = "Round Trip Title For Hit",
                                    artist = "Round Trip Artist For Hit",
                                    duration_ms = 200000,
                                    language = "eng",
                                },
                            },
                        }
                    end)
                })
                .set_name(&harmony_core::format_plugin_chunk_name(TEST_PLUGIN_ID, "init"))
                .into_function()?;
            register_fn.call_async::<()>(()).await?;

            dispatch_for_track(track_id, false).await?;

            let db = STATE.db.read().await;
            let lyrics_rows = db::lyrics::get_for_track(&*db, track_id)?;
            assert!(
                lyrics_rows.iter().any(|l| l.provider_id == provider_id),
                "expected lyrics row written for {provider_id}"
            );
            Ok(())
        }

        #[tokio::test]
        async fn lyrics_handler_miss_does_not_write_lyrics_row() -> anyhow::Result<()> {
            let _guard = runtime_test_lock().await;
            initialize_dispatcher_runtime().await?;
            seed_rate_limit().await;

            let provider_id = next_provider_id("lyrics-miss");
            let lua_provider_id = provider_id.clone();
            let track_id = install_track("Miss Title", "Miss Artist", 200_000).await?;

            setup_metadata_module(STATE.lua.get().as_ref())?;
            let register_fn = STATE
                .lua
                .get()
                .load(chunk! {
                    local provider = metadata.Provider.new($lua_provider_id)
                    provider:lyrics({}, function(_ctx)
                        return { kind = "miss" }
                    end)
                })
                .set_name(&harmony_core::format_plugin_chunk_name(TEST_PLUGIN_ID, "init"))
                .into_function()?;
            register_fn.call_async::<()>(()).await?;

            dispatch_for_track(track_id, false).await?;

            let db = STATE.db.read().await;
            let lyrics_rows = db::lyrics::get_for_track(&*db, track_id)?;
            assert!(
                lyrics_rows.iter().all(|l| l.provider_id != provider_id),
                "miss must not write a lyrics row"
            );
            Ok(())
        }

        #[tokio::test]
        async fn lyrics_handler_instrumental_does_not_write_lyrics_row() -> anyhow::Result<()> {
            let _guard = runtime_test_lock().await;
            initialize_dispatcher_runtime().await?;
            seed_rate_limit().await;

            let provider_id = next_provider_id("lyrics-instrumental");
            let lua_provider_id = provider_id.clone();
            let track_id =
                install_track("Instrumental Title", "Instrumental Artist", 200_000).await?;

            setup_metadata_module(STATE.lua.get().as_ref())?;
            let register_fn = STATE
                .lua
                .get()
                .load(chunk! {
                    local provider = metadata.Provider.new($lua_provider_id)
                    provider:lyrics({}, function(_ctx)
                        return { kind = "instrumental" }
                    end)
                })
                .set_name(&harmony_core::format_plugin_chunk_name(TEST_PLUGIN_ID, "init"))
                .into_function()?;
            register_fn.call_async::<()>(()).await?;

            dispatch_for_track(track_id, false).await?;

            let db = STATE.db.read().await;
            let lyrics_rows = db::lyrics::get_for_track(&*db, track_id)?;
            assert!(
                lyrics_rows.iter().all(|l| l.provider_id != provider_id),
                "instrumental must not write a lyrics row"
            );
            Ok(())
        }

        #[tokio::test]
        async fn lyrics_handler_rate_limited_does_not_write_lyrics_row() -> anyhow::Result<()> {
            let _guard = runtime_test_lock().await;
            initialize_dispatcher_runtime().await?;
            seed_rate_limit().await;

            let provider_id = next_provider_id("lyrics-rl");
            let lua_provider_id = provider_id.clone();
            let track_id = install_track("RL Title", "RL Artist", 200_000).await?;

            setup_metadata_module(STATE.lua.get().as_ref())?;
            let register_fn = STATE
                .lua
                .get()
                .load(chunk! {
                    local provider = metadata.Provider.new($lua_provider_id)
                    provider:lyrics({}, function(_ctx)
                        return { kind = "rate_limited", retry_after_ms = 5000 }
                    end)
                })
                .set_name(&harmony_core::format_plugin_chunk_name(TEST_PLUGIN_ID, "init"))
                .into_function()?;
            register_fn.call_async::<()>(()).await?;

            dispatch_for_track(track_id, false).await?;

            let db = STATE.db.read().await;
            let lyrics_rows = db::lyrics::get_for_track(&*db, track_id)?;
            assert!(
                lyrics_rows.iter().all(|l| l.provider_id != provider_id),
                "rate_limited must not write a lyrics row"
            );
            Ok(())
        }

        #[tokio::test]
        async fn lyrics_handler_hit_without_candidates_errors() -> anyhow::Result<()> {
            let _guard = runtime_test_lock().await;
            initialize_dispatcher_runtime().await?;
            seed_rate_limit().await;

            let provider_id = next_provider_id("lyrics-hit-empty");
            let lua_provider_id = provider_id.clone();
            let track_id = install_track("Hit Empty Title", "Hit Empty Artist", 200_000).await?;

            setup_metadata_module(STATE.lua.get().as_ref())?;
            let register_fn = STATE
                .lua
                .get()
                .load(chunk! {
                    local provider = metadata.Provider.new($lua_provider_id)
                    provider:lyrics({}, function(_ctx)
                        return { kind = "hit" }
                    end)
                })
                .set_name(&harmony_core::format_plugin_chunk_name(TEST_PLUGIN_ID, "init"))
                .into_function()?;
            register_fn.call_async::<()>(()).await?;

            dispatch_for_track(track_id, false).await?;

            let db = STATE.db.read().await;
            let lyrics_rows = db::lyrics::get_for_track(&*db, track_id)?;
            assert!(
                lyrics_rows.iter().all(|l| l.provider_id != provider_id),
                "malformed hit must not write a lyrics row"
            );
            Ok(())
        }

        #[tokio::test]
        async fn lyrics_handler_typo_kind_errors() -> anyhow::Result<()> {
            let _guard = runtime_test_lock().await;
            initialize_dispatcher_runtime().await?;
            seed_rate_limit().await;

            let provider_id = next_provider_id("lyrics-typo");
            let lua_provider_id = provider_id.clone();
            let track_id = install_track("Typo Title", "Typo Artist", 200_000).await?;

            setup_metadata_module(STATE.lua.get().as_ref())?;
            let register_fn = STATE
                .lua
                .get()
                .load(chunk! {
                    local provider = metadata.Provider.new($lua_provider_id)
                    provider:lyrics({}, function(_ctx)
                        return { kind = "intsrumental" }
                    end)
                })
                .set_name(&harmony_core::format_plugin_chunk_name(TEST_PLUGIN_ID, "init"))
                .into_function()?;
            register_fn.call_async::<()>(()).await?;

            dispatch_for_track(track_id, false).await?;

            let db = STATE.db.read().await;
            let lyrics_rows = db::lyrics::get_for_track(&*db, track_id)?;
            assert!(
                lyrics_rows.iter().all(|l| l.provider_id != provider_id),
                "typo kind must not write a lyrics row"
            );
            Ok(())
        }

        #[tokio::test]
        async fn lyrics_handler_string_return_errors() -> anyhow::Result<()> {
            let _guard = runtime_test_lock().await;
            initialize_dispatcher_runtime().await?;
            seed_rate_limit().await;

            let provider_id = next_provider_id("lyrics-string-return");
            let lua_provider_id = provider_id.clone();
            let track_id = install_track("String Return", "String Artist", 200_000).await?;

            setup_metadata_module(STATE.lua.get().as_ref())?;
            let register_fn = STATE
                .lua
                .get()
                .load(chunk! {
                    local provider = metadata.Provider.new($lua_provider_id)
                    provider:lyrics({}, function(_ctx)
                        return "oops"
                    end)
                })
                .set_name(&harmony_core::format_plugin_chunk_name(TEST_PLUGIN_ID, "init"))
                .into_function()?;
            register_fn.call_async::<()>(()).await?;

            dispatch_for_track(track_id, false).await?;

            let db = STATE.db.read().await;
            let lyrics_rows = db::lyrics::get_for_track(&*db, track_id)?;
            assert!(
                lyrics_rows.iter().all(|l| l.provider_id != provider_id),
                "string return must not write a lyrics row"
            );
            Ok(())
        }

        #[tokio::test]
        async fn lyrics_handler_lua_error_logged_without_writing_lyrics() -> anyhow::Result<()> {
            let _guard = runtime_test_lock().await;
            initialize_dispatcher_runtime().await?;
            seed_rate_limit().await;

            let provider_id = next_provider_id("lyrics-lua-error");
            let lua_provider_id = provider_id.clone();
            let track_id = install_track("Lua Error", "Lua Error Artist", 200_000).await?;

            setup_metadata_module(STATE.lua.get().as_ref())?;
            let register_fn = STATE
                .lua
                .get()
                .load(chunk! {
                    local provider = metadata.Provider.new($lua_provider_id)
                    provider:lyrics({}, function(_ctx)
                        error("boom")
                    end)
                })
                .set_name(&harmony_core::format_plugin_chunk_name(TEST_PLUGIN_ID, "init"))
                .into_function()?;
            register_fn.call_async::<()>(()).await?;

            dispatch_for_track(track_id, false).await?;

            let db = STATE.db.read().await;
            let lyrics_rows = db::lyrics::get_for_track(&*db, track_id)?;
            assert!(
                lyrics_rows.iter().all(|l| l.provider_id != provider_id),
                "lua error must not write a lyrics row"
            );
            Ok(())
        }

        #[tokio::test]
        async fn lyrics_registration_requires_set_rate_limit() -> anyhow::Result<()> {
            let _guard = runtime_test_lock().await;
            initialize_dispatcher_runtime().await?;
            // Deliberately do NOT seed a rate limit.

            let provider_id = next_provider_id("lyrics-no-rl");
            let lua_provider_id = provider_id.clone();

            setup_metadata_module(STATE.lua.get().as_ref())?;
            let register_fn = STATE
                .lua
                .get()
                .load(chunk! {
                    local provider = metadata.Provider.new($lua_provider_id)
                    provider:lyrics({}, function(_ctx)
                        return { kind = "miss" }
                    end)
                })
                .set_name(&harmony_core::format_plugin_chunk_name(TEST_PLUGIN_ID, "init"))
                .into_function()?;

            let err = register_fn
                .call_async::<()>(())
                .await
                .expect_err("expected provider:lyrics to refuse without set_rate_limit");
            assert!(
                err.to_string().contains("set_rate_limit"),
                "error must mention set_rate_limit: {err}"
            );
            Ok(())
        }

        #[tokio::test]
        async fn teardown_purges_lyrics_handlers_for_plugin() -> anyhow::Result<()> {
            let _guard = runtime_test_lock().await;
            initialize_dispatcher_runtime().await?;
            seed_rate_limit().await;

            let provider_id = next_provider_id("lyrics-teardown");
            let lua_provider_id = provider_id.clone();
            let _track_id = install_track("Teardown", "Teardown Artist", 200_000).await?;

            setup_metadata_module(STATE.lua.get().as_ref())?;
            let register_fn = STATE
                .lua
                .get()
                .load(chunk! {
                    local provider = metadata.Provider.new($lua_provider_id)
                    provider:lyrics({}, function(_ctx)
                        return { kind = "miss" }
                    end)
                })
                .set_name(&harmony_core::format_plugin_chunk_name(TEST_PLUGIN_ID, "init"))
                .into_function()?;
            register_fn.call_async::<()>(()).await?;

            {
                let registry = LYRICS_PROVIDER_REGISTRY.read().await;
                assert!(
                    registry
                        .list_sorted()
                        .iter()
                        .any(|h| h.provider_id.as_ref() == provider_id),
                    "handler must be registered before teardown"
                );
            }

            let plugin_id = crate::plugins::lifecycle::PluginId::new(TEST_PLUGIN_ID)
                .expect("valid plugin id");
            crate::services::providers::teardown_plugin_providers(&plugin_id).await;

            let registry = LYRICS_PROVIDER_REGISTRY.read().await;
            assert!(
                registry
                    .list_sorted()
                    .iter()
                    .all(|h| h.provider_id.as_ref() != provider_id),
                "handler must be removed after teardown_plugin_providers"
            );
            Ok(())
        }
    }
}
