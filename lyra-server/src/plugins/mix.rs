// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use agdb::DbId;
use harmony_core::ModuleSpec;
use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeInterface,
    DescribeModule,
    DescribeUserData,
    FieldDescriptor,
    TypeAliasDescriptor,
    render_definition_file_with_support,
};
use harmony_luau::{
    FunctionParameter,
    LuauType,
    LuauTypeInfo,
};

use crate::{
    plugins::OptionConfig,
    plugins::args::JsonArg,
    plugins::db::Db,
    services::auth::Principal,
    services::mix::{
        self as mix_service,
        MAX_LIMIT,
        MixOptions,
        MixSeedId,
        MixSeedType,
    },
    services::options::{
        OptionDeclaration,
        OptionType,
    },
    services::plugin_dispatch::PluginCallbackHandle,
};
use lyra_db::Track;

struct MixHandler;
struct MixRecentListensHandler;
pub(crate) struct MixModule;

pub(crate) struct MixCallbackRegistry {
    handlers: crate::plugins::callback_table::CallbackTable<MixCallback>,
}

impl MixCallbackRegistry {
    pub(crate) fn new(vm_id: u64) -> Self {
        Self {
            handlers: crate::plugins::callback_table::CallbackTable::new(vm_id),
        }
    }

    fn register(
        &self,
        mixer_id: String,
        _seed_type: MixSeedType,
        function: luau::Function,
        context: harmony_core::CallContext,
    ) -> PluginCallbackHandle {
        self.handlers.insert(MixCallback {
            mixer_id,
            function,
            context,
        })
    }

    pub(crate) fn get(&self, handle: PluginCallbackHandle) -> anyhow::Result<MixCallback> {
        self.handlers.get(handle)
    }

    pub(crate) fn remove_plugin(&self, plugin_id: &str) {
        self.handlers.remove_plugin(plugin_id);
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.handlers.len()
    }
}

#[derive(Clone)]
pub(crate) struct MixCallback {
    pub(crate) mixer_id: String,
    pub(crate) function: luau::Function,
    pub(crate) context: harmony_core::CallContext,
}

impl crate::plugins::callback_table::PluginCallback for MixCallback {
    fn context(&self) -> &harmony_core::CallContext {
        &self.context
    }
}

impl LuauTypeInfo for MixHandler {
    fn luau_type() -> LuauType {
        LuauType::function(
            vec![FunctionParameter {
                name: Some("ctx"),
                ty: LuauType::literal("MixContext"),
                variadic: false,
            }],
            vec![LuauType::literal("MixResult")],
        )
    }
}

impl LuauTypeInfo for MixRecentListensHandler {
    fn luau_type() -> LuauType {
        LuauType::function(
            vec![FunctionParameter {
                name: Some("ctx"),
                ty: LuauType::literal("MixRecentListensContext"),
                variadic: false,
            }],
            vec![LuauType::literal("MixResult")],
        )
    }
}

#[harmony_macros::userdata(name = "Mixer")]
#[derive(Clone)]
struct Mixer {
    id: String,
}

#[harmony_macros::userdata_methods]
#[allow(clippy::wrong_self_convention)]
impl Mixer {
    #[harmony(
        description = "Registers a handler for generating a mix from a seed track.",
        args(handler: MixHandler)
    )]
    fn from_track(
        &self,
        context: &luau::CallContext,
        vm: &luau::Vm,
        handler: luau::Function,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        self.register_handler(context, vm, MixSeedType::Track, handler)
    }

    #[harmony(
        description = "Registers a handler for generating a mix from a seed release.",
        args(handler: MixHandler)
    )]
    fn from_release(
        &self,
        context: &luau::CallContext,
        vm: &luau::Vm,
        handler: luau::Function,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        self.register_handler(context, vm, MixSeedType::Release, handler)
    }

    #[harmony(
        description = "Registers a handler for generating a mix from a seed artist.",
        args(handler: MixHandler)
    )]
    fn from_artist(
        &self,
        context: &luau::CallContext,
        vm: &luau::Vm,
        handler: luau::Function,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        self.register_handler(context, vm, MixSeedType::Artist, handler)
    }

    #[harmony(
        description = "Registers a handler for generating a mix from a user's recent listens.",
        args(handler: MixRecentListensHandler)
    )]
    fn from_recent_listens(
        &self,
        context: &luau::CallContext,
        vm: &luau::Vm,
        handler: luau::Function,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        self.register_handler(context, vm, MixSeedType::RecentListens, handler)
    }

    #[harmony(
        description = "Registers a handler for generating a mix from a seed genre.",
        args(handler: MixHandler)
    )]
    fn from_genre(
        &self,
        context: &luau::CallContext,
        vm: &luau::Vm,
        handler: luau::Function,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        self.register_handler(context, vm, MixSeedType::Genre, handler)
    }

    #[harmony(
        description = "Registers a handler for generating a mix from a seed playlist.",
        args(handler: MixHandler)
    )]
    fn from_playlist(
        &self,
        context: &luau::CallContext,
        vm: &luau::Vm,
        handler: luau::Function,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        self.register_handler(context, vm, MixSeedType::Playlist, handler)
    }

    #[harmony(
        description = "Declares an option clients can toggle when requesting a mix.",
        args(config: OptionConfig)
    )]
    fn declare_option(
        &self,
        context: &luau::CallContext,
        vm: &luau::Vm,
        config: luau::Table,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        let plugin_id = self.current_plugin_id(vm, context)?;
        let option = parse_option_declaration(vm, &config)?;
        let id = self.id.clone();
        Ok(luau::ScheduledFuture::new(async move {
            let generation = crate::plugins::STATE.generation();
            let _registration = generation
                .registries
                .ensure_registrations_open(&plugin_id)
                .await?;
            mix_service::mix_registry()
                .write_owned()
                .await
                .declare_option(&id, option)
                .map_err(crate::plugins::runtime_error)
        }))
    }

    #[harmony(skip)]
    fn register_handler(
        &self,
        context: &luau::CallContext,
        vm: &luau::Vm,
        seed_type: MixSeedType,
        function: luau::Function,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        let plugin_id = self.current_plugin_id(vm, context)?;
        let id = self.id.clone();
        let context = context.clone();
        let vm = vm.clone();
        Ok(luau::ScheduledFuture::new(async move {
            let generation = crate::plugins::STATE.generation();
            let _registration = generation
                .registries
                .ensure_registrations_open(&plugin_id)
                .await?;
            let mut registry = mix_service::mix_registry().write_owned().await;
            let handlers = vm.data().get::<MixCallbackRegistry>()?;
            let handler = handlers.register(id.clone(), seed_type, function, context);
            registry.set_seed_callback(&id, seed_type, handler);
            Ok(())
        }))
    }

    #[harmony(skip)]
    fn current_plugin_id(
        &self,
        vm: &luau::Vm,
        context: &luau::CallContext,
    ) -> luau::runtime::Result<crate::services::plugin_scope::PluginId> {
        let plugin_id = context.origin.plugin.clone().ok_or_else(|| {
            crate::plugins::runtime_error("mix.Mixer methods must be called from plugin Luau code")
        })?;
        harmony_core::ensure_current_plugin_run(vm, context)?;
        crate::services::plugin_scope::PluginId::new(plugin_id.to_string())
            .map_err(crate::plugins::runtime_error)
    }
}

/// What `Mixer.new` takes from its call: the calling plugin, which must be the current run, and
/// the handles its registration needs.
struct MixerHost {
    plugin_id: crate::services::plugin_scope::PluginId,
    vm: luau::Vm,
    origin: harmony_core::ChunkOrigin,
}

impl luau::FromCallFrame for MixerHost {
    fn from_call_frame(vm: &luau::Vm, context: &luau::CallContext) -> luau::runtime::Result<Self> {
        let Some(plugin_id) = context.origin.plugin.clone() else {
            return Err(crate::plugins::runtime_error(
                "mix.Mixer.new must be called from plugin Luau code",
            ));
        };
        let plugin_id = crate::services::plugin_scope::PluginId::new(plugin_id.to_string())?;
        harmony_core::ensure_current_plugin_run(vm, context)?;
        Ok(Self {
            plugin_id,
            vm: vm.clone(),
            origin: context.origin.clone(),
        })
    }
}

/// The documented type of the optional `opts` of the mix consumers.
#[cfg(feature = "docgen")]
struct MixConsumerOptions;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for MixConsumerOptions {
    fn luau_type() -> LuauType {
        LuauType::literal("MixConsumerOptions")
    }
}

/// The documented return of the mix consumers.
#[cfg(feature = "docgen")]
struct OptionalTrackList;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for OptionalTrackList {
    fn luau_type() -> LuauType {
        LuauType::Optional(Box::new(LuauType::literal("{ Track }")))
    }
}

#[harmony_macros::module(id = "lyra/mix", docs_feature = "docgen", extend = "extend_module")]
impl MixModule {
    #[harmony(
        name = "Mixer.new",
        description = "Creates a mixer registration object.",
        returns(Mixer)
    )]
    async fn mixer_new(
        id: String,
        #[context] host: MixerHost,
        #[context] db: Db,
    ) -> luau::runtime::Result<luau::UserData> {
        let MixerHost {
            plugin_id,
            vm,
            origin,
        } = host;
        let generation = crate::plugins::STATE.generation();
        let _registration = generation
            .registries
            .ensure_registrations_open(&plugin_id)
            .await?;
        mix_service::mix_registry()
            .write_owned()
            .await
            .register(plugin_id, id.clone())?;

        let mut db = db.write().await;
        lyra_db::mixers::register(&mut db, &id)?;

        drop(db);
        Mixer::_harmony_userdata_class().create(&vm, &origin, Mixer { id })
    }

    #[harmony(
        description = "Mix from a seed track. Nil if missing.",
        args(id: f64, opts: Option<MixConsumerOptions>),
        returns(OptionalTrackList)
    )]
    async fn from_track(
        seed_id: i64,
        opts: Option<JsonArg>,
        #[context] principal: Principal,
        #[context] db: Db,
    ) -> luau::runtime::Result<luau::Value> {
        consume("from_track", MixSeedId::Track, seed_id, opts, principal, db).await
    }

    #[harmony(
        description = "Mix from a seed release. Nil if missing.",
        args(id: f64, opts: Option<MixConsumerOptions>),
        returns(OptionalTrackList)
    )]
    async fn from_release(
        seed_id: i64,
        opts: Option<JsonArg>,
        #[context] principal: Principal,
        #[context] db: Db,
    ) -> luau::runtime::Result<luau::Value> {
        consume(
            "from_release",
            MixSeedId::Release,
            seed_id,
            opts,
            principal,
            db,
        )
        .await
    }

    #[harmony(
        description = "Mix from a seed artist. Nil if missing.",
        args(id: f64, opts: Option<MixConsumerOptions>),
        returns(OptionalTrackList)
    )]
    async fn from_artist(
        seed_id: i64,
        opts: Option<JsonArg>,
        #[context] principal: Principal,
        #[context] db: Db,
    ) -> luau::runtime::Result<luau::Value> {
        consume(
            "from_artist",
            MixSeedId::Artist,
            seed_id,
            opts,
            principal,
            db,
        )
        .await
    }

    #[harmony(
        description = "Mix from a seed genre. Nil if missing.",
        args(id: f64, opts: Option<MixConsumerOptions>),
        returns(OptionalTrackList)
    )]
    async fn from_genre(
        seed_id: i64,
        opts: Option<JsonArg>,
        #[context] principal: Principal,
        #[context] db: Db,
    ) -> luau::runtime::Result<luau::Value> {
        consume("from_genre", MixSeedId::Genre, seed_id, opts, principal, db).await
    }

    #[harmony(
        description = "Mix from a seed playlist. Nil if missing.",
        args(id: f64, opts: Option<MixConsumerOptions>),
        returns(OptionalTrackList)
    )]
    async fn from_playlist(
        seed_id: i64,
        opts: Option<JsonArg>,
        #[context] principal: Principal,
        #[context] db: Db,
    ) -> luau::runtime::Result<luau::Value> {
        consume(
            "from_playlist",
            MixSeedId::Playlist,
            seed_id,
            opts,
            principal,
            db,
        )
        .await
    }

    #[harmony(
        description = "Mix from a seed track with the seed pinned at index 1. Nil if missing.",
        args(id: f64, opts: Option<MixConsumerOptions>),
        returns(OptionalTrackList)
    )]
    async fn instant_mix_from_audio(
        seed_id: i64,
        opts: Option<JsonArg>,
        #[context] principal: Principal,
        #[context] db: Db,
    ) -> luau::runtime::Result<luau::Value> {
        let seed = parse_seed_id(seed_id, "instant_mix_from_audio")?;
        let options = parse_consumer_options(opts.map(|opts| opts.0), &principal)?;
        let Some(seed) = seed_public_id(db, seed).await? else {
            return tracks_to_luau(None);
        };
        let tracks = mix_service::instant_mix_from_audio(&seed, &options).await?;
        tracks_to_luau(tracks)
    }
}

fn extend_module(spec: ModuleSpec) -> ModuleSpec {
    spec.userdata(Mixer::_harmony_userdata_spec())
}

async fn consume(
    label: &'static str,
    variant: fn(String) -> MixSeedId,
    seed_id: i64,
    opts: Option<JsonArg>,
    principal: Principal,
    db: Db,
) -> luau::runtime::Result<luau::Value> {
    let seed = parse_seed_id(seed_id, label)?;
    let options = parse_consumer_options(opts.map(|opts| opts.0), &principal)?;
    let Some(seed) = seed_public_id(db, seed).await? else {
        return tracks_to_luau(None);
    };
    let tracks = mix_service::from_seed(variant(seed), &options).await?;
    tracks_to_luau(tracks)
}

fn parse_consumer_options(
    opts: Option<serde_json::Value>,
    principal: &Principal,
) -> luau::runtime::Result<MixOptions> {
    let json = opts.unwrap_or_else(|| serde_json::Value::Object(Default::default()));
    let json = match json {
        serde_json::Value::Array(values) if values.is_empty() => {
            serde_json::Value::Object(Default::default())
        }
        json => json,
    };
    let object = json
        .as_object()
        .ok_or_else(|| crate::plugins::runtime_error("mix options must be a table"))?;

    let limit = object
        .get("limit")
        .map(|value| parse_positive_usize(value, "limit"))
        .transpose()?;
    if let Some(limit) = limit
        && limit > MAX_LIMIT
    {
        return Err(crate::plugins::runtime_error(format!(
            "mix options 'limit' must be <= {MAX_LIMIT}, got {limit}"
        )));
    }
    let mut extra = HashMap::new();
    if let Some(options) = object.get("options") {
        let options = options.as_object().ok_or_else(|| {
            crate::plugins::runtime_error("mix options 'options' must be a table")
        })?;
        for (key, value) in options {
            let value = match value {
                serde_json::Value::String(value) => value.clone(),
                serde_json::Value::Bool(value) => value.to_string(),
                serde_json::Value::Number(value) => value.to_string(),
                other => {
                    return Err(crate::plugins::runtime_error(format!(
                        "mix options 'options.{key}' must be a string, boolean, or number, got {other}"
                    )));
                }
            };
            extra.insert(key.clone(), value);
        }
    }

    Ok(MixOptions::for_principal(principal, limit, extra))
}

fn parse_option_declaration(
    vm: &luau::Vm,
    config: &luau::Table,
) -> luau::runtime::Result<OptionDeclaration> {
    let name = required_string_field(vm, config, "name")?;
    let label = required_string_field(vm, config, "label")?;
    let option_type = match required_string_field(vm, config, "type")?.as_str() {
        "boolean" => OptionType::Boolean,
        "number" => OptionType::Number,
        "string" => OptionType::String,
        other => {
            return Err(crate::plugins::runtime_error(format!(
                "unsupported mix option type '{other}'; expected boolean, number, or string"
            )));
        }
    };
    let default = match config.get_raw(vm, "default")? {
        luau::Value::Nil => serde_json::Value::Null,
        value => harmony_serde::luau_to_json(vm, &value, 0)?,
    };
    let requires_settings = match config.get_raw(vm, "requires_settings")? {
        luau::Value::Nil => Vec::new(),
        luau::Value::Table(table) => {
            let mut values = Vec::new();
            for (key, value) in table.pairs_raw(vm)? {
                if !matches!(key, luau::Value::Integer(_) | luau::Value::Number(_)) {
                    continue;
                }
                let luau::Value::String(value) = value else {
                    continue;
                };
                values.push(String::from_utf8(value).map_err(crate::plugins::runtime_error)?);
            }
            values
        }
        other => {
            return Err(crate::plugins::runtime_error(format!(
                "mix option requires_settings must be an array table, got {}",
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

fn required_string_field(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &'static str,
) -> luau::runtime::Result<String> {
    match table.get_raw(vm, key)? {
        luau::Value::String(value) => {
            String::from_utf8(value).map_err(crate::plugins::runtime_error)
        }
        luau::Value::Nil => Err(crate::plugins::runtime_error(format!(
            "mix option '{key}' is required"
        ))),
        other => Err(crate::plugins::runtime_error(format!(
            "mix option '{key}' must be a string, got {}",
            other.type_name()
        ))),
    }
}

/// The mix service seeds by public id, which it re-resolves under each guard it takes.
async fn seed_public_id(db: Db, seed: DbId) -> luau::runtime::Result<Option<String>> {
    let db = db.read().await;
    lyra_db::lookup::find_id_by_db_id(&*db, seed).map_err(crate::plugins::runtime_error)
}

fn parse_seed_id(seed_id: i64, label: &'static str) -> luau::runtime::Result<DbId> {
    if seed_id <= 0 {
        return Err(crate::plugins::runtime_error(format!(
            "mix.{label}: seed id must be a positive number, got {seed_id}"
        )));
    }
    Ok(DbId(seed_id))
}

fn parse_positive_usize(
    value: &serde_json::Value,
    field: &'static str,
) -> luau::runtime::Result<usize> {
    let value = parse_positive_i64(value, field)?;
    usize::try_from(value)
        .map_err(|_| crate::plugins::runtime_error(format!("mix options '{field}' is too large")))
}

fn parse_positive_i64(
    value: &serde_json::Value,
    field: &'static str,
) -> luau::runtime::Result<i64> {
    let Some(value) = value.as_i64() else {
        return Err(crate::plugins::runtime_error(format!(
            "mix options '{field}' must be a whole number"
        )));
    };
    if value <= 0 {
        return Err(crate::plugins::runtime_error(format!(
            "mix options '{field}' must be positive, got {value}"
        )));
    }
    Ok(value)
}

fn tracks_to_luau(tracks: Option<Vec<Track>>) -> luau::runtime::Result<luau::Value> {
    let Some(tracks) = tracks else {
        return Ok(luau::Value::Nil);
    };
    harmony_luau::serializable_to_luau_owned(tracks)
}

#[cfg(feature = "docgen")]
fn support_aliases() -> Vec<TypeAliasDescriptor> {
    vec![
        TypeAliasDescriptor {
            name: "Track",
            description: Some("Track table returned by mix consumers."),
            ty: track_type(),
        },
        TypeAliasDescriptor {
            name: "MixHandler",
            description: Some(
                "A handler function that receives a context table and returns a mix result.",
            ),
            ty: MixHandler::luau_type(),
        },
        TypeAliasDescriptor {
            name: "MixRecentListensHandler",
            description: Some("A handler function for recent-listens mixes."),
            ty: MixRecentListensHandler::luau_type(),
        },
    ]
}

#[cfg(feature = "docgen")]
fn track_type() -> LuauType {
    LuauType::object(vec![
        FieldDescriptor {
            name: "db_id",
            ty: Option::<i64>::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "id",
            ty: String::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "track_title",
            ty: String::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "sort_title",
            ty: Option::<String>::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "year",
            ty: Option::<u32>::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "disc",
            ty: Option::<u32>::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "disc_total",
            ty: Option::<u32>::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "track",
            ty: Option::<u32>::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "track_total",
            ty: Option::<u32>::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "duration_ms",
            ty: Option::<u64>::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "track_gain_db",
            ty: Option::<f64>::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "album_gain_db",
            ty: Option::<f64>::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "locked",
            ty: Option::<bool>::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "created_at",
            ty: Option::<u64>::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "ctime",
            ty: Option::<u64>::luau_type(),
            description: None,
        },
    ])
}

#[cfg(feature = "docgen")]
fn support_interfaces() -> Vec<harmony_luau::InterfaceDescriptor> {
    vec![
        OptionConfig::interface_descriptor(),
        harmony_luau::InterfaceDescriptor {
            name: "MixContext",
            description: Some("Context passed to a mix handler."),
            fields: vec![
                FieldDescriptor {
                    name: "seed_id",
                    ty: f64::luau_type(),
                    description: None,
                },
                FieldDescriptor {
                    name: "limit",
                    ty: LuauType::Optional(Box::new(f64::luau_type())),
                    description: None,
                },
                FieldDescriptor {
                    name: "options",
                    ty: LuauType::Optional(Box::new(LuauType::literal(
                        "{ [string]: boolean | string | number }",
                    ))),
                    description: None,
                },
            ],
        },
        harmony_luau::InterfaceDescriptor {
            name: "MixRecentListensContext",
            description: Some("Context passed to a recent-listens mix handler."),
            fields: vec![
                FieldDescriptor {
                    name: "limit",
                    ty: LuauType::Optional(Box::new(f64::luau_type())),
                    description: None,
                },
                FieldDescriptor {
                    name: "recent_track_ids",
                    ty: LuauType::literal("{ number }"),
                    description: None,
                },
                FieldDescriptor {
                    name: "options",
                    ty: LuauType::Optional(Box::new(LuauType::literal(
                        "{ [string]: boolean | string | number }",
                    ))),
                    description: None,
                },
            ],
        },
        harmony_luau::InterfaceDescriptor {
            name: "MixResultTrack",
            description: Some("A track entry in a mix result."),
            fields: vec![FieldDescriptor {
                name: "track_id",
                ty: f64::luau_type(),
                description: None,
            }],
        },
        harmony_luau::InterfaceDescriptor {
            name: "MixResult",
            description: Some("The result returned by a mix handler."),
            fields: vec![FieldDescriptor {
                name: "tracks",
                ty: LuauType::literal("{ MixResultTrack }"),
                description: None,
            }],
        },
        harmony_luau::InterfaceDescriptor {
            name: "MixConsumerOptions",
            description: Some("Options for mix.from_* and instant_mix_from_audio."),
            fields: vec![
                FieldDescriptor {
                    name: "limit",
                    ty: LuauType::Optional(Box::new(f64::luau_type())),
                    description: None,
                },
                FieldDescriptor {
                    name: "options",
                    ty: LuauType::Optional(Box::new(LuauType::literal(
                        "{ [string]: boolean | string | number }",
                    ))),
                    description: None,
                },
            ],
        },
    ]
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &MixModule::module_descriptor(),
        &support_aliases(),
        &support_interfaces(),
        &[Mixer::class_descriptor()],
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn principal(permissions: Vec<lyra_db::Permission>) -> Principal {
        Principal::from_parts(
            DbId(7),
            "viewer".to_string(),
            "viewer".to_string(),
            permissions,
            None,
            HashSet::from(["visible-library".to_string()]),
        )
    }

    #[test]
    fn consumer_options_take_viewer_and_libraries_from_principal() -> luau::runtime::Result<()> {
        let options = parse_consumer_options(None, &principal(Vec::new()))?;
        assert_eq!(
            options
                .viewer
                .as_ref()
                .map(|viewer| viewer.user_public_id.as_str()),
            Some("viewer")
        );
        assert_eq!(
            options.viewer_accessible_library_ids,
            Some(HashSet::from(["visible-library".to_string()]))
        );

        let admin = parse_consumer_options(None, &principal(vec![lyra_db::Permission::Admin]))?;
        assert!(admin.viewer.is_some());
        assert_eq!(admin.viewer_accessible_library_ids, None);
        Ok(())
    }
}
