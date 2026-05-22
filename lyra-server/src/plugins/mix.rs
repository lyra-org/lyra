// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    cell::{
        Cell,
        RefCell,
    },
    collections::HashMap,
    sync::Arc,
};

use agdb::DbId;
use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;
use harmony_luau::{
    DescribeInterface,
    DescribeModule,
    DescribeUserData,
    FieldDescriptor,
    FunctionParameter,
    LuauType,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    TypeAliasDescriptor,
    render_definition_file_with_support,
};
use nanoid::nanoid;

use crate::plugins::db::{
    self,
    Track,
    mixers::MixerConfig,
};

use crate::{
    STATE,
    plugins::OptionConfig,
    services::mix::{
        self as mix_service,
        MAX_LIMIT,
        MixOptions,
        MixSeedType,
    },
    services::options::{
        OptionDeclaration,
        OptionType,
    },
};

struct MixHandler;
struct MixRecentListensHandler;
struct MixModule;

#[derive(Default)]
pub(crate) struct MixCallbackRegistry {
    next_handler_id: Cell<u64>,
    handlers: RefCell<HashMap<u64, MixCallback>>,
}

impl MixCallbackRegistry {
    pub(crate) fn new() -> Self {
        Self {
            next_handler_id: Cell::new(1),
            handlers: RefCell::new(HashMap::new()),
        }
    }

    fn register(
        &self,
        mixer_id: String,
        _seed_type: MixSeedType,
        function: luau::Function,
        context: harmony_core::CallContext,
    ) -> u64 {
        let id = self.next_handler_id.get();
        self.next_handler_id.set(id.saturating_add(1));
        self.handlers.borrow_mut().insert(
            id,
            MixCallback {
                mixer_id,
                function,
                context,
            },
        );
        id
    }

    pub(crate) fn get(&self, id: u64) -> Option<MixCallback> {
        self.handlers.borrow().get(&id).cloned()
    }
}

#[derive(Clone)]
pub(crate) struct MixCallback {
    pub(crate) mixer_id: String,
    pub(crate) function: luau::Function,
    pub(crate) context: harmony_core::CallContext,
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
    ) -> luau::runtime::Result<()> {
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
    ) -> luau::runtime::Result<()> {
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
    ) -> luau::runtime::Result<()> {
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
    ) -> luau::runtime::Result<()> {
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
    ) -> luau::runtime::Result<()> {
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
    ) -> luau::runtime::Result<()> {
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
    ) -> luau::runtime::Result<()> {
        let plugin_id = self.current_plugin_id(context)?;
        ensure_registration_open(&plugin_id)?;
        let option = parse_option_declaration(vm, &config)?;
        futures::executor::block_on(async {
            mix_service::MIX_REGISTRY
                .write()
                .await
                .declare_option(&self.id, option)
        })
        .map_err(crate::plugins::runtime_error)
    }

    #[harmony(skip)]
    fn register_handler(
        &self,
        context: &luau::CallContext,
        vm: &luau::Vm,
        seed_type: MixSeedType,
        function: luau::Function,
    ) -> luau::runtime::Result<()> {
        let plugin_id = self.current_plugin_id(context)?;
        ensure_registration_open(&plugin_id)?;
        let handlers = vm.data().get::<MixCallbackRegistry>()?;
        let handler_id = handlers.register(
            self.id.clone(),
            seed_type,
            function,
            core_call_context(context),
        );
        futures::executor::block_on(async {
            mix_service::MIX_REGISTRY
                .write()
                .await
                .set_seed_callback(&self.id, seed_type, handler_id);
        });
        Ok(())
    }

    #[harmony(skip)]
    fn current_plugin_id(
        &self,
        context: &luau::CallContext,
    ) -> luau::runtime::Result<crate::plugins::lifecycle::PluginId> {
        let plugin_id = context.origin.plugin.clone().ok_or_else(|| {
            crate::plugins::runtime_error("mix.Mixer methods must be called from plugin Luau code")
        })?;
        crate::plugins::lifecycle::PluginId::new(plugin_id.to_string())
            .map_err(crate::plugins::runtime_error)
    }
}

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/mix")
        .capability("lyra.mix")
        .function(
            FunctionSpec::sync_fn("Mixer.new")
                .arg_name("id")
                .args::<String>()
                .returns::<Mixer>()
                .call(mixer_new_callback),
        )
        .function(consumer_spec("from_track", from_track_callback))
        .function(consumer_spec("from_release", from_release_callback))
        .function(consumer_spec("from_artist", from_artist_callback))
        .function(consumer_spec("from_genre", from_genre_callback))
        .function(consumer_spec("from_playlist", from_playlist_callback))
        .function(consumer_spec(
            "instant_mix_from_audio",
            instant_mix_from_audio_callback,
        ))
        .userdata(Mixer::_harmony_userdata_spec())
        .install(|_| Ok(ModuleExport::new(MixModule)))
}

fn consumer_spec(
    name: &'static str,
    callback: fn(luau::AsyncCallFrame<'_>) -> luau::runtime::Result<luau::ScheduledFuture>,
) -> FunctionSpec {
    FunctionSpec::async_fn(name)
        .arg_name("seed_id")
        .args::<i64>()
        .arg_name("opts")
        .args::<Option<luau::Value>>()
        .returns::<Option<Vec<Track>>>()
        .call_async(Arc::new(callback))
}

fn mixer_new_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let id: String = frame.args.read_named("id")?;
    let Some(plugin_id) = frame.context.origin.plugin.clone() else {
        return Err(crate::plugins::runtime_error(
            "mix.Mixer.new must be called from plugin Luau code",
        ));
    };
    let plugin_id = crate::plugins::lifecycle::PluginId::new(plugin_id.to_string())
        .map_err(crate::plugins::runtime_error)?;
    futures::executor::block_on(async {
        let _registration = STATE
            .plugin_registries
            .ensure_registrations_open(&plugin_id)
            .await
            .map_err(crate::plugins::runtime_error)?;
        mix_service::MIX_REGISTRY
            .write()
            .await
            .register(plugin_id, id.clone())
            .map_err(crate::plugins::runtime_error)?;

        let mut db = STATE.db.write().await;
        if db::mixers::get_by_mixer_id(&db, &id)
            .map_err(crate::plugins::runtime_error)?
            .is_none()
        {
            let mixer_config = MixerConfig {
                db_id: None,
                id: nanoid!(),
                mixer_id: id.clone(),
                display_name: id.clone(),
                priority: 50,
                enabled: true,
            };
            db::mixers::upsert(&mut db, &mixer_config).map_err(crate::plugins::runtime_error)?;
        }

        Ok::<(), luau::Error>(())
    })?;

    let mixer = Mixer { id };
    frame.returns.write(Mixer::_harmony_userdata_class().create(
        frame.vm,
        &frame.context.origin,
        mixer,
    )?)
}

fn ensure_registration_open(
    plugin_id: &crate::plugins::lifecycle::PluginId,
) -> luau::runtime::Result<()> {
    futures::executor::block_on(async {
        let _registration = STATE
            .plugin_registries
            .ensure_registrations_open(plugin_id)
            .await
            .map_err(crate::plugins::runtime_error)?;
        Ok(())
    })
}

fn from_track_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let seed = parse_seed_id(frame.args.read_named("seed_id")?, "from_track")?;
    let options = parse_consumer_options(frame.vm, frame.args.read_optional_named("opts")?)?;
    Ok(luau::ScheduledFuture::new(async move {
        let tracks = mix_service::from_track(seed, &options)
            .await
            .map_err(crate::plugins::runtime_error)?;
        Ok(tracks_to_luau(tracks)?)
    }))
}

fn from_release_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let seed = parse_seed_id(frame.args.read_named("seed_id")?, "from_release")?;
    let options = parse_consumer_options(frame.vm, frame.args.read_optional_named("opts")?)?;
    Ok(luau::ScheduledFuture::new(async move {
        let tracks = mix_service::from_release(seed, &options)
            .await
            .map_err(crate::plugins::runtime_error)?;
        Ok(tracks_to_luau(tracks)?)
    }))
}

fn from_artist_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let seed = parse_seed_id(frame.args.read_named("seed_id")?, "from_artist")?;
    let options = parse_consumer_options(frame.vm, frame.args.read_optional_named("opts")?)?;
    Ok(luau::ScheduledFuture::new(async move {
        let tracks = mix_service::from_artist(seed, &options)
            .await
            .map_err(crate::plugins::runtime_error)?;
        Ok(tracks_to_luau(tracks)?)
    }))
}

fn from_genre_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let seed = parse_seed_id(frame.args.read_named("seed_id")?, "from_genre")?;
    let options = parse_consumer_options(frame.vm, frame.args.read_optional_named("opts")?)?;
    Ok(luau::ScheduledFuture::new(async move {
        let tracks = mix_service::from_genre(seed, &options)
            .await
            .map_err(crate::plugins::runtime_error)?;
        Ok(tracks_to_luau(tracks)?)
    }))
}

fn from_playlist_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let seed = parse_seed_id(frame.args.read_named("seed_id")?, "from_playlist")?;
    let options = parse_consumer_options(frame.vm, frame.args.read_optional_named("opts")?)?;
    Ok(luau::ScheduledFuture::new(async move {
        let tracks = mix_service::from_playlist(seed, &options)
            .await
            .map_err(crate::plugins::runtime_error)?;
        Ok(tracks_to_luau(tracks)?)
    }))
}

fn instant_mix_from_audio_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let seed = parse_seed_id(frame.args.read_named("seed_id")?, "instant_mix_from_audio")?;
    let options = parse_consumer_options(frame.vm, frame.args.read_optional_named("opts")?)?;
    Ok(luau::ScheduledFuture::new(async move {
        let tracks = mix_service::instant_mix_from_audio(seed, &options)
            .await
            .map_err(crate::plugins::runtime_error)?;
        Ok(tracks_to_luau(tracks)?)
    }))
}

fn parse_consumer_options(
    vm: &luau::Vm,
    opts: Option<luau::Value>,
) -> luau::runtime::Result<MixOptions> {
    let Some(opts) = opts else {
        return Ok(MixOptions::default());
    };
    if matches!(opts, luau::Value::Nil) {
        return Ok(MixOptions::default());
    }
    let json = harmony_json::luau_to_json(vm, &opts, 0)?;
    if matches!(&json, serde_json::Value::Array(values) if values.is_empty()) {
        return Ok(MixOptions::default());
    }
    let object = json
        .as_object()
        .ok_or_else(|| crate::plugins::runtime_error("mix options must be a table"))?;

    let limit = object
        .get("limit")
        .map(|value| parse_positive_usize(value, "limit"))
        .transpose()?;
    if let Some(limit) = limit {
        if limit > MAX_LIMIT {
            return Err(crate::plugins::runtime_error(format!(
                "mix options 'limit' must be <= {MAX_LIMIT}, got {limit}"
            )));
        }
    }
    let user_db_id = object
        .get("user_id")
        .map(|value| parse_positive_i64(value, "user_id").map(DbId))
        .transpose()?;
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

    Ok(MixOptions {
        limit,
        user_db_id,
        extra,
    })
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
        value => harmony_json::luau_to_json(vm, &value, 0)?,
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

fn core_call_context(context: &luau::CallContext) -> harmony_core::CallContext {
    let mut caller = harmony_core::ContextBag::default();
    for (type_id, value) in context.caller.cloned_entries() {
        caller.insert_shared(type_id, value);
    }
    harmony_core::CallContext {
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

fn param(name: &'static str, ty: LuauType) -> ParameterDescriptor {
    ParameterDescriptor {
        name,
        ty,
        description: None,
        variadic: false,
    }
}

fn id_param() -> ParameterDescriptor {
    param("id", f64::luau_type())
}

fn opts_param() -> ParameterDescriptor {
    ParameterDescriptor {
        name: "opts",
        ty: LuauType::Optional(Box::new(LuauType::literal("MixConsumerOptions"))),
        description: None,
        variadic: false,
    }
}

impl DescribeModule for MixModule {
    fn module_descriptor() -> ModuleDescriptor {
        let mut descriptor = ModuleDescriptor::new("Mix", "mix", None);
        let optional_track_list =
            || vec![LuauType::Optional(Box::new(LuauType::literal("{ Track }")))];

        descriptor.functions.extend(vec![
            ModuleFunctionDescriptor {
                path: vec!["Mixer", "new"],
                description: Some("Creates a mixer registration object."),
                params: vec![param("id", String::luau_type())],
                returns: vec![LuauType::literal("Mixer")],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["from_track"],
                description: Some("Mix from a seed track. Nil if missing."),
                params: vec![id_param(), opts_param()],
                returns: optional_track_list(),
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["from_release"],
                description: Some("Mix from a seed release. Nil if missing."),
                params: vec![id_param(), opts_param()],
                returns: optional_track_list(),
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["from_artist"],
                description: Some("Mix from a seed artist. Nil if missing."),
                params: vec![id_param(), opts_param()],
                returns: optional_track_list(),
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["from_genre"],
                description: Some("Mix from a seed genre. Nil if missing."),
                params: vec![id_param(), opts_param()],
                returns: optional_track_list(),
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["from_playlist"],
                description: Some("Mix from a seed playlist. Nil if missing."),
                params: vec![id_param(), opts_param()],
                returns: optional_track_list(),
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["instant_mix_from_audio"],
                description: Some(
                    "Mix from a seed track with the seed pinned at index 1. Nil if missing.",
                ),
                params: vec![id_param(), opts_param()],
                returns: optional_track_list(),
                yields: true,
            },
        ]);
        descriptor
    }
}

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
            name: "sample_rate_hz",
            ty: Option::<u32>::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "channel_count",
            ty: Option::<u32>::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "bit_depth",
            ty: Option::<u32>::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "bitrate_bps",
            ty: Option::<u32>::luau_type(),
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
                    name: "user_id",
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
                    name: "user_id",
                    ty: f64::luau_type(),
                    description: None,
                },
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
                    name: "user_id",
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

pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &MixModule::module_descriptor(),
        &support_aliases(),
        &support_interfaces(),
        &[Mixer::class_descriptor()],
    )
}
