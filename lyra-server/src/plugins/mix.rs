// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::time::Duration;

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
    plugins::args::{
        self,
        JsonArg,
    },
    plugins::db::Db,
    plugins::types::LuauMixSeedType,
    services::auth::Principal,
    services::mix::{
        self as mix_service,
        DEFAULT_MIXER_HANDLER_TIMEOUT,
        MIX_DISPATCH_BUDGET,
        MixError,
        MixHandlerRegistration,
        MixHandlerSpec,
        MixRequest,
        MixRequestFields,
        MixShape,
        MixViewer,
    },
    services::options::{
        OptionDeclaration,
        OptionType,
    },
    services::plugin_dispatch::PluginCallbackHandle,
    services::plugin_scope::PluginId,
};
use lyra_db::Track;

struct MixBlendHandler;
struct MixPathHandler;
struct MixBlendSpec;
struct MixPathSpec;
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
        function: luau::Function,
        context: harmony_core::CallContext,
    ) -> PluginCallbackHandle {
        self.handlers.insert(MixCallback {
            mixer_id,
            function,
            context,
        })
    }

    fn remove(&self, handle: PluginCallbackHandle) {
        self.handlers.remove(handle);
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

fn handler_type(context: &'static str) -> LuauType {
    LuauType::function(
        vec![FunctionParameter {
            name: Some("ctx"),
            ty: LuauType::literal(context),
            variadic: false,
        }],
        vec![LuauType::literal("MixResult")],
    )
}

impl LuauTypeInfo for MixBlendHandler {
    fn luau_type() -> LuauType {
        handler_type("MixBlendContext")
    }
}

impl LuauTypeInfo for MixPathHandler {
    fn luau_type() -> LuauType {
        handler_type("MixPathContext")
    }
}

impl LuauTypeInfo for MixBlendSpec {
    fn luau_type() -> LuauType {
        LuauType::literal("MixBlendSpec")
    }
}

impl LuauTypeInfo for MixPathSpec {
    fn luau_type() -> LuauType {
        LuauType::literal("MixPathSpec")
    }
}

#[harmony_macros::userdata(name = "Mixer")]
#[derive(Clone)]
struct Mixer {
    plugin_id: PluginId,
    id: String,
}

#[harmony_macros::userdata_methods]
impl Mixer {
    #[harmony(
        description = "Registers the handler for blends: mixes of tracks like the seeds, or like what the query describes. A blend goes to the handler only when its spec covers the request's seed types, seed count, excluded seeds and query; registering again replaces the handler.",
        args(spec: MixBlendSpec, handler: MixBlendHandler)
    )]
    fn blend(
        &self,
        context: &luau::CallContext,
        vm: &luau::Vm,
        spec: luau::Table,
        handler: luau::Function,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        self.register_handler(context, vm, MixShape::Blend, spec, handler)
    }

    #[harmony(
        description = "Registers the handler for paths: tracks leading from the first seed to the second, in play order. A path goes to the handler only when its spec covers the two seeds' types and the query; registering again replaces the handler.",
        args(spec: MixPathSpec, handler: MixPathHandler)
    )]
    fn path(
        &self,
        context: &luau::CallContext,
        vm: &luau::Vm,
        spec: luau::Table,
        handler: luau::Function,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        self.register_handler(context, vm, MixShape::Path, spec, handler)
    }

    #[harmony(
        description = "Declares an option clients can set when requesting a mix, as `<mixer_id>.<name>`.",
        args(config: OptionConfig)
    )]
    fn declare_option(
        &self,
        context: &luau::CallContext,
        vm: &luau::Vm,
        config: luau::Table,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        self.ensure_owner(vm, context)?;
        let option = parse_option_declaration(vm, &config)?;
        let mixer = self.clone();
        Ok(luau::ScheduledFuture::new(async move {
            let generation = crate::plugins::STATE.generation();
            let _registration = generation
                .registries
                .ensure_registrations_open(&mixer.plugin_id)
                .await?;
            mix_service::mix_registry()
                .write_owned()
                .await
                .declare_option(&mixer.id, option)
                .map_err(crate::plugins::runtime_error)
        }))
    }

    #[harmony(skip)]
    fn register_handler(
        &self,
        context: &luau::CallContext,
        vm: &luau::Vm,
        shape: MixShape,
        spec: luau::Table,
        function: luau::Function,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        self.ensure_owner(vm, context)?;
        let (spec, timeout) = parse_handler_spec(vm, &spec, shape)?;
        let mixer = self.clone();
        let context = context.clone();
        let vm = vm.clone();
        Ok(luau::ScheduledFuture::new(async move {
            let generation = crate::plugins::STATE.generation();
            let _registration = generation
                .registries
                .ensure_registrations_open(&mixer.plugin_id)
                .await?;
            let mut registry = mix_service::mix_registry().write_owned().await;
            let handlers = vm.data().get::<MixCallbackRegistry>()?;
            let handler = handlers.register(mixer.id.clone(), function, context);
            let registration = MixHandlerRegistration {
                spec,
                timeout,
                handler,
            };
            match registry.set_handler(&mixer.id, shape, registration) {
                Ok(replaced) => {
                    if let Some(replaced) = replaced {
                        handlers.remove(replaced);
                    }
                    Ok(())
                }
                Err(error) => {
                    handlers.remove(handler);
                    Err(error.into())
                }
            }
        }))
    }

    #[harmony(skip)]
    fn ensure_owner(
        &self,
        vm: &luau::Vm,
        context: &luau::CallContext,
    ) -> luau::runtime::Result<()> {
        if context.origin.plugin.as_deref() != Some(self.plugin_id.as_ref()) {
            return Err(crate::plugins::runtime_error(format!(
                "mixer '{}' method must be called by owning plugin '{}'",
                self.id, self.plugin_id
            )));
        }
        harmony_core::ensure_current_plugin_run(vm, context)
    }
}

/// What `Mixer.new` takes from its call: the calling plugin, which must be the current run, and
/// the handles its registration needs.
struct MixerHost {
    plugin_id: PluginId,
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
        let plugin_id = PluginId::new(plugin_id.to_string())?;
        harmony_core::ensure_current_plugin_run(vm, context)?;
        Ok(Self {
            plugin_id,
            vm: vm.clone(),
            origin: context.origin.clone(),
        })
    }
}

/// The documented types of the module's arguments and returns.
#[cfg(feature = "docgen")]
macro_rules! documented_type {
    ($marker:ident, $ty:expr) => {
        #[cfg(feature = "docgen")]
        struct $marker;

        #[cfg(feature = "docgen")]
        impl LuauTypeInfo for $marker {
            fn luau_type() -> LuauType {
                $ty
            }
        }
    };
}

#[cfg(feature = "docgen")]
documented_type!(MixerOptions, LuauType::literal("MixerOptions"));
#[cfg(feature = "docgen")]
documented_type!(MixRequestType, LuauType::literal("MixRequest"));
#[cfg(feature = "docgen")]
documented_type!(MixPathRequest, LuauType::literal("MixPathRequest"));
#[cfg(feature = "docgen")]
documented_type!(
    OptionalTrackList,
    LuauType::Optional(Box::new(LuauType::literal("{ Track }")))
);

#[harmony_macros::module(id = "lyra/mix", docs_feature = "docgen", userdata(LuauMixSeedType, Mixer))]
impl MixModule {
    #[harmony(
        name = "Mixer.new",
        description = "Creates a mixer registration object.",
        args(id: String, options: Option<MixerOptions>),
        returns(Mixer)
    )]
    async fn mixer_new(
        id: String,
        options: Option<luau::Table>,
        #[context] host: MixerHost,
        #[context] db: Db,
    ) -> luau::runtime::Result<luau::UserData> {
        let MixerHost {
            plugin_id,
            vm,
            origin,
        } = host;
        let display_name = match &options {
            Some(options) => optional_display_name(&vm, options)?,
            None => None,
        };
        let generation = crate::plugins::STATE.generation();
        let _registration = generation
            .registries
            .ensure_registrations_open(&plugin_id)
            .await?;
        mix_service::mix_registry()
            .write_owned()
            .await
            .register(plugin_id.clone(), id.clone())?;

        let mut db = db.write().await;
        lyra_db::mixers::register(&mut db, &id, display_name.as_deref())?;

        drop(db);
        Mixer::_harmony_userdata_class().create(&vm, &origin, Mixer { plugin_id, id })
    }

    #[harmony(
        description = "Mixes tracks like the request's seeds, the caller's recent listens with `from_history`, or what `q` describes, as `POST /api/mix` does. Nil if a seed is missing or hidden from the caller; errors when the request is invalid or no mixer can serve it.",
        args(request: MixRequestType),
        returns(OptionalTrackList)
    )]
    async fn blend(
        request: JsonArg,
        #[context] principal: Principal,
        #[context] db: Db,
    ) -> luau::runtime::Result<luau::Value> {
        consume(MixShape::Blend, request, principal, db).await
    }

    #[harmony(
        description = "Tracks leading from the request's first seed to its second, in the mixer's order, up to 2000 tracks; tracks the caller can't see are left out and `limit` is only a step hint. Nil if a seed is missing or hidden from the caller; errors when the request is invalid or no mixer can serve it.",
        args(request: MixPathRequest),
        returns(OptionalTrackList)
    )]
    async fn path(
        request: JsonArg,
        #[context] principal: Principal,
        #[context] db: Db,
    ) -> luau::runtime::Result<luau::Value> {
        consume(MixShape::Path, request, principal, db).await
    }
}

fn optional_display_name(
    vm: &luau::Vm,
    options: &luau::Table,
) -> luau::runtime::Result<Option<String>> {
    match options.get_raw(vm, "display_name")? {
        luau::Value::Nil => Ok(None),
        luau::Value::String(bytes) => {
            let name = String::from_utf8(bytes).map_err(crate::plugins::runtime_error)?;
            let name = name.trim();
            Ok((!name.is_empty()).then(|| name.to_string()))
        }
        other => Err(crate::plugins::runtime_error(format!(
            "mix.Mixer.new: display_name must be a string, got {}",
            other.type_name()
        ))),
    }
}

/// The spec a handler registers with, and its timeout.
fn parse_handler_spec(
    vm: &luau::Vm,
    spec: &luau::Table,
    shape: MixShape,
) -> luau::runtime::Result<(MixHandlerSpec, Duration)> {
    let owner = format!("mixer:{} spec", shape.as_str());
    let fields: &[&str] = match shape {
        MixShape::Blend => &["seeds", "max_seeds", "exclude", "query", "timeout_ms"],
        MixShape::Path => &["seeds", "query", "timeout_ms"],
    };
    for (key, _) in spec.pairs_raw(vm)? {
        let known = match &key {
            luau::Value::String(key) => fields.iter().any(|field| field.as_bytes() == key),
            _ => false,
        };
        if !known {
            return Err(crate::plugins::runtime_error(format!(
                "{owner}: unknown field {}; expected {}",
                describe_key(&key),
                fields.join(", ")
            )));
        }
    }

    let mut seed_types = Vec::new();
    match spec.get_raw(vm, "seeds")? {
        luau::Value::Nil => {}
        luau::Value::Table(seeds) => {
            for (_, value) in args::array_values(vm, &seeds)? {
                let seed_type = LuauMixSeedType::_harmony_userdata_class()
                    .read_value(vm, "seeds", value)?
                    .into();
                if !seed_types.contains(&seed_type) {
                    seed_types.push(seed_type);
                }
            }
        }
        other => {
            return Err(crate::plugins::runtime_error(format!(
                "{owner}.seeds must be an array of SeedType, got {}",
                other.type_name()
            )));
        }
    }
    let query = args::optional_bool(vm, spec, "query", &owner)?.unwrap_or(false);
    let (max_seeds, exclude) = match shape {
        MixShape::Blend => (
            args::optional_i64(vm, spec, "max_seeds", &owner)?
                .map(|max_seeds| {
                    usize::try_from(max_seeds)
                        .ok()
                        .filter(|max| *max >= 1)
                        .ok_or_else(|| {
                            crate::plugins::runtime_error(format!(
                                "{owner}.max_seeds must be an integer >= 1"
                            ))
                        })
                })
                .transpose()?
                .unwrap_or(1),
            args::optional_bool(vm, spec, "exclude", &owner)?.unwrap_or(false),
        ),
        MixShape::Path => (2, false),
    };
    if seed_types.is_empty() && (shape == MixShape::Path || !query) {
        return Err(crate::plugins::runtime_error(match shape {
            MixShape::Blend => format!("{owner} needs seeds or query"),
            MixShape::Path => format!("{owner}.seeds must name a SeedType"),
        }));
    }

    let timeout = args::optional_timeout(
        vm,
        spec,
        "timeout_ms",
        &owner,
        DEFAULT_MIXER_HANDLER_TIMEOUT,
    )?;
    if timeout > MIX_DISPATCH_BUDGET {
        return Err(crate::plugins::runtime_error(format!(
            "{owner}.timeout_ms must be <= {}",
            MIX_DISPATCH_BUDGET.as_millis()
        )));
    }

    Ok((
        MixHandlerSpec {
            seed_types,
            max_seeds,
            exclude,
            query,
        },
        timeout,
    ))
}

fn describe_key(key: &luau::Value) -> String {
    match key {
        luau::Value::String(key) => format!("'{}'", String::from_utf8_lossy(key)),
        other => other.type_name().to_string(),
    }
}

/// Mixes `request`, a REST body with seeds by database id, in the shape the function called names.
async fn consume(
    shape: MixShape,
    request: JsonArg,
    principal: Principal,
    db: Db,
) -> luau::runtime::Result<luau::Value> {
    let label = format!("mix.{}", shape.as_str());
    let fields = request_fields(request.0, &label)?;
    let fields = {
        let db = db.read().await;
        fields.with_public_ids(|id| {
            let id = args::positive_id(id, "seeds[].id")?;
            lyra_db::lookup::find_id_by_db_id(&*db, id).map_err(crate::plugins::runtime_error)
        })?
    };
    let Some(fields) = fields else {
        return tracks_to_luau(None);
    };
    let request = MixRequest {
        shape,
        ..fields.into_request()
    };
    match mix_service::mix(&request, &MixViewer::for_principal(&principal)).await {
        Ok(tracks) => tracks_to_luau(Some(tracks)),
        Err(MixError::SeedNotFound(_)) => tracks_to_luau(None),
        Err(MixError::Internal(error)) => Err(error.into()),
        Err(error) => Err(crate::plugins::runtime_error(format!("{label}: {error}"))),
    }
}

/// The request table's fields; the shape comes from the function called.
fn request_fields(
    mut json: serde_json::Value,
    label: &str,
) -> luau::runtime::Result<MixRequestFields<i64>> {
    // An empty Luau table reads as an empty array; the request and its options are objects.
    let empty_as_object = |value: &mut serde_json::Value| {
        if value.as_array().is_some_and(Vec::is_empty) {
            *value = serde_json::Value::Object(Default::default());
        }
    };
    empty_as_object(&mut json);
    if let Some(options) = json.get_mut("options") {
        empty_as_object(options);
    }
    let fields: MixRequestFields<i64> = serde_json::from_value(json)
        .map_err(|error| crate::plugins::runtime_error(format!("{label}: request: {error}")))?;
    if fields.shape.is_some() {
        return Err(crate::plugins::runtime_error(format!(
            "{label}: request: unknown field `shape`; call mix.blend or mix.path"
        )));
    }
    Ok(fields)
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

fn tracks_to_luau(tracks: Option<Vec<Track>>) -> luau::runtime::Result<luau::Value> {
    let Some(tracks) = tracks else {
        return Ok(luau::Value::Nil);
    };
    harmony_luau::serializable_to_luau_owned(tracks)
}

#[cfg(feature = "docgen")]
fn opt(ty: LuauType) -> LuauType {
    LuauType::Optional(Box::new(ty))
}

#[cfg(feature = "docgen")]
fn field(name: &'static str, ty: LuauType, description: Option<&'static str>) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description,
    }
}

#[cfg(feature = "docgen")]
fn interface(
    name: &'static str,
    description: &'static str,
    fields: Vec<FieldDescriptor>,
) -> harmony_luau::InterfaceDescriptor {
    harmony_luau::InterfaceDescriptor {
        name,
        description: Some(description),
        fields,
    }
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
            name: "MixBlendHandler",
            description: Some("Handles a blend; returns the mix's tracks in order."),
            ty: MixBlendHandler::luau_type(),
        },
        TypeAliasDescriptor {
            name: "MixPathHandler",
            description: Some(
                "Handles a path; returns its tracks in play order, from the start seed to the end seed.",
            ),
            ty: MixPathHandler::luau_type(),
        },
        TypeAliasDescriptor {
            name: "MixSeedInput",
            description: Some("A seed's database id, or a seed entry that can exclude it."),
            ty: LuauType::union(vec![f64::luau_type(), LuauType::literal("MixSeedEntry")]),
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
fn mixer_field() -> FieldDescriptor {
    field(
        "mixer",
        opt(String::luau_type()),
        Some("Serves the request with this mixer alone, with no fallback."),
    )
}

#[cfg(feature = "docgen")]
fn options_field() -> FieldDescriptor {
    field(
        "options",
        opt(LuauType::literal("{ [string]: boolean | string | number }")),
        Some("Mixer options keyed `<mixer_id>.<name>`, each of the option's declared type."),
    )
}

#[cfg(feature = "docgen")]
fn support_interfaces() -> Vec<harmony_luau::InterfaceDescriptor> {
    let options_map = || opt(LuauType::literal("{ [string]: boolean | string | number }"));
    let seed_types = || opt(LuauType::literal("{ SeedType }"));
    // The description spells out the budget and default timeout.
    const _: () = assert!(MIX_DISPATCH_BUDGET.as_millis() == 15000);
    const _: () = assert!(DEFAULT_MIXER_HANDLER_TIMEOUT.as_millis() == 10000);
    let timeout_ms = || {
        field(
            "timeout_ms",
            opt(f64::luau_type()),
            Some(
                "How long the handler may run, up to 15000; defaults to 10000. Mixers share one 15000 ms budget per request, so a handler tried after others gets at most what remains.",
            ),
        )
    };
    let context_fields = |seeds: &'static str| {
        vec![
            field("seeds", LuauType::literal("{ MixSeed }"), Some(seeds)),
            field("q", opt(String::luau_type()), None),
            field("limit", opt(f64::luau_type()), None),
            field(
                "options",
                options_map(),
                Some("The mixer's own declared options the request set."),
            ),
        ]
    };
    vec![
        OptionConfig::interface_descriptor(),
        interface(
            "MixerOptions",
            "Options for `Mixer.new`.",
            vec![field(
                "display_name",
                opt(String::luau_type()),
                Some("Human-readable mixer name. Defaults to the mixer id."),
            )],
        ),
        interface(
            "MixBlendSpec",
            "What a blend handler takes.",
            vec![
                field(
                    "seeds",
                    seed_types(),
                    Some("The seed types it mixes from; may be empty for a query-only handler."),
                ),
                field(
                    "max_seeds",
                    opt(f64::luau_type()),
                    Some(
                        "The most explicit seeds it takes, and so the most seeds a request's recent listens fill. Defaults to 1.",
                    ),
                ),
                field(
                    "exclude",
                    opt(bool::luau_type()),
                    Some("Whether it takes excluded seeds, to mix away from. Defaults to false."),
                ),
                field(
                    "query",
                    opt(bool::luau_type()),
                    Some("Whether it takes a free-text query. Defaults to false."),
                ),
                timeout_ms(),
            ],
        ),
        interface(
            "MixPathSpec",
            "What a path handler takes.",
            vec![
                field(
                    "seeds",
                    LuauType::literal("{ SeedType }"),
                    Some("The seed types it starts and ends a path at."),
                ),
                field(
                    "query",
                    opt(bool::luau_type()),
                    Some("Whether it takes a free-text query. Defaults to false."),
                ),
                timeout_ms(),
            ],
        ),
        interface(
            "MixSeed",
            "A seed handed to a mix handler.",
            vec![
                field("id", f64::luau_type(), Some("The seed's database id.")),
                field("type", LuauType::literal("SeedType"), None),
                field(
                    "exclude",
                    bool::luau_type(),
                    Some("Whether to mix away from the seed instead of toward it."),
                ),
            ],
        ),
        interface(
            "MixBlendContext",
            "Context passed to a blend handler.",
            context_fields(
                "The seeds in request order; a request's recent listens follow as track seeds, most recent first, up to `max_seeds`.",
            ),
        ),
        interface(
            "MixPathContext",
            "Context passed to a path handler; `limit` is a step hint.",
            context_fields("The start, then the end."),
        ),
        interface(
            "MixResultTrack",
            "A track entry in a mix result.",
            vec![field("track_id", f64::luau_type(), None)],
        ),
        interface(
            "MixResult",
            "The result returned by a mix handler.",
            vec![field(
                "tracks",
                LuauType::literal("{ MixResultTrack }"),
                None,
            )],
        ),
        interface(
            "MixSeedEntry",
            "A seed, optionally excluded to mix away from it.",
            vec![
                field("id", f64::luau_type(), Some("The seed's database id.")),
                field(
                    "exclude",
                    opt(bool::luau_type()),
                    Some("Mixes away from the seed instead of toward it. Defaults to false."),
                ),
            ],
        ),
        interface(
            "MixRequest",
            "A blend request for `mix.blend`, as `POST /api/mix` takes it with seeds by database id; it needs a seed that isn't excluded, `from_history`, or `q`.",
            vec![
                field(
                    "seeds",
                    opt(LuauType::literal("{ MixSeedInput }")),
                    Some("Tracks, releases, artists, genres or playlists, by database id."),
                ),
                field(
                    "from_history",
                    opt(bool::luau_type()),
                    Some(
                        "Adds the caller's recent listens as track seeds, up to what the serving mixer takes beyond the explicit seeds.",
                    ),
                ),
                field("q", opt(String::luau_type()), Some("A free-text query.")),
                mixer_field(),
                field(
                    "pin_seed",
                    opt(bool::luau_type()),
                    Some(
                        "Leads with the single track seed; only its chain's later tracks in the mix follow it.",
                    ),
                ),
                field(
                    "limit",
                    opt(f64::luau_type()),
                    Some("The most tracks returned, up to 2000. Defaults to 200."),
                ),
                options_field(),
            ],
        ),
        interface(
            "MixPathRequest",
            "A path request for `mix.path`, as `POST /api/mix` takes it with seeds by database id.",
            vec![
                field(
                    "seeds",
                    LuauType::literal("{ MixSeedInput }"),
                    Some("The start and end, by database id."),
                ),
                field("q", opt(String::luau_type()), Some("A free-text query.")),
                mixer_field(),
                field(
                    "limit",
                    opt(f64::luau_type()),
                    Some("A hint for how many steps to take."),
                ),
                options_field(),
            ],
        ),
    ]
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &MixModule::module_descriptor(),
        &support_aliases(),
        &support_interfaces(),
        &[
            <LuauMixSeedType as DescribeUserData>::class_descriptor(),
            Mixer::class_descriptor(),
        ],
    )
}
