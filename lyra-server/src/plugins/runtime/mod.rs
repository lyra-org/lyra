// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use crate::plugins::lifecycle::PluginId;
use std::{
    collections::{
        HashMap,
        HashSet,
    },
    sync::{
        Arc,
        Mutex,
    },
};

use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
    UserDataSpec,
};
use harmony_luau as luau;
use harmony_luau::{
    ClassDescriptor,
    DescribeInterface,
    DescribeTypeAlias,
    DescribeUserData,
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

mod settings;

pub(crate) use self::settings::{
    ChoiceOption,
    FieldDefinition,
    FieldGroupDefinition,
    FieldProps,
    REGISTRY,
    Registry,
    Schema,
    SettingsScope,
    freeze_registry,
    initialize_registry,
    refreeze_plugin_settings,
    teardown_plugin_settings,
    unfreeze_plugin_settings,
};
use crate::plugins::db::DbAsync;
use crate::services::plugin_settings as plugin_settings_service;

struct SettingsConfig;

impl LuauTypeInfo for SettingsConfig {
    fn luau_type() -> LuauType {
        LuauType::Map {
            key: Box::new(String::luau_type()),
            value: Box::new(LuauType::optional(LuauType::union(vec![
                String::luau_type(),
                f64::luau_type(),
                bool::luau_type(),
            ]))),
        }
    }
}

impl DescribeTypeAlias for SettingsConfig {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "SettingsConfig",
            Self::luau_type(),
            Some("Settings configuration table returned by declare_settings."),
        )
    }
}

struct SettingsCallback;

impl LuauTypeInfo for SettingsCallback {
    fn luau_type() -> LuauType {
        LuauType::function(
            vec![FunctionParameter {
                name: Some("ui"),
                ty: LuauType::literal("SettingsBuilder"),
                variadic: false,
            }],
            vec![],
        )
    }
}

impl DescribeTypeAlias for SettingsCallback {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "SettingsCallback",
            Self::luau_type(),
            Some("Callback function that receives a SettingsBuilder to declare plugin settings."),
        )
    }
}

struct UserSettingsAccessor;

impl LuauTypeInfo for UserSettingsAccessor {
    fn luau_type() -> LuauType {
        LuauType::literal("UserSettingsAccessor")
    }
}

impl DescribeUserData for UserSettingsAccessor {
    fn class_descriptor() -> ClassDescriptor {
        ClassDescriptor {
            name: "UserSettingsAccessor",
            description: None,
            fields: vec![],
            methods: vec![MethodDescriptor {
                name: "get",
                description: None,
                params: vec![ParameterDescriptor {
                    name: "user_id",
                    ty: i64::luau_type(),
                    description: None,
                    variadic: false,
                }],
                returns: vec![SettingsConfig::luau_type()],
                yields: true,
                kind: MethodKind::Instance,
            }],
        }
    }
}

struct PluginManifest;

struct SettingsChoiceOption;

struct SettingsStringProps;

struct SettingsNumberProps;

struct SettingsBoolProps;

struct SettingsChoiceProps;

impl DescribeUserData for SettingsBuilder {
    fn class_descriptor() -> ClassDescriptor {
        ClassDescriptor {
            name: "SettingsBuilder",
            description: Some("Builder for declaring plugin settings."),
            fields: vec![],
            methods: vec![
                MethodDescriptor {
                    name: "group",
                    description: Some("Starts a settings group."),
                    params: vec![
                        ParameterDescriptor {
                            name: "id",
                            ty: String::luau_type(),
                            description: Some("Stable group identifier."),
                            variadic: false,
                        },
                        ParameterDescriptor {
                            name: "label",
                            ty: String::luau_type(),
                            description: Some("Group heading text."),
                            variadic: false,
                        },
                    ],
                    returns: vec![],
                    yields: false,
                    kind: MethodKind::Instance,
                },
                MethodDescriptor {
                    name: "string",
                    description: Some("Declares a string setting."),
                    params: vec![
                        ParameterDescriptor {
                            name: "key",
                            ty: String::luau_type(),
                            description: Some("Unique setting key."),
                            variadic: false,
                        },
                        ParameterDescriptor {
                            name: "props",
                            ty: SettingsStringProps::luau_type(),
                            description: Some("Setting properties."),
                            variadic: false,
                        },
                    ],
                    returns: vec![<Option<String> as LuauTypeInfo>::luau_type()],
                    yields: false,
                    kind: MethodKind::Instance,
                },
                MethodDescriptor {
                    name: "number",
                    description: Some("Declares a number setting."),
                    params: vec![
                        ParameterDescriptor {
                            name: "key",
                            ty: String::luau_type(),
                            description: Some("Unique setting key."),
                            variadic: false,
                        },
                        ParameterDescriptor {
                            name: "props",
                            ty: SettingsNumberProps::luau_type(),
                            description: Some("Setting properties."),
                            variadic: false,
                        },
                    ],
                    returns: vec![<Option<f64> as LuauTypeInfo>::luau_type()],
                    yields: false,
                    kind: MethodKind::Instance,
                },
                MethodDescriptor {
                    name: "bool",
                    description: Some("Declares a boolean setting."),
                    params: vec![
                        ParameterDescriptor {
                            name: "key",
                            ty: String::luau_type(),
                            description: Some("Unique setting key."),
                            variadic: false,
                        },
                        ParameterDescriptor {
                            name: "props",
                            ty: SettingsBoolProps::luau_type(),
                            description: Some("Setting properties."),
                            variadic: false,
                        },
                    ],
                    returns: vec![<Option<bool> as LuauTypeInfo>::luau_type()],
                    yields: false,
                    kind: MethodKind::Instance,
                },
                MethodDescriptor {
                    name: "choice",
                    description: Some("Declares a single-choice setting."),
                    params: vec![
                        ParameterDescriptor {
                            name: "key",
                            ty: String::luau_type(),
                            description: Some("Unique setting key."),
                            variadic: false,
                        },
                        ParameterDescriptor {
                            name: "props",
                            ty: SettingsChoiceProps::luau_type(),
                            description: Some("Setting properties."),
                            variadic: false,
                        },
                    ],
                    returns: vec![<Option<String> as LuauTypeInfo>::luau_type()],
                    yields: false,
                    kind: MethodKind::Instance,
                },
            ],
        }
    }
}

#[derive(Clone)]
struct SettingsBuilder {
    groups: Arc<Mutex<Vec<FieldGroupDefinition>>>,
    stored_values: Arc<HashMap<String, serde_json::Value>>,
}

#[derive(Clone, Default)]
pub(crate) struct PluginManifestModuleStore {
    manifests: Arc<[harmony_core::PluginManifest]>,
}
impl PluginManifestModuleStore {
    pub(crate) fn new(manifests: Arc<[harmony_core::PluginManifest]>) -> Self {
        Self { manifests }
    }

    fn iter(&self) -> impl Iterator<Item = &harmony_core::PluginManifest> {
        self.manifests.iter()
    }

    fn find(&self, id: &str) -> Option<&harmony_core::PluginManifest> {
        self.manifests.iter().find(|manifest| manifest.id == id)
    }
}
#[derive(Clone, Default)]
pub(crate) struct PluginSettingsModuleStore {
    db: Option<DbAsync>,
}
impl PluginSettingsModuleStore {
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: DbAsync) -> Self {
        Self { db: Some(db) }
    }

    fn load_stored_values(
        &self,
        plugin_id: &PluginId,
    ) -> luau::runtime::Result<HashMap<String, serde_json::Value>> {
        let Some(db) = &self.db else {
            return Ok(HashMap::new());
        };
        let db = futures::executor::block_on(db.read());
        plugin_settings_service::load_stored_values(&db, plugin_id.as_str())
            .map_err(crate::plugins::runtime_error)
    }

    fn load_user_stored_values(
        &self,
        user_db_id: agdb::DbId,
        plugin_id: &PluginId,
        schema: &Schema,
    ) -> luau::runtime::Result<HashMap<String, serde_json::Value>> {
        let db = self.db.as_ref().ok_or_else(|| {
            luau::Error::Runtime("plugin settings database is unavailable".into())
        })?;
        let db = futures::executor::block_on(db.read());
        plugin_settings_service::load_validated_user_stored_values(
            &db,
            user_db_id,
            plugin_id.as_str(),
            schema,
        )
        .map_err(crate::plugins::runtime_error)
    }
}
fn declare_settings_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let plugin_id = current_plugin_id(&frame.context)?;
    let callback: luau::Function = frame.args.read_named("callback")?;
    let store = frame
        .vm
        .data()
        .get::<PluginSettingsModuleStore>()?
        .as_ref()
        .clone();
    let stored_values = store.load_stored_values(&plugin_id)?;
    let builder = settings_builder(stored_values);
    let builder_table = settings_builder_table(frame.context.origin.clone(), builder.clone());

    callback.call(frame.vm, &[luau::Value::TableData(builder_table)])?;

    let groups = take_groups(&builder)?;
    let schema = Schema { groups };
    plugin_settings_service::validate_stored_values(
        plugin_id.as_str(),
        &schema,
        &builder.stored_values,
    )
    .map_err(crate::plugins::runtime_error)?;
    let config = build_config_table(&schema.groups, &builder)?;

    futures::executor::block_on(REGISTRY.write())
        .register_schema(plugin_id, SettingsScope::Global, schema)
        .map_err(crate::plugins::runtime_error)?;

    frame.returns.write(config)?;
    Ok(())
}
fn declare_user_settings_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let plugin_id = current_plugin_id(&frame.context)?;
    let callback: luau::Function = frame.args.read_named("callback")?;
    let store = frame
        .vm
        .data()
        .get::<PluginSettingsModuleStore>()?
        .as_ref()
        .clone();
    let builder = settings_builder(HashMap::new());
    let builder_table = settings_builder_table(frame.context.origin.clone(), builder.clone());

    callback.call(frame.vm, &[luau::Value::TableData(builder_table)])?;

    let groups = take_groups(&builder)?;
    let schema = Schema { groups };

    futures::executor::block_on(REGISTRY.write())
        .register_schema(plugin_id.clone(), SettingsScope::User, schema.clone())
        .map_err(crate::plugins::runtime_error)?;

    frame.returns.write(user_settings_accessor_table(
        frame.context.origin.clone(),
        store,
        plugin_id,
        schema,
    ))?;
    Ok(())
}
fn settings_builder(stored_values: HashMap<String, serde_json::Value>) -> SettingsBuilder {
    SettingsBuilder {
        groups: Arc::new(Mutex::new(Vec::new())),
        stored_values: Arc::new(stored_values),
    }
}
fn current_plugin_id(context: &luau::CallContext) -> luau::runtime::Result<PluginId> {
    let Some(plugin_id) = context.origin.plugin.as_ref() else {
        return Err(luau::Error::Runtime(
            "plugins.* must be called from plugin Lua code".to_string(),
        ));
    };
    PluginId::new(plugin_id.clone()).map_err(crate::plugins::runtime_error)
}
fn settings_builder_table(origin: luau::ChunkOrigin, builder: SettingsBuilder) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 5);
    table.set_field(
        "group",
        settings_method(&origin, "SettingsBuilder.group", ["self", "id", "label"], {
            let builder = builder.clone();
            move |mut frame| {
                read_self(&mut frame.args)?;
                let id: String = frame.args.read_named("id")?;
                let label: String = frame.args.read_named("label")?;
                push_group(&builder, id, label)
            }
        }),
    );
    table.set_field(
        "string",
        settings_method(
            &origin,
            "SettingsBuilder.string",
            ["self", "key", "props"],
            {
                let builder = builder.clone();
                move |mut frame| {
                    read_self(&mut frame.args)?;
                    let key = register_key(&builder, frame.args.read_named("key")?)?;
                    let props: luau::Table = frame.args.read_named("props")?;
                    let field = FieldDefinition::String {
                        key,
                        props: build_field_props(
                            frame.vm,
                            &props,
                            parse_string_default(frame.vm, &props)?,
                        )?,
                    };
                    let value =
                        primitive_value_for_field(&field, builder.stored_values.get(field.key()))?;
                    push_field(&builder, field)?;
                    frame.returns.write(value)
                }
            },
        ),
    );
    table.set_field(
        "number",
        settings_method(
            &origin,
            "SettingsBuilder.number",
            ["self", "key", "props"],
            {
                let builder = builder.clone();
                move |mut frame| {
                    read_self(&mut frame.args)?;
                    let key = register_key(&builder, frame.args.read_named("key")?)?;
                    let props: luau::Table = frame.args.read_named("props")?;
                    let min = optional_number(frame.vm, &props, "min")?;
                    let max = optional_number(frame.vm, &props, "max")?;
                    if let (Some(min), Some(max)) = (min, max)
                        && min > max
                    {
                        return Err(luau::Error::Runtime(
                            "min must be less than or equal to max".to_string(),
                        ));
                    }
                    let default_value = parse_number_default(frame.vm, &props)?;
                    if let Some(default) = default_value.as_ref().and_then(|value| value.as_f64()) {
                        if let Some(min) = min
                            && default < min
                        {
                            return Err(luau::Error::Runtime(
                                "default value must be greater than or equal to min".to_string(),
                            ));
                        }
                        if let Some(max) = max
                            && default > max
                        {
                            return Err(luau::Error::Runtime(
                                "default value must be less than or equal to max".to_string(),
                            ));
                        }
                    }
                    let field = FieldDefinition::Number {
                        key,
                        props: build_field_props(frame.vm, &props, default_value)?,
                        min,
                        max,
                    };
                    let value =
                        primitive_value_for_field(&field, builder.stored_values.get(field.key()))?;
                    push_field(&builder, field)?;
                    frame.returns.write(value)
                }
            },
        ),
    );
    table.set_field(
        "bool",
        settings_method(&origin, "SettingsBuilder.bool", ["self", "key", "props"], {
            let builder = builder.clone();
            move |mut frame| {
                read_self(&mut frame.args)?;
                let key = register_key(&builder, frame.args.read_named("key")?)?;
                let props: luau::Table = frame.args.read_named("props")?;
                let field = FieldDefinition::Bool {
                    key,
                    props: build_field_props(
                        frame.vm,
                        &props,
                        parse_bool_default(frame.vm, &props)?,
                    )?,
                };
                let value =
                    primitive_value_for_field(&field, builder.stored_values.get(field.key()))?;
                push_field(&builder, field)?;
                frame.returns.write(value)
            }
        }),
    );
    table.set_field(
        "choice",
        settings_method(
            &origin,
            "SettingsBuilder.choice",
            ["self", "key", "props"],
            {
                let builder = builder.clone();
                move |mut frame| {
                    read_self(&mut frame.args)?;
                    let key = register_key(&builder, frame.args.read_named("key")?)?;
                    let props: luau::Table = frame.args.read_named("props")?;
                    let options = parse_choice_options(frame.vm, &props)?;
                    let default_value = parse_choice_default(frame.vm, &props, &options)?;
                    let field = FieldDefinition::Choice {
                        key,
                        props: build_field_props(frame.vm, &props, default_value)?,
                        options,
                    };
                    let value =
                        primitive_value_for_field(&field, builder.stored_values.get(field.key()))?;
                    push_field(&builder, field)?;
                    frame.returns.write(value)
                }
            },
        ),
    );
    table
}
fn user_settings_accessor_table(
    origin: luau::ChunkOrigin,
    store: PluginSettingsModuleStore,
    plugin_id: PluginId,
    schema: Schema,
) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 1);
    table.set_field(
        "get",
        settings_method(
            &origin,
            "UserSettingsAccessor.get",
            ["self", "user_id"],
            move |mut frame| {
                read_self(&mut frame.args)?;
                let caller = current_plugin_id(&frame.context)?;
                if caller != plugin_id {
                    return Err(luau::Error::Runtime(format!(
                        "user settings accessor for plugin '{}' must be used by the owning plugin",
                        plugin_id
                    )));
                }
                let user_id: i64 = frame.args.read_named("user_id")?;
                if user_id <= 0 {
                    return Err(luau::Error::Runtime("user_id must be positive".to_string()));
                }
                let stored =
                    store.load_user_stored_values(agdb::DbId(user_id), &plugin_id, &schema)?;
                let builder = settings_builder(stored);
                let config = build_config_table(&schema.groups, &builder)?;
                frame.returns.write(config)
            },
        ),
    );
    table
}
fn settings_method(
    origin: &luau::ChunkOrigin,
    name: &'static str,
    args: impl IntoIterator<Item = &'static str>,
    callback: impl for<'vm> Fn(luau::CallFrame<'vm>) -> luau::runtime::Result<()>
    + Send
    + Sync
    + 'static,
) -> luau::Value {
    let options = luau::NativeFunctionOptions::new(origin.clone())
        .function_name(name)
        .argument_names(args.into_iter().map(Arc::<str>::from));
    luau::Value::NativeFunction(luau::NativeFunctionValue::new(options, Arc::new(callback)))
}
fn read_self(args: &mut luau::ArgReader<'_>) -> luau::runtime::Result<()> {
    let _: luau::Table = args.read_named("self")?;
    Ok(())
}
fn register_key(builder: &SettingsBuilder, key: String) -> luau::runtime::Result<String> {
    let key = require_non_empty_string(key, "key")?;
    if builder
        .groups
        .lock()
        .unwrap()
        .iter()
        .flat_map(|group| group.fields.iter())
        .any(|field| field.key() == key.as_str())
    {
        return Err(luau::Error::Runtime(format!(
            "setting key '{key}' is already declared"
        )));
    }
    Ok(key)
}
fn push_group(builder: &SettingsBuilder, id: String, label: String) -> luau::runtime::Result<()> {
    let id = require_non_empty_string(id, "id")?;
    let label = require_non_empty_string(label, "label")?;
    let mut groups = builder.groups.lock().unwrap();
    if let Some(previous) = groups.last()
        && previous.fields.is_empty()
    {
        return Err(luau::Error::Runtime(format!(
            "settings group '{}' must declare at least one setting",
            previous.id
        )));
    }
    if groups.iter().any(|group| group.id == id) {
        return Err(luau::Error::Runtime(format!(
            "settings group '{id}' is already declared"
        )));
    }
    groups.push(FieldGroupDefinition {
        id,
        label,
        fields: Vec::new(),
    });
    Ok(())
}
fn push_field(builder: &SettingsBuilder, field: FieldDefinition) -> luau::runtime::Result<()> {
    let mut groups = builder.groups.lock().unwrap();
    let Some(group) = groups.last_mut() else {
        return Err(luau::Error::Runtime(
            "declare a group before adding settings".to_string(),
        ));
    };
    group.fields.push(field);
    Ok(())
}
fn take_groups(builder: &SettingsBuilder) -> luau::runtime::Result<Vec<FieldGroupDefinition>> {
    let mut groups = builder.groups.lock().unwrap();
    if let Some(last) = groups.last()
        && last.fields.is_empty()
    {
        return Err(luau::Error::Runtime(format!(
            "settings group '{}' must declare at least one setting",
            last.id
        )));
    }
    Ok(std::mem::take(&mut *groups))
}
fn require_non_empty_string(value: String, field_name: &str) -> luau::runtime::Result<String> {
    let value = value.trim().to_string();
    if value.is_empty() {
        return Err(luau::Error::Runtime(format!(
            "{field_name} must not be empty"
        )));
    }
    Ok(value)
}
fn build_field_props(
    vm: &luau::Vm,
    props: &luau::Table,
    default_value: Option<serde_json::Value>,
) -> luau::runtime::Result<FieldProps> {
    Ok(FieldProps {
        label: require_non_empty_string(required_string(vm, props, "label")?, "label")?,
        description: optional_string(vm, props, "description")?,
        required: optional_bool(vm, props, "required")?.unwrap_or(false),
        default_value,
    })
}
fn required_string(vm: &luau::Vm, table: &luau::Table, key: &str) -> luau::runtime::Result<String> {
    match table.get_raw(vm, key)? {
        luau::Value::String(value) => {
            String::from_utf8(value).map_err(crate::plugins::runtime_error)
        }
        other => Err(luau::Error::Runtime(format!(
            "{key} must be a string, got {}",
            other.type_name()
        ))),
    }
}
fn optional_string(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Option<String>> {
    match table.get_raw(vm, key)? {
        luau::Value::Nil => Ok(None),
        luau::Value::String(value) => String::from_utf8(value)
            .map(Some)
            .map_err(crate::plugins::runtime_error),
        other => Err(luau::Error::Runtime(format!(
            "{key} must be a string or nil, got {}",
            other.type_name()
        ))),
    }
}
fn optional_bool(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Option<bool>> {
    match table.get_raw(vm, key)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Boolean(value) => Ok(Some(value)),
        other => Err(luau::Error::Runtime(format!(
            "{key} must be a boolean or nil, got {}",
            other.type_name()
        ))),
    }
}
fn optional_number(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Option<f64>> {
    match table.get_raw(vm, key)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Integer(value) => Ok(Some(value as f64)),
        luau::Value::Number(value) => Ok(Some(value)),
        other => Err(luau::Error::Runtime(format!(
            "{key} must be a number or nil, got {}",
            other.type_name()
        ))),
    }
}
fn parse_string_default(
    vm: &luau::Vm,
    props: &luau::Table,
) -> luau::runtime::Result<Option<serde_json::Value>> {
    match props.get_raw(vm, "default")? {
        luau::Value::Nil => Ok(None),
        luau::Value::String(value) => String::from_utf8(value)
            .map(serde_json::Value::String)
            .map(Some)
            .map_err(crate::plugins::runtime_error),
        _ => Err(luau::Error::Runtime(
            "default value must be a string or nil".to_string(),
        )),
    }
}
fn parse_number_default(
    vm: &luau::Vm,
    props: &luau::Table,
) -> luau::runtime::Result<Option<serde_json::Value>> {
    match props.get_raw(vm, "default")? {
        luau::Value::Nil => Ok(None),
        luau::Value::Integer(value) => Ok(Some(serde_json::json!(value))),
        luau::Value::Number(value) => Ok(Some(serde_json::json!(value))),
        _ => Err(luau::Error::Runtime(
            "default value must be a number or nil".to_string(),
        )),
    }
}
fn parse_bool_default(
    vm: &luau::Vm,
    props: &luau::Table,
) -> luau::runtime::Result<Option<serde_json::Value>> {
    match props.get_raw(vm, "default")? {
        luau::Value::Nil => Ok(None),
        luau::Value::Boolean(value) => Ok(Some(serde_json::Value::Bool(value))),
        _ => Err(luau::Error::Runtime(
            "default value must be a boolean or nil".to_string(),
        )),
    }
}
fn parse_choice_options(
    vm: &luau::Vm,
    props: &luau::Table,
) -> luau::runtime::Result<Vec<ChoiceOption>> {
    let luau::Value::Table(options_table) = props.get_raw(vm, "options")? else {
        return Err(luau::Error::Runtime(
            "choice settings require an options array".to_string(),
        ));
    };
    let mut entries = options_table
        .pairs_raw(vm)?
        .into_iter()
        .filter_map(|(key, value)| Some((sequence_index(key)?, value)))
        .collect::<Vec<_>>();
    entries.sort_by_key(|(index, _)| *index);

    let mut options = Vec::with_capacity(entries.len());
    let mut seen_values = HashSet::new();
    for (_, value) in entries {
        let luau::Value::Table(option_table) = value else {
            return Err(luau::Error::Runtime(
                "choice options must be tables".to_string(),
            ));
        };
        let value =
            require_non_empty_string(required_string(vm, &option_table, "value")?, "value")?;
        let label =
            require_non_empty_string(required_string(vm, &option_table, "label")?, "label")?;
        if !seen_values.insert(value.clone()) {
            return Err(luau::Error::Runtime(format!(
                "choice option value '{value}' is already declared"
            )));
        }
        options.push(ChoiceOption {
            value,
            label,
            description: optional_string(vm, &option_table, "description")?,
        });
    }

    if options.is_empty() {
        return Err(luau::Error::Runtime(
            "choice settings require at least one option".to_string(),
        ));
    }
    Ok(options)
}
fn parse_choice_default(
    vm: &luau::Vm,
    props: &luau::Table,
    options: &[ChoiceOption],
) -> luau::runtime::Result<Option<serde_json::Value>> {
    match props.get_raw(vm, "default")? {
        luau::Value::Nil => Ok(None),
        luau::Value::String(value) => {
            let value = String::from_utf8(value).map_err(crate::plugins::runtime_error)?;
            if !options.iter().any(|option| option.value == value) {
                return Err(luau::Error::Runtime(
                    "default value must match one of the declared choice options".to_string(),
                ));
            }
            Ok(Some(serde_json::Value::String(value)))
        }
        _ => Err(luau::Error::Runtime(
            "default value must be a string or nil".to_string(),
        )),
    }
}
fn primitive_value_for_field(
    field: &FieldDefinition,
    stored: Option<&serde_json::Value>,
) -> luau::runtime::Result<luau::Value> {
    if let Some(stored) = stored {
        field
            .validate_value(stored)
            .map_err(crate::plugins::runtime_error)?;
    }
    Ok(resolve_settings_value(stored, &field.props().default_value))
}
fn resolve_settings_value(
    stored: Option<&serde_json::Value>,
    default: &Option<serde_json::Value>,
) -> luau::Value {
    let value = stored
        .filter(|value| !value.is_null())
        .or_else(|| default.as_ref().filter(|value| !value.is_null()));

    match value {
        Some(serde_json::Value::Bool(value)) => luau::Value::Boolean(*value),
        Some(serde_json::Value::Number(value)) => {
            if let Some(value) = value.as_i64() {
                luau::Value::Integer(value)
            } else if let Some(value) = value.as_f64() {
                luau::Value::Number(value)
            } else {
                luau::Value::Nil
            }
        }
        Some(serde_json::Value::String(value)) => luau::Value::String(value.clone().into_bytes()),
        _ => luau::Value::Nil,
    }
}
fn build_config_table(
    groups: &[FieldGroupDefinition],
    builder: &SettingsBuilder,
) -> luau::runtime::Result<luau::OwnedTable> {
    let mut config = luau::OwnedTable::new();
    for group in groups {
        for field in &group.fields {
            let value = primitive_value_for_field(field, builder.stored_values.get(field.key()))?;
            config.set_field(field.key(), value);
        }
    }
    Ok(config)
}
fn sequence_index(value: luau::Value) -> Option<i64> {
    match value {
        luau::Value::Integer(value) if value > 0 => Some(value),
        luau::Value::Number(value) if value.is_finite() && value.fract() == 0.0 && value > 0.0 => {
            Some(value as i64)
        }
        _ => None,
    }
}

struct PluginsModule;

impl PluginsModule {}

pub(crate) fn module_spec() -> ModuleSpec {
    let spec = ModuleSpec::new("lyra/plugins")
        .capability("lyra.plugins")
        .function(plugin_id_spec())
        .function(plugin_manifest_spec())
        .function(plugin_list_spec())
        .function(plugin_get_spec())
        .install(|_| Ok(ModuleExport::new(PluginsModule)));
    let spec = spec
        .function(declare_settings_spec())
        .function(declare_user_settings_spec())
        .userdata(
            UserDataSpec::new("UserSettingsAccessor").method(user_settings_accessor_get_spec()),
        );
    spec
}
fn declare_settings_spec() -> FunctionSpec {
    let spec = FunctionSpec::async_fn("declare_settings")
        .context::<Option<Arc<str>>>()
        .named_arg::<SettingsCallback>("callback")
        .returns::<SettingsConfig>();
    spec.call(declare_settings_callback)
}
fn declare_user_settings_spec() -> FunctionSpec {
    let spec = FunctionSpec::async_fn("declare_user_settings")
        .context::<Option<Arc<str>>>()
        .named_arg::<SettingsCallback>("callback")
        .returns::<UserSettingsAccessor>();
    spec.call(declare_user_settings_callback)
}
fn user_settings_accessor_get_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get")
        .context::<Option<Arc<str>>>()
        .arg_name("user_id")
        .args::<i64>()
        .returns::<SettingsConfig>()
}

fn plugin_id_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("id").returns::<String>();
    spec.call(plugin_id_callback)
}
fn plugin_id_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let Some(plugin_id) = frame.context.origin.plugin.as_deref() else {
        return Err(luau::Error::Runtime(
            "plugins.id must be called from plugin Lua code".to_string(),
        ));
    };
    frame.returns.write(plugin_id)?;
    Ok(())
}

fn plugin_manifest_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("manifest").returns::<PluginManifest>();
    spec.call(plugin_manifest_callback)
}

fn plugin_list_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("list").returns::<Vec<PluginManifest>>();
    spec.call(plugin_list_callback)
}

fn plugin_get_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("get")
        .arg_name("id")
        .args::<String>()
        .returns::<Option<PluginManifest>>();
    spec.call(plugin_get_callback)
}
fn plugin_manifest_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let Some(plugin_id) = frame.context.origin.plugin.as_deref() else {
        return Err(luau::Error::Runtime(
            "plugins.manifest must be called from plugin Lua code".to_string(),
        ));
    };
    let manifests = frame.vm.data().get::<PluginManifestModuleStore>()?;
    let Some(manifest) = manifests.find(plugin_id) else {
        return Err(luau::Error::Runtime(format!(
            "plugin manifest not found: {plugin_id}"
        )));
    };
    frame.returns.write(manifest_table(manifest))?;
    Ok(())
}
fn plugin_list_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let manifests = frame.vm.data().get::<PluginManifestModuleStore>()?;
    let mut table = luau::OwnedTable::with_capacity(manifests.manifests.len(), 0);
    for manifest in manifests.iter() {
        table.push_array(luau::Value::TableData(manifest_table(manifest)));
    }
    frame.returns.write(table)?;
    Ok(())
}
fn plugin_get_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let id: String = frame.args.read_named("id")?;
    let id = id.trim();
    if id.is_empty() {
        return Err(luau::Error::Runtime("id must not be empty".to_string()));
    }

    let manifests = frame.vm.data().get::<PluginManifestModuleStore>()?;
    match manifests.find(id) {
        Some(manifest) => frame.returns.write(manifest_table(manifest))?,
        None => frame.returns.write(luau::Value::Nil)?,
    }
    Ok(())
}
fn manifest_table(manifest: &harmony_core::PluginManifest) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 6);
    table.set_field(
        "schema_version",
        luau::Value::Integer(i64::from(manifest.schema_version)),
    );
    table.set_field("id", luau::Value::String(manifest.id.clone().into_bytes()));
    table.set_field(
        "name",
        luau::Value::String(manifest.name.clone().into_bytes()),
    );
    table.set_field(
        "version",
        luau::Value::String(manifest.version.clone().into_bytes()),
    );
    table.set_field(
        "description",
        luau::Value::String(manifest.description.clone().into_bytes()),
    );
    table.set_field(
        "entrypoint",
        luau::Value::String(manifest.entrypoint.clone().into_bytes()),
    );
    table
}

impl LuauTypeInfo for PluginManifest {
    fn luau_type() -> LuauType {
        LuauType::literal("PluginManifest")
    }
}

impl DescribeInterface for PluginManifest {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("PluginManifest", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "schema_version",
                ty: u32::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "id",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "name",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "version",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "description",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "entrypoint",
                ty: String::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for SettingsChoiceOption {
    fn luau_type() -> LuauType {
        LuauType::literal("SettingsChoiceOption")
    }
}

impl DescribeInterface for SettingsChoiceOption {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("SettingsChoiceOption", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "value",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "label",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "description",
                ty: Option::<String>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

fn settings_common_fields() -> Vec<FieldDescriptor> {
    vec![
        FieldDescriptor {
            name: "label",
            ty: String::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "description",
            ty: Option::<String>::luau_type(),
            description: None,
        },
    ]
}

impl LuauTypeInfo for SettingsStringProps {
    fn luau_type() -> LuauType {
        LuauType::literal("SettingsStringProps")
    }
}

impl DescribeInterface for SettingsStringProps {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("SettingsStringProps", None);
        descriptor.fields.extend(settings_common_fields());
        descriptor.fields.extend([
            FieldDescriptor {
                name: "default",
                ty: Option::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "required",
                ty: Option::<bool>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for SettingsNumberProps {
    fn luau_type() -> LuauType {
        LuauType::literal("SettingsNumberProps")
    }
}

impl DescribeInterface for SettingsNumberProps {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("SettingsNumberProps", None);
        descriptor.fields.extend(settings_common_fields());
        descriptor.fields.extend([
            FieldDescriptor {
                name: "default",
                ty: Option::<f64>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "min",
                ty: Option::<f64>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "max",
                ty: Option::<f64>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "required",
                ty: Option::<bool>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for SettingsBoolProps {
    fn luau_type() -> LuauType {
        LuauType::literal("SettingsBoolProps")
    }
}

impl DescribeInterface for SettingsBoolProps {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("SettingsBoolProps", None);
        descriptor.fields.extend(settings_common_fields());
        descriptor.fields.extend([
            FieldDescriptor {
                name: "default",
                ty: Option::<bool>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "required",
                ty: Option::<bool>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for SettingsChoiceProps {
    fn luau_type() -> LuauType {
        LuauType::literal("SettingsChoiceProps")
    }
}

impl DescribeInterface for SettingsChoiceProps {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("SettingsChoiceProps", None);
        descriptor.fields.extend(settings_common_fields());
        descriptor.fields.extend([
            FieldDescriptor {
                name: "default",
                ty: Option::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "options",
                ty: Vec::<SettingsChoiceOption>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "required",
                ty: Option::<bool>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

fn param(name: &'static str, ty: LuauType) -> ParameterDescriptor {
    ParameterDescriptor {
        name,
        ty,
        description: None,
        variadic: false,
    }
}

fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "Plugins",
        local_name: "plugins",
        description: None,
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["declare_settings"],
                description: None,
                params: vec![param("callback", SettingsCallback::luau_type())],
                returns: vec![SettingsConfig::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["declare_user_settings"],
                description: None,
                params: vec![param("callback", SettingsCallback::luau_type())],
                returns: vec![UserSettingsAccessor::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["id"],
                description: None,
                params: vec![],
                returns: vec![String::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["manifest"],
                description: None,
                params: vec![],
                returns: vec![PluginManifest::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["list"],
                description: None,
                params: vec![],
                returns: vec![Vec::<PluginManifest>::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["get"],
                description: None,
                params: vec![param("id", String::luau_type())],
                returns: vec![Option::<PluginManifest>::luau_type()],
                yields: false,
            },
        ],
    }
}

pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &module_descriptor(),
        &[
            SettingsConfig::type_alias_descriptor(),
            SettingsCallback::type_alias_descriptor(),
        ],
        &[
            PluginManifest::interface_descriptor(),
            SettingsChoiceOption::interface_descriptor(),
            SettingsStringProps::interface_descriptor(),
            SettingsNumberProps::interface_descriptor(),
            SettingsBoolProps::interface_descriptor(),
            SettingsChoiceProps::interface_descriptor(),
        ],
        &[
            SettingsBuilder::class_descriptor(),
            UserSettingsAccessor::class_descriptor(),
        ],
    )
}
