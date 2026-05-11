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
    LuaAsyncExt,
    LuaFunctionAsyncExt,
    LuaUserDataAsyncExt,
};
use harmony_luau::{
    ClassDescriptor,
    DescribeTypeAlias,
    DescribeUserData,
    FunctionParameter,
    LuauType,
    LuauTypeInfo,
    MethodDescriptor,
    MethodKind,
    ParameterDescriptor,
    TypeAliasDescriptor,
};
use mlua::{
    Function,
    Lua,
    LuaSerdeExt,
    Result,
    Table,
    UserData,
    UserDataMethods,
    Value,
};
use serde::Serialize;

mod settings;

pub(crate) use self::settings::{
    ChoiceOption,
    FieldDefinition,
    FieldGroupDefinition,
    FieldProps,
    REGISTRY,
    Schema,
    SettingsScope,
    freeze_registry,
    initialize_registry,
    refreeze_plugin_settings,
    teardown_plugin_settings,
    unfreeze_plugin_settings,
};

use crate::{
    STATE,
    plugins::require_non_empty_string,
    services::plugin_settings as plugin_settings_service,
};

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

#[derive(Clone)]
struct UserSettingsAccessor {
    plugin_id: PluginId,
    schema: Schema,
}

impl UserSettingsAccessor {
    fn ensure_owner(&self, caller: Option<&PluginId>) -> Result<()> {
        match caller {
            Some(id) if id == &self.plugin_id => Ok(()),
            _ => Err(mlua::Error::runtime(format!(
                "user settings accessor for plugin '{}' must be used by the owning plugin",
                self.plugin_id
            ))),
        }
    }
}

#[harmony_macros::implementation(plugin_scoped)]
impl UserSettingsAccessor {
    #[harmony(returns(SettingsConfig))]
    pub(crate) async fn get(&self, plugin_id: Option<Arc<str>>, user_id: i64) -> Result<Table> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        self.ensure_owner(plugin_id.as_ref())?;
        let user_db_id = crate::plugins::require_positive_id(user_id, "user_id")?;

        let stored = plugin_settings_service::load_validated_user_stored_values(
            &*STATE.db.read().await,
            user_db_id,
            self.plugin_id.as_str(),
            &self.schema,
        )
        .map_err(mlua::Error::external)?;

        let lua = STATE.lua.get();
        let config = lua.create_table()?;
        for group in &self.schema.groups {
            for field in &group.fields {
                let key = field.key();
                let stored_value = stored.get(key);
                let default = &field.props().default_value;
                let value = resolve_value_with_lua(&lua, stored_value, default)?;
                config.set(key, value)?;
            }
        }

        Ok(config)
    }
}

harmony_macros::compile!(
    type_path = UserSettingsAccessor,
    fields = false,
    methods = true
);

#[harmony_macros::interface]
#[derive(Clone, Debug, Serialize)]
struct PluginManifest {
    schema_version: u32,
    id: String,
    name: String,
    version: String,
    description: String,
    entrypoint: String,
}

#[harmony_macros::interface]
struct SettingsChoiceOption {
    value: String,
    label: String,
    description: Option<String>,
}

#[harmony_macros::interface]
struct SettingsStringProps {
    label: String,
    description: Option<String>,
    default: Option<String>,
    required: Option<bool>,
}

#[harmony_macros::interface]
struct SettingsNumberProps {
    label: String,
    description: Option<String>,
    default: Option<f64>,
    min: Option<f64>,
    max: Option<f64>,
    required: Option<bool>,
}

#[harmony_macros::interface]
struct SettingsBoolProps {
    label: String,
    description: Option<String>,
    default: Option<bool>,
    required: Option<bool>,
}

#[harmony_macros::interface]
struct SettingsChoiceProps {
    label: String,
    description: Option<String>,
    default: Option<String>,
    options: Vec<SettingsChoiceOption>,
    required: Option<bool>,
}

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

impl SettingsBuilder {
    fn new(stored_values: HashMap<String, serde_json::Value>) -> Self {
        Self {
            groups: Arc::new(Mutex::new(Vec::new())),
            stored_values: Arc::new(stored_values),
        }
    }

    fn register_key(&self, key: String) -> Result<String> {
        let key = require_non_empty_string(key, "key")?;

        if self
            .groups
            .lock()
            .unwrap()
            .iter()
            .flat_map(|group| group.fields.iter())
            .any(|field| field.key() == key.as_str())
        {
            return Err(mlua::Error::runtime(format!(
                "setting key '{key}' is already declared"
            )));
        }

        Ok(key)
    }

    fn push_group(&self, id: String, label: String) -> Result<()> {
        let id = require_non_empty_string(id, "id")?;
        let label = require_non_empty_string(label, "label")?;

        let mut groups = self.groups.lock().unwrap();
        if let Some(previous) = groups.last()
            && previous.fields.is_empty()
        {
            return Err(mlua::Error::runtime(format!(
                "settings group '{}' must declare at least one setting",
                previous.id
            )));
        }
        if groups.iter().any(|group| group.id == id) {
            return Err(mlua::Error::runtime(format!(
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

    fn push_field(&self, field: FieldDefinition) -> Result<()> {
        let mut groups = self.groups.lock().unwrap();
        let Some(group) = groups.last_mut() else {
            return Err(mlua::Error::runtime(
                "declare a group before adding settings",
            ));
        };

        group.fields.push(field);
        Ok(())
    }

    fn current_value(&self, key: &str) -> Option<&serde_json::Value> {
        self.stored_values.get(key)
    }

    fn stored_values(&self) -> &HashMap<String, serde_json::Value> {
        &self.stored_values
    }

    fn take_groups(&self) -> Result<Vec<FieldGroupDefinition>> {
        let mut groups = self.groups.lock().unwrap();
        if let Some(last) = groups.last()
            && last.fields.is_empty()
        {
            return Err(mlua::Error::runtime(format!(
                "settings group '{}' must declare at least one setting",
                last.id
            )));
        }

        Ok(std::mem::take(&mut *groups))
    }
}

fn parse_field_metadata(props: &Table) -> Result<(String, Option<String>, bool)> {
    Ok((
        require_non_empty_string(props.get::<String>("label")?, "label")?,
        props.get::<Option<String>>("description")?,
        props.get::<Option<bool>>("required")?.unwrap_or(false),
    ))
}

fn build_field_props(
    props: &Table,
    default_value: Option<serde_json::Value>,
) -> Result<FieldProps> {
    let (label, description, required) = parse_field_metadata(props)?;
    Ok(FieldProps {
        label,
        description,
        required,
        default_value,
    })
}

fn parse_string_default(props: &Table) -> Result<Option<serde_json::Value>> {
    let value: Value = props.get("default")?;
    match value {
        Value::Nil => Ok(None),
        Value::String(s) => Ok(Some(serde_json::Value::String(s.to_str()?.to_string()))),
        _ => Err(mlua::Error::runtime(
            "default value must be a string or nil",
        )),
    }
}

fn parse_number_default(props: &Table) -> Result<Option<serde_json::Value>> {
    let value: Value = props.get("default")?;
    match value {
        Value::Nil => Ok(None),
        Value::Integer(i) => Ok(Some(serde_json::json!(i))),
        Value::Number(n) => Ok(Some(serde_json::json!(n))),
        _ => Err(mlua::Error::runtime(
            "default value must be a number or nil",
        )),
    }
}

fn parse_bool_default(props: &Table) -> Result<Option<serde_json::Value>> {
    let value: Value = props.get("default")?;
    match value {
        Value::Nil => Ok(None),
        Value::Boolean(b) => Ok(Some(serde_json::Value::Bool(b))),
        _ => Err(mlua::Error::runtime(
            "default value must be a boolean or nil",
        )),
    }
}

fn parse_choice_options(props: &Table) -> Result<Vec<ChoiceOption>> {
    let options_table: Table = props.get("options")?;
    let mut options = Vec::new();
    let mut seen_values = HashSet::new();

    for entry in options_table.sequence_values::<Table>() {
        let entry = entry?;
        let value = require_non_empty_string(entry.get::<String>("value")?, "value")?;
        let label = require_non_empty_string(entry.get::<String>("label")?, "label")?;
        if !seen_values.insert(value.clone()) {
            return Err(mlua::Error::runtime(format!(
                "choice option value '{value}' is already declared"
            )));
        }

        options.push(ChoiceOption {
            value,
            label,
            description: entry.get::<Option<String>>("description")?,
        });
    }

    if options.is_empty() {
        return Err(mlua::Error::runtime(
            "choice settings require at least one option",
        ));
    }

    Ok(options)
}

fn parse_choice_default(
    props: &Table,
    options: &[ChoiceOption],
) -> Result<Option<serde_json::Value>> {
    let value: Value = props.get("default")?;
    match value {
        Value::Nil => Ok(None),
        Value::String(s) => {
            let value = s.to_str()?.to_string();
            if !options.iter().any(|option| option.value == value) {
                return Err(mlua::Error::runtime(
                    "default value must match one of the declared choice options",
                ));
            }
            Ok(Some(serde_json::Value::String(value)))
        }
        _ => Err(mlua::Error::runtime(
            "default value must be a string or nil",
        )),
    }
}

fn resolve_value_with_lua(
    lua: &Lua,
    stored: Option<&serde_json::Value>,
    default: &Option<serde_json::Value>,
) -> Result<Value> {
    let value = stored
        .filter(|value| !value.is_null())
        .or_else(|| default.as_ref().filter(|value| !value.is_null()));

    match value {
        Some(serde_json::Value::Bool(b)) => Ok(Value::Boolean(*b)),
        Some(serde_json::Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Integer(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Number(f))
            } else {
                Ok(Value::Nil)
            }
        }
        Some(serde_json::Value::String(s)) => Ok(Value::String(lua.create_string(s)?)),
        _ => Ok(Value::Nil),
    }
}

fn validate_stored_field_value(
    field: &FieldDefinition,
    stored: Option<&serde_json::Value>,
) -> Result<()> {
    if let Some(stored) = stored {
        field
            .validate_value(stored)
            .map_err(|error| mlua::Error::runtime(error.to_string()))?;
    }
    Ok(())
}

fn primitive_value_for_field(
    lua: &Lua,
    field: &FieldDefinition,
    stored: Option<&serde_json::Value>,
) -> Result<Value> {
    validate_stored_field_value(field, stored)?;
    let default = &field.props().default_value;
    resolve_value_with_lua(lua, stored, default)
}

impl UserData for SettingsBuilder {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("group", |_, this, (id, label): (String, String)| {
            this.push_group(id, label)
        });

        methods.add_method("string", |lua, this, (key, props): (String, Table)| {
            let key = this.register_key(key)?;
            let field = FieldDefinition::String {
                key,
                props: build_field_props(&props, parse_string_default(&props)?)?,
            };
            let stored = this.current_value(field.key());
            let value = primitive_value_for_field(lua, &field, stored)?;
            this.push_field(field)?;
            Ok(value)
        });

        methods.add_method("number", |lua, this, (key, props): (String, Table)| {
            let key = this.register_key(key)?;
            let min = props.get::<Option<f64>>("min")?;
            let max = props.get::<Option<f64>>("max")?;
            if let (Some(min), Some(max)) = (min, max)
                && min > max
            {
                return Err(mlua::Error::runtime(
                    "min must be less than or equal to max",
                ));
            }

            let default_value = parse_number_default(&props)?;
            if let Some(default) = default_value.as_ref().and_then(|value| value.as_f64()) {
                if let Some(min) = min
                    && default < min
                {
                    return Err(mlua::Error::runtime(
                        "default value must be greater than or equal to min",
                    ));
                }
                if let Some(max) = max
                    && default > max
                {
                    return Err(mlua::Error::runtime(
                        "default value must be less than or equal to max",
                    ));
                }
            }

            let field = FieldDefinition::Number {
                key,
                props: build_field_props(&props, default_value)?,
                min,
                max,
            };
            let stored = this.current_value(field.key());
            let value = primitive_value_for_field(lua, &field, stored)?;
            this.push_field(field)?;
            Ok(value)
        });

        methods.add_method("bool", |lua, this, (key, props): (String, Table)| {
            let key = this.register_key(key)?;
            let field = FieldDefinition::Bool {
                key,
                props: build_field_props(&props, parse_bool_default(&props)?)?,
            };
            let stored = this.current_value(field.key());
            let value = primitive_value_for_field(lua, &field, stored)?;
            this.push_field(field)?;
            Ok(value)
        });

        methods.add_method("choice", |lua, this, (key, props): (String, Table)| {
            let key = this.register_key(key)?;
            let options = parse_choice_options(&props)?;
            let field = FieldDefinition::Choice {
                key,
                props: build_field_props(&props, parse_choice_default(&props, &options)?)?,
                options,
            };
            let stored = this.current_value(field.key());
            let value = primitive_value_for_field(lua, &field, stored)?;
            this.push_field(field)?;
            Ok(value)
        });
    }
}

fn build_config_table(
    lua: &Lua,
    groups: &[FieldGroupDefinition],
    builder: &SettingsBuilder,
) -> Result<Table> {
    let config = lua.create_table()?;

    for group in groups {
        for field in &group.fields {
            let key = field.key();
            let stored = builder.current_value(key);
            let value = primitive_value_for_field(lua, field, stored)?;
            config.set(key, value)?;
        }
    }

    Ok(config)
}

fn project_plugin_manifest(manifest: &harmony_core::PluginManifest) -> PluginManifest {
    PluginManifest {
        schema_version: manifest.schema_version,
        id: manifest.id.clone(),
        name: manifest.name.clone(),
        version: manifest.version.clone(),
        description: manifest.description.clone(),
        entrypoint: manifest.entrypoint.clone(),
    }
}

pub(crate) fn current_plugin_id(lua: &Lua) -> Result<PluginId> {
    crate::plugins::lifecycle::resolve_caller_plugin_id(lua)
        .ok_or_else(|| mlua::Error::runtime("plugins.* must be called from plugin Lua code"))
}

fn current_plugin_manifest(lua: &Lua) -> Result<PluginManifest> {
    let plugin_id = current_plugin_id(lua)?;
    let manifests = STATE.plugin_manifests.get();
    manifests
        .iter()
        .find(|manifest| manifest.id == plugin_id.as_str())
        .map(project_plugin_manifest)
        .ok_or_else(|| mlua::Error::runtime(format!("plugin manifest not found: {plugin_id}")))
}

async fn declare_settings_impl(plugin_id: PluginId, callback: Function) -> Result<Table> {
    let _registration = STATE
        .plugin_registries
        .ensure_registrations_open(&plugin_id)
        .await?;

    let stored_values =
        plugin_settings_service::load_stored_values(&*STATE.db.read().await, plugin_id.as_str())
            .map_err(mlua::Error::external)?;

    let builder = SettingsBuilder::new(stored_values);
    let builder_ref = builder.clone();

    callback.call_async::<()>(builder_ref).await?;

    let groups = builder.take_groups()?;
    let schema = Schema { groups };
    plugin_settings_service::validate_stored_values(
        plugin_id.as_str(),
        &schema,
        builder.stored_values(),
    )
    .map_err(mlua::Error::external)?;

    let lua = STATE.lua.get();
    let config = build_config_table(&lua, &schema.groups, &builder)?;

    REGISTRY
        .write()
        .await
        .register_schema(plugin_id, settings::SettingsScope::Global, schema)
        .map_err(mlua::Error::external)?;

    Ok(config)
}

async fn declare_user_settings_impl(
    plugin_id: PluginId,
    callback: Function,
) -> Result<UserSettingsAccessor> {
    let _registration = STATE
        .plugin_registries
        .ensure_registrations_open(&plugin_id)
        .await?;

    let builder = SettingsBuilder::new(HashMap::new());
    let builder_ref = builder.clone();

    callback.call_async::<()>(builder_ref).await?;

    let groups = builder.take_groups()?;
    let schema = Schema { groups };

    REGISTRY
        .write()
        .await
        .register_schema(
            plugin_id.clone(),
            settings::SettingsScope::User,
            schema.clone(),
        )
        .map_err(mlua::Error::external)?;

    Ok(UserSettingsAccessor { plugin_id, schema })
}

struct PluginsModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "Plugins",
    local = "plugins",
    path = "lyra/plugins",
    aliases(SettingsConfig, SettingsCallback),
    interfaces(
        PluginManifest,
        SettingsChoiceOption,
        SettingsStringProps,
        SettingsNumberProps,
        SettingsBoolProps,
        SettingsChoiceProps
    ),
    classes(SettingsBuilder, UserSettingsAccessor)
)]
impl PluginsModule {
    #[harmony(args(callback: SettingsCallback), returns(SettingsConfig))]
    pub(crate) async fn declare_settings(
        plugin_id: Option<Arc<str>>,
        callback: Function,
    ) -> Result<Table> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        let plugin_id = plugin_id.ok_or_else(|| {
            mlua::Error::runtime("plugins.declare_settings must be called from plugin Lua code")
        })?;
        declare_settings_impl(plugin_id, callback).await
    }

    #[harmony(args(callback: SettingsCallback), returns(UserSettingsAccessor))]
    pub(crate) async fn declare_user_settings(
        plugin_id: Option<Arc<str>>,
        callback: Function,
    ) -> Result<UserSettingsAccessor> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        let plugin_id = plugin_id.ok_or_else(|| {
            mlua::Error::runtime(
                "plugins.declare_user_settings must be called from plugin Lua code",
            )
        })?;
        declare_user_settings_impl(plugin_id, callback).await
    }

    pub(crate) fn id(lua: &Lua, _: ()) -> Result<String> {
        Ok(current_plugin_id(&lua)?.to_string())
    }

    #[harmony(returns(PluginManifest))]
    pub(crate) fn manifest(lua: &Lua, _: ()) -> Result<Value> {
        let manifest = current_plugin_manifest(lua)?;
        lua.to_value_with(&manifest, crate::plugins::LUA_SERIALIZE_OPTIONS)
    }

    #[harmony(returns(Vec<PluginManifest>))]
    pub(crate) fn list(lua: &Lua, _: ()) -> Result<Value> {
        let manifests = STATE.plugin_manifests.get();
        let manifests = manifests
            .iter()
            .map(project_plugin_manifest)
            .collect::<Vec<_>>();
        lua.to_value_with(&manifests, crate::plugins::LUA_SERIALIZE_OPTIONS)
    }

    #[harmony(returns(Option<PluginManifest>))]
    pub(crate) fn get(lua: &Lua, id: String) -> Result<Value> {
        let id = require_non_empty_string(id, "id")?;
        let manifests = STATE.plugin_manifests.get();
        let manifest = manifests
            .iter()
            .find(|manifest| manifest.id == id)
            .map(project_plugin_manifest);

        match manifest {
            Some(manifest) => lua.to_value_with(&manifest, crate::plugins::LUA_SERIALIZE_OPTIONS),
            None => Ok(Value::Nil),
        }
    }
}

crate::plugins::plugin_surface_exports!(
    PluginsModule,
    "lyra.plugins",
    "Manage installed plugins (list, install, restart, uninstall).",
    High
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_choice_options_reads_named_fields() -> anyhow::Result<()> {
        let lua = Lua::new();
        let props = lua.create_table()?;
        let options = lua.create_table()?;
        let option = lua.create_table()?;
        option.set("value", "spotify")?;
        option.set("label", "Spotify")?;
        option.set("description", "Streaming service")?;
        options.set(1, option)?;
        props.set("options", options)?;

        let parsed = parse_choice_options(&props)?;
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].value, "spotify");
        assert_eq!(parsed[0].label, "Spotify");
        assert_eq!(parsed[0].description.as_deref(), Some("Streaming service"));

        Ok(())
    }

    #[test]
    fn parse_number_default_rejects_non_numbers() -> anyhow::Result<()> {
        let lua = Lua::new();
        let props = lua.create_table()?;
        props.set("default", "loud")?;

        let error = parse_number_default(&props).unwrap_err();
        assert!(error.to_string().contains("number or nil"));

        Ok(())
    }

    #[test]
    fn parse_choice_default_requires_declared_option() -> anyhow::Result<()> {
        let lua = Lua::new();
        let props = lua.create_table()?;
        props.set("default", "maybe")?;

        let error = parse_choice_default(
            &props,
            &[ChoiceOption {
                value: "yes".to_string(),
                label: "Yes".to_string(),
                description: None,
            }],
        )
        .unwrap_err();
        assert!(error.to_string().contains("declared choice options"));

        Ok(())
    }

    #[test]
    fn register_key_rejects_duplicate_setting_keys() -> anyhow::Result<()> {
        let builder = SettingsBuilder::new(HashMap::new());
        builder.push_group("credentials".to_string(), "Credentials".to_string())?;
        builder.push_field(FieldDefinition::String {
            key: "token".to_string(),
            props: FieldProps {
                label: "Token".to_string(),
                description: None,
                required: false,
                default_value: None,
            },
        })?;

        let error = builder.register_key("token".to_string()).unwrap_err();
        assert!(error.to_string().contains("already declared"));

        Ok(())
    }

    #[test]
    fn push_field_requires_group() {
        let builder = SettingsBuilder::new(HashMap::new());

        let error = builder
            .push_field(FieldDefinition::String {
                key: "token".to_string(),
                props: FieldProps {
                    label: "Token".to_string(),
                    description: None,
                    required: false,
                    default_value: None,
                },
            })
            .unwrap_err();
        assert!(error.to_string().contains("declare a group"));
    }

    #[test]
    fn push_group_rejects_empty_previous_group() -> anyhow::Result<()> {
        let builder = SettingsBuilder::new(HashMap::new());
        builder.push_group("credentials".to_string(), "Credentials".to_string())?;

        let error = builder
            .push_group("advanced".to_string(), "Advanced".to_string())
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("must declare at least one setting")
        );

        Ok(())
    }

    #[test]
    fn build_config_table_keeps_missing_bool_as_nil() -> anyhow::Result<()> {
        let lua = Lua::new();
        let builder = SettingsBuilder::new(HashMap::new());
        let groups = vec![FieldGroupDefinition {
            id: "general".to_string(),
            label: "General".to_string(),
            fields: vec![FieldDefinition::Bool {
                key: "enabled".to_string(),
                props: FieldProps {
                    label: "Enabled".to_string(),
                    description: None,
                    required: false,
                    default_value: None,
                },
            }],
        }];

        let config = build_config_table(&lua, &groups, &builder)?;
        let value: Value = config.get("enabled")?;
        assert!(matches!(value, Value::Nil));

        Ok(())
    }

    #[test]
    fn build_config_table_uses_default_when_stored_value_is_null() -> anyhow::Result<()> {
        let lua = Lua::new();
        let builder = SettingsBuilder::new(HashMap::from([(
            "token".to_string(),
            serde_json::Value::Null,
        )]));
        let groups = vec![FieldGroupDefinition {
            id: "credentials".to_string(),
            label: "Credentials".to_string(),
            fields: vec![FieldDefinition::String {
                key: "token".to_string(),
                props: FieldProps {
                    label: "Token".to_string(),
                    description: None,
                    required: false,
                    default_value: Some(serde_json::json!("fallback")),
                },
            }],
        }];

        let config = build_config_table(&lua, &groups, &builder)?;
        let value: String = config.get("token")?;
        assert_eq!(value, "fallback");

        Ok(())
    }

    #[test]
    fn current_plugin_id_reads_calling_plugin_source() -> anyhow::Result<()> {
        let lua = Lua::new();
        let plugin_id = lua.create_function(|lua, ()| Ok(current_plugin_id(lua)?.to_string()))?;
        lua.globals().set("plugin_id", plugin_id)?;

        let current: String = lua
            .load("return plugin_id()")
            .set_name(&harmony_core::format_plugin_chunk_name("demo", "init"))
            .eval()?;

        assert_eq!(current, "demo");

        Ok(())
    }

    #[test]
    fn current_plugin_id_rejects_non_plugin_callers() -> anyhow::Result<()> {
        let lua = Lua::new();
        let plugin_id = lua.create_function(|lua, ()| Ok(current_plugin_id(lua)?.to_string()))?;
        lua.globals().set("plugin_id", plugin_id)?;

        let error = lua
            .load("return plugin_id()")
            .set_name("scratch")
            .eval::<String>()
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("must be called from plugin Lua code")
        );

        Ok(())
    }
}
