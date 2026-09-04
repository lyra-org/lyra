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

use harmony_core::FunctionSpec;
use harmony_luau as luau;

use super::{
    ChoiceOption,
    FieldDefinition,
    FieldGroupDefinition,
    FieldProps,
    Schema,
    SettingsScope,
    descriptors::{
        SettingsBoolProps,
        SettingsCallback,
        SettingsChoiceProps,
        SettingsConfig,
        SettingsNumberProps,
        SettingsStringProps,
    },
};
use crate::plugins::db::DbAsync;
use crate::services::settings::plugins as plugin_settings_service;

#[harmony_macros::userdata(
    name = "SettingsBuilder",
    description = "Builder for declaring plugin settings."
)]
#[derive(Clone)]
pub(super) struct SettingsBuilder {
    groups: Arc<Mutex<Vec<FieldGroupDefinition>>>,
    stored_values: Arc<HashMap<String, serde_json::Value>>,
}

#[harmony_macros::userdata_methods]
impl SettingsBuilder {
    #[harmony(
        description = "Starts a settings group.",
        args(id: String, label: String)
    )]
    fn group(&self, id: String, label: String) -> luau::runtime::Result<()> {
        push_group(self, id, label)
    }

    #[harmony(
        description = "Declares a string setting.",
        args(key: String, props: SettingsStringProps),
        returns(Option<String>)
    )]
    fn string(
        &self,
        vm: &luau::Vm,
        key: String,
        props: luau::Table,
    ) -> luau::runtime::Result<luau::Value> {
        let key = register_key(self, key)?;
        let field = FieldDefinition::String {
            key,
            props: build_field_props(vm, &props, parse_string_default(vm, &props)?)?,
        };
        let value = primitive_value_for_field(&field, self.stored_values.get(field.key()))?;
        push_field(self, field)?;
        Ok(value)
    }

    #[harmony(
        description = "Declares a number setting.",
        args(key: String, props: SettingsNumberProps),
        returns(Option<f64>)
    )]
    fn number(
        &self,
        vm: &luau::Vm,
        key: String,
        props: luau::Table,
    ) -> luau::runtime::Result<luau::Value> {
        let key = register_key(self, key)?;
        let min = optional_number(vm, &props, "min")?;
        let max = optional_number(vm, &props, "max")?;
        if let (Some(min), Some(max)) = (min, max)
            && min > max
        {
            return Err(luau::Error::Runtime(
                "min must be less than or equal to max".to_string(),
            ));
        }
        let default_value = parse_number_default(vm, &props)?;
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
            props: build_field_props(vm, &props, default_value)?,
            min,
            max,
        };
        let value = primitive_value_for_field(&field, self.stored_values.get(field.key()))?;
        push_field(self, field)?;
        Ok(value)
    }

    #[harmony(
        description = "Declares a boolean setting.",
        args(key: String, props: SettingsBoolProps),
        returns(Option<bool>)
    )]
    fn bool(
        &self,
        vm: &luau::Vm,
        key: String,
        props: luau::Table,
    ) -> luau::runtime::Result<luau::Value> {
        let key = register_key(self, key)?;
        let field = FieldDefinition::Bool {
            key,
            props: build_field_props(vm, &props, parse_bool_default(vm, &props)?)?,
        };
        let value = primitive_value_for_field(&field, self.stored_values.get(field.key()))?;
        push_field(self, field)?;
        Ok(value)
    }

    #[harmony(
        description = "Declares a single-choice setting.",
        args(key: String, props: SettingsChoiceProps),
        returns(Option<String>)
    )]
    fn choice(
        &self,
        vm: &luau::Vm,
        key: String,
        props: luau::Table,
    ) -> luau::runtime::Result<luau::Value> {
        let key = register_key(self, key)?;
        let options = parse_choice_options(vm, &props)?;
        let default_value = parse_choice_default(vm, &props, &options)?;
        let field = FieldDefinition::Choice {
            key,
            props: build_field_props(vm, &props, default_value)?,
            options,
        };
        let value = primitive_value_for_field(&field, self.stored_values.get(field.key()))?;
        push_field(self, field)?;
        Ok(value)
    }
}

#[harmony_macros::userdata(name = "UserSettingsAccessor")]
#[derive(Clone)]
pub(super) struct UserSettingsAccessor {
    store: PluginSettingsModuleStore,
    plugin_id: PluginId,
    schema: Schema,
}

#[harmony_macros::userdata_methods]
impl UserSettingsAccessor {
    #[harmony(
        description = "Returns validated settings for a user.",
        returns(SettingsConfig)
    )]
    fn get(
        &self,
        context: &luau::CallContext,
        user_id: i64,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let caller = current_plugin_id(context)?;
        if caller != self.plugin_id {
            return Err(luau::Error::Runtime(format!(
                "user settings accessor for plugin '{}' must be used by the owning plugin",
                self.plugin_id
            )));
        }
        if user_id <= 0 {
            return Err(luau::Error::Runtime("user_id must be positive".to_string()));
        }
        let stored = self.store.load_user_stored_values(
            agdb::DbId(user_id),
            &self.plugin_id,
            &self.schema,
        )?;
        let builder = settings_builder(stored);
        build_config_table(&self.schema.groups, &builder)
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
    let builder_value = SettingsBuilder::_harmony_userdata_class().create_value(
        frame.vm,
        &frame.context.origin,
        builder.clone(),
    )?;

    callback.call(frame.vm, &[builder_value])?;

    let groups = take_groups(&builder)?;
    let schema = Schema { groups };
    plugin_settings_service::validate_stored_values(
        plugin_id.as_str(),
        &schema,
        &builder.stored_values,
    )
    .map_err(crate::plugins::runtime_error)?;
    let config = build_config_table(&schema.groups, &builder)?;

    futures::executor::block_on(super::settings_registry().write_owned())
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
    let builder_value = SettingsBuilder::_harmony_userdata_class().create_value(
        frame.vm,
        &frame.context.origin,
        builder.clone(),
    )?;

    callback.call(frame.vm, &[builder_value])?;

    let groups = take_groups(&builder)?;
    let schema = Schema { groups };

    futures::executor::block_on(super::settings_registry().write_owned())
        .register_schema(plugin_id.clone(), SettingsScope::User, schema.clone())
        .map_err(crate::plugins::runtime_error)?;

    frame
        .returns
        .write(UserSettingsAccessor::_harmony_userdata_class().create(
            frame.vm,
            &frame.context.origin,
            UserSettingsAccessor {
                store,
                plugin_id,
                schema,
            },
        )?)?;
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

pub(crate) fn declare_settings_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("declare_settings")
        .context::<Option<Arc<str>>>()
        .named_arg::<SettingsCallback>("callback")
        .returns::<SettingsConfig>();
    spec.call(declare_settings_callback)
}

pub(crate) fn declare_user_settings_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("declare_user_settings")
        .context::<Option<Arc<str>>>()
        .named_arg::<SettingsCallback>("callback")
        .returns::<UserSettingsAccessor>();
    spec.call(declare_user_settings_callback)
}

pub(crate) fn user_settings_accessor_spec() -> harmony_core::UserDataSpec {
    UserSettingsAccessor::_harmony_userdata_spec()
}
