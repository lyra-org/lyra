// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub(crate) mod descriptors;
mod luau;
mod registry;
mod schema;

#[cfg(test)]
mod tests;

pub(crate) use self::luau::{
    PluginSettingsModuleStore,
    declare_settings_spec,
    declare_user_settings_spec,
    user_settings_accessor_spec,
};
pub(crate) use self::registry::{
    Registry,
    SettingsRegistries,
    SettingsScope,
    freeze_registry,
    initialize_registry,
    refreeze_plugin_settings,
    settings_registry,
    teardown_plugin_settings,
    unfreeze_plugin_settings,
};
pub(crate) use self::schema::{
    ChoiceOption,
    FieldDefinition,
    FieldGroupDefinition,
    FieldProps,
    Schema,
};
