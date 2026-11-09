// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub(crate) mod descriptors;
mod luau;
mod registry;

#[cfg(test)]
mod tests;

pub(crate) use self::luau::{
    SettingsBuilder,
    UserSettingsAccessor,
    declare_settings,
    declare_user_settings,
};
#[cfg(test)]
pub(crate) use self::registry::initialize_registry;
pub(crate) use self::registry::{
    Registry,
    SettingsRegistries,
    SettingsScope,
    freeze_registry,
    settings_registry,
};
pub(crate) use crate::services::settings::{
    ChoiceOption,
    FieldDefinition,
    FieldGroupDefinition,
    FieldProps,
    Schema,
};
