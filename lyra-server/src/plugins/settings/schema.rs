// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use anyhow::bail;

#[derive(Clone, Debug)]
pub(crate) struct Schema {
    pub(crate) groups: Vec<FieldGroupDefinition>,
}

#[derive(Clone, Debug)]
pub(crate) struct FieldGroupDefinition {
    pub(crate) id: String,
    pub(crate) label: String,
    pub(crate) fields: Vec<FieldDefinition>,
}

#[derive(Clone, Debug)]
pub(crate) struct ChoiceOption {
    pub(crate) value: String,
    pub(crate) label: String,
    pub(crate) description: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct FieldProps {
    pub(crate) label: String,
    pub(crate) description: Option<String>,
    pub(crate) required: bool,
    pub(crate) default_value: Option<serde_json::Value>,
}

#[derive(Clone, Debug)]
pub(crate) enum FieldDefinition {
    String {
        key: String,
        props: FieldProps,
    },
    Number {
        key: String,
        props: FieldProps,
        min: Option<f64>,
        max: Option<f64>,
    },
    Bool {
        key: String,
        props: FieldProps,
    },
    Choice {
        key: String,
        props: FieldProps,
        options: Vec<ChoiceOption>,
    },
}

impl FieldDefinition {
    pub(crate) fn key(&self) -> &str {
        match self {
            Self::String { key, .. }
            | Self::Number { key, .. }
            | Self::Bool { key, .. }
            | Self::Choice { key, .. } => key,
        }
    }

    pub(crate) fn props(&self) -> &FieldProps {
        match self {
            Self::String { props, .. }
            | Self::Number { props, .. }
            | Self::Bool { props, .. }
            | Self::Choice { props, .. } => props,
        }
    }

    pub(crate) fn validate_value(&self, value: &serde_json::Value) -> anyhow::Result<()> {
        match self {
            Self::String { key, props } => validate_string_value(key, props, value),
            Self::Number {
                key,
                props,
                min,
                max,
            } => validate_number_value(key, props, value, *min, *max),
            Self::Bool { key, props } => validate_bool_value(key, props, value),
            Self::Choice {
                key,
                props,
                options,
            } => validate_choice_value(key, props, options, value),
        }
    }
}

impl Schema {
    pub(crate) fn field(&self, key: &str) -> Option<&FieldDefinition> {
        self.groups
            .iter()
            .flat_map(|group| group.fields.iter())
            .find(|field| field.key() == key)
    }
}

fn validate_nullable_value(
    key: &str,
    props: &FieldProps,
    value: &serde_json::Value,
) -> anyhow::Result<bool> {
    if value.is_null() {
        if props.required && props.default_value.is_none() {
            bail!("setting '{key}' is required");
        }
        return Ok(true);
    }

    Ok(false)
}

fn validate_string_value(
    key: &str,
    props: &FieldProps,
    value: &serde_json::Value,
) -> anyhow::Result<()> {
    if validate_nullable_value(key, props, value)? {
        return Ok(());
    }
    if !value.is_string() {
        bail!("setting '{key}' must be a string or null");
    }
    Ok(())
}

fn validate_number_value(
    key: &str,
    props: &FieldProps,
    value: &serde_json::Value,
    min: Option<f64>,
    max: Option<f64>,
) -> anyhow::Result<()> {
    if validate_nullable_value(key, props, value)? {
        return Ok(());
    }

    let Some(number) = value.as_f64() else {
        bail!("setting '{key}' must be a number or null");
    };

    if let Some(min) = min
        && number < min
    {
        bail!("setting '{key}' must be greater than or equal to {min}");
    }
    if let Some(max) = max
        && number > max
    {
        bail!("setting '{key}' must be less than or equal to {max}");
    }

    Ok(())
}

fn validate_bool_value(
    key: &str,
    props: &FieldProps,
    value: &serde_json::Value,
) -> anyhow::Result<()> {
    if validate_nullable_value(key, props, value)? {
        return Ok(());
    }
    if value.is_boolean() {
        return Ok(());
    }
    bail!("setting '{key}' must be a boolean or null");
}

fn validate_choice_value(
    key: &str,
    props: &FieldProps,
    options: &[ChoiceOption],
    value: &serde_json::Value,
) -> anyhow::Result<()> {
    if validate_nullable_value(key, props, value)? {
        return Ok(());
    }

    let Some(choice) = value.as_str() else {
        bail!("setting '{key}' must be a string or null");
    };

    if options.iter().any(|option| option.value == choice) {
        return Ok(());
    }

    bail!(
        "setting '{key}' must be one of: {}",
        options
            .iter()
            .map(|option| option.value.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
}
