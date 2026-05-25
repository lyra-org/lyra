// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[derive(Clone, Debug)]
pub(crate) enum OptionType {
    Boolean,
    String,
    Number,
}

#[derive(Clone, Debug)]
pub(crate) struct OptionDeclaration {
    pub(crate) name: std::string::String,
    pub(crate) label: std::string::String,
    pub(crate) option_type: OptionType,
    pub(crate) default: serde_json::Value,
    pub(crate) requires_settings: Vec<std::string::String>,
}

/// Coerces a raw query string value to the declared option type.
pub(crate) fn coerce_option_value(raw: &str, option_type: &OptionType) -> serde_json::Value {
    match option_type {
        OptionType::Boolean => serde_json::Value::Bool(
            raw.eq_ignore_ascii_case("true") || raw == "1" || raw.eq_ignore_ascii_case("yes"),
        ),
        OptionType::Number => raw
            .parse::<f64>()
            .ok()
            .filter(|n| n.is_finite())
            .map(|n| serde_json::json!(n))
            .unwrap_or(serde_json::Value::Null),
        OptionType::String => serde_json::Value::String(raw.to_string()),
    }
}
