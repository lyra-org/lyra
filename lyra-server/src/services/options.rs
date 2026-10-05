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

/// The options passed for `provider_id`, keyed by declared name. Callers pass
/// options as `<provider_id>.<name>`, so each provider gets only its own.
pub(crate) fn provider_options(
    provider_id: &str,
    declared: &[OptionDeclaration],
    passed: &std::collections::HashMap<std::string::String, std::string::String>,
) -> serde_json::Map<std::string::String, serde_json::Value> {
    declared
        .iter()
        .filter_map(|decl| {
            let raw = passed.get(&format!("{provider_id}.{}", decl.name))?;
            Some((
                decl.name.clone(),
                coerce_option_value(raw, &decl.option_type),
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn declaration(name: &str, option_type: OptionType) -> OptionDeclaration {
        OptionDeclaration {
            name: name.to_string(),
            label: name.to_string(),
            option_type,
            default: serde_json::Value::Null,
            requires_settings: Vec::new(),
        }
    }

    fn passed(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn provider_options_takes_only_the_providers_namespaced_options() {
        let declared = [
            declaration("fingerprint", OptionType::Boolean),
            declaration("limit", OptionType::Number),
        ];
        let options = provider_options(
            "musicbrainz",
            &declared,
            &passed(&[
                ("musicbrainz.fingerprint", "true"),
                ("musicbrainz.limit", "3"),
                ("theaudiodb.fingerprint", "false"),
                ("fingerprint", "false"),
                ("musicbrainz.undeclared", "true"),
            ]),
        );

        assert_eq!(options.len(), 2);
        assert_eq!(options["fingerprint"], serde_json::json!(true));
        assert_eq!(options["limit"], serde_json::json!(3.0));
    }

    #[test]
    fn provider_options_is_empty_without_the_providers_options() {
        let declared = [declaration("fingerprint", OptionType::Boolean)];
        let options = provider_options(
            "musicbrainz",
            &declared,
            &passed(&[("theaudiodb.fingerprint", "true")]),
        );

        assert!(options.is_empty());
    }
}
