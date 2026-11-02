// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[derive(Clone, Debug)]
pub(crate) enum OptionType {
    Boolean,
    String,
    Number,
    /// A string, one of the listed values.
    Choice(Vec<OptionChoice>),
}

#[derive(Clone, Debug)]
pub(crate) struct OptionChoice {
    pub(crate) value: std::string::String,
    pub(crate) label: std::string::String,
}

impl OptionType {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            OptionType::Boolean => "boolean",
            OptionType::String => "string",
            OptionType::Number => "number",
            OptionType::Choice(_) => "choice",
        }
    }
}

/// Checks that `value` is one of `choices`, naming `subject` and the accepted values when it isn't.
pub(crate) fn check_choice(
    subject: &str,
    choices: &[OptionChoice],
    value: &str,
) -> Result<(), std::string::String> {
    if choices.iter().any(|choice| choice.value == value) {
        return Ok(());
    }
    let accepted = choices
        .iter()
        .map(|choice| format!("'{}'", choice.value))
        .collect::<Vec<_>>()
        .join(", ");
    Err(format!("{subject} must be one of {accepted}"))
}

#[derive(Clone, Debug)]
pub(crate) struct OptionDeclaration {
    pub(crate) name: std::string::String,
    pub(crate) label: std::string::String,
    pub(crate) option_type: OptionType,
    pub(crate) default: serde_json::Value,
    pub(crate) requires_settings: Vec<std::string::String>,
}

/// Coerces a raw query string value to the declared option type; a choice outside its list fails.
fn coerce_option_value(
    key: &str,
    raw: &str,
    option_type: &OptionType,
) -> Result<serde_json::Value, std::string::String> {
    Ok(match option_type {
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
        OptionType::Choice(choices) => {
            check_choice(&format!("option '{key}'"), choices, raw)?;
            serde_json::Value::String(raw.to_string())
        }
    })
}

/// The options passed for `provider_id`, keyed by declared name. Callers pass
/// options as `<provider_id>.<name>`, so each provider gets only its own; a choice outside its
/// list fails.
pub(crate) fn provider_options(
    provider_id: &str,
    declared: &[OptionDeclaration],
    passed: &std::collections::HashMap<std::string::String, std::string::String>,
) -> Result<serde_json::Map<std::string::String, serde_json::Value>, std::string::String> {
    declared
        .iter()
        .filter_map(|decl| {
            let key = format!("{provider_id}.{}", decl.name);
            let raw = passed.get(&key)?;
            Some(
                coerce_option_value(&key, raw, &decl.option_type)
                    .map(|value| (decl.name.clone(), value)),
            )
        })
        .collect()
}

pub(crate) const MAX_PASSED_OPTIONS: usize = 20;
pub(crate) const MAX_OPTION_KEY_LEN: usize = 64;
pub(crate) const MAX_OPTION_VALUE_LEN: usize = 256;

/// Checks options a request passes as JSON, keyed `<owner_id>.<name>`: a bounded number of
/// short keys, each set to a boolean, a number or a short string.
pub(crate) fn check_passed_options(
    passed: &std::collections::HashMap<std::string::String, serde_json::Value>,
) -> Result<(), std::string::String> {
    if passed.len() > MAX_PASSED_OPTIONS {
        return Err(format!(
            "options takes at most {MAX_PASSED_OPTIONS} entries, got {}",
            passed.len()
        ));
    }
    for (key, value) in passed {
        if key.len() > MAX_OPTION_KEY_LEN {
            return Err(format!(
                "option keys must be at most {MAX_OPTION_KEY_LEN} bytes"
            ));
        }
        match value {
            serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
            serde_json::Value::String(value) if value.len() <= MAX_OPTION_VALUE_LEN => {}
            serde_json::Value::String(_) => {
                return Err(format!(
                    "option '{key}' must be at most {MAX_OPTION_VALUE_LEN} bytes"
                ));
            }
            _ => {
                return Err(format!(
                    "option '{key}' must be a boolean, number, or string"
                ));
            }
        }
    }
    Ok(())
}

/// The JSON options passed for `owner_id`, keyed by declared name. Callers pass options as
/// `<owner_id>.<name>`, so each owner gets only its own; a value that isn't its option's declared
/// type, or a choice outside its list, fails.
pub(crate) fn typed_options(
    owner_id: &str,
    declared: &[OptionDeclaration],
    passed: &std::collections::HashMap<std::string::String, serde_json::Value>,
) -> Result<serde_json::Map<std::string::String, serde_json::Value>, std::string::String> {
    declared
        .iter()
        .filter_map(|decl| {
            let key = format!("{owner_id}.{}", decl.name);
            let value = passed.get(&key)?;
            let value = match (&decl.option_type, value) {
                (OptionType::Boolean, serde_json::Value::Bool(_))
                | (OptionType::String, serde_json::Value::String(_)) => Ok(value.clone()),
                (OptionType::Number, serde_json::Value::Number(number)) => Ok(number
                    .as_f64()
                    .map(|number| serde_json::json!(number))
                    .unwrap_or(serde_json::Value::Null)),
                (OptionType::Choice(choices), value) => value
                    .as_str()
                    .ok_or_else(|| format!("option '{key}' must be a string"))
                    .and_then(|choice| check_choice(&format!("option '{key}'"), choices, choice))
                    .map(|()| value.clone()),
                (option_type, _) => Err(format!("option '{key}' must be a {}", option_type.name())),
            };
            Some(value.map(|value| (decl.name.clone(), value)))
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
        )
        .expect("provider options");

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
        )
        .expect("provider options");

        assert!(options.is_empty());
    }

    fn space() -> OptionType {
        OptionType::Choice(
            ["audio", "lyrics"]
                .map(|value| OptionChoice {
                    value: value.to_string(),
                    label: value.to_string(),
                })
                .to_vec(),
        )
    }

    #[test]
    fn provider_options_take_only_listed_choices() {
        let declared = [declaration("space", space())];
        let options = provider_options(
            "similar",
            &declared,
            &passed(&[("similar.space", "lyrics")]),
        )
        .expect("listed choice");
        assert_eq!(options["space"], serde_json::json!("lyrics"));
        assert_eq!(
            provider_options("similar", &declared, &passed(&[("similar.space", "video")])),
            Err("option 'similar.space' must be one of 'audio', 'lyrics'".to_string())
        );
    }

    #[test]
    fn typed_options_take_only_listed_choices() {
        let declared = [declaration("space", space())];
        let options = typed_options(
            "similar",
            &declared,
            &HashMap::from([("similar.space".to_string(), serde_json::json!("audio"))]),
        )
        .expect("listed choice");
        assert_eq!(options["space"], serde_json::json!("audio"));
        assert_eq!(
            typed_options(
                "similar",
                &declared,
                &HashMap::from([("similar.space".to_string(), serde_json::json!("video"))]),
            ),
            Err("option 'similar.space' must be one of 'audio', 'lyrics'".to_string())
        );
        assert_eq!(
            typed_options(
                "similar",
                &declared,
                &HashMap::from([("similar.space".to_string(), serde_json::json!(1))]),
            ),
            Err("option 'similar.space' must be a string".to_string())
        );
    }

    #[test]
    fn typed_options_take_the_owners_options_of_their_declared_types() {
        let declared = [
            declaration("wide", OptionType::Boolean),
            declaration("depth", OptionType::Number),
            declaration("mood", OptionType::String),
        ];
        let passed = HashMap::from([
            ("similar.wide".to_string(), serde_json::json!(true)),
            ("similar.depth".to_string(), serde_json::json!(3)),
            ("similar.mood".to_string(), serde_json::json!("calm")),
            ("other.wide".to_string(), serde_json::json!(false)),
            ("similar.undeclared".to_string(), serde_json::json!(1)),
        ]);
        let options = typed_options("similar", &declared, &passed).expect("typed options");
        assert_eq!(
            serde_json::Value::Object(options),
            serde_json::json!({ "wide": true, "depth": 3.0, "mood": "calm" })
        );

        let mistyped = HashMap::from([("similar.wide".to_string(), serde_json::json!("yes"))]);
        assert_eq!(
            typed_options("similar", &declared, &mistyped),
            Err("option 'similar.wide' must be a boolean".to_string())
        );
    }

    #[test]
    fn passed_options_are_bounded_scalars() {
        let check = |pairs: Vec<(String, serde_json::Value)>| {
            check_passed_options(&pairs.into_iter().collect::<HashMap<_, _>>())
        };
        assert!(check(vec![("a.b".to_string(), serde_json::json!("x"))]).is_ok());
        assert!(check(vec![("a.b".to_string(), serde_json::json!([1]))]).is_err());
        assert!(
            check(vec![(
                "a.b".to_string(),
                serde_json::json!("x".repeat(257))
            )])
            .is_err()
        );
        assert!(check(vec![("k".repeat(65), serde_json::json!(true))]).is_err());
        assert!(
            check(
                (0..=MAX_PASSED_OPTIONS)
                    .map(|n| (format!("a.{n}"), serde_json::json!(true)))
                    .collect()
            )
            .is_err()
        );
    }
}
