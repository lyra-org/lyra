// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use serde_json::Value;

use super::ProviderRequireSpec;

fn value_at_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cursor = value;
    for segment in path.split('.') {
        let Value::Object(object) = cursor else {
            return None;
        };
        cursor = object.get(segment)?;
    }
    Some(cursor)
}

fn is_present(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::String(value) => !value.trim().is_empty(),
        Value::Array(values) => !values.is_empty(),
        Value::Object(values) => !values.is_empty(),
        _ => true,
    }
}

pub(crate) fn requirements_match(context: &Value, require: &ProviderRequireSpec) -> bool {
    requirements_match_with(require, |path| {
        value_at_path(context, path).is_some_and(is_present)
    })
}

pub(crate) fn requirements_match_with(
    require: &ProviderRequireSpec,
    mut path_is_present: impl FnMut(&str) -> bool,
) -> bool {
    require.all_of.iter().all(|path| path_is_present(path))
        && (require.any_of.is_empty() || require.any_of.iter().any(|path| path_is_present(path)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_all_of_and_any_of_paths() {
        let context = serde_json::json!({
            "ids": { "release_id": "release-1" },
            "artist_names": ["Artist"]
        });
        assert!(requirements_match(
            &context,
            &ProviderRequireSpec {
                all_of: vec!["ids.release_id".to_string()],
                any_of: vec!["missing".to_string(), "artist_names".to_string()],
            }
        ));
        assert!(!requirements_match(
            &context,
            &ProviderRequireSpec {
                all_of: vec!["ids.missing".to_string()],
                any_of: Vec::new(),
            }
        ));
    }

    #[test]
    fn treats_empty_values_as_missing() {
        let context = serde_json::json!({
            "empty_string": "  ",
            "empty_array": [],
            "empty_object": {},
            "null": null,
            "false_value": false,
            "zero": 0,
        });
        for path in ["empty_string", "empty_array", "empty_object", "null"] {
            assert!(!requirements_match(
                &context,
                &ProviderRequireSpec {
                    all_of: vec![path.to_string()],
                    any_of: Vec::new(),
                }
            ));
        }
        for path in ["false_value", "zero"] {
            assert!(requirements_match(
                &context,
                &ProviderRequireSpec {
                    all_of: vec![path.to_string()],
                    any_of: Vec::new(),
                }
            ));
        }
    }
}
