// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use agdb::{
    DbAny,
    DbId,
};
use anyhow::{
    Context,
    anyhow,
};

use crate::{
    db,
    plugins::runtime::{
        FieldDefinition,
        Schema,
    },
};

#[derive(Debug)]
pub(crate) enum ValidatedChange {
    Upsert { key: String, json: String },
    Remove { key: String },
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub(crate) struct InvalidStoredSettings {
    message: String,
}

impl InvalidStoredSettings {
    fn global(plugin_id: &str, message: impl Into<String>) -> Self {
        Self {
            message: format!(
                "{}; delete /api/plugins/{plugin_id}/settings to clear stale state",
                message.into()
            ),
        }
    }

    fn user(plugin_id: &str, message: impl Into<String>) -> Self {
        Self {
            message: format!(
                "{}; delete /api/me/plugins/{plugin_id}/settings to clear stale state",
                message.into()
            ),
        }
    }
}

fn parse_entries(
    plugin_id: &str,
    entries: Vec<db::settings::SettingEntry>,
    make_error: fn(&str, String) -> InvalidStoredSettings,
) -> anyhow::Result<HashMap<String, serde_json::Value>> {
    let mut values = HashMap::with_capacity(entries.len());

    for entry in entries {
        let value = serde_json::from_str(&entry.value).map_err(|error| {
            make_error(
                plugin_id,
                format!(
                    "invalid stored JSON for plugin setting {plugin_id}.{}: {error}",
                    entry.key
                ),
            )
        })?;

        if values.insert(entry.key.clone(), value).is_some() {
            return Err(make_error(
                plugin_id,
                format!(
                    "multiple stored values found for plugin setting {plugin_id}.{}",
                    entry.key
                ),
            )
            .into());
        }
    }

    Ok(values)
}

fn validate_stored_values_inner(
    plugin_id: &str,
    schema: &Schema,
    values: &HashMap<String, serde_json::Value>,
    make_error: fn(&str, String) -> InvalidStoredSettings,
) -> anyhow::Result<()> {
    let mut entries = values.iter().collect::<Vec<_>>();
    entries.sort_by(|(left, _), (right, _)| left.cmp(right));

    for (key, value) in entries {
        let field = schema.field(key).ok_or_else(|| {
            make_error(
                plugin_id,
                format!("stored plugin setting {plugin_id}.{key} is no longer declared"),
            )
        })?;
        field.validate_value(value).map_err(|error| {
            make_error(
                plugin_id,
                format!("invalid stored value for plugin setting {plugin_id}.{key}: {error}"),
            )
        })?;
    }

    Ok(())
}

fn validate_change(
    field: &FieldDefinition,
    value: &serde_json::Value,
) -> anyhow::Result<ValidatedChange> {
    let key = field.key();
    field.validate_value(value)?;

    if value.is_null() {
        return Ok(ValidatedChange::Remove {
            key: key.to_string(),
        });
    }

    upsert_change(key, value)
}

fn upsert_change(key: &str, value: &serde_json::Value) -> anyhow::Result<ValidatedChange> {
    Ok(ValidatedChange::Upsert {
        key: key.to_string(),
        json: serde_json::to_string(value)
            .with_context(|| format!("failed to serialize plugin setting {key}"))?,
    })
}

pub(crate) fn validate_updates(
    schema: &Schema,
    values: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<Vec<ValidatedChange>> {
    values
        .iter()
        .map(|(key, value)| {
            let field = schema
                .field(key)
                .ok_or_else(|| anyhow!("unknown plugin setting: {key}"))?;
            validate_change(field, value)
        })
        .collect()
}

fn node_db_id(plugin_node: db::settings::PluginSettings, plugin_id: &str) -> anyhow::Result<DbId> {
    plugin_node
        .db_id
        .ok_or_else(|| anyhow!("plugin settings node missing db_id: {plugin_id}"))
        .map(Into::into)
}

fn load_stored_values_with<A: db::DbAccess>(
    db: &A,
    plugin_id: &str,
) -> anyhow::Result<HashMap<String, serde_json::Value>> {
    let Some(plugin_node) = db::settings::find_plugin_settings_with(db, plugin_id)? else {
        return Ok(HashMap::new());
    };
    let plugin_db_id = node_db_id(plugin_node, plugin_id)?;
    let entries = db::settings::get_all_settings_with(db, plugin_db_id)?;
    parse_entries(plugin_id, entries, InvalidStoredSettings::global)
}

pub(crate) fn load_stored_values(
    db: &DbAny,
    plugin_id: &str,
) -> anyhow::Result<HashMap<String, serde_json::Value>> {
    load_stored_values_with(db, plugin_id)
}

pub(crate) fn validate_stored_values(
    plugin_id: &str,
    schema: &Schema,
    values: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<()> {
    validate_stored_values_inner(plugin_id, schema, values, InvalidStoredSettings::global)
}

fn load_validated_stored_values_with<A: db::DbAccess>(
    db: &A,
    plugin_id: &str,
    schema: &Schema,
) -> anyhow::Result<HashMap<String, serde_json::Value>> {
    let values = load_stored_values_with(db, plugin_id)?;
    validate_stored_values(plugin_id, schema, &values)?;
    Ok(values)
}

pub(crate) fn load_validated_stored_values(
    db: &DbAny,
    plugin_id: &str,
    schema: &Schema,
) -> anyhow::Result<HashMap<String, serde_json::Value>> {
    load_validated_stored_values_with(db, plugin_id, schema)
}

pub(crate) fn clear_stored_values(db: &mut DbAny, plugin_id: &str) -> anyhow::Result<()> {
    db.transaction_mut(|t| db::settings::remove_plugin_settings_with(t, plugin_id))
}

pub(crate) fn apply_updates(
    db: &mut DbAny,
    plugin_id: &str,
    schema: &Schema,
    changes: &[ValidatedChange],
) -> anyhow::Result<()> {
    if changes.is_empty() {
        return Ok(());
    }

    db.transaction_mut(|t| -> anyhow::Result<()> {
        load_validated_stored_values_with(t, plugin_id, schema)?;
        let mut plugin_db_id = db::settings::find_plugin_settings_with(t, plugin_id)?
            .map(|node| node_db_id(node, plugin_id))
            .transpose()?;

        for change in changes {
            match change {
                ValidatedChange::Upsert { key, json } => {
                    let plugin_db_id = match plugin_db_id {
                        Some(plugin_db_id) => plugin_db_id,
                        None => {
                            let node =
                                db::settings::get_or_create_plugin_settings_with(t, plugin_id)?;
                            let created_plugin_db_id = node_db_id(node, plugin_id)?;
                            plugin_db_id = Some(created_plugin_db_id);
                            created_plugin_db_id
                        }
                    };

                    db::settings::upsert_setting_with(t, plugin_db_id, key.clone(), json.clone())?;
                }
                ValidatedChange::Remove { key } => {
                    if let Some(plugin_db_id) = plugin_db_id {
                        db::settings::remove_setting_with(t, plugin_db_id, key)?;
                    }
                }
            }
        }

        if let Some(plugin_db_id) = plugin_db_id
            && db::settings::get_all_settings_with(t, plugin_db_id)?.is_empty()
        {
            db::settings::remove_plugin_settings_with(t, plugin_id)?;
        }

        Ok(())
    })
}

fn user_node_db_id(
    node: db::settings::UserPluginSettings,
    user_db_id: DbId,
    plugin_id: &str,
) -> anyhow::Result<DbId> {
    node.db_id
        .ok_or_else(|| {
            anyhow!(
                "user plugin settings node missing db_id: user={} plugin={plugin_id}",
                user_db_id.0
            )
        })
        .map(Into::into)
}

fn load_user_stored_values_with<A: db::DbAccess>(
    db: &A,
    user_db_id: DbId,
    plugin_id: &str,
) -> anyhow::Result<HashMap<String, serde_json::Value>> {
    let Some(node) = db::settings::find_user_plugin_settings_with(db, user_db_id, plugin_id)?
    else {
        return Ok(HashMap::new());
    };
    let node_id = user_node_db_id(node, user_db_id, plugin_id)?;
    let entries = db::settings::get_all_settings_with(db, node_id)?;
    parse_entries(plugin_id, entries, InvalidStoredSettings::user)
}

fn validate_user_stored_values(
    plugin_id: &str,
    schema: &Schema,
    values: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<()> {
    validate_stored_values_inner(plugin_id, schema, values, InvalidStoredSettings::user)
}

pub(crate) fn load_validated_user_stored_values(
    db: &DbAny,
    user_db_id: DbId,
    plugin_id: &str,
    schema: &Schema,
) -> anyhow::Result<HashMap<String, serde_json::Value>> {
    let values = load_user_stored_values_with(db, user_db_id, plugin_id)?;
    validate_user_stored_values(plugin_id, schema, &values)?;
    Ok(values)
}

pub(crate) fn apply_user_updates(
    db: &mut DbAny,
    user_db_id: DbId,
    plugin_id: &str,
    schema: &Schema,
    changes: &[ValidatedChange],
) -> anyhow::Result<()> {
    if changes.is_empty() {
        return Ok(());
    }

    db.transaction_mut(|t| -> anyhow::Result<()> {
        let values = load_user_stored_values_with(t, user_db_id, plugin_id)?;
        validate_user_stored_values(plugin_id, schema, &values)?;

        let mut settings_db_id =
            db::settings::find_user_plugin_settings_with(t, user_db_id, plugin_id)?
                .map(|node| user_node_db_id(node, user_db_id, plugin_id))
                .transpose()?;

        for change in changes {
            match change {
                ValidatedChange::Upsert { key, json } => {
                    let parent_id = match settings_db_id {
                        Some(id) => id,
                        None => {
                            let node = db::settings::get_or_create_user_plugin_settings_with(
                                t, user_db_id, plugin_id,
                            )?;
                            let created_id = user_node_db_id(node, user_db_id, plugin_id)?;
                            settings_db_id = Some(created_id);
                            created_id
                        }
                    };

                    db::settings::upsert_setting_with(t, parent_id, key.clone(), json.clone())?;
                }
                ValidatedChange::Remove { key } => {
                    if let Some(parent_id) = settings_db_id {
                        db::settings::remove_setting_with(t, parent_id, key)?;
                    }
                }
            }
        }

        if let Some(parent_id) = settings_db_id
            && db::settings::get_all_settings_with(t, parent_id)?.is_empty()
        {
            db::settings::remove_user_plugin_settings_with(t, user_db_id, plugin_id)?;
        }

        Ok(())
    })
}

pub(crate) fn clear_user_stored_values(
    db: &mut DbAny,
    user_db_id: DbId,
    plugin_id: &str,
) -> anyhow::Result<()> {
    db.transaction_mut(|t| db::settings::remove_user_plugin_settings_with(t, user_db_id, plugin_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            self,
            test_db::new_test_db,
        },
        plugins::runtime::{
            ChoiceOption,
            FieldGroupDefinition,
            FieldProps,
        },
    };
    use agdb::QueryBuilder;

    fn create_plugin_settings(
        db: &mut DbAny,
        plugin_id: &str,
    ) -> anyhow::Result<db::settings::PluginSettings> {
        db.transaction_mut(|t| db::settings::get_or_create_plugin_settings_with(t, plugin_id))
    }

    fn upsert_setting(
        db: &mut DbAny,
        plugin_settings_id: DbId,
        key: String,
        value: String,
    ) -> anyhow::Result<DbId> {
        db.transaction_mut(|t| db::settings::upsert_setting_with(t, plugin_settings_id, key, value))
    }

    fn get_setting(
        db: &DbAny,
        plugin_settings_id: DbId,
        key: &str,
    ) -> anyhow::Result<Option<db::settings::SettingEntry>> {
        let entries = db::settings::get_all_settings_with(db, plugin_settings_id)?;
        Ok(entries.into_iter().find(|e| e.key == key))
    }

    fn props(required: bool) -> FieldProps {
        FieldProps {
            label: "Label".to_string(),
            description: None,
            required,
            default_value: None,
        }
    }

    fn props_with_default(required: bool, default_value: serde_json::Value) -> FieldProps {
        FieldProps {
            label: "Label".to_string(),
            description: None,
            required,
            default_value: Some(default_value),
        }
    }

    fn group(id: &str, fields: Vec<FieldDefinition>) -> FieldGroupDefinition {
        FieldGroupDefinition {
            id: id.to_string(),
            label: id.to_string(),
            fields,
        }
    }

    fn schema(groups: Vec<FieldGroupDefinition>) -> Schema {
        Schema { groups }
    }

    fn test_user(db: &mut DbAny) -> anyhow::Result<DbId> {
        let user = db::users::test_user("testuser")?;
        Ok(db::users::create(db, &user)?)
    }

    #[test]
    fn load_stored_values_is_read_only_for_missing_plugins() -> anyhow::Result<()> {
        let db = new_test_db()?;

        assert!(db::settings::find_plugin_settings_with(&db, "demo")?.is_none());
        let values = load_stored_values(&db, "demo")?;
        assert!(values.is_empty());
        assert!(db::settings::find_plugin_settings_with(&db, "demo")?.is_none());

        Ok(())
    }

    #[test]
    fn load_stored_values_fails_on_invalid_json() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let plugin = create_plugin_settings(&mut db, "demo")?;
        let plugin_db_id: DbId = plugin.db_id.unwrap().into();
        upsert_setting(&mut db, plugin_db_id, "token".into(), "not-json".into())?;

        let error = load_stored_values(&db, "demo").unwrap_err();
        assert!(error.to_string().contains("invalid stored JSON"));

        Ok(())
    }

    #[test]
    fn load_stored_values_fails_on_duplicate_keys() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let plugin = create_plugin_settings(&mut db, "demo")?;
        let plugin_db_id: DbId = plugin.db_id.unwrap().into();
        upsert_setting(&mut db, plugin_db_id, "token".into(), "\"first\"".into())?;

        let duplicate = db::settings::SettingEntry {
            db_id: None,
            id: "duplicate".to_string(),
            key: "token".to_string(),
            value: "\"second\"".to_string(),
        };
        let duplicate_db_id = db
            .exec_mut(QueryBuilder::insert().element(&duplicate).query())?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(plugin_db_id)
                .to(duplicate_db_id)
                .query(),
        )?;

        let error = load_stored_values(&db, "demo").unwrap_err();
        assert!(error.to_string().contains("multiple stored values found"));

        Ok(())
    }

    #[test]
    fn validate_updates_rejects_unknown_keys() {
        let schema = schema(vec![group(
            "credentials",
            vec![FieldDefinition::String {
                key: "token".to_string(),
                props: props(false),
            }],
        )]);
        let values = HashMap::from([("typo".to_string(), serde_json::json!("abc"))]);

        let error = validate_updates(&schema, &values).unwrap_err();
        assert!(error.to_string().contains("unknown plugin setting"));
    }

    #[test]
    fn validate_stored_values_rejects_removed_keys() {
        let schema = schema(vec![group(
            "credentials",
            vec![FieldDefinition::String {
                key: "token".to_string(),
                props: props(false),
            }],
        )]);
        let values = HashMap::from([("legacy".to_string(), serde_json::json!("abc"))]);

        let error = validate_stored_values("demo", &schema, &values).unwrap_err();
        assert!(error.to_string().contains("no longer declared"));
        assert!(
            error
                .to_string()
                .contains("delete /api/plugins/demo/settings")
        );
    }

    #[test]
    fn validate_updates_rejects_invalid_types_and_ranges() {
        let schema = schema(vec![group(
            "general",
            vec![
                FieldDefinition::Number {
                    key: "volume".to_string(),
                    props: props(false),
                    min: Some(0.0),
                    max: Some(10.0),
                },
                FieldDefinition::Choice {
                    key: "mode".to_string(),
                    props: props(false),
                    options: vec![
                        ChoiceOption {
                            value: "off".to_string(),
                            label: "Off".to_string(),
                            description: None,
                        },
                        ChoiceOption {
                            value: "on".to_string(),
                            label: "On".to_string(),
                            description: None,
                        },
                    ],
                },
            ],
        )]);

        let range_error = validate_updates(
            &schema,
            &HashMap::from([("volume".to_string(), serde_json::json!(42))]),
        )
        .unwrap_err();
        assert!(range_error.to_string().contains("less than or equal to 10"));

        let choice_error = validate_updates(
            &schema,
            &HashMap::from([("mode".to_string(), serde_json::json!("maybe"))]),
        )
        .unwrap_err();
        assert!(choice_error.to_string().contains("must be one of"));
    }

    #[test]
    fn validate_updates_allows_clearing_required_values_that_have_defaults() -> anyhow::Result<()> {
        let schema = schema(vec![group(
            "credentials",
            vec![FieldDefinition::String {
                key: "token".to_string(),
                props: props_with_default(true, serde_json::json!("fallback")),
            }],
        )]);

        let changes = validate_updates(
            &schema,
            &HashMap::from([("token".to_string(), serde_json::Value::Null)]),
        )?;

        assert!(matches!(
            changes.as_slice(),
            [ValidatedChange::Remove { key }] if key == "token"
        ));

        Ok(())
    }

    #[test]
    fn apply_updates_removes_optional_values_on_null() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let schema = schema(vec![group(
            "credentials",
            vec![FieldDefinition::String {
                key: "token".to_string(),
                props: props(false),
            }],
        )]);

        let changes = validate_updates(
            &schema,
            &HashMap::from([("token".to_string(), serde_json::json!("abc"))]),
        )?;
        apply_updates(&mut db, "demo", &schema, &changes)?;

        let changes = validate_updates(
            &schema,
            &HashMap::from([("token".to_string(), serde_json::Value::Null)]),
        )?;
        apply_updates(&mut db, "demo", &schema, &changes)?;

        let values = load_stored_values(&db, "demo")?;
        assert!(!values.contains_key("token"));
        assert!(db::settings::find_plugin_settings_with(&db, "demo")?.is_none());

        Ok(())
    }

    #[test]
    fn apply_updates_rejects_invalid_existing_state_without_mutating() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let plugin = create_plugin_settings(&mut db, "demo")?;
        let plugin_db_id: DbId = plugin.db_id.unwrap().into();
        upsert_setting(&mut db, plugin_db_id, "legacy".into(), "\"abc\"".into())?;

        let schema = schema(vec![group(
            "credentials",
            vec![FieldDefinition::String {
                key: "token".to_string(),
                props: props(false),
            }],
        )]);
        let changes = validate_updates(
            &schema,
            &HashMap::from([("token".to_string(), serde_json::json!("new-token"))]),
        )?;

        let error = apply_updates(&mut db, "demo", &schema, &changes).unwrap_err();
        assert!(error.to_string().contains("no longer declared"));
        assert!(
            get_setting(&db, plugin_db_id, "token")?.is_none(),
            "failed update should not persist new values"
        );
        assert!(get_setting(&db, plugin_db_id, "legacy")?.is_some());

        Ok(())
    }

    #[test]
    fn clear_stored_values_removes_plugin_state() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let plugin = create_plugin_settings(&mut db, "demo")?;
        let plugin_db_id: DbId = plugin.db_id.unwrap().into();
        upsert_setting(&mut db, plugin_db_id, "token".into(), "\"abc\"".into())?;

        clear_stored_values(&mut db, "demo")?;

        assert!(db::settings::find_plugin_settings_with(&db, "demo")?.is_none());
        Ok(())
    }

    #[test]
    fn user_settings_round_trip() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = test_user(&mut db)?;
        let schema = schema(vec![group(
            "auth",
            vec![FieldDefinition::String {
                key: "token".to_string(),
                props: props(false),
            }],
        )]);

        let changes = validate_updates(
            &schema,
            &HashMap::from([("token".to_string(), serde_json::json!("user-token"))]),
        )?;
        apply_user_updates(&mut db, user_db_id, "demo", &schema, &changes)?;

        let values = load_user_stored_values_with(&db, user_db_id, "demo")?;
        assert_eq!(values.get("token"), Some(&serde_json::json!("user-token")));

        Ok(())
    }

    #[test]
    fn user_settings_isolated_between_users() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_a = test_user(&mut db)?;
        let user_b = {
            let user = db::users::test_user("otheruser")?;
            db::users::create(&mut db, &user)?
        };
        let schema = schema(vec![group(
            "auth",
            vec![FieldDefinition::String {
                key: "token".to_string(),
                props: props(false),
            }],
        )]);

        let changes_a = validate_updates(
            &schema,
            &HashMap::from([("token".to_string(), serde_json::json!("token-a"))]),
        )?;
        apply_user_updates(&mut db, user_a, "demo", &schema, &changes_a)?;

        let changes_b = validate_updates(
            &schema,
            &HashMap::from([("token".to_string(), serde_json::json!("token-b"))]),
        )?;
        apply_user_updates(&mut db, user_b, "demo", &schema, &changes_b)?;

        let values_a = load_user_stored_values_with(&db, user_a, "demo")?;
        let values_b = load_user_stored_values_with(&db, user_b, "demo")?;

        assert_eq!(values_a.get("token"), Some(&serde_json::json!("token-a")));
        assert_eq!(values_b.get("token"), Some(&serde_json::json!("token-b")));

        Ok(())
    }

    #[test]
    fn clear_user_settings_removes_only_target_user() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_a = test_user(&mut db)?;
        let user_b = {
            let user = db::users::test_user("otheruser")?;
            db::users::create(&mut db, &user)?
        };
        let schema = schema(vec![group(
            "auth",
            vec![FieldDefinition::String {
                key: "token".to_string(),
                props: props(false),
            }],
        )]);

        let changes = validate_updates(
            &schema,
            &HashMap::from([("token".to_string(), serde_json::json!("abc"))]),
        )?;
        apply_user_updates(&mut db, user_a, "demo", &schema, &changes)?;
        apply_user_updates(&mut db, user_b, "demo", &schema, &changes)?;

        clear_user_stored_values(&mut db, user_a, "demo")?;

        assert!(load_user_stored_values_with(&db, user_a, "demo")?.is_empty());
        assert!(!load_user_stored_values_with(&db, user_b, "demo")?.is_empty());

        Ok(())
    }

    #[test]
    fn user_settings_error_points_to_me_endpoint() {
        let schema = schema(vec![group(
            "auth",
            vec![FieldDefinition::String {
                key: "token".to_string(),
                props: props(false),
            }],
        )]);
        let values = HashMap::from([("legacy".to_string(), serde_json::json!("abc"))]);

        let error = validate_user_stored_values("demo", &schema, &values).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("delete /api/me/plugins/demo/settings")
        );
    }

    #[test]
    fn apply_user_updates_cleans_up_empty_node() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = test_user(&mut db)?;
        let schema = schema(vec![group(
            "auth",
            vec![FieldDefinition::String {
                key: "token".to_string(),
                props: props(false),
            }],
        )]);

        let changes = validate_updates(
            &schema,
            &HashMap::from([("token".to_string(), serde_json::json!("abc"))]),
        )?;
        apply_user_updates(&mut db, user_db_id, "demo", &schema, &changes)?;

        let changes = validate_updates(
            &schema,
            &HashMap::from([("token".to_string(), serde_json::Value::Null)]),
        )?;
        apply_user_updates(&mut db, user_db_id, "demo", &schema, &changes)?;

        assert!(load_user_stored_values_with(&db, user_db_id, "demo")?.is_empty());
        assert!(db::settings::find_user_plugin_settings_with(&db, user_db_id, "demo")?.is_none());

        Ok(())
    }
}
