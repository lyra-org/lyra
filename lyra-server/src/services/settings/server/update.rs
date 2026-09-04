// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Writes to stored server settings: validate against the current
//! resolution, persist in one transaction, re-resolve and publish, then
//! apply live hooks.

use std::{
    collections::HashMap,
    sync::Arc,
};

use anyhow::{
    Context,
    Result,
};
use serde_json::Value;

use super::{
    RECOVERY_HINT,
    ResolvedSettings,
    load_stored,
    registry,
    resolve,
};
use crate::{
    STATE,
    config::BootConfig,
    db,
};

#[derive(Debug, PartialEq)]
pub(crate) enum Change {
    Upsert { key: &'static str, value: Value },
    Remove { key: &'static str },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum UpdateError {
    #[error("unknown server setting: {0}")]
    Undeclared(String),
    #[error("invalid value for server setting '{key}': {source:#}")]
    Invalid { key: String, source: anyhow::Error },
    #[error("settings locked by config.json: {}", .0.join(", "))]
    Locked(Vec<String>),
}

/// Checks every requested key before any write: undeclared keys and locked
/// keys are rejected as a whole request, so a partial update never lands.
/// Null clears the stored value; anything else is normalized the same way
/// a file or stored value is.
pub(crate) fn validate_updates(
    current: &ResolvedSettings,
    boot: &BootConfig,
    values: &HashMap<String, Value>,
) -> Result<Vec<Change>, UpdateError> {
    let registry = registry();
    let mut keys: Vec<&String> = values.keys().collect();
    keys.sort_unstable();

    let definitions = keys
        .iter()
        .map(|key| {
            registry
                .definition(key)
                .ok_or_else(|| UpdateError::Undeclared((*key).clone()))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let locked: Vec<String> = definitions
        .iter()
        .filter(|definition| {
            current
                .effective(definition.key)
                .is_some_and(|setting| setting.locked)
        })
        .map(|definition| definition.key.to_string())
        .collect();
    if !locked.is_empty() {
        return Err(UpdateError::Locked(locked));
    }

    definitions
        .into_iter()
        .map(|definition| {
            let key = definition.key;
            let value = &values[key];
            if value.is_null() {
                return Ok(Change::Remove { key });
            }
            let value =
                definition
                    .kind
                    .normalize(value, boot)
                    .map_err(|source| UpdateError::Invalid {
                        key: key.to_string(),
                        source,
                    })?;
            Ok(Change::Upsert { key, value })
        })
        .collect()
}

pub(crate) fn apply_updates(db: &mut agdb::DbAny, changes: &[Change]) -> Result<()> {
    db::settings::server::ensure(db)?;
    db.transaction_mut(|t| -> Result<()> {
        for change in changes {
            match change {
                Change::Upsert { key, value } => {
                    let json = serde_json::to_string(value)
                        .with_context(|| format!("serialize server setting '{key}'"))?;
                    db::settings::server::upsert_with(t, (*key).to_string(), json)?;
                }
                Change::Remove { key } => db::settings::server::remove_with(t, key)?,
            }
        }
        Ok(())
    })
}

/// Re-resolves against the stored values now in `db`, keeping the boot
/// config, the file layer, and the library seed from the current
/// resolution, and publishes the result. Call while holding the database
/// write lock so concurrent writes serialize with their republish. A
/// failure here leaves the database ahead of the running config, which is
/// logged with the recovery step.
pub(crate) fn republish(db: &mut agdb::DbAny) -> Result<()> {
    let boot = STATE.boot.get();
    let previous = STATE.settings.get();
    let resolved = load_stored(db).and_then(|stored| {
        resolve(
            &boot,
            previous.config.library.clone(),
            previous.file.clone(),
            &stored,
        )
    });
    match resolved {
        Ok(resolved) => {
            STATE.publish_settings(Arc::new(resolved));
            Ok(())
        }
        Err(error) => {
            tracing::error!(
                error = %error,
                "stored server settings were written but could not be applied; the database is ahead of the running config; {RECOVERY_HINT}"
            );
            Err(error)
        }
    }
}

/// Wakes live consumers, which read the current published settings themselves.
pub(crate) async fn apply_live() {
    crate::services::hls::state::notify_transcode_capacity_changed();
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serde_json::json;

    use super::*;
    use crate::config::FileSettings;

    fn boot() -> BootConfig {
        BootConfig {
            data_dir: PathBuf::from("/srv/lyra"),
            ..BootConfig::default()
        }
    }

    fn resolved(file: &[(&str, Value)]) -> ResolvedSettings {
        let file: FileSettings = file
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect();
        resolve(&boot(), None, file, &[]).expect("resolves")
    }

    fn values(entries: &[(&str, Value)]) -> HashMap<String, Value> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect()
    }

    #[test]
    fn normalizes_values_and_maps_null_to_removal() {
        let changes = validate_updates(
            &resolved(&[]),
            &boot(),
            &values(&[
                ("published_url", json!(" http://LOCALHOST:8080/ ")),
                ("covers_path", json!("covers")),
                ("sync.interval_secs", json!(null)),
            ]),
        )
        .expect("valid updates");

        assert_eq!(
            changes,
            vec![
                Change::Upsert {
                    key: "covers_path",
                    value: json!("/srv/lyra/covers"),
                },
                Change::Upsert {
                    key: "published_url",
                    value: json!("http://localhost:8080"),
                },
                Change::Remove {
                    key: "sync.interval_secs",
                },
            ]
        );
    }

    #[test]
    fn rejects_undeclared_keys() {
        let error = validate_updates(
            &resolved(&[]),
            &boot(),
            &values(&[("auth.enabled", json!(false)), ("nope", json!(1))]),
        )
        .expect_err("undeclared key should fail");

        assert!(matches!(error, UpdateError::Undeclared(key) if key == "nope"));
    }

    #[test]
    fn rejects_invalid_values_with_key() {
        let error = validate_updates(
            &resolved(&[]),
            &boot(),
            &values(&[("rate_limit.login_burst", json!("many"))]),
        )
        .expect_err("invalid value should fail");

        match error {
            UpdateError::Invalid { key, source } => {
                assert_eq!(key, "rate_limit.login_burst");
                assert!(source.to_string().contains("non-negative integer"));
            }
            other => panic!("expected invalid value, got {other:?}"),
        }
    }

    #[test]
    fn lists_every_locked_key_before_validating_values() {
        let current = resolved(&[
            ("auth.enabled", json!(false)),
            ("published_url", json!(null)),
        ]);
        let error = validate_updates(
            &current,
            &boot(),
            &values(&[
                ("published_url", json!("https://example.com")),
                ("auth.enabled", json!("not even a bool")),
                ("sync.interval_secs", json!(5)),
            ]),
        )
        .expect_err("locked keys should fail");

        assert!(error.to_string().contains("config.json"));
        assert!(matches!(
            error,
            UpdateError::Locked(keys) if keys == vec!["auth.enabled", "published_url"]
        ));
    }

    #[test]
    fn apply_updates_persists_and_reset_clears() -> Result<()> {
        let mut db = crate::db::test_db::new_test_db()?;

        apply_updates(
            &mut db,
            &[
                Change::Upsert {
                    key: "sync.interval_secs",
                    value: json!(5),
                },
                Change::Upsert {
                    key: "auth.enabled",
                    value: json!(false),
                },
            ],
        )?;
        apply_updates(
            &mut db,
            &[Change::Remove {
                key: "auth.enabled",
            }],
        )?;

        let stored = load_stored(&mut db)?;
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].key, "sync.interval_secs");
        assert_eq!(stored[0].value, json!(5));

        assert_eq!(
            super::super::reset_stored(&mut db)?,
            vec!["sync.interval_secs"]
        );
        assert!(load_stored(&mut db)?.is_empty());
        Ok(())
    }
}
