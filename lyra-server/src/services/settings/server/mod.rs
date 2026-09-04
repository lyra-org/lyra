// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Server settings: declared once in [`definitions`], resolved from defaults,
//! stored database values, and `config.json` into the runtime [`Config`].

use std::collections::BTreeSet;

use anyhow::{
    Context,
    Result,
    anyhow,
    bail,
};

use crate::{
    config::{
        BootConfig,
        Config,
        FileSettings,
        LibraryConfig,
    },
    db,
};

mod definitions;
mod kind;

pub(super) use kind::Kind;

const RECOVERY_HINT: &str = "run `lyra settings reset` to clear stored server settings";

#[derive(Debug)]
pub(crate) struct SettingDefinition {
    pub(crate) key: &'static str,
    pub(crate) kind: Kind,
}

pub(crate) struct Registry {
    definitions: &'static [SettingDefinition],
}

impl Registry {
    fn new(definitions: &'static [SettingDefinition]) -> Self {
        let mut keys = BTreeSet::new();
        for definition in definitions {
            assert!(
                keys.insert(definition.key),
                "duplicate server setting key '{}'",
                definition.key
            );
        }
        Self { definitions }
    }

    pub(crate) fn definitions(&self) -> &'static [SettingDefinition] {
        self.definitions
    }

    pub(crate) fn definition(&self, key: &str) -> Option<&'static SettingDefinition> {
        self.definitions
            .iter()
            .find(|definition| definition.key == key)
    }
}

pub(crate) fn registry() -> &'static Registry {
    static REGISTRY: std::sync::LazyLock<Registry> =
        std::sync::LazyLock::new(|| Registry::new(definitions::ALL));
    &REGISTRY
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SettingSource {
    Default,
    Database,
    File,
}

#[derive(Clone, Debug)]
pub(crate) struct EffectiveSetting {
    pub(crate) definition: &'static SettingDefinition,
    pub(crate) value: serde_json::Value,
    pub(crate) source: SettingSource,
    /// Set in `config.json`; the file value wins over anything stored.
    pub(crate) locked: bool,
}

#[derive(Debug)]
pub(crate) struct ResolvedSettings {
    pub(crate) config: Config,
    pub(crate) effective: Vec<EffectiveSetting>,
}

/// Typed reads over resolved values. Values are normalized before they get
/// here, so an undeclared key or a value that does not deserialize into the
/// requested type is a programming error caught by the defaults test.
pub(crate) struct Lookup<'a> {
    effective: &'a [EffectiveSetting],
}

impl Lookup<'_> {
    pub(crate) fn value<T: serde::de::DeserializeOwned>(&self, key: &str) -> T {
        self.effective
            .iter()
            .find(|setting| setting.definition.key == key)
            .ok_or_else(|| anyhow!("not declared"))
            .and_then(|setting| Ok(serde_json::from_value(setting.value.clone())?))
            .unwrap_or_else(|err| panic!("server setting '{key}': {err}"))
    }
}

/// Rejects undeclared keys and invalid values before the server binds or
/// opens anything. Returns the canonical file values.
pub(crate) fn normalize_file(file: &FileSettings, boot: &BootConfig) -> Result<FileSettings> {
    let registry = registry();
    let mut normalized = FileSettings::new();
    for (key, value) in file {
        let Some(definition) = registry.definition(key) else {
            bail!("unknown config key '{key}'");
        };
        let value = definition
            .kind
            .normalize(value, boot)
            .with_context(|| format!("invalid config {key}"))?;
        normalized.insert(key.clone(), value);
    }
    Ok(normalized)
}

/// Stored server settings, decoded from their JSON entries.
#[derive(Debug)]
pub(crate) struct StoredSetting {
    pub(crate) key: String,
    pub(crate) value: serde_json::Value,
}

pub(crate) fn load_stored(db: &mut agdb::DbAny) -> Result<Vec<StoredSetting>> {
    db::settings::server::ensure(db)?;
    db::settings::server::get_all_with(db)?
        .into_iter()
        .map(|entry| {
            let value = serde_json::from_str(&entry.value).with_context(|| {
                format!(
                    "stored server setting '{}' is not valid JSON; {RECOVERY_HINT}",
                    entry.key
                )
            })?;
            Ok(StoredSetting {
                key: entry.key,
                value,
            })
        })
        .collect()
}

/// Removes every stored server setting and returns the keys removed. The
/// parent node is kept (or created) so the next start reads an empty set.
pub(crate) fn reset_stored(db: &mut agdb::DbAny) -> Result<Vec<String>> {
    db::settings::server::ensure(db)?;
    let mut keys: Vec<String> = db::settings::server::get_all_with(db)?
        .into_iter()
        .map(|entry| entry.key)
        .collect();
    keys.sort_unstable();
    db.transaction_mut(|t| db::settings::server::clear_with(t))?;
    Ok(keys)
}

/// Resolution order per key: default, then the stored value, then the file
/// value. `file` must already be normalized by [`normalize_file`]. A stored
/// value that is undeclared or no longer validates is an error rather than
/// being ignored: silently falling back would run with settings the operator
/// did not choose.
pub(crate) fn resolve(
    boot: &BootConfig,
    library: Option<LibraryConfig>,
    file: &FileSettings,
    stored: &[StoredSetting],
) -> Result<ResolvedSettings> {
    let registry = registry();
    for entry in stored {
        if registry.definition(&entry.key).is_none() {
            bail!(
                "stored server setting '{}' is not a declared setting; {RECOVERY_HINT}",
                entry.key
            );
        }
    }

    let mut effective = Vec::with_capacity(registry.definitions().len());
    for definition in registry.definitions() {
        let key = definition.key;
        let kind = definition.kind;
        let default = kind.default(boot)?;
        let (mut value, mut source) = (default, SettingSource::Default);

        if let Some(entry) = stored.iter().find(|entry| entry.key == key) {
            value = kind.normalize(&entry.value, boot).map_err(|err| {
                anyhow!("stored server setting '{key}' is invalid: {err}; {RECOVERY_HINT}")
            })?;
            source = SettingSource::Database;
        }

        let locked = file.contains_key(key);
        if let Some(file_value) = file.get(key) {
            value = kind
                .normalize(file_value, boot)
                .with_context(|| format!("invalid config {key}"))?;
            source = SettingSource::File;
        }

        effective.push(EffectiveSetting {
            definition,
            value,
            source,
            locked,
        });
    }

    let config = Config::from_settings(
        &Lookup {
            effective: &effective,
        },
        library,
    );
    Ok(ResolvedSettings { config, effective })
}

#[cfg(test)]
mod tests {
    use std::{
        net::{
            IpAddr,
            Ipv4Addr,
            Ipv6Addr,
        },
        path::PathBuf,
    };

    use agdb::DbId;
    use serde_json::json;

    use super::*;
    use crate::config::{
        AuthConfig,
        CorsConfig,
        HlsConfig,
        RateLimitConfig,
        SyncConfig,
    };

    fn boot() -> BootConfig {
        BootConfig {
            data_dir: PathBuf::from("/srv/lyra"),
            ..BootConfig::default()
        }
    }

    fn stored(pairs: &[(&str, serde_json::Value)]) -> Vec<StoredSetting> {
        pairs
            .iter()
            .map(|(key, value)| StoredSetting {
                key: key.to_string(),
                value: value.clone(),
            })
            .collect()
    }

    fn file(pairs: &[(&str, serde_json::Value)]) -> FileSettings {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.clone()))
            .collect()
    }

    fn effective<'a>(resolved: &'a ResolvedSettings, key: &str) -> &'a EffectiveSetting {
        resolved
            .effective
            .iter()
            .find(|setting| setting.definition.key == key)
            .expect("setting should be declared")
    }

    #[test]
    fn all_defaults_match_hand_built_config() -> anyhow::Result<()> {
        let resolved = resolve(&boot(), None, &FileSettings::default(), &[])?;

        let expected = Config {
            published_url: None,
            cors: CorsConfig {
                allowed_origins: Vec::new(),
            },
            rate_limit: RateLimitConfig {
                enabled: true,
                trusted_proxies: vec![
                    IpAddr::V4(Ipv4Addr::LOCALHOST),
                    IpAddr::V6(Ipv6Addr::LOCALHOST),
                ],
                global_per_minute: 1_200,
                global_burst: 300,
                authenticated_per_minute: 600,
                authenticated_burst: 120,
                login_per_minute: 10,
                login_burst: 3,
            },
            library: None,
            covers_path: PathBuf::from("/srv/lyra/covers"),
            auth: AuthConfig {
                enabled: true,
                allow_default_login_when_disabled: true,
                session_ttl_seconds: 2_592_000,
            },
            sync: SyncConfig { interval_secs: 0 },
            hls: HlsConfig {
                temp_disk_budget_bytes: None,
                cleanup_startup_purge: true,
                max_concurrent_transcodes: 0,
            },
        };
        assert_eq!(resolved.config, expected);
        assert_eq!(resolved.effective.len(), registry().definitions().len());
        assert!(
            resolved
                .effective
                .iter()
                .all(|setting| setting.source == SettingSource::Default && !setting.locked)
        );
        Ok(())
    }

    #[test]
    fn database_beats_default_and_file_beats_database() -> anyhow::Result<()> {
        let stored = stored(&[
            ("rate_limit.login_burst", json!(9)),
            ("auth.session_ttl_seconds", json!(60)),
        ]);
        let file = file(&[("auth.session_ttl_seconds", json!(120))]);

        let resolved = resolve(&boot(), None, &file, &stored)?;

        assert_eq!(resolved.config.rate_limit.login_burst, 9);
        assert_eq!(resolved.config.auth.session_ttl_seconds, 120);
        let burst = effective(&resolved, "rate_limit.login_burst");
        assert_eq!(burst.source, SettingSource::Database);
        assert!(!burst.locked);
        let ttl = effective(&resolved, "auth.session_ttl_seconds");
        assert_eq!(ttl.source, SettingSource::File);
        assert!(ttl.locked);
        assert_eq!(
            effective(&resolved, "auth.enabled").source,
            SettingSource::Default
        );
        Ok(())
    }

    #[test]
    fn file_presence_locks_even_when_equal_to_default() -> anyhow::Result<()> {
        let file = file(&[("auth.enabled", json!(true))]);

        let resolved = resolve(&boot(), None, &file, &[])?;

        let setting = effective(&resolved, "auth.enabled");
        assert!(setting.locked);
        assert_eq!(setting.source, SettingSource::File);
        Ok(())
    }

    #[test]
    fn normalize_file_rejects_unknown_leaf() {
        let error = normalize_file(&file(&[("hls.signed_url_ttl_seconds", json!(1))]), &boot())
            .expect_err("unknown key should fail");
        assert!(
            error
                .to_string()
                .contains("unknown config key 'hls.signed_url_ttl_seconds'")
        );

        let error = normalize_file(&file(&[("prot", json!(5000))]), &boot())
            .expect_err("unknown key should fail");
        assert!(error.to_string().contains("'prot'"));
    }

    #[test]
    fn normalize_file_rejects_empty_object_leaves() {
        let error = normalize_file(&file(&[("rate_limit", json!({}))]), &boot())
            .expect_err("empty group should fail");
        assert!(
            error
                .to_string()
                .contains("unknown config key 'rate_limit'")
        );

        let error = normalize_file(&file(&[("cors.allowed_origins", json!({}))]), &boot())
            .expect_err("empty object value should fail");
        assert!(
            error
                .to_string()
                .contains("invalid config cors.allowed_origins")
        );
    }

    #[test]
    fn normalize_file_canonicalizes_and_names_invalid_keys() -> anyhow::Result<()> {
        let normalized = normalize_file(
            &file(&[("published_url", json!("http://LOCALHOST:8080/"))]),
            &boot(),
        )?;
        assert_eq!(normalized["published_url"], json!("http://localhost:8080"));

        let error = normalize_file(
            &file(&[("published_url", json!("https://example.com/app"))]),
            &boot(),
        )
        .expect_err("path should be rejected");
        assert!(error.to_string().contains("invalid config published_url"));
        Ok(())
    }

    #[test]
    fn file_null_locks_nullable_settings_and_rejects_others() -> anyhow::Result<()> {
        let stored = stored(&[("published_url", json!("https://stored.example"))]);
        let file = normalize_file(&file(&[("published_url", json!(null))]), &boot())?;

        let resolved = resolve(&boot(), None, &file, &stored)?;
        assert!(resolved.config.published_url.is_none());
        let setting = effective(&resolved, "published_url");
        assert!(setting.locked);
        assert_eq!(setting.source, SettingSource::File);

        let error = normalize_file(&self::file(&[("auth.enabled", json!(null))]), &boot())
            .expect_err("null for a required setting should fail");
        let message = format!("{error:#}");
        assert!(message.contains("invalid config auth.enabled"));
        assert!(message.contains("must be a boolean"));
        Ok(())
    }

    #[test]
    fn undeclared_stored_key_fails_with_recovery_hint() {
        let stored = stored(&[("legacy.key", json!(1))]);

        let error = resolve(&boot(), None, &FileSettings::default(), &stored)
            .expect_err("undeclared stored key should fail");

        let message = error.to_string();
        assert!(message.contains("'legacy.key'"));
        assert!(message.contains("lyra settings reset"));
    }

    #[test]
    fn invalid_stored_value_fails_with_recovery_hint() {
        let stored = stored(&[("rate_limit.login_burst", json!("many"))]);

        let error = resolve(&boot(), None, &FileSettings::default(), &stored)
            .expect_err("invalid stored value should fail");

        let message = error.to_string();
        assert!(message.contains("'rate_limit.login_burst'"));
        assert!(message.contains("lyra settings reset"));
    }

    #[test]
    fn library_passes_through_resolution() -> anyhow::Result<()> {
        let library = LibraryConfig {
            path: Some(PathBuf::from("/music")),
            ..LibraryConfig::default()
        };

        let resolved = resolve(
            &boot(),
            Some(library.clone()),
            &FileSettings::default(),
            &[],
        )?;

        assert_eq!(resolved.config.library, Some(library));
        Ok(())
    }

    #[test]
    fn nullable_settings_accept_null_from_every_source() -> anyhow::Result<()> {
        let stored = stored(&[("published_url", json!(null))]);
        let file = file(&[("hls.temp_disk_budget_bytes", json!(null))]);

        let resolved = resolve(&boot(), None, &file, &stored)?;

        assert!(resolved.config.published_url.is_none());
        assert!(resolved.config.hls.temp_disk_budget_bytes.is_none());
        assert_eq!(
            effective(&resolved, "published_url").source,
            SettingSource::Database
        );
        Ok(())
    }

    fn stored_parent(db: &agdb::DbAny) -> anyhow::Result<DbId> {
        Ok(DbId::from(
            db::settings::server::find_with(db)?
                .and_then(|node| node.db_id)
                .expect("ensure creates the parent"),
        ))
    }

    #[test]
    fn load_stored_reads_entries_and_reset_clears_them() -> anyhow::Result<()> {
        let mut db = crate::db::test_db::new_test_db()?;
        assert!(load_stored(&mut db)?.is_empty());

        let parent = stored_parent(&db)?;
        db::settings::upsert_setting_with(&mut db, parent, "auth.enabled".into(), "false".into())?;
        db::settings::upsert_setting_with(
            &mut db,
            parent,
            "sync.interval_secs".into(),
            "5".into(),
        )?;

        let stored = load_stored(&mut db)?;
        assert_eq!(stored.len(), 2);
        let resolved = resolve(&boot(), None, &FileSettings::default(), &stored)?;
        assert!(!resolved.config.auth.enabled);
        assert_eq!(resolved.config.sync.interval_secs, 5);

        assert_eq!(
            reset_stored(&mut db)?,
            vec!["auth.enabled", "sync.interval_secs"]
        );
        assert!(load_stored(&mut db)?.is_empty());
        assert!(reset_stored(&mut db)?.is_empty());
        Ok(())
    }

    #[test]
    fn load_stored_rejects_malformed_json_with_recovery_hint() -> anyhow::Result<()> {
        let mut db = crate::db::test_db::new_test_db()?;
        load_stored(&mut db)?;
        let parent = stored_parent(&db)?;
        db::settings::upsert_setting_with(&mut db, parent, "auth.enabled".into(), "nope".into())?;

        let error = load_stored(&mut db).expect_err("malformed JSON should fail");
        assert!(error.to_string().contains("'auth.enabled'"));
        assert!(error.to_string().contains("lyra settings reset"));
        Ok(())
    }
}
