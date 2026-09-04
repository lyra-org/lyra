// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The on-disk `config.json`. Boot values and the dev-only library seed are
//! typed here; every other leaf is handed to the server settings registry
//! keyed by its dotted path, so the file never mirrors runtime settings.

use anyhow::{
    Context,
    Result,
    anyhow,
    bail,
};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    env,
    ffi::OsString,
    path::{
        Path,
        PathBuf,
    },
};

use super::DbKind;

const LYRA_CONFIG_PATH_ENV: &str = "LYRA_CONFIG_PATH";

/// Runtime settings present in the file, keyed by dotted path
/// (`rate_limit.login_burst`). Presence locks the setting to the file value.
pub(crate) type FileSettings = BTreeMap<String, serde_json::Value>;

#[derive(Clone, Default)]
pub(crate) struct ConfigFile {
    pub(crate) port: Option<u16>,
    pub(crate) db: Option<DbFile>,
    /// Dev-only bootstrap that seeds one library at startup; libraries are
    /// otherwise managed through the API.
    pub(crate) library: Option<LibraryFile>,
    pub(crate) settings: FileSettings,
}

/// Dev-only bootstrap; see [`crate::config::LibraryConfig`].
#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct LibraryFile {
    pub(crate) path: Option<PathBuf>,
    pub(crate) name: Option<String>,
    pub(crate) language: Option<String>,
    pub(crate) country: Option<String>,
}

#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct DbFile {
    pub(crate) kind: Option<DbKind>,
    pub(crate) path: Option<PathBuf>,
}

impl ConfigFile {
    pub(crate) fn parse(contents: &str) -> Result<Self> {
        let root: serde_json::Value = serde_json::from_str(contents)?;
        let serde_json::Value::Object(mut root) = root else {
            bail!("config must be a JSON object");
        };

        let port = take_typed(&mut root, "port")?;
        let db = take_typed(&mut root, "db")?;
        let library = take_typed(&mut root, "library")?;

        let mut settings = FileSettings::new();
        for (key, value) in root {
            collect_leaves(&key, value, &mut settings)?;
        }

        Ok(Self {
            port,
            db,
            library,
            settings,
        })
    }

    /// Loads the file named by `LYRA_CONFIG_PATH` or the first candidate on
    /// disk. A missing file is not an error; an absent explicit path is.
    pub(crate) fn load() -> Result<Option<Self>> {
        let Some(path) = locate_config_file(env::var_os(LYRA_CONFIG_PATH_ENV))? else {
            tracing::info!("no config file found; using defaults");
            return Ok(None);
        };

        let contents = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read config file at {}", path.display()))?;
        let file = Self::parse(&contents)
            .with_context(|| format!("failed to parse config file at {}", path.display()))?;
        tracing::info!(path = %path.display(), "loaded config file");
        Ok(Some(file))
    }
}

fn take_typed<T: serde::de::DeserializeOwned>(
    root: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<Option<T>> {
    match root.remove(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => serde_json::from_value(value)
            .map(Some)
            .with_context(|| format!("invalid config {key}")),
    }
}

/// Non-empty objects nest into dotted keys; anything else (arrays and empty
/// objects included) is a leaf, so `{"rate_limit": {}}` surfaces as a key
/// instead of vanishing.
fn collect_leaves(prefix: &str, value: serde_json::Value, out: &mut FileSettings) -> Result<()> {
    match value {
        serde_json::Value::Object(map) if !map.is_empty() => {
            for (key, value) in map {
                collect_leaves(&format!("{prefix}.{key}"), value, out)?;
            }
        }
        leaf => {
            if out.insert(prefix.to_string(), leaf).is_some() {
                bail!("config key '{prefix}' is set more than once");
            }
        }
    }
    Ok(())
}

fn config_candidate_paths() -> Vec<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let manifest_parent = manifest_dir
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| manifest_dir.clone());

    let mut candidates = Vec::new();

    if let Ok(cwd) = env::current_dir() {
        candidates.push(cwd.join("config.json"));
    }

    if let Ok(exe) = env::current_exe()
        && let Some(exe_dir) = exe.parent()
    {
        candidates.push(exe_dir.join("config.json"));
        candidates.push(exe_dir.join("..").join("config.json"));
    }

    candidates.push(manifest_parent.join("config.json"));
    candidates.push(manifest_dir.join("config.json"));

    candidates
}

fn locate_config_file(explicit: Option<OsString>) -> Result<Option<PathBuf>> {
    if let Some(path) = super::non_empty_path(explicit) {
        if !path.exists() {
            return Err(anyhow!(
                "config file not found at {LYRA_CONFIG_PATH_ENV} '{}'",
                path.display()
            ));
        }
        require_regular_file(&path)?;
        return Ok(Some(path));
    }

    for path in config_candidate_paths() {
        if path.exists() {
            require_regular_file(&path)?;
            return Ok(Some(path));
        }
    }
    Ok(None)
}

/// A bind mount of a missing host file yields a directory; loading defaults
/// silently in that case would hide the misconfiguration.
fn require_regular_file(path: &Path) -> Result<()> {
    if !path.is_file() {
        return Err(anyhow!(
            "config path '{}' exists but is not a regular file",
            path.display()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ConfigFile,
        locate_config_file,
    };
    use crate::config::temp_dir;

    #[test]
    fn explicit_config_path_must_exist() -> anyhow::Result<()> {
        let parent = temp_dir("missing-config")?;
        let path = parent.path().join("config.json");

        let error = locate_config_file(Some(path.into_os_string()))
            .expect_err("missing explicit config file should be rejected");

        assert!(error.to_string().contains("LYRA_CONFIG_PATH"));
        Ok(())
    }

    #[test]
    fn explicit_config_path_is_used_when_present() -> anyhow::Result<()> {
        let parent = temp_dir("config")?;
        let path = parent.path().join("config.json");
        std::fs::write(&path, "{}")?;

        let located = locate_config_file(Some(path.clone().into_os_string()))?;

        assert_eq!(located, Some(path));
        Ok(())
    }

    #[test]
    fn config_candidates_that_are_directories_are_rejected() -> anyhow::Result<()> {
        let parent = temp_dir("dir-config")?;
        let path = parent.path().join("config.json");
        std::fs::create_dir(&path)?;

        let error = locate_config_file(Some(path.into_os_string()))
            .expect_err("directory at config path should be rejected");

        assert!(error.to_string().contains("not a regular file"));
        Ok(())
    }

    #[test]
    fn typed_keys_and_settings_are_separated() -> anyhow::Result<()> {
        let file = ConfigFile::parse(
            r#"{
                "port": 5000,
                "db": {"kind": "memory"},
                "library": {"path": "/music"},
                "published_url": "http://localhost",
                "auth": {"enabled": false},
                "cors": {"allowed_origins": ["*"]},
                "rate_limit": {"login": {"burst": 1}}
            }"#,
        )?;

        assert_eq!(file.port, Some(5000));
        assert!(file.db.is_some());
        assert_eq!(
            file.library.and_then(|library| library.path).as_deref(),
            Some(std::path::Path::new("/music"))
        );
        assert_eq!(
            file.settings.keys().collect::<Vec<_>>(),
            vec![
                "auth.enabled",
                "cors.allowed_origins",
                "published_url",
                "rate_limit.login.burst"
            ]
        );
        assert_eq!(file.settings["auth.enabled"], serde_json::json!(false));
        assert_eq!(
            file.settings["cors.allowed_origins"],
            serde_json::json!(["*"])
        );
        Ok(())
    }

    #[test]
    fn duplicate_dotted_keys_are_rejected() {
        let error = ConfigFile::parse(r#"{"auth": {"enabled": false}, "auth.enabled": true}"#)
            .err()
            .expect("duplicate key should fail");
        assert!(error.to_string().contains("'auth.enabled'"));
    }

    #[test]
    fn empty_objects_are_leaves() -> anyhow::Result<()> {
        let file = ConfigFile::parse(r#"{"rate_limit": {}, "cors": {"allowed_origins": {}}}"#)?;

        assert_eq!(file.settings["rate_limit"], serde_json::json!({}));
        assert_eq!(file.settings["cors.allowed_origins"], serde_json::json!({}));
        Ok(())
    }

    #[test]
    fn unknown_typed_subkeys_are_rejected() {
        assert!(ConfigFile::parse(r#"{"db": {"kinds": "memory"}}"#).is_err());
        assert!(ConfigFile::parse(r#"{"library": {"paths": "/music"}}"#).is_err());
        assert!(ConfigFile::parse(r#"[]"#).is_err());
    }

    #[test]
    fn empty_file_leaves_every_field_unset() -> anyhow::Result<()> {
        let file = ConfigFile::parse("{}")?;

        assert!(file.port.is_none());
        assert!(file.db.is_none());
        assert!(file.library.is_none());
        assert!(file.settings.is_empty());
        Ok(())
    }
}
