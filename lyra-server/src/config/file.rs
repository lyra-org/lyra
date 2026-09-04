// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The on-disk `config.json`. Every field is optional so the layers above can
//! tell "set in the file" apart from "left to the default".

use anyhow::{
    Context,
    Result,
    anyhow,
};
use serde::Deserialize;
use std::{
    env,
    ffi::OsString,
    net::IpAddr,
    path::{
        Path,
        PathBuf,
    },
};

use super::DbKind;

const LYRA_CONFIG_PATH_ENV: &str = "LYRA_CONFIG_PATH";

#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ConfigFile {
    pub(crate) port: Option<u16>,
    pub(crate) published_url: Option<String>,
    pub(crate) cors: Option<CorsFile>,
    pub(crate) rate_limit: Option<RateLimitFile>,
    /// Dev-only bootstrap that seeds one library at startup; libraries are
    /// otherwise managed through the API.
    pub(crate) library: Option<LibraryFile>,
    pub(crate) covers_path: Option<PathBuf>,
    pub(crate) db: Option<DbFile>,
    pub(crate) auth: Option<AuthFile>,
    pub(crate) sync: Option<SyncFile>,
    pub(crate) hls: Option<HlsFile>,
}

#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct CorsFile {
    pub(crate) allowed_origins: Option<Vec<String>>,
}

#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct RateLimitFile {
    pub(crate) enabled: Option<bool>,
    pub(crate) trusted_proxies: Option<Vec<IpAddr>>,
    pub(crate) global_per_minute: Option<u32>,
    pub(crate) global_burst: Option<u32>,
    pub(crate) authenticated_per_minute: Option<u32>,
    pub(crate) authenticated_burst: Option<u32>,
    pub(crate) login_per_minute: Option<u32>,
    pub(crate) login_burst: Option<u32>,
}

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

#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct AuthFile {
    pub(crate) enabled: Option<bool>,
    pub(crate) allow_default_login_when_disabled: Option<bool>,
    pub(crate) session_ttl_seconds: Option<u64>,
}

#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct SyncFile {
    pub(crate) interval_secs: Option<u64>,
}

#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct HlsFile {
    pub(crate) temp_disk_budget_bytes: Option<u64>,
    pub(crate) cleanup_startup_purge: Option<bool>,
    pub(crate) max_concurrent_transcodes: Option<u32>,
}

impl ConfigFile {
    pub(crate) fn parse(contents: &str) -> Result<Self> {
        Ok(serde_json::from_str(contents)?)
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
    fn file_preserves_field_presence() -> anyhow::Result<()> {
        let file = ConfigFile::parse(r#"{"port": 5000, "auth": {"enabled": false}}"#)?;

        assert_eq!(file.port, Some(5000));
        let auth = file.auth.expect("auth block should be present");
        assert_eq!(auth.enabled, Some(false));
        assert_eq!(auth.session_ttl_seconds, None);
        assert!(file.db.is_none());
        assert!(file.covers_path.is_none());
        Ok(())
    }

    #[test]
    fn unknown_keys_are_rejected() {
        assert!(ConfigFile::parse(r#"{"prot": 5000}"#).is_err());
        assert!(ConfigFile::parse(r#"{"hls": {"signed_url_ttl_seconds": 1}}"#).is_err());
    }

    #[test]
    fn empty_file_leaves_every_field_unset() -> anyhow::Result<()> {
        let file = ConfigFile::parse("{}")?;

        assert!(file.port.is_none());
        assert!(file.auth.is_none());
        assert!(file.rate_limit.is_none());
        assert!(file.library.is_none());
        Ok(())
    }
}
