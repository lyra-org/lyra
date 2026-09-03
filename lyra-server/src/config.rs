// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use anyhow::{
    Context,
    Result,
    anyhow,
};
use serde::Deserialize;
use std::{
    env,
    ffi::OsString,
    net::{
        IpAddr,
        Ipv4Addr,
        Ipv6Addr,
    },
    path::{
        Path,
        PathBuf,
    },
};

use crate::locale::{
    validate_country,
    validate_language,
};

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

pub(crate) const DEFAULT_PORT: u16 = 4746;
const LYRA_CONFIG_PATH_ENV: &str = "LYRA_CONFIG_PATH";
const LYRA_DATA_DIR_ENV: &str = "LYRA_DATA_DIR";
const LYRA_DB_DIR_ENV: &str = "LYRA_DB_DIR";
const LYRA_PORT_ENV: &str = "LYRA_PORT";
const DEFAULT_DATA_DIR: &str = "data";
const DEFAULT_DB_FILE_NAME: &str = "lyra.db";
const DEFAULT_COVERS_DIR_NAME: &str = "covers";

/// Boot-class environment overrides. Read once from the process environment;
/// tests construct it directly so they never touch real variables.
#[derive(Default)]
struct BootEnv {
    config_path: Option<OsString>,
    data_dir: Option<OsString>,
    db_dir: Option<OsString>,
    port: Option<OsString>,
}

impl BootEnv {
    fn from_process() -> Self {
        Self {
            config_path: env::var_os(LYRA_CONFIG_PATH_ENV),
            data_dir: env::var_os(LYRA_DATA_DIR_ENV),
            db_dir: env::var_os(LYRA_DB_DIR_ENV),
            port: env::var_os(LYRA_PORT_ENV),
        }
    }
}

fn non_empty_path(raw: Option<OsString>) -> Option<PathBuf> {
    raw.map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
}

#[derive(Clone, Deserialize)]
#[serde(default)]
pub(crate) struct Config {
    pub(crate) port: u16,
    /// Root for server-owned state. Resolved from `LYRA_DATA_DIR` at boot;
    /// not settable from the config file.
    #[serde(skip)]
    pub(crate) data_dir: PathBuf,
    pub(crate) published_url: Option<String>,
    pub(crate) cors: CorsConfig,
    pub(crate) rate_limit: RateLimitConfig,
    pub(crate) library: Option<LibraryConfig>,
    pub(crate) covers_path: Option<PathBuf>,
    pub(crate) db: DbConfig,
    pub(crate) auth: AuthConfig,
    pub(crate) sync: SyncConfig,
    pub(crate) hls: HlsConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: DEFAULT_PORT,
            data_dir: PathBuf::from(DEFAULT_DATA_DIR),
            published_url: None,
            cors: CorsConfig::default(),
            rate_limit: RateLimitConfig::default(),
            library: None,
            covers_path: None,
            db: DbConfig::default(),
            auth: AuthConfig::default(),
            sync: SyncConfig::default(),
            hls: HlsConfig::default(),
        }
    }
}

#[derive(Clone, Default, Deserialize)]
pub(crate) struct CorsConfig {
    #[serde(default)]
    pub(crate) allowed_origins: Vec<String>,
}

#[derive(Clone, Deserialize)]
pub(crate) struct RateLimitConfig {
    #[serde(default = "default_rate_limit_enabled")]
    pub(crate) enabled: bool,
    #[serde(default = "default_rate_limit_trusted_proxies")]
    pub(crate) trusted_proxies: Vec<IpAddr>,
    #[serde(default = "default_global_rate_limit_per_minute")]
    pub(crate) global_per_minute: u32,
    #[serde(default = "default_global_rate_limit_burst")]
    pub(crate) global_burst: u32,
    // Checked in addition to the global client bucket.
    #[serde(default = "default_authenticated_rate_limit_per_minute")]
    pub(crate) authenticated_per_minute: u32,
    #[serde(default = "default_authenticated_rate_limit_burst")]
    pub(crate) authenticated_burst: u32,
    #[serde(default = "default_login_rate_limit_per_minute")]
    pub(crate) login_per_minute: u32,
    #[serde(default = "default_login_rate_limit_burst")]
    pub(crate) login_burst: u32,
}

#[derive(Clone, Default, Deserialize)]
pub(crate) struct LibraryConfig {
    #[serde(default)]
    pub(crate) path: Option<PathBuf>,
    /// Display name; defaults to `"Music"`. Override when another library
    /// already uses the default — names are unique.
    #[serde(default)]
    pub(crate) name: Option<String>,
    #[serde(default)]
    pub(crate) language: Option<String>,
    #[serde(default)]
    pub(crate) country: Option<String>,
}

#[derive(Clone, Deserialize)]
pub(crate) struct DbConfig {
    #[serde(default)]
    pub(crate) kind: DbKind,
    #[serde(default = "default_db_path")]
    pub(crate) path: PathBuf,
}

#[derive(Clone, Deserialize)]
pub(crate) struct AuthConfig {
    #[serde(default = "default_auth_enabled")]
    pub(crate) enabled: bool,
    #[serde(default = "default_allow_default_login_when_disabled")]
    pub(crate) allow_default_login_when_disabled: bool,
    #[serde(default = "default_session_ttl_seconds")]
    pub(crate) session_ttl_seconds: u64,
}

#[derive(Clone, Default, Deserialize)]
pub(crate) struct SyncConfig {
    #[serde(default)]
    pub(crate) interval_secs: u64,
}

#[derive(Clone, Default, Deserialize)]
pub(crate) struct HlsConfig {
    pub(crate) temp_disk_budget_bytes: Option<u64>,
    pub(crate) cleanup_startup_purge: Option<bool>,
    pub(crate) max_concurrent_transcodes: Option<u32>,
}

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DbKind {
    Memory,
    File,
    #[default]
    Mmap,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            kind: DbKind::Mmap,
            path: default_db_path(),
        }
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: default_auth_enabled(),
            allow_default_login_when_disabled: default_allow_default_login_when_disabled(),
            session_ttl_seconds: default_session_ttl_seconds(),
        }
    }
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: default_rate_limit_enabled(),
            trusted_proxies: default_rate_limit_trusted_proxies(),
            global_per_minute: default_global_rate_limit_per_minute(),
            global_burst: default_global_rate_limit_burst(),
            authenticated_per_minute: default_authenticated_rate_limit_per_minute(),
            authenticated_burst: default_authenticated_rate_limit_burst(),
            login_per_minute: default_login_rate_limit_per_minute(),
            login_burst: default_login_rate_limit_burst(),
        }
    }
}

fn default_db_path() -> PathBuf {
    PathBuf::from(DEFAULT_DB_FILE_NAME)
}

fn default_rate_limit_enabled() -> bool {
    true
}

fn default_rate_limit_trusted_proxies() -> Vec<IpAddr> {
    vec![
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(Ipv6Addr::LOCALHOST),
    ]
}

fn default_global_rate_limit_per_minute() -> u32 {
    1_200
}

fn default_global_rate_limit_burst() -> u32 {
    300
}

fn default_authenticated_rate_limit_per_minute() -> u32 {
    600
}

fn default_authenticated_rate_limit_burst() -> u32 {
    120
}

fn default_login_rate_limit_per_minute() -> u32 {
    10
}

fn default_login_rate_limit_burst() -> u32 {
    3
}

fn default_auth_enabled() -> bool {
    true
}

fn default_allow_default_login_when_disabled() -> bool {
    true
}

fn default_session_ttl_seconds() -> u64 {
    2_592_000 // 30 days
}

/// Memory-kind paths are pure identifiers and stay as written: agdb's
/// `DbMemory::new` would otherwise load a stale on-disk file of that name.
fn normalize_db_path(config: &mut Config, raw: Option<OsString>) -> Result<()> {
    if matches!(config.db.kind, DbKind::Memory) {
        return Ok(());
    }

    let db_dir = match configured_db_dir(raw)? {
        Some(db_dir) => db_dir,
        None => config.data_dir.clone(),
    };

    if config.db.path.is_relative() {
        config.db.path = db_dir.join(&config.db.path);
    }

    Ok(())
}

fn configured_db_dir(raw: Option<OsString>) -> Result<Option<PathBuf>> {
    let Some(path) = non_empty_path(raw) else {
        return Ok(None);
    };

    std::fs::create_dir_all(&path).with_context(|| {
        format!(
            "{LYRA_DB_DIR_ENV} points to '{}' but it is not a directory",
            path.display()
        )
    })?;

    Ok(Some(path))
}

fn data_dir_path(raw: Option<OsString>) -> Result<PathBuf> {
    let path = non_empty_path(raw).unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIR));
    if path.is_absolute() {
        return Ok(path);
    }

    let cwd = env::current_dir().context("failed to resolve the current working directory")?;
    Ok(cwd.join(path))
}

fn ensure_data_dir(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path).with_context(|| {
        format!(
            "{LYRA_DATA_DIR_ENV} points to '{}' but it could not be created as a directory",
            path.display()
        )
    })
}

fn apply_port_override(config: &mut Config, raw: Option<OsString>) -> Result<()> {
    let Some(raw) = raw else {
        return Ok(());
    };
    let value = raw.to_string_lossy();
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(());
    }

    config.port = trimmed
        .parse::<u16>()
        .map_err(|err| anyhow!("invalid {LYRA_PORT_ENV} value '{trimmed}': {err}"))?;
    Ok(())
}

/// Returns the config file to load: the explicit `LYRA_CONFIG_PATH`, which
/// must exist, or the first candidate found on disk, or none.
fn locate_config_file(explicit: Option<OsString>) -> Result<Option<PathBuf>> {
    if let Some(path) = non_empty_path(explicit) {
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

fn read_config_file(path: &Path) -> Result<Config> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config file at {}", path.display()))?;
    serde_json::from_str(&contents)
        .with_context(|| format!("failed to parse config file at {}", path.display()))
}

pub(crate) fn load_config() -> Result<Config> {
    let BootEnv {
        config_path,
        data_dir,
        db_dir,
        port,
    } = BootEnv::from_process();
    let file = match locate_config_file(config_path)? {
        Some(path) => {
            let config = read_config_file(&path)?;
            tracing::info!(path = %path.display(), "loaded config file");
            Some(config)
        }
        None => {
            tracing::info!("no config file found; using defaults");
            None
        }
    };

    let config = finalize_config(
        file.unwrap_or_default(),
        BootEnv {
            config_path: None,
            data_dir,
            db_dir,
            port,
        },
    )?;
    ensure_data_dir(&config.data_dir)?;
    Ok(config)
}

/// Applies boot-class environment overrides and defaults derived from them
/// (env > file > default). Only `LYRA_DB_DIR` touches the filesystem.
fn finalize_config(mut config: Config, boot: BootEnv) -> Result<Config> {
    apply_port_override(&mut config, boot.port)?;
    config.data_dir = data_dir_path(boot.data_dir)?;
    normalize_db_path(&mut config, boot.db_dir)?;
    if config.covers_path.is_none() {
        config.covers_path = Some(config.data_dir.join(DEFAULT_COVERS_DIR_NAME));
    }
    normalize_config_library_locale_inputs(&mut config)?;
    normalize_published_url(&mut config)?;
    normalize_cors_allowed_origins(&mut config)?;

    Ok(config)
}

fn normalize_published_url(config: &mut Config) -> Result<()> {
    let Some(raw) = config.published_url.as_deref() else {
        return Ok(());
    };

    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("invalid config published_url: empty URL"));
    }

    let parsed = url::Url::parse(trimmed)
        .map_err(|err| anyhow!("invalid config published_url '{trimmed}': {err}"))?;

    let scheme = parsed.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(anyhow!(
            "invalid config published_url '{trimmed}': scheme must be http or https, got '{scheme}'"
        ));
    }
    if parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.path() != "/"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(anyhow!(
            "invalid config published_url '{trimmed}': expected an origin without path, query, fragment, or credentials"
        ));
    }

    config.published_url = Some(parsed.origin().ascii_serialization());
    Ok(())
}

fn normalize_cors_allowed_origins(config: &mut Config) -> Result<()> {
    let mut origins = Vec::with_capacity(config.cors.allowed_origins.len());
    for raw in &config.cors.allowed_origins {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(anyhow!(
                "invalid config cors.allowed_origins entry: empty origin"
            ));
        }

        if trimmed == "*" {
            origins.push(trimmed.to_string());
            continue;
        }

        let parsed = url::Url::parse(trimmed).map_err(|err| {
            anyhow!("invalid config cors.allowed_origins entry '{trimmed}': {err}")
        })?;
        let scheme = parsed.scheme();
        if scheme != "http" && scheme != "https" {
            return Err(anyhow!(
                "invalid config cors.allowed_origins entry '{trimmed}': scheme must be http or https, got '{scheme}'"
            ));
        }
        if parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.path() != "/"
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(anyhow!(
                "invalid config cors.allowed_origins entry '{trimmed}': expected an origin without path, query, fragment, or credentials"
            ));
        }

        origins.push(parsed.origin().ascii_serialization());
    }
    config.cors.allowed_origins = origins;
    Ok(())
}

fn normalize_config_library_locale_inputs(config: &mut Config) -> Result<()> {
    let Some(library) = config.library.as_mut() else {
        return Ok(());
    };

    if let Some(language) = library.language.as_ref() {
        let normalized = validate_language(language).map_err(|err| {
            anyhow!(
                "invalid config library.language '{}': {}",
                language.trim(),
                err
            )
        })?;
        library.language = Some(normalized);
    }

    if let Some(country) = library.country.as_ref() {
        let normalized = validate_country(country).map_err(|err| {
            anyhow!(
                "invalid config library.country '{}': {}",
                country.trim(),
                err
            )
        })?;
        library.country = Some(normalized);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        AuthConfig,
        BootEnv,
        Config,
        CorsConfig,
        DEFAULT_PORT,
        DbConfig,
        DbKind,
        HlsConfig,
        LibraryConfig,
        RateLimitConfig,
        SyncConfig,
        apply_port_override,
        configured_db_dir,
        data_dir_path,
        ensure_data_dir,
        finalize_config,
        locate_config_file,
        normalize_config_library_locale_inputs,
        normalize_cors_allowed_origins,
        normalize_db_path,
        normalize_published_url,
    };
    use std::{
        ffi::OsString,
        path::PathBuf,
    };

    fn base_config_with_library(library: Option<LibraryConfig>) -> Config {
        Config {
            port: DEFAULT_PORT,
            data_dir: PathBuf::from("data"),
            published_url: None,
            cors: CorsConfig::default(),
            rate_limit: RateLimitConfig::default(),
            library,
            covers_path: None,
            db: DbConfig::default(),
            auth: AuthConfig::default(),
            sync: SyncConfig::default(),
            hls: HlsConfig::default(),
        }
    }

    fn temp_dir(label: &str) -> std::io::Result<tempfile::TempDir> {
        tempfile::Builder::new()
            .prefix(&format!("lyra-{label}-"))
            .tempdir()
    }

    #[test]
    fn db_dir_env_resolves_relative_db_paths() -> anyhow::Result<()> {
        let db_dir = temp_dir("db-dir")?;
        let mut config = Config::default();
        config.db.kind = DbKind::Mmap;
        config.db.path = PathBuf::from("custom.db");

        normalize_db_path(
            &mut config,
            Some(db_dir.path().to_path_buf().into_os_string()),
        )?;

        assert_eq!(config.db.path, db_dir.path().join("custom.db"));
        Ok(())
    }

    #[test]
    fn db_dir_env_preserves_absolute_db_paths() -> anyhow::Result<()> {
        let db_dir = temp_dir("db-dir")?;
        let mut config = Config::default();
        config.db.kind = DbKind::Mmap;
        config.db.path = PathBuf::from("/var/lib/lyra/custom.db");

        normalize_db_path(
            &mut config,
            Some(db_dir.path().to_path_buf().into_os_string()),
        )?;

        assert_eq!(config.db.path, PathBuf::from("/var/lib/lyra/custom.db"));
        Ok(())
    }

    #[test]
    fn db_dir_env_ignores_empty_values() -> anyhow::Result<()> {
        let resolved = configured_db_dir(Some(std::ffi::OsString::new()))?;

        assert_eq!(resolved, None);
        Ok(())
    }

    #[test]
    fn db_dir_env_rejects_non_directories() -> anyhow::Result<()> {
        let parent = temp_dir("file-db-dir")?;
        let path = parent.path().join("file");
        std::fs::write(&path, "")?;

        let error = configured_db_dir(Some(path.into_os_string()))
            .expect_err("file at db directory should be rejected");

        assert!(error.to_string().contains("LYRA_DB_DIR"));
        Ok(())
    }

    #[test]
    fn db_dir_env_creates_missing_directories() -> anyhow::Result<()> {
        let parent = temp_dir("missing-db-dir")?;
        let path = parent.path().join("missing");

        let resolved = configured_db_dir(Some(path.clone().into_os_string()))?;

        assert_eq!(resolved, Some(path.clone()));
        assert!(path.is_dir());
        Ok(())
    }

    #[test]
    fn memory_db_paths_are_not_joined_with_data_dir() -> anyhow::Result<()> {
        let data_dir = temp_dir("data-dir")?;
        let mut config = Config {
            data_dir: data_dir.path().to_path_buf(),
            ..Config::default()
        };
        config.db.kind = DbKind::Memory;
        config.db.path = PathBuf::from("scratch");

        normalize_db_path(&mut config, None)?;

        assert_eq!(config.db.path, PathBuf::from("scratch"));
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
    fn finalize_config_derives_defaults_from_data_dir() -> anyhow::Result<()> {
        let data_dir = temp_dir("data-dir")?;
        let boot = BootEnv {
            data_dir: Some(data_dir.path().as_os_str().to_owned()),
            ..BootEnv::default()
        };

        let config = finalize_config(Config::default(), boot)?;

        assert_eq!(config.port, DEFAULT_PORT);
        assert!(matches!(config.db.kind, DbKind::Mmap));
        assert_eq!(config.data_dir, data_dir.path());
        assert_eq!(
            config.covers_path.as_deref(),
            Some(data_dir.path().join("covers").as_path())
        );
        Ok(())
    }

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
    fn port_env_overrides_file_value() -> anyhow::Result<()> {
        let mut config = Config {
            port: 5000,
            ..Config::default()
        };

        apply_port_override(&mut config, Some(OsString::from("6000")))?;
        assert_eq!(config.port, 6000);

        apply_port_override(&mut config, None)?;
        assert_eq!(config.port, 6000);

        apply_port_override(&mut config, Some(OsString::new()))?;
        assert_eq!(config.port, 6000);
        Ok(())
    }

    #[test]
    fn port_env_rejects_invalid_values() {
        let mut config = Config::default();

        let error = apply_port_override(&mut config, Some(OsString::from("not-a-port")))
            .expect_err("invalid port should be rejected");
        assert!(error.to_string().contains("LYRA_PORT"));

        let error = apply_port_override(&mut config, Some(OsString::from("70000")))
            .expect_err("out-of-range port should be rejected");
        assert!(error.to_string().contains("LYRA_PORT"));
    }

    #[test]
    fn data_dir_defaults_to_data_under_cwd() -> anyhow::Result<()> {
        let resolved = data_dir_path(None)?;

        assert_eq!(resolved, std::env::current_dir()?.join("data"));
        assert_eq!(data_dir_path(Some(OsString::new()))?, resolved);
        Ok(())
    }

    #[test]
    fn data_dir_env_relative_values_resolve_under_cwd() -> anyhow::Result<()> {
        let resolved = data_dir_path(Some(OsString::from("state/lyra")))?;

        assert_eq!(resolved, std::env::current_dir()?.join("state/lyra"));
        Ok(())
    }

    #[test]
    fn data_dir_is_created_when_missing() -> anyhow::Result<()> {
        let parent = temp_dir("data-dir")?;
        let path = parent.path().join("nested").join("data");

        ensure_data_dir(&path)?;

        assert!(path.is_dir());
        Ok(())
    }

    #[test]
    fn data_dir_rejects_files() -> anyhow::Result<()> {
        let parent = temp_dir("data-dir")?;
        let path = parent.path().join("data");
        std::fs::write(&path, "")?;

        let error = ensure_data_dir(&path).expect_err("file at data dir should be rejected");

        assert!(error.to_string().contains("LYRA_DATA_DIR"));
        Ok(())
    }

    #[test]
    fn db_dir_defaults_to_data_dir() -> anyhow::Result<()> {
        let data_dir = temp_dir("data-dir")?;
        let mut config = Config {
            data_dir: data_dir.path().to_path_buf(),
            ..Config::default()
        };

        normalize_db_path(&mut config, None)?;

        assert_eq!(config.db.path, data_dir.path().join("lyra.db"));
        Ok(())
    }

    #[test]
    fn db_dir_env_takes_precedence_over_data_dir() -> anyhow::Result<()> {
        let data_dir = temp_dir("data-dir")?;
        let db_dir = temp_dir("db-dir")?;
        let mut config = Config {
            data_dir: data_dir.path().to_path_buf(),
            ..Config::default()
        };

        normalize_db_path(
            &mut config,
            Some(db_dir.path().to_path_buf().into_os_string()),
        )?;

        assert_eq!(config.db.path, db_dir.path().join("lyra.db"));
        Ok(())
    }

    #[test]
    fn configured_covers_path_is_preserved() -> anyhow::Result<()> {
        let data_dir = temp_dir("data-dir")?;
        let boot = BootEnv {
            data_dir: Some(data_dir.path().as_os_str().to_owned()),
            ..BootEnv::default()
        };
        let config = Config {
            covers_path: Some(PathBuf::from("/srv/covers")),
            ..Config::default()
        };

        let config = finalize_config(config, boot)?;

        assert_eq!(
            config.covers_path.as_deref(),
            Some(std::path::Path::new("/srv/covers"))
        );
        Ok(())
    }

    #[test]
    fn partial_auth_block_keeps_auth_enabled() -> anyhow::Result<()> {
        let config: Config = serde_json::from_str(r#"{"auth": {"session_ttl_seconds": 60}}"#)?;

        assert!(config.auth.enabled);
        assert_eq!(config.auth.session_ttl_seconds, 60);
        Ok(())
    }

    #[test]
    fn config_library_locale_inputs_are_normalized() -> anyhow::Result<()> {
        let mut config = base_config_with_library(Some(LibraryConfig {
            path: None,
            name: None,
            language: Some("Japanese".to_string()),
            country: Some("Japan".to_string()),
        }));

        normalize_config_library_locale_inputs(&mut config)?;

        let library = config.library.expect("library should be present");
        assert_eq!(library.language.as_deref(), Some("jpn"));
        assert_eq!(library.country.as_deref(), Some("JP"));
        Ok(())
    }

    #[test]
    fn invalid_library_language_returns_error() {
        let mut config = base_config_with_library(Some(LibraryConfig {
            path: None,
            name: None,
            language: Some("not-a-language".to_string()),
            country: None,
        }));

        let error =
            normalize_config_library_locale_inputs(&mut config).expect_err("expected error");
        assert!(
            error
                .to_string()
                .contains("invalid config library.language")
        );
    }

    #[test]
    fn published_url_is_normalized_to_origin() -> anyhow::Result<()> {
        let mut config = Config {
            published_url: Some(" http://LOCALHOST:8080/ ".to_string()),
            ..Config::default()
        };

        normalize_published_url(&mut config)?;

        assert_eq!(
            config.published_url.as_deref(),
            Some("http://localhost:8080")
        );
        Ok(())
    }

    #[test]
    fn published_url_rejects_paths() {
        let mut config = Config {
            published_url: Some("https://example.com/app".to_string()),
            ..Config::default()
        };

        let error = normalize_published_url(&mut config).expect_err("expected error");
        assert!(error.to_string().contains("expected an origin"));
    }

    #[test]
    fn published_url_rejects_query_strings() {
        let mut config = Config {
            published_url: Some("https://example.com?token=secret".to_string()),
            ..Config::default()
        };

        let error = normalize_published_url(&mut config).expect_err("expected error");
        assert!(error.to_string().contains("expected an origin"));
    }

    #[test]
    fn cors_allowed_origins_are_normalized() -> anyhow::Result<()> {
        let mut config = Config::default();
        config.cors.allowed_origins = vec![
            " http://LOCALHOST:8080 ".to_string(),
            "https://example.com".to_string(),
            "*".to_string(),
        ];

        normalize_cors_allowed_origins(&mut config)?;

        assert_eq!(
            config.cors.allowed_origins,
            vec![
                "http://localhost:8080".to_string(),
                "https://example.com".to_string(),
                "*".to_string(),
            ]
        );
        Ok(())
    }

    #[test]
    fn cors_allowed_origins_reject_paths() {
        let mut config = Config::default();
        config.cors.allowed_origins = vec!["http://localhost:8080/app".to_string()];

        let error = normalize_cors_allowed_origins(&mut config).expect_err("expected error");
        assert!(error.to_string().contains("expected an origin"));
    }

    #[test]
    fn cors_allowed_origins_reject_non_http_schemes() {
        let mut config = Config::default();
        config.cors.allowed_origins = vec!["file://localhost/tmp".to_string()];

        let error = normalize_cors_allowed_origins(&mut config).expect_err("expected error");
        assert!(error.to_string().contains("scheme must be http or https"));
    }
}
