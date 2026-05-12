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
    path::PathBuf,
};

use crate::locale::{
    validate_country,
    validate_language,
};

fn config_candidate_paths() -> Vec<PathBuf> {
    if let Some(path) = env::var_os("LYRA_CONFIG_PATH") {
        let path = PathBuf::from(path);
        if !path.as_os_str().is_empty() {
            return vec![path];
        }
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let manifest_parent = manifest_dir
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| manifest_dir.clone());

    let mut candidates = Vec::new();

    if let Ok(cwd) = env::current_dir() {
        candidates.push(cwd.join("config.json"));
    }

    if let Ok(exe) = env::current_exe() {
        if let Some(exe_dir) = exe.parent() {
            candidates.push(exe_dir.join("config.json"));
            candidates.push(exe_dir.join("..").join("config.json"));
        }
    }

    candidates.push(manifest_parent.join("config.json"));
    candidates.push(manifest_dir.join("config.json"));

    candidates
}

pub(crate) const DEFAULT_PORT: u16 = 4746;
const LYRA_DB_DIR_ENV: &str = "LYRA_DB_DIR";
const DEFAULT_DB_FILE_NAME: &str = "lyra.db";

#[derive(Clone, Deserialize)]
#[serde(default)]
pub(crate) struct Config {
    pub(crate) port: u16,
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

#[derive(Clone, Deserialize)]
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
    #[serde(default)]
    pub(crate) enabled: bool,
    #[serde(default = "default_allow_default_login_when_disabled")]
    pub(crate) allow_default_login_when_disabled: bool,
    #[serde(default = "default_default_username")]
    pub(crate) default_username: String,
    #[serde(default = "default_session_ttl_seconds")]
    pub(crate) session_ttl_seconds: u64,
}

#[derive(Clone, Deserialize)]
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

impl Default for SyncConfig {
    fn default() -> Self {
        Self { interval_secs: 0 }
    }
}

impl Default for LibraryConfig {
    fn default() -> Self {
        Self {
            path: None,
            name: None,
            language: None,
            country: None,
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DbKind {
    Memory,
    File,
    Mmap,
}

impl Default for DbKind {
    fn default() -> Self {
        DbKind::Memory
    }
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            kind: DbKind::Memory,
            path: default_db_path(),
        }
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            allow_default_login_when_disabled: default_allow_default_login_when_disabled(),
            default_username: default_default_username(),
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

fn default_allow_default_login_when_disabled() -> bool {
    true
}

fn default_default_username() -> String {
    "default".to_string()
}

fn default_session_ttl_seconds() -> u64 {
    2_592_000 // 30 days
}

fn normalize_db_path(config: &mut Config) -> Result<()> {
    normalize_db_path_with_dir(config, env::var_os(LYRA_DB_DIR_ENV))
}

fn normalize_db_path_with_dir(config: &mut Config, raw: Option<OsString>) -> Result<()> {
    let Some(db_dir) = configured_db_dir(raw)? else {
        return Ok(());
    };

    if config.db.path.is_relative() {
        config.db.path = db_dir.join(&config.db.path);
    }

    Ok(())
}

fn configured_db_dir(raw: Option<OsString>) -> Result<Option<PathBuf>> {
    let Some(raw) = raw else {
        return Ok(None);
    };

    let path = PathBuf::from(raw);
    if path.as_os_str().is_empty() {
        return Ok(None);
    }

    if !path.is_dir() {
        return Err(anyhow!(
            "{LYRA_DB_DIR_ENV} points to '{}' but it is not a directory",
            path.display()
        ));
    }

    Ok(Some(path))
}

pub(crate) fn load_config() -> Result<Config> {
    let candidates = config_candidate_paths();
    let Some(path) = candidates.iter().find(|path| path.is_file()).cloned() else {
        if let Ok(explicit) = env::var("LYRA_CONFIG_PATH") {
            return Err(anyhow!(
                "config file not found at LYRA_CONFIG_PATH '{explicit}'"
            ));
        }
        return Err(anyhow!(
            "failed to locate config.json at any known location"
        ));
    };

    let contents = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read config file at {}", path.display()))?;
    let mut config: Config = serde_json::from_str(&contents)
        .with_context(|| format!("failed to parse config file at {}", path.display()))?;

    normalize_db_path(&mut config)?;
    normalize_config_library_locale_inputs(&mut config)?;
    normalize_published_url(&mut config)?;
    normalize_cors_allowed_origins(&mut config)?;

    Ok(config)
}

fn normalize_published_url(config: &mut Config) -> Result<()> {
    let Some(raw) = config.published_url.as_deref() else {
        return Ok(());
    };

    let parsed = url::Url::parse(raw)
        .map_err(|err| anyhow!("invalid config published_url '{raw}': {err}"))?;

    let scheme = parsed.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(anyhow!(
            "invalid config published_url '{raw}': scheme must be http or https, got '{scheme}'"
        ));
    }

    config.published_url = Some(match (parsed.query(), parsed.fragment()) {
        (None, None) => parsed.to_string().trim_end_matches('/').to_string(),
        _ => parsed.to_string(),
    });
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
        Config,
        CorsConfig,
        DEFAULT_PORT,
        DbConfig,
        DbKind,
        HlsConfig,
        LibraryConfig,
        RateLimitConfig,
        SyncConfig,
        configured_db_dir,
        normalize_config_library_locale_inputs,
        normalize_cors_allowed_origins,
        normalize_db_path_with_dir,
    };
    use std::path::PathBuf;

    fn base_config_with_library(library: Option<LibraryConfig>) -> Config {
        Config {
            port: DEFAULT_PORT,
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

    fn unique_temp_path(label: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("lyra-{label}-{}-{nanos}", std::process::id()))
    }

    #[test]
    fn db_dir_env_resolves_relative_db_paths() -> anyhow::Result<()> {
        let db_dir = unique_temp_path("db-dir");
        std::fs::create_dir(&db_dir)?;
        let mut config = Config::default();
        config.db.kind = DbKind::Mmap;
        config.db.path = PathBuf::from("custom.db");

        normalize_db_path_with_dir(&mut config, Some(db_dir.clone().into_os_string()))?;

        assert_eq!(config.db.path, db_dir.join("custom.db"));
        std::fs::remove_dir(&db_dir)?;
        Ok(())
    }

    #[test]
    fn db_dir_env_preserves_absolute_db_paths() -> anyhow::Result<()> {
        let db_dir = unique_temp_path("db-dir");
        std::fs::create_dir(&db_dir)?;
        let mut config = Config::default();
        config.db.kind = DbKind::Mmap;
        config.db.path = PathBuf::from("/var/lib/lyra/custom.db");

        normalize_db_path_with_dir(&mut config, Some(db_dir.clone().into_os_string()))?;

        assert_eq!(config.db.path, PathBuf::from("/var/lib/lyra/custom.db"));
        std::fs::remove_dir(&db_dir)?;
        Ok(())
    }

    #[test]
    fn db_dir_env_ignores_empty_values() -> anyhow::Result<()> {
        let resolved = configured_db_dir(Some(std::ffi::OsString::new()))?;

        assert_eq!(resolved, None);
        Ok(())
    }

    #[test]
    fn db_dir_env_rejects_non_directories() {
        let path = unique_temp_path("missing-db-dir");

        let error = configured_db_dir(Some(path.into_os_string()))
            .expect_err("missing db directory should be rejected");

        assert!(error.to_string().contains("LYRA_DB_DIR"));
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
