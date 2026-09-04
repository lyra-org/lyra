// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use anyhow::{
    Result,
    anyhow,
};
use std::{
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

mod boot;
mod file;

pub(crate) use boot::{
    BootConfig,
    BootEnv,
    DbConfig,
    DbKind,
};
pub(crate) use file::{
    ConfigFile,
    LibraryFile,
};

fn non_empty_path(raw: Option<OsString>) -> Option<PathBuf> {
    raw.map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
}

/// Everything read at startup: the boot values plus the file they came from.
/// The runtime config is resolved later, once the database is open.
pub(crate) struct LoadedConfig {
    pub(crate) boot: BootConfig,
    pub(crate) file: ConfigFile,
}

/// Locates and parses the config file and resolves boot values. Touches no
/// directories; the serving path creates them via
/// [`BootConfig::ensure_directories`].
pub(crate) fn load() -> Result<LoadedConfig> {
    let file = ConfigFile::load()?.unwrap_or_default();
    let boot = BootConfig::resolve(&file, BootEnv::from_process())?;
    Ok(LoadedConfig { boot, file })
}

/// Runtime configuration: defaults overlaid with the file. Boot values live on
/// [`BootConfig`] and are not duplicated here.
#[derive(Clone)]
pub(crate) struct Config {
    pub(crate) published_url: Option<String>,
    pub(crate) cors: CorsConfig,
    pub(crate) rate_limit: RateLimitConfig,
    pub(crate) library: Option<LibraryConfig>,
    pub(crate) covers_path: PathBuf,
    pub(crate) auth: AuthConfig,
    pub(crate) sync: SyncConfig,
    pub(crate) hls: HlsConfig,
}

fn overlay<T: Clone>(target: &mut T, value: &Option<T>) {
    if let Some(value) = value {
        *target = value.clone();
    }
}

impl Config {
    /// Needs nothing from the database: file values are validated up front so
    /// a bad file fails before any directory or database is touched.
    pub(crate) fn resolve(file: &ConfigFile, boot: &BootConfig) -> Result<Self> {
        let published_url = file
            .published_url
            .as_deref()
            .map(|raw| {
                normalize_origin(raw)
                    .map_err(|err| anyhow!("invalid config published_url '{}': {err}", raw.trim()))
            })
            .transpose()?;

        let mut cors = CorsConfig::default();
        if let Some(allowed_origins) = file
            .cors
            .as_ref()
            .and_then(|cors| cors.allowed_origins.as_ref())
        {
            cors.allowed_origins = allowed_origins
                .iter()
                .map(|raw| {
                    normalize_cors_origin(raw).map_err(|err| {
                        anyhow!(
                            "invalid config cors.allowed_origins entry '{}': {err}",
                            raw.trim()
                        )
                    })
                })
                .collect::<Result<Vec<_>>>()?;
        }

        let mut rate_limit = RateLimitConfig::default();
        if let Some(file) = &file.rate_limit {
            overlay(&mut rate_limit.enabled, &file.enabled);
            overlay(&mut rate_limit.trusted_proxies, &file.trusted_proxies);
            overlay(&mut rate_limit.global_per_minute, &file.global_per_minute);
            overlay(&mut rate_limit.global_burst, &file.global_burst);
            overlay(
                &mut rate_limit.authenticated_per_minute,
                &file.authenticated_per_minute,
            );
            overlay(
                &mut rate_limit.authenticated_burst,
                &file.authenticated_burst,
            );
            overlay(&mut rate_limit.login_per_minute, &file.login_per_minute);
            overlay(&mut rate_limit.login_burst, &file.login_burst);
        }

        let library = file
            .library
            .as_ref()
            .map(|library| -> Result<LibraryConfig> {
                Ok(LibraryConfig {
                    path: library.path.clone(),
                    name: library.name.clone(),
                    language: library
                        .language
                        .as_deref()
                        .map(|raw| {
                            validate_language(raw).map_err(|err| {
                                anyhow!("invalid config library.language '{}': {err}", raw.trim())
                            })
                        })
                        .transpose()?,
                    country: library
                        .country
                        .as_deref()
                        .map(|raw| {
                            validate_country(raw).map_err(|err| {
                                anyhow!("invalid config library.country '{}': {err}", raw.trim())
                            })
                        })
                        .transpose()?,
                })
            })
            .transpose()?;

        let mut auth = AuthConfig::default();
        if let Some(file) = &file.auth {
            overlay(&mut auth.enabled, &file.enabled);
            overlay(
                &mut auth.allow_default_login_when_disabled,
                &file.allow_default_login_when_disabled,
            );
            overlay(&mut auth.session_ttl_seconds, &file.session_ttl_seconds);
        }

        let mut sync = SyncConfig::default();
        if let Some(file) = &file.sync {
            overlay(&mut sync.interval_secs, &file.interval_secs);
        }

        let hls = file
            .hls
            .as_ref()
            .map(|hls| HlsConfig {
                temp_disk_budget_bytes: hls.temp_disk_budget_bytes,
                cleanup_startup_purge: hls.cleanup_startup_purge,
                max_concurrent_transcodes: hls.max_concurrent_transcodes,
            })
            .unwrap_or_default();

        Ok(Self {
            published_url,
            cors,
            rate_limit,
            library,
            covers_path: file
                .covers_path
                .clone()
                .unwrap_or_else(|| boot.default_covers_path()),
            auth,
            sync,
            hls,
        })
    }

    /// Defaults derived the same way as at runtime, for tests that need a
    /// config without a file.
    #[cfg(test)]
    pub(crate) fn for_tests() -> Self {
        Self::resolve(&ConfigFile::default(), &BootConfig::default())
            .expect("default config resolves")
    }
}

#[derive(Clone, Default)]
pub(crate) struct CorsConfig {
    pub(crate) allowed_origins: Vec<String>,
}

#[derive(Clone)]
pub(crate) struct RateLimitConfig {
    pub(crate) enabled: bool,
    pub(crate) trusted_proxies: Vec<IpAddr>,
    pub(crate) global_per_minute: u32,
    pub(crate) global_burst: u32,
    // Checked in addition to the global client bucket.
    pub(crate) authenticated_per_minute: u32,
    pub(crate) authenticated_burst: u32,
    pub(crate) login_per_minute: u32,
    pub(crate) login_burst: u32,
}

#[derive(Clone, Default)]
pub(crate) struct LibraryConfig {
    pub(crate) path: Option<PathBuf>,
    /// Display name; defaults to `"Music"`. Override when another library
    /// already uses the default — names are unique.
    pub(crate) name: Option<String>,
    pub(crate) language: Option<String>,
    pub(crate) country: Option<String>,
}

#[derive(Clone)]
pub(crate) struct AuthConfig {
    pub(crate) enabled: bool,
    pub(crate) allow_default_login_when_disabled: bool,
    pub(crate) session_ttl_seconds: u64,
}

#[derive(Clone, Default)]
pub(crate) struct SyncConfig {
    pub(crate) interval_secs: u64,
}

#[derive(Clone, Default)]
pub(crate) struct HlsConfig {
    pub(crate) temp_disk_budget_bytes: Option<u64>,
    pub(crate) cleanup_startup_purge: Option<bool>,
    pub(crate) max_concurrent_transcodes: Option<u32>,
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

/// Normalizes an http(s) origin: trims, lowercases the host, and rejects
/// paths, queries, fragments, and credentials.
pub(crate) fn normalize_origin(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("empty origin"));
    }

    let parsed = url::Url::parse(trimmed)?;
    let scheme = parsed.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(anyhow!("scheme must be http or https, got '{scheme}'"));
    }
    if parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.path() != "/"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(anyhow!(
            "expected an origin without path, query, fragment, or credentials"
        ));
    }

    Ok(parsed.origin().ascii_serialization())
}

/// CORS entries are origins, plus the `*` wildcard.
pub(crate) fn normalize_cors_origin(raw: &str) -> Result<String> {
    if raw.trim() == "*" {
        return Ok("*".to_string());
    }
    normalize_origin(raw)
}

#[cfg(test)]
pub(super) fn temp_dir(label: &str) -> std::io::Result<tempfile::TempDir> {
    tempfile::Builder::new()
        .prefix(&format!("lyra-{label}-"))
        .tempdir()
}

#[cfg(test)]
mod tests {
    use super::{
        BootConfig,
        Config,
        ConfigFile,
        normalize_origin,
    };
    use std::path::{
        Path,
        PathBuf,
    };

    fn resolve(json: &str) -> anyhow::Result<Config> {
        Config::resolve(&ConfigFile::parse(json)?, &BootConfig::default())
    }

    fn boot_with_data_dir(data_dir: &Path) -> BootConfig {
        BootConfig {
            data_dir: data_dir.to_path_buf(),
            ..BootConfig::default()
        }
    }

    #[test]
    fn resolve_without_file_uses_defaults() -> anyhow::Result<()> {
        let boot = boot_with_data_dir(Path::new("/srv/lyra"));

        let config = Config::resolve(&ConfigFile::default(), &boot)?;

        assert_eq!(config.covers_path, PathBuf::from("/srv/lyra/covers"));
        assert!(config.auth.enabled);
        assert!(config.rate_limit.enabled);
        assert!(config.library.is_none());
        assert!(config.published_url.is_none());
        Ok(())
    }

    #[test]
    fn resolve_overlays_file_values_on_defaults() -> anyhow::Result<()> {
        let boot = boot_with_data_dir(Path::new("/srv/lyra"));
        let file = ConfigFile::parse(
            r#"{
                "published_url": "http://LOCALHOST:8080/",
                "covers_path": "/srv/covers",
                "rate_limit": {"login_burst": 9},
                "auth": {"session_ttl_seconds": 60},
                "sync": {"interval_secs": 5},
                "hls": {"max_concurrent_transcodes": 2},
                "library": {"path": "/music", "language": "Japanese"}
            }"#,
        )?;

        let config = Config::resolve(&file, &boot)?;

        assert_eq!(config.covers_path, PathBuf::from("/srv/covers"));
        assert_eq!(
            config.published_url.as_deref(),
            Some("http://localhost:8080")
        );
        assert_eq!(config.rate_limit.login_burst, 9);
        assert_eq!(config.rate_limit.global_burst, 300);
        assert_eq!(config.auth.session_ttl_seconds, 60);
        assert!(config.auth.enabled);
        assert_eq!(config.sync.interval_secs, 5);
        assert_eq!(config.hls.max_concurrent_transcodes, Some(2));
        assert_eq!(config.hls.cleanup_startup_purge, None);
        let library = config.library.expect("library should be present");
        assert_eq!(library.path.as_deref(), Some(Path::new("/music")));
        assert_eq!(library.language.as_deref(), Some("jpn"));
        Ok(())
    }

    #[test]
    fn partial_auth_block_keeps_auth_enabled() -> anyhow::Result<()> {
        let file = ConfigFile::parse(r#"{"auth": {"session_ttl_seconds": 60}}"#)?;

        let config = Config::resolve(&file, &BootConfig::default())?;

        assert!(config.auth.enabled);
        assert_eq!(config.auth.session_ttl_seconds, 60);
        Ok(())
    }

    #[test]
    fn config_library_locale_inputs_are_normalized() -> anyhow::Result<()> {
        let config = resolve(r#"{"library": {"language": "Japanese", "country": "Japan"}}"#)?;

        let library = config.library.expect("library should be present");
        assert_eq!(library.language.as_deref(), Some("jpn"));
        assert_eq!(library.country.as_deref(), Some("JP"));
        Ok(())
    }

    #[test]
    fn invalid_library_language_returns_error() {
        let error = resolve(r#"{"library": {"language": "not-a-language"}}"#)
            .err()
            .expect("expected error");

        assert!(
            error
                .to_string()
                .contains("invalid config library.language")
        );
    }

    #[test]
    fn origin_is_normalized() -> anyhow::Result<()> {
        assert_eq!(
            normalize_origin(" http://LOCALHOST:8080/ ")?,
            "http://localhost:8080"
        );
        Ok(())
    }

    #[test]
    fn origin_rejects_paths() {
        let error = normalize_origin("https://example.com/app").expect_err("expected error");
        assert!(error.to_string().contains("expected an origin"));
    }

    #[test]
    fn origin_rejects_query_strings() {
        let error =
            normalize_origin("https://example.com?token=secret").expect_err("expected error");
        assert!(error.to_string().contains("expected an origin"));
    }

    #[test]
    fn origin_rejects_non_http_schemes() {
        let error = normalize_origin("file://localhost/tmp").expect_err("expected error");
        assert!(error.to_string().contains("scheme must be http or https"));
    }

    #[test]
    fn published_url_errors_name_the_field() {
        let error = resolve(r#"{"published_url": "https://example.com/app"}"#)
            .err()
            .expect("expected error");
        assert!(error.to_string().contains("invalid config published_url"));
    }

    #[test]
    fn cors_allowed_origins_are_normalized() -> anyhow::Result<()> {
        let config = resolve(
            r#"{"cors": {"allowed_origins": [" http://LOCALHOST:8080 ", "https://example.com", "*"]}}"#,
        )?;

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
        let error = resolve(r#"{"cors": {"allowed_origins": ["http://localhost:8080/app"]}}"#)
            .err()
            .expect("expected error");
        let message = error.to_string();
        assert!(message.contains("invalid config cors.allowed_origins"));
        assert!(message.contains("expected an origin"));
    }
}
