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
    net::IpAddr,
    path::PathBuf,
};

use crate::{
    locale::{
        validate_country,
        validate_language,
    },
    services::settings::server::Lookup,
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
    FileSettings,
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

/// Runtime configuration. Every field except `library` is a declared server
/// setting written by `services::settings::server`; boot values live on
/// [`BootConfig`] and are not duplicated here.
#[derive(Clone, Debug, PartialEq)]
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

impl Config {
    /// Builds the typed config from resolved settings. Every field reads its
    /// declared key here, so a missing or mistyped key fails the defaults
    /// test rather than silently keeping a placeholder.
    pub(crate) fn from_settings(settings: &Lookup<'_>, library: Option<LibraryConfig>) -> Self {
        Self {
            published_url: settings.value("published_url"),
            cors: CorsConfig {
                allowed_origins: settings.value("cors.allowed_origins"),
            },
            rate_limit: RateLimitConfig {
                enabled: settings.value("rate_limit.enabled"),
                trusted_proxies: settings.value("rate_limit.trusted_proxies"),
                global_per_minute: settings.value("rate_limit.global_per_minute"),
                global_burst: settings.value("rate_limit.global_burst"),
                authenticated_per_minute: settings.value("rate_limit.authenticated_per_minute"),
                authenticated_burst: settings.value("rate_limit.authenticated_burst"),
                login_per_minute: settings.value("rate_limit.login_per_minute"),
                login_burst: settings.value("rate_limit.login_burst"),
            },
            library,
            covers_path: settings.value("covers_path"),
            auth: AuthConfig {
                enabled: settings.value("auth.enabled"),
                allow_default_login_when_disabled: settings
                    .value("auth.allow_default_login_when_disabled"),
                session_ttl_seconds: settings.value("auth.session_ttl_seconds"),
            },
            sync: SyncConfig {
                interval_secs: settings.value("sync.interval_secs"),
            },
            hls: HlsConfig {
                temp_disk_budget_bytes: settings.value("hls.temp_disk_budget_bytes"),
                cleanup_startup_purge: settings.value("hls.cleanup_startup_purge"),
                max_concurrent_transcodes: settings.value("hls.max_concurrent_transcodes"),
            },
        }
    }

    /// Defaults derived the same way as at runtime, for tests that need a
    /// config without a file or database.
    #[cfg(test)]
    pub(crate) fn for_tests() -> Self {
        let resolved = crate::services::settings::server::resolve(
            &BootConfig::default(),
            None,
            FileSettings::default(),
            &[],
        )
        .expect("default config resolves");
        (*resolved.config).clone()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CorsConfig {
    pub(crate) allowed_origins: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
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

/// The config `library` block: a developer bootstrap that seeds one library
/// at startup. Not the supported way to add a library — that is
/// `POST /api/libraries`.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct LibraryConfig {
    pub(crate) path: Option<PathBuf>,
    /// Display name; defaults to `"Music"`. Override when another library
    /// already uses the default — names are unique.
    pub(crate) name: Option<String>,
    pub(crate) language: Option<String>,
    pub(crate) country: Option<String>,
}

impl LibraryConfig {
    /// Validates the locale inputs up front so a bad file fails before any
    /// directory or database is touched.
    pub(crate) fn resolve(file: Option<&LibraryFile>) -> Result<Option<Self>> {
        file.map(|library| -> Result<Self> {
            Ok(Self {
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
        .transpose()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AuthConfig {
    pub(crate) enabled: bool,
    pub(crate) allow_default_login_when_disabled: bool,
    pub(crate) session_ttl_seconds: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SyncConfig {
    pub(crate) interval_secs: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct HlsConfig {
    /// `None` or `0` means no budget.
    pub(crate) temp_disk_budget_bytes: Option<u64>,
    pub(crate) cleanup_startup_purge: bool,
    /// `0` means unlimited.
    pub(crate) max_concurrent_transcodes: u32,
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
        ConfigFile,
        LibraryConfig,
    };
    use std::path::Path;

    fn resolve_library(json: &str) -> anyhow::Result<Option<LibraryConfig>> {
        LibraryConfig::resolve(ConfigFile::parse(json)?.library.as_ref())
    }

    #[test]
    fn config_library_locale_inputs_are_normalized() -> anyhow::Result<()> {
        let library = resolve_library(
            r#"{"library": {"path": "/music", "language": "Japanese", "country": "Japan"}}"#,
        )?
        .expect("library should be present");

        assert_eq!(library.path.as_deref(), Some(Path::new("/music")));
        assert_eq!(library.language.as_deref(), Some("jpn"));
        assert_eq!(library.country.as_deref(), Some("JP"));
        Ok(())
    }

    #[test]
    fn missing_library_resolves_to_none() -> anyhow::Result<()> {
        assert!(resolve_library("{}")?.is_none());
        Ok(())
    }

    #[test]
    fn invalid_library_language_returns_error() {
        let error = resolve_library(r#"{"library": {"language": "not-a-language"}}"#)
            .expect_err("expected error");

        assert!(
            error
                .to_string()
                .contains("invalid config library.language")
        );
    }
}
