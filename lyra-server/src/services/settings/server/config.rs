// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    net::IpAddr,
    path::PathBuf,
};

use super::Lookup;
#[cfg(test)]
use crate::config::{
    BootConfig,
    FileSettings,
};
use crate::services::libraries::LibraryConfig;

/// Runtime configuration. Every field except `library` is a declared server
/// setting; boot values live on [`crate::config::BootConfig`] and are not
/// duplicated here.
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
        let resolved = super::resolve(&BootConfig::default(), None, FileSettings::default(), &[])
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
