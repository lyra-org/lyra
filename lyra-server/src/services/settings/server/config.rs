// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    net::IpAddr,
    path::{
        Path,
        PathBuf,
    },
};

#[cfg(test)]
use super::FileSettings;
use super::{
    Lookup,
    resolve_path,
};
use crate::services::libraries::LibraryConfig;

/// Runtime configuration. Every field except `library` is a declared server
/// setting; boot values live on [`crate::services::boot::Boot`] and are not
/// duplicated here.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Config {
    pub(crate) published_url: Option<String>,
    pub(crate) cors: CorsConfig,
    pub(crate) rate_limit: RateLimitConfig,
    pub(crate) library: Option<LibraryConfig>,
    pub(crate) covers_path: PathBuf,
    pub(crate) instance_stats: bool,
    pub(crate) auth: AuthConfig,
    pub(crate) sync: SyncConfig,
    pub(crate) hls: HlsConfig,
    pub(crate) transcoding: TranscodingConfig,
    pub(crate) covers: CoversConfig,
    pub(crate) plugins: PluginsConfig,
}

impl Config {
    /// Builds the typed config from resolved settings. Every field reads its
    /// declared key here, so a missing or mistyped key fails the defaults
    /// test rather than silently keeping a placeholder.
    pub(crate) fn from_settings(
        settings: &Lookup<'_>,
        library: Option<LibraryConfig>,
        data_dir: &Path,
    ) -> Self {
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
            covers_path: resolve_path(data_dir, &settings.value::<PathBuf>("covers_path")),
            instance_stats: settings.value("instance_stats"),
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
            },
            transcoding: TranscodingConfig {
                max_concurrent: settings.value("transcoding.max_concurrent"),
                max_concurrent_per_user: settings.value("transcoding.max_concurrent_per_user"),
                cache_max_bytes: settings.value("transcoding.cache_max_bytes"),
                cache_ttl_seconds: settings.value("transcoding.cache_ttl_seconds"),
            },
            covers: CoversConfig {
                transform_cache_max_bytes: settings.value("covers.transform_cache_max_bytes"),
            },
            plugins: PluginsConfig {
                auto_update: settings.value("plugins.auto_update"),
            },
        }
    }

    /// Defaults derived the same way as at runtime, for tests that need a
    /// config without a file or database.
    #[cfg(test)]
    pub(crate) fn for_tests() -> Self {
        let resolved = super::resolve(
            std::path::Path::new("data"),
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
    /// Per credential, or per client address for anonymous requests.
    pub(crate) global_per_minute: u32,
    pub(crate) global_burst: u32,
    /// Per credential, for API calls on top of the global budget.
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
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TranscodingConfig {
    /// `0` means unlimited.
    pub(crate) max_concurrent: u32,
    /// `0` means unlimited.
    pub(crate) max_concurrent_per_user: u32,
    pub(crate) cache_max_bytes: u64,
    pub(crate) cache_ttl_seconds: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CoversConfig {
    pub(crate) transform_cache_max_bytes: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PluginsConfig {
    /// Install updates the daily check finds that need no approval.
    pub(crate) auto_update: bool,
}
