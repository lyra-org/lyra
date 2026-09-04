// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::{
    ApplyMode::{
        self,
        Live,
        RestartRequired,
    },
    Kind,
    SettingDefinition,
    SettingGroup,
};
use crate::config::BootConfig;

const SERVER: SettingGroup = SettingGroup {
    id: "server",
    label: "Server",
};
const AUTH: SettingGroup = SettingGroup {
    id: "auth",
    label: "Authentication",
};
const CORS: SettingGroup = SettingGroup {
    id: "cors",
    label: "CORS",
};
const RATE_LIMIT: SettingGroup = SettingGroup {
    id: "rate_limit",
    label: "Rate limiting",
};
const SYNC: SettingGroup = SettingGroup {
    id: "sync",
    label: "Sync",
};
const HLS: SettingGroup = SettingGroup {
    id: "hls",
    label: "HLS",
};

const fn setting(
    key: &'static str,
    group: SettingGroup,
    label: &'static str,
    description: &'static str,
    apply: ApplyMode,
    kind: Kind,
) -> SettingDefinition {
    SettingDefinition {
        key,
        group,
        label,
        description,
        apply,
        kind,
    }
}

pub(super) static ALL: &[SettingDefinition] = &[
    setting(
        "published_url",
        SERVER,
        "Published URL",
        "Public http(s) origin clients use to reach this server.",
        Live,
        Kind::NullableOrigin,
    ),
    setting(
        "covers_path",
        SERVER,
        "Covers path",
        "Directory cover images are stored in; relative paths resolve under the data directory.",
        Live,
        Kind::Path {
            default: BootConfig::default_covers_path,
        },
    ),
    setting(
        "cors.allowed_origins",
        CORS,
        "Allowed origins",
        "Origins allowed by CORS; use * to allow any origin.",
        Live,
        Kind::OriginList { default: &[] },
    ),
    setting(
        "rate_limit.enabled",
        RATE_LIMIT,
        "Enabled",
        "Apply request rate limits.",
        RestartRequired,
        Kind::Bool { default: true },
    ),
    setting(
        "rate_limit.trusted_proxies",
        RATE_LIMIT,
        "Trusted proxies",
        "IP addresses whose forwarded client address is trusted for rate limiting.",
        RestartRequired,
        Kind::IpList {
            default: &["127.0.0.1", "::1"],
        },
    ),
    setting(
        "rate_limit.global_per_minute",
        RATE_LIMIT,
        "Global requests per minute",
        "Sustained requests per minute allowed per client.",
        RestartRequired,
        Kind::U32 { default: 1_200 },
    ),
    setting(
        "rate_limit.global_burst",
        RATE_LIMIT,
        "Global burst",
        "Burst allowance on top of the global per-minute limit.",
        RestartRequired,
        Kind::U32 { default: 300 },
    ),
    setting(
        "rate_limit.authenticated_per_minute",
        RATE_LIMIT,
        "Authenticated requests per minute",
        "Sustained requests per minute allowed per authenticated principal.",
        RestartRequired,
        Kind::U32 { default: 600 },
    ),
    setting(
        "rate_limit.authenticated_burst",
        RATE_LIMIT,
        "Authenticated burst",
        "Burst allowance on top of the authenticated per-minute limit.",
        RestartRequired,
        Kind::U32 { default: 120 },
    ),
    setting(
        "rate_limit.login_per_minute",
        RATE_LIMIT,
        "Login attempts per minute",
        "Sustained login attempts per minute allowed per client.",
        RestartRequired,
        Kind::U32 { default: 10 },
    ),
    setting(
        "rate_limit.login_burst",
        RATE_LIMIT,
        "Login burst",
        "Burst allowance on top of the login per-minute limit.",
        RestartRequired,
        Kind::U32 { default: 3 },
    ),
    setting(
        "auth.enabled",
        AUTH,
        "Enabled",
        "Require authentication for API access.",
        Live,
        Kind::Bool { default: true },
    ),
    setting(
        "auth.allow_default_login_when_disabled",
        AUTH,
        "Allow default login when disabled",
        "Let the default user log in while authentication is disabled.",
        Live,
        Kind::Bool { default: true },
    ),
    setting(
        "auth.session_ttl_seconds",
        AUTH,
        "Session TTL (seconds)",
        "How long a session stays valid without activity.",
        Live,
        Kind::U64 { default: 2_592_000 },
    ),
    setting(
        "sync.interval_secs",
        SYNC,
        "Provider sync interval (seconds)",
        "Seconds between provider sync runs; 0 disables the loop.",
        Live,
        Kind::U64 { default: 0 },
    ),
    setting(
        "hls.temp_disk_budget_bytes",
        HLS,
        "Temporary disk budget (bytes)",
        "Upper bound for HLS temporary files; unset means no budget.",
        Live,
        Kind::NullableU64,
    ),
    setting(
        "hls.cleanup_startup_purge",
        HLS,
        "Purge temporary files at startup",
        "Delete leftover HLS temporary files when the server starts.",
        RestartRequired,
        Kind::Bool { default: true },
    ),
    setting(
        "hls.max_concurrent_transcodes",
        HLS,
        "Max concurrent transcodes",
        "Concurrent HLS transcodes allowed; 0 means unlimited.",
        Live,
        Kind::U32 { default: 0 },
    ),
];
