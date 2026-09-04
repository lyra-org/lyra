// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::{
    Kind,
    SettingDefinition,
};
use crate::config::BootConfig;

const fn setting(key: &'static str, kind: Kind) -> SettingDefinition {
    SettingDefinition { key, kind }
}

pub(super) static ALL: &[SettingDefinition] = &[
    setting("published_url", Kind::NullableOrigin),
    setting(
        "covers_path",
        Kind::Path {
            default: BootConfig::default_covers_path,
        },
    ),
    setting("cors.allowed_origins", Kind::OriginList { default: &[] }),
    setting("rate_limit.enabled", Kind::Bool { default: true }),
    setting(
        "rate_limit.trusted_proxies",
        Kind::IpList {
            default: &["127.0.0.1", "::1"],
        },
    ),
    setting("rate_limit.global_per_minute", Kind::U32 { default: 1_200 }),
    setting("rate_limit.global_burst", Kind::U32 { default: 300 }),
    setting(
        "rate_limit.authenticated_per_minute",
        Kind::U32 { default: 600 },
    ),
    setting("rate_limit.authenticated_burst", Kind::U32 { default: 120 }),
    setting("rate_limit.login_per_minute", Kind::U32 { default: 10 }),
    setting("rate_limit.login_burst", Kind::U32 { default: 3 }),
    setting("auth.enabled", Kind::Bool { default: true }),
    setting(
        "auth.allow_default_login_when_disabled",
        Kind::Bool { default: true },
    ),
    setting("auth.session_ttl_seconds", Kind::U64 { default: 2_592_000 }),
    setting("sync.interval_secs", Kind::U64 { default: 0 }),
    setting("hls.temp_disk_budget_bytes", Kind::NullableU64),
    setting("hls.cleanup_startup_purge", Kind::Bool { default: true }),
    setting("hls.max_concurrent_transcodes", Kind::U32 { default: 0 }),
];
