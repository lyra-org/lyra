// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Token-bucket primitives and login throttling, shared with the HTTP request
//! limiter in `routes::rate_limit`.

pub(crate) mod code_guesses;
pub(crate) mod password_checks;

use std::{
    collections::HashMap,
    sync::{
        LazyLock,
        Mutex,
    },
    time::{
        Duration,
        Instant,
    },
};

use crate::services::auth::{
    CredentialHolder,
    caller::ClientAddress,
};

const CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
const IDLE_BUCKET_TTL: Duration = Duration::from_secs(600);

#[derive(Clone, Copy)]
pub(crate) struct Policy {
    burst: u32,
    refill_per_second: f64,
}

impl Policy {
    pub(crate) fn new(per_minute: u32, burst: u32) -> Option<Self> {
        if per_minute == 0 || burst == 0 {
            return None;
        }

        Some(Self {
            burst,
            refill_per_second: per_minute as f64 / 60.0,
        })
    }

    pub(crate) fn capacity(self) -> f64 {
        self.burst as f64
    }

    pub(crate) fn retry_after(self, tokens: f64) -> Option<Duration> {
        if tokens >= 1.0 {
            return None;
        }

        let seconds = ((1.0 - tokens) / self.refill_per_second).ceil().max(1.0) as u64;
        Some(Duration::from_secs(seconds))
    }
}

static LOGIN_LIMITER: LazyLock<Mutex<LimiterState>> =
    LazyLock::new(|| Mutex::new(LimiterState::new()));

/// Charges one login attempt against the client's brute-force budget. Lives in the service layer
/// so every credential-verifying caller is covered, not just path-matched REST routes.
pub(crate) fn check_login_rate(client: ClientAddress) -> Result<(), Duration> {
    let config = crate::services::STATE.startup_config.get();
    if !config.rate_limit.enabled {
        return Ok(());
    }
    let Some(policy) = Policy::new(
        config.rate_limit.login_per_minute,
        config.rate_limit.login_burst,
    ) else {
        return Ok(());
    };

    check_login_bucket(&LOGIN_LIMITER, policy, client, Instant::now())
}

fn check_login_bucket(
    limiter: &Mutex<LimiterState>,
    policy: Policy,
    client: ClientAddress,
    now: Instant,
) -> Result<(), Duration> {
    let mut state = limiter.lock().expect("login rate limiter state poisoned");
    state.cleanup(now);

    let bucket = state
        .buckets
        .entry(BucketKey::Login(client))
        .or_insert_with(|| Bucket::new(policy.capacity(), now));
    bucket.refill(policy, now);
    if let Some(retry_after) = policy.retry_after(bucket.tokens) {
        return Err(retry_after);
    }

    bucket.tokens -= 1.0;
    Ok(())
}

pub(crate) struct LimiterState {
    pub(crate) buckets: HashMap<BucketKey, Bucket>,
    last_cleanup: Instant,
}

impl LimiterState {
    pub(crate) fn new() -> Self {
        Self {
            buckets: HashMap::new(),
            last_cleanup: Instant::now(),
        }
    }

    pub(crate) fn cleanup(&mut self, now: Instant) {
        if now.saturating_duration_since(self.last_cleanup) < CLEANUP_INTERVAL {
            return;
        }

        self.buckets
            .retain(|_, bucket| now.saturating_duration_since(bucket.last_seen) < IDLE_BUCKET_TTL);
        self.last_cleanup = now;
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum BucketKey {
    Client(ClientAddress),
    Credential(CredentialHolder),
    User(String),
    Authenticated(CredentialHolder),
    Lookup(ClientAddress),
    Login(ClientAddress),
    /// A hash of the account name a password was submitted for.
    LoginName(u64),
}

pub(crate) struct Bucket {
    pub(crate) tokens: f64,
    updated_at: Instant,
    last_seen: Instant,
}

impl Bucket {
    pub(crate) fn new(capacity: f64, now: Instant) -> Self {
        Self {
            tokens: capacity,
            updated_at: now,
            last_seen: now,
        }
    }

    pub(crate) fn refill(&mut self, policy: Policy, now: Instant) {
        let elapsed = now.saturating_duration_since(self.updated_at).as_secs_f64();
        if elapsed > 0.0 {
            self.tokens = (self.tokens + elapsed * policy.refill_per_second).min(policy.capacity());
            self.updated_at = now;
        }
        self.last_seen = now;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn address(raw: &str) -> ClientAddress {
        ClientAddress::new(raw.parse().expect("valid ip"))
    }

    #[tokio::test]
    async fn login_policy_stays_at_startup_values() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let mut startup = (*crate::services::STATE.config()).clone();
        startup.rate_limit.enabled = true;
        startup.rate_limit.login_per_minute = 1;
        startup.rate_limit.login_burst = 1;
        startup.rate_limit.trusted_proxies =
            crate::services::settings::server::TrustedProxies::none();
        crate::services::STATE
            .startup_config
            .replace(Arc::new(startup.clone()));
        let client = address("192.0.2.200");
        LOGIN_LIMITER.lock().unwrap().buckets.clear();
        assert!(check_login_rate(client).is_ok());
        startup.rate_limit.enabled = false;
        crate::services::testing::publish_config(startup);
        assert!(!crate::services::STATE.config().rate_limit.enabled);
        assert!(check_login_rate(client).is_err());
        Ok(())
    }

    #[test]
    fn login_bucket_enforces_burst_per_client() {
        let limiter = Mutex::new(LimiterState::new());
        let policy = Policy::new(60, 2).expect("policy should be enabled");
        let now = Instant::now();

        let client = address("203.0.113.5");

        assert!(check_login_bucket(&limiter, policy, client, now).is_ok());
        assert!(check_login_bucket(&limiter, policy, client, now).is_ok());
        let retry_after = check_login_bucket(&limiter, policy, client, now)
            .expect_err("third attempt should throttle");
        assert_eq!(retry_after, Duration::from_secs(1));
        assert!(check_login_bucket(&limiter, policy, address("203.0.113.6"), now).is_ok());
    }

    #[test]
    fn login_bucket_groups_ipv6_clients_by_their_64() {
        let limiter = Mutex::new(LimiterState::new());
        let policy = Policy::new(60, 1).expect("policy should be enabled");
        let now = Instant::now();

        assert!(check_login_bucket(&limiter, policy, address("2001:db8:1:2::1"), now).is_ok());
        assert!(
            check_login_bucket(&limiter, policy, address("2001:db8:1:2:ffff::2"), now).is_err(),
            "rotating addresses within a /64 buys no more attempts"
        );
        assert!(check_login_bucket(&limiter, policy, address("2001:db8:1:3::1"), now).is_ok());
    }
}
