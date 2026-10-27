// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        HashMap,
        HashSet,
    },
    net::{
        IpAddr,
        SocketAddr,
    },
    sync::{
        Arc,
        LazyLock,
        Mutex,
    },
    time::{
        Duration,
        Instant,
    },
};

use axum::{
    Router,
    extract::{
        ConnectInfo,
        Request,
        State,
    },
    http::{
        HeaderValue,
        Method,
        StatusCode,
        header,
    },
    middleware::{
        self,
        Next,
    },
    response::{
        IntoResponse,
        Response,
    },
};

use crate::services::{
    auth::caller::ClientAddress,
    settings::server::{
        Config,
        RateLimitConfig,
    },
};

const CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
const IDLE_BUCKET_TTL: Duration = Duration::from_secs(600);

/// Resolves each request's [`ClientAddress`] into its extensions and, when rate limiting is
/// enabled, limits the request.
pub(crate) fn apply(router: Router, config: &Config) -> Router {
    let limiter = Arc::new(RateLimiter::new(&config.rate_limit));
    router.layer(middleware::from_fn_with_state(limiter, enforce))
}

async fn enforce(
    State(limiter): State<Arc<RateLimiter>>,
    mut request: Request,
    next: Next,
) -> Response {
    let client = ClientAddress::resolve(
        peer_addr(&request),
        request.headers(),
        &limiter.config.trusted_proxies,
    );
    request.extensions_mut().insert(client);
    match limiter.check(&request, client) {
        Ok(()) => next.run(request).await,
        Err(retry_after) => too_many_requests_response(retry_after),
    }
}

fn too_many_requests_response(retry_after: Duration) -> Response {
    let seconds = retry_after.as_secs().max(1).to_string();
    let mut response = (StatusCode::TOO_MANY_REQUESTS, "Error: too many requests").into_response();
    response.headers_mut().insert(
        header::RETRY_AFTER,
        HeaderValue::from_str(&seconds).expect("retry-after seconds should be a valid header"),
    );
    response
}

#[derive(Clone, Copy)]
struct Policy {
    burst: u32,
    refill_per_second: f64,
}

impl Policy {
    fn new(per_minute: u32, burst: u32) -> Option<Self> {
        if per_minute == 0 || burst == 0 {
            return None;
        }

        Some(Self {
            burst,
            refill_per_second: per_minute as f64 / 60.0,
        })
    }

    fn capacity(self) -> f64 {
        self.burst as f64
    }

    fn retry_after(self, tokens: f64) -> Option<Duration> {
        if tokens >= 1.0 {
            return None;
        }

        let seconds = ((1.0 - tokens) / self.refill_per_second).ceil().max(1.0) as u64;
        Some(Duration::from_secs(seconds))
    }
}

#[derive(Clone)]
struct RuntimeConfig {
    enabled: bool,
    trusted_proxies: HashSet<IpAddr>,
    global: Option<Policy>,
    authenticated: Option<Policy>,
}

impl RuntimeConfig {
    fn from_config(config: &RateLimitConfig) -> Self {
        Self {
            enabled: config.enabled,
            trusted_proxies: config.trusted_proxies.iter().copied().collect(),
            global: Policy::new(config.global_per_minute, config.global_burst),
            authenticated: Policy::new(config.authenticated_per_minute, config.authenticated_burst),
        }
    }
}

static LOGIN_LIMITER: LazyLock<Mutex<LimiterState>> =
    LazyLock::new(|| Mutex::new(LimiterState::new()));

/// Charges one login attempt against the client's brute-force budget. Lives in
/// the service layer so every credential-verifying caller is covered, not just
/// path-matched REST routes.
pub(crate) fn check_login_rate(client: ClientAddress) -> Result<(), Duration> {
    let config = crate::STATE.startup_config.get();
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

struct RateLimiter {
    config: RuntimeConfig,
    state: Mutex<LimiterState>,
}

impl RateLimiter {
    fn new(config: &RateLimitConfig) -> Self {
        Self {
            config: RuntimeConfig::from_config(config),
            state: Mutex::new(LimiterState::new()),
        }
    }

    fn check(&self, request: &Request, client: ClientAddress) -> Result<(), Duration> {
        if !self.config.enabled {
            return Ok(());
        }
        let path = request.uri().path();
        let plugin_route = !is_api_path(path) && crate::plugins::api::is_plugin_route_path(path);
        let checks = self.checks_for_request(request, client, plugin_route);
        if checks.is_empty() {
            return Ok(());
        }

        let now = Instant::now();
        let mut state = self.state.lock().expect("rate limiter state poisoned");
        state.cleanup(now);

        let retry_after = checks
            .iter()
            .filter_map(|check| {
                let bucket = state
                    .buckets
                    .entry(check.key.clone())
                    .or_insert_with(|| Bucket::new(check.policy.capacity(), now));
                bucket.refill(check.policy, now);
                check.policy.retry_after(bucket.tokens)
            })
            .max();

        if let Some(retry_after) = retry_after {
            return Err(retry_after);
        }

        for check in checks {
            let bucket = state
                .buckets
                .get_mut(&check.key)
                .expect("rate limit bucket should exist before token consumption");
            bucket.tokens -= 1.0;
        }

        Ok(())
    }

    fn checks_for_request(
        &self,
        request: &Request,
        client: ClientAddress,
        plugin_route: bool,
    ) -> Vec<Check> {
        let method = request.method();
        if *method == Method::OPTIONS {
            return Vec::new();
        }

        // Plugin surfaces serve media-heavy clients; like core media paths
        // they get the per-client global policy only.
        if plugin_route {
            let Some(policy) = self.config.global else {
                return Vec::new();
            };
            return vec![Check {
                key: BucketKey::Global(client),
                policy,
            }];
        }

        let path = request.uri().path();
        if !is_api_path(path) || is_streaming_path(path) {
            return Vec::new();
        }

        let mut checks = Vec::with_capacity(2);
        if let Some(policy) = self.config.global {
            checks.push(Check {
                key: BucketKey::Global(client),
                policy,
            });
        }

        if is_media_path(path) {
            return checks;
        }

        if let Some(credential) = credential_key(request)
            && let Some(policy) = self.config.authenticated
        {
            checks.push(Check {
                key: BucketKey::Authenticated(credential),
                policy,
            });
        }

        checks
    }
}

struct LimiterState {
    buckets: HashMap<BucketKey, Bucket>,
    last_cleanup: Instant,
}

impl LimiterState {
    fn new() -> Self {
        Self {
            buckets: HashMap::new(),
            last_cleanup: Instant::now(),
        }
    }

    fn cleanup(&mut self, now: Instant) {
        if now.saturating_duration_since(self.last_cleanup) < CLEANUP_INTERVAL {
            return;
        }

        self.buckets
            .retain(|_, bucket| now.saturating_duration_since(bucket.last_seen) < IDLE_BUCKET_TTL);
        self.last_cleanup = now;
    }
}

struct Check {
    key: BucketKey,
    policy: Policy,
}

#[derive(Clone, Eq, Hash, PartialEq)]
enum BucketKey {
    Global(ClientAddress),
    Authenticated(String),
    Login(ClientAddress),
}

struct Bucket {
    tokens: f64,
    updated_at: Instant,
    last_seen: Instant,
}

impl Bucket {
    fn new(capacity: f64, now: Instant) -> Self {
        Self {
            tokens: capacity,
            updated_at: now,
            last_seen: now,
        }
    }

    fn refill(&mut self, policy: Policy, now: Instant) {
        let elapsed = now.saturating_duration_since(self.updated_at).as_secs_f64();
        if elapsed > 0.0 {
            self.tokens = (self.tokens + elapsed * policy.refill_per_second).min(policy.capacity());
            self.updated_at = now;
        }
        self.last_seen = now;
    }
}

fn is_api_path(path: &str) -> bool {
    path == "/api" || path.starts_with("/api/")
}

fn is_streaming_path(path: &str) -> bool {
    path == "/api/stream" || path.starts_with("/api/stream/")
}

fn is_media_path(path: &str) -> bool {
    path == "/api/covers"
        || path.starts_with("/api/covers/")
        || path == "/api/download"
        || path.starts_with("/api/download/")
}

fn peer_addr(request: &Request) -> Option<SocketAddr> {
    request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| *addr)
}

fn credential_key(request: &Request) -> Option<String> {
    let raw = request
        .headers()
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    Some(blake3::hash(trimmed.as_bytes()).to_hex().to_string())
}

#[cfg(test)]
mod tests {
    use std::net::{
        IpAddr,
        Ipv4Addr,
        SocketAddr,
    };

    use axum::{
        body::Body,
        extract::ConnectInfo,
        http::{
            HeaderValue,
            Method,
            Request,
            StatusCode,
            header,
        },
        routing::get,
    };
    use tower::ServiceExt;

    use super::*;

    fn address(raw: &str) -> ClientAddress {
        ClientAddress::new(raw.parse().expect("valid ip"))
    }

    fn disabled_rate_limit_config() -> RateLimitConfig {
        RateLimitConfig {
            enabled: true,
            trusted_proxies: Vec::new(),
            global_per_minute: 0,
            global_burst: 0,
            authenticated_per_minute: 0,
            authenticated_burst: 0,
            login_per_minute: 0,
            login_burst: 0,
        }
    }

    fn config_with_rate_limit(rate_limit: RateLimitConfig) -> Config {
        Config {
            rate_limit,
            ..Config::for_tests()
        }
    }

    fn request(method: Method, path: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(path)
            .body(Body::empty())
            .expect("request should build")
    }

    fn request_with_peer(method: Method, path: &str, peer_ip: impl Into<IpAddr>) -> Request<Body> {
        let mut request = request(method, path);
        request
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::new(peer_ip.into(), 4746)));
        request
    }

    fn request_with_forwarded_for(
        method: Method,
        path: &str,
        peer_ip: [u8; 4],
        forwarded_for: &str,
    ) -> Request<Body> {
        let mut request = request_with_peer(method, path, peer_ip);
        request.headers_mut().insert(
            "x-forwarded-for",
            HeaderValue::from_str(forwarded_for).expect("forwarded-for should be a valid header"),
        );
        request
    }

    #[tokio::test]
    async fn login_policy_stays_at_startup_values() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        crate::testing::init_default_test_state()?;
        let mut startup = (*crate::STATE.config()).clone();
        startup.rate_limit.enabled = true;
        startup.rate_limit.login_per_minute = 1;
        startup.rate_limit.login_burst = 1;
        startup.rate_limit.trusted_proxies.clear();
        crate::STATE
            .startup_config
            .replace(Arc::new(startup.clone()));
        let client = address("192.0.2.200");
        LOGIN_LIMITER.lock().unwrap().buckets.clear();
        assert!(check_login_rate(client).is_ok());
        startup.rate_limit.enabled = false;
        crate::testing::publish_config(startup);
        assert!(!crate::STATE.config().rate_limit.enabled);
        assert!(check_login_rate(client).is_err());
        Ok(())
    }

    #[test]
    fn plugin_routes_use_global_limit_only() {
        let mut rate_limit = disabled_rate_limit_config();
        rate_limit.global_per_minute = 60;
        rate_limit.global_burst = 1;
        rate_limit.authenticated_per_minute = 60;
        rate_limit.authenticated_burst = 1;
        let limiter = RateLimiter::new(&rate_limit);

        let mut plugin_request = request_with_peer(Method::GET, "/demo/Users", [203, 0, 113, 20]);
        plugin_request.headers_mut().insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Custom Token=abc"),
        );
        let client = address("203.0.113.20");
        let checks = limiter.checks_for_request(&plugin_request, client, true);
        assert_eq!(checks.len(), 1);
        assert!(matches!(checks[0].key, BucketKey::Global(_)));

        let static_request = request_with_peer(Method::GET, "/assets/app.js", [203, 0, 113, 20]);
        assert!(
            limiter
                .checks_for_request(&static_request, client, false)
                .is_empty()
        );
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

    #[tokio::test]
    async fn client_address_is_resolved_with_rate_limiting_disabled() -> anyhow::Result<()> {
        let mut rate_limit = disabled_rate_limit_config();
        rate_limit.enabled = false;
        let app = apply(
            Router::new().route(
                "/api/auth/login",
                axum::routing::post(
                    |axum::Extension(client): axum::Extension<ClientAddress>| async move {
                        client.to_string()
                    },
                ),
            ),
            &config_with_rate_limit(rate_limit),
        );

        let response = app
            .oneshot(request_with_peer(
                Method::POST,
                "/api/auth/login",
                "2001:db8:1:2::9".parse::<IpAddr>()?,
            ))
            .await?;

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(body, "2001:db8:1:2::/64");
        Ok(())
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

    #[tokio::test]
    async fn global_limit_applies_to_api_routes() -> anyhow::Result<()> {
        let mut rate_limit = disabled_rate_limit_config();
        rate_limit.global_per_minute = 60;
        rate_limit.global_burst = 1;
        let app = apply(
            Router::new().route("/api/server/public", get(|| async { "ok" })),
            &config_with_rate_limit(rate_limit),
        );

        let first = app
            .clone()
            .oneshot(request(Method::GET, "/api/server/public"))
            .await?;
        assert_eq!(first.status(), StatusCode::OK);

        let second = app
            .oneshot(request(Method::GET, "/api/server/public"))
            .await?;
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            second.headers().get(header::RETRY_AFTER),
            Some(&HeaderValue::from_static("1"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn trusted_proxy_uses_forwarded_client_ip() -> anyhow::Result<()> {
        let mut rate_limit = disabled_rate_limit_config();
        rate_limit.trusted_proxies = vec![IpAddr::V4(Ipv4Addr::LOCALHOST)];
        rate_limit.global_per_minute = 60;
        rate_limit.global_burst = 1;
        let app = apply(
            Router::new().route("/api/server/public", get(|| async { "ok" })),
            &config_with_rate_limit(rate_limit),
        );

        let first = app
            .clone()
            .oneshot(request_with_forwarded_for(
                Method::GET,
                "/api/server/public",
                [127, 0, 0, 1],
                "203.0.113.10",
            ))
            .await?;
        let second_client = app
            .clone()
            .oneshot(request_with_forwarded_for(
                Method::GET,
                "/api/server/public",
                [127, 0, 0, 1],
                "203.0.113.11",
            ))
            .await?;
        let first_client_again = app
            .oneshot(request_with_forwarded_for(
                Method::GET,
                "/api/server/public",
                [127, 0, 0, 1],
                "203.0.113.10",
            ))
            .await?;

        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(second_client.status(), StatusCode::OK);
        assert_eq!(first_client_again.status(), StatusCode::TOO_MANY_REQUESTS);
        Ok(())
    }

    #[tokio::test]
    async fn untrusted_proxy_ignores_forwarded_client_ip() -> anyhow::Result<()> {
        let mut rate_limit = disabled_rate_limit_config();
        rate_limit.trusted_proxies = vec![IpAddr::V4(Ipv4Addr::LOCALHOST)];
        rate_limit.global_per_minute = 60;
        rate_limit.global_burst = 1;
        let app = apply(
            Router::new().route("/api/server/public", get(|| async { "ok" })),
            &config_with_rate_limit(rate_limit),
        );

        let first = app
            .clone()
            .oneshot(request_with_forwarded_for(
                Method::GET,
                "/api/server/public",
                [198, 51, 100, 1],
                "203.0.113.10",
            ))
            .await?;
        let second = app
            .oneshot(request_with_forwarded_for(
                Method::GET,
                "/api/server/public",
                [198, 51, 100, 1],
                "203.0.113.11",
            ))
            .await?;

        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
        Ok(())
    }

    #[tokio::test]
    async fn ipv6_clients_share_a_request_budget_across_their_64() -> anyhow::Result<()> {
        let mut rate_limit = disabled_rate_limit_config();
        rate_limit.global_per_minute = 60;
        rate_limit.global_burst = 1;
        let app = apply(
            Router::new().route("/api/server/public", get(|| async { "ok" })),
            &config_with_rate_limit(rate_limit),
        );
        let from = |raw: &str| {
            request_with_peer(
                Method::GET,
                "/api/server/public",
                raw.parse::<IpAddr>().expect("valid ip"),
            )
        };

        let first = app.clone().oneshot(from("2001:db8:1:2::1")).await?;
        let same_64 = app.clone().oneshot(from("2001:db8:1:2:ffff::2")).await?;
        let other_64 = app.oneshot(from("2001:db8:1:3::1")).await?;

        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(same_64.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(other_64.status(), StatusCode::OK);
        Ok(())
    }

    #[tokio::test]
    async fn stream_routes_skip_rate_limit() -> anyhow::Result<()> {
        let mut rate_limit = disabled_rate_limit_config();
        rate_limit.global_per_minute = 60;
        rate_limit.global_burst = 1;
        let app = apply(
            Router::new().route("/api/stream/track-1", get(|| async { "ok" })),
            &config_with_rate_limit(rate_limit),
        );

        let first = app
            .clone()
            .oneshot(request(Method::GET, "/api/stream/track-1"))
            .await?;
        let second = app
            .clone()
            .oneshot(request(Method::GET, "/api/stream/track-1"))
            .await?;

        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(second.status(), StatusCode::OK);
        Ok(())
    }

    #[tokio::test]
    async fn media_routes_use_global_limit() -> anyhow::Result<()> {
        let mut rate_limit = disabled_rate_limit_config();
        rate_limit.global_per_minute = 60;
        rate_limit.global_burst = 1;
        let app = apply(
            Router::new()
                .route("/api/covers/cover-1", get(|| async { "ok" }))
                .route("/api/download/track-1", get(|| async { "ok" }))
                .route("/api/server/public", get(|| async { "ok" })),
            &config_with_rate_limit(rate_limit),
        );

        let cover_first = app
            .clone()
            .oneshot(request_with_peer(
                Method::GET,
                "/api/covers/cover-1",
                [203, 0, 113, 10],
            ))
            .await?;
        let cover_second = app
            .clone()
            .oneshot(request_with_peer(
                Method::GET,
                "/api/covers/cover-1",
                [203, 0, 113, 10],
            ))
            .await?;
        let download_first = app
            .clone()
            .oneshot(request_with_peer(
                Method::GET,
                "/api/download/track-1",
                [203, 0, 113, 11],
            ))
            .await?;
        let download_second = app
            .clone()
            .oneshot(request_with_peer(
                Method::GET,
                "/api/download/track-1",
                [203, 0, 113, 11],
            ))
            .await?;
        let metered_first = app
            .clone()
            .oneshot(request_with_peer(
                Method::GET,
                "/api/server/public",
                [203, 0, 113, 12],
            ))
            .await?;
        let metered_second = app
            .oneshot(request_with_peer(
                Method::GET,
                "/api/server/public",
                [203, 0, 113, 12],
            ))
            .await?;

        assert_eq!(cover_first.status(), StatusCode::OK);
        assert_eq!(cover_second.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(download_first.status(), StatusCode::OK);
        assert_eq!(download_second.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(metered_first.status(), StatusCode::OK);
        assert_eq!(metered_second.status(), StatusCode::TOO_MANY_REQUESTS);
        Ok(())
    }
}
