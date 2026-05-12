// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc,
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

use crate::config::{
    Config,
    RateLimitConfig,
};

const CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
const IDLE_BUCKET_TTL: Duration = Duration::from_secs(600);
const UNKNOWN_CLIENT: &str = "unknown";

pub(crate) fn apply(router: Router, config: &Config) -> Router {
    if !config.rate_limit.enabled {
        return router;
    }

    let limiter = Arc::new(RateLimiter::new(&config.rate_limit));
    router.layer(middleware::from_fn_with_state(limiter, enforce))
}

async fn enforce(
    State(limiter): State<Arc<RateLimiter>>,
    request: Request,
    next: Next,
) -> Response {
    match limiter.check(&request) {
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

#[derive(Clone, Copy)]
struct RuntimeConfig {
    global: Option<Policy>,
    authenticated: Option<Policy>,
    login: Option<Policy>,
}

impl RuntimeConfig {
    fn from_config(config: &RateLimitConfig) -> Self {
        Self {
            global: Policy::new(config.global_per_minute, config.global_burst),
            authenticated: Policy::new(config.authenticated_per_minute, config.authenticated_burst),
            login: Policy::new(config.login_per_minute, config.login_burst),
        }
    }
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

    fn check(&self, request: &Request) -> Result<(), Duration> {
        let checks = self.checks_for_request(request);
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
                    .or_insert_with(|| Bucket::new(check.policy, now));
                bucket.refill(check.policy, now);
                check.policy.retry_after(bucket.tokens)
            })
            .max();

        if let Some(retry_after) = retry_after {
            return Err(retry_after);
        }

        for check in checks {
            if let Some(bucket) = state.buckets.get_mut(&check.key) {
                bucket.tokens -= 1.0;
            }
        }

        Ok(())
    }

    fn checks_for_request(&self, request: &Request) -> Vec<Check> {
        let method = request.method();
        if *method == Method::OPTIONS {
            return Vec::new();
        }

        let path = request.uri().path();
        if !is_api_path(path) || is_unmetered_path(path) {
            return Vec::new();
        }

        let client = client_key(request);
        let mut checks = Vec::with_capacity(2);
        if let Some(policy) = self.config.global {
            checks.push(Check {
                key: BucketKey::Global(client.clone()),
                policy,
            });
        }

        if is_login_request(method, path) {
            if let Some(policy) = self.config.login {
                checks.push(Check {
                    key: BucketKey::Login(client),
                    policy,
                });
            }
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
    Global(String),
    Authenticated(String),
    Login(String),
}

struct Bucket {
    tokens: f64,
    updated_at: Instant,
    last_seen: Instant,
}

impl Bucket {
    fn new(policy: Policy, now: Instant) -> Self {
        Self {
            tokens: policy.capacity(),
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

fn is_unmetered_path(path: &str) -> bool {
    path == "/api/covers"
        || path.starts_with("/api/covers/")
        || path == "/api/download"
        || path.starts_with("/api/download/")
        || path == "/api/stream"
        || path.starts_with("/api/stream/")
}

fn is_login_request(method: &Method, path: &str) -> bool {
    *method == Method::POST && path == "/api/users/login"
}

fn client_key(request: &Request) -> String {
    request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| addr.ip().to_string())
        .unwrap_or_else(|| UNKNOWN_CLIENT.to_string())
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
    use axum::{
        body::Body,
        http::{
            Request,
            StatusCode,
            header,
        },
        routing::{
            get,
            post,
        },
    };
    use tower::ServiceExt;

    use super::*;

    fn rate_limit_config() -> RateLimitConfig {
        RateLimitConfig {
            enabled: true,
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
            ..Config::default()
        }
    }

    fn request(method: Method, path: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(path)
            .body(Body::empty())
            .expect("request should build")
    }

    #[tokio::test]
    async fn login_limit_returns_retry_after_only() -> anyhow::Result<()> {
        let mut rate_limit = rate_limit_config();
        rate_limit.login_per_minute = 60;
        rate_limit.login_burst = 1;
        let app = apply(
            Router::new().route("/api/users/login", post(|| async { "ok" })),
            &config_with_rate_limit(rate_limit),
        );

        let first = app
            .clone()
            .oneshot(request(Method::POST, "/api/users/login"))
            .await?;
        assert_eq!(first.status(), StatusCode::OK);

        let second = app
            .oneshot(request(Method::POST, "/api/users/login"))
            .await?;
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            second.headers().get(header::RETRY_AFTER),
            Some(&HeaderValue::from_static("1"))
        );
        assert!(second.headers().get("x-ratelimit-limit").is_none());
        assert!(second.headers().get("x-ratelimit-remaining").is_none());
        assert!(second.headers().get("x-ratelimit-reset").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn global_limit_applies_to_api_routes() -> anyhow::Result<()> {
        let mut rate_limit = rate_limit_config();
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
    async fn unmetered_routes_skip_rate_limit() -> anyhow::Result<()> {
        let mut rate_limit = rate_limit_config();
        rate_limit.global_per_minute = 60;
        rate_limit.global_burst = 1;
        let app = apply(
            Router::new()
                .route("/api/covers/cover-1", get(|| async { "ok" }))
                .route("/api/stream/track-1", get(|| async { "ok" })),
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

        let cover_first = app
            .clone()
            .oneshot(request(Method::GET, "/api/covers/cover-1"))
            .await?;
        let cover_second = app
            .oneshot(request(Method::GET, "/api/covers/cover-1"))
            .await?;

        assert_eq!(cover_first.status(), StatusCode::OK);
        assert_eq!(cover_second.status(), StatusCode::OK);
        Ok(())
    }
}
