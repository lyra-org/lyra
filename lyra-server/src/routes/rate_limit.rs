// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The HTTP request rate limiter.

use std::{
    collections::HashSet,
    net::{
        IpAddr,
        SocketAddr,
    },
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
        Uri,
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

use super::plugin_api;
use crate::services::{
    auth::{
        caller::{
            self,
            ClientAddress,
            Identity,
        },
        media_tokens::{
            self,
            MediaTokenPurpose,
        },
    },
    rate_limit::{
        Bucket,
        BucketKey,
        LimiterState,
        Policy,
    },
    settings::server::{
        Config,
        RateLimitConfig,
    },
};

/// How many credentials' global budgets one user may spend together: enough for a handful of
/// devices all busy at once, while minting more sessions or API keys buys no more.
const USER_CREDENTIALS: u32 = 4;

/// Resolves each request's [`ClientAddress`] into its extensions and, when rate limiting is
/// enabled, charges the request to its caller.
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
    let Some(route) = limiter.route(&request) else {
        return next.run(request).await;
    };
    let charged = match route {
        // A plugin's protocol authenticates its own way, after this runs, so its callers are
        // only known by their address.
        Route::Plugin => limiter.charge(limiter.checks(route, client, None)),
        Route::Api | Route::Media => {
            let bearer_hash = caller::bearer_hash(request.headers());
            let uri = request.uri().clone();
            limiter
                .identify(bearer_hash, &uri, client)
                .await
                .and_then(|identity| {
                    limiter.charge(limiter.checks(route, client, identity.as_ref()))
                })
        }
    };
    match charged {
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

#[derive(Clone)]
struct RuntimeConfig {
    enabled: bool,
    trusted_proxies: HashSet<IpAddr>,
    global: Option<Policy>,
    user: Option<Policy>,
    authenticated: Option<Policy>,
}

impl RuntimeConfig {
    fn from_config(config: &RateLimitConfig) -> Self {
        Self {
            enabled: config.enabled,
            trusted_proxies: config.trusted_proxies.iter().copied().collect(),
            global: Policy::new(config.global_per_minute, config.global_burst),
            user: Policy::new(
                config.global_per_minute.saturating_mul(USER_CREDENTIALS),
                config.global_burst.saturating_mul(USER_CREDENTIALS),
            ),
            authenticated: Policy::new(config.authenticated_per_minute, config.authenticated_burst),
        }
    }
}

/// Which limits a request is subject to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Route {
    /// A core API route.
    Api,
    /// A core route serving covers or downloads, which clients fetch in bulk.
    Media,
    /// A route a plugin registered.
    Plugin,
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

    /// The limits `request` is subject to, or `None` when it is not limited.
    fn route(&self, request: &Request) -> Option<Route> {
        if !self.config.enabled || request.method() == Method::OPTIONS {
            return None;
        }
        let path = request.uri().path();
        if !is_api_path(path) {
            return plugin_api::is_plugin_route_path(path).then_some(Route::Plugin);
        }
        // A playback fetches its stream in many range or segment requests, and what is costly
        // about streaming, transcoding, is bounded by the transcode capacity instead.
        if is_streaming_path(path) {
            return None;
        }
        Some(if is_media_path(path) {
            Route::Media
        } else {
            Route::Api
        })
    }

    /// Who sent a core request. With authentication disabled everyone is the same user, so callers
    /// are told apart by address alone.
    async fn identify(
        &self,
        bearer_hash: Option<String>,
        uri: &Uri,
        client: ClientAddress,
    ) -> Result<Option<Identity>, Duration> {
        if !crate::services::STATE.config().auth.enabled {
            return Ok(None);
        }
        let identity = match bearer_hash {
            Some(hash) => match caller::remembered_identity(&hash) {
                Some(identity) => identity,
                None => {
                    // Looking up a credential not seen lately is charged to its address, so a
                    // flood of made-up credentials is held to that budget before reaching the
                    // database. It is a budget of its own, which the address's anonymous
                    // requests cannot spend for the users behind it.
                    if let Some(policy) = self.config.global {
                        self.charge(vec![Check {
                            key: BucketKey::Lookup(client),
                            policy,
                        }])?;
                    }
                    caller::look_up_identity(&hash).await
                }
            },
            None => None,
        };
        Ok(identity.or_else(|| download_token_identity(uri)))
    }

    fn charge(&self, checks: Vec<Check>) -> Result<(), Duration> {
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

    /// An anonymous request is charged to its address. A user's is charged to the credential's
    /// global budget and the user's larger one, and their API calls, but not their media
    /// fetches, also to the credential's stricter authenticated budget.
    fn checks(
        &self,
        route: Route,
        client: ClientAddress,
        identity: Option<&Identity>,
    ) -> Vec<Check> {
        let Some(identity) = identity else {
            return self
                .config
                .global
                .map(|policy| Check {
                    key: BucketKey::Client(client),
                    policy,
                })
                .into_iter()
                .collect();
        };
        let mut checks = Vec::with_capacity(3);
        if let Some(policy) = self.config.global {
            checks.push(Check {
                key: BucketKey::Credential(identity.clone()),
                policy,
            });
        }
        if let Some(policy) = self.config.user {
            checks.push(Check {
                key: BucketKey::User(identity.user.clone()),
                policy,
            });
        }
        if route == Route::Api
            && let Some(policy) = self.config.authenticated
        {
            checks.push(Check {
                key: BucketKey::Authenticated(identity.clone()),
                policy,
            });
        }
        checks
    }
}

struct Check {
    key: BucketKey,
    policy: Policy,
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

/// The identity of the media token a download request carries, when it was issued to download
/// that track. Only downloads take a media token among the limited routes; elsewhere one is
/// ignored, so a token leaked with a stream URL cannot spend its user's budget.
fn download_token_identity(uri: &Uri) -> Option<Identity> {
    let track_id = uri.path().strip_prefix("/api/download/")?;
    let (_, token) = url::form_urlencoded::parse(uri.query()?.as_bytes())
        .find(|(key, _)| key == "media_token")?;
    media_tokens::token_identity(&token, MediaTokenPurpose::Download, track_id)
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

    fn identity(user: &str, session: &str) -> Identity {
        Identity {
            user: user.to_string(),
            credential: crate::services::auth::AuthCredential::Session {
                public_id: session.to_string(),
            },
        }
    }

    #[test]
    fn requests_are_charged_to_their_address_or_credential() {
        let mut rate_limit = disabled_rate_limit_config();
        rate_limit.global_per_minute = 60;
        rate_limit.global_burst = 1;
        rate_limit.authenticated_per_minute = 60;
        rate_limit.authenticated_burst = 1;
        let limiter = RateLimiter::new(&rate_limit);
        let client = address("203.0.113.20");
        let laptop = identity("amy", "laptop");

        let keys = |route, identity: Option<&Identity>| {
            limiter
                .checks(route, client, identity)
                .into_iter()
                .map(|check| check.key)
                .collect::<Vec<_>>()
        };
        assert_eq!(keys(Route::Plugin, None), [BucketKey::Client(client)]);
        assert_eq!(keys(Route::Api, None), [BucketKey::Client(client)]);
        assert_eq!(
            keys(Route::Media, Some(&laptop)),
            [
                BucketKey::Credential(laptop.clone()),
                BucketKey::User("amy".to_string()),
            ]
        );
        assert_eq!(
            keys(Route::Api, Some(&laptop)),
            [
                BucketKey::Credential(laptop.clone()),
                BucketKey::User("amy".to_string()),
                BucketKey::Authenticated(laptop.clone()),
            ]
        );

        let static_request = request_with_peer(Method::GET, "/assets/app.js", [203, 0, 113, 20]);
        assert_eq!(limiter.route(&static_request), None);
    }

    #[test]
    fn a_users_credentials_have_their_own_budgets_under_a_shared_ceiling() {
        let mut rate_limit = disabled_rate_limit_config();
        rate_limit.global_per_minute = 60;
        rate_limit.global_burst = 1;
        let limiter = RateLimiter::new(&rate_limit);
        let client = address("203.0.113.20");
        let charge = |identity: &Identity| {
            limiter.charge(limiter.checks(Route::Media, client, Some(identity)))
        };

        assert!(charge(&identity("amy", "laptop")).is_ok());
        assert!(
            charge(&identity("amy", "laptop")).is_err(),
            "one credential spends only its own budget"
        );
        for device in ["phone", "tv", "desktop"] {
            assert!(
                charge(&identity("amy", device)).is_ok(),
                "a busy device leaves the user's others theirs"
            );
        }
        assert!(
            charge(&identity("amy", "fifth")).is_err(),
            "more credentials buy no more than the user's ceiling"
        );
        assert!(charge(&identity("bob", "laptop")).is_ok());
    }

    #[test]
    fn anonymous_clients_share_no_budget() {
        let mut rate_limit = disabled_rate_limit_config();
        rate_limit.global_per_minute = 60;
        rate_limit.global_burst = 1;
        let limiter = RateLimiter::new(&rate_limit);

        for host in 1..=64 {
            let client = address(&format!("203.0.113.{host}"));
            assert!(
                limiter
                    .charge(limiter.checks(Route::Api, client, None))
                    .is_ok(),
                "no number of other clients exhausts a fresh one's budget"
            );
        }
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

    #[tokio::test]
    async fn global_limit_applies_to_api_routes() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
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
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
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
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
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
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
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

    async fn initialize_user_runtime()
    -> anyhow::Result<(agdb::DbId, crate::services::auth::Principal)> {
        crate::plugins::testing::init_library_test_state(std::path::PathBuf::from(".")).await?;
        let user_db_id = {
            let mut db = crate::services::STATE.db.write().await;
            lyra_db::users::create(&mut db, &lyra_db::fixtures::test_user("budget")?)?
        };
        Ok((
            user_db_id,
            crate::services::testing::user_principal(user_db_id).await?,
        ))
    }

    async fn session_token(principal: &crate::services::auth::Principal) -> anyhow::Result<String> {
        let metadata = crate::services::auth::sessions::SessionMetadata {
            user_agent: None,
            client_name: None,
        };
        Ok(
            crate::services::auth::sessions::create_session_for_user(principal, metadata)
                .await?
                .token,
        )
    }

    fn bearer_request(path: &str, token: &str, peer: [u8; 4]) -> Request<Body> {
        let mut request = request_with_peer(Method::GET, path, peer);
        request.headers_mut().insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).expect("valid header"),
        );
        request
    }

    async fn status(app: &Router, request: Request<Body>) -> anyhow::Result<StatusCode> {
        Ok(app.clone().oneshot(request).await?.status())
    }

    fn budget_app(burst: u32) -> Router {
        let mut rate_limit = disabled_rate_limit_config();
        rate_limit.global_per_minute = 60;
        rate_limit.global_burst = burst;
        apply(
            Router::new()
                .route("/api/server/public", get(|| async { "ok" }))
                .route("/api/download/{track_id}", get(|| async { "ok" })),
            &config_with_rate_limit(rate_limit),
        )
    }

    #[tokio::test]
    async fn credentials_are_charged_wherever_they_come_from() -> anyhow::Result<()> {
        use crate::services::{
            auth::media_tokens,
            playback_sources::SourceVersion,
        };

        let _guard = crate::services::testing::runtime_test_lock().await;
        let (_, principal) = initialize_user_runtime().await?;
        let laptop = session_token(&principal).await?;
        let phone = session_token(&principal).await?;
        let auth = crate::services::auth::resolve_auth_from_bearer(Some(&laptop))
            .await?
            .expect("session resolves");
        let media_token = media_tokens::issue_media_token(
            "track-1",
            media_tokens::MediaTokenPurpose::Download,
            media_tokens::MediaTokenHolder::new(&auth),
            SourceVersion::fixed("source"),
            media_tokens::DeliveryOptions::default(),
        )
        .token;
        let app = budget_app(1);

        assert_eq!(
            status(
                &app,
                bearer_request("/api/server/public", &laptop, [203, 0, 113, 1])
            )
            .await?,
            StatusCode::OK
        );
        assert_eq!(
            status(
                &app,
                bearer_request("/api/server/public", &laptop, [198, 51, 100, 2])
            )
            .await?,
            StatusCode::TOO_MANY_REQUESTS,
            "a credential's budget follows it across addresses"
        );
        assert_eq!(
            status(
                &app,
                bearer_request("/api/server/public", &phone, [192, 0, 2, 7])
            )
            .await?,
            StatusCode::OK,
            "the user's other session keeps its own budget"
        );
        assert_eq!(
            status(
                &app,
                request_with_peer(
                    Method::GET,
                    &format!("/api/download/track-1?media_token={media_token}"),
                    [192, 0, 2, 3],
                )
            )
            .await?,
            StatusCode::TOO_MANY_REQUESTS,
            "a download's media token counts against the session that obtained it"
        );
        for (path, host) in [
            (format!("/api/server/public?media_token={media_token}"), 4),
            (
                format!("/api/download/track-2?media_token={media_token}"),
                5,
            ),
        ] {
            assert_eq!(
                status(
                    &app,
                    request_with_peer(Method::GET, &path, [192, 0, 2, host])
                )
                .await?,
                StatusCode::OK,
                "a token is ignored where it does not apply, so {path} counts against its address"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn unidentified_credentials_count_against_their_address() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let (user_db_id, _) = initialize_user_runtime().await?;
        let expired = {
            let mut db = crate::services::STATE.db.write().await;
            let mut session = lyra_db::test_db::test_session(&crate::services::auth::hash_secret(
                "expired-session",
            ));
            session.expires_at = lyra_db::users::now_secs() - 1;
            lyra_db::users::login(&mut db, user_db_id, &session)?;
            "expired-session"
        };
        let app = budget_app(2);

        for token in ["made-up-credential", expired] {
            let peer = if token == expired {
                [198, 51, 100, 9]
            } else {
                [203, 0, 113, 9]
            };
            for _ in 0..2 {
                assert_eq!(
                    status(&app, bearer_request("/api/server/public", token, peer)).await?,
                    StatusCode::OK
                );
            }
            assert_eq!(
                status(
                    &app,
                    request_with_peer(Method::GET, "/api/server/public", peer)
                )
                .await?,
                StatusCode::TOO_MANY_REQUESTS,
                "{token} is charged to its address like an anonymous request"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn disabled_authentication_tells_callers_apart_by_address() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let (_, principal) = initialize_user_runtime().await?;
        let token = session_token(&principal).await?;
        let mut config = crate::services::STATE.config().as_ref().clone();
        config.auth.enabled = false;
        crate::services::testing::publish_config(config);
        let app = budget_app(1);

        assert_eq!(
            status(
                &app,
                bearer_request("/api/server/public", &token, [203, 0, 113, 1])
            )
            .await?,
            StatusCode::OK
        );
        assert_eq!(
            status(
                &app,
                bearer_request("/api/server/public", &token, [198, 51, 100, 2])
            )
            .await?,
            StatusCode::OK,
            "another address is another caller"
        );
        assert_eq!(
            status(
                &app,
                request_with_peer(Method::GET, "/api/server/public", [203, 0, 113, 1])
            )
            .await?,
            StatusCode::TOO_MANY_REQUESTS
        );
        Ok(())
    }

    #[tokio::test]
    async fn stream_routes_skip_rate_limit() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
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
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
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
