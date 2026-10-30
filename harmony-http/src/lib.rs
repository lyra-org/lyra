// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock as StdRwLock;
use std::time::{
    Duration,
    Instant,
    SystemTime,
};

use harmony_luau as luau;
use harmony_luau::{
    DescribeInterface,
    DescribeModule,
    DescribeTypeAlias,
    DescribeUserData,
    LuauType,
    LuauTypeInfo,
    render_definition_file_with_support,
};
use harmony_luau::{
    FieldDescriptor,
    InterfaceDescriptor,
};
use percent_encoding::{
    AsciiSet,
    NON_ALPHANUMERIC,
};
use std::collections::BTreeMap;
use std::sync::LazyLock;
use tokio::sync::{
    OwnedSemaphorePermit,
    RwLock,
    Semaphore,
};

/// Raw response body bytes that report as `string` in Luau type annotations.
#[derive(Clone, Debug, Default)]
struct BodyBytes(Vec<u8>);

#[derive(Clone, Debug, PartialEq, Eq)]
struct LuaBinaryInput(Vec<u8>);

impl LuauTypeInfo for LuaBinaryInput {
    fn luau_type() -> LuauType {
        LuauType::union(vec![String::luau_type(), LuauType::literal("buffer")])
    }
}

impl LuauTypeInfo for BodyBytes {
    fn luau_type() -> LuauType {
        String::luau_type()
    }
}

const URI_COMPONENT_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'!')
    .remove(b'~')
    .remove(b'*')
    .remove(b'\'')
    .remove(b'(')
    .remove(b')');

static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    let _ = rustls::crypto::ring::default_provider().install_default();
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("Failed to create HTTP client")
});

// TODO(provider-cooldown): persist per-domain state so cooldowns survive restart.
static RATE_LIMITER: LazyLock<Arc<RwLock<RateLimiter>>> =
    LazyLock::new(|| Arc::new(RwLock::new(RateLimiter::new())));
static CONCURRENCY_LIMITER: LazyLock<Arc<RwLock<ConcurrencyLimiter>>> =
    LazyLock::new(|| Arc::new(RwLock::new(ConcurrencyLimiter::new())));
static DEFAULT_USER_AGENT: LazyLock<StdRwLock<Option<String>>> =
    LazyLock::new(|| StdRwLock::new(None));

pub fn shared_client() -> &'static reqwest::Client {
    &HTTP_CLIENT
}

#[derive(Clone, Debug)]
struct RateLimitConfig {
    requests_per_second: f64,
    retry_status_codes: Vec<u16>,
    max_retries: u32,
    initial_backoff: Duration,
}

/// The plugin that set a limit, or `None` for a chunk outside any plugin.
type LimitOwner = Option<Arc<str>>;

/// Each owner's limit for one host. A stale limit belongs to a plugin run
/// that has ended: it still applies until it is swept, but no longer counts
/// as the plugin's own.
struct OwnedLimits<T> {
    entries: BTreeMap<LimitOwner, (T, bool)>,
}

impl<T> Default for OwnedLimits<T> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }
}

impl<T> OwnedLimits<T> {
    fn set(&mut self, owner: LimitOwner, limit: T) {
        self.entries.insert(owner, (limit, false));
    }

    fn mark_stale(&mut self, plugin_id: &str) {
        for (owner, (_, stale)) in &mut self.entries {
            if owner.as_deref() == Some(plugin_id) {
                *stale = true;
            }
        }
    }

    /// True when it removed anything.
    fn remove_stale(&mut self, plugin_id: &str) -> bool {
        let before = self.entries.len();
        self.entries
            .retain(|owner, (_, stale)| !(*stale && owner.as_deref() == Some(plugin_id)));
        self.entries.len() != before
    }

    fn has_any(&self, plugin_id: &str) -> bool {
        self.entries
            .keys()
            .any(|owner| owner.as_deref() == Some(plugin_id))
    }

    fn has_live(&self, plugin_id: &str) -> bool {
        self.entries
            .iter()
            .any(|(owner, (_, stale))| !stale && owner.as_deref() == Some(plugin_id))
    }

    fn limits(&self) -> impl Iterator<Item = &T> {
        self.entries.values().map(|(limit, _)| limit)
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Outlives its configs, so a domain keeps the server's back-off when its
/// limits are swept and set again.
struct DomainState {
    configs: OwnedLimits<RateLimitConfig>,
    next_allowed: Instant,
    server_remaining: Option<u32>,
    server_reset_at: Option<Instant>,
}

impl DomainState {
    /// The strictest config, the one with the lowest `requests_per_second`,
    /// paces the domain and brings its retry policy along.
    fn config(&self) -> Option<&RateLimitConfig> {
        self.configs
            .limits()
            .min_by(|a, b| a.requests_per_second.total_cmp(&b.requests_per_second))
    }
}

struct ServerRateLimitInfo {
    remaining: u32,
    reset_at: Instant,
}

struct RateLimiter {
    domains: HashMap<String, DomainState>,
}

impl RateLimiter {
    fn new() -> Self {
        Self {
            domains: HashMap::new(),
        }
    }

    fn set_config(&mut self, domain: String, config: RateLimitConfig, owner: LimitOwner) {
        self.domains
            .entry(domain)
            .or_insert_with(|| DomainState {
                configs: OwnedLimits::default(),
                next_allowed: Instant::now(),
                server_remaining: None,
                server_reset_at: None,
            })
            .configs
            .set(owner, config);
    }

    fn mark_plugin_stale(&mut self, plugin_id: &str) {
        for state in self.domains.values_mut() {
            state.configs.mark_stale(plugin_id);
        }
    }

    fn remove_stale_plugin(&mut self, plugin_id: &str) {
        for state in self.domains.values_mut() {
            state.configs.remove_stale(plugin_id);
        }
    }

    fn has_entry_for_plugin(&self, plugin_id: &str) -> bool {
        self.domains
            .values()
            .any(|state| state.configs.has_live(plugin_id))
    }

    fn get_config(&self, domain: &str) -> Option<&RateLimitConfig> {
        self.domains.get(domain).and_then(DomainState::config)
    }

    fn update_from_response(&mut self, domain: &str, info: ServerRateLimitInfo) {
        let Some(state) = self.domains.get_mut(domain) else {
            return;
        };
        let now = Instant::now();
        if info.reset_at <= now {
            state.server_remaining = None;
            state.server_reset_at = None;
            return;
        }
        state.server_remaining = Some(info.remaining);
        state.server_reset_at = Some(info.reset_at);
    }

    fn acquire(&mut self, domain: &str) -> Option<Duration> {
        let state = self.domains.get_mut(domain)?;
        let requests_per_second = state.config()?.requests_per_second;
        if !requests_per_second.is_finite() || requests_per_second <= 0.0 {
            return None;
        }

        let static_interval = Duration::from_secs_f64(1.0 / requests_per_second);
        let now = Instant::now();

        let interval = match (state.server_remaining, state.server_reset_at) {
            (Some(remaining), Some(reset_at)) if reset_at > now => {
                if remaining == 0 {
                    let wait = reset_at - now;
                    state.next_allowed = reset_at + static_interval;
                    return Some(wait);
                }
                let time_until_reset = reset_at - now;
                let server_interval = time_until_reset / remaining;
                static_interval.max(server_interval)
            }
            _ => static_interval,
        };

        if now < state.next_allowed {
            let wait_time = state.next_allowed - now;
            state.next_allowed += interval;
            Some(wait_time)
        } else {
            state.next_allowed = now + interval;
            None
        }
    }
}

struct ConcurrencyEntry {
    limits: OwnedLimits<usize>,
    semaphore: Arc<Semaphore>,
    max_in_flight: usize,
}

impl ConcurrencyEntry {
    /// Applies the lowest cap. Loosening adds permits; tightening closes the
    /// semaphore for a fresh one, and its waiters queue on that instead.
    /// Permits live requests already hold stay valid.
    fn apply_strictest(&mut self) {
        let Some(&max_in_flight) = self.limits.limits().min() else {
            return;
        };
        if max_in_flight > self.max_in_flight {
            self.semaphore
                .add_permits(max_in_flight - self.max_in_flight);
        } else if max_in_flight < self.max_in_flight {
            self.semaphore.close();
            self.semaphore = Arc::new(Semaphore::new(max_in_flight));
        }
        self.max_in_flight = max_in_flight;
    }
}

/// Process-wide host-keyed in-flight cap, complementary to [`RateLimiter`]
/// (which paces requests-per-second). Keyed on the host string from
/// [`extract_domain`] so a single registration covers every caller in the
/// process; per-handle keying would re-introduce bursts as soon as a second
/// caller appeared.
struct ConcurrencyLimiter {
    entries: HashMap<String, ConcurrencyEntry>,
}

impl ConcurrencyLimiter {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    fn set_limit(&mut self, host: String, max_in_flight: usize, owner: LimitOwner) {
        let entry = self
            .entries
            .entry(host)
            .or_insert_with(|| ConcurrencyEntry {
                limits: OwnedLimits::default(),
                semaphore: Arc::new(Semaphore::new(max_in_flight)),
                max_in_flight,
            });
        entry.limits.set(owner, max_in_flight);
        entry.apply_strictest();
    }

    fn mark_plugin_stale(&mut self, plugin_id: &str) {
        for entry in self.entries.values_mut() {
            entry.limits.mark_stale(plugin_id);
        }
    }

    /// A host left without caps closes its semaphore and lets its waiters
    /// through uncapped.
    fn remove_stale_plugin(&mut self, plugin_id: &str) {
        self.entries.retain(|_, entry| {
            if !entry.limits.remove_stale(plugin_id) {
                return true;
            }
            if entry.limits.is_empty() {
                entry.semaphore.close();
                return false;
            }
            entry.apply_strictest();
            true
        });
    }

    fn get(&self, host: &str) -> Option<Arc<Semaphore>> {
        self.entries.get(host).map(|e| e.semaphore.clone())
    }
}

fn parse_rate_limit_headers(headers: &HttpHeaderMap) -> Option<ServerRateLimitInfo> {
    let mut remaining: Option<u32> = None;
    let mut reset_ts: Option<u64> = None;

    for (key, value) in headers.iter() {
        match key.as_str() {
            "ratelimit-remaining" | "x-ratelimit-remaining" if remaining.is_none() => {
                remaining = value.parse().ok();
            }
            "ratelimit-reset" | "x-ratelimit-reset" if reset_ts.is_none() => {
                reset_ts = value.parse().ok();
            }
            _ => {}
        }
    }

    let remaining = remaining?;
    let reset_value = reset_ts?;

    // Distinguish absolute Unix timestamps from relative deltas.
    // Values below year-2000 epoch are treated as seconds-from-now.
    const EPOCH_2000: u64 = 946_684_800;
    let reset_at = if reset_value < EPOCH_2000 {
        Instant::now() + Duration::from_secs(reset_value)
    } else {
        let now_unix = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .ok()?
            .as_secs();
        if reset_value <= now_unix {
            return None;
        }
        Instant::now() + Duration::from_secs(reset_value - now_unix)
    };

    Some(ServerRateLimitInfo {
        remaining,
        reset_at,
    })
}

fn extract_domain(url_str: &str) -> Option<String> {
    url::Url::parse(url_str)
        .ok()
        .and_then(|u| u.host_str().map(|s| s.to_string()))
}

/// Normalizes a full URL or bare host to the key request-time lookups use.
/// `url::Url` lowercases hosts on parse, so the bare-host fallback lowercases
/// too or registrations under uppercase keys silently miss.
fn host_key(host_or_url: &str) -> String {
    extract_domain(host_or_url).unwrap_or_else(|| host_or_url.to_ascii_lowercase())
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct HttpHeaderMap(BTreeMap<String, String>);

impl HttpHeaderMap {
    fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
        self.0.iter()
    }
}

impl LuauTypeInfo for HttpHeaderMap {
    fn luau_type() -> LuauType {
        LuauType::literal("HttpHeaderMap")
    }
}

impl DescribeTypeAlias for HttpHeaderMap {
    fn type_alias_descriptor() -> harmony_luau::TypeAliasDescriptor {
        harmony_luau::TypeAliasDescriptor::new(
            "HttpHeaderMap",
            LuauType::map(String::luau_type(), String::luau_type()),
            Some("A string-to-string header or cookie map."),
        )
    }
}

#[harmony_macros::userdata(name = "HttpMethod")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
    Patch,
    Head,
}

#[derive(Clone, Debug)]
struct HttpRequestOptions {
    url: String,
    method: HttpMethod,
    body: Option<LuaBinaryInput>,
    headers: Option<HttpHeaderMap>,
    cookies: Option<HttpHeaderMap>,
}

#[derive(Clone, Debug)]
struct HttpRateLimitOptions {
    domain: String,
    /// Defaults to `1.0`.
    requests_per_second: Option<f64>,
    retry_on: Option<Vec<u16>>,
    /// Defaults to `3`.
    max_retries: Option<u32>,
    /// Defaults to `1000`.
    backoff_ms: Option<u64>,
}

#[derive(Clone, Debug)]
struct HttpConcurrencyOptions {
    /// Host or full URL; only the host portion is keyed, matching the
    /// request-time lookup in [`extract_domain`].
    host: String,
    /// Maximum simultaneous in-flight requests to this host. Must be ≥ 1.
    max_in_flight: u32,
}

#[derive(Clone, Debug)]
struct HttpResponse {
    success: bool,
    status_code: Option<u16>,
    status_message: String,
    headers: HttpHeaderMap,
    cookies: HttpHeaderMap,
    body: BodyBytes,
    /// Transport error category when `success` is false.
    error_kind: Option<String>,
    retries: u32,
    rate_limited: bool,
}

impl HttpResponse {
    fn success(
        status_code: u16,
        status_message: String,
        headers: HttpHeaderMap,
        cookies: HttpHeaderMap,
        body: Vec<u8>,
    ) -> Self {
        let body = BodyBytes(body);
        Self {
            success: true,
            status_code: Some(status_code),
            status_message,
            headers,
            cookies,
            body,
            error_kind: None,
            retries: 0,
            rate_limited: false,
        }
    }

    fn error(kind: &str, status_code: Option<u16>, message: String) -> Self {
        Self {
            success: false,
            status_code,
            status_message: message,
            headers: HttpHeaderMap::default(),
            cookies: HttpHeaderMap::default(),
            body: BodyBytes::default(),
            error_kind: Some(kind.to_string()),
            retries: 0,
            rate_limited: false,
        }
    }

    fn from_reqwest_error(err: &reqwest::Error) -> Self {
        let status_code = err.status().map(|s| s.as_u16());
        let (kind, message) = classify_reqwest_error(err);
        Self::error(kind, status_code, message)
    }

    fn with_retry_info(mut self, retries: u32, rate_limited: bool) -> Self {
        self.retries = retries;
        self.rate_limited = rate_limited;
        self
    }
}

fn classify_reqwest_error(err: &reqwest::Error) -> (&'static str, String) {
    if err.is_timeout() {
        ("timeout", "request timed out".to_string())
    } else if err.is_connect() {
        ("connect", format!("failed to connect: {err}"))
    } else if err.is_redirect() {
        ("redirect", format!("too many redirects: {err}"))
    } else if err.is_request() {
        ("request", format!("request error: {err}"))
    } else if err.is_body() {
        ("body", format!("body error: {err}"))
    } else if err.is_decode() {
        ("decode", format!("decode error: {err}"))
    } else if err.is_builder() {
        ("builder", format!("request builder error: {err}"))
    } else {
        ("unknown", format!("unknown error: {err}"))
    }
}

fn build_request(
    client: &reqwest::Client,
    method: HttpMethod,
    url: &str,
) -> reqwest::RequestBuilder {
    match method {
        HttpMethod::Get => client.get(url),
        HttpMethod::Post => client.post(url),
        HttpMethod::Put => client.put(url),
        HttpMethod::Delete => client.delete(url),
        HttpMethod::Patch => client.patch(url),
        HttpMethod::Head => client.head(url),
    }
}

fn apply_body(
    req: reqwest::RequestBuilder,
    options: &HttpRequestOptions,
) -> reqwest::RequestBuilder {
    match options.body {
        Some(ref body) => req.body(body.0.clone()),
        None => req,
    }
}

fn apply_cookies(
    req: reqwest::RequestBuilder,
    options: &HttpRequestOptions,
) -> reqwest::RequestBuilder {
    let Some(cookies) = options.cookies.as_ref() else {
        return req;
    };

    let cookie_str = cookies
        .0
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("; ");

    if cookie_str.is_empty() {
        req
    } else {
        req.header("Cookie", cookie_str)
    }
}

fn apply_headers(
    req: reqwest::RequestBuilder,
    options: &HttpRequestOptions,
) -> reqwest::RequestBuilder {
    let mut req = req;
    let mut has_user_agent = false;

    if let Some(headers) = options.headers.as_ref() {
        for (key, value) in &headers.0 {
            if key.eq_ignore_ascii_case("user-agent") {
                has_user_agent = true;
            }
            req = req.header(key, value);
        }
    }

    if !has_user_agent && let Some(default_user_agent) = get_default_user_agent() {
        req = req.header(reqwest::header::USER_AGENT, default_user_agent);
    }

    req
}

fn extract_headers(headers: &reqwest::header::HeaderMap) -> HttpHeaderMap {
    HttpHeaderMap(
        headers
            .iter()
            .filter_map(|(key, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|v| (key.to_string(), v.to_string()))
            })
            .collect(),
    )
}

fn extract_cookies(headers: &reqwest::header::HeaderMap) -> HttpHeaderMap {
    HttpHeaderMap(
        headers
            .get_all("set-cookie")
            .iter()
            .filter_map(|value| {
                let val_str = value.to_str().ok()?;
                let (cookie_pair, _) = val_str.split_once(';')?;
                let (name, value) = cookie_pair.split_once('=')?;
                Some((name.trim().to_string(), value.trim().to_string()))
            })
            .collect(),
    )
}

async fn execute_single_request(options: &HttpRequestOptions) -> HttpResponse {
    let req = build_request(&HTTP_CLIENT, options.method, &options.url);
    let req = apply_body(req, options);
    let req = apply_cookies(req, options);
    let req = apply_headers(req, options);

    let resp = match req.send().await {
        Ok(r) => r,
        Err(err) => return HttpResponse::from_reqwest_error(&err),
    };

    let status_code = resp.status().as_u16();
    let status_message = resp
        .status()
        .canonical_reason()
        .unwrap_or("Unknown Status")
        .to_string();
    let headers = extract_headers(resp.headers());
    let cookies = extract_cookies(resp.headers());

    let body = match resp.bytes().await {
        Ok(bytes) => bytes.to_vec(),
        Err(err) => return HttpResponse::from_reqwest_error(&err),
    };

    HttpResponse::success(status_code, status_message, headers, cookies, body)
}

async fn acquire_concurrency_permit(domain: Option<&str>) -> Option<OwnedSemaphorePermit> {
    let host = domain?;
    loop {
        let semaphore = {
            let limiter = CONCURRENCY_LIMITER.read().await;
            match limiter.get(host) {
                Some(s) => s,
                None => {
                    // Trace level so unregistered hosts stay quiet at info but
                    // remain visible when diagnosing burst behaviour.
                    tracing::trace!(
                        host = %host,
                        "no concurrency cap registered; request will not be capped",
                    );
                    return None;
                }
            }
        };
        // Permit lives for the whole request — including retry-backoff sleep —
        // so a sibling can't burst past a `Retry-After` cooldown.
        // A closed semaphore was replaced or dropped; look the host up again.
        if let Ok(permit) = semaphore.acquire_owned().await {
            return Some(permit);
        }
    }
}

/// Drives the rate-limit + retry + concurrency-cap loop. `request_fn` is
/// called once per attempt; production passes [`execute_single_request`] while
/// tests inject a deterministic fake to exercise retry/permit-hold paths
/// without real HTTP.
async fn execute_request_loop<F, Fut>(options: HttpRequestOptions, request_fn: F) -> HttpResponse
where
    F: Fn(HttpRequestOptions) -> Fut,
    Fut: std::future::Future<Output = HttpResponse>,
{
    let domain = extract_domain(&options.url);

    // Acquire the permit before rate-limit pacing: pacing alone lets N tasks
    // sleep concurrently then burst-fire; the semaphore caps the
    // "paced + executing" phase to `max_in_flight`.
    let _concurrency_permit = acquire_concurrency_permit(domain.as_deref()).await;

    let config = if let Some(ref d) = domain {
        let limiter = RATE_LIMITER.read().await;
        limiter.get_config(d).cloned()
    } else {
        None
    };

    let rate_limited = config.is_some();
    let mut retries = 0u32;
    let mut backoff = config
        .as_ref()
        .map(|c| c.initial_backoff)
        .unwrap_or(Duration::from_secs(1));

    loop {
        if let (Some(d), Some(_)) = (&domain, &config) {
            let mut limiter = RATE_LIMITER.write().await;
            if let Some(wait_time) = limiter.acquire(d) {
                drop(limiter);
                tokio::time::sleep(wait_time).await;
            }
        }

        let response = request_fn(options.clone()).await;

        if let Some(ref d) = domain
            && let Some(info) = parse_rate_limit_headers(&response.headers)
        {
            let mut limiter = RATE_LIMITER.write().await;
            limiter.update_from_response(d, info);
        }

        let should_retry = config.as_ref().is_some_and(|cfg| {
            if retries >= cfg.max_retries {
                return false;
            }

            if response
                .status_code
                .is_some_and(|code| cfg.retry_status_codes.contains(&code))
            {
                return true;
            }

            if !response.success {
                return response
                    .error_kind
                    .as_deref()
                    .is_some_and(|kind| matches!(kind, "connect" | "timeout"));
            }

            false
        });

        if !should_retry {
            return response.with_retry_info(retries, rate_limited);
        }

        // Per-request backoff; domain pacing already updated above.
        let retry_after = response
            .headers
            .iter()
            .find(|(k, _)| *k == "retry-after")
            .and_then(|(_, v)| parse_retry_after(v, SystemTime::now()));

        let wait_time = retry_after.unwrap_or_else(|| jittered_backoff(backoff));
        tokio::time::sleep(wait_time).await;

        retries += 1;
        backoff = backoff.saturating_mul(2);
    }
}

/// RFC 7231: integer seconds or HTTP-date.
fn parse_retry_after(value: &str, now: SystemTime) -> Option<Duration> {
    let trimmed = value.trim();
    if let Ok(secs) = trimmed.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    let target = httpdate::parse_http_date(trimmed).ok()?;
    Some(target.duration_since(now).unwrap_or(Duration::ZERO))
}

/// Full-jitter sleep in `[0, cap]` — prevents thundering-herd retry bursts.
fn jittered_backoff(cap: Duration) -> Duration {
    let cap_nanos = cap.as_nanos().min(u64::MAX as u128) as u64;
    if cap_nanos == 0 {
        return Duration::ZERO;
    }
    Duration::from_nanos(fastrand::u64(0..=cap_nanos))
}

async fn execute_request_with_rate_limit(options: HttpRequestOptions) -> HttpResponse {
    execute_request_loop(options, |opts| async move {
        execute_single_request(&opts).await
    })
    .await
}

pub fn set_default_user_agent(user_agent: impl Into<String>) {
    let user_agent = user_agent.into().trim().to_string();
    let mut guard = DEFAULT_USER_AGENT
        .write()
        .expect("failed to acquire default user-agent write lock");
    if user_agent.is_empty() {
        *guard = None;
    } else {
        *guard = Some(user_agent);
    }
}

fn get_default_user_agent() -> Option<String> {
    DEFAULT_USER_AGENT
        .read()
        .expect("failed to acquire default user-agent read lock")
        .clone()
}

pub struct HttpModule;

async fn configure_rate_limit(
    plugin_id: Option<Arc<str>>,
    options: HttpRateLimitOptions,
) -> anyhow::Result<()> {
    let requests_per_second = options.requests_per_second.unwrap_or(1.0);
    if !requests_per_second.is_finite() || requests_per_second <= 0.0 {
        anyhow::bail!("requests_per_second must be a positive number");
    }

    let domain = host_key(&options.domain);

    let config = RateLimitConfig {
        requests_per_second,
        retry_status_codes: options.retry_on.unwrap_or_else(|| vec![429, 503]),
        max_retries: options.max_retries.unwrap_or(3),
        initial_backoff: Duration::from_millis(options.backoff_ms.unwrap_or(1000)),
    };

    let mut limiter = RATE_LIMITER.write().await;
    limiter.set_config(domain, config, plugin_id);

    Ok(())
}

async fn configure_max_in_flight(
    plugin_id: Option<Arc<str>>,
    options: HttpConcurrencyOptions,
) -> anyhow::Result<()> {
    if options.max_in_flight == 0 {
        anyhow::bail!("max_in_flight must be at least 1");
    }

    let host = host_key(&options.host);

    let mut limiter = CONCURRENCY_LIMITER.write().await;
    limiter.set_limit(host, options.max_in_flight as usize, plugin_id);

    Ok(())
}

/// The capability hosts conventionally gate this module behind.
pub const SCOPE: &str = "harmony.http";

#[harmony_macros::module(
    id = "harmony/http",
    name = "Http",
    local = "http",
    description = "Outbound HTTP requests, rate limits, and URI encoding.",
    userdata(HttpMethod)
)]
impl HttpModule {
    #[harmony(
        description = "Makes an outbound HTTP request.",
        args(options: HttpRequestOptions),
        returns(HttpResponse)
    )]
    fn request(
        vm: &luau::Vm,
        options: luau::Table,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        let options = request_options_from_luau(vm, &options)?;
        Ok(luau::ScheduledFuture::new(async move {
            let response = execute_request_with_rate_limit(options).await;
            Ok(response.into_luau_value())
        }))
    }

    #[harmony(
        description = "Registers retry and pacing policy for a host or URL. A host follows the strictest policy any plugin sets for it, the one with the lowest `requests_per_second`; a plugin may loosen its own.",
        args(options: HttpRateLimitOptions)
    )]
    fn set_rate_limit(
        origin: &luau::ChunkOrigin,
        options: luau::Table,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        let options = rate_limit_options_from_luau(&options)?;
        let plugin_id = origin.plugin.clone();
        Ok(luau::ScheduledFuture::new(async move {
            configure_rate_limit(plugin_id, options)
                .await
                .map_err(|error| luau::Error::Runtime(error.to_string()))?;
            Ok(())
        }))
    }

    #[harmony(
        description = "Registers a maximum concurrent request count for a host or URL. A host follows the lowest count any plugin sets for it; a plugin may loosen its own.",
        args(options: HttpConcurrencyOptions)
    )]
    fn set_max_in_flight(
        origin: &luau::ChunkOrigin,
        options: luau::Table,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        let options = concurrency_options_from_luau(&options)?;
        let plugin_id = origin.plugin.clone();
        Ok(luau::ScheduledFuture::new(async move {
            configure_max_in_flight(plugin_id, options)
                .await
                .map_err(|error| luau::Error::Runtime(error.to_string()))?;
            Ok(())
        }))
    }

    #[harmony(description = "Percent-encodes a string for use in a URI component.")]
    fn encode_uri_component(input: String) -> String {
        percent_encoding::utf8_percent_encode(&input, URI_COMPONENT_SET).to_string()
    }
}

fn request_options_from_luau(
    vm: &luau::Vm,
    table: &luau::Table,
) -> luau::runtime::Result<HttpRequestOptions> {
    Ok(HttpRequestOptions {
        url: table.required_string_field("url")?,
        method: required_method_field(vm, table, "method")?,
        body: optional_binary_field(table, "body")?,
        headers: optional_header_map_field(table, "headers")?,
        cookies: optional_header_map_field(table, "cookies")?,
    })
}

fn rate_limit_options_from_luau(
    table: &luau::Table,
) -> luau::runtime::Result<HttpRateLimitOptions> {
    Ok(HttpRateLimitOptions {
        domain: table.required_string_field("domain")?,
        requests_per_second: optional_f64_field(table, "requests_per_second")?,
        retry_on: optional_u16_array_field(table, "retry_on")?,
        max_retries: optional_u32_field(table, "max_retries")?,
        backoff_ms: optional_u64_field(table, "backoff_ms")?,
    })
}

fn concurrency_options_from_luau(
    table: &luau::Table,
) -> luau::runtime::Result<HttpConcurrencyOptions> {
    Ok(HttpConcurrencyOptions {
        host: table.required_string_field("host")?,
        max_in_flight: required_u32_field(table, "max_in_flight")?,
    })
}

fn optional_f64_field(
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<Option<f64>> {
    match table.get(field)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Integer(value) => Ok(Some(value as f64)),
        luau::Value::Number(value) => Ok(Some(value)),
        other => Err(luau_field_type_error(field, "number", other.type_name())),
    }
}

fn optional_u32_field(
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<Option<u32>> {
    match table.get(field)? {
        luau::Value::Nil => Ok(None),
        value => number_to_u32(field, value).map(Some),
    }
}

fn optional_u64_field(
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<Option<u64>> {
    match table.get(field)? {
        luau::Value::Nil => Ok(None),
        value => number_to_u64(field, value).map(Some),
    }
}

fn required_u32_field(table: &luau::Table, field: &'static str) -> luau::runtime::Result<u32> {
    match table.get(field)? {
        luau::Value::Nil => Err(luau::Error::Runtime(format!("missing '{field}' field"))),
        value => number_to_u32(field, value),
    }
}

fn optional_binary_field(
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<Option<LuaBinaryInput>> {
    match table.get(field)? {
        luau::Value::Nil => Ok(None),
        luau::Value::String(value) | luau::Value::Buffer(value) => Ok(Some(LuaBinaryInput(value))),
        other => Err(luau_field_type_error(
            field,
            "string or buffer",
            other.type_name(),
        )),
    }
}

fn optional_header_map_field(
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<Option<HttpHeaderMap>> {
    match table.get(field)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Table(value) => header_map_from_luau(&value, field).map(Some),
        other => Err(luau_field_type_error(field, "table", other.type_name())),
    }
}

fn optional_u16_array_field(
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<Option<Vec<u16>>> {
    match table.get(field)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Table(value) => {
            let mut output = Vec::new();
            for (_key, value) in value.pairs()? {
                output.push(number_to_u16(field, value)?);
            }
            Ok(Some(output))
        }
        other => Err(luau_field_type_error(field, "table", other.type_name())),
    }
}

fn required_method_field(
    vm: &luau::Vm,
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<HttpMethod> {
    let value = table.get(field)?;
    if matches!(value, luau::Value::Nil) {
        return Err(luau::Error::Runtime(format!("missing '{field}' field")));
    }
    HttpMethod::_harmony_userdata_class().read_value(vm, field, value)
}

fn header_map_from_luau(
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<HttpHeaderMap> {
    let mut map = BTreeMap::new();
    for (key, value) in table.pairs()? {
        let luau::Value::String(key) = key else {
            return Err(luau_field_type_error(
                field,
                "table<string, string>",
                key.type_name(),
            ));
        };
        let luau::Value::String(value) = value else {
            return Err(luau_field_type_error(
                field,
                "table<string, string>",
                value.type_name(),
            ));
        };
        let key = String::from_utf8(key).map_err(|error| {
            luau::Error::Runtime(format!("'{field}' key must be UTF-8: {error}"))
        })?;
        let value = String::from_utf8(value).map_err(|error| {
            luau::Error::Runtime(format!("'{field}' value must be UTF-8: {error}"))
        })?;
        map.insert(key, value);
    }
    Ok(HttpHeaderMap(map))
}

fn number_to_u16(field: &'static str, value: luau::Value) -> luau::runtime::Result<u16> {
    let value = number_to_u64(field, value)?;
    u16::try_from(value)
        .map_err(|_| luau::Error::Runtime(format!("'{field}' value is out of range for u16")))
}

fn number_to_u32(field: &'static str, value: luau::Value) -> luau::runtime::Result<u32> {
    let value = number_to_u64(field, value)?;
    u32::try_from(value)
        .map_err(|_| luau::Error::Runtime(format!("'{field}' value is out of range for u32")))
}

fn number_to_u64(field: &'static str, value: luau::Value) -> luau::runtime::Result<u64> {
    match value {
        luau::Value::Integer(value) if value >= 0 => Ok(value as u64),
        luau::Value::Number(value)
            if value.is_finite()
                && value >= 0.0
                && value <= u64::MAX as f64
                && value.fract() == 0.0 =>
        {
            Ok(value as u64)
        }
        other => Err(luau_field_type_error(
            field,
            "non-negative integer",
            other.type_name(),
        )),
    }
}

fn luau_field_type_error(field: &str, expected: &str, actual: &str) -> luau::Error {
    luau::Error::Runtime(format!(
        "invalid '{field}' field: expected {expected}, got {actual}"
    ))
}

impl HttpResponse {
    fn into_luau_value(self) -> luau::Value {
        let mut table = luau::OwnedTable::with_capacity(0, 9);
        table.set_field("success", self.success);
        // Luau LUA_TINTEGER and LUA_TNUMBER are distinct types under raw
        // equality; `Integer(200) == 200` from a script would be false.
        table.set_field(
            "status_code",
            self.status_code
                .map(|value| luau::Value::Number(f64::from(value)))
                .unwrap_or(luau::Value::Nil),
        );
        table.set_field("status_message", self.status_message);
        table.set_field("headers", self.headers.into_luau_value());
        table.set_field("cookies", self.cookies.into_luau_value());
        table.set_field("body", luau::Value::String(self.body.0));
        table.set_field(
            "error_kind",
            self.error_kind
                .map(|value| luau::Value::String(value.into_bytes()))
                .unwrap_or(luau::Value::Nil),
        );
        table.set_field("retries", luau::Value::Number(f64::from(self.retries)));
        table.set_field("rate_limited", self.rate_limited);
        luau::Value::TableData(table)
    }
}

impl HttpHeaderMap {
    fn into_luau_value(self) -> luau::Value {
        let mut table = luau::OwnedTable::with_capacity(0, self.0.len());
        for (key, value) in self.0 {
            table.set_field(key, value);
        }
        luau::Value::TableData(table)
    }
}

/// True when `plugin_id` holds a `set_rate_limit` entry for at least one
/// domain. Plugin surfaces gate on this before registering.
pub async fn has_rate_limit_for_plugin(plugin_id: &str) -> bool {
    let limiter = RATE_LIMITER.read().await;
    limiter.has_entry_for_plugin(plugin_id)
}

/// Marks every limit `plugin_id` set as stale: each still applies, but no
/// longer counts for [`has_rate_limit_for_plugin`] until the plugin sets it
/// again.
pub async fn mark_plugin_limits_stale(plugin_id: &str) {
    RATE_LIMITER.write().await.mark_plugin_stale(plugin_id);
    CONCURRENCY_LIMITER
        .write()
        .await
        .mark_plugin_stale(plugin_id);
}

/// Drops the limits of `plugin_id` still stale. Each host falls back to the
/// strictest limit its other owners hold.
pub async fn remove_stale_plugin_limits(plugin_id: &str) {
    RATE_LIMITER.write().await.remove_stale_plugin(plugin_id);
    CONCURRENCY_LIMITER
        .write()
        .await
        .remove_stale_plugin(plugin_id);
}

/// Whether `plugin_id` holds any limit, stale or live.
#[doc(hidden)]
pub async fn test_holds_any_limit(plugin_id: &str) -> bool {
    let rate = RATE_LIMITER
        .read()
        .await
        .domains
        .values()
        .any(|state| state.configs.has_any(plugin_id));
    let concurrency = CONCURRENCY_LIMITER
        .read()
        .await
        .entries
        .values()
        .any(|entry| entry.limits.has_any(plugin_id));
    rate || concurrency
}

#[doc(hidden)]
pub async fn test_seed_rate_limit(domain: impl Into<String>, plugin_id: impl Into<Arc<str>>) {
    let mut limiter = RATE_LIMITER.write().await;
    limiter.set_config(
        domain.into(),
        RateLimitConfig {
            requests_per_second: 1.0,
            retry_status_codes: vec![429],
            max_retries: 0,
            initial_backoff: Duration::from_millis(100),
        },
        Some(plugin_id.into()),
    );
}

impl LuauTypeInfo for HttpRequestOptions {
    fn luau_type() -> LuauType {
        LuauType::literal("HttpRequestOptions")
    }
}

impl DescribeInterface for HttpRequestOptions {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("HttpRequestOptions", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "url",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "method",
                ty: HttpMethod::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "body",
                ty: Option::<LuaBinaryInput>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "headers",
                ty: Option::<HttpHeaderMap>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "cookies",
                ty: Option::<HttpHeaderMap>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for HttpRateLimitOptions {
    fn luau_type() -> LuauType {
        LuauType::literal("HttpRateLimitOptions")
    }
}

impl DescribeInterface for HttpRateLimitOptions {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("HttpRateLimitOptions", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "domain",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "requests_per_second",
                ty: Option::<f64>::luau_type(),
                description: Some("Defaults to `1.0`."),
            },
            FieldDescriptor {
                name: "retry_on",
                ty: Option::<Vec<u16>>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "max_retries",
                ty: Option::<u32>::luau_type(),
                description: Some("Defaults to `3`."),
            },
            FieldDescriptor {
                name: "backoff_ms",
                ty: Option::<u64>::luau_type(),
                description: Some("Defaults to `1000`."),
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for HttpConcurrencyOptions {
    fn luau_type() -> LuauType {
        LuauType::literal("HttpConcurrencyOptions")
    }
}

impl DescribeInterface for HttpConcurrencyOptions {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("HttpConcurrencyOptions", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "host",
                ty: String::luau_type(),
                description: Some(
                    "Host or full URL; only the host portion is keyed, matching request-time lookup.",
                ),
            },
            FieldDescriptor {
                name: "max_in_flight",
                ty: u32::luau_type(),
                description: Some("Maximum simultaneous in-flight requests to this host."),
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for HttpResponse {
    fn luau_type() -> LuauType {
        LuauType::literal("HttpResponse")
    }
}

impl DescribeInterface for HttpResponse {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("HttpResponse", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "success",
                ty: bool::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "status_code",
                ty: Option::<u16>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "status_message",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "headers",
                ty: HttpHeaderMap::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "cookies",
                ty: HttpHeaderMap::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "body",
                ty: BodyBytes::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "error_kind",
                ty: Option::<String>::luau_type(),
                description: Some("Transport error category when `success` is false."),
            },
            FieldDescriptor {
                name: "retries",
                ty: u32::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "rate_limited",
                ty: bool::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

pub fn render_luau_definition() -> Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &HttpModule::module_descriptor(),
        &[HttpHeaderMap::type_alias_descriptor()],
        &[
            HttpRequestOptions::interface_descriptor(),
            HttpRateLimitOptions::interface_descriptor(),
            HttpConcurrencyOptions::interface_descriptor(),
            HttpResponse::interface_descriptor(),
        ],
        &[HttpMethod::class_descriptor()],
    )
}

#[cfg(test)]
mod tests {
    use super::HttpModule;

    fn max_in_flight(limiter: &super::ConcurrencyLimiter, host: &str) -> Option<usize> {
        limiter.entries.get(host).map(|entry| entry.max_in_flight)
    }

    #[test]
    fn luau_module_registers_encode_uri_component() -> harmony_luau::runtime::Result<()> {
        let vm = harmony_luau::Vm::new()?;
        let spec = HttpModule::module_spec();
        let table =
            harmony_core::luau::install_module(&vm, &harmony_core::ChunkOrigin::default(), &spec)?;
        vm.set_global_table("http", &table)?;

        let values = vm.eval(
            std::sync::Arc::<[u8]>::from(
                &br#"return http.encode_uri_component("a b/c?d=e&x=1-_.!~*'()")"#[..],
            ),
            harmony_luau::ChunkOrigin::default(),
        )?;

        assert_eq!(
            values,
            vec![harmony_luau::Value::String(
                b"a%20b%2Fc%3Fd%3De%26x%3D1-_.!~*'()".to_vec()
            )]
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn luau_module_registers_http_methods_and_async_setters()
    -> harmony_luau::runtime::Result<()> {
        let vm = harmony_luau::Vm::new()?;
        vm.data().insert(harmony_core::LocalScheduler::new())?;
        let scheduler = vm.data().get::<harmony_core::LocalScheduler>()?;
        let origin = harmony_core::ChunkOrigin {
            plugin: Some(std::sync::Arc::from("luau-http-plugin")),
            ..harmony_core::ChunkOrigin::default()
        };
        let spec = HttpModule::module_spec();
        let table = harmony_core::luau::install_module(&vm, &origin, &spec)?;
        vm.set_global_table("http", &table)?;

        let root = vm.load_chunk(&harmony_luau::Chunk::new(
            std::sync::Arc::<[u8]>::from(
                &br#"
                    http.set_rate_limit({
                        domain = "Example.com",
                        requests_per_second = 2,
                        retry_on = { 429, 503 },
                        max_retries = 1,
                        backoff_ms = 10,
                    })
                    http.set_max_in_flight({
                        host = "https://EXAMPLE.com/path",
                        max_in_flight = 2,
                    })
                    stored_method = http.HttpMethod.Post
                "#[..],
            ),
            harmony_luau::ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&root)?;
        scheduler.spawn_luau_thread(
            harmony_core::CallContext::default(),
            vm.clone(),
            thread,
            vec![],
        );

        assert_eq!(scheduler.poll_ready(), 1);
        assert_eq!(scheduler.poll_ready(), 0);
        assert_eq!(scheduler.poll_ready(), 0);

        assert!(super::has_rate_limit_for_plugin("luau-http-plugin").await);
        {
            let limiter = super::CONCURRENCY_LIMITER.read().await;
            assert_eq!(max_in_flight(&limiter, "example.com"), Some(2));
        }
        let mut values = vm.eval(
            std::sync::Arc::<[u8]>::from(&b"return stored_method"[..]),
            harmony_luau::ChunkOrigin::default(),
        )?;
        assert_eq!(values.len(), 1);
        let stored_method = super::HttpMethod::_harmony_userdata_class().read_value(
            &vm,
            "stored_method",
            values.pop().unwrap(),
        )?;
        assert_eq!(stored_method, super::HttpMethod::Post);
        Ok(())
    }

    #[test]
    fn into_luau_value_pushes_numeric_fields_as_number() {
        let value = super::HttpResponse::success(
            200,
            "OK".into(),
            super::HttpHeaderMap::default(),
            super::HttpHeaderMap::default(),
            Vec::new(),
        )
        .with_retry_info(3, false)
        .into_luau_value();
        let table = match value {
            harmony_luau::Value::TableData(table) => table,
            other => panic!("expected TableData, got {other:?}"),
        };
        let field = |name: &str| {
            table
                .fields()
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
                .unwrap_or_else(|| panic!("missing field {name}"))
        };
        assert!(
            matches!(field("status_code"), harmony_luau::Value::Number(n) if n == 200.0),
            "status_code must be Number, got {:?}",
            field("status_code"),
        );
        assert!(
            matches!(field("retries"), harmony_luau::Value::Number(n) if n == 3.0),
            "retries must be Number, got {:?}",
            field("retries"),
        );
    }

    fn rate_limit_config(requests_per_second: f64) -> super::RateLimitConfig {
        super::RateLimitConfig {
            requests_per_second,
            retry_status_codes: vec![429],
            max_retries: 0,
            initial_backoff: std::time::Duration::from_millis(10),
        }
    }

    fn requests_per_second(limiter: &super::RateLimiter, domain: &str) -> Option<f64> {
        limiter
            .get_config(domain)
            .map(|config| config.requests_per_second)
    }

    fn remove_plugin(limiter: &mut super::RateLimiter, plugin_id: &str) {
        limiter.mark_plugin_stale(plugin_id);
        limiter.remove_stale_plugin(plugin_id);
    }

    #[test]
    fn rate_limit_follows_the_strictest_plugin() {
        let mut limiter = super::RateLimiter::new();
        let domain = "example.com";
        limiter.set_config(
            domain.to_string(),
            rate_limit_config(5.0),
            Some("loose".into()),
        );
        limiter.set_config(
            domain.to_string(),
            rate_limit_config(2.0),
            Some("strict".into()),
        );
        // A limit set outside any plugin is its own entry; it replaces none.
        limiter.set_config(domain.to_string(), rate_limit_config(10.0), None);
        assert_eq!(requests_per_second(&limiter, domain), Some(2.0));
        assert!(limiter.has_entry_for_plugin("strict"));
        assert!(!limiter.has_entry_for_plugin("other"));

        remove_plugin(&mut limiter, "strict");
        assert_eq!(requests_per_second(&limiter, domain), Some(5.0));
        assert!(limiter.has_entry_for_plugin("loose"));
        assert!(!limiter.has_entry_for_plugin("strict"));

        remove_plugin(&mut limiter, "loose");
        assert_eq!(requests_per_second(&limiter, domain), Some(10.0));
    }

    #[test]
    fn stale_rate_limits_apply_until_swept_without_counting_for_the_plugin() {
        let mut limiter = super::RateLimiter::new();
        let domain = "example.com";
        let shared = "shared.example";
        limiter.set_config(
            domain.to_string(),
            rate_limit_config(2.0),
            Some("restarted".into()),
        );
        limiter.set_config(
            shared.to_string(),
            rate_limit_config(2.0),
            Some("restarted".into()),
        );
        limiter.set_config(
            shared.to_string(),
            rate_limit_config(5.0),
            Some("live".into()),
        );

        limiter.mark_plugin_stale("restarted");
        assert!(!limiter.has_entry_for_plugin("restarted"));
        assert!(limiter.has_entry_for_plugin("live"));
        assert_eq!(requests_per_second(&limiter, domain), Some(2.0));
        assert_eq!(requests_per_second(&limiter, shared), Some(2.0));

        limiter.set_config(
            domain.to_string(),
            rate_limit_config(3.0),
            Some("restarted".into()),
        );
        assert!(limiter.has_entry_for_plugin("restarted"));
        limiter.remove_stale_plugin("restarted");
        assert_eq!(requests_per_second(&limiter, domain), Some(3.0));
        assert_eq!(
            requests_per_second(&limiter, shared),
            Some(5.0),
            "the live plugin's limit remains once the stale one is swept",
        );
    }

    #[test]
    fn swept_domain_keeps_its_server_back_off() {
        let mut limiter = super::RateLimiter::new();
        let domain = "example.com";
        limiter.set_config(
            domain.to_string(),
            rate_limit_config(100.0),
            Some("demo".into()),
        );
        limiter.update_from_response(
            domain,
            super::ServerRateLimitInfo {
                remaining: 0,
                reset_at: std::time::Instant::now() + std::time::Duration::from_secs(60),
            },
        );
        remove_plugin(&mut limiter, "demo");
        assert_eq!(limiter.acquire(domain), None);

        limiter.set_config(
            domain.to_string(),
            rate_limit_config(100.0),
            Some("demo".into()),
        );
        let wait = limiter
            .acquire(domain)
            .expect("server back-off still applies");
        assert!(wait > std::time::Duration::from_secs(30), "{wait:?}");
    }

    fn remove_concurrency_plugin(limiter: &mut super::ConcurrencyLimiter, plugin_id: &str) {
        limiter.mark_plugin_stale(plugin_id);
        limiter.remove_stale_plugin(plugin_id);
    }

    #[tokio::test]
    async fn concurrency_cap_follows_the_strictest_plugin() {
        let mut limiter = super::ConcurrencyLimiter::new();
        let host = "example.com";
        limiter.set_limit(host.to_string(), 4, Some("loose".into()));
        limiter.set_limit(host.to_string(), 2, Some("strict".into()));
        assert_eq!(max_in_flight(&limiter, host), Some(2));
        let sem = limiter.get(host).expect("cap registered above");
        assert_eq!(sem.available_permits(), 2);

        limiter.set_limit(host.to_string(), 3, Some("strict".into()));
        assert_eq!(max_in_flight(&limiter, host), Some(3));
        assert_eq!(sem.available_permits(), 3);

        limiter.mark_plugin_stale("strict");
        assert_eq!(
            max_in_flight(&limiter, host),
            Some(3),
            "a stale cap still applies",
        );
        limiter.remove_stale_plugin("strict");
        assert_eq!(max_in_flight(&limiter, host), Some(4));
        assert_eq!(sem.available_permits(), 4);

        remove_concurrency_plugin(&mut limiter, "loose");
        assert_eq!(max_in_flight(&limiter, host), None);
        assert!(sem.is_closed());
    }

    #[tokio::test]
    async fn concurrency_tightening_moves_waiters_to_the_tighter_cap() {
        let host = "permit-tighten-test.example";
        {
            let mut limiter = super::CONCURRENCY_LIMITER.write().await;
            limiter.set_limit(host.to_string(), 2, Some("loose".into()));
        }
        let first = super::acquire_concurrency_permit(Some(host)).await;
        let _second = super::acquire_concurrency_permit(Some(host)).await;
        let waiter = tokio::spawn(super::acquire_concurrency_permit(Some(host)));
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        {
            let mut limiter = super::CONCURRENCY_LIMITER.write().await;
            limiter.set_limit(host.to_string(), 1, Some("strict".into()));
        }
        let permit = waiter.await.expect("waiter task");
        assert!(
            permit.is_some(),
            "the waiter takes the fresh semaphore's permit"
        );
        let next = tokio::spawn(super::acquire_concurrency_permit(Some(host)));
        tokio::task::yield_now().await;
        assert!(
            !next.is_finished(),
            "the tighter cap holds new requests even while old permits are released",
        );
        drop(first);
        tokio::task::yield_now().await;
        assert!(!next.is_finished());
        drop(permit);
        assert!(next.await.expect("next task").is_some());
    }

    #[tokio::test]
    async fn concurrency_waiters_go_uncapped_only_when_the_host_has_no_cap() {
        let host = "permit-removed-test.example";
        {
            let mut limiter = super::CONCURRENCY_LIMITER.write().await;
            limiter.set_limit(host.to_string(), 1, Some("demo".into()));
        }
        let _held = super::acquire_concurrency_permit(Some(host)).await;
        let waiter = tokio::spawn(super::acquire_concurrency_permit(Some(host)));
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        super::mark_plugin_limits_stale("demo").await;
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished(), "a stale cap still holds waiters");
        super::remove_stale_plugin_limits("demo").await;
        assert!(waiter.await.expect("waiter task").is_none());
    }

    #[tokio::test]
    async fn concurrency_permit_shared_across_callers() {
        // Unique host to avoid bleed with siblings sharing the global limiter.
        let host = "permit-shared-test.example".to_string();
        {
            let mut limiter = super::CONCURRENCY_LIMITER.write().await;
            limiter.set_limit(host.clone(), 1, None);
        }

        let sem = {
            let limiter = super::CONCURRENCY_LIMITER.read().await;
            limiter.get(&host).expect("cap registered above")
        };
        assert_eq!(sem.available_permits(), 1);

        let first = super::acquire_concurrency_permit(Some(&host))
            .await
            .expect("first caller registered cap");
        assert_eq!(
            sem.available_permits(),
            0,
            "first caller must consume the only permit on the shared semaphore",
        );

        drop(first);
        assert_eq!(
            sem.available_permits(),
            1,
            "permit must return to the shared semaphore on release",
        );

        let _second = super::acquire_concurrency_permit(Some(&host))
            .await
            .expect("second caller proceeds after first releases");
        assert_eq!(sem.available_permits(), 0);
    }

    #[test]
    fn host_key_normalizes_bare_hosts_and_urls_alike() {
        assert_eq!(super::host_key("EXAMPLE.com"), "example.com");
        assert_eq!(super::host_key("https://Example.com/foo"), "example.com");
    }

    #[tokio::test]
    async fn concurrency_permit_held_across_retries() {
        use std::sync::Arc;
        use std::sync::atomic::{
            AtomicUsize,
            Ordering,
        };

        // Unique host to avoid global-state bleed between tests.
        let host = "permit-retries-test.example".to_string();
        let url = format!("https://{host}/x");

        {
            let mut limiter = super::CONCURRENCY_LIMITER.write().await;
            limiter.set_limit(host.clone(), 1, None);
        }
        {
            let mut limiter = super::RATE_LIMITER.write().await;
            limiter.set_config(
                host.clone(),
                super::RateLimitConfig {
                    requests_per_second: 100.0,
                    retry_status_codes: vec![503],
                    max_retries: 1,
                    initial_backoff: std::time::Duration::from_millis(40),
                },
                None,
            );
        }

        let options = super::HttpRequestOptions {
            url: url.clone(),
            method: super::HttpMethod::Get,
            body: None,
            headers: None,
            cookies: None,
        };

        let attempt_counter = Arc::new(AtomicUsize::new(0));
        let (signal_tx, mut signal_rx) = tokio::sync::mpsc::unbounded_channel::<usize>();

        let request_fn = {
            let attempt_counter = attempt_counter.clone();
            let signal_tx = signal_tx.clone();
            move |_opts: super::HttpRequestOptions| {
                let attempt_counter = attempt_counter.clone();
                let signal_tx = signal_tx.clone();
                async move {
                    let n = attempt_counter.fetch_add(1, Ordering::SeqCst);
                    let _ = signal_tx.send(n);
                    if n == 0 {
                        super::HttpResponse::error("retry", Some(503), "first attempt".into())
                    } else {
                        super::HttpResponse::success(
                            200,
                            "OK".into(),
                            super::HttpHeaderMap::default(),
                            super::HttpHeaderMap::default(),
                            Vec::new(),
                        )
                    }
                }
            }
        };

        let h = tokio::spawn(super::execute_request_loop(options, request_fn));

        signal_rx
            .recv()
            .await
            .expect("first attempt must fire and signal");

        let sem = {
            let limiter = super::CONCURRENCY_LIMITER.read().await;
            limiter.get(&host).expect("cap registered above")
        };
        assert!(
            sem.clone().try_acquire_owned().is_err(),
            "sibling acquire must be blocked while main request is in retry backoff",
        );

        signal_rx
            .recv()
            .await
            .expect("second attempt must fire after backoff");
        let response = h.await.expect("request loop completes");

        assert_eq!(response.status_code, Some(200));
        assert_eq!(
            response.retries, 1,
            "retry count must reflect the 503-then-200 sequence",
        );

        let _ = sem
            .clone()
            .try_acquire_owned()
            .expect("sibling acquires immediately after main releases");
    }

    #[test]
    fn jittered_backoff_spreads_wakeups_across_window() {
        use std::time::Duration;

        let cap = Duration::from_millis(800);
        let samples: Vec<Duration> = (0..256).map(|_| super::jittered_backoff(cap)).collect();

        for s in &samples {
            assert!(*s <= cap, "sample {s:?} exceeded cap {cap:?}");
        }

        let min = samples.iter().min().copied().unwrap();
        let max = samples.iter().max().copied().unwrap();
        // Without jitter every sample equals `cap`. Demand a non-trivial spread
        // (>25% of cap) so a future regression to a fixed-wait backoff fails here.
        let spread = max.saturating_sub(min);
        assert!(
            spread > cap / 4,
            "expected jitter spread > {:?}, got min={min:?} max={max:?}",
            cap / 4,
        );

        // Sanity-check that we're hitting both halves of the window.
        let half = cap / 2;
        assert!(
            samples.iter().any(|s| *s < half) && samples.iter().any(|s| *s > half),
            "samples clustered to one half of [0, cap]: min={min:?} max={max:?}",
        );
    }

    #[test]
    fn jittered_backoff_zero_cap_returns_zero() {
        assert_eq!(
            super::jittered_backoff(std::time::Duration::ZERO),
            std::time::Duration::ZERO,
        );
    }

    #[test]
    fn parse_retry_after_accepts_integer_seconds() {
        let now = std::time::SystemTime::now();
        assert_eq!(
            super::parse_retry_after("12", now),
            Some(std::time::Duration::from_secs(12)),
        );
        assert_eq!(
            super::parse_retry_after("  7  ", now),
            Some(std::time::Duration::from_secs(7)),
        );
    }

    #[test]
    fn parse_retry_after_accepts_http_date() {
        // Anchor "now" at a fixed instant so the assertion is hermetic.
        let now =
            httpdate::parse_http_date("Wed, 21 Oct 2026 07:28:00 GMT").expect("parse anchor date");
        let target = "Wed, 21 Oct 2026 07:28:30 GMT";

        let waited = super::parse_retry_after(target, now).expect("HTTP-date is accepted");
        assert_eq!(waited, std::time::Duration::from_secs(30));
    }

    #[test]
    fn parse_retry_after_past_http_date_yields_zero() {
        // RFC 7231 doesn't forbid a date already in the past; treat it as
        // "retry now" rather than panicking on the SystemTime subtraction.
        let now =
            httpdate::parse_http_date("Wed, 21 Oct 2026 07:28:00 GMT").expect("parse anchor date");
        let past = "Wed, 21 Oct 2026 07:27:00 GMT";

        assert_eq!(
            super::parse_retry_after(past, now),
            Some(std::time::Duration::ZERO),
        );
    }

    #[test]
    fn parse_retry_after_rejects_garbage() {
        let now = std::time::SystemTime::now();
        assert_eq!(super::parse_retry_after("not-a-date", now), None);
        assert_eq!(super::parse_retry_after("", now), None);
    }
}
