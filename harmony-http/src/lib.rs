// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod cache;
mod hosts;

pub use cache::MAX_CACHE_TTL_MS;
pub use hosts::{
    HostEntry,
    HostsGrant,
    host_not_allowed_message,
    remove_plugin_hosts,
    set_plugin_hosts,
    test_plugin_reaches,
};

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock as StdRwLock;
use std::time::{
    Duration,
    SystemTime,
};

use harmony_core::UserDataBinding;
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
use std::sync::{
    Mutex as StdMutex,
    MutexGuard,
    PoisonError,
};
use tokio::sync::{
    OwnedSemaphorePermit,
    Semaphore,
};
use tokio::time::Instant;

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
        // Redirects are followed in `execute_request`, so each hop runs
        // under its own host's limits.
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("Failed to create HTTP client")
});

// TODO(provider-cooldown): persist per-domain state so cooldowns survive restart.
// Held only briefly and never across an await, so plain mutexes: a request
// queues for its turn to pace on the domain's own `DomainState::turn`.
static RATE_LIMITER: LazyLock<StdMutex<RateLimiter>> =
    LazyLock::new(|| StdMutex::new(RateLimiter::new()));
static CONCURRENCY_LIMITER: LazyLock<StdMutex<ConcurrencyLimiter>> =
    LazyLock::new(|| StdMutex::new(ConcurrencyLimiter::new()));

fn rate_limiter() -> MutexGuard<'static, RateLimiter> {
    RATE_LIMITER.lock().unwrap_or_else(PoisonError::into_inner)
}

fn concurrency_limiter() -> MutexGuard<'static, ConcurrencyLimiter> {
    CONCURRENCY_LIMITER
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}
static DEFAULT_USER_AGENT: LazyLock<StdRwLock<Option<String>>> =
    LazyLock::new(|| StdRwLock::new(None));

/// Redirect hops a request follows before it fails.
const MAX_REDIRECTS: usize = 5;

/// The response body cap a Luau request gets when it sets no `max_body_bytes`.
pub const DEFAULT_MAX_BODY_BYTES: usize = 16 * 1024 * 1024;
/// The largest `max_body_bytes` a Luau request may set.
pub const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

fn is_http_scheme(url: &url::Url) -> bool {
    matches!(url.scheme(), "http" | "https")
}

/// Why the redirect to `next` is refused, given the URLs the request has
/// already visited. A request stays on http(s), and once on https it stays
/// there.
fn redirect_refusal(next: &url::Url, previous: &[url::Url]) -> Option<String> {
    if previous.len() > MAX_REDIRECTS {
        return Some(format!("more than {MAX_REDIRECTS} redirects"));
    }
    if !is_http_scheme(next) {
        return Some(format!(
            "redirect to unsupported scheme '{}'",
            next.scheme()
        ));
    }
    if next.scheme() == "http" && previous.iter().any(|url| url.scheme() == "https") {
        return Some(format!("redirect downgrades https to http: {next}"));
    }
    None
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
    /// Requests pace one at a time, in the order they ask: only the one
    /// holding the turn waits for a slot, and one that gives up leaves the
    /// queue without holding back those behind it.
    turn: Arc<tokio::sync::Mutex<()>>,
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
                turn: Arc::default(),
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

    fn turn(&self, domain: &str) -> Option<Arc<tokio::sync::Mutex<()>>> {
        self.domains.get(domain).map(|state| state.turn.clone())
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

    /// Takes the domain's next request slot, or returns how long to wait before
    /// asking again. A wait reserves nothing, so a waiter that gives up, as a
    /// request past its `timeout_ms` does, holds back no later request.
    fn acquire(&mut self, domain: &str) -> Option<Duration> {
        let state = self.domains.get_mut(domain)?;
        let static_interval = request_interval(state.config()?.requests_per_second)?;
        let now = Instant::now();

        let interval = match (state.server_remaining, state.server_reset_at) {
            (Some(remaining), Some(reset_at)) if reset_at > now => {
                if remaining == 0 {
                    return Some(reset_at - now);
                }
                let time_until_reset = reset_at - now;
                let server_interval = time_until_reset / remaining;
                static_interval.max(server_interval)
            }
            _ => static_interval,
        };

        if now < state.next_allowed {
            return Some(state.next_allowed - now);
        }
        let Some(next_allowed) = now.checked_add(interval) else {
            return Some(interval);
        };
        state.next_allowed = next_allowed;
        None
    }
}

/// The spacing `requests_per_second` asks for, if it is a positive rate whose
/// interval fits a [`Duration`].
fn request_interval(requests_per_second: f64) -> Option<Duration> {
    if !requests_per_second.is_finite() || requests_per_second <= 0.0 {
        return None;
    }
    Duration::try_from_secs_f64(1.0 / requests_per_second).ok()
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
    let until_reset = if reset_value < EPOCH_2000 {
        reset_value
    } else {
        let now_unix = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .ok()?
            .as_secs();
        if reset_value <= now_unix {
            return None;
        }
        reset_value - now_unix
    };
    let reset_at = Instant::now().checked_add(Duration::from_secs(until_reset))?;

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
    max_body_bytes: Option<usize>,
    /// Bounds the whole request, every redirect hop included, with the waits
    /// for pacing, concurrency caps, retries, backoff and `Retry-After`.
    timeout_ms: Option<u64>,
    /// How long to keep the response for the same request; see [`cache`].
    cache_ttl_ms: Option<u64>,
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
    /// The response's own headers forbid keeping it; see [`cache::store_vetoed`].
    store_vetoed: bool,
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
            store_vetoed: false,
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
            store_vetoed: false,
        }
    }

    fn from_reqwest_error(mut err: reqwest::Error) -> Self {
        if let Some(url) = err.url_mut() {
            strip_credentials(url);
        }
        let status_code = err.status().map(|s| s.as_u16());
        let (kind, message) = classify_reqwest_error(&err);
        Self::error(kind, status_code, message)
    }

    fn with_retry_info(mut self, retries: u32, rate_limited: bool) -> Self {
        self.retries = retries;
        self.rate_limited = rate_limited;
        self
    }
}

/// Drops the parts of `url` that carry credentials, so it can be logged or
/// shown to a plugin: userinfo, query and fragment.
fn strip_credentials(url: &mut url::Url) {
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
}

fn loggable_url(raw: &str) -> String {
    match url::Url::parse(raw) {
        Ok(mut url) => {
            strip_credentials(&mut url);
            url.into()
        }
        Err(_) => "<invalid url>".to_string(),
    }
}

fn classify_reqwest_error(err: &reqwest::Error) -> (&'static str, String) {
    if err.is_timeout() {
        ("timeout", "request timed out".to_string())
    } else if err.is_connect() {
        ("connect", format!("failed to connect: {err}"))
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
        Err(err) => return HttpResponse::from_reqwest_error(err),
    };

    let status_code = resp.status().as_u16();
    let status_message = resp
        .status()
        .canonical_reason()
        .unwrap_or("Unknown Status")
        .to_string();
    let headers = extract_headers(resp.headers());
    let cookies = extract_cookies(resp.headers());
    let store_vetoed = cache::store_vetoed(resp.headers());

    let body = match read_body(resp, options.max_body_bytes).await {
        Ok(body) => body,
        Err(response) => return response,
    };

    let mut response = HttpResponse::success(status_code, status_message, headers, cookies, body);
    response.store_vetoed = store_vetoed;
    response
}

async fn read_body(
    mut resp: reqwest::Response,
    max_bytes: Option<usize>,
) -> Result<Vec<u8>, HttpResponse> {
    let status = resp.status().as_u16();
    let too_large = |limit: usize| {
        HttpResponse::error(
            "body",
            Some(status),
            format!("response body exceeds {limit} bytes"),
        )
    };
    if let Some(limit) = max_bytes
        && resp
            .content_length()
            .is_some_and(|length| length > limit as u64)
    {
        return Err(too_large(limit));
    }
    let mut body = Vec::new();
    loop {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                if let Some(limit) = max_bytes
                    && body.len() + chunk.len() > limit
                {
                    return Err(too_large(limit));
                }
                body.extend_from_slice(&chunk);
            }
            Ok(None) => return Ok(body),
            Err(err) => return Err(HttpResponse::from_reqwest_error(err)),
        }
    }
}

async fn acquire_concurrency_permit(domain: Option<&str>) -> Option<OwnedSemaphorePermit> {
    let host = domain?;
    loop {
        let semaphore = {
            let limiter = concurrency_limiter();
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

/// Waits for `domain`'s next request slot and takes it, in turn behind the
/// requests that asked before.
async fn pace(domain: &str) {
    let Some(turn) = rate_limiter().turn(domain) else {
        return;
    };
    let _turn = turn.lock().await;
    loop {
        let wait_time = rate_limiter().acquire(domain);
        let Some(wait_time) = wait_time else {
            return;
        };
        tokio::time::sleep(wait_time).await;
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
        let limiter = rate_limiter();
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
            pace(d).await;
        }

        let response = request_fn(options.clone()).await;

        if let Some(ref d) = domain
            && let Some(info) = parse_rate_limit_headers(&response.headers)
        {
            let mut limiter = rate_limiter();
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

/// The `error_kind` of a request refused because the plugin's hosts do not
/// allow the host of its URL, or of a redirect it was sent to.
pub const HOST_NOT_ALLOWED: &str = "host_not_allowed";

/// The error answering a request to `url` that `allowed` refuses.
fn host_refusal(allowed: Option<&hosts::AllowedHosts>, url: &url::Url) -> Option<HttpResponse> {
    let reason = allowed?.refusal(url)?;
    tracing::debug!(reason, "request refused");
    Some(HttpResponse::error(HOST_NOT_ALLOWED, None, reason))
}

/// Every outbound request, a plugin's own or one the host makes on its
/// behalf, runs through here, attributed to `plugin`. Each hop of a redirect
/// chain runs under the limits of the host it reaches, and reaches it only
/// when the plugin's hosts, as they stand when the request starts, allow it.
/// A request outside any plugin reaches any host.
async fn execute_request(plugin: Option<&str>, options: HttpRequestOptions) -> HttpResponse {
    let span = tracing::debug_span!("http_request", plugin, url = loggable_url(&options.url));
    let request = async {
        let allowed = plugin.map(hosts::snapshot);
        // Ahead of the cache, which answers without following anything.
        if let Ok(url) = url::Url::parse(&options.url)
            && is_http_scheme(&url)
            && let Some(refused) = host_refusal(allowed.as_deref(), &url)
        {
            return refused;
        }
        cache::cached(plugin, options, |options| {
            run_request(options, allowed.clone(), |opts| async move {
                execute_single_request(&opts).await
            })
        })
        .await
    };
    tracing::Instrument::instrument(request, span).await
}

/// Runs `options` and the redirects it leads to within its `timeout_ms`,
/// answering a `timeout` error once it runs out. `request_fn` sends each
/// attempt; tests pass a fake in place of [`execute_single_request`].
async fn run_request<F, Fut>(
    options: HttpRequestOptions,
    allowed: Option<Arc<hosts::AllowedHosts>>,
    request_fn: F,
) -> HttpResponse
where
    F: Fn(HttpRequestOptions) -> Fut,
    Fut: std::future::Future<Output = HttpResponse>,
{
    let Some(timeout_ms) = options.timeout_ms else {
        return follow_redirects(options, allowed.as_deref(), request_fn).await;
    };
    tokio::time::timeout(
        Duration::from_millis(timeout_ms),
        follow_redirects(options, allowed.as_deref(), request_fn),
    )
    .await
    .unwrap_or_else(|_| {
        HttpResponse::error(
            "timeout",
            None,
            format!("request timed out after {timeout_ms} ms"),
        )
    })
}

async fn follow_redirects<F, Fut>(
    mut options: HttpRequestOptions,
    allowed: Option<&hosts::AllowedHosts>,
    request_fn: F,
) -> HttpResponse
where
    F: Fn(HttpRequestOptions) -> Fut,
    Fut: std::future::Future<Output = HttpResponse>,
{
    let mut visited: Vec<url::Url> = Vec::new();
    loop {
        let url = match url::Url::parse(&options.url) {
            Ok(url) => url,
            Err(err) => {
                return HttpResponse::error("builder", None, format!("invalid URL: {err}"));
            }
        };
        if visited.is_empty() {
            if !is_http_scheme(&url) {
                return HttpResponse::error(
                    "request",
                    None,
                    format!("unsupported URL scheme '{}'", url.scheme()),
                );
            }
        } else if let Some(reason) = redirect_refusal(&url, &visited) {
            return HttpResponse::error("redirect", None, reason);
        }
        if let Some(refused) = host_refusal(allowed, &url) {
            return refused;
        }

        let response = execute_request_loop(options.clone(), &request_fn).await;
        let Some(next) = redirect_target(&response, &url) else {
            return response;
        };
        options = redirected(options, response.status_code, &url, &next);
        visited.push(url);
    }
}

/// Where a redirect response points, resolved against the URL it came from.
fn redirect_target(response: &HttpResponse, url: &url::Url) -> Option<url::Url> {
    if !response.success || !matches!(response.status_code, Some(301 | 302 | 303 | 307 | 308)) {
        return None;
    }
    let location = response.headers.0.get("location")?;
    url.join(location).ok()
}

/// The request to send to `next` after `status` redirected `options` away
/// from `from`, as browsers and reqwest rewrite it: 301, 302 and 303 turn
/// any method but GET and HEAD into a bodiless GET, and credentials stay
/// behind on another origin.
fn redirected(
    mut options: HttpRequestOptions,
    status: Option<u16>,
    from: &url::Url,
    next: &url::Url,
) -> HttpRequestOptions {
    const BODY_HEADERS: &[&str] = &[
        "content-type",
        "content-length",
        "content-encoding",
        "transfer-encoding",
    ];
    const CREDENTIAL_HEADERS: &[&str] = &[
        "authorization",
        "cookie",
        "proxy-authorization",
        "www-authenticate",
    ];
    let mut dropped: Vec<&str> = Vec::new();
    if matches!(status, Some(301..=303))
        && !matches!(options.method, HttpMethod::Get | HttpMethod::Head)
    {
        options.method = HttpMethod::Get;
        options.body = None;
        dropped.extend(BODY_HEADERS);
    }
    if from.origin() != next.origin() {
        options.cookies = None;
        dropped.extend(CREDENTIAL_HEADERS);
    }
    if let Some(headers) = options.headers.as_mut() {
        headers
            .0
            .retain(|key, _| !dropped.iter().any(|name| key.eq_ignore_ascii_case(name)));
    }
    options.url = next.to_string();
    options
}

/// A request the host makes on a plugin's behalf through [`request_as`].
#[derive(Clone, Debug)]
pub struct Request {
    method: HttpMethod,
    url: String,
    max_body_bytes: Option<usize>,
}

impl Request {
    pub fn get(url: impl Into<String>) -> Self {
        Self {
            method: HttpMethod::Get,
            url: url.into(),
            max_body_bytes: None,
        }
    }

    /// Fails the request once its response body would exceed `limit`.
    pub fn max_body_bytes(mut self, limit: usize) -> Self {
        self.max_body_bytes = Some(limit);
        self
    }
}

/// A response with any status, as a plugin's request receives it.
#[derive(Clone, Debug)]
pub struct Response {
    pub status: u16,
    pub body: Vec<u8>,
}

/// A request that got no response, with the `error_kind` a plugin's request
/// would report.
#[derive(Clone, Debug)]
pub struct Error {
    kind: String,
    message: String,
}

impl Error {
    pub fn kind(&self) -> &str {
        &self.kind
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

/// Makes `request` on behalf of `plugin_id`, as the plugin's own request
/// would run: under the same per-host rate limits, cooldowns and
/// concurrency caps, with the default user agent and redirect policy, and
/// to the plugin's hosts alone. The host decides whether the plugin may
/// reach the network; this only runs the request.
pub async fn request_as(plugin_id: &str, request: Request) -> Result<Response, Error> {
    let options = HttpRequestOptions {
        url: request.url,
        method: request.method,
        body: None,
        headers: None,
        cookies: None,
        max_body_bytes: request.max_body_bytes,
        timeout_ms: None,
        cache_ttl_ms: None,
    };
    let response = execute_request(Some(plugin_id), options).await;
    match (response.success, response.status_code) {
        (true, Some(status)) => Ok(Response {
            status,
            body: response.body.0,
        }),
        _ => Err(Error {
            kind: response.error_kind.unwrap_or_else(|| "unknown".to_string()),
            message: response.status_message,
        }),
    }
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

/// The module plugins send HTTP requests through.
///
/// A plugin's requests reach only the hosts set for it, and none until some
/// are, so a host application installing this module for plugins must:
///
/// - register [`HostsGrant`] for the scope it gates the module behind with
///   `PluginHost::with_scope_grant`, so manifests list their hosts and
///   updates reaching more are seen;
/// - call [`set_plugin_hosts`] with each plugin's own list before the
///   plugin's entrypoint runs, on every start, reload and restart, passing
///   the host each named entry stands for in its place;
/// - call [`remove_plugin_hosts`] when a plugin is torn down or uninstalled.
///
/// A chunk outside any plugin is not bound by any list.
pub struct HttpModule;

async fn configure_rate_limit(
    plugin_id: Option<Arc<str>>,
    options: HttpRateLimitOptions,
) -> anyhow::Result<()> {
    let requests_per_second = options.requests_per_second.unwrap_or(1.0);
    if request_interval(requests_per_second).is_none() {
        anyhow::bail!(
            "requests_per_second must be a positive number whose request interval fits a duration"
        );
    }

    let domain = host_key(&options.domain);

    let config = RateLimitConfig {
        requests_per_second,
        retry_status_codes: options.retry_on.unwrap_or_else(|| vec![429, 503]),
        max_retries: options.max_retries.unwrap_or(3),
        initial_backoff: Duration::from_millis(options.backoff_ms.unwrap_or(1000)),
    };

    let mut limiter = rate_limiter();
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

    let mut limiter = concurrency_limiter();
    limiter.set_limit(host, options.max_in_flight as usize, plugin_id);

    Ok(())
}

fn install_fields(spec: harmony_core::ModuleSpec) -> harmony_core::ModuleSpec {
    spec.initializer(|_vm, _origin, root| {
        root.set(
            "MAX_CACHE_TTL_MS",
            luau::Value::Number(cache::MAX_CACHE_TTL_MS as f64),
        )?;
        root.set(
            "DEFAULT_MAX_BODY_BYTES",
            luau::Value::Number(DEFAULT_MAX_BODY_BYTES as f64),
        )?;
        root.set("MAX_BODY_BYTES", luau::Value::Number(MAX_BODY_BYTES as f64))
    })
}

fn describe_fields(descriptor: &mut harmony_luau::ModuleDescriptor) {
    descriptor.fields.push(harmony_luau::ModuleFieldDescriptor {
        path: vec!["MAX_CACHE_TTL_MS"],
        description: Some("The longest a request's `cache_ttl_ms` keeps its response."),
        ty: u64::luau_type(),
    });
    descriptor.fields.push(harmony_luau::ModuleFieldDescriptor {
        path: vec!["DEFAULT_MAX_BODY_BYTES"],
        description: Some("The response body cap of a request that sets no `max_body_bytes`."),
        ty: u64::luau_type(),
    });
    descriptor.fields.push(harmony_luau::ModuleFieldDescriptor {
        path: vec!["MAX_BODY_BYTES"],
        description: Some("The largest `max_body_bytes` a request may set."),
        ty: u64::luau_type(),
    });
}

/// The capability hosts conventionally gate this module behind.
pub const SCOPE: &str = "harmony.http";

#[harmony_macros::module(
    id = "harmony/http",
    name = "Http",
    local = "http",
    description = "Outbound HTTP requests, rate limits, and URI encoding.",
    userdata(HttpMethod),
    extend = install_fields,
    describe = describe_fields
)]
impl HttpModule {
    #[harmony(
        description = "Makes an outbound HTTP request.",
        args(options: HttpRequestOptions),
        returns(HttpResponse)
    )]
    fn request(
        origin: &luau::ChunkOrigin,
        options: luau::Table,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        let options = request_options_from_luau(&options)?;
        let plugin_id = origin.plugin.clone();
        Ok(luau::ScheduledFuture::new(async move {
            let response = execute_request(plugin_id.as_deref(), options).await;
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

fn request_options_from_luau(table: &luau::Table) -> luau::runtime::Result<HttpRequestOptions> {
    Ok(HttpRequestOptions {
        url: table.required_string_field("url")?,
        method: required_method_field(table, "method")?,
        body: optional_binary_field(table, "body")?,
        headers: optional_header_map_field(table, "headers")?,
        cookies: optional_header_map_field(table, "cookies")?,
        max_body_bytes: Some(luau_max_body_bytes(table)?),
        timeout_ms: optional_u64_field(table, "timeout_ms")?,
        cache_ttl_ms: optional_u64_field(table, "cache_ttl_ms")?,
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

/// The response body cap a Luau request's options table asks for, validated
/// against [`MAX_BODY_BYTES`] and defaulting to [`DEFAULT_MAX_BODY_BYTES`].
pub fn luau_max_body_bytes(table: &luau::Table) -> luau::runtime::Result<usize> {
    let value = match table.get("max_body_bytes")? {
        luau::Value::Nil => return Ok(DEFAULT_MAX_BODY_BYTES),
        luau::Value::Integer(value) => value,
        luau::Value::Number(value) if value.is_finite() && value.fract() == 0.0 => value as i64,
        other => {
            return Err(luau_field_type_error(
                "max_body_bytes",
                "integer",
                other.type_name(),
            ));
        }
    };
    match usize::try_from(value) {
        Ok(bytes) if (1..=MAX_BODY_BYTES).contains(&bytes) => Ok(bytes),
        _ => Err(luau::Error::Runtime(format!(
            "max_body_bytes must be between 1 and MAX_BODY_BYTES ({MAX_BODY_BYTES}), got {value}"
        ))),
    }
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
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<HttpMethod> {
    let value = table.get(field)?;
    if matches!(value, luau::Value::Nil) {
        return Err(luau::Error::Runtime(format!("missing '{field}' field")));
    }
    HttpMethod::class().read_value(field, value)
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
    let limiter = rate_limiter();
    limiter.has_entry_for_plugin(plugin_id)
}

/// Marks every limit `plugin_id` set as stale: each still applies, but no
/// longer counts for [`has_rate_limit_for_plugin`] until the plugin sets it
/// again.
pub async fn mark_plugin_limits_stale(plugin_id: &str) {
    rate_limiter().mark_plugin_stale(plugin_id);
    concurrency_limiter().mark_plugin_stale(plugin_id);
}

/// Drops every response `plugin_id` asked to keep, and keeps the requests it has in flight
/// from keeping theirs.
pub fn clear_plugin_cache(plugin_id: &str) {
    cache::clear_plugin(plugin_id);
}

/// Drops the limits of `plugin_id` still stale. Each host falls back to the
/// strictest limit its other owners hold.
pub async fn remove_stale_plugin_limits(plugin_id: &str) {
    rate_limiter().remove_stale_plugin(plugin_id);
    concurrency_limiter().remove_stale_plugin(plugin_id);
}

/// Whether `plugin_id` holds any limit, stale or live.
#[doc(hidden)]
pub async fn test_holds_any_limit(plugin_id: &str) -> bool {
    let rate = rate_limiter()
        .domains
        .values()
        .any(|state| state.configs.has_any(plugin_id));
    let concurrency = concurrency_limiter()
        .entries
        .values()
        .any(|entry| entry.limits.has_any(plugin_id));
    rate || concurrency
}

#[doc(hidden)]
pub async fn test_seed_rate_limit(domain: impl Into<String>, plugin_id: impl Into<Arc<str>>) {
    let mut limiter = rate_limiter();
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
            FieldDescriptor {
                name: "max_body_bytes",
                ty: Option::<u64>::luau_type(),
                description: Some(
                    "Caps the response body, between `1` and `MAX_BODY_BYTES`; a larger body fails the response with `error_kind` `body`. Default `DEFAULT_MAX_BODY_BYTES`.",
                ),
            },
            FieldDescriptor {
                name: "timeout_ms",
                ty: Option::<u64>::luau_type(),
                description: Some(
                    "Bounds the whole request, every redirect hop included, with the waits for pacing, concurrency caps, retries, backoff and `Retry-After`; past it the response fails with `error_kind` `timeout`.",
                ),
            },
            FieldDescriptor {
                name: "cache_ttl_ms",
                ty: Option::<u64>::luau_type(),
                description: Some(
                    "Keeps the response this long, at most `MAX_CACHE_TTL_MS`, and answers the same request with it meanwhile without sending it, pacing it or retrying it. Only a GET without a body, cookies or `Authorization`/`Proxy-Authorization` header is kept, only once it ends in a 2xx response the server lets a shared cache store, and per plugin, URL and headers; a redirected request is kept under the URL asked for. `0` sends the request and drops what was kept for it. Default none: nothing is read or kept.",
                ),
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
                description: Some(
                    "Transport error category when `success` is false; `host_not_allowed` when the plugin's manifest does not list the host of the URL, or of a redirect.",
                ),
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
    use std::time::Duration;

    use tokio::time::Instant;

    use harmony_core::UserDataBinding;

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
        scheduler.spawn_luau_thread(harmony_core::CallContext::default(), thread, vec![]);

        assert_eq!(scheduler.poll_ready(), 1);
        assert_eq!(scheduler.poll_ready(), 0);
        assert_eq!(scheduler.poll_ready(), 0);

        assert!(super::has_rate_limit_for_plugin("luau-http-plugin").await);
        {
            let limiter = super::concurrency_limiter();
            assert_eq!(max_in_flight(&limiter, "example.com"), Some(2));
        }
        let mut values = vm.eval(
            std::sync::Arc::<[u8]>::from(&b"return stored_method"[..]),
            harmony_luau::ChunkOrigin::default(),
        )?;
        assert_eq!(values.len(), 1);
        let stored_method =
            super::HttpMethod::class().read_value("stored_method", values.pop().unwrap())?;
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
    fn rate_limit_reset_past_instant_range_is_ignored() {
        let headers = super::HttpHeaderMap(std::collections::BTreeMap::from([
            ("x-ratelimit-remaining".to_string(), "0".to_string()),
            ("x-ratelimit-reset".to_string(), u64::MAX.to_string()),
        ]));
        assert!(super::parse_rate_limit_headers(&headers).is_none());
    }

    #[tokio::test]
    async fn rate_too_small_for_an_interval_is_rejected() {
        let options = super::HttpRateLimitOptions {
            domain: "tiny-rate.example".to_string(),
            requests_per_second: Some(1e-300),
            retry_on: None,
            max_retries: None,
            backoff_ms: None,
        };
        let error = super::configure_rate_limit(None, options)
            .await
            .expect_err("an interval past Duration::MAX is refused");
        assert!(error.to_string().contains("requests_per_second"), "{error}");
        assert!(
            super::rate_limiter()
                .get_config("tiny-rate.example")
                .is_none()
        );
    }

    #[test]
    fn interval_past_instant_range_waits_instead_of_panicking() {
        let mut limiter = super::RateLimiter::new();
        let domain = "slow.example";
        limiter.set_config(
            domain.to_string(),
            rate_limit_config(1.0 / 1.8e19),
            Some("demo".into()),
        );
        assert!(limiter.acquire(domain).is_some());
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
                reset_at: tokio::time::Instant::now() + std::time::Duration::from_secs(60),
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
            let mut limiter = super::concurrency_limiter();
            limiter.set_limit(host.to_string(), 2, Some("loose".into()));
        }
        let first = super::acquire_concurrency_permit(Some(host)).await;
        let _second = super::acquire_concurrency_permit(Some(host)).await;
        let waiter = tokio::spawn(super::acquire_concurrency_permit(Some(host)));
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        {
            let mut limiter = super::concurrency_limiter();
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
            let mut limiter = super::concurrency_limiter();
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
            let mut limiter = super::concurrency_limiter();
            limiter.set_limit(host.clone(), 1, None);
        }

        let sem = {
            let limiter = super::concurrency_limiter();
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
            let mut limiter = super::concurrency_limiter();
            limiter.set_limit(host.clone(), 1, None);
        }
        {
            let mut limiter = super::rate_limiter();
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
            max_body_bytes: None,
            timeout_ms: None,
            cache_ttl_ms: None,
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
            let limiter = super::concurrency_limiter();
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

    fn url(raw: &str) -> url::Url {
        url::Url::parse(raw).expect("valid test URL")
    }

    #[test]
    fn redirects_keep_to_http_and_never_downgrade() {
        let refusal = |next: &str, previous: &[&str]| {
            let previous: Vec<_> = previous.iter().map(|raw| url(raw)).collect();
            super::redirect_refusal(&url(next), &previous)
        };

        assert_eq!(refusal("https://b.example/", &["http://a.example/"]), None);
        assert_eq!(refusal("http://b.example/", &["http://a.example/"]), None);
        assert!(refusal("http://b.example/", &["https://a.example/"]).is_some());
        assert!(
            refusal(
                "http://c.example/",
                &["https://a.example/", "https://b.example/"]
            )
            .is_some()
        );
        assert!(refusal("file:///etc/passwd", &["http://a.example/"]).is_some());
        assert!(refusal("ftp://a.example/", &["https://a.example/"]).is_some());
    }

    #[test]
    fn redirects_stop_after_the_hop_limit() {
        let chain: Vec<_> = (0..=super::MAX_REDIRECTS)
            .map(|hop| url(&format!("https://a.example/{hop}")))
            .collect();
        let next = url("https://a.example/next");

        assert_eq!(
            super::redirect_refusal(&next, &chain[..super::MAX_REDIRECTS]),
            None
        );
        assert!(super::redirect_refusal(&next, &chain).is_some());
    }

    /// Serves each connection the response `respond` builds from its request
    /// path and full request text, on a local port.
    async fn serve(respond: fn(&str, &str, u16) -> Vec<u8>) -> u16 {
        use tokio::io::{
            AsyncReadExt,
            AsyncWriteExt,
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let port = listener.local_addr().expect("test server address").port();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buffer = vec![0; 4096];
                    let read = stream.read(&mut buffer).await.unwrap_or(0);
                    let request = String::from_utf8_lossy(&buffer[..read]);
                    let path = request.split_whitespace().nth(1).unwrap_or("/").to_string();
                    let _ = stream.write_all(&respond(&path, &request, port)).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        port
    }

    fn ok(body: &[u8]) -> Vec<u8> {
        let mut response = format!(
            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(body);
        response
    }

    fn redirect(location: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 302 Found\r\nlocation: {location}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
        )
        .into_bytes()
    }

    fn allow(plugin_id: &str, hosts: &[&str]) {
        super::set_plugin_hosts(
            plugin_id,
            hosts.iter().map(|host| host.parse().expect("host entry")),
        );
    }

    fn get(url: String) -> super::HttpRequestOptions {
        options(super::HttpMethod::Get, url)
    }

    #[tokio::test]
    async fn a_plugin_reaches_only_its_hosts() {
        use std::sync::atomic::{
            AtomicUsize,
            Ordering,
        };
        static HITS: AtomicUsize = AtomicUsize::new(0);

        let port = serve(|_, _, _| {
            HITS.fetch_add(1, Ordering::SeqCst);
            ok(b"reached")
        })
        .await;
        let url = format!("http://127.0.0.1:{port}/");

        let response = super::execute_request(Some("hosts-none-set"), get(url.clone())).await;
        assert_eq!(
            response.error_kind.as_deref(),
            Some(super::HOST_NOT_ALLOWED),
            "a plugin without a list reaches no host"
        );
        assert_eq!(response.status_code, None);
        assert_eq!(
            response.status_message,
            "host '127.0.0.1' is not among the plugin's hosts"
        );

        allow("hosts-other", &["localhost", "*.example.org"]);
        let response = super::execute_request(Some("hosts-other"), get(url.clone())).await;
        assert_eq!(
            response.error_kind.as_deref(),
            Some(super::HOST_NOT_ALLOWED)
        );
        let error = super::request_as("hosts-other", super::Request::get(url.clone()))
            .await
            .expect_err("a request on the plugin's behalf is bound by its hosts");
        assert_eq!(error.kind(), super::HOST_NOT_ALLOWED);
        assert_eq!(HITS.load(Ordering::SeqCst), 0, "nothing was sent");

        allow("hosts-listed", &["127.0.0.1"]);
        let response = super::execute_request(Some("hosts-listed"), get(url.clone())).await;
        assert!(response.success, "{:?}", response.status_message);
        let response = super::execute_request(None, get(url)).await;
        assert!(
            response.success,
            "a request outside any plugin is not bound"
        );
        assert_eq!(HITS.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn every_redirect_hop_must_be_among_the_plugins_hosts() {
        // `127.0.0.1` and `localhost` are two hosts.
        let port = serve(|path, _, port| match path {
            "/start" => redirect(&format!("http://localhost:{port}/landed")),
            _ => ok(b"landed"),
        })
        .await;
        let start = format!("http://127.0.0.1:{port}/start");

        allow("hosts-first-hop", &["127.0.0.1"]);
        let response = super::execute_request(Some("hosts-first-hop"), get(start.clone())).await;
        assert_eq!(
            response.error_kind.as_deref(),
            Some(super::HOST_NOT_ALLOWED)
        );
        assert_eq!(
            response.status_message,
            "host 'localhost' is not among the plugin's hosts"
        );

        allow("hosts-both-hops", &["127.0.0.1", "localhost"]);
        let response = super::execute_request(Some("hosts-both-hops"), get(start)).await;
        assert!(response.success, "{:?}", response.status_message);
        assert_eq!(response.body.0, b"landed");
    }

    #[tokio::test]
    async fn a_request_keeps_the_hosts_it_started_with() {
        let port = serve(|path, _, port| match path {
            "/start" => {
                super::remove_plugin_hosts("hosts-snapshot");
                redirect(&format!("http://localhost:{port}/landed"))
            }
            _ => ok(b"landed"),
        })
        .await;
        allow("hosts-snapshot", &["127.0.0.1", "localhost"]);

        let start = format!("http://127.0.0.1:{port}/start");
        let response = super::execute_request(Some("hosts-snapshot"), get(start.clone())).await;
        assert!(response.success, "{:?}", response.status_message);

        let response = super::execute_request(Some("hosts-snapshot"), get(start)).await;
        assert_eq!(
            response.error_kind.as_deref(),
            Some(super::HOST_NOT_ALLOWED),
            "the next request reads the hosts anew"
        );
    }

    #[tokio::test]
    async fn a_kept_response_does_not_answer_for_a_host_no_longer_allowed() {
        use std::sync::atomic::{
            AtomicUsize,
            Ordering,
        };
        static HITS: AtomicUsize = AtomicUsize::new(0);

        let port = serve(|_, _, _| {
            HITS.fetch_add(1, Ordering::SeqCst);
            ok(b"kept")
        })
        .await;
        let request = || {
            let mut options = get(format!("http://127.0.0.1:{port}/"));
            options.cache_ttl_ms = Some(60_000);
            options
        };

        allow("hosts-cached", &["127.0.0.1"]);
        for _ in 0..2 {
            let response = super::execute_request(Some("hosts-cached"), request()).await;
            assert!(response.success, "{:?}", response.status_message);
        }
        assert_eq!(HITS.load(Ordering::SeqCst), 1, "the response was kept");

        allow("hosts-cached", &[]);
        let response = super::execute_request(Some("hosts-cached"), request()).await;
        assert_eq!(
            response.error_kind.as_deref(),
            Some(super::HOST_NOT_ALLOWED)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn luau_sees_a_refused_host_as_an_error_kind() -> harmony_luau::runtime::Result<()> {
        let vm = harmony_luau::Vm::new()?;
        vm.data().insert(harmony_core::LocalScheduler::new())?;
        let scheduler = vm.data().get::<harmony_core::LocalScheduler>()?;
        let origin = harmony_core::ChunkOrigin {
            plugin: Some(std::sync::Arc::from("luau-hosts-plugin")),
            ..harmony_core::ChunkOrigin::default()
        };
        let spec = HttpModule::module_spec();
        let table = harmony_core::luau::install_module(&vm, &origin, &spec)?;
        vm.set_global_table("http", &table)?;
        allow("luau-hosts-plugin", &["listed.example"]);

        let root = vm.load_chunk(&harmony_luau::Chunk::new(
            std::sync::Arc::<[u8]>::from(
                &br#"
                    local response = http.request({
                        url = "https://unlisted.example/path?key=secret",
                        method = http.HttpMethod.Get,
                    })
                    seen_success = response.success
                    seen_status = response.status_code
                    seen_kind = response.error_kind
                    seen_message = response.status_message
                "#[..],
            ),
            harmony_luau::ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&root)?;
        scheduler.spawn_luau_thread(harmony_core::CallContext::default(), thread, vec![]);
        for _ in 0..100 {
            scheduler.poll_ready();
            tokio::task::yield_now().await;
        }

        let values = vm.eval(
            std::sync::Arc::<[u8]>::from(
                &b"return seen_success, seen_status, seen_kind, seen_message"[..],
            ),
            harmony_luau::ChunkOrigin::default(),
        )?;
        let text = |value: &harmony_luau::Value| match value {
            harmony_luau::Value::String(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            other => format!("{other:?}"),
        };
        assert_eq!(
            values.iter().map(text).collect::<Vec<_>>(),
            [
                "Boolean(false)",
                "Nil",
                "host_not_allowed",
                "host 'unlisted.example' is not among the plugin's hosts",
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn request_as_follows_redirects_up_to_the_limit() {
        let port = serve(|path, _, port| {
            let hop: usize = path.trim_start_matches('/').parse().unwrap_or(0);
            if hop == 0 {
                ok(b"done")
            } else {
                redirect(&format!("http://127.0.0.1:{port}/{}", hop - 1))
            }
        })
        .await;
        let at = |hop: usize| super::Request::get(format!("http://127.0.0.1:{port}/{hop}"));
        allow("test", &["127.0.0.1"]);

        let response = super::request_as("test", at(super::MAX_REDIRECTS))
            .await
            .expect("redirects within the limit are followed");
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"done");

        let error = super::request_as("test", at(super::MAX_REDIRECTS + 1))
            .await
            .expect_err("one hop past the limit fails");
        assert_eq!(error.kind(), "redirect");
    }

    #[tokio::test]
    async fn request_as_caps_the_response_body() {
        let port = serve(|path, _, _| match path {
            "/declared" => ok(&[0; 64]),
            _ => {
                let mut response = b"HTTP/1.1 200 OK\r\nconnection: close\r\n\r\n".to_vec();
                response.extend_from_slice(&[0; 64]);
                response
            }
        })
        .await;
        let request = |path: &str| super::Request::get(format!("http://127.0.0.1:{port}{path}"));
        allow("test", &["127.0.0.1"]);

        for path in ["/declared", "/streamed"] {
            let error = super::request_as("test", request(path).max_body_bytes(63))
                .await
                .expect_err("a body past the cap fails");
            assert_eq!(error.kind(), "body", "{path}");
            let response = super::request_as("test", request(path).max_body_bytes(64))
                .await
                .expect("a body within the cap is read");
            assert_eq!(response.body.len(), 64, "{path}");
        }
    }

    /// The options `http.request` reads from the Luau table `fields` builds.
    fn luau_request_options(
        fields: &str,
    ) -> harmony_luau::runtime::Result<super::HttpRequestOptions> {
        let vm = harmony_luau::Vm::new()?;
        let spec = HttpModule::module_spec();
        let table =
            harmony_core::luau::install_module(&vm, &harmony_core::ChunkOrigin::default(), &spec)?;
        vm.set_global_table("http", &table)?;
        let source = format!(
            "return {{ url = \"http://a.example/\", method = http.HttpMethod.Get, {fields} }}"
        );
        let values = vm.eval(
            std::sync::Arc::<[u8]>::from(source.as_bytes()),
            harmony_luau::ChunkOrigin::default(),
        )?;
        let Some(harmony_luau::Value::Table(options)) = values.into_iter().next() else {
            panic!("the chunk returns a table");
        };
        super::request_options_from_luau(&options)
    }

    #[test]
    fn luau_requests_cap_the_body_by_default() -> harmony_luau::runtime::Result<()> {
        assert_eq!(
            luau_request_options("")?.max_body_bytes,
            Some(super::DEFAULT_MAX_BODY_BYTES)
        );
        assert_eq!(
            luau_request_options("max_body_bytes = 1")?.max_body_bytes,
            Some(1)
        );
        assert_eq!(
            luau_request_options("max_body_bytes = http.MAX_BODY_BYTES")?.max_body_bytes,
            Some(super::MAX_BODY_BYTES)
        );
        Ok(())
    }

    #[test]
    fn luau_requests_refuse_body_caps_out_of_range() {
        for value in ["0", "-1", "http.MAX_BODY_BYTES + 1", "2 ^ 70", "-2 ^ 70"] {
            let error = luau_request_options(&format!("max_body_bytes = {value}"))
                .expect_err("an out-of-range cap is refused");
            assert!(
                error
                    .to_string()
                    .contains("max_body_bytes must be between 1 and MAX_BODY_BYTES"),
                "{value}: {error}"
            );
        }
        for value in ["1.5", "0 / 0", "1 / 0", "-1 / 0", "\"16\"", "true"] {
            let error = luau_request_options(&format!("max_body_bytes = {value}"))
                .expect_err("a value that is not an integer is refused");
            assert!(
                error.to_string().contains("'max_body_bytes'"),
                "{value}: {error}"
            );
        }
    }

    #[tokio::test]
    async fn luau_requests_fail_past_their_body_cap() -> harmony_luau::runtime::Result<()> {
        let port = serve(|_, _, _| ok(&[0; 64])).await;
        let url = format!("http://127.0.0.1:{port}/");

        let mut options = luau_request_options("max_body_bytes = 63")?;
        options.url = url.clone();
        let response = super::execute_request(None, options).await;
        assert!(!response.success);
        assert_eq!(response.error_kind.as_deref(), Some("body"));

        let mut options = luau_request_options("max_body_bytes = 64")?;
        options.url = url;
        let response = super::execute_request(None, options).await;
        assert!(response.success, "{:?}", response.error_kind);
        assert_eq!(response.body.0.len(), 64);
        Ok(())
    }

    #[tokio::test]
    async fn request_errors_omit_url_credentials() {
        let port = serve(|_, _, _| Vec::new()).await;
        allow("test", &["127.0.0.1"]);
        let url = format!("http://user:hunter2@127.0.0.1:{port}/rest/ping?apiKey=secret#frag");

        let error = super::request_as("test", super::Request::get(url))
            .await
            .expect_err("a connection closed without a response fails");
        let message = error.to_string();
        assert!(
            message.contains(&format!("127.0.0.1:{port}/rest/ping")),
            "{message}"
        );
        for secret in ["user", "hunter2", "apiKey", "secret", "frag"] {
            assert!(!message.contains(secret), "{secret} in {message}");
        }
    }

    #[test]
    fn loggable_url_omits_credentials() {
        assert_eq!(
            super::loggable_url("https://user:pw@api.example/v1/song?api_key=secret#x"),
            "https://api.example/v1/song"
        );
        assert_eq!(
            super::loggable_url("not a url?api_key=secret"),
            "<invalid url>"
        );
    }

    fn options(method: super::HttpMethod, url: String) -> super::HttpRequestOptions {
        super::HttpRequestOptions {
            url,
            method,
            body: None,
            headers: None,
            cookies: None,
            max_body_bytes: None,
            timeout_ms: None,
            cache_ttl_ms: None,
        }
    }

    #[tokio::test]
    async fn each_redirect_hop_runs_under_its_own_hosts_limits() {
        use std::sync::atomic::{
            AtomicBool,
            Ordering,
        };
        static FAILED_ONCE: AtomicBool = AtomicBool::new(false);

        // `127.0.0.1` and `localhost` are two hosts to the limiters.
        let port = serve(|path, _, port| match path {
            "/start" => redirect(&format!("http://localhost:{port}/limited")),
            _ if !FAILED_ONCE.swap(true, Ordering::SeqCst) => {
                b"HTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                    .to_vec()
            }
            _ => ok(b"limited"),
        })
        .await;
        super::rate_limiter().set_config(
            "localhost".to_string(),
            super::RateLimitConfig {
                requests_per_second: 100.0,
                retry_status_codes: vec![503],
                max_retries: 1,
                initial_backoff: std::time::Duration::from_millis(1),
            },
            None,
        );

        let response = super::execute_request(
            None,
            options(
                super::HttpMethod::Get,
                format!("http://127.0.0.1:{port}/start"),
            ),
        )
        .await;

        assert_eq!(response.status_code, Some(200));
        assert_eq!(response.body.0, b"limited");
        assert_eq!(
            response.retries, 1,
            "the redirect target retries under its own host's policy"
        );
    }

    #[tokio::test]
    async fn redirects_keep_credentials_to_their_origin_and_rewrite_see_other() {
        let port = serve(|path, request, port| {
            let request = request.to_ascii_lowercase();
            let echo = || {
                ok(format!(
                    "{} auth={} cookie={}",
                    request.split_whitespace().next().unwrap_or(""),
                    request.contains("authorization:"),
                    request.contains("cookie:"),
                )
                .as_bytes())
            };
            match path {
                "/same" => redirect(&format!("http://127.0.0.1:{port}/echo")),
                "/other" => redirect(&format!("http://localhost:{port}/echo")),
                "/see-other" => b"HTTP/1.1 303 See Other\r\nlocation: /echo\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                    .to_vec(),
                _ => echo(),
            }
        })
        .await;
        let request = |method, path: &str| {
            let mut options = options(method, format!("http://127.0.0.1:{port}{path}"));
            options.headers = Some(super::HttpHeaderMap(
                [("Authorization".to_string(), "secret".to_string())].into(),
            ));
            options.cookies = Some(super::HttpHeaderMap(
                [("session".to_string(), "secret".to_string())].into(),
            ));
            options
        };
        let body = |response: super::HttpResponse| String::from_utf8(response.body.0).unwrap();

        let same = super::execute_request(None, request(super::HttpMethod::Get, "/same")).await;
        assert_eq!(body(same), "get auth=true cookie=true");
        let other = super::execute_request(None, request(super::HttpMethod::Get, "/other")).await;
        assert_eq!(body(other), "get auth=false cookie=false");
        let see_other =
            super::execute_request(None, request(super::HttpMethod::Post, "/see-other")).await;
        assert_eq!(body(see_other), "get auth=true cookie=true");
    }

    #[tokio::test]
    async fn request_as_refuses_other_schemes() {
        let error = super::request_as("test", super::Request::get("file:///etc/passwd"))
            .await
            .expect_err("file URLs are refused");
        assert_eq!(error.kind(), "request");
    }

    // The timing tests below run on tokio's paused clock: sleeps end when the
    // runtime has nothing else to do, so durations are exact, not raced.

    fn options_with_timeout(url: &str, timeout_ms: Option<u64>) -> super::HttpRequestOptions {
        super::HttpRequestOptions {
            url: url.to_string(),
            method: super::HttpMethod::Get,
            body: None,
            headers: None,
            cookies: None,
            max_body_bytes: None,
            timeout_ms,
            cache_ttl_ms: None,
        }
    }

    fn set_test_rate_limit(host: &str, requests_per_second: f64, backoff: Duration) {
        super::rate_limiter().set_config(
            host.to_string(),
            super::RateLimitConfig {
                requests_per_second,
                retry_status_codes: vec![503],
                max_retries: 3,
                initial_backoff: backoff,
            },
            None,
        );
    }

    fn ok_response() -> super::HttpResponse {
        super::HttpResponse::success(
            200,
            "OK".into(),
            super::HttpHeaderMap::default(),
            super::HttpHeaderMap::default(),
            Vec::new(),
        )
    }

    fn assert_timed_out(response: &super::HttpResponse, started: Instant, timeout: Duration) {
        assert!(!response.success);
        assert_eq!(response.error_kind.as_deref(), Some("timeout"));
        assert_eq!(response.status_code, None);
        assert_eq!(
            started.elapsed(),
            timeout,
            "the request ends at its timeout"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_bounds_rate_limit_pacing() {
        let host = "timeout-pacing-test.example";
        set_test_rate_limit(host, 0.2, Duration::from_millis(1));
        assert_eq!(super::rate_limiter().acquire(host), None);

        let started = Instant::now();
        let response = super::run_request(
            options_with_timeout(&format!("https://{host}/x"), Some(100)),
            None,
            |_| async { ok_response() },
        )
        .await;
        assert_timed_out(&response, started, Duration::from_millis(100));
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_bounds_retry_backoff() {
        let host = "timeout-backoff-test.example";
        set_test_rate_limit(host, 1000.0, Duration::from_secs(1000));

        let started = Instant::now();
        let response = super::run_request(
            options_with_timeout(&format!("https://{host}/x"), Some(100)),
            None,
            |_| async { super::HttpResponse::error("retry", Some(503), "unavailable".into()) },
        )
        .await;
        assert_timed_out(&response, started, Duration::from_millis(100));
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_bounds_retry_after() {
        use std::sync::Arc;
        use std::sync::atomic::{
            AtomicUsize,
            Ordering,
        };

        let host = "timeout-retry-after-test.example";
        set_test_rate_limit(host, 1000.0, Duration::from_millis(1));
        let attempts = Arc::new(AtomicUsize::new(0));

        let started = Instant::now();
        let response = super::run_request(
            options_with_timeout(&format!("https://{host}/x"), Some(100)),
            None,
            |_| {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    let mut response =
                        super::HttpResponse::error("retry", Some(503), "unavailable".into());
                    response
                        .headers
                        .0
                        .insert("retry-after".to_string(), "120".to_string());
                    response
                }
            },
        )
        .await;
        assert_timed_out(&response, started, Duration::from_millis(100));
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "the timeout cuts the wait short"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_request_within_its_timeout_answers_as_usual() {
        let response = super::run_request(
            options_with_timeout("https://timeout-in-time-test.example/x", Some(1000)),
            None,
            |_| async { ok_response() },
        )
        .await;
        assert!(response.success);
        assert_eq!(response.status_code, Some(200));
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_bounds_a_whole_redirect_chain() {
        use std::sync::Arc;
        use std::sync::atomic::{
            AtomicUsize,
            Ordering,
        };

        let first = "timeout-chain-first.example";
        let second = "timeout-chain-second.example";
        let hops = Arc::new(AtomicUsize::new(0));

        // Each hop answers in 60 ms; the chain of them doesn't within 100.
        let started = Instant::now();
        let response = super::run_request(
            options_with_timeout(&format!("https://{first}/start"), Some(100)),
            None,
            |opts| {
                let hops = hops.clone();
                async move {
                    hops.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(60)).await;
                    let mut response = super::HttpResponse::success(
                        302,
                        "Found".into(),
                        super::HttpHeaderMap::default(),
                        super::HttpHeaderMap::default(),
                        Vec::new(),
                    );
                    let next = if opts.url.contains(first) {
                        format!("https://{second}/next")
                    } else {
                        format!("https://{first}/start?again")
                    };
                    response.headers.0.insert("location".to_string(), next);
                    response
                }
            },
        )
        .await;
        assert_timed_out(&response, started, Duration::from_millis(100));
        assert_eq!(hops.load(Ordering::SeqCst), 2, "the timeout spans the hops");
    }

    /// Starts a request per entry of `timeouts` on `host`, in order, each
    /// queued before the next starts, and returns when each was admitted
    /// (sent), or `None` for one that timed out.
    async fn admissions(host: &str, timeouts: &[Option<u64>]) -> Vec<Option<Duration>> {
        let started = Instant::now();
        let mut requests = Vec::new();
        for timeout_ms in timeouts {
            let url = format!("https://{host}/x");
            let timeout_ms = *timeout_ms;
            requests.push(tokio::spawn(async move {
                let admitted = std::sync::Arc::new(std::sync::Mutex::new(None));
                let response =
                    super::run_request(options_with_timeout(&url, timeout_ms), None, |_| {
                        let admitted = admitted.clone();
                        async move {
                            *admitted.lock().unwrap() = Some(started.elapsed());
                            ok_response()
                        }
                    })
                    .await;
                let admitted = *admitted.lock().unwrap();
                if response.success { admitted } else { None }
            }));
            // Lets the request reach the domain's queue before the next starts.
            tokio::task::yield_now().await;
        }
        let mut results = Vec::new();
        for request in requests {
            results.push(request.await.expect("request completes"));
        }
        results
    }

    #[tokio::test(start_paused = true)]
    async fn paced_requests_are_admitted_in_arrival_order() {
        let host = "pacing-order-test.example";
        set_test_rate_limit(host, 10.0, Duration::from_millis(1));
        assert_eq!(super::rate_limiter().acquire(host), None);

        let admitted = admissions(host, &[None, None, None, None]).await;
        let ms = |n: u64| Some(Duration::from_millis(n));
        assert_eq!(
            admitted,
            vec![ms(100), ms(200), ms(300), ms(400)],
            "one slot per interval, oldest first, so none waits behind later arrivals"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_request_arriving_as_a_slot_opens_waits_behind_older_ones() {
        let host = "pacing-arrival-test.example";
        set_test_rate_limit(host, 10.0, Duration::from_millis(1));
        assert_eq!(super::rate_limiter().acquire(host), None);
        let started = Instant::now();

        // Starts at the 100 ms slot, its timer ahead of the waiters' own.
        let late = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let admitted = std::sync::Arc::new(std::sync::Mutex::new(None));
            super::run_request(
                options_with_timeout(&format!("https://{host}/x"), None),
                None,
                |_| {
                    let admitted = admitted.clone();
                    async move {
                        *admitted.lock().unwrap() = Some(started.elapsed());
                        ok_response()
                    }
                },
            )
            .await;
            *admitted.lock().unwrap()
        });
        tokio::task::yield_now().await;
        let waiting = admissions(host, &[None, None]).await;
        let ms = |n: u64| Some(Duration::from_millis(n));
        assert_eq!(waiting, vec![ms(100), ms(200)]);
        assert_eq!(
            late.await.expect("late request completes"),
            ms(300),
            "the newcomer queues behind the requests already waiting"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_waiter_that_gives_up_frees_its_place() {
        let host = "pacing-cancel-test.example";
        set_test_rate_limit(host, 2.0, Duration::from_millis(1));
        assert_eq!(super::rate_limiter().acquire(host), None);

        // The second gives up at 600 ms while waiting for the 1000 ms slot.
        let admitted = admissions(host, &[None, Some(600), None]).await;
        let ms = |n: u64| Some(Duration::from_millis(n));
        assert_eq!(
            admitted,
            vec![ms(500), None, ms(1000)],
            "the slot it waited for goes to the next in line"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn timed_out_pacing_waits_hold_back_no_later_request() {
        let host = "timeout-reservation-test.example";
        set_test_rate_limit(host, 2.0, Duration::from_millis(1));
        assert_eq!(super::rate_limiter().acquire(host), None);

        let mut timeouts = vec![Some(50); 5];
        timeouts.push(None);
        let admitted = admissions(host, &timeouts).await;
        assert_eq!(&admitted[..5], &[None; 5]);
        assert_eq!(
            admitted[5],
            Some(Duration::from_millis(500)),
            "the next slot goes to the waiter left, not one per waiter that gave up"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_exhausted_server_quota_holds_requests_until_its_reset() {
        let host = "pacing-reset-test.example";
        set_test_rate_limit(host, 10.0, Duration::from_millis(1));
        super::rate_limiter().update_from_response(
            host,
            super::ServerRateLimitInfo {
                remaining: 0,
                reset_at: Instant::now() + Duration::from_secs(2),
            },
        );

        let admitted = admissions(host, &[None, None]).await;
        let ms = |n: u64| Some(Duration::from_millis(n));
        assert_eq!(
            admitted,
            vec![ms(2000), ms(2100)],
            "the reset, then one interval on"
        );
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
