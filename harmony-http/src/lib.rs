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

use harmony_core::{
    ChunkOrigin,
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;
use harmony_luau::{
    ClassDescriptor,
    FieldDescriptor,
    InterfaceDescriptor,
};
use harmony_luau::{
    DescribeInterface,
    DescribeTypeAlias,
    DescribeUserData,
    LuauType,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
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

struct DomainState {
    config: RateLimitConfig,
    next_allowed: Instant,
    server_remaining: Option<u32>,
    server_reset_at: Option<Instant>,
    /// Drives `has_rate_limit_for_plugin`.
    set_by: Option<Arc<str>>,
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

    fn set_config(&mut self, domain: String, config: RateLimitConfig, set_by: Option<Arc<str>>) {
        self.domains.insert(
            domain,
            DomainState {
                config,
                next_allowed: Instant::now(),
                server_remaining: None,
                server_reset_at: None,
                set_by,
            },
        );
    }

    fn has_entry_for_plugin(&self, plugin_id: &str) -> bool {
        self.domains
            .values()
            .any(|state| state.set_by.as_deref() == Some(plugin_id))
    }

    fn get_config(&self, domain: &str) -> Option<&RateLimitConfig> {
        self.domains.get(domain).map(|s| &s.config)
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
        if !state.config.requests_per_second.is_finite() || state.config.requests_per_second <= 0.0
        {
            return None;
        }

        let static_interval = Duration::from_secs_f64(1.0 / state.config.requests_per_second);
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
    semaphore: Arc<Semaphore>,
    max_in_flight: usize,
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

    /// Tighten-only: a registration is replaced only when the new cap is
    /// strictly tighter. Last-write-wins would let any caller silently widen
    /// another caller's cap for the same host; min-wins removes that footgun
    /// without requiring per-plugin identity at this surface.
    fn set_limit(&mut self, host: String, max_in_flight: usize) {
        if let Some(existing) = self.entries.get(&host)
            && max_in_flight >= existing.max_in_flight
        {
            tracing::trace!(
                host = %host,
                requested = max_in_flight,
                effective = existing.max_in_flight,
                "ignoring concurrency relaxation; tighter cap remains in force",
            );
            return;
        }
        self.entries.insert(
            host,
            ConcurrencyEntry {
                semaphore: Arc::new(Semaphore::new(max_in_flight)),
                max_in_flight,
            },
        );
    }

    fn get(&self, host: &str) -> Option<Arc<Semaphore>> {
        self.entries.get(host).map(|e| e.semaphore.clone())
    }

    #[cfg(test)]
    fn max_in_flight(&self, host: &str) -> Option<usize> {
        self.entries.get(host).map(|e| e.max_in_flight)
    }
}

fn parse_rate_limit_headers(headers: &HttpHeaderMap) -> Option<ServerRateLimitInfo> {
    let mut remaining: Option<u32> = None;
    let mut reset_ts: Option<u64> = None;

    for (key, value) in headers.iter() {
        match key.as_str() {
            "ratelimit-remaining" | "x-ratelimit-remaining" => {
                if remaining.is_none() {
                    remaining = value.parse().ok();
                }
            }
            "ratelimit-reset" | "x-ratelimit-reset" => {
                if reset_ts.is_none() {
                    reset_ts = value.parse().ok();
                }
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
    // The semaphore is never closed, so `acquire_owned` can only fail if it is.
    Some(
        semaphore
            .acquire_owned()
            .await
            .expect("concurrency semaphore is never closed"),
    )
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

        if let Some(ref d) = domain {
            if let Some(info) = parse_rate_limit_headers(&response.headers) {
                let mut limiter = RATE_LIMITER.write().await;
                limiter.update_from_response(d, info);
            }
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

struct HttpModule;

async fn configure_rate_limit(
    plugin_id: Option<Arc<str>>,
    options: HttpRateLimitOptions,
) -> anyhow::Result<()> {
    let requests_per_second = options.requests_per_second.unwrap_or(1.0);
    if !requests_per_second.is_finite() || requests_per_second <= 0.0 {
        anyhow::bail!("requests_per_second must be a positive number");
    }

    // Normalize via extract_domain so callers can pass either a bare host
    // or a full URL and match the request-time lookup key. `url::Url`
    // lowercases hosts on parse; lowercase the bare-host fallback too so
    // `set_rate_limit("EXAMPLE.com", ...)` and a request to
    // `https://example.com/...` hit the same key.
    let domain =
        extract_domain(&options.domain).unwrap_or_else(|| options.domain.to_ascii_lowercase());

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

async fn configure_max_in_flight(options: HttpConcurrencyOptions) -> anyhow::Result<()> {
    if options.max_in_flight == 0 {
        anyhow::bail!("max_in_flight must be at least 1");
    }

    // Mirror the set_rate_limit normalization (see above) so registrations
    // share the host key the request path looks up.
    let host = extract_domain(&options.host).unwrap_or_else(|| options.host.to_ascii_lowercase());

    let mut limiter = CONCURRENCY_LIMITER.write().await;
    limiter.set_limit(host, options.max_in_flight as usize);

    Ok(())
}

pub fn module_spec() -> ModuleSpec {
    let spec = ModuleSpec::new("harmony/http")
        .capability("harmony.http")
        .function(request_spec())
        .function(set_rate_limit_spec())
        .function(set_max_in_flight_spec())
        .function(encode_uri_component_spec())
        .install(|_| Ok(ModuleExport::new(HttpModule)));
    spec.luau_initializer(init_luau_http_module_callback)
}

fn request_spec() -> FunctionSpec {
    let spec = FunctionSpec::async_fn("request")
        .context::<ChunkOrigin>()
        .arg_name("options")
        .args::<HttpRequestOptions>()
        .returns::<HttpResponse>();
    spec.call_async_native(Arc::new(request_callback))
}

fn set_rate_limit_spec() -> FunctionSpec {
    let spec = FunctionSpec::async_fn("set_rate_limit")
        .context::<ChunkOrigin>()
        .arg_name("options")
        .args::<HttpRateLimitOptions>();
    spec.call_async_native(Arc::new(set_rate_limit_callback))
}

fn set_max_in_flight_spec() -> FunctionSpec {
    let spec = FunctionSpec::async_fn("set_max_in_flight")
        .context::<ChunkOrigin>()
        .arg_name("options")
        .args::<HttpConcurrencyOptions>();
    spec.call_async_native(Arc::new(set_max_in_flight_callback))
}

fn encode_uri_component_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("encode_uri_component")
        .named_arg::<String>("input")
        .returns::<String>();
    spec.call(encode_uri_component_callback)
}

fn encode_uri_component_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let input: String = frame.args.read_named("input")?;
    frame
        .returns
        .write(percent_encoding::utf8_percent_encode(&input, URI_COMPONENT_SET).to_string())?;
    Ok(())
}

fn request_callback(
    mut frame: luau::CallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let table: luau::Table = frame.args.read_named("options")?;
    let options = request_options_from_luau(frame.vm, &table)?;
    let future: luau::ScheduledFuture = Box::pin(async move {
        let response = execute_request_with_rate_limit(options).await;
        Ok(vec![response.into_luau_value()])
    });
    Ok(future)
}

fn set_rate_limit_callback(
    mut frame: luau::CallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let table: luau::Table = frame.args.read_named("options")?;
    let options = rate_limit_options_from_luau(frame.vm, &table)?;
    let plugin_id = frame.context.origin.plugin.clone();
    let future: luau::ScheduledFuture = Box::pin(async move {
        configure_rate_limit(plugin_id, options)
            .await
            .map_err(|error| luau::Error::Runtime(error.to_string()))?;
        Ok(Vec::new())
    });
    Ok(future)
}

fn set_max_in_flight_callback(
    mut frame: luau::CallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let table: luau::Table = frame.args.read_named("options")?;
    let options = concurrency_options_from_luau(frame.vm, &table)?;
    let future: luau::ScheduledFuture = Box::pin(async move {
        configure_max_in_flight(options)
            .await
            .map_err(|error| luau::Error::Runtime(error.to_string()))?;
        Ok(Vec::new())
    });
    Ok(future)
}

fn init_luau_http_module_callback(
    vm: &luau::Vm,
    _origin: &ChunkOrigin,
    table: &luau::Table,
) -> luau::runtime::Result<()> {
    let methods = vm.create_table_with_capacity(0, 6)?;
    for (name, method) in [
        ("Get", "GET"),
        ("Post", "POST"),
        ("Put", "PUT"),
        ("Delete", "DELETE"),
        ("Patch", "PATCH"),
        ("Head", "HEAD"),
    ] {
        methods.set_raw(vm, name, luau::Value::String(method.as_bytes().to_vec()))?;
    }
    methods.set_readonly(vm, true)?;
    table.set_table_raw(vm, "HttpMethod", &methods)?;
    Ok(())
}

fn request_options_from_luau(
    vm: &luau::Vm,
    table: &luau::Table,
) -> luau::runtime::Result<HttpRequestOptions> {
    Ok(HttpRequestOptions {
        url: required_string_field(vm, table, "url")?,
        method: required_method_field(vm, table, "method")?,
        body: optional_binary_field(vm, table, "body")?,
        headers: optional_header_map_field(vm, table, "headers")?,
        cookies: optional_header_map_field(vm, table, "cookies")?,
    })
}

fn rate_limit_options_from_luau(
    vm: &luau::Vm,
    table: &luau::Table,
) -> luau::runtime::Result<HttpRateLimitOptions> {
    Ok(HttpRateLimitOptions {
        domain: required_string_field(vm, table, "domain")?,
        requests_per_second: optional_f64_field(vm, table, "requests_per_second")?,
        retry_on: optional_u16_array_field(vm, table, "retry_on")?,
        max_retries: optional_u32_field(vm, table, "max_retries")?,
        backoff_ms: optional_u64_field(vm, table, "backoff_ms")?,
    })
}

fn concurrency_options_from_luau(
    vm: &luau::Vm,
    table: &luau::Table,
) -> luau::runtime::Result<HttpConcurrencyOptions> {
    Ok(HttpConcurrencyOptions {
        host: required_string_field(vm, table, "host")?,
        max_in_flight: required_u32_field(vm, table, "max_in_flight")?,
    })
}

fn required_string_field(
    vm: &luau::Vm,
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<String> {
    match table.get_raw(vm, field)? {
        luau::Value::String(value) => String::from_utf8(value).map_err(|error| {
            luau::Error::Runtime(format!("'{field}' must be valid UTF-8: {error}"))
        }),
        luau::Value::Nil => Err(luau::Error::Runtime(format!("missing '{field}' field"))),
        other => Err(luau_field_type_error(field, "string", other.type_name())),
    }
}

fn optional_f64_field(
    vm: &luau::Vm,
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<Option<f64>> {
    match table.get_raw(vm, field)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Integer(value) => Ok(Some(value as f64)),
        luau::Value::Number(value) => Ok(Some(value)),
        other => Err(luau_field_type_error(field, "number", other.type_name())),
    }
}

fn optional_u32_field(
    vm: &luau::Vm,
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<Option<u32>> {
    match table.get_raw(vm, field)? {
        luau::Value::Nil => Ok(None),
        value => number_to_u32(field, value).map(Some),
    }
}

fn optional_u64_field(
    vm: &luau::Vm,
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<Option<u64>> {
    match table.get_raw(vm, field)? {
        luau::Value::Nil => Ok(None),
        value => number_to_u64(field, value).map(Some),
    }
}

fn required_u32_field(
    vm: &luau::Vm,
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<u32> {
    match table.get_raw(vm, field)? {
        luau::Value::Nil => Err(luau::Error::Runtime(format!("missing '{field}' field"))),
        value => number_to_u32(field, value),
    }
}

fn optional_binary_field(
    vm: &luau::Vm,
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<Option<LuaBinaryInput>> {
    match table.get_raw(vm, field)? {
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
    vm: &luau::Vm,
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<Option<HttpHeaderMap>> {
    match table.get_raw(vm, field)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Table(value) => header_map_from_luau(vm, &value, field).map(Some),
        other => Err(luau_field_type_error(field, "table", other.type_name())),
    }
}

fn optional_u16_array_field(
    vm: &luau::Vm,
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<Option<Vec<u16>>> {
    match table.get_raw(vm, field)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Table(value) => {
            let mut output = Vec::new();
            for (_key, value) in value.pairs_raw(vm)? {
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
    match table.get_raw(vm, field)? {
        luau::Value::String(value) => {
            let method = String::from_utf8(value).map_err(|error| {
                luau::Error::Runtime(format!("'{field}' must be valid UTF-8: {error}"))
            })?;
            parse_http_method(field, &method)
        }
        luau::Value::Nil => Err(luau::Error::Runtime(format!("missing '{field}' field"))),
        other => Err(luau_field_type_error(
            field,
            "HttpMethod",
            other.type_name(),
        )),
    }
}

fn parse_http_method(field: &'static str, method: &str) -> luau::runtime::Result<HttpMethod> {
    match method.to_ascii_uppercase().as_str() {
        "GET" => Ok(HttpMethod::Get),
        "POST" => Ok(HttpMethod::Post),
        "PUT" => Ok(HttpMethod::Put),
        "DELETE" => Ok(HttpMethod::Delete),
        "PATCH" => Ok(HttpMethod::Patch),
        "HEAD" => Ok(HttpMethod::Head),
        _ => Err(luau::Error::Runtime(format!(
            "'{field}' must be one of Get, Post, Put, Delete, Patch, or Head"
        ))),
    }
}

fn header_map_from_luau(
    vm: &luau::Vm,
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<HttpHeaderMap> {
    let mut map = BTreeMap::new();
    for (key, value) in table.pairs_raw(vm)? {
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
        table.set_field("success", luau::Value::Boolean(self.success));
        // Luau LUA_TINTEGER and LUA_TNUMBER are distinct types under raw
        // equality; `Integer(200) == 200` from a script would be false.
        table.set_field(
            "status_code",
            self.status_code
                .map(|value| luau::Value::Number(f64::from(value)))
                .unwrap_or(luau::Value::Nil),
        );
        table.set_field(
            "status_message",
            luau::Value::String(self.status_message.into_bytes()),
        );
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
        table.set_field("rate_limited", luau::Value::Boolean(self.rate_limited));
        luau::Value::TableData(table)
    }
}

impl HttpHeaderMap {
    fn into_luau_value(self) -> luau::Value {
        let mut table = luau::OwnedTable::with_capacity(0, self.0.len());
        for (key, value) in self.0 {
            table.set_field(key, luau::Value::String(value.into_bytes()));
        }
        luau::Value::TableData(table)
    }
}

/// True when at least one `set_rate_limit` entry was registered under
/// `plugin_id`. Plugin surfaces gate on this before registering.
pub async fn has_rate_limit_for_plugin(plugin_id: &str) -> bool {
    let limiter = RATE_LIMITER.read().await;
    limiter.has_entry_for_plugin(plugin_id)
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

#[doc(hidden)]
pub async fn test_clear_rate_limits_for_plugin(plugin_id: &str) {
    let mut limiter = RATE_LIMITER.write().await;
    limiter
        .domains
        .retain(|_, state| state.set_by.as_deref() != Some(plugin_id));
}

fn http_module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "Http",
        local_name: "http",
        description: Some("Outbound HTTP requests, rate limits, and URI encoding."),
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["request"],
                description: Some("Makes an outbound HTTP request."),
                params: vec![ParameterDescriptor {
                    name: "options",
                    ty: HttpRequestOptions::luau_type(),
                    description: None,
                    variadic: false,
                }],
                returns: vec![HttpResponse::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["set_rate_limit"],
                description: Some("Registers retry and pacing policy for a host or URL."),
                params: vec![ParameterDescriptor {
                    name: "options",
                    ty: HttpRateLimitOptions::luau_type(),
                    description: None,
                    variadic: false,
                }],
                returns: Vec::new(),
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["set_max_in_flight"],
                description: Some(
                    "Registers a maximum concurrent request count for a host or URL.",
                ),
                params: vec![ParameterDescriptor {
                    name: "options",
                    ty: HttpConcurrencyOptions::luau_type(),
                    description: None,
                    variadic: false,
                }],
                returns: Vec::new(),
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["encode_uri_component"],
                description: Some("Percent-encodes a string for use in a URI component."),
                params: vec![ParameterDescriptor {
                    name: "input",
                    ty: String::luau_type(),
                    description: None,
                    variadic: false,
                }],
                returns: vec![String::luau_type()],
                yields: false,
            },
        ],
    }
}

impl LuauTypeInfo for HttpMethod {
    fn luau_type() -> LuauType {
        LuauType::literal("HttpMethod")
    }
}

impl DescribeUserData for HttpMethod {
    fn class_descriptor() -> ClassDescriptor {
        let mut descriptor = ClassDescriptor::new("HttpMethod", None);
        descriptor.fields.extend(
            ["Get", "Post", "Put", "Delete", "Patch", "Head"]
                .into_iter()
                .map(|name| FieldDescriptor {
                    name,
                    ty: HttpMethod::luau_type(),
                    description: None,
                }),
        );
        descriptor
    }
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
        &http_module_descriptor(),
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
    use super::{
        extract_domain,
        module_spec,
        render_luau_definition,
    };

    #[test]
    fn extract_domain_strips_scheme_and_port() {
        assert_eq!(extract_domain("http://host:8080").as_deref(), Some("host"),);
        assert_eq!(
            extract_domain("https://example.com/path?q=1").as_deref(),
            Some("example.com"),
        );
    }

    #[test]
    fn exposes_handwritten_module_spec() {
        let spec = module_spec();

        assert_eq!(spec.id.0.as_ref(), "harmony/http");
        assert_eq!(spec.capability.as_ref().unwrap().0.as_ref(), "harmony.http");
        assert_eq!(spec.functions.len(), 4);
        assert_eq!(spec.functions[0].name.as_ref(), "request");
        assert!(spec.functions[0].yields);
        assert!(
            spec.functions[0]
                .context_type
                .is_some_and(|name| name.contains("ChunkOrigin"))
        );
        assert_eq!(spec.functions[3].name.as_ref(), "encode_uri_component");
        assert!(!spec.functions[3].yields);
    }

    #[test]
    fn luau_module_registers_encode_uri_component() -> harmony_luau::runtime::Result<()> {
        let vm = harmony_luau::Vm::new()?;
        let spec = module_spec();
        let table =
            harmony_core::install_luau_module(&vm, &harmony_core::ChunkOrigin::default(), &spec)?;
        vm.set_global_table("http", &table)?;

        let values = vm.eval(
            std::sync::Arc::<[u8]>::from(&b"return http.encode_uri_component('a b/c?d=e&x=1')"[..]),
            harmony_luau::ChunkOrigin::default(),
        )?;

        assert_eq!(
            values,
            vec![harmony_luau::Value::String(
                b"a%20b%2Fc%3Fd%3De%26x%3D1".to_vec()
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
        let spec = module_spec();
        let table = harmony_core::install_luau_module(&vm, &origin, &spec)?;
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
        assert_eq!(scheduler.poll_ready(), 1);
        assert_eq!(scheduler.poll_ready(), 1);

        assert!(super::has_rate_limit_for_plugin("luau-http-plugin").await);
        {
            let limiter = super::CONCURRENCY_LIMITER.read().await;
            assert_eq!(limiter.max_in_flight("example.com"), Some(2));
        }
        assert_eq!(
            vm.eval(
                std::sync::Arc::<[u8]>::from(&b"return stored_method"[..]),
                harmony_luau::ChunkOrigin::default(),
            )?,
            vec![harmony_luau::Value::String(b"POST".to_vec())]
        );
        Ok(())
    }

    #[test]
    fn extract_domain_lowercases_authority() {
        assert_eq!(
            extract_domain("http://EXAMPLE.com").as_deref(),
            Some("example.com"),
        );
    }

    #[test]
    fn extract_domain_preserves_ipv6_brackets() {
        assert_eq!(
            extract_domain("http://[::1]:8080/").as_deref(),
            Some("[::1]"),
        );
    }

    #[test]
    fn extract_domain_returns_none_for_bare_host() {
        // Bare hosts aren't parseable as URLs; callers are expected to fall
        // back to the raw value when the parser returns None.
        assert!(extract_domain("musicbrainz.org").is_none());
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

    #[tokio::test]
    async fn concurrency_limiter_stores_host_key_for_full_url_input() {
        let mut limiter = super::ConcurrencyLimiter::new();
        let normalized =
            super::extract_domain("https://lrclib.net/api/get").unwrap_or("lrclib.net".to_string());
        limiter.set_limit(normalized, 1);
        assert!(limiter.get("lrclib.net").is_some());
        assert!(limiter.get("musicbrainz.org").is_none());
    }

    #[tokio::test]
    async fn concurrency_limiter_serializes_in_flight_requests() {
        let mut limiter = super::ConcurrencyLimiter::new();
        limiter.set_limit("example.com".to_string(), 1);

        let sem = limiter.get("example.com").expect("limit set above");

        let permit = sem
            .clone()
            .try_acquire_owned()
            .expect("first acquire succeeds while host is uncontended");

        assert!(
            sem.clone().try_acquire_owned().is_err(),
            "second permit must fail while first is held",
        );

        drop(permit);

        let _ = sem
            .clone()
            .try_acquire_owned()
            .expect("second acquire succeeds after release");
    }

    #[tokio::test]
    async fn has_rate_limit_for_plugin_walks_set_by() {
        use std::sync::Arc;
        let plugin_id: Arc<str> = Arc::from("plugin-rl-walker");
        let domain = "rate-limit-walker.example".to_string();
        {
            let mut limiter = super::RATE_LIMITER.write().await;
            limiter.set_config(
                domain.clone(),
                super::RateLimitConfig {
                    requests_per_second: 1.0,
                    retry_status_codes: vec![429],
                    max_retries: 0,
                    initial_backoff: std::time::Duration::from_millis(10),
                },
                Some(plugin_id.clone()),
            );
        }
        assert!(super::has_rate_limit_for_plugin("plugin-rl-walker").await);
        assert!(!super::has_rate_limit_for_plugin("plugin-rl-other").await);

        // Overwrite without a plugin id; the predicate must report false.
        {
            let mut limiter = super::RATE_LIMITER.write().await;
            limiter.set_config(
                domain,
                super::RateLimitConfig {
                    requests_per_second: 1.0,
                    retry_status_codes: vec![429],
                    max_retries: 0,
                    initial_backoff: std::time::Duration::from_millis(10),
                },
                None,
            );
        }
        assert!(!super::has_rate_limit_for_plugin("plugin-rl-walker").await);
    }

    #[tokio::test]
    async fn concurrency_limiter_tighten_only_rejects_relaxation() {
        let mut limiter = super::ConcurrencyLimiter::new();
        limiter.set_limit("example.com".to_string(), 5);
        assert_eq!(limiter.max_in_flight("example.com"), Some(5));

        limiter.set_limit("example.com".to_string(), 10);
        assert_eq!(limiter.max_in_flight("example.com"), Some(5));

        limiter.set_limit("example.com".to_string(), 2);
        assert_eq!(limiter.max_in_flight("example.com"), Some(2));

        limiter.set_limit("example.com".to_string(), 2);
        assert_eq!(limiter.max_in_flight("example.com"), Some(2));
    }

    #[tokio::test]
    async fn concurrency_permit_shared_across_callers() {
        // Unique host to avoid bleed with siblings sharing the global limiter.
        let host = "permit-shared-test.example".to_string();
        {
            let mut limiter = super::CONCURRENCY_LIMITER.write().await;
            limiter.set_limit(host.clone(), 1);
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
    fn bare_host_setter_fallback_matches_url_request_extraction() {
        // url::Url lowercases hosts on parse; the bare-host fallback must
        // lowercase too or registrations under uppercase keys silently miss.
        let bare_uppercase = "EXAMPLE.com";
        let stored_key = super::extract_domain(bare_uppercase)
            .unwrap_or_else(|| bare_uppercase.to_ascii_lowercase());
        let url_extracted =
            super::extract_domain("https://example.com/foo").expect("URL parses to host");

        assert_eq!(stored_key, "example.com");
        assert_eq!(stored_key, url_extracted);
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
            limiter.set_limit(host.clone(), 1);
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
    fn renders_http_module_definition() {
        let rendered = render_luau_definition().expect("render harmony/http docs");

        assert!(rendered.contains("@class Http"));
        assert!(rendered.contains("http.HttpMethod = nil :: HttpMethod"));
        assert!(
            rendered.contains("function http.request(options: HttpRequestOptions): HttpResponse")
        );
        assert!(rendered.contains("function http.set_rate_limit(options: HttpRateLimitOptions)"));
        assert!(
            !rendered.contains("function http.set_rate_limit(options: HttpRateLimitOptions): ()")
        );
        assert!(
            rendered.contains("function http.set_max_in_flight(options: HttpConcurrencyOptions)")
        );
        assert!(rendered.contains("string | buffer"));
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
