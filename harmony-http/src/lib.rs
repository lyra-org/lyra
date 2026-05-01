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
    LuaAsyncExt,
    Module,
};
use harmony_luau::{
    DescribeTypeAlias,
    LuauType,
    LuauTypeInfo,
};
use mlua::{
    FromLua,
    IntoLua,
    Lua,
    Table,
    Value,
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

impl FromLua for LuaBinaryInput {
    fn from_lua(value: Value, _lua: &Lua) -> mlua::Result<Self> {
        match value {
            Value::String(text) => Ok(Self(text.as_bytes().to_vec())),
            Value::Buffer(buffer) => Ok(Self(buffer.to_vec())),
            other => Err(mlua::Error::FromLuaConversionError {
                from: other.type_name(),
                to: "(string | buffer)".to_string(),
                message: Some("expected raw byte payload".to_string()),
            }),
        }
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

    fn set_config(&mut self, domain: String, config: RateLimitConfig) {
        self.domains.insert(
            domain,
            DomainState {
                config,
                next_allowed: Instant::now(),
                server_remaining: None,
                server_reset_at: None,
            },
        );
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

impl FromLua for HttpHeaderMap {
    fn from_lua(value: Value, lua: &Lua) -> mlua::Result<Self> {
        let table = Table::from_lua(value, lua)?;
        let mut map = BTreeMap::new();
        for pair in table.pairs::<String, String>() {
            let (key, value) = pair?;
            map.insert(key, value);
        }
        Ok(Self(map))
    }
}

impl IntoLua for HttpHeaderMap {
    fn into_lua(self, lua: &Lua) -> mlua::Result<Value> {
        let table = lua.create_table()?;
        for (key, value) in self.0 {
            table.set(key, value)?;
        }
        Ok(Value::Table(table))
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
#[harmony_macros::enumeration]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
    Patch,
    Head,
}

harmony_macros::compile!(type_path = HttpMethod, variants = true);

#[derive(Clone, Debug)]
#[harmony_macros::interface]
struct HttpRequestOptions {
    url: String,
    method: HttpMethod,
    body: Option<LuaBinaryInput>,
    headers: Option<HttpHeaderMap>,
    cookies: Option<HttpHeaderMap>,
}

impl FromLua for HttpRequestOptions {
    fn from_lua(value: Value, lua: &Lua) -> mlua::Result<Self> {
        let table = Table::from_lua(value, lua)
            .map_err(|_| mlua::Error::runtime("expected options to be a table"))?;

        Ok(Self {
            url: table.get("url").map_err(|e| {
                mlua::Error::runtime(format!("missing or invalid 'url' field: {e}"))
            })?,
            method: table.get("method").map_err(|e| {
                mlua::Error::runtime(format!("missing or invalid 'method' field: {e}"))
            })?,
            body: table
                .get("body")
                .map_err(|e| mlua::Error::runtime(format!("invalid 'body' field: {e}")))?,
            headers: table
                .get("headers")
                .map_err(|e| mlua::Error::runtime(format!("invalid 'headers' field: {e}")))?,
            cookies: table
                .get("cookies")
                .map_err(|e| mlua::Error::runtime(format!("invalid 'cookies' field: {e}")))?,
        })
    }
}

#[derive(Clone, Debug)]
#[harmony_macros::interface]
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
#[harmony_macros::interface]
struct HttpConcurrencyOptions {
    /// Host or full URL; only the host portion is keyed, matching the
    /// request-time lookup in [`extract_domain`].
    host: String,
    /// Maximum simultaneous in-flight requests to this host. Must be ≥ 1.
    max_in_flight: u32,
}

impl FromLua for HttpConcurrencyOptions {
    fn from_lua(value: Value, lua: &Lua) -> mlua::Result<Self> {
        let table = Table::from_lua(value, lua)
            .map_err(|_| mlua::Error::runtime("expected options table"))?;

        Ok(Self {
            host: table
                .get("host")
                .map_err(|_| mlua::Error::runtime("missing 'host' field"))?,
            max_in_flight: table
                .get("max_in_flight")
                .map_err(|e| mlua::Error::runtime(format!("invalid 'max_in_flight' field: {e}")))?,
        })
    }
}

impl FromLua for HttpRateLimitOptions {
    fn from_lua(value: Value, lua: &Lua) -> mlua::Result<Self> {
        let table = Table::from_lua(value, lua)
            .map_err(|_| mlua::Error::runtime("expected options table"))?;

        Ok(Self {
            domain: table
                .get("domain")
                .map_err(|_| mlua::Error::runtime("missing 'domain' field"))?,
            requests_per_second: table.get("requests_per_second").map_err(|e| {
                mlua::Error::runtime(format!("invalid 'requests_per_second' field: {e}"))
            })?,
            retry_on: table
                .get("retry_on")
                .map_err(|e| mlua::Error::runtime(format!("invalid 'retry_on' field: {e}")))?,
            max_retries: table
                .get("max_retries")
                .map_err(|e| mlua::Error::runtime(format!("invalid 'max_retries' field: {e}")))?,
            backoff_ms: table
                .get("backoff_ms")
                .map_err(|e| mlua::Error::runtime(format!("invalid 'backoff_ms' field: {e}")))?,
        })
    }
}

#[derive(Clone, Debug)]
#[harmony_macros::interface]
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

    fn into_lua_table(self, lua: &Lua) -> anyhow::Result<Table> {
        let table = lua.create_table()?;
        table.set("success", self.success)?;
        table.set("status_message", self.status_message)?;

        if let Some(code) = self.status_code {
            table.set("status_code", code)?;
        } else {
            table.set("status_code", Value::Nil)?;
        }

        if let Some(kind) = self.error_kind {
            table.set("error_kind", kind)?;
        }

        table.set("headers", self.headers.into_lua(lua)?)?;
        table.set("cookies", self.cookies.into_lua(lua)?)?;

        table.set("body", lua.create_string(&self.body.0)?)?;
        table.set("retries", self.retries)?;
        table.set("rate_limited", self.rate_limited)?;
        Ok(table)
    }
}

impl IntoLua for HttpResponse {
    fn into_lua(self, lua: &Lua) -> mlua::Result<Value> {
        Ok(Value::Table(
            self.into_lua_table(lua).map_err(mlua::Error::external)?,
        ))
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
            .and_then(|(_, v)| v.parse::<u64>().ok())
            .map(Duration::from_secs);

        let wait_time = retry_after.unwrap_or(backoff);
        tokio::time::sleep(wait_time).await;

        retries += 1;
        backoff = backoff.saturating_mul(2);
    }
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

#[harmony_macros::module(
    name = "Http",
    local = "http",
    path = "harmony/http",
    aliases(HttpHeaderMap),
    classes(HttpMethod),
    interfaces(
        HttpRequestOptions,
        HttpRateLimitOptions,
        HttpConcurrencyOptions,
        HttpResponse,
    )
)]
impl HttpModule {
    pub async fn request(options: HttpRequestOptions) -> mlua::Result<HttpResponse> {
        Ok(execute_request_with_rate_limit(options).await)
    }

    pub async fn set_rate_limit(_lua: Lua, options: HttpRateLimitOptions) -> mlua::Result<()> {
        let requests_per_second = options.requests_per_second.unwrap_or(1.0);
        if !requests_per_second.is_finite() || requests_per_second <= 0.0 {
            return Err(mlua::Error::runtime(
                "requests_per_second must be a positive number",
            ));
        }

        // Normalize via extract_domain so callers can pass either a bare host
        // or a full URL and match the request-time lookup key. `url::Url`
        // lowercases hosts on parse; lowercase the bare-host fallback too so
        // `set_rate_limit("EXAMPLE.com", ...)` and a request to
        // `https://example.com/...` hit the same key.
        let domain = extract_domain(&options.domain)
            .unwrap_or_else(|| options.domain.to_ascii_lowercase());

        let config = RateLimitConfig {
            requests_per_second,
            retry_status_codes: options.retry_on.unwrap_or_else(|| vec![429, 503]),
            max_retries: options.max_retries.unwrap_or(3),
            initial_backoff: Duration::from_millis(options.backoff_ms.unwrap_or(1000)),
        };

        let mut limiter = RATE_LIMITER.write().await;
        limiter.set_config(domain, config);

        Ok(())
    }

    /// Register an in-flight cap for `host`. Tighten-only — see
    /// [`ConcurrencyLimiter::set_limit`]. Reconfiguring mid-burst briefly
    /// allows `old_in_flight + new_in_flight` permits while the old
    /// semaphore drains; avoid live reconfiguration in hot windows.
    pub async fn set_max_in_flight(_lua: Lua, options: HttpConcurrencyOptions) -> mlua::Result<()> {
        if options.max_in_flight == 0 {
            return Err(mlua::Error::runtime("max_in_flight must be at least 1"));
        }

        // Mirror the set_rate_limit normalization (see above) so registrations
        // share the host key the request path looks up.
        let host = extract_domain(&options.host)
            .unwrap_or_else(|| options.host.to_ascii_lowercase());

        let mut limiter = CONCURRENCY_LIMITER.write().await;
        limiter.set_limit(host, options.max_in_flight as usize);

        Ok(())
    }

    pub fn encode_uri_component(_lua: &Lua, input: String) -> mlua::Result<String> {
        Ok(percent_encoding::utf8_percent_encode(&input, URI_COMPONENT_SET).to_string())
    }
}

pub fn get_module() -> Module {
    Module {
        path: "harmony/http".into(),
        setup: std::sync::Arc::new(|lua: &Lua| {
            let table = HttpModule::_harmony_module_table(lua)?;
            table.set("HttpMethod", lua.create_proxy::<HttpMethod>()?)?;
            Ok(table)
        }),
        scope: harmony_core::Scope {
            id: "harmony.http".into(),
            description: "Make outbound HTTP requests to any server on the internet.",
            danger: harmony_core::Danger::High,
        },
    }
}

pub fn render_luau_definition() -> Result<String, std::fmt::Error> {
    HttpModule::render_luau_definition()
}

#[cfg(test)]
mod tests {
    use super::{
        HttpMethod,
        HttpRequestOptions,
        LuaBinaryInput,
        extract_domain,
        get_module,
        render_luau_definition,
    };
    use mlua::{
        FromLua,
        Function,
        Lua,
        Value,
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
    fn generated_module_registers_http_exports() {
        let module = get_module();
        assert_eq!(&*module.path, "harmony/http");

        let lua = Lua::new();
        let table = (module.setup)(&lua).expect("create harmony/http module table");

        let _: Function = table.get("request").expect("register request");
        let _: Function = table
            .get("set_rate_limit")
            .expect("register set_rate_limit");
        let _: Function = table
            .get("set_max_in_flight")
            .expect("register set_max_in_flight");
        let _: Function = table
            .get("encode_uri_component")
            .expect("register encode_uri_component");
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
    fn request_options_reject_non_table_values() {
        let lua = Lua::new();
        let value = Value::String(lua.create_string("not a table").expect("create string"));

        let error = HttpRequestOptions::from_lua(value, &lua)
            .expect_err("reject non-table request options");

        assert!(error.to_string().contains("expected options to be a table"));
    }

    #[test]
    fn request_options_accept_string_and_buffer_bodies() {
        let lua = Lua::new();

        let string_table = lua.create_table().expect("create string request table");
        string_table
            .set("url", "https://example.com/upload")
            .expect("set string url");
        string_table
            .set("method", HttpMethod::Post)
            .expect("set string method");
        string_table
            .set(
                "body",
                lua.create_string(b"\0abc").expect("create string body"),
            )
            .expect("set string body");

        let string_options = HttpRequestOptions::from_lua(Value::Table(string_table), &lua)
            .expect("convert string request options");
        assert_eq!(string_options.body, Some(LuaBinaryInput(b"\0abc".to_vec())));

        let buffer_table = lua.create_table().expect("create buffer request table");
        buffer_table
            .set("url", "https://example.com/upload")
            .expect("set buffer url");
        buffer_table
            .set("method", HttpMethod::Post)
            .expect("set buffer method");
        buffer_table
            .set(
                "body",
                Value::Buffer(lua.create_buffer([0, 255, 65]).expect("create buffer body")),
            )
            .expect("set buffer body");

        let buffer_options = HttpRequestOptions::from_lua(Value::Table(buffer_table), &lua)
            .expect("convert buffer request options");
        assert_eq!(buffer_options.body, Some(LuaBinaryInput(vec![0, 255, 65])));
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
}
