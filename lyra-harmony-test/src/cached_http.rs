// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};
use std::io::{
    Read,
    Write,
};
use std::path::{
    Path,
    PathBuf,
};
use std::sync::atomic::{
    AtomicUsize,
    Ordering,
};
use std::sync::{
    Arc,
    LazyLock,
    Mutex,
};
use std::time::{
    Duration,
    Instant,
};

use flate2::read::GzDecoder;
use flate2::{
    Compression,
    GzBuilder,
};
use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;
use percent_encoding::{
    AsciiSet,
    NON_ALPHANUMERIC,
};
use serde::{
    Deserialize,
    Serialize,
};
use tokio::sync::RwLock;

/// Matches JS `encodeURIComponent`
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
const DEFAULT_USER_AGENT: &str = "Lyra/0.0.1-dev ( blue@spook.rip )";
static RATE_LIMITER: LazyLock<Mutex<RateLimiter>> =
    LazyLock::new(|| Mutex::new(RateLimiter::new()));

#[derive(Serialize, Deserialize)]
struct CachedResponse {
    url: String,
    status_code: u16,
    body: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestTraceEntry {
    pub cache_key: String,
    pub response_hash: String,
    pub status_code: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ScenarioManifest {
    test_name: String,
    scenario_id: String,
    responses: Vec<ScenarioResponse>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ScenarioResponse {
    cache_key: String,
    url: String,
    status_code: u16,
    body_hash: String,
}

#[derive(Clone, Debug)]
pub struct StoredScenario {
    pub scenario_id: String,
    pub cache_dir: PathBuf,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LivePolicy {
    AllowLive,
    CacheOnly,
}

#[derive(Clone, Debug)]
struct RateLimitConfig {
    requests_per_second: f64,
    retry_status_codes: Vec<u16>,
    max_retries: u32,
    initial_backoff: Duration,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            requests_per_second: 1.0,
            retry_status_codes: vec![429, 503],
            max_retries: 3,
            initial_backoff: Duration::from_millis(1000),
        }
    }
}

struct DomainState {
    config: RateLimitConfig,
    next_allowed: Instant,
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
        let next_allowed = self
            .domains
            .get(&domain)
            .map(|state| state.next_allowed)
            .unwrap_or_else(Instant::now);
        self.domains.insert(
            domain,
            DomainState {
                config,
                next_allowed,
            },
        );
    }

    fn get_config(&self, domain: &str) -> Option<RateLimitConfig> {
        self.domains.get(domain).map(|state| state.config.clone())
    }

    fn acquire(&mut self, domain: &str) -> Option<Duration> {
        let state = self.domains.get_mut(domain)?;
        if !state.config.requests_per_second.is_finite() || state.config.requests_per_second <= 0.0
        {
            return None;
        }

        let interval = Duration::from_secs_f64(1.0 / state.config.requests_per_second);
        let now = Instant::now();
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

/// Tracks which cache keys were accessed during the run (for pruning).
pub type AccessedKeys = Arc<RwLock<HashSet<String>>>;
/// Tracks total http.request calls during one test run.
pub type RequestCount = Arc<AtomicUsize>;
/// Tracks live HTTP requests during one test run.
pub type LiveRequestCount = Arc<AtomicUsize>;
/// Tracks cache misses encountered during one test run.
pub type CacheMisses = Arc<RwLock<Vec<String>>>;
/// Ordered request trace for one test run.
pub type RequestTrace = Arc<RwLock<Vec<RequestTraceEntry>>>;

pub fn new_accessed_keys() -> AccessedKeys {
    Arc::new(RwLock::new(HashSet::new()))
}

pub fn new_request_count() -> RequestCount {
    Arc::new(AtomicUsize::new(0))
}

pub fn new_live_request_count() -> LiveRequestCount {
    Arc::new(AtomicUsize::new(0))
}

pub fn new_cache_misses() -> CacheMisses {
    Arc::new(RwLock::new(Vec::new()))
}

pub fn new_request_trace() -> RequestTrace {
    Arc::new(RwLock::new(Vec::new()))
}

pub async fn take_cache_misses(cache_misses: &CacheMisses) -> Vec<String> {
    std::mem::take(&mut *cache_misses.write().await)
}

#[derive(Clone)]
struct CachedHttpState {
    base_cache_dir: PathBuf,
    overlay_cache_dir: Option<PathBuf>,
    accessed_keys: AccessedKeys,
    request_count: RequestCount,
    live_request_count: LiveRequestCount,
    cache_misses: CacheMisses,
    request_trace: RequestTrace,
    live_policy: LivePolicy,
    plugin_id: String,
}

pub fn module_spec(
    base_cache_dir: PathBuf,
    overlay_cache_dir: Option<PathBuf>,
    accessed_keys: AccessedKeys,
    request_count: RequestCount,
    live_request_count: LiveRequestCount,
    cache_misses: CacheMisses,
    request_trace: RequestTrace,
    live_policy: LivePolicy,
    plugin_id: String,
) -> ModuleSpec {
    let state = CachedHttpState {
        base_cache_dir,
        overlay_cache_dir,
        accessed_keys,
        request_count,
        live_request_count,
        cache_misses,
        request_trace,
        live_policy,
        plugin_id,
    };
    let request_state = state.clone();
    let rate_limit_plugin_id = state.plugin_id.clone();

    ModuleSpec::new("harmony/http")
        .capability("harmony.http")
        .function(
            FunctionSpec::async_fn("request")
                .arg_name("options")
                .call_async(Arc::new(move |frame| {
                    request_callback(frame, request_state.clone())
                })),
        )
        .function(
            FunctionSpec::async_fn("set_rate_limit")
                .arg_name("options")
                .call_async(Arc::new(move |frame| {
                    set_rate_limit_callback(frame, rate_limit_plugin_id.clone())
                })),
        )
        .function(
            FunctionSpec::async_fn("set_max_in_flight")
                .arg_name("options")
                .call_async(Arc::new(set_max_in_flight_callback)),
        )
        .function(
            FunctionSpec::sync_fn("encode_uri_component")
                .named_arg::<String>("input")
                .returns::<String>()
                .call(encode_uri_component_callback),
        )
        .install(|_| Ok(ModuleExport::new(CachedHttpModule)))
        .initializer(init_luau_http_module)
}

struct CachedHttpModule;

fn init_luau_http_module(
    vm: &luau::Vm,
    _origin: &harmony_core::ChunkOrigin,
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
    table.set_table_raw(vm, "HttpMethod", &methods)
}

fn xxh3_hex(input: &str) -> String {
    format!("{:016x}", xxh3::hash64_with_seed(input.as_bytes(), 0))
}

fn xxh3_hex_bytes(input: &[u8]) -> String {
    format!("{:016x}", xxh3::hash64_with_seed(input, 0))
}

fn extract_domain(url_str: &str) -> Option<String> {
    url::Url::parse(url_str)
        .ok()
        .and_then(|url| url.host_str().map(|host| host.to_string()))
}

fn read_cache(
    scenario_cache_dir: &Path,
    cache_key: &str,
    url: &str,
) -> std::io::Result<Option<CachedResponse>> {
    let Some(entry) = read_response_entry(scenario_cache_dir, cache_key)? else {
        return Ok(None);
    };
    if entry.url != url || !(200..400).contains(&entry.status_code) {
        return Ok(None);
    }
    let body = read_response_body(scenario_cache_dir, &entry.body_hash)?;
    Ok(Some(CachedResponse {
        url: entry.url,
        status_code: entry.status_code,
        body,
    }))
}

fn write_cache(
    scenario_cache_dir: &Path,
    cache_key: &str,
    response: &CachedResponse,
) -> std::io::Result<()> {
    if !(200..400).contains(&response.status_code) {
        return Ok(());
    }
    let body_hash = response_body_hash(&response.body);
    write_response_body(scenario_cache_dir, &body_hash, &response.body)?;
    let mut manifest = match read_scenario_manifest(scenario_cache_dir) {
        Ok(manifest) => manifest,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            default_scenario_manifest(scenario_cache_dir)
        }
        Err(e) => return Err(e),
    };
    let entry = ScenarioResponse {
        cache_key: cache_key.to_string(),
        url: response.url.clone(),
        status_code: response.status_code,
        body_hash,
    };
    if let Some(existing) = manifest
        .responses
        .iter_mut()
        .find(|existing| existing.cache_key == entry.cache_key)
    {
        *existing = entry;
    } else {
        manifest.responses.push(entry);
    }
    manifest
        .responses
        .sort_by(|a, b| a.cache_key.cmp(&b.cache_key));
    write_scenario_manifest(scenario_cache_dir, &manifest)
}

async fn record_trace_entry(
    request_trace: &RequestTrace,
    cache_key: &str,
    status_code: u16,
    body: &str,
) {
    request_trace.write().await.push(RequestTraceEntry {
        cache_key: cache_key.to_string(),
        response_hash: xxh3_hex_bytes(body.as_bytes()),
        status_code,
    });
}

struct LiveResponse {
    status_code: u16,
    body: String,
}

static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    let _ = rustls::crypto::ring::default_provider().install_default();
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("failed to build HTTP client")
});

#[derive(Clone, Copy)]
enum CachedHttpMethod {
    Get,
    Post,
    Put,
    Delete,
    Patch,
    Head,
}

#[derive(Clone)]
struct RequestOptions {
    url: String,
    method: CachedHttpMethod,
    body: Option<Vec<u8>>,
    headers: HashMap<String, String>,
}

fn request_callback(
    mut frame: luau::AsyncCallFrame<'_>,
    state: CachedHttpState,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let options_table: luau::Table = frame.args.read_named("options")?;
    let options = request_options_from_luau(frame.vm, &options_table)?;
    let future = luau::ScheduledFuture::new(async move {
        state.request_count.fetch_add(1, Ordering::Relaxed);

        let cache_key = xxh3_hex(&options.url);
        {
            let mut keys = state.accessed_keys.write().await;
            keys.insert(cache_key.clone());
        }

        let cached = if let Some(overlay_cache_dir) = state.overlay_cache_dir.as_deref() {
            match read_cache(overlay_cache_dir, &cache_key, &options.url)
                .map_err(runtime_io_error)?
            {
                Some(cached) => Some(cached),
                None => read_cache(&state.base_cache_dir, &cache_key, &options.url)
                    .map_err(runtime_io_error)?,
            }
        } else {
            read_cache(&state.base_cache_dir, &cache_key, &options.url).map_err(runtime_io_error)?
        };

        if let Some(cached) = cached {
            record_trace_entry(
                &state.request_trace,
                &cache_key,
                cached.status_code,
                &cached.body,
            )
            .await;
            return Ok(response_value(cached.status_code, &cached.body, 0));
        }

        if state.live_policy == LivePolicy::CacheOnly {
            state.cache_misses.write().await.push(options.url.clone());
            return Err(luau::Error::Runtime(format!(
                "cache miss for {}",
                options.url
            )));
        }

        state.live_request_count.fetch_add(1, Ordering::Relaxed);

        let domain = extract_domain(&options.url);
        let config = domain.as_deref().and_then(|domain| {
            RATE_LIMITER
                .lock()
                .expect("rate limiter mutex poisoned")
                .get_config(domain)
        });

        if let Some(domain) = domain.as_deref()
            && config.is_some()
        {
            let wait_time = RATE_LIMITER
                .lock()
                .expect("rate limiter mutex poisoned")
                .acquire(domain);
            if let Some(wait_time) = wait_time {
                tokio::time::sleep(wait_time).await;
            }
        }

        let mut retries = 0u32;
        let mut backoff = config
            .as_ref()
            .map(|cfg| cfg.initial_backoff)
            .unwrap_or(Duration::from_secs(1));

        loop {
            let response = execute_single_request(&options)
                .await
                .map_err(runtime_anyhow_error)?;
            let write_cache_dir = state
                .overlay_cache_dir
                .as_ref()
                .unwrap_or(&state.base_cache_dir);
            write_cache(
                write_cache_dir,
                &cache_key,
                &CachedResponse {
                    url: options.url.clone(),
                    status_code: response.status_code,
                    body: response.body.clone(),
                },
            )
            .map_err(runtime_io_error)?;

            let should_retry = config.as_ref().is_some_and(|cfg| {
                retries < cfg.max_retries && cfg.retry_status_codes.contains(&response.status_code)
            });
            if !should_retry {
                record_trace_entry(
                    &state.request_trace,
                    &cache_key,
                    response.status_code,
                    &response.body,
                )
                .await;
                return Ok(response_value(
                    response.status_code,
                    &response.body,
                    retries,
                ));
            }

            tokio::time::sleep(backoff).await;
            retries += 1;
            backoff = backoff.saturating_mul(2);
        }
    });
    Ok(future)
}

fn set_rate_limit_callback(
    mut frame: luau::AsyncCallFrame<'_>,
    plugin_id: String,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let options_table: luau::Table = frame.args.read_named("options")?;
    let domain = required_string_field(frame.vm, &options_table, "domain")?;
    let requests_per_second =
        optional_f64_field(frame.vm, &options_table, "requests_per_second")?.unwrap_or(1.0);
    if !requests_per_second.is_finite() || requests_per_second <= 0.0 {
        return Err(luau::Error::Runtime(
            "requests_per_second must be a positive number".to_string(),
        ));
    }

    let config = RateLimitConfig {
        requests_per_second,
        retry_status_codes: optional_u16_array_field(frame.vm, &options_table, "retry_on")?
            .unwrap_or_else(|| vec![429, 503]),
        max_retries: optional_u32_field(frame.vm, &options_table, "max_retries")?.unwrap_or(3),
        initial_backoff: Duration::from_millis(
            optional_u64_field(frame.vm, &options_table, "backoff_ms")?.unwrap_or(1000),
        ),
    };

    let future = luau::ScheduledFuture::new(async move {
        RATE_LIMITER
            .lock()
            .expect("rate limiter mutex poisoned")
            .set_config(domain.clone(), config);
        harmony_http::test_seed_rate_limit(domain, plugin_id).await;
        Ok(())
    });
    Ok(future)
}

fn set_max_in_flight_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let _options: luau::Value = frame.args.read_named("options")?;
    Ok(luau::ScheduledFuture::new(async { Ok(()) }))
}

fn encode_uri_component_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let input: String = frame.args.read_named("input")?;
    frame
        .returns
        .write(percent_encoding::utf8_percent_encode(&input, URI_COMPONENT_SET).to_string())?;
    Ok(())
}

async fn execute_single_request(options: &RequestOptions) -> anyhow::Result<LiveResponse> {
    let client = &*HTTP_CLIENT;

    let mut req = match options.method {
        CachedHttpMethod::Get => client.get(&options.url),
        CachedHttpMethod::Post => client.post(&options.url),
        CachedHttpMethod::Put => client.put(&options.url),
        CachedHttpMethod::Delete => client.delete(&options.url),
        CachedHttpMethod::Patch => client.patch(&options.url),
        CachedHttpMethod::Head => client.head(&options.url),
    };

    let mut has_user_agent = false;
    for (key, value) in &options.headers {
        if key.eq_ignore_ascii_case("user-agent") {
            has_user_agent = true;
        }
        req = req.header(key, value);
    }
    if !has_user_agent {
        req = req.header(reqwest::header::USER_AGENT, DEFAULT_USER_AGENT);
    }

    if let Some(body) = &options.body {
        req = req.body(body.clone());
    }

    let resp = req.send().await?;
    Ok(LiveResponse {
        status_code: resp.status().as_u16(),
        body: resp.text().await?,
    })
}

fn request_options_from_luau(
    vm: &luau::Vm,
    table: &luau::Table,
) -> luau::runtime::Result<RequestOptions> {
    Ok(RequestOptions {
        url: required_string_field(vm, table, "url")?,
        method: optional_method_field(vm, table, "method")?.unwrap_or(CachedHttpMethod::Get),
        body: optional_binary_field(vm, table, "body")?,
        headers: optional_string_map_field(vm, table, "headers")?.unwrap_or_default(),
    })
}

fn response_value(status_code: u16, body: &str, retries: u32) -> luau::Value {
    let mut table = luau::OwnedTable::with_capacity(0, 8);
    table.set_field(
        "success",
        luau::Value::Boolean((200..400).contains(&status_code)),
    );
    table.set_field("status_code", luau::Value::Number(f64::from(status_code)));
    table.set_field(
        "status_message",
        luau::Value::String(status_message(status_code).as_bytes().to_vec()),
    );
    table.set_field("body", luau::Value::String(body.as_bytes().to_vec()));
    table.set_field(
        "headers",
        luau::Value::TableData(luau::OwnedTable::with_capacity(0, 0)),
    );
    table.set_field(
        "cookies",
        luau::Value::TableData(luau::OwnedTable::with_capacity(0, 0)),
    );
    table.set_field("retries", luau::Value::Number(f64::from(retries)));
    table.set_field("rate_limited", luau::Value::Boolean(retries > 0));
    luau::Value::TableData(table)
}

fn required_string_field(
    vm: &luau::Vm,
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<String> {
    match table.get_raw(vm, field)? {
        luau::Value::String(value) => String::from_utf8(value)
            .map_err(|error| luau::Error::Runtime(format!("'{field}' must be UTF-8: {error}"))),
        luau::Value::Nil => Err(luau::Error::Runtime(format!("missing '{field}' field"))),
        other => Err(field_type_error(field, "string", other.type_name())),
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
        other => Err(field_type_error(field, "number", other.type_name())),
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
        other => Err(field_type_error(field, "table", other.type_name())),
    }
}

fn optional_binary_field(
    vm: &luau::Vm,
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<Option<Vec<u8>>> {
    match table.get_raw(vm, field)? {
        luau::Value::Nil => Ok(None),
        luau::Value::String(value) | luau::Value::Buffer(value) => Ok(Some(value)),
        other => Err(field_type_error(
            field,
            "string or buffer",
            other.type_name(),
        )),
    }
}

fn optional_string_map_field(
    vm: &luau::Vm,
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<Option<HashMap<String, String>>> {
    match table.get_raw(vm, field)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Table(value) => {
            let mut output = HashMap::new();
            for (key, value) in value.pairs_raw(vm)? {
                let luau::Value::String(key) = key else {
                    return Err(field_type_error(
                        field,
                        "table<string, string>",
                        key.type_name(),
                    ));
                };
                let luau::Value::String(value) = value else {
                    return Err(field_type_error(
                        field,
                        "table<string, string>",
                        value.type_name(),
                    ));
                };
                output.insert(
                    String::from_utf8(key).map_err(|error| {
                        luau::Error::Runtime(format!("'{field}' key must be UTF-8: {error}"))
                    })?,
                    String::from_utf8(value).map_err(|error| {
                        luau::Error::Runtime(format!("'{field}' value must be UTF-8: {error}"))
                    })?,
                );
            }
            Ok(Some(output))
        }
        other => Err(field_type_error(field, "table", other.type_name())),
    }
}

fn optional_method_field(
    vm: &luau::Vm,
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<Option<CachedHttpMethod>> {
    match table.get_raw(vm, field)? {
        luau::Value::Nil => Ok(None),
        luau::Value::String(value) => {
            let method = String::from_utf8(value).map_err(|error| {
                luau::Error::Runtime(format!("'{field}' must be UTF-8: {error}"))
            })?;
            parse_method(field, &method).map(Some)
        }
        other => Err(field_type_error(field, "HttpMethod", other.type_name())),
    }
}

fn parse_method(field: &'static str, method: &str) -> luau::runtime::Result<CachedHttpMethod> {
    match method.to_ascii_uppercase().as_str() {
        "GET" => Ok(CachedHttpMethod::Get),
        "POST" => Ok(CachedHttpMethod::Post),
        "PUT" => Ok(CachedHttpMethod::Put),
        "DELETE" => Ok(CachedHttpMethod::Delete),
        "PATCH" => Ok(CachedHttpMethod::Patch),
        "HEAD" => Ok(CachedHttpMethod::Head),
        _ => Err(luau::Error::Runtime(format!(
            "'{field}' must be one of Get, Post, Put, Delete, Patch, or Head"
        ))),
    }
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
        other => Err(field_type_error(
            field,
            "non-negative integer",
            other.type_name(),
        )),
    }
}

fn field_type_error(field: &str, expected: &str, actual: &str) -> luau::Error {
    luau::Error::Runtime(format!(
        "invalid '{field}' field: expected {expected}, got {actual}"
    ))
}

fn runtime_io_error(error: std::io::Error) -> luau::Error {
    luau::Error::Runtime(error.to_string())
}

fn runtime_anyhow_error(error: anyhow::Error) -> luau::Error {
    luau::Error::Runtime(error.to_string())
}

fn status_message(code: u16) -> &'static str {
    match code {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        301 => "Moved Permanently",
        302 => "Found",
        304 => "Not Modified",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Unknown Status",
    }
}

pub fn scenario_id_for_trace(entries: &[RequestTraceEntry]) -> std::io::Result<String> {
    let encoded = serde_json::to_vec(entries).map_err(std::io::Error::other)?;
    Ok(xxh3_hex_bytes(&encoded))
}

pub fn scenarios_root(cache_dir: &Path) -> PathBuf {
    cache_dir.join("_scenarios")
}

pub fn fixture_scenarios_root(cache_dir: &Path, test_name: &str) -> PathBuf {
    scenarios_root(cache_dir).join(xxh3_hex(test_name))
}

pub fn scenario_cache_dir(cache_dir: &Path, test_name: &str, scenario_id: &str) -> PathBuf {
    fixture_scenarios_root(cache_dir, test_name).join(scenario_id)
}

fn scenario_manifest_path(scenario_cache_dir: &Path) -> PathBuf {
    scenario_cache_dir.join("scenario.json")
}

fn default_scenario_manifest(scenario_cache_dir: &Path) -> ScenarioManifest {
    ScenarioManifest {
        test_name: String::new(),
        scenario_id: scenario_cache_dir
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string(),
        responses: Vec::new(),
    }
}

fn read_scenario_manifest(scenario_cache_dir: &Path) -> std::io::Result<ScenarioManifest> {
    let data = std::fs::read(scenario_manifest_path(scenario_cache_dir))?;
    serde_json::from_slice(&data).map_err(std::io::Error::other)
}

fn write_scenario_manifest(
    scenario_cache_dir: &Path,
    manifest: &ScenarioManifest,
) -> std::io::Result<()> {
    std::fs::create_dir_all(scenario_cache_dir)?;
    let mut json = serde_json::to_vec_pretty(manifest).map_err(std::io::Error::other)?;
    json.push(b'\n');
    std::fs::write(scenario_manifest_path(scenario_cache_dir), json)
}

fn read_response_entry(
    scenario_cache_dir: &Path,
    cache_key: &str,
) -> std::io::Result<Option<ScenarioResponse>> {
    let manifest = match read_scenario_manifest(scenario_cache_dir) {
        Ok(manifest) => manifest,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    Ok(manifest
        .responses
        .into_iter()
        .find(|entry| entry.cache_key == cache_key))
}

fn cache_root_for_scenario_dir(scenario_cache_dir: &Path) -> std::io::Result<PathBuf> {
    let scenarios_dir = scenario_cache_dir
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "scenario cache path is not under a cache/_scenarios tree: {}",
                    scenario_cache_dir.display()
                ),
            )
        })?;
    if scenarios_dir.file_name().and_then(|name| name.to_str()) != Some("_scenarios") {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "scenario cache path is not under a cache/_scenarios tree: {}",
                scenario_cache_dir.display()
            ),
        ));
    }
    scenarios_dir
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "scenario cache path has no cache root: {}",
                    scenario_cache_dir.display()
                ),
            )
        })
}

fn response_body_hash(body: &str) -> String {
    xxh3_hex_bytes(body.as_bytes())
}

fn response_body_path(cache_root: &Path, body_hash: &str) -> PathBuf {
    let prefix = body_hash.get(..2).unwrap_or("00");
    cache_root
        .join("_responses")
        .join(prefix)
        .join(format!("{body_hash}.body.gz"))
}

fn read_response_body(scenario_cache_dir: &Path, body_hash: &str) -> std::io::Result<String> {
    let cache_root = cache_root_for_scenario_dir(scenario_cache_dir)?;
    read_response_body_file(&response_body_path(&cache_root, body_hash), body_hash)
}

fn read_response_body_file(path: &Path, body_hash: &str) -> std::io::Result<String> {
    let file = std::fs::File::open(path)?;
    let mut decoder = GzDecoder::new(file);
    let mut body = String::new();
    decoder.read_to_string(&mut body)?;
    validate_response_body_hash(path, body_hash, &body)?;
    Ok(body)
}

fn validate_response_body_hash(
    path: &Path,
    expected_hash: &str,
    body: &str,
) -> std::io::Result<()> {
    let actual_hash = response_body_hash(body);
    if actual_hash == expected_hash {
        return Ok(());
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "cached response body hash mismatch for {}: expected {expected_hash}, got {actual_hash}",
            path.display()
        ),
    ))
}

fn write_response_body(
    scenario_cache_dir: &Path,
    body_hash: &str,
    body: &str,
) -> std::io::Result<()> {
    let cache_root = cache_root_for_scenario_dir(scenario_cache_dir)?;
    let path = response_body_path(&cache_root, body_hash);
    if path.is_file() {
        let existing_body = read_response_body_file(&path, body_hash)?;
        if existing_body != body {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "cached response body hash collision for {}: existing body differs",
                    path.display()
                ),
            ));
        }
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(&path)?;
    let mut encoder = GzBuilder::new().mtime(0).write(file, Compression::best());
    encoder.write_all(body.as_bytes())?;
    encoder.finish()?;
    let written_body = read_response_body_file(&path, body_hash)?;
    if written_body != body {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "cached response body write verification failed for {}",
                path.display()
            ),
        ));
    }
    Ok(())
}

fn copy_response_body(
    source_scenario_cache_dir: &Path,
    dest_scenario_cache_dir: &Path,
    body_hash: &str,
) -> std::io::Result<()> {
    let source_root = cache_root_for_scenario_dir(source_scenario_cache_dir)?;
    let dest_root = cache_root_for_scenario_dir(dest_scenario_cache_dir)?;
    let source = response_body_path(&source_root, body_hash);
    read_response_body_file(&source, body_hash)?;
    if source_root == dest_root {
        return Ok(());
    }
    let dest = response_body_path(&dest_root, body_hash);
    if dest.is_file() {
        read_response_body_file(&dest, body_hash)?;
        return Ok(());
    }
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::copy(source, &dest)?;
    read_response_body_file(&dest, body_hash)?;
    Ok(())
}

pub fn persist_scenario(
    cache_dir: &Path,
    test_name: &str,
    scenario_id: &str,
    seed_cache_dir: &Path,
    overlay_cache_dir: &Path,
    cache_keys: &[String],
) -> std::io::Result<bool> {
    let scenario_dir = scenario_cache_dir(cache_dir, test_name, scenario_id);
    let is_new = !scenario_dir.exists();
    std::fs::create_dir_all(&scenario_dir)?;

    let mut responses = Vec::with_capacity(cache_keys.len());
    for cache_key in cache_keys {
        let (source_dir, entry) = match read_response_entry(overlay_cache_dir, cache_key)? {
            Some(entry) => (overlay_cache_dir, entry),
            None => match read_response_entry(seed_cache_dir, cache_key)? {
                Some(entry) => (seed_cache_dir, entry),
                None => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("missing cached response for key {cache_key}"),
                    ));
                }
            },
        };
        copy_response_body(source_dir, &scenario_dir, &entry.body_hash)?;
        responses.push(entry);
    }
    responses.sort_by(|a, b| a.cache_key.cmp(&b.cache_key));

    let manifest = ScenarioManifest {
        test_name: test_name.to_string(),
        scenario_id: scenario_id.to_string(),
        responses,
    };
    write_scenario_manifest(&scenario_dir, &manifest)?;
    Ok(is_new)
}

pub fn prune_fixture_scenarios(
    cache_dir: &Path,
    test_name: &str,
    keep_scenario_ids: &HashSet<String>,
) -> std::io::Result<usize> {
    let fixture_root = fixture_scenarios_root(cache_dir, test_name);
    let entries = match std::fs::read_dir(&fixture_root) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };

    let mut pruned = 0;
    for entry in entries.flatten() {
        let scenario_dir = entry.path();
        if !scenario_dir.is_dir() {
            continue;
        }

        let keep_dir = std::fs::read(scenario_manifest_path(&scenario_dir))
            .ok()
            .and_then(|content| serde_json::from_slice::<ScenarioManifest>(&content).ok())
            .is_some_and(|manifest| {
                manifest.test_name == test_name && keep_scenario_ids.contains(&manifest.scenario_id)
            });

        if keep_dir {
            continue;
        }

        std::fs::remove_dir_all(&scenario_dir)?;
        pruned += 1;
    }

    if pruned > 0 && fixture_root.is_dir() && std::fs::read_dir(&fixture_root)?.next().is_none() {
        std::fs::remove_dir(&fixture_root)?;
    }

    Ok(pruned)
}

pub fn load_fixture_scenarios(
    cache_dir: &Path,
    test_name: &str,
) -> std::io::Result<Vec<StoredScenario>> {
    let fixture_root = fixture_scenarios_root(cache_dir, test_name);
    let entries = match std::fs::read_dir(&fixture_root) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };

    let mut scenarios = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let manifest_path = scenario_manifest_path(&path);
        let content = match std::fs::read(&manifest_path) {
            Ok(content) => content,
            Err(_) => continue,
        };
        let manifest: ScenarioManifest = match serde_json::from_slice(&content) {
            Ok(manifest) => manifest,
            Err(_) => continue,
        };
        if manifest.test_name != test_name {
            continue;
        }
        scenarios.push(StoredScenario {
            scenario_id: manifest.scenario_id,
            cache_dir: path,
        });
    }
    scenarios.sort_by(|a, b| a.scenario_id.cmp(&b.scenario_id));
    Ok(scenarios)
}

pub fn prune_stale_scenarios(
    cache_dir: &Path,
    active_test_names: &HashSet<String>,
) -> std::io::Result<usize> {
    let entries = match std::fs::read_dir(scenarios_root(cache_dir)) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };

    let mut pruned = 0;
    for entry in entries.flatten() {
        let fixture_root = entry.path();
        if !fixture_root.is_dir() {
            continue;
        }
        let mut remove_fixture_root = true;
        if let Ok(scenario_entries) = std::fs::read_dir(&fixture_root) {
            for scenario_entry in scenario_entries.flatten() {
                let scenario_dir = scenario_entry.path();
                if !scenario_dir.is_dir() {
                    continue;
                }
                let manifest_path = scenario_manifest_path(&scenario_dir);
                let Ok(content) = std::fs::read(&manifest_path) else {
                    continue;
                };
                let Ok(manifest) = serde_json::from_slice::<ScenarioManifest>(&content) else {
                    continue;
                };
                if active_test_names.contains(&manifest.test_name) {
                    remove_fixture_root = false;
                    break;
                }
            }
        }
        if remove_fixture_root {
            std::fs::remove_dir_all(fixture_root)?;
            pruned += 1;
        }
    }
    Ok(pruned)
}

pub fn prune_unreferenced_responses(cache_dir: &Path) -> std::io::Result<usize> {
    let mut referenced = HashSet::new();
    let scenarios_dir = scenarios_root(cache_dir);
    if scenarios_dir.is_dir() {
        for fixture_entry in std::fs::read_dir(&scenarios_dir)?.flatten() {
            let fixture_root = fixture_entry.path();
            if !fixture_root.is_dir() {
                continue;
            }
            for scenario_entry in std::fs::read_dir(fixture_root)?.flatten() {
                let scenario_dir = scenario_entry.path();
                if !scenario_dir.is_dir() {
                    continue;
                }
                let manifest = match read_scenario_manifest(&scenario_dir) {
                    Ok(manifest) => manifest,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e),
                };
                referenced.extend(manifest.responses.into_iter().map(|entry| entry.body_hash));
            }
        }
    }

    let responses_root = cache_dir.join("_responses");
    let mut response_paths = Vec::new();
    collect_response_body_paths(&responses_root, &mut response_paths)?;

    let mut pruned = 0;
    for response_path in response_paths {
        let Some(file_name) = response_path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(body_hash) = file_name.strip_suffix(".body.gz") else {
            continue;
        };
        if referenced.contains(body_hash) {
            continue;
        }
        std::fs::remove_file(response_path)?;
        pruned += 1;
    }

    remove_empty_response_dirs(&responses_root)?;
    Ok(pruned)
}

fn collect_response_body_paths(root: &Path, paths: &mut Vec<PathBuf>) -> std::io::Result<()> {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_response_body_paths(&path, paths)?;
        } else {
            paths.push(path);
        }
    }
    Ok(())
}

fn remove_empty_response_dirs(root: &Path) -> std::io::Result<bool> {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e),
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() && remove_empty_response_dirs(&path)? {
            std::fs::remove_dir(&path)?;
        }
    }

    Ok(std::fs::read_dir(root)?.next().is_none())
}
