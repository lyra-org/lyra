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
    LuaAsyncExt,
    Module,
};
use harmony_http::HttpMethod;
use mlua::{
    Lua,
    Table,
    Value,
};
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

pub fn get_module(
    base_cache_dir: PathBuf,
    overlay_cache_dir: Option<PathBuf>,
    accessed_keys: AccessedKeys,
    request_count: RequestCount,
    live_request_count: LiveRequestCount,
    cache_misses: CacheMisses,
    request_trace: RequestTrace,
    live_policy: LivePolicy,
    plugin_id: String,
) -> Module {
    Module {
        path: "harmony/http".into(),
        setup: Arc::new(move |lua: &Lua| -> anyhow::Result<Table> {
            let table = lua.create_table()?;
            table.set("HttpMethod", lua.create_proxy::<HttpMethod>()?)?;
            let base_cache_dir = base_cache_dir.clone();
            let overlay_cache_dir = overlay_cache_dir.clone();
            let accessed_keys = accessed_keys.clone();
            let request_count = request_count.clone();
            let live_request_count = live_request_count.clone();
            let cache_misses = cache_misses.clone();
            let request_trace = request_trace.clone();
            let live_policy = live_policy;
            let plugin_id = plugin_id.clone();

            let base_cache_dir_req = base_cache_dir.clone();
            let overlay_cache_dir_req = overlay_cache_dir.clone();
            let accessed_keys_req = accessed_keys.clone();
            let request_count_req = request_count.clone();
            let live_request_count_req = live_request_count.clone();
            let cache_misses_req = cache_misses.clone();
            let request_trace_req = request_trace.clone();
            table.set(
                "request",
                lua.create_async_function(move |lua, options: Table| {
                    let base_cache_dir = base_cache_dir_req.clone();
                    let overlay_cache_dir = overlay_cache_dir_req.clone();
                    let accessed_keys = accessed_keys_req.clone();
                    let request_count = request_count_req.clone();
                    let live_request_count = live_request_count_req.clone();
                    let cache_misses = cache_misses_req.clone();
                    let request_trace = request_trace_req.clone();
                    async move {
                        request_count.fetch_add(1, Ordering::Relaxed);

                        let url: String = options
                            .get("url")
                            .map_err(|_| mlua::Error::runtime("missing or invalid 'url' field"))?;

                        let cache_key = xxh3_hex(&url);
                        {
                            let mut keys = accessed_keys.write().await;
                            keys.insert(cache_key.clone());
                        }

                        let cached = if let Some(overlay_cache_dir) = overlay_cache_dir.as_deref() {
                            match read_cache(overlay_cache_dir, &cache_key, &url)
                                .map_err(mlua::Error::external)?
                            {
                                Some(cached) => Some(cached),
                                None => read_cache(&base_cache_dir, &cache_key, &url)
                                    .map_err(mlua::Error::external)?,
                            }
                        } else {
                            read_cache(&base_cache_dir, &cache_key, &url)
                                .map_err(mlua::Error::external)?
                        };

                        if let Some(cached) = cached {
                            record_trace_entry(
                                &request_trace,
                                &cache_key,
                                cached.status_code,
                                &cached.body,
                            )
                            .await;
                            return build_response_table(&lua, cached.status_code, &cached.body, 0);
                        }

                        if live_policy == LivePolicy::CacheOnly {
                            cache_misses.write().await.push(url.clone());
                            return Err(mlua::Error::runtime(format!("cache miss for {url}")));
                        }

                        live_request_count.fetch_add(1, Ordering::Relaxed);

                        let domain = extract_domain(&url);
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
                            let response = execute_single_request(&url, options.clone())
                                .await
                                .map_err(mlua::Error::external)?;
                            let write_cache_dir =
                                overlay_cache_dir.as_ref().unwrap_or(&base_cache_dir);
                            write_cache(
                                write_cache_dir,
                                &cache_key,
                                &CachedResponse {
                                    url: url.clone(),
                                    status_code: response.status_code,
                                    body: response.body.clone(),
                                },
                            )
                            .map_err(mlua::Error::external)?;

                            let should_retry = config.as_ref().is_some_and(|cfg| {
                                retries < cfg.max_retries
                                    && cfg.retry_status_codes.contains(&response.status_code)
                            });
                            if !should_retry {
                                record_trace_entry(
                                    &request_trace,
                                    &cache_key,
                                    response.status_code,
                                    &response.body,
                                )
                                .await;
                                return build_response_table(
                                    &lua,
                                    response.status_code,
                                    &response.body,
                                    retries,
                                );
                            }

                            tokio::time::sleep(backoff).await;
                            retries += 1;
                            backoff = backoff.saturating_mul(2);
                        }
                    }
                })?,
            )?;

            table.set(
                "set_rate_limit",
                lua.create_async_function(move |_lua, options: Value| {
                    let plugin_id = plugin_id.clone();
                    async move {
                        let table = match options {
                            Value::Table(table) => table,
                            _ => {
                                return Err(mlua::Error::runtime(
                                    "http.set_rate_limit expects a table",
                                ));
                            }
                        };

                        let domain: String = table.get("domain").map_err(|e| {
                            mlua::Error::runtime(format!("invalid 'domain' field: {e}"))
                        })?;
                        let requests_per_second = table
                            .get::<Option<f64>>("requests_per_second")
                            .map_err(|e| {
                                mlua::Error::runtime(format!(
                                    "invalid 'requests_per_second' field: {e}"
                                ))
                            })?
                            .unwrap_or(1.0);
                        if !requests_per_second.is_finite() || requests_per_second <= 0.0 {
                            return Err(mlua::Error::runtime(
                                "requests_per_second must be a positive number",
                            ));
                        }

                        let config = RateLimitConfig {
                            requests_per_second,
                            retry_status_codes: table
                                .get::<Option<Vec<u16>>>("retry_on")
                                .map_err(|e| {
                                    mlua::Error::runtime(format!("invalid 'retry_on' field: {e}"))
                                })?
                                .unwrap_or_else(|| vec![429, 503]),
                            max_retries: table
                                .get::<Option<u32>>("max_retries")
                                .map_err(|e| {
                                    mlua::Error::runtime(format!(
                                        "invalid 'max_retries' field: {e}"
                                    ))
                                })?
                                .unwrap_or(3),
                            initial_backoff: Duration::from_millis(
                                table
                                    .get::<Option<u64>>("backoff_ms")
                                    .map_err(|e| {
                                        mlua::Error::runtime(format!(
                                            "invalid 'backoff_ms' field: {e}"
                                        ))
                                    })?
                                    .unwrap_or(1000),
                            ),
                        };

                        RATE_LIMITER
                            .lock()
                            .expect("rate limiter mutex poisoned")
                            .set_config(domain.clone(), config);

                        harmony_http::test_seed_rate_limit(domain, plugin_id).await;

                        Ok(())
                    }
                })?,
            )?;

            table.set(
                "encode_uri_component",
                lua.create_function(|_, input: String| {
                    Ok(
                        percent_encoding::utf8_percent_encode(&input, URI_COMPONENT_SET)
                            .to_string(),
                    )
                })?,
            )?;

            Ok(table)
        }),
        scope: harmony_core::Scope {
            id: "harmony.http".into(),
            description: "Make outbound HTTP requests to any server on the internet.",
            danger: harmony_core::Danger::High,
        },
    }
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

async fn execute_single_request(url: &str, options: Table) -> anyhow::Result<LiveResponse> {
    let method: HttpMethod = options.get("method").unwrap_or_else(|_| HttpMethod::Get);

    let client = &*HTTP_CLIENT;

    let mut req = match method {
        HttpMethod::Get => client.get(url),
        HttpMethod::Post => client.post(url),
        HttpMethod::Put => client.put(url),
        HttpMethod::Delete => client.delete(url),
        HttpMethod::Patch => client.patch(url),
        HttpMethod::Head => client.head(url),
    };

    let mut has_user_agent = false;
    if let Ok(headers) = options.get::<Table>("headers") {
        for pair in headers.pairs::<String, String>().flatten() {
            if pair.0.eq_ignore_ascii_case("user-agent") {
                has_user_agent = true;
            }
            req = req.header(&pair.0, &pair.1);
        }
    }
    if !has_user_agent {
        req = req.header(reqwest::header::USER_AGENT, DEFAULT_USER_AGENT);
    }

    if let Ok(body) = options.get::<String>("body") {
        req = req.body(body);
    }

    let resp = req.send().await?;
    Ok(LiveResponse {
        status_code: resp.status().as_u16(),
        body: resp.text().await?,
    })
}

fn build_response_table(
    lua: &Lua,
    status_code: u16,
    body: &str,
    retries: u32,
) -> mlua::Result<Table> {
    let table = lua.create_table()?;
    let success = (200..400).contains(&status_code);
    table.set("success", success)?;
    table.set("status_code", status_code)?;
    table.set("status_message", status_message(status_code))?;
    table.set("body", body)?;
    table.set("headers", lua.create_table()?)?;
    table.set("cookies", lua.create_table()?)?;
    table.set("retries", retries)?;
    table.set("rate_limited", retries > 0)?;
    Ok(table)
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
