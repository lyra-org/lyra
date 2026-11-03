// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Responses a plugin asked to keep with `cache_ttl_ms`, answered again for the same request
//! until they expire. Only plain GETs that carry no credentials are kept, and only final 2xx
//! responses the server lets a shared cache store.

use std::collections::HashMap;
use std::sync::{
    Arc,
    LazyLock,
    Mutex as StdMutex,
    MutexGuard,
    PoisonError,
    Weak,
};
use std::time::Duration;

use tokio::time::Instant;

use super::{
    HttpMethod,
    HttpRequestOptions,
    HttpResponse,
};

/// The longest a response is kept, whatever a request asks.
pub const MAX_CACHE_TTL_MS: u64 = 15 * 60 * 1000;
/// A larger response is answered but not kept.
const MAX_ENTRY_BYTES: usize = 1024 * 1024;
/// What every plugin's kept responses may take together.
const BUDGET_BYTES: usize = 32 * 1024 * 1024;
/// What one plugin's kept responses may take.
const PLUGIN_BUDGET_BYTES: usize = 8 * 1024 * 1024;

const CREDENTIAL_HEADERS: &[&str] = &["authorization", "proxy-authorization", "cookie"];

static RESPONSE_CACHE: LazyLock<StdMutex<ResponseCache>> =
    LazyLock::new(|| StdMutex::new(ResponseCache::new(Instant::now)));

fn response_cache() -> MutexGuard<'static, ResponseCache> {
    RESPONSE_CACHE
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// Requests whose responses are interchangeable: the same plugin's GET of the same URL, fragment
/// aside, with the same headers.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CacheKey {
    plugin: String,
    url: String,
    /// Lowercased names, in order.
    headers: Vec<(String, String)>,
}

impl CacheKey {
    /// The key of `options`, or none when the request is not one a cache may answer.
    fn of(plugin: Option<&str>, options: &HttpRequestOptions) -> Option<Self> {
        if options.method != HttpMethod::Get || options.body.is_some() {
            return None;
        }
        if options
            .cookies
            .as_ref()
            .is_some_and(|cookies| !cookies.0.is_empty())
        {
            return None;
        }
        let mut headers = Vec::new();
        for (name, value) in options.headers.iter().flat_map(|headers| headers.0.iter()) {
            let name = name.to_ascii_lowercase();
            if CREDENTIAL_HEADERS.contains(&name.as_str()) {
                return None;
            }
            headers.push((name, value.clone()));
        }
        headers.sort();
        let mut url = url::Url::parse(&options.url).ok()?;
        url.set_fragment(None);
        Some(Self {
            plugin: plugin.unwrap_or_default().to_string(),
            url: url.to_string(),
            headers,
        })
    }
}

struct Entry {
    response: HttpResponse,
    expires_at: Instant,
    bytes: usize,
    last_used: u64,
}

/// Whether response `headers` forbid a shared cache from keeping the response: any `Set-Cookie`,
/// or a `no-store` or `private` directive in any `Cache-Control` header, its value aside. Read
/// before the headers are collapsed to one value per name.
pub(crate) fn store_vetoed(headers: &reqwest::header::HeaderMap) -> bool {
    if headers.contains_key(reqwest::header::SET_COOKIE) {
        return true;
    }
    headers
        .get_all(reqwest::header::CACHE_CONTROL)
        .iter()
        .any(|value| {
            String::from_utf8_lossy(value.as_bytes())
                .split(',')
                .any(|directive| {
                    let name = directive.split('=').next().unwrap_or_default().trim();
                    name.eq_ignore_ascii_case("no-store") || name.eq_ignore_ascii_case("private")
                })
        })
}

/// Whether a shared cache may keep `response`.
fn storable(response: &HttpResponse) -> bool {
    response.success
        && !response.store_vetoed
        && response
            .status_code
            .is_some_and(|code| (200..300).contains(&code))
        && response.body.0.len() <= MAX_ENTRY_BYTES
        && response.cookies.0.is_empty()
}

fn entry_bytes(key: &CacheKey, response: &HttpResponse) -> usize {
    let pairs = |map: &super::HttpHeaderMap| {
        map.iter()
            .map(|(name, value)| name.len() + value.len())
            .sum::<usize>()
    };
    key.url.len()
        + key
            .headers
            .iter()
            .map(|(name, value)| name.len() + value.len())
            .sum::<usize>()
        + response.body.0.len()
        + response.status_message.len()
        + pairs(&response.headers)
}

pub(crate) struct ResponseCache {
    now: fn() -> Instant,
    entries: HashMap<CacheKey, Entry>,
    bytes: usize,
    plugin_bytes: HashMap<String, usize>,
    /// Numbers each fetch as it starts and each clear and bypass as it happens, so a fetch
    /// started before either cannot store.
    sequence: u64,
    /// When each plugin's responses were last cleared.
    cleared_at: HashMap<String, u64>,
    /// When each key with a fetch in flight was last bypassed.
    bypassed_at: HashMap<CacheKey, u64>,
    /// One fetch per key at a time; the others wait for it and read what it kept.
    in_flight: HashMap<CacheKey, Weak<tokio::sync::Mutex<()>>>,
    tick: u64,
}

impl ResponseCache {
    fn new(now: fn() -> Instant) -> Self {
        Self {
            now,
            entries: HashMap::new(),
            bytes: 0,
            plugin_bytes: HashMap::new(),
            sequence: 0,
            cleared_at: HashMap::new(),
            bypassed_at: HashMap::new(),
            in_flight: HashMap::new(),
            tick: 0,
        }
    }

    fn next_sequence(&mut self) -> u64 {
        self.sequence += 1;
        self.sequence
    }

    /// Whether a fetch of `key` started at `started` was overtaken by a clear or a bypass.
    fn overtaken(&self, key: &CacheKey, started: u64) -> bool {
        self.cleared_at
            .get(&key.plugin)
            .is_some_and(|&cleared| cleared > started)
            || self
                .bypassed_at
                .get(key)
                .is_some_and(|&bypassed| bypassed > started)
    }

    /// Drops what was kept for `key` and keeps the fetches of it already started from storing.
    fn bypass(&mut self, key: &CacheKey) {
        self.remove(key);
        let now = self.next_sequence();
        self.bypassed_at.insert(key.clone(), now);
    }

    fn remove(&mut self, key: &CacheKey) {
        if let Some(entry) = self.entries.remove(key) {
            self.bytes -= entry.bytes;
            if let Some(bytes) = self.plugin_bytes.get_mut(&key.plugin) {
                *bytes -= entry.bytes;
                if *bytes == 0 {
                    self.plugin_bytes.remove(&key.plugin);
                }
            }
        }
    }

    fn sweep_expired(&mut self) {
        let now = (self.now)();
        let expired = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.expires_at <= now)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in expired {
            self.remove(&key);
        }
        self.in_flight.retain(|_, lock| lock.strong_count() > 0);
        // A fetch starting after a bypass is never overtaken by it.
        let in_flight = &self.in_flight;
        self.bypassed_at
            .retain(|key, _| in_flight.contains_key(key));
    }

    /// The kept response for `key`, unless it is larger than `max_body_bytes`.
    fn get(&mut self, key: &CacheKey, max_body_bytes: Option<usize>) -> Option<HttpResponse> {
        self.sweep_expired();
        self.tick += 1;
        let tick = self.tick;
        let entry = self.entries.get_mut(key)?;
        if max_body_bytes.is_some_and(|limit| entry.response.body.0.len() > limit) {
            return None;
        }
        entry.last_used = tick;
        Some(entry.response.clone())
    }

    /// Keeps `response` for `ttl` when the server allows it and no clear or bypass overtook the
    /// fetch started at `started`, evicting the least recently used past the budgets.
    fn put(&mut self, key: CacheKey, started: u64, response: &HttpResponse, ttl: Duration) {
        if self.overtaken(&key, started) || !storable(response) {
            return;
        }
        self.sweep_expired();
        self.remove(&key);
        let bytes = entry_bytes(&key, response);
        self.tick += 1;
        let mut response = response.clone();
        response.retries = 0;
        response.rate_limited = false;
        self.bytes += bytes;
        *self.plugin_bytes.entry(key.plugin.clone()).or_default() += bytes;
        let plugin = key.plugin.clone();
        self.entries.insert(
            key,
            Entry {
                response,
                expires_at: (self.now)() + ttl,
                bytes,
                last_used: self.tick,
            },
        );
        while self.plugin_bytes.get(&plugin).copied().unwrap_or(0) > PLUGIN_BUDGET_BYTES {
            self.evict_least_recent(Some(&plugin));
        }
        while self.bytes > BUDGET_BYTES {
            self.evict_least_recent(None);
        }
    }

    fn evict_least_recent(&mut self, plugin: Option<&str>) {
        let oldest = self
            .entries
            .iter()
            .filter(|(key, _)| plugin.is_none_or(|plugin| key.plugin == plugin))
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(key, _)| key.clone());
        if let Some(key) = oldest {
            self.remove(&key);
        }
    }

    fn clear_plugin(&mut self, plugin: &str) {
        let keys = self
            .entries
            .keys()
            .filter(|key| key.plugin == plugin)
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            self.remove(&key);
        }
        let now = self.next_sequence();
        self.cleared_at.insert(plugin.to_string(), now);
    }

    fn fetch_lock(&mut self, key: &CacheKey) -> Arc<tokio::sync::Mutex<()>> {
        if let Some(lock) = self.in_flight.get(key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        self.in_flight.insert(key.clone(), Arc::downgrade(&lock));
        lock
    }
}

/// Drops every response `plugin` kept, and keeps a request it started before from storing one.
pub(crate) fn clear_plugin(plugin: &str) {
    response_cache().clear_plugin(plugin);
}

/// Runs `options` through `fetch`, answering from what an earlier request kept when it asked
/// for `cache_ttl_ms`. Its `timeout_ms` bounds its wait for another fetch of the same request as
/// well as its own.
pub(crate) async fn cached<F, Fut>(
    plugin: Option<&str>,
    options: HttpRequestOptions,
    fetch: F,
) -> HttpResponse
where
    F: FnOnce(HttpRequestOptions) -> Fut,
    Fut: std::future::Future<Output = HttpResponse>,
{
    cached_in(&RESPONSE_CACHE, plugin, options, fetch).await
}

async fn cached_in<F, Fut>(
    cache: &StdMutex<ResponseCache>,
    plugin: Option<&str>,
    options: HttpRequestOptions,
    fetch: F,
) -> HttpResponse
where
    F: FnOnce(HttpRequestOptions) -> Fut,
    Fut: std::future::Future<Output = HttpResponse>,
{
    let lock = || cache.lock().unwrap_or_else(PoisonError::into_inner);
    let Some(ttl_ms) = options.cache_ttl_ms else {
        return fetch(options).await;
    };
    let Some(key) = CacheKey::of(plugin, &options) else {
        return fetch(options).await;
    };
    if ttl_ms == 0 {
        lock().bypass(&key);
        return fetch(options).await;
    }
    let ttl = Duration::from_millis(ttl_ms.min(MAX_CACHE_TTL_MS));
    if let Some(hit) = lock().get(&key, options.max_body_bytes) {
        return hit;
    }

    let Some(timeout_ms) = options.timeout_ms else {
        return fetch_once(cache, key, ttl, options, fetch).await;
    };
    let mut options = options;
    options.timeout_ms = None;
    tokio::time::timeout(
        Duration::from_millis(timeout_ms),
        fetch_once(cache, key, ttl, options, fetch),
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

/// Fetches `key` unless another request fetching it meanwhile kept its response.
async fn fetch_once<F, Fut>(
    cache: &StdMutex<ResponseCache>,
    key: CacheKey,
    ttl: Duration,
    options: HttpRequestOptions,
    fetch: F,
) -> HttpResponse
where
    F: FnOnce(HttpRequestOptions) -> Fut,
    Fut: std::future::Future<Output = HttpResponse>,
{
    let lock = || cache.lock().unwrap_or_else(PoisonError::into_inner);
    // Numbered before the wait, so a clear or bypass while this request waits overtakes it too.
    let (fetch_lock, started) = {
        let mut cache = lock();
        (cache.fetch_lock(&key), cache.next_sequence())
    };
    let _fetching = fetch_lock.lock().await;
    if let Some(hit) = lock().get(&key, options.max_body_bytes) {
        return hit;
    }
    let response = fetch(options).await;
    lock().put(key, started, &response, ttl);
    response
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{
        AtomicUsize,
        Ordering,
    };

    use super::*;
    use crate::{
        BodyBytes,
        HttpHeaderMap,
    };

    fn options(url: &str, ttl_ms: Option<u64>) -> HttpRequestOptions {
        HttpRequestOptions {
            url: url.to_string(),
            method: HttpMethod::Get,
            body: None,
            headers: None,
            cookies: None,
            max_body_bytes: None,
            timeout_ms: None,
            cache_ttl_ms: ttl_ms,
        }
    }

    fn ok(body: &str) -> HttpResponse {
        HttpResponse::success(
            200,
            "OK".to_string(),
            HttpHeaderMap::default(),
            HttpHeaderMap::default(),
            body.as_bytes().to_vec(),
        )
    }

    fn raw_headers(headers: &[(&'static str, &'static str)]) -> reqwest::header::HeaderMap {
        let mut map = reqwest::header::HeaderMap::new();
        for (name, value) in headers {
            map.append(*name, reqwest::header::HeaderValue::from_static(value));
        }
        map
    }

    /// A 200 answered with `headers`, as `execute_single_request` reads them.
    fn ok_with_headers(body: &str, headers: &[(&'static str, &'static str)]) -> HttpResponse {
        let mut response = ok(body);
        response.store_vetoed = store_vetoed(&raw_headers(headers));
        response
    }

    fn new_cache() -> StdMutex<ResponseCache> {
        StdMutex::new(ResponseCache::new(Instant::now))
    }

    /// Answers each fetch with the next of `responses`, counting the fetches.
    struct Upstream {
        calls: AtomicUsize,
        responses: Vec<HttpResponse>,
    }

    impl Upstream {
        fn new(responses: Vec<HttpResponse>) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                responses,
            }
        }

        async fn fetch(&self, _options: HttpRequestOptions) -> HttpResponse {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            self.responses[call.min(self.responses.len() - 1)].clone()
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    async fn get(
        cache: &StdMutex<ResponseCache>,
        upstream: &Upstream,
        plugin: &str,
        options: HttpRequestOptions,
    ) -> HttpResponse {
        cached_in(cache, Some(plugin), options, |options| {
            upstream.fetch(options)
        })
        .await
    }

    fn body(response: &HttpResponse) -> &str {
        std::str::from_utf8(&response.body.0).expect("utf-8 body")
    }

    #[tokio::test(start_paused = true)]
    async fn a_kept_response_answers_until_it_expires() {
        let cache = new_cache();
        let upstream = Upstream::new(vec![ok("first"), ok("second")]);
        let url = "https://example.test/a";

        assert_eq!(
            body(&get(&cache, &upstream, "p", options(url, Some(60_000))).await),
            "first"
        );
        let hit = get(&cache, &upstream, "p", options(url, Some(60_000))).await;
        assert_eq!((body(&hit), upstream.calls()), ("first", 1));
        assert_eq!((hit.retries, hit.rate_limited), (0, false));

        tokio::time::advance(Duration::from_millis(60_001)).await;
        assert_eq!(
            body(&get(&cache, &upstream, "p", options(url, Some(60_000))).await),
            "second"
        );
        assert_eq!(upstream.calls(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_ttl_past_the_cap_keeps_the_response_only_as_long_as_the_cap() {
        let cache = new_cache();
        let upstream = Upstream::new(vec![ok("first"), ok("second")]);
        let url = "https://example.test/a";
        get(&cache, &upstream, "p", options(url, Some(u64::MAX))).await;

        tokio::time::advance(Duration::from_millis(MAX_CACHE_TTL_MS + 1)).await;
        get(&cache, &upstream, "p", options(url, Some(u64::MAX))).await;
        assert_eq!(upstream.calls(), 2);
    }

    #[tokio::test]
    async fn without_a_ttl_nothing_is_read_or_kept() {
        let cache = new_cache();
        let upstream = Upstream::new(vec![ok("first"), ok("second")]);
        let url = "https://example.test/a";
        get(&cache, &upstream, "p", options(url, Some(60_000))).await;

        assert_eq!(
            body(&get(&cache, &upstream, "p", options(url, None)).await),
            "second"
        );
        assert_eq!(
            body(&get(&cache, &upstream, "p", options(url, Some(60_000))).await),
            "first"
        );
    }

    #[tokio::test]
    async fn a_zero_ttl_fetches_fresh_and_evicts() {
        let cache = new_cache();
        let upstream = Upstream::new(vec![ok("first"), ok("second"), ok("third")]);
        let url = "https://example.test/a";
        get(&cache, &upstream, "p", options(url, Some(60_000))).await;

        assert_eq!(
            body(&get(&cache, &upstream, "p", options(url, Some(0))).await),
            "second"
        );
        assert_eq!(
            body(&get(&cache, &upstream, "p", options(url, Some(60_000))).await),
            "third",
            "the bypass evicted the kept response and kept nothing itself"
        );
    }

    #[tokio::test]
    async fn keys_separate_plugins_headers_and_urls_but_not_fragments() {
        let cache = new_cache();
        let upstream = Upstream::new(vec![ok("a"), ok("b"), ok("c"), ok("d"), ok("e")]);
        let ttl = Some(60_000);
        get(
            &cache,
            &upstream,
            "p",
            options("https://Example.test/a?x=1#top", ttl),
        )
        .await;

        get(
            &cache,
            &upstream,
            "p",
            options("https://example.test/a?x=1", ttl),
        )
        .await;
        assert_eq!(
            upstream.calls(),
            1,
            "host case and fragment are the same request"
        );

        get(
            &cache,
            &upstream,
            "q",
            options("https://example.test/a?x=1", ttl),
        )
        .await;
        assert_eq!(upstream.calls(), 2, "another plugin's request is its own");

        let mut accept = options("https://example.test/a?x=1", ttl);
        accept.headers = Some(HttpHeaderMap(
            [("Accept".to_string(), "application/json".to_string())].into(),
        ));
        get(&cache, &upstream, "p", accept.clone()).await;
        assert_eq!(upstream.calls(), 3, "other headers make another request");
        let mut lower = accept;
        lower.headers = Some(HttpHeaderMap(
            [("accept".to_string(), "application/json".to_string())].into(),
        ));
        get(&cache, &upstream, "p", lower).await;
        assert_eq!(upstream.calls(), 3, "header names compare without case");

        get(
            &cache,
            &upstream,
            "p",
            options("https://example.test/a?x=2", ttl),
        )
        .await;
        assert_eq!(upstream.calls(), 4);
    }

    #[tokio::test]
    async fn requests_with_credentials_or_a_body_or_another_method_are_not_kept() {
        let url = "https://example.test/a";
        let mut authorized = options(url, Some(60_000));
        authorized.headers = Some(HttpHeaderMap(
            [("Authorization".to_string(), "Bearer x".to_string())].into(),
        ));
        let mut proxied = options(url, Some(60_000));
        proxied.headers = Some(HttpHeaderMap(
            [("proxy-authorization".to_string(), "x".to_string())].into(),
        ));
        let mut with_cookies = options(url, Some(60_000));
        with_cookies.cookies = Some(HttpHeaderMap(
            [("session".to_string(), "x".to_string())].into(),
        ));
        let mut with_body = options(url, Some(60_000));
        with_body.body = Some(crate::LuaBinaryInput(b"x".to_vec()));
        let mut posted = options(url, Some(60_000));
        posted.method = HttpMethod::Post;
        for request in [authorized, proxied, with_cookies, with_body, posted] {
            let cache = new_cache();
            let upstream = Upstream::new(vec![ok("first"), ok("second")]);
            get(&cache, &upstream, "p", request.clone()).await;
            assert_eq!(body(&get(&cache, &upstream, "p", request).await), "second");
        }
    }

    #[tokio::test]
    async fn responses_the_server_withholds_or_that_failed_are_not_kept() {
        let mut not_found = ok("missing");
        not_found.status_code = Some(404);
        let mut redirect = ok("");
        redirect.status_code = Some(301);
        let mut cookie = ok("cookie");
        cookie.cookies = HttpHeaderMap([("session".to_string(), "x".to_string())].into());
        let failed = HttpResponse::error("connect", None, "refused".to_string());
        let large = ok(&"x".repeat(MAX_ENTRY_BYTES + 1));
        for response in [
            not_found,
            redirect,
            cookie,
            failed,
            large,
            ok_with_headers("no-store", &[("cache-control", "max-age=60, no-store")]),
            ok_with_headers(
                "split",
                &[
                    ("cache-control", "no-store"),
                    ("cache-control", "max-age=60"),
                ],
            ),
            ok_with_headers("private", &[("cache-control", "Private=\"set-cookie\"")]),
            ok_with_headers("set-cookie", &[("set-cookie", "a=b")]),
        ] {
            let cache = new_cache();
            let upstream = Upstream::new(vec![response, ok("fresh")]);
            let url = "https://example.test/a";
            get(&cache, &upstream, "p", options(url, Some(60_000))).await;
            assert_eq!(
                body(&get(&cache, &upstream, "p", options(url, Some(60_000))).await),
                "fresh"
            );
        }
    }

    #[test]
    fn every_cache_control_header_and_directive_name_counts() {
        assert!(store_vetoed(&raw_headers(&[
            ("cache-control", "max-age=60"),
            ("cache-control", "NO-STORE"),
        ])));
        assert!(store_vetoed(&raw_headers(&[(
            "cache-control",
            "private=\"x-user\", max-age=60"
        )])));
        assert!(store_vetoed(&raw_headers(&[("set-cookie", "a=b")])));
        assert!(!store_vetoed(&raw_headers(&[(
            "cache-control",
            "public, max-age=60, no-cache=\"x\""
        )])));
    }

    #[tokio::test]
    async fn a_bypass_keeps_an_older_fetch_from_storing() {
        let cache = new_cache();
        let gate = tokio::sync::Notify::new();
        let url = "https://example.test/a";
        let older = cached_in(&cache, Some("p"), options(url, Some(60_000)), |_| async {
            gate.notified().await;
            ok("older")
        });
        let bypass = async {
            tokio::task::yield_now().await;
            let fresh = cached_in(&cache, Some("p"), options(url, Some(0)), |_| async {
                ok("fresh")
            })
            .await;
            gate.notify_waiters();
            fresh
        };
        let (older, fresh) = tokio::join!(older, bypass);
        assert_eq!((body(&older), body(&fresh)), ("older", "fresh"));

        let upstream = Upstream::new(vec![ok("next")]);
        assert_eq!(
            body(&get(&cache, &upstream, "p", options(url, Some(60_000))).await),
            "next",
            "the older fetch kept nothing"
        );
    }

    #[tokio::test]
    async fn a_request_queued_before_a_clear_keeps_nothing() {
        let cache = new_cache();
        let gate = tokio::sync::Notify::new();
        let url = "https://example.test/a";
        let mut not_found = ok("missing");
        not_found.status_code = Some(404);
        let first = cached_in(&cache, Some("p"), options(url, Some(60_000)), |_| async {
            gate.notified().await;
            not_found.clone()
        });
        let queued = cached_in(&cache, Some("p"), options(url, Some(60_000)), |_| async {
            ok("queued")
        });
        let clear = async {
            tokio::task::yield_now().await;
            cache.lock().expect("cache").clear_plugin("p");
            gate.notify_waiters();
        };
        let (first, queued, ()) = tokio::join!(first, queued, clear);
        assert_eq!((body(&first), body(&queued)), ("missing", "queued"));

        let upstream = Upstream::new(vec![ok("after the clear")]);
        assert_eq!(
            body(&get(&cache, &upstream, "p", options(url, Some(60_000))).await),
            "after the clear",
            "the request queued before the clear kept nothing"
        );
    }

    #[tokio::test]
    async fn a_hit_larger_than_the_callers_limit_is_a_miss() {
        let cache = new_cache();
        let upstream = Upstream::new(vec![ok("0123456789"), ok("small")]);
        let url = "https://example.test/a";
        get(&cache, &upstream, "p", options(url, Some(60_000))).await;

        let mut limited = options(url, Some(60_000));
        limited.max_body_bytes = Some(5);
        assert_eq!(body(&get(&cache, &upstream, "p", limited).await), "small");
    }

    #[tokio::test]
    async fn the_least_recently_used_go_past_a_plugins_budget() {
        let cache = new_cache();
        let chunk = "x".repeat(MAX_ENTRY_BYTES - 1024);
        let upstream = Upstream::new(vec![ok(&chunk)]);
        let count = PLUGIN_BUDGET_BYTES / MAX_ENTRY_BYTES + 2;
        for index in 0..count {
            let url = format!("https://example.test/{index}");
            get(&cache, &upstream, "p", options(&url, Some(60_000))).await;
            if index == 0 {
                continue;
            }
            // Keeps the first one in use.
            get(
                &cache,
                &upstream,
                "p",
                options("https://example.test/0", Some(60_000)),
            )
            .await;
        }
        let state = cache.lock().expect("cache");
        assert!(state.plugin_bytes["p"] <= PLUGIN_BUDGET_BYTES);
        assert!(
            state.entries.keys().any(|key| key.url.ends_with("/0")),
            "the used one stays"
        );
        assert!(
            !state.entries.keys().any(|key| key.url.ends_with("/1")),
            "the oldest unused goes"
        );
        drop(state);
        assert_eq!(upstream.calls(), count);
    }

    #[tokio::test]
    async fn concurrent_callers_share_one_fetch() {
        let cache = new_cache();
        let calls = AtomicUsize::new(0);
        let gate = tokio::sync::Notify::new();
        let url = "https://example.test/a";
        let fetch = |_options: HttpRequestOptions| async {
            calls.fetch_add(1, Ordering::SeqCst);
            gate.notified().await;
            ok("shared")
        };
        let first = cached_in(&cache, Some("p"), options(url, Some(60_000)), fetch);
        let second = cached_in(&cache, Some("p"), options(url, Some(60_000)), fetch);
        let release = async {
            tokio::task::yield_now().await;
            gate.notify_waiters();
        };
        let (first, second, ()) = tokio::join!(first, second, release);

        assert_eq!((body(&first), body(&second)), ("shared", "shared"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_waiters_timeout_bounds_only_its_own_wait() {
        let cache = new_cache();
        let url = "https://example.test/a";
        let slow = |_options: HttpRequestOptions| async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            ok("slow")
        };
        let first = cached_in(&cache, Some("p"), options(url, Some(60_000)), slow);
        let mut impatient = options(url, Some(60_000));
        impatient.timeout_ms = Some(1_000);
        let second = cached_in(&cache, Some("p"), impatient, slow);
        let (first, second) = tokio::join!(first, second);

        assert_eq!(body(&first), "slow");
        assert_eq!(second.error_kind.as_deref(), Some("timeout"));
    }

    #[tokio::test]
    async fn clearing_a_plugin_drops_its_responses_and_what_is_in_flight() {
        let cache = new_cache();
        let upstream = Upstream::new(vec![ok("first"), ok("second"), ok("third")]);
        let url = "https://example.test/a";
        get(&cache, &upstream, "p", options(url, Some(60_000))).await;
        get(&cache, &upstream, "q", options(url, Some(60_000))).await;

        cache.lock().expect("cache").clear_plugin("p");
        assert_eq!(
            body(&get(&cache, &upstream, "q", options(url, Some(60_000))).await),
            "second"
        );
        assert_eq!(upstream.calls(), 2, "another plugin's responses stay");

        let in_flight = cached_in(&cache, Some("p"), options(url, Some(60_000)), |_| async {
            cache.lock().expect("cache").clear_plugin("p");
            ok("before the clear")
        });
        assert_eq!(body(&in_flight.await), "before the clear");
        assert_eq!(
            body(&get(&cache, &upstream, "p", options(url, Some(60_000))).await),
            "third",
            "a request started before the clear keeps nothing"
        );
    }

    #[test]
    fn credential_free_header_maps_build_sorted_keys() {
        let mut request = options("https://example.test/a", Some(1));
        request.headers = Some(HttpHeaderMap(
            [
                ("X-B".to_string(), "2".to_string()),
                ("x-a".to_string(), "1".to_string()),
            ]
            .into(),
        ));
        let key = CacheKey::of(Some("p"), &request).expect("cacheable");
        assert_eq!(
            key.headers,
            [
                ("x-a".to_string(), "1".to_string()),
                ("x-b".to_string(), "2".to_string())
            ]
        );
        let _ = BodyBytes::default();
    }
}
