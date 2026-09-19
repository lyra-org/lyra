// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashSet;
use std::path::{
    Path,
    PathBuf,
};
use std::sync::atomic::Ordering;
use std::time::Instant;

use serde::{
    Deserialize,
    Serialize,
};

use crate::cached_http::AccessedKeys;
use crate::cached_http::CachedHttpState;
use crate::expect::Expectations;
use crate::fixture::{
    Fixture,
    RunMode,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResult {
    pub test_name: String,
    pub failures: Vec<String>,
    pub captured: Expectations,
    pub accessed_cache_keys: Vec<String>,
    pub live_requests: usize,
    pub scenario_id: String,
}

impl RunResult {
    pub fn passed(&self) -> bool {
        self.failures.is_empty()
    }
}

pub(crate) struct RunOptions<'a> {
    pub(crate) test_name: &'a str,
    pub(crate) fixture: &'a Fixture,
    pub(crate) plugin: &'a lyra_server::testing::PluginUnderTest,
    pub(crate) base_cache_dir: &'a Path,
    pub(crate) overlay_cache_dir: Option<&'a Path>,
    pub(crate) live_policy: crate::cached_http::LivePolicy,
    pub(crate) accessed_keys: &'a AccessedKeys,
    pub(crate) max_release_requests: Option<usize>,
}

pub async fn run(options: RunOptions<'_>) -> anyhow::Result<RunResult> {
    let RunOptions {
        test_name,
        fixture,
        plugin,
        base_cache_dir,
        overlay_cache_dir,
        live_policy,
        accessed_keys,
        max_release_requests,
    } = options;

    fixture
        .check_mapping_version()
        .map_err(|err| anyhow::anyhow!("{test_name}: {err}"))?;

    let debug_timing = std::env::var_os("LYRA_HARMONY_TEST_TIMINGS").is_some();
    let total_started = Instant::now();
    let request_count = crate::cached_http::new_request_count();
    let live_request_count = crate::cached_http::new_live_request_count();
    let cache_misses = crate::cached_http::new_cache_misses();
    let request_trace = crate::cached_http::new_request_trace();
    let library = lyra_server::testing::LibraryFixtureConfig {
        directory: PathBuf::from(&fixture.library.directory),
        language: fixture.library.language.clone(),
        country: fixture.library.country.clone(),
    };

    let started = Instant::now();
    lyra_server::testing::initialize_runtime(&library).await?;
    log_timing(debug_timing, test_name, "initialize_runtime", started);
    let started = Instant::now();
    let prepared =
        lyra_server::testing::prepare_fixture(&library, fixture.raw_tags.clone()).await?;
    log_timing(debug_timing, test_name, "prepare_fixture", started);

    let http_module = crate::cached_http::module_spec(CachedHttpState {
        base_cache_dir: base_cache_dir.to_path_buf(),
        overlay_cache_dir: overlay_cache_dir.map(Path::to_path_buf),
        accessed_keys: accessed_keys.clone(),
        request_count: request_count.clone(),
        live_request_count: live_request_count.clone(),
        cache_misses: cache_misses.clone(),
        request_trace: request_trace.clone(),
        live_policy,
        plugin_id: plugin.id().to_string(),
    });
    harmony_http::test_clear_rate_limits_for_plugin(plugin.id()).await;
    let started = Instant::now();
    let _plugin_sources = lyra_server::testing::exec_plugins(plugin, http_module).await?;
    fail_on_cache_misses(live_policy, &cache_misses).await?;
    log_timing(debug_timing, test_name, "exec_plugins", started);

    let mut total_requests = 0usize;
    request_count.store(0, Ordering::Relaxed);
    live_request_count.store(0, Ordering::Relaxed);
    let started = Instant::now();
    match fixture.run {
        RunMode::Refresh => {
            lyra_server::testing::refresh_release(prepared.release_id).await?;
        }
        RunMode::Sync => {
            lyra_server::testing::sync_provider(plugin.id()).await?;
        }
    }
    fail_on_cache_misses(live_policy, &cache_misses).await?;
    log_timing(debug_timing, test_name, "provider_run", started);
    total_requests += request_count.load(Ordering::Relaxed);
    let live_requests = live_request_count.load(Ordering::Relaxed);

    let started = Instant::now();
    let snapshot = lyra_server::testing::snapshot_fixture(&prepared).await?;
    log_timing(debug_timing, test_name, "snapshot_fixture", started);

    let mut failures = Vec::new();
    if let Some(max) = max_release_requests
        && total_requests > max
    {
        failures.push(format!(
            "  request limit exceeded: {total_requests} requests (max {max})"
        ));
    }

    fixture.expect.check(&snapshot, &mut failures);
    let captured = Expectations::capture(&snapshot);

    let mut accessed_cache_keys: Vec<String> = accessed_keys.read().await.iter().cloned().collect();
    accessed_cache_keys.sort();
    let scenario_id = {
        let trace = request_trace.read().await;
        crate::cached_http::scenario_id_for_trace(&trace)?
    };

    Ok(RunResult {
        test_name: test_name.to_string(),
        failures,
        captured,
        accessed_cache_keys,
        live_requests,
        scenario_id,
    })
    .inspect(|_| log_timing(debug_timing, test_name, "run_test_total", total_started))
}

async fn fail_on_cache_misses(
    live_policy: crate::cached_http::LivePolicy,
    cache_misses: &crate::cached_http::CacheMisses,
) -> anyhow::Result<()> {
    if live_policy != crate::cached_http::LivePolicy::CacheOnly {
        return Ok(());
    }

    let misses = crate::cached_http::take_cache_misses(cache_misses).await;
    if misses.is_empty() {
        return Ok(());
    }

    let mut unique = Vec::new();
    let mut seen = HashSet::new();
    for miss in misses {
        if seen.insert(miss.clone()) {
            unique.push(miss);
        }
    }

    let mut message = if unique.len() == 1 {
        String::from("cache miss under CacheOnly")
    } else {
        String::from("cache misses under CacheOnly:")
    };
    if unique.len() == 1 {
        message.push_str(": ");
        message.push_str(&unique[0]);
    } else {
        for miss in unique.iter().take(3) {
            message.push_str("\n  ");
            message.push_str(miss);
        }
        if unique.len() > 3 {
            message.push_str(&format!("\n  ... and {} more", unique.len() - 3));
        }
    }
    message.push_str("\nrerun with --discover to record missing responses");
    anyhow::bail!(message);
}

fn log_timing(enabled: bool, test_name: &str, stage: &str, started: Instant) {
    if enabled {
        eprintln!(
            "[lyra-harmony-test] {test_name} {stage}: {:.3}s",
            started.elapsed().as_secs_f64()
        );
    }
}
