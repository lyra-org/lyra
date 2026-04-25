// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    BTreeMap,
    HashSet,
};
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
use crate::test_case::{
    AcceptedValues,
    ExpectedEntity,
    ExpectedExpectations,
    RunMode,
    TestCase,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CapturedCredit {
    artist_id: String,
    credit_type: String,
    detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CapturedEntity {
    fields: BTreeMap<String, serde_json::Value>,
    external_ids: BTreeMap<String, String>,
    #[serde(default)]
    credits: Vec<CapturedCredit>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResult {
    pub test_name: String,
    pub failures: Vec<String>,
    pub captured: ExpectedExpectations,
    pub accessed_cache_keys: Vec<String>,
    pub live_requests: usize,
    pub scenario_id: String,
}

impl RunResult {
    pub fn passed(&self) -> bool {
        self.failures.is_empty()
    }
}

pub async fn run_test(
    test_name: &str,
    test_case: &TestCase,
    test_dir: &Path,
    base_cache_dir: &Path,
    overlay_cache_dir: Option<&Path>,
    live_policy: crate::cached_http::LivePolicy,
    accessed_keys: &AccessedKeys,
    max_release_requests: Option<usize>,
) -> anyhow::Result<RunResult> {
    test_case
        .check_mapping_version()
        .map_err(|err| anyhow::anyhow!("{test_name}: {err}"))?;

    let debug_timing = std::env::var_os("LYRA_HARMONY_TEST_TIMINGS").is_some();
    let total_started = Instant::now();
    let request_count = crate::cached_http::new_request_count();
    let live_request_count = crate::cached_http::new_live_request_count();
    let cache_misses = crate::cached_http::new_cache_misses();
    let request_trace = crate::cached_http::new_request_trace();
    let library = lyra_server::testing::LibraryFixtureConfig {
        directory: PathBuf::from(&test_case.library.directory),
        language: test_case.library.language.clone(),
        country: test_case.library.country.clone(),
    };

    let started = Instant::now();
    lyra_server::testing::initialize_runtime(&library).await?;
    log_timing(debug_timing, test_name, "initialize_runtime", started);
    let started = Instant::now();
    let prepared =
        lyra_server::testing::prepare_fixture(&library, test_case.raw_tags.clone()).await?;
    log_timing(debug_timing, test_name, "prepare_fixture", started);

    let plugins_dir = find_plugins_dir(test_dir)?;
    let parent = plugins_dir.parent().unwrap_or(Path::new("/")).to_path_buf();
    let http_module = crate::cached_http::get_module(
        base_cache_dir.to_path_buf(),
        overlay_cache_dir.map(Path::to_path_buf),
        accessed_keys.clone(),
        request_count.clone(),
        live_request_count.clone(),
        cache_misses.clone(),
        request_trace.clone(),
        live_policy,
    );
    let started = Instant::now();
    lyra_server::testing::exec_plugins(&parent, plugins_dir, http_module, &test_case.plugin)
        .await?;
    fail_on_cache_misses(live_policy, &cache_misses).await?;
    log_timing(debug_timing, test_name, "exec_plugins", started);

    let mut total_requests = 0usize;
    request_count.store(0, Ordering::Relaxed);
    live_request_count.store(0, Ordering::Relaxed);
    let started = Instant::now();
    match test_case.run {
        RunMode::Refresh => {
            lyra_server::testing::refresh_release(prepared.release_id).await?;
        }
        RunMode::Sync => {
            lyra_server::testing::sync_provider(&test_case.plugin).await?;
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

    let actual_release = snapshot.release.as_ref().map(snapshot_to_captured);
    let actual_artists: Vec<CapturedEntity> =
        snapshot.artists.iter().map(snapshot_to_captured).collect();
    let actual_tracks: Vec<CapturedEntity> =
        snapshot.tracks.iter().map(snapshot_to_captured).collect();

    if let Some(expected_release) = &test_case.expect.release {
        match actual_release.as_ref() {
            Some(actual) => {
                compare_expected_entity("release", expected_release, actual, &mut failures)
            }
            None => failures.push("  release: entity missing from final DB state".to_string()),
        }
    }

    compare_expected_entities(
        "artist",
        &test_case.expect.artists,
        &actual_artists,
        &mut failures,
    );
    compare_expected_entities(
        "track",
        &test_case.expect.tracks,
        &actual_tracks,
        &mut failures,
    );

    let mut captured = ExpectedExpectations::default();
    if let Some(release) = &actual_release {
        captured.release = Some(captured_entity_to_expected(release));
    }
    for artist in &actual_artists {
        if let Some((_, ext_id)) = artist.external_ids.first_key_value() {
            captured
                .artists
                .insert(ext_id.clone(), captured_entity_to_expected(artist));
        }
    }
    for track in &actual_tracks {
        if let Some((_, ext_id)) = track.external_ids.first_key_value() {
            captured
                .tracks
                .insert(ext_id.clone(), captured_entity_to_expected(track));
        }
    }

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

fn resolve_entity_by_id_values<'a>(
    entities: &'a [CapturedEntity],
    id_type: &str,
    accepted: &AcceptedValues,
) -> Option<&'a CapturedEntity> {
    entities.iter().find(|entity| {
        entity
            .external_ids
            .get(id_type)
            .is_some_and(|value| accepted.contains(value))
    })
}

fn compare_expected_entity(
    label: &str,
    expected: &ExpectedEntity,
    actual: &CapturedEntity,
    failures: &mut Vec<String>,
) {
    for (id_type, accepted) in &expected.ids {
        match actual.external_ids.get(id_type) {
            None => failures.push(format!(
                "  {label}: id '{id_type}' missing\n    expected: {}",
                accepted.display()
            )),
            Some(actual_value) if !accepted.contains(actual_value) => failures.push(format!(
                "  {label}: id '{id_type}' mismatch\n    expected: {}\n    actual:   {actual_value}",
                accepted.display()
            )),
            _ => {}
        }
    }

    for (field_name, expected_value) in &expected.fields {
        let expected_json = toml_to_json(expected_value);
        match actual.fields.get(field_name) {
            None => failures.push(format!(
                "  {label}: field '{field_name}' missing\n    expected: {expected_value}"
            )),
            Some(actual_value) if *actual_value != expected_json => {
                let actual_toml = json_to_toml(actual_value);
                failures.push(format!(
                    "  {label}: field '{field_name}' mismatch\n    expected: {expected_value}\n    actual:   {actual_toml}"
                ));
            }
            _ => {}
        }
    }

    for (artist_ext_id, expected_credit) in &expected.credits {
        let matching = actual.credits.iter().find(|c| {
            c.artist_id == *artist_ext_id
                && expected_credit
                    .credit_type
                    .as_ref()
                    .is_none_or(|ct| ct == &c.credit_type)
                && expected_credit.detail == c.detail
        });
        if matching.is_none() {
            let mut desc = format!("  {label}: credit for artist '{artist_ext_id}' missing");
            if let Some(ct) = &expected_credit.credit_type {
                desc.push_str(&format!("\n    expected credit_type: {ct}"));
            }
            if let Some(d) = &expected_credit.detail {
                desc.push_str(&format!("\n    expected detail: {d}"));
            }
            failures.push(desc);
        }
    }
}

fn compare_expected_entities(
    label_prefix: &str,
    expected_map: &BTreeMap<String, ExpectedEntity>,
    actual_list: &[CapturedEntity],
    failures: &mut Vec<String>,
) {
    for (ext_id_value, expected) in expected_map {
        let resolved = resolve_entity_by_any_id(actual_list, ext_id_value, expected);

        match resolved {
            Some(actual) => {
                compare_expected_entity(
                    &format!("{label_prefix} {ext_id_value}"),
                    expected,
                    actual,
                    failures,
                );
            }
            None => failures.push(format!(
                "  {label_prefix} {ext_id_value}: entity missing from final DB state"
            )),
        }
    }
}

fn resolve_entity_by_any_id<'a>(
    entities: &'a [CapturedEntity],
    ext_id_value: &str,
    expected: &ExpectedEntity,
) -> Option<&'a CapturedEntity> {
    // If the expected entity specifies ID types, match on those.
    for (id_type, accepted) in &expected.ids {
        if let Some(entity) = resolve_entity_by_id_values(entities, id_type, accepted) {
            return Some(entity);
        }
    }
    // Otherwise, match the key against any external ID value on any entity.
    entities
        .iter()
        .find(|entity| entity.external_ids.values().any(|v| v == ext_id_value))
}

fn snapshot_to_captured(snapshot: &lyra_server::testing::EntitySnapshot) -> CapturedEntity {
    CapturedEntity {
        fields: snapshot.fields.clone(),
        external_ids: snapshot.external_ids.clone(),
        credits: snapshot
            .credits
            .iter()
            .map(|c| CapturedCredit {
                artist_id: c.artist_id.clone(),
                credit_type: c.credit_type.clone(),
                detail: c.detail.clone(),
            })
            .collect(),
    }
}

fn captured_entity_to_expected(entity: &CapturedEntity) -> ExpectedEntity {
    let ids = entity
        .external_ids
        .iter()
        .map(|(key, value)| (key.clone(), AcceptedValues::Single(value.clone())))
        .collect();
    let fields = entity
        .fields
        .iter()
        .map(|(key, value)| (key.clone(), json_to_toml(value)))
        .collect();
    ExpectedEntity {
        ids,
        fields,
        credits: BTreeMap::new(),
    }
}

pub fn find_plugins_dir(test_dir: &Path) -> anyhow::Result<PathBuf> {
    let mut current = test_dir.canonicalize()?;
    loop {
        let candidate = current.join("plugins");
        if candidate.is_dir() {
            return Ok(candidate);
        }
        if !current.pop() {
            anyhow::bail!(
                "failed to locate plugins directory while searching upward from {}",
                test_dir.display()
            );
        }
    }
}

pub(crate) fn json_to_toml(value: &serde_json::Value) -> toml::Value {
    match value {
        serde_json::Value::Null => toml::Value::String("null".to_string()),
        serde_json::Value::Bool(value) => toml::Value::Boolean(*value),
        serde_json::Value::Number(value) => {
            if let Some(integer) = value.as_i64() {
                toml::Value::Integer(integer)
            } else if let Some(float) = value.as_f64() {
                toml::Value::Float(float)
            } else {
                toml::Value::String(value.to_string())
            }
        }
        serde_json::Value::String(value) => toml::Value::String(value.clone()),
        serde_json::Value::Array(values) => {
            toml::Value::Array(values.iter().map(json_to_toml).collect())
        }
        serde_json::Value::Object(values) => {
            let mut table = toml::map::Map::new();
            for (key, value) in values {
                table.insert(key.clone(), json_to_toml(value));
            }
            toml::Value::Table(table)
        }
    }
}

fn toml_to_json(value: &toml::Value) -> serde_json::Value {
    match value {
        toml::Value::String(value) if value == "null" => serde_json::Value::Null,
        toml::Value::String(value) => serde_json::Value::String(value.clone()),
        toml::Value::Integer(value) => serde_json::Value::Number((*value).into()),
        toml::Value::Float(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        toml::Value::Boolean(value) => serde_json::Value::Bool(*value),
        toml::Value::Datetime(value) => serde_json::Value::String(value.to_string()),
        toml::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(toml_to_json).collect())
        }
        toml::Value::Table(values) => {
            let mut object = serde_json::Map::new();
            for (key, value) in values {
                object.insert(key.clone(), toml_to_json(value));
            }
            serde_json::Value::Object(object)
        }
    }
}
