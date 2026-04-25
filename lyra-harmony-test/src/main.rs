// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod cached_http;
mod generate;
mod runner;
mod test_case;

use std::collections::HashSet;
use std::path::{
    Path,
    PathBuf,
};

use test_case::TestCase;

#[derive(Default)]
struct Args {
    test_dir: PathBuf,
    filter: Option<String>,
    prune: bool,
    record: bool,
    discover: bool,
    generate: Option<PathBuf>,
    output_dir: Option<PathBuf>,
    include_all: bool,
    max_release_requests: Option<usize>,
    max_scenarios: Option<usize>,
}

fn parse_args() -> anyhow::Result<Args> {
    let mut args = Args::default();
    let mut raw_args: Vec<String> = std::env::args().skip(1).collect();

    // Extract flags
    args.prune = extract_flag(&mut raw_args, "--prune");
    args.record = extract_flag(&mut raw_args, "--record");
    args.discover = extract_flag(&mut raw_args, "--discover");
    args.include_all = extract_flag(&mut raw_args, "--include-all");

    args.filter = extract_string_flag(&mut raw_args, "--filter")?;
    args.generate = extract_path_flag(&mut raw_args, "--generate")?;
    args.output_dir = extract_path_flag(&mut raw_args, "--output-dir")?;
    args.max_release_requests =
        extract_positive_usize_flag(&mut raw_args, "--max-release-requests")?;
    args.max_scenarios = extract_positive_usize_flag(&mut raw_args, "--max-scenarios")?;

    if args.generate.is_some() {
        if !raw_args.is_empty() {
            args.test_dir = PathBuf::from(raw_args.remove(0));
        }
        return Ok(args);
    }

    // Last positional arg is the test directory
    if raw_args.is_empty() {
        anyhow::bail!(
            "Usage: lyra-harmony-test [--filter PATTERN] [--prune] [--record] [--discover] [--max-release-requests N] [--max-scenarios N] <test-dir>\n       lyra-harmony-test --generate <capture.json> --output-dir <dir>"
        );
    }
    args.test_dir = PathBuf::from(raw_args.remove(0));

    Ok(args)
}

fn extract_flag(args: &mut Vec<String>, flag: &str) -> bool {
    if let Some(pos) = args.iter().position(|a| a == flag) {
        args.remove(pos);
        true
    } else {
        false
    }
}

fn extract_string_flag(args: &mut Vec<String>, flag: &str) -> anyhow::Result<Option<String>> {
    if let Some(pos) = args.iter().position(|a| a == flag) {
        args.remove(pos);
        if pos < args.len() {
            Ok(Some(args.remove(pos)))
        } else {
            anyhow::bail!("{flag} requires a value");
        }
    } else {
        Ok(None)
    }
}

fn extract_path_flag(args: &mut Vec<String>, flag: &str) -> anyhow::Result<Option<PathBuf>> {
    let Some(pos) = args.iter().position(|a| a == flag) else {
        return Ok(None);
    };
    args.remove(pos);
    if pos < args.len() {
        Ok(Some(PathBuf::from(args.remove(pos))))
    } else {
        anyhow::bail!("{flag} requires a value");
    }
}

fn extract_positive_usize_flag(
    args: &mut Vec<String>,
    flag: &str,
) -> anyhow::Result<Option<usize>> {
    let Some(pos) = args.iter().position(|a| a == flag) else {
        return Ok(None);
    };
    args.remove(pos);
    if pos >= args.len() {
        anyhow::bail!("{flag} requires a value");
    }
    let value = args.remove(pos);
    let parsed = value
        .parse::<usize>()
        .map_err(|_| anyhow::anyhow!("{flag} requires a positive integer, got '{value}'"))?;
    if parsed == 0 {
        anyhow::bail!("{flag} must be greater than 0");
    }
    Ok(Some(parsed))
}

fn discover_tests(dir: &Path, filter: Option<&str>) -> anyhow::Result<Vec<(String, PathBuf)>> {
    let mut tests = Vec::new();
    let entries = std::fs::read_dir(dir)?;

    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "toml") {
            let name = path
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            if let Some(filter) = filter {
                if !name.contains(filter) {
                    continue;
                }
            }
            tests.push((name, path));
        }
    }

    tests.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(tests)
}

struct LoadedTest {
    name: String,
    path: PathBuf,
    test_case: TestCase,
}

struct ScenarioExecution {
    scenario_id: String,
    outcome: anyhow::Result<runner::RunResult>,
}

struct FixtureExecution<'a> {
    test: &'a LoadedTest,
    scenario_runs: Vec<ScenarioExecution>,
    discovered_scenario_ids: Option<HashSet<String>>,
    discovery_complete: bool,
}

async fn replay_scenario(
    test: &LoadedTest,
    test_base_dir: &Path,
    scenario: &cached_http::StoredScenario,
    max_release_requests: Option<usize>,
) -> ScenarioExecution {
    let accessed_keys = cached_http::new_accessed_keys();
    let outcome = runner::run_test(
        &test.name,
        &test.test_case,
        test_base_dir,
        &scenario.cache_dir,
        None,
        cached_http::LivePolicy::CacheOnly,
        &accessed_keys,
        max_release_requests,
    )
    .await;

    ScenarioExecution {
        scenario_id: scenario.scenario_id.clone(),
        outcome,
    }
}

async fn discover_seeded_scenario(
    test: &LoadedTest,
    test_base_dir: &Path,
    cache_dir: &Path,
    scenario: &cached_http::StoredScenario,
    max_release_requests: Option<usize>,
) -> ScenarioExecution {
    let accessed_keys = cached_http::new_accessed_keys();
    let staging_scenario_id = format!("_discover-{}", scenario.scenario_id);
    let staging_cache_dir =
        cached_http::scenario_cache_dir(cache_dir, &test.name, &staging_scenario_id);
    let _ = std::fs::remove_dir_all(&staging_cache_dir);
    let outcome = runner::run_test(
        &test.name,
        &test.test_case,
        test_base_dir,
        &scenario.cache_dir,
        Some(&staging_cache_dir),
        cached_http::LivePolicy::AllowLive,
        &accessed_keys,
        max_release_requests,
    )
    .await
    .and_then(|result| {
        cached_http::persist_scenario(
            cache_dir,
            &test.name,
            &result.scenario_id,
            &scenario.cache_dir,
            &staging_cache_dir,
            &result.accessed_cache_keys,
        )?;
        Ok(result)
    });
    let _ = std::fs::remove_dir_all(&staging_cache_dir);

    ScenarioExecution {
        scenario_id: outcome
            .as_ref()
            .map(|result| result.scenario_id.clone())
            .unwrap_or_else(|_| scenario.scenario_id.clone()),
        outcome,
    }
}

async fn discover_scenario(
    test: &LoadedTest,
    test_base_dir: &Path,
    cache_dir: &Path,
    max_release_requests: Option<usize>,
) -> ScenarioExecution {
    let accessed_keys = cached_http::new_accessed_keys();
    let staging_scenario_id = "_discover";
    let staging_cache_dir =
        cached_http::scenario_cache_dir(cache_dir, &test.name, staging_scenario_id);
    let _ = std::fs::remove_dir_all(&staging_cache_dir);
    let outcome = runner::run_test(
        &test.name,
        &test.test_case,
        test_base_dir,
        &staging_cache_dir,
        None,
        cached_http::LivePolicy::AllowLive,
        &accessed_keys,
        max_release_requests,
    )
    .await
    .and_then(|result| {
        cached_http::persist_scenario(
            cache_dir,
            &test.name,
            &result.scenario_id,
            &staging_cache_dir,
            &staging_cache_dir,
            &result.accessed_cache_keys,
        )?;
        Ok(result)
    });
    let _ = std::fs::remove_dir_all(&staging_cache_dir);

    ScenarioExecution {
        scenario_id: outcome
            .as_ref()
            .map(|result| result.scenario_id.clone())
            .unwrap_or_else(|_| staging_scenario_id.to_string()),
        outcome,
    }
}

async fn run_fixture<'a>(
    test: &'a LoadedTest,
    test_base_dir: &Path,
    cache_dir: &Path,
    discover: bool,
    max_release_requests: Option<usize>,
    max_scenarios: Option<usize>,
) -> anyhow::Result<FixtureExecution<'a>> {
    let scenarios = cached_http::load_fixture_scenarios(cache_dir, &test.name)?;
    let (scenario_runs, discovered_scenario_ids, discovery_complete) = if scenarios.is_empty() {
        if discover {
            let run = discover_scenario(test, test_base_dir, cache_dir, max_release_requests).await;
            let mut discovered_ids = HashSet::new();
            let discovery_complete = if let Ok(result) = &run.outcome {
                discovered_ids.insert(result.scenario_id.clone());
                true
            } else {
                false
            };
            (vec![run], Some(discovered_ids), discovery_complete)
        } else {
            (
                vec![ScenarioExecution {
                    scenario_id: "missing".to_string(),
                    outcome: Err(anyhow::anyhow!("no stored scenarios for fixture")),
                }],
                None,
                false,
            )
        }
    } else {
        let mut runs = Vec::with_capacity(scenarios.len());
        let mut discovered_ids = HashSet::new();
        let mut discovery_complete = true;
        let mut scenarios_discovered = 0usize;
        for scenario in &scenarios {
            let run = if discover {
                let run = discover_seeded_scenario(
                    test,
                    test_base_dir,
                    cache_dir,
                    scenario,
                    max_release_requests,
                )
                .await;
                if run.outcome.is_ok() {
                    scenarios_discovered += 1;
                    if let Some(max) = max_scenarios {
                        if scenarios_discovered >= max {
                            if let Ok(result) = &run.outcome {
                                discovered_ids.insert(result.scenario_id.clone());
                            }
                            runs.push(run);
                            return Ok(FixtureExecution {
                                test,
                                scenario_runs: vec![ScenarioExecution {
                                    scenario_id: "max-scenarios".to_string(),
                                    outcome: Err(anyhow::anyhow!(
                                        "hit --max-scenarios limit ({max} scenarios discovered)"
                                    )),
                                }],
                                discovered_scenario_ids: Some(discovered_ids),
                                discovery_complete: false,
                            });
                        }
                    }
                }
                run
            } else {
                replay_scenario(test, test_base_dir, scenario, max_release_requests).await
            };
            if discover {
                if let Ok(result) = &run.outcome {
                    discovered_ids.insert(result.scenario_id.clone());
                } else {
                    discovery_complete = false;
                }
            }
            runs.push(run);
        }
        (
            runs,
            if discover { Some(discovered_ids) } else { None },
            if discover { discovery_complete } else { false },
        )
    };

    Ok(FixtureExecution {
        test,
        scenario_runs,
        discovered_scenario_ids,
        discovery_complete,
    })
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let args = parse_args()?;

    if let Some(ref capture_path) = args.generate {
        let output_dir = args
            .output_dir
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("--generate requires --output-dir"))?;
        return generate::run_generate(capture_path, output_dir, args.include_all);
    }

    let (test_base_dir, tests) = if args.test_dir.is_file() {
        let name = args
            .test_dir
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let dir = args
            .test_dir
            .parent()
            .unwrap_or(&args.test_dir)
            .to_path_buf();
        (dir, vec![(name, args.test_dir.clone())])
    } else {
        let dir = args.test_dir.clone();
        (dir.clone(), discover_tests(&dir, args.filter.as_deref())?)
    };
    if tests.is_empty() {
        eprintln!("No test cases found in {}", test_base_dir.display());
        std::process::exit(1);
    }

    let loaded_tests: Vec<LoadedTest> = tests
        .into_iter()
        .map(|(name, path)| {
            let content = std::fs::read_to_string(&path)?;
            let test_case: TestCase = toml::from_str(&content)
                .map_err(|e| anyhow::anyhow!("Failed to parse {}: {}", path.display(), e))?;
            Ok(LoadedTest {
                name,
                path,
                test_case,
            })
        })
        .collect::<anyhow::Result<_>>()?;

    let cache_dir = test_base_dir.join("cache");
    let mut passed = 0usize;
    let mut failed = 0usize;
    let mut pruned_scenarios = 0usize;
    let mut results = Vec::new();
    let mut executions = Vec::with_capacity(loaded_tests.len());
    for test in &loaded_tests {
        let execution = run_fixture(
            test,
            &test_base_dir,
            &cache_dir,
            args.discover,
            args.max_release_requests,
            args.max_scenarios,
        )
        .await?;
        if args.discover
            && args.prune
            && execution.discovery_complete
            && let Some(discovered_scenario_ids) = execution.discovered_scenario_ids.as_ref()
        {
            pruned_scenarios += cached_http::prune_fixture_scenarios(
                &cache_dir,
                &execution.test.name,
                discovered_scenario_ids,
            )?;
        }
        executions.push(execution);
    }

    if args.prune && args.test_dir.is_dir() && args.filter.is_none() {
        let active_test_names: HashSet<String> =
            loaded_tests.iter().map(|test| test.name.clone()).collect();
        pruned_scenarios += cached_http::prune_stale_scenarios(&cache_dir, &active_test_names)?;
    }

    for execution in executions {
        record_fixture_result(execution, &mut passed, &mut failed, &mut results);
    }

    if args.prune && pruned_scenarios > 0 {
        println!("Pruned {pruned_scenarios} stale scenario entries");
    }

    // --record: update [expect] sections
    // Only record for passing tests or tests with no existing expectations (seeded tests).
    // Never overwrite correct expectations with wrong plugin output.
    if args.record {
        for (path, test_case, result) in &results {
            if result.captured.is_empty() {
                continue;
            }
            if !result.passed() && !test_case.expect.is_empty() {
                println!(
                    "  skipped recording for {} (test failed, keeping existing expects)",
                    result.test_name
                );
                continue;
            }
            let merged = merge_recorded_ids(&test_case.expect, &result.captured);
            write_expect_section(path, &merged)?;
            println!("  recorded expectations for {}", result.test_name);
        }
    }

    let total = passed + failed;
    println!("{total} tests: {passed} passed, {failed} failed");

    if failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}

fn record_fixture_result<'a>(
    execution: FixtureExecution<'a>,
    passed: &mut usize,
    failed: &mut usize,
    results: &mut Vec<(&'a PathBuf, &'a TestCase, runner::RunResult)>,
) {
    let multiple_scenarios = execution.scenario_runs.len() > 1;
    let mut fixture_failed = false;
    let mut detail_lines = Vec::new();
    let mut recordable_result = None;

    for scenario_run in execution.scenario_runs {
        match scenario_run.outcome {
            Ok(result) => {
                if result.passed() {
                    if !multiple_scenarios && recordable_result.is_none() {
                        recordable_result = Some(result);
                    }
                    continue;
                }

                fixture_failed = true;
                if multiple_scenarios {
                    detail_lines.push(format!(
                        "  scenario {}",
                        short_scenario_id(&scenario_run.scenario_id)
                    ));
                }
                detail_lines.extend(result.failures);
            }
            Err(err) => {
                fixture_failed = true;
                if multiple_scenarios {
                    detail_lines.push(format!(
                        "  scenario {} error: {err}",
                        short_scenario_id(&scenario_run.scenario_id)
                    ));
                } else {
                    detail_lines.push(format!("  {err}"));
                }
            }
        }
    }

    if fixture_failed {
        println!("FAIL {}", execution.test.name);
        for line in detail_lines {
            println!("{line}");
        }
        *failed += 1;
    } else {
        println!("PASS {}", execution.test.name);
        *passed += 1;
        if let Some(result) = recordable_result {
            results.push((&execution.test.path, &execution.test.test_case, result));
        }
    }
}

fn short_scenario_id(scenario_id: &str) -> &str {
    scenario_id.get(..8).unwrap_or(scenario_id)
}

/// Merge recorded (single-value) IDs with existing multi-value IDs from the test file.
/// If the existing test accepts multiple values and the recorded value is one of them,
/// preserve the full list. Otherwise use the recorded value.
fn merge_recorded_ids(
    existing: &test_case::ExpectedExpectations,
    captured: &test_case::ExpectedExpectations,
) -> test_case::ExpectedExpectations {
    let mut merged = captured.clone();

    if let (Some(existing_release), Some(captured_release)) =
        (&existing.release, &mut merged.release)
    {
        merge_entity_ids(captured_release, existing_release);
    }

    for (artist_ext_id, captured_artist) in &mut merged.artists {
        if let Some(existing_artist) = existing.artists.get(artist_ext_id) {
            merge_entity_ids(captured_artist, existing_artist);
        }
    }

    for (recording_id, captured_track) in &mut merged.tracks {
        if let Some(existing_track) = existing.tracks.get(recording_id) {
            merge_entity_ids(captured_track, existing_track);
        }
    }

    merged
}

/// Rewrite the [expect] section of a TOML test file with captured results.
fn write_expect_section(
    path: &Path,
    expect: &test_case::ExpectedExpectations,
) -> anyhow::Result<()> {
    let content = std::fs::read_to_string(path)?;

    // Find where [expect] section starts (or append)
    let expect_start = content
        .find("\n[expect.")
        .or_else(|| content.find("\n[expect]"))
        .map(|pos| pos + 1); // +1 to skip the leading newline

    let prefix = match expect_start {
        Some(pos) => &content[..pos],
        None => &content,
    };

    let mut output = prefix.trim_end().to_string();
    output.push_str("\n\n");

    if let Some(release) = &expect.release {
        output.push_str("[expect.release]\n");
        write_expected_entity(&mut output, release, None);
    }

    for (artist_ext_id, artist_entity) in &expect.artists {
        output.push_str(&format!(
            "[expect.artists.\"{}\"]\n",
            escape_toml_key(artist_ext_id)
        ));
        write_expected_entity(
            &mut output,
            artist_entity,
            Some(("artist_id", artist_ext_id.as_str())),
        );
    }

    for (recording_id, track) in &expect.tracks {
        output.push_str(&format!(
            "[expect.tracks.\"{}\"]\n",
            escape_toml_key(recording_id)
        ));
        write_expected_entity(
            &mut output,
            track,
            Some(("recording_id", recording_id.as_str())),
        );
    }

    std::fs::write(path, output.trim_end().to_string() + "\n")?;
    Ok(())
}

fn write_expected_entity(
    output: &mut String,
    entity: &test_case::ExpectedEntity,
    skip_id: Option<(&str, &str)>,
) {
    let ids_pairs: Vec<String> = entity
        .ids
        .iter()
        .filter(|(k, v)| {
            if let Some((skip_key, skip_value)) = skip_id {
                if k.as_str() == skip_key {
                    return !matches!(v, test_case::AcceptedValues::Single(value) if value == skip_value);
                }
            }
            true
        })
        .map(|(k, v)| match v {
            test_case::AcceptedValues::Single(s) => format!("{k} = \"{s}\""),
            test_case::AcceptedValues::Multiple(vals) => {
                let quoted: Vec<String> = vals.iter().map(|s| format!("\"{s}\"")).collect();
                format!("{k} = [{}]", quoted.join(", "))
            }
        })
        .collect();

    if !ids_pairs.is_empty() {
        output.push_str("ids = { ");
        output.push_str(&ids_pairs.join(", "));
        output.push_str(" }\n");
    }

    if !entity.fields.is_empty() {
        output.push_str("fields = { ");
        let pairs: Vec<String> = entity
            .fields
            .iter()
            .map(|(k, v)| {
                let val_str = match v {
                    toml::Value::String(s) => format!("\"{s}\""),
                    other => other.to_string(),
                };
                format!("{k} = {val_str}")
            })
            .collect();
        output.push_str(&pairs.join(", "));
        output.push_str(" }\n");
    }

    output.push('\n');
}

fn merge_entity_ids(
    captured: &mut test_case::ExpectedEntity,
    existing: &test_case::ExpectedEntity,
) {
    for (id_type, captured_val) in captured.ids.iter_mut() {
        let Some(existing_val) = existing.ids.get(id_type) else {
            continue;
        };
        if let (
            test_case::AcceptedValues::Single(recorded),
            test_case::AcceptedValues::Multiple(existing_vals),
        ) = (&*captured_val, existing_val)
        {
            if existing_vals.iter().any(|v| v == recorded) {
                *captured_val = test_case::AcceptedValues::Multiple(existing_vals.clone());
            }
        }
    }
}

fn escape_toml_key(key: &str) -> String {
    key.replace('\\', "\\\\").replace('"', "\\\"")
}
