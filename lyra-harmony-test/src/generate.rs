// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    BTreeMap,
    HashMap,
    HashSet,
};
use std::io::Write;
use std::path::Path;

use serde::Deserialize;

use crate::runner::json_to_toml;

const FIXTURE_LIBRARY_DIRECTORY: &str = ".";

#[derive(Deserialize)]
struct CaptureFile {
    library: CaptureLibrary,
    releases: Vec<CapturedRelease>,
}

#[derive(Deserialize)]
struct CaptureLibrary {
    directory: String,
    language: Option<String>,
    country: Option<String>,
}

#[derive(Deserialize)]
struct CapturedRelease {
    context: serde_json::Value,
    raw_tags: Vec<lyra_metadata::RawTrackTags>,
    results: HashMap<String, HashMap<String, CapturedEntity>>,
}

#[derive(Deserialize)]
struct CapturedEntity {
    ids: HashMap<String, String>,
    fields: HashMap<String, serde_json::Value>,
}

struct IdEdit {
    real_entity_id: i64,
    id_type: String,
    values: Vec<String>,
}

enum EditMode {
    Amend,
    Replace,
}

struct IdEdits {
    edits: Vec<IdEdit>,
    mode: EditMode,
}

struct GenerateReleaseView<'a> {
    release: &'a CapturedRelease,
    release_title: String,
    first_artist: String,
    track_count: usize,
    search_blob: String,
}

enum PromptAction {
    Skip,
    Include,
    Ids,
    Modify,
    Quit,
    Next,
    Prev,
    ShowFilter,
    SetFilter(String),
    ClearFilter,
    Unknown(String),
}

pub fn run_generate(
    capture_path: &Path,
    output_dir: &Path,
    include_all: bool,
) -> anyhow::Result<()> {
    let content = std::fs::read_to_string(capture_path)?;
    let capture: CaptureFile = serde_json::from_str(&content)?;

    std::fs::create_dir_all(output_dir)?;

    let all_id_types = collect_all_id_types(&capture);

    let releases_with_results: Vec<&CapturedRelease> = capture
        .releases
        .iter()
        .filter(|a| !a.results.is_empty() && !a.raw_tags.is_empty())
        .collect();

    let release_views: Vec<GenerateReleaseView<'_>> = releases_with_results
        .into_iter()
        .map(|release| GenerateReleaseView {
            release,
            release_title: extract_release_title(&release.context),
            first_artist: extract_first_artist(&release.context),
            track_count: release.raw_tags.len(),
            search_blob: build_search_blob(&release.context),
        })
        .collect();

    let total = release_views.len();
    if total == 0 {
        eprintln!("No releases with results and raw_tags found in capture file.");
        return Ok(());
    }

    let mut generated = 0usize;
    let mut skipped = 0usize;

    if include_all {
        for (i, release_view) in release_views.iter().enumerate() {
            let base_filename =
                base_filename(&release_view.release_title, &release_view.first_artist);
            if file_exists_with_base(output_dir, &base_filename) {
                println!(
                    "\n--- Release {}/{} --- (skipped, already exists)",
                    i + 1,
                    total
                );
                println!("Title: {}", release_view.release_title);
                skipped += 1;
                continue;
            }

            println!("\n--- Release {}/{} ---", i + 1, total);
            println!("Title: {}", release_view.release_title);
            println!("Artist: {}", release_view.first_artist);
            println!("Tracks: {}", release_view.track_count);
            print_release_results(release_view.release);

            let toml_content = generate_toml(release_view.release, &capture.library, None)?;
            let filename = auto_filename(
                &release_view.release_title,
                &release_view.first_artist,
                output_dir,
            );
            let output_path = output_dir.join(&filename);
            std::fs::write(&output_path, &toml_content)?;
            println!("  -> {}", output_path.display());
            generated += 1;
        }

        println!("\n{total} releases: {generated} generated, {skipped} skipped");
        return Ok(());
    }

    let search_blobs: Vec<String> = release_views
        .iter()
        .map(|rv| rv.search_blob.clone())
        .collect();
    let mut processed = vec![false; total];
    let mut processed_count = 0usize;
    let mut current_index = 0usize;
    let mut active_filter: Option<String> = None;
    let mut matched_indices: Vec<usize> = (0..total).collect();

    while processed_count < total {
        let visible_indices = remaining_indices(&processed, &matched_indices);

        if visible_indices.is_empty() {
            let filter = active_filter.as_deref().unwrap_or("");
            println!("\nNo remaining releases match filter '{filter}'.");
            print!("Use [f <query>], [c]lear filter, or [q]uit? ");
            std::io::stdout().flush()?;

            let mut line = String::new();
            std::io::stdin().read_line(&mut line)?;
            match parse_prompt_action(&line) {
                PromptAction::SetFilter(query) => {
                    let query = normalize_query(&query);
                    active_filter = Some(query.clone());
                    matched_indices = compute_matches(&search_blobs, &query);
                    println!("  filter: '{query}' ({} matches)", matched_indices.len());
                }
                PromptAction::ShowFilter => {
                    if let Some(filter) = active_filter.as_deref() {
                        println!("  filter: '{filter}' ({} matches)", matched_indices.len());
                    } else {
                        println!("  filter: (none)");
                    }
                }
                PromptAction::ClearFilter => {
                    active_filter = None;
                    matched_indices = (0..total).collect();
                    println!("  filter cleared");
                }
                PromptAction::Quit => {
                    println!("  quitting (re-run to continue where you left off)");
                    break;
                }
                _ => {
                    println!("  no releases available. change/clear filter or quit.");
                }
            }
            continue;
        }

        if !visible_indices.contains(&current_index) {
            current_index = visible_indices[0];
        }

        let visible_pos = visible_indices
            .iter()
            .position(|&idx| idx == current_index)
            .map(|idx| idx + 1)
            .unwrap_or(1);
        let visible_total = visible_indices.len();
        let release_view = &release_views[current_index];

        let base_filename = base_filename(&release_view.release_title, &release_view.first_artist);
        if file_exists_with_base(output_dir, &base_filename) {
            println!(
                "\n--- Release {}/{} --- (skipped, already exists)",
                current_index + 1,
                total
            );
            if let Some(filter) = active_filter.as_deref() {
                println!("Filter: {filter} ({visible_pos}/{visible_total} remaining matches)");
            } else {
                println!("Filter: (none)");
            }
            println!("Title: {}", release_view.release_title);
            skipped += 1;
            mark_processed(&mut processed, &mut processed_count, current_index);
            if let Some(next) = next_visible_index(current_index, &visible_indices) {
                current_index = next;
            }
            continue;
        }

        println!("\n--- Release {}/{} ---", current_index + 1, total);
        if let Some(filter) = active_filter.as_deref() {
            println!("Filter: {filter} ({visible_pos}/{visible_total} remaining matches)");
        } else {
            println!("Filter: (none)");
        }
        println!("Title: {}", release_view.release_title);
        println!("Artist: {}", release_view.first_artist);
        println!("Tracks: {}", release_view.track_count);
        print_release_results(release_view.release);

        print!(
            "\n[s]kip, [i]nclude, i[d]s, [m]odify, [f <query>] filter, [c]lear filter, [n]ext, [p]rev, [q]uit? "
        );
        std::io::stdout().flush()?;

        let mut line = String::new();
        std::io::stdin().read_line(&mut line)?;
        let action = parse_prompt_action(&line);
        match action {
            PromptAction::Ids => {
                let id_edits = interactive_ids(release_view.release, &all_id_types)?;
                match id_edits {
                    Some(edits) => {
                        let toml_content =
                            generate_toml(release_view.release, &capture.library, Some(&edits))?;
                        let filename = auto_filename(
                            &release_view.release_title,
                            &release_view.first_artist,
                            output_dir,
                        );
                        let output_path = output_dir.join(&filename);
                        std::fs::write(&output_path, &toml_content)?;
                        println!("  -> {}", output_path.display());
                        generated += 1;
                    }
                    None => {
                        println!("  no edits, skipping");
                        skipped += 1;
                    }
                }
                mark_processed(&mut processed, &mut processed_count, current_index);
                if let Some(next) = next_visible_index(current_index, &visible_indices) {
                    current_index = next;
                }
            }
            PromptAction::Include => {
                let toml_content = generate_toml(release_view.release, &capture.library, None)?;
                let filename = auto_filename(
                    &release_view.release_title,
                    &release_view.first_artist,
                    output_dir,
                );
                let output_path = output_dir.join(&filename);
                std::fs::write(&output_path, &toml_content)?;
                println!("  -> {}", output_path.display());
                generated += 1;

                mark_processed(&mut processed, &mut processed_count, current_index);
                if let Some(next) = next_visible_index(current_index, &visible_indices) {
                    current_index = next;
                }
            }
            PromptAction::Modify => {
                let toml_content = generate_toml(release_view.release, &capture.library, None)?;
                let filename = auto_filename(
                    &release_view.release_title,
                    &release_view.first_artist,
                    output_dir,
                );
                let output_path = output_dir.join(&filename);
                let temp_path = output_dir.join(format!(".{}.tmp", filename));
                std::fs::write(&temp_path, &toml_content)?;

                let editor = std::env::var("EDITOR").unwrap_or_else(|_| "vi".to_string());
                let status = std::process::Command::new(&editor)
                    .arg(&temp_path)
                    .stdin(std::process::Stdio::inherit())
                    .stdout(std::process::Stdio::inherit())
                    .stderr(std::process::Stdio::inherit())
                    .status();

                match status {
                    Ok(s) if s.success() => {
                        std::fs::rename(&temp_path, &output_path)?;
                        println!("  -> {}", output_path.display());
                        generated += 1;
                    }
                    Ok(s) => {
                        let _ = std::fs::remove_file(&temp_path);
                        println!("  editor exited with status {}, skipping", s);
                        skipped += 1;
                    }
                    Err(err) => {
                        let _ = std::fs::remove_file(&temp_path);
                        println!("  failed to launch editor: {err}");
                        skipped += 1;
                    }
                }

                mark_processed(&mut processed, &mut processed_count, current_index);
                if let Some(next) = next_visible_index(current_index, &visible_indices) {
                    current_index = next;
                }
            }
            PromptAction::Skip => {
                skipped += 1;
                mark_processed(&mut processed, &mut processed_count, current_index);
                if let Some(next) = next_visible_index(current_index, &visible_indices) {
                    current_index = next;
                }
            }
            PromptAction::Next => {
                if let Some(next) = next_visible_index(current_index, &visible_indices) {
                    current_index = next;
                }
            }
            PromptAction::Prev => {
                if let Some(prev) = prev_visible_index(current_index, &visible_indices) {
                    current_index = prev;
                }
            }
            PromptAction::SetFilter(query) => {
                let query = normalize_query(&query);
                active_filter = Some(query.clone());
                matched_indices = compute_matches(&search_blobs, &query);
                println!("  filter: '{query}' ({} matches)", matched_indices.len());
            }
            PromptAction::ShowFilter => {
                if let Some(filter) = active_filter.as_deref() {
                    println!("  filter: '{filter}' ({} matches)", matched_indices.len());
                } else {
                    println!("  filter: (none)");
                }
            }
            PromptAction::ClearFilter => {
                active_filter = None;
                matched_indices = (0..total).collect();
                println!("  filter cleared");
            }
            PromptAction::Quit => {
                println!("  quitting (re-run to continue where you left off)");
                break;
            }
            PromptAction::Unknown(raw) => {
                println!("  unknown command: '{raw}'");
            }
        }
    }

    println!("\n{total} releases: {generated} generated, {skipped} skipped");
    Ok(())
}

fn parse_prompt_action(line: &str) -> PromptAction {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return PromptAction::Skip;
    }

    let mut parts = trimmed.splitn(2, char::is_whitespace);
    let cmd = parts.next().unwrap_or_default();
    let arg = parts.next().map(str::trim).unwrap_or_default();

    if cmd.eq_ignore_ascii_case("f") {
        if arg.is_empty() {
            return PromptAction::ShowFilter;
        }
        return PromptAction::SetFilter(arg.to_string());
    }

    if cmd.eq_ignore_ascii_case("s") {
        PromptAction::Skip
    } else if cmd.eq_ignore_ascii_case("i") {
        PromptAction::Include
    } else if cmd.eq_ignore_ascii_case("d") {
        PromptAction::Ids
    } else if cmd.eq_ignore_ascii_case("m") {
        PromptAction::Modify
    } else if cmd.eq_ignore_ascii_case("q") {
        PromptAction::Quit
    } else if cmd.eq_ignore_ascii_case("n") {
        PromptAction::Next
    } else if cmd.eq_ignore_ascii_case("p") {
        PromptAction::Prev
    } else if cmd.eq_ignore_ascii_case("c") {
        PromptAction::ClearFilter
    } else {
        PromptAction::Unknown(trimmed.to_string())
    }
}

fn extract_release_title(context: &serde_json::Value) -> String {
    context
        .get("release_title")
        .and_then(|v| v.as_str())
        .unwrap_or("Unknown")
        .to_string()
}

fn extract_first_artist(context: &serde_json::Value) -> String {
    context
        .get("artists")
        .and_then(|v| v.as_array())
        .and_then(|a| a.first())
        .and_then(|a| {
            a.get("scan_name")
                .and_then(|v| v.as_str())
                .or_else(|| a.get("artist_name").and_then(|v| v.as_str()))
        })
        .unwrap_or("Unknown")
        .to_string()
}

fn build_search_blob(context: &serde_json::Value) -> String {
    let mut parts = Vec::new();

    if let Some(release_title) = context.get("release_title").and_then(|v| v.as_str()) {
        let title = release_title.trim();
        if !title.is_empty() {
            parts.push(title.to_string());
        }
    }

    if let Some(artists_array) = context.get("artists").and_then(|v| v.as_array()) {
        for artist_entry in artists_array {
            let Some(name) = artist_entry
                .get("scan_name")
                .and_then(|v| v.as_str())
                .or_else(|| artist_entry.get("artist_name").and_then(|v| v.as_str()))
            else {
                continue;
            };
            let name = name.trim();
            if !name.is_empty() {
                parts.push(name.to_string());
            }
        }
    }

    parts.join(" ").to_lowercase()
}

fn normalize_query(query: &str) -> String {
    query.trim().to_lowercase()
}

fn compute_matches<T: AsRef<str>>(search_blobs: &[T], query: &str) -> Vec<usize> {
    let normalized = normalize_query(query);
    if normalized.is_empty() {
        return (0..search_blobs.len()).collect();
    }

    search_blobs
        .iter()
        .enumerate()
        .filter_map(|(idx, blob)| blob.as_ref().contains(&normalized).then_some(idx))
        .collect()
}

fn remaining_indices(processed: &[bool], candidates: &[usize]) -> Vec<usize> {
    candidates
        .iter()
        .copied()
        .filter(|idx| !processed[*idx])
        .collect()
}

fn next_visible_index(current: usize, indices: &[usize]) -> Option<usize> {
    if indices.is_empty() {
        return None;
    }

    indices
        .iter()
        .copied()
        .find(|&idx| idx > current)
        .or_else(|| indices.first().copied())
}

fn prev_visible_index(current: usize, indices: &[usize]) -> Option<usize> {
    if indices.is_empty() {
        return None;
    }

    indices
        .iter()
        .rev()
        .copied()
        .find(|&idx| idx < current)
        .or_else(|| indices.last().copied())
}

fn mark_processed(processed: &mut [bool], processed_count: &mut usize, index: usize) {
    if !processed[index] {
        processed[index] = true;
        *processed_count += 1;
    }
}

fn print_release_results(release: &CapturedRelease) {
    for (provider_id, entities) in &release.results {
        println!("\nPlugin '{}' results:", provider_id);
        for (entity_id_str, entity) in entities {
            let label = entity_label(&release.context, entity_id_str);
            print!("  {} ({}):", label, entity_id_str);

            if !entity.ids.is_empty() {
                let pairs: Vec<String> = entity
                    .ids
                    .iter()
                    .map(|(k, v)| format!("{k} = {v}"))
                    .collect();
                print!("\n    ids: {}", pairs.join(", "));
            }

            if !entity.fields.is_empty() {
                let pairs: Vec<String> = entity
                    .fields
                    .iter()
                    .map(|(k, v)| format!("{k} = {}", format_json_value(v)))
                    .collect();
                print!("\n    fields: {}", pairs.join(", "));
            }
            println!();
        }
    }
}

/// Collect all ID type names across every release in the capture.
fn collect_all_id_types(capture: &CaptureFile) -> Vec<String> {
    let mut types = BTreeMap::<String, ()>::new();
    for release in &capture.releases {
        for entities in release.results.values() {
            for entity in entities.values() {
                for key in entity.ids.keys() {
                    types.entry(key.clone()).or_default();
                }
            }
        }
    }
    types.into_keys().collect()
}

/// Collect (real_db_id, label) pairs from a context, deduped, in display order.
fn collect_entities(context: &serde_json::Value) -> Vec<(i64, String)> {
    let mut entities = Vec::new();
    let mut seen = HashSet::new();

    if let Some(id) = context.get("db_id").and_then(|v| v.as_i64()) {
        seen.insert(id);
        entities.push((id, "Release".to_string()));
    }

    if let Some(artists_array) = context.get("artists").and_then(|v| v.as_array()) {
        for artist_entry in artists_array {
            if let Some(id) = artist_entry.get("db_id").and_then(|v| v.as_i64()) {
                if seen.insert(id) {
                    let name = artist_display_name(artist_entry);
                    entities.push((id, format!("Artist ({name})")));
                }
            }
        }
    }

    if let Some(tracks) = context.get("tracks").and_then(|v| v.as_array()) {
        for (i, track) in tracks.iter().enumerate() {
            if let Some(track_artists_array) = track.get("artists").and_then(|v| v.as_array()) {
                for artist_entry in track_artists_array {
                    if let Some(id) = artist_entry.get("db_id").and_then(|v| v.as_i64()) {
                        if seen.insert(id) {
                            let name = artist_display_name(artist_entry);
                            entities.push((id, format!("Artist ({name})")));
                        }
                    }
                }
            }
            if let Some(id) = track.get("db_id").and_then(|v| v.as_i64()) {
                if seen.insert(id) {
                    let title = track
                        .get("track_title")
                        .and_then(|v| v.as_str())
                        .unwrap_or("?");
                    entities.push((id, format!("Track {} \u{2014} {title}", i + 1)));
                }
            }
        }
    }

    entities
}

/// Interactive ID selection loop. Returns `None` if the user made no edits.
fn interactive_ids(
    release: &CapturedRelease,
    all_id_types: &[String],
) -> anyhow::Result<Option<IdEdits>> {
    let entities = collect_entities(&release.context);
    let plugin = release.results.keys().next().cloned().unwrap_or_default();
    let plugin_results = release.results.get(&plugin);

    println!("\nEntities:");
    for (i, (_, label)) in entities.iter().enumerate() {
        println!("  {}. {}", i + 1, label);
    }

    println!("\nAvailable ID types: {}", all_id_types.join(", "));

    let mut edits = Vec::new();

    loop {
        print!("\nEntity (or empty to finish)? ");
        std::io::stdout().flush()?;
        let mut line = String::new();
        std::io::stdin().read_line(&mut line)?;
        let line = line.trim();

        if line.is_empty() {
            break;
        }

        let idx: usize = match line.parse::<usize>() {
            Ok(n) if n >= 1 && n <= entities.len() => n - 1,
            _ => {
                println!("  invalid entity number");
                continue;
            }
        };

        let (real_id, label) = &entities[idx];
        println!("  Selected: {label}");

        // Show current IDs for this entity
        if let Some(results) = plugin_results {
            if let Some(entity) = results.get(&real_id.to_string()) {
                if !entity.ids.is_empty() {
                    println!("  Current IDs:");
                    for (k, v) in &entity.ids {
                        println!("    {k} = {v}");
                    }
                }
            }
        }

        print!("ID type? ");
        std::io::stdout().flush()?;
        let mut id_type = String::new();
        std::io::stdin().read_line(&mut id_type)?;
        let id_type = id_type.trim().to_string();

        if id_type.is_empty() {
            continue;
        }

        // Show current value for this specific ID
        let current = plugin_results
            .and_then(|r| r.get(&real_id.to_string()))
            .and_then(|e| e.ids.get(&id_type));

        if let Some(val) = current {
            println!("  Current value: {val}");
            print!("New value (enter to keep, comma-separate for alternatives)? ");
        } else {
            println!("  Current value: (none)");
            print!("New value (comma-separate for alternatives)? ");
        }
        std::io::stdout().flush()?;

        let mut value = String::new();
        std::io::stdin().read_line(&mut value)?;
        let value = value.trim().to_string();

        let final_values: Vec<String> = if value.is_empty() {
            if let Some(val) = current {
                vec![val.clone()]
            } else {
                println!("  skipped (no value entered)");
                continue;
            }
        } else {
            value.split(',').map(|s| s.trim().to_string()).collect()
        };

        edits.push(IdEdit {
            real_entity_id: *real_id,
            id_type,
            values: final_values,
        });
    }

    if edits.is_empty() {
        return Ok(None);
    }

    print!("\n[a]mend or [r]eplace? ");
    std::io::stdout().flush()?;
    let mut mode_line = String::new();
    std::io::stdin().read_line(&mut mode_line)?;
    let mode = match mode_line.trim().chars().next() {
        Some('r') | Some('R') => EditMode::Replace,
        _ => EditMode::Amend,
    };

    Ok(Some(IdEdits { edits, mode }))
}

/// Determine a display label for an entity ID based on context.
fn entity_label(context: &serde_json::Value, entity_id_str: &str) -> String {
    let target_id: i64 = match entity_id_str.parse() {
        Ok(id) => id,
        Err(_) => return "Unknown".to_string(),
    };

    if context
        .get("db_id")
        .and_then(|v| v.as_i64())
        .is_some_and(|id| id == target_id)
    {
        return "Release".to_string();
    }

    if let Some(artists_array) = context.get("artists").and_then(|v| v.as_array()) {
        for artist_entry in artists_array {
            if artist_entry
                .get("db_id")
                .and_then(|v| v.as_i64())
                .is_some_and(|id| id == target_id)
            {
                let name = artist_display_name(artist_entry);
                return format!("Artist ({name})");
            }
        }
    }

    if let Some(tracks) = context.get("tracks").and_then(|v| v.as_array()) {
        for track in tracks {
            if track
                .get("db_id")
                .and_then(|v| v.as_i64())
                .is_some_and(|id| id == target_id)
            {
                let title = track
                    .get("track_title")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?");
                return format!("Track ({title})");
            }

            if let Some(track_artists_array) = track.get("artists").and_then(|v| v.as_array()) {
                for artist_entry in track_artists_array {
                    if artist_entry
                        .get("db_id")
                        .and_then(|v| v.as_i64())
                        .is_some_and(|id| id == target_id)
                    {
                        let name = artist_display_name(artist_entry);
                        return format!("Artist ({name})");
                    }
                }
            }
        }
    }

    "Unknown".to_string()
}

fn artist_display_name(entry: &serde_json::Value) -> &str {
    entry
        .get("scan_name")
        .and_then(|v| v.as_str())
        .or_else(|| entry.get("artist_name").and_then(|v| v.as_str()))
        .unwrap_or("?")
}

fn format_json_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => format!("\"{s}\""),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(format_json_value).collect();
            format!("[{}]", items.join(", "))
        }
        other => other.to_string(),
    }
}

fn escape_toml_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn normalize_fixture_path(path: &str) -> String {
    path.replace('\\', "/")
}

fn strip_fixture_library_root(file_path: &str, library_directory: &str) -> anyhow::Result<String> {
    let normalized_path = normalize_fixture_path(file_path);
    let normalized_library = normalize_fixture_path(library_directory);
    let library_root = normalized_library.trim_end_matches('/');

    if library_root.is_empty() || library_root == "." {
        if is_absolute_fixture_path(&normalized_path) {
            anyhow::bail!(
                "raw tag file_path '{}' is absolute, but the captured library directory is '{}'",
                file_path,
                library_directory
            );
        }
        return Ok(normalized_path);
    }

    if normalized_path.eq_ignore_ascii_case(library_root) {
        return Ok(normalized_path
            .rsplit('/')
            .next()
            .unwrap_or(&normalized_path)
            .to_string());
    }

    if normalized_path.len() > library_root.len()
        && normalized_path[..library_root.len()].eq_ignore_ascii_case(library_root)
        && normalized_path.as_bytes().get(library_root.len()) == Some(&b'/')
    {
        return Ok(normalized_path[library_root.len() + 1..].to_string());
    }

    if is_absolute_fixture_path(&normalized_path) {
        anyhow::bail!(
            "raw tag file_path '{}' is outside captured library directory '{}'",
            file_path,
            library_directory
        );
    }

    Ok(normalized_path)
}

fn is_absolute_fixture_path(path: &str) -> bool {
    path.starts_with('/') || path.as_bytes().get(1).is_some_and(|byte| *byte == b':')
}

/// Write `[[raw_tags]]` array-of-tables sections for each raw track.
fn write_raw_tags_sections(
    out: &mut String,
    raw_tags: &[lyra_metadata::RawTrackTags],
    library_directory: &str,
) -> anyhow::Result<()> {
    for raw in raw_tags {
        out.push_str("\n[[raw_tags]]\n");
        let fixture_path = strip_fixture_library_root(&raw.file_path, library_directory)?;
        let escaped_path = escape_toml_string(&fixture_path);
        out.push_str(&format!("file_path = \"{escaped_path}\"\n"));
        if let Some(ref album) = raw.album {
            let escaped = escape_toml_string(album);
            out.push_str(&format!("album = \"{escaped}\"\n"));
        }
        if !raw.album_artists.is_empty() {
            let quoted: Vec<String> = raw
                .album_artists
                .iter()
                .map(|s| format!("\"{}\"", escape_toml_string(s)))
                .collect();
            out.push_str(&format!("album_artists = [{}]\n", quoted.join(", ")));
        }
        if !raw.artists.is_empty() {
            let quoted: Vec<String> = raw
                .artists
                .iter()
                .map(|s: &String| format!("\"{}\"", escape_toml_string(s)))
                .collect();
            out.push_str(&format!("artists = [{}]\n", quoted.join(", ")));
        }
        if let Some(ref title) = raw.title {
            let escaped = escape_toml_string(title);
            out.push_str(&format!("title = \"{escaped}\"\n"));
        }
        if let Some(ref date) = raw.date {
            let escaped = escape_toml_string(date);
            out.push_str(&format!("date = \"{escaped}\"\n"));
        }
        if let Some(ref copyright) = raw.copyright {
            let escaped = escape_toml_string(copyright);
            out.push_str(&format!("copyright = \"{escaped}\"\n"));
        }
        if let Some(ref genre) = raw.genre {
            let escaped = escape_toml_string(genre);
            out.push_str(&format!("genre = \"{escaped}\"\n"));
        }
        if let Some(ref label) = raw.label {
            let escaped = escape_toml_string(label);
            out.push_str(&format!("label = \"{escaped}\"\n"));
        }
        if let Some(ref catalog_number) = raw.catalog_number {
            let escaped = escape_toml_string(catalog_number);
            out.push_str(&format!("catalog_number = \"{escaped}\"\n"));
        }
        if let Some(disc) = raw.disc {
            out.push_str(&format!("disc = {disc}\n"));
        }
        if let Some(disc_total) = raw.disc_total {
            out.push_str(&format!("disc_total = {disc_total}\n"));
        }
        if let Some(track) = raw.track {
            out.push_str(&format!("track = {track}\n"));
        }
        if let Some(track_total) = raw.track_total {
            out.push_str(&format!("track_total = {track_total}\n"));
        }
        out.push_str(&format!("duration_ms = {}\n", raw.duration_ms));
        if let Some(sample_rate_hz) = raw.sample_rate_hz {
            out.push_str(&format!("sample_rate_hz = {sample_rate_hz}\n"));
        }
        if let Some(channel_count) = raw.channel_count {
            out.push_str(&format!("channel_count = {channel_count}\n"));
        }
        if let Some(bit_depth) = raw.bit_depth {
            out.push_str(&format!("bit_depth = {bit_depth}\n"));
        }
    }

    Ok(())
}

fn generate_toml(
    release: &CapturedRelease,
    library: &CaptureLibrary,
    id_edits: Option<&IdEdits>,
) -> anyhow::Result<String> {
    let mut out = String::new();

    // Plugin
    let plugin = release
        .results
        .keys()
        .next()
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());
    out.push_str(&format!("plugin = \"{plugin}\"\n"));

    // Library
    out.push_str("\n[library]\n");
    out.push_str(&format!("directory = \"{FIXTURE_LIBRARY_DIRECTORY}\"\n"));
    if let Some(ref lang) = library.language {
        out.push_str(&format!("language = \"{lang}\"\n"));
    }
    if let Some(ref country) = library.country {
        out.push_str(&format!("country = \"{country}\"\n"));
    }

    // Raw tags
    write_raw_tags_sections(&mut out, &release.raw_tags, &library.directory)?;

    // Expect section
    match id_edits {
        Some(IdEdits {
            mode: EditMode::Replace,
            edits,
        }) => {
            write_replace_expect(&mut out, edits);
        }
        _ => {
            let amend_edits = id_edits
                .filter(|e| matches!(e.mode, EditMode::Amend))
                .map(|e| &e.edits[..]);
            write_full_expect(&mut out, release, &plugin, amend_edits);
        }
    }

    Ok(out)
}

/// Write expect sections containing only user-specified IDs (replace mode).
fn write_replace_expect(out: &mut String, edits: &[IdEdit]) {
    let mut release_entity = OutputEntity::default();
    let mut artists_map: BTreeMap<String, OutputEntity> = BTreeMap::new();
    let mut tracks: BTreeMap<String, OutputEntity> = BTreeMap::new();

    for edit in edits {
        let target = classify_expect_target_from_edit(&edit.id_type, &edit.values);
        match target {
            ExpectTarget::Track(recording_id) => {
                let entity = tracks.entry(recording_id).or_default();
                entity.ids.insert(edit.id_type.clone(), edit.values.clone());
            }
            ExpectTarget::Artist(artist_id) => {
                let entity = artists_map.entry(artist_id).or_default();
                entity.ids.insert(edit.id_type.clone(), edit.values.clone());
            }
            ExpectTarget::Release => {
                release_entity
                    .ids
                    .insert(edit.id_type.clone(), edit.values.clone());
            }
        }
    }

    write_grouped_expect(out, &release_entity, &artists_map, &tracks);
}

/// Write full expect sections from capture results, optionally overlaying amend edits.
fn write_full_expect(
    out: &mut String,
    release: &CapturedRelease,
    plugin: &str,
    amend_edits: Option<&[IdEdit]>,
) {
    let mut release_expect = OutputEntity::default();
    let mut artists_map: BTreeMap<String, OutputEntity> = BTreeMap::new();
    let mut tracks: BTreeMap<String, OutputEntity> = BTreeMap::new();

    // Track which real entity IDs appear in results (for adding amend-only entities later)
    let mut seen_entity_ids = HashSet::new();

    for (provider_id, entities) in &release.results {
        if *provider_id != *plugin {
            continue;
        }

        for (entity_id_str, entity) in entities {
            let real_id: i64 = entity_id_str.parse().unwrap_or(0);
            seen_entity_ids.insert(real_id);

            let mut ids: BTreeMap<String, Vec<String>> = entity
                .ids
                .iter()
                .map(|(k, v)| (k.clone(), vec![v.clone()]))
                .collect();

            if let Some(edits) = amend_edits {
                for edit in edits.iter().filter(|e| e.real_entity_id == real_id) {
                    ids.insert(edit.id_type.clone(), edit.values.clone());
                }
            }

            let fields: BTreeMap<String, toml::Value> = entity
                .fields
                .iter()
                .filter(|(k, _)| !is_internal_field(k))
                .map(|(k, v)| (k.clone(), json_to_toml(v)))
                .collect();

            match classify_expect_target_from_ids(&ids) {
                ExpectTarget::Track(recording_id) => {
                    let slot = tracks.entry(recording_id).or_default();
                    merge_output_entity(slot, &ids, &fields);
                }
                ExpectTarget::Artist(artist_id) => {
                    let slot = artists_map.entry(artist_id).or_default();
                    merge_output_entity(slot, &ids, &fields);
                }
                ExpectTarget::Release => {
                    merge_output_entity(&mut release_expect, &ids, &fields);
                }
            }
        }
    }

    if let Some(edits) = amend_edits {
        for edit in edits {
            if seen_entity_ids.contains(&edit.real_entity_id) {
                continue;
            }
            let target = classify_expect_target_from_edit(&edit.id_type, &edit.values);
            match target {
                ExpectTarget::Track(recording_id) => {
                    let slot = tracks.entry(recording_id).or_default();
                    slot.ids.insert(edit.id_type.clone(), edit.values.clone());
                }
                ExpectTarget::Artist(artist_id) => {
                    let slot = artists_map.entry(artist_id).or_default();
                    slot.ids.insert(edit.id_type.clone(), edit.values.clone());
                }
                ExpectTarget::Release => {
                    release_expect
                        .ids
                        .insert(edit.id_type.clone(), edit.values.clone());
                }
            }
        }
    }

    write_grouped_expect(out, &release_expect, &artists_map, &tracks);
}

#[derive(Debug, Clone, Default)]
struct OutputEntity {
    ids: BTreeMap<String, Vec<String>>,
    fields: BTreeMap<String, toml::Value>,
}

#[derive(Debug, Clone)]
enum ExpectTarget {
    Release,
    Artist(String),
    Track(String),
}

fn classify_expect_target_from_ids(ids: &BTreeMap<String, Vec<String>>) -> ExpectTarget {
    if let Some(recording_ids) = ids.get("recording_id").and_then(|v| v.first()) {
        return ExpectTarget::Track(recording_ids.clone());
    }
    if let Some(artist_ids) = ids.get("artist_id").and_then(|v| v.first()) {
        return ExpectTarget::Artist(artist_ids.clone());
    }
    ExpectTarget::Release
}

fn classify_expect_target_from_edit(id_type: &str, values: &[String]) -> ExpectTarget {
    match id_type {
        "recording_id" => values
            .first()
            .cloned()
            .map(ExpectTarget::Track)
            .unwrap_or(ExpectTarget::Release),
        "artist_id" => values
            .first()
            .cloned()
            .map(ExpectTarget::Artist)
            .unwrap_or(ExpectTarget::Release),
        _ => ExpectTarget::Release,
    }
}

fn merge_output_entity(
    target: &mut OutputEntity,
    ids: &BTreeMap<String, Vec<String>>,
    fields: &BTreeMap<String, toml::Value>,
) {
    for (id_type, id_values) in ids {
        let slot = target.ids.entry(id_type.clone()).or_default();
        for value in id_values {
            if !slot.contains(value) {
                slot.push(value.clone());
            }
        }
    }
    for (field_name, field_value) in fields {
        target
            .fields
            .entry(field_name.clone())
            .or_insert_with(|| field_value.clone());
    }
}

fn write_grouped_expect(
    out: &mut String,
    release: &OutputEntity,
    artists_map: &BTreeMap<String, OutputEntity>,
    tracks: &BTreeMap<String, OutputEntity>,
) {
    if !release.ids.is_empty() || !release.fields.is_empty() {
        out.push_str("\n[expect.release]\n");
        write_output_entity(out, release, None);
    }

    for (artist_ext_id, entity) in artists_map {
        out.push_str(&format!(
            "\n[expect.artists.\"{}\"]\n",
            escape_toml_key(artist_ext_id)
        ));
        write_output_entity(out, entity, Some(("artist_id", artist_ext_id.as_str())));
    }

    for (recording_id, entity) in tracks {
        out.push_str(&format!(
            "\n[expect.tracks.\"{}\"]\n",
            escape_toml_key(recording_id)
        ));
        write_output_entity(out, entity, Some(("recording_id", recording_id.as_str())));
    }
}

fn write_output_entity(out: &mut String, entity: &OutputEntity, skip_id: Option<(&str, &str)>) {
    if !entity.ids.is_empty() {
        let pairs: Vec<String> = entity
            .ids
            .iter()
            .filter(|(id_type, values)| {
                if let Some((skip_type, skip_value)) = skip_id
                    && id_type.as_str() == skip_type
                    && values.len() == 1
                    && values[0] == skip_value
                {
                    return false;
                }
                true
            })
            .map(|(id_type, values)| format!("{id_type} = {}", format_id_values(values)))
            .collect();

        if !pairs.is_empty() {
            out.push_str("ids = { ");
            out.push_str(&pairs.join(", "));
            out.push_str(" }\n");
        }
    }

    if !entity.fields.is_empty() {
        out.push_str("fields = { ");
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
        out.push_str(&pairs.join(", "));
        out.push_str(" }\n");
    }
}

fn escape_toml_key(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn is_internal_field(name: &str) -> bool {
    matches!(
        name,
        "locked" | "created_at" | "ctime" | "library_id" | "db_id"
    )
}

/// Format ID values as TOML: `"single"` or `["a", "b"]` for multiple.
fn format_id_values(values: &[String]) -> String {
    if values.len() == 1 {
        format!("\"{}\"", values[0])
    } else {
        let quoted: Vec<String> = values.iter().map(|v| format!("\"{v}\"")).collect();
        format!("[{}]", quoted.join(", "))
    }
}

fn base_filename(release_title: &str, first_artist: &str) -> String {
    let base = format!(
        "{}_{}",
        to_snake_case(release_title),
        to_snake_case(first_artist)
    );
    if base.is_empty() {
        "test".to_string()
    } else {
        base
    }
}

fn file_exists_with_base(output_dir: &Path, base: &str) -> bool {
    if output_dir.join(format!("{base}.toml")).exists() {
        return true;
    }
    for n in 2..1000 {
        let candidate = output_dir.join(format!("{base}_{n}.toml"));
        if candidate.exists() {
            return true;
        }
        if !output_dir.join(format!("{base}_{}.toml", n - 1)).exists() && n > 2 {
            break;
        }
    }
    false
}

fn auto_filename(release_title: &str, first_artist: &str, output_dir: &Path) -> String {
    let base = base_filename(release_title, first_artist);

    let candidate = format!("{base}.toml");
    if !output_dir.join(&candidate).exists() {
        return candidate;
    }

    for n in 2..1000 {
        let candidate = format!("{base}_{n}.toml");
        if !output_dir.join(&candidate).exists() {
            return candidate;
        }
    }

    format!("{base}.toml")
}

fn to_snake_case(s: &str) -> String {
    s.chars()
        .filter_map(|c| {
            if c.is_alphanumeric() {
                Some(c.to_lowercase().next().unwrap_or(c))
            } else if c == ' ' || c == '-' || c == '_' {
                Some('_')
            } else {
                None
            }
        })
        .collect::<String>()
        .split('_')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("_")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        build_search_blob,
        compute_matches,
        next_visible_index,
        prev_visible_index,
        strip_fixture_library_root,
    };

    #[test]
    fn strips_unix_library_root_from_generated_fixture_paths() {
        let path =
            strip_fixture_library_root("/fixture-library/Artist/Album/01.flac", "/fixture-library")
                .expect("path should strip");

        assert_eq!(path, "Artist/Album/01.flac");
    }

    #[test]
    fn rejects_absolute_paths_outside_capture_library() {
        let result =
            strip_fixture_library_root("/other-library/Artist/Album/01.flac", "/fixture-library");

        assert!(result.is_err());
    }

    #[test]
    fn build_search_blob_includes_release_and_artists() {
        let context = json!({
            "release_title": "Again",
            "artists": [
                {"scan_name": "Beverly"},
                {"artist_name": "Guest Artist"}
            ]
        });

        let blob = build_search_blob(&context);
        assert!(blob.contains("again"));
        assert!(blob.contains("beverly"));
        assert!(blob.contains("guest artist"));
    }

    #[test]
    fn compute_matches_is_case_insensitive_contains() {
        let search_blobs = vec!["again beverly".to_string(), "other album".to_string()];
        assert_eq!(compute_matches(&search_blobs, "AGAIN"), vec![0]);
        assert_eq!(compute_matches(&search_blobs, "alb"), vec![1]);
    }

    #[test]
    fn compute_matches_does_not_use_track_titles() {
        let context = json!({
            "release_title": "Different Album",
            "artists": [{"scan_name": "Someone"}],
            "tracks": [{"track_title": "Needle Song"}]
        });

        let blob = build_search_blob(&context);
        let search_blobs = vec![blob];
        assert!(compute_matches(&search_blobs, "needle").is_empty());
    }

    #[test]
    fn compute_matches_handles_missing_artists() {
        let context = json!({
            "release_title": "Solo Album"
        });

        let blob = build_search_blob(&context);
        let search_blobs = vec![blob];
        assert_eq!(compute_matches(&search_blobs, "solo"), vec![0]);
    }

    #[test]
    fn navigation_wraps_for_next_and_prev() {
        let indices = vec![2usize, 5, 9];

        assert_eq!(next_visible_index(2, &indices), Some(5));
        assert_eq!(next_visible_index(9, &indices), Some(2));
        assert_eq!(next_visible_index(4, &indices), Some(5));

        assert_eq!(prev_visible_index(5, &indices), Some(2));
        assert_eq!(prev_visible_index(2, &indices), Some(9));
        assert_eq!(prev_visible_index(4, &indices), Some(2));
    }

    #[test]
    fn no_matches_and_empty_navigation_are_safe() {
        let search_blobs = vec!["again beverly".to_string()];
        assert!(compute_matches(&search_blobs, "nomatch").is_empty());
        assert_eq!(next_visible_index(0, &[]), None);
        assert_eq!(prev_visible_index(0, &[]), None);
    }
}
