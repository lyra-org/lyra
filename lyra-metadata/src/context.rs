// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use crate::{
    LookupHints,
    TrackMetadata,
    artists::normalize_unicode_nfc,
    extract_lookup_hints_from_file_path_with_library_root,
    infer_lookup_hints_from_tracks,
    year::normalize_release_date,
};

/// Build plugin-ready release context JSON from processed metadata.
/// Groups tracks into a single release, derives album artists, and assigns synthetic db_ids.
pub fn build_release_context_from_tags(tracks: &[TrackMetadata]) -> serde_json::Value {
    build_release_context_from_tags_with_library_root(tracks, None)
}

/// Build plugin-ready release context JSON from processed metadata.
/// Same as `build_release_context_from_tags`, but uses an optional library root
/// to derive lookup hints from paths relative to the library directory.
pub fn build_release_context_from_tags_with_library_root(
    tracks: &[TrackMetadata],
    library_root: Option<&str>,
) -> serde_json::Value {
    if tracks.is_empty() {
        return serde_json::json!({});
    }

    // Derive album artists: explicit tag > majority track artists > empty
    let mut album_artists = tracks
        .iter()
        .find_map(|t| t.album_artists.clone())
        .unwrap_or_else(|| {
            let total = tracks.len();
            let mut counts: HashMap<&str, usize> = HashMap::new();
            let mut order: Vec<&str> = Vec::new();
            for track in tracks {
                if let Some(artists) = track.artists.as_deref() {
                    for name in artists {
                        if !counts.contains_key(name.as_str()) {
                            order.push(name.as_str());
                        }
                        *counts.entry(name.as_str()).or_default() += 1;
                    }
                }
            }
            let mut result: Vec<String> = order
                .into_iter()
                .filter(|name| counts[name] > total / 2)
                .map(|name| name.to_string())
                .collect();
            result.sort();
            result
        });

    // Reorder album artists by track frequency (most common first) to match
    // the server's entity ordering for multi-artist releases.
    if album_artists.len() > 1 {
        let mut track_counts: HashMap<&str, usize> = HashMap::new();
        for track in tracks {
            if let Some(artists) = track.artists.as_deref() {
                for name in artists {
                    *track_counts.entry(name.as_str()).or_default() += 1;
                }
            }
        }
        album_artists.sort_by(|a, b| {
            let count_a = track_counts.get(a.as_str()).copied().unwrap_or(0);
            let count_b = track_counts.get(b.as_str()).copied().unwrap_or(0);
            count_b.cmp(&count_a)
        });
    }

    // Assign synthetic db_ids: release=1, artists=2..N, tracks=N+1..M
    let mut next_id: i64 = 1;
    let release_id = next_id;
    next_id += 1;

    // Collect unique artist names preserving order, assign IDs
    let mut artist_id_map: HashMap<String, i64> = HashMap::new();
    let mut artist_order: Vec<String> = Vec::new();
    for name in &album_artists {
        if !artist_id_map.contains_key(name) {
            artist_id_map.insert(name.clone(), next_id);
            artist_order.push(name.clone());
            next_id += 1;
        }
    }
    // Also collect track artists
    for track in tracks {
        if let Some(artists) = &track.artists {
            for name in artists {
                if !artist_id_map.contains_key(name) {
                    artist_id_map.insert(name.clone(), next_id);
                    artist_order.push(name.clone());
                    next_id += 1;
                }
            }
        }
    }

    // Build album artists JSON
    let album_artists_json: Vec<serde_json::Value> = album_artists
        .iter()
        .map(|name| {
            let id = artist_id_map[name];
            serde_json::json!({
                "db_id": id,
                "artist_name": name,
                "scan_name": name,
                "sort_name": null,
                "external_ids": {}
            })
        })
        .collect();

    let track_lookup_hints: Vec<LookupHints> = tracks
        .iter()
        .map(|track| {
            track
                .file_path
                .as_deref()
                .map(|path| {
                    extract_lookup_hints_from_file_path_with_library_root(path, library_root)
                })
                .unwrap_or_default()
        })
        .collect();
    let release_lookup_hints = infer_lookup_hints_from_tracks(&track_lookup_hints);

    // Build tracks JSON
    let mut tracks_json: Vec<serde_json::Value> = Vec::new();
    for (track, lookup_hints) in tracks.iter().zip(track_lookup_hints.iter()) {
        let track_id = next_id;
        next_id += 1;

        let track_artists: Vec<serde_json::Value> = track
            .artists
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|name| {
                let id = artist_id_map[name];
                serde_json::json!({
                    "db_id": id,
                    "artist_name": name,
                    "scan_name": name,
                    "sort_name": null,
                    "external_ids": {}
                })
            })
            .collect();

        let mut track_obj = serde_json::json!({
            "db_id": track_id,
            "track_title": track.title.as_deref().unwrap_or(""),
            "artists": track_artists,
            "external_ids": {}
        });

        if let Some(file_path) = track.file_path.as_deref() {
            track_obj["file_path"] = serde_json::json!(file_path);
        }
        if let Some(disc) = track.disc {
            track_obj["disc"] = serde_json::json!(disc);
        }
        if let Some(disc_total) = track.disc_total {
            track_obj["disc_total"] = serde_json::json!(disc_total);
        }
        if let Some(track_num) = track.track {
            track_obj["track"] = serde_json::json!(track_num);
        }
        if let Some(track_total) = track.track_total {
            track_obj["track_total"] = serde_json::json!(track_total);
        }
        if let Some(duration_ms) = track.duration_ms {
            track_obj["duration_ms"] = serde_json::json!(duration_ms);
        }
        track_obj["lookup_hints"] = serde_json::json!(lookup_hints);

        tracks_json.push(track_obj);
    }

    let release_title = normalize_unicode_nfc(tracks[0].album.as_deref().unwrap_or(""));

    let mut context = serde_json::json!({
        "db_id": release_id,
        "album_title": release_title,
        "artists": album_artists_json,
        "tracks": tracks_json,
        "lookup_hints": release_lookup_hints,
        "external_ids": {}
    });

    if let Some(release_date) = tracks.iter().filter_map(release_date_from_track).max() {
        context["release_date"] = serde_json::json!(release_date);
    }

    context
}

fn release_date_from_track(track: &TrackMetadata) -> Option<String> {
    track
        .date
        .as_deref()
        .and_then(normalize_release_date)
        .or_else(|| track.year.map(|year| format!("{year:04}")))
}
