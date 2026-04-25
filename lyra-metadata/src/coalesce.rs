// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        BTreeMap,
        HashMap,
        HashSet,
    },
    path::Path,
};

use crate::{
    ParsedReleaseGroup,
    ReleaseCoalesceTrack,
    artists::normalize_unicode_nfc,
};

#[derive(Debug, Clone)]
struct ReleaseGroupCandidate<T: ReleaseCoalesceTrack> {
    source_dir: String,
    tracks: Vec<T>,
    inferred_disc_from_dir: Option<u32>,
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
struct DiscTrack {
    disc: u32,
    track: u32,
}

pub fn coalesce_release_groups<T: ReleaseCoalesceTrack>(
    groups: Vec<ParsedReleaseGroup<T>>,
) -> Vec<Vec<T>> {
    if groups.is_empty() {
        return Vec::new();
    }

    let mut by_coalesce_group: BTreeMap<usize, Vec<ReleaseGroupCandidate<T>>> = BTreeMap::new();
    for group in groups {
        if group.tracks.is_empty() {
            continue;
        }

        let inferred_disc_from_dir = infer_disc_from_folder_name(&group.source_dir);
        let tracks_by_release_key = split_tracks_by_release_key(group.tracks);

        for tracks in tracks_by_release_key.into_values() {
            by_coalesce_group
                .entry(group.coalesce_group_key)
                .or_default()
                .push(ReleaseGroupCandidate {
                    source_dir: group.source_dir.clone(),
                    tracks,
                    inferred_disc_from_dir,
                });
        }
    }

    let mut coalesced_batches = Vec::new();
    for mut candidates in by_coalesce_group.into_values() {
        candidates.sort_by(|left, right| {
            left.source_dir.cmp(&right.source_dir).then_with(|| {
                representative_release_title(&left.tracks)
                    .unwrap_or_default()
                    .cmp(&representative_release_title(&right.tracks).unwrap_or_default())
            })
        });

        let mut buckets: Vec<Vec<ReleaseGroupCandidate<T>>> = Vec::new();

        for candidate in candidates {
            let mut merged = false;
            for bucket in &mut buckets {
                if bucket
                    .iter()
                    .all(|existing| groups_can_merge(existing, &candidate))
                {
                    bucket.push(candidate.clone());
                    merged = true;
                    break;
                }
            }

            if !merged {
                buckets.push(vec![candidate]);
            }
        }

        for mut bucket in buckets {
            if bucket.len() > 1 {
                let canonical_title = select_canonical_release_title(&bucket);
                for group in &mut bucket {
                    for track in &mut group.tracks {
                        track.set_album(canonical_title.clone());
                    }
                }
                bucket.sort_by(compare_canonical_priority);
            }

            let mut merged_tracks = Vec::new();
            for group in bucket {
                merged_tracks.extend(group.tracks);
            }

            if !merged_tracks.is_empty() {
                coalesced_batches.push(merged_tracks);
            }
        }
    }

    coalesced_batches
}

fn groups_can_merge<T: ReleaseCoalesceTrack>(
    left: &ReleaseGroupCandidate<T>,
    right: &ReleaseGroupCandidate<T>,
) -> bool {
    if !soft_checks_compatible(left, right) {
        return false;
    }

    structural_merge_allowed(left, right)
}

fn soft_checks_compatible<T: ReleaseCoalesceTrack>(
    left: &ReleaseGroupCandidate<T>,
    right: &ReleaseGroupCandidate<T>,
) -> bool {
    if let (Some(left_artist), Some(right_artist)) = (
        representative_artist_key(&left.tracks),
        representative_artist_key(&right.tracks),
    ) && left_artist != right_artist
    {
        return false;
    }

    if let (Some(left_year), Some(right_year)) = (
        representative_year(&left.tracks),
        representative_year(&right.tracks),
    ) && left_year != right_year
    {
        return false;
    }

    true
}

fn structural_merge_allowed<T: ReleaseCoalesceTrack>(
    left: &ReleaseGroupCandidate<T>,
    right: &ReleaseGroupCandidate<T>,
) -> bool {
    let left_tracks = match track_numbers(&left.tracks) {
        Some(numbers) => numbers,
        None => return false,
    };
    let right_tracks = match track_numbers(&right.tracks) {
        Some(numbers) => numbers,
        None => return false,
    };

    let left_known_pairs = known_disc_track_pairs(&left.tracks);
    let right_known_pairs = known_disc_track_pairs(&right.tracks);
    if has_overlap(&left_known_pairs, &right_known_pairs) {
        return false;
    }

    if !requires_disc_fallback(&left.tracks, &right.tracks, &left_tracks, &right_tracks) {
        return true;
    }

    let left_requires_inference = has_missing_disc(&left.tracks);
    let right_requires_inference = has_missing_disc(&right.tracks);
    if left_requires_inference && left.inferred_disc_from_dir.is_none() {
        return false;
    }
    if right_requires_inference && right.inferred_disc_from_dir.is_none() {
        return false;
    }

    let left_resolved =
        match resolve_disc_track_pairs_with_fallback(&left.tracks, left.inferred_disc_from_dir) {
            Some(pairs) => pairs,
            None => return false,
        };
    let right_resolved =
        match resolve_disc_track_pairs_with_fallback(&right.tracks, right.inferred_disc_from_dir) {
            Some(pairs) => pairs,
            None => return false,
        };

    !has_overlap(&left_resolved, &right_resolved)
}

fn requires_disc_fallback<T: ReleaseCoalesceTrack>(
    left_tracks_meta: &[T],
    right_tracks_meta: &[T],
    left_tracks: &HashSet<u32>,
    right_tracks: &HashSet<u32>,
) -> bool {
    let left_missing_disc_tracks = match missing_disc_track_numbers(left_tracks_meta) {
        Some(numbers) => numbers,
        None => return false,
    };
    let right_missing_disc_tracks = match missing_disc_track_numbers(right_tracks_meta) {
        Some(numbers) => numbers,
        None => return false,
    };

    left_missing_disc_tracks
        .iter()
        .any(|track_number| right_tracks.contains(track_number))
        || right_missing_disc_tracks
            .iter()
            .any(|track_number| left_tracks.contains(track_number))
}

fn track_numbers<T: ReleaseCoalesceTrack>(tracks: &[T]) -> Option<HashSet<u32>> {
    let mut values = HashSet::new();
    for track in tracks {
        values.insert(track.track()?);
    }
    Some(values)
}

fn has_missing_disc<T: ReleaseCoalesceTrack>(tracks: &[T]) -> bool {
    tracks.iter().any(|track| track.disc().is_none())
}

fn missing_disc_track_numbers<T: ReleaseCoalesceTrack>(tracks: &[T]) -> Option<HashSet<u32>> {
    let mut missing_disc_tracks = HashSet::new();
    for track in tracks {
        let track_number = track.track()?;
        if track.disc().is_none() {
            missing_disc_tracks.insert(track_number);
        }
    }
    Some(missing_disc_tracks)
}

fn known_disc_track_pairs<T: ReleaseCoalesceTrack>(tracks: &[T]) -> HashSet<DiscTrack> {
    tracks
        .iter()
        .filter_map(|track| {
            Some(DiscTrack {
                disc: track.disc()?,
                track: track.track()?,
            })
        })
        .collect()
}

fn resolve_disc_track_pairs_with_fallback<T: ReleaseCoalesceTrack>(
    tracks: &[T],
    fallback_disc: Option<u32>,
) -> Option<HashSet<DiscTrack>> {
    let mut resolved = HashSet::new();
    for track in tracks {
        let track_number = track.track()?;
        let disc_number = track.disc().or(fallback_disc)?;
        resolved.insert(DiscTrack {
            disc: disc_number,
            track: track_number,
        });
    }
    Some(resolved)
}

fn infer_disc_from_folder_name(source_dir: &str) -> Option<u32> {
    let name = Path::new(source_dir).file_name()?.to_string_lossy();
    let normalized = name.trim().to_ascii_lowercase();
    for prefix in ["disc", "cd", "disk"] {
        if let Some(suffix) = normalized.strip_prefix(prefix) {
            let suffix = suffix.trim_start_matches([' ', '-', '_']);
            if suffix.is_empty() || !suffix.chars().all(|ch| ch.is_ascii_digit()) {
                return None;
            }
            let parsed = suffix.parse::<u32>().ok()?;
            if parsed == 0 {
                return None;
            }
            return Some(parsed);
        }
    }

    None
}

fn representative_artist_key<T: ReleaseCoalesceTrack>(tracks: &[T]) -> Option<String> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    let mut observed_tracks = 0usize;
    for track in tracks {
        let names =
            if let Some(album_artists) = track.album_artists().filter(|value| !value.is_empty()) {
                album_artists
            } else if let Some(artists) = track.artists() {
                artists
            } else {
                continue;
            };

        let normalized_names: Vec<String> = names
            .iter()
            .map(|name| normalize_text_key(name))
            .filter(|name| !name.is_empty())
            .collect();
        if normalized_names.is_empty() {
            continue;
        }

        observed_tracks += 1;
        let key = normalized_names.join("\u{1f}");
        *counts.entry(key).or_insert(0) += 1;
    }

    let (best_key, best_count) =
        counts
            .into_iter()
            .max_by(|(left_key, left_count), (right_key, right_count)| {
                left_count
                    .cmp(right_count)
                    .then_with(|| right_key.cmp(left_key))
            })?;

    if best_count == observed_tracks || (best_count >= 2 && best_count * 2 > observed_tracks) {
        Some(best_key)
    } else {
        None
    }
}

fn representative_year<T: ReleaseCoalesceTrack>(tracks: &[T]) -> Option<u32> {
    let mut counts: HashMap<u32, usize> = HashMap::new();
    for track in tracks {
        let year = track.year()?;
        *counts.entry(year).or_insert(0) += 1;
    }

    counts
        .into_iter()
        .max_by(|(left_year, left_count), (right_year, right_count)| {
            left_count
                .cmp(right_count)
                .then_with(|| right_year.cmp(left_year))
        })
        .map(|(year, _)| year)
}

fn select_canonical_release_title<T: ReleaseCoalesceTrack>(
    bucket: &[ReleaseGroupCandidate<T>],
) -> Option<String> {
    #[derive(Debug)]
    struct Candidate {
        title: Option<String>,
        normalized_title: String,
        min_disc: Option<u32>,
        track_count: usize,
    }

    let mut candidates: Vec<Candidate> = bucket
        .iter()
        .map(|group| {
            let title = representative_release_title(&group.tracks);
            let normalized_title = title.as_deref().map(normalize_text_key).unwrap_or_default();
            let min_disc = minimum_disc_number(group);

            Candidate {
                title,
                normalized_title,
                min_disc,
                track_count: group.tracks.len(),
            }
        })
        .collect();

    if candidates.iter().any(|candidate| candidate.title.is_some()) {
        candidates.retain(|candidate| candidate.title.is_some());
    }

    candidates.sort_by(|left, right| {
        compare_optional_u32(left.min_disc, right.min_disc)
            .then_with(|| right.track_count.cmp(&left.track_count))
            .then_with(|| left.normalized_title.cmp(&right.normalized_title))
    });

    candidates.into_iter().find_map(|candidate| candidate.title)
}

fn representative_release_title<T: ReleaseCoalesceTrack>(tracks: &[T]) -> Option<String> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for track in tracks {
        let Some(album) = track.album() else {
            continue;
        };
        let normalized = normalize_unicode_nfc(album).trim().to_string();
        if normalized.is_empty() {
            continue;
        }
        *counts.entry(normalized).or_insert(0) += 1;
    }

    counts
        .into_iter()
        .max_by(|(left_title, left_count), (right_title, right_count)| {
            left_count
                .cmp(right_count)
                .then_with(|| right_title.cmp(left_title))
        })
        .map(|(title, _)| title)
}

fn minimum_disc_number<T: ReleaseCoalesceTrack>(group: &ReleaseGroupCandidate<T>) -> Option<u32> {
    group
        .tracks
        .iter()
        .filter_map(|track| track.disc())
        .min()
        .or(group.inferred_disc_from_dir)
}

fn compare_optional_u32(left: Option<u32>, right: Option<u32>) -> std::cmp::Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn compare_canonical_priority<T: ReleaseCoalesceTrack>(
    left: &ReleaseGroupCandidate<T>,
    right: &ReleaseGroupCandidate<T>,
) -> std::cmp::Ordering {
    compare_optional_u32(minimum_disc_number(left), minimum_disc_number(right))
        .then_with(|| right.tracks.len().cmp(&left.tracks.len()))
        .then_with(|| left.source_dir.cmp(&right.source_dir))
}

fn normalize_text_key(value: &str) -> String {
    normalize_unicode_nfc(value).trim().to_lowercase()
}

fn split_tracks_by_release_key<T: ReleaseCoalesceTrack>(
    tracks: Vec<T>,
) -> BTreeMap<String, Vec<T>> {
    let mut by_release_key: BTreeMap<String, Vec<T>> = BTreeMap::new();
    for track in tracks {
        let release_key = normalize_unicode_nfc(track.album().unwrap_or(""));
        by_release_key.entry(release_key).or_default().push(track);
    }
    by_release_key
}

fn has_overlap<T: Eq + std::hash::Hash>(left: &HashSet<T>, right: &HashSet<T>) -> bool {
    if left.len() <= right.len() {
        left.iter().any(|value| right.contains(value))
    } else {
        right.iter().any(|value| left.contains(value))
    }
}
