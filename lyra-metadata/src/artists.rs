// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use unicode_normalization::UnicodeNormalization;

pub fn normalize_unicode_nfc(value: &str) -> String {
    value.nfc().collect()
}

pub fn normalize_unicode_nfkc(value: &str) -> String {
    value.nfkc().collect()
}

/// Split a single part on a case-insensitive word separator (e.g. " and ", " feat.").
pub fn split_on_word(s: &str, sep: &str) -> Vec<String> {
    let lower = s.to_ascii_lowercase();
    let mut parts = Vec::new();
    let mut start = 0;
    while let Some(pos) = lower[start..].find(sep) {
        let before = s[start..start + pos].trim();
        if !before.is_empty() {
            parts.push(before.to_string());
        }
        start += pos + sep.len();
    }
    let tail = s[start..].trim();
    if !tail.is_empty() {
        parts.push(tail.to_string());
    }
    parts
}

/// Split artist strings using only `feat.` as a delimiter.
pub fn split_artist_string(items: Vec<String>) -> Vec<String> {
    if items.len() != 1 {
        return items;
    }
    let s = &items[0];
    let result = split_on_word(s, " feat.");
    if result.len() > 1 { result } else { items }
}

/// Split delimited strings if there's only one entry with separators.
pub fn split_delimited_string(items: Vec<String>) -> Vec<String> {
    if items.len() != 1 {
        return items;
    }
    let s = &items[0];
    let mut result = Vec::new();
    // Split on delimiters only at parenthesis depth 0
    let mut depth = 0u32;
    let mut start = 0;
    for (i, ch) in s.char_indices() {
        match ch {
            '(' | '\u{ff08}' => depth += 1,
            ')' | '\u{ff09}' => depth = depth.saturating_sub(1),
            ',' | '&' | '\u{3001}' if depth == 0 => {
                let part = s[start..i].trim();
                if !part.is_empty() {
                    for sub in split_on_word(part, " and ") {
                        result.extend(split_on_word(&sub, " feat."));
                    }
                }
                start = i + ch.len_utf8();
            }
            _ => {}
        }
    }
    let tail = s[start..].trim();
    if !tail.is_empty() {
        for sub in split_on_word(tail, " and ") {
            result.extend(split_on_word(&sub, " feat."));
        }
    }
    if result.len() > 1 {
        result
    } else {
        vec![s.clone()]
    }
}

fn is_feature_marker_boundary(title: &str, index: usize) -> bool {
    if index == 0 {
        return true;
    }

    title[..index].chars().next_back().is_some_and(|ch| {
        ch.is_whitespace()
            || matches!(
                ch,
                '(' | '\u{ff08}' | '[' | '\u{3010}' | '-' | '\u{2013}' | '\u{2014}' | '/' | '|'
            )
    })
}

fn find_feature_marker_range(title: &str) -> Option<(usize, usize)> {
    const MARKERS: [&str; 5] = ["featuring ", "feat. ", "feat ", "ft. ", "ft "];
    let lower = title.to_lowercase();
    let mut best: Option<(usize, usize)> = None;

    for marker in MARKERS {
        let mut offset = 0usize;
        while let Some(rel_pos) = lower[offset..].find(marker) {
            let start = offset + rel_pos;
            if is_feature_marker_boundary(&lower, start) {
                let end = start + marker.len();
                if best.is_none_or(|(best_start, _)| start < best_start) {
                    best = Some((start, end));
                }
                break;
            }
            offset = start + 1;
        }
    }

    best
}

pub(crate) fn extract_featured_artists_from_title(title: &str) -> Vec<String> {
    let Some((start, marker_end)) = find_feature_marker_range(title) else {
        return Vec::new();
    };

    let prefix = &title[..start];
    let mut guest_segment = title[marker_end..]
        .trim_start_matches(|ch: char| {
            ch.is_whitespace() || matches!(ch, ':' | '-' | '\u{2013}' | '\u{2014}')
        })
        .to_string();

    if prefix
        .chars()
        .next_back()
        .is_some_and(|ch| matches!(ch, '(' | '\u{ff08}' | '[' | '\u{3010}'))
    {
        let mut close_idx: Option<usize> = None;
        for closer in [')', '\u{ff09}', ']', '\u{3011}'] {
            if let Some(idx) = guest_segment.find(closer) {
                close_idx = Some(close_idx.map_or(idx, |current| current.min(idx)));
            }
        }
        if let Some(idx) = close_idx {
            guest_segment.truncate(idx);
        }
    }

    guest_segment = guest_segment
        .trim()
        .trim_end_matches(|ch: char| {
            matches!(
                ch,
                ' ' | '\t' | '\n' | '\r' | ')' | '\u{ff09}' | ']' | '\u{3011}' | '.' | '!' | '?'
            )
        })
        .trim()
        .to_string();

    if guest_segment.is_empty() {
        return Vec::new();
    }

    split_delimited_string(vec![guest_segment])
        .into_iter()
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .collect()
}

fn contains_artist_case_insensitive(artists: &[String], candidate: &str) -> bool {
    let candidate_lower = candidate.to_lowercase();
    artists
        .iter()
        .any(|artist| artist == candidate || artist.to_lowercase() == candidate_lower)
}

pub(crate) fn enrich_artists_with_title_features(artists: &mut Vec<String>, title: Option<&str>) {
    if artists.len() != 1 {
        return;
    }
    let Some(title) = title else {
        return;
    };

    for featured in extract_featured_artists_from_title(title) {
        if !contains_artist_case_insensitive(artists, &featured) {
            artists.push(featured);
        }
    }
}
