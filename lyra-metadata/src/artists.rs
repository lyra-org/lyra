// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use unicode_normalization::UnicodeNormalization;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtistRelationKind {
    VoiceActor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ParsedArtistType {
    Person,
    Character,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ArtistRelationMetadata {
    pub source_artist: String,
    pub target_artist: String,
    pub relation_type: ArtistRelationKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_artist_type: Option<ParsedArtistType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_artist_type: Option<ParsedArtistType>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParsedArtistCredits {
    pub artists: Vec<String>,
    pub relations: Vec<ArtistRelationMetadata>,
}

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

fn is_open_paren(ch: char) -> bool {
    matches!(ch, '(' | '\u{ff08}')
}

fn is_close_paren(ch: char) -> bool {
    matches!(ch, ')' | '\u{ff09}')
}

fn find_trailing_parenthetical(value: &str) -> Option<(usize, usize)> {
    let trimmed = value.trim();
    let (close_idx, close_ch) = trimmed.char_indices().next_back()?;
    if !is_close_paren(close_ch) {
        return None;
    }

    let mut depth = 0u32;
    for (idx, ch) in trimmed.char_indices().rev() {
        if is_close_paren(ch) {
            depth += 1;
        } else if is_open_paren(ch) {
            depth = depth.saturating_sub(1);
            if depth == 0 {
                return Some((idx, close_idx));
            }
        }
    }

    None
}

fn strip_cv_marker(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    let mut chars = trimmed.char_indices();
    let (_, first) = chars.next()?;
    let (second_idx, second) = chars.next()?;
    if !first.eq_ignore_ascii_case(&'c') || !second.eq_ignore_ascii_case(&'v') {
        return None;
    }
    let marker_end = second_idx + second.len_utf8();

    let rest = trimmed[marker_end..]
        .trim_start()
        .trim_start_matches(|ch: char| matches!(ch, ':' | '\u{ff1a}' | '.' | '\u{ff0e}'))
        .trim_start();
    (!rest.is_empty()).then_some(rest)
}

fn add_unique_artist(artists: &mut Vec<String>, name: String) {
    if !artists.iter().any(|artist| artist == &name) {
        artists.push(name);
    }
}

fn add_unique_relation(
    relations: &mut Vec<ArtistRelationMetadata>,
    relation: ArtistRelationMetadata,
) {
    if !relations.iter().any(|existing| existing == &relation) {
        relations.push(relation);
    }
}

fn parse_cv_artist_credit(value: &str) -> Option<ParsedArtistCredits> {
    let trimmed = value.trim();
    let (open_idx, close_idx) = find_trailing_parenthetical(trimmed)?;
    let subject = trimmed[..open_idx].trim();
    let open_len = trimmed[open_idx..]
        .chars()
        .next()
        .map(char::len_utf8)
        .unwrap_or(1);
    let cv_segment = &trimmed[open_idx + open_len..close_idx];
    let voice_actors = strip_cv_marker(cv_segment)?;
    if subject.is_empty() {
        return None;
    }

    let primary_artists = split_delimited_string(vec![subject.to_string()]);
    let voice_actor_artists = split_delimited_string(vec![voice_actors.to_string()]);
    if primary_artists.is_empty() || voice_actor_artists.is_empty() {
        return None;
    }

    let mut parsed = ParsedArtistCredits {
        artists: primary_artists.clone(),
        relations: Vec::new(),
    };

    if primary_artists.len() == voice_actor_artists.len() {
        for (target_artist, source_artist) in primary_artists.iter().zip(voice_actor_artists.iter())
        {
            add_unique_relation(
                &mut parsed.relations,
                ArtistRelationMetadata {
                    source_artist: source_artist.clone(),
                    target_artist: target_artist.clone(),
                    relation_type: ArtistRelationKind::VoiceActor,
                    source_artist_type: Some(ParsedArtistType::Person),
                    target_artist_type: Some(ParsedArtistType::Character),
                },
            );
        }
    } else if voice_actor_artists.len() == 1 {
        for target_artist in &primary_artists {
            add_unique_relation(
                &mut parsed.relations,
                ArtistRelationMetadata {
                    source_artist: voice_actor_artists[0].clone(),
                    target_artist: target_artist.clone(),
                    relation_type: ArtistRelationKind::VoiceActor,
                    source_artist_type: Some(ParsedArtistType::Person),
                    target_artist_type: Some(ParsedArtistType::Character),
                },
            );
        }
    } else if primary_artists.len() == 1 {
        for source_artist in &voice_actor_artists {
            add_unique_relation(
                &mut parsed.relations,
                ArtistRelationMetadata {
                    source_artist: source_artist.clone(),
                    target_artist: primary_artists[0].clone(),
                    relation_type: ArtistRelationKind::VoiceActor,
                    source_artist_type: Some(ParsedArtistType::Person),
                    target_artist_type: None,
                },
            );
        }
    }

    Some(parsed)
}

pub fn parse_cv_artist_credits(items: Vec<String>) -> ParsedArtistCredits {
    let mut parsed = ParsedArtistCredits::default();

    for item in items {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Some(cv_credit) = parse_cv_artist_credit(trimmed) {
            for artist in cv_credit.artists {
                add_unique_artist(&mut parsed.artists, artist);
            }
            for relation in cv_credit.relations {
                add_unique_relation(&mut parsed.relations, relation);
            }
        } else {
            add_unique_artist(&mut parsed.artists, trimmed.to_string());
        }
    }

    parsed
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
