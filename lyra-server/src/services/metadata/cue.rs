// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        HashMap,
        HashSet,
    },
    path::{
        Path,
        PathBuf,
    },
};

use agdb::DbId;
use anyhow::{
    Context,
    anyhow,
};
use lofty::file::AudioFile;
use lyra_metadata::{
    normalize_unicode_nfc,
    normalize_unicode_nfkc,
};

use super::model::TrackMetadata;
use crate::db::Entry;

const SOURCE_KIND_CUE: &str = "cue";

#[derive(Debug, Clone)]
pub(super) struct ParsedCueTrack {
    pub(super) track_no: u32,
    pub(super) file_ref: String,
    pub(super) title: Option<String>,
    pub(super) performer: Option<String>,
    pub(super) index00_frames: Option<u32>,
    pub(super) index01_frames: Option<u32>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct ParsedCueSheet {
    pub(super) album_title: Option<String>,
    pub(super) album_performer: Option<String>,
    pub(super) disc_number: Option<u32>,
    pub(super) tracks: Vec<ParsedCueTrack>,
}

#[derive(Debug, Clone)]
pub(super) struct AudioEntryRef {
    pub(super) entry_id: DbId,
    pub(super) file_path: PathBuf,
}

pub(super) struct AudioLookup {
    pub(super) by_path: HashMap<String, AudioEntryRef>,
    pub(super) by_name: HashMap<String, AudioEntryRef>,
    pub(super) by_stem: HashMap<String, Vec<AudioEntryRef>>,
}

#[derive(Debug, Clone)]
pub(super) struct ResolvedCueTrack {
    pub(super) track_no: u32,
    pub(super) title: Option<String>,
    pub(super) performer: Option<String>,
    pub(super) audio_entry: AudioEntryRef,
    pub(super) index00_frames: Option<u32>,
    pub(super) index01_frames: u32,
}

pub(super) fn normalize_lookup_key(value: &str) -> String {
    let compat = normalize_unicode_nfkc(value);
    compat
        .chars()
        .map(|ch| match ch {
            '\\' | '\u{2044}' | '\u{2215}' | '\u{29f8}' => '/',
            _ => ch,
        })
        .collect::<String>()
        .to_lowercase()
}

fn normalize_path_for_lookup(path: &Path) -> String {
    let path = path.to_string_lossy().replace('\\', "/");
    normalize_lookup_key(&path)
}

fn parse_cue_value(raw: &str) -> Option<String> {
    let value = raw.trim();
    if value.is_empty() {
        return None;
    }

    if let Some(rest) = value.strip_prefix('"') {
        if let Some(end) = rest.find('"') {
            return Some(normalize_unicode_nfc(&rest[..end]));
        }
        return Some(normalize_unicode_nfc(rest));
    }

    Some(normalize_unicode_nfc(value))
}

fn parse_cue_file_ref(raw: &str) -> Option<String> {
    let value = raw.trim();
    if value.is_empty() {
        return None;
    }

    if let Some(rest) = value.strip_prefix('"') {
        if let Some(end) = rest.find('"') {
            return Some(normalize_unicode_nfc(&rest[..end]));
        }
        return Some(normalize_unicode_nfc(rest));
    }

    value.split_whitespace().next().map(normalize_unicode_nfc)
}

fn parse_positive_u32(value: &str) -> Option<u32> {
    let number = value.parse::<u32>().ok()?;
    (number > 0).then_some(number)
}

fn parse_disc_number_suffix(token: &str) -> Option<u32> {
    for prefix in ["disc", "cd", "disk"] {
        if let Some(rest) = token.strip_prefix(prefix) {
            if rest.is_empty() {
                return None;
            }
            if rest.chars().all(|ch| ch.is_ascii_digit()) {
                return parse_positive_u32(rest);
            }
        }
    }
    None
}

fn infer_disc_from_designator(value: &str) -> Option<u32> {
    let normalized = normalize_lookup_key(value);
    let tokens: Vec<&str> = normalized
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .collect();
    if tokens.is_empty() {
        return None;
    }

    let last = tokens[tokens.len() - 1];
    if let Some(disc) = parse_disc_number_suffix(last) {
        return Some(disc);
    }

    if tokens.len() < 2 {
        return None;
    }
    let prev = tokens[tokens.len() - 2];
    if matches!(prev, "disc" | "cd" | "disk") {
        return parse_positive_u32(last);
    }

    None
}

fn parse_disc_number_from_cue_value(raw: &str) -> Option<u32> {
    let normalized = normalize_lookup_key(&parse_cue_value(raw)?);
    normalized
        .split(|ch: char| !ch.is_ascii_digit())
        .find(|token| !token.is_empty())
        .and_then(parse_positive_u32)
}

fn parse_cue_disc_number_line(line: &str) -> Option<u32> {
    let mut candidate = line.trim();
    if candidate
        .get(..4)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("REM "))
    {
        candidate = candidate.get(4..)?.trim_start();
    }

    let upper = candidate.to_ascii_uppercase();
    for key in ["DISCNUMBER", "DISCNO", "DISC"] {
        let Some(rest) = candidate.get(key.len()..) else {
            continue;
        };
        if !upper.starts_with(key) {
            continue;
        }
        let Some(first) = rest.chars().next() else {
            continue;
        };
        if !(first.is_whitespace() || matches!(first, '=' | ':')) {
            continue;
        }
        let value =
            rest.trim_start_matches(|ch: char| ch.is_whitespace() || matches!(ch, '=' | ':'));
        return parse_disc_number_from_cue_value(value);
    }
    None
}

pub(super) fn infer_cue_disc_number(cue_path: &Path, explicit_disc: Option<u32>) -> Option<u32> {
    let parent_disc = cue_path
        .parent()
        .and_then(|parent| parent.file_name())
        .and_then(|value| value.to_str())
        .and_then(infer_disc_from_designator);
    let stem_disc = cue_path
        .file_stem()
        .and_then(|value| value.to_str())
        .and_then(infer_disc_from_designator);

    let mut resolved = None;
    for disc in [explicit_disc, parent_disc, stem_disc]
        .into_iter()
        .flatten()
    {
        if disc == 0 {
            continue;
        }
        match resolved {
            None => resolved = Some(disc),
            Some(existing) if existing == disc => {}
            Some(_) => return None,
        }
    }
    resolved
}

pub(super) fn parse_cue_timestamp_to_frames(raw: &str) -> Option<u32> {
    let mut parts = raw.trim().split(':');
    let mm = parts.next()?.parse::<u32>().ok()?;
    let ss = parts.next()?.parse::<u32>().ok()?;
    let ff = parts.next()?.parse::<u32>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some(mm.saturating_mul(60 * 75) + ss.saturating_mul(75) + ff)
}

pub(super) fn frames_to_ms(frames: u32) -> u64 {
    ((frames as u64) * 1000) / 75
}

pub(super) fn parse_cue_sheet(text: &str) -> ParsedCueSheet {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let mut parsed = ParsedCueSheet::default();
    let mut current_file_ref: Option<String> = None;
    let mut current_track: Option<ParsedCueTrack> = None;

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        if parsed.disc_number.is_none() {
            parsed.disc_number = parse_cue_disc_number_line(line);
            if parsed.disc_number.is_some() {
                continue;
            }
        }

        if let Some(rest) = line.strip_prefix("FILE ") {
            let next_file_ref = parse_cue_file_ref(rest);
            if let Some(track) = current_track.as_mut()
                && track.index01_frames.is_none()
            {
                if let Some(file_ref) = next_file_ref.as_ref() {
                    track.file_ref = file_ref.clone();
                }
                current_file_ref = next_file_ref;
                continue;
            }
            if let Some(track) = current_track.take() {
                parsed.tracks.push(track);
            }
            current_file_ref = next_file_ref;
            continue;
        }

        if let Some(rest) = line.strip_prefix("TRACK ") {
            if let Some(track) = current_track.take() {
                parsed.tracks.push(track);
            }

            let mut parts = rest.split_whitespace();
            let Some(track_no) = parts.next().and_then(|value| value.parse::<u32>().ok()) else {
                current_track = None;
                continue;
            };

            current_track = Some(ParsedCueTrack {
                track_no,
                file_ref: current_file_ref.clone().unwrap_or_default(),
                title: None,
                performer: None,
                index00_frames: None,
                index01_frames: None,
            });
            continue;
        }

        if let Some(rest) = line.strip_prefix("TITLE ") {
            if let Some(track) = current_track.as_mut() {
                track.title = parse_cue_value(rest);
            } else {
                parsed.album_title = parse_cue_value(rest);
            }
            continue;
        }

        if let Some(rest) = line.strip_prefix("PERFORMER ") {
            if let Some(track) = current_track.as_mut() {
                track.performer = parse_cue_value(rest);
            } else {
                parsed.album_performer = parse_cue_value(rest);
            }
            continue;
        }

        if let Some(rest) = line.strip_prefix("INDEX ") {
            let Some(track) = current_track.as_mut() else {
                continue;
            };
            let mut parts = rest.split_whitespace();
            let Some(index_no) = parts.next().and_then(|value| value.parse::<u32>().ok()) else {
                continue;
            };
            let Some(frames) = parts.next().and_then(parse_cue_timestamp_to_frames) else {
                continue;
            };

            match index_no {
                0 => track.index00_frames = Some(frames),
                1 => track.index01_frames = Some(frames),
                _ => {}
            }
            continue;
        }
    }

    if let Some(track) = current_track.take() {
        parsed.tracks.push(track);
    }

    parsed
}

pub(super) fn build_audio_lookup(audio_entries: &[Entry]) -> AudioLookup {
    let mut by_path = HashMap::new();
    let mut by_name = HashMap::new();
    let mut by_stem = HashMap::new();

    for entry in audio_entries {
        let Some(entry_id) = entry.db_id else {
            continue;
        };

        let audio_ref = AudioEntryRef {
            entry_id,
            file_path: entry.full_path.clone(),
        };

        by_path.insert(
            normalize_path_for_lookup(&entry.full_path),
            audio_ref.clone(),
        );

        if let Some(name) = entry
            .full_path
            .file_name()
            .and_then(|value| value.to_str())
            .map(normalize_lookup_key)
        {
            by_name.entry(name).or_insert(audio_ref.clone());
        }

        if let Some(stem) = entry
            .full_path
            .file_stem()
            .and_then(|value| value.to_str())
            .map(normalize_lookup_key)
        {
            by_stem
                .entry(stem)
                .or_insert_with(Vec::new)
                .push(audio_ref.clone());
        }
    }

    AudioLookup {
        by_path,
        by_name,
        by_stem,
    }
}

pub(super) async fn parse_cue_metadata_for_entry(
    cue_entry: &Entry,
    audio_by_path: &HashMap<String, AudioEntryRef>,
    audio_by_name: &HashMap<String, AudioEntryRef>,
    audio_by_stem: &HashMap<String, Vec<AudioEntryRef>>,
) -> anyhow::Result<(Vec<TrackMetadata>, HashSet<DbId>)> {
    let cue_entry_id = cue_entry
        .db_id
        .ok_or_else(|| anyhow!("cue entry missing db_id for {:?}", cue_entry.full_path))?;
    let cue_text = tokio::fs::read_to_string(&cue_entry.full_path)
        .await
        .with_context(|| format!("failed to read cue file: {}", cue_entry.full_path.display()))?;

    let parsed_sheet = parse_cue_sheet(&cue_text);
    let source_hash = cue_entry
        .hash
        .clone()
        .unwrap_or_else(|| format!("{:032x}", xxh3::hash128_with_seed(cue_text.as_bytes(), 0)));

    let album_title = parsed_sheet.album_title.clone().or_else(|| {
        cue_entry
            .full_path
            .file_stem()
            .and_then(|value| value.to_str())
            .map(normalize_unicode_nfc)
    });
    let album_artists = parsed_sheet
        .album_performer
        .clone()
        .map(|value| vec![value]);
    let inferred_disc = infer_cue_disc_number(&cue_entry.full_path, parsed_sheet.disc_number);

    let mut resolved_tracks = Vec::new();
    let mut missing_index01_tracks = Vec::new();
    let mut unresolved_audio_tracks = Vec::new();
    for track in parsed_sheet.tracks {
        let Some(index01_frames) = track.index01_frames else {
            missing_index01_tracks.push(track.track_no);
            tracing::debug!(
                path = %cue_entry.full_path.display(),
                track_no = track.track_no,
                "skipping cue track without INDEX 01"
            );
            continue;
        };

        let Some(audio_entry) = resolve_audio_entry_for_cue_track(
            &cue_entry.full_path,
            &track.file_ref,
            audio_by_path,
            audio_by_name,
            audio_by_stem,
        ) else {
            unresolved_audio_tracks.push((track.track_no, track.file_ref.clone()));
            tracing::debug!(
                path = %cue_entry.full_path.display(),
                track_no = track.track_no,
                file_ref = %track.file_ref,
                "skipping cue track because referenced audio file was not found in scanned entries"
            );
            continue;
        };

        resolved_tracks.push(ResolvedCueTrack {
            track_no: track.track_no,
            title: track.title,
            performer: track.performer,
            audio_entry,
            index00_frames: track.index00_frames,
            index01_frames,
        });
    }

    if !missing_index01_tracks.is_empty() {
        tracing::warn!(
            path = %cue_entry.full_path.display(),
            skipped_count = missing_index01_tracks.len(),
            "skipped cue tracks without INDEX 01"
        );
    }
    if !unresolved_audio_tracks.is_empty() {
        tracing::warn!(
            path = %cue_entry.full_path.display(),
            skipped_count = unresolved_audio_tracks.len(),
            "skipped cue tracks because referenced audio files were not found in scanned entries"
        );
    }

    if resolved_tracks.is_empty() {
        return Ok((Vec::new(), HashSet::new()));
    }

    let track_total = resolved_tracks.len() as u32;
    let mut claimed_audio_entries = HashSet::new();
    let mut metadata = Vec::with_capacity(resolved_tracks.len());

    let mut audio_duration_ms: HashMap<DbId, u64> = HashMap::new();
    {
        let mut probed = HashSet::new();
        for track in &resolved_tracks {
            if !probed.insert(track.audio_entry.entry_id) {
                continue;
            }
            let path = track.audio_entry.file_path.clone();
            if let Ok(Some(duration)) =
                tokio::task::spawn_blocking(move || probe_audio_duration_ms(&path)).await
            {
                audio_duration_ms.insert(track.audio_entry.entry_id, duration);
            }
        }
    }

    for (index, track) in resolved_tracks.iter().enumerate() {
        claimed_audio_entries.insert(track.audio_entry.entry_id);

        let start_ms = frames_to_ms(track.index01_frames);
        let next_start_frames = resolved_tracks
            .iter()
            .skip(index + 1)
            .find(|candidate| candidate.audio_entry.entry_id == track.audio_entry.entry_id)
            .map(|candidate| candidate.index01_frames);
        let end_ms = next_start_frames
            .map(frames_to_ms)
            .or_else(|| audio_duration_ms.get(&track.audio_entry.entry_id).copied());
        let duration_ms = end_ms.map(|end| end.saturating_sub(start_ms));

        let track_title = track
            .title
            .clone()
            .or_else(|| Some(format!("Track {:02}", track.track_no)));
        let performer = track
            .performer
            .clone()
            .or_else(|| parsed_sheet.album_performer.clone());

        metadata.push(TrackMetadata {
            entry_db_id: track.audio_entry.entry_id,
            album: album_title.clone(),
            album_artists: album_artists.clone(),
            date: None,
            year: None,
            title: track_title,
            artists: performer.map(|value| vec![value]),
            disc: inferred_disc,
            disc_total: None,
            track: Some(track.track_no),
            track_total: Some(track_total),
            duration_ms,
            genres: None,
            label: None,
            catalog_number: None,
            source_kind: Some(SOURCE_KIND_CUE.to_string()),
            source_key: Some(build_cue_source_key(cue_entry_id, track.track_no)),
            segment_start_ms: Some(start_ms),
            segment_end_ms: end_ms,
            cue_sheet_entry_id: Some(cue_entry_id),
            cue_sheet_hash: Some(source_hash.clone()),
            cue_track_no: Some(track.track_no),
            cue_audio_entry_id: Some(track.audio_entry.entry_id),
            cue_index00_frames: track.index00_frames,
            cue_index01_frames: Some(track.index01_frames),
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
        });
    }

    Ok((metadata, claimed_audio_entries))
}

fn probe_audio_duration_ms(path: &Path) -> Option<u64> {
    let tagged_file = lofty::probe::Probe::open(path).ok()?.read().ok()?;
    Some(tagged_file.properties().duration().as_millis() as u64)
}

pub(super) fn resolve_audio_entry_for_cue_track(
    cue_path: &Path,
    file_ref: &str,
    audio_by_path: &HashMap<String, AudioEntryRef>,
    audio_by_name: &HashMap<String, AudioEntryRef>,
    audio_by_stem: &HashMap<String, Vec<AudioEntryRef>>,
) -> Option<AudioEntryRef> {
    let normalized_ref = file_ref.replace('\\', "/");
    let candidate = PathBuf::from(&normalized_ref);
    let resolved = if candidate.is_absolute() {
        candidate
    } else {
        cue_path
            .parent()
            .unwrap_or_else(|| Path::new(""))
            .join(candidate)
    };

    let lookup_key = normalize_path_for_lookup(&resolved);
    if let Some(audio_entry) = audio_by_path.get(&lookup_key) {
        return Some(audio_entry.clone());
    }

    let name_key = resolved
        .file_name()
        .and_then(|value| value.to_str())
        .map(normalize_lookup_key)?;
    if let Some(audio_entry) = audio_by_name.get(&name_key) {
        return Some(audio_entry.clone());
    }

    let stem_key = resolved
        .file_stem()
        .and_then(|value| value.to_str())
        .map(normalize_lookup_key)?;
    if let Some(candidates) = audio_by_stem.get(&stem_key) {
        if candidates.len() == 1 {
            return candidates.first().cloned();
        }
    }

    // Keep a literal fallback for malformed CUE refs that include slash-like
    // characters inside filenames (e.g. "w/o"), which Path parsing treats as
    // separators.
    let raw_name_key = normalize_lookup_key(&normalized_ref);
    if let Some(audio_entry) = audio_by_name.get(&raw_name_key) {
        return Some(audio_entry.clone());
    }

    let raw_stem_key = normalized_ref
        .rsplit_once('.')
        .map(|(stem, _)| stem)
        .unwrap_or(normalized_ref.as_str());
    match audio_by_stem.get(&normalize_lookup_key(raw_stem_key)) {
        Some(candidates) if candidates.len() == 1 => candidates.first().cloned(),
        _ => None,
    }
}

pub(super) fn build_embedded_source_key(entry_id: DbId) -> String {
    format!("entry:{}:embedded", entry_id.0)
}

pub(super) fn build_cue_source_key(cue_entry_id: DbId, track_no: u32) -> String {
    format!("cue:{}:track:{}", cue_entry_id.0, track_no)
}

pub(super) fn merge_embedded_into_cue_metadata(
    cue: &mut TrackMetadata,
    embedded: &TrackMetadata,
    preserve_cue_track_identity: bool,
) {
    /// Overwrites `dst` with `src` when `src` is `Some` (and non-empty for `Vec` values).
    fn merge_opt<T: Clone>(dst: &mut Option<T>, src: &Option<T>) {
        if src.is_some() {
            *dst = src.clone();
        }
    }

    fn merge_opt_vec<T: Clone>(dst: &mut Option<Vec<T>>, src: &Option<Vec<T>>) {
        if src.as_ref().is_some_and(|v| !v.is_empty()) {
            *dst = src.clone();
        }
    }

    merge_opt(&mut cue.album, &embedded.album);
    merge_opt_vec(&mut cue.album_artists, &embedded.album_artists);
    merge_opt(&mut cue.date, &embedded.date);
    merge_opt(&mut cue.year, &embedded.year);
    merge_opt_vec(&mut cue.genres, &embedded.genres);
    merge_opt(&mut cue.sample_rate_hz, &embedded.sample_rate_hz);
    merge_opt(&mut cue.channel_count, &embedded.channel_count);
    merge_opt(&mut cue.bit_depth, &embedded.bit_depth);
    merge_opt(&mut cue.bitrate_bps, &embedded.bitrate_bps);

    if !preserve_cue_track_identity {
        merge_opt(&mut cue.title, &embedded.title);
        merge_opt_vec(&mut cue.artists, &embedded.artists);
    }
}

pub(super) fn sort_track_metadata(metadata: &mut [TrackMetadata]) {
    metadata.sort_by(|a, b| {
        let artist_key = |t: &TrackMetadata| {
            normalize_lookup_key(
                &t.album_artists
                    .as_ref()
                    .map(|value| value.join(", "))
                    .unwrap_or_default(),
            )
        };

        artist_key(a)
            .cmp(&artist_key(b))
            .then_with(|| a.year.unwrap_or(0).cmp(&b.year.unwrap_or(0)))
            .then_with(|| {
                let a_album = normalize_lookup_key(a.album.as_deref().unwrap_or(""));
                let b_album = normalize_lookup_key(b.album.as_deref().unwrap_or(""));
                a_album.cmp(&b_album)
            })
            .then_with(|| a.disc.unwrap_or(1).cmp(&b.disc.unwrap_or(1)))
            .then_with(|| a.track.unwrap_or(0).cmp(&b.track.unwrap_or(0)))
            .then_with(|| {
                let a_title = normalize_lookup_key(a.title.as_deref().unwrap_or(""));
                let b_title = normalize_lookup_key(b.title.as_deref().unwrap_or(""));
                a_title.cmp(&b_title)
            })
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_track(entry_id: i64) -> super::TrackMetadata {
        super::TrackMetadata {
            entry_db_id: DbId(entry_id),
            album: None,
            album_artists: None,
            date: None,
            year: None,
            title: None,
            artists: None,
            disc: None,
            disc_total: None,
            track: None,
            track_total: None,
            duration_ms: None,
            genres: None,
            label: None,
            catalog_number: None,
            source_kind: None,
            source_key: None,
            segment_start_ms: None,
            segment_end_ms: None,
            cue_sheet_entry_id: None,
            cue_sheet_hash: None,
            cue_track_no: None,
            cue_audio_entry_id: None,
            cue_index00_frames: None,
            cue_index01_frames: None,
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
        }
    }

    #[test]
    fn merge_embedded_into_cue_prefers_embedded_descriptive_fields() {
        let mut cue = test_track(1);
        cue.album = Some("TV........................".to_string());
        cue.album_artists = Some(vec!["....".to_string()]);
        cue.title = Some("Cue Track".to_string());
        cue.artists = Some(vec!["Cue Performer".to_string()]);
        cue.disc = Some(2);
        cue.track = Some(32);
        cue.track_total = Some(32);

        let mut embedded = test_track(1);
        embedded.album = Some("TVアニメ「【推しの子】」オリジナルサウンドトラック".to_string());
        embedded.album_artists = Some(vec!["伊賀拓郎".to_string()]);
        embedded.date = Some("2023".to_string());
        embedded.year = Some(2023);
        embedded.title = Some("True Friends (w／o Gt.Solo)".to_string());
        embedded.artists = Some(vec!["Kengo Morimoto".to_string()]);
        embedded.genres = Some(vec!["Soundtrack".to_string()]);
        embedded.disc_total = Some(1);

        merge_embedded_into_cue_metadata(&mut cue, &embedded, false);

        assert_eq!(
            cue.album.as_deref(),
            Some("TVアニメ「【推しの子】」オリジナルサウンドトラック")
        );
        assert_eq!(cue.album_artists, Some(vec!["伊賀拓郎".to_string()]));
        assert_eq!(cue.date.as_deref(), Some("2023"));
        assert_eq!(cue.year, Some(2023));
        assert_eq!(cue.title.as_deref(), Some("True Friends (w／o Gt.Solo)"));
        assert_eq!(cue.artists, Some(vec!["Kengo Morimoto".to_string()]));
        assert_eq!(cue.genres, Some(vec!["Soundtrack".to_string()]));
        // Structural CUE fields stay authoritative.
        assert_eq!(cue.disc, Some(2));
        assert_eq!(cue.track, Some(32));
        assert_eq!(cue.track_total, Some(32));
        assert_eq!(cue.disc_total, None);
    }

    #[test]
    fn merge_embedded_into_cue_keeps_cue_fields_when_embedded_missing() {
        let mut cue = test_track(1);
        cue.album = Some("Cue Album".to_string());
        cue.title = Some("Cue Title".to_string());
        cue.artists = Some(vec!["Cue Artist".to_string()]);
        cue.year = Some(2024);

        let embedded = test_track(1);
        merge_embedded_into_cue_metadata(&mut cue, &embedded, false);

        assert_eq!(cue.album.as_deref(), Some("Cue Album"));
        assert_eq!(cue.title.as_deref(), Some("Cue Title"));
        assert_eq!(cue.artists, Some(vec!["Cue Artist".to_string()]));
        assert_eq!(cue.year, Some(2024));
    }

    #[test]
    fn merge_embedded_into_cue_preserves_cue_track_identity_for_single_file_cues() {
        let mut cue = test_track(1);
        cue.album = Some("Cue Album".to_string());
        cue.title = Some("Cue Track".to_string());
        cue.artists = Some(vec!["Cue Performer".to_string()]);

        let mut embedded = test_track(1);
        embedded.album = Some("Embedded Album".to_string());
        embedded.date = Some("2023".to_string());
        embedded.year = Some(2023);
        embedded.title = Some("Container Title".to_string());
        embedded.artists = Some(vec!["Container Artist".to_string()]);
        embedded.genres = Some(vec!["Soundtrack".to_string()]);

        merge_embedded_into_cue_metadata(&mut cue, &embedded, true);

        assert_eq!(cue.album.as_deref(), Some("Embedded Album"));
        assert_eq!(cue.date.as_deref(), Some("2023"));
        assert_eq!(cue.year, Some(2023));
        assert_eq!(cue.genres, Some(vec!["Soundtrack".to_string()]));
        assert_eq!(cue.title.as_deref(), Some("Cue Track"));
        assert_eq!(cue.artists, Some(vec!["Cue Performer".to_string()]));
    }

    #[test]
    fn normalize_lookup_key_folds_compatibility_slash_variants() {
        assert_eq!(
            normalize_lookup_key("32. True Friends (w/o Gt.Solo).wav"),
            normalize_lookup_key("32. True Friends (w／o Gt.Solo).flac").replace(".flac", ".wav")
        );
    }

    #[test]
    fn parse_cue_sheet_reads_explicit_disc_number() {
        let cue = r#"
REM DISCNUMBER 2
FILE "01 intro.wav" WAVE
  TRACK 01 AUDIO
    INDEX 01 00:00:00
"#;

        let parsed = parse_cue_sheet(cue);
        assert_eq!(parsed.disc_number, Some(2));
    }

    #[test]
    fn infer_cue_disc_number_rejects_conflicting_signals() {
        let inferred = infer_cue_disc_number(Path::new("/music/Disc 1/Album Disc 2.cue"), None);
        assert_eq!(inferred, None);
    }

    #[test]
    fn infer_cue_disc_number_uses_parent_folder_when_unambiguous() {
        let inferred = infer_cue_disc_number(
            Path::new(
                "/music/Disc 2/TVアニメ「【推しの子】」オリジナルサウンドトラック Disc 2.cue",
            ),
            None,
        );
        assert_eq!(inferred, Some(2));
    }

    #[test]
    fn infer_cue_disc_number_respects_explicit_disc_when_agreeing() {
        let inferred = infer_cue_disc_number(
            Path::new(
                "/music/Disc 2/TVアニメ「【推しの子】」オリジナルサウンドトラック Disc 2.cue",
            ),
            Some(2),
        );
        assert_eq!(inferred, Some(2));
    }

    #[test]
    fn parse_cue_sheet_keeps_track_open_across_file_line_until_index01() {
        let cue = r#"
FILE "01 first.wav" WAVE
  TRACK 01 AUDIO
    TITLE "First"
    INDEX 01 00:00:00
  TRACK 02 AUDIO
    TITLE "Second"
    INDEX 00 03:00:00
FILE "02 second.wav" WAVE
    INDEX 01 00:00:00
"#;

        let parsed = parse_cue_sheet(cue);
        assert_eq!(parsed.tracks.len(), 2);
        assert_eq!(parsed.tracks[1].track_no, 2);
        assert_eq!(parsed.tracks[1].file_ref, "02 second.wav");
        assert_eq!(
            parsed.tracks[1].index00_frames,
            parse_cue_timestamp_to_frames("03:00:00")
        );
        assert_eq!(
            parsed.tracks[1].index01_frames,
            parse_cue_timestamp_to_frames("00:00:00")
        );
    }

    #[test]
    fn resolve_audio_entry_matches_fullwidth_solidus_file_refs() {
        let audio = AudioEntryRef {
            entry_id: DbId(9),
            file_path: PathBuf::from("/music/oshi-no-ko/Disc 1/11. Mystery (w／o Perc.).flac"),
        };
        let mut by_stem = HashMap::new();
        by_stem.insert(
            normalize_lookup_key("11. Mystery (w／o Perc.)"),
            vec![audio.clone()],
        );

        let resolved = resolve_audio_entry_for_cue_track(
            Path::new(
                "/music/oshi-no-ko/Disc 1/TVアニメ「【推しの子】」オリジナルサウンドトラック Disc 1.cue",
            ),
            "11. Mystery (w/o Perc.).wav",
            &HashMap::new(),
            &HashMap::new(),
            &by_stem,
        );

        assert!(resolved.is_some());
        assert_eq!(resolved.unwrap().entry_id, audio.entry_id);
    }

    #[test]
    fn resolve_audio_entry_falls_back_to_stem_for_extension_mismatch() {
        let audio = AudioEntryRef {
            entry_id: DbId(1),
            file_path: PathBuf::from("/music/Persona 4 Dancing All Night/disc 1/01 Dance!.flac"),
        };
        let mut by_stem = HashMap::new();
        by_stem.insert(normalize_lookup_key("01 Dance!"), vec![audio.clone()]);

        let resolved = resolve_audio_entry_for_cue_track(
            Path::new("/music/Persona 4 Dancing All Night/Disc 1.cue"),
            "01 Dance!.wav",
            &HashMap::new(),
            &HashMap::new(),
            &by_stem,
        );

        assert!(resolved.is_some());
        assert_eq!(resolved.unwrap().entry_id, audio.entry_id);
    }

    #[test]
    fn resolve_audio_entry_stem_fallback_requires_unique_match() {
        let mut by_stem = HashMap::new();
        by_stem.insert(
            normalize_lookup_key("01 Intro"),
            vec![
                AudioEntryRef {
                    entry_id: DbId(1),
                    file_path: PathBuf::from("/music/disc 1/01 Intro.flac"),
                },
                AudioEntryRef {
                    entry_id: DbId(2),
                    file_path: PathBuf::from("/music/disc 2/01 Intro.flac"),
                },
            ],
        );

        let resolved = resolve_audio_entry_for_cue_track(
            Path::new("/music/album.cue"),
            "01 Intro.wav",
            &HashMap::new(),
            &HashMap::new(),
            &by_stem,
        );
        assert!(resolved.is_none());
    }
}
