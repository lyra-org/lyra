// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::time::{
    SystemTime,
    UNIX_EPOCH,
};

use agdb::{
    DbAny,
    DbId,
};
use serde::Deserialize;

use crate::db::{
    self,
    IdSource,
    NodeId,
    lyrics::{
        LineInput,
        LyricsDetail,
        LyricsInput,
        WordInput,
    },
};

const LYRICS_UPLOAD_MAX_BYTES: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub(crate) enum LyricsUploadError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[derive(Deserialize)]
struct LyricsJsonRequest {
    id: Option<String>,
    language: String,
    plain_text: String,
    #[serde(default)]
    lines: Vec<LyricsLineRequest>,
}

#[derive(Deserialize)]
struct LyricsLineRequest {
    ts_ms: u64,
    text: String,
    #[serde(default)]
    words: Vec<LyricsWordRequest>,
}

#[derive(Deserialize)]
struct LyricsWordRequest {
    ts_ms: u64,
    char_start: u32,
    char_end: u32,
}

impl LyricsJsonRequest {
    fn into_input(self, now_ms: u64) -> Result<LyricsInput, LyricsUploadError> {
        let id = self.id.unwrap_or_else(|| "user".to_string());
        if id.trim().is_empty() {
            return Err(LyricsUploadError::BadRequest(
                "lyrics id cannot be empty".to_string(),
            ));
        }

        Ok(LyricsInput {
            id,
            provider_id: String::new(),
            language: self.language,
            plain_text: self.plain_text,
            lines: self
                .lines
                .into_iter()
                .map(|line| LineInput {
                    ts_ms: line.ts_ms,
                    text: line.text,
                    words: line
                        .words
                        .into_iter()
                        .map(|word| WordInput {
                            ts_ms: word.ts_ms,
                            char_start: word.char_start,
                            char_end: word.char_end,
                        })
                        .collect(),
                })
                .collect(),
            last_checked_at: now_ms,
        })
    }
}

pub(crate) fn now_ms() -> Result<u64, LyricsUploadError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| LyricsUploadError::BadRequest("system clock is before unix epoch".into()))?;
    Ok(now.as_millis() as u64)
}

pub(crate) fn input_from_upload(
    content_type: &str,
    body: &[u8],
    language: Option<String>,
    now_ms: u64,
) -> Result<LyricsInput, LyricsUploadError> {
    if body.len() > LYRICS_UPLOAD_MAX_BYTES {
        return Err(LyricsUploadError::BadRequest(format!(
            "lyrics upload exceeds maximum {LYRICS_UPLOAD_MAX_BYTES} bytes"
        )));
    }

    match content_type {
        "application/json" => serde_json::from_slice::<LyricsJsonRequest>(body)
            .map_err(|err| LyricsUploadError::BadRequest(format!("invalid lyrics JSON: {err}")))?
            .into_input(now_ms),
        "application/lrc" | "text/x-lrc" => {
            let text = std::str::from_utf8(body).map_err(|_| {
                LyricsUploadError::BadRequest("LRC upload must be valid UTF-8".into())
            })?;
            lrc_to_input(
                text,
                "user".to_string(),
                language.unwrap_or_else(|| "und".to_string()),
                now_ms,
            )
        }
        "text/plain" => {
            let text = std::str::from_utf8(body).map_err(|_| {
                LyricsUploadError::BadRequest("plain lyrics upload must be valid UTF-8".into())
            })?;
            Ok(plain_text_to_input(
                text,
                language.unwrap_or_else(|| "und".to_string()),
                now_ms,
            ))
        }
        other => Err(LyricsUploadError::BadRequest(format!(
            "unsupported lyrics Content-Type: {other}. Supported: application/json, application/lrc, text/x-lrc, text/plain"
        ))),
    }
}

pub(crate) fn upsert_user_lyrics(
    db: &mut DbAny,
    track_public_id: &str,
    input: LyricsInput,
) -> Result<LyricsDetail, LyricsUploadError> {
    let track_db_id = db::lookup::find_node_id_by_id(db, track_public_id)?.ok_or_else(|| {
        LyricsUploadError::NotFound(format!("Track not found: {track_public_id}"))
    })?;
    upsert_user_lyrics_by_db_id(db, track_db_id, input)
}

pub(crate) fn upsert_plugin_lyrics(
    db: &mut DbAny,
    track_db_id: DbId,
    mut input: LyricsInput,
    provider_id: String,
) -> Result<DbId, LyricsUploadError> {
    input.provider_id = provider_id;
    let track = db::tracks::get_by_id(db, track_db_id)?.ok_or_else(|| {
        LyricsUploadError::NotFound(format!("Track not found: {}", track_db_id.0))
    })?;
    db::lyrics::upsert_from_plugin(db, track_db_id, input, track.duration_ms)
        .map_err(|err| LyricsUploadError::BadRequest(err.to_string()))
}

pub(crate) fn delete_user_lyrics_for_track(
    db: &mut DbAny,
    track_public_id: &str,
) -> Result<bool, LyricsUploadError> {
    let track_db_id = db::lookup::find_node_id_by_id(db, track_public_id)?.ok_or_else(|| {
        LyricsUploadError::NotFound(format!("Track not found: {track_public_id}"))
    })?;
    delete_user_lyrics_for_track_by_db_id(db, track_db_id)
}

pub(crate) fn delete_user_lyrics_for_track_by_db_id(
    db: &mut DbAny,
    track_db_id: DbId,
) -> Result<bool, LyricsUploadError> {
    db::tracks::get_by_id(db, track_db_id)?.ok_or_else(|| {
        LyricsUploadError::NotFound(format!("Track not found: {}", track_db_id.0))
    })?;
    let lyrics = db::lyrics::get_for_track(db, track_db_id)?;
    let user_lyrics_ids: Vec<DbId> = lyrics
        .into_iter()
        .filter(|lyrics| {
            matches!(lyrics.origin, IdSource::User)
                && lyrics.provider_id.eq_ignore_ascii_case("user")
        })
        .filter_map(|lyrics| lyrics.db_id.map(NodeId::into))
        .collect();

    let removed = !user_lyrics_ids.is_empty();
    for lyrics_db_id in user_lyrics_ids {
        db::lyrics::delete_by_db_id(db, lyrics_db_id)?;
    }
    Ok(removed)
}

pub(crate) fn delete_all_lyrics_for_track(
    db: &mut DbAny,
    track_db_id: DbId,
) -> Result<(), LyricsUploadError> {
    db::lyrics::delete_for_track(db, track_db_id)?;
    Ok(())
}

pub(crate) fn upsert_user_lyrics_by_db_id(
    db: &mut DbAny,
    track_db_id: DbId,
    input: LyricsInput,
) -> Result<LyricsDetail, LyricsUploadError> {
    let track = db::tracks::get_by_id(db, track_db_id)?.ok_or_else(|| {
        LyricsUploadError::NotFound(format!("Track not found: {}", track_db_id.0))
    })?;

    let lyrics_db_id = db::lyrics::upsert_user_override(db, track_db_id, input, track.duration_ms)
        .map_err(|err| LyricsUploadError::BadRequest(err.to_string()))?;
    db::lyrics::get_detail(db, lyrics_db_id)?
        .ok_or_else(|| LyricsUploadError::NotFound("lyrics not found after upsert".to_string()))
}

fn plain_text_to_input(contents: &str, language: String, now_ms: u64) -> LyricsInput {
    LyricsInput {
        id: "user".to_string(),
        provider_id: String::new(),
        language,
        plain_text: contents.to_string(),
        lines: Vec::new(),
        last_checked_at: now_ms,
    }
}

/// Parse LRC text into a `LyricsInput`. Handles the standard `[mm:ss.xx]` line
/// timestamps plus Enhanced-LRC `<mm:ss.xx>` word cues, with per-`WordInput`
/// offsets keyed to character positions in the cleaned line.
pub(crate) fn lrc_to_input(
    contents: &str,
    id: String,
    language: String,
    now_ms: u64,
) -> Result<LyricsInput, LyricsUploadError> {
    let mut lines = Vec::new();

    for (line_no, raw_line) in contents.lines().enumerate() {
        let line_no = line_no + 1;
        let raw_line = raw_line.trim_end_matches('\r');
        if raw_line.trim().is_empty() {
            continue;
        }

        let (timestamps, text) = parse_lrc_line(raw_line).map_err(|message| {
            LyricsUploadError::BadRequest(format!("invalid LRC at line {line_no}: {message}"))
        })?;
        if timestamps.is_empty() {
            continue;
        }

        let (cleaned_text, words) = parse_enhanced_lrc_words(text).map_err(|message| {
            LyricsUploadError::BadRequest(format!(
                "invalid LRC word cue at line {line_no}: {message}"
            ))
        })?;

        for ts_ms in timestamps {
            lines.push(LineInput {
                ts_ms,
                text: cleaned_text.clone(),
                words: words.clone(),
            });
        }
    }

    if lines.is_empty() {
        return Err(LyricsUploadError::BadRequest(
            "LRC upload contains no timestamped lyric lines".to_string(),
        ));
    }

    lines.sort_by_key(|line| line.ts_ms);
    let plain_text = lines
        .iter()
        .map(|line| line.text.as_str())
        .collect::<Vec<_>>()
        .join("\n");

    Ok(LyricsInput {
        id,
        provider_id: String::new(),
        language,
        plain_text,
        lines,
        last_checked_at: now_ms,
    })
}

fn parse_lrc_line(raw_line: &str) -> Result<(Vec<u64>, &str), &'static str> {
    let mut rest = raw_line;
    let mut timestamps = Vec::new();

    while let Some(tag) = rest.strip_prefix('[') {
        let Some(close_idx) = tag.find(']') else {
            return Err("missing closing bracket");
        };
        let value = &tag[..close_idx];
        rest = &tag[close_idx + 1..];

        if let Some(ts_ms) = parse_lrc_timestamp(value)? {
            timestamps.push(ts_ms);
        } else if !timestamps.is_empty() {
            return Err("metadata tag cannot appear after timestamp tags");
        }
    }

    if timestamps.is_empty() && !looks_like_lrc_metadata(raw_line) {
        return Err("expected one or more timestamp tags");
    }

    Ok((timestamps, rest))
}

fn parse_lrc_timestamp(value: &str) -> Result<Option<u64>, &'static str> {
    let Some((minutes, rest)) = value.split_once(':') else {
        return Ok(None);
    };
    if minutes.is_empty() || !minutes.chars().all(|c| c.is_ascii_digit()) {
        return Ok(None);
    }

    let (seconds, fraction) = rest.split_once('.').unwrap_or((rest, ""));
    if seconds.len() != 2 || !seconds.chars().all(|c| c.is_ascii_digit()) {
        return Ok(None);
    }

    let minutes: u64 = minutes.parse().map_err(|_| "timestamp minutes overflow")?;
    let seconds: u64 = seconds.parse().map_err(|_| "timestamp seconds overflow")?;
    if seconds >= 60 {
        return Err("timestamp seconds must be less than 60");
    }

    let millis = match fraction.len() {
        0 => 0,
        2 if fraction.chars().all(|c| c.is_ascii_digit()) => {
            fraction
                .parse::<u64>()
                .map_err(|_| "timestamp fraction overflow")?
                * 10
        }
        3 if fraction.chars().all(|c| c.is_ascii_digit()) => fraction
            .parse::<u64>()
            .map_err(|_| "timestamp fraction overflow")?,
        _ => return Err("timestamp fraction must be centiseconds or milliseconds"),
    };

    Ok(Some((minutes * 60 + seconds) * 1000 + millis))
}

/// Strip Enhanced-LRC `<mm:ss.xx>` word cues and emit the corresponding
/// [`WordInput`]s. Each cue marks the start time of the segment that follows;
/// `char_start`/`char_end` are character offsets (not bytes) into the cleaned
/// line text.
fn parse_enhanced_lrc_words(text: &str) -> Result<(String, Vec<WordInput>), &'static str> {
    let mut cleaned = String::new();
    let mut words: Vec<WordInput> = Vec::new();
    let mut rest = text;
    let mut pending_ts: Option<u64> = None;

    loop {
        let Some(open_idx) = rest.find('<') else {
            if let Some(ts_ms) = pending_ts.take() {
                let char_start = cleaned.chars().count() as u32;
                cleaned.push_str(rest);
                let char_end = cleaned.chars().count() as u32;
                words.push(WordInput {
                    ts_ms,
                    char_start,
                    char_end,
                });
            } else {
                cleaned.push_str(rest);
            }
            break;
        };

        let segment = &rest[..open_idx];
        if let Some(ts_ms) = pending_ts.take() {
            let char_start = cleaned.chars().count() as u32;
            cleaned.push_str(segment);
            let char_end = cleaned.chars().count() as u32;
            // Emit even when char_start == char_end so consumers can derive
            // the previous word's end-time from this cue's ts_ms.
            words.push(WordInput {
                ts_ms,
                char_start,
                char_end,
            });
        } else {
            cleaned.push_str(segment);
        }

        rest = &rest[open_idx + 1..];
        let Some(close_idx) = rest.find('>') else {
            return Err("missing closing '>' for word cue");
        };
        let cue_value = &rest[..close_idx];
        rest = &rest[close_idx + 1..];

        match parse_lrc_timestamp(cue_value)? {
            Some(ts_ms) => pending_ts = Some(ts_ms),
            None => return Err("word cue must be a valid timestamp"),
        }
    }

    Ok((cleaned, words))
}

fn looks_like_lrc_metadata(raw_line: &str) -> bool {
    let Some(stripped) = raw_line.strip_prefix('[') else {
        return false;
    };
    let Some(tag) = stripped.strip_suffix(']') else {
        return false;
    };
    let Some((key, _value)) = tag.split_once(':') else {
        return false;
    };
    !key.is_empty()
        && key
            .chars()
            .all(|c| c.is_ascii_alphabetic() || c.is_ascii_digit() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_lrc_timestamp_accepts_centiseconds_and_milliseconds() {
        assert_eq!(parse_lrc_timestamp("01:02.34").unwrap(), Some(62_340));
        assert_eq!(parse_lrc_timestamp("01:02.345").unwrap(), Some(62_345));
        assert_eq!(parse_lrc_timestamp("01:02").unwrap(), Some(62_000));
    }

    #[test]
    fn parse_lrc_line_accepts_repeated_timestamps() {
        let (timestamps, text) = parse_lrc_line("[00:01.00][00:02.50]hello").unwrap();

        assert_eq!(timestamps, vec![1_000, 2_500]);
        assert_eq!(text, "hello");
    }

    #[test]
    fn lrc_to_input_ignores_metadata_and_sorts_lines() {
        let input = lrc_to_input(
            "[ar:artist]\n[00:03.00]third\n[00:01.00]first\n[00:02.00]second",
            "user".to_string(),
            "eng".to_string(),
            42,
        )
        .unwrap();

        assert_eq!(input.id, "user");
        assert_eq!(input.language, "eng");
        assert_eq!(input.last_checked_at, 42);
        assert_eq!(input.plain_text, "first\nsecond\nthird");
        assert_eq!(
            input
                .lines
                .iter()
                .map(|line| line.ts_ms)
                .collect::<Vec<_>>(),
            vec![1_000, 2_000, 3_000]
        );
    }

    #[test]
    fn lrc_to_input_uses_caller_supplied_id() {
        let input = lrc_to_input(
            "[00:01.00]hello",
            "lrclib:42".to_string(),
            "eng".to_string(),
            0,
        )
        .unwrap();
        assert_eq!(input.id, "lrclib:42");
    }

    #[test]
    fn lrc_to_input_rejects_non_timestamped_lyrics() {
        assert!(
            lrc_to_input(
                "plain lyric line",
                "user".to_string(),
                "und".to_string(),
                0
            )
            .is_err()
        );
    }

    #[test]
    fn parse_enhanced_lrc_words_passes_through_text_without_cues() {
        let (cleaned, words) = parse_enhanced_lrc_words("hello world").unwrap();
        assert_eq!(cleaned, "hello world");
        assert!(words.is_empty());
    }

    #[test]
    fn parse_enhanced_lrc_words_emits_words_for_each_cue() {
        let (cleaned, words) =
            parse_enhanced_lrc_words("<00:01.00>Hello <00:01.50>world<00:02.00>").unwrap();
        assert_eq!(cleaned, "Hello world");
        assert_eq!(words.len(), 3);
        assert_eq!(words[0].ts_ms, 1_000);
        assert_eq!(words[0].char_start, 0);
        assert_eq!(words[0].char_end, 6);
        assert_eq!(words[1].ts_ms, 1_500);
        assert_eq!(words[1].char_start, 6);
        assert_eq!(words[1].char_end, 11);
        assert_eq!(words[2].ts_ms, 2_000);
        assert_eq!(words[2].char_start, 11);
        // Trailing zero-width word: lets consumers derive prev word's end-time.
        assert_eq!(words[2].char_end, 11);
    }

    #[test]
    fn parse_enhanced_lrc_words_uses_character_positions_not_bytes() {
        // "héllo" is 5 chars / 6 bytes — assertions catch a byte-counting regression.
        let (cleaned, words) = parse_enhanced_lrc_words("<00:01.00>héllo<00:02.00>").unwrap();
        assert_eq!(cleaned, "héllo");
        assert_eq!(words.len(), 2);
        assert_eq!(words[0].char_start, 0);
        assert_eq!(words[0].char_end, 5);
    }

    #[test]
    fn parse_enhanced_lrc_words_rejects_unclosed_cue() {
        let err = parse_enhanced_lrc_words("<00:01.00 hello").unwrap_err();
        assert!(err.contains("closing"));
    }

    #[test]
    fn parse_enhanced_lrc_words_rejects_invalid_timestamp() {
        // `<i>`-style markup must not be silently swallowed as a cue.
        let err = parse_enhanced_lrc_words("<i>hello<00:01.00>world").unwrap_err();
        assert!(err.contains("valid timestamp"));
    }

    #[test]
    fn lrc_to_input_threads_word_cues_into_each_line() {
        let input = lrc_to_input(
            "[00:01.00]<00:01.00>Hello <00:01.50>world<00:02.00>",
            "user".to_string(),
            "eng".to_string(),
            0,
        )
        .unwrap();

        assert_eq!(input.lines.len(), 1);
        let line = &input.lines[0];
        assert_eq!(line.ts_ms, 1_000);
        assert_eq!(line.text, "Hello world");
        assert_eq!(line.words.len(), 3);
        assert_eq!(line.words[0].ts_ms, 1_000);
        assert_eq!(line.words[1].ts_ms, 1_500);
        assert_eq!(line.words[2].ts_ms, 2_000);
        assert_eq!(input.plain_text, "Hello world");
    }

    #[test]
    fn lrc_to_input_clones_words_for_repeated_line_timestamps() {
        let input = lrc_to_input(
            "[00:01.00][00:02.00]<00:00.10>Hello",
            "user".to_string(),
            "eng".to_string(),
            0,
        )
        .unwrap();

        assert_eq!(input.lines.len(), 2);
        for line in &input.lines {
            assert_eq!(line.text, "Hello");
            assert_eq!(line.words.len(), 1);
            assert_eq!(line.words[0].ts_ms, 100);
        }
    }

    #[test]
    fn text_plain_upload_stores_non_timestamped_plain_text() {
        let input = input_from_upload(
            "text/plain",
            b"[00:01.00]not lrc\nplain lyric line",
            Some("ENG".to_string()),
            42,
        )
        .unwrap();

        assert_eq!(input.id, "user");
        assert_eq!(input.language, "ENG");
        assert_eq!(input.last_checked_at, 42);
        assert_eq!(input.plain_text, "[00:01.00]not lrc\nplain lyric line");
        assert!(input.lines.is_empty());
    }
}
