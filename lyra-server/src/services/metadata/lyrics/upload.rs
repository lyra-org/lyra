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
            lrc_to_input(text, language.unwrap_or_else(|| "und".to_string()), now_ms)
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

fn lrc_to_input(
    contents: &str,
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

        for ts_ms in timestamps {
            lines.push(LineInput {
                ts_ms,
                text: text.to_string(),
                words: Vec::new(),
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
        id: "user".to_string(),
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
    fn lrc_to_input_rejects_non_timestamped_lyrics() {
        assert!(lrc_to_input("plain lyric line", "und".to_string(), 0).is_err());
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
