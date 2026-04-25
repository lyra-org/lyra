// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::path::Path;

use crate::TrackMetadata;

/// Fill `None` fields on `meta` from filename and parent directory.
/// `known_artists` = artist names collected from tag data across the directory group.
pub fn fill_from_filename(meta: &mut TrackMetadata, file_path: &Path, known_artists: &[String]) {
    let stem = match file_path.file_stem().and_then(|s| s.to_str()) {
        Some(s) => s,
        None => return,
    };

    let (disc, track, remainder) = extract_prefix(stem);

    if meta.disc.is_none() {
        meta.disc = disc;
    }
    if meta.track.is_none() {
        meta.track = track;
    }

    if !remainder.is_empty() {
        let (artist, title) = split_artist_title(remainder, known_artists);

        if meta.artists.is_none() {
            if let Some(a) = artist {
                meta.artists = Some(vec![a.to_string()]);
            }
        }
        if meta.title.is_none() {
            meta.title = Some(title.to_string());
        }
    }

    if meta.album.is_none() {
        if let Some(parent) = file_path.parent().and_then(|p| p.file_name()) {
            if let Some(name) = parent.to_str() {
                meta.album = Some(name.to_string());
            }
        }
    }
}

fn extract_prefix(stem: &str) -> (Option<u32>, Option<u32>, &str) {
    if let Some((disc, track, rest)) = try_disc_dot_track_dot(stem) {
        return (Some(disc), Some(track), rest);
    }
    if let Some((disc, track, rest)) = try_disc_dash_track(stem) {
        return (Some(disc), Some(track), rest);
    }
    if let Some((track, rest)) = try_track_dot(stem) {
        return (None, Some(track), rest);
    }
    if let Some((track, rest)) = try_track_dash(stem) {
        return (None, Some(track), rest);
    }
    if let Some((track, rest)) = try_track_space(stem) {
        return (None, Some(track), rest);
    }
    (None, None, stem)
}

/// `D.TT. rest` — e.g. "1.01. HELLO"
fn try_disc_dot_track_dot(s: &str) -> Option<(u32, u32, &str)> {
    let bytes = s.as_bytes();
    if bytes.is_empty() || !bytes[0].is_ascii_digit() || bytes[0] == b'0' {
        return None;
    }
    if bytes.len() < 2 || bytes[1] != b'.' {
        return None;
    }
    let disc = (bytes[0] - b'0') as u32;
    let rest = &s[2..];
    let digit_end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    if digit_end < 2 {
        return None;
    }
    let track: u32 = rest[..digit_end].parse().ok()?;
    if track > 999 {
        return None;
    }
    let after_digits = &rest[digit_end..];
    if !after_digits.starts_with('.') {
        return None;
    }
    let remainder = after_digits[1..].trim_start();
    Some((disc, track, remainder))
}

/// `D-TT rest` — e.g. "1-01 - Koidatowa"
fn try_disc_dash_track(s: &str) -> Option<(u32, u32, &str)> {
    let bytes = s.as_bytes();
    if bytes.is_empty() || !bytes[0].is_ascii_digit() || bytes[0] == b'0' {
        return None;
    }
    if bytes.len() < 2 || bytes[1] != b'-' {
        return None;
    }
    let disc = (bytes[0] - b'0') as u32;
    let rest = &s[2..];
    let digit_end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    if digit_end < 2 {
        return None;
    }
    let track: u32 = rest[..digit_end].parse().ok()?;
    if track > 999 {
        return None;
    }
    let remainder = rest[digit_end..].trim_start();
    let remainder = remainder.strip_prefix("- ").unwrap_or(remainder);
    Some((disc, track, remainder))
}

/// `TT. rest` — e.g. "05. title"
fn try_track_dot(s: &str) -> Option<(u32, &str)> {
    let digit_end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    if digit_end == 0 {
        return None;
    }
    let track: u32 = s[..digit_end].parse().ok()?;
    if track > 999 {
        return None;
    }
    let after = &s[digit_end..];
    if !after.starts_with('.') {
        return None;
    }
    let remainder = after[1..].trim_start();
    Some((track, remainder))
}

/// `TT - rest` — e.g. "02 - title" (requires 2+ digit track)
fn try_track_dash(s: &str) -> Option<(u32, &str)> {
    let digit_end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    if digit_end < 2 {
        return None;
    }
    let track: u32 = s[..digit_end].parse().ok()?;
    if track > 999 {
        return None;
    }
    let after = &s[digit_end..];
    let trimmed = after.trim_start();
    if !trimmed.starts_with("- ") {
        return None;
    }
    let remainder = &trimmed[2..];
    Some((track, remainder))
}

/// `TT rest` — e.g. "04 title" (requires 2+ digit track)
fn try_track_space(s: &str) -> Option<(u32, &str)> {
    let digit_end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    if digit_end < 2 {
        return None;
    }
    let track: u32 = s[..digit_end].parse().ok()?;
    if track > 999 {
        return None;
    }
    let after = &s[digit_end..];
    if !after.starts_with(' ') {
        return None;
    }
    let remainder = after.trim_start();
    Some((track, remainder))
}

fn split_artist_title<'a>(
    remainder: &'a str,
    known_artists: &[String],
) -> (Option<&'a str>, &'a str) {
    if let Some(pos) = remainder.find(" - ") {
        let candidate = &remainder[..pos];
        if known_artists
            .iter()
            .any(|a| a.eq_ignore_ascii_case(candidate))
        {
            let title = &remainder[pos + 3..];
            return (Some(candidate), title);
        }
    }
    (None, remainder)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_meta() -> TrackMetadata {
        TrackMetadata {
            id: 0,
            file_path: None,
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
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
        }
    }

    #[test]
    fn track_space_separator() {
        let mut meta = empty_meta();
        let path = Path::new("/music/SomeAlbum/04 ライアー -Instrumental-.flac");
        fill_from_filename(&mut meta, path, &[]);
        assert_eq!(meta.track, Some(4));
        assert_eq!(meta.disc, None);
        assert_eq!(meta.title.as_deref(), Some("ライアー -Instrumental-"));
        assert_eq!(meta.album.as_deref(), Some("SomeAlbum"));
    }

    #[test]
    fn artist_dash_title_with_known_artist() {
        let mut meta = empty_meta();
        let path = Path::new("/music/Album/HoneyComeBear - Rainy Girl - レイニーガール.flac");
        let known = vec!["HoneyComeBear".to_string()];
        fill_from_filename(&mut meta, path, &known);
        assert_eq!(meta.track, None);
        assert_eq!(
            meta.artists.as_deref(),
            Some(["HoneyComeBear".to_string()].as_slice())
        );
        assert_eq!(meta.title.as_deref(), Some("Rainy Girl - レイニーガール"));
    }

    #[test]
    fn track_dot_separator() {
        let mut meta = empty_meta();
        let path = Path::new("/music/Album/05. ゼロのままでいられたら.mp3");
        fill_from_filename(&mut meta, path, &[]);
        assert_eq!(meta.track, Some(5));
        assert_eq!(meta.title.as_deref(), Some("ゼロのままでいられたら"));
    }

    #[test]
    fn disc_dot_track_dot() {
        let mut meta = empty_meta();
        let path = Path::new("/music/Album/1.01. HELLO.flac");
        fill_from_filename(&mut meta, path, &[]);
        assert_eq!(meta.disc, Some(1));
        assert_eq!(meta.track, Some(1));
        assert_eq!(meta.title.as_deref(), Some("HELLO"));
    }

    #[test]
    fn track_dash_separator() {
        let mut meta = empty_meta();
        let path = Path::new("/music/Album/02 - 僕らは今のなかで.flac");
        fill_from_filename(&mut meta, path, &[]);
        assert_eq!(meta.track, Some(2));
        assert_eq!(meta.title.as_deref(), Some("僕らは今のなかで"));
    }

    #[test]
    fn no_prefix_bare_title() {
        let mut meta = empty_meta();
        let path = Path::new("/music/Album/チェリーポップ.flac");
        fill_from_filename(&mut meta, path, &[]);
        assert_eq!(meta.track, None);
        assert_eq!(meta.title.as_deref(), Some("チェリーポップ"));
    }

    #[test]
    fn disc_dash_track() {
        let mut meta = empty_meta();
        let path = Path::new("/music/Album/1-01 - Koidatowa.flac");
        fill_from_filename(&mut meta, path, &[]);
        assert_eq!(meta.disc, Some(1));
        assert_eq!(meta.track, Some(1));
        assert_eq!(meta.title.as_deref(), Some("Koidatowa"));
    }

    #[test]
    fn track_dot_with_known_artist() {
        let mut meta = empty_meta();
        let path = Path::new("/music/Album/1. かわにしなつき - 理想的ガール.flac");
        let known = vec!["かわにしなつき".to_string()];
        fill_from_filename(&mut meta, path, &known);
        assert_eq!(meta.track, Some(1));
        assert_eq!(
            meta.artists.as_deref(),
            Some(["かわにしなつき".to_string()].as_slice())
        );
        assert_eq!(meta.title.as_deref(), Some("理想的ガール"));
    }

    #[test]
    fn track_starting_with_digit() {
        let mut meta = empty_meta();
        let path = Path::new("/music/Album/01 1番輝く星.flac");
        fill_from_filename(&mut meta, path, &[]);
        assert_eq!(meta.track, Some(1));
        assert_eq!(meta.title.as_deref(), Some("1番輝く星"));
    }

    #[test]
    fn does_not_overwrite_existing_fields() {
        let mut meta = empty_meta();
        meta.title = Some("Existing Title".to_string());
        meta.track = Some(99);
        meta.album = Some("Existing Album".to_string());
        let path = Path::new("/music/DirAlbum/05. Different Title.flac");
        fill_from_filename(&mut meta, path, &[]);
        assert_eq!(meta.title.as_deref(), Some("Existing Title"));
        assert_eq!(meta.track, Some(99));
        assert_eq!(meta.album.as_deref(), Some("Existing Album"));
    }

    #[test]
    fn unknown_artist_not_extracted() {
        let mut meta = empty_meta();
        let path = Path::new("/music/Album/SomeArtist - SomeTitle.flac");
        fill_from_filename(&mut meta, path, &[]);
        assert_eq!(meta.artists, None);
        assert_eq!(meta.title.as_deref(), Some("SomeArtist - SomeTitle"));
    }

    #[test]
    fn case_insensitive_artist_match() {
        let mut meta = empty_meta();
        let path = Path::new("/music/Album/honeycomebear - Song.flac");
        let known = vec!["HoneyComeBear".to_string()];
        fill_from_filename(&mut meta, path, &known);
        assert_eq!(
            meta.artists.as_deref(),
            Some(["honeycomebear".to_string()].as_slice())
        );
        assert_eq!(meta.title.as_deref(), Some("Song"));
    }
}
