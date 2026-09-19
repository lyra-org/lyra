// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Media formats and barcodes for distinguishing release editions.

use serde::{
    Deserialize,
    Serialize,
};

const GTIN_LENGTH: usize = 14;

/// Common medium families for tag and provider format names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaFormat {
    Cd,
    Vinyl,
    Digital,
    Cassette,
    Sacd,
    Dvd,
    Bluray,
    Minidisc,
    Other,
}

impl MediaFormat {
    pub const ALL: [Self; 9] = [
        Self::Cd,
        Self::Vinyl,
        Self::Digital,
        Self::Cassette,
        Self::Sacd,
        Self::Dvd,
        Self::Bluray,
        Self::Minidisc,
        Self::Other,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cd => "cd",
            Self::Vinyl => "vinyl",
            Self::Digital => "digital",
            Self::Cassette => "cassette",
            Self::Sacd => "sacd",
            Self::Dvd => "dvd",
            Self::Bluray => "bluray",
            Self::Minidisc => "minidisc",
            Self::Other => "other",
        }
    }
}

/// Distinct formats in first-appearance order; unrecognized parts are ignored.
/// Accepts compound values such as `"CD / DVD-Video"` and counts such as `"2xCD"`.
pub fn parse_media_formats(input: &str) -> Vec<MediaFormat> {
    let mut formats = Vec::new();
    for part in input.split([';', ',']).flat_map(|part| part.split(" / ")) {
        if let Some(format) = parse_single_media_format(part)
            && !formats.contains(&format)
        {
            formats.push(format);
        }
    }
    formats
}

/// Distinct formats across several raw values, in order of first appearance.
pub fn collect_media_formats<'a>(inputs: impl IntoIterator<Item = &'a str>) -> Vec<MediaFormat> {
    let mut formats = Vec::new();
    for input in inputs {
        for format in parse_media_formats(input) {
            if !formats.contains(&format) {
                formats.push(format);
            }
        }
    }
    formats
}

/// Distinct formats across a release's tracks, in order of first appearance.
pub fn release_media_formats<'a>(
    per_track: impl IntoIterator<Item = &'a [MediaFormat]>,
) -> Vec<MediaFormat> {
    let mut formats = Vec::new();
    for format in per_track.into_iter().flatten() {
        if !formats.contains(format) {
            formats.push(*format);
        }
    }
    formats
}

/// The barcode shared by a release's tracks. Tracks without one are ignored;
/// conflicting barcodes yield `None`.
pub fn release_barcode<'a>(per_track: impl IntoIterator<Item = Option<&'a str>>) -> Option<String> {
    let mut barcodes = per_track.into_iter().flatten();
    let first = barcodes.next()?;
    barcodes
        .all(|barcode| barcode == first)
        .then(|| first.to_string())
}

/// True when the two sides share at least one format. Empty sides never match.
pub fn media_formats_match(a: &[MediaFormat], b: &[MediaFormat]) -> bool {
    a.iter().any(|format| b.contains(format))
}

fn parse_single_media_format(part: &str) -> Option<MediaFormat> {
    let lowered = part
        .trim()
        .trim_matches(|c| c == '(' || c == ')')
        .to_lowercase();
    let name = strip_medium_count(lowered.trim());
    if name.is_empty() {
        return None;
    }

    if let Some(format) = MediaFormat::ALL
        .into_iter()
        .find(|format| format.as_str() == name)
    {
        return Some(format);
    }

    // ID3v2 `TMED` codes carry `/`-separated qualifiers, e.g. `CD/DD`, `TT/33`.
    let code = name.split('/').next().unwrap_or(name).trim();
    match code {
        "dig" | "web" | "digital media" | "download" | "file" | "streaming" => {
            return Some(MediaFormat::Digital);
        }
        "tt" | "lp" => return Some(MediaFormat::Vinyl),
        "mc" => return Some(MediaFormat::Cassette),
        "md" => return Some(MediaFormat::Minidisc),
        "bd" => return Some(MediaFormat::Bluray),
        _ => {}
    }

    // Match SACD and VCD variants before the broader "cd" check.
    if name.contains("sacd") {
        Some(MediaFormat::Sacd)
    } else if name.contains("dvd") {
        Some(MediaFormat::Dvd)
    } else if name.contains("blu-ray") || name.contains("bluray") {
        Some(MediaFormat::Bluray)
    } else if name.contains("vinyl") {
        Some(MediaFormat::Vinyl)
    } else if name.contains("cassette") {
        Some(MediaFormat::Cassette)
    } else if name.contains("minidisc") {
        Some(MediaFormat::Minidisc)
    } else if matches!(name, "vcd" | "svcd" | "cdv") {
        Some(MediaFormat::Other)
    } else if name.contains("cd") {
        Some(MediaFormat::Cd)
    } else {
        None
    }
}

fn strip_medium_count(name: &str) -> &str {
    let digits_end = name
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(name.len());
    if digits_end == 0 {
        return name;
    }
    let rest = name[digits_end..].trim_start();
    match rest.strip_prefix('x').or_else(|| rest.strip_prefix('×')) {
        Some(stripped) => stripped.trim_start(),
        None => name,
    }
}

/// Removes spaces and hyphens, then zero-pads to GTIN-14 so UPC/EAN/GTIN forms match.
/// Requires 8–14 digits, at least one nonzero; does not validate the check digit.
pub fn normalize_barcode(input: &str) -> Option<String> {
    let mut digits = String::new();
    for c in input.trim().chars() {
        match c {
            '0'..='9' => digits.push(c),
            ' ' | '-' => {}
            _ => return None,
        }
    }
    if !(8..=GTIN_LENGTH).contains(&digits.len()) || digits.bytes().all(|b| b == b'0') {
        return None;
    }
    Some(format!("{digits:0>GTIN_LENGTH$}"))
}

/// True when both sides normalize to the same barcode.
pub fn barcodes_match(a: &str, b: &str) -> bool {
    match (normalize_barcode(a), normalize_barcode(b)) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_each_tag_vocabulary() {
        for (input, expected) in [
            ("WEB", MediaFormat::Digital),
            ("Digital Media", MediaFormat::Digital),
            ("DIG", MediaFormat::Digital),
            ("(DIG/A)", MediaFormat::Digital),
            ("digital", MediaFormat::Digital),
            ("Digital", MediaFormat::Digital),
            ("File", MediaFormat::Digital),
            ("1 x File, FLAC", MediaFormat::Digital),
            ("CDr", MediaFormat::Cd),
            ("CD, Album, Reissue", MediaFormat::Cd),
            ("CD", MediaFormat::Cd),
            ("CD/DD", MediaFormat::Cd),
            ("SHM-CD", MediaFormat::Cd),
            ("Blu-spec CD", MediaFormat::Cd),
            ("8cm CD+G", MediaFormat::Cd),
            ("Hybrid SACD (CD layer)", MediaFormat::Sacd),
            ("12\" Vinyl", MediaFormat::Vinyl),
            ("TT/33", MediaFormat::Vinyl),
            ("MC/IV", MediaFormat::Cassette),
            ("Microcassette", MediaFormat::Cassette),
            ("DVD-Video", MediaFormat::Dvd),
            ("DualDisc (DVD-Audio side)", MediaFormat::Dvd),
            ("Blu-ray-R", MediaFormat::Bluray),
            ("MD", MediaFormat::Minidisc),
            ("SVCD", MediaFormat::Other),
            ("Other", MediaFormat::Other),
        ] {
            assert_eq!(parse_media_formats(input), vec![expected], "{input}");
        }
    }

    #[test]
    fn drops_unrecognized_and_blank_values() {
        assert!(parse_media_formats("").is_empty());
        assert!(parse_media_formats("   ").is_empty());
        assert!(parse_media_formats("Soundboard").is_empty());
        assert!(parse_media_formats("Piano roll").is_empty());
    }

    #[test]
    fn splits_multi_medium_values() {
        assert_eq!(
            parse_media_formats("CD / DVD-Video"),
            vec![MediaFormat::Cd, MediaFormat::Dvd]
        );
        assert_eq!(parse_media_formats("2xCD"), vec![MediaFormat::Cd]);
        assert_eq!(parse_media_formats("3 × Vinyl"), vec![MediaFormat::Vinyl]);
        assert_eq!(
            parse_media_formats("CD / SHM-CD / Enhanced CD"),
            vec![MediaFormat::Cd]
        );
        assert_eq!(
            parse_media_formats("CD; Digital Media"),
            vec![MediaFormat::Cd, MediaFormat::Digital]
        );
    }

    #[test]
    fn collects_distinct_formats_across_values() {
        assert_eq!(
            collect_media_formats(["WEB", "Digital Media", "CD"]),
            vec![MediaFormat::Digital, MediaFormat::Cd]
        );
    }

    #[test]
    fn release_signals_aggregate_across_tracks() {
        let digital = [MediaFormat::Digital];
        let cd = [MediaFormat::Cd];
        let none: [MediaFormat; 0] = [];
        assert_eq!(
            release_media_formats([&digital[..], &none[..], &cd[..], &digital[..]]),
            vec![MediaFormat::Digital, MediaFormat::Cd]
        );

        assert_eq!(
            release_barcode([Some("123456789012"), None, Some("123456789012")]).as_deref(),
            Some("123456789012")
        );
        assert_eq!(
            release_barcode([Some("123456789012"), Some("999999999999")]),
            None
        );
        assert_eq!(release_barcode([None, None]), None);
    }

    #[test]
    fn formats_match_on_any_overlap() {
        let local = [MediaFormat::Cd, MediaFormat::Dvd];
        assert!(media_formats_match(&local, &[MediaFormat::Dvd]));
        assert!(!media_formats_match(&local, &[MediaFormat::Digital]));
        assert!(!media_formats_match(&local, &[]));
        assert!(!media_formats_match(&[], &[]));
    }

    #[test]
    fn serializes_as_snake_case() {
        assert_eq!(
            serde_json::to_string(&MediaFormat::Bluray).unwrap(),
            "\"bluray\""
        );
        for format in MediaFormat::ALL {
            assert_eq!(
                serde_json::to_string(&format).unwrap(),
                format!("\"{}\"", format.as_str())
            );
        }
    }

    #[test]
    fn normalizes_barcodes() {
        assert_eq!(
            normalize_barcode("199957588430").as_deref(),
            Some("00199957588430")
        );
        assert_eq!(
            normalize_barcode("0199957588430").as_deref(),
            Some("00199957588430")
        );
        assert_eq!(
            normalize_barcode(" 4988-0315 54531 ").as_deref(),
            Some("04988031554531")
        );
        assert_eq!(normalize_barcode("00000000"), None);
        assert_eq!(normalize_barcode("1234567"), None);
        assert_eq!(normalize_barcode("123456789012345"), None);
        assert_eq!(normalize_barcode("ABCD12345678"), None);
        assert_eq!(normalize_barcode(""), None);
    }

    #[test]
    fn barcode_normalization_is_idempotent() {
        for input in ["00012345", "01234565", "000001234567", "199957588430"] {
            let normalized = normalize_barcode(input).unwrap();
            assert_eq!(
                normalize_barcode(&normalized).as_deref(),
                Some(normalized.as_str())
            );
            assert!(barcodes_match(input, &normalized), "{input}");
        }
    }

    #[test]
    fn barcodes_match_across_zero_padding() {
        assert!(barcodes_match("602455505934", "0602455505934"));
        assert!(barcodes_match("00602455505934", "602455505934"));
        assert!(!barcodes_match("199957588430", "199957588416"));
        assert!(!barcodes_match("", ""));
    }
}
