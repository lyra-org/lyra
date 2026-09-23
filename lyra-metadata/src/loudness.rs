// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use serde::{
    Deserialize,
    Serialize,
};

/// R128 gains target -23 LUFS; ReplayGain 2.0 targets -18 LUFS.
const R128_TO_REPLAYGAIN_DB: f64 = 5.0;

/// ReplayGain and EBU R128 gain tags as read from the file.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RawGainTags {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replaygain_track_gain: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replaygain_album_gain: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r128_track_gain: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r128_album_gain: Option<String>,
}

impl RawGainTags {
    pub(crate) fn track_gain_db(&self) -> Option<f64> {
        resolve_gain_db(
            self.replaygain_track_gain.as_deref(),
            self.r128_track_gain.as_deref(),
        )
    }

    pub(crate) fn album_gain_db(&self) -> Option<f64> {
        resolve_gain_db(
            self.replaygain_album_gain.as_deref(),
            self.r128_album_gain.as_deref(),
        )
    }
}

/// Gain in dB against the ReplayGain 2.0 reference, preferring ReplayGain over R128.
fn resolve_gain_db(replaygain: Option<&str>, r128: Option<&str>) -> Option<f64> {
    replaygain
        .and_then(parse_replaygain_gain)
        .or_else(|| r128.and_then(parse_r128_gain))
}

/// Parses values such as `-6.54 dB`.
fn parse_replaygain_gain(value: &str) -> Option<f64> {
    let value = value.trim();
    let number = match value.get(value.len().saturating_sub(2)..) {
        Some(unit) if unit.eq_ignore_ascii_case("db") => &value[..value.len() - 2],
        _ => value,
    };
    number
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|gain| gain.is_finite())
}

/// Parses a Q7.8 fixed-point R128 gain.
fn parse_r128_gain(value: &str) -> Option<f64> {
    let q78 = value.trim().parse::<i16>().ok()?;
    Some(f64::from(q78) / 256.0 + R128_TO_REPLAYGAIN_DB)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_replaygain_gains() {
        assert_eq!(parse_replaygain_gain("-6.54 dB"), Some(-6.54));
        assert_eq!(parse_replaygain_gain("+1.20 DB"), Some(1.2));
        assert_eq!(parse_replaygain_gain(" 3.5dB "), Some(3.5));
        assert_eq!(parse_replaygain_gain("0.00"), Some(0.0));
        assert_eq!(parse_replaygain_gain("dB"), None);
        assert_eq!(parse_replaygain_gain("NaN dB"), None);
        assert_eq!(parse_replaygain_gain("loud"), None);
    }

    #[test]
    fn converts_r128_gains_to_replaygain_reference() {
        assert_eq!(parse_r128_gain("-1280"), Some(0.0));
        assert_eq!(parse_r128_gain("0"), Some(5.0));
        assert_eq!(parse_r128_gain("256"), Some(6.0));
        assert_eq!(parse_r128_gain("-6.5"), None);
        assert_eq!(parse_r128_gain("40000"), None);
    }

    #[test]
    fn prefers_replaygain_over_r128() {
        assert_eq!(resolve_gain_db(Some("-2 dB"), Some("0")), Some(-2.0));
        assert_eq!(resolve_gain_db(Some("bad"), Some("0")), Some(5.0));
        assert_eq!(resolve_gain_db(None, None), None);
    }
}
