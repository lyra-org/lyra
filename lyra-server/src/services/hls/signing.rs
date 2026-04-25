// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::DbId;
use argon2::password_hash::rand_core::{
    OsRng,
    RngCore,
};
use base64::{
    Engine,
    engine::general_purpose,
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::LazyLock;

use crate::STATE;

const HLS_SIGNED_URL_TTL_SECONDS_DEFAULT: u64 = 90;

static HLS_SEGMENT_SIGNING_KEY: LazyLock<[u8; 32]> = LazyLock::new(|| {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    bytes
});

#[derive(Default, Deserialize, JsonSchema)]
pub(crate) struct HlsSegmentQuery {
    #[serde(default)]
    pub(crate) exp: Option<u64>,
    #[serde(default)]
    pub(crate) sig: Option<String>,
}

fn hls_signed_url_ttl_seconds() -> u64 {
    STATE
        .config
        .get()
        .hls
        .signed_url_ttl_seconds
        .filter(|ttl| *ttl > 0)
        .unwrap_or(HLS_SIGNED_URL_TTL_SECONDS_DEFAULT)
}

// Signed tokens are scoped to (track_id, session_id, expiry). The session_id
// binding prevents cross-session replay: a token minted for one session cannot
// authorize segment fetches under a different session, even for the same track.
// Tokens are NOT segment-scoped; any segment within the session's track can be
// fetched with the same token until it expires.
fn hls_segment_signature_payload(track_db_id: DbId, session_id: &str, exp: u64) -> String {
    format!("{}:{session_id}:{exp}", track_db_id.0)
}

pub(crate) fn hls_sign_segment_token(track_db_id: DbId, session_id: &str, exp: u64) -> String {
    let payload = hls_segment_signature_payload(track_db_id, session_id, exp);
    let signature = blake3::keyed_hash(&*HLS_SEGMENT_SIGNING_KEY, payload.as_bytes());
    general_purpose::URL_SAFE_NO_PAD.encode(signature.as_bytes())
}

pub(crate) fn hls_signed_segment_query(track_db_id: DbId, session_id: &str) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let exp = now.saturating_add(hls_signed_url_ttl_seconds());
    let sig = hls_sign_segment_token(track_db_id, session_id, exp);

    format!("?exp={exp}&sig={sig}")
}

fn append_signed_query_to_uri(uri: &str, signed_query: &str) -> String {
    if uri.contains('?') {
        if let Some(rest) = signed_query.strip_prefix('?') {
            return format!("{uri}&{rest}");
        }
    }

    format!("{uri}{signed_query}")
}

pub(crate) fn validate_signed_segment_query(
    track_db_id: DbId,
    session_id: &str,
    query: &HlsSegmentQuery,
) -> bool {
    let exp = match query.exp {
        Some(exp) => exp,
        None => return false,
    };
    let sig = match query.sig.as_deref() {
        Some(sig) if !sig.is_empty() => sig,
        _ => return false,
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if exp < now {
        return false;
    }

    let sig_bytes = match general_purpose::URL_SAFE_NO_PAD.decode(sig) {
        Ok(bytes) => bytes,
        Err(_) => return false,
    };
    let sig_arr: [u8; 32] = match sig_bytes.try_into() {
        Ok(arr) => arr,
        Err(_) => return false,
    };
    let received_hash = blake3::Hash::from_bytes(sig_arr);

    let payload = hls_segment_signature_payload(track_db_id, session_id, exp);
    let expected_hash = blake3::keyed_hash(&*HLS_SEGMENT_SIGNING_KEY, payload.as_bytes());

    // blake3::Hash::eq uses constant_time_eq internally
    expected_hash == received_hash
}

pub(crate) fn rewrite_playlist_segments(
    playlist: &str,
    track_db_id: DbId,
    session_id: &str,
    signed_query: &str,
) -> String {
    let segment_prefix = format!("/api/stream/by-db-id/{}/hls/{session_id}/", track_db_id.0);
    let mut rewritten = String::with_capacity(playlist.len() + 256);

    for line in playlist.lines() {
        if line.trim().is_empty() {
            rewritten.push_str(line);
        } else if line.starts_with("#EXT-X-MAP:") || line.starts_with("#EXT-X-KEY:") {
            rewritten.push_str(&rewrite_playlist_uri_attr(
                line,
                &segment_prefix,
                signed_query,
            ));
        } else if line.starts_with('#') {
            rewritten.push_str(line);
        } else {
            rewritten.push_str(&segment_prefix);
            rewritten.push_str(&append_signed_query_to_uri(line.trim(), signed_query));
        }
        rewritten.push('\n');
    }

    rewritten
}

fn rewrite_playlist_uri_attr(line: &str, segment_prefix: &str, signed_query: &str) -> String {
    const URI_KEY: &str = "URI=\"";

    let Some(uri_start_idx) = line.find(URI_KEY) else {
        return line.to_string();
    };
    let value_start = uri_start_idx + URI_KEY.len();
    let value_rest = &line[value_start..];
    let Some(value_end_rel) = value_rest.find('"') else {
        return line.to_string();
    };
    let uri_value = &value_rest[..value_end_rel];

    if uri_value.is_empty() || uri_value.starts_with('/') || uri_value.contains("://") {
        return line.to_string();
    }

    let rewritten_uri = append_signed_query_to_uri(uri_value, signed_query);

    let mut rewritten =
        String::with_capacity(line.len() + segment_prefix.len() + rewritten_uri.len());
    rewritten.push_str(&line[..value_start]);
    rewritten.push_str(segment_prefix);
    rewritten.push_str(&rewritten_uri);
    rewritten.push_str(&value_rest[value_end_rel..]);
    rewritten
}

#[cfg(test)]
mod tests {
    use super::*;
    use agdb::DbId;
    use std::time::{
        SystemTime,
        UNIX_EPOCH,
    };

    #[test]
    fn playlist_rewrite_prefixes_ext_x_map_uri() {
        let original = "#EXTM3U\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:6.0,\nsegment-00001.m4s\n";
        let rewritten = rewrite_playlist_segments(original, DbId(99), "sess", "?exp=2&sig=abc");

        assert!(rewritten.contains(
            "#EXT-X-MAP:URI=\"/api/stream/by-db-id/99/hls/sess/init.mp4?exp=2&sig=abc\""
        ));
        assert!(
            rewritten.contains("/api/stream/by-db-id/99/hls/sess/segment-00001.m4s?exp=2&sig=abc")
        );
    }

    #[test]
    fn signed_segment_query_validation_accepts_session_scoped_token() {
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("valid timestamp")
            .as_secs()
            + 30;
        let sig = hls_sign_segment_token(DbId(7), "sess", exp);

        let query = HlsSegmentQuery {
            exp: Some(exp),
            sig: Some(sig),
        };

        assert!(validate_signed_segment_query(DbId(7), "sess", &query));
    }

    #[test]
    fn signed_segment_query_validation_rejects_expired_token() {
        let exp = 1;
        let sig = hls_sign_segment_token(DbId(7), "sess", exp);

        let query = HlsSegmentQuery {
            exp: Some(exp),
            sig: Some(sig),
        };

        assert!(!validate_signed_segment_query(DbId(7), "sess", &query));
    }

    #[test]
    fn signed_segment_query_validation_rejects_cross_session_replay() {
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("valid timestamp")
            .as_secs()
            + 30;
        let sig = hls_sign_segment_token(DbId(7), "session-a", exp);

        let query = HlsSegmentQuery {
            exp: Some(exp),
            sig: Some(sig),
        };

        assert!(!validate_signed_segment_query(DbId(7), "session-b", &query));
    }
}
