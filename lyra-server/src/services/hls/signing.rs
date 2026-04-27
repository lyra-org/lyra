// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

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

// Signed tokens are scoped to (session_id, expiry). Tokens are NOT
// segment-scoped; any segment within the session can be fetched with the same
// token until it expires.
fn hls_segment_signature_payload(session_id: &str, exp: u64) -> String {
    format!("{session_id}:{exp}")
}

pub(crate) fn hls_sign_segment_token(session_id: &str, exp: u64) -> String {
    let payload = hls_segment_signature_payload(session_id, exp);
    let signature = blake3::keyed_hash(&*HLS_SEGMENT_SIGNING_KEY, payload.as_bytes());
    general_purpose::URL_SAFE_NO_PAD.encode(signature.as_bytes())
}

pub(crate) fn hls_signed_segment_query(session_id: &str) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let exp = now.saturating_add(hls_signed_url_ttl_seconds());
    let sig = hls_sign_segment_token(session_id, exp);

    format!("?exp={exp}&sig={sig}")
}

pub(crate) fn validate_signed_segment_query(session_id: &str, query: &HlsSegmentQuery) -> bool {
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

    let payload = hls_segment_signature_payload(session_id, exp);
    let expected_hash = blake3::keyed_hash(&*HLS_SEGMENT_SIGNING_KEY, payload.as_bytes());

    // blake3::Hash::eq uses constant_time_eq internally
    expected_hash == received_hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{
        SystemTime,
        UNIX_EPOCH,
    };

    #[test]
    fn signed_segment_query_validation_accepts_session_scoped_token() {
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("valid timestamp")
            .as_secs()
            + 30;
        let sig = hls_sign_segment_token("sess", exp);

        let query = HlsSegmentQuery {
            exp: Some(exp),
            sig: Some(sig),
        };

        assert!(validate_signed_segment_query("sess", &query));
    }

    #[test]
    fn signed_segment_query_validation_rejects_expired_token() {
        let exp = 1;
        let sig = hls_sign_segment_token("sess", exp);

        let query = HlsSegmentQuery {
            exp: Some(exp),
            sig: Some(sig),
        };

        assert!(!validate_signed_segment_query("sess", &query));
    }

    #[test]
    fn signed_segment_query_validation_rejects_cross_session_replay() {
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("valid timestamp")
            .as_secs()
            + 30;
        let sig = hls_sign_segment_token("session-a", exp);

        let query = HlsSegmentQuery {
            exp: Some(exp),
            sig: Some(sig),
        };

        assert!(!validate_signed_segment_query("session-b", &query));
    }
}
