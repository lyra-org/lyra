// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use axum::http::HeaderMap;

/// Non-browser clients omit `Origin`; browsers send it truthfully, so
/// checking it blocks CSWSH for cookie-bearing sessions without
/// penalizing CLIs and native apps.
pub(crate) fn validate(headers: &HeaderMap) -> Result<(), &'static str> {
    let Some(origin) = headers.get("origin").and_then(|v| v.to_str().ok()) else {
        return Ok(());
    };

    let Ok(url) = url::Url::parse(origin) else {
        return Err("cross-origin WebSocket connection rejected");
    };

    let host = url.host_str().unwrap_or("");
    if host == "localhost" || host == "127.0.0.1" || host == "[::1]" {
        return Ok(());
    }

    if let Some(host_header) = headers.get("host").and_then(|v| v.to_str().ok()) {
        let server_host = host_header.split(':').next().unwrap_or("");
        if host == server_host {
            return Ok(());
        }
    }

    Err("cross-origin WebSocket connection rejected")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{
        HeaderName,
        HeaderValue,
    };

    fn make_headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        map
    }

    #[test]
    fn missing_origin_is_allowed() {
        assert!(validate(&make_headers(&[("Host", "lyra.example.com")])).is_ok());
    }

    #[test]
    fn localhost_origin_is_allowed() {
        assert!(
            validate(&make_headers(&[
                ("Origin", "http://localhost:3000"),
                ("Host", "lyra.example.com"),
            ]))
            .is_ok()
        );
    }

    #[test]
    fn loopback_ipv4_is_allowed() {
        assert!(
            validate(&make_headers(&[
                ("Origin", "http://127.0.0.1:8000"),
                ("Host", "lyra.example.com"),
            ]))
            .is_ok()
        );
    }

    #[test]
    fn same_host_origin_is_allowed() {
        assert!(
            validate(&make_headers(&[
                ("Origin", "https://lyra.example.com"),
                ("Host", "lyra.example.com:443"),
            ]))
            .is_ok()
        );
    }

    #[test]
    fn cross_origin_is_rejected() {
        assert!(
            validate(&make_headers(&[
                ("Origin", "https://evil.example.com"),
                ("Host", "lyra.example.com"),
            ]))
            .is_err()
        );
    }
}
