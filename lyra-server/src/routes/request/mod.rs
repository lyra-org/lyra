// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! What a request presents about its client: its bearer credential, its address, and its origin.

mod address;
pub(crate) mod origin;

pub(crate) use address::client_address;

use axum::http::{
    HeaderMap,
    header::AUTHORIZATION,
};

/// The credential of a request's `Authorization: Bearer` header.
pub(crate) fn bearer_credential(headers: &HeaderMap) -> Option<&str> {
    let header = headers.get(AUTHORIZATION)?.to_str().ok()?;
    let (scheme, credential) = header.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    Some(credential.trim()).filter(|credential| !credential.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_credential_parser_rejects_non_bearer() {
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "Basic abc".parse().expect("valid header"));
        assert!(bearer_credential(&headers).is_none());
    }

    #[test]
    fn bearer_credential_parser_extracts_credential() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            "Bearer abc123".parse().expect("valid header"),
        );
        assert_eq!(bearer_credential(&headers), Some("abc123"));
    }

    #[test]
    fn extract_bearer_credential_requires_bearer() {
        let headers = HeaderMap::new();
        assert!(bearer_credential(&headers).is_none());
    }
}
