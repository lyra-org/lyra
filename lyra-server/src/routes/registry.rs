// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use anyhow::{
    Result,
    bail,
};

static SUPPORTED_METHODS: &[&str] = &["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"];

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct RouteKey {
    pub(crate) method: Arc<str>,
    pub(crate) path: Arc<str>,
}

impl RouteKey {
    pub(crate) fn new(method: impl AsRef<str>, path: impl AsRef<str>) -> Result<Self> {
        let method = normalize_method(method.as_ref())?;
        let path = normalize_path(path.as_ref())?;
        Ok(Self {
            method: method.into(),
            path: path.into(),
        })
    }

    pub(crate) fn new_case_insensitive(
        method: impl AsRef<str>,
        path: impl AsRef<str>,
    ) -> Result<Self> {
        let method = normalize_method(method.as_ref())?;
        let path = normalize_path(path.as_ref())?;
        for segment in path.split('/') {
            validate_case_insensitive_segment(segment)?;
        }
        let path = lowercase_literal_segments(&path);
        Ok(Self {
            method: method.into(),
            path: path.into(),
        })
    }
}

pub(crate) fn is_placeholder_segment(segment: &str) -> bool {
    segment.starts_with('{') && !segment.starts_with("{{")
}

// Matchit is the final gatekeeper at insert time — this pre-check only
// rejects segments that mix literal and placeholder, which would break
// case-insensitive lowering.
fn validate_case_insensitive_segment(segment: &str) -> Result<()> {
    if segment.starts_with("{{") {
        return Ok(());
    }
    if !segment.contains('{') && !segment.contains('}') {
        return Ok(());
    }
    if segment.starts_with('{') && segment.ends_with('}') {
        let inner = &segment[1..segment.len() - 1];
        if !inner.contains('{') && !inner.contains('}') {
            return Ok(());
        }
    }
    bail!(
        "case-insensitive route segment must be a pure literal, '{{name}}', or '{{*name}}': '{segment}'"
    )
}

pub(crate) fn lowercase_literal_segments(path: &str) -> String {
    path.split('/')
        .map(|segment| {
            if is_placeholder_segment(segment) {
                segment.to_string()
            } else {
                segment.to_ascii_lowercase()
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn normalize_method(method: &str) -> Result<String> {
    let method = method.trim().to_ascii_uppercase();
    if SUPPORTED_METHODS
        .iter()
        .any(|supported| *supported == method)
    {
        Ok(method)
    } else {
        bail!(
            "unsupported method '{}'; expected one of {}",
            method,
            SUPPORTED_METHODS.join(", ")
        );
    }
}

fn normalize_path(path: &str) -> Result<String> {
    let path = path.trim();
    if path.is_empty() {
        bail!("path must not be empty");
    }
    if !path.starts_with('/') {
        bail!("path must start with '/'");
    }
    if path.contains('?') {
        bail!("path must not contain query string");
    }
    if path.contains("//") {
        bail!("path must not contain empty segments");
    }
    Ok(path.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_case_insensitive_lowercases_literals_preserves_placeholders() {
        let key = RouteKey::new_case_insensitive("GET", "/Users/{userId}/Items").unwrap();
        assert_eq!(key.path.as_ref(), "/users/{userId}/items");
    }

    #[test]
    fn new_case_insensitive_preserves_catchall() {
        let key = RouteKey::new_case_insensitive("GET", "/Files/{*rest}").unwrap();
        assert_eq!(key.path.as_ref(), "/files/{*rest}");
    }

    #[test]
    fn new_case_insensitive_treats_brace_escape_as_literal() {
        // matchit's `{{...}}` brace escape is literal text, not a placeholder.
        let key = RouteKey::new_case_insensitive("GET", "/A/{{Lit}}/B").unwrap();
        assert_eq!(key.path.as_ref(), "/a/{{lit}}/b");
    }

    #[test]
    fn new_case_insensitive_leaves_non_ascii_untouched() {
        let key = RouteKey::new_case_insensitive("GET", "/Café/Items").unwrap();
        assert_eq!(key.path.as_ref(), "/café/items");
    }

    #[test]
    fn new_case_insensitive_rejects_mixed_literal_and_placeholder() {
        let err = RouteKey::new_case_insensitive("GET", "/files/{id}.json").unwrap_err();
        assert!(
            err.to_string().contains("must be a pure literal"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn new_case_insensitive_rejects_prefix_then_placeholder() {
        let err = RouteKey::new_case_insensitive("GET", "/files/prefix{id}").unwrap_err();
        assert!(
            err.to_string().contains("must be a pure literal"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn new_case_insensitive_rejects_unclosed_placeholder() {
        let err = RouteKey::new_case_insensitive("GET", "/files/{id").unwrap_err();
        assert!(
            err.to_string().contains("must be a pure literal"),
            "unexpected error: {err}"
        );
    }
}
