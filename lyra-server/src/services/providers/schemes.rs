// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

/// Checks that `scheme` is `namespace:kind` or a bare standard name, where
/// each part is lowercase ASCII letters, digits, `-` or `_`, starting with a
/// letter.
pub(crate) fn validate_id_scheme(scheme: &str) -> Result<(), String> {
    let mut parts = scheme.split(':');
    let valid = parts.next().is_some_and(is_valid_part)
        && parts.next().is_none_or(is_valid_part)
        && parts.next().is_none();
    if valid {
        Ok(())
    } else {
        Err(format!(
            "scheme '{scheme}' must be lowercase 'namespace:kind' or a bare name (e.g. 'example:thing', 'isrc')"
        ))
    }
}

fn is_valid_part(part: &str) -> bool {
    let mut chars = part.chars();
    chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_namespaced_and_bare_schemes() {
        for scheme in ["example:thing", "example:thing-group", "isrc", "a1:b_2"] {
            assert!(validate_id_scheme(scheme).is_ok(), "{scheme}");
        }
    }

    #[test]
    fn rejects_malformed_schemes() {
        for scheme in [
            "",
            "Example:thing",
            "example:",
            ":thing",
            "example:thing:extra",
            "example.thing",
            "example thing",
            "1example",
            "example:-thing",
        ] {
            assert!(validate_id_scheme(scheme).is_err(), "{scheme}");
        }
    }
}
