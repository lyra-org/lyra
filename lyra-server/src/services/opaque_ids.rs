// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Shape checks for the opaque ids clients send.

use std::ops::RangeInclusive;

/// Whether `candidate` uses only `[A-Za-z0-9_-]` and has a length in `lengths`.
pub(crate) fn is_well_formed(candidate: &str, lengths: RangeInclusive<usize>) -> bool {
    lengths.contains(&candidate.len())
        && candidate
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Whether `candidate` could be a public entity id.
pub(crate) fn is_public_id(candidate: &str) -> bool {
    is_well_formed(candidate, 6..=64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_ids_accept_nanoid_shape() {
        assert!(is_public_id("V1StGXR8_Z5jdHi6B-myT"));
        assert!(is_public_id("abc123"));
    }

    #[test]
    fn public_ids_reject_malformed() {
        assert!(!is_public_id(""));
        assert!(!is_public_id("abc"));
        assert!(!is_public_id("has spaces"));
        assert!(!is_public_id("has/slashes"));
        assert!(!is_public_id(&"x".repeat(100)));
    }

    #[test]
    fn well_formed_respects_the_length_range() {
        assert!(is_well_formed(&"a".repeat(16), 16..=64));
        assert!(is_well_formed(&"a".repeat(64), 16..=64));
        assert!(!is_well_formed(&"a".repeat(15), 16..=64));
        assert!(!is_well_formed(&"a".repeat(65), 16..=64));
        assert!(!is_well_formed("ünïcode-not-ascii", 1..=64));
    }
}
