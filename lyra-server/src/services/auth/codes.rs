// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Codes a person reads off one screen and types into another.

use argon2::password_hash::rand_core::{
    OsRng,
    RngCore,
};

/// A-Z without I, L and O, which read as digits or as each other.
pub(super) const LETTERS: &[u8; 23] = b"ABCDEFGHJKMNPQRSTUVWXYZ";

/// A value in `0..bound` from `next`. Values at or past the last whole multiple of `bound` are
/// drawn again, since keeping them would favour the small results.
fn sample_below(bound: u32, mut next: impl FnMut() -> u32) -> u32 {
    let limit = u32::MAX - u32::MAX % bound;
    loop {
        let value = next();
        if value < limit {
            return value % bound;
        }
    }
}

fn random_below(bound: u32) -> u32 {
    sample_below(bound, || OsRng.next_u32())
}

/// `length` of [`LETTERS`], each drawn evenly.
pub(super) fn generate(length: usize) -> String {
    (0..length)
        .map(|_| char::from(LETTERS[random_below(LETTERS.len() as u32) as usize]))
        .collect()
}

/// A typed code as it is compared: upper case, without whitespace.
pub(super) fn normalize(code: &str) -> String {
    code.chars()
        .filter(|c| !c.is_whitespace())
        .map(|c| c.to_ascii_uppercase())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sampling_rejects_values_past_the_last_whole_multiple() {
        let mut values = [u32::MAX, u32::MAX - 1, 7].into_iter();
        assert_eq!(
            sample_below(10, || values.next().expect("a value is accepted")),
            7
        );
    }

    #[test]
    fn generated_codes_are_the_asked_length_of_the_letters() {
        for _ in 0..500 {
            let code = generate(6);
            assert_eq!(code.len(), 6);
            assert!(code.bytes().all(|byte| LETTERS.contains(&byte)));
        }
    }

    #[test]
    fn letters_leave_out_the_ambiguous_ones() {
        for letter in [b'I', b'L', b'O'] {
            assert!(!LETTERS.contains(&letter));
        }
    }

    #[test]
    fn codes_normalize_whitespace_and_case() {
        assert_eq!(normalize(" ab c\tde\nf "), "ABCDEF");
        assert_eq!(normalize("123 456"), "123456");
    }
}
