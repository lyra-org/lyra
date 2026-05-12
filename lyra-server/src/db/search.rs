// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::cell::RefCell;

use nucleo_matcher::{
    Config,
    Matcher,
    Utf32Str,
    pattern::{
        CaseMatching,
        Normalization,
        Pattern,
    },
};

thread_local! {
    static MATCHER: RefCell<Matcher> = RefCell::new(Matcher::new(Config::DEFAULT));
}

/// Empty/whitespace `needle` is a no-op; a non-empty `needle` that parses to
/// zero atoms (e.g. `^^^`) clears `entries` rather than leaking the unfiltered
/// list. Not reentrant: the matcher is held across `retain_mut`, so `text_of`
/// and `set_score` must not call back in, and no `.await` may be added inside.
pub(crate) fn fuzzy_filter<T>(
    entries: &mut Vec<T>,
    needle: &str,
    text_of: impl Fn(&T) -> &str,
    set_score: impl Fn(&mut T, u32),
) {
    let trimmed = needle.trim();
    if trimmed.is_empty() {
        return;
    }

    let pattern = Pattern::parse(trimmed, CaseMatching::Ignore, Normalization::Smart);
    if pattern.atoms.is_empty() {
        entries.clear();
        return;
    }

    MATCHER.with_borrow_mut(|matcher| {
        let mut buf = Vec::new();
        entries.retain_mut(|entry| {
            let utf32 = Utf32Str::new(text_of(entry), &mut buf);
            match pattern.score(utf32, matcher) {
                Some(score) => {
                    set_score(entry, score);
                    true
                }
                None => false,
            }
        });
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(needle: &str, mut entries: Vec<&'static str>) -> Vec<&'static str> {
        fuzzy_filter(&mut entries, needle, |s| *s, |_, _| {});
        entries
    }

    #[test]
    fn empty_needle_is_a_noop() {
        let result = run("", vec!["Alpha", "Beta"]);
        assert_eq!(result, vec!["Alpha", "Beta"]);
    }

    #[test]
    fn whitespace_needle_is_a_noop() {
        let result = run("   ", vec!["Alpha", "Beta"]);
        assert_eq!(result, vec!["Alpha", "Beta"]);
    }

    #[test]
    fn non_empty_needle_with_zero_atoms_clears_entries() {
        let result = run("^^^", vec!["Alpha", "Beta"]);
        assert!(
            result.is_empty(),
            "garbage needle must not behave like empty needle"
        );
    }

    #[test]
    fn case_matching_is_case_insensitive() {
        let lower = run("blue", vec!["Blue Train", "Red Album"]);
        let upper = run("BLUE", vec!["Blue Train", "Red Album"]);
        assert_eq!(lower, upper);
        assert_eq!(lower, vec!["Blue Train"]);
    }
}
