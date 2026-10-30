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

/// A blank `needle` keeps every entry; one with nothing to match, like `^^^`,
/// keeps none. `text_of` and `set_score` must not call back into this.
pub(crate) fn fuzzy_filter<T>(
    entries: &mut Vec<T>,
    needle: &str,
    text_of: impl Fn(&T) -> &str,
    set_score: impl Fn(&mut T, u32),
) {
    fuzzy_retain(entries, needle, |entry, score| {
        match score(text_of(entry)) {
            Some(value) => {
                set_score(entry, value);
                true
            }
            None => false,
        }
    });
}

/// Keeps the entries `keep` accepts, handing it a scorer for any of the entry's texts, under the
/// same needle rules as [`fuzzy_filter`]. `keep` must not call back into this.
pub(crate) fn fuzzy_retain<T>(
    entries: &mut Vec<T>,
    needle: &str,
    mut keep: impl FnMut(&mut T, &mut dyn FnMut(&str) -> Option<u32>) -> bool,
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
        let mut score = |text: &str| pattern.score(Utf32Str::new(text, &mut buf), matcher);
        entries.retain_mut(|entry| keep(entry, &mut score));
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
