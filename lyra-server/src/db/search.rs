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

/// Filter and score `entries` against `needle` using a thread-local nucleo matcher.
///
/// Two reentrancy hazards to watch for:
///
/// 1. The `text_of` / `set_score` closures must NOT call `fuzzy_filter` again,
///    directly or transitively. The matcher lives in a `RefCell` borrowed for
///    the whole `retain_mut` pass; recursive entry will panic on
///    `RefCell::borrow_mut`.
/// 2. Do not introduce any `.await` (or any other point that yields control
///    back to the tokio scheduler) inside the borrow region — the synchronous
///    closure above is intentional. A future maintainer adding async work here
///    invites the matcher being held across a yield point, at which a
///    different future polled on the same task could reenter and panic.
///
/// Cross-task / cross-thread parallelism is fine: the matcher is thread-local,
/// and `tokio::join!` of two callers in different tasks (or `tokio::spawn`'d
/// onto separate workers) each gets its own matcher instance.
///
/// Behavior on inputs that produce no atoms:
///
/// - Empty / whitespace-only `needle` → no-op (caller didn't supply a query).
/// - Non-empty `needle` whose `Pattern::parse` produces zero atoms (e.g.
///   `^^^`, `!!`) → entries are cleared. This avoids a silent enumeration
///   oracle where garbage queries return the unfiltered list.
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
        // Pinned to CaseMatching::Ignore — uppercase needle must match
        // lowercase content the same as lowercase needle.
        let lower = run("blue", vec!["Blue Train", "Red Album"]);
        let upper = run("BLUE", vec!["Blue Train", "Red Album"]);
        assert_eq!(lower, upper);
        assert_eq!(lower, vec!["Blue Train"]);
    }
}
