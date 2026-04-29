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

    let pattern = Pattern::parse(trimmed, CaseMatching::Smart, Normalization::Smart);
    if pattern.atoms.is_empty() {
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
