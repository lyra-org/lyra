// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Search matching. Each word of a query must match the start of a word in the searched text,
//! with case, diacritics, apostrophes and punctuation folded on both sides. A query word in a
//! script written without spaces matches anywhere in a word instead.

use unicode_normalization::{
    UnicodeNormalization,
    char::is_combining_mark,
};

/// `text` lowercased, without diacritics or apostrophes.
pub(crate) fn fold(text: &str) -> String {
    let mut folded = String::with_capacity(text.len());
    if text.is_ascii() {
        folded.extend(
            text.bytes()
                .filter(|byte| !matches!(byte, b'\'' | b'`'))
                .map(|byte| char::from(byte.to_ascii_lowercase())),
        );
        return folded;
    }
    for c in text.chars().filter(|c| !is_apostrophe(*c)).nfkd() {
        match c {
            // Kana voicing marks change the word, not its accent.
            '\u{3099}' | '\u{309A}' => folded.push(c),
            c if is_combining_mark(c) => {}
            'ß' | 'ẞ' => folded.push_str("ss"),
            'æ' | 'Æ' => folded.push_str("ae"),
            'œ' | 'Œ' => folded.push_str("oe"),
            'ø' | 'Ø' => folded.push('o'),
            'ł' | 'Ł' => folded.push('l'),
            'đ' | 'Đ' => folded.push('d'),
            'ı' => folded.push('i'),
            c => folded.extend(c.to_lowercase()),
        }
    }
    folded
}

fn is_apostrophe(c: char) -> bool {
    matches!(
        c,
        '\'' | '`' | '\u{00B4}' | '\u{2018}' | '\u{2019}' | '\u{02BB}' | '\u{02BC}'
    )
}

/// Whether `c` belongs to a script written without spaces between words.
fn unspaced(c: char) -> bool {
    matches!(
        u32::from(c),
        0x0E00..=0x0EFF   // Thai, Lao
        | 0x0F00..=0x0FFF // Tibetan
        | 0x1000..=0x109F // Myanmar
        | 0x1780..=0x17FF // Khmer
        | 0x3040..=0x30FF // Hiragana, Katakana
        | 0x31F0..=0x31FF
        | 0x3400..=0x4DBF // CJK ideographs
        | 0x4E00..=0x9FFF
        | 0xF900..=0xFAFF
        | 0xFF66..=0xFF9F
        | 0x20000..=0x3FFFF
    )
}

/// The words of `folded` text a query word can match the start of: each run between spaces, the
/// runs of letters and digits in it, and those runs joined, so `ac/dc` holds `ac/dc`, `ac`, `dc`
/// and `acdc`.
pub(crate) fn words(folded: &str, mut each: impl FnMut(&str)) {
    for chunk in folded.split_whitespace() {
        each(chunk);
        let mut runs = chunk
            .split(|c: char| !c.is_alphanumeric())
            .filter(|run| !run.is_empty());
        let Some(first) = runs.next() else {
            continue;
        };
        if first.len() == chunk.len() {
            continue;
        }
        each(first);
        let mut joined = None::<String>;
        for run in runs {
            each(run);
            joined
                .get_or_insert_with(|| first.to_string())
                .push_str(run);
        }
        if let Some(joined) = joined {
            each(&joined);
        }
    }
}

/// What exact and prefix matches compare: the letters and digits of `folded` text, or, for text
/// without any, the text without its spaces.
pub(crate) fn key(folded: &str) -> String {
    let key = folded
        .chars()
        .filter(|c| c.is_alphanumeric())
        .collect::<String>();
    if key.is_empty() {
        folded.chars().filter(|c| !c.is_whitespace()).collect()
    } else {
        key
    }
}

/// How well a search matches an entity, worst first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Tier {
    /// The words match only the entity's other texts, such as its artists' names.
    Related,
    /// Some words match the name or sort name and the rest only the entity's other texts.
    Partial,
    /// Every word starts a word of the name.
    Words,
    /// The name starts with the query.
    Prefix,
    /// The name is the query.
    Exact,
}

/// One word of a query.
#[derive(Debug)]
pub(crate) struct Word {
    literal: String,
    /// The letters and digits of a word that has other characters too.
    joined: Option<String>,
    unspaced: bool,
}

impl Word {
    /// The forms of the word one of which must start a word of the text, or none for a word that
    /// matches anywhere in a word instead.
    pub(crate) fn prefixes(&self) -> impl Iterator<Item = &str> {
        (!self.unspaced)
            .then(|| std::iter::once(self.literal.as_str()).chain(self.joined.as_deref()))
            .into_iter()
            .flatten()
    }

    /// The form of the word that must appear somewhere in a word of the text, for a word in a
    /// script written without spaces.
    pub(crate) fn infix(&self) -> Option<&str> {
        self.unspaced
            .then(|| self.joined.as_deref().unwrap_or(&self.literal))
    }

    /// Whether the word matches one of the words of `folded` text.
    pub(crate) fn matches(&self, word: &str) -> bool {
        match self.infix() {
            Some(infix) => word.contains(infix),
            None => self.prefixes().any(|prefix| word.starts_with(prefix)),
        }
    }
}

/// A parsed search query.
#[derive(Debug)]
pub(crate) struct Needle {
    words: Vec<Word>,
    key: String,
}

impl Needle {
    /// The query's words, or `None` for a blank query, which searches nothing out. A query whose
    /// words fold away, such as `'`, has no words and matches nothing.
    pub(crate) fn parse(query: &str) -> Option<Self> {
        let query = query.trim();
        if query.is_empty() {
            return None;
        }
        let folded = fold(query);
        let words = folded
            .split_whitespace()
            .map(|literal| {
                let joined = literal
                    .chars()
                    .filter(|c| c.is_alphanumeric())
                    .collect::<String>();
                Word {
                    unspaced: literal.chars().any(unspaced),
                    joined: (!joined.is_empty() && joined.len() != literal.len()).then_some(joined),
                    literal: literal.to_string(),
                }
            })
            .collect();
        Some(Self {
            words,
            key: key(&folded),
        })
    }

    pub(crate) fn words(&self) -> &[Word] {
        &self.words
    }

    /// How well a name whose words all match does, given the name's [`key`].
    pub(crate) fn tier(&self, name_key: &str) -> Tier {
        if name_key == self.key {
            Tier::Exact
        } else if name_key.starts_with(&self.key) {
            Tier::Prefix
        } else {
            Tier::Words
        }
    }

    /// How well `text` matches as a name, or `None` when some word misses it.
    pub(crate) fn score(&self, text: &str) -> Option<Tier> {
        if self.words.is_empty() {
            return None;
        }
        let folded = fold(text);
        let mut matched = vec![false; self.words.len()];
        words(&folded, |text_word| {
            for (matched, word) in matched.iter_mut().zip(&self.words) {
                *matched = *matched || word.matches(text_word);
            }
        });
        matched
            .into_iter()
            .all(|matched| matched)
            .then(|| self.tier(&key(&folded)))
    }
}

/// Keeps the entries whose text matches `query`, handing each its tier as a score where a higher
/// score is a better match. A blank query keeps every entry.
pub(crate) fn filter<T>(
    entries: &mut Vec<T>,
    query: &str,
    text_of: impl Fn(&T) -> &str,
    set_score: impl Fn(&mut T, u32),
) {
    let Some(needle) = Needle::parse(query) else {
        return;
    };
    entries.retain_mut(|entry| match needle.score(text_of(entry)) {
        Some(tier) => {
            set_score(entry, tier as u32);
            true
        }
        None => false,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(query: &str, mut entries: Vec<&'static str>) -> Vec<&'static str> {
        filter(&mut entries, query, |s| s, |_, _| {});
        entries
    }

    #[test]
    fn a_blank_query_keeps_every_entry() {
        assert_eq!(run("", vec!["Alpha", "Beta"]), vec!["Alpha", "Beta"]);
        assert_eq!(run("   ", vec!["Alpha", "Beta"]), vec!["Alpha", "Beta"]);
    }

    #[test]
    fn a_query_that_folds_away_matches_nothing() {
        assert!(run("'", vec!["Alpha", "Beta"]).is_empty());
    }

    #[test]
    fn every_word_must_start_a_word_of_the_text() {
        let names = vec!["Blue Train", "Red Album", "Bluebird", "True Blue"];
        assert_eq!(
            run("BLUE", names.clone()),
            vec!["Blue Train", "Bluebird", "True Blue"]
        );
        assert_eq!(run("tr bl", names.clone()), vec!["Blue Train", "True Blue"]);
        assert!(run("lue", names).is_empty());
    }

    #[test]
    fn diacritics_fold_in_both_directions() {
        assert_eq!(run("beyonce", vec!["Beyoncé", "Bey"]), vec!["Beyoncé"]);
        assert_eq!(run("beyoncé", vec!["Beyonce", "Bey"]), vec!["Beyonce"]);
        assert_eq!(run("motley crue", vec!["Mötley Crüe"]), vec!["Mötley Crüe"]);
        assert_eq!(run("sigur ros", vec!["Sigur Rós"]), vec!["Sigur Rós"]);
    }

    #[test]
    fn sharp_s_folds_to_ss_in_either_case() {
        let names = vec!["Straße", "STRAẞE", "Strasse", "Stras"];
        for query in ["STRASSE", "straße", "STRAẞE"] {
            assert_eq!(
                run(query, names.clone()),
                vec!["Straße", "STRAẞE", "Strasse"],
                "{query}"
            );
        }
    }

    #[test]
    fn punctuation_and_apostrophes_fold() {
        assert_eq!(run("ac dc", vec!["AC/DC"]), vec!["AC/DC"]);
        assert_eq!(run("acdc", vec!["AC/DC"]), vec!["AC/DC"]);
        assert_eq!(run("ac/dc", vec!["AC/DC"]), vec!["AC/DC"]);
        assert_eq!(
            run("guns n roses", vec!["Guns N' Roses"]),
            vec!["Guns N' Roses"]
        );
        assert_eq!(
            run("guns n' roses", vec!["Guns N’ Roses"]),
            vec!["Guns N’ Roses"]
        );
        assert_eq!(run("dont", vec!["Don’t Stop"]), vec!["Don’t Stop"]);
        assert_eq!(run("jay z", vec!["JAY-Z"]), vec!["JAY-Z"]);
    }

    #[test]
    fn operator_characters_search_literally() {
        let names = vec!["!!!", "$uicideboy$", "^Up", "Plain"];
        assert_eq!(run("!!!", names.clone()), vec!["!!!"]);
        assert_eq!(run("$uicideboy$", names.clone()), vec!["$uicideboy$"]);
        assert_eq!(run("uicideboy", names.clone()), vec!["$uicideboy$"]);
        assert_eq!(run("^up", names.clone()), vec!["^Up"]);
        assert_eq!(run("!!", names), vec!["!!!"]);
    }

    #[test]
    fn unspaced_scripts_match_anywhere() {
        let names = vec!["夜に駆ける", "優しい彗星", "Tokyo"];
        assert_eq!(run("駆ける", names.clone()), vec!["夜に駆ける"]);
        assert_eq!(run("彗星", names), vec!["優しい彗星"]);
    }

    #[test]
    fn exact_names_beat_prefixes_beat_words() {
        let needle = Needle::parse("Blue").expect("a query");
        assert_eq!(needle.score("blue"), Some(Tier::Exact));
        assert_eq!(needle.score("Bluebird"), Some(Tier::Prefix));
        assert_eq!(needle.score("True Blue"), Some(Tier::Words));
        assert_eq!(needle.score("Red"), None);
        let needle = Needle::parse("ac dc").expect("a query");
        assert_eq!(needle.score("AC/DC"), Some(Tier::Exact));
    }
}
