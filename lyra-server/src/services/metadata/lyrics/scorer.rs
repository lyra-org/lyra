// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Frozen-threshold gate between provider candidates and the lyrics store.

use unicode_normalization::UnicodeNormalization;

/// Title token-set ratio threshold (long form).
const TITLE_TOKEN_RATIO_MIN: f64 = 0.85;
/// Artist token-set ratio threshold (long form).
const ARTIST_TOKEN_RATIO_MIN: f64 = 0.7;
const DURATION_DELTA_MS_MAX: u64 = 5_000;
/// ≤ this many tokens forces exact-match.
const SHORT_TOKEN_LIMIT: usize = 2;
/// Any artist token ≤ this many chars forces exact-match (catches "U2", "Yes").
const SHORT_TOKEN_CHAR_LIMIT: usize = 3;

#[derive(Clone, Debug)]
pub(crate) struct LocalTrackContext {
    pub track_title: String,
    pub artist_name: String,
    pub duration_ms: Option<u64>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct LyricsHandlerCandidate {
    pub lyrics: crate::plugins::lyrics::PluginLyricsInput,
    pub title: String,
    pub artist: String,
    /// `None` fails the duration gate.
    pub duration_ms: Option<u64>,
    /// Tie-break only; mismatch never rejects.
    pub language: Option<String>,
}

pub(crate) struct ScoreInput<'a> {
    pub local_track: &'a LocalTrackContext,
    pub candidates: &'a [LyricsHandlerCandidate],
    pub preferred_languages: &'a [String],
}

#[derive(Clone, Copy, Debug)]
struct GateScores {
    title_score: f64,
    artist_score: f64,
    duration_delta_ms: u64,
}

/// Tie-break: preferred language (in list order, present beats absent),
/// then title_score, artist_score, smaller duration_delta_ms.
pub(crate) fn pick_best<'a>(input: ScoreInput<'a>) -> Option<&'a LyricsHandlerCandidate> {
    let local_title_norm = normalize_for_compare(&input.local_track.track_title);
    let local_artist_norm = normalize_for_compare(&input.local_track.artist_name);
    let local_title_tokens = tokenize(&local_title_norm);
    let local_artist_tokens = tokenize(&local_artist_norm);

    let mut passing: Vec<(&LyricsHandlerCandidate, GateScores)> = Vec::new();

    for candidate in input.candidates {
        let cand_title_norm = normalize_for_compare(&candidate.title);
        let cand_artist_norm = normalize_for_compare(&candidate.artist);
        let cand_title_tokens = tokenize(&cand_title_norm);
        let cand_artist_tokens = tokenize(&cand_artist_norm);

        let title_score = title_gate_score(
            &local_title_norm,
            &cand_title_norm,
            &local_title_tokens,
            &cand_title_tokens,
        );
        let Some(title_score) = title_score else {
            continue;
        };

        let artist_score = artist_gate_score(
            &local_artist_norm,
            &cand_artist_norm,
            &local_artist_tokens,
            &cand_artist_tokens,
        );
        let Some(artist_score) = artist_score else {
            continue;
        };

        let Some(local_duration) = input.local_track.duration_ms else {
            // Reject rather than silently accept when we can't gate.
            continue;
        };
        let Some(cand_duration) = candidate.duration_ms else {
            continue;
        };
        let delta = local_duration.abs_diff(cand_duration);
        if delta > DURATION_DELTA_MS_MAX {
            continue;
        }

        passing.push((
            candidate,
            GateScores {
                title_score,
                artist_score,
                duration_delta_ms: delta,
            },
        ));
    }

    if passing.is_empty() {
        return None;
    }

    // 0 = best (earliest preferred-language match); None falls into the
    // unmatched bucket, which is only consulted when no preferred match exists.
    let lang_rank = |cand: &LyricsHandlerCandidate| -> Option<usize> {
        let cand_lang = cand.language.as_deref()?;
        input
            .preferred_languages
            .iter()
            .position(|p| language_matches(cand_lang, p))
    };

    let any_preferred = passing.iter().any(|(c, _)| lang_rank(c).is_some());
    let pool: Vec<&(&LyricsHandlerCandidate, GateScores)> = if any_preferred {
        passing
            .iter()
            .filter(|(c, _)| lang_rank(c).is_some())
            .collect()
    } else {
        passing.iter().collect()
    };

    pool.into_iter()
        .min_by(|a, b| {
            let a_rank = lang_rank(a.0).unwrap_or(usize::MAX);
            let b_rank = lang_rank(b.0).unwrap_or(usize::MAX);
            a_rank
                .cmp(&b_rank)
                // Higher title_score wins → reverse cmp.
                .then_with(|| {
                    b.1.title_score
                        .partial_cmp(&a.1.title_score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| {
                    b.1.artist_score
                        .partial_cmp(&a.1.artist_score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| a.1.duration_delta_ms.cmp(&b.1.duration_delta_ms))
        })
        .map(|(cand, _)| *cand)
}

/// `Some(score)` when the gate passes; `None` otherwise. Exact-match short-
/// form returns 1.0 so it tie-breaks above any fuzzy pass.
fn title_gate_score(
    local_norm: &str,
    cand_norm: &str,
    local_tokens: &[&str],
    cand_tokens: &[&str],
) -> Option<f64> {
    if local_tokens.len() <= SHORT_TOKEN_LIMIT {
        if local_norm == cand_norm {
            Some(1.0)
        } else {
            None
        }
    } else {
        let ratio = token_set_ratio(local_tokens, cand_tokens);
        if ratio >= TITLE_TOKEN_RATIO_MIN {
            Some(ratio)
        } else {
            None
        }
    }
}

fn artist_gate_score(
    local_norm: &str,
    cand_norm: &str,
    local_tokens: &[&str],
    cand_tokens: &[&str],
) -> Option<f64> {
    let force_exact = local_tokens.len() <= SHORT_TOKEN_LIMIT
        || local_tokens
            .iter()
            .any(|t| t.chars().count() <= SHORT_TOKEN_CHAR_LIMIT);
    if force_exact {
        if local_norm == cand_norm {
            Some(1.0)
        } else {
            None
        }
    } else {
        let ratio = token_set_ratio(local_tokens, cand_tokens);
        if ratio >= ARTIST_TOKEN_RATIO_MIN {
            Some(ratio)
        } else {
            None
        }
    }
}

/// |intersection| / |union| over deduplicated token sets. 1.0 when both
/// sides are empty.
fn token_set_ratio(a: &[&str], b: &[&str]) -> f64 {
    use std::collections::HashSet;
    let set_a: HashSet<&str> = a.iter().copied().collect();
    let set_b: HashSet<&str> = b.iter().copied().collect();
    if set_a.is_empty() && set_b.is_empty() {
        return 1.0;
    }
    let inter = set_a.intersection(&set_b).count();
    let union = set_a.union(&set_b).count();
    if union == 0 {
        0.0
    } else {
        inter as f64 / union as f64
    }
}

fn tokenize(normalized: &str) -> Vec<&str> {
    normalized
        .split_whitespace()
        .filter(|t| !t.is_empty())
        .collect()
}

/// Normalise: NFKC, ASCII-casefold via `to_lowercase` (Unicode-aware in the
/// std lib), strip punctuation, collapse whitespace.
fn normalize_for_compare(input: &str) -> String {
    let nfkc: String = input.nfkc().collect();
    let lower = nfkc.to_lowercase();
    let mut out = String::with_capacity(lower.len());
    let mut last_space = true;
    for ch in lower.chars() {
        if ch.is_alphanumeric() {
            out.push(ch);
            last_space = false;
        } else if ch.is_whitespace() {
            if !last_space {
                out.push(' ');
                last_space = true;
            }
        } else {
            // Punctuation breaks tokens — "rock'n'roll" → 3 tokens.
            if !last_space {
                out.push(' ');
                last_space = true;
            }
        }
    }
    let trimmed = out.trim();
    trimmed.to_string()
}

fn language_matches(stored: &str, hint: &str) -> bool {
    stored.trim().eq_ignore_ascii_case(hint.trim())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::lyrics::PluginLyricsInput;

    fn lyrics_payload(id: &str, language: &str) -> PluginLyricsInput {
        PluginLyricsInput {
            id: id.to_string(),
            language: language.to_string(),
            plain_text: String::new(),
            lines: Vec::new(),
        }
    }

    fn cand(
        title: &str,
        artist: &str,
        duration_ms: Option<u64>,
        language: Option<&str>,
    ) -> LyricsHandlerCandidate {
        LyricsHandlerCandidate {
            lyrics: lyrics_payload("payload", language.unwrap_or("und")),
            title: title.to_string(),
            artist: artist.to_string(),
            duration_ms,
            language: language.map(str::to_string),
        }
    }

    fn local(title: &str, artist: &str, duration_ms: Option<u64>) -> LocalTrackContext {
        LocalTrackContext {
            track_title: title.to_string(),
            artist_name: artist.to_string(),
            duration_ms,
        }
    }

    #[test]
    fn rejects_artist_mismatch_short_title_short_artist() {
        // Pearl Jam's "Black" vs Sabbath's "Black": title exact-matches,
        // artist exact-matches and rejects.
        let local_track = local("Black", "Pearl Jam", Some(343_000));
        let candidates = vec![cand("Black", "Black Sabbath", Some(343_000), Some("eng"))];
        let prefs: Vec<String> = vec![];
        assert!(
            pick_best(ScoreInput {
                local_track: &local_track,
                candidates: &candidates,
                preferred_languages: &prefs,
            })
            .is_none()
        );
    }

    #[test]
    fn rejects_title_mismatch_under_two_token_exact_rule() {
        // Short-form gate (≤2 tokens) requires exact normalised equality.
        let local_track = local("Yes", "Some Artist", Some(180_000));
        let candidates = vec![cand("Yes I Am", "Some Artist", Some(180_000), Some("eng"))];
        let prefs: Vec<String> = vec![];
        assert!(
            pick_best(ScoreInput {
                local_track: &local_track,
                candidates: &candidates,
                preferred_languages: &prefs,
            })
            .is_none()
        );
    }

    #[test]
    fn rejects_duration_delta_above_threshold() {
        let local_track = local(
            "A Long Song Title Here",
            "An Artist Group Name",
            Some(180_000),
        );
        // 6 seconds off — over the 5s allowance.
        let candidates = vec![cand(
            "A Long Song Title Here",
            "An Artist Group Name",
            Some(186_000),
            Some("eng"),
        )];
        let prefs: Vec<String> = vec![];
        assert!(
            pick_best(ScoreInput {
                local_track: &local_track,
                candidates: &candidates,
                preferred_languages: &prefs,
            })
            .is_none()
        );
    }

    #[test]
    fn rejects_candidate_without_duration() {
        let local_track = local(
            "A Long Song Title Here",
            "An Artist Group Name",
            Some(180_000),
        );
        let candidates = vec![cand(
            "A Long Song Title Here",
            "An Artist Group Name",
            None,
            Some("eng"),
        )];
        let prefs: Vec<String> = vec![];
        assert!(
            pick_best(ScoreInput {
                local_track: &local_track,
                candidates: &candidates,
                preferred_languages: &prefs,
            })
            .is_none()
        );
    }

    #[test]
    fn accepts_obvious_match() {
        let local_track = local(
            "A Long Song Title Here",
            "An Artist Group Name",
            Some(180_000),
        );
        let candidates = vec![cand(
            "A Long Song Title Here",
            "An Artist Group Name",
            Some(180_500),
            Some("eng"),
        )];
        let prefs: Vec<String> = vec![];
        assert!(
            pick_best(ScoreInput {
                local_track: &local_track,
                candidates: &candidates,
                preferred_languages: &prefs,
            })
            .is_some()
        );
    }

    #[test]
    fn accepts_short_title_exact_long_artist_fuzzy() {
        // Artist escapes exact-match (>2 tokens, all >3 chars); 3/4 = 0.75
        // ≥ artist threshold.
        let local_track = local("Yes", "Massive Atomic Quartet Players", Some(180_000));
        let candidates = vec![cand(
            "Yes",
            "Massive Atomic Quartet",
            Some(180_000),
            Some("eng"),
        )];
        let prefs: Vec<String> = vec![];
        assert!(
            pick_best(ScoreInput {
                local_track: &local_track,
                candidates: &candidates,
                preferred_languages: &prefs,
            })
            .is_some()
        );
    }

    #[test]
    fn accepts_within_duration_window() {
        let local_track = local(
            "A Long Song Title Here",
            "An Artist Group Name",
            Some(180_000),
        );
        let candidates = vec![cand(
            "A Long Song Title Here",
            "An Artist Group Name",
            Some(184_999),
            Some("eng"),
        )];
        let prefs: Vec<String> = vec![];
        assert!(
            pick_best(ScoreInput {
                local_track: &local_track,
                candidates: &candidates,
                preferred_languages: &prefs,
            })
            .is_some()
        );
    }

    #[test]
    fn preferred_language_wins_over_higher_score() {
        // A has tighter duration but wrong language; B is preferred-language.
        // Tie-break must promote B.
        let local_track = local(
            "Some Reasonably Long Track Title",
            "An Artist Group Name",
            Some(180_000),
        );
        let candidates = vec![
            cand(
                "Some Reasonably Long Track Title",
                "An Artist Group Name",
                Some(180_000),
                Some("fra"),
            ),
            cand(
                // Identical title/artist so language is the only differing signal.
                "Some Reasonably Long Track Title",
                "An Artist Group Name",
                Some(181_000),
                Some("eng"),
            ),
        ];
        let prefs = vec!["eng".to_string()];
        let winner = pick_best(ScoreInput {
            local_track: &local_track,
            candidates: &candidates,
            preferred_languages: &prefs,
        })
        .expect("winner");
        assert_eq!(winner.language.as_deref(), Some("eng"));
    }

    #[test]
    fn tie_break_falls_through_to_score_when_no_preferred_match() {
        let local_track = local(
            "Some Slightly Fuzzy Title",
            "An Artist Group Name",
            Some(180_000),
        );
        let candidates = vec![
            cand(
                "Some Slightly Different Title",
                "An Artist Group Name",
                Some(180_000),
                Some("fra"),
            ),
            cand(
                "Some Slightly Fuzzy Title",
                "An Artist Group Name",
                Some(180_000),
                Some("deu"),
            ),
        ];
        let prefs = vec!["eng".to_string()];
        // Neither preferred; falls back to title_score.
        let winner = pick_best(ScoreInput {
            local_track: &local_track,
            candidates: &candidates,
            preferred_languages: &prefs,
        })
        .expect("winner");
        assert_eq!(winner.language.as_deref(), Some("deu"));
    }

    #[test]
    fn tie_break_falls_back_to_smaller_duration_delta() {
        let local_track = local(
            "Identical Title Here",
            "An Artist Group Name",
            Some(180_000),
        );
        let candidates = vec![
            cand(
                "Identical Title Here",
                "An Artist Group Name",
                Some(184_000),
                Some("eng"),
            ),
            cand(
                "Identical Title Here",
                "An Artist Group Name",
                Some(180_500),
                Some("eng"),
            ),
        ];
        let prefs = vec!["eng".to_string()];
        let winner = pick_best(ScoreInput {
            local_track: &local_track,
            candidates: &candidates,
            preferred_languages: &prefs,
        })
        .expect("winner");
        assert_eq!(winner.duration_ms, Some(180_500));
    }

    #[test]
    fn empty_candidates_returns_none() {
        let local_track = local("Title", "Artist", Some(180_000));
        let prefs: Vec<String> = vec![];
        assert!(
            pick_best(ScoreInput {
                local_track: &local_track,
                candidates: &[],
                preferred_languages: &prefs,
            })
            .is_none()
        );
    }

    #[test]
    fn local_without_duration_rejects_all() {
        let local_track = local("A Long Song Title Here", "An Artist Group Name", None);
        let candidates = vec![cand(
            "A Long Song Title Here",
            "An Artist Group Name",
            Some(180_000),
            Some("eng"),
        )];
        let prefs: Vec<String> = vec![];
        assert!(
            pick_best(ScoreInput {
                local_track: &local_track,
                candidates: &candidates,
                preferred_languages: &prefs,
            })
            .is_none()
        );
    }

    #[test]
    fn short_artist_token_forces_exact_match() {
        // "U2" hits the short-token rule and forces exact match.
        let local_track = local("Where The Streets Have No Name", "U2", Some(280_000));
        let candidates = vec![cand(
            "Where The Streets Have No Name",
            "U2 Band",
            Some(280_000),
            Some("eng"),
        )];
        let prefs: Vec<String> = vec![];
        assert!(
            pick_best(ScoreInput {
                local_track: &local_track,
                candidates: &candidates,
                preferred_languages: &prefs,
            })
            .is_none()
        );
    }

    #[test]
    fn tokenize_strips_punctuation() {
        let normalized = normalize_for_compare("Rock'n'Roll!");
        let tokens = tokenize(&normalized);
        assert_eq!(tokens, vec!["rock", "n", "roll"]);
    }

    #[test]
    fn nfkc_canonicalizes_compatibility_forms() {
        // ﬁ (U+FB01 LATIN SMALL LIGATURE FI) decomposes to "fi" under NFKC.
        let normalized = normalize_for_compare("ﬁnal");
        assert_eq!(normalized, "final");
    }
}
