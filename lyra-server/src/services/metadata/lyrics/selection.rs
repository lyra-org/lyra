// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use agdb::DbId;

use crate::db::{
    self,
    DbAccess,
    IdSource,
    Lyrics,
    ProviderConfig,
    lyrics::LyricsDetail,
};

/// Coverage threshold for the selector's synced tier.
const MEANINGFUL_SYNCED_COVERAGE: f64 = 0.5;

fn priority_map(providers: &[ProviderConfig]) -> HashMap<&str, u32> {
    providers
        .iter()
        .filter(|p| p.enabled)
        .map(|p| (p.provider_id.as_str(), p.priority))
        .collect()
}

/// Minimum non-zero-ts lines for the selector's synced tier.
const MIN_SYNCED_LINES: u32 = 2;

fn has_meaningful_synced(lyrics: &Lyrics, duration_ms: Option<u64>) -> bool {
    if lyrics.synced_line_count < MIN_SYNCED_LINES || lyrics.max_synced_ts_ms == 0 {
        return false;
    }
    let Some(duration_ms) = duration_ms else {
        return true;
    };
    if duration_ms == 0 {
        return true;
    }
    let threshold = (duration_ms as f64 * MEANINGFUL_SYNCED_COVERAGE) as u64;
    lyrics.max_synced_ts_ms >= threshold
}

fn language_matches(stored: &str, hint: &str) -> bool {
    stored.eq_ignore_ascii_case(hint.trim())
}

/// Ordering (first-difference wins): `is_user_override`, `language_match`,
/// `has_meaningful_synced` (≥ `MIN_SYNCED_LINES` non-zero-ts lines AND
/// `max_synced_ts_ms` ≥ 50% of duration), `provider_priority`,
/// `updated_at`, `provider_id` asc.
///
/// `require_synced = true` filters plain candidates pre-rank for `format=lrc`.
pub(crate) fn pick_preferred<'a>(
    candidates: &'a [Lyrics],
    providers: &[ProviderConfig],
    language_hint: Option<&str>,
    duration_ms: Option<u64>,
    require_synced: bool,
) -> Option<&'a Lyrics> {
    let priorities = priority_map(providers);

    candidates
        .iter()
        .filter(|lyrics| {
            matches!(lyrics.origin, IdSource::User)
                || priorities.contains_key(lyrics.provider_id.as_str())
        })
        .filter(|lyrics| !require_synced || has_meaningful_synced(lyrics, duration_ms))
        .max_by(|a, b| {
            let a_user = u8::from(matches!(a.origin, IdSource::User));
            let b_user = u8::from(matches!(b.origin, IdSource::User));

            let a_lang = language_hint.is_some_and(|h| language_matches(&a.language, h));
            let b_lang = language_hint.is_some_and(|h| language_matches(&b.language, h));

            let a_synced = has_meaningful_synced(a, duration_ms);
            let b_synced = has_meaningful_synced(b, duration_ms);

            let a_priority = priorities.get(a.provider_id.as_str()).copied().unwrap_or(0);
            let b_priority = priorities.get(b.provider_id.as_str()).copied().unwrap_or(0);

            a_user
                .cmp(&b_user)
                .then_with(|| a_lang.cmp(&b_lang))
                .then_with(|| a_synced.cmp(&b_synced))
                .then_with(|| a_priority.cmp(&b_priority))
                .then_with(|| a.updated_at.cmp(&b.updated_at))
                // Lexical tie-break inverted: max_by takes the greater, but we
                // want the lexically-smaller provider_id to win.
                .then_with(|| b.provider_id.cmp(&a.provider_id))
        })
}

pub(crate) fn get_preferred_detail(
    db: &impl DbAccess,
    track_db_id: DbId,
    language_hint: Option<&str>,
    require_synced: bool,
) -> anyhow::Result<Option<LyricsDetail>> {
    let Some(track) = db::tracks::get_by_id(db, track_db_id)? else {
        return Ok(None);
    };
    let candidates = db::lyrics::get_for_track(db, track_db_id)?;
    let providers = db::providers::get(db)?;

    let Some(winner) = pick_preferred(
        &candidates,
        &providers,
        language_hint,
        track.duration_ms,
        require_synced,
    ) else {
        return Ok(None);
    };
    let Some(winner_db_id) = winner.db_id.clone().map(Into::into) else {
        return Ok(None);
    };

    db::lyrics::get_detail(db, winner_db_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lyrics(
        id: &str,
        provider_id: &str,
        language: &str,
        origin: IdSource,
        line_count: u32,
        max_synced_ts_ms: u64,
        updated_at: u64,
    ) -> Lyrics {
        // Tests needing the degenerate "one real ts" case build the struct explicitly.
        let synced_line_count = if max_synced_ts_ms > 0 { line_count } else { 0 };
        Lyrics {
            db_id: None,
            id: id.to_string(),
            provider_id: provider_id.to_string(),
            language: language.to_string(),
            origin,
            plain_text: String::new(),
            line_count,
            synced_line_count,
            max_synced_ts_ms,
            has_word_cues: false,
            content_hash: String::new(),
            last_checked_at: updated_at,
            updated_at,
        }
    }

    fn provider(provider_id: &str, priority: u32) -> ProviderConfig {
        ProviderConfig {
            db_id: None,
            id: provider_id.to_string(),
            provider_id: provider_id.to_string(),
            display_name: provider_id.to_string(),
            priority,
            enabled: true,
        }
    }

    fn disabled(provider_id: &str, priority: u32) -> ProviderConfig {
        ProviderConfig {
            db_id: None,
            id: provider_id.to_string(),
            provider_id: provider_id.to_string(),
            display_name: provider_id.to_string(),
            priority,
            enabled: false,
        }
    }

    #[test]
    fn user_override_beats_everything() {
        let cands = vec![
            lyrics("p1", "plug", "eng", IdSource::Plugin, 100, 180_000, 2000),
            lyrics("u1", "user", "eng", IdSource::User, 0, 0, 1000),
        ];
        let providers = vec![provider("plug", 100)];
        let winner = pick_preferred(&cands, &providers, Some("eng"), Some(200_000), false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("u1"));
    }

    #[test]
    fn language_match_beats_mismatch() {
        let cands = vec![
            lyrics("p1", "plug", "fra", IdSource::Plugin, 50, 120_000, 2000),
            lyrics("p2", "other", "eng", IdSource::Plugin, 0, 0, 1000),
        ];
        let providers = vec![provider("plug", 100), provider("other", 50)];
        let winner = pick_preferred(&cands, &providers, Some("eng"), Some(200_000), false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("p2"));
    }

    #[test]
    fn language_match_is_case_insensitive() {
        let cands = vec![
            lyrics("p1", "plug", "fra", IdSource::Plugin, 0, 0, 2000),
            lyrics("p2", "other", "ENG", IdSource::Plugin, 0, 0, 1000),
        ];
        let providers = vec![provider("plug", 100), provider("other", 50)];
        let winner = pick_preferred(&cands, &providers, Some("eng"), None, false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("p2"));
    }

    #[test]
    fn meaningful_synced_beats_plain_across_providers() {
        let cands = vec![
            lyrics("p1", "high", "eng", IdSource::Plugin, 0, 0, 2000),
            lyrics("p2", "low", "eng", IdSource::Plugin, 80, 120_000, 1000),
        ];
        let providers = vec![provider("high", 100), provider("low", 50)];
        let winner = pick_preferred(&cands, &providers, Some("eng"), Some(200_000), false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("p2"));
    }

    #[test]
    fn synced_requires_coverage_threshold() {
        // Synced but last ts covers only 10% of duration; plain wins on updated_at.
        let cands = vec![
            lyrics("thin", "plug", "eng", IdSource::Plugin, 2, 20_000, 2000),
            lyrics("plain", "plug", "eng", IdSource::Plugin, 0, 0, 3000),
        ];
        let providers = vec![provider("plug", 100)];
        let winner = pick_preferred(&cands, &providers, Some("eng"), Some(200_000), false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("plain"));
    }

    #[test]
    fn single_forged_timestamp_does_not_qualify_as_synced() {
        // One pinned line + 49 zero-ts: max gate would accept, synced_line_count rejects.
        let forged = Lyrics {
            db_id: None,
            id: "forged".to_string(),
            provider_id: "plug".to_string(),
            language: "eng".to_string(),
            origin: IdSource::Plugin,
            plain_text: String::new(),
            line_count: 50,
            synced_line_count: 1,
            max_synced_ts_ms: 180_000,
            has_word_cues: false,
            content_hash: String::new(),
            last_checked_at: 2000,
            updated_at: 2000,
        };
        let honest_plain = lyrics("plain", "plug", "eng", IdSource::Plugin, 0, 0, 1000);
        let cands = vec![forged, honest_plain];
        let providers = vec![provider("plug", 100)];
        let winner = pick_preferred(&cands, &providers, None, Some(200_000), false);
        assert_eq!(
            winner.map(|w| w.id.as_str()),
            Some("forged"),
            "updated_at wins tie"
        );
        let synced_only = pick_preferred(&cands, &providers, None, Some(200_000), true);
        assert!(
            synced_only.is_none(),
            "one forged timestamp must not satisfy require_synced"
        );
    }

    #[test]
    fn all_zero_timestamps_are_not_meaningful_synced() {
        let cands = vec![
            lyrics("zeros", "plug", "eng", IdSource::Plugin, 50, 0, 2000),
            lyrics("plain", "plug", "eng", IdSource::Plugin, 0, 0, 1000),
        ];
        let providers = vec![provider("plug", 100)];
        let winner = pick_preferred(&cands, &providers, None, Some(200_000), false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("zeros"));
        let synced_only = pick_preferred(&cands, &providers, None, Some(200_000), true);
        assert!(synced_only.is_none());
    }

    #[test]
    fn require_synced_filters_plain_candidates_regardless_of_rank() {
        let cands = vec![
            // User override would win overall, but it's plain.
            lyrics("u1", "user", "eng", IdSource::User, 0, 0, 5000),
            lyrics("syn", "plug", "eng", IdSource::Plugin, 80, 120_000, 1000),
        ];
        let providers = vec![provider("plug", 100)];
        let winner = pick_preferred(&cands, &providers, Some("eng"), Some(200_000), true);
        assert_eq!(
            winner.map(|w| w.id.as_str()),
            Some("syn"),
            "LRC requests must fall through plain winners to real synced data"
        );
    }

    #[test]
    fn require_synced_returns_none_when_no_synced_candidate_exists() {
        let cands = vec![lyrics("plain", "plug", "eng", IdSource::Plugin, 0, 0, 1000)];
        let providers = vec![provider("plug", 100)];
        let winner = pick_preferred(&cands, &providers, None, Some(200_000), true);
        assert!(winner.is_none());
    }

    #[test]
    fn provider_priority_then_recency_then_provider_id() {
        let cands = vec![
            lyrics("a", "aa", "eng", IdSource::Plugin, 0, 0, 1000),
            lyrics("b", "bb", "eng", IdSource::Plugin, 0, 0, 2000),
            lyrics("c", "cc", "eng", IdSource::Plugin, 0, 0, 2000),
        ];
        // Newer pair ties; lex-smaller provider_id breaks it.
        let providers = vec![provider("aa", 10), provider("bb", 10), provider("cc", 10)];
        let winner = pick_preferred(&cands, &providers, None, None, false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("b"));
    }

    #[test]
    fn disabled_provider_is_excluded() {
        let cands = vec![
            lyrics("dis", "off", "eng", IdSource::Plugin, 100, 180_000, 5000),
            lyrics("on", "live", "eng", IdSource::Plugin, 0, 0, 1000),
        ];
        let providers = vec![disabled("off", 100), provider("live", 50)];
        let winner = pick_preferred(&cands, &providers, None, Some(200_000), false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("on"));
    }

    #[test]
    fn user_override_accepted_even_when_provider_not_configured() {
        let cands = vec![lyrics("u1", "user", "eng", IdSource::User, 0, 0, 1000)];
        let providers = vec![];
        let winner = pick_preferred(&cands, &providers, None, None, false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("u1"));
    }

    #[test]
    fn empty_candidates_returns_none() {
        let cands: Vec<Lyrics> = vec![];
        let providers = vec![];
        assert!(pick_preferred(&cands, &providers, None, None, false).is_none());
    }

    #[test]
    fn unknown_duration_treats_synced_with_nonzero_ts_as_meaningful() {
        let cands = vec![
            lyrics("thin", "plug", "eng", IdSource::Plugin, 2, 20_000, 2000),
            lyrics("plain", "plug", "eng", IdSource::Plugin, 0, 0, 3000),
        ];
        let providers = vec![provider("plug", 100)];
        let winner = pick_preferred(&cands, &providers, None, None, false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("thin"));
    }
}
