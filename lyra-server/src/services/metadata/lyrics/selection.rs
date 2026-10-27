// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use agdb::{
    DbAny,
    DbId,
};

use crate::services::auth::Principal;
use lyra_db::{
    Lyrics,
    ProviderConfig,
    lyrics::LyricsDetail,
    lyrics::LyricsKind,
};

/// Coverage threshold for the selector's synced tier.
const MEANINGFUL_SYNCED_COVERAGE: f64 = 0.5;

fn position_map(providers: &[ProviderConfig]) -> HashMap<&str, u32> {
    providers
        .iter()
        .filter(|p| p.enabled)
        .map(|p| (p.provider_id.as_str(), p.position))
        .collect()
}

/// Minimum non-zero-ts lines for the selector's synced tier.
const MIN_SYNCED_LINES: u32 = 2;

pub(crate) fn has_meaningful_synced(lyrics: &Lyrics, duration_ms: Option<u64>) -> bool {
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

/// Ownership uses [`Principal::require`].
pub(crate) fn visible_for_track(
    db: &DbAny,
    track_db_id: DbId,
    viewer: Option<&Principal>,
) -> anyhow::Result<Vec<Lyrics>> {
    let owner = viewer.map(|principal| principal.require(db)).transpose()?;
    lyra_db::lyrics::get_visible_for_track(db, track_db_id, owner)
}

fn is_eligible(lyrics: &Lyrics, positions: &HashMap<&str, u32>) -> bool {
    lyrics.kind != LyricsKind::Provider
        || lyrics
            .provider_id
            .as_deref()
            .is_some_and(|provider_id| positions.contains_key(provider_id))
}

pub(crate) fn normalize_language_hint(raw: Option<&str>) -> anyhow::Result<Option<String>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("und") {
        return Ok(Some("und".to_string()));
    }
    Ok(Some(lyra_metadata::locale::validate_language(trimmed)?))
}

pub(crate) fn eligible_candidates<'a>(
    candidates: &'a [Lyrics],
    providers: &[ProviderConfig],
    require_synced: bool,
    duration_ms: Option<u64>,
) -> Vec<&'a Lyrics> {
    let positions = position_map(providers);
    candidates
        .iter()
        .filter(|lyrics| is_eligible(lyrics, &positions))
        .filter(|lyrics| !require_synced || has_meaningful_synced(lyrics, duration_ms))
        .collect()
}

/// Ordering (first-difference wins): personal, shared manual, language match,
/// `has_meaningful_synced` (≥ `MIN_SYNCED_LINES` non-zero-ts lines AND
/// `max_synced_ts_ms` ≥ 50% of duration), provider position,
/// `updated_at`, `provider_id` asc.
///
/// `require_synced = true` filters plain candidates pre-rank for `format=lrc`.
///
/// `candidates` must already be visibility-filtered, e.g. by [`visible_for_track`].
pub(crate) fn pick_preferred<'a>(
    candidates: &'a [Lyrics],
    providers: &[ProviderConfig],
    provider_id: Option<&str>,
    language_hint: Option<&str>,
    duration_ms: Option<u64>,
    require_synced: bool,
) -> Option<&'a Lyrics> {
    let positions = position_map(providers);

    candidates
        .iter()
        .filter(|lyrics| is_eligible(lyrics, &positions))
        .filter(|lyrics| {
            provider_id.is_none_or(|provider_id| {
                lyrics.kind == LyricsKind::Provider
                    && lyrics.provider_id.as_deref() == Some(provider_id)
            })
        })
        .filter(|lyrics| !require_synced || has_meaningful_synced(lyrics, duration_ms))
        .max_by(|a, b| {
            let tier = |lyrics: &Lyrics| match lyrics.kind {
                LyricsKind::Provider => 0_u8,
                LyricsKind::Shared => 1,
                LyricsKind::Personal => 2,
            };
            let a_tier = tier(a);
            let b_tier = tier(b);

            let a_lang = language_hint.is_some_and(|h| language_matches(&a.language, h));
            let b_lang = language_hint.is_some_and(|h| language_matches(&b.language, h));

            let a_synced = has_meaningful_synced(a, duration_ms);
            let b_synced = has_meaningful_synced(b, duration_ms);

            let a_position = a
                .provider_id
                .as_deref()
                .and_then(|provider_id| positions.get(provider_id))
                .copied()
                .unwrap_or(u32::MAX);
            let b_position = b
                .provider_id
                .as_deref()
                .and_then(|provider_id| positions.get(provider_id))
                .copied()
                .unwrap_or(u32::MAX);

            a_tier
                .cmp(&b_tier)
                .then_with(|| a_lang.cmp(&b_lang))
                .then_with(|| a_synced.cmp(&b_synced))
                .then_with(|| b_position.cmp(&a_position))
                .then_with(|| a.updated_at.cmp(&b.updated_at))
                // Lexical tie-break inverted: max_by takes the greater, but we
                // want the lexically-smaller provider_id to win.
                .then_with(|| b.provider_id.cmp(&a.provider_id))
        })
}

pub(crate) fn get_preferred_detail(
    db: &DbAny,
    track_db_id: DbId,
    viewer: Option<&Principal>,
    provider_id: Option<&str>,
    language_hint: Option<&str>,
    require_synced: bool,
) -> anyhow::Result<Option<LyricsDetail>> {
    let Some(track) = lyra_db::tracks::get_by_id(db, track_db_id)? else {
        return Ok(None);
    };
    let candidates = visible_for_track(db, track_db_id, viewer)?;
    let providers = lyra_db::providers::get(db)?;
    let language_hint = normalize_language_hint(language_hint)?;

    let Some(winner) = pick_preferred(
        &candidates,
        &providers,
        provider_id,
        language_hint.as_deref(),
        track.duration_ms,
        require_synced,
    ) else {
        return Ok(None);
    };
    let Some(winner_db_id) = winner.db_id.clone().map(Into::into) else {
        return Ok(None);
    };

    lyra_db::lyrics::get_detail(db, winner_db_id)
}

#[cfg(test)]
mod tests {
    use LyricsKind::{
        Personal,
        Provider,
        Shared,
    };

    use super::*;

    fn lyrics(
        id: &str,
        provider_id: &str,
        language: &str,
        kind: LyricsKind,
        line_count: u32,
        max_synced_ts_ms: u64,
        updated_at: u64,
    ) -> Lyrics {
        // Tests needing the degenerate "one real ts" case build the struct explicitly.
        let synced_line_count = if max_synced_ts_ms > 0 { line_count } else { 0 };
        Lyrics {
            db_id: None,
            id: id.to_string(),
            provider_id: (kind == Provider).then(|| provider_id.to_string()),
            language: language.to_string(),
            kind,
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

    fn provider(provider_id: &str, position: u32) -> ProviderConfig {
        ProviderConfig {
            db_id: None,
            provider_id: provider_id.to_string(),
            position,
            enabled: true,
        }
    }

    fn disabled(provider_id: &str, position: u32) -> ProviderConfig {
        ProviderConfig {
            db_id: None,
            provider_id: provider_id.to_string(),
            position,
            enabled: false,
        }
    }

    fn pick_preferred<'a>(
        candidates: &'a [Lyrics],
        providers: &[ProviderConfig],
        language_hint: Option<&str>,
        duration_ms: Option<u64>,
        require_synced: bool,
    ) -> Option<&'a Lyrics> {
        super::pick_preferred(
            candidates,
            providers,
            None,
            language_hint,
            duration_ms,
            require_synced,
        )
    }

    #[test]
    fn personal_lyrics_beat_everything() {
        let cands = vec![
            lyrics("p1", "plug", "eng", Provider, 100, 180_000, 2000),
            lyrics("u1", "user", "eng", Personal, 0, 0, 1000),
        ];
        let providers = vec![provider("plug", 0)];
        let winner = pick_preferred(&cands, &providers, Some("eng"), Some(200_000), false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("u1"));
    }

    #[test]
    fn language_match_beats_mismatch() {
        let cands = vec![
            lyrics("p1", "plug", "fra", Provider, 50, 120_000, 2000),
            lyrics("p2", "other", "eng", Provider, 0, 0, 1000),
        ];
        let providers = vec![provider("plug", 0), provider("other", 1)];
        let winner = pick_preferred(&cands, &providers, Some("eng"), Some(200_000), false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("p2"));
    }

    #[test]
    fn language_match_is_case_insensitive() {
        let cands = vec![
            lyrics("p1", "plug", "fra", Provider, 0, 0, 2000),
            lyrics("p2", "other", "ENG", Provider, 0, 0, 1000),
        ];
        let providers = vec![provider("plug", 0), provider("other", 1)];
        let winner = pick_preferred(&cands, &providers, Some("eng"), None, false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("p2"));
    }

    #[test]
    fn meaningful_synced_beats_plain_across_providers() {
        let cands = vec![
            lyrics("p1", "high", "eng", Provider, 0, 0, 2000),
            lyrics("p2", "low", "eng", Provider, 80, 120_000, 1000),
        ];
        let providers = vec![provider("high", 0), provider("low", 1)];
        let winner = pick_preferred(&cands, &providers, Some("eng"), Some(200_000), false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("p2"));
    }

    #[test]
    fn synced_requires_coverage_threshold() {
        // Synced but last ts covers only 10% of duration; plain wins on updated_at.
        let cands = vec![
            lyrics("thin", "plug", "eng", Provider, 2, 20_000, 2000),
            lyrics("plain", "plug", "eng", Provider, 0, 0, 3000),
        ];
        let providers = vec![provider("plug", 0)];
        let winner = pick_preferred(&cands, &providers, Some("eng"), Some(200_000), false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("plain"));
    }

    #[test]
    fn single_forged_timestamp_does_not_qualify_as_synced() {
        // One pinned line + 49 zero-ts: max gate would accept, synced_line_count rejects.
        let forged = Lyrics {
            db_id: None,
            id: "forged".to_string(),
            provider_id: Some("plug".to_string()),
            language: "eng".to_string(),
            kind: Provider,
            plain_text: String::new(),
            line_count: 50,
            synced_line_count: 1,
            max_synced_ts_ms: 180_000,
            has_word_cues: false,
            content_hash: String::new(),
            last_checked_at: 2000,
            updated_at: 2000,
        };
        let honest_plain = lyrics("plain", "plug", "eng", Provider, 0, 0, 1000);
        let cands = vec![forged, honest_plain];
        let providers = vec![provider("plug", 0)];
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
            lyrics("zeros", "plug", "eng", Provider, 50, 0, 2000),
            lyrics("plain", "plug", "eng", Provider, 0, 0, 1000),
        ];
        let providers = vec![provider("plug", 0)];
        let winner = pick_preferred(&cands, &providers, None, Some(200_000), false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("zeros"));
        let synced_only = pick_preferred(&cands, &providers, None, Some(200_000), true);
        assert!(synced_only.is_none());
    }

    #[test]
    fn require_synced_filters_plain_candidates_regardless_of_rank() {
        let cands = vec![
            // User override would win overall, but it's plain.
            lyrics("u1", "user", "eng", Personal, 0, 0, 5000),
            lyrics("syn", "plug", "eng", Provider, 80, 120_000, 1000),
        ];
        let providers = vec![provider("plug", 0)];
        let winner = pick_preferred(&cands, &providers, Some("eng"), Some(200_000), true);
        assert_eq!(
            winner.map(|w| w.id.as_str()),
            Some("syn"),
            "LRC requests must fall through plain winners to real synced data"
        );
    }

    #[test]
    fn require_synced_returns_none_when_no_synced_candidate_exists() {
        let cands = vec![lyrics("plain", "plug", "eng", Provider, 0, 0, 1000)];
        let providers = vec![provider("plug", 0)];
        let winner = pick_preferred(&cands, &providers, None, Some(200_000), true);
        assert!(winner.is_none());
    }

    #[test]
    fn provider_priority_then_recency_then_provider_id() {
        let cands = vec![
            lyrics("a", "aa", "eng", Provider, 0, 0, 1000),
            lyrics("b", "bb", "eng", Provider, 0, 0, 2000),
            lyrics("c", "cc", "eng", Provider, 0, 0, 2000),
        ];
        // Newer pair ties; lex-smaller provider_id breaks it.
        let providers = vec![provider("aa", 0), provider("bb", 0), provider("cc", 0)];
        let winner = pick_preferred(&cands, &providers, None, None, false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("b"));
    }

    #[test]
    fn disabled_provider_is_excluded() {
        let cands = vec![
            lyrics("dis", "off", "eng", Provider, 100, 180_000, 5000),
            lyrics("on", "live", "eng", Provider, 0, 0, 1000),
        ];
        let providers = vec![disabled("off", 0), provider("live", 1)];
        let winner = pick_preferred(&cands, &providers, None, Some(200_000), false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("on"));
        let eligible = eligible_candidates(&cands, &providers, false, Some(200_000));
        assert_eq!(eligible.len(), 1);
        assert_eq!(eligible[0].id, "on");
    }

    #[test]
    fn personal_lyrics_are_eligible_without_provider_configuration() {
        let cands = vec![lyrics("u1", "user", "eng", Personal, 0, 0, 1000)];
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
            lyrics("thin", "plug", "eng", Provider, 2, 20_000, 2000),
            lyrics("plain", "plug", "eng", Provider, 0, 0, 3000),
        ];
        let providers = vec![provider("plug", 0)];
        let winner = pick_preferred(&cands, &providers, None, None, false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("thin"));
    }

    #[test]
    fn personal_lyrics_beat_language_hints() {
        let personal = lyrics("personal", "manual", "fra", Personal, 0, 0, 1000);
        let provider_lyrics = lyrics("provider", "plug", "eng", Provider, 0, 0, 3000);
        let candidates = vec![personal, provider_lyrics];
        let providers = vec![provider("plug", 0)];

        let winner = pick_preferred(&candidates, &providers, Some("eng"), None, false);
        assert_eq!(winner.map(|lyrics| lyrics.id.as_str()), Some("personal"));
    }

    #[test]
    fn explicit_provider_selector_bypasses_manual_precedence() {
        let personal = lyrics("personal", "manual", "eng", Personal, 0, 0, 3000);
        let provider_lyrics = lyrics("provider", "plug", "eng", Provider, 0, 0, 1000);
        let candidates = vec![personal, provider_lyrics];
        let providers = vec![provider("plug", 0)];

        let winner =
            super::pick_preferred(&candidates, &providers, Some("plug"), None, None, false);
        assert_eq!(winner.map(|lyrics| lyrics.id.as_str()), Some("provider"));
    }

    #[test]
    fn shared_manual_beats_provider_but_not_personal() {
        let shared = lyrics("shared", "manual", "eng", Shared, 0, 0, 3000);
        let personal = lyrics("personal", "manual", "fra", Personal, 0, 0, 1000);
        let provider_lyrics = lyrics("provider", "plug", "eng", Provider, 0, 0, 5000);
        let providers = vec![provider("plug", 0)];

        let candidates = vec![shared.clone(), personal, provider_lyrics.clone()];
        let winner = pick_preferred(&candidates, &providers, Some("eng"), None, false);
        assert_eq!(winner.map(|lyrics| lyrics.id.as_str()), Some("personal"));

        let without_personal = vec![shared, provider_lyrics];
        let winner = pick_preferred(&without_personal, &providers, Some("eng"), None, false);
        assert_eq!(winner.map(|lyrics| lyrics.id.as_str()), Some("shared"));
    }
}
