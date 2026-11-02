// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::cmp::Ordering;

use agdb::{
    DbAny,
    DbId,
};

use crate::services::{
    auth::Principal,
    providers::ActiveProviders,
};
use lyra_db::{
    Lyrics,
    lyrics::LyricsDetail,
    lyrics::LyricsKind,
};

/// Coverage threshold for the selector's synced tier.
const MEANINGFUL_SYNCED_COVERAGE: f64 = 0.5;

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

fn is_eligible(lyrics: &Lyrics, providers: &ActiveProviders) -> bool {
    lyrics.kind != LyricsKind::Provider
        || lyrics
            .provider_id
            .as_deref()
            .is_some_and(|provider_id| providers.contains(provider_id))
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

/// What a ranking keeps and prefers.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Ranking<'a> {
    /// Keeps only this provider's rows, bypassing manual precedence.
    pub(crate) provider_id: Option<&'a str>,
    /// Prefers rows in this language; already normalized.
    pub(crate) language_hint: Option<&'a str>,
    /// Keeps only rows with meaningful synced content.
    pub(crate) require_synced: bool,
}

/// Ordering, best first (first difference wins): personal, shared manual, language match,
/// `has_meaningful_synced` (≥ `MIN_SYNCED_LINES` non-zero-ts lines AND `max_synced_ts_ms`
/// ≥ 50% of duration), provider position, newer `updated_at_ms`, `provider_id` asc.
fn rank_order(
    a: &Lyrics,
    b: &Lyrics,
    providers: &ActiveProviders,
    language_hint: Option<&str>,
    duration_ms: Option<u64>,
) -> Ordering {
    let tier = |lyrics: &Lyrics| match lyrics.kind {
        LyricsKind::Personal => 0_u8,
        LyricsKind::Shared => 1,
        LyricsKind::Provider => 2,
    };
    let language = |lyrics: &Lyrics| {
        !language_hint.is_some_and(|hint| language_matches(&lyrics.language, hint))
    };
    let unsynced = |lyrics: &Lyrics| !has_meaningful_synced(lyrics, duration_ms);
    let position = |lyrics: &Lyrics| {
        lyrics
            .provider_id
            .as_deref()
            .and_then(|provider_id| providers.position(provider_id))
            .unwrap_or(u32::MAX)
    };

    tier(a)
        .cmp(&tier(b))
        .then_with(|| language(a).cmp(&language(b)))
        .then_with(|| unsynced(a).cmp(&unsynced(b)))
        .then_with(|| position(a).cmp(&position(b)))
        .then_with(|| b.updated_at_ms.cmp(&a.updated_at_ms))
        .then_with(|| a.provider_id.cmp(&b.provider_id))
}

/// Whether `ranking` keeps `lyrics`. Provider rows are eligible while their provider is active.
fn keeps(
    lyrics: &Lyrics,
    providers: &ActiveProviders,
    ranking: Ranking<'_>,
    duration_ms: Option<u64>,
) -> bool {
    is_eligible(lyrics, providers)
        && ranking.provider_id.is_none_or(|provider_id| {
            lyrics.kind == LyricsKind::Provider
                && lyrics.provider_id.as_deref() == Some(provider_id)
        })
        && (!ranking.require_synced || has_meaningful_synced(lyrics, duration_ms))
}

/// The `candidates` that `ranking` keeps, best first by [`rank_order`].
///
/// `candidates` must already be visibility-filtered, e.g. by [`visible_for_track`].
pub(crate) fn rank<'a>(
    candidates: &'a [Lyrics],
    providers: &ActiveProviders,
    ranking: Ranking<'_>,
    duration_ms: Option<u64>,
) -> Vec<&'a Lyrics> {
    let mut ranked: Vec<&Lyrics> = candidates
        .iter()
        .filter(|lyrics| keeps(lyrics, providers, ranking, duration_ms))
        .collect();
    ranked.sort_by(|a, b| rank_order(a, b, providers, ranking.language_hint, duration_ms));
    ranked
}

/// The first of [`rank`]'s rows.
pub(crate) fn pick_preferred<'a>(
    candidates: &'a [Lyrics],
    providers: &ActiveProviders,
    ranking: Ranking<'_>,
    duration_ms: Option<u64>,
) -> Option<&'a Lyrics> {
    rank(candidates, providers, ranking, duration_ms)
        .into_iter()
        .next()
}

/// A track's lyrics as a viewer ranks them.
pub(crate) struct RankedLyrics {
    pub(crate) duration_ms: Option<u64>,
    /// Best first.
    pub(crate) lyrics: Vec<Lyrics>,
}

/// The lyrics of each of `track_db_ids` that `principal` can see and `ranking` keeps, best
/// first. Tracks that are missing or hidden from `principal` are left out.
pub(crate) fn ranked_for_tracks(
    db: &DbAny,
    track_db_ids: &[DbId],
    principal: &Principal,
    ranking: Ranking<'_>,
) -> anyhow::Result<Vec<(DbId, RankedLyrics)>> {
    let owner = principal.require(db)?;
    let providers = crate::services::providers::active_providers(db)?;
    let mut ranked = Vec::with_capacity(track_db_ids.len());
    for &track_db_id in track_db_ids {
        let Some(track) = lyra_db::tracks::get_by_id(db, track_db_id)? else {
            continue;
        };
        if !crate::services::auth::access::entity_accessible(db, principal, track_db_id)? {
            continue;
        }
        let mut lyrics = lyra_db::lyrics::get_visible_for_track(db, track_db_id, Some(owner))?;
        lyrics.retain(|lyrics| keeps(lyrics, &providers, ranking, track.duration_ms));
        lyrics
            .sort_by(|a, b| rank_order(a, b, &providers, ranking.language_hint, track.duration_ms));
        ranked.push((
            track_db_id,
            RankedLyrics {
                duration_ms: track.duration_ms,
                lyrics,
            },
        ));
    }
    Ok(ranked)
}

/// A lyrics row in full, with what its track adds.
pub(crate) struct LyricsEntry {
    pub(crate) track_id: String,
    pub(crate) duration_ms: Option<u64>,
    pub(crate) detail: LyricsDetail,
}

/// The lyrics rows `lyrics_db_ids` name, in full, when `principal` can see the track and the
/// row: a personal row only its owner, a provider row while its provider is active. Other ids
/// are left out.
pub(crate) fn entries_by_ids(
    db: &DbAny,
    lyrics_db_ids: &[DbId],
    principal: &Principal,
) -> anyhow::Result<Vec<(DbId, LyricsEntry)>> {
    let owner = principal.require(db)?;
    let providers = crate::services::providers::active_providers(db)?;
    let mut entries = Vec::with_capacity(lyrics_db_ids.len());
    for &lyrics_db_id in lyrics_db_ids {
        let Some((track, row)) = lyra_db::lyrics::get_visible(db, lyrics_db_id, Some(owner))?
        else {
            continue;
        };
        let Some(track_db_id) = track.db_id.map(DbId::from) else {
            continue;
        };
        if !is_eligible(&row, &providers)
            || !crate::services::auth::access::entity_accessible(db, principal, track_db_id)?
        {
            continue;
        }
        entries.push((
            lyrics_db_id,
            LyricsEntry {
                track_id: track.id,
                duration_ms: track.duration_ms,
                detail: lyra_db::lyrics::detail_of(db, row)?,
            },
        ));
    }
    Ok(entries)
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
        updated_at_ms: u64,
    ) -> Lyrics {
        // Tests needing the degenerate "one real ts" case build the struct explicitly.
        let synced_line_count = if max_synced_ts_ms > 0 { line_count } else { 0 };
        Lyrics {
            db_id: None,
            id: id.to_string(),
            provider_id: (kind == Provider).then(|| provider_id.to_string()),
            provider_lyrics_id: (kind == Provider).then(|| id.to_string()),
            language: language.to_string(),
            kind,
            plain_text: String::new(),
            line_count,
            synced_line_count,
            max_synced_ts_ms,
            has_word_cues: false,
            content_hash: String::new(),
            last_checked_at_ms: updated_at_ms,
            updated_at_ms,
        }
    }

    fn provider(provider_id: &str, position: u32) -> lyra_db::ProviderConfig {
        lyra_db::ProviderConfig {
            db_id: None,
            provider_id: provider_id.to_string(),
            position,
            enabled: true,
        }
    }

    fn pick_preferred<'a>(
        candidates: &'a [Lyrics],
        providers: &ActiveProviders,
        language_hint: Option<&str>,
        duration_ms: Option<u64>,
        require_synced: bool,
    ) -> Option<&'a Lyrics> {
        super::pick_preferred(
            candidates,
            providers,
            Ranking {
                provider_id: None,
                language_hint,
                require_synced,
            },
            duration_ms,
        )
    }

    #[test]
    fn personal_lyrics_beat_everything() {
        let cands = vec![
            lyrics("p1", "plug", "eng", Provider, 100, 180_000, 2000),
            lyrics("u1", "user", "eng", Personal, 0, 0, 1000),
        ];
        let providers = ActiveProviders::from_configs(vec![provider("plug", 0)]);
        let winner = pick_preferred(&cands, &providers, Some("eng"), Some(200_000), false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("u1"));
    }

    #[test]
    fn language_match_beats_mismatch() {
        let cands = vec![
            lyrics("p1", "plug", "fra", Provider, 50, 120_000, 2000),
            lyrics("p2", "other", "eng", Provider, 0, 0, 1000),
        ];
        let providers =
            ActiveProviders::from_configs(vec![provider("plug", 0), provider("other", 1)]);
        let winner = pick_preferred(&cands, &providers, Some("eng"), Some(200_000), false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("p2"));
    }

    #[test]
    fn language_match_is_case_insensitive() {
        let cands = vec![
            lyrics("p1", "plug", "fra", Provider, 0, 0, 2000),
            lyrics("p2", "other", "ENG", Provider, 0, 0, 1000),
        ];
        let providers =
            ActiveProviders::from_configs(vec![provider("plug", 0), provider("other", 1)]);
        let winner = pick_preferred(&cands, &providers, Some("eng"), None, false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("p2"));
    }

    #[test]
    fn meaningful_synced_beats_plain_across_providers() {
        let cands = vec![
            lyrics("p1", "high", "eng", Provider, 0, 0, 2000),
            lyrics("p2", "low", "eng", Provider, 80, 120_000, 1000),
        ];
        let providers =
            ActiveProviders::from_configs(vec![provider("high", 0), provider("low", 1)]);
        let winner = pick_preferred(&cands, &providers, Some("eng"), Some(200_000), false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("p2"));
    }

    #[test]
    fn synced_requires_coverage_threshold() {
        // Synced but last ts covers only 10% of duration; plain wins on updated_at_ms.
        let cands = vec![
            lyrics("thin", "plug", "eng", Provider, 2, 20_000, 2000),
            lyrics("plain", "plug", "eng", Provider, 0, 0, 3000),
        ];
        let providers = ActiveProviders::from_configs(vec![provider("plug", 0)]);
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
            provider_lyrics_id: Some("forged".to_string()),
            language: "eng".to_string(),
            kind: Provider,
            plain_text: String::new(),
            line_count: 50,
            synced_line_count: 1,
            max_synced_ts_ms: 180_000,
            has_word_cues: false,
            content_hash: String::new(),
            last_checked_at_ms: 2000,
            updated_at_ms: 2000,
        };
        let honest_plain = lyrics("plain", "plug", "eng", Provider, 0, 0, 1000);
        let cands = vec![forged, honest_plain];
        let providers = ActiveProviders::from_configs(vec![provider("plug", 0)]);
        let winner = pick_preferred(&cands, &providers, None, Some(200_000), false);
        assert_eq!(
            winner.map(|w| w.id.as_str()),
            Some("forged"),
            "updated_at_ms wins tie"
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
        let providers = ActiveProviders::from_configs(vec![provider("plug", 0)]);
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
        let providers = ActiveProviders::from_configs(vec![provider("plug", 0)]);
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
        let providers = ActiveProviders::from_configs(vec![provider("plug", 0)]);
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
        let providers = ActiveProviders::from_configs(vec![
            provider("aa", 0),
            provider("bb", 0),
            provider("cc", 0),
        ]);
        let winner = pick_preferred(&cands, &providers, None, None, false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("b"));
    }

    #[test]
    fn unlisted_provider_is_excluded() {
        let cands = vec![
            lyrics("off", "off", "eng", Provider, 100, 180_000, 5000),
            lyrics("on", "live", "eng", Provider, 0, 0, 1000),
        ];
        let providers = ActiveProviders::from_configs(vec![provider("live", 1)]);
        let winner = pick_preferred(&cands, &providers, None, Some(200_000), false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("on"));
        let eligible = rank(&cands, &providers, Ranking::default(), Some(200_000));
        assert_eq!(eligible.len(), 1);
        assert_eq!(eligible[0].id, "on");
    }

    #[test]
    fn personal_lyrics_are_eligible_without_provider_configuration() {
        let cands = vec![lyrics("u1", "user", "eng", Personal, 0, 0, 1000)];
        let providers = ActiveProviders::from_configs(vec![]);
        let winner = pick_preferred(&cands, &providers, None, None, false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("u1"));
    }

    #[test]
    fn empty_candidates_returns_none() {
        let cands: Vec<Lyrics> = vec![];
        let providers = ActiveProviders::from_configs(vec![]);
        assert!(pick_preferred(&cands, &providers, None, None, false).is_none());
    }

    #[test]
    fn unknown_duration_treats_synced_with_nonzero_ts_as_meaningful() {
        let cands = vec![
            lyrics("thin", "plug", "eng", Provider, 2, 20_000, 2000),
            lyrics("plain", "plug", "eng", Provider, 0, 0, 3000),
        ];
        let providers = ActiveProviders::from_configs(vec![provider("plug", 0)]);
        let winner = pick_preferred(&cands, &providers, None, None, false);
        assert_eq!(winner.map(|w| w.id.as_str()), Some("thin"));
    }

    #[test]
    fn personal_lyrics_beat_language_hints() {
        let personal = lyrics("personal", "manual", "fra", Personal, 0, 0, 1000);
        let provider_lyrics = lyrics("provider", "plug", "eng", Provider, 0, 0, 3000);
        let candidates = vec![personal, provider_lyrics];
        let providers = ActiveProviders::from_configs(vec![provider("plug", 0)]);

        let winner = pick_preferred(&candidates, &providers, Some("eng"), None, false);
        assert_eq!(winner.map(|lyrics| lyrics.id.as_str()), Some("personal"));
    }

    #[test]
    fn explicit_provider_selector_bypasses_manual_precedence() {
        let personal = lyrics("personal", "manual", "eng", Personal, 0, 0, 3000);
        let provider_lyrics = lyrics("provider", "plug", "eng", Provider, 0, 0, 1000);
        let candidates = vec![personal, provider_lyrics];
        let providers = ActiveProviders::from_configs(vec![provider("plug", 0)]);

        let ranking = Ranking {
            provider_id: Some("plug"),
            ..Ranking::default()
        };
        let winner = super::pick_preferred(&candidates, &providers, ranking, None);
        assert_eq!(winner.map(|lyrics| lyrics.id.as_str()), Some("provider"));
    }

    #[test]
    fn shared_manual_beats_provider_but_not_personal() {
        let shared = lyrics("shared", "manual", "eng", Shared, 0, 0, 3000);
        let personal = lyrics("personal", "manual", "fra", Personal, 0, 0, 1000);
        let provider_lyrics = lyrics("provider", "plug", "eng", Provider, 0, 0, 5000);
        let providers = ActiveProviders::from_configs(vec![provider("plug", 0)]);

        let candidates = vec![shared.clone(), personal, provider_lyrics.clone()];
        let winner = pick_preferred(&candidates, &providers, Some("eng"), None, false);
        assert_eq!(winner.map(|lyrics| lyrics.id.as_str()), Some("personal"));

        let without_personal = vec![shared, provider_lyrics];
        let winner = pick_preferred(&without_personal, &providers, Some("eng"), None, false);
        assert_eq!(winner.map(|lyrics| lyrics.id.as_str()), Some("shared"));
    }

    #[test]
    fn ranking_lists_every_eligible_row_with_the_preferred_one_first() {
        let cands = vec![
            lyrics("old", "low", "eng", Provider, 0, 0, 1000),
            lyrics("fra", "high", "fra", Provider, 80, 120_000, 3000),
            lyrics("shared", "manual", "fra", Shared, 0, 0, 500),
            lyrics("synced", "low", "eng", Provider, 80, 120_000, 2000),
            lyrics("high", "high", "eng", Provider, 0, 0, 1000),
            lyrics("off", "off", "eng", Provider, 80, 120_000, 9000),
            lyrics("personal", "manual", "eng", Personal, 0, 0, 100),
        ];
        let providers =
            ActiveProviders::from_configs(vec![provider("high", 0), provider("low", 1)]);
        let ranking = Ranking {
            language_hint: Some("eng"),
            ..Ranking::default()
        };
        let ranked: Vec<&str> = rank(&cands, &providers, ranking, Some(200_000))
            .into_iter()
            .map(|lyrics| lyrics.id.as_str())
            .collect();
        assert_eq!(
            ranked,
            ["personal", "shared", "synced", "high", "old", "fra"]
        );
        let preferred = super::pick_preferred(&cands, &providers, ranking, Some(200_000));
        assert_eq!(preferred.map(|lyrics| lyrics.id.as_str()), Some(ranked[0]));
    }
}
