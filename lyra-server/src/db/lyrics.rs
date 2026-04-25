// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    CountComparison,
    DbAny,
    DbElement,
    DbId,
    DbValue,
    QueryBuilder,
};
use blake3::Hasher;
use nanoid::nanoid;
use serde::{
    Deserialize,
    Serialize,
};

use super::{
    DbAccess,
    IdSource,
    NodeId,
};

const EDGE_LINE_IDX_KEY: &str = "line_idx";
const EDGE_WORD_IDX_KEY: &str = "word_idx";

/// ISO-639-2 "unknown" — substituted for empty/whitespace input at upsert.
const LANGUAGE_UNKNOWN: &str = "und";

/// Reserved for user overrides; plugins can't claim it (case-insensitive).
const USER_PROVIDER_ID: &str = "user";

// Size caps sized to cover typical LRC / USLT payloads ~10× over and block
// pathological plugin input. Picked without corpus data; tune as it arrives.
const MAX_PLAIN_TEXT_BYTES: usize = 64 * 1024;
const MAX_LINE_TEXT_BYTES: usize = 2048;
const MAX_LINES_PER_LYRIC: usize = 10_000;
const MAX_WORDS_PER_LINE: usize = 500;
const MAX_LYRICS_PER_TRACK: usize = 32;

fn normalize_language(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        LANGUAGE_UNKNOWN.to_string()
    } else {
        trimmed.to_ascii_lowercase()
    }
}

fn is_valid_provider_id(provider_id: &str) -> bool {
    // No leading/trailing/interior whitespace — the string lands verbatim in
    // logs and admin UIs, and surrounding whitespace would create two rows
    // that compare unequal under the exact-match upsert lookup.
    !provider_id.is_empty()
        && provider_id.is_ascii()
        && !provider_id.chars().any(|c| c.is_whitespace())
}

fn is_user_namespace(provider_id: &str) -> bool {
    provider_id.trim().eq_ignore_ascii_case(USER_PROVIDER_ID)
}

#[derive(DbElement, Serialize, Deserialize, Clone, Debug)]
pub(crate) struct Lyrics {
    #[serde(skip)]
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) provider_id: String,
    pub(crate) language: String,
    pub(crate) origin: IdSource,
    pub(crate) plain_text: String,
    pub(crate) line_count: u32,
    /// Count of lines with `ts_ms > 0`; gates the selector's synced tier.
    pub(crate) synced_line_count: u32,
    pub(crate) max_synced_ts_ms: u64,
    pub(crate) has_word_cues: bool,
    pub(crate) content_hash: String,
    pub(crate) last_checked_at: u64,
    pub(crate) updated_at: u64,
}

#[derive(DbElement, Serialize, Deserialize, Clone, Debug)]
pub(crate) struct LyricLine {
    #[serde(skip)]
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) ts_ms: u64,
    pub(crate) text: String,
}

#[derive(DbElement, Serialize, Deserialize, Clone, Debug)]
pub(crate) struct LyricWord {
    #[serde(skip)]
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) ts_ms: u64,
    pub(crate) char_start: u32,
    pub(crate) char_end: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct WordInput {
    pub ts_ms: u64,
    pub char_start: u32,
    pub char_end: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct LineInput {
    pub ts_ms: u64,
    pub text: String,
    pub words: Vec<WordInput>,
}

#[derive(Clone, Debug)]
pub(crate) struct LyricsInput {
    pub id: String,
    /// Ignored by `upsert_user_override` (always stamped `"user"`).
    pub provider_id: String,
    pub language: String,
    pub plain_text: String,
    pub lines: Vec<LineInput>,
    pub last_checked_at: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct LyricsDetail {
    pub lyrics: Lyrics,
    pub lines: Vec<LineDetail>,
}

#[derive(Debug, Clone)]
pub(crate) struct LineDetail {
    pub line: LyricLine,
    pub words: Vec<LyricWord>,
}

fn hash_bytes(hasher: &mut Hasher, bytes: &[u8]) {
    // Length-prefix so delimiter bytes inside a string can't reshape the hash.
    hasher.update(&(bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn origin_tag(origin: IdSource) -> u8 {
    match origin {
        IdSource::User => b'u',
        IdSource::Plugin => b'p',
    }
}

fn compute_content_hash(input: &LyricsInput, origin: IdSource) -> String {
    let mut hasher = Hasher::new();
    // Mix origin so two rows sharing a natural key but differing in origin
    // can't collide on hash (defence-in-depth against a future refactor that
    // removes the reserved-namespace gate).
    hasher.update(&[origin_tag(origin)]);
    hash_bytes(&mut hasher, input.provider_id.as_bytes());
    hash_bytes(&mut hasher, input.language.as_bytes());
    hash_bytes(&mut hasher, input.plain_text.as_bytes());
    hasher.update(&(input.lines.len() as u64).to_be_bytes());
    for line in &input.lines {
        hasher.update(&line.ts_ms.to_be_bytes());
        hash_bytes(&mut hasher, line.text.as_bytes());
        hasher.update(&(line.words.len() as u64).to_be_bytes());
        for word in &line.words {
            hasher.update(&word.ts_ms.to_be_bytes());
            hasher.update(&word.char_start.to_be_bytes());
            hasher.update(&word.char_end.to_be_bytes());
        }
    }
    hasher.finalize().to_hex().to_string()
}

fn validate_line_text(lines: &[LineInput]) -> anyhow::Result<()> {
    for (idx, line) in lines.iter().enumerate() {
        if line.text.chars().any(char::is_control) {
            anyhow::bail!(
                "lyric line {idx} contains control characters (CR/LF/NUL/C0); reject at ingest"
            );
        }
    }
    Ok(())
}

fn validate_size_caps(input: &LyricsInput) -> anyhow::Result<()> {
    if input.plain_text.len() > MAX_PLAIN_TEXT_BYTES {
        anyhow::bail!(
            "plain_text length {} exceeds maximum {MAX_PLAIN_TEXT_BYTES} bytes",
            input.plain_text.len()
        );
    }
    if input.lines.len() > MAX_LINES_PER_LYRIC {
        anyhow::bail!(
            "lines.len() {} exceeds maximum {MAX_LINES_PER_LYRIC}",
            input.lines.len()
        );
    }
    for (idx, line) in input.lines.iter().enumerate() {
        if line.text.len() > MAX_LINE_TEXT_BYTES {
            anyhow::bail!(
                "lyric line {idx} text length {} exceeds maximum {MAX_LINE_TEXT_BYTES} bytes",
                line.text.len()
            );
        }
        if line.words.len() > MAX_WORDS_PER_LINE {
            anyhow::bail!(
                "lyric line {idx} word count {} exceeds maximum {MAX_WORDS_PER_LINE}",
                line.words.len()
            );
        }
    }
    Ok(())
}

fn validate_ts_ms(lines: &[LineInput], max_ts_ms: Option<u64>) -> anyhow::Result<()> {
    let Some(max) = max_ts_ms else {
        return Ok(());
    };
    for (idx, line) in lines.iter().enumerate() {
        if line.ts_ms > max {
            anyhow::bail!(
                "lyric line {idx} ts_ms {} exceeds maximum {max}",
                line.ts_ms
            );
        }
        for (word_idx, word) in line.words.iter().enumerate() {
            if word.ts_ms > max {
                anyhow::bail!(
                    "lyric line {idx} word {word_idx} ts_ms {} exceeds maximum {max}",
                    word.ts_ms
                );
            }
        }
    }
    Ok(())
}

fn validate_word_offsets(lines: &[LineInput]) -> anyhow::Result<()> {
    for (idx, line) in lines.iter().enumerate() {
        if line.words.is_empty() {
            continue;
        }
        let char_len = u32::try_from(line.text.chars().count()).unwrap_or(u32::MAX);
        let mut prev_end: u32 = 0;
        for (word_idx, word) in line.words.iter().enumerate() {
            if word.char_start >= word.char_end {
                anyhow::bail!(
                    "lyric line {idx} word {word_idx} has empty range (char_start {} >= char_end {})",
                    word.char_start,
                    word.char_end
                );
            }
            if word.char_end > char_len {
                anyhow::bail!(
                    "lyric line {idx} word {word_idx} char_end {} exceeds text length {char_len}",
                    word.char_end
                );
            }
            if word.char_start < prev_end {
                anyhow::bail!(
                    "lyric line {idx} word {word_idx} char_start {} overlaps previous word ending at {prev_end}",
                    word.char_start
                );
            }
            prev_end = word.char_end;
        }
    }
    Ok(())
}

pub(crate) fn get_for_track(db: &impl DbAccess, track_id: DbId) -> anyhow::Result<Vec<Lyrics>> {
    let rows: Vec<Lyrics> = db
        .exec(
            QueryBuilder::select()
                .elements::<Lyrics>()
                .search()
                .from(track_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;
    Ok(rows)
}

pub(crate) fn get_by_id(db: &impl DbAccess, lyrics_id: DbId) -> anyhow::Result<Option<Lyrics>> {
    let rows: Vec<Lyrics> = db
        .exec(QueryBuilder::select().ids(lyrics_id).query())?
        .try_into()?;
    Ok(rows.into_iter().next())
}

pub(crate) fn get_detail(
    db: &impl DbAccess,
    lyrics_id: DbId,
) -> anyhow::Result<Option<LyricsDetail>> {
    let Some(lyrics) = get_by_id(db, lyrics_id)? else {
        return Ok(None);
    };
    let lines = collect_lines(db, lyrics_id)?;
    let expected = lyrics.line_count as usize;
    if expected != lines.len() {
        tracing::warn!(
            lyrics_id = lyrics_id.0,
            expected,
            collected = lines.len(),
            "lyrics line_count drift; some children may have lost their line_idx edge property"
        );
    }
    Ok(Some(LyricsDetail { lyrics, lines }))
}

fn collect_lines(db: &impl DbAccess, lyrics_id: DbId) -> anyhow::Result<Vec<LineDetail>> {
    let outgoing = db.exec(
        QueryBuilder::select()
            .search()
            .from(lyrics_id)
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(1))
            .end_where()
            .query(),
    )?;

    let mut line_refs: Vec<(u32, DbId)> = Vec::new();
    for edge in outgoing.elements {
        if edge.from != Some(lyrics_id) {
            continue;
        }
        let Some(to_id) = edge.to else { continue };
        let idx = edge_u32(&edge, EDGE_LINE_IDX_KEY);
        let Some(idx) = idx else { continue };
        line_refs.push((idx, to_id));
    }
    line_refs.sort_by_key(|(idx, _)| *idx);

    let mut detail = Vec::with_capacity(line_refs.len());
    for (_, line_db_id) in line_refs {
        let line_rows: Vec<LyricLine> = db
            .exec(QueryBuilder::select().ids(line_db_id).query())?
            .try_into()?;
        let Some(line) = line_rows.into_iter().next() else {
            continue;
        };
        let words = collect_words(db, line_db_id)?;
        detail.push(LineDetail { line, words });
    }
    Ok(detail)
}

fn collect_words(db: &impl DbAccess, line_id: DbId) -> anyhow::Result<Vec<LyricWord>> {
    let outgoing = db.exec(
        QueryBuilder::select()
            .search()
            .from(line_id)
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(1))
            .end_where()
            .query(),
    )?;

    let mut word_refs: Vec<(u32, DbId)> = Vec::new();
    for edge in outgoing.elements {
        if edge.from != Some(line_id) {
            continue;
        }
        let Some(to_id) = edge.to else { continue };
        let Some(idx) = edge_u32(&edge, EDGE_WORD_IDX_KEY) else {
            continue;
        };
        word_refs.push((idx, to_id));
    }
    word_refs.sort_by_key(|(idx, _)| *idx);

    let mut words = Vec::with_capacity(word_refs.len());
    for (_, word_db_id) in word_refs {
        let rows: Vec<LyricWord> = db
            .exec(QueryBuilder::select().ids(word_db_id).query())?
            .try_into()?;
        if let Some(word) = rows.into_iter().next() {
            words.push(word);
        }
    }
    Ok(words)
}

fn edge_u32(edge: &DbElement, key: &str) -> Option<u32> {
    edge.values
        .iter()
        .find_map(|kv| match (&kv.key, &kv.value) {
            (DbValue::String(k), DbValue::U64(v)) if k == key => u32::try_from(*v).ok(),
            (DbValue::String(k), DbValue::I64(v)) if k == key => u32::try_from(*v).ok(),
            _ => None,
        })
}

/// Stamps `origin = Plugin`. `provider_id` must be a non-empty ASCII string
/// with no whitespace, and not in the reserved `"user"` namespace.
pub(crate) fn upsert_from_plugin(
    db: &mut DbAny,
    track_id: DbId,
    input: LyricsInput,
    max_ts_ms: Option<u64>,
) -> anyhow::Result<DbId> {
    anyhow::ensure!(
        is_valid_provider_id(&input.provider_id),
        "provider_id must be a non-empty ASCII string with no whitespace; got {:?}",
        input.provider_id
    );
    anyhow::ensure!(
        !is_user_namespace(&input.provider_id),
        "plugin upsert cannot use the reserved '{USER_PROVIDER_ID}' provider_id"
    );
    db.transaction_mut(|t| -> anyhow::Result<DbId> {
        upsert_inner(t, track_id, input, IdSource::Plugin, max_ts_ms)
    })
}

/// Stamps `origin = User` and forces `provider_id = "user"`.
pub(crate) fn upsert_user_override(
    db: &mut DbAny,
    track_id: DbId,
    mut input: LyricsInput,
    max_ts_ms: Option<u64>,
) -> anyhow::Result<DbId> {
    input.provider_id = USER_PROVIDER_ID.to_string();
    db.transaction_mut(|t| -> anyhow::Result<DbId> {
        upsert_inner(t, track_id, input, IdSource::User, max_ts_ms)
    })
}

fn upsert_inner(
    db: &mut impl DbAccess,
    track_id: DbId,
    mut input: LyricsInput,
    origin: IdSource,
    max_ts_ms: Option<u64>,
) -> anyhow::Result<DbId> {
    // Cheapest rejection first: a track already at cap fails without us
    // scanning potentially-oversized input.
    let existing_rows_for_track = get_for_track(db, track_id)?;
    let existing_db_id = existing_rows_for_track
        .iter()
        .find(|row| row.provider_id == input.provider_id && row.id == input.id)
        .and_then(|row| row.db_id.clone().map(Into::into));

    if existing_db_id.is_none() && existing_rows_for_track.len() >= MAX_LYRICS_PER_TRACK {
        anyhow::bail!(
            "track already has {} lyrics rows (cap {MAX_LYRICS_PER_TRACK}); reject new row",
            existing_rows_for_track.len()
        );
    }

    validate_size_caps(&input)?;
    validate_line_text(&input.lines)?;
    anyhow::ensure!(
        max_ts_ms.is_some() || input.lines.is_empty(),
        "max_ts_ms required when lines.len() > 0"
    );
    validate_ts_ms(&input.lines, max_ts_ms)?;
    validate_word_offsets(&input.lines)?;

    input.language = normalize_language(&input.language);
    let content_hash = compute_content_hash(&input, origin);
    let line_count = u32::try_from(input.lines.len()).unwrap_or(u32::MAX);
    let synced_line_count =
        u32::try_from(input.lines.iter().filter(|line| line.ts_ms > 0).count()).unwrap_or(u32::MAX);
    let max_synced_ts_ms = input.lines.iter().map(|line| line.ts_ms).max().unwrap_or(0);
    let has_word_cues = input.lines.iter().any(|line| !line.words.is_empty());
    let now = input.last_checked_at;

    if let Some(existing_db_id) = existing_db_id {
        let existing_rows: Vec<Lyrics> = db
            .exec(QueryBuilder::select().ids(existing_db_id).query())?
            .try_into()?;
        let existing = existing_rows.into_iter().next();

        if let Some(existing) = existing
            && existing.content_hash == content_hash
        {
            let bumped = Lyrics {
                db_id: existing.db_id.clone(),
                id: existing.id,
                provider_id: existing.provider_id,
                language: existing.language,
                origin: existing.origin,
                plain_text: existing.plain_text,
                line_count: existing.line_count,
                synced_line_count: existing.synced_line_count,
                max_synced_ts_ms: existing.max_synced_ts_ms,
                has_word_cues: existing.has_word_cues,
                content_hash: existing.content_hash,
                last_checked_at: now,
                updated_at: existing.updated_at,
            };
            db.exec_mut(QueryBuilder::insert().element(&bumped).query())?;
            return Ok(existing_db_id);
        }

        remove_children(db, existing_db_id)?;

        let lyrics = Lyrics {
            db_id: Some(NodeId::from(existing_db_id)),
            id: input.id,
            provider_id: input.provider_id,
            language: input.language,
            origin,
            plain_text: input.plain_text,
            line_count,
            synced_line_count,
            max_synced_ts_ms,
            has_word_cues,
            content_hash,
            last_checked_at: now,
            updated_at: now,
        };
        db.exec_mut(QueryBuilder::insert().element(&lyrics).query())?;

        insert_children(db, existing_db_id, &input.lines)?;
        return Ok(existing_db_id);
    }

    let lyrics = Lyrics {
        db_id: None,
        id: input.id,
        provider_id: input.provider_id,
        language: input.language,
        origin,
        plain_text: input.plain_text,
        line_count,
        synced_line_count,
        max_synced_ts_ms,
        has_word_cues,
        content_hash,
        last_checked_at: now,
        updated_at: now,
    };
    let lyrics_db_id = db
        .exec_mut(QueryBuilder::insert().element(&lyrics).query())?
        .elements
        .first()
        .map(|element| element.id)
        .ok_or_else(|| anyhow::anyhow!("lyrics insert returned no id"))?;

    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from("lyrics")
            .to(lyrics_db_id)
            .query(),
    )?;
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(track_id)
            .to(lyrics_db_id)
            .query(),
    )?;

    insert_children(db, lyrics_db_id, &input.lines)?;
    Ok(lyrics_db_id)
}

fn insert_children(
    db: &mut impl DbAccess,
    lyrics_db_id: DbId,
    lines: &[LineInput],
) -> anyhow::Result<()> {
    for (idx, line) in lines.iter().enumerate() {
        let line_idx = u32::try_from(idx).unwrap_or(u32::MAX);
        let line_row = LyricLine {
            db_id: None,
            id: nanoid!(),
            ts_ms: line.ts_ms,
            text: line.text.clone(),
        };
        let line_db_id = db
            .exec_mut(QueryBuilder::insert().element(&line_row).query())?
            .elements
            .first()
            .map(|element| element.id)
            .ok_or_else(|| anyhow::anyhow!("lyric line insert returned no id"))?;

        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(lyrics_db_id)
                .to(line_db_id)
                .values_uniform([(EDGE_LINE_IDX_KEY, u64::from(line_idx)).into()])
                .query(),
        )?;

        for (word_idx, word) in line.words.iter().enumerate() {
            let word_idx = u32::try_from(word_idx).unwrap_or(u32::MAX);
            let word_row = LyricWord {
                db_id: None,
                id: nanoid!(),
                ts_ms: word.ts_ms,
                char_start: word.char_start,
                char_end: word.char_end,
            };
            let word_db_id = db
                .exec_mut(QueryBuilder::insert().element(&word_row).query())?
                .elements
                .first()
                .map(|element| element.id)
                .ok_or_else(|| anyhow::anyhow!("lyric word insert returned no id"))?;
            db.exec_mut(
                QueryBuilder::insert()
                    .edges()
                    .from(line_db_id)
                    .to(word_db_id)
                    .values_uniform([(EDGE_WORD_IDX_KEY, u64::from(word_idx)).into()])
                    .query(),
            )?;
        }
    }
    Ok(())
}

fn remove_children(db: &mut impl DbAccess, lyrics_db_id: DbId) -> anyhow::Result<()> {
    // Graph walk: Lyrics -[edge]-> LyricLine -[edge]-> LyricWord, so lines
    // sit at distance 2 and words at distance 4 from the Lyrics node.
    let line_ids: Vec<DbId> = db
        .exec(
            QueryBuilder::search()
                .from(lyrics_db_id)
                .where_()
                .distance(CountComparison::Equal(2))
                .and()
                .key("db_element_id")
                .value("LyricLine")
                .query(),
        )?
        .ids()
        .into_iter()
        .filter(|id| id.0 > 0)
        .collect();

    let word_ids: Vec<DbId> = if line_ids.is_empty() {
        Vec::new()
    } else {
        db.exec(
            QueryBuilder::search()
                .from(lyrics_db_id)
                .where_()
                .distance(CountComparison::Equal(4))
                .and()
                .key("db_element_id")
                .value("LyricWord")
                .query(),
        )?
        .ids()
        .into_iter()
        .filter(|id| id.0 > 0)
        .collect()
    };

    let mut to_remove = Vec::with_capacity(line_ids.len() + word_ids.len());
    to_remove.extend(word_ids);
    to_remove.extend(line_ids);

    if !to_remove.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(to_remove).query())?;
    }
    Ok(())
}

pub(crate) fn delete_by_db_id(db: &mut DbAny, lyrics_db_id: DbId) -> anyhow::Result<()> {
    db.transaction_mut(|t| -> anyhow::Result<()> {
        remove_children(t, lyrics_db_id)?;
        t.exec_mut(QueryBuilder::remove().ids(lyrics_db_id).query())?;
        Ok(())
    })
}

pub(crate) fn delete_for_track(db: &mut DbAny, track_id: DbId) -> anyhow::Result<()> {
    db.transaction_mut(|t| -> anyhow::Result<()> {
        let ids: Vec<DbId> = t
            .exec(
                QueryBuilder::search()
                    .from(track_id)
                    .where_()
                    .distance(CountComparison::Equal(2))
                    .and()
                    .key("db_element_id")
                    .value("Lyrics")
                    .query(),
            )?
            .ids()
            .into_iter()
            .filter(|id| id.0 > 0)
            .collect();

        for lyrics_db_id in ids {
            remove_children(t, lyrics_db_id)?;
            t.exec_mut(QueryBuilder::remove().ids(lyrics_db_id).query())?;
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db;

    fn line(ts_ms: u64, text: &str) -> LineInput {
        LineInput {
            ts_ms,
            text: text.to_string(),
            words: Vec::new(),
        }
    }

    fn plugin_input(id: &str, provider: &str, plain: &str) -> LyricsInput {
        LyricsInput {
            id: id.to_string(),
            provider_id: provider.to_string(),
            language: "eng".to_string(),
            plain_text: plain.to_string(),
            lines: Vec::new(),
            last_checked_at: 100,
        }
    }

    #[test]
    fn plugin_upsert_creates_row_and_connects_to_track_and_lyrics_collection() -> anyhow::Result<()>
    {
        let mut db = test_db::new_test_db()?;
        let track_id = test_db::insert_track(&mut db, "song")?;

        let lyrics_db_id = upsert_from_plugin(
            &mut db,
            track_id,
            plugin_input("abc", "plug", "hello"),
            None,
        )?;

        let rows = get_for_track(&db, track_id)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "abc");
        assert_eq!(rows[0].provider_id, "plug");
        assert_eq!(rows[0].origin, IdSource::Plugin);
        assert_eq!(rows[0].plain_text, "hello");
        assert_eq!(rows[0].line_count, 0);
        assert_eq!(rows[0].synced_line_count, 0);
        assert_eq!(rows[0].max_synced_ts_ms, 0);
        assert!(!rows[0].has_word_cues);
        assert_eq!(rows[0].last_checked_at, 100);
        assert_eq!(
            rows[0].db_id.as_ref().map(|n| DbId::from(n.clone())),
            Some(lyrics_db_id)
        );
        Ok(())
    }

    #[test]
    fn plugin_upsert_rejects_user_provider_id() {
        let mut db = test_db::new_test_db().unwrap();
        let track_id = test_db::insert_track(&mut db, "song").unwrap();

        let err =
            upsert_from_plugin(&mut db, track_id, plugin_input("x", "USER", ""), None).unwrap_err();
        assert!(err.to_string().contains("reserved"), "error was: {err}");
    }

    #[test]
    fn plugin_upsert_rejects_non_ascii_provider_id() {
        let mut db = test_db::new_test_db().unwrap();
        let track_id = test_db::insert_track(&mut db, "song").unwrap();

        // Cyrillic "user" homoglyph — blocked at the ASCII gate so the admin
        // UI can't surface spoofed provider names.
        let err = upsert_from_plugin(
            &mut db,
            track_id,
            plugin_input("x", "us\u{0435}r", ""),
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("provider_id"), "error was: {err}");

        for bad in ["", "   ", " plug", "plug ", "a b", "plug\ttab"] {
            let err = upsert_from_plugin(&mut db, track_id, plugin_input("x", bad, ""), None)
                .unwrap_err();
            assert!(
                err.to_string().contains("provider_id"),
                "expected rejection for {bad:?}, got: {err}"
            );
        }
    }

    #[test]
    fn plugin_upsert_rejects_synced_lines_without_max_ts_ms() {
        let mut db = test_db::new_test_db().unwrap();
        let track_id = test_db::insert_track(&mut db, "song").unwrap();

        let mut input = plugin_input("x", "plug", "");
        input.lines = vec![line(1_000, "one"), line(2_000, "two")];
        let err = upsert_from_plugin(&mut db, track_id, input, None).unwrap_err();
        assert!(err.to_string().contains("max_ts_ms"), "error was: {err}");

        // Plain-text-only writes without a bound are still allowed.
        upsert_from_plugin(&mut db, track_id, plugin_input("p", "plug", "plain"), None).unwrap();
    }

    #[test]
    fn user_override_stamps_reserved_provider_and_origin() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;
        let track_id = test_db::insert_track(&mut db, "song")?;

        let mut input = plugin_input("u-1", "ignored", "mine");
        input.provider_id = "plugin-says".to_string();
        upsert_user_override(&mut db, track_id, input, None)?;

        let rows = get_for_track(&db, track_id)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].provider_id, "user");
        assert_eq!(rows[0].origin, IdSource::User);
        Ok(())
    }

    #[test]
    fn upsert_scopes_by_track_so_shared_id_does_not_cross_tracks() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;
        let track_a = test_db::insert_track(&mut db, "a")?;
        let track_b = test_db::insert_track(&mut db, "b")?;

        let a_id = upsert_from_plugin(&mut db, track_a, plugin_input("x", "plug", "A"), None)?;
        let b_id = upsert_from_plugin(&mut db, track_b, plugin_input("x", "plug", "B"), None)?;

        assert_ne!(
            a_id, b_id,
            "same plugin id on two tracks must create two rows"
        );

        let a_rows = get_for_track(&db, track_a)?;
        let b_rows = get_for_track(&db, track_b)?;
        assert_eq!(a_rows.len(), 1);
        assert_eq!(b_rows.len(), 1);
        assert_eq!(a_rows[0].plain_text, "A");
        assert_eq!(b_rows[0].plain_text, "B");
        Ok(())
    }

    #[test]
    fn upsert_scopes_by_provider_so_shared_id_stays_separate() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;
        let track_id = test_db::insert_track(&mut db, "song")?;

        upsert_from_plugin(
            &mut db,
            track_id,
            plugin_input("x", "plug", "from-plug"),
            None,
        )?;
        upsert_from_plugin(
            &mut db,
            track_id,
            plugin_input("x", "embedded", "from-embedded"),
            None,
        )?;

        let mut rows = get_for_track(&db, track_id)?;
        rows.sort_by(|a, b| a.provider_id.cmp(&b.provider_id));
        assert_eq!(
            rows.len(),
            2,
            "same id under different providers must stay separate"
        );
        assert_eq!(rows[0].provider_id, "embedded");
        assert_eq!(rows[0].plain_text, "from-embedded");
        assert_eq!(rows[1].provider_id, "plug");
        assert_eq!(rows[1].plain_text, "from-plug");
        Ok(())
    }

    #[test]
    fn user_override_survives_plugin_write_with_identical_natural_key() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;
        let track_id = test_db::insert_track(&mut db, "song")?;

        let user_input = plugin_input("shared-id", "user", "user-authored");
        upsert_user_override(&mut db, track_id, user_input, None)?;

        // A plugin tries to sneak in with provider_id = "user".
        let plugin_attempt = plugin_input("shared-id", "user", "plugin-injected");
        let err = upsert_from_plugin(&mut db, track_id, plugin_attempt, None).unwrap_err();
        assert!(err.to_string().contains("reserved"), "error was: {err}");

        let rows = get_for_track(&db, track_id)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].origin, IdSource::User);
        assert_eq!(rows[0].plain_text, "user-authored");
        Ok(())
    }

    #[test]
    fn upsert_rejects_control_characters_in_line_text() {
        let mut db = test_db::new_test_db().unwrap();
        let track_id = test_db::insert_track(&mut db, "song").unwrap();

        let cases = [
            "clean\nsecond",
            "clean\rseparator",
            "nul\0inside",
            "bell\x07here",
        ];
        for bad in cases {
            let mut input = plugin_input("x", "plug", "");
            input.lines = vec![line(0, bad)];
            let err = upsert_from_plugin(&mut db, track_id, input, None).unwrap_err();
            assert!(
                err.to_string().contains("control characters"),
                "expected rejection for {bad:?}, got: {err}"
            );
        }

        let rows = get_for_track(&db, track_id).unwrap();
        assert!(rows.is_empty(), "no bad input should have landed");
    }

    #[test]
    fn upsert_rejects_ts_ms_above_max() {
        let mut db = test_db::new_test_db().unwrap();
        let track_id = test_db::insert_track(&mut db, "song").unwrap();

        let mut input = plugin_input("x", "plug", "");
        input.lines = vec![line(100_000, "ok"), line(u64::MAX, "forged")];

        let err = upsert_from_plugin(&mut db, track_id, input, Some(200_000)).unwrap_err();
        assert!(
            err.to_string().contains("exceeds maximum"),
            "error was: {err}"
        );

        let rows = get_for_track(&db, track_id).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn upsert_normalizes_empty_language_to_und() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;
        let track_id = test_db::insert_track(&mut db, "song")?;

        let mut input = plugin_input("abc", "plug", "hello");
        input.language = "   ".to_string();
        upsert_from_plugin(&mut db, track_id, input, None)?;

        let rows = get_for_track(&db, track_id)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].language, "und");
        Ok(())
    }

    #[test]
    fn upsert_casefolds_language() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;
        let track_id = test_db::insert_track(&mut db, "song")?;

        let mut input = plugin_input("abc", "plug", "hello");
        input.language = "  ENG  ".to_string();
        upsert_from_plugin(&mut db, track_id, input, None)?;

        let rows = get_for_track(&db, track_id)?;
        assert_eq!(rows[0].language, "eng");
        Ok(())
    }

    #[test]
    fn upsert_rejects_invalid_word_ranges() {
        let mut db = test_db::new_test_db().unwrap();
        let track_id = test_db::insert_track(&mut db, "song").unwrap();

        let make = |words: Vec<WordInput>| {
            let mut input = plugin_input("x", "plug", "");
            input.lines = vec![LineInput {
                ts_ms: 100,
                text: "hello".to_string(),
                words,
            }];
            input
        };

        // char_start >= char_end
        let err = upsert_from_plugin(
            &mut db,
            track_id,
            make(vec![WordInput {
                ts_ms: 0,
                char_start: 2,
                char_end: 2,
            }]),
            Some(10_000),
        )
        .unwrap_err();
        assert!(err.to_string().contains("empty range"), "got: {err}");

        // char_end > text.chars().count()
        let err = upsert_from_plugin(
            &mut db,
            track_id,
            make(vec![WordInput {
                ts_ms: 0,
                char_start: 0,
                char_end: 99,
            }]),
            Some(10_000),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("exceeds text length"),
            "got: {err}"
        );

        // Overlapping words
        let err = upsert_from_plugin(
            &mut db,
            track_id,
            make(vec![
                WordInput {
                    ts_ms: 0,
                    char_start: 0,
                    char_end: 3,
                },
                WordInput {
                    ts_ms: 10,
                    char_start: 2,
                    char_end: 5,
                },
            ]),
            Some(10_000),
        )
        .unwrap_err();
        assert!(err.to_string().contains("overlaps"), "got: {err}");

        assert!(get_for_track(&db, track_id).unwrap().is_empty());
    }

    #[test]
    fn content_hash_length_prefixes_strings() {
        let mut shifted_plain = plugin_input("x", "plug", "ab\x1fcd");
        shifted_plain.lines = vec![line(0, "")];
        let mut different_shape = plugin_input("x", "plug", "ab");
        different_shape.lines = vec![line(0, "cd")];

        let h1 = compute_content_hash(&shifted_plain, IdSource::Plugin);
        let h2 = compute_content_hash(&different_shape, IdSource::Plugin);
        assert_ne!(
            h1, h2,
            "length-prefixing must distinguish payloads that would otherwise hash identically under delimiter-only encoding"
        );
    }

    #[test]
    fn upsert_rejects_oversize_plain_text() {
        let mut db = test_db::new_test_db().unwrap();
        let track_id = test_db::insert_track(&mut db, "song").unwrap();

        let big = "a".repeat(MAX_PLAIN_TEXT_BYTES + 1);
        let input = plugin_input("x", "plug", &big);
        let err = upsert_from_plugin(&mut db, track_id, input, None).unwrap_err();
        assert!(err.to_string().contains("plain_text"), "got: {err}");
    }

    #[test]
    fn upsert_rejects_oversize_line_count() {
        let mut db = test_db::new_test_db().unwrap();
        let track_id = test_db::insert_track(&mut db, "song").unwrap();

        let mut input = plugin_input("x", "plug", "");
        input.lines = (0..(MAX_LINES_PER_LYRIC + 1))
            .map(|i| line(i as u64, "x"))
            .collect();
        let err = upsert_from_plugin(&mut db, track_id, input, None).unwrap_err();
        assert!(err.to_string().contains("lines.len()"), "got: {err}");
    }

    #[test]
    fn upsert_rejects_oversize_line_text() {
        let mut db = test_db::new_test_db().unwrap();
        let track_id = test_db::insert_track(&mut db, "song").unwrap();

        let mut input = plugin_input("x", "plug", "");
        input.lines = vec![line(0, &"x".repeat(MAX_LINE_TEXT_BYTES + 1))];
        let err = upsert_from_plugin(&mut db, track_id, input, None).unwrap_err();
        assert!(err.to_string().contains("text length"), "got: {err}");
    }

    #[test]
    fn upsert_rejects_excess_lyrics_per_track() {
        let mut db = test_db::new_test_db().unwrap();
        let track_id = test_db::insert_track(&mut db, "song").unwrap();

        for i in 0..MAX_LYRICS_PER_TRACK {
            upsert_from_plugin(
                &mut db,
                track_id,
                plugin_input(&format!("id-{i}"), "plug", "x"),
                None,
            )
            .unwrap();
        }
        let err = upsert_from_plugin(
            &mut db,
            track_id,
            plugin_input("id-over", "plug", "overflow"),
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("cap"), "got: {err}");
    }

    #[test]
    fn upsert_accepts_valid_adjacent_word_ranges() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;
        let track_id = test_db::insert_track(&mut db, "song")?;

        let mut input = plugin_input("x", "plug", "");
        input.lines = vec![LineInput {
            ts_ms: 100,
            text: "ab cd".to_string(),
            words: vec![
                WordInput {
                    ts_ms: 0,
                    char_start: 0,
                    char_end: 2,
                },
                WordInput {
                    ts_ms: 50,
                    char_start: 3,
                    char_end: 5,
                },
            ],
        }];
        upsert_from_plugin(&mut db, track_id, input, Some(10_000))?;
        assert_eq!(get_for_track(&db, track_id)?.len(), 1);
        Ok(())
    }

    #[test]
    fn upsert_same_content_is_noop_but_bumps_last_checked_at() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;
        let track_id = test_db::insert_track(&mut db, "song")?;

        let first = upsert_from_plugin(
            &mut db,
            track_id,
            plugin_input("abc", "plug", "hello"),
            None,
        )?;

        let mut second_input = plugin_input("abc", "plug", "hello");
        second_input.last_checked_at = 200;
        let second = upsert_from_plugin(&mut db, track_id, second_input, None)?;

        assert_eq!(first, second);

        let rows = get_for_track(&db, track_id)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].last_checked_at, 200);
        assert_eq!(rows[0].updated_at, 100, "updated_at must not bump on no-op");
        Ok(())
    }

    #[test]
    fn upsert_changed_content_replaces_children_and_bumps_updated_at() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;
        let track_id = test_db::insert_track(&mut db, "song")?;

        let bound = Some(10_000);
        let mut first_input = plugin_input("abc", "plug", "");
        first_input.lines = vec![line(0, "one"), line(1000, "two")];
        let lyrics_id = upsert_from_plugin(&mut db, track_id, first_input, bound)?;

        let detail = get_detail(&db, lyrics_id)?.expect("lyrics");
        assert_eq!(detail.lines.len(), 2);
        assert_eq!(detail.lines[0].line.text, "one");
        assert_eq!(detail.lines[1].line.ts_ms, 1000);
        assert_eq!(detail.lyrics.line_count, 2);
        assert_eq!(detail.lyrics.max_synced_ts_ms, 1000);

        let mut second_input = plugin_input("abc", "plug", "");
        second_input.lines = vec![line(0, "alpha"), line(500, "beta"), line(2000, "gamma")];
        second_input.last_checked_at = 500;
        upsert_from_plugin(&mut db, track_id, second_input, bound)?;

        let detail = get_detail(&db, lyrics_id)?.expect("lyrics");
        assert_eq!(detail.lines.len(), 3);
        assert_eq!(detail.lines[0].line.text, "alpha");
        assert_eq!(detail.lines[2].line.ts_ms, 2000);
        assert_eq!(detail.lyrics.line_count, 3);
        assert_eq!(detail.lyrics.max_synced_ts_ms, 2000);
        assert_eq!(detail.lyrics.updated_at, 500);
        Ok(())
    }

    #[test]
    fn upsert_with_word_cues_flips_has_word_cues() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;
        let track_id = test_db::insert_track(&mut db, "song")?;

        let mut input = plugin_input("xyz", "plug", "");
        input.lines = vec![LineInput {
            ts_ms: 0,
            text: "ohayou".to_string(),
            words: vec![
                WordInput {
                    ts_ms: 0,
                    char_start: 0,
                    char_end: 1,
                },
                WordInput {
                    ts_ms: 100,
                    char_start: 1,
                    char_end: 3,
                },
                WordInput {
                    ts_ms: 200,
                    char_start: 3,
                    char_end: 6,
                },
            ],
        }];
        let lyrics_id = upsert_from_plugin(&mut db, track_id, input, Some(10_000))?;

        let detail = get_detail(&db, lyrics_id)?.expect("lyrics");
        assert!(detail.lyrics.has_word_cues);
        assert_eq!(detail.lines[0].words.len(), 3);
        assert_eq!(detail.lines[0].words[1].char_start, 1);
        assert_eq!(detail.lines[0].words[2].ts_ms, 200);
        Ok(())
    }

    #[test]
    fn multiple_providers_on_same_track_coexist() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;
        let track_id = test_db::insert_track(&mut db, "song")?;

        upsert_from_plugin(
            &mut db,
            track_id,
            plugin_input("l-1", "plug", "from plug"),
            None,
        )?;
        upsert_from_plugin(
            &mut db,
            track_id,
            plugin_input("e-1", "embedded", "from embedded"),
            None,
        )?;
        upsert_user_override(
            &mut db,
            track_id,
            plugin_input("u-1", "anything", "from user"),
            None,
        )?;

        let mut rows = get_for_track(&db, track_id)?;
        rows.sort_by(|a, b| a.provider_id.cmp(&b.provider_id));
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].provider_id, "embedded");
        assert_eq!(rows[1].provider_id, "plug");
        assert_eq!(rows[2].provider_id, "user");
        assert_eq!(rows[2].origin, IdSource::User);
        Ok(())
    }

    #[test]
    fn delete_for_track_removes_lyrics_and_children() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;
        let track_id = test_db::insert_track(&mut db, "song")?;

        let mut input = plugin_input("abc", "plug", "");
        input.lines = vec![LineInput {
            ts_ms: 0,
            text: "one".to_string(),
            words: vec![WordInput {
                ts_ms: 0,
                char_start: 0,
                char_end: 3,
            }],
        }];
        upsert_from_plugin(&mut db, track_id, input, Some(10_000))?;

        assert_eq!(get_for_track(&db, track_id)?.len(), 1);
        delete_for_track(&mut db, track_id)?;
        assert!(get_for_track(&db, track_id)?.is_empty());
        Ok(())
    }

    #[test]
    fn delete_by_db_id_removes_only_target() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;
        let track_id = test_db::insert_track(&mut db, "song")?;

        let first_id =
            upsert_from_plugin(&mut db, track_id, plugin_input("a", "plug", "first"), None)?;
        let _second_id = upsert_from_plugin(
            &mut db,
            track_id,
            plugin_input("b", "embedded", "second"),
            None,
        )?;

        delete_by_db_id(&mut db, first_id)?;

        let rows = get_for_track(&db, track_id)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].provider_id, "embedded");
        Ok(())
    }
}
