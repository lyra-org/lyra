// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};

use agdb::{
    DbAny,
    DbId,
};
use mlua::Table;
use rand::seq::SliceRandom;

use agdb::QueryBuilder;

use crate::STATE;
use crate::db::{
    self,
    Track,
};

mod registry;

pub(crate) use registry::{
    MIX_REGISTRY,
    MixSeedType,
    reset_mix_registry_for_test,
    teardown_plugin_mixers,
};

const DEFAULT_LIMIT: usize = 200;
const MAX_PER_ARTIST: usize = 3;
const MIXER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Clone, Debug, Default)]
pub(crate) struct MixOptions {
    pub(crate) limit: Option<usize>,
    pub(crate) user_db_id: Option<DbId>,
    /// Arbitrary key-value options forwarded from query parameters.
    /// Plugins declare these via `declare_option` and values are coerced
    /// using declared types before being set as `ctx.options`.
    pub(crate) extra: HashMap<String, String>,
}

pub(crate) async fn from_track(
    track_db_id: DbId,
    options: &MixOptions,
) -> anyhow::Result<Vec<Track>> {
    if let Some(tracks) = dispatch_mixer(MixSeedType::Track, track_db_id, options).await? {
        return Ok(tracks);
    }
    let db = &*STATE.db.read().await;
    builtin_from_track(db, track_db_id, options)
}

pub(crate) async fn from_release(
    release_db_id: DbId,
    options: &MixOptions,
) -> anyhow::Result<Vec<Track>> {
    if let Some(tracks) = dispatch_mixer(MixSeedType::Release, release_db_id, options).await? {
        return Ok(tracks);
    }
    let db = &*STATE.db.read().await;
    builtin_from_release(db, release_db_id, options)
}

pub(crate) async fn from_artist(
    artist_db_id: DbId,
    options: &MixOptions,
) -> anyhow::Result<Vec<Track>> {
    if let Some(tracks) = dispatch_mixer(MixSeedType::Artist, artist_db_id, options).await? {
        return Ok(tracks);
    }
    let db = &*STATE.db.read().await;
    builtin_from_artist(db, artist_db_id, options)
}

pub(crate) async fn from_recent_listens(
    user_db_id: DbId,
    options: &MixOptions,
) -> anyhow::Result<Vec<Track>> {
    if let Some(tracks) = dispatch_recent_listens_mixer(user_db_id, options).await? {
        return Ok(tracks);
    }
    let db = &*STATE.db.read().await;
    builtin_from_recent_listens(db, user_db_id, options)
}

/// Returns mixer IDs sorted by priority (highest first), filtered to those
/// that have a handler for the given seed type.
async fn prioritized_mixer_ids(seed_type: MixSeedType) -> anyhow::Result<Vec<String>> {
    let db = STATE.db.read().await;
    let mut configs = db::mixers::get(&*db)?;
    configs.retain(|c| c.enabled);
    configs.sort_by(|a, b| b.priority.cmp(&a.priority));

    let registry = MIX_REGISTRY.read().await;
    let ids: Vec<String> = configs
        .into_iter()
        .filter(|c| registry.has_handler(&c.mixer_id, seed_type))
        .map(|c| c.mixer_id)
        .collect();
    Ok(ids)
}

/// Tries plugin mixers in descending priority and returns the first non-empty
/// result. `Ok(None)` when every mixer errored, timed out, or returned empty —
/// the caller falls through to the built-in mixer in that case.
async fn dispatch_mixer(
    seed_type: MixSeedType,
    seed_id: DbId,
    options: &MixOptions,
) -> anyhow::Result<Option<Vec<Track>>> {
    let mixer_ids = prioritized_mixer_ids(seed_type).await?;
    if mixer_ids.is_empty() {
        return Ok(None);
    }

    for mixer_id in &mixer_ids {
        let handler = {
            let registry = MIX_REGISTRY.read().await;
            registry.get_handler(mixer_id, seed_type).cloned()
        };
        let Some(handler) = handler else {
            continue;
        };

        let lua = match handler.try_upgrade_lua() {
            Some(lua) => lua,
            None => continue,
        };
        let ctx = lua.create_table()?;
        ctx.set("seed_id", seed_id.0)?;
        if let Some(limit) = options.limit {
            ctx.set("limit", limit)?;
        }
        if let Some(user_db_id) = options.user_db_id {
            ctx.set("user_id", user_db_id.0)?;
        }
        set_extra_options(&lua, &ctx, mixer_id, &options.extra).await?;

        match tokio::time::timeout(MIXER_TIMEOUT, handler.call_async::<_, Table>(ctx)).await {
            Ok(Ok(result)) => match parse_mix_result(&result).await {
                Ok(mut tracks) if !tracks.is_empty() => {
                    let limit = options.limit.unwrap_or(DEFAULT_LIMIT);
                    tracks.truncate(limit);
                    return Ok(Some(tracks));
                }
                Ok(_) => {
                    tracing::debug!(mixer = %mixer_id, "mixer returned empty, trying next");
                }
                Err(err) => {
                    tracing::warn!(mixer = %mixer_id, error = %err, "mixer result parse error, trying next");
                }
            },
            Ok(Err(err)) => {
                tracing::warn!(mixer = %mixer_id, error = %err, "mixer handler error, trying next");
            }
            Err(_) => {
                tracing::warn!(mixer = %mixer_id, "mixer handler timed out, trying next");
            }
        }
    }

    Ok(None)
}

/// Same as `dispatch_mixer` for the recent-listens seed type; the context
/// receives pre-resolved recent track IDs.
async fn dispatch_recent_listens_mixer(
    user_db_id: DbId,
    options: &MixOptions,
) -> anyhow::Result<Option<Vec<Track>>> {
    let mixer_ids = prioritized_mixer_ids(MixSeedType::RecentListens).await?;
    if mixer_ids.is_empty() {
        return Ok(None);
    }

    let recent_track_ids = {
        let db = STATE.db.read().await;
        recent_listen_track_ids(&db, user_db_id)?
    };

    for mixer_id in &mixer_ids {
        let handler = {
            let registry = MIX_REGISTRY.read().await;
            registry
                .get_handler(mixer_id, MixSeedType::RecentListens)
                .cloned()
        };
        let Some(handler) = handler else {
            continue;
        };

        let lua = match handler.try_upgrade_lua() {
            Some(lua) => lua,
            None => continue,
        };
        let ctx = lua.create_table()?;
        ctx.set("user_id", user_db_id.0)?;
        if let Some(limit) = options.limit {
            ctx.set("limit", limit)?;
        }
        set_extra_options(&lua, &ctx, mixer_id, &options.extra).await?;
        let track_ids_table = lua.create_table()?;
        for (i, id) in recent_track_ids.iter().enumerate() {
            track_ids_table.set(i + 1, id.0)?;
        }
        ctx.set("recent_track_ids", track_ids_table)?;

        match tokio::time::timeout(MIXER_TIMEOUT, handler.call_async::<_, Table>(ctx)).await {
            Ok(Ok(result)) => match parse_mix_result(&result).await {
                Ok(mut tracks) if !tracks.is_empty() => {
                    let limit = options.limit.unwrap_or(DEFAULT_LIMIT);
                    tracks.truncate(limit);
                    return Ok(Some(tracks));
                }
                Ok(_) => {
                    tracing::debug!(mixer = %mixer_id, "mixer returned empty, trying next");
                }
                Err(err) => {
                    tracing::warn!(mixer = %mixer_id, error = %err, "mixer result parse error, trying next");
                }
            },
            Ok(Err(err)) => {
                tracing::warn!(mixer = %mixer_id, error = %err, "mixer handler error, trying next");
            }
            Err(_) => {
                tracing::warn!(mixer = %mixer_id, "mixer handler timed out, trying next");
            }
        }
    }

    Ok(None)
}

use super::options::coerce_option_value;

/// Sets extra options from the query string as a typed `options` table on the context,
/// coercing values using declared option types from the mixer registry.
async fn set_extra_options(
    lua: &mlua::Lua,
    ctx: &Table,
    mixer_id: &str,
    extra: &HashMap<String, String>,
) -> anyhow::Result<()> {
    if extra.is_empty() {
        return Ok(());
    }
    let registry = MIX_REGISTRY.read().await;
    let declared = registry.get_options(mixer_id);
    let options_table = lua.create_table()?;
    for (key, raw_value) in extra {
        if let Some(decl) = declared.iter().find(|d| d.name == *key) {
            let coerced = coerce_option_value(raw_value, &decl.option_type);
            let lua_val = mlua::LuaSerdeExt::to_value(lua, &coerced)?;
            options_table.set(key.as_str(), lua_val)?;
        }
    }
    ctx.set("options", options_table)?;
    Ok(())
}

/// Hard ceiling on the number of track IDs we'll resolve from a plugin result,
/// preventing a buggy or malicious plugin from triggering unbounded DB queries.
const MAX_MIXER_RESULT_IDS: usize = 500;

/// Parse the MixResult table returned by a mixer handler into Track objects.
async fn parse_mix_result(result: &Table) -> anyhow::Result<Vec<Track>> {
    let tracks_table: Table = result.get("tracks")?;
    let mut track_ids = Vec::new();
    for value in tracks_table.sequence_values::<Table>() {
        let entry = value?;
        let track_id: i64 = entry.get("track_id")?;
        track_ids.push(DbId(track_id));
        if track_ids.len() >= MAX_MIXER_RESULT_IDS {
            break;
        }
    }

    if track_ids.is_empty() {
        return Ok(Vec::new());
    }

    let requested_count = track_ids.len();
    let db = STATE.db.read().await;
    let tracks_by_id: HashMap<DbId, Track> =
        db::graph::bulk_fetch_typed(&*db, track_ids.clone(), "Track")?;

    // Preserve the plugin's ordering by iterating in the original ID order.
    let mut tracks = Vec::with_capacity(requested_count);
    for id in &track_ids {
        if let Some(track) = tracks_by_id.get(id) {
            tracks.push(track.clone());
        }
    }

    let dropped = requested_count - tracks.len();
    if dropped > 0 {
        tracing::warn!(
            dropped,
            requested = requested_count,
            "mixer returned track IDs that could not be resolved"
        );
    }

    Ok(tracks)
}

// --- Built-in genre-overlap mix algorithm (fallback) ---

fn expand_and_weight(
    db: &DbAny,
    seed_genres: &[db::genres::Genre],
) -> anyhow::Result<Vec<(DbId, u32)>> {
    let seed_ids: Vec<DbId> = seed_genres
        .iter()
        .filter_map(|g| g.db_id.clone().map(DbId::from))
        .collect();
    db::genres::expand_related(db, &seed_ids, 2)
}

fn builtin_from_track(
    db: &DbAny,
    track_db_id: DbId,
    options: &MixOptions,
) -> anyhow::Result<Vec<Track>> {
    let releases = db::releases::get_by_track(db, track_db_id)?;
    let seed_genres = collect_genres_from_releases(db, &releases)?;
    let weighted = expand_and_weight(db, &seed_genres)?;
    tracks_for_genres(db, &weighted, options)
}

fn builtin_from_release(
    db: &DbAny,
    release_db_id: DbId,
    options: &MixOptions,
) -> anyhow::Result<Vec<Track>> {
    let seed_genres = db::genres::get_for_release(db, release_db_id)?;
    let weighted = expand_and_weight(db, &seed_genres)?;
    tracks_for_genres(db, &weighted, options)
}

fn builtin_from_artist(
    db: &DbAny,
    artist_db_id: DbId,
    options: &MixOptions,
) -> anyhow::Result<Vec<Track>> {
    let releases = db::releases::get_by_artist(db, artist_db_id)?;
    let seed_genres = collect_genres_from_releases(db, &releases)?;
    let weighted = expand_and_weight(db, &seed_genres)?;
    tracks_for_genres(db, &weighted, options)
}

const RECENT_LISTEN_COUNT: usize = 50;

fn builtin_from_recent_listens(
    db: &DbAny,
    user_db_id: DbId,
    options: &MixOptions,
) -> anyhow::Result<Vec<Track>> {
    let track_ids = recent_listen_track_ids(db, user_db_id)?;
    if track_ids.is_empty() {
        return Ok(Vec::new());
    }

    let releases_by_track = db::releases::get_by_tracks(db, &track_ids)?;
    let mut seen_release_ids = HashSet::new();
    let all_releases: Vec<db::Release> = releases_by_track
        .into_values()
        .flatten()
        .filter(|a| {
            a.db_id
                .clone()
                .map(DbId::from)
                .is_some_and(|id| seen_release_ids.insert(id))
        })
        .collect();
    let seed_genres = collect_genres_from_releases(db, &all_releases)?;
    let weighted = expand_and_weight(db, &seed_genres)?;

    tracks_for_genres(db, &weighted, options)
}

fn recent_listen_track_ids(db: &DbAny, user_db_id: DbId) -> anyhow::Result<Vec<DbId>> {
    // Get listen nodes pointing at this user
    let mut listens: Vec<db::listens::Listen> = db
        .exec(
            QueryBuilder::select()
                .elements::<db::listens::Listen>()
                .search()
                .to(user_db_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    // Sort by recency, take the most recent N
    listens.sort_unstable_by(|a, b| b.listened_at_ms.cmp(&a.listened_at_ms));
    listens.truncate(RECENT_LISTEN_COUNT);

    // For each listen, find the track it points to
    let mut track_ids = Vec::new();
    let mut seen = HashSet::new();
    for listen in &listens {
        let Some(listen_db_id) = listen.db_id else {
            continue;
        };
        let neighbors: Vec<db::Track> = db
            .exec(
                QueryBuilder::select()
                    .elements::<db::Track>()
                    .search()
                    .from(listen_db_id)
                    .where_()
                    .neighbor()
                    .end_where()
                    .query(),
            )?
            .try_into()?;
        for track in neighbors {
            if let Some(track_db_id) = track.db_id.clone().map(DbId::from) {
                if seen.insert(track_db_id) {
                    track_ids.push(track_db_id);
                }
            }
        }
    }

    Ok(track_ids)
}

fn collect_genres_from_releases(
    db: &DbAny,
    releases: &[db::Release],
) -> anyhow::Result<Vec<db::genres::Genre>> {
    let mut seen = HashSet::new();
    let mut genres = Vec::new();
    for release in releases {
        let Some(release_db_id) = release.db_id.clone().map(DbId::from) else {
            continue;
        };
        for genre in db::genres::get_for_release(db, release_db_id)? {
            let Some(genre_db_id) = genre.db_id.clone().map(DbId::from) else {
                continue;
            };
            if seen.insert(genre_db_id) {
                genres.push(genre);
            }
        }
    }
    Ok(genres)
}

fn tracks_for_genres(
    db: &DbAny,
    genres: &[(DbId, u32)],
    options: &MixOptions,
) -> anyhow::Result<Vec<Track>> {
    if genres.is_empty() {
        return Ok(Vec::new());
    }

    let release_scores = release_weighted_scores(db, genres)?;
    if release_scores.is_empty() {
        return Ok(Vec::new());
    }

    // Group releases by score tier
    let mut releases_by_score: HashMap<u32, Vec<DbId>> = HashMap::new();
    for (&release_id, &score) in &release_scores {
        releases_by_score.entry(score).or_default().push(release_id);
    }
    let mut tiers: Vec<u32> = releases_by_score.keys().copied().collect();
    tiers.sort_unstable_by(|a, b| b.cmp(a));

    let mut rng = rand::rng();
    let limit = options.limit.unwrap_or(DEFAULT_LIMIT);
    let mut all_tracks: Vec<Track> = Vec::new();
    let mut seen = HashSet::new();

    // Fetch tracks tier-by-tier
    for tier in &tiers {
        let Some(mut tier_releases) = releases_by_score.remove(tier) else {
            continue;
        };
        tier_releases.shuffle(&mut rng);

        let tier_tracks = db::tracks::get_direct_many(db, &tier_releases)?;
        let mut tier_flat: Vec<Track> = tier_tracks.into_values().flatten().collect();
        tier_flat.retain(|t| {
            t.db_id
                .clone()
                .map(DbId::from)
                .is_some_and(|id| seen.insert(id))
        });
        tier_flat.shuffle(&mut rng);
        all_tracks.extend(tier_flat);
    }

    // Partition into unheard and heard, preserving score order within each
    let (unheard, heard) = partition_by_listen_history(db, all_tracks, options.user_db_id)?;

    let mut combined = unheard;
    combined.extend(heard);

    let result = cap_per_artist(db, combined, limit)?;

    Ok(result)
}

fn release_weighted_scores(
    db: &DbAny,
    genres: &[(DbId, u32)],
) -> anyhow::Result<HashMap<DbId, u32>> {
    let mut scores: HashMap<DbId, u32> = HashMap::new();
    for &(genre_id, weight) in genres {
        let releases: Vec<db::Release> = db
            .exec(
                QueryBuilder::select()
                    .elements::<db::Release>()
                    .search()
                    .from(genre_id)
                    .where_()
                    .neighbor()
                    .end_where()
                    .query(),
            )?
            .try_into()?;
        for release in releases {
            if let Some(db_id) = release.db_id.map(DbId::from) {
                *scores.entry(db_id).or_insert(0) += weight;
            }
        }
    }
    Ok(scores)
}

fn partition_by_listen_history(
    db: &DbAny,
    tracks: Vec<Track>,
    user_db_id: Option<DbId>,
) -> anyhow::Result<(Vec<Track>, Vec<Track>)> {
    let Some(user_db_id) = user_db_id else {
        return Ok((tracks, Vec::new()));
    };

    let track_ids: Vec<DbId> = tracks
        .iter()
        .filter_map(|t| t.db_id.clone().map(DbId::from))
        .collect();

    let counts = db::listens::get_counts(db, &track_ids, Some(user_db_id))?;

    let mut unheard = Vec::new();
    let mut heard = Vec::new();
    for track in tracks {
        let track_db_id = track.db_id.clone().map(DbId::from);
        let listen_count = track_db_id
            .as_ref()
            .and_then(|id| counts.get(id))
            .copied()
            .unwrap_or(0);
        if listen_count == 0 {
            unheard.push(track);
        } else {
            heard.push(track);
        }
    }

    Ok((unheard, heard))
}

fn cap_per_artist(db: &DbAny, tracks: Vec<Track>, limit: usize) -> anyhow::Result<Vec<Track>> {
    let track_ids: Vec<DbId> = tracks
        .iter()
        .filter_map(|t| t.db_id.clone().map(DbId::from))
        .collect();

    let artists_by_track = db::artists::get_many_by_owner(db, &track_ids)?;

    let primary_artist_for = |track: &Track| -> Option<DbId> {
        let track_db_id = track.db_id.clone().map(DbId::from)?;
        artists_by_track
            .get(&track_db_id)?
            .first()?
            .db_id
            .clone()
            .map(DbId::from)
    };

    // First pass: prefer diversity — cap per artist
    let mut artist_counts: HashMap<DbId, usize> = HashMap::new();
    let mut result = Vec::with_capacity(limit);
    let mut overflow = Vec::new();

    for track in tracks {
        match primary_artist_for(&track) {
            Some(artist_id) => {
                let count = artist_counts.entry(artist_id).or_insert(0);
                if *count < MAX_PER_ARTIST {
                    *count += 1;
                    result.push(track);
                } else {
                    overflow.push(track);
                }
            }
            None => {
                result.push(track);
            }
        }
    }

    // Second pass: if under limit, backfill from overflow
    if result.len() < limit {
        for track in overflow {
            result.push(track);
            if result.len() >= limit {
                break;
            }
        }
    }

    result.truncate(limit);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{
        genres,
        listens,
        playback_sessions::{
            PlaybackSession,
            PlaybackState,
        },
        test_db::{
            connect,
            connect_artist,
            insert_artist,
            insert_release,
            insert_track,
            new_test_db,
        },
    };

    #[test]
    fn from_track_returns_genre_matched_tracks() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let rock_id = genres::resolve_by_name(&mut db, "Rock")?;
        let jazz_id = genres::resolve_by_name(&mut db, "Jazz")?;

        let release_a = insert_release(&mut db, "Rock Release")?;
        genres::link_to_release(&mut db, rock_id, release_a)?;
        let track_a = insert_track(&mut db, "Rock Track")?;
        connect(&mut db, release_a, track_a)?;

        let release_b = insert_release(&mut db, "Another Rock Release")?;
        genres::link_to_release(&mut db, rock_id, release_b)?;
        let track_b = insert_track(&mut db, "Another Rock Track")?;
        connect(&mut db, release_b, track_b)?;

        let release_c = insert_release(&mut db, "Jazz Release")?;
        genres::link_to_release(&mut db, jazz_id, release_c)?;
        let track_c = insert_track(&mut db, "Jazz Track")?;
        connect(&mut db, release_c, track_c)?;

        let result = builtin_from_track(&db, track_a, &MixOptions::default())?;

        let titles: HashSet<&str> = result.iter().map(|t| t.track_title.as_str()).collect();
        assert!(titles.contains("Rock Track"));
        assert!(titles.contains("Another Rock Track"));
        assert!(!titles.contains("Jazz Track"));

        Ok(())
    }

    #[test]
    fn from_release_returns_genre_matched_tracks() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let jazz_id = genres::resolve_by_name(&mut db, "Jazz")?;

        let release_a = insert_release(&mut db, "Jazz Release A")?;
        genres::link_to_release(&mut db, jazz_id, release_a)?;
        let track_a = insert_track(&mut db, "Jazz A")?;
        connect(&mut db, release_a, track_a)?;

        let release_b = insert_release(&mut db, "Jazz Release B")?;
        genres::link_to_release(&mut db, jazz_id, release_b)?;
        let track_b = insert_track(&mut db, "Jazz B")?;
        connect(&mut db, release_b, track_b)?;

        let result = builtin_from_release(&db, release_a, &MixOptions::default())?;
        assert_eq!(result.len(), 2);

        Ok(())
    }

    #[test]
    fn from_artist_returns_genre_matched_tracks() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let blues_id = genres::resolve_by_name(&mut db, "Blues")?;

        let artist = insert_artist(&mut db, "BB King")?;
        let release = insert_release(&mut db, "Live at the Regal")?;
        connect_artist(&mut db, release, artist)?;
        genres::link_to_release(&mut db, blues_id, release)?;
        let track = insert_track(&mut db, "Every Day I Have the Blues")?;
        connect(&mut db, release, track)?;

        let release_b = insert_release(&mut db, "Other Blues")?;
        genres::link_to_release(&mut db, blues_id, release_b)?;
        let track_b = insert_track(&mut db, "Blues Track")?;
        connect(&mut db, release_b, track_b)?;

        let result = builtin_from_artist(&db, artist, &MixOptions::default())?;
        assert_eq!(result.len(), 2);

        Ok(())
    }

    #[test]
    fn from_track_with_no_genres_returns_empty() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let release = insert_release(&mut db, "No Genre Release")?;
        let track = insert_track(&mut db, "Orphan Track")?;
        connect(&mut db, release, track)?;

        let result = builtin_from_track(&db, track, &MixOptions::default())?;
        assert!(result.is_empty());

        Ok(())
    }

    #[test]
    fn artist_diversity_caps_per_artist() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let rock_id = genres::resolve_by_name(&mut db, "Rock")?;
        let prolific = insert_artist(&mut db, "Prolific Artist")?;
        let other = insert_artist(&mut db, "Other Artist")?;

        // 5 tracks by the prolific artist
        for i in 0..5 {
            let release = insert_release(&mut db, &format!("Prolific Release {i}"))?;
            genres::link_to_release(&mut db, rock_id, release)?;
            let track = insert_track(&mut db, &format!("Prolific Track {i}"))?;
            connect(&mut db, release, track)?;
            connect_artist(&mut db, track, prolific)?;
        }

        // 2 tracks by another artist
        for i in 0..2 {
            let release = insert_release(&mut db, &format!("Other Release {i}"))?;
            genres::link_to_release(&mut db, rock_id, release)?;
            let track = insert_track(&mut db, &format!("Other Track {i}"))?;
            connect(&mut db, release, track)?;
            connect_artist(&mut db, track, other)?;
        }

        let seed_release = insert_release(&mut db, "Seed")?;
        genres::link_to_release(&mut db, rock_id, seed_release)?;
        let seed_track = insert_track(&mut db, "Seed Track")?;
        connect(&mut db, seed_release, seed_track)?;
        connect_artist(&mut db, seed_track, prolific)?;

        // With a tight limit, the cap should prefer diversity
        let result = builtin_from_track(
            &db,
            seed_track,
            &MixOptions {
                limit: Some(5),
                ..Default::default()
            },
        )?;
        assert_eq!(result.len(), 5);

        // Prolific artist should be capped at MAX_PER_ARTIST in the first pass,
        // so with limit=5, other artist's tracks should all appear
        let other_count = result
            .iter()
            .filter(|t| t.track_title.starts_with("Other"))
            .count();
        assert_eq!(other_count, 2);

        let prolific_count = result
            .iter()
            .filter(|t| t.track_title.starts_with("Prolific") || t.track_title.starts_with("Seed"))
            .count();
        assert_eq!(prolific_count, 3);

        Ok(())
    }

    #[test]
    fn artist_diversity_backfills_when_under_limit() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let rock_id = genres::resolve_by_name(&mut db, "Rock")?;
        let solo = insert_artist(&mut db, "Solo Artist")?;

        // 5 tracks all by the same artist — only source of tracks
        for i in 0..5 {
            let release = insert_release(&mut db, &format!("Solo Release {i}"))?;
            genres::link_to_release(&mut db, rock_id, release)?;
            let track = insert_track(&mut db, &format!("Solo Track {i}"))?;
            connect(&mut db, release, track)?;
            connect(&mut db, track, solo)?;
        }

        let seed_release = insert_release(&mut db, "Seed")?;
        genres::link_to_release(&mut db, rock_id, seed_release)?;
        let seed_track = insert_track(&mut db, "Seed Track")?;
        connect(&mut db, seed_release, seed_track)?;
        connect(&mut db, seed_track, solo)?;

        // All 6 tracks are by the same artist, but backfill should include them all
        let result = builtin_from_track(&db, seed_track, &MixOptions::default())?;
        assert_eq!(result.len(), 6);

        Ok(())
    }

    #[test]
    fn limit_caps_results() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let rock_id = genres::resolve_by_name(&mut db, "Rock")?;

        for i in 0..10 {
            let release = insert_release(&mut db, &format!("Release {i}"))?;
            genres::link_to_release(&mut db, rock_id, release)?;
            let track = insert_track(&mut db, &format!("Track {i}"))?;
            connect(&mut db, release, track)?;
        }

        let seed_release = insert_release(&mut db, "Seed Release")?;
        genres::link_to_release(&mut db, rock_id, seed_release)?;
        let seed_track = insert_track(&mut db, "Seed Track")?;
        connect(&mut db, seed_release, seed_track)?;

        let result = builtin_from_track(
            &db,
            seed_track,
            &MixOptions {
                limit: Some(5),
                ..Default::default()
            },
        )?;
        assert_eq!(result.len(), 5);

        Ok(())
    }

    fn insert_user(db: &mut DbAny) -> anyhow::Result<DbId> {
        use crate::db::users::User;
        use agdb::QueryBuilder;

        let user = User {
            db_id: None,
            id: nanoid::nanoid!(),
            username: "testuser".to_string(),
            password: "hashed".to_string(),
        };
        let user_id = db
            .exec_mut(QueryBuilder::insert().element(&user).query())?
            .ids()[0];
        Ok(user_id)
    }

    fn record_listen(db: &mut DbAny, track_db_id: DbId, user_db_id: DbId) -> anyhow::Result<()> {
        let listen = listens::Listen {
            db_id: None,
            id: nanoid::nanoid!(),
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: 180_000,
            state: PlaybackState::Completed,
            listened_at_ms: 1_000_000,
            created_at_ms: 1_000_000,
        };
        let session = PlaybackSession {
            db_id: None,
            id: nanoid::nanoid!(),
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: Some(180_000),
            last_position_ms: None,
            state: PlaybackState::Completed,
            listen_recorded: Some(true),
            updated_at_ms: 1_000_000,
            created_at_ms: 1_000_000,
        };
        listens::create_and_mark_recorded(db, &listen, track_db_id, user_db_id, &session)
    }

    #[test]
    fn unheard_tracks_appear_before_heard() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let rock_id = genres::resolve_by_name(&mut db, "Rock")?;
        let user_id = insert_user(&mut db)?;

        // Create heard tracks
        let mut heard_track_ids = Vec::new();
        for i in 0..3 {
            let release = insert_release(&mut db, &format!("Heard Release {i}"))?;
            genres::link_to_release(&mut db, rock_id, release)?;
            let track = insert_track(&mut db, &format!("Heard Track {i}"))?;
            connect(&mut db, release, track)?;
            record_listen(&mut db, track, user_id)?;
            heard_track_ids.push(track);
        }

        // Create unheard tracks
        for i in 0..3 {
            let release = insert_release(&mut db, &format!("Unheard Release {i}"))?;
            genres::link_to_release(&mut db, rock_id, release)?;
            let track = insert_track(&mut db, &format!("Unheard Track {i}"))?;
            connect(&mut db, release, track)?;
        }

        let seed_release = insert_release(&mut db, "Seed")?;
        genres::link_to_release(&mut db, rock_id, seed_release)?;
        let seed_track = insert_track(&mut db, "Seed Track")?;
        connect(&mut db, seed_release, seed_track)?;

        let result = builtin_from_track(
            &db,
            seed_track,
            &MixOptions {
                user_db_id: Some(user_id),
                ..Default::default()
            },
        )?;

        // All 7 tracks should be present (3 heard + 3 unheard + 1 seed)
        assert_eq!(result.len(), 7);

        // The first 4 tracks should all be unheard (3 unheard + 1 seed)
        let heard_ids: HashSet<DbId> = heard_track_ids.into_iter().collect();
        let first_four: Vec<bool> = result[..4]
            .iter()
            .map(|t| {
                t.db_id
                    .clone()
                    .map(DbId::from)
                    .is_some_and(|id| heard_ids.contains(&id))
            })
            .collect();
        assert!(
            first_four.iter().all(|&is_heard| !is_heard),
            "expected first 4 tracks to be unheard, but some were heard"
        );

        Ok(())
    }

    #[test]
    fn higher_genre_overlap_ranks_first() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let rock_id = genres::resolve_by_name(&mut db, "Rock")?;
        let blues_id = genres::resolve_by_name(&mut db, "Blues")?;
        let pop_id = genres::resolve_by_name(&mut db, "Pop")?;

        // Seed track: Rock + Blues
        let seed_release = insert_release(&mut db, "Seed Release")?;
        genres::link_to_release(&mut db, rock_id, seed_release)?;
        genres::link_to_release(&mut db, blues_id, seed_release)?;
        let seed_track = insert_track(&mut db, "Seed Track")?;
        connect(&mut db, seed_release, seed_track)?;

        // Release with 2 genre overlap (Rock + Blues) — should rank higher
        let high_release = insert_release(&mut db, "High Overlap")?;
        genres::link_to_release(&mut db, rock_id, high_release)?;
        genres::link_to_release(&mut db, blues_id, high_release)?;
        let high_track = insert_track(&mut db, "High Overlap Track")?;
        connect(&mut db, high_release, high_track)?;

        // Release with 1 genre overlap (Rock only) — should rank lower
        let low_release = insert_release(&mut db, "Low Overlap")?;
        genres::link_to_release(&mut db, rock_id, low_release)?;
        genres::link_to_release(&mut db, pop_id, low_release)?;
        let low_track = insert_track(&mut db, "Low Overlap Track")?;
        connect(&mut db, low_release, low_track)?;

        let result = builtin_from_track(&db, seed_track, &MixOptions::default())?;
        assert_eq!(result.len(), 3);

        // Find positions of high and low overlap tracks
        let high_pos = result
            .iter()
            .position(|t| t.track_title == "High Overlap Track")
            .expect("high overlap track present");
        let low_pos = result
            .iter()
            .position(|t| t.track_title == "Low Overlap Track")
            .expect("low overlap track present");

        assert!(
            high_pos < low_pos,
            "expected high overlap track (pos {high_pos}) before low overlap track (pos {low_pos})"
        );

        Ok(())
    }

    #[test]
    fn from_recent_listens_uses_listened_genres() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let rock_id = genres::resolve_by_name(&mut db, "Rock")?;
        let jazz_id = genres::resolve_by_name(&mut db, "Jazz")?;
        let user_id = insert_user(&mut db)?;

        // User listened to a Rock track
        let listened_release = insert_release(&mut db, "Listened Rock Release")?;
        genres::link_to_release(&mut db, rock_id, listened_release)?;
        let listened_track = insert_track(&mut db, "Listened Rock Track")?;
        connect(&mut db, listened_release, listened_track)?;
        record_listen(&mut db, listened_track, user_id)?;

        // Other Rock tracks in library (should appear in mix)
        let rock_release = insert_release(&mut db, "Other Rock Release")?;
        genres::link_to_release(&mut db, rock_id, rock_release)?;
        let rock_track = insert_track(&mut db, "Other Rock Track")?;
        connect(&mut db, rock_release, rock_track)?;

        // Jazz tracks in library (should NOT appear — user hasn't listened to jazz)
        let jazz_release = insert_release(&mut db, "Jazz Release")?;
        genres::link_to_release(&mut db, jazz_id, jazz_release)?;
        let jazz_track = insert_track(&mut db, "Jazz Track")?;
        connect(&mut db, jazz_release, jazz_track)?;

        let options = MixOptions {
            user_db_id: Some(user_id),
            ..Default::default()
        };
        let result = builtin_from_recent_listens(&db, user_id, &options)?;

        let titles: HashSet<&str> = result.iter().map(|t| t.track_title.as_str()).collect();
        assert!(titles.contains("Other Rock Track"));
        assert!(titles.contains("Listened Rock Track"));
        assert!(!titles.contains("Jazz Track"));

        Ok(())
    }

    #[test]
    fn from_recent_listens_returns_empty_with_no_history() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_id = insert_user(&mut db)?;

        let options = MixOptions {
            user_db_id: Some(user_id),
            ..Default::default()
        };
        let result = builtin_from_recent_listens(&db, user_id, &options)?;
        assert!(result.is_empty());

        Ok(())
    }
}
