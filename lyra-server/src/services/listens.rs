// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! What counts as a listen, for live playback and submitted plays alike.

use std::collections::{
    HashMap,
    HashSet,
    hash_map::Entry,
};

use agdb::{
    DbAny,
    DbId,
};
use lyra_db::nanoid;
use lyra_db::{
    Listen,
    ResolveId,
    entities::MetadataEntityType,
};

use serde::{
    Deserialize,
    Serialize,
};

use super::plugin_dispatch::send_update;
use super::{
    auth::Principal,
    catalog::{
        self,
        CatalogError,
        Viewer,
        artists::{
            ArtistKey,
            Artists,
        },
        pipeline::Catalog,
        releases::{
            ReleaseKey,
            Releases,
        },
        tracks::{
            TrackKey,
            Tracks,
        },
    },
};

pub(crate) mod summaries;

const MIN_PLAYED_MS: u64 = 30_000;
const SHORT_TRACK_MS: u64 = 180_000;
const SHORT_TRACK_PLAYED_PERCENT: u64 = 20;
/// 2010-01-01T00:00:00Z; earlier starts come from reset device clocks.
const EARLIEST_STARTED_AT_MS: u64 = 1_262_304_000_000;
/// How far past the server clock a play may end, for client clock drift.
const FUTURE_END_ALLOWANCE_MS: u64 = 60_000;

/// One play of a track by a user, as the player observed it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Play<'a> {
    pub(crate) play_id: &'a str,
    pub(crate) track_db_id: DbId,
    pub(crate) started_at_ms: u64,
    pub(crate) played_ms: u64,
}

/// One user's listens recorded by one commit, as plugins see them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ListenUpdatePayload {
    pub event: String,
    pub user_id: String,
    pub listens: Vec<ListenPayload>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ListenPayload {
    pub listen_id: String,
    pub track_id: String,
    pub started_at_ms: u64,
    pub played_ms: Option<u64>,
}

/// A listen as its transaction stored it, with the public id of its track, which announcing it
/// after the commit can no longer look up.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecordedListen {
    pub(crate) listen: Listen,
    pub(crate) track_id: String,
}

impl From<RecordedListen> for ListenPayload {
    fn from(recorded: RecordedListen) -> Self {
        Self {
            listen_id: recorded.listen.id,
            track_id: recorded.track_id,
            started_at_ms: recorded.listen.started_at_ms,
            played_ms: recorded.listen.played_ms,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RecordOutcome {
    /// The play counted; the new listen is announced once its transaction commits.
    Recorded(RecordedListen),
    /// The user already has a listen for this play.
    Duplicate,
    /// The play was too short to count.
    NotListened,
    /// The play started before the earliest plausible time or ends in the future.
    InvalidTime,
}

const PLAY_ID_LENGTHS: std::ops::RangeInclusive<usize> = 16..=64;

/// The play id contract, for API and plugin documentation.
pub(crate) const PLAY_ID_DESCRIPTION: &str = "Client-generated id for one play of a track: 16-64 characters of `[A-Za-z0-9_-]`, unique per user. Every play, including a repeat, gets a new id; a play continued elsewhere, such as on a handoff target, keeps its id. A play counts as at most one listen, however and wherever it is reported.";

/// The `listened` contract of a submitted play, for API and plugin documentation.
#[cfg(feature = "docgen")]
pub(crate) const LISTENED_DESCRIPTION: &str = "Whether the player already judged the play a listen, as a scrobble does; default false. A declared listen counts however long it played, and may leave out its playing time: it then ends a track's duration after its start, or at its start when the duration is unknown.";
/// The playing time contract of a submitted play, for API and plugin documentation.
#[cfg(feature = "docgen")]
pub(crate) const SUBMITTED_PLAYED_MS_DESCRIPTION: &str = "Time the track actually played, excluding pauses and skipped spans. Required unless the play is a declared listen.";

/// Whether `candidate` has the shape of a client-generated play id.
pub(crate) fn is_play_id(candidate: &str) -> bool {
    super::opaque_ids::is_well_formed(candidate, PLAY_ID_LENGTHS)
}

/// The playing time a play needs to count as a listen of a track with this catalog duration.
fn min_played_ms(duration_ms: Option<u64>) -> u64 {
    if let Some(duration_ms) = duration_ms
        && duration_ms > 0
        && duration_ms < SHORT_TRACK_MS
    {
        // Round up to avoid a zero threshold for very short tracks.
        let share_ms = duration_ms
            .saturating_mul(SHORT_TRACK_PLAYED_PERCENT)
            .saturating_add(99)
            / 100;
        return share_ms.min(MIN_PLAYED_MS);
    }

    MIN_PLAYED_MS
}

fn is_listen(played_ms: u64, duration_ms: Option<u64>) -> bool {
    played_ms >= min_played_ms(duration_ms)
}

/// Records the play as the user's listen when it qualifies, inside the caller's transaction. A
/// listen that already exists keeps the longest playing time reported for its play.
pub(crate) fn record(
    db: &mut impl lyra_db::DbAccess,
    user_db_id: DbId,
    play: Play<'_>,
    now_ms: u64,
) -> anyhow::Result<RecordOutcome> {
    let track = lyra_db::tracks::get_by_id(db, play.track_db_id)?
        .ok_or_else(|| anyhow::anyhow!("track {:?} not found", play.track_db_id))?;
    record_for_track(db, user_db_id, play, None, &track, now_ms)
}

/// A listen the player declared, which [`record_for_track`] judges in place of a [`Play`]'s
/// playing time.
#[derive(Clone, Copy, Debug)]
struct Declared {
    /// The playing time the player gave, if any.
    played_ms: Option<u64>,
    /// How long after its start the play ends.
    span_ms: u64,
}

/// [`record`] for a play of `track`, already loaded from `play.track_db_id`. A `declared` listen
/// skips the playing time a listen needs and keeps its own, ignoring `play.played_ms`.
fn record_for_track(
    db: &mut impl lyra_db::DbAccess,
    user_db_id: DbId,
    play: Play<'_>,
    declared: Option<Declared>,
    track: &lyra_db::Track,
    now_ms: u64,
) -> anyhow::Result<RecordOutcome> {
    let played_ms = match declared {
        Some(declared) => declared.played_ms,
        None => Some(play.played_ms),
    };
    let latest_end_ms = now_ms.saturating_add(FUTURE_END_ALLOWANCE_MS);
    if let Some(listen) = lyra_db::listens::get_by_play_id(db, user_db_id, play.play_id)? {
        // Never beyond what fits between the counted start and now.
        let played_ms =
            played_ms.map(|ms| ms.min(latest_end_ms.saturating_sub(listen.started_at_ms)));
        if let Some(played_ms) = played_ms
            && listen.played_ms.is_none_or(|stored| played_ms > stored)
            && let Some(listen_db_id) = listen.db_id
            && lyra_db::listens::track_of(db, listen_db_id)? == Some(play.track_db_id)
        {
            lyra_db::listens::set_played_ms(db, listen_db_id, played_ms)?;
        }
        return Ok(RecordOutcome::Duplicate);
    }
    let span_ms = declared.map_or(play.played_ms, |declared| declared.span_ms);
    if play.started_at_ms < EARLIEST_STARTED_AT_MS
        || play.started_at_ms.saturating_add(span_ms) > latest_end_ms
    {
        return Ok(RecordOutcome::InvalidTime);
    }
    if declared.is_none() && !is_listen(play.played_ms, track.duration_ms) {
        return Ok(RecordOutcome::NotListened);
    }

    let listen = Listen {
        db_id: None,
        id: nanoid(),
        play_id: play.play_id.to_string(),
        started_at_ms: play.started_at_ms,
        played_ms,
        created_at_ms: now_ms,
    };
    let db_id = lyra_db::listens::insert(db, &listen, play.track_db_id, user_db_id)?;
    Ok(RecordOutcome::Recorded(RecordedListen {
        listen: Listen {
            db_id: Some(db_id),
            ..listen
        },
        track_id: track.id.clone(),
    }))
}

/// Tells plugins about one user's listens from one commit, as a single update, right after the
/// commit. Listens skip the playback updates' rate limit: each is announced once, so a limited one
/// would be lost for good. Best effort otherwise: a saturated plugin runtime drops the update.
pub(crate) fn dispatch_recorded(
    user_public_id: &str,
    listens: impl IntoIterator<Item = RecordedListen>,
) {
    let listens = listens
        .into_iter()
        .map(ListenPayload::from)
        .collect::<Vec<_>>();
    if listens.is_empty() {
        return;
    }
    let payload = ListenUpdatePayload {
        event: "recorded".to_string(),
        user_id: user_public_id.to_string(),
        listens,
    };
    send_update("listen", |dispatch| {
        dispatch.dispatch_listen_update(payload)
    });
}

/// The most plays one submission may carry.
pub(crate) const MAX_SUBMITTED_PLAYS: usize = 500;

/// A play a client recorded while it couldn't report live.
#[derive(Clone, Debug)]
pub(crate) struct SubmittedPlay {
    pub(crate) play_id: String,
    pub(crate) track: ResolveId,
    pub(crate) started_at_ms: u64,
    /// Present unless the play is a declared listen; a listen recorded without one stores none.
    pub(crate) played_ms: Option<u64>,
    /// The player already judged the play a listen.
    pub(crate) listened: bool,
}

/// Fails a submitted play that gives the server nothing to judge it by.
pub(crate) fn check_submitted_played_ms(
    played_ms: Option<u64>,
    listened: bool,
) -> Result<(), &'static str> {
    if played_ms.is_none() && !listened {
        return Err("played_ms is required unless listened is true");
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SubmissionOutcome {
    Judged(RecordOutcome),
    /// The track doesn't exist or the user can't see it.
    TrackNotFound,
}

/// Judges the submitted plays in one transaction, keyed by play id, and announces the listens it
/// recorded. A play id repeated in the batch is judged by its first occurrence.
pub(crate) fn record_submitted(
    db: &mut DbAny,
    principal: &Principal,
    user_db_id: DbId,
    plays: Vec<SubmittedPlay>,
    now_ms: u64,
) -> anyhow::Result<HashMap<String, SubmissionOutcome>> {
    let (outcomes, recorded) = db.transaction_mut(|t| {
        let mut public_ids = HashMap::new();
        let mut tracks = HashMap::<DbId, Option<lyra_db::Track>>::new();
        let mut outcomes = HashMap::with_capacity(plays.len());
        let mut recorded = Vec::new();
        for submitted in plays {
            if outcomes.contains_key(&submitted.play_id) {
                continue;
            }
            let track = match submitted_track_db_id(t, &submitted.track, &mut public_ids)? {
                Some(track_db_id) => {
                    let track = match tracks.entry(track_db_id) {
                        Entry::Occupied(entry) => entry.into_mut(),
                        Entry::Vacant(entry) => {
                            entry.insert(accessible_track(t, principal, track_db_id)?)
                        }
                    };
                    track.as_ref().map(|track| (track_db_id, track))
                }
                None => None,
            };
            let outcome = match track {
                Some((track_db_id, track)) => SubmissionOutcome::Judged(record_for_track(
                    t,
                    user_db_id,
                    Play {
                        play_id: &submitted.play_id,
                        track_db_id,
                        started_at_ms: submitted.started_at_ms,
                        played_ms: submitted.played_ms.unwrap_or(0),
                    },
                    submitted.listened.then(|| Declared {
                        played_ms: submitted.played_ms,
                        span_ms: submitted
                            .played_ms
                            .unwrap_or(track.duration_ms.unwrap_or(0)),
                    }),
                    track,
                    now_ms,
                )?),
                None => SubmissionOutcome::TrackNotFound,
            };
            if let SubmissionOutcome::Judged(RecordOutcome::Recorded(listen)) = &outcome {
                recorded.push(listen.clone());
            }
            outcomes.insert(submitted.play_id, outcome);
        }
        Ok::<_, anyhow::Error>((outcomes, recorded))
    })?;
    dispatch_recorded(&principal.user_public_id, recorded);
    Ok(outcomes)
}

fn submitted_track_db_id(
    db: &impl lyra_db::DbAccess,
    track: &ResolveId,
    public_ids: &mut HashMap<String, Option<DbId>>,
) -> anyhow::Result<Option<DbId>> {
    match track {
        ResolveId::DbId(db_id) => Ok(Some(*db_id)),
        ResolveId::Nanoid(public_id) => match public_ids.get(public_id) {
            Some(db_id) => Ok(*db_id),
            None => {
                let db_id = lyra_db::lookup::find_node_id_by_id(db, public_id)?;
                public_ids.insert(public_id.clone(), db_id);
                Ok(db_id)
            }
        },
        // Aliases name root collections, never tracks.
        ResolveId::Alias(_) => Ok(None),
    }
}

fn accessible_track(
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
    track_db_id: DbId,
) -> anyhow::Result<Option<lyra_db::Track>> {
    let Some(track) = lyra_db::tracks::get_by_id(db, track_db_id)? else {
        return Ok(None);
    };
    if !super::auth::access::entity_accessible(db, principal, track_db_id)? {
        return Ok(None);
    }
    Ok(Some(track))
}

/// A viewer's listens to one track, release or artist.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ListenStats {
    pub(crate) count: u64,
    pub(crate) last_played_at_ms: Option<u64>,
}

impl ListenStats {
    /// The listens of several tracks counted as one.
    pub(crate) fn over(totals: impl IntoIterator<Item = lyra_db::listens::ListenTotal>) -> Self {
        let mut stats = Self {
            count: 0,
            last_played_at_ms: None,
        };
        for total in totals {
            stats.count = stats.count.saturating_add(total.count);
            stats.last_played_at_ms = stats.last_played_at_ms.max(Some(total.last_played_at_ms));
        }
        stats
    }
}

/// The viewer's listens to each of the tracks, releases and artists it can see, counted as the
/// catalog counts them to sort by: a release or artist totals the tracks it holds or is credited
/// on. With `unique_track_id_pairs`, a track also counts the listens to every visible track that
/// shares one of those external ids with it. Targets without listens, of another kind, or unseen
/// are absent.
pub(crate) fn stats(
    db: &DbAny,
    viewer: &Viewer,
    target_db_ids: &[DbId],
    unique_track_id_pairs: Option<&HashSet<(String, String)>>,
) -> Result<HashMap<DbId, ListenStats>, CatalogError> {
    let mut tracks = Vec::new();
    let mut releases = Vec::new();
    let mut artists = Vec::new();
    for &target_db_id in target_db_ids {
        if !viewer.sees(db, target_db_id)? {
            continue;
        }
        match lyra_db::entities::metadata_entity_type(db, target_db_id)? {
            Some(MetadataEntityType::Track) => tracks.push(target_db_id),
            Some(MetadataEntityType::Release) => releases.push(target_db_id),
            Some(MetadataEntityType::Artist) => artists.push(target_db_id),
            None => {}
        }
    }
    let mut stats = match unique_track_id_pairs {
        Some(pairs) => merged_track_stats(db, viewer, tracks, pairs)?,
        None => stats_of::<Tracks>(
            db,
            viewer,
            tracks,
            [TrackKey::ListenCount, TrackKey::LastPlayedAt],
        )?,
    };
    stats.extend(stats_of::<Releases>(
        db,
        viewer,
        releases,
        [ReleaseKey::ListenCount, ReleaseKey::LastPlayedAt],
    )?);
    stats.extend(stats_of::<Artists>(
        db,
        viewer,
        artists,
        [ArtistKey::ListenCount, ArtistKey::LastPlayedAt],
    )?);
    Ok(stats)
}

fn stats_of<C: Catalog>(
    db: &DbAny,
    viewer: &Viewer,
    ids: Vec<DbId>,
    keys: [C::Key; 2],
) -> Result<HashMap<DbId, ListenStats>, CatalogError> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let filter = C::id_filter(ids.clone());
    Ok(catalog::numbers::<C>(db, viewer, &filter, ids, &keys)?
        .into_iter()
        .filter_map(listened)
        .collect())
}

fn merged_track_stats(
    db: &DbAny,
    viewer: &Viewer,
    tracks: Vec<DbId>,
    unique_track_id_pairs: &HashSet<(String, String)>,
) -> Result<HashMap<DbId, ListenStats>, CatalogError> {
    let user_db_id = viewer.user_db_id("listen stats")?;
    let mut merged_by_track = Vec::with_capacity(tracks.len());
    for track in tracks {
        let mut merged = Vec::new();
        for merged_id in super::playback_sessions::resolve_merged_track_ids_for_play_count(
            db,
            track,
            unique_track_id_pairs,
        )? {
            if viewer.sees_entity(db, merged_id)? {
                merged.push(merged_id);
            }
        }
        merged_by_track.push((track, merged));
    }
    let totals = lyra_db::listens::totals(db, user_db_id)?;
    Ok(merged_by_track
        .into_iter()
        .filter_map(|(track, merged)| {
            let stats = ListenStats::over(merged.iter().filter_map(|id| totals.get(id)).copied());
            (stats.count > 0).then_some((track, stats))
        })
        .collect())
}

fn listened((id, values): (DbId, Vec<Option<u64>>)) -> Option<(DbId, ListenStats)> {
    let count = values.first().copied().flatten().unwrap_or(0);
    (count > 0).then(|| {
        (
            id,
            ListenStats {
                count,
                last_played_at_ms: values.get(1).copied().flatten(),
            },
        )
    })
}

#[cfg(test)]
mod tests {
    use agdb::DbAny;
    use lyra_db::test_db::new_test_db;

    use super::*;

    const NOW_MS: u64 = 1_800_000_000_000;

    fn insert_track(db: &mut DbAny, duration_ms: Option<u64>) -> anyhow::Result<DbId> {
        let track_db_id = lyra_db::fixtures::insert_track(db, "Track")?;
        let mut track = lyra_db::tracks::get_by_id(db, track_db_id)?
            .ok_or_else(|| anyhow::anyhow!("track missing"))?;
        track.duration_ms = duration_ms;
        lyra_db::tracks::update(db, &track)?;
        Ok(track_db_id)
    }

    fn play(play_id: &str, track_db_id: DbId, played_ms: u64) -> Play<'_> {
        Play {
            play_id,
            track_db_id,
            started_at_ms: NOW_MS - 600_000,
            played_ms,
        }
    }

    fn record_in_transaction(
        db: &mut DbAny,
        user_db_id: DbId,
        play: Play<'_>,
    ) -> anyhow::Result<RecordOutcome> {
        db.transaction_mut(|t| record(t, user_db_id, play, NOW_MS))
    }

    #[test]
    fn min_played_ms_is_thirty_seconds_for_long_or_unknown_durations() {
        assert_eq!(min_played_ms(Some(300_000)), 30_000);
        assert_eq!(min_played_ms(Some(SHORT_TRACK_MS)), 30_000);
        assert_eq!(min_played_ms(None), 30_000);
        assert_eq!(min_played_ms(Some(0)), 30_000);
    }

    #[test]
    fn min_played_ms_is_a_fifth_of_short_tracks_rounded_up() {
        assert_eq!(min_played_ms(Some(60_000)), 12_000);
        assert_eq!(min_played_ms(Some(150_000)), 30_000);
        assert_eq!(min_played_ms(Some(179_999)), 30_000);
        assert_eq!(min_played_ms(Some(1)), 1);
        assert_eq!(min_played_ms(Some(1_001)), 201);
    }

    fn submit_declared(
        db: &mut DbAny,
        user: DbId,
        track: DbId,
        play_id: &str,
        started_at_ms: u64,
        played_ms: Option<u64>,
    ) -> anyhow::Result<SubmissionOutcome> {
        let principal = Principal::for_user(
            db,
            user,
            vec![lyra_db::Permission::Admin],
            std::collections::HashSet::new(),
        );
        let track_public_id =
            lyra_db::lookup::find_id_by_db_id(db, track)?.expect("track public id");
        let mut outcomes = record_submitted(
            db,
            &principal,
            user,
            vec![SubmittedPlay {
                play_id: play_id.to_string(),
                track: ResolveId::Nanoid(track_public_id),
                started_at_ms,
                played_ms,
                listened: true,
            }],
            NOW_MS,
        )?;
        Ok(outcomes.remove(play_id).expect("outcome"))
    }

    fn recorded(outcome: &SubmissionOutcome) -> bool {
        matches!(
            outcome,
            SubmissionOutcome::Judged(RecordOutcome::Recorded(_))
        )
    }

    #[test]
    fn submitted_plays_need_a_playing_time_unless_declared_listens() {
        assert!(check_submitted_played_ms(None, false).is_err());
        assert!(check_submitted_played_ms(None, true).is_ok());
        assert!(check_submitted_played_ms(Some(0), false).is_ok());
    }

    #[test]
    fn a_declared_listen_counts_without_playing_time() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = lyra_db::test_db::insert_user(&mut db, "listener")?;
        let unknown = insert_track(&mut db, None)?;
        let timed = insert_track(&mut db, Some(200_000))?;
        let start = NOW_MS - 600_000;

        let outcome = submit_declared(&mut db, user, unknown, "declared-unknown-1", start, None)?;
        assert!(recorded(&outcome), "{outcome:?}");
        let listen = stored_listen(&db, user, "declared-unknown-1")?;
        assert_eq!(listen.played_ms, None);

        let outcome = submit_declared(
            &mut db,
            user,
            timed,
            "declared-short-001",
            start,
            Some(1_000),
        )?;
        assert!(recorded(&outcome), "{outcome:?}");
        assert_eq!(
            stored_listen(&db, user, "declared-short-001")?.played_ms,
            Some(1_000)
        );

        let outcome = submit_declared(&mut db, user, unknown, "declared-unknown-1", start, None)?;
        assert!(
            matches!(outcome, SubmissionOutcome::Judged(RecordOutcome::Duplicate)),
            "{outcome:?}"
        );
        Ok(())
    }

    #[test]
    fn a_declared_listen_without_playing_time_ends_a_duration_after_its_start() -> anyhow::Result<()>
    {
        let mut db = new_test_db()?;
        let user = lyra_db::test_db::insert_user(&mut db, "listener")?;
        let timed = insert_track(&mut db, Some(200_000))?;
        let unknown = insert_track(&mut db, None)?;
        let latest_end = NOW_MS + FUTURE_END_ALLOWANCE_MS;

        let cases = [
            ("timed-at-limit-01", timed, latest_end - 200_000, true),
            ("timed-too-late-01", timed, latest_end - 199_999, false),
            ("unknown-at-limit1", unknown, latest_end, true),
            ("unknown-too-late1", unknown, latest_end + 1, false),
            (
                "before-2010-00001",
                timed,
                EARLIEST_STARTED_AT_MS - 1,
                false,
            ),
        ];
        for (play_id, track, started_at_ms, valid) in cases {
            let outcome = submit_declared(&mut db, user, track, play_id, started_at_ms, None)?;
            let invalid = matches!(
                outcome,
                SubmissionOutcome::Judged(RecordOutcome::InvalidTime)
            );
            assert_eq!(!invalid, valid, "{play_id}: {outcome:?}");
            assert_eq!(recorded(&outcome), valid, "{play_id}: {outcome:?}");
        }
        Ok(())
    }

    #[test]
    fn a_declared_listen_with_a_playing_time_ends_by_it() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = lyra_db::test_db::insert_user(&mut db, "listener")?;
        let timed = insert_track(&mut db, Some(200_000))?;
        let latest_end = NOW_MS + FUTURE_END_ALLOWANCE_MS;

        let cases = [
            ("zero-at-limit-001", latest_end, Some(0), true),
            ("zero-too-late-001", latest_end + 1, Some(0), false),
            ("timed-at-limit-02", latest_end - 1_000, Some(1_000), true),
            ("timed-too-late-02", latest_end - 999, Some(1_000), false),
            ("inferred-at-now01", NOW_MS, None, false),
        ];
        for (play_id, started_at_ms, played_ms, valid) in cases {
            let outcome = submit_declared(&mut db, user, timed, play_id, started_at_ms, played_ms)?;
            assert_eq!(recorded(&outcome), valid, "{play_id}: {outcome:?}");
        }
        Ok(())
    }

    #[test]
    fn a_declared_listen_keeps_the_longest_playing_time_given() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = lyra_db::test_db::insert_user(&mut db, "listener")?;
        let track = insert_track(&mut db, Some(200_000))?;
        let start = NOW_MS - 600_000;
        let stored = |db: &DbAny| -> anyhow::Result<Option<u64>> {
            Ok(stored_listen(db, user, "declared-merge-001")?.played_ms)
        };

        submit_declared(&mut db, user, track, "declared-merge-001", start, None)?;
        assert_eq!(
            stored(&db)?,
            None,
            "a declared listen without a playing time"
        );

        for (played_ms, expected) in [
            (Some(50_000), Some(50_000)),
            (Some(20_000), Some(50_000)),
            (None, Some(50_000)),
            (Some(80_000), Some(80_000)),
        ] {
            let outcome =
                submit_declared(&mut db, user, track, "declared-merge-001", start, played_ms)?;
            assert!(
                matches!(outcome, SubmissionOutcome::Judged(RecordOutcome::Duplicate)),
                "{outcome:?}"
            );
            assert_eq!(stored(&db)?, expected, "after {played_ms:?}");
        }

        let live = record_in_transaction(
            &mut db,
            user,
            Play {
                started_at_ms: start,
                ..play("declared-merge-001", track, 120_000)
            },
        )?;
        assert_eq!(live, RecordOutcome::Duplicate);
        assert_eq!(stored(&db)?, Some(120_000), "a live report raises it too");
        Ok(())
    }

    #[test]
    fn record_judges_against_the_catalog_duration() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = lyra_db::test_db::insert_user(&mut db, "listener")?;
        let short = insert_track(&mut db, Some(60_000))?;
        let near_long = insert_track(&mut db, Some(170_000))?;
        let long = insert_track(&mut db, Some(300_000))?;
        let unknown = insert_track(&mut db, None)?;

        for (play_id, track, played_ms, recorded) in [
            ("short-below-00001", short, 11_999, false),
            ("short-at-0000001", short, 12_000, true),
            ("near-long-at-0001", near_long, 30_000, true),
            ("long-below-000001", long, 29_999, false),
            ("long-at-00000001", long, 30_000, true),
            ("unknown-below-001", unknown, 29_999, false),
            ("unknown-at-000001", unknown, 30_000, true),
        ] {
            let outcome = record_in_transaction(&mut db, user, play(play_id, track, played_ms))?;
            assert_eq!(
                matches!(outcome, RecordOutcome::Recorded(_)),
                recorded,
                "{play_id}: {outcome:?}"
            );
            if !recorded {
                assert!(
                    matches!(outcome, RecordOutcome::NotListened),
                    "{play_id}: {outcome:?}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn record_stores_the_play() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = lyra_db::test_db::insert_user(&mut db, "listener")?;
        let track = insert_track(&mut db, Some(200_000))?;
        let track_public_id = lyra_db::tracks::get_by_id(&db, track)?
            .ok_or_else(|| anyhow::anyhow!("track missing"))?
            .id;

        let outcome =
            record_in_transaction(&mut db, user, play("play-stored-0001", track, 45_000))?;
        assert!(
            matches!(&outcome, RecordOutcome::Recorded(recorded) if recorded.track_id == track_public_id),
            "{outcome:?}"
        );
        let listen_db_id = lyra_db::listens::find_by_play_id(&db, user, "play-stored-0001")?
            .ok_or_else(|| anyhow::anyhow!("listen missing"))?;
        let listen = lyra_db::graph::bulk_fetch_typed::<Listen>(&db, vec![listen_db_id], "Listen")?
            .remove(&listen_db_id)
            .ok_or_else(|| anyhow::anyhow!("listen missing"))?;
        assert_eq!(listen.play_id, "play-stored-0001");
        assert_eq!(lyra_db::listens::track_of(&db, listen_db_id)?, Some(track));
        assert_eq!(listen.started_at_ms, NOW_MS - 600_000);
        assert_eq!(listen.played_ms, Some(45_000));
        assert_eq!(listen.created_at_ms, NOW_MS);

        assert_eq!(
            lyra_db::listens::total(&db, user, track)?,
            Some(lyra_db::listens::ListenTotal {
                count: 1,
                last_played_at_ms: NOW_MS - 600_000,
            })
        );
        Ok(())
    }

    #[test]
    fn record_counts_each_play_once_per_user() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = lyra_db::test_db::insert_user(&mut db, "listener")?;
        let other_user = lyra_db::test_db::insert_user(&mut db, "other")?;
        let track = insert_track(&mut db, None)?;

        let first = record_in_transaction(&mut db, user, play("play-repeat-0001", track, 30_000))?;
        assert!(matches!(first, RecordOutcome::Recorded(_)), "{first:?}");
        let retry = record_in_transaction(&mut db, user, play("play-repeat-0001", track, 90_000))?;
        assert!(matches!(retry, RecordOutcome::Duplicate), "{retry:?}");
        let shorter =
            record_in_transaction(&mut db, user, play("play-repeat-0001", track, 40_000))?;
        assert!(matches!(shorter, RecordOutcome::Duplicate), "{shorter:?}");
        let listen_db_id = lyra_db::listens::find_by_play_id(&db, user, "play-repeat-0001")?
            .ok_or_else(|| anyhow::anyhow!("listen missing"))?;
        let listen = lyra_db::graph::bulk_fetch_typed::<Listen>(&db, vec![listen_db_id], "Listen")?
            .remove(&listen_db_id)
            .ok_or_else(|| anyhow::anyhow!("listen missing"))?;
        assert_eq!(
            listen.played_ms,
            Some(90_000),
            "a duplicate keeps the longest playing time"
        );
        let other =
            record_in_transaction(&mut db, other_user, play("play-repeat-0001", track, 30_000))?;
        assert!(matches!(other, RecordOutcome::Recorded(_)), "{other:?}");

        let total = lyra_db::listens::total(&db, user, track)?;
        assert_eq!(total.map(|total| total.count), Some(1));
        Ok(())
    }

    #[test]
    fn record_allows_a_play_that_did_not_count_to_count_later() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = lyra_db::test_db::insert_user(&mut db, "listener")?;
        let track = insert_track(&mut db, None)?;

        let early = record_in_transaction(&mut db, user, play("play-growing-0001", track, 10_000))?;
        assert!(matches!(early, RecordOutcome::NotListened), "{early:?}");
        let later = record_in_transaction(&mut db, user, play("play-growing-0001", track, 30_000))?;
        assert!(matches!(later, RecordOutcome::Recorded(_)), "{later:?}");
        Ok(())
    }

    #[test]
    fn record_rejects_implausible_times() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = lyra_db::test_db::insert_user(&mut db, "listener")?;
        let track = insert_track(&mut db, None)?;

        let cases = [
            (
                "before-2010-00001",
                EARLIEST_STARTED_AT_MS - 1,
                30_000,
                false,
            ),
            ("at-2010-000000001", EARLIEST_STARTED_AT_MS, 30_000, true),
            ("ends-too-late-001", NOW_MS + 30_001, 30_000, false),
            ("ends-at-limit-001", NOW_MS + 30_000, 30_000, true),
            ("overflowing-00001", u64::MAX, 30_000, false),
        ];
        for (play_id, started_at_ms, played_ms, valid) in cases {
            let outcome = record_in_transaction(
                &mut db,
                user,
                Play {
                    play_id,
                    track_db_id: track,
                    started_at_ms,
                    played_ms,
                },
            )?;
            assert_eq!(
                !matches!(outcome, RecordOutcome::InvalidTime),
                valid,
                "{play_id}: {outcome:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn record_rolls_back_with_the_callers_transaction() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = lyra_db::test_db::insert_user(&mut db, "listener")?;
        let track = insert_track(&mut db, None)?;

        let result = db.transaction_mut(|t| -> anyhow::Result<()> {
            let outcome = record(t, user, play("play-rolled-0001", track, 30_000), NOW_MS)?;
            anyhow::ensure!(matches!(outcome, RecordOutcome::Recorded(_)));
            anyhow::bail!("caller fails after recording")
        });
        assert!(result.is_err());
        assert_eq!(
            lyra_db::listens::find_by_play_id(&db, user, "play-rolled-0001")?,
            None
        );
        Ok(())
    }
    fn stored_listen(db: &DbAny, user: DbId, play_id: &str) -> anyhow::Result<Listen> {
        lyra_db::listens::get_by_play_id(db, user, play_id)?
            .ok_or_else(|| anyhow::anyhow!("no listen for {play_id}"))
    }

    #[test]
    fn a_play_of_a_removed_track_stays_a_duplicate() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = lyra_db::test_db::insert_user(&mut db, "listener")?;
        let gone = insert_track(&mut db, None)?;
        let other = insert_track(&mut db, None)?;
        record_in_transaction(&mut db, user, play("play-orphaned-0001", gone, 30_000))?;
        db.exec_mut(agdb::QueryBuilder::remove().ids(gone).query())?;

        let again =
            record_in_transaction(&mut db, user, play("play-orphaned-0001", other, 90_000))?;

        assert_eq!(again, RecordOutcome::Duplicate);
        assert_eq!(
            stored_listen(&db, user, "play-orphaned-0001")?.played_ms,
            Some(30_000)
        );
        assert_eq!(lyra_db::listens::total(&db, user, other)?, None);
        Ok(())
    }

    #[test]
    fn a_duplicate_raises_playing_time_only_as_far_as_the_counted_play_allows() -> anyhow::Result<()>
    {
        let mut db = new_test_db()?;
        let user = lyra_db::test_db::insert_user(&mut db, "listener")?;
        let track = insert_track(&mut db, None)?;
        let other_track = insert_track(&mut db, None)?;
        let started_at_ms = NOW_MS - 600_000;
        record_in_transaction(&mut db, user, play("play-bounded-0001", track, 30_000))?;

        let other = record_in_transaction(
            &mut db,
            user,
            Play {
                track_db_id: other_track,
                ..play("play-bounded-0001", track, 200_000)
            },
        )?;
        assert_eq!(other, RecordOutcome::Duplicate);
        assert_eq!(
            stored_listen(&db, user, "play-bounded-0001")?.played_ms,
            Some(30_000)
        );

        let fabricated = record_in_transaction(
            &mut db,
            user,
            Play {
                started_at_ms: EARLIEST_STARTED_AT_MS - 1,
                ..play("play-bounded-0001", track, u64::MAX)
            },
        )?;
        assert_eq!(
            fabricated,
            RecordOutcome::Duplicate,
            "a counted play stays a duplicate"
        );
        assert_eq!(
            stored_listen(&db, user, "play-bounded-0001")?.played_ms,
            Some(NOW_MS + FUTURE_END_ALLOWANCE_MS - started_at_ms)
        );
        Ok(())
    }
}
