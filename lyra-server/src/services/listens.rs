// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! What counts as a listen, for live playback and submitted plays alike.

use std::collections::{
    HashMap,
    hash_map::Entry,
};

use agdb::{
    DbAny,
    DbId,
};
use lyra_db::{
    Listen,
    ResolveId,
};
use nanoid::nanoid;

use serde::{
    Deserialize,
    Serialize,
};

use super::auth::Principal;

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
    pub played_ms: u64,
}

impl From<Listen> for ListenPayload {
    fn from(listen: Listen) -> Self {
        Self {
            listen_id: listen.id,
            track_id: listen.track_public_id,
            started_at_ms: listen.started_at_ms,
            played_ms: listen.played_ms,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RecordOutcome {
    /// The play counted; the new listen is announced once its transaction commits.
    Recorded(Listen),
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
    record_for_track(db, user_db_id, play, &track, now_ms)
}

/// [`record`] for a play of `track`, already loaded from `play.track_db_id`.
fn record_for_track(
    db: &mut impl lyra_db::DbAccess,
    user_db_id: DbId,
    play: Play<'_>,
    track: &lyra_db::Track,
    now_ms: u64,
) -> anyhow::Result<RecordOutcome> {
    let latest_end_ms = now_ms.saturating_add(FUTURE_END_ALLOWANCE_MS);
    if let Some(listen) = lyra_db::listens::get_by_play_id(db, user_db_id, play.play_id)? {
        // Never beyond what fits between the counted start and now.
        let played_ms = play
            .played_ms
            .min(latest_end_ms.saturating_sub(listen.started_at_ms));
        if listen.track_public_id == track.id
            && played_ms > listen.played_ms
            && let Some(listen_db_id) = listen.db_id
        {
            lyra_db::listens::set_played_ms(db, listen_db_id, played_ms)?;
        }
        return Ok(RecordOutcome::Duplicate);
    }
    if play.started_at_ms < EARLIEST_STARTED_AT_MS
        || play.started_at_ms.saturating_add(play.played_ms) > latest_end_ms
    {
        return Ok(RecordOutcome::InvalidTime);
    }
    if !is_listen(play.played_ms, track.duration_ms) {
        return Ok(RecordOutcome::NotListened);
    }

    let listen = Listen {
        db_id: None,
        id: nanoid!(),
        play_id: play.play_id.to_string(),
        track_public_id: track.id.clone(),
        started_at_ms: play.started_at_ms,
        played_ms: play.played_ms,
        created_at_ms: now_ms,
    };
    let db_id = lyra_db::listens::insert(db, &listen, play.track_db_id, user_db_id)?;
    Ok(RecordOutcome::Recorded(Listen {
        db_id: Some(db_id),
        ..listen
    }))
}

/// Tells plugins about one user's listens from one commit, as a single update, right after the
/// commit. Listens skip the playback updates' rate limit: each is announced once, so a limited one
/// would be lost for good. Best effort otherwise: a saturated plugin runtime drops the update.
pub(crate) fn dispatch_recorded(user_public_id: &str, listens: impl IntoIterator<Item = Listen>) {
    let listens = listens
        .into_iter()
        .map(ListenPayload::from)
        .collect::<Vec<_>>();
    if listens.is_empty() {
        return;
    }
    let Some(runtime) = super::STATE.generation().plugin_dispatch.get() else {
        return;
    };
    let payload = ListenUpdatePayload {
        event: "recorded".to_string(),
        user_id: user_public_id.to_string(),
        listens,
    };
    if let Err(error) = runtime.dispatch_listen_update(payload) {
        tracing::warn!(error = %error, "failed to enqueue plugin listen on_update dispatch");
    }
}

/// The most plays one submission may carry.
pub(crate) const MAX_SUBMITTED_PLAYS: usize = 500;

/// A play a client recorded while it couldn't report live.
#[derive(Clone, Debug)]
pub(crate) struct SubmittedPlay {
    pub(crate) play_id: String,
    pub(crate) track: ResolveId,
    pub(crate) started_at_ms: u64,
    pub(crate) played_ms: u64,
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
                        played_ms: submitted.played_ms,
                    },
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
        assert!(matches!(outcome, RecordOutcome::Recorded(_)), "{outcome:?}");
        let listen_db_id = lyra_db::listens::find_by_play_id(&db, user, "play-stored-0001")?
            .ok_or_else(|| anyhow::anyhow!("listen missing"))?;
        let listen = lyra_db::graph::bulk_fetch_typed::<Listen>(&db, vec![listen_db_id], "Listen")?
            .remove(&listen_db_id)
            .ok_or_else(|| anyhow::anyhow!("listen missing"))?;
        assert_eq!(listen.play_id, "play-stored-0001");
        assert_eq!(listen.track_public_id, track_public_id);
        assert_eq!(listen.started_at_ms, NOW_MS - 600_000);
        assert_eq!(listen.played_ms, 45_000);
        assert_eq!(listen.created_at_ms, NOW_MS);

        let stats = lyra_db::listens::get_stats(&db, &[track], user)?;
        assert_eq!(stats[0].count, 1);
        assert_eq!(stats[0].last_played, Some(NOW_MS - 600_000));
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
            listen.played_ms, 90_000,
            "a duplicate keeps the longest playing time"
        );
        let other =
            record_in_transaction(&mut db, other_user, play("play-repeat-0001", track, 30_000))?;
        assert!(matches!(other, RecordOutcome::Recorded(_)), "{other:?}");

        let stats = lyra_db::listens::get_stats(&db, &[track], user)?;
        assert_eq!(stats[0].count, 1);
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
            30_000
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
            NOW_MS + FUTURE_END_ALLOWANCE_MS - started_at_ms
        );
        Ok(())
    }
}
