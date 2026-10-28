// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! What counts as a listen, for live playback and submitted plays alike.

use agdb::DbId;
use lyra_db::Listen;
use nanoid::nanoid;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RecordOutcome {
    Recorded,
    /// The user already has a listen for this play.
    Duplicate,
    /// The play was too short to count.
    NotListened,
    /// The play started before the earliest plausible time or ends in the future.
    InvalidTime,
}

/// The playing time a play needs to count as a listen of a track with this catalog duration.
pub(crate) fn min_played_ms(duration_ms: Option<u64>) -> u64 {
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

pub(crate) fn is_listen(played_ms: u64, duration_ms: Option<u64>) -> bool {
    played_ms >= min_played_ms(duration_ms)
}

/// Records the play as the user's listen when it qualifies, inside the caller's transaction.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "playback and listen submission adopt `record` next"
    )
)]
pub(crate) fn record(
    db: &mut impl lyra_db::DbAccess,
    user_db_id: DbId,
    play: Play<'_>,
    now_ms: u64,
) -> anyhow::Result<RecordOutcome> {
    if play.started_at_ms < EARLIEST_STARTED_AT_MS
        || play.started_at_ms.saturating_add(play.played_ms)
            > now_ms.saturating_add(FUTURE_END_ALLOWANCE_MS)
    {
        return Ok(RecordOutcome::InvalidTime);
    }
    if lyra_db::listens::find_by_play_id(db, user_db_id, play.play_id)?.is_some() {
        return Ok(RecordOutcome::Duplicate);
    }
    let track = lyra_db::tracks::get_by_id(db, play.track_db_id)?
        .ok_or_else(|| anyhow::anyhow!("track {:?} not found", play.track_db_id))?;
    if !is_listen(play.played_ms, track.duration_ms) {
        return Ok(RecordOutcome::NotListened);
    }

    let listen = Listen {
        db_id: None,
        id: nanoid!(),
        play_id: play.play_id.to_string(),
        track_public_id: track.id,
        started_at_ms: play.started_at_ms,
        played_ms: play.played_ms,
        created_at_ms: now_ms,
    };
    lyra_db::listens::insert(db, &listen, play.track_db_id, user_db_id)?;
    Ok(RecordOutcome::Recorded)
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
                matches!(outcome, RecordOutcome::Recorded),
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
        assert_eq!(outcome, RecordOutcome::Recorded);
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
        assert!(matches!(first, RecordOutcome::Recorded), "{first:?}");
        let retry = record_in_transaction(&mut db, user, play("play-repeat-0001", track, 90_000))?;
        assert!(matches!(retry, RecordOutcome::Duplicate), "{retry:?}");
        let other =
            record_in_transaction(&mut db, other_user, play("play-repeat-0001", track, 30_000))?;
        assert!(matches!(other, RecordOutcome::Recorded), "{other:?}");

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
        assert!(matches!(later, RecordOutcome::Recorded), "{later:?}");
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
            anyhow::ensure!(matches!(outcome, RecordOutcome::Recorded));
            anyhow::bail!("caller fails after recording")
        });
        assert!(result.is_err());
        assert_eq!(
            lyra_db::listens::find_by_play_id(&db, user, "play-rolled-0001")?,
            None
        );
        Ok(())
    }
}
