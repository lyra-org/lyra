// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    fmt,
    time::{
        SystemTime,
        UNIX_EPOCH,
    },
};

use agdb::DbId;

use serde::Deserialize;

mod updates;
mod workflow;

#[cfg(test)]
pub(crate) use self::workflow::{
    collect_playback_records,
    report_playback,
};
pub(crate) use self::workflow::{
    current_position_ms,
    evicted_records,
    pause_playback_in_transaction,
    report_playback_in_transaction,
    resolve_merged_track_ids_for_play_count,
    start_playback_in_transaction,
};
pub(crate) use lyra_db::PlaybackSession;
use lyra_db::PlaybackState;
pub(crate) use updates::{
    PlaybackUpdatePayload,
    PlaybackUpdateRegistries,
    dispatch_evicted_updates,
    dispatch_evicted_updates_for_caller,
    dispatch_playback_update,
    dispatch_playback_update_for_caller,
    notify_connection_change,
    subscribe_playback_events,
    unsubscribe_playback_events,
    watch_connection_changes,
};

pub(crate) const ACTIVE_SESSION_TTL_MS: u64 = 5 * 60 * 1000;
pub(crate) const PREVIOUS_PLAYBACK_GRACE_MS: u64 = 30_000;
/// Playing time a play may be credited beyond the server's elapsed time, to absorb report latency.
const PLAY_LATENCY_ALLOWANCE_MS: u64 = 5_000;

#[derive(Debug, thiserror::Error)]
pub(crate) enum PlaybackServiceError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    /// The play already has a session that isn't the one being reported to.
    #[error("play {0} already has a session")]
    PlayConflict(String),
    #[error(transparent)]
    Internal(anyhow::Error),
}

impl PlaybackServiceError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest(message.into())
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound(message.into())
    }

    fn internal(error: impl Into<anyhow::Error>) -> Self {
        Self::Internal(error.into())
    }
}

impl From<agdb::DbError> for PlaybackServiceError {
    fn from(error: agdb::DbError) -> Self {
        Self::internal(error)
    }
}

pub(crate) type ServiceResult<T> = std::result::Result<T, PlaybackServiceError>;

#[derive(Clone, Debug, Default)]
pub(crate) struct PlaybackMutation {
    pub(crate) position_ms: Option<u64>,
    pub(crate) duration_ms: Option<u64>,
    pub(crate) state: Option<PlaybackState>,
}

#[derive(Clone, Debug)]
pub(crate) struct StartPlaybackRequest {
    pub(crate) play_id: String,
    pub(crate) track_db_id: DbId,
    pub(crate) user_db_id: DbId,
    pub(crate) client_name: Option<String>,
    /// The reporting plugin; `None` for Lyra's own clients.
    pub(crate) plugin_id: Option<String>,
    pub(crate) mutation: PlaybackMutation,
    pub(crate) now_ms: u64,
    pub(crate) active_event: ActiveEvent,
}

#[derive(Clone, Debug)]
pub(crate) struct ReportPlaybackRequest {
    /// The playback's owner; reports against anyone else's playback are rejected.
    pub(crate) user_db_id: DbId,
    pub(crate) mutation: PlaybackMutation,
    pub(crate) now_ms: u64,
    pub(crate) active_event: ActiveEvent,
}

#[derive(Clone, Debug)]
pub(crate) struct PlaybackRecord {
    pub(crate) playback_session_id: DbId,
    pub(crate) playback_session_public_id: String,
    pub(crate) track_db_id: DbId,
    pub(crate) track_public_id: String,
    pub(crate) user_db_id: DbId,
    pub(crate) user_public_id: String,
    pub(crate) library_public_id: Option<String>,
    pub(crate) playback: PlaybackSession,
}

#[derive(Clone, Debug)]
pub(crate) struct TransactionPlaybackUpdate {
    pub(crate) playback: PlaybackRecord,
    pub(crate) event: String,
    /// The listen the report recorded, for the transaction's owner to announce after commit.
    pub(crate) recorded_listen: Option<lyra_db::Listen>,
}

impl TransactionPlaybackUpdate {
    /// Announces the recorded listen, if any; call right after the transaction commits.
    pub(crate) fn announce_recorded_listen(&self) {
        crate::services::listens::dispatch_recorded(
            &self.playback.user_public_id,
            self.recorded_listen.clone(),
        );
    }
}

#[derive(Clone, Debug)]
pub(crate) struct EvictedPlaybackRecord {
    pub(crate) playback_session_id: DbId,
    pub(crate) playback_session_public_id: String,
    pub(crate) track_db_id: DbId,
    pub(crate) track_public_id: String,
    pub(crate) user_db_id: DbId,
    pub(crate) user_public_id: String,
    pub(crate) library_public_id: Option<String>,
    pub(crate) playback: PlaybackSession,
}

impl From<EvictedPlaybackRecord> for PlaybackRecord {
    fn from(value: EvictedPlaybackRecord) -> Self {
        Self {
            playback_session_id: value.playback_session_id,
            playback_session_public_id: value.playback_session_public_id,
            track_db_id: value.track_db_id,
            track_public_id: value.track_public_id,
            user_db_id: value.user_db_id,
            user_public_id: value.user_public_id,
            library_public_id: value.library_public_id,
            playback: value.playback,
        }
    }
}

pub(crate) fn now_ms() -> ServiceResult<u64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| PlaybackServiceError::bad_request("system clock is before unix epoch"))?;
    Ok(now.as_millis() as u64)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ActiveEvent {
    Started,
    Progress,
}

impl std::fmt::Display for ActiveEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Started => f.write_str("started"),
            Self::Progress => f.write_str("progress"),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum PlaybackEvent {
    Started,
    Progress,
    Stopped,
    Completed,
}

impl fmt::Display for PlaybackEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Started => f.write_str("started"),
            Self::Progress => f.write_str("progress"),
            Self::Stopped => f.write_str("stopped"),
            Self::Completed => f.write_str("completed"),
        }
    }
}

impl PlaybackEvent {
    fn to_active_event(self) -> ActiveEvent {
        match self {
            Self::Started => ActiveEvent::Started,
            _ => ActiveEvent::Progress,
        }
    }
}

/// Classifies a raw event string into an active-session event category.
/// Terminal events (stopped/completed) map to Progress because the terminal
/// state is recorded via PlaybackState, not the event — see `event_for_state`.
pub(crate) fn classify_active_event(event: Option<PlaybackEvent>) -> ServiceResult<ActiveEvent> {
    Ok(event.unwrap_or(PlaybackEvent::Progress).to_active_event())
}

#[cfg(test)]
mod tests {
    use super::workflow::{
        apply_playback_mutation,
        collect_unique_track_external_id_keys,
    };
    use super::*;
    use agdb::DbAny;
    use agdb::QueryBuilder;
    use lyra_db::IdSource;
    use lyra_db::NodeId;
    use lyra_db::roots::{
        self,
        Root,
    };
    use lyra_db::test_db::new_test_db;
    use nanoid::nanoid;
    use std::collections::HashSet;
    use std::iter::FromIterator;

    fn insert_track(db: &mut DbAny, title: &str, duration_ms: u64) -> anyhow::Result<DbId> {
        let track = lyra_db::Track {
            db_id: None,
            id: nanoid!(),
            track_title: title.to_string(),
            sort_title: None,
            year: None,
            disc: None,
            disc_total: None,
            track: None,
            track_total: None,
            duration_ms: Some(duration_ms),
            track_gain_db: None,
            album_gain_db: None,
            locked: Some(false),
            created_at_ms: Some(0),
            file_created_at_ms: Some(0),
        };
        let track_db_id = db
            .exec_mut(QueryBuilder::insert().element(&track).query())?
            .ids()[0];
        roots::link(db, Root::Tracks, track_db_id)?;
        Ok(track_db_id)
    }

    #[test]
    fn unique_track_external_id_key_filter_matches_declared_pairs_only() {
        let external_ids = vec![
            lyra_db::external_ids::ExternalId {
                db_id: Some(NodeId::from(DbId(1))),
                provider_id: "musicbrainz".to_string(),
                id_type: "recording_id".to_string(),
                id_value: "abc123".to_string(),
                source: IdSource::Resolved,
                resolved_value: None,
            },
            lyra_db::external_ids::ExternalId {
                db_id: Some(NodeId::from(DbId(2))),
                provider_id: "musicbrainz".to_string(),
                id_type: "release_id".to_string(),
                id_value: "release-1".to_string(),
                source: IdSource::Resolved,
                resolved_value: None,
            },
            lyra_db::external_ids::ExternalId {
                db_id: Some(NodeId::from(DbId(3))),
                provider_id: "discogs".to_string(),
                id_type: "recording_id".to_string(),
                id_value: "x".to_string(),
                source: IdSource::Resolved,
                resolved_value: None,
            },
        ];
        let unique_track_id_pairs =
            HashSet::from_iter([("musicbrainz".to_string(), "recording_id".to_string())]);

        let keys = collect_unique_track_external_id_keys(&external_ids, &unique_track_id_pairs);
        let expected = HashSet::from_iter([(
            "musicbrainz".to_string(),
            "recording_id".to_string(),
            "abc123".to_string(),
        )]);
        assert_eq!(keys, expected);
    }

    #[test]
    fn unique_track_external_id_key_filter_ignores_blank_values() {
        let external_ids = vec![lyra_db::external_ids::ExternalId {
            db_id: Some(NodeId::from(DbId(1))),
            provider_id: "musicbrainz".to_string(),
            id_type: "recording_id".to_string(),
            id_value: "   ".to_string(),
            source: IdSource::Resolved,
            resolved_value: None,
        }];
        let unique_track_id_pairs =
            HashSet::from_iter([("musicbrainz".to_string(), "recording_id".to_string())]);

        let keys = collect_unique_track_external_id_keys(&external_ids, &unique_track_id_pairs);
        assert!(keys.is_empty());
    }

    #[test]
    fn resolve_merged_track_ids_finds_tracks_sharing_external_ids() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let track_a = insert_track(&mut db, "Track A", 200_000)?;
        let track_b = insert_track(&mut db, "Track B", 200_000)?;
        let track_c = insert_track(&mut db, "Track C", 200_000)?;

        lyra_db::external_ids::upsert(
            &mut db,
            track_a,
            "musicbrainz",
            "recording_id",
            "rec-123",
            lyra_db::external_ids::IdSource::Resolved,
        )?;
        lyra_db::external_ids::upsert(
            &mut db,
            track_b,
            "musicbrainz",
            "recording_id",
            " rec-123 ",
            lyra_db::external_ids::IdSource::Manual,
        )?;
        let artist = lyra_db::fixtures::insert_artist(&mut db, "Not A Track")?;
        lyra_db::external_ids::upsert(
            &mut db,
            artist,
            "musicbrainz",
            "recording_id",
            "rec-123",
            lyra_db::external_ids::IdSource::Resolved,
        )?;
        lyra_db::external_ids::upsert(
            &mut db,
            track_c,
            "musicbrainz",
            "recording_id",
            "rec-other",
            lyra_db::external_ids::IdSource::Resolved,
        )?;

        let pairs = HashSet::from_iter([("musicbrainz".to_string(), "recording_id".to_string())]);
        let mut merged = resolve_merged_track_ids_for_play_count(&db, track_a, &pairs)?;
        merged.sort_by_key(|id| id.0);
        let mut expected = vec![track_a, track_b];
        expected.sort_by_key(|id| id.0);
        assert_eq!(merged, expected);

        Ok(())
    }

    #[test]
    fn resolve_merged_track_ids_returns_self_when_no_pairs() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track_db_id = insert_track(&mut db, "Solo Track", 200_000)?;

        let empty_pairs = HashSet::new();
        let merged = resolve_merged_track_ids_for_play_count(&db, track_db_id, &empty_pairs)?;
        assert_eq!(merged, vec![track_db_id]);

        Ok(())
    }

    fn playing_session(position_ms: u64, at_ms: u64) -> PlaybackSession {
        PlaybackSession {
            db_id: None,
            id: nanoid!(),
            play_id: nanoid!(),
            client_name: None,
            plugin_id: None,
            position_ms,
            duration_ms: Some(300_000),
            played_ms: 0,
            last_position_ms: position_ms,
            last_position_at_ms: at_ms,
            latency_allowance_used_ms: 0,
            state: PlaybackState::Playing,
            updated_at_ms: at_ms,
            created_at_ms: 0,
        }
    }

    #[test]
    fn current_position_advances_from_the_last_reported_position_while_playing() {
        let mut playback = playing_session(10_000, 1_000);
        // A report without a position leaves the position's time alone.
        playback.updated_at_ms = 5_000;
        assert_eq!(current_position_ms(&playback, 11_000), 20_000);
        assert_eq!(current_position_ms(&playback, 1_000_000), 300_000);

        playback.state = PlaybackState::Paused;
        assert_eq!(current_position_ms(&playback, 11_000), 10_000);
    }

    #[test]
    fn playback_mutation_counts_sparse_progress_with_elapsed_time_window() {
        let mut playback = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            client_name: None,
            plugin_id: None,
            position_ms: 0,
            duration_ms: Some(300_000),
            played_ms: 0,
            last_position_ms: 0,
            state: PlaybackState::Playing,
            play_id: nanoid::nanoid!(),
            latency_allowance_used_ms: 0,
            last_position_at_ms: 1_000,
            updated_at_ms: 1_000,
            created_at_ms: 0,
        };

        apply_playback_mutation(
            &mut playback,
            &PlaybackMutation {
                position_ms: Some(150_000),
                duration_ms: None,
                state: Some(PlaybackState::Playing),
            },
            151_000,
        );

        assert_eq!(playback.played_ms, 150_000);
    }

    #[test]
    fn playback_mutation_caps_a_forward_seek_at_elapsed_time_and_latency_allowance() {
        let mut playback = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            client_name: None,
            plugin_id: None,
            position_ms: 0,
            duration_ms: Some(300_000),
            played_ms: 0,
            last_position_ms: 0,
            state: PlaybackState::Playing,
            play_id: nanoid::nanoid!(),
            latency_allowance_used_ms: 0,
            last_position_at_ms: 1_000,
            updated_at_ms: 1_000,
            created_at_ms: 0,
        };

        apply_playback_mutation(
            &mut playback,
            &PlaybackMutation {
                position_ms: Some(30_000),
                duration_ms: None,
                state: Some(PlaybackState::Playing),
            },
            11_000,
        );

        assert_eq!(playback.played_ms, 15_000);
        assert_eq!(
            playback.latency_allowance_used_ms,
            PLAY_LATENCY_ALLOWANCE_MS
        );
    }

    #[test]
    fn playback_mutation_spends_the_latency_allowance_once_per_play() {
        let mut playback = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            play_id: nanoid!(),
            client_name: None,
            plugin_id: None,
            position_ms: 0,
            duration_ms: Some(300_000),
            played_ms: 0,
            last_position_ms: 0,
            latency_allowance_used_ms: 0,
            state: PlaybackState::Playing,
            last_position_at_ms: 1_000,
            updated_at_ms: 1_000,
            created_at_ms: 0,
        };

        // A burst of reports, each claiming 4 s of progress with no time passing.
        for report in 1..=10 {
            apply_playback_mutation(
                &mut playback,
                &PlaybackMutation {
                    position_ms: Some(report * 4_000),
                    duration_ms: None,
                    state: Some(PlaybackState::Playing),
                },
                1_000,
            );
        }

        assert_eq!(playback.played_ms, PLAY_LATENCY_ALLOWANCE_MS);
        assert_eq!(
            playback.latency_allowance_used_ms,
            PLAY_LATENCY_ALLOWANCE_MS
        );
    }

    #[test]
    fn playback_mutation_credits_progress_up_to_a_pause() {
        let mut playback = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            client_name: None,
            plugin_id: None,
            position_ms: 0,
            duration_ms: Some(300_000),
            played_ms: 0,
            last_position_ms: 0,
            state: PlaybackState::Playing,
            play_id: nanoid::nanoid!(),
            latency_allowance_used_ms: 0,
            last_position_at_ms: 1_000,
            updated_at_ms: 1_000,
            created_at_ms: 0,
        };

        apply_playback_mutation(
            &mut playback,
            &PlaybackMutation {
                position_ms: Some(5_000),
                duration_ms: None,
                state: Some(PlaybackState::Paused),
            },
            6_000,
        );

        assert_eq!(playback.played_ms, 5_000);
    }

    #[test]
    fn playback_mutation_credits_the_final_report_after_playing() {
        let mut playback = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            client_name: None,
            plugin_id: None,
            position_ms: 22_000,
            duration_ms: Some(200_000),
            played_ms: 22_000,
            last_position_ms: 22_000,
            state: PlaybackState::Playing,
            play_id: nanoid::nanoid!(),
            latency_allowance_used_ms: 0,
            last_position_at_ms: 22_000,
            updated_at_ms: 22_000,
            created_at_ms: 0,
        };

        apply_playback_mutation(
            &mut playback,
            &PlaybackMutation {
                position_ms: Some(196_000),
                duration_ms: None,
                state: Some(PlaybackState::Stopped),
            },
            197_000,
        );

        assert_eq!(playback.played_ms, 196_000);
    }

    #[test]
    fn playback_mutation_does_not_credit_progress_reported_while_paused() {
        let mut playback = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            client_name: None,
            plugin_id: None,
            position_ms: 22_000,
            duration_ms: Some(200_000),
            played_ms: 22_000,
            last_position_ms: 22_000,
            state: PlaybackState::Paused,
            play_id: nanoid::nanoid!(),
            latency_allowance_used_ms: 0,
            last_position_at_ms: 22_000,
            updated_at_ms: 22_000,
            created_at_ms: 0,
        };

        apply_playback_mutation(
            &mut playback,
            &PlaybackMutation {
                position_ms: Some(196_000),
                duration_ms: None,
                state: Some(PlaybackState::Stopped),
            },
            197_000,
        );

        assert_eq!(playback.played_ms, 22_000);
    }

    #[test]
    fn playback_mutation_keeps_played_time_after_seeking_back_to_the_start() {
        let mut playback = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            play_id: nanoid!(),
            client_name: None,
            plugin_id: None,
            position_ms: 150_000,
            duration_ms: Some(200_000),
            played_ms: 150_000,
            last_position_ms: 150_000,
            latency_allowance_used_ms: 0,
            state: PlaybackState::Playing,
            last_position_at_ms: 150_000,
            updated_at_ms: 150_000,
            created_at_ms: 0,
        };

        apply_playback_mutation(
            &mut playback,
            &PlaybackMutation {
                position_ms: Some(1_500),
                duration_ms: None,
                state: Some(PlaybackState::Playing),
            },
            151_000,
        );

        assert_eq!(playback.played_ms, 150_000);
        assert_eq!(playback.last_position_ms, 1_500);
        assert_eq!(
            playback.created_at_ms, 0,
            "a seek does not start a new play"
        );
    }

    #[test]
    fn playback_mutation_measures_elapsed_time_from_the_last_reported_position() {
        let mut playback = PlaybackSession {
            db_id: None,
            id: nanoid!(),
            play_id: nanoid!(),
            client_name: None,
            plugin_id: None,
            position_ms: 0,
            duration_ms: Some(300_000),
            played_ms: 0,
            last_position_ms: 0,
            last_position_at_ms: 0,
            latency_allowance_used_ms: 0,
            state: PlaybackState::Playing,
            updated_at_ms: 0,
            created_at_ms: 0,
        };

        for (mutation, now_ms) in [
            (
                PlaybackMutation {
                    state: Some(PlaybackState::Playing),
                    ..PlaybackMutation::default()
                },
                14_000,
            ),
            (
                PlaybackMutation {
                    position_ms: Some(30_000),
                    state: Some(PlaybackState::Playing),
                    ..PlaybackMutation::default()
                },
                30_000,
            ),
        ] {
            apply_playback_mutation(&mut playback, &mutation, now_ms);
        }

        assert_eq!(playback.played_ms, 30_000);
        assert_eq!(playback.latency_allowance_used_ms, 0);

        // Paused without a position, resumed without one after 10 minutes, then a jump far
        // ahead: only the time since the resume counts, plus the latency allowance.
        let reports = [
            (None, PlaybackState::Paused, 31_000),
            (None, PlaybackState::Playing, 631_000),
            (Some(205_000), PlaybackState::Playing, 636_000),
        ];
        for (position_ms, state, now_ms) in reports {
            apply_playback_mutation(
                &mut playback,
                &PlaybackMutation {
                    position_ms,
                    state: Some(state),
                    ..PlaybackMutation::default()
                },
                now_ms,
            );
        }

        assert_eq!(
            playback.played_ms,
            30_000 + 5_000 + PLAY_LATENCY_ALLOWANCE_MS
        );
    }
}
