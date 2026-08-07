// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashSet;

use agdb::{DbAny, DbId};
use serde::{Deserialize, Serialize};

use crate::{
    db,
    services::{
        auth::{Principal, access},
        pagination,
        playback_sessions,
    },
};

pub(crate) const QUEUE_ITEM_HARD_CAP: usize = 1_000;
pub(crate) use crate::db::playbacks::RepeatMode;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct QueueSnapshot {
    pub(crate) track_ids: Vec<String>,
    pub(crate) current_index: u64,
    pub(crate) repeat_mode: RepeatMode,
    pub(crate) shuffle_enabled: bool,
}

impl QueueSnapshot {
    #[cfg(test)]
    pub(crate) fn single(track_id: String) -> Self {
        Self {
            track_ids: vec![track_id],
            current_index: 0,
            repeat_mode: RepeatMode::None,
            shuffle_enabled: false,
        }
    }

    pub(crate) fn current_track_id(&self) -> &str {
        &self.track_ids[self.current_index as usize]
    }
}

pub(crate) fn queue_from_playback(
    playback: &db::playbacks::Playback,
) -> Result<QueueSnapshot, PlaybackError> {
    let queue = QueueSnapshot {
        track_ids: playback.track_ids.clone(),
        current_index: playback.current_index,
        repeat_mode: playback.repeat_mode,
        shuffle_enabled: playback.shuffle_enabled,
    };
    let current_index = usize::try_from(queue.current_index).ok();
    if queue.track_ids.is_empty()
        || queue.track_ids.len() > QUEUE_ITEM_HARD_CAP
        || current_index.is_none_or(|index| index >= queue.track_ids.len())
    {
        return Err(PlaybackError::Internal(anyhow::anyhow!(
            "playback {} has invalid stored queue shape",
            playback.id
        )));
    }
    Ok(queue)
}

#[derive(Clone, Debug)]
pub(crate) struct PlaybackDetail {
    pub(crate) playback_db_id: DbId,
    pub(crate) playback: db::playbacks::Playback,
    pub(crate) current_session: Option<CurrentSession>,
}

#[derive(Clone, Debug)]
pub(crate) struct CurrentSession {
    pub(crate) playback_session_id: DbId,
    pub(crate) track_db_id: DbId,
    pub(crate) track_public_id: String,
    pub(crate) playback: db::PlaybackSession,
}

impl From<&playback_sessions::PlaybackRecord> for CurrentSession {
    fn from(record: &playback_sessions::PlaybackRecord) -> Self {
        Self {
            playback_session_id: record.playback_session_id,
            track_db_id: record.track_db_id,
            track_public_id: record.track_public_id.clone(),
            playback: record.playback.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ValidatedQueue {
    pub(crate) snapshot: QueueSnapshot,
    pub(crate) current_track_db_id: DbId,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PlaybackError {
    #[error("{0}")]
    InvalidQueue(String),
    #[error("playback not found")]
    NotFound,
    #[error("queue revision conflict: expected {expected_revision}, current {current_revision}")]
    RevisionConflict {
        expected_revision: u64,
        current_revision: u64,
    },
    #[error("queue revision exhausted")]
    RevisionExhausted,
    #[error("playback limit reached")]
    LimitReached,
    #[error(transparent)]
    Session(#[from] playback_sessions::PlaybackServiceError),
    #[error(transparent)]
    Database(#[from] agdb::DbError),
    #[error(transparent)]
    Pagination(#[from] pagination::PaginationError),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

pub(crate) struct PlaybackUpdate {
    pub(crate) playback: db::playbacks::Playback,
    pub(crate) session: playback_sessions::PlaybackRecord,
    pub(crate) event: String,
    pub(crate) evicted_playbacks: Vec<playback_sessions::EvictedPlaybackRecord>,
}

pub(crate) struct CreatePlaybackRequest {
    pub(crate) id: String,
    pub(crate) user_db_id: DbId,
    pub(crate) client_name: Option<String>,
    pub(crate) queue: ValidatedQueue,
    pub(crate) mutation: playback_sessions::PlaybackMutation,
    pub(crate) now_ms: u64,
}

pub(crate) struct ReportProgressRequest {
    pub(crate) playback_db_id: DbId,
    pub(crate) user_db_id: DbId,
    pub(crate) client_name: Option<String>,
    pub(crate) queue_revision: u64,
    pub(crate) current_track_db_id: DbId,
    pub(crate) mutation: playback_sessions::PlaybackMutation,
    pub(crate) now_ms: u64,
    pub(crate) require_full_queue_access: bool,
}

pub(crate) struct CompensateProgressRequest {
    pub(crate) playback_db_id: DbId,
    pub(crate) playback_public_id: String,
    pub(crate) user_db_id: DbId,
    pub(crate) queue_revision: u64,
    pub(crate) expected_session: db::PlaybackSession,
    pub(crate) now_ms: u64,
}

fn cleanup_after_commit(
    db: &mut DbAny,
    now_ms: u64,
) -> Vec<playback_sessions::EvictedPlaybackRecord> {
    match playback_sessions::cleanup_evicted_playbacks(db, now_ms) {
        Ok(evicted) => evicted,
        Err(error) => {
            tracing::error!(%error, "playback cleanup failed after durable mutation committed");
            Vec::new()
        }
    }
}

pub(crate) fn create_playback(
    db: &mut DbAny,
    request: CreatePlaybackRequest,
) -> Result<PlaybackUpdate, PlaybackError> {
    let CreatePlaybackRequest {
        id,
        user_db_id,
        client_name,
        queue,
        mutation,
        now_ms,
    } = request;
    let ValidatedQueue {
        snapshot,
        current_track_db_id,
    } = queue;
    let playback = db::playbacks::Playback {
        db_id: None,
        id,
        queue_revision: 1,
        track_ids: snapshot.track_ids,
        current_index: snapshot.current_index,
        repeat_mode: snapshot.repeat_mode,
        shuffle_enabled: snapshot.shuffle_enabled,
        created_at_ms: now_ms,
        updated_at_ms: now_ms,
    };

    let (playback, session) = db.transaction_mut(|t| {
        if db::playbacks::count_for_user_up_to_limit(t, user_db_id)?
            >= db::playbacks::MAX_PLAYBACKS_PER_USER
        {
            return Err(PlaybackError::LimitReached);
        }
        let session = playback_sessions::start_playback_in_transaction(
            t,
            playback_sessions::StartPlaybackRequest {
                track_db_id: current_track_db_id,
                user_db_id,
                client_name,
                mutation,
                now_ms,
                active_event: playback_sessions::ActiveEvent::Started,
            },
        )?;
        let playback_db_id = db::playbacks::insert(
            t,
            &playback,
            user_db_id,
            session.playback.playback_session_id,
        )?;
        let mut playback = playback.clone();
        playback.db_id = Some(playback_db_id);
        Ok::<_, PlaybackError>((playback, session))
    })?;
    let evicted_playbacks = cleanup_after_commit(db, now_ms);

    Ok(PlaybackUpdate {
        playback,
        event: session.event,
        session: session.playback,
        evicted_playbacks,
    })
}

pub(crate) fn report_progress(
    db: &mut DbAny,
    request: ReportProgressRequest,
) -> Result<PlaybackUpdate, PlaybackError> {
    let ReportProgressRequest {
        playback_db_id,
        user_db_id,
        client_name,
        queue_revision,
        current_track_db_id,
        mutation,
        now_ms,
        require_full_queue_access,
    } = request;

    let (playback, session) = db.transaction_mut(|t| {
        if db::playbacks::get_owner_id(t, playback_db_id)? != Some(user_db_id) {
            return Err(PlaybackError::NotFound);
        }
        let mut playback = db::playbacks::get_by_id(t, playback_db_id)?
            .ok_or(PlaybackError::NotFound)?;
        if playback.queue_revision != queue_revision {
            return Err(PlaybackError::RevisionConflict {
                expected_revision: queue_revision,
                current_revision: playback.queue_revision,
            });
        }
        let queue = queue_from_playback(&playback)?;
        if require_full_queue_access
            && !queue_tracks_accessible_to_user(t, user_db_id, &queue)?
        {
            return Err(PlaybackError::NotFound);
        }
        if db::lookup::find_node_id_by_id(t, queue.current_track_id())?
            != Some(current_track_db_id)
        {
            return Err(PlaybackError::Internal(anyhow::anyhow!(
                "playback {} current queue track changed during progress",
                playback.id
            )));
        }

        let session = if let Some(session_id) =
            db::playbacks::get_current_session_id(t, playback_db_id)?
        {
            if db::playback_sessions::get_track_id(t, session_id)?
                != Some(current_track_db_id)
                || db::playback_sessions::get_user_id(t, session_id)? != Some(user_db_id)
            {
                return Err(PlaybackError::Internal(anyhow::anyhow!(
                    "playback {} current session does not match its queue and owner",
                    playback.id
                )));
            }
            let session = playback_sessions::report_playback_in_transaction(
                t,
                playback_sessions::ReportPlaybackRequest {
                    playback_session_id: session_id,
                    user_db_id: Some(user_db_id),
                    mutation,
                    now_ms,
                    activity_policy: playback_sessions::ActivityPolicy::AnyState,
                    active_event: playback_sessions::ActiveEvent::Progress,
                },
            )?;
            db::playbacks::touch(t, playback_db_id, now_ms)?;
            session
        } else {
            if mutation.state.is_some_and(db::PlaybackState::is_terminal) {
                return Err(PlaybackError::Session(
                    playback_sessions::PlaybackServiceError::BadRequest(
                        "cannot create a current track session with terminal state".to_string(),
                    ),
                ));
            }
            let session = playback_sessions::start_playback_in_transaction(
                t,
                playback_sessions::StartPlaybackRequest {
                    track_db_id: current_track_db_id,
                    user_db_id,
                    client_name,
                    mutation: playback_sessions::PlaybackMutation {
                        state: mutation.state.or(Some(db::PlaybackState::Playing)),
                        ..mutation
                    },
                    now_ms,
                    active_event: playback_sessions::ActiveEvent::Started,
                },
            )?;
            db::playbacks::link_current_session(
                t,
                playback_db_id,
                session.playback.playback_session_id,
                now_ms,
            )?;
            session
        };

        playback.updated_at_ms = now_ms;
        Ok::<_, PlaybackError>((playback, session))
    })?;

    if session.playback.playback.state.is_terminal() {
        playback_sessions::clear_session_bindings_for_playback(
            session.playback.playback_session_id,
            &session.playback.playback_session_public_id,
        );
    }
    let evicted_playbacks = cleanup_after_commit(db, now_ms);
    Ok(PlaybackUpdate {
        playback,
        event: session.event,
        session: session.playback,
        evicted_playbacks,
    })
}

pub(crate) fn compensate_failed_handoff_progress(
    db: &mut DbAny,
    request: CompensateProgressRequest,
) -> Result<Option<PlaybackUpdate>, PlaybackError> {
    let CompensateProgressRequest {
        playback_db_id,
        playback_public_id,
        user_db_id,
        queue_revision,
        expected_session,
        now_ms,
    } = request;
    let playback_session_id = expected_session.db_id.ok_or_else(|| {
        PlaybackError::Internal(anyhow::anyhow!(
            "handoff compensation session is missing its database ID"
        ))
    })?;
    let Some(compensated) = playback_sessions::with_no_current_binding_for_playback(
        playback_session_id,
        &expected_session.id,
        || {
            db.transaction_mut(|t| {
                if db::playbacks::get_owner_id(t, playback_db_id)? != Some(user_db_id) {
                    return Ok::<_, PlaybackError>(None);
                }
                let Some(mut playback) = db::playbacks::get_by_id(t, playback_db_id)? else {
                    return Ok(None);
                };
                if playback.id != playback_public_id
                    || playback.queue_revision != queue_revision
                    || db::playbacks::get_current_session_id(t, playback_db_id)?
                        != Some(playback_session_id)
                {
                    return Ok(None);
                }
                let Some(session) =
                    db::playback_sessions::get_by_id(t, playback_session_id)?
                else {
                    return Ok(None);
                };
                if session != expected_session
                    || !matches!(
                        session.state,
                        db::PlaybackState::Playing | db::PlaybackState::Buffering
                    )
                {
                    return Ok(None);
                }
                let Some(session) = playback_sessions::pause_playback_in_transaction(
                    t,
                    playback_session_id,
                    user_db_id,
                    now_ms.max(session.updated_at_ms),
                )? else {
                    return Ok(None);
                };
                playback.updated_at_ms = session.playback.playback.updated_at_ms;
                db::playbacks::touch(t, playback_db_id, playback.updated_at_ms)?;
                Ok(Some((playback, session)))
            })
        },
    ) else {
        return Ok(None);
    };
    let compensated = compensated?;
    let Some((playback, session)) = compensated else {
        return Ok(None);
    };
    let evicted_playbacks = cleanup_after_commit(db, now_ms);
    Ok(Some(PlaybackUpdate {
        playback,
        event: session.event,
        session: session.playback,
        evicted_playbacks,
    }))
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum HandoffValidationError {
    #[error("playback not found")]
    NotFound,
    #[error("playback is owned by another user")]
    WrongOwner,
    #[error("queue revision conflict: expected {expected_revision}, current {current_revision}")]
    RevisionConflict {
        expected_revision: u64,
        current_revision: u64,
    },
    #[error("playback queue is no longer accessible")]
    Inaccessible,
    #[error(transparent)]
    Database(#[from] agdb::DbError),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

fn get_current_session(
    db: &impl db::DbAccess,
    playback_session_id: DbId,
) -> Result<Option<CurrentSession>, PlaybackError> {
    let Some(playback) = db::playback_sessions::get_by_id(db, playback_session_id)? else {
        return Ok(None);
    };
    let Some(track_db_id) = db::playback_sessions::get_track_id(db, playback_session_id)? else {
        return Ok(None);
    };
    if db::tracks::get_by_id(db, track_db_id)?.is_none() {
        return Ok(None);
    }
    let Some(track_public_id) = db::lookup::find_id_by_db_id(db, track_db_id)? else {
        return Ok(None);
    };
    Ok(Some(CurrentSession {
        playback_session_id,
        track_db_id,
        track_public_id,
        playback,
    }))
}

fn detail_from_playback(
    db: &DbAny,
    playback: db::playbacks::Playback,
) -> Result<PlaybackDetail, PlaybackError> {
    let playback_db_id = playback
        .db_id
        .ok_or_else(|| anyhow::anyhow!("persisted playback missing db_id"))?;
    let current_session = match db::playbacks::get_current_session_id(db, playback_db_id)? {
        Some(session_id) => get_current_session(db, session_id)?,
        None => None,
    };
    Ok(PlaybackDetail {
        playback_db_id,
        playback,
        current_session,
    })
}

pub(crate) fn get_owned_detail(
    db: &DbAny,
    playback_db_id: DbId,
    user_db_id: DbId,
) -> Result<Option<PlaybackDetail>, PlaybackError> {
    if db::playbacks::get_owner_id(db, playback_db_id)? != Some(user_db_id) {
        return Ok(None);
    }
    let Some(playback) = db::playbacks::get_by_id(db, playback_db_id)? else {
        return Ok(None);
    };
    detail_from_playback(db, playback).map(Some)
}

pub(crate) fn get_visible_detail(
    db: &DbAny,
    playback_db_id: DbId,
    principal: &Principal,
) -> Result<Option<PlaybackDetail>, PlaybackError> {
    let Some(detail) = get_owned_detail(db, playback_db_id, principal.user_db_id)? else {
        return Ok(None);
    };
    let is_admin = db::roles::has_admin_role(db, principal.user_db_id)?;
    let accessible_library_ids =
        db::libraries::accessible_library_ids(db, principal.user_db_id)?;
    if let Some(current) = detail.current_session.as_ref()
        && !track_accessible_in_scope(
            db,
            current.track_db_id,
            is_admin,
            &accessible_library_ids,
        )?
    {
        return Ok(None);
    }
    Ok(Some(detail))
}

pub(crate) fn get_owned_projection(
    db: &DbAny,
    playback_db_id: DbId,
    user_db_id: DbId,
) -> Result<Option<db::playbacks::PlaybackListProjection>, PlaybackError> {
    if db::playbacks::get_owner_id(db, playback_db_id)? != Some(user_db_id) {
        return Ok(None);
    }
    Ok(db::playbacks::get_projection_by_id(db, playback_db_id)?)
}

pub(crate) fn list_visible_projections(
    db: &DbAny,
    principal: &Principal,
    active_only: bool,
    now_ms: u64,
) -> Result<Vec<db::playbacks::PlaybackListProjection>, PlaybackError> {
    let projections = db::playbacks::list_projections_for_user(db, principal.user_db_id)?;
    if projections.len() > db::playbacks::MAX_PLAYBACKS_PER_USER
        || projections.len() > pagination::snapshot_item_capacity()
    {
        return Err(pagination::PaginationError::SnapshotTooLarge.into());
    }
    let projection_ids = projections.iter().map(|projection| projection.db_id).collect::<Vec<_>>();
    let current_ids = db::playbacks::current_session_ids(db, &projection_ids)?;
    let session_ids = current_ids.values().copied().collect::<Vec<_>>();
    let sessions = db::playback_sessions::get_list_projections_by_ids(db, session_ids.clone())?;
    let track_ids = db::playback_sessions::track_ids_for_sessions(db, &session_ids)?;
    let is_admin = db::roles::has_admin_role(db, principal.user_db_id)?;
    let accessible_track_ids = if is_admin {
        HashSet::new()
    } else {
        let current_track_ids = track_ids.values().copied().collect::<Vec<_>>();
        db::libraries::accessible_track_ids(
            db,
            principal.user_db_id,
            &current_track_ids,
        )?
    };
    let active_cutoff = now_ms.saturating_sub(playback_sessions::ACTIVE_SESSION_TTL_MS);
    let mut visible = Vec::with_capacity(projections.len());
    for projection in projections {
        let current = current_ids
            .get(&projection.db_id)
            .and_then(|session_id| sessions.get(session_id).map(|session| (*session_id, session)));
        let accessible = match current {
            Some((session_id, _)) => match track_ids.get(&session_id) {
                Some(track_db_id) => is_admin || accessible_track_ids.contains(track_db_id),
                None => true,
            },
            None => true,
        };
        if !accessible {
            continue;
        }
        if active_only
            && !current.is_some_and(|(session_id, current)| {
                track_ids.contains_key(&session_id)
                    && !current.state.is_terminal()
                    && current.updated_at_ms >= active_cutoff
            })
        {
            continue;
        }
        visible.push(projection);
    }
    Ok(visible)
}

pub(crate) fn validate_queue(
    db: &impl db::DbAccess,
    principal: &Principal,
    snapshot: QueueSnapshot,
) -> Result<ValidatedQueue, PlaybackError> {
    if snapshot.track_ids.is_empty() {
        return Err(PlaybackError::InvalidQueue("track_ids cannot be empty".to_string()));
    }
    if snapshot.track_ids.len() > QUEUE_ITEM_HARD_CAP {
        return Err(PlaybackError::InvalidQueue(format!(
            "track_ids cap exceeded: {} > {QUEUE_ITEM_HARD_CAP}",
            snapshot.track_ids.len()
        )));
    }
    let current_index = usize::try_from(snapshot.current_index).map_err(|_| {
        PlaybackError::InvalidQueue(format!(
            "current_index {} is outside track_ids length {}",
            snapshot.current_index,
            snapshot.track_ids.len()
        ))
    })?;
    if current_index >= snapshot.track_ids.len() {
        return Err(PlaybackError::InvalidQueue(format!(
            "current_index {} is outside track_ids length {}",
            snapshot.current_index,
            snapshot.track_ids.len()
        )));
    }

    let mut current_track_db_id = None;
    for (index, public_id) in snapshot.track_ids.iter().enumerate() {
        if public_id.trim().is_empty() {
            return Err(PlaybackError::InvalidQueue(format!(
                "track_ids[{index}] cannot be blank"
            )));
        }
        let track_db_id = db::lookup::find_node_id_by_id(db, public_id)?
            .ok_or_else(|| PlaybackError::InvalidQueue(format!("track not found: {public_id}")))?;
        if db::tracks::get_by_id(db, track_db_id)?.is_none() {
            return Err(PlaybackError::InvalidQueue(format!("track not found: {public_id}")));
        }
        access::require_entity_accessible(db, principal, track_db_id, || {
            PlaybackError::InvalidQueue(format!("track not found: {public_id}"))
        })?;
        if index == current_index {
            current_track_db_id = Some(track_db_id);
        }
    }

    Ok(ValidatedQueue {
        snapshot,
        current_track_db_id: current_track_db_id
            .ok_or_else(|| anyhow::anyhow!("validated queue missing current track"))?,
    })
}

pub(crate) fn queue_tracks_accessible_to_user(
    db: &impl db::DbAccess,
    user_db_id: DbId,
    snapshot: &QueueSnapshot,
) -> Result<bool, PlaybackError> {
    let is_admin = db::roles::has_admin_role(db, user_db_id)?;
    let accessible_library_ids = db::libraries::accessible_library_ids(db, user_db_id)?;
    for public_id in &snapshot.track_ids {
        let Some(track_db_id) = db::lookup::find_node_id_by_id(db, public_id)? else {
            return Ok(false);
        };
        if db::tracks::get_by_id(db, track_db_id)?.is_none() {
            return Ok(false);
        }
        if !track_accessible_in_scope(
            db,
            track_db_id,
            is_admin,
            &accessible_library_ids,
        )? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn track_accessible_in_scope(
    db: &impl db::DbAccess,
    track_db_id: DbId,
    is_admin: bool,
    accessible_library_ids: &HashSet<String>,
) -> Result<bool, PlaybackError> {
    if is_admin {
        return Ok(true);
    }
    Ok(db::libraries::get_for_entity(db, track_db_id)?
        .iter()
        .any(|library| accessible_library_ids.contains(&library.id)))
}

pub(crate) fn queue_visible_to_principal(
    db: &DbAny,
    principal: &Principal,
    playback: &db::playbacks::Playback,
) -> Result<Option<QueueSnapshot>, PlaybackError> {
    let queue = queue_from_playback(playback)?;
    if queue_tracks_accessible_to_user(db, principal.user_db_id, &queue)? {
        Ok(Some(queue))
    } else {
        Ok(None)
    }
}

pub(crate) fn validate_handoff_queue(
    db: &DbAny,
    user_db_id: DbId,
    playback_id: &str,
    expected_revision: u64,
) -> Result<(), HandoffValidationError> {
    let playback_db_id = db::lookup::find_node_id_by_id(db, playback_id)?
        .ok_or(HandoffValidationError::NotFound)?;
    let Some(playback) = db::playbacks::get_by_id(db, playback_db_id)? else {
        return Err(HandoffValidationError::NotFound);
    };
    if db::playbacks::get_owner_id(db, playback_db_id)? != Some(user_db_id) {
        return Err(HandoffValidationError::WrongOwner);
    }
    if playback.queue_revision != expected_revision {
        return Err(HandoffValidationError::RevisionConflict {
            expected_revision,
            current_revision: playback.queue_revision,
        });
    }
    let queue = queue_from_playback(&playback)
        .map_err(|error| HandoffValidationError::Internal(anyhow::anyhow!(error.to_string())))?;
    if !queue_tracks_accessible_to_user(db, user_db_id, &queue)
        .map_err(|error| HandoffValidationError::Internal(anyhow::anyhow!(error.to_string())))?
    {
        return Err(HandoffValidationError::Inaccessible);
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub(crate) struct DetachedSession {
    pub(crate) playback_session_id: DbId,
    pub(crate) playback_session_public_id: String,
    pub(crate) update: Option<playback_sessions::TransactionPlaybackUpdate>,
}

pub(crate) struct ReplaceQueueUpdate {
    pub(crate) playback: db::playbacks::Playback,
    pub(crate) queue: QueueSnapshot,
    pub(crate) detached_session: Option<DetachedSession>,
}

pub(crate) fn replace_queue(
    db: &mut DbAny,
    playback_db_id: DbId,
    user_db_id: DbId,
    expected_revision: u64,
    queue: ValidatedQueue,
    now_ms: u64,
) -> Result<ReplaceQueueUpdate, PlaybackError> {
    let ValidatedQueue {
        snapshot,
        current_track_db_id,
    } = queue;
    let (playback, detached_session) = db.transaction_mut(|t| {
        if db::playbacks::get_owner_id(t, playback_db_id)? != Some(user_db_id) {
            return Err(PlaybackError::NotFound);
        }
        let raw_session_id = db::playbacks::get_current_session_id(t, playback_db_id)?;
        let clear_current_session = match raw_session_id {
            Some(session_id) => {
                db::playback_sessions::get_track_id(t, session_id)?
                    != Some(current_track_db_id)
                    || db::playback_sessions::get_user_id(t, session_id)? != Some(user_db_id)
            }
            None => false,
        };
        let detached_session = if clear_current_session {
            match raw_session_id {
                Some(session_id) => match db::playback_sessions::get_by_id(t, session_id)? {
                    Some(session) => {
                        let update = if db::playback_sessions::get_track_id(t, session_id)?
                            .is_some()
                            && db::playback_sessions::get_user_id(t, session_id)?
                                == Some(user_db_id)
                        {
                            playback_sessions::pause_playback_in_transaction(
                                t,
                                session_id,
                                user_db_id,
                                now_ms,
                            )?
                        } else {
                            None
                        };
                        Some(DetachedSession {
                            playback_session_id: session_id,
                            playback_session_public_id: session.id,
                            update,
                        })
                    }
                    None => None,
                },
                None => None,
            }
        } else {
            None
        };
        let playback = db::playbacks::replace_queue_in_transaction(
            t,
            playback_db_id,
            expected_revision,
            snapshot.track_ids.clone(),
            snapshot.current_index,
            snapshot.repeat_mode,
            snapshot.shuffle_enabled,
            now_ms,
            clear_current_session,
        )
        .map_err(|error| match error {
            db::playbacks::ReplaceQueueError::NotFound => PlaybackError::NotFound,
            db::playbacks::ReplaceQueueError::RevisionConflict {
                expected_revision,
                current_revision,
            } => PlaybackError::RevisionConflict {
                expected_revision,
                current_revision,
            },
            db::playbacks::ReplaceQueueError::RevisionExhausted => PlaybackError::RevisionExhausted,
            db::playbacks::ReplaceQueueError::Database(error) => PlaybackError::Internal(error.into()),
            db::playbacks::ReplaceQueueError::Internal(error) => PlaybackError::Internal(error),
        })?;
        Ok::<_, PlaybackError>((playback, detached_session))
    })?;
    Ok(ReplaceQueueUpdate {
        playback,
        queue: snapshot,
        detached_session,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use agdb::QueryBuilder;
    use crate::db::test_db::{insert_track, new_test_db, test_user};

    fn setup() -> anyhow::Result<(DbAny, DbId, DbId, DbId, String, String)> {
        let mut db = new_test_db()?;
        let user_db_id = db::users::create(&mut db, &test_user("durable-playback")?)?;
        let first_track_db_id = insert_track(&mut db, "First")?;
        let second_track_db_id = insert_track(&mut db, "Second")?;
        let first_track_id = db::lookup::find_id_by_db_id(&db, first_track_db_id)?
            .expect("first track must have a public ID");
        let second_track_id = db::lookup::find_id_by_db_id(&db, second_track_db_id)?
            .expect("second track must have a public ID");
        Ok((
            db,
            user_db_id,
            first_track_db_id,
            second_track_db_id,
            first_track_id,
            second_track_id,
        ))
    }

    #[test]
    fn queue_snapshot_rejects_unknown_fields() {
        let result = serde_json::from_str::<QueueSnapshot>(
            r#"{"track_ids":["a"],"current_index":0,"repeat_mode":"none","shuffle_enabled":false,"position_ms":1}"#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn single_queue_has_canonical_defaults() {
        let queue = QueueSnapshot::single("track".to_string());
        assert_eq!(queue.current_track_id(), "track");
        assert_eq!(queue.repeat_mode, RepeatMode::None);
        assert!(!queue.shuffle_enabled);
    }

    #[test]
    fn initial_reported_position_is_not_counted_as_observed_activity() -> anyhow::Result<()> {
        let (mut db, user_db_id, first_track_db_id, _, first_track_id, _) = setup()?;
        let update = create_playback(
            &mut db,
            CreatePlaybackRequest {
                id: nanoid::nanoid!(),
                user_db_id,
                client_name: None,
                queue: ValidatedQueue {
                    snapshot: QueueSnapshot::single(first_track_id),
                    current_track_db_id: first_track_db_id,
                },
                mutation: playback_sessions::PlaybackMutation {
                    position_ms: Some(30_000),
                    duration_ms: Some(100_000),
                    state: Some(db::PlaybackState::Playing),
                },
                now_ms: 30_000,
            },
        )?;
        assert_eq!(update.session.playback.activity_ms, Some(0));
        assert_ne!(update.session.playback.listen_recorded, Some(true));
        Ok(())
    }

    #[test]
    fn replacing_queue_heals_unresolvable_current_session_edge() -> anyhow::Result<()> {
        let (
            mut db,
            user_db_id,
            first_track_db_id,
            second_track_db_id,
            first_track_id,
            second_track_id,
        ) = setup()?;
        let created = create_playback(
            &mut db,
            CreatePlaybackRequest {
                id: nanoid::nanoid!(),
                user_db_id,
                client_name: None,
                queue: ValidatedQueue {
                    snapshot: QueueSnapshot::single(first_track_id),
                    current_track_db_id: first_track_db_id,
                },
                mutation: playback_sessions::PlaybackMutation::default(),
                now_ms: 1,
            },
        )?;
        let playback_db_id = created.playback.db_id.expect("persisted playback");
        let session_id = created.session.playback_session_id;
        db.exec_mut(QueryBuilder::remove().ids(first_track_db_id).query())?;

        let replaced = replace_queue(
            &mut db,
            playback_db_id,
            user_db_id,
            1,
            ValidatedQueue {
                snapshot: QueueSnapshot::single(second_track_id),
                current_track_db_id: second_track_db_id,
            },
            2,
        )?;

        assert_eq!(
            replaced.detached_session.as_ref().map(|session| session.playback_session_id),
            Some(session_id)
        );
        assert_eq!(db::playbacks::get_current_session_id(&db, playback_db_id)?, None);
        assert!(db::playback_sessions::get_by_id(&db, session_id)?.is_some());
        Ok(())
    }

    #[test]
    fn replacing_queue_pauses_displaced_session_at_effective_position() -> anyhow::Result<()> {
        let (
            mut db,
            user_db_id,
            first_track_db_id,
            second_track_db_id,
            first_track_id,
            second_track_id,
        ) = setup()?;
        let created = create_playback(
            &mut db,
            CreatePlaybackRequest {
                id: nanoid::nanoid!(),
                user_db_id,
                client_name: None,
                queue: ValidatedQueue {
                    snapshot: QueueSnapshot::single(first_track_id),
                    current_track_db_id: first_track_db_id,
                },
                mutation: playback_sessions::PlaybackMutation {
                    position_ms: Some(1_000),
                    duration_ms: Some(100_000),
                    state: Some(db::PlaybackState::Playing),
                },
                now_ms: 1_000,
            },
        )?;
        let replaced = replace_queue(
            &mut db,
            created.playback.db_id.expect("persisted playback"),
            user_db_id,
            1,
            ValidatedQueue {
                snapshot: QueueSnapshot::single(second_track_id),
                current_track_db_id: second_track_db_id,
            },
            6_000,
        )?;
        let paused = &replaced
            .detached_session
            .expect("session must be detached")
            .update
            .expect("playing session must be paused")
            .playback
            .playback;
        assert_eq!(paused.state, db::PlaybackState::Paused);
        assert_eq!(paused.position_ms, 6_000);
        assert_eq!(paused.activity_ms, Some(5_000));
        Ok(())
    }
}
