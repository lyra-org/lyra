// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashMap,
    sync::Arc,
};

use agdb::DbId;
use tokio::sync::{
    Notify,
    oneshot,
};

use super::{
    messages::{
        ForwardedCommand,
        OutgoingMessage,
    },
    registry::{
        self,
        ConnectionId,
        ConnectionSnapshot,
    },
};
use crate::{
    STATE,
    services::{
        playback_sessions,
        playbacks::{
            self,
            validate_handoff_queue,
        },
    },
};

const HANDOFF_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);
const MAX_PENDING_HANDOFFS: usize = 256;
const MAX_PENDING_HANDOFFS_PER_TARGET: usize = 4;

enum HandoffPhase {
    AwaitingProgress,
    Applying { failure: Option<String> },
}

struct PendingHandoff {
    source_id: Option<ConnectionId>,
    source_binding: Option<ExpectedSourceBinding>,
    target_id: ConnectionId,
    user_db_id: DbId,
    playback_id: String,
    queue_revision: u64,
    phase: HandoffPhase,
    timeout_cancel: Arc<Notify>,
    completion_tx: oneshot::Sender<Result<(), String>>,
}

#[derive(Clone, Debug)]
pub(super) struct ExpectedSourceBinding {
    pub(super) user_db_id: DbId,
    pub(super) session_key: String,
    pub(super) snapshot: playback_sessions::CurrentBindingSnapshot,
}

pub(super) struct ProgressReady {
    pub(super) source_binding: Option<ExpectedSourceBinding>,
    pub(super) target_id: ConnectionId,
}

pub(super) struct PendingHandoffs {
    entries: HashMap<String, PendingHandoff>,
}

impl PendingHandoffs {
    pub(super) fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub(super) fn begin(
        &mut self,
        source_id: Option<ConnectionId>,
        source_binding: Option<ExpectedSourceBinding>,
        target_id: ConnectionId,
        user_db_id: DbId,
        playback_id: String,
        queue_revision: u64,
    ) -> Result<(String, oneshot::Receiver<Result<(), String>>, Arc<Notify>), String> {
        if self.entries.len() >= MAX_PENDING_HANDOFFS {
            return Err("too many pending handoffs".to_string());
        }
        if self
            .entries
            .values()
            .filter(|pending| pending.target_id == target_id)
            .count()
            >= MAX_PENDING_HANDOFFS_PER_TARGET
        {
            return Err("too many pending handoffs for target".to_string());
        }
        let token = loop {
            let token = nanoid::nanoid!();
            if !self.entries.contains_key(&token) {
                break token;
            }
        };
        let (completion_tx, completion_rx) = oneshot::channel();
        let timeout_cancel = Arc::new(Notify::new());
        self.entries.insert(
            token.clone(),
            PendingHandoff {
                source_id,
                source_binding,
                target_id,
                user_db_id,
                playback_id,
                queue_revision,
                phase: HandoffPhase::AwaitingProgress,
                timeout_cancel: timeout_cancel.clone(),
                completion_tx,
            },
        );
        Ok((token, completion_rx, timeout_cancel))
    }

    pub(super) fn pending_target(&self, token: &str) -> Result<ConnectionId, String> {
        let pending = self
            .entries
            .get(token)
            .ok_or_else(|| "handoff is not pending".to_string())?;
        if !matches!(pending.phase, HandoffPhase::AwaitingProgress) {
            return Err("handoff is no longer awaiting progress".to_string());
        }
        Ok(pending.target_id)
    }

    pub(super) fn validate_progress_reference(
        &self,
        token: &str,
        user_db_id: DbId,
        playback_id: &str,
        queue_revision: u64,
    ) -> Result<ConnectionId, String> {
        let pending = self
            .entries
            .get(token)
            .ok_or_else(|| "handoff is not pending".to_string())?;
        if pending.user_db_id != user_db_id
            || pending.playback_id != playback_id
            || pending.queue_revision != queue_revision
        {
            return Err("handoff progress does not match the pending reference".to_string());
        }
        if !matches!(pending.phase, HandoffPhase::AwaitingProgress) {
            return Err("handoff is not awaiting progress".to_string());
        }
        Ok(pending.target_id)
    }

    pub(super) fn claim_progress(&mut self, token: &str) -> Result<(), String> {
        let pending = self
            .entries
            .get_mut(token)
            .ok_or_else(|| "handoff is not pending".to_string())?;
        if !matches!(pending.phase, HandoffPhase::AwaitingProgress) {
            return Err("handoff is not awaiting progress".to_string());
        }
        pending.phase = HandoffPhase::Applying { failure: None };
        Ok(())
    }

    pub(super) fn progress_result(&self, token: &str) -> Option<Result<ProgressReady, String>> {
        let pending = self.entries.get(token)?;
        let HandoffPhase::Applying { failure } = &pending.phase else {
            return None;
        };
        Some(match failure {
            Some(error) => Err(error.clone()),
            None => Ok(ProgressReady {
                source_binding: pending.source_binding.clone(),
                target_id: pending.target_id,
            }),
        })
    }

    pub(super) fn finish_progress(&mut self, token: &str) -> bool {
        let result = {
            let Some(pending) = self.entries.get_mut(token) else {
                return false;
            };
            let HandoffPhase::Applying { failure } = &mut pending.phase else {
                return false;
            };
            failure.take().map_or(Ok(()), Err)
        };
        self.remove_with_result(token, result);
        true
    }

    pub(super) fn abort_progress(&mut self, token: &str, error: String) -> bool {
        let error = {
            let Some(pending) = self.entries.get_mut(token) else {
                return false;
            };
            let HandoffPhase::Applying { failure } = &mut pending.phase else {
                return false;
            };
            failure.take().unwrap_or(error)
        };
        self.remove_with_result(token, Err(error));
        true
    }

    pub(super) fn fail(&mut self, token: &str, error: impl Into<String>) -> bool {
        let error = error.into();
        let remove = {
            let Some(pending) = self.entries.get_mut(token) else {
                return false;
            };
            match &mut pending.phase {
                HandoffPhase::AwaitingProgress => true,
                HandoffPhase::Applying { failure } => {
                    if failure.is_none() {
                        *failure = Some(error.clone());
                    }
                    pending.timeout_cancel.notify_one();
                    false
                }
            }
        };
        if remove {
            self.remove_with_result(token, Err(error));
        }
        true
    }

    pub(super) fn fail_for_connection(&mut self, connection_id: ConnectionId) -> usize {
        let tokens = self
            .entries
            .iter()
            .filter_map(|(token, pending)| {
                (pending.source_id == Some(connection_id) || pending.target_id == connection_id)
                    .then(|| token.clone())
            })
            .collect::<Vec<_>>();
        for token in &tokens {
            self.fail(token, "handoff connection disconnected");
        }
        tokens.len()
    }

    pub(super) fn fail_for_playback_revision(
        &mut self,
        playback_id: &str,
        current_revision: u64,
    ) -> usize {
        let tokens = self
            .entries
            .iter()
            .filter_map(|(token, pending)| {
                (pending.playback_id == playback_id && pending.queue_revision != current_revision)
                    .then(|| token.clone())
            })
            .collect::<Vec<_>>();
        for token in &tokens {
            self.fail(
                token,
                format!("queue revision changed to {current_revision} during handoff"),
            );
        }
        tokens.len()
    }

    pub(super) fn fail_for_playback(&mut self, playback_id: &str) -> usize {
        let tokens = self
            .entries
            .iter()
            .filter_map(|(token, pending)| {
                (pending.playback_id == playback_id).then(|| token.clone())
            })
            .collect::<Vec<_>>();
        for token in &tokens {
            self.fail(token, "playback deleted during handoff");
        }
        tokens.len()
    }

    fn remove_with_result(&mut self, token: &str, result: Result<(), String>) {
        if let Some(pending) = self.entries.remove(token) {
            pending.timeout_cancel.notify_one();
            let _ = pending.completion_tx.send(result);
        }
    }
}

pub(crate) async fn begin(
    source_id: Option<ConnectionId>,
    target_id: ConnectionId,
    user_db_id: DbId,
    playback_id: String,
    queue_revision: u64,
) -> Result<(String, oneshot::Receiver<Result<(), String>>), String> {
    let (token, completion_rx, timeout_cancel) = registry::insert_handoff(
        source_id,
        target_id,
        user_db_id,
        playback_id,
        queue_revision,
    )
    .await?;
    let expiry_token = token.clone();
    tokio::spawn(async move {
        tokio::select! {
            () = tokio::time::sleep(HANDOFF_TIMEOUT) => {
                registry::fail_handoff(&expiry_token, "handoff timed out").await;
            }
            () = timeout_cancel.notified() => {}
        }
    });
    Ok((token, completion_rx))
}

pub(crate) struct ProgressClaim {
    token: Option<String>,
}

impl ProgressClaim {
    pub(crate) fn commit(mut self, applied: AppliedProgress) -> CommittedProgress {
        CommittedProgress {
            token: self.token.take(),
            applied: Some(applied),
        }
    }

    pub(crate) async fn abort(mut self, error: impl Into<String>) -> bool {
        let aborted = registry::abort_handoff_progress(
            self.token
                .as_deref()
                .expect("active progress claim must retain its token"),
            error.into(),
        )
        .await;
        self.token.take();
        aborted
    }
}

pub(crate) struct CommittedProgress {
    token: Option<String>,
    applied: Option<AppliedProgress>,
}

#[derive(Clone, Debug)]
pub(crate) struct AppliedProgress {
    pub(crate) user_db_id: DbId,
    pub(crate) playback_db_id: DbId,
    pub(crate) playback_public_id: String,
    pub(crate) queue_revision: u64,
    pub(crate) expected_session: crate::db::PlaybackSession,
}

impl CommittedProgress {
    pub(crate) async fn finish(mut self) -> bool {
        let token = self
            .token
            .take()
            .expect("committed progress must retain its token");
        let applied = self
            .applied
            .take()
            .expect("committed progress must retain its applied session");
        tokio::spawn(async move { finish_committed_progress(&token, &applied).await })
            .await
            .unwrap_or(false)
    }
}

async fn finish_committed_progress(token: &str, applied: &AppliedProgress) -> bool {
    match registry::finish_handoff_progress(token, applied).await {
        registry::FinishProgress::Completed => true,
        registry::FinishProgress::Failed | registry::FinishProgress::Missing => {
            let now_ms = playback_sessions::now_ms()
                .unwrap_or(applied.expected_session.updated_at_ms)
                .max(applied.expected_session.updated_at_ms);
            let update = {
                let mut db = STATE.db.write().await;
                playbacks::compensate_failed_handoff_progress(
                    &mut db,
                    playbacks::CompensateProgressRequest {
                        playback_db_id: applied.playback_db_id,
                        playback_public_id: applied.playback_public_id.clone(),
                        queue_revision: applied.queue_revision,
                        user_db_id: applied.user_db_id,
                        expected_session: applied.expected_session.clone(),
                        now_ms,
                    },
                )
            };
            match update {
                Ok(Some(update)) => {
                    playback_sessions::dispatch_playback_update(&update.session, update.event);
                    playback_sessions::dispatch_evicted_updates(update.evicted_playbacks);
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::error!(%error, "failed to pause playback after handoff target disappeared");
                }
            }
            false
        }
    }
}

impl Drop for ProgressClaim {
    fn drop(&mut self) {
        let Some(token) = self.token.take() else {
            return;
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                registry::abort_handoff_progress(
                    &token,
                    "handoff progress request cancelled".to_string(),
                )
                .await;
            });
        }
    }
}

impl Drop for CommittedProgress {
    fn drop(&mut self) {
        let Some(token) = self.token.take() else {
            return;
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let applied = self
                .applied
                .take()
                .expect("committed progress must retain its applied session");
            runtime.spawn(async move {
                finish_committed_progress(&token, &applied).await;
            });
        }
    }
}

pub(crate) async fn claim_progress(
    token: &str,
    user_db_id: DbId,
    session_key: &str,
    playback_id: &str,
    queue_revision: u64,
) -> Result<ProgressClaim, String> {
    registry::claim_handoff_progress(token, user_db_id, session_key, playback_id, queue_revision)
        .await?;
    Ok(ProgressClaim {
        token: Some(token.to_string()),
    })
}

pub(crate) async fn fail_for_playback_revision(playback_id: &str, current_revision: u64) -> usize {
    registry::fail_handoffs_for_playback_revision(playback_id, current_revision).await
}

pub(crate) async fn fail_for_playback(playback_id: &str) -> usize {
    registry::fail_handoffs_for_playback(playback_id).await
}

pub(crate) async fn dispatch_and_wait(
    source_id: Option<ConnectionId>,
    target: &ConnectionSnapshot,
    playback_id: &str,
    queue_revision: u64,
) -> Result<(), String> {
    let (handoff_token, completion_rx) = begin(
        source_id,
        target.connection_id,
        target.user_db_id,
        playback_id.to_string(),
        queue_revision,
    )
    .await?;
    let reference_is_current = {
        let db = STATE.db.read().await;
        validate_handoff_queue(&db, target.user_db_id, playback_id, queue_revision)
    };
    if let Err(error) = reference_is_current {
        let message = error.to_string();
        registry::fail_handoff(&handoff_token, message.clone()).await;
        return Err(message);
    }
    let command = OutgoingMessage::Command(ForwardedCommand::HandoffQueue {
        from: source_id,
        playback_id: playback_id.to_string(),
        queue_revision,
        handoff_token: handoff_token.clone(),
    });
    if let Err(error) =
        registry::queue_handoff_command(&handoff_token, target.connection_id, command).await
    {
        registry::fail_handoff(&handoff_token, error.clone()).await;
        return Err(error);
    }
    match completion_rx.await {
        Ok(result) => result,
        Err(_) => Err("handoff completion channel closed".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending_handoff(
        store: &mut PendingHandoffs,
    ) -> (String, oneshot::Receiver<Result<(), String>>) {
        let (token, completion_rx, _) = store
            .begin(None, None, 2, DbId(9), "playback".to_string(), 4)
            .unwrap();
        (token, completion_rx)
    }

    fn test_applied_progress() -> AppliedProgress {
        AppliedProgress {
            user_db_id: DbId(9),
            playback_db_id: DbId(20),
            playback_public_id: "playback".to_string(),
            queue_revision: 4,
            expected_session: crate::db::PlaybackSession {
                db_id: Some(DbId(10)),
                id: "playback-session".to_string(),
                client_name: None,
                position_ms: 0,
                duration_ms: None,
                activity_ms: Some(0),
                last_position_ms: Some(0),
                state: crate::db::PlaybackState::Playing,
                listen_recorded: None,
                updated_at_ms: 2,
                created_at_ms: 1,
            },
        }
    }

    #[test]
    fn exact_progress_completes_handoff() {
        let mut store = PendingHandoffs::new();
        let (token, mut completion_rx) = pending_handoff(&mut store);
        assert_eq!(
            store
                .validate_progress_reference(&token, DbId(9), "playback", 4)
                .unwrap(),
            2
        );
        store.claim_progress(&token).unwrap();
        assert!(store.finish_progress(&token));
        assert!(matches!(completion_rx.try_recv(), Ok(Ok(()))));
        assert!(store.pending_target(&token).is_err());
    }

    #[test]
    fn failure_during_progress_is_deferred_until_progress_finishes() {
        let mut store = PendingHandoffs::new();
        let (token, mut completion_rx) = pending_handoff(&mut store);
        store.claim_progress(&token).unwrap();
        assert!(store.fail(&token, "handoff timed out"));
        assert!(matches!(
            completion_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(store.finish_progress(&token));
        assert_eq!(
            completion_rx.try_recv().unwrap().unwrap_err(),
            "handoff timed out"
        );
    }

    #[test]
    fn disconnect_and_revision_change_resolve_waiters() {
        let mut store = PendingHandoffs::new();
        let (disconnect_token, mut disconnect_rx) = pending_handoff(&mut store);
        assert_eq!(store.fail_for_connection(2), 1);
        assert!(disconnect_rx.try_recv().unwrap().is_err());
        assert!(store.pending_target(&disconnect_token).is_err());

        let (revision_token, mut revision_rx) = pending_handoff(&mut store);
        assert_eq!(store.fail_for_playback_revision("playback", 5), 1);
        assert!(revision_rx.try_recv().unwrap().is_err());
        assert!(store.pending_target(&revision_token).is_err());
    }

    #[tokio::test]
    async fn dropped_progress_claim_aborts_handoff() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        for connection in registry::list_connections().await {
            registry::unregister(connection.connection_id).await;
        }
        let registered = registry::register(
            DbId(9),
            "user".to_string(),
            None,
            "target-session".to_string(),
            Arc::new(Notify::new()),
        )
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let (token, completion_rx) = begin(
            None,
            registered.connection_id,
            DbId(9),
            "playback".to_string(),
            4,
        )
        .await
        .map_err(anyhow::Error::msg)?;

        let claim = claim_progress(&token, DbId(9), "target-session", "playback", 4)
            .await
            .map_err(anyhow::Error::msg)?;
        drop(claim);

        let result = tokio::time::timeout(std::time::Duration::from_millis(100), completion_rx)
            .await
            .expect("dropped progress claim should resolve promptly")
            .expect("completion sender should remain live");
        assert_eq!(result.unwrap_err(), "handoff progress request cancelled");
        registry::unregister(registered.connection_id).await;
        drop(registered.command_rx);
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_committed_progress_still_completes() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        crate::testing::init_default_test_state()?;
        for connection in registry::list_connections().await {
            registry::unregister(connection.connection_id).await;
        }
        let registered = registry::register(
            DbId(9),
            "user".to_string(),
            None,
            "target-session".to_string(),
            Arc::new(Notify::new()),
        )
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let (token, completion_rx) = begin(
            None,
            registered.connection_id,
            DbId(9),
            "playback".to_string(),
            4,
        )
        .await
        .map_err(anyhow::Error::msg)?;
        let committed = claim_progress(&token, DbId(9), "target-session", "playback", 4)
            .await
            .map_err(anyhow::Error::msg)?
            .commit(test_applied_progress());

        let registry_guard = registry::hold_write_lock_for_test().await;
        let finish_task = tokio::spawn(async move { committed.finish().await });
        tokio::task::yield_now().await;
        finish_task.abort();
        assert!(finish_task.await.unwrap_err().is_cancelled());
        drop(registry_guard);

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), completion_rx)
                .await
                .expect("committed progress should finish after cancellation")
                .expect("completion sender should remain live")
                .is_ok()
        );
        playback_sessions::clear_playback_session_scope(&playback_sessions::PlaybackScopeKey {
            plugin_id: "native",
            user_db_id: DbId(9),
            session_key: "target-session",
        });
        registry::unregister(registered.connection_id).await;
        drop(registered.command_rx);
        Ok(())
    }

    #[tokio::test]
    async fn successful_progress_moves_native_scope_from_source_to_target() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        crate::testing::init_default_test_state()?;
        for connection in registry::list_connections().await {
            registry::unregister(connection.connection_id).await;
        }
        let source = registry::register(
            DbId(9),
            "user".to_string(),
            None,
            "source-session".to_string(),
            Arc::new(Notify::new()),
        )
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let target = registry::register(
            DbId(9),
            "user".to_string(),
            None,
            "target-session".to_string(),
            Arc::new(Notify::new()),
        )
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let source_scope = playback_sessions::PlaybackScopeKey {
            plugin_id: "native",
            user_db_id: DbId(9),
            session_key: "source-session",
        };
        playback_sessions::bind_current_playback_session_scope(
            &source_scope,
            DbId(10),
            "playback-session".to_string(),
            1,
        );
        let (token, completion_rx) = begin(
            Some(source.connection_id),
            target.connection_id,
            DbId(9),
            "playback".to_string(),
            4,
        )
        .await
        .map_err(anyhow::Error::msg)?;
        playback_sessions::bind_current_playback_session_scope(
            &source_scope,
            DbId(11),
            "newer-source-playback".to_string(),
            2,
        );
        let committed = claim_progress(&token, DbId(9), "target-session", "playback", 4)
            .await
            .map_err(anyhow::Error::msg)?
            .commit(test_applied_progress());

        assert!(committed.finish().await);
        assert!(completion_rx.await?.is_ok());
        let source_after = playback_sessions::get_playback_session(&source_scope)
            .expect("newer source binding must survive handoff completion");
        assert_eq!(source_after.current_playback_session_id, Some(DbId(11)));
        assert_eq!(source_after.previous_playback_session_id, Some(DbId(10)));
        let target_scope = playback_sessions::PlaybackScopeKey {
            plugin_id: "native",
            user_db_id: DbId(9),
            session_key: "target-session",
        };
        assert_eq!(
            playback_sessions::get_playback_session(&target_scope)
                .and_then(|scope| scope.current_playback_session_id),
            Some(DbId(10))
        );
        playback_sessions::clear_playback_session_scope(&target_scope);
        playback_sessions::clear_playback_session_scope(&source_scope);
        registry::unregister(source.connection_id).await;
        registry::unregister(target.connection_id).await;
        drop(source.command_rx);
        drop(target.command_rx);
        Ok(())
    }

    #[tokio::test]
    async fn disconnected_target_cannot_be_bound_after_progress_claim() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        crate::testing::init_default_test_state()?;
        for connection in registry::list_connections().await {
            registry::unregister(connection.connection_id).await;
        }
        let target = registry::register(
            DbId(9),
            "user".to_string(),
            None,
            "target-session".to_string(),
            Arc::new(Notify::new()),
        )
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let (token, completion_rx) = begin(
            None,
            target.connection_id,
            DbId(9),
            "playback".to_string(),
            4,
        )
        .await
        .map_err(anyhow::Error::msg)?;
        let _claim = claim_progress(&token, DbId(9), "target-session", "playback", 4)
            .await
            .map_err(anyhow::Error::msg)?;
        registry::unregister(target.connection_id).await;
        let applied = test_applied_progress();

        assert!(matches!(
            registry::finish_handoff_progress(&token, &applied).await,
            registry::FinishProgress::Failed
        ));
        assert!(completion_rx.await?.is_err());
        let target_scope = playback_sessions::PlaybackScopeKey {
            plugin_id: "native",
            user_db_id: DbId(9),
            session_key: "target-session",
        };
        assert!(playback_sessions::get_playback_session(&target_scope).is_none());
        drop(target.command_rx);
        Ok(())
    }
}
