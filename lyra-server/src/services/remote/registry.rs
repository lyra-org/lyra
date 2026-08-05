// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};
use std::sync::{
    Arc,
    LazyLock,
};

use agdb::DbId;
use tokio::sync::{
    Notify,
    RwLock,
    mpsc,
    oneshot,
};

use super::constants::{
    MAX_CONNECTIONS_PER_USER,
    RemoteAction,
};
use super::handoffs::{
    AppliedProgress,
    ExpectedSourceBinding,
    PendingHandoffs,
};
use super::messages::OutgoingMessage;
use crate::services::playback_sessions;

pub(crate) type ConnectionId = u64;

const COMMAND_CHANNEL_CAPACITY: usize = 16;

/// `user_db_id` is metadata only; authorize/evict/count on `user_public_id`.
pub(crate) struct ConnectionHandle {
    pub(crate) connection_id: ConnectionId,
    pub(crate) token: String,
    pub(crate) user_db_id: DbId,
    pub(crate) user_public_id: String,
    pub(crate) client_name: Option<String>,
    pub(crate) session_key: String,
    pub(crate) cancel: Arc<Notify>,
    pub(crate) command_tx: mpsc::Sender<OutgoingMessage>,
    pub(crate) supported_commands: HashSet<RemoteAction>,
}

static REGISTRY: LazyLock<RwLock<ConnectionRegistry>> =
    LazyLock::new(|| RwLock::new(ConnectionRegistry::new()));

struct ConnectionRegistry {
    connections: HashMap<ConnectionId, ConnectionHandle>,
    tokens: HashMap<String, ConnectionId>,
    next_id: ConnectionId,
    pending_handoffs: PendingHandoffs,
}

impl ConnectionRegistry {
    fn new() -> Self {
        Self {
            connections: HashMap::new(),
            tokens: HashMap::new(),
            next_id: 1,
            pending_handoffs: PendingHandoffs::new(),
        }
    }

    fn count_user_connections(&self, user_public_id: &str) -> usize {
        self.connections
            .values()
            .filter(|h| h.user_public_id == user_public_id)
            .count()
    }

    /// The evicted connection's `unregister` call will block on the registry write
    /// lock until the caller's `register` (which holds the lock for both `evict_duplicate`
    /// and `insert`) releases it. This ordering is intentional and load-bearing.
    fn evict_duplicate(
        &mut self,
        user_public_id: &str,
        session_key: &str,
    ) -> Option<ConnectionHandle> {
        let dup_id = self.connections.iter().find_map(|(&id, h)| {
            (h.user_public_id == user_public_id && h.session_key == session_key).then_some(id)
        });
        if let Some(id) = dup_id {
            let handle = self.remove(id)?;
            handle.cancel.notify_one();
            Some(handle)
        } else {
            None
        }
    }

    fn insert(
        &mut self,
        user_db_id: DbId,
        user_public_id: String,
        client_name: Option<String>,
        session_key: String,
        cancel: Arc<Notify>,
        command_tx: mpsc::Sender<OutgoingMessage>,
    ) -> Result<ConnectionId, RegistryError> {
        let id = self
            .next_id
            .checked_add(1)
            .ok_or(RegistryError::IdExhausted)?;

        if self.count_user_connections(&user_public_id) >= MAX_CONNECTIONS_PER_USER {
            return Err(RegistryError::TooManyConnections);
        }

        let connection_id = self.next_id;
        self.next_id = id;
        let token = nanoid::nanoid!();
        self.tokens.insert(token.clone(), connection_id);
        self.connections.insert(
            connection_id,
            ConnectionHandle {
                connection_id,
                token: token.clone(),
                user_db_id,
                user_public_id,
                client_name,
                session_key,
                cancel,
                command_tx,
                supported_commands: HashSet::new(),
            },
        );
        Ok(connection_id)
    }

    fn remove(&mut self, id: ConnectionId) -> Option<ConnectionHandle> {
        let handle = self.connections.remove(&id)?;
        self.tokens.remove(&handle.token);
        self.pending_handoffs.fail_for_connection(id);
        Some(handle)
    }

    fn connection_and_token_target(
        &self,
        source_id: ConnectionId,
        target_token: &str,
    ) -> (Option<ConnectionSnapshot>, Option<ConnectionSnapshot>) {
        let source = self.connections.get(&source_id).map(snapshot_from_handle);
        let target = self
            .tokens
            .get(target_token)
            .and_then(|id| self.connections.get(id))
            .map(snapshot_from_handle);
        (source, target)
    }
}

#[derive(Debug)]
pub(crate) enum RegistryError {
    TooManyConnections,
    IdExhausted,
}

impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooManyConnections => write!(f, "too many connections for this user"),
            Self::IdExhausted => write!(f, "connection ID space exhausted"),
        }
    }
}

/// Pre-checks ID availability and connection cap before evicting, so a failed
/// registration never tears down an existing connection.
pub(crate) struct RegisterResult {
    pub(crate) connection_id: ConnectionId,
    pub(crate) evicted: Option<ConnectionHandle>,
    pub(crate) command_rx: mpsc::Receiver<OutgoingMessage>,
}

pub(crate) async fn register(
    user_db_id: DbId,
    user_public_id: String,
    client_name: Option<String>,
    session_key: String,
    cancel: Arc<Notify>,
) -> Result<RegisterResult, RegistryError> {
    let mut registry = REGISTRY.write().await;

    registry
        .next_id
        .checked_add(1)
        .ok_or(RegistryError::IdExhausted)?;

    let has_duplicate = registry
        .connections
        .values()
        .any(|h| h.user_public_id == user_public_id && h.session_key == session_key);
    let effective_count =
        registry.count_user_connections(&user_public_id) - if has_duplicate { 1 } else { 0 };
    if effective_count >= MAX_CONNECTIONS_PER_USER {
        return Err(RegistryError::TooManyConnections);
    }

    let evicted = registry.evict_duplicate(&user_public_id, &session_key);
    let (command_tx, command_rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
    let connection_id = registry.insert(
        user_db_id,
        user_public_id,
        client_name,
        session_key,
        cancel,
        command_tx,
    )?;
    Ok(RegisterResult {
        connection_id,
        evicted,
        command_rx,
    })
}

pub(crate) async fn set_supported_commands(
    connection_id: ConnectionId,
    commands: HashSet<RemoteAction>,
) -> bool {
    let mut registry = REGISTRY.write().await;
    if let Some(handle) = registry.connections.get_mut(&connection_id) {
        handle.supported_commands = commands;
        true
    } else {
        false
    }
}

pub(crate) async fn get_connection_and_token_target(
    source_id: ConnectionId,
    target_token: &str,
) -> (Option<ConnectionSnapshot>, Option<ConnectionSnapshot>) {
    let registry = REGISTRY.read().await;
    registry.connection_and_token_target(source_id, target_token)
}

/// Errors if the target is not found or the message could not be queued.
pub(crate) async fn send_to_connection(
    target_id: ConnectionId,
    msg: OutgoingMessage,
) -> Result<(), String> {
    let registry = REGISTRY.read().await;
    let handle = registry
        .connections
        .get(&target_id)
        .ok_or_else(|| format!("target connection {target_id} not found"))?;
    handle
        .command_tx
        .try_send(msg)
        .map_err(|_| "could not queue message for target connection".to_string())
}

pub(super) async fn insert_handoff(
    source_id: Option<ConnectionId>,
    target_id: ConnectionId,
    user_db_id: DbId,
    playback_id: String,
    queue_revision: u64,
) -> Result<(String, oneshot::Receiver<Result<(), String>>, Arc<Notify>), String> {
    let mut registry = REGISTRY.write().await;
    if source_id.is_some_and(|source_id| !registry.connections.contains_key(&source_id)) {
        return Err("source connection not found".to_string());
    }
    if !registry.connections.contains_key(&target_id) {
        return Err("target connection not found".to_string());
    }
    let source_binding = source_id
        .and_then(|source_id| registry.connections.get(&source_id))
        .and_then(|source| {
            let scope_key = playback_sessions::PlaybackScopeKey {
                plugin_id: "native",
                user_db_id: source.user_db_id,
                session_key: &source.session_key,
            };
            playback_sessions::snapshot_current_binding(&scope_key).map(|snapshot| {
                ExpectedSourceBinding {
                    user_db_id: source.user_db_id,
                    session_key: source.session_key.clone(),
                    snapshot,
                }
            })
        });
    registry.pending_handoffs.begin(
        source_id,
        source_binding,
        target_id,
        user_db_id,
        playback_id,
        queue_revision,
    )
}

pub(super) async fn queue_handoff_command(
    token: &str,
    target_id: ConnectionId,
    msg: OutgoingMessage,
) -> Result<(), String> {
    let registry = REGISTRY.write().await;
    let pending_target = registry.pending_handoffs.pending_target(token)?;
    if pending_target != target_id {
        return Err("handoff command target changed".to_string());
    }
    let handle = registry
        .connections
        .get(&target_id)
        .ok_or_else(|| format!("target connection {target_id} not found"))?;
    handle
        .command_tx
        .try_send(msg)
        .map_err(|_| "could not queue message for target connection".to_string())
}

pub(super) async fn claim_handoff_progress(
    token: &str,
    user_db_id: DbId,
    session_key: &str,
    playback_id: &str,
    queue_revision: u64,
) -> Result<(), String> {
    let mut registry = REGISTRY.write().await;
    let target_id = registry.pending_handoffs.validate_progress_reference(
        token,
        user_db_id,
        playback_id,
        queue_revision,
    )?;
    let target = registry
        .connections
        .get(&target_id)
        .ok_or_else(|| "handoff target connection not found".to_string())?;
    if target.user_db_id != user_db_id || target.session_key != session_key {
        return Err("handoff progress did not come from the designated target".to_string());
    }
    registry.pending_handoffs.claim_progress(token)
}

pub(super) enum FinishProgress {
    Completed,
    Failed,
    Missing,
}

pub(super) async fn finish_handoff_progress(
    token: &str,
    applied: &AppliedProgress,
) -> FinishProgress {
    let mut registry = REGISTRY.write().await;
    let Some(result) = registry.pending_handoffs.progress_result(token) else {
        return FinishProgress::Missing;
    };
    let Ok(ready) = result else {
        registry.pending_handoffs.finish_progress(token);
        return FinishProgress::Failed;
    };
    let Some(target) = registry.connections.get(&ready.target_id) else {
        registry.pending_handoffs.fail(token, "handoff target connection disconnected");
        registry.pending_handoffs.finish_progress(token);
        return FinishProgress::Failed;
    };
    playback_sessions::bind_current_playback_session_scope(
        &playback_sessions::PlaybackScopeKey {
            plugin_id: "native",
            user_db_id: target.user_db_id,
            session_key: &target.session_key,
        },
        applied
            .expected_session
            .db_id
            .expect("applied handoff progress must retain its session database ID"),
        applied.expected_session.id.clone(),
        applied.expected_session.updated_at_ms,
    );
    if let Some(source) = ready.source_binding {
        playback_sessions::clear_current_binding_if_unchanged(
            &playback_sessions::PlaybackScopeKey {
                plugin_id: "native",
                user_db_id: source.user_db_id,
                session_key: &source.session_key,
            },
            &source.snapshot,
        );
    }
    registry.pending_handoffs.finish_progress(token);
    FinishProgress::Completed
}

pub(super) async fn abort_handoff_progress(token: &str, error: String) -> bool {
    REGISTRY
        .write()
        .await
        .pending_handoffs
        .abort_progress(token, error)
}

#[cfg(test)]
pub(super) async fn hold_write_lock_for_test() -> impl Drop {
    REGISTRY.write().await
}

pub(super) async fn fail_handoff(token: &str, error: impl Into<String>) -> bool {
    REGISTRY.write().await.pending_handoffs.fail(token, error)
}

pub(super) async fn fail_handoffs_for_playback_revision(
    playback_id: &str,
    current_revision: u64,
) -> usize {
    REGISTRY
        .write()
        .await
        .pending_handoffs
        .fail_for_playback_revision(playback_id, current_revision)
}

pub(super) async fn fail_handoffs_for_playback(playback_id: &str) -> usize {
    REGISTRY
        .write()
        .await
        .pending_handoffs
        .fail_for_playback(playback_id)
}

pub(crate) async fn resolve_token(token: &str) -> Option<ConnectionSnapshot> {
    let registry = REGISTRY.read().await;
    let &id = registry.tokens.get(token)?;
    registry.connections.get(&id).map(snapshot_from_handle)
}

pub(crate) async fn unregister(connection_id: ConnectionId) -> Option<ConnectionHandle> {
    REGISTRY.write().await.remove(connection_id)
}

pub(crate) async fn list_connections() -> Vec<ConnectionSnapshot> {
    let registry = REGISTRY.read().await;
    registry
        .connections
        .values()
        .map(snapshot_from_handle)
        .collect()
}

fn snapshot_from_handle(handle: &ConnectionHandle) -> ConnectionSnapshot {
    ConnectionSnapshot {
        connection_id: handle.connection_id,
        token: handle.token.clone(),
        user_db_id: handle.user_db_id,
        user_public_id: handle.user_public_id.clone(),
        client_name: handle.client_name.clone(),
        session_key: handle.session_key.clone(),
        supported_commands: handle.supported_commands.iter().cloned().collect(),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ConnectionSnapshot {
    pub(crate) connection_id: ConnectionId,
    pub(crate) token: String,
    pub(crate) user_db_id: DbId,
    pub(crate) user_public_id: String,
    pub(crate) client_name: Option<String>,
    pub(crate) session_key: String,
    pub(crate) supported_commands: Vec<RemoteAction>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    fn test_registry() -> ConnectionRegistry {
        ConnectionRegistry::new()
    }

    fn test_cancel() -> Arc<Notify> {
        Arc::new(Notify::new())
    }

    fn test_tx() -> mpsc::Sender<OutgoingMessage> {
        mpsc::channel(1).0
    }

    #[test]
    fn insert_assigns_sequential_ids() {
        let mut reg = test_registry();
        let id1 = reg
            .insert(
                DbId(1),
                "user-1".into(),
                None,
                "a".into(),
                test_cancel(),
                test_tx(),
            )
            .unwrap();
        let id2 = reg
            .insert(
                DbId(1),
                "user-1".into(),
                None,
                "b".into(),
                test_cancel(),
                test_tx(),
            )
            .unwrap();
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        let token1 = reg.connections[&id1].token.clone();
        let token2 = reg.connections[&id2].token.clone();
        assert_ne!(token1, token2);
        assert_eq!(reg.tokens.len(), 2);
        assert_eq!(reg.tokens[&token1], id1);
        assert_eq!(reg.tokens[&token2], id2);
    }

    #[test]
    fn insert_rejects_when_user_at_cap() {
        let mut reg = test_registry();
        for i in 0..MAX_CONNECTIONS_PER_USER {
            reg.insert(
                DbId(1),
                "user-1".into(),
                None,
                format!("key-{i}"),
                test_cancel(),
                test_tx(),
            )
            .unwrap();
        }
        let err = reg
            .insert(
                DbId(1),
                "user-1".into(),
                None,
                "overflow".into(),
                test_cancel(),
                test_tx(),
            )
            .unwrap_err();
        assert!(matches!(err, RegistryError::TooManyConnections));
    }

    #[test]
    fn insert_allows_different_users_independently() {
        let mut reg = test_registry();
        reg.insert(
            DbId(1),
            "user-1".into(),
            None,
            "a".into(),
            test_cancel(),
            test_tx(),
        )
        .unwrap();
        reg.insert(
            DbId(2),
            "user-2".into(),
            None,
            "a".into(),
            test_cancel(),
            test_tx(),
        )
        .unwrap();
        assert_eq!(reg.count_user_connections("user-1"), 1);
        assert_eq!(reg.count_user_connections("user-2"), 1);
    }

    #[test]
    fn count_user_connections_keys_on_public_id_not_db_id() {
        let mut reg = test_registry();
        reg.insert(
            DbId(42),
            "alice".into(),
            None,
            "a".into(),
            test_cancel(),
            test_tx(),
        )
        .unwrap();
        reg.insert(
            DbId(42),
            "bob".into(),
            None,
            "a".into(),
            test_cancel(),
            test_tx(),
        )
        .unwrap();
        assert_eq!(reg.count_user_connections("alice"), 1);
        assert_eq!(reg.count_user_connections("bob"), 1);
    }

    #[test]
    fn evict_duplicate_removes_matching_connection_and_token() {
        let mut reg = test_registry();
        let id = reg
            .insert(
                DbId(1),
                "user-1".into(),
                None,
                "key".into(),
                test_cancel(),
                test_tx(),
            )
            .unwrap();
        let token = reg.connections[&id].token.clone();
        assert!(reg.tokens.contains_key(&token));
        let evicted = reg.evict_duplicate("user-1", "key");
        assert!(evicted.is_some());
        assert_eq!(reg.count_user_connections("user-1"), 0);
        assert!(!reg.tokens.contains_key(&token));
    }

    #[test]
    fn evict_duplicate_returns_none_when_no_match() {
        let mut reg = test_registry();
        reg.insert(
            DbId(1),
            "user-1".into(),
            None,
            "key".into(),
            test_cancel(),
            test_tx(),
        )
        .unwrap();
        let evicted = reg.evict_duplicate("user-1", "other-key");
        assert!(evicted.is_none());
        assert_eq!(reg.count_user_connections("user-1"), 1);
    }

    #[test]
    fn evict_duplicate_does_not_match_recycled_db_id_with_different_public_id() {
        let mut reg = test_registry();
        reg.insert(
            DbId(42),
            "alice".into(),
            None,
            "key".into(),
            test_cancel(),
            test_tx(),
        )
        .unwrap();
        let evicted = reg.evict_duplicate("bob", "key");
        assert!(
            evicted.is_none(),
            "must not evict alice's connection on bob's register",
        );
        assert_eq!(reg.count_user_connections("alice"), 1);
    }

    #[test]
    fn evict_duplicate_notifies_cancel() {
        let mut reg = test_registry();
        let cancel = test_cancel();
        let cancel_clone = cancel.clone();
        reg.insert(
            DbId(1),
            "user-1".into(),
            None,
            "key".into(),
            cancel,
            test_tx(),
        )
        .unwrap();

        reg.evict_duplicate("user-1", "key");

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(async {
            tokio::time::timeout(
                std::time::Duration::from_millis(10),
                cancel_clone.notified(),
            )
            .await
            .expect("cancel should have been notified");
        });
    }

    #[test]
    fn remove_returns_handle_and_cleans_token() {
        let mut reg = test_registry();
        let id = reg
            .insert(
                DbId(1),
                "user-1".into(),
                None,
                "key".into(),
                test_cancel(),
                test_tx(),
            )
            .unwrap();
        let token = reg.connections[&id].token.clone();
        assert!(reg.tokens.contains_key(&token));
        let handle = reg.remove(id);
        assert!(handle.is_some());
        assert_eq!(handle.unwrap().session_key, "key");
        assert!(reg.remove(id).is_none());
        assert!(!reg.tokens.contains_key(&token));
    }

    #[test]
    fn connection_and_token_target_resolves_opaque_target_token() {
        let mut reg = test_registry();
        let source_id = reg
            .insert(
                DbId(1),
                "user-1".into(),
                None,
                "source".into(),
                test_cancel(),
                test_tx(),
            )
            .unwrap();
        let target_id = reg
            .insert(
                DbId(1),
                "user-1".into(),
                Some("Living Room".to_string()),
                "target".into(),
                test_cancel(),
                test_tx(),
            )
            .unwrap();
        let target_token = reg.connections[&target_id].token.clone();
        let (source, target) = reg.connection_and_token_target(source_id, &target_token);

        assert_eq!(source.unwrap().connection_id, source_id);
        let target = target.unwrap();
        assert_eq!(target.connection_id, target_id);
        assert_eq!(target.client_name.as_deref(), Some("Living Room"));
        assert_eq!(target.session_key, "target");
    }

    #[test]
    fn id_exhaustion_detected() {
        let mut reg = test_registry();
        reg.next_id = u64::MAX;
        let err = reg
            .insert(
                DbId(1),
                "user-1".into(),
                None,
                "key".into(),
                test_cancel(),
                test_tx(),
            )
            .unwrap_err();
        assert!(matches!(err, RegistryError::IdExhausted));
    }
}
