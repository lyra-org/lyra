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

use tokio::sync::{
    Notify,
    RwLock,
    mpsc,
    watch,
};

use super::constants::{
    CloseReason,
    MAX_CONNECTIONS_PER_USER,
    RemoteAction,
};
use super::control::ControlError;
use super::handoffs::{
    AppliedProgress,
    ExpectedSourceBinding,
    PendingHandoffs,
};
use super::messages::OutgoingMessage;
use crate::services::playback_sessions;
use crate::services::playbacks::bindings;

pub(crate) type ConnectionId = u64;

const COMMAND_CHANNEL_CAPACITY: usize = 16;

pub(crate) type CloseSender = watch::Sender<Option<CloseReason>>;

pub(crate) struct ConnectionHandle {
    pub(crate) connection_id: ConnectionId,
    pub(crate) public_id: String,
    pub(crate) user_public_id: String,
    pub(crate) client_name: Option<String>,
    pub(crate) session_key: String,
    pub(crate) connected_at_ms: u64,
    pub(crate) close: CloseSender,
    pub(crate) command_tx: mpsc::Sender<OutgoingMessage>,
    pub(crate) supported_commands: HashSet<RemoteAction>,
}

static REGISTRY: LazyLock<RwLock<ConnectionRegistry>> =
    LazyLock::new(|| RwLock::new(ConnectionRegistry::new()));

struct ConnectionRegistry {
    connections: HashMap<ConnectionId, ConnectionHandle>,
    public_ids: HashMap<String, ConnectionId>,
    next_id: ConnectionId,
    pending_handoffs: PendingHandoffs,
}

impl ConnectionRegistry {
    fn new() -> Self {
        Self {
            connections: HashMap::new(),
            public_ids: HashMap::new(),
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
            request_close(&handle.close, CloseReason::DuplicateSession);
            Some(handle)
        } else {
            None
        }
    }

    fn insert(
        &mut self,
        user_public_id: String,
        client_name: Option<String>,
        session_key: String,
        connected_at_ms: u64,
        close: CloseSender,
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
        let public_id = nanoid::nanoid!();
        self.public_ids.insert(public_id.clone(), connection_id);
        self.connections.insert(
            connection_id,
            ConnectionHandle {
                connection_id,
                public_id,
                user_public_id,
                client_name,
                session_key,
                connected_at_ms,
                close,
                command_tx,
                supported_commands: HashSet::new(),
            },
        );
        Ok(connection_id)
    }

    fn remove(&mut self, id: ConnectionId) -> Option<ConnectionHandle> {
        let handle = self.connections.remove(&id)?;
        self.public_ids.remove(&handle.public_id);
        self.pending_handoffs.fail_for_connection(id);
        Some(handle)
    }

    fn connection_and_target(
        &self,
        source_id: ConnectionId,
        target_public_id: &str,
    ) -> (Option<ConnectionSnapshot>, Option<ConnectionSnapshot>) {
        let source = self.connections.get(&source_id).map(snapshot_from_handle);
        let target = self
            .public_ids
            .get(target_public_id)
            .and_then(|id| self.connections.get(id))
            .map(snapshot_from_handle);
        (source, target)
    }
}

#[derive(Debug)]
pub(crate) enum RegistryError {
    TooManyConnections,
    IdExhausted,
    Clock,
}

impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooManyConnections => write!(f, "too many connections for this user"),
            Self::IdExhausted => write!(f, "connection ID space exhausted"),
            Self::Clock => write!(f, "system clock is before unix epoch"),
        }
    }
}

/// Pre-checks ID availability and connection cap before evicting, so a failed
/// registration never tears down an existing connection.
pub(crate) struct RegisterResult {
    pub(crate) connection_id: ConnectionId,
    pub(crate) public_id: String,
    pub(crate) evicted: Option<ConnectionHandle>,
    pub(crate) command_rx: mpsc::Receiver<OutgoingMessage>,
}

pub(crate) async fn register(
    user_public_id: String,
    client_name: Option<String>,
    session_key: String,
    close: CloseSender,
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

    let connected_at_ms = playback_sessions::now_ms().map_err(|_| RegistryError::Clock)?;
    let evicted = registry.evict_duplicate(&user_public_id, &session_key);
    let (command_tx, command_rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
    let connection_id = registry.insert(
        user_public_id.clone(),
        client_name,
        session_key,
        connected_at_ms,
        close,
        command_tx,
    )?;
    playback_sessions::notify_connection_change(&user_public_id);
    let public_id = registry.connections[&connection_id].public_id.clone();
    Ok(RegisterResult {
        connection_id,
        public_id,
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
        playback_sessions::notify_connection_change(&handle.user_public_id);
        true
    } else {
        false
    }
}

pub(super) async fn get_connection_and_target(
    source_id: ConnectionId,
    target_public_id: &str,
) -> (Option<ConnectionSnapshot>, Option<ConnectionSnapshot>) {
    let registry = REGISTRY.read().await;
    registry.connection_and_target(source_id, target_public_id)
}

/// Errors if the target is not found or the message could not be queued.
pub(super) async fn send_to_connection(
    target_id: ConnectionId,
    msg: OutgoingMessage,
) -> Result<(), ControlError> {
    let registry = REGISTRY.read().await;
    let handle = registry
        .connections
        .get(&target_id)
        .ok_or(ControlError::TargetNotFound)?;
    handle
        .command_tx
        .try_send(msg)
        .map_err(|_| ControlError::TargetBusy)
}

pub(super) async fn insert_handoff(
    source_id: Option<ConnectionId>,
    target_id: ConnectionId,
    user_public_id: String,
    playback_id: String,
    queue_revision: u64,
) -> Result<(String, super::handoffs::HandoffCompletion, Arc<Notify>), String> {
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
            let key = bindings::ConnectionKey::new(&source.user_public_id, &source.session_key);
            bindings::snapshot_current(&key).map(|snapshot| ExpectedSourceBinding {
                user_public_id: source.user_public_id.clone(),
                session_key: source.session_key.clone(),
                snapshot,
            })
        });
    registry.pending_handoffs.begin(
        source_id,
        source_binding,
        target_id,
        user_public_id,
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
    user_public_id: &str,
    session_key: &str,
    playback_id: &str,
    queue_revision: u64,
) -> Result<(), String> {
    let mut registry = REGISTRY.write().await;
    let target_id = registry.pending_handoffs.validate_progress_reference(
        token,
        user_public_id,
        playback_id,
        queue_revision,
    )?;
    let target = registry
        .connections
        .get(&target_id)
        .ok_or_else(|| "handoff target connection not found".to_string())?;
    if target.user_public_id != user_public_id || target.session_key != session_key {
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
    invalid_reference: Option<String>,
) -> FinishProgress {
    let mut registry = REGISTRY.write().await;
    if let Some(error) = invalid_reference {
        if !registry.pending_handoffs.fail(token, error) {
            return FinishProgress::Missing;
        }
        registry.pending_handoffs.finish_progress(token);
        return FinishProgress::Failed;
    }
    let Some(result) = registry.pending_handoffs.progress_result(token) else {
        return FinishProgress::Missing;
    };
    let Ok(ready) = result else {
        registry.pending_handoffs.finish_progress(token);
        return FinishProgress::Failed;
    };
    let Some(target) = registry.connections.get(&ready.target_id) else {
        registry
            .pending_handoffs
            .fail(token, "handoff target connection disconnected");
        registry.pending_handoffs.finish_progress(token);
        return FinishProgress::Failed;
    };
    bindings::bind_current(
        &bindings::ConnectionKey::new(&target.user_public_id, &target.session_key),
        &applied.playback_public_id,
        applied
            .expected_session
            .db_id
            .expect("applied handoff progress must retain its session database ID"),
        applied.expected_session.id.clone(),
        applied.expected_session.updated_at_ms,
    );
    if let Some(source) = ready.source_binding {
        bindings::clear_current_if_unchanged(
            &bindings::ConnectionKey::new(&source.user_public_id, &source.session_key),
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

pub(super) async fn resolve_public_id(public_id: &str) -> Option<ConnectionSnapshot> {
    let registry = REGISTRY.read().await;
    let &id = registry.public_ids.get(public_id)?;
    registry.connections.get(&id).map(snapshot_from_handle)
}

pub(crate) async fn unregister(connection_id: ConnectionId) -> Option<ConnectionHandle> {
    unregister_with(connection_id, |_| ())
        .await
        .map(|(handle, ())| handle)
}

/// [`unregister`], running `before_removal` on the connection while it is still registered.
pub(crate) async fn unregister_with<T>(
    connection_id: ConnectionId,
    before_removal: impl FnOnce(&ConnectionHandle) -> T,
) -> Option<(ConnectionHandle, T)> {
    let mut registry = REGISTRY.write().await;
    let observed = before_removal(registry.connections.get(&connection_id)?);
    let handle = registry.remove(connection_id)?;
    drop(registry);
    playback_sessions::notify_connection_change(&handle.user_public_id);
    Some((handle, observed))
}

fn request_close(close: &CloseSender, reason: CloseReason) {
    close.send_if_modified(|current| {
        if current.is_some() {
            return false;
        }
        *current = Some(reason);
        true
    });
}

/// Asks every connection of the user to close; each unregisters itself on the way out.
pub(crate) async fn close_user_connections(user_public_id: &str, reason: CloseReason) -> usize {
    let registry = REGISTRY.read().await;
    let mut closed = 0;
    for handle in registry
        .connections
        .values()
        .filter(|handle| handle.user_public_id == user_public_id)
    {
        request_close(&handle.close, reason);
        closed += 1;
    }
    closed
}

pub(crate) async fn list_connections() -> Vec<ConnectionSnapshot> {
    let registry = REGISTRY.read().await;
    registry
        .connections
        .values()
        .map(snapshot_from_handle)
        .collect()
}

pub(crate) async fn list_user_connections(user_public_id: &str) -> Vec<ConnectionSnapshot> {
    let registry = REGISTRY.read().await;
    registry
        .connections
        .values()
        .filter(|handle| handle.user_public_id == user_public_id)
        .map(snapshot_from_handle)
        .collect()
}

fn snapshot_from_handle(handle: &ConnectionHandle) -> ConnectionSnapshot {
    let mut supported_commands = handle
        .supported_commands
        .iter()
        .copied()
        .collect::<Vec<_>>();
    supported_commands.sort();
    ConnectionSnapshot {
        connection_id: handle.connection_id,
        public_id: handle.public_id.clone(),
        user_public_id: handle.user_public_id.clone(),
        client_name: handle.client_name.clone(),
        session_key: handle.session_key.clone(),
        connected_at_ms: handle.connected_at_ms,
        supported_commands,
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ConnectionSnapshot {
    pub(crate) connection_id: ConnectionId,
    pub(crate) public_id: String,
    pub(crate) user_public_id: String,
    pub(crate) client_name: Option<String>,
    pub(crate) session_key: String,
    pub(crate) connected_at_ms: u64,
    pub(crate) supported_commands: Vec<RemoteAction>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    fn test_registry() -> ConnectionRegistry {
        ConnectionRegistry::new()
    }

    fn test_close() -> CloseSender {
        watch::channel(None).0
    }

    fn test_tx() -> mpsc::Sender<OutgoingMessage> {
        mpsc::channel(1).0
    }

    #[test]
    fn insert_assigns_sequential_ids() {
        let mut reg = test_registry();
        let id1 = reg
            .insert(
                "user-1".into(),
                None,
                "a".into(),
                0,
                test_close(),
                test_tx(),
            )
            .unwrap();
        let id2 = reg
            .insert(
                "user-1".into(),
                None,
                "b".into(),
                0,
                test_close(),
                test_tx(),
            )
            .unwrap();
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        let id1_public = reg.connections[&id1].public_id.clone();
        let id2_public = reg.connections[&id2].public_id.clone();
        assert_eq!(reg.public_ids.len(), 2);
        assert_eq!(reg.public_ids[&id1_public], id1);
        assert_eq!(reg.public_ids[&id2_public], id2);
    }

    #[test]
    fn insert_rejects_when_user_at_cap() {
        let mut reg = test_registry();
        for i in 0..MAX_CONNECTIONS_PER_USER {
            reg.insert(
                "user-1".into(),
                None,
                format!("key-{i}"),
                0,
                test_close(),
                test_tx(),
            )
            .unwrap();
        }
        let err = reg
            .insert(
                "user-1".into(),
                None,
                "overflow".into(),
                0,
                test_close(),
                test_tx(),
            )
            .unwrap_err();
        assert!(matches!(err, RegistryError::TooManyConnections));
    }

    #[test]
    fn insert_allows_different_users_independently() {
        let mut reg = test_registry();
        reg.insert(
            "user-1".into(),
            None,
            "a".into(),
            0,
            test_close(),
            test_tx(),
        )
        .unwrap();
        reg.insert(
            "user-2".into(),
            None,
            "a".into(),
            0,
            test_close(),
            test_tx(),
        )
        .unwrap();
        assert_eq!(reg.count_user_connections("user-1"), 1);
        assert_eq!(reg.count_user_connections("user-2"), 1);
    }

    #[test]
    fn evict_duplicate_removes_matching_connection_and_public_id() {
        let mut reg = test_registry();
        let id = reg
            .insert(
                "user-1".into(),
                None,
                "key".into(),
                0,
                test_close(),
                test_tx(),
            )
            .unwrap();
        let public_id = reg.connections[&id].public_id.clone();
        assert!(reg.public_ids.contains_key(&public_id));
        let evicted = reg.evict_duplicate("user-1", "key");
        assert!(evicted.is_some());
        assert_eq!(reg.count_user_connections("user-1"), 0);
        assert!(!reg.public_ids.contains_key(&public_id));
    }

    #[test]
    fn evict_duplicate_returns_none_when_no_match() {
        let mut reg = test_registry();
        reg.insert(
            "user-1".into(),
            None,
            "key".into(),
            0,
            test_close(),
            test_tx(),
        )
        .unwrap();
        let evicted = reg.evict_duplicate("user-1", "other-key");
        assert!(evicted.is_none());
        assert_eq!(reg.count_user_connections("user-1"), 1);
    }

    #[test]
    fn evict_duplicate_only_matches_the_same_user() {
        let mut reg = test_registry();
        reg.insert(
            "alice".into(),
            None,
            "key".into(),
            0,
            test_close(),
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
    fn evict_duplicate_requests_duplicate_session_close() {
        let mut reg = test_registry();
        let (close, close_rx) = watch::channel(None);
        reg.insert("user-1".into(), None, "key".into(), 0, close, test_tx())
            .unwrap();

        reg.evict_duplicate("user-1", "key");

        assert_eq!(*close_rx.borrow(), Some(CloseReason::DuplicateSession));
    }

    #[tokio::test]
    async fn close_user_connections_only_signals_that_user() {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state().expect("test state");
        for connection in list_connections().await {
            unregister(connection.connection_id).await;
        }
        let (alice_close, alice_rx) = watch::channel(None);
        let (bob_close, bob_rx) = watch::channel(None);
        let alice = register("alice".into(), None, "key".into(), alice_close)
            .await
            .unwrap();
        let bob = register("bob".into(), None, "key".into(), bob_close)
            .await
            .unwrap();

        assert_eq!(
            close_user_connections("alice", CloseReason::UserDeleted).await,
            1
        );
        assert_eq!(*alice_rx.borrow(), Some(CloseReason::UserDeleted));
        assert_eq!(*bob_rx.borrow(), None);

        unregister(alice.connection_id).await;
        unregister(bob.connection_id).await;
    }

    #[test]
    fn remove_returns_handle_and_cleans_public_id() {
        let mut reg = test_registry();
        let id = reg
            .insert(
                "user-1".into(),
                None,
                "key".into(),
                0,
                test_close(),
                test_tx(),
            )
            .unwrap();
        let public_id = reg.connections[&id].public_id.clone();
        assert!(reg.public_ids.contains_key(&public_id));
        let handle = reg.remove(id);
        assert!(handle.is_some());
        assert_eq!(handle.unwrap().session_key, "key");
        assert!(reg.remove(id).is_none());
        assert!(!reg.public_ids.contains_key(&public_id));
    }

    #[test]
    fn connection_and_target_resolves_the_target_public_id() {
        let mut reg = test_registry();
        let source_id = reg
            .insert(
                "user-1".into(),
                None,
                "source".into(),
                0,
                test_close(),
                test_tx(),
            )
            .unwrap();
        let target_id = reg
            .insert(
                "user-1".into(),
                Some("Living Room".to_string()),
                "target".into(),
                0,
                test_close(),
                test_tx(),
            )
            .unwrap();
        let target_public_id = reg.connections[&target_id].public_id.clone();
        let (source, target) = reg.connection_and_target(source_id, &target_public_id);

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
                "user-1".into(),
                None,
                "key".into(),
                0,
                test_close(),
                test_tx(),
            )
            .unwrap_err();
        assert!(matches!(err, RegistryError::IdExhausted));
    }
}
