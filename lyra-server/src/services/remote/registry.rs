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
};

use super::constants::{
    MAX_CONNECTIONS_PER_USER,
    RemoteAction,
};
use super::messages::OutgoingMessage;

pub(crate) type ConnectionId = u64;

const COMMAND_CHANNEL_CAPACITY: usize = 16;

pub(crate) struct ConnectionHandle {
    pub(crate) connection_id: ConnectionId,
    pub(crate) token: String,
    pub(crate) user_db_id: DbId,
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
}

impl ConnectionRegistry {
    fn new() -> Self {
        Self {
            connections: HashMap::new(),
            tokens: HashMap::new(),
            next_id: 1,
        }
    }

    fn count_user_connections(&self, user_db_id: DbId) -> usize {
        self.connections
            .values()
            .filter(|h| h.user_db_id == user_db_id)
            .count()
    }

    /// The evicted connection's `unregister` call will block on the registry write
    /// lock until the caller's `register` (which holds the lock for both `evict_duplicate`
    /// and `insert`) releases it. This ordering is intentional and load-bearing.
    fn evict_duplicate(&mut self, user_db_id: DbId, session_key: &str) -> Option<ConnectionHandle> {
        let dup_id = self.connections.iter().find_map(|(&id, h)| {
            (h.user_db_id == user_db_id && h.session_key == session_key).then_some(id)
        });
        if let Some(id) = dup_id {
            let handle = self.connections.remove(&id)?;
            self.tokens.remove(&handle.token);
            handle.cancel.notify_one();
            Some(handle)
        } else {
            None
        }
    }

    fn insert(
        &mut self,
        user_db_id: DbId,
        session_key: String,
        cancel: Arc<Notify>,
        command_tx: mpsc::Sender<OutgoingMessage>,
    ) -> Result<ConnectionId, RegistryError> {
        let id = self
            .next_id
            .checked_add(1)
            .ok_or(RegistryError::IdExhausted)?;

        if self.count_user_connections(user_db_id) >= MAX_CONNECTIONS_PER_USER {
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
        Some(handle)
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
        .any(|h| h.user_db_id == user_db_id && h.session_key == session_key);
    let effective_count =
        registry.count_user_connections(user_db_id) - if has_duplicate { 1 } else { 0 };
    if effective_count >= MAX_CONNECTIONS_PER_USER {
        return Err(RegistryError::TooManyConnections);
    }

    let evicted = registry.evict_duplicate(user_db_id, &session_key);
    let (command_tx, command_rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
    let connection_id = registry.insert(user_db_id, session_key, cancel, command_tx)?;
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

#[allow(dead_code)]
pub(crate) async fn get_connection(connection_id: ConnectionId) -> Option<ConnectionSnapshot> {
    let registry = REGISTRY.read().await;
    registry
        .connections
        .get(&connection_id)
        .map(snapshot_from_handle)
}

pub(crate) async fn get_connection_pair(
    id_a: ConnectionId,
    id_b: ConnectionId,
) -> (Option<ConnectionSnapshot>, Option<ConnectionSnapshot>) {
    let registry = REGISTRY.read().await;
    let a = registry.connections.get(&id_a).map(snapshot_from_handle);
    let b = registry.connections.get(&id_b).map(snapshot_from_handle);
    (a, b)
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
        session_key: handle.session_key.clone(),
        supported_commands: handle.supported_commands.iter().cloned().collect(),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ConnectionSnapshot {
    pub(crate) connection_id: ConnectionId,
    pub(crate) token: String,
    pub(crate) user_db_id: DbId,
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
            .insert(DbId(1), "a".into(), test_cancel(), test_tx())
            .unwrap();
        let id2 = reg
            .insert(DbId(1), "b".into(), test_cancel(), test_tx())
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
            reg.insert(DbId(1), format!("key-{i}"), test_cancel(), test_tx())
                .unwrap();
        }
        let err = reg
            .insert(DbId(1), "overflow".into(), test_cancel(), test_tx())
            .unwrap_err();
        assert!(matches!(err, RegistryError::TooManyConnections));
    }

    #[test]
    fn insert_allows_different_users_independently() {
        let mut reg = test_registry();
        reg.insert(DbId(1), "a".into(), test_cancel(), test_tx())
            .unwrap();
        reg.insert(DbId(2), "a".into(), test_cancel(), test_tx())
            .unwrap();
        assert_eq!(reg.count_user_connections(DbId(1)), 1);
        assert_eq!(reg.count_user_connections(DbId(2)), 1);
    }

    #[test]
    fn evict_duplicate_removes_matching_connection_and_token() {
        let mut reg = test_registry();
        let id = reg
            .insert(DbId(1), "key".into(), test_cancel(), test_tx())
            .unwrap();
        let token = reg.connections[&id].token.clone();
        assert!(reg.tokens.contains_key(&token));
        let evicted = reg.evict_duplicate(DbId(1), "key");
        assert!(evicted.is_some());
        assert_eq!(reg.count_user_connections(DbId(1)), 0);
        assert!(!reg.tokens.contains_key(&token));
    }

    #[test]
    fn evict_duplicate_returns_none_when_no_match() {
        let mut reg = test_registry();
        reg.insert(DbId(1), "key".into(), test_cancel(), test_tx())
            .unwrap();
        let evicted = reg.evict_duplicate(DbId(1), "other-key");
        assert!(evicted.is_none());
        assert_eq!(reg.count_user_connections(DbId(1)), 1);
    }

    #[test]
    fn evict_duplicate_notifies_cancel() {
        let mut reg = test_registry();
        let cancel = test_cancel();
        let cancel_clone = cancel.clone();
        reg.insert(DbId(1), "key".into(), cancel, test_tx())
            .unwrap();

        reg.evict_duplicate(DbId(1), "key");

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
            .insert(DbId(1), "key".into(), test_cancel(), test_tx())
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
    fn id_exhaustion_detected() {
        let mut reg = test_registry();
        reg.next_id = u64::MAX;
        let err = reg
            .insert(DbId(1), "key".into(), test_cancel(), test_tx())
            .unwrap_err();
        assert!(matches!(err, RegistryError::IdExhausted));
    }
}
