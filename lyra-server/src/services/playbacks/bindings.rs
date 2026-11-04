// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Which track session each of Lyra's own client connections drives, by the connection's user and
//! session key. Plugin-reported playback has none: its playback's current session and the one that
//! session replaced are edges in the database.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        RwLock,
        atomic::{
            AtomicU64,
            Ordering,
        },
    },
};

use agdb::DbId;
use lyra_db::{
    EvictedPlayback,
    PlaybackSession,
};

use crate::services::playback_sessions;

#[derive(Clone, Debug)]
pub(crate) struct ConnectionBinding {
    pub(crate) current_playback_session_id: Option<DbId>,
    pub(crate) current_playback_session_public_id: Option<String>,
    /// The public id of the playback the current track session belongs to.
    pub(crate) current_playback_id: Option<String>,
    pub(crate) previous_playback_session_id: Option<DbId>,
    pub(crate) previous_playback_session_public_id: Option<String>,
    pub(crate) previous_demoted_at_ms: Option<u64>,
    pub(crate) previous_expires_at_ms: Option<u64>,
    pub(crate) updated_at_ms: u64,
    /// When the current play's live window ends, as last stored while it was live; the binding
    /// lapses then. An ended play keeps the window it had: clearing the session unbinds it, while
    /// the binding and its previous session's grace outlast it.
    pub(crate) live_until_ms: u64,
    /// Cleared when the current player reports an observation or its binding changes.
    /// The connection is degraded if a dispatched command exceeds the timeout.
    pub(crate) command_dispatched_at_ms: Option<u64>,
    /// Unique to each bind, across all bindings.
    pub(crate) current_binding_epoch: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct CurrentBindingSnapshot {
    pub(crate) playback_session_id: DbId,
    pub(crate) playback_session_public_id: String,
    pub(crate) epoch: u64,
}

pub(crate) struct ConnectionKey<'a> {
    pub(crate) user_public_id: &'a str,
    pub(crate) session_key: &'a str,
}

impl<'a> ConnectionKey<'a> {
    pub(crate) fn new(user_public_id: &'a str, session_key: &'a str) -> Self {
        Self {
            user_public_id,
            session_key,
        }
    }

    fn owned(&self) -> OwnedConnectionKey {
        // Lookups allocate; binding counts are bounded by active clients.
        OwnedConnectionKey {
            user_public_id: self.user_public_id.to_string(),
            session_key: self.session_key.to_string(),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct OwnedConnectionKey {
    user_public_id: String,
    session_key: String,
}

type BindingMap = HashMap<OwnedConnectionKey, ConnectionBinding>;

static NEXT_BINDING_EPOCH: AtomicU64 = AtomicU64::new(1);

/// Generation-owned connection bindings.
#[derive(Default)]
pub(crate) struct ConnectionBindings(Arc<RwLock<BindingMap>>);

/// What a binding shows of the connection with its session key: the playback it drives and when
/// a command it hasn't acknowledged was sent, from which `remote_control_degraded` follows.
#[derive(Debug, Default, PartialEq)]
struct ConnectionView {
    playback_id: Option<String>,
    command_dispatched_at_ms: Option<u64>,
}

fn connection_view(binding: Option<&ConnectionBinding>) -> ConnectionView {
    binding.map_or_else(ConnectionView::default, |binding| ConnectionView {
        playback_id: binding.current_playback_id.clone(),
        command_dispatched_at_ms: binding.command_dispatched_at_ms,
    })
}

/// Called with the bindings lock released.
fn notify_binding_changes(user_public_ids: impl IntoIterator<Item = String>) {
    let mut notified = Vec::<String>::new();
    for user_public_id in user_public_ids {
        if !notified.contains(&user_public_id) {
            playback_sessions::notify_connection_change(&user_public_id);
            notified.push(user_public_id);
        }
    }
}

/// Removes the bindings `remove` picks, returning how many went and the users whose connections
/// look different without them.
fn remove_where(
    bindings: &mut BindingMap,
    remove: impl Fn(&OwnedConnectionKey, &ConnectionBinding) -> bool,
) -> (usize, Vec<String>) {
    let removed = bindings
        .iter()
        .filter(|(key, binding)| remove(key, binding))
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    let mut changed = Vec::new();
    for key in &removed {
        let before = connection_view(bindings.remove(key).as_ref());
        if before != ConnectionView::default() {
            changed.push(key.user_public_id.clone());
        }
    }
    (removed.len(), changed)
}

/// Unbinds the current track session. A command sent to it goes unacknowledged by nothing once
/// the connection drives no playback, so it stops counting against the connection too.
fn clear_current_binding(binding: &mut ConnectionBinding) {
    binding.current_playback_session_id = None;
    binding.current_playback_session_public_id = None;
    binding.current_playback_id = None;
    binding.command_dispatched_at_ms = None;
}

fn bindings() -> Arc<RwLock<BindingMap>> {
    crate::services::STATE
        .generation()
        .connection_bindings
        .0
        .clone()
}

pub(crate) fn get(key: &ConnectionKey<'_>) -> Option<ConnectionBinding> {
    let handle = bindings();
    let bindings = handle.read().expect("connection bindings RwLock poisoned");
    bindings.get(&key.owned()).cloned()
}

/// Binds the connection's current track session, `session` stored as `playback_session_id`, a
/// session of the playback `playback_id`.
pub(crate) fn bind_current(
    key: &ConnectionKey<'_>,
    playback_id: &str,
    playback_session_id: DbId,
    session: &PlaybackSession,
    now_ms: u64,
) {
    let playback_session_public_id = session.id.clone();
    let key = key.owned();
    let handle = bindings();
    let mut bindings = handle.write().expect("connection bindings RwLock poisoned");
    let before = connection_view(bindings.get(&key));
    let entry = bindings
        .entry(key.clone())
        .or_insert_with(|| ConnectionBinding {
            current_playback_session_id: None,
            current_playback_session_public_id: None,
            current_playback_id: None,
            previous_playback_session_id: None,
            previous_playback_session_public_id: None,
            previous_demoted_at_ms: None,
            previous_expires_at_ms: None,
            updated_at_ms: now_ms,
            live_until_ms: session.live_until_ms,
            command_dispatched_at_ms: None,
            current_binding_epoch: 0,
        });

    entry.updated_at_ms = now_ms;
    entry.live_until_ms = session.live_until_ms;
    entry.command_dispatched_at_ms = None;
    entry.current_binding_epoch = NEXT_BINDING_EPOCH.fetch_add(1, Ordering::Relaxed);

    if entry.current_playback_session_id != Some(playback_session_id) {
        entry.previous_playback_session_id = entry.current_playback_session_id;
        entry.previous_playback_session_public_id =
            entry.current_playback_session_public_id.clone();
        entry.previous_demoted_at_ms = entry.current_playback_session_id.map(|_| now_ms);
        entry.previous_expires_at_ms = entry
            .current_playback_session_id
            .map(|_| now_ms.saturating_add(playback_sessions::PREVIOUS_PLAYBACK_GRACE_MS));
        entry.current_playback_session_id = Some(playback_session_id);
    }
    entry.current_playback_session_public_id = Some(playback_session_public_id);
    entry.current_playback_id = Some(playback_id.to_string());
    let after = connection_view(Some(entry));
    drop(bindings);
    if before != after {
        notify_binding_changes([key.user_public_id]);
    }
}

pub(crate) fn clear(key: &ConnectionKey<'_>) {
    let key = key.owned();
    let handle = bindings();
    let mut bindings = handle.write().expect("connection bindings RwLock poisoned");
    let before = connection_view(bindings.remove(&key).as_ref());
    drop(bindings);
    if before != ConnectionView::default() {
        notify_binding_changes([key.user_public_id]);
    }
}

pub(crate) fn clear_for_user(user_public_id: &str) {
    let handle = bindings();
    let mut bindings = handle.write().expect("connection bindings RwLock poisoned");
    let (_, changed) = remove_where(&mut bindings, |key, _| key.user_public_id == user_public_id);
    drop(bindings);
    notify_binding_changes(changed);
}

pub(crate) fn snapshot_current(key: &ConnectionKey<'_>) -> Option<CurrentBindingSnapshot> {
    let binding = get(key)?;
    Some(CurrentBindingSnapshot {
        playback_session_id: binding.current_playback_session_id?,
        playback_session_public_id: binding.current_playback_session_public_id?,
        epoch: binding.current_binding_epoch,
    })
}

pub(crate) fn clear_current_if_unchanged(
    key: &ConnectionKey<'_>,
    expected: &CurrentBindingSnapshot,
) -> bool {
    let key = key.owned();
    let handle = bindings();
    let mut bindings = handle.write().expect("connection bindings RwLock poisoned");
    let before = connection_view(bindings.get(&key));
    let Some(binding) = bindings.get_mut(&key) else {
        return false;
    };
    if binding.current_playback_session_id == Some(expected.playback_session_id)
        && binding.current_playback_session_public_id.as_deref()
            == Some(expected.playback_session_public_id.as_str())
        && binding.current_binding_epoch == expected.epoch
    {
        clear_current_binding(binding);
        if binding.previous_playback_session_id.is_none() {
            bindings.remove(&key);
        }
        let after = connection_view(bindings.get(&key));
        drop(bindings);
        if before != after {
            notify_binding_changes([key.user_public_id]);
        }
        return true;
    }
    false
}

/// Runs `f` unless a connection drives the track session, holding the bindings meanwhile.
pub(crate) fn with_no_current_binding_for_session<T>(
    playback_session_id: DbId,
    playback_session_public_id: &str,
    f: impl FnOnce() -> T,
) -> Option<T> {
    let handle = bindings();
    let bindings = handle.read().expect("connection bindings RwLock poisoned");
    if bindings.values().any(|binding| {
        binding.current_playback_session_id == Some(playback_session_id)
            && binding.current_playback_session_public_id.as_deref()
                == Some(playback_session_public_id)
    }) {
        None
    } else {
        Some(f())
    }
}

/// Unbinds the track session from every connection, as current or previous session.
pub(crate) fn clear_session(playback_session_id: DbId, playback_session_public_id: &str) -> usize {
    let handle = bindings();
    let mut bindings = handle.write().expect("connection bindings RwLock poisoned");
    let mut to_remove = Vec::new();
    let mut touched = Vec::new();

    for (key, binding) in bindings.iter_mut() {
        let current = binding.current_playback_session_id == Some(playback_session_id)
            && binding.current_playback_session_public_id.as_deref()
                == Some(playback_session_public_id);
        let previous = binding.previous_playback_session_id == Some(playback_session_id)
            && binding.previous_playback_session_public_id.as_deref()
                == Some(playback_session_public_id);
        if !current && !previous {
            continue;
        }
        touched.push((key.clone(), connection_view(Some(binding))));

        if current {
            clear_current_binding(binding);
        }
        if previous {
            binding.previous_playback_session_id = None;
            binding.previous_playback_session_public_id = None;
            binding.previous_demoted_at_ms = None;
            binding.previous_expires_at_ms = None;
        }
        if binding.current_playback_session_id.is_none()
            && binding.previous_playback_session_id.is_none()
        {
            to_remove.push(key.clone());
        }
    }

    for key in to_remove {
        bindings.remove(&key);
    }
    let changed = touched
        .iter()
        .filter(|(key, before)| *before != connection_view(bindings.get(key)))
        .map(|(key, _)| key.user_public_id.clone())
        .collect::<Vec<_>>();
    drop(bindings);
    notify_binding_changes(changed);

    touched.len()
}

pub(crate) fn clear_sessions(playbacks: &[EvictedPlayback]) -> usize {
    let mut removed = 0;
    for evicted in playbacks {
        let Some(playback_session_id) = evicted.playback.db_id else {
            continue;
        };
        removed += clear_session(playback_session_id, &evicted.playback.id);
    }
    removed
}

impl ConnectionBinding {
    fn is_live(&self, now_ms: u64) -> bool {
        now_ms <= self.live_until_ms
    }
}

/// Moves the live window of each binding whose current play is the session, stored as
/// `playback_session_id`, to the session's while the play is not ended.
pub(crate) fn follow_live_window(playback_session_id: DbId, session: &PlaybackSession) {
    if session.state.is_terminal() {
        return;
    }
    let handle = bindings();
    let mut bindings = handle.write().expect("connection bindings RwLock poisoned");
    for binding in bindings.values_mut() {
        if binding.current_playback_session_id == Some(playback_session_id) {
            binding.live_until_ms = session.live_until_ms;
        }
    }
}

/// Whether a connection whose bound play is live at `now_ms` holds the track session, as its
/// current session or as a previous one still in its grace.
pub(crate) fn holds_session(playback_session_id: DbId, now_ms: u64) -> bool {
    let handle = bindings();
    let bindings = handle.read().expect("connection bindings RwLock poisoned");
    bindings.values().any(|binding| {
        binding.is_live(now_ms)
            && (binding.current_playback_session_id == Some(playback_session_id)
                || (binding.previous_playback_session_id == Some(playback_session_id)
                    && binding
                        .previous_expires_at_ms
                        .is_none_or(|expires_at_ms| expires_at_ms >= now_ms)))
    })
}

/// The public ids of the playbacks a connection drives now.
pub(crate) fn driven_playback_ids() -> std::collections::HashSet<String> {
    let handle = bindings();
    let bindings = handle.read().expect("connection bindings RwLock poisoned");
    bindings
        .values()
        .filter_map(|binding| binding.current_playback_id.clone())
        .collect()
}

/// Removes the bindings whose bound play stopped being live before `now_ms`.
pub(crate) fn cleanup_stale(now_ms: u64) -> usize {
    let handle = bindings();
    let mut bindings = handle.write().expect("connection bindings RwLock poisoned");
    let (removed, changed) = remove_where(&mut bindings, |_, binding| !binding.is_live(now_ms));
    drop(bindings);
    notify_binding_changes(changed);
    removed
}

const COMMAND_DEGRADED_TIMEOUT_MS: u64 = 30_000;

/// No-op if the connection has no binding, e.g. a command targets a connection that hasn't
/// reported playback yet.
pub(crate) fn mark_command_dispatched(key: &ConnectionKey<'_>, now_ms: u64) {
    let key = key.owned();
    let handle = bindings();
    let mut bindings = handle.write().expect("connection bindings RwLock poisoned");
    let Some(binding) = bindings.get_mut(&key) else {
        return;
    };
    let before = connection_view(Some(binding));
    binding.command_dispatched_at_ms = Some(now_ms);
    let after = connection_view(Some(binding));
    drop(bindings);
    if before != after {
        notify_binding_changes([key.user_public_id]);
    }
}

#[cfg(test)]
pub(crate) fn is_remote_control_degraded(key: &ConnectionKey<'_>, now_ms: u64) -> bool {
    remote_control_degraded_at_ms(key).is_some_and(|degraded_at_ms| now_ms >= degraded_at_ms)
}

/// When the connection degrades unless the command sent to it is acknowledged first; `None` while
/// no command is pending.
pub(crate) fn remote_control_degraded_at_ms(key: &ConnectionKey<'_>) -> Option<u64> {
    let handle = bindings();
    let bindings = handle.read().expect("connection bindings RwLock poisoned");
    bindings
        .get(&key.owned())
        .and_then(|binding| binding.command_dispatched_at_ms)
        .map(|dispatched_at| dispatched_at.saturating_add(COMMAND_DEGRADED_TIMEOUT_MS))
}

#[cfg(test)]
/// A playing session, as a binding sees it, live until `live_until_ms`.
pub(crate) fn bound_session(public_id: &str, live_until_ms: u64) -> PlaybackSession {
    PlaybackSession {
        db_id: None,
        id: public_id.to_string(),
        play_id: public_id.to_string(),
        client_name: None,
        plugin_id: None,
        position_ms: 0,
        duration_ms: None,
        played_ms: 0,
        last_position_ms: 0,
        last_position_at_ms: 0,
        latency_allowance_used_ms: 0,
        rate_permille: playback_sessions::NORMAL_RATE_PERMILLE,
        counts_as_listen: true,
        state: lyra_db::PlaybackState::Playing,
        updated_at_ms: 0,
        live_until_ms,
        created_at_ms: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dispatched_command_degrades_the_connection_until_the_next_bind() {
        let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
        crate::services::testing::init_default_test_state().expect("init test state");
        let key = ConnectionKey::new("user", "device");
        bind_current(
            &key,
            "playback",
            DbId(1),
            &bound_session("session", 1_000),
            1_000,
        );
        assert!(!is_remote_control_degraded(&key, 100_000));

        mark_command_dispatched(&key, 10_000);
        assert!(!is_remote_control_degraded(&key, 10_000 + 29_000));
        assert!(is_remote_control_degraded(&key, 10_000 + 30_000));

        bind_current(
            &key,
            "playback",
            DbId(1),
            &bound_session("session", 50_000),
            50_000,
        );
        assert!(!is_remote_control_degraded(&key, 50_000 + 30_000));
    }

    #[test]
    fn rebinding_keeps_the_replaced_session_through_its_grace() {
        let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
        crate::services::testing::init_default_test_state().expect("init test state");
        let key = ConnectionKey::new("user", "device");
        bind_current(
            &key,
            "playback",
            DbId(1),
            &bound_session("first", 1_000),
            1_000,
        );
        bind_current(
            &key,
            "playback",
            DbId(2),
            &bound_session("second", 2_000),
            2_000,
        );

        let binding = get(&key).expect("binding");
        assert_eq!(binding.current_playback_session_id, Some(DbId(2)));
        assert_eq!(binding.previous_playback_session_id, Some(DbId(1)));
        assert_eq!(binding.previous_demoted_at_ms, Some(2_000));
        assert_eq!(
            binding.previous_expires_at_ms,
            Some(2_000 + playback_sessions::PREVIOUS_PLAYBACK_GRACE_MS)
        );
        assert!(holds_session(DbId(1), 2_000));
        assert!(
            !holds_session(DbId(1), 2_001),
            "a binding whose play is no longer live holds nothing"
        );
    }

    #[test]
    fn a_binding_follows_its_plays_live_window() {
        let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
        crate::services::testing::init_default_test_state().expect("init test state");
        let key = ConnectionKey::new("user", "device");
        let mut session = bound_session("session", 10_000);
        bind_current(&key, "playback", DbId(1), &session, 1_000);
        session.live_until_ms = 20_000;
        follow_live_window(DbId(2), &session);
        cleanup_stale(10_001);
        assert!(
            get(&key).is_none(),
            "another session's window moves nothing"
        );

        bind_current(
            &key,
            "playback",
            DbId(1),
            &bound_session("session", 10_000),
            1_000,
        );
        follow_live_window(DbId(1), &session);
        cleanup_stale(10_001);
        assert!(holds_session(DbId(1), 20_000));
        session.state = lyra_db::PlaybackState::Stopped;
        session.live_until_ms = 1_000;
        follow_live_window(DbId(1), &session);
        assert!(
            holds_session(DbId(1), 20_000),
            "an ended play leaves the window it had; clearing the session unbinds it"
        );
    }
}
