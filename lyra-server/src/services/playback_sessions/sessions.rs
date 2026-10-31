// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        RwLock,
    },
};

use agdb::DbId;

use lyra_db::{
    EvictedPlayback,
    PlaybackSession,
};

#[derive(Clone, Debug)]
pub(crate) struct PlaybackSessionScope {
    pub(crate) current_playback_session_id: Option<DbId>,
    pub(crate) current_playback_session_public_id: Option<String>,
    /// The public id of the playback the current track session belongs to.
    pub(crate) current_playback_id: Option<String>,
    pub(crate) previous_playback_session_id: Option<DbId>,
    pub(crate) previous_playback_session_public_id: Option<String>,
    pub(crate) previous_demoted_at_ms: Option<u64>,
    pub(crate) previous_expires_at_ms: Option<u64>,
    pub(crate) updated_at_ms: u64,
    /// Cleared when the current player reports an observation or its binding changes.
    /// Scope is degraded if a dispatched command exceeds the timeout.
    pub(crate) command_dispatched_at_ms: Option<u64>,
    pub(crate) current_binding_epoch: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct CurrentBindingSnapshot {
    pub(crate) playback_session_id: DbId,
    pub(crate) playback_session_public_id: String,
    pub(crate) epoch: u64,
}

pub(crate) struct PlaybackScopeKey<'a> {
    pub(crate) plugin_id: &'a str,
    pub(crate) user_public_id: &'a str,
    pub(crate) session_key: &'a str,
}

/// The plugin id of scopes reported by Lyra's own clients, keyed by their connection session key.
pub(crate) const NATIVE_PLUGIN_ID: &str = "native";

impl<'a> PlaybackScopeKey<'a> {
    pub(crate) fn native(user_public_id: &'a str, session_key: &'a str) -> Self {
        Self {
            plugin_id: NATIVE_PLUGIN_ID,
            user_public_id,
            session_key,
        }
    }
}

impl PlaybackScopeKey<'_> {
    fn owned(&self) -> OwnedPlaybackScopeKey {
        // Lookups allocate; scope counts are bounded by active clients and plugins.
        OwnedPlaybackScopeKey {
            plugin_id: self.plugin_id.to_string(),
            user_public_id: self.user_public_id.to_string(),
            session_key: self.session_key.to_string(),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct OwnedPlaybackScopeKey {
    pub(crate) plugin_id: String,
    pub(crate) user_public_id: String,
    pub(crate) session_key: String,
}

impl OwnedPlaybackScopeKey {
    pub(crate) fn as_borrowed(&self) -> PlaybackScopeKey<'_> {
        PlaybackScopeKey {
            plugin_id: &self.plugin_id,
            user_public_id: &self.user_public_id,
            session_key: &self.session_key,
        }
    }
}

/// Generation-owned playback session scopes keyed by (plugin, user, session).
#[derive(Default)]
pub(crate) struct PlaybackScopes(Arc<RwLock<HashMap<OwnedPlaybackScopeKey, PlaybackSessionScope>>>);

/// What a native scope shows of the connection with its session key: the playback it drives and
/// when a command it hasn't acknowledged was sent, from which `remote_control_degraded` follows.
#[derive(Debug, Default, PartialEq)]
struct ConnectionView {
    playback_id: Option<String>,
    command_dispatched_at_ms: Option<u64>,
}

fn connection_view(scope: Option<&PlaybackSessionScope>) -> ConnectionView {
    scope.map_or_else(ConnectionView::default, |scope| ConnectionView {
        playback_id: scope.current_playback_id.clone(),
        command_dispatched_at_ms: scope.command_dispatched_at_ms,
    })
}

/// Whether a change to a scope changes how a connection looks, which only native scopes back.
fn connection_view_changed(
    key: &OwnedPlaybackScopeKey,
    before: &ConnectionView,
    after: &ConnectionView,
) -> bool {
    key.plugin_id == NATIVE_PLUGIN_ID && before != after
}

/// Called with the scopes lock released.
fn notify_binding_changes(user_public_ids: impl IntoIterator<Item = String>) {
    let mut notified = Vec::<String>::new();
    for user_public_id in user_public_ids {
        if !notified.contains(&user_public_id) {
            super::updates::notify_connection_change(&user_public_id);
            notified.push(user_public_id);
        }
    }
}

/// Removes the scopes `remove` picks, returning how many went and the users whose connections
/// look different without them.
fn remove_scopes_where(
    scopes: &mut HashMap<OwnedPlaybackScopeKey, PlaybackSessionScope>,
    remove: impl Fn(&OwnedPlaybackScopeKey, &PlaybackSessionScope) -> bool,
) -> (usize, Vec<String>) {
    let removed = scopes
        .iter()
        .filter(|(key, scope)| remove(key, scope))
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    let mut changed = Vec::new();
    for key in &removed {
        let before = connection_view(scopes.remove(key).as_ref());
        if connection_view_changed(key, &before, &ConnectionView::default()) {
            changed.push(key.user_public_id.clone());
        }
    }
    (removed.len(), changed)
}

/// Unbinds the scope's current track session. A command sent to it goes unacknowledged by
/// nothing once the scope drives no playback, so it stops counting against the connection too.
fn clear_current_binding(scope: &mut PlaybackSessionScope) {
    scope.current_playback_session_id = None;
    scope.current_playback_session_public_id = None;
    scope.current_playback_id = None;
    scope.command_dispatched_at_ms = None;
}

fn playback_scopes() -> Arc<RwLock<HashMap<OwnedPlaybackScopeKey, PlaybackSessionScope>>> {
    crate::services::STATE
        .generation()
        .playback_scopes
        .0
        .clone()
}

pub(crate) fn get_playback_session(scope: &PlaybackScopeKey<'_>) -> Option<PlaybackSessionScope> {
    let key = scope.owned();
    let scopes_handle = playback_scopes();
    let scopes = scopes_handle
        .read()
        .expect("playback session scopes RwLock poisoned");
    scopes.get(&key).cloned()
}

pub(crate) fn get_playback_sessions_for_user_session(
    user_public_id: &str,
    session_key: &str,
) -> Vec<(OwnedPlaybackScopeKey, PlaybackSessionScope)> {
    let scopes_handle = playback_scopes();
    let scopes = scopes_handle
        .read()
        .expect("playback session scopes RwLock poisoned");
    scopes
        .iter()
        .filter(|(key, _)| key.user_public_id == user_public_id && key.session_key == session_key)
        .map(|(key, scope)| (key.clone(), scope.clone()))
        .collect()
}

pub(crate) fn apply_reported_scope(
    scope: &PlaybackScopeKey<'_>,
    mut session: PlaybackSessionScope,
    current_observed: bool,
) {
    let key = scope.owned();
    let scopes_handle = playback_scopes();
    let mut scopes = scopes_handle
        .write()
        .expect("playback session scopes RwLock poisoned");
    let before = connection_view(scopes.get(&key));
    if let Some(previous) = scopes.get(&key) {
        if current_observed {
            session.current_binding_epoch = previous.current_binding_epoch.wrapping_add(1);
        } else if previous.current_playback_session_public_id
            == session.current_playback_session_public_id
        {
            session.current_binding_epoch = previous.current_binding_epoch;
            session.command_dispatched_at_ms = previous.command_dispatched_at_ms;
        }
    }
    let after = connection_view(Some(&session));
    scopes.insert(key.clone(), session);
    drop(scopes);
    if connection_view_changed(&key, &before, &after) {
        notify_binding_changes([key.user_public_id]);
    }
}

/// Binds the scope's current track session, a session of the playback `playback_id`.
pub(crate) fn bind_current_playback_session_scope(
    scope: &PlaybackScopeKey<'_>,
    playback_id: &str,
    playback_session_id: DbId,
    playback_session_public_id: String,
    now_ms: u64,
) {
    let key = scope.owned();
    let scopes_handle = playback_scopes();
    let mut scopes = scopes_handle
        .write()
        .expect("playback session scopes RwLock poisoned");
    let before = connection_view(scopes.get(&key));
    let entry = scopes
        .entry(key.clone())
        .or_insert_with(|| PlaybackSessionScope {
            current_playback_session_id: None,
            current_playback_session_public_id: None,
            current_playback_id: None,
            previous_playback_session_id: None,
            previous_playback_session_public_id: None,
            previous_demoted_at_ms: None,
            previous_expires_at_ms: None,
            updated_at_ms: now_ms,
            command_dispatched_at_ms: None,
            current_binding_epoch: 0,
        });

    entry.updated_at_ms = now_ms;
    entry.command_dispatched_at_ms = None;
    entry.current_binding_epoch = entry.current_binding_epoch.wrapping_add(1);

    if entry.current_playback_session_id != Some(playback_session_id) {
        entry.previous_playback_session_id = entry.current_playback_session_id;
        entry.previous_playback_session_public_id =
            entry.current_playback_session_public_id.clone();
        entry.previous_demoted_at_ms = entry.current_playback_session_id.map(|_| now_ms);
        entry.previous_expires_at_ms = entry
            .current_playback_session_id
            .map(|_| now_ms.saturating_add(super::PREVIOUS_PLAYBACK_GRACE_MS));
        entry.current_playback_session_id = Some(playback_session_id);
    }
    entry.current_playback_session_public_id = Some(playback_session_public_id);
    entry.current_playback_id = Some(playback_id.to_string());
    let after = connection_view(Some(entry));
    drop(scopes);
    if connection_view_changed(&key, &before, &after) {
        notify_binding_changes([key.user_public_id]);
    }
}

pub(crate) struct BoundPlayback {
    pub(crate) playback_session_id: DbId,
    pub(crate) track_db_id: DbId,
    pub(crate) playback: PlaybackSession,
}

fn resolve_playback_by_id_and_public_id(
    db: &impl lyra_db::DbAccess,
    playback_session_id: DbId,
    public_id: Option<&str>,
) -> anyhow::Result<Option<BoundPlayback>> {
    let Some(playback) = lyra_db::playback_sessions::get_by_id(db, playback_session_id)? else {
        return Ok(None);
    };
    if let Some(public_id) = public_id
        && playback.id != public_id
    {
        return Ok(None);
    }
    let Some(track_db_id) = lyra_db::playback_sessions::get_track_id(db, playback_session_id)?
    else {
        return Ok(None);
    };

    Ok(Some(BoundPlayback {
        playback_session_id,
        track_db_id,
        playback,
    }))
}

pub(crate) fn resolve_current_playback(
    db: &impl lyra_db::DbAccess,
    session: &PlaybackSessionScope,
) -> anyhow::Result<Option<BoundPlayback>> {
    let Some(playback_session_id) = session.current_playback_session_id else {
        return Ok(None);
    };
    resolve_playback_by_id_and_public_id(
        db,
        playback_session_id,
        session.current_playback_session_public_id.as_deref(),
    )
}

pub(crate) fn resolve_previous_playback(
    db: &impl lyra_db::DbAccess,
    session: &PlaybackSessionScope,
) -> anyhow::Result<Option<BoundPlayback>> {
    let Some(playback_session_id) = session.previous_playback_session_id else {
        return Ok(None);
    };
    resolve_playback_by_id_and_public_id(
        db,
        playback_session_id,
        session.previous_playback_session_public_id.as_deref(),
    )
}

pub(crate) fn clear_playback_session_scope(scope: &PlaybackScopeKey<'_>) {
    let key = scope.owned();
    let scopes_handle = playback_scopes();
    let mut scopes = scopes_handle
        .write()
        .expect("playback session scopes RwLock poisoned");
    let before = connection_view(scopes.remove(&key).as_ref());
    drop(scopes);
    if connection_view_changed(&key, &before, &ConnectionView::default()) {
        notify_binding_changes([key.user_public_id]);
    }
}

pub(crate) fn clear_playback_scopes_for_user(user_public_id: &str) {
    let scopes_handle = playback_scopes();
    let mut scopes = scopes_handle
        .write()
        .expect("playback session scopes RwLock poisoned");
    let (_, changed) =
        remove_scopes_where(&mut scopes, |key, _| key.user_public_id == user_public_id);
    drop(scopes);
    notify_binding_changes(changed);
}

pub(crate) fn snapshot_current_binding(
    scope_key: &PlaybackScopeKey<'_>,
) -> Option<CurrentBindingSnapshot> {
    let scope = get_playback_session(scope_key)?;
    Some(CurrentBindingSnapshot {
        playback_session_id: scope.current_playback_session_id?,
        playback_session_public_id: scope.current_playback_session_public_id?,
        epoch: scope.current_binding_epoch,
    })
}

pub(crate) fn clear_current_binding_if_unchanged(
    scope_key: &PlaybackScopeKey<'_>,
    expected: &CurrentBindingSnapshot,
) -> bool {
    let key = scope_key.owned();
    let scopes_handle = playback_scopes();
    let mut scopes = scopes_handle
        .write()
        .expect("playback session scopes RwLock poisoned");
    let before = connection_view(scopes.get(&key));
    let Some(scope) = scopes.get_mut(&key) else {
        return false;
    };
    if scope.current_playback_session_id == Some(expected.playback_session_id)
        && scope.current_playback_session_public_id.as_deref()
            == Some(expected.playback_session_public_id.as_str())
        && scope.current_binding_epoch == expected.epoch
    {
        clear_current_binding(scope);
        if scope.previous_playback_session_id.is_none() {
            scopes.remove(&key);
        }
        let after = connection_view(scopes.get(&key));
        drop(scopes);
        if connection_view_changed(&key, &before, &after) {
            notify_binding_changes([key.user_public_id]);
        }
        return true;
    }
    false
}

pub(crate) fn with_no_current_binding_for_playback<T>(
    playback_session_id: DbId,
    playback_session_public_id: &str,
    f: impl FnOnce() -> T,
) -> Option<T> {
    let scopes_handle = playback_scopes();
    let scopes = scopes_handle
        .read()
        .expect("playback session scopes RwLock poisoned");
    if scopes.values().any(|scope| {
        scope.current_playback_session_id == Some(playback_session_id)
            && scope.current_playback_session_public_id.as_deref()
                == Some(playback_session_public_id)
    }) {
        None
    } else {
        Some(f())
    }
}

pub(crate) fn clear_session_bindings_for_playback(
    playback_session_id: DbId,
    playback_session_public_id: &str,
) -> usize {
    let scopes_handle = playback_scopes();
    let mut scopes = scopes_handle
        .write()
        .expect("playback session scopes RwLock poisoned");
    let mut to_remove = Vec::new();
    let mut touched = Vec::new();

    for (key, scope) in scopes.iter_mut() {
        let current = scope.current_playback_session_id == Some(playback_session_id)
            && scope.current_playback_session_public_id.as_deref()
                == Some(playback_session_public_id);
        let previous = scope.previous_playback_session_id == Some(playback_session_id)
            && scope.previous_playback_session_public_id.as_deref()
                == Some(playback_session_public_id);
        if !current && !previous {
            continue;
        }
        touched.push((key.clone(), connection_view(Some(scope))));

        if current {
            clear_current_binding(scope);
        }
        if previous {
            scope.previous_playback_session_id = None;
            scope.previous_playback_session_public_id = None;
            scope.previous_demoted_at_ms = None;
            scope.previous_expires_at_ms = None;
        }
        if scope.current_playback_session_id.is_none()
            && scope.previous_playback_session_id.is_none()
        {
            to_remove.push(key.clone());
        }
    }

    for key in to_remove {
        scopes.remove(&key);
    }
    let changed = touched
        .iter()
        .filter(|(key, before)| {
            connection_view_changed(key, before, &connection_view(scopes.get(key)))
        })
        .map(|(key, _)| key.user_public_id.clone())
        .collect::<Vec<_>>();
    drop(scopes);
    notify_binding_changes(changed);

    touched.len()
}

pub(crate) fn clear_session_bindings_for_playbacks(playbacks: &[EvictedPlayback]) -> usize {
    let mut removed = 0;
    for evicted in playbacks {
        let Some(playback_session_id) = evicted.playback.db_id else {
            continue;
        };
        removed += clear_session_bindings_for_playback(playback_session_id, &evicted.playback.id);
    }
    removed
}

pub(crate) fn has_active_scope_for_playback(
    playback_session_id: DbId,
    active_cutoff_ms: u64,
) -> bool {
    let scopes_handle = playback_scopes();
    let scopes = scopes_handle
        .read()
        .expect("playback session scopes RwLock poisoned");
    for scope in scopes.values() {
        if scope.updated_at_ms < active_cutoff_ms {
            continue;
        }
        if scope.current_playback_session_id == Some(playback_session_id) {
            return true;
        }
        if scope.previous_playback_session_id == Some(playback_session_id)
            && scope
                .previous_expires_at_ms
                .is_none_or(|expires_at_ms| expires_at_ms >= active_cutoff_ms)
        {
            return true;
        }
    }
    false
}

pub(crate) fn cleanup_stale_scopes(now_ms: u64, stale_ttl_ms: u64) -> usize {
    let cutoff_ms = now_ms.saturating_sub(stale_ttl_ms);
    let scopes_handle = playback_scopes();
    let mut scopes = scopes_handle
        .write()
        .expect("playback session scopes RwLock poisoned");
    let (removed, changed) =
        remove_scopes_where(&mut scopes, |_, scope| scope.updated_at_ms < cutoff_ms);
    drop(scopes);
    notify_binding_changes(changed);
    removed
}

const COMMAND_DEGRADED_TIMEOUT_MS: u64 = 30_000;

/// No-op if the scope doesn't exist (e.g. command targets a connection
/// that hasn't reported playback yet).
pub(crate) fn mark_command_dispatched(scope: &PlaybackScopeKey<'_>, now_ms: u64) {
    let key = scope.owned();
    let scopes_handle = playback_scopes();
    let mut scopes = scopes_handle
        .write()
        .expect("playback session scopes RwLock poisoned");
    let Some(session) = scopes.get_mut(&key) else {
        return;
    };
    let before = connection_view(Some(session));
    session.command_dispatched_at_ms = Some(now_ms);
    let after = connection_view(Some(session));
    drop(scopes);
    if connection_view_changed(&key, &before, &after) {
        notify_binding_changes([key.user_public_id]);
    }
}

#[cfg(test)]
pub(crate) fn is_remote_control_degraded(scope: &PlaybackScopeKey<'_>, now_ms: u64) -> bool {
    remote_control_degraded_at_ms(scope).is_some_and(|degraded_at_ms| now_ms >= degraded_at_ms)
}

/// When the scope degrades unless the command sent to it is acknowledged first; `None` while no
/// command is pending.
pub(crate) fn remote_control_degraded_at_ms(scope: &PlaybackScopeKey<'_>) -> Option<u64> {
    let key = scope.owned();
    let scopes_handle = playback_scopes();
    let scopes = scopes_handle
        .read()
        .expect("playback session scopes RwLock poisoned");
    scopes
        .get(&key)
        .and_then(|s| s.command_dispatched_at_ms)
        .map(|dispatched_at| dispatched_at.saturating_add(COMMAND_DEGRADED_TIMEOUT_MS))
}
