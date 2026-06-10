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

use agdb::{
    DbAny,
    DbId,
};

use crate::db::{
    self,
    EvictedPlayback,
    PlaybackSession,
};

#[derive(Clone, Debug)]
pub(crate) struct PlaybackSessionScope {
    pub(crate) current_playback_session_id: Option<DbId>,
    pub(crate) current_playback_session_public_id: Option<String>,
    pub(crate) previous_playback_session_id: Option<DbId>,
    pub(crate) previous_playback_session_public_id: Option<String>,
    pub(crate) previous_demoted_at_ms: Option<u64>,
    pub(crate) previous_expires_at_ms: Option<u64>,
    pub(crate) updated_at_ms: u64,
    /// Set on command dispatch, cleared on any scope upsert (including
    /// non-state-changing reports). Scope is degraded if this exceeds the timeout.
    pub(crate) command_dispatched_at_ms: Option<u64>,
}

pub(crate) struct PlaybackScopeKey<'a> {
    pub(crate) plugin_id: &'a str,
    pub(crate) user_db_id: DbId,
    pub(crate) session_key: &'a str,
}

impl PlaybackScopeKey<'_> {
    fn owned(&self) -> OwnedPlaybackScopeKey {
        // Lookups allocate; scope counts are bounded by active clients and plugins.
        OwnedPlaybackScopeKey {
            plugin_id: self.plugin_id.to_string(),
            user_db_id: self.user_db_id,
            session_key: self.session_key.to_string(),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct OwnedPlaybackScopeKey {
    pub(crate) plugin_id: String,
    pub(crate) user_db_id: DbId,
    pub(crate) session_key: String,
}

impl OwnedPlaybackScopeKey {
    pub(crate) fn as_borrowed(&self) -> PlaybackScopeKey<'_> {
        PlaybackScopeKey {
            plugin_id: &self.plugin_id,
            user_db_id: self.user_db_id,
            session_key: &self.session_key,
        }
    }
}

/// Generation-owned playback session scopes keyed by (plugin, user, session).
#[derive(Default)]
pub(crate) struct PlaybackScopes(Arc<RwLock<HashMap<OwnedPlaybackScopeKey, PlaybackSessionScope>>>);

fn playback_scopes() -> Arc<RwLock<HashMap<OwnedPlaybackScopeKey, PlaybackSessionScope>>> {
    crate::STATE.generation().playback_scopes.0.clone()
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
    user_db_id: DbId,
    session_key: &str,
) -> Vec<(OwnedPlaybackScopeKey, PlaybackSessionScope)> {
    let scopes_handle = playback_scopes();
    let scopes = scopes_handle
        .read()
        .expect("playback session scopes RwLock poisoned");
    scopes
        .iter()
        .filter_map(|(key, scope)| {
            (key.user_db_id == user_db_id && key.session_key == session_key)
                .then(|| (key.clone(), scope.clone()))
        })
        .collect()
}

pub(crate) fn upsert_playback_session(
    scope: &PlaybackScopeKey<'_>,
    now_ms: u64,
) -> PlaybackSessionScope {
    let key = scope.owned();
    let scopes_handle = playback_scopes();
    let mut scopes = scopes_handle
        .write()
        .expect("playback session scopes RwLock poisoned");
    let entry = scopes
        .entry(key)
        .and_modify(|s| {
            s.updated_at_ms = now_ms;
            s.command_dispatched_at_ms = None;
        })
        .or_insert_with(|| PlaybackSessionScope {
            current_playback_session_id: None,
            current_playback_session_public_id: None,
            previous_playback_session_id: None,
            previous_playback_session_public_id: None,
            previous_demoted_at_ms: None,
            previous_expires_at_ms: None,
            updated_at_ms: now_ms,
            command_dispatched_at_ms: None,
        });
    entry.clone()
}

pub(crate) fn bind_current_playback_session_scope(
    scope: &PlaybackScopeKey<'_>,
    playback_session_id: DbId,
    playback_session_public_id: String,
    now_ms: u64,
) {
    let key = scope.owned();
    let scopes_handle = playback_scopes();
    let mut scopes = scopes_handle
        .write()
        .expect("playback session scopes RwLock poisoned");
    let entry = scopes.entry(key).or_insert_with(|| PlaybackSessionScope {
        current_playback_session_id: None,
        current_playback_session_public_id: None,
        previous_playback_session_id: None,
        previous_playback_session_public_id: None,
        previous_demoted_at_ms: None,
        previous_expires_at_ms: None,
        updated_at_ms: now_ms,
        command_dispatched_at_ms: None,
    });

    entry.updated_at_ms = now_ms;
    entry.command_dispatched_at_ms = None;

    if entry.current_playback_session_id == Some(playback_session_id) {
        entry.current_playback_session_public_id = Some(playback_session_public_id);
        return;
    }

    entry.previous_playback_session_id = entry.current_playback_session_id;
    entry.previous_playback_session_public_id = entry.current_playback_session_public_id.clone();
    entry.previous_demoted_at_ms = entry.current_playback_session_id.map(|_| now_ms);
    entry.previous_expires_at_ms = entry
        .current_playback_session_id
        .map(|_| now_ms.saturating_add(super::PREVIOUS_PLAYBACK_GRACE_MS));
    entry.current_playback_session_id = Some(playback_session_id);
    entry.current_playback_session_public_id = Some(playback_session_public_id);
}

pub(crate) fn update_playback_session(
    scope: &PlaybackScopeKey<'_>,
    session: &PlaybackSessionScope,
) {
    let key = scope.owned();
    let scopes_handle = playback_scopes();
    let mut scopes = scopes_handle
        .write()
        .expect("playback session scopes RwLock poisoned");
    if let Some(existing) = scopes.get_mut(&key) {
        *existing = session.clone();
    }
}

pub(crate) struct BoundPlayback {
    pub(crate) playback_session_id: DbId,
    pub(crate) track_db_id: DbId,
    pub(crate) playback: PlaybackSession,
}

fn resolve_playback_by_id_and_public_id(
    db: &DbAny,
    playback_session_id: DbId,
    public_id: Option<&str>,
) -> anyhow::Result<Option<BoundPlayback>> {
    let Some(playback) = db::playback_sessions::get_by_id(db, playback_session_id)? else {
        return Ok(None);
    };
    if let Some(public_id) = public_id
        && playback.id != public_id
    {
        return Ok(None);
    }
    let Some(track_db_id) = db::playback_sessions::get_track_id(db, playback_session_id)? else {
        return Ok(None);
    };

    Ok(Some(BoundPlayback {
        playback_session_id,
        track_db_id,
        playback,
    }))
}

pub(crate) fn resolve_current_playback(
    db: &DbAny,
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
    db: &DbAny,
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
    scopes.remove(&key);
}

pub(crate) fn clear_session_bindings_for_playback(
    playback_session_id: DbId,
    playback_session_public_id: &str,
) -> usize {
    let scopes_handle = playback_scopes();
    let mut scopes = scopes_handle
        .write()
        .expect("playback session scopes RwLock poisoned");
    let mut cleared = 0usize;
    let mut to_remove = Vec::new();

    for (key, scope) in scopes.iter_mut() {
        let mut binding_changed = false;

        if scope.current_playback_session_id == Some(playback_session_id)
            && scope.current_playback_session_public_id.as_deref()
                == Some(playback_session_public_id)
        {
            scope.current_playback_session_id = None;
            scope.current_playback_session_public_id = None;
            binding_changed = true;
        }
        if scope.previous_playback_session_id == Some(playback_session_id)
            && scope.previous_playback_session_public_id.as_deref()
                == Some(playback_session_public_id)
        {
            scope.previous_playback_session_id = None;
            scope.previous_playback_session_public_id = None;
            scope.previous_demoted_at_ms = None;
            scope.previous_expires_at_ms = None;
            binding_changed = true;
        }
        if !binding_changed {
            continue;
        }

        cleared += 1;
        if scope.current_playback_session_id.is_none()
            && scope.previous_playback_session_id.is_none()
        {
            to_remove.push(key.clone());
        }
    }

    for key in to_remove {
        scopes.remove(&key);
    }

    cleared
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
                .is_none_or(|expires_at| expires_at >= active_cutoff_ms)
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
    let before = scopes.len();
    scopes.retain(|_, scope| scope.updated_at_ms >= cutoff_ms);
    before - scopes.len()
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
    if let Some(session) = scopes.get_mut(&key) {
        session.command_dispatched_at_ms = Some(now_ms);
    }
}

pub(crate) fn is_remote_control_degraded(scope: &PlaybackScopeKey<'_>, now_ms: u64) -> bool {
    let key = scope.owned();
    let scopes_handle = playback_scopes();
    let scopes = scopes_handle
        .read()
        .expect("playback session scopes RwLock poisoned");
    scopes
        .get(&key)
        .and_then(|s| s.command_dispatched_at_ms)
        .is_some_and(|dispatched_at| {
            now_ms.saturating_sub(dispatched_at) >= COMMAND_DEGRADED_TIMEOUT_MS
        })
}
