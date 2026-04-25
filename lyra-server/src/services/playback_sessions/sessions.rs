// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        LazyLock,
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
    pub(crate) previous_playback_session_id: Option<DbId>,
    pub(crate) previous_expires_at_ms: Option<u64>,
    pub(crate) updated_at_ms: u64,
    /// Set on command dispatch, cleared on any scope upsert (including
    /// non-state-changing reports). Scope is degraded if this exceeds the timeout.
    pub(crate) command_dispatched_at_ms: Option<u64>,
}

static PLAYBACK_SESSION_SCOPES: LazyLock<Arc<RwLock<HashMap<String, PlaybackSessionScope>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));

fn playback_session_scope_alias(plugin_id: &str, user_db_id: DbId, session_key: &str) -> String {
    let user_id_text = user_db_id.0.to_string();
    format!(
        "playback_session_scope|{}:{}|{}:{}|{}:{}",
        plugin_id.len(),
        plugin_id,
        user_id_text.len(),
        user_id_text,
        session_key.len(),
        session_key
    )
}

pub(crate) struct PlaybackScopeKey<'a> {
    pub(crate) plugin_id: &'a str,
    pub(crate) user_db_id: DbId,
    pub(crate) session_key: &'a str,
}

impl PlaybackScopeKey<'_> {
    fn alias(&self) -> String {
        playback_session_scope_alias(self.plugin_id, self.user_db_id, self.session_key)
    }
}

pub(crate) fn get_playback_session(scope: &PlaybackScopeKey<'_>) -> Option<PlaybackSessionScope> {
    let alias = scope.alias();
    let scopes = PLAYBACK_SESSION_SCOPES
        .read()
        .expect("playback session scopes RwLock poisoned");
    scopes.get(&alias).cloned()
}

pub(crate) fn upsert_playback_session(
    scope: &PlaybackScopeKey<'_>,
    now_ms: u64,
) -> PlaybackSessionScope {
    let alias = scope.alias();
    let mut scopes = PLAYBACK_SESSION_SCOPES
        .write()
        .expect("playback session scopes RwLock poisoned");
    let entry = scopes
        .entry(alias)
        .and_modify(|s| {
            s.updated_at_ms = now_ms;
            s.command_dispatched_at_ms = None;
        })
        .or_insert_with(|| PlaybackSessionScope {
            current_playback_session_id: None,
            previous_playback_session_id: None,
            previous_expires_at_ms: None,
            updated_at_ms: now_ms,
            command_dispatched_at_ms: None,
        });
    entry.clone()
}

pub(crate) fn update_playback_session(
    scope: &PlaybackScopeKey<'_>,
    session: &PlaybackSessionScope,
) {
    let alias = scope.alias();
    let mut scopes = PLAYBACK_SESSION_SCOPES
        .write()
        .expect("playback session scopes RwLock poisoned");
    if let Some(existing) = scopes.get_mut(&alias) {
        *existing = session.clone();
    }
}

pub(crate) struct BoundPlayback {
    pub(crate) playback_session_id: DbId,
    pub(crate) track_db_id: DbId,
    pub(crate) playback: PlaybackSession,
}

fn resolve_playback_by_id(
    db: &DbAny,
    playback_session_id: DbId,
) -> anyhow::Result<Option<BoundPlayback>> {
    let Some(playback) = db::playback_sessions::get_by_id(db, playback_session_id)? else {
        return Ok(None);
    };
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
    resolve_playback_by_id(db, playback_session_id)
}

pub(crate) fn resolve_previous_playback(
    db: &DbAny,
    session: &PlaybackSessionScope,
) -> anyhow::Result<Option<BoundPlayback>> {
    let Some(playback_session_id) = session.previous_playback_session_id else {
        return Ok(None);
    };
    resolve_playback_by_id(db, playback_session_id)
}

pub(crate) fn clear_playback_session_scope(scope: &PlaybackScopeKey<'_>) {
    let alias = scope.alias();
    let mut scopes = PLAYBACK_SESSION_SCOPES
        .write()
        .expect("playback session scopes RwLock poisoned");
    scopes.remove(&alias);
}

pub(crate) fn clear_session_bindings_for_playback(playback_session_id: DbId) -> usize {
    let mut scopes = PLAYBACK_SESSION_SCOPES
        .write()
        .expect("playback session scopes RwLock poisoned");
    let mut cleared = 0usize;
    let mut to_remove = Vec::new();

    for (alias, scope) in scopes.iter_mut() {
        let mut binding_changed = false;

        if scope.current_playback_session_id == Some(playback_session_id) {
            scope.current_playback_session_id = None;
            binding_changed = true;
        }
        if scope.previous_playback_session_id == Some(playback_session_id) {
            scope.previous_playback_session_id = None;
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
            to_remove.push(alias.clone());
        }
    }

    for alias in to_remove {
        scopes.remove(&alias);
    }

    cleared
}

pub(crate) fn clear_session_bindings_for_playbacks(playbacks: &[EvictedPlayback]) -> usize {
    let mut removed = 0;
    for evicted in playbacks {
        let Some(playback_session_id) = evicted.playback.db_id else {
            continue;
        };
        removed += clear_session_bindings_for_playback(playback_session_id);
    }
    removed
}

pub(crate) fn has_active_scope_for_playback(
    playback_session_id: DbId,
    active_cutoff_ms: u64,
) -> bool {
    let scopes = PLAYBACK_SESSION_SCOPES
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
    let mut scopes = PLAYBACK_SESSION_SCOPES
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
    let alias = scope.alias();
    let mut scopes = PLAYBACK_SESSION_SCOPES
        .write()
        .expect("playback session scopes RwLock poisoned");
    if let Some(session) = scopes.get_mut(&alias) {
        session.command_dispatched_at_ms = Some(now_ms);
    }
}

pub(crate) fn is_remote_control_degraded(scope: &PlaybackScopeKey<'_>, now_ms: u64) -> bool {
    let alias = scope.alias();
    let scopes = PLAYBACK_SESSION_SCOPES
        .read()
        .expect("playback session scopes RwLock poisoned");
    scopes
        .get(&alias)
        .and_then(|s| s.command_dispatched_at_ms)
        .is_some_and(|dispatched_at| {
            now_ms.saturating_sub(dispatched_at) >= COMMAND_DEGRADED_TIMEOUT_MS
        })
}

pub(crate) fn clear_all_scopes_for_test() {
    let mut scopes = PLAYBACK_SESSION_SCOPES
        .write()
        .expect("playback session scopes RwLock poisoned (test)");
    scopes.clear();
}
