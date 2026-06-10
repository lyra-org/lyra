// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashMap,
    ops::DerefMut,
};

use agdb::DbId;
use nanoid::nanoid;

use crate::{
    STATE,
    db::{
        self,
        Session,
    },
    services::auth::{
        hash_secret,
        random_hex_secret,
    },
};

pub(crate) const MAX_CLIENT_NAME_LEN: usize = 128;

#[derive(Clone, Debug, Default)]
pub(crate) struct SessionMetadata {
    pub(crate) user_agent: Option<String>,
    pub(crate) client_name: Option<String>,
}

impl SessionMetadata {
    fn normalized(self) -> Self {
        Self {
            user_agent: normalize_text(self.user_agent),
            client_name: normalize_text(self.client_name),
        }
    }
}

fn normalize_text(value: Option<String>) -> Option<String> {
    let trimmed = value?.trim().to_string();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed)
}

#[derive(Clone, Debug)]
pub(crate) struct CreatedSession {
    pub(crate) token: String,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SessionServiceError {
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

type SessionServiceResult<T> = Result<T, SessionServiceError>;

const SESSION_TOKEN_BYTES: usize = 16;
const LAST_SEEN_DEBOUNCE_SECS: i64 = 60;
const LAST_SEEN_SWEEP_THRESHOLD: usize = 1024;

// Debounce last_seen_at writes to avoid grabbing the DB write lock on every
// session-authed request. Only the in-memory mutex is taken on steady-state auth.
fn last_seen_cache() -> std::sync::MutexGuard<'static, HashMap<DbId, i64>> {
    STATE
        .auth_caches
        .session_last_seen
        .lock()
        .expect("session last_seen cache poisoned")
}

fn should_persist_last_seen(session_id: DbId, now: i64) -> bool {
    let cache = last_seen_cache();
    match cache.get(&session_id) {
        Some(&last) if now.saturating_sub(last) < LAST_SEEN_DEBOUNCE_SECS => false,
        _ => true,
    }
}

fn record_persisted_last_seen(session_id: DbId, now: i64) {
    let mut cache = last_seen_cache();
    if cache.len() >= LAST_SEEN_SWEEP_THRESHOLD {
        let stale_cutoff = 2 * LAST_SEEN_DEBOUNCE_SECS;
        cache.retain(|_, last| now.saturating_sub(*last) < stale_cutoff);
    }
    if cache.len() >= LAST_SEEN_SWEEP_THRESHOLD {
        let overflow = cache.len() + 1 - LAST_SEEN_SWEEP_THRESHOLD;
        let mut by_age: Vec<(DbId, i64)> = cache.iter().map(|(id, ts)| (*id, *ts)).collect();
        by_age.sort_by_key(|&(_, ts)| ts);
        for (id, _) in by_age.into_iter().take(overflow) {
            cache.remove(&id);
        }
    }
    cache.insert(session_id, now);
}

fn forget_last_seen(session_id: DbId) {
    last_seen_cache().remove(&session_id);
}

pub(crate) async fn create_session_for_user(
    user_db_id: DbId,
    metadata: SessionMetadata,
) -> SessionServiceResult<CreatedSession> {
    let metadata = metadata.normalized();
    let token = random_hex_secret::<SESSION_TOKEN_BYTES>();
    let token_hash = hash_secret(&token);
    let now = db::users::now_secs();
    let ttl = STATE.config.get().auth.session_ttl_seconds;
    let expires_at = if ttl > 0 {
        now.saturating_add(ttl as i64)
    } else {
        0
    };
    let session = Session {
        db_id: None,
        id: nanoid!(),
        token_hash,
        expires_at,
        created_at: now,
        last_seen_at: now,
        user_agent: metadata.user_agent,
        client_name: metadata.client_name,
    };
    let session_id = db::users::login(STATE.db.write().await.deref_mut(), user_db_id, &session)?;
    // Seed the cache so the next request inside the debounce window is a no-op.
    record_persisted_last_seen(session_id, now);

    Ok(CreatedSession { token })
}

pub(crate) async fn touch_last_seen(session_id: DbId) {
    let now = db::users::now_secs();
    if !should_persist_last_seen(session_id, now) {
        return;
    }

    let mut db_write = STATE.db.write().await;
    // Re-verify under the write lock: the session may have been revoked between
    // the cache check and acquiring the lock.
    let exists = match db::users::find_session_by_id(&*db_write, session_id) {
        Ok(session) => session.is_some(),
        Err(err) => {
            tracing::warn!(
                error = %err,
                session_id = ?session_id,
                "failed to verify session before last_seen update",
            );
            return;
        }
    };
    if !exists {
        drop(db_write);
        forget_last_seen(session_id);
        return;
    }

    match db::users::update_session_last_seen(&mut *db_write, session_id, now) {
        Ok(()) => {
            drop(db_write);
            record_persisted_last_seen(session_id, now);
        }
        Err(err) => {
            drop(db_write);
            tracing::warn!(
                error = %err,
                session_id = ?session_id,
                "failed to persist session last_seen_at; continuing with successful auth",
            );
        }
    }
}

pub(crate) async fn revoke_session_by_token(token: &str) -> SessionServiceResult<bool> {
    let mut db = STATE.db.write().await;
    let token_hash = hash_secret(token.trim());
    let session_id = db::users::find_by_session_token_hash(&*db, &token_hash)?
        .map(|(_, _, session_id)| session_id);
    let removed = db::users::revoke_session_by_token_hash(&mut db, &token_hash)?;
    drop(db);
    if removed {
        if let Some(session_id) = session_id {
            forget_last_seen(session_id);
        }
    }
    Ok(removed)
}
