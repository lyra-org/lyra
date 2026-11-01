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
use unicode_normalization::UnicodeNormalization;
use unicode_properties::{
    GeneralCategory,
    UnicodeGeneralCategory,
};

use crate::{
    services::STATE,
    services::auth::{
        AuthError,
        Principal,
        hash_secret,
        owners::{
            self,
            Owners,
        },
        random_hex_secret,
    },
};
use lyra_db::{
    Permission,
    Session,
};

const MAX_CLIENT_NAME_LEN: usize = 128;

#[derive(Clone, Debug, Default)]
pub(crate) struct SessionMetadata {
    pub(crate) user_agent: Option<String>,
    pub(crate) client_name: Option<String>,
}

impl SessionMetadata {
    fn normalized(self) -> Result<Self, ClientNameError> {
        Ok(Self {
            user_agent: normalize_text(self.user_agent),
            client_name: normalize_client_name(self.client_name)?,
        })
    }
}

fn normalize_text(value: Option<String>) -> Option<String> {
    let trimmed = value?.trim().to_string();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed)
}

/// U+034F CGJ is not `Cf`, but it also blocks NFC canonicalization.
fn is_invisible_strippable(c: char) -> bool {
    c == '\u{034F}' || c.general_category() == GeneralCategory::Format
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum ClientNameError {
    #[error("client_name cannot contain control characters")]
    ContainsControl,
    #[error("client_name cannot exceed {MAX_CLIENT_NAME_LEN} characters")]
    TooLong,
}

/// Strips the characters `remove` selects, trims Unicode whitespace, and NFC-composes.
/// Stripping precedes trimming because format controls are not whitespace.
fn canonical_client_name(client_name: &str, remove: impl Fn(char) -> bool) -> String {
    let kept: String = client_name.chars().filter(|c| !remove(*c)).collect();
    kept.trim_matches(char::is_whitespace).nfc().collect()
}

/// Canonicalize an optional client device label by stripping Unicode format controls and
/// CGJ, trimming Unicode whitespace, and NFC-composing. Rejects control characters and
/// labels over [`MAX_CLIENT_NAME_LEN`]; a label that normalizes away entirely becomes
/// `None`.
pub(crate) fn normalize_client_name(
    value: Option<String>,
) -> Result<Option<String>, ClientNameError> {
    let Some(client_name) = value else {
        return Ok(None);
    };

    let client_name = canonical_client_name(&client_name, is_invisible_strippable);
    if client_name.is_empty() {
        return Ok(None);
    }
    if client_name.chars().any(char::is_control) {
        return Err(ClientNameError::ContainsControl);
    }
    if client_name.chars().count() > MAX_CLIENT_NAME_LEN {
        return Err(ClientNameError::TooLong);
    }
    Ok(Some(client_name))
}

/// Canonicalizes a client label that comes from outside Lyra's own clients, such as a
/// protocol header, as [`normalize_client_name`] does, but strips control characters and
/// truncates to [`MAX_CLIENT_NAME_LEN`] instead of rejecting the label.
pub(crate) fn tidy_client_name(value: Option<String>) -> Option<String> {
    let client_name =
        canonical_client_name(&value?, |c| c.is_control() || is_invisible_strippable(c));
    let truncated: String = client_name.chars().take(MAX_CLIENT_NAME_LEN).collect();
    let client_name = truncated.trim_end_matches(char::is_whitespace);
    (!client_name.is_empty()).then(|| client_name.to_string())
}

#[derive(Clone, Debug)]
pub(crate) struct CreatedSession {
    pub(crate) token: String,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SessionServiceError {
    #[error(transparent)]
    InvalidClientName(#[from] ClientNameError),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

type SessionServiceResult<T> = Result<T, SessionServiceError>;

const SESSION_TOKEN_BYTES: usize = 16;
const LAST_SEEN_DEBOUNCE_MS: i64 = 60_000;
const LAST_SEEN_SWEEP_THRESHOLD: usize = 1024;

// Debounce last_seen_at_ms writes to avoid grabbing the DB write lock on every
// session-authed request. Only the in-memory mutex is taken on steady-state auth.
fn last_seen_cache(
    generation: &crate::services::state::GenerationState,
) -> std::sync::MutexGuard<'_, HashMap<String, i64>> {
    generation
        .auth_caches
        .session_last_seen
        .lock()
        .expect("session last_seen cache poisoned")
}

fn should_persist_last_seen(session_public_id: &str, now: i64) -> bool {
    let generation = STATE.generation();
    let cache = last_seen_cache(&generation);
    !matches!(
        cache.get(session_public_id),
        Some(&last) if now.saturating_sub(last) < LAST_SEEN_DEBOUNCE_MS
    )
}

fn record_persisted_last_seen(session_public_id: &str, now: i64) {
    let generation = STATE.generation();
    let mut cache = last_seen_cache(&generation);
    if cache.len() >= LAST_SEEN_SWEEP_THRESHOLD {
        let stale_cutoff = 2 * LAST_SEEN_DEBOUNCE_MS;
        cache.retain(|_, last| now.saturating_sub(*last) < stale_cutoff);
    }
    if cache.len() >= LAST_SEEN_SWEEP_THRESHOLD {
        let overflow = cache.len() + 1 - LAST_SEEN_SWEEP_THRESHOLD;
        let mut by_age: Vec<(String, i64)> =
            cache.iter().map(|(id, ts)| (id.clone(), *ts)).collect();
        by_age.sort_by_key(|(_, ts)| *ts);
        for (id, _) in by_age.into_iter().take(overflow) {
            cache.remove(&id);
        }
    }
    cache.insert(session_public_id.to_string(), now);
}

fn forget_last_seen(session_public_id: &str) {
    let generation = STATE.generation();
    last_seen_cache(&generation).remove(session_public_id);
}

/// Creates a session for the principal's user, verifying the user under the write lock that
/// inserts the session.
pub(crate) async fn create_session_for_user(
    principal: &Principal,
    metadata: SessionMetadata,
) -> Result<CreatedSession, AuthError> {
    let metadata = metadata.normalized()?;
    let token = random_hex_secret::<SESSION_TOKEN_BYTES>();
    let token_hash = hash_secret(&token);
    let now = lyra_db::users::now_ms();
    let ttl = STATE.config().auth.session_ttl_seconds;
    let expires_at_ms = if ttl > 0 {
        now.saturating_add((ttl as i64).saturating_mul(1000))
    } else {
        0
    };
    let session = Session {
        db_id: None,
        id: nanoid!(),
        token_hash,
        expires_at_ms,
        created_at_ms: now,
        last_seen_at_ms: now,
        user_agent: metadata.user_agent,
        client_name: metadata.client_name,
    };
    let mut db_write = STATE.db.write().await;
    let user_db_id = principal.require(&db_write)?;
    lyra_db::users::login(db_write.deref_mut(), user_db_id, &session)?;
    drop(db_write);
    // Seed the cache so the next request inside the debounce window is a no-op.
    record_persisted_last_seen(&session.id, now);

    Ok(CreatedSession { token })
}

pub(crate) async fn touch_last_seen(session_public_id: &str) {
    let now = lyra_db::users::now_ms();
    if !should_persist_last_seen(session_public_id, now) {
        return;
    }

    let mut db_write = STATE.db.write_outside_catalog().await;
    // Resolve under the write lock: the session may have been revoked since auth.
    let session_id = match lyra_db::users::find_session_by_public_id(&*db_write, session_public_id)
    {
        Ok(Some((session_id, _))) => session_id,
        Ok(None) => {
            drop(db_write);
            forget_last_seen(session_public_id);
            return;
        }
        Err(err) => {
            tracing::warn!(
                error = %err,
                session_public_id,
                "failed to verify session before last_seen update",
            );
            return;
        }
    };

    match lyra_db::users::update_session_last_seen(&mut *db_write, session_id, now) {
        Ok(()) => {
            drop(db_write);
            record_persisted_last_seen(session_public_id, now);
        }
        Err(err) => {
            drop(db_write);
            tracing::warn!(
                error = %err,
                session_public_id,
                "failed to persist session last_seen_at_ms; continuing with successful auth",
            );
        }
    }
}

pub(crate) fn is_expired(session: &Session) -> bool {
    session.expires_at_ms > 0 && lyra_db::users::now_ms() >= session.expires_at_ms
}

/// Revokes the session behind `token_hash` if it has expired, resolving it under the write lock
/// that removes it.
pub(super) async fn revoke_expired_session(token_hash: &str) -> anyhow::Result<bool> {
    let mut db = STATE.db.write().await;
    let Some((_, session, session_id)) =
        lyra_db::users::find_by_session_token_hash(&*db, token_hash)?
    else {
        return Ok(false);
    };
    if !is_expired(&session) {
        return Ok(false);
    }
    let removed = lyra_db::users::revoke_session_by_id(&mut db, session_id)?;
    drop(db);
    if removed {
        forget_last_seen(&session.id);
    }
    Ok(removed)
}

pub(crate) async fn revoke_session_by_token(token: &str) -> SessionServiceResult<bool> {
    let mut db = STATE.db.write().await;
    let token_hash = hash_secret(token.trim());
    let session_public_id = lyra_db::users::find_by_session_token_hash(&*db, &token_hash)?
        .map(|(_, session, _)| session.id);
    let removed = lyra_db::users::revoke_session_by_token_hash(&mut db, &token_hash)?;
    drop(db);
    if removed && let Some(session_public_id) = session_public_id {
        forget_last_seen(&session_public_id);
    }
    Ok(removed)
}

#[derive(Clone, Debug)]
pub(crate) struct SessionInfo {
    pub(crate) id: String,
    pub(crate) user_id: String,
    pub(crate) username: String,
    pub(crate) created_at_ms: i64,
    pub(crate) last_seen_at_ms: i64,
    pub(crate) client_name: Option<String>,
    pub(crate) user_agent: Option<String>,
    /// The listing plugin's own data on the session.
    pub(crate) plugin_data: serde_json::Value,
}

/// The live sessions of `owners`, oldest first, each with `plugin_id`'s data on it.
pub(crate) async fn list_sessions(
    principal: &Principal,
    owners: Owners<'_>,
    plugin_id: &str,
) -> Result<Vec<SessionInfo>, AuthError> {
    let db = STATE.db.read().await;
    let principal_db_id = principal.require(&*db)?;
    let users = owners::resolve(&*db, principal, principal_db_id, owners)?;

    let mut sessions = Vec::new();
    for user in users {
        let Some(user_db_id) = user.db_id else {
            continue;
        };
        for session in lyra_db::users::sessions_for_user(&*db, user_db_id)? {
            let Some(session_id) = session.db_id.clone().map(DbId::from) else {
                continue;
            };
            if is_expired(&session) {
                continue;
            }
            let plugin_data = parse_plugin_data(lyra_db::users::session_plugin_data(
                &*db, session_id, plugin_id,
            )?)?;
            sessions.push(SessionInfo {
                id: session.id,
                user_id: user.id.clone(),
                username: user.username.clone(),
                created_at_ms: session.created_at_ms,
                last_seen_at_ms: session.last_seen_at_ms,
                client_name: session.client_name,
                user_agent: session.user_agent,
                plugin_data,
            });
        }
    }
    sessions.sort_by(|a, b| {
        a.created_at_ms
            .cmp(&b.created_at_ms)
            .then_with(|| a.id.cmp(&b.id))
    });
    Ok(sessions)
}

/// Revokes the session with public id `session_public_id`. A session the principal does not
/// manage, per [`manages_sessions_of`], is reported as absent.
pub(crate) async fn revoke_session(
    principal: &Principal,
    session_public_id: &str,
) -> Result<bool, AuthError> {
    let mut db = STATE.db.write().await;
    let principal_db_id = principal.require(&*db)?;
    let Some((session_id, _)) = lyra_db::users::find_session_by_public_id(&*db, session_public_id)?
    else {
        return Ok(false);
    };
    let owner_db_id = lyra_db::users::session_owner(&*db, session_id)?.and_then(|user| user.db_id);
    let manages = match owner_db_id {
        Some(owner_db_id) => owners::manages(&*db, principal, principal_db_id, owner_db_id)?,
        None => lyra_db::roles::has_permission(&principal.permissions, Permission::ManageUsers),
    };
    if !manages {
        return Ok(false);
    }
    let removed = lyra_db::users::revoke_session_by_id(&mut db, session_id)?;
    drop(db);
    if removed {
        forget_last_seen(session_public_id);
    }
    Ok(removed)
}

pub(crate) const MAX_PLUGIN_SESSION_DATA_BYTES: usize = 16 * 1024;

fn parse_plugin_data(value: Option<String>) -> anyhow::Result<serde_json::Value> {
    value
        .map(|value| serde_json::from_str(&value).map_err(Into::into))
        .transpose()
        .map(|value| value.unwrap_or(serde_json::Value::Null))
}

fn live_session_id(db: &impl lyra_db::DbAccess, token: &str) -> anyhow::Result<DbId> {
    let (_, session, session_id) =
        lyra_db::users::find_by_session_token_hash(db, &hash_secret(token.trim()))?
            .ok_or_else(|| anyhow::anyhow!("invalid session token"))?;
    if is_expired(&session) {
        anyhow::bail!("session expired");
    }
    Ok(session_id)
}

pub(crate) async fn get_plugin_data(
    token: &str,
    plugin_id: &str,
) -> anyhow::Result<serde_json::Value> {
    let db = STATE.db.read().await;
    let session_id = live_session_id(&*db, token)?;
    parse_plugin_data(lyra_db::users::session_plugin_data(
        &*db, session_id, plugin_id,
    )?)
}

pub(crate) async fn set_plugin_data(
    token: &str,
    plugin_id: &str,
    value: serde_json::Value,
) -> anyhow::Result<()> {
    let value = if value.is_null() {
        None
    } else {
        Some(serde_json::to_string(&value)?)
    };
    anyhow::ensure!(
        value
            .as_ref()
            .is_none_or(|value| value.len() <= MAX_PLUGIN_SESSION_DATA_BYTES),
        "session data exceeds 16 KiB"
    );
    let mut db = STATE.db.write().await;
    let session_id = live_session_id(&*db, token)?;
    lyra_db::users::set_session_plugin_data(&mut *db, session_id, plugin_id, value.as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn plugin_session_data_is_scoped_bounded_and_revoked_with_session() -> anyhow::Result<()>
    {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let user_id = {
            let mut db = STATE.db.write().await;
            lyra_db::users::create(&mut db, &lyra_db::fixtures::test_user("session-data")?)?
        };
        let token = crate::services::testing::create_session(user_id, SessionMetadata::default())
            .await?
            .token;
        let other_token =
            crate::services::testing::create_session(user_id, SessionMetadata::default())
                .await?
                .token;
        let data = serde_json::json!({"device_id": "phone"});
        assert_eq!(
            get_plugin_data(&token, "external").await?,
            serde_json::Value::Null
        );
        set_plugin_data(&token, "external", data.clone()).await?;
        assert_eq!(get_plugin_data(&token, "external").await?, data);
        assert!(get_plugin_data(&other_token, "external").await?.is_null());
        set_plugin_data(&other_token, "external", data.clone()).await?;
        {
            let mut db = STATE.db.write().await;
            let session_id = live_session_id(&*db, &other_token)?;
            db.exec_mut(
                agdb::QueryBuilder::insert()
                    .values_uniform([("expires_at_ms", lyra_db::users::now_ms() - 1).into()])
                    .ids(session_id)
                    .query(),
            )?;
        }
        assert!(get_plugin_data(&other_token, "external").await.is_err());
        assert!(
            set_plugin_data(&other_token, "external", data.clone())
                .await
                .is_err()
        );
        assert_eq!(
            get_plugin_data(&token, "other").await?,
            serde_json::Value::Null
        );
        set_plugin_data(&token, "other", serde_json::json!({"device_id": "tablet"})).await?;
        set_plugin_data(&token, "external", serde_json::Value::Null).await?;
        assert_eq!(
            get_plugin_data(&token, "external").await?,
            serde_json::Value::Null
        );
        assert!(!get_plugin_data(&token, "other").await?.is_null());
        assert!(
            set_plugin_data(
                &token,
                "external",
                serde_json::json!("x".repeat(MAX_PLUGIN_SESSION_DATA_BYTES))
            )
            .await
            .is_err()
        );
        assert!(revoke_session_by_token(&token).await?);
        assert!(get_plugin_data(&token, "other").await.is_err());
        assert!(set_plugin_data(&token, "other", data).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn listed_sessions_carry_only_the_listing_plugins_data_and_skip_expired()
    -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let user_id = {
            let mut db = STATE.db.write().await;
            lyra_db::users::create(&mut db, &lyra_db::fixtures::test_user("session-list")?)?
        };
        let principal = crate::services::testing::user_principal(user_id).await?;
        let token = crate::services::testing::create_session(user_id, SessionMetadata::default())
            .await?
            .token;
        let expired_token =
            crate::services::testing::create_session(user_id, SessionMetadata::default())
                .await?
                .token;
        set_plugin_data(
            &token,
            "external",
            serde_json::json!({"device_id": "phone"}),
        )
        .await?;
        {
            let mut db = STATE.db.write().await;
            let session_id = live_session_id(&*db, &expired_token)?;
            db.exec_mut(
                agdb::QueryBuilder::insert()
                    .values_uniform([("expires_at_ms", lyra_db::users::now_ms() - 1).into()])
                    .ids(session_id)
                    .query(),
            )?;
        }

        let external = list_sessions(&principal, Owners::Principal, "external").await?;
        assert_eq!(external.len(), 1);
        assert_eq!(
            external[0].plugin_data,
            serde_json::json!({"device_id": "phone"})
        );
        let other = list_sessions(&principal, Owners::Principal, "other").await?;
        assert_eq!(other.len(), 1);
        assert!(other[0].plugin_data.is_null());
        Ok(())
    }

    #[test]
    fn normalize_client_name_strips_invisibles_and_trims() {
        assert_eq!(
            normalize_client_name(Some("  Lyra\u{200B} Web  ".to_string())).unwrap(),
            Some("Lyra Web".to_string())
        );
    }

    #[test]
    fn normalize_client_name_discards_names_that_normalize_away() {
        assert_eq!(
            normalize_client_name(Some(" \u{200B} ".to_string())).unwrap(),
            None
        );
    }

    #[test]
    fn normalize_client_name_rejects_control_characters() {
        assert_eq!(
            normalize_client_name(Some("Lyra\nWeb".to_string())),
            Err(ClientNameError::ContainsControl)
        );
    }

    #[test]
    fn normalize_client_name_rejects_oversized_values() {
        assert_eq!(
            normalize_client_name(Some("x".repeat(MAX_CLIENT_NAME_LEN + 1))),
            Err(ClientNameError::TooLong)
        );
    }

    #[test]
    fn tidy_client_name_strips_controls_and_invisibles() {
        assert_eq!(
            tidy_client_name(Some(" Pixel\n\u{85}\u{200B} 8\t".to_string())),
            Some("Pixel 8".to_string())
        );
    }

    #[test]
    fn tidy_client_name_composes_and_truncates() {
        let long = format!("{} tail", "e\u{301}".repeat(MAX_CLIENT_NAME_LEN - 1));
        let tidied = tidy_client_name(Some(long)).unwrap();
        assert_eq!(tidied, "\u{e9}".repeat(MAX_CLIENT_NAME_LEN - 1));
        assert_eq!(
            normalize_client_name(Some(tidied.clone())),
            Ok(Some(tidied))
        );
    }

    #[test]
    fn tidy_client_name_discards_names_that_tidy_away() {
        assert_eq!(tidy_client_name(Some("\n\u{200B} ".to_string())), None);
        assert_eq!(tidy_client_name(None), None);
    }
}
