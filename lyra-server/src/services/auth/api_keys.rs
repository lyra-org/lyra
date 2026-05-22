// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashMap,
    sync::{
        LazyLock,
        Mutex,
    },
};

use agdb::DbId;

use crate::{
    STATE,
    db,
    services::auth::{
        hash_secret,
        random_hex_secret,
    },
};

const API_KEY_BYTES: usize = 16;
pub(crate) const MAX_API_KEYS_PER_USER: usize = 50;
pub(crate) use db::api_keys::MAX_NAME_LEN as MAX_API_KEY_NAME_LEN;
const LAST_USED_DEBOUNCE_SECS: i64 = 60;
const LAST_USED_SWEEP_THRESHOLD: usize = 1024;

// Debounce last_used_at writes to avoid grabbing the DB write lock on every
// api-key-authed request. Only the in-memory mutex is taken on steady-state auth.
static LAST_USED_CACHE: LazyLock<Mutex<HashMap<DbId, i64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn last_used_cache() -> std::sync::MutexGuard<'static, HashMap<DbId, i64>> {
    LAST_USED_CACHE
        .lock()
        .expect("api key last_used cache poisoned")
}

fn should_persist_last_used(api_key_id: DbId, now: i64) -> bool {
    let cache = last_used_cache();
    match cache.get(&api_key_id) {
        Some(&last) if now.saturating_sub(last) < LAST_USED_DEBOUNCE_SECS => false,
        _ => true,
    }
}

fn record_persisted_last_used(api_key_id: DbId, now: i64) {
    let mut cache = last_used_cache();
    if cache.len() >= LAST_USED_SWEEP_THRESHOLD {
        let stale_cutoff = 2 * LAST_USED_DEBOUNCE_SECS;
        cache.retain(|_, last| now.saturating_sub(*last) < stale_cutoff);
    }
    // Age-based retain may free nothing if all entries are fresh; hard-evict oldest to cap.
    if cache.len() >= LAST_USED_SWEEP_THRESHOLD {
        let overflow = cache.len() + 1 - LAST_USED_SWEEP_THRESHOLD;
        let mut by_age: Vec<(DbId, i64)> = cache.iter().map(|(id, ts)| (*id, *ts)).collect();
        by_age.sort_by_key(|&(_, ts)| ts);
        for (id, _) in by_age.into_iter().take(overflow) {
            cache.remove(&id);
        }
    }
    cache.insert(api_key_id, now);
}

fn forget_last_used(api_key_id: DbId) {
    last_used_cache().remove(&api_key_id);
}

pub(crate) fn forget_last_used_many(ids: impl IntoIterator<Item = DbId>) {
    let mut cache = last_used_cache();
    for id in ids {
        cache.remove(&id);
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ApiKeyServiceError {
    #[error("{0}")]
    BadRequest(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

type ApiKeyResult<T> = std::result::Result<T, ApiKeyServiceError>;

#[derive(Clone, Debug)]
pub(crate) struct CreatedApiKey {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) key: String,
    pub(crate) created_at: i64,
    pub(crate) last_used_at: Option<i64>,
}

#[derive(Clone, Debug)]
pub(crate) struct ApiKeyInfo {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) created_at: i64,
    pub(crate) last_used_at: Option<i64>,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedApiKey {
    pub(crate) api_key_id: DbId,
    pub(crate) name: String,
    pub(crate) user_db_id: DbId,
}

fn to_api_key_info(api_key: db::api_keys::ApiKey) -> ApiKeyInfo {
    assert!(
        api_key.db_id.is_some(),
        "persisted api key must have db_id set"
    );
    ApiKeyInfo {
        id: api_key.id,
        name: api_key.name,
        created_at: api_key.created_at,
        last_used_at: api_key.last_used_at,
    }
}

pub(crate) async fn create_api_key(user_db_id: DbId, name: &str) -> ApiKeyResult<CreatedApiKey> {
    let name = name.trim();
    if name.is_empty() {
        return Err(ApiKeyServiceError::BadRequest(
            "api key name cannot be empty".into(),
        ));
    }
    if name.chars().count() > MAX_API_KEY_NAME_LEN {
        return Err(ApiKeyServiceError::BadRequest(format!(
            "api key name cannot exceed {MAX_API_KEY_NAME_LEN} characters"
        )));
    }

    let key = random_hex_secret::<API_KEY_BYTES>();
    let key_hash = hash_secret(&key);
    let created_at = db::users::now_secs();

    let mut db_write = STATE.db.write().await;
    if db::api_keys::count_for_user(&db_write, user_db_id)? >= MAX_API_KEYS_PER_USER {
        return Err(ApiKeyServiceError::BadRequest(format!(
            "api key limit reached ({MAX_API_KEYS_PER_USER} per user); revoke an existing key first"
        )));
    }

    let api_key_id = db::api_keys::create(&mut db_write, user_db_id, name, &key_hash, created_at)?;
    let api_key = db::api_keys::get_by_id(&db_write, api_key_id)?
        .ok_or_else(|| anyhow::anyhow!("created api key could not be read"))?;

    Ok(CreatedApiKey {
        id: api_key.id,
        name: api_key.name,
        key,
        created_at: api_key.created_at,
        last_used_at: api_key.last_used_at,
    })
}

pub(crate) async fn list_api_keys_for_user(user_db_id: DbId) -> anyhow::Result<Vec<ApiKeyInfo>> {
    let db_read = STATE.db.read().await;
    Ok(db::api_keys::list_for_user(&db_read, user_db_id)?
        .into_iter()
        .map(to_api_key_info)
        .collect())
}

pub(crate) async fn revoke_api_key_for_user(user_db_id: DbId, id: &str) -> anyhow::Result<bool> {
    let mut db_write = STATE.db.write().await;
    let Some(api_key) = db::api_keys::get_by_public_id(&db_write, id)? else {
        return Ok(false);
    };
    let Some(api_key_id) = api_key.db_id.map(Into::into) else {
        return Ok(false);
    };
    if db::api_keys::get_owner_id(&db_write, api_key_id)? != Some(user_db_id) {
        return Ok(false);
    }

    let deleted = db::api_keys::delete_by_id(&mut db_write, api_key_id)?;
    if deleted {
        forget_last_used(api_key_id);
    }
    Ok(deleted)
}

pub(crate) async fn resolve_api_key(key: &str) -> anyhow::Result<Option<ResolvedApiKey>> {
    let key = key.trim();
    if key.is_empty() {
        return Ok(None);
    }

    let key_hash = hash_secret(key);
    let db_read = STATE.db.read().await;
    let Some(api_key) = db::api_keys::find_by_key_hash(&db_read, &key_hash)? else {
        return Ok(None);
    };
    let Some(api_key_id) = api_key.db_id.map(Into::into) else {
        return Ok(None);
    };
    let Some(user_db_id) = db::api_keys::get_owner_id(&db_read, api_key_id)? else {
        return Ok(None);
    };
    let name = api_key.name;
    drop(db_read);

    let now = db::users::now_secs();
    if should_persist_last_used(api_key_id, now) {
        let mut db_write = STATE.db.write().await;
        // Re-verify under write lock: key may have been revoked between read and write.
        let fresh_id = match db::api_keys::find_by_key_hash(&db_write, &key_hash)? {
            Some(fresh) => fresh.db_id.map(Into::into),
            None => None,
        };
        if fresh_id != Some(api_key_id) {
            drop(db_write);
            forget_last_used(api_key_id);
            return Ok(None);
        }
        match db::api_keys::update_last_used_at(&mut db_write, api_key_id, now) {
            Ok(()) => {
                drop(db_write);
                record_persisted_last_used(api_key_id, now);
            }
            Err(err) => {
                drop(db_write);
                tracing::warn!(
                    error = %err,
                    api_key_id = ?api_key_id,
                    "failed to persist api key last_used_at; continuing with successful auth",
                );
            }
        }
    }

    Ok(Some(ResolvedApiKey {
        api_key_id,
        name,
        user_db_id,
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex as StdMutex,
        MutexGuard as StdMutexGuard,
    };

    use super::*;

    static CACHE_TEST_LOCK: StdMutex<()> = StdMutex::new(());

    fn cache_test_guard() -> StdMutexGuard<'static, ()> {
        CACHE_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    async fn initialize_api_key_test_runtime() -> anyhow::Result<()> {
        {
            let _guard = cache_test_guard();
            last_used_cache().clear();
        }
        crate::testing::initialize_runtime(&crate::testing::LibraryFixtureConfig {
            directory: std::path::PathBuf::from("."),
            language: None,
            country: None,
        })
        .await
    }

    #[tokio::test]
    async fn create_api_key_rejects_empty_name() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        initialize_api_key_test_runtime().await?;

        let user_db_id = {
            let mut db = STATE.db.write().await;
            db::users::create(&mut db, &db::users::test_user("alice")?)?
        };

        let err = create_api_key(user_db_id, "   ")
            .await
            .expect_err("empty name should be rejected");
        assert!(
            matches!(err, ApiKeyServiceError::BadRequest(_)),
            "expected bad request, got {err:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn create_api_key_rejects_names_exceeding_max_length() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        initialize_api_key_test_runtime().await?;

        let user_db_id = {
            let mut db = STATE.db.write().await;
            db::users::create(&mut db, &db::users::test_user("alice")?)?
        };

        let long_name: String = "x".repeat(MAX_API_KEY_NAME_LEN + 1);
        let err = create_api_key(user_db_id, &long_name)
            .await
            .expect_err("overlong name should be rejected");
        assert!(
            matches!(err, ApiKeyServiceError::BadRequest(_)),
            "expected bad request, got {err:?}"
        );

        let boundary: String = "y".repeat(MAX_API_KEY_NAME_LEN);
        create_api_key(user_db_id, &boundary)
            .await
            .expect("boundary-length name should succeed");

        Ok(())
    }

    #[tokio::test]
    async fn create_api_key_enforces_per_user_cap() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        initialize_api_key_test_runtime().await?;

        let user_db_id = {
            let mut db = STATE.db.write().await;
            db::users::create(&mut db, &db::users::test_user("alice")?)?
        };

        for i in 0..MAX_API_KEYS_PER_USER {
            create_api_key(user_db_id, &format!("key-{i}")).await?;
        }

        let err = create_api_key(user_db_id, "one-too-many")
            .await
            .expect_err("per-user cap should be enforced");
        assert!(
            matches!(err, ApiKeyServiceError::BadRequest(_)),
            "expected bad request, got {err:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn resolve_api_key_updates_last_used_at() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        initialize_api_key_test_runtime().await?;

        let user_db_id = {
            let mut db = STATE.db.write().await;
            db::users::create(&mut db, &db::users::test_user("alice")?)?
        };
        let created = create_api_key(user_db_id, "laptop").await?;

        assert_eq!(
            list_api_keys_for_user(user_db_id).await?[0].last_used_at,
            None
        );

        let resolved = resolve_api_key(&created.key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("api key should resolve"))?;
        assert_eq!(resolved.user_db_id, user_db_id);

        let listed = list_api_keys_for_user(user_db_id).await?;
        assert!(listed[0].last_used_at.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn resolve_api_key_returns_none_after_revoke() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        initialize_api_key_test_runtime().await?;

        let user_db_id = {
            let mut db = STATE.db.write().await;
            db::users::create(&mut db, &db::users::test_user("alice")?)?
        };
        let created = create_api_key(user_db_id, "laptop").await?;

        assert!(revoke_api_key_for_user(user_db_id, &created.id).await?);

        let resolved = resolve_api_key(&created.key).await?;
        assert!(
            resolved.is_none(),
            "resolve must not authorize a revoked api key; got {resolved:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn resolve_api_key_debounces_last_used_at_writes() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        initialize_api_key_test_runtime().await?;

        let user_db_id = {
            let mut db = STATE.db.write().await;
            db::users::create(&mut db, &db::users::test_user("alice")?)?
        };
        let created = create_api_key(user_db_id, "laptop").await?;

        resolve_api_key(&created.key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("api key should resolve"))?;
        let first_bump = list_api_keys_for_user(user_db_id).await?[0]
            .last_used_at
            .expect("first resolve should persist last_used_at");

        {
            let mut db = STATE.db.write().await;
            let api_key = db::api_keys::get_by_public_id(&db, &created.id)?
                .ok_or_else(|| anyhow::anyhow!("api key should exist"))?;
            let api_key_db_id: DbId = api_key
                .db_id
                .ok_or_else(|| anyhow::anyhow!("persisted api key has db_id"))?
                .into();
            db::api_keys::update_last_used_at(&mut db, api_key_db_id, first_bump - 1)?;
        }

        resolve_api_key(&created.key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("api key should resolve"))?;

        let after_second = list_api_keys_for_user(user_db_id).await?[0]
            .last_used_at
            .expect("last_used_at remains set after second resolve");
        assert_eq!(
            after_second,
            first_bump - 1,
            "second resolve inside debounce window must not overwrite persisted timestamp"
        );

        Ok(())
    }

    #[test]
    fn should_persist_last_used_returns_true_on_first_call() {
        let _guard = cache_test_guard();
        last_used_cache().clear();
        let id = DbId(9_001);
        assert!(should_persist_last_used(id, 1_000));
    }

    #[test]
    fn should_persist_last_used_debounces_within_window() {
        let _guard = cache_test_guard();
        last_used_cache().clear();
        let id = DbId(9_002);
        record_persisted_last_used(id, 1_000);
        assert!(!should_persist_last_used(
            id,
            1_000 + LAST_USED_DEBOUNCE_SECS - 1
        ));
        assert!(should_persist_last_used(
            id,
            1_000 + LAST_USED_DEBOUNCE_SECS
        ));
    }

    #[test]
    fn record_persisted_last_used_sweeps_stale_entries() {
        let _guard = cache_test_guard();
        last_used_cache().clear();
        for i in 0..=LAST_USED_SWEEP_THRESHOLD {
            record_persisted_last_used(DbId(10_000 + i as i64), 0);
        }
        record_persisted_last_used(DbId(20_000), 10 * LAST_USED_DEBOUNCE_SECS);
        let cache = last_used_cache();
        assert!(cache.contains_key(&DbId(20_000)));
        assert!(!cache.contains_key(&DbId(10_000)));
    }

    #[test]
    fn record_persisted_last_used_force_evicts_when_all_entries_are_fresh() {
        let _guard = cache_test_guard();
        last_used_cache().clear();
        for i in 0..LAST_USED_SWEEP_THRESHOLD {
            record_persisted_last_used(DbId(30_000 + i as i64), i as i64);
        }
        assert_eq!(last_used_cache().len(), LAST_USED_SWEEP_THRESHOLD);

        let now = (LAST_USED_SWEEP_THRESHOLD as i64) + 1;
        record_persisted_last_used(DbId(40_000), now);
        let cache = last_used_cache();
        assert!(cache.len() <= LAST_USED_SWEEP_THRESHOLD);
        assert!(cache.contains_key(&DbId(40_000)));
        assert!(
            !cache.contains_key(&DbId(30_000)),
            "oldest entry should be force-evicted when cache is full of fresh entries"
        );
    }
}
