// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    fmt::Write,
    sync::LazyLock,
};

use agdb::DbId;
use argon2::{
    Argon2,
    PasswordHash,
    PasswordHasher,
    PasswordVerifier,
    password_hash::{
        SaltString,
        rand_core::{
            OsRng,
            RngCore,
        },
    },
};
use axum::http::HeaderMap;

use crate::{
    STATE,
    config::Config,
    db::{
        self,
        Permission,
    },
};

// Defeat user-enumeration via response timing: the miss branch of login_with_password
// verifies against this pre-computed hash so both hit and miss pay the same argon2 cost.
static DUMMY_PASSWORD_HASH: LazyLock<String> = LazyLock::new(|| {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(b"lyra-timing-oracle-dummy-password", &salt)
        .expect("dummy password hash must hash successfully")
        .to_string()
});

pub(crate) mod api_keys;
pub(crate) mod sessions;

pub(crate) fn random_hex_secret<const BYTES: usize>() -> String {
    let mut bytes = [0u8; BYTES];
    OsRng.fill_bytes(&mut bytes);

    let mut secret = String::with_capacity(BYTES * 2);
    for byte in bytes {
        write!(&mut secret, "{byte:02x}").expect("writing to String cannot fail");
    }
    secret
}

pub(crate) fn hash_secret(secret: &str) -> String {
    blake3::hash(secret.as_bytes()).to_hex().to_string()
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AuthError {
    #[error("missing bearer credential")]
    MissingBearerCredential,
    #[error("invalid bearer credential")]
    InvalidBearerCredential,
    #[error("session expired")]
    SessionExpired,
    #[error("forbidden: {0}")]
    Forbidden(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

type AuthResult<T> = std::result::Result<T, AuthError>;

#[derive(Clone, Debug)]
pub(crate) struct Principal {
    pub(crate) user_db_id: DbId,
    pub(crate) username: String,
    pub(crate) permissions: Vec<Permission>,
    pub(crate) role_name: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AuthCredential {
    Session { session_id: DbId },
    ApiKey { api_key_id: DbId, name: String },
    Default,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedAuth {
    pub(crate) principal: Principal,
    pub(crate) credential: AuthCredential,
}

impl ResolvedAuth {
    pub(crate) fn into_principal(self) -> Principal {
        self.principal
    }
}

pub(crate) fn require_permission(
    principal: &Principal,
    permission: Permission,
) -> Result<(), AuthError> {
    if !db::roles::has_permission(&principal.permissions, permission) {
        return Err(AuthError::Forbidden(format!(
            "{permission:?} permission required"
        )));
    }
    Ok(())
}

async fn require_permission_principal(
    headers: &HeaderMap,
    permission: Permission,
) -> AuthResult<Principal> {
    let principal = require_principal(headers).await?;
    require_permission(&principal, permission)?;
    Ok(principal)
}

pub(crate) async fn require_authenticated(headers: &HeaderMap) -> AuthResult<Principal> {
    require_principal(headers).await
}

macro_rules! define_permission_guard {
    ($name:ident, $permission:expr) => {
        pub(crate) async fn $name(headers: &HeaderMap) -> AuthResult<Principal> {
            require_permission_principal(headers, $permission).await
        }
    };
}

define_permission_guard!(require_download, Permission::Download);
define_permission_guard!(require_manage_libraries, Permission::ManageLibraries);
define_permission_guard!(require_manage_metadata, Permission::ManageMetadata);
define_permission_guard!(require_manage_plugins, Permission::ManagePlugins);
define_permission_guard!(require_manage_providers, Permission::ManageProviders);
define_permission_guard!(require_manage_roles, Permission::ManageRoles);
define_permission_guard!(require_manage_users, Permission::ManageUsers);
define_permission_guard!(require_sync_metadata, Permission::SyncMetadata);

pub(crate) struct LoginResult {
    pub(crate) principal: Principal,
    pub(crate) token: String,
}

fn extract_bearer_credential(headers: &HeaderMap) -> Option<String> {
    let header = headers.get(axum::http::header::AUTHORIZATION)?;
    let header = header.to_str().ok()?;
    let (scheme, credential) = header.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let credential = credential.trim();
    if credential.is_empty() {
        None
    } else {
        Some(credential.to_string())
    }
}

fn resolve_role_info(db: &agdb::DbAny, user_db_id: DbId) -> (Vec<Permission>, Option<String>) {
    match db::roles::get_role_for_user(db, user_db_id) {
        Ok(Some(role)) => (role.permissions, Some(role.name)),
        Ok(None) => (vec![], None),
        Err(e) => {
            tracing::warn!(
                user_db_id = user_db_id.0,
                error = %e,
                "failed to resolve role for user, defaulting to no permissions"
            );
            (vec![], None)
        }
    }
}

async fn resolve_auth_from_session_token(token: &str) -> AuthResult<Option<ResolvedAuth>> {
    let token = token.trim();
    if token.is_empty() {
        return Ok(None);
    }

    let db = STATE.db.read().await;
    let token_hash = hash_secret(token);
    let Some((user, session, session_id)) =
        db::users::find_by_session_token_hash(&db, &token_hash).map_err(AuthError::from)?
    else {
        return Ok(None);
    };
    let Some(user_db_id) = user.db_id else {
        return Ok(None);
    };

    if session.expires_at > 0 && db::users::now_secs() >= session.expires_at {
        drop(db);
        let mut db_write = STATE.db.write().await;
        if let Err(e) = db::users::revoke_session_by_id(&mut db_write, session_id) {
            tracing::warn!(error = %e, "failed to revoke expired session");
        }
        return Err(AuthError::SessionExpired);
    }

    let (permissions, role_name) = resolve_role_info(&db, user_db_id);

    Ok(Some(ResolvedAuth {
        principal: Principal {
            user_db_id,
            username: user.username,
            permissions,
            role_name,
        },
        credential: AuthCredential::Session { session_id },
    }))
}

async fn resolve_auth_from_api_key(key: &str) -> AuthResult<Option<ResolvedAuth>> {
    let Some(api_key) = api_keys::resolve_api_key(key)
        .await
        .map_err(|e| AuthError::Internal(e.into()))?
    else {
        return Ok(None);
    };

    let db = STATE.db.read().await;
    let Some(user) = db::users::get_by_id(&db, api_key.user_db_id).map_err(AuthError::from)? else {
        return Ok(None);
    };

    let (permissions, role_name) = resolve_role_info(&db, api_key.user_db_id);

    Ok(Some(ResolvedAuth {
        principal: Principal {
            user_db_id: api_key.user_db_id,
            username: user.username,
            permissions,
            role_name,
        },
        credential: AuthCredential::ApiKey {
            api_key_id: api_key.api_key_id,
            name: api_key.name,
        },
    }))
}

pub(crate) async fn resolve_auth_from_bearer(
    bearer: Option<&str>,
) -> AuthResult<Option<ResolvedAuth>> {
    let bearer = bearer.map(str::trim).filter(|bearer| !bearer.is_empty());

    if !STATE.config.get().auth.enabled {
        let default_principal = resolve_default_principal().await.map_err(AuthError::from)?;
        let Some(bearer) = bearer else {
            return Ok(Some(ResolvedAuth {
                principal: default_principal,
                credential: AuthCredential::Default,
            }));
        };

        if let Some(session_auth) = resolve_auth_from_session_token(bearer).await.ok().flatten()
            && session_auth.principal.user_db_id == default_principal.user_db_id
        {
            return Ok(Some(session_auth));
        }

        if let Some(api_key_auth) = resolve_auth_from_api_key(bearer).await.ok().flatten()
            && api_key_auth.principal.user_db_id == default_principal.user_db_id
        {
            return Ok(Some(api_key_auth));
        }

        return Ok(Some(ResolvedAuth {
            principal: default_principal,
            credential: AuthCredential::Default,
        }));
    }

    let Some(bearer) = bearer else {
        return Ok(None);
    };

    if let Some(session_auth) = resolve_auth_from_session_token(bearer).await? {
        return Ok(Some(session_auth));
    }

    resolve_auth_from_api_key(bearer).await
}

async fn resolve_default_principal() -> AuthResult<Principal> {
    let username = STATE.config.get().auth.default_username.to_lowercase();

    let db = STATE.db.read().await;
    let user = db::users::get_by_username(&db, &username)?.ok_or_else(|| {
        anyhow::anyhow!(
            "default user '{}' does not exist",
            STATE.config.get().auth.default_username
        )
    })?;
    let user_db_id = user.db_id.ok_or_else(|| {
        anyhow::anyhow!(
            "default user '{}' has no db_id",
            STATE.config.get().auth.default_username
        )
    })?;

    let (permissions, role_name) = resolve_role_info(&db, user_db_id);

    Ok(Principal {
        user_db_id,
        username: user.username,
        permissions,
        role_name,
    })
}

pub(crate) async fn logout_with_token(token: Option<&str>) -> AuthResult<bool> {
    let token = token.map(str::trim).filter(|token| !token.is_empty());
    let Some(token) = token else {
        return Ok(false);
    };

    sessions::revoke_session_by_token(token)
        .await
        .map_err(|e| AuthError::Internal(e.into()))
}

async fn create_login_result(
    user_db_id: DbId,
    username: String,
    permissions: Vec<Permission>,
    role_name: Option<String>,
) -> AuthResult<LoginResult> {
    let session = sessions::create_session_for_user(user_db_id)
        .await
        .map_err(|e| AuthError::Internal(e.into()))?;

    Ok(LoginResult {
        principal: Principal {
            user_db_id,
            username,
            permissions,
            role_name,
        },
        token: session.token,
    })
}

pub(crate) async fn login_with_password(
    username: &str,
    password: &str,
) -> AuthResult<Option<LoginResult>> {
    let username = username.trim();
    if username.is_empty() {
        return Ok(None);
    }

    if !STATE.config.get().auth.enabled {
        if !STATE.config.get().auth.allow_default_login_when_disabled {
            return Ok(None);
        }

        let default_username = STATE.config.get().auth.default_username.to_lowercase();
        if username.to_lowercase() != default_username {
            return Ok(None);
        }

        let db = STATE.db.read().await;
        let user = db::users::get_by_username(&db, &default_username)
            .map_err(AuthError::from)?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "default user '{}' does not exist",
                    STATE.config.get().auth.default_username
                )
            })
            .map_err(AuthError::from)?;

        let user_db_id = user
            .db_id
            .ok_or_else(|| anyhow::anyhow!("default user has no db_id"))?;
        let (permissions, role_name) = resolve_role_info(&db, user_db_id);
        drop(db);

        return create_login_result(user_db_id, default_username, permissions, role_name)
            .await
            .map(Some);
    }

    let db = STATE.db.read().await;
    let maybe_user = db::users::get_by_username(&db, username).map_err(AuthError::from)?;

    let argon2 = Argon2::default();
    let Some(user) = maybe_user else {
        let dummy = PasswordHash::new(&DUMMY_PASSWORD_HASH)
            .map_err(anyhow::Error::from)
            .map_err(AuthError::from)?;
        let _ = argon2.verify_password(password.as_bytes(), &dummy);
        return Ok(None);
    };

    let parsed_hash = PasswordHash::new(&user.password)
        .map_err(anyhow::Error::from)
        .map_err(AuthError::from)?;
    if argon2
        .verify_password(password.as_bytes(), &parsed_hash)
        .is_err()
    {
        return Ok(None);
    }

    let user_db_id = user
        .db_id
        .ok_or_else(|| anyhow::anyhow!("user has no db_id"))?;
    let (permissions, role_name) = resolve_role_info(&db, user_db_id);
    drop(db);

    create_login_result(user_db_id, user.username, permissions, role_name)
        .await
        .map(Some)
}

pub(crate) async fn require_principal(headers: &HeaderMap) -> Result<Principal, AuthError> {
    Ok(require_auth(headers).await?.into_principal())
}

pub(crate) async fn require_auth(headers: &HeaderMap) -> Result<ResolvedAuth, AuthError> {
    let bearer = extract_bearer_credential(headers);
    if STATE.config.get().auth.enabled && bearer.is_none() {
        return Err(AuthError::MissingBearerCredential);
    }

    let Some(auth) = resolve_auth_from_bearer(bearer.as_deref()).await? else {
        return Err(AuthError::InvalidBearerCredential);
    };

    Ok(auth)
}

pub(crate) async fn resolve_optional_auth(headers: &HeaderMap) -> AuthResult<Option<ResolvedAuth>> {
    let bearer = extract_bearer_credential(headers);
    resolve_auth_from_bearer(bearer.as_deref()).await
}

pub(crate) async fn ensure_default_user(config: &Config) -> anyhow::Result<()> {
    {
        let mut db_write = STATE.db.write().await;
        let user_db_id =
            db::users::ensure_default_user(&mut db_write, &config.auth.default_username)?;
        db::roles::ensure_builtin_roles(&mut db_write)?;
        db::roles::ensure_user_has_role(&mut db_write, user_db_id, db::roles::BUILTIN_ADMIN_ROLE)?;
    }
    let _ = &*DUMMY_PASSWORD_HASH;
    if !config.auth.enabled {
        tracing::warn!(
            default_user = %config.auth.default_username,
            "authentication is disabled; all requests have admin privileges — do not use in production"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn initialize_auth_test_runtime() -> anyhow::Result<()> {
        crate::testing::initialize_runtime(&crate::testing::LibraryFixtureConfig {
            directory: std::path::PathBuf::from("."),
            language: None,
            country: None,
        })
        .await
    }

    #[test]
    fn random_hex_secret_returns_lowercase_hex_of_expected_length() {
        let secret = random_hex_secret::<16>();
        assert_eq!(secret.len(), 32);
        assert!(secret.chars().all(|ch| ch.is_ascii_hexdigit()));
        assert_eq!(secret, secret.to_ascii_lowercase());
    }

    #[test]
    fn hash_secret_is_deterministic_and_not_plaintext() {
        let first = hash_secret("secret-value");
        let second = hash_secret("secret-value");
        let other = hash_secret("other-value");
        assert_eq!(first, second);
        assert_ne!(first, other);
        assert_ne!(first, "secret-value");
    }

    #[test]
    fn bearer_credential_parser_rejects_non_bearer() {
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "Basic abc".parse().expect("valid header"));
        assert!(extract_bearer_credential(&headers).is_none());
    }

    #[test]
    fn bearer_credential_parser_extracts_credential() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            "Bearer abc123".parse().expect("valid header"),
        );
        assert_eq!(
            extract_bearer_credential(&headers).as_deref(),
            Some("abc123")
        );
    }

    #[test]
    fn extract_bearer_credential_extracts_bearer() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            "Bearer bearer_token".parse().expect("valid header"),
        );

        assert_eq!(
            extract_bearer_credential(&headers).as_deref(),
            Some("bearer_token")
        );
    }

    #[test]
    fn extract_bearer_credential_requires_bearer() {
        let headers = HeaderMap::new();
        assert!(extract_bearer_credential(&headers).is_none());
    }

    #[tokio::test]
    async fn resolve_auth_from_bearer_accepts_api_key() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        initialize_auth_test_runtime().await?;

        let user_db_id = {
            let mut db = STATE.db.write().await;
            db::users::create(&mut db, &db::users::test_user("alice")?)?
        };
        let api_key = api_keys::create_api_key(user_db_id, "laptop").await?;
        let api_key_db_id = {
            let db = STATE.db.read().await;
            db::api_keys::get_by_public_id(&db, &api_key.id)?
                .and_then(|api_key| api_key.db_id)
                .map(Into::into)
                .ok_or_else(|| anyhow::anyhow!("api key should have a db id"))?
        };

        let auth = resolve_auth_from_bearer(Some(&api_key.key))
            .await?
            .ok_or_else(|| anyhow::anyhow!("api key should resolve"))?;

        assert_eq!(auth.principal.user_db_id, user_db_id);
        assert_eq!(auth.principal.username, "alice");
        assert_eq!(
            auth.credential,
            AuthCredential::ApiKey {
                api_key_id: api_key_db_id,
                name: "laptop".to_string()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn resolve_auth_rejects_expired_session() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        initialize_auth_test_runtime().await?;

        let mut config = STATE.config.get().as_ref().clone();
        config.auth.enabled = true;
        config.auth.session_ttl_seconds = 60;
        STATE.config.replace(std::sync::Arc::new(config));

        let user_db_id = {
            let mut db = STATE.db.write().await;
            db::users::create(&mut db, &db::users::test_user("expiring")?)?
        };
        let session = sessions::create_session_for_user(user_db_id).await?;
        let session_db_id = {
            let db = STATE.db.read().await;
            db::users::find_by_session_token_hash(&db, &hash_secret(&session.token))?
                .map(|(_, _, session_id)| session_id)
                .ok_or_else(|| anyhow::anyhow!("session should have a db id"))?
        };
        {
            let mut db = STATE.db.write().await;
            db.exec_mut(
                agdb::QueryBuilder::insert()
                    .values_uniform([("expires_at", db::users::now_secs() - 1).into()])
                    .ids(session_db_id)
                    .query(),
            )?;
        }

        let err = resolve_auth_from_bearer(Some(&session.token))
            .await
            .expect_err("expired session must not resolve");
        assert!(matches!(err, AuthError::SessionExpired), "got {err:?}");

        {
            let db = STATE.db.read().await;
            assert!(
                db::users::find_by_session_token_hash(&db, &hash_secret(&session.token))?.is_none(),
                "expired session should be revoked"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn resolve_auth_allows_ttl_disabled_session_after_config_tightens() -> anyhow::Result<()>
    {
        let _guard = crate::testing::runtime_test_lock().await;
        initialize_auth_test_runtime().await?;

        let mut config = STATE.config.get().as_ref().clone();
        config.auth.enabled = true;
        config.auth.session_ttl_seconds = 0;
        STATE.config.replace(std::sync::Arc::new(config));

        let user_db_id = {
            let mut db = STATE.db.write().await;
            db::users::create(&mut db, &db::users::test_user("no-ttl")?)?
        };
        let session = sessions::create_session_for_user(user_db_id).await?;

        let mut config = STATE.config.get().as_ref().clone();
        config.auth.session_ttl_seconds = 1;
        STATE.config.replace(std::sync::Arc::new(config));

        let auth = resolve_auth_from_bearer(Some(&session.token))
            .await?
            .ok_or_else(|| anyhow::anyhow!("ttl-disabled session should still resolve"))?;
        assert_eq!(auth.principal.user_db_id, user_db_id);

        Ok(())
    }

    #[tokio::test]
    async fn resolve_auth_from_bearer_prefers_session_over_api_key() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        initialize_auth_test_runtime().await?;

        let (session_user_db_id, api_key_user_db_id) = {
            let mut db = STATE.db.write().await;
            let session_user_db_id =
                db::users::create(&mut db, &db::users::test_user("session-user")?)?;
            let api_key_user_db_id =
                db::users::create(&mut db, &db::users::test_user("api-key-user")?)?;
            (session_user_db_id, api_key_user_db_id)
        };

        let session = sessions::create_session_for_user(session_user_db_id).await?;
        let session_db_id = {
            let db = STATE.db.read().await;
            db::users::find_by_session_token_hash(&db, &hash_secret(&session.token))?
                .map(|(_, _, session_id)| session_id)
                .ok_or_else(|| anyhow::anyhow!("session should have a db id"))?
        };
        let key_hash = blake3::hash(session.token.as_bytes()).to_hex().to_string();
        {
            let mut db = STATE.db.write().await;
            db::api_keys::create(
                &mut db,
                api_key_user_db_id,
                "colliding key",
                &key_hash,
                db::users::now_secs(),
            )?;
        }

        let auth = resolve_auth_from_bearer(Some(&session.token))
            .await?
            .ok_or_else(|| anyhow::anyhow!("session should resolve"))?;

        assert_eq!(auth.principal.user_db_id, session_user_db_id);
        assert_eq!(
            auth.credential,
            AuthCredential::Session {
                session_id: session_db_id
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn auth_disabled_does_not_persist_unknown_bearer_as_session() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        initialize_auth_test_runtime().await?;

        let mut config = STATE.config.get().as_ref().clone();
        config.auth.enabled = false;
        STATE.config.replace(std::sync::Arc::new(config));
        ensure_default_user(&STATE.config.get()).await?;

        let bearer = "not-a-session";
        let auth = resolve_auth_from_bearer(Some(bearer))
            .await?
            .ok_or_else(|| anyhow::anyhow!("default auth should resolve"))?;
        assert_eq!(auth.credential, AuthCredential::Default);

        {
            let db = STATE.db.read().await;
            let token_hash = hash_secret(bearer);
            assert!(db::users::find_by_session_token_hash(&db, &token_hash)?.is_none());
        }

        let mut config = STATE.config.get().as_ref().clone();
        config.auth.enabled = true;
        STATE.config.replace(std::sync::Arc::new(config));

        assert!(resolve_auth_from_bearer(Some(bearer)).await?.is_none());

        Ok(())
    }
}
