// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::ops::{
    Deref,
    DerefMut,
};

use aide::axum::{
    ApiRouter,
    routing::{
        delete_with,
        get_with,
        post_with,
        put_with,
    },
};
use aide::transform::TransformOperation;
use argon2::{
    Argon2,
    PasswordHash,
    PasswordVerifier,
    password_hash::{
        PasswordHasher,
        SaltString,
        rand_core::OsRng,
    },
};
use axum::Json;
use axum::extract::{
    Path,
    Query,
};
use axum::http::{
    HeaderMap,
    StatusCode,
};
use nanoid::nanoid;
use schemars::JsonSchema;
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    STATE,
    db::{
        self,
        Permission,
        User,
    },
    routes::{
        AppError,
        deserialize_inc,
    },
    services::auth::{
        AuthCredential,
        AuthError,
        ResolvedAuth,
        api_keys,
        login_with_password,
        require_auth,
        require_manage_roles,
        require_manage_users,
        require_permission,
        require_principal,
    },
};

#[derive(Serialize, JsonSchema)]
struct LoginResponse {
    token: String,
}

#[derive(Debug, Serialize, JsonSchema)]
struct ApiKeyResponse {
    id: String,
    name: String,
    created_at: i64,
    last_used_at: Option<i64>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct CreatedApiKeyResponse {
    id: String,
    name: String,
    key: String,
    created_at: i64,
    last_used_at: Option<i64>,
}

#[derive(Deserialize, JsonSchema)]
struct UserRequest {
    #[schemars(description = "ASCII username, minimum 3 characters.")]
    username: String,
    #[schemars(description = "ASCII password, minimum 8 characters.")]
    password: String,
}

#[derive(Serialize, JsonSchema)]
struct PublicUser {
    id: String,
    username: String,
    role: Option<String>,
}

#[derive(Serialize, JsonSchema)]
struct MeResponse {
    id: String,
    username: String,
    role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    permissions: Option<Vec<Permission>>,
}

#[derive(Deserialize, JsonSchema)]
struct UpdatePasswordRequest {
    #[schemars(description = "New password, minimum 8 ASCII characters.")]
    password: String,
}

#[derive(Deserialize, JsonSchema)]
struct UpdateRoleRequest {
    #[schemars(description = "Role name to assign.")]
    role: String,
}

#[derive(Deserialize, JsonSchema)]
struct UpdateMeRequest {
    #[schemars(description = "Current password, required when changing password.")]
    current_password: Option<String>,
    #[schemars(description = "New password, minimum 8 ASCII characters.")]
    new_password: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct CreateApiKeyRequest {
    #[schemars(description = "Human-readable API key name.")]
    name: String,
}

#[derive(Deserialize, JsonSchema)]
struct MeQuery {
    #[schemars(description = "Comma-separated or repeated values: permissions.")]
    #[serde(default, deserialize_with = "deserialize_inc")]
    inc: Option<Vec<String>>,
}

impl From<api_keys::ApiKeyInfo> for ApiKeyResponse {
    fn from(api_key: api_keys::ApiKeyInfo) -> Self {
        Self {
            id: api_key.id,
            name: api_key.name,
            created_at: api_key.created_at,
            last_used_at: api_key.last_used_at,
        }
    }
}

impl From<api_keys::CreatedApiKey> for CreatedApiKeyResponse {
    fn from(api_key: api_keys::CreatedApiKey) -> Self {
        Self {
            id: api_key.id,
            name: api_key.name,
            key: api_key.key,
            created_at: api_key.created_at,
            last_used_at: api_key.last_used_at,
        }
    }
}

fn validate_username(username: &str) -> Result<String, AppError> {
    if username.trim().is_empty() {
        return Err(AppError::bad_request("username cannot be empty"));
    }
    if username.len() < 3 {
        return Err(AppError::bad_request(
            "username must be at least 3 characters long",
        ));
    }
    if !username.is_ascii() {
        return Err(AppError::bad_request("username must be ASCII"));
    }
    if username.contains(' ') {
        return Err(AppError::bad_request("username cannot contain any spaces"));
    }
    Ok(username.to_lowercase())
}

fn validate_password(password: &str) -> Result<(), AppError> {
    if password.is_empty() {
        return Err(AppError::bad_request("password cannot be empty"));
    }
    if password.len() < 8 {
        return Err(AppError::bad_request(
            "password must be at least 8 characters long",
        ));
    }
    if !password.is_ascii() {
        return Err(AppError::bad_request("password must be ASCII"));
    }
    Ok(())
}

fn hash_password(password: &str) -> Result<String, AppError> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    Ok(argon2
        .hash_password(password.as_bytes(), &salt)?
        .to_string())
}

fn parse_me_includes(inc: Option<Vec<String>>) -> Result<bool, AppError> {
    let values = super::parse_inc_values(inc, &["permissions"])?;
    Ok(values.iter().any(|value| value == "permissions"))
}

async fn forbid_api_key_credential(headers: &HeaderMap) -> Result<ResolvedAuth, AppError> {
    let auth = require_auth(headers).await?;
    match auth.credential {
        AuthCredential::ApiKey { .. } => Err(AppError::forbidden(
            "this operation cannot be performed with an api key credential",
        )),
        AuthCredential::Session { .. } | AuthCredential::Default => Ok(auth),
    }
}

async fn create_user(
    headers: HeaderMap,
    Json(user): Json<UserRequest>,
) -> Result<StatusCode, AppError> {
    let normalized_username = validate_username(&user.username)?;
    if normalized_username == STATE.config.get().auth.default_username.to_lowercase() {
        return Err(AppError::bad_request(format!(
            "username '{}' is reserved",
            STATE.config.get().auth.default_username
        )));
    }

    validate_password(&user.password)?;
    let password_hash = hash_password(&user.password)?;

    let principal = match require_principal(&headers).await {
        Ok(p) => Some(p),
        Err(AuthError::MissingBearerCredential) => None,
        Err(e) => return Err(e.into()),
    };

    let mut db = STATE.db.write().await;

    let default_username = &STATE.config.get().auth.default_username;
    let has_admin = db::roles::has_non_default_admin(&db, default_username)?;

    let role_name = if has_admin {
        let principal =
            principal.ok_or_else(|| AppError::unauthorized("missing bearer credential"))?;
        require_permission(&principal, Permission::ManageUsers)?;
        db::roles::BUILTIN_USER_ROLE
    } else {
        db::roles::BUILTIN_ADMIN_ROLE
    };

    if db::users::get_by_username(db.deref(), &normalized_username)?.is_some() {
        return Err(AppError::bad_request(format!(
            "user {} already exists",
            user.username,
        )));
    }

    let user_db_id = db::users::create(
        db.deref_mut(),
        &User {
            db_id: None,
            id: nanoid!(),
            username: normalized_username,
            password: password_hash,
        },
    )?;

    db::roles::ensure_user_has_role(&mut db, user_db_id, role_name)?;

    Ok(StatusCode::CREATED)
}

async fn list_users(headers: HeaderMap) -> Result<Json<Vec<PublicUser>>, AppError> {
    let _principal = require_manage_users(&headers).await?;

    let db = STATE.db.read().await;
    let users = db::users::get(&db)?;
    let public_users = users
        .into_iter()
        .map(|user| {
            let role_name = user
                .db_id
                .and_then(|db_id| db::roles::get_role_for_user(&db, db_id).ok().flatten())
                .map(|role| role.name);
            PublicUser {
                id: user.id,
                username: user.username,
                role: role_name,
            }
        })
        .collect();

    Ok(Json(public_users))
}

async fn delete_user(
    headers: HeaderMap,
    Path(user_id): Path<String>,
) -> Result<StatusCode, AppError> {
    let principal = require_manage_users(&headers).await?;

    let mut db = STATE.db.write().await;
    let user = db::users::get_by_public_id(&db, &user_id)?
        .ok_or_else(|| AppError::not_found(format!("user not found: {user_id}")))?;
    let user_db_id = user
        .db_id
        .ok_or_else(|| AppError::not_found(format!("user has no db_id: {user_id}")))?;

    if user_db_id == principal.user_db_id {
        return Err(AppError::bad_request("cannot delete yourself"));
    }

    if db::roles::has_admin_role(&db, user_db_id)? {
        if !db::roles::has_permission(&principal.permissions, Permission::Admin) {
            return Err(AppError::forbidden("cannot delete an admin user"));
        }
        if db::roles::count_admins(&db)? <= 1 {
            return Err(AppError::bad_request("cannot delete the last admin"));
        }
    }

    let revoked_api_key_ids = db.transaction_mut(|t| -> anyhow::Result<Vec<agdb::DbId>> {
        let revoked = db::api_keys::delete_all_for_user(t, user_db_id)?;
        db::users::delete_user(t, user_db_id)?;
        Ok(revoked)
    })?;
    drop(db);
    api_keys::forget_last_used_many(revoked_api_key_ids);
    Ok(StatusCode::NO_CONTENT)
}

async fn update_password(
    headers: HeaderMap,
    Path(user_id): Path<String>,
    Json(body): Json<UpdatePasswordRequest>,
) -> Result<StatusCode, AppError> {
    let principal = require_manage_users(&headers).await?;

    validate_password(&body.password)?;
    let password_hash = hash_password(&body.password)?;

    let mut db = STATE.db.write().await;
    let user = db::users::get_by_public_id(&db, &user_id)?
        .ok_or_else(|| AppError::not_found(format!("user not found: {user_id}")))?;
    let user_db_id = user
        .db_id
        .ok_or_else(|| AppError::not_found(format!("user has no db_id: {user_id}")))?;

    if db::roles::has_admin_role(&db, user_db_id)?
        && !db::roles::has_permission(&principal.permissions, Permission::Admin)
    {
        return Err(AppError::forbidden("cannot reset an admin user's password"));
    }

    let revoked_api_key_ids = db.transaction_mut(|t| -> anyhow::Result<Vec<agdb::DbId>> {
        db::users::update_user_password(t, user_db_id, &password_hash)?;
        db::users::revoke_all_sessions_for_user(t, user_db_id)?;
        let revoked = db::api_keys::delete_all_for_user(t, user_db_id)?;
        Ok(revoked)
    })?;
    drop(db);
    api_keys::forget_last_used_many(revoked_api_key_ids);
    Ok(StatusCode::NO_CONTENT)
}

async fn update_role(
    headers: HeaderMap,
    Path(user_id): Path<String>,
    Json(body): Json<UpdateRoleRequest>,
) -> Result<StatusCode, AppError> {
    let principal = require_manage_roles(&headers).await?;

    let mut db = STATE.db.write().await;
    let user = db::users::get_by_public_id(&db, &user_id)?
        .ok_or_else(|| AppError::not_found(format!("user not found: {user_id}")))?;
    let user_db_id = user
        .db_id
        .ok_or_else(|| AppError::not_found(format!("user has no db_id: {user_id}")))?;

    let target_role = db::roles::get_by_name(&db, &body.role)?
        .ok_or_else(|| AppError::not_found(format!("role not found: {}", body.role)))?;
    let target_role_db_id = target_role
        .db_id
        .ok_or_else(|| AppError::not_found("role has no db_id"))?;

    let target_has_admin = target_role.permissions.contains(&Permission::Admin);
    let current_is_admin = db::roles::has_admin_role(&db, user_db_id)?;

    if target_has_admin && !db::roles::has_permission(&principal.permissions, Permission::Admin) {
        return Err(AppError::forbidden(
            "Admin permission required to assign an admin role",
        ));
    }

    if current_is_admin && !target_has_admin {
        if user.username == STATE.config.get().auth.default_username.to_lowercase() {
            return Err(AppError::bad_request("cannot demote the default user"));
        }
        if user_db_id == principal.user_db_id {
            return Err(AppError::bad_request("cannot demote yourself"));
        }
        if db::roles::count_admins(&db)? <= 1 {
            return Err(AppError::bad_request("cannot demote the last admin"));
        }
    }

    db::roles::assign_role_to_user(&mut db, user_db_id, target_role_db_id)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, thiserror::Error)]
enum PasswordRotationError {
    #[error("incorrect current password")]
    IncorrectCurrentPassword,
    #[error("user not found")]
    UserNotFound,
}

async fn update_me(
    headers: HeaderMap,
    Json(body): Json<UpdateMeRequest>,
) -> Result<StatusCode, AppError> {
    let auth = forbid_api_key_credential(&headers).await?;
    let principal = auth.principal;

    if let Some(new_password) = &body.new_password {
        let session_id = match auth.credential {
            AuthCredential::Session { session_id } => session_id,
            AuthCredential::Default => {
                return Err(AppError::forbidden(
                    "password change is not supported when authentication is disabled",
                ));
            }
            AuthCredential::ApiKey { .. } => {
                unreachable!("forbid_api_key_credential already rejected api key credentials")
            }
        };

        let current_password = body.current_password.as_deref().ok_or_else(|| {
            AppError::bad_request("current_password is required to change password")
        })?;

        validate_password(new_password)?;

        let new_hash = hash_password(new_password)?;
        let user_db_id = principal.user_db_id;
        let current_password = current_password.to_string();

        let mut db = STATE.db.write().await;
        let revoked_api_key_ids = db
            .transaction_mut(|t| -> anyhow::Result<Vec<agdb::DbId>> {
                let user = db::users::get_by_id(t, user_db_id)?
                    .ok_or(PasswordRotationError::UserNotFound)?;

                let parsed_hash = PasswordHash::new(&user.password)
                    .map_err(|err| anyhow::anyhow!("stored password hash is unparseable: {err}"))?;
                if Argon2::default()
                    .verify_password(current_password.as_bytes(), &parsed_hash)
                    .is_err()
                {
                    return Err(PasswordRotationError::IncorrectCurrentPassword.into());
                }

                db::users::update_user_password(t, user_db_id, &new_hash)?;
                db::users::revoke_sessions_for_user_except(t, user_db_id, session_id)?;
                let revoked = db::api_keys::delete_all_for_user(t, user_db_id)?;
                Ok(revoked)
            })
            .map_err(map_password_rotation_error)?;
        drop(db);
        api_keys::forget_last_used_many(revoked_api_key_ids);
    }

    Ok(StatusCode::NO_CONTENT)
}

fn map_password_rotation_error(err: anyhow::Error) -> AppError {
    match err.downcast_ref::<PasswordRotationError>() {
        Some(PasswordRotationError::IncorrectCurrentPassword) => {
            AppError::unauthorized("incorrect current password")
        }
        Some(PasswordRotationError::UserNotFound) => AppError::not_found("user not found"),
        None => err.into(),
    }
}

async fn get_me(
    headers: HeaderMap,
    Query(query): Query<MeQuery>,
) -> Result<Json<MeResponse>, AppError> {
    let principal = require_principal(&headers).await?;
    let include_permissions = parse_me_includes(query.inc)?;

    let db = STATE.db.read().await;
    let user = db::users::get_by_id(&db, principal.user_db_id)?
        .ok_or_else(|| AppError::not_found("user not found"))?;

    Ok(Json(MeResponse {
        id: user.id,
        username: user.username,
        role: principal.role_name,
        permissions: include_permissions.then_some(principal.permissions),
    }))
}

async fn login_user(Json(user): Json<UserRequest>) -> Result<Json<LoginResponse>, AppError> {
    let config = STATE.config.get();
    if !config.auth.enabled {
        let default_username = config.auth.default_username.to_lowercase();
        if !config.auth.allow_default_login_when_disabled {
            return Err(AppError::forbidden(
                "password login is disabled because authentication is disabled",
            ));
        }
        if user.username.trim().to_lowercase() != default_username {
            return Err(AppError::forbidden(
                "password login for non-default users is disabled because authentication is disabled",
            ));
        }
    }

    let Some(login_result) = login_with_password(&user.username, &user.password).await? else {
        return Err(AppError::unauthorized("invalid username or password"));
    };

    Ok(Json(LoginResponse {
        token: login_result.token,
    }))
}

async fn create_api_key(
    headers: HeaderMap,
    Json(body): Json<CreateApiKeyRequest>,
) -> Result<(StatusCode, Json<CreatedApiKeyResponse>), AppError> {
    let principal = forbid_api_key_credential(&headers).await?.principal;
    let api_key = api_keys::create_api_key(principal.user_db_id, &body.name).await?;

    Ok((StatusCode::CREATED, Json(api_key.into())))
}

async fn list_api_keys(headers: HeaderMap) -> Result<Json<Vec<ApiKeyResponse>>, AppError> {
    let principal = forbid_api_key_credential(&headers).await?.principal;
    let api_keys = api_keys::list_api_keys_for_user(principal.user_db_id).await?;

    Ok(Json(api_keys.into_iter().map(Into::into).collect()))
}

async fn delete_api_key(
    headers: HeaderMap,
    Path(api_key_id): Path<String>,
) -> Result<StatusCode, AppError> {
    let principal = forbid_api_key_credential(&headers).await?.principal;
    if !api_keys::revoke_api_key_for_user(principal.user_db_id, &api_key_id).await? {
        return Err(AppError::not_found("api key not found"));
    }

    Ok(StatusCode::NO_CONTENT)
}

fn create_user_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Create user")
        .description(
            "Creates a new user account. Requires ManageUsers permission after initial setup. \
             During initial setup (no admin exists), the first user is created as admin without \
             authentication.",
        )
        .response::<201, ()>()
}

fn list_users_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List users")
        .description("Returns all users. Requires ManageUsers permission.")
}

fn delete_user_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Delete user")
        .description(
            "Deletes a user and revokes all their sessions. Requires ManageUsers permission. \
             Deleting admin users requires Admin permission.",
        )
        .response::<204, ()>()
}

fn update_password_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update user password")
        .description(
            "Updates a user's password and revokes all their sessions. Requires ManageUsers \
             permission.",
        )
        .response::<204, ()>()
}

fn update_role_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update user role")
        .description(
            "Assigns a role to a user. Requires ManageRoles permission. Assigning admin-capable \
             roles also requires Admin permission.",
        )
        .response::<204, ()>()
}

fn update_me_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update current user")
        .description(
            "Updates the authenticated user's own account. Currently supports password changes \
             with `current_password` and `new_password`.",
        )
        .response::<204, ()>()
}

fn get_me_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get current user").description(
        "Returns the authenticated user's account information. Use `inc=permissions` to include the resolved effective permissions.",
    )
}

fn login_user_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Login user")
        .description("Validates credentials and returns a session token.")
}

fn create_api_key_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Create API key")
        .description(
            "Creates an API key for the authenticated user. The key secret is returned only once.",
        )
        .response::<201, Json<CreatedApiKeyResponse>>()
}

fn list_api_keys_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List API keys")
        .description("Returns metadata for the authenticated user's API keys.")
}

fn delete_api_key_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Delete API key")
        .description("Deletes one of the authenticated user's API keys.")
        .response::<204, ()>()
}

pub fn user_routes() -> ApiRouter {
    ApiRouter::new()
        .api_route("/", post_with(create_user, create_user_docs))
        .api_route("/", get_with(list_users, list_users_docs))
        .api_route("/{user_id}", delete_with(delete_user, delete_user_docs))
        .api_route(
            "/{user_id}/password",
            put_with(update_password, update_password_docs),
        )
        .api_route("/{user_id}/role", put_with(update_role, update_role_docs))
        .api_route("/login", post_with(login_user, login_user_docs))
}

pub fn me_routes() -> ApiRouter {
    ApiRouter::new()
        .api_route(
            "/",
            get_with(get_me, get_me_docs).patch_with(update_me, update_me_docs),
        )
        .api_route(
            "/api-keys",
            get_with(list_api_keys, list_api_keys_docs)
                .post_with(create_api_key, create_api_key_docs),
        )
        .api_route(
            "/api-keys/{api_key_id}",
            delete_with(delete_api_key, delete_api_key_docs),
        )
        .nest("/plugins", super::plugins::me_plugin_settings_routes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        services::auth::sessions,
        testing::{
            LibraryFixtureConfig,
            initialize_runtime,
            runtime_test_lock,
        },
    };
    use axum::{
        Json,
        body::to_bytes,
        extract::Path,
        http::{
            HeaderMap,
            StatusCode,
            header::AUTHORIZATION,
        },
        response::IntoResponse,
    };
    use std::{
        path::PathBuf,
        time::{
            SystemTime,
            UNIX_EPOCH,
        },
    };

    async fn initialize_test_runtime() -> anyhow::Result<PathBuf> {
        let test_dir = std::env::temp_dir().join(format!(
            "lyra-user-routes-test-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        std::fs::create_dir_all(&test_dir)?;
        initialize_runtime(&LibraryFixtureConfig {
            directory: test_dir.clone(),
            language: None,
            country: None,
        })
        .await?;
        Ok(test_dir)
    }

    async fn create_user(username: &str) -> anyhow::Result<(String, agdb::DbId)> {
        let mut db = STATE.db.write().await;
        let user = User {
            db_id: None,
            id: nanoid!(),
            username: username.to_string(),
            password: "unused".to_string(),
        };
        let public_id = user.id.clone();
        let db_id = db::users::create(&mut db, &user)?;
        Ok((public_id, db_id))
    }

    fn bearer_headers(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            format!("Bearer {token}")
                .parse()
                .expect("valid auth header"),
        );
        headers
    }

    async fn create_headers_with_role(
        username: &str,
        role_name: &str,
    ) -> anyhow::Result<HeaderMap> {
        create_headers_with_permissions(username, role_name, vec![Permission::ManageRoles]).await
    }

    async fn create_headers_for_user(user_db_id: agdb::DbId) -> anyhow::Result<HeaderMap> {
        let session = sessions::create_session_for_user(user_db_id).await?;
        Ok(bearer_headers(&session.token))
    }

    async fn create_headers_with_permissions(
        username: &str,
        role_name: &str,
        permissions: Vec<Permission>,
    ) -> anyhow::Result<HeaderMap> {
        let user_db_id = {
            let mut db = STATE.db.write().await;
            db::roles::ensure_builtin_roles(&mut db)?;
            let user = User {
                db_id: None,
                id: nanoid!(),
                username: username.to_string(),
                password: "unused".to_string(),
            };
            let user_db_id = db::users::create(&mut db, &user)?;
            let assigned_role_name = if db::roles::is_builtin_role_name(role_name) {
                role_name.to_string()
            } else {
                let role = db::roles::Role {
                    db_id: None,
                    id: nanoid!(),
                    name: role_name.to_string(),
                    permissions,
                };
                db::roles::create(&mut db, &role)?;
                role_name.to_string()
            };

            db::roles::ensure_user_has_role(&mut db, user_db_id, &assigned_role_name)?;
            user_db_id
        };

        create_headers_for_user(user_db_id).await
    }

    #[tokio::test]
    async fn login_non_default_user_when_auth_disabled_reports_configuration() -> anyhow::Result<()>
    {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let mut config = STATE.config.get().as_ref().clone();
        config.auth.enabled = false;
        STATE.config.replace(std::sync::Arc::new(config));

        let err = match login_user(Json(UserRequest {
            username: "listener".to_string(),
            password: "password123".to_string(),
        }))
        .await
        {
            Ok(_) => panic!("non-default login should explain disabled auth"),
            Err(err) => err,
        };

        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let body = String::from_utf8(body.to_vec())?;
        assert!(
            body.contains("authentication is disabled"),
            "unexpected error: {body}"
        );

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test]
    async fn manage_roles_without_admin_cannot_assign_admin_role() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let headers = create_headers_with_role("role-manager", "role-manager").await?;
        let (target_user_id, _target_db_id) = create_user("listener").await?;

        let status = update_role(
            headers,
            Path(target_user_id),
            Json(UpdateRoleRequest {
                role: db::roles::BUILTIN_ADMIN_ROLE.to_string(),
            }),
        )
        .await
        .expect_err("non-admin role manager should not assign admin role")
        .into_response()
        .status();
        assert_eq!(status, StatusCode::FORBIDDEN);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test]
    async fn api_key_routes_create_list_and_delete_current_user_keys() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let headers = create_headers_with_role("api-key-user", "api-key-user").await?;

        let (_, Json(created)) = create_api_key(
            headers.clone(),
            Json(CreateApiKeyRequest {
                name: " laptop ".to_string(),
            }),
        )
        .await
        .expect("session auth should create an api key");

        assert_eq!(created.name, "laptop");
        assert_eq!(created.key.len(), 32);
        assert!(created.key.chars().all(|ch| ch.is_ascii_hexdigit()));
        assert_eq!(created.key, created.key.to_ascii_lowercase());
        assert!(created.last_used_at.is_none());

        let Json(listed) = list_api_keys(headers.clone())
            .await
            .expect("session auth should list api keys");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, created.id);
        assert_eq!(listed[0].name, "laptop");
        assert_eq!(listed[0].created_at, created.created_at);
        assert_eq!(listed[0].last_used_at, None);

        delete_api_key(headers.clone(), Path(created.id.clone()))
            .await
            .expect("session auth should delete its api key");
        let Json(listed) = list_api_keys(headers)
            .await
            .expect("deleted api key should no longer be listed");
        assert!(listed.is_empty());

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test]
    async fn api_key_routes_do_not_allow_api_keys_to_manage_keys() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let headers = create_headers_with_role("api-key-user", "api-key-user").await?;

        let (_, Json(created)) = create_api_key(
            headers,
            Json(CreateApiKeyRequest {
                name: "laptop".to_string(),
            }),
        )
        .await
        .expect("session auth should create an api key");

        let status = list_api_keys(bearer_headers(&created.key))
            .await
            .expect_err("api keys should not manage api keys")
            .into_response()
            .status();
        assert_eq!(status, StatusCode::FORBIDDEN);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test]
    async fn update_me_password_change_revokes_api_keys() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;

        let initial_password_hash = hash_password("initial-password-abc")
            .map_err(|err| anyhow::anyhow!("hash_password failed: {err:?}"))?;
        let user_db_id = {
            let mut db = STATE.db.write().await;
            db::roles::ensure_builtin_roles(&mut db)?;
            let user = User {
                db_id: None,
                id: nanoid!(),
                username: "rotate-user".to_string(),
                password: initial_password_hash,
            };
            let user_db_id = db::users::create(&mut db, &user)?;
            db::roles::ensure_user_has_role(&mut db, user_db_id, "admin")?;
            user_db_id
        };

        let session_headers = create_headers_for_user(user_db_id).await?;

        let (_, Json(created)) = create_api_key(
            session_headers.clone(),
            Json(CreateApiKeyRequest {
                name: "laptop".to_string(),
            }),
        )
        .await
        .expect("session auth should create api key");

        assert!(
            api_keys::resolve_api_key(&created.key).await?.is_some(),
            "api key should resolve before password rotation"
        );

        update_me(
            session_headers,
            Json(UpdateMeRequest {
                current_password: Some("initial-password-abc".to_string()),
                new_password: Some("rotated-password-xyz".to_string()),
            }),
        )
        .await
        .expect("password rotation should succeed");

        assert!(
            api_keys::resolve_api_key(&created.key).await?.is_none(),
            "api key must not resolve after password rotation"
        );

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test]
    async fn update_password_as_admin_revokes_target_user_api_keys() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;

        let target_user_id = nanoid!();
        let target_user_db_id = {
            let mut db = STATE.db.write().await;
            db::roles::ensure_builtin_roles(&mut db)?;
            let target = User {
                db_id: None,
                id: target_user_id.clone(),
                username: "target-user".to_string(),
                password: hash_password("original-password-abc")
                    .map_err(|err| anyhow::anyhow!("hash_password failed: {err:?}"))?,
            };
            db::users::create(&mut db, &target)?
        };

        let target_session_headers = create_headers_for_user(target_user_db_id).await?;
        let (_, Json(target_api_key)) = create_api_key(
            target_session_headers,
            Json(CreateApiKeyRequest {
                name: "target-laptop".to_string(),
            }),
        )
        .await
        .expect("target user should create api key");

        assert!(
            api_keys::resolve_api_key(&target_api_key.key)
                .await?
                .is_some(),
            "api key should resolve before admin reset"
        );

        let admin_headers = create_headers_with_permissions(
            "admin-user",
            "admin-user",
            vec![Permission::ManageUsers, Permission::ManageRoles],
        )
        .await?;

        update_password(
            admin_headers,
            Path(target_user_id),
            Json(UpdatePasswordRequest {
                password: "reset-password-xyz".to_string(),
            }),
        )
        .await
        .expect("admin password reset should succeed");

        assert!(
            api_keys::resolve_api_key(&target_api_key.key)
                .await?
                .is_none(),
            "target's api key must not resolve after admin password reset"
        );

        let listed = api_keys::list_api_keys_for_user(target_user_db_id).await?;
        assert!(
            listed.is_empty(),
            "target user should have no api keys after admin reset; got {listed:?}"
        );

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test]
    async fn update_me_rejects_api_key_credentials() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let session_headers = create_headers_with_role("update-me-user", "update-me-user").await?;

        let (_, Json(created)) = create_api_key(
            session_headers,
            Json(CreateApiKeyRequest {
                name: "laptop".to_string(),
            }),
        )
        .await
        .expect("session auth should create an api key");

        let status = update_me(
            bearer_headers(&created.key),
            Json(UpdateMeRequest {
                current_password: Some("irrelevant".to_string()),
                new_password: Some("new-password-123".to_string()),
            }),
        )
        .await
        .expect_err("api keys must not be able to call update_me")
        .into_response()
        .status();
        assert_eq!(status, StatusCode::FORBIDDEN);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test]
    async fn delete_api_key_rejects_other_users_keys() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let owner_headers = create_headers_with_role("api-key-owner", "api-key-owner").await?;
        let other_headers = create_headers_with_role("api-key-other", "api-key-other").await?;

        let (_, Json(created)) = create_api_key(
            owner_headers.clone(),
            Json(CreateApiKeyRequest {
                name: "laptop".to_string(),
            }),
        )
        .await
        .expect("session auth should create an api key");

        let status = delete_api_key(other_headers, Path(created.id.clone()))
            .await
            .expect_err("users should not delete other users' api keys")
            .into_response()
            .status();
        assert_eq!(status, StatusCode::NOT_FOUND);

        let Json(listed) = list_api_keys(owner_headers)
            .await
            .expect("owner api key should remain listed");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, created.id);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test]
    async fn get_me_returns_role_without_permissions_by_default() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let headers = create_headers_with_role("role-manager", "role-manager").await?;

        let Json(response) = get_me(headers, Query(MeQuery { inc: None }))
            .await
            .expect("authenticated user should be able to fetch self");
        assert_eq!(response.username, "role-manager");
        assert_eq!(response.role.as_deref(), Some("role-manager"));
        assert!(response.permissions.is_none());

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test]
    async fn get_me_includes_permissions_when_requested() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let headers = create_headers_with_permissions(
            "metadata-user",
            "metadata-user",
            vec![Permission::ManageMetadata, Permission::Download],
        )
        .await?;

        let Json(response) = get_me(
            headers,
            Query(MeQuery {
                inc: Some(vec!["permissions".to_string()]),
            }),
        )
        .await
        .expect("authenticated user should receive requested permissions");
        assert_eq!(response.role.as_deref(), Some("metadata-user"));
        assert_eq!(
            response.permissions,
            Some(vec![Permission::ManageMetadata, Permission::Download])
        );

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }
}
