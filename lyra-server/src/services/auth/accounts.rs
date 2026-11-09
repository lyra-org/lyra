// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Which users read as accounts, creating users and changing their passwords. Each change is
//! authorised and checked before its password is hashed, then checked again under the write lock,
//! since hashing runs without it.

use agdb::{
    DbAny,
    DbId,
};
use nanoid::nanoid;

use super::{
    AuthError,
    Principal,
    Recheck,
    api_keys,
    owners,
    passwords::{
        self,
        InvalidPassword,
    },
    require_principal,
};
use crate::services::STATE;
use lyra_db::{
    Permission,
    User,
    users::DEFAULT_USERNAME,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum AccountError {
    #[error("{0}")]
    InvalidRequest(String),
    #[error(transparent)]
    InvalidPassword(#[from] InvalidPassword),
    #[error("incorrect current password")]
    IncorrectCurrentPassword,
    #[error(transparent)]
    Auth(#[from] AuthError),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<Recheck> for AccountError {
    fn from(error: Recheck) -> Self {
        match error {
            Recheck::Internal(error) => Self::Internal(error),
            other => AuthError::Forbidden(other.to_string()).into(),
        }
    }
}

type AccountResult<T> = Result<T, AccountError>;

/// The default user stands in for callers only while authentication is off. While it is on the
/// user is no one's account, though its data stays stored.
pub(crate) fn is_account(user: &User) -> bool {
    !STATE.config().auth.enabled || user.username != DEFAULT_USERNAME
}

/// Every user that reads as an account.
pub(crate) fn list(db: &impl lyra_db::DbAccess) -> anyhow::Result<Vec<User>> {
    let mut users = lyra_db::users::get(db)?;
    users.retain(is_account);
    Ok(users)
}

/// The user with this public id, if it reads as an account.
pub(crate) fn get_by_public_id(
    db: &impl lyra_db::DbAccess,
    id: &str,
) -> anyhow::Result<Option<User>> {
    Ok(lyra_db::users::get_by_public_id(db, id)?.filter(is_account))
}

pub(crate) struct CreatedUser {
    pub(crate) id: String,
    pub(crate) username: String,
    pub(crate) role: &'static str,
}

/// Creates a user. Until a real admin exists anyone may create one, and it becomes the admin;
/// after that the caller needs `ManageUsers`.
pub(crate) async fn create_user(
    bearer: Option<&str>,
    username: &str,
    password: &str,
) -> AccountResult<CreatedUser> {
    let normalized_username = validate_username(username)?;
    if normalized_username == DEFAULT_USERNAME {
        return Err(AccountError::InvalidRequest(format!(
            "username '{DEFAULT_USERNAME}' is reserved"
        )));
    }
    passwords::validate(password)?;

    let caller = match require_principal(bearer).await {
        Ok(principal) => Some(principal),
        Err(AuthError::MissingBearerCredential) => None,
        Err(error) => return Err(error.into()),
    };
    authorize_create(
        &*STATE.db.read().await,
        caller.as_ref(),
        username,
        &normalized_username,
    )?;

    let password_hash = passwords::hash(password).await?;

    let mut db = STATE.db.write().await;
    let role = authorize_create(&db, caller.as_ref(), username, &normalized_username)?;
    let user = User {
        db_id: None,
        id: nanoid!(),
        username: normalized_username,
        password: password_hash,
    };
    let user_db_id = lyra_db::users::create(&mut db, &user)?;
    lyra_db::roles::ensure_user_has_role(&mut db, user_db_id, role)?;

    Ok(CreatedUser {
        id: user.id,
        username: user.username,
        role,
    })
}

/// Sets another user's password and signs them out everywhere. Whoever sets it can sign in as
/// that user, so the caller must already hold everything the account would give them.
pub(crate) async fn set_password(
    principal: &Principal,
    user_public_id: &str,
    password: &str,
) -> AccountResult<()> {
    passwords::validate(password)?;
    authorize_set_password(&*STATE.db.read().await, principal, user_public_id)?;

    let password_hash = passwords::hash(password).await?;

    let mut db = STATE.db.write().await;
    let user_db_id = authorize_set_password(&db, principal, user_public_id)?;
    let revoked_api_key_ids = db.transaction_mut(|t| -> anyhow::Result<Vec<DbId>> {
        lyra_db::users::update_user_password(t, user_db_id, &password_hash)?;
        lyra_db::users::revoke_all_sessions_for_user(t, user_db_id)?;
        lyra_db::api_keys::delete_all_for_user(t, user_db_id)
    })?;
    drop(db);
    api_keys::forget_last_used_many(revoked_api_key_ids);
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum PasswordRotationError {
    #[error("incorrect current password")]
    IncorrectCurrentPassword,
    #[error("user not found")]
    UserNotFound,
}

/// Changes the caller's own password, keeping only the session that changed it.
pub(crate) async fn change_own_password(
    principal: &Principal,
    session_public_id: &str,
    current_password: &str,
    new_password: &str,
) -> AccountResult<()> {
    passwords::validate(new_password)?;

    let checked_hash = {
        let db = STATE.db.read().await;
        let user_db_id = principal.require(&db)?;
        lyra_db::users::get_by_id(&db, user_db_id)?
            .ok_or_else(|| AuthError::NotFound("user not found".to_string()))?
            .password
    };
    if !passwords::matches(current_password, checked_hash.clone()).await? {
        return Err(AccountError::IncorrectCurrentPassword);
    }

    let new_hash = passwords::hash(new_password).await?;

    let mut db = STATE.db.write().await;
    let user_db_id = principal.require(&db)?;
    let revoked_api_key_ids = db
        .transaction_mut(|t| -> anyhow::Result<Vec<DbId>> {
            let user = lyra_db::users::get_by_id(t, user_db_id)?
                .ok_or(PasswordRotationError::UserNotFound)?;
            if user.password != checked_hash {
                return Err(PasswordRotationError::IncorrectCurrentPassword.into());
            }

            lyra_db::users::update_user_password(t, user_db_id, &new_hash)?;
            lyra_db::users::revoke_sessions_for_user_except(t, user_db_id, session_public_id)?;
            lyra_db::api_keys::delete_all_for_user(t, user_db_id)
        })
        .map_err(
            |error| match error.downcast_ref::<PasswordRotationError>() {
                Some(PasswordRotationError::IncorrectCurrentPassword) => {
                    AccountError::IncorrectCurrentPassword
                }
                Some(PasswordRotationError::UserNotFound) => {
                    AuthError::NotFound("user not found".to_string()).into()
                }
                None => error.into(),
            },
        )?;
    drop(db);
    api_keys::forget_last_used_many(revoked_api_key_ids);
    Ok(())
}

fn validate_username(username: &str) -> AccountResult<String> {
    let invalid = |message: &str| Err(AccountError::InvalidRequest(message.to_string()));
    if username.trim().is_empty() {
        return invalid("username cannot be empty");
    }
    if username.len() < 3 {
        return invalid("username must be at least 3 characters long");
    }
    if !username.is_ascii() {
        return invalid("username must be ASCII");
    }
    if username.contains(' ') {
        return invalid("username cannot contain any spaces");
    }
    Ok(username.to_lowercase())
}

/// Returns the role the new user gets. Checks the caller as the database has it now, since a
/// caller can lose its rights while the password hashes.
fn authorize_create(
    db: &DbAny,
    caller: Option<&Principal>,
    username: &str,
    normalized_username: &str,
) -> AccountResult<&'static str> {
    let role = if lyra_db::roles::has_non_default_admin(db)? {
        let caller = caller.ok_or(AuthError::MissingBearerCredential)?;
        caller.current_with(db, Permission::ManageUsers)?;
        lyra_db::roles::BUILTIN_USER_ROLE
    } else {
        lyra_db::roles::BUILTIN_ADMIN_ROLE
    };
    if lyra_db::users::get_by_username(db, normalized_username)?.is_some() {
        return Err(AccountError::InvalidRequest(format!(
            "user {username} already exists"
        )));
    }
    Ok(role)
}

/// Checks the caller as the database has it now, like [`authorize_create`].
fn authorize_set_password(
    db: &DbAny,
    principal: &Principal,
    user_public_id: &str,
) -> AccountResult<DbId> {
    let caller = principal.current_with(db, Permission::ManageUsers)?;
    let user_db_id = lyra_db::users::get_by_public_id(db, user_public_id)?
        .ok_or_else(|| AuthError::NotFound(format!("user not found: {user_public_id}")))?
        .db_id
        .ok_or_else(|| AuthError::NotFound(format!("user has no db_id: {user_public_id}")))?;
    if !owners::holds_permissions_of(db, &caller, user_db_id)? {
        return Err(AuthError::Forbidden(
            "cannot set the password of a user with permissions you do not hold".to_string(),
        )
        .into());
    }
    if !lyra_db::libraries::accessible_library_ids(db, user_db_id)?
        .is_subset(&caller.accessible_library_ids)
    {
        return Err(AuthError::Forbidden(
            "cannot set the password of a user with libraries you cannot access".to_string(),
        )
        .into());
    }
    Ok(user_db_id)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::services::testing::{
        init_default_test_state,
        runtime_test_lock,
        user_principal,
    };

    async fn insert_user(username: &str, role: &str) -> anyhow::Result<DbId> {
        let mut db = STATE.db.write().await;
        lyra_db::roles::ensure_builtin_roles(&mut db)?;
        let user_db_id = lyra_db::users::create(&mut db, &lyra_db::fixtures::test_user(username)?)?;
        lyra_db::roles::ensure_user_has_role(&mut db, user_db_id, role)?;
        Ok(user_db_id)
    }

    /// Fails if `work` waits for a password slot, which [`passwords::hold_all_slots`] keeps taken.
    async fn without_hashing<T>(work: impl Future<Output = T>) -> T {
        let _slots = passwords::hold_all_slots().await;
        tokio::time::timeout(Duration::from_secs(5), work)
            .await
            .expect("refused before hashing")
    }

    #[tokio::test]
    async fn anonymous_create_after_setup_is_refused_before_hashing() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        insert_user("first-admin", lyra_db::roles::BUILTIN_ADMIN_ROLE).await?;

        let result = without_hashing(create_user(None, "intruder", "long-enough-password")).await;

        assert!(matches!(
            result,
            Err(AccountError::Auth(AuthError::MissingBearerCredential))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn non_admin_setting_an_admin_password_is_refused_before_hashing() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let admin_db_id = insert_user("target-admin", lyra_db::roles::BUILTIN_ADMIN_ROLE).await?;
        let admin_id = lyra_db::users::get_by_id(&*STATE.db.read().await, admin_db_id)?
            .ok_or_else(|| anyhow::anyhow!("admin should exist"))?
            .id;
        let caller_db_id = insert_user("plain-user", lyra_db::roles::BUILTIN_USER_ROLE).await?;
        let caller = user_principal(caller_db_id).await?;

        let result =
            without_hashing(set_password(&caller, &admin_id, "long-enough-password")).await;

        assert!(matches!(
            result,
            Err(AccountError::Auth(AuthError::Forbidden(_)))
        ));
        Ok(())
    }

    async fn insert_user_with_permissions(
        username: &str,
        permissions: Vec<Permission>,
    ) -> anyhow::Result<(DbId, String)> {
        let role_name = format!("{username}-role");
        let mut db = STATE.db.write().await;
        lyra_db::roles::create(
            &mut db,
            &lyra_db::roles::Role {
                db_id: None,
                id: nanoid!(),
                name: role_name.clone(),
                permissions,
            },
        )?;
        let user = lyra_db::fixtures::test_user(username)?;
        let public_id = user.id.clone();
        let user_db_id = lyra_db::users::create(&mut db, &user)?;
        lyra_db::roles::ensure_user_has_role(&mut db, user_db_id, &role_name)?;
        Ok((user_db_id, public_id))
    }

    async fn grant_library(user_db_id: DbId, library_db_id: DbId) -> anyhow::Result<()> {
        lyra_db::libraries::grant_access(
            &mut *STATE.db.write().await,
            user_db_id,
            library_db_id,
            lyra_db::libraries::AccessKind::ReadWrite,
        )
    }

    #[tokio::test]
    async fn setting_the_password_of_a_user_holding_more_is_refused_before_hashing()
    -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let (caller_db_id, _) =
            insert_user_with_permissions("manager", vec![Permission::ManageUsers]).await?;
        let (_, target_id) = insert_user_with_permissions(
            "plugin-owner",
            vec![Permission::ManageUsers, Permission::ManagePlugins],
        )
        .await?;
        let caller = user_principal(caller_db_id).await?;

        let result =
            without_hashing(set_password(&caller, &target_id, "long-enough-password")).await;

        assert!(matches!(
            result,
            Err(AccountError::Auth(AuthError::Forbidden(_)))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn setting_a_password_needs_every_library_the_user_has() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let (caller_db_id, _) =
            insert_user_with_permissions("manager", vec![Permission::ManageUsers]).await?;
        let (target_db_id, target_id) =
            insert_user_with_permissions("listener", Vec::new()).await?;
        let library_db_id = lyra_db::test_db::insert_library(
            &mut *STATE.db.write().await,
            "Private",
            "/accounts/private",
        )?;
        grant_library(target_db_id, library_db_id).await?;

        let caller = user_principal(caller_db_id).await?;
        let refused =
            without_hashing(set_password(&caller, &target_id, "long-enough-password")).await;
        assert!(matches!(
            refused,
            Err(AccountError::Auth(AuthError::Forbidden(_)))
        ));

        grant_library(caller_db_id, library_db_id).await?;
        let caller = user_principal(caller_db_id).await?;
        set_password(&caller, &target_id, "long-enough-password").await?;
        Ok(())
    }

    #[tokio::test]
    async fn caller_demoted_while_the_password_hashes_cannot_set_it() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let caller_db_id = insert_user("demoted-admin", lyra_db::roles::BUILTIN_ADMIN_ROLE).await?;
        let target_db_id = insert_user("target-user", lyra_db::roles::BUILTIN_USER_ROLE).await?;
        let target_id = lyra_db::users::get_by_id(&*STATE.db.read().await, target_db_id)?
            .ok_or_else(|| anyhow::anyhow!("target should exist"))?
            .id;
        let caller = user_principal(caller_db_id).await?;

        let slots = passwords::hold_all_slots().await;
        let change =
            tokio::spawn(
                async move { set_password(&caller, &target_id, "long-enough-password").await },
            );
        tokio::time::sleep(Duration::from_millis(100)).await;
        lyra_db::roles::ensure_user_has_role(
            &mut *STATE.db.write().await,
            caller_db_id,
            lyra_db::roles::BUILTIN_USER_ROLE,
        )?;
        drop(slots);

        assert!(matches!(
            change.await?,
            Err(AccountError::Auth(AuthError::Forbidden(_)))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn first_user_becomes_admin_and_closes_setup() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
        }

        let first = create_user(None, "First-Admin", "long-enough-password").await?;
        assert_eq!(first.username, "first-admin");
        assert_eq!(first.role, lyra_db::roles::BUILTIN_ADMIN_ROLE);

        let second = create_user(None, "second-admin", "long-enough-password").await;
        assert!(matches!(
            second,
            Err(AccountError::Auth(AuthError::MissingBearerCredential))
        ));
        Ok(())
    }
}
