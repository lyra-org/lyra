// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Whose personal data a read covers, and which of those users the principal may read for.

use agdb::DbId;
use lyra_db::{
    Permission,
    User,
};

use super::{
    AuthError,
    Principal,
    accounts,
    require_permission,
};

/// Whose data a read covers. Anyone's but the principal's requires [`Permission::ManageUsers`],
/// and an admin's also [`Permission::Admin`].
#[derive(Clone, Copy, Debug)]
pub(crate) enum Owners<'a> {
    Principal,
    User(&'a str),
    All,
}

/// The users `owners` covers that the principal may read for. A named admin the principal may
/// not read for is refused; all users leaves them out. A named user that doesn't exist is none.
pub(crate) fn resolve(
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
    principal_db_id: DbId,
    owners: Owners<'_>,
) -> Result<Vec<User>, AuthError> {
    let owners = match owners {
        Owners::User(user_id) if user_id == principal.user_public_id => Owners::Principal,
        owners => owners,
    };
    Ok(match owners {
        Owners::Principal => lyra_db::users::get_by_id(db, principal_db_id)?
            .into_iter()
            .collect(),
        Owners::User(user_id) => {
            require_permission(principal, Permission::ManageUsers)?;
            let user = accounts::get_by_public_id(db, user_id)?;
            if let Some(user_db_id) = user.as_ref().and_then(|user| user.db_id)
                && !manages(db, principal, principal_db_id, user_db_id)?
            {
                return Err(AuthError::Forbidden(
                    "Admin permission required for an admin's data".to_string(),
                ));
            }
            user.into_iter().collect()
        }
        Owners::All => {
            require_permission(principal, Permission::ManageUsers)?;
            let mut users = Vec::new();
            for user in accounts::list(db)? {
                if let Some(user_db_id) = user.db_id
                    && manages(db, principal, principal_db_id, user_db_id)?
                {
                    users.push(user);
                }
            }
            users
        }
    })
}

/// Whether the principal may see and manage `owner_db_id`'s personal data: its own, anyone's with
/// [`Permission::ManageUsers`], and an admin's only with [`Permission::Admin`] as well, like the
/// user routes that delete an admin or set their password.
pub(crate) fn manages(
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
    principal_db_id: DbId,
    owner_db_id: DbId,
) -> anyhow::Result<bool> {
    if owner_db_id == principal_db_id {
        return Ok(true);
    }
    if !lyra_db::roles::has_permission(&principal.permissions, Permission::ManageUsers) {
        return Ok(false);
    }
    Ok(
        lyra_db::roles::has_permission(&principal.permissions, Permission::Admin)
            || !lyra_db::roles::has_admin_role(db, owner_db_id)?,
    )
}
