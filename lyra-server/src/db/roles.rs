// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    CountComparison,
    DbAny,
    DbElement,
    DbError,
    DbId,
    DbTypeMarker,
    DbValue,
    QueryBuilder,
};
use nanoid::nanoid;
use schemars::JsonSchema;
use serde::{
    Deserialize,
    Serialize,
};

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema, DbTypeMarker,
)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Permission {
    Admin,
    ManageUsers,
    ManageRoles,
    ManageLibraries,
    SyncMetadata,
    ManagePlugins,
    ManageProviders,
    ManageMetadata,
    Download,
}

impl Permission {
    fn as_db_str(self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::ManageUsers => "manage_users",
            Self::ManageRoles => "manage_roles",
            Self::ManageLibraries => "manage_libraries",
            Self::SyncMetadata => "sync_metadata",
            Self::ManagePlugins => "manage_plugins",
            Self::ManageProviders => "manage_providers",
            Self::ManageMetadata => "manage_metadata",
            Self::Download => "download",
        }
    }

    fn from_db_str(value: &str) -> Result<Self, DbError> {
        match value {
            "admin" => Ok(Self::Admin),
            "manage_users" => Ok(Self::ManageUsers),
            "manage_roles" => Ok(Self::ManageRoles),
            "manage_libraries" => Ok(Self::ManageLibraries),
            "sync_metadata" => Ok(Self::SyncMetadata),
            "manage_plugins" => Ok(Self::ManagePlugins),
            "manage_providers" => Ok(Self::ManageProviders),
            "manage_metadata" => Ok(Self::ManageMetadata),
            "download" => Ok(Self::Download),
            _ => Err(DbError::from(format!("invalid Permission value '{value}'"))),
        }
    }
}

impl From<Permission> for DbValue {
    fn from(value: Permission) -> Self {
        Self::from(value.as_db_str())
    }
}

impl From<&Permission> for DbValue {
    fn from(value: &Permission) -> Self {
        (*value).into()
    }
}

impl TryFrom<DbValue> for Permission {
    type Error = DbError;

    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        Self::from_db_str(value.string()?)
    }
}

#[derive(DbElement, Clone, Debug)]
pub(crate) struct Role {
    pub(crate) db_id: Option<DbId>,
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) permissions: Vec<Permission>,
}

pub(crate) const BUILTIN_ADMIN_ROLE: &str = "admin";
pub(crate) const BUILTIN_USER_ROLE: &str = "user";

pub(crate) fn is_builtin_role_name(name: &str) -> bool {
    name.eq_ignore_ascii_case(BUILTIN_ADMIN_ROLE) || name.eq_ignore_ascii_case(BUILTIN_USER_ROLE)
}

pub(crate) fn get(db: &DbAny) -> anyhow::Result<Vec<Role>> {
    let mut roles: Vec<Role> = db
        .exec(
            QueryBuilder::select()
                .elements::<Role>()
                .search()
                .from("roles")
                .where_()
                .distance(CountComparison::Equal(1))
                .and()
                .node()
                .and()
                .key("db_element_id")
                .value("Role")
                .end_where()
                .query(),
        )?
        .try_into()?;
    roles.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(roles)
}

pub(crate) fn get_by_name(db: &DbAny, name: &str) -> anyhow::Result<Option<Role>> {
    let mut roles: Vec<Role> = db
        .exec(
            QueryBuilder::select()
                .elements::<Role>()
                .search()
                .from("roles")
                .where_()
                .key("name")
                .value(name)
                .end_where()
                .query(),
        )?
        .try_into()?;
    Ok(roles.pop())
}

pub(crate) fn get_by_id(
    db: &impl super::DbAccess,
    role_db_id: DbId,
) -> anyhow::Result<Option<Role>> {
    super::graph::fetch_typed_by_id(db, role_db_id, "Role")
}

pub(crate) fn get_role_for_user<A: super::DbAccess>(
    db: &A,
    user_db_id: DbId,
) -> anyhow::Result<Option<Role>> {
    let mut roles: Vec<Role> = db
        .exec(
            QueryBuilder::select()
                .elements::<Role>()
                .search()
                .from(user_db_id)
                .where_()
                .distance(CountComparison::Equal(2))
                .and()
                .node()
                .and()
                .key("db_element_id")
                .value("Role")
                .end_where()
                .query(),
        )?
        .try_into()?;
    Ok(roles.pop())
}

pub(crate) fn create(db: &mut DbAny, role: &Role) -> anyhow::Result<DbId> {
    db.transaction_mut(|t| -> anyhow::Result<DbId> {
        let db_id = t
            .exec_mut(QueryBuilder::insert().element(role).query())?
            .ids()[0];
        t.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("roles")
                .to(db_id)
                .query(),
        )?;
        Ok(db_id)
    })
}

pub(crate) fn update(db: &mut impl super::DbAccess, role: &Role) -> anyhow::Result<()> {
    db.exec_mut(QueryBuilder::insert().element(role).query())?;
    Ok(())
}

pub(crate) fn delete(db: &mut DbAny, role_db_id: DbId) -> anyhow::Result<()> {
    db.exec_mut(QueryBuilder::remove().ids(role_db_id).query())?;
    Ok(())
}

pub(crate) fn assign_role_to_user(
    db: &mut DbAny,
    user_db_id: DbId,
    role_db_id: DbId,
) -> anyhow::Result<()> {
    db.transaction_mut(|t| -> anyhow::Result<()> {
        if let Some(current) = get_role_for_user(t, user_db_id)? {
            if current.db_id == Some(role_db_id) {
                return Ok(());
            }
            // Remove the edge from user to the current role
            if let Some(current_db_id) = current.db_id {
                let edges = t.exec(
                    QueryBuilder::search()
                        .from(user_db_id)
                        .to(current_db_id)
                        .where_()
                        .edge()
                        .end_where()
                        .query(),
                )?;
                let edge_ids: Vec<DbId> = edges.elements.into_iter().map(|el| el.id).collect();
                if !edge_ids.is_empty() {
                    t.exec_mut(QueryBuilder::remove().ids(edge_ids).query())?;
                }
            }
        }

        t.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(user_db_id)
                .to(role_db_id)
                .query(),
        )?;

        Ok(())
    })
}

pub(crate) fn has_permission(permissions: &[Permission], required: Permission) -> bool {
    permissions.contains(&Permission::Admin) || permissions.contains(&required)
}

pub(crate) fn has_admin_role(db: &DbAny, user_db_id: DbId) -> anyhow::Result<bool> {
    let role = get_role_for_user(db, user_db_id)?;
    Ok(role
        .map(|r| r.permissions.contains(&Permission::Admin))
        .unwrap_or(false))
}

pub(crate) fn get_users_with_role(
    db: &DbAny,
    role_db_id: DbId,
) -> anyhow::Result<Vec<super::User>> {
    let mut users: Vec<super::User> = db
        .exec(
            QueryBuilder::select()
                .elements::<super::User>()
                .search()
                .to(role_db_id)
                .where_()
                .distance(CountComparison::Equal(2))
                .and()
                .node()
                .and()
                .key("db_element_id")
                .value("User")
                .end_where()
                .query(),
        )?
        .try_into()?;
    users.sort_by(|a, b| a.username.cmp(&b.username));
    Ok(users)
}

pub(crate) fn count_users_with_role(db: &DbAny, role_db_id: DbId) -> anyhow::Result<usize> {
    Ok(get_users_with_role(db, role_db_id)?.len())
}

fn get_admin_users(db: &DbAny) -> anyhow::Result<Vec<super::User>> {
    let mut admin_users = Vec::new();
    for user in super::users::get(db)? {
        let Some(user_db_id) = user.db_id else {
            continue;
        };
        let role = get_role_for_user(db, user_db_id)?;
        if role
            .map(|role| role.permissions.contains(&Permission::Admin))
            .unwrap_or(false)
        {
            admin_users.push(user);
        }
    }
    Ok(admin_users)
}

pub(crate) fn count_admins(db: &DbAny) -> anyhow::Result<usize> {
    Ok(get_admin_users(db)?.len())
}

pub(crate) fn has_non_default_admin(db: &DbAny, default_username: &str) -> anyhow::Result<bool> {
    let default_lower = default_username.to_lowercase();
    Ok(get_admin_users(db)?
        .iter()
        .any(|user| user.username != default_lower))
}

pub(crate) fn ensure_builtin_roles(db: &mut DbAny) -> anyhow::Result<()> {
    if get_by_name(db, BUILTIN_ADMIN_ROLE)?.is_none() {
        create(
            db,
            &Role {
                db_id: None,
                id: nanoid!(),
                name: BUILTIN_ADMIN_ROLE.to_string(),
                permissions: vec![Permission::Admin],
            },
        )?;
    }

    if get_by_name(db, BUILTIN_USER_ROLE)?.is_none() {
        create(
            db,
            &Role {
                db_id: None,
                id: nanoid!(),
                name: BUILTIN_USER_ROLE.to_string(),
                permissions: vec![],
            },
        )?;
    }

    Ok(())
}

pub(crate) fn ensure_user_has_role(
    db: &mut DbAny,
    user_db_id: DbId,
    role_name: &str,
) -> anyhow::Result<()> {
    if let Some(current) = get_role_for_user(db, user_db_id)? {
        if current.name == role_name {
            return Ok(());
        }
    }

    let role = get_by_name(db, role_name)?
        .ok_or_else(|| anyhow::anyhow!("role not found: {role_name}"))?;
    let role_db_id = role
        .db_id
        .ok_or_else(|| anyhow::anyhow!("role has no db_id: {role_name}"))?;

    assign_role_to_user(db, user_db_id, role_db_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::new_test_db;
    use crate::db::users;
    use agdb::DbValue;

    fn element_value(element: &agdb::DbElement, key: &str) -> Option<DbValue> {
        element.values.iter().find_map(|kv| {
            let Ok(found_key) = kv.key.string() else {
                return None;
            };
            if found_key == key {
                Some(kv.value.clone())
            } else {
                None
            }
        })
    }

    #[test]
    fn permission_uses_stable_string_db_values() -> anyhow::Result<()> {
        assert_eq!(DbValue::from(Permission::Admin), DbValue::from("admin"));
        assert_eq!(
            Permission::try_from(DbValue::from("manage_plugins"))?,
            Permission::ManagePlugins
        );
        assert!(Permission::try_from(DbValue::from("editor")).is_err());
        assert_eq!(
            DbValue::from(vec![Permission::Admin, Permission::ManageUsers]),
            DbValue::VecString(vec!["admin".to_string(), "manage_users".to_string()])
        );
        assert_eq!(
            Vec::<Permission>::try_from(DbValue::VecString(vec![
                "admin".to_string(),
                "manage_users".to_string(),
            ]))?,
            vec![Permission::Admin, Permission::ManageUsers]
        );
        Ok(())
    }

    #[test]
    fn ensure_builtin_roles_creates_admin_and_user() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        ensure_builtin_roles(&mut db)?;

        let admin = get_by_name(&db, "admin")?;
        assert!(admin.is_some());
        assert_eq!(admin.unwrap().permissions, vec![Permission::Admin]);

        let user = get_by_name(&db, "user")?;
        assert!(user.is_some());
        assert!(user.unwrap().permissions.is_empty());

        Ok(())
    }

    #[test]
    fn ensure_builtin_roles_is_idempotent() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        ensure_builtin_roles(&mut db)?;
        ensure_builtin_roles(&mut db)?;

        assert!(get_by_name(&db, "admin")?.is_some());
        assert!(get_by_name(&db, "user")?.is_some());

        Ok(())
    }

    #[test]
    fn assign_role_to_user_creates_edge() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        ensure_builtin_roles(&mut db)?;

        let user_db_id = users::create(&mut db, &users::test_user("alice")?)?;
        ensure_user_has_role(&mut db, user_db_id, "admin")?;

        let role = get_role_for_user(&db, user_db_id)?;
        assert!(role.is_some());
        assert_eq!(role.unwrap().name, "admin");

        Ok(())
    }

    #[test]
    fn assign_role_replaces_previous_role() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        ensure_builtin_roles(&mut db)?;

        let user_db_id = users::create(&mut db, &users::test_user("alice")?)?;
        ensure_user_has_role(&mut db, user_db_id, "admin")?;

        let user_role = get_by_name(&db, "user")?.unwrap();
        assign_role_to_user(&mut db, user_db_id, user_role.db_id.unwrap())?;

        let role = get_role_for_user(&db, user_db_id)?;
        assert_eq!(role.unwrap().name, "user");

        Ok(())
    }

    #[test]
    fn has_permission_checks_admin_bypass() {
        assert!(has_permission(
            &[Permission::Admin],
            Permission::ManageUsers
        ));
        assert!(has_permission(
            &[Permission::Admin],
            Permission::ManageLibraries
        ));
        assert!(!has_permission(
            &[Permission::ManageUsers],
            Permission::ManageLibraries
        ));
        assert!(has_permission(
            &[Permission::ManageUsers],
            Permission::ManageUsers
        ));
        assert!(!has_permission(&[], Permission::ManageUsers));
    }

    #[test]
    fn count_admins_counts_users_with_admin_role() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        ensure_builtin_roles(&mut db)?;

        assert_eq!(count_admins(&db)?, 0);

        let alice = users::create(&mut db, &users::test_user("alice")?)?;
        ensure_user_has_role(&mut db, alice, "admin")?;
        assert_eq!(count_admins(&db)?, 1);

        let bob = users::create(&mut db, &users::test_user("bob")?)?;
        ensure_user_has_role(&mut db, bob, "admin")?;
        assert_eq!(count_admins(&db)?, 2);

        Ok(())
    }

    #[test]
    fn has_non_default_admin_excludes_default_user() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        ensure_builtin_roles(&mut db)?;

        let default_id = users::ensure_default_user(&mut db, "default")?;
        ensure_user_has_role(&mut db, default_id, "admin")?;
        assert!(!has_non_default_admin(&db, "default")?);

        let alice = users::create(&mut db, &users::test_user("alice")?)?;
        ensure_user_has_role(&mut db, alice, "admin")?;
        assert!(has_non_default_admin(&db, "default")?);

        Ok(())
    }

    #[test]
    fn count_admins_counts_users_with_custom_admin_roles() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        ensure_builtin_roles(&mut db)?;

        let custom_admin_role = Role {
            db_id: None,
            id: nanoid!(),
            name: "ops-admin".to_string(),
            permissions: vec![Permission::Admin],
        };
        let custom_admin_role_id = create(&mut db, &custom_admin_role)?;

        let alice = users::create(&mut db, &users::test_user("alice")?)?;
        assign_role_to_user(&mut db, alice, custom_admin_role_id)?;

        assert_eq!(count_admins(&db)?, 1);
        Ok(())
    }

    #[test]
    fn role_persists_permissions_as_string_vector() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let role = Role {
            db_id: None,
            id: nanoid!(),
            name: "editor".to_string(),
            permissions: vec![Permission::ManageMetadata, Permission::Download],
        };
        let role_id = create(&mut db, &role)?;

        let element = db
            .exec(QueryBuilder::select().ids(role_id).query())?
            .elements
            .into_iter()
            .next()
            .expect("role element");

        assert_eq!(
            element_value(&element, "permissions"),
            Some(DbValue::VecString(vec![
                "manage_metadata".to_string(),
                "download".to_string(),
            ]))
        );
        assert_eq!(
            get_by_id(&db, role_id)?
                .expect("role should round-trip")
                .permissions,
            vec![Permission::ManageMetadata, Permission::Download]
        );

        Ok(())
    }

    #[test]
    fn has_non_default_admin_counts_custom_admin_roles() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        ensure_builtin_roles(&mut db)?;

        let default_id = users::ensure_default_user(&mut db, "default")?;
        ensure_user_has_role(&mut db, default_id, "admin")?;

        let custom_admin_role = Role {
            db_id: None,
            id: nanoid!(),
            name: "ops-admin".to_string(),
            permissions: vec![Permission::Admin],
        };
        let custom_admin_role_id = create(&mut db, &custom_admin_role)?;

        let alice = users::create(&mut db, &users::test_user("alice")?)?;
        assign_role_to_user(&mut db, alice, custom_admin_role_id)?;

        assert!(has_non_default_admin(&db, "default")?);
        Ok(())
    }

    #[test]
    fn builtin_role_names_are_reserved_case_insensitively() {
        assert!(is_builtin_role_name("admin"));
        assert!(is_builtin_role_name("Admin"));
        assert!(is_builtin_role_name("user"));
        assert!(is_builtin_role_name("USER"));
        assert!(!is_builtin_role_name("editor"));
    }
}
