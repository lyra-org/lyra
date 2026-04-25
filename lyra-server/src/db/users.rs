// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    CountComparison,
    DbAny,
    DbElement,
    DbId,
    QueryBuilder,
};
use argon2::{
    Argon2,
    password_hash::{
        PasswordHasher,
        SaltString,
        rand_core::{
            OsRng,
            RngCore,
        },
    },
};
use nanoid::nanoid;
use schemars::JsonSchema;
use serde::Serialize;

use super::NodeId;

pub(crate) fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[derive(DbElement, Clone, Debug)]
pub(crate) struct User {
    pub(crate) db_id: Option<DbId>,
    pub(crate) id: String,
    pub(crate) username: String,
    pub(crate) password: String,
}

#[derive(Serialize, DbElement, JsonSchema)]
pub(crate) struct Session {
    #[serde(skip)]
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) token_hash: String,
    #[serde(skip)]
    pub(crate) expires_at: i64,
}

/// Looks up a user by username. The lookup is case-insensitive (normalized to lowercase).
pub(crate) fn get_by_username(db: &DbAny, username: &str) -> anyhow::Result<Option<User>> {
    let mut users: Vec<User> = db
        .exec(
            QueryBuilder::select()
                .elements::<User>()
                .search()
                .from("users")
                .where_()
                .key("username")
                .value(username.to_lowercase())
                .end_where()
                .query(),
        )?
        .try_into()?;
    Ok(users.pop())
}

pub(crate) fn get_by_id(
    db: &impl super::DbAccess,
    user_db_id: DbId,
) -> anyhow::Result<Option<User>> {
    super::graph::fetch_typed_by_id(db, user_db_id, "User")
}

pub(crate) fn get_by_public_id(db: &DbAny, id: &str) -> anyhow::Result<Option<User>> {
    let mut users: Vec<User> = db
        .exec(
            QueryBuilder::select()
                .elements::<User>()
                .search()
                .from("users")
                .where_()
                .key("id")
                .value(id)
                .end_where()
                .query(),
        )?
        .try_into()?;
    Ok(users.pop())
}

pub(crate) fn get(db: &DbAny) -> anyhow::Result<Vec<User>> {
    let mut users: Vec<User> = db
        .exec(
            QueryBuilder::select()
                .elements::<User>()
                .search()
                .from("users")
                .query(),
        )?
        .try_into()?;

    users.sort_by_key(|user| user.username.to_ascii_lowercase());
    Ok(users)
}

pub(crate) fn find_session_by_id(
    db: &impl super::DbAccess,
    session_id: DbId,
) -> anyhow::Result<Option<Session>> {
    let mut sessions: Vec<Session> = db
        .exec(
            QueryBuilder::select()
                .elements::<Session>()
                .search()
                .from("sessions")
                .where_()
                .ids(session_id)
                .query(),
        )?
        .try_into()?;

    Ok(sessions.pop())
}

pub(crate) fn find_by_session_token_hash(
    db: &impl super::DbAccess,
    token_hash: &str,
) -> anyhow::Result<Option<(User, Session, DbId)>> {
    let index_result = db.exec(
        QueryBuilder::search()
            .index("token_hash")
            .value(token_hash)
            .query(),
    )?;

    for session_db_id in index_result.ids().into_iter().filter(|id| id.0 > 0) {
        let Some(session) = find_session_by_id(db, session_db_id)? else {
            continue;
        };
        let mut users: Vec<User> = db
            .exec(
                QueryBuilder::select()
                    .elements::<User>()
                    .search()
                    .from(session_db_id)
                    .where_()
                    .neighbor()
                    .end_where()
                    .query(),
            )?
            .try_into()?;
        if let Some(user) = users.pop() {
            return Ok(Some((user, session, session_db_id)));
        }
    }

    Ok(None)
}

fn hash_random_secret() -> anyhow::Result<String> {
    let mut secret = [0_u8; 32];
    OsRng.fill_bytes(&mut secret);
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let hash = argon2.hash_password(&secret, &salt)?.to_string();
    Ok(hash)
}

pub(crate) fn ensure_default_user(db: &mut DbAny, default_username: &str) -> anyhow::Result<DbId> {
    let username = default_username.trim().to_lowercase();
    if username.is_empty() {
        return Err(anyhow::anyhow!("default username cannot be empty"));
    }

    if let Some(user) = get_by_username(db, &username)? {
        if let Some(db_id) = user.db_id {
            return Ok(db_id);
        }
    }

    create(
        db,
        &User {
            db_id: None,
            id: nanoid!(),
            username,
            password: hash_random_secret()?,
        },
    )
}

pub(crate) fn create(db: &mut DbAny, user: &User) -> anyhow::Result<DbId> {
    db.transaction_mut(|t| -> anyhow::Result<DbId> {
        let user_db_id = t
            .exec_mut(QueryBuilder::insert().element(user).query())?
            .ids()[0];
        t.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("users")
                .to(user_db_id)
                .query(),
        )?;

        Ok(user_db_id)
    })
}

pub(crate) fn login(db: &mut DbAny, user_db_id: DbId, session: &Session) -> anyhow::Result<DbId> {
    db.transaction_mut(|t| -> anyhow::Result<DbId> {
        let session_id = t
            .exec_mut(QueryBuilder::insert().element(session).query())?
            .ids()[0];

        t.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("sessions")
                .to(session_id)
                .query(),
        )?;
        t.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(session_id)
                .to(user_db_id)
                .query(),
        )?;

        Ok(session_id)
    })
}

pub(crate) fn revoke_session_by_id(db: &mut DbAny, session_id: DbId) -> anyhow::Result<bool> {
    if find_session_by_id(db, session_id)?.is_none() {
        return Ok(false);
    }
    db.exec_mut(QueryBuilder::remove().ids(session_id).query())?;
    Ok(true)
}

pub(crate) fn revoke_session_by_token_hash(
    db: &mut DbAny,
    token_hash: &str,
) -> anyhow::Result<bool> {
    let Some((_, _, session_id)) = find_by_session_token_hash(db, token_hash)? else {
        return Ok(false);
    };

    db.exec_mut(QueryBuilder::remove().ids(session_id).query())?;
    Ok(true)
}

pub(crate) fn find_sessions_for_user(
    db: &impl super::DbAccess,
    user_db_id: DbId,
) -> anyhow::Result<Vec<DbId>> {
    let result = db.exec(
        QueryBuilder::search()
            .to(user_db_id)
            .where_()
            .distance(CountComparison::Equal(2))
            .and()
            .node()
            .and()
            .key("db_element_id")
            .value("Session")
            .end_where()
            .query(),
    )?;

    Ok(result
        .elements
        .into_iter()
        .filter(|el| el.id.0 > 0)
        .map(|el| el.id)
        .collect())
}

pub(crate) fn revoke_sessions_for_user_except(
    db: &mut impl super::DbAccess,
    user_db_id: DbId,
    keep: DbId,
) -> anyhow::Result<u64> {
    let session_ids: Vec<DbId> = find_sessions_for_user(db, user_db_id)?
        .into_iter()
        .filter(|id| *id != keep)
        .collect();
    let count = session_ids.len() as u64;
    if !session_ids.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(session_ids).query())?;
    }
    Ok(count)
}

pub(crate) fn revoke_all_sessions_for_user(
    db: &mut impl super::DbAccess,
    user_db_id: DbId,
) -> anyhow::Result<u64> {
    let session_ids = find_sessions_for_user(db, user_db_id)?;
    let count = session_ids.len() as u64;
    if !session_ids.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(session_ids).query())?;
    }
    Ok(count)
}

pub(crate) fn delete_user(db: &mut impl super::DbAccess, user_db_id: DbId) -> anyhow::Result<()> {
    revoke_all_sessions_for_user(db, user_db_id)?;
    super::settings::remove_all_user_plugin_settings_for_user(db, user_db_id)?;
    super::favorites::remove_outbound_for_user(db, user_db_id)?;
    super::tags::remove_outbound_for_user(db, user_db_id)?;
    db.exec_mut(QueryBuilder::remove().ids(user_db_id).query())?;
    Ok(())
}

pub(crate) fn update_user_password(
    db: &mut impl super::DbAccess,
    user_db_id: DbId,
    password_hash: &str,
) -> anyhow::Result<()> {
    db.exec_mut(
        QueryBuilder::insert()
            .values(vec![vec![("password", password_hash).into()]])
            .ids(user_db_id)
            .query(),
    )?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn test_user(username: &str) -> anyhow::Result<User> {
    Ok(User {
        db_id: None,
        id: nanoid!(),
        username: username.to_string(),
        password: hash_random_secret()?,
    })
}

#[cfg(test)]
pub(crate) fn test_session(token_hash: &str) -> Session {
    Session {
        db_id: None,
        id: nanoid!(),
        token_hash: token_hash.to_string(),
        expires_at: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::new_test_db;
    use anyhow::anyhow;
    use argon2::password_hash::PasswordHash;

    #[test]
    fn ensure_default_user_is_idempotent() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let first = ensure_default_user(&mut db, "default")?;
        let second = ensure_default_user(&mut db, "default")?;
        assert_eq!(first, second);

        let user = get_by_username(&db, "default")?;
        assert!(user.is_some());

        Ok(())
    }

    #[test]
    fn find_user_by_session_token_hash_resolves_owner() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create(&mut db, &test_user("alice")?)?;

        let session_ref = "token-hash-123".to_string();
        let session_id = login(&mut db, user_db_id, &test_session(&session_ref))?;

        let (user, _session, found_session_id) = find_by_session_token_hash(&db, &session_ref)?
            .ok_or_else(|| anyhow!("expected user for session token hash"))?;

        assert_eq!(found_session_id, session_id);
        assert_eq!(user.username, "alice");

        Ok(())
    }

    #[test]
    fn revoke_session_by_id_removes_session() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create(&mut db, &test_user("alice")?)?;

        let session_ref = "token-hash-abc".to_string();
        let session_id = login(&mut db, user_db_id, &test_session(&session_ref))?;

        let removed = revoke_session_by_id(&mut db, session_id)?;
        assert!(removed);
        assert!(find_by_session_token_hash(&db, &session_ref)?.is_none());

        Ok(())
    }

    #[test]
    fn revoke_session_by_token_hash_removes_session() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create(&mut db, &test_user("alice")?)?;

        let session_ref = "token-hash-def".to_string();
        let _session_id = login(&mut db, user_db_id, &test_session(&session_ref))?;

        let removed = revoke_session_by_token_hash(&mut db, &session_ref)?;
        assert!(removed);
        assert!(find_by_session_token_hash(&db, &session_ref)?.is_none());

        Ok(())
    }

    #[test]
    fn default_user_has_valid_hash() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = ensure_default_user(&mut db, "default")?;
        let user =
            get_by_id(&db, user_db_id)?.ok_or_else(|| anyhow!("default user should exist"))?;

        PasswordHash::new(&user.password)?;

        Ok(())
    }

    #[test]
    fn get_users_returns_all_users_sorted_by_username() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        create(&mut db, &test_user("zoe")?)?;
        create(&mut db, &test_user("alice")?)?;
        create(&mut db, &test_user("Bob")?)?;

        let users = get(&db)?;
        let usernames: Vec<String> = users.into_iter().map(|user| user.username).collect();
        assert_eq!(usernames, vec!["alice", "Bob", "zoe"]);

        Ok(())
    }

    #[test]
    fn delete_user_cascades_sessions() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create(&mut db, &test_user("alice")?)?;

        login(&mut db, user_db_id, &test_session("token-hash-1"))?;
        login(&mut db, user_db_id, &test_session("token-hash-2"))?;

        assert!(find_by_session_token_hash(&db, "token-hash-1")?.is_some());

        delete_user(&mut db, user_db_id)?;

        assert!(find_by_session_token_hash(&db, "token-hash-1")?.is_none());
        assert!(find_by_session_token_hash(&db, "token-hash-2")?.is_none());
        assert!(get_by_id(&db, user_db_id)?.is_none());

        Ok(())
    }

    #[test]
    fn revoke_all_sessions_for_user_removes_all() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create(&mut db, &test_user("alice")?)?;

        login(&mut db, user_db_id, &test_session("token-hash-a"))?;
        login(&mut db, user_db_id, &test_session("token-hash-b"))?;

        let count = revoke_all_sessions_for_user(&mut db, user_db_id)?;
        assert_eq!(count, 2);
        assert!(find_by_session_token_hash(&db, "token-hash-a")?.is_none());
        assert!(find_by_session_token_hash(&db, "token-hash-b")?.is_none());
        assert!(get_by_id(&db, user_db_id)?.is_some());

        Ok(())
    }
}
