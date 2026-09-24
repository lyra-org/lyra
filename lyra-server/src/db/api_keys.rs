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
use nanoid::nanoid;
use serde::Serialize;

use super::NodeId;

pub(crate) const MAX_NAME_LEN: usize = 128;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, DbElement, Serialize)]
pub(crate) struct ApiKey {
    #[serde(skip)]
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) key_hash: String,
    pub(crate) created_at: i64,
    pub(crate) last_used_at: Option<i64>,
}

pub(crate) fn create(
    db: &mut DbAny,
    user_db_id: DbId,
    name: &str,
    key_hash: &str,
    created_at: i64,
) -> anyhow::Result<DbId> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        anyhow::bail!("db::api_keys::create invariant: name is empty or whitespace");
    }
    if trimmed.chars().count() > MAX_NAME_LEN {
        anyhow::bail!("db::api_keys::create invariant: name exceeds {MAX_NAME_LEN} characters",);
    }
    if key_hash.is_empty() {
        anyhow::bail!("db::api_keys::create invariant: key_hash is empty");
    }

    let api_key = ApiKey {
        db_id: None,
        id: nanoid!(),
        name: trimmed.to_string(),
        key_hash: key_hash.to_string(),
        created_at,
        last_used_at: None,
    };

    db.transaction_mut(|t| -> anyhow::Result<DbId> {
        let api_key_db_id = t
            .exec_mut(QueryBuilder::insert().element(&api_key).query())?
            .ids()[0];

        t.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("api_keys")
                .to(api_key_db_id)
                .query(),
        )?;

        t.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(api_key_db_id)
                .to(user_db_id)
                .query(),
        )?;

        Ok(api_key_db_id)
    })
}

pub(crate) fn get_by_id(
    db: &impl super::DbAccess,
    api_key_db_id: DbId,
) -> anyhow::Result<Option<ApiKey>> {
    super::graph::fetch_typed_by_id(db, api_key_db_id, "ApiKey")
}

pub(crate) fn get_by_public_id(
    db: &impl super::DbAccess,
    id: &str,
) -> anyhow::Result<Option<ApiKey>> {
    super::graph::fetch_typed_by_index(db, "id", id, "ApiKey")
}

pub(crate) fn find_by_key_hash(
    db: &impl super::DbAccess,
    key_hash: &str,
) -> anyhow::Result<Option<ApiKey>> {
    super::graph::fetch_typed_by_index(db, "key_hash", key_hash, "ApiKey")
}

pub(crate) fn list_for_user(
    db: &impl super::DbAccess,
    user_db_id: DbId,
) -> anyhow::Result<Vec<ApiKey>> {
    let mut api_keys: Vec<ApiKey> = db
        .exec(
            QueryBuilder::select()
                .elements::<ApiKey>()
                .search()
                .to(user_db_id)
                .where_()
                .distance(CountComparison::Equal(2))
                .and()
                .node()
                .and()
                .key("db_element_id")
                .value("ApiKey")
                .end_where()
                .query(),
        )?
        .try_into()?;

    api_keys.sort_by(|a, b| {
        a.created_at
            .cmp(&b.created_at)
            .then_with(|| a.name.cmp(&b.name))
            .then_with(|| a.id.cmp(&b.id))
    });
    Ok(api_keys)
}

pub(crate) fn count_for_user(db: &impl super::DbAccess, user_db_id: DbId) -> anyhow::Result<usize> {
    let result = db.exec(
        QueryBuilder::search()
            .to(user_db_id)
            .where_()
            .distance(CountComparison::Equal(2))
            .and()
            .node()
            .and()
            .key("db_element_id")
            .value("ApiKey")
            .end_where()
            .query(),
    )?;
    Ok(result.elements.iter().filter(|el| el.id.0 > 0).count())
}

pub(crate) fn get_owner_id(
    db: &impl super::DbAccess,
    api_key_db_id: DbId,
) -> anyhow::Result<Option<DbId>> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .from(api_key_db_id)
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(1))
            .end_where()
            .query(),
    )?;

    Ok(result
        .elements
        .into_iter()
        .find_map(|element| (element.to.0 > 0).then_some(element.to)))
}

pub(crate) fn delete_by_id(
    db: &mut impl super::DbAccess,
    api_key_db_id: DbId,
) -> anyhow::Result<bool> {
    if get_by_id(db, api_key_db_id)?.is_none() {
        return Ok(false);
    }

    db.exec_mut(QueryBuilder::remove().ids(api_key_db_id).query())?;
    Ok(true)
}

pub(crate) fn update_last_used_at(
    db: &mut impl super::DbAccess,
    api_key_db_id: DbId,
    last_used_at: i64,
) -> anyhow::Result<()> {
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform([("last_used_at", last_used_at).into()])
            .ids(api_key_db_id)
            .query(),
    )?;
    Ok(())
}

pub(crate) fn delete_all_for_user(
    db: &mut impl super::DbAccess,
    user_db_id: DbId,
) -> anyhow::Result<Vec<DbId>> {
    let api_keys = list_for_user(db, user_db_id)?;
    let ids: Vec<DbId> = api_keys
        .into_iter()
        .filter_map(|api_key| api_key.db_id.map(Into::into))
        .collect();

    if !ids.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(ids.clone()).query())?;
    }

    Ok(ids)
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;

    use super::*;
    use crate::db::{
        test_db::{
            new_test_db,
            test_user,
        },
        users,
    };

    fn create_test_user(db: &mut DbAny, username: &str) -> anyhow::Result<DbId> {
        users::create(db, &test_user(username)?)
    }

    #[test]
    fn get_by_public_id_rejects_other_element_types() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db, "alice")?;
        let api_key_db_id = create(&mut db, user_db_id, "laptop", "hash-typed", 1)?;
        let api_key = get_by_id(&db, api_key_db_id)?.ok_or_else(|| anyhow!("key must exist"))?;
        let user = users::get_by_id(&db, user_db_id)?.ok_or_else(|| anyhow!("user must exist"))?;

        assert_eq!(
            get_by_public_id(&db, &api_key.id)?.map(|api_key| api_key.name),
            Some("laptop".to_string())
        );
        assert!(get_by_public_id(&db, &user.id)?.is_none());
        assert!(!delete_by_id(&mut db, user_db_id)?);
        assert!(users::get_by_id(&db, user_db_id)?.is_some());
        assert!(delete_by_id(&mut db, api_key_db_id)?);
        assert!(get_by_public_id(&db, &api_key.id)?.is_none());

        Ok(())
    }

    #[test]
    fn create_api_key_links_key_to_user() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db, "alice")?;

        let api_key_db_id = create(&mut db, user_db_id, "laptop", "hash-1", 100)?;

        let api_key =
            get_by_id(&db, api_key_db_id)?.ok_or_else(|| anyhow!("api key should exist"))?;
        assert_eq!(api_key.name, "laptop");
        assert_eq!(api_key.key_hash, "hash-1");
        assert_eq!(get_owner_id(&db, api_key_db_id)?, Some(user_db_id));

        Ok(())
    }

    #[test]
    fn find_by_key_hash_returns_key() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db, "alice")?;
        let api_key_db_id = create(&mut db, user_db_id, "laptop", "hash-1", 100)?;

        let api_key = find_by_key_hash(&db, "hash-1")?
            .ok_or_else(|| anyhow!("api key should resolve by key hash"))?;

        assert_eq!(api_key.db_id.map(Into::into), Some(api_key_db_id));
        assert!(find_by_key_hash(&db, "missing")?.is_none());

        Ok(())
    }

    #[test]
    fn list_for_user_excludes_other_users_keys() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let alice = create_test_user(&mut db, "alice")?;
        let bob = create_test_user(&mut db, "bob")?;

        create(&mut db, alice, "alice laptop", "hash-a", 200)?;
        create(&mut db, bob, "bob laptop", "hash-b", 100)?;
        create(&mut db, alice, "alice phone", "hash-c", 300)?;

        let names: Vec<String> = list_for_user(&db, alice)?
            .into_iter()
            .map(|api_key| api_key.name)
            .collect();

        assert_eq!(names, vec!["alice laptop", "alice phone"]);

        Ok(())
    }

    #[test]
    fn delete_by_id_removes_key_and_owner_edge() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db, "alice")?;
        let api_key_db_id = create(&mut db, user_db_id, "laptop", "hash-1", 100)?;

        assert!(delete_by_id(&mut db, api_key_db_id)?);
        assert!(!delete_by_id(&mut db, api_key_db_id)?);
        assert!(get_by_id(&db, api_key_db_id)?.is_none());
        assert!(find_by_key_hash(&db, "hash-1")?.is_none());
        assert!(list_for_user(&db, user_db_id)?.is_empty());

        Ok(())
    }

    #[test]
    fn update_last_used_at_sets_timestamp() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db, "alice")?;
        let api_key_db_id = create(&mut db, user_db_id, "laptop", "hash-1", 100)?;

        update_last_used_at(&mut db, api_key_db_id, 250)?;

        let api_key =
            get_by_id(&db, api_key_db_id)?.ok_or_else(|| anyhow!("api key should exist"))?;
        assert_eq!(api_key.last_used_at, Some(250));

        Ok(())
    }
}
