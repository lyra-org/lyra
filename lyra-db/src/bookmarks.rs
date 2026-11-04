// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};

use agdb::{
    CountComparison,
    DbElement,
    DbId,
    DbValue,
    QueryBuilder,
};

use super::DbAccess;

const POSITION_KEY: &str = "bookmark_position_ms";
const COMMENT_KEY: &str = "bookmark_comment";
const CREATED_AT_KEY: &str = "bookmark_created_at_ms";
const UPDATED_AT_KEY: &str = "bookmark_updated_at_ms";

/// The most characters a bookmark comment holds.
pub const MAX_COMMENT_CHARS: usize = 1000;

/// A user's saved place in a track.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bookmark {
    pub position_ms: u64,
    pub comment: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Sets the user's bookmark of the target, keeping when it was first created. An empty
/// comment is stored as none.
pub fn upsert(
    db: &mut impl DbAccess,
    user_db_id: DbId,
    target_db_id: DbId,
    position_ms: u64,
    comment: Option<&str>,
    now_ms: i64,
) -> anyhow::Result<()> {
    let comment = comment.filter(|comment| !comment.is_empty());
    if comment.is_some_and(|comment| comment.chars().count() > MAX_COMMENT_CHARS) {
        anyhow::bail!(
            "db::bookmarks::upsert invariant: comment exceeds {MAX_COMMENT_CHARS} characters"
        );
    }
    let (edge_id, created_at_ms) = match find_bookmark_edge(db, user_db_id, target_db_id)? {
        Some(element) => {
            let created_at_ms = parse_bookmark(&element).map_or(now_ms, |b| b.created_at_ms);
            (element.id, created_at_ms)
        }
        None => (
            db.exec_mut(
                QueryBuilder::insert()
                    .edges()
                    .from(user_db_id)
                    .to(target_db_id)
                    .query(),
            )?
            .ids()[0],
            now_ms,
        ),
    };
    write_values(db, edge_id, position_ms, comment, created_at_ms, now_ms)
}

fn write_values(
    db: &mut impl DbAccess,
    edge_id: DbId,
    position_ms: u64,
    comment: Option<&str>,
    created_at_ms: i64,
    updated_at_ms: i64,
) -> anyhow::Result<()> {
    let mut values = vec![
        (POSITION_KEY, position_ms).into(),
        (CREATED_AT_KEY, created_at_ms).into(),
        (UPDATED_AT_KEY, updated_at_ms).into(),
    ];
    match comment {
        Some(comment) => values.push((COMMENT_KEY, comment).into()),
        None => {
            db.exec_mut(
                QueryBuilder::remove()
                    .values([COMMENT_KEY])
                    .ids(edge_id)
                    .query(),
            )?;
        }
    }
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform(values)
            .ids(edge_id)
            .query(),
    )?;
    Ok(())
}

pub fn get(
    db: &impl DbAccess,
    user_db_id: DbId,
    target_db_id: DbId,
) -> anyhow::Result<Option<Bookmark>> {
    Ok(find_bookmark_edge(db, user_db_id, target_db_id)?
        .as_ref()
        .and_then(parse_bookmark))
}

/// The user's bookmarks of the targets; targets without one are absent.
pub fn for_targets(
    db: &impl DbAccess,
    user_db_id: DbId,
    target_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Bookmark>> {
    if target_db_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let requested: HashSet<DbId> = target_db_ids.iter().copied().collect();
    Ok(read_outbound_bookmark_edges(db, user_db_id)?
        .into_iter()
        .filter(|element| requested.contains(&element.to))
        .filter_map(|element| Some((element.to, parse_bookmark(&element)?)))
        .collect())
}

/// When the user last changed each of their bookmarks, keyed by target.
pub fn updated_at(db: &impl DbAccess, user_db_id: DbId) -> anyhow::Result<HashMap<DbId, i64>> {
    Ok(read_outbound_bookmark_edges(db, user_db_id)?
        .into_iter()
        .filter_map(|element| Some((element.to, parse_bookmark(&element)?.updated_at_ms)))
        .collect())
}

/// The targets the user bookmarked.
pub fn target_ids(db: &impl DbAccess, user_db_id: DbId) -> anyhow::Result<HashSet<DbId>> {
    Ok(updated_at(db, user_db_id)?.into_keys().collect())
}

/// Idempotent.
pub fn remove(
    db: &mut impl DbAccess,
    user_db_id: DbId,
    target_db_id: DbId,
) -> anyhow::Result<bool> {
    let Some(element) = find_bookmark_edge(db, user_db_id, target_db_id)? else {
        return Ok(false);
    };
    db.exec_mut(QueryBuilder::remove().ids(element.id).query())?;
    Ok(true)
}

pub(crate) fn remove_outbound_for_user(
    db: &mut impl DbAccess,
    user_db_id: DbId,
) -> anyhow::Result<()> {
    let edge_ids: Vec<DbId> = read_outbound_bookmark_edges(db, user_db_id)?
        .into_iter()
        .map(|element| element.id)
        .collect();
    if !edge_ids.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(edge_ids).query())?;
    }
    Ok(())
}

pub(crate) fn remove_inbound_for_target(
    db: &mut impl DbAccess,
    target_db_id: DbId,
) -> anyhow::Result<()> {
    let edge_ids: Vec<DbId> = read_inbound_bookmark_edges(db, target_db_id)?
        .into_iter()
        .map(|element| element.id)
        .collect();
    if !edge_ids.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(edge_ids).query())?;
    }
    Ok(())
}

fn find_bookmark_edge(
    db: &impl DbAccess,
    user_db_id: DbId,
    target_db_id: DbId,
) -> anyhow::Result<Option<DbElement>> {
    Ok(read_outbound_bookmark_edges(db, user_db_id)?
        .into_iter()
        .find(|element| element.to == target_db_id))
}

fn read_outbound_bookmark_edges(
    db: &impl DbAccess,
    user_db_id: DbId,
) -> anyhow::Result<Vec<DbElement>> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .from(user_db_id)
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
        .filter(|element| element.from == user_db_id && element_is_bookmark(element))
        .collect())
}

fn read_inbound_bookmark_edges(
    db: &impl DbAccess,
    target_db_id: DbId,
) -> anyhow::Result<Vec<DbElement>> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .to(target_db_id)
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
        .filter(|element| element.to == target_db_id && element_is_bookmark(element))
        .collect())
}

fn element_is_bookmark(element: &DbElement) -> bool {
    element.id.0 < 0
        && element
            .values
            .iter()
            .any(|kv| matches!(&kv.key, DbValue::String(key) if key == POSITION_KEY))
}

fn parse_bookmark(element: &DbElement) -> Option<Bookmark> {
    let mut position_ms = None;
    let mut comment = None;
    let mut created_at_ms = None;
    let mut updated_at_ms = None;
    for kv in &element.values {
        let DbValue::String(key) = &kv.key else {
            continue;
        };
        match (key.as_str(), &kv.value) {
            (POSITION_KEY, DbValue::U64(value)) => position_ms = Some(*value),
            (COMMENT_KEY, DbValue::String(value)) => comment = Some(value.clone()),
            (CREATED_AT_KEY, DbValue::I64(value)) => created_at_ms = Some(*value),
            (UPDATED_AT_KEY, DbValue::I64(value)) => updated_at_ms = Some(*value),
            _ => {}
        }
    }
    Some(Bookmark {
        position_ms: position_ms?,
        comment,
        created_at_ms: created_at_ms?,
        updated_at_ms: updated_at_ms?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_db::new_test_db;
    use agdb::DbAny;

    fn create_node(db: &mut DbAny) -> anyhow::Result<DbId> {
        Ok(db
            .exec_mut(QueryBuilder::insert().nodes().count(1).query())?
            .ids()[0])
    }

    #[test]
    fn upsert_keeps_created_at_and_replaces_position_and_comment() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_node(&mut db)?;
        let target = create_node(&mut db)?;

        upsert(&mut db, user, target, 1_000, Some("intro"), 100)?;
        upsert(&mut db, user, target, 2_000, None, 200)?;

        assert_eq!(
            get(&db, user, target)?,
            Some(Bookmark {
                position_ms: 2_000,
                comment: None,
                created_at_ms: 100,
                updated_at_ms: 200,
            })
        );
        assert_eq!(target_ids(&db, user)?, HashSet::from([target]));
        Ok(())
    }

    #[test]
    fn empty_comment_is_stored_as_none() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_node(&mut db)?;
        let target = create_node(&mut db)?;

        upsert(&mut db, user, target, 0, Some(""), 100)?;

        assert_eq!(get(&db, user, target)?.unwrap().comment, None);
        Ok(())
    }

    #[test]
    fn comment_past_the_cap_is_refused() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_node(&mut db)?;
        let target = create_node(&mut db)?;
        let at_cap = "é".repeat(MAX_COMMENT_CHARS);

        upsert(&mut db, user, target, 0, Some(&at_cap), 100)?;
        assert!(upsert(&mut db, user, target, 0, Some(&format!("{at_cap}a")), 200).is_err());
        assert_eq!(get(&db, user, target)?.unwrap().comment, Some(at_cap));
        Ok(())
    }

    #[test]
    fn for_targets_returns_only_the_users_requested_bookmarks() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_a = create_node(&mut db)?;
        let user_b = create_node(&mut db)?;
        let first = create_node(&mut db)?;
        let second = create_node(&mut db)?;
        let unrequested = create_node(&mut db)?;
        upsert(&mut db, user_a, first, 10, None, 100)?;
        upsert(&mut db, user_a, unrequested, 20, None, 100)?;
        upsert(&mut db, user_b, second, 30, None, 100)?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(user_a)
                .to(second)
                .query(),
        )?;

        assert!(for_targets(&db, user_a, &[])?.is_empty());
        let found = for_targets(&db, user_a, &[first, second])?;
        assert_eq!(found.keys().copied().collect::<Vec<_>>(), vec![first]);
        assert_eq!(found[&first].position_ms, 10);
        Ok(())
    }

    #[test]
    fn remove_is_idempotent_and_cleanup_only_removes_bookmark_edges() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_node(&mut db)?;
        let other = create_node(&mut db)?;
        let target = create_node(&mut db)?;
        upsert(&mut db, user, target, 10, None, 100)?;

        assert!(remove(&mut db, user, target)?);
        assert!(!remove(&mut db, user, target)?);

        upsert(&mut db, user, target, 10, None, 100)?;
        upsert(&mut db, other, target, 10, None, 100)?;
        let unrelated_edge = db
            .exec_mut(QueryBuilder::insert().edges().from(user).to(target).query())?
            .ids()[0];
        remove_outbound_for_user(&mut db, user)?;
        assert!(get(&db, user, target)?.is_none());
        assert!(get(&db, other, target)?.is_some());
        remove_inbound_for_target(&mut db, target)?;
        assert!(get(&db, other, target)?.is_none());
        assert_eq!(
            db.exec(QueryBuilder::select().ids(unrelated_edge).query())?
                .elements
                .len(),
            1,
        );
        Ok(())
    }
}
