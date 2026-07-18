// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    CountComparison,
    DbElement,
    DbId,
    DbValue,
    QueryBuilder,
};

use super::DbAccess;

const KIND_KEY: &str = "rating_kind";
const VALUE_KEY: &str = "rating_value";
const CREATED_AT_KEY: &str = "rating_created_at_ms";
const UPDATED_AT_KEY: &str = "rating_updated_at_ms";

pub(crate) const MIN_VALUE: u8 = 1;
pub(crate) const MAX_VALUE: u8 = 5;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RatingValue(u8);

impl RatingValue {
    pub(crate) fn new(value: u8) -> Option<Self> {
        (MIN_VALUE..=MAX_VALUE)
            .contains(&value)
            .then_some(Self(value))
    }

    pub(crate) fn get(self) -> u8 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RatingKind {
    Track,
    Release,
    Artist,
}

impl RatingKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Track => "track",
            Self::Release => "release",
            Self::Artist => "artist",
        }
    }
}

impl TryFrom<&str> for RatingKind {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "track" => Ok(Self::Track),
            "release" => Ok(Self::Release),
            "artist" => Ok(Self::Artist),
            _ => Err(()),
        }
    }
}

impl From<super::entities::MetadataEntityType> for RatingKind {
    fn from(value: super::entities::MetadataEntityType) -> Self {
        match value {
            super::entities::MetadataEntityType::Track => Self::Track,
            super::entities::MetadataEntityType::Release => Self::Release,
            super::entities::MetadataEntityType::Artist => Self::Artist,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RatingEdge {
    pub(crate) target_db_id: DbId,
    pub(crate) kind: RatingKind,
    pub(crate) value: RatingValue,
    pub(crate) created_at_ms: i64,
    pub(crate) updated_at_ms: i64,
}

pub(crate) fn upsert(
    db: &mut impl DbAccess,
    user_db_id: DbId,
    target_db_id: DbId,
    kind: RatingKind,
    value: RatingValue,
    now_ms: i64,
) -> anyhow::Result<()> {
    if let Some(edge_id) = find_rating_edge(db, user_db_id, target_db_id)? {
        db.exec_mut(
            QueryBuilder::insert()
                .values_uniform([
                    (KIND_KEY, kind.as_str()).into(),
                    (VALUE_KEY, u64::from(value.get())).into(),
                    (UPDATED_AT_KEY, now_ms).into(),
                ])
                .ids(edge_id)
                .query(),
        )?;
        return Ok(());
    }

    let edge_id = db
        .exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(user_db_id)
                .to(target_db_id)
                .query(),
        )?
        .ids()[0];
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform([
                (KIND_KEY, kind.as_str()).into(),
                (VALUE_KEY, u64::from(value.get())).into(),
                (CREATED_AT_KEY, now_ms).into(),
                (UPDATED_AT_KEY, now_ms).into(),
            ])
            .ids(edge_id)
            .query(),
    )?;
    Ok(())
}

pub(crate) fn get(
    db: &impl DbAccess,
    user_db_id: DbId,
    target_db_id: DbId,
) -> anyhow::Result<Option<RatingEdge>> {
    for element in read_outbound_rating_edges(db, user_db_id)? {
        if element.to == target_db_id {
            return Ok(parse_rating_edge(element));
        }
    }
    Ok(None)
}

#[cfg(test)]
fn list(db: &impl DbAccess, user_db_id: DbId) -> anyhow::Result<Vec<RatingEdge>> {
    Ok(read_outbound_rating_edges(db, user_db_id)?
        .into_iter()
        .filter_map(parse_rating_edge)
        .collect())
}

/// Idempotent.
pub(crate) fn remove(
    db: &mut impl DbAccess,
    user_db_id: DbId,
    target_db_id: DbId,
) -> anyhow::Result<bool> {
    let Some(edge_id) = find_rating_edge(db, user_db_id, target_db_id)? else {
        return Ok(false);
    };
    db.exec_mut(QueryBuilder::remove().ids(edge_id).query())?;
    Ok(true)
}

pub(crate) fn remove_outbound_for_user(
    db: &mut impl DbAccess,
    user_db_id: DbId,
) -> anyhow::Result<()> {
    let edge_ids: Vec<DbId> = read_outbound_rating_edges(db, user_db_id)?
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
    let edge_ids: Vec<DbId> = read_inbound_rating_edges(db, target_db_id)?
        .into_iter()
        .map(|element| element.id)
        .collect();
    if !edge_ids.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(edge_ids).query())?;
    }
    Ok(())
}

fn find_rating_edge(
    db: &impl DbAccess,
    user_db_id: DbId,
    target_db_id: DbId,
) -> anyhow::Result<Option<DbId>> {
    Ok(read_outbound_rating_edges(db, user_db_id)?
        .into_iter()
        .find(|element| element.to == target_db_id)
        .map(|element| element.id))
}

fn read_outbound_rating_edges(
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
        .filter(|element| element.from == user_db_id && element_is_rating(element))
        .collect())
}

fn read_inbound_rating_edges(
    db: &impl DbAccess,
    target_db_id: DbId,
) -> anyhow::Result<Vec<DbElement>> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .to(target_db_id)
            .where_()
            .edge()
            .end_where()
            .query(),
    )?;
    Ok(result
        .elements
        .into_iter()
        .filter(|element| element.to == target_db_id && element_is_rating(element))
        .collect())
}

fn element_is_rating(element: &DbElement) -> bool {
    element
        .values
        .iter()
        .any(|kv| matches!(&kv.key, DbValue::String(key) if key == KIND_KEY))
}

fn parse_rating_edge(element: DbElement) -> Option<RatingEdge> {
    if element.id.0 >= 0 {
        return None;
    }
    let target_db_id = (element.to.0 != 0).then_some(element.to)?;
    let mut kind = None;
    let mut value = None;
    let mut created_at_ms = None;
    let mut updated_at_ms = None;

    for kv in &element.values {
        let DbValue::String(key) = &kv.key else {
            continue;
        };
        match key.as_str() {
            KIND_KEY => {
                if let DbValue::String(raw) = &kv.value {
                    kind = RatingKind::try_from(raw.as_str()).ok();
                }
            }
            VALUE_KEY => value = db_value_to_rating(&kv.value),
            CREATED_AT_KEY => created_at_ms = db_value_to_i64(&kv.value),
            UPDATED_AT_KEY => updated_at_ms = db_value_to_i64(&kv.value),
            _ => {}
        }
    }

    Some(RatingEdge {
        target_db_id,
        kind: kind?,
        value: value?,
        created_at_ms: created_at_ms?,
        updated_at_ms: updated_at_ms?,
    })
}

fn db_value_to_rating(value: &DbValue) -> Option<RatingValue> {
    let value = match value {
        DbValue::U64(value) => u8::try_from(*value).ok()?,
        DbValue::I64(value) => u8::try_from(*value).ok()?,
        _ => return None,
    };
    RatingValue::new(value)
}

fn db_value_to_i64(value: &DbValue) -> Option<i64> {
    match value {
        DbValue::I64(value) => Some(*value),
        DbValue::U64(value) => i64::try_from(*value).ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::new_test_db;
    use agdb::DbAny;

    fn create_node(db: &mut DbAny) -> anyhow::Result<DbId> {
        Ok(db
            .exec_mut(QueryBuilder::insert().nodes().count(1).query())?
            .ids()[0])
    }

    #[test]
    fn upsert_preserves_created_at_and_updates_value() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_node(&mut db)?;
        let target = create_node(&mut db)?;

        upsert(
            &mut db,
            user,
            target,
            RatingKind::Track,
            RatingValue::new(2).unwrap(),
            100,
        )?;
        upsert(
            &mut db,
            user,
            target,
            RatingKind::Track,
            RatingValue::new(5).unwrap(),
            200,
        )?;

        let ratings = list(&db, user)?;
        assert_eq!(ratings.len(), 1);
        assert_eq!(ratings[0].value.get(), 5);
        assert_eq!(ratings[0].created_at_ms, 100);
        assert_eq!(ratings[0].updated_at_ms, 200);
        Ok(())
    }

    #[test]
    fn ratings_are_isolated_per_user() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_a = create_node(&mut db)?;
        let user_b = create_node(&mut db)?;
        let target = create_node(&mut db)?;
        upsert(
            &mut db,
            user_a,
            target,
            RatingKind::Track,
            RatingValue::new(4).unwrap(),
            100,
        )?;

        assert!(get(&db, user_b, target)?.is_none());
        assert!(list(&db, user_b)?.is_empty());
        Ok(())
    }

    #[test]
    fn remove_is_idempotent() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_node(&mut db)?;
        let target = create_node(&mut db)?;
        upsert(
            &mut db,
            user,
            target,
            RatingKind::Track,
            RatingValue::new(4).unwrap(),
            100,
        )?;

        assert!(remove(&mut db, user, target)?);
        assert!(!remove(&mut db, user, target)?);
        Ok(())
    }

    #[test]
    fn cleanup_only_removes_rating_edges() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_node(&mut db)?;
        let target = create_node(&mut db)?;
        upsert(
            &mut db,
            user,
            target,
            RatingKind::Track,
            RatingValue::new(4).unwrap(),
            100,
        )?;
        let unrelated_edge = db
            .exec_mut(QueryBuilder::insert().edges().from(user).to(target).query())?
            .ids()[0];

        remove_outbound_for_user(&mut db, user)?;

        assert!(list(&db, user)?.is_empty());
        assert_eq!(
            db.exec(QueryBuilder::select().ids(unrelated_edge).query())?
                .elements
                .len(),
            1,
        );
        Ok(())
    }

    #[test]
    fn invalid_stored_value_is_ignored() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_node(&mut db)?;
        let target = create_node(&mut db)?;
        let edge = db
            .exec_mut(QueryBuilder::insert().edges().from(user).to(target).query())?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .values_uniform([
                    (KIND_KEY, "track").into(),
                    (VALUE_KEY, 99_u64).into(),
                    (CREATED_AT_KEY, 100_i64).into(),
                    (UPDATED_AT_KEY, 100_i64).into(),
                ])
                .ids(edge)
                .query(),
        )?;

        assert!(list(&db, user)?.is_empty());
        Ok(())
    }
}
