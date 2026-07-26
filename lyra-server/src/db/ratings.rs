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

const KIND_KEY: &str = "rating_kind";
const VALUE_KEY: &str = "rating_value";
const CREATED_AT_KEY: &str = "rating_created_at_ms";
const UPDATED_AT_KEY: &str = "rating_updated_at_ms";

pub(crate) const MIN_VALUE: u8 = 1;
pub(crate) const MAX_VALUE: u8 = 5;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RatingFilter {
    min: Option<RatingValue>,
    max: Option<RatingValue>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct InvertedRatingFilter;

impl RatingFilter {
    pub(crate) fn new(
        min: Option<RatingValue>,
        max: Option<RatingValue>,
    ) -> Result<Self, InvertedRatingFilter> {
        if min.zip(max).is_some_and(|(min, max)| min > max) {
            return Err(InvertedRatingFilter);
        }
        Ok(Self { min, max })
    }

    pub(crate) fn is_empty(self) -> bool {
        self.min.is_none() && self.max.is_none()
    }

    pub(crate) fn bounds(self) -> (Option<u8>, Option<u8>) {
        (
            self.min.map(RatingValue::get),
            self.max.map(RatingValue::get),
        )
    }

    fn contains(self, value: RatingValue) -> bool {
        self.min.is_none_or(|min| value >= min) && self.max.is_none_or(|max| value <= max)
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

pub(crate) fn values_for_targets(
    db: &impl DbAccess,
    user_db_id: DbId,
    target_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, RatingEdge>> {
    if target_db_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let requested: HashSet<DbId> = target_db_ids.iter().copied().collect();
    Ok(read_outbound_rating_edges(db, user_db_id)?
        .into_iter()
        .filter_map(parse_rating_edge)
        .filter(|edge| requested.contains(&edge.target_db_id))
        .map(|edge| (edge.target_db_id, edge))
        .collect())
}

pub(crate) fn target_ids_matching(
    db: &impl DbAccess,
    user_db_id: DbId,
    filter: RatingFilter,
) -> anyhow::Result<HashSet<DbId>> {
    anyhow::ensure!(
        !filter.is_empty(),
        "ratings::target_ids_matching requires at least one bound",
    );
    Ok(read_outbound_rating_edges(db, user_db_id)?
        .into_iter()
        .filter_map(parse_rating_edge)
        .filter(|edge| filter.contains(edge.value))
        .map(|edge| edge.target_db_id)
        .collect())
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
    fn values_for_targets_subsets_once_and_preserves_rating_state() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_a = create_node(&mut db)?;
        let user_b = create_node(&mut db)?;
        let track = create_node(&mut db)?;
        let artist = create_node(&mut db)?;
        let other_user_target = create_node(&mut db)?;
        let unrated = create_node(&mut db)?;

        upsert(
            &mut db,
            user_a,
            track,
            RatingKind::Track,
            RatingValue::new(2).unwrap(),
            100,
        )?;
        upsert(
            &mut db,
            user_a,
            artist,
            RatingKind::Artist,
            RatingValue::new(5).unwrap(),
            200,
        )?;
        upsert(
            &mut db,
            user_b,
            other_user_target,
            RatingKind::Release,
            RatingValue::new(4).unwrap(),
            300,
        )?;

        assert!(values_for_targets(&db, user_a, &[])?.is_empty());
        let values = values_for_targets(&db, user_a, &[track, other_user_target, unrated])?;
        assert_eq!(values.len(), 1);
        let edge = values.get(&track).expect("requested track rating");
        assert_eq!(edge.kind, RatingKind::Track);
        assert_eq!(edge.value.get(), 2);
        assert_eq!(edge.created_at_ms, 100);
        assert_eq!(edge.updated_at_ms, 100);
        assert!(
            !values.contains_key(&artist),
            "unrequested rating must be omitted"
        );
        assert!(
            !values.contains_key(&other_user_target),
            "another user's rating must be omitted",
        );
        Ok(())
    }

    #[test]
    fn target_ids_matching_applies_inclusive_range_per_user() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_a = create_node(&mut db)?;
        let user_b = create_node(&mut db)?;
        let low = create_node(&mut db)?;
        let middle = create_node(&mut db)?;
        let upper = create_node(&mut db)?;
        let high = create_node(&mut db)?;

        for (target, value) in [(low, 2), (middle, 3), (upper, 4), (high, 5)] {
            upsert(
                &mut db,
                user_a,
                target,
                RatingKind::Track,
                RatingValue::new(value).unwrap(),
                100,
            )?;
        }
        upsert(
            &mut db,
            user_b,
            high,
            RatingKind::Track,
            RatingValue::new(4).unwrap(),
            100,
        )?;

        let filter = RatingFilter::new(RatingValue::new(3), RatingValue::new(4)).unwrap();
        let matches = target_ids_matching(&db, user_a, filter)?;
        assert_eq!(matches, HashSet::from([middle, upper]));

        let exact = RatingFilter::new(RatingValue::new(4), RatingValue::new(4)).unwrap();
        assert_eq!(
            target_ids_matching(&db, user_a, exact)?,
            HashSet::from([upper]),
        );

        let at_most = RatingFilter::new(None, RatingValue::new(2)).unwrap();
        assert_eq!(
            target_ids_matching(&db, user_a, at_most)?,
            HashSet::from([low]),
        );
        Ok(())
    }

    #[test]
    fn rating_filter_accepts_single_and_equal_bounds() {
        assert!(RatingFilter::new(RatingValue::new(1), None).is_ok());
        assert!(RatingFilter::new(None, RatingValue::new(5)).is_ok());
        assert!(RatingFilter::new(RatingValue::new(3), RatingValue::new(3)).is_ok());
        assert!(RatingFilter::new(RatingValue::new(4), RatingValue::new(3)).is_err());
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
