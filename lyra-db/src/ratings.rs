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
use super::graph::RetargetLookup;

const VALUE_KEY: &str = "rating_value";
const CREATED_AT_KEY: &str = "rating_created_at_ms";
const UPDATED_AT_KEY: &str = "rating_updated_at_ms";

pub const MIN_VALUE: u8 = 1;
pub const MAX_VALUE: u8 = 5;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RatingValue(u8);

impl RatingValue {
    pub fn new(value: u8) -> Option<Self> {
        (MIN_VALUE..=MAX_VALUE)
            .contains(&value)
            .then_some(Self(value))
    }

    pub fn get(self) -> u8 {
        self.0
    }
}

/// Inclusive bounds on a rating, the minimum never above the maximum.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RatingBounds {
    min: Option<RatingValue>,
    max: Option<RatingValue>,
}

impl RatingBounds {
    pub fn new(
        min: Option<RatingValue>,
        max: Option<RatingValue>,
    ) -> Result<Self, InvalidRatingFilter> {
        if min.zip(max).is_some_and(|(min, max)| min > max) {
            return Err(InvalidRatingFilter::Inverted);
        }
        Ok(Self { min, max })
    }

    pub fn is_unbounded(self) -> bool {
        self.min.is_none() && self.max.is_none()
    }

    pub fn get(self) -> (Option<u8>, Option<u8>) {
        (
            self.min.map(RatingValue::get),
            self.max.map(RatingValue::get),
        )
    }

    fn contains(self, value: RatingValue) -> bool {
        self.min.is_none_or(|min| value >= min) && self.max.is_none_or(|max| value <= max)
    }
}

/// Which of a user's rated and unrated targets a query keeps.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RatingFilter {
    #[default]
    Any,
    /// Rated targets within the bounds.
    Rated(RatingBounds),
    Unrated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InvalidRatingFilter {
    #[error("the minimum rating exceeds the maximum")]
    Inverted,
    #[error("rating bounds keep only rated targets")]
    BoundsOnUnrated,
}

impl RatingFilter {
    /// `rated` keeps rated (`true`) or unrated (`false`) targets; a bound implies rated.
    pub fn new(
        rated: Option<bool>,
        min: Option<RatingValue>,
        max: Option<RatingValue>,
    ) -> Result<Self, InvalidRatingFilter> {
        let bounds = RatingBounds::new(min, max)?;
        match rated {
            Some(false) if !bounds.is_unbounded() => Err(InvalidRatingFilter::BoundsOnUnrated),
            Some(false) => Ok(Self::Unrated),
            None if bounds.is_unbounded() => Ok(Self::Any),
            _ => Ok(Self::Rated(bounds)),
        }
    }

    pub fn is_any(self) -> bool {
        self == Self::Any
    }

    pub fn rated(self) -> Option<bool> {
        match self {
            Self::Any => None,
            Self::Rated(_) => Some(true),
            Self::Unrated => Some(false),
        }
    }

    pub fn bounds(self) -> (Option<u8>, Option<u8>) {
        match self {
            Self::Rated(bounds) => bounds.get(),
            Self::Any | Self::Unrated => (None, None),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RatingEdge {
    pub(crate) target_db_id: DbId,
    pub value: RatingValue,
    pub(crate) created_at_ms: i64,
    pub updated_at_ms: i64,
}

pub fn upsert(
    db: &mut impl DbAccess,
    user_db_id: DbId,
    target_db_id: DbId,
    value: RatingValue,
    now_ms: i64,
) -> anyhow::Result<()> {
    if let Some(edge_id) = find_rating_edge(db, user_db_id, target_db_id)? {
        db.exec_mut(
            QueryBuilder::insert()
                .values_uniform([
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
                (VALUE_KEY, u64::from(value.get())).into(),
                (CREATED_AT_KEY, now_ms).into(),
                (UPDATED_AT_KEY, now_ms).into(),
            ])
            .ids(edge_id)
            .query(),
    )?;
    Ok(())
}

pub fn get(
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

pub fn values_for_targets(
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

/// The mean of every user's rating of each target; unrated targets are absent.
pub fn averages_for_targets(
    db: &impl DbAccess,
    target_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, f64>> {
    let mut averages = HashMap::new();
    for &target_db_id in target_db_ids {
        let (sum, count) = read_inbound_rating_edges(db, target_db_id)?
            .into_iter()
            .filter_map(parse_rating_edge)
            .fold((0_u64, 0_u64), |(sum, count), edge| {
                (sum + u64::from(edge.value.get()), count + 1)
            });
        if count > 0 {
            averages.insert(target_db_id, sum as f64 / count as f64);
        }
    }
    Ok(averages)
}

/// The targets the user rated within `bounds`.
pub fn rated_target_ids(
    db: &impl DbAccess,
    user_db_id: DbId,
    bounds: RatingBounds,
) -> anyhow::Result<HashSet<DbId>> {
    Ok(read_outbound_rating_edges(db, user_db_id)?
        .into_iter()
        .filter_map(parse_rating_edge)
        .filter(|edge| bounds.contains(edge.value))
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
pub fn remove(
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

/// Moves the ratings of `from` onto `to`. A user who rated both keeps the rating they changed
/// last, created at the earlier of the two times.
pub fn retarget(
    db: &mut impl DbAccess,
    from: DbId,
    to: DbId,
    lookup: RetargetLookup,
) -> anyhow::Result<()> {
    let moved_edges = read_inbound_rating_edges(db, from)?;
    if moved_edges.is_empty() {
        return Ok(());
    }
    let on_target: Option<HashMap<DbId, DbId>> = match lookup {
        RetargetLookup::Owners => None,
        RetargetLookup::Target => Some(
            read_inbound_rating_edges(db, to)?
                .into_iter()
                .map(|element| (element.from, element.id))
                .collect(),
        ),
    };
    for element in moved_edges {
        let user_db_id = element.from;
        let edge_id = element.id;
        let Some(moved) = parse_rating_edge(element) else {
            continue;
        };
        let existing = match &on_target {
            Some(on_target) => on_target.get(&user_db_id).copied(),
            None => find_rating_edge(db, user_db_id, to)?,
        };
        let (edge, kept) = match existing {
            Some(existing) => (
                existing,
                parse_rating_edge(
                    db.exec(QueryBuilder::select().ids(existing).query())?
                        .elements
                        .remove(0),
                ),
            ),
            None => (
                db.exec_mut(
                    QueryBuilder::insert()
                        .edges()
                        .from(user_db_id)
                        .to(to)
                        .query(),
                )?
                .ids()[0],
                None,
            ),
        };
        let (value, updated_at_ms) = match &kept {
            Some(kept) if kept.updated_at_ms >= moved.updated_at_ms => {
                (kept.value, kept.updated_at_ms)
            }
            _ => (moved.value, moved.updated_at_ms),
        };
        let created_at_ms = kept.map_or(moved.created_at_ms, |kept| {
            kept.created_at_ms.min(moved.created_at_ms)
        });
        db.exec_mut(
            QueryBuilder::insert()
                .values_uniform([
                    (VALUE_KEY, u64::from(value.get())).into(),
                    (CREATED_AT_KEY, created_at_ms).into(),
                    (UPDATED_AT_KEY, updated_at_ms).into(),
                ])
                .ids(edge)
                .query(),
        )?;
        db.exec_mut(QueryBuilder::remove().ids(edge_id).query())?;
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
            .and()
            .distance(CountComparison::Equal(1))
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
        .any(|kv| matches!(&kv.key, DbValue::String(key) if key == VALUE_KEY))
}

fn parse_rating_edge(element: DbElement) -> Option<RatingEdge> {
    if element.id.0 >= 0 {
        return None;
    }
    let target_db_id = (element.to.0 != 0).then_some(element.to)?;
    let mut value = None;
    let mut created_at_ms = None;
    let mut updated_at_ms = None;

    for kv in &element.values {
        let DbValue::String(key) = &kv.key else {
            continue;
        };
        match key.as_str() {
            VALUE_KEY => value = db_value_to_rating(&kv.value),
            CREATED_AT_KEY => created_at_ms = db_value_to_i64(&kv.value),
            UPDATED_AT_KEY => updated_at_ms = db_value_to_i64(&kv.value),
            _ => {}
        }
    }

    Some(RatingEdge {
        target_db_id,
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
    use crate::test_db::new_test_db;
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

        upsert(&mut db, user, target, RatingValue::new(2).unwrap(), 100)?;
        upsert(&mut db, user, target, RatingValue::new(5).unwrap(), 200)?;

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
        upsert(&mut db, user_a, target, RatingValue::new(4).unwrap(), 100)?;

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

        upsert(&mut db, user_a, track, RatingValue::new(2).unwrap(), 100)?;
        upsert(&mut db, user_a, artist, RatingValue::new(5).unwrap(), 200)?;
        upsert(
            &mut db,
            user_b,
            other_user_target,
            RatingValue::new(4).unwrap(),
            300,
        )?;

        assert!(values_for_targets(&db, user_a, &[])?.is_empty());
        let values = values_for_targets(&db, user_a, &[track, other_user_target, unrated])?;
        assert_eq!(values.len(), 1);
        let edge = values.get(&track).expect("requested track rating");
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
    fn averages_for_targets_take_the_mean_across_users() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_a = create_node(&mut db)?;
        let user_b = create_node(&mut db)?;
        let user_c = create_node(&mut db)?;
        let shared = create_node(&mut db)?;
        let single = create_node(&mut db)?;
        let unrated = create_node(&mut db)?;

        for (user, value) in [(user_a, 2), (user_b, 5), (user_c, 4)] {
            upsert(&mut db, user, shared, RatingValue::new(value).unwrap(), 100)?;
        }
        upsert(&mut db, user_a, single, RatingValue::new(3).unwrap(), 100)?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(user_b)
                .to(single)
                .query(),
        )?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(user_c)
                .to(unrated)
                .values([[("other_value", 1_u64).into()]])
                .query(),
        )?;

        assert!(averages_for_targets(&db, &[])?.is_empty());
        let averages = averages_for_targets(&db, &[shared, single, unrated])?;
        assert_eq!(
            averages,
            HashMap::from([(shared, 11.0 / 3.0), (single, 3.0)])
        );
        Ok(())
    }

    #[test]
    fn rated_target_ids_applies_inclusive_bounds_per_user() -> anyhow::Result<()> {
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
                RatingValue::new(value).unwrap(),
                100,
            )?;
        }
        upsert(&mut db, user_b, high, RatingValue::new(4).unwrap(), 100)?;

        let bounds = |min, max| RatingBounds::new(RatingValue::new(min), RatingValue::new(max));
        assert_eq!(
            rated_target_ids(&db, user_a, bounds(3, 4)?)?,
            HashSet::from([middle, upper]),
        );
        assert_eq!(
            rated_target_ids(&db, user_a, bounds(4, 4)?)?,
            HashSet::from([upper]),
        );
        assert_eq!(
            rated_target_ids(&db, user_a, RatingBounds::new(None, RatingValue::new(2))?)?,
            HashSet::from([low]),
        );
        assert_eq!(
            rated_target_ids(&db, user_b, RatingBounds::default())?,
            HashSet::from([high]),
        );
        Ok(())
    }

    #[test]
    fn rating_bounds_accept_single_and_equal_bounds() {
        assert!(RatingBounds::new(RatingValue::new(1), None).is_ok());
        assert!(RatingBounds::new(None, RatingValue::new(5)).is_ok());
        assert!(RatingBounds::new(RatingValue::new(3), RatingValue::new(3)).is_ok());
        assert_eq!(
            RatingBounds::new(RatingValue::new(4), RatingValue::new(3)),
            Err(InvalidRatingFilter::Inverted),
        );
    }

    #[test]
    fn rating_filter_reads_rated_alongside_bounds() {
        assert_eq!(RatingFilter::new(None, None, None), Ok(RatingFilter::Any));
        assert_eq!(
            RatingFilter::new(Some(true), None, None),
            Ok(RatingFilter::Rated(RatingBounds::default())),
        );
        assert_eq!(
            RatingFilter::new(Some(true), RatingValue::new(2), None),
            RatingFilter::new(None, RatingValue::new(2), None),
        );
        assert_eq!(
            RatingFilter::new(Some(false), None, None),
            Ok(RatingFilter::Unrated)
        );
        assert_eq!(
            RatingFilter::new(Some(false), RatingValue::new(1), None),
            Err(InvalidRatingFilter::BoundsOnUnrated),
        );
        assert_eq!(
            RatingFilter::new(None, RatingValue::new(4), RatingValue::new(3)),
            Err(InvalidRatingFilter::Inverted),
        );
    }

    #[test]
    fn remove_is_idempotent() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_node(&mut db)?;
        let target = create_node(&mut db)?;
        upsert(&mut db, user, target, RatingValue::new(4).unwrap(), 100)?;

        assert!(remove(&mut db, user, target)?);
        assert!(!remove(&mut db, user, target)?);
        Ok(())
    }

    #[test]
    fn cleanup_only_removes_rating_edges() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_node(&mut db)?;
        let target = create_node(&mut db)?;
        upsert(&mut db, user, target, RatingValue::new(4).unwrap(), 100)?;
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
