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
    DbType,
    DbValue,
    QueryBuilder,
};

use super::DbAccess;

pub(crate) fn direct_edges_from(
    db: &impl DbAccess,
    from_id: DbId,
) -> anyhow::Result<Vec<DbElement>> {
    let result = db.exec(
        QueryBuilder::search()
            .from(from_id)
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(1))
            .query(),
    )?;

    Ok(result
        .elements
        .into_iter()
        .filter(|element| element.from == Some(from_id))
        .collect())
}

pub(crate) fn direct_edge_ids(
    db: &impl DbAccess,
    from: DbId,
    to: DbId,
) -> anyhow::Result<Vec<DbId>> {
    Ok(direct_edges_from(db, from)?
        .into_iter()
        .filter_map(|element| (element.to == Some(to)).then_some(element.id))
        .collect())
}

pub(crate) fn edge_exists(db: &impl DbAccess, from: DbId, to: DbId) -> anyhow::Result<bool> {
    Ok(!direct_edge_ids(db, from, to)?.is_empty())
}

pub(crate) fn remove_edges_between(
    db: &mut impl DbAccess,
    from: DbId,
    to: DbId,
) -> anyhow::Result<()> {
    let edge_ids = direct_edge_ids(db, from, to)?;
    if !edge_ids.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(&edge_ids).query())?;
    }

    Ok(())
}

pub(crate) fn ensure_owned_edge(
    db: &mut impl DbAccess,
    from: DbId,
    to: DbId,
) -> anyhow::Result<()> {
    if !edge_exists(db, from, to)? {
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(from)
                .to(to)
                .values_uniform([("owned", 1).into()])
                .query(),
        )?;
    }

    Ok(())
}

pub(crate) fn edge_count_map(
    db: &impl DbAccess,
    ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, u64>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }

    let result = db.exec(QueryBuilder::select().edge_count().ids(ids).query())?;
    let mut counts = HashMap::new();
    for element in result.elements {
        let count = element
            .values
            .iter()
            .find_map(|kv| {
                if matches!(&kv.key, DbValue::String(key) if key == "edge_count") {
                    match &kv.value {
                        DbValue::U64(value) => Some(*value),
                        DbValue::I64(value) => Some((*value).max(0) as u64),
                        _ => None,
                    }
                } else {
                    None
                }
            })
            .unwrap_or(0);
        counts.insert(element.id, count);
    }

    Ok(counts)
}

/// Result of [`collect_related_ids_by_owner`]: per-owner related IDs and a
/// deduplicated list of all related IDs across all owners (for bulk fetch).
pub(crate) struct RelatedIdsByOwner {
    pub(crate) per_owner: HashMap<DbId, Vec<DbId>>,
    pub(crate) all_ids: Vec<DbId>,
}

/// Walk outgoing edges from each owner and collect the target node IDs.
///
/// `edge_filter` is called for each edge element and should return `true` if
/// the edge qualifies (e.g. checking for an `owned` key). This lets callers
/// apply different edge predicates without duplicating the traversal logic.
pub(crate) fn collect_related_ids_by_owner(
    db: &impl DbAccess,
    owner_ids: &[DbId],
    edge_filter: impl Fn(&DbElement) -> bool,
) -> anyhow::Result<RelatedIdsByOwner> {
    let mut per_owner: HashMap<DbId, Vec<DbId>> = owner_ids
        .iter()
        .copied()
        .map(|id| (id, Vec::new()))
        .collect();
    let mut all_ids = Vec::new();
    let mut seen_all = HashSet::new();

    for owner_id in owner_ids {
        let edges = db.exec(
            QueryBuilder::select()
                .search()
                .from(*owner_id)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .query(),
        )?;
        let Some(owner_related) = per_owner.get_mut(owner_id) else {
            continue;
        };
        let mut seen_owner = HashSet::new();
        for edge in edges.elements {
            if !edge_filter(&edge) {
                continue;
            }
            let related_id = match (edge.from, edge.to) {
                (Some(from), Some(to)) if from == *owner_id => Some(to),
                (Some(from), Some(to)) if to == *owner_id => Some(from),
                _ => None,
            };
            let Some(related_id) = related_id else {
                continue;
            };
            if related_id.0 <= 0 || related_id == *owner_id || !seen_owner.insert(related_id) {
                continue;
            }
            owner_related.push(related_id);
            if seen_all.insert(related_id) {
                all_ids.push(related_id);
            }
        }
    }

    Ok(RelatedIdsByOwner { per_owner, all_ids })
}

fn element_has_discriminator(element: &DbElement, key: &DbValue, value: &DbValue) -> bool {
    element
        .values
        .iter()
        .any(|kv| kv.key == *key && kv.value == *value)
}

pub(crate) fn is_element_type(element: &DbElement, discriminator: &str) -> bool {
    element_has_discriminator(
        element,
        &DbValue::from("db_element_id"),
        &DbValue::from(discriminator),
    )
}

/// Fetch a single node by ID, returning `None` if the ID does not exist or the
/// node is not of the expected `DbElement` discriminator type.
pub(crate) fn fetch_typed_by_id<T: DbType<ValueType = T>>(
    db: &impl DbAccess,
    id: DbId,
    discriminator: &str,
) -> anyhow::Result<Option<T>> {
    let result = match db.exec(QueryBuilder::select().ids(id).query()) {
        Ok(result) => result,
        Err(err) => {
            tracing::warn!(
                error = %err,
                id = ?id,
                discriminator,
                "fetch_typed_by_id: db.exec failed; treating as missing",
            );
            return Ok(None);
        }
    };
    let Some(element) = result.elements.into_iter().next() else {
        return Ok(None);
    };
    if !is_element_type(&element, discriminator) {
        return Ok(None);
    }
    Ok(Some(T::from_db_element(&element)?))
}

/// Bulk-fetch nodes by ID, filter to a single `DbElement` discriminator type,
/// and deserialize using `from_db_element`. Returns a map from `DbId` to the
/// deserialized value.
pub(crate) fn bulk_fetch_typed<T: DbType<ValueType = T>>(
    db: &impl DbAccess,
    ids: Vec<DbId>,
    discriminator: &str,
) -> anyhow::Result<HashMap<DbId, T>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let key = DbValue::from("db_element_id");
    let value = DbValue::from(discriminator);
    let elements = db.exec(QueryBuilder::select().ids(ids).query())?.elements;
    let mut map: HashMap<DbId, T> = HashMap::new();
    for element in elements {
        if !element_has_discriminator(&element, &key, &value) {
            continue;
        }
        map.insert(element.id, T::from_db_element(&element)?);
    }
    Ok(map)
}
