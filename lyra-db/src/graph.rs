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
    DbKeyValue,
    DbType,
    DbValue,
    QueryBuilder,
    QueryId,
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
        .filter(|element| element.from == from_id)
        .collect())
}

pub fn direct_edge_ids(db: &impl DbAccess, from: DbId, to: DbId) -> anyhow::Result<Vec<DbId>> {
    Ok(direct_edges_from(db, from)?
        .into_iter()
        .filter_map(|element| (element.to == to).then_some(element.id))
        .collect())
}

pub fn edge_exists(db: &impl DbAccess, from: DbId, to: DbId) -> anyhow::Result<bool> {
    Ok(!direct_edge_ids(db, from, to)?.is_empty())
}

pub fn remove_edges_between(db: &mut impl DbAccess, from: DbId, to: DbId) -> anyhow::Result<bool> {
    let edge_ids = direct_edge_ids(db, from, to)?;
    if edge_ids.is_empty() {
        return Ok(false);
    }

    db.exec_mut(QueryBuilder::remove().ids(&edge_ids).query())?;
    Ok(true)
}

pub fn ensure_owned_edge(db: &mut impl DbAccess, from: DbId, to: DbId) -> anyhow::Result<bool> {
    if edge_exists(db, from, to)? {
        return Ok(false);
    }

    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(from)
            .to(to)
            .values_uniform([("owned", 1).into()])
            .query(),
    )?;
    Ok(true)
}

pub fn edge_count_map(db: &impl DbAccess, ids: &[DbId]) -> anyhow::Result<HashMap<DbId, u64>> {
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

/// The `kind` nodes one outgoing edge from `from`.
pub fn neighbor_ids(
    db: &impl DbAccess,
    from: impl Into<QueryId>,
    kind: &str,
) -> anyhow::Result<Vec<DbId>> {
    Ok(db
        .exec(
            QueryBuilder::search()
                .from(from)
                .where_()
                .neighbor()
                .and()
                .key("db_element_id")
                .value(kind)
                .query(),
        )?
        .ids())
}

/// The `kind` nodes one incoming edge from `to`.
pub fn inbound_neighbor_ids(db: &impl DbAccess, to: DbId, kind: &str) -> anyhow::Result<Vec<DbId>> {
    Ok(db
        .exec(
            QueryBuilder::search()
                .to(to)
                .where_()
                .neighbor()
                .and()
                .key("db_element_id")
                .value(kind)
                .query(),
        )?
        .ids())
}

/// The ids that name an existing `kind` node, in order.
pub fn existing_ids(db: &impl DbAccess, ids: &[DbId], kind: &str) -> anyhow::Result<Vec<DbId>> {
    let nodes = ids
        .iter()
        .copied()
        .filter(|id| id.0 > 0)
        .collect::<Vec<_>>();
    if nodes.is_empty() {
        return Ok(Vec::new());
    }
    let elements = match db.exec(
        QueryBuilder::select()
            .values(["db_element_id"])
            .ids(&nodes)
            .query(),
    ) {
        Ok(result) => result.elements,
        Err(error) if error.ty == agdb::DbErrorType::NotFound => {
            let mut elements = Vec::new();
            for id in nodes {
                match db.exec(
                    QueryBuilder::select()
                        .values(["db_element_id"])
                        .ids(id)
                        .query(),
                ) {
                    Ok(result) => elements.extend(result.elements),
                    Err(error) if error.ty == agdb::DbErrorType::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
            elements
        }
        Err(error) => return Err(error.into()),
    };
    Ok(elements
        .into_iter()
        .filter(|element| is_element_type(element, kind))
        .map(|element| element.id)
        .collect())
}

/// Some stored values of one element.
pub struct Fields(Vec<DbKeyValue>);

impl Fields {
    fn get(&self, key: &str) -> Option<&DbValue> {
        self.0
            .iter()
            .find(|kv| kv.key.string().is_ok_and(|stored| stored == key))
            .map(|kv| &kv.value)
    }

    pub fn text(&self, key: &str) -> Option<&str> {
        self.get(key)?.string().ok().map(String::as_str)
    }

    /// Moves the text out of `key`, leaving a default value behind.
    pub fn take_text(&mut self, key: &str) -> Option<String> {
        let kv = self
            .0
            .iter_mut()
            .find(|kv| kv.key.string().is_ok_and(|stored| stored == key))?;
        match std::mem::take(&mut kv.value) {
            DbValue::String(text) => Some(text),
            other => {
                kv.value = other;
                None
            }
        }
    }

    pub fn number(&self, key: &str) -> Option<u64> {
        self.get(key)?.to_u64().ok()
    }
}

/// The values `keys` hold on each of the existing `ids`, leaving out any key an element lacks.
///
/// agdb rejects an explicit-id select of keys an element lacks, so every value is read in one
/// pass and the unwanted ones dropped; a pass costs about the same whatever it reads. Reading
/// in id order keeps the reads close together in storage.
pub fn select_fields(
    db: &impl DbAccess,
    ids: &[DbId],
    keys: &[&str],
) -> anyhow::Result<HashMap<DbId, Fields>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let mut ids = ids.to_vec();
    ids.sort_unstable();
    Ok(db
        .exec(QueryBuilder::select().ids(ids).query())?
        .elements
        .into_iter()
        .map(|mut element| {
            element.values.retain(|kv| {
                kv.key
                    .string()
                    .is_ok_and(|stored| keys.contains(&stored.as_str()))
            });
            (element.id, Fields(element.values))
        })
        .collect())
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
                (from, to) if from.0 != 0 && to.0 != 0 && from == *owner_id => Some(to),
                (from, to) if from.0 != 0 && to.0 != 0 && to == *owner_id => Some(from),
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
        Err(err) if err.ty == agdb::DbErrorType::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let Some(element) = result.elements.into_iter().next() else {
        return Ok(None);
    };
    if !is_element_type(&element, discriminator) {
        return Ok(None);
    }
    Ok(Some(T::from_db_element(&element)?))
}

/// Nodes of the expected `DbElement` discriminator type whose `index` key holds `value`, up to
/// `limit` (0 for all).
fn select_typed_by_index(
    db: &impl DbAccess,
    index: &str,
    value: &str,
    discriminator: &str,
    limit: u64,
) -> anyhow::Result<Vec<DbElement>> {
    Ok(db
        .exec(
            QueryBuilder::select()
                .search()
                .index(index)
                .value(value)
                .limit(limit)
                .where_()
                .node()
                .and()
                .key("db_element_id")
                .value(discriminator)
                .query(),
        )?
        .elements)
}

/// The node of the expected `DbElement` discriminator type whose `index` key holds `value` and that
/// links directly to `owner`.
pub(crate) fn find_owned_by_index(
    db: &impl DbAccess,
    index: &str,
    value: &str,
    discriminator: &str,
    owner: DbId,
) -> anyhow::Result<Option<DbId>> {
    let candidates = db
        .exec(
            QueryBuilder::search()
                .index(index)
                .value(value)
                .where_()
                .node()
                .and()
                .key("db_element_id")
                .value(discriminator)
                .query(),
        )?
        .ids();
    for candidate in candidates {
        if edge_exists(db, candidate, owner)? {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}

/// Fetch the first node of the expected `DbElement` discriminator type whose `index` key holds
/// `value`, or `None` when no such node exists.
pub(crate) fn fetch_typed_by_index<T: DbType<ValueType = T>>(
    db: &impl DbAccess,
    index: &str,
    value: &str,
    discriminator: &str,
) -> anyhow::Result<Option<T>> {
    select_typed_by_index(db, index, value, discriminator, 1)?
        .first()
        .map(T::from_db_element)
        .transpose()
        .map_err(Into::into)
}

/// Fetch every node of the expected `DbElement` discriminator type whose `index` key holds
/// `value`.
pub(crate) fn fetch_all_typed_by_index<T: DbType<ValueType = T>>(
    db: &impl DbAccess,
    index: &str,
    value: &str,
    discriminator: &str,
) -> anyhow::Result<Vec<T>> {
    select_typed_by_index(db, index, value, discriminator, 0)?
        .iter()
        .map(|element| T::from_db_element(element).map_err(Into::into))
        .collect()
}

/// Bulk-fetch nodes by ID, filter to a single `DbElement` discriminator type,
/// and deserialize using `from_db_element`. Returns a map from `DbId` to the
/// deserialized value.
pub fn bulk_fetch_typed<T: DbType<ValueType = T>>(
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

#[cfg(test)]
mod tests {
    use super::*;

    struct FailingDb(agdb::DbErrorType);

    impl DbAccess for FailingDb {
        fn exec<T: agdb::Query>(&self, _query: T) -> Result<agdb::QueryResult, agdb::DbError> {
            Err(agdb::DbError::db(self.0, "lookup failed"))
        }

        fn exec_mut<T: agdb::QueryMut>(
            &mut self,
            _query: T,
        ) -> Result<agdb::QueryResult, agdb::DbError> {
            unreachable!()
        }
    }

    #[derive(agdb::DbType)]
    struct TestNode {
        name: String,
    }

    #[test]
    fn typed_lookup_only_treats_not_found_as_missing() {
        assert!(
            fetch_typed_by_id::<TestNode>(
                &FailingDb(agdb::DbErrorType::NotFound),
                DbId(1),
                "TestNode"
            )
            .unwrap()
            .is_none()
        );
        let error = fetch_typed_by_id::<TestNode>(
            &FailingDb(agdb::DbErrorType::TypeError),
            DbId(1),
            "TestNode",
        )
        .err()
        .expect("non-missing error must propagate");
        assert_eq!(
            error.downcast_ref::<agdb::DbError>().unwrap().ty,
            agdb::DbErrorType::TypeError
        );
    }

    #[derive(agdb::DbElement)]
    struct IndexedNode {
        db_id: Option<DbId>,
        lookup_key: String,
    }

    #[derive(agdb::DbElement)]
    struct OtherNode {
        lookup_key: String,
    }

    #[test]
    fn typed_index_lookup_skips_edges_and_other_types() -> anyhow::Result<()> {
        let mut db = super::super::test_db::new_test_db()?;
        db.exec_mut(QueryBuilder::insert().index("lookup_key").query())?;
        let other = db
            .exec_mut(
                QueryBuilder::insert()
                    .element(&OtherNode {
                        lookup_key: "shared".to_string(),
                    })
                    .query(),
            )?
            .ids()[0];
        let wanted = db
            .exec_mut(
                QueryBuilder::insert()
                    .element(&IndexedNode {
                        db_id: None,
                        lookup_key: "shared".to_string(),
                    })
                    .query(),
            )?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(other)
                .to(wanted)
                .values_uniform([("lookup_key", "shared").into()])
                .query(),
        )?;

        let found: Option<IndexedNode> =
            fetch_typed_by_index(&db, "lookup_key", "shared", "IndexedNode")?;
        assert_eq!(found.and_then(|node| node.db_id), Some(wanted));

        let all: Vec<IndexedNode> =
            fetch_all_typed_by_index(&db, "lookup_key", "shared", "IndexedNode")?;
        assert_eq!(
            all.into_iter().map(|node| node.db_id).collect::<Vec<_>>(),
            vec![Some(wanted)]
        );
        Ok(())
    }
}
