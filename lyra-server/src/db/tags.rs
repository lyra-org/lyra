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

use nanoid::nanoid;
use unicode_normalization::UnicodeNormalization;
use unicode_properties::{
    GeneralCategory,
    UnicodeGeneralCategory,
};

use super::DbAccess;
use super::NodeId;

pub(crate) const MAX_TAG_NAME_LEN: usize = 128;
pub(crate) const HAS_MANY_CAP: usize = 1024;
pub(crate) const GET_TAGGED_CAP: usize = 10_000;
pub(crate) const LIST_HARD_LIMIT: u64 = 500;

const TAG_EDGE_KEY: &str = "tag_edge";

/// Tag→owner edge marker. Distinct from playlists' `("owner", 1)` so user-delete cascades
/// can't wipe playlists as collateral damage.
const TAG_OWNER_KEY: &str = "tag_owner";

/// Composite `(owner, name)` key indexed in bootstrap for O(log N) tag lookup.
const TAG_OWNER_NAME_KEY: &str = "tag_owner_name";

fn tag_owner_name_key(owner_db_id: DbId, normalized_name: &str) -> String {
    format!("{}:{}", owner_db_id.0, normalized_name)
}

/// Strip Unicode `Cf` (Format) — zero-widths, bidi controls, SHY, BOM — plus U+034F CGJ,
/// which blocks NFC canonicalization and would otherwise let `"Chill\u{034F}"` duplicate
/// `"Chill"`.
fn is_invisible_strippable(c: char) -> bool {
    c == '\u{034F}' || c.general_category() == GeneralCategory::Format
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum TagNormalizeError {
    #[error("tag name cannot be empty after normalization")]
    Empty,
    #[error("tag name contains control characters")]
    ContainsControl,
    #[error("tag name exceeds maximum length of {MAX_TAG_NAME_LEN} codepoints")]
    TooLong,
}

/// Canonicalize a tag name: strip invisibles ([`is_invisible_strippable`]), trim by Unicode
/// `White_Space`, NFC-compose. Rejects control chars and names over [`MAX_TAG_NAME_LEN`].
/// Case-sensitive. Stripping must precede trimming because the invisibles aren't `White_Space`.
pub(crate) fn normalize_tag_name(raw: &str) -> Result<String, TagNormalizeError> {
    let stripped: String = raw
        .chars()
        .filter(|c| !is_invisible_strippable(*c))
        .collect();

    let trimmed = stripped.trim_matches(char::is_whitespace);

    let normalized: String = trimmed.nfc().collect();

    if normalized.is_empty() {
        return Err(TagNormalizeError::Empty);
    }
    if normalized.chars().any(char::is_control) {
        return Err(TagNormalizeError::ContainsControl);
    }
    if normalized.chars().count() > MAX_TAG_NAME_LEN {
        return Err(TagNormalizeError::TooLong);
    }

    Ok(normalized)
}

#[derive(agdb::DbElement, Clone, Debug)]
pub(crate) struct Tag {
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) tag: String,
    pub(crate) color: String,
    pub(crate) created_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CreateOutcome {
    Created,
    ReusedExisting,
}

/// Insert or reuse a tag under `owner_db_id`, then attach `target_db_id`. On reuse, the
/// existing tag (including its color) is returned as-is; re-attach is a no-op.
pub(crate) fn create(
    db: &mut impl DbAccess,
    owner_db_id: DbId,
    target_db_id: DbId,
    normalized_name: &str,
    color: &str,
    now_ms: i64,
) -> anyhow::Result<(DbId, CreateOutcome)> {
    let existing = find_tag_id_by_owner_and_name(db, owner_db_id, normalized_name)?;
    let (tag_id, outcome) = if let Some(existing_id) = existing {
        (existing_id, CreateOutcome::ReusedExisting)
    } else {
        let tag_node = Tag {
            db_id: None,
            id: nanoid!(),
            tag: normalized_name.to_string(),
            color: color.to_string(),
            created_at_ms: now_ms,
        };
        let insert_result = db.exec_mut(QueryBuilder::insert().element(&tag_node).query())?;
        let tag_id = insert_result
            .ids()
            .first()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("tag creation missing id"))?;
        db.exec_mut(
            QueryBuilder::insert()
                .values_uniform([(
                    TAG_OWNER_NAME_KEY,
                    tag_owner_name_key(owner_db_id, normalized_name).as_str(),
                )
                    .into()])
                .ids(tag_id)
                .query(),
        )?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("tags")
                .to(tag_id)
                .query(),
        )?;
        let owner_edge = db
            .exec_mut(
                QueryBuilder::insert()
                    .edges()
                    .from(tag_id)
                    .to(owner_db_id)
                    .query(),
            )?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .values_uniform([(TAG_OWNER_KEY, 1_i64).into()])
                .ids(owner_edge)
                .query(),
        )?;
        (tag_id, CreateOutcome::Created)
    };

    if !has_target_edge(db, tag_id, target_db_id)? {
        let edge_id = db
            .exec_mut(
                QueryBuilder::insert()
                    .edges()
                    .from(tag_id)
                    .to(target_db_id)
                    .query(),
            )?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .values_uniform([(TAG_EDGE_KEY, 1_i64).into()])
                .ids(edge_id)
                .query(),
        )?;
    }

    Ok((tag_id, outcome))
}

#[derive(Debug, thiserror::Error)]
#[error("tag name '{0}' already exists for this user")]
pub(crate) struct RenameConflict(pub String);

/// Update a tag's name and/or color. Rename onto an existing `(owner, name)` returns
/// [`RenameConflict`] instead of merging.
pub(crate) fn update(
    db: &mut impl DbAccess,
    tag_id: DbId,
    normalized_name: Option<&str>,
    color: Option<&str>,
) -> anyhow::Result<Result<Tag, RenameConflict>> {
    let owner_for_rename = if normalized_name.is_some() {
        let owner = get_owner(db, tag_id)?.ok_or_else(|| {
            anyhow::anyhow!(
                "tag {} has no owner edge; schema invariant violated",
                tag_id.0
            )
        })?;
        if let Some(new_name) = normalized_name
            && let Some(colliding) = find_tag_id_by_owner_and_name(db, owner, new_name)?
            && colliding != tag_id
        {
            return Ok(Err(RenameConflict(new_name.to_string())));
        }
        Some(owner)
    } else {
        None
    };

    let mut values: Vec<agdb::DbKeyValue> = Vec::with_capacity(3);
    if let Some(new_name) = normalized_name {
        values.push(agdb::DbKeyValue {
            key: DbValue::from("tag"),
            value: DbValue::from(new_name),
        });
        // Rewrite composite key so the index tracks the rename; see agdb_index_contract.
        if let Some(owner) = owner_for_rename {
            values.push(agdb::DbKeyValue {
                key: DbValue::from(TAG_OWNER_NAME_KEY),
                value: DbValue::from(tag_owner_name_key(owner, new_name).as_str()),
            });
        }
    }
    if let Some(new_color) = color {
        values.push(agdb::DbKeyValue {
            key: DbValue::from("color"),
            value: DbValue::from(new_color),
        });
    }

    if !values.is_empty() {
        db.exec_mut(
            QueryBuilder::insert()
                .values_uniform(values)
                .ids(tag_id)
                .query(),
        )?;
    }

    let tag = get_by_id(db, tag_id)?.ok_or_else(|| {
        anyhow::anyhow!(
            "tag {} disappeared during update; schema invariant violated",
            tag_id.0
        )
    })?;
    Ok(Ok(tag))
}

pub(crate) fn delete(db: &mut impl DbAccess, tag_id: DbId) -> anyhow::Result<()> {
    db.exec_mut(QueryBuilder::remove().ids(tag_id).query())?;
    Ok(())
}

pub(crate) fn get_by_id(db: &impl DbAccess, tag_id: DbId) -> anyhow::Result<Option<Tag>> {
    super::graph::fetch_typed_by_id(db, tag_id, "Tag")
}

fn tag_node_id_raw(tag: &Tag) -> i64 {
    tag.db_id
        .as_ref()
        .map(|id| DbId::from(id.clone()).0)
        .unwrap_or_default()
}

pub(crate) fn find_tag_id_by_owner_and_name(
    db: &impl DbAccess,
    owner_db_id: DbId,
    normalized_name: &str,
) -> anyhow::Result<Option<DbId>> {
    let key = tag_owner_name_key(owner_db_id, normalized_name);
    let result = db.exec(
        QueryBuilder::search()
            .index(TAG_OWNER_NAME_KEY)
            .value(key.as_str())
            .query(),
    )?;
    Ok(result.ids().into_iter().find(|id| id.0 > 0))
}

pub(crate) fn owner_tag_ids(db: &impl DbAccess, owner_db_id: DbId) -> anyhow::Result<Vec<DbId>> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .to(owner_db_id)
            .where_()
            .edge()
            .and()
            .key(TAG_OWNER_KEY)
            .value(DbValue::I64(1))
            .end_where()
            .query(),
    )?;
    let mut ids = Vec::new();
    for element in result.elements {
        if let Some(from_id) = element.from {
            if from_id.0 > 0 {
                ids.push(from_id);
            }
        }
    }
    Ok(ids)
}

pub(crate) fn get_owner(db: &impl DbAccess, tag_id: DbId) -> anyhow::Result<Option<DbId>> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .from(tag_id)
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(1))
            .and()
            .key(TAG_OWNER_KEY)
            .value(DbValue::I64(1))
            .end_where()
            .query(),
    )?;
    for element in result.elements {
        if let Some(to_id) = element.to {
            if to_id.0 > 0 {
                return Ok(Some(to_id));
            }
        }
    }
    Ok(None)
}

pub(crate) fn get_targets(db: &impl DbAccess, tag_id: DbId) -> anyhow::Result<Vec<DbId>> {
    let mut targets = Vec::new();
    let mut seen = HashSet::new();
    for element in tag_target_edges_from(db, tag_id)? {
        let Some(target_id) = element.to else {
            continue;
        };
        if seen.insert(target_id) {
            targets.push(target_id);
        }
    }
    Ok(targets)
}

fn sort_tags(tags: &mut [Tag]) {
    tags.sort_by(|a, b| {
        b.created_at_ms
            .cmp(&a.created_at_ms)
            .then_with(|| tag_node_id_raw(a).cmp(&tag_node_id_raw(b)))
    });
}

pub(crate) fn get_for_target(
    db: &impl DbAccess,
    owner_db_id: DbId,
    target_db_id: DbId,
) -> anyhow::Result<Vec<Tag>> {
    let mut result = get_for_targets_many(db, owner_db_id, &[target_db_id])?;
    Ok(result.remove(&target_db_id).unwrap_or_default())
}

pub(crate) fn get_for_targets_many(
    db: &impl DbAccess,
    owner_db_id: DbId,
    target_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Vec<Tag>>> {
    let mut result: HashMap<DbId, Vec<Tag>> = target_db_ids
        .iter()
        .copied()
        .map(|id| (id, Vec::new()))
        .collect();
    if target_db_ids.is_empty() {
        return Ok(result);
    }

    let owner_tags: HashSet<DbId> = owner_tag_ids(db, owner_db_id)?.into_iter().collect();
    if owner_tags.is_empty() {
        return Ok(result);
    }

    let mut all_tag_ids = Vec::new();
    let mut all_seen = HashSet::new();
    let mut tag_ids_by_target: HashMap<DbId, Vec<DbId>> = HashMap::new();
    for &target_db_id in target_db_ids {
        let mut target_seen = HashSet::new();
        for element in inbound_tag_edges(db, target_db_id)? {
            let Some(tag_id) = element.from else {
                continue;
            };
            if !owner_tags.contains(&tag_id) || !target_seen.insert(tag_id) {
                continue;
            }
            tag_ids_by_target
                .entry(target_db_id)
                .or_default()
                .push(tag_id);
            if all_seen.insert(tag_id) {
                all_tag_ids.push(tag_id);
            }
        }
    }
    if all_tag_ids.is_empty() {
        return Ok(result);
    }

    let tags_by_id: HashMap<DbId, Tag> = super::graph::bulk_fetch_typed(db, all_tag_ids, "Tag")?;
    for (target_db_id, tag_ids) in tag_ids_by_target {
        let mut tags = Vec::with_capacity(tag_ids.len());
        for tag_id in tag_ids {
            if let Some(tag) = tags_by_id.get(&tag_id) {
                tags.push(tag.clone());
            }
        }
        sort_tags(&mut tags);
        result.insert(target_db_id, tags);
    }
    Ok(result)
}

fn tag_target_edges_from(db: &impl DbAccess, tag_id: DbId) -> anyhow::Result<Vec<DbElement>> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .from(tag_id)
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(1))
            .and()
            .key(TAG_EDGE_KEY)
            .value(DbValue::I64(1))
            .end_where()
            .query(),
    )?;
    Ok(result
        .elements
        .into_iter()
        .filter(|e| e.from == Some(tag_id))
        .collect())
}

fn has_target_edge(db: &impl DbAccess, tag_id: DbId, target_db_id: DbId) -> anyhow::Result<bool> {
    for element in tag_target_edges_from(db, tag_id)? {
        if element.to == Some(target_db_id) {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn has_target(
    db: &impl DbAccess,
    owner_db_id: DbId,
    target_db_id: DbId,
    normalized_name: &str,
) -> anyhow::Result<bool> {
    let Some(tag_id) = find_tag_id_by_owner_and_name(db, owner_db_id, normalized_name)? else {
        return Ok(false);
    };
    has_target_edge(db, tag_id, target_db_id)
}

pub(crate) fn has_targets(
    db: &impl DbAccess,
    owner_db_id: DbId,
    target_db_ids: &[DbId],
    normalized_name: &str,
) -> anyhow::Result<HashMap<DbId, bool>> {
    let mut response: HashMap<DbId, bool> = target_db_ids
        .iter()
        .copied()
        .map(|id| (id, false))
        .collect();

    let Some(tag_id) = find_tag_id_by_owner_and_name(db, owner_db_id, normalized_name)? else {
        return Ok(response);
    };

    let attached: HashSet<DbId> = get_targets(db, tag_id)?.into_iter().collect();
    for id in target_db_ids {
        if attached.contains(id) {
            response.insert(*id, true);
        }
    }
    Ok(response)
}

/// Targets tagged `normalized_name` by `owner`. Errs above [`GET_TAGGED_CAP`].
pub(crate) fn get_targets_by_tag(
    db: &impl DbAccess,
    owner_db_id: DbId,
    normalized_name: &str,
) -> anyhow::Result<Vec<DbId>> {
    get_targets_by_tag_with_cap(db, owner_db_id, normalized_name, GET_TAGGED_CAP)
}

fn get_targets_by_tag_with_cap(
    db: &impl DbAccess,
    owner_db_id: DbId,
    normalized_name: &str,
    cap: usize,
) -> anyhow::Result<Vec<DbId>> {
    let Some(tag_id) = find_tag_id_by_owner_and_name(db, owner_db_id, normalized_name)? else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for target in get_targets(db, tag_id)? {
        if out.len() >= cap {
            anyhow::bail!(
                "get_targets_by_tag cap exceeded: >{cap} targets for tag '{normalized_name}'",
            );
        }
        out.push(target);
    }
    Ok(out)
}

/// Detach a target from a named tag. Auto-removes the tag node when its last target detaches.
pub(crate) fn remove_target(
    db: &mut impl DbAccess,
    owner_db_id: DbId,
    target_db_id: DbId,
    normalized_name: &str,
) -> anyhow::Result<bool> {
    let Some(tag_id) = find_tag_id_by_owner_and_name(db, owner_db_id, normalized_name)? else {
        return Ok(false);
    };

    let edge_id = tag_target_edges_from(db, tag_id)?
        .into_iter()
        .find(|e| e.to == Some(target_db_id))
        .map(|e| e.id);
    let Some(edge_id) = edge_id else {
        return Ok(false);
    };

    db.exec_mut(QueryBuilder::remove().ids(edge_id).query())?;

    if get_targets(db, tag_id)?.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(tag_id).query())?;
    }
    Ok(true)
}

#[derive(Debug, Clone)]
pub(crate) struct PagedTags {
    pub(crate) tags: Vec<Tag>,
    pub(crate) next_cursor: Option<TagListCursor>,
}

/// Anchored on `(created_at_ms, tag_db_id)` — both immutable, so rename doesn't reorder.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) struct TagListCursor {
    pub(crate) created_at_ms: i64,
    pub(crate) tag_db_id: i64,
}

/// List a user's tags by `created_at_ms DESC, tag_db_id ASC`.
pub(crate) fn list_for_user(
    db: &impl DbAccess,
    owner_db_id: DbId,
    limit: u64,
    cursor: Option<TagListCursor>,
) -> anyhow::Result<PagedTags> {
    if limit == 0 {
        return Ok(PagedTags {
            tags: Vec::new(),
            next_cursor: None,
        });
    }

    let mut tags: Vec<Tag> = Vec::new();
    for tag_id in owner_tag_ids(db, owner_db_id)? {
        if let Some(tag) = get_by_id(db, tag_id)? {
            tags.push(tag);
        }
    }

    sort_tags(&mut tags);

    if let Some(cursor) = cursor {
        tags.retain(|tag| {
            let tag_id = tag_node_id_raw(tag);
            match tag.created_at_ms.cmp(&cursor.created_at_ms) {
                std::cmp::Ordering::Less => true,
                std::cmp::Ordering::Equal => tag_id > cursor.tag_db_id,
                std::cmp::Ordering::Greater => false,
            }
        });
    }

    let mut next_cursor = None;
    if tags.len() > limit as usize {
        tags.truncate(limit as usize);
        if let Some(last) = tags.last() {
            next_cursor = Some(TagListCursor {
                created_at_ms: last.created_at_ms,
                tag_db_id: tag_node_id_raw(last),
            });
        }
    }

    Ok(PagedTags { tags, next_cursor })
}

/// Paginated target list for a tag, anchored on `target_db_id ASC`.
#[derive(Debug, Clone)]
pub(crate) struct PagedTargets {
    pub(crate) target_db_ids: Vec<DbId>,
    pub(crate) next_cursor: Option<TargetListCursor>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) struct TargetListCursor {
    pub(crate) target_db_id: i64,
}

pub(crate) fn list_targets(
    db: &impl DbAccess,
    tag_id: DbId,
    limit: u64,
    cursor: Option<TargetListCursor>,
) -> anyhow::Result<PagedTargets> {
    if limit == 0 {
        return Ok(PagedTargets {
            target_db_ids: Vec::new(),
            next_cursor: None,
        });
    }

    let mut ids = get_targets(db, tag_id)?;
    ids.sort_by_key(|id| id.0);

    if let Some(cursor) = cursor {
        ids.retain(|id| id.0 > cursor.target_db_id);
    }

    let mut next_cursor = None;
    if ids.len() > limit as usize {
        ids.truncate(limit as usize);
        if let Some(last) = ids.last() {
            next_cursor = Some(TargetListCursor {
                target_db_id: last.0,
            });
        }
    }

    Ok(PagedTargets {
        target_db_ids: ids,
        next_cursor,
    })
}

pub(crate) fn remove_outbound_for_user(
    db: &mut impl DbAccess,
    owner_db_id: DbId,
) -> anyhow::Result<()> {
    let tag_ids = owner_tag_ids(db, owner_db_id)?;
    if !tag_ids.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(tag_ids).query())?;
    }
    Ok(())
}

/// Remove inbound tag edges for a batch of deleted targets, then drop any tag node left
/// empty. Dedupes impacted tags and checks emptiness per-tag once, AFTER all its edges are
/// gone — so a tag with two targets in the same batch isn't dropped prematurely.
pub(crate) fn remove_inbound_for_target_with_orphan_cleanup(
    db: &mut impl DbAccess,
    target_db_ids: &[DbId],
) -> anyhow::Result<()> {
    if target_db_ids.is_empty() {
        return Ok(());
    }

    let mut impacted_tag_ids: HashSet<DbId> = HashSet::new();
    let mut edge_ids_to_remove: Vec<DbId> = Vec::new();

    for &target_db_id in target_db_ids {
        for element in inbound_tag_edges(db, target_db_id)? {
            if let Some(from_id) = element.from {
                impacted_tag_ids.insert(from_id);
            }
            edge_ids_to_remove.push(element.id);
        }
    }

    if !edge_ids_to_remove.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(edge_ids_to_remove).query())?;
    }

    let mut orphan_tag_ids = Vec::new();
    for tag_id in impacted_tag_ids {
        if get_targets(db, tag_id)?.is_empty() {
            orphan_tag_ids.push(tag_id);
        }
    }
    if !orphan_tag_ids.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(orphan_tag_ids).query())?;
    }

    Ok(())
}

fn inbound_tag_edges(db: &impl DbAccess, target_db_id: DbId) -> anyhow::Result<Vec<DbElement>> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .to(target_db_id)
            .where_()
            .edge()
            .and()
            .key(TAG_EDGE_KEY)
            .value(DbValue::I64(1))
            .end_where()
            .query(),
    )?;
    Ok(result
        .elements
        .into_iter()
        .filter(|e| e.to == Some(target_db_id))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::new_test_db;
    use agdb::DbAny;

    fn create_test_user(db: &mut DbAny) -> anyhow::Result<DbId> {
        let user_db_id = db
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values([[("username", "testuser").into()]])
                    .query(),
            )?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("users")
                .to(user_db_id)
                .query(),
        )?;
        Ok(user_db_id)
    }

    fn create_test_track(db: &mut DbAny) -> anyhow::Result<DbId> {
        let track_db_id = db
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values([[("track_title", "Track").into()]])
                    .query(),
            )?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("tracks")
                .to(track_db_id)
                .query(),
        )?;
        Ok(track_db_id)
    }

    #[test]
    fn get_for_target_returns_owner_tags_sorted() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let alice = create_test_user(&mut db)?;
        let bob = create_test_user(&mut db)?;
        let track = create_test_track(&mut db)?;

        create(&mut db, alice, track, "Old", "blue", 1_000)?;
        create(&mut db, alice, track, "New", "red", 2_000)?;
        create(&mut db, bob, track, "Bob", "gray", 3_000)?;

        let tags = get_for_target(&db, alice, track)?;
        let names: Vec<_> = tags.iter().map(|tag| tag.tag.as_str()).collect();
        assert_eq!(names, vec!["New", "Old"]);
        Ok(())
    }

    #[test]
    fn get_for_targets_many_returns_empty_entries() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let alice = create_test_user(&mut db)?;
        let tagged = create_test_track(&mut db)?;
        let untagged = create_test_track(&mut db)?;

        create(&mut db, alice, tagged, "Tagged", "blue", 1_000)?;

        let tags = get_for_targets_many(&db, alice, &[tagged, untagged])?;
        assert_eq!(
            tags.get(&tagged)
                .expect("tagged target key exists")
                .iter()
                .map(|tag| tag.tag.as_str())
                .collect::<Vec<_>>(),
            vec!["Tagged"],
        );
        assert!(
            tags.get(&untagged)
                .expect("untagged target key exists")
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn normalize_strips_combining_grapheme_joiner() {
        assert_eq!(normalize_tag_name("Chill\u{034F}").unwrap(), "Chill");
    }

    #[test]
    fn normalize_strips_bidi_and_format_controls() {
        assert_eq!(
            normalize_tag_name("\u{200E} Chill \u{200F}").unwrap(),
            "Chill",
            "LRM/RLM must strip",
        );
        assert_eq!(
            normalize_tag_name("Chill\u{00AD}").unwrap(),
            "Chill",
            "soft hyphen (SHY) must strip",
        );
    }

    #[test]
    fn normalize_handles_zero_width_armored_whitespace() {
        assert_eq!(
            normalize_tag_name("\u{200B} Chill \u{FEFF}").unwrap(),
            "Chill"
        );
        assert_eq!(
            normalize_tag_name("\u{00A0}\u{200B}\u{00A0}Chill\u{FEFF}\u{3000}").unwrap(),
            "Chill"
        );
    }

    #[test]
    fn normalize_rejects_zero_width_armored_blank() {
        assert_eq!(
            normalize_tag_name("\u{200B}   \u{FEFF}").unwrap_err(),
            TagNormalizeError::Empty,
        );
        assert_eq!(
            normalize_tag_name("   \u{200B}   \u{FEFF}   ").unwrap_err(),
            TagNormalizeError::Empty,
        );
    }

    #[test]
    fn normalize_rejects_control_chars() {
        let err = normalize_tag_name("bad\x00name").unwrap_err();
        assert_eq!(err, TagNormalizeError::ContainsControl);
    }

    #[test]
    fn normalize_rejects_empty() {
        assert_eq!(
            normalize_tag_name("").unwrap_err(),
            TagNormalizeError::Empty
        );
        assert_eq!(
            normalize_tag_name("   ").unwrap_err(),
            TagNormalizeError::Empty
        );
    }

    #[test]
    fn normalize_enforces_length_cap() {
        let long = "a".repeat(MAX_TAG_NAME_LEN + 1);
        assert_eq!(
            normalize_tag_name(&long).unwrap_err(),
            TagNormalizeError::TooLong
        );
        let ok = "a".repeat(MAX_TAG_NAME_LEN);
        assert!(normalize_tag_name(&ok).is_ok());
    }

    #[test]
    fn normalize_applies_nfc_composition() {
        let precomposed = "café";
        let decomposed = "cafe\u{0301}";
        assert_eq!(
            normalize_tag_name(precomposed).unwrap(),
            normalize_tag_name(decomposed).unwrap(),
        );
    }

    #[test]
    fn create_inserts_tag_and_owner_edge() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_test_user(&mut db)?;
        let track = create_test_track(&mut db)?;

        let (tag_id, outcome) = create(&mut db, user, track, "Workout", "blue", 1000)?;
        assert_eq!(outcome, CreateOutcome::Created);

        let tag = get_by_id(&db, tag_id)?.expect("tag exists");
        assert_eq!(tag.tag, "Workout");
        assert_eq!(tag.color, "blue");
        assert_eq!(tag.created_at_ms, 1000);

        assert_eq!(get_owner(&db, tag_id)?, Some(user));
        assert!(has_target(&db, user, track, "Workout")?);

        Ok(())
    }

    #[test]
    fn create_reuses_existing_tag_and_ignores_color() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_test_user(&mut db)?;
        let track_a = create_test_track(&mut db)?;
        let track_b = create_test_track(&mut db)?;

        let (first_id, first_outcome) = create(&mut db, user, track_a, "Mood", "blue", 1000)?;
        assert_eq!(first_outcome, CreateOutcome::Created);

        let (second_id, second_outcome) = create(&mut db, user, track_b, "Mood", "red", 2000)?;
        assert_eq!(first_id, second_id);
        assert_eq!(second_outcome, CreateOutcome::ReusedExisting);

        let tag = get_by_id(&db, first_id)?.expect("tag exists");
        assert_eq!(tag.color, "blue", "color must NOT change on reuse");
        assert_eq!(
            tag.created_at_ms, 1000,
            "created_at must NOT change on reuse",
        );

        let targets = get_targets(&db, first_id)?;
        assert_eq!(targets.len(), 2);

        Ok(())
    }

    #[test]
    fn per_user_isolation_two_users_same_name() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let alice = create_test_user(&mut db)?;
        let bob = create_test_user(&mut db)?;
        let track_alice = create_test_track(&mut db)?;
        let track_bob = create_test_track(&mut db)?;

        let (alice_tag_id, _) = create(&mut db, alice, track_alice, "Chill", "blue", 1)?;
        let (bob_tag_id, _) = create(&mut db, bob, track_bob, "Chill", "red", 2)?;

        assert_ne!(
            alice_tag_id, bob_tag_id,
            "same name under different owners must be distinct nodes",
        );
        assert!(has_target(&db, alice, track_alice, "Chill")?);
        assert!(!has_target(&db, bob, track_alice, "Chill")?);
        assert!(has_target(&db, bob, track_bob, "Chill")?);
        assert!(!has_target(&db, alice, track_bob, "Chill")?);

        Ok(())
    }

    #[test]
    fn case_sensitive_tags_are_distinct() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_test_user(&mut db)?;
        let track = create_test_track(&mut db)?;

        let (upper_id, _) = create(&mut db, user, track, "Chill", "blue", 1)?;
        let (lower_id, _) = create(&mut db, user, track, "chill", "red", 2)?;

        assert_ne!(upper_id, lower_id);
        Ok(())
    }

    #[test]
    fn update_rename_collides_on_existing_name() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_test_user(&mut db)?;
        let track = create_test_track(&mut db)?;

        let (workout_id, _) = create(&mut db, user, track, "Workout", "blue", 1)?;
        let (_mood_id, _) = create(&mut db, user, track, "Mood", "red", 2)?;

        let result = update(&mut db, workout_id, Some("Mood"), None)?;
        assert!(result.is_err(), "rename onto existing name must collide");
        Ok(())
    }

    #[test]
    fn update_renames_cleanly() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_test_user(&mut db)?;
        let track = create_test_track(&mut db)?;

        let (tag_id, _) = create(&mut db, user, track, "Workout", "blue", 1)?;
        let result = update(&mut db, tag_id, Some("Exercise"), None)?
            .expect("rename to unused name must succeed");
        assert_eq!(result.tag, "Exercise");
        Ok(())
    }

    #[test]
    fn remove_target_cleans_up_empty_tag() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_test_user(&mut db)?;
        let track = create_test_track(&mut db)?;

        let (tag_id, _) = create(&mut db, user, track, "Only", "blue", 1)?;
        assert!(remove_target(&mut db, user, track, "Only")?);
        assert!(
            get_by_id(&db, tag_id)?.is_none(),
            "tag node must be removed when last target detaches",
        );
        Ok(())
    }

    #[test]
    fn orphan_cleanup_batched_across_multiple_targets_on_same_tag() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_test_user(&mut db)?;
        let track_a = create_test_track(&mut db)?;
        let track_b = create_test_track(&mut db)?;

        let (tag_id, _) = create(&mut db, user, track_a, "Shared", "blue", 1)?;
        create(&mut db, user, track_b, "Shared", "blue", 2)?;
        assert_eq!(get_targets(&db, tag_id)?.len(), 2);

        remove_inbound_for_target_with_orphan_cleanup(&mut db, &[track_a, track_b])?;

        assert!(
            get_by_id(&db, tag_id)?.is_none(),
            "tag node must be orphan-cleaned when all its targets are in one cascade batch",
        );

        Ok(())
    }

    #[test]
    fn orphan_cleanup_preserves_tag_with_surviving_target() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_test_user(&mut db)?;
        let track_a = create_test_track(&mut db)?;
        let track_b = create_test_track(&mut db)?;

        let (tag_id, _) = create(&mut db, user, track_a, "Shared", "blue", 1)?;
        create(&mut db, user, track_b, "Shared", "blue", 2)?;

        remove_inbound_for_target_with_orphan_cleanup(&mut db, &[track_a])?;

        assert!(
            get_by_id(&db, tag_id)?.is_some(),
            "tag node must survive while at least one target remains",
        );
        assert_eq!(get_targets(&db, tag_id)?, vec![track_b]);

        Ok(())
    }

    #[test]
    fn remove_outbound_for_user_does_not_touch_playlists() -> anyhow::Result<()> {
        // Regression: tag cascade must not touch playlists even though both carry an "owner" edge.
        use nanoid::nanoid;
        let mut db = new_test_db()?;
        let user = create_test_user(&mut db)?;
        let track = create_test_track(&mut db)?;

        let playlist = crate::db::Playlist {
            db_id: None,
            id: nanoid!(),
            name: "my playlist".to_string(),
            description: None,
            is_public: Some(true),
            created_at: None,
            updated_at: None,
        };
        let playlist_db_id = crate::db::playlists::create(&mut db, &playlist, user)?;
        create(&mut db, user, track, "Workout", "blue", 1)?;

        let ids = owner_tag_ids(&db, user)?;
        assert!(!ids.contains(&playlist_db_id));

        remove_outbound_for_user(&mut db, user)?;

        assert!(crate::db::playlists::get_by_id(&db, playlist_db_id)?.is_some());

        Ok(())
    }

    #[test]
    fn remove_outbound_for_user_removes_all_user_tags() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let alice = create_test_user(&mut db)?;
        let bob = create_test_user(&mut db)?;
        let track = create_test_track(&mut db)?;

        create(&mut db, alice, track, "Alice's Tag", "blue", 1)?;
        create(&mut db, bob, track, "Bob's Tag", "red", 2)?;

        remove_outbound_for_user(&mut db, alice)?;

        assert!(!has_target(&db, alice, track, "Alice's Tag")?);
        assert!(
            has_target(&db, bob, track, "Bob's Tag")?,
            "other users' tags must survive",
        );

        Ok(())
    }

    #[test]
    fn list_for_user_paginates_by_created_at_desc() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_test_user(&mut db)?;
        let track = create_test_track(&mut db)?;

        create(&mut db, user, track, "First", "blue", 1000)?;
        create(&mut db, user, track, "Second", "red", 2000)?;
        create(&mut db, user, track, "Third", "green", 3000)?;

        let page1 = list_for_user(&db, user, 2, None)?;
        assert_eq!(page1.tags.len(), 2);
        assert_eq!(page1.tags[0].tag, "Third");
        assert_eq!(page1.tags[1].tag, "Second");
        let cursor = page1.next_cursor.expect("cursor present");

        let page2 = list_for_user(&db, user, 2, Some(cursor))?;
        assert_eq!(page2.tags.len(), 1);
        assert_eq!(page2.tags[0].tag, "First");
        assert!(page2.next_cursor.is_none());

        Ok(())
    }

    #[test]
    fn list_targets_paginates_by_target_id_asc() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_test_user(&mut db)?;
        let track_a = create_test_track(&mut db)?;
        let track_b = create_test_track(&mut db)?;
        let track_c = create_test_track(&mut db)?;

        let (tag_id, _) = create(&mut db, user, track_a, "Shared", "blue", 1)?;
        create(&mut db, user, track_b, "Shared", "blue", 2)?;
        create(&mut db, user, track_c, "Shared", "blue", 3)?;

        let page1 = list_targets(&db, tag_id, 2, None)?;
        assert_eq!(page1.target_db_ids.len(), 2);
        let cursor = page1.next_cursor.expect("cursor present");

        let page2 = list_targets(&db, tag_id, 2, Some(cursor))?;
        assert_eq!(page2.target_db_ids.len(), 1);
        assert!(page2.next_cursor.is_none());

        Ok(())
    }

    #[test]
    fn get_targets_by_tag_errs_on_overflow() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_test_user(&mut db)?;
        for _ in 0..3 {
            let track = create_test_track(&mut db)?;
            create(&mut db, user, track, "Many", "blue", 1)?;
        }
        let err = get_targets_by_tag_with_cap(&db, user, "Many", 2).expect_err("cap");
        assert!(err.to_string().contains("cap exceeded"));

        Ok(())
    }

    #[test]
    fn nfc_normalization_prevents_duplicate_tag_via_decomposed_form() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_test_user(&mut db)?;
        let track = create_test_track(&mut db)?;

        let precomposed = normalize_tag_name("café")?;
        let decomposed = normalize_tag_name("cafe\u{0301}")?;
        assert_eq!(precomposed, decomposed);

        let (first_id, _) = create(&mut db, user, track, &precomposed, "blue", 1)?;
        let (second_id, outcome) = create(&mut db, user, track, &decomposed, "red", 2)?;
        assert_eq!(first_id, second_id);
        assert_eq!(outcome, CreateOutcome::ReusedExisting);

        Ok(())
    }
}

#[cfg(test)]
mod benches {
    extern crate test;

    use test::Bencher;

    use super::*;
    use crate::db::test_db::new_test_db;
    use agdb::DbAny;

    fn insert_user(db: &mut DbAny) -> DbId {
        let id = db
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values([[("username", "bench").into()]])
                    .query(),
            )
            .unwrap()
            .ids()[0];
        db.exec_mut(QueryBuilder::insert().edges().from("users").to(id).query())
            .unwrap();
        id
    }

    fn insert_track(db: &mut DbAny) -> DbId {
        let id = db
            .exec_mut(QueryBuilder::insert().nodes().count(1).query())
            .unwrap()
            .ids()[0];
        db.exec_mut(QueryBuilder::insert().edges().from("tracks").to(id).query())
            .unwrap();
        id
    }

    fn seed_user_with_tags(n: usize) -> (DbAny, DbId, DbId) {
        let mut db = new_test_db().unwrap();
        let user = insert_user(&mut db);
        let track = insert_track(&mut db);
        for i in 0..n {
            let name = format!("tag-{i:08}");
            create(&mut db, user, track, &name, "blue", i as i64).unwrap();
        }
        (db, user, track)
    }

    #[bench]
    fn owner_tag_ids_only_100(b: &mut Bencher) {
        let (db, user, _) = seed_user_with_tags(100);
        b.iter(|| owner_tag_ids(&db, user).unwrap());
    }

    #[bench]
    fn owner_tag_ids_only_1000(b: &mut Bencher) {
        let (db, user, _) = seed_user_with_tags(1_000);
        b.iter(|| owner_tag_ids(&db, user).unwrap());
    }

    #[bench]
    fn normalize_ascii_simple(b: &mut Bencher) {
        b.iter(|| normalize_tag_name("Chill").unwrap());
    }

    #[bench]
    fn normalize_mixed_invisibles(b: &mut Bencher) {
        b.iter(|| normalize_tag_name("\u{200B}\u{00A0} Ch\u{200D}i\u{FEFF}ll \u{200F}").unwrap());
    }

    #[bench]
    fn normalize_nfc_decomposed(b: &mut Bencher) {
        b.iter(|| normalize_tag_name("cafe\u{0301}").unwrap());
    }

    #[bench]
    fn find_by_owner_and_name_miss_100(b: &mut Bencher) {
        let (db, user, _) = seed_user_with_tags(100);
        b.iter(|| find_tag_id_by_owner_and_name(&db, user, "absent").unwrap());
    }

    #[bench]
    fn find_by_owner_and_name_miss_1000(b: &mut Bencher) {
        let (db, user, _) = seed_user_with_tags(1_000);
        b.iter(|| find_tag_id_by_owner_and_name(&db, user, "absent").unwrap());
    }

    #[bench]
    fn create_reuse_user_with_100_tags(b: &mut Bencher) {
        let (mut db, user, track) = seed_user_with_tags(100);
        let name = format!("tag-{:08}", 50);
        b.iter(|| create(&mut db, user, track, &name, "blue", 0).unwrap());
    }

    #[bench]
    fn create_reuse_user_with_1000_tags(b: &mut Bencher) {
        let (mut db, user, track) = seed_user_with_tags(1_000);
        let name = format!("tag-{:08}", 500);
        b.iter(|| create(&mut db, user, track, &name, "blue", 0).unwrap());
    }

    #[bench]
    fn list_for_user_with_100_tags(b: &mut Bencher) {
        let (db, user, _) = seed_user_with_tags(100);
        b.iter(|| list_for_user(&db, user, 500, None).unwrap());
    }

    #[bench]
    fn list_for_user_with_1000_tags(b: &mut Bencher) {
        let (db, user, _) = seed_user_with_tags(1_000);
        b.iter(|| list_for_user(&db, user, 500, None).unwrap());
    }

    // Rename onto itself is idempotent — safe to re-run in a loop against seeded state.
    #[bench]
    fn update_rename_user_with_100_tags(b: &mut Bencher) {
        let (mut db, _user, _track) = seed_user_with_tags(100);
        let tag_id = find_tag_id_by_owner_and_name(&db, _user, &format!("tag-{:08}", 50))
            .unwrap()
            .unwrap();
        b.iter(|| {
            update(&mut db, tag_id, Some(&format!("tag-{:08}", 50)), None)
                .unwrap()
                .expect("no collision");
        });
    }

    #[bench]
    fn update_rename_user_with_1000_tags(b: &mut Bencher) {
        let (mut db, _user, _track) = seed_user_with_tags(1_000);
        let tag_id = find_tag_id_by_owner_and_name(&db, _user, &format!("tag-{:08}", 500))
            .unwrap()
            .unwrap();
        b.iter(|| {
            update(&mut db, tag_id, Some(&format!("tag-{:08}", 500)), None)
                .unwrap()
                .expect("no collision");
        });
    }
}
