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
    DbAny,
    DbElement,
    DbId,
    QueryBuilder,
};
use nanoid::nanoid;

use super::DbAccess;
use super::NodeId;
use super::providers::external_ids;

#[derive(DbElement, Clone, Debug)]
pub(crate) struct Label {
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) label_scan_name: String,
    pub(crate) created_at: Option<u64>,
}

/// Intermediate node carrying the catalog number on the Release–Label link.
/// Follows the `Credit` pattern for edge-metadata nodes.
#[derive(DbElement, Clone, Debug)]
pub(crate) struct ReleaseLabel {
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) catalog_number: Option<String>,
    pub(crate) scan_catalog_number: Option<String>,
}

pub(crate) struct ResolveLabel<'a> {
    pub(crate) name: &'a str,
    pub(crate) external_id: Option<ResolveExternalId<'a>>,
}

pub(crate) struct ResolveExternalId<'a> {
    pub(crate) provider_id: &'a str,
    pub(crate) id_type: &'a str,
    pub(crate) id_value: &'a str,
}

#[derive(Debug, Clone)]
pub(crate) struct LabelInput {
    pub(crate) name: String,
    pub(crate) catalog_number: Option<String>,
    pub(crate) external_id: Option<LabelExternalIdInput>,
}

#[derive(Debug, Clone)]
pub(crate) struct LabelExternalIdInput {
    pub(crate) provider_id: String,
    pub(crate) id_type: String,
    pub(crate) id_value: String,
}

/// Joined result for `get_for_release` / `get_for_releases_many`.
#[derive(Clone, Debug)]
pub(crate) struct LabelForRelease {
    pub(crate) label: Label,
    pub(crate) catalog_number: Option<String>,
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock drift")
        .as_secs()
}

/// Canonicalize label names so provider-emitted NFD and tag-path NFC
/// variants (e.g. "Béla Records") converge on one `Label` node.
pub(crate) fn normalize_label_name(name: &str) -> String {
    lyra_metadata::normalize_unicode_nfc(name).to_lowercase()
}

/// Normalize a catalog number for diff-key comparison: strip whitespace and
/// hyphens, then lowercase. Collapses "BN-1234", "BN 1234", "bn1234" to the
/// same key so trivial formatting variants don't inflate the diff set.
pub(crate) fn normalize_catalog_number(raw: &str) -> Option<String> {
    let normalized: String = raw
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '-')
        .collect::<String>()
        .to_lowercase();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

pub(crate) fn find_by_name(db: &impl DbAccess, name: &str) -> anyhow::Result<Option<DbId>> {
    super::lookup::find_id_by_indexed_string_field(
        db,
        "labels",
        "label_scan_name",
        "label_scan_name",
        &normalize_label_name(name),
    )
}

pub(crate) fn find_by_external_id(
    db: &impl DbAccess,
    provider_id: &str,
    id_type: &str,
    id_value: &str,
) -> anyhow::Result<Option<DbId>> {
    external_ids::get_owner(db, provider_id, id_type, id_value, Some("Label"))
}

fn insert_label(db: &mut impl DbAccess, name: &str) -> anyhow::Result<DbId> {
    let label = Label {
        db_id: None,
        id: nanoid!(),
        name: name.to_string(),
        label_scan_name: normalize_label_name(name),
        created_at: Some(now_secs()),
    };
    let result = db.exec_mut(QueryBuilder::insert().element(&label).query())?;
    let label_id = result
        .ids()
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("label creation returned no id"))?;
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from("labels")
            .to(label_id)
            .query(),
    )?;
    Ok(label_id)
}

/// Resolve by external ID, then name, else insert.
///
/// On external_id miss we insert-new instead of matching by name. "Universal",
/// "Columbia" and friends share surface strings across distinct corporate
/// entities; a cross-match would corrupt hundreds of releases in one rescan.
///
/// Use [`resolve_inside_tx`] when already inside a transaction.
pub(crate) fn resolve(db: &mut DbAny, request: &ResolveLabel) -> anyhow::Result<DbId> {
    db.transaction_mut(|t| resolve_inside_tx(t, request))
}

/// Transaction-capable variant of [`resolve`].
pub(crate) fn resolve_inside_tx(
    db: &mut impl DbAccess,
    request: &ResolveLabel,
) -> anyhow::Result<DbId> {
    let label_id = if let Some(ext) = &request.external_id {
        let existing = find_by_external_id(db, ext.provider_id, ext.id_type, ext.id_value)?;
        match existing {
            Some(id) => id,
            None => {
                // Narrow fallback: attach ext_id to a name-matched Label only
                // if that Label has no external_ids yet. Once any provider
                // has claimed it, a later ext_id miss on the same surface
                // name is more likely a separate entity than the same one —
                // fail-closed to insert-new rather than cross-merge.
                if let Some(by_name) = find_by_name(db, request.name)?
                    && external_ids::get_for_entity_inside_tx(db, by_name)?.is_empty()
                {
                    by_name
                } else {
                    insert_label(db, request.name)?
                }
            }
        }
    } else if let Some(id) = find_by_name(db, request.name)? {
        id
    } else {
        insert_label(db, request.name)?
    };

    if let Some(ext) = &request.external_id {
        external_ids::upsert_inside_tx(
            db,
            label_id,
            ext.provider_id,
            ext.id_type,
            ext.id_value,
            super::IdSource::Plugin,
        )?;
    }

    Ok(label_id)
}

/// Resolve-and-link a label to a release atomically. Prefer this over
/// [`resolve`] + [`upsert_release_label`] in separate transactions: a partial
/// failure between them would leave a `Label` without its `ReleaseLabel`,
/// and a retry would double-link.
///
/// Callers must check the release's lock state first and skip on locked
/// releases — resolving without linking leaks a Label that refcount GC
/// cannot reach.
pub(crate) fn add_label_to_release(
    db: &mut DbAny,
    release_id: DbId,
    request: &ResolveLabel,
    catalog_number: Option<&str>,
) -> anyhow::Result<DbId> {
    db.transaction_mut(|t| -> anyhow::Result<DbId> {
        let label_id = resolve_inside_tx(t, request)?;
        upsert_release_label(t, release_id, label_id, catalog_number)?;
        Ok(label_id)
    })
}

fn find_release_label(
    db: &impl DbAccess,
    release_id: DbId,
    label_id: DbId,
) -> anyhow::Result<Option<(DbId, ReleaseLabel)>> {
    let rls: Vec<ReleaseLabel> = db
        .exec(
            QueryBuilder::select()
                .elements::<ReleaseLabel>()
                .search()
                .from(release_id)
                .where_()
                .distance(CountComparison::Equal(2))
                .and()
                .key("db_element_id")
                .value("ReleaseLabel")
                .query(),
        )?
        .try_into()?;

    for rl in rls {
        let Some(rl_db_id) = rl.db_id.clone().map(DbId::from) else {
            continue;
        };
        if super::graph::edge_exists(db, rl_db_id, label_id)? {
            return Ok(Some((rl_db_id, rl)));
        }
    }
    Ok(None)
}

fn insert_release_label(
    db: &mut impl DbAccess,
    release_id: DbId,
    label_id: DbId,
    catalog_number: Option<&str>,
) -> anyhow::Result<DbId> {
    let cat_trim = catalog_number.and_then(|c| {
        let trimmed = c.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    });
    let scan_cat = cat_trim.as_deref().and_then(normalize_catalog_number);

    let rl = ReleaseLabel {
        db_id: None,
        id: nanoid!(),
        catalog_number: cat_trim,
        scan_catalog_number: scan_cat,
    };
    let result = db.exec_mut(QueryBuilder::insert().element(&rl).query())?;
    let rl_id = result
        .ids()
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("release_label creation returned no id"))?;

    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from("release_labels")
            .to(rl_id)
            .query(),
    )?;
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(release_id)
            .to(rl_id)
            .query(),
    )?;
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(rl_id)
            .to(label_id)
            .query(),
    )?;

    Ok(rl_id)
}

fn update_release_label_catalog(
    db: &mut impl DbAccess,
    rl_id: DbId,
    catalog_number: Option<&str>,
) -> anyhow::Result<()> {
    let cat_trim = catalog_number.and_then(|c| {
        let trimmed = c.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    });
    let scan_cat = cat_trim.as_deref().and_then(normalize_catalog_number);

    // `DbElement` requires every field — rewrite the node's values rather than
    // trying to mutate individual keys. Keep `id` stable.
    let existing: Vec<ReleaseLabel> = db
        .exec(QueryBuilder::select().ids(rl_id).query())?
        .try_into()?;
    let Some(existing) = existing.into_iter().next() else {
        anyhow::bail!("release_label {} disappeared mid-update", rl_id.0);
    };

    let updated = ReleaseLabel {
        db_id: existing.db_id,
        id: existing.id,
        catalog_number: cat_trim,
        scan_catalog_number: scan_cat,
    };
    db.exec_mut(QueryBuilder::insert().element(&updated).query())?;
    Ok(())
}

/// Upsert the `(release_id, label_id)` pairing's catalog number.
/// Caller-priority wins; policy (a) enforces one `ReleaseLabel` per pair.
///
/// Call through [`sync_release_labels`] or [`add_label_to_release`] — both
/// wrap this with label resolution under a single transaction. Calling it
/// directly outside one risks half-built state on partial failure.
fn upsert_release_label(
    db: &mut impl DbAccess,
    release_id: DbId,
    label_id: DbId,
    catalog_number: Option<&str>,
) -> anyhow::Result<DbId> {
    if let Some((rl_id, _existing)) = find_release_label(db, release_id, label_id)? {
        update_release_label_catalog(db, rl_id, catalog_number)?;
        Ok(rl_id)
    } else {
        insert_release_label(db, release_id, label_id, catalog_number)
    }
}

fn label_has_other_release_labels(
    db: &impl DbAccess,
    label_id: DbId,
    excluding: DbId,
) -> anyhow::Result<bool> {
    let result = db.exec(
        QueryBuilder::search()
            .to(label_id)
            .where_()
            .distance(CountComparison::Equal(2))
            .and()
            .key("db_element_id")
            .value("ReleaseLabel")
            .query(),
    )?;
    for id in result.ids() {
        if id != excluding && id.0 > 0 {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Move all `(label, catalog_number)` pairings from loser onto winner during
/// release dedup. Winner's existing entries are preserved on conflict; the
/// loser's stale RLs are left for the caller's cascade step.
///
/// Call BEFORE the loser's cascade deletion so the shared Label isn't GC'd
/// while we still need it.
pub(crate) fn migrate_release_labels(
    db: &mut DbAny,
    winner: DbId,
    loser: DbId,
) -> anyhow::Result<()> {
    if winner == loser {
        return Ok(());
    }

    let loser_entries = {
        let db_ref: &DbAny = db;
        get_release_label_entries(db_ref, loser)?
    };
    if loser_entries.is_empty() {
        return Ok(());
    }

    let winner_label_ids: HashSet<DbId> = get_release_label_entries(db, winner)?
        .into_iter()
        .map(|e| e.label_id)
        .collect();

    db.transaction_mut(|t| -> anyhow::Result<()> {
        for entry in loser_entries {
            if winner_label_ids.contains(&entry.label_id) {
                continue;
            }
            upsert_release_label(t, winner, entry.label_id, entry.catalog_number.as_deref())?;
        }
        Ok(())
    })
}

/// Remove every `ReleaseLabel` owned by a release before the release node is
/// deleted. agdb cascades the outgoing edges, but the `ReleaseLabel` nodes
/// and their linked Labels would persist — blocking Label GC indefinitely.
/// Call from the Release-cascade path only; use [`sync_release_labels`] for
/// single-label unlinks.
pub(crate) fn cascade_remove_release_labels_for_owner(
    db: &mut impl DbAccess,
    owner_id: DbId,
) -> anyhow::Result<()> {
    let entries = get_release_label_entries(db, owner_id)?;
    for entry in entries {
        remove_release_label(db, owner_id, entry.rl_id)?;
    }
    Ok(())
}

/// Remove a `ReleaseLabel` and GC its `Label` if this was the last reference.
/// The Label's `ExternalId`s cascade with it.
fn remove_release_label(
    db: &mut impl DbAccess,
    release_id: DbId,
    rl_id: DbId,
) -> anyhow::Result<()> {
    let label_candidates = db.exec(
        QueryBuilder::search()
            .from(rl_id)
            .where_()
            .distance(CountComparison::Equal(2))
            .and()
            .key("db_element_id")
            .value("Label")
            .query(),
    )?;
    let label_id = label_candidates.ids().into_iter().find(|id| id.0 > 0);

    super::graph::remove_edges_between(db, release_id, rl_id)?;
    db.exec_mut(QueryBuilder::remove().ids(rl_id).query())?;

    if let Some(label_id) = label_id {
        if !label_has_other_release_labels(db, label_id, rl_id)? {
            external_ids::remove_all_for_owner(db, label_id)?;
            db.exec_mut(QueryBuilder::remove().ids(label_id).query())?;
        }
    }

    Ok(())
}

fn desired_diff_key(label_id: DbId, catalog_number: Option<&str>) -> (DbId, Option<String>) {
    let scan_cat = catalog_number.and_then(normalize_catalog_number);
    (label_id, scan_cat)
}

/// Reconcile the labels on a release with the desired set. Wraps the entire
/// diff, upsert, and orphan-GC sequence in a single `transaction_mut` so
/// partial failures roll back cleanly. Policy (a): duplicate inputs collapse
/// to one `ReleaseLabel` per `(release_id, label_id)` pair.
pub(crate) fn sync_release_labels(
    db: &mut DbAny,
    release_id: DbId,
    inputs: &[LabelInput],
) -> anyhow::Result<()> {
    db.transaction_mut(|t| -> anyhow::Result<()> {
        sync_release_labels_inside_tx(t, release_id, inputs)
    })
}

/// Transaction-capable variant of [`sync_release_labels`].
pub(crate) fn sync_release_labels_inside_tx(
    db: &mut impl DbAccess,
    release_id: DbId,
    inputs: &[LabelInput],
) -> anyhow::Result<()> {
    let existing = get_release_label_entries(db, release_id)?;

    // Policy (a): duplicate inputs for the same label collapse. Last-write
    // wins on catalog number; the upsert loop runs once per unique label.
    let mut desired_keys: HashSet<(DbId, Option<String>)> = HashSet::new();
    let mut desired_by_label: HashMap<DbId, Option<String>> = HashMap::new();
    for input in inputs {
        let ext = input.external_id.as_ref().map(|e| ResolveExternalId {
            provider_id: &e.provider_id,
            id_type: &e.id_type,
            id_value: &e.id_value,
        });
        let label_id = resolve_inside_tx(
            db,
            &ResolveLabel {
                name: &input.name,
                external_id: ext,
            },
        )?;
        let key = desired_diff_key(label_id, input.catalog_number.as_deref());
        if desired_keys.insert(key) {
            desired_by_label.insert(label_id, input.catalog_number.clone());
        }
    }

    for (label_id, catalog_number) in &desired_by_label {
        upsert_release_label(db, release_id, *label_id, catalog_number.as_deref())?;
    }

    for entry in existing {
        if !desired_by_label.contains_key(&entry.label_id) {
            remove_release_label(db, release_id, entry.rl_id)?;
        }
    }

    Ok(())
}

/// Invariant: every `ReleaseLabel` has exactly one outbound edge to a `Label`.
/// Enforced by [`insert_release_label`] plus the single `transaction_mut`
/// wrapping sync; a missing edge means graph corruption — read path logs
/// and skips.
#[derive(Debug, Clone)]
struct ReleaseLabelEntry {
    rl_id: DbId,
    label_id: DbId,
    catalog_number: Option<String>,
}

fn get_release_label_entries(
    db: &impl DbAccess,
    release_id: DbId,
) -> anyhow::Result<Vec<ReleaseLabelEntry>> {
    let rls: Vec<ReleaseLabel> = db
        .exec(
            QueryBuilder::select()
                .elements::<ReleaseLabel>()
                .search()
                .from(release_id)
                .where_()
                .distance(CountComparison::Equal(2))
                .and()
                .key("db_element_id")
                .value("ReleaseLabel")
                .query(),
        )?
        .try_into()?;

    let mut entries = Vec::with_capacity(rls.len());
    for rl in rls {
        let Some(rl_db_id) = rl.db_id.clone().map(DbId::from) else {
            continue;
        };
        let label_result = db.exec(
            QueryBuilder::search()
                .from(rl_db_id)
                .where_()
                .distance(CountComparison::Equal(2))
                .and()
                .key("db_element_id")
                .value("Label")
                .query(),
        )?;
        let Some(label_id) = label_result.ids().into_iter().find(|id| id.0 > 0) else {
            tracing::warn!(
                release_id = release_id.0,
                release_label_id = rl_db_id.0,
                "release label has no outbound Label edge; skipping (graph invariant violated)"
            );
            continue;
        };
        entries.push(ReleaseLabelEntry {
            rl_id: rl_db_id,
            label_id,
            catalog_number: rl.catalog_number,
        });
    }
    Ok(entries)
}

pub(crate) fn get_for_release(
    db: &impl DbAccess,
    release_id: DbId,
) -> anyhow::Result<Vec<LabelForRelease>> {
    let entries = get_release_label_entries(db, release_id)?;
    if entries.is_empty() {
        return Ok(Vec::new());
    }

    let label_ids: Vec<DbId> = entries.iter().map(|e| e.label_id).collect();
    let labels_by_id: HashMap<DbId, Label> =
        super::graph::bulk_fetch_typed(db, label_ids, "Label")?;

    let mut result = Vec::with_capacity(entries.len());
    for entry in entries {
        let Some(label) = labels_by_id.get(&entry.label_id).cloned() else {
            continue;
        };
        result.push(LabelForRelease {
            label,
            catalog_number: entry.catalog_number,
        });
    }
    Ok(result)
}

pub(crate) fn get_for_releases_many(
    db: &impl DbAccess,
    release_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Vec<LabelForRelease>>> {
    let unique_release_ids = super::dedup_positive_ids(release_ids);
    let mut result: HashMap<DbId, Vec<LabelForRelease>> = unique_release_ids
        .iter()
        .copied()
        .map(|id| (id, Vec::new()))
        .collect();
    if unique_release_ids.is_empty() {
        return Ok(result);
    }

    let mut entries_by_release: HashMap<DbId, Vec<ReleaseLabelEntry>> = HashMap::new();
    let mut all_label_ids: Vec<DbId> = Vec::new();
    let mut seen = HashSet::new();

    for &release_id in &unique_release_ids {
        let entries = get_release_label_entries(db, release_id)?;
        for entry in &entries {
            if seen.insert(entry.label_id) {
                all_label_ids.push(entry.label_id);
            }
        }
        entries_by_release.insert(release_id, entries);
    }

    if all_label_ids.is_empty() {
        return Ok(result);
    }

    let labels_by_id: HashMap<DbId, Label> =
        super::graph::bulk_fetch_typed(db, all_label_ids, "Label")?;

    for &release_id in &unique_release_ids {
        let Some(entries) = entries_by_release.remove(&release_id) else {
            continue;
        };
        let Some(bucket) = result.get_mut(&release_id) else {
            continue;
        };
        for entry in entries {
            let Some(label) = labels_by_id.get(&entry.label_id).cloned() else {
                continue;
            };
            bucket.push(LabelForRelease {
                label,
                catalog_number: entry.catalog_number,
            });
        }
    }
    Ok(result)
}

pub(crate) fn get_all(db: &impl DbAccess) -> anyhow::Result<Vec<Label>> {
    let labels: Vec<Label> = db
        .exec(
            QueryBuilder::select()
                .elements::<Label>()
                .search()
                .from("labels")
                .where_()
                .distance(CountComparison::Equal(2))
                .query(),
        )?
        .try_into()?;
    Ok(labels)
}

pub(crate) fn get_by_id(db: &impl DbAccess, label_id: DbId) -> anyhow::Result<Option<Label>> {
    super::graph::fetch_typed_by_id(db, label_id, "Label")
}

/// Return `(release_db_id, catalog_number)` pairs for every release linked to
/// a label. Callers that also need display IDs should pipe the release IDs
/// through `lookup::find_ids_by_db_ids` for a single bulk resolve.
pub(crate) fn get_releases_with_catalog(
    db: &impl DbAccess,
    label_id: DbId,
) -> anyhow::Result<Vec<(DbId, Option<String>)>> {
    let rls: Vec<ReleaseLabel> = db
        .exec(
            QueryBuilder::select()
                .elements::<ReleaseLabel>()
                .search()
                .to(label_id)
                .where_()
                .distance(CountComparison::Equal(2))
                .and()
                .key("db_element_id")
                .value("ReleaseLabel")
                .query(),
        )?
        .try_into()?;

    let mut result = Vec::with_capacity(rls.len());
    for rl in rls {
        let Some(rl_db_id) = rl.db_id.clone().map(DbId::from) else {
            continue;
        };
        let release_result = db.exec(
            QueryBuilder::search()
                .to(rl_db_id)
                .where_()
                .distance(CountComparison::Equal(2))
                .and()
                .key("db_element_id")
                .value("Release")
                .query(),
        )?;
        let Some(release_id) = release_result.ids().into_iter().find(|id| id.0 > 0) else {
            continue;
        };
        result.push((release_id, rl.catalog_number));
    }
    Ok(result)
}

/// Return the release IDs linked to a label via `ReleaseLabel` intermediates.
pub(crate) fn get_releases(db: &impl DbAccess, label_id: DbId) -> anyhow::Result<Vec<DbId>> {
    // Walk back from Label (distance=2 → ReleaseLabel, distance=4 → Release via
    // the owning edge on ReleaseLabel).
    let result = db.exec(
        QueryBuilder::search()
            .to(label_id)
            .where_()
            .distance(CountComparison::Equal(4))
            .and()
            .key("db_element_id")
            .value("Release")
            .query(),
    )?;
    let mut ids: Vec<DbId> = result.ids().into_iter().filter(|id| id.0 > 0).collect();
    ids.sort_by_key(|id| id.0);
    ids.dedup();
    Ok(ids)
}

pub(crate) fn get_releases_many(
    db: &impl DbAccess,
    label_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Vec<DbId>>> {
    let unique_label_ids = super::dedup_positive_ids(label_ids);
    let mut result: HashMap<DbId, Vec<DbId>> = unique_label_ids
        .iter()
        .copied()
        .map(|id| (id, Vec::new()))
        .collect();
    for &label_id in &unique_label_ids {
        let release_ids = get_releases(db, label_id)?;
        result.insert(label_id, release_ids);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::{
        insert_release,
        new_test_db,
    };

    fn resolve_simple(db: &mut DbAny, name: &str) -> anyhow::Result<DbId> {
        resolve(
            db,
            &ResolveLabel {
                name,
                external_id: None,
            },
        )
    }

    fn mb(id: &str) -> LabelExternalIdInput {
        LabelExternalIdInput {
            provider_id: "musicbrainz".to_string(),
            id_type: "label_id".to_string(),
            id_value: id.to_string(),
        }
    }

    #[test]
    fn resolve_creates_label_on_first_call() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        resolve_simple(&mut db, "Blue Note")?;
        assert!(find_by_name(&db, "Blue Note")?.is_some());
        Ok(())
    }

    #[test]
    fn resolve_deduplicates_by_scan_name() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let first = resolve_simple(&mut db, "Blue Note")?;
        let second = resolve_simple(&mut db, "blue note")?;
        assert_eq!(first, second);
        Ok(())
    }

    #[test]
    fn resolve_matches_by_external_id() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let first = resolve(
            &mut db,
            &ResolveLabel {
                name: "Blue Note",
                external_id: Some(ResolveExternalId {
                    provider_id: "musicbrainz",
                    id_type: "label_id",
                    id_value: "bn-001",
                }),
            },
        )?;
        let second = resolve(
            &mut db,
            &ResolveLabel {
                name: "Blue Note Records",
                external_id: Some(ResolveExternalId {
                    provider_id: "musicbrainz",
                    id_type: "label_id",
                    id_value: "bn-001",
                }),
            },
        )?;
        assert_eq!(first, second);
        Ok(())
    }

    #[test]
    fn resolve_ext_id_miss_attaches_to_unclaimed_name_match() -> anyhow::Result<()> {
        // Ingest-then-enrich flow: the tag path creates a bare Label by name
        // (no external_id). A later plugin enrichment emits
        // `{name, ext_id: Some(...)}`. The name-matched Label has no ext_id
        // for this (provider, type), so narrow-fallback attaches the ext_id
        // to it instead of creating a duplicate. Two EMIs must not coexist
        // when one was bare and got enriched.
        let mut db = new_test_db()?;

        let by_name = resolve_simple(&mut db, "EMI")?;

        let enriched = resolve(
            &mut db,
            &ResolveLabel {
                name: "EMI",
                external_id: Some(ResolveExternalId {
                    provider_id: "musicbrainz",
                    id_type: "label_id",
                    id_value: "emi-mbid",
                }),
            },
        )?;

        assert_eq!(
            by_name, enriched,
            "unclaimed name-matched Label should gain the ext_id, not be shadowed by a duplicate",
        );
        assert_eq!(get_all(&db)?.len(), 1);
        Ok(())
    }

    #[test]
    fn resolve_ext_id_miss_does_not_steal_label_with_claimed_ext_id() -> anyhow::Result<()> {
        // Safety property: once any provider has claimed the name-matched
        // Label with an ext_id, a later ext_id miss must insert new. Same
        // (provider, type) with different id_value is the canonical case
        // (Universal Music vs Universal Pictures on the same MB type).
        let mut db = new_test_db()?;

        // First call: Label "Universal" with MBID "universal-music".
        let umg = resolve(
            &mut db,
            &ResolveLabel {
                name: "Universal",
                external_id: Some(ResolveExternalId {
                    provider_id: "musicbrainz",
                    id_type: "label_id",
                    id_value: "universal-music",
                }),
            },
        )?;

        // Second call: same name, different MBID. The existing Label has a
        // claimed ext_id for this (provider, type), so we must insert new.
        let other = resolve(
            &mut db,
            &ResolveLabel {
                name: "Universal",
                external_id: Some(ResolveExternalId {
                    provider_id: "musicbrainz",
                    id_type: "label_id",
                    id_value: "universal-pictures",
                }),
            },
        )?;

        assert_ne!(
            umg, other,
            "conflicting ext_id for name-matched Label must insert new, not cross-merge",
        );
        assert_eq!(get_all(&db)?.len(), 2);
        Ok(())
    }

    #[test]
    fn resolve_ext_id_miss_does_not_cross_merge_across_providers() -> anyhow::Result<()> {
        // Cross-provider cross-merge guard: Discogs claims "Universal" first
        // with its own ID; MusicBrainz then arrives with a DIFFERENT real
        // entity that also happens to be named "Universal." The narrow
        // fallback must NOT attach MB's ID to the Discogs-backed Label just
        // because MB had no prior claim on it — two distinct corporate
        // entities would collapse. Once any provider has claimed a Label,
        // subsequent ext_id misses from other providers must insert new.
        let mut db = new_test_db()?;

        let discogs = resolve(
            &mut db,
            &ResolveLabel {
                name: "Universal",
                external_id: Some(ResolveExternalId {
                    provider_id: "discogs",
                    id_type: "label_id",
                    id_value: "111",
                }),
            },
        )?;

        let mb = resolve(
            &mut db,
            &ResolveLabel {
                name: "Universal",
                external_id: Some(ResolveExternalId {
                    provider_id: "musicbrainz",
                    id_type: "label_id",
                    id_value: "222",
                }),
            },
        )?;

        assert_ne!(
            discogs, mb,
            "MB ext_id miss must insert new when the name-matched Label already has a Discogs claim",
        );
        assert_eq!(get_all(&db)?.len(), 2);
        Ok(())
    }

    #[test]
    fn normalize_catalog_number_collapses_variants() {
        assert_eq!(
            normalize_catalog_number("BN-1234"),
            Some("bn1234".to_string())
        );
        assert_eq!(
            normalize_catalog_number("BN 1234"),
            Some("bn1234".to_string())
        );
        assert_eq!(
            normalize_catalog_number("bn1234"),
            Some("bn1234".to_string())
        );
        assert_eq!(
            normalize_catalog_number("  BN - 1234  "),
            Some("bn1234".to_string())
        );
        assert_eq!(normalize_catalog_number(""), None);
        assert_eq!(normalize_catalog_number("   "), None);
        assert_eq!(normalize_catalog_number("--"), None);
    }

    #[test]
    fn upsert_release_label_creates_on_first_call() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let label_id = resolve_simple(&mut db, "Blue Note")?;
        let release_id = insert_release(&mut db, "Blue Train")?;

        upsert_release_label(&mut db, release_id, label_id, Some("BN-1577"))?;

        let labels = get_for_release(&db, release_id)?;
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].label.name, "Blue Note");
        assert_eq!(labels[0].catalog_number.as_deref(), Some("BN-1577"));
        Ok(())
    }

    #[test]
    fn upsert_release_label_updates_value_not_duplicates_edge() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let label_id = resolve_simple(&mut db, "Blue Note")?;
        let release_id = insert_release(&mut db, "Blue Train")?;

        upsert_release_label(&mut db, release_id, label_id, Some("123"))?;
        upsert_release_label(&mut db, release_id, label_id, Some("BN-1577"))?;

        let labels = get_for_release(&db, release_id)?;
        assert_eq!(labels.len(), 1, "duplicate edges must not accumulate");
        assert_eq!(labels[0].catalog_number.as_deref(), Some("BN-1577"));
        Ok(())
    }

    #[test]
    fn sync_release_labels_adds_and_removes() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Changes")?;

        sync_release_labels(
            &mut db,
            release_id,
            &[
                LabelInput {
                    name: "Blue Note".to_string(),
                    catalog_number: Some("BN-1".to_string()),
                    external_id: None,
                },
                LabelInput {
                    name: "Impulse!".to_string(),
                    catalog_number: None,
                    external_id: None,
                },
            ],
        )?;
        let labels = get_for_release(&db, release_id)?;
        assert_eq!(labels.len(), 2);

        sync_release_labels(
            &mut db,
            release_id,
            &[LabelInput {
                name: "Impulse!".to_string(),
                catalog_number: Some("A-77".to_string()),
                external_id: None,
            }],
        )?;
        let labels = get_for_release(&db, release_id)?;
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].label.name, "Impulse!");
        assert_eq!(labels[0].catalog_number.as_deref(), Some("A-77"));
        Ok(())
    }

    #[test]
    fn sync_release_labels_empty_drops_all() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Gone")?;

        sync_release_labels(
            &mut db,
            release_id,
            &[LabelInput {
                name: "Blue Note".to_string(),
                catalog_number: None,
                external_id: None,
            }],
        )?;
        assert_eq!(get_for_release(&db, release_id)?.len(), 1);

        sync_release_labels(&mut db, release_id, &[])?;
        assert_eq!(get_for_release(&db, release_id)?.len(), 0);
        Ok(())
    }

    #[test]
    fn orphaned_label_is_gced_when_last_release_unlinks() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_a = insert_release(&mut db, "A")?;
        let release_b = insert_release(&mut db, "B")?;

        sync_release_labels(
            &mut db,
            release_a,
            &[LabelInput {
                name: "Blue Note".to_string(),
                catalog_number: Some("BN-1".to_string()),
                external_id: None,
            }],
        )?;
        sync_release_labels(
            &mut db,
            release_b,
            &[LabelInput {
                name: "Blue Note".to_string(),
                catalog_number: Some("BN-2".to_string()),
                external_id: None,
            }],
        )?;

        let label_id = find_by_name(&db, "Blue Note")?.expect("Blue Note exists");

        // Drop from A: Label still referenced by B, not GC'd.
        sync_release_labels(&mut db, release_a, &[])?;
        assert!(find_by_name(&db, "Blue Note")?.is_some());

        // Drop from B: Label now orphaned and GC'd.
        sync_release_labels(&mut db, release_b, &[])?;
        assert!(find_by_name(&db, "Blue Note")?.is_none());
        assert!(get_by_id(&db, label_id)?.is_none());
        Ok(())
    }

    #[test]
    fn orphan_gc_removes_external_ids() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "OnlyRelease")?;

        sync_release_labels(
            &mut db,
            release_id,
            &[LabelInput {
                name: "Blue Note".to_string(),
                catalog_number: None,
                external_id: Some(mb("bn-001")),
            }],
        )?;
        assert!(
            external_ids::get_owner(&db, "musicbrainz", "label_id", "bn-001", Some("Label"))?
                .is_some()
        );

        sync_release_labels(&mut db, release_id, &[])?;
        assert!(
            external_ids::get_owner(&db, "musicbrainz", "label_id", "bn-001", Some("Label"))?
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn sync_release_labels_updates_cat_number_on_same_label() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Reissue")?;

        sync_release_labels(
            &mut db,
            release_id,
            &[LabelInput {
                name: "Blue Note".to_string(),
                catalog_number: Some("BN-1".to_string()),
                external_id: None,
            }],
        )?;
        sync_release_labels(
            &mut db,
            release_id,
            &[LabelInput {
                name: "Blue Note".to_string(),
                catalog_number: Some("BN-2".to_string()),
                external_id: None,
            }],
        )?;

        let labels = get_for_release(&db, release_id)?;
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].catalog_number.as_deref(), Some("BN-2"));
        Ok(())
    }

    #[test]
    fn get_for_releases_many_is_single_pass() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let a = insert_release(&mut db, "A")?;
        let b = insert_release(&mut db, "B")?;
        let c = insert_release(&mut db, "C")?;

        sync_release_labels(
            &mut db,
            a,
            &[LabelInput {
                name: "Blue Note".to_string(),
                catalog_number: Some("BN-1".to_string()),
                external_id: None,
            }],
        )?;
        sync_release_labels(
            &mut db,
            b,
            &[
                LabelInput {
                    name: "Blue Note".to_string(),
                    catalog_number: Some("BN-2".to_string()),
                    external_id: None,
                },
                LabelInput {
                    name: "Impulse!".to_string(),
                    catalog_number: None,
                    external_id: None,
                },
            ],
        )?;
        // C has no labels.

        let map = get_for_releases_many(&db, &[a, b, c])?;
        assert_eq!(map.get(&a).unwrap().len(), 1);
        assert_eq!(map.get(&b).unwrap().len(), 2);
        assert_eq!(map.get(&c).unwrap().len(), 0);
        Ok(())
    }

    #[test]
    fn get_releases_returns_releases_for_label() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let a = insert_release(&mut db, "A")?;
        let b = insert_release(&mut db, "B")?;

        sync_release_labels(
            &mut db,
            a,
            &[LabelInput {
                name: "Blue Note".to_string(),
                catalog_number: None,
                external_id: None,
            }],
        )?;
        sync_release_labels(
            &mut db,
            b,
            &[LabelInput {
                name: "Blue Note".to_string(),
                catalog_number: None,
                external_id: None,
            }],
        )?;

        let label_id = find_by_name(&db, "Blue Note")?.unwrap();
        let mut ids = get_releases(&db, label_id)?;
        ids.sort_by_key(|id| id.0);
        let mut expected = vec![a, b];
        expected.sort_by_key(|id| id.0);
        assert_eq!(ids, expected);
        Ok(())
    }

    #[test]
    fn get_all_lists_all_labels() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        resolve_simple(&mut db, "Blue Note")?;
        resolve_simple(&mut db, "Impulse!")?;
        resolve_simple(&mut db, "ECM")?;

        let names: HashSet<String> = get_all(&db)?.into_iter().map(|l| l.name).collect();
        assert_eq!(names.len(), 3);
        assert!(names.contains("Blue Note"));
        assert!(names.contains("Impulse!"));
        assert!(names.contains("ECM"));
        Ok(())
    }

    #[test]
    fn migrate_release_labels_moves_pairings_and_preserves_orphaned_labels() -> anyhow::Result<()> {
        // Release dedup path: winner keeps its labels, gains loser's labels
        // that it didn't already have. Loser's labels remain reachable
        // through the winner, so the subsequent cascade-delete of the loser
        // doesn't orphan them.
        let mut db = new_test_db()?;
        let winner = insert_release(&mut db, "Winner")?;
        let loser = insert_release(&mut db, "Loser")?;

        sync_release_labels(
            &mut db,
            winner,
            &[LabelInput {
                name: "Blue Note".to_string(),
                catalog_number: Some("BN-1".to_string()),
                external_id: None,
            }],
        )?;
        sync_release_labels(
            &mut db,
            loser,
            &[
                LabelInput {
                    name: "Blue Note".to_string(),
                    catalog_number: Some("BN-2".to_string()),
                    external_id: None,
                },
                LabelInput {
                    name: "Impulse!".to_string(),
                    catalog_number: Some("A-77".to_string()),
                    external_id: None,
                },
            ],
        )?;

        migrate_release_labels(&mut db, winner, loser)?;

        // Winner has both labels. `Blue Note` keeps the winner's cat# (winner
        // authority); `Impulse!` arrives with the loser's cat#.
        let mut winner_labels = get_for_release(&db, winner)?;
        winner_labels.sort_by(|a, b| a.label.name.cmp(&b.label.name));
        assert_eq!(winner_labels.len(), 2);
        assert_eq!(winner_labels[0].label.name, "Blue Note");
        assert_eq!(winner_labels[0].catalog_number.as_deref(), Some("BN-1"));
        assert_eq!(winner_labels[1].label.name, "Impulse!");
        assert_eq!(winner_labels[1].catalog_number.as_deref(), Some("A-77"));

        // Simulate the caller's cascade of the loser and verify that the
        // Labels the winner now shares survive (winner is still a referrer).
        cascade_remove_release_labels_for_owner(&mut db, loser)?;
        assert!(find_by_name(&db, "Blue Note")?.is_some());
        assert!(find_by_name(&db, "Impulse!")?.is_some());
        Ok(())
    }

    #[test]
    fn cascade_remove_release_labels_for_owner_drops_rls_and_gcs_orphan_label() -> anyhow::Result<()>
    {
        // When a Release is about to be deleted (e.g. by the
        // cleanup-orphaned-metadata sweep), its ReleaseLabels must be removed
        // first — agdb only cascades edges, not the ReleaseLabel nodes, so
        // without the explicit walk the RLs and their Labels would leak.
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Only")?;
        sync_release_labels(
            &mut db,
            release_id,
            &[LabelInput {
                name: "Blue Note".to_string(),
                catalog_number: Some("BN-1".to_string()),
                external_id: Some(mb("bn-1")),
            }],
        )?;
        assert!(find_by_name(&db, "Blue Note")?.is_some());

        cascade_remove_release_labels_for_owner(&mut db, release_id)?;

        // No ReleaseLabel left for this release.
        assert_eq!(get_for_release(&db, release_id)?.len(), 0);
        // Label was the only reference — must be GC'd.
        assert!(find_by_name(&db, "Blue Note")?.is_none());
        // ExternalId cascaded with the Label.
        assert!(
            external_ids::get_owner(&db, "musicbrainz", "label_id", "bn-1", Some("Label"))?
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn label_scan_name_is_isolated_from_artist_scan_name() -> anyhow::Result<()> {
        // Insert an Artist named "EMI" and a Label named "EMI". Each lives in
        // its own collection, with its own index key (`scan_name` vs
        // `label_scan_name`). `find_by_name` must return only the Label, and
        // the Artist resolution path (see services::metadata::ingestion::artists)
        // must only return the Artist.
        let mut db = new_test_db()?;
        let artist_id = crate::db::test_db::insert_artist(&mut db, "EMI")?;
        let label_id = resolve_simple(&mut db, "EMI")?;

        assert_ne!(artist_id, label_id);
        assert_eq!(find_by_name(&db, "EMI")?, Some(label_id));
        Ok(())
    }
}
