// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};
use std::time::{
    SystemTime,
    UNIX_EPOCH,
};

use agdb::{
    DbAny,
    DbId,
};

use crate::db::{
    self,
    Tag,
    tags::{
        CreateOutcome,
        HAS_MANY_CAP,
        RenameConflict,
        TagNormalizeError,
    },
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum TagServiceError {
    #[error("tag name is invalid: {0}")]
    BadTagName(TagNormalizeError),
    #[error("color cannot be empty")]
    EmptyColor,
    #[error("target is not a supported kind")]
    NotTargetable,
    #[error("tag not found")]
    NotFound,
    #[error("tag name already exists for this user")]
    RenameConflict,
    #[error("empty patch body — at least one of `tag` or `color` must be provided")]
    EmptyPatch,
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CreateResult {
    Created,
    Reused,
}

/// Create or reuse a tag, then attach the target. Resolves + whitelists + writes atomically.
/// Color is ignored on reuse.
pub(crate) fn create(
    db: &mut DbAny,
    owner_db_id: DbId,
    public_target_id: &str,
    raw_tag_name: &str,
    color: &str,
) -> Result<CreateResult, TagServiceError> {
    let normalized =
        db::tags::normalize_tag_name(raw_tag_name).map_err(TagServiceError::BadTagName)?;
    let color = color.trim();
    if color.is_empty() {
        return Err(TagServiceError::EmptyColor);
    }
    let color = color.to_string();
    let now_ms = now_ms().map_err(TagServiceError::Internal)?;

    db.transaction_mut(
        |t| -> anyhow::Result<Result<CreateResult, TagServiceError>> {
            let visibility = TargetVisibility::for_user(t, owner_db_id)?;
            let Some((target_db_id, _)) = resolve_targetable(t, &visibility, public_target_id)?
            else {
                return Ok(Err(TagServiceError::NotTargetable));
            };
            let (_, outcome) =
                db::tags::create(t, owner_db_id, target_db_id, &normalized, &color, now_ms)?;
            let result = match outcome {
                CreateOutcome::Created => CreateResult::Created,
                CreateOutcome::ReusedExisting => CreateResult::Reused,
            };
            Ok(Ok(result))
        },
    )
    .map_err(TagServiceError::Internal)?
}

/// Detach a target from the caller's named tag. No visibility gate.
pub(crate) fn remove_target_by_tag_id(
    db: &mut DbAny,
    owner_db_id: DbId,
    tag_db_id: DbId,
    public_target_id: &str,
) -> Result<(), TagServiceError> {
    ensure_owner(db, tag_db_id, owner_db_id)?;
    db.transaction_mut(|t| -> anyhow::Result<Result<(), TagServiceError>> {
        let Some((target_db_id, _)) = resolve_whitelisted(t, public_target_id)? else {
            return Ok(Err(TagServiceError::NotTargetable));
        };
        let tag = db::tags::get_by_id(t, tag_db_id)?.ok_or(TagServiceError::NotFound)?;
        db::tags::remove_target(t, owner_db_id, target_db_id, &tag.tag)?;
        Ok(Ok(()))
    })
    .map_err(TagServiceError::Internal)?
}

pub(crate) fn has_target_by_tag_id(
    db: &DbAny,
    owner_db_id: DbId,
    tag_db_id: DbId,
    public_target_id: &str,
) -> Result<bool, TagServiceError> {
    ensure_owner(db, tag_db_id, owner_db_id)?;
    let tag = db::tags::get_by_id(db, tag_db_id)
        .map_err(TagServiceError::Internal)?
        .ok_or(TagServiceError::NotFound)?;
    let visibility =
        TargetVisibility::for_user(db, owner_db_id).map_err(TagServiceError::Internal)?;
    let Some((target_db_id, _)) =
        resolve_targetable(db, &visibility, public_target_id).map_err(TagServiceError::Internal)?
    else {
        return Ok(false);
    };
    db::tags::has_target(db, owner_db_id, target_db_id, &tag.tag).map_err(TagServiceError::Internal)
}

/// Plugin-host batch check. Non-whitelisted or non-visible targets map to `false`.
pub(crate) fn has_targets_by_db_id(
    db: &DbAny,
    owner_db_id: DbId,
    target_db_ids: &[DbId],
    raw_tag_name: &str,
) -> Result<HashMap<DbId, bool>, TagServiceError> {
    if target_db_ids.len() > HAS_MANY_CAP {
        return Err(TagServiceError::Internal(anyhow::anyhow!(
            "has_targets cap exceeded: {} > {HAS_MANY_CAP}",
            target_db_ids.len()
        )));
    }
    let normalized =
        db::tags::normalize_tag_name(raw_tag_name).map_err(TagServiceError::BadTagName)?;
    let visibility =
        TargetVisibility::for_user(db, owner_db_id).map_err(TagServiceError::Internal)?;
    let mut visible: Vec<DbId> = Vec::with_capacity(target_db_ids.len());
    for &id in target_db_ids {
        if resolve_targetable_by_db_id(db, &visibility, id)
            .map_err(TagServiceError::Internal)?
            .is_some()
        {
            visible.push(id);
        }
    }
    let raw = db::tags::has_targets(db, owner_db_id, &visible, &normalized)
        .map_err(TagServiceError::Internal)?;
    let mut out: HashMap<DbId, bool> = target_db_ids
        .iter()
        .copied()
        .map(|id| (id, false))
        .collect();
    for (id, v) in raw {
        out.insert(id, v);
    }
    Ok(out)
}

pub(crate) fn get_for_target_by_db_id(
    db: &DbAny,
    owner_db_id: DbId,
    target_db_id: DbId,
) -> Result<Vec<Tag>, TagServiceError> {
    let visibility =
        TargetVisibility::for_user(db, owner_db_id).map_err(TagServiceError::Internal)?;
    let Some((target_db_id, _)) = resolve_targetable_by_db_id(db, &visibility, target_db_id)
        .map_err(TagServiceError::Internal)?
    else {
        return Ok(Vec::new());
    };
    db::tags::get_for_target(db, owner_db_id, target_db_id).map_err(TagServiceError::Internal)
}

pub(crate) fn get_for_targets_many_by_db_id(
    db: &DbAny,
    owner_db_id: DbId,
    target_db_ids: &[DbId],
) -> Result<HashMap<DbId, Vec<Tag>>, TagServiceError> {
    if target_db_ids.len() > HAS_MANY_CAP {
        return Err(TagServiceError::Internal(anyhow::anyhow!(
            "get_for_targets_many cap exceeded: {} > {HAS_MANY_CAP}",
            target_db_ids.len()
        )));
    }

    let visibility =
        TargetVisibility::for_user(db, owner_db_id).map_err(TagServiceError::Internal)?;
    let mut visible: Vec<DbId> = Vec::with_capacity(target_db_ids.len());
    let mut out: HashMap<DbId, Vec<Tag>> = target_db_ids
        .iter()
        .copied()
        .map(|id| (id, Vec::new()))
        .collect();
    for &id in target_db_ids {
        if resolve_targetable_by_db_id(db, &visibility, id)
            .map_err(TagServiceError::Internal)?
            .is_some()
        {
            visible.push(id);
        }
    }
    if visible.is_empty() {
        return Ok(out);
    }

    let raw = db::tags::get_for_targets_many(db, owner_db_id, &visible)
        .map_err(TagServiceError::Internal)?;
    for (id, tags) in raw {
        out.insert(id, tags);
    }
    Ok(out)
}

pub(crate) fn create_by_db_id(
    db: &mut DbAny,
    owner_db_id: DbId,
    target_db_id: DbId,
    raw_tag_name: &str,
    color: &str,
) -> Result<(CreateResult, String), TagServiceError> {
    let normalized =
        db::tags::normalize_tag_name(raw_tag_name).map_err(TagServiceError::BadTagName)?;
    let color = color.trim();
    if color.is_empty() {
        return Err(TagServiceError::EmptyColor);
    }
    let color = color.to_string();
    let now_ms = now_ms().map_err(TagServiceError::Internal)?;
    let normalized_clone = normalized.clone();

    db.transaction_mut(
        |t| -> anyhow::Result<Result<(CreateResult, String), TagServiceError>> {
            let visibility = TargetVisibility::for_user(t, owner_db_id)?;
            let Some((target_db_id, _)) =
                resolve_targetable_by_db_id(t, &visibility, target_db_id)?
            else {
                return Ok(Err(TagServiceError::NotTargetable));
            };
            let (_, outcome) = db::tags::create(
                t,
                owner_db_id,
                target_db_id,
                &normalized_clone,
                &color,
                now_ms,
            )?;
            let result = match outcome {
                CreateOutcome::Created => CreateResult::Created,
                CreateOutcome::ReusedExisting => CreateResult::Reused,
            };
            Ok(Ok((result, normalized_clone.clone())))
        },
    )
    .map_err(TagServiceError::Internal)?
}

/// No visibility gate.
pub(crate) fn remove_target_by_db_id(
    db: &mut DbAny,
    owner_db_id: DbId,
    target_db_id: DbId,
    raw_tag_name: &str,
) -> Result<(), TagServiceError> {
    let normalized =
        db::tags::normalize_tag_name(raw_tag_name).map_err(TagServiceError::BadTagName)?;

    db.transaction_mut(|t| -> anyhow::Result<Result<(), TagServiceError>> {
        let Some(_) = resolve_whitelisted_by_db_id(t, target_db_id)? else {
            return Ok(Err(TagServiceError::NotTargetable));
        };
        db::tags::remove_target(t, owner_db_id, target_db_id, &normalized)?;
        Ok(Ok(()))
    })
    .map_err(TagServiceError::Internal)?
}

pub(crate) fn has_target_by_db_id(
    db: &DbAny,
    owner_db_id: DbId,
    target_db_id: DbId,
    raw_tag_name: &str,
) -> Result<bool, TagServiceError> {
    let normalized =
        db::tags::normalize_tag_name(raw_tag_name).map_err(TagServiceError::BadTagName)?;
    let visibility =
        TargetVisibility::for_user(db, owner_db_id).map_err(TagServiceError::Internal)?;
    let Some((target_db_id, _)) = resolve_targetable_by_db_id(db, &visibility, target_db_id)
        .map_err(TagServiceError::Internal)?
    else {
        return Ok(false);
    };
    db::tags::has_target(db, owner_db_id, target_db_id, &normalized)
        .map_err(TagServiceError::Internal)
}

/// Targets tagged `raw_tag_name` by `owner`. Non-visible playlists are filtered out; underlying
/// edges persist in the graph.
pub(crate) fn get_tagged(
    db: &DbAny,
    owner_db_id: DbId,
    raw_tag_name: &str,
) -> Result<(Vec<DbId>, String), TagServiceError> {
    let normalized =
        db::tags::normalize_tag_name(raw_tag_name).map_err(TagServiceError::BadTagName)?;
    let visibility =
        TargetVisibility::for_user(db, owner_db_id).map_err(TagServiceError::Internal)?;
    let ids = db::tags::get_targets_by_tag(db, owner_db_id, &normalized)
        .map_err(TagServiceError::Internal)?;
    let mut visible = Vec::with_capacity(ids.len());
    for id in ids {
        if resolve_targetable_by_db_id(db, &visibility, id)
            .map_err(TagServiceError::Internal)?
            .is_some()
        {
            visible.push(id);
        }
    }
    Ok((visible, normalized))
}

/// Fetch a tag by public nanoid. Non-owner returns `NotFound` (opaque).
pub(crate) fn get_by_public_id(
    db: &DbAny,
    owner_db_id: DbId,
    public_tag_id: &str,
) -> Result<Tag, TagServiceError> {
    let Some(tag_db_id) =
        db::lookup::find_node_id_by_id(db, public_tag_id).map_err(TagServiceError::Internal)?
    else {
        return Err(TagServiceError::NotFound);
    };
    let tag = db::tags::get_by_id(db, tag_db_id)
        .map_err(TagServiceError::Internal)?
        .ok_or(TagServiceError::NotFound)?;
    ensure_owner(db, tag_db_id, owner_db_id)?;
    Ok(tag)
}

pub(crate) fn resolve_owned_tag_id(
    db: &DbAny,
    owner_db_id: DbId,
    public_tag_id: &str,
) -> Result<DbId, TagServiceError> {
    let Some(tag_db_id) =
        db::lookup::find_node_id_by_id(db, public_tag_id).map_err(TagServiceError::Internal)?
    else {
        return Err(TagServiceError::NotFound);
    };
    if db::tags::get_by_id(db, tag_db_id)
        .map_err(TagServiceError::Internal)?
        .is_none()
    {
        return Err(TagServiceError::NotFound);
    }
    ensure_owner(db, tag_db_id, owner_db_id)?;
    Ok(tag_db_id)
}

fn ensure_owner(
    db: &impl db::DbAccess,
    tag_db_id: DbId,
    owner_db_id: DbId,
) -> Result<(), TagServiceError> {
    let owner = db::tags::get_owner(db, tag_db_id).map_err(TagServiceError::Internal)?;
    match owner {
        Some(id) if id == owner_db_id => Ok(()),
        _ => Err(TagServiceError::NotFound),
    }
}

/// Update a tag's name and/or color. Empty patch → `EmptyPatch`; collision → `RenameConflict`.
pub(crate) fn update(
    db: &mut DbAny,
    owner_db_id: DbId,
    tag_db_id: DbId,
    raw_new_name: Option<&str>,
    new_color: Option<&str>,
) -> Result<Tag, TagServiceError> {
    ensure_owner(db, tag_db_id, owner_db_id)?;
    if raw_new_name.is_none() && new_color.is_none() {
        return Err(TagServiceError::EmptyPatch);
    }

    let normalized = raw_new_name
        .map(db::tags::normalize_tag_name)
        .transpose()
        .map_err(TagServiceError::BadTagName)?;
    let color_trimmed = new_color
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string);

    let result = db
        .transaction_mut(|t| -> anyhow::Result<Result<Tag, RenameConflict>> {
            db::tags::update(
                t,
                tag_db_id,
                normalized.as_deref(),
                color_trimmed.as_deref(),
            )
        })
        .map_err(TagServiceError::Internal)?;

    result.map_err(|_| TagServiceError::RenameConflict)
}

pub(crate) fn delete(
    db: &mut DbAny,
    owner_db_id: DbId,
    tag_db_id: DbId,
) -> Result<(), TagServiceError> {
    ensure_owner(db, tag_db_id, owner_db_id)?;
    db.transaction_mut(|t| -> anyhow::Result<()> {
        db::tags::delete(t, tag_db_id)?;
        Ok(())
    })
    .map_err(TagServiceError::Internal)
}

pub(crate) fn list_for_user(db: &DbAny, owner_db_id: DbId) -> Result<Vec<Tag>, TagServiceError> {
    db::tags::list_for_user(db, owner_db_id).map_err(TagServiceError::Internal)
}

pub(crate) fn hydrate_tag_snapshot(
    db: &DbAny,
    owner_db_id: DbId,
    public_tag_ids: &[String],
) -> Result<Vec<Tag>, TagServiceError> {
    let mut tags = Vec::with_capacity(public_tag_ids.len());
    for public_tag_id in public_tag_ids {
        match get_by_public_id(db, owner_db_id, public_tag_id) {
            Ok(tag) => tags.push(tag),
            Err(TagServiceError::NotFound) => {}
            Err(err) => return Err(err),
        }
    }
    Ok(tags)
}

#[derive(Debug)]
pub(crate) struct TargetListItem {
    edge_db_id: DbId,
    pub(crate) target_id: String,
}

impl TargetListItem {
    pub(crate) fn snapshot_id(&self) -> String {
        format!("{}:{}", self.edge_db_id.0, self.target_id)
    }
}

/// Target list for a tag, filtered by caller-side visibility.
pub(crate) fn list_targets(
    db: &DbAny,
    owner_db_id: DbId,
    tag_db_id: DbId,
) -> Result<Vec<TargetListItem>, TagServiceError> {
    ensure_owner(db, tag_db_id, owner_db_id)?;
    let targets = db::tags::list_targets(db, tag_db_id).map_err(TagServiceError::Internal)?;

    let visibility =
        TargetVisibility::for_user(db, owner_db_id).map_err(TagServiceError::Internal)?;
    let target_db_ids = targets
        .iter()
        .map(|target| target.target_db_id)
        .collect::<Vec<_>>();
    let target_ids =
        db::lookup::find_ids_by_db_ids(db, &target_db_ids).map_err(TagServiceError::Internal)?;
    let mut filtered = Vec::with_capacity(targets.len());
    for target in targets {
        if resolve_targetable_by_db_id(db, &visibility, target.target_db_id)
            .map_err(TagServiceError::Internal)?
            .is_some()
            && let Some(target_id) = target_ids.get(&target.target_db_id)
        {
            filtered.push(TargetListItem {
                edge_db_id: target.edge_db_id,
                target_id: target_id.clone(),
            });
        }
    }

    Ok(filtered)
}

pub(crate) fn hydrate_target_snapshot(
    db: &DbAny,
    owner_db_id: DbId,
    tag_db_id: DbId,
    snapshot_ids: &[String],
) -> Result<Vec<TargetListItem>, TagServiceError> {
    ensure_owner(db, tag_db_id, owner_db_id)?;
    let identities = snapshot_ids
        .iter()
        .filter_map(|snapshot_id| {
            let (edge_db_id, target_id) = snapshot_id.split_once(':')?;
            Some((DbId(edge_db_id.parse::<i64>().ok()?), target_id))
        })
        .collect::<Vec<_>>();
    let edge_ids = identities
        .iter()
        .map(|(edge_db_id, _)| *edge_db_id)
        .collect::<Vec<_>>();
    let mut targets = db::tags::get_targets_by_edge_ids(db, tag_db_id, &edge_ids)
        .map_err(TagServiceError::Internal)?;
    let target_db_ids = targets
        .values()
        .map(|target| target.target_db_id)
        .collect::<Vec<_>>();
    let target_ids =
        db::lookup::find_ids_by_db_ids(db, &target_db_ids).map_err(TagServiceError::Internal)?;
    let visibility =
        TargetVisibility::for_user(db, owner_db_id).map_err(TagServiceError::Internal)?;

    let mut items = Vec::with_capacity(identities.len());
    for (edge_db_id, expected_target_id) in identities {
        let Some(target) = targets.remove(&edge_db_id) else {
            continue;
        };
        if resolve_targetable_by_db_id(db, &visibility, target.target_db_id)
            .map_err(TagServiceError::Internal)?
            .is_none()
        {
            continue;
        }
        let Some(target_id) = target_ids.get(&target.target_db_id) else {
            continue;
        };
        if target_id != expected_target_id {
            continue;
        }
        items.push(TargetListItem {
            edge_db_id,
            target_id: target_id.clone(),
        });
    }
    Ok(items)
}

fn resolve_targetable(
    db: &impl db::DbAccess,
    visibility: &TargetVisibility,
    public_target_id: &str,
) -> anyhow::Result<Option<(DbId, TargetKind)>> {
    let Some(target_db_id) = db::lookup::find_node_id_by_id(db, public_target_id)? else {
        return Ok(None);
    };
    resolve_targetable_by_db_id(db, visibility, target_db_id)
}

fn resolve_targetable_by_db_id(
    db: &impl db::DbAccess,
    visibility: &TargetVisibility,
    target_db_id: DbId,
) -> anyhow::Result<Option<(DbId, TargetKind)>> {
    let Some((target_db_id, kind)) = resolve_whitelisted_by_db_id(db, target_db_id)? else {
        return Ok(None);
    };
    if !visibility.can_access_target(db, target_db_id, kind)? {
        return Ok(None);
    }
    Ok(Some((target_db_id, kind)))
}

fn resolve_whitelisted(
    db: &impl db::DbAccess,
    public_target_id: &str,
) -> anyhow::Result<Option<(DbId, TargetKind)>> {
    let Some(target_db_id) = db::lookup::find_node_id_by_id(db, public_target_id)? else {
        return Ok(None);
    };
    resolve_whitelisted_by_db_id(db, target_db_id)
}

fn resolve_whitelisted_by_db_id(
    db: &impl db::DbAccess,
    target_db_id: DbId,
) -> anyhow::Result<Option<(DbId, TargetKind)>> {
    match db::favorites::target_kind(db, target_db_id)? {
        Some(db::favorites::FavoriteKind::Track) => Ok(Some((target_db_id, TargetKind::Track))),
        Some(db::favorites::FavoriteKind::Release) => Ok(Some((target_db_id, TargetKind::Release))),
        Some(db::favorites::FavoriteKind::Artist) => Ok(Some((target_db_id, TargetKind::Artist))),
        Some(db::favorites::FavoriteKind::Playlist) => {
            Ok(Some((target_db_id, TargetKind::Playlist)))
        }
        None => Ok(None),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TargetKind {
    Track,
    Release,
    Artist,
    Playlist,
}

struct TargetVisibility {
    user_db_id: DbId,
    can_access_all_libraries: bool,
    accessible_library_ids: HashSet<String>,
}

impl TargetVisibility {
    fn for_user(db: &impl db::DbAccess, user_db_id: DbId) -> anyhow::Result<Self> {
        let can_access_all_libraries = db::roles::get_role_for_user(db, user_db_id)?
            .is_some_and(|role| role.permissions.contains(&db::Permission::Admin));
        let accessible_library_ids = if can_access_all_libraries {
            HashSet::new()
        } else {
            db::libraries::accessible_library_ids(db, user_db_id)?
        };
        Ok(Self {
            user_db_id,
            can_access_all_libraries,
            accessible_library_ids,
        })
    }

    fn can_access_target(
        &self,
        db: &impl db::DbAccess,
        target_db_id: DbId,
        kind: TargetKind,
    ) -> anyhow::Result<bool> {
        match kind {
            TargetKind::Playlist => playlist_is_visible(db, self.user_db_id, target_db_id),
            TargetKind::Track | TargetKind::Release | TargetKind::Artist => {
                if self.can_access_all_libraries {
                    return Ok(true);
                }
                Ok(db::libraries::get_for_entity(db, target_db_id)?
                    .into_iter()
                    .any(|library| self.accessible_library_ids.contains(&library.id)))
            }
        }
    }
}

fn playlist_is_visible(
    db: &impl db::DbAccess,
    user_db_id: DbId,
    playlist_db_id: DbId,
) -> anyhow::Result<bool> {
    let Some(playlist) = db::playlists::get_by_id(db, playlist_db_id)? else {
        return Ok(false);
    };
    if playlist.is_public.unwrap_or(false) {
        return Ok(true);
    }
    let owner = db::playlists::get_owner(db, playlist_db_id)?;
    Ok(owner == Some(user_db_id))
}

fn now_ms() -> anyhow::Result<i64> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    Ok(nanos as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::{
        new_test_db,
        test_user,
    };
    use crate::db::{
        Playlist,
        users,
    };
    use agdb::{
        DbAny,
        QueryBuilder,
    };
    use nanoid::nanoid;

    fn setup_id_index(db: &mut DbAny) -> anyhow::Result<()> {
        db::indexes::ensure_index(db, "id")?;
        Ok(())
    }

    fn create_user(db: &mut DbAny, username: &str) -> anyhow::Result<DbId> {
        users::create(db, &test_user(username)?)
    }

    fn create_track(db: &mut DbAny) -> anyhow::Result<(DbId, String)> {
        let public_id = nanoid!();
        let track_db_id = db
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values([[
                        ("id", public_id.as_str()).into(),
                        ("track_title", "Track").into(),
                    ]])
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
        Ok((track_db_id, public_id))
    }

    fn library_insert(name: &str) -> db::libraries::LibraryInsert {
        let suffix = nanoid!();
        let path = std::path::PathBuf::from(format!("/tmp/lyra-tags-test-{suffix}"));
        let path_key = db::libraries::path_key_for(&path);
        db::libraries::LibraryInsert {
            id: nanoid!(),
            name: format!("{name}-{suffix}"),
            path,
            path_key,
            language: None,
            country: None,
        }
    }

    fn attach_entity_to_library(
        db: &mut DbAny,
        library_db_id: DbId,
        target_db_id: DbId,
    ) -> anyhow::Result<()> {
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(library_db_id)
                .to(target_db_id)
                .query(),
        )?;
        Ok(())
    }

    fn create_accessible_track(
        db: &mut DbAny,
        user_db_id: DbId,
    ) -> anyhow::Result<(DbId, String, DbId)> {
        let (track_db_id, public_id) = create_track(db)?;
        let library = db::libraries::create_with_creator(db, library_insert("Tags"), user_db_id)?;
        let library_db_id = library.db_id.expect("library db id");
        attach_entity_to_library(db, library_db_id, track_db_id)?;
        Ok((track_db_id, public_id, library_db_id))
    }

    fn create_inaccessible_track(db: &mut DbAny) -> anyhow::Result<(DbId, String)> {
        let (track_db_id, public_id) = create_track(db)?;
        let library = db::libraries::create_system(db, library_insert("HiddenTags"))?;
        let library_db_id = library.db_id.expect("library db id");
        attach_entity_to_library(db, library_db_id, track_db_id)?;
        Ok((track_db_id, public_id))
    }

    fn create_playlist(
        db: &mut DbAny,
        owner: DbId,
        is_public: bool,
    ) -> anyhow::Result<(DbId, String)> {
        let public_id = nanoid!();
        let playlist = Playlist {
            db_id: None,
            id: public_id.clone(),
            name: "p".to_string(),
            description: None,
            is_public: Some(is_public),
            created_at: None,
            updated_at: None,
        };
        let db_id = db::playlists::create(db, &playlist, owner)?;
        Ok((db_id, public_id))
    }

    #[test]
    fn create_and_reuse_via_service() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;
        let (_, track_a, _) = create_accessible_track(&mut db, user)?;
        let (_, track_b, _) = create_accessible_track(&mut db, user)?;

        assert_eq!(
            create(&mut db, user, &track_a, "Workout", "blue")?,
            CreateResult::Created
        );
        assert_eq!(
            create(&mut db, user, &track_b, "Workout", "red")?,
            CreateResult::Reused
        );
        Ok(())
    }

    #[test]
    fn detach_final_target_preserves_manageable_empty_tag() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;
        let (_, track_id, _) = create_accessible_track(&mut db, user)?;

        create(&mut db, user, &track_id, "Keep", "blue")?;
        let original = list_for_user(&db, user)?.pop().expect("tag present");
        let tag_db_id = original.db_id.clone().expect("tag db id").into();

        remove_target_by_tag_id(&mut db, user, tag_db_id, &track_id)?;
        remove_target_by_tag_id(&mut db, user, tag_db_id, &track_id)?;

        let detached = get_by_public_id(&db, user, &original.id)?;
        assert_eq!(detached.id, original.id);
        assert_eq!(detached.tag, original.tag);
        assert_eq!(detached.color, original.color);
        assert_eq!(detached.created_at_ms, original.created_at_ms);
        assert_eq!(list_for_user(&db, user)?.len(), 1);
        assert!(list_targets(&db, user, tag_db_id)?.is_empty());

        let updated = update(&mut db, user, tag_db_id, Some("Detached"), Some("red"))?;
        assert_eq!(updated.id, original.id);
        assert_eq!(updated.tag, "Detached");
        assert_eq!(updated.color, "red");
        assert!(list_targets(&db, user, tag_db_id)?.is_empty());

        delete(&mut db, user, tag_db_id)?;
        assert!(matches!(
            get_by_public_id(&db, user, &original.id),
            Err(TagServiceError::NotFound),
        ));
        Ok(())
    }

    #[test]
    fn create_rejects_bad_tag_name() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;
        let (_, track_id, _) = create_accessible_track(&mut db, user)?;

        let err = create(&mut db, user, &track_id, "bad\x00name", "blue").unwrap_err();
        assert!(matches!(err, TagServiceError::BadTagName(_)));
        Ok(())
    }

    #[test]
    fn create_rejects_track_without_library_access() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;
        let (_, track_id) = create_inaccessible_track(&mut db)?;

        let err = create(&mut db, user, &track_id, "Hidden", "blue").unwrap_err();
        assert!(matches!(err, TagServiceError::NotTargetable));
        Ok(())
    }

    #[test]
    fn create_on_private_playlist_as_non_owner_is_not_targetable() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let owner = create_user(&mut db, "alice")?;
        let intruder = create_user(&mut db, "bob")?;
        let (_, public_id) = create_playlist(&mut db, owner, false)?;

        let err = create(&mut db, intruder, &public_id, "Stalker", "red").unwrap_err();
        assert!(matches!(err, TagServiceError::NotTargetable));
        Ok(())
    }

    #[test]
    fn remove_target_bypasses_visibility() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let alice = create_user(&mut db, "alice")?;
        let bob = create_user(&mut db, "bob")?;
        let (playlist_db_id, public_id) = create_playlist(&mut db, alice, true)?;

        create(&mut db, bob, &public_id, "Ref", "blue")?;
        let tag_id = db::tags::list_for_user(&db, bob)?
            .first()
            .and_then(|tag| tag.db_id.clone())
            .expect("tag present")
            .into();
        assert!(has_target_by_tag_id(&db, bob, tag_id, &public_id)?);

        db::playlists::update(
            &mut db,
            &Playlist {
                db_id: Some(playlist_db_id.into()),
                id: public_id.clone(),
                name: "p".to_string(),
                description: None,
                is_public: Some(false),
                created_at: None,
                updated_at: None,
            },
        )?;

        assert!(
            !has_target_by_tag_id(&db, bob, tag_id, &public_id)?,
            "has is opaque-false after visibility loss",
        );
        remove_target_by_tag_id(&mut db, bob, tag_id, &public_id)?;

        Ok(())
    }

    #[test]
    fn foreign_tag_filtered_but_persists_across_visibility_flips() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let alice = create_user(&mut db, "alice")?;
        let bob = create_user(&mut db, "bob")?;
        let (playlist_db_id, public_id) = create_playlist(&mut db, alice, true)?;

        create(&mut db, bob, &public_id, "Ref", "blue")?;

        db::playlists::update(
            &mut db,
            &Playlist {
                db_id: Some(playlist_db_id.into()),
                id: public_id.clone(),
                name: "p".to_string(),
                description: None,
                is_public: Some(false),
                created_at: None,
                updated_at: None,
            },
        )?;

        let (targets, _) = get_tagged(&db, bob, "Ref")?;
        assert!(
            targets.is_empty(),
            "hydration filter drops non-visible targets",
        );

        db::playlists::update(
            &mut db,
            &Playlist {
                db_id: Some(playlist_db_id.into()),
                id: public_id.clone(),
                name: "p".to_string(),
                description: None,
                is_public: Some(true),
                created_at: None,
                updated_at: None,
            },
        )?;
        let (targets, _) = get_tagged(&db, bob, "Ref")?;
        assert_eq!(
            targets.len(),
            1,
            "tag edge re-surfaces when target becomes visible again — persistence confirmed",
        );

        Ok(())
    }

    #[test]
    fn get_for_targets_many_filters_non_visible_playlists() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let alice = create_user(&mut db, "alice")?;
        let bob = create_user(&mut db, "bob")?;
        let (playlist_db_id, public_id) = create_playlist(&mut db, alice, true)?;

        create(&mut db, bob, &public_id, "Visible", "blue")?;
        let visible = get_for_targets_many_by_db_id(&db, bob, &[playlist_db_id])?;
        assert_eq!(visible[&playlist_db_id].len(), 1);

        db::playlists::update(
            &mut db,
            &Playlist {
                db_id: Some(playlist_db_id.into()),
                id: public_id,
                name: "p".to_string(),
                description: None,
                is_public: Some(false),
                created_at: None,
                updated_at: None,
            },
        )?;

        let hidden = get_for_targets_many_by_db_id(&db, bob, &[playlist_db_id])?;
        assert!(
            hidden[&playlist_db_id].is_empty(),
            "non-visible playlist keeps an empty batch entry",
        );
        Ok(())
    }

    #[test]
    fn media_tags_are_filtered_after_library_access_is_revoked() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;
        let (track_db_id, track_id, library_db_id) = create_accessible_track(&mut db, user)?;

        create(&mut db, user, &track_id, "Visible", "blue")?;
        assert!(has_target_by_db_id(&db, user, track_db_id, "Visible")?);

        db::libraries::revoke_access(&mut db, user, library_db_id)?;

        assert!(!has_target_by_db_id(&db, user, track_db_id, "Visible")?);
        assert!(get_for_target_by_db_id(&db, user, track_db_id)?.is_empty());
        let (targets, _) = get_tagged(&db, user, "Visible")?;
        assert!(targets.is_empty());
        Ok(())
    }

    #[test]
    fn get_by_public_id_rejects_non_owner() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let alice = create_user(&mut db, "alice")?;
        let bob = create_user(&mut db, "bob")?;
        let (_, track_id, _) = create_accessible_track(&mut db, alice)?;
        create(&mut db, alice, &track_id, "Private", "blue")?;

        let tags = db::tags::list_for_user(&db, alice)?;
        let public_id = tags.first().map(|t| t.id.clone()).expect("alice has a tag");

        let err = get_by_public_id(&db, bob, &public_id).unwrap_err();
        assert!(matches!(err, TagServiceError::NotFound));
        Ok(())
    }

    #[test]
    fn update_rename_collision_returns_error() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;
        let (_, track_id, _) = create_accessible_track(&mut db, user)?;

        create(&mut db, user, &track_id, "Workout", "blue")?;
        create(&mut db, user, &track_id, "Mood", "red")?;

        let workout_id = db::tags::list_for_user(&db, user)?
            .into_iter()
            .find(|t| t.tag == "Workout")
            .and_then(|t| t.db_id.clone())
            .expect("Workout tag present")
            .into();

        let err = update(&mut db, user, workout_id, Some("Mood"), None).unwrap_err();
        assert!(matches!(err, TagServiceError::RenameConflict));
        Ok(())
    }

    #[test]
    fn update_empty_patch_rejected() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;
        let (_, track_id, _) = create_accessible_track(&mut db, user)?;
        create(&mut db, user, &track_id, "X", "blue")?;
        let tag_id = db::tags::list_for_user(&db, user)?
            .first()
            .and_then(|t| t.db_id.clone())
            .expect("tag present")
            .into();

        let err = update(&mut db, user, tag_id, None, None).unwrap_err();
        assert!(matches!(err, TagServiceError::EmptyPatch));
        Ok(())
    }
}
