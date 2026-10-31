// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;
use std::time::{
    SystemTime,
    UNIX_EPOCH,
};

use agdb::{
    DbAny,
    DbId,
};

use crate::services::auth::{
    Principal,
    access,
};
use lyra_db::{
    Tag,
    tags::{
        CreateOutcome,
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

/// A tag name and color validated for [`create`].
#[derive(Debug)]
pub(crate) struct NewTag {
    name: String,
    color: String,
}

impl NewTag {
    pub(crate) fn parse(raw_name: &str, color: &str) -> Result<Self, TagServiceError> {
        let name =
            lyra_db::tags::normalize_tag_name(raw_name).map_err(TagServiceError::BadTagName)?;
        let color = color.trim();
        if color.is_empty() {
            return Err(TagServiceError::EmptyColor);
        }
        Ok(Self {
            name,
            color: color.to_string(),
        })
    }

    /// The canonical tag name.
    pub(crate) fn into_name(self) -> String {
        self.name
    }
}

/// Create or reuse a tag, then attach the target. Whitelists + writes atomically. Color is
/// ignored on reuse.
pub(crate) fn create(
    db: &mut DbAny,
    owner_db_id: DbId,
    target_db_id: DbId,
    tag: &NewTag,
) -> Result<CreateResult, TagServiceError> {
    let now_ms = now_ms().map_err(TagServiceError::Internal)?;

    db.transaction_mut(
        |t| -> anyhow::Result<Result<CreateResult, TagServiceError>> {
            let visibility = TargetVisibility::for_user(t, owner_db_id)?;
            if !targetable(t, &visibility, target_db_id)? {
                return Ok(Err(TagServiceError::NotTargetable));
            }
            let (_, outcome) =
                lyra_db::tags::create(t, owner_db_id, target_db_id, &tag.name, &tag.color, now_ms)?;
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
pub(crate) fn remove_target(
    db: &mut DbAny,
    owner_db_id: DbId,
    target_db_id: DbId,
    raw_tag_name: &str,
) -> Result<(), TagServiceError> {
    let normalized =
        lyra_db::tags::normalize_tag_name(raw_tag_name).map_err(TagServiceError::BadTagName)?;

    db.transaction_mut(|t| -> anyhow::Result<Result<(), TagServiceError>> {
        if target_kind(t, target_db_id)?.is_none() {
            return Ok(Err(TagServiceError::NotTargetable));
        }
        lyra_db::tags::remove_target(t, owner_db_id, target_db_id, &normalized)?;
        Ok(Ok(()))
    })
    .map_err(TagServiceError::Internal)?
}

pub(crate) fn has_target(
    db: &DbAny,
    owner_db_id: DbId,
    target_db_id: DbId,
    raw_tag_name: &str,
) -> Result<bool, TagServiceError> {
    let normalized =
        lyra_db::tags::normalize_tag_name(raw_tag_name).map_err(TagServiceError::BadTagName)?;
    let visibility =
        TargetVisibility::for_user(db, owner_db_id).map_err(TagServiceError::Internal)?;
    if !targetable(db, &visibility, target_db_id).map_err(TagServiceError::Internal)? {
        return Ok(false);
    }
    lyra_db::tags::has_target(db, owner_db_id, target_db_id, &normalized)
        .map_err(TagServiceError::Internal)
}

/// Batch check. Non-whitelisted or non-visible targets map to `false`.
pub(crate) fn has_targets(
    db: &DbAny,
    owner_db_id: DbId,
    target_db_ids: &[DbId],
    raw_tag_name: &str,
) -> Result<HashMap<DbId, bool>, TagServiceError> {
    let normalized =
        lyra_db::tags::normalize_tag_name(raw_tag_name).map_err(TagServiceError::BadTagName)?;
    let visibility =
        TargetVisibility::for_user(db, owner_db_id).map_err(TagServiceError::Internal)?;
    let visible = visible_targets(db, &visibility, target_db_ids)?;
    let raw = lyra_db::tags::has_targets(db, owner_db_id, &visible, &normalized)
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

pub(crate) fn get_for_target(
    db: &DbAny,
    owner_db_id: DbId,
    target_db_id: DbId,
) -> Result<Vec<Tag>, TagServiceError> {
    let visibility =
        TargetVisibility::for_user(db, owner_db_id).map_err(TagServiceError::Internal)?;
    if !targetable(db, &visibility, target_db_id).map_err(TagServiceError::Internal)? {
        return Ok(Vec::new());
    }
    lyra_db::tags::get_for_target(db, owner_db_id, target_db_id).map_err(TagServiceError::Internal)
}

pub(crate) fn get_for_targets_many(
    db: &DbAny,
    owner_db_id: DbId,
    target_db_ids: &[DbId],
) -> Result<HashMap<DbId, Vec<Tag>>, TagServiceError> {
    let visibility =
        TargetVisibility::for_user(db, owner_db_id).map_err(TagServiceError::Internal)?;
    let mut out: HashMap<DbId, Vec<Tag>> = target_db_ids
        .iter()
        .copied()
        .map(|id| (id, Vec::new()))
        .collect();
    let visible = visible_targets(db, &visibility, target_db_ids)?;
    if visible.is_empty() {
        return Ok(out);
    }

    let raw = lyra_db::tags::get_for_targets_many(db, owner_db_id, &visible)
        .map_err(TagServiceError::Internal)?;
    for (id, tags) in raw {
        out.insert(id, tags);
    }
    Ok(out)
}

/// Targets tagged `raw_tag_name` by `owner`. Non-visible playlists are filtered out; underlying
/// edges persist in the graph.
pub(crate) fn get_tagged(
    db: &DbAny,
    owner_db_id: DbId,
    raw_tag_name: &str,
) -> Result<(Vec<DbId>, String), TagServiceError> {
    let normalized =
        lyra_db::tags::normalize_tag_name(raw_tag_name).map_err(TagServiceError::BadTagName)?;
    let visibility =
        TargetVisibility::for_user(db, owner_db_id).map_err(TagServiceError::Internal)?;
    let ids = lyra_db::tags::get_targets_by_tag(db, owner_db_id, &normalized)
        .map_err(TagServiceError::Internal)?;
    let visible = visible_targets(db, &visibility, &ids)?;
    Ok((visible, normalized))
}

/// Fetch a tag by public nanoid. Non-owner returns `NotFound` (opaque).
pub(crate) fn get_by_public_id(
    db: &DbAny,
    owner_db_id: DbId,
    public_tag_id: &str,
) -> Result<Tag, TagServiceError> {
    let Some(tag_db_id) = lyra_db::lookup::find_node_id_by_id(db, public_tag_id)
        .map_err(TagServiceError::Internal)?
    else {
        return Err(TagServiceError::NotFound);
    };
    let tag = lyra_db::tags::get_by_id(db, tag_db_id)
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
    let Some(tag_db_id) = lyra_db::lookup::find_node_id_by_id(db, public_tag_id)
        .map_err(TagServiceError::Internal)?
    else {
        return Err(TagServiceError::NotFound);
    };
    if lyra_db::tags::get_by_id(db, tag_db_id)
        .map_err(TagServiceError::Internal)?
        .is_none()
    {
        return Err(TagServiceError::NotFound);
    }
    ensure_owner(db, tag_db_id, owner_db_id)?;
    Ok(tag_db_id)
}

fn ensure_owner(
    db: &impl lyra_db::DbAccess,
    tag_db_id: DbId,
    owner_db_id: DbId,
) -> Result<(), TagServiceError> {
    let owner = lyra_db::tags::get_owner(db, tag_db_id).map_err(TagServiceError::Internal)?;
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
        .map(lyra_db::tags::normalize_tag_name)
        .transpose()
        .map_err(TagServiceError::BadTagName)?;
    let color_trimmed = new_color
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string);

    let result = db
        .transaction_mut(|t| -> anyhow::Result<Result<Tag, RenameConflict>> {
            lyra_db::tags::update(
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
        lyra_db::tags::delete(t, tag_db_id)?;
        Ok(())
    })
    .map_err(TagServiceError::Internal)
}

pub(crate) fn list_for_user(db: &DbAny, owner_db_id: DbId) -> Result<Vec<Tag>, TagServiceError> {
    lyra_db::tags::list_for_user(db, owner_db_id).map_err(TagServiceError::Internal)
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
    let targets = lyra_db::tags::list_targets(db, tag_db_id).map_err(TagServiceError::Internal)?;

    let visibility =
        TargetVisibility::for_user(db, owner_db_id).map_err(TagServiceError::Internal)?;
    let target_db_ids = targets
        .iter()
        .map(|target| target.target_db_id)
        .collect::<Vec<_>>();
    let target_ids = lyra_db::lookup::find_ids_by_db_ids(db, &target_db_ids)
        .map_err(TagServiceError::Internal)?;
    let mut filtered = Vec::with_capacity(targets.len());
    for target in targets {
        if targetable(db, &visibility, target.target_db_id).map_err(TagServiceError::Internal)?
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
    let mut targets = lyra_db::tags::get_targets_by_edge_ids(db, tag_db_id, &edge_ids)
        .map_err(TagServiceError::Internal)?;
    let target_db_ids = targets
        .values()
        .map(|target| target.target_db_id)
        .collect::<Vec<_>>();
    let target_ids = lyra_db::lookup::find_ids_by_db_ids(db, &target_db_ids)
        .map_err(TagServiceError::Internal)?;
    let visibility =
        TargetVisibility::for_user(db, owner_db_id).map_err(TagServiceError::Internal)?;

    let mut items = Vec::with_capacity(identities.len());
    for (edge_db_id, expected_target_id) in identities {
        let Some(target) = targets.remove(&edge_db_id) else {
            continue;
        };
        if !targetable(db, &visibility, target.target_db_id).map_err(TagServiceError::Internal)? {
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

fn visible_targets(
    db: &DbAny,
    visibility: &TargetVisibility,
    target_db_ids: &[DbId],
) -> Result<Vec<DbId>, TagServiceError> {
    let mut visible = Vec::with_capacity(target_db_ids.len());
    for &id in target_db_ids {
        if targetable(db, visibility, id).map_err(TagServiceError::Internal)? {
            visible.push(id);
        }
    }
    Ok(visible)
}

fn targetable(
    db: &impl lyra_db::DbAccess,
    visibility: &TargetVisibility,
    target_db_id: DbId,
) -> anyhow::Result<bool> {
    match target_kind(db, target_db_id)? {
        Some(kind) => visibility.can_access_target(db, target_db_id, kind),
        None => Ok(false),
    }
}

fn target_kind(
    db: &impl lyra_db::DbAccess,
    target_db_id: DbId,
) -> anyhow::Result<Option<TargetKind>> {
    Ok(
        lyra_db::favorites::target_kind(db, target_db_id)?.map(|kind| match kind {
            lyra_db::favorites::FavoriteKind::Track => TargetKind::Track,
            lyra_db::favorites::FavoriteKind::Release => TargetKind::Release,
            lyra_db::favorites::FavoriteKind::Artist => TargetKind::Artist,
            lyra_db::favorites::FavoriteKind::Playlist => TargetKind::Playlist,
        }),
    )
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
    principal: Principal,
}

impl TargetVisibility {
    fn for_user(db: &impl lyra_db::DbAccess, user_db_id: DbId) -> anyhow::Result<Self> {
        let principal = Principal::load(db, user_db_id)?
            .ok_or_else(|| anyhow::anyhow!("tag owner {user_db_id:?} does not exist"))?;
        Ok(Self {
            user_db_id,
            principal,
        })
    }

    fn can_access_target(
        &self,
        db: &impl lyra_db::DbAccess,
        target_db_id: DbId,
        kind: TargetKind,
    ) -> anyhow::Result<bool> {
        let principal = &self.principal;
        match kind {
            TargetKind::Playlist => {
                access::playlist_visible_to_user(db, self.user_db_id, target_db_id)
            }
            TargetKind::Artist => access::artist_accessible(db, principal, target_db_id),
            TargetKind::Track | TargetKind::Release => {
                access::entity_accessible(db, principal, target_db_id)
            }
        }
    }
}

fn now_ms() -> anyhow::Result<i64> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    Ok(nanos as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agdb::{
        DbAny,
        QueryBuilder,
    };
    use lyra_db::fixtures::test_user;
    use lyra_db::test_db::new_test_db;
    use lyra_db::{
        Playlist,
        users,
    };
    use nanoid::nanoid;

    fn create_tag(
        db: &mut DbAny,
        owner_db_id: DbId,
        target_db_id: DbId,
        name: &str,
        color: &str,
    ) -> Result<CreateResult, TagServiceError> {
        create(db, owner_db_id, target_db_id, &NewTag::parse(name, color)?)
    }

    fn setup_id_index(db: &mut DbAny) -> anyhow::Result<()> {
        lyra_db::indexes::ensure_index(db, "id")?;
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

    fn library_insert(name: &str) -> lyra_db::libraries::LibraryInsert {
        let suffix = nanoid!();
        let path = std::path::PathBuf::from(format!("/tmp/lyra-tags-test-{suffix}"));
        let path_key = lyra_db::libraries::path_key_for(&path);
        lyra_db::libraries::LibraryInsert {
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
        let library =
            lyra_db::libraries::create_with_creator(db, library_insert("Tags"), user_db_id)?;
        let library_db_id = library.db_id.expect("library db id");
        attach_entity_to_library(db, library_db_id, track_db_id)?;
        Ok((track_db_id, public_id, library_db_id))
    }

    fn create_inaccessible_track(db: &mut DbAny) -> anyhow::Result<(DbId, String)> {
        let (track_db_id, public_id) = create_track(db)?;
        let library = lyra_db::libraries::create_system(db, library_insert("HiddenTags"))?;
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
            created_at_ms: None,
            updated_at_ms: None,
        };
        let db_id = lyra_db::playlists::create(db, &playlist, owner)?;
        Ok((db_id, public_id))
    }

    #[test]
    fn create_and_reuse_via_service() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;
        let (track_a, _, _) = create_accessible_track(&mut db, user)?;
        let (track_b, _, _) = create_accessible_track(&mut db, user)?;

        assert_eq!(NewTag::parse(" Workout", "blue")?.into_name(), "Workout");
        assert_eq!(
            create_tag(&mut db, user, track_a, " Workout", "blue")?,
            CreateResult::Created
        );
        assert_eq!(
            create_tag(&mut db, user, track_b, "Workout", "red")?,
            CreateResult::Reused
        );
        Ok(())
    }

    #[test]
    fn detach_final_target_preserves_manageable_empty_tag() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;
        let (track, _, _) = create_accessible_track(&mut db, user)?;

        create_tag(&mut db, user, track, "Keep", "blue")?;
        let original = list_for_user(&db, user)?.pop().expect("tag present");
        let tag_db_id = original.db_id.clone().expect("tag db id").into();

        remove_target(&mut db, user, track, &original.tag)?;
        remove_target(&mut db, user, track, &original.tag)?;

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
        let (track, _, _) = create_accessible_track(&mut db, user)?;

        let err = create_tag(&mut db, user, track, "bad\x00name", "blue").unwrap_err();
        assert!(matches!(err, TagServiceError::BadTagName(_)));
        assert!(matches!(
            NewTag::parse("Name", " "),
            Err(TagServiceError::EmptyColor)
        ));
        Ok(())
    }

    #[test]
    fn create_rejects_track_without_library_access() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;
        let (track, _) = create_inaccessible_track(&mut db)?;

        let err = create_tag(&mut db, user, track, "Hidden", "blue").unwrap_err();
        assert!(matches!(err, TagServiceError::NotTargetable));
        Ok(())
    }

    #[test]
    fn create_on_private_playlist_as_non_owner_is_not_targetable() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let owner = create_user(&mut db, "alice")?;
        let intruder = create_user(&mut db, "bob")?;
        let (playlist, _) = create_playlist(&mut db, owner, false)?;

        let err = create_tag(&mut db, intruder, playlist, "Stalker", "red").unwrap_err();
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

        create_tag(&mut db, bob, playlist_db_id, "Ref", "blue")?;
        assert!(has_target(&db, bob, playlist_db_id, "Ref")?);

        lyra_db::playlists::update(
            &mut db,
            &Playlist {
                db_id: Some(playlist_db_id.into()),
                id: public_id.clone(),
                name: "p".to_string(),
                description: None,
                is_public: Some(false),
                created_at_ms: None,
                updated_at_ms: None,
            },
        )?;

        assert!(
            !has_target(&db, bob, playlist_db_id, "Ref")?,
            "has is opaque-false after visibility loss",
        );
        remove_target(&mut db, bob, playlist_db_id, "Ref")?;
        assert!(lyra_db::tags::get_for_target(&db, bob, playlist_db_id)?.is_empty());

        Ok(())
    }

    #[test]
    fn foreign_tag_filtered_but_persists_across_visibility_flips() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let alice = create_user(&mut db, "alice")?;
        let bob = create_user(&mut db, "bob")?;
        let (playlist_db_id, public_id) = create_playlist(&mut db, alice, true)?;

        create_tag(&mut db, bob, playlist_db_id, "Ref", "blue")?;

        lyra_db::playlists::update(
            &mut db,
            &Playlist {
                db_id: Some(playlist_db_id.into()),
                id: public_id.clone(),
                name: "p".to_string(),
                description: None,
                is_public: Some(false),
                created_at_ms: None,
                updated_at_ms: None,
            },
        )?;

        let (targets, _) = get_tagged(&db, bob, "Ref")?;
        assert!(
            targets.is_empty(),
            "hydration filter drops non-visible targets",
        );

        lyra_db::playlists::update(
            &mut db,
            &Playlist {
                db_id: Some(playlist_db_id.into()),
                id: public_id.clone(),
                name: "p".to_string(),
                description: None,
                is_public: Some(true),
                created_at_ms: None,
                updated_at_ms: None,
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

        create_tag(&mut db, bob, playlist_db_id, "Visible", "blue")?;
        let visible = get_for_targets_many(&db, bob, &[playlist_db_id])?;
        assert_eq!(visible[&playlist_db_id].len(), 1);

        lyra_db::playlists::update(
            &mut db,
            &Playlist {
                db_id: Some(playlist_db_id.into()),
                id: public_id,
                name: "p".to_string(),
                description: None,
                is_public: Some(false),
                created_at_ms: None,
                updated_at_ms: None,
            },
        )?;

        let hidden = get_for_targets_many(&db, bob, &[playlist_db_id])?;
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
        let (track_db_id, _, library_db_id) = create_accessible_track(&mut db, user)?;

        create_tag(&mut db, user, track_db_id, "Visible", "blue")?;
        assert!(has_target(&db, user, track_db_id, "Visible")?);

        lyra_db::libraries::revoke_access(&mut db, user, library_db_id)?;

        assert!(!has_target(&db, user, track_db_id, "Visible")?);
        assert!(get_for_target(&db, user, track_db_id)?.is_empty());
        let (targets, _) = get_tagged(&db, user, "Visible")?;
        assert!(targets.is_empty());
        Ok(())
    }

    #[test]
    fn missing_and_unsupported_targets_are_not_targetable() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;
        let other_user = create_user(&mut db, "bob")?;

        for target in [DbId(999_999_999), DbId(0), other_user] {
            assert!(matches!(
                create_tag(&mut db, user, target, "X", "blue"),
                Err(TagServiceError::NotTargetable),
            ));
            assert!(matches!(
                remove_target(&mut db, user, target, "X"),
                Err(TagServiceError::NotTargetable),
            ));
            assert!(!has_target(&db, user, target, "X")?);
            assert!(get_for_target(&db, user, target)?.is_empty());
        }
        let many = has_targets(&db, user, &[DbId(999_999_999), other_user], "X")?;
        assert_eq!(
            many,
            HashMap::from([(DbId(999_999_999), false), (other_user, false)])
        );
        Ok(())
    }

    #[test]
    fn get_by_public_id_rejects_non_owner() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let alice = create_user(&mut db, "alice")?;
        let bob = create_user(&mut db, "bob")?;
        let (track, _, _) = create_accessible_track(&mut db, alice)?;
        create_tag(&mut db, alice, track, "Private", "blue")?;

        let tags = lyra_db::tags::list_for_user(&db, alice)?;
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
        let (track, _, _) = create_accessible_track(&mut db, user)?;

        create_tag(&mut db, user, track, "Workout", "blue")?;
        create_tag(&mut db, user, track, "Mood", "red")?;

        let workout_id = lyra_db::tags::list_for_user(&db, user)?
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
        let (track, _, _) = create_accessible_track(&mut db, user)?;
        create_tag(&mut db, user, track, "X", "blue")?;
        let tag_id = lyra_db::tags::list_for_user(&db, user)?
            .first()
            .and_then(|t| t.db_id.clone())
            .expect("tag present")
            .into();

        let err = update(&mut db, user, tag_id, None, None).unwrap_err();
        assert!(matches!(err, TagServiceError::EmptyPatch));
        Ok(())
    }
}
