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

use crate::db::{
    self,
    Tag,
    tags::{
        CreateOutcome,
        HAS_MANY_CAP,
        LIST_HARD_LIMIT,
        PagedTags,
        PagedTargets,
        RenameConflict,
        TagListCursor,
        TagNormalizeError,
        TargetListCursor,
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
            let Some((target_db_id, _)) = resolve_targetable(t, owner_db_id, public_target_id)?
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
    let Some((target_db_id, _)) =
        resolve_targetable(db, owner_db_id, public_target_id).map_err(TagServiceError::Internal)?
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
    let mut visible: Vec<DbId> = Vec::with_capacity(target_db_ids.len());
    for &id in target_db_ids {
        if resolve_targetable_by_db_id(db, owner_db_id, id)
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
    let Some((target_db_id, _)) = resolve_targetable_by_db_id(db, owner_db_id, target_db_id)
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

    let mut visible: Vec<DbId> = Vec::with_capacity(target_db_ids.len());
    let mut out: HashMap<DbId, Vec<Tag>> = target_db_ids
        .iter()
        .copied()
        .map(|id| (id, Vec::new()))
        .collect();
    for &id in target_db_ids {
        if resolve_targetable_by_db_id(db, owner_db_id, id)
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
            let Some((target_db_id, _)) =
                resolve_targetable_by_db_id(t, owner_db_id, target_db_id)?
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
    let Some((target_db_id, _)) = resolve_targetable_by_db_id(db, owner_db_id, target_db_id)
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
    let ids = db::tags::get_targets_by_tag(db, owner_db_id, &normalized)
        .map_err(TagServiceError::Internal)?;
    let mut visible = Vec::with_capacity(ids.len());
    for id in ids {
        if resolve_targetable_by_db_id(db, owner_db_id, id)
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

pub(crate) fn list_for_user(
    db: &DbAny,
    owner_db_id: DbId,
    limit: u64,
    cursor: Option<TagListCursor>,
) -> Result<PagedTags, TagServiceError> {
    let clamped = limit.min(LIST_HARD_LIMIT).max(1);
    db::tags::list_for_user(db, owner_db_id, clamped, cursor).map_err(TagServiceError::Internal)
}

/// Paginated target list for a tag, filtered by caller-side playlist visibility.
pub(crate) fn list_targets(
    db: &DbAny,
    owner_db_id: DbId,
    tag_db_id: DbId,
    limit: u64,
    cursor: Option<TargetListCursor>,
) -> Result<PagedTargets, TagServiceError> {
    ensure_owner(db, tag_db_id, owner_db_id)?;
    let clamped = limit.min(LIST_HARD_LIMIT).max(1);
    let page = db::tags::list_targets(db, tag_db_id, clamped, cursor)
        .map_err(TagServiceError::Internal)?;

    let mut filtered = Vec::with_capacity(page.target_db_ids.len());
    for id in page.target_db_ids {
        if resolve_targetable_by_db_id(db, owner_db_id, id)
            .map_err(TagServiceError::Internal)?
            .is_some()
        {
            filtered.push(id);
        }
    }

    Ok(PagedTargets {
        target_db_ids: filtered,
        next_cursor: page.next_cursor,
    })
}

fn resolve_targetable(
    db: &impl db::DbAccess,
    user_db_id: DbId,
    public_target_id: &str,
) -> anyhow::Result<Option<(DbId, TargetKind)>> {
    let Some(target_db_id) = db::lookup::find_node_id_by_id(db, public_target_id)? else {
        return Ok(None);
    };
    resolve_targetable_by_db_id(db, user_db_id, target_db_id)
}

fn resolve_targetable_by_db_id(
    db: &impl db::DbAccess,
    user_db_id: DbId,
    target_db_id: DbId,
) -> anyhow::Result<Option<(DbId, TargetKind)>> {
    let Some((target_db_id, kind)) = resolve_whitelisted_by_db_id(db, target_db_id)? else {
        return Ok(None);
    };
    if matches!(kind, TargetKind::Playlist) && !playlist_is_visible(db, user_db_id, target_db_id)? {
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
    use crate::db::test_db::new_test_db;
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
        users::create(db, &users::test_user(username)?)
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
        let (_, track_a) = create_track(&mut db)?;
        let (_, track_b) = create_track(&mut db)?;

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
    fn create_rejects_bad_tag_name() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;
        let (_, track_id) = create_track(&mut db)?;

        let err = create(&mut db, user, &track_id, "bad\x00name", "blue").unwrap_err();
        assert!(matches!(err, TagServiceError::BadTagName(_)));
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
        let tag_id = db::tags::list_for_user(&db, bob, 10, None)?
            .tags
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
    fn get_by_public_id_rejects_non_owner() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let alice = create_user(&mut db, "alice")?;
        let bob = create_user(&mut db, "bob")?;
        let (_, track_id) = create_track(&mut db)?;
        create(&mut db, alice, &track_id, "Private", "blue")?;

        let tags = db::tags::list_for_user(&db, alice, 10, None)?;
        let public_id = tags
            .tags
            .first()
            .map(|t| t.id.clone())
            .expect("alice has a tag");

        let err = get_by_public_id(&db, bob, &public_id).unwrap_err();
        assert!(matches!(err, TagServiceError::NotFound));
        Ok(())
    }

    #[test]
    fn update_rename_collision_returns_error() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        setup_id_index(&mut db)?;
        let user = create_user(&mut db, "alice")?;
        let (_, track_id) = create_track(&mut db)?;

        create(&mut db, user, &track_id, "Workout", "blue")?;
        create(&mut db, user, &track_id, "Mood", "red")?;

        let workout_id = db::tags::list_for_user(&db, user, 10, None)?
            .tags
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
        let (_, track_id) = create_track(&mut db)?;
        create(&mut db, user, &track_id, "X", "blue")?;
        let tag_id = db::tags::list_for_user(&db, user, 10, None)?
            .tags
            .first()
            .and_then(|t| t.db_id.clone())
            .expect("tag present")
            .into();

        let err = update(&mut db, user, tag_id, None, None).unwrap_err();
        assert!(matches!(err, TagServiceError::EmptyPatch));
        Ok(())
    }
}
