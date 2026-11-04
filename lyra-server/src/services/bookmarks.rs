// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashMap,
    time::{
        SystemTime,
        UNIX_EPOCH,
    },
};

use agdb::{
    DbAny,
    DbId,
};

use crate::services::auth::Principal;
use lyra_db::{
    bookmarks::{
        Bookmark,
        MAX_COMMENT_CHARS,
    },
    entities::MetadataEntityType,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MutationOutcome {
    Applied,
    NotTargetable,
}

/// The largest position every client reads exactly, as a double holds integers.
const MAX_POSITION_MS: u64 = (1 << 53) - 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum InvalidBookmark {
    #[error("position_ms must be a non-negative integer within the exact numeric range")]
    InvalidPosition,
    #[error("comment exceeds {MAX_COMMENT_CHARS} characters")]
    CommentTooLong,
}

/// A bookmark as a caller sets it: a position not yet clamped to the track, and a comment
/// within [`MAX_COMMENT_CHARS`], empty for none.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BookmarkInput {
    position_ms: u64,
    comment: Option<String>,
}

impl BookmarkInput {
    pub(crate) fn new(position_ms: i64, comment: Option<String>) -> Result<Self, InvalidBookmark> {
        let position_ms = u64::try_from(position_ms)
            .ok()
            .filter(|&position_ms| position_ms <= MAX_POSITION_MS)
            .ok_or(InvalidBookmark::InvalidPosition)?;
        let comment = comment.filter(|comment| !comment.is_empty());
        if comment
            .as_ref()
            .is_some_and(|comment| comment.chars().count() > MAX_COMMENT_CHARS)
        {
            return Err(InvalidBookmark::CommentTooLong);
        }
        Ok(Self {
            position_ms,
            comment,
        })
    }
}

/// Sets the caller's bookmark of a track it can see, clamping the position to the track's
/// duration when that is known.
pub(crate) fn set_for_principal(
    db: &mut DbAny,
    principal: &Principal,
    track_db_id: DbId,
    input: &BookmarkInput,
) -> anyhow::Result<MutationOutcome> {
    let now_ms = now_ms()?;
    db.transaction_mut(|t| -> anyhow::Result<MutationOutcome> {
        if !targetable_by_principal(t, principal, track_db_id)? {
            return Ok(MutationOutcome::NotTargetable);
        }
        let duration_ms =
            lyra_db::tracks::get_by_id(t, track_db_id)?.and_then(|track| track.duration_ms);
        let position_ms = duration_ms.map_or(input.position_ms, |duration| {
            input.position_ms.min(duration)
        });
        lyra_db::bookmarks::upsert(
            t,
            principal.require(t)?,
            track_db_id,
            position_ms,
            input.comment.as_deref(),
            now_ms,
        )?;
        Ok(MutationOutcome::Applied)
    })
}

/// Clears the caller's bookmark even of a track it can no longer see, so stored state can be
/// removed after access changes. A hidden track still answers `NotTargetable`, so removal does
/// not reveal that it exists.
pub(crate) fn remove_for_principal(
    db: &mut DbAny,
    principal: &Principal,
    track_db_id: DbId,
) -> anyhow::Result<MutationOutcome> {
    db.transaction_mut(|t| -> anyhow::Result<MutationOutcome> {
        if !is_track(t, track_db_id)? {
            return Ok(MutationOutcome::NotTargetable);
        }
        lyra_db::bookmarks::remove(t, principal.require(t)?, track_db_id)?;
        Ok(if targetable_by_principal(t, principal, track_db_id)? {
            MutationOutcome::Applied
        } else {
            MutationOutcome::NotTargetable
        })
    })
}

pub(crate) fn get_for_principal(
    db: &DbAny,
    principal: &Principal,
    track_db_id: DbId,
) -> anyhow::Result<Option<Bookmark>> {
    if !targetable_by_principal(db, principal, track_db_id)? {
        return Ok(None);
    }
    lyra_db::bookmarks::get(db, principal.require(db)?, track_db_id)
}

/// The caller's bookmarks of the tracks it can see; tracks without one are absent.
pub(crate) fn for_principal(
    db: &DbAny,
    principal: &Principal,
    track_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Bookmark>> {
    let mut bookmarks = HashMap::new();
    for (track_db_id, bookmark) in
        lyra_db::bookmarks::for_targets(db, principal.require(db)?, track_db_ids)?
    {
        if targetable_by_principal(db, principal, track_db_id)? {
            bookmarks.insert(track_db_id, bookmark);
        }
    }
    Ok(bookmarks)
}

fn targetable_by_principal(
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
    track_db_id: DbId,
) -> anyhow::Result<bool> {
    Ok(is_track(db, track_db_id)?
        && crate::services::auth::access::entity_accessible(db, principal, track_db_id)?)
}

/// Plugins pass raw ids, so an id naming no node is untargetable rather than an error.
fn is_track(db: &impl lyra_db::DbAccess, db_id: DbId) -> anyhow::Result<bool> {
    if lyra_db::lookup::find_id_by_db_id(db, db_id)?.is_none() {
        return Ok(false);
    }
    Ok(lyra_db::entities::metadata_entity_type(db, db_id)? == Some(MetadataEntityType::Track))
}

fn now_ms() -> anyhow::Result<i64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    use lyra_db::{
        fixtures::{
            connect,
            insert_artist,
            insert_release,
            insert_track,
            test_user,
        },
        test_db::{
            connect_artist,
            insert_library,
            new_test_db,
        },
        users,
    };

    struct Setup {
        db: DbAny,
        user: DbId,
        library: DbId,
        principal: Principal,
    }

    fn setup(name: &str) -> anyhow::Result<Setup> {
        let mut db = new_test_db()?;
        let user = users::create(&mut db, &test_user("alice")?)?;
        let library = insert_library(&mut db, "Music", &format!("/tmp/lyra-bookmarks-{name}"))?;
        let library_id = lyra_db::lookup::find_id_by_db_id(&db, library)?.unwrap();
        let principal = Principal::for_user(&db, user, Vec::new(), HashSet::from([library_id]));
        Ok(Setup {
            db,
            user,
            library,
            principal,
        })
    }

    fn track(setup: &mut Setup, duration_ms: Option<u64>) -> anyhow::Result<DbId> {
        let track = insert_track(&mut setup.db, "Track")?;
        connect(&mut setup.db, setup.library, track)?;
        if let Some(duration_ms) = duration_ms {
            let mut stored = lyra_db::tracks::get_by_id(&setup.db, track)?.unwrap();
            stored.set_duration_ms(duration_ms);
            lyra_db::tracks::update(&mut setup.db, &stored)?;
        }
        Ok(track)
    }

    fn input(position_ms: i64, comment: Option<&str>) -> BookmarkInput {
        BookmarkInput::new(position_ms, comment.map(str::to_string)).unwrap()
    }

    #[test]
    fn input_rejects_negative_positions_and_long_comments() {
        assert_eq!(
            BookmarkInput::new(-1, None),
            Err(InvalidBookmark::InvalidPosition)
        );
        assert_eq!(
            BookmarkInput::new(MAX_POSITION_MS as i64 + 1, None),
            Err(InvalidBookmark::InvalidPosition)
        );
        assert_eq!(
            BookmarkInput::new(MAX_POSITION_MS as i64, None),
            Ok(input(MAX_POSITION_MS as i64, None))
        );
        assert_eq!(
            BookmarkInput::new(0, Some("a".repeat(MAX_COMMENT_CHARS + 1))),
            Err(InvalidBookmark::CommentTooLong),
        );
        assert_eq!(
            BookmarkInput::new(0, Some(String::new())),
            Ok(input(0, None))
        );
    }

    #[test]
    fn position_is_clamped_to_a_known_duration() -> anyhow::Result<()> {
        let mut setup = setup("clamp")?;
        let timed = track(&mut setup, Some(180_000))?;
        let untimed = track(&mut setup, None)?;
        let Setup { db, principal, .. } = &mut setup;

        for target in [timed, untimed] {
            assert_eq!(
                set_for_principal(db, principal, target, &input(500_000, None))?,
                MutationOutcome::Applied,
            );
        }
        set_for_principal(db, principal, timed, &input(500_000, None))?;
        assert_eq!(
            get_for_principal(db, principal, timed)?
                .unwrap()
                .position_ms,
            180_000
        );
        assert_eq!(
            get_for_principal(db, principal, untimed)?
                .unwrap()
                .position_ms,
            500_000
        );
        Ok(())
    }

    #[test]
    fn update_keeps_created_at_and_replaces_the_rest() -> anyhow::Result<()> {
        let mut setup = setup("update")?;
        let target = track(&mut setup, None)?;
        let Setup {
            db,
            user,
            principal,
            ..
        } = &mut setup;
        lyra_db::bookmarks::upsert(db, *user, target, 1_000, Some("first"), 100)?;

        set_for_principal(db, principal, target, &input(2_000, None))?;

        let bookmark = get_for_principal(db, principal, target)?.unwrap();
        assert_eq!(bookmark.position_ms, 2_000);
        assert_eq!(bookmark.comment, None);
        assert_eq!(bookmark.created_at_ms, 100);
        assert!(bookmark.updated_at_ms > 100);
        Ok(())
    }

    #[test]
    fn only_visible_tracks_take_bookmarks() -> anyhow::Result<()> {
        let mut setup = setup("targets")?;
        let visible = track(&mut setup, None)?;
        let Setup {
            db,
            user,
            library,
            principal,
        } = &mut setup;
        let release = insert_release(db, "Release")?;
        let artist = insert_artist(db, "Artist")?;
        connect(db, *library, release)?;
        connect_artist(db, release, artist)?;
        let hidden = insert_track(db, "Hidden")?;
        let other_user = users::create(db, &test_user("bob")?)?;

        for target in [release, artist, hidden, other_user, DbId(999_999_999)] {
            assert_eq!(
                set_for_principal(db, principal, target, &input(0, None))?,
                MutationOutcome::NotTargetable,
            );
        }
        for target in [release, artist, other_user, DbId(999_999_999)] {
            assert_eq!(
                remove_for_principal(db, principal, target)?,
                MutationOutcome::NotTargetable
            );
        }
        lyra_db::bookmarks::upsert(db, *user, hidden, 5, None, 100)?;
        set_for_principal(db, principal, visible, &input(10, Some("note")))?;

        let found = for_principal(db, principal, &[visible, hidden, release])?;
        assert_eq!(found.keys().copied().collect::<Vec<_>>(), vec![visible]);
        assert_eq!(found[&visible].comment.as_deref(), Some("note"));
        assert!(get_for_principal(db, principal, hidden)?.is_none());
        assert_eq!(
            remove_for_principal(db, principal, hidden)?,
            MutationOutcome::NotTargetable
        );
        assert!(lyra_db::bookmarks::get(db, *user, hidden)?.is_none());
        Ok(())
    }

    #[test]
    fn deleting_the_user_or_the_track_leaves_no_bookmark() -> anyhow::Result<()> {
        let mut setup = setup("cleanup")?;
        let kept = track(&mut setup, None)?;
        let removed = track(&mut setup, None)?;
        let Setup { db, user, .. } = &mut setup;
        let bob = users::create(db, &test_user("bob")?)?;
        lyra_db::bookmarks::upsert(db, *user, kept, 1, None, 100)?;
        lyra_db::bookmarks::upsert(db, *user, removed, 1, None, 100)?;
        lyra_db::bookmarks::upsert(db, bob, kept, 2, None, 100)?;

        lyra_db::metadata::cascade_remove_entities_in_txn(db, &[removed])?;
        assert_eq!(
            lyra_db::bookmarks::target_ids(db, *user)?,
            HashSet::from([kept])
        );

        users::delete_user(db, *user)?;
        assert_eq!(
            lyra_db::bookmarks::for_targets(db, bob, &[kept])?
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![kept],
        );
        Ok(())
    }
}
