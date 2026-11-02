// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Cover records whose image file is gone. Listings never look at the files. Serving the bytes
//! and syncing do, and remove a record once its file is confirmed gone, so the owner shows its
//! next source on the next read.

use std::{
    collections::{
        HashMap,
        HashSet,
    },
    path::Path,
};

use agdb::{
    DbAny,
    DbId,
};

use super::ResolvedCover;
use lyra_db::{
    Cover,
    DbAccess,
    covers::CoverSource,
};

/// Whether the cover file at `path` is confirmed gone: its directory lists without error and the
/// file itself is not there. A directory that is missing or fails to list, as on a library or
/// covers root that is unavailable, is no evidence.
pub(crate) fn confirmed_gone(path: &Path) -> bool {
    let Some(dir) = path.parent() else {
        return false;
    };
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    if entries.into_iter().any(|entry| entry.is_err()) {
        return false;
    }
    matches!(
        std::fs::symlink_metadata(path),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound
    )
}

fn cover_gone(cover: &Cover) -> bool {
    confirmed_gone(Path::new(&cover.path))
}

/// An owner's cover from one source whose file was confirmed gone when read, identified so that
/// it can be acted on after the read's guard is released: database ids can name another node by
/// then, public ids can't.
#[derive(Clone, Debug)]
pub(crate) struct StaleCover {
    owner_db_id: DbId,
    owner_id: String,
    source: CoverSource,
    cover_id: String,
    /// The gone file. An upsert can keep a cover's public id while pointing it elsewhere.
    path: String,
}

impl StaleCover {
    /// Each of the owner's covers whose file is confirmed gone.
    pub(crate) fn read_all(db: &impl DbAccess, owner_db_id: DbId) -> anyhow::Result<Vec<Self>> {
        let mut stale = Vec::new();
        for sourced in lyra_db::covers::sources(db, owner_db_id)? {
            stale.extend(Self::of(db, owner_db_id, sourced.source, &sourced.cover)?);
        }
        Ok(stale)
    }

    /// `cover`, the owner's cover from `source`, when its file is confirmed gone.
    fn of(
        db: &impl DbAccess,
        owner_db_id: DbId,
        source: CoverSource,
        cover: &Cover,
    ) -> anyhow::Result<Option<Self>> {
        if !cover_gone(cover) {
            return Ok(None);
        }
        let Some(owner_id) = lyra_db::lookup::find_id_by_db_id(db, owner_db_id)? else {
            return Ok(None);
        };
        Ok(Some(Self {
            owner_db_id,
            owner_id,
            source,
            cover_id: cover.id.clone(),
            path: cover.path.clone(),
        }))
    }

    pub(crate) fn owner_db_id(&self) -> DbId {
        self.owner_db_id
    }

    pub(crate) fn source(&self) -> CoverSource {
        self.source
    }

    /// The stale cover, when the owner read is still there and still holds it, at the same file,
    /// from the source.
    pub(crate) fn current(&self, db: &impl DbAccess) -> anyhow::Result<Option<Cover>> {
        if lyra_db::lookup::find_id_by_db_id(db, self.owner_db_id)?.as_deref()
            != Some(&self.owner_id)
        {
            return Ok(None);
        }
        Ok(
            lyra_db::covers::get_source(db, self.owner_db_id, self.source)?
                .filter(|cover| cover.id == self.cover_id && cover.path == self.path),
        )
    }
}

/// Leaves out the covers whose file is not there, for callers about to serve the image, and
/// returns those whose file is confirmed gone, for [`remove_stale`] once the read is over.
pub(crate) fn keep_present<K>(
    db: &impl DbAccess,
    covers: &mut HashMap<K, ResolvedCover>,
) -> anyhow::Result<Vec<StaleCover>> {
    let mut missing = Vec::new();
    covers.retain(|_, resolved| {
        let present = std::fs::metadata(&resolved.cover.path).is_ok_and(|meta| meta.is_file());
        if !present {
            missing.push((resolved.owner_db_id, resolved.cover.clone()));
        }
        present
    });
    let mut stale = Vec::new();
    for (owner_db_id, cover) in missing {
        let source = lyra_db::covers::sources(db, owner_db_id)?
            .into_iter()
            .find(|sourced| sourced.cover.id == cover.id)
            .map(|sourced| sourced.source);
        if let Some(source) = source {
            stale.extend(StaleCover::of(db, owner_db_id, source, &cover)?);
        }
    }
    Ok(stale)
}

/// Removes the `stale` covers whose files are still confirmed gone, each in a transaction of its
/// own, and returns how many it removed. A cover whose owner was removed, or which was removed or
/// replaced, since it was read is passed over. A pin on a removed source goes with it.
pub(crate) fn remove_stale(db: &mut DbAny, stale: &[StaleCover]) -> anyhow::Result<usize> {
    let mut seen = HashSet::new();
    let mut removed = 0;
    for stale in stale {
        if !seen.insert((stale.owner_db_id, stale.source, &stale.cover_id)) {
            continue;
        }
        let removed_one = db.transaction_mut(|t| -> anyhow::Result<bool> {
            let Some(cover) = stale.current(t)? else {
                return Ok(false);
            };
            if !cover_gone(&cover) {
                return Ok(false);
            }
            Ok(lyra_db::covers::remove(t, stale.owner_db_id, stale.source)?.is_some())
        })?;
        removed += usize::from(removed_one);
    }
    Ok(removed)
}

/// Removes the cover with public id `cover_id` from every owner holding it when its file is
/// still confirmed gone. A cover removed since it was read is passed over.
pub(crate) fn remove_stale_cover(db: &mut DbAny, cover_id: &str) -> anyhow::Result<()> {
    db.transaction_mut(|t| -> anyhow::Result<()> {
        let Some(cover) = lyra_db::covers::get_by_public_id(t, cover_id)? else {
            return Ok(());
        };
        let Some(cover_db_id) = cover.db_id else {
            return Ok(());
        };
        if !cover_gone(&cover) {
            return Ok(());
        }
        for (owner_db_id, source) in lyra_db::covers::owners(t, cover_db_id)? {
            lyra_db::covers::remove(t, owner_db_id, source)?;
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use lyra_db::{
        fixtures::insert_release,
        test_db::new_test_db,
    };

    /// A cover stored for `owner` from `source` at `path`, whose file isn't written.
    fn stored_cover(
        db: &mut DbAny,
        owner: DbId,
        source: CoverSource,
        path: &Path,
    ) -> anyhow::Result<Cover> {
        Ok(db
            .transaction_mut(|t| {
                lyra_db::covers::upsert(
                    t,
                    owner,
                    source,
                    Cover {
                        db_id: None,
                        id: nanoid::nanoid!(),
                        path: path.to_string_lossy().into_owned(),
                        mime_type: "image/png".to_string(),
                        hash: source.as_str().to_string(),
                        blurhash: None,
                        provider_id: None,
                    },
                )
            })?
            .cover)
    }

    fn shown_source(db: &DbAny, owner: DbId) -> anyhow::Result<Option<CoverSource>> {
        let sources = lyra_db::covers::sources(db, owner)?;
        Ok(lyra_db::covers::shown(&sources).map(|sourced| sourced.source))
    }

    #[test]
    fn only_a_file_missing_from_a_listable_directory_is_gone() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let present = dir.path().join("present.png");
        std::fs::write(&present, b"cover")?;
        assert!(!confirmed_gone(&present));
        assert!(confirmed_gone(&dir.path().join("missing.png")));
        assert!(
            !confirmed_gone(&dir.path().join("unmounted/cover.png")),
            "a missing directory is no evidence"
        );
        Ok(())
    }

    #[test]
    fn an_unlistable_directory_is_no_evidence() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir()?;
        let locked = dir.path().join("locked");
        std::fs::create_dir(&locked)?;
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000))?;
        let listable = std::fs::read_dir(&locked).is_ok();
        let gone = confirmed_gone(&locked.join("cover.png"));
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755))?;
        if listable {
            // Running as root, every directory lists.
            return Ok(());
        }
        assert!(!gone);
        Ok(())
    }

    #[test]
    fn a_cover_in_an_unmounted_directory_is_kept() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let mut db = new_test_db()?;
        let release = insert_release(&mut db, "Album")?;
        let unmounted = dir.path().join("unmounted/cover.png");
        let cover = stored_cover(&mut db, release, CoverSource::Local, &unmounted)?;
        let mut found = HashMap::from([(
            release,
            ResolvedCover {
                owner_db_id: release,
                cover: cover.clone(),
            },
        )]);

        let stale = keep_present(&db, &mut found)?;
        assert!(found.is_empty(), "nothing is served without its file");
        assert!(stale.is_empty(), "but nothing is confirmed gone");
        remove_stale_cover(&mut db, &cover.id)?;
        assert!(lyra_db::covers::get(&db, release)?.is_some());
        Ok(())
    }

    #[test]
    fn a_gone_manual_cover_gives_way_to_the_provider_cover() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let mut db = new_test_db()?;
        let release = insert_release(&mut db, "Album")?;
        let provider_path = dir.path().join("provider.png");
        std::fs::write(&provider_path, b"cover")?;
        stored_cover(&mut db, release, CoverSource::Provider, &provider_path)?;
        let manual = stored_cover(
            &mut db,
            release,
            CoverSource::Manual,
            &dir.path().join("manual.png"),
        )?;
        let mut found = HashMap::from([(
            release,
            ResolvedCover {
                owner_db_id: release,
                cover: manual,
            },
        )]);

        let stale = keep_present(&db, &mut found)?;
        assert_eq!(remove_stale(&mut db, &stale)?, 1);
        assert_eq!(shown_source(&db, release)?, Some(CoverSource::Provider));
        Ok(())
    }

    #[test]
    fn a_gone_cover_leaves_every_owner_once() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let mut db = new_test_db()?;
        let first = insert_release(&mut db, "First")?;
        let second = insert_release(&mut db, "Second")?;
        let cover = stored_cover(
            &mut db,
            first,
            CoverSource::Provider,
            &dir.path().join("gone.png"),
        )?;
        lyra_db::covers::adopt_missing(&mut db, second, first)?;

        remove_stale_cover(&mut db, &cover.id)?;
        assert!(lyra_db::covers::get(&db, first)?.is_none());
        assert!(lyra_db::covers::get(&db, second)?.is_none());
        assert!(lyra_db::covers::get_by_public_id(&db, &cover.id)?.is_none());

        // A second request for the same gone cover finds nothing left to remove.
        remove_stale_cover(&mut db, &cover.id)?;
        Ok(())
    }

    #[test]
    fn owners_and_covers_changed_since_they_were_read_are_passed_over() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let gone = |name: &str| -> PathBuf { dir.path().join(name) };
        let mut db = new_test_db()?;
        let deleted = insert_release(&mut db, "Deleted")?;
        let replaced = insert_release(&mut db, "Replaced")?;
        let kept = insert_release(&mut db, "Kept")?;
        let mut stale = Vec::new();
        for owner in [deleted, replaced, kept] {
            stored_cover(&mut db, owner, CoverSource::Local, &gone("old.png"))?;
            stale.extend(StaleCover::read_all(&db, owner)?);
        }
        assert_eq!(stale.len(), 3);
        lyra_db::covers::remove_all(&mut db, deleted)?;
        db.exec_mut(agdb::QueryBuilder::remove().ids(deleted).query())?;
        // Updated in place, the cover keeps its public id but points at another file.
        let replacement = stored_cover(&mut db, replaced, CoverSource::Local, &gone("new.png"))?;

        stale.push(stale[2].clone());
        assert_eq!(remove_stale(&mut db, &stale)?, 1);
        assert_eq!(
            lyra_db::covers::get(&db, replaced)?.map(|cover| cover.path),
            Some(replacement.path),
            "a cover stored since the read stays"
        );
        assert!(lyra_db::covers::get(&db, kept)?.is_none());
        Ok(())
    }

    #[test]
    fn an_owner_id_reused_since_the_read_keeps_its_cover() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let mut db = new_test_db()?;
        let removed = insert_release(&mut db, "Removed")?;
        let cover = stored_cover(
            &mut db,
            removed,
            CoverSource::Local,
            &dir.path().join("gone.png"),
        )?;
        let stale = StaleCover::read_all(&db, removed)?;
        assert_eq!(stale.len(), 1);
        let other = insert_release(&mut db, "Other")?;
        // Another owner keeps the cover node alive while the first is deleted.
        lyra_db::covers::adopt_missing(&mut db, other, removed)?;
        db.exec_mut(agdb::QueryBuilder::remove().ids(removed).query())?;
        let reused = insert_release(&mut db, "Reused")?;
        assert_eq!(reused, removed, "agdb hands the freed id to the next node");
        // Even the very cover node the read saw, now held by the new node, stays.
        lyra_db::covers::adopt_missing(&mut db, reused, other)?;
        assert_eq!(
            lyra_db::covers::get(&db, reused)?.map(|found| found.id),
            Some(cover.id)
        );

        assert_eq!(remove_stale(&mut db, &stale)?, 0);
        assert!(lyra_db::covers::get(&db, reused)?.is_some());
        Ok(())
    }
}
