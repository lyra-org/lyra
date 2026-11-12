// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The files of removed covers, until the server has discarded them.
//!
//! The write that removes a cover record, or points it at another file, lists the file it let go
//! of in the same transaction, so a file stays listed however long the server takes to get to it,
//! and a write that rolls back lists nothing.

use agdb::{
    DbElement,
    DbId,
    QueryBuilder,
};

use super::Cover;
use crate::{
    DbAccess,
    graph,
    roots::{
        self,
        Root,
    },
};

/// A file no cover record holds anymore, listed until the server discards it.
#[derive(DbElement, Clone, Debug, PartialEq, Eq)]
pub struct DiscardedCover {
    pub db_id: Option<DbId>,
    pub path: String,
    /// The image's content hash, which its transformed variants are kept under.
    pub hash: String,
}

pub(super) fn enlist(db: &mut impl DbAccess, cover: &Cover) -> anyhow::Result<()> {
    let discarded = DiscardedCover {
        db_id: None,
        path: cover.path.clone(),
        hash: cover.hash.clone(),
    };
    let id = db
        .exec_mut(QueryBuilder::insert().element(&discarded).query())?
        .ids()
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("discarded cover insert returned no id"))?;
    roots::link(db, Root::CoverDiscards, id)
}

/// Every listed file, in no particular order.
pub fn listed(db: &impl DbAccess) -> anyhow::Result<Vec<DiscardedCover>> {
    roots::members(db, Root::CoverDiscards)
}

pub fn any(db: &impl DbAccess) -> anyhow::Result<bool> {
    roots::has_members(db, Root::CoverDiscards)
}

/// Takes the `handled` files off the list, as [`listed`] read them. An entry that went since,
/// whose id may name another one by now, is passed over.
pub fn forget(db: &mut impl DbAccess, handled: &[DiscardedCover]) -> anyhow::Result<()> {
    let mut ids = Vec::new();
    for handled in handled {
        let Some(id) = handled.db_id else {
            continue;
        };
        if graph::fetch_typed_by_id::<DiscardedCover>(db, id, "DiscardedCover")?
            .is_some_and(|stored| stored == *handled)
        {
            ids.push(id);
        }
    }
    if !ids.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(ids).query())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use agdb::DbAny;

    use super::*;
    use crate::{
        covers::{
            self,
            CoverSource,
        },
        fixtures::insert_release,
        test_db::new_test_db,
    };

    fn cover(id: &str, path: &str) -> Cover {
        Cover {
            db_id: None,
            id: id.to_string(),
            path: path.to_string(),
            mime_type: "image/jpeg".to_string(),
            hash: format!("{id}-hash"),
            blurhash: None,
            provider_id: None,
        }
    }

    fn release_with_cover(db: &mut DbAny, id: &str, path: &str) -> anyhow::Result<DbId> {
        let release = insert_release(db, id)?;
        db.transaction_mut(|t| covers::upsert(t, release, CoverSource::Provider, cover(id, path)))?;
        Ok(release)
    }

    #[test]
    fn a_removed_cover_is_listed_in_the_write_that_removes_it() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release = release_with_cover(&mut db, "gone", "/covers/1/provider.jpg")?;

        db.transaction_mut(|t| -> anyhow::Result<()> {
            covers::remove(t, release, CoverSource::Provider)?;
            assert_eq!(listed(t)?.len(), 1, "listed before the write is over");
            Ok(())
        })?;

        let listed = listed(&db)?;
        assert_eq!(
            listed
                .iter()
                .map(|listed| (listed.path.as_str(), listed.hash.as_str()))
                .collect::<Vec<_>>(),
            [("/covers/1/provider.jpg", "gone-hash")]
        );
        assert!(covers::get_by_public_id(&db, "gone")?.is_none());
        assert!(!covers::holds_file(&db, "/covers/1/provider.jpg")?);
        Ok(())
    }

    #[test]
    fn a_write_that_rolls_back_lists_nothing() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release = release_with_cover(&mut db, "kept", "/covers/1/provider.jpg")?;

        let failed = db.transaction_mut(|t| -> anyhow::Result<()> {
            crate::metadata::cascade_remove_entities_in_txn(t, &[release])?;
            anyhow::bail!("the write fails after removing the cover");
        });

        assert!(failed.is_err());
        assert!(!any(&db)?);
        assert!(covers::get_by_public_id(&db, "kept")?.is_some());
        assert!(covers::holds_file(&db, "/covers/1/provider.jpg")?);
        Ok(())
    }

    #[test]
    fn forgetting_takes_only_listed_files_off_the_list() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let first = release_with_cover(&mut db, "first", "/covers/1/provider.jpg")?;
        let second = release_with_cover(&mut db, "second", "/covers/2/provider.jpg")?;
        crate::metadata::cascade_remove_entities(&mut db, &[first, second])?;
        let handled = listed(&db)?
            .into_iter()
            .filter(|listed| listed.path == "/covers/1/provider.jpg")
            .collect::<Vec<_>>();
        forget(&mut db, &handled)?;
        enlist(&mut db, &cover("third", "/covers/3/provider.jpg"))?;
        assert!(
            listed(&db)?
                .iter()
                .any(|listed| listed.db_id == handled[0].db_id),
            "agdb hands the freed id to the next file listed"
        );
        forget(&mut db, &handled)?;

        let mut left = listed(&db)?
            .into_iter()
            .map(|listed| listed.path)
            .collect::<Vec<_>>();
        left.sort();
        assert_eq!(left, ["/covers/2/provider.jpg", "/covers/3/provider.jpg"]);
        Ok(())
    }
}
