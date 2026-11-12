// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The root collections that grow with the catalog.
//!
//! Every read and write of these collections goes through this module, so their layout can change
//! here alone. Each root links its members directly.

use agdb::{
    CountComparison,
    DbElement,
    DbId,
    DbType,
    QueryBuilder,
};

use super::DbAccess;

/// The distance of a root's members from the root: root, edge, member.
pub(crate) const MEMBERS: CountComparison = CountComparison::Equal(2);

/// The distance from the root of the members' outgoing edges.
const MEMBER_EDGES: CountComparison = CountComparison::Equal(3);

/// The distance from the root of the nodes the members' outgoing edges lead to.
pub(crate) const MEMBER_CHILDREN: CountComparison = CountComparison::Equal(4);

/// The root collections that something enumerates in full and that grow with the catalog.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Root {
    Artists,
    /// The files of removed covers, until the server has discarded them.
    CoverDiscards,
    CueSheets,
    CueTracks,
    Genres,
    Labels,
    /// The entries a sync last failed to find, so availability reads them without walking every
    /// entry.
    MissingEntries,
    Releases,
    Tracks,
    TrackSources,
}

impl Root {
    /// The root node's alias. Search from it at [`MEMBERS`] rather than for its neighbors.
    pub(crate) const fn alias(self) -> &'static str {
        match self {
            Self::Artists => "artists",
            Self::CoverDiscards => "cover_discards",
            Self::CueSheets => "cue_sheets",
            Self::CueTracks => "cue_tracks",
            Self::Genres => "genres",
            Self::Labels => "labels",
            Self::MissingEntries => "missing_entries",
            Self::Releases => "releases",
            Self::Tracks => "tracks",
            Self::TrackSources => "track_sources",
        }
    }
}

/// Makes `id` a member of `root`.
pub fn link(db: &mut impl DbAccess, root: Root, id: DbId) -> anyhow::Result<()> {
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(root.alias())
            .to(id)
            .query(),
    )?;
    Ok(())
}

/// Makes `ids` members of `root`.
pub fn link_all(db: &mut impl DbAccess, root: Root, ids: &[DbId]) -> anyhow::Result<()> {
    if !ids.is_empty() {
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(root.alias())
                .to(ids)
                .query(),
        )?;
    }
    Ok(())
}

/// Whether `root` has any member.
pub(crate) fn has_members(db: &impl DbAccess, root: Root) -> anyhow::Result<bool> {
    Ok(!newest_member_edges(db, root, 1)?.is_empty())
}

/// Up to `limit` of the root's edges to its members, as `(edge, member)`, newest first: the
/// order in which removing them is cheap.
pub(crate) fn newest_member_edges(
    db: &impl DbAccess,
    root: Root,
    limit: usize,
) -> anyhow::Result<Vec<(DbId, DbId)>> {
    Ok(db
        .exec(
            QueryBuilder::search()
                .from(root.alias())
                .limit(limit as u64)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .query(),
        )?
        .elements
        .into_iter()
        .map(|edge| (edge.id, edge.to))
        .collect())
}

/// Ends the membership of `ids` in `root`, leaving the nodes.
pub fn unlink(db: &mut impl DbAccess, root: Root, ids: &[DbId]) -> anyhow::Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    let ids = ids
        .iter()
        .copied()
        .collect::<std::collections::HashSet<_>>();
    let edges = member_edge_ids(db, root)?
        .into_iter()
        .filter(|(_, to)| ids.contains(to))
        .map(|(edge, _)| edge)
        .collect::<Vec<_>>();
    if !edges.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(edges).query())?;
    }
    Ok(())
}

/// The root's edges to its members, as `(edge, member)`, newest first.
fn member_edge_ids(db: &impl DbAccess, root: Root) -> anyhow::Result<Vec<(DbId, DbId)>> {
    Ok(db
        .exec(
            QueryBuilder::search()
                .from(root.alias())
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .query(),
        )?
        .elements
        .into_iter()
        .map(|edge| (edge.id, edge.to))
        .collect())
}

/// The members of `root`, in no particular order.
pub fn member_ids(db: &impl DbAccess, root: Root) -> anyhow::Result<Vec<DbId>> {
    Ok(db
        .exec(
            QueryBuilder::search()
                .from(root.alias())
                .where_()
                .distance(MEMBERS)
                .query(),
        )?
        .ids())
}

/// The members of `root` that are `T` elements, in no particular order.
pub fn members<T: DbType<ValueType = T>>(db: &impl DbAccess, root: Root) -> anyhow::Result<Vec<T>> {
    Ok(db
        .exec(
            QueryBuilder::select()
                .elements::<T>()
                .search()
                .from(root.alias())
                .where_()
                .distance(MEMBERS)
                .query(),
        )?
        .try_into()?)
}

/// The members' outgoing edges.
pub(crate) fn member_edges(db: &impl DbAccess, root: Root) -> anyhow::Result<Vec<DbElement>> {
    Ok(db
        .exec(
            QueryBuilder::search()
                .from(root.alias())
                .where_()
                .edge()
                .and()
                .distance(MEMBER_EDGES)
                .query(),
        )?
        .elements)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::test_db::new_test_db;

    fn insert_node(db: &mut impl DbAccess) -> anyhow::Result<DbId> {
        Ok(db
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values([[("db_element_id", "Member").into()]])
                    .query(),
            )?
            .ids()[0])
    }

    #[test]
    fn members_round_trip() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let mut ids = Vec::new();
        for _ in 0..64 {
            let id = insert_node(&mut db)?;
            link(&mut db, Root::Tracks, id)?;
            ids.push(id);
        }
        let other = insert_node(&mut db)?;
        link(&mut db, Root::Releases, other)?;

        let members = member_ids(&db, Root::Tracks)?;
        assert_eq!(
            members.iter().copied().collect::<HashSet<_>>(),
            ids.iter().copied().collect::<HashSet<_>>()
        );
        assert_eq!(members.len(), ids.len());

        db.exec_mut(QueryBuilder::remove().ids(&ids[..32]).query())?;
        assert_eq!(
            member_ids(&db, Root::Tracks)?
                .into_iter()
                .collect::<HashSet<_>>(),
            ids[32..].iter().copied().collect::<HashSet<_>>()
        );
        assert_eq!(member_ids(&db, Root::Releases)?, vec![other]);
        Ok(())
    }

    #[test]
    fn members_hang_directly_off_the_root() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let member = insert_node(&mut db)?;
        let child = insert_node(&mut db)?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(member)
                .to(child)
                .query(),
        )?;
        link(&mut db, Root::Artists, member)?;

        let neighbors = db
            .exec(
                QueryBuilder::search()
                    .from(Root::Artists.alias())
                    .where_()
                    .neighbor()
                    .query(),
            )?
            .ids();
        assert_eq!(neighbors, vec![member]);
        let edges = member_edges(&db, Root::Artists)?;
        assert_eq!(edges.len(), 1);
        assert_eq!((edges[0].from, edges[0].to), (member, child));
        let children = db
            .exec(
                QueryBuilder::search()
                    .from(Root::Artists.alias())
                    .where_()
                    .distance(MEMBER_CHILDREN)
                    .query(),
            )?
            .ids();
        assert_eq!(children, vec![child]);
        Ok(())
    }
}
