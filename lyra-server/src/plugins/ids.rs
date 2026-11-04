// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The public ids plugins name nodes by, resolved to the database ids the services take.
//! Database ids never reach Luau: agdb reuses them after deletions.

use std::collections::HashMap;

use agdb::{
    DbAny,
    DbId,
};
use harmony_luau as luau;
use lyra_db::DbAccess;

use crate::services::catalog::Viewer;

/// Public ids resolved to the nodes they name, in the order given. Ids that name no node are
/// left out.
pub(crate) struct Resolved {
    db_ids: Vec<DbId>,
    public: HashMap<DbId, String>,
}

impl Resolved {
    pub(crate) fn of(db: &impl DbAccess, ids: Vec<String>) -> luau::runtime::Result<Self> {
        let mut db_ids = Vec::with_capacity(ids.len());
        let mut public = HashMap::with_capacity(ids.len());
        for id in ids {
            if let Some(db_id) = db_id(db, &id)? {
                db_ids.push(db_id);
                public.insert(db_id, id);
            }
        }
        Ok(Self { db_ids, public })
    }

    pub(crate) fn db_ids(&self) -> &[DbId] {
        &self.db_ids
    }

    /// The public id `db_id` was resolved from.
    pub(crate) fn public(&self, db_id: DbId) -> Option<&str> {
        self.public.get(&db_id).map(String::as_str)
    }

    /// A Luau table of `values`, keyed by the public ids their database ids were resolved from.
    pub(crate) fn keyed<V: Into<luau::Value>>(
        &self,
        values: impl IntoIterator<Item = (DbId, V)>,
    ) -> luau::OwnedTable {
        let mut table = luau::OwnedTable::new();
        for (db_id, value) in values {
            if let Some(id) = self.public(db_id) {
                table.set_field(id, value);
            }
        }
        table
    }
}

/// The node `id` names.
pub(crate) fn db_id(db: &impl DbAccess, id: &str) -> luau::runtime::Result<Option<DbId>> {
    lyra_db::lookup::find_node_id_by_id(db, id).map_err(crate::plugins::runtime_error)
}

/// The public ids of these nodes. Nodes without one are absent.
pub(crate) fn public_ids(
    db: &impl DbAccess,
    db_ids: &[DbId],
) -> luau::runtime::Result<HashMap<DbId, String>> {
    lyra_db::lookup::find_ids_by_db_ids(db, db_ids).map_err(crate::plugins::runtime_error)
}

/// The public id of a node.
pub(crate) fn public_id(db: &impl DbAccess, db_id: DbId) -> luau::runtime::Result<String> {
    lyra_db::lookup::find_id_by_db_id(db, db_id)
        .map_err(crate::plugins::runtime_error)?
        .ok_or_else(|| crate::plugins::runtime_error("the node has no public id"))
}

/// The public ids of `db_ids` as a Luau list, in order. Nodes without one are left out.
pub(crate) fn public_id_list(
    db: &impl DbAccess,
    db_ids: &[DbId],
) -> luau::runtime::Result<luau::OwnedTable> {
    let public_ids = public_ids(db, db_ids)?;
    Ok(luau::OwnedTable::from_array(
        db_ids.iter().filter_map(|id| public_ids.get(id).cloned()),
    ))
}

/// What kind of node `db_id` is, as its stored type names it, such as `"Track"`.
pub(crate) fn kind(db: &DbAny, db_id: DbId) -> luau::runtime::Result<Option<String>> {
    lyra_db::graph::element_kind(db, db_id).map_err(crate::plugins::runtime_error)
}

/// The node `id` names when it is a `kind`, such as `"Genre"`.
pub(crate) fn db_id_of(db: &DbAny, id: &str, kind: &str) -> luau::runtime::Result<Option<DbId>> {
    let Some(db_id) = db_id(db, id)? else {
        return Ok(None);
    };
    Ok((self::kind(db, db_id)?.as_deref() == Some(kind)).then_some(db_id))
}

/// The node `id` names when `viewer` can see it, so a hidden node reads as missing.
pub(crate) fn visible_db_id(
    db: &DbAny,
    viewer: &Viewer,
    id: &str,
) -> luau::runtime::Result<Option<DbId>> {
    let Some(db_id) = db_id(db, id)? else {
        return Ok(None);
    };
    let visible = viewer
        .sees(db, db_id)
        .map_err(crate::plugins::runtime_error)?;
    Ok(visible.then_some(db_id))
}

/// [`visible_db_id`] for a node that must be a `kind`, such as `"Track"`.
pub(crate) fn visible_db_id_of(
    db: &DbAny,
    viewer: &Viewer,
    id: &str,
    kind: &str,
) -> luau::runtime::Result<Option<DbId>> {
    let Some(db_id) = db_id_of(db, id, kind)? else {
        return Ok(None);
    };
    let visible = viewer
        .sees(db, db_id)
        .map_err(crate::plugins::runtime_error)?;
    Ok(visible.then_some(db_id))
}

/// The entries of `values` keyed by nodes `viewer` can see, so a batch read leaves out what a
/// single-target read would.
pub(crate) fn seen_by<V>(
    db: &DbAny,
    viewer: &Viewer,
    values: HashMap<DbId, V>,
) -> luau::runtime::Result<HashMap<DbId, V>> {
    let mut seen = HashMap::with_capacity(values.len());
    for (db_id, value) in values {
        if viewer
            .sees(db, db_id)
            .map_err(crate::plugins::runtime_error)?
        {
            seen.insert(db_id, value);
        }
    }
    Ok(seen)
}

/// The caller's viewer, verified under the guard `db` was read through.
pub(crate) fn viewer_of(
    db: &impl DbAccess,
    principal: &crate::services::auth::Principal,
) -> luau::runtime::Result<Viewer> {
    Viewer::listener(db, principal.clone()).map_err(crate::plugins::errors::auth_error)
}

/// [`visible_db_id_of`] for the dispatch principal's viewer.
pub(crate) fn visible_to(
    db: &DbAny,
    principal: &crate::services::auth::Principal,
    id: &str,
    kind: &str,
) -> luau::runtime::Result<Option<DbId>> {
    visible_db_id_of(db, &viewer_of(db, principal)?, id, kind)
}
