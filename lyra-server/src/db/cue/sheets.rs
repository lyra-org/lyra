// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    CountComparison,
    DbElement,
    DbId,
    QueryBuilder,
};

use nanoid::nanoid;

use crate::db::{
    DbAccess,
    lookup,
};

#[derive(DbElement, Clone, Debug)]
pub(crate) struct CueSheet {
    pub(crate) db_id: Option<DbId>,
    pub(crate) id: String,
    pub(crate) source_hash: String,
    pub(crate) identity: String,
}

fn cue_sheet_identity(cue_entry_id: DbId) -> String {
    format!("cue_entry:{}", cue_entry_id.0)
}

fn find_sheet_id_by_identity(db: &impl DbAccess, identity: &str) -> anyhow::Result<Option<DbId>> {
    lookup::find_id_by_indexed_string_field(db, "cue_sheets", "identity", "identity", identity)
}

pub(crate) fn upsert(
    db: &mut impl DbAccess,
    cue_entry_id: DbId,
    source_hash: &str,
) -> anyhow::Result<DbId> {
    let identity = cue_sheet_identity(cue_entry_id);
    let existing_id = find_sheet_id_by_identity(db, &identity)?;
    let sheet = CueSheet {
        db_id: existing_id,
        id: nanoid!(),
        source_hash: source_hash.to_string(),
        identity,
    };

    let result = db.exec_mut(QueryBuilder::insert().element(&sheet).query())?;
    let sheet_id = existing_id
        .or_else(|| result.elements.first().map(|element| element.id))
        .ok_or_else(|| anyhow::anyhow!("upsert cue sheet returned no id"))?;

    if existing_id.is_none() {
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("cue_sheets")
                .to(sheet_id)
                .query(),
        )?;
    }

    let has_cue_entry_edge = db
        .exec(
            QueryBuilder::search()
                .from(sheet_id)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .query(),
        )?
        .elements
        .into_iter()
        .any(|edge| edge.from == Some(sheet_id) && edge.to == Some(cue_entry_id));
    if !has_cue_entry_edge {
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(sheet_id)
                .to(cue_entry_id)
                .query(),
        )?;
    }

    Ok(sheet_id)
}
