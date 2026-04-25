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
    entries,
    graph,
    lookup,
};

#[derive(DbElement, Clone, Debug)]
pub(crate) struct CueTrack {
    pub(crate) db_id: Option<DbId>,
    pub(crate) id: String,
    pub(crate) track_no: u32,
    pub(crate) index00_frames: Option<u32>,
    pub(crate) index01_frames: u32,
    pub(crate) identity: String,
}

fn cue_track_identity(cue_entry_id: DbId, track_no: u32) -> String {
    format!("cue_entry:{}#track:{}", cue_entry_id.0, track_no)
}

fn find_track_id_by_identity(db: &impl DbAccess, identity: &str) -> anyhow::Result<Option<DbId>> {
    lookup::find_id_by_indexed_string_field(db, "cue_tracks", "identity", "identity", identity)
}

pub(crate) fn get_by_id(
    db: &impl DbAccess,
    cue_track_id: DbId,
) -> anyhow::Result<Option<CueTrack>> {
    graph::fetch_typed_by_id(db, cue_track_id, "CueTrack")
}

pub(crate) fn upsert(
    db: &mut impl DbAccess,
    cue_sheet_id: DbId,
    cue_entry_id: DbId,
    track_no: u32,
    audio_entry_id: DbId,
    index00_frames: Option<u32>,
    index01_frames: u32,
) -> anyhow::Result<DbId> {
    let identity = cue_track_identity(cue_entry_id, track_no);
    let existing_id = find_track_id_by_identity(db, &identity)?;
    let cue_track = CueTrack {
        db_id: existing_id,
        id: nanoid!(),
        track_no,
        index00_frames,
        index01_frames,
        identity,
    };

    let result = db.exec_mut(QueryBuilder::insert().element(&cue_track).query())?;
    let cue_track_id = existing_id
        .or_else(|| result.elements.first().map(|element| element.id))
        .ok_or_else(|| anyhow::anyhow!("upsert cue track returned no id"))?;

    if existing_id.is_none() {
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("cue_tracks")
                .to(cue_track_id)
                .query(),
        )?;
    }

    let has_sheet_edge = db
        .exec(
            QueryBuilder::search()
                .from(cue_sheet_id)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .query(),
        )?
        .elements
        .into_iter()
        .any(|edge| edge.from == Some(cue_sheet_id) && edge.to == Some(cue_track_id));
    if !has_sheet_edge {
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(cue_sheet_id)
                .to(cue_track_id)
                .query(),
        )?;
    }

    let outgoing_edges = db.exec(
        QueryBuilder::search()
            .from(cue_track_id)
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(1))
            .query(),
    )?;
    for edge in outgoing_edges.elements {
        if edge.from != Some(cue_track_id) {
            continue;
        }
        let Some(target_id) = edge.to else {
            continue;
        };

        let entry_query = db.exec(
            QueryBuilder::select()
                .elements::<entries::Entry>()
                .ids(target_id)
                .query(),
        )?;
        let entry_targets: Vec<entries::Entry> = match entry_query.try_into() {
            Ok(entries) => entries,
            Err(_) => Vec::new(),
        };
        let is_entry_target = !entry_targets.is_empty();
        if is_entry_target && target_id != audio_entry_id {
            db.exec_mut(QueryBuilder::remove().ids(edge.id).query())?;
        }
    }

    let has_audio_entry_edge = db
        .exec(
            QueryBuilder::search()
                .from(cue_track_id)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .query(),
        )?
        .elements
        .into_iter()
        .any(|edge| edge.from == Some(cue_track_id) && edge.to == Some(audio_entry_id));
    if !has_audio_entry_edge {
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(cue_track_id)
                .to(audio_entry_id)
                .query(),
        )?;
    }

    Ok(cue_track_id)
}
