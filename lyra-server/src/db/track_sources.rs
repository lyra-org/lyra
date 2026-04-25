// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    CountComparison,
    DbAny,
    DbElement,
    DbId,
    QueryBuilder,
};

use nanoid::nanoid;

use super::DbAccess;

#[derive(DbElement, Clone, Debug)]
pub(crate) struct TrackSource {
    pub(crate) db_id: Option<DbId>,
    pub(crate) id: String,
    pub(crate) source_kind: String,
    pub(crate) source_key: String,
    pub(crate) identity: String,
    pub(crate) is_primary: bool,
    pub(crate) start_ms: Option<u64>,
    pub(crate) end_ms: Option<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct TrackSourceUpsert {
    pub(crate) source_kind: String,
    pub(crate) source_key: String,
    pub(crate) is_primary: bool,
    pub(crate) start_ms: Option<u64>,
    pub(crate) end_ms: Option<u64>,
}

fn find_source_id_by_identity(db: &impl DbAccess, identity: &str) -> anyhow::Result<Option<DbId>> {
    super::lookup::find_id_by_indexed_string_field(
        db,
        "track_sources",
        "identity",
        "identity",
        identity,
    )
}

pub(crate) fn get_by_id(
    db: &impl super::DbAccess,
    source_id: DbId,
) -> anyhow::Result<Option<TrackSource>> {
    super::graph::fetch_typed_by_id(db, source_id, "TrackSource")
}

pub(crate) fn get_by_track(db: &DbAny, track_db_id: DbId) -> anyhow::Result<Vec<TrackSource>> {
    let sources: Vec<TrackSource> = db
        .exec(
            QueryBuilder::select()
                .elements::<TrackSource>()
                .search()
                .from(track_db_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    Ok(sources)
}

pub(crate) fn get_primary_by_track(
    db: &DbAny,
    track_db_id: DbId,
) -> anyhow::Result<Option<TrackSource>> {
    let mut sources = get_by_track(db, track_db_id)?;
    sources.sort_by_key(|source| !source.is_primary);
    Ok(sources.into_iter().next())
}

pub(crate) fn get_entry_id(db: &DbAny, source_id: DbId) -> anyhow::Result<Option<DbId>> {
    let entries: Vec<super::entries::Entry> = db
        .exec(
            QueryBuilder::select()
                .elements::<super::entries::Entry>()
                .search()
                .from(source_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    Ok(entries.into_iter().find_map(|e| e.db_id))
}

pub(crate) fn get_track_id_by_source_key(
    db: &DbAny,
    source_key: &str,
) -> anyhow::Result<Option<DbId>> {
    let Some(source_id) = find_source_id_by_identity(db, source_key)? else {
        return Ok(None);
    };

    let tracks: Vec<super::tracks::Track> = db
        .exec(
            QueryBuilder::select()
                .elements::<super::tracks::Track>()
                .search()
                .to(source_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    Ok(tracks.into_iter().find_map(|t| t.db_id.map(DbId::from)))
}

pub(crate) fn upsert(
    db: &mut impl DbAccess,
    track_db_id: DbId,
    entry_db_id: DbId,
    source: TrackSourceUpsert,
    cue_track_id: Option<DbId>,
) -> anyhow::Result<DbId> {
    let existing_id = find_source_id_by_identity(db, &source.source_key)?;
    let source_node = TrackSource {
        db_id: existing_id,
        id: nanoid!(),
        source_kind: source.source_kind,
        source_key: source.source_key.clone(),
        identity: source.source_key,
        is_primary: source.is_primary,
        start_ms: source.start_ms,
        end_ms: source.end_ms,
    };

    let result = db.exec_mut(QueryBuilder::insert().element(&source_node).query())?;
    let source_id = existing_id
        .or_else(|| result.elements.first().map(|element| element.id))
        .ok_or_else(|| anyhow::anyhow!("upsert track source returned no id"))?;

    if existing_id.is_none() {
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("track_sources")
                .to(source_id)
                .query(),
        )?;
    }

    let has_track_edge = db
        .exec(
            QueryBuilder::search()
                .from(track_db_id)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .query(),
        )?
        .elements
        .into_iter()
        .any(|edge| edge.from == Some(track_db_id) && edge.to == Some(source_id));
    if !has_track_edge {
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(track_db_id)
                .to(source_id)
                .query(),
        )?;
    }

    if source_node.is_primary {
        let connected_edges = db.exec(
            QueryBuilder::search()
                .from(track_db_id)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .query(),
        )?;
        for edge in connected_edges.elements {
            if edge.from != Some(track_db_id) {
                continue;
            }
            let Some(target_id) = edge.to else {
                continue;
            };

            let source_query = db.exec(
                QueryBuilder::select()
                    .elements::<TrackSource>()
                    .ids(target_id)
                    .query(),
            )?;
            let mut connected_sources: Vec<TrackSource> = match source_query.try_into() {
                Ok(sources) => sources,
                Err(_) => continue,
            };
            let Some(connected) = connected_sources.pop() else {
                continue;
            };
            let Some(connected_id) = connected.db_id else {
                continue;
            };

            if connected_id == source_id || !connected.is_primary {
                continue;
            }

            db.exec_mut(
                QueryBuilder::insert()
                    .values_uniform([("is_primary", false).into()])
                    .ids(connected_id)
                    .query(),
            )?;
        }
    }

    let outgoing_edges = db.exec(
        QueryBuilder::search()
            .from(source_id)
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(1))
            .query(),
    )?;

    for edge in outgoing_edges.elements {
        if edge.from != Some(source_id) {
            continue;
        }
        let Some(target_id) = edge.to else {
            continue;
        };

        let entry_query = db.exec(
            QueryBuilder::select()
                .elements::<super::entries::Entry>()
                .ids(target_id)
                .query(),
        )?;
        let entry_targets: Vec<super::entries::Entry> = match entry_query.try_into() {
            Ok(entries) => entries,
            Err(_) => Vec::new(),
        };
        if !entry_targets.is_empty() && target_id != entry_db_id {
            db.exec_mut(QueryBuilder::remove().ids(edge.id).query())?;
            continue;
        }

        let cue_track_query = db.exec(
            QueryBuilder::select()
                .elements::<super::cue::tracks::CueTrack>()
                .ids(target_id)
                .query(),
        )?;
        let cue_track_targets: Vec<super::cue::tracks::CueTrack> = match cue_track_query.try_into()
        {
            Ok(cue_tracks) => cue_tracks,
            Err(_) => Vec::new(),
        };
        if !cue_track_targets.is_empty() && Some(target_id) != cue_track_id {
            db.exec_mut(QueryBuilder::remove().ids(edge.id).query())?;
        }
    }

    let has_entry_edge = db
        .exec(
            QueryBuilder::search()
                .from(source_id)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .query(),
        )?
        .elements
        .into_iter()
        .any(|edge| edge.from == Some(source_id) && edge.to == Some(entry_db_id));
    if !has_entry_edge {
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(source_id)
                .to(entry_db_id)
                .query(),
        )?;
    }

    if let Some(cue_track_id) = cue_track_id {
        let has_cue_track_edge = db
            .exec(
                QueryBuilder::search()
                    .from(source_id)
                    .where_()
                    .edge()
                    .and()
                    .distance(CountComparison::Equal(1))
                    .query(),
            )?
            .elements
            .into_iter()
            .any(|edge| edge.from == Some(source_id) && edge.to == Some(cue_track_id));
        if !has_cue_track_edge {
            db.exec_mut(
                QueryBuilder::insert()
                    .edges()
                    .from(source_id)
                    .to(cue_track_id)
                    .query(),
            )?;
        }
    }

    Ok(source_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::{
        insert_track,
        new_test_db,
    };
    use anyhow::anyhow;

    fn insert_entry(db: &mut DbAny) -> anyhow::Result<DbId> {
        let entry = super::super::entries::Entry {
            db_id: None,
            id: nanoid!(),
            full_path: std::path::PathBuf::from("/music/test.mp3"),
            kind: super::super::entries::EntryKind::File,
            file_kind: Some("audio".to_string()),
            name: "test.mp3".to_string(),
            hash: None,
            size: 1,
            mtime: 1,
            ctime: 1,
        };
        let qr = db.exec_mut(QueryBuilder::insert().element(&entry).query())?;
        qr.ids()
            .first()
            .copied()
            .ok_or_else(|| anyhow!("entry insert returned no id"))
    }

    #[test]
    fn get_entry_id_returns_entry_linked_to_source() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track_db_id = insert_track(&mut db, "Track 1")?;
        let entry_db_id = insert_entry(&mut db)?;
        let source_key = format!("entry:{}:embedded", entry_db_id.0);

        let source_id = upsert(
            &mut db,
            track_db_id,
            entry_db_id,
            TrackSourceUpsert {
                source_kind: "embedded_tags".to_string(),
                source_key,
                is_primary: true,
                start_ms: None,
                end_ms: None,
            },
            None,
        )?;

        let found_entry_db_id = get_entry_id(&db, source_id)?;
        assert_eq!(found_entry_db_id, Some(entry_db_id));
        Ok(())
    }

    #[test]
    fn get_entry_id_returns_none_when_no_entry_linked() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let bare_node = db
            .exec_mut(QueryBuilder::insert().nodes().count(1).query())?
            .ids()[0];

        let found = get_entry_id(&db, bare_node)?;
        assert_eq!(found, None);
        Ok(())
    }

    #[test]
    fn get_by_track_returns_sources_for_track() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track_db_id = insert_track(&mut db, "Track 1")?;
        let entry_db_id = insert_entry(&mut db)?;

        upsert(
            &mut db,
            track_db_id,
            entry_db_id,
            TrackSourceUpsert {
                source_kind: "embedded_tags".to_string(),
                source_key: "key1".to_string(),
                is_primary: true,
                start_ms: None,
                end_ms: None,
            },
            None,
        )?;

        let sources = get_by_track(&db, track_db_id)?;
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].source_key, "key1");
        Ok(())
    }

    #[test]
    fn get_track_id_by_source_key_follows_incoming_track_edge() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track_db_id = insert_track(&mut db, "Track 1")?;
        let entry_qr = db.exec_mut(QueryBuilder::insert().nodes().count(1).query())?;
        let entry_db_id = entry_qr
            .ids()
            .first()
            .copied()
            .ok_or_else(|| anyhow!("entry insert returned no id"))?;
        let source_key = format!("entry:{}:embedded", entry_db_id.0);

        upsert(
            &mut db,
            track_db_id,
            entry_db_id,
            TrackSourceUpsert {
                source_kind: "embedded_tags".to_string(),
                source_key: source_key.clone(),
                is_primary: true,
                start_ms: None,
                end_ms: None,
            },
            None,
        )?;

        let found_track_db_id = get_track_id_by_source_key(&db, &source_key)?;
        assert_eq!(found_track_db_id, Some(track_db_id));
        Ok(())
    }
}
