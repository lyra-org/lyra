// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use agdb::{
    CountComparison,
    DbAny,
    DbElement,
    DbId,
    DbValue,
    QueryBuilder,
};
use nanoid::nanoid;
use schemars::JsonSchema;
use serde::Serialize;

use super::DbAccess;
use super::NodeId;

#[derive(DbElement, Serialize, Clone, Debug, JsonSchema)]
pub(crate) struct Playlist {
    #[serde(skip)]
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) description: Option<String>,
    pub(crate) is_public: Option<bool>,
    pub(crate) created_at: Option<u64>,
    pub(crate) updated_at: Option<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct PlaylistTrack {
    pub(crate) edge_id: DbId,
    pub(crate) entry_id: String,
    pub(crate) position: u64,
}

pub(crate) fn create(
    db: &mut DbAny,
    playlist: &Playlist,
    owner_db_id: DbId,
) -> anyhow::Result<DbId> {
    db.transaction_mut(|t| -> anyhow::Result<DbId> {
        let playlist_db_id = t
            .exec_mut(QueryBuilder::insert().element(playlist).query())?
            .ids()[0];

        // Edge from "playlists" collection to this playlist node
        t.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("playlists")
                .to(playlist_db_id)
                .query(),
        )?;

        // Ownership edge from playlist to user, tagged with ("owner", 1)
        let owner_edge = t
            .exec_mut(
                QueryBuilder::insert()
                    .edges()
                    .from(playlist_db_id)
                    .to(owner_db_id)
                    .query(),
            )?
            .ids()[0];
        t.exec_mut(
            QueryBuilder::insert()
                .values_uniform([("owner", 1).into()])
                .ids(owner_edge)
                .query(),
        )?;

        Ok(playlist_db_id)
    })
}

pub(crate) fn get(db: &DbAny) -> anyhow::Result<Vec<Playlist>> {
    let playlists: Vec<Playlist> = db
        .exec(
            QueryBuilder::select()
                .elements::<Playlist>()
                .search()
                .from("playlists")
                .query(),
        )?
        .try_into()?;

    Ok(playlists)
}

pub(crate) fn get_by_user(db: &DbAny, user_db_id: DbId) -> anyhow::Result<Vec<Playlist>> {
    // Find edges pointing TO user_db_id that have the "owner" key
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .to(user_db_id)
            .where_()
            .edge()
            .and()
            .key("owner")
            .value(agdb::DbValue::I64(1))
            .end_where()
            .query(),
    )?;

    let mut playlists = Vec::new();
    for element in &result.elements {
        let Some(from_id) = element.from else {
            continue;
        };
        if from_id.0 <= 0 {
            continue;
        }
        if let Some(playlist) = get_by_id(db, from_id)? {
            playlists.push(playlist);
        }
    }

    Ok(playlists)
}

pub(crate) fn get_by_id(
    db: &impl super::DbAccess,
    playlist_db_id: DbId,
) -> anyhow::Result<Option<Playlist>> {
    super::graph::fetch_typed_by_id(db, playlist_db_id, "Playlist")
}

pub(crate) fn update(db: &mut impl super::DbAccess, playlist: &Playlist) -> anyhow::Result<()> {
    db.exec_mut(QueryBuilder::insert().element(playlist).query())?;
    Ok(())
}

pub(crate) fn delete(db: &mut DbAny, playlist_db_id: DbId) -> anyhow::Result<()> {
    db.transaction_mut(|t| -> anyhow::Result<()> {
        super::favorites::remove_inbound_for_target(t, playlist_db_id)?;
        super::tags::remove_inbound_for_target_with_orphan_cleanup(t, &[playlist_db_id])?;
        t.exec_mut(QueryBuilder::remove().ids(playlist_db_id).query())?;
        Ok(())
    })
}

pub(crate) fn get_owner(
    db: &impl super::DbAccess,
    playlist_db_id: DbId,
) -> anyhow::Result<Option<DbId>> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .from(playlist_db_id)
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(1))
            .and()
            .key("owner")
            .value(agdb::DbValue::I64(1))
            .end_where()
            .query(),
    )?;

    for element in &result.elements {
        let Some(to_id) = element.to else {
            continue;
        };
        if to_id.0 > 0 {
            return Ok(Some(to_id));
        }
    }

    Ok(None)
}

pub(crate) fn add_track(
    db: &mut impl DbAccess,
    playlist_db_id: DbId,
    track_db_id: DbId,
) -> anyhow::Result<PlaylistTrack> {
    let max_position = get_max_position(db, playlist_db_id)?;
    let position = max_position + 1;
    let entry_id = nanoid!();

    let edge_id = db
        .exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(playlist_db_id)
                .to(track_db_id)
                .query(),
        )?
        .ids()[0];

    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform([
                ("position", position).into(),
                ("entry_id", entry_id.as_str()).into(),
            ])
            .ids(edge_id)
            .query(),
    )?;

    Ok(PlaylistTrack {
        edge_id,
        entry_id,
        position,
    })
}

/// Add multiple tracks, returning a `PlaylistTrack` per track.
pub(crate) fn add_tracks(
    db: &mut impl DbAccess,
    playlist_db_id: DbId,
    track_db_ids: &[DbId],
) -> anyhow::Result<Vec<PlaylistTrack>> {
    let mut position = get_max_position(db, playlist_db_id)?;
    let mut results = Vec::with_capacity(track_db_ids.len());

    for &track_db_id in track_db_ids {
        position += 1;
        let entry_id = nanoid!();

        let edge_id = db
            .exec_mut(
                QueryBuilder::insert()
                    .edges()
                    .from(playlist_db_id)
                    .to(track_db_id)
                    .query(),
            )?
            .ids()[0];

        db.exec_mut(
            QueryBuilder::insert()
                .values_uniform([
                    ("position", position).into(),
                    ("entry_id", entry_id.as_str()).into(),
                ])
                .ids(edge_id)
                .query(),
        )?;

        results.push(PlaylistTrack {
            edge_id,
            entry_id,
            position,
        });
    }

    Ok(results)
}

pub(crate) fn remove_track(db: &mut DbAny, edge_id: DbId) -> anyhow::Result<()> {
    db.exec_mut(QueryBuilder::remove().ids(edge_id).query())?;
    Ok(())
}

pub(crate) fn get_tracks(
    db: &impl DbAccess,
    playlist_db_id: DbId,
) -> anyhow::Result<Vec<PlaylistTrack>> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .from(playlist_db_id)
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(1))
            .end_where()
            .query(),
    )?;

    let mut tracks = Vec::new();
    for element in &result.elements {
        // Skip the owner edge
        let is_owner = element
            .values
            .iter()
            .any(|kv| matches!(&kv.key, DbValue::String(k) if k == "owner"));
        if is_owner {
            continue;
        }

        let position = element
            .values
            .iter()
            .find_map(|kv| match (&kv.key, &kv.value) {
                (DbValue::String(k), DbValue::U64(v)) if k == "position" => Some(*v),
                _ => None,
            })
            .unwrap_or(0);

        let entry_id = element
            .values
            .iter()
            .find_map(|kv| match (&kv.key, &kv.value) {
                (DbValue::String(k), DbValue::String(v)) if k == "entry_id" => Some(v.clone()),
                _ => None,
            })
            .unwrap_or_default();

        tracks.push(PlaylistTrack {
            edge_id: element.id,
            entry_id,
            position,
        });
    }

    tracks.sort_by_key(|t| t.position);
    Ok(tracks)
}

/// Return tracks for multiple playlists in a single pass.
pub(crate) fn get_tracks_many(
    db: &impl DbAccess,
    playlist_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Vec<PlaylistTrack>>> {
    let unique_ids = super::dedup_positive_ids(playlist_db_ids);
    let mut result: HashMap<DbId, Vec<PlaylistTrack>> = unique_ids
        .iter()
        .copied()
        .map(|id| (id, Vec::new()))
        .collect();
    if unique_ids.is_empty() {
        return Ok(result);
    }

    for &playlist_id in &unique_ids {
        let tracks = get_tracks(db, playlist_id)?;
        result.insert(playlist_id, tracks);
    }

    Ok(result)
}

pub(crate) fn move_track(
    db: &mut impl DbAccess,
    playlist_db_id: DbId,
    edge_id: DbId,
    new_position: u64,
) -> anyhow::Result<()> {
    let mut tracks = get_tracks(db, playlist_db_id)?;

    // Find the track being moved
    let current_idx = tracks
        .iter()
        .position(|t| t.edge_id == edge_id)
        .ok_or_else(|| anyhow::anyhow!("edge not found in playlist"))?;

    // Remove from current position and insert at new position
    let track = tracks.remove(current_idx);
    let insert_at = (new_position as usize).min(tracks.len());
    tracks.insert(insert_at, track);

    // Renumber all positions sequentially
    for (i, t) in tracks.iter().enumerate() {
        db.exec_mut(
            QueryBuilder::insert()
                .values_uniform([("position", (i as u64)).into()])
                .ids(t.edge_id)
                .query(),
        )?;
    }

    Ok(())
}

pub(crate) fn resolve_edge_targets(db: &DbAny, edge_ids: &[DbId]) -> anyhow::Result<Vec<DbId>> {
    if edge_ids.is_empty() {
        return Ok(Vec::new());
    }
    let result = db.exec(QueryBuilder::select().ids(edge_ids).query())?;
    Ok(result.elements.iter().filter_map(|e| e.to).collect())
}

fn get_max_position(db: &impl DbAccess, playlist_db_id: DbId) -> anyhow::Result<u64> {
    let tracks = get_tracks(db, playlist_db_id)?;
    Ok(tracks.iter().map(|t| t.position).max().unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::new_test_db;
    use agdb::{
        DbAny,
        QueryBuilder,
    };
    use nanoid::nanoid;

    fn create_test_user(db: &mut DbAny) -> anyhow::Result<DbId> {
        let user_db_id = db
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values([[("username", "testuser").into()]])
                    .query(),
            )?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("users")
                .to(user_db_id)
                .query(),
        )?;
        Ok(user_db_id)
    }

    fn create_test_track(db: &mut DbAny, title: &str) -> anyhow::Result<DbId> {
        let track_db_id = db
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values([[("track_title", title).into()]])
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
        Ok(track_db_id)
    }

    #[test]
    fn create_and_get_playlist() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db)?;

        let playlist = Playlist {
            db_id: None,
            id: nanoid!(),
            name: "My Playlist".to_string(),
            description: Some("A test playlist".to_string()),
            is_public: Some(false),
            created_at: Some(1000),
            updated_at: Some(1000),
        };
        let playlist_db_id = create(&mut db, &playlist, user_db_id)?;

        let fetched = get_by_id(&db, playlist_db_id)?.expect("playlist should exist");
        assert_eq!(fetched.name, "My Playlist");
        assert_eq!(fetched.description.as_deref(), Some("A test playlist"));

        Ok(())
    }

    #[test]
    fn get_playlists_by_user() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_a = create_test_user(&mut db)?;
        let user_b = create_test_user(&mut db)?;

        let p1 = Playlist {
            db_id: None,
            id: nanoid!(),
            name: "User A Playlist".to_string(),
            description: None,
            is_public: None,
            created_at: None,
            updated_at: None,
        };
        create(&mut db, &p1, user_a)?;

        let p2 = Playlist {
            db_id: None,
            id: nanoid!(),
            name: "User B Playlist".to_string(),
            description: None,
            is_public: None,
            created_at: None,
            updated_at: None,
        };
        create(&mut db, &p2, user_b)?;

        let a_playlists = get_by_user(&db, user_a)?;
        assert_eq!(a_playlists.len(), 1);
        assert_eq!(a_playlists[0].name, "User A Playlist");

        let b_playlists = get_by_user(&db, user_b)?;
        assert_eq!(b_playlists.len(), 1);
        assert_eq!(b_playlists[0].name, "User B Playlist");

        Ok(())
    }

    #[test]
    fn playlist_owner() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db)?;

        let playlist = Playlist {
            db_id: None,
            id: nanoid!(),
            name: "Owned".to_string(),
            description: None,
            is_public: None,
            created_at: None,
            updated_at: None,
        };
        let playlist_db_id = create(&mut db, &playlist, user_db_id)?;

        let owner = get_owner(&db, playlist_db_id)?;
        assert_eq!(owner, Some(user_db_id));

        Ok(())
    }

    #[test]
    fn add_and_get_tracks() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db)?;
        let track_a = create_test_track(&mut db, "Track A")?;
        let track_b = create_test_track(&mut db, "Track B")?;

        let playlist = Playlist {
            db_id: None,
            id: nanoid!(),
            name: "Tracks Test".to_string(),
            description: None,
            is_public: None,
            created_at: None,
            updated_at: None,
        };
        let playlist_db_id = create(&mut db, &playlist, user_db_id)?;

        let pt_a = add_track(&mut db, playlist_db_id, track_a)?;
        let pt_b = add_track(&mut db, playlist_db_id, track_b)?;

        assert!(pt_a.edge_id.0 < 0, "edge ids should be negative");
        assert!(pt_b.edge_id.0 < 0, "edge ids should be negative");

        let tracks = get_tracks(&db, playlist_db_id)?;
        assert_eq!(tracks.len(), 2);
        assert_eq!(tracks[0].position, 1);
        assert_eq!(tracks[1].position, 2);

        let track_ids = resolve_edge_targets(&db, &[tracks[0].edge_id, tracks[1].edge_id])?;
        assert_eq!(track_ids[0], track_a);
        assert_eq!(track_ids[1], track_b);

        Ok(())
    }

    #[test]
    fn remove_track_deletes_edge() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db)?;
        let track = create_test_track(&mut db, "Track")?;

        let playlist = Playlist {
            db_id: None,
            id: nanoid!(),
            name: "Remove Test".to_string(),
            description: None,
            is_public: None,
            created_at: None,
            updated_at: None,
        };
        let playlist_db_id = create(&mut db, &playlist, user_db_id)?;
        let pt = add_track(&mut db, playlist_db_id, track)?;

        remove_track(&mut db, pt.edge_id)?;

        let tracks = get_tracks(&db, playlist_db_id)?;
        assert!(tracks.is_empty());

        Ok(())
    }

    #[test]
    fn move_track_reorders_positions() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db)?;
        let track_a = create_test_track(&mut db, "A")?;
        let track_b = create_test_track(&mut db, "B")?;
        let track_c = create_test_track(&mut db, "C")?;

        let playlist = Playlist {
            db_id: None,
            id: nanoid!(),
            name: "Move Test".to_string(),
            description: None,
            is_public: None,
            created_at: None,
            updated_at: None,
        };
        let playlist_db_id = create(&mut db, &playlist, user_db_id)?;

        let _pt_a = add_track(&mut db, playlist_db_id, track_a)?;
        let _pt_b = add_track(&mut db, playlist_db_id, track_b)?;
        let pt_c = add_track(&mut db, playlist_db_id, track_c)?;

        // Move C to position 0 (first)
        move_track(&mut db, playlist_db_id, pt_c.edge_id, 0)?;

        let tracks = get_tracks(&db, playlist_db_id)?;
        assert_eq!(tracks.len(), 3);

        let track_ids = resolve_edge_targets(
            &db,
            &[tracks[0].edge_id, tracks[1].edge_id, tracks[2].edge_id],
        )?;
        assert_eq!(track_ids[0], track_c);
        assert_eq!(track_ids[1], track_a);
        assert_eq!(track_ids[2], track_b);

        Ok(())
    }

    #[test]
    fn delete_playlist_removes_node() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db)?;

        let playlist = Playlist {
            db_id: None,
            id: nanoid!(),
            name: "Delete Me".to_string(),
            description: None,
            is_public: None,
            created_at: None,
            updated_at: None,
        };
        let playlist_db_id = create(&mut db, &playlist, user_db_id)?;
        delete(&mut db, playlist_db_id)?;

        let result = get_by_id(&db, playlist_db_id)?;
        assert!(result.is_none());

        Ok(())
    }

    #[test]
    fn duplicate_tracks_allowed() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db)?;
        let track = create_test_track(&mut db, "Same Track")?;

        let playlist = Playlist {
            db_id: None,
            id: nanoid!(),
            name: "Dupes".to_string(),
            description: None,
            is_public: None,
            created_at: None,
            updated_at: None,
        };
        let playlist_db_id = create(&mut db, &playlist, user_db_id)?;

        let pt_1 = add_track(&mut db, playlist_db_id, track)?;
        let pt_2 = add_track(&mut db, playlist_db_id, track)?;

        assert_ne!(pt_1.edge_id, pt_2.edge_id);

        let tracks = get_tracks(&db, playlist_db_id)?;
        assert_eq!(tracks.len(), 2);

        let track_ids = resolve_edge_targets(&db, &[tracks[0].edge_id, tracks[1].edge_id])?;
        assert_eq!(track_ids[0], track);
        assert_eq!(track_ids[1], track);

        Ok(())
    }
}
