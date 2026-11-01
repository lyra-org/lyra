// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use agdb::{
    CountComparison,
    DbAny,
    DbAnyTransactionMut,
    DbElement,
    DbId,
    DbValue,
    QueryBuilder,
};
use nanoid::nanoid;
use serde::Serialize;

use super::DbAccess;
use super::NodeId;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(DbElement, Serialize, Clone, Debug)]
pub struct Playlist {
    #[serde(skip)]
    pub db_id: Option<NodeId>,
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub is_public: Option<bool>,
    pub created_at_ms: Option<u64>,
    pub updated_at_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct PlaylistTrack {
    pub edge_id: DbId,
    pub entry_id: String,
    pub position: u64,
}

#[cfg(any(test, feature = "testing"))]
pub fn create(db: &mut DbAny, playlist: &Playlist, owner_db_id: DbId) -> anyhow::Result<DbId> {
    db.transaction_mut(|t| create_in_transaction(t, playlist, owner_db_id))
}

pub fn create_in_transaction(
    t: &mut DbAnyTransactionMut<'_>,
    playlist: &Playlist,
    owner_db_id: DbId,
) -> anyhow::Result<DbId> {
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
}

pub fn get(db: &DbAny) -> anyhow::Result<Vec<Playlist>> {
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

pub fn get_by_user(db: &DbAny, user_db_id: DbId) -> anyhow::Result<Vec<Playlist>> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .to(user_db_id)
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

    let mut playlists = Vec::new();
    for element in &result.elements {
        if let Some(playlist) = get_by_id(db, element.from)? {
            playlists.push(playlist);
        }
    }

    Ok(playlists)
}

pub fn get_by_id(
    db: &impl super::DbAccess,
    playlist_db_id: DbId,
) -> anyhow::Result<Option<Playlist>> {
    super::graph::fetch_typed_by_id(db, playlist_db_id, "Playlist")
}

/// Atomically aligns the stored row to `playlist`.
pub fn update(db: &mut DbAny, playlist: &Playlist) -> anyhow::Result<()> {
    db.transaction_mut(|t| update_in_transaction(t, playlist))
}

pub fn update_in_transaction(
    db: &mut DbAnyTransactionMut<'_>,
    playlist: &Playlist,
) -> anyhow::Result<()> {
    let playlist_db_id = playlist
        .db_id
        .clone()
        .map(DbId::from)
        .ok_or_else(|| anyhow::anyhow!("playlist update missing db_id"))?;
    super::replace_element_in_transaction(
        db,
        playlist_db_id,
        [
            ("description", playlist.description.is_none()),
            ("is_public", playlist.is_public.is_none()),
            ("created_at_ms", playlist.created_at_ms.is_none()),
            ("updated_at_ms", playlist.updated_at_ms.is_none()),
        ],
        playlist,
    )
}

pub fn delete(db: &mut DbAny, playlist_db_id: DbId) -> anyhow::Result<()> {
    db.transaction_mut(|t| -> anyhow::Result<()> {
        super::favorites::remove_inbound_for_target(t, playlist_db_id)?;
        super::covers::display::remove_playlist_cover_profile(t, playlist_db_id)?;
        t.exec_mut(QueryBuilder::remove().ids(playlist_db_id).query())?;
        Ok(())
    })
}

pub fn get_owner(db: &impl super::DbAccess, playlist_db_id: DbId) -> anyhow::Result<Option<DbId>> {
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
        let to_id = element.to;
        if to_id.0 == 0 {
            continue;
        }
        if to_id.0 > 0 {
            return Ok(Some(to_id));
        }
    }

    Ok(None)
}

pub fn add_track(
    db: &mut impl DbAccess,
    playlist_db_id: DbId,
    track_db_id: DbId,
) -> anyhow::Result<PlaylistTrack> {
    add_tracks(db, playlist_db_id, &[track_db_id])?
        .pop()
        .ok_or_else(|| anyhow::anyhow!("adding a playlist entry inserted none"))
}

/// Add multiple tracks, returning a `PlaylistTrack` per track.
pub fn add_tracks(
    db: &mut impl DbAccess,
    playlist_db_id: DbId,
    track_db_ids: &[DbId],
) -> anyhow::Result<Vec<PlaylistTrack>> {
    let stored = stored_entries(db, playlist_db_id)?;
    let next_stored = stored
        .iter()
        .map(|t| t.position.saturating_add(1))
        .max()
        .unwrap_or(0);
    let mut added = insert_entries(db, playlist_db_id, track_db_ids, next_stored)?;
    // Positions read as indexes, which a gap left by an older build may put behind the stored one.
    for (index, entry) in (stored.len() as u64..).zip(&mut added) {
        entry.position = index;
    }
    Ok(added)
}

/// Inserts `track_db_ids` in order at the 0-based `position`, clamped to the playlist's length,
/// renumbering every entry as [`move_track`] does. Returns a `PlaylistTrack` per track.
pub fn insert_tracks(
    db: &mut impl DbAccess,
    playlist_db_id: DbId,
    track_db_ids: &[DbId],
    position: u64,
) -> anyhow::Result<Vec<PlaylistTrack>> {
    let tracks = stored_entries(db, playlist_db_id)?;
    let insert_at = usize::try_from(position)
        .unwrap_or(usize::MAX)
        .min(tracks.len());
    let (before, after) = tracks.split_at(insert_at);
    set_positions(db, before.iter().map(|t| t.edge_id), 0)?;
    set_positions(
        db,
        after.iter().map(|t| t.edge_id),
        (insert_at + track_db_ids.len()) as u64,
    )?;
    insert_entries(db, playlist_db_id, track_db_ids, insert_at as u64)
}

fn insert_entries(
    db: &mut impl DbAccess,
    playlist_db_id: DbId,
    track_db_ids: &[DbId],
    first_position: u64,
) -> anyhow::Result<Vec<PlaylistTrack>> {
    if track_db_ids.is_empty() {
        return Ok(Vec::new());
    }
    let entries = (first_position..)
        .zip(track_db_ids)
        .map(|(position, _)| (position, nanoid!()))
        .collect::<Vec<_>>();
    let values = entries
        .iter()
        .map(|(position, entry_id)| {
            vec![
                ("position", *position).into(),
                ("entry_id", entry_id.as_str()).into(),
            ]
        })
        .collect::<Vec<_>>();
    // One pairwise insert for every entry: an insert per entry costs a write each.
    let edge_ids = db
        .exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(vec![playlist_db_id; track_db_ids.len()])
                .to(track_db_ids)
                .values(values)
                .query(),
        )?
        .ids();
    anyhow::ensure!(
        edge_ids.len() == entries.len(),
        "inserted {} playlist entries, expected {}",
        edge_ids.len(),
        entries.len()
    );
    Ok(edge_ids
        .into_iter()
        .zip(entries)
        .map(|(edge_id, (position, entry_id))| PlaylistTrack {
            edge_id,
            entry_id,
            position,
        })
        .collect())
}

/// Removes one entry edge without renumbering the rest, for tests that need a gap.
#[cfg(test)]
pub fn remove_track(db: &mut impl DbAccess, edge_id: DbId) -> anyhow::Result<()> {
    db.exec_mut(QueryBuilder::remove().ids(edge_id).query())?;
    Ok(())
}

/// Removes these entry edges of the playlist in one query.
///
/// agdb keeps a node's outgoing edges in a singly linked list and unlinks an edge by walking
/// from the head to its predecessor, so the edges go in list order: removing a whole playlist
/// is then linear rather than quadratic.
pub fn remove_tracks(
    db: &mut impl DbAccess,
    playlist_db_id: DbId,
    edge_ids: &[DbId],
) -> anyhow::Result<()> {
    if edge_ids.is_empty() {
        return Ok(());
    }
    let removing = edge_ids
        .iter()
        .copied()
        .collect::<std::collections::HashSet<_>>();
    // The playlist's entry edges in list order: never its owner edge or any other.
    let listed = db
        .exec(
            QueryBuilder::search()
                .from(playlist_db_id)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .and()
                .keys("entry_id")
                .query(),
        )?
        .ids();
    let ordered = listed
        .into_iter()
        .filter(|edge_id| removing.contains(edge_id))
        .collect::<Vec<_>>();
    anyhow::ensure!(
        ordered.len() == removing.len(),
        "a playlist entry to remove is not in the playlist"
    );
    db.exec_mut(QueryBuilder::remove().ids(ordered).query())?;
    renumber(db, playlist_db_id)
}

/// Numbers the playlist's entries densely from 0 in order, writing only from the first one out
/// of place.
fn renumber(db: &mut impl DbAccess, playlist_db_id: DbId) -> anyhow::Result<()> {
    let entries = stored_entries(db, playlist_db_id)?;
    let Some(first) = entries
        .iter()
        .zip(0u64..)
        .position(|(entry, index)| entry.position != index)
    else {
        return Ok(());
    };
    set_positions(db, entries[first..].iter().map(|t| t.edge_id), first as u64)
}

/// The playlist's entries in order, each `position` its 0-based index, which `move_track` and
/// `insert_tracks` take. An older build left gaps in the stored positions after removals; the
/// index reads past them.
pub fn get_tracks(db: &impl DbAccess, playlist_db_id: DbId) -> anyhow::Result<Vec<PlaylistTrack>> {
    let mut tracks = stored_entries(db, playlist_db_id)?;
    for (index, track) in (0u64..).zip(&mut tracks) {
        track.position = index;
    }
    Ok(tracks)
}

/// The playlist's entries in order, with their stored positions.
fn stored_entries(db: &impl DbAccess, playlist_db_id: DbId) -> anyhow::Result<Vec<PlaylistTrack>> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .from(playlist_db_id)
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(1))
            .and()
            .keys("entry_id")
            .end_where()
            .query(),
    )?;

    let mut tracks = Vec::new();
    for element in &result.elements {
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

/// Return tracks for multiple playlists, keyed by playlist.
///
/// This runs one graph search per playlist; only the caller's downstream track
/// load can be batched.
pub fn get_tracks_many(
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

pub fn move_track(
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
    let insert_at = usize::try_from(new_position)
        .unwrap_or(usize::MAX)
        .min(tracks.len());
    tracks.insert(insert_at, track);

    set_positions(db, tracks.iter().map(|t| t.edge_id), 0)
}

/// Numbers `edge_ids` in order from `first`, in one query.
fn set_positions(
    db: &mut impl DbAccess,
    edge_ids: impl IntoIterator<Item = DbId>,
    first: u64,
) -> anyhow::Result<()> {
    let (ids, values): (Vec<DbId>, Vec<Vec<agdb::DbKeyValue>>) = edge_ids
        .into_iter()
        .zip(first..)
        .map(|(edge_id, position)| (edge_id, vec![("position", position).into()]))
        .unzip();
    if ids.is_empty() {
        return Ok(());
    }
    db.exec_mut(QueryBuilder::insert().values(values).ids(ids).query())?;
    Ok(())
}

pub fn resolve_edge_targets(db: &impl DbAccess, edge_ids: &[DbId]) -> anyhow::Result<Vec<DbId>> {
    if edge_ids.is_empty() {
        return Ok(Vec::new());
    }
    let result = db.exec(QueryBuilder::select().ids(edge_ids).query())?;
    Ok(result
        .elements
        .iter()
        .filter_map(|e| (e.id.0 < 0 && e.to.0 != 0).then_some(e.to))
        .collect())
}

/// Return the playlists holding at least one entry for `track_db_id`.
///
/// Uses one inbound search from the track and needs no root lookup.
pub(crate) fn get_by_track(db: &impl DbAccess, track_db_id: DbId) -> anyhow::Result<Vec<DbId>> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .to(track_db_id)
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(1))
            .and()
            .keys("entry_id")
            .end_where()
            .query(),
    )?;

    let mut playlist_db_ids = Vec::new();
    for element in &result.elements {
        let from_id = element.from;
        if from_id.0 <= 0 || playlist_db_ids.contains(&from_id) {
            continue;
        }
        playlist_db_ids.push(from_id);
    }

    Ok(playlist_db_ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_db::new_test_db;
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
            created_at_ms: Some(1000),
            updated_at_ms: Some(1000),
        };
        let playlist_db_id = create(&mut db, &playlist, user_db_id)?;

        let fetched = get_by_id(&db, playlist_db_id)?.expect("playlist should exist");
        assert_eq!(fetched.name, "My Playlist");
        assert_eq!(fetched.description.as_deref(), Some("A test playlist"));

        Ok(())
    }

    #[test]
    fn get_by_user_reads_only_the_users_own_playlists() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let alice = create_test_user(&mut db)?;
        let bob = create_test_user(&mut db)?;
        let playlist = |name: &str| Playlist {
            db_id: None,
            id: nanoid!(),
            name: name.to_string(),
            description: None,
            is_public: Some(true),
            created_at_ms: None,
            updated_at_ms: None,
        };
        let alices = create(&mut db, &playlist("Alice's"), alice)?;
        create(&mut db, &playlist("Bob's"), bob)?;
        super::super::favorites::add(
            &mut db,
            bob,
            alices,
            super::super::favorites::FavoriteKind::Playlist,
            1,
        )?;

        let names: Vec<String> = get_by_user(&db, alice)?
            .into_iter()
            .map(|playlist| playlist.name)
            .collect();
        assert_eq!(
            names,
            ["Alice's"],
            "Bob's favorite of Alice's playlist must not lead to Bob's playlists"
        );
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
            created_at_ms: None,
            updated_at_ms: None,
        };
        create(&mut db, &p1, user_a)?;

        let p2 = Playlist {
            db_id: None,
            id: nanoid!(),
            name: "User B Playlist".to_string(),
            description: None,
            is_public: None,
            created_at_ms: None,
            updated_at_ms: None,
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
            created_at_ms: None,
            updated_at_ms: None,
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
            created_at_ms: None,
            updated_at_ms: None,
        };
        let playlist_db_id = create(&mut db, &playlist, user_db_id)?;

        add_track(&mut db, playlist_db_id, track_a)?;
        add_track(&mut db, playlist_db_id, track_b)?;

        let tracks = get_tracks(&db, playlist_db_id)?;
        assert_eq!(tracks.len(), 2);
        assert_eq!(tracks[0].position, 0);
        assert_eq!(tracks[1].position, 1);

        let track_ids = resolve_edge_targets(&db, &[tracks[0].edge_id, tracks[1].edge_id])?;
        assert_eq!(track_ids[0], track_a);
        assert_eq!(track_ids[1], track_b);

        Ok(())
    }

    /// Appends and `move_track` must share one position base. `move_track`
    /// treats `new_position` as a 0-based index, so moving an entry to the
    /// position the API already reports for it has to be a no-op.
    #[test]
    fn append_positions_match_move_track_index_base() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db)?;
        let track_a = create_test_track(&mut db, "Track A")?;
        let track_b = create_test_track(&mut db, "Track B")?;
        let track_c = create_test_track(&mut db, "Track C")?;

        let playlist = Playlist {
            db_id: None,
            id: nanoid!(),
            name: "Position Base".to_string(),
            description: None,
            is_public: None,
            created_at_ms: None,
            updated_at_ms: None,
        };
        let playlist_db_id = create(&mut db, &playlist, user_db_id)?;

        let first = add_track(&mut db, playlist_db_id, track_a)?;
        assert_eq!(first.position, 0, "first append into empty playlist");
        let appended = add_tracks(&mut db, playlist_db_id, &[track_b, track_c])?;
        assert_eq!(appended[0].position, 1);
        assert_eq!(appended[1].position, 2);

        let before = get_tracks(&db, playlist_db_id)?;
        let head = &before[0];
        move_track(&mut db, playlist_db_id, head.edge_id, head.position)?;
        let after = get_tracks(&db, playlist_db_id)?;
        assert_eq!(
            after.iter().map(|t| t.entry_id.clone()).collect::<Vec<_>>(),
            before
                .iter()
                .map(|t| t.entry_id.clone())
                .collect::<Vec<_>>(),
            "moving an entry to its reported position must not reorder"
        );

        let empty = create(
            &mut db,
            &Playlist {
                db_id: None,
                id: nanoid!(),
                name: "Bulk Base".to_string(),
                description: None,
                is_public: None,
                created_at_ms: None,
                updated_at_ms: None,
            },
            user_db_id,
        )?;
        let bulk = add_tracks(&mut db, empty, &[track_a, track_b])?;
        assert_eq!(bulk[0].position, 0);
        assert_eq!(bulk[1].position, 1);

        Ok(())
    }

    #[test]
    fn insert_tracks_opens_a_gap_at_the_clamped_position() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db)?;
        let track_a = create_test_track(&mut db, "Track A")?;
        let track_b = create_test_track(&mut db, "Track B")?;
        let track_c = create_test_track(&mut db, "Track C")?;
        let playlist_db_id = create(
            &mut db,
            &Playlist {
                db_id: None,
                id: nanoid!(),
                name: "Inserted".to_string(),
                description: None,
                is_public: None,
                created_at_ms: None,
                updated_at_ms: None,
            },
            user_db_id,
        )?;
        let appended = add_tracks(&mut db, playlist_db_id, &[track_a, track_b, track_a])?;
        // A removal leaves a gap in the stored positions; insertion counts entries, not positions.
        remove_track(&mut db, appended[1].edge_id)?;

        let inserted = insert_tracks(&mut db, playlist_db_id, &[track_c, track_b], 1)?;
        assert_eq!(
            inserted.iter().map(|t| t.position).collect::<Vec<_>>(),
            [1, 2]
        );
        let past_end = insert_tracks(&mut db, playlist_db_id, &[track_c], 99)?;
        assert_eq!(past_end[0].position, 4);

        let tracks = get_tracks(&db, playlist_db_id)?;
        assert_eq!(
            tracks.iter().map(|t| t.position).collect::<Vec<_>>(),
            [0, 1, 2, 3, 4]
        );
        let edge_ids = tracks.iter().map(|t| t.edge_id).collect::<Vec<_>>();
        assert_eq!(
            resolve_edge_targets(&db, &edge_ids)?,
            [track_a, track_c, track_b, track_a, track_c]
        );

        Ok(())
    }

    /// Guards the hand-maintained clear list in `update` against struct drift.
    #[test]
    fn update_clears_every_optional_key() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db)?;
        let playlist_db_id = create(
            &mut db,
            &Playlist {
                db_id: None,
                id: "drift-guard".to_string(),
                name: "Drift Guard".to_string(),
                description: None,
                is_public: None,
                created_at_ms: None,
                updated_at_ms: None,
            },
            user_db_id,
        )?;
        let db_id = Some(super::super::NodeId::from(playlist_db_id));

        update(
            &mut db,
            &Playlist {
                db_id: db_id.clone(),
                id: "drift-guard".to_string(),
                name: "Drift Guard".to_string(),
                description: Some("d".to_string()),
                is_public: Some(true),
                created_at_ms: Some(1),
                updated_at_ms: Some(2),
            },
        )?;

        update(
            &mut db,
            &Playlist {
                db_id,
                id: "drift-guard".to_string(),
                name: "Drift Guard".to_string(),
                description: None,
                is_public: None,
                created_at_ms: None,
                updated_at_ms: None,
            },
        )?;

        let keys = crate::test_db::stored_keys(&db, playlist_db_id)?;
        assert_eq!(
            keys,
            ["db_element_id", "id", "name"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            "only non-Option keys may remain after an all-None update"
        );
        Ok(())
    }

    #[test]
    fn update_in_transaction_rolls_back_clears() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db)?;
        let playlist_db_id = create(
            &mut db,
            &Playlist {
                db_id: None,
                id: "rollback".to_string(),
                name: "Before".to_string(),
                description: Some("keep".to_string()),
                is_public: None,
                created_at_ms: None,
                updated_at_ms: None,
            },
            user_db_id,
        )?;

        let result = db.transaction_mut(|t| -> anyhow::Result<()> {
            update_in_transaction(
                t,
                &Playlist {
                    db_id: Some(super::super::NodeId::from(playlist_db_id)),
                    id: "rollback".to_string(),
                    name: "After".to_string(),
                    description: None,
                    is_public: None,
                    created_at_ms: None,
                    updated_at_ms: None,
                },
            )?;
            anyhow::bail!("force rollback")
        });
        assert!(result.is_err());

        let stored = get_by_id(&db, playlist_db_id)?.expect("playlist remains");
        assert_eq!(stored.name, "Before");
        assert_eq!(stored.description.as_deref(), Some("keep"));
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
            created_at_ms: None,
            updated_at_ms: None,
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
            created_at_ms: None,
            updated_at_ms: None,
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
            created_at_ms: None,
            updated_at_ms: None,
        };
        let playlist_db_id = create(&mut db, &playlist, user_db_id)?;
        delete(&mut db, playlist_db_id)?;

        let result = get_by_id(&db, playlist_db_id)?;
        assert!(result.is_none());

        Ok(())
    }

    #[test]
    fn get_tracks_ignores_edges_without_entries() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db)?;
        let track = create_test_track(&mut db, "Real Entry")?;

        let playlist = Playlist {
            db_id: None,
            id: nanoid!(),
            name: "Entry Filter".to_string(),
            description: None,
            is_public: None,
            created_at_ms: None,
            updated_at_ms: None,
        };
        let playlist_db_id = create(&mut db, &playlist, user_db_id)?;
        add_track(&mut db, playlist_db_id, track)?;

        // Playlists carry outbound edges that are not entries — the owner edge
        // and the display cover profile edge. Only `entry_id` edges are tracks.
        let unrelated = create_test_track(&mut db, "Not An Entry")?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(playlist_db_id)
                .to(unrelated)
                .query(),
        )?;

        let tracks = get_tracks(&db, playlist_db_id)?;
        assert_eq!(tracks.len(), 1);
        let track_ids = resolve_edge_targets(&db, &[tracks[0].edge_id])?;
        assert_eq!(track_ids, [track]);

        Ok(())
    }

    /// A playlist holding one entry per title, in order.
    fn playlist_of(db: &mut DbAny, titles: &[&str]) -> anyhow::Result<(DbId, Vec<DbId>)> {
        let user_db_id = create_test_user(db)?;
        let playlist = Playlist {
            db_id: None,
            id: nanoid!(),
            name: "Positions".to_string(),
            description: None,
            is_public: None,
            created_at_ms: None,
            updated_at_ms: None,
        };
        let playlist_db_id = create(db, &playlist, user_db_id)?;
        let tracks = titles
            .iter()
            .map(|title| create_test_track(db, title))
            .collect::<anyhow::Result<Vec<_>>>()?;
        add_tracks(db, playlist_db_id, &tracks)?;
        Ok((playlist_db_id, tracks))
    }

    /// The tracks in order, and whether the positions read as their indexes.
    fn order(db: &DbAny, playlist_db_id: DbId) -> anyhow::Result<Vec<DbId>> {
        let tracks = get_tracks(db, playlist_db_id)?;
        for (index, track) in (0u64..).zip(&tracks) {
            assert_eq!(track.position, index, "positions read as indexes");
        }
        let edge_ids = tracks.iter().map(|t| t.edge_id).collect::<Vec<_>>();
        resolve_edge_targets(db, &edge_ids)
    }

    fn stored(db: &DbAny, playlist_db_id: DbId) -> anyhow::Result<Vec<u64>> {
        Ok(stored_entries(db, playlist_db_id)?
            .into_iter()
            .map(|t| t.position)
            .collect())
    }

    #[test]
    fn removal_renumbers_so_a_move_takes_the_reported_position() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (playlist_db_id, tracks) = playlist_of(&mut db, &["A", "B", "C", "D"])?;
        let [a, b, c, d] = tracks[..] else {
            unreachable!()
        };
        let entries = get_tracks(&db, playlist_db_id)?;
        remove_tracks(&mut db, playlist_db_id, &[entries[0].edge_id])?;
        assert_eq!(stored(&db, playlist_db_id)?, [0, 1, 2]);

        let c_entry = get_tracks(&db, playlist_db_id)?[1].clone();
        assert_eq!(c_entry.position, 1);
        move_track(&mut db, playlist_db_id, c_entry.edge_id, c_entry.position)?;
        assert_eq!(
            order(&db, playlist_db_id)?,
            [b, c, d],
            "a move to its own place keeps it"
        );

        let appended = add_tracks(&mut db, playlist_db_id, &[a])?;
        assert_eq!(appended[0].position, 3);
        assert_eq!(order(&db, playlist_db_id)?, [b, c, d, a]);
        assert_eq!(stored(&db, playlist_db_id)?, [0, 1, 2, 3]);
        Ok(())
    }

    #[test]
    fn gapped_positions_read_and_take_as_indexes() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (playlist_db_id, tracks) = playlist_of(&mut db, &["A", "B", "C"])?;
        let [a, b, c] = tracks[..] else {
            unreachable!()
        };
        // An older build left gaps after removals.
        let entries = stored_entries(&db, playlist_db_id)?;
        for (entry, position) in entries.iter().zip([1u64, 4, 9]) {
            db.exec_mut(
                QueryBuilder::insert()
                    .values_uniform([("position", position).into()])
                    .ids(entry.edge_id)
                    .query(),
            )?;
        }
        assert_eq!(order(&db, playlist_db_id)?, [a, b, c]);

        let d = create_test_track(&mut db, "D")?;
        let appended = add_tracks(&mut db, playlist_db_id, &[d])?;
        assert_eq!(appended[0].position, 3, "an append reports its index");
        assert_eq!(order(&db, playlist_db_id)?, [a, b, c, d]);

        let c_entry = get_tracks(&db, playlist_db_id)?[2].clone();
        move_track(&mut db, playlist_db_id, c_entry.edge_id, 1)?;
        assert_eq!(order(&db, playlist_db_id)?, [a, c, b, d]);

        let e = create_test_track(&mut db, "E")?;
        let inserted = insert_tracks(&mut db, playlist_db_id, &[e], 2)?;
        assert_eq!(inserted[0].position, 2);
        assert_eq!(order(&db, playlist_db_id)?, [a, c, e, b, d]);
        assert_eq!(stored(&db, playlist_db_id)?, [0, 1, 2, 3, 4]);
        Ok(())
    }

    #[test]
    fn remove_tracks_refuses_edges_that_are_not_entries() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (playlist_db_id, _) = playlist_of(&mut db, &["A"])?;
        let owner_edge = db
            .exec(
                QueryBuilder::search()
                    .from(playlist_db_id)
                    .where_()
                    .edge()
                    .and()
                    .distance(CountComparison::Equal(1))
                    .and()
                    .keys("owner")
                    .query(),
            )?
            .ids();
        let entry = get_tracks(&db, playlist_db_id)?[0].edge_id;

        assert!(remove_tracks(&mut db, playlist_db_id, &[entry, owner_edge[0]]).is_err());
        assert!(
            get_owner(&db, playlist_db_id)?.is_some(),
            "the owner edge stays"
        );
        assert_eq!(
            get_tracks(&db, playlist_db_id)?.len(),
            1,
            "and so does the entry"
        );
        Ok(())
    }

    #[test]
    fn get_by_track_returns_holding_playlists() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db)?;
        let track = create_test_track(&mut db, "Shared")?;
        let untracked = create_test_track(&mut db, "Elsewhere")?;

        let mut playlist_db_ids = Vec::new();
        for name in ["First", "Second"] {
            let playlist = Playlist {
                db_id: None,
                id: nanoid!(),
                name: name.to_string(),
                description: None,
                is_public: None,
                created_at_ms: None,
                updated_at_ms: None,
            };
            let playlist_db_id = create(&mut db, &playlist, user_db_id)?;
            // Twice, to prove duplicate entries do not duplicate the playlist.
            add_track(&mut db, playlist_db_id, track)?;
            add_track(&mut db, playlist_db_id, track)?;
            playlist_db_ids.push(playlist_db_id);
        }

        let mut holders = get_by_track(&db, track)?;
        holders.sort_by_key(|id| id.0);
        playlist_db_ids.sort_by_key(|id| id.0);
        assert_eq!(holders, playlist_db_ids);
        assert!(get_by_track(&db, untracked)?.is_empty());

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
            created_at_ms: None,
            updated_at_ms: None,
        };
        let playlist_db_id = create(&mut db, &playlist, user_db_id)?;

        add_track(&mut db, playlist_db_id, track)?;
        add_track(&mut db, playlist_db_id, track)?;

        let tracks = get_tracks(&db, playlist_db_id)?;
        assert_eq!(tracks.len(), 2);

        let track_ids = resolve_edge_targets(&db, &[tracks[0].edge_id, tracks[1].edge_id])?;
        assert_eq!(track_ids[0], track);
        assert_eq!(track_ids[1], track);

        Ok(())
    }
}
