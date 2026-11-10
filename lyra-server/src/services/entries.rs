// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};

use agdb::{
    DbAny,
    DbId,
};

use crate::services::catalog::{
    CatalogError,
    Viewer,
};
use lyra_db::{
    Artist,
    Entry,
    Release,
    Track,
    entries::EntryKind,
};

#[derive(Clone, Copy)]
pub(crate) struct EntryIncludes {
    pub(crate) tracks: bool,
    pub(crate) releases: bool,
    pub(crate) artists: bool,
}

pub(crate) struct EntryDetails {
    pub(crate) entry: Entry,
    pub(crate) tracks: Option<Vec<Track>>,
    pub(crate) releases: Option<Vec<Release>>,
    pub(crate) artists: Option<Vec<Artist>>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum EntryServiceError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

fn collect_releases(db: &DbAny, tracks: &[Track]) -> anyhow::Result<Vec<Release>> {
    let mut releases = Vec::new();
    let mut seen = HashSet::new();

    for track in tracks {
        let Some(track_db_id) = track.db_id.clone() else {
            continue;
        };
        let track_releases = lyra_db::releases::get_by_track(db, DbId::from(track_db_id))?;
        for release in track_releases {
            let Some(release_db_id) = release.db_id.clone() else {
                releases.push(release);
                continue;
            };
            if seen.insert(DbId::from(release_db_id)) {
                releases.push(release);
            }
        }
    }

    Ok(releases)
}

fn collect_artists(db: &DbAny, releases: &[Release]) -> anyhow::Result<Vec<Artist>> {
    let mut artists = Vec::new();
    let mut seen = HashSet::new();

    for release in releases {
        let Some(release_db_id) = release.db_id.clone() else {
            continue;
        };
        let release_artists = lyra_db::artists::get(db, DbId::from(release_db_id))?;
        for artist in release_artists {
            let Some(artist_db_id) = artist.db_id.clone() else {
                artists.push(artist);
                continue;
            };
            if seen.insert(DbId::from(artist_db_id)) {
                artists.push(artist);
            }
        }
    }

    Ok(artists)
}

fn build_entry_details(
    db: &DbAny,
    entry: Entry,
    include: EntryIncludes,
) -> anyhow::Result<EntryDetails> {
    let entry_db_id = entry
        .db_id
        .ok_or_else(|| anyhow::anyhow!("entry missing db id"))?;
    let need_tracks = include.tracks || include.releases || include.artists;
    let tracks = if need_tracks {
        lyra_db::tracks::get_by_entry(db, entry_db_id)?
    } else {
        Vec::new()
    };
    let releases = if include.releases || include.artists {
        collect_releases(db, &tracks)?
    } else {
        Vec::new()
    };
    let artists = if include.artists {
        Some(collect_artists(db, &releases)?)
    } else {
        None
    };

    Ok(EntryDetails {
        entry,
        tracks: include.tracks.then_some(tracks),
        releases: include.releases.then_some(releases),
        artists,
    })
}

/// A stored entry and the library or directory entry holding it.
#[derive(Clone, Debug)]
pub(crate) struct PlacedEntry {
    pub(crate) entry: Entry,
    pub(crate) parent_id: DbId,
}

impl PlacedEntry {
    fn new(entry: Entry, parent_id: DbId) -> Self {
        Self { entry, parent_id }
    }

    pub(crate) fn locate(db: &impl lyra_db::DbAccess, entry: Entry) -> anyhow::Result<Self> {
        let entry_db_id = entry
            .db_id
            .ok_or_else(|| anyhow::anyhow!("entry missing db id"))?;
        let parent_id = lyra_db::entries::parent_id(db, entry_db_id)?
            .ok_or_else(|| anyhow::anyhow!("entry {} has no parent", entry.id))?;
        Ok(Self::new(entry, parent_id))
    }
}

/// The entry `entry_db_id` names, if it exists and the viewer can see it.
pub(crate) fn visible_entry(
    db: &DbAny,
    viewer: &Viewer,
    entry_db_id: DbId,
) -> anyhow::Result<Option<Entry>> {
    let Some(entry) = lyra_db::entries::get_by_id(db, entry_db_id)? else {
        return Ok(None);
    };
    let libraries = viewer.visible_libraries(db)?;
    Ok(sees_held(db, libraries.as_deref(), entry_db_id)?.then_some(entry))
}

/// Whether a viewer limited to `libraries`, or unlimited when `None`, sees the entry
/// `entry_db_id`. An entry is seen through the library or directory holding it, never through
/// the tracks sourced from it.
fn sees_held(db: &DbAny, libraries: Option<&[DbId]>, entry_db_id: DbId) -> anyhow::Result<bool> {
    if libraries.is_none() {
        return Ok(true);
    }
    match lyra_db::entries::parent_id(db, entry_db_id)? {
        Some(parent) => sees_children(db, libraries, parent),
        None => Ok(false),
    }
}

/// Whether a viewer limited to `libraries` sees what `parent`, a library or a directory entry,
/// holds.
fn sees_children(db: &DbAny, libraries: Option<&[DbId]>, parent: DbId) -> anyhow::Result<bool> {
    if lyra_db::libraries::get_by_id(db, parent)?.is_some() {
        return Ok(libraries.is_none_or(|libraries| libraries.contains(&parent)));
    }
    if !lyra_db::entries::get_by_id(db, parent)?.is_some_and(|entry| entry.kind == EntryKind::Dir) {
        return Ok(false);
    }
    sees_held(db, libraries, parent)
}

/// The entries among `ids` that the viewer can see, in order. Siblings share one check of what
/// holds them.
pub(crate) fn visible_ids(db: &DbAny, viewer: &Viewer, ids: &[DbId]) -> anyhow::Result<Vec<DbId>> {
    let libraries = viewer.visible_libraries(db)?;
    let mut parents: HashMap<DbId, bool> = HashMap::new();
    let mut visible = Vec::new();
    for &id in ids {
        if lyra_db::graph::element_kind(db, id)?.as_deref() != Some("Entry") {
            continue;
        }
        if libraries.is_some() {
            let Some(parent) = lyra_db::entries::parent_id(db, id)? else {
                continue;
            };
            let seen = match parents.get(&parent) {
                Some(&seen) => seen,
                None => {
                    let seen = sees_children(db, libraries.as_deref(), parent)?;
                    parents.insert(parent, seen);
                    seen
                }
            };
            if !seen {
                continue;
            }
        }
        visible.push(id);
    }
    Ok(visible)
}

/// `entries` without those the viewer can't see, in order.
pub(crate) fn visible(
    db: &DbAny,
    viewer: &Viewer,
    entries: Vec<Entry>,
) -> anyhow::Result<Vec<Entry>> {
    if viewer.sees_everything() {
        return Ok(entries);
    }
    let ids = entries
        .iter()
        .filter_map(|entry| entry.db_id)
        .collect::<Vec<_>>();
    let seen = visible_ids(db, viewer, &ids)?
        .into_iter()
        .collect::<HashSet<_>>();
    Ok(entries
        .into_iter()
        .filter(|entry| entry.db_id.is_some_and(|id| seen.contains(&id)))
        .collect())
}

/// The entries with these ids that the viewer can see, keyed by id.
pub(crate) fn get(
    db: &DbAny,
    viewer: &Viewer,
    ids: &[DbId],
) -> Result<HashMap<DbId, PlacedEntry>, CatalogError> {
    let mut entries = HashMap::new();
    for &id in ids {
        if let Some(entry) = visible_entry(db, viewer, id)? {
            entries.insert(id, PlacedEntry::locate(db, entry)?);
        }
    }
    Ok(entries)
}

/// The entries directly under each visible library or directory entry, by name.
pub(crate) fn by_parent(
    db: &DbAny,
    viewer: &Viewer,
    parents: &[DbId],
) -> Result<HashMap<DbId, Vec<PlacedEntry>>, CatalogError> {
    let libraries = viewer.visible_libraries(db)?;
    let mut children = HashMap::new();
    for &parent in parents {
        if !sees_children(db, libraries.as_deref(), parent)? {
            continue;
        }
        let mut entries = lyra_db::entries::children(db, parent)?;
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        children.insert(
            parent,
            entries
                .into_iter()
                .map(|entry| PlacedEntry::new(entry, parent))
                .collect(),
        );
    }
    Ok(children)
}

/// The visible entries holding each visible track's audio, by name.
pub(crate) fn by_track(
    db: &DbAny,
    viewer: &Viewer,
    tracks: &[DbId],
) -> Result<HashMap<DbId, Vec<PlacedEntry>>, CatalogError> {
    let libraries = viewer.visible_libraries(db)?;
    let mut entries = HashMap::new();
    for &track in tracks {
        if lyra_db::tracks::get_by_id(db, track)?.is_none() || !viewer.sees_entity(db, track)? {
            continue;
        }
        let mut placed = Vec::new();
        for entry in lyra_db::entries::get_by_track(db, track)? {
            let entry_db_id = entry
                .db_id
                .ok_or_else(|| anyhow::anyhow!("entry missing db id"))?;
            if sees_held(db, libraries.as_deref(), entry_db_id)? {
                placed.push(PlacedEntry::locate(db, entry)?);
            }
        }
        placed.sort_by(|a, b| {
            a.entry
                .name
                .cmp(&b.entry.name)
                .then_with(|| a.entry.id.cmp(&b.entry.id))
        });
        entries.insert(track, placed);
    }
    Ok(entries)
}

/// Every entry in the libraries the viewer can see.
fn all_visible(db: &DbAny, viewer: &Viewer) -> anyhow::Result<Vec<Entry>> {
    let libraries = match viewer.visible_libraries(db)? {
        Some(libraries) => libraries,
        None => lyra_db::libraries::get(db)?
            .into_iter()
            .filter_map(|library| library.db_id)
            .collect(),
    };
    let mut entries = Vec::new();
    for library in libraries {
        entries.extend(lyra_db::entries::load_existing(db, library)?);
    }
    Ok(entries)
}

pub(crate) fn list_details(
    db: &DbAny,
    viewer: &Viewer,
    include: EntryIncludes,
) -> Result<Vec<EntryDetails>, EntryServiceError> {
    let entries = all_visible(db, viewer)?;
    let mut details = Vec::with_capacity(entries.len());
    for entry in entries {
        details.push(build_entry_details(db, entry, include)?);
    }
    Ok(details)
}

pub(crate) fn get_details(
    db: &DbAny,
    viewer: &Viewer,
    id: &str,
    include: EntryIncludes,
) -> Result<EntryDetails, EntryServiceError> {
    let not_found = || EntryServiceError::NotFound(format!("Entry not found: {id}"));
    let entry_db_id = lyra_db::lookup::find_node_id_by_id(db, id)?.ok_or_else(not_found)?;
    let entry = visible_entry(db, viewer, entry_db_id)?.ok_or_else(not_found)?;

    Ok(build_entry_details(db, entry, include)?)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        path::PathBuf,
    };

    use agdb::QueryBuilder;
    use lyra_db::nanoid;

    use super::*;
    use crate::services::auth::Principal;
    use lyra_db::{
        entries::EntryKind,
        fixtures::{
            connect,
            insert_track,
            test_user,
        },
        test_db::{
            insert_library,
            new_test_db,
        },
    };

    fn insert_entry(
        db: &mut DbAny,
        parent: DbId,
        name: &str,
        kind: EntryKind,
    ) -> anyhow::Result<DbId> {
        let entry = Entry {
            db_id: None,
            id: nanoid(),
            full_path: PathBuf::from(format!("/music/{name}")),
            file_kind: (kind == EntryKind::File).then(|| "audio".to_string()),
            kind,
            name: name.to_string(),
            hash: None,
            size: 1,
            file_modified_at_ms: 1,
            file_created_at_ms: 1,
            synced_at_ms: None,
            missing_at_ms: None,
        };
        let entry_db_id = db
            .exec_mut(QueryBuilder::insert().element(&entry).query())?
            .ids()[0];
        connect(db, parent, entry_db_id)?;
        Ok(entry_db_id)
    }

    fn names(entries: &[Entry]) -> HashSet<String> {
        entries.iter().map(|entry| entry.name.clone()).collect()
    }

    #[test]
    fn a_shared_track_shows_no_listing_the_entries_of_another_library() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = lyra_db::users::create(&mut db, &test_user("listener")?)?;
        let shown_db_id = insert_library(&mut db, "Shown", "/music/shown")?;
        let hidden_db_id = insert_library(&mut db, "Hidden", "/music/hidden")?;
        let shown_public_id = lyra_db::libraries::get_by_id(&db, shown_db_id)?.unwrap().id;
        insert_entry(&mut db, shown_db_id, "shown.flac", EntryKind::File)?;
        let hidden_entry = insert_entry(&mut db, hidden_db_id, "hidden.flac", EntryKind::File)?;
        let shared_track = insert_track(&mut db, "Shared")?;
        connect(&mut db, shown_db_id, shared_track)?;
        connect(&mut db, hidden_db_id, shared_track)?;
        connect(&mut db, shared_track, hidden_entry)?;

        let listener = Viewer::user(
            &db,
            Principal::for_user(&db, user_db_id, vec![], HashSet::from([shown_public_id])),
        )?;
        let includes = EntryIncludes {
            tracks: false,
            releases: false,
            artists: false,
        };
        let listed = list_details(&db, &listener, includes)?
            .into_iter()
            .map(|details| details.entry)
            .collect::<Vec<_>>();
        assert_eq!(names(&listed), HashSet::from(["shown.flac".to_string()]));
        Ok(())
    }

    #[test]
    fn visible_entries_follow_library_access() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = lyra_db::users::create(&mut db, &test_user("listener")?)?;
        let shown_db_id = insert_library(&mut db, "Shown", "/music/shown")?;
        let hidden_db_id = insert_library(&mut db, "Hidden", "/music/hidden")?;
        let shown_public_id = lyra_db::libraries::get_by_id(&db, shown_db_id)?.unwrap().id;
        let shown_entry = insert_entry(&mut db, shown_db_id, "shown.flac", EntryKind::File)?;
        let hidden_entry = insert_entry(&mut db, hidden_db_id, "hidden.flac", EntryKind::File)?;
        let hidden_track = insert_track(&mut db, "Hidden")?;
        connect(&mut db, hidden_db_id, hidden_track)?;
        connect(&mut db, hidden_track, hidden_entry)?;

        let listener = Viewer::user(
            &db,
            Principal::for_user(&db, user_db_id, vec![], HashSet::from([shown_public_id])),
        )?;
        let admin = Viewer::user(
            &db,
            Principal::for_user(
                &db,
                user_db_id,
                vec![lyra_db::Permission::Admin],
                HashSet::new(),
            ),
        )?;

        assert_eq!(
            names(&all_visible(&db, &listener)?),
            HashSet::from(["shown.flac".into()])
        );
        let shown = by_parent(&db, &listener, &[shown_db_id, hidden_db_id])?;
        assert_eq!(shown.len(), 1);
        assert_eq!(shown[&shown_db_id][0].entry.name, "shown.flac");
        assert_eq!(shown[&shown_db_id][0].parent_id, shown_db_id);
        let found = get(&db, &listener, &[shown_entry, hidden_entry])?;
        assert_eq!(found.len(), 1);
        assert_eq!(found[&shown_entry].parent_id, shown_db_id);
        assert!(by_track(&db, &listener, &[hidden_track])?.is_empty());
        let hidden_public_id = lyra_db::entries::get_by_id(&db, hidden_entry)?.unwrap().id;
        let include = EntryIncludes {
            tracks: false,
            releases: false,
            artists: false,
        };
        assert!(matches!(
            get_details(&db, &listener, &hidden_public_id, include),
            Err(EntryServiceError::NotFound(_))
        ));

        for viewer in [&admin, &Viewer::System] {
            assert_eq!(
                names(&all_visible(&db, viewer)?),
                HashSet::from(["shown.flac".into(), "hidden.flac".into()])
            );
            let held = by_track(&db, viewer, &[hidden_track])?;
            assert_eq!(held[&hidden_track].len(), 1);
            assert_eq!(held[&hidden_track][0].parent_id, hidden_db_id);
        }
        Ok(())
    }

    #[test]
    fn entries_are_seen_through_what_holds_them() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = lyra_db::users::create(&mut db, &test_user("listener")?)?;
        let shown_db_id = insert_library(&mut db, "Shown", "/music/shown")?;
        let hidden_db_id = insert_library(&mut db, "Hidden", "/music/hidden")?;
        let shown_public_id = lyra_db::libraries::get_by_id(&db, shown_db_id)?.unwrap().id;
        let album = insert_entry(&mut db, shown_db_id, "Album", EntryKind::Dir)?;
        let first = insert_entry(&mut db, album, "01.flac", EntryKind::File)?;
        let second = insert_entry(&mut db, album, "02.flac", EntryKind::File)?;
        let hidden_file = insert_entry(&mut db, hidden_db_id, "hidden.flac", EntryKind::File)?;
        // The shown library holds a track sourced from the hidden library's file.
        let track = insert_track(&mut db, "Shared")?;
        connect(&mut db, shown_db_id, track)?;
        connect(&mut db, track, hidden_file)?;

        let listener = Viewer::user(
            &db,
            Principal::for_user(&db, user_db_id, vec![], HashSet::from([shown_public_id])),
        )?;

        assert_eq!(
            visible_ids(&db, &listener, &[first, hidden_file, second, album, track])?,
            vec![first, second, album]
        );
        assert!(listener.sees(&db, track)?);
        assert!(!listener.sees(&db, hidden_file)?);
        assert!(visible_entry(&db, &listener, hidden_file)?.is_none());
        assert!(by_track(&db, &listener, &[track])?[&track].is_empty());
        assert_eq!(
            visible_ids(&db, &Viewer::System, &[first, hidden_file])?,
            vec![first, hidden_file]
        );
        Ok(())
    }
}
