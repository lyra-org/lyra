// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashSet;

use agdb::{
    DbAny,
    DbId,
};

use crate::{
    db::{
        self,
        Artist,
        Entry,
        Release,
        Track,
    },
    services::catalog::Viewer,
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
        let track_releases = db::releases::get_by_track(db, DbId::from(track_db_id))?;
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
        let release_artists = db::artists::get(db, DbId::from(release_db_id))?;
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
        db::tracks::get_by_entry(db, entry_db_id)?
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

/// The entries `viewer` can see: those of every library, or those under an entry or library, or
/// linked to a track. Anything else, or anything the viewer can't see, is not found.
pub(crate) fn visible(
    db: &DbAny,
    viewer: &Viewer,
    from: Option<DbId>,
) -> Result<Vec<Entry>, EntryServiceError> {
    let not_found = || EntryServiceError::NotFound(format!("no visible entries under {from:?}"));
    let Some(from) = from else {
        let Some(libraries) = viewer.visible_libraries(db)? else {
            return Ok(db::entries::get(db, "libraries")?);
        };
        let mut entries = Vec::new();
        for library in libraries {
            entries.extend(db::entries::get(db, library)?);
        }
        return Ok(entries);
    };

    if db::tracks::get_by_id(db, from)?.is_some() {
        if !viewer.sees_entity(db, from)? {
            return Err(not_found());
        }
        let mut entries = Vec::new();
        for entry in db::entries::get_by_track(db, from)? {
            let entry_db_id = entry
                .db_id
                .ok_or_else(|| anyhow::anyhow!("entry missing db id"))?;
            if viewer.sees_entity(db, entry_db_id)? {
                entries.push(entry);
            }
        }
        return Ok(entries);
    }

    let visible = if db::libraries::get_by_id(db, from)?.is_some() {
        viewer
            .visible_libraries(db)?
            .is_none_or(|libraries| libraries.contains(&from))
    } else {
        db::entries::get_by_id(db, from)?.is_some() && viewer.sees_entity(db, from)?
    };
    if !visible {
        return Err(not_found());
    }
    Ok(db::entries::get(db, from)?)
}

pub(crate) fn list_details(
    db: &DbAny,
    viewer: &Viewer,
    include: EntryIncludes,
) -> Result<Vec<EntryDetails>, EntryServiceError> {
    let entries = visible(db, viewer, None)?;
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
    let entry_db_id = db::lookup::find_node_id_by_id(db, id)?.ok_or_else(not_found)?;
    let entry = db::entries::get_by_id(db, entry_db_id)?.ok_or_else(not_found)?;
    if !viewer.sees_entity(db, entry_db_id)? {
        return Err(not_found());
    }

    Ok(build_entry_details(db, entry, include)?)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        path::PathBuf,
    };

    use agdb::QueryBuilder;
    use nanoid::nanoid;

    use super::*;
    use crate::{
        db::{
            entries::EntryKind,
            libraries::LibraryInsert,
            test_db::{
                connect,
                insert_track,
                new_test_db,
                test_user,
            },
        },
        services::auth::Principal,
    };

    fn create_library(db: &mut DbAny, name: &str) -> anyhow::Result<db::libraries::Library> {
        let path = PathBuf::from(format!("/tmp/lyra-entries-test-{}", nanoid!()));
        let request = LibraryInsert {
            id: nanoid!(),
            name: name.to_string(),
            path: path.clone(),
            path_key: db::libraries::path_key_for(&path),
            language: None,
            country: None,
        };
        db.transaction_mut(|t| -> anyhow::Result<db::libraries::Library> {
            Ok(db::libraries::create_system(t, request)?)
        })
    }

    fn insert_entry(db: &mut DbAny, parent: DbId, name: &str) -> anyhow::Result<DbId> {
        let entry = Entry {
            db_id: None,
            id: nanoid!(),
            full_path: PathBuf::from(format!("/music/{name}")),
            kind: EntryKind::File,
            file_kind: Some("audio".to_string()),
            name: name.to_string(),
            hash: None,
            size: 1,
            mtime: 1,
            ctime: 1,
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
    fn visible_entries_follow_library_access() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = db::users::create(&mut db, &test_user("listener")?)?;
        let shown = create_library(&mut db, "Shown")?;
        let hidden = create_library(&mut db, "Hidden")?;
        let (shown_db_id, hidden_db_id) = (shown.db_id.unwrap(), hidden.db_id.unwrap());
        let shown_entry = insert_entry(&mut db, shown_db_id, "shown.flac")?;
        let hidden_entry = insert_entry(&mut db, hidden_db_id, "hidden.flac")?;
        let hidden_track = insert_track(&mut db, "Hidden")?;
        connect(&mut db, hidden_db_id, hidden_track)?;
        connect(&mut db, hidden_track, hidden_entry)?;

        let listener = Viewer::user(
            &db,
            Principal::for_user(&db, user_db_id, vec![], HashSet::from([shown.id.clone()])),
        )?;
        let admin = Viewer::user(
            &db,
            Principal::for_user(&db, user_db_id, vec![db::Permission::Admin], HashSet::new()),
        )?;

        assert_eq!(
            names(&visible(&db, &listener, None)?),
            HashSet::from(["shown.flac".into()])
        );
        assert_eq!(
            names(&visible(&db, &listener, Some(shown_db_id))?),
            HashSet::from(["shown.flac".into()])
        );
        assert_eq!(names(&visible(&db, &listener, Some(shown_entry))?).len(), 1);
        for hidden_node in [hidden_db_id, hidden_entry, hidden_track, user_db_id] {
            assert!(matches!(
                visible(&db, &listener, Some(hidden_node)),
                Err(EntryServiceError::NotFound(_))
            ));
        }
        let hidden_public_id = db::entries::get_by_id(&db, hidden_entry)?.unwrap().id;
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
                names(&visible(&db, viewer, None)?),
                HashSet::from(["shown.flac".into(), "hidden.flac".into()])
            );
            assert_eq!(names(&visible(&db, viewer, Some(hidden_track))?).len(), 1);
        }
        Ok(())
    }
}
