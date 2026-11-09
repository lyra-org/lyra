// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashSet,
    path::{
        Path,
        PathBuf,
    },
};

use agdb::{
    DbAny,
    DbId,
};
use anyhow::{
    Context,
    Result,
};
use rayon::iter::{
    IntoParallelIterator,
    ParallelIterator,
};

use crate::services::{
    STATE,
    libraries::MediaRoots,
};
use lyra_db::Track;

use super::{
    folders::{
        AudioFolders,
        FolderClaim,
        FolderDepth,
    },
    image::COVER_EXTENSIONS,
};

pub(crate) fn configured_covers_root() -> PathBuf {
    STATE.config().covers_path.clone()
}

pub(crate) fn configured_cover_dir_for_release(covers_root: &Path, release_id: DbId) -> PathBuf {
    covers_root.join(release_id.0.to_string())
}

pub(crate) fn configured_cover_dir_for_artist(covers_root: &Path, artist_id: DbId) -> PathBuf {
    covers_root.join("artists").join(artist_id.0.to_string())
}

pub(crate) fn configured_cover_dir_for_playlist(covers_root: &Path, playlist_id: DbId) -> PathBuf {
    covers_root
        .join("playlists")
        .join(playlist_id.0.to_string())
}

/// The files of the owner's stored covers, for [`any_cover_file`] once the read is over.
pub(crate) fn stored_cover_files(
    db: &impl lyra_db::DbAccess,
    owner_db_id: DbId,
) -> Result<Vec<PathBuf>> {
    Ok(lyra_db::covers::sources(db, owner_db_id)?
        .into_iter()
        .map(|sourced| PathBuf::from(sourced.cover.path))
        .collect())
}

pub(crate) fn any_cover_file(files: &[PathBuf]) -> bool {
    files.iter().any(|file| file.is_file())
}

/// [`any_cover_file`] off the async threads.
pub(crate) async fn search_cover_files(files: Vec<PathBuf>) -> Result<bool> {
    tokio::task::spawn_blocking(move || any_cover_file(&files))
        .await
        .context("cover file search task panicked")
}

/// Whether the owner has a stored cover from any source whose file is there.
pub(crate) fn has_stored_cover(db: &impl lyra_db::DbAccess, owner_db_id: DbId) -> Result<bool> {
    Ok(lyra_db::covers::sources(db, owner_db_id)?
        .iter()
        .any(|sourced| Path::new(&sourced.cover.path).is_file()))
}

/// Whether automatic scans and provider syncs must leave the owner's covers alone.
pub(crate) fn covers_locked(db: &impl lyra_db::DbAccess, owner_db_id: DbId) -> Result<bool> {
    let locked = match lyra_db::graph::element_kind(db, owner_db_id)?.as_deref() {
        Some("Release") => {
            lyra_db::releases::get_by_id(db, owner_db_id)?.and_then(|release| release.locked)
        }
        Some("Artist") => {
            lyra_db::artists::get_by_id(db, owner_db_id)?.and_then(|artist| artist.locked)
        }
        _ => None,
    };
    Ok(locked.unwrap_or(false))
}

/// Where the file's name ranks among cover images, if it names one: stem `cover`, any case, and
/// one of [`COVER_EXTENSIONS`].
fn cover_file_priority(path: &Path) -> Option<usize> {
    let stem = path.file_stem()?.to_str()?;
    if !stem.eq_ignore_ascii_case("cover") {
        return None;
    }
    let ext = path.extension()?.to_str()?.to_ascii_lowercase();
    COVER_EXTENSIONS.iter().position(|value| *value == ext)
}

/// Whether the file's name is that of a cover image.
pub(crate) fn is_cover_file(path: &Path) -> bool {
    cover_file_priority(path).is_some()
}

/// The directory's cover image `roots` allow; none when the directory doesn't exist.
fn find_cover_in_directory(dir: &Path, roots: &MediaRoots) -> Result<Option<PathBuf>> {
    let mut candidates: Vec<(usize, PathBuf)> = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(anyhow::Error::from(err)
                .context(format!("failed to list covers in {}", dir.display())));
        }
    };

    for entry in entries {
        let path = entry
            .with_context(|| format!("failed to list covers in {}", dir.display()))?
            .path();
        let Some(priority) = cover_file_priority(&path) else {
            continue;
        };
        if roots.admits_file(&path).is_none() {
            continue;
        }

        candidates.push((priority, path));
    }

    candidates.sort_by(|(a_pri, a_path), (b_pri, b_path)| {
        a_pri
            .cmp(b_pri)
            .then_with(|| a_path.to_string_lossy().cmp(&b_path.to_string_lossy()))
    });
    Ok(candidates.into_iter().next().map(|(_, path)| path))
}

/// The folders around the release's tracks, sorted out by who the audio in them belongs to:
/// a track's own folder counts when all the audio directly in it is the release's, and the folder
/// above when all the audio beneath it is, so a folder of several releases gives none of them
/// its image.
pub(crate) fn cover_dirs_for_release(
    db: &DbAny,
    folders: &AudioFolders,
    release_id: DbId,
    tracks: &[Track],
    library_root: Option<&Path>,
) -> Result<Vec<PathBuf>> {
    let mut seen_tracks = HashSet::new();
    let mut judged = HashSet::new();
    let mut owned = HashSet::new();
    let mut dirs = Vec::new();

    for track in tracks {
        let Some(track_id) = track.db_id.as_ref() else {
            continue;
        };
        let track_id: DbId = track_id.clone().into();
        if !seen_tracks.insert(track_id) {
            continue;
        }

        let entries = lyra_db::entries::get_by_track(db, track_id)?;
        for entry in entries {
            if entry.kind != lyra_db::entries::EntryKind::File {
                continue;
            }

            let Some(track_dir) = entry.full_path.parent() else {
                continue;
            };

            if let Some(root) = library_root
                && !track_dir.starts_with(root)
            {
                continue;
            }

            let mut candidate_dirs = vec![(track_dir, FolderDepth::Direct)];

            if let Some(parent_dir) = track_dir.parent() {
                let parent_in_library =
                    library_root.is_none_or(|root| parent_dir.starts_with(root));
                if parent_dir != track_dir && parent_in_library {
                    candidate_dirs.push((parent_dir, FolderDepth::Recursive));
                }
            }

            for (dir, depth) in candidate_dirs {
                if judged.insert((dir.to_path_buf(), depth))
                    && folders.claim(db, dir, release_id, depth)? == FolderClaim::Sole
                    && owned.insert(dir.to_path_buf())
                {
                    dirs.push(dir.to_path_buf());
                }
            }
        }
    }

    Ok(dirs)
}

/// The release's cover in its library: a `cover.*` image in the first of `dirs` holding one, the
/// folders [`cover_dirs_for_release`] found to be the release's.
pub(crate) fn find_local_cover(dirs: &[PathBuf], roots: &MediaRoots) -> Result<Option<PathBuf>> {
    for dir in dirs {
        if let Some(cover) = find_cover_in_directory(dir, roots)? {
            return Ok(Some(cover));
        }
    }
    Ok(None)
}

/// [`find_local_cover`] off the async threads.
pub(crate) async fn search_local_cover(
    dirs: Vec<PathBuf>,
    roots: MediaRoots,
) -> Result<Option<PathBuf>> {
    tokio::task::spawn_blocking(move || find_local_cover(&dirs, &roots))
        .await
        .context("local cover search task panicked")?
}

/// The local cover in each search's directories, or why looking failed, off the async threads.
pub(crate) async fn search_local_covers<T: Send + 'static>(
    searches: Vec<(T, Vec<PathBuf>)>,
    roots: MediaRoots,
) -> Result<Vec<(T, Result<Option<PathBuf>>)>> {
    tokio::task::spawn_blocking(move || {
        searches
            .into_par_iter()
            .map(|(owner, dirs)| {
                let found = find_local_cover(&dirs, &roots);
                (owner, found)
            })
            .collect()
    })
    .await
    .context("local cover search task panicked")
}

#[cfg(test)]
mod tests {
    use super::*;
    use agdb::QueryBuilder;
    use std::path::Path;

    use nanoid::nanoid;

    use lyra_db::{
        Entry,
        NodeId,
        Track,
        TrackSource,
        test_db::TestDb,
    };

    #[test]
    fn a_cover_link_to_something_else_is_passed_over() -> anyhow::Result<()> {
        let (library, outside) = (tempfile::tempdir()?, tempfile::tempdir()?);
        let config = outside.path().join("config.json");
        std::fs::write(&config, "secret")?;
        std::os::unix::fs::symlink(&config, library.path().join("cover.jpg"))?;
        let mut db = TestDb::initialized()?.into_inner();
        lyra_db::test_db::insert_library(
            &mut db,
            "Library",
            library.path().to_str().expect("UTF-8 temp path"),
        )?;
        let dirs = [library.path().to_path_buf()];
        let roots = || Ok::<_, anyhow::Error>(MediaRoots::for_principal(&db, None)?.for_covers());

        assert_eq!(find_local_cover(&dirs, &roots()?)?, None);

        let png = library.path().join("cover.png");
        std::fs::write(&png, "image")?;
        assert_eq!(find_local_cover(&dirs, &roots()?)?, Some(png));
        Ok(())
    }

    #[test]
    fn cover_dirs_for_release_keeps_library_root_and_skips_parent_outside_root()
    -> anyhow::Result<()> {
        let mut db = TestDb::initialized()?.into_inner();
        let library = lyra_db::test_db::insert_library(&mut db, "Library", "/music")?;
        let release_id = lyra_db::fixtures::insert_release(&mut db, "Release")?;
        lyra_db::fixtures::connect(&mut db, library, release_id)?;

        let track_node = Track {
            db_id: None,
            id: nanoid!(),
            track_title: "song".to_string(),
            sort_title: None,
            year: None,
            disc: None,
            disc_total: None,
            track: None,
            track_total: None,
            duration_ms: None,
            track_gain_db: None,
            album_gain_db: None,
            locked: None,
            created_at_ms: None,
            file_created_at_ms: None,
        };
        let track_id = db
            .exec_mut(QueryBuilder::insert().element(&track_node).query())?
            .ids()
            .first()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("track insert returned no id"))?;

        let entry = Entry {
            db_id: None,
            id: nanoid!(),
            full_path: PathBuf::from("/music/song.mp3"),
            kind: lyra_db::entries::EntryKind::File,
            file_kind: Some("audio".to_string()),
            name: "song.mp3".to_string(),
            hash: None,
            size: 1,
            file_modified_at_ms: 1,
            file_created_at_ms: 1,
            synced_at_ms: None,
            missing_at_ms: None,
        };
        let entry_id = db
            .exec_mut(QueryBuilder::insert().element(&entry).query())?
            .ids()
            .first()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("entry insert returned no id"))?;
        let source = TrackSource {
            db_id: None,
            id: nanoid!(),
            source_kind: "embedded_tags".to_string(),
            source_key: format!("entry:{}:embedded", entry_id.0),
            identity: format!("entry:{}:embedded", entry_id.0),
            is_primary: true,
            start_ms: None,
            end_ms: None,
            container: None,
            codec: None,
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
        };
        let source_id = db
            .exec_mut(QueryBuilder::insert().element(&source).query())?
            .ids()
            .first()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("source insert returned no id"))?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(track_id)
                .to(source_id)
                .query(),
        )?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(source_id)
                .to(entry_id)
                .query(),
        )?;

        lyra_db::fixtures::connect(&mut db, release_id, track_id)?;
        lyra_db::fixtures::connect(&mut db, library, entry_id)?;

        let tracks = vec![Track {
            db_id: Some(NodeId::from(track_id)),
            id: nanoid!(),
            track_title: "song".to_string(),
            sort_title: None,
            year: None,
            disc: None,
            disc_total: None,
            track: None,
            track_total: None,
            duration_ms: None,
            track_gain_db: None,
            album_gain_db: None,
            locked: None,
            created_at_ms: None,
            file_created_at_ms: None,
        }];

        let folders = AudioFolders::of_library(&db, library)?;
        let dirs = cover_dirs_for_release(
            &db,
            &folders,
            release_id,
            &tracks,
            Some(Path::new("/music")),
        )?;
        assert_eq!(dirs, vec![PathBuf::from("/music")]);

        Ok(())
    }
}
