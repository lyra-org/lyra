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
use anyhow::Result;

use crate::{
    STATE,
    db::{
        self,
        Track,
    },
};

use super::{
    CoverPaths,
    image::COVER_EXTENSIONS,
};

pub(crate) fn resolve_cover_storage_root(configured_root: Option<&Path>) -> Option<PathBuf> {
    configured_root.map(Path::to_path_buf)
}

pub(crate) fn configured_covers_root() -> Option<PathBuf> {
    let config = STATE.config.get();
    resolve_cover_storage_root(config.covers_path.as_deref())
}

pub(crate) fn configured_cover_dir_for_release(
    covers_root: Option<&Path>,
    release_id: DbId,
) -> Option<PathBuf> {
    covers_root.map(|root| root.join(release_id.0.to_string()))
}

pub(crate) fn configured_cover_dir_for_artist(
    covers_root: Option<&Path>,
    artist_id: DbId,
) -> Option<PathBuf> {
    covers_root.map(|root| root.join("artists").join(artist_id.0.to_string()))
}

pub(super) fn cover_path_from_db(db: &DbAny, owner_db_id: DbId) -> Result<Option<PathBuf>> {
    let Some(cover) = db::covers::get(db, owner_db_id)? else {
        return Ok(None);
    };

    let path = PathBuf::from(cover.path);
    if path.is_file() {
        return Ok(Some(path));
    }

    Ok(None)
}

pub(crate) fn find_cover_in_directory(dir: &Path) -> Option<PathBuf> {
    let mut candidates: Vec<(usize, PathBuf)> = Vec::new();
    let entries = std::fs::read_dir(dir).ok()?;

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        if !stem.eq_ignore_ascii_case("cover") {
            continue;
        }

        let Some(ext) = path.extension().and_then(|value| value.to_str()) else {
            continue;
        };
        let ext = ext.to_ascii_lowercase();
        let Some(priority) = COVER_EXTENSIONS.iter().position(|value| *value == ext) else {
            continue;
        };

        candidates.push((priority, path));
    }

    candidates.sort_by(|(a_pri, a_path), (b_pri, b_path)| {
        a_pri
            .cmp(b_pri)
            .then_with(|| a_path.to_string_lossy().cmp(&b_path.to_string_lossy()))
    });
    candidates.into_iter().next().map(|(_, path)| path)
}

pub(crate) fn cover_dirs_for_release(
    db: &DbAny,
    tracks: &[Track],
    library_root: Option<&Path>,
) -> Result<Vec<PathBuf>> {
    let mut seen_tracks = HashSet::new();
    let mut seen_dirs = HashSet::new();
    let mut dirs = Vec::new();

    for track in tracks {
        let Some(track_id) = track.db_id.as_ref() else {
            continue;
        };
        let track_id: DbId = track_id.clone().into();
        if !seen_tracks.insert(track_id) {
            continue;
        }

        let entries = db::entries::get_by_track(db, track_id)?;
        for entry in entries {
            if entry.kind != crate::db::entries::EntryKind::File {
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

            let mut candidate_dirs = Vec::new();
            candidate_dirs.push(track_dir.to_path_buf());

            if let Some(parent_dir) = track_dir.parent() {
                let parent_in_library =
                    library_root.is_none_or(|root| parent_dir.starts_with(root));
                if parent_dir != track_dir && parent_in_library {
                    candidate_dirs.push(parent_dir.to_path_buf());
                }
            }

            for dir in candidate_dirs {
                if seen_dirs.insert(dir.clone()) {
                    dirs.push(dir);
                }
            }
        }
    }

    Ok(dirs)
}

pub(crate) fn resolve_cover_for_release(
    db: &DbAny,
    release_id: DbId,
    tracks: &[Track],
    paths: CoverPaths<'_>,
) -> Result<Option<PathBuf>> {
    if let Some(path) = cover_path_from_db(db, release_id)? {
        return Ok(Some(path));
    }

    if let Some(configured_dir) = configured_cover_dir_for_release(paths.covers_root, release_id)
        && let Some(cover) = find_cover_in_directory(&configured_dir)
    {
        return Ok(Some(cover));
    }

    for dir in cover_dirs_for_release(db, tracks, paths.library_root)? {
        if let Some(cover) = find_cover_in_directory(&dir) {
            return Ok(Some(cover));
        }
    }

    Ok(None)
}

pub(crate) fn resolve_cover_for_release_id(
    db: &DbAny,
    release_id: DbId,
    paths: CoverPaths<'_>,
) -> Result<Option<PathBuf>> {
    if db::releases::get_by_id(db, release_id)?.is_none() {
        return Ok(None);
    }

    let tracks = db::tracks::get_direct(db, release_id)?;
    resolve_cover_for_release(db, release_id, &tracks, paths)
}

pub(crate) fn resolve_cover_for_artist_id(
    db: &DbAny,
    artist_id: DbId,
    paths: CoverPaths<'_>,
) -> Result<Option<PathBuf>> {
    if db::artists::get_by_id(db, artist_id)?.is_none() {
        return Ok(None);
    }

    if let Some(path) = cover_path_from_db(db, artist_id)? {
        return Ok(Some(path));
    }

    if let Some(configured_dir) = configured_cover_dir_for_artist(paths.covers_root, artist_id)
        && let Some(cover) = find_cover_in_directory(&configured_dir)
    {
        return Ok(Some(cover));
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agdb::QueryBuilder;
    use std::path::Path;

    use nanoid::nanoid;

    use crate::db::{
        Entry,
        NodeId,
        Track,
        TrackSource,
        test_db::TestDb,
    };

    fn new_test_db() -> anyhow::Result<DbAny> {
        Ok(TestDb::new()?.into_inner())
    }

    #[test]
    fn cover_dirs_for_release_keeps_library_root_and_skips_parent_outside_root()
    -> anyhow::Result<()> {
        let mut db = new_test_db()?;

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
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
            locked: None,
            created_at: None,
            ctime: None,
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
            kind: crate::db::entries::EntryKind::File,
            file_kind: Some("audio".to_string()),
            name: "song.mp3".to_string(),
            hash: None,
            size: 1,
            mtime: 1,
            ctime: 1,
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
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
            locked: None,
            created_at: None,
            ctime: None,
        }];

        let dirs = cover_dirs_for_release(&db, &tracks, Some(Path::new("/music")))?;
        assert_eq!(dirs, vec![PathBuf::from("/music")]);

        Ok(())
    }
}
