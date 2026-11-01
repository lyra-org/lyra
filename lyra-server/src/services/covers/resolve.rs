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

use crate::services::STATE;
use lyra_db::Track;

use super::image::COVER_EXTENSIONS;

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

/// The directory's cover image, or none at all when the directory doesn't exist.
fn list_cover_in_directory(dir: &Path) -> Result<Option<Option<PathBuf>>> {
    let mut candidates: Vec<(usize, PathBuf)> = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(anyhow::Error::from(err)
                .context(format!("failed to list covers in {}", dir.display())));
        }
    };

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
    Ok(Some(candidates.into_iter().next().map(|(_, path)| path)))
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

/// The release's cover in its library: a `cover.*` image in a folder holding its tracks or the
/// folder above.
pub(crate) fn resolve_local_cover_for_release(
    db: &DbAny,
    tracks: &[Track],
    library_root: Option<&Path>,
) -> Result<Option<PathBuf>> {
    Ok(scan_local_cover_for_release(db, tracks, library_root)?.cover)
}

/// What a look for a release's cover in its library found.
pub(crate) struct LocalCoverScan {
    pub(crate) cover: Option<PathBuf>,
    /// Whether a folder holding the release's tracks could be listed, so a missing cover is
    /// really gone rather than on a library that is unavailable.
    pub(crate) reached: bool,
}

pub(crate) fn scan_local_cover_for_release(
    db: &DbAny,
    tracks: &[Track],
    library_root: Option<&Path>,
) -> Result<LocalCoverScan> {
    let mut reached = false;
    for dir in cover_dirs_for_release(db, tracks, library_root)? {
        let Some(listed) = list_cover_in_directory(&dir)? else {
            continue;
        };
        reached = true;
        if let Some(cover) = listed {
            return Ok(LocalCoverScan {
                cover: Some(cover),
                reached,
            });
        }
    }
    Ok(LocalCoverScan {
        cover: None,
        reached,
    })
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

        let dirs = cover_dirs_for_release(&db, &tracks, Some(Path::new("/music")))?;
        assert_eq!(dirs, vec![PathBuf::from("/music")]);

        Ok(())
    }
}
