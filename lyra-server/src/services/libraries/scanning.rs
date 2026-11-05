// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        HashMap,
        HashSet,
    },
    fs::File,
    path::{
        Path,
        PathBuf,
    },
    time,
};

use blake3::Hasher;
use multimap::MultiMap;
use nanoid::nanoid;
use rayon::iter::{
    IntoParallelIterator,
    ParallelIterator,
};
use walkdir::WalkDir;

use crate::services::metadata::ingestion::{
    SourceDirGroup,
    group_by_source_dir,
};
use lyra_db::{
    Entry,
    Library,
    entries::{
        EntryKind,
        classify_file_kind,
    },
    is_supported_extension,
};

#[derive(Debug)]
pub(crate) struct EntryScanPlan {
    pub(crate) groups: Vec<SourceDirGroup>,
    pub(crate) observed_paths: HashSet<PathBuf>,
    /// The paths the scan couldn't read, with why. What lies under them is no evidence either way.
    pub(crate) unlistable: Vec<(PathBuf, String)>,
}

impl EntryScanPlan {
    pub(crate) fn protected_paths(&self) -> Vec<PathBuf> {
        self.unlistable
            .iter()
            .map(|(path, _)| path.clone())
            .collect()
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ScanError {
    #[error("library root has no supported files")]
    EmptyRoot,
    #[error("library root can't be listed: {0}")]
    UnlistableRoot(String),
}

pub(crate) struct ScanResult {
    pub(crate) entries: Vec<Entry>,
    pub(crate) unlistable: Vec<(PathBuf, String)>,
}

pub(crate) fn scan_fs(root: &Path) -> anyhow::Result<ScanResult> {
    let mut entries = Vec::new();
    let mut unlistable = Vec::new();

    for entry in WalkDir::new(root).into_iter() {
        let de = match entry {
            Ok(de) => de,
            Err(err) => {
                let message = err.to_string();
                match err.path() {
                    Some(path) if path != root => unlistable.push((path.to_path_buf(), message)),
                    _ => return Err(ScanError::UnlistableRoot(message).into()),
                }
                continue;
            }
        };

        let md = match de.metadata() {
            Ok(md) => md,
            Err(err) => {
                if de.path() == root {
                    return Err(ScanError::UnlistableRoot(err.to_string()).into());
                }
                unlistable.push((de.path().to_path_buf(), err.to_string()));
                continue;
            }
        };

        let is_file = md.is_file();
        if is_file && !is_supported_extension(de.path()) {
            continue;
        }
        let file_kind = if is_file {
            classify_file_kind(de.path())
        } else {
            None
        };

        let size = if is_file { md.len() } else { 0 };
        let file_modified_at_ms = match md
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(time::UNIX_EPOCH).ok())
        {
            Some(modified) => modified.as_millis() as u64,
            None => {
                unlistable.push((
                    de.path().to_path_buf(),
                    "failed to read the modification time".to_string(),
                ));
                continue;
            }
        };

        let file_created_at_ms = md
            .created()
            .ok()
            .and_then(|created| created.duration_since(time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as u64)
            .unwrap_or(file_modified_at_ms);

        entries.push(Entry {
            db_id: None,
            id: nanoid!(),
            full_path: de.path().to_path_buf(),
            name: de.file_name().to_string_lossy().into(),
            kind: if is_file {
                EntryKind::File
            } else {
                EntryKind::Dir
            },
            file_kind: file_kind.map(str::to_string),
            hash: None,
            size,
            file_modified_at_ms,
            file_created_at_ms,
            synced_at_ms: None,
            missing_at_ms: None,
        });
    }

    if !unlistable.is_empty() {
        tracing::warn!(
            path = %unlistable[0].0.display(),
            error = %unlistable[0].1,
            count = unlistable.len(),
            "filesystem scan could not read some paths"
        );
    }

    // prune empty folders that contain no supported files
    let mut valid_dirs: HashSet<PathBuf> = HashSet::new();
    for entry in &entries {
        if entry.kind == EntryKind::File {
            let mut current = entry.full_path.parent();
            while let Some(p) = current {
                valid_dirs.insert(p.to_path_buf());
                current = p.parent();
            }
        }
    }

    let entries = entries
        .into_iter()
        .filter(|entry| entry.kind == EntryKind::File || valid_dirs.contains(&entry.full_path))
        .collect();
    Ok(ScanResult {
        entries,
        unlistable,
    })
}

pub(crate) fn diff_and_needs_hash(
    scanned: Vec<Entry>,
    existing: Vec<Entry>,
) -> (Vec<Entry>, Vec<PathBuf>) {
    let mut db_by_path = HashMap::new();
    let mut db_by_sha1 = MultiMap::new();
    for e in existing.into_iter() {
        if let Some(ref h) = e.hash {
            db_by_sha1.insert(h.clone(), e.clone());
        }
        db_by_path.insert(e.full_path.clone(), e.clone());
    }

    let mut enriched = Vec::with_capacity(scanned.len());
    let mut to_hash = Vec::new();
    for mut e in scanned.into_iter() {
        if let Some(old) = db_by_path.get(&e.full_path) {
            e.db_id = old.db_id;
            e.id = old.id.clone();
            if old.size == e.size && old.file_modified_at_ms == e.file_modified_at_ms {
                e.hash = old.hash.clone();
            }
        }
        if e.kind == EntryKind::File && e.hash.is_none() {
            to_hash.push(e.full_path.clone());
        }
        enriched.push(e);
    }

    (enriched, to_hash)
}

pub(crate) fn hash_entry_group(mut entries: Vec<Entry>) -> Vec<Entry> {
    let to_hash = entries
        .iter()
        .filter(|entry| entry.kind == EntryKind::File && entry.hash.is_none())
        .map(|entry| entry.full_path.clone())
        .collect();
    let hash_map = compute_hashes(to_hash);
    for entry in &mut entries {
        if entry.kind == EntryKind::File
            && entry.hash.is_none()
            && let Some(hash) = hash_map.get(entry.full_path.to_string_lossy().as_ref())
        {
            entry.hash = Some(hash.clone());
        }
    }
    entries
}

pub(crate) fn compute_hashes(to_hash: Vec<PathBuf>) -> HashMap<String, String> {
    let pairs: Vec<(String, String)> = to_hash
        .into_par_iter()
        .filter_map(|path| {
            let file_path = path.to_string_lossy().into_owned();
            let mut file = match File::open(&path) {
                Ok(file) => file,
                Err(err) => {
                    tracing::warn!(
                        path = %file_path,
                        error = %err,
                        "failed to open file for hashing"
                    );
                    return None;
                }
            };
            let mut hasher = Hasher::new();
            if let Err(err) = hasher.update_reader(&mut file) {
                tracing::warn!(
                    path = %file_path,
                    error = %err,
                    "failed to hash file"
                );
                return None;
            }
            Some((file_path, hasher.finalize().to_hex().to_string()))
        })
        .collect();

    pairs.into_iter().collect()
}

/// Scans the library and plans what changed. Fails closed when the scan finds no supported file
/// while the library has entries, as on an unmounted share: nothing then counts as gone.
pub(crate) fn prepare_entry_scan_plan(
    library: &Library,
    existing: Vec<Entry>,
) -> anyhow::Result<EntryScanPlan> {
    let ScanResult {
        entries: scanned,
        unlistable,
    } = scan_fs(&library.path)?;
    if !existing.is_empty() && !scanned.iter().any(|entry| entry.kind == EntryKind::File) {
        return Err(ScanError::EmptyRoot.into());
    }
    let (enriched, _) = diff_and_needs_hash(scanned, existing);
    let observed_paths = enriched
        .iter()
        .map(|entry| entry.full_path.clone())
        .collect::<HashSet<_>>();
    let groups = group_by_source_dir(&library.path, enriched);
    Ok(EntryScanPlan {
        groups,
        observed_paths,
        unlistable,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::fs;
    use std::time::{
        SystemTime,
        UNIX_EPOCH,
    };

    use agdb::DbAny;

    use super::*;
    use lyra_db::entries::{
        EntryKind,
        load_existing,
        mark_missing,
        sync_entry_group,
    };
    use lyra_db::test_db::TestDb;

    fn temp_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock drift")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "lyra-items-{}-{}-{}",
            name,
            std::process::id(),
            nanos
        ))
    }

    fn new_db() -> anyhow::Result<DbAny> {
        Ok(TestDb::initialized()?.into_inner())
    }

    fn new_library(db: &mut DbAny, root: &Path) -> anyhow::Result<Library> {
        lyra_db::test_db::insert_test_library_node(db, "Test", root.to_path_buf())
    }

    fn full_sync(db: &mut DbAny, library: &Library) -> anyhow::Result<()> {
        let library_db_id = library
            .db_id
            .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;
        let existing = load_existing(db, library_db_id)?;
        let plan = prepare_entry_scan_plan(library, existing)?;
        let protected = plan.protected_paths();
        for group in plan.groups {
            sync_entry_group(db, library, hash_entry_group(group.entries))?;
        }
        mark_missing(
            db,
            library,
            &plan.observed_paths,
            &protected,
            |removed, surviving| {
                crate::services::metadata::ingestion::albums_left_by(
                    &library.path,
                    removed,
                    surviving,
                )
            },
        )?;
        Ok(())
    }

    #[test]
    fn full_sync_marks_a_deleted_directory_subtree_missing() -> anyhow::Result<()> {
        let root = temp_path("dir-delete");
        let album_dir = root.join("album");
        let disc_dir = album_dir.join("disc1");
        fs::create_dir_all(&disc_dir)?;
        fs::write(album_dir.join("track1.mp3"), b"track1")?;
        fs::write(disc_dir.join("track2.mp3"), b"track2")?;
        let kept = root.join("kept.mp3");
        fs::write(&kept, b"kept")?;

        let mut db = new_db()?;
        let library = new_library(&mut db, &root)?;

        full_sync(&mut db, &library)?;
        let existing = load_existing(&db, library.db_id.unwrap())?;
        assert_eq!(existing.len(), 5);

        fs::remove_dir_all(&album_dir)?;
        full_sync(&mut db, &library)?;
        let remaining = load_existing(&db, library.db_id.unwrap())?;
        assert_eq!(remaining.len(), 5);
        let present = remaining
            .iter()
            .filter(|entry| !entry.is_missing())
            .map(|entry| entry.full_path.clone())
            .collect::<Vec<_>>();
        assert_eq!(present, vec![kept]);

        fs::create_dir_all(&disc_dir)?;
        fs::write(album_dir.join("track1.mp3"), b"track1")?;
        fs::write(disc_dir.join("track2.mp3"), b"track2")?;
        full_sync(&mut db, &library)?;
        let revived = load_existing(&db, library.db_id.unwrap())?;
        assert_eq!(revived.len(), 5);
        assert!(revived.iter().all(|entry| !entry.is_missing()));

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn full_sync_marks_only_the_missing_file() -> anyhow::Result<()> {
        let root = temp_path("file-delete");
        let album_dir = root.join("album");
        fs::create_dir_all(&album_dir)?;
        let track1 = album_dir.join("track1.mp3");
        let track2 = album_dir.join("track2.mp3");
        fs::write(&track1, b"track1")?;
        fs::write(&track2, b"track2")?;

        let mut db = new_db()?;
        let library = new_library(&mut db, &root)?;

        full_sync(&mut db, &library)?;
        fs::remove_file(&track1)?;
        full_sync(&mut db, &library)?;

        let remaining = load_existing(&db, library.db_id.unwrap())?
            .into_iter()
            .filter(|entry| !entry.is_missing())
            .collect::<Vec<_>>();
        let paths: HashSet<PathBuf> = remaining
            .iter()
            .map(|entry| entry.full_path.clone())
            .collect();
        assert_eq!(remaining.len(), 2);
        assert!(paths.contains(&album_dir));
        assert!(paths.contains(&track2));
        assert!(remaining.iter().any(|entry| entry.kind == EntryKind::Dir));
        assert!(remaining.iter().any(|entry| entry.kind == EntryKind::File));

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn full_sync_updates_modified_file() -> anyhow::Result<()> {
        let root = temp_path("file-update");
        let album_dir = root.join("album");
        fs::create_dir_all(&album_dir)?;
        let track = album_dir.join("track1.mp3");
        fs::write(&track, b"track1")?;

        let mut db = new_db()?;
        let library = new_library(&mut db, &root)?;

        full_sync(&mut db, &library)?;
        let existing = load_existing(&db, library.db_id.unwrap())?;
        let entry = existing
            .iter()
            .find(|entry| entry.full_path == track)
            .expect("entry exists");
        let entry_db_id = entry.db_id.unwrap();
        let old_hash = entry.hash.clone().unwrap();

        fs::write(&track, b"track1-updated")?;

        full_sync(&mut db, &library)?;
        let updated = load_existing(&db, library.db_id.unwrap())?;
        let updated_entry = updated
            .iter()
            .find(|entry| entry.full_path == track)
            .expect("entry exists");
        let new_hash = updated_entry.hash.clone().unwrap();

        assert_ne!(old_hash, new_hash);
        assert_eq!(updated_entry.db_id, Some(entry_db_id));
        assert!(
            updated_entry.synced_at_ms.is_none(),
            "the changed entry kept its sync"
        );

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[tokio::test]
    async fn sync_entry_group_preserves_disc_directories_for_metadata_grouping()
    -> anyhow::Result<()> {
        let root = temp_path("disc-group");
        let album = root.join("album");
        let discs = [album.join("Disc 1"), album.join("Disc 2")];
        for disc in &discs {
            fs::create_dir_all(disc)?;
            fs::write(disc.join("01.mp3"), b"track-one")?;
            fs::write(disc.join("02.mp3"), b"track-two")?;
        }

        let mut db = new_db()?;
        let library = new_library(&mut db, &root)?;
        let plan = prepare_entry_scan_plan(&library, Vec::new())?;
        assert_eq!(plan.groups.len(), 1);
        for group in plan.groups {
            sync_entry_group(&mut db, &library, group.entries)?;
        }

        let entries = load_existing(&db, library.db_id.unwrap())?;
        for disc in &discs {
            assert!(entries.iter().any(|entry| &entry.full_path == disc));
        }
        let groups =
            crate::services::metadata::ingestion::metadata_groups(&library.path, entries).await;
        assert_eq!(groups.len(), 1, "disc tracks must share one metadata group");
        assert_eq!(
            groups[0]
                .entries
                .iter()
                .filter(|entry| entry.kind == EntryKind::File)
                .count(),
            4,
        );

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn sync_entry_group_defers_missing_entry_marking() -> anyhow::Result<()> {
        let root = temp_path("group-prune");
        let album_a = root.join("album-a");
        let album_b = root.join("album-b");
        fs::create_dir_all(&album_a)?;
        fs::create_dir_all(&album_b)?;
        let track_a = album_a.join("track-a.mp3");
        let track_b = album_b.join("track-b.mp3");
        fs::write(&track_a, b"track-a")?;
        fs::write(&track_b, b"track-b")?;

        let mut db = new_db()?;
        let library = new_library(&mut db, &root)?;

        let first_plan = prepare_entry_scan_plan(&library, Vec::new())?;
        for group in first_plan.groups {
            sync_entry_group(&mut db, &library, hash_entry_group(group.entries))?;
        }
        let first_entries = load_existing(&db, library.db_id.unwrap())?;
        assert!(first_entries.iter().any(|entry| entry.full_path == track_a));
        assert!(first_entries.iter().any(|entry| entry.full_path == track_b));

        fs::remove_dir_all(&album_a)?;
        let existing = load_existing(&db, library.db_id.unwrap())?;
        let second_plan = prepare_entry_scan_plan(&library, existing)?;
        let observed_paths = second_plan.observed_paths.clone();
        for group in second_plan.groups {
            sync_entry_group(&mut db, &library, hash_entry_group(group.entries))?;
        }

        let before_prune = load_existing(&db, library.db_id.unwrap())?;
        assert!(
            before_prune.iter().any(|entry| entry.full_path == track_a),
            "group sync should not delete entries missing from other groups"
        );

        mark_missing(
            &mut db,
            &library,
            &observed_paths,
            &[],
            |removed, surviving| {
                crate::services::metadata::ingestion::albums_left_by(
                    &library.path,
                    removed,
                    surviving,
                )
            },
        )?;
        let after_prune = load_existing(&db, library.db_id.unwrap())?;
        assert!(
            after_prune
                .iter()
                .any(|entry| entry.full_path == track_a && entry.is_missing())
        );
        assert!(after_prune.iter().any(|entry| entry.full_path == track_b));

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn an_emptied_root_fails_closed() -> anyhow::Result<()> {
        let root = temp_path("empty-root");
        let album_dir = root.join("album");
        fs::create_dir_all(&album_dir)?;
        fs::write(album_dir.join("track1.mp3"), b"track1")?;

        let mut db = new_db()?;
        let library = new_library(&mut db, &root)?;
        full_sync(&mut db, &library)?;

        fs::remove_dir_all(&album_dir)?;
        let err = full_sync(&mut db, &library).expect_err("an emptied root syncs nothing");
        assert!(matches!(
            err.downcast_ref::<ScanError>(),
            Some(ScanError::EmptyRoot)
        ));
        assert_eq!(load_existing(&db, library.db_id.unwrap())?.len(), 2);

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn an_empty_new_library_scans() -> anyhow::Result<()> {
        let root = temp_path("empty-new");
        fs::create_dir_all(&root)?;
        let mut db = new_db()?;
        let library = new_library(&mut db, &root)?;
        full_sync(&mut db, &library)?;
        assert!(load_existing(&db, library.db_id.unwrap())?.is_empty());
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn an_unlistable_folder_keeps_its_entries() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let root = temp_path("unlistable");
        let locked = root.join("locked");
        let open = root.join("open");
        fs::create_dir_all(&locked)?;
        fs::create_dir_all(&open)?;
        fs::write(locked.join("a.mp3"), b"a")?;
        fs::write(open.join("b.mp3"), b"b")?;
        fs::write(open.join("c.mp3"), b"c")?;

        let mut db = new_db()?;
        let library = new_library(&mut db, &root)?;
        full_sync(&mut db, &library)?;

        fs::set_permissions(&locked, fs::Permissions::from_mode(0o000))?;
        if fs::read_dir(&locked).is_ok() {
            // Running with privileges that ignore permissions.
            fs::set_permissions(&locked, fs::Permissions::from_mode(0o755))?;
            let _ = fs::remove_dir_all(&root);
            return Ok(());
        }
        fs::remove_file(open.join("c.mp3"))?;
        let plan = prepare_entry_scan_plan(&library, load_existing(&db, library.db_id.unwrap())?)?;
        assert_eq!(plan.protected_paths(), vec![locked.clone()]);
        let result = full_sync(&mut db, &library);
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o755))?;
        result?;

        let missing = load_existing(&db, library.db_id.unwrap())?
            .into_iter()
            .map(|entry| (entry.full_path.clone(), entry.is_missing()))
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(missing.get(&locked), Some(&false));
        assert_eq!(missing.get(&locked.join("a.mp3")), Some(&false));
        assert_eq!(missing.get(&open.join("b.mp3")), Some(&false));
        assert_eq!(missing.get(&open.join("c.mp3")), Some(&true));

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }
}
