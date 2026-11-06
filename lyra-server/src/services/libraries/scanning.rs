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
use walkdir::{
    DirEntry,
    WalkDir,
};

use crate::services::{
    libraries::{
        MediaRoots,
        is_media_file,
    },
    metadata::ingestion::{
        SourceDirGroup,
        group_by_source_dir,
    },
};
use lyra_db::{
    Entry,
    Library,
    entries::{
        EntryKind,
        StoredEntries,
        classify_file_kind,
        is_protected,
    },
    is_supported_extension,
};

#[derive(Debug)]
pub(crate) struct EntryScanPlan {
    /// The directories scanned, shallowest first, each after the ones holding it.
    pub(crate) dirs: Vec<Entry>,
    /// The files scanned, by source directory.
    pub(crate) groups: Vec<SourceDirGroup>,
    /// What the scan was compared against.
    pub(crate) stored: StoredEntries,
    pub(crate) observed_paths: HashSet<PathBuf>,
    /// The entries that moved, as they were before.
    pub(crate) moved_from: Vec<Entry>,
    pub(crate) gaps: ScanGaps,
}

/// What a scan left out.
#[derive(Debug, Default)]
pub(crate) struct ScanGaps {
    /// The paths the scan couldn't read, with why. What lies under them is no evidence either way.
    pub(crate) unlistable: Vec<(PathBuf, String)>,
    /// The links the scan left out, with why. Unlike unlistable paths, they protect nothing.
    pub(crate) skipped_links: Vec<(PathBuf, String)>,
}

impl ScanGaps {
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
    EmptyRoot { gaps: ScanGaps },
    #[error("library root can't be listed: {0}")]
    UnlistableRoot(String),
}

pub(crate) struct ScanResult {
    pub(crate) entries: Vec<Entry>,
    pub(crate) gaps: ScanGaps,
}

/// What a scan following links has to remember, to read no folder twice.
pub(crate) struct FollowedLinks {
    roots: MediaRoots,
    /// The folders followed links led to so far.
    followed: Vec<PathBuf>,
}

impl FollowedLinks {
    /// A scan following links in a library of `roots`, which should show every library.
    pub(crate) fn new(roots: MediaRoots) -> Self {
        Self {
            roots,
            followed: Vec::new(),
        }
    }

    /// Why the scan leaves out the link `entry`, if it does. A folder it keeps counts as followed.
    fn refusal(&mut self, entry: &DirEntry) -> std::io::Result<Option<&'static str>> {
        let target = std::fs::canonicalize(entry.path())?;
        // Under a library nested in this one, that library's setting decides, as it does at serve.
        if !self.roots.follows_links_at(entry.path()) {
            return Ok(Some("the library holding the link doesn't follow links"));
        }
        if entry.file_type().is_dir() {
            if self.roots.in_library(&target) {
                return Ok(Some("the link leads into a library"));
            }
            if self
                .followed
                .iter()
                .any(|folder| target.starts_with(folder))
            {
                return Ok(Some("the link leads into a folder already followed"));
            }
            self.followed.push(target);
            return Ok(None);
        }
        if !entry.file_type().is_file() {
            return Ok(Some("the link's target is not a regular file"));
        }
        if !is_media_file(&target) {
            return Ok(Some(
                "the link's target is not audio, a cue sheet or a cover",
            ));
        }
        Ok(None)
    }
}

/// Scans `root`, following its links with `followed`, or skipping them without.
pub(crate) fn scan_fs(
    root: &Path,
    mut followed: Option<FollowedLinks>,
) -> anyhow::Result<ScanResult> {
    let mut entries = Vec::new();
    let mut gaps = ScanGaps::default();

    let walker = WalkDir::new(root).follow_links(followed.is_some());
    // Which of two links to one folder is followed shouldn't change between syncs.
    let mut walker = if followed.is_some() {
        walker.sort_by_file_name()
    } else {
        walker
    }
    .into_iter();
    while let Some(entry) = walker.next() {
        let de = match entry {
            Ok(de) => de,
            Err(err) => {
                let message = err.to_string();
                match err.path() {
                    Some(path) if path != root => {
                        gaps.unlistable.push((path.to_path_buf(), message))
                    }
                    _ => return Err(ScanError::UnlistableRoot(message).into()),
                }
                continue;
            }
        };

        // A stored path is text, so one that isn't would never match itself again.
        if de.path().to_str().is_none() {
            gaps.unlistable.push((
                de.path().to_path_buf(),
                "the path is not valid UTF-8".to_string(),
            ));
            if de.file_type().is_dir() {
                walker.skip_current_dir();
            }
            continue;
        }

        if de.depth() > 0 && de.path_is_symlink() {
            let refusal = match &mut followed {
                Some(followed) => followed.refusal(&de),
                None => Ok(Some("symbolic links are not followed in this library")),
            };
            let left_out = match refusal {
                Ok(None) => false,
                Ok(Some(reason)) => {
                    gaps.skipped_links
                        .push((de.path().to_path_buf(), reason.to_string()));
                    true
                }
                Err(err) => {
                    gaps.unlistable
                        .push((de.path().to_path_buf(), err.to_string()));
                    true
                }
            };
            if left_out {
                if de.file_type().is_dir() {
                    walker.skip_current_dir();
                }
                continue;
            }
        }

        let md = match de.metadata() {
            Ok(md) => md,
            Err(err) => {
                if de.path() == root {
                    return Err(ScanError::UnlistableRoot(err.to_string()).into());
                }
                gaps.unlistable
                    .push((de.path().to_path_buf(), err.to_string()));
                if de.file_type().is_dir() {
                    walker.skip_current_dir();
                }
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
                gaps.unlistable.push((
                    de.path().to_path_buf(),
                    "failed to read the modification time".to_string(),
                ));
                // Entries under a folder that has none would have no parent to go under.
                if md.is_dir() && de.path() != root {
                    walker.skip_current_dir();
                }
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

    if !gaps.unlistable.is_empty() {
        tracing::warn!(
            path = %gaps.unlistable[0].0.display(),
            error = %gaps.unlistable[0].1,
            count = gaps.unlistable.len(),
            "filesystem scan could not read some paths"
        );
    }
    if let Some((path, reason)) = gaps.skipped_links.first() {
        tracing::warn!(
            path = %path.display(),
            reason,
            count = gaps.skipped_links.len(),
            "filesystem scan skipped symbolic links"
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
    Ok(ScanResult { entries, gaps })
}

/// The scanned entries with what the library already knows of them: the stored ids of those at a
/// known path, and the hash a file keeps while its size and modification time do. A file at a new
/// path takes over the entry of a file the scan no longer sees, one outside `protected` paths,
/// when exactly one such entry and exactly one new file share their content. Returns the entries
/// and, as they were, those that moved.
pub(crate) fn diff_and_match_moves(
    scanned: Vec<Entry>,
    stored: &StoredEntries,
    protected: &[PathBuf],
) -> (Vec<Entry>, Vec<Entry>) {
    let scanned_paths = scanned
        .iter()
        .map(|entry| entry.full_path.clone())
        .collect::<HashSet<_>>();
    let mut db_by_sha1: MultiMap<String, &Entry> = MultiMap::new();
    for e in stored.entries() {
        if e.kind == EntryKind::File
            && !scanned_paths.contains(&e.full_path)
            && !is_protected(&e.full_path, protected)
            && let Some(ref h) = e.hash
        {
            db_by_sha1.insert(h.clone(), e);
        }
    }

    let mut enriched = Vec::with_capacity(scanned.len());
    let mut new_files = Vec::new();
    for mut e in scanned.into_iter() {
        match stored.at_path(&e.full_path) {
            Some(old) => {
                e.db_id = old.db_id;
                e.id = old.id.clone();
                if old.size == e.size && old.file_modified_at_ms == e.file_modified_at_ms {
                    e.hash = old.hash.clone();
                }
            }
            None if e.kind == EntryKind::File => new_files.push(enriched.len()),
            None => {}
        }
        enriched.push(e);
    }

    let mut moved_from = Vec::new();
    if db_by_sha1.is_empty() || new_files.is_empty() {
        return (enriched, moved_from);
    }
    let hashes = compute_hashes(
        new_files
            .iter()
            .map(|&index| enriched[index].full_path.clone())
            .collect(),
    );
    let mut new_by_hash: MultiMap<String, usize> = MultiMap::new();
    for &index in &new_files {
        let entry = &mut enriched[index];
        entry.hash = hashes
            .get(entry.full_path.to_string_lossy().as_ref())
            .cloned();
        if let Some(hash) = &entry.hash {
            new_by_hash.insert(hash.clone(), index);
        }
    }
    for (hash, indices) in new_by_hash.iter_all() {
        let ([index], Some([old])) = (
            indices.as_slice(),
            db_by_sha1.get_vec(hash).map(Vec::as_slice),
        ) else {
            continue;
        };
        let entry = &mut enriched[*index];
        if entry.file_kind != old.file_kind {
            continue;
        }
        entry.db_id = old.db_id;
        entry.id = old.id.clone();
        moved_from.push((*old).clone());
    }
    (enriched, moved_from)
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

/// Scans the library, following links with `followed` or skipping them without, and plans what
/// changed. Fails closed when the scan finds no supported file while the
/// library has entries, as on an unmounted share: nothing then counts as gone.
pub(crate) fn prepare_entry_scan_plan(
    library: &Library,
    existing: Vec<Entry>,
    followed: Option<FollowedLinks>,
) -> anyhow::Result<EntryScanPlan> {
    let ScanResult {
        entries: scanned,
        gaps,
    } = scan_fs(&library.path, followed)?;
    let stored = StoredEntries::new(library, existing)?;
    if !stored.is_empty() && !scanned.iter().any(|entry| entry.kind == EntryKind::File) {
        return Err(ScanError::EmptyRoot { gaps }.into());
    }
    let protected = gaps.protected_paths();
    let (enriched, moved_from) = diff_and_match_moves(scanned, &stored, &protected);
    let observed_paths = enriched
        .iter()
        .map(|entry| entry.full_path.clone())
        .collect::<HashSet<_>>();
    let (mut dirs, files): (Vec<_>, Vec<_>) = enriched
        .into_iter()
        .partition(|entry| entry.kind == EntryKind::Dir);
    dirs.sort_by_key(|dir| dir.full_path.components().count());
    Ok(EntryScanPlan {
        dirs,
        groups: group_by_source_dir(&library.path, files),
        stored,
        observed_paths,
        moved_from,
        gaps,
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

    use super::*;
    use crate::services::libraries::sync::{
        mark_missing_entries_for_scan,
        sync_entries_incremental,
        sync_entry_dirs,
    };
    use lyra_db::DbAsync;
    use lyra_db::entries::{
        EntryKind,
        mark_missing,
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

    fn new_db() -> anyhow::Result<DbAsync> {
        Ok(DbAsync::new(TestDb::initialized()?.into_inner()))
    }

    async fn new_library(db: &DbAsync, root: &Path) -> anyhow::Result<Library> {
        lyra_db::test_db::insert_test_library_node(
            &mut *db.write().await,
            "Test",
            root.to_path_buf(),
        )
    }

    async fn load_existing(db: &DbAsync, library: &Library) -> anyhow::Result<Vec<Entry>> {
        lyra_db::entries::load_existing(&*db.read().await, library.db_id.unwrap())
    }

    async fn full_sync(db: &DbAsync, library: &Library) -> anyhow::Result<()> {
        let observation = sync_entries_incremental(
            db,
            library,
            crate::services::libraries::SyncRunProgress::unreported_for_test(),
        )
        .await?;
        mark_missing_entries_for_scan(
            db,
            library,
            &observation,
            crate::services::libraries::SyncRunProgress::unreported_for_test(),
        )
        .await
    }

    #[tokio::test]
    async fn full_sync_marks_a_deleted_directory_subtree_missing() -> anyhow::Result<()> {
        let root = temp_path("dir-delete");
        let album_dir = root.join("album");
        let disc_dir = album_dir.join("disc1");
        fs::create_dir_all(&disc_dir)?;
        fs::write(album_dir.join("track1.mp3"), b"track1")?;
        fs::write(disc_dir.join("track2.mp3"), b"track2")?;
        let kept = root.join("kept.mp3");
        fs::write(&kept, b"kept")?;

        let db = new_db()?;
        let library = new_library(&db, &root).await?;

        full_sync(&db, &library).await?;
        let existing = load_existing(&db, &library).await?;
        assert_eq!(existing.len(), 5);

        fs::remove_dir_all(&album_dir)?;
        full_sync(&db, &library).await?;
        let remaining = load_existing(&db, &library).await?;
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
        full_sync(&db, &library).await?;
        let revived = load_existing(&db, &library).await?;
        assert_eq!(revived.len(), 5);
        assert!(revived.iter().all(|entry| !entry.is_missing()));

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[tokio::test]
    async fn full_sync_marks_only_the_missing_file() -> anyhow::Result<()> {
        let root = temp_path("file-delete");
        let album_dir = root.join("album");
        fs::create_dir_all(&album_dir)?;
        let track1 = album_dir.join("track1.mp3");
        let track2 = album_dir.join("track2.mp3");
        fs::write(&track1, b"track1")?;
        fs::write(&track2, b"track2")?;

        let db = new_db()?;
        let library = new_library(&db, &root).await?;

        full_sync(&db, &library).await?;
        fs::remove_file(&track1)?;
        full_sync(&db, &library).await?;

        let remaining = load_existing(&db, &library)
            .await?
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

    #[tokio::test]
    async fn full_sync_updates_modified_file() -> anyhow::Result<()> {
        let root = temp_path("file-update");
        let album_dir = root.join("album");
        fs::create_dir_all(&album_dir)?;
        let track = album_dir.join("track1.mp3");
        fs::write(&track, b"track1")?;

        let db = new_db()?;
        let library = new_library(&db, &root).await?;

        full_sync(&db, &library).await?;
        let existing = load_existing(&db, &library).await?;
        let entry = existing
            .iter()
            .find(|entry| entry.full_path == track)
            .expect("entry exists");
        let entry_db_id = entry.db_id.unwrap();
        let old_hash = entry.hash.clone().unwrap();

        fs::write(&track, b"track1-updated")?;

        full_sync(&db, &library).await?;
        let updated = load_existing(&db, &library).await?;
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
    async fn a_scan_preserves_disc_directories_for_metadata_grouping() -> anyhow::Result<()> {
        let root = temp_path("disc-group");
        let album = root.join("album");
        let discs = [album.join("Disc 1"), album.join("Disc 2")];
        for disc in &discs {
            fs::create_dir_all(disc)?;
            fs::write(disc.join("01.mp3"), b"track-one")?;
            fs::write(disc.join("02.mp3"), b"track-two")?;
        }

        let db = new_db()?;
        let library = new_library(&db, &root).await?;
        let plan = prepare_entry_scan_plan(&library, Vec::new(), None)?;
        assert_eq!(plan.groups.len(), 1);
        full_sync(&db, &library).await?;

        let entries = load_existing(&db, &library).await?;
        for disc in &discs {
            assert!(entries.iter().any(|entry| &entry.full_path == disc));
        }
        let groups = crate::services::metadata::ingestion::metadata_groups(
            &library.path,
            entries,
            &Default::default(),
            &std::sync::Arc::new(crate::services::libraries::MediaRoots::around([library
                .path
                .clone()])),
        )
        .await;
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

    #[tokio::test]
    async fn applying_a_group_defers_missing_entry_marking() -> anyhow::Result<()> {
        let root = temp_path("group-prune");
        let album_a = root.join("album-a");
        let album_b = root.join("album-b");
        fs::create_dir_all(&album_a)?;
        fs::create_dir_all(&album_b)?;
        let track_a = album_a.join("track-a.mp3");
        let track_b = album_b.join("track-b.mp3");
        fs::write(&track_a, b"track-a")?;
        fs::write(&track_b, b"track-b")?;

        let db = new_db()?;
        let library = new_library(&db, &root).await?;

        sync_entries_incremental(
            &db,
            &library,
            crate::services::libraries::SyncRunProgress::unreported_for_test(),
        )
        .await?;
        let first_entries = load_existing(&db, &library).await?;
        assert!(first_entries.iter().any(|entry| entry.full_path == track_a));
        assert!(first_entries.iter().any(|entry| entry.full_path == track_b));

        fs::remove_dir_all(&album_a)?;
        let observation = sync_entries_incremental(
            &db,
            &library,
            crate::services::libraries::SyncRunProgress::unreported_for_test(),
        )
        .await?;

        let before_prune = load_existing(&db, &library).await?;
        assert!(
            before_prune.iter().any(|entry| entry.full_path == track_a),
            "group sync should not delete entries missing from other groups"
        );

        mark_missing(
            &mut *db.write().await,
            &library,
            &observation.observed_paths,
            &[],
            &[],
            |removed, surviving| {
                crate::services::metadata::ingestion::albums_left_by(
                    &library.path,
                    removed,
                    surviving,
                )
            },
        )?;
        let after_prune = load_existing(&db, &library).await?;
        assert!(
            after_prune
                .iter()
                .any(|entry| entry.full_path == track_a && entry.is_missing())
        );
        assert!(after_prune.iter().any(|entry| entry.full_path == track_b));

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[tokio::test]
    async fn siblings_share_one_new_ancestor_and_files_go_under_their_folders() -> anyhow::Result<()>
    {
        let root = temp_path("shared-ancestor");
        let artist = root.join("artist");
        let albums = [artist.join("one"), artist.join("two")];
        for album in &albums {
            fs::create_dir_all(album)?;
            fs::write(album.join("track.mp3"), album.to_string_lossy().as_bytes())?;
        }

        let db = new_db()?;
        let library = new_library(&db, &root).await?;
        let plan = prepare_entry_scan_plan(&library, Vec::new(), None)?;
        assert_eq!(plan.groups.len(), 2);
        assert!(
            plan.groups
                .iter()
                .flat_map(|group| &group.entries)
                .all(|entry| entry.kind == EntryKind::File),
            "directories go ahead of the groups"
        );
        assert_eq!(plan.dirs.first().map(|dir| &dir.full_path), Some(&root));
        assert_eq!(plan.dirs[1].full_path, artist);

        full_sync(&db, &library).await?;
        let entries = load_existing(&db, &library).await?;
        let id_of = |path: &Path| {
            let found = entries
                .iter()
                .filter(|entry| entry.full_path == path)
                .collect::<Vec<_>>();
            assert_eq!(found.len(), 1, "{} stored once", path.display());
            found[0].db_id.expect("entry db_id")
        };
        assert_eq!(entries.len(), 5);
        let db_read = db.read().await;
        let parent_of = |entry| lyra_db::entries::parent_id(&*db_read, entry);
        assert_eq!(parent_of(id_of(&artist))?, library.db_id);
        for album in &albums {
            assert_eq!(parent_of(id_of(album))?, Some(id_of(&artist)));
            assert_eq!(
                parent_of(id_of(&album.join("track.mp3")))?,
                Some(id_of(album))
            );
        }
        drop(db_read);

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[tokio::test]
    async fn a_new_directory_kept_without_its_files_is_the_next_syncs() -> anyhow::Result<()> {
        let root = temp_path("dirs-only");
        let kept = root.join("kept");
        let gone = root.join("gone");
        for album in [&kept, &gone] {
            fs::create_dir_all(album)?;
            fs::write(album.join("track.mp3"), album.to_string_lossy().as_bytes())?;
        }

        let db = new_db()?;
        let library = new_library(&db, &root).await?;
        let EntryScanPlan {
            dirs, mut stored, ..
        } = prepare_entry_scan_plan(&library, Vec::new(), None)?;
        sync_entry_dirs(&db, &mut stored, dirs).await?;
        assert_eq!(load_existing(&db, &library).await?.len(), 2);

        fs::remove_dir_all(&gone)?;
        full_sync(&db, &library).await?;

        let entries = load_existing(&db, &library).await?;
        let paths = entries
            .iter()
            .map(|entry| entry.full_path.clone())
            .collect::<HashSet<_>>();
        assert_eq!(
            paths,
            HashSet::from([kept.clone(), kept.join("track.mp3")]),
            "the kept folder got its file, and the gone one went without a trace"
        );
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|entry| !entry.is_missing()));

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    /// Gives `dir` a modification time before the epoch, or returns false when this filesystem
    /// keeps none.
    fn age_past_the_epoch(dir: &Path) -> anyhow::Result<bool> {
        let before_epoch = UNIX_EPOCH - std::time::Duration::from_secs(86_400);
        fs::File::open(dir)?.set_modified(before_epoch)?;
        Ok(fs::metadata(dir)?.modified()? < UNIX_EPOCH)
    }

    #[tokio::test]
    async fn a_folder_without_an_entry_keeps_the_scan_out_and_its_entries() -> anyhow::Result<()> {
        let root = temp_path("no-mtime");
        let old = root.join("old");
        let open = root.join("open");
        fs::create_dir_all(old.join("nested"))?;
        fs::create_dir_all(&open)?;
        fs::write(old.join("nested/a.mp3"), b"a")?;
        fs::write(open.join("b.mp3"), b"b")?;

        let db = new_db()?;
        let library = new_library(&db, &root).await?;
        full_sync(&db, &library).await?;
        if !age_past_the_epoch(&old)? {
            let _ = fs::remove_dir_all(&root);
            return Ok(());
        }
        let late = old.join("nested/late.mp3");
        fs::write(&late, b"late")?;

        let plan = prepare_entry_scan_plan(&library, load_existing(&db, &library).await?, None)?;
        assert_eq!(plan.gaps.protected_paths(), vec![old.clone()]);
        full_sync(&db, &library).await?;

        let entries = load_existing(&db, &library).await?;
        assert_eq!(entries.len(), 5, "nothing new came from under the folder");
        assert!(entries.iter().all(|entry| !entry.is_missing()));

        let fresh = temp_path("no-mtime-fresh");
        fs::create_dir_all(fresh.join("old"))?;
        fs::create_dir_all(fresh.join("open"))?;
        fs::write(fresh.join("old/a.mp3"), b"a")?;
        fs::write(fresh.join("open/b.mp3"), b"b")?;
        age_past_the_epoch(&fresh.join("old"))?;
        let library = lyra_db::test_db::insert_test_library_node(
            &mut *db.write().await,
            "Fresh",
            fresh.clone(),
        )?;
        full_sync(&db, &library).await?;
        let paths = load_existing(&db, &library)
            .await?
            .into_iter()
            .map(|entry| entry.full_path)
            .collect::<HashSet<_>>();
        assert_eq!(
            paths,
            HashSet::from([fresh.join("open"), fresh.join("open/b.mp3")]),
            "a first scan with such a folder still finishes"
        );

        let _ = fs::remove_dir_all(&root);
        let _ = fs::remove_dir_all(&fresh);
        Ok(())
    }

    #[tokio::test]
    async fn a_path_that_is_not_text_is_reported_and_left_out() -> anyhow::Result<()> {
        use std::{
            ffi::OsStr,
            os::unix::ffi::OsStrExt,
        };

        let root = temp_path("not-text");
        let folder = root.join(OsStr::from_bytes(b"folder\xff"));
        let file = root.join(OsStr::from_bytes(b"file\xfe.mp3"));
        fs::create_dir_all(&folder)?;
        fs::write(folder.join("inside.mp3"), b"inside")?;
        fs::write(&file, b"file")?;
        let plain = root.join("plain.mp3");
        fs::write(&plain, b"plain")?;

        let db = new_db()?;
        let library = new_library(&db, &root).await?;
        for _ in 0..2 {
            full_sync(&db, &library).await?;
            let entries = load_existing(&db, &library).await?;
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].full_path, plain);
        }
        let plan = prepare_entry_scan_plan(&library, Vec::new(), None)?;
        let unlistable = plan
            .gaps
            .protected_paths()
            .into_iter()
            .collect::<HashSet<_>>();
        assert_eq!(unlistable, HashSet::from([folder, file]));

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[tokio::test]
    async fn an_emptied_root_fails_closed() -> anyhow::Result<()> {
        let root = temp_path("empty-root");
        let album_dir = root.join("album");
        fs::create_dir_all(&album_dir)?;
        fs::write(album_dir.join("track1.mp3"), b"track1")?;

        let db = new_db()?;
        let library = new_library(&db, &root).await?;
        full_sync(&db, &library).await?;

        fs::remove_dir_all(&album_dir)?;
        let err = full_sync(&db, &library)
            .await
            .expect_err("an emptied root syncs nothing");
        assert!(matches!(
            err.downcast_ref::<ScanError>(),
            Some(ScanError::EmptyRoot { .. })
        ));
        assert_eq!(load_existing(&db, &library).await?.len(), 2);

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[tokio::test]
    async fn links_go_unfollowed_and_reported_apart_from_unlistable_paths() -> anyhow::Result<()> {
        let root = temp_path("links-off");
        let outside = temp_path("links-off-outside");
        fs::create_dir_all(&root)?;
        fs::create_dir_all(&outside)?;
        fs::write(root.join("song.mp3"), b"song")?;
        fs::write(outside.join("elsewhere.mp3"), b"elsewhere")?;
        fs::write(outside.join("config.json"), b"secret")?;
        let linked_folder = root.join("linked");
        let linked_file = root.join("x.flac");
        let broken = root.join("broken.flac");
        std::os::unix::fs::symlink(&outside, &linked_folder)?;
        std::os::unix::fs::symlink(outside.join("config.json"), &linked_file)?;
        std::os::unix::fs::symlink(root.join("nothing"), &broken)?;

        let ScanResult { entries, gaps } = scan_fs(&root, None)?;

        let paths = entries
            .iter()
            .map(|entry| entry.full_path.clone())
            .collect::<HashSet<_>>();
        assert_eq!(paths, HashSet::from([root.clone(), root.join("song.mp3")]));
        assert!(gaps.unlistable.is_empty());
        let skipped = gaps
            .skipped_links
            .into_iter()
            .map(|(path, _)| path)
            .collect::<HashSet<_>>();
        assert_eq!(skipped, HashSet::from([linked_folder, linked_file, broken]));

        let _ = fs::remove_dir_all(&root);
        let _ = fs::remove_dir_all(&outside);
        Ok(())
    }

    #[tokio::test]
    async fn a_file_replaced_by_a_link_counts_as_gone() -> anyhow::Result<()> {
        let root = temp_path("link-replaces");
        fs::create_dir_all(&root)?;
        let song = root.join("song.mp3");
        fs::write(&song, b"song")?;
        fs::write(root.join("kept.mp3"), b"kept")?;
        let outside = temp_path("link-replaces-outside");
        fs::write(&outside, b"secret")?;

        let db = new_db()?;
        let library = new_library(&db, &root).await?;
        full_sync(&db, &library).await?;
        fs::remove_file(&song)?;
        std::os::unix::fs::symlink(&outside, &song)?;
        full_sync(&db, &library).await?;

        let entries = load_existing(&db, &library).await?;
        let entry = entries
            .iter()
            .find(|entry| entry.full_path == song)
            .expect("the entry stays");
        assert!(entry.is_missing());

        let _ = fs::remove_dir_all(&root);
        let _ = fs::remove_file(&outside);
        Ok(())
    }

    #[tokio::test]
    async fn a_root_that_is_a_link_still_scans_as_named() -> anyhow::Result<()> {
        let real = temp_path("link-root-real");
        fs::create_dir_all(&real)?;
        fs::write(real.join("song.mp3"), b"song")?;
        let root = temp_path("link-root");
        std::os::unix::fs::symlink(&real, &root)?;

        let ScanResult { entries, gaps } = scan_fs(&root, None)?;

        assert!(gaps.skipped_links.is_empty());
        assert!(
            entries
                .iter()
                .any(|entry| entry.full_path == root.join("song.mp3"))
        );

        let _ = fs::remove_file(&root);
        let _ = fs::remove_dir_all(&real);
        Ok(())
    }

    /// `root` and `outside` for a test of following links, removed when the test is over.
    fn following_layout(name: &str) -> anyhow::Result<(tempfile::TempDir, PathBuf, PathBuf)> {
        let dir = tempfile::Builder::new().prefix(name).tempdir()?;
        let base = fs::canonicalize(dir.path())?;
        let (root, outside) = (base.join("library"), base.join("outside"));
        fs::create_dir_all(&root)?;
        fs::create_dir_all(outside.join("album/disc"))?;
        fs::write(outside.join("album/song.mp3"), b"song")?;
        fs::write(outside.join("album/disc/deep.mp3"), b"deep")?;
        fs::write(outside.join("album/notes.txt"), b"notes")?;
        fs::write(outside.join("config.json"), b"secret")?;
        Ok((dir, root, outside))
    }

    fn scan_following(root: &Path, others: &[PathBuf]) -> anyhow::Result<ScanResult> {
        let roots =
            MediaRoots::around(others.iter().cloned().chain([root.to_path_buf()])).following(root);
        scan_fs(root, Some(FollowedLinks::new(roots)))
    }

    fn scanned(result: &ScanResult) -> HashSet<PathBuf> {
        result
            .entries
            .iter()
            .map(|entry| entry.full_path.clone())
            .collect()
    }

    fn skipped(result: &ScanResult) -> HashSet<PathBuf> {
        result
            .gaps
            .skipped_links
            .iter()
            .map(|(path, _)| path.clone())
            .collect()
    }

    #[test]
    fn a_followed_folder_keeps_its_lexical_paths() -> anyhow::Result<()> {
        let (_dir, root, outside) = following_layout("follow-folder")?;
        let linked = root.join("linked");
        std::os::unix::fs::symlink(outside.join("album"), &linked)?;

        let result = scan_following(&root, &[])?;

        assert_eq!(
            scanned(&result),
            HashSet::from([
                root.clone(),
                linked.clone(),
                linked.join("song.mp3"),
                linked.join("disc"),
                linked.join("disc/deep.mp3"),
            ])
        );
        assert!(result.gaps.skipped_links.is_empty());
        assert!(result.gaps.unlistable.is_empty());
        Ok(())
    }

    #[test]
    fn a_file_link_is_indexed_only_for_a_media_target() -> anyhow::Result<()> {
        let (_dir, root, outside) = following_layout("follow-file")?;
        let (audio, secret) = (root.join("a.mp3"), root.join("x.flac"));
        std::os::unix::fs::symlink(outside.join("album/song.mp3"), &audio)?;
        std::os::unix::fs::symlink(outside.join("config.json"), &secret)?;
        let fifo = outside.join("fifo.flac");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&fifo)
                .status()?
                .success()
        );
        let piped = root.join("piped.flac");
        std::os::unix::fs::symlink(&fifo, &piped)?;

        let result = scan_following(&root, &[])?;

        assert_eq!(scanned(&result), HashSet::from([root.clone(), audio]));
        assert_eq!(skipped(&result), HashSet::from([secret, piped]));
        Ok(())
    }

    #[test]
    fn a_folder_link_into_a_library_is_skipped_and_a_file_link_is_not() -> anyhow::Result<()> {
        let (_dir, root, outside) = following_layout("follow-library")?;
        let other = outside.join("album");
        let (folder, own, file) = (root.join("other"), root.join("own"), root.join("song.mp3"));
        fs::create_dir_all(root.join("real"))?;
        fs::write(root.join("real/kept.mp3"), b"kept")?;
        std::os::unix::fs::symlink(&other, &folder)?;
        std::os::unix::fs::symlink(root.join("real"), &own)?;
        std::os::unix::fs::symlink(other.join("song.mp3"), &file)?;

        let result = scan_following(&root, std::slice::from_ref(&other))?;

        assert_eq!(skipped(&result), HashSet::from([folder, own]));
        assert_eq!(
            scanned(&result),
            HashSet::from([
                root.clone(),
                root.join("real"),
                root.join("real/kept.mp3"),
                file,
            ])
        );
        Ok(())
    }

    #[test]
    fn a_link_in_a_nested_library_goes_by_that_librarys_setting() -> anyhow::Result<()> {
        let (_dir, root, outside) = following_layout("follow-nested")?;
        let nested = root.join("nested");
        fs::create_dir_all(&nested)?;
        fs::write(nested.join("own.mp3"), b"own")?;
        let (kept, left_out) = (root.join("a.mp3"), nested.join("b.mp3"));
        std::os::unix::fs::symlink(outside.join("album/song.mp3"), &kept)?;
        std::os::unix::fs::symlink(outside.join("album/song.mp3"), &left_out)?;

        let result = scan_following(&root, std::slice::from_ref(&nested))?;

        assert_eq!(skipped(&result), HashSet::from([left_out]));
        assert!(scanned(&result).contains(&kept));
        Ok(())
    }

    #[test]
    fn a_folder_already_followed_is_read_once() -> anyhow::Result<()> {
        let (_dir, root, outside) = following_layout("follow-twice")?;
        let album = outside.join("album");
        let (first, same, under, file) = (
            root.join("a"),
            root.join("b"),
            root.join("c"),
            root.join("d.mp3"),
        );
        std::os::unix::fs::symlink(&album, &first)?;
        std::os::unix::fs::symlink(&album, &same)?;
        std::os::unix::fs::symlink(album.join("disc"), &under)?;
        std::os::unix::fs::symlink(album.join("song.mp3"), &file)?;

        let result = scan_following(&root, &[])?;

        assert_eq!(skipped(&result), HashSet::from([same, under]));
        let paths = scanned(&result);
        assert!(paths.contains(&first.join("song.mp3")));
        assert!(paths.contains(&file), "a file link is read regardless");
        Ok(())
    }

    #[test]
    fn broken_links_and_loops_are_unlistable_while_following() -> anyhow::Result<()> {
        let (_dir, root, _outside) = following_layout("follow-broken")?;
        fs::write(root.join("song.mp3"), b"song")?;
        let (broken, up, spin) = (
            root.join("broken.flac"),
            root.join("up"),
            root.join("spin.flac"),
        );
        std::os::unix::fs::symlink(root.join("nothing"), &broken)?;
        std::os::unix::fs::symlink(&root, &up)?;
        std::os::unix::fs::symlink(&spin, &spin)?;

        let result = scan_following(&root, &[])?;

        let unlistable = result
            .gaps
            .unlistable
            .iter()
            .map(|(path, _)| path.clone())
            .collect::<HashSet<_>>();
        assert_eq!(unlistable, HashSet::from([broken, up, spin]));
        Ok(())
    }

    #[test]
    fn a_link_for_a_root_is_followed_as_the_root() -> anyhow::Result<()> {
        let (_dir, real, outside) = following_layout("follow-root")?;
        fs::write(real.join("song.mp3"), b"song")?;
        let root = outside.join("alias");
        std::os::unix::fs::symlink(&real, &root)?;

        let result = scan_following(&root, &[])?;

        assert!(result.gaps.skipped_links.is_empty());
        assert!(scanned(&result).contains(&root.join("song.mp3")));
        Ok(())
    }

    #[tokio::test]
    async fn an_empty_new_library_scans() -> anyhow::Result<()> {
        let root = temp_path("empty-new");
        fs::create_dir_all(&root)?;
        let db = new_db()?;
        let library = new_library(&db, &root).await?;
        full_sync(&db, &library).await?;
        assert!(load_existing(&db, &library).await?.is_empty());
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[tokio::test]
    async fn an_unlistable_folder_keeps_its_entries() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let root = temp_path("unlistable");
        let locked = root.join("locked");
        let open = root.join("open");
        fs::create_dir_all(&locked)?;
        fs::create_dir_all(&open)?;
        fs::write(locked.join("a.mp3"), b"a")?;
        fs::write(open.join("b.mp3"), b"b")?;
        fs::write(open.join("c.mp3"), b"c")?;

        let db = new_db()?;
        let library = new_library(&db, &root).await?;
        full_sync(&db, &library).await?;

        fs::set_permissions(&locked, fs::Permissions::from_mode(0o000))?;
        if fs::read_dir(&locked).is_ok() {
            // Running with privileges that ignore permissions.
            fs::set_permissions(&locked, fs::Permissions::from_mode(0o755))?;
            let _ = fs::remove_dir_all(&root);
            return Ok(());
        }
        fs::remove_file(open.join("c.mp3"))?;
        let plan = prepare_entry_scan_plan(&library, load_existing(&db, &library).await?, None)?;
        assert_eq!(plan.gaps.protected_paths(), vec![locked.clone()]);
        let result = full_sync(&db, &library).await;
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o755))?;
        result?;

        let missing = load_existing(&db, &library)
            .await?
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
