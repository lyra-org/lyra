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

use crate::{
    Library,
    db::{
        Entry,
        entries::{
            EntryKind,
            classify_file_kind,
        },
        is_supported_extension,
    },
};

pub(crate) fn scan_fs(root: &Path) -> anyhow::Result<Vec<Entry>> {
    let mut entries = Vec::new();
    let mut errors = Vec::new();

    for entry in WalkDir::new(root).into_iter() {
        let de = match entry {
            Ok(de) => de,
            Err(err) => {
                errors.push(err.into());
                continue;
            }
        };

        let md = match de.metadata() {
            Ok(md) => md,
            Err(err) => {
                errors.push(err.into());
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
        let mtime = match md
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(time::UNIX_EPOCH).ok())
        {
            Some(modified) => modified.as_secs(),
            None => {
                errors.push(anyhow::anyhow!(
                    "failed to read mtime for {}",
                    de.path().display()
                ));
                continue;
            }
        };

        let ctime = md
            .created()
            .ok()
            .and_then(|created| created.duration_since(time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(mtime);

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
            mtime,
            ctime,
        });
    }

    if !errors.is_empty() {
        let first = errors.swap_remove(0);
        if entries.is_empty() {
            return Err(anyhow::anyhow!(
                "filesystem scan failed for {} entries (first error: {})",
                errors.len() + 1,
                first
            ));
        }
        tracing::warn!(
            error = %first,
            error_count = errors.len() + 1,
            "filesystem scan skipped some entries"
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

    Ok(entries
        .into_iter()
        .filter(|entry| {
            if entry.kind == EntryKind::File {
                return true;
            }

            valid_dirs.contains(&entry.full_path)
        })
        .collect())
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
            if old.size == e.size && old.mtime == e.mtime {
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

pub(crate) fn prepare_entries(
    library: &Library,
    existing: Vec<Entry>,
) -> anyhow::Result<Vec<Entry>> {
    let scanned = scan_fs(&library.path)?;
    let (mut enriched, to_hash) = diff_and_needs_hash(scanned, existing);
    let hash_map = compute_hashes(to_hash);
    for e in &mut enriched {
        if e.kind == EntryKind::File
            && e.hash.is_none()
            && let Some(h) = hash_map.get(e.full_path.to_string_lossy().as_ref())
        {
            e.hash = Some(h.clone());
        }
    }
    Ok(enriched)
}
