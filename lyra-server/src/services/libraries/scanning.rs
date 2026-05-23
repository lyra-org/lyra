// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        BTreeMap,
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

#[derive(Debug)]
pub(crate) struct EntryScanPlan {
    pub(crate) groups: Vec<ScannedEntryGroup>,
    pub(crate) observed_paths: HashSet<PathBuf>,
}

#[derive(Debug)]
pub(crate) struct ScannedEntryGroup {
    pub(crate) source_dir: PathBuf,
    pub(crate) entries: Vec<Entry>,
}

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

fn group_entries_by_source_dir(library_root: &Path, entries: Vec<Entry>) -> Vec<ScannedEntryGroup> {
    let mut dirs_by_path = BTreeMap::new();
    let mut files_by_source_dir: BTreeMap<PathBuf, Vec<Entry>> = BTreeMap::new();

    for entry in entries {
        match entry.kind {
            EntryKind::Dir => {
                dirs_by_path.insert(entry.full_path.clone(), entry);
            }
            EntryKind::File => {
                let parent = entry
                    .full_path
                    .parent()
                    .map(Path::to_path_buf)
                    .unwrap_or_else(|| library_root.to_path_buf());
                let source_dir = if parent != library_root
                    && parent
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(folder_name_looks_like_disc)
                {
                    parent.parent().map(Path::to_path_buf).unwrap_or(parent)
                } else {
                    parent
                };
                files_by_source_dir
                    .entry(source_dir)
                    .or_default()
                    .push(entry);
            }
        }
    }

    files_by_source_dir
        .into_iter()
        .map(|(source_dir, mut files)| {
            let mut entries = Vec::new();
            let mut ancestors = Vec::new();
            let mut current = Some(source_dir.as_path());
            while let Some(path) = current {
                if let Some(dir) = dirs_by_path.get(path) {
                    ancestors.push(dir.clone());
                }
                if path == library_root {
                    break;
                }
                current = path.parent();
            }
            ancestors.reverse();
            entries.extend(ancestors);
            entries.append(&mut files);
            ScannedEntryGroup {
                source_dir,
                entries,
            }
        })
        .collect()
}

fn folder_name_looks_like_disc(name: &str) -> bool {
    let normalized = name
        .to_ascii_lowercase()
        .replace(['_', '-', '.'], " ")
        .trim()
        .to_string();
    let compact = normalized.replace(' ', "");
    compact
        .strip_prefix("disc")
        .or_else(|| compact.strip_prefix("disk"))
        .or_else(|| compact.strip_prefix("cd"))
        .is_some_and(|suffix| !suffix.is_empty() && suffix.chars().all(|ch| ch.is_ascii_digit()))
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

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

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

pub(crate) fn prepare_entry_scan_plan(
    library: &Library,
    existing: Vec<Entry>,
) -> anyhow::Result<EntryScanPlan> {
    let scanned = scan_fs(&library.path)?;
    let (enriched, _) = diff_and_needs_hash(scanned, existing);
    let observed_paths = enriched
        .iter()
        .map(|entry| entry.full_path.clone())
        .collect::<HashSet<_>>();
    let groups = group_entries_by_source_dir(&library.path, enriched);
    Ok(EntryScanPlan {
        groups,
        observed_paths,
    })
}
