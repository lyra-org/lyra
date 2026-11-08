// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Which entries a sync reads together: an album's source directory, the directories nested in
//! it or beside it in the same parent, and the directories its cue sheets' audio lives in. Folders
//! only decide what is compared; coalescing decides from the tags what merges.

use std::{
    collections::{
        BTreeMap,
        HashMap,
    },
    path::{
        Path,
        PathBuf,
    },
    sync::Arc,
};

use agdb::DbId;
use lyra_db::{
    Entry,
    entries::EntryKind,
};

use crate::services::libraries::MediaRoots;
use crate::services::metadata::{
    CueLinks,
    classify_entry_file_kind,
    cue::{
        build_audio_lookup,
        cue_audio_entry_ids,
    },
};

#[derive(Debug)]
pub(crate) struct SourceDirGroup {
    pub(crate) source_dir: PathBuf,
    pub(crate) entries: Vec<Entry>,
}

/// The directory whose files make one album: the file's own, or the one holding its disc folder.
fn source_dir_of(library_root: &Path, file: &Path) -> PathBuf {
    let parent = file
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| library_root.to_path_buf());
    let is_disc_folder = parent != library_root
        && parent
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| lyra_metadata::disc_number_from_folder_name(name).is_some());
    if is_disc_folder {
        parent.parent().map(Path::to_path_buf).unwrap_or(parent)
    } else {
        parent
    }
}

/// Each source directory's files, after the directories holding them.
pub(crate) fn group_by_source_dir(library_root: &Path, entries: Vec<Entry>) -> Vec<SourceDirGroup> {
    let mut dirs_by_path = BTreeMap::new();
    let mut files_by_source_dir: BTreeMap<PathBuf, Vec<Entry>> = BTreeMap::new();

    for entry in entries {
        match entry.kind {
            EntryKind::Dir => {
                dirs_by_path.insert(entry.full_path.clone(), entry);
            }
            EntryKind::File => {
                files_by_source_dir
                    .entry(source_dir_of(library_root, &entry.full_path))
                    .or_default()
                    .push(entry);
            }
        }
    }

    files_by_source_dir
        .into_iter()
        .map(|(source_dir, mut files)| {
            let mut ancestors = BTreeMap::new();
            for file in &files {
                let mut current = file.full_path.parent();
                while let Some(path) = current {
                    if ancestors.contains_key(path) {
                        break;
                    }
                    if let Some(dir) = dirs_by_path.get(path) {
                        ancestors.insert(path.to_path_buf(), dir.clone());
                    }
                    if path == library_root {
                        break;
                    }
                    current = path.parent();
                }
            }
            let mut entries = ancestors.into_values().collect::<Vec<_>>();
            entries.append(&mut files);
            SourceDirGroup {
                source_dir,
                entries,
            }
        })
        .collect()
}

/// Entries parsed together, their releases coalescing only with one another.
#[derive(Debug)]
pub(crate) struct MetadataGroup {
    pub(crate) source_dir: PathBuf,
    pub(crate) entries: Vec<Entry>,
    /// Source directories nested in folders that hold nothing else, by the folder they stand for.
    album_roots: HashMap<PathBuf, PathBuf>,
}

impl MetadataGroup {
    /// Each file's [`Self::candidate_dir`], by its entry.
    pub(crate) fn candidate_dirs(&self) -> BTreeMap<DbId, PathBuf> {
        self.entries
            .iter()
            .filter(|entry| entry.kind == EntryKind::File)
            .filter_map(|entry| Some((entry.db_id?, self.candidate_dir(&entry.full_path))))
            .collect()
    }

    /// The directory a file's tracks coalesce as: its album root, or its own folder below one.
    pub(crate) fn candidate_dir(&self, file: &Path) -> PathBuf {
        let parent = file.parent().unwrap_or(Path::new(""));
        self.album_roots
            .get(parent)
            .unwrap_or(&parent.to_path_buf())
            .clone()
    }
}

struct Unions(Vec<usize>);

impl Unions {
    fn new(len: usize) -> Self {
        Self((0..len).collect())
    }

    fn find(&mut self, index: usize) -> usize {
        let mut root = index;
        while self.0[root] != root {
            root = self.0[root];
        }
        self.0[index] = root;
        root
    }

    fn join(&mut self, left: usize, right: usize) {
        let (left, right) = (self.find(left), self.find(right));
        self.0[left.max(right)] = left.min(right);
    }

    fn merge(mut self, groups: Vec<MetadataGroup>) -> Vec<MetadataGroup> {
        let mut merged: BTreeMap<usize, MetadataGroup> = BTreeMap::new();
        for (index, group) in groups.into_iter().enumerate() {
            let root = self.find(index);
            match merged.get_mut(&root) {
                Some(target) => {
                    target.entries.extend(group.entries);
                    target.album_roots.extend(group.album_roots);
                }
                None => {
                    merged.insert(root, group);
                }
            }
        }
        merged.into_values().collect()
    }
}

/// The folder a source directory stands for: it climbs through parents that hold no files and
/// no other folder, as a `CD1/audio` stands for `CD1`, but never to a folder directly under the
/// library or above.
fn album_root(
    library_root: &Path,
    source_dir: &Path,
    file_dirs: &std::collections::HashSet<&Path>,
    subdirs: &HashMap<&Path, std::collections::HashSet<&Path>>,
) -> PathBuf {
    let mut dir = source_dir;
    while let Some(parent) = dir.parent() {
        let only_child = subdirs
            .get(parent)
            .is_some_and(|children| children.len() == 1);
        let under_root = parent != library_root && parent.starts_with(library_root);
        let holds_files = file_dirs.contains(parent);
        if !under_root || holds_files || !only_child {
            break;
        }
        match parent.parent() {
            Some(grandparent) if grandparent != library_root => dir = parent,
            _ => break,
        }
    }
    dir.to_path_buf()
}

/// A library's entries as candidate albums: each source directory with the ones nested in it and
/// its siblings, except directly under the library.
pub(crate) fn album_groups(library_root: &Path, entries: Vec<Entry>) -> Vec<MetadataGroup> {
    let groups = group_by_source_dir(library_root, entries);
    let file_dirs = groups
        .iter()
        .flat_map(|group| &group.entries)
        .filter(|entry| entry.kind == EntryKind::File)
        .filter_map(|entry| entry.full_path.parent())
        .collect::<std::collections::HashSet<_>>();
    let mut subdirs: HashMap<&Path, std::collections::HashSet<&Path>> = HashMap::new();
    for dir in &file_dirs {
        for child in dir.ancestors().take_while(|dir| *dir != library_root) {
            if let Some(parent) = child.parent() {
                subdirs.entry(parent).or_default().insert(child);
            }
        }
    }
    let roots = groups
        .iter()
        .map(|group| album_root(library_root, &group.source_dir, &file_dirs, &subdirs))
        .collect::<Vec<_>>();

    let mut unions = Unions::new(groups.len());
    let group_of_dir = groups
        .iter()
        .enumerate()
        .map(|(index, group)| (group.source_dir.clone(), index))
        .collect::<HashMap<_, _>>();
    for (index, group) in groups.iter().enumerate() {
        if group.source_dir == library_root {
            continue;
        }
        let enclosing = group
            .source_dir
            .ancestors()
            .skip(1)
            .take_while(|dir| *dir != library_root && dir.starts_with(library_root))
            .find_map(|dir| group_of_dir.get(dir).copied());
        if let Some(enclosing) = enclosing {
            unions.join(index, enclosing);
        }
    }
    let mut first_in_parent: HashMap<&Path, usize> = HashMap::new();
    for (index, root) in roots.iter().enumerate() {
        let Some(parent) = root
            .parent()
            .filter(|parent| *parent != library_root && parent.starts_with(library_root))
        else {
            continue;
        };
        match first_in_parent.get(parent) {
            Some(&first) => unions.join(index, first),
            None => {
                first_in_parent.insert(parent, index);
            }
        }
    }
    let groups = groups
        .into_iter()
        .zip(roots.iter())
        .map(|(group, root)| MetadataGroup {
            album_roots: (*root != group.source_dir)
                .then(|| (group.source_dir.clone(), root.clone()))
                .into_iter()
                .collect(),
            source_dir: group.source_dir,
            entries: group.entries,
        })
        .collect();
    unions.merge(groups)
}

/// The surviving files of every album a removed file belonged to.
pub(crate) fn albums_left_by(
    library_root: &Path,
    removed: &[Entry],
    surviving: &[Entry],
) -> Vec<DbId> {
    let removed_files = removed
        .iter()
        .filter(|entry| entry.kind == EntryKind::File)
        .map(|entry| entry.full_path.as_path())
        .collect::<std::collections::HashSet<_>>();
    if removed_files.is_empty() {
        return Vec::new();
    }
    album_groups(
        library_root,
        removed.iter().chain(surviving).cloned().collect(),
    )
    .into_iter()
    .filter(|album| {
        album
            .entries
            .iter()
            .any(|entry| removed_files.contains(entry.full_path.as_path()))
    })
    .flat_map(|album| album.entries)
    .filter(|entry| {
        entry.kind == EntryKind::File && !removed_files.contains(entry.full_path.as_path())
    })
    .filter_map(|entry| entry.db_id)
    .collect()
}

/// A library's albums, joined with those holding audio their cue sheets name by path.
pub(crate) async fn metadata_groups(
    library_root: &Path,
    entries: Vec<Entry>,
    cue_links: &CueLinks,
    roots: &Arc<MediaRoots>,
) -> Vec<MetadataGroup> {
    let albums = album_groups(library_root, entries);
    let mut unions = Unions::new(albums.len());
    let files = || {
        albums.iter().enumerate().flat_map(|(index, album)| {
            album
                .entries
                .iter()
                .filter(|entry| entry.kind == EntryKind::File)
                .map(move |entry| (index, entry))
        })
    };
    let audio = files()
        .filter(|(_, entry)| classify_entry_file_kind(entry).as_deref() == Some("audio"))
        .map(|(_, entry)| entry.clone())
        .collect::<Vec<_>>();
    let lookup = build_audio_lookup(&audio);
    let album_of_entry = files()
        .filter_map(|(index, entry)| Some((entry.db_id?, index)))
        .collect::<HashMap<DbId, usize>>();
    let cues = files()
        .filter(|(_, entry)| classify_entry_file_kind(entry).as_deref() == Some("cue"))
        .map(|(index, entry)| (index, entry.clone()))
        .collect::<Vec<_>>();
    // A cue sheet groups with the audio its tracks name and with the audio its stored tracks
    // play, which a move of the audio alone leaves unnamed.
    for (index, cue) in cues {
        let linked = cue
            .db_id
            .and_then(|id| cue_links.get(&id))
            .into_iter()
            .flat_map(|links| links.values().copied());
        for audio_id in cue_audio_entry_ids(&cue, &lookup, roots)
            .await
            .into_iter()
            .chain(linked)
        {
            if let Some(&audio_album) = album_of_entry.get(&audio_id) {
                unions.join(index, audio_album);
            }
        }
    }
    unions.merge(albums)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::metadata::tests::file_entry;

    async fn grouped(root: &Path, files: &[&str]) -> Vec<Vec<PathBuf>> {
        let entries = files
            .iter()
            .enumerate()
            .map(|(index, file)| file_entry(index as i64 + 1, &root.join(file)))
            .collect();
        let roots = Arc::new(MediaRoots::around([root.to_path_buf()]));
        metadata_groups(root, entries, &Default::default(), &roots)
            .await
            .into_iter()
            .map(|group| {
                group
                    .entries
                    .into_iter()
                    .map(|entry| entry.full_path.strip_prefix(root).expect("in root").into())
                    .collect()
            })
            .collect()
    }

    #[tokio::test]
    async fn albums_group_with_their_nested_and_sibling_folders() {
        let groups = grouped(
            Path::new("/music"),
            &[
                "Artist/One/01.flac",
                "Artist/One/Disc 2/01.flac",
                "Artist/One/Bonus/01.flac",
                "Artist/Two/01.flac",
                "Solo/01.flac",
                "loose.flac",
            ],
        )
        .await;

        assert_eq!(
            groups,
            [
                vec![PathBuf::from("loose.flac")],
                vec![
                    PathBuf::from("Artist/One/01.flac"),
                    PathBuf::from("Artist/One/Disc 2/01.flac"),
                    PathBuf::from("Artist/One/Bonus/01.flac"),
                    PathBuf::from("Artist/Two/01.flac"),
                ],
                vec![PathBuf::from("Solo/01.flac")],
            ]
        );
    }

    #[tokio::test]
    async fn cue_sheets_group_with_the_audio_they_name() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        std::fs::create_dir_all(dir.path().join("Sheets"))?;
        std::fs::write(
            dir.path().join("Sheets/album.cue"),
            "FILE \"../Audio/album.flac\" WAVE\n  TRACK 01 AUDIO\n    INDEX 01 00:00:00\n",
        )?;
        let groups = grouped(
            dir.path(),
            &["Audio/album.flac", "Other/album.flac", "Sheets/album.cue"],
        )
        .await;

        assert_eq!(
            groups,
            [
                vec![
                    PathBuf::from("Audio/album.flac"),
                    PathBuf::from("Sheets/album.cue"),
                ],
                vec![PathBuf::from("Other/album.flac")],
            ]
        );
        Ok(())
    }
}
