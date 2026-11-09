// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Which release a library folder belongs to, which decides whether a `cover.*` image in it is
//! that release's cover.

use std::{
    collections::HashMap,
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
use lyra_db::{
    DbAccess,
    Library,
    entries::EntryKind,
};

struct AudioFile {
    entry_id: DbId,
    /// Whether a scan has finished with the file. One without tracks that isn't may still get
    /// some, so it can't yet count against anyone.
    read: bool,
}

#[derive(Default)]
struct AudioFolder {
    files: Vec<AudioFile>,
    subfolders: Vec<PathBuf>,
}

/// How much of a folder is judged.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum FolderDepth {
    /// The audio directly in it.
    Direct,
    /// All the audio beneath it.
    Recursive,
}

/// What a folder's audio says about a release's claim to the folder.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FolderClaim {
    /// All of it is the release's, and no other's.
    Sole,
    /// Some of it is another release's: a track on several releases counts for none of them
    /// alone. Audio read and on no release says nothing either way.
    Disputed,
    /// None of it is known to be another's, but some has yet to be read.
    Unsettled,
}

/// Where a library's audio files are, read once for a pass, so judging a release costs about its
/// own size and not the library's. Only a run of the library changes its entries and one runs at
/// a time, so this stays good for the run. Who a file belongs to is not kept here: it is read
/// from the database at each judgment, as a provider refresh can merge releases during the run.
/// Files marked missing are here too, so an unavailable disc doesn't change who owns a folder.
#[derive(Default)]
pub(crate) struct AudioFolders {
    folders: HashMap<PathBuf, AudioFolder>,
}

impl AudioFolders {
    pub(crate) fn of_libraries(db: &impl DbAccess, libraries: &[Library]) -> Result<Self> {
        let mut folders = Self::default();
        for library in libraries {
            let Some(library_id) = library.db_id else {
                continue;
            };
            for entry in lyra_db::entries::load_existing(db, library_id)? {
                if entry.kind != EntryKind::File || entry.file_kind.as_deref() != Some("audio") {
                    continue;
                }
                let (Some(entry_id), Some(dir)) = (entry.db_id, entry.full_path.parent()) else {
                    continue;
                };
                folders.folder(dir, &library.path).files.push(AudioFile {
                    entry_id,
                    read: entry.synced_at_ms.is_some(),
                });
            }
        }
        Ok(folders)
    }

    pub(crate) fn of_library(db: &impl DbAccess, library_id: DbId) -> Result<Self> {
        let libraries = lyra_db::libraries::get_by_id(db, library_id)?;
        Self::of_libraries(db, libraries.as_slice())
    }

    /// The folders of every library holding the release, for judging it alone.
    pub(crate) fn of_release(db: &impl DbAccess, release_id: DbId) -> Result<Self> {
        Self::of_libraries(db, &lyra_db::libraries::get_by_release(db, release_id)?)
    }

    /// The folder at `dir`, linked into the folder above it up to `root` when it is new.
    fn folder(&mut self, dir: &Path, root: &Path) -> &mut AudioFolder {
        if !self.folders.contains_key(dir) {
            self.folders
                .insert(dir.to_path_buf(), AudioFolder::default());
            let mut child = dir;
            while child != root
                && let Some(parent) = child.parent()
                && parent.starts_with(root)
            {
                let known = self.folders.contains_key(parent);
                self.folders
                    .entry(parent.to_path_buf())
                    .or_default()
                    .subfolders
                    .push(child.to_path_buf());
                if known {
                    break;
                }
                child = parent;
            }
        }
        self.folders.get_mut(dir).expect("inserted above")
    }

    /// Judges the release's claim to `dir`, stopping at the first file that disproves it, so the
    /// walk only ever goes through audio of the release's own and one other file. A folder with
    /// no audio to go by, such as one outside the library, is no release's.
    pub(super) fn claim(
        &self,
        db: &DbAny,
        dir: &Path,
        release_id: DbId,
        depth: FolderDepth,
    ) -> Result<FolderClaim> {
        let mut walk = ClaimWalk {
            db,
            release_id,
            depth,
            found_audio: false,
            unsettled: false,
        };
        if walk.disproved_in(self, dir)? || !walk.found_audio {
            return Ok(FolderClaim::Disputed);
        }
        Ok(if walk.unsettled {
            FolderClaim::Unsettled
        } else {
            FolderClaim::Sole
        })
    }
}

struct ClaimWalk<'a> {
    db: &'a DbAny,
    release_id: DbId,
    depth: FolderDepth,
    found_audio: bool,
    unsettled: bool,
}

impl ClaimWalk<'_> {
    fn disproved_in(&mut self, folders: &AudioFolders, dir: &Path) -> Result<bool> {
        let Some(folder) = folders.folders.get(dir) else {
            return Ok(false);
        };
        for file in &folder.files {
            let releases = lyra_db::entries::release_ids(self.db, file.entry_id)?;
            if releases.is_empty() && file.read {
                continue;
            }
            self.found_audio = true;
            if releases.is_empty() {
                self.unsettled = true;
            } else if releases.len() != 1 || !releases.contains(&self.release_id) {
                return Ok(true);
            }
        }
        if self.depth == FolderDepth::Recursive {
            for subfolder in &folder.subfolders {
                if self.disproved_in(folders, subfolder)? {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}
